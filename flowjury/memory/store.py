"""SQLite investigation store with evidence-change detection.

The SQLite backend stores metadata evidence and agent proposals, never warehouse
rows or secrets. A production deployment can replace this adapter with a
LangGraph Postgres or Redis Store without changing the agent-facing policy.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


_VOLATILE_EVIDENCE_FIELDS = {"current_runtime_min"}


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def text_fingerprint(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def evidence_snapshot(
    evidence: Any,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return the stable normalized evidence used for temporal comparison."""
    raw = _jsonable(evidence)
    if not isinstance(raw, dict):
        raise TypeError("evidence must be a dataclass or mapping")
    snapshot = {
        key: value for key, value in sorted(raw.items()) if key not in _VOLATILE_EVIDENCE_FIELDS
    }
    for key, value in sorted((context or {}).items()):
        snapshot[f"context_{key}"] = _jsonable(value)
    return snapshot


def evidence_fingerprint(snapshot: Dict[str, Any]) -> str:
    payload = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compare_snapshots(before: Dict[str, Any], after: Dict[str, Any]) -> List[dict]:
    return [
        {"field": field, "before": before.get(field), "after": after.get(field)}
        for field in sorted(set(before) | set(after))
        if before.get(field) != after.get(field)
    ]


def _clip(value: Any, limit: int) -> str:
    text = str(value or "")
    return text if len(text) <= limit else text[:limit] + "…"


def _compact_evidence(raw: Any) -> List[dict]:
    """Keep recalled evidence useful while guaranteeing bounded prompt payloads."""
    if not isinstance(raw, list):
        return []
    compact = []
    for item in raw[:2]:
        if not isinstance(item, dict):
            continue
        compact.append(
            {
                "claim": _clip(item.get("claim"), 140),
                "source_tool": _clip(item.get("source_tool"), 80),
                "observation": _clip(item.get("observation"), 180),
            }
        )
    return compact


class InvestigationMemory:
    """Store prior FlowJury investigations and recall comparable episodes."""

    def __init__(self, path: Path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(self.path))
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS investigation_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL UNIQUE,
                pipeline TEXT NOT NULL,
                domain TEXT,
                verdict TEXT NOT NULL,
                confidence REAL NOT NULL,
                summary TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                risks_json TEXT NOT NULL,
                skills_json TEXT NOT NULL,
                observations_json TEXT NOT NULL,
                snapshot_json TEXT NOT NULL,
                context_fingerprint TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            )
            """
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_memory_pipeline_time "
            "ON investigation_memory(pipeline, recorded_at DESC)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_memory_domain_time "
            "ON investigation_memory(domain, recorded_at DESC)"
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "InvestigationMemory":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def _latest(self, pipeline: str) -> Optional[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM investigation_memory WHERE pipeline = ? "
            "ORDER BY recorded_at DESC, id DESC LIMIT 1",
            (pipeline,),
        ).fetchone()

    def compare(self, evidence: Any, context: Optional[Dict[str, Any]] = None) -> dict:
        current = evidence_snapshot(evidence, context)
        current_fingerprint = evidence_fingerprint(current)
        previous = self._latest(str(current.get("pipeline", "")))
        if previous is None:
            return {
                "status": "FIRST_SEEN",
                "current_fingerprint": current_fingerprint,
                "changed_fields": [],
                "previous": None,
            }
        prior_snapshot = json.loads(previous["snapshot_json"])
        changes = compare_snapshots(prior_snapshot, current)
        return {
            "status": "UNCHANGED" if not changes else "CHANGED",
            "current_fingerprint": current_fingerprint,
            "changed_fields": changes,
            "previous": {
                "run_id": previous["run_id"],
                "verdict": previous["verdict"],
                "confidence": previous["confidence"],
                "recorded_at": previous["recorded_at"],
                "context_fingerprint": previous["context_fingerprint"],
            },
        }

    @staticmethod
    def _episode(row: sqlite3.Row, relation: str) -> dict:
        evidence = json.loads(row["evidence_json"])
        risks = json.loads(row["risks_json"])
        skills = json.loads(row["skills_json"])
        return {
            "relation": relation,
            "run_id": row["run_id"],
            "pipeline": row["pipeline"],
            "domain": row["domain"],
            "verdict": row["verdict"],
            "confidence": row["confidence"],
            "summary": _clip(row["summary"], 360),
            "evidence": _compact_evidence(evidence),
            "risks": [_clip(item, 180) for item in risks[:2]] if isinstance(risks, list) else [],
            "skills_applied": skills[:8] if isinstance(skills, list) else [],
            "context_fingerprint": row["context_fingerprint"],
            "recorded_at": row["recorded_at"],
        }

    def recall(
        self,
        evidence: Any,
        query: str = "",
        limit: int = 5,
        context: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """Recall exact history plus a few domain/text-related investigations."""
        snapshot = evidence_snapshot(evidence, context)
        pipeline = str(snapshot.get("pipeline", ""))
        domain = snapshot.get("domain")
        limit = max(1, min(int(limit), 10))
        exact = self.connection.execute(
            "SELECT * FROM investigation_memory WHERE pipeline = ? "
            "ORDER BY recorded_at DESC, id DESC LIMIT ?",
            (pipeline, min(limit, 3)),
        ).fetchall()

        episodes = [self._episode(row, "same_pipeline") for row in exact]
        seen = {row["run_id"] for row in exact}
        remaining = limit - len(episodes)
        if remaining > 0 and query.strip():
            escaped = query.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            pattern = f"%{escaped}%"
            related = self.connection.execute(
                "SELECT * FROM investigation_memory "
                "WHERE pipeline <> ? AND (pipeline LIKE ? ESCAPE '\\' "
                "OR summary LIKE ? ESCAPE '\\' OR evidence_json LIKE ? ESCAPE '\\') "
                "ORDER BY recorded_at DESC, id DESC LIMIT ?",
                (pipeline, pattern, pattern, pattern, remaining),
            ).fetchall()
            for row in related:
                if row["run_id"] not in seen:
                    episodes.append(self._episode(row, "text_match"))
                    seen.add(row["run_id"])

        remaining = limit - len(episodes)
        if remaining > 0 and domain:
            related = self.connection.execute(
                "SELECT * FROM investigation_memory WHERE pipeline <> ? AND domain = ? "
                "ORDER BY recorded_at DESC, id DESC LIMIT ?",
                (pipeline, domain, remaining),
            ).fetchall()
            for row in related:
                if row["run_id"] not in seen:
                    episodes.append(self._episode(row, "same_domain"))
                    seen.add(row["run_id"])

        return {
            "temporal_comparison": self.compare(evidence, context),
            "episodes": episodes[:limit],
            "safety_note": (
                "Prior verdicts are historical leads only. Re-check current DataHub evidence "
                "and never use memory alone to justify retirement."
            ),
        }

    def contains_run(self, run_id: str) -> bool:
        """Return whether an episode with this stable run ID already exists."""
        return (
            self.connection.execute(
                "SELECT 1 FROM investigation_memory WHERE run_id = ? LIMIT 1",
                (run_id,),
            ).fetchone()
            is not None
        )

    def record(
        self,
        result: Any,
        evidence: Any,
        observations: List[Any],
        context: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
    ) -> str:
        """Record an episode, optionally using a stable ID for idempotent seed data."""
        run_id = run_id or str(uuid.uuid4())
        if self.contains_run(run_id):
            return run_id
        snapshot = evidence_snapshot(evidence, context)
        serialized_observations = [
            item.as_dict() if hasattr(item, "as_dict") else _jsonable(item) for item in observations
        ]
        self.connection.execute(
            """
            INSERT INTO investigation_memory (
                run_id, pipeline, domain, verdict, confidence, summary,
                evidence_json, risks_json, skills_json, observations_json,
                snapshot_json, context_fingerprint, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                result.pipeline,
                snapshot.get("domain"),
                result.recommendation,
                float(result.confidence),
                result.summary,
                json.dumps(_jsonable(result.evidence), sort_keys=True),
                json.dumps(_jsonable(result.risks), sort_keys=True),
                json.dumps(_jsonable(result.skills_applied), sort_keys=True),
                json.dumps(serialized_observations, sort_keys=True),
                json.dumps(snapshot, sort_keys=True),
                evidence_fingerprint(snapshot),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.connection.commit()
        return run_id
