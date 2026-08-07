"""DataHub context resolved once for a FlowJury assessment batch."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

from datahub.metadata.schema_classes import DataFlowInfoClass

from flowjury.agent.serialization import to_jsonable
from flowjury.integrations.datahub.client import FlowJuryClient
from flowjury.memory.store import text_fingerprint


class InvestigationContext:
    """Resolve pipeline names, outputs, peer evidence, and source fingerprints."""

    def __init__(
        self,
        datahub: FlowJuryClient,
        jobs: Optional[Sequence[str]] = None,
        evidence_items: Optional[Sequence[Any]] = None,
    ):
        self.datahub = datahub
        self.by_name: Dict[str, List[dict]] = {}
        for job in jobs or datahub.list_pipeline_jobs():
            flow_urn = datahub.flow_urn_of(job)
            info = datahub.flow_info(flow_urn)
            _, outputs = datahub.io(job)
            name = info.get("name") or job
            self.by_name.setdefault(name, []).append(
                {
                    "flow": flow_urn,
                    "job": job,
                    "outputs": [str(output) for output in outputs],
                }
            )
        self.evidence_by_name = {
            evidence.pipeline: to_jsonable(asdict(evidence)) for evidence in (evidence_items or [])
        }

    def _resolve(self, name: str) -> Tuple[Optional[dict], Optional[str]]:
        matches = self.by_name.get(name, [])
        if not matches:
            return None, f"No pipeline named '{name}'."
        if len(matches) > 1:
            return None, f"Pipeline name '{name}' is ambiguous ({len(matches)} matches)."
        return matches[0], None

    def dag_source(self, name: str) -> str:
        entry, error = self._resolve(name)
        if error:
            return error
        info = self.datahub.graph.get_aspect(entry["flow"], DataFlowInfoClass)
        properties = dict(info.customProperties or {}) if info else {}
        return properties.get("dag_source", "(no DAG source recorded)")

    def output_urns(self, name: str) -> List[str]:
        entry, _ = self._resolve(name)
        return list(entry["outputs"]) if entry else []

    def flow_urn(self, name: str) -> Optional[str]:
        entry, _ = self._resolve(name)
        return entry["flow"] if entry else None

    def peer_evidence(self, name: str) -> List[dict]:
        """Return comparison facts for every other pipeline in this run."""
        keys = (
            "pipeline",
            "domain",
            "schedule",
            "inputs",
            "output_cols",
            "consumer_count",
            "queried",
            "usage_available",
            "max_queries_per_day",
        )
        return [
            {key: facts.get(key) for key in keys}
            for peer_name, facts in sorted(self.evidence_by_name.items())
            if peer_name != name
        ]

    def memory_context(self, name: str) -> dict:
        """Capture stable facts not represented in normalized evidence."""
        return {
            "dag_source_sha256": text_fingerprint(self.dag_source(name)),
            "output_urns": sorted(self.output_urns(name)),
        }
