"""Seed safe historical FlowJury episodes for the demo environment.

Run after ``seed_demo_catalog.py`` and ``add_demo_dag_sources.py``. The script reads the current
DataHub metadata to create accurate evidence snapshots, but the narrative notes
are explicitly historical leads. They never replace current investigation.

    python -m scripts.seed_demo_memory
    python -m scripts.seed_demo_memory --memory-db ./state/flowjury-memory.sqlite3
"""

from __future__ import annotations

import argparse
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Optional, Sequence

from datahub.metadata.schema_classes import DataFlowInfoClass

from flowjury.analysis.evidence import gather_evidence
from flowjury.integrations.datahub.client import FlowJuryClient
from flowjury.memory.store import InvestigationMemory, text_fingerprint


GMS = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
TOKEN = os.environ.get("DATAHUB_GMS_TOKEN")
DEFAULT_MEMORY_DB = Path(__file__).resolve().parents[1] / ".flowjury" / "memory.sqlite3"
SEED_VERSION = "flowjury-demo-memory-v1"


@dataclass(frozen=True)
class DemoEpisode:
    pipeline: str
    recommendation: str
    confidence: float
    summary: str
    prior_observation: str
    risks: tuple[str, ...]
    skills: tuple[str, ...]


# At least one episode per seeded domain means every demo pipeline can retrieve
# either exact history or a related same-domain investigation on its first run.
DEMO_EPISODES = (
    DemoEpisode(
        "Nightly Marketing Customer Export",
        "UNKNOWN",
        0.45,
        "Previous review suspected an obsolete campaign export but left retirement unapproved.",
        "The campaign appeared complete and the owner appeared inactive; source, "
        "lineage, and blast radius still required fresh verification.",
        ("A previously uncataloged export destination could still exist.",),
        ("decide-pipeline-verdict", "handle-uncertain-pipelines"),
    ),
    DemoEpisode(
        "Nightly Hightouch Sync",
        "UNKNOWN",
        0.72,
        "Previous review found a reverse-ETL destination outside SQL lineage.",
        "Executable source previously referenced a Hightouch sync to HubSpot; "
        "re-check current code before relying on it.",
        ("External SaaS consumers are not fully represented in catalog lineage.",),
        ("decide-pipeline-verdict", "handle-uncertain-pipelines"),
    ),
    DemoEpisode(
        "Quarterly Compliance Close",
        "PROTECT",
        0.95,
        "Previous review treated this quarterly close as a protected control workload.",
        "Compliance and retention metadata previously explained the intentionally sparse "
        "schedule and usage.",
        ("Control ownership and retention requirements can change.",),
        ("decide-pipeline-verdict", "protect-regulated-pipelines"),
    ),
    DemoEpisode(
        "Customer Feature Materialization (Offline + Online)",
        "KEEP",
        0.82,
        "Previous review found an online feature-store consumer absent from SQL lineage.",
        "The prior DAG materialized customer features through Feast to an online store; "
        "current executable source remains authoritative.",
        ("The online serving path may have migrated since the previous review.",),
        ("decide-pipeline-verdict", "assess-healthy-pipelines"),
    ),
    DemoEpisode(
        "Enterprise Customer CDC Fanout",
        "KEEP",
        0.84,
        "Previous review found operational Kafka consumers behind the checkpoint output.",
        "The prior source published customer changes to fraud, support, and search topics "
        "that SQL lineage could not see.",
        ("Topic names and active subscriber services require current verification.",),
        ("decide-pipeline-verdict", "assess-healthy-pipelines"),
    ),
    DemoEpisode(
        "Customer 360 Daily Build",
        "KEEP",
        0.88,
        "Previous review found active downstream usage for the customer profile.",
        "The output previously had live consumers and sustained query activity.",
        ("Usage can decline after application migrations.",),
        ("decide-pipeline-verdict", "assess-healthy-pipelines"),
    ),
    DemoEpisode(
        "Fraud Scoring Batch",
        "FIX_OR_FOLD",
        0.78,
        "Previous review found repeated failures in a risk-scoring workload.",
        "Recent run history previously showed a sustained failure pattern; current runs "
        "and owner intent must be checked again.",
        ("Retiring a broken fraud pipeline without confirming replacement coverage is unsafe.",),
        ("decide-pipeline-verdict", "diagnose-pipeline-runs"),
    ),
)


def _stable_run_id(pipeline: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{SEED_VERSION}:{pipeline}"))


def _memory_context(dh: FlowJuryClient, job: str, pipeline: str) -> dict:
    flow_urn = dh.flow_urn_of(job)
    info = dh.graph.get_aspect(flow_urn, DataFlowInfoClass)
    properties = dict(info.customProperties or {}) if info else {}
    _, outputs = dh.io(job)
    return {
        "dag_source_sha256": text_fingerprint(
            properties.get("dag_source", "(no DAG source recorded)")
        ),
        "output_urns": sorted(str(output) for output in outputs),
    }


def _result(episode: DemoEpisode) -> SimpleNamespace:
    return SimpleNamespace(
        pipeline=episode.pipeline,
        recommendation=episode.recommendation,
        confidence=episode.confidence,
        summary=f"[DEMO SEED] {episode.summary}",
        evidence=[
            {
                "claim": "historical investigation lead",
                "source_tool": "demo_memory_seed",
                "observation": episode.prior_observation,
            }
        ],
        risks=list(episode.risks),
        skills_applied=list(episode.skills),
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed idempotent historical episodes into FlowJury memory."
    )
    parser.add_argument(
        "--memory-db",
        type=Path,
        default=DEFAULT_MEMORY_DB,
        help=f"Memory database (default: {DEFAULT_MEMORY_DB}).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    dh = FlowJuryClient(GMS, TOKEN)
    wanted = {episode.pipeline: episode for episode in DEMO_EPISODES}
    jobs_by_name = {}
    for job in dh.list_pipeline_jobs():
        name = dh.flow_info(dh.flow_urn_of(job)).get("name") or job
        if name in wanted:
            jobs_by_name[name] = job

    if not jobs_by_name:
        print("No matching demo pipelines found — run scripts.seed_demo_catalog first.")
        return 1

    created = 0
    existing = 0
    skipped = 0
    with InvestigationMemory(args.memory_db) as memory:
        for episode in DEMO_EPISODES:
            job = jobs_by_name.get(episode.pipeline)
            if job is None:
                print(f"skip (pipeline not found): {episode.pipeline}")
                skipped += 1
                continue
            run_id = _stable_run_id(episode.pipeline)
            if memory.contains_run(run_id):
                print(f"exists: {episode.pipeline}")
                existing += 1
                continue
            evidence = gather_evidence(dh, job)
            context = _memory_context(dh, job, episode.pipeline)
            observation = {
                "tool": "demo_memory_seed",
                "argument": episode.pipeline,
                "output": episode.prior_observation,
                "ok": True,
            }
            memory.record(
                _result(episode),
                evidence,
                [observation],
                context=context,
                run_id=run_id,
            )
            print(f"seeded: {episode.pipeline} -> {episode.recommendation}")
            created += 1

    print(
        f"\nMemory ready at {args.memory_db.resolve()} "
        f"({created} created, {existing} already present, {skipped} skipped)."
    )
    print("These are historical leads; every agent run must re-check current DataHub evidence.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
