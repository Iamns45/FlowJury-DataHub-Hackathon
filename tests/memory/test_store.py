"""Tests for durable investigation memory and evidence drift."""

import json
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from flowjury.domain.models import Evidence, Signal
from flowjury.memory.store import InvestigationMemory, evidence_snapshot, text_fingerprint


def recommendation(pipeline: str, verdict: str = "KEEP"):
    return SimpleNamespace(
        pipeline=pipeline,
        recommendation=verdict,
        confidence=0.8,
        summary=f"{pipeline} was reviewed.",
        evidence=[{"claim": "current context", "source_tool": "datahub_evidence"}],
        risks=["metadata may change"],
        skills_applied=["decide-pipeline-verdict", "assess-healthy-pipelines"],
    )


def base_evidence(name: str = "customer pipeline") -> Evidence:
    return Evidence(
        pipeline=name,
        domain="growth",
        consumer_count=1,
        has_consumers=Signal.TRUE,
        queried=Signal.TRUE,
        usage_available=True,
        usage_days=30,
    )


def test_memory_records_recalls_and_detects_temporal_change():
    with TemporaryDirectory() as directory:
        memory = InvestigationMemory(Path(directory) / "memory.sqlite3")
        evidence = base_evidence()
        run_id = memory.record(recommendation(evidence.pipeline), evidence, [])

        recalled = memory.recall(evidence)
        assert recalled["temporal_comparison"]["status"] == "UNCHANGED"
        assert recalled["episodes"][0]["run_id"] == run_id

        changed = replace(
            evidence,
            consumer_count=0,
            has_consumers=Signal.FALSE,
        )
        comparison = memory.compare(changed)
        assert comparison["status"] == "CHANGED"
        assert {item["field"] for item in comparison["changed_fields"]} >= {
            "consumer_count",
            "has_consumers",
        }
        memory.close()


def test_memory_retrieves_same_domain_episode():
    with TemporaryDirectory() as directory:
        memory = InvestigationMemory(Path(directory) / "memory.sqlite3")
        other = base_evidence("other growth pipeline")
        memory.record(recommendation(other.pipeline), other, [])

        recalled = memory.recall(base_evidence("new growth pipeline"))
        assert recalled["episodes"][0]["relation"] == "same_domain"
        assert recalled["episodes"][0]["pipeline"] == other.pipeline
        memory.close()


def test_runtime_minutes_are_excluded_from_fingerprint_noise():
    evidence = base_evidence()
    first = replace(evidence, current_runtime_min=10)
    second = replace(evidence, current_runtime_min=11)
    assert evidence_snapshot(first) == evidence_snapshot(second)


def test_source_change_is_part_of_temporal_context():
    with TemporaryDirectory() as directory:
        memory = InvestigationMemory(Path(directory) / "memory.sqlite3")
        evidence = base_evidence()
        before = {"dag_source_sha256": text_fingerprint("write_to_table()")}
        after = {"dag_source_sha256": text_fingerprint("producer.produce('topic')")}
        memory.record(recommendation(evidence.pipeline), evidence, [], context=before)

        comparison = memory.compare(evidence, context=after)

        assert comparison["status"] == "CHANGED"
        assert "context_dag_source_sha256" in {
            item["field"] for item in comparison["changed_fields"]
        }
        memory.close()


def test_stable_run_id_makes_demo_seeding_idempotent():
    with TemporaryDirectory() as directory:
        memory = InvestigationMemory(Path(directory) / "memory.sqlite3")
        evidence = base_evidence("seeded pipeline")
        run_id = "demo-seed-run-id"

        assert memory.contains_run(run_id) is False
        assert (
            memory.record(recommendation(evidence.pipeline), evidence, [], run_id=run_id) == run_id
        )
        assert (
            memory.record(recommendation(evidence.pipeline), evidence, [], run_id=run_id) == run_id
        )
        assert memory.contains_run(run_id) is True
        assert len(memory.recall(evidence)["episodes"]) == 1
        memory.close()


def test_recalled_episodes_stay_bounded_and_valid_for_tool_transport():
    with TemporaryDirectory() as directory:
        memory = InvestigationMemory(Path(directory) / "memory.sqlite3")
        evidence = base_evidence("verbose pipeline")
        verbose = SimpleNamespace(
            pipeline=evidence.pipeline,
            recommendation="KEEP",
            confidence=0.8,
            summary="s" * 10000,
            evidence=[
                {
                    "claim": "c" * 10000,
                    "source_tool": "datahub_evidence",
                    "observation": "o" * 10000,
                }
            ]
            * 10,
            risks=["r" * 10000] * 10,
            skills_applied=["decide-pipeline-verdict", "assess-healthy-pipelines"],
        )
        memory.record(verbose, evidence, [])
        memory.record(verbose, evidence, [])

        payload = memory.recall(evidence, limit=2)
        serialized = json.dumps(payload)

        assert len(payload["episodes"]) == 2
        assert len(serialized) < 6000
        assert json.loads(serialized)["episodes"]
        assert payload["episodes"][0]["summary"].endswith("…")
        memory.close()
