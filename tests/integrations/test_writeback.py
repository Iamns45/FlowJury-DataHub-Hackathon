"""Tests for writeback-only import of saved FlowJury proposals."""

import json

import pytest

from flowjury.integrations.datahub import writeback


def proposal(pipeline: str = "Nightly Marketing Customer Export") -> dict:
    return {
        "pipeline": pipeline,
        "recommendation": "KILL",
        "confidence": 0.92,
        "summary": "No current consumer was found.",
        "evidence": [
            {
                "claim": "No downstream assets.",
                "source_tool": "trace_downstream",
                "observation": "total=0",
            }
        ],
        "risks": ["A hidden consumer may exist."],
        "next_action": "Request owner approval.",
        "skills_applied": ["decide-pipeline-verdict", "retire-orphan-pipelines"],
        "temporal_change": {"status": "CHANGED"},
        "skeptic_review": {"decision": "PASS"},
        "memory_id": "episode-1",
    }


def test_load_proposal_file_builds_recommendations(tmp_path):
    path = tmp_path / "proposals.json"
    path.write_text(json.dumps([proposal()]), encoding="utf-8")

    results = writeback.load_proposal_file(path)

    assert len(results) == 1
    assert results[0].pipeline == "Nightly Marketing Customer Export"
    assert results[0].recommendation == "KILL"
    assert results[0].confidence == 0.92


def test_load_proposal_file_rejects_incomplete_payload_before_write(tmp_path):
    path = tmp_path / "proposals.json"
    path.write_text(json.dumps([{"pipeline": "missing fields"}]), encoding="utf-8")

    with pytest.raises(ValueError, match="is missing"):
        writeback.load_proposal_file(path)


def test_writeback_does_not_bypass_risky_skeptic_gate(tmp_path):
    path = tmp_path / "proposals.json"
    unsafe = proposal()
    unsafe["skeptic_review"] = None
    path.write_text(json.dumps([unsafe]), encoding="utf-8")

    class FakeDataHub:
        graph = object()

        def list_pipeline_jobs(self):
            return []

    report = writeback.write_existing_proposals(
        FakeDataHub(), "emitter", writeback.load_proposal_file(path)
    )

    assert report["written"] == 0
    assert report["errors"] == [
        "Nightly Marketing Customer Export: risky proposal lacks a passing skeptic review"
    ]


def test_writeback_skips_incomplete_proposal_without_blocking_file_load(tmp_path):
    path = tmp_path / "proposals.json"
    incomplete = proposal()
    incomplete["recommendation"] = "UNKNOWN"
    incomplete["investigation_status"] = "INCOMPLETE"
    path.write_text(json.dumps([incomplete]), encoding="utf-8")

    class FakeDataHub:
        graph = object()

        def list_pipeline_jobs(self):
            return []

    report = writeback.write_existing_proposals(
        FakeDataHub(), "emitter", writeback.load_proposal_file(path)
    )

    assert report["written"] == 0
    assert report["errors"] == [
        "Nightly Marketing Customer Export: incomplete proposal was skipped"
    ]


def test_write_existing_proposals_resolves_names_without_running_agent(monkeypatch):
    class FakeDataHub:
        graph = object()

        def list_pipeline_jobs(self):
            return ["job-1"]

        def flow_urn_of(self, job_urn):
            return "flow-1"

        def flow_info(self, flow_urn):
            return {"name": "Nightly Marketing Customer Export"}

    captured = []

    def fake_write(graph, emitter, flow_urn, result):
        captured.append((graph, emitter, flow_urn, result.recommendation))
        return True

    monkeypatch.setattr(writeback, "write_agent_proposal", fake_write)
    saved = proposal()
    recommendation = writeback.AgentRecommendation(
        pipeline=saved["pipeline"],
        recommendation=saved["recommendation"],
        confidence=saved["confidence"],
        summary=saved["summary"],
        evidence=saved["evidence"],
        risks=saved["risks"],
        next_action=saved["next_action"],
        skills_applied=saved["skills_applied"],
        skeptic_review=saved["skeptic_review"],
    )

    report = writeback.write_existing_proposals(FakeDataHub(), "emitter", [recommendation])

    assert report == {"written": 1, "errors": []}
    assert captured == [(FakeDataHub.graph, "emitter", "flow-1", "KILL")]


def test_write_existing_proposals_reports_missing_pipeline():
    class EmptyDataHub:
        graph = object()

        def list_pipeline_jobs(self):
            return []

    saved = proposal("Unknown Pipeline")
    recommendation = writeback.AgentRecommendation(
        pipeline=saved["pipeline"],
        recommendation=saved["recommendation"],
        confidence=saved["confidence"],
        summary=saved["summary"],
        evidence=saved["evidence"],
        risks=saved["risks"],
        next_action=saved["next_action"],
        skills_applied=saved["skills_applied"],
        skeptic_review=saved["skeptic_review"],
    )

    report = writeback.write_existing_proposals(EmptyDataHub(), "emitter", [recommendation])

    assert report["written"] == 0
    assert report["errors"] == ["Unknown Pipeline: no matching DataHub pipeline"]
