"""Tests for readable terminal decision cards."""

from types import SimpleNamespace

from flowjury.ui.console import (
    render_assessment_header,
    render_recommendation,
    render_run_banner,
)


def recommendation(verdict: str = "KILL") -> SimpleNamespace:
    return SimpleNamespace(
        recommendation=verdict,
        confidence=0.92,
        investigation_status="COMPLETED",
        summary="The pipeline has no current consumers.",
        skills_applied=["decide-pipeline-verdict", "retire-orphan-pipelines"],
        evidence=[
            {
                "source_tool": "trace_downstream",
                "claim": "No downstream assets were found.",
                "observation": "DataHub lineage returned zero results.",
            }
        ],
        risks=["An uncataloged external consumer may still exist."],
        temporal_change={"status": "UNCHANGED", "changed_fields": []},
        skeptic_review={"decision": "PASS", "summary": "No blocker was found."},
        next_action="Route the proposal to the owner for approval.",
        memory_id="episode-123",
    )


def test_plain_decision_card_has_clear_explanation_sections():
    rendered = render_recommendation(recommendation(), color=False, width=90)

    assert "KILL" in rendered
    assert "CONFIDENCE 92%" in rendered
    assert "WHY THIS VERDICT" in rendered
    assert "SKILLS APPLIED" in rendered
    assert "EVIDENCE  (1 citations)" in rendered
    assert "PRIMARY RISKS" in rendered
    assert "MEMORY" in rendered
    assert "SKEPTIC REVIEW" in rendered
    assert "NEXT ACTION" in rendered
    assert "\033[" not in rendered


def test_colored_decision_card_uses_ansi_but_keeps_text():
    rendered = render_recommendation(recommendation("PROTECT"), color=True, width=90)

    assert "\033[" in rendered
    assert "PROTECT" in rendered
    assert "DataHub lineage returned zero results." in rendered


def test_run_and_assessment_headers_show_progress():
    banner = render_run_banner(18, color=False, width=90)
    header = render_assessment_header(
        "Nightly Marketing Customer Export", 9, 18, color=False, width=90
    )

    assert "FLOWJURY" in banner
    assert "18 PIPELINES" in banner
    assert "09/18" in header
    assert "Nightly Marketing Customer Export" in header
