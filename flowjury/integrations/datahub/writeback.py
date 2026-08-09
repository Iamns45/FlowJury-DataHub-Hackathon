"""Write FlowJury review metadata back to DataHub without changing jobs.

After FlowJury decides, this adapter stores the review as separate DataHub tags
and custom properties so the next person or agent can inherit it.

Everything is provenance-stamped ('FlowJury') and reversible — it only ADDS
tags/properties, never deletes or overwrites your existing metadata.

"""

import json
import time
from pathlib import Path
from typing import Any

from flowjury.agent.models import AgentRecommendation


_REQUIRED_PROPOSAL_FIELDS = {
    "pipeline",
    "recommendation",
    "confidence",
    "summary",
    "evidence",
    "risks",
    "next_action",
    "skills_applied",
}
_VALID_RECOMMENDATIONS = {
    "KEEP",
    "KILL",
    "DOWNSHIFT",
    "REDUNDANT",
    "TRIM",
    "RUNAWAY",
    "FIX/FOLD",
    "PROTECT",
    "UNKNOWN",
}
_RISKY_RECOMMENDATIONS = {"KILL", "REDUNDANT"}


def merge_tags(graph, emitter, urn: str, tag_names) -> None:
    """Add tags without dropping any that already exist."""
    from datahub.emitter.mce_builder import make_tag_urn
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

    existing = graph.get_aspect(urn, GlobalTagsClass)
    tags = list(existing.tags) if existing and existing.tags else []
    have = {t.tag for t in tags}
    for name in tag_names:
        turn = make_tag_urn(name)
        if turn not in have:
            tags.append(TagAssociationClass(tag=turn))
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=GlobalTagsClass(tags=tags)))


def merge_properties(graph, emitter, flow_urn: str, props: dict) -> None:
    """Add custom properties, preserving schedule / dag_source / everything else."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import DataFlowInfoClass

    info = graph.get_aspect(flow_urn, DataFlowInfoClass)
    if info is None:
        return
    cp = dict(info.customProperties or {})
    cp.update(props)
    info.customProperties = cp
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=flow_urn, aspect=info))


def writeback_block_reason(result) -> str | None:
    """Return why a proposal cannot safely be represented as a reviewed DataHub verdict."""
    if result.investigation_status != "COMPLETED":
        return "incomplete proposal was skipped"
    if (
        result.recommendation in _RISKY_RECOMMENDATIONS
        and (result.skeptic_review or {}).get("decision") != "PASS"
    ):
        return "risky proposal lacks a passing skeptic review"
    return None


def write_agent_proposal(graph, emitter, flow_urn: str, result) -> bool:
    """Persist one agent proposal without mutating the pipeline or its schedule."""
    if writeback_block_reason(result):
        return False
    reviewed_at = time.strftime("%Y-%m-%d %H:%M")
    safe_recommendation = result.recommendation.replace("/", "-")
    merge_tags(
        graph,
        emitter,
        flow_urn,
        ["FlowJury-Agent-Reviewed", f"FlowJury-Agent-{safe_recommendation}"],
    )
    merge_properties(
        graph,
        emitter,
        flow_urn,
        {
            "flowjury_agent_recommendation": result.recommendation,
            "flowjury_agent_confidence": str(result.confidence),
            "flowjury_agent_summary": result.summary[:1400],
            "flowjury_agent_evidence": json.dumps(result.evidence)[:1400],
            "flowjury_agent_risks": json.dumps(result.risks)[:1400],
            "flowjury_agent_next_action": result.next_action[:1400],
            "flowjury_agent_skills": json.dumps(result.skills_applied)[:1400],
            "flowjury_agent_temporal_change": json.dumps(result.temporal_change)[:1400],
            "flowjury_agent_skeptic_review": json.dumps(result.skeptic_review)[:1400],
            "flowjury_agent_memory_id": str(result.memory_id or ""),
            "flowjury_agent_reviewed_at": reviewed_at,
        },
    )
    return True


def load_proposal_file(path: Path) -> list[AgentRecommendation]:
    """Validate and deserialize an existing FlowJury JSON result file."""
    path = Path(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"Cannot read proposal file {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Proposal file is not valid JSON: {exc}") from exc
    if not isinstance(payload, list) or not payload:
        raise ValueError("Proposal file must contain a non-empty JSON list")

    results = []
    for index, item in enumerate(payload, 1):
        if not isinstance(item, dict):
            raise ValueError(f"Proposal {index} must be a JSON object")
        missing = sorted(_REQUIRED_PROPOSAL_FIELDS - set(item))
        if missing:
            raise ValueError(f"Proposal {index} is missing: {', '.join(missing)}")
        if not isinstance(item["pipeline"], str) or not item["pipeline"].strip():
            raise ValueError(f"Proposal {index} has an invalid pipeline name")
        if item["recommendation"] not in _VALID_RECOMMENDATIONS:
            raise ValueError(f"Proposal {index} has an unsupported recommendation")
        if not isinstance(item["summary"], str) or not item["summary"].strip():
            raise ValueError(f"Proposal {index} summary must be non-empty")
        if not isinstance(item["next_action"], str) or not item["next_action"].strip():
            raise ValueError(f"Proposal {index} next_action must be non-empty")
        if not isinstance(item["evidence"], list) or not isinstance(item["risks"], list):
            raise ValueError(f"Proposal {index} evidence and risks must be lists")
        if not isinstance(item["skills_applied"], list):
            raise ValueError(f"Proposal {index} skills_applied must be a list")
        try:
            confidence = float(item["confidence"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Proposal {index} confidence must be numeric") from exc
        if not 0 <= confidence <= 1:
            raise ValueError(f"Proposal {index} confidence must be between 0 and 1")
        results.append(
            AgentRecommendation(
                pipeline=item["pipeline"].strip(),
                recommendation=str(item["recommendation"]),
                confidence=confidence,
                summary=str(item["summary"]),
                evidence=item["evidence"],
                risks=item["risks"],
                next_action=str(item["next_action"]),
                skills_applied=item["skills_applied"],
                investigation_status=str(item.get("investigation_status", "COMPLETED")),
                temporal_change=item.get("temporal_change"),
                skeptic_review=item.get("skeptic_review"),
                memory_id=item.get("memory_id"),
            )
        )
    return results


def write_existing_proposals(datahub: Any, emitter: Any, results) -> dict:
    """Resolve saved pipeline names and write their proposals without running the agent."""
    flows_by_name: dict[str, set[str]] = {}
    for job_urn in datahub.list_pipeline_jobs():
        flow_urn = datahub.flow_urn_of(job_urn)
        name = datahub.flow_info(flow_urn).get("name")
        if name:
            flows_by_name.setdefault(name.casefold(), set()).add(flow_urn)

    written = 0
    errors = []
    for result in results:
        blocked = writeback_block_reason(result)
        if blocked:
            errors.append(f"{result.pipeline}: {blocked}")
            continue
        matches = sorted(flows_by_name.get(result.pipeline.casefold(), set()))
        if not matches:
            errors.append(f"{result.pipeline}: no matching DataHub pipeline")
            continue
        if len(matches) > 1:
            errors.append(f"{result.pipeline}: ambiguous DataHub pipeline name")
            continue
        try:
            if write_agent_proposal(datahub.graph, emitter, matches[0], result):
                written += 1
            else:
                errors.append(f"{result.pipeline}: proposal was not written")
        except Exception as exc:
            errors.append(f"{result.pipeline}: {exc}")
    return {"written": written, "errors": errors}
