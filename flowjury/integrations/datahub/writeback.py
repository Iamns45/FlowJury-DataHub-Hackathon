"""Write FlowJury review metadata back to DataHub without changing jobs.

After FlowJury decides, this adapter stores the review as separate DataHub tags
and custom properties so the next person or agent can inherit it.

Everything is provenance-stamped ('FlowJury') and reversible — it only ADDS
tags/properties, never deletes or overwrites your existing metadata.

"""

import json
import time

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    GlobalTagsClass,
    TagAssociationClass,
)


def merge_tags(graph, emitter, urn: str, tag_names) -> None:
    """Add tags without dropping any that already exist."""
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
    info = graph.get_aspect(flow_urn, DataFlowInfoClass)
    if info is None:
        return
    cp = dict(info.customProperties or {})
    cp.update(props)
    info.customProperties = cp
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=flow_urn, aspect=info))


def write_agent_proposal(graph, emitter, flow_urn: str, result) -> bool:
    """Persist one agent proposal without mutating the pipeline or its schedule."""
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
