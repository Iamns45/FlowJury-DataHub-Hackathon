"""FlowJury's skill-first pipeline decision agent.

Python normalizes DataHub metadata into evidence but does not choose a verdict.
For every pipeline, the configured LLM discovers project-local business skills, loads the
ones relevant to the current evidence, gathers any missing context, and submits
a structured proposal. Proposals never disable a job; optional write-back stores
them in separate ``flowjury_agent_*`` properties for human review.

    pip install -r requirements.txt
    export DATAHUB_GMS_URL=http://localhost:8080
    export LLM_API_KEY=...
    export LLM_NAME=...
    python -m flowjury
    python -m flowjury --pipeline "Nightly Hightouch Sync" --json-out proposals.json
    python -m flowjury --writeback-proposals  # explicit opt-in; still no job changes
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Dict, List, Literal, Optional, Sequence, Tuple

from langgraph.graph import END, START, StateGraph

from datahub_agent_context.mcp_tools.lineage import get_lineage as ack_lineage
from datahub_agent_context.mcp_tools.search import search as ack_search

from flowjury.analysis.blast_radius import summarize_blast_radius
from flowjury.agent.context import InvestigationContext
from flowjury.agent.models import AgentRecommendation, InvestigationState, ToolObservation
from flowjury.agent.serialization import clip_tool_output as _short
from flowjury.agent.serialization import to_jsonable as _jsonable
from flowjury.agent.skills import SkillRegistry
from flowjury.integrations.llm.client import (
    LLM_NAME,
    LLM_TEMPERATURE,
)
from flowjury.memory.store import InvestigationMemory
from flowjury.settings import (
    DEFAULT_MAX_SUPERVISION_CYCLES,
    MAX_REPAIR_ATTEMPTS,
    RISKY_VERDICTS,
)


INVESTIGATION_TOOLS = [
    {
        "name": "list_business_skills",
        "description": (
            "List the names and trigger descriptions of available FlowJury business skills. "
            "The runtime normally supplies this once in bootstrap context."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "load_business_skill",
        "description": (
            "Load the full instructions for one business skill. Always load "
            "decide-pipeline-verdict plus every specialist skill relevant to the evidence."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"skill": {"type": "string"}},
            "required": ["skill"],
            "additionalProperties": False,
        },
    },
    {
        "name": "recall_investigation_memory",
        "description": (
            "Recall prior investigations for this pipeline plus related organizational episodes, "
            "and compare the prior evidence fingerprint with current DataHub evidence. The runtime "
            "normally supplies this once in bootstrap context. Historical verdicts are leads, "
            "not truth."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "pipeline": {"type": "string"},
                "query": {
                    "type": "string",
                    "description": "Optional concept such as Kafka, finance close, or privacy.",
                },
            },
            "required": ["pipeline"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_dag_source",
        "description": (
            "Read the pipeline's DAG or transform source. Use it to find external sinks, "
            "exports, API writes, schedules, and consumers missing from catalog metadata."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"pipeline": {"type": "string"}},
            "required": ["pipeline"],
            "additionalProperties": False,
        },
    },
    {
        "name": "search_datahub",
        "description": (
            "Search the DataHub context graph for related datasets, dashboards, pipelines, "
            "destinations, or business terms. Choose a focused query from current evidence."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": False,
        },
    },
    {
        "name": "trace_downstream",
        "description": (
            "Trace up to two downstream hops from every output dataset. This covers cataloged "
            "consumers but can still miss external systems."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"pipeline": {"type": "string"}},
            "required": ["pipeline"],
            "additionalProperties": False,
        },
    },
    {
        "name": "simulate_retirement",
        "description": (
            "Simulate stopping the pipeline and summarize its cataloged downstream blast radius, "
            "protected context, and executable-source signals for hidden external consumers. "
            "Required before KILL and recommended before REDUNDANT consolidation."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"pipeline": {"type": "string"}},
            "required": ["pipeline"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_peer_evidence",
        "description": (
            "Return normalized comparison facts for other pipelines in the same run. Use when "
            "checking redundancy, canonical alternatives, or relative consumer strength."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"pipeline": {"type": "string"}},
            "required": ["pipeline"],
            "additionalProperties": False,
        },
    },
]

SUBMIT_TOOL = {
    "name": "submit_recommendation",
    "description": (
        "Finish the investigation by submitting a structured proposal. This proposes a human "
        "review action; it never changes or stops the pipeline."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "recommendation": {
                "type": "string",
                "enum": [
                    "KEEP",
                    "KILL",
                    "DOWNSHIFT",
                    "REDUNDANT",
                    "TRIM",
                    "RUNAWAY",
                    "FIX/FOLD",
                    "PROTECT",
                    "UNKNOWN",
                ],
            },
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "summary": {"type": "string"},
            "evidence": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "properties": {
                        "claim": {"type": "string"},
                        "source_tool": {
                            "type": "string",
                            "enum": [
                                "datahub_evidence",
                                "load_business_skill",
                                "get_dag_source",
                                "search_datahub",
                                "trace_downstream",
                                "get_peer_evidence",
                                "recall_investigation_memory",
                                "simulate_retirement",
                                "skeptic_review",
                            ],
                        },
                        "observation": {"type": "string"},
                    },
                    "required": ["claim", "source_tool", "observation"],
                    "additionalProperties": False,
                },
            },
            "risks": {"type": "array", "items": {"type": "string"}},
            "next_action": {"type": "string"},
            "skills_applied": {
                "type": "array",
                "minItems": 2,
                "items": {"type": "string"},
            },
        },
        "required": [
            "recommendation",
            "confidence",
            "summary",
            "evidence",
            "risks",
            "next_action",
            "skills_applied",
        ],
        "additionalProperties": False,
    },
}

SKEPTIC_TOOL = {
    "name": "submit_skeptic_review",
    "description": (
        "Independently accept or block a risky FlowJury proposal. Block whenever current "
        "evidence does not rule out hidden consumers, semantic differences, or protected use."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "decision": {"type": "string", "enum": ["PASS", "BLOCK"]},
            "summary": {"type": "string"},
            "concerns": {"type": "array", "items": {"type": "string"}},
            "missing_evidence": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["decision", "summary", "concerns", "missing_evidence"],
        "additionalProperties": False,
    },
}

BOOTSTRAPPED_TOOL_NAMES = {
    "list_business_skills",
    "recall_investigation_memory",
}
SUPERVISOR_TOOLS = [
    tool for tool in INVESTIGATION_TOOLS if tool["name"] not in BOOTSTRAPPED_TOOL_NAMES
] + [SUBMIT_TOOL]

SUPERVISOR_SYSTEM = """You are FlowJury's investigation supervisor. Python has normalized
DataHub metadata into facts but has not assigned a verdict. On every supervision cycle, review
the current evidence and tool observations, then issue a concise batch of tool calls representing
the next steps. When the investigation is complete, call submit_recommendation instead of
requesting more tools.

Rules:
- Bootstrap context already contains the skill catalog, full decide-pipeline-verdict router, and
  compact memory recall when enabled. They are already audited observations; do not request them.
- Use the catalog summaries and router to select only the relevant specialist skill or skills.
  Load those skills and request the focused evidence they need in one batch.
- If bootstrap memory contains prior episodes, load use-investigation-memory and compare their
  evidence fingerprint with current facts. Prior verdicts are leads, not truth.
- Apply only loaded skill instructions and observed DataHub facts. Never invent evidence.
- Load multiple specialist skills when evidence could match competing policies.
- Do not submit a verdict in the same batch as unread evidence. First inspect the executor's
  results. Re-plan only for a failed lookup, a material contradiction, or a decision-critical gap;
  otherwise submit the proposal on the next supervision cycle.
- external_sink is one catalog fact, not a workflow switch. FALSE does not prove that a hidden
  sink is absent. Read source or lineage only when the candidate verdict or selected skill needs it.
- Treat DAG comments as weaker evidence than executable calls and catalog facts.
- Before KILL, inspect DAG source, trace downstream, search for a hidden consumer, simulate the
  retirement blast radius, and require human approval as the next action.
- Treat prior verdicts as investigation leads only. Current DataHub evidence always wins.
- KILL and REDUNDANT proposals receive a separate skeptic review after submission.
- Missing or failed evidence supports UNKNOWN, never KILL.
- Batch independent tool calls together to reduce supervision cycles.
- Keep progress text brief; do not expose private chain-of-thought.
- Finish only by calling submit_recommendation with evidence citations, skills_applied, and risks.

The submission is a proposal only. It cannot disable or edit a pipeline.
"""

REPAIR_SYSTEM = (
    "You are FlowJury's supervisor repairing only a proposal's structured fields; do not perform "
    "new investigation. Return exactly one submit_recommendation tool call. Use only the listed "
    "loaded_skills and allowed_evidence_sources. Every evidence item needs claim, source_tool, "
    "and observation. risks must be a JSON array, while summary and next_action must be non-empty "
    "strings. Do not invent facts, skills, or successful safety checks. If the supplied facts "
    "cannot support the original verdict, use UNKNOWN."
)

FINALIZE_SYSTEM = (
    "You are FlowJury's supervisor making one final decision from an already completed evidence "
    "packet. Do not request new tools. Return exactly one submit_recommendation tool call. "
    "Apply the loaded business skills and current DataHub evidence, preserve their precedence, "
    "and cite only allowed evidence sources. Do not choose UNKNOWN merely because the supervision "
    "budget ended: use it only when a decision-critical fact is genuinely missing, failed, or "
    "contradictory. Never invent facts or claim that the proposal changed a pipeline."
)


def dispatch(
    ctx: InvestigationContext,
    skills: SkillRegistry,
    memory: Optional[InvestigationMemory],
    current_evidence,
    name: str,
    inp: dict,
) -> str:
    """Execute one read-only investigation tool and return model-safe text."""
    try:
        if name == "list_business_skills":
            return _short(skills.summaries())
        if name == "load_business_skill":
            skill = skills.load(inp.get("skill", ""))
            return _short(
                {
                    "name": skill.name,
                    "description": skill.description,
                    "instructions": skill.instructions,
                }
            )
        if name == "recall_investigation_memory":
            if memory is None:
                return _short({"status": "DISABLED", "episodes": []})
            requested = inp.get("pipeline", "")
            if requested.casefold() != current_evidence.pipeline.casefold():
                return "(tool error: memory can only be recalled for the current pipeline)"
            return _short(
                memory.recall(
                    current_evidence,
                    query=inp.get("query", ""),
                    # Keep the compact payload valid JSON so output clipping cannot hide
                    # episode metadata from either the model or the safety validator.
                    limit=2,
                    context=ctx.memory_context(current_evidence.pipeline),
                )
            )
        if name == "get_dag_source":
            return _short(ctx.dag_source(inp.get("pipeline", "")))
        if name == "search_datahub":
            return _short(ack_search(query=inp.get("query", "*"), num_results=8))
        if name == "trace_downstream":
            pipeline = inp.get("pipeline", "")
            urns = ctx.output_urns(pipeline)
            if not urns:
                return "No output datasets found for this pipeline."
            results = []
            for urn in urns:
                try:
                    lineage = ack_lineage(urn=urn, upstream=False, max_hops=2, max_results=20)
                    results.append({"output_urn": urn, "downstream": _jsonable(lineage)})
                except Exception as exc:
                    results.append({"output_urn": urn, "error": str(exc)})
            return _short(results)
        if name == "simulate_retirement":
            pipeline = inp.get("pipeline", "")
            urns = ctx.output_urns(pipeline)
            if not urns:
                return "No output datasets found for this pipeline."
            lineage_by_output = {}
            lineage_errors = []
            for urn in urns:
                try:
                    lineage_by_output[urn] = ack_lineage(
                        urn=urn, upstream=False, max_hops=3, max_results=50
                    )
                except Exception as exc:
                    lineage_errors.append({"output_urn": urn, "error": str(exc)})
            report = summarize_blast_radius(
                urns,
                lineage_by_output,
                ctx.dag_source(pipeline),
                bool(current_evidence.protected),
            )
            report["lineage_errors"] = lineage_errors
            if lineage_errors and not report["retirement_blockers"]:
                report["conclusion"] = "INCONCLUSIVE_TOOL_ERROR"
            return _short(report)
        if name == "get_peer_evidence":
            return _short(ctx.peer_evidence(inp.get("pipeline", "")))
    except Exception as exc:
        return f"(tool error: {exc})"
    return f"Unknown tool: {name}"


def validate_submission(
    evidence, payload: dict, observations: Sequence[ToolObservation], memory_enabled: bool = False
) -> List[str]:
    """Validate process and safety without selecting the business verdict."""
    errors: List[str] = []
    recommendation = payload.get("recommendation")
    valid_verdicts = {
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
    if recommendation not in valid_verdicts:
        errors.append("recommendation is not a supported FlowJury verdict")

    confidence = payload.get("confidence")
    if (
        not isinstance(confidence, (int, float))
        or isinstance(confidence, bool)
        or not 0 <= confidence <= 1
    ):
        errors.append("confidence must be a number from 0 to 1")

    successful = {o.tool for o in observations if o.ok}
    if "list_business_skills" not in successful:
        errors.append("business skills were not discovered")
    loaded_skills = {o.argument for o in observations if o.ok and o.tool == "load_business_skill"}
    if "decide-pipeline-verdict" not in loaded_skills:
        errors.append("decide-pipeline-verdict must be loaded")
    if len(loaded_skills - {"decide-pipeline-verdict"}) < 1:
        errors.append("at least one specialist business skill must be loaded")
    if memory_enabled and "recall_investigation_memory" not in successful:
        errors.append("investigation memory must be recalled")

    recalled_episodes = False
    for observation in observations:
        if not observation.ok or observation.tool != "recall_investigation_memory":
            continue
        try:
            recalled_episodes = bool(json.loads(observation.output).get("episodes"))
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    if recalled_episodes and "use-investigation-memory" not in loaded_skills:
        errors.append("use-investigation-memory must be loaded when prior episodes exist")

    applied = payload.get("skills_applied")
    if not isinstance(applied, list) or len(applied) < 2:
        errors.append("skills_applied must contain the router and a specialist skill")
    else:
        unobserved = sorted(set(applied) - loaded_skills)
        if unobserved:
            errors.append("skills_applied contains unloaded skills: " + ", ".join(unobserved))
        if "decide-pipeline-verdict" not in applied:
            errors.append("skills_applied must include decide-pipeline-verdict")
        if recalled_episodes and "use-investigation-memory" not in applied:
            errors.append(
                "skills_applied must include use-investigation-memory when episodes exist"
            )

    if recommendation == "KILL":
        required = {
            "get_dag_source",
            "trace_downstream",
            "search_datahub",
            "simulate_retirement",
        }
        missing = sorted(required - successful)
        if missing:
            errors.append("KILL requires completed safety checks: " + ", ".join(missing))
        if evidence.external_sink or evidence.protected:
            errors.append("KILL is blocked by a protection or external-sink fact")
        for observation in observations:
            if not observation.ok or observation.tool != "simulate_retirement":
                continue
            try:
                report = json.loads(observation.output)
            except (TypeError, ValueError, json.JSONDecodeError):
                errors.append("KILL requires a parseable blast-radius report")
                continue
            if report.get("retirement_blockers"):
                errors.append("KILL is blocked by the retirement blast-radius report")

    if recommendation == "REDUNDANT":
        required = {"get_peer_evidence", "get_dag_source", "simulate_retirement"}
        missing = sorted(required - successful)
        if missing:
            errors.append("REDUNDANT requires comparison checks: " + ", ".join(missing))

    evidence = payload.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        errors.append("at least one evidence citation is required")
    else:
        allowed_sources = successful | {"datahub_evidence"}
        for index, item in enumerate(evidence, start=1):
            if not isinstance(item, dict):
                errors.append(f"evidence item {index} must be an object")
                continue
            if item.get("source_tool") not in allowed_sources:
                errors.append(
                    f"evidence item {index} cites an unobserved tool: {item.get('source_tool')}"
                )
            if not item.get("claim") or not item.get("observation"):
                errors.append(f"evidence item {index} needs a claim and observation")
    for field_name in ("summary", "next_action"):
        if not isinstance(payload.get(field_name), str) or not payload[field_name].strip():
            errors.append(f"{field_name} must be non-empty")
    if not isinstance(payload.get("risks"), list):
        errors.append("risks must be a list")
    return errors


def _recalled_episodes_exist(observations: Sequence[ToolObservation]) -> bool:
    for observation in observations:
        if not observation.ok or observation.tool != "recall_investigation_memory":
            continue
        try:
            if json.loads(observation.output).get("episodes"):
                return True
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
    return False


def normalize_submission_payload(
    payload: dict,
    observations: Sequence[ToolObservation],
) -> Tuple[dict, List[str]]:
    """Repair proposal container fields without selecting or changing its verdict.

    The LLM remains responsible for recommendation, confidence, and cited evidence. Python only
    makes reliably recoverable schema fields canonical so an otherwise supported decision is not
    discarded because a list was returned as text or an audit field was omitted.
    """
    normalized = dict(payload)
    changed: List[str] = []

    risks = normalized.get("risks")
    if isinstance(risks, str):
        normalized["risks"] = [risks.strip()] if risks.strip() else []
        changed.append("risks")
    elif not isinstance(risks, list):
        normalized["risks"] = []
        changed.append("risks")

    recommendation = normalized.get("recommendation")
    if not isinstance(normalized.get("summary"), str) or not normalized["summary"].strip():
        verdict = recommendation if isinstance(recommendation, str) else "review"
        normalized["summary"] = (
            f"The agent proposed {verdict} based on the cited current investigation evidence."
        )
        changed.append("summary")

    if not isinstance(normalized.get("next_action"), str) or not normalized["next_action"].strip():
        if recommendation in RISKY_VERDICTS:
            normalized["next_action"] = (
                "Obtain explicit pipeline-owner approval before any retirement or "
                "consolidation action."
            )
        else:
            normalized["next_action"] = (
                "Have the pipeline owner review the cited evidence before taking action."
            )
        changed.append("next_action")

    loaded_skills = list(
        dict.fromkeys(
            observation.argument
            for observation in observations
            if observation.ok and observation.tool == "load_business_skill"
        )
    )
    supplied = normalized.get("skills_applied")
    if isinstance(supplied, list):
        applied = list(
            dict.fromkeys(
                skill for skill in supplied if isinstance(skill, str) and skill in loaded_skills
            )
        )
    else:
        applied = []

    required_skills = []
    if "decide-pipeline-verdict" in loaded_skills:
        required_skills.append("decide-pipeline-verdict")
    if _recalled_episodes_exist(observations) and "use-investigation-memory" in loaded_skills:
        required_skills.append("use-investigation-memory")
    if not any(skill != "decide-pipeline-verdict" for skill in applied):
        required_skills.extend(
            skill for skill in loaded_skills if skill != "decide-pipeline-verdict"
        )
    selected_skills = set(applied) | set(required_skills)
    applied = [skill for skill in loaded_skills if skill in selected_skills]
    if applied != supplied:
        normalized["skills_applied"] = applied
        changed.append("skills_applied")

    return normalized, changed


_NON_REPAIRABLE_VALIDATION_PREFIXES = (
    "business skills were not discovered",
    "decide-pipeline-verdict must be loaded",
    "at least one specialist business skill must be loaded",
    "investigation memory must be recalled",
    "use-investigation-memory must be loaded",
    "KILL requires completed safety checks",
    "KILL requires a parseable blast-radius report",
    "KILL is blocked",
    "REDUNDANT requires comparison checks",
)


def submission_can_be_repaired(errors: Sequence[str]) -> bool:
    """Return True only for payload-shape errors, never missing safety work."""
    return bool(errors) and not any(
        error.startswith(prefix)
        for error in errors
        for prefix in _NON_REPAIRABLE_VALIDATION_PREFIXES
    )


def _argument(block) -> str:
    return str(
        block.input.get("pipeline") or block.input.get("query") or block.input.get("skill") or ""
    )


def _tool_succeeded(output: str, tool: str = "") -> bool:
    """Distinguish a completed empty lookup from a malformed or failed lookup."""
    failure_prefixes = (
        "(tool error:",
        "Unknown tool:",
        "No pipeline named",
        "Pipeline name '",
        "(no DAG source recorded)",
        "No output datasets found",
    )
    if output.startswith(failure_prefixes):
        return False
    if tool == "trace_downstream" and '"error"' in output:
        return False
    if tool == "simulate_retirement":
        try:
            return not bool(json.loads(output).get("lineage_errors"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
    return True


def bootstrap_investigation_context(
    ctx: InvestigationContext,
    skills: SkillRegistry,
    memory: Optional[InvestigationMemory],
    evidence,
) -> Tuple[dict, List[ToolObservation]]:
    """Load universal context once, before the supervisor starts reasoning.

    Skill selection remains agentic: bootstrap provides only the specialist summaries and the
    mandatory router's full policy. The supervisor still chooses which specialist instructions and
    expensive DataHub evidence to load for this pipeline.
    """
    summaries = skills.summaries()
    router = skills.load("decide-pipeline-verdict")
    router_payload = {
        "name": router.name,
        "description": router.description,
        "instructions": router.instructions,
    }
    observations = [
        ToolObservation("list_business_skills", "", _short(summaries), True),
        ToolObservation(
            "load_business_skill",
            router.name,
            _short(router_payload),
            True,
        ),
    ]

    memory_payload: dict = {
        "status": "DISABLED",
        "episodes": [],
        "safety_note": "Memory is disabled for this run.",
    }
    if memory is not None:
        try:
            memory_payload = memory.recall(
                evidence,
                limit=2,
                context=ctx.memory_context(evidence.pipeline),
            )
            output = _short(memory_payload)
            observations.append(
                ToolObservation(
                    "recall_investigation_memory",
                    evidence.pipeline,
                    output,
                    _tool_succeeded(output, "recall_investigation_memory"),
                )
            )
        except Exception as exc:
            output = f"(tool error: {exc})"
            memory_payload = {
                "status": "ERROR",
                "episodes": [],
                "error": str(exc),
            }
            observations.append(
                ToolObservation(
                    "recall_investigation_memory",
                    evidence.pipeline,
                    output,
                    False,
                )
            )

    return {
        "available_skill_summaries": summaries,
        "router_skill": router_payload,
        "memory": memory_payload,
        "reasoning_contract": (
            "Select only relevant specialist skills, investigate decision-critical gaps, "
            "then cite observed results. Do not treat prior verdicts as current proof."
        ),
    }, observations


def fallback_recommendation(
    evidence,
    observations: Sequence[ToolObservation],
    reason: str,
) -> AgentRecommendation:
    return AgentRecommendation(
        pipeline=evidence.pipeline,
        recommendation="UNKNOWN",
        confidence=0.2,
        summary="The skill-first investigation ended without a valid, supported proposal.",
        evidence=[
            {
                "claim": "Available DataHub evidence is insufficient for a safe verdict.",
                "source_tool": "datahub_evidence",
                "observation": reason,
            }
        ],
        risks=[reason, "Acting with incomplete evidence could interrupt an uncataloged consumer."],
        next_action="Ask the pipeline owner to review the DAG and consumer inventory.",
        skills_applied=[
            o.argument for o in observations if o.ok and o.tool == "load_business_skill"
        ],
        investigation_status="INCOMPLETE",
        investigation=list(observations),
    )


SKEPTIC_EVIDENCE_TOOLS = {
    "get_dag_source",
    "search_datahub",
    "trace_downstream",
    "get_peer_evidence",
    "simulate_retirement",
}


def build_skeptic_dossier(
    evidence,
    proposal: AgentRecommendation,
    observations: Sequence[ToolObservation],
) -> dict:
    """Build a compact, current-evidence packet without replaying the full investigation.

    The proposal already contains the supervisor's claims. The skeptic receives those claims, the
    normalized current facts, the names of applied policies, and only decision-critical tool
    results. Raw skill text, memory episodes, supervisor messages, and the proposal's duplicated
    investigation audit trail are intentionally excluded.
    """
    loaded_skills = list(
        dict.fromkeys(
            item.argument for item in observations if item.ok and item.tool == "load_business_skill"
        )
    )
    decision_evidence = []
    seen = set()
    for item in observations:
        if not item.ok or item.tool not in SKEPTIC_EVIDENCE_TOOLS:
            continue
        key = (item.tool, item.argument, item.output)
        if key in seen:
            continue
        seen.add(key)
        decision_evidence.append(
            {
                "tool": item.tool,
                "argument": item.argument,
                "output": item.output[:1800],
            }
        )

    failed_tools = [
        {
            "tool": item.tool,
            "argument": item.argument,
            "error": item.output[:600],
        }
        for item in observations
        if not item.ok and item.tool != "skeptic_review"
    ]
    successful_tools = {item.tool for item in observations if item.ok}

    return {
        "normalized_current_evidence": _jsonable(asdict(evidence)),
        "proposal": {
            "pipeline": proposal.pipeline,
            "recommendation": proposal.recommendation,
            "confidence": proposal.confidence,
            "summary": proposal.summary,
            "evidence": proposal.evidence,
            "risks": proposal.risks,
            "next_action": proposal.next_action,
            "skills_applied": proposal.skills_applied,
            "temporal_change": proposal.temporal_change,
        },
        "loaded_skills": loaded_skills,
        "safety_check_status": {
            name: name in successful_tools for name in sorted(SKEPTIC_EVIDENCE_TOOLS)
        },
        "decision_critical_tool_results": decision_evidence,
        "failed_tools": failed_tools,
        "memory_note": (
            "Raw memory is intentionally omitted: historical episodes are leads, not current "
            "proof. Any memory-derived claim must also be supported by current evidence."
        ),
    }


def apply_skeptic_review(
    proposal: AgentRecommendation,
    review: dict,
    observations: Sequence[ToolObservation],
) -> AgentRecommendation:
    """Preserve a PASS or safely downgrade a blocked risky proposal."""
    proposal.skeptic_review = review
    proposal.investigation = list(observations)
    if review.get("decision") == "PASS":
        return proposal

    original = proposal.recommendation
    return AgentRecommendation(
        pipeline=proposal.pipeline,
        recommendation="UNKNOWN",
        confidence=min(proposal.confidence, 0.35),
        summary=f"Skeptic blocked the proposed {original}: {review['summary']}",
        evidence=proposal.evidence
        + [
            {
                "claim": f"Independent review blocked the proposed {original} verdict.",
                "source_tool": "skeptic_review",
                "observation": review["summary"],
            }
        ],
        risks=list(
            dict.fromkeys(
                list(review.get("concerns", []))
                + list(review.get("missing_evidence", []))
                + proposal.risks
            )
        ),
        next_action=(
            "Resolve the skeptic's concerns and obtain owner approval before changing "
            "the pipeline."
        ),
        skills_applied=proposal.skills_applied,
        investigation=list(observations),
        temporal_change=proposal.temporal_change,
        skeptic_review=review,
    )


def build_investigation_graph(
    llm_client,
    ctx: InvestigationContext,
    skills: SkillRegistry,
    evidence,
    max_supervision_cycles: int,
    memory: Optional[InvestigationMemory] = None,
):
    """Compile the supervisor-executor loop with an optional skeptic branch."""

    def accepted_proposal(
        payload: dict, observations: Sequence[ToolObservation]
    ) -> AgentRecommendation:
        return AgentRecommendation(
            pipeline=evidence.pipeline,
            recommendation=payload["recommendation"],
            confidence=float(payload["confidence"]),
            summary=payload["summary"].strip(),
            evidence=list(payload["evidence"]),
            risks=list(payload["risks"]),
            next_action=payload["next_action"].strip(),
            skills_applied=list(payload["skills_applied"]),
            investigation=list(observations),
            temporal_change=(
                memory.compare(evidence, ctx.memory_context(evidence.pipeline)) if memory else None
            ),
        )

    def safe_fallback(state: InvestigationState, reason: str) -> AgentRecommendation:
        result = fallback_recommendation(evidence, state.get("observations", []), reason)
        result.temporal_change = (
            memory.compare(evidence, ctx.memory_context(evidence.pipeline)) if memory else None
        )
        return result

    def repair_packet(state: InvestigationState) -> dict:
        observations = list(state.get("observations", []))
        return {
            "pipeline": evidence.pipeline,
            "normalized_evidence": _jsonable(asdict(evidence)),
            "invalid_submission": state.get("candidate_payload") or {},
            "validation_errors": list(state.get("validation_errors", [])),
            "loaded_skills": [
                item.argument
                for item in observations
                if item.ok and item.tool == "load_business_skill"
            ],
            "allowed_evidence_sources": sorted(
                {item.tool for item in observations if item.ok} | {"datahub_evidence"}
            ),
            "observations": [
                {
                    "tool": item.tool,
                    "argument": item.argument,
                    "ok": item.ok,
                    "output": item.output[:1200],
                }
                for item in observations
            ],
        }

    def supervisor_node(state: InvestigationState) -> InvestigationState:
        """Choose the next tool batch or submit the final proposal."""
        validation_errors = list(state.get("validation_errors", []))
        repair_mode = submission_can_be_repaired(validation_errors)
        repair_attempts = state.get("repair_attempts", 0)
        supervision_cycles = state.get("supervision_cycles", 0)
        finalization_attempted = state.get("finalization_attempted", False)
        budget_finalize = not repair_mode and supervision_cycles >= max_supervision_cycles

        if repair_mode and repair_attempts >= MAX_REPAIR_ATTEMPTS:
            reason = (
                f"Proposal formatting remained invalid after {MAX_REPAIR_ATTEMPTS} "
                "supervisor repair attempts: " + "; ".join(validation_errors)
            )
            return {
                "result": safe_fallback(state, reason),
                "tool_blocks": [],
                "supervisor_actions": [],
                "repair_mode": False,
            }

        if budget_finalize and finalization_attempted:
            return {
                "result": safe_fallback(
                    state,
                    f"Supervision budget exhausted after {max_supervision_cycles} cycles and the "
                    "final evidence-only submission did not produce an acceptable proposal.",
                ),
                "tool_blocks": [],
                "supervisor_actions": [],
                "repair_mode": False,
            }

        try:
            if repair_mode:
                attempt = repair_attempts + 1
                print(f"  🛠 supervisor repairing proposal ({attempt}/{MAX_REPAIR_ATTEMPTS})")
                response = llm_client.messages.create(
                    model=LLM_NAME,
                    max_tokens=1200,
                    temperature=LLM_TEMPERATURE,
                    system=REPAIR_SYSTEM,
                    tools=[SUBMIT_TOOL],
                    tool_choice={"type": "tool", "name": "submit_recommendation"},
                    messages=[
                        {
                            "role": "user",
                            "content": json.dumps(repair_packet(state), default=str),
                        }
                    ],
                )
                next_cycles = supervision_cycles
                next_repair_attempts = attempt
                next_finalization_attempted = finalization_attempted
            elif budget_finalize:
                print("  🧭 supervision budget reached; forcing final evidence-based proposal")
                response = llm_client.messages.create(
                    model=LLM_NAME,
                    max_tokens=1200,
                    temperature=LLM_TEMPERATURE,
                    system=FINALIZE_SYSTEM,
                    tools=[SUBMIT_TOOL],
                    tool_choice={"type": "tool", "name": "submit_recommendation"},
                    messages=[
                        {
                            "role": "user",
                            "content": json.dumps(repair_packet(state), default=str),
                        }
                    ],
                )
                next_cycles = supervision_cycles
                next_repair_attempts = repair_attempts
                next_finalization_attempted = True
            else:
                next_cycles = supervision_cycles + 1
                next_repair_attempts = repair_attempts
                next_finalization_attempted = finalization_attempted
                response = llm_client.messages.create(
                    model=LLM_NAME,
                    max_tokens=1600,
                    temperature=LLM_TEMPERATURE,
                    system=SUPERVISOR_SYSTEM,
                    tools=SUPERVISOR_TOOLS,
                    messages=state["messages"],
                )
        except Exception as exc:
            if repair_mode:
                error = f"proposal repair call failed: {exc}"
                return {
                    "validation_errors": [error],
                    "repair_attempts": repair_attempts + 1,
                    "repair_mode": True,
                    "tool_blocks": [],
                    "supervisor_actions": [],
                    "result": None,
                }
            if budget_finalize:
                return {
                    "result": safe_fallback(
                        state,
                        f"Final evidence-only proposal call failed after "
                        f"{max_supervision_cycles} supervision cycles: {exc}",
                    ),
                    "tool_blocks": [],
                    "supervisor_actions": [],
                    "repair_mode": False,
                    "finalization_attempted": True,
                }
            return {
                "result": safe_fallback(state, f"Supervisor call failed: {exc}"),
                "tool_blocks": [],
                "supervisor_actions": [],
                "repair_mode": False,
            }

        messages = list(state["messages"])
        messages.append({"role": "assistant", "content": response.content})
        tool_blocks = [block for block in response.content if block.type == "tool_use"]
        supervisor_actions = [
            {"tool": block.name, "arguments": dict(block.input)} for block in tool_blocks
        ]

        for block in response.content:
            if block.type == "text" and block.text.strip():
                print(f"  🧠 {block.text.strip()}")

        if supervisor_actions:
            names = ", ".join(action["tool"] for action in supervisor_actions)
            label = (
                "repair"
                if repair_mode
                else "finalize" if budget_finalize else f"supervise {next_cycles}"
            )
            print(f"  🧭 {label}: {names}")
        else:
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "Return the next investigation steps as tool calls, or finish with "
                        "submit_recommendation."
                    ),
                }
            )

        return {
            "messages": messages,
            "supervision_cycles": next_cycles,
            "supervisor_actions": supervisor_actions,
            "tool_blocks": tool_blocks,
            "repair_attempts": next_repair_attempts,
            "repair_mode": repair_mode,
            "finalization_attempted": next_finalization_attempted,
            "result": None,
        }

    def route_after_supervisor(
        state: InvestigationState,
    ) -> Literal["supervisor", "executor", "end"]:
        if state.get("result") is not None:
            return "end"
        if state.get("tool_blocks"):
            return "executor"
        return "supervisor"

    def executor_node(state: InvestigationState) -> InvestigationState:
        """Execute the supervisor's tool batch and validate any submitted proposal."""
        observations = list(state.get("observations", []))
        tool_results: Dict[str, str] = {}
        accepted: Optional[AgentRecommendation] = None
        candidate_payload: Optional[dict] = None
        validation_errors: List[str] = []
        submission_attempted = False

        blocks = list(state.get("tool_blocks", []))
        evidence_requested = any(block.name != "submit_recommendation" for block in blocks)
        for block in blocks:
            if block.name == "submit_recommendation":
                continue
            argument = _argument(block)
            cycle = state.get("supervision_cycles", 0)
            print(f"  🔍 execute cycle {cycle}: {block.name}({argument!r})")
            output = dispatch(ctx, skills, memory, evidence, block.name, block.input)
            observations.append(
                ToolObservation(
                    block.name,
                    argument,
                    output,
                    _tool_succeeded(output, block.name),
                )
            )
            tool_results[block.id] = output

        for block in blocks:
            if block.name != "submit_recommendation":
                continue
            if evidence_requested:
                tool_results[block.id] = (
                    "Submission deferred: read the evidence returned by this executor cycle, "
                    "then submit a supported recommendation in the next supervision cycle."
                )
                print("  ↩ proposal deferred until the supervisor reads new evidence")
                continue
            submission_attempted = True
            candidate_payload, normalized_fields = normalize_submission_payload(
                dict(block.input), observations
            )
            if normalized_fields:
                print("  🧱 normalized proposal fields: " + ", ".join(normalized_fields))
            errors = validate_submission(
                evidence,
                candidate_payload,
                observations,
                memory_enabled=memory is not None,
            )
            if errors:
                validation_errors = errors
                tool_results[block.id] = (
                    "Submission rejected by process safety gates: "
                    + "; ".join(errors)
                    + ". Revise the plan or proposal."
                )
                print(f"  ⚠ submission rejected: {'; '.join(errors)}")
            else:
                accepted = accepted_proposal(candidate_payload, observations)
                tool_results[block.id] = "Recommendation accepted."

        messages = list(state["messages"])
        messages.append(
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": tool_results.get(block.id, "Tool call was not executed."),
                    }
                    for block in state.get("tool_blocks", [])
                ],
            }
        )

        if accepted is not None:
            if state.get("repair_mode"):
                observations.append(
                    ToolObservation(
                        "supervisor_repair",
                        evidence.pipeline,
                        json.dumps(candidate_payload, sort_keys=True, default=str),
                        True,
                    )
                )
                accepted.investigation = list(observations)
                print("  ✓ supervisor repair accepted")
            return {
                "messages": messages,
                "observations": observations,
                "result": accepted,
                "candidate_payload": None,
                "validation_errors": [],
                "repair_mode": False,
            }

        if submission_attempted:
            if state.get("repair_mode"):
                observations.append(
                    ToolObservation(
                        "supervisor_repair",
                        evidence.pipeline,
                        json.dumps(candidate_payload or {}, sort_keys=True, default=str),
                        False,
                    )
                )
            return {
                "messages": messages,
                "observations": observations,
                "result": None,
                "candidate_payload": candidate_payload,
                "validation_errors": validation_errors,
                "repair_attempts": (
                    state.get("repair_attempts", 0) if state.get("repair_mode") else 0
                ),
                "repair_mode": False,
            }

        return {
            "messages": messages,
            "observations": observations,
            "result": None,
            "candidate_payload": None,
            "validation_errors": [],
            "repair_attempts": 0,
            "repair_mode": False,
        }

    def route_after_executor(
        state: InvestigationState,
    ) -> Literal["supervisor", "skeptic", "end"]:
        result = state.get("result")
        if result is None:
            return "supervisor"
        return "skeptic" if result.recommendation in RISKY_VERDICTS else "end"

    def skeptic_review_node(state: InvestigationState) -> InvestigationState:
        proposal = state.get("result")
        if proposal is None:
            return {}
        packet = build_skeptic_dossier(
            evidence,
            proposal,
            state.get("observations", []),
        )
        review = {
            "decision": "BLOCK",
            "summary": "The skeptic did not return a valid structured review.",
            "concerns": ["Risky proposals fail closed when independent review is unavailable."],
            "missing_evidence": ["valid skeptic review"],
        }
        review_ok = False
        try:
            response = llm_client.messages.create(
                model=LLM_NAME,
                max_tokens=700,
                temperature=LLM_TEMPERATURE,
                system=(
                    "You are FlowJury's independent skeptic. Try to falsify the proposed "
                    "KILL or REDUNDANT verdict using only the supplied compact decision dossier. "
                    "Do not repeat the supervisor's investigation or request tools. Prior "
                    "memory is not current proof. BLOCK if a hidden "
                    "consumer, protection obligation, semantic difference, failed lookup, or "
                    "missing required fact remains plausible. Do not provide chain-of-thought; "
                    "submit only the structured review."
                ),
                tools=[SKEPTIC_TOOL],
                tool_choice={"type": "tool", "name": "submit_skeptic_review"},
                messages=[{"role": "user", "content": json.dumps(packet, default=str)}],
            )
            for block in response.content:
                if block.type == "tool_use" and block.name == "submit_skeptic_review":
                    candidate = dict(block.input)
                    if (
                        candidate.get("decision") in {"PASS", "BLOCK"}
                        and isinstance(candidate.get("summary"), str)
                        and candidate["summary"].strip()
                        and isinstance(candidate.get("concerns"), list)
                        and isinstance(candidate.get("missing_evidence"), list)
                    ):
                        review = {
                            "decision": candidate["decision"],
                            "summary": candidate["summary"].strip(),
                            "concerns": candidate["concerns"],
                            "missing_evidence": candidate["missing_evidence"],
                        }
                        review_ok = True
                        break
        except Exception as exc:
            review["summary"] = f"Skeptic review failed closed: {exc}"

        observation = ToolObservation(
            "skeptic_review",
            evidence.pipeline,
            json.dumps(review, sort_keys=True),
            review_ok,
        )
        observations = list(state.get("observations", [])) + [observation]
        proposal = apply_skeptic_review(proposal, review, observations)
        return {
            "result": proposal,
            "observations": observations,
            "skeptic_review": review,
        }

    builder = StateGraph(InvestigationState)
    builder.add_node("supervisor", supervisor_node)
    builder.add_node("executor", executor_node)
    builder.add_node("skeptic_review", skeptic_review_node)
    builder.add_edge(START, "supervisor")
    builder.add_conditional_edges(
        "supervisor",
        route_after_supervisor,
        {"supervisor": "supervisor", "executor": "executor", "end": END},
    )
    builder.add_conditional_edges(
        "executor",
        route_after_executor,
        {"supervisor": "supervisor", "skeptic": "skeptic_review", "end": END},
    )
    builder.add_edge("skeptic_review", END)
    return builder.compile()


def investigate(
    llm_client,
    ctx: InvestigationContext,
    skills: SkillRegistry,
    evidence,
    max_supervision_cycles: int = DEFAULT_MAX_SUPERVISION_CYCLES,
    memory: Optional[InvestigationMemory] = None,
) -> AgentRecommendation:
    """Run one supervisor-executor investigation and persist its completed episode."""
    facts = _jsonable(asdict(evidence))
    bootstrap, bootstrap_observations = bootstrap_investigation_context(
        ctx,
        skills,
        memory,
        evidence,
    )
    initial: InvestigationState = {
        "messages": [
            {
                "role": "user",
                "content": (
                    f"Pipeline: {evidence.pipeline}\n"
                    f"Normalized DataHub evidence: {json.dumps(facts, sort_keys=True)}\n"
                    f"Bootstrap context: {json.dumps(bootstrap, sort_keys=True)}\n\n"
                    "Reason over the supplied context once. Select only the specialist "
                    "policies and decision-critical evidence that are still missing, then "
                    "finish with one reviewable recommendation."
                ),
            }
        ],
        "observations": bootstrap_observations,
        "supervision_cycles": 0,
        "supervisor_actions": [],
        "tool_blocks": [],
        "result": None,
        "candidate_payload": None,
        "validation_errors": [],
        "repair_attempts": 0,
        "repair_mode": False,
        "finalization_attempted": False,
        "skeptic_review": None,
    }
    graph = build_investigation_graph(
        llm_client, ctx, skills, evidence, max_supervision_cycles, memory=memory
    )
    final_state = graph.invoke(initial, {"recursion_limit": max_supervision_cycles * 4 + 20})
    observations = list(final_state.get("observations", []))
    result = final_state.get("result") or fallback_recommendation(
        evidence,
        observations,
        "LangGraph completed without a recommendation.",
    )
    result.investigation = observations
    if result.temporal_change is None and memory is not None:
        result.temporal_change = memory.compare(evidence, ctx.memory_context(evidence.pipeline))

    if memory is not None and result.investigation_status == "COMPLETED":
        try:
            memory_id = memory.record(
                result,
                evidence,
                observations,
                context=ctx.memory_context(evidence.pipeline),
            )
            result.memory_id = memory_id
            print(f"  🧠 remembered investigation {memory_id[:8]}")
        except Exception as exc:
            print(f"  ⚠ memory write failed: {exc}")
    elif memory is not None:
        print("  ℹ incomplete investigation was not written to durable memory")

    return result
