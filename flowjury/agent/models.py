"""State and result models for the LangGraph investigation runtime."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, List, Optional, TypedDict


@dataclass
class ToolObservation:
    """Auditable result from one agent-selected investigation tool."""

    tool: str
    argument: str
    output: str
    ok: bool

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class AgentRecommendation:
    """Final review proposal produced for one DataHub pipeline."""

    pipeline: str
    recommendation: str
    confidence: float
    summary: str
    evidence: List[dict]
    risks: List[str]
    next_action: str
    skills_applied: List[str]
    investigation: List[ToolObservation] = field(default_factory=list)
    temporal_change: Optional[dict] = None
    skeptic_review: Optional[dict] = None
    memory_id: Optional[str] = None

    def as_dict(self) -> dict:
        data = asdict(self)
        data["investigation"] = [item.as_dict() for item in self.investigation]
        return data


class InvestigationState(TypedDict, total=False):
    """Minimal state shared by the planner, executor, and skeptic nodes."""

    messages: List[dict]
    observations: List[ToolObservation]
    planning_cycles: int
    planned_actions: List[dict]
    tool_blocks: List[Any]
    result: Optional[AgentRecommendation]
    candidate_payload: Optional[dict]
    validation_errors: List[str]
    repair_attempts: int
    repair_mode: bool
    finalization_attempted: bool
    skeptic_review: Optional[dict]
