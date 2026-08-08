import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[2]


def stub(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


class FakeStateGraph:
    def __init__(self, state_type):
        self.state_type = state_type
        self.nodes = set()
        self.functions = {}
        self.edges = []

    def add_node(self, name, function):
        self.nodes.add(name)
        self.functions[name] = function

    def add_edge(self, source, target):
        self.edges.append((source, target))

    def add_conditional_edges(self, source, router, mapping):
        self.edges.extend((source, target) for target in mapping.values())

    def compile(self):
        return self


stub("anthropic", Anthropic=object)
stub("langgraph")
stub("langgraph.graph", END="END", START="START", StateGraph=FakeStateGraph)
stub("datahub")
stub("datahub.metadata")
stub("datahub.metadata.schema_classes", DataFlowInfoClass=type("DataFlowInfoClass", (), {}))
stub("datahub.sdk")
stub("datahub.sdk.main_client", DataHubClient=type("DataHubClient", (), {}))
stub("datahub_agent_context")
stub("datahub_agent_context.context", set_client=lambda client: None)
stub("datahub_agent_context.mcp_tools")
stub("datahub_agent_context.mcp_tools.lineage", get_lineage=lambda **kwargs: {})
stub("datahub_agent_context.mcp_tools.search", search=lambda **kwargs: {})
stub(
    "flowjury.integrations.datahub.client",
    FlowJuryClient=type("FlowJuryClient", (), {}),
)

spec = importlib.util.spec_from_file_location(
    "flowjury_agent_under_test",
    ROOT / "flowjury" / "agent" / "runtime.py",
)
agent = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = agent
spec.loader.exec_module(agent)


def payload(recommendation="KEEP"):
    return {
        "recommendation": recommendation,
        "confidence": 0.8,
        "summary": "Supported proposal.",
        "evidence": [
            {
                "claim": "Normalized evidence was inspected.",
                "source_tool": "datahub_evidence",
                "observation": "Usage and lineage facts were supplied.",
            }
        ],
        "risks": ["Catalog coverage may be incomplete."],
        "next_action": "Ask the owner to approve.",
        "skills_applied": ["decide-pipeline-verdict", "assess-healthy-pipelines"],
    }


BASE_OBSERVATIONS = [
    agent.ToolObservation("list_business_skills", "", "skills", True),
    agent.ToolObservation("load_business_skill", "decide-pipeline-verdict", "router", True),
    agent.ToolObservation("load_business_skill", "assess-healthy-pipelines", "healthy", True),
]


def test_keep_submission_passes_skill_process_gates():
    evidence = SimpleNamespace(external_sink=False, protected=False)
    assert agent.validate_submission(evidence, payload("KEEP"), BASE_OBSERVATIONS) == []


def test_kill_requires_three_investigations():
    evidence = SimpleNamespace(external_sink=False, protected=False)
    errors = agent.validate_submission(evidence, payload("KILL"), BASE_OBSERVATIONS)
    assert any("safety checks" in error for error in errors)


def test_kill_is_blocked_for_external_or_protected_pipeline():
    observations = BASE_OBSERVATIONS + [
        agent.ToolObservation("get_dag_source", "pipeline", "source", True),
        agent.ToolObservation("trace_downstream", "pipeline", "[]", True),
        agent.ToolObservation("search_datahub", "pipeline", "[]", True),
    ]
    evidence = SimpleNamespace(external_sink=True, protected=False)
    errors = agent.validate_submission(evidence, payload("KILL"), observations)
    assert any("protection or external-sink" in error for error in errors)


def test_langgraph_contains_supervisor_executor_and_skeptic_only():
    evidence = SimpleNamespace(pipeline="test")
    graph = agent.build_investigation_graph(None, None, None, evidence, 8)
    assert graph.nodes == {"supervisor", "executor", "skeptic_review"}
    assert ("START", "supervisor") in graph.edges
    assert ("supervisor", "supervisor") in graph.edges
    assert ("supervisor", "executor") in graph.edges
    assert ("executor", "supervisor") in graph.edges
    assert ("executor", "skeptic_review") in graph.edges
    assert ("supervisor", "END") in graph.edges
    assert ("executor", "END") in graph.edges
    assert ("skeptic_review", "END") in graph.edges


def test_supervisor_cannot_re_request_bootstrapped_context():
    names = {tool["name"] for tool in agent.SUPERVISOR_TOOLS}
    assert "list_business_skills" not in names
    assert "recall_investigation_memory" not in names
    assert "load_business_skill" in names
    assert "submit_recommendation" in names


def test_bootstrap_supplies_catalog_router_and_memory_once():
    class FakeContext:
        @staticmethod
        def memory_context(name):
            return {"pipeline": name}

    class FakeMemory:
        def __init__(self):
            self.calls = 0

        def recall(self, evidence, limit, context):
            self.calls += 1
            return {
                "temporal_comparison": {"status": "UNCHANGED"},
                "episodes": [{"verdict": "KEEP"}],
                "safety_note": "History is not proof.",
            }

    evidence = SimpleNamespace(pipeline="test pipeline")
    skills = agent.SkillRegistry(ROOT / "skills")
    memory = FakeMemory()

    context, observations = agent.bootstrap_investigation_context(
        FakeContext(), skills, memory, evidence
    )

    assert memory.calls == 1
    assert context["router_skill"]["name"] == "decide-pipeline-verdict"
    assert context["available_skill_summaries"]
    assert context["memory"]["episodes"]
    assert [item.tool for item in observations] == [
        "list_business_skills",
        "load_business_skill",
        "recall_investigation_memory",
    ]


def test_memory_is_required_only_when_enabled():
    evidence = SimpleNamespace(external_sink=False, protected=False)
    errors = agent.validate_submission(
        evidence, payload("KEEP"), BASE_OBSERVATIONS, memory_enabled=True
    )
    assert "investigation memory must be recalled" in errors


def test_prior_episodes_require_memory_skill():
    evidence = SimpleNamespace(external_sink=False, protected=False)
    observations = BASE_OBSERVATIONS + [
        agent.ToolObservation(
            "recall_investigation_memory",
            "pipeline",
            '{"episodes": [{"verdict": "KEEP"}]}',
            True,
        )
    ]
    errors = agent.validate_submission(evidence, payload("KEEP"), observations, memory_enabled=True)
    assert "use-investigation-memory must be loaded when prior episodes exist" in errors


def test_proposal_normalizer_preserves_decisive_verdict_and_evidence():
    observations = [
        agent.ToolObservation("list_business_skills", "", "skills", True),
        agent.ToolObservation("load_business_skill", "decide-pipeline-verdict", "router", True),
        agent.ToolObservation("load_business_skill", "diagnose-pipeline-runs", "runs", True),
        agent.ToolObservation(
            "recall_investigation_memory",
            "Realtime Events Aggregation",
            '{"episodes": [{"verdict": "RUNAWAY"}]}',
            True,
        ),
        agent.ToolObservation("load_business_skill", "use-investigation-memory", "memory", True),
    ]
    malformed = payload("RUNAWAY")
    malformed.update(
        {
            "summary": "",
            "risks": "A legitimate long-running backfill could be interrupted.",
            "next_action": "",
            "skills_applied": [],
        }
    )

    normalized, changed = agent.normalize_submission_payload(malformed, observations)

    assert normalized["recommendation"] == malformed["recommendation"]
    assert normalized["confidence"] == malformed["confidence"]
    assert normalized["evidence"] == malformed["evidence"]
    assert normalized["risks"] == ["A legitimate long-running backfill could be interrupted."]
    assert normalized["skills_applied"] == [
        "decide-pipeline-verdict",
        "diagnose-pipeline-runs",
        "use-investigation-memory",
    ]
    assert set(changed) == {"summary", "risks", "next_action", "skills_applied"}
    evidence = SimpleNamespace(external_sink=False, protected=False)
    assert agent.validate_submission(evidence, normalized, observations, memory_enabled=True) == []


def test_blast_radius_blocks_kill_when_known_impact_exists():
    observations = BASE_OBSERVATIONS + [
        agent.ToolObservation("get_dag_source", "pipeline", "source", True),
        agent.ToolObservation("trace_downstream", "pipeline", "[]", True),
        agent.ToolObservation("search_datahub", "pipeline", "[]", True),
        agent.ToolObservation(
            "simulate_retirement",
            "pipeline",
            '{"retirement_blockers": ["1 cataloged downstream asset"]}',
            True,
        ),
    ]
    evidence = SimpleNamespace(external_sink=False, protected=False)
    errors = agent.validate_submission(evidence, payload("KILL"), observations)
    assert "KILL is blocked by the retirement blast-radius report" in errors


def test_skeptic_block_downgrades_risky_proposal_to_unknown():
    proposal = agent.AgentRecommendation(
        pipeline="legacy revenue",
        recommendation="REDUNDANT",
        confidence=0.8,
        summary="Looks duplicated.",
        evidence=payload()["evidence"],
        risks=[],
        next_action="Consolidate it.",
        skills_applied=payload()["skills_applied"],
    )
    review = {
        "decision": "BLOCK",
        "summary": "Tax semantics were not compared.",
        "concerns": ["Similar schemas may have different business meaning."],
        "missing_evidence": ["source comparison"],
    }

    result = agent.apply_skeptic_review(proposal, review, BASE_OBSERVATIONS)

    assert result.recommendation == "UNKNOWN"
    assert result.confidence <= 0.35
    assert result.skeptic_review == review


def test_fallback_is_incomplete_and_not_a_completed_business_verdict():
    evidence = SimpleNamespace(pipeline="failed transport")

    result = agent.fallback_recommendation(evidence, [], "Supervisor call failed")

    assert result.recommendation == "UNKNOWN"
    assert result.investigation_status == "INCOMPLETE"


def test_skeptic_dossier_omits_raw_memory_and_duplicate_audit_trail():
    @dataclass
    class FakeEvidence:
        pipeline: str = "legacy revenue"
        external_sink: bool = False
        protected: bool = False

    observations = BASE_OBSERVATIONS + [
        agent.ToolObservation(
            "recall_investigation_memory",
            "legacy revenue",
            '{"episodes": [{"summary": "RAW-MEMORY-MARKER"}]}',
            True,
        ),
        agent.ToolObservation(
            "get_dag_source",
            "legacy revenue",
            "delete_orphan_table()",
            True,
        ),
    ]
    proposal = agent.AgentRecommendation(
        pipeline="legacy revenue",
        recommendation="KILL",
        confidence=0.8,
        summary="No current consumer was found.",
        evidence=payload()["evidence"],
        risks=[],
        next_action="Request human approval.",
        skills_applied=payload()["skills_applied"],
        investigation=observations,
    )

    dossier = agent.build_skeptic_dossier(FakeEvidence(), proposal, observations)
    serialized = str(dossier)

    assert "investigation" not in dossier["proposal"]
    assert "RAW-MEMORY-MARKER" not in serialized
    assert "delete_orphan_table()" in serialized
    assert dossier["loaded_skills"] == [
        "decide-pipeline-verdict",
        "assess-healthy-pipelines",
    ]


def test_failed_or_partial_safety_tools_do_not_count_as_success():
    assert not agent._tool_succeeded("(no DAG source recorded)", "get_dag_source")
    assert not agent._tool_succeeded(
        '[{"output_urn": "x", "error": "lineage unavailable"}]',
        "trace_downstream",
    )
    assert not agent._tool_succeeded(
        '{"lineage_errors": [{"error": "timeout"}]}',
        "simulate_retirement",
    )


def test_structural_submission_errors_can_use_repair_node():
    assert agent.submission_can_be_repaired(
        [
            "at least one evidence citation is required",
            "summary must be non-empty",
            "risks must be a list",
        ]
    )


def test_missing_investigation_cannot_be_format_repaired():
    assert not agent.submission_can_be_repaired(
        ["KILL requires completed safety checks: search_datahub, trace_downstream"]
    )
    assert not agent.submission_can_be_repaired(
        ["use-investigation-memory must be loaded when prior episodes exist"]
    )


def test_supervisor_handles_structured_repair_without_an_extra_graph_node():
    @dataclass
    class FakeEvidence:
        pipeline: str = "test pipeline"
        external_sink: bool = False
        protected: bool = False

    class FakeMessages:
        def __init__(self):
            self.calls = []

        def create(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(
                content=[
                    SimpleNamespace(
                        type="tool_use",
                        name="submit_recommendation",
                        input=payload("KEEP"),
                        id="repair-1",
                    )
                ]
            )

    messages = FakeMessages()
    llm_client = SimpleNamespace(messages=messages)
    graph = agent.build_investigation_graph(
        llm_client, None, None, FakeEvidence(), max_supervision_cycles=8
    )
    state = {
        "messages": [{"role": "user", "content": "repair the proposal"}],
        "observations": BASE_OBSERVATIONS,
        "supervision_cycles": 3,
        "candidate_payload": {"recommendation": "KEEP", "confidence": 0.8},
        "validation_errors": ["summary must be non-empty"],
        "repair_attempts": 0,
        "repair_mode": False,
    }
    supervised = graph.functions["supervisor"](state)
    repaired = graph.functions["executor"]({**state, **supervised})

    assert repaired["result"].recommendation == "KEEP"
    assert repaired["validation_errors"] == []
    assert supervised["repair_attempts"] == 1
    assert supervised["repair_mode"] is True
    assert messages.calls[0]["tools"] == [agent.SUBMIT_TOOL]
    assert messages.calls[0]["tool_choice"] == {"type": "tool", "name": "submit_recommendation"}
    assert messages.calls[0]["temperature"] == 0


def test_budget_limit_forces_one_final_proposal_before_unknown():
    @dataclass
    class FakeEvidence:
        pipeline: str = "Quarterly Compliance Close"
        external_sink: bool = False
        protected: bool = True

    class FakeMessages:
        def __init__(self):
            self.calls = []

        def create(self, **kwargs):
            self.calls.append(kwargs)
            final = payload("PROTECT")
            final["skills_applied"] = ["decide-pipeline-verdict", "protect-regulated-pipelines"]
            return SimpleNamespace(
                content=[
                    SimpleNamespace(
                        type="tool_use",
                        name="submit_recommendation",
                        input=final,
                        id="finalize-1",
                    )
                ]
            )

    observations = [
        agent.ToolObservation("list_business_skills", "", "skills", True),
        agent.ToolObservation("load_business_skill", "decide-pipeline-verdict", "router", True),
        agent.ToolObservation(
            "load_business_skill", "protect-regulated-pipelines", "protect", True
        ),
    ]
    messages = FakeMessages()
    graph = agent.build_investigation_graph(
        SimpleNamespace(messages=messages),
        None,
        None,
        FakeEvidence(),
        max_supervision_cycles=1,
    )
    state = {
        "messages": [{"role": "user", "content": "finalize from current evidence"}],
        "observations": observations,
        "supervision_cycles": 1,
        "validation_errors": [],
        "repair_attempts": 0,
        "repair_mode": False,
        "finalization_attempted": False,
    }

    supervised = graph.functions["supervisor"](state)
    executed = graph.functions["executor"]({**state, **supervised})

    assert supervised["finalization_attempted"] is True
    assert messages.calls[0]["system"] == agent.FINALIZE_SYSTEM
    assert messages.calls[0]["tool_choice"] == {"type": "tool", "name": "submit_recommendation"}
    assert messages.calls[0]["temperature"] == 0
    assert executed["result"].recommendation == "PROTECT"


def test_executor_accepts_structurally_incomplete_runaway_proposal():
    @dataclass
    class FakeEvidence:
        pipeline: str = "Realtime Events Aggregation"
        external_sink: bool = False
        protected: bool = False

    observations = [
        agent.ToolObservation("list_business_skills", "", "skills", True),
        agent.ToolObservation("load_business_skill", "decide-pipeline-verdict", "router", True),
        agent.ToolObservation("load_business_skill", "diagnose-pipeline-runs", "runs", True),
    ]
    malformed = payload("RUNAWAY")
    malformed.update(
        {
            "summary": "",
            "risks": "Runtime spike may be a legitimate backfill.",
            "next_action": "",
            "skills_applied": [],
        }
    )
    graph = agent.build_investigation_graph(
        None, None, None, FakeEvidence(), max_supervision_cycles=8
    )
    state = {
        "messages": [{"role": "user", "content": "submit"}],
        "observations": observations,
        "supervision_cycles": 2,
        "tool_blocks": [
            SimpleNamespace(
                type="tool_use",
                name="submit_recommendation",
                input=malformed,
                id="submit-runaway",
            )
        ],
        "repair_mode": False,
    }

    executed = graph.functions["executor"](state)

    assert executed["result"].recommendation == "RUNAWAY"
    assert executed["result"].confidence == malformed["confidence"]
    assert executed["result"].evidence == malformed["evidence"]
    assert executed["validation_errors"] == []


def test_executor_defers_submission_until_supervisor_reads_same_batch_results():
    @dataclass
    class FakeEvidence:
        pipeline: str = "test pipeline"
        external_sink: bool = False
        protected: bool = False

    graph = agent.build_investigation_graph(
        None,
        None,
        agent.SkillRegistry(ROOT / "skills"),
        FakeEvidence(),
        max_supervision_cycles=8,
    )
    blocks = [
        SimpleNamespace(
            type="tool_use",
            name="load_business_skill",
            input={"skill": "diagnose-pipeline-runs"},
            id="skill-1",
        ),
        SimpleNamespace(
            type="tool_use",
            name="submit_recommendation",
            input=payload("KEEP"),
            id="submit-1",
        ),
    ]
    state = {
        "messages": [{"role": "user", "content": "investigate"}],
        "observations": BASE_OBSERVATIONS,
        "supervision_cycles": 1,
        "tool_blocks": blocks,
        "repair_mode": False,
    }

    executed = graph.functions["executor"](state)

    assert executed["result"] is None
    tool_results = executed["messages"][-1]["content"]
    deferred = next(item for item in tool_results if item["tool_use_id"] == "submit-1")
    assert deferred["content"].startswith("Submission deferred")
