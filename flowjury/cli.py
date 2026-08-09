"""Command-line application for running FlowJury assessments."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Optional, Sequence

from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.context import set_client

from flowjury.agent.runtime import (
    AgentRecommendation,
    fallback_recommendation,
    investigate,
)
from flowjury.agent.context import InvestigationContext
from flowjury.agent.skills import SkillRegistry
from flowjury.analysis.evidence import gather_evidence
from flowjury.integrations.datahub.client import FlowJuryClient
from flowjury.integrations.datahub.writeback import (
    load_proposal_file,
    write_agent_proposal,
    write_existing_proposals,
)
from flowjury.integrations.llm.client import create_llm_client, missing_llm_configuration
from flowjury.memory.store import InvestigationMemory
from flowjury.settings import (
    DATAHUB_GMS_TOKEN,
    DATAHUB_GMS_URL,
    DEFAULT_MAX_SUPERVISION_CYCLES,
    DEFAULT_MEMORY_DB,
    DEFAULT_SKILLS_DIR,
)
from flowjury.ui import (
    activity,
    configure_color,
    render_assessment_header,
    render_recommendation,
    render_run_banner,
)


def print_recommendation(result: AgentRecommendation) -> None:
    """Render one human-readable assessment to stdout."""
    print(render_recommendation(result))


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse and validate the public FlowJury CLI contract."""
    parser = argparse.ArgumentParser(
        description="Assign skill-guided verdicts to DataHub pipelines."
    )
    parser.add_argument("--pipeline", help="Assess one pipeline name (case-insensitive).")
    parser.add_argument(
        "--max-supervision-cycles",
        "--max-planning-cycles",
        "--max-rounds",
        dest="max_supervision_cycles",
        type=int,
        default=DEFAULT_MAX_SUPERVISION_CYCLES,
        help=(
            "Maximum supervisor cycles per pipeline "
            f"(default: {DEFAULT_MAX_SUPERVISION_CYCLES}; the older "
            "--max-planning-cycles and --max-rounds names remain aliases)."
        ),
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        help="Write structured proposals to this JSON file.",
    )
    parser.add_argument(
        "--skills-dir",
        type=Path,
        default=DEFAULT_SKILLS_DIR,
        help=f"Business skill root (default: {DEFAULT_SKILLS_DIR}).",
    )
    parser.add_argument(
        "--memory-db",
        type=Path,
        default=DEFAULT_MEMORY_DB,
        help=f"Durable investigation memory (default: {DEFAULT_MEMORY_DB}).",
    )
    parser.add_argument(
        "--no-memory",
        action="store_true",
        help="Run statelessly without recalling or recording investigation episodes.",
    )
    parser.add_argument(
        "--writeback-proposals",
        action="store_true",
        help="Write separate flowjury_agent_* proposal metadata to DataHub. Never changes jobs.",
    )
    parser.add_argument(
        "--writeback-json",
        type=Path,
        help="Write an existing FlowJury JSON result file to DataHub without rerunning the agent.",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colors (colors also turn off automatically when output is redirected).",
    )
    args = parser.parse_args(argv)
    if not 1 <= args.max_supervision_cycles <= 20:
        parser.error("--max-supervision-cycles must be between 1 and 20")
    if args.writeback_json and any(
        (args.pipeline, args.json_out, args.writeback_proposals, args.no_memory)
    ):
        parser.error(
            "--writeback-json is a standalone mode; do not combine it with assessment options"
        )
    return args


def writeback_json(path: Path) -> int:
    """Write saved recommendations to DataHub without invoking the reasoning graph."""
    from datahub.emitter.rest_emitter import DatahubRestEmitter

    try:
        results = load_proposal_file(path)
        datahub = FlowJuryClient(DATAHUB_GMS_URL, DATAHUB_GMS_TOKEN)
        emitter = DatahubRestEmitter(
            gms_server=DATAHUB_GMS_URL,
            token=DATAHUB_GMS_TOKEN,
        )
        report = write_existing_proposals(datahub, emitter, results)
    except Exception as exc:
        print(activity(f"⚠ writeback failed: {exc}", "warning"))
        return 1

    for error in report["errors"]:
        print(activity(f"  ⚠ {error}", "warning"))
    written = report["written"]
    print(activity(f"✓ wrote {written}/{len(results)} saved proposals to DataHub", "success"))
    return 0 if not report["errors"] else 1


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Assess every discovered pipeline, or one selected with ``--pipeline``."""
    args = parse_args(argv)
    if args.no_color:
        configure_color(False)
    if args.writeback_json:
        return writeback_json(args.writeback_json)
    missing_llm = missing_llm_configuration()
    if missing_llm:
        print("Set LLM configuration first: " + ", ".join(missing_llm))
        return 1

    set_client(DataHubClient(server=DATAHUB_GMS_URL, token=DATAHUB_GMS_TOKEN))
    datahub = FlowJuryClient(DATAHUB_GMS_URL, DATAHUB_GMS_TOKEN)
    jobs = datahub.list_pipeline_jobs()
    if not jobs:
        print("No pipelines found — run scripts/seed_demo_catalog.py first.")
        return 1

    print("Collecting normalized evidence and discovering business skills...")
    all_evidence = [gather_evidence(datahub, job) for job in jobs]
    evidence_to_assess = list(all_evidence)
    if args.pipeline:
        wanted = args.pipeline.casefold()
        evidence_to_assess = [
            evidence for evidence in evidence_to_assess if evidence.pipeline.casefold() == wanted
        ]
        if not evidence_to_assess:
            print(f"No pipeline named {args.pipeline!r}.")
            return 1

    skills = SkillRegistry(args.skills_dir)
    context = InvestigationContext(datahub, jobs, all_evidence)
    llm_client = create_llm_client()
    memory = None if args.no_memory else InvestigationMemory(args.memory_db)
    emitter = None
    if args.writeback_proposals:
        from datahub.emitter.rest_emitter import DatahubRestEmitter

        emitter = DatahubRestEmitter(
            gms_server=DATAHUB_GMS_URL,
            token=DATAHUB_GMS_TOKEN,
        )

    print(f"Loaded {len(skills)} skill descriptors.")
    print("Memory disabled." if memory is None else f"Memory: {memory.path}")
    print("\n" + render_run_banner(len(evidence_to_assess)))
    results: List[AgentRecommendation] = []
    for position, evidence in enumerate(evidence_to_assess, 1):
        print(
            render_assessment_header(
                evidence.pipeline,
                position,
                len(evidence_to_assess),
            )
        )
        try:
            result = investigate(
                llm_client,
                context,
                skills,
                evidence,
                args.max_supervision_cycles,
                memory=memory,
            )
        except Exception as exc:
            result = fallback_recommendation(evidence, [], f"Agent call failed: {exc}")

        if (
            memory is not None
            and result.memory_id is None
            and result.investigation_status == "COMPLETED"
        ):
            try:
                memory_context = context.memory_context(evidence.pipeline)
                result.temporal_change = result.temporal_change or memory.compare(
                    evidence, memory_context
                )
                result.memory_id = memory.record(
                    result,
                    evidence,
                    result.investigation,
                    context=memory_context,
                )
            except Exception as exc:
                print(f"  ⚠ memory write failed: {exc}")

        results.append(result)
        print_recommendation(result)
        if emitter:
            try:
                flow_urn = context.flow_urn(result.pipeline)
                written = bool(flow_urn) and write_agent_proposal(
                    datahub.graph,
                    emitter,
                    flow_urn,
                    result,
                )
                print(
                    "  ✓ proposal written to DataHub"
                    if written
                    else "  ⚠ writeback skipped by safety gate or target not found"
                )
            except Exception as exc:
                print(f"  ⚠ proposal writeback failed: {exc}")
        print()

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(
            json.dumps([result.as_dict() for result in results], indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Structured proposals written to {args.json_out}")
    if memory is not None:
        memory.close()
    return 0
