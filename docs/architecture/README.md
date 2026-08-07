# FlowJury architecture guide

This directory contains the editable draw.io diagrams for the full system and
the three-node LangGraph. The code follows the same boundaries shown in those
diagrams.

## Dependency rules

1. `flowjury/domain` contains business data types and must not import DataHub,
   LangGraph, model SDKs, or persistence code.
2. `flowjury/integrations` owns external APIs. Provider-specific code belongs
   behind an adapter in this directory.
3. `flowjury/analysis` turns metadata into evidence or impact summaries; it
   does not invoke the reasoning model.
4. `flowjury/agent` owns planning, execution, validation, skeptic review, and
   skill loading. It consumes normalized evidence instead of raw SDK objects.
5. `flowjury/cli.py` is the composition root. It is the only place that wires
   all production components together for a complete application run.
6. `scripts` may create demo data but must never be imported by production code.

## Where new functionality belongs

| Change | Location |
|---|---|
| New verdict data field | `flowjury/domain/models.py` |
| New DataHub read/write | `flowjury/integrations/datahub/` |
| New model provider | `flowjury/integrations/llm/` |
| New evidence calculation | `flowjury/analysis/` |
| New agent state or result type | `flowjury/agent/models.py` |
| New business rule | `skills/<skill-name>/SKILL.md` |
| New demo pipeline | `scripts/seed_demo_catalog.py` |
| New public CLI option | `flowjury/cli.py` |

## Diagrams

- `system-overview.drawio` shows DataHub ingestion, evidence normalization,
  agent reasoning, memory, proposal output, and optional DataHub write-back.
- `langgraph.drawio` shows planner → executor routing and the conditional
  skeptic review for `KILL` and `REDUNDANT` proposals.
