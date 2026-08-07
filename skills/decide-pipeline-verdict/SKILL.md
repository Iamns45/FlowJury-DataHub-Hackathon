---
name: decide-pipeline-verdict
description: Route normalized DataHub pipeline evidence to the appropriate FlowJury business-policy skills and select one final verdict. Load first for every pipeline assessment and use with one or more specialized pipeline skills.
---

# Decide Pipeline Verdict

Treat normalized DataHub evidence as facts, not as a verdict. Load the specialized skills whose descriptions match the evidence, gather any evidence they require, and then choose exactly one verdict.

## Workflow

1. Inspect the complete evidence object and list of available skills.
2. Load this routing skill, then load every specialized skill that could plausibly change the outcome.
3. Follow each loaded skill's required checks. Use DataHub tools when existing evidence is insufficient.
4. Prefer verified executable behavior and catalog facts over names, descriptions, or comments.
5. Resolve conflicting matches in this safety order: `PROTECT`, `RUNAWAY`, `FIX/FOLD`, `REDUNDANT`, uncertainty/newness, `KILL`, `DOWNSHIFT`, `TRIM`, `KEEP`.
6. Return `UNKNOWN` when a required fact is missing or contradictory.
7. Submit one verdict with cited facts, cited loaded skills, the largest remaining risk, and a human-review next action.

Never claim that a proposal changed or stopped a pipeline. FlowJury verdicts are review recommendations.
