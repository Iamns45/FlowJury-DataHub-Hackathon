# FlowJury 

**A DataHub-powered compute-waste investigator for data pipelines.** FlowJury finds
runaway, orphaned, over-scheduled, redundant, and silently failing workflows, proves
*whether it is safe to act*, and writes its findings back into DataHub so the next
person or agent inherits the decision instead of re-investigating.

Built for **Build with DataHub: The Agent Hackathon** — Track 1 (Agents That Do Real Work).

---

## The problem

A dead table costs storage. A forgotten *pipeline* costs compute on every run, forever. It often
goes unnoticed precisely because it never fails: a broken DAG gets attention; a pointless one
that succeeds nightly is invisible. When nobody can prove that a pipeline has no hidden consumer,
nobody feels safe turning it off.

The blocker is not detection. The blocker is **trust**: finding hidden consumers such as
reverse-ETL syncs, Kafka topics, online feature stores, file exports, and operational APIs before
recommending retirement. That proof is what FlowJury produces.

## What FlowJury does

For every pipeline in DataHub, FlowJury runs one stateful loop — **plan → execute → re-plan →
challenge risky verdicts** — and
produces one of nine verdicts:

| Verdict | Meaning |
|---|---|
| `KEEP` | Actively used or too new to judge as waste |
| `KILL` | Orphan — nothing consumes it, zero queries |
| `DOWNSHIFT` | Runs far more often than its output is consumed |
| `REDUNDANT` | Duplicates another pipeline and has fewer consumers |
| `TRIM` | Builds columns nobody reads |
| `RUNAWAY` | A run is stuck far past its historical baseline |
| `FIX/FOLD` | Fails repeatedly and unnoticed |
| `PROTECT` | Compliance, privacy, or retention workflow — never touch |
| `UNKNOWN` | Not enough evidence to decide safely |

## Four ideas that make it trustworthy

**1. Business policy lives in skills.** Python normalizes DataHub metadata but does not choose a
verdict in the agent-first path. The runtime preloads the compact skill catalog and mandatory
routing policy once. The configured LLM then selects only the specialist policies relevant to that
pipeline, gathers the evidence they require, and returns a cited proposal. A team changes a
business rule by reviewing one versioned `SKILL.md`, not by editing orchestration code.

**2. Agentic does not mean unbounded.** Every signal is `TRUE`, `FALSE`, or `UNKNOWN`, and missing
evidence never becomes "unused." The executor validates schema, loaded skills, citations, tool
budget, and retirement checks. The supervisor handles two bounded format-repair attempts
internally; missing investigation work returns to the supervisor-executor loop. The agent can
choose the verdict; it cannot disable a job, and failures fall back to `UNKNOWN`.

**3. Memory is change-aware.** Each investigation is stored as a durable episode with a
fingerprint of the normalized evidence. On the next run, FlowJury reports exactly which fields
changed. Prior verdicts are historical leads only; fresh DataHub evidence always wins.

**4. Risky proposals must survive opposition.** `KILL` and `REDUNDANT` proposals go through a
separate skeptic model call. It does not repeat the investigation. The skeptic receives a compact
decision dossier containing the current normalized facts, the proposal, applied policy names,
decision-critical source/lineage/blast-radius results, and failed checks. Raw memory and the full
audit trail are excluded. Missing or failed review downgrades the result to `UNKNOWN`.

## How it uses DataHub

FlowJury reads and **writes back** to the DataHub context graph:

- **Reads** — pipelines (`DataFlow`/`DataJob`), lineage, column-level lineage, usage statistics,
  run history (`DataProcessInstance`), ownership, domains, tags, and DAG source.
- **Agentic reads via the DataHub Agent Context Kit** — the agent chooses among skill loading,
  source inspection, catalog search, multi-output lineage, peer evidence, memory recall, and
  retirement simulation.
- **Write-back** — explicit `--writeback-proposals` stores `FlowJury-Agent-<VERDICT>` tags,
  confidence, applied skills, evidence, risks, and next action. It never changes schedules.

## Architecture

```text
DataHub ─→ Python normalization ─→ bootstrap once
                                  facts + skill catalog/router + compact memory
                                               │
                                               ▼
                         ┌──────────────────────────────┐
                         │ SUPERVISOR                   │
                         │ reason over current context  │
                         │ choose only missing evidence │
                         │ or submit final proposal      │
                         └──────────────┬───────────────┘
                                        │ actions
                                        ▼
                         ┌──────────────────────────────┐
                         │ EXECUTOR                     │
                         │ run tools + validate output  │
                         └──────────────┬───────────────┘
                                        │ observations
                                        └──────────────→ supervisor

Supervisor completes ── normal verdict ──────────────────────→ proposal
                     └─ KILL / REDUNDANT ─→ compact dossier
                                              └─ SKEPTIC ────→ proposal or UNKNOWN

After LangGraph ends: persist memory → JSON → optional DataHub write-back

Python owns: data access, schemas, budgets, audit log, safety protocol
Skills own: business thresholds, precedence, verdict guidance
Supervisor owns: which skills/tools to use and the final review verdict
```

FlowJury uses **LangGraph directly** with only three nodes: `supervisor`, `executor`, and
conditional `skeptic_review`. Validation, proposal repair, fallback, blast-radius analysis, and
memory writes remain ordinary Python helpers instead of graph nodes. LangChain is not required: a
small LLM adapter owns model transport, while LangGraph supplies state and the bounded
supervisor-executor loop.

The normal path needs two supervisor passes: first select a specialist and a focused evidence
batch; then read those results and submit the verdict. A third pass happens only when a lookup
failed, evidence conflicts, or a loaded specialist reveals a material gap. The executor refuses
to accept a proposal submitted alongside unread evidence.

## Repository layout

```text
flowjury-hackathon/
├── agent.py                    # compatibility wrapper; use python -m flowjury
├── flowjury/
│   ├── __main__.py             # canonical application entry point
│   ├── cli.py                  # arguments, console output, run orchestration
│   ├── settings.py             # paths, budgets, DataHub connection settings
│   ├── agent/                  # LangGraph runtime and skill loading
│   ├── analysis/               # evidence collection and blast-radius analysis
│   ├── domain/                 # Evidence, Signal, Finding, and Verdict types
│   ├── integrations/
│   │   ├── datahub/            # DataHub read and proposal-write adapters
│   │   └── llm/                # provider-neutral model adapter
│   └── memory/                 # SQLite episode store and temporal comparison
├── skills/                     # versioned business policies (SKILL.md)
├── scripts/                    # local DataHub demo-environment builders
├── docs/
│   ├── architecture/           # editable draw.io diagrams
│   └── examples/               # enterprise scenarios and memory dry run
└── tests/                      # behavior, safety, and architecture guardrails
```

The dependency direction is intentional: `domain` has no infrastructure dependency; `analysis`
and `agent` use adapters through `integrations`; `cli.py` is the composition root that wires the
application together. Demo seeders never participate in production assessments. See
[`docs/architecture/README.md`](docs/architecture/README.md) for extension rules.

## Five enterprise cases that fool simple detectors

| Pipeline | Why it looks disposable | Evidence the agent must discover | Expected verdict |
|---|---|---|---|
| Customer CDC fan-out | Its checkpoint table has no SQL consumers | Source publishes changes to fraud, support, and search Kafka topics | `KEEP` |
| Month-end close orchestrator | Runs monthly and its balance output has sparse queries | A second output is SOX evidence; source has controls and controller approval | `PROTECT` |
| Customer feature materialization | Offline snapshot looks unused | Source materializes features into Feast/Redis for online scoring | `KEEP` |
| Regional tax reconciliation | Shares inputs and most columns with a revenue transform | Jurisdiction, legal-entity, tax, and FX semantics differ; a VAT report consumes it | `KEEP` |
| GDPR erasure propagation | Rarely runs and its audit table is not queried | Source fans deletions into operational systems and writes an immutable audit | `PROTECT` |

These are intentionally difficult cases: catalog-only heuristics can misclassify all five. See
[`docs/examples/enterprise_scenarios.md`](docs/examples/enterprise_scenarios.md) for their
step-by-step investigation routes and expected evidence.

## Setup and run

**Prerequisites:** Docker, Python 3.11, an LLM API key, and an LLM name supported by the installed
adapter.

```bash
# 1. Create an environment and start DataHub locally
conda create -n flowjury python=3.11 -y
conda activate flowjury
pip install -r requirements.txt
datahub docker quickstart                      # UI at http://localhost:9002

# 2. Point FlowJury at DataHub and the configured LLM
export DATAHUB_GMS_URL=http://localhost:8080
export LLM_API_KEY=...
export LLM_NAME=...

# 3. Build the demo world
python -m scripts.seed_demo_catalog            # 18 pipelines
python -m scripts.add_demo_dag_sources         # add executable source clues
python -m scripts.seed_demo_memory              # 7 safe historical demo episodes

# 4. Run the primary workflow
python -m flowjury --json-out proposals.json
python -m flowjury --writeback-proposals        # explicit optional proposal write-back

# Optional: compare with the deterministic baseline
python -m flowjury.analysis.evidence
python -m flowjury.analysis.evidence --selftest
```

`LLM_TEMPERATURE` defaults to `0` for repeatable verdicts. If the selected model no longer accepts
that sampling control, the transport adapter retries once without it and uses the model's defaults
for the rest of the run. Incomplete transport or validation fallbacks remain visible as `UNKNOWN`,
but are not recorded as durable business-decision memory.

### Agent modes

The default run is read-only and assesses every pipeline. It stores investigation episodes in
`.flowjury/memory.sqlite3`. Before the first model call, Python records the available skill
summaries, loads `decide-pipeline-verdict`, and recalls compact memory once. The configured LLM
must then load relevant specialists and cite only observed evidence. A `KILL` proposal additionally
requires source, downstream lineage, catalog search, and a blast-radius simulation. `KILL` and
`REDUNDANT` must then pass the independent skeptic node. LangGraph routes invalid or incomplete
proposals back to the supervisor until the budget is exhausted.

`scripts/seed_demo_memory.py` makes the first demo run more illustrative by preloading seven
explicitly historical episodes. It uses current DataHub metadata for each stored snapshot, is
idempotent, and covers every seeded business domain so all 18 pipelines can retrieve exact or
same-domain history. The episodes are investigation leads only; current DataHub evidence and
safety gates still win.

```bash
# Assess one pipeline and save a machine-readable audit trail
python -m flowjury --pipeline "Nightly Hightouch Sync" --json-out proposals.json

# Put durable organizational memory somewhere explicit
python -m flowjury --memory-db ./state/flowjury-memory.sqlite3

# Compare behavior without memory
python -m flowjury --no-memory --pipeline "Nightly Hightouch Sync"

# Test another version of the business policies
python -m flowjury --skills-dir ./skills-experiment --pipeline "Hourly Inventory Snapshot"

# Explicitly store the proposal in separate flowjury_agent_* DataHub properties
python -m flowjury --writeback-proposals
```

Proposal write-back adds `FlowJury-Agent-Reviewed` and `FlowJury-Agent-<RECOMMENDATION>` tags.
It does not disable a schedule or execute the proposed action. If the model fails, skips required
skills or evidence, or exhausts its bounded round budget, the graph falls back to `UNKNOWN`.

## DataHub MCP option

The core uses DataHub's Agent Context Kit directly because it is the shortest path for an embedded
Python agent. If FlowJury needs to be called by an external MCP client, run DataHub's MCP server
as a context interface and expose FlowJury operations such as
`investigate_pipeline` and `simulate_retirement` through an MCP wrapper. This is optional; it is
not required for the local demo and does not change the memory or safety model.

## LLM configuration

FlowJury deliberately exposes only provider-neutral settings:

```bash
export LLM_API_KEY=...
export LLM_NAME=...
export LLM_TEMPERATURE=0
```

There is no hard-coded model name. `flowjury/integrations/llm/client.py` is the adapter boundary:
the current adapter uses the installed transport SDK, while LangGraph and the rest of FlowJury
remain unaware of provider-specific environment-variable names. Supporting another API means
replacing or extending that adapter; the public configuration contract can remain unchanged.
`LLM_TEMPERATURE` defaults to `0` because pipeline verdicts are analytical decisions where
repeatability matters more than creative variation.

Before falling back to `UNKNOWN` at the supervision limit, FlowJury makes one forced evidence-only
submission. The executor also canonicalizes recoverable proposal fields such as `risks`,
`next_action`, and `skills_applied`; it never changes the model-selected verdict, confidence, or
cited evidence.

Then open **http://localhost:9002**, search an assessed pipeline, and inspect the agent verdict,
loaded skills, evidence, risk, and next action.

## Sample investigations

See [`docs/examples/enterprise_scenarios.md`](docs/examples/enterprise_scenarios.md) for five
expected investigation traces. Skill-first JSON output includes `skills_applied` and the complete
tool-observation audit trail.

See [`docs/examples/memory_dry_run.md`](docs/examples/memory_dry_run.md) for a three-run temporal
example in which a prior `KILL` candidate gains a dashboard consumer and later an uncataloged
Kafka sink.

## How this maps to the judging criteria

- **Use of DataHub** — reads the context graph and writes findings back; agentic lookups use the
  Agent Context Kit.
- **Technical execution** — LangGraph manages evidence loops and skeptic routing; durable episodic
  memory detects context drift, while policies remain progressively disclosed.
- **Originality** — the tri-state trust model, runtime skill selection, temporal evidence
  fingerprints, source-aware blast-radius simulation, and adversarial review.
- **Real-world usefulness** — proposes and proves a safe action without auto-deleting anything.

## Limitations by design

- **Only as good as the metadata.** Incomplete lineage and usage can cause false positives, which
  is why `UNKNOWN` exists. FlowJury refuses to guess when the graph is thin.
- **Cost figures are estimates**, not billing-accurate, until connected to a warehouse cost source.
- **Decision support, not autopilot.** FlowJury proposes; a human approves.
- **LLM verdicts are not reproducible rules.** Skill text makes policy inspectable, but model
  interpretation can vary. Use audit output, model/version pinning, and offline evaluation before
  production adoption.
- **Local memory is a development backend.** The SQLite store is suitable for the hackathon and a
  single process. Use a governed Postgres or Redis LangGraph Store for a multi-worker deployment.

## License

Apache 2.0 — see [LICENSE](LICENSE).
