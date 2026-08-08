# Latest demo results

Updated: 2026-08-08

This is the canonical result matrix for the seeded FlowJury catalog. The matrix is validated by
the repository's demo tests and deterministic evidence self-test. A live LLM run may safely return
`UNKNOWN` when a required DataHub lookup fails; rerun the idempotent demo seeders to restore the
complete evidence world before recording a demonstration.

| Pipeline | Verdict | Deciding evidence |
|---|---|---|
| Daily Revenue Summary | `KEEP` | Active reporting-mart consumer and real query demand |
| Customer 360 Daily Build | `KEEP` | Active downstream use and healthy operation |
| Nightly Marketing Customer Export | `KILL` | Known-zero usage, no consumers, inactive owner, source checked, and no blast-radius blocker |
| Hourly Inventory Snapshot | `DOWNSHIFT` | Runs hourly while its output is consumed weekly |
| Legacy Daily Revenue (Business) | `REDUNDANT` | Same transformation as the canonical revenue feature pipeline |
| Revenue Features Daily (ML) | `KEEP` | Canonical, healthy feature pipeline with active consumers |
| Wide User Profile Build | `TRIM` | Produces 15 columns while downstream readers use about four |
| Fraud Scoring Batch | `FIX/FOLD` | Repeated failure rate exceeds the operational threshold |
| Realtime Events Aggregation | `RUNAWAY` | Current execution is far beyond its historical runtime baseline |
| Quarterly Compliance Close | `PROTECT` | Protected SOX/finance evidence overrides sparse usage |
| Nightly Hightouch Sync | `KEEP` | Executable reverse-ETL call reveals an external consumer absent from SQL lineage |
| A/B Test Metrics Daily | `UNKNOWN` | Usage coverage is unavailable and a possible Looker consumer remains uncataloged |
| New Signup Features | `KEEP` | Low usage is explained by pipeline age, not abandonment |
| Enterprise Customer CDC Fanout | `KEEP` | Kafka consumers are visible in executable source but absent from SQL lineage |
| Month-End Finance Close Orchestrator | `PROTECT` | Multi-output SOX close produces retained control evidence |
| Customer Feature Materialization (Offline + Online) | `KEEP` | Feast/Redis online serving is a real consumer outside warehouse lineage |
| Regional Tax Reconciliation | `KEEP` | Similar structure to revenue jobs, but tax/FX semantics are materially different |
| GDPR Erasure Propagation | `PROTECT` | Rare mandatory privacy-deletion workflow with immutable audit evidence |

## Terminal color legend

| Verdict or section | Terminal color |
|---|---|
| `KEEP` | Green |
| `PROTECT` | Magenta |
| `KILL`, `RUNAWAY` | Red |
| `REDUNDANT`, `FIX/FOLD` | Yellow |
| `TRIM` | Cyan |
| `DOWNSHIFT` | Blue |
| `UNKNOWN` | Gray |
| Evidence | Cyan |
| Risk | Yellow |
| Memory | Blue |
| Skeptic review | Magenta |
| Next action | Green |

The terminal renderer uses ANSI colors only for interactive sessions. `proposals.json` remains
plain structured data suitable for automation and audit.

## Reproduce the complete demo

```bash
python -m scripts.seed_demo_catalog
python -m scripts.add_demo_dag_sources
python -m scripts.seed_demo_memory
python agent.py --json-out proposals.json
```

`KILL` and `REDUNDANT` remain proposals: they must pass deterministic validation and independent
skeptic review, and FlowJury never disables a pipeline.
