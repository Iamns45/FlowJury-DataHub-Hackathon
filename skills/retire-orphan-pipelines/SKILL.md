---
name: retire-orphan-pipelines
description: Evaluate whether a pipeline is a safe KILL candidate because its outputs are unused and have no consumers. Load when usage is known, query count is zero, catalog consumers are absent, or an owner may be inactive.
---

# Retire Orphan Pipelines

Recommend `KILL` only after establishing all of the following:

1. Usage coverage exists for a meaningful window, normally at least 14 days.
2. Recorded queries are known zero, not `UNKNOWN`.
3. Catalog lineage has no downstream consumers.
4. DAG source shows no external export, API write, reverse-ETL call, or hidden sink.
5. A focused DataHub search finds no related consumer.
6. A retirement blast-radius simulation finds no cataloged or source-code blocker.
7. No compliance, retention, or external-sink signal applies.

Treat inactive ownership as supporting evidence, not a requirement. Treat any failed or missing required check as `UNKNOWN`, never as evidence for retirement. A skeptic review must independently challenge the proposal before it is accepted.

Start confidence around `0.85` when every check is satisfied. Increase slightly for a confirmed inactive individual owner; lower it for limited history or ambiguous naming. Always make human approval the next action.
