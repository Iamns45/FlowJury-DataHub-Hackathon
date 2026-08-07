---
name: optimize-pipeline-compute
description: Identify DOWNSHIFT and TRIM opportunities caused by excessive schedule frequency or unused output columns. Load when a pipeline runs hourly or more often, has low daily reads, or builds a wide schema with sparse field usage.
---

# Optimize Pipeline Compute

Choose `DOWNSHIFT` when the schedule runs hourly or more often, the output has a real consumer, and maximum reads are no more than three per day. Confirm that downstream freshness requirements do not justify the cadence. Use confidence around `0.70` unless stronger SLA evidence exists.

Choose `TRIM` when field-level usage is available, the output contains at least eight columns, and fewer than half are read. Name the observed counts and ask the owner to validate dynamic or non-SQL consumers. Use confidence around `0.70`.

Do not treat missing field usage as zero columns read. Do not optimize a protected, runaway, failing, redundant, or insufficiently observed pipeline before resolving that higher-priority condition.
