---
name: diagnose-pipeline-runs
description: Diagnose currently runaway or repeatedly failing pipeline executions and choose RUNAWAY or FIX/FOLD. Load when run history exists, a run is active, runtime exceeds baseline, or failure rate is elevated.
---

# Diagnose Pipeline Runs

Use completed runs as the baseline and distinguish stopping one execution from retiring its schedule.

- Recommend `RUNAWAY` when a run is active, exceeds both three times the median completed runtime and 45 minutes, and the timestamps are credible. Recommend stopping the stuck execution while keeping the schedule.
- Recommend `FIX/FOLD` when at least 25% of recent completed runs failed. Ask the owner to repair the job or explicitly retire it.
- Do not infer healthy execution when run records are unavailable.
- If both conditions match, prefer `RUNAWAY` because compute is burning now, and mention the failure history as additional evidence.

Confidence guidance:

- Use about `0.90` for a clearly active runaway with a stable baseline.
- For `FIX/FOLD`, start near `0.60` and increase with the failure rate, capped near `0.90`.
