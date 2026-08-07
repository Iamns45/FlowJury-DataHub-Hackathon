---
name: assess-healthy-pipelines
description: Recognize actively used or newly launched pipelines that should receive KEEP. Load when outputs have consumers and queries, the usage window is new, or no waste, failure, protection, or uncertainty policy has matched.
---

# Assess Healthy Pipelines

Recommend `KEEP` when cataloged consumers and recent queries demonstrate active use and no higher-priority skill identifies risk or waste.

Also recommend `KEEP` for a pipeline with a positive but shorter-than-14-day usage window; low usage is expected during launch. Mention that the recommendation should be revisited after more history accumulates.

Do not use `KEEP` as a default when usage, lineage, or run health is missing. Load the uncertainty skill instead.

Start confidence near `0.90` for corroborated consumers plus queries. Subtract about `0.15` for each important unknown signal. Use about `0.70` for a new pipeline.
