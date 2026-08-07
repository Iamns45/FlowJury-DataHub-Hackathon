---
name: handle-uncertain-pipelines
description: Investigate incomplete usage, uncataloged consumers, external sinks, streaming fanout, online feature stores, ambiguous ownership, or contradictory pipeline evidence and choose KEEP or UNKNOWN safely. Load whenever an important Signal is UNKNOWN or a non-SQL integration may exist.
---

# Handle Uncertain Pipelines

Never translate missing metadata into a negative fact.

- When usage coverage is absent, inspect lineage, DAG source, and related assets. Return `UNKNOWN` if use still cannot be verified.
- When tags or source suggest an external sink, inspect executable calls. Recommend `KEEP` when executable code clearly performs a current external write; otherwise recommend `UNKNOWN` and request owner confirmation.
- Recognize Kafka producers, webhooks, SaaS API calls, file delivery, Snowflake shares, Feast materialization, Redis/online stores, and operational queues as consumers even when dataset lineage and SQL usage are empty.
- For fanout pipelines, identify the concrete destinations or topic names and distinguish a checkpoint table from the pipeline's real product.
- For offline-plus-online ML pipelines, treat online materialization as production consumption and ask whether model-serving lineage is registered.
- Treat comments and descriptions as leads, not proof.
- Treat a tool failure as missing evidence.
- Prefer `UNKNOWN` whenever acting could interrupt an uncataloged consumer.

Use confidence near `0.40` for unresolved missing usage and near `0.50` for an unverified external-sink signal. Raise confidence for `KEEP` only when executable behavior or an authoritative external asset corroborates the consumer.
