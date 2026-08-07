---
name: protect-regulated-pipelines
description: Decide whether compliance, legal-hold, audit, privacy, or retention obligations require a pipeline to receive PROTECT. Load when tags, domains, descriptions, names, or DAG source indicate regulated or retained data.
---

# Protect Regulated Pipelines

Recommend `PROTECT` when authoritative metadata contains a compliance or retention tag such as `compliance`, `retention`, `pii-retention`, `sox`, or `gdpr`.

Also consider executable DAG behavior and explicit policy metadata. Multi-output finance closes may produce both balances and immutable control evidence; privacy workflows may exist to propagate deletion rather than serve queries. Treat a pipeline name, free-text description, or source-code comment alone as insufficient; load more evidence or return `UNKNOWN` if the obligation cannot be verified.

Protection overrides optimization or retirement recommendations. Explain the governing signal and recommend owner or governance review rather than schedule changes.

Confidence guidance:

- Use about `0.99` for an authoritative protection tag.
- Use at most `0.75` for corroborated but indirect policy evidence.
- Use `UNKNOWN` when the only signal is an unverified hint.
