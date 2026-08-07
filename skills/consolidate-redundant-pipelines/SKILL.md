---
name: consolidate-redundant-pipelines
description: Decide whether one pipeline is REDUNDANT with a stronger peer. Load when pipelines share inputs, produce similar output columns, have overlapping purposes, or differ substantially in consumer count.
---

# Consolidate Redundant Pipelines

Load peer evidence before deciding. Compare input dataset sets and output-column sets rather than names alone.

Recommend `REDUNDANT` when input-set similarity and output-column similarity are each at least 80%, the pipelines serve equivalent purposes, and one has fewer consumers. Prefer the more consumed or explicitly canonical pipeline as the survivor.

Do not recommend immediate deletion. Identify the stronger peer, verify that consumers can migrate, and make consolidation planning the next action. Inspect transformation source when structural similarity is high: currency translation, jurisdiction rules, legal-entity scope, privacy filtering, accounting definitions, and slowly-changing-dimension semantics can make similar schemas non-equivalent. Return `KEEP` when executable logic and downstream purpose clearly differ; return `UNKNOWN` when schemas or semantics cannot be verified.

Use confidence around `0.75` for strong structural similarity and clear consumer asymmetry.
