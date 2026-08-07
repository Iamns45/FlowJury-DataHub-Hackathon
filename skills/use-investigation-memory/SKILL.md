---
name: use-investigation-memory
description: Recall prior FlowJury investigations and compare their evidence with the current DataHub context. Load when memory returns a prior or similar episode, when a verdict changed over time, or when current evidence conflicts with an earlier decision.
---

# Use Investigation Memory

Treat memory as historical evidence, never as current truth.

## Workflow

1. Inspect `temporal_comparison` before reading earlier verdicts.
2. If status is `CHANGED`, explain the material changed fields and re-run the skills affected by
   those changes. Ownership, protection, external sinks, consumers, source, and usage changes can
   invalidate the previous conclusion.
3. If status is `UNCHANGED`, use the earlier episode to avoid repeating irrelevant searches, but
   still verify the current facts required by the selected verdict skill.
4. Use same-domain or text-matched episodes only as investigation leads. Do not transfer their
   verdict to the current pipeline.
5. Prefer fresh DataHub facts and executable source over every stored episode.
6. Never use memory alone to support `KILL`, `REDUNDANT`, or a compliance decision.

## Conflict handling

- Return `UNKNOWN` when memory conflicts with current evidence and the conflict cannot be resolved.
- Treat a newly discovered consumer, external sink, or protection signal as a veto against `KILL`.
- Treat a disappeared consumer as a reason to run a new blast-radius simulation, not proof that
  retirement is safe.
- Cite recalled memory as `recall_investigation_memory` and cite the current verification
  separately.
