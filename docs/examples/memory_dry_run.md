# Memory and temporal-change dry run

This is an expected trace showing how the same pipeline is reassessed as organizational context
changes. It is illustrative, not captured model output.

## Run 1: apparently safe retirement

`Nightly Marketing Export` has 30 days of known-zero queries, no catalog consumers, inactive
ownership, and no external sink in its executable source.

1. Runtime bootstrap supplies a single `recall_investigation_memory` result with `FIRST_SEEN`.
2. The agent loads the routing and orphan-retirement skills.
3. Source, search, and downstream checks find no known consumer.
4. `simulate_retirement` returns `INCONCLUSIVE_NO_KNOWN_IMPACT`. This is not proof by itself, so
   the other retirement checks are still required.
5. The primary agent proposes `KILL` with human approval as the next action.
6. `skeptic_review` independently checks a compact decision dossier and returns `PASS`.
7. After LangGraph ends, the memory adapter stores the verdict, evidence snapshot, DAG-source hash,
   observations, and a unique episode ID.

## Run 2: a new dashboard appears

Before the next assessment, a campaign dashboard is registered downstream of the export.

1. Memory returns `CHANGED` rather than copying the old `KILL` verdict.
2. `changed_fields` includes `consumer_count` and `has_consumers`.
3. The agent loads `use-investigation-memory` and re-runs the checks affected by the change.
4. The blast-radius simulation now lists the dashboard as a cataloged downstream asset.
5. The previous `KILL` is treated as obsolete historical evidence; the new verdict is `KEEP`.
6. A second episode is recorded without modifying or deleting the first.

## Run 3: an uncataloged Kafka delivery is added

The dashboard is later removed, but the DAG begins publishing the export to an operational Kafka
topic.

1. Normalized lineage may again look unused.
2. Memory still reports `CHANGED` because `context_dag_source_sha256` differs.
3. Source inspection and retirement simulation identify a Kafka producer signal.
4. `KILL` is blocked even though catalog usage is zero.
5. The agent returns `KEEP` or `UNKNOWN` and recommends registering the Kafka dependency in
   DataHub.

The important property is that memory improves investigation efficiency without becoming an
authority. Every episode remains auditable, while present-tense evidence controls the verdict.
