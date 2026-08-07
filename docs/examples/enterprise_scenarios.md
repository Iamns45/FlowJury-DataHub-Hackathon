# Enterprise scenario guide

These are seeded demonstration fixtures, not captured model output. Each case is designed so a
simple rule such as "zero table queries means kill" reaches the wrong answer. A live run may vary
in wording, but it should discover the listed evidence and stay within the expected safety class.

Before every trace, the runtime bootstraps the skill catalog, routing policy, and one compact
`recall_investigation_memory` result. On the first run memory reports `FIRST_SEEN`; later runs
report `UNCHANGED` or `CHANGED` with field-level differences. Every accepted result is stored as a
new episode after LangGraph ends. `KILL` and `REDUNDANT` proposals also pass through the independent
`skeptic_review` node first.

## 1. Customer CDC fan-out

**Initial catalog view:** `enterprise_customer_cdc_fanout` writes a checkpoint dataset with no
SQL consumers. It can look like an abandoned ingestion job.

**Expected LangGraph dry run:**

1. `planner` uses the bootstrapped catalog/router to select the uncertain-pipeline skill.
2. The agent sees zero downstream SQL consumers but treats that as incomplete evidence.
3. `executor` loads the skills and fetches the DAG source requested by the plan.
4. Source inspection finds Kafka producers for fraud, support, and search topics.
5. `simulate_retirement` identifies a non-catalog Kafka consumer signal.
6. The planner reads those results and ends with `KEEP`, citing the source and external delivery
   risk.

**Why this matters:** Kafka consumers usually do not appear as warehouse-query consumers.

## 2. Month-end close orchestrator

**Initial catalog view:** `month_end_close_orchestrator` runs only monthly and its balance table
has sparse usage.

**Expected LangGraph dry run:**

1. The agent loads the routing and regulated-pipeline skills.
2. Multi-output lineage reveals both a financial balance dataset and SOX audit evidence.
3. Tags and domain metadata indicate finance, compliance, and retention obligations.
4. Source inspection finds FX controls, reconciliations, and controller approval.
5. The compliance precedence rule overrides compute-optimization signals.
6. The graph records `PROTECT` with the SOX context fingerprint for future drift detection.

**Why this matters:** one low-usage output can hide a second legally important output.

## 3. Customer feature materialization

**Initial catalog view:** `customer_feature_materialization` produces an offline snapshot that
appears unused by analysts.

**Expected LangGraph dry run:**

1. The agent loads the uncertain-pipeline skill because warehouse usage is inconclusive.
2. Lineage shows no obvious downstream BI table.
3. Source inspection finds `FeatureStore.materialize_incremental` and an online Redis store.
4. The agent recognizes online model serving as an external consumer.
5. Retirement simulation reports the Feast/Redis source signal as a blocker.
6. The graph ends with `KEEP`, recommending that the online dependency be registered in DataHub.

**Why this matters:** an offline feature table may only be staging for production ML.

## 4. Regional tax reconciliation

**Initial catalog view:** `regional_tax_reconciliation` shares the raw transactions input and
roughly 80% of its schema with a revenue pipeline, which resembles redundant processing.

**Expected LangGraph dry run:**

1. The agent loads the redundancy and healthy-pipeline skills.
2. Peer search confirms overlapping inputs and columns.
3. Source inspection identifies tax jurisdiction, legal entity, VAT, and FX-date semantics.
4. Downstream lineage finds the statutory VAT report.
5. The redundancy skill says semantic purpose and regulated consumers outweigh structural overlap.
6. If the primary agent still proposes `REDUNDANT`, the skeptic blocks it because tax semantics
   were not proven equivalent.
7. The graph ends with `KEEP` or safe `UNKNOWN`; it may suggest sharing lower-level transforms instead of merging
   the business outputs.

**Why this matters:** schema similarity is not business equivalence.

## 5. GDPR erasure propagation

**Initial catalog view:** `gdpr_erasure_propagation` runs only when deletion requests arrive, and
its audit dataset has effectively no query activity.

**Expected LangGraph dry run:**

1. The agent loads the regulated and uncertain-pipeline skills.
2. Tags identify privacy, compliance, and retention obligations.
3. Source inspection finds deletion calls to several operational systems.
4. It also finds an immutable erasure audit record.
5. Protection precedence blocks any compute-waste verdict even though runs and reads are rare.
6. The graph records `PROTECT`; later removal of a GDPR tag appears as a temporal change but does
   not erase the earlier compliance episode.

**Why this matters:** event-driven compliance jobs should be quiet during normal operation.

## Safety invariant across all five

The model proposes a verdict; it never disables a schedule. A `KILL` proposal is accepted only
after source, lineage, catalog search, blast-radius evidence, and an independent skeptic pass are
present and no protection or external-sink fact is known. Otherwise the graph loops for more
evidence or returns `UNKNOWN`.
