"""Build the realistic 18-pipeline FlowJury demonstration catalog.

The catalog covers every supported verdict plus difficult enterprise cases in
which catalog-only heuristics would make the wrong decision. Each scenario is
tagged with the verdict the skill-guided agent should eventually reach.

  ACTIONABLE CASES
    [KILL]      nightly_marketing_export     no consumers, 0 queries, owner suspended
    [DOWNSHIFT] hourly_inventory_snapshot    runs hourly, output consumed weekly
    [REDUNDANT] legacy_daily_revenue         duplicates revenue_features_daily
    [TRIM]      wide_user_profile_build      builds 15 cols, ~4 read downstream
    [FIX/FOLD]  fraud_scoring_batch          repeated failures without active response
    [RUNAWAY]   realtime_events_agg          a run started 6h ago, way past baseline

  SAFETY EDGE CASES
    [PROTECT]   quarterly_compliance_close   looks dead, but quarterly + compliance tag
    [KEEP]      nightly_hightouch_sync       executable reverse-ETL sink outside lineage
    [UNKNOWN]   ab_test_metrics_daily        no usage data at all (unknown != zero)
    [KEEP-NEW]  new_signup_features          low usage only because it's 5 days old

  HEALTHY CONTROLS
    [KEEP]      daily_revenue, customer_360_daily, revenue_features_daily

  ENTERPRISE INVESTIGATIONS (require skills + source/context reasoning)
    [KEEP]      enterprise_customer_cdc_fanout  Kafka consumers absent from SQL lineage
    [PROTECT]   month_end_close_orchestrator    multi-output SOX close with sparse usage
    [KEEP]      customer_feature_materialization offline + Feast/Redis online serving
    [KEEP]      regional_tax_reconciliation     structural duplicate, different semantics
    [PROTECT]   gdpr_erasure_propagation        rare privacy workflow with no query demand

Run against `datahub docker quickstart`:
    export DATAHUB_GMS_URL=http://localhost:8080
    export DATAHUB_GMS_TOKEN=<PAT>          # omit on default OSS
    python -m scripts.seed_demo_catalog

Offline validation (no DataHub, no network):
    DRY_RUN=1 python -m scripts.seed_demo_catalog
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import List, Optional

from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_domain_urn,
    make_group_urn,
    make_tag_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    CorpUserInfoClass,
    DatasetFieldUsageCountsClass,
    DatasetLineageTypeClass,
    DatasetUsageStatisticsClass,
    DomainPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TagAssociationClass,
    TimeWindowSizeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import DatasetUrn

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
GMS_TOKEN = os.environ.get("DATAHUB_GMS_TOKEN")
PLATFORM = "snowflake"
ENV = "PROD"
DAY_MS = 24 * 60 * 60 * 1000
HOUR_MS = 60 * 60 * 1000
NOW_MS = int(time.time() * 1000)


class _DryRunEmitter:
    def __init__(self) -> None:
        self.count = 0

    def emit(self, item, callback=None):
        self.count += 1

    def emit_mcp(self, mcp, **kw):
        self.count += 1

    def emit_mce(self, mce, **kw):
        self.count += 1

    def flush(self):
        pass


def get_emitter():
    if os.environ.get("DRY_RUN"):
        return _DryRunEmitter()
    return DatahubRestEmitter(gms_server=GMS_URL, token=GMS_TOKEN)


# --------------------------------------------------------------------------- #
# Emit helpers
# --------------------------------------------------------------------------- #
def emit_dataset(emitter, name: str, fields: List[tuple]) -> str:
    urn = make_dataset_urn(PLATFORM, name, ENV)
    schema_fields = []
    for fname, t in fields:
        cls = NumberTypeClass() if t in ("number", "int", "float") else StringTypeClass()
        native = {"number": "NUMBER", "int": "INT", "float": "FLOAT"}.get(t, "VARCHAR")
        schema_fields.append(
            SchemaFieldClass(
                fieldPath=fname,
                type=SchemaFieldDataTypeClass(type=cls),
                nativeDataType=native,
            )
        )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=SchemaMetadataClass(
                schemaName=name,
                platform=f"urn:li:dataPlatform:{PLATFORM}",
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=schema_fields,
            ),
        )
    )
    return urn


def emit_dataset_lineage(emitter, downstream_urn: str, upstream_urns: List[str]) -> None:
    upstreams = [
        UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED) for u in upstream_urns
    ]
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=downstream_urn, aspect=UpstreamLineageClass(upstreams=upstreams)
        )
    )


def emit_usage(
    emitter,
    dataset_urn: str,
    days: int,
    queries_per_day: int,
    users: int,
    field_counts: Optional[dict] = None,
) -> None:
    """One daily usage bucket per day. field_counts = {col: queries_per_day_for_col}."""
    granularity = TimeWindowSizeClass(unit=CalendarIntervalClass.DAY, multiple=1)
    fc = None
    if field_counts:
        fc = [DatasetFieldUsageCountsClass(fieldPath=k, count=v) for k, v in field_counts.items()]
    for d in range(days):
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetUsageStatisticsClass(
                    timestampMillis=NOW_MS - d * DAY_MS,
                    eventGranularity=granularity,
                    uniqueUserCount=users,
                    totalSqlQueries=queries_per_day,
                    fieldCounts=fc,
                ),
            )
        )


def emit_domain(emitter, domain_id: str, name: str) -> None:
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=make_domain_urn(domain_id),
            aspect=DomainPropertiesClass(name=name),
        )
    )


def set_domain(emitter, entity_urn: str, domain_id: str) -> None:
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=entity_urn, aspect=DomainsClass(domains=[make_domain_urn(domain_id)])
        )
    )


def add_tags(emitter, entity_urn: str, tag_names: List[str]) -> None:
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=GlobalTagsClass(
                tags=[TagAssociationClass(tag=make_tag_urn(t)) for t in tag_names]
            ),
        )
    )


def suspend_user(emitter, user_id: str, display: str) -> None:
    """Mark a corp user inactive (the 'owner has left' signal)."""
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=make_user_urn(user_id),
            aspect=CorpUserInfoClass(active=False, displayName=display),
        )
    )


def emit_pipeline(
    emitter,
    flow_id: str,
    name: str,
    description: str,
    inlets: List[str],
    outlets: List[str],
    owner_user: str,
    owner_group: str,
    schedule_cron: str,
    domain: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> DataJob:
    flow = DataFlow(
        id=flow_id,
        orchestrator="airflow",
        env=ENV,
        name=name,
        description=description,
        properties={"schedule": schedule_cron},
        owners={make_user_urn(owner_user)},
        group_owners={make_group_urn(owner_group)},
    )
    flow.emit(emitter)
    if domain:
        set_domain(emitter, str(flow.urn), domain)
    if tags:
        add_tags(emitter, str(flow.urn), tags)

    job = DataJob(
        id=f"{flow_id}_task",
        flow_urn=flow.urn,
        name=name,
        inlets=[DatasetUrn.from_string(u) for u in inlets],
        outlets=[DatasetUrn.from_string(u) for u in outlets],
        owners={make_user_urn(owner_user)},
        group_owners={make_group_urn(owner_group)},
    )
    job.emit(emitter)
    return job


def emit_runs(
    emitter,
    job: DataJob,
    day_offsets: List[int],
    duration_min: int,
    result: InstanceRunResult = InstanceRunResult.SUCCESS,
    hour: int = 2,
) -> None:
    """Emit one daily run per offset in day_offsets."""
    first = True
    for d in day_offsets:
        day_start = NOW_MS - d * DAY_MS
        start_ms = day_start - (day_start % DAY_MS) + hour * HOUR_MS
        end_ms = start_ms + duration_min * 60 * 1000
        tag = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y%m%d")
        dpi = DataProcessInstance.from_datajob(
            datajob=job, id=f"{job.id}-{tag}", clone_inlets=True, clone_outlets=True
        )
        dpi.emit_process_start(emitter, start_ms, emit_template=first, materialize_iolets=False)
        dpi.emit_process_end(
            emitter, end_ms, result=result, result_type="AIRFLOW", start_timestamp_millis=start_ms
        )
        first = False


def emit_hourly_runs(emitter, job: DataJob, days: int, duration_min: int) -> None:
    """Emit 24 runs/day for the last `days` days (to establish hourly cadence)."""
    first = True
    for h in range(days * 24):
        start_ms = NOW_MS - h * HOUR_MS
        end_ms = start_ms + duration_min * 60 * 1000
        dpi = DataProcessInstance.from_datajob(
            datajob=job, id=f"{job.id}-h{h}", clone_inlets=True, clone_outlets=True
        )
        dpi.emit_process_start(emitter, start_ms, emit_template=first, materialize_iolets=False)
        dpi.emit_process_end(
            emitter,
            end_ms,
            result=InstanceRunResult.SUCCESS,
            result_type="AIRFLOW",
            start_timestamp_millis=start_ms,
        )
        first = False


def emit_running_now(emitter, job: DataJob, running_hours: int) -> None:
    """A run that STARTED running_hours ago and has NOT finished (the runaway)."""
    start_ms = NOW_MS - running_hours * HOUR_MS
    dpi = DataProcessInstance.from_datajob(
        datajob=job, id=f"{job.id}-RUNNING", clone_inlets=True, clone_outlets=True
    )
    dpi.emit_process_start(emitter, start_ms, emit_template=False, materialize_iolets=False)
    # deliberately no emit_process_end -> stays in STARTED state


# --------------------------------------------------------------------------- #
# The scenario
# --------------------------------------------------------------------------- #
DAILY_90 = list(range(90))


def build(emitter) -> None:
    # --- domains (business areas / owning teams) ---
    for did, dname in [
        ("finance", "Finance"),
        ("marketing", "Marketing"),
        ("ml-platform", "ML Platform"),
        ("data-platform", "Data Platform"),
        ("growth", "Growth"),
        ("risk", "Risk"),
    ]:
        emit_domain(emitter, did, dname)

    # --- shared source tables ---
    raw_customers = emit_dataset(
        emitter,
        "raw.crm.customers",
        [
            ("customer_id", "int"),
            ("email", "string"),
            ("region", "string"),
            ("signup_ts", "string"),
        ],
    )
    raw_txns = emit_dataset(
        emitter,
        "raw.finance.transactions",
        [("txn_id", "int"), ("customer_id", "int"), ("amount", "number"), ("txn_ts", "string")],
    )
    raw_events = emit_dataset(
        emitter,
        "raw.product.events",
        [
            ("event_id", "int"),
            ("customer_id", "int"),
            ("event_type", "string"),
            ("event_ts", "string"),
        ],
    )
    raw_inventory = emit_dataset(
        emitter,
        "raw.ops.inventory",
        [("sku", "string"), ("warehouse", "string"), ("qty", "int"), ("snapshot_ts", "string")],
    )

    # ========================== HEALTHY CONTROLS ========================== #
    # H1: daily_revenue -> feeds a mart, real queries
    rev_summary = emit_dataset(
        emitter,
        "analytics.finance.daily_revenue_summary",
        [
            ("day", "string"),
            ("region", "string"),
            ("gross_revenue", "number"),
            ("txn_count", "int"),
        ],
    )
    rev_mart = emit_dataset(
        emitter,
        "analytics.finance.revenue_reporting_mart",
        [("day", "string"), ("region", "string"), ("gross_revenue", "number")],
    )
    emit_dataset_lineage(emitter, rev_summary, [raw_txns])
    emit_dataset_lineage(emitter, rev_mart, [rev_summary])
    emit_usage(emitter, rev_summary, 90, queries_per_day=18, users=6)
    emit_usage(emitter, rev_mart, 90, queries_per_day=25, users=9)
    j = emit_pipeline(
        emitter,
        "daily_revenue",
        "Daily Revenue Summary",
        "Aggregates transactions into the daily revenue summary.",
        [raw_txns],
        [rev_summary],
        "sam.torres",
        "data-platform",
        "0 3 * * *",
        domain="finance",
    )
    emit_runs(emitter, j, DAILY_90, duration_min=22)

    # H2: customer_360_daily -> actively used
    c360 = emit_dataset(
        emitter,
        "analytics.growth.customer_360",
        [("customer_id", "int"), ("ltv", "number"), ("segment", "string"), ("last_seen", "string")],
    )
    emit_dataset_lineage(emitter, c360, [raw_customers, raw_events])
    emit_usage(emitter, c360, 90, queries_per_day=40, users=15)
    j = emit_pipeline(
        emitter,
        "customer_360_daily",
        "Customer 360 Daily Build",
        "Builds the customer 360 profile used across Growth.",
        [raw_customers, raw_events],
        [c360],
        "priya.nair",
        "growth",
        "0 4 * * *",
        domain="growth",
    )
    emit_runs(emitter, j, DAILY_90, duration_min=35)

    # ===================== [KILL] nightly_marketing_export ================= #
    # Orphan: output has no consumers, zero queries for 90d, owner has left.
    mkt_export = emit_dataset(
        emitter,
        "analytics.marketing.customer_export",
        [("customer_id", "int"), ("email", "string"), ("region", "string")],
    )
    emit_dataset_lineage(emitter, mkt_export, [raw_customers])
    emit_usage(emitter, mkt_export, 90, queries_per_day=0, users=0)  # KNOWN zero
    suspend_user(emitter, "dana.lee", "Dana Lee (left company)")
    j = emit_pipeline(
        emitter,
        "nightly_marketing_export",
        "Nightly Marketing Customer Export",
        "Exports the customer list for a marketing campaign that ended.",
        [raw_customers],
        [mkt_export],
        "dana.lee",
        "marketing",
        "0 2 * * *",
        domain="marketing",
    )
    emit_runs(emitter, j, DAILY_90, duration_min=48)

    # ================== [DOWNSHIFT] hourly_inventory_snapshot ============== #
    # Runs hourly, but its output feeds a report queried ~weekly. 167 wasted runs/wk.
    inv_snapshot = emit_dataset(
        emitter,
        "analytics.ops.inventory_snapshot",
        [("sku", "string"), ("warehouse", "string"), ("qty", "int"), ("as_of", "string")],
    )
    inv_report = emit_dataset(
        emitter,
        "analytics.ops.weekly_inventory_report",
        [("warehouse", "string"), ("total_qty", "int"), ("week", "string")],
    )
    emit_dataset_lineage(emitter, inv_snapshot, [raw_inventory])
    emit_dataset_lineage(emitter, inv_report, [inv_snapshot])
    emit_usage(emitter, inv_snapshot, 90, queries_per_day=1, users=1)  # rarely read directly
    emit_usage(emitter, inv_report, 90, queries_per_day=0, users=2)  # weekly report, sparse
    j = emit_pipeline(
        emitter,
        "hourly_inventory_snapshot",
        "Hourly Inventory Snapshot",
        "Snapshots inventory every hour; only the weekly report consumes it.",
        [raw_inventory],
        [inv_snapshot],
        "sam.torres",
        "data-platform",
        "0 * * * *",
        domain="data-platform",
    )
    emit_hourly_runs(emitter, j, days=3, duration_min=6)

    # ============ [REDUNDANT] legacy_daily_revenue vs revenue_features ===== #
    # Two teams computing near-identical revenue features from the same source.
    legacy_rev = emit_dataset(
        emitter,
        "analytics.finance.legacy_revenue_daily",
        [("day", "string"), ("customer_id", "int"), ("revenue", "number"), ("region", "string")],
    )
    rev_features = emit_dataset(
        emitter,
        "ml.features.revenue_features_daily",
        [("day", "string"), ("customer_id", "int"), ("revenue", "number"), ("region", "string")],
    )
    # The canonical ML pipeline has several consumers; the legacy pipeline has one.
    ml_model_a = emit_dataset(
        emitter, "ml.models.churn_features", [("customer_id", "int"), ("revenue", "number")]
    )
    ml_model_b = emit_dataset(
        emitter, "ml.models.ltv_features", [("customer_id", "int"), ("revenue", "number")]
    )
    ml_dash = emit_dataset(
        emitter, "analytics.ml.revenue_monitor", [("day", "string"), ("revenue", "number")]
    )
    legacy_consumer = emit_dataset(
        emitter, "analytics.finance.old_exec_report", [("day", "string"), ("revenue", "number")]
    )
    emit_dataset_lineage(emitter, legacy_rev, [raw_txns])
    emit_dataset_lineage(emitter, rev_features, [raw_txns])
    emit_dataset_lineage(emitter, legacy_consumer, [legacy_rev])  # 1 consumer
    for c in (ml_model_a, ml_model_b, ml_dash):
        emit_dataset_lineage(emitter, c, [rev_features])  # many consumers
    emit_usage(emitter, legacy_rev, 90, queries_per_day=1, users=1)
    emit_usage(emitter, rev_features, 90, queries_per_day=30, users=12)
    j = emit_pipeline(
        emitter,
        "legacy_daily_revenue",
        "Legacy Daily Revenue (Business)",
        "Older revenue table; overlaps the ML team's revenue_features_daily.",
        [raw_txns],
        [legacy_rev],
        "morgan.diaz",
        "finance",
        "0 1 * * *",
        domain="finance",
    )
    emit_runs(emitter, j, DAILY_90, duration_min=40)
    j = emit_pipeline(
        emitter,
        "revenue_features_daily",
        "Revenue Features Daily (ML)",
        "Canonical revenue features consumed across ML.",
        [raw_txns],
        [rev_features],
        "alex.kim",
        "ml-platform",
        "30 1 * * *",
        domain="ml-platform",
    )
    emit_runs(emitter, j, DAILY_90, duration_min=38)

    # ===================== [TRIM] wide_user_profile_build ================== #
    # Builds 15 columns; downstream reads only ~4. Compute wasted on 11 columns.
    wide_cols = [
        ("customer_id", "int"),
        ("email", "string"),
        ("segment", "string"),
        ("ltv", "number"),
    ] + [(f"feature_{i}", "number") for i in range(1, 12)]
    user_wide = emit_dataset(emitter, "ml.features.user_profile_wide", wide_cols)
    wide_consumer = emit_dataset(
        emitter,
        "ml.models.reco_features",
        [("customer_id", "int"), ("segment", "string"), ("ltv", "number")],
    )
    emit_dataset_lineage(emitter, user_wide, [raw_customers, raw_events])
    emit_dataset_lineage(emitter, wide_consumer, [user_wide])
    # field usage shows only 4 of 15 columns are ever queried
    emit_usage(
        emitter,
        user_wide,
        90,
        queries_per_day=12,
        users=5,
        field_counts={"customer_id": 12, "segment": 9, "ltv": 8, "email": 3},
    )
    j = emit_pipeline(
        emitter,
        "wide_user_profile_build",
        "Wide User Profile Build",
        "Computes 15 user features; only a handful are consumed.",
        [raw_customers, raw_events],
        [user_wide],
        "alex.kim",
        "ml-platform",
        "0 2 * * *",
        domain="ml-platform",
    )
    emit_runs(emitter, j, DAILY_90, duration_min=55)

    # ==================== [FIX/FOLD] fraud_scoring_batch =================== #
    # This pipeline fails every night for 30 days without an active response.
    fraud_scores = emit_dataset(
        emitter,
        "analytics.risk.fraud_scores",
        [("txn_id", "int"), ("score", "number"), ("scored_at", "string")],
    )
    fraud_consumer = emit_dataset(
        emitter, "analytics.risk.fraud_review_queue", [("txn_id", "int"), ("score", "number")]
    )
    emit_dataset_lineage(emitter, fraud_scores, [raw_txns])
    emit_dataset_lineage(emitter, fraud_consumer, [fraud_scores])
    emit_usage(emitter, fraud_scores, 90, queries_per_day=5, users=3)
    j = emit_pipeline(
        emitter,
        "fraud_scoring_batch",
        "Fraud Scoring Batch",
        "Scores transactions for fraud; has been failing silently.",
        [raw_txns],
        [fraud_scores],
        "morgan.diaz",
        "risk",
        "0 5 * * *",
        domain="risk",
    )
    emit_runs(emitter, j, list(range(30)), duration_min=15, result=InstanceRunResult.FAILURE)
    emit_runs(emitter, j, list(range(30, 90)), duration_min=15)  # was fine before

    # ==================== [RUNAWAY] realtime_events_agg ==================== #
    # 90 days of ~18-min runs, plus one run STARTED 6h ago and still going.
    events_agg = emit_dataset(
        emitter,
        "analytics.product.events_agg_hourly",
        [("hour", "string"), ("event_type", "string"), ("cnt", "int")],
    )
    events_consumer = emit_dataset(
        emitter, "analytics.product.live_dashboard", [("hour", "string"), ("cnt", "int")]
    )
    emit_dataset_lineage(emitter, events_agg, [raw_events])
    emit_dataset_lineage(emitter, events_consumer, [events_agg])
    emit_usage(emitter, events_agg, 90, queries_per_day=50, users=20)
    j = emit_pipeline(
        emitter,
        "realtime_events_agg",
        "Realtime Events Aggregation",
        "Aggregates product events; normally ~18 min, one run is stuck.",
        [raw_events],
        [events_agg],
        "priya.nair",
        "product",
        "0 * * * *",
        domain="growth",
    )
    emit_runs(emitter, j, DAILY_90, duration_min=18)
    emit_running_now(emitter, j, running_hours=6)  # the runaway

    # ================ [PROTECT] quarterly_compliance_close ================ #
    # Appears dormant after 60 days with sparse queries, but runs quarterly for compliance.
    compliance_out = emit_dataset(
        emitter,
        "analytics.finance.quarterly_close",
        [("quarter", "string"), ("entity", "string"), ("balance", "number")],
    )
    emit_dataset_lineage(emitter, compliance_out, [raw_txns])
    emit_usage(emitter, compliance_out, 90, queries_per_day=0, users=1)
    j = emit_pipeline(
        emitter,
        "quarterly_compliance_close",
        "Quarterly Compliance Close",
        "Regulatory quarterly close. MUST keep running.",
        [raw_txns],
        [compliance_out],
        "sam.torres",
        "finance",
        "0 0 1 */3 *",
        domain="finance",
        tags=["compliance", "retention"],
    )
    emit_runs(emitter, j, [60, 150, 240], duration_min=90)  # ran quarterly

    # =================== [KEEP] nightly_hightouch_sync ==================== #
    # No cataloged consumer and low query volume suggest an orphan, but executable
    # source proves that it writes to a current reverse-ETL destination.
    sync_stage = emit_dataset(
        emitter,
        "analytics.marketing.crm_sync_staging",
        [("customer_id", "int"), ("trait", "string"), ("value", "string")],
    )
    emit_dataset_lineage(emitter, sync_stage, [raw_customers])
    emit_usage(emitter, sync_stage, 90, queries_per_day=0, users=0)
    j = emit_pipeline(
        emitter,
        "nightly_hightouch_sync",
        "Nightly Hightouch Sync",
        "Syncs traits to HubSpot via Hightouch (external, uncataloged consumer).",
        [raw_customers],
        [sync_stage],
        "priya.nair",
        "growth",
        "0 2 * * *",
        domain="marketing",
        tags=["reverse-etl", "has-external-sink"],
    )
    emit_runs(emitter, j, DAILY_90, duration_min=20)

    # ================= [UNKNOWN] ab_test_metrics_daily ==================== #
    # No usage statistics are emitted, so usage is unknown rather than zero.
    ab_metrics = emit_dataset(
        emitter,
        "analytics.growth.ab_test_metrics",
        [("experiment", "string"), ("variant", "string"), ("conversions", "int")],
    )
    emit_dataset_lineage(emitter, ab_metrics, [raw_events])
    # (intentionally NO emit_usage)
    j = emit_pipeline(
        emitter,
        "ab_test_metrics_daily",
        "A/B Test Metrics Daily",
        "Experiment metrics; usage ingestion has not run for this table.",
        [raw_events],
        [ab_metrics],
        "priya.nair",
        "growth",
        "0 6 * * *",
        domain="growth",
    )
    emit_runs(emitter, j, DAILY_90, duration_min=12)

    # =================== [KEEP-NEW] new_signup_features =================== #
    # The pipeline is only five days old; low usage does not imply abandonment.
    new_feats = emit_dataset(
        emitter,
        "ml.features.signup_features",
        [("customer_id", "int"), ("days_since_signup", "int"), ("activated", "int")],
    )
    new_consumer = emit_dataset(
        emitter, "ml.models.activation_model", [("customer_id", "int"), ("activated", "int")]
    )
    emit_dataset_lineage(emitter, new_feats, [raw_customers])
    emit_dataset_lineage(emitter, new_consumer, [new_feats])
    emit_usage(emitter, new_feats, 5, queries_per_day=3, users=2)  # only 5 days of history
    j = emit_pipeline(
        emitter,
        "new_signup_features",
        "New Signup Features",
        "Recently created (5 days ago); low usage is expected.",
        [raw_customers],
        [new_feats],
        "alex.kim",
        "ml-platform",
        "0 2 * * *",
        domain="ml-platform",
    )
    emit_runs(emitter, j, list(range(5)), duration_min=10)

    # ========== [ENTERPRISE KEEP] enterprise_customer_cdc_fanout ========== #
    # A central CDC fanout looks like an unused checkpoint table in DataHub, but
    # executable code publishes customer changes to several non-SQL Kafka consumers.
    cdc_checkpoint = emit_dataset(
        emitter,
        "platform.streaming.customer_cdc_checkpoint",
        [("topic", "string"), ("partition", "int"), ("offset", "int"), ("checkpoint_ts", "string")],
    )
    emit_dataset_lineage(emitter, cdc_checkpoint, [raw_customers])
    emit_usage(emitter, cdc_checkpoint, 90, queries_per_day=0, users=0)
    j = emit_pipeline(
        emitter,
        "enterprise_customer_cdc_fanout",
        "Enterprise Customer CDC Fanout",
        "Publishes customer-change events to operational services; SQL usage is only a checkpoint.",
        [raw_customers],
        [cdc_checkpoint],
        "liam.chen",
        "data-platform",
        "*/5 * * * *",
        domain="data-platform",
    )
    emit_hourly_runs(emitter, j, days=3, duration_min=4)

    # ========== [ENTERPRISE PROTECT] month_end_close_orchestrator ========== #
    # Multi-output monthly finance close: sparse queries are expected and the
    # audit-evidence output exists for SOX control, not interactive consumption.
    close_balances = emit_dataset(
        emitter,
        "analytics.finance.month_end_entity_balances",
        [
            ("period", "string"),
            ("legal_entity", "string"),
            ("currency", "string"),
            ("ending_balance", "number"),
        ],
    )
    close_audit = emit_dataset(
        emitter,
        "governance.finance.month_end_close_evidence",
        [
            ("period", "string"),
            ("control_id", "string"),
            ("status", "string"),
            ("approved_at", "string"),
        ],
    )
    emit_dataset_lineage(emitter, close_balances, [raw_txns])
    emit_dataset_lineage(emitter, close_audit, [raw_txns, close_balances])
    emit_usage(emitter, close_balances, 90, queries_per_day=1, users=4)
    emit_usage(emitter, close_audit, 90, queries_per_day=0, users=1)
    j = emit_pipeline(
        emitter,
        "month_end_close_orchestrator",
        "Month-End Finance Close Orchestrator",
        "Coordinates entity close, FX translation, reconciliation, and immutable audit evidence.",
        [raw_txns],
        [close_balances, close_audit],
        "maya.patel",
        "finance",
        "0 1 1 * *",
        domain="finance",
        tags=["sox", "compliance", "retention"],
    )
    emit_runs(emitter, j, [3, 34, 64], duration_min=135)

    # ======= [ENTERPRISE KEEP] customer_feature_materialization =========== #
    # The offline table has zero SQL consumers, while production model serving
    # reads the same features from Feast's Redis online store.
    feature_snapshot = emit_dataset(
        emitter,
        "ml.features.customer_realtime_snapshot",
        [
            ("customer_id", "int"),
            ("risk_score", "number"),
            ("ltv_30d", "number"),
            ("last_event_ts", "string"),
        ],
    )
    emit_dataset_lineage(emitter, feature_snapshot, [raw_customers, raw_events])
    emit_usage(emitter, feature_snapshot, 90, queries_per_day=0, users=0)
    j = emit_pipeline(
        emitter,
        "customer_feature_materialization",
        "Customer Feature Materialization (Offline + Online)",
        "Builds an offline snapshot and materializes features to the production online store.",
        [raw_customers, raw_events],
        [feature_snapshot],
        "alex.kim",
        "ml-platform",
        "0 * * * *",
        domain="ml-platform",
    )
    emit_hourly_runs(emitter, j, days=3, duration_min=9)

    # ========= [ENTERPRISE KEEP] regional_tax_reconciliation ============== #
    # Structurally resembles revenue pipelines (same input and 80% schema
    # overlap) but applies jurisdiction, FX, and legal-entity rules for filings.
    tax_recon = emit_dataset(
        emitter,
        "analytics.risk.regional_tax_reconciliation",
        [
            ("day", "string"),
            ("customer_id", "int"),
            ("revenue", "number"),
            ("region", "string"),
            ("tax_amount", "number"),
        ],
    )
    tax_filing = emit_dataset(
        emitter,
        "analytics.risk.vat_filing_report",
        [("day", "string"), ("region", "string"), ("revenue", "number"), ("tax_amount", "number")],
    )
    emit_dataset_lineage(emitter, tax_recon, [raw_txns])
    emit_dataset_lineage(emitter, tax_filing, [tax_recon])
    emit_usage(emitter, tax_recon, 90, queries_per_day=8, users=5)
    j = emit_pipeline(
        emitter,
        "regional_tax_reconciliation",
        "Regional Tax Reconciliation",
        "Computes jurisdiction-specific tax and FX adjustments for statutory filings.",
        [raw_txns],
        [tax_recon],
        "maya.patel",
        "risk",
        "0 4 * * *",
        domain="risk",
    )
    emit_runs(emitter, j, DAILY_90, duration_min=52)

    # ========== [ENTERPRISE PROTECT] gdpr_erasure_propagation ============== #
    # A rare privacy-control workflow is intentionally not queried; its value is
    # deleting a subject across SaaS, lake, warehouse, and feature-store systems.
    erasure_audit = emit_dataset(
        emitter,
        "governance.privacy.erasure_audit_log",
        [
            ("request_id", "string"),
            ("subject_id", "string"),
            ("systems_deleted", "string"),
            ("completed_ts", "string"),
        ],
    )
    emit_dataset_lineage(emitter, erasure_audit, [raw_customers])
    emit_usage(emitter, erasure_audit, 90, queries_per_day=0, users=1)
    j = emit_pipeline(
        emitter,
        "gdpr_erasure_propagation",
        "GDPR Erasure Propagation",
        "Propagates verified deletion requests across every customer-data system.",
        [raw_customers],
        [erasure_audit],
        "nora.singh",
        "risk",
        "*/15 * * * *",
        domain="risk",
        tags=["gdpr", "compliance", "pii-retention"],
    )
    emit_runs(emitter, j, list(range(0, 90, 7)), duration_min=8)


SUMMARY = [
    ("daily_revenue", "KEEP", "healthy — feeds mart, real queries"),
    ("customer_360_daily", "KEEP", "healthy — heavily used"),
    ("nightly_marketing_export", "KILL", "orphan — no consumers, 0 queries, owner left"),
    ("hourly_inventory_snapshot", "DOWNSHIFT", "hourly run, weekly consumption"),
    (
        "legacy_daily_revenue",
        "REDUNDANT",
        "duplicates revenue_features_daily (1 vs many consumers)",
    ),
    ("revenue_features_daily", "KEEP", "canonical revenue features"),
    ("wide_user_profile_build", "TRIM", "15 cols built, ~4 read downstream"),
    ("fraud_scoring_batch", "FIX/FOLD", "failing every night, unnoticed"),
    ("realtime_events_agg", "RUNAWAY", "a run started 6h ago vs ~18 min baseline"),
    ("quarterly_compliance_close", "PROTECT", "quarterly control with compliance metadata"),
    ("nightly_hightouch_sync", "KEEP", "executable reverse-ETL sink outside SQL lineage"),
    ("ab_test_metrics_daily", "UNKNOWN", "no usage data — unknown, not zero"),
    ("new_signup_features", "KEEP", "5 days old — new, not abandoned"),
    ("enterprise_customer_cdc_fanout", "KEEP", "hidden Kafka consumers in executable DAG"),
    ("month_end_close_orchestrator", "PROTECT", "multi-output SOX close + audit evidence"),
    ("customer_feature_materialization", "KEEP", "Feast/Redis online consumer is uncataloged"),
    ("regional_tax_reconciliation", "KEEP", "similar schema, distinct tax/FX semantics"),
    ("gdpr_erasure_propagation", "PROTECT", "rare but mandatory privacy deletion workflow"),
]


def main() -> None:
    emitter = get_emitter()
    build(emitter)
    emitter.flush()
    if isinstance(emitter, _DryRunEmitter):
        print(f"DRY RUN OK — constructed {emitter.count} metadata events (nothing sent).")
    else:
        print("Seed complete. 18 pipelines loaded. Open http://localhost:9002.")
    print("\n  pipeline                        expected agent verdict   why")
    print("  " + "-" * 76)
    for name, verdict, why in SUMMARY:
        print(f"  {name:<31} {verdict:<17} {why}")


if __name__ == "__main__":
    main()
