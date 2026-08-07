"""Add executable DAG-source clues to the seeded demo pipelines
to investigate. Merges a `dag_source` property into each DataFlow (keeps the
schedule and everything else). Run once after ``seed_demo_catalog.py``.

    conda activate datahub
    export DATAHUB_GMS_URL=http://localhost:8080
    python -m scripts.add_demo_dag_sources
"""

import os

from datahub.emitter.mce_builder import make_data_flow_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.metadata.schema_classes import DataFlowInfoClass

GMS = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
TOKEN = os.environ.get("DATAHUB_GMS_TOKEN")

# Several cases reveal consumers or obligations the catalog alone cannot see,
# so the agent must inspect executable source rather than trust a usage heuristic.
DAG_SOURCE = {
    "daily_revenue": (
        'df = spark.table("raw.finance.transactions")\n'
        'agg = df.groupBy("txn_date","region").agg(\n'
        '    F.sum("amount").alias("gross_revenue"), F.count("*").alias("txn_count"))\n'
        'agg.write.mode("overwrite").saveAsTable("analytics.finance.daily_revenue_summary")'
    ),
    "customer_360_daily": (
        'c = spark.table("raw.crm.customers"); e = spark.table("raw.product.events")\n'
        'prof = c.join(e, "customer_id").groupBy("customer_id").agg(...)\n'
        'prof.write.mode("overwrite").saveAsTable("analytics.growth.customer_360")'
    ),
    "nightly_marketing_export": (
        "# Built for the Q1 winback campaign (owner: dana.lee)\n"
        'c = spark.table("raw.crm.customers").select("customer_id","email","region")\n'
        'c.write.mode("overwrite").saveAsTable("analytics.marketing.customer_export")'
    ),
    "hourly_inventory_snapshot": (
        'inv = spark.table("raw.ops.inventory")\n'
        'inv.write.mode("overwrite").saveAsTable("analytics.ops.inventory_snapshot")'
    ),
    "legacy_daily_revenue": (
        't = spark.table("raw.finance.transactions")\n'
        't.groupBy("txn_date","customer_id","region").agg(F.sum("amount").alias("revenue")) \\\n'
        '  .write.mode("overwrite").saveAsTable("analytics.finance.legacy_revenue_daily")'
    ),
    "revenue_features_daily": (
        't = spark.table("raw.finance.transactions")\n'
        'feats = t.groupBy("txn_date","customer_id","region")'
        '.agg(F.sum("amount").alias("revenue"))\n'
        'feats.write.mode("overwrite").saveAsTable("ml.features.revenue_features_daily")'
    ),
    "wide_user_profile_build": (
        'c = spark.table("raw.crm.customers"); e = spark.table("raw.product.events")\n'
        "# builds 15 cols: customer_id, email, segment, ltv, feature_1..feature_11\n"
        "wide = build_features(c, e)\n"
        'wide.write.mode("overwrite").saveAsTable("ml.features.user_profile_wide")'
    ),
    "quarterly_compliance_close": (
        "# SOX quarterly close — retained per finance policy\n"
        't = spark.table("raw.finance.transactions")\n'
        't.groupBy("quarter","entity").agg(F.sum("amount").alias("balance")) \\\n'
        '  .write.mode("overwrite").saveAsTable("analytics.finance.quarterly_close")'
    ),
    "nightly_hightouch_sync": (
        "import hightouch\n"
        'c = spark.table("raw.crm.customers")\n'
        'traits = c.select("customer_id","email","region")\n'
        'traits.write.mode("overwrite").saveAsTable("analytics.marketing.crm_sync_staging")\n'
        "# push customer traits to HubSpot via Hightouch reverse-ETL (external, uncataloged)\n"
        'hightouch.trigger_sync(sync_id="hs_crm_traits_prod",\n'
        '                       source="analytics.marketing.crm_sync_staging")'
    ),
    "fraud_scoring_batch": (
        't = spark.table("raw.finance.transactions")\n'
        'scored = model.transform(t).select("txn_id","score", '
        'F.current_timestamp().alias("scored_at"))\n'
        'scored.write.mode("overwrite").saveAsTable("analytics.risk.fraud_scores")'
    ),
    "realtime_events_agg": (
        'e = spark.table("raw.product.events")\n'
        'agg = e.groupBy(F.window("event_ts","1 hour"),"event_type").count()\n'
        'agg.write.mode("overwrite").saveAsTable("analytics.product.events_agg_hourly")'
    ),
    "ab_test_metrics_daily": (
        'e = spark.table("raw.product.events")\n'
        "metrics = compute_experiment_metrics(e)  # experiment, variant, conversions\n"
        'metrics.write.mode("overwrite").saveAsTable("analytics.growth.ab_test_metrics")\n'
        "# read by the Experimentation dashboard in Looker (not wired into catalog usage)"
    ),
    "new_signup_features": (
        'c = spark.table("raw.crm.customers")\n'
        'feats = c.select("customer_id", days_since_signup("signup_ts"), activated_flag())\n'
        'feats.write.mode("overwrite").saveAsTable("ml.features.signup_features")'
    ),
    "enterprise_customer_cdc_fanout": (
        "from confluent_kafka import Producer\n"
        "changes = read_customer_cdc_stream("
        'checkpoint="platform.streaming.customer_cdc_checkpoint")\n'
        "producer = Producer(kafka_prod_config())\n"
        "for event in changes.toLocalIterator():\n"
        '    for topic in ("fraud.customer-updates", "support.customer-updates",\n'
        '                  "search.customer-index-updates"):\n'
        "        producer.produce(topic, key=event.customer_id, value=event.as_json())\n"
        "producer.flush(); persist_offsets(changes)"
    ),
    "month_end_close_orchestrator": (
        'txns = spark.table("raw.finance.transactions")\n'
        "fx = load_month_end_fx_rates(period=close_period())\n"
        "balances = translate_and_reconcile_by_legal_entity(txns, fx)\n"
        'balances.write.mode("overwrite").saveAsTable(\n'
        '    "analytics.finance.month_end_entity_balances")\n'
        'evidence = run_sox_controls(balances, controls=["FIN-07", "FIN-12", "FIN-19"])\n'
        'evidence.write.mode("append").saveAsTable(\n'
        '    "governance.finance.month_end_close_evidence")\n'
        "require_controller_approval(evidence)"
    ),
    "customer_feature_materialization": (
        "from feast import FeatureStore\n"
        "features = build_customer_features(\n"
        '    spark.table("raw.crm.customers"), spark.table("raw.product.events"))\n'
        'features.write.mode("overwrite").saveAsTable(\n'
        '    "ml.features.customer_realtime_snapshot")\n'
        "# The production model reads Redis through Feast, not the Snowflake snapshot.\n"
        'store = FeatureStore(repo_path="feature_repo/customer_prod")\n'
        "store.materialize_incremental(end_date=utcnow())"
    ),
    "regional_tax_reconciliation": (
        'txns = spark.table("raw.finance.transactions")\n'
        'fx = spark.table("reference.finance.daily_fx_rates")\n'
        'rules = spark.table("reference.tax.jurisdiction_rules")\n'
        'recon = apply_statutory_tax_and_fx(txns.join(fx, "day").join(rules, "region"))\n'
        'recon.select("day", "customer_id", "revenue", "region", "tax_amount") \\\n'
        '  .write.mode("overwrite").saveAsTable("analytics.risk.regional_tax_reconciliation")\n'
        "# Similar to revenue aggregation structurally, but values are filing-specific."
    ),
    "gdpr_erasure_propagation": (
        'requests = privacy_queue.read_verified_requests(regulation="GDPR")\n'
        "for req in requests:\n"
        "    warehouse.delete_subject(req.subject_id)\n"
        "    lakehouse.expire_subject_files(req.subject_id)\n"
        "    crm.delete_contact(req.subject_id)\n"
        "    feature_store.delete_entity(req.subject_id)\n"
        '    write_immutable_audit(req, table="governance.privacy.erasure_audit_log")\n'
        "privacy_queue.mark_complete(requests)"
    ),
}

graph = DataHubGraph(DatahubClientConfig(server=GMS, token=TOKEN))
emitter = DatahubRestEmitter(gms_server=GMS, token=TOKEN)

for flow_id, code in DAG_SOURCE.items():
    flow_urn = make_data_flow_urn("airflow", flow_id, "PROD")
    info = graph.get_aspect(flow_urn, DataFlowInfoClass)
    if info is None:
        print("skip (not found):", flow_id)
        continue
    props = dict(info.customProperties or {})
    props["dag_source"] = code
    info.customProperties = props
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=flow_urn, aspect=info))
    print("added dag_source ->", flow_id)

print("\nDone. The agent can now read each pipeline's code via get_dag_source.")
