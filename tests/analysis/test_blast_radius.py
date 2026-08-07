"""Tests for conservative retirement impact analysis."""

from flowjury.analysis.blast_radius import summarize_blast_radius


def test_blast_radius_finds_catalog_and_source_consumers():
    output = "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.output,PROD)"
    lineage = {
        output: {
            "entities": [
                "urn:li:dashboard:(looker,finance-close)",
                "urn:li:mlModel:(fraud-score,PROD)",
            ]
        }
    }
    report = summarize_blast_radius(
        [output],
        lineage,
        'producer.produce("fraud.customer-updates", value=event)',
        protected=False,
    )

    assert report["cataloged_downstream_assets"] == 2
    assert "Kafka or event stream" in report["hidden_sink_signals"]
    assert report["safe_to_retire"] is False
    assert report["conclusion"] == "BLOCKED_BY_KNOWN_IMPACT"


def test_no_known_impact_remains_inconclusive():
    report = summarize_blast_radius(
        ["urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.output,PROD)"],
        {},
        "spark.table('raw').write.saveAsTable('prod.output')",
        protected=False,
    )

    assert report["safe_to_retire"] is None
    assert report["conclusion"] == "INCONCLUSIVE_NO_KNOWN_IMPACT"
