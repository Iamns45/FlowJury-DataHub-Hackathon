import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ENTERPRISE_EXPECTED = {
    "enterprise_customer_cdc_fanout": "KEEP",
    "month_end_close_orchestrator": "PROTECT",
    "customer_feature_materialization": "KEEP",
    "regional_tax_reconciliation": "KEEP",
    "gdpr_erasure_propagation": "PROTECT",
}


def assigned_literal(path: Path, name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {path}")


def test_seed_contains_18_unique_pipeline_scenarios():
    summary = assigned_literal(ROOT / "scripts" / "seed_demo_catalog.py", "SUMMARY")
    ids = [row[0] for row in summary]

    assert len(summary) == 18
    assert len(ids) == len(set(ids))
    assert {row[0]: row[1] for row in summary}.items() >= ENTERPRISE_EXPECTED.items()


def test_every_scenario_has_investigable_dag_source():
    summary = assigned_literal(ROOT / "scripts" / "seed_demo_catalog.py", "SUMMARY")
    dag_sources = assigned_literal(
        ROOT / "scripts" / "add_demo_dag_sources.py",
        "DAG_SOURCE",
    )

    assert {row[0] for row in summary} == set(dag_sources)
    assert "Producer" in dag_sources["enterprise_customer_cdc_fanout"]
    assert "FeatureStore" in dag_sources["customer_feature_materialization"]
    assert "jurisdiction_rules" in dag_sources["regional_tax_reconciliation"]
    assert "delete_subject" in dag_sources["gdpr_erasure_propagation"]
