"""Architecture guardrails for keeping the repository understandable."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_root_contains_only_one_compatibility_python_entrypoint():
    assert sorted(path.name for path in ROOT.glob("*.py")) == ["agent.py"]


def test_responsibility_based_packages_exist():
    expected = {
        "agent",
        "analysis",
        "domain",
        "integrations",
        "memory",
    }
    actual = {
        path.name
        for path in (ROOT / "flowjury").iterdir()
        if path.is_dir() and not path.name.startswith("__")
    }
    assert actual >= expected


def test_demo_and_documentation_assets_are_separated_from_runtime():
    assert (ROOT / "scripts" / "seed_demo_catalog.py").is_file()
    assert (ROOT / "docs" / "architecture" / "system-overview.drawio").is_file()
    assert (ROOT / "docs" / "examples" / "enterprise_scenarios.md").is_file()
