from pathlib import Path

from flowjury.agent.skills import SkillRegistry


ROOT = Path(__file__).resolve().parents[2]


def test_business_skills_are_discoverable():
    registry = SkillRegistry(ROOT / "skills")
    names = [item["name"] for item in registry.summaries()]

    assert len(registry) == 9
    assert names == sorted(names)
    assert "decide-pipeline-verdict" in names
    assert "handle-uncertain-pipelines" in names
    assert "use-investigation-memory" in names


def test_enterprise_policy_clues_are_documented():
    registry = SkillRegistry(ROOT / "skills")

    uncertainty = registry.load("handle-uncertain-pipelines").instructions
    redundancy = registry.load("consolidate-redundant-pipelines").instructions
    protection = registry.load("protect-regulated-pipelines").instructions
    memory = registry.load("use-investigation-memory").instructions

    assert "Kafka" in uncertainty
    assert "Feast" in uncertainty
    assert "jurisdiction" in redundancy
    assert "privacy" in protection
    assert "historical evidence" in memory
    assert "blast-radius" in memory


def test_unknown_skill_is_rejected():
    registry = SkillRegistry(ROOT / "skills")

    try:
        registry.load("does-not-exist")
    except KeyError:
        return
    raise AssertionError("unknown skills must be rejected")
