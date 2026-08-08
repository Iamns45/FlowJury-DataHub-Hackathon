import importlib
import os
from pathlib import Path
from types import SimpleNamespace

from flowjury.integrations.llm import client as llm


ROOT = Path(__file__).resolve().parents[2]


def test_generic_llm_environment_contract():
    old_key = os.environ.get("LLM_API_KEY")
    old_name = os.environ.get("LLM_NAME")
    old_temperature = os.environ.get("LLM_TEMPERATURE")
    try:
        os.environ["LLM_API_KEY"] = "test-key"
        os.environ["LLM_NAME"] = "test-llm"
        os.environ["LLM_TEMPERATURE"] = "0"
        module = importlib.reload(llm)
        assert module.llm_configured()
        assert module.missing_llm_configuration() == []
        assert module.LLM_TEMPERATURE == 0
    finally:
        if old_key is None:
            os.environ.pop("LLM_API_KEY", None)
        else:
            os.environ["LLM_API_KEY"] = old_key
        if old_name is None:
            os.environ.pop("LLM_NAME", None)
        else:
            os.environ["LLM_NAME"] = old_name
        if old_temperature is None:
            os.environ.pop("LLM_TEMPERATURE", None)
        else:
            os.environ["LLM_TEMPERATURE"] = old_temperature
        importlib.reload(llm)


def test_temperature_is_provider_neutral_and_configurable():
    old_temperature = os.environ.get("LLM_TEMPERATURE")
    try:
        os.environ["LLM_TEMPERATURE"] = "0.2"
        module = importlib.reload(llm)
        assert module.LLM_TEMPERATURE == 0.2
    finally:
        if old_temperature is None:
            os.environ.pop("LLM_TEMPERATURE", None)
        else:
            os.environ["LLM_TEMPERATURE"] = old_temperature
        importlib.reload(llm)


def test_transport_retries_without_temperature_and_remembers_capability():
    class FakeMessages:
        def __init__(self):
            self.calls = []

        def create(self, **kwargs):
            self.calls.append(kwargs)
            if "temperature" in kwargs:
                raise RuntimeError("`temperature` is deprecated for this model")
            return "accepted"

    provider_messages = FakeMessages()
    client = llm.LLMTransportAdapter(SimpleNamespace(messages=provider_messages))

    assert client.messages.create(model="test", temperature=0) == "accepted"
    assert client.messages.create(model="test", temperature=0) == "accepted"
    assert len(provider_messages.calls) == 3
    assert "temperature" in provider_messages.calls[0]
    assert "temperature" not in provider_messages.calls[1]
    assert "temperature" not in provider_messages.calls[2]


def test_transport_does_not_retry_unrelated_model_errors():
    class FakeMessages:
        def __init__(self):
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            raise RuntimeError("invalid model name")

    provider_messages = FakeMessages()
    client = llm.LLMTransportAdapter(SimpleNamespace(messages=provider_messages))

    try:
        client.messages.create(model="missing", temperature=0)
    except RuntimeError as exc:
        assert "invalid model name" in str(exc)
    else:
        raise AssertionError("unrelated provider errors must propagate")
    assert provider_messages.calls == 1


def test_no_provider_specific_environment_names_in_runtime_entrypoints():
    combined = "\n".join(
        (ROOT / name).read_text(encoding="utf-8")
        for name in (
            "agent.py",
            "flowjury/cli.py",
            "flowjury/integrations/datahub/writeback.py",
        )
    )
    assert "ANTHROPIC_API_KEY" not in combined
    assert "FLOWJURY_MODEL" not in combined


def test_env_example_uses_neutral_names():
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "LLM_API_KEY=" in example
    assert "LLM_NAME=" in example
    assert "LLM_TEMPERATURE=0" in example
