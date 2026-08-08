"""Provider-neutral configuration and transport adapter for the reasoning model."""

from __future__ import annotations

import os
from typing import Any


LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_NAME = os.environ.get("LLM_NAME", "")


def _configured_temperature() -> float:
    raw = os.environ.get("LLM_TEMPERATURE", "0")
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError("LLM_TEMPERATURE must be a number from 0 to 1") from exc
    if not 0 <= value <= 1:
        raise ValueError("LLM_TEMPERATURE must be a number from 0 to 1")
    return value


# Pipeline verdicts are analytical decisions, so repeatability is the safe default. Teams can
# still override this through the provider-neutral public configuration contract.
LLM_TEMPERATURE = _configured_temperature()


def _temperature_not_supported(exc: Exception) -> bool:
    """Recognize model APIs that reject sampling-temperature controls."""
    message = str(exc).lower()
    markers = ("deprecated", "not supported", "unsupported", "not allowed")
    return "temperature" in message and any(marker in message for marker in markers)


class _MessagesAdapter:
    """Retry once without temperature when the selected model does not accept it."""

    def __init__(self, messages: Any):
        self._messages = messages
        self._temperature_supported = True

    def create(self, **kwargs):
        request = dict(kwargs)
        if not self._temperature_supported:
            request.pop("temperature", None)
        try:
            return self._messages.create(**request)
        except Exception as exc:
            if "temperature" not in request or not _temperature_not_supported(exc):
                raise
            self._temperature_supported = False
            request.pop("temperature")
            print("  ℹ selected model ignores temperature; retrying with model defaults")
            return self._messages.create(**request)


class LLMTransportAdapter:
    """Expose the provider transport through FlowJury's stable client contract."""

    def __init__(self, provider_client: Any):
        self._provider_client = provider_client
        self.messages = _MessagesAdapter(provider_client.messages)

    def __getattr__(self, name: str):
        return getattr(self._provider_client, name)


def llm_configured() -> bool:
    return bool(LLM_API_KEY and LLM_NAME)


def missing_llm_configuration() -> list[str]:
    missing = []
    if not LLM_API_KEY:
        missing.append("LLM_API_KEY")
    if not LLM_NAME:
        missing.append("LLM_NAME")
    return missing


def create_llm_client():
    """Build the installed transport adapter using neutral FlowJury settings."""
    if not llm_configured():
        raise RuntimeError("Missing LLM configuration: " + ", ".join(missing_llm_configuration()))
    # Keep provider-specific transport details behind this one adapter boundary.
    from anthropic import Anthropic

    return LLMTransportAdapter(Anthropic(api_key=LLM_API_KEY))
