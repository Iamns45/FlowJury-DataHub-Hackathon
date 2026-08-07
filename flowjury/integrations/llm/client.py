"""Provider-neutral configuration and transport adapter for the reasoning model."""

from __future__ import annotations

import os


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

    return Anthropic(api_key=LLM_API_KEY)
