"""Prompt-safe serialization helpers shared by agent components."""

from __future__ import annotations

import json
from enum import Enum
from typing import Any

from flowjury.settings import MAX_TOOL_OUTPUT


def to_jsonable(value: Any) -> Any:
    """Convert enum-containing structures into stable JSON-compatible values."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    return value


def clip_tool_output(value: Any, limit: int = MAX_TOOL_OUTPUT) -> str:
    """Serialize and cap tool output before adding it to model context."""
    text = value if isinstance(value, str) else json.dumps(to_jsonable(value), default=str)
    return text[:limit] + (" …(truncated)" if len(text) > limit else "")
