"""Readable, dependency-free terminal rendering with optional ANSI color."""

from __future__ import annotations

import os
import shutil
import sys
import textwrap
from typing import Any, Optional


RESET = "\033[0m"
BOLD = "1"
DIM = "2"
RED = "31"
GREEN = "32"
YELLOW = "33"
BLUE = "34"
MAGENTA = "35"
CYAN = "36"
WHITE = "37"
BRIGHT_BLACK = "90"
BRIGHT_RED = "91"
BRIGHT_GREEN = "92"
BRIGHT_YELLOW = "93"
BRIGHT_BLUE = "94"
BRIGHT_MAGENTA = "95"
BRIGHT_CYAN = "96"

_COLOR_OVERRIDE: Optional[bool] = None

VERDICT_STYLES = {
    "KEEP": ("✓", BRIGHT_GREEN),
    "KILL": ("✕", BRIGHT_RED),
    "REDUNDANT": ("≋", BRIGHT_YELLOW),
    "TRIM": ("✂", BRIGHT_CYAN),
    "DOWNSHIFT": ("↓", BRIGHT_BLUE),
    "RUNAWAY": ("⚠", BRIGHT_RED),
    "PROTECT": ("◆", BRIGHT_MAGENTA),
    "FIX/FOLD": ("↻", YELLOW),
    "UNKNOWN": ("?", BRIGHT_BLACK),
}

ACTIVITY_STYLES = {
    "supervisor": BRIGHT_BLUE,
    "executor": BRIGHT_CYAN,
    "memory": BRIGHT_MAGENTA,
    "warning": BRIGHT_YELLOW,
    "success": BRIGHT_GREEN,
    "info": CYAN,
}


def configure_color(enabled: Optional[bool]) -> None:
    """Override automatic color detection; ``None`` restores auto mode."""
    global _COLOR_OVERRIDE
    _COLOR_OVERRIDE = enabled


def _color_enabled(force: Optional[bool] = None) -> bool:
    if force is not None:
        return force
    if _COLOR_OVERRIDE is not None:
        return _COLOR_OVERRIDE
    return (
        sys.stdout.isatty()
        and os.environ.get("TERM", "") != "dumb"
        and "NO_COLOR" not in os.environ
    )


def _paint(text: str, *codes: str, color: Optional[bool] = None) -> str:
    if not _color_enabled(color) or not codes:
        return text
    return f"\033[{';'.join(codes)}m{text}{RESET}"


def _terminal_width(width: Optional[int]) -> int:
    detected = width or shutil.get_terminal_size((100, 24)).columns
    return min(max(detected, 72), 116)


def _wrapped(text: Any, width: int, indent: str = "  ") -> list[str]:
    normalized = " ".join(str(text or "").split())
    return textwrap.wrap(
        normalized,
        width=max(30, width - len(indent)),
        initial_indent=indent,
        subsequent_indent=indent,
    ) or [indent.rstrip()]


def activity(message: str, kind: str = "info", *, color: Optional[bool] = None) -> str:
    """Color one concise runtime event without changing its text."""
    return _paint(message, ACTIVITY_STYLES.get(kind, WHITE), color=color)


def render_run_banner(
    pipeline_count: int, *, color: Optional[bool] = None, width: Optional[int] = None
) -> str:
    """Render the run-level title and activity legend."""
    width = _terminal_width(width)
    title = f" FLOWJURY  •  {pipeline_count} PIPELINE{'S' if pipeline_count != 1 else ''} "
    border = "═" * max(1, width - len(title) - 2)
    lines = [
        _paint(f"╔═{title}{border}╗", BOLD, BRIGHT_MAGENTA, color=color),
        _paint(
            "  🧭 Supervisor   🔍 Executor   🧠 Memory   ⚠ Validator   ✓ Accepted",
            DIM,
            color=color,
        ),
    ]
    return "\n".join(lines)


def render_assessment_header(
    pipeline: str,
    position: int,
    total: int,
    *,
    color: Optional[bool] = None,
    width: Optional[int] = None,
) -> str:
    """Render a strong visual boundary before one pipeline investigation."""
    width = _terminal_width(width)
    label = f" {position:02d}/{total:02d}  ASSESSING  {pipeline} "
    line = label + "━" * max(1, width - len(label))
    return "\n" + _paint(line[:width], BOLD, BRIGHT_BLUE, color=color)


def _section(
    lines: list[str],
    title: str,
    body: Any,
    tone: str,
    width: int,
    color: Optional[bool],
) -> None:
    lines.append("")
    lines.append(_paint(f"  {title}", BOLD, tone, color=color))
    lines.extend(_wrapped(body, width, "    "))


def render_recommendation(
    result: Any, *, color: Optional[bool] = None, width: Optional[int] = None
) -> str:
    """Render one recommendation as a scannable, color-coded decision card."""
    width = _terminal_width(width)
    verdict = str(result.recommendation).upper()
    symbol, verdict_color = VERDICT_STYLES.get(verdict, ("•", WHITE))
    confidence = f"{float(result.confidence) * 100:.0f}%"
    status = getattr(result, "investigation_status", "COMPLETED")
    decision = f" {symbol}  {verdict:<10}  CONFIDENCE {confidence:<4}  STATUS {status} "
    inner_width = width - 2

    lines = [
        _paint("┏" + "━" * inner_width + "┓", BOLD, verdict_color, color=color),
        _paint(
            "┃" + decision[:inner_width].ljust(inner_width) + "┃",
            BOLD,
            verdict_color,
            color=color,
        ),
        _paint("┗" + "━" * inner_width + "┛", BOLD, verdict_color, color=color),
    ]

    _section(lines, "WHY THIS VERDICT", result.summary, WHITE, width, color)

    skills = ", ".join(result.skills_applied) if result.skills_applied else "(fallback only)"
    _section(lines, "SKILLS APPLIED", skills, MAGENTA, width, color)

    lines.append("")
    lines.append(_paint(f"  EVIDENCE  ({len(result.evidence)} citations)", BOLD, CYAN, color=color))
    for index, item in enumerate(result.evidence, 1):
        source = item.get("source_tool", "unknown")
        claim = item.get("claim", "")
        observation = item.get("observation", "")
        lines.extend(_wrapped(f"{index}. [{source}] {claim}", width, "    "))
        lines.extend(_wrapped(f"→ {observation}", width, "       "))

    if result.risks:
        lines.append("")
        lines.append(_paint("  PRIMARY RISKS", BOLD, BRIGHT_YELLOW, color=color))
        for risk in result.risks:
            lines.extend(_wrapped(f"⚠ {risk}", width, "    "))

    temporal = getattr(result, "temporal_change", None)
    if temporal:
        memory_status = temporal.get("status", "UNKNOWN")
        changed = [
            item.get("field") for item in temporal.get("changed_fields", []) if item.get("field")
        ]
        memory_text = memory_status
        if changed:
            memory_text += " • changed: " + ", ".join(changed[:5])
        _section(lines, "MEMORY", memory_text, BLUE, width, color)

    review = getattr(result, "skeptic_review", None)
    if review:
        review_text = f"{review.get('decision', 'BLOCK')}: {review.get('summary', '')}"
        _section(lines, "SKEPTIC REVIEW", review_text, BRIGHT_MAGENTA, width, color)

    _section(lines, "NEXT ACTION", result.next_action, BRIGHT_GREEN, width, color)

    memory_id = getattr(result, "memory_id", None)
    if memory_id:
        lines.append("")
        lines.append(_paint(f"  AUDIT EPISODE  {memory_id}", DIM, BRIGHT_BLACK, color=color))
    return "\n".join(lines)
