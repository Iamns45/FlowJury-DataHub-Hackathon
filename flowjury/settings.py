"""Application paths and DataHub connection settings.

Reasoning-model configuration remains inside its transport adapter so provider
details do not leak into the rest of the application.
"""

from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SKILLS_DIR = PROJECT_ROOT / "skills"
DEFAULT_MEMORY_DB = PROJECT_ROOT / ".flowjury" / "memory.sqlite3"

DATAHUB_GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_GMS_TOKEN = os.environ.get("DATAHUB_GMS_TOKEN")

DEFAULT_MAX_PLANNING_CYCLES = 8
MAX_REPAIR_ATTEMPTS = 2
MAX_TOOL_OUTPUT = 6000
RISKY_VERDICTS = frozenset({"KILL", "REDUNDANT"})
