"""Discover and safely load project-local business skills."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


@dataclass(frozen=True)
class BusinessSkill:
    name: str
    description: str
    instructions: str
    path: Path

    def summary(self) -> dict:
        return {"name": self.name, "description": self.description}


def _parse_skill(path: Path) -> BusinessSkill:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    if len(lines) < 4 or lines[0].strip() != "---":
        raise ValueError(f"{path}: missing YAML frontmatter")
    try:
        closing = lines.index("---", 1)
    except ValueError as exc:
        raise ValueError(f"{path}: unterminated YAML frontmatter") from exc

    metadata: Dict[str, str] = {}
    for line in lines[1:closing]:
        if not line.strip() or ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.strip().strip('"').strip("'")
    name = metadata.get("name", "")
    description = metadata.get("description", "")
    if not name or not description:
        raise ValueError(f"{path}: frontmatter requires name and description")
    if path.parent.name != name:
        raise ValueError(f"{path}: skill name must match its directory")
    return BusinessSkill(name, description, "\n".join(lines[closing + 1 :]).strip(), path)


class SkillRegistry:
    """Discover only direct child ``SKILL.md`` files beneath a trusted root."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        if not self.root.is_dir():
            raise ValueError(f"Skills directory does not exist: {self.root}")
        skills: Dict[str, BusinessSkill] = {}
        for path in sorted(self.root.glob("*/SKILL.md")):
            skill = _parse_skill(path)
            if skill.name in skills:
                raise ValueError(f"Duplicate skill name: {skill.name}")
            skills[skill.name] = skill
        if not skills:
            raise ValueError(f"No SKILL.md files found beneath {self.root}")
        self._skills = skills

    def summaries(self) -> List[dict]:
        return [self._skills[name].summary() for name in sorted(self._skills)]

    def load(self, name: str) -> BusinessSkill:
        try:
            return self._skills[name]
        except KeyError as exc:
            available = ", ".join(sorted(self._skills))
            raise KeyError(f"Unknown business skill {name!r}. Available: {available}") from exc

    def __len__(self) -> int:
        return len(self._skills)
