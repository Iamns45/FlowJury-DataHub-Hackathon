"""Compatibility entry point for existing ``python agent.py`` workflows.

The canonical entry point is ``python -m flowjury``. This wrapper preserves
existing commands and documentation that reference ``agent.py``.
"""

from flowjury.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
