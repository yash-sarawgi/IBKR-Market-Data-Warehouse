"""Compatibility wrapper for the generic breadth washout strategy."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover - direct script bootstrap only
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from strategies.breadth_washout import main
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    from breadth_washout import main


if __name__ == "__main__":
    main()
