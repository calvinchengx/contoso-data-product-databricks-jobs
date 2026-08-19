"""End-to-end: provision → seed → ingest → bronze → silver → register → gold → govern."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
STEPS = (
    "provision",
    "seed_secrets",
    "ingest",
    "bronze",
    "silver",
    "register",
    "gold",
    "govern",
)


def main() -> int:
    sys.path.insert(0, str(ROOT))
    for name in STEPS:
        print(f"==> {name}", flush=True)
        runpy.run_path(str(ROOT / f"{name}.py"), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
