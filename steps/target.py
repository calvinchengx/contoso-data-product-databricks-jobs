"""This platform's policy on top of the published databricks-target contract.

THE CONTRACT IS NOT WRITTEN HERE. It is `databricks-target`, published from
databricks-emulator's release and installed by this repo. This file adds only
the decisions that are this platform's: warehouse name, catalog name, where
landing lives, whether seed_secrets may run.
"""

from __future__ import annotations

import os
from pathlib import Path

import databricks_target

WORKSPACE = "contoso-analytics"
WAREHOUSE = "contoso_warehouse"
CATALOG = "contoso"
LANDING_NAME = "landing"
TABLES_NAME = "tables"
ROOT = Path(__file__).resolve().parent.parent


def T():
    os.environ.setdefault("DATABRICKS_EMULATOR_URL", "http://127.0.0.1:18470")
    os.environ.setdefault("DATABRICKS_DATA_DIR", str(ROOT / "data"))
    os.environ.setdefault("DATABRICKS_SPARK_CONNECT_URL", "http://127.0.0.1:18170")
    os.environ.setdefault("DATABRICKS_UC_URL", "http://127.0.0.1:18471")
    os.environ.setdefault("DATABRICKS_WAREHOUSE", WAREHOUSE)
    os.environ.setdefault("OM_URL", "http://127.0.0.1:18585/api/v1")
    if not os.environ.get("DATABRICKS_TOKEN"):
        tok = _emulator_pat()
        if tok:
            os.environ["DATABRICKS_TOKEN"] = tok
    return databricks_target.target()


def _emulator_pat() -> str:
    """The workspace token, placed here by whichever platform is running us.

    A PRODUCT DOES NOT REACH INTO A PLATFORM. This function used to shell out
    to `docker compose cp` against the platform's compose files when the file
    was missing -- which worked only while the product lived inside the
    platform repository, and broke the moment the two were separated: the
    product's ROOT has no compose/ directory, and never should.

    That coupling is the thing the split was for. A Databricks Job does not
    know how its workspace was started; it is handed a credential. So the
    platform delivers one (`make token` copies it out of the emulator, and
    `make verify` depends on that), and this reads it.

    On a real target there is nothing to seed: DATABRICKS_TOKEN is already in
    the environment and `T()` never calls this.
    """
    pat = ROOT / "data" / "admin.pat"
    try:
        return pat.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SystemExit(
            f"no workspace token at {pat} ({exc}).\n\n"
            f"On the emulator the platform places it there -- run the pipeline "
            f"through the platform (`make verify PRODUCT=...`), which depends "
            f"on its `token` step. On a real workspace, export DATABRICKS_TOKEN."
        ) from exc


def landing_path() -> str:
    """Engine-visible landing directory. Name-based; the scheme is the target's."""
    root = os.environ.get("CONTOSO_DELTA", "/data/delta")
    return f"{root}/{LANDING_NAME}"


def tables_path() -> str:
    root = os.environ.get("CONTOSO_DELTA", "/data/delta")
    return f"{root}/{TABLES_NAME}"


def host_delta() -> Path:
    """The same volume, as the operator's host sees it."""
    return Path(os.environ.get("DELTA_DATA", "/tmp/contoso-dbx-delta"))
