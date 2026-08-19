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
    """Seeded PAT. The published image writes it 0600 as nonroot; read via compose if needed."""
    pat = ROOT / "data" / "admin.pat"
    try:
        return pat.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    import subprocess

    dest = ROOT / "data" / "admin.pat"
    dest.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "docker",
        "compose",
        "--env-file",
        "versions.env",
        "--profile",
        "governance",
        "-f",
        "compose/docker-compose.yml",
        "-f",
        "compose/governance.yml",
        "cp",
        "databricks:/data/admin.pat",
        str(dest),
    ]
    subprocess.check_call(cmd, cwd=ROOT)
    return dest.read_text(encoding="utf-8").strip()


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
