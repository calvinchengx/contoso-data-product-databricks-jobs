"""What this leaf must be, independent of the platform that runs it."""

from __future__ import annotations

import ast
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
STEPS = ROOT / "steps"


def test_the_product_is_imported_not_restated():
    """Bronze, silver and every line of gold SQL come from core.

    A second `fct_sales.sql` is how "one data product, many engines" stops
    being true. This leaf binds the product to Databricks; it does not
    reimplement it.
    """
    bronze = (STEPS / "bronze.py").read_text(encoding="utf-8")
    silver = (STEPS / "silver.py").read_text(encoding="utf-8")
    assert "from contoso_product import run_bronze" in bronze
    assert "from contoso_product import run_silver" in silver
    assert "decimal(19,4)" not in silver, "a money type is being restated here"


def test_no_dependency_comes_from_a_sibling_checkout():
    """This leaf must clone and build on its own — DoD item 1.

    A `path = "../…"` source is invisible to everyone who already has the
    siblings on disk and fails for everyone who does not. It also leaves the
    repo with no version to bump, which is how snowflake-platform-tasks went
    past two consecutive core releases without anything failing.
    """
    proj = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    offenders = [
        line.strip()
        for line in proj.splitlines()
        if "path = " in line and "../" in line and not line.lstrip().startswith("#")
    ]
    assert not offenders, "a dependency resolves from a sibling checkout: " + str(
        offenders
    )


def test_core_is_pinned_to_a_release():
    """By tag or wheel URL, never a branch. A leaf that floats on core's main
    cannot say which product it built."""
    proj = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert re.search(
        r"contoso-data-product = \{ url = .*releases/download/v\d", proj
    ), "contoso-data-product must come from a published release"


def test_ingest_pulls_from_vendors_rather_than_writing_fixtures():
    """No ingest step may author the data it claims to have ingested.

    This platform once wrote three customers and two orders here, and every
    number it published downstream was then true about a fixture it had
    invented — indistinguishable from a real run except by comparing against
    another runtime, which is exactly what the invented fixture destroyed.
    """
    offenders = []
    for p in sorted(STEPS.glob("ingest*.py")):
        text = p.read_text(encoding="utf-8")
        body = re.sub(r'"""(?:.|\n)*?"""', "", text)
        for marker in ("customer_id,name,email", "write_text("):
            if marker in body:
                offenders.append(f"{p.name}: {marker}")
    assert not offenders, "an ingest step composes bytes rather than fetching: " + str(
        offenders
    )


def test_no_vendor_credential_is_written_in_this_repository():
    """Keys come from the vendor or the environment, never the tree.

    `seed_secrets.py` once carried `string_value="pos-dev-key"` — a credential
    in the source tree, and the wrong one: the vendor issues
    `pos-key-8843-dev`, so anything reading that scope entry was refused 401 by
    the very vendor it was seeded for.
    """
    suspicious = []
    for p in sorted(STEPS.glob("*.py")):
        for line in p.read_text(encoding="utf-8").splitlines():
            if line.lstrip().startswith("#"):
                continue
            if "string_value=" in line and '"' in line.split("string_value=", 1)[1]:
                suspicious.append(f"{p.name}: {line.strip()}")
    assert not suspicious, "a literal credential reaches the secret scope: " + str(
        suspicious
    )


def test_gold_records_the_measurement_and_still_fails():
    """A failing contract must not erase the numbers, or hide behind them.

    Recording a measurement and asserting a pass are two things. Both halves
    are load-bearing, so both are checked: the snapshot is written BEFORE the
    exit, and the exit still happens.
    """
    gold = (STEPS / "gold.py").read_text(encoding="utf-8")
    write = gold.index('Path("product_snapshot.json").write_text')
    raise_after = gold.index("gold's numbers were recorded, and this run FAILED")
    assert write < raise_after, (
        "a failing contract would erase the evidence along with the pass"
    )
    assert "contract_failures" in gold


def test_contract_results_come_from_the_test_invocation():
    """dbt overwrites run_results.json, and `dbt run` shares the target dir.

    Read without checking which command wrote it, the file reports the models:
    nine rows, zero failures. Believed, that publishes a snapshot asserting no
    contract failures on a run where two failed.
    """
    gold = (STEPS / "gold.py").read_text(encoding="utf-8")
    assert 'which != "test"' in gold


def test_every_step_is_runnable_on_its_own():
    """Each step is a script the platform invokes, not a library.

    A Databricks Job runs one task per step, so every step needs its own entry
    point — and `make verify` running them in sequence is the same contract
    without the Jobs wrapper.
    """
    missing = []
    for p in sorted(STEPS.glob("*.py")):
        if p.name in (
            "target.py",
            "landing.py",
            "credentials.py",
            "sources.py",
            "spark_session.py",
        ):
            continue  # binding and helpers, imported rather than invoked
        tree = ast.parse(p.read_text(encoding="utf-8"))
        has_main = any(
            isinstance(n, ast.FunctionDef) and n.name == "main" for n in tree.body
        )
        if not has_main:
            missing.append(p.name)
    assert not missing, "steps with no main(): " + str(missing)
