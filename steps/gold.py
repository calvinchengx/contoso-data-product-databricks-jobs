"""dbt-databricks over the product gold project. Adapter only; SQL is the product's."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

from contoso_product import gold_dir
from target import CATALOG, T, WAREHOUSE

# Contract failures this platform can already explain, by contract name. Both
# of these are one emulator defect: decimal columns are registered in Unity
# Catalog with `type_name: DOUBLE`, so every money column in gold is READ as a
# binary float even though the Delta log, the Parquet physical type and
# `DESCRIBE` all still say `decimal(19,4)`. The numbers are right; their type
# is not. Remove an entry when its issue closes -- a cause that outlives its
# defect is a worse lie than no cause at all.
# NO KNOWN CAUSES. This mapped both money contracts to databricks-emulator#46,
# which was true for as long as this platform ran 0.2.4: decimal columns were
# registered in Unity Catalog with column metadata that could not express them,
# so they read as float and the type contract failed. 0.2.5 registers no column
# metadata, the Delta log is the schema again, and both contracts pass — so the
# cause is gone and naming it would be a worse lie than naming none.
#
# Repopulate it if this platform ever runs with a defect it is knowingly living
# with. A cause is only worth carrying while it is true, and the entry should
# die in the same change that makes it false, which is this one.
KNOWN_CAUSES: dict[str, str] = {}


def _query(w, warehouse_id: str, statement: str) -> list:
    """Run one statement and return its rows, whatever shape they arrive in.

    NOT `statement_execution.execute_statement`. That returns a typed
    `ResultData`, and the SDK's model carries `data_array` and no `text` --
    so when this warehouse answers with `result.text` (the payload as a nested
    JSON string) the SDK drops it on the floor and every read looks like an
    empty table. Measured: the statement reported SUCCEEDED, `data_array` was
    None, and the star held four rows summing to 37 the whole time.

    `api_client.do` is the same transport and the same auth, minus the model
    that discards the field. Both shapes are then accepted rather than one
    being declared correct: real Databricks returns `data_array`, and a fix
    that only understood the emulator would break against the thing this
    platform exists to rehearse.
    """
    payload = w.api_client.do(
        "POST",
        "/api/2.0/sql/statements",
        body={
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "30s",
        },
    )
    state = (payload.get("status") or {}).get("state")
    if state != "SUCCEEDED":
        message = ((payload.get("status") or {}).get("error") or {}).get("message", "")
        raise SystemExit(f"statement did not succeed ({state}): {message[:200]}")
    result = payload.get("result") or {}
    if "data_array" in result:
        return result["data_array"] or []
    if "text" in result:
        return json.loads(result["text"]).get("data") or []
    return []


def _run_contracts(work: Path, env: dict) -> list[dict]:
    """Run the ODCS contracts and return what failed, in dbt's own words.

    THE CONTRACTS, ACTUALLY RUN. This step once invoked `dbt run` alone and then
    published a snapshot listing five contracts by GLOBBING THEIR FILENAMES off
    disk -- so the snapshot named five guarantees this runtime had never once
    evaluated, and `compare_products` compared that list against a runtime where
    they had genuinely passed. Two runtimes "agreeing on contracts" while only
    one ran them is worse than not comparing at all.
    """
    rc = subprocess.call(
        ["dbt", "test", "--project-dir", str(work), "--profiles-dir", str(work)],
        env=env,
    )
    results = work / "target" / "run_results.json"
    if not results.exists():
        raise SystemExit(
            f"dbt test exited {rc} but wrote no {results} -- refusing to guess "
            f"whether the contracts passed."
        )
    payload = json.loads(results.read_text(encoding="utf-8"))

    # ASSERT WHICH INVOCATION WROTE THIS. dbt overwrites run_results.json on
    # every invocation and `dbt run` shares this target directory, so the file
    # is only the contracts' verdict if the last command was `dbt test`. Reading
    # it without checking is not theoretical: inspecting it after a later `dbt
    # run` returned `which: "run"`, nine model rows and zero failures -- which,
    # believed, publishes a snapshot asserting NO contract failures on a run
    # where two failed. That is the precise false green this whole design exists
    # to prevent, so it fails loudly instead.
    which = (payload.get("args") or {}).get("which")
    if which != "test":
        raise SystemExit(
            f"{results} was written by `dbt {which}`, not `dbt test` -- refusing "
            f"to report contract results from another command's artefact."
        )

    failures = []
    for r in payload.get("results", []):
        if r.get("status") in ("pass", "success"):
            continue
        # dbt names a singular test `test.<project>.<name>.<hash>`; the snapshot
        # names contracts bare, as `contracts` already does, so the two join.
        unique_id = r.get("unique_id", "")
        name = unique_id.split(".")[2] if unique_id.count(".") >= 2 else unique_id
        failures.append(
            {
                "contract": name,
                "status": r.get("status"),
                "failures": r.get("failures"),
                "detail": (r.get("message") or "").strip()[:200],
                # OPTIONAL, and supplied by the PLATFORM. A platform knows which of
                # its own emulator's defects it is living with; the product should
                # not have to. A failure with no cause reads as unexplained, which
                # is a worse state and should look like one.
                **({"cause": KNOWN_CAUSES[name]} if name in KNOWN_CAUSES else {}),
            }
        )
    if rc != 0 and not failures:
        raise SystemExit(
            f"dbt test exited {rc} but run_results names no failing test -- "
            f"something failed that this cannot describe, so it is not "
            f"publishing a snapshot that implies otherwise."
        )
    return failures


def main() -> int:
    t = T()
    wh = t.warehouse(WAREHOUSE)
    product = gold_dir()
    work = Path("gold")
    for name in ("models", "macros", "tests"):
        dest = work / name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(product / name, dest)

    host = t.host
    dbt_host = host.replace("https://", "").replace("http://", "")
    path = wh.http_path
    uri = f"{host}{path}"
    env = os.environ.copy()
    env.update(
        {
            "DATABRICKS_HOST": dbt_host,
            "DATABRICKS_TOKEN": t.token,
            "DATABRICKS_HTTP_PATH": path,
            "DATABRICKS_CONNECTION_URI": uri,
            "DATABRICKS_CATALOG": CATALOG,
            "CONTOSO_SILVER_DATABASE": CATALOG,
            "CONTOSO_SILVER_SCHEMA": "silver",
            "LAKEHOUSE_ID": CATALOG,
            "DBT_PROFILES_DIR": str(work.resolve()),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        }
    )
    subprocess.check_call(
        ["dbt", "run", "--project-dir", str(work), "--profiles-dir", str(work)],
        env=env,
    )
    # THE CONTRACTS, ACTUALLY RUN. This step used to invoke `dbt run` alone and
    # then publish a snapshot listing five ODCS contracts by GLOBBING THEIR
    # FILENAMES off disk -- so the snapshot named five guarantees this runtime
    # had never once evaluated, and `compare_products` compared that list of
    # names against a runtime where they had genuinely passed. Two runtimes
    # "agreeing on contracts" while only one ran them is worse than not
    # comparing at all.
    #
    # RECORDING A MEASUREMENT AND ASSERTING A PASS ARE TWO THINGS, and this used
    # to do both in one act: a failing contract stopped the snapshot being
    # written, so the failure erased the evidence along with the pass.
    #
    # That is right in general and wrong here. This runtime's gold is CORRECT --
    # its aggregates are identical to the Fabric runtime's to the last decimal
    # place -- and the two contracts that fail do so because of an emulator
    # defect (databricks-emulator#46), not a product one. Refusing to publish
    # took the cell out of the cross-runtime comparison the family exists to
    # make, for a reason belonging to neither the product nor this platform.
    #
    # So the run still FAILS -- see the exit at the end, nothing is softened --
    # but the numbers are written down first, carrying the failures with them.
    # Evidence is worth recording even when the run that produced it failed;
    # what must never happen is evidence recorded without the failure attached,
    # which is exactly the stale snapshot this platform once published, silently
    # outliving its own fix.
    contract_failures = _run_contracts(work, env)
    w = t.workspace_client()
    # READ MONEY AT MONEY'S OWN GRAIN, and cast in the ENGINE rather than
    # rounding in Python.
    #
    # Money columns in this catalog are READ as binary floats, so the total
    # arrives as 129341157.67000002 -- the right number carrying ~2e-8 of float
    # error, against the Fabric runtime's exact 129341157.6700.
    #
    # NOT a `sum()` defect, though this comment said so for a while and the
    # family's plan inherited the mistake. `sum()` is fine: a fresh
    # `CREATE TABLE t AS SELECT CAST(1.5 AS DECIMAL(19,4)) AS m` answers
    # `typeof(sum(m))` with `decimal(29,4)`, correctly. The cause is that
    # databricks-emulator registers decimal columns in Unity Catalog with
    # `type_name: DOUBLE` (`internal/sqlshim/shim.go`, `sparkToUC`), while the
    # Delta log, the Parquet physical type and `DESCRIBE` all still say
    # `decimal(19,4)`. The planner trusts UC, so the column reads as a float.
    # See databricks-emulator#46 -- and note the emulator does this because
    # Sail's unity provider rejects `decimal(p,s)` outright, so the eventual
    # fix is probably upstream of both.
    #
    # The cast recovers the value because money is defined to four decimal
    # places and the error is eight orders of magnitude below that. It does NOT
    # repair the column and is not meant to: this line only stops a read-path
    # artefact from being mistaken for two runtimes disagreeing about revenue.
    # Cast to STRING in SQL as well, so the exact digits survive a JSON number.
    money = "CAST(CAST(coalesce(sum({}),0) AS DECIMAL(19,4)) AS STRING)"
    data = _query(
        w,
        wh.id,
        f"SELECT {money.format('revenue_usd')}, {money.format('cancelled_revenue_usd')}, "
        f"coalesce(sum(sale_lines),0) FROM {CATALOG}.gold.fct_revenue_summary",
    )
    if not data:
        # "COULD NOT READ" IS NOT "ZERO", and defaulting to 0 here published a
        # snapshot claiming this runtime built nothing while dbt had just
        # reported nine models built. compare_products then refused it as an
        # empty runtime -- the right call on the evidence, and the wrong
        # diagnosis. Measured: the star held 4 rows and revenue 37 the whole
        # time; the read was blind, not the warehouse.
        raise SystemExit(
            "gold built, but its aggregates came back with no rows -- refusing "
            "to publish a snapshot of zeros."
        )
    snapshot = {
        "revenue_usd": str(data[0][0]),
        "cancelled_revenue_usd": str(data[0][1]),
        "sale_lines": str(data[0][2]),
        "contracts": sorted(p.stem for p in (product / "tests").glob("*.sql")),
        "runtime": "databricks",
        "catalog": CATALOG,
    }
    # ABSENT WHEN CLEAN, rather than an empty list on every green snapshot. An
    # always-present `[]` makes "this runtime evaluated its contracts and they
    # passed" indistinguishable from "this runtime never checked", which is the
    # distinction the field exists to carry.
    if contract_failures:
        snapshot["contract_failures"] = contract_failures
    Path("product_snapshot.json").write_text(
        json.dumps(snapshot, indent=2) + "\n", encoding="utf-8"
    )
    print(f"gold snapshot {snapshot}")
    if contract_failures:
        named = ", ".join(f["contract"] for f in contract_failures)
        raise SystemExit(
            f"gold's numbers were recorded, and this run FAILED: {named}. "
            f"The snapshot carries the failures; `make verify` is red and "
            f"should be."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
