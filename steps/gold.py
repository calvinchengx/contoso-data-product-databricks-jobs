"""The product's gold project, run AS A DATABRICKS JOB TASK.

NOT `dbt` on this host. This step used to shell out to `dbt run` and `dbt test`
from the machine driving the platform, which meant the thing being rehearsed --
"gold builds on Databricks" -- was only ever "gold builds on a laptop that can
reach Databricks". Every Jobs concern that a real deployment has to survive
(the project reaching the workspace, the runtime resolving a profile, the run
having a state, artefacts coming back from somewhere this process cannot see)
was skipped by construction, and the cell was scored on a path nobody ships.

The project is now uploaded to the workspace and run as a `dbt_task`, so dbt
executes INSIDE the runtime and this process learns what happened only through
the Jobs API -- run state, and `run_results.json` returned as a task artefact.
"""

from __future__ import annotations

import base64
import json
import shutil
import time
from pathlib import Path

from contoso_product import gold_dir
from target import CATALOG, T, WAREHOUSE

# Where the project lands in the workspace. `dbt_task.project_directory` reads
# from here, not from this host's disk -- the runtime cannot see this host.
WORKSPACE_DIR = "/Workspace/contoso/gold"

# The schema gold materialises into, and the one the generated profile carries.
GOLD_SCHEMA = "gold"


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


def _upload(w, work: Path) -> list[str]:
    """Put the assembled project into the workspace, which is where Jobs reads it.

    format=RAW, because these are workspace FILES and not notebooks. A dbt
    project imported as SOURCE is stored as Python and comes back wrong.

    The whole project goes up, `profiles.yml` included, even though `dbt_task`
    generates its own profile and runs dbt with `--profiles-dir` pointing at it.
    Uploading exactly what is on disk keeps the workspace copy a faithful copy;
    dropping the one file the emulator happens to ignore would make this
    platform's artefact differ from the project it claims to have run, for the
    benefit of nothing.
    """
    sent = []
    for path in sorted(work.rglob("*")):
        if not path.is_file() or "target" in path.parts or path.name.startswith("."):
            continue
        rel = path.relative_to(work).as_posix()
        w.api_client.do(
            "POST",
            "/api/2.0/workspace/import",
            body={
                "path": f"{WORKSPACE_DIR}/{rel}",
                "format": "RAW",
                "overwrite": True,
                "content": base64.b64encode(path.read_bytes()).decode(),
            },
        )
        sent.append(rel)
    if "dbt_project.yml" not in sent:
        raise SystemExit(f"assembled no dbt_project.yml to upload: {sent}")
    return sent


def _run_gold_job(w, warehouse_id: str) -> tuple[dict, dict]:
    """Create and run the dbt job, and return (run, run_results.json).

    Both `dbt run` and `dbt test` are ONE task, in that order, because the
    contracts must be evaluated by the same runtime that built the models --
    running them from here again would be the host path this step exists to
    leave behind.

    The run is polled rather than awaited with the SDK's waiter: a failing
    contract makes the run FAIL, and the waiter raises on that. A failed run is
    exactly when its artefacts matter, so the failure has to be a value here,
    not an exception thrown before the artefact is read.
    """
    created = w.api_client.do(
        "POST",
        "/api/2.2/jobs/create",
        body={
            "name": "contoso-gold",
            "tasks": [
                {
                    "task_key": "gold",
                    "dbt_task": {
                        "commands": ["dbt run", "dbt test"],
                        "project_directory": WORKSPACE_DIR,
                        "warehouse_id": warehouse_id,
                        "catalog": CATALOG,
                        "schema": GOLD_SCHEMA,
                    },
                    # The product's `sources.yml` reads the silver location
                    # from the environment, so the environment has to reach the
                    # RUNTIME -- this host's exported vars mean nothing there.
                    # `spark_env_vars` is the documented way a task carries
                    # them, and it is what makes the same project text work
                    # against a real workspace.
                    #
                    # DBT_-PREFIXED SINCE CORE v0.6.0, and the reason is not
                    # Databricks': Snowflake's dbt Projects refuse any env var
                    # key that is not UPPERCASE and DBT_-prefixed, so the names
                    # this used to set could not be supplied there at all and
                    # gold ran on every engine except the one named for running
                    # dbt as a first-class object. The rename made the product
                    # portable; nothing about it is specific to this cell.
                    #
                    # LAKEHOUSE_ID is GONE, not renamed. It was only ever here
                    # because gold's default was `env_var('CONTOSO_SILVER_
                    # DATABASE', env_var('LAKEHOUSE_ID'))` and Jinja evaluates
                    # a default EAGERLY -- so a Fabric-only variable was
                    # mandatory on Databricks. Core stopped nesting it.
                    "new_cluster": {
                        "spark_env_vars": {
                            "DBT_SILVER_DATABASE": CATALOG,
                            "DBT_SILVER_SCHEMA": "silver",
                        }
                    },
                }
            ],
        },
    )
    job_id = created["job_id"]
    run_id = w.api_client.do("POST", "/api/2.2/jobs/run-now", body={"job_id": job_id})[
        "run_id"
    ]

    deadline = time.time() + 900.0
    run: dict = {}
    while time.time() < deadline:
        run = w.api_client.do("GET", f"/api/2.2/jobs/runs/get?run_id={run_id}")
        if (run.get("state") or {}).get("life_cycle_state") in (
            "TERMINATED",
            "SKIPPED",
            "INTERNAL_ERROR",
        ):
            break
        time.sleep(1.0)
    else:
        raise SystemExit(f"gold job run {run_id} never reached a terminal state: {run}")

    # RAW, not `w.jobs.get_run_output`. The SDK's `DbtOutput` models
    # `artifacts_link` and `artifacts_headers` -- a URL to fetch elsewhere --
    # and this emulator returns the artefacts INLINE under `artifacts`. The
    # typed model has no field for that, so the SDK drops it and every run
    # looks like it produced nothing. Same transport, same auth, minus the
    # model that discards the field, exactly as `_query` does above.
    #
    # This is a real deviation from Databricks and it is written down rather
    # than hidden: against a real workspace this read becomes a fetch of
    # `artifacts_link`. It is the one place in this step where the emulator and
    # the thing it rehearses do not agree.
    output = w.api_client.do("GET", f"/api/2.2/jobs/runs/get-output?run_id={run_id}")
    artifacts = ((output.get("dbt_output") or {}).get("artifacts")) or {}
    raw = artifacts.get("run_results.json")
    if not raw:
        state = (run.get("state") or {}).get("result_state")
        raise SystemExit(
            f"the gold job finished {state} and returned no run_results.json -- "
            f"refusing to guess whether the models built or the contracts passed. "
            f"stderr: {(output.get('error') or '')[:300]}"
        )
    return run, json.loads(raw)


def _contract_failures(payload: dict, expected: list[str]) -> list[dict]:
    """Read the contracts' verdict out of the job's own artefact."""
    # ASSERT WHICH INVOCATION WROTE THIS. dbt overwrites run_results.json on
    # every invocation and `dbt run` shares the target directory, so the file
    # is only the contracts' verdict if the last command was `dbt test`. It
    # still bites here, and for a sharper reason than before: the task runs the
    # commands in order and STOPS AT THE FIRST FAILURE, so a `dbt run` that
    # fails leaves `run`'s own artefact behind. Believed, that publishes a
    # snapshot asserting no contract failures for a run where the models never
    # built at all.
    which = (payload.get("args") or {}).get("which")
    if which != "test":
        raise SystemExit(
            f"the job's run_results.json was written by `dbt {which}`, not `dbt "
            f"test` -- the task stops at its first failing command, so this is "
            f"a run where `dbt {which}` failed, not a contract verdict."
        )

    failures = []
    evaluated = set()
    for r in payload.get("results", []):
        # dbt names a singular test `test.<project>.<name>.<hash>`; the snapshot
        # names contracts bare, as `contracts` already does, so the two join.
        unique_id = r.get("unique_id", "")
        name = unique_id.split(".")[2] if unique_id.count(".") >= 2 else unique_id
        evaluated.add(name)
        if r.get("status") in ("pass", "success"):
            continue
        failures.append(
            {
                "contract": name,
                "status": r.get("status"),
                "failures": r.get("failures"),
                "detail": (r.get("message") or "").strip()[:200],
                # OPTIONAL, and supplied by the PLATFORM. A platform knows which
                # of its own emulator's defects it is living with; the product
                # should not have to. A failure with no cause reads as
                # unexplained, which is a worse state and should look like one.
                **({"cause": KNOWN_CAUSES[name]} if name in KNOWN_CAUSES else {}),
            }
        )

    # EVERY NAMED CONTRACT WAS ACTUALLY EVALUATED. The snapshot lists contracts
    # by globbing their filenames, and a glob proves a file exists, not that the
    # runtime ran it -- selection, a parse error in one file, or a rename all
    # produce a snapshot naming guarantees this run never checked. Now that the
    # verdict comes from dbt's own artefact, the two can be joined, and a
    # contract that exists but did not run is a hard failure instead of a
    # silently stronger-sounding snapshot.
    missing = sorted(set(expected) - evaluated)
    if missing:
        raise SystemExit(
            f"the snapshot would name contracts this run never evaluated: "
            f"{missing}. dbt tested {sorted(evaluated)}."
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

    w = t.workspace_client()
    sent = _upload(w, work)
    print(f"gold project uploaded to {WORKSPACE_DIR}: {len(sent)} files")

    # The contracts this snapshot will name, taken from the product. They are
    # joined against what dbt actually evaluated inside `_contract_failures`,
    # so this list can no longer outrun the run that is supposed to back it.
    expected = sorted(p.stem for p in (product / "tests").glob("*.sql"))

    # RECORDING A MEASUREMENT AND ASSERTING A PASS ARE TWO THINGS, and this step
    # used to do both in one act: a failing contract stopped the snapshot being
    # written, so the failure erased the evidence along with the pass. That is
    # right in general and wrong here -- refusing to publish takes the cell out
    # of the cross-runtime comparison the family exists to make, for a reason
    # that may belong to neither the product nor this platform.
    #
    # So the run still FAILS -- see the exit at the end, nothing is softened --
    # but the numbers are written down first, carrying the failures with them.
    # Evidence is worth recording even when the run that produced it failed;
    # what must never happen is evidence recorded without the failure attached,
    # which is exactly the stale snapshot this platform once published, silently
    # outliving its own fix.
    run, results = _run_gold_job(w, wh.id)
    state = (run.get("state") or {}).get("result_state")
    print(f"gold job {state}")
    contract_failures = _contract_failures(results, expected)

    # A RUN THAT FAILED FOR A REASON DBT DID NOT NAME. `dbt test` failing on a
    # contract is expected and is carried in the snapshot; the run failing while
    # run_results names no failing test means something else broke -- the task,
    # the agent, the warehouse -- and publishing a clean snapshot for it would
    # assert a green this run never earned.
    if state != "SUCCESS" and not contract_failures:
        raise SystemExit(
            f"the gold job finished {state} but its run_results names no failing "
            f"test -- something failed that this cannot describe, so it is not "
            f"publishing a snapshot that implies otherwise."
        )

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
        "contracts": expected,
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
