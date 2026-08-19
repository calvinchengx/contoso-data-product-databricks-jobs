"""Register silver Delta paths as UC EXTERNAL tables. MANAGED is refused on the emulator."""

from __future__ import annotations

from target import CATALOG, T, tables_path

SILVER_TABLES = (
    "silver_customers",
    "silver_orders",
    "silver_quarantine_orders",
    "silver_product_hierarchy",
    "silver_fx_daily",
    "silver_web_customers",
    "silver_web_order_lines",
    "silver_party",
)


def main() -> int:
    t = T()
    w = t.workspace_client()
    wh = t.warehouse()
    root = tables_path()
    failed = []
    for name in SILVER_TABLES:
        loc = f"{root}/{name}"
        sql = (
            f"CREATE TABLE IF NOT EXISTS {CATALOG}.silver.{name} "
            f"USING delta LOCATION '{loc}'"
        )
        stmt = w.statement_execution.execute_statement(warehouse_id=wh.id, statement=sql)
        state = stmt.status.state.value if stmt.status and stmt.status.state else None
        print(f"  {name}: {state}")
        if state != "SUCCEEDED":
            message = ""
            if stmt.status and stmt.status.error:
                message = (stmt.status.error.message or "")[:300]
            failed.append(f"{name}: {state} {message}")
    # PRINTING "FAILED" AND RETURNING 0 IS NOT REPORTING A FAILURE. This step
    # did exactly that, and the cost was concrete: every silver table failed to
    # register, `make verify` carried on, and gold ran against a catalog that
    # had nothing in it. The word FAILED was right there in the log, eight
    # times, and the pipeline was green.
    #
    # The cause was worth naming too -- `session ... is not running`, the
    # shared Spark Connect session dropped by the engine and never
    # re-established by the agent. That is an operational fault with an obvious
    # remedy (restart the agent), which is precisely the kind of thing a step
    # must refuse to paper over.
    if failed:
        raise SystemExit(
            "silver tables did not register, so gold would build against an "
            "empty catalog:\n  " + "\n  ".join(failed)
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
