"""Run the product bronze against paths this target resolved."""

from __future__ import annotations

import json
from pathlib import Path

import landing
from contoso_product import run_bronze
from spark_session import connect
from target import landing_path, tables_path

WEB_CUSTOMER_DDL = "array<struct<email:STRING,country:STRING>>"
WEB_PRODUCT_DDL = "array<struct<product_id:STRING,name:STRING>>"
WEB_ORDER_DDL = (
    "array<struct<web_order_id:STRING,email:STRING,placed_at:STRING,status:STRING,"
    "lines:array<struct<line_no:STRING,product_id:STRING,quantity:STRING,unit_price:STRING>>>>"
)


def main() -> int:
    # THE DAY THE INGEST STEPS DECIDED, not one computed here. Reading the
    # partition bronze happens to be run on would silently read an empty
    # directory whenever a run crosses midnight -- and an empty landing is not
    # an error to Spark, it is zero rows.
    day = landing.day()
    spark = connect()
    metrics = run_bronze(
        spark,
        landing=landing_path(),
        tables=tables_path(),
        day=day,
        web_customer_ddl=WEB_CUSTOMER_DDL,
        web_product_ddl=WEB_PRODUCT_DDL,
        web_order_ddl=WEB_ORDER_DDL,
        web_customer_fields=["email", "country"],
        web_product_fields=["product_id", "name"],
        web_order_fields=["web_order_id", "email", "lines"],
    )
    Path("bronze_metrics.json").write_text(
        json.dumps(metrics, default=str, indent=2), encoding="utf-8"
    )
    print(
        f"bronze: {metrics['bronze_customers']} POS customers, {metrics['bronze_orders']} orders"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
