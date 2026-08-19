"""Run the product silver against paths this target resolved."""

from __future__ import annotations

import json
from pathlib import Path

from contoso_product import run_silver
from spark_session import connect
from target import tables_path


def main() -> int:
    spark = connect()
    metrics = run_silver(spark, tables=tables_path())
    Path("silver_metrics.json").write_text(
        json.dumps(metrics, default=str, indent=2), encoding="utf-8"
    )
    print(
        f"silver: {metrics['silver_customers']} customers, "
        f"{metrics['silver_party']} parties, {metrics['party_matched']} matched"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
