"""Create the named warehouse, UC catalog, and schemas. Ids are resolved, never stored in product code."""

from __future__ import annotations

import json
from pathlib import Path

import landing
from target import CATALOG, T, WAREHOUSE, WORKSPACE


def main() -> int:
    t = T()
    w = t.workspace_client()
    existing = {}
    try:
        existing = {wh.name: wh for wh in w.warehouses.list()}
    except TypeError:
        existing = {}
    if WAREHOUSE in existing:
        wh = existing[WAREHOUSE]
    else:
        created = w.warehouses.create(name=WAREHOUSE).result()
        wh = created
    try:
        w.catalogs.create(name=CATALOG)
    except Exception as exc:
        if "already" not in str(exc).lower() and "RESOURCE_ALREADY_EXISTS" not in str(
            exc
        ):
            # UC OSS create is idempotent enough; a 409 is fine.
            if "409" not in str(exc):
                print(f"catalog create: {exc}")
    for schema in ("landing", "silver", "gold"):
        try:
            w.schemas.create(name=schema, catalog_name=CATALOG)
        except Exception as exc:
            if "409" not in str(exc) and "already" not in str(exc).lower():
                print(f"schema {schema}: {exc}")
    try:
        w.secrets.create_scope(scope=t.secret_scope)
    except Exception as exc:
        if "already" not in str(exc).lower():
            print(f"secret scope: {exc}")

    # MERGED, NOT REPLACED. This used to overwrite state.json wholesale, which
    # was harmless only because provision happens to run first: any later
    # re-provision would drop `landing_day`, bronze would compute a fresh date,
    # and it would read an empty landing directory -- which is not an error to
    # Spark, it is zero rows.
    state = landing._state()
    state.update(
        {
            "workspace": WORKSPACE,
            "warehouse": WAREHOUSE,
            "warehouse_id": wh.id,
            "http_path": f"/sql/1.0/endpoints/{wh.id}",
            "catalog": CATALOG,
            "target": t.name,
            "host": t.host,
        }
    )
    Path("state.json").write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    print(f"provisioned warehouse {WAREHOUSE} id={wh.id} catalog={CATALOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
