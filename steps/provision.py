"""Create the named warehouse, UC catalog, and schemas. Ids are resolved, never stored in product code."""

from __future__ import annotations

import json
from pathlib import Path

import landing
from target import CATALOG, WAREHOUSE, WORKSPACE, T


def _already_exists(exc: Exception) -> bool:
    """Is this the provider saying the thing is already there?

    THE THREE CREATE CALLS BELOW ARE IDEMPOTENT BY INTENT: provision runs
    repeatedly against a stack that may already hold the catalog, the schemas
    and the secret scope. The SDK does not raise a distinct type for that, so
    the only signal is the message, and each call site had grown its own
    slightly different spelling of the same test. One spelling here, so a
    fourth call site cannot invent a fifth.

    Matched on the message rather than a type, which is exactly why the callers
    keep a `# noqa: BLE001`: narrowing the `except` would need a type the SDK
    does not offer, and guessing one would let a real failure through silently.
    """
    text = str(exc)
    return (
        "already" in text.lower() or "RESOURCE_ALREADY_EXISTS" in text or "409" in text
    )


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
    except Exception as exc:  # noqa: BLE001 -- see _already_exists
        # UC OSS create is idempotent enough; a 409 is fine.
        if not _already_exists(exc):
            print(f"catalog create: {exc}")
    for schema in ("landing", "silver", "gold"):
        try:
            w.schemas.create(name=schema, catalog_name=CATALOG)
        except Exception as exc:  # noqa: BLE001 -- see _already_exists
            if not _already_exists(exc):
                print(f"schema {schema}: {exc}")
    try:
        w.secrets.create_scope(scope=t.secret_scope)
    except Exception as exc:  # noqa: BLE001 -- see _already_exists
        if not _already_exists(exc):
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
