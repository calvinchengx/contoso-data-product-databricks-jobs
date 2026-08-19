"""Publish the same product entities to OpenMetadata. UC remains the engine catalog."""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path

import requests
from contoso_product.contracts import DOMAIN, METRICS, PRODUCT_NAME, contract_id

OM = os.environ.get("OM_URL", "http://localhost:18585/api/v1").rstrip("/")
OM_USER = os.environ.get("OM_USER", "admin@open-metadata.org")
OM_PASSWORD = os.environ.get("OM_PASSWORD", "admin")

S = requests.Session()


def login() -> None:
    r = S.post(
        f"{OM}/users/login",
        json={
            "email": OM_USER,
            "password": base64.b64encode(OM_PASSWORD.encode()).decode(),
        },
        timeout=60,
    )
    r.raise_for_status()
    S.headers["Authorization"] = f"Bearer {r.json()['accessToken']}"


def put(path: str, body: dict) -> dict:
    r = S.put(f"{OM}/{path}", json=body, timeout=60)
    # RAISE WITH THE CATALOG'S OWN WORDS. `raise_for_status()` alone reports
    # "400 Client Error: Bad Request for url: .../domains" and throws the body
    # away -- and the body is the entire diagnosis: OpenMetadata answers
    # `[query param domainType must not be null]`, which names the field. One
    # missing field cost a round of guessing that reading the response would
    # have ended immediately.
    if r.status_code >= 400:
        raise SystemExit(
            f"OpenMetadata refused PUT /{path}: {r.status_code} {r.text[:400]}"
        )
    return r.json() if r.content else {}


def main() -> int:
    login()
    put(
        "domains",
        {
            "name": DOMAIN,
            "displayName": "Contoso Commerce",
            # REQUIRED, and its absence only shows on a FRESH catalog. OpenMetadata
            # answers `[query param domainType must not be null]` with a 400, but
            # a PUT over a domain that already exists does not need it -- so this
            # step passed for as long as the catalog outlived a run and failed the
            # first time the stack came down with its volumes. A field that is
            # only mandatory on first use is one a re-run will not catch.
            #
            # `Consumer-aligned` matches what fabric-platform-notebook-pipelines
            # already publishes for its domain, and matching matters more here
            # than the taxonomy does: this is a HUMAN catalog, and the same
            # product described two ways by two runtimes is exactly the
            # disagreement it exists to remove. Accepted values are Aggregate,
            # Consumer-aligned and Source-aligned -- verified against this
            # OpenMetadata, which 400s anything else as "Invalid request format".
            "domainType": "Consumer-aligned",
            "description": "One product, two runtimes (Fabric and Databricks).",
        },
    )
    put(
        "services/databaseServices",
        {
            "name": "contoso-databricks",
            "serviceType": "Databricks",
            "connection": {
                "config": {
                    "type": "Databricks",
                    "hostPort": os.environ.get(
                        "DATABRICKS_HOST", "http://localhost:18470"
                    ),
                    "httpPath": "/sql/1.0/endpoints/wh-1",
                    # WRAPPED IN `authType`, not a bare `token`. OpenMetadata
                    # encrypts a service's connection when it stores it, and a
                    # field it does not recognise fails that encryption rather
                    # than being ignored: "Failed to encrypt 'Databricks'
                    # connection stored in DB due to an unrecognized field:
                    # 'token'". Verified against this catalog -- the wrapper is
                    # accepted, the bare field is not.
                    #
                    # NOT A REAL CREDENTIAL. This platform publishes the SHAPE
                    # of the connection to the human catalog; the token that
                    # actually reaches the warehouse comes from the secret
                    # scope, and nothing here should be usable if the catalog
                    # leaks.
                    "authType": {"token": "not-stored"},
                }
            },
        },
    )
    contracts = [contract_id("fct_revenue_summary")]
    for metric in METRICS:
        put(
            "metrics",
            {
                "name": metric,
                "displayName": metric,
                "description": f"Product metric {metric} on {PRODUCT_NAME}",
            },
        )
    catalogued = {
        "product": PRODUCT_NAME,
        "domain": DOMAIN,
        "service": "contoso-databricks",
        "fqn": "contoso-databricks.contoso.gold.fct_revenue_summary",
        "contracts": contracts,
        "metrics": list(METRICS),
    }
    Path("catalog.json").write_text(
        json.dumps(catalogued, indent=2) + "\n", encoding="utf-8"
    )
    print(f"catalogued {PRODUCT_NAME} as {catalogued['fqn']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
