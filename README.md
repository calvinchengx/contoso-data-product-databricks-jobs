# contoso-data-product-databricks-jobs

The Contoso data product as a **Databricks** team would write it: ingest from
the four vendors, the medallion runners, and the dbt profile that platform
needs. One cell of the [family matrix](https://github.com/calvinchengx/contoso-data-product/blob/main/docs/00-family.md).

```sh
git clone https://github.com/calvinchengx/databricks-platform-jobs
make -C databricks-platform-jobs up verify PRODUCT=../contoso-data-product-databricks-jobs
```

The platform starts the workspace, the engine and the vendors; this repository
supplies the steps it runs. Neither names the other in code — the platform
takes `PRODUCT` as a path.

## What is here

`steps/` is one module per pipeline stage, each with its own `main()`, because
a Databricks Job runs one task per step:

| step | |
|---|---|
| `provision` | the warehouse, catalog and schemas this product needs |
| `seed_secrets` | the vendors' API keys into the secret scope |
| `ingest` | four vendors over four transports, landed verbatim |
| `bronze` → `silver` | **the product's** `run_bronze` / `run_silver` |
| `register` | silver Delta paths as Unity Catalog EXTERNAL tables |
| `gold` | `dbt-databricks` over **the product's** gold project |
| `govern` | the same entities into OpenMetadata |

## What is deliberately NOT here

**No transform SQL, no ODCS contract, no expected number.** Bronze, silver and
every line of gold come from
[contoso-data-product](https://github.com/calvinchengx/contoso-data-product),
pinned by release. A second `fct_sales.sql` in this repository is how "one
product, many engines" would stop being true, and
`test_the_product_is_imported_not_restated` fails if one appears.

**No compose, no emulator pin, no vendor definition.** Those belong to
[databricks-platform-jobs](https://github.com/calvinchengx/databricks-platform-jobs),
which stands the vendors up from `contoso-sources`.

## The numbers this cell produces

```
revenue_usd 129,341,157.6700 · cancelled 2,800,504.4000 · sale_lines 474,044
```

Identical to the Fabric runtimes, which is the point of the family:
`compare_products.py` is green only when every runtime agrees, and agreement
means something only because all of them pull the same vendor bytes.
