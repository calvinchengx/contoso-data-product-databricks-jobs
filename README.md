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

## What the product contains

The SQL is not here. It lives in the core so seven leaves cannot drift into
seven versions of it, and that costs you a click, so this list gives it back.
`make show-product` copies the same files into `product/` where you can open
them; the block below is generated from the pinned package and a test fails
when it falls behind.

Using CPython 3.13.7
Creating virtual environment at: .venv
Installed 17 packages in 71ms
<!-- BEGIN product inventory: python -m contoso_product.show --markdown -->

The product is [`contoso-data-product`](https://github.com/calvinchengx/contoso-data-product/tree/v0.6.0) at **v0.6.0**, the version this repository pins. It is not vendored here: these files live there and are staged locally by `make show-product`.

**silver**: 8 models, 1 singular test

- [`silver_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_customers.sql)
- [`silver_fx_daily`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_fx_daily.sql)
- [`silver_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_orders.sql)
- [`silver_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_party.sql)
- [`silver_product_hierarchy`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_product_hierarchy.sql)
- [`silver_quarantine_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_quarantine_orders.sql)
- [`silver_web_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_web_customers.sql)
- [`silver_web_order_lines`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_web_order_lines.sql)

Assertions over silver, each failing the build on its own:

- [`silver_orders_never_holds_a_non_positive_quantity`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/tests/silver_orders_never_holds_a_non_positive_quantity.sql)

**gold**: 9 models, 5 singular tests

- [`dim_country`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/dim_country.sql)
- [`dim_customer`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/dim_customer.sql)
- [`dim_date`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/dim_date.sql)
- [`dim_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/dim_party.sql)
- [`dim_product`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/dim_product.sql)
- [`fct_daily_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/fct_daily_revenue.sql)
- [`fct_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/fct_orders.sql)
- [`fct_revenue_summary`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/fct_revenue_summary.sql)
- [`fct_sales`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/fct_sales.sql)

Assertions over gold, each failing the build on its own:

- [`both_selling_systems_reach_the_pack`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/tests/both_selling_systems_reach_the_pack.sql)
- [`every_country_resolves_to_the_dimension`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/tests/every_country_resolves_to_the_dimension.sql)
- [`fiscal_year_is_not_the_calendar_year`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/tests/fiscal_year_is_not_the_calendar_year.sql)
- [`money_is_never_stored_as_float`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/tests/money_is_never_stored_as_float.sql)
- [`revenue_summary_loses_no_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/tests/revenue_summary_loses_no_revenue.sql)

<!-- END product inventory -->

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
| `gold` | **the product's** gold project, uploaded to the workspace and run as a Jobs `dbt_task` |
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
