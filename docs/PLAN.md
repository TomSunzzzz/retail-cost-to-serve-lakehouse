# Project plan

## Goal

An end-to-end, re-runnable data pipeline and Power BI report on real retail data, answering:

| Business question | Report page |
|---|---|
| How much are we selling? (month, state, category) | Sales overview |
| Are we on budget? | Budget vs actual |
| Where is fulfilment too expensive? | Cost-to-serve |

## Workflow

```
Step 0  Data        9 Olist CSVs (data_set/archive/) + synthetic monthly budget (scripts/)
Step 1  Bronze      ingest raw CSVs as-is, add _ingested_at and _source_file → Delta
Step 2  Silver      clean and conform each table + data-quality checks → Delta + dq_results
Step 3  Gold        star schema for reporting → Delta
Step 4  Report      Power BI Desktop on Gold tables: relationships, DAX measures, 3 pages
Step 5  Package     README with architecture diagram, design decisions, screenshots → GitHub
```

One command runs Steps 1-3: `docker compose run --rm spark python -m src.run_pipeline`

## Source tables (Olist)

| File | Rows | Key |
|---|---|---|
| olist_orders_dataset.csv | 99,441 | order_id |
| olist_order_items_dataset.csv | 112,650 | order_id + order_item_id |
| olist_order_payments_dataset.csv | 103,886 | order_id + payment_sequential |
| olist_order_reviews_dataset.csv | 104,719 | review_id |
| olist_customers_dataset.csv | 99,441 | customer_id (customer_unique_id = real person) |
| olist_sellers_dataset.csv | 3,095 | seller_id |
| olist_products_dataset.csv | 32,951 | product_id |
| olist_geolocation_dataset.csv | 1,000,163 | zip prefix (many rows per prefix) |
| product_category_name_translation.csv | 71 | Portuguese → English category |

## Layer specs

### Bronze — `lakehouse/bronze/<table>`
- Read every column as string (no type inference) so nothing is lost or silently changed.
- Add `_ingested_at` (timestamp) and `_source_file`.
- Overwrite on each run.

### Silver — `lakehouse/silver/<table>`
- Cast types (timestamps, decimals, ints); trim strings; lower-case and strip accents from city names.
- Deduplicate on each table's key.
- Translate product categories to English; unknown → "unknown".
- Geolocation: one row per zip prefix (median lat/lng) — the raw file has many duplicates per prefix.
- Business rule: keep all order statuses in Silver; Gold revenue counts only `delivered` orders (documented in README).
- Write check results to `lakehouse/silver/dq_results` (run_id, table, rule, failed_rows, total_rows, status).

Minimum data-quality rules (at least 5):
1. Primary key is unique (each table)
2. Primary key is not null (each table)
3. Every order_item has a matching order (referential integrity)
4. price > 0 and freight_value >= 0
5. Delivered orders have a delivered date, and delivered date >= purchase date
6. Every product category maps to an English name

### Gold — `lakehouse/gold/<table>` (star schema)
| Table | Grain |
|---|---|
| fact_order_item | one row per order item (price, freight, dates, keys) |
| fact_budget | one row per month × state (synthetic budget revenue) |
| dim_date | one row per calendar day |
| dim_customer | one row per customer_id, with city, state, region |
| dim_seller | one row per seller, with city, state |
| dim_product | one row per product, with English category |

Power BI reads Gold via exported Parquet/CSV (local) or Direct Lake (Fabric).

### Power BI measures (minimum)
Total Revenue, Orders, Average Order Value, Total Freight, Freight % of Revenue, Budget, Variance, Variance %, Revenue YoY %.

## Minimum product (definition of done)
- [ ] One command runs Bronze → Silver → Gold end to end
- [ ] At least 5 data-quality rules, results logged to dq_results every run
- [ ] Gold star schema: 2 fact tables + 4 dimensions
- [ ] Power BI: 3 pages (sales overview, budget vs actual, cost-to-serve)
- [ ] Public GitHub repo with an English README (architecture, how to run, design decisions, screenshots, honest notes)
- [ ] Owner can explain every layer

## Stretch (only after the minimum product)
1. (moved to main line, see below)
2. Power BI page 4: data-quality dashboard
3. LLM classification of review comments into complaint themes
4. Incremental loads (process only new data)

## Schedule
| Day | Work |
|---|---|
| 2026-10-25 (Sun) | Environment, dataset, repo skeleton, smoke test |
| 2026-10-26 (Mon) | Steps 0-3: Bronze, Silver + DQ, Gold |
| 2026-10-27 (Tue) | Run the pipeline in Microsoft Fabric (Lakehouse, Notebooks, Data Factory pipeline); Step 4 Power BI (built by Tom); Step 5 README + screenshots — minimum product done |
| 2026-10-28 (Wed) | Learning and review |
