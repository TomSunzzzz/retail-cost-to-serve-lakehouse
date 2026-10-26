"""Gold layer: a star schema built for reporting.

Facts
- fact_order_item: one row per item sold (price, freight, delivery, distance, review)
- fact_budget:     one row per month x customer state (SYNTHETIC budget, see build_fact_budget)

Dimensions
- dim_date, dim_customer, dim_seller, dim_product, dim_state

Business definitions (also in the README):
- Revenue = item price of orders with status 'delivered'. Freight is a cost-to-serve driver,
  not revenue. Cancelled / unavailable / in-progress orders are kept but flagged is_revenue = false.
- Reporting period = 2017-01 to 2018-08. Months outside it have only a handful of orders
  (the dataset starts and ends mid-stream), so trends and budget use the full months only.
"""
from __future__ import annotations

import random

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.config import GOLD_DIR, SILVER_DIR
from src.dq import DQ

PERIOD_START, PERIOD_END = "2017-01-01", "2018-08-31"

STATES = {
    # code: (name, region)
    "AC": ("Acre", "North"), "AP": ("Amapa", "North"), "AM": ("Amazonas", "North"),
    "PA": ("Para", "North"), "RO": ("Rondonia", "North"), "RR": ("Roraima", "North"),
    "TO": ("Tocantins", "North"),
    "AL": ("Alagoas", "Northeast"), "BA": ("Bahia", "Northeast"), "CE": ("Ceara", "Northeast"),
    "MA": ("Maranhao", "Northeast"), "PB": ("Paraiba", "Northeast"), "PE": ("Pernambuco", "Northeast"),
    "PI": ("Piaui", "Northeast"), "RN": ("Rio Grande do Norte", "Northeast"), "SE": ("Sergipe", "Northeast"),
    "DF": ("Distrito Federal", "Central-West"), "GO": ("Goias", "Central-West"),
    "MT": ("Mato Grosso", "Central-West"), "MS": ("Mato Grosso do Sul", "Central-West"),
    "ES": ("Espirito Santo", "Southeast"), "MG": ("Minas Gerais", "Southeast"),
    "RJ": ("Rio de Janeiro", "Southeast"), "SP": ("Sao Paulo", "Southeast"),
    "PR": ("Parana", "South"), "RS": ("Rio Grande do Sul", "South"), "SC": ("Santa Catarina", "South"),
}

BUDGET_UPLIFT = 1.05  # management target: 5% above the revenue trend line
BUDGET_NOISE = 0.06  # +/- 6% month-to-month planning variation
BUDGET_SEED = 42


def silver(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.format("delta").load(f"{SILVER_DIR}/{table}").drop("_ingested_at")


def save(df: DataFrame, table: str):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{GOLD_DIR}/{table}")


def date_key(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


def haversine_km(lat1, lng1, lat2, lng2):
    """Straight-line distance between two coordinates, in kilometres."""
    dlat = F.radians(lat2 - lat1)
    dlng = F.radians(lng2 - lng1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlng / 2) ** 2
    return F.round(2 * 6371 * F.asin(F.sqrt(a)), 1)


# --- dimensions ----------------------------------------------------------------------

def build_dim_date(spark):
    days = spark.sql("SELECT explode(sequence(DATE'2016-09-01', DATE'2018-12-31', INTERVAL 1 DAY)) AS date")
    return days.select(
        date_key(F.col("date")).alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.trunc("date", "month").alias("month_start"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        F.col("date").between(PERIOD_START, PERIOD_END).alias("in_reporting_period"),
    )


def build_dim_state(spark):
    rows = [(code, name, region) for code, (name, region) in STATES.items()]
    return spark.createDataFrame(rows, "state string, state_name string, region string")


def build_dim_customer(customers, geo):
    return customers.join(geo.select("zip_code_prefix", "lat", "lng"), "zip_code_prefix", "left").select(
        "customer_id", "customer_unique_id", "zip_code_prefix", "city", "state", "lat", "lng"
    )


def build_dim_seller(sellers, geo):
    return sellers.join(geo.select("zip_code_prefix", "lat", "lng"), "zip_code_prefix", "left").select(
        "seller_id", "zip_code_prefix", F.coalesce("city", F.lit("unknown")).alias("city"), "state", "lat", "lng"
    )


def build_dim_product(products):
    return products.select(
        "product_id",
        "category",
        F.initcap(F.regexp_replace("category", "_", " ")).alias("category_name"),
        F.col("category_pt").alias("category_original"),
        "product_weight_g",
        (F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")).alias("product_volume_cm3"),
        "product_photos_qty",
    )


# --- facts ---------------------------------------------------------------------------

def build_fact_order_item(items, orders, reviews, dim_customer, dim_seller):
    latest_review = (
        reviews.withColumn(
            "rn", F.row_number().over(Window.partitionBy("order_id").orderBy(F.desc("review_answered_at")))
        )
        .where("rn = 1")
        .select("order_id", "review_score")
    )
    cust = dim_customer.select(
        "customer_id", F.col("state").alias("customer_state"),
        F.col("lat").alias("c_lat"), F.col("lng").alias("c_lng"),
    )
    sell = dim_seller.select(
        "seller_id", F.col("state").alias("seller_state"),
        F.col("lat").alias("s_lat"), F.col("lng").alias("s_lng"),
    )
    df = (
        items.join(orders, "order_id", "inner")
        .join(cust, "customer_id", "left")
        .join(sell, "seller_id", "left")
        .join(latest_review, "order_id", "left")
    )
    delivered_ok = F.col("delivered_at").isNotNull()
    return df.select(
        "order_id",
        "order_item_id",
        "customer_id",
        "seller_id",
        "product_id",
        "customer_state",
        "seller_state",
        date_key(F.col("purchased_at")).alias("order_date_key"),
        F.to_date("purchased_at").alias("order_date"),
        "order_status",
        (F.col("order_status") == "delivered").alias("is_revenue"),
        "price",
        "freight_value",
        (F.col("price") + F.col("freight_value")).alias("item_total"),
        (F.col("customer_state") != F.col("seller_state")).alias("is_interstate"),
        haversine_km(F.col("s_lat"), F.col("s_lng"), F.col("c_lat"), F.col("c_lng")).alias("distance_km"),
        F.when(delivered_ok, F.datediff("delivered_at", "purchased_at")).alias("delivery_days"),
        F.when(delivered_ok, F.col("delivered_at") > F.col("estimated_delivery_at")).alias("is_late"),
        "review_score",
    )


def build_fact_budget(spark, fact: DataFrame):
    """SYNTHETIC monthly budget by customer state. Olist publishes no budget, so one is generated:

    1. Baseline for a month = average actual revenue of that month and its two neighbours
       (a smoothed run-rate, standing in for the forecast a planning team would start from).
    2. Company budget = baseline x 1.05 (a 5% stretch target), with a fixed-seed +/-6%
       planning variation, so months land both above and below plan. Spikes such as
       Black Friday (November) are smoothed out of the baseline and therefore beat plan.
    3. Split each month's budget across states by each state's share of total revenue.
    The table carries is_synthetic = true and the README says so.
    """
    in_period = fact.where("is_revenue").where(F.col("order_date").between(PERIOD_START, PERIOD_END))
    monthly = (
        in_period.groupBy(F.trunc("order_date", "month").alias("month_start"))
        .agg(F.sum("price").cast("double").alias("revenue"))
        .orderBy("month_start")
        .collect()
    )
    revenue = [r["revenue"] for r in monthly]
    rng = random.Random(BUDGET_SEED)
    company = []
    for i, r in enumerate(monthly):
        window = revenue[max(0, i - 1): i + 2]
        baseline = sum(window) / len(window)
        company.append(
            (r["month_start"], baseline * BUDGET_UPLIFT * (1 + rng.uniform(-BUDGET_NOISE, BUDGET_NOISE)))
        )

    shares = in_period.groupBy("customer_state").agg(F.sum("price").cast("double").alias("rev")).collect()
    total = sum(s["rev"] for s in shares)
    rows = [
        (date_start, s["customer_state"], round(amount * s["rev"] / total, 2))
        for date_start, amount in company
        for s in shares
    ]
    return spark.createDataFrame(rows, "month_start date, state string, budget_revenue double").select(
        date_key(F.col("month_start")).alias("date_key"),
        "month_start",
        "state",
        F.col("budget_revenue").cast("decimal(14,2)").alias("budget_revenue"),
        F.lit(True).alias("is_synthetic"),
    )


# --- run -----------------------------------------------------------------------------

def run(spark: SparkSession, dq: DQ) -> dict[str, DataFrame]:
    print("Gold: building star schema")
    geo = silver(spark, "geolocation")
    dims = {
        "dim_date": build_dim_date(spark),
        "dim_state": build_dim_state(spark),
        "dim_customer": build_dim_customer(silver(spark, "customers"), geo),
        "dim_seller": build_dim_seller(silver(spark, "sellers"), geo),
        "dim_product": build_dim_product(silver(spark, "products")),
    }
    for name, df in dims.items():
        save(df, name)
    g = {name: spark.read.format("delta").load(f"{GOLD_DIR}/{name}") for name in dims}

    fact = build_fact_order_item(
        silver(spark, "order_items"), silver(spark, "orders"), silver(spark, "order_reviews"),
        g["dim_customer"], g["dim_seller"],
    )
    save(fact, "fact_order_item")
    g["fact_order_item"] = spark.read.format("delta").load(f"{GOLD_DIR}/fact_order_item")

    save(build_fact_budget(spark, g["fact_order_item"]), "fact_budget")
    g["fact_budget"] = spark.read.format("delta").load(f"{GOLD_DIR}/fact_budget")

    print("Gold: data-quality checks")
    L, f = "gold", g["fact_order_item"]
    silver_items = silver(spark, "order_items")
    dq.add(L, "fact_order_item", "row count equals silver order_items", "error",
           abs(f.count() - silver_items.count()), silver_items.count(), "no rows lost or duplicated by joins")
    silver_rev = (
        silver_items.join(silver(spark, "orders").where("order_status = 'delivered'"), "order_id")
        .agg(F.sum("price")).first()[0]
    )
    gold_rev = f.where("is_revenue").agg(F.sum("price")).first()[0]
    dq.add(L, "fact_order_item", "revenue reconciles to silver", "error",
           0 if gold_rev == silver_rev else 1, 1, f"gold {gold_rev} vs silver {silver_rev}")
    dq.unique_key(L, "fact_order_item", f, ["order_id", "order_item_id"])
    for key, dim in [("customer_id", "dim_customer"), ("seller_id", "dim_seller"), ("product_id", "dim_product")]:
        dq.has_parent(L, "fact_order_item", f, g[dim], key, dim)
    dq.has_parent(L, "fact_order_item", f.withColumnRenamed("order_date_key", "date_key"), g["dim_date"],
                  "date_key", "dim_date")
    dq.has_parent(L, "fact_order_item", f.withColumnRenamed("customer_state", "state"), g["dim_state"],
                  "state", "dim_state")
    dq.has_parent(L, "fact_budget", g["fact_budget"], g["dim_state"], "state", "dim_state")
    dq.unique_key(L, "fact_budget", g["fact_budget"], ["date_key", "state"])
    dq.condition(L, "fact_order_item", f, "distance available", "distance_km IS NULL", severity="warn",
                 note="customer or seller zip has no coordinates")
    for name, df in g.items():
        print(f"  gold.{name:<16} {df.count():>9,} rows")
    return g


if __name__ == "__main__":
    import uuid

    from src.spark_session import get_spark

    spark = get_spark("gold")
    dq = DQ(run_id=str(uuid.uuid4())[:8])
    run(spark, dq)
    dq.save(spark, f"{SILVER_DIR}/dq_results")
    if dq.failures():
        raise SystemExit(f"{len(dq.failures())} data-quality rule(s) failed")
    spark.stop()
