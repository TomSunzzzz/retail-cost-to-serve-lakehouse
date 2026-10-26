"""Silver layer: clean, typed, de-duplicated tables, plus data-quality checks.

Issues found when profiling Bronze, and how this layer handles them:
- Everything is text in Bronze -> cast to timestamps, integers and decimals here.
- Seller cities typed in many ways ('sp / sp', 'auriflama/sp', 'rio de janeiro, rio de janeiro,
  brasil', a zip code) -> one normalised spelling (src/cleaning.normalize_city).
- Accents in geolocation cities ('teresópolis') -> stripped so they join with customer cities.
- Geolocation has ~1M rows for ~19k zip prefixes, and some points outside Brazil
  -> drop points outside Brazil, keep one row per zip prefix (median coordinates).
- Two product categories have no English translation, 610 products have no category
  -> add the two translations, label missing ones 'unknown'.
- Product columns are misspelled in the source ('lenght') -> renamed.
- Delivered orders without a delivery date -> kept, flagged, excluded from delivery-time metrics.
All order statuses are kept here; Gold decides what counts as revenue.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.cleaning import normalize_city, strip_accents, to_money, to_ts
from src.config import BRONZE_DIR, SILVER_DIR
from src.dq import DQ

# Brazil's bounding box, used to reject impossible coordinates.
BRAZIL_LAT = (-34.0, 6.0)
BRAZIL_LNG = (-74.0, -34.0)

MISSING_TRANSLATIONS = {
    "pc_gamer": "gaming_pc",
    "portateis_cozinha_e_preparadores_de_alimentos": "portable_kitchen_and_food_preparers",
}


def bronze(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.format("delta").load(f"{BRONZE_DIR}/{table}").drop("_source_file")


def save(df: DataFrame, table: str):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER_DIR}/{table}")


def build_orders(spark):
    df = bronze(spark, "orders").select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.lower(F.trim("order_status")).alias("order_status"),
        to_ts("order_purchase_timestamp").alias("purchased_at"),
        to_ts("order_approved_at").alias("approved_at"),
        to_ts("order_delivered_carrier_date").alias("shipped_at"),
        to_ts("order_delivered_customer_date").alias("delivered_at"),
        to_ts("order_estimated_delivery_date").alias("estimated_delivery_at"),
        "_ingested_at",
    )
    return df.dropDuplicates(["order_id"]).withColumn(
        "is_missing_delivery_date",
        (F.col("order_status") == "delivered") & F.col("delivered_at").isNull(),
    )


def build_order_items(spark):
    df = bronze(spark, "order_items").select(
        F.trim("order_id").alias("order_id"),
        F.col("order_item_id").cast("int").alias("order_item_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("seller_id").alias("seller_id"),
        to_ts("shipping_limit_date").alias("shipping_limit_at"),
        to_money("price").alias("price"),
        to_money("freight_value").alias("freight_value"),
        "_ingested_at",
    )
    return df.dropDuplicates(["order_id", "order_item_id"])


def build_order_payments(spark):
    df = bronze(spark, "order_payments").select(
        F.trim("order_id").alias("order_id"),
        F.col("payment_sequential").cast("int").alias("payment_sequential"),
        F.lower(F.trim("payment_type")).alias("payment_type"),
        F.col("payment_installments").cast("int").alias("payment_installments"),
        to_money("payment_value").alias("payment_value"),
        "_ingested_at",
    )
    return df.dropDuplicates(["order_id", "payment_sequential"])


def build_order_reviews(spark):
    df = bronze(spark, "order_reviews").select(
        F.trim("review_id").alias("review_id"),
        F.trim("order_id").alias("order_id"),
        F.col("review_score").cast("int").alias("review_score"),
        F.trim("review_comment_title").alias("review_comment_title"),
        F.trim("review_comment_message").alias("review_comment_message"),
        to_ts("review_creation_date").alias("review_created_at"),
        to_ts("review_answer_timestamp").alias("review_answered_at"),
        "_ingested_at",
    )
    return df.dropDuplicates(["review_id", "order_id"])


def build_customers(spark):
    df = bronze(spark, "customers").select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias("customer_unique_id"),
        F.lpad(F.trim("customer_zip_code_prefix"), 5, "0").alias("zip_code_prefix"),
        normalize_city(F.col("customer_city")).alias("city"),
        F.upper(F.trim("customer_state")).alias("state"),
        "_ingested_at",
    )
    return df.dropDuplicates(["customer_id"])


def build_sellers(spark):
    df = bronze(spark, "sellers").select(
        F.trim("seller_id").alias("seller_id"),
        F.lpad(F.trim("seller_zip_code_prefix"), 5, "0").alias("zip_code_prefix"),
        F.col("seller_city").alias("city_raw"),
        normalize_city(F.col("seller_city")).alias("city"),
        F.upper(F.trim("seller_state")).alias("state"),
        "_ingested_at",
    )
    return df.dropDuplicates(["seller_id"])


def build_category_translation(spark):
    src = bronze(spark, "category_translation").select(
        F.trim("product_category_name").alias("category_pt"),
        F.trim("product_category_name_english").alias("category_en"),
    )
    extra = spark.createDataFrame(list(MISSING_TRANSLATIONS.items()), "category_pt string, category_en string")
    return src.unionByName(extra).dropDuplicates(["category_pt"])


def build_products(spark, translation: DataFrame):
    df = bronze(spark, "products").select(
        F.trim("product_id").alias("product_id"),
        F.trim("product_category_name").alias("category_pt"),
        F.col("product_name_lenght").cast("int").alias("product_name_length"),
        F.col("product_description_lenght").cast("int").alias("product_description_length"),
        F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
        F.col("product_weight_g").cast("int").alias("product_weight_g"),
        F.col("product_length_cm").cast("int").alias("product_length_cm"),
        F.col("product_height_cm").cast("int").alias("product_height_cm"),
        F.col("product_width_cm").cast("int").alias("product_width_cm"),
        "_ingested_at",
    ).dropDuplicates(["product_id"])
    return df.join(translation, "category_pt", "left").withColumn(
        "category", F.coalesce(F.col("category_en"), F.lit("unknown"))
    ).drop("category_en")


def build_geolocation(spark):
    df = bronze(spark, "geolocation").select(
        F.lpad(F.trim("geolocation_zip_code_prefix"), 5, "0").alias("zip_code_prefix"),
        F.col("geolocation_lat").cast("double").alias("lat"),
        F.col("geolocation_lng").cast("double").alias("lng"),
        normalize_city(F.col("geolocation_city")).alias("city"),
        F.upper(F.trim("geolocation_state")).alias("state"),
    )
    in_brazil = df.where(
        F.col("lat").between(*BRAZIL_LAT) & F.col("lng").between(*BRAZIL_LNG)
    )
    return in_brazil.groupBy("zip_code_prefix").agg(
        F.percentile_approx("lat", 0.5).alias("lat"),
        F.percentile_approx("lng", 0.5).alias("lng"),
        F.mode("city").alias("city"),
        F.mode("state").alias("state"),
        F.count("*").alias("source_points"),
    )


def run(spark: SparkSession, dq: DQ) -> dict[str, DataFrame]:
    print("Silver: building tables")
    translation = build_category_translation(spark)
    tables = {
        "orders": build_orders(spark),
        "order_items": build_order_items(spark),
        "order_payments": build_order_payments(spark),
        "order_reviews": build_order_reviews(spark),
        "customers": build_customers(spark),
        "sellers": build_sellers(spark),
        "category_translation": translation,
        "products": build_products(spark, translation),
        "geolocation": build_geolocation(spark),
    }
    for name, df in tables.items():
        save(df, name)
    t = {name: spark.read.format("delta").load(f"{SILVER_DIR}/{name}") for name in tables}

    print("Silver: data-quality checks")
    L = "silver"
    keys = {
        "orders": ["order_id"],
        "order_items": ["order_id", "order_item_id"],
        "order_payments": ["order_id", "payment_sequential"],
        "order_reviews": ["review_id", "order_id"],
        "customers": ["customer_id"],
        "sellers": ["seller_id"],
        "products": ["product_id"],
        "geolocation": ["zip_code_prefix"],
    }
    for name, k in keys.items():
        dq.not_null(L, name, t[name], k)
        dq.unique_key(L, name, t[name], k)

    # referential integrity: every child row points to a real parent
    dq.has_parent(L, "order_items", t["order_items"], t["orders"], "order_id", "orders")
    dq.has_parent(L, "order_items", t["order_items"], t["products"], "product_id", "products")
    dq.has_parent(L, "order_items", t["order_items"], t["sellers"], "seller_id", "sellers")
    dq.has_parent(L, "orders", t["orders"], t["customers"], "customer_id", "customers")

    # business rules
    dq.condition(L, "order_items", t["order_items"], "price > 0", "price IS NULL OR price <= 0")
    dq.condition(L, "order_items", t["order_items"], "freight_value >= 0", "freight_value IS NULL OR freight_value < 0")
    dq.condition(L, "order_items", t["order_items"], "freight_value is not zero", "freight_value = 0",
                 severity="warn", note="free shipping is plausible; kept")
    dq.condition(L, "orders", t["orders"], "purchase timestamp parsed", "purchased_at IS NULL")
    dq.condition(L, "orders", t["orders"], "delivered orders have a delivery date", "is_missing_delivery_date",
                 severity="warn", note="kept; excluded from delivery-time metrics")
    dq.condition(L, "orders", t["orders"], "delivered on or after purchase", "delivered_at < purchased_at")
    dq.condition(L, "order_payments", t["order_payments"], "payment_value > 0", "payment_value <= 0",
                 severity="warn", note="zero-value vouchers")
    dq.condition(L, "order_payments", t["order_payments"], "payment_type is known", "payment_type = 'not_defined'",
                 severity="warn")
    reviews = t["order_reviews"]
    dq.add(L, "order_reviews", "review_id used by one order only", "warn",
           reviews.count() - reviews.select("review_id").distinct().count(), reviews.count(),
           "one review shared by several orders of the same purchase; key is (review_id, order_id)")
    dq.condition(L, "order_reviews", t["order_reviews"], "review_score between 1 and 5",
                 "review_score IS NULL OR review_score NOT BETWEEN 1 AND 5")
    dq.condition(L, "products", t["products"], "category has an English name",
                 "category = 'unknown' AND category_pt IS NOT NULL")
    dq.condition(L, "products", t["products"], "product has a category", "category_pt IS NULL",
                 severity="warn", note="labelled 'unknown'")
    dq.condition(L, "sellers", t["sellers"], "seller city is usable", "city IS NULL", severity="warn",
                 note="city field contained a zip code or was blank")
    dq.add(L, "geolocation", "points outside Brazil removed", "warn",
           bronze(spark, "geolocation").count() - t["geolocation"].agg(F.sum("source_points")).first()[0],
           bronze(spark, "geolocation").count(), "dropped before aggregation")
    customer_zips_missing = t["customers"].join(t["geolocation"], "zip_code_prefix", "left_anti").count()
    dq.add(L, "customers", "zip prefix found in geolocation", "warn", customer_zips_missing,
           t["customers"].count(), "no coordinates for these customers")
    return t


if __name__ == "__main__":
    import uuid

    from src.spark_session import get_spark

    spark = get_spark("silver")
    dq = DQ(run_id=str(uuid.uuid4())[:8])
    run(spark, dq)
    dq.save(spark, f"{SILVER_DIR}/dq_results")
    if dq.failures():
        raise SystemExit(f"{len(dq.failures())} data-quality rule(s) failed")
    spark.stop()
