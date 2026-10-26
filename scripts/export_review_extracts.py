"""Export the extra tables a human reviewer needs, beyond the Gold export.

Writes lakehouse/review/*.parquet: row counts per layer, the seller-city cleaning
(before -> after), and the product-category mapping. scripts/build_review_workbook.py
then turns these plus lakehouse/export/*.parquet into an Excel review workbook.

Run: docker compose run --rm spark python -m scripts.export_review_extracts
"""
from pathlib import Path

from pyspark.sql import functions as F

from src.config import BRONZE_DIR, GOLD_DIR, LAKEHOUSE_DIR, SILVER_DIR, SOURCES
from src.export import GOLD_TABLES, write_single_parquet
from src.spark_session import get_spark

OUT = Path(LAKEHOUSE_DIR) / "review"


def main():
    spark = get_spark("review-extracts")
    OUT.mkdir(parents=True, exist_ok=True)
    load = lambda path: spark.read.format("delta").load(path)

    counts = []
    for table in SOURCES:
        counts.append(("bronze", table, load(f"{BRONZE_DIR}/{table}").count()))
        counts.append(("silver", table, load(f"{SILVER_DIR}/{table}").count()))
    for table in GOLD_TABLES:
        counts.append(("gold", table, load(f"{GOLD_DIR}/{table}").count()))
    write_single_parquet(
        spark.createDataFrame(counts, "layer string, table_name string, row_count long"), OUT / "row_counts.parquet"
    )

    sellers = load(f"{SILVER_DIR}/sellers").select(
        "seller_id", F.col("city_raw").alias("city_before"), F.col("city").alias("city_after"), "state",
        (F.lower(F.trim("city_raw")) != F.coalesce(F.col("city"), F.lit(""))).alias("changed"),
    )
    write_single_parquet(sellers.orderBy(F.desc("changed"), "city_after"), OUT / "seller_city_cleaning.parquet")

    categories = (
        load(f"{SILVER_DIR}/products")
        .groupBy(F.col("category_pt").alias("category_original"), F.col("category").alias("category_english"))
        .agg(F.count("*").alias("products"))
        .orderBy(F.desc("products"))
    )
    write_single_parquet(categories, OUT / "category_mapping.parquet")

    print(f"review extracts written to {OUT}")
    spark.stop()


if __name__ == "__main__":
    main()
