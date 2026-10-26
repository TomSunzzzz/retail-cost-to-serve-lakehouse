"""Bronze layer: land every raw CSV as a Delta table, unchanged.

Why:
- Keep an exact copy of the source so any later step can be re-run from it.
- Every column is read as a string, so nothing is lost or silently converted.
- Two audit columns record when each row was loaded and from which file (lineage).
- Each run checks that the row count in Delta matches the number of records in the CSV.
"""
from __future__ import annotations

import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import BRONZE_DIR, RAW_DIR, SOURCES


def count_csv_records(path: str) -> int:
    """Count records with Python's csv module, which handles line breaks inside quoted text."""
    with open(path, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1  # minus header


def read_raw_csv(spark: SparkSession, path: str):
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)  # keep everything as string in Bronze
        .option("multiLine", True)  # review comments contain line breaks
        .option("escape", '"')
        .csv(path)
    )


def run(spark: SparkSession) -> list[dict]:
    results = []
    for table, (file_name, _keys) in SOURCES.items():
        src = f"{RAW_DIR}/{file_name}"
        df = (
            read_raw_csv(spark, src)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(file_name))
        )
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(
            f"{BRONZE_DIR}/{table}"
        )

        delta_rows = spark.read.format("delta").load(f"{BRONZE_DIR}/{table}").count()
        csv_rows = count_csv_records(src)
        status = "OK" if delta_rows == csv_rows else "MISMATCH"
        results.append(
            {"table": table, "csv_records": csv_rows, "bronze_rows": delta_rows, "status": status}
        )
        print(f"  bronze.{table:<22} {delta_rows:>9,} rows  (csv {csv_rows:,})  {status}")

    if any(r["status"] != "OK" for r in results):
        raise RuntimeError("Bronze row counts do not match the source CSVs")
    return results


if __name__ == "__main__":
    from src.spark_session import get_spark

    spark = get_spark("bronze")
    Path(BRONZE_DIR).mkdir(parents=True, exist_ok=True)
    run(spark)
    spark.stop()
