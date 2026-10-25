"""Smoke test: prove the local runtime can read a raw CSV, write a Delta table and read it back.

Run: docker compose run --rm spark python scripts/smoke_test.py
"""
import shutil
from pathlib import Path

from src.spark_session import get_spark

RAW = "data_set/archive/olist_orders_dataset.csv"
OUT = "lakehouse/_smoke/orders"

spark = get_spark("smoke-test")

raw = spark.read.option("header", True).csv(RAW)
print(f"[1/3] read CSV: {raw.count()} rows, {len(raw.columns)} columns")

raw.write.format("delta").mode("overwrite").save(OUT)
print(f"[2/3] wrote Delta table to {OUT}")

back = spark.read.format("delta").load(OUT)
history = spark.sql(f"DESCRIBE HISTORY delta.`/workspace/{OUT}`").select("version", "operation")
print(f"[3/3] read Delta back: {back.count()} rows; history:")
history.show(truncate=False)

spark.stop()
shutil.rmtree(Path(OUT).parent, ignore_errors=True)
print("SMOKE TEST PASSED")
