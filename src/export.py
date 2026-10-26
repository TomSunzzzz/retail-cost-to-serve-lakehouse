"""Export Gold tables (and DQ results) as single Parquet files for Power BI Desktop.

Power BI Desktop reads one Parquet file per table via Get data > Parquet. In Microsoft Fabric
this step is not needed: Power BI reads the Lakehouse Delta tables directly (Direct Lake).
"""
from __future__ import annotations

import glob
import shutil
from pathlib import Path

from pyspark.sql import SparkSession

from src.config import EXPORT_DIR, GOLD_DIR, SILVER_DIR

GOLD_TABLES = [
    "dim_date", "dim_state", "dim_customer", "dim_seller", "dim_product",
    "fact_order_item", "fact_budget",
]


def write_single_parquet(df, target: Path):
    tmp = target.with_suffix(".tmp")
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp))
    part = glob.glob(str(tmp / "part-*.parquet"))[0]
    shutil.move(part, target)
    shutil.rmtree(tmp)


def run(spark: SparkSession):
    out = Path(EXPORT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    sources = {t: f"{GOLD_DIR}/{t}" for t in GOLD_TABLES}
    sources["dq_results"] = f"{SILVER_DIR}/dq_results"
    for table, path in sources.items():
        df = spark.read.format("delta").load(path)
        write_single_parquet(df, out / f"{table}.parquet")
        print(f"  export/{table}.parquet")


if __name__ == "__main__":
    from src.spark_session import get_spark

    spark = get_spark("export")
    run(spark)
    spark.stop()
