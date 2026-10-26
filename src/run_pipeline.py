"""Run the whole pipeline: Bronze -> Silver -> Gold -> export for Power BI.

Run: docker compose run --rm spark python -m src.run_pipeline

One run_id ties together every data-quality result written during the run.
Any error-level data-quality failure stops the pipeline before Gold is built or exported.
"""
from __future__ import annotations

import time
import uuid

from src import bronze, export, gold, silver
from src.config import SILVER_DIR
from src.dq import DQ
from src.spark_session import get_spark


def main():
    started = time.time()
    run_id = time.strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:4]
    spark = get_spark("retail-lakehouse")
    dq = DQ(run_id)
    print(f"Run {run_id}")

    def stop_if_failed(layer):
        if dq.failures():
            dq.save(spark, f"{SILVER_DIR}/dq_results")
            names = ", ".join(f"{c.table}: {c.rule}" for c in dq.failures())
            raise SystemExit(f"Stopped after {layer}: data-quality failure(s): {names}")

    print("1/4 Bronze")
    bronze.run(spark)
    print("2/4 Silver")
    silver.run(spark, dq)
    stop_if_failed("Silver")
    print("3/4 Gold")
    gold.run(spark, dq)
    stop_if_failed("Gold")
    dq.save(spark, f"{SILVER_DIR}/dq_results")
    print("4/4 Export for Power BI")
    export.run(spark)

    statuses = [c.status for c in dq.checks]
    print(
        f"Done in {time.time() - started:.0f}s. Data quality: "
        f"{statuses.count('PASS')} pass, {statuses.count('WARN')} warn, {statuses.count('FAIL')} fail."
    )
    spark.stop()


if __name__ == "__main__":
    main()
