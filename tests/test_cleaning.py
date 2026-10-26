"""Checks for the city normaliser, using real values found in the Olist data.

Run: docker compose run --rm spark python -m tests.test_cleaning
"""
from pyspark.sql import functions as F

from src.cleaning import normalize_city
from src.spark_session import get_spark

CASES = [
    # messy seller input -> expected
    ("Auriflama/SP", "auriflama"),
    ("andira-pr", "andira"),
    ("sao paulo sp", "sao paulo"),
    ("SP / SP", "sao paulo"),
    ("sbc/sp", "sao bernardo do campo"),
    ("lages - sc", "lages"),
    ("Rio de Janeiro, Rio de Janeiro, Brasil", "rio de janeiro"),
    ("rio de janeiro \\rio de janeiro", "rio de janeiro"),
    ("santa barbara d´oeste", "santa barbara d'oeste"),
    ("Teresópolis", "teresopolis"),
    ("04482255", None),
    # real city names that must NOT change
    ("igarape-miri", "igarape-miri"),
    ("embu-guacu", "embu-guacu"),
    ("governador dix-sept rosado", "governador dix-sept rosado"),
    ("sento se", "sento se"),
    ("pau d'arco", "pau d'arco"),
]


def main():
    spark = get_spark("test-cleaning")
    rows = (
        spark.createDataFrame([(raw,) for raw, _ in CASES], "raw string")
        .withColumn("clean", normalize_city(F.col("raw")))
        .collect()
    )
    got = {r["raw"]: r["clean"] for r in rows}
    failures = [(raw, exp, got[raw]) for raw, exp in CASES if got[raw] != exp]
    for raw, exp, actual in failures:
        print(f"FAIL  {raw!r}: expected {exp!r}, got {actual!r}")
    print(f"{len(CASES) - len(failures)}/{len(CASES)} city cases passed")
    spark.stop()
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
