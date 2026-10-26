# Handoff log

Newest entry on top. Every agent adds an entry at the end of its session.

---

## 2026-10-26 — Claude Code (Opus 5.5) — Bronze

**Done**
- `src/config.py` (paths + source tables with primary keys), `src/bronze.py`.
- Bronze runs in about 1 minute: `docker compose run --rm spark python -m src.bronze`. All 9 tables match their CSV record counts:
  orders 99,441 · order_items 112,650 · order_payments 103,886 · order_reviews 99,224 · customers 99,441 · sellers 3,095 · products 32,951 · geolocation 1,000,163 · category_translation 71.
- Note: the reviews CSV has 104,719 text lines but 99,224 records, because review comments contain line breaks. Bronze reads with `multiLine` and checks counts with Python's csv module, not `wc -l`.
- The container's Python is 3.8: use `from __future__ import annotations` for `list[dict]`-style hints.

**Next**
- Silver: `src/silver.py` + `src/dq.py` (rules in PLAN.md), then Gold, then `src/run_pipeline.py`.

---

## 2026-10-25 — Claude Code (Opus 5.5)

**Done**
- Dataset in place: `data_set/archive/`, 9 Olist CSVs, sizes match the original Kaggle release.
- Docker Desktop repaired (stale `.sock` files from an unclean shutdown blocked startup; the folders were renamed to `*.stale-20261025` under `%LOCALAPPDATA%`, nothing deleted). If Docker fails again with "The file cannot be accessed by the system" on a `.sock` file, do the same.
- Power BI Desktop installed (Microsoft Store).
- Repo skeleton: AGENTS.md, docs/PLAN.md, Dockerfile (Spark 3.5.3 + delta-spark 3.2.1, Delta jars baked into the image at build time), docker-compose.yml, src/spark_session.py, scripts/smoke_test.py.
- **Smoke test passed**: read 99,441 orders from CSV → wrote Delta → read back, history shows version 0 WRITE.
- Note: the image's interpreter is `python3`; the Dockerfile adds a `python` symlink.

**Next**
- Monday: Step 1 Bronze (`src/bronze.py`), Step 2 Silver + DQ (`src/silver.py`, `src/dq.py`), Step 3 Gold (`src/gold.py`), `src/run_pipeline.py`, synthetic budget (`scripts/make_budget.py`).

**Microsoft Fabric**
- Tom has a working Fabric login until about 2026-11-11. Fabric is now part of the main line (Tuesday), not a stretch goal.
- Approach: the local Docker pipeline stays the source of truth (it is reproducible after the Fabric access ends). The same PySpark code is run in Fabric (Lakehouse + Notebooks + Data Factory pipeline + Power BI semantic model) for screenshots and a short screen recording, saved under `docs/fabric/`.

**GitHub**
- Repo: https://github.com/TomSunzzzz/retail-cost-to-serve-lakehouse (private until the README and report are finished, then make public).
- GitHub CLI installed and logged in as TomSunzzzz; `git push` works from the command line.
