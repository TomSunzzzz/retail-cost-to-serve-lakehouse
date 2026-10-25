# AGENTS.md — shared instructions for every AI agent on this project

Several AI coding agents work on this repository (Claude Code, OpenAI Codex, DeepSeek-based tools). The project owner is Tom (Zhihan Sun). Read this file first, then `docs/PLAN.md` and `docs/HANDOFF.md`, before changing anything.

## What this project is

**Retail Sales & Cost-to-Serve Lakehouse.** An end-to-end data pipeline over the public Olist Brazilian e-commerce dataset (about 100k orders, 9 CSV tables), built with a medallion architecture (Bronze → Silver → Gold) on PySpark + Delta Lake, and a Power BI report on top. It answers three business questions:

1. How much are we selling? (by month, region, category)
2. Are we on budget? (actual vs budget)
3. Where is fulfilment too expensive? (cost-to-serve: freight as a share of revenue, by region / seller / category)

The stack mirrors a Microsoft Fabric Lakehouse (Spark + Delta), so the code can later be moved into Fabric notebooks with minimal change.

## Layout

```
data_set/archive/      raw Olist CSVs (input; never modified; not committed)
lakehouse/             pipeline output: bronze/, silver/, gold/ Delta tables (not committed)
src/                   pipeline code (PySpark)
scripts/               one-off helpers (smoke test, budget generator)
powerbi/               Power BI .pbix file and screenshots
docs/PLAN.md           workflow, layer specs, minimum product checklist
docs/HANDOFF.md        running log: what is done, what is next, who did it
Dockerfile, docker-compose.yml   local Spark 3.5 + Delta runtime
```

## How to run

Docker Desktop must be running.

```bash
docker compose build
docker compose run --rm spark python scripts/smoke_test.py
```

## Rules for every agent

1. **Raw data is read-only.** Never edit or delete anything in `data_set/`.
2. **Log your work in `docs/HANDOFF.md`** at the end of every session: date, agent name, what changed, what is next, anything broken. The next agent starts from there.
3. **Keep to the plan in `docs/PLAN.md`.** If you change the design (table names, layers, rules), update PLAN.md in the same change.
4. **English only** in code, comments, docs, commit messages and the report. This repository is public and is part of a job application.
5. **Be honest in the README.** The budget table is synthetic and must be labelled so. The README states that the project was built with AI coding assistants under Tom's design and review.
6. **Tom must be able to explain every step.** Prefer clear, readable PySpark over clever code. Each pipeline step gets a short docstring saying what it does and why.
7. **No secrets in the repo.** No API keys, tokens or credentials in any file.
8. Do not commit `data_set/` or `lakehouse/` (see `.gitignore`).
