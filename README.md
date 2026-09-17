# dbt Migration Agent

An 8-agent pipeline that automates migrating a Snowflake + dbt project to
Databricks — connectivity checks, macro/config fixes, source data
handling, Snowflake → Databricks SQL transpilation, execution, automatic
error diagnosis, and post-migration validation, each backed by real dbt
runs against a live SQL Warehouse rather than static analysis alone.

Point it at your project's path and it tells you what's blocking a clean
migration, fixes what it can automatically, and flags the rest for human
review with a specific reason — not a generic "manual migration needed."

## Why

Migrating a real dbt project off Snowflake usually means hand-fixing the
same handful of problem categories over and over: macros that assume
Snowflake-only functions, config fields that don't exist on Databricks,
Snowflake dialect SQL that needs rewriting, and dbt runs that fail in ways
that are individually diagnosable but tedious to chase down one model at a
time. This tool automates that loop instead of leaving it to a checklist.

## The 8 agents

| # | Agent | Does |
|---|---|---|
| 1 | Preflight | Connectivity, catalog/schema/audit-table setup, `dbt_project.yml` validation + auto-fix |
| 2 | Macro Resolver | Classifies every macro used, auto-generates the Databricks-side dispatch pattern |
| 3 | Data Loader | Redirects/copies source tables (e.g. Snowflake sample data → Databricks `samples`) |
| 4 | Analyzer | Parses the DAG, scores each model's migration complexity |
| 5 | Transpiler | Snowflake → Databricks SQL conversion (via Lakebridge/Morpheus), with its own corruption detectors |
| 6 | Executor | Runs the migrated project (`dbt run`) against a real SQL Warehouse |
| 7 | Diagnostician | Classifies failures, auto-fixes what's safe, LLM fallback for the rest |
| 8 | Validator | Schema, row-count, checksum, and business-rule (dbt test) checks on what passed |

Full responsibilities and the failure-routing rules between agents are in
[AGENT_DESIGN.md](AGENT_DESIGN.md).

## Quick start

```bash
cp .env.example .env   # fill in your Databricks host/token/warehouse/catalog + project path
docker compose up -d --build
```

Then either open **http://localhost:8501** (Streamlit UI) or run
individual/full-pipeline commands via
`docker compose exec migration-agent python scripts/cli.py <command> /data/project`.

Full setup details, credential handling, and a local-development path (for
working on the agents themselves) are in [SETUP.md](SETUP.md).

## Status

Built and validated end-to-end against a real TPC-H-based Snowflake+dbt
sample project (52 models) — every corruption mode, dialect gap, and
false-positive/false-negative found along the way was root-caused against
a live warehouse, not assumed from docs. See
[CHECKPOINT.md](CHECKPOINT.md) for the full running log of what was built,
what broke, and how it was fixed; [FINDINGS.md](FINDINGS.md) and
[MACRO_ANALYSIS.md](MACRO_ANALYSIS.md) for the specific migration patterns
discovered. This is a working prototype validated on one real project, not
yet battle-tested across many — the agents are built to generalize (no
project-specific logic baked in beyond documented, generic heuristics),
but treat that as a design intent to verify against your own project, not
a guarantee.

## Requirements

Docker + Docker Compose, and a Databricks workspace with a Unity Catalog
catalog and a SQL Warehouse already created. See
[SETUP.md](SETUP.md#what-you-need-first) for details.
