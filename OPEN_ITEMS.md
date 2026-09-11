# DBT Migration Agent — Open Items

Tracks all unresolved dependencies, decisions, and blockers.
Updated as items are resolved or new ones surface.

---

## Status Legend
| Status | Meaning |
|--------|---------|
| 🔴 Open | Unresolved, actively blocking |
| 🟡 Parked | Deprioritized, not blocking POC |
| 🟢 Resolved | Confirmed and documented |

---

## Official Project — Open Items

| # | Item | Status | Blocking | Notes |
|---|------|--------|---------|-------|
| 1 | Macro centralization — shared package or scattered? | 🔴 Open | Yes — Macro Resolver scan strategy | |
| 2 | `cli.py` routing logic from Snowflake POC | 🔴 Open | Yes — slash command routing | Requires repo access |
| 3 | LLM model used for Diagnostician in Snowflake POC | 🔴 Open | No — deterministic fixes first | |
| 4 | Model partitioning strategy across developers | 🟡 Parked | No — post-POC | |
| 5 | LATERAL FLATTEN — inline vs macro in actual client repo | 🔴 Open | Yes — Bronze effort classification | First grep to run on repo access |
| 6 | `dbt_utils` version audit in actual client repo | 🔴 Open | Yes — macro compatibility | Check `packages.yml` |
| 7 | Snowflake warehouse sizes per model in production | 🔴 Open | Yes — Executor Agent warehouse mapping | |

---

## Learning POC — Open Items

| # | Item | Status | Blocking | Notes |
|---|------|--------|---------|-------|
| 1 | `dbt_constraints` → native dbt contracts | 🔴 Open | Yes — Macro Resolver Agent automates this | 43 refs across 6 files |
| 2 | Snapshot + incremental MERGE strategy testing | 🔴 Open | No | Not covered in sample project |
| 3 | `get_stream` macro → Delta CDF architectural decision | 🔴 Open | Yes — blocks stream models | Hard stop in agent |
| 4 | `streaming_table` STREAM keyword requirement | 🔴 Open | No — architectural per model | `dynamic_table` → `streaming_table` not sufficient |
| 5 | `dbt_utils` macros not yet tested through Lakebridge | 🔴 Open | Yes | `generate_surrogate_key`, `star`, `union_relations` |
| 6 | Custom materializations testing | 🔴 Open | No | If any exist in official repo |
| 7 | Nested macros testing through Lakebridge | 🟡 Parked | No | |
| 8 | Mock staging data for `STAGING` source | 🔴 Open | Yes — unblocks SCD model testing | `CUSTOMER` and `SALESORDER` tables needed |

---

## Jinja / dbt Constructs — Testing Status

| Construct | Priority | Status |
|-----------|----------|--------|
| `{{ ref() }}` | 🔴 High | ✅ Validated |
| `{{ this }}` | 🔴 High | ✅ Validated |
| `{% if is_incremental() %}` | 🔴 High | ✅ Validated |
| `{{ adapter.dispatch() }}` | 🔴 High | ✅ Validated |
| `{% for item in list %}` | 🟡 Medium | ✅ Validated |
| `{% set x = ... %}` | 🟡 Medium | ✅ Validated |
| `{{ var('variable_name') }}` | 🟡 Medium | ✅ Validated |
| `dbt_utils` macros | 🔴 High | 🔴 Open |
| Custom materializations | 🔴 High | 🔴 Open |
| Nested macros | 🟡 Medium | 🔴 Open |
| Incremental MERGE strategy | 🟡 Medium | 🔴 Open |
| `{%- -%}` whitespace control | 🟡 Medium | 🔴 Open |

---

## Decisions Made

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Macros migrated via dispatch pattern, not rewritten | Preserves original logic, fixes once covers all models |
| 2 | `dbt run --no-fail-fast` for bulk error detection | Full error surface in one pass (`--continue-on-error` does not exist in dbt) |
| 3 | `blocked_by_upstream` flag in audit table | Distinguishes genuine failures from cascade failures |
| 4 | Pattern library shared across all developers | Avoids rework — early runs build library for later runs |
| 5 | Diagnostician feedback loop inside task, not recursive Workflow | Simpler, easier to debug for POC |
| 6 | Warehouse size from Snowflake reference first, Analyzer fallback | Preserves existing performance tuning decisions |
| 7 | Feature branches per developer → common repo | Standard Git workflow, no schema isolation needed |
| 8 | Serverless SQL Warehouse for POC | Lower startup time, no cluster management, auto-scaling |
| 9 | Layer-level parallelism for POC | Easier to reason about failures |
| 10 | `/dbt-migrate:run` always full pipeline, no resume | Individual agent commands handle partial/resume |
| 11 | Raw `.sql` files as Transpiler input (macros unresolved) | Preserves Jinja, Macro Resolver handles macros separately |
| 12 | Lakebridge Morpheus as primary transpiler | Only Lakebridge component relevant for SQL |
| 13 | Overwrite original macro file when applying dispatch pattern | dbt disallows duplicate macro names across files |
| 14 | Use Python scripts for multi-line yml block commenting | sed cannot handle YAML block structure |
| 15 | Remove Snowflake config properties entirely from config() blocks | Comments inside config() blocks cause parse errors |
| 16 | VS Code + Claude Code for agent development phase | Better tool for file editing, terminal access, no log uploads |

---

## POC Results (Sample Project — snowflake-dbt-demo)

| Metric | Result |
|--------|--------|
| Total models | 52 |
| Passing (v10) | 31 (60%) |
| Adjusted pass rate (excl. POC skips + hard stops) | 77% |
| Hard stops | 3 — `customer_cdc_stream`, `dim_customer_changes`, architectural streaming |
| POC skips | 5 — Python cluster, Cybersyn data, Snowflake system table |
| Manual runs to reach stable state | 10 |
| Patterns discovered | 25+ (see `FINDINGS.md` Section 4) |

---

## Resolution Log

| Date | Item | Resolution |
|------|------|------------|
| Session 1 | `dbt run --continue-on-error` flag | Does not exist — use `--no-fail-fast` |
| Session 1 | TPC-H source data | Databricks `samples.tpch` catalog — no loading needed |
| Session 1 | `dim_calendar_day` rewrite | Manual rewrite — `GENERATOR`+`seq4()` → `explode(sequence())`, `decode()` → `CASE WHEN` |
| Session 1 | Jinja constructs `ref()`, `this`, `is_incremental()` | ✅ All preserved through Lakebridge transpilation |
