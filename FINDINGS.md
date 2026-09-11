# DBT Migration Agent — Findings

## 1. dbt_project.yml — Snowflake-Specific Configs

Issues found and fixes applied manually. These become automated Preflight Agent tasks.

| Config | Issue | Fix |
|--------|-------|-----|
| `profile: "SNOWFLAKE"` | Profile name mismatch | Rename to `DATABRICKS` |
| `+snowflake_warehouse: "{{ target.warehouse }}"` | Snowflake-only target property | Comment out |
| `+database/+schema: env_var(target.*)` | `target.database/schema` undefined pre-connection | Comment out |
| `materialized='dynamic_table'` | Snowflake-only materialization | Replace with `streaming_table` |
| `snowflake_warehouse=target.warehouse` in config block | Snowflake-only config | Remove entirely |
| `transient=false` in config block | Snowflake-only config | Remove entirely — do NOT replace with comment |

**Critical note:** Snowflake-specific config properties must be **removed**, not commented out.
Jinja and SQL comments inside `{{ config() }}` blocks cause parse errors.

---

## 2. Package Compatibility

| Package | Version | Databricks Support | Action |
|---------|---------|-------------------|--------|
| `dbt-labs/dbt_utils` | 1.4.1 | ✅ Full support | Keep |
| `brooklyn-data/dbt_artifacts` | 2.11.0 | ✅ Works | Keep — deprecation warnings only |
| `Snowflake-Labs/dbt_constraints` | 1.0.9 | ❌ No Databricks support | Replace with dbt native contracts |

### dbt_constraints Replacement
- 43 references across 6 yml files
- Replace `dbt_constraints.primary_key/foreign_key/unique_key` with native dbt `constraints:` blocks
- `dbt-databricks` adapter supports constraints natively (dbt 1.5+)
- Reference: https://docs.getdbt.com/reference/global-configs/databricks-changes
- Affected files: `models/gold/_models.yml`, `models/other/tpc_h_benchmarks/_models.yml`, `models/other/_models.yml`, `models/bronze/_sources.yml`, `models/bronze/_models.yml`, `models/silver/_models.yml`

---

## 3. Macro Analysis

### 3.1 Dispatch Pattern (Template)

```sql
{% macro macro_name(args) %}
    {{ return(adapter.dispatch('macro_name', 'project_name')(args)) }}
{% endmacro %}

{% macro default__macro_name(args) %}
    -- original Snowflake code
{% endmacro %}

{% macro databricks__macro_name(args) %}
    -- Databricks equivalent or logged no-op
{% endmacro %}
```

**Critical:** `project_name` read dynamically from `dbt_project.yml` → `name` field, never hardcoded.
**Critical:** Always overwrite original macro file — never create a duplicate alongside it (dbt disallows duplicate macro names).

### 3.2 Macro Categorization

#### Auto-Resolve
| Macro | Databricks Action | Status |
|-------|------------------|--------|
| `create_masking_policies` | No-op stub — Unity Catalog column masks post-migration | ✅ Validated |
| `set_warehouse` | No-op stub — Executor Agent handles sizing | ✅ Validated |
| `integration_key` | Dispatch — `CAST(field AS STRING)` replaces `field::VARCHAR` | ✅ Validated |
| `snowflake_query_tags` | No-op stub | Pending |
| `copy_log_to_snowflake` | No-op stub | Pending |
| `tag_columns` | No-op stub — Unity Catalog tagging | Pending |
| `greatest_date` | Pure SQL — portable as-is | Pending |
| `custom_schemas` | Standard dbt macro — portable | Pending |

#### Flag for Review
| Macro | Issue | Status |
|-------|-------|--------|
| `get_scd_sql` | `sysdate()`, `null::type` casting, sequence calls | ✅ Partially fixed |
| `sequence_nextval_as_surrogate_key` | No sequences — `row_number()` + high watermark | ✅ Validated |
| `sequence_get_nextval` | No sequences — `row_number()` | ✅ Validated |
| `insert_ghost_key` | Pure SQL — portable as-is | ✅ Validated |
| `snowflake_optimized_snapshot_hash_arguments` | `databricks__` dispatch with `STRING` cast | ✅ Validated |

#### Hard Stop
| Macro | Issue |
|-------|-------|
| `get_stream` | Snowflake Streams → Delta CDF — architectural redesign needed |
| `snowflake_get_sequence` | No sequences in Databricks — design decision needed |

---

## 4. SQL Dialect Fixes (Transpiler Post-Processor)

### 4.1 Type Casting
| Snowflake | Databricks | Notes |
|-----------|------------|-------|
| `field::varchar` | `CAST(field AS STRING)` | Regex: `([\w.]+)::varchar` — must handle `table.field` |
| `field::VARCHAR(n)` | `CAST(field AS STRING)` | Drop size parameter |
| `field::integer` | `CAST(field AS INTEGER)` | |
| `field::timestamp_ntz` | `CAST(field AS TIMESTAMP)` | |
| `field::number(p,s)` | `CAST(field AS DECIMAL)` | |
| `field::date` | `CAST(field AS DATE)` | |
| `null::integer` | `CAST(null AS INTEGER)` | Common in SCD patterns |
| `null::varchar` | `CAST(null AS STRING)` | Common in SCD patterns |
| `null::timestamp_ntz` | `CAST(null AS TIMESTAMP)` | Common in SCD patterns |

**Critical regex:** Use `([\w.]+)::type` not `(\w+)::type`.
Failure produces broken SQL: `table.CAST(column AS STRING)` instead of `CAST(table.column AS STRING)`.

### 4.2 Functions
| Snowflake | Databricks | Notes |
|-----------|------------|-------|
| `sysdate()` / `SYSDATE()` | `current_timestamp()` | Fix both upper and lowercase |
| `iff(cond, true, false)` | `CASE WHEN cond THEN true ELSE false END` | |
| `decode(expr, v1, r1, v2, r2)` | `CASE WHEN expr=v1 THEN r1 WHEN expr=v2 THEN r2 END` | |
| `last_day(date, 'MONTH')` | `last_day(date)` | |
| `last_day(date, 'WEEK')` | `dateadd(day, 6, date_trunc('WEEK', date))` | |
| `last_day(date, 'YEAR')` | Date arithmetic | Manual calculation |
| `CURRENT_WAREHOUSE()` | No-op | |
| `CURRENT_ROLE()` | `current_user()` | Different concept |

### 4.3 Data Types in yml files
| Snowflake | Databricks |
|-----------|------------|
| `data_type: number` | `data_type: bigint` |
| `data_type: varchar` | `data_type: string` |
| `data_type: timestamp_ntz` | `data_type: timestamp` |

**Note:** `timestamp_ntz` in yml resolves to `Undefined` tag value — causes parse error.

### 4.4 Sampling
| Snowflake | Databricks |
|-----------|------------|
| `table AS alias SAMPLE ROW (n ROWS)` | `table TABLESAMPLE (n ROWS) AS alias` |

Alias must come **after** TABLESAMPLE clause, not before.

### 4.5 Array Literals
| Context | Snowflake | Databricks |
|---------|-----------|------------|
| SQL query | `['val1', 'val2']` | `ARRAY('val1', 'val2')` |
| dbt config block | `['val1', 'val2']` | `['val1', 'val2']` — do NOT convert |

**Critical:** Array regex must NOT match dbt config blocks (tags, accepted_values, etc.).

### 4.6 Data Generation
| Snowflake | Databricks |
|-----------|------------|
| `TABLE(GENERATOR(rowcount => n))` with `seq4()` | `explode(sequence(1, n))` |

### 4.7 Session Commands (Remove)
| Command | Action |
|---------|--------|
| `ALTER SESSION SET WEEK_START = n` | Remove from pre_hook |
| `ALTER SESSION SET WEEK_OF_YEAR_POLICY = n` | Remove from pre_hook |
| `USE WAREHOUSE name` | No-op |

### 4.8 Materialization
| Snowflake | Databricks | Notes |
|-----------|------------|-------|
| `dynamic_table` | `materialized_view` | Corrected 2026-09-12 — see MACRO_ANALYSIS.md Section 4.6. `streaming_table` was the original guidance but is wrong for general use: confirmed against the real warehouse it rejects aggregation and self-referencing correlated subqueries, both common in real dynamic_table models. `materialized_view` is batch re-computation on a schedule (no such restriction) and is the true Databricks equivalent of Snowflake's dynamic_table concept. `streaming_table` is still correct, but only for genuinely append-only ingestion sources. |

### 4.9 Stream Metadata (Hard Stop)
| Snowflake | Status |
|-----------|--------|
| `metadata$action` | 🔴 Hard stop |
| `metadata$isupdate` | 🔴 Hard stop |
| `SHOW STREAMS` | 🔴 Hard stop |

### 4.10 Snowflake-Specific Extract Parts
| Snowflake | Databricks |
|-----------|------------|
| `extract(dayofweekiso from date)` | `CASE dayofweek(date) WHEN 1 THEN 7 ELSE dayofweek(date) - 1 END` |
| `extract(weekiso from date)` | `weekofyear(date)` |
| `extract(yearofweekiso from date)` | `year(date)` |

---

## 5. yml File Handling

**Use Python scripts for multi-line yml block commenting — sed cannot handle block structure.**

When commenting out `dbt_constraints` references, the full block including `arguments:` children must be commented — not just the parent line. Leaving orphaned child keys causes YAML parse errors.

Also comment out orphaned `tests:` keys where all children are commented out.

---

## 6. dbt Run Progression

| Run | Pass | Error | Skip | Key Fix |
|-----|------|-------|------|---------|
| v1 | 13 | 15 | 19 | First compile, packages installed |
| v2 | 21 | 16 | 10 | TPC-H source → `samples.tpch` |
| v3 | 21 | 16 | 10 | Sequence dispatch pattern applied |
| v4 | 21 | 16 | 10 | `get_stream` dispatch, `::varchar` regex (broken) |
| v5 | 21 | 16 | 10 | `::varchar` regex fixed (partial) |
| v6 | 23 | 16 | 10 | `dim_calendar_day` rewritten, Q3 TABLESAMPLE fixed |
| v7 | 27 | 15 | 9 | `get_scd_sql` casting fixed, `sysdate()` fixed |
| v8 | 26 | 12 | 9 | Regression — `SYSDATE()` uppercase missed |
| v9 | 30 | 10 | 7 | `clean_nations` fixed, `data_type` yml fixes, tags fixed |
| v10 | 31 | 9 | 7 | Q2 TABLESAMPLE alias order fixed |

### Final State — v10
- **PASS: 31 / 52 (60%)**
- **Adjusted pass rate** (excluding POC skips + hard stops): **31/40 = 77%**

### Remaining 9 Errors — Final Classification
| Model | Error | Category |
|-------|-------|---------|
| `async_bulk_operations` | All-purpose cluster needed | ⏭️ Skip POC |
| `customer_clustering` | All-purpose cluster needed | ⏭️ Skip POC |
| `customer_cdc_stream` | Snowflake Stream | 🔴 Hard stop |
| `dbt_query_history` | Snowflake system table | ⏭️ Skip POC |
| `lkp_exchange_rates` | Cybersyn data missing | ⏭️ Skip POC |
| `lkp_exchange_rates_legacy` | Cybersyn data missing | ⏭️ Skip POC |
| `int_fx_rates__daily` | Cybersyn data missing | ⏭️ Skip POC |
| `order_facts_dynamic` | Streaming table STREAM keyword | 🔴 Architectural |
| `dim_customer_changes` | `metadata$action`, `iff()` | 🔴 Hard stop |
| `dim_current_year_orders` | Streaming table STREAM keyword | 🔴 Architectural |

---

## 7. Gotchas & Lessons Learned

| # | Gotcha | Lesson |
|---|--------|--------|
| 1 | `(\w+)::varchar` regex breaks on `table.col::varchar` | Always use `([\w.]+)` for column references |
| 2 | Comments inside `{{ config() }}` blocks cause parse errors | Remove Snowflake configs entirely, never comment |
| 3 | Array fix regex corrupted dbt config `tags` lists | Scope SQL array fix to query context only |
| 4 | `sed` is case-sensitive — missed `SYSDATE()` uppercase | Always fix both upper and lowercase variants |
| 5 | Commenting `dbt_constraints` left orphaned `arguments:` blocks | Use Python scripts for multi-line yml block removal |
| 6 | Duplicate macro names crash dbt even with dispatch intent | Always overwrite original file, never create alongside |
| 7 | `data_type: timestamp_ntz` in yml → `Undefined` tag | Replace all Snowflake-specific data types in yml |
| 8 | `TABLESAMPLE` alias must come after clause | `table TABLESAMPLE (n ROWS) AS alias` |
| 9 | `streaming_table` requires STREAM keyword in FROM | Dynamic table → streaming table is architectural |
| 10 | Fixing one pattern can break another | Always run full `dbt compile` after each fix batch |

---

## 8. Lakebridge Findings

- Jinja fully preserved through transpilation ✅
- `adapter.dispatch()` pattern works correctly on Databricks ✅
- `databricks__` prefix preferred over `spark__` for clarity
- Inline `--` comment bug in multi-statement files → pre-process one statement per file
- Known post-processor patterns: `VARCHAR(16777216)` → `STRING`, epoch timestamps, `LATERAL FLATTEN`
