# Macro Analysis & Migration Guide

This document serves as a reference for the Macro Resolver Agent and migration engineers.
It covers how to classify, handle, and migrate dbt macros from Snowflake to Databricks.

---

## 1. Macro Resolver Agent — Decision Framework

Every macro encountered during migration falls into one of three categories:

```
┌─────────────────────────────────────────────────────────┐
│  MACRO RESOLVER AGENT                                   │
│                                                         │
│  For each macro:                                        │
│  1. Scan for Snowflake-specific constructs              │
│  2. Classify → Auto-resolve / Flag / Hard Stop          │
│  3. Apply dispatch pattern or generate stub             │
│  4. Log outcome to audit table                          │
│  5. Add to human review queue if flagged/hard stop      │
└─────────────────────────────────────────────────────────┘
```

### Classification Criteria

| Category | When to use | Agent action |
|----------|-------------|--------------|
| **Auto-resolve** | Macro wraps a Snowflake feature with a direct Databricks equivalent, or can be safely stubbed as no-op | Apply dispatch pattern automatically |
| **Flag for review** | Macro logic is portable but uses Snowflake-specific syntax that needs testing or validation | Generate dispatch stub, add to human review queue with context |
| **Hard stop** | Macro relies on a Snowflake-native architecture with no direct Databricks equivalent | Stop pipeline for this model, require human architectural decision |

---

## 2. The Dispatch Pattern

The adapter dispatch pattern is the standard mechanism for making macros dialect-agnostic.
It is the correct approach for all macros — do not disable or delete original macro files.

### Template

```sql
{#
  MACRO: macro_name
  Category: Auto-resolve / Flag / Hard Stop
  Databricks: brief description of what databricks__ does
#}

{# Step 1: Dispatcher — routes to correct implementation based on adapter #}
{% macro macro_name(args) %}
    {{ return(adapter.dispatch('macro_name', 'project_name')(args)) }}
{% endmacro %}

{# Step 2: Snowflake implementation — original logic preserved, untouched #}
{% macro default__macro_name(args) %}
    -- original Snowflake code here
{% endmacro %}

{# Step 3: Databricks implementation — equivalent or logged no-op #}
{% macro databricks__macro_name(args) %}
    -- Databricks equivalent
    -- OR:
    {%- do log("macro_name: <reason for no-op>. <post-migration action>", info=true) -%}
{% endmacro %}
```

### Critical Rules
- `project_name` must be read dynamically from `dbt_project.yml` → `name` field, never hardcoded
- Always preserve original logic in `default__` — never delete or modify it
- Always log no-ops so nothing silently disappears from the run output
- **Always overwrite the original macro file** — never create a new file alongside it
- dbt does not allow duplicate macro names across any files in the project — this causes a hard compile failure

---

## 3. Package-Level Migration

Before individual macro analysis, assess all packages in `packages.yml`.

### Package Assessment

| Package | Databricks Support | Action |
|---------|-------------------|--------|
| `dbt-labs/dbt_utils` | ✅ Full support | Keep — works as-is |
| `brooklyn-data/dbt_artifacts` | ✅ Works with deprecation warnings | Keep — minor yml fixes |
| `Snowflake-Labs/dbt_constraints` | ❌ Snowflake/Postgres/Oracle only | Replace with native dbt contracts |
| `dbt-labs/dbt_project_evaluator` | ✅ Adapter agnostic | Keep if used |

### dbt_constraints → Native dbt Contracts

`dbt_constraints` is a Snowflake-specific package. The `dbt-databricks` adapter provides
native constraint support without any third-party package.

**Reference:** https://docs.getdbt.com/reference/global-configs/databricks-changes
**Reference:** https://medium.com/dbsql-sme-engineering/how-to-build-an-end-to-end-testing-pipeline-with-dbt-on-databricks-cb6e179e646c

#### Migration Steps
1. Remove `dbt_constraints` from `packages.yml`
2. Find all `dbt_constraints.*` references in yml files:
   ```bash
   grep -r "dbt_constraints" models/ --include="*.yml" -l
   ```
3. Replace with native dbt `constraints:` blocks (see below)
4. Use Python scripts to comment out full blocks including `arguments:` children — sed cannot handle multi-line yml structure

```yaml
# Before — dbt_constraints (Snowflake)
models:
  - name: my_model
    tests:
      - dbt_constraints.primary_key:
          column_name: id
      - dbt_constraints.foreign_key:
          column_name: customer_id
          pk_table_name: ref('customers')
          pk_column_name: id

# After — native dbt contracts (Databricks)
models:
  - name: my_model
    constraints:
      - type: primary_key
        columns: [id]
      - type: foreign_key
        columns: [customer_id]
        to: ref('customers')
        to_columns: [id]
```

#### Constraint Types Supported Natively on Databricks
- `primary_key`
- `foreign_key`
- `unique`
- `not_null`
- `check`

---

## 4. Common Snowflake Macro Patterns & Databricks Equivalents

### 4.1 Warehouse Sizing Macros
**Pattern:** Macros that dynamically switch Snowflake warehouse based on model or run type.

```sql
-- Snowflake
USE WAREHOUSE {{ warehouse_name }};
CURRENT_WAREHOUSE()
EXECUTE IMMEDIATE $$ ... USE WAREHOUSE ... $$
```

**Databricks equivalent:** No direct equivalent — warehouse is set at the connection level.
Warehouse sizing is handled by the **Executor Agent** based on model complexity classification.

**Agent action:** Auto-resolve → no-op stub with log message.

---

### 4.2 Masking Policies
**Pattern:** Macros that create Snowflake column-level security masking policies.

```sql
-- Snowflake
CREATE MASKING POLICY IF NOT EXISTS policy_name AS (val string) RETURNS string ->
CASE WHEN CURRENT_ROLE() IN ('ROLE_NAME') THEN val
ELSE '**********'
END
```

**Databricks equivalent:** Unity Catalog column masks.
```sql
ALTER TABLE catalog.schema.table
ALTER COLUMN sensitive_col
SET MASK catalog.schema.mask_function;
```

**Agent action:** Auto-resolve → no-op stub for POC. Post-migration: implement via Unity Catalog column masks.
**Validated:** ✅ No-op stub working — logged correctly in run output.

---

### 4.3 Query Tags
**Pattern:** Macros that tag Snowflake queries for monitoring/cost attribution.

```sql
-- Snowflake
ALTER SESSION SET QUERY_TAG = '{{ tag_value }}';
```

**Databricks equivalent:** Cluster tags or query comments.
**Agent action:** Auto-resolve → no-op stub. Unity Catalog audit logs provide equivalent visibility.

---

### 4.4 Sequences & Surrogate Keys
**Pattern:** Macros that use Snowflake sequences to generate surrogate keys.

```sql
-- Snowflake
CREATE SEQUENCE IF NOT EXISTS my_seq;
SELECT my_seq.nextval;
```

**Databricks:** No sequence support in Databricks SQL.

**Replacement options:**
| Option | When to use |
|--------|-------------|
| `row_number() OVER (ORDER BY (SELECT NULL))` | Non-incremental models |
| `row_number() + max(column)` | Incremental models — preserves high watermark |
| `BIGINT GENERATED ALWAYS AS IDENTITY` | Table-level auto-increment |
| `uuid()` | Non-sequential, guaranteed unique |

**For incremental models with high watermark:**
```sql
{% macro databricks__sequence_nextval_as_surrogate_key(column_name, sequence_name=none) %}
    {% if is_incremental() %}
        row_number() OVER (ORDER BY (SELECT NULL))
        + coalesce((SELECT max({{ column_name }}) FROM {{ this }}), 0)
        as {{ column_name }}
    {% else %}
        row_number() OVER (ORDER BY (SELECT NULL)) as {{ column_name }}
    {% endif %}
{% endmacro %}
```

**Agent action:** Flag for review — logic is replaceable but needs validation per model.
**Validated:** ✅ Working in `DIM__CUSTOMERS`, `DIM__ORDERS`, `dim_customers_incremental_macro`, `dim_orders_incremental_macro`.

---

### 4.5 Streams (CDC)
**Pattern:** Macros that create/query Snowflake Streams for change data capture.

```sql
-- Snowflake
CREATE OR REPLACE STREAM my_stream ON TABLE my_table SHOW_INITIAL_ROWS = TRUE;
SELECT * FROM my_stream WHERE METADATA$ACTION = 'INSERT';
```

**Databricks equivalent:** Delta Change Data Feed (CDF)
```sql
-- Databricks
ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
SELECT * FROM table_changes('my_table', startingVersion, endingVersion);
```

**Agent action:** Hard stop — CDF is architecturally different from Streams.
Models using streams need redesign, not just syntax replacement.
**Validated:** ✅ Hard stop message logged correctly — `get_stream` dispatch stub working.

---

### 4.6 Dynamic Tables
**Pattern:** Models materialized as Snowflake Dynamic Tables (auto-refresh).

```sql
-- Snowflake config
materialized: dynamic_table
snowflake_warehouse: target.warehouse
target_lag: '1 hour'
```

**Databricks equivalent: Materialized View, not Streaming Table** (corrected 2026-09-12,
after the original `streaming_table` guidance below caused real failures in this
project). `materialized: streaming_table` looks like the obvious name match, but
Databricks Streaming Tables are built for incremental/append-only *ingestion*
(processing a stream of new rows), not general auto-refreshing transformation —
confirmed directly against the real warehouse that they reject:
- aggregation (`STREAMING_TABLE_QUERY_INVALID`, "add the STREAM keyword" —
  `order_facts_dynamic`'s `GROUP BY` hit this)
- self-referencing correlated subqueries (`dim_current_year_orders`'s
  `WHERE order_date >= (SELECT MAX(order_date) FROM {{ ref(same_model) }})` hit
  this too)

Both of those are common in real Snowflake dynamic_table usage — dynamic tables
are Snowflake's general-purpose "auto-refreshing materialized query" construct,
and **Databricks Materialized Views are the direct equivalent of that concept**:
same idea (declaratively define a query, Databricks keeps it refreshed on a
schedule), but implemented as batch re-computation rather than incremental
stream processing, so none of the structural restrictions above apply. Confirmed
working end-to-end with a live `dbt run` (aggregation query, real data).

```sql
-- Databricks config
materialized: materialized_view
```

**Agent action:**
- Transpiler Agent — auto-replace `dynamic_table` with `materialized_view` at
  transpile time, remove `snowflake_warehouse`/`target_lag` config
  (`agents/transpiler.py`'s `post_process()`).
- Diagnostician Agent — if a model still ends up with `materialized:
  streaming_table` (e.g. transpiled before this fix) and fails with
  `STREAMING_TABLE_QUERY_INVALID` at run time, auto-fix by switching to
  `materialized_view` (`agents/diagnostician.py`'s
  `fix_streaming_table_error()`) — category 13, now auto-fixable rather than
  human-review-only.
- Streaming Table remains correct only for genuinely append-only, high-volume
  ingestion sources — not detectable generically from the SQL alone, so not
  attempted automatically. Flag for human review if that's actually the intent.

---

### 4.7 SCD (Slowly Changing Dimensions)
**Pattern:** Macros that implement SCD merge logic using sequences and Snowflake functions.

Key issues in `get_scd_sql` pattern:
| Snowflake | Databricks |
|-----------|------------|
| `sysdate()` / `SYSDATE()` | `current_timestamp()` |
| `null::integer` | `CAST(null AS INTEGER)` |
| `null::varchar` | `CAST(null AS STRING)` |
| `null::timestamp_ntz` | `CAST(null AS TIMESTAMP)` |
| `sequence.nextval` | `row_number()` dispatch |

**Agent action:** Flag for review — fix dialect issues, test output thoroughly.
SCD logic is business-critical and must be validated by Validator Agent.
**Validated:** ✅ Dialect fixes working. Surrogate key generation validated.

---

### 4.8 Integration Key Pattern
**Pattern:** Macro that builds composite integration keys by concatenating fields.

```sql
-- Snowflake
"COALESCE(" ~ field ~ "::VARCHAR, '')"

-- Databricks dispatch
"COALESCE(CAST(" ~ field ~ " AS STRING), '')"
```

**Agent action:** Auto-resolve — dispatch pattern with `CAST(field AS STRING)` replacement.
**Validated:** ✅ Working in `dim_customers_macro_example`, `dim_orders_macro_example`.

---

### 4.9 AI / ML Functions
**Pattern:** Macros or models using Snowflake Cortex AI functions.

```sql
-- Snowflake Cortex
ai_classify(column, ['LABEL1', 'LABEL2']):labels[0] as result
```

**Databricks equivalent:** Databricks AI Functions
```sql
-- Databricks
ai_classify(column, ARRAY('LABEL1', 'LABEL2')) as result
```

Two differences:
1. Colon accessor `:labels[0]` — remove, Databricks returns value directly
2. Array literal `['val']` → `ARRAY('val')` in SQL context only

**Agent action:** Flag for review — near-equivalent exists but needs syntax adjustment and testing.
**Validated:** ✅ Fix working in `clean_nations`.

---

### 4.10 Snapshot Hash Arguments
**Pattern:** Optimized snapshot hash macro that avoids MD5 for better Snowflake merge performance.

```sql
-- Snowflake
coalesce(cast({{ arg }} as varchar), '') || '|' || ...)::varchar

-- Databricks dispatch
coalesce(cast({{ arg }} as STRING), '') || '|' || ...)
```

Drop the outer `::varchar` cast — not needed in Databricks.
**Agent action:** Flag for review — dispatch pattern with STRING replacement.

---

## 5. Common SQL Dialect Differences (Non-Macro)

Handled by the Transpiler Agent post-processor.

| Snowflake | Databricks | Notes |
|-----------|------------|-------|
| `QUALIFY` | Subquery with `ROW_NUMBER()` | Context-dependent |
| `LATERAL FLATTEN` | `EXPLODE()` or `variant_explode()` | Check inline vs macro |
| `FLATTEN` | `EXPLODE()` | |
| `PIVOT` | Similar syntax, minor differences | |
| `GENERATOR` | `explode(sequence(1, n))` | |
| `VARIANT` type | `STRING` → parse with `from_json()` | |
| `ILIKE` | `LIKE` (case-insensitive default) | |
| `DATEADD(part, n, date)` | Same syntax — Databricks compatible | ✅ No change needed |
| `sysdate()` / `SYSDATE()` | `current_timestamp()` | Fix both cases |
| `null::type` | `CAST(null AS type)` | |
| `field::type` | `CAST(field AS type)` | Use `([\w.]+)::type` regex |
| `VARCHAR(16777216)` | `STRING` | Lakebridge handles |
| `CREATE SEQUENCE` | Not supported | Use `row_number()` or `IDENTITY` |
| `EXECUTE IMMEDIATE` | Supported but different procedural SQL | Review case by case |
| `SHOW STREAMS` | Not applicable | Hard stop |
| `CURRENT_WAREHOUSE()` | Not applicable | No-op |
| `CURRENT_ROLE()` | `current_user()` | Different concept |
| `decode()` | `CASE WHEN` | Direct replacement |
| `table AS alias SAMPLE ROW (n ROWS)` | `table TABLESAMPLE (n ROWS) AS alias` | Alias position changes |
| `['val1', 'val2']` in SQL | `ARRAY('val1', 'val2')` | SQL context only |
| `ALTER SESSION SET WEEK_START` | Not supported — remove | |
| `last_day(date, 'YEAR'/'WEEK')` | Date arithmetic | Manual calculation |

---

## 6. Macro Resolver Agent — Output Contract

For each macro processed, the agent writes to `migration.audit.macro_resolution`:

```sql
migration.audit.macro_resolution
├── macro_name
├── macro_file
├── category              -- auto_resolve / flag / hard_stop
├── snowflake_construct   -- what Snowflake feature it uses
├── databricks_action     -- what was done or recommended
├── dispatch_applied      -- bool
├── requires_human_review -- bool
├── review_notes          -- context for human reviewer
└── processed_at
```

---

## 7. Reference Links

- dbt adapter dispatch: https://docs.getdbt.com/reference/dbt-jinja-functions/adapter#dispatch
- dbt native constraints: https://docs.getdbt.com/reference/global-configs/databricks-changes
- Databricks constraints with dbt: https://medium.com/dbsql-sme-engineering/how-to-build-an-end-to-end-testing-pipeline-with-dbt-on-databricks-cb6e179e646c
- Delta Change Data Feed: https://docs.delta.io/latest/delta-change-data-feed.html
- Databricks AI Functions: https://docs.databricks.com/en/large-language-models/ai-functions.html
- dbt-databricks adapter: https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup
