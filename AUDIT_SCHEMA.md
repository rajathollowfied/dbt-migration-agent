# DBT Migration Agent — Audit Schema

This document defines all Delta tables used by the migration agent system.
All tables live in the `migration.audit` schema within Unity Catalog.

---

## Setup

```sql
-- Create audit schema if not exists
CREATE SCHEMA IF NOT EXISTS dbt_migration.audit;
```

---

## 1. model_runs

Tracks every model's migration status across all agent runs.
Primary table for monitoring overall migration progress.

```sql
CREATE TABLE IF NOT EXISTS dbt_migration.audit.model_runs (
    model_name              STRING NOT NULL,
    layer                   STRING,             -- landing / bronze / silver / gold
    complexity              STRING,             -- easy / medium / complex
    warehouse_size          STRING,             -- 2XS / XS / S / M / L / XL
    warehouse_source        STRING,             -- snowflake_ref / analyzer
    transpile_status        STRING,             -- success / failed / skipped
    run_status              STRING,             -- pass / fail / blocked
    blocked_by_upstream     BOOLEAN,            -- true if parent model failed
    error_category          STRING,             -- diagnostician error class (1-14)
    attempted_fix           STRING,             -- fix applied by diagnostician
    fix_successful          BOOLEAN,
    retry_count             INT,
    final_error_message     STRING,
    requires_human_review   BOOLEAN,
    developer               STRING,             -- git branch / dev identity
    run_id                  STRING,             -- databricks workflow run id
    run_timestamp           TIMESTAMP
)
USING DELTA
COMMENT 'Tracks migration status of every dbt model across all agent runs';
```

### Key Queries

```sql
-- Overall migration health per layer
SELECT
    layer,
    COUNT(*)                                                    AS total_models,
    SUM(CASE WHEN run_status = 'pass' THEN 1 ELSE 0 END)       AS passed,
    SUM(CASE WHEN run_status = 'fail' THEN 1 ELSE 0 END)       AS failed,
    SUM(CASE WHEN blocked_by_upstream THEN 1 ELSE 0 END)       AS blocked,
    SUM(CASE WHEN requires_human_review THEN 1 ELSE 0 END)     AS needs_review,
    ROUND(AVG(retry_count), 2)                                  AS avg_retries
FROM dbt_migration.audit.model_runs
GROUP BY layer
ORDER BY layer;

-- Human review queue
SELECT
    model_name,
    layer,
    complexity,
    error_category,
    final_error_message,
    retry_count,
    developer
FROM dbt_migration.audit.model_runs
WHERE requires_human_review = true
ORDER BY layer, complexity DESC;

-- Developer progress
SELECT
    developer,
    COUNT(*)                                                AS total,
    SUM(CASE WHEN run_status = 'pass' THEN 1 ELSE 0 END)   AS passed,
    SUM(CASE WHEN requires_human_review THEN 1 ELSE 0 END) AS needs_review
FROM dbt_migration.audit.model_runs
GROUP BY developer;
```

---

## 2. macro_resolution

Tracks every macro processed by the Macro Resolver Agent.
Reference for understanding what was changed and why.

```sql
CREATE TABLE IF NOT EXISTS dbt_migration.audit.macro_resolution (
    macro_name              STRING NOT NULL,
    macro_file              STRING,
    category                STRING,     -- auto_resolve / flag / hard_stop
    snowflake_construct     STRING,     -- what Snowflake feature it uses
    databricks_action       STRING,     -- what was done or recommended
    dispatch_applied        BOOLEAN,
    requires_human_review   BOOLEAN,
    review_notes            STRING,     -- context for human reviewer
    processed_at            TIMESTAMP
)
USING DELTA
COMMENT 'Tracks macro classification and resolution decisions by Macro Resolver Agent';
```

### Key Queries

```sql
-- Macro resolution summary
SELECT
    category,
    COUNT(*) AS macro_count
FROM dbt_migration.audit.macro_resolution
GROUP BY category;

-- Macros requiring human review
SELECT
    macro_name,
    macro_file,
    snowflake_construct,
    databricks_action,
    review_notes
FROM dbt_migration.audit.macro_resolution
WHERE requires_human_review = true;
```

---

## 3. pattern_library

Shared library of error patterns and fixes accumulated across all developer runs.
Grows over time — early runs hit more LLM fallbacks, later runs benefit from
accumulated patterns. Shared across all developers from day one.

```sql
CREATE TABLE IF NOT EXISTS dbt_migration.audit.pattern_library (
    pattern_id          STRING NOT NULL,        -- uuid
    error_category      STRING,                 -- one of 14 diagnostician categories
    regex_pattern       STRING,                 -- pattern to match error message
    fix_template        STRING,                 -- fix to apply
    source              STRING,                 -- deterministic / llm_generated
    times_applied       INT     DEFAULT 0,
    last_applied        TIMESTAMP,
    created_at          TIMESTAMP,
    created_by          STRING                  -- developer who triggered the fix
)
USING DELTA
COMMENT 'Shared pattern library for Diagnostician Agent — grows with each run';
```

### Key Queries

```sql
-- Most common error patterns
SELECT
    error_category,
    regex_pattern,
    times_applied,
    source
FROM dbt_migration.audit.pattern_library
ORDER BY times_applied DESC;

-- LLM-generated patterns (review for accuracy)
SELECT *
FROM dbt_migration.audit.pattern_library
WHERE source = 'llm_generated'
ORDER BY created_at DESC;
```

---

## 4. validation_results

Stores Validator Agent output — comparison between Snowflake source and
Databricks target for each model.

```sql
CREATE TABLE IF NOT EXISTS dbt_migration.audit.validation_results (
    model_name              STRING NOT NULL,
    layer                   STRING,
    schema_match            BOOLEAN,
    schema_match_notes      STRING,         -- column-level diff if mismatch
    row_count_source        BIGINT,
    row_count_target        BIGINT,
    row_count_match         BOOLEAN,
    row_count_threshold     FLOAT,          -- configurable match threshold (e.g. 0.001)
    checksum_source         STRING,
    checksum_target         STRING,
    checksum_match          BOOLEAN,
    business_rules_pass     BOOLEAN,
    business_rules_notes    STRING,
    migration_score         FLOAT,          -- weighted score 0-100
    validated_at            TIMESTAMP,
    developer               STRING
)
USING DELTA
COMMENT 'Validator Agent results — Snowflake source vs Databricks target comparison';
```

### Migration Score Calculation

```python
def calculate_migration_score(result: dict) -> float:
    score = 0.0
    if result["schema_match"]:         score += 40.0
    if result["row_count_match"]:      score += 30.0
    if result["checksum_match"]:       score += 20.0
    if result["business_rules_pass"]:  score += 10.0
    return score
```

### Key Queries

```sql
-- Validation summary
SELECT
    layer,
    COUNT(*)                                                        AS total,
    SUM(CASE WHEN schema_match THEN 1 ELSE 0 END)                   AS schema_ok,
    SUM(CASE WHEN row_count_match THEN 1 ELSE 0 END)                AS rowcount_ok,
    SUM(CASE WHEN checksum_match THEN 1 ELSE 0 END)                 AS checksum_ok,
    ROUND(AVG(migration_score), 2)                                  AS avg_score
FROM dbt_migration.audit.validation_results
GROUP BY layer;

-- Models not fully validated
SELECT
    model_name,
    layer,
    schema_match,
    row_count_match,
    checksum_match,
    migration_score,
    schema_match_notes
FROM dbt_migration.audit.validation_results
WHERE migration_score < 100
ORDER BY migration_score ASC;
```

---

## 5. pipeline_runs

Tracks each full pipeline execution — one row per `/dbt-migrate:run` invocation.

```sql
CREATE TABLE IF NOT EXISTS dbt_migration.audit.pipeline_runs (
    run_id              STRING NOT NULL,        -- databricks workflow run id
    developer           STRING,
    branch              STRING,
    command             STRING,                 -- run / analyze / convert etc.
    max_retries         INT,
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    duration_minutes    FLOAT,
    total_models        INT,
    passed              INT,
    failed              INT,
    blocked             INT,
    needs_review        INT,
    pipeline_status     STRING                  -- success / partial / failed
)
USING DELTA
COMMENT 'One row per pipeline invocation — top-level run tracking';
```

---

## 6. source_load

Tracks every source table the Data Loader Agent processed — one row per
(source, table) per run. Not in the original design doc; added when the Data
Loader Agent was built since no audit table existed for its output yet.

```sql
CREATE TABLE IF NOT EXISTS dbt_migration.audit.source_load (
    source_name           STRING NOT NULL,
    table_name             STRING NOT NULL,
    strategy                STRING,        -- native_redirect / copied / unused / unavailable
    target_location         STRING,        -- e.g. samples.tpch.nation or dbt_migration.landing.x__y
    row_count_source         BIGINT,
    row_count_target         BIGINT,
    row_count_match          BOOLEAN,
    status                   STRING,        -- ok / skipped / failed
    requires_human_review    BOOLEAN,
    notes                    STRING,
    loaded_at                TIMESTAMP
)
USING DELTA
COMMENT 'Tracks every source table the Data Loader Agent processed — native redirect, copied, or skipped';
```

---

## 6. Schema Initialization Script

Run once to set up all audit tables in Unity Catalog:

```sql
-- 1. Schema
CREATE SCHEMA IF NOT EXISTS dbt_migration.audit;

-- 2. Tables
CREATE TABLE IF NOT EXISTS dbt_migration.audit.model_runs ( ... );
CREATE TABLE IF NOT EXISTS dbt_migration.audit.macro_resolution ( ... );
CREATE TABLE IF NOT EXISTS dbt_migration.audit.pattern_library ( ... );
CREATE TABLE IF NOT EXISTS dbt_migration.audit.validation_results ( ... );
CREATE TABLE IF NOT EXISTS dbt_migration.audit.pipeline_runs ( ... );
```

Or as a Python script in `scripts/init_audit_tables.py`:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="free_community")

ddl_statements = [
    "CREATE SCHEMA IF NOT EXISTS dbt_migration.audit",
    # ... table DDLs
]

for ddl in ddl_statements:
    w.statement_execution.execute_statement(
        warehouse_id="b05480be6edc2be5",
        statement=ddl,
        catalog="dbt_migration"
    )
    print(f"Executed: {ddl[:60]}...")
```

---

## 7. Notes

- All tables use Delta format — enables time travel, ACID transactions, and CDC
- Tables are shared across all developers — `developer` and `branch` columns scope results
- `pattern_library` is append-only — never delete patterns, use `times_applied` to assess relevance
- `pipeline_runs` is the entry point for lead-level monitoring — drill into `model_runs` for detail
