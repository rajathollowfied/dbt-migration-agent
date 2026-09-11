# DBT Migration Agent — Agent Design Guide

This document is a developer guide for building the dbt Snowflake → Databricks migration
agent system. It covers architecture, agent responsibilities, implementation patterns,
and how agents communicate with each other.

---

## 1. Overview

The migration system is a multi-agent pipeline where each agent has a single,
well-defined responsibility. Agents are orchestrated by Databricks Workflows and
exposed to developers via a Streamlit chat interface using slash commands.

### Core Principle
> Each agent does one thing. The pipeline does everything.

### General Agentic Pattern (Transferable)
```
Define agent → Give it tools → Trace its decisions → Evaluate output → Improve
```

This pattern applies to any future agentic project regardless of domain.

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────┐
│                 CHAT UI (Streamlit)                  │
│        /dbt-migrate:run  or  :analyze  etc.         │
└──────────────────┬──────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────┐
│                   cli.py router                      │
│         maps command → agent or full pipeline        │
└──────────────────┬──────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────┐
│              Databricks Workflows Job                │
│         sequences, retries, task values             │
└──────────────────┬──────────────────────────────────┘
                   ↓
        ┌──────────────────────┐
        │   8 Agent Pipeline   │
        └──────────────────────┘
```

---

## 3. Agent Pipeline Sequence

```
1. Preflight Agent          → connectivity, catalog, warehouse, token, project config
2. Macro Resolver Agent     → macro inventory, dispatch pattern, package assessment
3. Data Loader Agent        → source tables → Unity Catalog landing/bronze
4. Analyzer Agent           → classify models, build DAG, warehouse mapping
5. Transpiler Agent         → Lakebridge + post-processor, dbt compile
6. Executor Agent           → dbt run --no-fail-fast, parallel threads
7. Diagnostician Agent      → error classification, fixes, LLM fallback, retry loop
8. Validator Agent          → schema, row counts, checksums, business rules
```

---

## 4. Agent Responsibilities

### Agent 1 — Preflight Agent
**Purpose:** Validate everything before any data moves. Fail fast.

**Checks:**
- Databricks workspace connectivity
- Unity Catalog access (`dbt_migration` catalog + all schemas)
- SQL Warehouse reachability (HTTP path valid)
- dbt profile valid (`dbt debug`)
- Git repo + branch check
- `dbt_project.yml` scan — auto-fix Snowflake-specific configs:
  - `profile:` rename
  - `+snowflake_warehouse` — remove entirely (not comment)
  - `target.warehouse / target.database` references — remove entirely
  - `transient=false` in config blocks — remove entirely
- Token validity

**Output:** go/no-go signal → passed as task value to next agent

**Failure behavior:** Hard stop — nothing downstream runs

**Key principle:** A preflight that catches issues early saves hours of debugging downstream.

---

### Agent 2 — Macro Resolver Agent
**Purpose:** Make all macros dialect-agnostic before any model touches them.

**Steps:**
1. Read `project_name` from `dbt_project.yml` → `name` field (for dispatch namespace)
2. Inventory all macros + packages (`packages.yml`)
3. Assess package compatibility (see `MACRO_ANALYSIS.md` Section 3)
4. Replace incompatible packages with native equivalents (e.g. `dbt_constraints` → native contracts)
5. Classify each macro: auto-resolve / flag / hard stop
6. Apply dispatch pattern — **always overwrite original file, never create new file alongside**
7. Generate stubs for flagged macros, add to human review queue
8. Hard stop on architectural blockers with clear message
9. Fix yml `data_type` fields: `number`→`bigint`, `varchar`→`string`, `timestamp_ntz`→`timestamp`
10. Comment out `dbt_constraints` references using Python scripts (not sed)
11. Run `dbt deps` + `dbt compile` to verify macro resolution

**Output:**
- Modified macro files with dispatch pattern applied
- `migration.audit.macro_resolution` table updated
- Human review queue populated for flagged/hard-stop macros

**Failure behavior:** Soft fail — flags problem macros, continues with resolvable ones

**Reference:** `MACRO_ANALYSIS.md` for full classification guide and dispatch pattern template

---

### Agent 3 — Data Loader Agent
**Purpose:** Copy source data from Snowflake to Databricks Unity Catalog.

**Steps:**
1. Read source definitions from `models/*/_sources.yml`
2. Check if Databricks native datasets exist (e.g. `samples.tpch` for TPC-H data)
3. If native datasets available — redirect `_sources.yml` to point at them
4. Otherwise connect to Snowflake source and copy tables to `dbt_migration.landing`
5. Handle type casting: `VARIANT` → `STRING`
6. Batch inserts (configurable batch size)
7. Create Unity Catalog schemas as needed
8. Verify row counts post-load

**Output:** Source tables in `dbt_migration.landing`, row count verification log

**Failure behavior:** Soft fail per table — log failures, continue with available sources

---

### Agent 4 — Analyzer Agent
**Purpose:** Understand the full scope of migration before touching any SQL.

**Steps:**
1. Scan all models for 19 Snowflake-specific patterns
2. Classify each model: Easy / Medium / Complex
3. Build dependency DAG
4. Map warehouse sizes:
   - Primary: check existing Snowflake warehouse size for that model
   - Fallback: use complexity classification
5. Write classification results to audit table

**Classification criteria:**
| Complexity | Indicators |
|------------|------------|
| Easy | Simple SELECT, no Snowflake-specific functions, no macros |
| Medium | 1-3 Snowflake patterns, standard macros, incremental |
| Complex | Streams, sequences, dynamic tables, custom macros, heavy Jinja |

**Warehouse size mapping:**
| Complexity | Warehouse Size |
|------------|---------------|
| Easy | 2XS / XS |
| Medium | S / M |
| Complex | L / XL |

**Output:** Classification table, DAG structure, warehouse map → passed as task values

---

### Agent 5 — Transpiler Agent
**Purpose:** Convert Snowflake SQL to Databricks SQL without touching business logic.

**Steps:**
1. Read raw `.sql` files — macros unresolved (Jinja preserved)
2. Pre-process: ensure one statement per file (Lakebridge inline comment bug workaround)
3. Run Lakebridge Morpheus transpiler (source dialect: `snowflake`)
4. Run post-processor for patterns Lakebridge misses (see below)
5. Write transpiled output to `output_databricks/`
6. Run `dbt compile` — bulk error detection across all models
7. Pass error list to Diagnostician Agent

**Post-processor steps (validated from manual runs):**

*Type casting:*
- `([\w.]+)::varchar(\(\d+\))?` → `CAST(\1 AS STRING)` — use qualified name regex
- `([\w.]+)::integer` → `CAST(\1 AS INTEGER)`
- `([\w.]+)::timestamp_ntz` → `CAST(\1 AS TIMESTAMP)`
- `([\w.]+)::number(\(\d+,\d+\))?` → `CAST(\1 AS DECIMAL)`
- `([\w.]+)::date` → `CAST(\1 AS DATE)`

*Functions (fix both cases):*
- `sysdate()` / `SYSDATE()` → `current_timestamp()`
- `iff(cond, t, f)` → `CASE WHEN cond THEN t ELSE f END`
- `decode(expr, ...)` → `CASE WHEN` equivalent

*Config block (remove entirely — do not comment):*
- `snowflake_warehouse=...` in config() → remove
- `transient=false` in config() → remove
- `materialized='dynamic_table'` → `materialized='streaming_table'`

*Sampling:*
- `table AS alias SAMPLE ROW (n ROWS)` → `table TABLESAMPLE (n ROWS) AS alias`

*SQL arrays (SQL context only — not config blocks):*
- `['val1', 'val2']` in SQL → `ARRAY('val1', 'val2')`

*Data generation:*
- `TABLE(GENERATOR(rowcount => n))` with `seq4()` → `explode(sequence(1, n))`

*Session commands (remove from pre_hook):*
- `ALTER SESSION SET WEEK_START = n` → remove
- `ALTER SESSION SET WEEK_OF_YEAR_POLICY = n` → remove

**Output:** Transpiled models in `output_databricks/`, error list for Diagnostician

**Failure behavior:** Soft fail — compile errors go to Diagnostician, not pipeline stop

---

### Agent 6 — Executor Agent
**Purpose:** Run dbt models and capture full error surface in one pass.

**Steps:**
1. Select warehouse size from Analyzer output
2. Run `dbt run --no-fail-fast` (process all models, don't stop at first error)
3. 8 parallel threads (configurable)
4. Capture pass/fail/skip per model
5. Write results to `migration.audit.model_runs`
6. Pass failed models to Diagnostician Agent

**Key flag:**
```bash
dbt run --no-fail-fast --threads 8
```

**Note:** `--continue-on-error` does not exist in dbt. Use `--no-fail-fast`.

**Output:** Full run results in audit table, failed model list for Diagnostician

---

### Agent 7 — Diagnostician Agent
**Purpose:** Classify errors and fix what can be fixed automatically.

**Error classification (14 categories):**
1. Type casting errors (`::type` syntax)
2. Missing functions (`sysdate`, `CURRENT_WAREHOUSE`, etc.)
3. Sequence errors (`CREATE SEQUENCE` not supported)
4. Stream errors (`SHOW STREAMS`, `metadata$action`)
5. Array literal errors (`['val']` in SQL)
6. Sampling errors (`SAMPLE ROW` syntax)
7. Materialization errors (`dynamic_table`)
8. Session command errors (`ALTER SESSION`)
9. Data type errors (`NUMBER`, `VARCHAR` without size)
10. Package errors (`dbt_constraints`)
11. Missing source data
12. Python model cluster errors
13. Streaming table errors (STREAM keyword)
14. Unknown / LLM fallback

**Steps:**
1. Receive failed model list from Executor
2. Classify each error into 14 categories
3. Apply deterministic regex fixes for known patterns (from `FINDINGS.md` Section 4)
4. For unknown patterns → LLM fallback (Databricks Model Serving)
5. Auto-save new patterns to `migration.audit.pattern_library`
6. Loop back to Transpiler (up to `max_retries` — global config)
7. Models exceeding max retries → human review queue

**The feedback loop:**
```
Transpiler → Executor → Diagnostician
                ↑              |
                |    retry     |
                └──────────────┘
                        |
                   max_retries exceeded
                        |
                   Human review queue
```

**LLM fallback scope:**
- Unknown SQL syntax variations not in pattern library
- Complex multi-construct rewrites (e.g. full calendar dim rewrite)
- Novel Snowflake functions with near-Databricks equivalents
- Does NOT fix: architectural decisions (streams, missing data, Python cluster config)

**Loop implementation:** Python while loop inside the Diagnostician task.
Simpler than recursive Workflows, sufficient for POC.

**Pattern library:** Shared across all developers from day one.

```sql
migration.audit.pattern_library
├── pattern_id
├── error_category
├── regex_pattern
├── fix_template
├── source              -- deterministic / llm_generated
├── times_applied
└── created_at
```

**Failure behavior:** Never hard stops. Max retries exceeded → human queue, continue pipeline.

---

### Agent 8 — Validator Agent
**Purpose:** Prove the migration produced correct results.

**Checks:**
1. Schema match — column names, data types, nullability
2. Row count match — exact or within configurable threshold
3. Checksum match — hash of ordered result set
4. Business rules — spot-check known calculations

**Comparison:** Snowflake source vs Databricks target

**Migration scoring:**
| Check | Weight |
|-------|--------|
| Schema match | 40% |
| Row count match | 30% |
| Checksum match | 20% |
| Business rules | 10% |

---

## 5. Audit Delta Tables

### model_runs
```sql
migration.audit.model_runs
├── model_name
├── layer                    -- landing/bronze/silver/gold
├── complexity               -- easy/medium/complex
├── warehouse_size
├── transpile_status         -- success/failed/skipped
├── run_status               -- pass/fail/blocked
├── blocked_by_upstream      -- bool
├── error_category           -- one of 14 categories
├── attempted_fix
├── fix_successful           -- bool
├── retry_count
├── final_error_message
├── requires_human_review    -- bool
├── developer                -- git branch
└── run_timestamp
```

---

## 6. Orchestration — Databricks Workflows

Each agent = one Workflow Task. Tasks declare dependencies.

### Job Structure
```yaml
Job: dbt-migration-pipeline
Parameters:
  - developer: "alice"
  - branch: "dev/alice/bronze-finance"
  - max_retries: 3

Tasks:
  - preflight_agent         on_failure: STOP
  - macro_resolver_agent    depends_on: preflight       on_failure: STOP
  - data_loader_agent       depends_on: macro_resolver
  - analyzer_agent          depends_on: data_loader
  - transpiler_agent        depends_on: analyzer        on_failure: → diagnostician
  - executor_agent          depends_on: transpiler      on_failure: → diagnostician
  - diagnostician_agent     depends_on: executor        retry: max_retries
  - validator_agent         depends_on: executor_success
```

### Task Communication
```python
# Agent writes output
dbutils.jobs.taskValues.set(key="complexity_map", value=json.dumps(complexity_map))

# Next agent reads it
complexity = json.loads(
    dbutils.jobs.taskValues.get(taskKey="analyzer_agent", key="complexity_map")
)
```

### Failure Routing
| Agent | On failure |
|-------|------------|
| Preflight, Macro Resolver | Hard stop |
| Transpiler, Executor | Soft fail → Diagnostician |
| Diagnostician | Retry up to max → human queue |
| Validator | Flag discrepancies → report |

---

## 7. Chat Interface — Streamlit App

### Slash Commands
| Command | Triggers |
|---------|---------|
| `/dbt-migrate:run` | Full pipeline (always end-to-end, no shortcuts) |
| `/dbt-migrate:preflight` | Agent 1 only |
| `/dbt-migrate:analyze` | Agent 4 only |
| `/dbt-migrate:convert` | Agent 5 only |
| `/dbt-migrate:diagnose` | Agent 7 only |
| `/dbt-migrate:validate` | Agent 8 only |
| `/dbt-migrate:status` | Current run status from audit table |
| `/dbt-migrate:help` | Lists available commands |

### Command Routing (cli.py) — Open Dependency
`cli.py` structure needs to be confirmed from Snowflake POC repo.
Current design: prefix match routing `/dbt-migrate:command` → agent dispatch.

### Progress Streaming
```
🔵 Preflight Agent... ✓
🔵 Macro Resolver... ✓  (12 auto-resolved, 3 flagged, 1 hard stop)
🔵 Data Loader... ✓  (13 tables loaded)
🔵 Analyzer... ✓  (5500 models classified)
🔵 Transpiler... running
⏳ Executor... waiting
⏳ Diagnostician... waiting
⏳ Validator... waiting
```

---

## 8. MLflow Tracing

```python
import mlflow

with mlflow.start_run(run_name="transpiler_agent"):
    mlflow.log_param("model_count", len(models))
    mlflow.log_param("developer", developer)
    mlflow.log_param("branch", branch)
    mlflow.log_param("max_retries", max_retries)

    # agent executes...

    mlflow.log_metric("success_rate", success_count / total)
    mlflow.log_metric("lakebridge_fix_rate", auto_fixed / total)
    mlflow.log_metric("llm_fallback_rate", llm_fixes / total)
    mlflow.log_artifact("failed_models.json")
```

---

## 9. Developer Git Workflow

```
main
  └── dev
        ├── dev/alice/bronze-finance
        ├── dev/bob/silver-marketing
        └── dev/carol/gold-reporting
```

Each developer's job run is parameterized with their branch name.
Audit table automatically scopes results per developer.

---

## 10. Evaluation — CLEARS Framework (MLflow 3)

| Dimension | Migration Equivalent |
|-----------|---------------------|
| **C**orrectness | Transpiled SQL produces same results as Snowflake source |
| **L**atency | Time per agent, time per model, total pipeline duration |
| **E**xecution | Agent completes without crashing, handles edge cases |
| **A**dherence | Agent follows defined scope — Transpiler only transpiles |
| **R**elevance | Diagnostician fixes relevant to actual error |
| **S**afety | No data loss, no unintended schema changes |

### POC Success Criteria
| Metric | Target | Baseline (sample project) |
|--------|--------|--------------------------|
| Pipeline end-to-end without manual intervention | ✅ | Not yet — agents not built |
| Auto-fix rate (Diagnostician) | > 80% | ~77% manual |
| Validator pass rate | > 90% | TBD |
| Human review queue | < 5% of models | ~23% (hard stops + architectural) |
| Reproducibility | Same result on 3 runs | TBD |

---

## 11. Repo Structure

```
dbt-migration-agent/
├── .databricks/
│   └── databricks.yml
├── agents/
│   ├── preflight.py
│   ├── macro_resolver.py
│   ├── data_loader.py
│   ├── analyzer.py
│   ├── transpiler.py
│   ├── executor.py
│   ├── diagnostician.py
│   └── validator.py
├── frontend/
│   └── app.py                  # Streamlit chat UI
├── plugin-skills/
│   ├── analyze/
│   ├── convert/
│   ├── diagnose/
│   ├── init/
│   ├── load/
│   ├── run/
│   ├── setup/
│   └── test/
├── migration-workspace/
├── output_databricks/
├── scripts/
│   └── warehouse_mapper.py
├── cli.py
├── SETUP.md
├── FINDINGS.md
├── MACRO_ANALYSIS.md
├── AGENT_DESIGN.md
├── AUDIT_SCHEMA.md
└── OPEN_ITEMS.md
```

---

## 12. Build vs Leverage

| Component | Build | Leverage |
|-----------|-------|---------|
| Preflight Agent | ✓ | Databricks SDK |
| Macro Resolver | ✓ | dbt dispatch |
| Data Loader | ✓ | Databricks COPY INTO |
| Analyzer | ✓ | — |
| Transpiler | ✓ | **Lakebridge Morpheus** |
| Executor | ✓ | dbt-databricks adapter |
| Diagnostician | ✓ | Databricks Model Serving |
| Validator | ✓ | — |
| Orchestration | — | **Databricks Workflows** |
| Chat UI | ✓ | **Streamlit on Databricks Apps** |
| LLM interface | — | **Databricks Model Serving** |
| Tracing | — | **MLflow 3** |
| Storage | — | **Delta + Unity Catalog** |
