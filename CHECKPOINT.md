# Project Checkpoint

Running, git-tracked log of architecture decisions, status, and every real bug
found while building the 8-agent Snowflake→Databricks dbt migration pipeline.
This is the traceable counterpart to the Claude session memory checkpoint
(which lives outside this repo, at
`~/.claude/projects/.../memory/dbt_migration_agent_pipeline.md`, and is not
visible in `git log` or to anyone without that Claude Code installation).
Update this file at the same time as that memory, after every major step —
see the "Commit and checkpoint" standing instruction.

Building an 8-agent pipeline (Preflight, Macro Resolver, Data Loader,
Analyzer, Transpiler, Executor, Diagnostician, Validator) per
`AGENT_DESIGN.md`. Agents run natively inside Databricks Free Edition,
orchestrated eventually as Databricks Workflow tasks via a bundle
(`databricks.yml`), with a Streamlit chat UI on Databricks Apps. Storage is
Unity Catalog Delta tables in catalog `dbt_migration` (schemas:
landing/bronze/silver/gold/audit). Test project is `snowflake-dbt-demo/`. CLI
profile `free_community`, workspace `dbc-d531ded2-aae8.cloud.databricks.com`,
SQL Warehouse id `b05480be6edc2be5`.

Key docs to consult per agent: `AGENT_DESIGN.md` (responsibilities),
`FINDINGS.md` (migration patterns), `MACRO_ANALYSIS.md` (macro handling),
`OPEN_ITEMS.md` (open dependencies), `AUDIT_SCHEMA.md` (Delta table DDL).

## Architecture decision (2026-09-11)

Every agent operates on an isolated copy under
`dbt-migration-agent/migration-workspace/<project_name>/` (via
`agents/common/workspace.py`'s `ensure_workspace_copy()`), never on the
user-provided project path directly. The copy is created once (idempotent —
existing copy is reused so fixes accumulate across the pipeline) and excludes
`.git`/`target`/`dbt_packages`/`logs`. `--reset-workspace` on any agent's CLI
discards and re-copies. Preflight's git-branch check reads from the original
source path (which still has `.git`) via `self.source_path`; every other file
mutation happens on `self.project_path` (the workspace copy).
`output_databricks/` (used by the Transpiler Agent) holds the final
post-processed model output before it's merged into the workspace copy.

## Agent status

- **Agent 1 — Preflight** (`agents/preflight.py`): 6 checks, auto-fixes
  `dbt_project.yml`, creates the audit tables.
- **Agent 2 — Macro Resolver** (`agents/macro_resolver.py`): classifies every
  macro (auto_resolve/flag/hard_stop) via a bespoke registry for
  known-validated macros (`get_stream`, `integration_key`, sequence macros,
  masking policies, `snapshot_hash_arguments`) plus a generic heuristic
  classifier + dispatch-scaffold/inline-fix generator for unrecognized macros
  (generalizes to other client projects). Fixes yml `data_type` fields,
  comments out live `dbt_constraints.*` test blocks (block-aware), downgrades
  `Snowflake-Labs/dbt_constraints` in packages.yml.
- **Agent 3 — Data Loader** (`agents/data_loader.py`): classifies every
  source table into native_redirect / unused / copied(-or-unavailable).
  Detects Snowflake's built-in `SNOWFLAKE_SAMPLE_DATA.TPCH*` and redirects
  `_sources.yml` to Databricks' `samples.tpch`. The live-Snowflake-copy path
  is implemented but **untested** — no real Snowflake account in this
  sandbox.
- **Agent 4 — Analyzer** (`agents/analyzer.py`): runs `dbt parse` and reads
  `manifest.json` for the DAG and raw SQL, scans for a 19-pattern
  Snowflake-construct list, cross-references called macros against Macro
  Resolver's own classifier, classifies Easy/Medium/Complex.
- **Agent 5 — Transpiler** (`agents/transpiler.py`): Lakebridge (`v0.15.1`,
  Morpheus `v0.10.0`) runs once over the whole `models/` tree per invocation.
  Post-processor covers `config()` kwarg cleanup, `dynamic_table` ->
  `materialized_view`, `VARCHAR(n)` -> `STRING`, `ALTER SESSION`/`USE
  WAREHOUSE` removal, TABLESAMPLE alias repositioning, bare `SAMPLE(n)` ->
  `TABLESAMPLE (n PERCENT)`, trailing-semicolon strip, plus 4 corruption
  detectors (see "Lakebridge findings" below).
- **Agent 6 — Executor** (`agents/executor.py`): runs `dbt run
  --no-fail-fast --threads N` against the workspace copy, parses
  `run_results.json`+`manifest.json` for per-model pass/fail/blocked, writes
  to `model_runs`, generates an Excel report via `scripts/dbt_report.py` to
  `dbt-migration-agent/reports/<run_id>.xlsx`.
- **Agent 7 — Diagnostician** (`agents/diagnostician.py`): classifies each
  failed model's error into 14 categories via regex, with
  `auto_fixable`/`llm_eligible` flags. Deterministic fixes reuse Transpiler's
  `post_process()`. LLM fallback uses Model Serving
  (`databricks-gpt-oss-120b`) for category 14 (unknown) and any category
  where the deterministic pass finds nothing.
- **Agent 8 — Validator** (`agents/validator.py`): validates every model that
  passed in the last Executor run — schema subset check, business rules via
  the model's own dbt tests, row count + checksum (Snowflake comparison when
  configured, Databricks-only sanity check otherwise). Migration score
  (40/30/20/10 weights) normalized over only the checks that actually ran.
- **`cli.py`**: the slash-command router chaining all 8 agents.
  `preflight`/`macros`/`load`/`analyze`/`transpile`/`execute`/`diagnose`/`validate`
  delegate to each agent's own `main()`. `run` is the full pipeline
  (hard-stop routing per AGENT_DESIGN.md Section 6). `status` reads the
  latest `pipeline_runs` row plus a deduplicated human-review queue.
  `apply-fix <project_path> <model_name>` (added 2026-09-14) is the explicit
  trigger for a Diagnostician recommendation — see "Advisory-then-apply
  workflow" below.
- No `databricks.yml` bundle yet — deliberately deferred until a stable
  checkpoint (build order: local first, bundle once stable).

## Lakebridge findings (empirically confirmed)

- Already handles correctly: `::type` casts, `iff()`->`IF()`,
  `decode()`->`CASE WHEN`, array literals, `sysdate()`->`CURRENT_TIMESTAMP()`,
  `extract('year',x)`, and all Jinja (config blocks, macro calls, ref/source,
  control flow).
- Jinja used as an inline VALUE inside an unusual SQL clause position can
  come out corrupted (a `PIVOT ... FOR x IN (...)` case left a broken
  placeholder token `!#Jinja0005#!` with zero reported errors, and silently
  dropped a second Jinja block). Confirmed narrow via a second, more
  Jinja-heavy model that transpiled perfectly. Transpiler hard-stops any file
  matching `!#Jinja\d+#!`, keeps the original raw SQL, flags for review.
- TABLESAMPLE alias position: Databricks requires the alias AFTER
  `TABLESAMPLE` — `AS alias TABLESAMPLE(...)` is a parse error. Lakebridge
  sometimes emits the invalid form; fixed by `post_process()`.
- Lakebridge's own CLI exits non-zero whenever *any* file in a batch has a
  parsing/analysis error — not a crash, per-file success is determined by
  output existence, not process exit code.
- The CLI needs `--output-folder`'s *parent* directory to already exist.

### Three distinct Lakebridge silent-corruption modes (found via live Executor testing)

1. **Trailing semicolon breaks ephemeral models.** Lakebridge always appends
   a `;` — harmless for a top-level statement, but a hard syntax error once
   dbt inlines an ephemeral model's SQL as a parenthesized CTE in every
   downstream consumer. Cascaded into 5+ failures. Fix: `post_process()`
   always strips a trailing `;`.
2. **Injected `-- internal error` comment** written directly into
   otherwise-successful-looking output when Lakebridge hits an internal
   transpilation error it can't fully recover from. Corrupted
   `dim_calendar_day.sql` (which needed zero changes) and 4 other files. Fix:
   `INTERNAL_ERROR_RE` detects the marker and hard-stops that file.
3. **Silently dropped CTE definitions with no error marker at all** — when a
   CTE's body is pure Jinja control flow with no literal SQL immediately
   after the opening paren, Lakebridge can drop the *definition* while
   keeping every *reference*, producing `TABLE_OR_VIEW_NOT_FOUND` at run
   time. Broke `DIM__CUSTOMERS`/`DIM__ORDERS`. Fix: `find_dropped_ctes()`
   verifies every such CTE is still defined in Lakebridge's output.

All three: hard-stop that file, keep original raw SQL, flag for human
review. Transpiler has 4 corruption detectors total (including the Jinja
placeholder case).

## Real bugs found and fixed, chronologically

**Transpiler idempotency (2026-09-11):** Transpiler merges its output back
into the workspace copy so `dbt compile` can see it — but a second run was
re-feeding its own prior output back into Lakebridge as raw Snowflake SQL,
causing corruption. Fixed by always transpiling from the pristine original,
never the workspace copy.

**Cross-session auth (2026-09-11):** `WorkspaceClient(profile=...)` can fail
with an ambiguous-host error even with an explicit profile — a
databricks-sdk quirk when another profile shares the same host. Fixed
centrally in `agents/common/db.py`'s `get_client()` by setting
`DATABRICKS_CONFIG_PROFILE` before constructing the client.
`DBT_DATABRICKS_TOKEN` also doesn't persist across shells — regenerate via
`databricks auth token --profile free_community` each session.

**Analyzer comment-stripping (2026-09-11):** the 19-pattern scanner matched
raw SQL text without stripping comments first, so a model whose header
comment *described* an already-fixed construct got flagged as still needing
it. Fixed via `strip_sql_comments()`.

**`pattern_library` DDL (2026-09-11):** `times_applied INT DEFAULT 0` fails
on this workspace (`WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED`).
Dropped the `DEFAULT 0`; writers supply it explicitly.

**Macro Resolver — nested-paren parser blind spot (2026-09-11):**
`get_stream` and both sequence macros never appeared in `macro_resolution`
on any run — their default-argument values use nested parens for grouping,
and the old regex couldn't handle nesting, so it silently skipped these
macros entirely. Fixed with a hand-rolled balanced-paren scanner
(`_find_matching_paren`/`_next_macro_start`).

**Macro Resolver — `requires_human_review` false flip (2026-09-11):**
re-verifying an already-resolved flag/hard_stop macro reclassified from the
dispatcher stub's own (trivially clean) body, always yielding
`auto_resolve`. Fixed by reclassifying from the `KNOWN_MACROS` registry
(ground truth) or the `databricks__<name>` implementation's body instead.

**`dynamic_table` should map to `materialized_view`, not `streaming_table`
(2026-09-12):** Databricks Streaming Tables reject aggregation and
self-referencing correlated subqueries — both common in real Snowflake
`dynamic_table` usage. Materialized View is the correct general-purpose
match (same declarative auto-refresh idea, batch re-computation instead of
incremental stream processing). Fixed in `transpiler.py`'s `post_process()`
and as a `diagnostician.py` run-time safety net (`fix_streaming_table_error`,
category 13, `auto_fixable=True`). `order_facts_dynamic` fully auto-fixed
(7,499,048 rows); `dim_current_year_orders` needed the further fix below.

**`blocked_by_upstream` always `False` (2026-09-12):** checked dbt's skip
`message` field for "upstream", which dbt never actually populates. Fixed to
derive from the real dependency graph (any direct dependency with a
non-success status) — naturally handles multi-level cascades too.

**Systemic `data_type: number -> bigint` bug in Macro Resolver (2026-09-14):**
`dim_current_year_orders` still failed after the materialized_view fix
above, with `DELTA_MERGE_INCOMPATIBLE_DATATYPE`. Root cause (after an
initial wrong hypothesis about `ORDER BY`, corrected via `dbt --debug` on
the real generated DDL): `materialized_view` emits explicit column-type DDL
from yml docs, and `total_price`'s doc said `bigint` when the real column is
`DECIMAL(18,2)` — traced to Macro Resolver's own `DATA_TYPE_MAP` collapsing
Snowflake's ambiguous bare `NUMBER` to `bigint`, silently losing precision
for currency-like columns. Same wrong `bigint` found on `account_balance`,
`extended_price`, `discount`, `tax`, `exchange_rate` (not exhaustively fixed
— not yet causing a build failure). Fixed: `DATA_TYPE_MAP` now maps `number`
-> `decimal(38,10)` (safe superset). Corrected the 3 confirmed `total_price`
yml entries in both the workspace copy and the original source project
(this one warranted fixing the source too, since it's a tool bug that would
silently reintroduce itself on `--reset-workspace`). Documented in
`FINDINGS.md` Section 4.3. `dim_current_year_orders` now builds successfully
(1,141,412 rows).

Refreshed the audit trail properly (`cli.py execute` then `cli.py
diagnose`): **32 pass / 8 fail / 5 blocked** (up from 29/9/7). A new failure
surfaced once `dim_current_year_open_orders` unblocked —
`executive_dashboard`, `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES` (column
names with spaces/`%`) — and Diagnostician's LLM fallback auto-fixed it
correctly on the first retry, the first genuine LLM auto-fix success in this
project.

Cybersyn-missing-data cascade failures (`int_fx_rates__daily`,
`lookup_exchange_rates`, and downstream `fct_order_lines`/`fact_order_line_*`)
confirmed with user (2026-09-12) as expected/acceptable — this sandbox has
no Snowflake Marketplace access.

## Advisory-then-apply workflow for hard-stop categories with a real fix (2026-09-14)

User asked that hard-stop error categories with an actual tested solution be
surfaced as *advice* first, with applying the fix a separate, explicit
user-triggered action (a UI button eventually; `cli.py apply-fix <model>`
today) — "This gives the user more control and awareness." Scope confirmed
via two questions:

1. Only categories with a genuine code-level tested fix get this treatment —
   `python_cluster_error`/`missing_source_data` have no code fix at all
   (compute config / absent data) so they're unchanged, just flagged.
2. `streaming_table_error` (category 13, the `materialized_view` swap) stays
   on **auto-apply** — already validated as a safe one-line mechanical fix,
   not architectural. The new advisory gate is for genuinely architectural
   categories like Streams, not this one.

Design question resolved without re-asking: Macro Resolver (Agent 2,
compile-time) keeps auto-generating its usual dispatch-scaffold-with-stub
for every macro including `get_stream` — that's necessary just to make the
project *compile*, not a functional fix, so it's out of scope. The advisory
gate lives entirely in Diagnostician (Agent 7, run-time): a
`RECOMMENDED_FIXES` registry in `agents/diagnostician.py`, checked in
`diagnose_one()` for any category with `auto_fixable=False,
llm_eligible=False`. If an entry exists, the recommendation text goes into
`attempted_fix` and a `pattern_library` row is logged with
`source='recommended', times_applied=0` — visible as advice, never conflated
with an applied fix — but the model file is untouched.
`DiagnosticianAgent.apply_recommended_fix(model_name)` is the explicit-apply
path: re-derives the model's error category, calls the registry entry's
`apply()` against the workspace copy, re-runs the model, logs
`source='recommended_applied_by_user'`. Wired to `cli.py apply-fix
<project_path> <model_name>`.

### The first RECOMMENDED_FIXES entry: `stream_error` — Snowflake Streams -> Delta CDF

`apply_stream_cdf_pattern()` rewrites `databricks__get_stream` (previously a
hard-stop stub returning `table` unchanged) to read `table_changes()` off
the source table instead, with **zero SQL changes required in any consuming
model** — every consumer already does `stream_alias.*`, so the macro emits
the real base-table columns (via `adapter.get_columns_in_relation`) plus
three synthetic ones:

- `` `metadata$action` `` — CDF's `_change_type` mapped
  `insert`/`update_postimage` -> `INSERT`, `delete`/`update_preimage` ->
  `DELETE` (reproduces a standard Snowflake stream's pre/post-image pairing)
- `` `metadata$isupdate` ``
- `source_commit_version` — CDF's `_commit_version`, passed straight through
  as the watermark

Watermark tracking needs no external control table: `source_commit_version`
is a normal passthrough column that lands in the consumer's own table
automatically, and the next incremental run reads it back via
`MAX(source_commit_version) FROM {{ this }}`.

Two real bugs found and fixed via live testing:

1. `table_changes()`'s starting-version argument must be a literal constant,
   not a subquery (`DELTA_CDC_NON_CONSTANT_ARGUMENT`) — fixed by resolving
   the watermark via `run_query()` to a literal before interpolating it.
2. CDF can't retroactively see history from before it was enabled, so
   `table_changes(t, 0)` fails on a table that already had rows — the
   non-incremental (first) branch snapshots the table directly as all-INSERT
   rows (mirrors Snowflake's `SHOW_INITIAL_ROWS=TRUE`) and seeds the
   watermark from the table's current version via `DESCRIBE HISTORY ... LIMIT
   1` (`get_current_delta_version()` helper macro).

CDF is enabled idempotently via an unconditional `ALTER TABLE ... SET
TBLPROPERTIES` on every macro call — no producer model needs manual opt-in.

Also fixed `customer_cdc_stream.sql`, which wasn't going through
`get_stream()` at all — it hand-rolled its own Snowflake `create/drop
stream` DDL directly in `pre_hook`/`post_hook` (a hard `PARSE_SYNTAX_ERROR`,
no macro involved). Those hooks are stripped and
`tblproperties={'delta.enableChangeDataFeed': 'true'}` added to its config
instead.

Validated end-to-end against the real warehouse: initial load (750,000
rows, watermark seeded correctly), a real incremental batch (one live
`UPDATE` + one `DELETE` on `dim_customers` -> exactly one
`INSERT`-with-`isupdate=true` row and one `DELETE` row appeared in
`dim_customer_changes`, `update_preimage` correctly excluded, watermark
advanced correctly), and a subsequent no-op rerun (zero rows, no error).
Test mutations cleaned up afterward via `--full-refresh` on both models.
Documented in `MACRO_ANALYSIS.md` Section 4.5. Only applied to the workspace
copy (by design, via the advisory mechanism) — the original
`snowflake-dbt-demo/` source is deliberately untouched, since this is an
optional, user-triggered redesign, not a tool bug that would silently
reintroduce itself on a workspace reset.

### Two more real bugs found while refreshing the audit trail (2026-09-14)

The first full-project `cli.py execute` after the CDF fix showed
`dim_customer_changes` passing but `customer_cdc_stream` failing with a new,
unrelated error — genuinely new because all manual CDF testing above used
`--full-refresh`, which always takes the non-incremental branch, so this was
the first time `customer_cdc_stream`'s incremental branch (`{% if
is_incremental() %} SAMPLE(10) {% endif %}`) ever actually ran.

1. **Snowflake's bare `SAMPLE(n)`** (no ROW/TABLESAMPLE keyword,
   percent-based by default) has no Databricks equivalent without the
   `TABLESAMPLE` keyword — confirmed against the real warehouse
   (`PARSE_SYNTAX_ERROR`). Neither Lakebridge nor the existing
   TABLESAMPLE-alias post-processor touches this shape. Fixed:
   `transpiler.py`'s `post_process()` gained `BARE_SAMPLE_RE` ->
   `TABLESAMPLE (n PERCENT)`; `diagnostician.py`'s category 6
   (`sampling_error`) pattern widened to `\bSAMPLE\s*\(` too (word boundary
   correctly excludes "SAMPLE" inside "TABLESAMPLE").
2. **Category 4 (`stream_error`) classifier was too broad.** Its old
   `metadata\$\w+` alternative matched any bare mention of a `metadata$`
   column anywhere in an error's SQL dump, not just a genuine unresolved
   reference. `customer_cdc_stream.sql` legitimately has its own
   `METADATA$ACTION` business column, so the unrelated `SAMPLE(10)` failure
   above got misclassified as `stream_error` — which would have surfaced the
   wrong (CDF) recommendation for a completely unrelated bug. Fixed by
   requiring `"cannot be resolved"` to co-occur with `metadata\$\w+` (note:
   the `[UNRESOLVED_COLUMN...]` bracket itself is always stripped before
   classification runs, so the literal text "UNRESOLVED_COLUMN" can't be
   matched on directly). Verified both directions: the real
   `customer_cdc_stream` bug now classifies as `sampling_error`, and
   `dim_customer_changes`'s genuine stream failure still classifies as
   `stream_error`.

Both fixes are in agent code, so they're proactive for any future project,
not just this one.

**Final confirmed state after the full `cli.py execute` + `cli.py diagnose`
refresh: 35 pass / 5 fail / 5 blocked** (up from 32/8/5 at this session's
start, 29/9/7 originally). `customer_cdc_stream` and `dim_customer_changes`
both pass; zero `stream_error` failures remain. The 5 remaining failures are
all pre-existing, already-accepted architectural/missing-data cases with no
code-level fix, correctly routed straight to human review with no
recommendation surfaced: `async_bulk_operations`/`customer_clustering`
(python_cluster_error x2), `int_fx_rates__daily`/`lookup_exchange_rates`/
`dbt_query_history` (missing_source_data x3 — Cybersyn Marketplace data + a
Snowflake-only system table, none available in this sandbox). Blocked=5 is
the Cybersyn cascade through `fct_order_lines` and its 3 downstream
consumers.

## `data_type: bigint` blast-radius investigation (2026-09-14)

Before bulk-fixing the ~93 remaining `data_type: bigint` occurrences flagged
as a follow-up above, checked whether they're actually causing malformed
values (precision/scale corruption) or are inert documentation, per explicit
user request: "if it is causing malformed values... fixed everywhere. If the
underlying data isn't affected at all, then just notify."

**Confirmed the mechanism precisely:** no `contract: enforced` exists
anywhere in this project; `dbt-databricks` only emits explicit column-type
DDL sourced from yml docs for `materialized_view`/`dynamic_table`
materializations (everything else infers its schema from the query and
ignores the yml doc); Validator's schema check only verifies documented
column *names* are present, never types, and its checksum/row-count checks
run against the real physical table's real values. **Result: ~90 of the ~93
occurrences are confirmed inert** — no functional impact today. Only two
models in this project use `materialized_view`/`dynamic_table` at all:
`dim_current_year_orders` (already fixed) and `order_facts_dynamic`.

**`order_facts_dynamic` was a second, dormant landmine.** Its entire yml
`columns:` block is commented out (collateral damage from the
`dbt_constraints` over-commenting bug), so its `total_order_value: bigint`
mislabel was inert — but would immediately reproduce the same class of
failure the moment that block is reactivated. Verified by temporarily
reactivating it in the workspace copy only and running `dbt run
--full-refresh`, then fixing forward through what surfaced — **two
independent failure modes, not just one**, both confirmed live:
1. `order_date` (computed via `DATE_TRUNC('DAY', o_orderdate)`) was
   documented as `date`, but Databricks/Spark SQL's `DATE_TRUNC()` **always
   returns `TIMESTAMP`**, never `DATE` (confirmed via `typeof()`) — a second,
   independent Snowflake→Databricks dialect difference, unrelated to the
   bigint bug.
2. `total_order_value` (computed via `SUM(o_totalprice)` on a
   `decimal(18,2)` source) needed `decimal(28,2)`, not `decimal(18,2)` —
   Spark SQL's `SUM()` aggregate **widens decimal precision by +10** (capped
   at 38), confirmed via `typeof()`. Matching the source column's own
   precision isn't sufficient once it passes through an aggregate.

Fixed both (workspace copy and the still-commented original source), then
reverted the block back to its original commented state (reactivating it
wasn't requested — that's the separate, already-known `dbt_constraints`
over-commenting issue). Full write-up with error messages in `FINDINGS.md`
Section 4.3.

**For the remaining ~90 inert occurrences** (genuinely decimal-but-mislabeled
columns confirmed via the real TPC-H source schema — `account_balance`,
`extended_price`, `discount`, `tax`, `exchange_rate`,
`avg_discount_rate`/`total_extended_price` and more) — per the user's own
rule, these are **noted, not bulk-fixed**, since the underlying data isn't
affected. Documented as a watch-list in `FINDINGS.md` Section 4.3: relevant
again only if any of these models is ever converted to
`materialized_view`/`dynamic_table`, or if model contracts are adopted.

`OPEN_ITEMS.md` reconciled against actual current state (it predates the
8-agent build and had several items marked open that are now resolved —
`get_stream`→CDF, `streaming_table`→`materialized_view`) — see its Resolution
Log for the full list.

Re-ran the full pipeline (`cli.py execute` then `cli.py validate`)
afterward: **35 pass / 5 fail / 5 blocked** (stable, matching the prior CDF
fix's numbers — the `bigint` investigation didn't touch any live model), and
Validator's average score rose to **97.9%** across all 35 passing models
(up from 97.1%). `dim_customer_changes` — the CDF model — scored 100%:
schema check clean, 3,000,000 rows, checksum computed. `order_facts_dynamic`
correctly shows `schema: None — no yml column documentation found` (its
`columns:` block is still, deliberately, commented out — Validator has
nothing to check against, exactly as expected). The only business-rule
failures are the same two already-known, already-accepted referential-
integrity gaps (`dim_customers`/`dim_orders` at 80%, from the Cybersyn
cascade) — no new data-quality issues surfaced.

## dbt snapshots wired into Transpiler and Executor (2026-09-14)

Found while confirming there were no other loose ends before moving to the
explicitly-deferred bundle/UI work: `AGENT_DESIGN.md` never mentions
"snapshot" anywhere — the whole 8-agent pipeline was built entirely around
`models/`. This project has 2 real Type-2 SCD snapshots
(`snapshots/30_presentation/DIM_CUSTOMERS_SCD.sql`,
`DIM_CUSTOMERS_FROM_STREAM.sql`) that had never been touched by any agent —
Transpiler only walked `models/`, Executor only ran `dbt run` (a separate
command from `dbt snapshot`). Investigated live (user's explicit call, not
deferred): running `dbt snapshot` directly against the raw, untranspiled
files showed `DIM_CUSTOMERS_SCD.sql` already works as-is, but
`DIM_CUSTOMERS_FROM_STREAM.sql` failed — double-quoted identifiers
(`"METADATA$ACTION"`, Snowflake-valid, invalid on Databricks by default) and
`iff()`. Pointed the existing `run_lakebridge()` at `snapshots/` directly
(reusing infra, no new transpiler needed) — Lakebridge fixed both issues
cleanly (backticks, `IFF()`), verified live: both snapshots build correctly
with real data (750,000 rows each, matching the TPC-H customer count).

**Wired in as a permanent, first-class part of the pipeline** (user's
explicit choice over documenting-only): `transpiler.py`'s per-file loop was
extracted into `_transpile_directory()`, now called once for `models/` and
again for `snapshots/` if it exists — identical Lakebridge + post_process()
+ all 4 corruption detectors, reused as-is. One real wrinkle: a snapshot's
dbt-visible name is declared inside the file (`{% snapshot NAME %}`), not
implied by the filename the way a model's is (confirmed via manifest.json:
`DIM_CUSTOMERS_FROM_STREAM.sql` declares snapshot name
`DIM_CUSTOMERS_STREAM_SCD`) — added `ModelTranspileResult.audit_name` so
Transpiler's own audit rows match what Executor's manifest-derived rows call
the same node; without this they'd silently disagree.
`executor.py` gained a `dbt snapshot` step (only if `snapshots/` exists and
has `.sql` files) after `dbt run`, reusing the exact snapshot/restore pattern
already established for `target/run_results.json` in Diagnostician/
Validator's own targeted `--select` calls — `dbt run` and `dbt snapshot` are
separate commands that each overwrite the whole file, so without
backup/restore, Executor's own final on-disk state would leave
Diagnostician/Validator reading snapshot-only results afterward. Refactored
`parse_run_results()` into `read_raw_results()` (just reads, filtered by
node-type prefix) + `build_results()` (derives `blocked_by_upstream` from a
combined models+snapshots status map — this project's `DIM_CUSTOMERS_STREAM_SCD`
snapshot actually depends on a model, `customer_cdc_stream`, so a
per-node-type-siloed status lookup would have missed that cross-type
dependency). The Excel report stays model-only by design (`scripts/
dbt_report.py` is a user-provided script, reused as-is, never scoped to
snapshots) — generated from `dbt run`'s results before the snapshot step
runs, so its behavior is completely unchanged from before.
**Explicitly not in scope**: Diagnostician's `read_failed_models()` still
only looks at `model.`-prefixed manifest nodes, so a failing snapshot isn't
picked up for auto-fix — it lands in the human review queue same as before,
just without a retry attempt. Noted, not silently expanded further.

**A second real bug found while testing this wiring, unrelated to
snapshots**: `agents/common/workspace.py`'s `ensure_workspace_copy()` never
validated `source_path` actually exists once the cached workspace copy was
already present — so a wrong/stale relative `project_path` (exactly what
happened this session: `cli.py <cmd> snowflake-dbt-demo` from inside
`dbt-migration-agent/`, when the sample project is actually a *sibling*
directory at `../snowflake-dbt-demo`) silently kept reusing the stale
cached copy instead of failing. This went completely undetected for the
entire session because every OTHER agent only ever touches the cached
workspace copy — only Transpiler reads `source_path` directly (by design,
for its own idempotency), and today was the first time Transpiler was
re-invoked standalone since the path mistake was made. Fixed: `not
source.exists()` now raises a clear `FileNotFoundError` immediately, so any
agent surfaces the same error a first-time caller would instead of silently
operating on stale state.
**Consequence discovered from this fix actually working correctly**:
re-running Transpiler for real (correct path, first time all session)
regenerated every model file from the *true* pristine source — which
revealed the workspace copy's `customer_cdc_stream.sql` CDF fix (applied
only to the workspace copy, by design, via the advisory mechanism) got
overwritten back to its broken pre_hook/post_hook state, since Transpiler
always regenerates from `source_path`, never the workspace copy (its own
established idempotency design). Re-applied `apply_stream_cdf_pattern()`
(idempotent, cheap) to restore it. Also revealed that the *original source
project* (`snowflake-dbt-demo/`, separately git-tracked, predates the agent
system) still has `materialized = 'streaming_table'` hand-baked into both
`dim_current_year_orders.sql` and `order_facts_dynamic.sql` — a leftover
from an early, WRONG manual migration attempt (per the old, now-corrected
AGENT_DESIGN.md/MACRO_ANALYSIS.md guidance) — not `dynamic_table`, the real
Snowflake value `post_process()`'s regex looks for, so Transpiler's
proactive fix never fires for these two files on a fresh regenerate.
Confirmed this self-heals correctly through the *existing*, already-
validated runtime safety net instead (Diagnostician's category 13
`fix_streaming_table_error`, `auto_fixable=True`): `cli.py diagnose` caught
and correctly fixed `order_facts_dynamic` in one retry.
`dim_current_year_orders`'s own retry got the identical correct file fix
written (`materialized='materialized_view'`, confirmed by reading the file
directly) but its live verification run hit the Free Edition serverless
compute quota (`RESOURCE_EXHAUSTED` — likely materialized-view pipeline
compute specifically, saturated from today's unusually heavy volume of live
testing, not the SQL Warehouse itself, which showed idle) three times in a
row before exhausting retries — a transient environmental issue, not a code
bug; needs wall-clock time to recover, not a fix.

**Confirmed resolved the next day (2026-09-15):** retried `dim_current_year_orders`
directly — passed cleanly (1,141,412 rows, matching the exact count from
its original fix earlier this session), confirming the quota exhaustion was
purely transient, as expected. Refreshed the full pipeline afterward
(`cli.py execute` then `cli.py diagnose`): **37 pass (35 models + the 2
snapshots) / 5 fail / 5 blocked**. `executive_dashboard` hit a fresh
instance of the invalid-column-names issue found earlier this session
(`DELTA_MERGE_UNRESOLVED_EXPRESSION` this time, not
`DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES` — same underlying cause, a
different symptom depending on exactly where in the MERGE it's hit) —
expected, since Transpiler always regenerates this file from pristine
source, which still has the raw invalid column names; self-healed correctly
via the same Diagnostician LLM-fallback path validated earlier (category 14,
fixed in 2 retries). The 5 remaining failures are the same already-accepted
architectural/missing-data cases as always (2 python_cluster_error, 3
missing_source_data). Snapshot wiring is now fully confirmed end-to-end with
a clean full-pipeline run, not just the standalone tests from the day
before.

## Deferred bundle/UI work — architecture pivot to external distribution (2026-09-15)

Picked back up the deferred `databricks.yml`/Streamlit work. Investigated
whether Lakebridge could run inside a Databricks Job task (needed if the
pipeline were deployed as an on-platform Databricks Workflow):
`databricks.labs.lakebridge.cli.transpile()` is a real, directly-callable
Python function (takes a `WorkspaceClient` — same pattern this codebase
already uses), so the `databricks` CLI subprocess dependency isn't a hard
requirement. But the actual transpiler engine lives in a separate,
**~1.25GB** local install (`~/.databricks/labs/lakebridge/` — a 1.1GB venv
plus a 153MB source checkout) that `databricks labs install lakebridge`
provisions, and that install flow is built around interactive prompts, not
a clean headless/automatable path. Not a hard wall, but a genuine
platform-provisioning problem with no clean answer yet (classic compute
with a custom image, a Unity Catalog Volume holding a pre-provisioned
install, or keeping Transpiler as a step outside the automated job).

**Stepped back and reconsidered the goal, rather than solving that problem
head-on — a real, deliberate architecture decision, not a stopgap.** The
current architecture already works effortlessly specifically *because*
nothing runs on Databricks compute: agents run locally, Databricks is used
purely as a backend (SQL Warehouse for dbt + audit writes, Unity Catalog
for storage, Model Serving for Diagnostician's LLM fallback). The
Lakebridge-provisioning problem only exists in the scenario where the
orchestration layer moves *onto* Databricks Job compute — a self-inflicted
problem, not one inherent to the tool. Decided: **keep agents external,
Databricks as backend-only** — drop the Databricks Workflow/Job deployment
target entirely (not deferred, dropped) rather than solve a problem created
by a goal that's no longer the goal.

This reframes what "solidify it" means: not deploying *onto* Databricks,
but making the *external* setup genuinely shareable — right now, using
this tool requires an identically-set-up Linux machine (Lakebridge's
~1.25GB install included) plus this repo, which is not a real distribution
story. Confirmed Streamlit itself has zero platform dependency either way —
it's a plain pip package (`streamlit run app.py` self-hosts anywhere, no
tie to Databricks Apps), so it belongs in the same external environment as
the agents, not built as a Databricks App.

**Plan, in order:** (1) Docker + docker-compose — bakes the ~1.25GB
Lakebridge install once at image-build time, so a new user needs Docker +
their own credentials, not a replicated dev machine; solves the
provisioning problem by moving it out of the shared-with-every-user path
entirely. (2) Fix the credential story — `DBT_DATABRICKS_TOKEN` has been
real friction all project (doesn't persist, manual regeneration every
session); needs OAuth-based auth or auto-refresh, not a raw PAT. (3)
Rewrite `SETUP.md` for the actual agent-based workflow (it's stale — still
describes manual `sed`-based project fixups Preflight/Macro Resolver now
automate, plus a stale open-items checklist). (4) Clean up tracked clutter
(`run_errors_v2.txt` through `v10.txt`, `compile_errors_raw.txt`,
`deps_output.txt`, `dbt_run_report.xlsx` — leftover pre-agent manual-
exploration debug files, still git-tracked from the initial commit). (5)
Streamlit UI, thin wrapper over the same agent classes `cli.py` already
wraps, shipped in the same container.

**Step (started) — remove hardcoded workspace defaults.** Every agent's
`__init__` and CLI argparse defaulted `catalog`/`warehouse_id`/`profile` to
THIS project's own workspace values (`dbt_migration`, `b05480be6edc2be5`,
`free_community`) independently in all 8 agent files plus `cli.py` — ~48
occurrences, a real blocker for "run with your own credentials." New
`agents/common/config.py` centralizes these as env-var-driven defaults,
using standard Databricks SDK/CLI env var names where they already exist
(`DATABRICKS_CONFIG_PROFILE`, `DATABRICKS_WAREHOUSE_ID`) — a workspace
already configured for the `databricks` CLI needs zero extra setup.
`profile=None` now means "let the SDK's own default resolution decide"
rather than forcing our profile name. `warehouse_id` has no generic
fallback (inherently workspace-specific) — `required_warehouse_id()` fails
loudly at the single `execute_sql()` choke point instead of failing
obscurely deep in an HTTP call. `catalog` keeps `"dbt_migration"` as a
generic default *name* (not a workspace identity). `transpiler.py`'s
`run_lakebridge()` updated to omit `--profile` entirely when unset, instead
of passing a literal `"None"` string to the subprocess. Verified end-to-end
via env vars alone (no CLI flags): `cli.py status` correctly resolves all
three and queries real data; unset `DATABRICKS_WAREHOUSE_ID` fails
immediately with a clear, actionable error. Docker/compose (step 1) and the
rest of the plan are next, not yet started.

**Streamlit UI built and tested locally before Docker** (reordered from the
original plan — same "local first, bundle once stable" precedent already
established for every agent, no reason the UI should be the exception;
caught by the user, not planned that way originally). `app.py` is a thin
wrapper over the exact same functions `cli.py` already wraps
(`run_full_pipeline`/`run_agent_command`/`run_apply_fix`/`fetch_status`) —
no new orchestration logic, just a live-log-streaming layer (stdout
redirected into an `st.empty()` placeholder, updated incrementally on every
write, since a full pipeline run can take 15-20 minutes) over what already
works. Four tabs: full pipeline, individual agents (same partial/resume use
case the CLI already supports via its own individual commands), apply-fix,
status. Small supporting refactor in `cli.py`: extracted `fetch_status()`
from `run_status()` so the CLI and the UI share the exact same audit-table
queries instead of `app.py` re-deriving its own SQL.
Verified for real, not just that it renders — `streamlit.testing.v1.AppTest`
(Streamlit's own headless test harness; a plain `curl` only proves the
server's JS shell loaded, not that the script executed without error, since
Streamlit renders over websocket): zero exceptions on load, correct
tab/widget rendering, sidebar correctly picks up the env-var-driven config
defaults from the previous step, the Status tab's live DB query works
end-to-end against real data, and a full UI-triggered Preflight run against
the real sample project correctly live-streamed its log and reflected the
actual result (NO-GO, from a real pre-existing `DBT_DATABRICKS_TOKEN`
issue in that shell — not an app.py bug, a live demonstration of exactly
the credential-story item already on the plan). `streamlit>=1.38.0` added
to `requirements.txt`.

## Dockerfile built and verified end-to-end (2026-09-15)

Two decisions settled first (user, one at a time): (1) each user builds
their own image with their own credentials — no shared pre-built image
published; (2) plain env vars for runtime auth, not mounted credential
files. In practice the build itself needed neither — see below.

**Real, material discovery that simplified the whole design**: the
`databricks labs install lakebridge` CLI flow I'd assumed was needed (the
~1.25GB interactive install investigated in the earlier bundle/UI
architecture pivot) is far more than what this project actually uses.
Traced the actual `transpile` code path to `MorpheusInstaller` — its
constructor takes no `WorkspaceClient` at all, and its `install()` is just
a public Maven artifact download (`com.databricks.labs:databricks-morph-
plugin`, ~68MB — confirmed via `du`). Verified live with every Databricks
env var explicitly unset: installs cleanly, no credentials needed at all.
`docker/install_morpheus.py` calls this installer directly in Python,
bypassing the full CLI wizard entirely. Also checked Lakebridge's separate
`analyze` command (asked directly) — confirmed unused by this project (our
own Agent 4 is fully custom-built) and, even if it were used, backed by a
plain pip package with no separate install step either.

**Two real pip dependency-resolution failures hit and fixed while building
for real:**
1. Loose `>=` version constraints in `requirements.txt` sent pip's resolver
   into a combinatorial explosion (`ResolutionTooDeep`, 200000 rounds)
   trying to jointly satisfy `databricks-labs-lakebridge`'s many transitive
   dependencies. Fixed by pinning every dependency to the exact versions
   already proven working together in local dev (not just the same
   *project*, the identical installed versions).
2. Even pinned, one genuine conflict remained: `databricks-labs-lakebridge`'s
   own transitive dependency (`databricks-bb-analyzer`) declares
   `jsonschema~=4.0.0`, incompatible with `dbt-core`'s `jsonschema>=4.19.1`
   under one unified resolution (confirmed: `ResolutionImpossible`). Yet
   both already coexist fine in local dev at `jsonschema==4.26.0` — evidently
   reached because local dev installed these across *separate* `pip install`
   commands over time, and pip doesn't retroactively re-validate an earlier
   package's constraint when a later command upgrades a shared dependency.
   Reproduced that deliberately in the Dockerfile: `databricks-labs-
   lakebridge` installed alone in its own `RUN` step first, then everything
   else in a second step — same end state, done on purpose instead of by
   accident.

`docker/profiles.yml.template` is baked into the image with zero secrets —
every value (`DATABRICKS_HOST`, `DATABRICKS_WAREHOUSE_ID`,
`DATABRICKS_TOKEN`, `DBT_MIGRATION_CATALOG`) resolves from the container's
own environment via Jinja `env_var()` at dbt-run time. Unified to one token
var for both dbt and the SDK (`DATABRICKS_TOKEN`), not the two separate
ones (`DBT_DATABRICKS_TOKEN` + SDK's own) local dev has used all project.

**Dropped git-based developer/branch auto-detection entirely** (`cli.py`'s
`get_git_branch`, `preflight.py`'s `check_git`) — prompted by the user
questioning why the container needed `git` at all. Traced it to exactly one
purpose: `AGENT_DESIGN.md`'s original multi-developer-on-one-shared-repo
workflow (Section 9) — auto-tagging audit rows with the project's git
branch so results from different developers didn't mix. That premise no
longer holds now that each developer runs their own instance against their
own project (this session's own architecture pivot) — any git-based merging
of finalized migrated files happens entirely outside this tool, which never
needed an opinion on git workflow to do its actual job. Removing it also
eliminates a real, confirmed mount-ownership friction point: bind-mounting
a host git repo into a root-owned container hits git's own dubious-
ownership safety check (CVE-2022-24765, `fatal: detected dubious ownership
in repository`) on every single run — reproduced and confirmed directly.
`--developer` already had an explicit override with a graceful `"unknown"`
fallback, so nothing about the actual pipeline regresses.

**Real correction found immediately after, while re-verifying**: removing
`git` from the image broke `dbt debug` itself — it has its own unconditional
internal "required dependencies" check for the `git` binary being on PATH,
entirely independent of our code and firing regardless of whether the
project uses any git-sourced package. Confirmed by a real failed build:
the actual Databricks connection succeeded (`Connection test: OK connection
ok`) but `dbt debug` still reported `1 check failed: git`, which flipped
our own *blocking* `check_dbt_debug()` to fail and Preflight to `NO-GO` for
a reason completely unrelated to connectivity. Fix: `git` binary went back
into the image — but purely as a system package for dbt's own internal
check, which never touches the mounted project directory. Since our own
code no longer runs `git -C <mounted-dir> ...` on anything, this does not
reintroduce the ownership problem — confirmed live, `GO/NO-GO: GO` with
`git` present and no ownership error anywhere.

**Verified fully end-to-end against the real workspace, not just that it
builds**: ran Preflight inside the container with a real project directory
bind-mounted and real Databricks credentials passed as env vars only (no
mounted credential files) — all 5 checks pass (workspace connectivity,
Unity Catalog, SQL Warehouse, audit tables, `dbt debug`), `GO/NO-GO: GO`.

## docker-compose.yml — the three remaining mount/volume decisions settled (2026-09-16)

Settled one at a time, as usual:
1. **One compose service** (`migration-agent`, running Streamlit), not a
   separate `cli` service — `app.py` already calls `cli.py`'s functions
   in-process, so pure-CLI usage works fine via `docker compose exec
   migration-agent python3 cli.py ...` against the same container.
2. **Bind mounts, not named volumes**, for `migration-workspace/`,
   `output_databricks/`, `reports/` — real files on the host, directly
   openable (an Excel report you can just double-click), matching how local
   non-Docker usage already works.
3. **Root user, accepting root-owned files** on those bind mounts, rather
   than matching a non-root user to the host UID/GID. Talked through with
   the user first: bind-mounted files a root container process creates are
   genuinely, persistently root-owned on the host disk (not a runtime-only
   illusion — same as running `sudo touch` yourself), which does require
   `sudo` for a non-root host user to later delete/overwrite them. But host
   `sudo` unconditionally overrides that regardless of who created the
   files (a separate, more powerful privilege than anything happening
   inside the container) — so the real cost is an occasional `sudo
   chown`/`sudo rm`, not a functional blocker. Not worth the UID/GID-export
   setup friction (`user: "${UID}:${GID}"`, which bash doesn't export GID
   for by default) for every future user of the image. Confirmed live via
   `stat` after a real compose run: `migration-workspace/project/` (written
   by the container) came out `root:root`, exactly as predicted.

The project's own path is bind-mounted from `PROJECT_PATH` (set in `.env`,
templated in `.env.example`) to a fixed internal path, `/data/project`.
`app.py`'s "Project path" sidebar field now reads its default from a new
`DEFAULT_PROJECT_PATH` env var rather than hardcoding `/data/project`
directly — compose sets it to `/data/project` so the field is pre-filled
automatically inside the container, while local (non-Docker) `streamlit
run app.py` leaves it unset and the field stays blank, exactly as before.
`.env` itself (the filled-in real version, with a real token) is gitignored
— only `.env.example` (the template) is committed.

**Verified fully end-to-end**, not just `docker compose config`: `docker
compose up -d` built and started cleanly, Streamlit answered on `:8501`
(HTTP 200), the project bind mount was correctly populated (`ls
/data/project` inside the container showed the real project's files), and
a real Preflight run via `docker compose exec` against the actual sample
project passed all 5 checks with `GO/NO-GO: GO` — the full local-dev
Preflight result, now reproduced through the finished compose setup.

Hit one real, unrelated environmental snag along the way: the `free_community`
CLI profile's OAuth refresh token had expired between sessions
(`Error: A new access token could not be retrieved because the refresh
token is invalid`) — needed an interactive `databricks auth login
--profile free_community` (browser-based, the user's own action, not
scriptable) before testing could continue. Not a code issue, just a
reminder that this profile's auth needs periodic manual refresh.

This closes out step (1) of the distribution plan (Docker + docker-compose)
from the earlier architecture-pivot entry above. Remaining steps from that
plan, still not started: fixing the credential story more durably (OAuth or
auto-refresh instead of a manually-regenerated token), rewriting `SETUP.md`
for the actual agent-based/Docker workflow, and cleaning up the tracked
pre-agent debug-artifact clutter (`run_errors_v2.txt`-`v10.txt`, etc.).

## Transpiler was completely non-functional inside Docker — found and fixed (2026-09-16/17)

Triggered via the Streamlit UI (user's own action, not mine): the first
genuine full `cli.py run` through the finished docker-compose setup. This
was also the first time Transpiler had ever actually been exercised inside
Docker — every earlier container test was Preflight-only, which never
touches Lakebridge at all.

**Real, serious bug, confirmed empirically**: every single file in the
whole project (41/41 non-Python models) came back `manual_review` with
zero `success` — Lakebridge silently never transpiled anything, the entire
run. Traced to `run_lakebridge()` shelling out to `databricks labs
lakebridge transpile`, which depends on "lakebridge" being registered in
the `databricks` CLI's own separate installed-apps bookkeeping
(`~/.databricks/labs/databrickslabs-repositories.json`) —
`docker/install_morpheus.py` never populates that, since it installs the
transpiler engine artifact directly via Python, deliberately bypassing the
full interactive `databricks labs install lakebridge` flow (see that
step's own entry above for why). Ran the exact subprocess command by hand
inside the container: `Error: unknown flag: --input-source`, falling back
to generic CLI help — confirmed, not inferred.

The path to finding this took a real, worthwhile detour: the odd content
in `customer_cdc_stream.sql` initially looked like it might be the
already-known CDF-fix regression (advisory-only fix wiped by a fresh
Transpiler regenerate) or a Lakebridge version mismatch. Traced it properly
via `output_databricks/` (showed pristine untranspiled content — Transpiler
genuinely never touched it) versus the workspace copy (showed backtick/
`TABLESAMPLE` conversions plus a `target.type != 'databricks'` wrapper
neither Lakebridge nor our own `post_process()` would produce) — that
turned out to be Diagnostician's own LLM fallback, correctly doing its job
on raw untranspiled SQL after 3 exhausted retries, creatively (and
incompletely) trying to fix the stream hooks itself. A real, useful
reminder to verify root cause via artifacts rather than pattern-match
against a prior hypothesis.

**Fixed by switching `run_lakebridge()` to call
`databricks.labs.lakebridge.cli.transpile()` directly in Python** instead
of shelling out — needs no CLI registration at all, since it talks to the
transpiler engine directly. Per the user's own instruction, confirmed via
isolated manual tests inside the running container *before* touching any
code: a single file, then a whole directory, both transpiled correctly.

Two more real crashes surfaced and fixed while getting the direct call
working:
1. `databricks.labs.blueprint`'s logging setup calls `find_project_root()`
   on first import of any lakebridge module — walks up from the importing
   file looking for `pyproject.toml`/`setup.py`, present in the git-clone-
   based `databricks labs install` layout this library normally expects,
   never present for a plain `pip install` (which is what this image uses).
   Fixed with an empty `pyproject.toml` dropped at the lakebridge package's
   own root in the Dockerfile.
2. `error_file_path` and `transpiler_config_path` both default to
   *workspace-stored* config (`ApplicationContext`/`Installation.load()`,
   persisted against the Databricks **user**, not the local machine) —
   since this container reuses the same Databricks user as local dev, it
   picked up local dev's own previously-cached paths (a literal local-
   machine absolute path leaked through and failed validation inside the
   container, where it obviously doesn't exist). Fixed by passing both
   explicitly: `error_file_path` to a real container-local path,
   `transpiler_config_path` resolved programmatically via
   `TranspilerRepository.transpiler_config_path("Morpheus")` rather than
   hardcoded (robust to the installed transpiler's own layout changing).

Also removed the `databricks` CLI binary from the Dockerfile entirely —
confirmed (`grep`) it's no longer used anywhere in this codebase once
`run_lakebridge()` stopped shelling out to it. Smaller image, one less
moving part, one less thing that could silently not work.

**Verified fully end-to-end against the real project, not just that it
compiles**: rebuilt the image, ran `cli.py transpile` through the
container — reproduces the exact **33 success / 8 hard_stop / 2 skipped**
result every local test this whole session has shown, plus a clean `dbt
compile: OK` afterward (needed a fresh token first — the one in `.env` had
simply expired, over an hour old, an unrelated environmental non-issue).

**Not yet done**: re-run the *full* `cli.py run` through the fixed
container (the one that surfaced this bug was against the broken
Transpiler) to get genuinely fresh, trustworthy `pipeline_runs`/`model_runs`
numbers — the 30/10/7 result recorded during this investigation should be
treated as invalid, a symptom of the bug rather than real pipeline health.

## How to apply

Before building the next agent, re-read this file plus the relevant
`AGENT_DESIGN.md` section, reuse `agents/common/db.py` /
`agents/common/audit_schema.py` / `agents/common/workspace.py` rather than
re-implementing SQL execution, audit DDL, or the workspace-copy step, and
remember to regenerate `DBT_DATABRICKS_TOKEN` at the start of a fresh shell
session.

## `.dockerignore` gap — real `.env` credentials were being baked into the image

User asked a direct sync-check question ("is Dockerfile basically the
whole directory, including non-required files?") which prompted actually
verifying `.dockerignore` via `docker compose exec ... ls -la /app/`
rather than assuming it worked. Two real gaps found:

1. **Serious**: `.env` (the real file, live Databricks token included) was
   never excluded — `COPY . .` baked it straight into an image layer.
   docker-compose does NOT mount `.env` into the container; compose only
   reads it on the **host** for variable interpolation. Confirmed via
   `docker compose exec migration-agent cat /app/.env` — the live token
   was sitting in the image, owned `root:root` (not the bind-mount UID),
   recoverable later via `docker history`/`docker save` even after the
   token in the real `.env` is rotated. Directly undermines the "no
   credentials baked into the image" design goal from the docker-compose
   work. Fixed by adding `.env` to `.dockerignore`.
2. **Minor**: `run_errors_raw.txt` (an old pre-agent debug artifact,
   confirmed present in the built image) didn't match the
   `run_errors_v*.txt` glob — no "v"/version number in that filename.
   Added it explicitly.

Verified the fix, not just applied it: rebuilt the image, confirmed
`/app/.env`, `/app/run_errors_raw.txt`, `/app/compile_errors_raw.txt`,
`/app/deps_output.txt`, `/app/run_errors_v2.txt` are all absent from the
new container. Also removed the dangling pre-fix image (`docker history`
confirmed its `COPY . .` layer was the leaking one) so the leaked-token
layer isn't left sitting on disk.

The bind-mounted dirs (`migration-workspace/`, `output_databricks/`,
`reports/`) showing up in `ls -la /app/` are expected and fine — those
come from `docker-compose.yml`'s `volumes:` at runtime (owned by the host
UID), not from the image build; `.dockerignore` correctly keeps them out
of the image itself.

**Not yet done**: the token that leaked was likely already expired
(~1hr TTL, and it had been sitting in the image since an earlier rebuild
this session) but rotate it anyway next time regardless, since it was
also echoed into this session's own tool output. Still open, unrelated:
the tracked debug-clutter files themselves (`run_errors_v2.txt`-`v10.txt`,
`compile_errors_raw.txt`, `deps_output.txt`, `run_errors_raw.txt`,
`dbt_run_report.xlsx`) are excluded from the image now but still tracked
in git at the repo root — deleting them from the repo entirely is a
separate, not-yet-done cleanup.

## How to apply

Whenever adding a new local file that might land at the `dbt-migration-agent/` root (scratch output, a new debug dump, a new credentials file), check `.dockerignore` covers it — don't assume a glob pattern written for one filename generalizes to a similarly-named one. For anything credential-shaped specifically, verify with `docker compose exec ... ls -la /app/` after a rebuild rather than trusting the ignore file's intent.

## `.md` docs excluded from the image; Status tab "Invalid Token" was just an expired token (2026-09-17)

Two quick follow-ups from the same sync-check thread. (1) User asked
whether the `.md` docs really need to ship in the image — confirmed via
`grep` that no agent/`scripts/` code opens/reads a `.md` file at runtime
(only comment references to filenames), so added `*.md` to
`.dockerignore`. (2) User reported the Streamlit Status tab failing with
`403 Forbidden < Invalid Token` from the SQL Warehouse API — not a code
bug, the `.env` token had simply expired again (decoded its JWT `exp`
claim directly: expired over an hour earlier), the same recurring ~1hr-TTL
friction from earlier this session. Regenerated via `databricks auth token
--profile free_community`, rebuilt (to fold in the `.dockerignore`
change), and confirmed the status query now runs cleanly through the
container (correctly reports "no pipeline runs recorded yet" — the audit
table is genuinely empty, not an error).

Still the single biggest recurring friction point this whole project —
next real fix (not yet started) is OAuth/auto-refresh instead of manual
`databricks auth token` regen every ~hour.

## `app.py`/`cli.py` moved into `scripts/` (2026-09-17)

User asked to move both entry-point files into `scripts/` (already home to
`dbt_report.py`) to reduce root clutter, with an explicit "skip it if too
much rework" out. It was moderate, not trivial: both files relied on the
project root being on `sys.path`, which happens automatically today only
because they sit AT the project root (Python auto-adds the executed
script's own directory). `git mv`'d both, then:
- Added `sys.path.insert(0, <project root>)` near the top of both files
  (before their `agents.*` imports) so `from agents.xxx import ...` keeps
  resolving once they're one level deeper.
- **Real bug caught by testing, not by inspection**: `app.py`'s `import cli`
  broke under `streamlit run`/`AppTest` — Streamlit's script runner doesn't
  reliably put the script's own directory on `sys.path` the way a plain
  `python3 scripts/app.py` does, so the bare name `cli` fell through to an
  unrelated `cli` package from a completely different project
  (`learn_airflow/cli/`) that happens to be on this shared dev venv's
  `sys.path` via an old editable-install `.pth` file. Confirmed directly
  (`python3 -c "import cli; print(cli.__file__)"` from this directory
  resolved to the wrong file). Fixed by using `import scripts.cli as cli`
  instead of a bare `import cli` — package-qualified, can't collide.
- Updated the two real runtime references: Dockerfile's `ENTRYPOINT`
  (`scripts/app.py`) and `docker-compose.yml`'s comment
  (`python scripts/cli.py ...`), plus `cli.py`'s own `--help` text which
  self-referenced its old path.

Verified via the same "local first, then Docker" order already established
for this project, not just that it imports cleanly:
1. Local: `python3 scripts/cli.py help` succeeds; a real
   `streamlit.testing.v1.AppTest` run of `scripts/app.py` shows zero
   exceptions and all 4 tabs (this is what caught the `import cli` bug
   above — a plain `python3 scripts/app.py` smoke test would have missed
   it, since that execution path doesn't hit the same `sys.path` gap).
2. Docker: rebuilt, `curl localhost:8501` → 200 via the Dockerfile's real
   `ENTRYPOINT`, and `docker compose exec migration-agent python
   scripts/cli.py help` succeeds inside the actual container.
Removed the dangling pre-move image afterward, same cleanup habit as the
`.env` fix above.

Left conceptual/historical mentions of `cli.py` alone in AGENT_DESIGN.md
(diagram), OPEN_ITEMS.md (decision log), MACRO_ANALYSIS.md (narrative
reference) — these name the router as a concept, not a literal runnable
command, and per `CLAUDE.md` AGENT_DESIGN.md is historical intent already,
not a living doc that needs to track file moves.

## How to apply

Any bare same-directory import in a file that can run under multiple
different launchers (`python3 file.py`, `streamlit run file.py`, a test
harness like `AppTest`) is not guaranteed to resolve the same way across
all of them — don't trust "it imports fine when I run it directly" as
proof; test it through the SAME launcher the file is actually meant to run
under in production (here, that's what caught the wrong-`cli`-package bug,
which a plain `python3 scripts/app.py` invocation wouldn't have surfaced).

## Tracked debug clutter moved into `trash/` (2026-09-17)

The pre-agent manual-exploration artifacts flagged since the architecture-pivot plan (`run_errors_v2.txt`-`v10.txt`, `run_errors_raw.txt`, `compile_errors_raw.txt`, `deps_output.txt`, plus the stale root-level `dbt_run_report.xlsx` predating the Executor's own `reports/<run_id>.xlsx` output) moved into a new `dbt-migration-agent/trash/` folder rather than deleted outright, per user's explicit choice. `dbt_run_report.xlsx` stays gitignored (path updated in the root `.gitignore` to `dbt-migration-agent/trash/dbt_run_report.xlsx`); everything else is a normal tracked `git mv`.

`.dockerignore`'s individual `run_errors_v*.txt` / `run_errors_raw.txt` / `compile_errors_raw.txt` / `deps_output.txt` / `dbt_run_report.xlsx` patterns collapsed into a single `trash/` directory exclude. Verified via rebuild + `docker compose exec migration-agent ls /app/trash` (no such directory) and `ls /app/` (only real source/config left) — no debug clutter reaches the image anymore, by construction rather than by enumerating filenames.

## Credential-refresh friction addressed: host-side refresh script + a real DatabricksAuthError, not OAuth M2M (2026-09-17)

Picked up the "credential story" loose end. First explored OAuth M2M (a
Service Principal with client_id/client_secret — the standard container-
native pattern, zero manual refresh ever). Confirmed this Free Edition
workspace supports it (`databricks service-principals list` works). User
correctly rejected building the tool's core credential story around it
though — **not every end user of this tool will have permission to create
a service principal in their own workspace**, and this tool's whole point
is generalizing to any Snowflake→Databricks migration, not just this dev
setup. Right call — mirrors the same reasoning that kept catalog/warehouse
env-var-driven instead of hardcoded earlier this project.

Landed on two things instead, keeping the PAT/token flow as the universal
baseline that needs zero special permissions:

1. **`docker/refresh_token.sh`** — a HOST-side (not container) helper.
   Runs `databricks auth token --profile <profile>`, writes the fresh
   access token into `.env`'s `DATABRICKS_TOKEN`, and prints a reminder to
   restart the container. Purely a convenience for users on OAuth-login
   profiles (short-lived tokens); a real static PAT never needs this.
   **Real bug found via testing**: first version merged stderr into the
   captured JSON (`2>&1`) — worked on a clean run but failed once with a
   `JSONDecodeError` on empty input, most likely a stray stderr line on a
   cold OAuth refresh (Free Edition has shown transient API hiccups
   before). Fixed by capturing stdout only and letting stderr pass through
   to the terminal naturally, rather than trying to explain the one-off
   and hoping it doesn't recur.

2. **`DatabricksAuthError`** (`agents/common/db.py`) — the actual "don't
   let the user get sidetracked" fix. The Status-tab bug from earlier this
   session showed the real cost: even *I* had to investigate a `403
   Forbidden < Invalid Token` as a possible code bug before recognizing it
   was just an expired token — that's exactly the trap an end user would
   fall into. Added `raise_if_auth_error()`, called at the two places a
   Databricks API call can hit a dead credential (`execute_sql()`'s
   `execute_statement()` call — the shared choke point every agent's SQL
   goes through; and Diagnostician's `serving_endpoints.query()` LLM
   fallback call). Detects the failure via `isinstance` against the SDK's
   own `Unauthenticated`/`PermissionDenied` types AND a message-text
   fallback, and re-raises a `DatabricksAuthError` with the exact fix
   command, instead of letting the SDK's own cryptic wrapper surface as-is.
   **Deliberately NOT a `StatementError` subclass** — `validator.py`,
   `data_loader.py`, and `preflight.py` all `except StatementError` to
   soft-fail one check and keep going; if a dead credential were wrapped
   as a `StatementError` too, those handlers would silently absorb "your
   whole credential is dead" as if it were just one failed check among
   many, which is worse than a loud crash.
   **Real bug found via testing, not assumed correct**: first version only
   matched on message-text substrings like `"403"`/`"401"`/`"invalid
   token"`. Tested against a deliberately garbage token and got back an
   `Unauthenticated`-typed exception whose message body
   ("Credential was not sent or was of an unsupported type...") contained
   NONE of those substrings — the class name isn't in `str(exc)`. Fixed by
   checking `isinstance(exc, (Unauthenticated, PermissionDenied))` first,
   keeping the string-match list as a fallback (needed separately — a
   real *expired* JWT reproduced the original "unable to parse response...
   likely a bug in the SDK" wording, which the SDK apparently doesn't map
   to either typed exception). Verified all three cases: garbage token
   (`Unauthenticated`, caught), a genuinely-expired real JWT (message-text
   fallback, caught — this reproduced by accident when an earlier "fresh"
   token from this same session expired again mid-testing), and a valid
   token (no false positive, 3 consecutive successful queries).
   Verified inside the actual container too, not just locally.

**Deferred (user's explicit call, revisit if it becomes a real problem)**:
per-file skip/resume in Transpiler for very large projects (2K+ models) —
today it always reinvokes Lakebridge over the *entire* `models/` tree on
every call (confirmed via code: `run_lakebridge()` takes a whole
`input_dir`, no per-file "already done, skip" check against
`output_databricks/`). Not unsafe (fully idempotent, a redo just produces
byte-identical output) but wasteful at scale if a huge batch fails near
the end. If this becomes real, the fix to explore is running Transpiler in
chunks rather than one giant invocation — check whether Lakebridge's own
API supports a file subset/batch boundary before building custom
skip-logic on our side.

## How to apply

When adding any detection heuristic for a third-party SDK's error shape
(auth failures, rate limits, whatever), test against the SDK's actual
exception instances, not just plausible-looking message text — this
session hit two different exception shapes for what looked like "the same
kind of failure" (a malformed token vs. a genuinely expired one), and a
heuristic tuned against only one of them silently misses the other.

## `SETUP.md` rewritten for the actual Docker/agent workflow (2026-09-17)

Closed the last open item from the original distribution plan. The old
`SETUP.md` documented the pre-agent manual exploration process — `git
clone` the sample project, hand-run `sed` commands to patch
`dbt_project.yml`, a "Known Issues" table of things to fix by hand, and a
`databricks labs install lakebridge` step that's the abandoned ~1.25GB CLI
flow. All of that is now either automated (Preflight/Macro Resolver) or
simply wrong (the Lakebridge install path, the local-only credential var
names, hardcoded references to this dev session's own workspace).

Per user's request, kept the old version as a local-only reference rather
than deleting it: `git mv SETUP.md SETUP.md.bak`, then `git rm --cached`
to untrack it and added `dbt-migration-agent/SETUP.md.bak` to
`.gitignore`. The content isn't lost — fully recoverable from git history
at this commit — it just won't carry forward into future clones, matching
the user's framing ("we remain aware... new one serves others").

New `SETUP.md` is Docker-first (the actual supported path): prerequisites,
`.env` setup table, `docker compose up -d --build`, then either the
Streamlit UI or `docker compose exec migration-agent python scripts/cli.py
<command> /data/project`. Explicitly documents what Preflight/Macro
Resolver now do automatically instead of a manual-fixup checklist, points
to `docker/refresh_token.sh` for the credential-refresh flow, and keeps a
"Local development" section (only for contributing to the agents
themselves) documenting the real difference from the Docker path — local
dev's `~/.dbt/profiles.yml` uses a separate `DBT_DATABRICKS_TOKEN` var,
unlike the image's unified single `DATABRICKS_TOKEN`.

Verified against the actual current code before writing, not from memory:
confirmed via `agents/preflight.py`'s `check_unity_catalog()` that
Preflight auto-creates missing schemas under the catalog (only the catalog
itself and the SQL Warehouse need to pre-exist — the old doc's "manually
create 5 schemas in the UI" step was already stale); confirmed
`docker/install_morpheus.py` has no location-dependent logic, so the
documented local-dev command works identically to how the Dockerfile
itself invokes it.

This closes out the original distribution plan from the architecture-pivot
entry: Docker + docker-compose, credential-refresh friction, `SETUP.md`,
and tracked-clutter cleanup are all done. Remaining open items are the
smaller ones noted along the way (Transpiler per-file resume for very
large projects, deferred; Podman/Windows portability, unverified but
expected to work).

## First public push — README.md added, repo split out via `git subtree` (2026-09-17)

Added `README.md` (front-door instructions: what the tool does, the 8
agents at a glance, quick start, status/requirements) and pushed this
directory's history to its own GitHub repo:
https://github.com/rajathollowfied/dbt-migration-agent

This local repo is rooted one level up (`dbt_migration/`, also containing
`CLAUDE.md`, gitignored `snowflake-dbt-demo/`, etc.), but the GitHub repo
is named for just this subdirectory — confirmed with the user that the
new repo's root should BE `dbt-migration-agent/`'s content directly, not
have it nested a level down. Used `git subtree split --prefix=dbt-migration-agent
-b dbt-migration-agent-export` to extract a branch with history rewritten
so this subdirectory becomes the root (only commits touching this path,
full history preserved for those), verified the resulting tree before
pushing (no `.env`, no `SETUP.md.bak`, no parent-repo files leaked in,
`trash/*.txt` present as expected per the earlier explicit decision to
keep those tracked-but-image-excluded), then pushed to
`git@github.com:rajathollowfied/dbt-migration-agent.git` (SSH — HTTPS
push failed with no credential helper configured; SSH auth was already
set up for this GitHub account).

Confirmed with the user beforehand: full history stays public (workspace
host/profile/warehouse-ID mentions throughout `CHECKPOINT.md` are real
identifying details but not secrets — no token is ever committed, `.env`
is gitignored and confirmed absent from every push).

The local `dbt-migration-agent-repo` remote and `dbt-migration-agent-export`
branch stay in this monorepo for future updates — re-run the same
`subtree split` + `push` sequence to sync new commits (the split is fast,
~27 commits took well under a second).

No LICENSE file exists yet — worth adding before treating this as a real
public release, not something to default silently on.
