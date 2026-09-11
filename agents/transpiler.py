"""Agent 5 — Transpiler Agent.

Purpose: convert Snowflake SQL to Databricks SQL without touching business
logic. See AGENT_DESIGN.md Section 4 (Agent 5) for the full spec.

What Lakebridge (Morpheus) already handles correctly on its own — confirmed
empirically against this project, not assumed from the docs:
  - `::type` casts -> CAST(... AS ...)
  - iff(...) -> IF(...)  (a real Spark SQL builtin, not a CASE WHEN rewrite)
  - decode(...) -> CASE WHEN ... END
  - ['a','b'] array literals -> ARRAY('a','b')
  - sysdate()/SYSDATE() -> CURRENT_TIMESTAMP()
  - extract('year', x) -> EXTRACT('year' FROM x)
  - Jinja (config blocks, macro calls, ref()/source(), control flow) — preserved

What it does NOT handle, confirmed by probing it directly (this is the actual
post-processor scope, not the full list AGENT_DESIGN.md speculated):
  - `{{ config(...) }}` kwargs it has no opinion on: snowflake_warehouse=,
    transient=false, materialized='dynamic_table', ALTER SESSION in pre_hook
  - VARCHAR(16777216) (Snowflake's max) instead of STRING
  - TABLESAMPLE: when the source had `table AS alias SAMPLE ROW (n)`, Lakebridge
    emits `table AS alias TABLESAMPLE (n)` — invalid on Databricks (confirmed
    against the real warehouse: alias must come AFTER TABLESAMPLE)

What it can silently break (discovered during testing, not documented anywhere):
  - Jinja used as an inline VALUE inside an unusual SQL clause position (e.g. a
    PIVOT's dynamic `IN (...)` list) can come out as a broken, never-resolved
    placeholder token like `!#Jinja0005#!`, with 0 reported errors. Verified
    this is narrow — ordinary Jinja (config blocks, macro calls, ref/source,
    control flow) survives perfectly even when heavily used in the same file.
  Confirmed with user (2026-09-11): hard-stop just that file rather than ship
  possibly-corrupted SQL — keep the original raw SQL, flag for human review.

Steps:
  1. Read raw .sql files from the migration-workspace copy (macros unresolved)
  2. Flag (not split) multi-statement files — genuine splitting is the
     documented Lakebridge workaround, but no file in this project actually
     has more than one top-level statement to validate that logic against
  3. Run Lakebridge once over the whole models/ tree (source dialect snowflake)
  4. Detect Jinja-corruption placeholders -> hard-stop that file
  5. Otherwise run the post-processor (see above) and write to output_databricks/,
     then merge (overwrite) into the migration-workspace copy
  6. Run `dbt compile` for bulk error detection
  7. Report the error list (Diagnostician Agent isn't built yet — returned/printed)

Failure behavior: soft fail — a broken/uncertain file is flagged and skipped,
the rest of the run continues.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk.errors import DatabricksError

from agents.analyzer import strip_sql_comments
from agents.common.db import StatementError, execute_sql, get_client
from agents.common.workspace import ensure_workspace_copy

OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "output_databricks"

JINJA_CORRUPTION_RE = re.compile(r"!#\s*Jinja\d+\s*#!", re.IGNORECASE)
# Lakebridge sometimes writes a file that "succeeds" (no error reported, output exists)
# but starts with an injected error comment — a silent internal failure that isn't
# caught by exit code or file-existence checks. Found on a file that was already
# fully Databricks-native and needed zero changes.
INTERNAL_ERROR_RE = re.compile(r"--\s*internal error\b", re.IGNORECASE)
# Lakebridge can also silently DROP an entire CTE definition (no error, no
# placeholder token) when the CTE's body is pure Jinja control flow with no literal
# SQL immediately after the opening paren — e.g. `existing_data as ( {% if ... %} )`.
# References to the CTE survive; only the definition vanishes. Detected by checking,
# for every CTE with this shape in the RAW source, whether it's still defined
# (not just referenced) in Lakebridge's output.
CTE_JINJA_BODY_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+as\s*\(\s*\{%-?\s*if\b", re.IGNORECASE)

CONFIG_KWARG_REMOVE_PATTERNS = [
    (re.compile(r"^[ \t]*snowflake_warehouse\s*=.*,?[ \t]*\n?", re.MULTILINE), "snowflake_warehouse config"),
    (re.compile(r"^[ \t]*transient\s*=\s*false\s*,?[ \t]*\n?", re.MULTILINE | re.IGNORECASE), "transient=false config"),
]
MATERIALIZED_DYNAMIC_TABLE_RE = re.compile(r"materialized\s*=\s*(['\"])dynamic_table\1")
VARCHAR_SIZE_RE = re.compile(r"\bVARCHAR\(\d+\)", re.IGNORECASE)
ALTER_SESSION_RE = re.compile(r"ALTER\s+SESSION\s+SET\s+(WEEK_START|WEEK_OF_YEAR_POLICY)\s*=\s*\d+\s*;?", re.IGNORECASE)
USE_WAREHOUSE_RE = re.compile(r"\bUSE\s+WAREHOUSE\s+\S+\s*;?", re.IGNORECASE)
TABLESAMPLE_ALIAS_BEFORE_RE = re.compile(
    r"(\bFROM\s+[\w.]+)\s+AS\s+(\w+)\s+(TABLESAMPLE\s*\([^)]*\))", re.IGNORECASE
)
# Patterns the post-processor deliberately does NOT auto-fix — the real fix (like
# dim_calendar_day's) is a manual SQL rewrite, not a safe mechanical substitution.
MANUAL_REWRITE_PATTERNS = [
    (re.compile(r"\bGENERATOR\s*\(|\bseq4\s*\(", re.IGNORECASE), "GENERATOR()/seq4() needs a manual explode(sequence()) rewrite"),
]


def has_multiple_statements(raw_sql: str) -> bool:
    """Heuristic only (per-file split/rejoin isn't implemented — see module docstring):
    strip Jinja/comments, then count top-level semicolons before the final one.
    """
    text = re.sub(r"\{[%{#].*?[%}#]\}", " ", raw_sql, flags=re.DOTALL)
    text = re.sub(r"--[^\n]*|/\*.*?\*/", " ", text, flags=re.DOTALL)
    text = re.sub(r"'[^']*'|\"[^\"]*\"", "''", text)
    stripped = text.strip().rstrip(";")
    return ";" in stripped


def post_process(sql: str) -> tuple[str, list[str]]:
    fixes = []
    new_sql = sql

    for pattern, desc in CONFIG_KWARG_REMOVE_PATTERNS:
        if pattern.search(new_sql):
            new_sql = pattern.sub("", new_sql)
            fixes.append(f"removed {desc}")

    if MATERIALIZED_DYNAMIC_TABLE_RE.search(new_sql):
        # Not streaming_table: confirmed by testing directly against the real
        # warehouse that Databricks Streaming Tables reject aggregation and
        # self-referencing correlated subqueries (STREAMING_TABLE_QUERY_INVALID —
        # "add the STREAM keyword"), which is exactly what Snowflake dynamic_table
        # models commonly contain (both real failures in this project did:
        # order_facts_dynamic's GROUP BY, dim_current_year_orders' self-join MAX()
        # filter). Materialized View is the correct general-purpose match for
        # Snowflake dynamic_table + target_lag — it's still an auto-refreshing,
        # declaratively-scheduled object, but a materialized view re-runs the full
        # query in batch on each refresh rather than processing an incremental
        # stream, so it has none of streaming_table's structural restrictions.
        # Confirmed working end-to-end with a live `dbt run` test (GROUP BY
        # aggregation). Streaming Table remains correct only for genuinely
        # append-only, high-volume ingestion — not detectable generically here, so
        # not attempted automatically; flag for review if that's actually needed.
        new_sql = MATERIALIZED_DYNAMIC_TABLE_RE.sub(lambda m: f"materialized={m.group(1)}materialized_view{m.group(1)}", new_sql)
        fixes.append("materialized dynamic_table -> materialized_view (not streaming_table — "
                     "see transpiler.py comment for why)")

    if VARCHAR_SIZE_RE.search(new_sql):
        new_sql = VARCHAR_SIZE_RE.sub("STRING", new_sql)
        fixes.append("VARCHAR(n) -> STRING")

    if ALTER_SESSION_RE.search(new_sql):
        new_sql = ALTER_SESSION_RE.sub("", new_sql)
        fixes.append("removed ALTER SESSION SET WEEK_START/WEEK_OF_YEAR_POLICY")

    if USE_WAREHOUSE_RE.search(new_sql):
        new_sql = USE_WAREHOUSE_RE.sub("", new_sql)
        fixes.append("removed USE WAREHOUSE")

    if TABLESAMPLE_ALIAS_BEFORE_RE.search(new_sql):
        new_sql = TABLESAMPLE_ALIAS_BEFORE_RE.sub(
            lambda m: f"{m.group(1)} {m.group(3)} AS {m.group(2)}", new_sql,
        )
        fixes.append("repositioned TABLESAMPLE alias (Databricks requires alias after clause)")

    # Lakebridge always appends a trailing `;`. Harmless for a model that runs as
    # its own top-level statement, but a hard syntax error for an ephemeral model —
    # dbt inlines its full compiled SQL as a parenthesized CTE body in every
    # downstream consumer, and a `;` inside that parenthesized expression breaks
    # the whole query. Confirmed against the real warehouse (5 models failed with
    # this exact error before the fix). Always strip it — dbt never wants it.
    stripped = new_sql.rstrip()
    if stripped.endswith(";"):
        new_sql = stripped[:-1] + "\n"
        fixes.append("removed trailing semicolon (breaks ephemeral-model CTE embedding)")

    return new_sql, fixes


def detect_manual_rewrite_needed(sql: str) -> list[str]:
    # Comments often *describe* a pattern that was already fixed (e.g. dim_calendar_day's
    # header literally says "GENERATOR + seq4() replaced with explode(sequence())") —
    # matching without stripping comments first re-flags already-fixed models.
    code = strip_sql_comments(sql)
    return [desc for pattern, desc in MANUAL_REWRITE_PATTERNS if pattern.search(code)]


def find_dropped_ctes(raw_sql: str, lb_output: str) -> list[str]:
    """CTE names whose body is pure Jinja control flow in the raw source but that
    no longer have a definition (just references) in Lakebridge's output."""
    dropped = []
    for cte_name in set(CTE_JINJA_BODY_RE.findall(raw_sql)):
        still_defined = re.search(rf"\b{re.escape(cte_name)}\s+as\s*\(", lb_output, re.IGNORECASE)
        if not still_defined:
            dropped.append(cte_name)
    return dropped


def run_lakebridge(input_dir: Path, output_dir: Path, profile: str, source_dialect: str = "snowflake") -> str:
    """Runs Lakebridge over the whole tree in one process. A non-zero exit here
    just means *some* files had parsing/analysis errors (Lakebridge's own
    per-file error count) — it still writes output for every file it could
    handle. Per-file success is determined by whether output exists for that
    file, not by this process's exit code.
    """
    env = dict(os.environ)
    env["DATABRICKS_CONFIG_PROFILE"] = profile
    proc = subprocess.run(
        [
            "databricks", "labs", "lakebridge", "transpile",
            "--input-source", str(input_dir),
            "--output-folder", str(output_dir),
            "--source-dialect", source_dialect,
            "--skip-validation", "true",
            "--profile", profile,
        ],
        capture_output=True, text=True, timeout=600, env=env,
    )
    return proc.stdout + proc.stderr


@dataclass
class ModelTranspileResult:
    model_path: str
    status: str  # success / hard_stop / manual_review / skipped
    fixes_applied: list[str]
    requires_human_review: bool
    notes: str


@dataclass
class TranspilerReport:
    run_id: str
    results: list[ModelTranspileResult]
    dbt_compile_ok: bool
    dbt_output_tail: str

    def to_dict(self) -> dict:
        return asdict(self)


class TranspilerAgent:
    def __init__(
        self,
        project_path: str,
        profile: str = "free_community",
        catalog: str = "dbt_migration",
        warehouse_id: str = "b05480be6edc2be5",
        dbt_target: str = "dev",
        developer: str = "unknown",
        source_dialect: str = "snowflake",
        reset_workspace: bool = False,
    ):
        self.source_path = Path(project_path).resolve()
        self.project_path = ensure_workspace_copy(self.source_path, reset=reset_workspace)
        self.profile = profile
        self.catalog = catalog
        self.warehouse_id = warehouse_id
        self.dbt_target = dbt_target
        self.developer = developer
        self.source_dialect = source_dialect
        self.output_dir = OUTPUT_ROOT / self.project_path.name
        self.client = None

    def transpile(self) -> TranspilerReport:
        run_id = str(uuid.uuid4())
        # Always transpile from the untouched ORIGINAL source, never from the workspace
        # copy's models/ — Transpiler itself is the only agent that writes there, so a
        # second run would otherwise re-feed its own prior output back into Lakebridge
        # as if it were raw Snowflake SQL (confirmed: causes real corruption on rerun).
        source_models_dir = self.source_path / "models"
        workspace_models_dir = self.project_path / "models"
        sql_files = sorted(p for p in source_models_dir.rglob("*.sql"))
        py_files = sorted(p for p in source_models_dir.rglob("*.py"))

        results: list[ModelTranspileResult] = []
        for p in py_files:
            results.append(ModelTranspileResult(
                str(p.relative_to(source_models_dir)), "skipped", [], False,
                "Python dbt model — not a SQL transpilation target",
            ))

        lakebridge_out = self.output_dir / "_lakebridge_raw"
        if lakebridge_out.exists():
            shutil.rmtree(lakebridge_out)
        lakebridge_out.parent.mkdir(parents=True, exist_ok=True)  # Lakebridge needs the parent to pre-exist
        lakebridge_log = run_lakebridge(source_models_dir, lakebridge_out, self.profile, self.source_dialect)

        def write_result(rel: Path, content: str) -> None:
            """Writes to both output_databricks/ (artifact) and the workspace copy
            (what dbt compile/run actually sees) — kept in lockstep always, so the
            workspace never carries stale content from a previous Transpiler run.
            """
            dest = self.output_dir / "models" / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(content)
            ws_dest = workspace_models_dir / rel
            ws_dest.parent.mkdir(parents=True, exist_ok=True)
            ws_dest.write_text(content)

        for sql_file in sql_files:
            rel = sql_file.relative_to(source_models_dir)
            raw_sql = sql_file.read_text()
            lb_file = lakebridge_out / rel

            if has_multiple_statements(raw_sql):
                write_result(rel, raw_sql)
                results.append(ModelTranspileResult(
                    str(rel), "manual_review", [], True,
                    "file appears to contain multiple top-level statements — "
                    "per-statement split/transpile is not implemented, review manually",
                ))
                continue

            if not lb_file.exists():
                write_result(rel, raw_sql)
                error_line = next(
                    (line for line in lakebridge_log.splitlines() if str(sql_file) in line and "ERROR" in line),
                    "Lakebridge did not produce output for this file (parsing/analysis error)",
                )
                results.append(ModelTranspileResult(
                    str(rel), "manual_review", [], True, error_line,
                ))
                continue

            lb_output = lb_file.read_text()

            if JINJA_CORRUPTION_RE.search(lb_output):
                write_result(rel, raw_sql)
                results.append(ModelTranspileResult(
                    str(rel), "hard_stop", [], True,
                    "Lakebridge produced a broken Jinja placeholder token in its output "
                    "(likely a Jinja expression used as a value inside an unusual SQL "
                    "clause, e.g. a PIVOT IN(...) list) — kept original raw SQL, needs "
                    "manual transpilation",
                ))
                continue

            if INTERNAL_ERROR_RE.search(lb_output):
                write_result(rel, raw_sql)
                results.append(ModelTranspileResult(
                    str(rel), "hard_stop", [], True,
                    "Lakebridge wrote output that starts with an injected '-- internal "
                    "error' comment — a silent internal failure not caught by exit code "
                    "or file-existence checks. Kept original raw SQL, needs manual "
                    "transpilation.",
                ))
                continue

            dropped_ctes = find_dropped_ctes(raw_sql, lb_output)
            if dropped_ctes:
                write_result(rel, raw_sql)
                results.append(ModelTranspileResult(
                    str(rel), "hard_stop", [], True,
                    f"Lakebridge silently dropped the CTE definition for "
                    f"{', '.join(dropped_ctes)} (body was pure Jinja control flow) while "
                    "keeping references to it — kept original raw SQL, needs manual "
                    "transpilation.",
                ))
                continue

            final_sql, fixes = post_process(lb_output)
            manual_flags = detect_manual_rewrite_needed(final_sql)
            write_result(rel, final_sql)

            if manual_flags:
                results.append(ModelTranspileResult(
                    str(rel), "manual_review", fixes, True, "; ".join(manual_flags),
                ))
            else:
                results.append(ModelTranspileResult(
                    str(rel), "success", fixes, False,
                    "; ".join(fixes) if fixes else "no post-processor fixes needed",
                ))

        try:
            self.write_audit(results, run_id)
        except (StatementError, DatabricksError) as e:
            print(f"[warn] could not write to audit table: {e}", file=sys.stderr)

        compile_ok, tail = self.run_dbt_compile()

        return TranspilerReport(run_id=run_id, results=results, dbt_compile_ok=compile_ok, dbt_output_tail=tail)

    def run_dbt_compile(self) -> tuple[bool, str]:
        try:
            proc = subprocess.run(
                ["dbt", "compile", "--project-dir", str(self.project_path), "--target", self.dbt_target],
                capture_output=True, text=True, timeout=300,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            return False, str(e)
        return proc.returncode == 0, (proc.stdout + proc.stderr)[-4000:]

    def write_audit(self, results: list[ModelTranspileResult], run_id: str) -> None:
        if not results:
            return
        self.client = self.client or get_client(self.profile)
        now = datetime.now(timezone.utc).isoformat()

        def esc(s: str) -> str:
            return s.replace("'", "''")

        rows_sql = []
        for r in results:
            model_name = Path(r.model_path).stem
            rows_sql.append("(" + ", ".join([
                f"'{esc(model_name)}'", f"'{esc(r.status)}'",
                str(r.requires_human_review).upper(), f"'{esc(self.developer)}'",
                f"'{esc(run_id)}'", f"TIMESTAMP'{now}'",
            ]) + ")")
        stmt = (
            f"INSERT INTO {self.catalog}.audit.model_runs "
            "(model_name, transpile_status, requires_human_review, developer, run_id, run_timestamp) "
            "VALUES " + ", ".join(rows_sql)
        )
        execute_sql(self.client, self.warehouse_id, stmt, catalog=self.catalog, schema="audit")


def main() -> int:
    parser = argparse.ArgumentParser(description="Transpiler Agent")
    parser.add_argument("project_path")
    parser.add_argument("--profile", default="free_community")
    parser.add_argument("--catalog", default="dbt_migration")
    parser.add_argument("--warehouse-id", default="b05480be6edc2be5")
    parser.add_argument("--dbt-target", default="dev")
    parser.add_argument("--developer", default="unknown")
    parser.add_argument("--source-dialect", default="snowflake")
    parser.add_argument("--reset-workspace", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    agent = TranspilerAgent(
        project_path=args.project_path, profile=args.profile, catalog=args.catalog,
        warehouse_id=args.warehouse_id, dbt_target=args.dbt_target, developer=args.developer,
        source_dialect=args.source_dialect, reset_workspace=args.reset_workspace,
    )
    report = agent.transpile()

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        print("Transpiler Agent")
        print(f"workspace copy: {agent.project_path}")
        print(f"output_databricks: {agent.output_dir}")
        print("=" * 60)
        for r in report.results:
            flag = " [REVIEW]" if r.requires_human_review else ""
            print(f"  [{r.status}] {r.model_path}{flag} — {r.notes}")
        counts: dict[str, int] = {}
        for r in report.results:
            counts[r.status] = counts.get(r.status, 0) + 1
        print(f"\n{counts}")
        print(f"dbt compile: {'OK' if report.dbt_compile_ok else 'FAILED'}")
        if not report.dbt_compile_ok:
            print(report.dbt_output_tail[-2000:])
        print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
