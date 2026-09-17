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

Also runs the identical pipeline (steps 2-6) over snapshots/ if it exists and
has any .sql files (2026-09-14 — AGENT_DESIGN.md never scoped snapshots in
originally; found live, tested, and wired in as a real gap, not assumed).
The only difference: a snapshot's dbt-visible node name is declared inside
the file itself (`{% snapshot NAME %}`), not implied by the filename the way
a model's is — see SNAPSHOT_NAME_RE / ModelTranspileResult.audit_name.

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
from agents.common.config import DEFAULT_CATALOG, DEFAULT_PROFILE, DEFAULT_WAREHOUSE_ID
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
# A snapshot file's dbt-visible node name is declared inside the tag itself
# (`{% snapshot NAME %}`) and is not required to match the filename — unlike
# models, where dbt enforces filename == model name. Extracted from the raw
# source (unaffected by transpilation) to keep Transpiler's own audit rows
# aligned with what Executor's manifest-derived rows will call the same node.
SNAPSHOT_NAME_RE = re.compile(r"\{%-?\s*snapshot\s+(\w+)\s*-?%\}")
VARCHAR_SIZE_RE = re.compile(r"\bVARCHAR\(\d+\)", re.IGNORECASE)
ALTER_SESSION_RE = re.compile(r"ALTER\s+SESSION\s+SET\s+(WEEK_START|WEEK_OF_YEAR_POLICY)\s*=\s*\d+\s*;?", re.IGNORECASE)
USE_WAREHOUSE_RE = re.compile(r"\bUSE\s+WAREHOUSE\s+\S+\s*;?", re.IGNORECASE)
TABLESAMPLE_ALIAS_BEFORE_RE = re.compile(
    r"(\bFROM\s+[\w.]+)\s+AS\s+(\w+)\s+(TABLESAMPLE\s*\([^)]*\))", re.IGNORECASE
)
# Snowflake's bare SAMPLE(n) (no ROW/TABLESAMPLE keyword) is percent-based by
# default and has no Databricks equivalent without the TABLESAMPLE keyword —
# confirmed against the real warehouse (PARSE_SYNTAX_ERROR). Neither Lakebridge
# nor the TABLESAMPLE-alias fix above touches this shape (real failure found
# 2026-09-14: customer_cdc_stream.sql's `SAMPLE(10)` survived transpilation
# untouched). \b blocks matching "SAMPLE" inside "TABLESAMPLE" itself.
BARE_SAMPLE_RE = re.compile(r"\bSAMPLE\s*\(\s*(\d+)\s*\)", re.IGNORECASE)
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

    if BARE_SAMPLE_RE.search(new_sql):
        new_sql = BARE_SAMPLE_RE.sub(lambda m: f"TABLESAMPLE ({m.group(1)} PERCENT)", new_sql)
        fixes.append("SAMPLE(n) -> TABLESAMPLE (n PERCENT) (Snowflake's bare form is percent-based; "
                     "Databricks requires the TABLESAMPLE keyword)")

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


def _ensure_lakebridge_importable() -> None:
    """databricks.labs.blueprint's logging setup calls find_project_root() the
    first time any databricks.labs.lakebridge submodule (e.g. .cli) is
    imported — it walks up from the importing file looking for a
    pyproject.toml/setup.py, present in the git-clone-based `databricks labs
    install` layout this library normally expects, but not guaranteed for a
    plain `pip install` (this project's own approach — see
    docker/install_morpheus.py). A bare `import databricks.labs.lakebridge`
    (this package's own __init__.py) does NOT trigger this — confirmed live
    — so it's safe to import first just to locate the package directory.

    Self-heals by dropping an empty pyproject.toml at the lakebridge
    package's own root if nothing walkable already exists, rather than
    depending on a Docker-build-time fix or an environment happening to
    already have one lying around by chance (confirmed: local dev's own
    working install only avoided this crash because of an unrelated stray
    pyproject.toml already sitting in site-packages/ from a different
    package — not a real guarantee for a fresh venv anywhere else).
    """
    import databricks.labs.lakebridge as lakebridge_pkg
    from databricks.labs.blueprint.entrypoint import find_dir_with_leaf

    pkg_dir = Path(lakebridge_pkg.__file__).resolve().parent
    has_marker = find_dir_with_leaf(pkg_dir, "pyproject.toml") or find_dir_with_leaf(pkg_dir, "setup.py")
    if not has_marker:
        (pkg_dir / "pyproject.toml").touch()


def run_lakebridge(input_dir: Path, output_dir: Path, profile: str | None, source_dialect: str = "snowflake") -> str:
    """Runs Lakebridge over the whole tree in one process, via its own Python
    API directly (databricks.labs.lakebridge.cli.transpile) rather than
    shelling out to `databricks labs lakebridge transpile`.

    Real bug found and fixed (2026-09-1x, see CHECKPOINT.md): the CLI
    subprocess route depends on "lakebridge" being registered in the
    `databricks` CLI's own separate installed-apps bookkeeping
    (~/.databricks/labs/databrickslabs-repositories.json) — which
    docker/install_morpheus.py never populates, since it installs the
    transpiler engine artifact directly via Python, deliberately bypassing
    the full interactive `databricks labs install lakebridge` flow (see that
    file's own docstring for why). Confirmed empirically inside the Docker
    image: the exact subprocess command fails with "unknown flag:
    --input-source", silently no-oping Transpiler for every single file,
    every run — the pipeline still "succeeds" (soft-fail by design) but
    nothing actually gets transpiled. The Python API needs no such CLI
    registration at all, since it talks to the transpiler engine directly.

    Two explicit overrides are required, confirmed by two real crashes while
    testing this: `error_file_path` and `transpiler_config_path` both
    otherwise default to *workspace-stored* config
    (ApplicationContext/Installation.load(), persisted against the
    Databricks user, not the local machine) — since this reuses the same
    Databricks user/workspace as local dev, a container run picked up local
    dev's own previously-cached paths (a literal local-machine absolute path
    leaked through and failed validation, since it doesn't exist in the
    container). Resolving `transpiler_config_path` from the repository
    (rather than hardcoding it) keeps this correct if the installed
    transpiler's own internal directory layout ever changes.

    A non-zero/error result here just means *some* files had parsing/
    analysis errors (Lakebridge's own per-file error count) — it still
    writes output for every file it could handle. Per-file success is
    determined by whether output exists for that file (see
    _transpile_directory), not by this succeeding cleanly.

    `profile=None` builds a bare WorkspaceClient(), which falls back to the
    SDK's own default auth resolution (~/.databrickscfg [DEFAULT], or
    DATABRICKS_HOST/DATABRICKS_TOKEN) — same convention as get_client().
    """
    import io
    from contextlib import redirect_stderr, redirect_stdout

    _ensure_lakebridge_importable()
    from databricks.labs.lakebridge.cli import transpile as lakebridge_transpile
    from databricks.labs.lakebridge.transpiler.repository import TranspilerRepository
    from databricks.sdk import WorkspaceClient

    if profile:
        os.environ["DATABRICKS_CONFIG_PROFILE"] = profile
        client = WorkspaceClient(profile=profile)
    else:
        client = WorkspaceClient()

    repo = TranspilerRepository.user_home()
    error_file_path = output_dir.parent / f"_lakebridge_errors_{output_dir.name}.log"
    error_file_path.parent.mkdir(parents=True, exist_ok=True)

    buf = io.StringIO()
    try:
        with redirect_stdout(buf), redirect_stderr(buf):
            lakebridge_transpile(
                w=client,
                source_dialect=source_dialect,
                input_source=str(input_dir),
                output_folder=str(output_dir),
                error_file_path=str(error_file_path),
                transpiler_config_path=str(repo.transpiler_config_path("Morpheus")),
                skip_validation="true",
            )
    except Exception as e:  # noqa: BLE001 - surfaced as log text, same as a non-zero CLI exit was
        buf.write(f"\nlakebridge transpile raised: {e}\n")

    log = buf.getvalue()
    if error_file_path.exists():
        log += "\n" + error_file_path.read_text()
    return log


@dataclass
class ModelTranspileResult:
    model_path: str
    status: str  # success / hard_stop / manual_review / skipped
    fixes_applied: list[str]
    requires_human_review: bool
    notes: str
    # Set only for snapshots: the dbt-visible node name declared inside
    # `{% snapshot NAME %}`, which is NOT necessarily the file's stem (unlike
    # models, where dbt requires filename == model name). Executor's own
    # audit rows key on this same manifest-derived `name`, so write_audit()
    # must match it exactly or Transpiler/Executor rows for the same node
    # would silently disagree. None for models, where the stem is correct.
    audit_name: str | None = None


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
        profile: str | None = DEFAULT_PROFILE,
        catalog: str = DEFAULT_CATALOG,
        warehouse_id: str | None = DEFAULT_WAREHOUSE_ID,
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

    def _transpile_directory(
        self, source_dir: Path, workspace_dir: Path, output_subdir: str, is_snapshot: bool = False,
    ) -> list[ModelTranspileResult]:
        """Runs Lakebridge + the post-processor + all 4 corruption detectors over
        every .sql file in source_dir, writing results to both output_databricks/
        and the workspace copy. Shared by models/ and snapshots/ — the only
        difference is is_snapshot, which controls how the audit-table name is
        derived (declared `{% snapshot NAME %}` name vs. filename stem)."""
        sql_files = sorted(p for p in source_dir.rglob("*.sql"))
        py_files = sorted(p for p in source_dir.rglob("*.py"))

        results: list[ModelTranspileResult] = []
        for p in py_files:
            results.append(ModelTranspileResult(
                str(p.relative_to(source_dir)), "skipped", [], False,
                "Python dbt model — not a SQL transpilation target",
            ))

        lakebridge_out = self.output_dir / f"_lakebridge_raw_{output_subdir}"
        if lakebridge_out.exists():
            shutil.rmtree(lakebridge_out)
        lakebridge_out.parent.mkdir(parents=True, exist_ok=True)  # Lakebridge needs the parent to pre-exist
        lakebridge_log = run_lakebridge(source_dir, lakebridge_out, self.profile, self.source_dialect)

        def write_result(rel: Path, content: str) -> None:
            """Writes to both output_databricks/ (artifact) and the workspace copy
            (what dbt compile/run actually sees) — kept in lockstep always, so the
            workspace never carries stale content from a previous Transpiler run.
            """
            dest = self.output_dir / output_subdir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(content)
            ws_dest = workspace_dir / rel
            ws_dest.parent.mkdir(parents=True, exist_ok=True)
            ws_dest.write_text(content)

        for sql_file in sql_files:
            rel = sql_file.relative_to(source_dir)
            raw_sql = sql_file.read_text()
            lb_file = lakebridge_out / rel
            # The declared snapshot name doesn't change through transpilation —
            # extract from raw source once, reused for every result below.
            audit_name = None
            if is_snapshot:
                m = SNAPSHOT_NAME_RE.search(raw_sql)
                audit_name = m.group(1) if m else None

            if has_multiple_statements(raw_sql):
                write_result(rel, raw_sql)
                results.append(ModelTranspileResult(
                    str(rel), "manual_review", [], True,
                    "file appears to contain multiple top-level statements — "
                    "per-statement split/transpile is not implemented, review manually",
                    audit_name,
                ))
                continue

            if not lb_file.exists():
                write_result(rel, raw_sql)
                error_line = next(
                    (line for line in lakebridge_log.splitlines() if str(sql_file) in line and "ERROR" in line),
                    "Lakebridge did not produce output for this file (parsing/analysis error)",
                )
                results.append(ModelTranspileResult(
                    str(rel), "manual_review", [], True, error_line, audit_name,
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
                    audit_name,
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
                    audit_name,
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
                    audit_name,
                ))
                continue

            final_sql, fixes = post_process(lb_output)
            manual_flags = detect_manual_rewrite_needed(final_sql)
            write_result(rel, final_sql)

            if manual_flags:
                results.append(ModelTranspileResult(
                    str(rel), "manual_review", fixes, True, "; ".join(manual_flags), audit_name,
                ))
            else:
                results.append(ModelTranspileResult(
                    str(rel), "success", fixes, False,
                    "; ".join(fixes) if fixes else "no post-processor fixes needed",
                    audit_name,
                ))

        return results

    def transpile(self) -> TranspilerReport:
        run_id = str(uuid.uuid4())
        # Always transpile from the untouched ORIGINAL source, never from the workspace
        # copy's models/ — Transpiler itself is the only agent that writes there, so a
        # second run would otherwise re-feed its own prior output back into Lakebridge
        # as if it were raw Snowflake SQL (confirmed: causes real corruption on rerun).
        source_models_dir = self.source_path / "models"
        workspace_models_dir = self.project_path / "models"
        results = self._transpile_directory(source_models_dir, workspace_models_dir, "models")

        # Snapshots are optional — most client projects won't have any — and use
        # the identical Lakebridge + post-processor + corruption-detector pipeline
        # as models/. Only real difference: a snapshot's dbt-visible name is
        # declared inside the file (`{% snapshot NAME %}`), not implied by the
        # filename, so _transpile_directory tracks that separately (audit_name).
        source_snapshots_dir = self.source_path / "snapshots"
        if source_snapshots_dir.exists() and any(source_snapshots_dir.rglob("*.sql")):
            workspace_snapshots_dir = self.project_path / "snapshots"
            results += self._transpile_directory(
                source_snapshots_dir, workspace_snapshots_dir, "snapshots", is_snapshot=True,
            )

        try:
            self.write_audit(results, run_id)
        except (StatementError, DatabricksError) as e:
            print(f"[warn] could not write to audit table: {e}")

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
            model_name = r.audit_name or Path(r.model_path).stem
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
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--warehouse-id", default=DEFAULT_WAREHOUSE_ID)
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
