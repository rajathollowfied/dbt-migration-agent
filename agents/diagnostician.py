"""Agent 7 — Diagnostician Agent.

Purpose: classify errors and fix what can be fixed automatically.
See AGENT_DESIGN.md Section 4 (Agent 7) for the full spec.

Retry loop design (confirmed with user, 2026-09-11): AGENT_DESIGN.md says the
loop "goes back to Transpiler" after a fix. Re-invoking the actual Transpiler
Agent would be self-defeating — it always regenerates every model from the
pristine original source (see transpiler.py's idempotency fix), so it would
discard whatever fix was just applied. What the "loop back to Transpiler" was
really meant to provide is a *safety net*: catch a syntax/dialect problem an
LLM-generated fix might introduce. So this agent reuses Transpiler's own
`post_process()` directly as that safety net (idempotent — a no-op if the fix
is already clean), edits the model file in place, and re-tests with a
*targeted* `dbt run --select <model>` — never re-invoking Lakebridge (feeding
already-valid Databricks SQL back through a Snowflake-source-dialect
transpiler risks mangling Databricks-only syntax it doesn't recognize).

Classification (14 categories) + auto-fix scope: reuses FINDINGS.md Section 4
patterns and existing regex logic from transpiler.py/macro_resolver.py where
possible, so the three agents never disagree about what a given error means.
Per AGENT_DESIGN.md, several categories are architectural and explicitly
excluded from auto-fix/LLM-fallback — those go straight to the human review
queue with no retry attempted.

LLM fallback: Databricks Model Serving, confirmed available in this workspace
(`databricks-gpt-oss-120b`, a reasoning model — response content is a list of
blocks, the fix text is the `type: "text"` block, not the `type: "reasoning"`
block).

Failure behavior: never hard-stops. Max retries exceeded -> human review
queue, continue.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from agents.common.db import StatementError, execute_sql, get_client
from agents.common.workspace import ensure_workspace_copy
from agents.transpiler import post_process

LLM_ENDPOINT = "databricks-gpt-oss-120b"

# ---------------------------------------------------------------------------
# 14-category classifier
# ---------------------------------------------------------------------------

# (category_id, name, error_message_pattern, auto_fixable, llm_eligible)
# auto_fixable/llm_eligible=False categories are architectural per AGENT_DESIGN.md
# ("Does NOT fix: architectural decisions (streams, missing data, Python cluster
# config)") — straight to human review, never retried.
CATEGORIES: list[tuple[int, str, re.Pattern, bool, bool]] = [
    (1, "type_casting", re.compile(r"::\s*(varchar|number|integer|timestamp_ntz|date)\b", re.IGNORECASE), True, True),
    (2, "missing_function", re.compile(r"UNRESOLVED_ROUTINE|\bsysdate\s*\(|CURRENT_WAREHOUSE\s*\(", re.IGNORECASE), True, True),
    (3, "sequence_error", re.compile(r"CREATE\s+SEQUENCE|\.nextval\b|for SEQUENCE:.*argument", re.IGNORECASE), False, False),
    (4, "stream_error", re.compile(r"SHOW\s+STREAMS|metadata\$\w+|near 'stream'", re.IGNORECASE), False, False),
    # Genuine array-literal syntax: brackets wrapping quoted values, e.g. ['a','b'].
    # NOT a bare `[ERROR_CODE]` prefix — Databricks puts one of those on every message.
    (5, "array_literal", re.compile(r"\[\s*['\"][^\]]*['\"]\s*\]"), True, True),
    (6, "sampling_error", re.compile(r"SAMPLE\s+ROW|TABLESAMPLE", re.IGNORECASE), True, True),
    (7, "materialization_error", re.compile(r"\bdynamic_table\b", re.IGNORECASE), True, False),
    (8, "session_command", re.compile(r"ALTER\s+SESSION|USE\s+WAREHOUSE", re.IGNORECASE), True, True),
    (9, "data_type_error", re.compile(r"\bVARCHAR\(\d+\)|\bNUMBER\(\d+\s*,\s*\d+\)", re.IGNORECASE), True, True),
    (10, "package_error", re.compile(r"dbt_constraints", re.IGNORECASE), True, False),
    (11, "missing_source_data", re.compile(r"TABLE_OR_VIEW_NOT_FOUND|cannot be found.*Verify the spelling", re.IGNORECASE), False, False),
    (12, "python_cluster_error", re.compile(r"all[_-]purpose cluster|http_path.*cluster_id", re.IGNORECASE), False, False),
    # auto_fixable: confirmed (live dbt run against the real warehouse) that
    # switching to materialized_view resolves this — see fix_streaming_table_error.
    (13, "streaming_table_error", re.compile(r"STREAMING_TABLE_QUERY_INVALID|STREAM keyword", re.IGNORECASE), True, False),
]
UNKNOWN_CATEGORY = (14, "unknown", None, False, True)

# Databricks prefixes an error with a bracketed code, e.g. `[PARSE_SYNTAX_ERROR]` —
# not necessarily at message start (often after a "Database Error in model X" wrapper).
# Strip every such occurrence before matching — otherwise a loose category pattern
# can accidentally match the error-code bracket itself rather than anything about
# the real cause (confirmed: `[PARSE_SYNTAX_ERROR] Syntax` matched an early
# array-literal pattern on an unrelated Snowflake-system-table error, burning 3
# retries + 3 LLM calls before landing in human review anyway).
_ERROR_CODE_PREFIX_RE = re.compile(r"\[[A-Z][A-Z0-9_.]*\]")


def classify_error(message: str) -> tuple[int, str, bool, bool]:
    searchable = _ERROR_CODE_PREFIX_RE.sub("", message)
    for cat_id, name, pattern, auto_fixable, llm_eligible in CATEGORIES:
        if pattern.search(searchable):
            return cat_id, name, auto_fixable, llm_eligible
    return UNKNOWN_CATEGORY[0], UNKNOWN_CATEGORY[1], UNKNOWN_CATEGORY[3], UNKNOWN_CATEGORY[4]


# ---------------------------------------------------------------------------
# Deterministic fixes — reuse existing, already-validated regex logic rather
# than re-implementing FINDINGS.md patterns a third time.
# ---------------------------------------------------------------------------

STREAMING_TABLE_RE = re.compile(r"materialized\s*=\s*(['\"])streaming_table\1")


def fix_streaming_table_error(sql: str) -> tuple[str, str]:
    """Databricks Streaming Tables reject aggregation and self-referencing
    correlated subqueries (STREAMING_TABLE_QUERY_INVALID) — confirmed against the
    real warehouse, and exactly what both real failures in this project do
    (order_facts_dynamic's GROUP BY, dim_current_year_orders' self-join MAX()
    filter). Materialized View has no such restriction (it's batch
    re-computation on a schedule, not incremental stream processing) and is
    still an auto-refreshing object — the correct general-purpose Databricks
    equivalent for what was Snowflake dynamic_table. Confirmed working via a
    live `dbt run` test."""
    if STREAMING_TABLE_RE.search(sql):
        new_sql = STREAMING_TABLE_RE.sub(lambda m: f"materialized={m.group(1)}materialized_view{m.group(1)}", sql)
        return new_sql, "materialized streaming_table -> materialized_view (streaming tables reject aggregation/self-referencing queries)"
    return sql, ""


def apply_deterministic_fix(category_name: str, sql: str) -> tuple[str, str]:
    """Returns (fixed_sql, description). description is '' if no deterministic
    fix applies to this category (caller falls back to LLM)."""
    if category_name == "streaming_table_error":
        new_sql, desc = fix_streaming_table_error(sql)
        if desc:
            return new_sql, desc
    new_sql, fixes = post_process(sql)
    if fixes:
        return new_sql, "; ".join(fixes)
    return sql, ""


# ---------------------------------------------------------------------------
# LLM fallback
# ---------------------------------------------------------------------------

def extract_text_response(message_content) -> str:
    """The configured endpoint (gpt-oss-120b) is a reasoning model — content is
    a list of blocks; the fix is in the `type: "text"` block, not `type:
    "reasoning"` (that's chain-of-thought, not the answer)."""
    if isinstance(message_content, str):
        return message_content
    for block in message_content or []:
        if isinstance(block, dict) and block.get("type") == "text":
            return block["text"]
    return ""


def apply_llm_fix(client, sql: str, error_message: str) -> tuple[str, str]:
    prompt = (
        "You are fixing a dbt SQL model that fails on Databricks SQL. "
        "Fix ONLY what's needed to resolve the error below — preserve all "
        "business logic, Jinja templating (config blocks, ref()/source() calls, "
        "macro calls, control flow), comments, and formatting style exactly as "
        "given wherever they aren't the cause of the error. "
        "Return ONLY the corrected SQL file content, no explanation, no markdown "
        "code fences.\n\n"
        f"Databricks error:\n{error_message}\n\n"
        f"Current SQL file:\n{sql}"
    )
    resp = client.serving_endpoints.query(
        name=LLM_ENDPOINT,
        messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
        max_tokens=4000,
    )
    fixed = extract_text_response(resp.choices[0].message.content).strip()
    if fixed.startswith("```"):
        fixed = re.sub(r"^```[a-zA-Z]*\n|\n```$", "", fixed)
    return fixed, "llm_generated fix via " + LLM_ENDPOINT


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

@dataclass
class DiagnosisResult:
    model_name: str
    error_category_id: int
    error_category: str
    attempted_fix: str
    fix_successful: bool
    retry_count: int
    final_error_message: str
    requires_human_review: bool


@dataclass
class DiagnosticianReport:
    run_id: str
    results: list[DiagnosisResult]

    def to_dict(self) -> dict:
        return asdict(self)


class DiagnosticianAgent:
    def __init__(
        self,
        project_path: str,
        profile: str = "free_community",
        catalog: str = "dbt_migration",
        warehouse_id: str = "b05480be6edc2be5",
        dbt_target: str = "dev",
        developer: str = "unknown",
        max_retries: int = 3,
        reset_workspace: bool = False,
    ):
        self.source_path = Path(project_path).resolve()
        self.project_path = ensure_workspace_copy(self.source_path, reset=reset_workspace)
        self.profile = profile
        self.catalog = catalog
        self.warehouse_id = warehouse_id
        self.dbt_target = dbt_target
        self.developer = developer
        self.max_retries = max_retries
        self.client = None

    def read_failed_models(self) -> list[tuple[str, str, str]]:
        """Returns (model_name, original_file_path relative to models/, error_message)
        for every model with status 'error'/'fail' in the last dbt run — skips
        'skipped' (blocked-by-upstream) since those resolve on their own once the
        upstream model is fixed."""
        run_results_path = self.project_path / "target" / "run_results.json"
        manifest_path = self.project_path / "target" / "manifest.json"
        if not run_results_path.exists():
            return []
        rr = json.loads(run_results_path.read_text())
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
        nodes = manifest.get("nodes", {})

        failed = []
        for r in rr.get("results", []):
            uid = r.get("unique_id", "")
            if not uid.startswith("model.") or r.get("status") not in ("error", "fail"):
                continue
            node = nodes.get(uid, {})
            failed.append((
                node.get("name", uid.split(".")[-1]),
                node.get("original_file_path", ""),
                (r.get("message") or "").strip(),
            ))
        return failed

    def run_single_model(self, model_name: str) -> tuple[bool, str]:
        try:
            proc = subprocess.run(
                ["dbt", "run", "--select", model_name, "--project-dir", str(self.project_path),
                 "--target", self.dbt_target],
                capture_output=True, text=True, timeout=600,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            return False, str(e)

        run_results_path = self.project_path / "target" / "run_results.json"
        if not run_results_path.exists():
            return proc.returncode == 0, (proc.stdout + proc.stderr)[-2000:]
        rr = json.loads(run_results_path.read_text())
        for r in rr.get("results", []):
            if r.get("unique_id", "").endswith(f".{model_name}"):
                ok = r.get("status") == "success"
                return ok, "" if ok else (r.get("message") or "").strip()
        return proc.returncode == 0, (proc.stdout + proc.stderr)[-2000:]

    def diagnose_one(self, model_name: str, rel_path: str, error_message: str) -> DiagnosisResult:
        cat_id, cat_name, auto_fixable, llm_eligible = classify_error(error_message)

        if not auto_fixable and not llm_eligible:
            return DiagnosisResult(
                model_name, cat_id, cat_name, "", False, 0, error_message,
                requires_human_review=True,
            )

        sql_path = self.project_path / rel_path
        if not sql_path.exists():
            return DiagnosisResult(
                model_name, cat_id, cat_name, "", False, 0,
                f"model file not found at {rel_path}", True,
            )

        current_error = error_message
        fix_desc = ""
        for attempt in range(1, self.max_retries + 1):
            current_sql = sql_path.read_text()
            fix_desc = ""

            if auto_fixable:
                fixed_sql, fix_desc = apply_deterministic_fix(cat_name, current_sql)
            else:
                fixed_sql = current_sql

            if not fix_desc and llm_eligible:
                self.client = self.client or get_client(self.profile)
                fixed_sql, fix_desc = apply_llm_fix(self.client, current_sql, current_error)

            if not fix_desc:
                break  # nothing left to try

            # Safety net: same idempotent post-processor Transpiler uses, catches
            # syntax/dialect issues an LLM fix might introduce.
            fixed_sql, safety_fixes = post_process(fixed_sql)
            if safety_fixes:
                fix_desc += "; safety-net: " + "; ".join(safety_fixes)

            sql_path.write_text(fixed_sql)
            ok, new_error = self.run_single_model(model_name)

            if ok:
                self.record_pattern(cat_name, fix_desc, "llm_generated" if "llm_generated" in fix_desc else "deterministic")
                return DiagnosisResult(
                    model_name, cat_id, cat_name, fix_desc, True, attempt, "", False,
                )
            current_error = new_error or current_error

        return DiagnosisResult(
            model_name, cat_id, cat_name, fix_desc,
            False, self.max_retries, current_error, True,
        )

    def record_pattern(self, category: str, fix_template: str, source: str) -> None:
        try:
            self.client = self.client or get_client(self.profile)
            now = datetime.now(timezone.utc).isoformat()

            def esc(s: str) -> str:
                return s.replace("'", "''")

            stmt = (
                f"INSERT INTO {self.catalog}.audit.pattern_library "
                "(pattern_id, error_category, regex_pattern, fix_template, source, "
                "times_applied, last_applied, created_at, created_by) VALUES ("
                f"'{uuid.uuid4()}', '{esc(category)}', '', '{esc(fix_template[:2000])}', "
                f"'{esc(source)}', 1, TIMESTAMP'{now}', TIMESTAMP'{now}', '{esc(self.developer)}')"
            )
            execute_sql(self.client, self.warehouse_id, stmt, catalog=self.catalog, schema="audit")
        except (StatementError, DatabricksError) as e:
            print(f"[warn] could not write pattern_library: {e}", file=sys.stderr)

    def write_audit(self, results: list[DiagnosisResult], run_id: str) -> None:
        if not results:
            return
        self.client = self.client or get_client(self.profile)
        now = datetime.now(timezone.utc).isoformat()

        def esc(s: str) -> str:
            return s.replace("'", "''")

        rows_sql = []
        for r in results:
            rows_sql.append("(" + ", ".join([
                f"'{esc(r.model_name)}'", f"'{esc(r.error_category)}'", f"'{esc(r.attempted_fix)}'",
                str(r.fix_successful).upper(), str(r.retry_count), f"'{esc(r.final_error_message)}'",
                str(r.requires_human_review).upper(), f"'{esc(self.developer)}'",
                f"'{esc(run_id)}'", f"TIMESTAMP'{now}'",
            ]) + ")")
        stmt = (
            f"INSERT INTO {self.catalog}.audit.model_runs "
            "(model_name, error_category, attempted_fix, fix_successful, retry_count, "
            "final_error_message, requires_human_review, developer, run_id, run_timestamp) "
            "VALUES " + ", ".join(rows_sql)
        )
        execute_sql(self.client, self.warehouse_id, stmt, catalog=self.catalog, schema="audit")

    def run(self) -> DiagnosticianReport:
        run_id = str(uuid.uuid4())
        failed_models = self.read_failed_models()

        # `run_single_model()` uses `dbt run --select <model>`, which overwrites
        # target/run_results.json with just that one model's result — confirmed
        # this silently corrupts the file for anyone reading it afterward (a
        # second Diagnostician invocation without an intervening Executor run saw
        # only 1 "failed" model instead of the real 9). Snapshot and restore the
        # full-run file so it always reflects the last full Executor run.
        run_results_path = self.project_path / "target" / "run_results.json"
        original_run_results = run_results_path.read_text() if run_results_path.exists() else None

        results = [self.diagnose_one(name, path, msg) for name, path, msg in failed_models]

        if original_run_results is not None:
            run_results_path.write_text(original_run_results)

        try:
            self.write_audit(results, run_id)
        except (StatementError, DatabricksError) as e:
            print(f"[warn] could not write to audit table: {e}", file=sys.stderr)

        return DiagnosticianReport(run_id=run_id, results=results)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnostician Agent")
    parser.add_argument("project_path")
    parser.add_argument("--profile", default="free_community")
    parser.add_argument("--catalog", default="dbt_migration")
    parser.add_argument("--warehouse-id", default="b05480be6edc2be5")
    parser.add_argument("--dbt-target", default="dev")
    parser.add_argument("--developer", default="unknown")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--reset-workspace", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    agent = DiagnosticianAgent(
        project_path=args.project_path, profile=args.profile, catalog=args.catalog,
        warehouse_id=args.warehouse_id, dbt_target=args.dbt_target, developer=args.developer,
        max_retries=args.max_retries, reset_workspace=args.reset_workspace,
    )
    report = agent.run()

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        print("Diagnostician Agent")
        print(f"workspace copy: {agent.project_path}")
        print("=" * 60)
        for r in report.results:
            status = "FIXED" if r.fix_successful else ("REVIEW" if r.requires_human_review else "?")
            print(f"  [{status}] {r.model_name} (category {r.error_category_id}: {r.error_category}, "
                  f"retries={r.retry_count}) — {r.attempted_fix or r.final_error_message}")
        fixed = sum(1 for r in report.results if r.fix_successful)
        print(f"\n{fixed}/{len(report.results)} auto-fixed, "
              f"{len(report.results) - fixed} sent to human review queue")
        print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
