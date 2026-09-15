"""cli.py — slash-command router for the dbt Snowflake -> Databricks migration
pipeline. See AGENT_DESIGN.md Section 7 for the original slash-command sketch.

That sketch only names 5 of the 8 agents as individual commands (preflight,
analyze, convert, diagnose, validate) — a documentation gap, same class as the
undocumented "19 patterns"/14 categories elsewhere in these docs. Filled in
the missing 3 (macros, load, execute) since OPEN_ITEMS.md's own decision #10
says "individual agent commands handle partial/resume" — that only works if
every agent has one.

Commands:
    preflight | macros | load | analyze | transpile | execute | diagnose | validate
        Run exactly that agent (thin wrapper around its own module's main()).
    run
        Full pipeline, Preflight through Validator, always end-to-end
        (OPEN_ITEMS.md decision #10 — no partial/resume for `run` itself).
        Failure routing per AGENT_DESIGN.md Section 6:
          Preflight, Macro Resolver -> hard stop, nothing downstream runs
          Data Loader, Analyzer, Transpiler, Executor, Diagnostician -> soft
            fail, always continue (each already handles its own per-item
            failures internally)
          Validator -> always runs, reports
        Writes one dbt_migration.audit.pipeline_runs row per invocation
        (INSERT at start, UPDATE at completion — this table is "one row per
        run", not an append log like the others).
    status
        Reads the most recent pipeline_runs row + model_runs summary +
        current human review queue size.
    help
        Lists commands.

Every agent already reads/writes its state to the shared migration-workspace
copy on disk (dbt_project.yml, macros/, models/, target/run_results.json) —
chaining agents in `run` doesn't need explicit task-value passing at this
layer; that only starts to matter once these become separate Databricks
Workflow tasks (a later, still-deferred step — see memory: build order is
local first, bundle once stable).
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from agents.analyzer import AnalyzerAgent
from agents.common.config import DEFAULT_CATALOG, DEFAULT_PROFILE, DEFAULT_WAREHOUSE_ID
from agents.common.db import StatementError, execute_sql, get_client
from agents.data_loader import DataLoaderAgent
from agents.diagnostician import DiagnosticianAgent
from agents.executor import ExecutorAgent
from agents.macro_resolver import MacroResolverAgent
from agents.preflight import PreflightAgent
from agents.transpiler import TranspilerAgent
from agents.validator import ValidatorAgent

AGENT_MODULES = {
    "preflight": "agents.preflight",
    "macros": "agents.macro_resolver",
    "load": "agents.data_loader",
    "analyze": "agents.analyzer",
    "transpile": "agents.transpiler",
    "execute": "agents.executor",
    "diagnose": "agents.diagnostician",
    "validate": "agents.validator",
}

HELP_TEXT = """dbt Migration Agent — commands

  /dbt-migrate:run         Full pipeline, Preflight through Validator (always end-to-end)
  /dbt-migrate:preflight   Agent 1 — connectivity, catalog, dbt_project.yml checks
  /dbt-migrate:macros      Agent 2 — macro classification + dispatch pattern
  /dbt-migrate:load        Agent 3 — source table redirect / copy
  /dbt-migrate:analyze     Agent 4 — model complexity + DAG + warehouse map
  /dbt-migrate:transpile   Agent 5 — Snowflake -> Databricks SQL conversion
  /dbt-migrate:execute     Agent 6 — dbt run --no-fail-fast
  /dbt-migrate:diagnose    Agent 7 — classify + auto-fix failed models
  /dbt-migrate:validate    Agent 8 — schema/row-count/checksum/business-rule checks
  /dbt-migrate:status      Current run status from the audit tables
  /dbt-migrate:apply-fix   Apply a Diagnostician *recommendation* (advisory-only
                           hard-stop categories, e.g. stream_error) that was
                           surfaced but not auto-applied — explicit opt-in
  /dbt-migrate:help        This message

Every command takes a project_path plus the shared flags (--profile,
--catalog, --warehouse-id, --dbt-target, --developer, --reset-workspace).
Run `python cli.py <command> --help` for a command's own flags.
"""


def run_agent_command(command: str, argv: list[str]) -> int:
    """Delegates to that agent module's own main() — no duplication of its
    argument parsing or pretty-printing."""
    import importlib
    module = importlib.import_module(AGENT_MODULES[command])
    sys.argv = [command] + argv
    return module.main()


class PipelineRunTracker:
    """Owns dbt_migration.audit.pipeline_runs — the one table that's an UPDATE
    target (one row per run_id) rather than an append-only log like every
    other audit table in this project."""

    def __init__(self, catalog: str, warehouse_id: str, profile: str):
        self.catalog = catalog
        self.warehouse_id = warehouse_id
        self.client = get_client(profile)
        self.run_id = str(uuid.uuid4())

    def start(self, developer: str, branch: str, command: str, max_retries: int) -> None:
        now = datetime.now(timezone.utc).isoformat()

        def esc(s) -> str:
            return str(s).replace("'", "''")

        stmt = (
            f"INSERT INTO {self.catalog}.audit.pipeline_runs "
            "(run_id, developer, branch, command, max_retries, start_time, pipeline_status) VALUES ("
            f"'{esc(self.run_id)}', '{esc(developer)}', '{esc(branch)}', '{esc(command)}', "
            f"{max_retries}, TIMESTAMP'{now}', 'running')"
        )
        try:
            execute_sql(self.client, self.warehouse_id, stmt, catalog=self.catalog, schema="audit")
        except StatementError as e:
            print(f"[warn] could not write pipeline_runs start: {e}", file=sys.stderr)

    def finish(self, start_time: datetime, summary: dict, status: str) -> None:
        end_time = datetime.now(timezone.utc)
        duration_minutes = round((end_time - start_time).total_seconds() / 60, 2)

        def sql_val(v):
            if v is None:
                return "NULL"
            if isinstance(v, (int, float)):
                return str(v)
            return f"'{str(v).replace(chr(39), chr(39) * 2)}'"

        stmt = (
            f"UPDATE {self.catalog}.audit.pipeline_runs SET "
            f"end_time = TIMESTAMP'{end_time.isoformat()}', "
            f"duration_minutes = {duration_minutes}, "
            f"total_models = {sql_val(summary.get('total_models'))}, "
            f"passed = {sql_val(summary.get('passed'))}, "
            f"failed = {sql_val(summary.get('failed'))}, "
            f"blocked = {sql_val(summary.get('blocked'))}, "
            f"needs_review = {sql_val(summary.get('needs_review'))}, "
            f"pipeline_status = {sql_val(status)} "
            f"WHERE run_id = '{self.run_id}'"
        )
        try:
            execute_sql(self.client, self.warehouse_id, stmt, catalog=self.catalog, schema="audit")
        except StatementError as e:
            print(f"[warn] could not write pipeline_runs completion: {e}", file=sys.stderr)


def run_full_pipeline(args: argparse.Namespace) -> int:
    developer = args.developer or "unknown"
    start_time = datetime.now(timezone.utc)
    tracker = PipelineRunTracker(args.catalog, args.warehouse_id, args.profile)
    tracker.start(developer, developer, "run", args.max_retries)

    # Not every agent's __init__ accepts the same kwargs (e.g. DataLoaderAgent has
    # no dbt_target, PreflightAgent/MacroResolverAgent/DataLoaderAgent have no
    # developer) — `common` only holds what every agent accepts; the rest are
    # added explicitly per call below.
    common = dict(
        project_path=args.project_path, profile=args.profile, catalog=args.catalog,
        warehouse_id=args.warehouse_id, reset_workspace=args.reset_workspace,
    )

    print("Agent 1/8 — Preflight...")
    preflight = PreflightAgent(**common, dbt_target=args.dbt_target)
    preflight_report = preflight.run()
    if not preflight_report.go:
        print("STOPPED at Preflight — see failed checks above. Nothing downstream ran.")
        tracker.finish(start_time, {}, "failed")
        return 1
    print("  GO ✓")

    print("Agent 2/8 — Macro Resolver...")
    macro_resolver = MacroResolverAgent(**common, dbt_target=args.dbt_target)
    macro_report = macro_resolver.run()
    if macro_report.compile_error_in_macro_layer:
        print("STOPPED at Macro Resolver — a compile error traces back to a macro this "
              "agent touched. Nothing downstream ran.")
        tracker.finish(start_time, {}, "failed")
        return 1
    print(f"  {len(macro_report.macro_resolutions)} macros processed ✓")

    print("Agent 3/8 — Data Loader...")
    data_loader = DataLoaderAgent(**common)
    load_report = data_loader.run()
    print(f"  {len(load_report.results)} source tables processed (soft fail per table, continuing)")

    print("Agent 4/8 — Analyzer...")
    analyzer = AnalyzerAgent(**common, dbt_target=args.dbt_target, developer=developer)
    analyzer_report = analyzer.run()
    print(f"  {analyzer_report.total_models} models classified: {analyzer_report.complexity_counts}")

    print("Agent 5/8 — Transpiler...")
    transpiler = TranspilerAgent(**common, dbt_target=args.dbt_target, developer=developer)
    transpile_report = transpiler.transpile()
    counts = {}
    for r in transpile_report.results:
        counts[r.status] = counts.get(r.status, 0) + 1
    print(f"  {counts} (soft fail — hard-stopped files kept original SQL, continuing)")

    print("Agent 6/8 — Executor...")
    executor = ExecutorAgent(**common, dbt_target=args.dbt_target, developer=developer)
    exec_report = executor.run()
    exec_counts: dict[str, int] = {}
    for r in exec_report.results:
        exec_counts[r.run_status] = exec_counts.get(r.run_status, 0) + 1
    print(f"  {exec_counts}")
    if exec_report.report_path:
        print(f"  Excel report: {exec_report.report_path}")

    print("Agent 7/8 — Diagnostician...")
    diagnostician = DiagnosticianAgent(**common, dbt_target=args.dbt_target, developer=developer, max_retries=args.max_retries)
    diag_report = diagnostician.run()
    fixed = sum(1 for r in diag_report.results if r.fix_successful)
    print(f"  {fixed}/{len(diag_report.results)} auto-fixed, "
          f"{len(diag_report.results) - fixed} sent to human review queue")

    print("Agent 8/8 — Validator...")
    validator = ValidatorAgent(**common, dbt_target=args.dbt_target, developer=developer)
    validation_results = validator.run()
    scored = [r.migration_score for r in validation_results if r.migration_score is not None]
    avg_score = round(sum(scored) / len(scored), 1) if scored else None
    print(f"  {len(validation_results)} models validated (only Executor-passing models are "
          f"eligible), avg validation score: {avg_score} — not an execution pass rate, see "
          f"the Excel report for that")

    needs_review = sum(1 for r in diag_report.results if r.requires_human_review)
    summary = {
        "total_models": len(exec_report.results),
        "passed": exec_counts.get("pass", 0),
        "failed": exec_counts.get("fail", 0),
        "blocked": exec_counts.get("blocked", 0),
        "needs_review": needs_review,
    }
    status = "success" if summary["failed"] == 0 and needs_review == 0 else "partial"
    tracker.finish(start_time, summary, status)

    print("=" * 60)
    print(f"Pipeline {status}. run_id: {tracker.run_id}")
    return 0


def run_apply_fix(args: argparse.Namespace) -> int:
    """Explicit, user-triggered apply for a Diagnostician recommendation.
    diagnose_one() never writes these fixes to disk on its own — it only logs
    a `source='recommended'` pattern_library row and points here. This is the
    interim CLI trigger for that "advise first, apply second" workflow (a
    future UI would call DiagnosticianAgent.apply_recommended_fix directly)."""
    agent = DiagnosticianAgent(
        project_path=args.project_path, profile=args.profile, catalog=args.catalog,
        warehouse_id=args.warehouse_id, dbt_target=args.dbt_target,
        developer=args.developer or "unknown",
    )
    ok, message = agent.apply_recommended_fix(args.model_name)
    print(f"[{'APPLIED' if ok else 'FAILED'}] {args.model_name}: {message}")
    return 0 if ok else 1


def fetch_status(client, catalog: str, warehouse_id: str):
    """Returns (latest_pipeline_run, review_queue) as raw SqlResult objects —
    shared by the CLI's `status` command and app.py's Status tab, so both
    read the exact same queries rather than app.py re-deriving its own.
    """
    from agents.common.db import execute_sql as _exec

    latest_pipeline_run = _exec(
        client, warehouse_id,
        f"SELECT run_id, developer, start_time, end_time, pipeline_status, total_models, "
        f"passed, failed, blocked, needs_review FROM {catalog}.audit.pipeline_runs "
        f"ORDER BY start_time DESC LIMIT 1",
        catalog=catalog,
    )

    # model_runs is an append-only log — every agent that touches a model writes
    # its own row, so the same failing model accumulates one row per agent per
    # run. Take only the most recent row per model, otherwise this lists the same
    # model multiple times (once from Executor's row with no error_category set,
    # once from Diagnostician's row with the real classification).
    review_queue = _exec(
        client, warehouse_id,
        f"""
        SELECT model_name, error_category, final_error_message FROM (
            SELECT model_name, error_category, final_error_message, requires_human_review,
                   ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY run_timestamp DESC) AS rn
            FROM {catalog}.audit.model_runs
        ) WHERE rn = 1 AND requires_human_review = true
        ORDER BY model_name
        LIMIT 20
        """,
        catalog=catalog,
    )
    return latest_pipeline_run, review_queue


def run_status(args: argparse.Namespace) -> int:
    client = get_client(args.profile)
    latest_pipeline_run, review_queue = fetch_status(client, args.catalog, args.warehouse_id)

    print("Most recent pipeline run:")
    if latest_pipeline_run.rows:
        for col, val in zip(latest_pipeline_run.columns, latest_pipeline_run.rows[0]):
            print(f"  {col}: {val}")
    else:
        print("  (no pipeline runs recorded yet)")

    print("\nHuman review queue (latest status per model, across all runs):")
    if review_queue.rows:
        for row in review_queue.rows:
            print(f"  - {row[0]} ({row[1]}): {str(row[2])[:100]}")
    else:
        print("  (empty)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="dbt Migration Agent CLI", add_help=True)
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p):
        p.add_argument("project_path")
        p.add_argument("--profile", default=DEFAULT_PROFILE)
        p.add_argument("--catalog", default=DEFAULT_CATALOG)
        p.add_argument("--warehouse-id", default=DEFAULT_WAREHOUSE_ID)
        p.add_argument("--dbt-target", default="dev")
        p.add_argument("--developer", default=None, help="defaults to 'unknown' if omitted")
        p.add_argument("--reset-workspace", action="store_true")

    run_p = sub.add_parser("run", help="Full pipeline, Preflight through Validator")
    add_common(run_p)
    run_p.add_argument("--max-retries", type=int, default=3)

    status_p = sub.add_parser("status", help="Current run status from the audit tables")
    status_p.add_argument("--profile", default=DEFAULT_PROFILE)
    status_p.add_argument("--catalog", default=DEFAULT_CATALOG)
    status_p.add_argument("--warehouse-id", default=DEFAULT_WAREHOUSE_ID)

    apply_fix_p = sub.add_parser(
        "apply-fix", help="Apply a Diagnostician recommendation (advisory-only categories)",
    )
    add_common(apply_fix_p)
    apply_fix_p.add_argument("model_name")

    sub.add_parser("help", help="List commands")

    for name in AGENT_MODULES:
        sub.add_parser(name, help=f"Run Agent — {name}", add_help=False)

    args, remainder = parser.parse_known_args()

    if args.command == "help" or args.command is None:
        print(HELP_TEXT)
        return 0
    if args.command == "run":
        return run_full_pipeline(args)
    if args.command == "status":
        return run_status(args)
    if args.command == "apply-fix":
        return run_apply_fix(args)
    if args.command in AGENT_MODULES:
        return run_agent_command(args.command, remainder)

    print(HELP_TEXT)
    return 1


if __name__ == "__main__":
    sys.exit(main())
