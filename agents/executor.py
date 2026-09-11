"""Agent 6 — Executor Agent.

Purpose: run dbt models and capture the full error surface in one pass.
See AGENT_DESIGN.md Section 4 (Agent 6) for the full spec.

Note on warehouse sizing: AGENT_DESIGN.md has this agent "select warehouse
size from Analyzer output" per model. This workspace has exactly one SQL
Warehouse (2X-Small, fixed) — confirmed via `databricks warehouses list`
before building this agent — so per-model warehouse routing isn't actionable
here. Analyzer's warehouse_size classification is still written to
model_runs as reference metadata for a real engagement with multiple
warehouses; this agent just runs everything through the one available
warehouse.

Steps:
  1. Run `dbt run --no-fail-fast --threads N` against the migration-workspace
     copy (never the original project — see agents/common/workspace.py)
  2. Parse target/run_results.json + target/manifest.json for per-model
     pass/fail/blocked status (blocked = skipped because an upstream model
     failed, per dbt's own skip message — distinct from a genuine failure)
  3. Write results to the existing dbt_migration.audit.model_runs table
  4. Generate a human-readable Excel report via scripts/dbt_report.py
     (parses the same two artifacts) and save it to dbt-migration-agent/reports/

Failure behavior: never hard-stops the pipeline — a failed model is reported,
not fatal to the run. The failed-model list is returned for Diagnostician
(Agent 7, not yet built) to consume once it exists.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk.errors import DatabricksError

from agents.common.db import StatementError, execute_sql, get_client
from agents.common.workspace import ensure_workspace_copy
from scripts.dbt_report import build_excel, parse_artifacts

REPORTS_ROOT = Path(__file__).resolve().parent.parent / "reports"

STATUS_MAP = {"success": "pass", "error": "fail", "fail": "fail"}  # skipped handled separately


@dataclass
class ModelRunResult:
    model_name: str
    layer: str
    run_status: str  # pass / fail / blocked
    blocked_by_upstream: bool
    execution_time: float
    final_error_message: str
    requires_human_review: bool


@dataclass
class ExecutorReport:
    run_id: str
    dbt_run_ok: bool
    results: list[ModelRunResult]
    report_path: str | None

    def to_dict(self) -> dict:
        return asdict(self)


class ExecutorAgent:
    def __init__(
        self,
        project_path: str,
        profile: str = "free_community",
        catalog: str = "dbt_migration",
        warehouse_id: str = "b05480be6edc2be5",
        dbt_target: str = "dev",
        developer: str = "unknown",
        threads: int = 8,
        reset_workspace: bool = False,
    ):
        self.source_path = Path(project_path).resolve()
        self.project_path = ensure_workspace_copy(self.source_path, reset=reset_workspace)
        self.profile = profile
        self.catalog = catalog
        self.warehouse_id = warehouse_id
        self.dbt_target = dbt_target
        self.developer = developer
        self.threads = threads
        self.client = None

    def run_dbt(self) -> tuple[bool, str]:
        try:
            proc = subprocess.run(
                [
                    "dbt", "run", "--no-fail-fast", "--threads", str(self.threads),
                    "--project-dir", str(self.project_path), "--target", self.dbt_target,
                ],
                capture_output=True, text=True, timeout=1800,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            return False, str(e)
        return proc.returncode == 0, (proc.stdout + proc.stderr)[-6000:]

    def parse_run_results(self) -> list[ModelRunResult]:
        run_results_path = self.project_path / "target" / "run_results.json"
        manifest_path = self.project_path / "target" / "manifest.json"
        if not run_results_path.exists():
            return []

        rr = json.loads(run_results_path.read_text())
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
        nodes = manifest.get("nodes", {})

        model_results = [r for r in rr.get("results", []) if r.get("unique_id", "").startswith("model.")]
        # dbt's run_results.json doesn't populate `message` for skipped nodes (confirmed:
        # always empty) — checking it for "upstream" always returned False, silently
        # defeating this column's whole purpose. Derive it from the real dependency graph
        # instead: a skipped model is blocked-by-upstream if any direct dependency also
        # didn't succeed. This naturally cascades through multi-level chains too, since a
        # dependency that was itself skipped-due-to-its-own-upstream already carries a
        # non-"success" status here.
        status_by_uid = {r["unique_id"]: r.get("status") for r in model_results}

        results = []
        for r in model_results:
            uid = r["unique_id"]
            node = nodes.get(uid, {})
            fqn = node.get("fqn", [])
            layer = fqn[1] if len(fqn) > 1 else "unknown"
            status = r.get("status", "unknown")
            message = (r.get("message") or "").strip()

            if status in ("skipped", "skip"):
                run_status = "blocked"
                deps = node.get("depends_on", {}).get("nodes", [])
                blocked_by_upstream = any(status_by_uid.get(dep) not in ("success", None) for dep in deps)
            else:
                run_status = STATUS_MAP.get(status, "fail")
                blocked_by_upstream = False

            results.append(ModelRunResult(
                model_name=node.get("name", uid.split(".")[-1]),
                layer=layer,
                run_status=run_status,
                blocked_by_upstream=blocked_by_upstream,
                execution_time=round(r.get("execution_time", 0) or 0, 2),
                final_error_message=message[:2000],
                requires_human_review=(run_status == "fail"),
            ))
        return results

    def generate_report(self, run_id: str) -> Path | None:
        run_results_path = self.project_path / "target" / "run_results.json"
        if not run_results_path.exists():
            return None
        rows, summary = parse_artifacts(self.project_path)
        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        report_path = REPORTS_ROOT / f"{run_id}.xlsx"
        build_excel(rows, summary, report_path)
        return report_path

    def write_audit(self, results: list[ModelRunResult], run_id: str) -> None:
        if not results:
            return
        self.client = self.client or get_client(self.profile)
        now = datetime.now(timezone.utc).isoformat()

        def esc(s: str) -> str:
            return s.replace("'", "''")

        rows_sql = []
        for r in results:
            rows_sql.append("(" + ", ".join([
                f"'{esc(r.model_name)}'", f"'{esc(r.layer)}'", f"'{esc(r.run_status)}'",
                str(r.blocked_by_upstream).upper(), f"'{esc(r.final_error_message)}'",
                str(r.requires_human_review).upper(), f"'{esc(self.developer)}'",
                f"'{esc(run_id)}'", f"TIMESTAMP'{now}'",
            ]) + ")")
        stmt = (
            f"INSERT INTO {self.catalog}.audit.model_runs "
            "(model_name, layer, run_status, blocked_by_upstream, final_error_message, "
            "requires_human_review, developer, run_id, run_timestamp) VALUES "
            + ", ".join(rows_sql)
        )
        execute_sql(self.client, self.warehouse_id, stmt, catalog=self.catalog, schema="audit")

    def run(self) -> ExecutorReport:
        run_id = str(uuid.uuid4())
        dbt_run_ok, _log = self.run_dbt()
        results = self.parse_run_results()

        try:
            self.write_audit(results, run_id)
        except (StatementError, DatabricksError) as e:
            print(f"[warn] could not write to audit table: {e}", file=sys.stderr)

        report_path = self.generate_report(run_id)

        return ExecutorReport(
            run_id=run_id, dbt_run_ok=dbt_run_ok, results=results,
            report_path=str(report_path) if report_path else None,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Executor Agent")
    parser.add_argument("project_path")
    parser.add_argument("--profile", default="free_community")
    parser.add_argument("--catalog", default="dbt_migration")
    parser.add_argument("--warehouse-id", default="b05480be6edc2be5")
    parser.add_argument("--dbt-target", default="dev")
    parser.add_argument("--developer", default="unknown")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--reset-workspace", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    agent = ExecutorAgent(
        project_path=args.project_path, profile=args.profile, catalog=args.catalog,
        warehouse_id=args.warehouse_id, dbt_target=args.dbt_target, developer=args.developer,
        threads=args.threads, reset_workspace=args.reset_workspace,
    )
    report = agent.run()

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        print("Executor Agent")
        print(f"workspace copy: {agent.project_path}")
        print("=" * 60)
        for r in report.results:
            flag = " [REVIEW]" if r.requires_human_review else ""
            print(f"  [{r.run_status}] {r.layer}.{r.model_name} ({r.execution_time}s){flag}"
                  f"{' — ' + r.final_error_message if r.final_error_message else ''}")
        counts: dict[str, int] = {}
        for r in report.results:
            counts[r.run_status] = counts.get(r.run_status, 0) + 1
        print(f"\n{counts}")
        print(f"dbt run exit ok: {report.dbt_run_ok}")
        if report.report_path:
            print(f"Excel report: {report.report_path}")
        print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
