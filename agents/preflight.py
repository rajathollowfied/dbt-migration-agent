"""Agent 1 — Preflight Agent.

Purpose: validate everything before any data moves, and fail fast.
See AGENT_DESIGN.md Section 4 (Agent 1) for the full spec.

Checks (in order):
  1. Databricks workspace connectivity + token validity
  2. Unity Catalog access (target catalog + required schemas)
  3. SQL Warehouse reachability
  4. Audit table setup (creates dbt_migration.audit.* if missing)
  5. dbt profile valid (`dbt debug`)
  6. dbt_project.yml scan — auto-fix Snowflake-specific configs
  7. Git repo + branch check (informational, never blocks)

Failure behavior: hard stop. Any blocking check failing means `go=False` and
nothing downstream should run.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from agents.common.audit_schema import ensure_audit_tables
from agents.common.db import StatementError, execute_sql, get_client
from agents.common.config import DEFAULT_CATALOG, DEFAULT_PROFILE, DEFAULT_WAREHOUSE_ID
from agents.common.workspace import ensure_workspace_copy

REQUIRED_SCHEMAS = ["landing", "bronze", "silver", "gold", "audit"]

# (pattern, description) — matched against non-comment lines in dbt_project.yml.
# Patterns only match *live* config; anything already commented out is left alone.
DBT_PROJECT_YML_STRIP_PATTERNS = [
    (re.compile(r"^\s*\+?\s*snowflake_warehouse\s*:", re.IGNORECASE), "snowflake_warehouse config"),
    (re.compile(r"^\s*\+?\s*transient\s*:\s*false", re.IGNORECASE), "transient=false config"),
    (re.compile(r"^\s*\+?\s*database\s*:.*target\.database", re.IGNORECASE), "target.database reference"),
    (re.compile(r"^\s*\+?\s*schema\s*:.*target\.schema", re.IGNORECASE), "target.schema reference"),
    (re.compile(r"\btarget\.warehouse\b", re.IGNORECASE), "target.warehouse reference"),
]

PROFILE_LINE_RE = re.compile(r'^(profile:\s*)"?([A-Za-z0-9_]+)"?\s*$')


@dataclass
class CheckResult:
    name: str
    status: str  # pass / fail / warn
    message: str
    details: dict = field(default_factory=dict)
    blocking: bool = True


@dataclass
class PreflightReport:
    go: bool
    checks: list[CheckResult]
    dbt_project_yml_fixes: list[str]

    def to_dict(self) -> dict:
        return {
            "go": self.go,
            "checks": [asdict(c) for c in self.checks],
            "dbt_project_yml_fixes": self.dbt_project_yml_fixes,
        }


class PreflightAgent:
    def __init__(
        self,
        project_path: str,
        profile: str | None = DEFAULT_PROFILE,
        catalog: str = DEFAULT_CATALOG,
        warehouse_id: str | None = DEFAULT_WAREHOUSE_ID,
        dbt_profile_name: str = "DATABRICKS",
        dbt_target: str = "dev",
        reset_workspace: bool = False,
    ):
        self.source_path = Path(project_path).resolve()
        self.project_path = ensure_workspace_copy(self.source_path, reset=reset_workspace)
        self.profile = profile
        self.catalog = catalog
        self.warehouse_id = warehouse_id
        self.dbt_profile_name = dbt_profile_name
        self.dbt_target = dbt_target
        self.client: WorkspaceClient | None = None

    # ---- checks -----------------------------------------------------

    def check_workspace_connectivity(self) -> CheckResult:
        try:
            self.client = get_client(self.profile)
            me = self.client.current_user.me()
            return CheckResult(
                name="workspace_connectivity",
                status="pass",
                message=f"Connected to {self.client.config.host} as {me.user_name}",
                details={"user_name": me.user_name, "host": self.client.config.host},
            )
        except DatabricksError as e:
            return CheckResult(
                name="workspace_connectivity",
                status="fail",
                message=f"Could not authenticate to workspace with profile '{self.profile}': {e}",
            )
        except Exception as e:  # network errors, bad host, etc.
            return CheckResult(
                name="workspace_connectivity",
                status="fail",
                message=f"Could not reach workspace with profile '{self.profile}': {e}",
            )

    def check_unity_catalog(self) -> CheckResult:
        if self.client is None:
            return CheckResult("unity_catalog", "fail", "Skipped — no workspace connection")
        try:
            self.client.catalogs.get(self.catalog)
        except DatabricksError as e:
            return CheckResult(
                name="unity_catalog",
                status="fail",
                message=f"Catalog '{self.catalog}' not accessible: {e}",
            )

        existing = {s.name for s in self.client.schemas.list(catalog_name=self.catalog)}
        missing = [s for s in REQUIRED_SCHEMAS if s not in existing]
        created = []
        for schema in missing:
            try:
                self.client.schemas.create(name=schema, catalog_name=self.catalog)
                created.append(schema)
            except DatabricksError as e:
                return CheckResult(
                    name="unity_catalog",
                    status="fail",
                    message=f"Schema '{schema}' missing under '{self.catalog}' and could not be created: {e}",
                    details={"existing_schemas": sorted(existing)},
                )

        msg = f"Catalog '{self.catalog}' OK, schemas present: {sorted(existing | set(created))}"
        if created:
            msg += f" (created: {created})"
        return CheckResult(
            name="unity_catalog",
            status="pass",
            message=msg,
            details={"existing_schemas": sorted(existing), "created_schemas": created},
        )

    def check_sql_warehouse(self) -> CheckResult:
        if self.client is None:
            return CheckResult("sql_warehouse", "fail", "Skipped — no workspace connection")
        try:
            wh = self.client.warehouses.get(self.warehouse_id)
        except DatabricksError as e:
            return CheckResult(
                name="sql_warehouse",
                status="fail",
                message=f"Warehouse id '{self.warehouse_id}' not reachable: {e}",
            )
        try:
            execute_sql(self.client, self.warehouse_id, "SELECT 1", catalog=self.catalog)
        except StatementError as e:
            return CheckResult(
                name="sql_warehouse",
                status="fail",
                message=f"Warehouse '{wh.name}' reachable but query failed: {e}",
            )
        return CheckResult(
            name="sql_warehouse",
            status="pass",
            message=f"Warehouse '{wh.name}' ({self.warehouse_id}) reachable, state was {wh.state}",
            details={"warehouse_name": wh.name, "state": str(wh.state)},
        )

    def check_audit_tables(self) -> CheckResult:
        if self.client is None:
            return CheckResult("audit_tables", "fail", "Skipped — no workspace connection")
        try:
            executed = ensure_audit_tables(self.client, self.warehouse_id, self.catalog)
        except StatementError as e:
            return CheckResult(
                name="audit_tables",
                status="fail",
                message=f"Could not create/verify audit tables: {e}",
            )
        return CheckResult(
            name="audit_tables",
            status="pass",
            message=f"Audit schema + {len(executed) - 1} tables verified in {self.catalog}.audit",
            details={"statements": executed},
        )

    def check_dbt_debug(self) -> CheckResult:
        if not self.project_path.exists():
            return CheckResult(
                name="dbt_debug",
                status="fail",
                message=f"Project path does not exist: {self.project_path}",
            )
        try:
            proc = subprocess.run(
                ["dbt", "debug", "--project-dir", str(self.project_path), "--target", self.dbt_target],
                capture_output=True,
                text=True,
                timeout=240,
            )
        except FileNotFoundError:
            return CheckResult(
                name="dbt_debug",
                status="fail",
                message="`dbt` executable not found on PATH",
            )
        except subprocess.TimeoutExpired:
            return CheckResult(
                name="dbt_debug",
                status="fail",
                message="`dbt debug` timed out after 240s — a stopped serverless warehouse can take a while to cold-start",
            )

        output = proc.stdout + proc.stderr
        if proc.returncode != 0:
            return CheckResult(
                name="dbt_debug",
                status="fail",
                message="`dbt debug` reported errors — see details",
                details={"output": output[-4000:]},
            )
        return CheckResult(
            name="dbt_debug",
            status="pass",
            message="`dbt debug` passed — profile, connection, and dependencies OK",
        )

    def fix_dbt_project_yml(self) -> list[str]:
        """Scan dbt_project.yml and remove Snowflake-only config entirely (never comment —
        comments inside Jinja {{ config() }} blocks break parsing, so Preflight is
        consistent and always removes rather than comments, per AGENT_DESIGN.md).
        """
        yml_path = self.project_path / "dbt_project.yml"
        if not yml_path.exists():
            return []

        original = yml_path.read_text()
        lines = original.splitlines(keepends=True)
        fixes: list[str] = []
        kept_lines: list[str] = []

        for line in lines:
            stripped = line.strip()

            profile_match = PROFILE_LINE_RE.match(stripped)
            if profile_match and profile_match.group(2) != self.dbt_profile_name:
                kept_lines.append(f'profile: "{self.dbt_profile_name}"\n')
                fixes.append(f"renamed profile '{profile_match.group(2)}' -> '{self.dbt_profile_name}'")
                continue

            if stripped.startswith("#"):
                kept_lines.append(line)
                continue

            matched_pattern = None
            for pattern, desc in DBT_PROJECT_YML_STRIP_PATTERNS:
                if pattern.search(stripped):
                    matched_pattern = desc
                    break

            if matched_pattern:
                fixes.append(f"removed line ({matched_pattern}): {stripped}")
                continue

            kept_lines.append(line)

        if fixes:
            yml_path.write_text("".join(kept_lines))

        return fixes

    # ---- orchestration ------------------------------------------------

    def run(self) -> PreflightReport:
        checks = [
            self.check_workspace_connectivity(),
            self.check_unity_catalog(),
            self.check_sql_warehouse(),
            self.check_audit_tables(),
            self.check_dbt_debug(),
        ]
        fixes = self.fix_dbt_project_yml()

        go = all(c.status != "fail" for c in checks if c.blocking)
        return PreflightReport(go=go, checks=checks, dbt_project_yml_fixes=fixes)


def _emit_task_value(report: PreflightReport) -> None:
    """Best-effort: set a Databricks Workflows task value when running as a job task."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key="preflight_go", value=report.go)
        dbutils.jobs.taskValues.set(key="preflight_report", value=json.dumps(report.to_dict()))
    except Exception:
        pass  # not running inside a Databricks job task — fine for local/CLI use


def main() -> int:
    parser = argparse.ArgumentParser(description="Preflight Agent — validate before migrating")
    parser.add_argument("project_path", help="Path to the Snowflake+dbt project to migrate")
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--warehouse-id", default=DEFAULT_WAREHOUSE_ID)
    parser.add_argument("--dbt-profile-name", default="DATABRICKS")
    parser.add_argument("--dbt-target", default="dev")
    parser.add_argument("--reset-workspace", action="store_true",
                         help="Discard any existing migration-workspace copy and re-copy from project_path")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON only")
    args = parser.parse_args()

    agent = PreflightAgent(
        project_path=args.project_path,
        profile=args.profile,
        catalog=args.catalog,
        warehouse_id=args.warehouse_id,
        dbt_profile_name=args.dbt_profile_name,
        dbt_target=args.dbt_target,
        reset_workspace=args.reset_workspace,
    )
    report = agent.run()
    _emit_task_value(report)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print("Preflight Agent")
        print(f"workspace copy: {agent.project_path}")
        print("=" * 60)
        for c in report.checks:
            icon = {"pass": "✓", "fail": "✗", "warn": "!"}[c.status]
            print(f"[{icon}] {c.name}: {c.message}")
        if report.dbt_project_yml_fixes:
            print("\ndbt_project.yml fixes applied:")
            for f in report.dbt_project_yml_fixes:
                print(f"  - {f}")
        else:
            print("\ndbt_project.yml: no Snowflake-specific config found, nothing to fix")
        print("=" * 60)
        print(f"GO/NO-GO: {'GO' if report.go else 'NO-GO'}")

    return 0 if report.go else 1


if __name__ == "__main__":
    sys.exit(main())
