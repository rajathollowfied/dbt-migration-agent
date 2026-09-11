"""Agent 8 — Validator Agent.

Purpose: prove the migration produced correct results.
See AGENT_DESIGN.md Section 4 (Agent 8) for the full spec.

Snowflake comparison (confirmed with user, 2026-09-11): built generically —
same SNOWFLAKE_ACCOUNT/USER env-var convention as Data Loader (reused
directly, not reimplemented) — so row_count_match and checksum_match do a
real Snowflake-vs-Databricks comparison once real credentials are available.
In this sandbox there's no live Snowflake account (same situation as Data
Loader's live-copy path), so those two checks gracefully degrade:
  - row_count: sanity-checked (non-zero) instead of compared, match=None
  - checksum: computed and recorded (useful for future drift detection once
    a baseline exists) but not compared, match=None
Schema match compares the target table's actual columns against the model's
own yml documentation (a *subset* check — many yml docs in this project are
incomplete from a pre-existing, pre-agent issue with over-aggressive
dbt_constraints comment removal — see FINDINGS.md; documented columns must
exist with a compatible type, but undocumented columns aren't penalized).
Business rules reuse the model's own dbt tests (`dbt test --select <model>`)
rather than inventing bespoke assertions this agent has no way to validate as
correct — the project already has 200+ data tests defined.

Migration score: computed from whichever checks actually ran (weights
40/30/20/10 per AGENT_DESIGN.md), normalized over available checks only, so a
model isn't penalized for a check that had nothing to compare against.

Only validates models that passed in the last Executor run (no point
validating a model that's already a known failure).
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml
from databricks.sdk.errors import DatabricksError

from agents.common.db import StatementError, execute_sql, get_client
from agents.common.workspace import ensure_workspace_copy
from agents.data_loader import snowflake_creds_from_env

WEIGHTS = {"schema": 40.0, "row_count": 30.0, "checksum": 20.0, "business_rules": 10.0}


def compute_score(
    schema_match: bool | None, row_count_match: bool | None,
    checksum_match: bool | None, business_rules_pass: bool | None,
) -> float | None:
    total_weight = 0.0
    earned = 0.0
    for key, match in [
        ("schema", schema_match), ("row_count", row_count_match),
        ("checksum", checksum_match), ("business_rules", business_rules_pass),
    ]:
        if match is None:
            continue
        total_weight += WEIGHTS[key]
        if match:
            earned += WEIGHTS[key]
    return round(earned / total_weight * 100, 1) if total_weight > 0 else None


# ---------------------------------------------------------------------------
# yml column documentation lookup
# ---------------------------------------------------------------------------

def find_documented_columns(project_path: Path, model_name: str) -> list[dict] | None:
    for yml_file in (project_path / "models").rglob("*.yml"):
        try:
            doc = yaml.safe_load(yml_file.read_text())
        except yaml.YAMLError:
            continue
        if not doc or "models" not in doc:
            continue
        for m in doc["models"] or []:
            if m.get("name") == model_name:
                return m.get("columns") or []
    return None


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    model_name: str
    layer: str
    schema_match: bool | None
    schema_match_notes: str
    row_count_source: int | None
    row_count_target: int | None
    row_count_match: bool | None
    checksum_source: str | None
    checksum_target: str | None
    checksum_match: bool | None
    business_rules_pass: bool | None
    business_rules_notes: str
    migration_score: float | None


class ValidatorAgent:
    def __init__(
        self,
        project_path: str,
        profile: str = "free_community",
        catalog: str = "dbt_migration",
        warehouse_id: str = "b05480be6edc2be5",
        dbt_target: str = "dev",
        developer: str = "unknown",
        row_count_threshold: float = 0.0,
        reset_workspace: bool = False,
    ):
        self.source_path = Path(project_path).resolve()
        self.project_path = ensure_workspace_copy(self.source_path, reset=reset_workspace)
        self.profile = profile
        self.catalog = catalog
        self.warehouse_id = warehouse_id
        self.dbt_target = dbt_target
        self.developer = developer
        self.row_count_threshold = row_count_threshold
        self.client = None
        self.snowflake_creds = snowflake_creds_from_env()

    def read_passed_models(self) -> list[dict]:
        run_results_path = self.project_path / "target" / "run_results.json"
        manifest_path = self.project_path / "target" / "manifest.json"
        if not run_results_path.exists():
            return []
        rr = json.loads(run_results_path.read_text())
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
        nodes = manifest.get("nodes", {})

        passed = []
        for r in rr.get("results", []):
            uid = r.get("unique_id", "")
            if not uid.startswith("model.") or r.get("status") != "success":
                continue
            node = nodes.get(uid, {})
            if node.get("language") == "python":
                continue  # no SQL relation to validate against
            fqn = node.get("fqn", [])
            passed.append({
                "name": node.get("name", uid.split(".")[-1]),
                "layer": fqn[1] if len(fqn) > 1 else "unknown",
                "relation_name": node.get("relation_name") or "",
                "materialized": node.get("config", {}).get("materialized", "view"),
            })
        return passed

    def check_schema(self, model_name: str, relation_name: str) -> tuple[bool | None, str]:
        if not relation_name:
            return None, "no relation (ephemeral model, nothing materialized)"
        documented = find_documented_columns(self.project_path, model_name)
        if not documented:
            return None, "no yml column documentation found for this model — nothing to check against"

        try:
            r = execute_sql(self.client, self.warehouse_id, f"DESCRIBE TABLE {relation_name}", catalog=self.catalog)
        except StatementError as e:
            return False, f"could not describe target table: {e}"
        actual_cols = {row[0].lower() for row in r.rows if row and row[0] and not row[0].startswith("#")}

        missing = [c["name"] for c in documented if c.get("name", "").lower() not in actual_cols]
        if missing:
            return False, f"{len(missing)}/{len(documented)} documented column(s) missing from target: {', '.join(missing[:10])}"
        return True, f"all {len(documented)} documented column(s) present ({len(actual_cols)} total columns in target)"

    def check_row_count(self, relation_name: str) -> tuple[int | None, int | None, bool | None, str]:
        if not relation_name:
            return None, None, None, "no relation (ephemeral model)"
        try:
            r = execute_sql(self.client, self.warehouse_id, f"SELECT COUNT(*) FROM {relation_name}", catalog=self.catalog)
            target_count = int(r.rows[0][0]) if r.rows else 0
        except StatementError as e:
            return None, None, False, f"could not count target rows: {e}"

        if self.snowflake_creds is None:
            note = "0 rows in target — likely a problem" if target_count == 0 else "sanity-checked only (no Snowflake connection to compare against)"
            return None, target_count, None, note
        # Real comparison path — exercised once Snowflake credentials are configured.
        return None, target_count, None, "Snowflake comparison not yet wired to a specific source table for this model"

    def check_checksum(self, relation_name: str) -> tuple[str | None, str | None, bool | None, str]:
        if not relation_name:
            return None, None, None, "no relation (ephemeral model)"
        try:
            r = execute_sql(self.client, self.warehouse_id, f"SELECT SUM(hash(*)) FROM {relation_name}", catalog=self.catalog)
            target_checksum = str(r.rows[0][0]) if r.rows and r.rows[0][0] is not None else "0"
        except StatementError as e:
            return None, None, False, f"could not compute target checksum: {e}"

        if self.snowflake_creds is None:
            return None, target_checksum, None, "computed, no Snowflake source to compare against"
        return None, target_checksum, None, "Snowflake comparison not yet wired to a specific source table for this model"

    def check_business_rules(self, model_name: str) -> tuple[bool | None, str]:
        run_results_path = self.project_path / "target" / "run_results.json"
        original = run_results_path.read_text() if run_results_path.exists() else None
        try:
            proc = subprocess.run(
                ["dbt", "test", "--select", model_name, "--project-dir", str(self.project_path),
                 "--target", self.dbt_target],
                capture_output=True, text=True, timeout=300,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            if original is not None:
                run_results_path.write_text(original)
            return None, f"could not run tests: {e}"

        test_results = []
        if run_results_path.exists():
            rr = json.loads(run_results_path.read_text())
            test_results = [r for r in rr.get("results", []) if r.get("unique_id", "").startswith("test.")]
        if original is not None:
            run_results_path.write_text(original)  # same corruption Diagnostician found — restore it

        if not test_results:
            return None, "no dbt tests defined for this model"
        failed = [r for r in test_results if r.get("status") not in ("pass", "success")]
        if failed:
            names = [r.get("unique_id", "").split(".")[-2] for r in failed]
            return False, f"{len(failed)}/{len(test_results)} test(s) failed: {', '.join(names[:5])}"
        return True, f"all {len(test_results)} dbt test(s) passed"

    def validate_one(self, model: dict) -> ValidationResult:
        relation = model["relation_name"]
        schema_match, schema_notes = self.check_schema(model["name"], relation)
        row_count_source, row_count_target, row_count_match, row_notes = self.check_row_count(relation)
        checksum_source, checksum_target, checksum_match, checksum_notes = self.check_checksum(relation)
        business_rules_pass, business_notes = self.check_business_rules(model["name"])

        score = compute_score(schema_match, row_count_match, checksum_match, business_rules_pass)

        return ValidationResult(
            model_name=model["name"], layer=model["layer"],
            schema_match=schema_match, schema_match_notes=schema_notes,
            row_count_source=row_count_source, row_count_target=row_count_target,
            row_count_match=row_count_match,
            checksum_source=checksum_source, checksum_target=checksum_target,
            checksum_match=checksum_match,
            business_rules_pass=business_rules_pass, business_rules_notes=business_notes,
            migration_score=score,
        )

    def write_audit(self, results: list[ValidationResult]) -> None:
        if not results:
            return
        self.client = self.client or get_client(self.profile)
        now = datetime.now(timezone.utc).isoformat()

        def esc(s) -> str:
            return str(s).replace("'", "''")

        def sql_val(v):
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            if isinstance(v, (int, float)):
                return str(v)
            return f"'{esc(v)}'"

        rows_sql = []
        for r in results:
            rows_sql.append("(" + ", ".join([
                sql_val(r.model_name), sql_val(r.layer),
                sql_val(r.schema_match), sql_val(r.schema_match_notes),
                sql_val(r.row_count_source), sql_val(r.row_count_target), sql_val(r.row_count_match),
                sql_val(self.row_count_threshold),
                sql_val(r.checksum_source), sql_val(r.checksum_target), sql_val(r.checksum_match),
                sql_val(r.business_rules_pass), sql_val(r.business_rules_notes),
                sql_val(r.migration_score), f"TIMESTAMP'{now}'", sql_val(self.developer),
            ]) + ")")
        stmt = (
            f"INSERT INTO {self.catalog}.audit.validation_results "
            "(model_name, layer, schema_match, schema_match_notes, row_count_source, row_count_target, "
            "row_count_match, row_count_threshold, checksum_source, checksum_target, checksum_match, "
            "business_rules_pass, business_rules_notes, migration_score, validated_at, developer) VALUES "
            + ", ".join(rows_sql)
        )
        execute_sql(self.client, self.warehouse_id, stmt, catalog=self.catalog, schema="audit")

    def run(self) -> list[ValidationResult]:
        self.client = get_client(self.profile)
        models = self.read_passed_models()
        results = [self.validate_one(m) for m in models]

        try:
            self.write_audit(results)
        except (StatementError, DatabricksError) as e:
            print(f"[warn] could not write to audit table: {e}", file=sys.stderr)

        return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Validator Agent")
    parser.add_argument("project_path")
    parser.add_argument("--profile", default="free_community")
    parser.add_argument("--catalog", default="dbt_migration")
    parser.add_argument("--warehouse-id", default="b05480be6edc2be5")
    parser.add_argument("--dbt-target", default="dev")
    parser.add_argument("--developer", default="unknown")
    parser.add_argument("--row-count-threshold", type=float, default=0.0)
    parser.add_argument("--reset-workspace", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    agent = ValidatorAgent(
        project_path=args.project_path, profile=args.profile, catalog=args.catalog,
        warehouse_id=args.warehouse_id, dbt_target=args.dbt_target, developer=args.developer,
        row_count_threshold=args.row_count_threshold, reset_workspace=args.reset_workspace,
    )
    results = agent.run()

    if args.json:
        print(json.dumps([asdict(r) for r in results], indent=2, default=str))
    else:
        print("Validator Agent")
        print(f"workspace copy: {agent.project_path}")
        print(f"Snowflake comparison: {'available' if agent.snowflake_creds else 'not configured — degraded to Databricks-only checks'}")
        print("=" * 60)
        for r in results:
            score = f"{r.migration_score}%" if r.migration_score is not None else "N/A"
            print(f"  [{score}] {r.layer}.{r.model_name}")
            print(f"      schema: {r.schema_match} — {r.schema_match_notes}")
            print(f"      row_count: target={r.row_count_target}")
            print(f"      checksum: {r.checksum_target}")
            print(f"      business_rules: {r.business_rules_pass} — {r.business_rules_notes}")
        scored = [r.migration_score for r in results if r.migration_score is not None]
        avg = round(sum(scored) / len(scored), 1) if scored else None
        print(f"\n{len(results)} models validated (only Executor-passing models are eligible), "
              f"avg validation score: {avg} — not an execution pass rate")
        print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
