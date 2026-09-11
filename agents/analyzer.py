"""Agent 4 — Analyzer Agent.

Purpose: understand the full scope of migration before touching any SQL.
See AGENT_DESIGN.md Section 4 (Agent 4) for the full spec.

The "19 Snowflake-specific patterns" and exact Easy/Medium/Complex thresholds
are not enumerated in AGENT_DESIGN.md — both were drafted from the dialect
differences already documented in FINDINGS.md/MACRO_ANALYSIS.md and confirmed
with the user (2026-09-10). Extend PATTERNS below as new issues are found.

Steps:
  1. Run `dbt parse` to get an authoritative manifest.json (DAG, raw SQL,
     macro/model dependencies) instead of re-implementing ref() resolution.
  2. Scan each model's raw SQL for the 19 patterns.
  3. Cross-reference macros each model calls against Macro Resolver's own
     classify_macro_body() (auto_resolve/flag/hard_stop).
  4. Classify Easy/Medium/Complex, map to a warehouse size.
  5. Write results into the existing dbt_migration.audit.model_runs table
     (already has complexity/warehouse_size/warehouse_source columns —
     no new audit table needed).

Output: classification + DAG + warehouse map, printed and returned as JSON
for the next agent (Transpiler) to consume as a task value.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from agents.common.db import StatementError, execute_sql, get_client
from agents.common.workspace import ensure_workspace_copy
from agents.macro_resolver import classify_macro_body, parse_macro_blocks

# ---------------------------------------------------------------------------
# The 19 Snowflake-specific patterns (drafted from FINDINGS.md Section 4 /
# MACRO_ANALYSIS.md Section 5, confirmed with user as a starting set).
# ---------------------------------------------------------------------------

PATTERNS: list[tuple[str, re.Pattern, str]] = [
    ("type_cast", re.compile(r"::\s*(varchar|number|integer|timestamp_ntz|date|timestamp)\b", re.IGNORECASE), "hard_stop_equivalent"),
    ("sysdate", re.compile(r"\bsysdate\s*\(", re.IGNORECASE), "syntax"),
    ("iff", re.compile(r"\biff\s*\(", re.IGNORECASE), "syntax"),
    ("decode", re.compile(r"\bdecode\s*\(", re.IGNORECASE), "syntax"),
    ("last_day_part", re.compile(r"\blast_day\s*\([^)]*,\s*['\"](month|week|year)['\"]", re.IGNORECASE), "syntax"),
    ("current_warehouse", re.compile(r"\bCURRENT_WAREHOUSE\s*\(", re.IGNORECASE), "syntax"),
    ("current_role", re.compile(r"\bCURRENT_ROLE\s*\(", re.IGNORECASE), "syntax"),
    ("sample_row", re.compile(r"\bSAMPLE\s+ROW\b", re.IGNORECASE), "syntax"),
    ("array_literal", re.compile(r"\[\s*['\"][^\]]*['\"]\s*(,\s*['\"][^\]]*['\"]\s*)*\]"), "syntax"),
    ("generator_seq", re.compile(r"\bGENERATOR\s*\(|\bseq4\s*\(", re.IGNORECASE), "syntax"),
    ("alter_session", re.compile(r"\bALTER\s+SESSION\b", re.IGNORECASE), "architectural"),
    ("use_warehouse", re.compile(r"\bUSE\s+WAREHOUSE\b", re.IGNORECASE), "architectural"),
    ("dynamic_table", re.compile(r"materialized\s*=\s*['\"]dynamic_table['\"]|dynamic_table", re.IGNORECASE), "hard_stop"),
    ("stream_metadata", re.compile(r"metadata\$\w+", re.IGNORECASE), "hard_stop"),
    ("show_streams", re.compile(r"\bSHOW\s+STREAMS\b", re.IGNORECASE), "hard_stop"),
    ("qualify", re.compile(r"\bQUALIFY\b", re.IGNORECASE), "syntax"),
    ("lateral_flatten", re.compile(r"\bLATERAL\s+FLATTEN\b|\bFLATTEN\s*\(", re.IGNORECASE), "syntax"),
    ("pivot", re.compile(r"\bPIVOT\s*\(", re.IGNORECASE), "syntax"),
    ("variant_type", re.compile(r"\bVARIANT\b", re.IGNORECASE), "syntax"),
    ("ilike", re.compile(r"\bILIKE\b", re.IGNORECASE), "syntax"),
    ("sequence", re.compile(r"\bCREATE\s+(OR\s+REPLACE\s+)?SEQUENCE\b|\.nextval\b", re.IGNORECASE), "hard_stop"),
    ("execute_immediate", re.compile(r"\bEXECUTE\s+IMMEDIATE\b", re.IGNORECASE), "architectural"),
    ("iso_week_extract", re.compile(r"extract\s*\(\s*(dayofweekiso|weekiso|yearofweekiso)", re.IGNORECASE), "syntax"),
]

_COMMENT_RE = re.compile(
    r"\{\#.*?\#\}"       # Jinja comment {# ... #}
    r"|/\*.*?\*/"        # SQL block comment
    r"|--[^\n]*",        # SQL line comment
    re.DOTALL,
)


def strip_sql_comments(text: str) -> str:
    """Comments often *describe* a Snowflake construct that was already removed
    (e.g. "decode() replaced with CASE WHEN") — matching on raw text without
    stripping comments first flags already-fixed models as still needing work.
    """
    return _COMMENT_RE.sub(" ", text)


def _has_sql_array_literal(text: str, pattern: re.Pattern) -> bool:
    """`['a','b']` is a Snowflake SQL array literal needing ARRAY(...) conversion —
    but the identical shape also shows up as a Jinja kwarg value (tags=['x','y'],
    accepted_values, etc.), which needs no fix (see FINDINGS.md Section 4.5 gotcha).
    Skip matches immediately preceded — past any whitespace — by `=`.
    """
    for m in pattern.finditer(text):
        prefix = text[:m.start()].rstrip()
        if prefix.endswith("="):
            continue
        return True
    return False


HARD_STOP_CATEGORIES = {"hard_stop"}

WAREHOUSE_MAP = {"easy": "XS", "medium": "S", "complex": "L"}


# ---------------------------------------------------------------------------
# manifest.json loading
# ---------------------------------------------------------------------------

def run_dbt_parse(project_path: Path, dbt_target: str) -> Path:
    subprocess.run(
        ["dbt", "parse", "--project-dir", str(project_path), "--target", dbt_target],
        capture_output=True, text=True, timeout=180, check=True,
    )
    manifest_path = project_path / "target" / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"dbt parse did not produce {manifest_path}")
    return manifest_path


def load_model_nodes(manifest_path: Path) -> dict[str, dict]:
    manifest = json.loads(manifest_path.read_text())
    return {k: v for k, v in manifest["nodes"].items() if v["resource_type"] == "model"}


# ---------------------------------------------------------------------------
# Macro call classification — reuses Macro Resolver's own classifier so the
# two agents never disagree about what "hard_stop"/"flag" means for a macro.
# ---------------------------------------------------------------------------

def classify_called_macros(macro_unique_ids: list[str], macros_dir: Path) -> str:
    """Returns the most severe category among the macros a model calls:
    hard_stop > flag > auto_resolve > none (no macros called)."""
    if not macros_dir.exists() or not macro_unique_ids:
        return "none"

    macro_names = set()
    for uid in macro_unique_ids:
        parts = uid.split(".")
        if parts[0] == "macro":
            macro_names.add(parts[-1])

    severity_order = {"none": 0, "auto_resolve": 1, "flag": 2, "hard_stop": 3}
    worst = "none"
    for sql_file in macros_dir.glob("*.sql"):
        text = sql_file.read_text()
        for block in parse_macro_blocks(text):
            lookup_name = block.name
            if lookup_name.startswith("default__"):
                lookup_name = lookup_name[len("default__"):]
            if lookup_name not in macro_names:
                continue
            category, _ = classify_macro_body(text[block.body_start:block.body_end])
            simplified = "hard_stop" if category == "hard_stop" else (
                "flag" if category in ("flag_sideeffect", "flag_inline") else "auto_resolve"
            )
            if severity_order[simplified] > severity_order[worst]:
                worst = simplified
    return worst


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

@dataclass
class ModelClassification:
    model_name: str
    layer: str
    language: str
    materialized: str
    is_incremental: bool
    matched_patterns: list[str]
    macro_severity: str
    complexity: str
    warehouse_size: str
    warehouse_source: str
    depends_on_models: list[str]


def classify_model(node: dict, macros_dir: Path) -> ModelClassification:
    layer = node["fqn"][1] if len(node["fqn"]) > 1 else "other"
    language = node.get("language", "sql")
    materialized = node.get("config", {}).get("materialized", "view")
    is_incremental = materialized == "incremental"

    raw_code = strip_sql_comments(node.get("raw_code") or "")
    matched = []
    if language == "sql":
        for pname, pattern, _ in PATTERNS:
            if pname == "array_literal":
                if _has_sql_array_literal(raw_code, pattern):
                    matched.append(pname)
            elif pattern.search(raw_code):
                matched.append(pname)

    macro_severity = classify_called_macros(node["depends_on"].get("macros", []), macros_dir)

    hard_stop_pattern_hit = any(
        pname in matched and cat == "hard_stop" for pname, _, cat in PATTERNS
    )

    pattern_count = len(matched)
    if language == "python":
        # Python dbt models need all-purpose cluster compute — always complex
        # (see FINDINGS.md: async_bulk_operations / customer_clustering).
        complexity = "complex"
    elif hard_stop_pattern_hit or macro_severity == "hard_stop":
        complexity = "complex"
    elif pattern_count > 3:
        complexity = "complex"
    elif pattern_count >= 1 or macro_severity == "flag" or is_incremental:
        complexity = "medium"
    else:
        complexity = "easy"

    depends_on_models = [
        uid.split(".")[-1] for uid in node["depends_on"].get("nodes", [])
        if uid.startswith("model.")
    ]

    return ModelClassification(
        model_name=node["name"], layer=layer, language=language, materialized=materialized,
        is_incremental=is_incremental, matched_patterns=matched, macro_severity=macro_severity,
        complexity=complexity, warehouse_size=WAREHOUSE_MAP[complexity], warehouse_source="analyzer",
        depends_on_models=depends_on_models,
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

@dataclass
class AnalyzerReport:
    run_id: str
    total_models: int
    complexity_counts: dict[str, int]
    classifications: list[ModelClassification]
    dag_edges: list[tuple[str, str]]

    def to_dict(self) -> dict:
        return asdict(self)


class AnalyzerAgent:
    def __init__(
        self,
        project_path: str,
        profile: str = "free_community",
        catalog: str = "dbt_migration",
        warehouse_id: str = "b05480be6edc2be5",
        dbt_target: str = "dev",
        developer: str = "unknown",
        reset_workspace: bool = False,
    ):
        self.source_path = Path(project_path).resolve()
        self.project_path = ensure_workspace_copy(self.source_path, reset=reset_workspace)
        self.profile = profile
        self.catalog = catalog
        self.warehouse_id = warehouse_id
        self.dbt_target = dbt_target
        self.developer = developer
        self.client: WorkspaceClient | None = None

    def write_audit(self, classifications: list[ModelClassification], run_id: str) -> None:
        if not classifications:
            return
        self.client = self.client or get_client(self.profile)
        now = datetime.now(timezone.utc).isoformat()

        def esc(s: str) -> str:
            return s.replace("'", "''")

        rows_sql = []
        for c in classifications:
            rows_sql.append("(" + ", ".join([
                f"'{esc(c.model_name)}'", f"'{esc(c.layer)}'", f"'{esc(c.complexity)}'",
                f"'{esc(c.warehouse_size)}'", f"'{esc(c.warehouse_source)}'",
                f"'{esc(self.developer)}'", f"'{esc(run_id)}'", f"TIMESTAMP'{now}'",
            ]) + ")")
        stmt = (
            f"INSERT INTO {self.catalog}.audit.model_runs "
            "(model_name, layer, complexity, warehouse_size, warehouse_source, developer, run_id, run_timestamp) "
            "VALUES " + ", ".join(rows_sql)
        )
        execute_sql(self.client, self.warehouse_id, stmt, catalog=self.catalog, schema="audit")

    def run(self) -> AnalyzerReport:
        run_id = str(uuid.uuid4())
        manifest_path = run_dbt_parse(self.project_path, self.dbt_target)
        model_nodes = load_model_nodes(manifest_path)
        macros_dir = self.project_path / "macros"

        classifications = [classify_model(node, macros_dir) for node in model_nodes.values()]
        classifications.sort(key=lambda c: (c.layer, c.model_name))

        dag_edges = []
        for c in classifications:
            for dep in c.depends_on_models:
                dag_edges.append((dep, c.model_name))

        counts = {"easy": 0, "medium": 0, "complex": 0}
        for c in classifications:
            counts[c.complexity] += 1

        try:
            self.write_audit(classifications, run_id)
        except (StatementError, DatabricksError) as e:
            print(f"[warn] could not write to audit table: {e}", file=sys.stderr)

        return AnalyzerReport(
            run_id=run_id, total_models=len(classifications), complexity_counts=counts,
            classifications=classifications, dag_edges=dag_edges,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyzer Agent")
    parser.add_argument("project_path")
    parser.add_argument("--profile", default="free_community")
    parser.add_argument("--catalog", default="dbt_migration")
    parser.add_argument("--warehouse-id", default="b05480be6edc2be5")
    parser.add_argument("--dbt-target", default="dev")
    parser.add_argument("--developer", default="unknown")
    parser.add_argument("--reset-workspace", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    agent = AnalyzerAgent(
        project_path=args.project_path, profile=args.profile, catalog=args.catalog,
        warehouse_id=args.warehouse_id, dbt_target=args.dbt_target, developer=args.developer,
        reset_workspace=args.reset_workspace,
    )
    report = agent.run()

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        print("Analyzer Agent")
        print(f"workspace copy: {agent.project_path}")
        print("=" * 60)
        print(f"run_id: {report.run_id}")
        print(f"total models: {report.total_models}")
        print(f"complexity: {report.complexity_counts}")
        print()
        for c in report.classifications:
            patterns = ",".join(c.matched_patterns) or "none"
            print(f"  [{c.complexity}/{c.warehouse_size}] {c.layer}.{c.model_name} "
                  f"({c.language}, {c.materialized}) patterns=[{patterns}] macro_severity={c.macro_severity}")
        print(f"\nDAG edges: {len(report.dag_edges)}")
        print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
