"""Agent 3 — Data Loader Agent.

Purpose: copy source data from Snowflake to Databricks Unity Catalog.
See AGENT_DESIGN.md Section 4 (Agent 3) for the full spec.

Every source table declared in `models/**/*.yml` (`sources:` blocks) is
classified into one of three strategies:
  - native_redirect: a Databricks-native dataset already covers this data
    (e.g. Snowflake's built-in SNOWFLAKE_SAMPLE_DATA.TPCH* -> samples.tpch).
    `_sources.yml` is rewritten to point at it; no data movement happens.
  - unused: no model or macro in the project references this source table —
    nothing to load, reported for visibility/cleanup only.
  - copied: connects to Snowflake (credentials from env vars) and copies the
    table into `<catalog>.landing.<source>__<table>`, batching inserts and
    casting VARIANT/OBJECT/ARRAY columns to STRING. If no Snowflake
    credentials are configured, this is soft-skipped and flagged for human
    review rather than failing the run.

Failure behavior: soft fail per table — log failures, continue with the rest.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from agents.common.db import StatementError, execute_sql, get_client
from agents.common.config import DEFAULT_CATALOG, DEFAULT_PROFILE, DEFAULT_WAREHOUSE_ID
from agents.common.workspace import ensure_workspace_copy

KNOWN_TPCH_TABLES = {"nation", "region", "customer", "orders", "lineitem", "part", "partsupp", "supplier"}

DEFAULT_BATCH_SIZE = 50_000


# ---------------------------------------------------------------------------
# Source parsing
# ---------------------------------------------------------------------------

@dataclass
class SourceTable:
    source_name: str
    table_name: str
    database_raw: str
    schema_raw: str
    yml_file: Path


def find_sources_yml_files(project_path: Path) -> list[Path]:
    models_dir = project_path / "models"
    if not models_dir.exists():
        return []
    found = []
    for yml_file in sorted(models_dir.rglob("*.yml")):
        try:
            text = yml_file.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        if re.search(r"^\s*sources\s*:\s*$", text, re.MULTILINE):
            found.append(yml_file)
    return found


def parse_source_tables(yml_file: Path) -> list[SourceTable]:
    try:
        doc = yaml.safe_load(yml_file.read_text())
    except yaml.YAMLError:
        return []
    if not doc or "sources" not in doc:
        return []
    tables = []
    for src in doc["sources"] or []:
        name = src.get("name", "")
        database_raw = str(src.get("database", "") or "")
        schema_raw = str(src.get("schema", "") or "")
        for t in src.get("tables", []) or []:
            tables.append(SourceTable(
                source_name=name, table_name=t.get("name", ""),
                database_raw=database_raw, schema_raw=schema_raw, yml_file=yml_file,
            ))
    return tables


def find_consumers(project_path: Path, source_name: str, table_name: str) -> list[str]:
    pattern = re.compile(
        rf"""source\(\s*['"]{re.escape(source_name)}['"]\s*,\s*['"]{re.escape(table_name)}['"]\s*\)""",
        re.IGNORECASE,
    )
    consumers = []
    for sql_file in list(project_path.rglob("*.sql")):
        if "target" in sql_file.parts or "dbt_packages" in sql_file.parts:
            continue
        try:
            text = sql_file.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        if pattern.search(text):
            consumers.append(str(sql_file.relative_to(project_path)))
    return consumers


# ---------------------------------------------------------------------------
# Native-dataset detection
# ---------------------------------------------------------------------------

def classify_native(
    database_raw: str, schema_raw: str, table_names: list[str], samples_schemas: set[str],
) -> tuple[str, str, str] | None:
    """Returns (catalog, schema, reason) if a Databricks-native dataset covers this source."""
    db = database_raw.strip().strip('"').upper()
    schema = schema_raw.strip().strip('"').upper()

    if db == "SNOWFLAKE_SAMPLE_DATA" and schema.startswith("TPCH"):
        return "samples", "tpch", "Snowflake's built-in TPC-H sample dataset has a direct Databricks equivalent"

    if db.lower() == "samples":
        return "samples", schema_raw.strip().strip('"').lower(), "already pointing at a Databricks native dataset"

    lowered_tables = {t.lower() for t in table_names}
    if lowered_tables and lowered_tables.issubset(KNOWN_TPCH_TABLES) and "tpch" in samples_schemas:
        return "samples", "tpch", "table names match the standard TPC-H schema"

    return None


# ---------------------------------------------------------------------------
# _sources.yml redirect writer (block-aware, only touches the matched source's
# database:/schema: lines — same approach as Macro Resolver's yml surgery)
# ---------------------------------------------------------------------------

def redirect_source_in_yml(text: str, source_name: str, new_database: str, new_schema: str) -> tuple[str, bool]:
    lines = text.splitlines(keepends=True)
    n = len(lines)
    name_re = re.compile(r'^(\s*)-\s*name:\s*["\']?' + re.escape(source_name) + r'["\']?\s*$')

    i = 0
    block_start = block_indent = None
    while i < n:
        m = name_re.match(lines[i].rstrip("\n"))
        if m:
            block_start = i
            block_indent = len(m.group(1))
            break
        i += 1
    if block_start is None:
        return text, False

    block_end = n
    j = block_start + 1
    while j < n:
        stripped = lines[j].strip()
        if stripped and (len(lines[j]) - len(lines[j].lstrip(" "))) <= block_indent:
            block_end = j
            break
        j += 1

    # `database:`/`schema:` can also appear nested (e.g. under `quoting:`) — only touch
    # the ones at the source block's own direct-child indent, anchored on `tables:`
    # which is always present as a direct child.
    direct_child_indent = None
    for idx in range(block_start + 1, block_end):
        m = re.match(r"^(\s*)tables\s*:\s*$", lines[idx].rstrip("\n"))
        if m:
            direct_child_indent = len(m.group(1))
            break
    if direct_child_indent is None:
        return text, False

    changed = False
    out = list(lines)
    k = block_start
    while k < block_end:
        line = out[k]
        stripped = line.strip()
        this_indent = len(line) - len(line.lstrip(" "))
        m_db = re.match(r"^(\s*)database\s*:(.*)$", line.rstrip("\n")) if this_indent == direct_child_indent else None
        m_schema = re.match(r"^(\s*)schema\s*:(.*)$", line.rstrip("\n")) if this_indent == direct_child_indent else None
        if m_db and not stripped.startswith("#"):
            indent = m_db.group(1)
            end = k + 1
            if m_db.group(2).strip() in ("|", "|-", ">", ">-", "") or m_db.group(2).strip() == "":
                while end < block_end and (len(out[end]) - len(out[end].lstrip(" "))) > len(indent) and out[end].strip():
                    end += 1
            out[k:end] = [f'{indent}database: "{new_database}"\n']
            block_end -= (end - k) - 1
            changed = True
            k += 1
            continue
        if m_schema and not stripped.startswith("#"):
            indent = m_schema.group(1)
            out[k] = f'{indent}schema: "{new_schema}"\n'
            changed = True
        k += 1

    return "".join(out), changed


# ---------------------------------------------------------------------------
# Snowflake connector (real copy path — untestable without live credentials,
# but built generically for any client engagement that has them)
# ---------------------------------------------------------------------------

SNOWFLAKE_TYPE_MAP = {
    0: "DECIMAL", 1: "DOUBLE", 2: "STRING", 3: "DATE", 4: "TIMESTAMP",
    5: "STRING", 6: "TIMESTAMP", 7: "TIMESTAMP", 8: "TIMESTAMP", 9: "STRING",
    10: "STRING", 11: "BINARY", 12: "STRING", 13: "BOOLEAN",
}
# type codes 5 (VARIANT), 9 (OBJECT), 10 (ARRAY) are semi-structured -> STRING


def snowflake_creds_from_env() -> dict | None:
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    if not account or not user:
        return None
    creds = {"account": account, "user": user}
    if os.environ.get("SNOWFLAKE_PASSWORD"):
        creds["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    elif os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"):
        creds["private_key_file"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    else:
        return None
    if os.environ.get("SNOWFLAKE_WAREHOUSE"):
        creds["warehouse"] = os.environ["SNOWFLAKE_WAREHOUSE"]
    if os.environ.get("SNOWFLAKE_ROLE"):
        creds["role"] = os.environ["SNOWFLAKE_ROLE"]
    return creds


def copy_table_from_snowflake(
    creds: dict, sf_database: str, sf_schema: str, sf_table: str,
    client: WorkspaceClient, warehouse_id: str, target_catalog: str, target_table: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> tuple[int, int]:
    """Returns (source_row_count, target_row_count). Raises on failure — caller soft-fails."""
    import snowflake.connector

    conn = snowflake.connector.connect(**creds)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {sf_database}.{sf_schema}.{sf_table}")
        source_count = cur.fetchone()[0]

        cur.execute(f"SELECT * FROM {sf_database}.{sf_schema}.{sf_table}")
        columns = [(d[0], SNOWFLAKE_TYPE_MAP.get(d[1], "STRING"), d[1]) for d in cur.description]
        col_defs = ", ".join(f"`{name}` {dtype}" for name, dtype, _ in columns)
        execute_sql(
            client, warehouse_id,
            f"CREATE TABLE IF NOT EXISTS {target_table} ({col_defs}) USING DELTA",
            catalog=target_catalog, schema="landing",
        )
        execute_sql(client, warehouse_id, f"TRUNCATE TABLE {target_table}", catalog=target_catalog, schema="landing")

        semi_structured_idx = {i for i, (_, _, code) in enumerate(columns) if code in (5, 9, 10)}
        col_names = ", ".join(f"`{name}`" for name, _, _ in columns)

        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            value_tuples = []
            for row in rows:
                vals = []
                for idx, v in enumerate(row):
                    if v is None:
                        vals.append("NULL")
                    elif idx in semi_structured_idx:
                        vals.append("'" + json.dumps(v).replace("'", "''") + "'")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    elif isinstance(v, bool):
                        vals.append("TRUE" if v else "FALSE")
                    else:
                        vals.append("'" + str(v).replace("'", "''") + "'")
                value_tuples.append("(" + ", ".join(vals) + ")")
            insert_stmt = f"INSERT INTO {target_table} ({col_names}) VALUES " + ", ".join(value_tuples)
            execute_sql(client, warehouse_id, insert_stmt, catalog=target_catalog, schema="landing")

        result = execute_sql(client, warehouse_id, f"SELECT COUNT(*) FROM {target_table}",
                              catalog=target_catalog, schema="landing")
        target_count = int(result.rows[0][0]) if result.rows else 0
        return source_count, target_count
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

@dataclass
class LoadResult:
    source_name: str
    table_name: str
    strategy: str  # native_redirect / unused / copied / unavailable
    target_location: str
    row_count_source: int | None
    row_count_target: int | None
    row_count_match: bool | None
    status: str  # ok / skipped / failed
    requires_human_review: bool
    notes: str


@dataclass
class DataLoaderReport:
    results: list[LoadResult]
    yml_redirects: dict[str, list[str]]

    def to_dict(self) -> dict:
        return asdict(self)


class DataLoaderAgent:
    def __init__(
        self,
        project_path: str,
        profile: str | None = DEFAULT_PROFILE,
        catalog: str = DEFAULT_CATALOG,
        warehouse_id: str | None = DEFAULT_WAREHOUSE_ID,
        batch_size: int = DEFAULT_BATCH_SIZE,
        apply_yml_fixes: bool = True,
        reset_workspace: bool = False,
        enabled: bool = True,
    ):
        self.source_path = Path(project_path).resolve()
        self.project_path = ensure_workspace_copy(self.source_path, reset=reset_workspace)
        self.profile = profile
        self.catalog = catalog
        self.warehouse_id = warehouse_id
        self.batch_size = batch_size
        self.apply_yml_fixes = apply_yml_fixes
        # Default ON: auto-redirects known native datasets (e.g. Snowflake's
        # built-in TPC-H sample data -> Databricks' samples.tpch) and attempts
        # a live Snowflake copy otherwise. A user with their own data source
        # can disable this entirely -- but doing so means THEY own the whole
        # source-resolution story: _sources.yml keeps its original Snowflake-
        # shaped database/schema untouched, so every model's source() call
        # will fail at Executor time unless their data already resolves under
        # those same names in Databricks, or they edit _sources.yml themselves.
        self.enabled = enabled
        self.client: WorkspaceClient | None = None

    def _samples_schemas(self) -> set[str]:
        try:
            return {s.name for s in self.client.schemas.list(catalog_name="samples")}
        except DatabricksError:
            return set()

    def process_source_group(
        self, yml_file: Path, source_name: str, tables: list[SourceTable],
        samples_schemas: set[str],
    ) -> tuple[list[LoadResult], list[str]]:
        results = []
        yml_fixes = []
        database_raw = tables[0].database_raw
        schema_raw = tables[0].schema_raw
        table_names = [t.table_name for t in tables]

        native = classify_native(database_raw, schema_raw, table_names, samples_schemas)
        if native:
            catalog, schema, reason = native
            already_correct = (
                database_raw.strip().strip('"').lower() == catalog
                and schema_raw.strip().strip('"').lower() == schema
            )
            if self.apply_yml_fixes and not already_correct:
                text = yml_file.read_text()
                new_text, changed = redirect_source_in_yml(text, source_name, catalog, schema)
                if changed:
                    yml_file.write_text(new_text)
                    yml_fixes.append(f"{source_name}: redirected to {catalog}.{schema} ({reason})")

            for t in tables:
                full_name = f"{catalog}.{schema}.{t.table_name.lower()}"
                try:
                    r = execute_sql(self.client, self.warehouse_id, f"SELECT COUNT(*) FROM {full_name}", catalog=catalog)
                    row_count = int(r.rows[0][0]) if r.rows else 0
                    results.append(LoadResult(
                        source_name, t.table_name, "native_redirect", full_name,
                        row_count, row_count, True, "ok", False, reason,
                    ))
                except StatementError as e:
                    results.append(LoadResult(
                        source_name, t.table_name, "native_redirect", full_name,
                        None, None, None, "failed", True, f"table not reachable: {e}",
                    ))
            return results, yml_fixes

        for t in tables:
            consumers = find_consumers(self.project_path, source_name, t.table_name)
            if not consumers:
                results.append(LoadResult(
                    source_name, t.table_name, "unused", "",
                    None, None, None, "skipped", False,
                    "no model or macro references this source table — nothing to load",
                ))
                continue

            creds = snowflake_creds_from_env()
            model_consumers = [c for c in consumers if c.startswith("models" + os.sep) or c.startswith("models/")]
            consumer_note = f"consumed by: {', '.join(consumers)}"
            if not model_consumers:
                consumer_note += " (macro-only reference — may be dead on Databricks if that macro was already dispatched to a no-op)"

            target_table = f"{self.catalog}.landing.{source_name}__{t.table_name}".lower()
            if creds is None:
                results.append(LoadResult(
                    source_name, t.table_name, "unavailable", target_table,
                    None, None, None, "skipped", True,
                    "Snowflake connection not configured (set SNOWFLAKE_ACCOUNT/SNOWFLAKE_USER + "
                    "SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH) — " + consumer_note,
                ))
                continue

            try:
                sf_db = database_raw if "{{" not in database_raw else self.catalog
                src_count, tgt_count = copy_table_from_snowflake(
                    creds, sf_db, schema_raw, t.table_name,
                    self.client, self.warehouse_id, self.catalog, target_table, self.batch_size,
                )
                results.append(LoadResult(
                    source_name, t.table_name, "copied", target_table,
                    src_count, tgt_count, src_count == tgt_count, "ok", False, consumer_note,
                ))
            except Exception as e:
                results.append(LoadResult(
                    source_name, t.table_name, "copied", target_table,
                    None, None, None, "failed", True, f"copy failed: {e}. {consumer_note}",
                ))

        return results, yml_fixes

    def write_audit(self, results: list[LoadResult]) -> None:
        if not results:
            return
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
                sql_val(r.source_name), sql_val(r.table_name), sql_val(r.strategy),
                sql_val(r.target_location), sql_val(r.row_count_source), sql_val(r.row_count_target),
                sql_val(r.row_count_match), sql_val(r.status), sql_val(r.requires_human_review),
                sql_val(r.notes), f"TIMESTAMP'{now}'",
            ]) + ")")
        stmt = (
            f"INSERT INTO {self.catalog}.audit.source_load "
            "(source_name, table_name, strategy, target_location, row_count_source, row_count_target, "
            "row_count_match, status, requires_human_review, notes, loaded_at) VALUES "
            + ", ".join(rows_sql)
        )
        execute_sql(self.client, self.warehouse_id, stmt, catalog=self.catalog, schema="audit")

    def run(self) -> DataLoaderReport:
        if not self.enabled:
            print("Data Loader disabled by user -- skipping entirely. _sources.yml is "
                  "left untouched; make sure your own data already resolves under "
                  "whatever database/schema _sources.yml currently declares, or edit "
                  "it yourself, before Executor runs.")
            return DataLoaderReport(results=[], yml_redirects={})

        self.client = get_client(self.profile)
        samples_schemas = self._samples_schemas()

        yml_files = find_sources_yml_files(self.project_path)
        all_results: list[LoadResult] = []
        all_yml_fixes: dict[str, list[str]] = {}

        for yml_file in yml_files:
            tables = parse_source_tables(yml_file)
            by_source: dict[str, list[SourceTable]] = {}
            for t in tables:
                by_source.setdefault(t.source_name, []).append(t)
            for source_name, group in by_source.items():
                results, fixes = self.process_source_group(yml_file, source_name, group, samples_schemas)
                all_results.extend(results)
                if fixes:
                    all_yml_fixes.setdefault(str(yml_file.relative_to(self.project_path)), []).extend(fixes)

        try:
            self.write_audit(all_results)
        except (StatementError, DatabricksError) as e:
            print(f"[warn] could not write to audit table: {e}")

        return DataLoaderReport(results=all_results, yml_redirects=all_yml_fixes)


def main() -> int:
    parser = argparse.ArgumentParser(description="Data Loader Agent")
    parser.add_argument("project_path")
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--warehouse-id", default=DEFAULT_WAREHOUSE_ID)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--no-yml-fixes", action="store_true")
    parser.add_argument("--reset-workspace", action="store_true")
    parser.add_argument(
        "--skip-data-loader", action="store_true",
        help="Skip Data Loader entirely -- you own loading your own data and making "
             "_sources.yml resolve correctly (see class docstring / --help for the tradeoff)",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    agent = DataLoaderAgent(
        project_path=args.project_path, profile=args.profile, catalog=args.catalog,
        warehouse_id=args.warehouse_id, batch_size=args.batch_size,
        apply_yml_fixes=not args.no_yml_fixes, reset_workspace=args.reset_workspace,
        enabled=not args.skip_data_loader,
    )
    report = agent.run()

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        print("Data Loader Agent")
        print(f"workspace copy: {agent.project_path}")
        print("=" * 60)
        for r in report.results:
            flag = " [REVIEW]" if r.requires_human_review else ""
            counts = f" ({r.row_count_source} -> {r.row_count_target})" if r.row_count_source is not None else ""
            print(f"  [{r.strategy}/{r.status}] {r.source_name}.{r.table_name}{counts}{flag} — {r.notes}")
        if report.yml_redirects:
            print("\n_sources.yml redirects:")
            for f, fixes in report.yml_redirects.items():
                for fx in fixes:
                    print(f"  {f}: {fx}")
        print("=" * 60)
        failed = [r for r in report.results if r.status == "failed"]
        print(f"{len(report.results)} tables processed, {len(failed)} failed")

    return 1 if any(r.status == "failed" for r in report.results) else 0


if __name__ == "__main__":
    sys.exit(main())
