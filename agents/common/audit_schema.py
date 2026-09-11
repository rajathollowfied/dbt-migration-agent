"""DDL for the shared `dbt_migration.audit` Delta tables (see AUDIT_SCHEMA.md).

Preflight owns making sure this schema exists before any other agent tries
to write to it — every downstream agent assumes these tables are already
there.
"""

from __future__ import annotations

from databricks.sdk import WorkspaceClient

from agents.common.db import execute_sql

SCHEMA_DDL = "CREATE SCHEMA IF NOT EXISTS {catalog}.audit"

TABLE_DDL = {
    "model_runs": """
        CREATE TABLE IF NOT EXISTS {catalog}.audit.model_runs (
            model_name              STRING NOT NULL,
            layer                   STRING,
            complexity              STRING,
            warehouse_size          STRING,
            warehouse_source        STRING,
            transpile_status        STRING,
            run_status              STRING,
            blocked_by_upstream     BOOLEAN,
            error_category          STRING,
            attempted_fix           STRING,
            fix_successful          BOOLEAN,
            retry_count             INT,
            final_error_message     STRING,
            requires_human_review   BOOLEAN,
            developer               STRING,
            run_id                  STRING,
            run_timestamp           TIMESTAMP
        )
        USING DELTA
        COMMENT 'Tracks migration status of every dbt model across all agent runs'
    """,
    "macro_resolution": """
        CREATE TABLE IF NOT EXISTS {catalog}.audit.macro_resolution (
            macro_name              STRING NOT NULL,
            macro_file              STRING,
            category                STRING,
            snowflake_construct     STRING,
            databricks_action       STRING,
            dispatch_applied        BOOLEAN,
            requires_human_review   BOOLEAN,
            review_notes            STRING,
            processed_at            TIMESTAMP
        )
        USING DELTA
        COMMENT 'Tracks macro classification and resolution decisions by Macro Resolver Agent'
    """,
    "pattern_library": """
        CREATE TABLE IF NOT EXISTS {catalog}.audit.pattern_library (
            pattern_id          STRING NOT NULL,
            error_category      STRING,
            regex_pattern       STRING,
            fix_template        STRING,
            source              STRING,
            times_applied       INT,
            last_applied        TIMESTAMP,
            created_at          TIMESTAMP,
            created_by          STRING
        )
        USING DELTA
        COMMENT 'Shared pattern library for Diagnostician Agent — grows with each run'
    """,
    "validation_results": """
        CREATE TABLE IF NOT EXISTS {catalog}.audit.validation_results (
            model_name              STRING NOT NULL,
            layer                   STRING,
            schema_match            BOOLEAN,
            schema_match_notes      STRING,
            row_count_source        BIGINT,
            row_count_target        BIGINT,
            row_count_match         BOOLEAN,
            row_count_threshold     FLOAT,
            checksum_source         STRING,
            checksum_target         STRING,
            checksum_match          BOOLEAN,
            business_rules_pass     BOOLEAN,
            business_rules_notes    STRING,
            migration_score         FLOAT,
            validated_at            TIMESTAMP,
            developer               STRING
        )
        USING DELTA
        COMMENT 'Validator Agent results — Snowflake source vs Databricks target comparison'
    """,
    "pipeline_runs": """
        CREATE TABLE IF NOT EXISTS {catalog}.audit.pipeline_runs (
            run_id              STRING NOT NULL,
            developer           STRING,
            branch              STRING,
            command             STRING,
            max_retries         INT,
            start_time          TIMESTAMP,
            end_time            TIMESTAMP,
            duration_minutes    FLOAT,
            total_models        INT,
            passed              INT,
            failed              INT,
            blocked             INT,
            needs_review        INT,
            pipeline_status     STRING
        )
        USING DELTA
        COMMENT 'One row per pipeline invocation — top-level run tracking'
    """,
    "source_load": """
        CREATE TABLE IF NOT EXISTS {catalog}.audit.source_load (
            source_name          STRING NOT NULL,
            table_name           STRING NOT NULL,
            strategy              STRING,
            target_location       STRING,
            row_count_source      BIGINT,
            row_count_target      BIGINT,
            row_count_match       BOOLEAN,
            status                STRING,
            requires_human_review BOOLEAN,
            notes                 STRING,
            loaded_at             TIMESTAMP
        )
        USING DELTA
        COMMENT 'Tracks every source table the Data Loader Agent processed — native redirect, copied, or skipped'
    """,
    "validation_results": """
        CREATE TABLE IF NOT EXISTS {catalog}.audit.validation_results (
            model_name              STRING NOT NULL,
            layer                   STRING,
            schema_match            BOOLEAN,
            schema_match_notes      STRING,
            row_count_source        BIGINT,
            row_count_target        BIGINT,
            row_count_match         BOOLEAN,
            row_count_threshold     FLOAT,
            checksum_source         STRING,
            checksum_target         STRING,
            checksum_match          BOOLEAN,
            business_rules_pass     BOOLEAN,
            business_rules_notes    STRING,
            migration_score         FLOAT,
            validated_at            TIMESTAMP,
            developer               STRING
        )
        USING DELTA
        COMMENT 'Validator Agent results — Snowflake source vs Databricks target comparison'
    """,
}


def ensure_audit_tables(client: WorkspaceClient, warehouse_id: str, catalog: str) -> list[str]:
    """Create the audit schema + all tables if missing. Returns statements executed."""
    executed = []

    stmt = SCHEMA_DDL.format(catalog=catalog)
    execute_sql(client, warehouse_id, stmt, catalog=catalog)
    executed.append(stmt.strip())

    for name, ddl in TABLE_DDL.items():
        stmt = ddl.format(catalog=catalog).strip()
        execute_sql(client, warehouse_id, stmt, catalog=catalog, schema="audit")
        executed.append(f"CREATE TABLE IF NOT EXISTS {catalog}.audit.{name}")

    return executed
