"""Environment-driven defaults, shared by every agent's CLI and constructor.

Real problem this fixes (2026-09-15): `catalog`/`warehouse_id`/`profile`
defaults were hardcoded to THIS project's own workspace values
(`dbt_migration`, `b05480be6edc2be5`, `free_community`) independently in
every one of the 8 agent files plus cli.py — ~48 occurrences total. That
directly contradicts the stated goal of being plug-and-play for any
Snowflake+dbt project: a new user pointing this at their own workspace would
silently keep hitting OUR warehouse/catalog/profile unless they remembered
to override every flag on every single command.

Conventions used, matching standard Databricks SDK/CLI env vars where they
already exist (so a workspace already configured for the `databricks` CLI or
other Databricks tooling needs zero extra setup):
  DATABRICKS_CONFIG_PROFILE   — same env var the SDK itself already reads;
                                 None here means "let the SDK's own default
                                 resolution (~/.databrickscfg [DEFAULT], or
                                 DATABRICKS_HOST/DATABRICKS_TOKEN) decide",
                                 exactly like passing profile=None already did.
  DATABRICKS_WAREHOUSE_ID     — no generic default is possible (a warehouse
                                 ID is inherently workspace-specific) —
                                 required_warehouse_id() below fails loudly
                                 with a clear message instead of letting a
                                 missing ID fail obscurely deep in an HTTP call.
  DBT_MIGRATION_CATALOG       — "dbt_migration" is kept as a generic default
                                 CATALOG NAME (not a workspace identity, just
                                 a naming convention any user can reuse or
                                 override) — same reasoning for _TARGET.
  DBT_MIGRATION_DBT_TARGET
"""

from __future__ import annotations

import os

DEFAULT_PROFILE: str | None = os.environ.get("DATABRICKS_CONFIG_PROFILE") or None
DEFAULT_CATALOG: str = os.environ.get("DBT_MIGRATION_CATALOG", "dbt_migration")
DEFAULT_WAREHOUSE_ID: str | None = os.environ.get("DATABRICKS_WAREHOUSE_ID") or None
DEFAULT_DBT_TARGET: str = os.environ.get("DBT_MIGRATION_DBT_TARGET", "dev")


def required_warehouse_id(warehouse_id: str | None) -> str:
    """Every agent that actually executes SQL calls this instead of using a
    possibly-None warehouse_id directly — fails immediately with a clear,
    actionable message rather than a confusing error deep inside an HTTP call
    to an empty/None warehouse path."""
    if not warehouse_id:
        raise ValueError(
            "No SQL Warehouse ID configured — set the DATABRICKS_WAREHOUSE_ID "
            "environment variable or pass --warehouse-id explicitly."
        )
    return warehouse_id
