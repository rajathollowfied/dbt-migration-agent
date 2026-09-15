"""Thin SQL execution helper over the Databricks SDK statement execution API.

Used by every agent that needs to talk to a SQL Warehouse (audit tables,
Unity Catalog checks, source/target comparisons). Centralized here so agents
never poll the statement execution API by hand.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from agents.common.config import required_warehouse_id


class StatementError(RuntimeError):
    """Raised when a SQL statement fails or times out on the warehouse."""


@dataclass
class SqlResult:
    columns: list[str] = field(default_factory=list)
    rows: list[list] = field(default_factory=list)


def get_client(profile: str | None = None) -> WorkspaceClient:
    """Construct a WorkspaceClient for `profile`.

    Passing `profile=` alone is not always enough: if `~/.databrickscfg` has more
    than one profile pointing at the same host (e.g. a "VSCode" profile added by
    the VS Code extension alongside a hand-configured one), the SDK's databricks-cli
    credential strategy can still fall back to host-based lookup internally and hit
    "multiple profiles match this host" — even though a profile was explicitly given.
    Setting DATABRICKS_CONFIG_PROFILE disambiguates that fallback path too.
    """
    if profile:
        os.environ["DATABRICKS_CONFIG_PROFILE"] = profile
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


def execute_sql(
    client: WorkspaceClient,
    warehouse_id: str | None,
    statement: str,
    catalog: str | None = None,
    schema: str | None = None,
    timeout_seconds: int = 50,
) -> SqlResult:
    """Run one statement to completion and return its result set (if any).

    `timeout_seconds` is capped at 50s because the API's synchronous
    `wait_timeout` maxes out at 50s server-side; longer-running statements
    should be split or handled with async polling — not needed for the
    preflight/audit-table use cases this helper currently serves.

    `warehouse_id` is typed as optional because callers now source it from
    agents.common.config.DEFAULT_WAREHOUSE_ID, which is None when
    DATABRICKS_WAREHOUSE_ID isn't set — validated here, the single choke
    point every agent's SQL goes through, rather than at each of the many
    call sites.
    """
    warehouse_id = required_warehouse_id(warehouse_id)
    wait = min(timeout_seconds, 50)
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        catalog=catalog,
        schema=schema,
        wait_timeout=f"{wait}s",
    )

    status = resp.status
    if status and status.state == StatementState.FAILED:
        err = status.error
        message = err.message if err else "unknown error"
        raise StatementError(f"statement failed: {message}\nSQL: {statement}")

    if status and status.state in (StatementState.CANCELED, StatementState.CLOSED):
        raise StatementError(f"statement ended in state {status.state}: {statement}")

    if resp.result is None or resp.manifest is None:
        return SqlResult()

    columns = [c.name for c in (resp.manifest.schema.columns or [])] if resp.manifest.schema else []
    rows = resp.result.data_array or []
    return SqlResult(columns=columns, rows=rows)
