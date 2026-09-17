"""Thin SQL execution helper over the Databricks SDK statement execution API.

Used by every agent that needs to talk to a SQL Warehouse (audit tables,
Unity Catalog checks, source/target comparisons). Centralized here so agents
never poll the statement execution API by hand.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied, Unauthenticated
from databricks.sdk.service.sql import StatementState

from agents.common.config import required_warehouse_id


class StatementError(RuntimeError):
    """Raised when a SQL statement fails or times out on the warehouse."""


class DatabricksAuthError(RuntimeError):
    """Raised when a Databricks API call fails because the credential itself
    is expired/invalid, not because of anything about the request. Deliberately
    NOT a StatementError subclass -- several agents `except StatementError` to
    soft-fail one check and keep going (validator.py, data_loader.py, preflight.py),
    and a dead credential should stop the run loudly, not be absorbed as if it
    were one failed check among many.
    """


# Message-text fallback for cases the SDK doesn't raise as a clean typed
# error -- confirmed empirically this happens (an expired-but-well-formed
# JWT produced an unparseable-response error the SDK itself couldn't map to
# Unauthenticated/PermissionDenied, per the literal "this is likely a bug in
# the SDK" text it prints). Checking exception TYPE alone isn't enough --
# tested a garbage/malformed token and it came back as Unauthenticated with
# message text containing none of "401"/"403"/"invalid token", so this list
# is deliberately broader than just the HTTP-code strings.
_AUTH_ERROR_SIGNALS = (
    "403",
    "401",
    "invalid token",
    "invalid access token",
    "unauthenticated",
    "permissiondenied",
    "credential was not sent",
    "unable to parse response",
)


def raise_if_auth_error(exc: Exception) -> None:
    """Re-raise `exc` as a DatabricksAuthError with an actionable message if it
    looks like an expired/invalid credential (a 401/403, or the SDK's own
    "unable to parse response" wrapper around one -- confirmed this is exactly
    what an expired token minted via `databricks auth token` looks like from
    this codebase's own call sites). Callers should call this in an `except
    Exception as e:` block and re-raise the original `e` themselves if this
    returns without raising -- it's a no-op for anything that isn't an auth
    problem.
    """
    if isinstance(exc, (Unauthenticated, PermissionDenied)):
        _raise_auth_error(exc)
    text = str(exc).lower()
    if any(signal in text for signal in _AUTH_ERROR_SIGNALS):
        _raise_auth_error(exc)


def _raise_auth_error(exc: Exception) -> None:
    raise DatabricksAuthError(
        "Databricks credentials look expired or invalid (saw a 401/403 from "
        "the API). If DATABRICKS_TOKEN was minted via `databricks auth token`, "
        "it's short-lived by design and this is almost always just that -- run "
        "`docker/refresh_token.sh <profile>` on your HOST machine, then restart "
        "the container (`docker compose up -d`), and retry.\n"
        f"Original error: {exc}"
    ) from exc


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
    try:
        resp = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=statement,
            catalog=catalog,
            schema=schema,
            wait_timeout=f"{wait}s",
        )
    except Exception as e:
        raise_if_auth_error(e)
        raise

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
