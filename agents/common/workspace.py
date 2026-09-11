"""Copies the user-provided Snowflake+dbt project into an isolated working
copy before any agent mutates a single file — the original input directory
(anywhere on disk, e.g. `snowflake-dbt-demo/`) is never touched.

See AGENT_DESIGN.md Section 11: `migration-workspace/` holds these copies,
`output_databricks/` (used by the Transpiler Agent) holds Lakebridge's raw
output before it's merged into the workspace copy.
"""

from __future__ import annotations

import shutil
from pathlib import Path

DEFAULT_WORKSPACE_ROOT = Path(__file__).resolve().parent.parent.parent / "migration-workspace"

_IGNORE = shutil.ignore_patterns(".git", "target", "dbt_packages", "dbt_internal_packages", "logs", "__pycache__")


def ensure_workspace_copy(
    source_path: Path | str,
    workspace_root: Path | str = DEFAULT_WORKSPACE_ROOT,
    reset: bool = False,
) -> Path:
    """Returns the workspace copy path, creating it on first call.

    Idempotent by default: if the copy already exists, it's left alone so
    fixes accumulated by earlier agent runs are preserved across the
    pipeline. Pass `reset=True` to discard the copy and start over from the
    current state of `source_path`.
    """
    source = Path(source_path).resolve()
    root = Path(workspace_root).resolve()
    target = root / source.name

    if reset and target.exists():
        shutil.rmtree(target)

    if not target.exists():
        root.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, target, ignore=_IGNORE)

    return target
