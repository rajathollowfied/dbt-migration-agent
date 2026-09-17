"""Streamlit UI — a thin wrapper over the exact same functions cli.py already
wraps (run_full_pipeline / run_agent_command / run_apply_fix / fetch_status).
No orchestration logic lives here; every button below calls straight into
cli.py, with stdout captured live into the page instead of a terminal.

Run locally with: streamlit run scripts/app.py
Self-hosted, no Databricks Apps dependency — see CHECKPOINT.md "Deferred
bundle/UI work" for why (2026-09-15 architecture decision: agents run
external to Databricks, which stays backend-only).
"""

from __future__ import annotations

import argparse
import io
import os
import re
import sys
from datetime import datetime
from pathlib import Path

# app.py lives in scripts/, but agents/ is a sibling of scripts/'s parent —
# see the matching comment in scripts/cli.py.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

# Package-qualified, not `import cli` -- Streamlit's script runner doesn't
# reliably put this file's own directory on sys.path the way a plain
# `python3 scripts/app.py` would, and a bare `cli` module name risks
# resolving to an unrelated package elsewhere on sys.path (confirmed this
# actually happens in a shared dev venv with another project's own `cli`
# package installed).
import scripts.cli as cli
from agents.common.config import DEFAULT_CATALOG, DEFAULT_PROFILE, DEFAULT_WAREHOUSE_ID
from agents.common.db import get_client

st.set_page_config(page_title="dbt Migration Agent", layout="wide")
st.title("dbt Snowflake → Databricks Migration Agent")

with st.sidebar:
    st.header("Configuration")
    project_path = st.text_input(
        "Project path", value=os.environ.get("DEFAULT_PROJECT_PATH", ""),
        help="Path to the Snowflake+dbt project to migrate. Inside the docker-compose "
             "container this is pre-filled to the bind-mounted project's fixed path "
             "(DEFAULT_PROJECT_PATH) — running locally, point it at any path on this machine.",
    )
    developer = st.text_input("Developer", value="", help="Defaults to 'unknown' if left blank")
    profile = st.text_input(
        "Databricks CLI profile", value=DEFAULT_PROFILE or "",
        help="Blank uses the SDK's own default auth resolution (DATABRICKS_CONFIG_PROFILE env var, "
             "~/.databrickscfg [DEFAULT], or DATABRICKS_HOST/DATABRICKS_TOKEN)",
    )
    catalog = st.text_input("Catalog", value=DEFAULT_CATALOG)
    warehouse_id = st.text_input(
        "SQL Warehouse ID", value=DEFAULT_WAREHOUSE_ID or "",
        help="Required — set DATABRICKS_WAREHOUSE_ID to prefill this",
    )
    dbt_target = st.text_input("dbt target", value="dev")
    max_retries = st.number_input("Max retries (Diagnostician)", min_value=1, max_value=10, value=3)
    reset_workspace = st.checkbox(
        "Reset workspace copy", value=False,
        help="Discards the cached migration-workspace copy and re-copies from project_path",
    )

    if not warehouse_id:
        st.warning("No SQL Warehouse ID set — every action below will fail until one is provided.")


def build_args(**overrides) -> argparse.Namespace:
    base = dict(
        project_path=project_path, profile=profile or None, catalog=catalog,
        warehouse_id=warehouse_id or None, dbt_target=dbt_target,
        developer=developer or None, reset_workspace=reset_workspace, max_retries=int(max_retries),
    )
    base.update(overrides)
    return argparse.Namespace(**base)


LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


class _StreamlitLogWriter(io.TextIOBase):
    """Mirrors stdout writes into a Streamlit placeholder as they happen, so a
    long-running agent's progress is visible live instead of appearing all at
    once when it finishes (a full pipeline run can take 15-20 minutes).

    Also mirrors into st.session_state (keyed by `session_key`) and a log file
    on disk -- a plain local buffer disappears the moment Streamlit reruns the
    script (switching tabs, touching any other widget), since a full rerun
    re-executes this whole file from scratch with fresh local variables.
    session_state survives that; the file survives past the browser session
    entirely, for debugging after the fact.
    """

    def __init__(self, placeholder, session_key: str, log_file: Path):
        self.placeholder = placeholder
        self.session_key = session_key
        self.buffer = ""
        self._fh = open(log_file, "a")

    def write(self, s: str) -> int:
        self.buffer += s
        st.session_state[self.session_key] = self.buffer
        self.placeholder.code(self.buffer[-8000:] or " ")
        self._fh.write(s)
        self._fh.flush()
        return len(s)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self._fh.close()


def render_persistent_log(session_key: str):
    """Call once near the top of a tab, before its button -- restores the
    last run's log from session_state on every rerun (e.g. after switching to
    a different tab and back), instead of showing a blank placeholder until
    the button is clicked again. Returns the placeholder to hand to
    run_with_live_log for a fresh run."""
    placeholder = st.empty()
    existing = st.session_state.get(session_key)
    if existing:
        placeholder.code(existing[-8000:])
    return placeholder


def run_with_live_log(fn, *fn_args, placeholder, session_key: str, log_name: str, **fn_kwargs):
    st.session_state[session_key] = ""  # fresh display for this run; the log FILE still gets its own timestamped name below, so history across runs isn't lost
    safe_log_name = re.sub(r"[^A-Za-z0-9_-]", "_", log_name)  # log_name can come from free-text input (e.g. model_name) -- keep it a safe filename component
    log_file = LOG_DIR / f"{safe_log_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    writer = _StreamlitLogWriter(placeholder, session_key, log_file)
    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout = writer
    sys.stderr = writer  # defense-in-depth: every agent's own audit-write warnings already
    # print via plain print() now (fixed 2026-09-18 -- they used to print(..., file=sys.stderr),
    # invisible here since only stdout was captured), but redirecting stderr too means any
    # future or library-originated stderr output shows up in the UI log as well, not just
    # in a terminal nobody's watching.
    try:
        return fn(*fn_args, **fn_kwargs)
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        writer.close()


tab_run, tab_agents, tab_fix, tab_status = st.tabs(
    ["Run Full Pipeline", "Individual Agents", "Apply Fix", "Status"]
)

with tab_run:
    st.write("Runs Preflight through Validator end-to-end — identical to `cli.py run`.")
    log_placeholder = render_persistent_log("log_run_full_pipeline")
    if st.button("Run full pipeline", type="primary", disabled=not project_path):
        with st.status("Running full pipeline...", expanded=True) as status_box:
            args = build_args()
            try:
                rc = run_with_live_log(
                    cli.run_full_pipeline, args,
                    placeholder=log_placeholder, session_key="log_run_full_pipeline", log_name="run_full_pipeline",
                )
            except Exception as e:
                status_box.update(label=f"Pipeline errored: {e}", state="error")
            else:
                status_box.update(
                    label="Pipeline finished" if rc == 0 else "Pipeline stopped early — see log above",
                    state="complete" if rc == 0 else "error",
                )

with tab_agents:
    st.write("Runs exactly one agent — identical to `cli.py <agent>`. Same interim/resume use case "
             "as the CLI: `run` above is always full-pipeline, use this to retry just one step.")
    agent_choice = st.selectbox("Agent", list(cli.AGENT_MODULES.keys()))
    log_placeholder = render_persistent_log(f"log_agent_{agent_choice}")
    if st.button(f"Run {agent_choice}", disabled=not project_path):
        argv = [project_path, "--catalog", catalog, "--dbt-target", dbt_target]
        if profile:
            argv += ["--profile", profile]
        if warehouse_id:
            argv += ["--warehouse-id", warehouse_id]
        if developer:
            argv += ["--developer", developer]
        if reset_workspace:
            argv.append("--reset-workspace")
        with st.status(f"Running {agent_choice}...", expanded=True) as status_box:
            try:
                rc = run_with_live_log(
                    cli.run_agent_command, agent_choice, argv,
                    placeholder=log_placeholder, session_key=f"log_agent_{agent_choice}", log_name=f"agent_{agent_choice}",
                )
            except Exception as e:
                status_box.update(label=f"{agent_choice} errored: {e}", state="error")
            else:
                status_box.update(
                    label=f"{agent_choice} finished" if rc == 0 else f"{agent_choice} failed — see log above",
                    state="complete" if rc == 0 else "error",
                )

with tab_fix:
    st.write(
        "Applies a Diagnostician *recommendation* — a hard-stop category with a real, "
        "tested fix that was surfaced but not auto-applied (see `RECOMMENDED_FIXES` in "
        "`agents/diagnostician.py`, and CHECKPOINT.md 'Advisory-then-apply workflow'). "
        "Run Diagnose first so there's a recommendation to apply."
    )
    if st.button("Show pending recommendations", disabled=not warehouse_id):
        try:
            client = get_client(profile or None)
            pending = cli.fetch_pending_recommendations(client, catalog, warehouse_id)
        except Exception as e:
            st.error(f"Could not fetch pending recommendations: {e}")
        else:
            if pending.rows:
                st.dataframe(
                    [dict(zip(pending.columns, row)) for row in pending.rows],
                    width="stretch",
                )
            else:
                st.write("(none right now — nothing currently has an unapplied recommendation)")
    model_name = st.text_input("Model name")
    log_placeholder = render_persistent_log("log_apply_fix")
    if st.button("Apply fix", disabled=not (project_path and model_name)):
        args = build_args(model_name=model_name)
        with st.status(f"Applying fix for {model_name}...", expanded=True) as status_box:
            try:
                rc = run_with_live_log(
                    cli.run_apply_fix, args,
                    placeholder=log_placeholder, session_key="log_apply_fix", log_name=f"apply_fix_{model_name}",
                )
            except Exception as e:
                status_box.update(label=f"Apply-fix errored: {e}", state="error")
            else:
                status_box.update(
                    label="Applied and verified" if rc == 0 else "Not applied — see log above",
                    state="complete" if rc == 0 else "error",
                )

with tab_status:
    st.write("Latest pipeline run and the current human review queue, from the audit tables.")
    if st.button("Refresh status", disabled=not warehouse_id):
        try:
            client = get_client(profile or None)
            latest_pipeline_run, review_queue = cli.fetch_status(client, catalog, warehouse_id)
        except Exception as e:
            st.error(f"Could not fetch status: {e}")
        else:
            st.subheader("Most recent pipeline run")
            if latest_pipeline_run.rows:
                st.json(dict(zip(latest_pipeline_run.columns, latest_pipeline_run.rows[0])), expanded=True)
            else:
                st.write("(no pipeline runs recorded yet)")

            st.subheader("Human review queue")
            if review_queue.rows:
                st.dataframe(
                    [dict(zip(review_queue.columns, row)) for row in review_queue.rows],
                    width="stretch",
                )
            else:
                st.write("(empty)")
