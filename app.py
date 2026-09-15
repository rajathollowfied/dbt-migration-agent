"""Streamlit UI — a thin wrapper over the exact same functions cli.py already
wraps (run_full_pipeline / run_agent_command / run_apply_fix / fetch_status).
No orchestration logic lives here; every button below calls straight into
cli.py, with stdout captured live into the page instead of a terminal.

Run locally with: streamlit run app.py
Self-hosted, no Databricks Apps dependency — see CHECKPOINT.md "Deferred
bundle/UI work" for why (2026-09-15 architecture decision: agents run
external to Databricks, which stays backend-only).
"""

from __future__ import annotations

import argparse
import io
import sys

import streamlit as st

import cli
from agents.common.config import DEFAULT_CATALOG, DEFAULT_PROFILE, DEFAULT_WAREHOUSE_ID
from agents.common.db import get_client

st.set_page_config(page_title="dbt Migration Agent", layout="wide")
st.title("dbt Snowflake → Databricks Migration Agent")

with st.sidebar:
    st.header("Configuration")
    project_path = st.text_input(
        "Project path", help="Path to the Snowflake+dbt project to migrate (on this machine)",
    )
    developer = st.text_input("Developer", value="", help="Defaults to the project's git branch if left blank")
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


class _StreamlitLogWriter(io.TextIOBase):
    """Mirrors stdout writes into a Streamlit placeholder as they happen, so a
    long-running agent's progress is visible live instead of appearing all at
    once when it finishes (a full pipeline run can take 15-20 minutes)."""

    def __init__(self, placeholder):
        self.placeholder = placeholder
        self.buffer = ""

    def write(self, s: str) -> int:
        self.buffer += s
        self.placeholder.code(self.buffer[-8000:] or " ")
        return len(s)

    def flush(self) -> None:
        pass


def run_with_live_log(fn, *fn_args, **fn_kwargs):
    placeholder = st.empty()
    writer = _StreamlitLogWriter(placeholder)
    old_stdout = sys.stdout
    sys.stdout = writer
    try:
        return fn(*fn_args, **fn_kwargs)
    finally:
        sys.stdout = old_stdout


tab_run, tab_agents, tab_fix, tab_status = st.tabs(
    ["Run Full Pipeline", "Individual Agents", "Apply Fix", "Status"]
)

with tab_run:
    st.write("Runs Preflight through Validator end-to-end — identical to `cli.py run`.")
    if st.button("Run full pipeline", type="primary", disabled=not project_path):
        with st.status("Running full pipeline...", expanded=True) as status_box:
            args = build_args()
            try:
                rc = run_with_live_log(cli.run_full_pipeline, args)
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
                rc = run_with_live_log(cli.run_agent_command, agent_choice, argv)
            except Exception as e:
                status_box.update(label=f"{agent_choice} errored: {e}", state="error")
            else:
                status_box.update(
                    label=f"{agent_choice} finished" if rc == 0 else f"{agent_choice} failed — see log above",
                    state="complete" if rc == 0 else "error",
                )

with tab_fix:
    st.write(
        "Applies a Diagnostician *recommendation* for a hard-stop category with a real, "
        "tested fix (currently: `stream_error`, the Snowflake Streams -> Delta CDF redesign) "
        "that was surfaced but not auto-applied. Run Diagnose first so there's a "
        "recommendation to apply — see CHECKPOINT.md 'Advisory-then-apply workflow'."
    )
    model_name = st.text_input("Model name")
    if st.button("Apply fix", disabled=not (project_path and model_name)):
        args = build_args(model_name=model_name)
        with st.status(f"Applying fix for {model_name}...", expanded=True) as status_box:
            try:
                rc = run_with_live_log(cli.run_apply_fix, args)
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
