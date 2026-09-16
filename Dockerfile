# Each user builds their own image with their own credentials (2026-09-15
# decision, see CHECKPOINT.md "Deferred bundle/UI work") -- no shared
# pre-built image is published. In practice this build itself needs no
# credentials at all (confirmed empirically -- see docker/install_morpheus.py);
# credentials only matter at container run time, via plain env vars
# (DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID).
FROM python:3.12-slim

# Java 21+ required by the Morpheus transpiler engine.
#
# git is back (2026-09-15, after briefly removing it): our OWN code no
# longer uses it at all (see agents/preflight.py / cli.py — the git-branch
# auto-detection for developer/branch audit tagging was dropped for good,
# along with the mount-ownership friction that caused, since this tool now
# targets each developer running their own instance against their own
# project rather than AGENT_DESIGN.md's original shared-repo workflow).
# But `dbt debug` itself has its own internal "required dependencies" check
# for the git binary being present on PATH — entirely independent of our
# code, and unconditional regardless of whether the project even uses a
# git-sourced package. Confirmed by a real failed build: with git missing,
# `dbt debug` reports "1 check failed: git" even though the actual
# Databricks connection succeeds, which flips our own (blocking)
# check_dbt_debug() to fail and Preflight to NO-GO for a reason that has
# nothing to do with connectivity. This is just the binary on PATH — dbt's
# own check never touches the mounted project directory, so it doesn't
# reintroduce the ownership problem our own removed git usage caused.
#
# No `databricks` CLI binary here (also removed 2026-09-1x, along with
# run_lakebridge()'s subprocess call to it): it's no longer used by any of
# our own code at all. Lakebridge now runs via its own Python API directly
# (databricks.labs.lakebridge.cli.transpile) — see run_lakebridge()'s own
# docstring in agents/transpiler.py for why the CLI subprocess route never
# actually worked inside this image in the first place.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jdk-headless \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
# databricks-labs-lakebridge's own transitive dependency (databricks-bb-
# analyzer) declares jsonschema~=4.0.0, which is incompatible with dbt-core's
# jsonschema>=4.19.1,<5.0 under one unified resolution (confirmed by a real
# failed build: ResolutionImpossible). The exact same set of packages already
# coexists fine in local dev at jsonschema==4.26.0 — evidently installed
# across separate pip commands over time, where a later install upgraded
# jsonschema without pip re-validating an earlier package's now-technically-
# violated constraint. Two separate RUN steps reproduces that same working
# state deliberately, rather than accidentally.
RUN pip install --no-cache-dir databricks-labs-lakebridge==0.14.2
RUN pip install --no-cache-dir -r requirements.txt

# Morpheus transpiler engine -- see docker/install_morpheus.py for why this
# is NOT `databricks labs install lakebridge` (the full interactive CLI
# flow), and why it needs zero Databricks credentials.
COPY docker/install_morpheus.py .
RUN python install_morpheus.py && rm install_morpheus.py

# databricks-labs-blueprint's own logging setup (imported transitively the
# first time any databricks.labs.lakebridge module loads) calls
# find_project_root(), which walks up from the importing file looking for a
# pyproject.toml/setup.py — present in the git-clone-based `databricks labs
# install lakebridge` layout this library normally expects, but never
# present for a plain `pip install`, which is what this image uses (see
# above). Confirmed by a real crash: NotADirectoryError: Cannot find
# project root, on the very first import. An empty pyproject.toml dropped
# at the lakebridge package's own root satisfies that walk-up search
# without needing to fake a real project structure.
RUN touch /usr/local/lib/python3.12/site-packages/databricks/labs/lakebridge/pyproject.toml

# Generic dbt profile — no secrets baked in, every value resolves from the
# container's own environment at dbt-run time. See the template's own header
# comment for the env var names.
RUN mkdir -p /root/.dbt
COPY docker/profiles.yml.template /root/.dbt/profiles.yml

COPY . .

EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
