# Setup

How to run this tool against your own Snowflake+dbt project. See
`CHECKPOINT.md` for the running log of decisions/bugs, `AGENT_DESIGN.md`
for the full architecture.

## What you need first

- **A Databricks workspace** with a Unity Catalog catalog and a SQL
  Warehouse already created (any names — the catalog just needs to exist;
  everything under it, schemas and audit tables, is created automatically
  by Preflight the first time you run it).
- **A Databricks credential** — either a personal access token (long-lived)
  or a token minted via `databricks auth token --profile <profile>`
  (short-lived, ~1hr, if your workspace uses OAuth login). See "Credential
  refresh" below if you're using the latter.
- **Docker + Docker Compose.** This is the supported path — it bakes the
  Lakebridge/Morpheus transpiler engine and every dependency into an image
  once, so you don't need a replicated dev machine (see "Local development"
  below only if you're working on the agents' own code).

## Quick start

```bash
cd dbt-migration-agent
cp .env.example .env
```

Fill in `.env`:

| Variable | Where to find it |
|---|---|
| `DATABRICKS_HOST` | Your workspace URL |
| `DATABRICKS_TOKEN` | See "What you need first" above |
| `DATABRICKS_WAREHOUSE_ID` | Databricks UI → SQL Warehouses → (your warehouse) → Connection details |
| `DBT_MIGRATION_CATALOG` | Optional — defaults to `dbt_migration` if left blank; the catalog must already exist |
| `PROJECT_PATH` | Path on your machine to the Snowflake+dbt project you're migrating |

Build and start:

```bash
docker compose up -d --build
```

Then either:
- Open **http://localhost:8501** for the Streamlit UI, or
- Use the CLI directly: `docker compose exec migration-agent python scripts/cli.py <command> /data/project`
  (`/data/project` is the fixed in-container path `PROJECT_PATH` gets mounted to)

Run `docker compose exec migration-agent python scripts/cli.py help` for
the full command list (`preflight`, `macros`, `load`, `analyze`,
`transpile`, `execute`, `diagnose`, `validate`, `run`, `status`,
`apply-fix`).

## What happens automatically

There's no manual project prep needed — this is the actual point of the
tool. Point it at your project and run Preflight first (or just `run` for
the full pipeline):

- **Preflight** checks connectivity, creates the catalog's schemas/audit
  tables if missing, and validates/auto-fixes `dbt_project.yml` (profile
  name, Snowflake-only config blocks) — the things that used to require
  hand-editing (`sed`, manually diagnosing config issues) are handled here
  now.
- **Macro Resolver** classifies every macro your project uses and
  auto-generates the Databricks-side dispatch pattern for the ones it
  recognizes, flagging anything it doesn't for review.
- The rest of the pipeline (Data Loader through Validator) transpiles,
  runs, diagnoses, and validates the migrated models — see
  `AGENT_DESIGN.md` for what each agent does.

If Preflight comes back NO-GO or something needs manual attention, the
tool tells you exactly what and why — it doesn't silently continue.

## Credential refresh

If `DATABRICKS_TOKEN` was minted via `databricks auth token` (OAuth
login), it's short-lived by design (~1hr) — a static PAT doesn't need any
of this. When a run fails with a `DatabricksAuthError`, that's what
happened, not a bug:

```bash
docker/refresh_token.sh <profile>   # run on your HOST machine, not in the container
docker compose up -d                # restart to pick up the new token
```

## Local development

Only needed if you're working on the agents' own code, not for using the
tool against a project.

```bash
pip install -r requirements.txt
pip install databricks-labs-lakebridge==0.14.2
```

You'll also need Java 21+ and the Morpheus transpiler engine installed —
`docker/install_morpheus.py` does this directly (no Databricks credentials
needed, ~68MB); run it once with `python docker/install_morpheus.py`.

`~/.dbt/profiles.yml` (note: local dev uses a *separate* env var,
`DBT_DATABRICKS_TOKEN`, for dbt specifically — unlike the Docker image's
`profiles.yml.template`, which unifies to one `DATABRICKS_TOKEN` for both
dbt and the SDK):

```yaml
DATABRICKS:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <your-workspace-host>
      http_path: /sql/1.0/warehouses/<your-warehouse-id>
      token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
      catalog: dbt_migration
      schema: bronze
      threads: 4
```

Then run agents directly, e.g.:

```bash
python3 scripts/cli.py preflight /path/to/your/project
python3 scripts/cli.py run /path/to/your/project
```
