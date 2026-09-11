# DBT Migration Agent — Environment Setup

## Prerequisites
- Databricks Free Edition account
- Linux environment with Python venv
- Java 21+

## 1. Unity Catalog Setup
In Databricks UI → Catalog → Create catalog:
- Name: `dbt_migration`
- Storage: Default managed storage

Create schemas under `dbt_migration`:
- `bronze`
- `silver`
- `gold`
- `audit`
- `landing`

## 2. SQL Warehouse
- Type: Serverless
- Size: 2XS (POC) — scale based on model complexity in production
- HTTP Path: `/sql/1.0/warehouses/<warehouse_id>`

## 3. Databricks CLI
```bash
# Verify install
databricks --version

# Verify profile
databricks auth profiles

# Profile used: free_community
# Host: https://dbc-d531ded2-aae8.cloud.databricks.com
```

## 4. Lakebridge
```bash
# Install
databricks labs install lakebridge --profile free_community

# Upgrade
databricks labs upgrade lakebridge

# Verify
databricks labs lakebridge describe-transpile --profile free_community
```
Transpiler: Morpheus | Source dialect: snowflake

## 5. Java
```bash
java -version
# Required: 21+
```

## 6. dbt Setup
```bash
# Install adapter
pip install dbt-databricks

# Verify
dbt --version
# dbt-core: 1.12.3
# dbt-databricks: 1.12.5
```

## 7. profiles.yml (~/.dbt/profiles.yml)
```yaml
DATABRICKS:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <workspace-host>
      http_path: <warehouse-http-path>
      token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
      catalog: dbt_migration
      schema: bronze
      threads: 4
```

```bash
# Set token
export DBT_DATABRICKS_TOKEN=your_token_here
```

## 8. Sample dbt Project (Guinea Pig)
```bash
git clone https://github.com/sfc-gh-dflippo/snowflake-dbt-demo.git
cd snowflake-dbt-demo

# Fix Snowflake-specific configs in dbt_project.yml
sed -i 's/profile: "SNOWFLAKE"/profile: "DATABRICKS"/' dbt_project.yml
sed -i 's/+snowflake_warehouse: "{{ target.warehouse }}"/#&/' dbt_project.yml
sed -i 's/+database: "{{ env_var.*target.database.* }}"/# &/' dbt_project.yml
sed -i 's/+schema: "{{ env_var.*target.schema.* }}"/# &/' dbt_project.yml

# Verify connection
dbt debug
```

## 9. Known dbt_project.yml Issues (Snowflake-specific)
| Config | Issue | Fix |
|--------|-------|-----|
| `profile: "SNOWFLAKE"` | Profile name mismatch | Changed to `DATABRICKS` |
| `+snowflake_warehouse: "{{ target.warehouse }}"` | Snowflake-only target property | Commented out |
| `+database/+schema: env_var(target.*)` | target.database/schema undefined pre-connection | Commented out |

## 10. Open Items
- [ ] Macro centralization confirmed
- [ ] cli.py routing logic (repo access needed)
- [ ] LLM model for Diagnostician
- [ ] Model partitioning across devs (parked)
- [ ] LATERAL FLATTEN — inline vs macro in actual repo
- [ ] dbt_utils version audit
