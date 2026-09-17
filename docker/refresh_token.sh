#!/usr/bin/env bash
# Run this on your HOST machine (not inside the container) whenever the
# agents/Streamlit UI start failing with a Databricks auth error -- it's
# almost always DATABRICKS_TOKEN in .env having expired, not a real bug.
# A token minted via `databricks auth token` is short-lived (~1hr) by
# design; a long-lived static PAT set by hand in .env never needs this.
#
# Usage: docker/refresh_token.sh <profile> [path/to/.env]
#   profile   the `databricks` CLI profile to mint a fresh token from
#             (must already be logged in -- `databricks auth login --profile <profile>`
#             once, interactively, if this script tells you the refresh
#             token itself has expired)
#   .env      defaults to .env next to this script's parent directory
#
# After this succeeds, restart the container so it picks up the new value:
#   docker compose up -d

set -euo pipefail

PROFILE="${1:?Usage: docker/refresh_token.sh <profile> [path/to/.env]}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${2:-$SCRIPT_DIR/../.env}"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "error: $ENV_FILE not found -- copy .env.example to .env first." >&2
    exit 1
fi

echo "Minting a fresh token from profile '$PROFILE'..."
# Deliberately not merging stderr into the captured output (no `2>&1`) --
# any stray warning/log line on stderr would silently corrupt the JSON
# parse below. Let stderr pass through to the terminal directly instead.
if ! AUTH_JSON=$(databricks auth token --profile "$PROFILE"); then
    echo >&2
    echo "error: 'databricks auth token --profile $PROFILE' failed (see output above)." >&2
    echo "This usually means the profile's own OAuth refresh token has expired" >&2
    echo "(happens far less often than the access token itself -- weeks, not" >&2
    echo "hours). Fix: run 'databricks auth login --profile $PROFILE' once," >&2
    echo "interactively, then re-run this script." >&2
    exit 1
fi

NEW_TOKEN=$(python3 -c "import json, sys; print(json.loads(sys.argv[1])['access_token'])" "$AUTH_JSON")

python3 - "$ENV_FILE" "$NEW_TOKEN" <<'PYEOF'
import re
import sys

env_file, new_token = sys.argv[1], sys.argv[2]
with open(env_file) as f:
    content = f.read()

if re.search(r"^DATABRICKS_TOKEN=", content, flags=re.MULTILINE):
    content = re.sub(r"^DATABRICKS_TOKEN=.*$", f"DATABRICKS_TOKEN={new_token}", content, flags=re.MULTILINE)
else:
    content = content.rstrip("\n") + f"\nDATABRICKS_TOKEN={new_token}\n"

with open(env_file, "w") as f:
    f.write(content)
PYEOF

echo "Token refreshed in $ENV_FILE."
echo
echo ">>> Restart the container now for this to take effect: docker compose up -d"
