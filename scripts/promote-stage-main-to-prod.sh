#!/bin/sh
set -eu

git remote get-url stage >/dev/null 2>&1 || {
  echo "Missing git remote 'stage'." >&2
  exit 1
}

git remote get-url origin >/dev/null 2>&1 || {
  echo "Missing git remote 'origin' for prod." >&2
  exit 1
}

echo "Fetching prod and stage refs..."
git fetch origin --quiet
git fetch stage --quiet

STAGE_MAIN="$(git rev-parse refs/remotes/stage/main)"
PROD_MAIN="$(git rev-parse refs/remotes/origin/main)"

if [ "$STAGE_MAIN" = "$PROD_MAIN" ]; then
  echo "Prod is already at stage/main (${STAGE_MAIN})."
  exit 0
fi

echo "Promoting stage/main (${STAGE_MAIN}) to prod main..."
git push origin "${STAGE_MAIN}:refs/heads/main"
