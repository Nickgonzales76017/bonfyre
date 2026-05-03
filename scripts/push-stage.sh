#!/bin/sh
set -eu

BRANCH="${1:-$(git branch --show-current)}"

if [ -z "$BRANCH" ]; then
  echo "Could not determine branch. Pass one explicitly: scripts/push-stage.sh <branch>" >&2
  exit 1
fi

git remote get-url stage >/dev/null 2>&1 || {
  echo "Missing git remote 'stage'. Configure it first." >&2
  exit 1
}

echo "Pushing ${BRANCH} to stage..."
git push -u stage "$BRANCH"
