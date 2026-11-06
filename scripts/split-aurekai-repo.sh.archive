#!/usr/bin/env bash
# split-aurekai-repo.sh
# Extracts Aurekai/ into a standalone git repo ready to push to github.com/aurekai/aurekai
#
# Usage:
#   bash scripts/split-aurekai-repo.sh [--dest /path/to/aurekai-standalone] [--push]
#
# The script:
#   1. Creates a fresh clone of the Aurekai/ subtree at $DEST
#   2. Rewrites history to contain only Aurekai/ paths (git-filter-repo)
#   3. Optionally pushes to origin github.com/aurekai/aurekai
#
# Prerequisites: git-filter-repo  (pip install git-filter-repo)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="${1:-$HOME/repos/aurekai}"
PUSH=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dest) DEST="$2"; shift 2 ;;
    --push) PUSH=1; shift ;;
    *) shift ;;
  esac
done

echo "==> Splitting Aurekai/ → $DEST"
echo "    Source: $REPO_ROOT"
echo ""

if ! command -v git-filter-repo >/dev/null 2>&1; then
  echo "ERROR: git-filter-repo is not installed or not on PATH"
  echo "Install: python3 -m pip install --user git-filter-repo"
  echo "Then: export PATH=\"$HOME/Library/Python/3.9/bin:$PATH\""
  exit 1
fi

if [[ -d "$DEST/.git" ]]; then
  echo "ERROR: $DEST already has a .git — remove it first."
  exit 1
fi

# 1. Clone bonfyre monorepo
git clone "$REPO_ROOT" "$DEST" --no-local

# 2. Filter to Aurekai/ subtree only, move root to top-level
cd "$DEST"
git filter-repo --subdirectory-filter Aurekai/ --force

# If there were no historical Aurekai commits in the source branch,
# seed from current working tree contents so publish can continue.
if ! git rev-parse --verify HEAD >/dev/null 2>&1; then
  echo "NOTICE: no historical Aurekai commits found after filter; seeding from working tree"
  find . -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +
  rsync -a \
    --exclude dist/ \
    --exclude node_modules/ \
    --exclude .git/ \
    "$REPO_ROOT/Aurekai/" ./
  chmod +x ./bin/akai.mjs 2>/dev/null || true
  git checkout -B main
  git add .
  git commit -m "Initial Aurekai public repo v0.8.0-alpha.1"
else
  git branch -M main
fi

# 3. Copy root-level files that belong in the public repo
cp "$REPO_ROOT/LICENSE" .
cp "$REPO_ROOT/CONTRIBUTING.md" .
git add LICENSE CONTRIBUTING.md 2>/dev/null || true
if ! git diff --cached --quiet; then
  git commit -m "Add top-level LICENSE and CONTRIBUTING"
fi

# 4. Set remote
git remote add origin "https://github.com/aurekai/aurekai.git" 2>/dev/null || \
  git remote set-url origin "https://github.com/aurekai/aurekai.git"

echo ""
echo "==> Standalone repo ready at $DEST"
echo "    Files:"
ls -1 "$DEST"
echo ""
echo "Next steps:"
echo "  cd $DEST"
echo "  git log --oneline | head"
echo "  # then when ready:"
echo "  git push -u origin main"

if [[ $PUSH -eq 1 ]]; then
  echo ""
  echo "==> Pushing to github.com/aurekai/aurekai ..."
  git push -u origin main
fi
