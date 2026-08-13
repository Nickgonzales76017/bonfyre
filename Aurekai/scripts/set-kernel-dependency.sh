#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PKG_JSON="$ROOT_DIR/package.json"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 --local | --published <version>"
  exit 2
fi

MODE="$1"
case "$MODE" in
  --local)
    SPEC="file:../aurekai-continuity-core"
    ;;
  --published)
    if [[ $# -lt 2 ]]; then
      echo "Missing version. Usage: $0 --published <version>"
      exit 2
    fi
    VERSION="$2"
    SPEC="$VERSION"
    ;;
  *)
    echo "Unknown mode: $MODE"
    exit 2
    ;;
esac

node -e "
const fs = require('fs');
const path = process.argv[1];
const spec = process.argv[2];
const pkg = JSON.parse(fs.readFileSync(path, 'utf8'));
pkg.dependencies = pkg.dependencies || {};
pkg.dependencies['@aurekai/continuity-core'] = spec;
fs.writeFileSync(path, JSON.stringify(pkg, null, 2) + '\\n');
" "$PKG_JSON" "$SPEC"

cd "$ROOT_DIR"
npm install --silent

echo "Set @aurekai/continuity-core dependency to: $SPEC"
