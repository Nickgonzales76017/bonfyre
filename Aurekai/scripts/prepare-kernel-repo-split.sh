#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/../aurekai-continuity-core"
FORCE="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --force)
      FORCE="true"
      shift 1
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -d "$OUT_DIR" ]] && [[ -n "$(ls -A "$OUT_DIR" 2>/dev/null || true)" ]] && [[ "$FORCE" != "true" ]]; then
  echo "Refusing to overwrite non-empty directory: $OUT_DIR"
  echo "Re-run with --force to replace it."
  exit 1
fi

if [[ "$FORCE" == "true" ]]; then
  if [[ -d "$OUT_DIR" && -n "$(ls -A "$OUT_DIR" 2>/dev/null || true)" ]]; then
    if [[ ! -f "$OUT_DIR/split-manifest.json" ]] || ! node -e 'const fs=require("node:fs"); const data=JSON.parse(fs.readFileSync(process.argv[1])); if(data.package!=="@aurekai/continuity-core")process.exit(1)' "$OUT_DIR/split-manifest.json"; then
      echo "Refusing --force on a directory not owned by the continuity-core split: $OUT_DIR" >&2
      exit 1
    fi
  fi
  rm -rf "$OUT_DIR"
fi

mkdir -p "$OUT_DIR/src" "$OUT_DIR/registry" "$OUT_DIR/schemas" "$OUT_DIR/scripts"

cp "$ROOT_DIR/kernel/src/index.mjs" "$OUT_DIR/src/index.mjs"
cp "$ROOT_DIR/kernel/src/state-runtime.mjs" "$OUT_DIR/src/state-runtime.mjs"
cp "$ROOT_DIR/kernel/src/trajectory-calculus.mjs" "$OUT_DIR/src/trajectory-calculus.mjs"
cp "$ROOT_DIR/kernel/src/transition-runtime.mjs" "$OUT_DIR/src/transition-runtime.mjs"
cp "$ROOT_DIR/kernel/src/trajectory-runtime.mjs" "$OUT_DIR/src/trajectory-runtime.mjs"
cp "$ROOT_DIR/kernel/src/claim-runtime.mjs" "$OUT_DIR/src/claim-runtime.mjs"
cp "$ROOT_DIR/kernel/src/policy-registry.mjs" "$OUT_DIR/src/policy-registry.mjs"
cp "$ROOT_DIR/kernel/src/chart-registry.mjs" "$OUT_DIR/src/chart-registry.mjs"
cp "$ROOT_DIR/kernel/src/schema-registry.mjs" "$OUT_DIR/src/schema-registry.mjs"
cp "$ROOT_DIR/src/integration-engine.mjs" "$OUT_DIR/src/integration-engine.mjs"
cp "$ROOT_DIR/src/integration-connectors.mjs" "$OUT_DIR/src/integration-connectors.mjs"
cp "$ROOT_DIR/src/policy-family-evaluator.mjs" "$OUT_DIR/src/policy-family-evaluator.mjs"
cp "$ROOT_DIR/src/residual-calibrator.mjs" "$OUT_DIR/src/residual-calibrator.mjs"
cp -R "$ROOT_DIR/registry/." "$OUT_DIR/registry/"
cp -R "$ROOT_DIR/schemas/." "$OUT_DIR/schemas/"

# Rewire engine import so the split repo is self-contained.
sed -i '' 's#"../kernel/src/trajectory-calculus.mjs"#"./trajectory-calculus.mjs"#g' "$OUT_DIR/src/integration-engine.mjs"
sed -i '' 's#"../../src/integration-engine.mjs"#"./integration-engine.mjs"#g' "$OUT_DIR/src/index.mjs"
sed -i '' 's#"../../src/integration-connectors.mjs"#"./integration-connectors.mjs"#g' "$OUT_DIR/src/index.mjs"

# Derive split package exports and version from their canonical package owner.
# Repeating them here lets the split silently omit a newly exported subpath.
node - "$ROOT_DIR/kernel/package.json" "$OUT_DIR/package.json" <<'NODE'
const fs = require('node:fs');
const [source, destination] = process.argv.slice(2);
const pkg = JSON.parse(fs.readFileSync(source, 'utf8'));
pkg.description = 'Standalone Aurekai continuity kernel';
pkg.engines = { node: '>=18' };
pkg.scripts = { ...pkg.scripts, verify: 'node ./scripts/verify-split.mjs' };
fs.writeFileSync(destination, JSON.stringify(pkg, null, 2) + '\n');
NODE

cat > "$OUT_DIR/scripts/verify-split.mjs" <<'EOF'
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const root = resolve(process.cwd());
const entry = pathToFileURL(resolve(root, "src/index.mjs")).href;

const mod = await import(entry);
const required = [
  "runKernelIntegration",
  "runKernelSurfaceBatch",
  "runKernelStateMachineBatch",
  "stateRuntime",
  "trajectoryCalculus",
  "transitionRuntime",
  "trajectoryRuntime",
  "claimRuntime",
  "policyRegistry",
  "chartRegistry",
  "schemaRegistry",
];

for (const key of required) {
  if (!(key in mod)) {
    throw new Error(`Missing export: ${key}`);
  }
}

console.log(JSON.stringify({
  ok: true,
  exports_verified: required,
  version: mod.CONTINUITY_CORE_VERSION,
}));
EOF

cat > "$OUT_DIR/README.md" <<'EOF'
# Aurekai Continuity Core

Standalone kernel package extracted from the monorepo.

## Verify

```bash
npm run verify
```

## API

- runKernelIntegration
- runKernelSurfaceBatch
- runKernelStateMachineBatch
- stateRuntime helpers
- trajectoryCalculus helpers
EOF

cat > "$OUT_DIR/split-manifest.json" <<EOF
{
  "generated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "source_repo": "$ROOT_DIR",
  "output_repo": "$OUT_DIR",
  "package": "@aurekai/continuity-core",
  "version": "0.1.0-alpha.0"
}
EOF

echo "Kernel split scaffold prepared at: $OUT_DIR"
