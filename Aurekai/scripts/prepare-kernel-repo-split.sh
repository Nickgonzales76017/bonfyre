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

cat > "$OUT_DIR/package.json" <<'EOF'
{
  "name": "@aurekai/continuity-core",
  "version": "0.1.0-alpha.0",
  "description": "Standalone Aurekai continuity kernel",
  "type": "module",
  "license": "MIT",
  "exports": {
    ".": "./src/index.mjs",
    "./state": "./src/state-runtime.mjs",
    "./transition": "./src/transition-runtime.mjs",
    "./trajectory": "./src/trajectory-runtime.mjs",
    "./trajectory-calculus": "./src/trajectory-calculus.mjs",
    "./claim": "./src/claim-runtime.mjs",
    "./policy": "./src/policy-registry.mjs",
    "./chart": "./src/chart-registry.mjs",
    "./schema-registry": "./src/schema-registry.mjs",
    "./state-runtime": "./src/state-runtime.mjs"
  },
  "engines": {
    "node": ">=18"
  },
  "scripts": {
    "verify": "node ./scripts/verify-split.mjs"
  }
}
EOF

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
