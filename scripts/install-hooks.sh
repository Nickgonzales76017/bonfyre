#!/usr/bin/env bash
# Point git at the repo's tracked hooks so the atlas stays valid + fresh on every
# commit. Run once per clone. Idempotent.
set -euo pipefail
root="$(git rev-parse --show-toplevel)"
git -C "$root" config core.hooksPath .githooks
chmod +x "$root/.githooks/"* 2>/dev/null || true
echo "✓ core.hooksPath = .githooks (atlas validate + export run on every commit)"
