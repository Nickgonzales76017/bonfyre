#!/usr/bin/env bash
# Pinned, isolated REA workstation adapter. Never scans or uploads an estate.
# Keep analyzer state/credentials distinct from Bonfyre's production runtime.
set -euo pipefail
umask 077
if [ "$#" -eq 0 ]; then set -- help; fi
action="$1"; shift
version=6.4.0
root="$HOME/.local/share/bonfyre/rea"
if [ -n "$*" ] && [ "$action" = install ]; then
  echo "install accepts no implicit analysis targets" >&2; exit 2
fi
if [ -n "$(printenv BONFYRE_REA_ROOT || true)" ]; then root="$BONFYRE_REA_ROOT"; fi
prefix="$root/releases/$version"
cli="$prefix/node_modules/rea-agents/scripts/rea.mjs"

need_node() {
  command -v node >/dev/null || { echo "Node is required: 22.19+, 24.11+ or 26+" >&2; exit 2; }
  node -e 'const [a,b]=process.versions.node.split(".").map(Number);
    if(!((a===22&&b>=19)||(a===24&&b>=11)||(a>=26))) {
      console.error("REA 6.4.0 requires Node 22.19+, 24.11+, or 26+: found "+process.version);
      process.exit(2);
    }'
}
need_rea() {
  need_node
  if [ ! -f "$cli" ] || [ -L "$cli" ]; then
    echo "REA 6.4.0 not installed in isolated prefix. Run: bash scripts/rea-local.sh install" >&2
    exit 2
  fi
}
rea() { node "$cli" "$@"; }

case "$action" in
  install)
    need_node
    command -v npm >/dev/null || { echo "npm is required" >&2; exit 2; }
    if [ -L "$root" ] || [ -L "$root/releases" ] || [ -L "$prefix" ]; then
      echo "refusing symlinked REA installation root" >&2; exit 2
    fi
    mkdir -p "$root/releases" "$root/npm-cache"
    if [ -e "$prefix" ]; then
      need_rea
      echo "REA $version already installed at $prefix"
      exit 0
    fi
    stage="$(mktemp -d "$root/releases/.rea-stage.XXXXXXXX")"
    cleanup() { if [ -n "$stage" ] && [ -d "$stage" ]; then rm -rf -- "$stage"; fi; }
    trap cleanup EXIT
    # Exact release, private prefix/cache. No global npm install, sudo, app
    # credentials, REA setup registration, or automatic target analysis.
    npm_config_cache="$root/npm-cache" npm install --prefix "$stage" \
      --no-audit --no-fund --no-save --no-package-lock \
      "rea-agents@$version"
    test -f "$stage/node_modules/rea-agents/scripts/rea.mjs"
    node "$stage/node_modules/rea-agents/scripts/rea.mjs" --help >/dev/null
    mv "$stage" "$prefix"
    stage=""
    printf 'REA %s installed in %s\n' "$version" "$prefix"
    ;;
  smoke)
    need_rea
    mkdir -p "$root"
    scratch="$(mktemp -d "$root/.rea-smoke.XXXXXXXX")"
    trap 'rm -rf -- "$scratch"' EXIT
    mkdir -p "$scratch/fixture/src"
    printf '%s\n' '{"name":"aurekai-rea-local-fixture","private":true,"type":"module","main":"src/index.mjs"}' > "$scratch/fixture/package.json"
    printf '%s\n' "export function renderWindow(frame){return {schema:'aurekai.creator.render-program.v1',frame};}" > "$scratch/fixture/src/index.mjs"
    rea analyze-javascript-application "$scratch/fixture" --json > "$scratch/evidence.json"
    node -e 'const fs=require("node:fs");let o=JSON.parse(fs.readFileSync(process.argv[1],"utf8"));
      if(!o||typeof o.evidence_id!=="string"||!o.evidence_id.startsWith("ev_"))process.exit(1);
      process.stdout.write(JSON.stringify({rea_version:"6.4.0",state:"static-evidence-verified",
        evidence_id:o.evidence_id,scope:"synthetic-local-only"})+"\n");' "$scratch/evidence.json"
    ;;
  doctor)
    need_rea
    printf 'REA prefix: %s\nNode: %s\n' "$prefix" "$(node --version)"
    if ! rea doctor --json; then
      echo "REA doctor reported an unavailable optional capability (check provider configuration)" >&2
    fi
    ;;
  targets)
    # Only existence/readability checks. No application launched or inspected.
    for app in \
      "/Applications/FL Studio.app" \
      "/Applications/Ableton Live 12 Suite.app" \
      "/Applications/Ableton Live 12 Standard.app" \
      "/Applications/Bitwig Studio.app" \
      "/Applications/REAPER.app" \
      "/Applications/Logic Pro.app" \
      "/Applications/Blender.app" \
      "/Applications/DaVinci Resolve/DaVinci Resolve.app" \
      "/Applications/Adobe Premiere Pro 2026/Adobe Premiere Pro 2026.app" \
      "/Applications/Adobe After Effects 2026/Adobe After Effects 2026.app" \
      "/Applications/OBS.app"; do
      if [ -d "$app" ]; then printf 'available %s\n' "$app"
      else printf 'unavailable %s\n' "$app"; fi
    done
    ;;
  static-js)
    need_rea
    if [ "$#" -ne 1 ] || [ ! -d "$1" ]; then
      echo "usage: bash scripts/rea-local.sh static-js /explicit/app-directory" >&2; exit 2
    fi
    rea analyze-javascript-application "$1" --json
    ;;
  native)
    need_rea
    if [ "$#" -ne 2 ] || [ ! -e "$2" ]; then
      echo "usage: bash scripts/rea-local.sh native ghidra|hopper /explicit/binary-or-app" >&2; exit 2
    fi
    case "$1" in ghidra|hopper) ;; *) echo "provider must be ghidra or hopper" >&2; exit 2;; esac
    # Explicit, caller-owned target: never default to /Applications or private estate.
    rea inspect "$2" --provider "$1" --json
    ;;
  setup-preview)
    need_rea
    rea setup --client codex --skill=false --dry-run --json
    ;;
  rea)
    need_rea
    if [ "$#" -lt 1 ]; then echo "provide explicit REA CLI arguments" >&2; exit 2; fi
    rea "$@"
    ;;
  help|*)
    cat <<'HELP'
Bonfyre-local REA 6.4.0 (pinned, isolated; no global npm or auto-inspection)
  bash scripts/rea-local.sh install
  bash scripts/rea-local.sh smoke
  bash scripts/rea-local.sh doctor
  bash scripts/rea-local.sh targets
  bash scripts/rea-local.sh static-js /explicit/authorized/source-directory
  bash scripts/rea-local.sh native ghidra|hopper /explicit/authorized/binary-or-app
  bash scripts/rea-local.sh setup-preview
  bash scripts/rea-local.sh rea providers --json
REA static analysis is not behavioral equivalence or execution authorization.
Hopper and Ghidra require independent installation and compatible licenses.
HELP
    ;;
esac
