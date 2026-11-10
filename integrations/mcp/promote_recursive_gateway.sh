#!/usr/bin/env bash
set -euo pipefail

ROOT="${BONFYRE_ROOT:-/opt/bonfyre}"
UNIT="${BONFYRE_MCP_SYSTEMD_UNIT:-bonfyre-mcp-gateway.service}"
TOKEN_FILE="${BONFYRE_MCP_TOKEN_FILE:-/etc/bonfyre/mcp.token}"
HOST="${BONFYRE_MCP_HOST:-127.0.0.1}"
PORT="${BONFYRE_MCP_PORT:-3052}"
HERE="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
OVERLAY="$HERE/recursive_overlay.mjs"
OVERLAY_TOOLS="$HERE/recursive_overlay_tools.mjs"
OVERLAY_CUE="$HERE/recursive_overlay.cue"
MATRIX_APERTURE="$HERE/matrix_aperture.mjs"
BOUNDED_PIPE="$HERE/bounded_pipe.mjs"
MATRIX_TOOLS="$HERE/matrix_tools.json"
CARRIERS="$HERE/carriers_contract.json"
CONTRACT="$HERE/recursive_closure_contract.mjs"
RICH_NATIVE_SOURCE="$HERE/../../mcp/rich-native-tools.mjs"
RICH_CATALOG="$HERE/../../mcp/rich-catalog.json"
FLEMING_PROJECTIONS="$HERE/../../mcp/fleming-projections.json"
ESTATE_SQL_GUARD="$HERE/../../mcp/estate-sql-guard.mjs"
DROPIN="$HERE/systemd/bonfyre-mcp-gateway.service.d/recursive-closure.conf"
DROPIN_DST="/etc/systemd/system/${UNIT}.d/recursive-closure.conf"

die() { printf 'recursive MCP promotion: %s\n' "$*" >&2; exit 1; }
for command in bun cue systemctl install; do command -v "$command" >/dev/null || die "missing $command"; done

[[ -f "$OVERLAY" ]] || die "missing overlay"
[[ -f "$OVERLAY_TOOLS" && -f "$OVERLAY_CUE" && -f "$MATRIX_APERTURE" && -f "$BOUNDED_PIPE" ]] || die "missing native overlay dependency"
[[ -f "$MATRIX_TOOLS" ]] || die "missing Matrix tool registry"
[[ -f "$CARRIERS" ]] || die "missing carrier registry"
[[ -f "$CONTRACT" ]] || die "missing contract"
[[ -f "$DROPIN" ]] || die "missing systemd drop-in"
[[ -f "$RICH_NATIVE_SOURCE" && -f "$RICH_CATALOG" && -f "$FLEMING_PROJECTIONS" && -f "$ESTATE_SQL_GUARD" ]] || die "native rich owner module or descriptor missing"
[[ -x /usr/local/bin/bonfyre-web-address ]] || die "BonfyreWebAddress missing"
[[ -f /var/lib/bonfyre/matrix.db ]] || die "Matrix DB missing"
[[ -r "$TOKEN_FILE" ]] || die "MCP token unreadable"
bun test "$HERE/tests/recursive_overlay.test.mjs" "$HERE/../../mcp/tests/rich-native-tools.test.mjs" "$HERE/../../mcp/tests/estate-sql-guard.test.mjs" >/dev/null
cue vet "$OVERLAY_CUE"

backup=""
installed=0
committed=0
base_was_active=0
base_was_enabled=0
systemctl is-active --quiet bonfyre-mcp-rich-base.service && base_was_active=1
systemctl is-enabled --quiet bonfyre-mcp-rich-base.service && base_was_enabled=1
rollback() {
  code=$?
  if (( code != 0 && installed == 1 && committed == 0 )); then
    if (( base_was_enabled == 1 )); then systemctl enable bonfyre-mcp-rich-base.service || true; fi
    if (( base_was_active == 1 )); then systemctl start bonfyre-mcp-rich-base.service || true; fi
    if [[ -n "$backup" ]]; then install -D -m 0644 "$backup" "$DROPIN_DST"; else rm -f "$DROPIN_DST"; fi
    systemctl daemon-reload || true
    systemctl restart "$UNIT" || true
  fi
  [[ -n "$backup" ]] && rm -f "$backup"
  exit "$code"
}
trap rollback EXIT

if [[ -f "$DROPIN_DST" ]]; then backup="$(mktemp)"; cp "$DROPIN_DST" "$backup"; fi
install -D -m 0755 "$OVERLAY" "$ROOT/integrations/mcp/recursive_overlay.mjs"
install -D -m 0644 "$OVERLAY_TOOLS" "$ROOT/integrations/mcp/recursive_overlay_tools.mjs"
install -D -m 0644 "$OVERLAY_CUE" "$ROOT/integrations/mcp/recursive_overlay.cue"
install -D -m 0644 "$MATRIX_APERTURE" "$ROOT/integrations/mcp/matrix_aperture.mjs"
install -D -m 0644 "$BOUNDED_PIPE" "$ROOT/integrations/mcp/bounded_pipe.mjs"
install -D -m 0644 "$MATRIX_TOOLS" "$ROOT/integrations/mcp/matrix_tools.json"
install -D -m 0644 "$CARRIERS" "$ROOT/integrations/mcp/carriers_contract.json"
install -D -m 0644 "$CONTRACT" "$ROOT/integrations/mcp/recursive_closure_contract.mjs"
install -D -m 0644 "$RICH_NATIVE_SOURCE" "$ROOT/mcp/rich-native-tools.mjs"
install -D -m 0644 "$RICH_CATALOG" "$ROOT/mcp/rich-catalog.json"
install -D -m 0644 "$FLEMING_PROJECTIONS" "$ROOT/mcp/fleming-projections.json"
install -D -m 0644 "$ESTATE_SQL_GUARD" "$ROOT/mcp/estate-sql-guard.mjs"
cue export "$OVERLAY_CUE" -e registry -o "$ROOT/integrations/mcp/recursive_overlay.registry.json"
install -D -m 0644 "$DROPIN" "$DROPIN_DST"
installed=1
systemctl daemon-reload
systemctl restart "$UNIT"
systemctl is-active --quiet "$UNIT" || die "$UNIT inactive"

# systemd reports active the moment the process is spawned, but this gateway
# still has to import the rich mesh and bind its socket. Verifying straight
# after the restart raced that, so a correct promotion failed with connection
# refused and rolled itself back. Wait for the port to actually answer, and
# treat never answering as its own failure rather than as a missing tool.
ready=0
for _ in $(seq 1 60); do
    if (exec 3<>"/dev/tcp/$HOST/$PORT") 2>/dev/null; then exec 3>&- 3<&-; ready=1; break; fi
    sleep 0.5
done
[[ "$ready" -eq 1 ]] || die "$UNIT never began listening on $HOST:$PORT"

bun "$HERE/tests/recursive_overlay_smoke.mjs" "$TOKEN_FILE" "$HOST" "$PORT"
if (( base_was_active == 1 || base_was_enabled == 1 )); then
  systemctl disable --now bonfyre-mcp-rich-base.service
  bun "$HERE/tests/recursive_overlay_smoke.mjs" "$TOKEN_FILE" "$HOST" "$PORT"
fi

committed=1
trap - EXIT
[[ -n "$backup" ]] && rm -f "$backup"
printf 'recursive MCP promotion: PASS (%s -> %s:%s)\n' "$UNIT" "$HOST" "$PORT"
