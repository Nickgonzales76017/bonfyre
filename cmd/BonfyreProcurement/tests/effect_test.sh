#!/bin/sh
set -eu
bin="$1";root=$(cd "$(dirname "$0")/../.." && pwd);sign="$root/BonfyreSign/bonfyre-sign"
make -s -C "$root/BonfyreSign" bonfyre-sign >/dev/null
tmp=$(mktemp -d);trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/key"
"$sign" keygen --key-dir "$tmp/key" >/dev/null
pub=$(cat "$tmp/key/institution.ed25519.public");seller="ed25519:$pub"
order=$(printf 'a%.0s' $(seq 1 64));quote=$(printf 'b%.0s' $(seq 1 64))
exchange=$(python3 - "$order" "$quote" "$seller" <<'PY'
import hashlib,json,sys
order,quote,seller=sys.argv[1:]
value={"order_intent":order,"quote":quote,"capability":"capability:test-widget","buyer":"buyer:test","seller":seller,"settlement":"settlement:test","deliverer":"deliverer:test"}
raw=json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()
print("procurement:"+hashlib.sha256(raw).hexdigest()[:48])
PY
)
cat >"$tmp/acceptance" <<EOF
bonfyre.procurement.supplier-acceptance.v1
exchange=$exchange
order_intent_digest=$order
quote_id=quote-test-001
quote_digest=$quote
supplier_id=$seller
capability=capability:test-widget
buyer=buyer:test
seller=$seller
settlement=settlement:test
deliverer=deliverer:test
amount_cents=21000
currency=USD
accepted_at=2026-10-23T22:00:00Z
EOF
att=$("$sign" sign --key-dir "$tmp/key" --file "$tmp/acceptance")
sig=$(printf '%s' "$att"|sed -n 's/.*"signature":"\([a-f0-9]*\)".*/\1/p')
sqlite3 "$tmp/auth.db" 'CREATE TABLE authority_edges(edge_id TEXT PRIMARY KEY,actor TEXT NOT NULL,permission TEXT NOT NULL,subject TEXT NOT NULL,purpose TEXT NOT NULL DEFAULT "",scope TEXT NOT NULL DEFAULT "",effective_from TEXT,expires_at TEXT,delegable INTEGER NOT NULL DEFAULT 0,revoked INTEGER NOT NULL DEFAULT 0,evidence TEXT NOT NULL DEFAULT "",originating_authority TEXT NOT NULL DEFAULT "",created_at TEXT NOT NULL);'
run(){ "$bin" enact --commitments-db "$tmp/commitments.db" --authority-db "$tmp/auth.db" --exchange "$exchange" --order-intent-digest "$order" --quote-id quote-test-001 --quote-digest "$quote" --supplier-id "$seller" --capability capability:test-widget --buyer buyer:test --seller "$seller" --settlement settlement:test --deliverer deliverer:test --amount-cents 21000 --currency USD --signature "$1" --at 2026-10-23T22:00:00Z; }
if run "$sig" >"$tmp/out" 2>"$tmp/err";then echo "effect accepted without authority" >&2;exit 1;fi
grep -q 'authority missing' "$tmp/err"
for row in "buyer:test|request:capability" "$seller|quote:capability" "settlement:test|accept:quote" "deliverer:test|deliver:capability";do
 actor=$(printf '%s' "$row"|cut -d'|' -f1);perm=$(printf '%s' "$row"|cut -d'|' -f2)
 sqlite3 "$tmp/auth.db" "INSERT INTO authority_edges(edge_id,actor,permission,subject,purpose,scope,created_at) VALUES(lower(hex(randomblob(16))),'$actor','$perm','$exchange','procurement','self','2026-10-23T21:00:00Z');"
done
run "$sig" >"$tmp/out"
grep -q '"supplier_identity_binding_verified":true' "$tmp/out";grep -q '"supplier_acceptance_verified":true' "$tmp/out";grep -q '"protocol_enacted":true' "$tmp/out";grep -q '"opened_count":2' "$tmp/out"
test "$(sqlite3 "$tmp/commitments.db" 'select count(*) from commitments')" = 2
test "$(sqlite3 "$tmp/commitments.db" 'select count(*) from commitment_events where state="open"')" = 2
run "$sig" >"$tmp/replay";grep -q '"replayed":true' "$tmp/replay"
bad=$(printf '%0128d' 0);if run "$bad" >/dev/null 2>&1;then echo "effect accepted invalid signature" >&2;exit 1;fi
if "$bin" enact --commitments-db "$tmp/other.db" --authority-db "$tmp/auth.db" --exchange "$exchange" --order-intent-digest "$order" --quote-id quote-test-001 --quote-digest "$quote" --supplier-id supplier:unbound --capability capability:test-widget --buyer buyer:test --seller "$seller" --settlement settlement:test --deliverer deliverer:test --amount-cents 21000 --currency USD --signature "$sig" --at 2026-10-23T22:00:00Z >/dev/null 2>&1;then echo "effect accepted unbound supplier identity" >&2;exit 1;fi
echo "procurement native effect: signed acceptance, authority, replay and refusal ok"
