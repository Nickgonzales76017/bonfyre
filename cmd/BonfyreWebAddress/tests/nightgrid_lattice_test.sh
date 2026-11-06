#!/usr/bin/env bash
# The Night Grid persona lattice: each persona reaches exactly its own authority.
#
# Before these accounts existed the estate had two authority states to test --
# a superuser holding every grant, and probe accounts holding none. Every
# interesting authority failure lives between those, including the one found
# earlier in this estate where any authenticated session could read the
# institution's economics.
#
# Credentials come from the provisioning run, not from this file. Point
# PERSONA_FILE at it; the file is chmod 600 and outside the repository.
#
# Cannot infer: passing is not a claim the lattice is the right lattice. It is
# a claim that the granted authority matches the declared one, and that a
# persona cannot reach past it.
set -uo pipefail
BASE="${BASE:-https://aurekai.life}"

# The subject of this test is a deployed estate, not this checkout. When that
# estate cannot be reached -- it is down, or behind a rate limit -- there is
# nothing here to be right or wrong about, and reporting FAIL points the
# reader at code that is fine.
# Two things have to hold: the estate answers at all, and it exposes the
# _surface projections these assertions read. The edge that serves the
# published pages does not serve _surface, so a 200 on /healthz there is
# not the estate this test is about. curl exits 0 on a 429 and on a 404,
# so the status itself is the test.
if [ "$(curl -sS --max-time 12 -o /dev/null -w '%{http_code}' "$BASE/healthz" 2>/dev/null)" != "200" ] \
   || [ "$(curl -sS --max-time 12 -o /dev/null -w '%{http_code}' "$BASE/_surface/routes" 2>/dev/null)" = "404" ]; then
  printf '%s: SKIP (%s does not serve _surface)\n' 'nightgrid lattice' "$BASE"
  exit 0
fi
FILE="${PERSONA_FILE:-$HOME/Documents/Bonfyre/NIGHTGRID-PERSONAS.md}"
pass=0; fail=0
ok()  { pass=$((pass+1)); printf '  ok   %s\n' "$1"; }
bad() { fail=$((fail+1)); printf '  FAIL %s\n' "$1"; }

[ -f "$FILE" ] || { echo "no persona file at $FILE; run scripts/provision_nightgrid_personas.sh" >&2; exit 1; }

# macOS ships bash 3.2, which has no associative arrays, so tokens are kept in
# a temp directory keyed by persona rather than in declare -A.
cred() { awk -F'|' -v p="$1" '{gsub(/[ `]/,"",$2); gsub(/[ `]/,"",$3); gsub(/[ `]/,"",$4); if ($2==p) {print $3" "$4; exit}}' "$FILE"; }
TOKDIR=$(mktemp -d); trap 'rm -rf "$TOKDIR"' EXIT
tok_set() { printf '%s' "$2" > "$TOKDIR/$1"; }
tok()     { [ -f "$TOKDIR/$1" ] && cat "$TOKDIR/$1" || printf ''; }

login() {
  local email="$1" password="$2"
  printf '{"email":"%s","password":"%s"}' "$email" "$password" \
    | curl -s --max-time 20 -X POST "$BASE/login" -H 'Content-Type: application/json' -H 'Accept: application/json' -d @- 2>/dev/null \
    | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("session",{}).get("token") or d.get("token",""))' 2>/dev/null
}

echo "== every persona authenticates =="
for persona in organizer venue creator sponsor editor safety member machine; do
  line=$(cred "$persona")
  email=${line%% *}; password=${line#* }
  if [ -z "$line" ] || [ -z "$password" ]; then bad "$persona has no recorded credential"; continue; fi
  t=$(login "$email" "$password")
  if [ -n "$t" ]; then tok_set "$persona" "$t"; ok "$persona signs in"; else bad "$persona cannot sign in"; fi
done

echo "== a read persona cannot reach the institution's interior =="
if [ -n "$(tok member)" ]; then
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $(tok member)" "$BASE/studio/v1/economy/budget")
  [ "$code" = "403" ] && ok "member is refused institutional economics ($code)" || bad "member reached economics with $code"
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $(tok member)" "$BASE/studio/v1/profile")
  [ "$code" = "200" ] && ok "member still reads its own closure" || bad "member cannot read its own profile ($code)"
fi

echo "== a write persona reaches the admitted surface a stranger cannot =="
if [ -n "$(tok venue)" ]; then
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $(tok venue)" "$BASE/_surface/night-work")
  anon=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' "$BASE/_surface/night-work")
  [ "$anon" = "403" ] && ok "the institutional projection refuses a stranger" || bad "anonymous got $anon"
  [ "$code" = "200" ] && ok "an admitted persona reads it ($code)" || bad "admitted persona got $code"
fi

echo "== the institutional role discriminates, not merely the session =="
# venue holds nightgrid:write and member holds nightgrid:read. Both are valid
# sessions in the same institution, so if the surface let member through it
# would be checking "is authenticated here" rather than "holds this role".
if [ -n "$(tok member)" ]; then
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $(tok member)" "$BASE/_surface/night-work")
  [ "$code" = "403" ] && ok "a read-role member of the same institution is refused ($code)" \
                      || bad "read-role member reached a write-role projection with $code"
fi
if [ -n "$(tok organizer)" ]; then
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $(tok organizer)" "$BASE/_surface/night-work")
  [ "$code" = "200" ] && ok "an admin-role persona satisfies a write requirement ($code)" \
                      || bad "admin role did not satisfy write; the ladder is not ordered ($code)"
fi

echo "== no persona inherits another institution =="
for persona in organizer venue member; do
  [ -z "$(tok "$persona")" ] && continue
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $(tok "$persona")" "$BASE/studio/v1/frappe-lineages")
  [ "$code" = "403" ] && ok "$persona cannot read another institution's interior" || bad "$persona reached frappe-lineages with $code"
done

echo
echo "nightgrid lattice: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
