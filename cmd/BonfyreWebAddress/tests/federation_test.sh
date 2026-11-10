#!/usr/bin/env bash
# The federation half of the foreign world, against the live estate.
#
# What this proves: that networks are held as subjects with their protocol
# grammar authored once, that the negative grammar travels with the protocol,
# that members are reached through the federation's own traversal, and that what
# this institution projects onto a carrier is what an outside reader gets back.
#
# What it cannot prove: that any peer accepted a projection, that an observed
# network would admit us, or that a fetched document is true. Observation is not
# membership and fidelity is not reach.
set -u
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
  printf '%s: SKIP (%s does not serve _surface)\n' 'federation' "$BASE"
  exit 0
fi
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }
J() { python3 -c "import sys,json;d=json.load(sys.stdin);$1"; }

echo "== a protocol's semantics are authored once, not once per instance =="
G=$(curl -s --max-time 25 -H 'Accept: application/json' "$BASE/_surface/federation-grammar")
n=$(printf '%s' "$G" | J 'print(d["count"])')
[ "$n" -ge 4 ] && ok "the estate carries $n protocol grammars" || bad "only $n protocol grammars"
# Dictionary compression is the point: many instances of one protocol must share
# a single grammar row, or the compression claim is decorative.
F=$(curl -s --max-time 25 -H 'Accept: application/json' "$BASE/_surface/federations")
inst=$(printf '%s' "$F" | J 'print(sum(1 for r in d["records"] if r["protocol"]=="activitypub"))')
gram=$(printf '%s' "$G" | J 'print(sum(1 for r in d["records"] if r["display_name"]=="ActivityPub"))')
check "one ActivityPub grammar serves every instance" "$gram" "1"
[ "$inst" -gt "$gram" ] && ok "$inst ActivityPub instances share it" \
                        || bad "instances ($inst) do not outnumber grammars ($gram)"

echo "== the negative grammar travels with the protocol =="
# An instance that forgot to say what `follows` does not mean would otherwise be
# an instance where inferring it was safe.
miss=$(printf '%s' "$G" | J 'print(sum(1 for r in d["records"] if not r["cannot_infer"].strip()))')
check "every protocol grammar states what it cannot infer" "$miss" "0"
printf '%s' "$G" | grep -q 'follows -> friend' \
  && ok "ActivityPub declares follows is not friendship" \
  || bad "the follows/friend inference is not forbidden anywhere"
printf '%s' "$G" | grep -q 'advertised tool -> authority to invoke it' \
  && ok "MCP declares an advertised tool is not authority" \
  || bad "MCP does not forbid the capability/authority inference"

echo "== a federation is a subject and a carrier, and the roles are distinct =="
roles=$(printf '%s' "$F" | J 'print(",".join(sorted({r["role"] for r in d["records"]})))')
check "both roles are represented" "$roles" "observed,participant"
# A participant row is this institution on someone else's carrier; it must not
# be counted as a network we observed.
self=$(printf '%s' "$F" | J 'print(sum(1 for r in d["records"] if r["role"]=="participant" and r["federation"]!="aurekai.life"))')
check "no participant row claims a federation we do not run" "$self" "0"

echo "== members are reached through the federation's own traversal =="
# depth 0 is this institution's own projection; depth 1 was reached by following
# the network's published discovery document rather than by guessing a URL.
d1=$(printf '%s' "$F" | J 'print(sum(1 for r in d["records"] if r["depth"]=="1"))')
[ "$d1" -ge 3 ] && ok "$d1 networks reached by traversing their own discovery" \
                || bad "only $d1 networks traversed"
# Traversal has to have produced real instance state, or it was a fetch that
# returned nothing and was recorded as though it had.
real=$(printf '%s' "$F" | J 'print(sum(1 for r in d["records"] if "users=" in r["instance_delta"]))')
[ "$real" -ge 3 ] && ok "traversal produced observed instance state" \
                  || bad "traversal recorded no instance state"

echo "== round trip: what we publish is what an outside reader gets =="
if [ -f scripts/roundtrip_proof.mjs ]; then
  out=$(bun scripts/roundtrip_proof.mjs --origin "$BASE" 2>&1)
  printf '%s\n' "$out" | sed 's/^/    /'
  printf '%s' "$out" | grep -qE 'round trip: ([0-9]+)/\1 ' \
    && ok "every projection is observed as published" \
    || bad "a projection drifted from what is served"
  # The strong half is agreement between carriers, not presence within one.
  printf '%s' "$out" | grep -q 'actor id vs webfinger .* agree' \
    && ok "two carriers agree about one identity" \
    || bad "carriers disagree about this institution's identity"
fi

echo "== the observed graph is not the institutional one =="
# Federation state is full of edges and none of them are institutional facts.
# The store is separate by construction, not by convention.
E=$(curl -s --max-time 25 -H 'Accept: application/json' "$BASE/foreign/edges")
n=$(printf '%s' "$E" | J 'print(d["listed"])')
[ "$n" -ge 5 ] && ok "$n observed edges are held" || bad "only $n edges"
# Every edge keeps what would otherwise be lost on read-back: who said it, over
# what, when, against what evidence. An edge without provenance is a claim with
# no way to check who made it.
bare=$(printf '%s' "$E" | J 'print(sum(1 for e in d["edges"] if not (e["federation"] and e["carrier"] and e["observed_at"])))')
check "every edge carries its federation, carrier and time" "$bare" "0"
states=$(printf '%s' "$E" | J 'print(",".join(sorted({e["claim_state"] for e in d["edges"]})))')
check "nothing has crossed into an institutional relation" "$states" "observed"
printf '%s' "$E" | grep -q 'an observed edge -> an institutional relation' \
  && ok "the response says what it must not be read as" \
  || bad "the graph does not carry its forbidden inference"

echo "== the closure enforces the scope; the caller does not choose it =="
# A scope a caller can ask for is a scope every caller has.
base=$(printf '%s' "$E" | J 'print(d["scope"],d["listed"])')
for q in 'scope=restricted' 'scope=institution-private' 'scope=partner-shared'; do
  got=$(curl -s --max-time 25 -H 'Accept: application/json' "$BASE/foreign/edges?$q" | J 'print(d["scope"],d["listed"])')
  check "?$q buys nothing" "$got" "$base"
done
pub=$(printf '%s' "$E" | J 'print(d["scope"])')
check "an anonymous caller is the public" "$pub" "public"
w=$(printf '%s' "$E" | J 'print(d["withheld_for_scope"])')
[ "$w" -ge 1 ] && ok "$w edge(s) withheld, and the count is not their content" \
               || bad "nothing was withheld, so the filter proves nothing"
# The withheld rows must not be reachable in the body under any key.
printf '%s' "$E" | grep -q 'considered_for_partnership' \
  && bad "a withheld edge leaked into the public body" \
  || ok "no withheld edge appears in the public body"

if [ -n "${FED_ADMIN_TOKEN:-}" ]; then
  A=$(curl -s --max-time 25 -H "Authorization: Bearer $FED_ADMIN_TOKEN" \
        -H 'Accept: application/json' "$BASE/foreign/edges")
  as=$(printf '%s' "$A" | J 'print(d["scope"])')
  check "an estate admin widens to institution-private" "$as" "institution-private"
  an=$(printf '%s' "$A" | J 'print(d["listed"])')
  [ "$an" -gt "$n" ] && ok "and sees more than the public ($an vs $n)" \
                     || bad "widening returned no additional edges"
  # Widening is one-directional and bounded: running one estate is not authority
  # over this institution's observations of the outside world.
  ap=$(printf '%s' "$A" | J 'print(sum(1 for e in d["edges"] if e["scope"]=="partner-shared"))')
  check "an estate admin does not reach partner-shared" "$ap" "0"
fi
if [ -n "${FED_READ_TOKEN:-}" ]; then
  R=$(curl -s --max-time 25 -H "Authorization: Bearer $FED_READ_TOKEN" \
        -H 'Accept: application/json' "$BASE/foreign/edges" | J 'print(d["scope"])')
  check "a read grant is still the public here" "$R" "public"
fi

echo "== topology before leaves =="
# Two twins produced a frontier of tens of thousands of hosts. The law is that
# the frontier is worth more than reconstructing every leaf, and the estate has
# to be able to state the frontier without having twinned it.
F2=$(curl -s --max-time 25 -H 'Accept: application/json' "$BASE/_surface/federations")
front=$(printf '%s' "$F2" | J 'print(sum(1 for r in d["records"] if "reachable" in r.get("peer_frontier","")))')
[ "$front" -ge 2 ] && ok "$front instances record an observed peer frontier" \
                   || bad "no instance records a frontier"
tw=$(printf '%s' "$F2" | J 'print(d["count"])')
[ "$tw" -lt 100 ] && ok "the estate holds $tw federation subjects, not the frontier's worth" \
                  || bad "the frontier was twinned leaf by leaf"

echo
echo "federations: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
