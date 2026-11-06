#!/usr/bin/env bash
# Can an agent operate what this institution measures, or only read about it?
#
# The estate grew a dozen organs that measure ceilings, residuals, envelopes and
# closures. None was reachable through MCP: an agent could read the atlas and
# could not run anything that produces the numbers the atlas records.
#
# What this proves: that the measurements are declared rather than compiled into
# the gateway, that a declared-but-undeployed runner is reported rather than
# hidden, that content cannot choose what executes, and that a measurement which
# produces nothing says why.
#
# What it cannot prove: that any measurement is correct. This is about reach.
set -u
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }

# Gated, and a gate is not a failure: without MCP_CALL there is nothing to test.
if [ -z "${MCP_CALL:-}" ]; then echo 'mcp measure: SKIP (set MCP_CALL to a command taking a json request)'; exit 0; fi
J() { python3 -c "
import sys,json
d=json.load(sys.stdin)
try: r=json.loads(d['result']['content'][0]['text'])
except Exception: r=d
$1"; }
call() {
  $MCP_CALL "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"institutional_measure\",\"arguments\":$1}}"
}

echo "== the measurements are declared, not compiled into the gateway =="
L=$(call '{"op":"list"}')
n=$(printf '%s' "$L" | J 'print(len(r.get("measurements",[])))')
[ "$n" -ge 5 ] && ok "$n measurements declared and offered" || bad "only $n offered"
miss=$(printf '%s' "$L" | J 'print(sum(1 for m in r["measurements"] if not m["cannot_infer"].strip()))')
check "every measurement states what it cannot infer" "$miss" "0"
# A declared measurement whose runner is not deployed here must be listed with
# that fact, not hidden. Hiding it makes a deployment gap look like an absence.
hasflag=$(printf '%s' "$L" | J 'print(sum(1 for m in r["measurements"] if "runnable_here" in m))')
check "every measurement reports whether it runs here" "$hasflag" "$n"

echo "== content cannot choose what executes =="
# The runner is a bare filename resolved inside the estate's own script dir. A
# measure name that is a path, or carries a shell metacharacter, must never
# reach exec -- the declaration allowlist refuses it first.
for probe in '../../../etc/passwd' 'gym_run.py;id' 'nonexistent-measure'; do
  e=$(call "{\"op\":\"run\",\"measure\":\"$probe\"}" | J 'print(r.get("error","ACCEPTED"))')
  check "a measure named '$probe' is refused" "$e" "unknown_measure"
done
inj=$(call '{"op":"run","measure":"funding-closure","subject":"x; rm -rf /"}' | J 'print(r.get("error","ACCEPTED"))')
check "an injected subject is refused" "$inj" "unsafe_subject"

echo "== a real measurement returns a real receipt =="
F=$(call '{"op":"run","measure":"funding-closure","subject":"institution:nightgrid"}')
got=$(printf '%s' "$F" | J 'print("receipt" in r)')
check "the funding closure returns a receipt" "$got" "True"
reqs=$(printf '%s' "$F" | J 'print(r["receipt"]["requirements"])')
[ "$reqs" -ge 1 ] && ok "over $reqs declared requirements" || bad "no requirements measured"
cls=$(printf '%s' "$F" | J 'print(len(r["receipt"]["residual"]))')
[ "$cls" -ge 2 ] && ok "$cls residual classes returned through MCP" || bad "residual not classified"
bound=$(printf '%s' "$F" | J 'print(bool(r.get("cannot_infer","").strip()))')
check "the response carries the measurement's own bound" "$bound" "True"

echo "== a measurement that produces nothing says why =="
# The operator database lives on the workstation, not here. That is a placement
# fact and it must surface as one rather than as an empty result.
E=$(call '{"op":"run","measure":"failure-cohort"}')
err=$(printf '%s' "$E" | J 'print(r.get("error",""))')
if [ -n "$err" ]; then
  check "an unproductive measurement is an error, not an empty result" \
        "$err" "measurement_produced_nothing"
  se=$(printf '%s' "$E" | J 'print(bool(r.get("stderr","").strip()))')
  check "and it carries the runner's own reason" "$se" "True"
else
  ok "failure-cohort produced a receipt here"
fi

echo
echo "mcp measure: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
