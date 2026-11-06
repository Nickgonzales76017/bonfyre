#!/bin/sh
# The Python lane that still runs the operator's goals must stop asking a
# provider that has said it has no credit, and must not retry a failing goal
# without limit. Exercised against a scratch database with its own interpreter.
set -eu
runtime=${BONFYRE_LEGACY_RUNTIME:-"$HOME/.bonfyre/agents-runtime"}
py="$runtime/venv/bin/python"
[ -x "$py" ] && [ -f "$runtime/operator_agents_runtime.py" ] || { echo "legacy runtime provider hold: SKIP (runtime not installed here)"; exit 0; }
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT INT TERM
mkdir -p "$tmp/home" "$tmp/op"
printf '{"autonomous": true, "max_cycles_per_hour": 100000, "retry_seconds": 900, "max_goal_attempts": 3, "provider_hold_seconds": 3600}' > "$tmp/home/policy.json"
BONFYRE_AGENTS_HOME="$tmp/home" BONFYRE_AGENTS_POLICY="$tmp/home/policy.json" BONFYRE_OPERATOR_DB="$tmp/op/operator.sqlite3" PYTHONPATH="$runtime" "$py" - <<'PY'
import asyncio, json, time
import operator_agents_runtime as r

r.poll_events = lambda: None
calls = []
def goal(title, priority):
    c = r.connect_db(); now = time.time()
    c.execute("INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,recurring,interval_seconds,retry_at,attempts,last_error,created_at,updated_at) VALUES(?,?,?,?,?, 'queued','test',0,0,0,0,'',?,?)", (title, title, title, "x", priority, now, now)); c.commit(); c.close()
def row(title):
    c = r.connect_db(); g = dict(c.execute("SELECT state,retry_at,attempts,last_error FROM goals WHERE id=?", (title,)).fetchone()); c.close(); return g
goal("a", 90); goal("b", 80)

async def no_credit(g): calls.append(g["id"]); raise RuntimeError("Error code: 429 - {'error': {'message': 'You have no credits remaining.', 'type': 'insufficient_quota', 'code': 'credit_balance_exhausted'}}")
r.run_goal = no_credit
assert asyncio.run(r.run_once()) is True
held = r.provider_hold()
assert held and held["cause"] in ("you have no credit", "insufficient_quota", "credit_balance_exhausted") and held["refusals"] == 1, held
a = row("a")
assert a["state"] == "retry" and a["attempts"] == 0 and a["retry_at"] > time.time() + 3000, a      # the goal is not charged for the provider's refusal
for _ in range(50): assert asyncio.run(r.run_once()) is False                                          # nothing is claimed while the hold stands
assert calls == ["a"], calls                                                                           # one request, not fifty-one
assert row("b")["attempts"] == 0

# The hold lapses: exactly one goal tries again, and a second refusal extends the same hold.
held["until"] = time.time() - 1; r.meta_set("provider_hold", json.dumps(held))
c = r.connect_db(); c.execute("UPDATE goals SET retry_at=0"); c.commit(); c.close()
assert asyncio.run(r.run_once()) is True and len(calls) == 2
again = r.provider_hold(); assert again["refusals"] == 2 and again["since"] == held["since"], again
assert asyncio.run(r.run_once()) is False and len(calls) == 2

# An ordinary failure is the goal's own: it waits longer each time and is blocked at the limit.
r.meta_set("provider_hold", "{}")
async def broken(g): calls.append(g["id"]); raise ValueError("tool returned nonsense")
r.run_goal = broken
waits = []
for _ in range(3):
    c = r.connect_db(); c.execute("UPDATE goals SET retry_at=0 WHERE id='a'"); c.execute("UPDATE goals SET retry_at=9e12 WHERE id='b'"); c.commit(); c.close()
    before = time.time(); assert asyncio.run(r.run_once()) is True
    g = row("a"); waits.append(round(g["retry_at"] - before) if g["state"] == "retry" else g["state"])
assert waits == [900, 1800, "blocked"], waits
assert row("a")["attempts"] == 3 and not r.provider_hold()
assert r.terminal_provider_cause("Incorrect API key provided") == "incorrect api key" and r.terminal_provider_cause("timeout") == ""
print("legacy runtime provider hold: ok")
PY
