#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# BONFYRE / AUREKAI — CLAUDE CAPITAL MANAGER
#
# Live sidecar for an ALREADY RUNNING Bonfyre Codex Capital Gym.
#
# DO NOT STOP THE CODEX RUN TO INSTALL OR START THIS.
#
# Hierarchy:
#   Claude = capital governor / allocator / critic
#   Codex  = execution engine
#   Shell  = continuity + scheduling
#
# Every 10 newly completed successful Codex turns, Claude performs an
# independent management review. Claude's only job is to get more economic
# value from the remaining Codex capacity.
#
# The manager:
#   - NEVER waits on itself before Codex may continue
#   - NEVER kills/stops the active capital-gym supervisor
#   - injects orders through STATE.md, which Codex already reads every turn
#   - can reprioritize, kill weak lanes, reprice, repackage, change budgets,
#     change target classes, impose exploration/exploitation ratios, and
#     request a fresh Codex thread when continuity becomes economically costly
#   - may modify capital-gym state/ledger/policy files as a manager
#   - does not book fake value or perform the executor's outbound work
#
# If Claude is rate/session limited, Codex continues untouched.
###############################################################################

ROOT="${BONFYRE_ROOT:-/Users/nickgonzales/Documents/Bonfyre}"

STATE_ROOT="${BONFYRE_CAPITAL_STATE:-$HOME/Library/Application Support/Bonfyre/CapitalGym}"
LOG_ROOT="${BONFYRE_CAPITAL_LOGS:-$HOME/Library/Logs/Bonfyre/CapitalGym}"
RUN_ROOT="$LOG_ROOT/runs"

DB="$STATE_ROOT/capital.db"
STATE_MD="$STATE_ROOT/STATE.md"
WINS_MD="$STATE_ROOT/WINS.md"
HUMAN_GATES_MD="$STATE_ROOT/HUMAN_GATES.md"
SESSION_FILE="$STATE_ROOT/codex-session-id"
INITIAL="$STATE_ROOT/INITIAL_CONDITIONS.md"

MANAGER_ROOT="$STATE_ROOT/claude-manager"
MANAGER_LOG_ROOT="$LOG_ROOT/claude-manager"
REVIEWS="$MANAGER_ROOT/reviews"
SNAPSHOTS="$MANAGER_ROOT/snapshots"
CURRENT_DIRECTIVE="$MANAGER_ROOT/CURRENT_DIRECTIVE.md"
MANAGER_STATE="$MANAGER_ROOT/manager.state"
MANAGER_CHARTER="$MANAGER_ROOT/MANAGER_CHARTER.md"
MANAGER_LOG="$MANAGER_LOG_ROOT/manager.log"
MISSED_REVIEWS="$MANAGER_ROOT/MISSED_REVIEWS.tsv"
ROLL_REQUEST="$MANAGER_ROOT/RESET_CODEX_SESSION.request"
LOCKDIR="$MANAGER_ROOT/.manager.lock"

WINDOW="${BONFYRE_CLAUDE_MANAGER_WINDOW:-10}"
POLL="${BONFYRE_CLAUDE_MANAGER_POLL_SECONDS:-1}"
IMMEDIATE="${BONFYRE_CLAUDE_MANAGER_IMMEDIATE:-1}"
MANAGER_MODEL="${BONFYRE_CLAUDE_MANAGER_MODEL:-opus}"

mkdir -p "$MANAGER_ROOT" "$MANAGER_LOG_ROOT" "$REVIEWS" "$SNAPSHOTS"
chmod 700 "$MANAGER_ROOT" 2>/dev/null || true
touch "$MANAGER_LOG" "$MISSED_REVIEWS"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

command -v sqlite3 >/dev/null 2>&1 || die "sqlite3 is required"
command -v claude >/dev/null 2>&1 || die "claude CLI is not installed"
[[ -f "$DB" ]] || die "capital ledger not found: $DB"
[[ -f "$STATE_MD" ]] || die "CapitalGym STATE.md not found: $STATE_MD"

###############################################################################
# MANAGER SINGLETON
###############################################################################

cleanup_lock() {
  if [[ -d "$LOCKDIR" ]]; then
    local owner=""
    owner="$(cat "$LOCKDIR/pid" 2>/dev/null || true)"
    if [[ "$owner" == "$$" ]]; then
      rm -f "$LOCKDIR/pid" 2>/dev/null || true
      rmdir "$LOCKDIR" 2>/dev/null || true
    fi
  fi
}

if [[ -d "$LOCKDIR" ]]; then
  OLD_PID="$(cat "$LOCKDIR/pid" 2>/dev/null || true)"
  if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    die "Claude capital manager already running (pid $OLD_PID)"
  fi
  rm -rf "$LOCKDIR"
fi

mkdir "$LOCKDIR"
printf '%s\n' "$$" > "$LOCKDIR/pid"

trap cleanup_lock EXIT
trap 'cleanup_lock; exit 130' INT
trap 'cleanup_lock; exit 143' TERM

###############################################################################
# SQLITE HELPERS
###############################################################################

db() {
  sqlite3 -batch -cmd '.timeout 5000' "$DB" "$@"
}

db_exec() {
  sqlite3 -batch -cmd '.timeout 5000' "$DB" "$@" >/dev/null
}

###############################################################################
# EXTEND THE EXISTING LEDGER WITH MANAGER GOVERNANCE
###############################################################################

db_exec <<'SQL'
CREATE TABLE IF NOT EXISTS manager_reviews (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  successful_turn_count INTEGER NOT NULL,
  from_turn_id INTEGER,
  through_turn_id INTEGER,
  status TEXT NOT NULL,
  reset_requested INTEGER NOT NULL DEFAULT 0,
  review_path TEXT,
  snapshot_path TEXT,
  directive_path TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS manager_decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  review_id INTEGER,
  decision_class TEXT NOT NULL,
  target TEXT,
  instruction TEXT NOT NULL,
  horizon_turns INTEGER DEFAULT 10,
  active INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY(review_id) REFERENCES manager_reviews(id)
);
SQL

###############################################################################
# MANAGER CHARTER
###############################################################################

cat > "$MANAGER_CHARTER" <<'EOF'
# CLAUDE — CHIEF CAPITAL ALLOCATOR

You are the manager above the running Codex capital operator.

Your only objective:

    INCREASE REAL ECONOMIC VALUE PRODUCED PER REMAINING CODEX CREDIT.

You are NOT another executor.

Codex builds, browses, submits, publishes, contacts, benchmarks, applies,
claims, packages and implements.

You manage Codex.

You are expected to be skeptical, quantitative where evidence allows, and
willing to overturn the current strategy.

## Authority

Every management window you may:

- replace the next 10-turn strategy;
- reprioritize every capital lane;
- cap or ban low-density activity classes;
- tell Codex to abandon actions/assets/offers;
- tell Codex to follow up or stop following up;
- change offer packaging and asking prices;
- require lane diversification;
- require concentration on a winning lane;
- change build-vs-sell ratios;
- change research budgets;
- change maximum candidate counts;
- impose provider cooldowns;
- define minimum expected-value thresholds;
- change bounty criteria;
- change outbound criteria;
- change productization criteria;
- force a conversion from proof -> offer -> distribution;
- force credits to compound into deployable commercial assets;
- create or retire capital experiments;
- edit CapitalGym state and strategy files;
- edit the economic ledger when correcting classifications;
- create manager decision records;
- request a fresh Codex session when accumulated context is wasting capacity.

You may inspect Bonfyre or external state narrowly when necessary to make a
management decision.

You may use the local Claude/Codex histories as targeted evidence.

Do not reread all history or rediscover Bonfyre.

## What you must NOT do

Do not compete with Codex by becoming the primary worker.

Do not spend a management review implementing a bounty, building a product,
writing a grant, or sending commercial outbound yourself.

Do not stop or signal the live Codex supervisor.

Do not delete active run logs.

Do not fabricate:
- money received;
- approved credits;
- customers;
- replies;
- partnerships;
- eligibility;
- benchmark results;
- conversion rates;
- market prices.

Do not use these excluded historical systems as evidence or commercial proof:
- [REDACTED]
- [REDACTED]
- [REDACTED]

Do not bypass CAPTCHA, KYC, MFA, rate limits, bans, geography restrictions,
duplicate-account controls, anti-bot controls, or authorization boundaries.

Do not enable paid subscriptions, credit purchases, debt, trading, gambling,
or Codex auto-top-up.

## Capital books remain separate

REALIZED:
- cash received
- credits approved/activated
- bounty awarded
- signed/awarded non-debt contract

COMPETITIVE:
- submitted grants
- accelerators
- competitive funding applications

COMMERCIAL ASK:
- qualified offers actually delivered
- paid-pilot proposals actually delivered
- quotes actually delivered

INVENTORY:
- deployed saleable assets
- listings
- APIs
- environments
- benchmarks
- packages

Never collapse them.

## Your main management tests

For each 10-turn window ask:

1. VALUE CONVERSION
   What changed externally?
   How much realized value?
   How much collectible value advanced?
   How much qualified ask value?
   How much reusable inventory?

2. TOKEN ECONOMICS
   How many input tokens?
   How many were cached?
   How many were fresh?
   How many output tokens?
   Is the same thread bloating?
   Is the operator spending cognition re-reading dead material?

3. EXTERNAL-STATE DENSITY
   How many turns produced:
   - submission;
   - account;
   - claim;
   - offer delivered;
   - listing;
   - deploy;
   - accepted response;
   - awarded value?

4. LOCAL-OPTIMUM DETECTION
   Is Codex repeatedly doing the first thing that worked?
   Example failure:
       verifier
       -> GitHub issues
       -> $1,500 audits
       -> more GitHub issues
       -> no money

5. RESEARCH WASTE
   Did broad search produce mostly rejected candidates?
   Did a provider rate-limit the operator?
   Are searches returning hundreds of low-value items?

6. BUILD/SELL BALANCE
   Are assets being exposed to demand quickly?
   Is Codex polishing instead of distributing?
   Is it selling before enough proof exists?

7. CAPITAL DIVERSITY
   Are credits, commercial sales, marketplace distribution, bounties,
   partner programs, RL/eval environments, FPQ/model products, Frappe
   deployments, artifact products and new compositions being explored
   proportionally to evidence?

8. COMPOUNDING
   Did a win unlock another asset or distribution route?
   Did a rejection teach a reusable filter?

9. REALITY
   Do not reward activity counts.
   100 searches are not better than one paid deposit.

## Default intervention rules

These are defaults, not laws. Override them when evidence says otherwise.

- If a lane consumes >50% of a window with no meaningful external progression,
  cap it for the next window.
- If a provider rate-limits broad discovery, place that discovery mode on
  cooldown rather than bypassing the limit.
- If generic bounty search yields mostly crowded/unfunded/ambiguous work,
  sharply reduce bounty search allocation.
- If a deployed asset has no distribution, prioritize distribution before
  another adjacent build.
- If there are many delivered asks but no replies, change targeting/offer
  construction rather than simply multiplying asks.
- If there are replies, meetings, accepted pilots, approved credits or paid
  outcomes, concentrate harder on the mechanism producing them.
- If context growth is materially reducing useful work per credit, request a
  fresh Codex session. The sidecar can roll the session between turns without
  stopping the CapitalGym.
- If realized value is still zero, do not panic early—but demand evidence that
  the operator is moving toward states controlled by counterparties, not
  merely producing internal assets.
- Never optimize vanity face value.

## Bonfyre-specific management expectation

Do not allow Codex to collapse the entire Bonfyre estate into whatever wedge
worked first.

The estate contains, among other things:
- the nine Frappe app families;
- the public Bonfyre command estate;
- FPQ/local model execution;
- WorkGraph and durable execution;
- authority/effect/review/receipt machinery;
- BonfyreFS;
- artifact/representation machinery;
- SERM;
- Scene Lab/media;
- distributed/network/provider machinery;
- Aurekai variation/counterfactual machinery;
- context/continuity;
- recursive learning;
- finance/value/offer/economy machinery;
- native financial-statement absorption work.

Use the breadth as optionality while preserving deep successful wedges.

## Output contract

Your FINAL response MUST contain these exact machine-readable control lines:

CONTROL_RESET_CODEX_SESSION: yes|no
CONTROL_NEXT_WINDOW_TURNS: 10
CONTROL_CONFIDENCE: low|medium|high

Then:

DIRECTIVE_START

Write a concise, forceful directive addressed directly to Codex.

It must include:
- diagnosis of the previous window;
- what to STOP;
- what to CONTINUE;
- what to START;
- explicit next-window allocation/priorities;
- measurable external-state objectives;
- token/research budget rules;
- conditions that cause an immediate pivot;
- any assets/offers/actions to kill, reprice, compound or revisit.

Do not write a retrospective essay.
Do not explain Bonfyre.
Do not produce generic business advice.
The directive should be directly executable by Codex over the next 10 turns.

DIRECTIVE_END
EOF

###############################################################################
# STATE INITIALIZATION
###############################################################################

successful_turn_count() {
  db "SELECT count(*) FROM turns WHERE ended_at IS NOT NULL AND exit_code=0;" 2>/dev/null | tr -d '[:space:]'
}

latest_success_turn_id() {
  db "SELECT coalesce(max(id),0) FROM turns WHERE ended_at IS NOT NULL AND exit_code=0;" 2>/dev/null | tr -d '[:space:]'
}

if [[ ! -f "$MANAGER_STATE" ]]; then
  NOW_COUNT="$(successful_turn_count)"
  NOW_ID="$(latest_success_turn_id)"

  if [[ "$IMMEDIATE" == "1" ]]; then
    BASE_COUNT=$(( NOW_COUNT > WINDOW ? NOW_COUNT - WINDOW : 0 ))
    BASE_ID="$(db "
      SELECT coalesce(id,0)
      FROM turns
      WHERE ended_at IS NOT NULL AND exit_code=0
      ORDER BY id DESC
      LIMIT 1 OFFSET $WINDOW;
    " 2>/dev/null | tr -d '[:space:]' || true)"
    BASE_ID="${BASE_ID:-0}"
  else
    BASE_COUNT="$NOW_COUNT"
    BASE_ID="$NOW_ID"
  fi

  cat > "$MANAGER_STATE" <<EOF
last_review_success_count=$BASE_COUNT
last_review_turn_id=$BASE_ID
immediate_pending=$IMMEDIATE
last_review_epoch=0
EOF
fi

state_get() {
  local key="$1"
  awk -F= -v k="$key" '$1==k { sub(/^[^=]*=/,""); print; exit }' "$MANAGER_STATE"
}

state_set_all() {
  local count="$1"
  local turn_id="$2"
  local immediate="$3"
  local epoch="$4"
  local tmp="$MANAGER_STATE.tmp.$$"
  cat > "$tmp" <<EOF
last_review_success_count=$count
last_review_turn_id=$turn_id
immediate_pending=$immediate
last_review_epoch=$epoch
EOF
  mv "$tmp" "$MANAGER_STATE"
}

###############################################################################
# CURRENT DIRECTIVE -> STATE.md
#
# The live v3 Codex supervisor already tells Codex to read STATE.md each turn.
# This makes Claude's authority effective without restarting or patching the
# running shell process.
###############################################################################

inject_directive_into_state() {
  [[ -s "$CURRENT_DIRECTIVE" ]] || return 0

  python3 - "$STATE_MD" "$CURRENT_DIRECTIVE" <<'PY'
from pathlib import Path
import re, sys, os, tempfile

state = Path(sys.argv[1])
directive = Path(sys.argv[2])

begin = "<!-- CLAUDE_CAPITAL_MANAGER_BEGIN -->"
end = "<!-- CLAUDE_CAPITAL_MANAGER_END -->"

body = state.read_text(errors="replace") if state.exists() else ""
d = directive.read_text(errors="replace").strip()

block = (
    "\n\n" + begin + "\n"
    "# CLAUDE CAPITAL MANAGER — ACTIVE OVERRIDE\n\n"
    "This block is authoritative capital-allocation policy for the current "
    "management window. Preserve it when updating STATE.md and obey it unless "
    "new external evidence makes an instruction impossible or unsafe. If so, "
    "record the evidence and choose the highest-density compliant alternative.\n\n"
    + d + "\n"
    + end + "\n"
)

pattern = re.compile(
    re.escape(begin) + r".*?" + re.escape(end) + r"\n?",
    re.S
)

if pattern.search(body):
    updated = pattern.sub(block.strip("\n") + "\n", body)
else:
    updated = body.rstrip() + block

tmp = state.with_name(state.name + ".manager-tmp")
tmp.write_text(updated)
os.replace(tmp, state)
PY
}

###############################################################################
# SNAPSHOT COMPILER
#
# Feed Claude a bounded management packet. Do NOT hand it giant raw histories.
###############################################################################

compile_snapshot() {
  local success_count="$1"
  local through_id="$2"
  local from_id="$3"
  local out="$4"

  {
    echo "# CLAUDE CAPITAL MANAGER SNAPSHOT"
    echo
    echo "generated_at=$(date)"
    echo "successful_turn_count=$success_count"
    echo "window_from_turn_id=$from_id"
    echo "window_through_turn_id=$through_id"
    echo

    echo "## ECONOMIC SCOREBOARD"
    sqlite3 -batch -cmd '.timeout 5000' -header -column "$DB" <<'SQL'
SELECT
  (SELECT count(*) FROM wins) AS wins,
  (SELECT printf('$%.2f',coalesce(sum(realized_value),0)) FROM wins) AS realized,
  (SELECT count(*) FROM assets WHERE status NOT IN ('abandoned')) AS active_assets,
  (SELECT count(*) FROM offers WHERE status NOT IN ('abandoned')) AS active_offers,
  (SELECT printf('$%.2f',coalesce(sum(asking_price),0)) FROM offers WHERE status NOT IN ('abandoned')) AS offer_ask_inventory,
  (SELECT count(*) FROM capital_actions) AS capital_actions,
  (SELECT count(*) FROM human_gates WHERE status='open') AS open_human_gates;
SQL
    echo

    echo "## LAST 20 SUCCESSFUL TURNS"
    sqlite3 -batch -cmd '.timeout 5000' -header -column "$DB" "
SELECT
  id,
  started_at,
  ended_at,
  input_tokens,
  cached_input_tokens,
  CASE
    WHEN input_tokens IS NULL THEN NULL
    ELSE max(input_tokens-coalesce(cached_input_tokens,0),0)
  END AS fresh_input_tokens,
  output_tokens
FROM turns
WHERE ended_at IS NOT NULL AND exit_code=0
ORDER BY id DESC
LIMIT 20;
"
    echo

    echo "## CURRENT 10-TURN WINDOW TOKEN TOTALS"
    sqlite3 -batch -cmd '.timeout 5000' -header -column "$DB" "
WITH w AS (
  SELECT *
  FROM turns
  WHERE ended_at IS NOT NULL
    AND exit_code=0
    AND id >= $from_id
    AND id <= $through_id
)
SELECT
  count(*) AS turns,
  coalesce(sum(input_tokens),0) AS input_tokens,
  coalesce(sum(cached_input_tokens),0) AS cached_input_tokens,
  coalesce(sum(max(input_tokens-coalesce(cached_input_tokens,0),0)),0) AS fresh_input_tokens,
  coalesce(sum(output_tokens),0) AS output_tokens
FROM w;
"
    echo

    echo "## CAPITAL ACTIONS"
    sqlite3 -batch -cmd '.timeout 5000' -header -column "$DB" "
SELECT
  id, action_class, target, status,
  printf('$%.2f',coalesce(face_value,0)) AS face_value,
  printf('$%.2f',coalesce(realized_value,0)) AS realized_value,
  probability,
  expected_value,
  next_state,
  blocker,
  external_ref
FROM capital_actions
ORDER BY id DESC
LIMIT 80;
"
    echo

    echo "## ASSETS"
    sqlite3 -batch -cmd '.timeout 5000' -header -column "$DB" "
SELECT id,name,class,status,deployment_ref,offer_ref,notes
FROM assets
ORDER BY id DESC
LIMIT 50;
"
    echo

    echo "## OFFERS"
    sqlite3 -batch -cmd '.timeout 5000' -header -column "$DB" "
SELECT id,name,offer_class,asking_price,currency,status,target_customer,external_ref,notes
FROM offers
ORDER BY id DESC
LIMIT 50;
"
    echo

    echo "## COMMERCIAL EXPERIMENTS"
    sqlite3 -batch -cmd '.timeout 5000' -header -column "$DB" "
SELECT id,hypothesis,variant,target,result,response_signal,learned
FROM commercial_experiments
ORDER BY id DESC
LIMIT 50;
"
    echo

    echo "## OPEN HUMAN GATES"
    sqlite3 -batch -cmd '.timeout 5000' -header -column "$DB" "
SELECT id,opportunity,gate_class,exact_gate,value_at_stake,external_ref,notes
FROM human_gates
WHERE status='open'
ORDER BY id DESC
LIMIT 50;
"
    echo

    echo "## WINS"
    sqlite3 -batch -cmd '.timeout 5000' -header -column "$DB" "
SELECT id,created_at,win_class,name,realized_value,face_value,external_ref,confirmation_id,notes
FROM wins
ORDER BY id DESC
LIMIT 50;
"
    echo

    echo "## CURRENT OPERATIONAL STATE"
    sed -n '1,320p' "$STATE_MD" 2>/dev/null || true
    echo

    echo "## CURRENT WINS FILE"
    sed -n '1,200p' "$WINS_MD" 2>/dev/null || true
    echo

    echo "## CURRENT HUMAN GATES FILE"
    sed -n '1,200p' "$HUMAN_GATES_MD" 2>/dev/null || true
    echo

    echo "## RECENT CODEX AGENT MESSAGES"
    # Only manager-relevant agent messages; never dump giant command output.
    find "$RUN_ROOT" -mindepth 1 -maxdepth 1 -type d -print0 2>/dev/null \
      | xargs -0 ls -td 2>/dev/null \
      | head -n 12 \
      | while IFS= read -r d; do
          f="$d/events.jsonl"
          [[ -f "$f" ]] || continue
          echo
          echo "### $(basename "$d")"
          if command -v jq >/dev/null 2>&1; then
            jq -r '
              select(.type=="item.completed")
              | select(.item.type=="agent_message")
              | .item.text
            ' "$f" 2>/dev/null | tail -n 12 || true
          else
            grep '"type":"agent_message"' "$f" 2>/dev/null | tail -n 12 || true
          fi
        done

    echo
    echo "## MANAGER HISTORY"
    sqlite3 -batch -cmd '.timeout 5000' -header -column "$DB" "
SELECT id,created_at,successful_turn_count,from_turn_id,through_turn_id,status,reset_requested,notes
FROM manager_reviews
ORDER BY id DESC
LIMIT 10;
"
  } > "$out"
}

###############################################################################
# CLAUDE CLI INVOCATION
###############################################################################

CLAUDE_HELP="$(claude --help 2>&1 || true)"

claude_run() {
  local prompt_file="$1"
  local stdout_file="$2"
  local stderr_file="$3"

  local flags=()

  if printf '%s\n' "$CLAUDE_HELP" | grep -q -- '--dangerously-skip-permissions'; then
    flags+=(--dangerously-skip-permissions)
  fi

  if [[ -n "$MANAGER_MODEL" ]] && printf '%s\n' "$CLAUDE_HELP" | grep -q -- '--model'; then
    flags+=(--model "$MANAGER_MODEL")
  fi

  local prompt
  prompt="$(cat "$prompt_file")"

  if printf '%s\n' "$CLAUDE_HELP" | grep -q -- '--print'; then
    claude "${flags[@]}" --print "$prompt" >"$stdout_file" 2>"$stderr_file"
  else
    claude "${flags[@]}" -p "$prompt" >"$stdout_file" 2>"$stderr_file"
  fi
}

claude_unavailable() {
  local combined="$1"
  grep -Eiq \
    'session limit|usage limit|rate limit|resets .*([ap]m|[0-9])|try again later|temporarily unavailable|overloaded|capacity|quota' \
    "$combined"
}

###############################################################################
# MANAGEMENT PROMPT
###############################################################################

build_manager_prompt() {
  local snapshot="$1"
  local prompt="$2"
  local from_id="$3"
  local through_id="$4"

  cat > "$prompt" <<EOF
You are Claude acting as the CHIEF CAPITAL ALLOCATOR over a running Codex
capital operator for Bonfyre/Aurekai.

This is a MANAGEMENT REVIEW, not an execution turn.

The Codex run is live right now. DO NOT stop it, signal it, kill it, wait for it,
or interfere with its current child process.

Your management window is:

    from successful turn id: $from_id
    through successful turn id: $through_id

Read these first:

    $MANAGER_CHARTER
    $snapshot

You may then inspect only the exact additional local/external evidence necessary
to make a better capital-allocation decision.

You may inspect:
    $DB
    $STATE_MD
    $RUN_ROOT
    $INITIAL
    $ROOT

You may use the existing targeted memory helper if present:
    $STATE_ROOT/bin/capital-memory-search

Do not rediscover the full architecture.

Do not read all Claude/Codex history.

Do not use the three excluded historical systems.

Your job is to answer:

    HOW DO I MAKE THE NEXT 10 CODEX TURNS PRODUCE MORE REAL VALUE
    PER REMAINING CODEX CREDIT THAN THE PREVIOUS 10?

You have broad management authority.

You may directly correct/reclassify CapitalGym DB/state records if they are
objectively wrong. Never fabricate realized value.

You may create/retire manager strategy files and capital experiments.

You may NOT do Codex's execution work merely because you can.

Examples of valid management interventions:

    "GitHub bounty discovery is consuming too much cognition; cap it at one
     bounded candidate check in the next 10 turns."

    "Three $1,500 offers with no response is not a sales system; require a
     different buyer class and different offer packaging."

    "FPQ has existing proof but zero external commercial state; allocate 2 of
     the next 10 turns to proof -> product -> qualified distribution."

    "Cloud credits have higher collectible-value density than another generic
     bounty sweep; finish the eligibility/application state."

    "The current Codex thread has grown too expensive; request a fresh thread
     backed by STATE.md continuity."

    "A lane just produced an actual reply/award; concentrate 60% of the next
     window there."

Do not prefer diversity for its own sake if a lane is genuinely converting.

Do not prefer concentration merely because Codex has already invested in it.

Judge evidence.

Your final answer MUST follow the exact output contract in MANAGER_CHARTER.md.
EOF
}

###############################################################################
# PARSE / APPLY CLAUDE DIRECTIVE
###############################################################################

extract_directive() {
  local review="$1"
  local directive_out="$2"

  awk '
    /^DIRECTIVE_START[[:space:]]*$/ { inside=1; next }
    /^DIRECTIVE_END[[:space:]]*$/ { inside=0; exit }
    inside { print }
  ' "$review" > "$directive_out"

  [[ -s "$directive_out" ]]
}

control_value() {
  local review="$1"
  local key="$2"
  sed -nE "s/^${key}:[[:space:]]*(.*)$/\1/p" "$review" | head -n 1 | tr -d '\r'
}

###############################################################################
# SAFE SESSION ROLLOVER
#
# The current v3 supervisor writes codex-session-id after each completed turn
# and sleeps ~2 seconds. If Claude requests a reset, this sidecar waits for the
# current active row to end, then removes only the session pointer during the
# between-turn gap. The CapitalGym process itself never stops.
#
# If the timing window is missed, the request stays pending and retries later.
###############################################################################

codex_turn_active() {
  local active
  active="$(db "
    SELECT CASE WHEN ended_at IS NULL THEN 1 ELSE 0 END
    FROM turns
    ORDER BY id DESC
    LIMIT 1;
  " 2>/dev/null | tr -d '[:space:]')"
  [[ "${active:-0}" == "1" ]]
}

attempt_session_rollover() {
  [[ -f "$ROLL_REQUEST" ]] || return 0

  if codex_turn_active; then
    return 0
  fi

  # Require an existing session pointer; deleting nothing should not count.
  if [[ -s "$SESSION_FILE" ]]; then
    OLD="$(cat "$SESSION_FILE" 2>/dev/null || true)"
    rm -f "$SESSION_FILE"
    {
      echo "$(date) manager removed completed Codex session pointer for fresh continuity-backed thread"
      echo "old_session=$OLD"
    } >> "$MANAGER_LOG"
    rm -f "$ROLL_REQUEST"
  fi
}

###############################################################################
# REVIEW EXECUTION
###############################################################################

run_review() {
  local success_count="$1"
  local through_id="$2"
  local previous_turn_id="$3"

  local from_id
  from_id="$(db "
    SELECT coalesce(min(id),$through_id)
    FROM (
      SELECT id
      FROM turns
      WHERE ended_at IS NOT NULL
        AND exit_code=0
        AND id<=$through_id
      ORDER BY id DESC
      LIMIT $WINDOW
    );
  " 2>/dev/null | tr -d '[:space:]')"

  [[ -n "$from_id" ]] || from_id="$through_id"

  local ts snapshot prompt review stderr combined directive_tmp
  ts="$(date '+%Y%m%d-%H%M%S')"
  snapshot="$SNAPSHOTS/${ts}-through-${through_id}.md"
  prompt="$MANAGER_ROOT/${ts}-manager-prompt.md"
  review="$REVIEWS/${ts}-through-${through_id}.md"
  stderr="$REVIEWS/${ts}-through-${through_id}.stderr.log"
  combined="$REVIEWS/${ts}-through-${through_id}.combined.log"
  directive_tmp="$REVIEWS/${ts}-directive.tmp"

  compile_snapshot "$success_count" "$through_id" "$from_id" "$snapshot"
  build_manager_prompt "$snapshot" "$prompt" "$from_id" "$through_id"

  echo "$(date) CLAUDE REVIEW starting: turns $from_id..$through_id" \
    | tee -a "$MANAGER_LOG"

  set +e
  claude_run "$prompt" "$review" "$stderr"
  rc=$?
  set -e

  cat "$review" "$stderr" > "$combined" 2>/dev/null || true

  if claude_unavailable "$combined"; then
    printf '%s\t%s\t%s\t%s\tclaude_unavailable\n' \
      "$(date '+%Y-%m-%d %H:%M:%S')" "$success_count" "$from_id" "$through_id" \
      >> "$MISSED_REVIEWS"

    db_exec "
      INSERT INTO manager_reviews(
        successful_turn_count,from_turn_id,through_turn_id,status,
        review_path,snapshot_path,notes
      ) VALUES(
        $success_count,$from_id,$through_id,'claude_unavailable',
        '$(printf "%s" "$review" | sed "s/'/''/g")',
        '$(printf "%s" "$snapshot" | sed "s/'/''/g")',
        'Claude unavailable; Codex explicitly left running'
      );
    "

    echo "$(date) Claude unavailable; Codex continues. Review remains pending." \
      | tee -a "$MANAGER_LOG"

    # Do NOT advance last_review_success_count. Retry later with a newer bounded
    # snapshot, but rate-limit retry attempts in the main loop.
    return 75
  fi

  if [[ "$rc" -ne 0 ]]; then
    db_exec "
      INSERT INTO manager_reviews(
        successful_turn_count,from_turn_id,through_turn_id,status,
        review_path,snapshot_path,notes
      ) VALUES(
        $success_count,$from_id,$through_id,'claude_failed',
        '$(printf "%s" "$review" | sed "s/'/''/g")',
        '$(printf "%s" "$snapshot" | sed "s/'/''/g")',
        'Claude exit code $rc; Codex left running'
      );
    "
    echo "$(date) Claude manager failed rc=$rc; Codex continues." \
      | tee -a "$MANAGER_LOG"
    return 76
  fi

  if ! extract_directive "$review" "$directive_tmp"; then
    db_exec "
      INSERT INTO manager_reviews(
        successful_turn_count,from_turn_id,through_turn_id,status,
        review_path,snapshot_path,notes
      ) VALUES(
        $success_count,$from_id,$through_id,'invalid_output',
        '$(printf "%s" "$review" | sed "s/'/''/g")',
        '$(printf "%s" "$snapshot" | sed "s/'/''/g")',
        'Missing DIRECTIVE_START/DIRECTIVE_END; Codex left running'
      );
    "
    echo "$(date) Claude returned no machine-parseable directive; Codex continues." \
      | tee -a "$MANAGER_LOG"
    rm -f "$directive_tmp"
    return 77
  fi

  mv "$directive_tmp" "$CURRENT_DIRECTIVE"

  reset="$(control_value "$review" "CONTROL_RESET_CODEX_SESSION")"
  confidence="$(control_value "$review" "CONTROL_CONFIDENCE")"

  reset_flag=0
  if [[ "$reset" == "yes" ]]; then
    reset_flag=1
    cat > "$ROLL_REQUEST" <<EOF
requested_at=$(date)
through_turn_id=$through_id
reason=Claude manager requested continuity-backed fresh Codex session
EOF
  fi

  inject_directive_into_state

  REVIEW_ID="$(db "
    INSERT INTO manager_reviews(
      successful_turn_count,from_turn_id,through_turn_id,status,reset_requested,
      review_path,snapshot_path,directive_path,notes
    ) VALUES(
      $success_count,$from_id,$through_id,'applied',$reset_flag,
      '$(printf "%s" "$review" | sed "s/'/''/g")',
      '$(printf "%s" "$snapshot" | sed "s/'/''/g")',
      '$(printf "%s" "$CURRENT_DIRECTIVE" | sed "s/'/''/g")',
      'confidence=$(printf "%s" "$confidence" | sed "s/'/''/g")'
    );
    SELECT last_insert_rowid();
  " 2>/dev/null | tail -n 1 | tr -d '[:space:]')"

  # Store a compact decision record for visibility in the same capital DB.
  DIRECTIVE_SQL="$(
    python3 - "$CURRENT_DIRECTIVE" <<'PY'
from pathlib import Path
import sys
s=Path(sys.argv[1]).read_text(errors="replace")
print(s.replace("'", "''"))
PY
  )"

  db_exec "
    INSERT INTO manager_decisions(
      review_id,decision_class,target,instruction,horizon_turns,active
    ) VALUES(
      ${REVIEW_ID:-NULL},
      'window_strategy',
      'Codex next $WINDOW successful turns',
      '$DIRECTIVE_SQL',
      $WINDOW,
      1
    );
  "

  echo "$(date) CLAUDE REVIEW applied: turns $from_id..$through_id reset=$reset confidence=$confidence" \
    | tee -a "$MANAGER_LOG"

  echo "$(date) ACTIVE DIRECTIVE:" >> "$MANAGER_LOG"
  sed -n '1,220p' "$CURRENT_DIRECTIVE" >> "$MANAGER_LOG"
  echo >> "$MANAGER_LOG"

  return 0
}

###############################################################################
# FIRST DIRECTIVE
#
# Before the first Claude review succeeds, install a tiny governance marker so
# Codex knows a manager may override strategy through STATE.md.
###############################################################################

if [[ ! -s "$CURRENT_DIRECTIVE" ]]; then
  cat > "$CURRENT_DIRECTIVE" <<'EOF'
Claude is now the capital-allocation manager above this Codex operator.

Until the first management review lands:
- keep executing the existing mission;
- do not wait for Claude;
- preserve truthful economic bookkeeping;
- treat provider rate limits as cooldowns, not obstacles to bypass;
- do not assume the first commercial wedge should consume the entire estate;
- continue moving external state.
EOF
fi

inject_directive_into_state

###############################################################################
# MAIN SIDE-CAR LOOP
###############################################################################

echo
echo "======================================================================"
echo " BONFYRE / AUREKAI — CLAUDE CAPITAL MANAGER"
echo "======================================================================"
echo " Codex run:       LEFT RUNNING"
echo " Capital DB:      $DB"
echo " State channel:   $STATE_MD"
echo " Review window:   every $WINDOW successful Codex turns"
echo " Claude model:    $MANAGER_MODEL (if supported by installed CLI)"
echo " Manager state:   $MANAGER_ROOT"
echo " Manager log:     $MANAGER_LOG"
echo " Directive:       $CURRENT_DIRECTIVE"
echo "======================================================================"
echo

LAST_RETRY_EPOCH=0
CLAUDE_RETRY_SECONDS="${BONFYRE_CLAUDE_MANAGER_RETRY_SECONDS:-300}"

while true; do
  # Keep the active directive present if Codex rewrote STATE.md.
  if ! grep -q '<!-- CLAUDE_CAPITAL_MANAGER_BEGIN -->' "$STATE_MD" 2>/dev/null; then
    inject_directive_into_state
  fi

  # Apply a requested context rollover only in a between-turn gap.
  attempt_session_rollover || true

  COUNT="$(successful_turn_count)"
  THROUGH="$(latest_success_turn_id)"

  LAST_COUNT="$(state_get last_review_success_count)"
  LAST_TURN_ID="$(state_get last_review_turn_id)"
  IMMEDIATE_PENDING="$(state_get immediate_pending)"
  LAST_REVIEW_EPOCH="$(state_get last_review_epoch)"

  LAST_COUNT="${LAST_COUNT:-0}"
  LAST_TURN_ID="${LAST_TURN_ID:-0}"
  IMMEDIATE_PENDING="${IMMEDIATE_PENDING:-0}"
  LAST_REVIEW_EPOCH="${LAST_REVIEW_EPOCH:-0}"

  SHOULD_REVIEW=0

  if [[ "$IMMEDIATE_PENDING" == "1" ]]; then
    SHOULD_REVIEW=1
  elif (( COUNT - LAST_COUNT >= WINDOW )); then
    SHOULD_REVIEW=1
  fi

  NOW_EPOCH="$(date +%s)"

  if [[ "$SHOULD_REVIEW" == "1" ]]; then
    # If Claude was unavailable/failing recently, do not hammer it every second.
    if (( NOW_EPOCH - LAST_REVIEW_EPOCH >= CLAUDE_RETRY_SECONDS )) || [[ "$IMMEDIATE_PENDING" == "1" ]]; then
      state_set_all "$LAST_COUNT" "$LAST_TURN_ID" "0" "$NOW_EPOCH"

      set +e
      run_review "$COUNT" "$THROUGH" "$LAST_TURN_ID"
      REVIEW_RC=$?
      set -e

      if [[ "$REVIEW_RC" -eq 0 ]]; then
        state_set_all "$COUNT" "$THROUGH" "0" "$NOW_EPOCH"
      else
        # Keep the old successful checkpoint so a later Claude invocation sees
        # the unreviewed economic window. Codex remains fully independent.
        state_set_all "$LAST_COUNT" "$LAST_TURN_ID" "0" "$NOW_EPOCH"
      fi
    fi
  fi

  sleep "$POLL"
done
