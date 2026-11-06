#!/bin/sh
# Work the SourceFrontier ledger down through the existing native WorkGraph.
#
# SourceFrontier remains semantic authority for channel state. WorkGraph owns
# only execution concerns: priority, leases, crash recovery, retries and worker
# routing. qualify_channels.mjs remains the observer/parser/admission owner.
#
# The old first-seen slice could bury newly useful material behind >100k older
# candidates. SourceFrontier now projects unresolved channels as deterministic
# WorkGraph missions, with observed publisher-declared links and sparse roots
# receiving higher execution priority. BonfyreQueue leases the exact slice.
set -u

ESTATE="${BONFYRE_ESTATE:-$HOME/.bonfyre/estate-local}"
REPO="${BONFYRE_REPO:-$HOME/Developer/Bonfyre}"
SLICE="${BONFYRE_QUALIFY_SLICE:-300}"
WORKERS="${BONFYRE_QUALIFY_WORKERS:-10}"
SEED="${BONFYRE_QUALIFY_WORKGRAPH_SEED:-5000}"
SEED_LOW="${BONFYRE_QUALIFY_WORKGRAPH_LOW_WATER:-1000}"
LEASE_MS="${BONFYRE_QUALIFY_LEASE_MS:-1800000}"

DB="$ESTATE/frontier.db"
CMS="$ESTATE/cms.db"
FOREIGN="$ESTATE/foreign"
STATE="$ESTATE/qualify-standing"
mkdir -p "$STATE"
LOG="$STATE/qualify.log"

FRONTIER="$REPO/cmd/BonfyreSourceFrontier/bonfyre-source-frontier"
PROJECT="$REPO/components/BonfyreProject/bonfyre-project"
CMS_BIN="$REPO/cmd/BonfyreCMS/bonfyre-cms"
QUALIFY="$REPO/cmd/BonfyreSourceFrontier/tools/qualify_channels.mjs"
APPLY="$REPO/cmd/BonfyreWebAddress/tools/apply_census_sources.mjs"
QUEUE="$REPO/cmd/BonfyreQueue/bonfyre-queue"
WORK_DB="${BONFYRE_WORKGRAPH_DB:-$REPO/components/BonfyreControlPlane/work_queue.db}"

note() { echo "$(date -u +%FT%TZ) $*" >> "$LOG"; }

for needed in "$DB" "$FRONTIER" "$PROJECT" "$QUALIFY" "$QUEUE"; do
    [ -e "$needed" ] || { note "missing: $needed"; exit 2; }
done

# Keep one census writer per standing invocation. WorkGraph itself permits more
# workers, but this guard preserves the existing CMS/application write contract
# while selection and crash recovery move into the native scheduler.
LOCK="$STATE/running.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
    if [ -n "$(find "$LOCK" -maxdepth 0 -mmin +60 2>/dev/null)" ]; then
        note "clearing a stale outer census lock"
        rmdir "$LOCK" 2>/dev/null || true
        mkdir "$LOCK" 2>/dev/null || exit 0
    else
        exit 0
    fi
fi
trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT INT TERM

stamp=$(date -u +%Y%m%dT%H%M%SZ)
worker="source-qualifier-$(hostname 2>/dev/null || echo local)-$$"
work="$STATE/work-$stamp.tsv"
claims="$STATE/claims-$stamp.jsonl"
ids="$STATE/channel-ids-$stamp.txt"
finish="$STATE/finish-$stamp.tsv"
rows="$STATE/admitted-$stamp.json"
declared="$STATE/declared-$stamp.jsonl"

# Maintain a bounded ready window rather than re-enqueueing the same 5k on
# every five-minute tick. The queue owner exposes its own read-only depth, so
# this script does not scrape WorkGraph tables.
depth_json=$("$QUEUE" depth --family source-qualify --db "$WORK_DB" 2>/dev/null || printf '{"claimable":0}')
claimable=$(printf '%s' "$depth_json" | bun -e 'try { const n=Number(JSON.parse(await Bun.stdin.text()).claimable ?? 0); console.log(Number.isFinite(n) ? Math.trunc(n) : 0); } catch { console.log(0); }')
if [ "$claimable" -lt "$SEED_LOW" ]; then
    if ! "$FRONTIER" qualification-work --db "$DB" --limit "$SEED" > "$work" 2>>"$LOG"; then
        note "SourceFrontier could not project qualification work"
        exit 2
    fi
    # With BONFYRE_QUALIFY_ORDER=yield, the frozen channel-yield weights set
    # each seed's priority. Every candidate is still enqueued and observed.
    if [ "${BONFYRE_QUALIFY_ORDER:-coverage}" = yield ] && [ -f "${BONFYRE_CHANNEL_YIELD_MODEL:-$ESTATE/channel-yield/model.json}" ]; then
        ranked=$(bun "$REPO/components/BonfyreDecision/channel-yield.mjs" rank --db "$DB" --work "$work" \
            --model "${BONFYRE_CHANNEL_YIELD_MODEL:-$ESTATE/channel-yield/model.json}" 2>&1 | tail -1)
        note "channel-yield rank: $(printf '%s' "$ranked" | cut -c1-200)"
    fi
    queue_seed=$("$QUEUE" enqueue-batch "$work" --db "$WORK_DB" 2>&1 | tail -1)
    note "workgraph seed: $queue_seed (claimable before seed: $claimable)"
else
    : > "$work"
    note "workgraph seed skipped: $claimable claimable source-qualify nodes"
fi
"$QUEUE" reap-expired --db "$WORK_DB" >/dev/null 2>>"$LOG" || true

# WorkGraph chooses the exact slice by priority and lease. The mission id is
# deterministic: source-qualify:<channel_id>. No address or semantic state is
# copied into WorkGraph.
if ! "$QUEUE" claim-batch "$worker" --family source-qualify --count "$SLICE" \
        --lease-ms "$LEASE_MS" --db "$WORK_DB" > "$claims" 2>>"$LOG"; then
    note "WorkGraph claim failed"
    exit 2
fi

bun "$REPO/scripts/qualify_claims.mjs" --claims "$claims" --ids "$ids" --finish "$finish" >/dev/null
claimed=$(wc -l < "$ids" | tr -d ' ')
[ "$claimed" -gt 0 ] || { note "workgraph: no source-qualify work ready"; exit 0; }
note "workgraph claimed: $claimed"

# CheapCognition in SHADOW: the DecisionPlane says, before the fetch, how
# likely each claimed observation is to yield. Nothing is skipped or
# reordered here; the receipts are written before the qualifier observes the
# slice, so its outcomes are an independent holdout for the frozen weights.
YIELD_MODEL="${BONFYRE_CHANNEL_YIELD_MODEL:-$ESTATE/channel-yield/model.json}"
if [ -f "$YIELD_MODEL" ]; then
    shadow=$(bun "$REPO/components/BonfyreDecision/channel-yield.mjs" shadow --db "$DB" --ids "$ids" \
        --model "$YIELD_MODEL" --out "$STATE/yield-$stamp.jsonl" 2>&1 | tail -1)
    note "channel-yield shadow: $(printf '%s' "$shadow" | cut -c1-300)"
fi

# Observation/parsing/admission is unchanged. The only semantic difference is
# that the exact channel ids came from a WorkGraph lease rather than FIFO SQL.
if answer=$(bun "$QUALIFY" --db "$DB" --frontier "$FRONTIER" --project "$PROJECT" \
        --foreign "$FOREIGN" --out "$rows" --limit "$SLICE" --workers "$WORKERS" \
        --cms "$CMS" --evidence "$STATE/evidence.jsonl" --discovered "$declared" \
        --channel-id-file "$ids" 2>&1); then
    note "$(printf '%s\n' "$answer" | tail -1)"
else
    rc=$?
    note "qualifier process failed rc=$rc: $(printf '%s\n' "$answer" | tail -1)"
    "$QUEUE" fail-batch "$finish" --class transient \
        --message "qualifier process exit $rc" --db "$WORK_DB" >> "$LOG" 2>&1 || true
    exit "$rc"
fi

# What answered and parsed becomes a source the pages can actually draw on.
if [ -s "$rows" ] && [ -x "$CMS_BIN" ] && grep -q '"source"' "$rows" 2>/dev/null; then
    applied=$(bun "$APPLY" --json "$rows" --db "$CMS" \
        --schemas "$ESTATE/schemas" --cms "$CMS_BIN" 2>&1 | tail -1)
    note "census: $applied"
fi

# A successful qualifier pass is terminal WorkGraph work even when the channel
# was refused: the authoritative outcome is now recorded in SourceFrontier.
# If a previous pass committed SourceFrontier and crashed before this ACK, the
# retry selects zero unresolved rows and lands here safely.
queue_ack=$("$QUEUE" ack-batch "$finish" --db "$WORK_DB" 2>&1 | tail -1)
note "workgraph ack: $queue_ack"

# Keep durable evidence, not disposable batch projections.
find "$STATE" -name 'admitted-*.json' -mmin +2880 -delete 2>/dev/null || true
find "$STATE" -name 'declared-*.jsonl' -mmin +2880 -delete 2>/dev/null || true
find "$STATE" -name 'work-*.tsv' -mmin +2880 -delete 2>/dev/null || true
find "$STATE" -name 'claims-*.jsonl' -mmin +2880 -delete 2>/dev/null || true
find "$STATE" -name 'channel-ids-*.txt' -mmin +2880 -delete 2>/dev/null || true
find "$STATE" -name 'finish-*.tsv' -mmin +2880 -delete 2>/dev/null || true
