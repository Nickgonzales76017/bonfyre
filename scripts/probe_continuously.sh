#!/bin/sh
# Ask the hosts this estate knows of and has never got anything from.
#
# Twenty-seven thousand roots are listed here with no admitted channel. Nearly
# all of them were never asked: the qualifier works a first-seen queue of the
# addresses someone listed, and a host whose feed nobody linked is a host this
# estate never reads. But a feed lives at /feed, a catalogue at /data.json, an
# interface at /openapi.json -- conventions, not secrets -- so the question can
# simply be asked.
#
# One slice per pass, bounded by the estate's own crawl ceiling, so this works
# through the backlog for as long as it takes rather than in one run nobody
# could supervise. A host that answers becomes a source; a host that does not
# is remembered as asked, so the next pass moves on instead of asking again.
#
#   probe_continuously.sh [--slice 160] [--workers 10]
set -eu

ESTATE="${BONFYRE_ESTATE:-$HOME/.bonfyre/estate-local}"
REPO="${BONFYRE_REPO:-$HOME/Developer/Bonfyre}"
SLICE="${BONFYRE_PROBE_SLICE:-160}"
WORKERS="${BONFYRE_PROBE_WORKERS:-10}"
while [ $# -gt 0 ]; do
    case "$1" in
        --slice) SLICE=$2; shift 2 ;;
        --workers) WORKERS=$2; shift 2 ;;
        *) shift ;;
    esac
done

STATE="$ESTATE/probe"; mkdir -p "$STATE"
LOG="$STATE/probe.log"
ASKED="$STATE/asked.db"
PROBE="$REPO/cmd/BonfyreSourceFrontier/tools/probe_catalogues.mjs"
FRONTIER="$REPO/cmd/BonfyreSourceFrontier/bonfyre-source-frontier"
APPLY="$REPO/cmd/BonfyreWebAddress/tools/apply_census_sources.mjs"
note() { echo "$(date -u +%FT%TZ) $*" >> "$LOG"; }
# Several of the steps below answer with indented JSON, and a log that kept
# only its last line recorded a lone closing brace. This keeps the answer whole
# and on one line, or passes plain text through unchanged.
oneline() { bun -e 'const raw = (await Bun.stdin.text()).trim(); try { console.log(JSON.stringify(JSON.parse(raw))); } catch { console.log(raw.replace(/\s+/g, " ").slice(0, 400)); }'; }

[ -f "$PROBE" ] || { echo "the probe is not here" >&2; exit 2; }

LOCK="$STATE/running.lock"
mkdir "$LOCK" 2>/dev/null || exit 0
trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT INT TERM

# Provider credentials live in one file and are read from it, never copied
# into a job definition where they would be a second place to leak from.
SECRETS="${BONFYRE_SECRETS:-$HOME/.bonfyre/secrets/provider.refs.env}"
if [ -f "$SECRETS" ]; then
    set -a; . "$SECRETS"; set +a
fi

export BONFYRE_CMS_DB="$ESTATE/cms.db"
export BONFYRE_FOREIGN_ROOT="$ESTATE/foreign"

sqlite3 "$ASKED" "CREATE TABLE IF NOT EXISTS asked(host TEXT PRIMARY KEY, at TEXT NOT NULL, answered INTEGER NOT NULL DEFAULT 0);" 2>/dev/null || true

# The next hosts this estate has never got a channel from and has never asked.
sqlite3 "$ESTATE/frontier.db" "
  SELECT DISTINCT r.host FROM root r
  WHERE r.host LIKE '%.%'
    AND NOT EXISTS (SELECT 1 FROM channel c WHERE c.root_id = r.root_id
                    AND c.state IN ('admitted','active','eligible'))
  ORDER BY (SELECT COUNT(*) FROM channel c WHERE c.root_id = r.root_id) DESC, r.host
  LIMIT $((SLICE * 6));" > "$STATE/pool.txt" 2>/dev/null || {
    note "could not read the ledger"; exit 2; }

: > "$STATE/slice.txt"
while IFS= read -r host; do
    [ -n "$host" ] || continue
    already=$(sqlite3 "$ASKED" "SELECT 1 FROM asked WHERE host='$(printf '%s' "$host" | sed "s/'/''/g")';" 2>/dev/null)
    [ -n "$already" ] && continue
    echo "$host" >> "$STATE/slice.txt"
    [ "$(wc -l < "$STATE/slice.txt" | tr -d ' ')" -ge "$SLICE" ] && break
done < "$STATE/pool.txt"

count=$(wc -l < "$STATE/slice.txt" | tr -d ' ')
[ "$count" -gt 0 ] || { note "nothing left unasked in this pass"; exit 0; }

stamp=$(date -u +%Y%m%dT%H%M%SZ)
channels="$STATE/found-$stamp.jsonl"
census="$STATE/census-$stamp.json"
answer=$(bun "$PROBE" --hosts "$STATE/slice.txt" --out "$channels" --census "$census" --foreign "$ESTATE/foreign" \
    --workers "$WORKERS" --limit "$count" 2>/dev/null | tail -1)
note "$answer"

# Everything asked is remembered as asked, answer or not, so the next pass
# moves on. A host that answered is marked so, which is what makes the log a
# record of what this estate learned rather than of what it tried.
bun "$PROBE" record-answers --db "$ASKED" --hosts "$STATE/slice.txt" --channels "$channels" >/dev/null

if [ -s "$channels" ]; then
    note "$("$FRONTIER" import --db "$ESTATE/frontier.db" --label CONVENTION_PROBE --jsonl "$channels" 2>&1 | tail -1)"
fi
if [ -s "$census" ] && grep -q '"source"' "$census" 2>/dev/null; then
    note "census: $(bun "$APPLY" --json "$census" --db "$ESTATE/cms.db" \
        --schemas "$ESTATE/schemas" --cms "$REPO/cmd/BonfyreCMS/bonfyre-cms" 2>&1 | tail -1)"
fi

# A second look at hosts that answered nothing, rendered, for the ones whose
# page only exists once a browser has built it. Bounded per pass because the
# credits are finite and this is the only thing in the estate that spends them
# on a host that has already said nothing once.
if [ -f "$REPO/scripts/firecrawl_second_look.mjs" ] && [ -n "${FIRECRAWL_API_KEY2:-}${FIRECRAWL_API_KEY:-}" ]; then
    rendered="$STATE/rendered-$stamp.jsonl"
    note "rendered: $(bun "$REPO/scripts/firecrawl_second_look.mjs" --estate "$ESTATE" \
        --limit "${BONFYRE_RENDER_SLICE:-12}" --out "$rendered" --apply 2>&1 | oneline)"
    if [ -s "$rendered" ]; then
        note "$("$FRONTIER" import --db "$ESTATE/frontier.db" --label RENDERED_SECOND_LOOK --jsonl "$rendered" 2>&1 | tail -1)"
        bun "$REPO/cmd/BonfyreSourceFrontier/tools/qualify_channels.mjs" \
            --db "$ESTATE/frontier.db" --frontier "$FRONTIER" \
            --project "$REPO/components/BonfyreProject/bonfyre-project" \
            --foreign "$ESTATE/foreign" --out "$STATE/rendered-$stamp.json" \
            --limit 60 --workers 6 --cms "$ESTATE/cms.db" \
            --only-label RENDERED_SECOND_LOOK --evidence "$STATE/rendered-evidence.jsonl" \
            >> "$LOG" 2>&1 || true
        if [ -s "$STATE/rendered-$stamp.json" ] && grep -q '"source"' "$STATE/rendered-$stamp.json" 2>/dev/null; then
            note "rendered census: $(bun "$APPLY" --json "$STATE/rendered-$stamp.json" --db "$ESTATE/cms.db" \
                --schemas "$ESTATE/schemas" --cms "$REPO/cmd/BonfyreCMS/bonfyre-cms" 2>&1 | tail -1)"
        fi
    fi
fi

# One running program per pass has its stated needs asked of SourceFrontier.
# Programs are few and the estate is large, so rotating through them costs
# little and means a program's evidence keeps growing without anyone starting
# it by hand. The instance chosen is the one least recently resolved.
if [ -f "$REPO/scripts/resolve_program_sources.mjs" ]; then
    instance=$(sqlite3 "$ESTATE/cms.db" "
      SELECT i.instance_id FROM program_instance i
      LEFT JOIN source_collection c
        ON c.collection_id = i.instance_id || '-sources' AND c.status='published'
      WHERE i.status='published'
      ORDER BY COALESCE(json_extract(c.source_selector,'\$.resolved_at'),'') ASC, i.instance_id
      LIMIT 1;" 2>/dev/null)
    if [ -n "$instance" ]; then
        note "program: $(bun "$REPO/scripts/resolve_program_sources.mjs" --estate "$ESTATE" \
            --repo "$REPO" --instance "$instance" --queries 6 --per-query 25 --apply 2>&1 | oneline)"
    fi
fi

find "$STATE" -name 'found-*.jsonl' -mtime +3 -delete 2>/dev/null || true
find "$STATE" -name 'census-*.json' -mtime +3 -delete 2>/dev/null || true
find "$STATE" -name 'rendered-*' -mtime +3 -delete 2>/dev/null || true
remaining=$(sqlite3 "$ESTATE/frontier.db" "SELECT COUNT(DISTINCT r.host) FROM root r WHERE r.host LIKE '%.%' AND NOT EXISTS (SELECT 1 FROM channel c WHERE c.root_id=r.root_id AND c.state IN ('admitted','active','eligible'));" 2>/dev/null)
answered_total=$(sqlite3 "$ASKED" "SELECT COUNT(*) FROM asked WHERE answered=1;" 2>/dev/null)
asked_total=$(sqlite3 "$ASKED" "SELECT COUNT(*) FROM asked;" 2>/dev/null)
note "asked $asked_total hosts so far, $answered_total answered, $remaining roots still without a channel"
printf '{"asked_this_pass":%s,"asked_total":%s,"answered_total":%s,"roots_without_a_channel":%s,' \
    "$count" "${asked_total:-0}" "${answered_total:-0}" "${remaining:-0}"
printf '"cannot_infer":"a host answering at a conventional address is a host that publishes there; it is not a relationship with this estate, not permission beyond what its robots policy gives, and not a claim about what it carries"}\n'
