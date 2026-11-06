#!/bin/sh
# Read the live fronts and every subdomain, and keep one operator goal per open
# defect so it reaches the owner's activity panel and inbox through the account
# work index. A defect that no longer shows is resolved; nothing is published,
# rolled out or repaired from here.
set -u
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
db=${BONFYRE_OPERATOR_DB:-$HOME/.bonfyre/operator/operator.sqlite3}
export BONFYRE_CHROMIUM_BINARY="${BONFYRE_CHROMIUM_BINARY:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-live-sweep.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

bun "$root/scripts/release/front_integrity.mjs" > "$tmp/front" 2>&1
sh "$root/scripts/release/edge_sweep.sh" ${BONFYRE_SWEEP_LOCAL:+--local} > "$tmp/edge" 2>/dev/null
# The source lake: read sources must keep their publisher identity and their
# subjects. The floor is the last cut with no regression; a dry sweep reads it
# and never raises it.
maturity=${BONFYRE_SURFACE_MATURITY_BINARY:-$HOME/.bonfyre/bin/bonfyre-surface-maturity}
lake_floor=${BONFYRE_SUBJECT_LAKE_FLOOR:-$HOME/.bonfyre/state/subject-lake.floor}
: > "$tmp/lake"
if [ -x "$maturity" ]; then
    lake_estate=${BONFYRE_ESTATE_DIR:-$HOME/.bonfyre/estate-local}
    if [ -n "${BONFYRE_SWEEP_DRY:-}" ]; then
        "$maturity" lake-check "$lake_estate" --floor "$lake_floor" > "$tmp/lake" 2>/dev/null || true
    else
        mkdir -p "$(dirname "$lake_floor")"
        "$maturity" lake-check "$lake_estate" --floor "$lake_floor" --write-floor "$lake_floor" > "$tmp/lake" 2>/dev/null || true
    fi
fi
grep -hE '^(FAIL|NOTE)	' "$tmp/front" "$tmp/edge" "$tmp/lake" > "$tmp/all" || true

# One goal per kind and host; widths and repeated hosts fold into its details.
awk -F'\t' '{
    host=$2; kind=(NF>=5?$4:$3); detail=(NF>=5?$5:$4)
    if(kind ~ /^local-missing$|^live-status$/ && host != "*") { key=kind; if(!((key,host) in seen)){seen[key,host]=1; n[key]++}; if(n[key]<=8) d[key]=d[key] host " " detail "; "; h[key]=n[key] " subdomains" }
    else { if(kind ~ /slow/){ detail=kind " " detail; kind="slow" }; key=kind "@" host; if(index(d[key],detail)==0) d[key]=d[key] detail "; "; h[key]=host }
    k[key]=kind; s[key]=(s[key]=="FAIL"?"FAIL":$1)
} END { for(key in k) printf "%s\t%s\t%s\t%s\n", key, s[key], h[key], d[key] }' "$tmp/all" > "$tmp/goals"

now=$(date +%s)
: > "$tmp/sql"
echo "BEGIN;" >> "$tmp/sql"
while IFS="$(printf '\t')" read -r key level where details; do
    kind=${key%%@*}
    case "$kind" in
        run-together) title="Bylines run together on $where" ;;
        unstyled|local-unstyled) title="Sections drawn without their stylesheet on $where" ;;
        collapsed) title="A section collapses to nothing on $where" ;;
        bar-offscreen|under-bar) title="The top bar covers or leaves the page on $where" ;;
        overflow) title="The page is wider than the screen on $where" ;;
        outreach-link) title="Public front links an outreach sample on $where" ;;
        subject-lake) title="Read sources lost publisher identity or subjects ($where)" ;;
        slow) title="Slow page load on $where" ;;
        local-missing) title="Edge pages link assets the upload does not carry ($where)" ;;
        live-status|status|unreachable) title="Pages not answering ($where)" ;;
        *) title="$kind on $where" ;;
    esac
    case "$level$kind" in FAIL*|*slow*) state=blocked priority=70 ;; *) state=queued priority=40 ;; esac
    id=$(printf 'live-sweep:%s' "$key" | shasum | cut -c1-32)
    q=$(printf '%s' "$details" | sed "s/'/''/g" | cut -c1-1800); t=$(printf '%s' "$title" | sed "s/'/''/g")
    echo "INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,created_at,updated_at) VALUES('$id','live-sweep:$key','$t','$q',$priority,'$state','live-sweep',$now,$now) ON CONFLICT(dedupe_key) DO UPDATE SET details=excluded.details,state=CASE WHEN goals.state IN('superseded') THEN goals.state ELSE excluded.state END,updated_at=excluded.updated_at;" >> "$tmp/sql"
done < "$tmp/goals"
echo "UPDATE goals SET state='resolved',updated_at=$now WHERE source='live-sweep' AND state NOT IN('resolved','superseded') AND updated_at<$now;" >> "$tmp/sql"
echo "COMMIT;" >> "$tmp/sql"
[ -n "${BONFYRE_SWEEP_DRY:-}" ] && { cut -f1-3 "$tmp/goals"; exit 0; }
sqlite3 "$db" < "$tmp/sql"
echo "LIVE_SWEEP open=$(wc -l < "$tmp/goals" | tr -d ' ') resolved=$(sqlite3 "$db" "select count(*) from goals where source='live-sweep' and state='resolved'")"
