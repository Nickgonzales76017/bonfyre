#!/bin/sh
# Give every institution this estate reads an address of its own.
#
# The estate had become all external material and no people: it read twenty-
# eight thousand publishers and showed none of them as anything but a source
# row. Meanwhile the matrix already had institutions, hosts, subjects and
# presences -- the whole model -- with nothing in it, because the rebuild only
# ran bootstrap-owned.
#
# So the two halves are fused here: a publisher the estate actually reads
# becomes an institution in the matrix and gets a host of its own, and the same
# organs that answer aurekai.life answer smh.aurekai.life for a different
# subject. Nothing is invented -- only publishers with material this estate has
# genuinely taken in are addressed, and being addressed is this estate's own
# decision to read them, never a relationship either party agreed to.
#
#   address_the_publishers.sh [--territory life] [--limit 400] [--apply]
set -eu

ESTATE="${BONFYRE_ESTATE:-$HOME/.bonfyre/estate-local}"
REPO="${BONFYRE_REPO:-$HOME/Developer/Bonfyre}"
WEB="$REPO/cmd/BonfyreWebAddress/bonfyre-web-address"
TERRITORY="life"
LIMIT=400
APPLY=0
while [ $# -gt 0 ]; do
    case "$1" in
        --territory) TERRITORY=$2; shift 2 ;;
        --limit) LIMIT=$2; shift 2 ;;
        --apply) APPLY=1; shift ;;
        *) shift ;;
    esac
done

[ -x "$WEB" ] || { echo "the web-address organ is not built" >&2; exit 2; }
[ -f "$ESTATE/frontier.db" ] || { echo "no ledger at $ESTATE/frontier.db" >&2; exit 2; }

STATE="$ESTATE/addressed"; mkdir -p "$STATE"
LOG="$STATE/addressed.log"
note() { echo "$(date -u +%FT%TZ) $*" >> "$LOG"; }

LOCK="$STATE/running.lock"
mkdir "$LOCK" 2>/dev/null || exit 0
trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT INT TERM

# Only publishers with something this estate actually reads. A listing is not
# a reason to give anyone an address.
sqlite3 "$ESTATE/frontier.db" "
  SELECT r.host, COALESCE(NULLIF(r.name,''), r.host)
  FROM root r
  WHERE EXISTS (SELECT 1 FROM channel c
                WHERE c.root_id = r.root_id AND c.state IN ('admitted','active'))
  ORDER BY (SELECT COUNT(*) FROM channel c
            WHERE c.root_id = r.root_id AND c.state IN ('admitted','active')) DESC
  LIMIT $LIMIT;" > "$STATE/publishers.tsv" 2>/dev/null || {
    echo "could not read the ledger" >&2; exit 2; }

addressed=0
skipped=0
while IFS='|' read -r host name; do
    [ -n "$host" ] || continue
    # A label a hostname can carry: the publisher's own host, flattened.
    label=$(printf '%s' "$host" | tr 'A-Z' 'a-z' | sed 's/^www\.//; s/[^a-z0-9]/-/g; s/--*/-/g; s/^-//; s/-$//' | cut -c1-48)
    [ -n "$label" ] || { skipped=$((skipped + 1)); continue; }
    subdomain="$label.aurekai.$TERRITORY"
    institution="inst:$label"
    digest=$(printf '%s' "$host" | shasum -a 256 | cut -d' ' -f1)

    if [ "$APPLY" != "1" ]; then
        printf '%-46s %s\n' "$subdomain" "$name"
        addressed=$((addressed + 1))
        continue
    fi

    # The institution, the subject that is that institution, and the host it
    # stands on. Each is idempotent: running this again re-states the same
    # facts rather than making new ones.
    "$WEB" institution --db "$ESTATE/matrix.db" --id "$institution" --kind publisher \
        --world live --evidence "a publisher this estate reads" >/dev/null 2>&1 || true
    "$WEB" subject --db "$ESTATE/matrix.db" --id "org:$label" --kind institution \
        --digest "$digest" --evidence "a publisher this estate reads" >/dev/null 2>&1 || true
    if "$WEB" domain --db "$ESTATE/matrix.db" --host "$subdomain" \
        --institution "$institution" --territory "$TERRITORY" --scope public \
        --world live --repr HTML \
        --evidence "an address this estate gives a publisher it reads" >/dev/null 2>&1; then
        addressed=$((addressed + 1))
    else
        skipped=$((skipped + 1))
    fi
done < "$STATE/publishers.tsv"

note "addressed $addressed publishers in $TERRITORY, $skipped refused"
printf '{"addressed":%d,"refused":%d,"territory":"%s","applied":%s,' \
    "$addressed" "$skipped" "$TERRITORY" "$([ "$APPLY" = 1 ] && echo true || echo false)"
printf '"cannot_infer":"an address here is this estate reading a publisher and saying so at a place of its own; it is not a relationship either party agreed to, not an endorsement, and not a page the publisher controls"}\n'
