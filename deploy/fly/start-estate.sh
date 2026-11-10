#!/bin/sh
# Bring the estate up on a fresh machine with a volume under it.
#
# The volume may be empty (a new home) or may already hold the estate (a
# restart, a redeploy, a move).  Either way this puts the state where the
# organs expect it, warms the pages so the first visitor does not pay for
# composing them, and then serves.
set -eu

state=${BONFYRE_STATE:-/var/lib/bonfyre}
for room in matrix cms foreign/bodies foreign/receipts frontier archive economics cache schemas; do
    mkdir -p "$state/$room"
done

# A seed is the estate's state as it was when the image was built, carried in
# the image only so a brand new volume is not an empty estate.  It is unpacked
# exactly once: after that the volume is the truth and the seed is ignored.
if [ -f /opt/bonfyre/seed/estate.tar.gz ] && [ ! -f "$state/.seeded" ]; then
    echo "first start on this volume: unpacking the estate it was given"
    tar -xzf /opt/bonfyre/seed/estate.tar.gz -C "$state"
    date -u +%FT%TZ > "$state/.seeded"
fi

if [ ! -f "$BONFYRE_MATRIX_DB" ]; then
    echo "no matrix yet: bootstrapping one"
    bonfyre-web-address bootstrap-owned --db "$BONFYRE_MATRIX_DB"
fi

# The census schema the CMS reads, in case this volume has never seen it.
if [ -d /opt/bonfyre/cmd/BonfyreCMS/content-types ] && [ ! -f "$state/cms/bonfyre_cms.db" ]; then
    cp /opt/bonfyre/cmd/BonfyreCMS/content-types/*.json "$state/schemas/" 2>/dev/null || true
    bonfyre-cms schema migrate --db "$BONFYRE_CMS_DB" --schemas "$state/schemas" >/dev/null 2>&1 || true
fi

echo "estate state:"
for db in "$BONFYRE_CMS_DB" "$BONFYRE_SOURCE_FRONTIER_DB" "$BONFYRE_ITEM_ARCHIVE_DB" "$BONFYRE_PRICES_DB"; do
    if [ -f "$db" ]; then
        printf '  %s %s\n' "$(basename "$db")" "$(du -h "$db" | cut -f1)"
    fi
done
printf '  retained bodies: %s\n' "$(find "$state/foreign/bodies" -type f 2>/dev/null | wc -l | tr -d ' ')"

# Compose every public route once before anyone asks for one.  Backgrounded:
# a warm page is worth having, but never worth delaying the door opening for.
( sleep 2
  bonfyre-web-address page-warm --site-root "$BONFYRE_SITE_ROOT" --host aurekai.life 2>&1 \
    | sed 's/^/warm: /' || true ) &

exec bonfyre-web-address serve \
    --db "$BONFYRE_MATRIX_DB" \
    --port "${BONFYRE_WEB_ADDRESS_PORT:-8787}" \
    --workers "${BONFYRE_WEB_ADDRESS_WORKERS:-2}" \
    --site-root "$BONFYRE_SITE_ROOT"
