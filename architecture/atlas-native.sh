#!/bin/sh
# The Architecture Atlas is native (cmd/BonfyreAtlas). Build it when missing or
# older than its source, then run it on this checkout's architecture/.
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
organ="$here/../cmd/BonfyreAtlas"
bin="$organ/bonfyre-atlas"
if [ ! -x "$bin" ] || [ "$organ/src/main.c" -nt "$bin" ] || [ "$here/atlas-fields.cue" -nt "$bin" ]; then
  make -s -C "$organ" >/dev/null 2>&1 || { echo "atlas: cannot build $organ (make, cc and cue are needed)" >&2; exit 2; }
fi
BONFYRE_ATLAS_DIR="${BONFYRE_ATLAS_DIR:-$here}" exec "$bin" "$@"
