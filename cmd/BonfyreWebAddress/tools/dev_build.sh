#!/bin/sh
# dev_build.sh OUTDIR [skip territories...] -- compile main.c into OUTDIR and
# link it against the shared build/ objects, so several people can build at
# once without touching build/web_main.o.  "skip" names front files to leave
# out (live shop life work) while someone else is mid-edit in them.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd); cd "$here"
out=$1; shift; mkdir -p "$out"
defs=""; for t in "$@"; do defs="$defs -DFRONT_SKIP_$(echo "$t" | tr a-z A-Z)"; done
flags="-O2 -Wall -Wextra -Werror -std=c11 -D_DARWIN_C_SOURCE -I../BonfyreLex/src -DBONFYRE_HAVE_ZSTD -I/opt/homebrew/include -I../../engine/core/include -I../../lib/libbonfyre/include -Isrc -I../BonfyreRecipe/src -I../BonfyreCompress/src -I../BonfyreExternalEstate/src"
# shellcheck disable=SC2086
cc $flags $defs -c -o "$out/web_main.o" src/main.c
objs=$(ls build/*.o | grep -v '^build/web_main.o$')
# shellcheck disable=SC2086
cc -o "$out/bonfyre-web-address" "$out/web_main.o" $objs -lsqlite3 -lm -lz /opt/homebrew/lib/libzstd.a
echo "$out/bonfyre-web-address"
