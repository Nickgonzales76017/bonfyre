#!/bin/sh
# Witness for the icon vocabulary: every icon compiled into glyph_set.h is
# declared in glyph_set.tsv with an Iconify collection:name identity and a
# license on the allowed list, and nothing is declared that is not compiled.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
h="$here/src/glyph_set.h"; m="$here/src/glyph_set.tsv"
allowed=' MIT ISC Apache-2.0 CC-BY-4.0 '
fail() { echo "glyph set: FAIL: $1" >&2; exit 1; }
compiled=$(grep -o 'define BF_ICON_[A-Z0-9_]*' "$h" | sed 's/define BF_ICON_//' | grep -v -x -e OPEN -e CLOSE -e STYLE | sort)
declared=$(grep -v '^#' "$m" | cut -f1 | sort)
[ "$compiled" = "$declared" ] || fail "compiled and declared icons differ: $(printf '%s\n%s\n' "$compiled" "$declared" | sort | uniq -u | tr '\n' ' ')"
tab=$(printf '\t_'); tab=${tab%_}
grep -v '^#' "$m" | while IFS="$tab" read -r name id collection license; do
  case "$id" in "$collection":[a-z0-9]*) ;; *) fail "$name: identity $id is not $collection:name" ;; esac
  case "$allowed" in *" $license "*) ;; *) fail "$name: license $license is not allowed" ;; esac
done
grep -q 'MIT License' "$h" || fail "the Tabler license notice is missing from glyph_set.h"
nav="$here/src/page_core_rendering.h"
for mapping in '"relationship",BF_ICON_GROUP' '"source-library",BF_ICON_BOOK' '"research",BF_ICON_SEARCH' '"narration",BF_ICON_MIC' '"finance",BF_ICON_COINS' '"feed",BF_ICON_RSS'; do
  grep -Fq "$mapping" "$nav" || fail "missing semantic glyph $mapping"
done
for mapping in '"feed-intake","feed"' '"relationship.observe","relationship"' '"source.save","source-library"' '"research.plan","research"' '"narration.create","narration"' '"finance.review","finance"'; do
  grep -Fq "$mapping" "$nav" || fail "missing native operation semantic icon $mapping"
done
for route in '"/sources","rss"' '"/channels","rss"' '"/habitat","life"' '"/market","shop"' '"/programs","board"' '"/finance","cost"'; do
  grep -Fq "$route" "$nav" || fail "missing route semantics $route"
done
echo "glyph set: PASS ($(printf '%s\n' "$declared" | grep -c .) icons)"
