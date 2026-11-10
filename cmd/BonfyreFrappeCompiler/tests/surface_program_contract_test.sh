#!/bin/sh
# A compiled layout is only worth rendering if it is complete and honest about
# what it will not render. This holds both against the app whose own scaffolds
# once crashed the compiler.
set -eu

compiler="$1"
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

# frappe is the app that has to work: every other Frappe application depends on
# it, and it is the one whose nameless-field scaffolds killed the compile.
"$compiler" --root "$root" \
  --emit-surface-programs "$tmp/programs.json" \
  --emit-surface-db "$tmp/surfaces.db" \
  -o "$tmp/pack" "$root/integrations/frappe-bench/apps/frappe" >"$tmp/out"

grep -q 'Rule universe derivation complete: [1-9][0-9]* universes' "$tmp/out"

jq -e '
  def fields: [.layout[].sections[].columns[].fields[]];
  .schema == "bonfyre.frappe-surface-programs.v1"
  and (.programs | length > 200)
  and all(.programs[];
    (fields) as $f
    # Every field lands in exactly one plane of the record: it is either
    # rendered with a presentation or refused with a reason.
    | all($f[]; (.presentation != "")
        and (.organ == "" or .organ == "list" or .organ == "space")
        and (.organ == "" or .presentation == "composition" or .presentation == "place")
        # A field the DocType never named cannot be addressed, so it must be
        # refused rather than emitted with an empty name.
        and (.field != "" or .withheld != ""))
    and (($f | length) == .counts.rendered + .counts.withheld)
    and (.counts.fields == ($f | length) + .counts.layout)
    and (.cannot_infer | length > 0))
' "$tmp/programs.json" >/dev/null || { echo "FAIL: surface program completeness" >&2; exit 1; }

# A secret is never given a presentation a renderer could act on, and a tab
# empty in the metadata is recorded as empty rather than vanishing.
jq -r '
  ([.programs[].layout[].sections[].columns[].fields[] | select(.withheld | contains("secret is never"))] | length) as $secrets
  | ([.programs[].layout[] | select(.empty_in_metadata)] | length) as $empty
  | if $secrets == 0 then error("frappe declares password fields; none were refused")
    elif $empty == 0 then error("frappe declares consecutive tab breaks; none were marked empty")
    else "surface programs: \(.programs | length), secrets refused: \($secrets), empty tabs named: \($empty)" end
' "$tmp/programs.json"

q() { sqlite3 "$tmp/surfaces.db" "$1"; }
programs=$(q "SELECT COUNT(*) FROM frappe_program")
[ "$programs" -gt 200 ] || { echo "FAIL: store holds $programs programs" >&2; exit 1; }
# The store must agree with itself: a program's counts are its own rows. An
# empty tab contributes a row that is neither rendered nor withheld.
short=$(q "SELECT COUNT(*) FROM (SELECT p.program_id FROM frappe_program p
  JOIN frappe_field f ON f.program_id = p.program_id
  GROUP BY p.program_id HAVING COUNT(f.ord) < p.rendered + p.withheld)")
[ "$short" -eq 0 ] || { echo "FAIL: $short programs hold fewer rows than they claim" >&2; exit 1; }
# Every program is reachable at an address a subject identifier can carry.
unaddressable=$(q "SELECT COUNT(*) FROM frappe_program WHERE slug = '' OR slug GLOB '*[^a-z0-9-]*'")
[ "$unaddressable" -eq 0 ] || { echo "FAIL: $unaddressable slugs are not addressable" >&2; exit 1; }
echo "store: $programs programs, $(q "SELECT COUNT(*) FROM frappe_field") field rows, every slug addressable"

echo "surface program contract: PASS"
