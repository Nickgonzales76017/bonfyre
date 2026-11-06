#!/bin/sh
# Installs released work examples (data/work-examples.jsonl) and the reusable
# Program templates they start (data/work-example-programs.jsonl) into a CMS
# database.  An example already present by example_id is replaced, so editing
# the data and re-running is how copy changes reach the profiles.  A template is
# created only when absent: prepared programs keep the reviewed digest of the
# template they were made from, so an existing template is never rewritten here.
# An example whose program_template is not published in the database is refused.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
db=${1:-${BONFYRE_CMS_DB:?cms database required}}
cms=${BONFYRE_CMS_BINARY:-"$here/bonfyre-cms"}
data=${BONFYRE_WORK_EXAMPLES:-"$here/data/work-examples.jsonl"}
programs=${BONFYRE_WORK_EXAMPLE_PROGRAMS:-"$here/data/work-example-programs.jsonl"}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-work-examples.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
cp "$here/content-types/work_example.json" "$here/content-types/program_template.json" "$tmp/"
"$cms" schema migrate --db "$db" --schemas "$tmp" >/dev/null
field() { printf '%s' "$1" | sed -n "s/.*\"$2\":\"\([A-Za-z0-9][A-Za-z0-9._-]*\)\".*/\1/p"; }
t=0 kept=0
while IFS= read -r line; do
    [ -n "$line" ] || continue
    id=$(field "$line" template_id)
    [ -n "$id" ] || { echo "work examples: template line without template_id" >&2; exit 1; }
    if [ "$(sqlite3 "$db" "SELECT count(*) FROM program_template WHERE namespace='root' AND template_id='$id'")" != 0 ]; then
        kept=$((kept + 1)); continue
    fi
    "$cms" entry create program_template "$line" --ns root --db "$db" --schemas "$tmp" >/dev/null
    t=$((t + 1))
done < "$programs"
n=0
while IFS= read -r line; do
    [ -n "$line" ] || continue
    id=$(field "$line" example_id)
    [ -n "$id" ] || { echo "work examples: line without example_id" >&2; exit 1; }
    program=$(field "$line" program_template)
    [ -n "$program" ] || { echo "work examples: $id names no program template" >&2; exit 1; }
    if [ "$(sqlite3 "$db" "SELECT count(*) FROM program_template WHERE namespace='root' AND status='published' AND template_id='$program'")" = 0 ]; then
        echo "work examples: $id names $program, which is not a published template" >&2; exit 1
    fi
    sqlite3 "$db" "DELETE FROM work_example WHERE namespace='root' AND example_id='$id'"
    "$cms" entry create work_example "$line" --ns root --db "$db" --schemas "$tmp" >/dev/null
    n=$((n + 1))
done < "$data"
echo "work examples: $n installed; program templates: $t created, $kept already present"
