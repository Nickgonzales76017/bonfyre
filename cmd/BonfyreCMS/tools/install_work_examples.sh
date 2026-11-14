#!/bin/sh
# Generic Bonfyre CMS importer for one named institution.
# Bundled source paths are legacy fixtures, not Aurekai business defaults.
# External files require an explicit namespace; replacement preserves row ids.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
db=${1:-${BONFYRE_CMS_DB:?cms database required}}
cms=${BONFYRE_CMS_BINARY:-"$here/bonfyre-cms"}
data=${BONFYRE_WORK_EXAMPLES:-"$here/data/work-examples.jsonl"}
programs=${BONFYRE_WORK_EXAMPLE_PROGRAMS:-"$here/data/work-example-programs.jsonl"}
if { [ -n "${BONFYRE_WORK_EXAMPLES:-}" ] || [ -n "${BONFYRE_WORK_EXAMPLE_PROGRAMS:-}" ]; } &&
   [ -z "${BONFYRE_CMS_NAMESPACE:-}" ]; then
    echo "work examples: externally owned source requires BONFYRE_CMS_NAMESPACE" >&2
    exit 2
fi
namespace=${BONFYRE_CMS_NAMESPACE:-root}
case "$namespace" in
    [a-zA-Z]* ) ;;
    * ) echo "work examples: invalid CMS namespace" >&2; exit 2 ;;
esac
case "$namespace" in
    *[!a-zA-Z0-9_-]* ) echo "work examples: invalid CMS namespace" >&2; exit 2 ;;
esac
[ "${#namespace}" -le 64 ] || { echo "work examples: CMS namespace too long" >&2; exit 2; }
if [ -n "${BONFYRE_WORK_EXAMPLES:-}${BONFYRE_WORK_EXAMPLE_PROGRAMS:-}" ]; then
    [ -f "$db" ] || { echo "work examples: externally owned CMS DB must already exist" >&2; exit 2; }
fi
[ -f "$data" ] && [ -f "$programs" ] || { echo "work examples: required source file missing" >&2; exit 2; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-work-examples.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
cp "$here/content-types/work_example.json" "$here/content-types/program_template.json" "$tmp/"
"$cms" schema migrate --db "$db" --schemas "$tmp" >/dev/null
field() { printf '%s' "$1" | sed -n "s/.*\"$2\"[[:space:]]*:[[:space:]]*\"\([A-Za-z0-9][A-Za-z0-9._-]*\)\".*/\1/p"; }
t=0 kept=0
while IFS= read -r line; do
    [ -n "$line" ] || continue
    id=$(field "$line" template_id)
    [ -n "$id" ] || { echo "work examples: template line without template_id" >&2; exit 1; }
    if [ "$(sqlite3 -readonly "$db" "SELECT count(*) FROM program_template WHERE namespace='$namespace' AND template_id='$id'")" != 0 ]; then
        kept=$((kept + 1)); continue
    fi
    "$cms" entry create program_template "$line" --ns "$namespace" --db "$db" --schemas "$tmp" >/dev/null
    t=$((t + 1))
done < "$programs"
n=0
while IFS= read -r line; do
    [ -n "$line" ] || continue
    id=$(field "$line" example_id)
    [ -n "$id" ] || { echo "work examples: line without example_id" >&2; exit 1; }
    program=$(field "$line" program_template)
    [ -n "$program" ] || { echo "work examples: $id names no program template" >&2; exit 1; }
    if [ "$(sqlite3 -readonly "$db" "SELECT count(*) FROM program_template WHERE namespace='$namespace' AND status='published' AND template_id='$program'")" = 0 ]; then
        echo "work examples: $id names $program, which is not a published template in $namespace" >&2; exit 1
    fi
    existing_id=$(sqlite3 -readonly "$db" "SELECT id FROM work_example WHERE namespace='$namespace' AND example_id='$id' ORDER BY id LIMIT 1")
    if [ -n "$existing_id" ]; then
        "$cms" entry update work_example "$existing_id" "$line" --db "$db" --schemas "$tmp" >/dev/null
    else
        "$cms" entry create work_example "$line" --ns "$namespace" --db "$db" --schemas "$tmp" >/dev/null
    fi
    n=$((n + 1))
done < "$data"
echo "work examples: $n installed; program templates: $t created, $kept already present (namespace=$namespace)"
