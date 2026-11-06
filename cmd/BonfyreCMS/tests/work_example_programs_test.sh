#!/bin/sh
# Released work examples bind to reusable Program templates.
# Proves: every example names a template the installer publishes; a second run
# leaves existing templates untouched (prepared programs keep their reviewed
# digest); an example naming an unpublished template is refused; the schema and
# data agree (no undeclared or unused field, no `terms` drift); and the names of
# the material an example was built with stay in its provenance, never in the
# reusable template a visitor starts.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms=${BONFYRE_TEST_CMS_BINARY:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
tool="$root/cmd/BonfyreCMS/tools/install_work_examples.sh"
data="$root/cmd/BonfyreCMS/data/work-examples.jsonl"
programs="$root/cmd/BonfyreCMS/data/work-example-programs.jsonl"
schema="$root/cmd/BonfyreCMS/content-types/work_example.json"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/work-example-programs.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
fail() { echo "FAIL: $*" >&2; exit 1; }
install() { BONFYRE_CMS_BINARY="$cms" sh "$tool" "$@" 2>"$tmp/err"; }

# Schema and data agree: every data key is declared, and nothing declared is dead.
grep -q '"terms"' "$schema" && fail "work_example still declares the unused terms field"
keys=$(sed 's/"\([a-z_]*\)":/\n\1:/g' "$data" | sed -n 's/^\([a-z_]*\):.*/\1/p' | sort -u)
for key in $keys; do grep -q "\"$key\": {" "$schema" || fail "data field $key is not in the work_example schema"; done
grep -q '"program_template": {' "$schema" || fail "work_example cannot name a program template"

db="$tmp/cms.db"
out=$(install "$db") || { cat "$tmp/err" >&2; fail "installer refused the shipped data"; }
examples=$(grep -c . "$data")
case "$out" in *"$examples installed; program templates: $(grep -c . "$programs") created, 0 already present"*) ;; *) fail "first install: $out";; esac
unbound=$(sqlite3 "$db" "SELECT count(*) FROM work_example w WHERE NOT EXISTS (SELECT 1 FROM program_template t WHERE t.namespace='root' AND t.status='published' AND t.template_id=w.program_template)")
[ "$unbound" = 0 ] || fail "$unbound examples name no published template"
[ "$(sqlite3 "$db" "SELECT count(*) FROM program_template WHERE subject_kinds LIKE '%\"person\"%'")" = "$(grep -c . "$programs")" ] || fail "a template cannot be started from a personal account"

# A changed template is not rewritten under prepared programs.
sqlite3 "$db" "UPDATE program_template SET description='edited by an owner' WHERE template_id='workbook-assumption-review'"
out=$(install "$db") || fail "second install refused"
case "$out" in *"0 created, $(grep -c . "$programs") already present"*) ;; *) fail "second install: $out";; esac
[ "$(sqlite3 "$db" "SELECT description FROM program_template WHERE template_id='workbook-assumption-review'")" = "edited by an owner" ] || fail "installer rewrote an existing template"
[ "$(sqlite3 "$db" "SELECT count(*) FROM work_example")" = "$examples" ] || fail "reinstall duplicated examples"

# An example naming a missing or unpublished template is refused before it is written.
printf '%s\n' '{"example_id":"orphan","pack":"experience.finance-workbooks.workspace","program_template":"no-such-template","territories":"work","position":9,"kicker":"k","headline":"h","dek":"d","made_with":"m","observed_at":"2026-10-29T00:00:00Z","picture":"/p.png","picture_alt":"a","status":"published"}' >"$tmp/orphan.jsonl"
BONFYRE_WORK_EXAMPLES="$tmp/orphan.jsonl" install "$db" >/dev/null && fail "an unbound example was installed"
grep -q "not a published template" "$tmp/err" || fail "refusal did not name the template"
sqlite3 "$db" "UPDATE program_template SET status='draft' WHERE template_id='research-to-migration'"
install "$db" >/dev/null && fail "an example bound to a draft template was installed"
[ "$(sqlite3 "$db" "SELECT count(*) FROM work_example WHERE example_id='orphan'")" = 0 ] || fail "refused example was written"

# Provenance stays with the example: no maker named in made_with appears in a template.
sed -n 's/.*"made_with":"\([^"]*\)".*/\1/p' "$data" | tr ',' '\n' | sed 's/ and /\n/g; s/^ *//; s/ *$//' | while IFS= read -r maker; do
    [ ${#maker} -ge 4 ] || continue
    case "$maker" in an\ *|a\ *|open\ *|observed\ *) continue;; esac
    grep -qiF "$maker" "$programs" && fail "template text names provenance material: $maker"
done
echo "work example programs: ok ($examples examples bound)"
