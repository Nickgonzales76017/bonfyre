#!/bin/sh
# What this proves: the score moves with the brief. A brief of whole statements
# taken from its source passes; one cut into pieces fails; one that invents
# fails; one with no source is not scored at all.
#   sh tests/proof_test.sh ./bonfyre-proof <a real document of several paragraphs>
set -eu
bin=${1:?bonfyre-proof required}
source=${2:?a real source document is required}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin";; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-proof-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
field() { sed -n "s/.*\"$1\": \"\{0,1\}\([^\",]*\).*/\1/p" "$2" | head -1; }
brief_dir() { mkdir -p "$tmp/$1"; printf '{"transcript_path": "%s"}\n' "$source" > "$tmp/$1/brief-meta.json"; }

# whole statements: the source's own sentences, one per bullet
brief_dir whole
{ printf '# Brief\n\n## Summary\n'; tr '\n' ' ' < "$source" | sed 's/\([.!?]\) /\1\n/g' | awk 'length($0) > 60 && /^[A-Z]/ && /[.!?]$/ {print "- " $0}' | head -6; } > "$tmp/whole/brief.md"
[ "$(grep -c '^- ' "$tmp/whole/brief.md")" -ge 3 ]
"$bin" score "$tmp/whole" "$tmp/whole-out" >/dev/null
[ "$(field status "$tmp/whole-out/proof-summary.json")" = pass ]
cmp -s "$tmp/whole-out/transcript.txt" "$source"
[ ! -e "$tmp/whole-out/proof-review.json" ]

# the same statements cut in half are fragments
brief_dir cut
{ printf '# Brief\n\n## Summary\n'; grep '^- ' "$tmp/whole/brief.md" | awk '{h=int(length($0)/2); print substr($0,1,h); print "- " substr($0,h+1)}'; } > "$tmp/cut/brief.md"
"$bin" score "$tmp/cut" "$tmp/cut-out" >/dev/null
[ "$(field status "$tmp/cut-out/proof-summary.json")" != pass ]
[ "$(field score "$tmp/cut-out/proof-summary.json")" -lt "$(field score "$tmp/whole-out/proof-summary.json")" ]

# statements the source never made are not grounded
brief_dir invented
printf '# Brief\n\n## Summary\n- Quarterly zeppelin shipments doubled across Antarctica yesterday.\n- Twelve penguins negotiated the cryptocurrency merger overnight.\n' > "$tmp/invented/brief.md"
"$bin" score "$tmp/invented" "$tmp/invented-out" >/dev/null
[ "$(field status "$tmp/invented-out/proof-summary.json")" = fail ]
[ "$(field grounded_statements "$tmp/invented-out/proof-summary.json")" = 0 ]

# nothing to compare with: nothing scored
mkdir -p "$tmp/alone"; cp "$tmp/whole/brief.md" "$tmp/alone/"
if "$bin" score "$tmp/alone" "$tmp/alone-out" >/dev/null 2>&1; then echo 'scored a brief with no source' >&2; exit 1; fi
[ ! -e "$tmp/alone-out/proof-summary.json" ]
printf '%s\n' 'proof: PASS'
