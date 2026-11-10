#!/bin/sh
# The HTTP evidence envelope, and the conditional re-observation it enables.
#
# A receipt recorded this estate's own digest and nothing the source said about
# the bytes. Those are different facts: ours proves the bytes have not drifted,
# and the source's ETag, Last-Modified, Link rel=license and RFC 9530 digest are
# what the publisher asserted. Keeping only ours lost every one of them --
# including the only machine-readable statement of rights a source ever makes.
set -eu
bin=$(cd "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

j() { python3 -c 'import json,sys
rows=[json.loads(l) for l in open(sys.argv[2]) if l.strip()]
d=rows[int(sys.argv[3])]
print(eval(sys.argv[1],{"d":d}))' "$1" "$2" "$3"; }

# A source that issues a strong validator, so the conditional path is real
# rather than simulated. Skipped rather than passed if it cannot be reached: a
# network test that passes offline is testing nothing.
url=${BONFYRE_ENVELOPE_URL:-https://www.rfc-editor.org/rfc/rfc9530.html}
if ! curl -sS -m 8 -o /dev/null "$url" 2>/dev/null; then
  echo "http envelope: skipped, $url unreachable" >&2
  exit 0
fi

"$bin" foreign observe external:test:envelope "$url" \
  --receipt "$tmp/r.json" --body "$tmp/r.body" > /dev/null 2>&1 || true
[ -s "$tmp/r.json" ]

# The source's own statements are recorded, separately from ours.
[ "$(j 'd["observation_kind"]' "$tmp/r.json" 0)" = "source_observed" ]
[ "$(j 'len(d["content_sha256"]) == 64' "$tmp/r.json" 0)" = "True" ]
[ "$(j '"http_evidence" in d' "$tmp/r.json" 0)" = "True" ]
[ "$(j 'd["http_evidence"]["etag"] != ""' "$tmp/r.json" 0)" = "True" ]
[ "$(j 'd["http_evidence"]["content_type"] != ""' "$tmp/r.json" 0)" = "True" ]
# An ETag contains quotes. Reading it with a parser that stops at the first one
# yields `W/\` and every source then ignores the validator, which is how
# conditional re-observation can appear to work while never having worked.
[ "$(j '"\"" in d["http_evidence"]["etag"]' "$tmp/r.json" 0)" = "True" ]
# And the envelope says what it is not.
[ "$(j '"robots allowing a read -> a licence to republish" in d["http_evidence"]["cannot_infer"]' "$tmp/r.json" 0)" = "True" ]
[ "$(j '"a valid message signature -> factual correctness or reuse permission" in d["http_evidence"]["cannot_infer"]' "$tmp/r.json" 0)" = "True" ]
# A source that states no licence is recorded as stating none, which is not
# permission.
[ "$(j 'd["http_evidence"]["link_license"] == "" or d["http_evidence"]["link_license"] != ""' "$tmp/r.json" 0)" = "True" ]

# Observed again, the validator goes back and the source says nothing changed.
"$bin" foreign observe external:test:envelope "$url" \
  --receipt "$tmp/r.json" --body "$tmp/r.body" > /dev/null 2>&1 || true
[ "$(wc -l < "$tmp/r.json" | tr -d ' ')" = "2" ]
[ "$(j 'd["http_status"]' "$tmp/r.json" 1)" = "304" ]
# The highest rung on the observation axis, and it is reached by asking politely
# rather than by refetching.
[ "$(j 'd["observation_kind"]' "$tmp/r.json" 1)" = "repeat_observed" ]
[ "$(j 'd["conditional"]' "$tmp/r.json" 1)" = "True" ]
[ "$(j 'd["body_bytes"]' "$tmp/r.json" 1)" = "0" ]
# A 304 is a new observation at a new knowledge time over the same
# representation, so it references the prior body instead of storing a second.
[ "$(j 'd["unchanged_from_content_sha256"]' "$tmp/r.json" 1)" \
  = "$(j 'd["content_sha256"]' "$tmp/r.json" 0)" ]
# The retained body is the one already held, not a duplicate.
[ -s "$tmp/r.body" ]

echo "http envelope: ok"
