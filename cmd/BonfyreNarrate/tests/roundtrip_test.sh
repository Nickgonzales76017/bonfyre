#!/bin/sh
# What this proves: what is narrated can be heard. The audio is transcribed
# back and must carry the words of the text; with no speech engine nothing is
# written that claims to be narration.
#   sh tests/roundtrip_test.sh ./bonfyre-narrate /path/to/bonfyre-transcribe
set -eu
bin=${1:?bonfyre-narrate required}
transcribe=${2:?bonfyre-transcribe required}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin";; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-narrate-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

printf 'The estate keeps a receipt for every piece of work, and it does not report work that did not happen.\n' > "$tmp/say.txt"
"$bin" "$tmp/say.txt" "$tmp/out" >/dev/null 2>&1
test -s "$tmp/out/artifact.wav"
grep -q '"renderStatus": "completed"' "$tmp/out/artifact.manifest.json"

"$transcribe" "$tmp/out/artifact.wav" "$tmp/heard" >/dev/null 2>&1
heard=$(tr 'A-Z' 'a-z' < "$tmp/heard/transcript.txt")
for word in receipt every piece work report happen; do
  case "$heard" in *"$word"*) ;; *) echo "the audio does not say '$word': $heard" >&2; exit 1;; esac
done

# No engine: no audio, no manifest, and a failing exit.
if env PATH=/usr/sbin "$bin" "$tmp/say.txt" "$tmp/silent" >/dev/null 2>&1; then echo 'reported narration with nothing spoken' >&2; exit 1; fi
[ ! -e "$tmp/silent/artifact.wav" ]
[ ! -e "$tmp/silent/artifact.manifest.json" ]
printf '%s\n' 'narrate roundtrip: PASS'
