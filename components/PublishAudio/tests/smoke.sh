#!/usr/bin/env bash
# What this proves: a real recording comes out normalized, and with an intro
# the published file is longer than the recording by the spoken intro.
#   tests/smoke.sh /path/to/real-recording.wav
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT/bin/publish_audio.sh"
AUDIO="${1:?a real recording is required}"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/publish-audio-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
dur(){ ffprobe -v error -show_entries format=duration -of csv=p=0 "$1"; }

"$SCRIPT" --audio "$AUDIO" --out "$tmp/plain" >/dev/null
plain="$tmp/plain/$(basename "${AUDIO%.*}")_publishable.wav"
[[ -s "$plain" ]]
[[ "$(ffprobe -v error -show_entries stream=sample_rate -of csv=p=0 "$plain")" = 16000 ]]

printf 'This recording was made for the estate.\n' > "$tmp/intro.txt"
"$SCRIPT" --audio "$AUDIO" --out "$tmp/intro" --intro-text "$tmp/intro.txt" >/dev/null
with="$tmp/intro/$(basename "${AUDIO%.*}")_publishable.wav"
[[ -s "$with" ]]
awk -v a="$(dur "$with")" -v b="$(dur "$plain")" 'BEGIN{exit !(a > b + 0.5)}'

# Something that is not audio fails, and leaves no publishable file.
printf 'not audio\n' > "$tmp/bad.wav"
if "$SCRIPT" --audio "$tmp/bad.wav" --out "$tmp/bad" >/dev/null 2>&1; then echo 'published a file that is not audio' >&2; exit 1; fi
[[ ! -e "$tmp/bad/bad_publishable.wav" ]]
echo "publish audio: PASS"
