#!/bin/sh
# Witness: the HCP repair pass performs a second recognition of segment audio
# and its counters report what happened. Needs macOS `say`/`afconvert` (or a
# 16 kHz mono WAV in REDECODE_WAV) and a whisper model; absent either, it
# reports SKIP rather than a pass.
set -eu
bin=${1:-./bonfyre-transcribe}
model=${BONFYRE_WHISPER_MODEL:-$HOME/.local/share/whisper/ggml-base.en.bin}
[ -x "$bin" ] || { echo "SKIP: $bin not built"; exit 0; }
[ -f "$model" ] || { echo "SKIP: no whisper model at $model"; exit 0; }
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
wav=${REDECODE_WAV:-}
if [ -z "$wav" ]; then
  command -v say >/dev/null && command -v afconvert >/dev/null || { echo "SKIP: no speech source"; exit 0; }
  say -o "$work/speech.aiff" "The council meets on Thursday evening. Bring the signed lease and two copies of the budget. The gate code changes at noon."
  afconvert -f WAVE -d LEI16@16000 -c 1 "$work/speech.aiff" "$work/speech.wav"
  wav="$work/speech.wav"
fi
fail=0
check() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: got '$2' want '$3'"; fail=1; fi; }
num() { python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(eval(sys.argv[2]))' "$1" "$2"; }

"$bin" "$wav" "$work/all" --model "$model" --no-normalize --no-gpu --redecode-all >/dev/null 2>"$work/all.err"
j="$work/all/$(basename "$wav" .wav).json"
total=$(num "$j" 'd["total_segments"]')
check "segments were recognised" "$([ "$total" -gt 0 ] && echo yes)" yes
check "every segment's audio was recognised a second time" "$(num "$j" 'd["hcp_redecoded_segments"]+d["hcp_redecode_failed_segments"]')" "$total"
check "at least one second recognition ran" "$([ "$(num "$j" 'd["hcp_redecoded_segments"]')" -gt 0 ] && echo yes)" yes
check "each re-decoded segment carries a scored candidate" "$(num "$j" 'all(s["repair"] in ("rejected","accepted") and "repair_candidate_quality" in s for s in d["segments"] if s["repair"]!="failed")')" True
check "accepted count equals segments marked accepted" "$(num "$j" 'd["hcp_repaired_segments"]==sum(s["repair"]=="accepted" for s in d["segments"])')" True
check "an accepted repair keeps the text it replaced and scored higher" "$(num "$j" 'all("text_before_repair" in s and s["text_before_repair"]!=s["text"] and s["repair_candidate_quality"]>s["quality"] for s in d["segments"] if s["repair"]=="accepted")')" True
check "a rejected candidate leaves no replaced text" "$(num "$j" 'all("text_before_repair" not in s for s in d["segments"] if s["repair"]!="accepted")')" True

"$bin" "$wav" "$work/off" --model "$model" --no-normalize --no-gpu --no-redecode >/dev/null 2>&1
k="$work/off/$(basename "$wav" .wav).json"
check "diagnosis alone reports zero re-decoded" "$(num "$k" 'd["hcp_redecoded_segments"]')" 0
check "diagnosis alone repairs nothing" "$(num "$k" 'all(s["repair"]=="none" for s in d["segments"])')" True

"$bin" "$wav" "$work/def" --model "$model" --no-normalize --no-gpu >/dev/null 2>&1
m="$work/def/$(basename "$wav" .wav).json"
check "default run re-decodes no more than HCP flagged" "$(num "$m" 'd["hcp_redecoded_segments"]+d["hcp_redecode_failed_segments"]==d["hcp_flagged_segments"]')" True
[ "$fail" = 0 ] && echo "PASS redecode witness ($total segments)" || { cat "$work/all.err" | tail -5; exit 1; }
