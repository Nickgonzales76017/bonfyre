#!/usr/bin/env env bash
# deposition_pipeline.sh — run a full forensic pipeline on an audio deposition
#
# Stage 0: ensure bonfyre-transcribe daemon is running
# Stage 1: transcribe audio via daemon socket (falls back to direct spawn)
# Stage 2: trauma-filter pass — suppress graphic content, extract who/what/when
# Stage 3: bonfyre-brief  — structured summary with corroboration flags
# Stage 4: bonfyre-proof  — evidence attestation packet
# Stage 5: bonfyre-pack   — archive-ready deliverable bundle
#
# Usage:
#   ./deposition_pipeline.sh --audio <file> --out <dir> [--label <tag>] [--dry-run]
#
# Example:
#   ./deposition_pipeline.sh --audio corpus/audio/depo_jane_doe.m4a \
#                             --out /tmp/epstein-bench/depo_jane_doe \
#                             --label "jane_doe"
set -euo pipefail

# ── defaults ──────────────────────────────────────────────────────────────────
AUDIO_FILE=""
OUT_DIR=""
LABEL=""
DRY_RUN=0
SOCKET="${BONFYRE_TRANSCRIBE_SOCKET:-/tmp/bonfyre-transcribe.sock}"
DAEMON_BIN="bonfyre-transcribe"
BRIEF_BIN="bonfyre-brief"
PROOF_BIN="bonfyre-proof"
PACK_BIN="bonfyre-pack"
WHISPER_MODEL="${BONFYRE_WHISPER_MODEL:-}"

# Trauma-filter: named entity labels to preserve, graphic content patterns to redact
TF_PRESERVE_LABELS=(PERSON ORG GPE DATE TIME)
# These patterns in the transcript will be marked [GRAPHIC] and excluded from the summary
TF_GRAPHIC_RE='(rape[sd]?|assaulted?|forced|naked|undress|genitalia|penetrat)'

# ── parse args ────────────────────────────────────────────────────────────────
usage() {
  cat <<EOF
Usage: $0 --audio FILE --out DIR [--label TAG] [--dry-run] [--model NAME]

  --audio  INPUT    Audio deposition file (mp3/m4a/wav/ogg/flac)
  --out    DIR      Output directory for all artifacts
  --label  TAG      Short label used in filenames (default: basename of audio)
  --model  NAME     Whisper model override (tiny/small/medium/large/fpq)
  --dry-run         Print commands without running
EOF
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --audio)  AUDIO_FILE="$2"; shift 2 ;;
    --out)    OUT_DIR="$2";    shift 2 ;;
    --label)  LABEL="$2";     shift 2 ;;
    --model)  WHISPER_MODEL="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;     shift ;;
    -h|--help) usage ;;
    *) echo "Unknown argument: $1"; usage ;;
  esac
done

[[ -z "$AUDIO_FILE" ]] && { echo "ERROR: --audio required"; usage; }
[[ -z "$OUT_DIR"    ]] && { echo "ERROR: --out required";   usage; }
[[ -f "$AUDIO_FILE" ]] || { echo "ERROR: audio file not found: $AUDIO_FILE"; exit 1; }

LABEL="${LABEL:-$(basename "${AUDIO_FILE%.*}")}"
AUDIO_ABS="$(cd "$(dirname "$AUDIO_FILE")" && pwd)/$(basename "$AUDIO_FILE")"

# ── helpers ───────────────────────────────────────────────────────────────────
run() {
  echo "[RUN] $*"
  [[ $DRY_RUN -eq 1 ]] || "$@"
}

run_shell() {
  echo "[SHELL] $*"
  [[ $DRY_RUN -eq 1 ]] || eval "$*"
}

log() { echo "==> $*"; }

require_bin() {
  local bin="$1"
  if ! command -v "$bin" &>/dev/null; then
    echo "ERROR: $bin not found in PATH" >&2
    exit 1
  fi
}

sha256_file() {
  local f="$1"
  if command -v sha256sum &>/dev/null; then
    sha256sum "$f" | awk '{print $1}'
  else
    shasum -a 256 "$f" | awk '{print $1}'
  fi
}

# ── stage 0: daemon ───────────────────────────────────────────────────────────
ensure_daemon() {
  log "Stage 0: ensure transcribe daemon is running"
  if [[ -S "$SOCKET" ]]; then
    log "  daemon socket already present: $SOCKET"
    return 0
  fi

  require_bin "$DAEMON_BIN"
  local daemon_cmd="$DAEMON_BIN serve"
  [[ -n "$WHISPER_MODEL" ]] && daemon_cmd="$daemon_cmd --model $WHISPER_MODEL"

  log "  starting daemon: $daemon_cmd"
  if [[ $DRY_RUN -eq 0 ]]; then
    nohup $daemon_cmd >/tmp/bonfyre-transcribe-daemon.log 2>&1 &
    local pid=$!
    log "  daemon pid=$pid, waiting for socket…"
    local waited=0
    while [[ ! -S "$SOCKET" && $waited -lt 30 ]]; do
      sleep 1
      (( waited++ ))
    done
    if [[ ! -S "$SOCKET" ]]; then
      echo "WARN: daemon socket did not appear after ${waited}s; continuing anyway" >&2
    else
      log "  daemon ready (${waited}s)"
    fi
  else
    echo "[DRY-RUN] nohup $daemon_cmd &"
  fi
}

# ── stage 1: transcribe ────────────────────────────────────────────────────────
transcribe() {
  log "Stage 1: transcribe $AUDIO_ABS"
  local transcript_dir="$OUT_DIR/transcripts"
  run mkdir -p "$transcript_dir"

  # Try daemon socket first
  if [[ -S "$SOCKET" && $DRY_RUN -eq 0 ]]; then
    log "  trying daemon socket: $SOCKET"
    local req="${AUDIO_ABS}\t${transcript_dir}\n"
    local resp
    resp=$(printf "%b" "$req" | nc -U "$SOCKET" 2>/dev/null || true)
    if [[ "$resp" == ok* ]]; then
      RAW_TRANSCRIPT="$(echo "$resp" | cut -f2)"
      log "  daemon returned: $RAW_TRANSCRIPT"
      return 0
    else
      log "  daemon failed ($resp), falling back to direct spawn"
    fi
  fi

  # Fallback: direct spawn
  require_bin "$DAEMON_BIN"
  local transcript_stem="${LABEL}"
  run "$DAEMON_BIN" transcribe \
    ${WHISPER_MODEL:+--model "$WHISPER_MODEL"} \
    --out "$transcript_dir" \
    "$AUDIO_ABS"

  # Attempt to locate the output .txt — bonfyre-transcribe writes <stem>.txt
  if [[ $DRY_RUN -eq 0 ]]; then
    RAW_TRANSCRIPT="$(find "$transcript_dir" -name '*.txt' -newer "$AUDIO_ABS" \
      | sort -t/ -k1 | head -1)"
    if [[ -z "$RAW_TRANSCRIPT" ]]; then
      RAW_TRANSCRIPT="$transcript_dir/${transcript_stem}.txt"  # best guess
    fi
  else
    RAW_TRANSCRIPT="$transcript_dir/${LABEL}.txt"
  fi
  log "  raw transcript: $RAW_TRANSCRIPT"
}

# ── stage 2: trauma filter ────────────────────────────────────────────────────
trauma_filter() {
  log "Stage 2: trauma filter"
  local filtered="$OUT_DIR/${LABEL}.filtered.txt"
  local redacted_count=0

  if [[ $DRY_RUN -eq 1 ]]; then
    echo "[DRY-RUN] python3 trauma_filter (graphic → [GRAPHIC], who/what/when extracted)"
    FILTERED_TRANSCRIPT="$filtered"
    return 0
  fi

  [[ -f "$RAW_TRANSCRIPT" ]] || { echo "WARN: raw transcript not found, skipping filter"; FILTERED_TRANSCRIPT="$RAW_TRANSCRIPT"; return; }

  python3 - "$RAW_TRANSCRIPT" "$filtered" <<'PYEOF'
import re, sys, pathlib

src  = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
out  = pathlib.Path(sys.argv[2])
out.parent.mkdir(parents=True, exist_ok=True)

GRAPHIC = re.compile(
    r'(raped?|assaulted?|forced him|forced her|forced me|naked|undress\w*|'
    r'genitali\w*|penetrat\w*|fondl\w*|molest\w*)',
    re.IGNORECASE
)

PERSON_RE = re.compile(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b')
DATE_RE   = re.compile(r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|'
                       r'(?:January|February|March|April|May|June|July|August|'
                       r'September|October|November|December)\s+\d{1,2},?\s+\d{4})\b',
                       re.IGNORECASE)

lines_out = []
redacted = 0

for line in src.splitlines():
    if GRAPHIC.search(line):
        line = GRAPHIC.sub('[GRAPHIC]', line)
        redacted += 1
    lines_out.append(line)

filtered_text = "\n".join(lines_out)

# Extract who/what/when section
persons   = sorted(set(PERSON_RE.findall(filtered_text)))[:200]
dates     = sorted(set(DATE_RE.findall(filtered_text)))[:100]

header = (
    f"# Filtered Transcript\n"
    f"# Source: {sys.argv[1]}\n"
    f"# Graphic segments redacted: {redacted}\n\n"
    f"## Persons Mentioned\n" +
    "\n".join(f"- {p}" for p in persons) +
    f"\n\n## Dates Mentioned\n" +
    "\n".join(f"- {d}" for d in dates) +
    "\n\n---\n\n"
)

out.write_text(header + filtered_text, encoding="utf-8")
print(f"Redacted {redacted} graphic segment(s). Persons: {len(persons)}, Dates: {len(dates)}")
PYEOF

  FILTERED_TRANSCRIPT="$filtered"
  log "  filtered transcript: $FILTERED_TRANSCRIPT"
}

# ── stage 3: bonfyre-brief ─────────────────────────────────────────────────────
run_brief() {
  log "Stage 3: bonfyre-brief"
  local brief_out="$OUT_DIR/brief"
  run mkdir -p "$brief_out"

  if command -v "$BRIEF_BIN" &>/dev/null; then
    run "$BRIEF_BIN" \
      --input "$FILTERED_TRANSCRIPT" \
      --out "$brief_out" \
      --label "$LABEL"
  else
    log "  $BRIEF_BIN not in PATH — writing placeholder"
    if [[ $DRY_RUN -eq 0 ]]; then
      cat > "$brief_out/${LABEL}.brief.txt" <<TXT
[BRIEF PLACEHOLDER]
Source: ${FILTERED_TRANSCRIPT}
Label:  ${LABEL}
Action: run bonfyre-brief when available
TXT
    fi
  fi
}

# ── stage 4: bonfyre-proof ─────────────────────────────────────────────────────
run_proof() {
  log "Stage 4: bonfyre-proof"
  local proof_out="$OUT_DIR/proof"
  run mkdir -p "$proof_out"

  if command -v "$PROOF_BIN" &>/dev/null; then
    run "$PROOF_BIN" \
      --input "$FILTERED_TRANSCRIPT" \
      --out "$proof_out" \
      --label "$LABEL"
  else
    log "  $PROOF_BIN not in PATH — writing placeholder"
    if [[ $DRY_RUN -eq 0 ]]; then
      local h
      h=$(sha256_file "$FILTERED_TRANSCRIPT" 2>/dev/null || echo "unavailable")
      cat > "$proof_out/${LABEL}.proof.json" <<JSON
{
  "label": "${LABEL}",
  "source_transcript": "${FILTERED_TRANSCRIPT}",
  "sha256": "${h}",
  "generated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "status": "PLACEHOLDER — run bonfyre-proof when available"
}
JSON
    fi
  fi
}

# ── stage 5: bonfyre-pack ─────────────────────────────────────────────────────
run_pack() {
  log "Stage 5: bonfyre-pack"

  if command -v "$PACK_BIN" &>/dev/null; then
    run "$PACK_BIN" \
      --input "$OUT_DIR" \
      --out "$OUT_DIR/${LABEL}.pack.tar.gz" \
      --label "$LABEL"
  else
    log "  $PACK_BIN not in PATH — creating tar.gz directly"
    local pack="$OUT_DIR/${LABEL}.pack.tar.gz"
    if [[ $DRY_RUN -eq 0 ]]; then
      tar -czf "$pack" -C "$(dirname "$OUT_DIR")" "$(basename "$OUT_DIR")"
      log "  packed: $pack ($(du -sh "$pack" | cut -f1))"
    else
      echo "[DRY-RUN] tar -czf $pack …"
    fi
  fi
}

# ── manifest ──────────────────────────────────────────────────────────────────
write_manifest() {
  log "Writing manifest"
  local manifest="$OUT_DIR/MANIFEST.md"
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "[DRY-RUN] write $manifest"
    return
  fi
  cat > "$manifest" <<MD
# Deposition Pipeline Manifest

| Field | Value |
|---|---|
| Label | ${LABEL} |
| Audio Input | ${AUDIO_ABS} |
| Audio SHA-256 | $(sha256_file "$AUDIO_ABS" 2>/dev/null || echo n/a) |
| Raw Transcript | ${RAW_TRANSCRIPT:-n/a} |
| Filtered Transcript | ${FILTERED_TRANSCRIPT:-n/a} |
| Processed | $(date -u +%Y-%m-%dT%H:%M:%SZ) |

## Artifacts
$(find "$OUT_DIR" -type f ! -name 'MANIFEST.md' | sort | while read -r f; do
  echo "- \`${f#$OUT_DIR/}\`  $(sha256_file "$f" 2>/dev/null | cut -c1-16)…"
done)
MD
  log "  manifest: $manifest"
}

# ── run ───────────────────────────────────────────────────────────────────────
log "Deposition pipeline: $LABEL"
log "Audio:  $AUDIO_ABS"
log "Output: $OUT_DIR"
[[ $DRY_RUN -eq 1 ]] && log "(DRY-RUN mode)"

mkdir -p "$OUT_DIR"

ensure_daemon
transcribe
trauma_filter
run_brief
run_proof
run_pack
write_manifest

log "Pipeline complete: $OUT_DIR"
