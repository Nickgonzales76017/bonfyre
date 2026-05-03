#!/usr/bin/env bash
# acquire_epstein_corpus.sh — pull all three Epstein corpus sources locally
#
# Sources
#   1. FBI Vault FOIA release (Parts 01-22)
#      https://vault.fbi.gov/jeffrey-epstein
#
#   2. EFTA Dump 1 — 3,100 PDFs from Epstein Files Transparency Act (Dec 2025)
#      archive.org identifier: efta-00000006
#      Filed at DOJ as EFTA00000001–EFTA00003100
#
#   3. EFTA Data Set 1-7 — Dec 19 2025 DOJ Epstein Library release
#      archive.org identifier: data-set-1
#      Includes Maxwell grand jury transcripts + investigative files
#
#   4. CourtListener / RECAP — Giuffre v. Maxwell (1:15-cv-07433, S.D.N.Y.)
#      Docket 4355835, ~3,300 entries, publicly available docs via RECAP
#
# Usage:
#   ./acquire_epstein_corpus.sh [--out-dir /path/to/corpus] [--skip-ia] [--skip-fbi] [--skip-cl]
#
# Requirements:
#   curl, sha256sum (or shasum -a 256), jq
#   Optional: internetarchive CLI (pip install internetarchive) for bulk IA downloads
#
# Outputs:
#   $OUT_DIR/
#     fbi_vault/          Part01.pdf … Part22.pdf
#     efta_dump1/         EFTA00000001.pdf … EFTA00003100.pdf  (via IA)
#     efta_dataset17/     EFTA00008744.pdf + related files
#     court_15cv7433/     <doc_id>.pdf  (publicly available RECAP docs)
#     manifest.sha256     sha256 of every file

set -euo pipefail

# ── defaults ────────────────────────────────────────────────────────────────
OUT_DIR="${BONFYRE_CORPUS_DIR:-$(pwd)/epstein-corpus}"
SKIP_IA=0
SKIP_FBI=0
SKIP_CL=0
DRY_RUN=0

# ── parse args ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)  OUT_DIR="$2"; shift 2 ;;
    --skip-ia)  SKIP_IA=1; shift ;;
    --skip-fbi) SKIP_FBI=1; shift ;;
    --skip-cl)  SKIP_CL=1; shift ;;
    --dry-run)  DRY_RUN=1; shift ;;
    *) echo "unknown arg: $1"; exit 1 ;;
  esac
done

# ── helpers ──────────────────────────────────────────────────────────────────
log()  { echo "[$(date +%H:%M:%S)] $*"; }
die()  { echo "FATAL: $*" >&2; exit 1; }

checksum_cmd() {
  if command -v sha256sum &>/dev/null; then
    echo "sha256sum"
  elif command -v shasum &>/dev/null; then
    echo "shasum -a 256"
  else
    die "no sha256 tool found"
  fi
}
SHA256=$(checksum_cmd)

fetch() {
  local url="$1" dest="$2"
  [[ -f "$dest" ]] && { log "skip (exists): $dest"; return 0; }
  if [[ $DRY_RUN -eq 1 ]]; then
    log "DRY: curl $url -> $dest"
  else
    curl -fsSL --retry 3 --retry-delay 2 -o "$dest" "$url"
  fi
}

ia_download() {
  local identifier="$1" dest_dir="$2"
  if command -v ia &>/dev/null; then
    log "ia download $identifier -> $dest_dir"
    # No --glob: let ia download all files; --no-clobber ensures resume safety.
    # PDF filtering is handled downstream by find -name '*.pdf' -o -name '*.PDF'
    [[ $DRY_RUN -eq 0 ]] && ia download "$identifier" --destdir "$dest_dir" --ignore-existing
  else
    log "internetarchive CLI not found — using curl manifest fallback"
    ia_curl_fallback "$identifier" "$dest_dir"
  fi
}

# Fallback: fetch the IA JSON metadata, parse file list, curl each PDF
ia_curl_fallback() {
  local identifier="$1" dest_dir="$2"
  local meta_url="https://archive.org/metadata/${identifier}"
  local meta_file="${dest_dir}/.ia_meta.json"

  [[ $DRY_RUN -eq 0 ]] && curl -fsSL --retry 3 -o "$meta_file" "$meta_url"

  if [[ $DRY_RUN -eq 0 && -f "$meta_file" ]]; then
    local base_url="https://archive.org/download/${identifier}"
    # extract server-relative paths for all PDF files
    jq -r '.files[] | select(.name | endswith(".pdf")) | .name' "$meta_file" \
    | while IFS= read -r fname; do
        local encoded
        encoded=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$fname")
        local dest="${dest_dir}/${fname##*/}"
        mkdir -p "$(dirname "${dest_dir}/${fname}")"
        fetch "${base_url}/${encoded}" "${dest_dir}/${fname}"
      done
  else
    log "DRY: would fetch all PDFs from https://archive.org/download/${identifier}/"
  fi
}

append_manifest() {
  local dir="$1"
  log "hashing $dir ..."
  if [[ $DRY_RUN -eq 0 ]]; then
    find "$dir" \( -iname '*.pdf' \) | sort | while IFS= read -r f; do
      $SHA256 "$f"
    done >> "${OUT_DIR}/manifest.sha256"
  fi
}

# ── setup ────────────────────────────────────────────────────────────────────
mkdir -p \
  "${OUT_DIR}/fbi_vault" \
  "${OUT_DIR}/efta_dump1" \
  "${OUT_DIR}/efta_dataset17" \
  "${OUT_DIR}/court_15cv7433"

[[ -f "${OUT_DIR}/manifest.sha256" ]] || touch "${OUT_DIR}/manifest.sha256"

# ── 1. FBI Vault (Parts 01-22) ───────────────────────────────────────────────
if [[ $SKIP_FBI -eq 0 ]]; then
  log "=== FBI Vault FOIA release ==="
  FBI_BASE="https://vault.fbi.gov/jeffrey-epstein"

  for i in $(seq -w 01 22); do
    # Parts 01-21 use plain numbers; Part 22 has "(Final)" suffix
    if [[ "$i" == "22" ]]; then
      label="Jeffrey Epstein Part 22 (Final)"
    else
      label="Jeffrey Epstein Part ${i}"
    fi
    encoded_label=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$label")
    url="${FBI_BASE}/${encoded_label}/at_download/file"
    dest="${OUT_DIR}/fbi_vault/Part${i}.pdf"
    log "FBI Vault Part ${i}"
    fetch "$url" "$dest"
  done

  append_manifest "${OUT_DIR}/fbi_vault"
fi

# ── 2. EFTA Dump 1 (archive.org: efta-00000006) ─────────────────────────────
# 3,100 PDFs named EFTA00000001–EFTA00003100, uploaded 2026-02-23
# Original DOJ Epstein Files Transparency Act data dump, Dec 2025
if [[ $SKIP_IA -eq 0 ]]; then
  log "=== EFTA Dump 1 (efta-00000006) ==="
  ia_download "efta-00000006" "${OUT_DIR}/efta_dump1"
  append_manifest "${OUT_DIR}/efta_dump1"

  # ── 3. EFTA Data Set 1-7 (archive.org: data-set-1) ───────────────────────
  # DOJ Dec 19 2025 release — grand jury transcripts + investigative files
  log "=== EFTA Data Set 1-7 (data-set-1) ==="
  ia_download "data-set-1" "${OUT_DIR}/efta_dataset17"
  append_manifest "${OUT_DIR}/efta_dataset17"
fi

# ── 4. CourtListener — Giuffre v. Maxwell 1:15-cv-07433 ─────────────────────
# Docket 4355835 on CourtListener; documents available via RECAP (no auth needed
# for publicly unsealed docs). We hit the REST v4 search endpoint to enumerate
# available PDFs, then download each.
#
# Note: PACER-gated docs require a PACER account + RECAP browser extension or
# the paid PACER API. This script only downloads freely available RECAP copies.
if [[ $SKIP_CL -eq 0 ]]; then
  log "=== CourtListener — Giuffre v. Maxwell (15-cv-07433) ==="
  CL_DOCKET_ID=4355835
  CL_API="https://www.courtlistener.com/api/rest/v4"
  CL_OUT="${OUT_DIR}/court_15cv7433"

  # Enumerate available (free) RECAP docs page by page
  page=1
  total=0
  while true; do
    url="${CL_API}/recap-documents/?docket_entry__docket=${CL_DOCKET_ID}&is_available=true&page=${page}&page_size=100"
    meta_file="${CL_OUT}/.cl_page${page}.json"
    log "  CourtListener page ${page}"
    if [[ $DRY_RUN -eq 0 ]]; then
      curl -fsSL --retry 3 -H "Accept: application/json" -o "$meta_file" "$url"
      count=$(jq '.count' "$meta_file" 2>/dev/null || echo 0)
      results=$(jq '.results | length' "$meta_file" 2>/dev/null || echo 0)
      [[ $results -eq 0 ]] && break
      jq -r '.results[] | select(.filepath_local != null) | "https://storage.courtlistener.com/" + .filepath_local' \
        "$meta_file" \
      | while IFS= read -r pdf_url; do
          fname="${pdf_url##*/}"
          fetch "$pdf_url" "${CL_OUT}/${fname}"
          ((total++)) || true
        done
      next=$(jq -r '.next // empty' "$meta_file")
      [[ -z "$next" ]] && break
      ((page++))
    else
      log "DRY: would fetch page ${page} of RECAP docs for docket ${CL_DOCKET_ID}"
      break
    fi
  done
  log "  Downloaded ${total} CourtListener RECAP docs"
  append_manifest "${CL_OUT}"
fi

# ── manifest dedup + summary ─────────────────────────────────────────────────
if [[ $DRY_RUN -eq 0 ]]; then
  sort -u "${OUT_DIR}/manifest.sha256" -o "${OUT_DIR}/manifest.sha256"
  total_files=$(wc -l < "${OUT_DIR}/manifest.sha256")
  total_mb=$(du -sm "${OUT_DIR}" 2>/dev/null | cut -f1 || echo "?")
  log "=== Acquisition complete ==="
  log "  Files hashed : ${total_files}"
  log "  Corpus size  : ${total_mb} MB"
  log "  Manifest     : ${OUT_DIR}/manifest.sha256"
else
  log "=== DRY RUN complete — no files downloaded ==="
fi

log "Done."
