#!/usr/bin/env bash
# Proof metadata for a brief directory is whatever the proof organ measures when
# it checks the brief against its source. This script used to write a score of
# 85, the status "verified" and the words "standard QA passed" for any
# directory it was pointed at.
set -euo pipefail
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <brief_dir>   (holds brief.md and a transcript or brief-meta.json naming one)" >&2
  exit 2
fi
brief_dir="$1"
proof=$(command -v bonfyre-proof 2>/dev/null || echo "$HOME/.bonfyre/bin/bonfyre-proof")
[ -x "$proof" ] || { echo "bonfyre-proof is not installed; no proof metadata was written" >&2; exit 3; }
"$proof" score "$brief_dir" "$brief_dir"
