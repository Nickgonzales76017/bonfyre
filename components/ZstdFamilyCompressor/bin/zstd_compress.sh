#!/usr/bin/env bash
set -euo pipefail
# ZstdFamilyCompressor — zstd compression with dictionary training for family-aware packing
# Usage: zstd_compress.sh <subcommand> <args> [--dry-run]
#   subcommands: train-dict | compress | decompress | pack-family

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DRY_RUN=false
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=true; done

subcmd="${1:-}"
shift || true

if [[ -z "$subcmd" ]]; then
  echo "Usage: zstd_compress.sh <train-dict|compress|decompress|pack-family> <args> [--dry-run]"
  exit 1
fi

case "$subcmd" in
  train-dict)
    # train-dict <samples-dir> [--dict-out path]
    samples="${1:-./samples}"
    dict_out="family.dict"
    prev=""
    for i in "$@"; do
      if [[ "$prev" == "--dict-out" ]]; then dict_out="$i"; fi
      prev="${i:-}"
    done
    cmd="zstd --train \"$samples\"/* -o \"$dict_out\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v zstd &>/dev/null; then echo "zstd not found. Install: brew install zstd"; exit 2; fi
    eval "$cmd"
    echo "Trained dictionary: $dict_out"
    ;;
  compress)
    # compress <input> [--dict path] [--out path] [--level 19]
    input="${1:-}"
    dict="" out="" level="19"
    prev=""
    for i in "$@"; do
      case "$prev" in --dict) dict="$i";; --out) out="$i";; --level) level="$i";; esac
      prev="$i"
    done
    [[ -z "$out" ]] && out="${input}.zst"
    if [[ -n "$dict" ]]; then
      cmd="zstd -\"$level\" -D \"$dict\" \"$input\" -o \"$out\""
    else
      cmd="zstd -\"$level\" \"$input\" -o \"$out\""
    fi
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v zstd &>/dev/null; then echo "zstd not found."; exit 2; fi
    eval "$cmd"
    echo "Compressed: $out"
    ;;
  decompress)
    input="${1:-}"
    dict="" out=""
    prev=""
    for i in "$@"; do
      case "$prev" in --dict) dict="$i";; --out) out="$i";; esac
      prev="$i"
    done
    [[ -z "$out" ]] && out="${input%.zst}"
    if [[ -n "$dict" ]]; then
      cmd="zstd -d -D \"$dict\" \"$input\" -o \"$out\""
    else
      cmd="zstd -d \"$input\" -o \"$out\""
    fi
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v zstd &>/dev/null; then echo "zstd not found."; exit 2; fi
    eval "$cmd"
    echo "Decompressed: $out"
    ;;
  pack-family)
    # pack-family <dir> [--dict path] [--out archive.tar.zst]
    dir="${1:-}"
    dict="" out="${dir}.tar.zst"
    prev=""
    for i in "$@"; do
      case "$prev" in --dict) dict="$i";; --out) out="$i";; esac
      prev="$i"
    done
    if [[ -n "$dict" ]]; then
      cmd="tar cf - \"$dir\" | zstd -19 -D \"$dict\" -o \"$out\""
    else
      cmd="tar cf - \"$dir\" | zstd -19 -o \"$out\""
    fi
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v zstd &>/dev/null; then echo "zstd not found."; exit 2; fi
    eval "$cmd"
    echo "Packed family: $out"
    ;;
  *)
    echo "Unknown subcommand: $subcmd"
    exit 1
    ;;
esac
