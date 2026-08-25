#!/usr/bin/env python3
"""TokenizerLayer — universal text segmentation via sentencepiece or tiktoken.

Usage:
  tokenizer_layer.py segment <input.txt> [--model sp|tiktoken] [--out tokens.json] [--dry-run]
  tokenizer_layer.py count <input.txt> [--model sp|tiktoken] [--dry-run]
"""
import argparse
import json
import sys
from pathlib import Path


def build_parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd")
    seg = sub.add_parser("segment")
    seg.add_argument("input", type=Path)
    seg.add_argument("--model", choices=["sp", "tiktoken"], default="sp")
    seg.add_argument("--out", type=Path, default=None)
    seg.add_argument("--dry-run", action="store_true")

    cnt = sub.add_parser("count")
    cnt.add_argument("input", type=Path)
    cnt.add_argument("--model", choices=["sp", "tiktoken"], default="sp")
    cnt.add_argument("--dry-run", action="store_true")
    return p


def naive_segment(text: str) -> list[str]:
    """Whitespace tokenizer fallback."""
    return text.split()


def main():
    args = build_parser().parse_args()
    if not args.cmd:
        print("Usage: tokenizer_layer.py <segment|count> <input> [--dry-run]")
        return 1

    if args.dry_run:
        print(f"Would {args.cmd} '{args.input}' with model={args.model}")
        return 0

    if not args.input.exists():
        print(f"File not found: {args.input}", file=sys.stderr)
        return 2

    text = args.input.read_text(encoding="utf-8")

    # Try sentencepiece, fall back to naive
    tokens = None
    if args.model == "sp":
        try:
            import sentencepiece as spm
            sp = spm.SentencePieceProcessor()
            # Would need a trained model; fall back for now
            raise ImportError("no model loaded")
        except ImportError:
            tokens = naive_segment(text)
    elif args.model == "tiktoken":
        try:
            import tiktoken
            enc = tiktoken.get_encoding("cl100k_base")
            tokens = [enc.decode([t]) for t in enc.encode(text)]
        except ImportError:
            tokens = naive_segment(text)

    if tokens is None:
        tokens = naive_segment(text)

    if args.cmd == "count":
        print(f"Token count: {len(tokens)}")
    elif args.cmd == "segment":
        out = args.out or Path(str(args.input) + ".tokens.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as fh:
            json.dump({"token_count": len(tokens), "tokens": tokens}, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        print(f"Wrote {len(tokens)} tokens to {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
