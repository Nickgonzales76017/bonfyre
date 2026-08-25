#!/usr/bin/env python3
"""Build a simple MFA dictionary from transcript text.

This is a lightweight, dry-run friendly helper that produces a word->pronunciation
lexicon where pronunciations are naive grapheme spell-outs. Designed as a placeholder
for MFA dictionary generation; replace with a phoneme-based generator if needed.
"""
import argparse
from pathlib import Path
import json
import re
import sys


def build_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--transcript", type=Path, required=True,
                   help="Path to transcript text or JSON file")
    p.add_argument("--out", type=Path, required=True,
                   help="Output lexicon file for MFA")
    p.add_argument("--dry-run", action="store_true")
    return p


def tokenize_text(text: str):
    words = re.findall(r"[A-Za-z']+", text)
    return [w.upper() for w in words]


def make_pronunciation(word: str):
    # Naive grapheme-based pronunciation (split into letters)
    letters = list(word)
    return " ".join(letters)


def load_transcript(path: Path):
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() == ".json":
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        texts = []
        if isinstance(data, dict):
            # assume id->text or single object with text
            if "text" in data:
                texts.append(data["text"])
            else:
                for v in data.values():
                    if isinstance(v, str):
                        texts.append(v)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "text" in item:
                    texts.append(item["text"])
                elif isinstance(item, str):
                    texts.append(item)
        return "\n".join(texts)
    else:
        return path.read_text(encoding="utf-8")


def main():
    args = build_parser().parse_args()
    if args.dry_run:
        try:
            txt = load_transcript(args.transcript)
        except FileNotFoundError:
            print(f"Would read transcript from {args.transcript} (file missing)" )
            print(f"Would write lexicon to {args.out}")
            return 0
        words = tokenize_text(txt)
        uniq = sorted(set(words))
        print(f"Would generate {len(uniq)} lexicon entries to {args.out}")
        if len(uniq) > 0:
            sample = uniq[:10]
            print("Sample entries:")
            for w in sample:
                print(f"{w} -> {make_pronunciation(w)}")
        return 0

    txt = load_transcript(args.transcript)
    words = tokenize_text(txt)
    uniq = sorted(set(words))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        for w in uniq:
            pron = make_pronunciation(w)
            fh.write(f"{w} {pron}\n")
    print(f"Wrote {len(uniq)} entries to {args.out}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
