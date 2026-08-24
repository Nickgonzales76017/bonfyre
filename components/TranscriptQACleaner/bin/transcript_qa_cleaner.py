#!/usr/bin/env python3
"""Local transcript QA and cleanup CLI."""

import argparse
import json
import re
import sys
from pathlib import Path

FILLER_RE = re.compile(r"\b(?:um|uh|erm|ah|hmm|hm|mm|mhm|uh-huh)\b", re.IGNORECASE)
HALLUCINATION_PATTERNS = [
    re.compile(r"(?:Thank you(?:\s*\.)?(?:\s+|$)){3,}", re.IGNORECASE),
    re.compile(r"(?:Thanks for watching(?:\s*\.)?(?:\s+|$)){2,}", re.IGNORECASE),
    re.compile(r"(?:Please subscribe(?:\s*\.)?(?:\s+|$)){2,}", re.IGNORECASE),
    re.compile(r"(?:\.\.\.)+"),
    re.compile(r"(?:you\s*){5,}", re.IGNORECASE),
    re.compile(r"\b(\w{2,})\s+(?:\1\s+){2,}\1\b", re.IGNORECASE),
]
REPEATED_PHRASE_RE = re.compile(r"\b(.{10,80}?)\s*(?:\1\s*){2,}", re.IGNORECASE)
SPACE_RE = re.compile(r"\s+")
REPEATED_PUNCT_RE = re.compile(r"([,.!?]){2,}")
SPACE_BEFORE_PUNCT_RE = re.compile(r"\s+([,.!?])")
CHUNK_HEADER_RE = re.compile(r"^##\s+Chunk\s+\d+\s*", re.IGNORECASE | re.MULTILINE)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transcript", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--status-out", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def clean_text(text: str) -> dict:
    raw_text = text.strip()
    cleaned_text = CHUNK_HEADER_RE.sub("", raw_text)
    filler_tokens_removed = len(FILLER_RE.findall(cleaned_text))
    cleaned_text = FILLER_RE.sub("", cleaned_text)

    hallucinations_removed = 0
    for pattern in HALLUCINATION_PATTERNS:
        matches = pattern.findall(cleaned_text)
        hallucinations_removed += len(matches)
        cleaned_text = pattern.sub("", cleaned_text)

    repeated_phrases_removed = 0
    repeated_match = REPEATED_PHRASE_RE.search(cleaned_text)
    while repeated_match:
        repeated_phrases_removed += 1
        cleaned_text = REPEATED_PHRASE_RE.sub(r"\1", cleaned_text, count=1)
        repeated_match = REPEATED_PHRASE_RE.search(cleaned_text)

    cleaned_text = SPACE_RE.sub(" ", cleaned_text)
    cleaned_text = SPACE_BEFORE_PUNCT_RE.sub(r"\1", cleaned_text)
    cleaned_text = REPEATED_PUNCT_RE.sub(r"\1", cleaned_text)
    cleaned_text = cleaned_text.strip()
    if cleaned_text and cleaned_text[-1] not in ".!?":
        cleaned_text += "."

    return {
        "cleaned_text": cleaned_text,
        "changed": cleaned_text != raw_text,
        "filler_tokens_removed": filler_tokens_removed,
        "hallucinations_removed": hallucinations_removed,
        "repeated_phrases_removed": repeated_phrases_removed,
    }


def main() -> int:
    args = build_parser().parse_args()
    if not args.transcript.exists():
        print(f"Missing transcript file: {args.transcript}", file=sys.stderr)
        return 2

    if args.dry_run:
        print(f"Would clean transcript: {args.transcript} -> {args.out}")
        return 0

    payload = clean_text(args.transcript.read_text(encoding="utf-8"))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(str(payload["cleaned_text"]) + "\n", encoding="utf-8")

    status_path = args.status_out or args.out.parent / "status.json"
    status_payload = {
        "sourceSystem": "TranscriptQACleaner",
        "status": "completed",
        "transcriptPath": str(args.transcript),
        "cleanedPath": str(args.out),
        "changed": payload["changed"],
        "fillerTokensRemoved": payload["filler_tokens_removed"],
        "hallucinationsRemoved": payload["hallucinations_removed"],
        "repeatedPhrasesRemoved": payload["repeated_phrases_removed"],
    }
    status_path.write_text(json.dumps(status_payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote cleaned transcript to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
