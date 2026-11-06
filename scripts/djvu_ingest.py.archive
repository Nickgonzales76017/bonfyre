#!/usr/bin/env python3
"""
djvu_ingest.py — Direct _djvu.txt ingest with document type classification.

Replaces the PDF→Whisper→transcript path for any corpus that already has
IA-generated OCR. Enters the bonfyre pipeline at paragraph stage, skipping
media-prep, transcribe, and transcript-clean entirely.

Document type classifier (regex heuristics → 4 classes):
  fbi_302     — "FEDERAL BUREAU OF INVESTIGATION" header
  grand_jury  — Q/A line pattern ("^Q\\." / "^A\\.")
  court_filing — "UNITED STATES DISTRICT COURT" / "IN THE CIRCUIT"
  investigative — everything else (narrative reports, exhibits)

Outputs:
  <out_dir>/<docid>.classified.txt  — cleaned text with type header
  <out_dir>/ingest_manifest.jsonl   — one JSON record per doc
  <out_dir>/ingest_stats.json       — aggregate type counts + quality metrics

Usage:
  python3 djvu_ingest.py --corpus /tmp/epstein-bench/corpus \\
                          --out /tmp/epstein-bench/ingested \\
                          [--min-words 20] [--max-docs 0]
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

# ── type classifier ─────────────────────────────────────────────────────────

_FBI_302      = re.compile(r'FEDERAL\s+BUREAU\s+OF\s+INVESTIGATION', re.I)
_GRAND_JURY   = re.compile(r'^(Q|A)\.\s', re.MULTILINE)
_COURT_FILING = re.compile(
    r'(UNITED\s+STATES\s+DISTRICT\s+COURT|IN\s+THE\s+CIRCUIT|'
    r'SOUTHERN\s+DISTRICT\s+OF|CASE\s+NO\.\s*\d)', re.I)
_EXEMPT_TAG   = re.compile(r'\b(EXEMPT|b\(\d\)|(?:\[|\()Redacted(?:\]|\)))\b', re.I)
_DATE_PATTERN = re.compile(
    r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|'
    r'(?:January|February|March|April|May|June|July|August|September|'
    r'October|November|December)\s+\d{1,2},?\s+\d{4})\b', re.I)
_PERSON_CAPS  = re.compile(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b')


def classify(text: str) -> str:
    if _FBI_302.search(text):
        return 'fbi_302'
    qa_hits = len(_GRAND_JURY.findall(text))
    if qa_hits >= 4:
        return 'grand_jury'
    if _COURT_FILING.search(text):
        return 'court_filing'
    return 'investigative'


# ── text cleaners ────────────────────────────────────────────────────────────

_NOISE_LINE = re.compile(
    r'^[\s\W]{0,3}$|'           # blank / punctuation-only line
    r'^\s*\d+\s*$|'             # standalone page numbers
    r'^[=\-_~]{3,}$'            # horizontal rules
)


def clean_djvu(raw: str) -> str:
    """Remove djvu OCR noise preserving meaningful lines."""
    lines = []
    for line in raw.splitlines():
        stripped = line.strip()
        if _NOISE_LINE.match(stripped):
            continue
        lines.append(stripped)
    return '\n'.join(lines)


# ── quality metrics ─────────────────────────────────────────────────────────

def quality_metrics(text: str) -> dict:
    words = text.split()
    n_words = len(words)
    n_dates = len(_DATE_PATTERN.findall(text))
    n_persons = len(set(_PERSON_CAPS.findall(text)))
    n_exempt = len(_EXEMPT_TAG.findall(text))
    avg_word_len = (sum(len(w) for w in words) / n_words) if n_words else 0
    # OCR quality heuristic: short avg word length = broken OCR
    ocr_quality = min(1.0, max(0.0, (avg_word_len - 2.0) / 4.0))
    return {
        'word_count': n_words,
        'date_count': n_dates,
        'person_count': n_persons,
        'exempt_count': n_exempt,
        'avg_word_len': round(avg_word_len, 2),
        'ocr_quality': round(ocr_quality, 3),
    }


# ── main ingest loop ─────────────────────────────────────────────────────────

def find_djvu_files(corpus_dir: Path) -> list[Path]:
    return sorted(corpus_dir.rglob('*_djvu.txt'))


def ingest(corpus_dir: Path, out_dir: Path, min_words: int, max_docs: int):
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / 'ingest_manifest.jsonl'
    stats_path    = out_dir / 'ingest_stats.json'

    files = find_djvu_files(corpus_dir)
    if max_docs > 0:
        files = files[:max_docs]

    type_counts: dict[str, int] = {'fbi_302': 0, 'grand_jury': 0,
                                    'court_filing': 0, 'investigative': 0}
    skipped = 0
    processed = 0
    t0 = time.monotonic()

    with manifest_path.open('w') as mf:
        for i, src in enumerate(files, 1):
            raw = src.read_text(encoding='utf-8', errors='replace')
            text = clean_djvu(raw)
            words = text.split()

            if len(words) < min_words:
                skipped += 1
                continue

            doc_type = classify(text)
            qm = quality_metrics(text)
            doc_id = src.stem.replace('_djvu', '')

            # Write classified text — pipeline enters at paragraph stage
            out_path = out_dir / f'{doc_id}.classified.txt'
            header = (
                f'# DOC_ID: {doc_id}\n'
                f'# DOC_TYPE: {doc_type}\n'
                f'# OCR_QUALITY: {qm["ocr_quality"]}\n'
                f'# WORD_COUNT: {qm["word_count"]}\n'
                f'# PERSONS: {qm["person_count"]}\n'
                f'# DATES: {qm["date_count"]}\n'
                f'# EXEMPT_SPANS: {qm["exempt_count"]}\n'
                f'---\n'
            )
            out_path.write_text(header + text, encoding='utf-8')

            record = {
                'doc_id': doc_id,
                'doc_type': doc_type,
                'source': str(src),
                'output': str(out_path),
                **qm,
            }
            mf.write(json.dumps(record) + '\n')
            type_counts[doc_type] += 1
            processed += 1

            if i % 200 == 0 or i == 1:
                elapsed = time.monotonic() - t0
                rate = processed / elapsed if elapsed > 0 else 0
                print(f'  {i}/{len(files)}: {doc_id}  type={doc_type}  '
                      f'words={qm["word_count"]}  rate={rate:.1f} docs/s',
                      file=sys.stderr)

    elapsed = time.monotonic() - t0
    stats = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'corpus_dir': str(corpus_dir),
        'total_files_found': len(files),
        'processed': processed,
        'skipped_low_quality': skipped,
        'type_counts': type_counts,
        'elapsed_sec': round(elapsed, 2),
        'docs_per_sec': round(processed / elapsed, 2) if elapsed > 0 else 0,
        'min_words_threshold': min_words,
    }
    stats_path.write_text(json.dumps(stats, indent=2))

    print(f'\nIngest complete: {processed} docs in {elapsed:.1f}s '
          f'({stats["docs_per_sec"]} docs/s)', file=sys.stderr)
    print(f'Types: {type_counts}', file=sys.stderr)
    print(f'Skipped (low OCR): {skipped}', file=sys.stderr)
    print(f'Manifest: {manifest_path}', file=sys.stderr)
    print(f'Stats:    {stats_path}', file=sys.stderr)

    return stats


def main():
    ap = argparse.ArgumentParser(description='Direct djvu.txt ingest + type classifier')
    ap.add_argument('--corpus', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--min-words', type=int, default=20,
                    help='Skip docs with fewer than N words after cleaning')
    ap.add_argument('--max-docs', type=int, default=0,
                    help='Cap docs processed (0=all)')
    args = ap.parse_args()

    ingest(Path(args.corpus), Path(args.out),
           args.min_words, args.max_docs)


if __name__ == '__main__':
    main()
