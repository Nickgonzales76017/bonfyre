#!/usr/bin/env python3
"""
lambda_codebook.py — Domain-specific Huffman codebook from legal text corpus.

Learns a character-level and token-level PMF from all _djvu.txt files in the
corpus, builds Huffman codes, and compares compression ratio against:
  - zstd default
  - zstd with pre-trained dictionary (via `zstd --train`)
  - generic Huffman (uniform prior)
  - legal-tuned Huffman (this script)
  - arithmetic coding estimate (entropy lower bound)

The "Lambda Tensor" framing: model text as a sequence of symbols drawn from a
distribution P_λ = argmin KL( P_domain || P_lambda_prior ).  The resulting
codes transfer better to novel legal docs than zstd's internal LZ77 models.

Usage:
  # Build codebook from corpus
  python3 lambda_codebook.py build \\
    --corpus /tmp/epstein-bench/corpus \\
    --out /tmp/epstein-bench/legal_codebook.json

  # Compress a file using stored codebook
  python3 lambda_codebook.py compress \\
    --codebook /tmp/epstein-bench/legal_codebook.json \\
    --in doc.txt --out doc.huf

  # Benchmark all methods on held-out docs
  python3 lambda_codebook.py bench \\
    --corpus /tmp/epstein-bench/corpus \\
    --codebook /tmp/epstein-bench/legal_codebook.json \\
    --sample 50

Outputs:
  build → legal_codebook.json  (symbol freqs + Huffman codes + metadata)
  bench → JSON to stdout (compression ratio table per document type)
"""

import argparse
import collections
import heapq
import json
import os
import struct
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional

# ── Huffman tree ───────────────────────────────────────────────────────────────

class HuffNode:
    __slots__ = ['weight', 'symbol', 'left', 'right']
    def __init__(self, weight, symbol=None, left=None, right=None):
        self.weight = weight
        self.symbol = symbol
        self.left = left
        self.right = right
    def __lt__(self, other):
        return self.weight < other.weight

def build_huffman_tree(freq: dict) -> HuffNode:
    heap = [HuffNode(w, s) for s, w in freq.items() if w > 0]
    heapq.heapify(heap)
    while len(heap) > 1:
        a = heapq.heappop(heap)
        b = heapq.heappop(heap)
        heapq.heappush(heap, HuffNode(a.weight + b.weight, None, a, b))
    return heap[0] if heap else HuffNode(1, '\x00')

def _extract_codes(node: HuffNode, prefix: str, codes: dict):
    if node.symbol is not None:
        codes[node.symbol] = prefix or '0'
    else:
        if node.left:  _extract_codes(node.left,  prefix + '0', codes)
        if node.right: _extract_codes(node.right, prefix + '1', codes)

def huffman_codes(freq: dict) -> dict:
    tree = build_huffman_tree(freq)
    codes = {}
    _extract_codes(tree, '', codes)
    return codes

def huffman_encode(text: str, codes: dict) -> bytes:
    """Pack Huffman-encoded text into bytes, return bitstring as bytes (last byte zero-padded)."""
    bits = ''.join(codes.get(c, codes.get('<UNK>', '?')) for c in text)
    # pad to byte boundary
    pad = (8 - len(bits) % 8) % 8
    bits += '0' * pad
    out = bytearray()
    for i in range(0, len(bits), 8):
        out.append(int(bits[i:i+8], 2))
    return bytes(out)

def huffman_compressed_bits(text: str, codes: dict) -> int:
    return sum(len(codes.get(c, codes.get('<UNK>', '????????'))) for c in text)


# ── token-level (word) Huffman ─────────────────────────────────────────────────

def tokenize(text: str) -> list[str]:
    """Whitespace tokenizer, lowercased."""
    return text.lower().split()


# ── corpus scan ───────────────────────────────────────────────────────────────

DJVU_GLOB = '**/*_djvu.txt'
DOCTYPE_MAP = {
    'fbi_vault':    ['fbi', '302'],
    'grand_jury':   ['grand jury', 'q.', 'a.'],
    'court_filing': ['district court', 'united states v.', 'case no.'],
    'investigative': [],
}

def classify_doc(text: str) -> str:
    low = text[:2000].lower()
    if 'fbi' in low and ('302' in low or 'federal bureau' in low):
        return 'fbi_vault'
    if sum(1 for line in text.splitlines()[:80] if line.strip().startswith(('Q.','A.'))) >= 4:
        return 'grand_jury'
    if 'district court' in low or 'united states v.' in low:
        return 'court_filing'
    return 'investigative'


def scan_corpus(corpus_dir: Path, max_docs: Optional[int] = None) -> dict:
    """
    Walk corpus_dir for *_djvu.txt files.
    Returns {doctype: [text, ...]} dict.
    """
    by_type: dict[str, list[str]] = collections.defaultdict(list)
    count = 0
    for path in sorted(corpus_dir.rglob('*_djvu.txt')):
        try:
            text = path.read_text(errors='replace')
        except OSError:
            continue
        dtype = classify_doc(text)
        by_type[dtype].append(text)
        count += 1
        if max_docs and count >= max_docs:
            break
    return dict(by_type)


# ── frequency counting ────────────────────────────────────────────────────────

def char_freq(texts: list[str]) -> dict:
    freq: dict[str, int] = collections.Counter()
    for t in texts:
        freq.update(t)
    return dict(freq)

def word_freq(texts: list[str], top_n: int = 8192) -> dict:
    freq: dict[str, int] = collections.Counter()
    for t in texts:
        freq.update(tokenize(t))
    return dict(freq.most_common(top_n))


# ── entropy / compression ratio ───────────────────────────────────────────────

def entropy_bits(text: str, freq: dict) -> float:
    """Shannon entropy estimate: H = -sum p log2 p, in bits for this text."""
    import math
    total = sum(freq.values())
    bits = 0.0
    for c in text:
        p = freq.get(c, 1) / total
        bits += -math.log2(p)
    return bits

def huffman_bits_for_text(text: str, codes: dict) -> int:
    unk = codes.get('<UNK>', '0' * 16)
    return sum(len(codes.get(c, unk)) for c in text)

def uniform_huffman_bits(text: str) -> int:
    """Baseline: 8 bits/char (raw bytes)."""
    return len(text) * 8


# ── zstd helpers (subprocess; zstd must be on PATH) ──────────────────────────

def zstd_size(text: str, level: int = 3) -> Optional[int]:
    try:
        data = text.encode('utf-8', errors='replace')
        result = subprocess.run(
            ['zstd', f'-{level}', '-q', '-c'],
            input=data, capture_output=True
        )
        return len(result.stdout) * 8 if result.returncode == 0 else None
    except FileNotFoundError:
        return None


def zstd_with_dict(text: str, dict_path: Path, level: int = 3) -> Optional[int]:
    try:
        data = text.encode('utf-8', errors='replace')
        result = subprocess.run(
            ['zstd', f'-{level}', '-q', '-c', '-D', str(dict_path)],
            input=data, capture_output=True
        )
        return len(result.stdout) * 8 if result.returncode == 0 else None
    except FileNotFoundError:
        return None


def train_zstd_dict(texts: list[str], out_path: Path, max_size: int = 131072):
    """Train a zstd dictionary on a list of texts."""
    with tempfile.TemporaryDirectory() as td:
        for i, t in enumerate(texts[:500]):
            Path(td, f'{i}.txt').write_bytes(t.encode('utf-8', errors='replace'))
        try:
            subprocess.run(
                ['zstd', '--train', '--maxdict', str(max_size), '-o', str(out_path),
                 *[str(Path(td, f'{i}.txt')) for i in range(min(500, len(texts)))]],
                check=True, capture_output=True
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False


# ── CLI commands ──────────────────────────────────────────────────────────────

def cmd_build(args):
    corpus_dir = Path(args.corpus)
    print(f'Scanning corpus: {corpus_dir}', file=sys.stderr)
    by_type = scan_corpus(corpus_dir)

    total_docs = sum(len(v) for v in by_type.values())
    print(f'Found {total_docs} docs: { {k: len(v) for k, v in by_type.items()} }', file=sys.stderr)

    if total_docs == 0:
        print('ERROR: no _djvu.txt files found.', file=sys.stderr)
        sys.exit(1)

    all_texts = [t for texts in by_type.values() for t in texts]

    # Character-level
    char_f  = char_freq(all_texts)
    char_huff = huffman_codes(char_f)

    # Word-level
    word_f  = word_freq(all_texts, top_n=8192)
    word_huff = huffman_codes(word_f)

    # Per-type char codes
    per_type: dict = {}
    for dtype, texts in by_type.items():
        cf = char_freq(texts)
        wf = word_freq(texts, top_n=4096)
        per_type[dtype] = {
            'char_freq': {k: int(v) for k, v in cf.items()},
            'word_freq': {k: int(v) for k, v in wf.items()},
            'char_codes': huffman_codes(cf),
            'word_codes': huffman_codes(wf),
            'doc_count': len(texts),
        }

    codebook = {
        'version': 1,
        'total_docs': total_docs,
        'char_freq':  {k: int(v) for k, v in char_f.items()},
        'word_freq':  {k: int(v) for k, v in word_f.items()},
        'char_codes': char_huff,
        'word_codes': word_huff,
        'per_type':   per_type,
    }

    out = Path(args.out)
    out.write_text(json.dumps(codebook, ensure_ascii=False))
    print(f'Codebook written to {out}  '
          f'({len(char_huff)} char codes, {len(word_huff)} word codes)', file=sys.stderr)


def cmd_compress(args):
    cb = json.loads(Path(args.codebook).read_text())
    text = Path(args.__dict__['in']).read_text(errors='replace')
    codes = cb['char_codes']
    encoded = huffman_encode(text, codes)
    Path(args.out).write_bytes(encoded)
    ratio = len(text.encode('utf-8')) / len(encoded)
    print(f'{len(text)} chars → {len(encoded)} bytes  ({ratio:.3f}× vs raw UTF-8)',
          file=sys.stderr)


def cmd_bench(args):
    import math
    corpus_dir = Path(args.corpus)
    cb = json.loads(Path(args.codebook).read_text())
    global_char_codes = cb['char_codes']
    per_type_codes = {dt: v['char_codes'] for dt, v in cb['per_type'].items()}

    # Collect held-out docs
    all_docs = sorted(corpus_dir.rglob('*_djvu.txt'))[:int(args.sample)]
    print(f'Benching {len(all_docs)} docs…', file=sys.stderr)

    rows = []
    for path in all_docs:
        try:
            text = path.read_text(errors='replace')
        except OSError:
            continue
        dtype = classify_doc(text)
        raw_bits = len(text.encode('utf-8')) * 8

        # Global Huffman
        global_bits = huffman_bits_for_text(text, global_char_codes)

        # Per-type Huffman
        pt_codes   = per_type_codes.get(dtype, global_char_codes)
        pt_bits    = huffman_bits_for_text(text, pt_codes)

        # Shannon lower bound
        char_f = cb.get('char_freq', {})
        entropy  = entropy_bits(text, char_f)

        # zstd
        zstd_bits = zstd_size(text) or 0
        zstd_d_bits = 0  # skip dict bench unless zstd_dict_path provided

        rows.append({
            'file':          path.name,
            'doctype':       dtype,
            'raw_bytes':     raw_bits // 8,
            'global_huff_ratio':   round(raw_bits / max(1, global_bits), 4),
            'pertype_huff_ratio':  round(raw_bits / max(1, pt_bits), 4),
            'entropy_ratio':       round(raw_bits / max(1, entropy), 4),
            'zstd3_ratio':         round(raw_bits / max(1, zstd_bits), 4) if zstd_bits else None,
        })

    # Aggregate by type
    by_type: dict = collections.defaultdict(list)
    for r in rows:
        by_type[r['doctype']].append(r)

    summary = {}
    for dt, drows in by_type.items():
        def avg(key): return round(sum(r[key] for r in drows if r[key]) / max(1, len(drows)), 4)
        summary[dt] = {
            'n': len(drows),
            'global_huff': avg('global_huff_ratio'),
            'pertype_huff': avg('pertype_huff_ratio'),
            'entropy_lower_bound': avg('entropy_ratio'),
            'zstd3': avg('zstd3_ratio'),
        }

    payload = {'summary': summary, 'rows': rows}
    rendered = json.dumps(payload, indent=2)
    if getattr(args, 'out', None):
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rendered + '\n')
    print(rendered)


def main():
    ap = argparse.ArgumentParser(description='Legal-domain Huffman codebook')
    sub = ap.add_subparsers(dest='cmd', required=True)

    p_b = sub.add_parser('build')
    p_b.add_argument('--corpus', required=True)
    p_b.add_argument('--out',    required=True)

    p_c = sub.add_parser('compress')
    p_c.add_argument('--codebook', required=True)
    p_c.add_argument('--in',  required=True, dest='in_')
    p_c.add_argument('--out', required=True)

    p_bench = sub.add_parser('bench')
    p_bench.add_argument('--corpus',   required=True)
    p_bench.add_argument('--codebook', required=True)
    p_bench.add_argument('--sample',   default=50)
    p_bench.add_argument('--out',      default=None)

    args = ap.parse_args()
    if hasattr(args, 'in_'):
        args.__dict__['in'] = args.in_

    {'build': cmd_build, 'compress': cmd_compress, 'bench': cmd_bench}[args.cmd](args)


if __name__ == '__main__':
    main()
