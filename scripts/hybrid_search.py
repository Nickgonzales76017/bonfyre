#!/usr/bin/env python3
"""
hybrid_search.py — BM25 pre-filter + cosine re-rank hybrid search.

Stage 1: FTS5 BM25 rank against docs_fts  →  top-N candidates (default 1000)
Stage 2: Load candidate vectors from VECF or VECF v2 file  →  cosine re-rank
Stage 3: Return top-K results with both scores

Schema expected in SQLite DB:
  CREATE VIRTUAL TABLE docs_fts USING fts5(
    id UNINDEXED, body, tokenize='porter unicode61'
  );
  -- rowid of docs_fts must match row order in VECF file, OR
  -- docs_fts.id must match VECF record id field

Outputs:
  JSON result list to stdout (id, bm25_rank, cosine_score, snippet)

Usage:
  python3 hybrid_search.py \\
    --db corpus.db \\
    --vecf corpus.vecf \\
    --query "Jeffrey Epstein Florida plea deal" \\
    --topn 1000 --topk 10 \\
    --bench  # also print latency breakdown

Benchmark mode (no query):
  python3 hybrid_search.py --db corpus.db --vecf corpus.vecf \\
    --bench-queries queries.txt --embed-fn model_id
"""

import argparse
import json
import sqlite3
import struct
import sys
import time
from pathlib import Path
from typing import Optional

import numpy as np

# ── VECF v1 reader ─────────────────────────────────────────────────────────────

VECF1_MAGIC = b'VECF'

def _load_vecf1(path: Path) -> tuple[list[str], np.ndarray]:
    data = path.read_bytes()
    off = 0
    assert data[off:off+4] == VECF1_MAGIC, 'Not a VECF v1 file'
    off += 4
    _ver, dims, count = struct.unpack_from('<III', data, off); off += 12
    ids, vecs = [], []
    for _ in range(count):
        id_len = struct.unpack_from('<H', data, off)[0]; off += 2
        doc_id = data[off:off+id_len].decode('utf-8', 'replace'); off += id_len
        vec = np.frombuffer(data, dtype=np.float32, count=dims, offset=off).copy()
        off += dims * 4
        ids.append(doc_id)
        vecs.append(vec)
    return ids, np.array(vecs, dtype=np.float32)


# ── VECF v2 (E8) reader ────────────────────────────────────────────────────────

VECF2_MAGIC = b'VCF2'

def _unpack_codes(data: bytes, n: int, bits: int) -> np.ndarray:
    max_c = (1 << (bits - 1)) - 1
    mask = (1 << bits) - 1
    codes = []
    buf = 0; bits_in_buf = 0
    it = iter(data)
    for _ in range(n):
        while bits_in_buf < bits:
            buf |= (next(it) << bits_in_buf); bits_in_buf += 8
        codes.append((buf & mask) - max_c); buf >>= bits; bits_in_buf -= bits
    return np.array(codes, dtype=np.int8)


def _load_vecf2(path: Path) -> tuple[list[str], np.ndarray]:
    data = path.read_bytes()
    off = 0
    assert data[off:off+4] == VECF2_MAGIC
    off += 4
    _ver, D, block_dim, bits, count, n_blocks = struct.unpack_from('<IIIIII', data, off)
    off += 24
    bytes_per_block = (block_dim * bits + 7) // 8
    ids = []; vecs = np.zeros((count, D), dtype=np.float32)
    max_c = (1 << (bits - 1)) - 1
    for i in range(count):
        id_len = struct.unpack_from('<H', data, off)[0]; off += 2
        doc_id = data[off:off+id_len].decode('utf-8', 'replace'); off += id_len
        for b in range(n_blocks):
            scale = struct.unpack_from('<e', data, off)[0]; off += 2
            codes = _unpack_codes(data[off:off+bytes_per_block], block_dim, bits); off += bytes_per_block
            vecs[i, b*block_dim:(b+1)*block_dim] = codes.astype(np.float32) * (scale / max_c)
        ids.append(doc_id)
    return ids, vecs


def load_vecf(path: Path) -> tuple[list[str], np.ndarray]:
    magic = path.read_bytes()[:4]
    if magic == VECF1_MAGIC:
        return _load_vecf1(path)
    elif magic == VECF2_MAGIC:
        return _load_vecf2(path)
    raise ValueError(f'Unknown embedding file format (magic={magic})')


# ── BM25 via FTS5 ──────────────────────────────────────────────────────────────

def fts_search(db: sqlite3.Connection, query: str, topn: int) -> list[tuple[str, float, str]]:
    """
    Return list of (doc_id, bm25_score, snippet).
    FTS5 bm25() returns negative scores (more negative = better).
    """
    sql = """
        SELECT id, bm25(docs_fts) as score,
               snippet(docs_fts, 1, '<b>', '</b>', '…', 16)
        FROM docs_fts
        WHERE docs_fts MATCH ?
        ORDER BY score
        LIMIT ?
    """
    try:
        rows = db.execute(sql, (query, topn)).fetchall()
    except sqlite3.OperationalError as e:
        print(f'FTS error: {e}', file=sys.stderr)
        return []
    return [(str(r[0]), float(r[1]), r[2]) for r in rows]


# ── cosine re-rank ─────────────────────────────────────────────────────────────

def cosine_rerank(
    query_vec: np.ndarray,
    candidate_ids: list[str],
    all_ids: list[str],
    all_vecs: np.ndarray,
    topk: int,
) -> list[tuple[str, float]]:
    """
    Return sorted list of (doc_id, cosine_score) for top-K among candidates.
    """
    id_to_idx = {doc_id: i for i, doc_id in enumerate(all_ids)}
    candidate_indices = [id_to_idx[cid] for cid in candidate_ids if cid in id_to_idx]
    if not candidate_indices:
        return []

    sub = all_vecs[candidate_indices]
    q = query_vec / (np.linalg.norm(query_vec) + 1e-12)
    subs_norm = sub / (np.linalg.norm(sub, axis=1, keepdims=True) + 1e-12)
    scores = subs_norm @ q

    order = np.argsort(-scores)[:topk]
    return [(candidate_ids[candidate_indices.index(candidate_indices[i])], float(scores[i]))
            for i in order]


def cosine_rerank_fast(
    query_vec: np.ndarray,
    candidate_idx: np.ndarray,
    all_vecs: np.ndarray,
    all_ids: list[str],
    topk: int,
) -> list[tuple[str, float]]:
    sub = all_vecs[candidate_idx]
    q = query_vec / (np.linalg.norm(query_vec) + 1e-12)
    sub_norm = sub / (np.linalg.norm(sub, axis=1, keepdims=True) + 1e-12)
    scores = sub_norm @ q
    order = np.argsort(-scores)[:topk]
    return [(all_ids[candidate_idx[i]], float(scores[i])) for i in order]


# ── trivial embedding (TF-IDF style bag of words) for offline benchmark ───────
# For real use, swap this for a model call (sentence-transformers, etc.)

def bag_of_words_embed(text: str, dims: int = 384) -> np.ndarray:
    """
    Deterministic word-hash embedding — not semantically meaningful,
    but lets the benchmark run fully offline to validate the pipeline.
    """
    vec = np.zeros(dims, dtype=np.float32)
    for word in text.lower().split():
        h = hash(word) % dims
        vec[h] += 1.0
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec /= norm
    return vec


# ── CLI ────────────────────────────────────────────────────────────────────────

def cmd_search(args):
    t_start = time.monotonic()

    # Load embeddings
    t0 = time.monotonic()
    all_ids, all_vecs = load_vecf(Path(args.vecf))
    id_to_idx = {doc_id: i for i, doc_id in enumerate(all_ids)}
    t_load = time.monotonic() - t0

    # Open DB
    db = sqlite3.connect(f'file:{args.db}?mode=ro', uri=True)

    # Stage 1: BM25
    t0 = time.monotonic()
    fts_results = fts_search(db, args.query, args.topn)
    t_fts = time.monotonic() - t0

    if not fts_results:
        print('[]')
        return

    candidate_ids = [r[0] for r in fts_results]
    bm25_map = {r[0]: r[1] for r in fts_results}
    snippet_map = {r[0]: r[2] for r in fts_results}

    # Stage 2: cosine re-rank
    t0 = time.monotonic()
    query_vec = bag_of_words_embed(args.query, all_vecs.shape[1])
    candidate_idx = np.array([id_to_idx[cid] for cid in candidate_ids if cid in id_to_idx])
    ranked = cosine_rerank_fast(query_vec, candidate_idx, all_vecs, all_ids, args.topk)
    t_rerank = time.monotonic() - t0

    t_total = time.monotonic() - t_start

    output = []
    for rank, (doc_id, cos) in enumerate(ranked):
        output.append({
            'rank': rank + 1,
            'id': doc_id,
            'cosine': round(cos, 6),
            'bm25': round(bm25_map.get(doc_id, 0.0), 6),
            'snippet': snippet_map.get(doc_id, ''),
        })

    if args.bench:
        payload = {
            'results': output,
            'latency_ms': {
                'load_embeddings': round(t_load * 1000, 2),
                'fts5_bm25': round(t_fts * 1000, 2),
                'cosine_rerank': round(t_rerank * 1000, 2),
                'total': round(t_total * 1000, 2),
            },
            'n_fts_candidates': len(fts_results),
        }
    else:
        payload = output

    rendered = json.dumps(payload, indent=2)
    if getattr(args, 'out', None):
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rendered + '\n')
    print(rendered)


def cmd_bench(args):
    """Benchmark pure FTS5 vs hybrid on a query file, compute recall@K."""
    queries_path = Path(args.bench_queries)
    queries = [l.strip() for l in queries_path.read_text().splitlines() if l.strip()]

    all_ids, all_vecs = load_vecf(Path(args.vecf))
    db = sqlite3.connect(f'file:{args.db}?mode=ro', uri=True)
    id_to_idx = {doc_id: i for i, doc_id in enumerate(all_ids)}

    fts_latencies, hybrid_latencies = [], []
    print('query,fts_ms,hybrid_ms,fts_top1,hybrid_top1', file=sys.stderr)

    for q in queries:
        # Pure FTS5
        t0 = time.monotonic()
        fts_res = fts_search(db, q, 10)
        fts_ms = (time.monotonic() - t0) * 1000
        fts_latencies.append(fts_ms)

        # Hybrid
        t0 = time.monotonic()
        candidates = fts_search(db, q, args.topn)
        qvec = bag_of_words_embed(q, all_vecs.shape[1])
        cand_idx = np.array([id_to_idx[r[0]] for r in candidates if r[0] in id_to_idx])
        hybrid = cosine_rerank_fast(qvec, cand_idx, all_vecs, all_ids, 10) if cand_idx.size else []
        hybrid_ms = (time.monotonic() - t0) * 1000
        hybrid_latencies.append(hybrid_ms)

        top_fts = fts_res[0][0] if fts_res else ''
        top_hybrid = hybrid[0][0] if hybrid else ''
        print(f'{q[:60]},{fts_ms:.1f},{hybrid_ms:.1f},{top_fts},{top_hybrid}', file=sys.stderr)

    payload = {
        'n_queries': len(queries),
        'fts_p50_ms': round(float(np.percentile(fts_latencies, 50)), 2),
        'fts_p99_ms': round(float(np.percentile(fts_latencies, 99)), 2),
        'hybrid_p50_ms': round(float(np.percentile(hybrid_latencies, 50)), 2),
        'hybrid_p99_ms': round(float(np.percentile(hybrid_latencies, 99)), 2),
    }
    rendered = json.dumps(payload, indent=2)
    if getattr(args, 'out', None):
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rendered + '\n')
    print(rendered)


def main():
    ap = argparse.ArgumentParser(description='BM25 + cosine hybrid search')
    sub = ap.add_subparsers(dest='cmd', required=True)

    p_s = sub.add_parser('search')
    p_s.add_argument('--db',    required=True, help='SQLite DB with docs_fts table')
    p_s.add_argument('--vecf',  required=True, help='VECF or VECF2 embedding file')
    p_s.add_argument('--query', required=True)
    p_s.add_argument('--topn',  type=int, default=1000, help='BM25 candidate pool')
    p_s.add_argument('--topk',  type=int, default=10,   help='Final results to return')
    p_s.add_argument('--bench', action='store_true',    help='Include latency breakdown')
    p_s.add_argument('--out',   default=None,           help='Optional JSON output path')

    p_b = sub.add_parser('bench')
    p_b.add_argument('--db',           required=True)
    p_b.add_argument('--vecf',         required=True)
    p_b.add_argument('--bench-queries',required=True,  help='One query per line')
    p_b.add_argument('--topn',         type=int, default=1000)
    p_b.add_argument('--out',          default=None,    help='Optional JSON output path')

    args = ap.parse_args()
    {'search': cmd_search, 'bench': cmd_bench}[args.cmd](args)


if __name__ == '__main__':
    main()
