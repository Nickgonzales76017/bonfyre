#!/usr/bin/env python3
"""
embedding_compress.py — Λ-profiler driven dimensionality reduction.

For each embedding dimension, compute:
  - kurtosis (leptokurtic dims = high information density)
  - spectral gap (eigenvalue ratio in PCA subspace)
  - outlier fraction (|z| > 3σ)

Rank by composite score, select top-K dims, write:
  - <out>/dim_ranking.json   full per-dim profile
  - <out>/projection.npy     K×D float32 projection matrix (rows = selected dims)
  - <out>/bench.json         recall@10 before/after + cosine preservation

Works directly on VECF files (bonfyre-embed output) or on .jsonl
embedding records {id, embedding: [...]}.

Usage:
  python3 embedding_compress.py \\
      --embeddings /tmp/epstein-bench/embeddings.jsonl \\
      --out /tmp/epstein-bench/lambda_profile \\
      --target-dims 128 \\
      [--recall-queries 200]
"""

import argparse
import json
import struct
import sys
import time
from pathlib import Path

import numpy as np


# ── VECF v1 reader (bonfyre-embed output format) ─────────────────────────────
# Header: magic(4) version(4) dims(4) count(4)
# Records: id_len(2) id(bytes) vector(dims × float32)

VECF_MAGIC = b'VECF'

def load_vecf(path: Path) -> tuple[list[str], np.ndarray]:
    data = path.read_bytes()
    off = 0
    magic = data[off:off+4]; off += 4
    if magic != VECF_MAGIC:
        raise ValueError(f'Not a VECF file: {magic}')
    _version = struct.unpack_from('<I', data, off)[0]; off += 4
    dims     = struct.unpack_from('<I', data, off)[0]; off += 4
    count    = struct.unpack_from('<I', data, off)[0]; off += 4

    ids = []
    vecs = np.zeros((count, dims), dtype=np.float32)
    for i in range(count):
        id_len = struct.unpack_from('<H', data, off)[0]; off += 2
        doc_id = data[off:off+id_len].decode('utf-8', 'replace'); off += id_len
        vec = struct.unpack_from(f'<{dims}f', data, off); off += dims * 4
        ids.append(doc_id)
        vecs[i] = vec

    return ids, vecs


def load_jsonl(path: Path) -> tuple[list[str], np.ndarray]:
    ids, vecs = [], []
    with path.open() as f:
        for line in f:
            rec = json.loads(line)
            ids.append(rec['id'])
            vecs.append(rec['embedding'])
    return ids, np.array(vecs, dtype=np.float32)


def load_embeddings(path: Path) -> tuple[list[str], np.ndarray]:
    if path.suffix == '.jsonl':
        return load_jsonl(path)
    return load_vecf(path)


# ── Λ profiler ────────────────────────────────────────────────────────────────

def kurtosis_excess(x: np.ndarray) -> float:
    """Fisher excess kurtosis of a 1-D array."""
    n = len(x)
    if n < 4:
        return 0.0
    mu = np.mean(x)
    s  = np.std(x) + 1e-12
    return float(np.mean(((x - mu) / s) ** 4) - 3.0)


def spectral_gap(x: np.ndarray, n_components: int = 4) -> float:
    """
    Ratio of top eigenvalue to (n_components+1)-th eigenvalue in the
    local PCA of this dimension's correlation with its neighbours.
    Uses a rank-4 approximation for speed.
    """
    n = len(x)
    if n < 8:
        return 1.0
    # Embed dimension into a Toeplitz-like local window of width 4
    w = min(n_components, n // 2)
    X = np.stack([x[i:n-w+i] for i in range(w)], axis=1)  # (n-w, w)
    _, s, _ = np.linalg.svd(X - X.mean(0), full_matrices=False)
    if len(s) < 2 or s[1] < 1e-10:
        return float(s[0]) / 1e-10
    return float(s[0] / s[1])


def outlier_fraction(x: np.ndarray, z_thresh: float = 3.0) -> float:
    mu, sigma = np.mean(x), np.std(x) + 1e-12
    return float(np.mean(np.abs((x - mu) / sigma) > z_thresh))


def profile_dimensions(vecs: np.ndarray) -> list[dict]:
    """
    Profile each embedding dimension.
    Returns list of dicts sorted by composite score (descending).
    """
    D = vecs.shape[1]
    profiles = []
    for d in range(D):
        col = vecs[:, d]
        kurt  = kurtosis_excess(col)
        sgap  = spectral_gap(col)
        ofrac = outlier_fraction(col)
        # Composite: high kurtosis + high spectral gap = information dense
        # Penalise high outlier fraction (broken dimension)
        score = (max(kurt, 0.0) + sgap) * (1.0 - min(ofrac, 0.5) * 2.0)
        profiles.append({
            'dim': d,
            'kurtosis': round(kurt, 4),
            'spectral_gap': round(sgap, 4),
            'outlier_frac': round(ofrac, 4),
            'score': round(score, 4),
        })
    profiles.sort(key=lambda p: -p['score'])
    return profiles


# ── recall@K evaluation ───────────────────────────────────────────────────────

def cosine_sim_matrix(A: np.ndarray, B: np.ndarray) -> np.ndarray:
    """Normalise and dot. A: (n, d), B: (m, d) → (n, m)"""
    A = A / (np.linalg.norm(A, axis=1, keepdims=True) + 1e-12)
    B = B / (np.linalg.norm(B, axis=1, keepdims=True) + 1e-12)
    return A @ B.T


def recall_at_k(full_vecs: np.ndarray,
                compressed_vecs: np.ndarray,
                n_queries: int = 200,
                k: int = 10) -> dict:
    """
    Ground truth: top-k neighbours in full space.
    Measure: how many of those top-k appear in top-k of compressed space.
    """
    N = len(full_vecs)
    n_queries = min(n_queries, N)
    rng = np.random.default_rng(42)
    qidxs = rng.choice(N, n_queries, replace=False)

    hits = 0
    for qi in qidxs:
        q_full = full_vecs[qi:qi+1]
        q_comp = compressed_vecs[qi:qi+1]

        full_sims = cosine_sim_matrix(q_full, full_vecs)[0]
        full_sims[qi] = -1e9
        gt_top_k = set(np.argsort(full_sims)[-k:].tolist())

        comp_sims = cosine_sim_matrix(q_comp, compressed_vecs)[0]
        comp_sims[qi] = -1e9
        pred_top_k = set(np.argsort(comp_sims)[-k:].tolist())

        hits += len(gt_top_k & pred_top_k)

    return {
        'recall_at_k': round(hits / (n_queries * k), 4),
        'k': k,
        'n_queries': n_queries,
    }


def mean_cosine_preservation(full_vecs: np.ndarray,
                              compressed_vecs: np.ndarray,
                              n_pairs: int = 500) -> float:
    """Sample pairs, measure cosine agreement between spaces."""
    N = len(full_vecs)
    rng = np.random.default_rng(0)
    pairs = rng.choice(N, (n_pairs, 2), replace=True)
    full_n  = full_vecs / (np.linalg.norm(full_vecs, axis=1, keepdims=True) + 1e-12)
    comp_n  = compressed_vecs / (np.linalg.norm(compressed_vecs, axis=1, keepdims=True) + 1e-12)
    full_cos = np.sum(full_n[pairs[:,0]] * full_n[pairs[:,1]], axis=1)
    comp_cos = np.sum(comp_n[pairs[:,0]] * comp_n[pairs[:,1]], axis=1)
    return float(np.mean(np.abs(full_cos - comp_cos)))


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description='Λ-profiler: rank embedding dims by information density, '
                    'select top-K, measure recall preservation')
    ap.add_argument('--embeddings', required=True,
                    help='VECF file or .jsonl with {id, embedding: [...]} records')
    ap.add_argument('--out', required=True)
    ap.add_argument('--target-dims', type=int, default=128)
    ap.add_argument('--recall-queries', type=int, default=200)
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    print('Loading embeddings…', file=sys.stderr)
    t0 = time.monotonic()
    ids, vecs = load_embeddings(Path(args.embeddings))
    D = vecs.shape[1]
    print(f'  {len(ids)} vectors × {D} dims in {time.monotonic()-t0:.1f}s',
          file=sys.stderr)

    print('Profiling dimensions (Λ-style kurtosis + spectral gap)…',
          file=sys.stderr)
    t1 = time.monotonic()
    profiles = profile_dimensions(vecs)
    print(f'  Done in {time.monotonic()-t1:.1f}s', file=sys.stderr)

    # Save full ranking
    ranking_path = out / 'dim_ranking.json'
    ranking_path.write_text(json.dumps({
        'total_dims': D,
        'target_dims': args.target_dims,
        'top_10': profiles[:10],
        'bottom_10': profiles[-10:],
        'all': profiles,
    }, indent=2))

    # Select top-K dims, build projection
    top_dims = [p['dim'] for p in profiles[:args.target_dims]]
    proj = np.eye(D, dtype=np.float32)[top_dims]  # K×D selection matrix
    np.save(str(out / 'projection.npy'), proj)

    # Compress
    compressed = vecs @ proj.T  # (N, K)
    print(f'Compressed: {D}-dim → {args.target_dims}-dim  '
          f'({D/args.target_dims:.1f}× size reduction)',
          file=sys.stderr)

    # Recall benchmark
    print('Benchmarking recall@10…', file=sys.stderr)
    recall = recall_at_k(vecs, compressed, args.recall_queries)
    cos_err = mean_cosine_preservation(vecs, compressed)

    bench = {
        'original_dims': D,
        'compressed_dims': args.target_dims,
        'dim_reduction_ratio': round(D / args.target_dims, 2),
        'recall_at_10': recall['recall_at_k'],
        'mean_cosine_error': round(cos_err, 5),
        'n_vectors': len(ids),
        'top_dim_scores': [p['score'] for p in profiles[:args.target_dims]],
        'bottom_dim_scores': [p['score'] for p in profiles[-args.target_dims:]],
    }
    (out / 'bench.json').write_text(json.dumps(bench, indent=2))

    print('\n── Λ Profile Results ──────────────────────────────', file=sys.stderr)
    print(f'  {D} → {args.target_dims} dims  ({bench["dim_reduction_ratio"]}×)',
          file=sys.stderr)
    print(f'  recall@10:         {bench["recall_at_10"]}', file=sys.stderr)
    print(f'  mean cosine error: {bench["mean_cosine_error"]}', file=sys.stderr)
    print(f'  projection saved:  {out}/projection.npy', file=sys.stderr)


if __name__ == '__main__':
    main()
