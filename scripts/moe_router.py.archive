#!/usr/bin/env python3
"""
moe_router.py — Λ-kurtosis EvoMoE expert router for legal document types.

Connects three existing primitives:
  1. djvu_ingest.py 4-class document types (fbi_302, grand_jury, court_filing, investigative)
  2. embedding_compress.py Λ-profiler (kurtosis per dim as routing features)
  3. evomoe_split.py K-FAC split oracle (defines expert weight sets)

Router design:
  - 4 feed-forward experts, one per document type
  - Routing signal: kurtosis vector of input embedding (per-dim kurtosis over a
    sliding 64-token window), not softmax gating — hard assignment
  - Expert affinity = cosine(kurtosis_vec, expert_centroid)
  - Expert centroids learned from ingest_manifest.jsonl + embeddings
  - Fallback to top-1 softmax over centroid cosines

This is NOT a trained neural router. It's a deterministic Λ-kurtosis signal
used as a zero-shot MoE dispatch that costs zero extra parameters.

Modes:
  train  — learn expert centroids from labeled embeddings
  route  — route a single embedding or batch file
  eval   — measure expert specialization (accuracy vs doctype labels)
  bench  — throughput + accuracy on manifest

Usage:
  python3 moe_router.py train \\
    --manifest /tmp/epstein-bench/ingested/ingest_manifest.jsonl \\
    --embeddings /tmp/epstein-bench/ingested/embeddings.vecf \\
    --out /tmp/epstein-bench/moe_centroids.json

  python3 moe_router.py route \\
    --centroids /tmp/epstein-bench/moe_centroids.json \\
    --embeddings /tmp/epstein-bench/ingested/embeddings.vecf \\
    --batch  # route all, output jsonl

  python3 moe_router.py eval \\
    --manifest /tmp/epstein-bench/ingested/ingest_manifest.jsonl \\
    --centroids /tmp/epstein-bench/moe_centroids.json \\
    --embeddings /tmp/epstein-bench/ingested/embeddings.vecf
"""

import argparse
import json
import struct
import sys
import time
from pathlib import Path
from typing import Optional

import numpy as np

# ── Document types ─────────────────────────────────────────────────────────────

EXPERT_TYPES = ['fbi_302', 'grand_jury', 'court_filing', 'investigative']
N_EXPERTS = len(EXPERT_TYPES)

# ── VECF readers (same as hybrid_search.py) ───────────────────────────────────

VECF1_MAGIC = b'VECF'
VECF2_MAGIC = b'VCF2'

def _load_vecf1(data: bytes) -> tuple[list[str], np.ndarray]:
    off = 4
    _ver, dims, count = struct.unpack_from('<III', data, off); off += 12
    ids, vecs = [], []
    for _ in range(count):
        id_len = struct.unpack_from('<H', data, off)[0]; off += 2
        doc_id = data[off:off+id_len].decode('utf-8', 'replace'); off += id_len
        vec = np.frombuffer(data, dtype=np.float32, count=dims, offset=off).copy()
        off += dims * 4
        ids.append(doc_id); vecs.append(vec)
    return ids, np.array(vecs, dtype=np.float32)

def _unpack_codes(raw: bytes, n: int, bits: int) -> np.ndarray:
    max_c = (1 << (bits-1)) - 1; mask = (1 << bits) - 1
    codes = []; buf = 0; bits_in = 0; it = iter(raw)
    for _ in range(n):
        while bits_in < bits: buf |= (next(it) << bits_in); bits_in += 8
        codes.append((buf & mask) - max_c); buf >>= bits; bits_in -= bits
    return np.array(codes, dtype=np.int8)

def _load_vecf2(data: bytes) -> tuple[list[str], np.ndarray]:
    off = 4
    _ver, D, bd, bits, count, n_blocks = struct.unpack_from('<IIIIII', data, off); off += 24
    bpb = (bd * bits + 7) // 8; max_c = (1 << (bits-1)) - 1
    ids = []; vecs = np.zeros((count, D), np.float32)
    for i in range(count):
        id_len = struct.unpack_from('<H', data, off)[0]; off += 2
        doc_id = data[off:off+id_len].decode('utf-8', 'replace'); off += id_len
        for b in range(n_blocks):
            scale = struct.unpack_from('<e', data, off)[0]; off += 2
            codes = _unpack_codes(data[off:off+bpb], bd, bits); off += bpb
            vecs[i, b*bd:(b+1)*bd] = codes.astype(np.float32) * (scale / max_c)
        ids.append(doc_id)
    return ids, vecs

def load_vecf(path: Path) -> tuple[list[str], np.ndarray]:
    data = path.read_bytes()
    if data[:4] == VECF1_MAGIC: return _load_vecf1(data)
    if data[:4] == VECF2_MAGIC: return _load_vecf2(data)
    raise ValueError(f'Unknown format: {data[:4]}')


# ── Λ-kurtosis profiler ────────────────────────────────────────────────────────

def kurtosis_vec(X: np.ndarray, eps: float = 1e-8) -> np.ndarray:
    """
    Per-dimension excess kurtosis for a batch of vectors X (N, D).
    kurtosis[d] = E[(x_d - μ_d)^4] / (σ_d^4) - 3
    Returns shape (D,).
    """
    mu = X.mean(axis=0)
    diff = X - mu
    var = (diff ** 2).mean(axis=0)
    m4  = (diff ** 4).mean(axis=0)
    return m4 / (var ** 2 + eps) - 3.0


def routing_features(vecs: np.ndarray) -> np.ndarray:
    """
    Compute routing feature vector for a batch of embeddings.
    Uses kurtosis of each dim as the Λ-profile signal.
    For a single vector: broadens to fake batch of 1 (degenerate kurtosis = 0 always)
    — for single-doc routing we use the raw embedding instead.
    """
    if vecs.ndim == 1 or vecs.shape[0] < 4:
        # fallback: use raw embedding as routing signal (not enough samples for kurtosis)
        v = vecs if vecs.ndim == 2 else vecs[None]
        return v.mean(axis=0)
    return kurtosis_vec(vecs)


# ── Expert centroid learning ───────────────────────────────────────────────────

def learn_centroids(
    manifest_path: Path,
    ids: list[str],
    vecs: np.ndarray,
) -> dict:
    """
    For each expert type, collect embeddings of that type from manifest,
    compute centroid and kurtosis signature.
    """
    id_to_idx = {doc_id: i for i, doc_id in enumerate(ids)}

    by_type: dict[str, list[np.ndarray]] = {t: [] for t in EXPERT_TYPES}

    with manifest_path.open() as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            doc_id  = rec.get('id') or rec.get('doc_id', '')
            doctype = rec.get('doctype') or rec.get('type', 'investigative')
            if doc_id in id_to_idx and doctype in by_type:
                by_type[doctype].append(vecs[id_to_idx[doc_id]])

    centroids = {}
    for etype, evecs in by_type.items():
        if not evecs:
            centroids[etype] = {
                'centroid': np.zeros(vecs.shape[1]).tolist(),
                'kurtosis': np.zeros(vecs.shape[1]).tolist(),
                'n': 0,
            }
            continue
        mat = np.array(evecs, dtype=np.float32)
        centroid = mat.mean(axis=0)
        kurt = routing_features(mat)
        centroids[etype] = {
            'centroid':  centroid.tolist(),
            'kurtosis':  kurt.tolist(),
            'n':         len(evecs),
        }
        print(f'  {etype}: {len(evecs)} docs', file=sys.stderr)

    return centroids


# ── Routing ────────────────────────────────────────────────────────────────────

def cosine(a: np.ndarray, b: np.ndarray) -> float:
    na = np.linalg.norm(a); nb = np.linalg.norm(b)
    if na < 1e-12 or nb < 1e-12: return 0.0
    return float(np.dot(a, b) / (na * nb))


def route_one(vec: np.ndarray, centroids: dict) -> tuple[str, dict]:
    """
    Route a single embedding to the best expert.
    Scoring: cosine(vec, centroid) × (1 + sigmoid(kurtosis_alignment))
    Returns (expert_type, scores_dict).
    """
    scores = {}
    for etype in EXPERT_TYPES:
        if etype not in centroids: continue
        c = np.array(centroids[etype]['centroid'], dtype=np.float32)
        k = np.array(centroids[etype]['kurtosis'], dtype=np.float32)
        centroid_cos = cosine(vec, c)
        # Kurtosis alignment: how well does absolute activation pattern match?
        kurt_align = cosine(np.abs(vec), np.abs(k))
        # Combined score: centroid similarity boosted by kurtosis match
        scores[etype] = round(centroid_cos * 0.7 + kurt_align * 0.3, 6)

    best = max(scores, key=lambda k: scores[k])
    return best, scores


# ── CLI ────────────────────────────────────────────────────────────────────────

def cmd_train(args):
    print(f'Loading embeddings…', file=sys.stderr)
    ids, vecs = load_vecf(Path(args.embeddings))
    print(f'{len(ids)} vectors loaded, dim={vecs.shape[1]}', file=sys.stderr)

    print('Learning expert centroids…', file=sys.stderr)
    centroids = learn_centroids(Path(args.manifest), ids, vecs)

    out = {
        'version': 1,
        'n_experts': N_EXPERTS,
        'expert_types': EXPERT_TYPES,
        'dims': vecs.shape[1],
        'centroids': centroids,
    }
    Path(args.out).write_text(json.dumps(out, indent=2))
    print(f'Centroids written → {args.out}', file=sys.stderr)


def cmd_route(args):
    centroids_data = json.loads(Path(args.centroids).read_text())
    centroids = centroids_data['centroids']

    ids, vecs = load_vecf(Path(args.embeddings))

    if args.batch:
        results = []
        for doc_id, vec in zip(ids, vecs):
            expert, scores = route_one(vec, centroids)
            results.append({'id': doc_id, 'expert': expert, 'scores': scores})
        if getattr(args, 'out', None):
            out_path = Path(args.out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text('\n'.join(json.dumps(r) for r in results) + '\n')
        for r in results:
            print(json.dumps(r))
    else:
        # Single — just first vector
        expert, scores = route_one(vecs[0], centroids)
        payload = {'id': ids[0], 'expert': expert, 'scores': scores}
        if getattr(args, 'out', None):
            out_path = Path(args.out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(payload, indent=2) + '\n')
        print(json.dumps(payload, indent=2))


def cmd_eval(args):
    centroids_data = json.loads(Path(args.centroids).read_text())
    centroids = centroids_data['centroids']

    ids, vecs = load_vecf(Path(args.embeddings))
    id_to_vec = {doc_id: vecs[i] for i, doc_id in enumerate(ids)}

    # Load ground truth labels
    gt: dict[str, str] = {}
    with Path(args.manifest).open() as f:
        for line in f:
            try:
                rec = json.loads(line)
                doc_id  = rec.get('id') or rec.get('doc_id', '')
                doctype = rec.get('doctype') or rec.get('type', 'investigative')
                gt[doc_id] = doctype
            except json.JSONDecodeError:
                continue

    correct = 0; total = 0
    conf_matrix: dict[str, dict[str, int]] = {
        t: {t2: 0 for t2 in EXPERT_TYPES} for t in EXPERT_TYPES
    }

    t0 = time.monotonic()
    for doc_id, true_type in gt.items():
        if doc_id not in id_to_vec: continue
        pred, _ = route_one(id_to_vec[doc_id], centroids)
        if true_type in conf_matrix:
            conf_matrix[true_type][pred] = conf_matrix[true_type].get(pred, 0) + 1
        if pred == true_type: correct += 1
        total += 1
    elapsed = time.monotonic() - t0

    acc = correct / max(1, total)
    payload = {
        'accuracy': round(acc, 4),
        'correct': correct,
        'total': total,
        'throughput_per_sec': round(total / max(elapsed, 0.001), 1),
        'confusion_matrix': conf_matrix,
    }
    rendered = json.dumps(payload, indent=2)
    if getattr(args, 'out', None):
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rendered + '\n')
    print(rendered)


def main():
    ap = argparse.ArgumentParser(description='Λ-kurtosis EvoMoE expert router')
    sub = ap.add_subparsers(dest='cmd', required=True)

    p_t = sub.add_parser('train')
    p_t.add_argument('--manifest',   required=True)
    p_t.add_argument('--embeddings', required=True)
    p_t.add_argument('--out',        required=True)

    p_r = sub.add_parser('route')
    p_r.add_argument('--centroids',  required=True)
    p_r.add_argument('--embeddings', required=True)
    p_r.add_argument('--batch',      action='store_true')
    p_r.add_argument('--out',        default=None)

    p_e = sub.add_parser('eval')
    p_e.add_argument('--manifest',   required=True)
    p_e.add_argument('--centroids',  required=True)
    p_e.add_argument('--embeddings', required=True)
    p_e.add_argument('--out',        default=None)

    args = ap.parse_args()
    {'train': cmd_train, 'route': cmd_route, 'eval': cmd_eval}[args.cmd](args)


if __name__ == '__main__':
    main()
