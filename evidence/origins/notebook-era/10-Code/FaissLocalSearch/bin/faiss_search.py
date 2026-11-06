#!/usr/bin/env python3
"""FaissLocalSearch — local embedded-vector search with FAISS or hnswlib.

Usage:
  faiss_search.py build-index <embeddings_dir> [--out index.faiss] [--dim 384] [--dry-run]
  faiss_search.py search <index> <query_embedding.json> [--k 5] [--dry-run]
  faiss_search.py add <index> <embedding.json> --id ID [--dry-run]
"""
import argparse
import json
import struct
import sys
from pathlib import Path


def build_parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd")

    bi = sub.add_parser("build-index")
    bi.add_argument("embeddings_dir", type=Path)
    bi.add_argument("--out", type=Path, default=Path("index.faiss"))
    bi.add_argument("--dim", type=int, default=384)
    bi.add_argument("--dry-run", action="store_true")

    se = sub.add_parser("search")
    se.add_argument("index", type=Path)
    se.add_argument("query", type=Path)
    se.add_argument("--k", type=int, default=5)
    se.add_argument("--dry-run", action="store_true")

    ad = sub.add_parser("add")
    ad.add_argument("index", type=Path)
    ad.add_argument("embedding", type=Path)
    ad.add_argument("--id", required=True)
    ad.add_argument("--dry-run", action="store_true")
    return p


def build_index_native(emb_dir: Path, out: Path, dim: int):
    """Build a naive brute-force index (fallback when faiss not installed)."""
    vectors = []
    ids = []
    for f in sorted(emb_dir.glob("*.json")):
        data = json.loads(f.read_text())
        vec = data.get("embedding", data.get("vector", []))
        if len(vec) != dim:
            print(f"  Skip {f.name}: dim={len(vec)} (expected {dim})")
            continue
        vectors.append(vec)
        ids.append(data.get("id", f.stem))

    # Write binary: [n_vectors:u32][dim:u32][ids_json_bytes_len:u32][ids_json][float32 matrix]
    with open(out, "wb") as fh:
        n = len(vectors)
        fh.write(struct.pack("<III", n, dim, 0))  # placeholder for ids_len
        ids_bytes = json.dumps(ids).encode("utf-8")
        fh.seek(8)
        fh.write(struct.pack("<I", len(ids_bytes)))
        fh.write(ids_bytes)
        for vec in vectors:
            fh.write(struct.pack(f"<{dim}f", *vec))
    print(f"Built index: {out} ({len(vectors)} vectors, dim={dim})")


def search_native(index_path: Path, query_path: Path, k: int):
    """Brute-force cosine search."""
    import math
    query_data = json.loads(query_path.read_text())
    q = query_data.get("embedding", query_data.get("vector", []))

    with open(index_path, "rb") as fh:
        n, dim, ids_len = struct.unpack("<III", fh.read(12))
        ids = json.loads(fh.read(ids_len).decode("utf-8"))
        vectors = []
        for _ in range(n):
            vec = list(struct.unpack(f"<{dim}f", fh.read(dim * 4)))
            vectors.append(vec)

    def cosine(a, b):
        dot = sum(x * y for x, y in zip(a, b))
        na = math.sqrt(sum(x * x for x in a))
        nb = math.sqrt(sum(x * x for x in b))
        if na == 0 or nb == 0:
            return 0.0
        return dot / (na * nb)

    scores = [(ids[i], cosine(q, vectors[i])) for i in range(len(vectors))]
    scores.sort(key=lambda x: -x[1])
    print(f"Top {k} results:")
    for ident, score in scores[:k]:
        print(f"  {ident}: {score:.4f}")


def main():
    args = build_parser().parse_args()
    if not args.cmd:
        print("Usage: faiss_search.py <build-index|search|add> ...")
        return 1

    if args.dry_run:
        print(f"Would run: {args.cmd}")
        return 0

    try:
        import faiss
        import numpy as np
        USE_FAISS = True
    except ImportError:
        USE_FAISS = False

    if args.cmd == "build-index":
        if USE_FAISS:
            index = faiss.IndexFlatIP(args.dim)
            for f in sorted(args.embeddings_dir.glob("*.json")):
                data = json.loads(f.read_text())
                vec = np.array(data.get("embedding", data.get("vector", [])), dtype=np.float32).reshape(1, -1)
                faiss.normalize_L2(vec)
                index.add(vec)
            faiss.write_index(index, str(args.out))
            print(f"Built FAISS index: {args.out} ({index.ntotal} vectors)")
        else:
            build_index_native(args.embeddings_dir, args.out, args.dim)

    elif args.cmd == "search":
        if USE_FAISS:
            index = faiss.read_index(str(args.index))
            query_data = json.loads(args.query.read_text())
            q = np.array(query_data.get("embedding", query_data.get("vector", [])), dtype=np.float32).reshape(1, -1)
            faiss.normalize_L2(q)
            D, I = index.search(q, args.k)
            print(f"Top {args.k}: indices={I[0].tolist()}, scores={D[0].tolist()}")
        else:
            search_native(args.index, args.query, args.k)

    elif args.cmd == "add":
        print("Add not yet implemented for running index. Rebuild index instead.")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
