#!/usr/bin/env python3
"""Deterministic local embedding CLI.

Usage:
  embed_onnx.py --text TEXT_FILE --out OUT_EMB.npy [--meta-out OUT.json] [--model MODEL.onnx] [--dims 768] [--dry-run]

This stays useful even without onnxruntime by producing a deterministic hashed
embedding from normalized tokens. If onnxruntime is installed later, this file
is the right seam to swap in a real sentence-transformer forward pass.
"""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import List


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--text", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--meta-out", type=Path)
    parser.add_argument("--model", default="sentence_transformer.onnx")
    parser.add_argument("--dims", type=int, default=768)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def normalize_tokens(text: str) -> List[str]:
    return re.findall(r"[a-z0-9']+", text.lower())


def build_deterministic_embedding(text: str, dims: int) -> List[float]:
    tokens = normalize_tokens(text)
    if not tokens:
        return [0.0] * dims

    vector = [0.0] * dims
    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % dims
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        weight = 1.0 + (digest[5] / 255.0)
        vector[index] += sign * weight

    norm = sum(value * value for value in vector) ** 0.5
    if norm > 0:
        vector = [value / norm for value in vector]
    return vector


def write_embedding(path: Path, vector: List[float]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import numpy as np  # type: ignore

        array = np.array(vector, dtype="float32")
        np.save(path, array)
        return "npy"
    except Exception:
        path.write_text(json.dumps({"vector": vector}, indent=2) + "\n", encoding="utf-8")
        return "json"


def main() -> int:
    args = build_parser().parse_args()
    if not args.text.exists():
        print(f"Missing text file: {args.text}", file=sys.stderr)
        return 2

    if args.dry_run:
        print(f"Would load model seam: {args.model}")
        print(f"Would embed transcript: {args.text}")
        print(f"Would write vector artifact: {args.out}")
        if args.meta_out:
            print(f"Would write metadata artifact: {args.meta_out}")
        return 0

    text = args.text.read_text(encoding="utf-8")
    vector = build_deterministic_embedding(text, args.dims)
    output_format = write_embedding(args.out, vector)

    meta_path = args.meta_out or args.out.with_suffix(".json")
    meta_payload = {
        "sourceSystem": "ONNXEmbedder",
        "textPath": str(args.text),
        "vectorPath": str(args.out),
        "vectorFormat": output_format,
        "dims": args.dims,
        "model": args.model,
        "tokens": len(normalize_tokens(text)),
        "deterministic": True,
        "backend": "hashed-token-fallback",
    }
    meta_path.write_text(json.dumps(meta_payload, indent=2) + "\n", encoding="utf-8")
    status_path = args.out.parent / "status.json"
    status_path.write_text(
        json.dumps(
            {
                "sourceSystem": "ONNXEmbedder",
                "status": "completed",
                "vectorPath": str(args.out),
                "metaPath": str(meta_path),
                "deterministic": True,
                "backend": "hashed-token-fallback",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print(f"Wrote embedding to {args.out}")
    print(f"Wrote metadata to {meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
