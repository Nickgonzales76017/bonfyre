#!/usr/bin/env python3
"""
vecf_e8.py — VECF v2: E8-lattice quantized embedding format.

Encodes embedding vectors using E8 lattice quantization on 8-dimensional
subspaces (384 dims = 48 E8 blocks of 8 dims each), matching the same
E8 + per-row scale pattern used in FPQ for weight tensors.

Bit budget:
  FP32 VECF v1:   D × 4 bytes = 1,536 bytes/vector (384-dim)
  VECF v2 E8 3-bit: 48 blocks × 8 coords × 3 bits / 8 + 48 × 2 (scale fp16)
                  = 144 + 96 = 240 bytes/vector  — 6.4× smaller
  VECF v2 E8 4-bit: 48 × 8 × 4 / 8 + 96 = 192 + 96 = 288 bytes/vector — 5.3×

File format (VECF v2):
  [Header]
    magic:       4 bytes  'VCF2'
    version:     uint32   2
    dims:        uint32   total embedding dims
    block_dim:   uint32   8  (E8 subspace size)
    bits:        uint32   3 or 4
    count:       uint32   number of vectors
  [Records] × count
    id_len:      uint16
    id:          bytes
    [Blocks] × (dims / block_dim)
      scale:     float16   per-block L2 norm
      codes:     ceil(block_dim × bits / 8) bytes  packed bit codes

Usage:
  python3 vecf_e8.py encode --in embeddings.jsonl --out corpus.vecf2 --bits 3
  python3 vecf_e8.py decode --in corpus.vecf2 --out decoded.npy [--ids ids.txt]
  python3 vecf_e8.py bench  --embeddings embeddings.jsonl --bits 3
"""

import argparse
import json
import struct
import sys
import time
from pathlib import Path

import numpy as np

VECF2_MAGIC = b'VCF2'
BLOCK_DIM = 8

# ── E8 lattice utilities ─────────────────────────────────────────────────────
# E8 is the densest sphere packing in 8D. A point x is in E8 iff:
#   (coords are all integers or all half-integers) AND (sum is even)
# For quantization: snap to nearest E8 point via:
#   1. Round to nearest integer (Z8 candidate)
#   2. Round to nearest half-integer (Z8 + 0.5 candidate)
#   3. In each, fix the coord with largest rounding error to enforce even sum
#   4. Pick candidate with smaller squared error

def _snap_to_e8(x: np.ndarray) -> np.ndarray:
    """
    Snap a batch of 8D vectors (shape ...,8) to the nearest E8 lattice point.
    Vectorised over leading batch dims.
    """
    # Z8 candidate
    r0 = np.round(x)
    s0 = r0.sum(axis=-1, keepdims=True) % 2
    # find coord with largest rounding error to flip
    err0 = np.abs(x - r0)
    flip0 = np.argmax(err0, axis=-1)
    c0 = r0.copy()
    idx = np.ndindex(r0.shape[:-1])
    for i in idx:
        if s0[i] != 0:
            c0[i][flip0[i]] += 1 if (x[i][flip0[i]] > r0[i][flip0[i]]) else -1

    # Z8+0.5 candidate
    r1 = np.floor(x) + 0.5
    s1 = (r1 * 2).astype(int).sum(axis=-1, keepdims=True) % 2
    err1 = np.abs(x - r1)
    flip1 = np.argmax(err1, axis=-1)
    c1 = r1.copy()
    for i in np.ndindex(r1.shape[:-1]):
        if s1[i] != 0:
            c1[i][flip1[i]] += 1 if (x[i][flip1[i]] > r1[i][flip1[i]]) else -1

    e0 = np.sum((x - c0) ** 2, axis=-1)
    e1 = np.sum((x - c1) ** 2, axis=-1)
    return np.where(e0[..., None] <= e1[..., None], c0, c1)


def quantize_block(block: np.ndarray, bits: int) -> tuple[float, np.ndarray]:
    """
    Quantize one 8-dim block via E8 snap + per-block scale.
    Returns (scale float32, int_codes int8 array of shape (8,)).
    """
    scale = float(np.linalg.norm(block))
    if scale < 1e-9:
        return scale, np.zeros(BLOCK_DIM, dtype=np.int8)
    normed = block / scale
    # Scale to fit E8 integer range for given bits
    max_coord = (1 << (bits - 1)) - 1  # e.g. 3-bit → 3
    scaled = normed * max_coord
    e8_snapped = _snap_to_e8(scaled.reshape(1, BLOCK_DIM)).reshape(BLOCK_DIM)
    codes = np.clip(e8_snapped, -max_coord, max_coord).astype(np.int8)
    return scale, codes


def dequantize_block(scale: float, codes: np.ndarray, bits: int) -> np.ndarray:
    max_coord = (1 << (bits - 1)) - 1
    return codes.astype(np.float32) * (scale / max_coord)


# ── bit packing ───────────────────────────────────────────────────────────────

def pack_codes(codes: np.ndarray, bits: int) -> bytes:
    """Pack int8 codes (range -(2^(bits-1)-1) .. 2^(bits-1)-1) into bytes."""
    n = len(codes)
    # Offset to unsigned: e.g. 3-bit range -3..3 → 0..6 (3 bits unsigned)
    max_c = (1 << (bits - 1)) - 1
    unsigned = (codes.astype(np.int16) + max_c).astype(np.uint16)
    out = bytearray()
    buf = 0
    bits_in_buf = 0
    for v in unsigned:
        buf |= (int(v) << bits_in_buf)
        bits_in_buf += bits
        while bits_in_buf >= 8:
            out.append(buf & 0xFF)
            buf >>= 8
            bits_in_buf -= 8
    if bits_in_buf > 0:
        out.append(buf & 0xFF)
    return bytes(out)


def unpack_codes(data: bytes, n: int, bits: int) -> np.ndarray:
    max_c = (1 << (bits - 1)) - 1
    mask = (1 << bits) - 1
    codes = []
    buf = 0
    bits_in_buf = 0
    byte_iter = iter(data)
    for _ in range(n):
        while bits_in_buf < bits:
            buf |= (next(byte_iter) << bits_in_buf)
            bits_in_buf += 8
        codes.append((buf & mask) - max_c)
        buf >>= bits
        bits_in_buf -= bits
    return np.array(codes, dtype=np.int8)


# ── encode / decode ───────────────────────────────────────────────────────────

def encode(ids: list[str], vecs: np.ndarray, bits: int) -> bytes:
    D = vecs.shape[1]
    assert D % BLOCK_DIM == 0, f'dims must be divisible by {BLOCK_DIM}'
    n_blocks = D // BLOCK_DIM
    bytes_per_block_codes = (BLOCK_DIM * bits + 7) // 8

    out = bytearray()
    # Header
    out += VECF2_MAGIC
    out += struct.pack('<IIIIII', 2, D, BLOCK_DIM, bits, len(ids), n_blocks)

    for vec_id, vec in zip(ids, vecs):
        id_bytes = vec_id.encode('utf-8')
        out += struct.pack('<H', len(id_bytes))
        out += id_bytes
        for b in range(n_blocks):
            block = vec[b*BLOCK_DIM:(b+1)*BLOCK_DIM]
            scale, codes = quantize_block(block, bits)
            out += struct.pack('<e', np.float16(scale))  # fp16 scale
            out += pack_codes(codes, bits)

    return bytes(out)


def decode(data: bytes) -> tuple[list[str], np.ndarray]:
    off = 0
    magic = data[off:off+4]; off += 4
    assert magic == VECF2_MAGIC, f'Bad magic: {magic}'
    _ver, D, block_dim, bits, count, n_blocks = struct.unpack_from('<IIIIII', data, off)
    off += 24

    ids = []
    vecs = np.zeros((count, D), dtype=np.float32)
    bytes_per_block_codes = (block_dim * bits + 7) // 8

    for i in range(count):
        id_len = struct.unpack_from('<H', data, off)[0]; off += 2
        doc_id = data[off:off+id_len].decode('utf-8', 'replace'); off += id_len
        ids.append(doc_id)
        for b in range(n_blocks):
            scale = struct.unpack_from('<e', data, off)[0]; off += 2
            code_bytes = data[off:off+bytes_per_block_codes]
            off += bytes_per_block_codes
            codes = unpack_codes(code_bytes, block_dim, bits)
            vecs[i, b*block_dim:(b+1)*block_dim] = dequantize_block(scale, codes, bits)

    return ids, vecs


# ── loaders (reuse from embedding_compress.py pattern) ───────────────────────

def load_jsonl(path: Path) -> tuple[list[str], np.ndarray]:
    ids, vecs = [], []
    with path.open() as f:
        for line in f:
            rec = json.loads(line)
            ids.append(rec['id'])
            vecs.append(rec['embedding'])
    return ids, np.array(vecs, dtype=np.float32)


def cosine_sim_pairwise(A: np.ndarray, B: np.ndarray, n: int = 500) -> float:
    rng = np.random.default_rng(0)
    idxs = rng.choice(len(A), min(n, len(A)), replace=False)
    a = A[idxs]; b = B[idxs]
    a /= (np.linalg.norm(a, axis=1, keepdims=True) + 1e-12)
    b /= (np.linalg.norm(b, axis=1, keepdims=True) + 1e-12)
    return float(np.mean(np.sum(a * b, axis=1)))


# ── CLI ────────────────────────────────────────────────────────────────────────

def cmd_encode(args):
    ids, vecs = load_jsonl(Path(args.__dict__['in']))
    D = vecs.shape[1]
    assert D % BLOCK_DIM == 0, \
        f'Embedding dim {D} must be divisible by {BLOCK_DIM} for E8 blocks'
    print(f'Encoding {len(ids)} × {D}-dim at {args.bits}-bit…', file=sys.stderr)
    t0 = time.monotonic()
    encoded = encode(ids, vecs, args.bits)
    elapsed = time.monotonic() - t0
    out_path = Path(args.out)
    out_path.write_bytes(encoded)
    v1_size = len(ids) * D * 4
    ratio = v1_size / len(encoded)
    print(f'  VECF v1: {v1_size/1024:.1f} KB  →  VECF v2: {len(encoded)/1024:.1f} KB  '
          f'({ratio:.2f}× smaller)  in {elapsed:.2f}s', file=sys.stderr)


def cmd_decode(args):
    data = Path(args.__dict__['in']).read_bytes()
    print('Decoding…', file=sys.stderr)
    ids, vecs = decode(data)
    np.save(args.out, vecs)
    if args.ids:
        Path(args.ids).write_text('\n'.join(ids))
    print(f'Decoded {len(ids)} vectors → {args.out}', file=sys.stderr)


def cmd_bench(args):
    ids, vecs = load_jsonl(Path(args.embeddings))
    D = vecs.shape[1]
    # Pad dims to multiple of 8 if needed
    if D % BLOCK_DIM != 0:
        pad = BLOCK_DIM - (D % BLOCK_DIM)
        vecs = np.pad(vecs, ((0,0),(0,pad)))
        D = vecs.shape[1]
        print(f'Padded to {D} dims', file=sys.stderr)

    results = []
    for bits in [3, 4]:
        t0 = time.monotonic()
        encoded = encode(ids, vecs, bits)
        enc_time = time.monotonic() - t0
        t1 = time.monotonic()
        _, decoded = decode(encoded)
        dec_time = time.monotonic() - t1

        cos = cosine_sim_pairwise(vecs, decoded)
        v1_size = len(ids) * D * 4
        ratio = v1_size / len(encoded)

        row = {
            'bits': bits,
            'n_vectors': len(ids),
            'original_dims': D,
            'v1_bytes': v1_size,
            'v2_bytes': len(encoded),
            'compression_ratio': round(ratio, 3),
            'mean_cosine': round(cos, 6),
            'encode_sec': round(enc_time, 3),
            'decode_sec': round(dec_time, 3),
        }
        results.append(row)
        print(f'  {bits}-bit: {ratio:.2f}× smaller  cos={cos:.6f}  '
              f'enc={enc_time:.2f}s  dec={dec_time:.2f}s', file=sys.stderr)

    print(json.dumps({'bench': results}, indent=2))


def main():
    ap = argparse.ArgumentParser(description='VECF v2 — E8-quantized embedding format')
    sub = ap.add_subparsers(dest='cmd', required=True)

    p_enc = sub.add_parser('encode')
    p_enc.add_argument('--in',  required=True, dest='in_', metavar='IN')
    p_enc.add_argument('--out', required=True)
    p_enc.add_argument('--bits', type=int, default=3, choices=[3, 4])

    p_dec = sub.add_parser('decode')
    p_dec.add_argument('--in',  required=True, dest='in_', metavar='IN')
    p_dec.add_argument('--out', required=True, help='.npy output')
    p_dec.add_argument('--ids', default=None)

    p_bench = sub.add_parser('bench')
    p_bench.add_argument('--embeddings', required=True)
    p_bench.add_argument('--bits', type=int, default=3, choices=[3, 4])

    args = ap.parse_args()

    # argparse dest collision workaround
    if hasattr(args, 'in_'):
        args.__dict__['in'] = args.in_

    {'encode': cmd_encode, 'decode': cmd_decode, 'bench': cmd_bench}[args.cmd](args)


if __name__ == '__main__':
    main()
