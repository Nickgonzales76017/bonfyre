#!/usr/bin/env python3
"""
fpq_legal_bench.py — FPQ COORD v4 perplexity on legal-domain corpus.

Extends perplexity_benchmark.py with a --corpus flag that draws text
from _djvu.txt files instead of WikiText-2.

Benchmark design:
  1. Load base model (Qwen/Qwen2-0.5B by default — tiny, fast)
  2. Apply FPQ COORD roundtrip to all weight tensors (pure numpy, no CUDA needed)
  3. Evaluate PPL on held-out eval split:
       a) WikiText-2 (reference baseline)
       b) Per-doctype legal text from _djvu.txt corpus
  4. Compute PPL delta (ppl_quantized / ppl_original) per corpus
  5. Hypothesis: domain-shift widens PPL ratio on legal proper nouns + date patterns

Interpretation:
  PPL ratio ≈ 1.0 → quantization is domain-neutral (good)
  PPL ratio >> 1.0 on legal specifically → the model's legal representations
  are more sensitive to quantization noise than WikiText-2 representations.
  This informs whether FPQ needs domain-adaptive scaling for legal deployments.

Usage:
  python3 fpq_legal_bench.py \\
    --corpus /tmp/epstein-bench/corpus \\
    --bits 3 \\
    --sample 20 \\
    --model Qwen/Qwen2-0.5B

  # Skip legal corpus, just WikiText-2
  python3 fpq_legal_bench.py --no-corpus --bits 3 --model Qwen/Qwen2-0.5B

  # Offline: use pre-extracted texts from ingest manifest
  python3 fpq_legal_bench.py \\
    --manifest /tmp/epstein-bench/ingested/ingest_manifest.jsonl \\
    --bits 3 --sample 20

Outputs: JSON to stdout
  {
    "model": "...",
    "bits": 3,
    "wikitext2": {"ppl_orig": 14.2, "ppl_quant": 14.8, "ratio": 1.042},
    "per_doctype": {
      "fbi_302":       {"ppl_orig": ..., "ppl_quant": ..., "ratio": ...},
      "grand_jury":    {...},
      "court_filing":  {...},
      "investigative": {...}
    }
  }
"""

import argparse
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Optional

import numpy as np

# ── FPQ COORD v4 roundtrip (copied verbatim from perplexity_benchmark.py) ────

BLOCK_DIM = 256

LLOYD = {
    2: {
        "bounds":  np.array([-0.9816, 0.0, 0.9816]),
        "centers": np.array([-1.5104, -0.4528, 0.4528, 1.5104]),
    },
    3: {
        "bounds":  np.array([-1.7479, -1.0500, -0.5006, 0.0, 0.5006, 1.0500, 1.7479]),
        "centers": np.array([-2.1520, -1.3440, -0.7560, -0.2451,
                               0.2451,  0.7560,  1.3440,  2.1520]),
    },
    4: {
        "bounds":  np.array([-2.4008, -1.8440, -1.4371, -1.0993, -0.7977,
                             -0.5157, -0.2451, 0.0, 0.2451, 0.5157,
                              0.7977,  1.0993,  1.4371,  1.8440,  2.4008]),
        "centers": np.array([-2.7326, -2.0690, -1.6180, -1.2562, -0.9423,
                             -0.6568, -0.3881, -0.1284,  0.1284,  0.3881,
                              0.6568,  0.9423,  1.2562,  1.6180,  2.0690,
                              2.7326]),
    },
}


def _random_signs(n: int, seed: int) -> np.ndarray:
    rng = np.random.RandomState(seed & 0xFFFFFFFF)
    return rng.choice([-1.0, 1.0], size=n).astype(np.float32)


def _fwht_batch(blocks: np.ndarray) -> np.ndarray:
    n = blocks.shape[1]
    h = 1
    while h < n:
        even = np.arange(0, n, 2 * h)
        for j_off in range(h):
            j = even + j_off
            a = blocks[:, j].copy(); b = blocks[:, j + h].copy()
            blocks[:, j] = a + b; blocks[:, j + h] = a - b
        h *= 2
    return blocks


def fpq_roundtrip_tensor(weights: np.ndarray, name: str, bits: int = 3):
    """Apply FPQ COORD roundtrip. Returns (reconstructed_weights, cosine_similarity)."""
    orig_shape = weights.shape
    flat = weights.flatten().astype(np.float32)
    total = len(flat)
    if total < BLOCK_DIM:
        return weights, 1.0

    seed = 0x12345678
    for ch in name:
        seed = (seed * 31 + ord(ch)) & 0xFFFFFFFFFFFFFFFF

    n_blocks = (total + BLOCK_DIM - 1) // BLOCK_DIM
    padded = np.zeros(n_blocks * BLOCK_DIM, dtype=np.float32)
    padded[:total] = flat
    blocks = padded.reshape(n_blocks, BLOCK_DIM)

    sign_arrays = [_random_signs(BLOCK_DIM, seed ^ b) for b in range(n_blocks)]
    for b, signs in enumerate(sign_arrays):
        blocks[b] *= signs

    blocks = _fwht_batch(blocks)
    rms = np.maximum(np.sqrt(np.mean(blocks ** 2, axis=1)), 1e-10)
    normalized = blocks / rms[:, None]

    bounds = LLOYD[bits]["bounds"]; centers = LLOYD[bits]["centers"]
    dequantized = centers[np.searchsorted(bounds, normalized)]

    blocks_recon = _fwht_batch(dequantized * rms[:, None]) / BLOCK_DIM
    for b, signs in enumerate(sign_arrays):
        blocks_recon[b] *= signs

    decoded = blocks_recon.flatten()[:total]
    cos = float(np.dot(flat, decoded) / (
        np.linalg.norm(flat) * np.linalg.norm(decoded) + 1e-10))
    return decoded.reshape(orig_shape), cos


# ── corpus loading ─────────────────────────────────────────────────────────────

DOCTYPES = ['fbi_302', 'grand_jury', 'court_filing', 'investigative']

def classify_doc(text: str) -> str:
    low = text[:2000].lower()
    if 'fbi' in low and ('302' in low or 'federal bureau' in low):
        return 'fbi_302'
    if sum(1 for l in text.splitlines()[:80] if l.strip().startswith(('Q.', 'A.'))) >= 4:
        return 'grand_jury'
    if 'district court' in low or 'united states v.' in low:
        return 'court_filing'
    return 'investigative'


def load_corpus_texts(
    corpus_dir: Optional[Path],
    manifest_path: Optional[Path],
    per_type_n: int = 20,
    min_chars: int = 500,
) -> dict[str, list[str]]:
    """Load up to per_type_n text samples per document type."""
    by_type: dict[str, list[str]] = {t: [] for t in DOCTYPES}

    if manifest_path and manifest_path.exists():
        with manifest_path.open() as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                dtype = rec.get('doctype') or rec.get('type', 'investigative')
                text  = rec.get('text', '')
                if dtype in by_type and len(text) >= min_chars:
                    if len(by_type[dtype]) < per_type_n:
                        by_type[dtype].append(text)
        return by_type

    if corpus_dir and corpus_dir.exists():
        for path in sorted(corpus_dir.rglob('*_djvu.txt')):
            try:
                text = path.read_text(errors='replace')
            except OSError:
                continue
            if len(text) < min_chars:
                continue
            dtype = classify_doc(text)
            if len(by_type[dtype]) < per_type_n:
                by_type[dtype].append(text)
        return by_type

    return by_type


def load_wikitext2(max_chars: int = 50000) -> str:
    """Load WikiText-2 from HuggingFace datasets if available."""
    try:
        from datasets import load_dataset
        ds = load_dataset('wikitext', 'wikitext-2-raw-v1', split='validation',
                          trust_remote_code=True)
        return ' '.join(ds['text'])[:max_chars]
    except Exception:
        pass
    return (
        "The history of natural language processing generally started in the 1950s, "
        "although work can be found from earlier periods. In 1950, Alan Turing published "
        "an article titled Computing Machinery and Intelligence which proposed what is now "
        "called the Turing test as a criterion of intelligence, though at the time that "
        "was not articulated as a problem separate from artificial intelligence."
    ) * 20


# ── PPL evaluation ─────────────────────────────────────────────────────────────

def evaluate_perplexity(model, tokenizer, text: str,
                        max_length: int = 512, stride: int = 256) -> float:
    import torch
    model.eval()
    encodings = tokenizer(text, return_tensors='pt', truncation=False)
    input_ids = encodings.input_ids[0]
    seq_len = len(input_ids)
    if seq_len < 4:
        return float('nan')

    nlls = []
    prev_end = 0
    for begin in range(0, seq_len, stride):
        end = min(begin + max_length, seq_len)
        target_len = end - max(begin, prev_end)
        if target_len <= 0:
            prev_end = end; continue

        chunk = input_ids[begin:end].unsqueeze(0)
        labels = chunk.clone()
        labels[:, :end - begin - target_len] = -100

        with torch.no_grad():
            out = model(chunk, labels=labels)
            nlls.append(out.loss.item() * target_len)
        prev_end = end
        if end == seq_len: break

    total = sum(nlls); tokens = seq_len
    return math.exp(total / tokens) if tokens > 0 else float('nan')


def sample_text(texts: list[str], max_chars: int = 30000) -> str:
    combined = ' '.join(t[:3000] for t in texts)
    return combined[:max_chars]


# ── quantize model ────────────────────────────────────────────────────────────

def quantize_model(model, bits: int) -> tuple:
    """
    Apply in-place FPQ COORD roundtrip to all nn.Linear weight tensors.
    Returns (modified_model, cosine_stats_dict).
    """
    import torch
    import torch.nn as nn
    cosines = {}
    with torch.no_grad():
        for name, module in model.named_modules():
            if isinstance(module, nn.Linear):
                w = module.weight.detach().cpu().numpy()
                q, cos = fpq_roundtrip_tensor(w, name, bits)
                module.weight.copy_(torch.from_numpy(q))
                cosines[name] = round(cos, 6)
    return model, cosines


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description='FPQ legal-domain PPL benchmark')
    ap.add_argument('--model',    default='Qwen/Qwen2-0.5B')
    ap.add_argument('--bits',     type=int, default=3, choices=[2, 3, 4])
    ap.add_argument('--corpus',   default=None,  help='Corpus dir with _djvu.txt files')
    ap.add_argument('--manifest', default=None,  help='ingest_manifest.jsonl from djvu_ingest.py')
    ap.add_argument('--sample',   type=int, default=20, help='Docs per doctype')
    ap.add_argument('--max-length', type=int, default=512)
    ap.add_argument('--no-corpus',  action='store_true', help='Skip legal corpus, WikiText-2 only')
    ap.add_argument('--out', default=None, help='Optional JSON output path')
    args = ap.parse_args()

    try:
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer
    except ImportError:
        print('ERROR: transformers and torch are required.  '
              'pip install transformers torch', file=sys.stderr)
        sys.exit(1)

    print(f'Loading {args.model}…', file=sys.stderr)
    t0 = time.monotonic()
    tokenizer = AutoTokenizer.from_pretrained(args.model)
    model = AutoModelForCausalLM.from_pretrained(args.model, torch_dtype=torch.float32)
    model.eval()
    print(f'Model loaded in {time.monotonic()-t0:.1f}s', file=sys.stderr)

    results: dict = {'model': args.model, 'bits': args.bits}

    # ── WikiText-2 baseline ────────────────────────────────────────────────────
    print('Evaluating WikiText-2 (original)…', file=sys.stderr)
    wt2_text = load_wikitext2()
    ppl_orig_wt2 = evaluate_perplexity(model, tokenizer, wt2_text,
                                        max_length=args.max_length)
    print(f'  WikiText-2 orig PPL: {ppl_orig_wt2:.3f}', file=sys.stderr)

    # ── Legal corpus baseline (before quantization) ───────────────────────────
    legal_orig: dict[str, float] = {}
    if not args.no_corpus:
        corpus_dir  = Path(args.corpus)  if args.corpus  else None
        manifest    = Path(args.manifest) if args.manifest else None
        by_type = load_corpus_texts(corpus_dir, manifest, per_type_n=args.sample)
        for dtype in DOCTYPES:
            texts = by_type.get(dtype, [])
            if not texts:
                print(f'  no {dtype} docs found, skipping', file=sys.stderr)
                legal_orig[dtype] = float('nan')
                continue
            text_sample = sample_text(texts)
            ppl = evaluate_perplexity(model, tokenizer, text_sample,
                                       max_length=args.max_length)
            legal_orig[dtype] = ppl
            print(f'  {dtype} orig PPL: {ppl:.3f}  ({len(texts)} docs)', file=sys.stderr)

    # ── Apply FPQ quantization ─────────────────────────────────────────────────
    print(f'Quantizing model (FPQ COORD {args.bits}-bit)…', file=sys.stderr)
    t0 = time.monotonic()
    model, cosines = quantize_model(model, args.bits)
    elapsed = time.monotonic() - t0
    mean_cos = float(np.mean(list(cosines.values()))) if cosines else 0.0
    print(f'  Quantized {len(cosines)} tensors in {elapsed:.1f}s  '
          f'mean cosine={mean_cos:.6f}', file=sys.stderr)

    # ── WikiText-2 post-quantization ───────────────────────────────────────────
    print('Evaluating WikiText-2 (quantized)…', file=sys.stderr)
    ppl_quant_wt2 = evaluate_perplexity(model, tokenizer, wt2_text,
                                         max_length=args.max_length)
    print(f'  WikiText-2 quant PPL: {ppl_quant_wt2:.3f}', file=sys.stderr)
    results['wikitext2'] = {
        'ppl_orig':  round(ppl_orig_wt2, 4),
        'ppl_quant': round(ppl_quant_wt2, 4),
        'ratio':     round(ppl_quant_wt2 / max(ppl_orig_wt2, 0.01), 6),
    }

    # ── Legal corpus post-quantization ────────────────────────────────────────
    if not args.no_corpus:
        per_doctype: dict = {}
        for dtype in DOCTYPES:
            texts = by_type.get(dtype, [])
            if not texts:
                per_doctype[dtype] = {'ppl_orig': legal_orig[dtype], 'ppl_quant': None, 'ratio': None}
                continue
            text_sample = sample_text(texts)
            ppl_q = evaluate_perplexity(model, tokenizer, text_sample,
                                          max_length=args.max_length)
            orig  = legal_orig[dtype]
            ratio = ppl_q / max(orig, 0.01) if not math.isnan(orig) else float('nan')
            per_doctype[dtype] = {
                'ppl_orig':  round(orig, 4)  if not math.isnan(orig)  else None,
                'ppl_quant': round(ppl_q, 4) if not math.isnan(ppl_q) else None,
                'ratio':     round(ratio, 6) if not math.isnan(ratio) else None,
                'n_docs':    len(texts),
            }
            print(f'  {dtype}: orig={orig:.3f}  quant={ppl_q:.3f}  '
                  f'ratio={ratio:.4f}', file=sys.stderr)
        results['per_doctype'] = per_doctype

    results['quantization'] = {
        'n_tensors_quantized': len(cosines),
        'mean_cosine':         round(mean_cos, 6),
        'min_cosine':          round(min(cosines.values()), 6) if cosines else None,
        'quantize_sec':        round(elapsed, 2),
    }

    rendered = json.dumps(results, indent=2)
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rendered + '\n')
    print(rendered)


if __name__ == '__main__':
    main()
