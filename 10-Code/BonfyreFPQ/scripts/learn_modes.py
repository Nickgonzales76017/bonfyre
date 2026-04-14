#!/usr/bin/env python3
"""
learn_modes.py — Phase 1 Koopman FPQ PoC: Learn and save Koopman modes.

What this does
--------------
For each MLP layer, collects h = silu(gate(x)) ⊙ up(x) ∈ R^{d_int},
the intermediate representation input to W_down. Then:

  1. PCA of H → eigenvectors V ∈ R^{d_int × r_h}
  2. Output modes M = W_down @ V ∈ R^{d_out × r_h}
  3. Reconstruction quality at multiple ranks (quality knee scan)

Koopman inference (replaces W_down @ h per token):
  c_t = V.T @ h_t         (r_h dot products in R^d_int)
  out_t ≈ M @ c_t          (r_h dot products in R^d_out)

  vs current: W_down @ h_t  (d_int × d_out = 5632 × 2048 ops)

Compute reduction per W_down call:
  12 ops (current) → (r_h × d_int + r_h × d_out) = r_h × (d_int + d_out)
  At r_h=128: 128 × 7680 / (5632 × 2048) = 983K / 11.5M ≈ 12× fewer ops

Bandwidth reduction (main benefit):
  W_down sits in DRAM, read once per token per layer.
  Modes M+V fit in L3 cache after first load (3.9 MB at r_h=128, fp32).
  Effective bandwidth per token: 226 Mbits → 2 Kbits (≈110,000× reduction).

Usage
-----
  python3 scripts/learn_modes.py \\
      --model TinyLlama/TinyLlama-1.1B-Chat-v1.0 \\
      --layers 0,4,8,12,16,20 \\
      --n-samples 300 \\
      --device mps \\
      --out-dir results/modes

Output
------
  results/modes/<model_slug>_modes.pt   (torch checkpoint)
  Results printed: per-layer rank scan table
"""

import argparse
import json
import os
import time
from datetime import datetime

import numpy as np
import torch
import torch.nn.functional as F
from transformers import AutoModelForCausalLM, AutoTokenizer

# ──────────────────────────────────────────────
#  CLI
# ──────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--model", default="TinyLlama/TinyLlama-1.1B-Chat-v1.0")
    p.add_argument("--layers", type=str, default=None,
                   help="Comma-separated layer indices (default: all MLP layers)")
    p.add_argument("--n-samples", type=int, default=300)
    p.add_argument("--device", default="cpu")
    p.add_argument("--max-seq-len", type=int, default=512)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--r-cutoff", type=float, default=0.90,
                   help="Explained-variance cutoff for default rank (default: 0.90)")
    p.add_argument("--rank-scan", type=str, default="0.50,0.80,0.90,0.95,0.99",
                   help="Comma-separated EV thresholds for quality knee scan")
    p.add_argument("--mode", default="output-pca",
                   choices=["output-pca", "input-pca"],
                   help="output-pca: SVD of W@h (recommended). "
                        "input-pca: SVD of h (original).")
    p.add_argument("--out-dir", default="results/modes")
    return p.parse_args()

# ──────────────────────────────────────────────
#  WikiText loader (same as koopman_experiments)
# ──────────────────────────────────────────────

WIKITEXT_SAMPLE = (
    "The transformer architecture was introduced in Attention Is All You Need. "
    "Neural networks are computing systems inspired by biological brains. "
    "Machine learning automates analytical model building. "
    "Quantum mechanics describes nature at the scale of atoms. "
    "The speed of light is approximately 299,792,458 metres per second. "
    "Climate change refers to long-term shifts in temperatures on Earth. "
    "DNA carries genetic information in four chemical bases. "
    "Gradient descent minimizes a function by iterating in steepest descent. "
    "The solar system consists of the Sun and eight planets. "
    "The internet is a global system of interconnected networks. "
)

def get_wikitext_tokens(tokenizer, n_sentences=300, max_len=512):
    try:
        from datasets import load_dataset
        ds = load_dataset("wikitext", "wikitext-2-raw-v1", split="train")
        texts = [x["text"] for x in ds if len(x["text"]) > 100][:n_sentences]
        print(f"  Using real WikiText-2 ({len(texts)} passages)")
    except Exception:
        sentences = WIKITEXT_SAMPLE.split(". ")
        texts = (sentences * ((n_sentences // len(sentences)) + 2))[:n_sentences]
        print(f"  Using built-in sample sentences ({len(texts)} passages)")
    chunks = []
    for t in texts:
        ids = tokenizer(t, return_tensors="pt", truncation=True, max_length=max_len).input_ids
        if ids.shape[1] >= 8:
            chunks.append(ids)
    return chunks

# ──────────────────────────────────────────────
#  h-activation collection (clean: separate counter)
# ──────────────────────────────────────────────

def collect_h_activations(model, tokenizer, layer_indices, n_samples, device, max_seq_len):
    """
    Collect h = silu(gate(x)) ⊙ up(x) cleanly without counter-sharing bug.
    Each layer gets exactly n_samples rows.
    """
    model.eval()
    h_records  = {idx: [] for idx in layer_indices}
    h_counts   = {idx: 0  for idx in layer_indices}
    hooks      = []

    def make_down_hook(layer_idx):
        def hook(module, inp):
            if h_counts[layer_idx] >= n_samples:
                return
            h = inp[0].detach().float()
            flat = h.reshape(-1, h.shape[-1])
            # Only take what we still need
            needed = n_samples - h_counts[layer_idx]
            flat = flat[:needed]
            h_records[layer_idx].append(flat.cpu())
            h_counts[layer_idx] += flat.shape[0]
        return hook

    for idx in layer_indices:
        mlp = model.model.layers[idx].mlp
        if hasattr(mlp, "down_proj"):
            hooks.append(mlp.down_proj.register_forward_pre_hook(make_down_hook(idx)))
        else:
            print(f"  WARNING: Layer {idx} has no down_proj — skipping h collection")

    chunks = get_wikitext_tokens(tokenizer, n_sentences=500, max_len=max_seq_len)
    print(f"  Collecting h activations ({n_samples} target per layer)...")

    with torch.no_grad():
        for chunk in chunks:
            if all(h_counts[idx] >= n_samples for idx in layer_indices):
                break
            try:
                model(chunk.to(device))
            except Exception as e:
                print(f"  [skip chunk: {e}]")

    for h in hooks:
        h.remove()

    results = {}
    for idx in layer_indices:
        if h_records[idx]:
            H = torch.cat(h_records[idx], dim=0)[:n_samples]
            results[idx] = H
            print(f"  Layer {idx}: {H.shape[0]} h-vectors, d_int={H.shape[1]}")
        else:
            print(f"  Layer {idx}: NO h activations captured")
            results[idx] = None

    return results

# ──────────────────────────────────────────────
#  Mode learning: PCA + modes + quality scan
# ──────────────────────────────────────────────

def learn_layer_modes(H, W_down, rank_thresholds, r_cutoff=0.90, mode="output-pca"):
    """
    Given:
      H       : (N, d_int) — h activations (pre-down_proj)
      W_down  : (d_out, d_int) — down_proj weight
      rank_thresholds: list of explained-variance thresholds
      mode    : 'input-pca'  — PCA of h (input variance, original approach)
              | 'output-pca' — PCA of W_down @ h (output variance, recommended)

    Returns:
      V         : (d_int, r_h) — input projectors  (hook: c = h_c @ V)
      M         : (d_out, r_h) — output modes       (hook: approx = c @ M.T)
      mu_offset : (d_out,)    — W_down @ mu_h, must be added back at inference
      stats     : dict

    Inference path (identical for both modes, only V/M differ):
      c_t    = (h_t - mu_h) @ V       # (r_h,)
      out_t  = c_t @ M.T + mu_offset  # (d_out,)

    output-pca (recommended):
      Directly minimises ||W_down h_c - M V.T h_c||_F on training data.
      Generalises much better out-of-sample than input-pca because we
      optimise the output reconstruction loss, not input variance.

    input-pca (original):
      V = top eigenvectors of Cov(h), M = W_down @ V.
      In-sample cosine high but degrades out-of-sample.
    """
    N, d_int = H.shape
    d_out = W_down.shape[0]

    # Center h
    mu_h  = H.mean(0)
    H_c   = (H - mu_h).float()
    W     = W_down.float()              # (d_out, d_int)
    mu_offset = (W @ mu_h.float()).contiguous()  # (d_out,)

    # ── Decomposition
    if mode == "output-pca":
        # SVD of Y_c = H_c @ W.T  (N × d_out)
        # Right sing-vecs of Y_c (= left sing-vecs of W @ H_c.T) are output modes
        Y_c          = H_c @ W.T                   # (N, d_out)
        _, S_y, Vy   = torch.linalg.svd(Y_c, full_matrices=False)
        # Vy: (min(N,d_out), d_out) — rows are right sing-vecs of Y_c
        V_out_full  = Vy.T                          # (d_out, min(N,d_out)) output modes
        V_stor_full = W.T @ V_out_full              # (d_int, min(N,d_out)) input projectors

        S2     = S_y ** 2
        cumvar = (S2 / S2.sum()).cumsum(0)
        n_modes = V_out_full.shape[1]

    else:  # mode == "input-pca"
        _, S_h, Vh   = torch.linalg.svd(H_c, full_matrices=False)
        V_stor_full = Vh.T                          # (d_int, min(N,d_int)) eigenvectors
        V_out_full  = W @ V_stor_full               # (d_out, min(N,d_int))

        S2     = S_h ** 2
        cumvar = (S2 / S2.sum()).cumsum(0)
        n_modes = V_stor_full.shape[1]

    r_h_default = int((cumvar < r_cutoff).sum().item()) + 1
    r_h_default = min(r_h_default, n_modes - 1)

    max_rank = int((cumvar < max(rank_thresholds)).sum().item()) + 2
    max_rank = min(max_rank, n_modes)

    V_stor_max = V_stor_full[:, :max_rank]   # (d_int, max_rank)
    V_out_max  = V_out_full[:, :max_rank]    # (d_out, max_rank)

    # ── Quality scan
    Y_c_real = H_c @ W.T   # (N, d_out) — centered real output

    scan = {}
    for thr in rank_thresholds:
        r = int((cumvar < thr).sum().item()) + 1
        r = min(r, max_rank)
        V_s = V_stor_max[:, :r]   # (d_int, r)
        V_o = V_out_max[:,  :r]   # (d_out, r)

        with torch.no_grad():
            C_r    = H_c @ V_s            # (N, r)
            approx = C_r @ V_o.T          # (N, d_out)
            cos    = F.cosine_similarity(approx, Y_c_real, dim=-1).mean().item()
            rel_err = ((approx - Y_c_real).norm() / Y_c_real.norm().clamp(min=1e-12)).item()

        scan[f"{thr:.2f}"] = {
            "r_h": r,
            "r_h_fraction": round(r / d_int, 4),
            "cosine": round(cos, 6),
            "rel_error": round(rel_err, 6),
            "AMB_kbits": round(r * 16 / 1e3, 3),
        }

    V_save = V_stor_max.contiguous()   # (d_int, max_rank) — stored as V in checkpoint
    M_save = V_out_max.contiguous()    # (d_out, max_rank) — stored as M in checkpoint

    stats = {
        "N": N,
        "d_int": d_int,
        "d_out": d_out,
        "mode": mode,
        "r_h_default": r_h_default,
        "r_h_fraction": round(r_h_default / d_int, 4),
        "rank_scan": scan,
        "mu_h_norm": round(mu_h.norm().item(), 4),
        "mu_offset_norm": round(mu_offset.norm().item(), 4),
        "max_rank_saved": max_rank,
    }

    return V_save, M_save, mu_offset, stats

# ──────────────────────────────────────────────
#  Main
# ──────────────────────────────────────────────

def main():
    args = parse_args()
    torch.manual_seed(args.seed)
    np.random.seed(args.seed)

    os.makedirs(args.out_dir, exist_ok=True)
    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_slug = args.model.replace("/", "_")
    out_path   = os.path.join(args.out_dir, f"{model_slug}_modes.pt")

    rank_thresholds = [float(x) for x in args.rank_scan.split(",")]
    r_cutoff = args.r_cutoff

    print(f"\n{'='*70}")
    print(f"  Koopman Mode Learning — BonfyreFPQ / Ember")
    print(f"  Model  : {args.model}")
    print(f"  Device : {args.device}")
    print(f"  N      : {args.n_samples}")
    print(f"  r_cutoff EV threshold : {r_cutoff*100:.0f}%")
    print(f"  Rank scan : {args.rank_scan}")
    print(f"  Mode      : {args.mode}")
    print(f"{'='*70}\n")

    print("Loading model...")
    tokenizer = AutoTokenizer.from_pretrained(args.model)
    model     = AutoModelForCausalLM.from_pretrained(
        args.model, torch_dtype=torch.float32, low_cpu_mem_usage=True
    ).to(args.device)
    model.eval()

    d         = model.config.hidden_size
    n_layers  = model.config.num_hidden_layers
    print(f"  d={d}, L={n_layers} layers\n")

    layer_indices = (
        [int(x) for x in args.layers.split(",")]
        if args.layers else list(range(n_layers))
    )

    # ── Collect h activations
    t0 = time.time()
    h_acts = collect_h_activations(
        model, tokenizer, layer_indices,
        args.n_samples, args.device, args.max_seq_len
    )

    # ── Learn modes per layer
    print(f"\nLearning modes (rank scan at {[int(t*100) for t in rank_thresholds]}% EV)...")
    print()

    AMB_now_full = sum(
        p.numel() for p in model.model.layers[layer_indices[0]].mlp.parameters()
    ) * 6.55  # bits, using FPQ v12 bpw

    saved_modes = {}
    all_stats   = {}

    for layer_idx in layer_indices:
        H = h_acts.get(layer_idx)
        if H is None:
            print(f"  Layer {layer_idx}: SKIPPED (no h activations)")
            continue

        W_down = model.model.layers[layer_idx].mlp.down_proj.weight.detach().cpu()

        V, M, mu_offset, stats = learn_layer_modes(
            H, W_down, rank_thresholds, r_cutoff=r_cutoff, mode=args.mode)

        saved_modes[layer_idx] = {
            "V": V,            # (d_int, max_rank)
            "M": M,            # (d_out, max_rank)
            "mu_h": H.mean(0).cpu().float(),      # (d_int,)
            "mu_offset": mu_offset.cpu().float(), # (d_out,) = W_down @ mu_h
            "r_h": stats["r_h_default"],
            "max_rank": stats["max_rank_saved"],
            "d_int": stats["d_int"],
            "d_out": stats["d_out"],
        }
        all_stats[layer_idx] = stats

        # Print rank scan table
        r_h   = stats["r_h_default"]
        d_int = stats["d_int"]
        print(f"  Layer {layer_idx}:  d_int={d_int}  N={stats['N']}  "
              f"default r_h@{r_cutoff*100:.0f}%EV = {r_h} ({r_h/d_int*100:.1f}%)")
        print(f"  {'Thresh':>8} | {'r_h':>6} | {'r/d_int':>8} | "
              f"{'cos(approx,real)':>18} | {'rel_err':>9} | {'AMB kbits':>10}")
        print(f"  {'-'*8}-+-{'-'*6}-+-{'-'*8}-+-{'-'*18}-+-{'-'*9}-+-{'-'*10}")
        for thr_key, s in stats["rank_scan"].items():
            print(f"  {thr_key:>8} | {s['r_h']:>6} | {s['r_h_fraction']*100:>7.1f}% | "
                  f"{s['cosine']:>18.6f} | {s['rel_error']:>9.6f} | {s['AMB_kbits']:>9.2f}K")

        best = stats["rank_scan"].get(f"{r_cutoff:.2f}", list(stats["rank_scan"].values())[0])
        AMB_koop = best["AMB_kbits"] * 1000  # bits
        gain = AMB_now_full / max(AMB_koop, 1)
        V_mb = V.numel() * 4 / 1e6
        M_mb = M.numel() * 4 / 1e6
        print(f"\n    AMB_now  = {AMB_now_full/1e6:.1f} Mbits  |  "
              f"AMB_koop = {best['AMB_kbits']:.2f} Kbits  |  "
              f"Gain = {gain:.0f}x")
        print(f"    Static overhead: V={V_mb:.2f} MB  M={M_mb:.2f} MB  "
              f"= {(V_mb+M_mb):.2f} MB/layer\n")

    elapsed = time.time() - t0

    # ── Save checkpoint
    checkpoint = {
        "model": args.model,
        "timestamp": timestamp,
        "elapsed_s": round(elapsed, 1),
        "n_samples": args.n_samples,
        "r_cutoff": r_cutoff,
        "rank_thresholds": rank_thresholds,
        "layer_indices": layer_indices,
        "d": d,
        "n_layers": n_layers,
        "modes": saved_modes,
        "stats": {str(k): v for k, v in all_stats.items()},
    }
    torch.save(checkpoint, out_path)

    # Also save a JSON summary (without tensors)
    summary_path = out_path.replace(".pt", "_summary.json")
    json_summary = {
        "model": args.model,
        "timestamp": timestamp,
        "elapsed_s": round(elapsed, 1),
        "n_samples": args.n_samples,
        "r_cutoff": r_cutoff,
        "layers": {
            str(k): {
                "r_h": v["r_h"],
                "d_int": v["d_int"],
                "d_out": v["d_out"],
                "V_shape": list(v["V"].shape),
                "M_shape": list(v["M"].shape),
                "max_rank_saved": v["max_rank"],
                "V_MB": round(v["V"].numel() * 4 / 1e6, 3),
                "M_MB": round(v["M"].numel() * 4 / 1e6, 3),
                "mu_offset_norm": round(all_stats[k]["mu_offset_norm"], 4),
                **{k2: v2 for k2, v2 in all_stats[k]["rank_scan"].items()},
            }
            for k, v in saved_modes.items()
        },
    }
    with open(summary_path, "w") as f:
        json.dump(json_summary, f, indent=2)

    total_MB = sum(
        (v["V"].numel() + v["M"].numel()) * 4 / 1e6
        for v in saved_modes.values()
    )
    print(f"{'='*70}")
    print(f"  Mode learning complete in {elapsed:.1f}s")
    print(f"  Layers saved : {len(saved_modes)}")
    print(f"  Total overhead: {total_MB:.1f} MB for {len(saved_modes)} layers")
    print(f"  Checkpoint   : {out_path}")
    print(f"  Summary      : {summary_path}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
