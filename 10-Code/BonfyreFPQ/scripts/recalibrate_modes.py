#!/usr/bin/env python3
"""
recalibrate_modes.py — Phase 3 Koopman FPQ PoC: Iterative re-calibration.

Problem addressed
-----------------
Phase 1+2 (learn_modes + validate_koopman) revealed a cascade degradation:
  - 6/22 layers Koopman (EV=0.99): ΔPPL = +0.29 (excellent)
  - All 22 layers Koopman (EV=0.99): ΔPPL = +7.94 (unacceptable)

Root cause: modes were fit on P_train(h_k), the h-distribution when all
prior layers use the exact W_down. When earlier layers are replaced by
Koopman approximations, layer k receives h from P_koop(h_k) ≠ P_train(h_k).
The modes for layer k are out-of-distribution, compounding errors.

Fix: Iterative re-calibration
-----------------------------
  Iteration 0: baseline modes (from learn_modes.py, on P_train)
  Iteration i: activate Koopman hooks using iter-(i-1) modes; collect
               new h activations under this perturbed distribution;
               re-fit modes on those. Repeat until PPL stabilises.

  Convergence criterion: ΔPPL between successive iterations < tol
  (typically 1-2 iterations suffice if per-layer error is small)

Algorithm per iteration
-----------------------
  For each layer k (in forward order):
    1. Activate iter-(i-1) Koopman hooks on layers 0..k-1
    2. Leave layer k and all later layers as exact W_down
    3. Run N tokens through the model to collect h_k ~ P_koop(h_k)
    4. Re-fit layer k modes on this h_k
  Save new modes checkpoint.

Why forward order matters:
  Layer k's h is only affected by layers 0..k-1. If we fit layer k's
  modes with exact upstream layers, the distribution is still P_train.
  Forward-order fitting ensures each layer sees its own causal predecessors
  already using Koopman approximations.

Usage
-----
  python3 scripts/recalibrate_modes.py \\
      --model TinyLlama/TinyLlama-1.1B-Chat-v1.0 \\
      --seed-modes results/modes/TinyLlama_TinyLlama-1.1B-Chat-v1.0_modes.pt \\
      --n-samples 3000 \\
      --ev 0.99 \\
      --n-iter 2 \\
      --device mps \\
      --out-dir results/modes

Output
------
  results/modes/<model_slug>_modes_recal_iter<N>.pt
  Per-iteration PPL printed; improvement vs baseline reported.
"""

import argparse
import json
import math
import os
import time
from datetime import datetime

import torch
import torch.nn as nn
import torch.nn.functional as F
from transformers import AutoModelForCausalLM, AutoTokenizer

# ──────────────────────────────────────────────
#  CLI
# ──────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--model", default="TinyLlama/TinyLlama-1.1B-Chat-v1.0")
    p.add_argument("--seed-modes", required=True,
                   help="Path to iter-0 modes .pt file from learn_modes.py")
    p.add_argument("--n-samples", type=int, default=3000,
                   help="Activations to collect per layer per iteration (default: 3000)")
    p.add_argument("--ev", type=float, default=0.99,
                   help="Explained-variance threshold at which to activate Koopman hooks (default: 0.99)")
    p.add_argument("--n-iter", type=int, default=2,
                   help="Number of re-calibration iterations (default: 2)")
    p.add_argument("--ppl-tol", type=float, default=0.05,
                   help="Stop early if ΔPPL across iterations < this (default: 0.05)")
    p.add_argument("--collapse-guard", type=int, default=50,
                   help="If a layer's r_h@EV falls below this after fitting on P_koop, "
                        "exclude it from Koopman hooks (use exact W_down). "
                        "Default: 50. Set 0 to disable.")
    p.add_argument("--n-ppl-tokens", type=int, default=5000,
                   help="WikiText-2 tokens for PPL measurement per iteration (default: 5000)")
    p.add_argument("--ppl-stride", type=int, default=512)
    p.add_argument("--max-seq-len", type=int, default=512)
    p.add_argument("--device", default="cpu")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--rank-scan", type=str, default="0.50,0.80,0.90,0.95,0.99",
                   help="EV thresholds to save in recalibrated checkpoint")
    p.add_argument("--out-dir", default="results/modes")
    return p.parse_args()


# ──────────────────────────────────────────────
#  WikiText-2 loaders
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

def get_train_texts(n_sentences=500):
    """WikiText-2 train split for activation collection."""
    try:
        from datasets import load_dataset
        ds = load_dataset("wikitext", "wikitext-2-raw-v1", split="train")
        texts = [x["text"] for x in ds if len(x["text"]) > 100][:n_sentences]
        print(f"  Using WikiText-2 train ({len(texts)} passages)")
        return texts
    except Exception:
        sentences = WIKITEXT_SAMPLE.split(". ")
        texts = (sentences * ((n_sentences // len(sentences)) + 2))[:n_sentences]
        print(f"  Using fallback sample sentences ({len(texts)})")
        return texts


def get_ppl_ids(tokenizer, n_tokens):
    """WikiText-2 test split as a flat token tensor for PPL."""
    try:
        from datasets import load_dataset
        ds = load_dataset("wikitext", "wikitext-2-raw-v1", split="test")
        texts = [x["text"] for x in ds if x["text"].strip()]
        corpus = "\n\n".join(texts)
        print(f"  Using WikiText-2 test for PPL")
    except Exception:
        corpus = WIKITEXT_SAMPLE * ((n_tokens // len(WIKITEXT_SAMPLE)) + 10)
        print("  Using fallback corpus for PPL")
    enc = tokenizer(corpus, return_tensors="pt")
    ids = enc.input_ids[0]
    if ids.shape[0] < n_tokens:
        ids = ids.repeat(math.ceil(n_tokens / ids.shape[0]))[:n_tokens]
    return ids[:n_tokens]


# ──────────────────────────────────────────────
#  Koopman hook (same logic as validate_koopman)
# ──────────────────────────────────────────────

class KoopmanHook:
    """Replaces down_proj output with mu_offset + M_r @ V_r.T @ (h - mu_h)."""

    def __init__(self, V, M, mu_h, mu_offset):
        """
        V         : (d_int, r) — input projectors, on device
        M         : (d_out, r) — output modes, on device
        mu_h      : (d_int,) — training mean of h, on device
        mu_offset : (d_out,) — W_down @ mu_h, on device
        """
        self.V = V
        self.M = M
        self.mu_h = mu_h
        self.mu_offset = mu_offset

    def __call__(self, module, inp, output):
        h = inp[0].float()
        shape = h.shape
        h_flat = h.reshape(-1, shape[-1])
        h_c = h_flat - self.mu_h
        c = h_c @ self.V            # (N, r)
        approx = c @ self.M.T       # (N, d_out)
        approx = approx + self.mu_offset
        if module.bias is not None:
            approx = approx + module.bias
        return approx.reshape(*shape[:-1], -1).to(inp[0].dtype)


def attach_hooks(model, modes_checkpoint, layer_indices, ev_threshold, device,
                 collapse_guard=50):
    """
    Attach KoopmanHook to each layer in layer_indices using the modes stored
    in modes_checkpoint at the rank corresponding to ev_threshold.
    Layers whose r_h < collapse_guard are skipped (exact W_down used instead).
    Returns list of hook handles (call handle.remove() to detach).
    """
    handles = []
    ev_key = f"{ev_threshold:.2f}"
    skipped = []

    for idx in layer_indices:
        str_idx = str(idx)
        if str_idx not in modes_checkpoint.get("stats", {}):
            continue
        layer_stats = modes_checkpoint["stats"][str_idx]
        scan = layer_stats.get("rank_scan", {})
        if ev_key not in scan:
            available = sorted(scan.keys())
            ev_key_used = available[-1] if available else None
            if ev_key_used is None:
                continue
        else:
            ev_key_used = ev_key

        r = scan[ev_key_used]["r_h"]

        # Collapse guard: skip degenerate layers
        if collapse_guard > 0 and r < collapse_guard:
            skipped.append((idx, r))
            continue

        m = modes_checkpoint["modes"][idx]
        V = m["V"][:, :r].to(device).float()
        M = m["M"][:, :r].to(device).float()
        mu_h = m["mu_h"].to(device).float()
        mu_offset = m["mu_offset"].to(device).float()

        hook = KoopmanHook(V, M, mu_h, mu_offset)
        handle = model.model.layers[idx].mlp.down_proj.register_forward_hook(hook)
        handles.append(handle)

    if skipped:
        print(f"    [collapse guard] Skipping layers {[i for i,r in skipped]} "
              f"(r_h={[r for i,r in skipped]} < guard={collapse_guard})")

    return handles


# ──────────────────────────────────────────────
#  h-activation collection (with active hooks on earlier layers)
# ──────────────────────────────────────────────

def collect_h_for_layer(model, tokenizer, target_layer_idx, n_samples,
                         device, max_seq_len, texts):
    """
    Collect h activations for a single target layer.
    Assumes Koopman hooks for layers 0..(target_layer_idx-1) are already
    attached on model — so the h at target_layer_idx is drawn from P_koop.
    """
    model.eval()
    h_buf = []
    h_count = 0

    def hook_fn(module, inp):
        nonlocal h_count
        if h_count >= n_samples:
            return
        h = inp[0].detach().float()
        flat = h.reshape(-1, h.shape[-1])
        needed = n_samples - h_count
        h_buf.append(flat[:needed].cpu())
        h_count += min(flat.shape[0], needed)

    mlp = model.model.layers[target_layer_idx].mlp
    if not hasattr(mlp, "down_proj"):
        return None
    handle = mlp.down_proj.register_forward_pre_hook(hook_fn)

    with torch.no_grad():
        for txt in texts:
            if h_count >= n_samples:
                break
            try:
                ids = tokenizer(txt, return_tensors="pt",
                                truncation=True, max_length=max_seq_len).input_ids
                if ids.shape[1] >= 4:
                    model(ids.to(device))
            except Exception as e:
                pass

    handle.remove()

    if not h_buf:
        return None
    H = torch.cat(h_buf, dim=0)[:n_samples]
    return H


# ──────────────────────────────────────────────
#  Mode fitting (same math as learn_modes.py)
# ──────────────────────────────────────────────

def fit_modes(H, W_down, rank_thresholds, r_cutoff):
    """
    output-PCA: SVD of Y_c = H_c @ W.T.
    Returns (V, M, mu_offset, stats) — same structure as learn_modes.py.
    """
    N, d_int = H.shape
    d_out = W_down.shape[0]
    mu_h = H.mean(0).float()
    H_c = (H - mu_h).float()
    W = W_down.float()
    mu_offset = (W @ mu_h).contiguous()

    Y_c = H_c @ W.T                       # (N, d_out)
    _, S_y, Vy = torch.linalg.svd(Y_c, full_matrices=False)
    V_out_full = Vy.T                      # (d_out, min(N,d_out))
    V_stor_full = W.T @ V_out_full         # (d_int, min(N,d_out))

    S2 = S_y ** 2
    cumvar = (S2 / S2.sum()).cumsum(0)
    n_modes = V_out_full.shape[1]

    r_h_default = int((cumvar < r_cutoff).sum().item()) + 1
    r_h_default = min(r_h_default, n_modes - 1)

    max_rank = int((cumvar < max(rank_thresholds)).sum().item()) + 2
    max_rank = min(max_rank, n_modes)

    V_stor_max = V_stor_full[:, :max_rank].contiguous()
    V_out_max = V_out_full[:, :max_rank].contiguous()

    # Quality scan on training data
    Y_c_real = H_c @ W.T
    scan = {}
    for thr in rank_thresholds:
        r = int((cumvar < thr).sum().item()) + 1
        r = min(r, max_rank)
        V_s = V_stor_max[:, :r]
        V_o = V_out_max[:, :r]
        with torch.no_grad():
            C_r = H_c @ V_s
            approx = C_r @ V_o.T
            cos = F.cosine_similarity(approx, Y_c_real, dim=-1).mean().item()
            rel_err = ((approx - Y_c_real).norm() / Y_c_real.norm().clamp(min=1e-12)).item()
        scan[f"{thr:.2f}"] = {
            "r_h": r,
            "r_h_fraction": round(r / d_int, 4),
            "cosine": round(cos, 6),
            "rel_error": round(rel_err, 6),
            "AMB_kbits": round(r * 16 / 1e3, 3),
        }

    stats = {
        "N": N,
        "d_int": d_int,
        "d_out": d_out,
        "mode": "output-pca",
        "r_h_default": r_h_default,
        "r_h_fraction": round(r_h_default / d_int, 4),
        "rank_scan": scan,
        "mu_h_norm": round(mu_h.norm().item(), 4),
        "mu_offset_norm": round(mu_offset.norm().item(), 4),
        "max_rank_saved": max_rank,
    }

    return V_stor_max, V_out_max, mu_h, mu_offset, stats


# ──────────────────────────────────────────────
#  PPL measurement
# ──────────────────────────────────────────────

def compute_ppl(model, input_ids, max_length, stride, device):
    model.eval()
    seq_len = input_ids.shape[0]
    nlls = []
    prev_end = 0
    with torch.no_grad():
        for begin in range(0, seq_len, stride):
            end = min(begin + max_length, seq_len)
            trg_len = end - prev_end
            ids = input_ids[begin:end].unsqueeze(0).to(device)
            tgt = ids.clone()
            tgt[:, :-trg_len] = -100
            out = model(ids, labels=tgt)
            nlls.append(out.loss.item() * trg_len)
            prev_end = end
            if end == seq_len:
                break
    return math.exp(sum(nlls) / seq_len)


def measure_ppl_with_modes(model, modes_checkpoint, all_layer_indices,
                            ev_threshold, input_ids, max_length, stride, device,
                            collapse_guard=50):
    """Attach all Koopman hooks, measure PPL, then detach."""
    handles = attach_hooks(model, modes_checkpoint, all_layer_indices, ev_threshold, device,
                           collapse_guard=collapse_guard)
    try:
        ppl = compute_ppl(model, input_ids, max_length, stride, device)
    finally:
        for h in handles:
            h.remove()
    return ppl


# ──────────────────────────────────────────────
#  One iteration of re-calibration
# ──────────────────────────────────────────────

def run_calibration_iter(model, tokenizer, prev_checkpoint, layer_indices,
                          ev_threshold, n_samples, rank_thresholds,
                          device, max_seq_len, collapse_guard=50):
    """
    Fit new modes for all layers in layer_indices using the Koopman-perturbed
    h distribution. Layer k is fit with hooks on layers 0..k-1 active.

    Returns a new modes_checkpoint (same schema as learn_modes.py output).
    """
    texts = get_train_texts(n_sentences=600)
    new_modes = {}
    new_stats = {}

    for ki, layer_idx in enumerate(sorted(layer_indices)):
        print(f"\n  [Layer {layer_idx}] collecting h from P_koop "
              f"(hooks active on {layer_indices[:ki]})...")

        # Attach Koopman hooks for all EARLIER layers using PREVIOUS iteration modes
        # Skips any earlier layer whose r_h < collapse_guard (exact W_down used instead)
        earlier = [l for l in layer_indices if l < layer_idx]
        handles = attach_hooks(model, prev_checkpoint, earlier, ev_threshold, device,
                               collapse_guard=collapse_guard)

        try:
            H = collect_h_for_layer(
                model, tokenizer, layer_idx, n_samples,
                device, max_seq_len, texts
            )
        finally:
            for h in handles:
                h.remove()

        if H is None or H.shape[0] < 50:
            print(f"  Layer {layer_idx}: WARNING — insufficient h ({H.shape[0] if H is not None else 0} rows), "
                  f"copying previous modes")
            new_modes[layer_idx] = prev_checkpoint["modes"][layer_idx]
            new_stats[str(layer_idx)] = prev_checkpoint["stats"][str(layer_idx)]
            continue

        W_down = model.model.layers[layer_idx].mlp.down_proj.weight.detach().cpu()
        V, M, mu_h, mu_offset, stats = fit_modes(H, W_down, rank_thresholds, r_cutoff=ev_threshold)

        new_modes[layer_idx] = {
            "V": V,
            "M": M,
            "mu_h": mu_h.cpu().float(),
            "mu_offset": mu_offset.cpu().float(),
            "r_h": stats["r_h_default"],
            "max_rank": stats["max_rank_saved"],
            "d_int": stats["d_int"],
            "d_out": stats["d_out"],
        }
        new_stats[str(layer_idx)] = stats

        ev_key = f"{ev_threshold:.2f}"
        scan_entry = stats["rank_scan"].get(ev_key, {})
        print(f"  Layer {layer_idx}: N={H.shape[0]}  "
              f"r_h@{ev_threshold*100:.0f}%EV = {scan_entry.get('r_h', '?')}  "
              f"cos(in-sample) = {scan_entry.get('cosine', '?')}")

    return new_modes, new_stats


# ──────────────────────────────────────────────
#  Main
# ──────────────────────────────────────────────

def main():
    args = parse_args()
    torch.manual_seed(args.seed)

    os.makedirs(args.out_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_slug = args.model.replace("/", "_")
    rank_thresholds = [float(x) for x in args.rank_scan.split(",")]

    print(f"\n{'='*70}")
    print(f"  Koopman Re-Calibration — Phase 3")
    print(f"  Model       : {args.model}")
    print(f"  Seed modes  : {args.seed_modes}")
    print(f"  EV threshold: {args.ev}")
    print(f"  N/iter      : {args.n_samples}")
    print(f"  Iterations  : {args.n_iter}")
    print(f"  PPL tol     : {args.ppl_tol}")
    print(f"  Device      : {args.device}")
    print(f"{'='*70}\n")

    # Load model
    print("Loading model...")
    tokenizer = AutoTokenizer.from_pretrained(args.model)
    model = AutoModelForCausalLM.from_pretrained(
        args.model, torch_dtype=torch.float32, low_cpu_mem_usage=True
    ).to(args.device)
    model.eval()

    max_length = model.config.max_position_embeddings

    # Load seed modes
    print(f"Loading seed modes from {args.seed_modes}...")
    seed_ckpt = torch.load(args.seed_modes, map_location="cpu")
    layer_indices = sorted(seed_ckpt["modes"].keys())
    print(f"  Layers in seed modes: {layer_indices}")

    # Baseline PPL (no hooks)
    print("\nMeasuring baseline PPL (no Koopman)...")
    ppl_ids = get_ppl_ids(tokenizer, args.n_ppl_tokens)
    t0 = time.time()
    baseline_ppl = compute_ppl(model, ppl_ids, max_length, args.ppl_stride, args.device)
    print(f"  Baseline PPL = {baseline_ppl:.4f}  ({time.time()-t0:.1f}s)")

    # Iter-0 PPL (seed modes, all layers)
    print(f"\nMeasuring iter-0 PPL (seed modes, all {len(layer_indices)} layers at EV={args.ev}, guard={args.collapse_guard})...")
    t0 = time.time()
    iter0_ppl = measure_ppl_with_modes(
        model, seed_ckpt, layer_indices, args.ev,
        ppl_ids, max_length, args.ppl_stride, args.device,
        collapse_guard=args.collapse_guard
    )
    print(f"  Iter-0 PPL = {iter0_ppl:.4f}  (ΔPPL = {iter0_ppl - baseline_ppl:+.4f})  ({time.time()-t0:.1f}s)")

    # Results tracking
    ppl_history = [("iter-0 (seed)", iter0_ppl, iter0_ppl - baseline_ppl)]
    current_ckpt = seed_ckpt
    prev_ppl = iter0_ppl

    for iteration in range(1, args.n_iter + 1):
        print(f"\n{'─'*70}")
        print(f"  RE-CALIBRATION ITERATION {iteration}/{args.n_iter}")
        print(f"{'─'*70}")
        t_iter = time.time()

        new_modes, new_stats = run_calibration_iter(
            model, tokenizer, current_ckpt, layer_indices,
            args.ev, args.n_samples, rank_thresholds,
            args.device, args.max_seq_len,
            collapse_guard=args.collapse_guard
        )

        # Build new checkpoint
        new_ckpt = {
            "model": args.model,
            "timestamp": timestamp,
            "elapsed_s": 0,
            "n_samples": args.n_samples,
            "r_cutoff": args.ev,
            "rank_thresholds": rank_thresholds,
            "layer_indices": layer_indices,
            "d": seed_ckpt["d"],
            "n_layers": seed_ckpt["n_layers"],
            "modes": new_modes,
            "stats": new_stats,
            "recalibration_iter": iteration,
            "seed_modes": args.seed_modes,
            "collapse_guard": args.collapse_guard,
        }

        # Measure PPL
        print(f"\n  Measuring iter-{iteration} PPL...")
        t0 = time.time()
        iter_ppl = measure_ppl_with_modes(
            model, new_ckpt, layer_indices, args.ev,
            ppl_ids, max_length, args.ppl_stride, args.device,
            collapse_guard=args.collapse_guard
        )
        elapsed_iter = time.time() - t_iter
        delta = iter_ppl - baseline_ppl
        improvement = prev_ppl - iter_ppl
        print(f"  Iter-{iteration} PPL = {iter_ppl:.4f}  "
              f"(ΔPPL vs baseline = {delta:+.4f},  "
              f"improvement vs prev = {improvement:+.4f})  "
              f"[{elapsed_iter:.0f}s]")

        ppl_history.append((f"iter-{iteration}", iter_ppl, delta))

        # Save this iteration's checkpoint
        out_name = f"{model_slug}_modes_recal_iter{iteration}_{timestamp}.pt"
        out_path = os.path.join(args.out_dir, out_name)
        new_ckpt["elapsed_s"] = round(elapsed_iter, 1)
        torch.save(new_ckpt, out_path)
        print(f"  Saved: {out_path}")

        current_ckpt = new_ckpt
        prev_ppl = iter_ppl

        # Early stopping
        if abs(improvement) < args.ppl_tol:
            print(f"\n  Early stop: improvement {improvement:+.4f} < tol {args.ppl_tol}")
            break

    # Final summary
    print(f"\n{'='*70}")
    print(f"  RE-CALIBRATION SUMMARY")
    print(f"{'='*70}")
    print(f"  Baseline PPL (no Koopman)          = {baseline_ppl:.4f}")
    print(f"  {'Label':<30} {'PPL':>8}  {'ΔPPL':>8}")
    print(f"  {'-'*30}  {'-'*8}  {'-'*8}")
    for label, ppl, delta in ppl_history:
        print(f"  {label:<30} {ppl:>8.4f}  {delta:>+8.4f}")

    # Save JSON summary
    summary_path = os.path.join(args.out_dir, f"{model_slug}_recal_summary_{timestamp}.json")
    with open(summary_path, "w") as f:
        json.dump({
            "model": args.model,
            "timestamp": timestamp,
            "ev_threshold": args.ev,
            "n_samples": args.n_samples,
            "n_iter_run": len(ppl_history) - 1,
            "baseline_ppl": round(baseline_ppl, 6),
            "ppl_history": [
                {"label": lbl, "ppl": round(ppl, 6), "delta_ppl": round(d, 6)}
                for lbl, ppl, d in ppl_history
            ],
        }, f, indent=2)
    print(f"\n  Summary JSON: {summary_path}")


if __name__ == "__main__":
    main()
