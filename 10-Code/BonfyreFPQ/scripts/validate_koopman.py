#!/usr/bin/env python3
"""
validate_koopman.py — Phase 2 Koopman FPQ PoC: Validate with real perplexity.

What this does
--------------
Loads the .pt modes file produced by learn_modes.py and:

  1. Per-layer cosine scan — for each specified rank, measures
     cos(W_down @ h,  M_r @ V_r.T @ h) on a held-out activation set.

  2. Full-model perplexity — registers forward hooks on down_proj that
     replace W_down @ h with M_r @ V_r.T @ h, then measures PPL on
     WikiText-2 (same 5K-token sliding window used by FPQ v8).

  3. Comparison table: baseline vs Koopman at each rank.

Ranks tested
------------
  All EV thresholds that are stored in the modes file (50%, 80%, 90%, 95%, 99%)
  plus any extra ranks passed via --ranks.

Reference baselines
-------------------
  FPQ v8   : PPL = 12.07  (cos = 0.9998 at 3-bit), AMB = 226.6 Mbits/token
  FPQx SLI : PPL ≈ 12.00  (cos = 1.0000 in domain),  AMB ≈ 28 Mbits/token
  Baseline : PPL = 11.95  (TinyLlama 1.1B full precision)

Usage
-----
  python3 scripts/validate_koopman.py \\
      --model TinyLlama/TinyLlama-1.1B-Chat-v1.0 \\
      --modes-file results/modes/TinyLlama_TinyLlama-1.1B-Chat-v1.0_modes.pt \\
      --n-tokens 5000 \\
      --device mps
"""

import argparse
import math
import os
import time

import torch
import torch.nn.functional as F
from transformers import AutoModelForCausalLM, AutoTokenizer

# ──────────────────────────────────────────────
#  CLI
# ──────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--model", default="TinyLlama/TinyLlama-1.1B-Chat-v1.0")
    p.add_argument("--modes-file", required=True,
                   help="Path to .pt modes file from learn_modes.py")
    p.add_argument("--layers", type=str, default=None,
                   help="Comma-separated layer indices to validate (default: all in modes file)")
    p.add_argument("--n-tokens", type=int, default=5000,
                   help="Number of WikiText-2 tokens for PPL (default: 5000)")
    p.add_argument("--stride", type=int, default=512,
                   help="Sliding window stride (default: 512)")
    p.add_argument("--device", default="cpu")
    p.add_argument("--n-cosine-samples", type=int, default=200,
                   help="Tokens to collect for cosine measurement (default: 200)")
    p.add_argument("--skip-cosine", action="store_true",
                   help="Skip per-layer cosine measurement, just run PPL")
    p.add_argument("--skip-ppl", action="store_true",
                   help="Skip PPL, just run cosine scan")
    return p.parse_args()


# ──────────────────────────────────────────────
#  WikiText-2 loader (same fallback as learn_modes)
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

def get_ppl_text(tokenizer, n_tokens):
    """Return a flat token tensor of length >= n_tokens from WikiText-2 test."""
    try:
        from datasets import load_dataset
        ds = load_dataset("wikitext", "wikitext-2-raw-v1", split="test")
        texts = [x["text"] for x in ds if x["text"].strip()]
        corpus = "\n\n".join(texts)
        print(f"  Using real WikiText-2 test split")
    except Exception:
        corpus = (WIKITEXT_SAMPLE * ((n_tokens // len(WIKITEXT_SAMPLE)) + 10))
        print("  Using built-in fallback corpus")

    enc = tokenizer(corpus, return_tensors="pt")
    ids = enc.input_ids[0]
    if ids.shape[0] < n_tokens:
        ids = ids.repeat(math.ceil(n_tokens / ids.shape[0]))[:n_tokens]
    else:
        ids = ids[:n_tokens]
    return ids   # (n_tokens,) int64


# ──────────────────────────────────────────────
#  Koopman hook: replaces down_proj with M @ V.T @ h
# ──────────────────────────────────────────────

class KoopmanHook:
    """
    Forward hook for nn.Linear that replaces W_down @ h with
    mu_offset + M_r @ V_r.T @ (h - mu_h).

    Full derivation:
      W_down @ h = W_down @ (mu_h + V V.T (h - mu_h))  + residual
                 = mu_offset + M @ V.T @ h_c             (where mu_offset = W_down @ mu_h)

    mu_offset MUST be added back; omitting it produces a large systematic
    bias in every MLP output and causes perplexity to blow up.
    """
    def __init__(self, V, M, mu_h, mu_offset, replace=True, record_cosine=False):
        """
        V         : (d_int, r_h) on device
        M         : (d_out, r_h) on device
        mu_h      : (d_int,) — training mean of h
        mu_offset : (d_out,) — W_down @ mu_h (precomputed, stored in checkpoint)
        replace   : if True return Koopman approx; if False use real output
        record_cosine : record per-call cosine vs real output
        """
        self.V          = V
        self.M          = M
        self.mu_h       = mu_h
        self.mu_offset  = mu_offset   # (d_out,)
        self.replace    = replace
        self.record_cosine = record_cosine
        self.cosines    = []

    def __call__(self, module, inp, output):
        # inp[0] : (batch, seq, d_int) or (N, d_int)
        h = inp[0].float()
        orig_shape = h.shape
        h_flat = h.reshape(-1, orig_shape[-1])

        h_c    = h_flat - self.mu_h          # center (same as during PCA fitting)
        c      = h_c    @ self.V             # (N, r_h)
        approx = c      @ self.M.T           # (N, d_out)
        approx = approx + self.mu_offset     # add back mean contribution

        # Add liner bias if present
        bias = module.bias
        if bias is not None:
            approx = approx + bias

        # Cosine vs real output
        if self.record_cosine:
            real_flat = output.reshape(-1, output.shape[-1]).float()
            cos = F.cosine_similarity(approx, real_flat, dim=-1)
            self.cosines.append(cos.mean().item())

        if self.replace:
            return approx.reshape(*orig_shape[:-1], -1).to(inp[0].dtype)
        # else: return nothing (default output used)

    def reset(self):
        self.cosines.clear()

    def mean_cosine(self):
        return sum(self.cosines) / len(self.cosines) if self.cosines else float("nan")


# ──────────────────────────────────────────────
#  Perplexity computation
# ──────────────────────────────────────────────

def compute_ppl(model, input_ids, max_length, stride, device):
    """
    Sliding-window perplexity, HuggingFace reference method.
    input_ids : (seq_len,) — already sliced to n_tokens
    Returns PPL (float)
    """
    model.eval()
    seq_len = input_ids.shape[0]
    nlls    = []
    prev_end = 0

    with torch.no_grad():
        for begin in range(0, seq_len, stride):
            end     = min(begin + max_length, seq_len)
            trg_len = end - prev_end

            ids = input_ids[begin:end].unsqueeze(0).to(device)
            tgt = ids.clone()
            tgt[:, :-trg_len] = -100

            out = model(ids, labels=tgt)
            nlls.append(out.loss.item() * trg_len)
            prev_end = end
            if end == seq_len:
                break

    ppl = math.exp(sum(nlls) / seq_len)
    return ppl


# ──────────────────────────────────────────────
#  Per-layer cosine scan (doesn't affect PPL)
# ──────────────────────────────────────────────

def cosine_scan(model, tokenizer, modes_checkpoint, layer_indices, n_samples, device):
    """
    For each layer and each rank stored in the modes file,
    compute cos(W_down @ h,  M_r @ V_r.T @ h) on n_samples tokens.
    Returns: dict[layer_idx][thresh_str] = mean_cosine
    """
    model.eval()

    # Collect h activations for cosine comparison
    h_records = {idx: [] for idx in layer_indices}
    h_counts  = {idx: 0  for idx in layer_indices}
    hooks     = []

    def make_h_hook(layer_idx):
        def hook(module, inp):
            if h_counts[layer_idx] >= n_samples:
                return
            h = inp[0].detach().float()
            flat = h.reshape(-1, h.shape[-1])
            needed = n_samples - h_counts[layer_idx]
            h_records[layer_idx].append(flat[:needed].cpu())
            h_counts[layer_idx] += min(flat.shape[0], needed)
        return hook

    for idx in layer_indices:
        mlp = model.model.layers[idx].mlp
        if hasattr(mlp, "down_proj"):
            hooks.append(mlp.down_proj.register_forward_pre_hook(make_h_hook(idx)))

    # Stream tokens until all layers filled
    try:
        from datasets import load_dataset
        ds = load_dataset("wikitext", "wikitext-2-raw-v1", split="validation")
        texts = [x["text"] for x in ds if x["text"].strip()]
    except Exception:
        texts = (WIKITEXT_SAMPLE.split(". ")) * 50

    with torch.no_grad():
        for txt in texts:
            if all(h_counts[idx] >= n_samples for idx in layer_indices):
                break
            try:
                ids = tokenizer(txt, return_tensors="pt", truncation=True,
                                max_length=512).input_ids
                if ids.shape[1] >= 4:
                    model(ids.to(device))
            except Exception:
                pass

    for h in hooks:
        h.remove()

    # Compute cosines from stored hactivations
    layer_cosines = {}
    stats = modes_checkpoint["stats"]

    for idx in layer_indices:
        if not h_records[idx]:
            print(f"  Layer {idx}: no h captured for cosine scan")
            continue

        H = torch.cat(h_records[idx], dim=0)   # (N, d_int)
        m = modes_checkpoint["modes"][idx]
        W_down = model.model.layers[idx].mlp.down_proj.weight.detach().cpu().float()
        mu_h   = m["mu_h"].cpu().float()

        H_c = H - mu_h
        real  = H_c @ W_down.T               # (N, d_out) true output

        V_full    = m["V"].cpu().float()        # (d_int, max_rank_saved)
        M_full    = m["M"].cpu().float()        # (d_out, max_rank_saved)
        mu_offset = m["mu_offset"].cpu().float()  # (d_out,)

        # Full real output includes mu contribution: W @ h = W @ h_c + W @ mu_h
        real_full = real + mu_offset  # (N, d_out)

        layer_stat = stats.get(str(idx), stats.get(idx, {}))
        scan = layer_stat.get("rank_scan", {})

        cos_dict = {}
        for thr_key, s in scan.items():
            r = s["r_h"]
            r = min(r, V_full.shape[1])
            V_r = V_full[:, :r]
            M_r = M_full[:, :r]
            # Koopman approx (full, including mu_offset)
            c      = H_c @ V_r                      # (N, r)
            approx = c  @ M_r.T + mu_offset          # (N, d_out)
            cos    = F.cosine_similarity(approx, real_full, dim=-1).mean().item()
            cos_dict[thr_key] = (round(cos, 6), r)

        layer_cosines[idx] = cos_dict

    return layer_cosines


# ──────────────────────────────────────────────
#  PPL at each EV threshold
# ──────────────────────────────────────────────

def run_ppl_at_rank(model, tokenizer, modes_checkpoint, layer_indices,
                    thr_key, input_ids, max_length, stride, device):
    """
    Register Koopman hooks at the rank corresponding to EV threshold thr_key,
    compute PPL, then remove hooks.
    """
    hooks  = []
    kh_map = {}
    stats  = modes_checkpoint["stats"]

    for idx in layer_indices:
        m         = modes_checkpoint["modes"].get(idx)
        if m is None:
            continue

        layer_stat = stats.get(str(idx), stats.get(idx, {}))
        scan       = layer_stat.get("rank_scan", {})
        if thr_key not in scan:
            continue

        r = scan[thr_key]["r_h"]
        r = min(r, m["V"].shape[1])

        V_r       = m["V"][:, :r].to(device).float()
        M_r       = m["M"][:, :r].to(device).float()
        mu_h      = m["mu_h"].to(device).float()
        mu_offset = m["mu_offset"].to(device).float()

        kh  = KoopmanHook(V_r, M_r, mu_h=mu_h, mu_offset=mu_offset, replace=True)
        mlp = model.model.layers[idx].mlp
        handle = mlp.down_proj.register_forward_hook(kh)
        hooks.append(handle)
        kh_map[idx] = kh

    ppl = compute_ppl(model, input_ids, max_length, stride, device)

    for h in hooks:
        h.remove()

    return ppl


# ──────────────────────────────────────────────
#  Main
# ──────────────────────────────────────────────

def main():
    args = parse_args()

    print(f"\n{'='*72}")
    print(f"  Koopman Validation — BonfyreFPQ / Ember")
    print(f"  Model      : {args.model}")
    print(f"  Modes file : {args.modes_file}")
    print(f"  Device     : {args.device}")
    print(f"  PPL tokens : {args.n_tokens}")
    print(f"{'='*72}\n")

    # ── Load modes checkpoint
    print("Loading modes checkpoint...")
    ckpt = torch.load(args.modes_file, map_location="cpu", weights_only=False)
    modes_map  = ckpt["modes"]          # dict[int_idx → {V, M, mu_h, ...}]
    stats_map  = ckpt["stats"]          # dict[str_idx → {rank_scan, ...}]
    ckpt["stats"] = {
        (int(k) if k.isdigit() else k): v for k, v in stats_map.items()
    }

    # Infer available layers
    all_layer_indices = sorted(modes_map.keys())
    layer_indices = (
        [int(x) for x in args.layers.split(",")]
        if args.layers else all_layer_indices
    )
    layer_indices = [idx for idx in layer_indices if idx in modes_map]

    # Available EV thresholds
    sample_stats = ckpt["stats"].get(layer_indices[0], {})
    thr_keys     = list(sample_stats.get("rank_scan", {}).keys())
    print(f"  Layers     : {layer_indices}")
    print(f"  EV thresholds in file : {thr_keys}\n")

    # ── Load model
    print("Loading model...")
    tokenizer = AutoTokenizer.from_pretrained(args.model)
    model = AutoModelForCausalLM.from_pretrained(
        args.model, torch_dtype=torch.float32, low_cpu_mem_usage=True
    ).to(args.device)
    model.eval()
    max_length = model.config.max_position_embeddings or 2048
    print(f"  max_position_embeddings = {max_length}")

    # ── Per-layer cosine scan
    layer_cosines = {}
    if not args.skip_cosine and thr_keys:
        print(f"\n── Per-layer cosine scan ({args.n_cosine_samples} tokens) ──")
        layer_cosines = cosine_scan(
            model, tokenizer, ckpt, layer_indices,
            args.n_cosine_samples, args.device
        )

        for idx in layer_indices:
            if idx not in layer_cosines:
                continue
            cd = layer_cosines[idx]
            print(f"\n  Layer {idx}:")
            print(f"  {'EV thresh':>10} | {'r_h':>6} | {'cosine':>10}")
            print(f"  {'-'*10}-+-{'-'*6}-+-{'-'*10}")
            for thr_key, (cos, r) in cd.items():
                print(f"  {thr_key:>10} | {r:>6} | {cos:>10.6f}")

    # ── PPL scan
    if args.skip_ppl:
        print("\nPPL skipped (--skip-ppl).")
        return

    print(f"\n── Perplexity scan ({args.n_tokens} tokens, stride={args.stride}) ──")
    print("  Tokenising corpus...")
    input_ids = get_ppl_text(tokenizer, args.n_tokens)
    print(f"  Token count : {input_ids.shape[0]}")

    # Baseline PPL (no hooks)
    print("\n  Running baseline PPL...")
    t0 = time.time()
    ppl_baseline = compute_ppl(model, input_ids, max_length, args.stride, args.device)
    t_base = time.time() - t0
    print(f"  Baseline PPL = {ppl_baseline:.4f}  ({t_base:.1f}s)")

    # Koopman PPL at each threshold (all layers simultaneously)
    ppl_results = {}
    for thr_key in thr_keys:
        print(f"\n  Running Koopman PPL @ EV={thr_key}...")
        t0   = time.time()
        ppl_k = run_ppl_at_rank(
            model, tokenizer, ckpt, layer_indices,
            thr_key, input_ids, max_length, args.stride, args.device
        )
        elapsed = time.time() - t0
        ppl_results[thr_key] = ppl_k

        r_sample = sample_stats.get("rank_scan", {}).get(thr_key, {}).get("r_h", "?")
        ambs     = sample_stats.get("rank_scan", {}).get(thr_key, {}).get("AMB_kbits", None)
        print(f"  r_h={r_sample}  PPL={ppl_k:.4f}  (Δ={ppl_k-ppl_baseline:+.4f})  {elapsed:.1f}s")

    # ── Summary table
    FPQ_V8_PPL = 12.07
    FPQ_V8_AMB = 226.6  # Mbits
    FPQX_PPL   = 12.00
    FPQX_AMB   = 28.0

    print(f"\n{'='*72}")
    print(f"  KOOPMAN VALIDATION SUMMARY")
    print(f"  Model  : {ckpt['model']}")
    print(f"  Layers : all {len(layer_indices)} ({min(layer_indices)}–{max(layer_indices)})")
    print(f"{'='*72}")
    print(f"  {'Config':>20} | {'r_h':>6} | {'PPL':>8} | "
          f"{'ΔPPL':>8} | {'AMB kbits':>10} | {'Gain vs FPQ':>12}")
    print(f"  {'-'*20}-+-{'-'*6}-+-{'-'*8}-+-{'-'*8}-+-{'-'*10}-+-{'-'*12}")

    # Reference rows
    print(f"  {'Baseline (fp32)':>20} | {'—':>6} | {ppl_baseline:>8.4f} | "
          f"{'—':>8} | {'—':>10} | {'—':>12}")
    print(f"  {'FPQ v8 (3-bit)':>20} | {'—':>6} | {FPQ_V8_PPL:>8.4f} | "
          f"{FPQ_V8_PPL-ppl_baseline:>+8.4f} | {FPQ_V8_AMB*1e3:>10.0f} | {'1×':>12}")
    print(f"  {'FPQx SLI':>20} | {'—':>6} | {FPQX_PPL:>8.4f} | "
          f"{FPQX_PPL-ppl_baseline:>+8.4f} | {FPQX_AMB*1e3:>10.0f} | {'8×':>12}")

    for thr_key, ppl_k in ppl_results.items():
        rs   = sample_stats.get("rank_scan", {}).get(thr_key, {})
        r_h  = rs.get("r_h", "?")
        amb_kb = rs.get("AMB_kbits", None)
        if amb_kb:
            gain = round(FPQ_V8_AMB * 1e3 / amb_kb, 0)
            gain_str = f"{int(gain):,}×"
            amb_str  = f"{amb_kb:>10.2f}"
        else:
            gain_str = "?"
            amb_str  = "?"
        label = f"Koopman EV={thr_key}"
        print(f"  {label:>20} | {str(r_h):>6} | {ppl_k:>8.4f} | "
              f"{ppl_k-ppl_baseline:>+8.4f} | {amb_str} | {gain_str:>12}")

    print(f"{'='*72}\n")

    # ── Per-layer cosine summary
    if layer_cosines:
        print(f"  PER-LAYER COSINE SUMMARY (all thresholds, mean across layers)\n")
        per_thr = {}
        for idx, cd in layer_cosines.items():
            for thr_key, (cos, r) in cd.items():
                per_thr.setdefault(thr_key, []).append(cos)
        print(f"  {'EV thresh':>10} | {'mean cos':>10} | {'min cos':>10}")
        print(f"  {'-'*10}-+-{'-'*10}-+-{'-'*10}")
        for thr_key in thr_keys:
            vals = per_thr.get(thr_key, [])
            if vals:
                print(f"  {thr_key:>10} | {sum(vals)/len(vals):>10.6f} | {min(vals):>10.6f}")
        print()

    print("Done.")


if __name__ == "__main__":
    main()
