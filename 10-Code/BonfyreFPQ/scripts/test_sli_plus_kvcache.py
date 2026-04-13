#!/usr/bin/env python3
"""
test_sli_plus_kvcache.py — Combined SLI weight compression + KV cache compression benchmark.

Runs four forward passes on TinyLlama-1.1B:
  1. Baseline (fp32 weights, no KV compression)
  2. SLI only  (bonfyre .fpq weights, no KV compression)
  3. KV cache only (fp32 weights, KV compressed at --bits)
  4. SLI + KV cache (both active simultaneously)

Each mode loads the model independently and releases it before the next pass
to avoid OOM on machines with limited RAM (no concurrent deepcopy).

Reports cosine similarity, MSE, and perplexity-equivalent divergence for each.

Usage:
  python3 scripts/test_sli_plus_kvcache.py \\
      --model TinyLlama/TinyLlama-1.1B-Chat-v1.0 \\
      --fpq   models/tinyllama-v12/model.fpq \\
      --bits  4 \\
      --device cpu   # or mps / cuda \\
      --out   results/sli_kvcache.json
"""
import argparse
import gc
import json
import math
import sys
import time
from pathlib import Path

import torch
import torch.nn.functional as F

# ─── locate fpq_bridge ───────────────────────────────────────────────────────
_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here))
from fpq_bridge import FPQModel, patch_model, patch_kv_cache, remove_kv_cache_hooks


# ─── helpers ─────────────────────────────────────────────────────────────────

def load_model(model_id: str, device: str, dtype: torch.dtype):
    from transformers import AutoModelForCausalLM, AutoTokenizer
    print(f"Loading {model_id} …")
    tok = AutoTokenizer.from_pretrained(model_id)
    model = AutoModelForCausalLM.from_pretrained(
        model_id, torch_dtype=dtype, device_map=device, low_cpu_mem_usage=True
    )
    model.eval()
    return model, tok


def free_model(model):
    """Delete model and flush allocator to release RAM before next pass."""
    del model
    gc.collect()
    if hasattr(torch, 'cuda'):
        torch.cuda.empty_cache()
    if hasattr(torch, 'mps') and hasattr(torch.mps, 'empty_cache'):
        torch.mps.empty_cache()


# ─── main ────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--model",  default="TinyLlama/TinyLlama-1.1B-Chat-v1.0")
    p.add_argument("--fpq",    required=True)
    p.add_argument("--bits",   type=int, default=4, help="KV cache bits (3/4/5)")
    p.add_argument("--device", default="cpu")
    p.add_argument("--seq",    type=int, default=64)
    p.add_argument("--seed",   type=int, default=42)
    p.add_argument("--out",    default="results/sli_kvcache.json")
    args = p.parse_args()

    device = args.device
    dtype  = torch.float32

    print("\n" + "="*70)
    print("SLI + KV Cache Combined Benchmark")
    print("="*70)
    print(f"  Model:  {args.model}")
    print(f"  FPQ:    {args.fpq}")
    print(f"  KV bits: {args.bits}  |  seq_len: {args.seq}  |  device: {device}")
    print("  (sequential mode — one model in RAM at a time)\n")

    # Each pass: load fresh → run → free.  Never hold >1 model at once.

    # ── 1. Baseline ──────────────────────────────────────────────────────────
    print("[1/4] Baseline forward pass (fp32 weights, no KV compression)…")
    model_base, tok = load_model(args.model, device, dtype)
    torch.manual_seed(args.seed)
    input_ids = torch.randint(100, model_base.config.vocab_size, (1, args.seq), device=device)

    t0 = time.time()
    with torch.no_grad():
        out = model_base(input_ids=input_ids, labels=input_ids)
    logits_base = out.logits.detach().cpu()
    loss_base   = out.loss.detach().cpu()
    t_base = time.time() - t0
    ppl_base = math.exp(loss_base.item())
    print(f"      loss={loss_base.item():.4f}  ppl={ppl_base:.2f}  ({t_base:.1f}s)")
    free_model(model_base)

    # ── 2. SLI only ──────────────────────────────────────────────────────────
    print("\n[2/4] SLI only (FPQ weights, no KV compression)…")
    model_sli, _ = load_model(args.model, device, dtype)
    fpq = FPQModel(args.fpq)
    print(f"      FPQ: {fpq.info()['n_sli']} SLI + {fpq.info()['n_passthrough']} passthrough")
    patched = patch_model(model_sli, fpq, prefix="model", verbose=False)
    print(f"      Patched {len(patched)} layers via SLI")

    t0 = time.time()
    with torch.no_grad():
        out_sli = model_sli(input_ids=input_ids, labels=input_ids)
    logits_sli = out_sli.logits.detach().cpu()
    loss_sli   = out_sli.loss.detach().cpu()
    t_sli = time.time() - t0
    ppl_sli = math.exp(loss_sli.item())
    cos_sli = (logits_base.reshape(-1) @ logits_sli.reshape(-1) /
               (logits_base.reshape(-1).norm() * logits_sli.reshape(-1).norm() + 1e-12)).item()
    mse_sli = (logits_base - logits_sli).pow(2).mean().item()
    print(f"      loss={loss_sli.item():.4f}  ppl={ppl_sli:.2f}  cos={cos_sli:.8f}  mse={mse_sli:.2e}  ({t_sli:.1f}s)")
    fpq.close()
    free_model(model_sli)

    # ── 3. KV cache only ─────────────────────────────────────────────────────
    print(f"\n[3/4] KV cache only ({args.bits}-bit, fp32 weights)…")
    model_kv, _ = load_model(args.model, device, dtype)
    kv_info = patch_kv_cache(model_kv, bits=args.bits, verbose=False)
    print(f"      Hooked {len(kv_info['layers'])} K/V projection layers")

    t0 = time.time()
    with torch.no_grad():
        out_kv = model_kv(input_ids=input_ids, labels=input_ids)
    logits_kv = out_kv.logits.detach().cpu()
    loss_kv   = out_kv.loss.detach().cpu()
    t_kv = time.time() - t0
    ppl_kv = math.exp(loss_kv.item())
    cos_kv = (logits_base.reshape(-1) @ logits_kv.reshape(-1) /
              (logits_base.reshape(-1).norm() * logits_kv.reshape(-1).norm() + 1e-12)).item()
    mse_kv = (logits_base - logits_kv).pow(2).mean().item()
    print(f"      loss={loss_kv.item():.4f}  ppl={ppl_kv:.2f}  cos={cos_kv:.8f}  mse={mse_kv:.2e}  ({t_kv:.1f}s)")
    remove_kv_cache_hooks(kv_info)
    free_model(model_kv)

    # ── 4. SLI + KV cache ────────────────────────────────────────────────────
    print(f"\n[4/4] SLI + KV cache ({args.bits}-bit) combined…")
    model_both, _ = load_model(args.model, device, dtype)
    fpq2   = FPQModel(args.fpq)
    patched2 = patch_model(model_both, fpq2, prefix="model", verbose=False)
    kv_info2 = patch_kv_cache(model_both, bits=args.bits, verbose=False)
    print(f"      {len(patched2)} SLI layers + {len(kv_info2['layers'])} KV hooks")

    t0 = time.time()
    with torch.no_grad():
        out_both = model_both(input_ids=input_ids, labels=input_ids)
    logits_both = out_both.logits.detach().cpu()
    loss_both   = out_both.loss.detach().cpu()
    t_both = time.time() - t0
    ppl_both = math.exp(loss_both.item())
    cos_both = (logits_base.reshape(-1) @ logits_both.reshape(-1) /
                (logits_base.reshape(-1).norm() * logits_both.reshape(-1).norm() + 1e-12)).item()
    mse_both = (logits_base - logits_both).pow(2).mean().item()
    print(f"      loss={loss_both.item():.4f}  ppl={ppl_both:.2f}  cos={cos_both:.8f}  mse={mse_both:.2e}  ({t_both:.1f}s)")
    fpq2.close()
    remove_kv_cache_hooks(kv_info2)
    free_model(model_both)

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "─"*70)
    print(f"{'Mode':<22} {'PPL':>8} {'ΔPPL':>8} {'Cos':>12} {'MSE':>10}")
    print("─"*70)
    print(f"{'Baseline (fp32)':<22} {ppl_base:>8.2f} {'—':>8} {'1.00000000':>12} {'0.00e+00':>10}")
    def ppl_d(p): return f"{p - ppl_base:+.2f}"
    print(f"{'SLI only':<22} {ppl_sli:>8.2f} {ppl_d(ppl_sli):>8} {cos_sli:>12.8f} {mse_sli:>10.2e}")
    print(f"{'KV '+str(args.bits)+'-bit only':<22} {ppl_kv:>8.2f} {ppl_d(ppl_kv):>8} {cos_kv:>12.8f} {mse_kv:>10.2e}")
    print(f"{'SLI + KV '+str(args.bits)+'-bit':<22} {ppl_both:>8.2f} {ppl_d(ppl_both):>8} {cos_both:>12.8f} {mse_both:>10.2e}")
    print("─"*70)

    delta_sli   = ppl_sli  - ppl_base
    delta_kv    = ppl_kv   - ppl_base
    delta_both  = ppl_both - ppl_base
    interaction = delta_both - (delta_sli + delta_kv)
    print(f"\nInteraction effect: {interaction:+.3f} PPL "
          f"({'additive ✓' if abs(interaction) < 0.5 else 'super-additive ⚠' if interaction > 0 else 'sub-additive ✓'})")

    # ── Save results ─────────────────────────────────────────────────────────
    results = {
        "model":   args.model,
        "fpq":     str(args.fpq),
        "kv_bits": args.bits,
        "seq_len": args.seq,
        "seed":    args.seed,
        "baseline": {"ppl": ppl_base, "loss": loss_base.item()},
        "sli_only": {
            "ppl": ppl_sli, "loss": loss_sli.item(),
            "cos": cos_sli, "mse": mse_sli,
            "patched_layers": len(patched),
        },
        "kv_only": {
            "ppl": ppl_kv, "loss": loss_kv.item(),
            "cos": cos_kv, "mse": mse_kv,
            "hooked_layers": len(kv_info["layers"]),
        },
        "sli_plus_kv": {
            "ppl": ppl_both, "loss": loss_both.item(),
            "cos": cos_both, "mse": mse_both,
        },
        "interaction_ppl_delta": interaction,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\nResults saved → {out_path}")


if __name__ == "__main__":
    main()
