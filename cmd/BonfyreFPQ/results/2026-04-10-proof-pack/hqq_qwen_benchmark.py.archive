#!/usr/bin/env python3
import argparse
import os
import sys

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from datasets import load_dataset
from hqq.models.hf.base import AutoHQQHFModel
from hqq.core.quantize import BaseQuantizeConfig

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from perplexity_benchmark import compute_perplexity


def main():
    parser = argparse.ArgumentParser(description="HQQ Qwen benchmark on WikiText-2 slice")
    parser.add_argument("--model", type=str, default="Qwen/Qwen2.5-0.5B")
    parser.add_argument("--bits", type=int, default=3)
    parser.add_argument("--group-size", type=int, default=64)
    parser.add_argument("--axis", type=int, default=1)
    parser.add_argument("--max-length", type=int, default=512)
    parser.add_argument("--stride", type=int, default=256)
    parser.add_argument("--max-tokens", type=int, default=1000)
    parser.add_argument("--device", type=str, default="cpu")
    args = parser.parse_args()

    print("═══════════════════════════════════════════════════════")
    print(" HQQ PERPLEXITY BENCHMARK")
    print(f" Model:      {args.model}")
    print(f" Bits:       {args.bits}")
    print(f" Group size: {args.group_size}")
    print(f" Axis:       {args.axis}")
    print(f" Device:     {args.device}")
    print("═══════════════════════════════════════════════════════")

    print("\n[1/5] Loading model and tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained(args.model)
    model = AutoModelForCausalLM.from_pretrained(args.model, torch_dtype=torch.float32)
    model.eval()
    model = model.to(args.device)

    total_params = sum(p.numel() for p in model.parameters())
    print(f"  Total params: {total_params:,}")

    print("\n[2/5] Loading WikiText-2 test set...")
    dataset = load_dataset("wikitext", "wikitext-2-raw-v1", split="test")
    text = "\n\n".join(dataset["text"])
    text = "\n".join([line for line in text.split("\n") if line.strip()])
    if args.max_tokens > 0:
        text = text[: args.max_tokens * 4]
    print(f"  Dataset chars: {len(text):,}")

    print("\n[3/5] Computing BASELINE perplexity...")
    ppl_base, n_tokens = compute_perplexity(
        model,
        tokenizer,
        text,
        max_length=args.max_length,
        stride=args.stride,
        device=args.device,
    )
    print(f"  Baseline PPL: {ppl_base:.4f}  ({n_tokens:,} tokens)")

    print(f"\n[4/5] Applying HQQ@{args.bits} quantization...")
    qc = BaseQuantizeConfig(nbits=args.bits, group_size=args.group_size, axis=args.axis)
    AutoHQQHFModel.quantize_model(
        model,
        quant_config=qc,
        compute_dtype=torch.float32,
        device=args.device,
    )
    q_proj_type = type(model.model.layers[0].self_attn.q_proj).__name__
    print(f"  Layer type after quantization: {q_proj_type}")

    print(f"\n[5/5] Computing HQQ@{args.bits} perplexity...")
    ppl_hqq, n_tokens_hqq = compute_perplexity(
        model,
        tokenizer,
        text,
        max_length=args.max_length,
        stride=args.stride,
        device=args.device,
    )
    print(f"  HQQ@{args.bits} PPL: {ppl_hqq:.4f}  ({n_tokens_hqq:,} tokens)")

    print("\n═══════════════════════════════════════════════════════")
    print(" HQQ BENCHMARK RESULTS")
    print("═══════════════════════════════════════════════════════")
    print(f"  Baseline PPL:          {ppl_base:.4f}")
    print(f"  HQQ@{args.bits} PPL:          {ppl_hqq:.4f}")
    ppl_increase = ((ppl_hqq - ppl_base) / ppl_base) * 100.0
    print(f"  PPL degradation:       {ppl_increase:+.2f}%")
    print(f"  Quantized layer type:  {q_proj_type}")
    print("═══════════════════════════════════════════════════════")


if __name__ == "__main__":
    main()
