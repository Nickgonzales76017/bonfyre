#!/usr/bin/env python3
"""
FPQ Perplexity Benchmark — The Victory Lap

Loads Gemma 2B-it (or fallback model), applies FPQ COORD roundtrip
to all weight tensors, and measures perplexity before/after.

Question: does 0.9955 cosine per-tensor → zero-loss reasoning?

Usage:
    python3 scripts/perplexity_benchmark.py [--model MODEL] [--bits 3|4] [--max-length 512]
"""
import argparse
import sys
import time
import math
import numpy as np
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

# ══════════════════════════════════════════════════════════
# FPQ COORD v4 Roundtrip (Pure NumPy)
# ══════════════════════════════════════════════════════════

BLOCK_DIM = 256

# Lloyd-Max optimal centroids for N(0,1) — matching fpq_codec.c exactly
LLOYD = {
    2: {
        "bounds": np.array([-0.9816, 0.0, 0.9816]),
        "centers": np.array([-1.5104, -0.4528, 0.4528, 1.5104]),
    },
    3: {
        "bounds": np.array([-1.7479, -1.0500, -0.5006, 0.0, 0.5006, 1.0500, 1.7479]),
        "centers": np.array([-2.1520, -1.3440, -0.7560, -0.2451,
                              0.2451,  0.7560,  1.3440,  2.1520]),
    },
    4: {
        "bounds": np.array([-2.4008, -1.8440, -1.4371, -1.0993, -0.7977,
                            -0.5157, -0.2451, 0.0, 0.2451, 0.5157,
                             0.7977,  1.0993,  1.4371,  1.8440,  2.4008]),
        "centers": np.array([-2.7326, -2.0690, -1.6180, -1.2562, -0.9423,
                             -0.6568, -0.3881, -0.1284,  0.1284,  0.3881,
                              0.6568,  0.9423,  1.2562,  1.6180,  2.0690,
                              2.7326]),
    },
}


def fwht_inplace(x):
    """Fast Walsh-Hadamard Transform (in-place, unnormalized)."""
    n = len(x)
    h = 1
    while h < n:
        for i in range(0, n, h * 2):
            for j in range(i, i + h):
                a = x[j]
                b = x[j + h]
                x[j] = a + b
                x[j + h] = a - b
        h *= 2
    return x


def fwht_batch(blocks):
    """Vectorized FWHT on (n_blocks, block_dim) array."""
    n = blocks.shape[1]
    h = 1
    while h < n:
        even = np.arange(0, n, 2 * h)
        for j_off in range(h):
            j = even + j_off
            a = blocks[:, j].copy()
            b = blocks[:, j + h].copy()
            blocks[:, j] = a + b
            blocks[:, j + h] = a - b
        h *= 2
    return blocks


def random_signs(n, seed):
    """Generate deterministic ±1 signs from seed."""
    rng = np.random.RandomState(seed & 0xFFFFFFFF)
    return rng.choice([-1.0, 1.0], size=n).astype(np.float32)


def fpq_roundtrip_tensor(weights, name, bits=3):
    """
    Apply FPQ COORD roundtrip to a 2D weight tensor.
    Returns the reconstructed weights with the same shape.
    """
    orig_shape = weights.shape
    flat = weights.flatten().astype(np.float32)
    total = len(flat)

    if total < BLOCK_DIM:
        return weights  # skip tiny tensors

    # Derive haar seed from name (matching C code)
    seed = 0x12345678
    for ch in name:
        seed = (seed * 31 + ord(ch)) & 0xFFFFFFFFFFFFFFFF
    seed &= 0xFFFFFFFFFFFFFFFF

    # Pad to multiple of BLOCK_DIM
    n_blocks = (total + BLOCK_DIM - 1) // BLOCK_DIM
    padded_len = n_blocks * BLOCK_DIM
    padded = np.zeros(padded_len, dtype=np.float32)
    padded[:total] = flat

    # Reshape into blocks
    blocks = padded.reshape(n_blocks, BLOCK_DIM)

    # Apply random signs per block
    sign_arrays = []
    for b in range(n_blocks):
        signs = random_signs(BLOCK_DIM, seed ^ b)
        sign_arrays.append(signs)
        blocks[b] *= signs

    # FWHT each block
    blocks = fwht_batch(blocks)

    # Per-block RMS normalization
    rms = np.sqrt(np.mean(blocks ** 2, axis=1))
    rms = np.maximum(rms, 1e-10)
    normalized = blocks / rms[:, np.newaxis]

    # Lloyd-Max quantization
    bounds = LLOYD[bits]["bounds"]
    centers = LLOYD[bits]["centers"]
    indices = np.searchsorted(bounds, normalized)  # quantize
    dequantized = centers[indices]  # dequantize

    # De-normalize
    blocks_recon = dequantized * rms[:, np.newaxis]

    # Inverse FWHT (same transform, then scale by 1/N)
    blocks_recon = fwht_batch(blocks_recon) / BLOCK_DIM

    # Undo random signs
    for b in range(n_blocks):
        blocks_recon[b] *= sign_arrays[b]

    # Flatten and trim
    decoded = blocks_recon.flatten()[:total]

    # Per-tensor cosine similarity
    dot = np.sum(flat * decoded)
    norm_a = np.sqrt(np.sum(flat ** 2))
    norm_b = np.sqrt(np.sum(decoded ** 2))
    cosine = dot / (norm_a * norm_b + 1e-10)

    return decoded.reshape(orig_shape), cosine


def evaluate_perplexity(model, tokenizer, text, max_length=512, stride=256,
                         device="cpu"):
    """Compute perplexity on a text string using sliding window."""
    model.eval()
    encodings = tokenizer(text, return_tensors="pt")
    input_ids = encodings.input_ids.to(device)
    seq_len = input_ids.size(1)

    nlls = []
    n_tokens = 0

    with torch.no_grad():
        for begin_loc in range(0, seq_len - 1, stride):
            end_loc = min(begin_loc + max_length, seq_len)
            target_len = end_loc - begin_loc - 1

            input_chunk = input_ids[:, begin_loc:end_loc]

            outputs = model(input_chunk, labels=input_chunk)
            # Only count the non-overlapping tokens
            shift = max(0, max_length - stride) if begin_loc > 0 else 0
            nll = outputs.loss.item() * target_len  # CE loss * n_tokens
            nlls.append(nll)
            n_tokens += target_len

            if end_loc >= seq_len:
                break

    avg_nll = sum(nlls) / n_tokens
    perplexity = math.exp(avg_nll)
    return perplexity, n_tokens


# ══════════════════════════════════════════════════════════
# Evaluation Corpus
# ══════════════════════════════════════════════════════════

EVAL_TEXT = """The transformer architecture has fundamentally changed natural language processing. 
Introduced in the paper "Attention Is All You Need" by Vaswani et al., the transformer relies 
entirely on self-attention mechanisms, dispensing with recurrence and convolution entirely. 
The key innovation is the scaled dot-product attention, which computes compatibility scores 
between all pairs of positions in a sequence simultaneously.

In a transformer, the input sequence is first embedded into continuous representations, then 
augmented with positional encodings. The self-attention mechanism allows each position to attend 
to all other positions, weighted by learned compatibility functions. This is computed as:

Attention(Q, K, V) = softmax(QK^T / sqrt(d_k)) V

where Q, K, and V are linear projections of the input. Multi-head attention extends this by 
running multiple attention operations in parallel, each with different learned projections.

The feed-forward network in each transformer layer consists of two linear transformations with 
a nonlinearity in between. Layer normalization and residual connections are applied around each 
sub-layer, facilitating training of deep networks.

Modern large language models like GPT, PaLM, LLaMA, and Gemma scale the transformer to billions 
of parameters, trained on trillions of tokens of text data. These models demonstrate remarkable 
capabilities in reasoning, summarization, code generation, and knowledge retrieval, though they 
remain fundamentally statistical pattern matchers operating on token distributions.

The quantization of neural network weights is a critical technique for deploying large models 
on resource-constrained hardware. Weight quantization reduces the precision of stored weights 
from 32-bit or 16-bit floating point to lower bit-widths, such as 8-bit, 4-bit, or even 
2-bit representations. The key challenge is preserving model quality while achieving significant 
compression ratios.

Post-training quantization methods operate on pre-trained weights without retraining. The 
simplest approach is round-to-nearest (RTN), which clips and rounds each weight to the nearest 
quantization level. More sophisticated methods like GPTQ and AWQ consider the Hessian matrix 
or activation-aware scaling to minimize the impact of quantization error on model outputs.

The Walsh-Hadamard Transform provides a theoretically elegant basis for quantization. By 
transforming weight vectors into the Walsh-Hadamard domain, the energy is distributed more 
uniformly across coordinates, making each coordinate approximately Gaussian distributed. This 
Gaussianization effect enables optimal scalar quantization using Lloyd-Max quantizers, achieving 
near-theoretical-minimum distortion for a given bit budget.

Functional Polar Quantization represents a novel approach that views weight compression through 
the lens of functional programming and information theory. Rather than storing weights directly, 
FPQ stores seed programs that reconstruct weight vectors via polar decompositions, spectral 
transforms, and Lloyd-Max quantization. This paradigm shift enables compression ratios that 
approach the rate-distortion bound while maintaining cosine similarity above 0.99 at 4 bits 
per weight."""


def load_eval_corpus(tokenizer, max_tokens=4096):
    """Load evaluation text. Try WikiText-2, fall back to built-in."""
    try:
        from datasets import load_dataset
        dataset = load_dataset("wikitext", "wikitext-2-raw-v1", split="test")
        text = "\n\n".join([t for t in dataset["text"] if len(t.strip()) > 50])
        tokens = tokenizer(text, return_tensors="pt")
        if tokens.input_ids.size(1) > max_tokens:
            # Truncate for speed
            text = tokenizer.decode(tokens.input_ids[0, :max_tokens])
        print(f"  Loaded WikiText-2 test set ({tokens.input_ids.size(1)} tokens)")
        return text
    except Exception as e:
        print(f"  WikiText-2 not available ({e}), using built-in corpus")
        return EVAL_TEXT


def main():
    parser = argparse.ArgumentParser(description="FPQ Perplexity Benchmark")
    parser.add_argument("--model", default="google/gemma-2-2b-it",
                        help="HuggingFace model name")
    parser.add_argument("--bits", type=int, default=4, choices=[2, 3, 4],
                        help="FPQ COORD bit depth (default: 4)")
    parser.add_argument("--max-length", type=int, default=512,
                        help="Max context length for perplexity eval")
    parser.add_argument("--skip-2d", action="store_true",
                        help="Only quantize 2D weight tensors (skip embeddings)")
    parser.add_argument("--device", default=None,
                        help="Device (auto-detected: mps/cuda/cpu)")
    args = parser.parse_args()

    # Device selection
    if args.device:
        device = args.device
    elif torch.backends.mps.is_available():
        device = "mps"
    elif torch.cuda.is_available():
        device = "cuda"
    else:
        device = "cpu"

    print("=" * 60)
    print(" FPQ PERPLEXITY BENCHMARK — The Victory Lap")
    print("=" * 60)
    print(f"  Model:    {args.model}")
    print(f"  Bits:     {args.bits}")
    print(f"  Device:   {device}")
    print(f"  Context:  {args.max_length}")
    print()

    # ── Phase 1: Load Model ──
    print("Phase 1: Loading model...")
    t0 = time.time()
    try:
        tokenizer = AutoTokenizer.from_pretrained(args.model)
        model = AutoModelForCausalLM.from_pretrained(
            args.model,
            torch_dtype=torch.float32,  # f32 for accurate roundtrip
            device_map=device if device != "mps" else None,
            low_cpu_mem_usage=True,
        )
        if device == "mps":
            model = model.to(device)
    except Exception as e:
        print(f"  Failed to load {args.model}: {e}")
        print("  Trying fallback model: Qwen/Qwen2.5-0.5B...")
        args.model = "Qwen/Qwen2.5-0.5B"
        tokenizer = AutoTokenizer.from_pretrained(args.model)
        model = AutoModelForCausalLM.from_pretrained(
            args.model,
            torch_dtype=torch.float32,
            device_map=device if device != "mps" else None,
            low_cpu_mem_usage=True,
        )
        if device == "mps":
            model = model.to(device)

    t_load = time.time() - t0
    n_params = sum(p.numel() for p in model.parameters())
    print(f"  Loaded in {t_load:.1f}s — {n_params/1e6:.0f}M parameters")

    # ── Phase 2: Load Eval Corpus ──
    print("\nPhase 2: Loading evaluation corpus...")
    eval_text = load_eval_corpus(tokenizer, max_tokens=2048)

    # ── Phase 3: Baseline Perplexity ──
    print("\nPhase 3: Baseline perplexity...")
    t0 = time.time()
    ppl_baseline, n_tokens = evaluate_perplexity(
        model, tokenizer, eval_text,
        max_length=args.max_length, stride=args.max_length // 2,
        device=device
    )
    t_eval = time.time() - t0
    print(f"  Baseline PPL = {ppl_baseline:.4f} ({n_tokens} tokens, {t_eval:.1f}s)")

    # ── Phase 4: Apply FPQ Roundtrip ──
    print(f"\nPhase 4: Applying FPQ COORD@{args.bits} roundtrip to all weights...")
    t0 = time.time()

    cosines = []
    n_modified = 0
    n_skipped = 0
    total_elements = 0

    # Move model to CPU for weight modification
    model = model.to("cpu")

    for name, param in model.named_parameters():
        w = param.data.numpy()

        # Skip 1D tensors (norms, biases) — they're too small and critical
        if w.ndim < 2 or w.size < BLOCK_DIM:
            n_skipped += 1
            continue

        # Apply FPQ roundtrip
        w_recon, cosine = fpq_roundtrip_tensor(w, name, bits=args.bits)
        cosines.append(cosine)
        total_elements += w.size
        n_modified += 1

        # Replace weight in-place
        param.data = torch.from_numpy(w_recon.astype(np.float32))

        if n_modified <= 5 or n_modified % 20 == 0:
            print(f"  [{n_modified}] {name}: {w.shape} → cos={cosine:.6f}")

    t_roundtrip = time.time() - t0

    avg_cosine = np.mean(cosines)
    min_cosine = np.min(cosines)
    print(f"\n  Modified {n_modified} tensors, skipped {n_skipped}")
    print(f"  Average cosine: {avg_cosine:.6f}")
    print(f"  Worst cosine:   {min_cosine:.6f}")
    print(f"  Total elements: {total_elements/1e6:.1f}M ({t_roundtrip:.1f}s)")

    # ── Phase 5: Modified Perplexity ──
    print(f"\nPhase 5: Modified perplexity (COORD@{args.bits})...")
    model = model.to(device)
    t0 = time.time()
    ppl_modified, _ = evaluate_perplexity(
        model, tokenizer, eval_text,
        max_length=args.max_length, stride=args.max_length // 2,
        device=device
    )
    t_eval = time.time() - t0
    print(f"  Modified PPL = {ppl_modified:.4f} ({t_eval:.1f}s)")

    # ── Phase 6: Victory Report ──
    delta_ppl = ppl_modified - ppl_baseline
    pct_change = (delta_ppl / ppl_baseline) * 100

    print()
    print("=" * 60)
    print(" FPQ PERPLEXITY BENCHMARK — RESULTS")
    print("=" * 60)
    print(f"  Model:           {args.model}")
    print(f"  Parameters:      {n_params/1e6:.0f}M")
    print(f"  FPQ Mode:        COORD@{args.bits} + Lloyd-Max")
    print(f"  Tensors Modified:{n_modified}")
    print(f"  Avg Cosine:      {avg_cosine:.6f}")
    print(f"  Worst Cosine:    {min_cosine:.6f}")
    print("-" * 60)
    print(f"  Baseline PPL:    {ppl_baseline:.4f}")
    print(f"  Modified PPL:    {ppl_modified:.4f}")
    print(f"  Δ PPL:           {delta_ppl:+.4f} ({pct_change:+.2f}%)")
    print("-" * 60)

    if abs(pct_change) < 1.0:
        print("  ✓ VERDICT: ZERO-LOSS REASONING — PPL change < 1%")
        print(f"    FPQ COORD@{args.bits} preserves model quality!")
    elif abs(pct_change) < 5.0:
        print(f"  ~ VERDICT: Near-lossless — PPL change {pct_change:+.2f}%")
    else:
        print(f"  ✗ VERDICT: Measurable degradation — PPL change {pct_change:+.2f}%")

    print("=" * 60)

    # Save results
    results_file = "fpq_perplexity_results.txt"
    with open(results_file, "w") as f:
        f.write(f"Model: {args.model}\n")
        f.write(f"Bits: {args.bits}\n")
        f.write(f"Baseline PPL: {ppl_baseline:.6f}\n")
        f.write(f"Modified PPL: {ppl_modified:.6f}\n")
        f.write(f"Delta PPL: {delta_ppl:+.6f} ({pct_change:+.4f}%)\n")
        f.write(f"Avg Cosine: {avg_cosine:.6f}\n")
        f.write(f"Worst Cosine: {min_cosine:.6f}\n")
        f.write(f"Tensors Modified: {n_modified}\n")
        f.write(f"Total Elements: {total_elements}\n")
    print(f"\nResults saved to {results_file}")


if __name__ == "__main__":
    main()
