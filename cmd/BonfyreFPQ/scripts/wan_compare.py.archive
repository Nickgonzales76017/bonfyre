#!/usr/bin/env python3
"""
Compare Wan2.1-T2V-1.3B video generation: original vs FPQ-compressed weights.
Generates a single denoising step output for both models and compares.

Usage:
  python3 scripts/wan_compare.py \
    --original ~/.local/share/models/wan2.1-t2v-1.3b \
    --fpq /tmp/wan_fpq_v9.safetensors \
    --out /tmp/wan_compare
"""
import argparse
import os
import sys
import time
import json
import torch
import numpy as np
from pathlib import Path


def compute_metrics(a, b):
    """Compute similarity metrics between two tensors."""
    a_flat = a.float().flatten()
    b_flat = b.float().flatten()

    # Cosine similarity
    cos = torch.nn.functional.cosine_similarity(a_flat.unsqueeze(0), b_flat.unsqueeze(0)).item()

    # MSE / PSNR
    mse = ((a_flat - b_flat) ** 2).mean().item()
    if mse > 0:
        # For normalized [-1,1] range, max val = 2
        psnr = 10 * np.log10(4.0 / mse)
    else:
        psnr = float('inf')

    # Max absolute error
    max_err = (a_flat - b_flat).abs().max().item()

    return {"cosine": cos, "mse": mse, "psnr_db": psnr, "max_abs_error": max_err}


def load_wan_model(model_dir, device="cpu"):
    """Load Wan2.1 T2V model components."""
    sys.path.insert(0, model_dir) 

    from diffusers import WanPipeline

    print(f"Loading Wan pipeline from {model_dir}...")
    t0 = time.time()
    pipe = WanPipeline.from_pretrained(
        model_dir,
        torch_dtype=torch.bfloat16,
    )
    print(f"  Loaded in {time.time()-t0:.1f}s")
    return pipe


def swap_dit_weights(pipe, fpq_path):
    """Replace the DiT (transformer) weights with FPQ-compressed version."""
    from safetensors.torch import load_file
    print(f"Loading FPQ weights from {fpq_path}...")
    fpq_state = load_file(fpq_path)

    dit_state = pipe.transformer.state_dict()
    replaced = 0
    for key in dit_state:
        if key in fpq_state:
            dit_state[key] = fpq_state[key].to(dit_state[key].dtype)
            replaced += 1

    pipe.transformer.load_state_dict(dit_state)
    print(f"  Replaced {replaced}/{len(dit_state)} DiT weights")
    return pipe


def run_denoising_comparison(pipe_original, pipe_fpq, prompt, seed, num_steps, out_dir):
    """Run identical denoising and compare intermediate latents and final output."""
    os.makedirs(out_dir, exist_ok=True)
    device = "mps" if torch.backends.mps.is_available() else "cpu"

    results = {}

    # Use a minimal config: 1 step for speed, small resolution
    gen_kwargs = dict(
        prompt=prompt,
        num_frames=1,
        height=480,
        width=832,
        num_inference_steps=num_steps,
        guidance_scale=6.0,
        generator=torch.Generator(device="cpu").manual_seed(seed),
        output_type="latent",
    )

    print(f"\n--- Generating with ORIGINAL weights (seed={seed}, steps={num_steps}) ---")
    pipe_original.to(device)
    t0 = time.time()
    out_orig = pipe_original(**gen_kwargs)
    latents_orig = out_orig.frames  # or .images depending on version
    t_orig = time.time() - t0
    print(f"  Done in {t_orig:.1f}s")
    pipe_original.to("cpu")
    torch.mps.empty_cache() if device == "mps" else None

    print(f"\n--- Generating with FPQ weights (seed={seed}, steps={num_steps}) ---")
    pipe_fpq.to(device)
    t0 = time.time()
    gen_kwargs["generator"] = torch.Generator(device="cpu").manual_seed(seed)
    out_fpq = pipe_fpq(**gen_kwargs)
    latents_fpq = out_fpq.frames
    t_fpq = time.time() - t0
    print(f"  Done in {t_fpq:.1f}s")
    pipe_fpq.to("cpu")
    torch.mps.empty_cache() if device == "mps" else None

    # Compare latent outputs
    print("\n--- Latent Comparison ---")
    if isinstance(latents_orig, list):
        latents_orig = latents_orig[0]
    if isinstance(latents_fpq, list):
        latents_fpq = latents_fpq[0]

    latent_metrics = compute_metrics(latents_orig, latents_fpq)
    for k, v in latent_metrics.items():
        print(f"  {k}: {v:.6f}" if isinstance(v, float) else f"  {k}: {v}")
    results["latent"] = latent_metrics

    # Now decode both through VAE for pixel comparison
    print("\n--- Decoding latents through VAE ---")
    pipe_original.to(device)
    with torch.no_grad():
        if hasattr(pipe_original, 'vae'):
            pixels_orig = pipe_original.vae.decode(latents_orig.to(device)).sample
            pixels_fpq = pipe_original.vae.decode(latents_fpq.to(device)).sample

            pixel_metrics = compute_metrics(pixels_orig, pixels_fpq)
            print("  Pixel-space metrics:")
            for k, v in pixel_metrics.items():
                print(f"    {k}: {v:.6f}" if isinstance(v, float) else f"    {k}: {v}")
            results["pixel"] = pixel_metrics

            # Save as images if possible
            try:
                from torchvision.utils import save_image
                save_image(pixels_orig[0, :, 0] * 0.5 + 0.5,
                           os.path.join(out_dir, "frame_original.png"))
                save_image(pixels_fpq[0, :, 0] * 0.5 + 0.5,
                           os.path.join(out_dir, "frame_fpq.png"))
                print(f"  Saved frames to {out_dir}/")
            except Exception as e:
                print(f"  (Could not save images: {e})")
        else:
            print("  No VAE found, skipping pixel decode")

    pipe_original.to("cpu")

    results["timing"] = {"original_s": t_orig, "fpq_s": t_fpq}
    results["config"] = {"prompt": prompt, "seed": seed, "steps": num_steps}

    # Save results
    results_path = os.path.join(out_dir, "comparison.json")
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to {results_path}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Wan2.1 original vs FPQ comparison")
    parser.add_argument("--original", required=True, help="Path to original model dir")
    parser.add_argument("--fpq", required=True, help="Path to FPQ safetensors")
    parser.add_argument("--out", default="/tmp/wan_compare", help="Output directory")
    parser.add_argument("--prompt", default="A cat walking on a beach at sunset",
                        help="Generation prompt")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--steps", type=int, default=20,
                        help="Denoising steps (fewer = faster)")
    args = parser.parse_args()

    # Strategy: load pipeline once, run original, swap weights, run FPQ
    print("=" * 60)
    print("Wan2.1-T2V-1.3B: Original vs FPQ v9 Comparison")
    print("=" * 60)

    # Load original pipeline
    pipe = load_wan_model(args.original)

    # Make a deep copy of the original transformer state for restoration
    import copy
    orig_state = copy.deepcopy(pipe.transformer.state_dict())

    # Run original
    print("\n[1/2] Running with original weights...")
    device = "mps" if torch.backends.mps.is_available() else "cpu"

    gen_kwargs = dict(
        prompt=args.prompt,
        num_frames=1,
        height=480,
        width=832,
        num_inference_steps=args.steps,
        guidance_scale=6.0,
        generator=torch.Generator(device="cpu").manual_seed(args.seed),
        output_type="latent",
    )

    pipe.to(device)
    t0 = time.time()
    out_orig = pipe(**gen_kwargs)
    t_orig = time.time() - t0
    latents_orig = out_orig.frames if hasattr(out_orig, 'frames') else out_orig.images
    if isinstance(latents_orig, list):
        latents_orig = latents_orig[0]
    latents_orig = latents_orig.cpu()
    print(f"  Original: {t_orig:.1f}s, latent shape: {latents_orig.shape}")
    pipe.to("cpu")
    if device == "mps":
        torch.mps.empty_cache()

    # Swap weights to FPQ
    print("\n[2/2] Swapping to FPQ weights and re-running...")
    pipe = swap_dit_weights(pipe, args.fpq)

    pipe.to(device)
    gen_kwargs["generator"] = torch.Generator(device="cpu").manual_seed(args.seed)
    t0 = time.time()
    out_fpq = pipe(**gen_kwargs)
    t_fpq = time.time() - t0
    latents_fpq = out_fpq.frames if hasattr(out_fpq, 'frames') else out_fpq.images
    if isinstance(latents_fpq, list):
        latents_fpq = latents_fpq[0]
    latents_fpq = latents_fpq.cpu()
    print(f"  FPQ: {t_fpq:.1f}s, latent shape: {latents_fpq.shape}")
    pipe.to("cpu")
    if device == "mps":
        torch.mps.empty_cache()

    # Compare latents
    print("\n" + "=" * 60)
    print("LATENT COMPARISON")
    print("=" * 60)
    metrics = compute_metrics(latents_orig, latents_fpq)
    for k, v in metrics.items():
        print(f"  {k}: {v:.6f}" if isinstance(v, float) and v != float('inf') else f"  {k}: {v}")

    # Decode through VAE for pixel comparison
    print("\n" + "=" * 60)
    print("PIXEL COMPARISON (after VAE decode)")
    print("=" * 60)
    os.makedirs(args.out, exist_ok=True)

    pipe.to(device)
    with torch.no_grad():
        if hasattr(pipe, 'vae') and pipe.vae is not None:
            pixels_orig = pipe.vae.decode(latents_orig.to(device, dtype=pipe.vae.dtype)).sample.cpu()
            pixels_fpq = pipe.vae.decode(latents_fpq.to(device, dtype=pipe.vae.dtype)).sample.cpu()

            px_metrics = compute_metrics(pixels_orig, pixels_fpq)
            for k, v in px_metrics.items():
                print(f"  {k}: {v:.6f}" if isinstance(v, float) and v != float('inf') else f"  {k}: {v}")

            # Save frames as images
            try:
                # Normalize from [-1,1] to [0,1]
                img_orig = (pixels_orig[0, :, 0].clamp(-1, 1) * 0.5 + 0.5)
                img_fpq = (pixels_fpq[0, :, 0].clamp(-1, 1) * 0.5 + 0.5)

                from PIL import Image
                def tensor_to_pil(t):
                    arr = (t.permute(1, 2, 0).float().numpy() * 255).clip(0, 255).astype(np.uint8)
                    return Image.fromarray(arr)

                tensor_to_pil(img_orig).save(os.path.join(args.out, "frame_original.png"))
                tensor_to_pil(img_fpq).save(os.path.join(args.out, "frame_fpq.png"))

                # Difference map (amplified 10x)
                diff = (img_orig - img_fpq).abs() * 10
                tensor_to_pil(diff.clamp(0, 1)).save(os.path.join(args.out, "frame_diff_10x.png"))

                print(f"\n  Saved: frame_original.png, frame_fpq.png, frame_diff_10x.png → {args.out}/")
            except Exception as e:
                print(f"  (Could not save images: {e})")
        else:
            print("  No VAE available for pixel decode")

    # Summary
    results = {
        "latent_metrics": metrics,
        "timing": {"original_s": t_orig, "fpq_s": t_fpq},
        "config": {"prompt": args.prompt, "seed": args.seed, "steps": args.steps,
                    "resolution": "832x480", "num_frames": 1},
    }
    if 'px_metrics' in dir():
        results["pixel_metrics"] = px_metrics

    with open(os.path.join(args.out, "comparison.json"), "w") as f:
        json.dump(results, f, indent=2, default=str)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Latent cosine similarity: {metrics['cosine']:.6f}")
    print(f"  Latent PSNR: {metrics['psnr_db']:.2f} dB")
    print(f"  Original time: {t_orig:.1f}s  |  FPQ time: {t_fpq:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
