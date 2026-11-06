#!/usr/bin/env python3
"""
Direct DiT forward-pass comparison: original vs FPQ-compressed Wan2.1 weights.
Tests the transformer in isolation with synthetic inputs — no T5 or VAE needed.
"""
import os, sys, time, json
import torch
import numpy as np


def load_dit_from_safetensors(path, config, dtype=torch.bfloat16):
    """Load a WanTransformer3DModel from a safetensors file with key remapping."""
    from diffusers import WanTransformer3DModel
    from safetensors.torch import load_file
    import re as re_mod

    print(f"  Loading WanTransformer3DModel from config...")
    model = WanTransformer3DModel(**config)

    print(f"  Loading weights from {os.path.basename(path)}...")
    state = load_file(path)

    def remap_key(file_key):
        k = file_key
        k = k.replace('.self_attn.', '.attn1.')
        k = k.replace('.cross_attn.', '.attn2.')
        k = re_mod.sub(r'\.attn([12])\.q\.', r'.attn\1.to_q.', k)
        k = re_mod.sub(r'\.attn([12])\.k\.', r'.attn\1.to_k.', k)
        k = re_mod.sub(r'\.attn([12])\.v\.', r'.attn\1.to_v.', k)
        k = re_mod.sub(r'\.attn([12])\.o\.', r'.attn\1.to_out.0.', k)
        k = re_mod.sub(r'\.ffn\.0\.', '.ffn.net.0.proj.', k)
        k = re_mod.sub(r'\.ffn\.2\.', '.ffn.net.2.', k)
        k = k.replace('.norm3.', '.norm2.')
        k = re_mod.sub(r'(blocks\.\d+)\.modulation$', r'\1.scale_shift_table', k)
        k = k.replace('head.head.', 'proj_out.')
        if k == 'head.modulation':
            k = 'scale_shift_table'
        k = k.replace('text_embedding.0.', 'condition_embedder.text_embedder.linear_1.')
        k = k.replace('text_embedding.2.', 'condition_embedder.text_embedder.linear_2.')
        k = k.replace('time_embedding.0.', 'condition_embedder.time_embedder.linear_1.')
        k = k.replace('time_embedding.2.', 'condition_embedder.time_embedder.linear_2.')
        k = k.replace('time_projection.1.', 'condition_embedder.time_proj.')
        return k

    model_state = model.state_dict()
    loaded = 0
    unmapped = []
    for file_key, tensor in state.items():
        model_key = remap_key(file_key)
        if model_key in model_state:
            target = model_state[model_key]
            # Reshape if needed (FPQ writer stores as 2D, model may expect 3D+)
            if tensor.shape != target.shape and tensor.numel() == target.numel():
                tensor = tensor.reshape(target.shape)
            model_state[model_key] = tensor.to(target.dtype)
            loaded += 1
        else:
            unmapped.append((file_key, model_key))

    if unmapped:
        print(f"  WARNING: {len(unmapped)} keys not mapped:")
        for fk, mk in unmapped[:5]:
            print(f"    {fk} -> {mk}")

    model.load_state_dict(model_state)
    model = model.to(dtype).eval()
    print(f"  Loaded {loaded}/{len(model_state)} weights")
    return model


def compute_metrics(a, b):
    """Compute similarity metrics between two tensors."""
    a_f = a.float().flatten()
    b_f = b.float().flatten()

    cos = torch.nn.functional.cosine_similarity(a_f.unsqueeze(0), b_f.unsqueeze(0)).item()
    mse = ((a_f - b_f) ** 2).mean().item()
    psnr = 10 * np.log10(4.0 / mse) if mse > 0 else float('inf')
    max_err = (a_f - b_f).abs().max().item()
    mean_err = (a_f - b_f).abs().mean().item()
    rel_err = mean_err / (a_f.abs().mean().item() + 1e-10)

    return {
        "cosine_similarity": cos,
        "mse": mse,
        "psnr_db": psnr,
        "max_abs_error": max_err,
        "mean_abs_error": mean_err,
        "relative_error": rel_err,
    }


def main():
    model_dir = os.path.expanduser("~/.local/share/models/wan2.1-t2v-1.3b")
    orig_path = os.path.join(model_dir, "diffusion_pytorch_model.safetensors")
    fpq_path = "/tmp/wan_fpq_v9.safetensors"
    out_dir = "/tmp/wan_compare"
    os.makedirs(out_dir, exist_ok=True)

    device = "mps" if torch.backends.mps.is_available() else "cpu"
    dtype = torch.bfloat16
    seed = 42

    print("=" * 65)
    print("Wan2.1-T2V-1.3B: DiT Forward Pass — Original vs FPQ v9")
    print("=" * 65)

    # Model config for 1.3B: dim=1536, 12 heads, head_dim=128, 30 layers
    config = {
        "num_attention_heads": 12,
        "attention_head_dim": 128,  # 12 * 128 = 1536 = dim
        "in_channels": 16,
        "out_channels": 16,
        "text_dim": 4096,
        "freq_dim": 256,
        "ffn_dim": 8960,
        "num_layers": 30,
        "cross_attn_norm": True,
        "qk_norm": "rms_norm_across_heads",
        "eps": 1e-06,
    }

    # Create synthetic inputs matching what the Wan DiT expects
    # The DiT takes: hidden_states (latent video), encoder_hidden_states (text), timestep
    torch.manual_seed(seed)
    batch = 1
    num_frames = 1
    h_latent, w_latent = 60, 104  # 480/8, 832/8
    latent_channels = 16
    text_seq_len = 64
    text_dim = 4096  # umt5-xxl hidden dim

    # Latent video input
    hidden_states = torch.randn(batch, latent_channels, num_frames, h_latent, w_latent,
                                dtype=dtype, device="cpu")
    # Text conditioning
    encoder_hidden_states = torch.randn(batch, text_seq_len, text_dim,
                                        dtype=dtype, device="cpu")
    # Timestep
    timestep = torch.tensor([500.0], dtype=dtype, device="cpu")  # mid-schedule

    print(f"\nSynthetic inputs:")
    print(f"  hidden_states:         {hidden_states.shape}")
    print(f"  encoder_hidden_states: {encoder_hidden_states.shape}")
    print(f"  timestep:              {timestep.shape}")
    print(f"  device: {device}, dtype: {dtype}")

    # ── Load and run original model ──
    print(f"\n{'─'*65}")
    print("Loading ORIGINAL model...")
    model_orig = load_dit_from_safetensors(orig_path, config, dtype)
    model_orig.to(device)

    print("Running forward pass (original)...")
    t0 = time.time()
    with torch.no_grad():
        out_orig = model_orig(
            hidden_states.to(device),
            encoder_hidden_states=encoder_hidden_states.to(device),
            timestep=timestep.to(device),
            return_dict=False,
        )
    if isinstance(out_orig, tuple):
        out_orig = out_orig[0]
    out_orig = out_orig.cpu()
    t_orig = time.time() - t0
    print(f"  Output shape: {out_orig.shape}")
    print(f"  Time: {t_orig:.2f}s")
    print(f"  Output stats: mean={out_orig.float().mean():.4f}, std={out_orig.float().std():.4f}")

    # Free original model
    del model_orig
    if device == "mps":
        torch.mps.empty_cache()
    import gc; gc.collect()

    # ── Load and run FPQ model ──
    print(f"\n{'─'*65}")
    print("Loading FPQ model...")
    model_fpq = load_dit_from_safetensors(fpq_path, config, dtype)
    model_fpq.to(device)

    print("Running forward pass (FPQ)...")
    t0 = time.time()
    with torch.no_grad():
        out_fpq = model_fpq(
            hidden_states.to(device),
            encoder_hidden_states=encoder_hidden_states.to(device),
            timestep=timestep.to(device),
            return_dict=False,
        )
    if isinstance(out_fpq, tuple):
        out_fpq = out_fpq[0]
    out_fpq = out_fpq.cpu()
    t_fpq = time.time() - t0
    print(f"  Output shape: {out_fpq.shape}")
    print(f"  Time: {t_fpq:.2f}s")
    print(f"  Output stats: mean={out_fpq.float().mean():.4f}, std={out_fpq.float().std():.4f}")

    del model_fpq
    if device == "mps":
        torch.mps.empty_cache()
    gc.collect()

    # ── Compare outputs ──
    print(f"\n{'='*65}")
    print("FORWARD PASS COMPARISON")
    print(f"{'='*65}")

    metrics = compute_metrics(out_orig, out_fpq)
    for k, v in metrics.items():
        if isinstance(v, float) and v != float('inf'):
            print(f"  {k:25s}: {v:.8f}")
        else:
            print(f"  {k:25s}: {v}")

    # Per-spatial-position analysis
    print(f"\n{'─'*65}")
    print("PER-CHANNEL ANALYSIS")
    print(f"{'─'*65}")
    for c in range(min(4, out_orig.shape[1])):
        ch_orig = out_orig[0, c].float().flatten()
        ch_fpq = out_fpq[0, c].float().flatten()
        ch_cos = torch.nn.functional.cosine_similarity(
            ch_orig.unsqueeze(0), ch_fpq.unsqueeze(0)).item()
        ch_mse = ((ch_orig - ch_fpq) ** 2).mean().item()
        print(f"  Channel {c:2d}: cos={ch_cos:.8f}  mse={ch_mse:.8e}")

    # Multiple timestep test
    print(f"\n{'─'*65}")
    print("TIMESTEP SWEEP (loading FPQ model once)")
    print(f"{'─'*65}")

    # Reload both models' outputs for multiple timesteps
    # Actually, re-run with different timesteps for just FPQ
    # to save memory, we reload once
    model_orig = load_dit_from_safetensors(orig_path, config, dtype)
    model_fpq = load_dit_from_safetensors(fpq_path, config, dtype)

    timesteps_to_test = [0.0, 100.0, 500.0, 900.0, 999.0]
    print(f"  {'Timestep':>10s}  {'Cosine':>12s}  {'PSNR (dB)':>10s}  {'MSE':>12s}")
    print(f"  {'─'*50}")

    sweep_results = []
    for ts_val in timesteps_to_test:
        ts = torch.tensor([ts_val], dtype=dtype, device="cpu")

        model_orig.to(device)
        with torch.no_grad():
            o_orig = model_orig(
                hidden_states.to(device),
                encoder_hidden_states=encoder_hidden_states.to(device),
                timestep=ts.to(device), return_dict=False)
        if isinstance(o_orig, tuple): o_orig = o_orig[0]
        o_orig = o_orig.cpu()
        model_orig.to("cpu")
        if device == "mps": torch.mps.empty_cache()

        model_fpq.to(device)
        with torch.no_grad():
            o_fpq = model_fpq(
                hidden_states.to(device),
                encoder_hidden_states=encoder_hidden_states.to(device),
                timestep=ts.to(device), return_dict=False)
        if isinstance(o_fpq, tuple): o_fpq = o_fpq[0]
        o_fpq = o_fpq.cpu()
        model_fpq.to("cpu")
        if device == "mps": torch.mps.empty_cache()

        m = compute_metrics(o_orig, o_fpq)
        print(f"  {ts_val:10.1f}  {m['cosine_similarity']:12.8f}  {m['psnr_db']:10.2f}  {m['mse']:12.8e}")
        sweep_results.append({"timestep": ts_val, **m})

    # Save all results
    results = {
        "forward_pass_metrics": metrics,
        "timestep_sweep": sweep_results,
        "config": {
            "seed": seed,
            "input_shape": list(hidden_states.shape),
            "text_shape": list(encoder_hidden_states.shape),
            "device": device,
            "dtype": str(dtype),
        },
        "timing": {"original_s": t_orig, "fpq_s": t_fpq},
    }

    results_path = os.path.join(out_dir, "dit_comparison.json")
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    # Final summary
    print(f"\n{'='*65}")
    print("SUMMARY")
    print(f"{'='*65}")
    print(f"  DiT forward pass cosine:  {metrics['cosine_similarity']:.8f}")
    print(f"  DiT forward pass PSNR:    {metrics['psnr_db']:.2f} dB")
    print(f"  DiT forward pass MSE:     {metrics['mse']:.8e}")
    print(f"  Timestep range cosine:    {min(r['cosine_similarity'] for r in sweep_results):.8f} – {max(r['cosine_similarity'] for r in sweep_results):.8f}")
    print(f"  Original time:            {t_orig:.2f}s")
    print(f"  FPQ time:                 {t_fpq:.2f}s")
    print(f"\n  Results: {results_path}")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()
