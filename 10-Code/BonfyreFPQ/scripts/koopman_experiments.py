#!/usr/bin/env python3
"""
Koopman Operator Field Experiments — v2 (theoretical optimum)
==============================================================
Four experiments that bound the Active Memory Bandwidth (AMB) reduction
achievable via Koopman decomposition of transformer MLP layers.

Theory recap
------------
Current FPQ:  every output token read d_in × d_out × bpw bits of weights
Koopman:      r_K eigenfunction evals  × b_psi bits — ONLY dynamic cost.
              r_K Koopman modes are STATIC (stored once, like the model).

  AMB_now   =  d_in × d_out × bpw
  AMB_koop  =  r_K  × b_psi             (modes pre-loaded, amortized to zero)
  Gain(raw) =  AMB_now / AMB_koop
  Gain(net) =  Gain(raw) / κ(G)         (interference penalty)

v2 improvements over v1
------------------------
  Exp A: removed broken Two-NN (unreliable N << exp(d)); added spectral decay
         exponent and multi-threshold explained-variance profile.
  Exp B: unchanged — effective-rank computation is correct; added Ledoit-Wolf
         shrinkage estimate for eigenvalue bias correction.
  Exp C: MAJOR — replaced full Jacobian PCA (r_K ≤ n_jac = 80) with the JVP
         ORACLE (single backward pass per sketch direction). Now n_pts × k_sketch
         = 300 × 32 = 9600 sketch vectors → reliable r_K up to d = 2048.
         Fixed AMB formula: modes are STATIC, only eigenfunction evals are
         dynamic per-token bandwidth.
  Exp D: REDESIGN — previous test was circular (k_active / k_cov ≈ 93% trivially).
         New test: SVD of W itself; actual structured quantization error; measure
         "damage ratio" = (error fraction in circuit) / (uniform-noise baseline).
         Damage ratio < 1 → circuit-preserving; = 1 → neutral; > 1 → harmful.
         Koopman rank-k_c SVD is shown as the theoretical circuit-preserving
         upper bound (damage ratio = 0 by Eckart-Young theorem).

Running
-------
  python3 scripts/koopman_experiments.py \\
      --model TinyLlama/TinyLlama-1.1B-Chat-v1.0 \\
      --n-samples 300 \\
      --layers 4,8,16 \\
      --k-sketch 32 \\
      --device mps

Output
------
  results/koopman_<model>_<timestamp>.json
  results/koopman_<model>_<timestamp>.md
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
    p.add_argument("--n-samples", type=int, default=300,
                   help="Activation vectors to collect per layer (Exp A/B/D)")
    p.add_argument("--layer-idx", type=int, default=8,
                   help="Primary layer to probe (overridden by --layers)")
    p.add_argument("--layers", type=str, default=None,
                   help="Comma-separated layer indices to sweep")
    p.add_argument("--k-sketch", type=int, default=32,
                   help="Random projection directions per activation point (Exp C JVP oracle)")
    p.add_argument("--n-jvp-pts", type=int, default=None,
                   help="Activation points used for JVP sketching (default: --n-samples)")
    p.add_argument("--b-psi", type=int, default=16,
                   help="Bits per eigenfunction evaluation (Koopman AMB formula)")
    p.add_argument("--bpw", type=float, default=6.55,
                   help="Current FPQ bits-per-weight for AMB baseline")
    p.add_argument("--k-circuit-pct", type=float, default=0.05,
                   help="Circuit rank as fraction of ||W||_F^2 captured (Exp D)")
    p.add_argument("--r-cutoff", type=float, default=0.99,
                   help="Explained variance cutoff for rank estimates")
    p.add_argument("--device", default="cpu")
    p.add_argument("--max-seq-len", type=int, default=512)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out-dir", default="results")
    return p.parse_args()

# ──────────────────────────────────────────────
#  Data: WikiText-2 sentences (no dependency)
# ──────────────────────────────────────────────

WIKITEXT_SAMPLE = """
The tower is 324 metres tall and is the tallest structure in Paris.
Its base is square, measuring 125 metres on each side.
During its construction, the Eiffel Tower surpassed the Washington Monument to become the tallest man-made structure in the world.
Napoleon Bonaparte was born in Corsica in 1769 and rose to become Emperor of France.
The French Revolution began in 1789 with the storming of the Bastille prison.
Quantum mechanics describes the physical properties of nature at the scale of atoms and subatomic particles.
The Heisenberg uncertainty principle states that the position and the velocity of an object cannot both be measured exactly at the same time.
Machine learning is a method of data analysis that automates analytical model building.
Neural networks are computing systems inspired by the biological neural networks that constitute animal brains.
The transformer architecture was introduced in the paper Attention Is All You Need by Vaswani et al.
Gradient descent is an optimization algorithm used to minimize a function by iteratively moving in the direction of steepest descent.
The Renaissance was a period of cultural and intellectual flourishing that began in Florence, Italy in the 14th century.
Shakespeare wrote 37 plays and 154 sonnets during his lifetime.
The speed of light in a vacuum is approximately 299,792,458 metres per second.
DNA carries genetic information in the form of a code using four chemical bases: adenine, guanine, cytosine, and thymine.
The solar system consists of the Sun and the objects that orbit it, including eight planets.
Climate change refers to long-term shifts in temperatures and weather patterns on Earth.
The internet is a global system of interconnected computer networks that uses the Internet protocol suite.
Artificial intelligence is the simulation of human intelligence processes by computer systems.
The stock market is a marketplace where buyers and sellers trade shares of publicly listed companies.
"""

def get_wikitext_tokens(tokenizer, n_sentences=200, max_len=512):
    """Return a list of token id tensors from built-in sentences + repetition."""
    sentences = [s.strip() for s in WIKITEXT_SAMPLE.strip().split('\n') if s.strip()]
    # Try to load real WikiText-2 if datasets is available
    try:
        from datasets import load_dataset
        ds = load_dataset("wikitext", "wikitext-2-raw-v1", split="train")
        texts = [x["text"] for x in ds if len(x["text"]) > 100][:n_sentences]
        print(f"  Using real WikiText-2 ({len(texts)} passages)")
    except Exception:
        texts = (sentences * ((n_sentences // len(sentences)) + 1))[:n_sentences]
        print(f"  Using built-in sample sentences ({len(texts)} passages)")

    chunks = []
    for t in texts:
        ids = tokenizer(t, return_tensors="pt", truncation=True,
                       max_length=max_len).input_ids
        if ids.shape[1] >= 8:
            chunks.append(ids)
    return chunks

# ──────────────────────────────────────────────
#  Activation collection
# ──────────────────────────────────────────────

def collect_activations(model, tokenizer, layer_indices, n_samples, device, max_seq_len):
    """
    Collect pre-MLP and post-MLP activations for specified layers.
    Returns dict: layer_idx -> {"pre": (N, d), "post": (N, d)}
    """
    model.eval()
    records = {idx: {"pre": [], "post": []} for idx in layer_indices}
    hooks = []
    per_layer_total = {idx: 0 for idx in layer_indices}

    def make_hooks(layer_idx):
        def pre_hook(module, inp):
            if per_layer_total[layer_idx] >= n_samples:
                return
            x = inp[0].detach().float()                     # (batch, seq, d)
            flat = x.reshape(-1, x.shape[-1])               # (batch*seq, d)
            records[layer_idx]["pre"].append(flat.cpu())
            per_layer_total[layer_idx] += flat.shape[0]

        def post_hook(module, inp, out):
            if isinstance(out, tuple):
                out = out[0]
            x = out.detach().float()
            flat = x.reshape(-1, x.shape[-1])
            records[layer_idx]["post"].append(flat.cpu())

        return pre_hook, post_hook

    # Attach hooks to the full MLP module (input/output at d=hidden_size)
    # This gives us the nonlinear map x -> mlp(x) in R^d, correct for Koopman.
    for idx in layer_indices:
        layer = model.model.layers[idx]
        mlp = layer.mlp
        ph, oh = make_hooks(idx)
        hooks.append(mlp.register_forward_pre_hook(ph))
        hooks.append(mlp.register_forward_hook(oh))

    print(f"  Collecting activations ({n_samples} target tokens)...")
    chunks = get_wikitext_tokens(tokenizer, n_sentences=300, max_len=max_seq_len)

    with torch.no_grad():
        for chunk in chunks:
            if all(per_layer_total[idx] >= n_samples for idx in layer_indices):
                break
            try:
                model(chunk.to(device))
            except Exception:
                continue

    for h in hooks:
        h.remove()

    # Consolidate
    result = {}
    for idx in layer_indices:
        pre  = torch.cat(records[idx]["pre"],  dim=0)[:n_samples]
        post = torch.cat(records[idx]["post"], dim=0)[:n_samples] if records[idx]["post"] else None
        result[idx] = {"pre": pre, "post": post}
        print(f"  Layer {idx}: collected {pre.shape[0]} activation vectors (d={pre.shape[1]})")

    return result

# ──────────────────────────────────────────────
#  Experiment A: Intrinsic Dimension (bounds T4)
# ──────────────────────────────────────────────

def experiment_A(activations, r_cutoff=0.99):
    """
    Measure intrinsic dimension of the MLP input activation manifold.

    Estimators:
      - PCA k@threshold: number of PCs needed to explain r_cutoff variance
      - Participation ratio (PR): (Σσ_i)² / Σσ_i² — robust, no threshold parameter
      - Spectral decay exponent α: fit log(σ_i) ~ -α·log(i) — decay rate quantifies
        manifold 'sharpness'; α > 1 implies geometric progression (fast decay, low dim)
      - Multi-threshold profile: k@{50, 80, 90, 95, 99}% for full picture

    Note: Two-NN intrinsic dimension estimator (Facco 2017) is NOT used here.
    With N=300 samples in R^2048, the local neighborhood structure is dominated
    by the curse of dimensionality — all pairwise distances concentrate at the
    same value, making the μ = r2/r1 ratio close to 1 everywhere and returning
    a spuriously low estimate. Two-NN is only valid when N >> exp(d_intrinsic / 2).

    T4 bound: AMB_T4 = AMB_now / (d / PR)  where PR ≈ k_intrinsic
    """
    print("\n[Experiment A] Intrinsic dimension of activation manifold")

    results = {}
    for layer_idx, acts in activations.items():
        X = acts["pre"]                         # (N, d)
        N, d = X.shape

        mu   = X.mean(0)
        X_c  = (X - mu).float()
        _, S, _ = torch.linalg.svd(X_c, full_matrices=False)   # S: min(N, d)

        S2   = S ** 2
        var  = S2 / S2.sum()
        cumvar = var.cumsum(0)

        # PCA rank at r_cutoff
        k_cutoff = int((cumvar < r_cutoff).sum().item()) + 1
        k_cutoff = min(k_cutoff, d)

        # Participation ratio (Legrand & Derrida 1988)
        PR   = (S.sum() ** 2) / S2.sum()

        # Multi-threshold profile
        thresholds = [0.50, 0.80, 0.90, 0.95, 0.99]
        k_profile  = {}
        for thr in thresholds:
            k_t = int((cumvar < thr).sum().item()) + 1
            k_profile[f"k_{int(thr*100)}pct"] = k_t

        # Spectral decay exponent α: fit log(σ_i) = C - α·log(i) for i = 1..k_fit
        k_fit = min(k_cutoff, 200)
        if k_fit >= 10:
            log_i = torch.log(torch.arange(1, k_fit + 1).float())
            log_s = torch.log(S[:k_fit].clamp(min=1e-12))
            # OLS: α = -cov(log_i, log_s) / var(log_i)
            li_c = log_i - log_i.mean()
            ls_c = log_s - log_s.mean()
            alpha = -(li_c * ls_c).sum() / (li_c ** 2).sum()
            alpha = alpha.item()
        else:
            alpha = float("nan")

        T4_gain = d / max(PR.item(), 1)

        print(f"  Layer {layer_idx}: d={d}, N={N}")
        print(f"    PCA k@{r_cutoff*100:.0f}%var        = {k_cutoff}  ({k_cutoff/d*100:.1f}% of d)")
        print(f"    Participation ratio PR = {PR.item():.1f}  ({PR.item()/d*100:.2f}% of d)")
        print(f"    Spectral decay α       = {alpha:.3f}  (α>1 → fast decay; power-law manifold)")
        for l, k in k_profile.items():
            print(f"    {l:12s} = {k:4d}  ({k/d*100:.1f}%)")
        print(f"    T4 manifold gain       = {T4_gain:.1f}x")

        results[layer_idx] = {
            "d": d, "N": N,
            "k_pca": k_cutoff,
            "k_pca_fraction": round(k_cutoff / d, 4),
            "participation_ratio": round(PR.item(), 2),
            "PR_fraction": round(PR.item() / d, 6),
            "spectral_decay_alpha": round(alpha, 4),
            **k_profile,
            "T4_AMB_gain": round(T4_gain, 1),
        }

    return results

# ──────────────────────────────────────────────
#  Experiment B: Effective rank of Sigma_x (bounds T3)
# ──────────────────────────────────────────────

def experiment_B(activations, r_cutoff=0.99):
    """
    Measure effective rank rho(Σ_x) = tr(Σ_x) / ‖Σ_x‖_op and the water-filling
    bound on AMB reduction from distributional concentration.

    Eigenvalue bias correction:
      Raw sample covariance eigenvalues are biased (Marchenko-Pastur) when N/d is
      small. Here N=300, d=2048 → N/d = 0.15 (severely under-sampled). We apply
      the Oracle Approximating Shrinkage (OAS / Ledoit-Wolf analytical formula):
          σ_i_shrunk = ((N-2)/N) σ_i + (1/N) tr(Σ̂)/d
      This pulls small eigenvalues up and large eigenvalues down, reducing the
      apparent concentration — making our T3 gain a LOWER bound (conservative).

    T3 bound:
      Water-filling optimal bits allocation: b_k* = max(0, ν - log₂(σ_k))
      Total bits relative to flat: T3_gain = d × bpw / Σ_k b_k* (at same distortion)
      Conservative proxy: T3_gain = d / rho (= 1/rho_fraction)
    """
    print("\n[Experiment B] Effective rank of input covariance Sigma_x")

    results = {}
    for layer_idx, acts in activations.items():
        X   = acts["pre"]
        N, d = X.shape
        mu  = X.mean(0)
        X_c = (X - mu).double()

        # Sample covariance
        Sigma_raw = (X_c.T @ X_c) / (N - 1)
        eigvals_raw = torch.linalg.eigvalsh(Sigma_raw).flip(0).clamp(min=0)  # descending

        # Ledoit-Wolf analytical shrinkage (Oracle Approx. Shrinkage for Gaussian)
        # Coefficient: ρ = ((n-2)/n + (d+2)/(n·d)) ← simplified OAS formula
        #   λ_shrunk = (1-ρ) λ_raw + ρ · tr(Σ̂)/d
        rho_lw = min(1.0, (d + 2) / (N * d) + (N - 2) / N)
        trace_raw = eigvals_raw.sum().item()
        mean_eig  = trace_raw / d
        eigvals   = (1 - rho_lw) * eigvals_raw + rho_lw * mean_eig
        eigvals   = eigvals.clamp(min=0)

        # Raw (uncorrected) effective rank for comparison
        eigvals_raw_desc = eigvals_raw.clone()
        trace_raw_ = eigvals_raw_desc.sum().item()
        op_raw_    = eigvals_raw_desc[0].item()
        rho_raw    = trace_raw_ / op_raw_ if op_raw_ > 0 else float("nan")

        trace   = eigvals.sum().item()
        op_norm = eigvals[0].item()
        rho_eff = trace / op_norm if op_norm > 0 else float("nan")   # LW-corrected effective rank
        rho_frac = rho_eff / d

        # Multi-threshold effective rank
        cumvar   = eigvals.cumsum(0) / trace
        k_eff_90 = int((cumvar < 0.90).sum().item()) + 1
        k_eff_95 = int((cumvar < 0.95).sum().item()) + 1
        k_eff_99 = int((cumvar < r_cutoff).sum().item()) + 1

        # Top-p% direction variance fraction
        top5_k   = max(1, int(0.05 * d))
        top5_var = eigvals[:top5_k].sum().item() / trace if trace > 0 else 0

        # Water-filling bound: T3_gain ≈ d / rho_eff  (tighter than naive k_eff / d)
        # Using LW (corrected) rho = conservative (closer to truth);
        # also report raw for comparison.
        T3_gain_lw  = d / max(rho_eff, 1.0)     # LW-corrected: very conservative
        T3_gain_raw = d / max(rho_raw, 1.0)      # raw sample: optimistic, biased

        # Flat-rate AMB reduction (no water-filling)
        flat_gain = d / max(k_eff_99, 1)

        print(f"  Layer {layer_idx}: N={N}, d={d}  (LW shrinkage ρ={rho_lw:.3f})")
        print(f"    rho raw (biased)   = {rho_raw:.1f}  ({rho_raw/d*100:.2f}% of d)  ← opt")
        print(f"    rho LW  (shrunk)   = {rho_eff:.1f}  ({rho_frac*100:.2f}% of d)  ← consrv")
        print(f"    k@90%      = {k_eff_90}   k@95% = {k_eff_95}   k@99% = {k_eff_99}")
        print(f"    Top-5% dirs capture = {top5_var*100:.1f}% variance")
        print(f"    T3 gain raw     = {T3_gain_raw:.1f}x  (sample-biased upper bound)")
        print(f"    T3 gain LW      = {T3_gain_lw:.1f}x  (shrinkage lower bound)")
        print(f"    T3 flat-rate    = {flat_gain:.1f}x  (k_eff99)")

        results[layer_idx] = {
            "rho_raw": round(rho_raw, 2),
            "rho_raw_fraction": round(rho_raw / d, 6),
            "rho_lw": round(rho_eff, 2),
            "rho_lw_fraction": round(rho_frac, 6),
            "lw_shrinkage_coeff": round(rho_lw, 4),
            "top5pct_variance": round(top5_var, 4),
            "k_eff_90": k_eff_90,
            "k_eff_95": k_eff_95,
            "k_eff_99": k_eff_99,
            "T3_AMB_gain_raw": round(T3_gain_raw, 1),
            "T3_AMB_gain_lw": round(T3_gain_lw, 1),
            "T3_AMB_gain_flatrate": round(flat_gain, 1),
        }

    return results

# ──────────────────────────────────────────────
#  Experiment C: Koopman rank via JVP oracle
# ──────────────────────────────────────────────

def _jvp_sketch(mlp, x, v, device):
    """
    Compute J(x)^T v via a single backward pass (JVP oracle).

      x : (d,)      — activation vector
      v : (d_out,)  — random sketch direction
    Returns:
      J(x)^T v ∈ R^{d_in}  — one column of the Jacobian transpose

    Theory: for a random isotropic v, J^T v ∈ R^{d_in} has range = row-space(J).
    By collecting n_pts × k_sketch such vectors and stacking into B ∈ R^{n×d_in},
    rank(B) converges to rank(J) = Koopman rank r_K as n → ∞.
    This is the correct way to estimate r_K without O(d²) memory per Jacobian.
    """
    x_t = x.clone().detach().to(device).float()
    x_t.requires_grad_(True)
    v_t = v.to(device).float()

    try:
        y = mlp(x_t.unsqueeze(0)).squeeze(0)    # (d_out,)
        s = (y * v_t.detach()).sum()             # scalar — safe: v is a direction, not learned
        s.backward()
        jvp = x_t.grad.detach().cpu()
    except Exception:
        jvp = torch.zeros(x_t.shape[0])

    return jvp


def experiment_C(model, activations, layer_indices, device,
                 k_sketch=32, r_cutoff=0.99, bpw=6.55, b_psi=16):
    """
    Measure Koopman operator rank r_K via the JVP oracle (sketched Jacobian).

    v1 problem fixed:
      Full Jacobians ∈ R^{d × d} with n_jac=80 → r_K ≤ 80 (sample-bound).
      The measured r_K=70 was not the true rank; it was the number of Jacobians.

    v2 approach (JVP oracle):
      For each activation x_i, sample k_sketch random vectors v_j ~ N(0, I_{d_out}).
      Compute J(x_i)^T v_j via ONE backward pass per (i, j).
      Stack B ∈ R^{n_pts × k_sketch, d_in}.  rank(B) = r_K (with high probability).
      With n_pts=300, k_sketch=32 → B is (9600, d_in) → reliable r_K up to d_in.

    Correct AMB formula (static vs dynamic cost separation):
      CURRENT FPQ — per token:
        AMB_now = d_in × d_out × bpw  bits  (read the whole weight matrix)
      KOOPMAN — per token:
        AMB_koop = r_K × b_psi_eff  bits  where b_psi_eff = b_psi + log₂(κ(G))
        Modes M_k ∈ R^{d_out × d_in} are STATIC — loaded once, like the model weights.
      GAIN:
        Gain_raw = (d_in × d_out × bpw) / (r_K × b_psi)
        Gain_net = (d_in × d_out × bpw) / (r_K × (b_psi + log₂(κ)))

    Gram matrix κ(G) — additive bits penalty:
      κ(G) = λ_max / λ_min of eigenfunction correlation matrix.
      κ = 1 → perfectly orthogonal, zero penalty.
      κ >> 1 → log₂(κ) extra bits per eigenfunction (numerical precision for inversion).
      Dividing gain by κ directly (v1 mistake) is wrong — κ should add log₂(κ) bits.

    CENTERING (critical fix vs v1):
      We center B by subtracting the mean JVP direction:
        B_c = B - B.mean(0)
      This isolates ΔJ(x) = J(x) - J̄ (nonlinear Jacobian variation around the mean).
      Without centering: rank of B = rank of J̄ ≈ d_in (always full rank).
      With centering: rank of B_c = rank of NONLINEAR VARIATIONS (the Koopman kernel).
      For SiLU MLPs: ΔJ is driven by gate node activations; expected rank << d.
    """
    print("\n[Experiment C] Koopman rank via JVP oracle (sketched Jacobians)")
    print(f"  k_sketch = {k_sketch}  b_psi = {b_psi}  bpw = {bpw}")

    results = {}

    for layer_idx in layer_indices:
        acts  = activations[layer_idx]
        X     = acts["pre"]                       # (N, d_in)
        N, d_in = X.shape
        n_pts = N

        layer = model.model.layers[layer_idx]
        mlp   = layer.mlp

        # Probe d_out from a test forward pass
        with torch.no_grad():
            x_tmp = X[0].to(device).float()
            d_out = mlp(x_tmp.unsqueeze(0)).shape[-1]

        print(f"  Layer {layer_idx}: d_in={d_in}, d_out={d_out}, "
              f"n_pts={n_pts}, k_sketch={k_sketch} "
              f"→ {n_pts * k_sketch} sketch vectors")

        # ── Collect sketched JVPs ──────────────────────────────────
        t0 = time.time()
        B = torch.zeros(n_pts * k_sketch, d_in)    # (n_pts*k_sketch, d_in)

        for i in range(n_pts):
            # Sample k_sketch directions from the unit sphere in R^{d_out}
            V = torch.randn(k_sketch, d_out)       # random projections
            V = V / V.norm(dim=1, keepdim=True).clamp(min=1e-12)

            for j in range(k_sketch):
                jvp = _jvp_sketch(mlp, X[i], V[j], device)
                B[i * k_sketch + j] = jvp

            if (i + 1) % 50 == 0:
                elapsed = time.time() - t0
                eta = elapsed / (i + 1) * (n_pts - i - 1)
                print(f"    {i+1}/{n_pts} pts ({elapsed:.1f}s elapsed, ETA {eta:.0f}s)")

        print(f"    JVP collection done in {time.time()-t0:.1f}s")

        # ── Center B to isolate nonlinear Jacobian variation ─────
        # WITHOUT centering: B spans row_space(J̄) — trivially full-rank ≈ d_in.
        # WITH centering:    B_c spans row_space(ΔJ) — the Koopman kernel.
        # ΔJ(x) = J(x) - E_x[J(x)] captures how the Jacobian changes with input.
        # For SiLU-gated MLPs this is driven by which gate nodes change; expected << d.
        B_mean = B.mean(0, keepdim=True)           # (1, d_in) — mean JVP direction
        B_c    = B - B_mean                        # (n_pts*k_sketch, d_in) — centered

        # ── Gram matrix G_c = B_c^T B_c / n  (d_in × d_in) ──────
        B_c_norm = B_c / (n_pts * k_sketch) ** 0.5
        G_c = B_c_norm.T @ B_c_norm                # (d_in, d_in)

        eigh_result  = torch.linalg.eigh(G_c)
        eigvals_G_c  = eigh_result.eigenvalues.flip(0).clamp(min=0)   # descending
        eigvecs_G_c  = eigh_result.eigenvectors.flip(1)               # (d_in, d_in)

        var_G     = eigvals_G_c / eigvals_G_c.sum().clamp(min=1e-30)
        cumvar_G  = var_G.cumsum(0)

        r_K    = int((cumvar_G < r_cutoff).sum().item()) + 1
        r_K    = min(r_K, n_pts * k_sketch)
        r_K_90 = int((cumvar_G < 0.90).sum().item()) + 1

        print(f"    Koopman rank r_K @90%var = {r_K_90}  (nonlinear variations)")
        print(f"    Koopman rank r_K @{r_cutoff*100:.0f}%var = {r_K}")

        # ── Gram matrix of Koopman eigenfunctions ─────────────────
        # Use only the significant eigenvectors (top-r_K_90) to avoid noise blowup.
        # Φ_k(x) ≈ v_k^T (x - x̄) = linear eigenfunction approximation.
        # Normalise to zero mean + unit std so G_{jk} = corr(Φ_j, Φ_k).
        # Cap k_gram: Gram matrix is only meaningful when k_gram << N
        # (k_gram > N → rank-deficient Gram → degenerate κ)
        k_gram = max(1, min(r_K_90, N // 5))
        X_c_act = (X.float() - X.float().mean(0))              # (N, d_in) centered acts
        Phi = X_c_act @ eigvecs_G_c[:, :k_gram]               # (N, k_gram)
        Phi_std = Phi.std(0).clamp(min=1e-12)
        Phi_n   = (Phi - Phi.mean(0)) / Phi_std               # zero-mean, unit-std

        G_phi = (Phi_n.T @ Phi_n) / N                         # (k_gram, k_gram) correlation
        eig_phi = torch.linalg.eigvalsh(G_phi).abs().flip(0).clamp(min=1e-12)
        kappa   = (eig_phi[0] / eig_phi[-1]).item()

        # Noise floor estimate: expected κ for pure noise ≈ ((1 + √(k/N))² / (1 - √(k/N))²)
        ratio    = (k_gram / N) ** 0.5
        kappa_mp = ((1 + ratio) / max(1 - ratio, 1e-6)) ** 2  # Marchenko-Pastur kappa
        kappa_signal = max(kappa / max(kappa_mp, 1.0), 1.0)   # signal-only kappa (adj)

        print(f"    Gram κ(G_φ) = {kappa:.2f}  (k={k_gram}, MP noise={kappa_mp:.1f}, adj={kappa_signal:.2f})")

        # ── Correct AMB formula ────────────────────────────────────
        # CURRENT FPQ: read full weight matrix per token
        AMB_now  = d_in * d_out * bpw               # bits per token per layer

        # KOOPMAN: read r_K eigenfunction evaluations per token (modes are static)
        # Effective bits per eval = b_psi + log₂(κ_signal) (inversion precision penalty)
        log2_kappa = max(0.0, float(np.log2(kappa_signal)))
        b_psi_eff  = b_psi + log2_kappa
        AMB_koop   = r_K * b_psi                     # raw (assuming perfect ortho)
        AMB_koop_eff = r_K * b_psi_eff               # with interference penalty

        # Static mode storage (pre-loaded, not per-token)
        # Eigenvectors v_k ∈ R^{d_in} + modes M_k ∈ R^{d_out}, 4 bytes each
        static_overhead_bits = r_K * (d_in + d_out) * 32  # bits (float32)

        gain_raw  = AMB_now / max(AMB_koop, 1)
        gain_net  = AMB_now / max(AMB_koop_eff, 1)

        print(f"    AMB now     = {AMB_now/1e6:.3f} Mbits/token")
        print(f"    AMB koop    = {AMB_koop/1e3:.2f} Kbits/token  (r_K×b_psi, perfect ortho)")
        print(f"    b_psi_eff   = {b_psi_eff:.1f} = {b_psi} + log₂({kappa_signal:.1f})")
        print(f"    Gain raw    = {gain_raw:.0f}x  (no interference)")
        print(f"    Gain net    = {gain_net:.0f}x  (with κ penalty)")
        print(f"    Static modes overhead = {static_overhead_bits/1e6:.1f} Mbits (once)")

        results[layer_idx] = {
            "d_in": d_in, "d_out": d_out,
            "n_sketch_vectors": n_pts * k_sketch,
            "r_K": r_K,
            "r_K_90pct": r_K_90,
            "r_K_fraction": round(r_K / d_in, 4),
            "kappa_G_phi": round(kappa, 2),
            "kappa_mp_noise": round(kappa_mp, 2),
            "kappa_signal_adj": round(kappa_signal, 2),
            "b_psi_eff": round(b_psi_eff, 2),
            "AMB_now_Mbits": round(AMB_now / 1e6, 4),
            "AMB_koop_Kbits": round(AMB_koop / 1e3, 3),
            "AMB_gain_raw": round(gain_raw, 0),
            "AMB_gain_net": round(gain_net, 0),
            "static_modes_Mbits": round(static_overhead_bits / 1e6, 2),
        }

    return results


# ──────────────────────────────────────────────
#  Experiment D: Circuit damage ratio (bounds T2)
# ──────────────────────────────────────────────

def experiment_D(model, layer_indices, k_circuit_pct=0.05, bits=4):
    """
    'Circuit damage ratio' for down_proj: does scalar quantization damage
    the directions that matter most for computation?

    v1 problem fixed:
      Previous test computed k_active = rank of W @ Σ_x^(1/2) and measured
      fraction of uniform noise in those directions. But k_cov (the rank of
      Σ_x^(1/2)) was bounded by N=300 samples, so k_active ≈ k_cov ≈ 93%
      trivially. The test was circular — it told us nothing.

    v2 correct test:
      1. SVD of W directly: W = U S V^T  (V^T ∈ R^{d_in × d_in})
         Circuit rank k_c = smallest k such that top-k captures ≥ k_circuit_pct
         fraction of ‖W‖_F² (the signal energy).
      2. Actual structured quantization: W_q = round(W / q_step) × q_step
         (4-bit symmetric scalar — good proxy for FPQ-style quantization)
         Error: E = W_q - W
      3. Circuit damage fraction:
         f_circ = ‖E V[:, :k_c]‖_F² / ‖E‖_F²
         Baseline (if E were random white noise): k_c / d_in
         Damage ratio DR = f_circ / (k_c / d_in)
           DR < 1 → quantization avoids circuit (circuit-preserving) ✓
           DR = 1 → neutral (error uniformly distributed)
           DR > 1 → quantization damages circuit (harmful) ✗

      4. Koopman ideal (Eckart-Young theorem):
         Best rank-k_c approximation error = U[:, k_c:] S[k_c:] V[k_c:]^T
         → all error is in BOTTOM singular directions → DR_koop = 0 (perfectly
         circuit-preserving by construction).
         This is the floor that Koopman-mode quantization approaches.

    Interpretation:
      DR > 1 → FPQ uniform quant damages circuits more than random noise would.
               Koopman decomposition (DR → 0) is necessary for circuit preservation.
      DR ≈ 1 → FPQ is neutral — Koopman gives an improvement but not critical.
      DR < 1 → FPQ naturally avoids circuits — Koopman ordering is a bonus.
    """
    print(f"\n[Experiment D] Circuit damage ratio (T2 bound, k_circuit={k_circuit_pct*100:.0f}%, {bits}-bit)")

    results = {}
    for layer_idx in layer_indices:
        W = model.model.layers[layer_idx].mlp.down_proj.weight.detach().float().cpu()
        d_out, d_in = W.shape

        # SVD of W
        U, S, Vh = torch.linalg.svd(W, full_matrices=False)         # Vh: (min(d_out,d_in), d_in)

        # Circuit rank k_c: top-k captures k_circuit_pct fraction of ‖W‖_F²
        S2 = S ** 2
        total_energy = S2.sum().item()
        k_c = int((S2.cumsum(0) / total_energy < k_circuit_pct).sum().item()) + 1
        k_c = max(k_c, 1)

        # Top-k captures this fraction of W's energy
        energy_captured = S2[:k_c].sum().item() / total_energy

        print(f"  Layer {layer_idx}: d_out={d_out}, d_in={d_in}")
        print(f"    Circuit rank k_c = {k_c}  ({k_c/d_in*100:.2f}% of d_in, "
              f"captures {energy_captured*100:.1f}% of ‖W‖_F²)")

        # Actual quantization error (4-bit symmetric scalar)
        q_scale = W.abs().max().item() / (2 ** (bits - 1) - 1)
        W_q     = (W / q_scale).round().clamp(-(2**(bits-1)), 2**(bits-1)-1) * q_scale
        E       = W_q - W                                              # (d_out, d_in)

        E_frob2 = (E ** 2).sum().item()

        # Project error onto top-k_c right singular vectors (circuit subspace)
        Vk     = Vh[:k_c]                                              # (k_c, d_in)
        E_circ = E @ Vk.T @ Vk                                         # (d_out, d_in)
        f_circ = (E_circ ** 2).sum().item() / max(E_frob2, 1e-30)

        baseline    = k_c / d_in                                       # expected if E is white
        damage_ratio = f_circ / max(baseline, 1e-12)

        # Koopman ideal: rank-k_c approximation error — all energy in BOTTOM directions
        # By Eckart-Young, error is exactly E_svd = Σ_{i>k_c} σ_i u_i v_i^T
        # Projection onto top-k_c circuit directions: exactly ZERO.
        DR_koopman = 0.0
        svd_error_frob2 = S2[k_c:].sum().item()
        svd_error_fraction = svd_error_frob2 / (S2.sum().item() + 1e-30)

        print(f"    Scalar quant error  ‖E‖_F² = {E_frob2:.4f}")
        print(f"    Error in circuit subspace   = {f_circ*100:.2f}%")
        print(f"    Uniform-noise baseline      = {baseline*100:.2f}%  (k_c/d_in)")
        print(f"    Circuit damage ratio DR     = {damage_ratio:.3f}")

        if damage_ratio < 0.9:
            verdict = f"✓ circuit-preserving (DR={damage_ratio:.2f} < 1)"
        elif damage_ratio < 1.1:
            verdict = f"~ neutral (DR={damage_ratio:.2f} ≈ 1)"
        else:
            verdict = f"✗ circuit-damaging (DR={damage_ratio:.2f} > 1) — Koopman needed"

        print(f"    Verdict: {verdict}")
        print(f"    Koopman ideal DR        = {DR_koopman:.1f}  (rank-k_c SVD is perfectly circuit-safe)")
        print(f"    SVD error as % of ‖W‖_F = {svd_error_fraction*100:.2f}%")

        results[layer_idx] = {
            "d_out": d_out, "d_in": d_in,
            "k_c": k_c,
            "k_c_fraction_din": round(k_c / d_in, 4),
            "energy_in_kc": round(energy_captured, 4),
            "E_frob2": round(E_frob2, 4),
            "f_circuit": round(f_circ, 4),
            "baseline": round(baseline, 4),
            "damage_ratio": round(damage_ratio, 4),
            "koopman_ideal_DR": 0.0,
            "svd_error_fraction": round(svd_error_fraction, 4),
            "verdict": verdict,
        }

    return results


# ──────────────────────────────────────────────
#  Summary: AMB reduction table
# ──────────────────────────────────────────────

def build_summary(exp_a, exp_b, exp_c, exp_d, bpw=6.55, b_psi=16):
    print("\n" + "═"*70)
    print("  KOOPMAN AMB REDUCTION SUMMARY")
    print("═"*70)

    rows = []
    for layer_idx in exp_a:
        a  = exp_a[layer_idx]
        b  = exp_b.get(layer_idx, {})
        c  = exp_c.get(layer_idx, {})
        d  = exp_d.get(layer_idx, {})

        t4       = a.get("T4_AMB_gain", "?")
        t3_raw   = b.get("T3_AMB_gain_raw", b.get("T3_AMB_gain_waterfill", "?"))
        t3_lw    = b.get("T3_AMB_gain_lw", "?")
        gain     = c.get("AMB_gain_net", "?")
        dr   = d.get("damage_ratio", "?")

        print(f"\n  Layer {layer_idx}:")
        print(f"    T4 manifold gain (PR-based)                  = {t4}x")
        print(f"    T3 distribution gain (raw, biased up)         = {t3_raw}x")
        print(f"    T3 distribution gain (LW-shrunk, biased down) = {t3_lw}x")
        print(f"    Koopman gain net (÷κ, correct AMB)           = {gain}x")
        print(f"    r_K                                           = {c.get('r_K', '?')}")
        print(f"    κ(G_φ)                                        = {c.get('kappa_G_phi', '?')}")
        print(f"    Circuit damage ratio DR                       = {dr}  (1=neutral, <1=safe)")
        if isinstance(c, dict) and "AMB_now_Mbits" in c:
            print(f"    AMB: {c['AMB_now_Mbits']:.3f} Mbits → {c.get('AMB_koop_Kbits', '?'):.2f} Kbits/token")

        rows.append({
            "layer_idx": layer_idx,
            "PR": a.get("participation_ratio"),
            "spectral_decay_alpha": a.get("spectral_decay_alpha"),
            "T4_gain": t4,
            "T3_gain_raw": t3_raw,
            "T3_gain_lw": t3_lw,
            "r_K": c.get("r_K"),
            "kappa": c.get("kappa_G_phi"),
            "koopman_gain_net": gain,
            "AMB_now_Mbits": c.get("AMB_now_Mbits"),
            "AMB_koop_Kbits": c.get("AMB_koop_Kbits"),
            "damage_ratio": dr,
            "verdict": d.get("verdict", "?"),
        })

    return rows


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
    out_base   = os.path.join(args.out_dir, f"koopman_v2_{model_slug}_{timestamp}")

    layer_indices = (
        [int(x) for x in args.layers.split(",")]
        if args.layers else [args.layer_idx]
    )
    n_jvp_pts = args.n_jvp_pts if args.n_jvp_pts else args.n_samples

    print(f"\n{'='*70}")
    print(f"  Koopman Experiments v2 — BonfyreFPQ / Ember")
    print(f"  Model   : {args.model}")
    print(f"  Layers  : {layer_indices}")
    print(f"  Device  : {args.device}")
    print(f"  N       : {args.n_samples}  k_sketch={args.k_sketch}  n_jvp_pts={n_jvp_pts}")
    print(f"  bpw     : {args.bpw}  b_psi={args.b_psi}")
    print(f"{'='*70}\n")

    print("Loading model...")
    dtype     = torch.float32
    tokenizer = AutoTokenizer.from_pretrained(args.model)
    model     = AutoModelForCausalLM.from_pretrained(
        args.model, dtype=dtype, low_cpu_mem_usage=True
    ).to(args.device)
    model.eval()

    d        = model.config.hidden_size
    n_layers = model.config.num_hidden_layers
    print(f"  d={d}, L={n_layers} layers")

    # ── Collect activations (Exp A/B use MLP-level, Exp C uses per-layer JVPs)
    activations = collect_activations(
        model, tokenizer, layer_indices,
        args.n_samples, args.device, args.max_seq_len
    )

    t_start = time.time()

    results_A = experiment_A(activations, r_cutoff=args.r_cutoff)
    results_B = experiment_B(activations, r_cutoff=args.r_cutoff)

    # For Exp C, slice activations to n_jvp_pts
    acts_jvp  = {
        idx: {"pre": acts["pre"][:n_jvp_pts], "post": acts["post"]}
        for idx, acts in activations.items()
    }
    results_C = experiment_C(
        model, acts_jvp, layer_indices,
        device=args.device,
        k_sketch=args.k_sketch,
        r_cutoff=args.r_cutoff,
        bpw=args.bpw,
        b_psi=args.b_psi,
    )
    results_D = experiment_D(
        model, layer_indices,
        k_circuit_pct=args.k_circuit_pct,
        bits=4,
    )

    summary = build_summary(results_A, results_B, results_C, results_D,
                             bpw=args.bpw, b_psi=args.b_psi)

    elapsed = time.time() - t_start
    print(f"\n  Total experiment time: {elapsed:.1f}s")

    # ── Save JSON
    output = {
        "meta": {
            "version": 2,
            "model": args.model,
            "layers": layer_indices,
            "n_samples": args.n_samples,
            "k_sketch": args.k_sketch,
            "n_jvp_pts": n_jvp_pts,
            "b_psi": args.b_psi,
            "bpw": args.bpw,
            "d": d,
            "n_layers": n_layers,
            "device": args.device,
            "timestamp": timestamp,
            "elapsed_s": round(elapsed, 1),
            "notes": (
                "v2: Two-NN removed (unreliable at N=300, d=2048); "
                "Exp C uses JVP oracle (single backward per sketch direction); "
                "AMB formula separates dynamic token cost from static mode storage; "
                "Exp D uses actual SVD-based circuit rank and damage ratio vs "
                "uniform-noise baseline."
            ),
        },
        "experiment_A": results_A,
        "experiment_B": results_B,
        "experiment_C": results_C,
        "experiment_D": results_D,
        "summary": summary,
    }

    json_path = out_base + ".json"
    with open(json_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n  Results saved: {json_path}")

    md_path = out_base + ".md"
    _write_report(output, md_path)
    print(f"  Report saved:  {md_path}")

    return output


def _write_report(data, path):
    meta = data["meta"]
    lines = [
        f"# Koopman Experiments v2 — {meta['model']}",
        f"**Date**: {meta['timestamp']}  |  **Device**: {meta['device']}  "
        f"|  **Elapsed**: {meta['elapsed_s']}s",
        f"**Model**: d={meta['d']}, L={meta['n_layers']} layers  "
        f"|  **N**: {meta['n_samples']}  k_sketch={meta['k_sketch']}  "
        f"bpw={meta['bpw']}  b_psi={meta['b_psi']}\n",
        "## Summary: Koopman AMB Reduction\n",
        "| Layer | PR | α | T3 gain | T4 gain | r_K | κ(G) | Koop gain | AMB now | AMB koop | DR |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]

    for row in data["summary"]:
        t3_raw = row.get('T3_gain_raw', row.get('T3_gain', '?'))
        t3_lw  = row.get('T3_gain_lw', '?')
        lines.append(
            f"| {row['layer_idx']} "
            f"| {row.get('PR', '?')} "
            f"| {row.get('spectral_decay_alpha', '?')} "
            f"| {t3_raw}x({t3_lw}x LW) "
            f"| {row['T4_gain']}x "
            f"| {row.get('r_K', '?')} "
            f"| {row.get('kappa', '?')} "
            f"| {row['koopman_gain_net']}x "
            f"| {row.get('AMB_now_Mbits', '?')} Mb "
            f"| {row.get('AMB_koop_Kbits', '?')} Kb "
            f"| {row['damage_ratio']} |"
        )

    lines += [
        "\n## Experiment A: Intrinsic Dimension\n",
        "| Layer | k_PCA | PR | α | k@50% | k@80% | k@90% | k@95% | k@99% | T4 gain |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for idx, r in data["experiment_A"].items():
        lines.append(
            f"| {idx} | {r['k_pca']} | {r['participation_ratio']} "
            f"| {r['spectral_decay_alpha']} "
            f"| {r.get('k_50pct','?')} | {r.get('k_80pct','?')} "
            f"| {r.get('k_90pct','?')} | {r.get('k_95pct','?')} "
            f"| {r.get('k_99pct','?')} | {r['T4_AMB_gain']}x |"
        )

    lines += [
        "\n## Experiment B: Input Covariance Rank (Ledoit-Wolf shrunk)\n",
        "| Layer | rho | rho/d % | Top-5% var | k@90% | k@95% | k@99% | T3 WF | T3 flat |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for idx, r in data["experiment_B"].items():
        rho_raw = r.get('rho_raw', r.get('rho_eff', '?'))
        rho_lw  = r.get('rho_lw', '?')
        rho_frac = r.get('rho_lw_fraction', r.get('rho_fraction', 0))
        t3_raw = r.get('T3_AMB_gain_raw', r.get('T3_AMB_gain_waterfill', '?'))
        t3_lw  = r.get('T3_AMB_gain_lw', '?')
        lines.append(
            f"| {idx} | {rho_raw}(raw) / {rho_lw}(LW) | {rho_frac*100:.2f}% "
            f"| {r.get('top5pct_variance', 0)*100:.1f}% "
            f"| {r.get('k_eff_90','?')} | {r.get('k_eff_95','?')} | {r.get('k_eff_99','?')} "
            f"| {t3_raw}x raw / {t3_lw}x LW | {r.get('T3_AMB_gain_flatrate','?')}x |"
        )

    lines += [
        "\n## Experiment C: Koopman Rank (JVP Oracle)\n",
        "**v2 fix**: r_K no longer sample-bounded. Used JVP oracle (n_sketch_vectors = n_pts × k_sketch).",
        "**AMB formula**: AMB_now = d_in × d_out × bpw;  AMB_koop = r_K × b_psi (modes are static).\n",
        "| Layer | d_in | d_out | n_sketch | r_K | r_K/d | κ(G) | AMB now | AMB koop | Gain raw | Gain net |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for idx, r in data["experiment_C"].items():
        lines.append(
            f"| {idx} | {r['d_in']} | {r['d_out']} | {r['n_sketch_vectors']} "
            f"| {r['r_K']} | {r['r_K_fraction']*100:.1f}% "
            f"| {r['kappa_G_phi']} | {r['AMB_now_Mbits']} Mb "
            f"| {r['AMB_koop_Kbits']} Kb "
            f"| {r['AMB_gain_raw']}x | {r['AMB_gain_net']}x |"
        )

    lines += [
        "\n## Experiment D: Circuit Damage Ratio\n",
        "**v2 fix**: Uses SVD of W directly; actual 4-bit scalar quantization error;",
        "damage ratio = (error in circuit) / (k_c/d_in baseline).  DR<1 → circuit-safe.\n",
        "| Layer | k_c | k_c/d_in | Energy@k_c | f_circuit | baseline | DR | Koop DR | Verdict |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for idx, r in data["experiment_D"].items():
        if "skipped" in r:
            lines.append(f"| {idx} | skipped | | | | | | | |")
            continue
        lines.append(
            f"| {idx} | {r['k_c']} | {r['k_c_fraction_din']*100:.2f}% "
            f"| {r['energy_in_kc']*100:.1f}% "
            f"| {r['f_circuit']*100:.1f}% | {r['baseline']*100:.2f}% "
            f"| {r['damage_ratio']} | {r['koopman_ideal_DR']} | {r['verdict']} |"
        )

    lines += [
        "\n## Theory Hierarchy\n",
        "| Method | per-token AMB | Gain mechanism |",
        "|---|---|---|",
        f"| Current FPQ v12 | d² × bpw = d² × {data['meta']['bpw']} bits | — |",
        "| T4 manifold | ≈ PR × bpw bits | k_intrinsic / d compression |",
        "| T3 distribution | ≈ rho × bpw bits | water-filling over Σ_x eigenvalues |",
        "| Koopman (T8) | r_K × b_psi bits | static modes; dynamic evals only |",
        "| Koopman net | r_K × b_psi × κ bits | interference penalty |",
        "| T6 SGD channel | 0.0015 bpw floor | information in training |",
        "",
        "**Key v2 insight**: Previous 4x gain was wrong (sample-bounded r_K=70,",
        "and the AMB formula included mode storage as per-token cost).",
        "Correct formula: Gain = d_in × d_out × bpw / (r_K × b_psi).",
        "With d=2048, b_psi=16: for r_K=100 → gain = 2048² × 6.55 / (100 × 16) = **17,200x**",
        "For r_K=500 (if true rank is higher) → still **3,400x**.",
    ]

    with open(path, "w") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()
