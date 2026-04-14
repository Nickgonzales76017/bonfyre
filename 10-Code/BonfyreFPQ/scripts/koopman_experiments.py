#!/usr/bin/env python3
"""
Koopman Operator Field Experiments
===================================
Three experiments that bound the Active Memory Bandwidth (AMB) reduction
achievable via Koopman decomposition of transformer layers.

Theory background (KOOPMAN_THEORY.md):
  - T4 (manifold):  15x AMB reduction if k_intrinsic ~ 50
  - T3 (distribution): 20-100x if rho(Sigma_x) / n ~ 0.01-0.05
  - Koopman basis:  3000-15000x combined if r_K ~ 20-50 per layer

Running:
  python3 scripts/koopman_experiments.py \
      --model TinyLlama/TinyLlama-1.1B-Chat-v1.0 \
      --n-samples 500 \
      --layer-idx 8 \
      --device mps

Output:
  results/koopman_<model>_<timestamp>.json
  results/koopman_<model>_<timestamp>.md   (human-readable report)
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
    p.add_argument("--n-samples", type=int, default=500,
                   help="Number of token activations to collect per layer")
    p.add_argument("--layer-idx", type=int, default=8,
                   help="Primary layer to probe (MLP/FFN down-proj)")
    p.add_argument("--layers", type=str, default=None,
                   help="Comma-separated layer indices to sweep (overrides --layer-idx)")
    p.add_argument("--device", default="cpu")
    p.add_argument("--r-cutoff", type=float, default=0.99,
                   help="Explained variance cutoff for intrinsic rank estimates")
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
    PCA on activation vectors. Intrinsic dim = number of PCs needed
    to explain r_cutoff fraction of variance. Bounds T4 manifold compression.

    If k_intrinsic ~ 50 for d=2048:  T4 gives ~15x AMB reduction
    If k_intrinsic ~ 200:            ~4x reduction
    """
    print("\n[Experiment A] Intrinsic dimension of activation manifold")

    results = {}
    for layer_idx, acts in activations.items():
        X = acts["pre"]                             # (N, d)
        N, d = X.shape

        # Center
        mu = X.mean(0)
        X_c = X - mu

        # SVD (economy)
        U, S, Vh = torch.linalg.svd(X_c, full_matrices=False)
        var = (S ** 2) / (S ** 2).sum()
        cumvar = var.cumsum(0)

        k_cutoff = int((cumvar < r_cutoff).sum().item()) + 1
        k_cutoff = min(k_cutoff, d)

        # Participation ratio (alternate rank measure, more robust)
        pr = (S.sum() ** 2) / (S ** 2).sum()

        # Two-NN intrinsic dimension estimator (Facco 2017)
        k_twonn = _twonn_estimate(X[:min(500, N)])

        print(f"  Layer {layer_idx}: d={d}, N={N}")
        print(f"    PCA k@{r_cutoff*100:.0f}%var = {k_cutoff}  ({k_cutoff/d*100:.1f}% of d)")
        print(f"    Participation ratio   = {pr.item():.1f}")
        print(f"    Two-NN intrinsic dim  = {k_twonn:.1f}")

        # AMB prediction from T4
        T4_gain = d / k_cutoff
        print(f"    T4 manifold gain      = {T4_gain:.1f}x")

        results[layer_idx] = {
            "d": d, "N": N,
            "k_pca": k_cutoff,
            "k_pca_fraction": round(k_cutoff / d, 4),
            "participation_ratio": round(pr.item(), 2),
            "k_twonn": round(k_twonn, 1),
            "T4_AMB_gain": round(T4_gain, 1),
        }

    return results


def _twonn_estimate(X):
    """
    Two-NN intrinsic dimension estimator (Facco et al. 2017).
    Samples up to 500 points for speed.
    """
    X = X.double()
    N = X.shape[0]
    if N < 10:
        return float("nan")

    mulist = []
    for i in range(N):
        dists = ((X - X[i]) ** 2).sum(1).sqrt()
        dists[i] = float("inf")
        sorted_d = dists.sort().values
        r1, r2 = sorted_d[0].item(), sorted_d[1].item()
        if r1 > 0:
            mulist.append(r2 / r1)

    mulist = sorted(mulist)
    n2 = len(mulist)
    # Empirical CDF
    cum = [(i + 1) / n2 for i in range(n2)]
    # Fit: ln(mu) ~ -ln(1-F) => slope = d
    lnmu = [np.log(m) for m in mulist if m > 1]
    lncum = [np.log(1 - c + 1e-10) for c in cum[:len(lnmu)]]
    if len(lnmu) < 5:
        return float("nan")

    # Linear regression through origin
    xs = np.array(lnmu)
    ys = np.array([-c for c in lncum])
    d_est = float(np.dot(xs, ys) / np.dot(xs, xs))
    return d_est

# ──────────────────────────────────────────────
#  Experiment B: Effective rank of Sigma_x (bounds T3)
# ──────────────────────────────────────────────

def experiment_B(activations, r_cutoff=0.99):
    """
    Measure effective rank rho(Sigma_x) = tr(Sigma_x) / ||Sigma_x||_op
    and the fraction of input variance in the top-5% directions.

    rho/d ~ 0.01-0.05  =>  T3 distribution gain 20-100x
    """
    print("\n[Experiment B] Effective rank of input covariance Sigma_x")

    results = {}
    for layer_idx, acts in activations.items():
        X = acts["pre"]
        N, d = X.shape
        mu = X.mean(0)
        X_c = (X - mu).double()

        Sigma = (X_c.T @ X_c) / (N - 1)          # (d, d)

        eigvals = torch.linalg.eigvalsh(Sigma)     # ascending
        eigvals = eigvals.flip(0).clamp(min=0)     # descending, non-negative

        trace  = eigvals.sum().item()
        op_norm = eigvals[0].item()
        rho    = trace / op_norm if op_norm > 0 else float("nan")
        rho_frac = rho / d

        # Fraction of variance in top-5% directions
        top5_k = max(1, int(0.05 * d))
        top5_var = eigvals[:top5_k].sum().item() / trace if trace > 0 else 0

        # Effective rank by cumulative variance
        cumvar = eigvals.cumsum(0) / trace
        k_eff  = int((cumvar < r_cutoff).sum().item()) + 1

        T3_gain_lo = 1 / max(rho_frac, 1e-6)      # 1/rho fraction
        T3_gain_hi = T3_gain_lo                    # same metric, water-filling upper bound

        print(f"  Layer {layer_idx}:")
        print(f"    rho(Sigma_x)          = {rho:.1f}  ({rho_frac*100:.2f}% of d)")
        print(f"    Top-5% dirs capture   = {top5_var*100:.1f}% variance")
        print(f"    k_eff @{r_cutoff*100:.0f}%var        = {k_eff}")
        print(f"    T3 distribution gain  = {T3_gain_lo:.1f}x")

        results[layer_idx] = {
            "rho": round(rho, 2),
            "rho_fraction": round(rho_frac, 6),
            "top5pct_variance": round(top5_var, 4),
            "k_eff": k_eff,
            "T3_AMB_gain": round(T3_gain_lo, 1),
        }

    return results

# ──────────────────────────────────────────────
#  Experiment C: Koopman rank (bounds T4 + T8)
# ──────────────────────────────────────────────

def experiment_C(model, activations, layer_indices, device, n_jacobian=100, r_cutoff=0.99):
    """
    Compute Jacobians of each layer's MLP at sampled activation points.
    PCA on Jacobians → Koopman rank r_K.
    Gram matrix → interference penalty kappa(G).

    r_K ~ 20-50  =>  Koopman AMB gain 3000-15000x
    """
    print("\n[Experiment C] Koopman operator rank via Jacobian PCA")

    results = {}

    for layer_idx in layer_indices:
        acts = activations[layer_idx]
        X    = acts["pre"]
        N, d = X.shape         # d = hidden_size (e.g. 2048)
        n_jac = min(n_jacobian, N)

        print(f"  Layer {layer_idx}: computing {n_jac} Jacobians (d={d})...")

        layer = model.model.layers[layer_idx]
        mlp   = layer.mlp

        # Move mlp to device for Jacobian computation, but keep activations on CPU
        jacobians = []
        t0 = time.time()
        for i in range(n_jac):
            x = X[i].clone().to(device).float()   # (d,) hidden state

            try:
                # Full Jacobian of mlp: R^d -> R^d
                def fwd(z):
                    return mlp(z.unsqueeze(0)).squeeze(0)

                J = torch.func.jacrev(fwd)(x)   # (d, d)
                jacobians.append(J.detach().float().cpu())
            except Exception:
                # Fallback: finite difference (slow but works everywhere)
                with torch.no_grad():
                    base = mlp(x.unsqueeze(0)).squeeze(0)
                d_out = base.shape[0]
                J = torch.zeros(d_out, d)
                eps = 1e-3
                for j in range(d):
                    xp = x.clone()
                    xp[j] += eps
                    with torch.no_grad():
                        delta = (mlp(xp.unsqueeze(0)).squeeze(0) - base) / eps
                    J[:, j] = delta.cpu()
                jacobians.append(J)

            if (i + 1) % 20 == 0:
                print(f"    {i+1}/{n_jac} done ({time.time()-t0:.1f}s)")

        # Stack Jacobians: (n_jac, d_out * d_in)
        d_out = jacobians[0].shape[0]
        J_mat = torch.stack([J.reshape(-1) for J in jacobians], dim=0)  # (n_jac, d_out*d_in)
        print(f"    Jacobian matrix: {J_mat.shape}")

        # PCA on Jacobians
        mu_J  = J_mat.mean(0)
        J_c   = J_mat - mu_J
        _, S, Vh = torch.linalg.svd(J_c, full_matrices=False)

        var    = (S ** 2) / (S ** 2).sum()
        cumvar = var.cumsum(0)
        r_K    = int((cumvar < r_cutoff).sum().item()) + 1
        r_K    = min(r_K, n_jac)

        print(f"    Koopman rank r_K @{r_cutoff*100:.0f}%var = {r_K}")

        # Gram matrix G of top r_K Koopman eigenfunctions
        # Approximate: project each Jacobian onto top-r_K PCA basis
        Phi = Vh[:r_K]                              # (r_K, d_out*d_in) — basis vectors
        coords = J_c @ Phi.T                        # (n_jac, r_K) — eigenfunction evals
        G = (coords.T @ coords) / n_jac             # (r_K, r_K) Gram matrix

        # Normalize G so diagonal ~ 1
        diag_inv = (G.diag().clamp(min=1e-12)).sqrt().reciprocal()
        G_norm = G * diag_inv.unsqueeze(0) * diag_inv.unsqueeze(1)

        eigG  = torch.linalg.eigvalsh(G_norm).abs()
        kappa = (eigG.max() / eigG.clamp(min=1e-12).min()).item()

        print(f"    Gram condition number kappa(G) = {kappa:.2f}")

        # Koopman mode rank: SVD of each basis vector reshaped as (d_out, d_in)
        mode_ranks = []
        for k in range(min(r_K, 10)):
            mode_mat = Phi[k].reshape(d_out, d)
            sv = torch.linalg.svdvals(mode_mat)
            sv_norm = sv / sv.sum()
            r_mode = int((sv_norm.cumsum(0) < r_cutoff).sum().item()) + 1
            mode_ranks.append(r_mode)

        r_mode_mean = float(np.mean(mode_ranks))
        print(f"    Mean Koopman mode rank = {r_mode_mean:.1f} (top-10 modes)")

        # AMB prediction
        b_psi   = np.log2(1e4) + np.log2(kappa)   # bits per eigenfunction eval
        amb_koop = r_K * b_psi + r_K * r_mode_mean * d * 6.55 / d
        amb_fpq  = 0.125 * d * d * 6.55           # beta * d^2 * bpw per layer

        koopman_gain = amb_fpq / amb_koop if amb_koop > 0 else float("inf")
        koopman_gain_with_interference = koopman_gain / kappa

        print(f"    Koopman AMB gain (no interference)   = {koopman_gain:.0f}x")
        print(f"    Koopman AMB gain (with interference) = {koopman_gain_with_interference:.0f}x")

        results[layer_idx] = {
            "r_K": r_K,
            "kappa_G": round(kappa, 2),
            "r_mode_mean": round(r_mode_mean, 1),
            "koopman_gain_ideal": round(koopman_gain, 0),
            "koopman_gain_realistic": round(koopman_gain_with_interference, 0),
        }

    return results


# ──────────────────────────────────────────────
#  Experiment D: Error concentration (bounds T2)
# ──────────────────────────────────────────────

def experiment_D(model, tokenizer, activations, layer_indices, device, r_cutoff=0.99):
    """
    Measure what fraction of FPQ-like quantization error falls OUTSIDE
    the top-k right singular vectors of W·Sigma_x^(1/2).

    If >90% of error is outside the active subspace → circuit-preserving.
    Uses down_proj weight (d_out=d, d_in=d_intermediate) and collects
    down_proj-specific activations for the covariance.
    """
    print("\n[Experiment D] Circuit error concentration (T2 bound)")
    results = {}

    # Collect down_proj-specific activations (d_intermediate ~ 5632 for TinyLlama)
    dp_acts = {idx: [] for idx in layer_indices}
    dp_total = {idx: 0 for idx in layer_indices}
    n_needed = max(a["pre"].shape[0] for a in activations.values())
    dp_hooks = []

    def make_dp_hook(idx):
        def hook(module, inp):
            if dp_total[idx] >= n_needed:
                return
            x = inp[0].detach().float()
            flat = x.reshape(-1, x.shape[-1])
            dp_acts[idx].append(flat.cpu())
            dp_total[idx] += flat.shape[0]
        return hook

    for idx in layer_indices:
        dp = model.model.layers[idx].mlp.down_proj
        dp_hooks.append(dp.register_forward_pre_hook(make_dp_hook(idx)))

    # One forward pass to collect
    tokenizer_d = tokenizer
    chunks = get_wikitext_tokens(tokenizer_d, n_sentences=100)
    model.eval()
    with torch.no_grad():
        for chunk in chunks:
            if all(dp_total[idx] >= n_needed for idx in layer_indices):
                break
            try:
                model(chunk.to(device))
            except Exception:
                continue

    for h in dp_hooks:
        h.remove()

    for layer_idx in layer_indices:
        W = model.model.layers[layer_idx].mlp.down_proj.weight.detach().float().cpu()
        d_out, d_in = W.shape

        if not dp_acts[layer_idx]:
            print(f"  Layer {layer_idx}: no down_proj activations collected, skipping")
            results[layer_idx] = {"skipped": True}
            continue

        X = torch.cat(dp_acts[layer_idx], dim=0)[:n_needed]  # (N, d_in)
        N = X.shape[0]

        mu = X.mean(0)
        X_c = (X - mu).double()
        Sigma = (X_c.T @ X_c) / (N - 1)
        eigvals, eigvecs = torch.linalg.eigh(Sigma.float())
        eigvals = eigvals.flip(0).clamp(min=0)
        eigvecs = eigvecs.flip(1)

        k_cov = int((eigvals.cumsum(0) / eigvals.sum() < r_cutoff).sum().item()) + 1
        Sigma_sqrt = eigvecs[:, :k_cov] * eigvals[:k_cov].sqrt().unsqueeze(0)

        WS = W @ Sigma_sqrt
        _, Sv, _ = torch.linalg.svd(WS, full_matrices=False)
        var_WS = (Sv ** 2) / (Sv ** 2).sum()
        k_active = int((var_WS.cumsum(0) < r_cutoff).sum().item()) + 1

        q_step = W.abs().max().item() / (2**3)
        E = (torch.rand_like(W) - 0.5) * q_step

        _, _, Vh_WS = torch.linalg.svd(WS, full_matrices=False)
        Vk = Vh_WS[:k_active]

        E_proj = E @ Sigma_sqrt
        E_active   = E_proj @ Vk.T @ Vk
        E_inactive = E_proj - E_active

        frac_active = (E_active ** 2).sum().item() / ((E_proj ** 2).sum().item() + 1e-12)

        print(f"  Layer {layer_idx}: down_proj d_in={d_in}, k_active={k_active}")
        print(f"    Error fraction in active subspace = {frac_active*100:.1f}%")
        print(f"    Error fraction outside (safe)     = {(1-frac_active)*100:.1f}%")
        if frac_active < 0.10:
            print(f"    → Circuit-preserving (>90% error in null space) ✓")
        else:
            print(f"    → Caution: non-trivial error in circuit subspace")

        results[layer_idx] = {
            "k_active": k_active,
            "error_frac_active": round(frac_active, 4),
            "error_frac_outside": round(1 - frac_active, 4),
            "circuit_preserving": frac_active < 0.10,
        }

    return results


# ──────────────────────────────────────────────
#  Summary: AMB reduction table
# ──────────────────────────────────────────────

def build_summary(exp_a, exp_b, exp_c, exp_d, d, b_fpq=6.55):
    print("\n" + "═"*60)
    print("  ACTIVE MEMORY BANDWIDTH REDUCTION SUMMARY")
    print("═"*60)

    rows = []
    for layer_idx in exp_a:
        a = exp_a[layer_idx]
        b = exp_b.get(layer_idx, {})
        c = exp_c.get(layer_idx, {})
        dd = exp_d.get(layer_idx, {})

        t4   = a.get("T4_AMB_gain", "?")
        t3   = b.get("T3_AMB_gain", "?")
        koop = c.get("koopman_gain_realistic", "?")
        circ = dd.get("circuit_preserving", "?")

        # Combined realistic estimate
        if all(isinstance(x, (int, float)) for x in [t4, t3, koop]):
            # Don't multiply — these are nested, tightest bound wins
            combined = koop  # Koopman subsumes T3 and T4
        else:
            combined = "?"

        # Current AMB per layer (tokens)
        amb_now  = 0.125 * d * d * b_fpq         # bits, beta * d^2 * bpw
        amb_then = amb_now / combined if isinstance(combined, (int,float)) and combined > 0 else "?"

        print(f"\n  Layer {layer_idx}:")
        print(f"    T4 manifold gain         : {t4}x")
        print(f"    T3 distribution gain     : {t3}x")
        print(f"    Koopman gain (realistic) : {koop}x")
        print(f"    Circuit-preserving       : {circ}")
        print(f"    Current AMB/layer        : {amb_now/1e6:.2f} Mbits")
        if isinstance(amb_then, float):
            print(f"    Koopman AMB/layer        : {amb_then/1e3:.1f} Kbits")
        else:
            print(f"    Koopman AMB/layer        : ?")

        rows.append({
            "layer_idx": layer_idx,
            "T4_gain": t4,
            "T3_gain": t3,
            "koopman_gain": koop,
            "circuit_preserving": circ,
            "AMB_now_Mbits": round(amb_now/1e6, 3),
            "AMB_koopman_Kbits": round(amb_then/1e3, 1) if isinstance(amb_then, float) else None,
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
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_slug = args.model.replace("/", "_")
    out_base = os.path.join(args.out_dir, f"koopman_{model_slug}_{timestamp}")

    layer_indices = (
        [int(x) for x in args.layers.split(",")]
        if args.layers else [args.layer_idx]
    )

    print(f"\n{'='*60}")
    print(f"  Koopman Experiments — BonfyreFPQ / Ember")
    print(f"  Model  : {args.model}")
    print(f"  Layers : {layer_indices}")
    print(f"  Device : {args.device}")
    print(f"  N      : {args.n_samples}")
    print(f"{'='*60}\n")

    # ── Load model
    print("Loading model...")
    dtype = torch.float32
    tokenizer = AutoTokenizer.from_pretrained(args.model)
    model = AutoModelForCausalLM.from_pretrained(
        args.model, torch_dtype=dtype, low_cpu_mem_usage=True
    ).to(args.device)
    model.eval()

    d = model.config.hidden_size
    n_layers = model.config.num_hidden_layers
    print(f"  Architecture: d={d}, L={n_layers} layers")

    # ── Collect activations
    activations = collect_activations(
        model, tokenizer, layer_indices,
        args.n_samples, args.device, args.max_seq_len
    )

    # ── Run experiments
    t_start = time.time()

    results_A = experiment_A(activations, r_cutoff=args.r_cutoff)
    results_B = experiment_B(activations, r_cutoff=args.r_cutoff)
    results_C = experiment_C(
        model, activations, layer_indices,
        device=args.device, n_jacobian=80, r_cutoff=args.r_cutoff
    )
    results_D = experiment_D(
        model, tokenizer, activations, layer_indices,
        device=args.device, r_cutoff=args.r_cutoff
    )

    summary = build_summary(results_A, results_B, results_C, results_D, d=d)

    elapsed = time.time() - t_start
    print(f"\n  Total experiment time: {elapsed:.1f}s")

    # ── Save JSON
    output = {
        "meta": {
            "model": args.model,
            "layers": layer_indices,
            "n_samples": args.n_samples,
            "d": d,
            "n_layers": n_layers,
            "device": args.device,
            "timestamp": timestamp,
            "elapsed_s": round(elapsed, 1),
        },
        "experiment_A": results_A,
        "experiment_B": results_B,
        "experiment_C": results_C,
        "experiment_D": results_D,
        "summary": summary,
        "theory_context": {
            "T1_hessian_gain": "100x (p/k_H, k_H/p~0.01)",
            "T2_circuit_gain": "20-50x (n/k_C, k_C~2048/32)",
            "T3_dist_gain": "see experiment_B.T3_AMB_gain",
            "T4_manifold_gain": "see experiment_A.T4_AMB_gain",
            "T5_von_neumann_floor_bpw": {
                "whisper_cross_attn": 1.70,
                "qwen_attn_kq": 4.50,
                "ffn_down": 10.50,
            },
            "T6_sgd_channel_bpw": 0.0015,
            "T7_compression_cliff_kv_factor": 78,
            "T8_koopman_gain": "see experiment_C.koopman_gain_realistic",
        },
    }

    json_path = out_base + ".json"
    with open(json_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n  Results saved: {json_path}")

    # ── Save Markdown report
    md_path = out_base + ".md"
    _write_report(output, md_path)
    print(f"  Report saved:  {md_path}")

    return output


def _write_report(data, path):
    meta = data["meta"]
    lines = [
        f"# Koopman Experiments — {meta['model']}",
        f"**Date**: {meta['timestamp']}  |  **Device**: {meta['device']}  "
        f"|  **Elapsed**: {meta['elapsed_s']}s",
        f"**Model**: d={meta['d']}, L={meta['n_layers']} layers  "
        f"|  **N samples**: {meta['n_samples']}\n",
        "## Summary: Active Memory Bandwidth Reduction\n",
        "| Layer | T3 dist | T4 manifold | Koopman (realistic) | Circuit-safe | AMB now | AMB Koopman |",
        "|---|---|---|---|---|---|---|",
    ]

    for row in data["summary"]:
        lines.append(
            f"| {row['layer_idx']} "
            f"| {row['T3_gain']}x "
            f"| {row['T4_gain']}x "
            f"| {row['koopman_gain']}x "
            f"| {row['circuit_preserving']} "
            f"| {row['AMB_now_Mbits']} Mb "
            f"| {row['AMB_koopman_Kbits']} Kb |"
        )

    lines += [
        "\n## Experiment A: Intrinsic Dimension\n",
        "| Layer | k_PCA | k/d % | PticipRatio | TwoNN | T4 gain |",
        "|---|---|---|---|---|---|",
    ]
    for idx, r in data["experiment_A"].items():
        lines.append(
            f"| {idx} | {r['k_pca']} | {r['k_pca_fraction']*100:.1f}% "
            f"| {r['participation_ratio']} | {r['k_twonn']} | {r['T4_AMB_gain']}x |"
        )

    lines += [
        "\n## Experiment B: Input Covariance Rank\n",
        "| Layer | rho | rho/d % | Top-5% var | k_eff | T3 gain |",
        "|---|---|---|---|---|---|",
    ]
    for idx, r in data["experiment_B"].items():
        lines.append(
            f"| {idx} | {r['rho']} | {r['rho_fraction']*100:.2f}% "
            f"| {r['top5pct_variance']*100:.1f}% | {r['k_eff']} | {r['T3_AMB_gain']}x |"
        )

    lines += [
        "\n## Experiment C: Koopman Rank\n",
        "| Layer | r_K | kappa(G) | Mode rank | Gain (ideal) | Gain (realistic) |",
        "|---|---|---|---|---|---|",
    ]
    for idx, r in data["experiment_C"].items():
        lines.append(
            f"| {idx} | {r['r_K']} | {r['kappa_G']} | {r['r_mode_mean']} "
            f"| {r['koopman_gain_ideal']}x | {r['koopman_gain_realistic']}x |"
        )

    lines += [
        "\n## Experiment D: Circuit Error Concentration\n",
        "| Layer | k_active | Error in active | Error outside | Circuit-safe |",
        "|---|---|---|---|---|",
    ]
    for idx, r in data["experiment_D"].items():
        lines.append(
            f"| {idx} | {r['k_active']} | {r['error_frac_active']*100:.1f}% "
            f"| {r['error_frac_outside']*100:.1f}% | {r['circuit_preserving']} |"
        )

    lines += [
        "\n## Theoretical Hierarchy\n",
        "| Method | bpw floor | Notes |",
        "|---|---|---|",
        "| Current FPQ v12 | 6.55 | E8+RVQ+rANS |",
        "| T4 manifold | ~0.44 | d/k_intrinsic gain |",
        "| T3 distribution | ~0.07–0.33 | water-filling Sigma_x |",
        "| T8 Koopman | ~0.001–0.002 | r_K modes + interference |",
        "| T6 SGD channel | 0.0015 | training info floor |",
        "| T7 info floor | 0.00000074 | I_min per token |",
        "",
        "**Key derived numbers:**",
        "- KV compression cliff: 2.5-bit floor (beyond = incoherent)",
        "- KV errors 78x more costly per cosine unit than weight errors",
        "- Whisper cross-attn von Neumann floor: 1.70 bpw",
        "- FFN_down von Neumann floor: ~10.5 bpw (incompressible statically)",
        "- Koopman decomposition closes FFN_down from 10.5 to ~3.9 bpw/program",
    ]

    with open(path, "w") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()
