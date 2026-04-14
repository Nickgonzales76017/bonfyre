# Koopman Experiments v2 — TinyLlama/TinyLlama-1.1B-Chat-v1.0
**Date**: 20260414_082354  |  **Device**: mps  |  **Elapsed**: 660.5s
**Model**: d=2048, L=22 layers  |  **N**: 300  k_sketch=32  bpw=6.55  b_psi=16

## Summary: Koopman AMB Reduction

| Layer | PR | α | T3 gain | T4 gain | r_K | κ(G) | Koop gain | AMB now | AMB koop | DR |
|---|---|---|---|---|---|---|---|---|---|---|
| 4 | 224.8 | 0.3311 | 79.9x(1.3x LW) | 9.1x | 1920 | 65.86 | 6128.0x | 226.6497 Mb | 30.72 Kb | 7.4353 |
| 8 | 215.09 | 0.3931 | 96.6x(1.3x LW) | 9.5x | 1891 | 179.5 | 5787.0x | 226.6497 Mb | 30.256 Kb | 10.4405 |
| 16 | 202.37 | 0.448 | 119.5x(1.4x LW) | 10.1x | 1871 | 151.72 | 5918.0x | 226.6497 Mb | 29.936 Kb | 8.7595 |

## Experiment A: Intrinsic Dimension

| Layer | k_PCA | PR | α | k@50% | k@80% | k@90% | k@95% | k@99% | T4 gain |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 252 | 224.8 | 0.3311 | 50 | 129 | 174 | 207 | 252 | 9.1x |
| 8 | 259 | 215.09 | 0.3931 | 39 | 117 | 167 | 205 | 259 | 9.5x |
| 16 | 261 | 202.37 | 0.448 | 30 | 103 | 155 | 199 | 261 | 10.1x |

## Experiment B: Input Covariance Rank (Ledoit-Wolf shrunk)

| Layer | rho | rho/d % | Top-5% var | k@90% | k@95% | k@99% | T3 WF | T3 flat |
|---|---|---|---|---|---|---|---|---|
| 4 | 25.62(raw) / 1621.71(LW) | 79.18% | 5.2% | 1843 | 1946 | 2028 | 79.9x raw / 1.3x LW | 1.0x |
| 8 | 21.2(raw) / 1553.45(LW) | 75.85% | 5.2% | 1843 | 1946 | 2028 | 96.6x raw / 1.3x LW | 1.0x |
| 16 | 17.14(raw) / 1468.66(LW) | 71.71% | 5.2% | 1843 | 1946 | 2028 | 119.5x raw / 1.4x LW | 1.0x |

## Experiment C: Koopman Rank (JVP Oracle)

**v2 fix**: r_K no longer sample-bounded. Used JVP oracle (n_sketch_vectors = n_pts × k_sketch).
**AMB formula**: AMB_now = d_in × d_out × bpw;  AMB_koop = r_K × b_psi (modes are static).

| Layer | d_in | d_out | n_sketch | r_K | r_K/d | κ(G) | AMB now | AMB koop | Gain raw | Gain net |
|---|---|---|---|---|---|---|---|---|---|---|
| 4 | 2048 | 2048 | 9600 | 1920 | 93.8% | 65.86 | 226.6497 Mb | 30.72 Kb | 7378.0x | 6128.0x |
| 8 | 2048 | 2048 | 9600 | 1891 | 92.3% | 179.5 | 226.6497 Mb | 30.256 Kb | 7491.0x | 5787.0x |
| 16 | 2048 | 2048 | 9600 | 1871 | 91.4% | 151.72 | 226.6497 Mb | 29.936 Kb | 7571.0x | 5918.0x |

## Experiment D: Circuit Damage Ratio

**v2 fix**: Uses SVD of W directly; actual 4-bit scalar quantization error;
damage ratio = (error in circuit) / (k_c/d_in baseline).  DR<1 → circuit-safe.

| Layer | k_c | k_c/d_in | Energy@k_c | f_circuit | baseline | DR | Koop DR | Verdict |
|---|---|---|---|---|---|---|---|---|
| 4 | 23 | 0.41% | 5.1% | 3.0% | 0.41% | 7.4353 | 0.0 | ✗ circuit-damaging (DR=7.44 > 1) — Koopman needed |
| 8 | 22 | 0.39% | 5.2% | 4.1% | 0.39% | 10.4405 | 0.0 | ✗ circuit-damaging (DR=10.44 > 1) — Koopman needed |
| 16 | 24 | 0.43% | 5.0% | 3.7% | 0.43% | 8.7595 | 0.0 | ✗ circuit-damaging (DR=8.76 > 1) — Koopman needed |

## Experiment E: Pre-down_proj Representation h (Tightest Bound)

**Direct PCA of h = silu(gate(x)) ⊙ up(x)**. No Jacobians needed.
**Full MLP AMB**: gate_proj + up_proj + down_proj = n_params × bpw.

| Layer | d_int | PR_h | r_h@90% | r_h@99% | α(h) | MP spikes | Gain@r90 | Gain@99 | Gain@MP |
|---|---|---|---|---|---|---|---|---|---|

## Theory Hierarchy

| Method | per-token AMB | Gain mechanism |
|---|---|---|
| Current FPQ v12 | d² × bpw = d² × 6.55 bits | — |
| T4 manifold | ≈ PR × bpw bits | k_intrinsic / d compression |
| T3 distribution | ≈ rho × bpw bits | water-filling over Σ_x eigenvalues |
| Koopman (T8) | r_K × b_psi bits | static modes; dynamic evals only |
| Koopman net | r_K × b_psi × κ bits | interference penalty |
| T6 SGD channel | 0.0015 bpw floor | information in training |

**Key v2 insight**: Previous 4x gain was wrong (sample-bounded r_K=70,
and the AMB formula included mode storage as per-token cost).
Correct formula: Gain = d_in × d_out × bpw / (r_K × b_psi).
With d=2048, b_psi=16: for r_K=100 → gain = 2048² × 6.55 / (100 × 16) = **17,200x**
For r_K=500 (if true rank is higher) → still **3,400x**.