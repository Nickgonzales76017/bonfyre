# Koopman Experiments — TinyLlama/TinyLlama-1.1B-Chat-v1.0
**Date**: 20260414_010302  |  **Device**: mps  |  **Elapsed**: 244.2s
**Model**: d=2048, L=22 layers  |  **N samples**: 300

## Summary: Active Memory Bandwidth Reduction

| Layer | T3 dist | T4 manifold | Koopman (realistic) | Circuit-safe | AMB now | AMB Koopman |
|---|---|---|---|---|---|---|
| 4 | 79.9x | 8.1x | 4.0x | False | 3.434 Mb | 858.5 Kb |
| 8 | 96.6x | 7.9x | 4.0x | False | 3.434 Mb | 858.5 Kb |
| 16 | 119.5x | 7.8x | 4.0x | False | 3.434 Mb | 858.5 Kb |

## Experiment A: Intrinsic Dimension

| Layer | k_PCA | k/d % | PticipRatio | TwoNN | T4 gain |
|---|---|---|---|---|---|
| 4 | 252 | 12.3% | 224.8 | 0.7 | 8.1x |
| 8 | 259 | 12.7% | 215.09 | 0.8 | 7.9x |
| 16 | 261 | 12.7% | 202.37 | 0.7 | 7.8x |

## Experiment B: Input Covariance Rank

| Layer | rho | rho/d % | Top-5% var | k_eff | T3 gain |
|---|---|---|---|---|---|
| 4 | 25.62 | 1.25% | 72.1% | 252 | 79.9x |
| 8 | 21.2 | 1.04% | 76.3% | 259 | 96.6x |
| 16 | 17.14 | 0.84% | 80.0% | 261 | 119.5x |

## Experiment C: Koopman Rank

| Layer | r_K | kappa(G) | Mode rank | Gain (ideal) | Gain (realistic) |
|---|---|---|---|---|---|
| 4 | 70 | 1.0 | 1780.0 | 4.0x | 4.0x |
| 8 | 71 | 1.0 | 1770.0 | 4.0x | 4.0x |
| 16 | 73 | 1.0 | 1767.7 | 4.0x | 4.0x |

## Experiment D: Circuit Error Concentration

| Layer | k_active | Error in active | Error outside | Circuit-safe |
|---|---|---|---|---|
| 4 | 244 | 98.4% | 1.6% | False |
| 8 | 251 | 98.2% | 1.8% | False |
| 16 | 249 | 98.4% | 1.6% | False |

## Theoretical Hierarchy

| Method | bpw floor | Notes |
|---|---|---|
| Current FPQ v12 | 6.55 | E8+RVQ+rANS |
| T4 manifold | ~0.44 | d/k_intrinsic gain |
| T3 distribution | ~0.07–0.33 | water-filling Sigma_x |
| T8 Koopman | ~0.001–0.002 | r_K modes + interference |
| T6 SGD channel | 0.0015 | training info floor |
| T7 info floor | 0.00000074 | I_min per token |

**Key derived numbers:**
- KV compression cliff: 2.5-bit floor (beyond = incoherent)
- KV errors 78x more costly per cosine unit than weight errors
- Whisper cross-attn von Neumann floor: 1.70 bpw
- FFN_down von Neumann floor: ~10.5 bpw (incompressible statically)
- Koopman decomposition closes FFN_down from 10.5 to ~3.9 bpw/program