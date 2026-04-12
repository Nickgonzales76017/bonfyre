/*
 * fpqx.h — FPQ-X: Generalized Compression Algebra
 *
 * Lineage: FPQ v4 → v7 → v8 → v9 → v10 → FPQ-X
 *
 * The right object to optimize is not the stored tensor, but the
 * executed information flow under bandwidth, cache, and context constraints.
 *
 * FPQ v10 represents: T ≈ B + R + P
 * FPQ-X generalizes:  𝒯(x,c,h,t) = (B + R + P) ⊙ S + Π(x,c,h,t) + Δ_seq(c,t)
 *
 * Six operator families:
 *   A  — Additive        (LR SVD + E8 + RVQ + ghost)      [inherited from v10]
 *   M  — Multiplicative   (low-rank scaling manifold)       [NEW]
 *   Π  — Predictive       (context-conditioned restoration) [NEW]
 *   D  — Distilled        (sequence-axis memory compression) [NEW]
 *   Λ  — Adaptive         (layer/domain/context policy)     [NEW]
 *   H  — Hardware-aligned  (kernel-traversal packing)       [NEW]
 *
 * Shorthand: FPQ-X = A + M + Π + D + Λ + H
 *
 * References:
 *   LoRDS (arXiv:2601.22716)  — multiplicative scaling
 *   WaterSIC (arXiv:2603.04956) — activation-aware distortion
 *   EchoKV (arXiv:2603.22910)  — predictive KV reconstruction
 *   KVSculpt (arXiv:2603.27819) — KV distillation
 *   KV-CoRE (arXiv:2602.05929)  — data-dependent compressibility
 *   InnerQ (arXiv:2602.23200)  — hardware-aligned grouping
 *   MoBiQuant (arXiv:2602.20191) — token-adaptive precision
 *   High-Rate QMM (arXiv:2601.17187) — activation-weighted objective
 */

#ifndef BONFYRE_FPQX_H
#define BONFYRE_FPQX_H

#include "fpq.h"
#include "weight_algebra.h"
#include <stddef.h>
#include <stdint.h>

/* ═══════════════════════════════════════════════════════════════════
 * Version and format constants
 * ═══════════════════════════════════════════════════════════════════ */

#define FPQX_VERSION         "1.0.0"
#define FPQX_MAGIC           0x46585031  /* "FXP1" */
#define FPQX_FORMAT_VERSION  1

/* ═══════════════════════════════════════════════════════════════════
 * Operator family enum — the six dimensions of FPQ-X
 * ═══════════════════════════════════════════════════════════════════ */

typedef enum {
    FPQX_OP_ADDITIVE       = 0x01,  /* A: B + R + P  (base + residual + patch) */
    FPQX_OP_MULTIPLICATIVE = 0x02,  /* M: ⊙ S  (low-rank scaling manifold) */
    FPQX_OP_PREDICTIVE     = 0x04,  /* Π: context-conditioned restoration */
    FPQX_OP_DISTILLED      = 0x08,  /* D: sequence-axis compression */
    FPQX_OP_ADAPTIVE       = 0x10,  /* Λ: data/layer/domain policy */
    FPQX_OP_HARDWARE       = 0x20,  /* H: kernel-aligned packing */
} fpqx_op_family_t;

/* ═══════════════════════════════════════════════════════════════════
 * M — Multiplicative Structure: Low-rank scaling manifold
 *
 * Instead of: Ŵ = B + R + P
 * Computes:   Ŵ = (B + R + P) ⊙ S, where S = 1 + A·Bᵀ
 *
 * The scale matrix S is a low-rank perturbation of identity.
 * This captures error modes that are multiplicative in nature —
 * channel-wise gain shifts, attention temperature drift, etc.
 *
 * Ref: LoRDS (arXiv:2601.22716) — continuous low-rank scaling
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    float *A;           /* left factor  [rows × scale_rank] */
    float *B;           /* right factor [cols × scale_rank] */
    int    scale_rank;  /* rank of multiplicative manifold */
    size_t rows;
    size_t cols;
} fpqx_scale_manifold_t;

/*
 * Learn S = 1 + AB^T such that W ≈ Ŵ ⊙ S minimizes ‖W - Ŵ⊙S‖²_F.
 *
 * W_original: full-precision weights [rows × cols]
 * W_additive: (B + R + P) reconstruction [rows × cols]
 * scale_rank: rank budget for S (typical: 2–8)
 *
 * Returns allocated manifold (caller frees with fpqx_scale_free).
 */
fpqx_scale_manifold_t *fpqx_scale_learn(const float *W_original,
                                          const float *W_additive,
                                          size_t rows, size_t cols,
                                          int scale_rank);

/*
 * Apply multiplicative correction: out = W_additive ⊙ (1 + A·B^T)
 */
void fpqx_scale_apply(const float *W_additive,
                       const fpqx_scale_manifold_t *S,
                       float *output);

void fpqx_scale_free(fpqx_scale_manifold_t *s);


/* ═══════════════════════════════════════════════════════════════════
 * Π — Predictive Structure: context-conditioned restoration
 *
 * Generalizes ghost heads from static rank-1 correction to a
 * lightweight predictor that uses activation statistics to
 * reconstruct dropped residual information.
 *
 * Offline: learn a small linear predictor φ that maps from the
 *   "easy-to-compress" part of W to the "hard residual."
 *   Π = φ(B, stats) where φ is a low-rank map.
 *
 * Ref: EchoKV (arXiv:2603.22910), MoBiQuant (arXiv:2602.20191)
 * ═══════════════════════════════════════════════════════════════════ */

typedef enum {
    FPQX_PREDICT_NONE      = 0,
    FPQX_PREDICT_LINEAR    = 1,   /* Π = P_pred · z + b */
    FPQX_PREDICT_LOWRANK   = 2,   /* Π = U_pred · (V_pred^T · z) */
} fpqx_predict_mode_t;

typedef struct {
    fpqx_predict_mode_t mode;
    int                  pred_rank;   /* rank of predictor */
    float               *P;           /* prediction weights [output_dim × input_dim] or factors */
    float               *bias;        /* prediction bias [output_dim] (may be NULL) */
    size_t               input_dim;   /* predictor input size */
    size_t               output_dim;  /* = rows * cols (flattened tensor) */
} fpqx_predictor_t;

/*
 * Learn a predictor that maps from the low-rank base B to the
 * residual that was lost during additive compression.
 *
 * W_original: full-precision weights
 * W_reconstructed: post-additive+scale reconstruction
 * L: the low-rank component from BWA decomposition
 * pred_rank: rank budget for the predictor (typical: 1–4)
 */
fpqx_predictor_t *fpqx_predict_learn(const float *W_original,
                                       const float *W_reconstructed,
                                       const float *L_base,
                                       size_t rows, size_t cols,
                                       int pred_rank);

/*
 * Apply predictive correction: output += Π(L_base)
 */
void fpqx_predict_apply(float *W_reconstructed,
                          const fpqx_predictor_t *pred,
                          const float *L_base,
                          size_t rows, size_t cols);

void fpqx_predict_free(fpqx_predictor_t *p);


/* ═══════════════════════════════════════════════════════════════════
 * D — Distilled Structure: sequence-axis memory compression
 *
 * For KV cache tensors: compress along the sequence axis rather than
 * only per-vector quantization. Distills N cache entries into K
 * "semantic atoms" via attention-weighted K-means.
 *
 * Ref: KVSculpt (arXiv:2603.27819), SemantiCache
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    float *atoms;        /* distilled atoms [n_atoms × head_dim] */
    int    n_atoms;      /* number of distilled entries (K ≪ N) */
    int    head_dim;     /* dimension per head */
    float *weights;      /* attention mass per atom [n_atoms] */
} fpqx_distilled_cache_t;

/*
 * Distill a KV cache from N entries to K semantic atoms.
 * Uses attention-weighted K-means on the key/value vectors.
 *
 * cache: [seq_len × head_dim] row-major
 * attn_weights: [seq_len] per-entry attention mass (NULL = uniform)
 * target_atoms: desired number of atoms
 */
fpqx_distilled_cache_t *fpqx_distill(const float *cache,
                                       size_t seq_len, size_t head_dim,
                                       const float *attn_weights,
                                       int target_atoms);

/*
 * Reconstruct: expand atoms back to approximation of full cache.
 * Each original entry is approximated by its nearest atom.
 */
void fpqx_distill_reconstruct(const fpqx_distilled_cache_t *dc,
                                float *output, size_t seq_len);

void fpqx_distill_free(fpqx_distilled_cache_t *dc);


/* ═══════════════════════════════════════════════════════════════════
 * Λ — Adaptive Structure: data-dependent policy selection
 *
 * Compressibility varies by layer, head, domain, data distribution.
 * The adaptive module profiles each tensor and selects the optimal
 * combination of operator families and bit budgets.
 *
 * Ref: KV-CoRE (arXiv:2602.05929)
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    /* Per-tensor profiling signals */
    float eta_L;              /* low-rank energy fraction */
    float spectral_gap;       /* σ₁/σ₂ ratio */
    float kurtosis;           /* weight distribution shape */
    float outlier_fraction;   /* fraction of weights > 3σ */

    /* Recommended policy */
    int   recommended_bits;   /* 2, 3, or 4 */
    int   use_scale;          /* enable multiplicative manifold? */
    int   scale_rank;         /* recommended manifold rank */
    int   use_predictor;      /* enable predictive correction? */
    int   pred_rank;          /* recommended predictor rank */
    float adaptive_keep;      /* pruning keep ratio */

    /* Active operator mask */
    uint32_t active_ops;      /* bitmask of fpqx_op_family_t */
} fpqx_policy_t;

/*
 * Profile a tensor and determine the optimal compression policy.
 * Uses empirical priors from ~1,790 production tensors.
 */
fpqx_policy_t fpqx_profile(const float *W, size_t rows, size_t cols,
                             const char *name, int base_bits);


/* ═══════════════════════════════════════════════════════════════════
 * H — Hardware Structure: kernel-aligned packing
 *
 * Layout quantization data to match the kernel traversal order.
 * Group scales along the inner dimension for scale reuse during
 * matrix multiplication.
 *
 * Ref: InnerQ (arXiv:2602.23200)
 * ═══════════════════════════════════════════════════════════════════ */

typedef enum {
    FPQX_PACK_DEFAULT      = 0,   /* row-major, no alignment */
    FPQX_PACK_INNER_GROUP  = 1,   /* group scales along K (inner) dim */
    FPQX_PACK_TILE_4x4     = 2,   /* 4×4 tile packing for SIMD */
    FPQX_PACK_NEON_128     = 3,   /* 128-bit NEON-aligned */
} fpqx_pack_mode_t;

typedef struct {
    fpqx_pack_mode_t mode;
    size_t           group_size;    /* quantization group size (32, 64, 128) */
    size_t           tile_rows;     /* tile height */
    size_t           tile_cols;     /* tile width */
    uint8_t         *packed_data;   /* packed binary data */
    size_t           packed_bytes;  /* total packed size */
    float           *scales;        /* per-group scale factors */
    size_t           n_groups;      /* number of groups */
} fpqx_packed_t;

/*
 * Pack tensor data for hardware-aligned access.
 * Reorders quantized data so group scales are contiguous along
 * the inner (K) dimension for scale reuse in matmul.
 */
fpqx_packed_t *fpqx_pack(const float *data, size_t rows, size_t cols,
                           int bits, fpqx_pack_mode_t mode, size_t group_size);

void fpqx_pack_free(fpqx_packed_t *p);


/* ═══════════════════════════════════════════════════════════════════
 * Unified FPQ-X Tensor: the compiled representation
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    char    name[128];
    size_t  rows;
    size_t  cols;

    /* Active operator bitmask */
    uint32_t active_ops;

    /* A — Additive (inherited from FPQ v10) */
    fpq_tensor_t          *additive;       /* B + R + P via v9/v10 encoder */

    /* M — Multiplicative */
    fpqx_scale_manifold_t *scale;          /* low-rank S (NULL = disabled) */

    /* Π — Predictive */
    fpqx_predictor_t      *predictor;      /* context predictor (NULL = disabled) */

    /* D — Distilled (for KV cache tensors) */
    fpqx_distilled_cache_t *distilled;     /* distilled memory (NULL = N/A) */

    /* Λ — Policy used for this tensor */
    fpqx_policy_t          policy;

    /* H — Packed representation */
    fpqx_packed_t          *packed;        /* hardware-aligned (NULL = default) */

    /* Quality metrics */
    float  cosine_pre_scale;    /* cos(W, B+R+P) */
    float  cosine_post_scale;   /* cos(W, (B+R+P)⊙S) */
    float  cosine_final;        /* cos(W, final reconstruction) */
    float  bpw;                 /* effective bits per weight */
} fpqx_tensor_t;


/* ═══════════════════════════════════════════════════════════════════
 * Full pipeline: encode and decode
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * FPQ-X full encode pipeline:
 *   1. Profile tensor → select policy (Λ)
 *   2. BWA decompose W = L + R, adaptive prune, curl/div correct
 *   3. FPQ v9/v10 encode: B + R + P (A)
 *   4. Learn multiplicative manifold S (M)
 *   5. Learn predictive correction Π (Π)
 *   6. Hardware-align pack (H)
 *
 * Returns allocated fpqx_tensor_t (caller frees with fpqx_tensor_free).
 */
fpqx_tensor_t *fpqx_encode(const float *W, size_t rows, size_t cols,
                             const char *name, int base_bits);

/*
 * FPQ-X decode: reconstruct full-precision tensor.
 *   output: pre-allocated [rows × cols]
 */
void fpqx_decode(const fpqx_tensor_t *t, float *output);

void fpqx_tensor_free(fpqx_tensor_t *t);


/* ═══════════════════════════════════════════════════════════════════
 * Compiled objective (rate–distortion–execution)
 *
 * min_θ E[L_task + α·L_op + β·C_bw + γ·C_lat + δ·C_ctx]
 *
 * We approximate this by profiling each tensor and selecting the
 * operator combination that minimizes the Lagrangian cost.
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    float alpha;     /* weight for output distortion term */
    float beta;      /* weight for bandwidth cost */
    float gamma;     /* weight for latency cost */
    float delta;     /* weight for context growth cost */
} fpqx_objective_t;

/* Default objective weights */
static const fpqx_objective_t FPQX_DEFAULT_OBJECTIVE = {
    .alpha = 1.0f,
    .beta  = 0.1f,
    .gamma = 0.05f,
    .delta = 0.01f,
};


/* ═══════════════════════════════════════════════════════════════════
 * Batch operations (model-level)
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    int    n_tensors;
    int    n_additive_only;     /* tensors using only A */
    int    n_with_scale;        /* tensors using A + M */
    int    n_with_predict;      /* tensors using A + M + Π */
    int    n_kv_distilled;      /* KV cache tensors distilled */
    float  mean_cosine;
    float  worst_cosine;
    float  mean_bpw;
    double total_original_bytes;
    double total_compressed_bytes;
} fpqx_model_stats_t;


#endif /* BONFYRE_FPQX_H */
