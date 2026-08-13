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
    int   *assignments;  /* per-position atom index [n_seq] (set by distill) */
    size_t n_seq;        /* original sequence length */
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


/* ═══════════════════════════════════════════════════════════════════
 * I — Spectral Lattice Inference (SLI)
 *
 * Compute W @ x entirely in the compressed domain — NO dequantization.
 *
 * Core insight (proved in SPECTRAL_LATTICE_INFERENCE.md):
 *   FWHT is self-adjoint, so the transform can be applied to the
 *   INPUT activation instead of the weights. E8 lattice points form
 *   a finite set enabling table precomputation. QJL projections are
 *   linear, enabling factored scoring.
 *
 * Result: <w_b, x_b> = Σ_g Γ[idx_g, g] + Σ_p Ω[tidx_p, p]
 *                     + (||r||/m) Σ_p y_p·π_p
 *
 * Every term is a table lookup or sign-weighted sum.
 * No weight is ever reconstructed to a dense float value.
 *
 * Bandwidth: 64 bytes per 256-element block vs 512 bytes BF16 → 8× reduction.
 * Quality: EXACT match to decoded weights (cosine = 1.000000).
 *
 * Ref: BonfyreFPQ SPECTRAL_LATTICE_INFERENCE.md (April 2026)
 * ═══════════════════════════════════════════════════════════════════ */

#define FPQX_OP_INFERENCE  0x40   /* I: Spectral Lattice Inference */

/* SLI score tables for one block-column position (precomputed per token) */
typedef struct {
    float  *x_spectral;    /* spectral-domain activation [BLOCK_DIM] */
    float  *gamma;         /* E8 lattice score table — NOT used in current
                              implementation (exact per-block unwarp is used
                              instead, since E8 points vary per row) */
    float  *proj_scores;   /* QJL projection scores π[p] = φ_p^T x̃ [QJL_PROJECTIONS] */
    size_t  block_dim;
    uint64_t block_seed;   /* random sign seed for this block */
    uint64_t proj_seed;    /* QJL projection seed for this block */
} fpqx_sli_table_t;

/* SLI context for an entire weight matrix (reused across tokens) */
typedef struct {
    size_t  rows;
    size_t  cols;
    size_t  blocks_per_row;
    size_t  n_block_cols;    /* = blocks_per_row */

    /* Per-block-column E8 + tile data from the encoded tensor */
    /* (These are pointers into the fpqx_tensor, not owned) */
    const fpqx_tensor_t *tensor;

    /* Scratch space for per-token tables (allocated once, reused) */
    fpqx_sli_table_t *tables;   /* [n_block_cols] */
    float  *output;              /* [rows] scratch for row scores */

    /* Precomputed z vectors: z = unwarp(E8+tile)*rms + QJL recon.
     * Written in-place over the E8 region of sbb_scale_delta during
     * prepare.  Points into enc->sbb_scale_delta (not owned). */
    float  *z_data;
    size_t z_data_mmap_bytes;   /* mmap-backed SLI z_data byte extent */
    int    z_data_is_mmap;      /* 1 when z_data must be munmap()ed */
/* [n_total_blocks × 256] */
    size_t  n_total_blocks;
    int     z_precomputed;       /* 1 if z vectors are ready */
    int     z_fwht_preapplied;   /* 1 if z_data stores FWHT(z) instead of z */

    /* Exact row-padded execution packing.  The per-block Haar signs are
     * static, so folding them into FWHT(z) turns SLI into one dense GEMV over
     * the already-compressed residual representation. */
    float  *z_linear_data;       /* [rows × blocks_per_row × 256], owned */

    /* Apple Metal execution state for the exact FP32 transformed residual.
     * Opaque so the core FPQ format and the non-Apple build stay C-only. */
    void   *metal_sli;

    /* M-operator approximate column fold (Innovation 2 — M-fold).
     * At fpqx_sli_prepare() time: if tensor->scale != NULL, compute the
     * per-column approximate fold factor col_fold[j] = 1 + dot(A_mean, B[j,:])
     * where A_mean[r] = mean over rows of A[:,r].  At inference time, multiply
     * x[j] by col_fold[j] before SLI scoring so ~90% of M's contribution is
     * captured without touching the weight decoder.  NULL means M is absent. */
    float  *m_fold_col;          /* [cols] owned; NULL = no M operator */

    /* FP16 prepared z path. Cuts prepared residency roughly in half while
     * preserving the same block-domain scoring structure as the FP32 path. */
    __fp16 *z_data_f16;          /* owned; NULL when inactive */

#ifdef FPQ_INT8_SDOT
    /* INT8 SDOT optional path (compile with -DFPQ_INT8_SDOT).
     * z_data_i8: quantized FWHT(z) blocks — [n_total_blocks × 256] INT8.
     * z_data_scale: per-block abs-max scale factor — [n_total_blocks] FP32.
     * dot(z_i8, x_i8) * z_scale * x_scale reconstructs the FP32 score. */
    int8_t *z_data_i8;           /* owned; NULL when inactive */
    float  *z_data_scale;        /* owned; NULL when inactive */
#endif

    /* Cached offsets for fast-path inference */
    size_t  v8_base;
    size_t  e8_off;
    int     lr_rank;
    size_t  us_off;
    size_t  vt_off;

#ifdef FPQ_JIT_SLI
    /* JIT-compiled inner kernel (Innovation 3 — JIT SLI).
     * fpqx_sli_prepare() generates a .c file with N_ROWS and BPR baked in as
     * integer literals, compiles it with cc -O3 -march=native -shared, and
     * dlopen()s the result.  The compiler can then fully unroll the block and
     * row loops and emit optimal SIMD.  Falls back silently to the regular
     * fast path if compilation fails or FPQ_JIT_SLI is not defined.
     * Signature: (z_fwht[n_blocks*256], x_raw[cols], output[rows]+=,
     *              haar_seed, cols) */
    void   *jit_handle;          /* dlopen handle; NULL = not compiled */
    void  (*jit_fn)(const float *z_fwht, const float *x, float *out,
                    unsigned long long haar_seed, unsigned long cols);
    char    jit_so_path[256];    /* cached .so path (for possible cleanup) */
#endif
} fpqx_sli_ctx_t;

/*
 * Prepare SLI context for a weight tensor.
 * Allocates tables and scratch space.  Call once per tensor.
 */
fpqx_sli_ctx_t *fpqx_sli_prepare(const fpqx_tensor_t *t);

/*
 * Compute y = W @ x via Spectral Lattice Inference.
 *
 * Phase 1: Precompute spectral activation + score tables (O(h) per token)
 * Phase 2: Score each row via table lookups (O(112 * V * h/256) per token)
 *
 * x:      input activation [cols]
 * output: result [rows]  (caller-allocated)
 *
 * Returns 0 on success.
 */
int fpqx_sli_matvec(fpqx_sli_ctx_t *ctx, const float *x, float *output);

/* Exact Metal implementation of the transformed residual score.  It is an
 * execution backend, not a new quantization: it consumes the same FWHT(z)
 * values as fpqx_sli_matvec and falls back to CPU on any backend failure. */
int fpqx_metal_sli_try_matvec(fpqx_sli_ctx_t *ctx,
                              const float *x,
                              float *output,
                              uint64_t haar_seed);
void fpqx_metal_sli_free(fpqx_sli_ctx_t *ctx);

/*
 * One-shot convenience: prepare + matvec + cleanup.
 * Simpler but less efficient for repeated calls.
 */
int fpqx_sli_matvec_oneshot(const fpqx_tensor_t *t,
                              const float *x, float *output);

void fpqx_sli_free(fpqx_sli_ctx_t *ctx);


/* ═══════════════════════════════════════════════════════════════════
 * Innovation 1: Shared Spectral Context — Q/K/V spectral reuse
 *
 * For attention layers where Q, K, V weight matrices receive the same
 * input x, the per-block sign patterns differ only by a XorShift-based
 * mixing.  A group context loads x_b into a local stack buffer ONCE per
 * block column, then reuses it across all n tensors — reducing main-memory
 * bandwidth for x by up to n×.  Each tensor still uses its own z_data
 * (no approximation: each score is exact).
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    int              n_tensors;     /* 2 or 3 (K/V sharing or full Q/K/V) */
    fpqx_sli_ctx_t **ctxs;         /* [n_tensors] — NOT owned, caller manages */
    uint64_t        *haar_seeds;    /* [n_tensors] cached haar_seed values */
    size_t           n_block_cols;  /* = ctxs[0]->blocks_per_row */
} fpqx_sli_shared_spectral_t;

/*
 * Build a shared spectral group from n SLI contexts.
 * The contexts must already be prepared (fpqx_sli_prepare called).
 * Caller still owns ctxs and must free them separately.
 */
fpqx_sli_shared_spectral_t *fpqx_sli_group_qkv(
    fpqx_sli_ctx_t **ctxs, int n);

/*
 * Batch matvec using shared x bandwidth.
 * Computes y_t = W_t @ x for each tensor t in the group.
 *
 * x:       input activation [min(ctxs[t]->cols)]
 * outputs: [n_tensors] caller-allocated arrays, each of size ctxs[t]->rows
 *
 * Returns 0 on success.
 */
int fpqx_sli_matvec_shared(fpqx_sli_shared_spectral_t *grp,
                             const float *x,
                             float **outputs);

void fpqx_sli_shared_free(fpqx_sli_shared_spectral_t *grp);


/* ═══════════════════════════════════════════════════════════════════
 * Fused Warp-Score Precomputation (discovered during SLI proof)
 *
 * Optimization: instead of computing unwarp(e8[i] / ls * wn) per
 * element per row, precompute the warp-weighted spectral activation
 * x̃_warp[i] = unwarp'(input/ls*wn) * x̃[i] so that tile scoring
 * becomes a simple dot product with the raw tile codebook entries.
 *
 * This eliminates the nonlinear unwarp from the inner scoring loop,
 * reducing it to pure linear algebra (lookups + multiply-adds).
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * Benchmark SLI vs dense decode matmul on a given tensor.
 * Prints quality metrics (cosine, top-k, Pearson) and bandwidth ratio.
 * Returns cosine similarity between SLI output and dense decode output.
 */
float fpqx_sli_benchmark(const fpqx_tensor_t *t,
                           const float *x, size_t x_len);

/* Run full optimization sweep: E8 analysis, QJL reduction experiments,
 * packed bandwidth calculations, and hardware projection table.
 * Prints results to stderr. */
void fpqx_sli_optimization_sweep(const fpqx_tensor_t *t,
                                   const float *x, size_t x_len);


/* ═══════════════════════════════════════════════════════════════════
 * Phase 7: FPQx Operator Fusion — zero-copy sequential matvec chain
 *
 * Eliminates intermediate heap allocations when chaining multiple
 * SLI matvec operations. A single scratch buffer (sized max(rows[i]))
 * is pingponged between stages — no malloc/free per inference call.
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct fpqx_fuse_chain_t fpqx_fuse_chain_t;

/* Create a prepared chain from n tensors (applied in order). */
fpqx_fuse_chain_t *fpqx_fuse_chain_create(const fpqx_tensor_t **tensors,
                                            int n);

/* Execute all stages with zero intermediate allocations.
 * x_in  : length = tensors[0]->cols
 * y_out : length = tensors[n-1]->rows  (caller allocates) */
int fpqx_fuse_chain_matvec(fpqx_fuse_chain_t *ch,
                             const float *x_in,
                             float       *y_out);

/* Release all resources. */
void fpqx_fuse_chain_free(fpqx_fuse_chain_t *ch);

/* Return the prepared SLI context for stage i (0-based). NULL if out of range. */
fpqx_sli_ctx_t *fpqx_fuse_chain_get_stage(fpqx_fuse_chain_t *ch, int i);


#endif /* BONFYRE_FPQX_H */
