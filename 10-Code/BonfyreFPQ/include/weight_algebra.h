/*
 * weight_algebra.h — Bonfyre Weight Algebra (BWA)
 *
 * Unified framework for weight tensor manipulation:
 *   W = L + R
 * where L = low-rank global structure, R = FPQ-structured residual.
 *
 * Operations: decompose, compress, prune, edit, merge
 * All operations use curl + divergence corrections to maintain
 * subspace compatibility and energy closure.
 *
 * Model-class routing based on empirical priors from 1,789 tensors:
 *   Whisper:  η_L ≈ 0.29–0.36  (LR-heavy)
 *   Phi-4:   η_L ≈ 0.049       (residual-heavy)
 *   Wan 14B: η_L ≈ 0.057       (residual-heavy)
 */
#ifndef BONFYRE_WEIGHT_ALGEBRA_H
#define BONFYRE_WEIGHT_ALGEBRA_H

#include "fpq.h"
#include <stddef.h>

/* ── Model class routing ── */

typedef enum {
    BWA_CLASS_AUTO          = 0,   /* auto-detect from η_L */
    BWA_CLASS_LR_HEAVY      = 1,   /* Whisper-like: η_L > 0.15 */
    BWA_CLASS_RESIDUAL_HEAVY = 2,  /* Phi/Wan-like: η_L ≤ 0.15 */
} bwa_model_class_t;

/* ── Tensor type classification ── */

typedef enum {
    BWA_TENSOR_UNKNOWN    = 0,
    BWA_TENSOR_FFN        = 1,   /* best algebra substrate */
    BWA_TENSOR_SELF_ATTN  = 2,   /* good for algebra */
    BWA_TENSOR_CROSS_ATTN = 3,   /* fragile — light LR, careful edits */
    BWA_TENSOR_EMBEDDING  = 4,   /* strong LR, good for algebra */
    BWA_TENSOR_NORM       = 5,   /* skip (1D) */
} bwa_tensor_type_t;

/* ── Prune mode ── */

typedef enum {
    BWA_PRUNE_RAW         = 0,   /* magnitude pruning (baseline) */
    BWA_PRUNE_RANK        = 1,   /* SVD rank truncation */
    BWA_PRUNE_HYBRID      = 2,   /* keep L intact, prune R, curl+div correct */
} bwa_prune_mode_t;

/* ── Edit mode ── */

typedef enum {
    BWA_EDIT_RAW          = 0,   /* W + scale * delta */
    BWA_EDIT_LR_ONLY      = 1,   /* project delta into L subspace */
    BWA_EDIT_RESIDUAL_ONLY = 2,  /* project delta into R complement */
    BWA_EDIT_SELECTIVE    = 3,   /* separate λ_L, λ_R scaling */
} bwa_edit_mode_t;

/* ── Decomposition result ── */

typedef struct {
    /* Decomposition W = L + R */
    float *L;                    /* low-rank component [rows × cols] */
    float *R;                    /* residual component [rows × cols] */
    size_t rows;
    size_t cols;

    /* SVD factors of L (for corrections and operations) */
    float *U;                    /* left singular vectors [rows × rank] */
    float *S;                    /* singular values [rank] */
    float *Vt;                   /* right singular vectors [rank × cols] */
    int    rank;                 /* actual rank used */

    /* Energy metrics */
    float eta_L;                 /* ‖L‖²_F / ‖W‖²_F */
    float eta_R;                 /* ‖R‖²_F / ‖W‖²_F */
    float total_energy;          /* ‖W‖²_F */

    /* Classification */
    bwa_model_class_t model_class;
    bwa_tensor_type_t tensor_type;
} bwa_decomposition_t;

/* ── Analysis report (per model) ── */

typedef struct {
    int    n_tensors;
    float  mean_eta_L;
    float  mean_cosine;
    float  worst_cosine;
    float  mean_bpw;
    bwa_model_class_t detected_class;

    /* Per tensor-type breakdown */
    int    n_ffn, n_self_attn, n_cross_attn, n_embedding;
    float  mean_cos_ffn, mean_cos_self_attn, mean_cos_cross_attn;
} bwa_analysis_t;


/* ═══════════════════════════════════════════════════════════════════
 * Core operations
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * Decompose W into L + R.
 * rank_ratio: fraction of min(rows,cols) to use as rank (default 0.25).
 * Returns allocated decomposition (caller must free with bwa_decomposition_free).
 */
bwa_decomposition_t *bwa_decompose(const float *W, size_t rows, size_t cols,
                                    float rank_ratio);

void bwa_decomposition_free(bwa_decomposition_t *d);

/*
 * Classify tensor type from name string.
 */
bwa_tensor_type_t bwa_classify_tensor(const char *name);

/*
 * Classify model from mean η_L across all tensors.
 */
bwa_model_class_t bwa_classify_model(float mean_eta_L);


/* ═══════════════════════════════════════════════════════════════════
 * Correction passes
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * Curl correction: project R into orthogonal complement of L's column space.
 *   R_corrected = (I - U_L @ U_L^T) @ R
 * U_L: [rows × rank], R: [rows × cols]
 * Result written in-place to R.
 */
void bwa_curl_correction(const float *U_L, int rank, float *R,
                          size_t rows, size_t cols);

/*
 * Two-sided curl correction:
 *   R ← (I - P_U) R (I - P_V)
 * U_L: [rows × rank], Vt_L: [rank × cols], R: [rows × cols]
 * Result written in-place to R.
 */
void bwa_curl_correction_twosided(const float *U_L, const float *Vt_L,
                                   int rank, float *R,
                                   size_t rows, size_t cols);

/*
 * Divergence correction: scale L+R so ‖L+R‖_F = target_norm.
 *   γ = √(max(target²  - ‖L‖², 0) / (‖R‖² + ε))
 *   R ← γR
 * L: [rows × cols], R: [rows × cols] (R modified in-place)
 */
void bwa_divergence_correction(const float *L, float *R,
                                size_t rows, size_t cols,
                                float target_norm_sq);

/*
 * Coupled correction: alternating curl + divergence for n_iters.
 * Modifies R in-place. L is read-only.
 */
void bwa_coupled_correction(const float *U_L, int rank,
                             float *L, float *R,
                             size_t rows, size_t cols,
                             float target_norm_sq, int n_iters);


/* ═══════════════════════════════════════════════════════════════════
 * Operators
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * Prune: structured weight pruning with model-class routing.
 * W: input [rows × cols], output: pruned [rows × cols] (caller allocates).
 * keep_ratio: fraction of weight magnitude/rank to retain.
 * rank_ratio: LR decomposition rank fraction (0.25 default).
 */
void bwa_prune(const float *W, size_t rows, size_t cols,
               const char *name, float keep_ratio, float rank_ratio,
               bwa_prune_mode_t mode, float *output);

/*
 * Merge: structured weight merging with curl+div correction.
 * W1, W2: two weight tensors [rows × cols].
 * alpha: interpolation weight (0=all W2, 1=all W1).
 * output: merged result [rows × cols] (caller allocates).
 */
void bwa_merge(const float *W1, const float *W2,
               size_t rows, size_t cols, const char *name,
               float alpha, float rank_ratio, float *output);

/*
 * Edit: apply delta in decomposition space.
 * W: original [rows × cols], delta: modification [rows × cols].
 * scale: overall edit strength.
 * mode: which subspace to apply delta in.
 * lambda_L, lambda_R: per-subspace scaling (for SELECTIVE mode).
 * output: edited result [rows × cols] (caller allocates).
 */
void bwa_edit(const float *W, const float *delta,
              size_t rows, size_t cols, const char *name,
              float scale, bwa_edit_mode_t mode,
              float lambda_L, float lambda_R,
              float rank_ratio, float *output);


/* ═══════════════════════════════════════════════════════════════════
 * Analysis
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * Analyze a full model: decompose all tensors, compute energy splits,
 * classify model, produce per-tensor-type breakdown.
 * Prints report to stderr, returns summary.
 */
bwa_analysis_t bwa_analyze_model(const char *model_path);


#endif /* BONFYRE_WEIGHT_ALGEBRA_H */
