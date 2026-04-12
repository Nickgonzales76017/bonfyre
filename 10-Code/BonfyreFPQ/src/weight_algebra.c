/*
 * weight_algebra.c — Bonfyre Weight Algebra (BWA)
 *
 * C implementation of the unified weight algebra framework:
 *   W = L + R  with  compress / prune / edit / merge operators
 *
 * Uses existing v9_truncated_svd() for SVD and BLAS for matrix ops.
 * Empirical priors from 1,789 tensors across 4 production models.
 */
#include "weight_algebra.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* ── BLAS declarations (same as v4_optimizations.c) ── */
#ifdef __APPLE__
#include <Accelerate/Accelerate.h>
#else
#ifdef HAVE_OPENBLAS
#include <cblas.h>
#endif
#endif

/* External: reuse the existing truncated SVD from v4_optimizations.c */
extern int v9_truncated_svd(const float *A, size_t m, size_t n,
                            int max_rank, float energy_threshold,
                            float *U_out, float *S_out, float *Vt_out);

/* ═══════════════════════════════════════════════════════════════════
 * Utility: Frobenius norm squared
 * ═══════════════════════════════════════════════════════════════════ */
static double frob_norm_sq(const float *M, size_t n) {
    double s = 0.0;
    for (size_t i = 0; i < n; i++)
        s += (double)M[i] * (double)M[i];
    return s;
}

/* ═══════════════════════════════════════════════════════════════════
 * Classification
 * ═══════════════════════════════════════════════════════════════════ */

bwa_tensor_type_t bwa_classify_tensor(const char *name) {
    if (!name) return BWA_TENSOR_UNKNOWN;

    /* Normalization / bias — skip */
    if (strstr(name, "norm") || strstr(name, "bias") ||
        strstr(name, "ln_") || strstr(name, "layer_norm"))
        return BWA_TENSOR_NORM;

    /* Cross attention */
    if (strstr(name, "cross_attn") || strstr(name, "cross_attention") ||
        strstr(name, "encoder_attn"))
        return BWA_TENSOR_CROSS_ATTN;

    /* Self attention */
    if (strstr(name, "self_attn") || strstr(name, "attn_q") ||
        strstr(name, "attn_k") || strstr(name, "attn_v") ||
        strstr(name, "attn_output") || strstr(name, ".query.") ||
        strstr(name, ".key.") || strstr(name, ".value.") ||
        strstr(name, ".out.") || strstr(name, "q_proj") ||
        strstr(name, "k_proj") || strstr(name, "v_proj") ||
        strstr(name, "o_proj"))
        return BWA_TENSOR_SELF_ATTN;

    /* FFN / MLP */
    if (strstr(name, "ffn") || strstr(name, "mlp") ||
        strstr(name, "fc1") || strstr(name, "fc2") ||
        strstr(name, "gate_proj") || strstr(name, "up_proj") ||
        strstr(name, "down_proj") || strstr(name, "ff."))
        return BWA_TENSOR_FFN;

    /* Embedding / projection */
    if (strstr(name, "embed") || strstr(name, "lm_head") ||
        strstr(name, "projection") || strstr(name, "proj.weight"))
        return BWA_TENSOR_EMBEDDING;

    return BWA_TENSOR_UNKNOWN;
}

bwa_model_class_t bwa_classify_model(float mean_eta_L) {
    /* Threshold from empirical data:
     * Whisper η_L ≈ 0.29–0.36 → LR_HEAVY
     * Phi-4  η_L ≈ 0.049      → RESIDUAL_HEAVY
     * Wan    η_L ≈ 0.057      → RESIDUAL_HEAVY
     * Threshold τ = 0.15 separates the two regimes cleanly. */
    if (mean_eta_L > 0.15f)
        return BWA_CLASS_LR_HEAVY;
    return BWA_CLASS_RESIDUAL_HEAVY;
}

static const char *bwa_class_name(bwa_model_class_t c) {
    switch (c) {
        case BWA_CLASS_LR_HEAVY:       return "LR-heavy (Whisper-like)";
        case BWA_CLASS_RESIDUAL_HEAVY: return "Residual-heavy (Phi/Wan-like)";
        default:                       return "auto";
    }
}

static const char *bwa_tensor_type_name(bwa_tensor_type_t t) {
    switch (t) {
        case BWA_TENSOR_FFN:        return "FFN";
        case BWA_TENSOR_SELF_ATTN:  return "self_attn";
        case BWA_TENSOR_CROSS_ATTN: return "cross_attn";
        case BWA_TENSOR_EMBEDDING:  return "embedding";
        case BWA_TENSOR_NORM:       return "norm";
        default:                    return "unknown";
    }
}

/* ═══════════════════════════════════════════════════════════════════
 * Decomposition: W = L + R
 * ═══════════════════════════════════════════════════════════════════ */

bwa_decomposition_t *bwa_decompose(const float *W, size_t rows, size_t cols,
                                    float rank_ratio) {
    size_t total = rows * cols;
    int mn_min = (int)(rows < cols ? rows : cols);
    int target_rank = (int)(mn_min * rank_ratio);
    if (target_rank < 1) target_rank = 1;
    if (target_rank > 64) target_rank = 64;  /* cap for memory */

    bwa_decomposition_t *d = (bwa_decomposition_t *)calloc(1, sizeof(bwa_decomposition_t));
    d->rows = rows;
    d->cols = cols;

    /* Allocate SVD output buffers */
    d->U  = (float *)calloc(rows * (size_t)target_rank, sizeof(float));
    d->S  = (float *)calloc((size_t)target_rank, sizeof(float));
    d->Vt = (float *)calloc((size_t)target_rank * cols, sizeof(float));

    /* Run truncated SVD (reuse existing v9 implementation) */
    d->rank = v9_truncated_svd(W, rows, cols, target_rank, 0.99f,
                                d->U, d->S, d->Vt);

    /* Compute L = U * diag(S) * Vt */
    d->L = (float *)calloc(total, sizeof(float));
    for (int r = 0; r < d->rank; r++) {
        float s = d->S[r];
        for (size_t i = 0; i < rows; i++) {
            float u_ir = d->U[i * target_rank + r] * s;
            for (size_t j = 0; j < cols; j++)
                d->L[i * cols + j] += u_ir * d->Vt[r * cols + j];
        }
    }

    /* R = W - L */
    d->R = (float *)malloc(total * sizeof(float));
    for (size_t i = 0; i < total; i++)
        d->R[i] = W[i] - d->L[i];

    /* Energy metrics */
    d->total_energy = (float)frob_norm_sq(W, total);
    float L_energy = (float)frob_norm_sq(d->L, total);
    float R_energy = (float)frob_norm_sq(d->R, total);

    d->eta_L = (d->total_energy > 1e-20f) ? L_energy / d->total_energy : 0.0f;
    d->eta_R = (d->total_energy > 1e-20f) ? R_energy / d->total_energy : 0.0f;

    d->model_class = BWA_CLASS_AUTO;
    d->tensor_type = BWA_TENSOR_UNKNOWN;

    return d;
}

void bwa_decomposition_free(bwa_decomposition_t *d) {
    if (!d) return;
    free(d->L);
    free(d->R);
    free(d->U);
    free(d->S);
    free(d->Vt);
    free(d);
}

/* ═══════════════════════════════════════════════════════════════════
 * Correction passes
 * ═══════════════════════════════════════════════════════════════════ */

void bwa_curl_correction(const float *U_L, int rank, float *R,
                          size_t rows, size_t cols) {
    if (rank <= 0) return;

    /*
     * R_corrected = (I - U_L @ U_L^T) @ R
     *
     * Two-step BLAS approach:
     *   1. P = U_L^T @ R     [rank × cols]
     *   2. R -= U_L @ P      [rows × cols]
     */
#if defined(__APPLE__) || defined(HAVE_OPENBLAS)
    float *P = (float *)malloc((size_t)rank * cols * sizeof(float));

    /* P = U_L^T @ R  [rank × cols] */
    cblas_sgemm(CblasRowMajor, CblasTrans, CblasNoTrans,
                rank, (int)cols, (int)rows,
                1.0f, U_L, rank,      /* U_L row-major [rows × rank] */
                R, (int)cols,          /* R row-major [rows × cols] */
                0.0f, P, (int)cols);   /* P row-major [rank × cols] */

    /* R -= U_L @ P */
    cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans,
                (int)rows, (int)cols, rank,
                -1.0f, U_L, rank,
                P, (int)cols,
                1.0f, R, (int)cols);

    free(P);
#else
    /* Fallback: naive triple loop */
    float *P = (float *)calloc((size_t)rank * cols, sizeof(float));
    for (int r = 0; r < rank; r++)
        for (size_t j = 0; j < cols; j++) {
            float s = 0.0f;
            for (size_t i = 0; i < rows; i++)
                s += U_L[i * rank + r] * R[i * cols + j];
            P[r * cols + j] = s;
        }
    for (size_t i = 0; i < rows; i++)
        for (size_t j = 0; j < cols; j++) {
            float s = 0.0f;
            for (int r = 0; r < rank; r++)
                s += U_L[i * rank + r] * P[r * cols + j];
            R[i * cols + j] -= s;
        }
    free(P);
#endif
}

void bwa_curl_correction_twosided(const float *U_L, const float *Vt_L,
                                   int rank, float *R,
                                   size_t rows, size_t cols) {
    if (rank <= 0) return;

    /* R ← (I - U U^T) R (I - V V^T)
     * Step 1: left projection — remove U column space */
    bwa_curl_correction(U_L, rank, R, rows, cols);

    /* Step 2: right projection — remove V row space
     * R^T ← (I - V V^T) R^T
     * We transpose R in-place, apply curl, transpose back. */
#if defined(__APPLE__) || defined(HAVE_OPENBLAS)
    /* V = Vt^T: [cols × rank] */
    float *V = (float *)malloc(cols * (size_t)rank * sizeof(float));
    for (int r = 0; r < rank; r++)
        for (size_t j = 0; j < cols; j++)
            V[j * rank + r] = Vt_L[r * cols + j];

    /* P = V^T @ R^T = Vt @ R^T  [rank × rows]
     * Equivalently P = (R @ V)^T, but we compute via R^T.
     * Use: R^T is col-major R. */
    float *Rt = (float *)malloc(cols * rows * sizeof(float));
    for (size_t i = 0; i < rows; i++)
        for (size_t j = 0; j < cols; j++)
            Rt[j * rows + i] = R[i * cols + j];

    float *P = (float *)malloc((size_t)rank * rows * sizeof(float));
    cblas_sgemm(CblasColMajor, CblasTrans, CblasNoTrans,
                rank, (int)rows, (int)cols,
                1.0f, V, (int)cols,
                Rt, (int)cols,
                0.0f, P, rank);

    /* Rt -= V @ P */
    cblas_sgemm(CblasColMajor, CblasNoTrans, CblasNoTrans,
                (int)cols, (int)rows, rank,
                -1.0f, V, (int)cols,
                P, rank,
                1.0f, Rt, (int)cols);

    /* Transpose back */
    for (size_t i = 0; i < rows; i++)
        for (size_t j = 0; j < cols; j++)
            R[i * cols + j] = Rt[j * rows + i];

    free(V);
    free(Rt);
    free(P);
#else
    /* Fallback: apply right-side projection naively
     * R ← R - R @ V @ V^T where V = Vt^T */
    float *RV = (float *)calloc(rows * (size_t)rank, sizeof(float));
    for (size_t i = 0; i < rows; i++)
        for (int r = 0; r < rank; r++) {
            float s = 0.0f;
            for (size_t j = 0; j < cols; j++)
                s += R[i * cols + j] * Vt_L[r * cols + j]; /* V^T row = Vt row */
            RV[i * rank + r] = s;
        }
    for (size_t i = 0; i < rows; i++)
        for (size_t j = 0; j < cols; j++) {
            float s = 0.0f;
            for (int r = 0; r < rank; r++)
                s += RV[i * rank + r] * Vt_L[r * cols + j];
            R[i * cols + j] -= s;
        }
    free(RV);
#endif
}

void bwa_divergence_correction(const float *L, float *R,
                                size_t rows, size_t cols,
                                float target_norm_sq) {
    size_t total = rows * cols;
    double R_norm_sq = frob_norm_sq(R, total);
    double L_norm_sq = frob_norm_sq(L, total);

    if (R_norm_sq < 1e-20) return;

    /* γ = √(max(‖W‖² - ‖L‖², 0) / (‖R‖² + ε)) */
    double target_R_sq = (double)target_norm_sq - L_norm_sq;
    if (target_R_sq < 0.0) target_R_sq = 0.0;

    float gamma = (float)sqrt(target_R_sq / (R_norm_sq + 1e-20));
    for (size_t i = 0; i < total; i++)
        R[i] *= gamma;
}

void bwa_coupled_correction(const float *U_L, int rank,
                             float *L, float *R,
                             size_t rows, size_t cols,
                             float target_norm_sq, int n_iters) {
    for (int iter = 0; iter < n_iters; iter++) {
        /* Curl: remove L's subspace from R */
        bwa_curl_correction(U_L, rank, R, rows, cols);

        /* Divergence: match total energy */
        size_t total = rows * cols;
        double combined_norm_sq = 0.0;
        for (size_t i = 0; i < total; i++) {
            double w = (double)L[i] + (double)R[i];
            combined_norm_sq += w * w;
        }
        if (combined_norm_sq < 1e-20) continue;

        float scale = (float)sqrt((double)target_norm_sq / combined_norm_sq);
        for (size_t i = 0; i < total; i++) {
            L[i] *= scale;
            R[i] *= scale;
        }
    }
}


/* ═══════════════════════════════════════════════════════════════════
 * Operator: PRUNE
 * ═══════════════════════════════════════════════════════════════════ */

/* Raw magnitude prune: zero out smallest entries */
static void prune_raw_magnitude(const float *W, size_t total,
                                 float keep_ratio, float *output) {
    /* Find threshold via partial sort approximation */
    size_t n_keep = (size_t)(total * keep_ratio);
    if (n_keep < 1) n_keep = 1;
    if (n_keep >= total) {
        memcpy(output, W, total * sizeof(float));
        return;
    }

    /* Histogram-based threshold approximation (avoid full sort) */
    float max_abs = 0.0f;
    for (size_t i = 0; i < total; i++) {
        float a = fabsf(W[i]);
        if (a > max_abs) max_abs = a;
    }
    if (max_abs < 1e-20f) {
        memset(output, 0, total * sizeof(float));
        return;
    }

    /* Binary search for threshold */
    float lo = 0.0f, hi = max_abs;
    float threshold = 0.0f;
    for (int iter = 0; iter < 40; iter++) {
        float mid = (lo + hi) * 0.5f;
        size_t count = 0;
        for (size_t i = 0; i < total; i++)
            if (fabsf(W[i]) >= mid) count++;
        if (count >= n_keep) {
            threshold = mid;
            lo = mid;
        } else {
            hi = mid;
        }
    }

    for (size_t i = 0; i < total; i++)
        output[i] = (fabsf(W[i]) >= threshold) ? W[i] : 0.0f;
}

/* Rank truncation prune: keep top-k singular modes */
static void prune_rank_truncation(const float *W, size_t rows, size_t cols,
                                   float keep_ratio, float *output) {
    int mn_min = (int)(rows < cols ? rows : cols);
    int max_rank = mn_min;
    if (max_rank > 128) max_rank = 128;

    float *U  = (float *)calloc(rows * (size_t)max_rank, sizeof(float));
    float *S  = (float *)calloc((size_t)max_rank, sizeof(float));
    float *Vt = (float *)calloc((size_t)max_rank * cols, sizeof(float));

    int full_rank = v9_truncated_svd(W, rows, cols, max_rank, 0.999f, U, S, Vt);

    int n_keep = (int)(full_rank * keep_ratio);
    if (n_keep < 1) n_keep = 1;

    /* Reconstruct with top n_keep modes */
    memset(output, 0, rows * cols * sizeof(float));
    for (int r = 0; r < n_keep; r++) {
        float s = S[r];
        for (size_t i = 0; i < rows; i++) {
            float u_s = U[i * max_rank + r] * s;
            for (size_t j = 0; j < cols; j++)
                output[i * cols + j] += u_s * Vt[r * cols + j];
        }
    }

    free(U); free(S); free(Vt);
}

/* Hybrid prune: keep L, prune R, curl+div correct */
static void prune_hybrid(const float *W, size_t rows, size_t cols,
                          const char *name, float keep_ratio,
                          float rank_ratio, float *output) {
    bwa_decomposition_t *d = bwa_decompose(W, rows, cols, rank_ratio);
    size_t total = rows * cols;

    /* Determine R prune ratio to hit overall keep target.
     * L is always preserved, so adjust R pruning accordingly. */
    double L_param_frac = (double)d->rank * (double)(rows + cols) / (double)total;
    double r_keep = (keep_ratio - L_param_frac * rank_ratio) /
                    (1.0 - L_param_frac * rank_ratio);
    if (r_keep < 0.01) r_keep = 0.01;
    if (r_keep > 1.0) r_keep = 1.0;

    /* Prune R by magnitude */
    float *R_pruned = (float *)malloc(total * sizeof(float));
    prune_raw_magnitude(d->R, total, (float)r_keep, R_pruned);

    /* Curl correction: pruned R may overlap L's subspace */
    bwa_curl_correction(d->U, d->rank, R_pruned, rows, cols);

    /* Divergence correction: match original energy */
    bwa_divergence_correction(d->L, R_pruned, rows, cols, d->total_energy);

    /* Output = L + R_corrected */
    for (size_t i = 0; i < total; i++)
        output[i] = d->L[i] + R_pruned[i];

    free(R_pruned);
    bwa_decomposition_free(d);
}

void bwa_prune(const float *W, size_t rows, size_t cols,
               const char *name, float keep_ratio, float rank_ratio,
               bwa_prune_mode_t mode, float *output) {
    size_t total = rows * cols;
    if (rows <= 1 || cols <= 1 || total < 512) {
        /* 1D or tiny tensors: raw prune only */
        prune_raw_magnitude(W, total, keep_ratio, output);
        return;
    }

    switch (mode) {
        case BWA_PRUNE_RAW:
            prune_raw_magnitude(W, total, keep_ratio, output);
            break;
        case BWA_PRUNE_RANK:
            prune_rank_truncation(W, rows, cols, keep_ratio, output);
            break;
        case BWA_PRUNE_HYBRID:
            prune_hybrid(W, rows, cols, name, keep_ratio, rank_ratio, output);
            break;
    }
}


/* ═══════════════════════════════════════════════════════════════════
 * Operator: MERGE
 * ═══════════════════════════════════════════════════════════════════ */

void bwa_merge(const float *W1, const float *W2,
               size_t rows, size_t cols, const char *name,
               float alpha, float rank_ratio, float *output) {
    size_t total = rows * cols;

    if (rows <= 1 || cols <= 1 || total < 512) {
        /* Small/1D tensors: raw merge */
        for (size_t i = 0; i < total; i++)
            output[i] = alpha * W1[i] + (1.0f - alpha) * W2[i];
        return;
    }

    /* Decompose both */
    bwa_decomposition_t *d1 = bwa_decompose(W1, rows, cols, rank_ratio);
    bwa_decomposition_t *d2 = bwa_decompose(W2, rows, cols, rank_ratio);

    /* Full-strength merge of global modes */
    float *L_merged = (float *)malloc(total * sizeof(float));
    for (size_t i = 0; i < total; i++)
        L_merged[i] = alpha * d1->L[i] + (1.0f - alpha) * d2->L[i];

    /* Coherence-attenuated merge of residuals.
     * γ = max(0.3, min(1.0, cos(R1, R2)))
     * If residuals align, keep full; if incoherent, attenuate. */
    float coherence = fpq_cosine_sim(d1->R, d2->R, total);
    float gamma = coherence;
    if (gamma < 0.3f) gamma = 0.3f;
    if (gamma > 1.0f) gamma = 1.0f;

    float *R_merged = (float *)malloc(total * sizeof(float));
    for (size_t i = 0; i < total; i++)
        R_merged[i] = gamma * (alpha * d1->R[i] + (1.0f - alpha) * d2->R[i]);

    /* Use d1's SVD basis for curl (could average bases, but this is simpler) */
    int rank = d1->rank < d2->rank ? d1->rank : d2->rank;

    /* Curl correction */
    /* Recompute U from L_merged for accurate projection */
    int mn_min = (int)(rows < cols ? rows : cols);
    int lr_rank = (int)(mn_min * rank_ratio);
    if (lr_rank < 1) lr_rank = 1;
    if (lr_rank > 64) lr_rank = 64;

    float *U_m  = (float *)calloc(rows * (size_t)lr_rank, sizeof(float));
    float *S_m  = (float *)calloc((size_t)lr_rank, sizeof(float));
    float *Vt_m = (float *)calloc((size_t)lr_rank * cols, sizeof(float));

    int merged_rank = v9_truncated_svd(L_merged, rows, cols, lr_rank, 0.99f,
                                        U_m, S_m, Vt_m);

    bwa_curl_correction(U_m, merged_rank, R_merged, rows, cols);

    /* Divergence: target = raw merge energy */
    float *W_target = (float *)malloc(total * sizeof(float));
    for (size_t i = 0; i < total; i++)
        W_target[i] = alpha * W1[i] + (1.0f - alpha) * W2[i];
    float target_norm_sq = (float)frob_norm_sq(W_target, total);

    bwa_divergence_correction(L_merged, R_merged, rows, cols, target_norm_sq);

    /* Output = L + R */
    for (size_t i = 0; i < total; i++)
        output[i] = L_merged[i] + R_merged[i];

    free(L_merged);
    free(R_merged);
    free(W_target);
    free(U_m); free(S_m); free(Vt_m);
    bwa_decomposition_free(d1);
    bwa_decomposition_free(d2);
}


/* ═══════════════════════════════════════════════════════════════════
 * Operator: EDIT
 * ═══════════════════════════════════════════════════════════════════ */

void bwa_edit(const float *W, const float *delta,
              size_t rows, size_t cols, const char *name,
              float scale, bwa_edit_mode_t mode,
              float lambda_L, float lambda_R,
              float rank_ratio, float *output) {
    size_t total = rows * cols;

    if (mode == BWA_EDIT_RAW || rows <= 1 || cols <= 1 || total < 512) {
        /* Raw edit: W + scale * delta */
        for (size_t i = 0; i < total; i++)
            output[i] = W[i] + scale * delta[i];
        return;
    }

    /* Decompose original tensor */
    bwa_decomposition_t *d = bwa_decompose(W, rows, cols, rank_ratio);

    /* Project delta relative to L's subspace:
     *   Δ_L = U @ U^T @ Δ @ V @ V^T  (component in L's subspace)
     *   Δ_R = Δ - Δ_L                  (component in complement) */

    float *delta_L = (float *)calloc(total, sizeof(float));
    float *delta_R = (float *)malloc(total * sizeof(float));

    if (d->rank > 0) {
#if defined(__APPLE__) || defined(HAVE_OPENBLAS)
        /* Δ_L = U @ (U^T @ Δ) projected through V as well.
         * For simplicity, use left-side projection only:
         *   Δ_L = U @ (U^T @ Δ)
         * This captures the column-space component. */
        int target_rank = d->rank;
        /* Step 1: P = U^T @ delta  [rank × cols] */
        float *P = (float *)malloc((size_t)target_rank * cols * sizeof(float));
        cblas_sgemm(CblasRowMajor, CblasTrans, CblasNoTrans,
                    target_rank, (int)cols, (int)rows,
                    1.0f, d->U, target_rank,
                    delta, (int)cols,
                    0.0f, P, (int)cols);
        /* Step 2: delta_L = U @ P */
        cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans,
                    (int)rows, (int)cols, target_rank,
                    1.0f, d->U, target_rank,
                    P, (int)cols,
                    0.0f, delta_L, (int)cols);
        free(P);
#else
        /* Fallback: naive projection */
        for (size_t i = 0; i < rows; i++)
            for (size_t j = 0; j < cols; j++) {
                float s = 0.0f;
                for (int r = 0; r < d->rank; r++) {
                    float ut_d = 0.0f;
                    for (size_t k = 0; k < rows; k++)
                        ut_d += d->U[k * d->rank + r] * delta[k * cols + j];
                    s += d->U[i * d->rank + r] * ut_d;
                }
                delta_L[i * cols + j] = s;
            }
#endif
    }

    for (size_t i = 0; i < total; i++)
        delta_R[i] = delta[i] - delta_L[i];

    /* Apply edit based on mode */
    switch (mode) {
        case BWA_EDIT_LR_ONLY:
            /* Only apply delta in L subspace */
            for (size_t i = 0; i < total; i++)
                output[i] = W[i] + scale * delta_L[i];
            break;

        case BWA_EDIT_RESIDUAL_ONLY:
            /* Only apply delta in R complement */
            for (size_t i = 0; i < total; i++)
                output[i] = W[i] + scale * delta_R[i];
            break;

        case BWA_EDIT_SELECTIVE:
            /* Separate scaling: λ_L for L channel, λ_R for R channel */
            for (size_t i = 0; i < total; i++)
                output[i] = W[i] + scale * (lambda_L * delta_L[i] +
                                             lambda_R * delta_R[i]);
            break;

        default:
            for (size_t i = 0; i < total; i++)
                output[i] = W[i] + scale * delta[i];
            break;
    }

    free(delta_L);
    free(delta_R);
    bwa_decomposition_free(d);
}


/* ═══════════════════════════════════════════════════════════════════
 * Analysis: full model decomposition report
 * ═══════════════════════════════════════════════════════════════════ */

bwa_analysis_t bwa_analyze_model(const char *model_path) {
    bwa_analysis_t result = {0};

    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_safetensors_read(model_path, &n_raw);
    if (!raw || n_raw == 0) {
        /* Try GGML/GGUF */
        raw = fpq_ggml_read(model_path, &n_raw);
    }
    if (!raw || n_raw == 0) {
        fprintf(stderr, "BWA: failed to read model from %s\n", model_path);
        return result;
    }

    fprintf(stderr,
        "\n══════════════════════════════════════════════════════════\n"
        " Bonfyre Weight Algebra — Model Analysis\n"
        " Model: %s\n"
        " Tensors: %zu\n"
        "══════════════════════════════════════════════════════════\n",
        model_path, n_raw);

    double sum_eta_L = 0.0;
    double sum_cos_ffn = 0.0, sum_cos_self = 0.0, sum_cos_cross = 0.0;
    int n_analyzed = 0;
    float worst_eta_L = 1.0f, best_eta_L = 0.0f;

    for (size_t i = 0; i < n_raw; i++) {
        if (raw[i].rows <= 1 || raw[i].cols <= 1) continue;
        if (raw[i].rows * raw[i].cols < 512) continue;

        bwa_tensor_type_t tt = bwa_classify_tensor(raw[i].name);
        if (tt == BWA_TENSOR_NORM) continue;

        bwa_decomposition_t *d = bwa_decompose(raw[i].data, raw[i].rows,
                                                raw[i].cols, 0.25f);

        sum_eta_L += d->eta_L;
        if (d->eta_L < worst_eta_L) worst_eta_L = d->eta_L;
        if (d->eta_L > best_eta_L) best_eta_L = d->eta_L;

        /* Quick prune test: hybrid at 75% keep → cosine */
        float *pruned = (float *)malloc(raw[i].rows * raw[i].cols * sizeof(float));
        bwa_prune(raw[i].data, raw[i].rows, raw[i].cols, raw[i].name,
                  0.75f, 0.25f, BWA_PRUNE_HYBRID, pruned);
        float cos = fpq_cosine_sim(raw[i].data, pruned, raw[i].rows * raw[i].cols);
        free(pruned);

        switch (tt) {
            case BWA_TENSOR_FFN:
                result.n_ffn++;
                sum_cos_ffn += cos;
                break;
            case BWA_TENSOR_SELF_ATTN:
                result.n_self_attn++;
                sum_cos_self += cos;
                break;
            case BWA_TENSOR_CROSS_ATTN:
                result.n_cross_attn++;
                sum_cos_cross += cos;
                break;
            case BWA_TENSOR_EMBEDDING:
                result.n_embedding++;
                break;
            default: break;
        }

        if (n_analyzed < 20 || (n_analyzed % 50 == 0)) {
            fprintf(stderr, "  [%zu] %-50s %6zux%-6zu  η_L=%.4f  rank=%d  type=%-10s  prune_cos=%.6f\n",
                    i, raw[i].name, raw[i].rows, raw[i].cols,
                    d->eta_L, d->rank, bwa_tensor_type_name(tt), cos);
        }

        n_analyzed++;
        bwa_decomposition_free(d);
    }

    if (n_analyzed == 0) {
        fprintf(stderr, "  No eligible 2D tensors found.\n");
        fpq_raw_tensor_free(raw, n_raw);
        return result;
    }

    result.n_tensors = n_analyzed;
    result.mean_eta_L = (float)(sum_eta_L / n_analyzed);
    result.detected_class = bwa_classify_model(result.mean_eta_L);

    if (result.n_ffn > 0)
        result.mean_cos_ffn = (float)(sum_cos_ffn / result.n_ffn);
    if (result.n_self_attn > 0)
        result.mean_cos_self_attn = (float)(sum_cos_self / result.n_self_attn);
    if (result.n_cross_attn > 0)
        result.mean_cos_cross_attn = (float)(sum_cos_cross / result.n_cross_attn);

    fprintf(stderr,
        "\n══════════════════════════════════════════════════════════\n"
        " BWA ANALYSIS SUMMARY\n"
        "══════════════════════════════════════════════════════════\n"
        "  Tensors analyzed:  %d\n"
        "  Mean η_L:          %.4f\n"
        "  Range η_L:         %.4f – %.4f\n"
        "  Detected class:    %s\n"
        "\n"
        "  Tensor-type breakdown (hybrid prune @75%% keep):\n"
        "    FFN:        %3d tensors  mean cos = %.6f\n"
        "    Self-attn:  %3d tensors  mean cos = %.6f\n"
        "    Cross-attn: %3d tensors  mean cos = %.6f\n"
        "    Embedding:  %3d tensors\n"
        "\n"
        "  Routing recommendation:\n",
        n_analyzed, result.mean_eta_L, worst_eta_L, best_eta_L,
        bwa_class_name(result.detected_class),
        result.n_ffn, result.mean_cos_ffn,
        result.n_self_attn, result.mean_cos_self_attn,
        result.n_cross_attn, result.mean_cos_cross_attn,
        result.n_embedding);

    if (result.detected_class == BWA_CLASS_LR_HEAVY) {
        fprintf(stderr,
            "    → LR-HEAVY: Use LR+FPQ compression path\n"
            "    → Prune aggressively in L (rank truncation)\n"
            "    → LR-channel edits will be most effective\n"
            "    → Strong candidate for structured merge\n");
    } else {
        fprintf(stderr,
            "    → RESIDUAL-HEAVY: Use FPQ-only compression for most tensors\n"
            "    → Preserve L modestly, prune mostly in R\n"
            "    → Edits need richer residual channel or tensor-specific targeting\n"
            "    → Merge benefits mainly in tensors with highest η_L\n");
    }

    fprintf(stderr,
        "══════════════════════════════════════════════════════════\n\n");

    fpq_raw_tensor_free(raw, n_raw);
    return result;
}
