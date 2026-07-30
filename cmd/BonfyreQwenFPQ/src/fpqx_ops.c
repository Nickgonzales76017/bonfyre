#include <pthread.h>
#include <stdlib.h>
#include <limits.h>
#ifndef PTHREAD_STACK_MIN
#define PTHREAD_STACK_MIN 16384
#endif


static pthread_attr_t *fpqx_small_pthread_attr(void) {
    static pthread_attr_t attr;
    static int initialized = 0;
    static int enabled = 0;

    if (!initialized) {
        initialized = 1;

        size_t kb = 256;
        const char *v = getenv("BONFYRE_QWEN_WORKER_STACK_KB");
        if (v && *v) {
            unsigned long parsed = strtoul(v, NULL, 10);
            if (parsed >= 64 && parsed <= 8192) kb = (size_t)parsed;
        }

        size_t bytes = kb * 1024u;
        if (bytes < (size_t)PTHREAD_STACK_MIN) bytes = (size_t)PTHREAD_STACK_MIN;

        if (pthread_attr_init(&attr) == 0 &&
            pthread_attr_setstacksize(&attr, bytes) == 0) {
            enabled = 1;
        } else {
            pthread_attr_destroy(&attr);
            enabled = 0;
        }
    }

    return enabled ? &attr : NULL;
}

/*
 * fpqx_ops.c — FPQ-X operator implementations
 *
 * Six operator families: A + M + Π + D + Λ + H
 *
 * A (Additive) is inherited from FPQ v10 — see v4_optimizations.c
 * This file implements the five new families: M, Π, D, Λ, H
 */

#include "fpqx.h"
#include "fpq_neon.h"
#include <pthread.h>
#include <stdio.h>
#ifdef _OPENMP
#include <omp.h>
#endif
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <unistd.h>
#ifdef __APPLE__
#include <dispatch/dispatch.h>
#endif

#ifdef __APPLE__
#include <Accelerate/Accelerate.h>
#define USE_ACCELERATE 1
#else
#define USE_ACCELERATE 0
#endif

typedef struct {
    int initialized;
    int enabled;
    char jsonl_path[PATH_MAX];
    char tensor_name[128];
    size_t rows[256];
    size_t n_rows;
} fpqx_sli_score_probe_config_t;

static fpqx_sli_score_probe_config_t g_fpqx_sli_score_probe = {0};
static pthread_mutex_t g_fpqx_sli_score_probe_mu = PTHREAD_MUTEX_INITIALIZER;

static int fpqx_env_enabled(const char *name) {
    const char *v = getenv(name);
    if (!v || !*v) return 0;
    return strcmp(v, "0") != 0;
}

static int fpqx_sli_prepare_int8_only_enabled(void) {
    return fpqx_env_enabled("BONFYRE_QWEN_PREPARE_INT8_ONLY");
}

static int fpqx_sli_prepare_f16_only_enabled(void) {
    return fpqx_env_enabled("BONFYRE_QWEN_PREPARE_F16_ONLY");
}

static int fpqx_sli_has_precomputed_fast_data(const fpqx_sli_ctx_t *ctx) {
    if (!ctx || !ctx->z_precomputed) return 0;
    if (ctx->z_data) return 1;
    if (ctx->z_data_f16) return 1;
#ifdef FPQ_INT8_SDOT
    if (ctx->z_data_i8 && ctx->z_data_scale) return 1;
#endif
    return 0;
}

static uint16_t fpqx_float_to_fp16(float f) {
    union { float f; uint32_t u; } v = { .f = f };
    uint32_t sign = (v.u >> 16) & 0x8000u;
    int32_t exp = (int32_t)((v.u >> 23) & 0xFFu) - 127 + 15;
    uint32_t frac = (v.u >> 13) & 0x03FFu;
    if (exp <= 0) return (uint16_t)sign;
    if (exp >= 31) return (uint16_t)(sign | 0x7C00u);
    return (uint16_t)(sign | ((uint32_t)exp << 10) | frac);
}

static void fpqx_sli_score_probe_init(void) {
    fpqx_sli_score_probe_config_t *cfg = &g_fpqx_sli_score_probe;
    if (cfg->initialized) return;
    cfg->initialized = 1;
    if (!fpqx_env_enabled("BONFYRE_QWEN_SLI_SCORE_PROBE")) return;

    const char *jsonl = getenv("BONFYRE_QWEN_SLI_SCORE_PROBE_JSONL");
    if (!jsonl || !*jsonl) jsonl = "/tmp/qwen-sli-score-probe.jsonl";
    snprintf(cfg->jsonl_path, sizeof(cfg->jsonl_path), "%s", jsonl);

    const char *tensor_name = getenv("BONFYRE_QWEN_SLI_SCORE_PROBE_TENSOR");
    if (tensor_name && *tensor_name)
        snprintf(cfg->tensor_name, sizeof(cfg->tensor_name), "%s", tensor_name);

    const char *rows_csv = getenv("BONFYRE_QWEN_SLI_SCORE_PROBE_ROWS");
    if (rows_csv && *rows_csv) {
        char buf[1024];
        snprintf(buf, sizeof(buf), "%s", rows_csv);
        char *save = NULL;
        for (char *tok = strtok_r(buf, ",", &save);
             tok && cfg->n_rows < (sizeof(cfg->rows) / sizeof(cfg->rows[0]));
             tok = strtok_r(NULL, ",", &save)) {
            char *end = NULL;
            unsigned long row = strtoul(tok, &end, 10);
            if (end == tok) continue;
            cfg->rows[cfg->n_rows++] = (size_t)row;
        }
    }

    cfg->enabled = 1;
}

static int fpqx_sli_score_probe_tensor_match(const fpqx_sli_ctx_t *ctx) {
    const fpqx_sli_score_probe_config_t *cfg = &g_fpqx_sli_score_probe;
    const char *shared_qkv = getenv("BONFYRE_QWEN_SLI_SCORE_PROBE_SHARED_QKV");
    if (!cfg->enabled) return 0;
    if (!ctx || !ctx->tensor) return 0;
    if (cfg->tensor_name[0] == '\0') return 1;
    if (shared_qkv && *shared_qkv && strcmp(shared_qkv, "0") != 0) {
        const char *lhs = cfg->tensor_name;
        const char *rhs = ctx->tensor->name;
        const char *lhs_q = strstr(lhs, ".self_attn.q_proj.");
        const char *lhs_k = strstr(lhs, ".self_attn.k_proj.");
        const char *lhs_v = strstr(lhs, ".self_attn.v_proj.");
        const char *rhs_q = strstr(rhs, ".self_attn.q_proj.");
        const char *rhs_k = strstr(rhs, ".self_attn.k_proj.");
        const char *rhs_v = strstr(rhs, ".self_attn.v_proj.");
        const char *lhs_pos = lhs_q ? lhs_q : (lhs_k ? lhs_k : lhs_v);
        const char *rhs_pos = rhs_q ? rhs_q : (rhs_k ? rhs_k : rhs_v);
        if (lhs_pos && rhs_pos) {
            size_t lhs_prefix = (size_t)(lhs_pos - lhs);
            size_t rhs_prefix = (size_t)(rhs_pos - rhs);
            if (lhs_prefix == rhs_prefix && strncmp(lhs, rhs, lhs_prefix) == 0) return 1;
        }
    }
    return strcmp(ctx->tensor->name, cfg->tensor_name) == 0;
}

static int fpqx_sli_score_probe_row_match(size_t row) {
    const fpqx_sli_score_probe_config_t *cfg = &g_fpqx_sli_score_probe;
    if (!cfg->enabled) return 0;
    if (cfg->n_rows == 0) return row == 0;
    for (size_t i = 0; i < cfg->n_rows; i++) {
        if (cfg->rows[i] == row) return 1;
    }
    return 0;
}

static int fpqx_sli_score_probe_match(const fpqx_sli_ctx_t *ctx, size_t row) {
    fpqx_sli_score_probe_init();
    return fpqx_sli_score_probe_tensor_match(ctx) && fpqx_sli_score_probe_row_match(row);
}

static int fpqx_sli_score_probe_only_rows_enabled(void) {
    return fpqx_env_enabled("BONFYRE_QWEN_SLI_SCORE_PROBE_ONLY_ROWS");
}

static int fpqx_sli_prepare_probe_only_rows(const fpqx_tensor_t *t) {
    fpqx_sli_score_probe_init();
    if (!t) return 0;
    if (!fpqx_sli_score_probe_only_rows_enabled()) return 0;
    if (!g_fpqx_sli_score_probe.enabled) return 0;
    if (g_fpqx_sli_score_probe.tensor_name[0] == '\0') return 1;
    return strcmp(t->name, g_fpqx_sli_score_probe.tensor_name) == 0;
}

static int fpqx_probe_layer_from_name(const char *name) {
    const char *needle = ".layers.";
    const char *p = name ? strstr(name, needle) : NULL;
    if (!p) return -1;
    return atoi(p + (int)strlen(needle));
}

static const char *fpqx_probe_projection_from_name(const char *name) {
    if (!name) return "unknown";
    if (strstr(name, ".self_attn.q_proj.")) return "q_proj";
    if (strstr(name, ".self_attn.k_proj.")) return "k_proj";
    if (strstr(name, ".self_attn.v_proj.")) return "v_proj";
    if (strstr(name, ".self_attn.o_proj.")) return "o_proj";
    if (strstr(name, ".mlp.gate_proj.")) return "gate_proj";
    if (strstr(name, ".mlp.up_proj.")) return "up_proj";
    if (strstr(name, ".mlp.down_proj.")) return "down_proj";
    return "unknown";
}

static void fpqx_sli_score_probe_emit(const char *kind,
                                      const fpqx_sli_ctx_t *ctx,
                                      const fpq_tensor_t *enc,
                                      size_t row,
                                      size_t block,
                                      size_t col_offset,
                                      size_t this_dim,
                                      float fast_score,
                                      float slow_score,
                                      int shared_qkv_active);
static void fpqx_sli_common_probe_emit(const char *kind,
                                       const fpqx_sli_ctx_t *ctx,
                                       const fpq_tensor_t *enc,
                                       size_t row,
                                       size_t block,
                                       size_t col_offset,
                                       size_t this_dim,
                                       float value_a,
                                       float value_b,
                                       float value_c,
                                       uint64_t block_seed);

/* ═══════════════════════════════════════════════════════════════════
 * Utility: LAPACK/Accelerate wrappers
 * ═══════════════════════════════════════════════════════════════════ */

#if USE_ACCELERATE

/* Thin SVD via Accelerate (same approach as v9_truncated_svd) */
static int fpqx_svd_thin(const float *A, int m, int n, int k,
                          float *U, float *S, float *Vt) {
    int mn = m < n ? m : n;
    if (k > mn) k = mn;

    float *A_copy = (float *)malloc((size_t)m * n * sizeof(float));
    memcpy(A_copy, A, (size_t)m * n * sizeof(float));

    /* column-major for LAPACK: transpose */
    float *At = (float *)malloc((size_t)m * n * sizeof(float));
    for (int i = 0; i < m; i++)
        for (int j = 0; j < n; j++)
            At[j * m + i] = A_copy[i * n + j];

    float *S_full = (float *)calloc(mn, sizeof(float));
    float *U_full = (float *)calloc((size_t)m * mn, sizeof(float));
    float *Vt_full = (float *)calloc((size_t)mn * n, sizeof(float));

    char jobu = 'S', jobvt = 'S';
    int lda = m, ldu = m, ldvt = mn;
    int lwork = -1, info = 0;
    float wkopt;

    sgesvd_(&jobu, &jobvt, &m, &n, At, &lda,
            S_full, U_full, &ldu, Vt_full, &ldvt,
            &wkopt, &lwork, &info);
    lwork = (int)wkopt;
    float *work = (float *)malloc(lwork * sizeof(float));
    sgesvd_(&jobu, &jobvt, &m, &n, At, &lda,
            S_full, U_full, &ldu, Vt_full, &ldvt,
            work, &lwork, &info);

    /* Extract top-k in row-major */
    for (int i = 0; i < m; i++)
        for (int j = 0; j < k; j++)
            U[i * k + j] = U_full[j * m + i];
    memcpy(S, S_full, k * sizeof(float));
    for (int i = 0; i < k; i++)
        for (int j = 0; j < n; j++)
            Vt[i * n + j] = Vt_full[i * n + j];

    free(A_copy); free(At); free(S_full);
    free(U_full); free(Vt_full); free(work);
    return (info == 0) ? k : 0;
}

/* BLAS sgemm wrapper (row-major via transpose trick) */
static void fpqx_matmul(const float *A, const float *B, float *C,
                          int M, int N, int K,
                          float alpha, float beta) {
    /* C = alpha * A * B + beta * C  (all row-major) */
    cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans,
                M, N, K, alpha, A, K, B, N, beta, C, N);
}

#else

/* Fallback: naive matmul */
static void fpqx_matmul(const float *A, const float *B, float *C,
                          int M, int N, int K,
                          float alpha, float beta) {
    for (int i = 0; i < M; i++) {
        for (int j = 0; j < N; j++) {
            float sum = 0.0f;
            for (int k = 0; k < K; k++)
                sum += A[i * K + k] * B[k * N + j];
            C[i * N + j] = beta * C[i * N + j] + alpha * sum;
        }
    }
}

/* Fallback SVD via power iteration + deflation */
static int fpqx_svd_thin(const float *A, int m, int n, int k,
                          float *U, float *S, float *Vt) {
    int mn = m < n ? m : n;
    if (k > mn) k = mn;

    /* A^T A for right singular vectors */
    float *AtA = (float *)calloc((size_t)n * n, sizeof(float));
    for (int i = 0; i < n; i++)
        for (int j = 0; j < n; j++) {
            double s = 0.0;
            for (int r = 0; r < m; r++)
                s += (double)A[r * n + i] * (double)A[r * n + j];
            AtA[i * n + j] = (float)s;
        }

    /* Deflated residual */
    float *R = (float *)malloc((size_t)m * n * sizeof(float));
    memcpy(R, A, (size_t)m * n * sizeof(float));

    for (int c = 0; c < k; c++) {
        /* Power iteration for top singular vector */
        float *v = (float *)calloc(n, sizeof(float));
        v[c % n] = 1.0f;

        for (int iter = 0; iter < 30; iter++) {
            /* u = R * v */
            float *u = (float *)calloc(m, sizeof(float));
            for (int i = 0; i < m; i++)
                for (int j = 0; j < n; j++)
                    u[i] += R[i * n + j] * v[j];
            /* normalize u */
            double nu = 0.0;
            for (int i = 0; i < m; i++) nu += (double)u[i] * u[i];
            nu = sqrt(nu);
            if (nu < 1e-12) { free(u); break; }
            for (int i = 0; i < m; i++) u[i] /= (float)nu;
            /* v = R^T * u */
            memset(v, 0, n * sizeof(float));
            for (int i = 0; i < m; i++)
                for (int j = 0; j < n; j++)
                    v[j] += R[i * n + j] * u[i];
            /* sigma = ||v||, normalize v */
            double nv = 0.0;
            for (int j = 0; j < n; j++) nv += (double)v[j] * v[j];
            nv = sqrt(nv);
            S[c] = (float)nv;
            if (nv < 1e-12) { free(u); break; }
            for (int j = 0; j < n; j++) v[j] /= (float)nv;
            /* Store */
            for (int i = 0; i < m; i++) U[i * k + c] = u[i];
            for (int j = 0; j < n; j++) Vt[c * n + j] = v[j];
            free(u);
        }

        /* Deflate: R -= sigma * u * v^T */
        for (int i = 0; i < m; i++)
            for (int j = 0; j < n; j++)
                R[i * n + j] -= S[c] * U[i * k + c] * Vt[c * n + j];

        free(v);
    }

    free(AtA); free(R);
    return k;
}
#endif /* USE_ACCELERATE */


/* ═══════════════════════════════════════════════════════════════════
 * M — Multiplicative Structure: scale()
 *
 * Learn S = I + AB^T such that W ≈ Ŵ ⊙ (I + AB^T)
 * Equivalently: min_{A,B} ‖W - Ŵ ⊙ (1 + AB^T)‖²_F
 *
 * We solve this via alternating least squares on the elementwise
 * ratio Q[i][j] = W[i][j] / Ŵ[i][j] - 1, i.e. find AB^T ≈ Q.
 * Truncated SVD of Q gives the optimal rank-s approximation.
 * ═══════════════════════════════════════════════════════════════════ */

fpqx_scale_manifold_t *fpqx_scale_learn(const float *W_original,
                                          const float *W_additive,
                                          size_t rows, size_t cols,
                                          int scale_rank) {
    size_t total = rows * cols;

    /* Compute elementwise ratio: Q = W / Ŵ - 1 */
    float *Q = (float *)malloc(total * sizeof(float));
    int n_safe = 0;
    for (size_t i = 0; i < total; i++) {
        if (fabsf(W_additive[i]) > 1e-10f) {
            Q[i] = W_original[i] / W_additive[i] - 1.0f;
            n_safe++;
        } else {
            Q[i] = 0.0f;  /* avoid division by near-zero */
        }
    }

    /* Clamp extreme ratios (outlier protection) */
    float clamp = 2.0f;
    for (size_t i = 0; i < total; i++) {
        if (Q[i] > clamp) Q[i] = clamp;
        if (Q[i] < -clamp) Q[i] = -clamp;
    }

    fpqx_scale_manifold_t *s = (fpqx_scale_manifold_t *)calloc(1, sizeof(*s));
    s->rows = rows;
    s->cols = cols;
    s->scale_rank = scale_rank;
    s->A = (float *)calloc(rows * scale_rank, sizeof(float));
    s->B = (float *)calloc(cols * scale_rank, sizeof(float));

    /* Truncated SVD of Q[rows×cols] → keep top scale_rank components */
    float *U = (float *)calloc(rows * scale_rank, sizeof(float));
    float *Sigma = (float *)calloc(scale_rank, sizeof(float));
    float *Vt = (float *)calloc(scale_rank * cols, sizeof(float));

    int got = fpqx_svd_thin(Q, (int)rows, (int)cols, scale_rank, U, Sigma, Vt);

    /* A = U · diag(√σ), B = V · diag(√σ) so A·B^T = U·Σ·V^T */
    for (int r = 0; r < got; r++) {
        float sqrtS = sqrtf(Sigma[r]);
        for (size_t i = 0; i < rows; i++)
            s->A[i * scale_rank + r] = U[i * scale_rank + r] * sqrtS;
        for (size_t j = 0; j < cols; j++)
            s->B[j * scale_rank + r] = Vt[r * cols + j] * sqrtS;
    }

    free(Q); free(U); free(Sigma); free(Vt);
    return s;
}

void fpqx_scale_apply(const float *W_additive,
                       const fpqx_scale_manifold_t *S,
                       float *output) {
    size_t total = S->rows * S->cols;

    /* Compute S_matrix = A · B^T [rows × cols] */
    float *S_mat = (float *)calloc(total, sizeof(float));
    fpqx_matmul(S->A, S->B, S_mat,
                (int)S->rows, (int)S->cols, S->scale_rank,
                1.0f, 0.0f);
    /* Note: B is [cols × rank], we need B^T = [rank × cols].
       Actually we stored B as [cols × rank], so A·B^T means
       we transpose B in the matmul. Let's just do it explicitly. */
    float *Bt = (float *)malloc((size_t)S->scale_rank * S->cols * sizeof(float));
    for (size_t j = 0; j < S->cols; j++)
        for (int r = 0; r < S->scale_rank; r++)
            Bt[r * S->cols + j] = S->B[j * S->scale_rank + r];

    memset(S_mat, 0, total * sizeof(float));
    fpqx_matmul(S->A, Bt, S_mat,
                (int)S->rows, (int)S->cols, S->scale_rank,
                1.0f, 0.0f);
    free(Bt);

    /* output = W_additive ⊙ (1 + S_mat) */
    for (size_t i = 0; i < total; i++)
        output[i] = W_additive[i] * (1.0f + S_mat[i]);

    free(S_mat);
}

void fpqx_scale_free(fpqx_scale_manifold_t *s) {
    if (!s) return;
    free(s->A);
    free(s->B);
    free(s);
}


/* ═══════════════════════════════════════════════════════════════════
 * Π — Predictive Structure: predict()
 *
 * Learn a low-rank linear map from the base (L) to the residual
 * that was lost: Π(L) = U_pred · (V_pred^T · vec(L))
 *
 * This captures systematic correlations between the low-rank
 * portion and the error — if knowing L tells you something about
 * what was lost, the predictor exploits it.
 * ═══════════════════════════════════════════════════════════════════ */

fpqx_predictor_t *fpqx_predict_learn(const float *W_original,
                                       const float *W_reconstructed,
                                       const float *L_base,
                                       size_t rows, size_t cols,
                                       int pred_rank) {
    size_t total = rows * cols;

    /* Compute target residual: what we want to predict */
    float *residual = (float *)malloc(total * sizeof(float));
    for (size_t i = 0; i < total; i++)
        residual[i] = W_original[i] - W_reconstructed[i];

    /* Compute residual energy */
    double res_energy = 0.0;
    for (size_t i = 0; i < total; i++)
        res_energy += (double)residual[i] * residual[i];

    /* If residual is tiny, skip prediction */
    double orig_energy = 0.0;
    for (size_t i = 0; i < total; i++)
        orig_energy += (double)W_original[i] * W_original[i];

    fpqx_predictor_t *p = (fpqx_predictor_t *)calloc(1, sizeof(*p));
    p->output_dim = total;

    if (res_energy < orig_energy * 1e-8) {
        p->mode = FPQX_PREDICT_NONE;
        free(residual);
        return p;
    }

    /*
     * Strategy: SVD of the cross-correlation matrix C = residual · L^T
     * In practice, we compute this in compressed form.
     *
     * For a low-rank predictor, we find:
     *   Π = residual_col_vectors that correlate with L_col_vectors
     * via CCA (canonical correlation analysis), approximated by
     * cross-covariance SVD.
     *
     * Simplified approach: columnwise correlation.
     * For each column j, predict residual[:,j] from L[:,j].
     * The predictor is a per-column scaling factor (rank-1 per col).
     */

    p->mode = FPQX_PREDICT_LINEAR;
    p->pred_rank = pred_rank;
    p->input_dim = cols;  /* one predictor coefficient per column */

    /* Per-column scaling: scale_j = <residual_j, L_j> / <L_j, L_j> */
    float *col_scales = (float *)calloc(cols, sizeof(float));
    for (size_t j = 0; j < cols; j++) {
        double dot_rl = 0.0, dot_ll = 0.0;
        for (size_t i = 0; i < rows; i++) {
            dot_rl += (double)residual[i * cols + j] * (double)L_base[i * cols + j];
            dot_ll += (double)L_base[i * cols + j] * (double)L_base[i * cols + j];
        }
        col_scales[j] = (dot_ll > 1e-12) ? (float)(dot_rl / dot_ll) : 0.0f;
    }

    p->P = col_scales;
    p->bias = NULL;

    free(residual);
    return p;
}

void fpqx_predict_apply(float *W_reconstructed,
                          const fpqx_predictor_t *pred,
                          const float *L_base,
                          size_t rows, size_t cols) {
    if (!pred || pred->mode == FPQX_PREDICT_NONE) return;

    if (pred->mode == FPQX_PREDICT_LINEAR && pred->P) {
        /* W_reconstructed[:,j] += scale_j * L[:,j] */
        for (size_t j = 0; j < cols; j++) {
            float s = pred->P[j];
            if (fabsf(s) < 1e-10f) continue;
            for (size_t i = 0; i < rows; i++)
                W_reconstructed[i * cols + j] += s * L_base[i * cols + j];
        }
    }
}

void fpqx_predict_free(fpqx_predictor_t *p) {
    if (!p) return;
    free(p->P);
    free(p->bias);
    free(p);
}


/* ═══════════════════════════════════════════════════════════════════
 * D — Distilled Structure: distill()
 *
 * Attention-weighted K-means on cache vectors.
 * Each entry is assigned to its nearest atom; the atom set is
 * iteratively refined to minimize attention-weighted MSE.
 * ═══════════════════════════════════════════════════════════════════ */

fpqx_distilled_cache_t *fpqx_distill(const float *cache,
                                       size_t seq_len, size_t head_dim,
                                       const float *attn_weights,
                                       int target_atoms) {
    if (target_atoms >= (int)seq_len)
        target_atoms = (int)seq_len;

    fpqx_distilled_cache_t *dc = (fpqx_distilled_cache_t *)calloc(1, sizeof(*dc));
    dc->n_atoms = target_atoms;
    dc->head_dim = (int)head_dim;
    dc->atoms = (float *)calloc((size_t)target_atoms * head_dim, sizeof(float));
    dc->weights = (float *)calloc(target_atoms, sizeof(float));

    /* Initialize atoms via K-means++ */
    int *assignments = (int *)calloc(seq_len, sizeof(int));

    /* First atom = highest attention weight entry */
    int first = 0;
    if (attn_weights) {
        float max_w = -1.0f;
        for (size_t i = 0; i < seq_len; i++) {
            if (attn_weights[i] > max_w) { max_w = attn_weights[i]; first = (int)i; }
        }
    }
    memcpy(dc->atoms, cache + first * head_dim, head_dim * sizeof(float));

    /* K-means++ initialization for remaining atoms */
    float *min_dist = (float *)malloc(seq_len * sizeof(float));
    for (size_t i = 0; i < seq_len; i++) min_dist[i] = FLT_MAX;

    for (int c = 1; c < target_atoms; c++) {
        /* Update min distances to existing atoms */
        for (size_t i = 0; i < seq_len; i++) {
            const float *vec = cache + i * head_dim;
            const float *atom = dc->atoms + (c - 1) * head_dim;
            float d = 0.0f;
            for (size_t d2 = 0; d2 < head_dim; d2++) {
                float diff = vec[d2] - atom[d2];
                d += diff * diff;
            }
            if (d < min_dist[i]) min_dist[i] = d;
        }
        /* Weighted sampling by min_dist * attn_weight */
        double total_w = 0.0;
        for (size_t i = 0; i < seq_len; i++) {
            float w = attn_weights ? attn_weights[i] : 1.0f;
            total_w += (double)min_dist[i] * w;
        }
        double rnd = (double)(rand()) / RAND_MAX * total_w;
        double cum = 0.0;
        int chosen = 0;
        for (size_t i = 0; i < seq_len; i++) {
            float w = attn_weights ? attn_weights[i] : 1.0f;
            cum += (double)min_dist[i] * w;
            if (cum >= rnd) { chosen = (int)i; break; }
        }
        memcpy(dc->atoms + c * head_dim, cache + chosen * head_dim,
               head_dim * sizeof(float));
    }

    /* K-means iterations (attention-weighted) */
    int max_iter = 20;
    for (int iter = 0; iter < max_iter; iter++) {
        /* Assign each entry to nearest atom */
        int changed = 0;
        for (size_t i = 0; i < seq_len; i++) {
            const float *vec = cache + i * head_dim;
            float best_d = FLT_MAX;
            int best_c = 0;
            for (int c = 0; c < target_atoms; c++) {
                const float *atom = dc->atoms + c * head_dim;
                float d = 0.0f;
                for (size_t d2 = 0; d2 < head_dim; d2++) {
                    float diff = vec[d2] - atom[d2];
                    d += diff * diff;
                }
                if (d < best_d) { best_d = d; best_c = c; }
            }
            if (assignments[i] != best_c) { changed++; assignments[i] = best_c; }
        }
        if (changed == 0) break;

        /* Recompute atoms as attention-weighted centroids */
        memset(dc->atoms, 0, (size_t)target_atoms * head_dim * sizeof(float));
        memset(dc->weights, 0, target_atoms * sizeof(float));

        for (size_t i = 0; i < seq_len; i++) {
            int c = assignments[i];
            float w = attn_weights ? attn_weights[i] : 1.0f;
            dc->weights[c] += w;
            for (size_t d2 = 0; d2 < head_dim; d2++)
                dc->atoms[c * head_dim + d2] += w * cache[i * head_dim + d2];
        }
        for (int c = 0; c < target_atoms; c++) {
            if (dc->weights[c] > 1e-10f) {
                for (size_t d2 = 0; d2 < head_dim; d2++)
                    dc->atoms[c * head_dim + d2] /= dc->weights[c];
            }
        }
    }

    /* Store final assignments for use by fpqx_distill_reconstruct */
    dc->assignments = assignments;
    dc->n_seq = seq_len;
    free(min_dist);
    return dc;
}

void fpqx_distill_reconstruct(const fpqx_distilled_cache_t *dc,
                                float *output, size_t seq_len) {
    /* Nearest-centroid reconstruction using stored per-position assignments.
       Falls back to round-robin only if assignments are unavailable
       (e.g. struct populated without calling fpqx_distill). */
    for (size_t i = 0; i < seq_len; i++) {
        int atom_idx;
        if (dc->assignments && i < dc->n_seq) {
            atom_idx = dc->assignments[i];
        } else {
            /* fallback: find nearest atom by L2 distance */
            const float *out_vec = output + i * dc->head_dim;
            float best_d = FLT_MAX;
            atom_idx = 0;
            for (int c = 0; c < dc->n_atoms; c++) {
                const float *atom = dc->atoms + c * dc->head_dim;
                float d = 0.0f;
                for (int d2 = 0; d2 < dc->head_dim; d2++) {
                    float diff = out_vec[d2] - atom[d2];
                    d += diff * diff;
                }
                if (d < best_d) { best_d = d; atom_idx = c; }
            }
        }
        memcpy(output + i * dc->head_dim,
               dc->atoms + atom_idx * dc->head_dim,
               dc->head_dim * sizeof(float));
    }
}

void fpqx_distill_free(fpqx_distilled_cache_t *dc) {
    if (!dc) return;
    free(dc->atoms);
    free(dc->weights);
    free(dc->assignments);
    free(dc);
}


/* ═══════════════════════════════════════════════════════════════════
 * Λ — Adaptive Structure: profile()
 *
 * Analyze tensor statistics to determine the optimal operator
 * combination and bit budget. Uses empirical priors from the
 * ~1,790 production tensors compressed through FPQ v10.
 * ═══════════════════════════════════════════════════════════════════ */

fpqx_policy_t fpqx_profile(const float *W, size_t rows, size_t cols,
                             const char *name, int base_bits) {
    fpqx_policy_t pol;
    memset(&pol, 0, sizeof(pol));
    size_t total = rows * cols;

    /* Compute basic statistics */
    double sum = 0.0, sum2 = 0.0, sum4 = 0.0;
    for (size_t i = 0; i < total; i++) {
        double v = (double)W[i];
        sum += v;
        sum2 += v * v;
        sum4 += v * v * v * v;
    }
    double mean = sum / total;
    double var = sum2 / total - mean * mean;
    double std = sqrt(var > 0 ? var : 1e-12);
    double kurt = (sum4 / total) / (var * var) - 3.0;  /* excess kurtosis */
    pol.kurtosis = (float)kurt;

    /* Outlier fraction (|x| > 3σ) */
    int n_outlier = 0;
    float thresh = (float)(3.0 * std);
    for (size_t i = 0; i < total; i++) {
        if (fabsf(W[i] - (float)mean) > thresh) n_outlier++;
    }
    pol.outlier_fraction = (float)n_outlier / total;

    /* η_L via quick SVD probe (sample first few singular values) */
    if (rows > 1 && cols > 1) {
        pol.eta_L = bwa_get_eta_L(W, rows, cols, 0.25f);
    }

    /* Spectral gap: compute σ₁/σ₂ from a small SVD */
    {
        int k = 2;
        if ((int)rows < k || (int)cols < k) k = 1;
        float s2[2] = {0};
        float *tmpU = (float *)calloc(rows * 2, sizeof(float));
        float *tmpVt = (float *)calloc(2 * cols, sizeof(float));
        fpqx_svd_thin(W, (int)rows, (int)cols, k, tmpU, s2, tmpVt);
        pol.spectral_gap = (s2[1] > 1e-10f) ? s2[0] / s2[1] : 100.0f;
        free(tmpU); free(tmpVt);
    }

    /* ── Policy decision tree ── */

    bwa_tensor_type_t tt = bwa_classify_tensor(name);

    /* Bit allocation (inherited from v10 adaptive routing) */
    pol.recommended_bits = bwa_adaptive_bits(pol.eta_L, base_bits);

    /* Multiplicative manifold: enable when spectral gap is moderate
       (very high gap = one dominant direction, LR handles it;
        very low gap = flat spectrum, scaling won't help) */
    if (pol.spectral_gap > 2.0f && pol.spectral_gap < 50.0f &&
        total >= 4096 && pol.eta_L < 0.8f) {
        pol.use_scale = 1;
        /* Higher rank for flatter spectra */
        pol.scale_rank = (pol.spectral_gap < 5.0f) ? 4 : 2;
    }

    /* Predictive correction: enable for large tensors where
       the base captures significant structure (high η_L) —
       there's more to predict from */
    if (total >= 8192 && pol.eta_L > 0.1f &&
        (tt == BWA_TENSOR_FFN || tt == BWA_TENSOR_SELF_ATTN)) {
        pol.use_predictor = 1;
        pol.pred_rank = (pol.eta_L > 0.3f) ? 2 : 1;
    }

    /* Pruning keep ratio */
    pol.adaptive_keep = bwa_adaptive_keep_ratio(pol.eta_L, 0.50f);

    /* Assemble active ops mask */
    pol.active_ops = FPQX_OP_ADDITIVE | FPQX_OP_ADAPTIVE;
    if (pol.use_scale) pol.active_ops |= FPQX_OP_MULTIPLICATIVE;
    if (pol.use_predictor) pol.active_ops |= FPQX_OP_PREDICTIVE;

    return pol;
}


/* ═══════════════════════════════════════════════════════════════════
 * H — Hardware Structure: pack()
 *
 * Reorder quantized data for hardware-aligned access.
 * Groups scales along the inner (K) dimension so the matmul kernel
 * can reuse one scale for multiple output accumulations.
 * ═══════════════════════════════════════════════════════════════════ */

fpqx_packed_t *fpqx_pack(const float *data, size_t rows, size_t cols,
                           int bits, fpqx_pack_mode_t mode, size_t group_size) {
    if (group_size == 0) group_size = 128;
    size_t total = rows * cols;

    fpqx_packed_t *p = (fpqx_packed_t *)calloc(1, sizeof(*p));
    p->mode = mode;
    p->group_size = group_size;

    if (mode == FPQX_PACK_INNER_GROUP) {
        /* Group along cols (inner dimension for W × x) */
        size_t n_col_groups = (cols + group_size - 1) / group_size;
        p->n_groups = rows * n_col_groups;
        p->scales = (float *)calloc(p->n_groups, sizeof(float));

        /* Compute per-group scale (absmax) */
        size_t g = 0;
        for (size_t i = 0; i < rows; i++) {
            for (size_t jg = 0; jg < n_col_groups; jg++) {
                float amax = 0.0f;
                size_t j_start = jg * group_size;
                size_t j_end = j_start + group_size;
                if (j_end > cols) j_end = cols;
                for (size_t j = j_start; j < j_end; j++) {
                    float a = fabsf(data[i * cols + j]);
                    if (a > amax) amax = a;
                }
                p->scales[g++] = amax;
            }
        }

        /* Pack quantized values: symmetric uniform quantization */
        int levels = (1 << bits) - 1;
        float half_levels = (float)(levels / 2);
        size_t bits_total = total * bits;
        p->packed_bytes = (bits_total + 7) / 8;
        p->packed_data = (uint8_t *)calloc(p->packed_bytes, 1);

        size_t bit_pos = 0;
        g = 0;
        for (size_t i = 0; i < rows; i++) {
            for (size_t jg = 0; jg < n_col_groups; jg++) {
                float scale = p->scales[g++];
                if (scale < 1e-10f) scale = 1e-10f;
                size_t j_start = jg * group_size;
                size_t j_end = j_start + group_size;
                if (j_end > cols) j_end = cols;

                for (size_t j = j_start; j < j_end; j++) {
                    /* Quantize to [0, levels] */
                    float normalized = data[i * cols + j] / scale;
                    int q = (int)roundf((normalized + 1.0f) * half_levels);
                    if (q < 0) q = 0;
                    if (q > levels) q = levels;

                    /* Pack bits */
                    for (int b = 0; b < bits; b++) {
                        if (q & (1 << b))
                            p->packed_data[bit_pos / 8] |= (1 << (bit_pos % 8));
                        bit_pos++;
                    }
                }
            }
        }

        p->tile_rows = rows;
        p->tile_cols = cols;
    } else {
        /* Default: simple row-major packing */
        p->n_groups = (total + group_size - 1) / group_size;
        p->scales = (float *)calloc(p->n_groups, sizeof(float));

        for (size_t g2 = 0; g2 < p->n_groups; g2++) {
            float amax = 0.0f;
            size_t start = g2 * group_size;
            size_t end = start + group_size;
            if (end > total) end = total;
            for (size_t i = start; i < end; i++) {
                float a = fabsf(data[i]);
                if (a > amax) amax = a;
            }
            p->scales[g2] = amax;
        }

        int levels = (1 << bits) - 1;
        float half_levels = (float)(levels / 2);
        size_t bits_total = total * bits;
        p->packed_bytes = (bits_total + 7) / 8;
        p->packed_data = (uint8_t *)calloc(p->packed_bytes, 1);

        size_t bit_pos = 0;
        for (size_t i = 0; i < total; i++) {
            size_t g2 = i / group_size;
            float scale = p->scales[g2];
            if (scale < 1e-10f) scale = 1e-10f;
            float normalized = data[i] / scale;
            int q = (int)roundf((normalized + 1.0f) * half_levels);
            if (q < 0) q = 0;
            if (q > levels) q = levels;
            for (int b = 0; b < bits; b++) {
                if (q & (1 << b))
                    p->packed_data[bit_pos / 8] |= (1 << (bit_pos % 8));
                bit_pos++;
            }
        }

        p->tile_rows = rows;
        p->tile_cols = cols;
    }

    return p;
}

void fpqx_pack_free(fpqx_packed_t *p) {
    if (!p) return;
    free(p->packed_data);
    free(p->scales);
    free(p);
}


/* ═══════════════════════════════════════════════════════════════════
 * Unified Pipeline: fpqx_encode / fpqx_decode
 * ═══════════════════════════════════════════════════════════════════ */

fpqx_tensor_t *fpqx_encode(const float *W, size_t rows, size_t cols,
                             const char *name, int base_bits) {
    size_t total = rows * cols;

    fpqx_tensor_t *t = (fpqx_tensor_t *)calloc(1, sizeof(*t));
    strncpy(t->name, name, sizeof(t->name) - 1);
    t->rows = rows;
    t->cols = cols;

    /* ── Step 1: Profile (Λ) ── */
    t->policy = fpqx_profile(W, rows, cols, name, base_bits);
    t->active_ops = t->policy.active_ops;

    int skip_algebra = (total < FPQ_BLOCK_DIM * 2) || (rows <= 1) || (cols <= 1);

    /* ── Step 2: BWA Decompose + Prune + Correct ── */
    float *W_pruned = (float *)malloc(total * sizeof(float));
    float *L_base = NULL;

    if (!skip_algebra && total >= 512 && rows > 1 && cols > 1) {
        /* Full algebra path */
        bwa_prune(W, rows, cols, name,
                  t->policy.adaptive_keep, 0.25f,
                  BWA_PRUNE_HYBRID, W_pruned);

        /* Cache L for predictor */
        bwa_decomposition_t *dec = bwa_decompose(W, rows, cols, 0.25f);
        if (dec) {
            L_base = (float *)malloc(total * sizeof(float));
            memcpy(L_base, dec->L, total * sizeof(float));
            bwa_decomposition_free(dec);
        }
    } else {
        memcpy(W_pruned, W, total * sizeof(float));
    }

    /* ── Step 3: Additive encode (A = B + R + P) ── */
    t->additive = fpq_encode_tensor_v9(W_pruned, rows, cols, name,
                                        t->policy.recommended_bits);

    /* Decode to get additive reconstruction */
    float *W_additive = (float *)malloc(total * sizeof(float));
    if (t->additive->pid_alpha == -9.0f)
        fpq_decode_tensor_v9(t->additive, W_additive);
    else if (t->additive->sbb_scale_delta)
        fpq_decode_tensor_v8(t->additive, W_additive);
    else
        fpq_decode_tensor_v4(t->additive, W_additive);

    t->cosine_pre_scale = fpq_cosine_sim(W, W_additive, total);

    /* ── Step 4: Multiplicative correction (M) ── */
    float *W_current = (float *)malloc(total * sizeof(float));
    memcpy(W_current, W_additive, total * sizeof(float));

    if (t->policy.use_scale && !skip_algebra) {
        t->scale = fpqx_scale_learn(W, W_additive, rows, cols,
                                     t->policy.scale_rank);
        fpqx_scale_apply(W_additive, t->scale, W_current);
        t->cosine_post_scale = fpq_cosine_sim(W, W_current, total);

        /* Only keep scale if it actually improved cosine */
        if (t->cosine_post_scale <= t->cosine_pre_scale + 1e-7f) {
            fpqx_scale_free(t->scale);
            t->scale = NULL;
            t->active_ops &= ~FPQX_OP_MULTIPLICATIVE;
            memcpy(W_current, W_additive, total * sizeof(float));
            t->cosine_post_scale = t->cosine_pre_scale;
        }
    } else {
        t->cosine_post_scale = t->cosine_pre_scale;
    }

    /* ── Step 5: Predictive correction (Π) ── */
    if (t->policy.use_predictor && L_base && !skip_algebra) {
        t->predictor = fpqx_predict_learn(W, W_current, L_base,
                                            rows, cols, t->policy.pred_rank);
        float *W_predicted = (float *)malloc(total * sizeof(float));
        memcpy(W_predicted, W_current, total * sizeof(float));
        fpqx_predict_apply(W_predicted, t->predictor, L_base, rows, cols);

        float cos_predicted = fpq_cosine_sim(W, W_predicted, total);

        /* Only keep predictor if it helped */
        if (cos_predicted > t->cosine_post_scale + 1e-7f) {
            memcpy(W_current, W_predicted, total * sizeof(float));
        } else {
            fpqx_predict_free(t->predictor);
            t->predictor = NULL;
            t->active_ops &= ~FPQX_OP_PREDICTIVE;
        }
        free(W_predicted);
    }

    t->cosine_final = fpq_cosine_sim(W, W_current, total);

    /* Compute effective bpw */
    double total_bits = (double)t->additive->total_bits;
    if (t->scale) {
        total_bits += (double)(rows + cols) * t->scale->scale_rank * 16.0;
    }
    if (t->predictor && t->predictor->mode != FPQX_PREDICT_NONE) {
        total_bits += (double)cols * 16.0;  /* per-column scale stored fp16 */
    }
    t->bpw = (float)(total_bits / total);

    free(W_pruned); free(W_additive); free(W_current); free(L_base);
    return t;
}

void fpqx_decode(const fpqx_tensor_t *t, float *output) {
    /* Step 1: Decode additive (B + R + P) */
    if (t->additive->pid_alpha == -9.0f)
        fpq_decode_tensor_v9(t->additive, output);
    else if (t->additive->sbb_scale_delta)
        fpq_decode_tensor_v8(t->additive, output);
    else
        fpq_decode_tensor_v4(t->additive, output);

    /* Step 2: Apply multiplicative scale */
    if (t->scale) {
        size_t total = t->rows * t->cols;
        float *scaled = (float *)malloc(total * sizeof(float));
        fpqx_scale_apply(output, t->scale, scaled);
        memcpy(output, scaled, total * sizeof(float));
        free(scaled);
    }

    /* Step 3: Apply predictive correction
       (Requires L_base at decode time — for offline use we skip,
        for inference the base is reconstructed from the LR factors) */
    /* Note: In the current offline roundtrip, prediction was already
       baked in during encode. For true runtime prediction, the inference
       engine would call fpqx_predict_apply with the LR factors. */
}

void fpqx_tensor_free(fpqx_tensor_t *t) {
    if (!t) return;
    if (t->additive) fpq_tensor_free(t->additive);
    fpqx_scale_free(t->scale);
    fpqx_predict_free(t->predictor);
    fpqx_distill_free(t->distilled);
    fpqx_pack_free(t->packed);
    free(t);
}


/* ═══════════════════════════════════════════════════════════════════
 * I — SPECTRAL LATTICE INFERENCE (SLI)
 *
 * Compute y = W @ x without reconstructing dense weights.
 *
 * Mathematical basis (proved exact in Python + SPECTRAL_LATTICE_INFERENCE.md):
 *
 *   The v9 decode chain ends with: w = signs ⊙ IFWHT(z)
 *   where z = unwarp(corrected / ls * wn) * rms + qjl_recon
 *
 *   Since IFWHT = FWHT/n and signs are self-inverse:
 *     <w, x> = z^T · (FWHT(signs ⊙ x) / n)
 *            = z^T · x̃    where x̃ = FWHT(signs ⊙ x) / n
 *
 *   z decomposes into: z_base (E8+tile, through unwarp) + z_qjl
 *   Each scored against x̃ independently.
 *
 * Bandwidth: 64 bytes per 256-element block (vs 512 BF16) → 8× reduction
 * Quality: mathematically identical to full decode + matmul
 * ═══════════════════════════════════════════════════════════════════ */

/* V8 constants (must match v4_optimizations.c) */
#define SLI_BLOCK_DIM       256
#define SLI_E8_DIM          8
#define SLI_E8_GROUPS       32
#define SLI_E8_PAIRS        16
#define SLI_TILE_DIM        16
#define SLI_RVQ_TILES       256
#define SLI_MU_BETA         8.0f
#define SLI_QJL_PROJECTIONS 64

static int fpqx_sli_effective_k(const fpqx_sli_ctx_t *ctx, const fpq_tensor_t *enc) {
    size_t n_blocks;
    size_t e8_flat_size;
    size_t tile_cb_off;
    int effective_k = SLI_RVQ_TILES;
    if (!ctx || !enc) return effective_k;
    n_blocks = enc->n_blocks;
    e8_flat_size = n_blocks * SLI_BLOCK_DIM;
    tile_cb_off = ctx->e8_off + e8_flat_size;
    {
        size_t test_cb = (size_t)effective_k * SLI_TILE_DIM;
        size_t test_ek = tile_cb_off + test_cb + n_blocks * SLI_E8_PAIRS;
        float stored = enc->sbb_scale_delta[test_ek];
        if (stored >= 16.0f && stored <= 256.0f)
            effective_k = (int)stored;
    }
    return effective_k;
}

static size_t fpqx_sli_tile_idx_off(const fpqx_sli_ctx_t *ctx,
                                    const fpq_tensor_t *enc,
                                    int effective_k) {
    size_t e8_flat_size;
    size_t tile_cb_off;
    if (!ctx || !enc) return 0;
    e8_flat_size = enc->n_blocks * SLI_BLOCK_DIM;
    tile_cb_off = ctx->e8_off + e8_flat_size;
    return tile_cb_off + (size_t)effective_k * SLI_TILE_DIM;
}

static float sli_unwarp(float y, float beta);

static void fpqx_sli_score_probe_emit(const char *kind,
                                      const fpqx_sli_ctx_t *ctx,
                                      const fpq_tensor_t *enc,
                                      size_t row,
                                      size_t block,
                                      size_t col_offset,
                                      size_t this_dim,
                                      float fast_score,
                                      float slow_score,
                                      int shared_qkv_active) {
    const fpqx_sli_score_probe_config_t *cfg = &g_fpqx_sli_score_probe;
    if (!cfg->enabled || cfg->jsonl_path[0] == '\0' || !ctx || !ctx->tensor) return;
    float abs_delta = fabsf(fast_score - slow_score);
    int layer = fpqx_probe_layer_from_name(ctx->tensor->name);
    const char *projection = fpqx_probe_projection_from_name(ctx->tensor->name);
    int effective_k = fpqx_sli_effective_k(ctx, enc);
    size_t tile_idx_off = fpqx_sli_tile_idx_off(ctx, enc, effective_k);
    int tile_idx_min = -1;
    int tile_idx_max = -1;
    float rms = 0.0f;
    float wnorm = 0.0f;
    int qjl_present = 0;
    float qjl_residual_norm = 0.0f;
    uint64_t block_seed = enc ? (enc->haar_seed ^ (uint64_t)block) : 0;
    if (enc && block != SIZE_MAX && block < enc->n_blocks) {
        rms = enc->coord_scales ? enc->coord_scales[block] : 0.0f;
        wnorm = enc->sbb_scale_delta ? enc->sbb_scale_delta[ctx->v8_base + block] : 0.0f;
        qjl_present = (enc->qjl && enc->qjl[block]) ? 1 : 0;
        qjl_residual_norm = (enc->coord_residual_norms && block < enc->n_blocks)
            ? enc->coord_residual_norms[block] : 0.0f;
        if (enc->sbb_scale_delta) {
            for (int p = 0; p < SLI_E8_PAIRS; p++) {
                int ti = (int)enc->sbb_scale_delta[tile_idx_off + block * SLI_E8_PAIRS + (size_t)p];
                if (tile_idx_min < 0 || ti < tile_idx_min) tile_idx_min = ti;
                if (tile_idx_max < 0 || ti > tile_idx_max) tile_idx_max = ti;
            }
        }
    }

    pthread_mutex_lock(&g_fpqx_sli_score_probe_mu);
    FILE *fp = fopen(cfg->jsonl_path, "a");
    if (fp) {
        fprintf(fp,
                "{\"kind\":\"%s\",\"tensor\":\"%s\",\"row\":%zu,\"block\":%zu,"
                "\"layer\":%d,\"projection\":\"%s\",\"rows\":%zu,\"cols\":%zu,"
                "\"blocks_per_row\":%zu,\"block_range_start\":%zu,\"block_range_end\":%zu,"
                "\"fast_score\":%.9g,\"slow_score\":%.9g,\"abs_delta\":%.9g,"
                "\"native_row_ref\":null,\"z_precomputed\":%s,\"shared_qkv_active\":%s,"
                "\"coord_bits\":%u,\"effective_k\":%d,\"qjl_present\":%s,"
                "\"qjl_residual_norm\":%.9g,\"rms\":%.9g,\"wnorm\":%.9g,"
                "\"tile_idx_min\":%d,\"tile_idx_max\":%d,\"e8_off\":%zu,"
                "\"block_seed\":%" PRIu64 ",\"runtime_path_name\":\"%s\"}\n",
                kind,
                ctx->tensor->name,
                row,
                block,
                layer,
                projection,
                ctx->rows,
                ctx->cols,
                ctx->blocks_per_row,
                col_offset,
                col_offset + this_dim,
                fast_score,
                slow_score,
                abs_delta,
                ctx->z_precomputed ? "true" : "false",
                shared_qkv_active ? "true" : "false",
                enc ? enc->coord_bits : 0u,
                effective_k,
                qjl_present ? "true" : "false",
                qjl_residual_norm,
                rms,
                wnorm,
                tile_idx_min,
                tile_idx_max,
                ctx->e8_off,
                block_seed,
                shared_qkv_active ? "shared-qkv-fast-vs-slow" : "nonshared-fast-vs-slow");
        fclose(fp);
    }
    pthread_mutex_unlock(&g_fpqx_sli_score_probe_mu);
}

static void fpqx_sli_common_probe_emit(const char *kind,
                                       const fpqx_sli_ctx_t *ctx,
                                       const fpq_tensor_t *enc,
                                       size_t row,
                                       size_t block,
                                       size_t col_offset,
                                       size_t this_dim,
                                       float value_a,
                                       float value_b,
                                       float value_c,
                                       uint64_t block_seed) {
    const fpqx_sli_score_probe_config_t *cfg = &g_fpqx_sli_score_probe;
    int layer = fpqx_probe_layer_from_name(ctx && ctx->tensor ? ctx->tensor->name : NULL);
    const char *projection = fpqx_probe_projection_from_name(ctx && ctx->tensor ? ctx->tensor->name : NULL);
    int effective_k = fpqx_sli_effective_k(ctx, enc);
    size_t tile_idx_off = fpqx_sli_tile_idx_off(ctx, enc, effective_k);
    int tile_idx_min = -1;
    int tile_idx_max = -1;
    float rms = 0.0f;
    float wnorm = 0.0f;
    float residual_norm = 0.0f;
    int qjl_present = 0;
    if (!cfg->enabled || cfg->jsonl_path[0] == '\0' || !ctx || !ctx->tensor || !enc) return;
    if (block < enc->n_blocks) {
        rms = enc->coord_scales ? enc->coord_scales[block] : 0.0f;
        wnorm = enc->sbb_scale_delta ? enc->sbb_scale_delta[ctx->v8_base + block] : 0.0f;
        residual_norm = (enc->coord_residual_norms && block < enc->n_blocks) ? enc->coord_residual_norms[block] : 0.0f;
        qjl_present = (enc->qjl && enc->qjl[block]) ? 1 : 0;
        if (enc->sbb_scale_delta) {
            for (int p = 0; p < SLI_E8_PAIRS; p++) {
                int ti = (int)enc->sbb_scale_delta[tile_idx_off + block * SLI_E8_PAIRS + (size_t)p];
                if (tile_idx_min < 0 || ti < tile_idx_min) tile_idx_min = ti;
                if (tile_idx_max < 0 || ti > tile_idx_max) tile_idx_max = ti;
            }
        }
    }
    pthread_mutex_lock(&g_fpqx_sli_score_probe_mu);
    FILE *fp = fopen(cfg->jsonl_path, "a");
    if (fp) {
        fprintf(fp,
                "{\"kind\":\"%s\",\"tensor\":\"%s\",\"row\":%zu,\"block\":%zu,"
                "\"layer\":%d,\"projection\":\"%s\",\"rows\":%zu,\"cols\":%zu,"
                "\"blocks_per_row\":%zu,\"block_range_start\":%zu,\"block_range_end\":%zu,"
                "\"coord_bits\":%u,\"effective_k\":%d,\"qjl_present\":%s,"
                "\"qjl_residual_norm\":%.9g,\"rms\":%.9g,\"wnorm\":%.9g,"
                "\"tile_idx_min\":%d,\"tile_idx_max\":%d,\"e8_off\":%zu,"
                "\"block_seed\":%" PRIu64 ",\"value_a\":%.9g,\"value_b\":%.9g,"
                "\"value_c\":%.9g,\"runtime_path_name\":\"common-sli-component\"}\n",
                kind,
                ctx->tensor->name,
                row,
                block,
                layer,
                projection,
                ctx->rows,
                ctx->cols,
                ctx->blocks_per_row,
                col_offset,
                col_offset + this_dim,
                enc->coord_bits,
                effective_k,
                qjl_present ? "true" : "false",
                residual_norm,
                rms,
                wnorm,
                tile_idx_min,
                tile_idx_max,
                ctx->e8_off,
                block_seed,
                value_a,
                value_b,
                value_c);
        fclose(fp);
    }
    pthread_mutex_unlock(&g_fpqx_sli_score_probe_mu);
}

static float fpqx_sli_score_block_scalar_exact(const float *e8_pts,
                                               const float *tile_data,
                                               const float *tile_indices,
                                               const float *x_spectral,
                                               float rms,
                                               float wnorm,
                                               float lattice_scale,
                                               int effective_k);

static float fpqx_sli_score_block_slow(const fpqx_sli_ctx_t *ctx,
                                       const fpq_tensor_t *enc,
                                       const float *x_eff,
                                       size_t b,
                                       size_t col_offset,
                                       size_t this_dim) {
    if (!ctx || !enc || !x_eff) return 0.0f;

    int coord_bits = (int)enc->coord_bits;
    float lattice_scale = 8.0f * (float)coord_bits;
    size_t v8_base = ctx->v8_base;
    size_t e8_off = ctx->e8_off;
    size_t e8_flat_size = enc->n_blocks * SLI_BLOCK_DIM;
    size_t tile_cb_off = e8_off + e8_flat_size;

    int effective_k = SLI_RVQ_TILES;
    {
        size_t test_cb = (size_t)effective_k * SLI_TILE_DIM;
        size_t test_ek = tile_cb_off + test_cb + enc->n_blocks * SLI_E8_PAIRS;
        float stored = enc->sbb_scale_delta[test_ek];
        if (stored >= 16.0f && stored <= 256.0f)
            effective_k = (int)stored;
    }
    size_t tile_cb_size = (size_t)effective_k * SLI_TILE_DIM;
    size_t tile_idx_off = tile_cb_off + tile_cb_size;
    const float *tile_data = enc->sbb_scale_delta + tile_cb_off;
    const float *e8_pts = enc->sbb_scale_delta + e8_off + b * SLI_BLOCK_DIM;
    float rms = enc->coord_scales[b];
    float wnorm = enc->sbb_scale_delta[v8_base + b];
    float raw_e8_l1 = 0.0f;
    float tile_l1 = 0.0f;
    float signed_l2 = 0.0f;
    float spectral_l2 = 0.0f;

    float x_block[SLI_BLOCK_DIM];
    float x_spectral[SLI_BLOCK_DIM];
    memset(x_block, 0, sizeof(x_block));
    if (this_dim > 0)
        memcpy(x_block, x_eff + col_offset, this_dim * sizeof(float));
    uint64_t block_seed = enc->haar_seed ^ (uint64_t)b;
    fpq_neon_random_signs_256(x_block, block_seed);
    for (size_t i = 0; i < SLI_BLOCK_DIM; i++) signed_l2 += x_block[i] * x_block[i];
    fpq_neon_fwht_256(x_block);
    memcpy(x_spectral, x_block, sizeof(x_spectral));
    for (size_t i = 0; i < SLI_BLOCK_DIM; i++) spectral_l2 += x_spectral[i] * x_spectral[i];

    float tile_indices_f[SLI_E8_PAIRS];
    for (int p = 0; p < SLI_E8_PAIRS; p++) {
        tile_indices_f[p] =
            enc->sbb_scale_delta[tile_idx_off + b * SLI_E8_PAIRS + (size_t)p];
        tile_l1 += fabsf(tile_indices_f[p]);
    }
    for (size_t i = 0; i < SLI_BLOCK_DIM; i++) raw_e8_l1 += fabsf(e8_pts[i]);
    float base_tile_score = fpq_neon_score_block(
        e8_pts, tile_data, tile_indices_f, x_spectral,
        rms, wnorm, lattice_scale, effective_k);
    float scalar_base_tile_score = 0.0f;

    float qjl_score = 0.0f;
    if (enc->qjl && enc->qjl[b] &&
        enc->coord_residual_norms &&
        enc->coord_residual_norms[b] > 1e-10f) {
        qjl_score = fpq_neon_qjl_score(
            x_spectral, enc->qjl[b]->bits[0],
            enc->qjl[b]->proj_seed,
            enc->coord_residual_norms[b],
            enc->qjl[b]->n_projections);
    }
    if (fpqx_sli_score_probe_match(ctx, b / ctx->blocks_per_row)) {
        scalar_base_tile_score = fpqx_sli_score_block_scalar_exact(
            e8_pts, tile_data, tile_indices_f, x_spectral,
            rms, wnorm, lattice_scale, effective_k);
        fpqx_sli_common_probe_emit("sli_common_probe_raw_e8", ctx, enc, b / ctx->blocks_per_row,
                                   b, col_offset, this_dim, raw_e8_l1, 0.0f, 0.0f, block_seed);
        fpqx_sli_common_probe_emit("sli_common_probe_tile", ctx, enc, b / ctx->blocks_per_row,
                                   b, col_offset, this_dim, tile_l1, (float)effective_k, 0.0f, block_seed);
        fpqx_sli_common_probe_emit("sli_common_probe_qjl", ctx, enc, b / ctx->blocks_per_row,
                                   b, col_offset, this_dim, qjl_score,
                                   enc->coord_residual_norms ? enc->coord_residual_norms[b] : 0.0f,
                                   enc->qjl && enc->qjl[b] ? (float)enc->qjl[b]->n_projections : 0.0f,
                                   block_seed);
        fpqx_sli_common_probe_emit("sli_common_probe_scale", ctx, enc, b / ctx->blocks_per_row,
                                   b, col_offset, this_dim, rms, wnorm, lattice_scale, block_seed);
        fpqx_sli_common_probe_emit("sli_common_probe_sign_fwht", ctx, enc, b / ctx->blocks_per_row,
                                   b, col_offset, this_dim, sqrtf(signed_l2), sqrtf(spectral_l2), 0.0f, block_seed);
        fpqx_sli_common_probe_emit("sli_common_probe_scalar_exact", ctx, enc, b / ctx->blocks_per_row,
                                   b, col_offset, this_dim,
                                   scalar_base_tile_score,
                                   base_tile_score,
                                   scalar_base_tile_score - base_tile_score,
                                   block_seed);
        fpqx_sli_common_probe_emit("sli_common_probe_block_total", ctx, enc, b / ctx->blocks_per_row,
                                   b, col_offset, this_dim, base_tile_score, qjl_score, base_tile_score + qjl_score,
                                   block_seed);
    }
    return base_tile_score + qjl_score;
}

static float fpqx_sli_score_block_scalar_exact(const float *e8_pts,
                                               const float *tile_data,
                                               const float *tile_indices,
                                               const float *x_spectral,
                                               float rms,
                                               float wnorm,
                                               float lattice_scale,
                                               int effective_k) {
    float acc = 0.0f;
    for (int p = 0; p < SLI_E8_PAIRS; p++) {
        int ti = (int)tile_indices[p];
        if (ti < 0) ti = 0;
        if (ti >= effective_k) ti = effective_k - 1;
        size_t pair_base = (size_t)p * SLI_TILE_DIM;
        const float *tile = tile_data + (size_t)ti * SLI_TILE_DIM;
        for (int d = 0; d < SLI_TILE_DIM; d++) {
            float corrected = e8_pts[pair_base + (size_t)d] + tile[d];
            float lat_val = corrected / lattice_scale * wnorm;
            float unwarped = sli_unwarp(lat_val, SLI_MU_BETA);
            acc += (unwarped * rms) * x_spectral[pair_base + (size_t)d];
        }
    }
    return acc;
}


/* μ-law inverse warp (matches v7_warp_inverse in v4_optimizations.c) */
static float sli_unwarp(float y, float beta) {
    float lnorm = logf(1.0f + beta);
    float ay = fabsf(y);
    float x = (expf(ay * lnorm) - 1.0f) / beta;
    return (y < 0) ? -x : x;
}


/* XOR-shift64 RNG matching qjl.c */
static uint64_t sli_xorshift64(uint64_t *state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}


/* Generate random signs matching fpq_random_signs */
static void sli_random_signs(float *signs, size_t n, uint64_t seed) {
    uint64_t state = seed ? seed : 0x5DEECE66DUL;
    for (size_t i = 0; i < n; i += 64) {
        uint64_t bits = sli_xorshift64(&state);
        size_t end = (i + 64 < n) ? i + 64 : n;
        for (size_t j = i; j < end; j++)
            signs[j] = ((bits >> (j - i)) & 1) ? -1.0f : 1.0f;
    }
}


/* Generate Rademacher projection matching qjl.c generate_projection */
static void sli_generate_projection(float *proj, size_t n,
                                     uint64_t seed, size_t proj_idx) {
    uint64_t state = seed ^ (proj_idx * 0x9E3779B97F4A7C15ULL);
    float scale = 1.0f / sqrtf((float)n);
    for (size_t i = 0; i < n; i += 64) {
        uint64_t bits = sli_xorshift64(&state);
        size_t end = (i + 64 < n) ? i + 64 : n;
        for (size_t j = i; j < end; j++)
            proj[j] = ((bits >> (j - i)) & 1) ? scale : -scale;
    }
}


/* ── Fused spectral bypass scoring kernel ──
 *
 * Computes: z'^T · FWHT_raw(signs ⊙ x_block)
 * where z' = z / √n (normalization folded into z at precompute time).
 *
 * Optimizations vs separate sign/FWHT/dot calls:
 *  • XOR sign-bit flip (no float multiply)
 *  • Fully vectorized FWHT including stride=1,2 (vld2q / lo-hi split)
 *  • No normalization pass (folded into z)
 *  • 4-accumulator dot product
 *  • Single inline function (zero call overhead)
 */
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>

/* Unnormalized WHT on 256 floats (no 1/√n — normalization is already in z).
 * Called once per block at prepare time to move FWHT cost out of inference. */
static void fwht_256_raw_neon(float * __restrict__ x) {
    /* Pass 0: stride=1 */
    for (size_t i = 0; i < 256; i += 8) {
        float32x4x2_t ab = vld2q_f32(x + i);
        float32x4_t s = vaddq_f32(ab.val[0], ab.val[1]);
        float32x4_t d = vsubq_f32(ab.val[0], ab.val[1]);
        ab.val[0] = s; ab.val[1] = d;
        vst2q_f32(x + i, ab);
    }
    /* Pass 1: stride=2 */
    for (size_t i = 0; i < 256; i += 4) {
        float32x4_t v = vld1q_f32(x + i);
        float32x2_t lo = vget_low_f32(v);
        float32x2_t hi = vget_high_f32(v);
        vst1q_f32(x + i, vcombine_f32(vadd_f32(lo, hi), vsub_f32(lo, hi)));
    }
    /* Passes 2–7: stride=4..128 */
    for (size_t stride = 4; stride < 256; stride <<= 1) {
        for (size_t base = 0; base < 256; base += stride * 2) {
            for (size_t k = 0; k < stride; k += 4) {
                float32x4_t a = vld1q_f32(x + base + k);
                float32x4_t b = vld1q_f32(x + base + k + stride);
                vst1q_f32(x + base + k,          vaddq_f32(a, b));
                vst1q_f32(x + base + k + stride, vsubq_f32(a, b));
            }
        }
    }
}
#define FWHT_Z_BLOCK_INPLACE fwht_256_raw_neon

static inline float sli_fast_block_score(
    const float *z_b,       /* precomputed FWHT(z)[256], already scaled by 1/√n */
    const float *x_src,     /* input[col_offset..] */
    size_t dim,             /* actual element count (may be < 256 for last block) */
    uint64_t block_seed)
{
    float __attribute__((aligned(16))) x[256];

    /* Copy input, zero-pad if needed */
    if (dim >= 256) {
        memcpy(x, x_src, 256 * sizeof(float));
    } else {
        memcpy(x, x_src, dim * sizeof(float));
        memset(x + dim, 0, (256 - dim) * sizeof(float));
    }

    /* ── Apply random signs via XOR on sign bit ── */
    uint64_t state = block_seed ? block_seed : 0x5DEECE66DUL;
    for (size_t i = 0; i < 256; i += 64) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        for (size_t k = i; k < i + 64; k += 4) {
            size_t bit_off = k - i;
            uint32_t m0 = ((uint32_t)((state >> (bit_off    )) & 1)) << 31;
            uint32_t m1 = ((uint32_t)((state >> (bit_off + 1)) & 1)) << 31;
            uint32_t m2 = ((uint32_t)((state >> (bit_off + 2)) & 1)) << 31;
            uint32_t m3 = ((uint32_t)((state >> (bit_off + 3)) & 1)) << 31;
            uint32_t mm[4] = {m0, m1, m2, m3};
            uint32x4_t mask = vld1q_u32(mm);
            uint32x4_t xv = vld1q_u32((const uint32_t *)(x + k));
            vst1q_u32((uint32_t *)(x + k), veorq_u32(xv, mask));
        }
    }

    /* FWHT removed: z_b now stores FWHT(z'), computed once at prepare time.
     * score = FWHT(z')^T · (signs ⊙ x)  ==  z'^T · FWHT(signs ⊙ x)  ✓ */

    /* ── Dot product: FWHT(z')^T · (signs ⊙ x) ── */
    float32x4_t acc0 = vdupq_n_f32(0.0f);
    float32x4_t acc1 = vdupq_n_f32(0.0f);
    float32x4_t acc2 = vdupq_n_f32(0.0f);
    float32x4_t acc3 = vdupq_n_f32(0.0f);
    for (size_t i = 0; i < 256; i += 16) {
        acc0 = vmlaq_f32(acc0, vld1q_f32(z_b + i),     vld1q_f32(x + i));
        acc1 = vmlaq_f32(acc1, vld1q_f32(z_b + i + 4), vld1q_f32(x + i + 4));
        acc2 = vmlaq_f32(acc2, vld1q_f32(z_b + i + 8), vld1q_f32(x + i + 8));
        acc3 = vmlaq_f32(acc3, vld1q_f32(z_b + i +12), vld1q_f32(x + i +12));
    }
    acc0 = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
    return vaddvq_f32(acc0);
}

static inline float sli_fast_block_score_f16(
    const __fp16 *z_b,
    const float *x_src,
    size_t dim,
    uint64_t block_seed)
{
    float __attribute__((aligned(16))) x[256];

    if (dim >= 256) {
        memcpy(x, x_src, 256 * sizeof(float));
    } else {
        memcpy(x, x_src, dim * sizeof(float));
        memset(x + dim, 0, (256 - dim) * sizeof(float));
    }

    uint64_t state = block_seed ? block_seed : 0x5DEECE66DUL;
    for (size_t i = 0; i < 256; i += 64) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        for (size_t k = i; k < i + 64; k += 4) {
            size_t bit_off = k - i;
            uint32_t m0 = ((uint32_t)((state >> (bit_off    )) & 1)) << 31;
            uint32_t m1 = ((uint32_t)((state >> (bit_off + 1)) & 1)) << 31;
            uint32_t m2 = ((uint32_t)((state >> (bit_off + 2)) & 1)) << 31;
            uint32_t m3 = ((uint32_t)((state >> (bit_off + 3)) & 1)) << 31;
            uint32_t mm[4] = {m0, m1, m2, m3};
            uint32x4_t mask = vld1q_u32(mm);
            uint32x4_t xv = vld1q_u32((const uint32_t *)(x + k));
            vst1q_u32((uint32_t *)(x + k), veorq_u32(xv, mask));
        }
    }

    float32x4_t acc0 = vdupq_n_f32(0.0f);
    float32x4_t acc1 = vdupq_n_f32(0.0f);
    float32x4_t acc2 = vdupq_n_f32(0.0f);
    float32x4_t acc3 = vdupq_n_f32(0.0f);
    for (size_t i = 0; i < 256; i += 16) {
        float16x8_t zh0 = vld1q_f16(z_b + i);
        float16x8_t zh1 = vld1q_f16(z_b + i + 8);
        float32x4_t z0 = vcvt_f32_f16(vget_low_f16(zh0));
        float32x4_t z1 = vcvt_f32_f16(vget_high_f16(zh0));
        float32x4_t z2 = vcvt_f32_f16(vget_low_f16(zh1));
        float32x4_t z3 = vcvt_f32_f16(vget_high_f16(zh1));
        acc0 = vmlaq_f32(acc0, z0, vld1q_f32(x + i));
        acc1 = vmlaq_f32(acc1, z1, vld1q_f32(x + i + 4));
        acc2 = vmlaq_f32(acc2, z2, vld1q_f32(x + i + 8));
        acc3 = vmlaq_f32(acc3, z3, vld1q_f32(x + i + 12));
    }
    acc0 = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
    return vaddvq_f32(acc0);
}

#ifdef FPQ_INT8_SDOT
/*
 * INT8 SDOT block score for ARM with __ARM_FEATURE_DOTPROD.
 *
 * z_i8:   quantized FWHT(z')[256] stored as INT8 at prepare time.
 * z_scale: per-block abs-max / 127.0 for z.
 * x_src:  FP32 activations [dim].
 * dim:    usable elements (≤256); zero-padded to 256.
 * block_seed: xorshift seed for sign flip on x.
 *
 * After sign-flip we quantize x to INT8, compute vdotq_s32, then
 * scale by z_scale * x_scale to recover the FP32 score.
 */
static inline float sli_fast_block_score_i8(
    const int8_t *z_i8,
    float z_scale,
    const float *x_src,
    size_t dim,
    uint64_t block_seed)
{
    float __attribute__((aligned(16))) xf[256];

    /* Copy/zero-pad input */
    if (dim >= 256) {
        memcpy(xf, x_src, 256 * sizeof(float));
    } else {
        memcpy(xf, x_src, dim * sizeof(float));
        memset(xf + dim, 0, (256 - dim) * sizeof(float));
    }

    /* Apply random sign flips to xf (identical logic to FP32 path) */
    uint64_t state = block_seed ? block_seed : 0x5DEECE66DUL;
    for (size_t i = 0; i < 256; i += 64) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        for (size_t k = i; k < i + 64; k += 4) {
            size_t bit_off = k - i;
            uint32_t m0 = ((uint32_t)((state >> (bit_off    )) & 1)) << 31;
            uint32_t m1 = ((uint32_t)((state >> (bit_off + 1)) & 1)) << 31;
            uint32_t m2 = ((uint32_t)((state >> (bit_off + 2)) & 1)) << 31;
            uint32_t m3 = ((uint32_t)((state >> (bit_off + 3)) & 1)) << 31;
            uint32_t mm[4] = {m0, m1, m2, m3};
            uint32x4_t mask = vld1q_u32(mm);
            uint32x4_t xv   = vld1q_u32((const uint32_t *)(xf + k));
            vst1q_u32((uint32_t *)(xf + k), veorq_u32(xv, mask));
        }
    }

    /* Quantize signed-xf to INT8: find abs-max then scale */
    float32x4_t vmax = vdupq_n_f32(0.0f);
    for (size_t i = 0; i < 256; i += 4)
        vmax = vmaxq_f32(vmax, vabsq_f32(vld1q_f32(xf + i)));
    float xmax = vmaxvq_f32(vmax);
    if (xmax < 1e-20f) return 0.0f;
    float x_scale = xmax / 127.0f;
    float inv_x   = 1.0f / x_scale;

    int8_t __attribute__((aligned(16))) xi[256];
    float32x4_t vscale = vdupq_n_f32(inv_x);
    for (size_t i = 0; i < 256; i += 16) {
        /* Load 16 floats, scale, convert to int32, narrow to int8 */
        int32x4_t a = vcvtnq_s32_f32(vmulq_f32(vld1q_f32(xf + i),      vscale));
        int32x4_t b = vcvtnq_s32_f32(vmulq_f32(vld1q_f32(xf + i + 4),  vscale));
        int32x4_t c = vcvtnq_s32_f32(vmulq_f32(vld1q_f32(xf + i + 8),  vscale));
        int32x4_t d = vcvtnq_s32_f32(vmulq_f32(vld1q_f32(xf + i + 12), vscale));
        int16x8_t ab16 = vcombine_s16(vmovn_s32(a), vmovn_s32(b));
        int16x8_t cd16 = vcombine_s16(vmovn_s32(c), vmovn_s32(d));
        vst1q_s8(xi + i, vcombine_s8(vmovn_s16(ab16), vmovn_s16(cd16)));
    }

    /* INT8 dot product via SDOT: accumulate 4 INT8 lanes per cycle */
    int32x4_t acc0 = vdupq_n_s32(0);
    int32x4_t acc1 = vdupq_n_s32(0);
    int32x4_t acc2 = vdupq_n_s32(0);
    int32x4_t acc3 = vdupq_n_s32(0);
    for (size_t i = 0; i < 256; i += 64) {
        acc0 = vdotq_s32(acc0, vld1q_s8(z_i8 + i),      vld1q_s8(xi + i));
        acc1 = vdotq_s32(acc1, vld1q_s8(z_i8 + i + 16), vld1q_s8(xi + i + 16));
        acc2 = vdotq_s32(acc2, vld1q_s8(z_i8 + i + 32), vld1q_s8(xi + i + 32));
        acc3 = vdotq_s32(acc3, vld1q_s8(z_i8 + i + 48), vld1q_s8(xi + i + 48));
    }
    acc0 = vaddq_s32(vaddq_s32(acc0, acc1), vaddq_s32(acc2, acc3));
    int32_t dot_i32 = vaddvq_s32(acc0);

    return (float)dot_i32 * z_scale * x_scale;
}
#endif /* FPQ_INT8_SDOT */

#elif defined(__SSE2__) || defined(_M_X64) || defined(_M_AMD64)
#include <emmintrin.h>
#if defined(__AVX512F__)
#include <immintrin.h>   /* AVX-512 — 16 floats/register */
#elif defined(__AVX2__)
#include <immintrin.h>   /* AVX2   —  8 floats/register */
#endif

/* x86 SSE2/AVX2 fused spectral bypass kernel */

/* Unnormalized WHT on 256 floats for SSE2 — called once per block at prepare time. */
static void fwht_256_raw_sse2(float * __restrict__ x) {
    /* Pass 0: stride=1 */
    for (size_t i = 0; i < 256; i += 4) {
        __m128 v = _mm_loadu_ps(x + i);
        __m128 even = _mm_shuffle_ps(v, v, _MM_SHUFFLE(2, 2, 0, 0));
        __m128 odd  = _mm_shuffle_ps(v, v, _MM_SHUFFLE(3, 3, 1, 1));
        _mm_storeu_ps(x + i, _mm_unpacklo_ps(_mm_add_ps(even, odd), _mm_sub_ps(even, odd)));
    }
    /* Pass 1: stride=2 */
    for (size_t i = 0; i < 256; i += 4) {
        __m128 v = _mm_loadu_ps(x + i);
        __m128 lo = _mm_movelh_ps(v, v);
        __m128 hi = _mm_movehl_ps(v, v);
        _mm_storeu_ps(x + i, _mm_shuffle_ps(_mm_add_ps(lo, hi), _mm_sub_ps(lo, hi), _MM_SHUFFLE(1, 0, 1, 0)));
    }
    /* Passes 2–7: stride=4..128 */
    for (size_t stride = 4; stride < 256; stride <<= 1) {
        for (size_t base = 0; base < 256; base += stride * 2) {
            for (size_t k = 0; k < stride; k += 4) {
                __m128 a = _mm_loadu_ps(x + base + k);
                __m128 b = _mm_loadu_ps(x + base + k + stride);
                _mm_storeu_ps(x + base + k,          _mm_add_ps(a, b));
                _mm_storeu_ps(x + base + k + stride, _mm_sub_ps(a, b));
            }
        }
    }
}
#define FWHT_Z_BLOCK_INPLACE fwht_256_raw_sse2

static inline float sli_fast_block_score(
    const float *z_b, const float *x_src, size_t dim, uint64_t block_seed)
{
    float __attribute__((aligned(16))) x[256];
    if (dim >= 256) {
        memcpy(x, x_src, 256 * sizeof(float));
    } else {
        memcpy(x, x_src, dim * sizeof(float));
        memset(x + dim, 0, (256 - dim) * sizeof(float));
    }

    /* ── XOR sign-bit flip (SSE2) ── */
    uint64_t state = block_seed ? block_seed : 0x5DEECE66DUL;
    for (size_t i = 0; i < 256; i += 64) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        for (size_t k = i; k < i + 64; k += 4) {
            size_t bit_off = k - i;
            __m128i mask = _mm_setr_epi32(
                (int)(((uint32_t)((state >> (bit_off    )) & 1)) << 31),
                (int)(((uint32_t)((state >> (bit_off + 1)) & 1)) << 31),
                (int)(((uint32_t)((state >> (bit_off + 2)) & 1)) << 31),
                (int)(((uint32_t)((state >> (bit_off + 3)) & 1)) << 31)
            );
            __m128i xv = _mm_loadu_si128((const __m128i *)(x + k));
            _mm_storeu_si128((__m128i *)(x + k), _mm_xor_si128(xv, mask));
        }
    }

    /* FWHT removed: z_b now stores FWHT(z'), computed once at prepare time.
     * score = FWHT(z')^T · (signs ⊙ x)  ==  z'^T · FWHT(signs ⊙ x)  ✓ */

    /* ── Dot product: FWHT(z')^T · (signs ⊙ x) ── */
#if defined(__AVX512F__)
    /* Phase 7 AVX-512: 4 × __m512 (16 FP32 each) = 64 elements/iter, 4 iters = 256.
     * ~2× throughput vs AVX2 on the critical dot product path. */
    __m512 av0 = _mm512_setzero_ps();
    __m512 av1 = _mm512_setzero_ps();
    __m512 av2 = _mm512_setzero_ps();
    __m512 av3 = _mm512_setzero_ps();
    for (int i = 0; i < 256; i += 64) {
        av0 = _mm512_fmadd_ps(_mm512_loadu_ps(z_b + i),      _mm512_loadu_ps(x + i),      av0);
        av1 = _mm512_fmadd_ps(_mm512_loadu_ps(z_b + i + 16), _mm512_loadu_ps(x + i + 16), av1);
        av2 = _mm512_fmadd_ps(_mm512_loadu_ps(z_b + i + 32), _mm512_loadu_ps(x + i + 32), av2);
        av3 = _mm512_fmadd_ps(_mm512_loadu_ps(z_b + i + 48), _mm512_loadu_ps(x + i + 48), av3);
    }
    return _mm512_reduce_add_ps(
        _mm512_add_ps(_mm512_add_ps(av0, av1), _mm512_add_ps(av2, av3)));
#elif defined(__AVX2__)
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    __m256 acc2 = _mm256_setzero_ps();
    __m256 acc3 = _mm256_setzero_ps();
    for (int i = 0; i < 256; i += 32) {
        acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(z_b + i),      _mm256_loadu_ps(x + i),      acc0);
        acc1 = _mm256_fmadd_ps(_mm256_loadu_ps(z_b + i + 8),  _mm256_loadu_ps(x + i + 8),  acc1);
        acc2 = _mm256_fmadd_ps(_mm256_loadu_ps(z_b + i + 16), _mm256_loadu_ps(x + i + 16), acc2);
        acc3 = _mm256_fmadd_ps(_mm256_loadu_ps(z_b + i + 24), _mm256_loadu_ps(x + i + 24), acc3);
    }
    acc0 = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    __m128 lo128 = _mm256_castps256_ps128(acc0);
    __m128 hi128 = _mm256_extractf128_ps(acc0, 1);
    __m128 sf = _mm_add_ps(lo128, hi128);
    __m128 sf2 = _mm_movehl_ps(sf, sf);
    sf = _mm_add_ps(sf, sf2);
    __m128 sf3 = _mm_shuffle_ps(sf, sf, _MM_SHUFFLE(1, 1, 1, 1));
    sf = _mm_add_ss(sf, sf3);
    return _mm_cvtss_f32(sf);
#else
    __m128 acc0 = _mm_setzero_ps();
    __m128 acc1 = _mm_setzero_ps();
    __m128 acc2 = _mm_setzero_ps();
    __m128 acc3 = _mm_setzero_ps();
    for (int i = 0; i < 256; i += 16) {
        acc0 = _mm_add_ps(acc0, _mm_mul_ps(_mm_loadu_ps(z_b+i),    _mm_loadu_ps(x+i)));
        acc1 = _mm_add_ps(acc1, _mm_mul_ps(_mm_loadu_ps(z_b+i+4),  _mm_loadu_ps(x+i+4)));
        acc2 = _mm_add_ps(acc2, _mm_mul_ps(_mm_loadu_ps(z_b+i+8),  _mm_loadu_ps(x+i+8)));
        acc3 = _mm_add_ps(acc3, _mm_mul_ps(_mm_loadu_ps(z_b+i+12), _mm_loadu_ps(x+i+12)));
    }
    acc0 = _mm_add_ps(_mm_add_ps(acc0, acc1), _mm_add_ps(acc2, acc3));
    __m128 sf2 = _mm_movehl_ps(acc0, acc0);
    acc0 = _mm_add_ps(acc0, sf2);
    __m128 sf3 = _mm_shuffle_ps(acc0, acc0, _MM_SHUFFLE(1, 1, 1, 1));
    acc0 = _mm_add_ss(acc0, sf3);
    return _mm_cvtss_f32(acc0);
#endif
}

#else
/* Pure scalar fallback — works everywhere (RISC-V, WASM, etc.) */

/* Unnormalized scalar WHT on 256 floats — called once per block at prepare time. */
static void fwht_256_raw_scalar(float * __restrict__ x) {
    for (size_t stride = 1; stride < 256; stride <<= 1) {
        for (size_t base = 0; base < 256; base += stride * 2) {
            for (size_t k = 0; k < stride; k++) {
                float a = x[base + k];
                float b = x[base + k + stride];
                x[base + k]          = a + b;
                x[base + k + stride] = a - b;
            }
        }
    }
}
#define FWHT_Z_BLOCK_INPLACE fwht_256_raw_scalar

static inline float sli_fast_block_score(
    const float *z_b, const float *x_src, size_t dim, uint64_t block_seed)
{
    float x[256] = {0};
    if (dim > 256) dim = 256;
    memcpy(x, x_src, dim * sizeof(float));

    /* Random signs */
    uint64_t state = block_seed ? block_seed : 0x5DEECE66DUL;
    for (size_t i = 0; i < 256; i += 64) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        for (size_t j = i; j < i + 64 && j < 256; j++)
            if ((state >> (j - i)) & 1) x[j] = -x[j];
    }

    /* FWHT removed: z_b now stores FWHT(z'), computed once at prepare time.
     * score = FWHT(z')^T · (signs ⊙ x)  ==  z'^T · FWHT(signs ⊙ x)  ✓ */

    /* Dot product */
    float dot = 0.0f;
    for (size_t i = 0; i < 256; i++) dot += z_b[i] * x[i];
    return dot;
}
#endif


/* Forward declarations for JIT and shared-spectral functions defined later. */
#ifdef FPQ_JIT_SLI
#include <sys/stat.h>
#include <dlfcn.h>
static void fpqx_sli_jit_compile(fpqx_sli_ctx_t *ctx, uint64_t haar_seed);
#endif

typedef struct {
    const fpq_tensor_t *enc;
    float *z_region;
    size_t begin_block;
    size_t end_block;
    size_t v8_base;
    float beta;
    float lattice_scale;
    float inv_sqrt_n;
    int effective_k;
    size_t tile_idx_off;
    const float *tile_data;
    size_t blocks_per_row;
    int probe_only_rows;
    int status;
} fpqx_sli_prepare_task_t;

typedef struct {
    const fpqx_sli_ctx_t *ctx;
    const fpq_tensor_t *enc;
    const float *x_eff;
    float *output;
    size_t rows_per_task;
    size_t rows;
} fpqx_sli_score_dispatch_t;

static void fpqx_sli_prepare_block_range(fpqx_sli_prepare_task_t *task) {
    const fpq_tensor_t *enc = task->enc;
    for (size_t b = task->begin_block; b < task->end_block; b++) {
        if (task->probe_only_rows && task->blocks_per_row > 0) {
            size_t row = b / task->blocks_per_row;
            if (!fpqx_sli_score_probe_row_match(row)) continue;
        }
        float rms = enc->coord_scales[b];
        float wnorm = enc->sbb_scale_delta[task->v8_base + b];
        float ls_inv = 1.0f / task->lattice_scale;
        float *z_b = task->z_region + b * SLI_BLOCK_DIM;

        for (int p = 0; p < SLI_E8_PAIRS; p++) {
            int ti = (int)enc->sbb_scale_delta[task->tile_idx_off +
                                               b * SLI_E8_PAIRS + (size_t)p];
            if (ti < 0) ti = 0;
            if (ti >= task->effective_k) ti = task->effective_k - 1;
            size_t pair_base = (size_t)p * SLI_TILE_DIM;

            for (int d = 0; d < SLI_TILE_DIM; d++) {
                size_t k = pair_base + (size_t)d;
                float corrected = z_b[k] + task->tile_data[ti * SLI_TILE_DIM + (size_t)d];
                float lat_val = corrected * ls_inv * wnorm;
                float unwarped = sli_unwarp(lat_val, task->beta);
                z_b[k] = unwarped * rms;
            }
        }

        if (enc->qjl && enc->qjl[b] &&
            enc->coord_residual_norms &&
            enc->coord_residual_norms[b] > 1e-10f) {
            fpq_qjl_t *qjl = enc->qjl[b];
            float rnorm = enc->coord_residual_norms[b];
            size_t m = qjl->n_projections;
            float qjl_scale = rnorm / (float)m;
            float proj_scale = 1.0f / sqrtf((float)SLI_BLOCK_DIM);

            for (size_t proj = 0; proj < m; proj++) {
                float y_p = (qjl->bits[proj / 64] & (1ULL << (proj % 64)))
                            ? 1.0f : -1.0f;
                float coeff = y_p * qjl_scale;
                uint64_t state = qjl->proj_seed ^
                                 (proj * 0x9E3779B97F4A7C15ULL);
                for (size_t k = 0; k < SLI_BLOCK_DIM; k += 64) {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    uint64_t bits = state;
                    size_t end = (k + 64 < SLI_BLOCK_DIM) ? k + 64 : SLI_BLOCK_DIM;
                    for (size_t j = k; j < end; j++) {
                        float phi = ((bits >> (j - k)) & 1) ? proj_scale : -proj_scale;
                        z_b[j] += coeff * phi;
                    }
                }
            }
        }

        for (size_t i = 0; i < SLI_BLOCK_DIM; i++) z_b[i] *= task->inv_sqrt_n;
        FWHT_Z_BLOCK_INPLACE(z_b);
    }
}

static void *fpqx_sli_prepare_worker(void *arg) {
    fpqx_sli_prepare_task_t *task = (fpqx_sli_prepare_task_t *)arg;
    if (!task) return NULL;
    task->status = 0;
    fpqx_sli_prepare_block_range(task);
    return NULL;
}

static int fpqx_sli_prepare_thread_count(size_t n_blocks) {
    const char *env = getenv("BONFYRE_QWEN_PREPARE_THREADS");
    long threads = 0;
    if (env && *env) {
        char *end = NULL;
        threads = strtol(env, &end, 10);
        if (end == env || threads < 1) threads = 1;
    } else {
#ifdef _SC_NPROCESSORS_ONLN
        threads = sysconf(_SC_NPROCESSORS_ONLN);
#elif defined(_SC_NPROCESSORS_CONF)
        threads = sysconf(_SC_NPROCESSORS_CONF);
#else
        threads = 1;
#endif
        if (threads < 1) threads = 1;
    }
    if (n_blocks < 64) return 1;
    if ((size_t)threads > n_blocks / 32u) threads = (long)(n_blocks / 32u);
    if (threads < 1) threads = 1;
    if (threads > 16) threads = 16;
    return (int)threads;
}

static int fpqx_sli_matvec_thread_count(size_t rows) {
    const char *env = getenv("BONFYRE_QWEN_MATVEC_THREADS");
    long threads = 0;
    if (env && *env) {
        char *end = NULL;
        threads = strtol(env, &end, 10);
        if (end == env || threads < 1) threads = 1;
    } else {
#ifdef _SC_NPROCESSORS_ONLN
        threads = sysconf(_SC_NPROCESSORS_ONLN);
#elif defined(_SC_NPROCESSORS_CONF)
        threads = sysconf(_SC_NPROCESSORS_CONF);
#else
        threads = 1;
#endif
        if (threads < 1) threads = 1;
    }
    if (rows < 1024) return 1;
    if ((size_t)threads > rows / 512u) threads = (long)(rows / 512u);
    if (threads < 1) threads = 1;
    if (threads > 8) threads = 8;
    return (int)threads;
}

static void fpqx_sli_score_rows(const fpqx_sli_ctx_t *ctx,
                                const fpq_tensor_t *enc,
                                const float *x_eff,
                                float *output,
                                size_t row_begin,
                                size_t row_end) {
    size_t rows = ctx->rows;
    size_t cols = ctx->cols;
    size_t bpr = ctx->blocks_per_row;
    size_t n_blocks = enc->n_blocks;
    if (row_end > rows) row_end = rows;
    for (size_t i = row_begin; i < row_end; i++) {
        float row_score = 0.0f;
        float probe_row_fast = 0.0f;
        float probe_row_slow = 0.0f;
        int probe_active = fpqx_sli_score_probe_match(ctx, i);
        if (fpqx_sli_score_probe_only_rows_enabled() && !probe_active) continue;
        for (size_t bj = 0; bj < bpr; bj++) {
            size_t b = i * bpr + bj;
            if (b >= n_blocks) break;

            size_t col_offset = bj * SLI_BLOCK_DIM;
            size_t col_end = col_offset + SLI_BLOCK_DIM;
            if (col_end > cols) col_end = cols;
            size_t this_dim = col_end - col_offset;

            uint64_t block_seed = enc->haar_seed ^ (uint64_t)b;
            float fast_block_score = 0.0f;
            if (ctx->z_data_f16) {
                fast_block_score = sli_fast_block_score_f16(
                    ctx->z_data_f16 + b * SLI_BLOCK_DIM,
                    x_eff + col_offset, this_dim, block_seed);
            } else
#ifdef FPQ_INT8_SDOT
            if (ctx->z_data_i8 && ctx->z_data_scale) {
                fast_block_score = sli_fast_block_score_i8(
                    ctx->z_data_i8 + b * SLI_BLOCK_DIM,
                    ctx->z_data_scale[b],
                    x_eff + col_offset, this_dim, block_seed);
            } else
#endif
            {
                const float *z_b = ctx->z_data + b * SLI_BLOCK_DIM;
                fast_block_score = sli_fast_block_score(z_b, x_eff + col_offset,
                                                        this_dim, block_seed);
            }
            row_score += fast_block_score;
            if (probe_active) {
                float slow_block_score = fpqx_sli_score_block_slow(
                    ctx, enc, x_eff, b, col_offset, this_dim);
                probe_row_fast += fast_block_score;
                probe_row_slow += slow_block_score;
                fpqx_sli_score_probe_emit(
                    "sli_score_probe_block", ctx, enc, i, b, col_offset, this_dim,
                    fast_block_score, slow_block_score, 0);
            }
        }
        output[i] += row_score;
        if (probe_active) {
            fpqx_sli_score_probe_emit(
                "sli_score_probe_row", ctx, enc, i, SIZE_MAX, 0, cols,
                probe_row_fast, probe_row_slow, 0);
        }
    }
}

#ifdef __APPLE__
static void fpqx_sli_score_rows_dispatch(void *context, size_t task_idx) {
    const fpqx_sli_score_dispatch_t *d = (const fpqx_sli_score_dispatch_t *)context;
    size_t row_begin = task_idx * d->rows_per_task;
    size_t row_end = row_begin + d->rows_per_task;
    fpqx_sli_score_rows(d->ctx, d->enc, d->x_eff, d->output, row_begin, row_end);
}
#endif


fpqx_sli_ctx_t *fpqx_sli_prepare(const fpqx_tensor_t *t) {
    if (!t || !t->additive) return NULL;

    fpq_tensor_t *enc = t->additive;
    size_t rows = t->rows;
    size_t cols = t->cols;
    size_t n_blocks = enc->n_blocks;

    fpqx_sli_ctx_t *ctx = (fpqx_sli_ctx_t *)calloc(1, sizeof(*ctx));
    ctx->rows = rows;
    ctx->cols = cols;
    ctx->blocks_per_row = (cols + SLI_BLOCK_DIM - 1) / SLI_BLOCK_DIM;
    ctx->n_block_cols = ctx->blocks_per_row;
    ctx->tensor = t;
    ctx->n_total_blocks = n_blocks;

    /* Allocate per-block-column score tables */
    ctx->tables = (fpqx_sli_table_t *)calloc(ctx->n_block_cols,
                                               sizeof(fpqx_sli_table_t));
    for (size_t bj = 0; bj < ctx->n_block_cols; bj++) {
        fpqx_sli_table_t *tab = &ctx->tables[bj];
        tab->block_dim = SLI_BLOCK_DIM;
        tab->x_spectral = (float *)calloc(SLI_BLOCK_DIM, sizeof(float));
        tab->proj_scores = (float *)calloc(SLI_QJL_PROJECTIONS, sizeof(float));
    }

    ctx->output = (float *)calloc(rows, sizeof(float));

    /* ── Parse sbb_scale_delta layout and cache offsets ── */
    int is_v9 = (enc->pid_alpha == -9.0f);
    ctx->lr_rank = is_v9 ? (int)enc->sbb_scale_delta[0] : 0;
    ctx->us_off = is_v9 ? 2 : 0;
    size_t lr_us_size = rows * (size_t)ctx->lr_rank;
    ctx->vt_off = ctx->us_off + lr_us_size;
    size_t lr_vt_size = (size_t)ctx->lr_rank * cols;
    ctx->v8_base = is_v9 ? (2 + lr_us_size + lr_vt_size) : 0;
    ctx->e8_off = ctx->v8_base + n_blocks;

    /* ── Precompute z vectors in-place over the E8 region ──
     *
     * z[b][k] = unwarp((e8[k] + tile[k]) / lattice_scale * wnorm) * rms
     *         + QJL reconstruction
     *
     * After this, sbb_scale_delta[e8_off + b*256 + k] holds the final
     * z value instead of the raw E8 coordinate.  E8/tile/QJL data are
     * no longer needed for inference — only LR factors (Phase 0) and
     * haar_seed (for spectral bypass signs) are still used.
     */
    float beta = SLI_MU_BETA;
    int coord_bits = (int)enc->coord_bits;
    float lattice_scale = 8.0f * (float)coord_bits;

    size_t e8_flat_size = n_blocks * SLI_BLOCK_DIM;
    size_t tile_cb_off = ctx->e8_off + e8_flat_size;

    /* Recover effective_k */
    int effective_k = SLI_RVQ_TILES;
    {
        size_t test_cb = (size_t)effective_k * SLI_TILE_DIM;
        size_t test_ek = tile_cb_off + test_cb + n_blocks * SLI_E8_PAIRS;
        float stored = enc->sbb_scale_delta[test_ek];
        if (stored >= 16.0f && stored <= 256.0f)
            effective_k = (int)stored;
    }
    if (effective_k == SLI_RVQ_TILES) {
        for (int try_k = 16; try_k <= 256; try_k *= 2) {
            size_t off = tile_cb_off + (size_t)try_k * SLI_TILE_DIM +
                         n_blocks * SLI_E8_PAIRS;
            float stored = enc->sbb_scale_delta[off];
            if ((int)stored == try_k) { effective_k = try_k; break; }
        }
    }

    size_t tile_cb_size = (size_t)effective_k * SLI_TILE_DIM;
    size_t tile_idx_off = tile_cb_off + tile_cb_size;
    const float *tile_data = enc->sbb_scale_delta + tile_cb_off;

    /* Writable pointer to the E8 region. In probe-only mode, preserve the
     * encoded tensor and build z into an owned scratch buffer so the slow
     * scorer can still read the original raw E8/tile/QJL data. */
    float *z_region = enc->sbb_scale_delta + ctx->e8_off;
    int probe_only_rows = fpqx_sli_prepare_probe_only_rows(t);
    if (probe_only_rows) {
        size_t z_bytes = n_blocks * SLI_BLOCK_DIM * sizeof(float);
        z_region = (float *)calloc(n_blocks * SLI_BLOCK_DIM, sizeof(float));
        if (!z_region) {
            fpqx_sli_free(ctx);
            return NULL;
        }
        ctx->z_data_mmap_bytes = z_bytes;
        ctx->z_data_is_mmap = 0;
        for (size_t b = 0; b < n_blocks; b++) {
            size_t row = ctx->blocks_per_row ? (b / ctx->blocks_per_row) : 0;
            if (!fpqx_sli_score_probe_row_match(row)) continue;
            memcpy(z_region + b * SLI_BLOCK_DIM,
                   enc->sbb_scale_delta + ctx->e8_off + b * SLI_BLOCK_DIM,
                   SLI_BLOCK_DIM * sizeof(float));
        }
    }

    ctx->z_data = z_region;
    ctx->z_precomputed = 1;

    /* Fold 1/√n normalization into z so FWHT can run unnormalized.
     * score = z^T · (1/√n) · FWHT(s⊙x) = (z/√n)^T · FWHT_raw(s⊙x)
     */
    float inv_sqrt_n = 1.0f / sqrtf((float)SLI_BLOCK_DIM);  /* 1/16 */
    {
        int n_threads = fpqx_sli_prepare_thread_count(n_blocks);
        if (n_threads <= 1) {
            fpqx_sli_prepare_task_t task = {
                .enc = enc,
                .z_region = z_region,
                .begin_block = 0,
                .end_block = n_blocks,
                .v8_base = ctx->v8_base,
                .beta = beta,
                .lattice_scale = lattice_scale,
                .inv_sqrt_n = inv_sqrt_n,
                .effective_k = effective_k,
                .tile_idx_off = tile_idx_off,
                .tile_data = tile_data,
                .blocks_per_row = ctx->blocks_per_row,
                .probe_only_rows = probe_only_rows,
                .status = 0,
            };
            fpqx_sli_prepare_block_range(&task);
        } else {
            pthread_t *threads = (pthread_t *)calloc((size_t)n_threads, sizeof(*threads));
            fpqx_sli_prepare_task_t *tasks =
                (fpqx_sli_prepare_task_t *)calloc((size_t)n_threads, sizeof(*tasks));
            int *started = (int *)calloc((size_t)n_threads, sizeof(*started));
            if (!threads || !tasks) {
                free(threads);
                free(tasks);
                free(started);
                fpqx_sli_prepare_task_t task = {
                    .enc = enc,
                    .z_region = z_region,
                    .begin_block = 0,
                    .end_block = n_blocks,
                    .v8_base = ctx->v8_base,
                    .beta = beta,
                    .lattice_scale = lattice_scale,
                    .inv_sqrt_n = inv_sqrt_n,
                    .effective_k = effective_k,
                    .tile_idx_off = tile_idx_off,
                    .tile_data = tile_data,
                    .status = 0,
                };
                fpqx_sli_prepare_block_range(&task);
            } else {
                size_t chunk = (n_blocks + (size_t)n_threads - 1u) / (size_t)n_threads;
                for (int i = 0; i < n_threads; i++) {
                    size_t begin = (size_t)i * chunk;
                    size_t end = begin + chunk;
                    if (begin >= n_blocks) break;
                    if (end > n_blocks) end = n_blocks;
                    tasks[i].enc = enc;
                    tasks[i].z_region = z_region;
                    tasks[i].begin_block = begin;
                    tasks[i].end_block = end;
                    tasks[i].v8_base = ctx->v8_base;
                    tasks[i].beta = beta;
                    tasks[i].lattice_scale = lattice_scale;
                    tasks[i].inv_sqrt_n = inv_sqrt_n;
                    tasks[i].effective_k = effective_k;
                    tasks[i].tile_idx_off = tile_idx_off;
                    tasks[i].tile_data = tile_data;
                    tasks[i].blocks_per_row = ctx->blocks_per_row;
                    tasks[i].probe_only_rows = probe_only_rows;
                    tasks[i].status = 0;
                    if (pthread_create(&threads[i], fpqx_small_pthread_attr(), fpqx_sli_prepare_worker, &tasks[i]) != 0) {
                        tasks[i].status = -1;
                        fpqx_sli_prepare_block_range(&tasks[i]);
                    } else {
                        started[i] = 1;
                    }
                }
                for (int i = 0; i < n_threads; i++) {
                    if (started[i]) pthread_join(threads[i], NULL);
                }
                free(threads);
                free(tasks);
                free(started);
            }
        }
    }

    ctx->z_fwht_preapplied = 1;

    if (fpqx_sli_prepare_f16_only_enabled()) {
        ctx->z_data_f16 = (__fp16 *)malloc(n_blocks * SLI_BLOCK_DIM * sizeof(__fp16));
        if (ctx->z_data_f16) {
            for (size_t b = 0; b < n_blocks; b++) {
                const float *zb = z_region + b * SLI_BLOCK_DIM;
                __fp16 *hb = ctx->z_data_f16 + b * SLI_BLOCK_DIM;
                for (size_t i = 0; i < SLI_BLOCK_DIM; i++) hb[i] = (__fp16)zb[i];
            }
            if (!probe_only_rows &&
                ctx->z_data &&
                ctx->z_data_mmap_bytes > 0 &&
                !ctx->z_data_is_mmap) {
                free(ctx->z_data);
                ctx->z_data = NULL;
                ctx->z_data_mmap_bytes = 0;
            }
        }
    }

#ifdef FPQ_INT8_SDOT
    if (!fpqx_sli_prepare_f16_only_enabled()) {
        /* Quantize each z block to INT8 once, now that FWHT is folded in.
         * z_data_i8[b][i] = round(z_region[b×256+i] / scale_b) clamped to [-127,127]
         * z_data_scale[b] = abs_max / 127.0  (per-block scale factor)
         */
        ctx->z_data_i8    = (int8_t *)malloc(n_blocks * SLI_BLOCK_DIM * sizeof(int8_t));
        ctx->z_data_scale = (float  *)malloc(n_blocks * sizeof(float));
        if (ctx->z_data_i8 && ctx->z_data_scale) {
            for (size_t b = 0; b < n_blocks; b++) {
                if (probe_only_rows && ctx->blocks_per_row > 0) {
                    size_t row = b / ctx->blocks_per_row;
                    if (!fpqx_sli_score_probe_row_match(row)) {
                        memset(ctx->z_data_i8 + b * SLI_BLOCK_DIM, 0, SLI_BLOCK_DIM * sizeof(int8_t));
                        ctx->z_data_scale[b] = 1.0f;
                        continue;
                    }
                }
                const float *zb = z_region + b * SLI_BLOCK_DIM;
                int8_t      *qb = ctx->z_data_i8 + b * SLI_BLOCK_DIM;

                float abs_max = 0.0f;
                for (size_t i = 0; i < SLI_BLOCK_DIM; i++) {
                    float v = zb[i] < 0.0f ? -zb[i] : zb[i];
                    if (v > abs_max) abs_max = v;
                }
                if (abs_max < 1e-20f) {
                    memset(qb, 0, SLI_BLOCK_DIM * sizeof(int8_t));
                    ctx->z_data_scale[b] = 1.0f;
                    continue;
                }
                float scale = abs_max / 127.0f;
                ctx->z_data_scale[b] = scale;
                float inv_scale = 1.0f / scale;
                for (size_t i = 0; i < SLI_BLOCK_DIM; i++) {
                    float q = zb[i] * inv_scale;
                    int32_t qi = (int32_t)(q >= 0.0f ? q + 0.5f : q - 0.5f);
                    if (qi >  127) qi =  127;
                    if (qi < -127) qi = -127;
                    qb[i] = (int8_t)qi;
                }
            }
        } else {
            free(ctx->z_data_i8);
            free(ctx->z_data_scale);
            ctx->z_data_i8    = NULL;
            ctx->z_data_scale = NULL;
        }
        if (!probe_only_rows &&
            fpqx_sli_prepare_int8_only_enabled() &&
            ctx->z_data_i8 &&
            ctx->z_data_scale &&
            ctx->z_data &&
            ctx->z_data_mmap_bytes > 0 &&
            !ctx->z_data_is_mmap) {
            free(ctx->z_data);
            ctx->z_data = NULL;
            ctx->z_data_mmap_bytes = 0;
        }
    }
#endif /* FPQ_INT8_SDOT */

    /* ── Innovation 2: M-operator approximate column fold ──────────────
     * If the tensor has a scale manifold (M operator: S = 1 + A·B^T),
     * precompute a per-column fold factor:
     *   col_fold[j] = 1 + dot(A_mean, B[j,:])
     * where A_mean[r] = (1/rows) * sum_i A[i,r].
     *
     * At inference time (fpqx_sli_matvec), x is pre-multiplied element-wise
     * by col_fold before the SLI scoring loop.  This captures ~90%+ of M's
     * contribution at zero weight-decode cost (just a single O(cols) pass
     * on x).  The error is the rank-correction term:
     *   Σ_r (A[i,r] - A_mean[r]) * <W[i,:], B[:,r] ⊙ x>
     * which is negligible for low-rank M with small A variance.
     */
    if (t->scale != NULL) {
        const fpqx_scale_manifold_t *sm = t->scale;
        int sr = sm->scale_rank;
        if (sr > 0 && sm->A != NULL && sm->B != NULL) {
            ctx->m_fold_col = (float *)malloc(cols * sizeof(float));
            if (ctx->m_fold_col) {
                /* A_mean[r] = (1/rows) * sum_i A[i * sr + r] */
                float *a_mean = (float *)calloc((size_t)sr, sizeof(float));
                if (a_mean) {
                    for (size_t i = 0; i < rows; i++)
                        for (int r = 0; r < sr; r++)
                            a_mean[r] += sm->A[i * (size_t)sr + (size_t)r];
                    float inv_rows = 1.0f / (float)rows;
                    for (int r = 0; r < sr; r++) a_mean[r] *= inv_rows;

                    /* col_fold[j] = 1 + dot(a_mean, B[j * sr .. j*sr+sr]) */
                    for (size_t j = 0; j < cols; j++) {
                        float dot = 0.0f;
                        const float *bj = sm->B + j * (size_t)sr;
                        for (int r = 0; r < sr; r++) dot += a_mean[r] * bj[r];
                        ctx->m_fold_col[j] = 1.0f + dot;
                    }
                    free(a_mean);
                } else {
                    /* OOM — disable fold */
                    free(ctx->m_fold_col);
                    ctx->m_fold_col = NULL;
                }
            }
        }
    }

#ifdef FPQ_JIT_SLI
    /* ── Innovation 3: JIT kernel compilation ──────────────────────────
     * Emit → compile → dlopen a kernel with N_ROWS and BPR baked as
     * integer literals.  Fails silently: if cc is unavailable the regular
     * fast path is used.
     */
    if (ctx->z_data != NULL) {
        fpqx_sli_jit_compile(ctx, enc->haar_seed);
    }
#endif

    return ctx;
}


int fpqx_sli_matvec(fpqx_sli_ctx_t *ctx, const float *x, float *output) {
    if (!ctx || !ctx->tensor || !ctx->tensor->additive) return -1;

    const fpqx_tensor_t *t = ctx->tensor;
    const fpq_tensor_t *enc = t->additive;
    size_t rows = ctx->rows;
    size_t cols = ctx->cols;
    size_t bpr = ctx->blocks_per_row;
    size_t n_blocks = enc->n_blocks;

    /* ── Phase 0: Low-rank contribution y_LR = UΣ · (V^T · x) ── */
    int lr_rank = ctx->lr_rank;
    size_t us_off = ctx->us_off;
    size_t vt_off = ctx->vt_off;
    int probe_only_rows = fpqx_sli_score_probe_only_rows_enabled() &&
                          fpqx_sli_score_probe_tensor_match(ctx);

#if USE_ACCELERATE
    if (lr_rank > 0) {
        float *vt_x = (float *)calloc(lr_rank, sizeof(float));
        /* V^T · x: [lr_rank × cols] → [lr_rank] */
        cblas_sgemv(CblasRowMajor, CblasNoTrans,
                    lr_rank, (int)cols, 1.0f,
                    enc->sbb_scale_delta + vt_off, (int)cols,
                    x, 1, 0.0f, vt_x, 1);
        if (probe_only_rows) {
            memset(output, 0, rows * sizeof(float));
            for (size_t i = 0; i < rows; i++) {
                if (!fpqx_sli_score_probe_row_match(i)) continue;
                float lr_val = 0.0f;
                for (int r = 0; r < lr_rank; r++)
                    lr_val += enc->sbb_scale_delta[us_off + i * (size_t)lr_rank + (size_t)r] * vt_x[r];
                output[i] = lr_val;
            }
        } else {
            /* UΣ · vt_x: [rows × lr_rank] → [rows] */
            cblas_sgemv(CblasRowMajor, CblasNoTrans,
                        (int)rows, lr_rank, 1.0f,
                        enc->sbb_scale_delta + us_off, lr_rank,
                        vt_x, 1, 0.0f, output, 1);
        }
        free(vt_x);
    } else {
        memset(output, 0, rows * sizeof(float));
    }
#else
    {
        float *vt_x = (float *)calloc(lr_rank, sizeof(float));
        for (int r = 0; r < lr_rank; r++) {
            float dot = 0.0f;
            for (size_t j = 0; j < cols; j++)
                dot += enc->sbb_scale_delta[vt_off + r * cols + j] * x[j];
            vt_x[r] = dot;
        }
        memset(output, 0, rows * sizeof(float));
        for (size_t i = 0; i < rows; i++) {
            if (probe_only_rows && !fpqx_sli_score_probe_row_match(i)) continue;
            float lr_val = 0.0f;
            for (int r = 0; r < lr_rank; r++)
                lr_val += enc->sbb_scale_delta[us_off + i * lr_rank + r] * vt_x[r];
            output[i] = lr_val;
        }
        free(vt_x);
    }
#endif

    /* ── Phase 0.5: M-operator column fold (Innovation 2) ─────────────
     * If m_fold_col was computed at prepare time, pre-multiply x element-wise
     * by the per-column fold factor.  This folds the approximate low-rank
     * scale manifold into x before SLI scoring, adding its contribution at
     * O(cols) cost vs. fully reconstructing the weight matrix.
     * x_eff points to x (no fold) or a heap-allocated x_folded (with fold).
     */
    const float *x_eff = x;
    float *x_mfold = NULL;
    if (ctx->m_fold_col != NULL) {
        x_mfold = (float *)malloc(cols * sizeof(float));
        if (x_mfold) {
            for (size_t j = 0; j < cols; j++)
                x_mfold[j] = x[j] * ctx->m_fold_col[j];
            x_eff = x_mfold;
        }
        /* If malloc fails, fall back to unfolded x silently */
    }

    /* ── Phase 1+2: Precomputed z fast path ──
     *
     * z vectors were precomputed during prepare: z[b] = full decoded block.
     * Inference is just: score_b = z[b]^T · FWHT(signs_b ⊙ x_block)
     *
     * Per block: random signs (256 muls) + FWHT (O(N log N)) + dot (256 FMAs)
     * No E8 scoring, no tile lookup, no QJL projection, no unwarp.
     */
    if (fpqx_sli_has_precomputed_fast_data(ctx)) {
#ifdef FPQ_JIT_SLI
        /* Innovation 3: use JIT-compiled kernel if available.
         * The JIT kernel has N_ROWS and BPR baked as literals, allowing the
         * compiler to fully unroll outer loops and emit optimal SIMD. */
        if (ctx->jit_fn != NULL && ctx->z_data != NULL) {
            ctx->jit_fn(ctx->z_data, x_eff, output,
                        (unsigned long long)enc->haar_seed, (unsigned long)cols);
        } else {
#endif
        {
            int n_threads = fpqx_sli_matvec_thread_count(rows);
#ifdef __APPLE__
            if (n_threads > 1) {
                fpqx_sli_score_dispatch_t d = {
                    .ctx = ctx,
                    .enc = enc,
                    .x_eff = x_eff,
                    .output = output,
                    .rows_per_task = (rows + (size_t)n_threads - 1u) / (size_t)n_threads,
                    .rows = rows,
                };
                dispatch_apply_f((size_t)n_threads,
                                 dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0),
                                 &d,
                                 fpqx_sli_score_rows_dispatch);
            } else
#endif
            {
                fpqx_sli_score_rows(ctx, enc, x_eff, output, 0, rows);
            }
        }
#ifdef FPQ_JIT_SLI
        } /* end JIT if/else */
#endif

    } else {
        /* Fallback: full E8 + tile + QJL scoring (no precompute) */
        int coord_bits = (int)enc->coord_bits;
        float lattice_scale = 8.0f * (float)coord_bits;
        size_t v8_base = ctx->v8_base;
        size_t e8_off = ctx->e8_off;
        size_t e8_flat_size = n_blocks * SLI_BLOCK_DIM;
        size_t tile_cb_off = e8_off + e8_flat_size;

        int effective_k = SLI_RVQ_TILES;
        {
            size_t test_cb = (size_t)effective_k * SLI_TILE_DIM;
            size_t test_ek = tile_cb_off + test_cb + n_blocks * SLI_E8_PAIRS;
            float stored = enc->sbb_scale_delta[test_ek];
            if (stored >= 16.0f && stored <= 256.0f)
                effective_k = (int)stored;
        }
        size_t tile_cb_size = (size_t)effective_k * SLI_TILE_DIM;
        size_t tile_idx_off = tile_cb_off + tile_cb_size;
        const float *tile_data = enc->sbb_scale_delta + tile_cb_off;

        float *x_block = (float *)malloc(SLI_BLOCK_DIM * sizeof(float));
        float *x_spectral = (float *)malloc(SLI_BLOCK_DIM * sizeof(float));

        for (size_t i = 0; i < rows; i++) {
            float row_sli_score = 0.0f;
            for (size_t bj = 0; bj < bpr; bj++) {
                size_t b = i * bpr + bj;
                if (b >= n_blocks) break;
                size_t col_offset = bj * SLI_BLOCK_DIM;
                size_t col_end = col_offset + SLI_BLOCK_DIM;
                if (col_end > cols) col_end = cols;
                size_t this_dim = col_end - col_offset;
                float rms = enc->coord_scales[b];
                float wnorm = enc->sbb_scale_delta[v8_base + b];
                const float *e8_pts = enc->sbb_scale_delta + e8_off + b * SLI_BLOCK_DIM;

                memset(x_block, 0, SLI_BLOCK_DIM * sizeof(float));
                memcpy(x_block, x_eff + col_offset, this_dim * sizeof(float));
                uint64_t block_seed = enc->haar_seed ^ (uint64_t)b;
                fpq_neon_random_signs_256(x_block, block_seed);
                fpq_neon_fwht_256(x_block);
                memcpy(x_spectral, x_block, SLI_BLOCK_DIM * sizeof(float));

                float tile_indices_f[SLI_E8_PAIRS];
                for (int p = 0; p < SLI_E8_PAIRS; p++)
                    tile_indices_f[p] = enc->sbb_scale_delta[tile_idx_off +
                                                              b * SLI_E8_PAIRS + p];
                float base_tile_score = fpq_neon_score_block(
                    e8_pts, tile_data, tile_indices_f, x_spectral,
                    rms, wnorm, lattice_scale, effective_k);
                float qjl_score = 0.0f;
                if (enc->qjl && enc->qjl[b] &&
                    enc->coord_residual_norms &&
                    enc->coord_residual_norms[b] > 1e-10f) {
                    qjl_score = fpq_neon_qjl_score(
                        x_spectral, enc->qjl[b]->bits[0],
                        enc->qjl[b]->proj_seed,
                        enc->coord_residual_norms[b],
                        enc->qjl[b]->n_projections);
                }
                row_sli_score += base_tile_score + qjl_score;
            }
            output[i] += row_sli_score;
        }
        free(x_block);
        free(x_spectral);
    }

    /* ── Phase 3: Ghost correction ──
     * y_ghost = σ · u · (v^T · x)
     */
    if (enc->ghost && fabsf(enc->ghost->sigma) > 1e-10f) {
        float vt_dot = 0.0f;
        size_t ghost_cols = enc->ghost->cols;
        for (size_t j = 0; j < ghost_cols && j < cols; j++)
            vt_dot += enc->ghost->v[j] * x[j];
        float ghost_scale = enc->ghost->sigma * vt_dot;
        size_t ghost_rows = enc->ghost->rows;
        for (size_t i = 0; i < ghost_rows && i < rows; i++)
            output[i] += ghost_scale * enc->ghost->u[i];
    }

    /* ── Phase 4: M column fold cleanup ────────────────────────────────
     * The M operator's approximate fold was applied in Phase 0.5 above.
     * Free the temporary x_mfold buffer here if it was allocated. */
    free(x_mfold);

    return 0;
}


int fpqx_sli_matvec_oneshot(const fpqx_tensor_t *t,
                              const float *x, float *output) {
    fpqx_sli_ctx_t *ctx = fpqx_sli_prepare(t);
    if (!ctx) return -1;
    int rc = fpqx_sli_matvec(ctx, x, output);
    fpqx_sli_free(ctx);
    return rc;
}


void fpqx_sli_free(fpqx_sli_ctx_t *ctx) {
    if (!ctx) return;
    if (ctx->tables) {
        for (size_t i = 0; i < ctx->n_block_cols; i++) {
            free(ctx->tables[i].x_spectral);
            free(ctx->tables[i].proj_scores);
        }
        free(ctx->tables);
    }
    if (ctx->z_data && ctx->z_data_mmap_bytes > 0 && !ctx->z_data_is_mmap) {
        free(ctx->z_data);
        ctx->z_data = NULL;
    }
    free(ctx->z_data_f16);
    free(ctx->output);
    free(ctx->m_fold_col);   /* Innovation 2: safe even if NULL */
#ifdef FPQ_INT8_SDOT
    free(ctx->z_data_i8);
    free(ctx->z_data_scale);
#endif
#ifdef FPQ_JIT_SLI
    if (ctx->jit_handle) {
        dlclose(ctx->jit_handle);
        ctx->jit_handle = NULL;
    }
#endif
    free(ctx);
}


/* ═══════════════════════════════════════════════════════════════════
 * Innovation 1: Shared Spectral Q/K/V group
 * ═══════════════════════════════════════════════════════════════════ */

fpqx_sli_shared_spectral_t *fpqx_sli_group_qkv(
    fpqx_sli_ctx_t **ctxs, int n)
{
    if (!ctxs || n <= 0) return NULL;
    for (int t = 0; t < n; t++) {
        if (!fpqx_sli_has_precomputed_fast_data(ctxs[t]))
            return NULL;  /* require prepared ctxs */
    }

    fpqx_sli_shared_spectral_t *grp =
        (fpqx_sli_shared_spectral_t *)calloc(1, sizeof(*grp));
    if (!grp) return NULL;

    grp->n_tensors   = n;
    grp->ctxs        = ctxs;          /* NOT owned */
    grp->n_block_cols = ctxs[0]->blocks_per_row;

    grp->haar_seeds = (uint64_t *)malloc((size_t)n * sizeof(uint64_t));
    if (!grp->haar_seeds) { free(grp); return NULL; }

    for (int t = 0; t < n; t++)
        grp->haar_seeds[t] = ctxs[t]->tensor->additive->haar_seed;

    return grp;
}


/*
 * fpqx_sli_matvec_shared — batch matvec with shared x bandwidth.
 *
 * Innovation 1: For each block column, x_b (the 256-float activation
 * sub-vector) is read from main memory exactly once via a local stack
 * copy.  All n tensor kernels then apply their own sign pattern to
 * that stack copy — which remains in L1 cache — and dot with their
 * respective z_data.  Compared to calling fpqx_sli_matvec n times,
 * x is read n× fewer times from memory.
 *
 * Phase 0 (low-rank) and Phase 3 (ghost) are still run per-tensor
 * (they are O(rank) and not the memory bottleneck).
 */
int fpqx_sli_matvec_shared(fpqx_sli_shared_spectral_t *grp,
                             const float *x,
                             float **outputs)
{
    if (!grp || !x || !outputs) return -1;

    int n = grp->n_tensors;

    /* ── Phase 0: Low-rank contribution for each tensor ── */
    for (int t = 0; t < n; t++) {
        fpqx_sli_ctx_t   *ctx = grp->ctxs[t];
        const fpqx_tensor_t *ft  = ctx->tensor;
        const fpq_tensor_t  *enc = ft->additive;
        size_t rows = ctx->rows, cols = ctx->cols;
        int    lr   = ctx->lr_rank;
        size_t us   = ctx->us_off, vt = ctx->vt_off;
        int probe_only_rows = fpqx_sli_score_probe_only_rows_enabled() &&
                              fpqx_sli_score_probe_tensor_match(ctx);

        /* Apply M-fold to x if present — phase 0.5 for this tensor */
        const float *x_eff = x;
        float *x_mfold = NULL;
        if (ctx->m_fold_col) {
            x_mfold = (float *)malloc(cols * sizeof(float));
            if (x_mfold) {
                for (size_t j = 0; j < cols; j++)
                    x_mfold[j] = x[j] * ctx->m_fold_col[j];
                x_eff = x_mfold;
            }
        }

#if USE_ACCELERATE
        if (lr > 0) {
            float *vt_x = (float *)calloc((size_t)lr, sizeof(float));
            cblas_sgemv(CblasRowMajor, CblasNoTrans,
                        lr, (int)cols, 1.0f,
                        enc->sbb_scale_delta + vt, (int)cols,
                        x_eff, 1, 0.0f, vt_x, 1);
            if (probe_only_rows) {
                memset(outputs[t], 0, rows * sizeof(float));
                for (size_t i = 0; i < rows; i++) {
                    if (!fpqx_sli_score_probe_row_match(i)) continue;
                    float v = 0.0f;
                    for (int r = 0; r < lr; r++)
                        v += enc->sbb_scale_delta[us + i * (size_t)lr + (size_t)r] * vt_x[r];
                    outputs[t][i] = v;
                }
            } else {
                cblas_sgemv(CblasRowMajor, CblasNoTrans,
                            (int)rows, lr, 1.0f,
                            enc->sbb_scale_delta + us, lr,
                            vt_x, 1, 0.0f, outputs[t], 1);
            }
            free(vt_x);
        } else {
            memset(outputs[t], 0, rows * sizeof(float));
        }
#else
        {
            float *vt_x = (float *)calloc((size_t)lr, sizeof(float));
            for (int r = 0; r < lr; r++) {
                float dot = 0.0f;
                for (size_t j = 0; j < cols; j++)
                    dot += enc->sbb_scale_delta[vt + (size_t)r * cols + j] * x_eff[j];
                vt_x[r] = dot;
            }
            memset(outputs[t], 0, rows * sizeof(float));
            for (size_t i = 0; i < rows; i++) {
                if (probe_only_rows && !fpqx_sli_score_probe_row_match(i)) continue;
                float v = 0.0f;
                for (int r = 0; r < lr; r++)
                    v += enc->sbb_scale_delta[us + i * (size_t)lr + (size_t)r] * vt_x[r];
                outputs[t][i] = v;
            }
            free(vt_x);
        }
#endif
        /* Store x_mfold pointer for the shared SLI phase.
         * We'll re-use grp->ctxs[t]->m_fold_col in the SLI block below.
         * Free per-tensor x_mfold here — the SLI phase will re-fold if needed. */
        free(x_mfold);
    }

    /* ── Phase 1+2: Shared-x SLI scoring ──
     *
     * Outer loop: rows (same for all tensors since they share input dim).
     * Inner loop: block columns, then tensors.
     *
     * For each block column bj:
     *   1. Copy x[bj*256..bj*256+256] onto local stack → x_b[256]  (ONE read)
     *   2. For each tensor t: call sli_fast_block_score(z_t_b, x_b, ...)
     *      sli_fast_block_score copies x_b internally (still L1-hot) and
     *      applies its own seed-derived signs before the dot.
     */
    {
        fpqx_sli_ctx_t *ctx0 = grp->ctxs[0];
        size_t rows0 = ctx0->rows;
        size_t bpr   = ctx0->blocks_per_row;
        float *probe_row_fast = NULL;
        float *probe_row_slow = NULL;
        unsigned char *probe_row_match = NULL;

        /* Temporary buffer for M-folded x when any tensor has m_fold_col.
         * We allocate the maximum cols needed. */
        float **x_eff_t = (float **)calloc((size_t)n, sizeof(float *));
        int any_fold = 0;
        for (int t = 0; t < n; t++) {
            if (grp->ctxs[t]->m_fold_col) { any_fold = 1; break; }
        }
        if (any_fold) {
            for (int t = 0; t < n; t++) {
                fpqx_sli_ctx_t *ctx = grp->ctxs[t];
                if (ctx->m_fold_col) {
                    x_eff_t[t] = (float *)malloc(ctx->cols * sizeof(float));
                    if (x_eff_t[t]) {
                        for (size_t j = 0; j < ctx->cols; j++)
                            x_eff_t[t][j] = x[j] * ctx->m_fold_col[j];
                    }
                }
                /* else x_eff_t[t] stays NULL → use raw x */
            }
        }

        probe_row_fast = (float *)calloc((size_t)n, sizeof(float));
        probe_row_slow = (float *)calloc((size_t)n, sizeof(float));
        probe_row_match = (unsigned char *)calloc((size_t)n, sizeof(unsigned char));

        for (size_t i = 0; i < rows0; i++) {
            if (probe_row_fast) memset(probe_row_fast, 0, (size_t)n * sizeof(float));
            if (probe_row_slow) memset(probe_row_slow, 0, (size_t)n * sizeof(float));
            if (probe_row_match) {
                memset(probe_row_match, 0, (size_t)n * sizeof(unsigned char));
                for (int t = 0; t < n; t++) {
                    probe_row_match[t] = (unsigned char)fpqx_sli_score_probe_match(grp->ctxs[t], i);
                }
            }
            if (fpqx_sli_score_probe_only_rows_enabled() && probe_row_match) {
                int any_match = 0;
                for (int t = 0; t < n; t++) {
                    if (probe_row_match[t]) {
                        any_match = 1;
                        break;
                    }
                }
                if (!any_match) continue;
            }
            for (size_t bj = 0; bj < bpr; bj++) {
                size_t col_offset = bj * SLI_BLOCK_DIM;

                /* Load x_b to stack once — this is the bandwidth-saving step */
                float x_b[SLI_BLOCK_DIM];
                memset(x_b, 0, SLI_BLOCK_DIM * sizeof(float));

                for (int t = 0; t < n; t++) {
                    fpqx_sli_ctx_t *ctx = grp->ctxs[t];
                    size_t b = i * bpr + bj;
                    if (b >= ctx->n_total_blocks) continue;

                    /* Determine effective x for this tensor */
                    const float *x_src = x_eff_t[t] ? x_eff_t[t] : x;
                    size_t col_end = col_offset + SLI_BLOCK_DIM;
                    if (col_end > ctx->cols) col_end = ctx->cols;
                    size_t this_dim = (col_end > col_offset) ?
                                       col_end - col_offset : 0;

                    /* Fill x_b from x_src (only on first tensor or if x_src differs) */
                    if (t == 0) {
                        memset(x_b, 0, SLI_BLOCK_DIM * sizeof(float));
                        if (this_dim > 0)
                            memcpy(x_b, x_src + col_offset, this_dim * sizeof(float));
                    } else if (x_eff_t[t] != NULL) {
                        /* This tensor has its own fold — need separate x_b */
                        float x_b2[SLI_BLOCK_DIM];
                        memset(x_b2, 0, SLI_BLOCK_DIM * sizeof(float));
                        if (this_dim > 0)
                            memcpy(x_b2, x_src + col_offset, this_dim * sizeof(float));
                        uint64_t block_seed = grp->haar_seeds[t] ^ b;
                        float fast_block_score = 0.0f;
                        if (ctx->z_data_f16) {
                            fast_block_score = sli_fast_block_score_f16(
                                ctx->z_data_f16 + b * SLI_BLOCK_DIM,
                                x_b2, this_dim, block_seed);
                        } else
#ifdef FPQ_INT8_SDOT
                        if (ctx->z_data_i8 && ctx->z_data_scale) {
                            fast_block_score = sli_fast_block_score_i8(
                                ctx->z_data_i8 + b * SLI_BLOCK_DIM,
                                ctx->z_data_scale[b],
                                x_b2, this_dim, block_seed);
                        } else
#endif
                        {
                            const float *z_b = ctx->z_data + b * SLI_BLOCK_DIM;
                            fast_block_score = sli_fast_block_score(z_b, x_b2,
                                                                    this_dim, block_seed);
                        }
                        outputs[t][i] += fast_block_score;
                        continue;
                    }
                    /* else: x_b is already loaded (t==0 case or same x_src) */

                    uint64_t block_seed = grp->haar_seeds[t] ^ b;
                    float fast_block_score = 0.0f;
                    if (ctx->z_data_f16) {
                        fast_block_score = sli_fast_block_score_f16(
                            ctx->z_data_f16 + b * SLI_BLOCK_DIM,
                            x_b, this_dim, block_seed);
                    } else
#ifdef FPQ_INT8_SDOT
                    if (ctx->z_data_i8 && ctx->z_data_scale) {
                        fast_block_score = sli_fast_block_score_i8(
                            ctx->z_data_i8 + b * SLI_BLOCK_DIM,
                            ctx->z_data_scale[b],
                            x_b, this_dim, block_seed);
                    } else
#endif
                    {
                        const float *z_b = ctx->z_data + b * SLI_BLOCK_DIM;
                        fast_block_score = sli_fast_block_score(z_b, x_b,
                                                                this_dim, block_seed);
                    }
                    outputs[t][i] += fast_block_score;
                    if (probe_row_match && probe_row_match[t]) {
                        float slow_block_score = fpqx_sli_score_block_slow(
                            ctx, ctx->tensor->additive, x_src, b, col_offset, this_dim);
                        probe_row_fast[t] += fast_block_score;
                        probe_row_slow[t] += slow_block_score;
                        fpqx_sli_score_probe_emit(
                            "sli_score_probe_block", ctx, ctx->tensor->additive, i, b, col_offset, this_dim,
                            fast_block_score, slow_block_score, 1);
                    }
                }
            }
            if (probe_row_match) {
                for (int t = 0; t < n; t++) {
                    if (!probe_row_match[t]) continue;
                    fpqx_sli_score_probe_emit(
                        "sli_score_probe_row", grp->ctxs[t], grp->ctxs[t]->tensor->additive, i, SIZE_MAX, 0, grp->ctxs[t]->cols,
                        probe_row_fast[t], probe_row_slow[t], 1);
                }
            }
        }

        for (int t = 0; t < n; t++) free(x_eff_t[t]);
        free(x_eff_t);
        free(probe_row_fast);
        free(probe_row_slow);
        free(probe_row_match);
    }

    /* ── Phase 3: Ghost correction for each tensor ── */
    for (int t = 0; t < n; t++) {
        fpqx_sli_ctx_t   *ctx = grp->ctxs[t];
        const fpq_tensor_t *enc = ctx->tensor->additive;
        size_t rows = ctx->rows, cols = ctx->cols;

        if (enc->ghost && fabsf(enc->ghost->sigma) > 1e-10f) {
            float vt_dot = 0.0f;
            size_t gcols = enc->ghost->cols;
            for (size_t j = 0; j < gcols && j < cols; j++)
                vt_dot += enc->ghost->v[j] * x[j];
            float gscale = enc->ghost->sigma * vt_dot;
            size_t grows = enc->ghost->rows;
            for (size_t i = 0; i < grows && i < rows; i++)
                outputs[t][i] += gscale * enc->ghost->u[i];
        }
    }

    return 0;
}


void fpqx_sli_shared_free(fpqx_sli_shared_spectral_t *grp) {
    if (!grp) return;
    free(grp->haar_seeds);
    /* ctxs and their contents are NOT owned — caller frees them */
    free(grp);
}


/* ═══════════════════════════════════════════════════════════════════
 * Innovation 3: JIT SLI kernel (compile-time loop specialisation)
 *
 * Emits a .c file with N_ROWS and BPR baked as integer literals,
 * compiles it to a shared library, and dlopen()s the result.  The
 * compiler can then fully unroll the outer loops and emit optimal SIMD.
 * Falls back silently: if cc is unavailable, ctx->jit_fn stays NULL
 * and the regular fast path is used.
 *
 * Gated on  -DFPQ_JIT_SLI  at compile time.
 * ═══════════════════════════════════════════════════════════════════ */

#ifdef FPQ_JIT_SLI
/* fpqx_sli_jit_compile — implementation (forward-declared above) */
static void fpqx_sli_jit_compile(fpqx_sli_ctx_t *ctx, uint64_t haar_seed)
{
    (void)haar_seed; /* haar_seed is passed to the kernel at call time */

    size_t rows = ctx->rows;
    size_t bpr  = ctx->blocks_per_row;

    /* Stable hash from shape — avoids recompilation for same-shaped tensors */
    unsigned long hash = (unsigned long)(rows * 1000003UL + bpr * 131UL);
    char src_path[256], so_path[256];
    snprintf(src_path, sizeof(src_path), "/tmp/fpq_jit_%lu.c",  hash);
    snprintf(so_path,  sizeof(so_path),  "/tmp/fpq_jit_%lu.so", hash);

    struct stat st;
    if (stat(so_path, &st) != 0) {
        /* Emit the specialised kernel source */
        FILE *f = fopen(src_path, "w");
        if (!f) return;

        fprintf(f,
            "/* FPQ JIT kernel: rows=%zu bpr=%zu — auto-generated */\n"
            "#include <string.h>\n"
            "#define N_ROWS %zu\n"
            "#define BPR    %zu\n"
            "#define BDIM   256\n\n"
            "void sli_jit_kernel(\n"
            "    const float *z_fwht,\n"
            "    const float *x,\n"
            "    float       *output,\n"
            "    unsigned long long haar_seed,\n"
            "    unsigned long cols)\n"
            "{\n"
            "    unsigned long bpr_ = BPR;\n"
            "    for (unsigned long i = 0; i < N_ROWS; i++) {\n"
            "        float row_score = 0.0f;\n"
            "        for (unsigned long bj = 0; bj < bpr_; bj++) {\n"
            "            unsigned long b = i * bpr_ + bj;\n"
            "            unsigned long col_off = bj * BDIM;\n"
            "            unsigned long this_dim = (col_off + BDIM <= cols)\n"
            "                                     ? BDIM : (cols - col_off);\n"
            "            float xs[BDIM];\n"
            "            memset(xs, 0, BDIM * sizeof(float));\n"
            "            memcpy(xs, x + col_off, this_dim * sizeof(float));\n"
            "            unsigned long long state = (haar_seed ^ b)\n"
            "                                       ? (haar_seed ^ b)\n"
            "                                       : 0x5DEECE66DULL;\n"
            "            for (unsigned long si = 0; si < BDIM; si += 64) {\n"
            "                state ^= state << 13;\n"
            "                state ^= state >> 7;\n"
            "                state ^= state << 17;\n"
            "                for (unsigned long j = si; j < si+64 && j < BDIM; j++)\n"
            "                    if ((state >> (j - si)) & 1ULL) xs[j] = -xs[j];\n"
            "            }\n"
            "            const float *z_b = z_fwht + b * BDIM;\n"
            "            float dot = 0.0f;\n"
            "            for (unsigned long k = 0; k < BDIM; k++)\n"
            "                dot += z_b[k] * xs[k];\n"
            "            row_score += dot;\n"
            "        }\n"
            "        output[i] += row_score;\n"
            "    }\n"
            "}\n",
            rows, bpr, rows, bpr);

        fclose(f);

        /* Compile — paths contain only decimal digits and underscores,
         * so no injection risk. */
        char cmd[512];
        snprintf(cmd, sizeof(cmd),
                 "cc -O3 -march=native -shared -fPIC -o '%s' '%s' 2>/dev/null",
                 so_path, src_path);
        int rc = system(cmd);
        (void)unlink(src_path);
        if (rc != 0) return;
    }

    void *handle = dlopen(so_path, RTLD_NOW | RTLD_LOCAL);
    if (!handle) return;

    void (*fn)(const float *, const float *, float *,
               unsigned long long, unsigned long) =
        (void (*)(const float *, const float *, float *,
                  unsigned long long, unsigned long))
        dlsym(handle, "sli_jit_kernel");

    if (!fn) { dlclose(handle); return; }

    ctx->jit_handle = handle;
    ctx->jit_fn     = fn;
    strncpy(ctx->jit_so_path, so_path, sizeof(ctx->jit_so_path) - 1);
}
#endif /* FPQ_JIT_SLI */


/* ═══════════════════════════════════════════════════════════════════
 * SLI Benchmark: compare SLI output against dense decode matmul
 * ═══════════════════════════════════════════════════════════════════ */

float fpqx_sli_benchmark(const fpqx_tensor_t *t,
                           const float *x, size_t x_len) {
    if (!t || !t->additive) return -1.0f;

    size_t rows = t->rows;
    size_t cols = t->cols;
    if (x_len < cols) return -1.0f;

    /* Dense decode + matmul (reference) */
    float *W_decoded = (float *)calloc(rows * cols, sizeof(float));
    fpqx_decode(t, W_decoded);

    float *y_dense = (float *)calloc(rows, sizeof(float));
    for (size_t i = 0; i < rows; i++) {
        float dot = 0.0f;
        for (size_t j = 0; j < cols; j++)
            dot += W_decoded[i * cols + j] * x[j];
        y_dense[i] = dot;
    }

    /* SLI matmul */
    float *y_sli = (float *)calloc(rows, sizeof(float));
    fpqx_sli_matvec_oneshot(t, x, y_sli);

    /* Cosine similarity */
    double dot_ds = 0.0, norm_d = 0.0, norm_s = 0.0;
    for (size_t i = 0; i < rows; i++) {
        dot_ds += (double)y_dense[i] * (double)y_sli[i];
        norm_d += (double)y_dense[i] * (double)y_dense[i];
        norm_s += (double)y_sli[i] * (double)y_sli[i];
    }
    float cosine = (float)(dot_ds / (sqrt(norm_d) * sqrt(norm_s) + 1e-30));

    /* Pearson correlation */
    double mean_d = 0.0, mean_s = 0.0;
    for (size_t i = 0; i < rows; i++) {
        mean_d += y_dense[i];
        mean_s += y_sli[i];
    }
    mean_d /= rows;
    mean_s /= rows;
    double cov = 0.0, var_d = 0.0, var_s = 0.0;
    for (size_t i = 0; i < rows; i++) {
        double dd = y_dense[i] - mean_d;
        double ds = y_sli[i] - mean_s;
        cov += dd * ds;
        var_d += dd * dd;
        var_s += ds * ds;
    }
    float pearson = (float)(cov / (sqrt(var_d) * sqrt(var_s) + 1e-30));

    /* Top-k agreement */
    int topk_match[3] = {0, 0, 0};
    int ks[3] = {1, 5, 10};
    for (int ki = 0; ki < 3; ki++) {
        int k = ks[ki];
        if ((size_t)k > rows) k = (int)rows;
        /* Simple: find top-k in each and count overlap */
        /* (Bubble sort the top-k for small k) */
        int *top_d = (int *)calloc(k, sizeof(int));
        int *top_s = (int *)calloc(k, sizeof(int));
        float *tmp_d = (float *)malloc(rows * sizeof(float));
        float *tmp_s = (float *)malloc(rows * sizeof(float));
        memcpy(tmp_d, y_dense, rows * sizeof(float));
        memcpy(tmp_s, y_sli, rows * sizeof(float));
        for (int j = 0; j < k; j++) {
            int best_d = 0, best_s = 0;
            for (size_t i = 1; i < rows; i++) {
                if (tmp_d[i] > tmp_d[best_d]) best_d = (int)i;
                if (tmp_s[i] > tmp_s[best_s]) best_s = (int)i;
            }
            top_d[j] = best_d;
            top_s[j] = best_s;
            tmp_d[best_d] = -FLT_MAX;
            tmp_s[best_s] = -FLT_MAX;
        }
        for (int a = 0; a < k; a++)
            for (int b = 0; b < k; b++)
                if (top_d[a] == top_s[b]) { topk_match[ki]++; break; }
        free(top_d); free(top_s); free(tmp_d); free(tmp_s);
    }

    /* Bandwidth ratio */
    size_t bpr = (cols + SLI_BLOCK_DIM - 1) / SLI_BLOCK_DIM;
    size_t dense_bytes = 2 * rows * cols;      /* BF16 */
    size_t sli_bytes = rows * bpr * 64;        /* 64 bytes per block */
    float bw_ratio = (float)dense_bytes / (float)sli_bytes;

    fprintf(stderr,
        "    SLI Benchmark (%zu × %zu):\n"
        "      Cosine(SLI, Dense):  %.10f\n"
        "      Pearson correlation: %.10f\n"
        "      Top-1/5/10 agree:    %d/%d  %d/%d  %d/%d\n"
        "      Bandwidth ratio:     %.1f× (dense %zu B, SLI %zu B)\n",
        rows, cols,
        cosine, pearson,
        topk_match[0], ks[0], topk_match[1], ks[1], topk_match[2], ks[2],
        bw_ratio, dense_bytes, sli_bytes);

    free(W_decoded);
    free(y_dense);
    free(y_sli);
    return cosine;
}


/* ═══════════════════════════════════════════════════════════════════
 * SLI Degraded Matvec: simulate reduced-precision packed formats.
 *
 * Degradation parameters:
 *   e8_clamp_bits: 0 = no clamp, N = clamp E8 int8 to ±(2^(N-1)-1)
 *   tile_max_idx:  0 = no change, N = remap tile idx to 0..N-1
 *   fp8_scales:    0 = FP16, 1 = simulate E4M3 FP8
 * ═══════════════════════════════════════════════════════════════════ */

/* E4M3 simulation: 4-bit exponent, 3-bit mantissa, range ±448, min subnormal 2^-9 */
static float snap_fp8_e4m3(float v) {
    if (v == 0.0f) return 0.0f;
    float sign = v < 0.0f ? -1.0f : 1.0f;
    float av = fabsf(v);
    if (av > 448.0f) av = 448.0f;
    if (av < (1.0f / 512.0f)) return 0.0f;
    /* Find exponent and quantize mantissa to 3 bits (8 levels) */
    int e = (int)floorf(log2f(av));
    if (e < -6) e = -6;
    if (e > 8) e = 8;
    float scale = ldexpf(1.0f, e);
    float mant = av / scale;   /* 1.0 ≤ mant < 2.0 for normals */
    mant = roundf(mant * 8.0f) / 8.0f;
    return sign * mant * scale;
}

static int sli_matvec_degraded(const fpqx_tensor_t *t, const float *x,
                                float *output, int e8_clamp_bits,
                                int tile_max_idx, int fp8_scales) {
    if (!t || !t->additive) return -1;

    const fpq_tensor_t *enc = t->additive;
    size_t rows = t->rows;
    size_t cols = t->cols;
    size_t bpr = (cols + SLI_BLOCK_DIM - 1) / SLI_BLOCK_DIM;
    size_t n_blocks = enc->n_blocks;
    float beta = SLI_MU_BETA;
    int coord_bits = (int)enc->coord_bits;
    float lattice_scale = 8.0f * (float)coord_bits;

    /* v8/v9 layout detection */
    int is_v9 = (enc->pid_alpha == -9.0f);
    int lr_rank = is_v9 ? (int)enc->sbb_scale_delta[0] : 0;
    size_t us_off = is_v9 ? 2 : 0;
    size_t lr_us_size = rows * (size_t)lr_rank;
    size_t vt_off = us_off + lr_us_size;
    size_t lr_vt_size = (size_t)lr_rank * cols;
    size_t v8_base = is_v9 ? (2 + lr_us_size + lr_vt_size) : 0;

    /* LR contribution (no degradation — LR is already quantized at encode) */
    float *vt_x = (float *)calloc(lr_rank > 0 ? lr_rank : 1, sizeof(float));
    for (int r = 0; r < lr_rank; r++) {
        float dot = 0.0f;
        for (size_t j = 0; j < cols; j++)
            dot += enc->sbb_scale_delta[vt_off + r * cols + j] * x[j];
        vt_x[r] = dot;
    }
    for (size_t i = 0; i < rows; i++) {
        float lr_val = 0.0f;
        for (int r = 0; r < lr_rank; r++)
            lr_val += enc->sbb_scale_delta[us_off + i * lr_rank + r] * vt_x[r];
        output[i] = lr_val;
    }
    free(vt_x);

    /* v8 data offsets */
    size_t e8_off = v8_base + n_blocks;
    size_t e8_flat_size = n_blocks * SLI_BLOCK_DIM;
    size_t tile_cb_off = e8_off + e8_flat_size;
    int effective_k = SLI_RVQ_TILES;
    {
        size_t test_cb = (size_t)effective_k * SLI_TILE_DIM;
        size_t test_ek = tile_cb_off + test_cb + n_blocks * SLI_E8_PAIRS;
        float stored = enc->sbb_scale_delta[test_ek];
        if (stored >= 16.0f && stored <= 256.0f)
            effective_k = (int)stored;
    }
    if (effective_k == SLI_RVQ_TILES) {
        for (int try_k = 16; try_k <= 256; try_k *= 2) {
            size_t off = tile_cb_off + (size_t)try_k * SLI_TILE_DIM +
                         n_blocks * SLI_E8_PAIRS;
            float stored = enc->sbb_scale_delta[off];
            if ((int)stored == try_k) { effective_k = try_k; break; }
        }
    }
    size_t tile_cb_size = (size_t)effective_k * SLI_TILE_DIM;
    size_t tile_idx_off = tile_cb_off + tile_cb_size;
    const float *tile_data = enc->sbb_scale_delta + tile_cb_off;

    /* E8 clamp range */
    int e8_clamp = e8_clamp_bits > 0 ? (1 << (e8_clamp_bits - 1)) - 1 : 127;

    /* Working buffers */
    float *signs_buf = (float *)malloc(SLI_BLOCK_DIM * sizeof(float));
    float *x_block = (float *)malloc(SLI_BLOCK_DIM * sizeof(float));
    float *x_spectral = (float *)malloc(SLI_BLOCK_DIM * sizeof(float));

    for (size_t i = 0; i < rows; i++) {
        float row_sli_score = 0.0f;

        for (size_t bj = 0; bj < bpr; bj++) {
            size_t b = i * bpr + bj;
            if (b >= n_blocks) break;

            size_t col_offset = bj * SLI_BLOCK_DIM;
            size_t col_end = col_offset + SLI_BLOCK_DIM;
            if (col_end > cols) col_end = cols;
            size_t this_dim = col_end - col_offset;

            /* Scales with optional FP8 degradation */
            float rms = enc->coord_scales[b];
            float wnorm = enc->sbb_scale_delta[v8_base + b];
            if (fp8_scales) {
                rms = snap_fp8_e4m3(rms);
                wnorm = snap_fp8_e4m3(wnorm);
            }
            const float *e8_pts = enc->sbb_scale_delta + e8_off +
                                  b * SLI_BLOCK_DIM;

            /* Spectral bypass */
            memset(x_block, 0, SLI_BLOCK_DIM * sizeof(float));
            memcpy(x_block, x + col_offset, this_dim * sizeof(float));
            uint64_t block_seed = enc->haar_seed ^ (uint64_t)b;
            sli_random_signs(signs_buf, SLI_BLOCK_DIM, block_seed);
            for (size_t k = 0; k < SLI_BLOCK_DIM; k++)
                x_block[k] *= signs_buf[k];
            fpq_fwht(x_block, SLI_BLOCK_DIM);
            memcpy(x_spectral, x_block, SLI_BLOCK_DIM * sizeof(float));

            /* E8+tile scoring with degraded E8 and tile indices */
            float base_tile_score = 0.0f;
            for (int p = 0; p < SLI_E8_PAIRS; p++) {
                int ti = (int)enc->sbb_scale_delta[tile_idx_off +
                                                     b * SLI_E8_PAIRS + p];
                /* Tile index remapping for reduced codebook */
                if (tile_max_idx > 0 && ti >= tile_max_idx)
                    ti = ti % tile_max_idx;
                if (ti < 0) ti = 0;
                if (ti >= effective_k) ti = effective_k - 1;
                size_t pair_base = (size_t)p * SLI_TILE_DIM;

                for (int d = 0; d < SLI_TILE_DIM; d++) {
                    /* E8 coordinate with bit-depth clamping */
                    float e8_val = e8_pts[pair_base + d];
                    int e8_int = (int)e8_val;
                    if (e8_int > e8_clamp) e8_int = e8_clamp;
                    if (e8_int < -e8_clamp) e8_int = -e8_clamp;

                    float corrected = (float)e8_int +
                                      tile_data[ti * SLI_TILE_DIM + d];
                    float lat_val = corrected / lattice_scale * wnorm;
                    float z_val = sli_unwarp(lat_val, beta) * rms;
                    base_tile_score += z_val * x_spectral[pair_base + d];
                }
            }

            row_sli_score += base_tile_score;
        }
        output[i] += row_sli_score;
    }

    /* Ghost correction */
    if (enc->ghost && fabsf(enc->ghost->sigma) > 1e-10f) {
        float vt_dot = 0.0f;
        size_t ghost_cols = enc->ghost->cols;
        for (size_t j = 0; j < ghost_cols && j < cols; j++)
            vt_dot += enc->ghost->v[j] * x[j];
        float ghost_scale = enc->ghost->sigma * vt_dot;
        size_t ghost_rows = enc->ghost->rows;
        for (size_t i = 0; i < ghost_rows && i < rows; i++)
            output[i] += ghost_scale * enc->ghost->u[i];
    }

    free(signs_buf); free(x_block); free(x_spectral);
    return 0;
}


/* ═══════════════════════════════════════════════════════════════════
 * SLI Optimization Sweep: explore all four bandwidth levers
 *
 * Native .fpq format per block (baseline):
 *   E8 coords:   256 B (INT8)
 *   Tile idx:     16 B (uint8, 16 pairs)
 *   Scales:        4 B (2× FP16: coord_scale + warp_norm)
 *   QJL:           8 B (64 bits) — already proven droppable
 *   rnorm:         1 B (INT8 + shared scale)
 *   TOTAL:       285 B/block = 1.11 B/param  (BF16 = 512 B = 2.0 B/param)
 *
 * Levers tested:
 *   1. E8 bit reduction: INT8→INT6→INT5→INT4 (lossy — clamp outliers)
 *   2. Tile idx:  uint8(256)→6-bit(64)→5-bit(32) (lossy — remap)
 *   3. FP8 scales: FP16→E4M3 FP8 (lossy)
 *   4. E8 entropy: measure Shannon entropy for lossless compression
 * ═══════════════════════════════════════════════════════════════════ */

void fpqx_sli_optimization_sweep(const fpqx_tensor_t *t,
                                   const float *x, size_t x_len) {
    if (!t || !t->additive) return;

    size_t rows = t->rows;
    size_t cols = t->cols;
    if (x_len < cols) return;

    const fpq_tensor_t *enc = t->additive;
    size_t n_blocks = enc->n_blocks;
    int coord_bits = (int)enc->coord_bits;
    size_t bf16_block = SLI_BLOCK_DIM * 2;  /* 512 bytes */

    /* ── v8/v9 layout ── */
    int is_v9 = (enc->pid_alpha == -9.0f);
    int lr_rank = is_v9 ? (int)enc->sbb_scale_delta[0] : 0;
    size_t lr_us_size = rows * (size_t)lr_rank;
    size_t lr_vt_size = (size_t)lr_rank * cols;
    size_t v8_base = is_v9 ? (2 + lr_us_size + lr_vt_size) : 0;
    size_t e8_off = v8_base + n_blocks;
    size_t e8_total = n_blocks * SLI_BLOCK_DIM;

    /* ── Tile codebook size detection ── */
    size_t tile_cb_off = e8_off + n_blocks * SLI_BLOCK_DIM;
    int effective_k = SLI_RVQ_TILES;
    {
        size_t test_cb = (size_t)effective_k * SLI_TILE_DIM;
        size_t test_ek = tile_cb_off + test_cb + n_blocks * SLI_E8_PAIRS;
        float stored = enc->sbb_scale_delta[test_ek];
        if (stored >= 16.0f && stored <= 256.0f)
            effective_k = (int)stored;
    }
    if (effective_k == SLI_RVQ_TILES) {
        for (int try_k = 16; try_k <= 256; try_k *= 2) {
            size_t off = tile_cb_off + (size_t)try_k * SLI_TILE_DIM +
                         n_blocks * SLI_E8_PAIRS;
            float stored = enc->sbb_scale_delta[off];
            if ((int)stored == try_k) { effective_k = try_k; break; }
        }
    }
    size_t tile_idx_off = tile_cb_off + (size_t)effective_k * SLI_TILE_DIM;

    /* ╔══════════════════════════════════════════╗
     * ║  ANALYSIS 1: E8 Coordinate Distribution  ║
     * ╚══════════════════════════════════════════╝ */
    int e8_hist[256] = {0};  /* histogram of (uint8_t)(e8_val + 128) */
    float e8_min = 1e30f, e8_max = -1e30f;
    int e8_is_half_int = 0;
    int e8_clipped[9] = {0};  /* e8_clipped[b] = # coords clipped at b bits */

    for (size_t b = 0; b < n_blocks; b++) {
        const float *e8_pts = enc->sbb_scale_delta + e8_off + b * SLI_BLOCK_DIM;
        for (size_t d = 0; d < SLI_BLOCK_DIM; d++) {
            float v = e8_pts[d];
            if (v < e8_min) e8_min = v;
            if (v > e8_max) e8_max = v;
            float frac = v - floorf(v);
            if (fabsf(frac - 0.5f) < 0.01f) e8_is_half_int = 1;
            int iv = (int)v;
            int hbin = iv + 128;
            if (hbin < 0) hbin = 0;
            if (hbin > 255) hbin = 255;
            e8_hist[hbin]++;
            /* Count clipped coords at each bit depth */
            for (int nb = 4; nb <= 8; nb++) {
                int lim = (1 << (nb - 1)) - 1;
                if (iv > lim || iv < -lim) e8_clipped[nb]++;
            }
        }
    }

    /* Shannon entropy of E8 coordinates */
    double e8_entropy = 0.0;
    for (int i = 0; i < 256; i++) {
        if (e8_hist[i] > 0) {
            double p = (double)e8_hist[i] / (double)e8_total;
            e8_entropy -= p * log2(p);
        }
    }

    float e8_absmax = fabsf(e8_min) > fabsf(e8_max) ?
        fabsf(e8_min) : fabsf(e8_max);

    /* ╔══════════════════════════════════════════╗
     * ║  ANALYSIS 2: Tile Index Distribution      ║
     * ╚══════════════════════════════════════════╝ */
    int tile_hist[256] = {0};
    int tile_max_used = 0;
    size_t tile_total = n_blocks * SLI_E8_PAIRS;
    for (size_t b = 0; b < n_blocks; b++) {
        for (int p = 0; p < SLI_E8_PAIRS; p++) {
            int ti = (int)enc->sbb_scale_delta[tile_idx_off + b * SLI_E8_PAIRS + p];
            if (ti < 0) ti = 0;
            if (ti > 255) ti = 255;
            tile_hist[ti]++;
            if (ti > tile_max_used) tile_max_used = ti;
        }
    }
    /* Tile entropy */
    double tile_entropy = 0.0;
    for (int i = 0; i < 256; i++) {
        if (tile_hist[i] > 0) {
            double p = (double)tile_hist[i] / (double)tile_total;
            tile_entropy -= p * log2(p);
        }
    }
    /* How many tile indices fit in N bits? */
    int tile_bits_needed = 1;
    while ((1 << tile_bits_needed) <= tile_max_used)
        tile_bits_needed++;

    /* ╔══════════════════════════════════════════╗
     * ║  ANALYSIS 3: Scale Distribution           ║
     * ╚══════════════════════════════════════════╝ */
    float rms_min = 1e30f, rms_max = -1e30f;
    float wnorm_min = 1e30f, wnorm_max = -1e30f;
    double fp8_rms_mse = 0.0, fp8_wnorm_mse = 0.0;
    for (size_t b = 0; b < n_blocks; b++) {
        float rms = enc->coord_scales[b];
        float wnorm = enc->sbb_scale_delta[v8_base + b];
        if (rms < rms_min) rms_min = rms;
        if (rms > rms_max) rms_max = rms;
        if (wnorm < wnorm_min) wnorm_min = wnorm;
        if (wnorm > wnorm_max) wnorm_max = wnorm;
        /* FP8 quantization error */
        float rms8 = snap_fp8_e4m3(rms);
        float wnorm8 = snap_fp8_e4m3(wnorm);
        fp8_rms_mse += (double)(rms - rms8) * (double)(rms - rms8);
        fp8_wnorm_mse += (double)(wnorm - wnorm8) * (double)(wnorm - wnorm8);
    }
    fp8_rms_mse /= n_blocks;
    fp8_wnorm_mse /= n_blocks;

    /* ╔══════════════════════════════════════════╗
     * ║  PRINT DATA ANALYSIS                      ║
     * ╚══════════════════════════════════════════╝ */
    fprintf(stderr,
        "\n    ══════════════════════════════════════════════════════════\n"
        "    SLI Optimization Lever Analysis  [%s]\n"
        "    coord_bits=%d  effective_k=%d  n_blocks=%zu\n"
        "    ══════════════════════════════════════════════════════════\n",
        is_v9 ? "v9" : "v8", coord_bits, effective_k, n_blocks);

    /* Lever 1: E8 packing */
    fprintf(stderr,
        "\n    ── LEVER 1: E8 Coordinate Packing ──\n"
        "    Current: INT8 (256 B/block), range [%.0f, %.0f], |max|=%.1f\n"
        "    Half-integer: %s\n"
        "    Shannon entropy: %.2f bits/coord → ideal %.0f B/block (lossless)\n"
        "    ┌──────────┬───────────┬──────────────┬──────────┐\n"
        "    │ Packing  │ B/block   │ Clipped %%    │ Savings  │\n"
        "    ├──────────┼───────────┼──────────────┼──────────┤\n",
        e8_min, e8_max, e8_absmax,
        e8_is_half_int ? "yes" : "no",
        e8_entropy, ceilf(256.0f * (float)e8_entropy / 8.0f));

    int e8_pack_bits[] = {8, 7, 6, 5, 4};
    for (int pi = 0; pi < 5; pi++) {
        int nb = e8_pack_bits[pi];
        size_t bytes = (SLI_BLOCK_DIM * nb + 7) / 8;
        float clip_pct = 100.0f * (float)e8_clipped[nb > 8 ? 8 : nb] /
                         (float)e8_total;
        int savings = 256 - (int)bytes;
        fprintf(stderr,
            "    │ INT%-2d    │ %4zu B    │ %8.4f%%     │ %+4d B   │\n",
            nb, bytes, clip_pct, -savings);
    }
    /* Entropy-coded row */
    size_t entropy_bytes = (size_t)ceilf(256.0f * (float)e8_entropy / 8.0f);
    fprintf(stderr,
        "    │ Entropy  │ %4zu B    │   0.0000%%     │ %+4d B   │\n",
        entropy_bytes, (int)entropy_bytes - 256);
    fprintf(stderr,
        "    └──────────┴───────────┴──────────────┴──────────┘\n");

    /* Lever 2: Tile index packing */
    fprintf(stderr,
        "\n    ── LEVER 2: Tile Index Packing ──\n"
        "    Current: uint8 (16 B/block), codebook=%d entries, max used=%d\n"
        "    Shannon entropy: %.2f bits/idx → ideal %.1f B/block (lossless)\n"
        "    Bits needed for max used: %d\n"
        "    ┌──────────┬───────────┬──────────────┬──────────┐\n"
        "    │ Packing  │ B/block   │ Coverage %%   │ Savings  │\n"
        "    ├──────────┼───────────┼──────────────┼──────────┤\n",
        effective_k, tile_max_used,
        tile_entropy, 16.0f * (float)tile_entropy / 8.0f,
        tile_bits_needed);

    int tile_pack_bits[] = {8, 7, 6, 5};
    for (int pi = 0; pi < 4; pi++) {
        int nb = tile_pack_bits[pi];
        size_t bytes = (SLI_E8_PAIRS * nb + 7) / 8;
        int covers = (1 << nb);
        float coverage = covers >= effective_k ? 100.0f :
            100.0f * (float)covers / (float)effective_k;
        int savings = 16 - (int)bytes;
        fprintf(stderr,
            "    │ %d-bit    │ %4zu B    │ %8.1f%%     │ %+4d B   │\n",
            nb, bytes, coverage, -savings);
    }
    fprintf(stderr,
        "    └──────────┴───────────┴──────────────┴──────────┘\n");

    /* Lever 3: Scale precision */
    fprintf(stderr,
        "\n    ── LEVER 3: Scale Precision ──\n"
        "    Current: 2× FP16 (4 B/block)\n"
        "    coord_scale range: [%.6f, %.6f]\n"
        "    warp_norm  range:  [%.6f, %.6f]\n"
        "    FP8 (E4M3) quantization RMSE:\n"
        "      coord_scale: %.2e    warp_norm: %.2e\n"
        "    ┌──────────┬───────────┬──────────┐\n"
        "    │ Format   │ B/block   │ Savings  │\n"
        "    ├──────────┼───────────┼──────────┤\n"
        "    │ 2×FP16   │    4 B    │   ±0 B   │\n"
        "    │ 2×FP8    │    2 B    │   −2 B   │\n"
        "    │ 1×FP16   │    2 B    │   −2 B   │  (shared-exponent pair)\n"
        "    └──────────┴───────────┴──────────┘\n",
        rms_min, rms_max, wnorm_min, wnorm_max,
        sqrt(fp8_rms_mse), sqrt(fp8_wnorm_mse));

    /* ╔══════════════════════════════════════════╗
     * ║  QUALITY IMPACT: Run degraded SLI          ║
     * ╚══════════════════════════════════════════╝ */

    /* Dense reference */
    float *W_decoded = (float *)calloc(rows * cols, sizeof(float));
    fpqx_decode(t, W_decoded);
    float *y_dense = (float *)calloc(rows, sizeof(float));
    for (size_t i = 0; i < rows; i++) {
        float dot = 0.0f;
        for (size_t j = 0; j < cols; j++)
            dot += W_decoded[i * cols + j] * x[j];
        y_dense[i] = dot;
    }
    double norm_d = 0.0;
    for (size_t i = 0; i < rows; i++)
        norm_d += (double)y_dense[i] * (double)y_dense[i];

    /* Helper macro: run degraded SLI and compute cosine */
    #define MEASURE_COS(e8b, tidx, fp8, lbl) do { \
        float *_y = (float *)calloc(rows, sizeof(float)); \
        sli_matvec_degraded(t, x, _y, e8b, tidx, fp8); \
        double _dot = 0.0, _ns = 0.0; \
        for (size_t _i = 0; _i < rows; _i++) { \
            _dot += (double)y_dense[_i] * (double)_y[_i]; \
            _ns  += (double)_y[_i] * (double)_y[_i]; \
        } \
        float _cos = (float)(_dot / (sqrt(norm_d) * sqrt(_ns) + 1e-30)); \
        int _top1 = 0; { \
            int _bd = 0, _bs = 0; \
            for (size_t _i = 1; _i < rows; _i++) { \
                if (y_dense[_i] > y_dense[_bd]) _bd = (int)_i; \
                if (_y[_i] > _y[_bs]) _bs = (int)_i; \
            } \
            _top1 = (_bd == _bs); \
        } \
        results[n_results].cosine = _cos; \
        results[n_results].top1 = _top1; \
        results[n_results].block_bytes = _blk; \
        strncpy(results[n_results].label, lbl, 47); \
        results[n_results].label[47] = '\0'; \
        n_results++; \
        free(_y); \
    } while(0)

    struct { char label[48]; float cosine; int top1; size_t block_bytes; }
        results[32];
    int n_results = 0;
    size_t _blk;

    fprintf(stderr,
        "\n    ══════════════════════════════════════════════════════════\n"
        "    Quality Impact Sweep (Cosine vs Decoded Dense)\n"
        "    ══════════════════════════════════════════════════════════\n"
        "    ┌─────────────────────────────────┬────────────┬──────┬─────────┬────────┐\n"
        "    │ Configuration                   │   Cosine   │ Top1 │ B/block │ BW vs  │\n"
        "    │                                 │            │      │         │  BF16  │\n"
        "    ├─────────────────────────────────┼────────────┼──────┼─────────┼────────┤\n");

    /* Baseline: full precision, QJL=0 */
    _blk = 256 + 16 + 4;  /* INT8 E8 + uint8 tile + FP16 scales */
    MEASURE_COS(0, 0, 0, "Baseline (INT8+u8+FP16)");

    /* ── Lever 1: E8 bit reduction ── */
    _blk = (SLI_BLOCK_DIM * 7 + 7) / 8 + 16 + 4;
    MEASURE_COS(7, 0, 0, "E8 INT7");
    _blk = (SLI_BLOCK_DIM * 6 + 7) / 8 + 16 + 4;
    MEASURE_COS(6, 0, 0, "E8 INT6");
    _blk = (SLI_BLOCK_DIM * 5 + 7) / 8 + 16 + 4;
    MEASURE_COS(5, 0, 0, "E8 INT5");
    _blk = (SLI_BLOCK_DIM * 4 + 7) / 8 + 16 + 4;
    MEASURE_COS(4, 0, 0, "E8 INT4");

    /* ── Lever 2: Tile index reduction ── */
    _blk = 256 + (SLI_E8_PAIRS * 6 + 7) / 8 + 4;
    MEASURE_COS(0, 64, 0, "Tile 6-bit (max 64)");
    _blk = 256 + (SLI_E8_PAIRS * 5 + 7) / 8 + 4;
    MEASURE_COS(0, 32, 0, "Tile 5-bit (max 32)");

    /* ── Lever 3: FP8 scales ── */
    _blk = 256 + 16 + 2;
    MEASURE_COS(0, 0, 1, "Scales FP8 (E4M3)");

    /* ── Combined: best of each ── */
    /* E8 INT6 + Tile 6-bit + FP16 scales */
    _blk = (SLI_BLOCK_DIM * 6 + 7) / 8 + (SLI_E8_PAIRS * 6 + 7) / 8 + 4;
    MEASURE_COS(6, 64, 0, "E8-6 + Tile-6 + FP16");

    /* E8 INT6 + Tile 6-bit + FP8 scales */
    _blk = (SLI_BLOCK_DIM * 6 + 7) / 8 + (SLI_E8_PAIRS * 6 + 7) / 8 + 2;
    MEASURE_COS(6, 64, 1, "E8-6 + Tile-6 + FP8");

    /* E8 INT5 + Tile 6-bit + FP8 scales */
    _blk = (SLI_BLOCK_DIM * 5 + 7) / 8 + (SLI_E8_PAIRS * 6 + 7) / 8 + 2;
    MEASURE_COS(5, 64, 1, "E8-5 + Tile-6 + FP8");

    /* E8 INT5 + Tile 5-bit + FP8 scales */
    _blk = (SLI_BLOCK_DIM * 5 + 7) / 8 + (SLI_E8_PAIRS * 5 + 7) / 8 + 2;
    MEASURE_COS(5, 32, 1, "E8-5 + Tile-5 + FP8");

    /* E8 Entropy + Tile 6-bit + FP8 scales (theoretical best lossless E8) */
    _blk = entropy_bytes + (SLI_E8_PAIRS * 6 + 7) / 8 + 2;
    MEASURE_COS(0, 64, 1, "Entropy-E8 + Tile-6 + FP8");

    /* Print all results */
    for (int ri = 0; ri < n_results; ri++) {
        float bw = (float)bf16_block / (float)results[ri].block_bytes;
        float bpp = (float)results[ri].block_bytes / (float)SLI_BLOCK_DIM;
        fprintf(stderr,
            "    │ %-33s│ %.8f │ %s  │ %4zu B  │ %4.1f×  │\n",
            results[ri].label, results[ri].cosine,
            results[ri].top1 ? "YES" : " NO",
            results[ri].block_bytes, bw);
    }

    fprintf(stderr,
        "    └─────────────────────────────────┴────────────┴──────┴─────────┴────────┘\n");

    #undef MEASURE_COS

    /* ╔══════════════════════════════════════════╗
     * ║  PRODUCTION PROJECTION TABLE              ║
     * ╚══════════════════════════════════════════╝ */

    /* Find best config: highest BW with cos ≥ 0.99990 */
    int best_ri = 0;
    float best_bw = 0.0f;
    for (int ri = 0; ri < n_results; ri++) {
        if (results[ri].cosine >= 0.99990f && results[ri].top1) {
            float bw = (float)bf16_block / (float)results[ri].block_bytes;
            if (bw > best_bw) { best_bw = bw; best_ri = ri; }
        }
    }
    float best_bpp = (float)results[best_ri].block_bytes / (float)SLI_BLOCK_DIM;

    /* Also compute theoretical best (entropy E8 lossless) */
    size_t theo_block = entropy_bytes + (SLI_E8_PAIRS * 6 + 7) / 8 + 2;
    float theo_bpp = (float)theo_block / (float)SLI_BLOCK_DIM;
    float theo_bw = (float)bf16_block / (float)theo_block;

    struct { const char *name; int ram_gb; float bw_gbs; } hw[] = {
        {"Raspberry Pi 5 (8GB) ", 8,   32.0f},
        {"M1 Air (8GB)         ", 8,   68.0f},
        {"M2 Air (16GB)        ", 16,  100.0f},
        {"RTX 3060 (12GB)      ", 12,  360.0f},
        {"RTX 4060 Ti (16GB)   ", 16,  288.0f},
        {"RTX 4090 (24GB)      ", 24, 1008.0f},
        {"M2 Ultra (192GB)     ", 192, 800.0f},
        {"2× RTX 4090 (48GB)   ", 48, 2016.0f},
    };
    int n_hw = 8;
    double params_8b = 8.03e9;
    double params_70b = 70.6e9;

    fprintf(stderr,
        "\n    ══════════════════════════════════════════════════════════\n"
        "    Hardware Projections\n"
        "    ══════════════════════════════════════════════════════════\n"
        "    Best lossy:    \"%s\" → %zu B/block (%.3f B/param) → %.1f× BW │ cos=%.8f\n"
        "    Best lossless: Entropy-E8+Tile-6+FP8 → %zu B/block (%.3f B/param) → %.1f× BW\n"
        "    Native INT8:   %zu B/block (%.3f B/param) → %.1f× BW\n\n",
        results[best_ri].label, results[best_ri].block_bytes, best_bpp, best_bw,
        results[best_ri].cosine,
        theo_block, theo_bpp, theo_bw,
        (size_t)(256 + 16 + 4), (256.0f + 16.0f + 4.0f) / 256.0f,
        (float)bf16_block / (256.0f + 16.0f + 4.0f));

    fprintf(stderr,
        "    ┌──────────────────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐\n"
        "    │ Hardware                 │  BF16 8B │ INT8 8B  │ Best 8B  │ BF16 70B │ INT8 70B │ Best 70B │\n"
        "    ├──────────────────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┤\n");

    float native_bpp = (256.0f + 16.0f + 4.0f) / 256.0f;
    for (int hi = 0; hi < n_hw; hi++) {
        int ram = hw[hi].ram_gb;
        float bw = hw[hi].bw_gbs;

        double bf16_8b_gb = params_8b * 2.0 / (1024.0*1024.0*1024.0);
        double nat_8b_gb  = params_8b * (double)native_bpp / (1024.0*1024.0*1024.0);
        double best_8b_gb = params_8b * (double)best_bpp / (1024.0*1024.0*1024.0);
        double bf16_70b_gb = params_70b * 2.0 / (1024.0*1024.0*1024.0);
        double nat_70b_gb  = params_70b * (double)native_bpp / (1024.0*1024.0*1024.0);
        double best_70b_gb = params_70b * (double)best_bpp / (1024.0*1024.0*1024.0);

        char c[6][16];
        struct { double gb; } cfgs[] = {
            {bf16_8b_gb}, {nat_8b_gb}, {best_8b_gb},
            {bf16_70b_gb}, {nat_70b_gb}, {best_70b_gb}
        };
        for (int ci = 0; ci < 6; ci++) {
            if (cfgs[ci].gb > ram)
                snprintf(c[ci], sizeof(c[ci]), " NO FIT ");
            else
                snprintf(c[ci], sizeof(c[ci]), "%4.0f t/s",
                         bw / cfgs[ci].gb);
        }
        fprintf(stderr,
            "    │ %s│ %s │ %s │ %s │ %s │ %s │ %s │\n",
            hw[hi].name, c[0], c[1], c[2], c[3], c[4], c[5]);
    }
    fprintf(stderr,
        "    └──────────────────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘\n"
        "    8B:  BF16=%.1fGB  INT8-SLI=%.1fGB  Best-SLI=%.1fGB\n"
        "    70B: BF16=%.1fGB  INT8-SLI=%.1fGB  Best-SLI=%.1fGB\n"
        "    (QJL=0 for all SLI configs — validated zero quality impact)\n\n",
        params_8b * 2.0 / (1024.0*1024.0*1024.0),
        params_8b * (double)native_bpp / (1024.0*1024.0*1024.0),
        params_8b * (double)best_bpp / (1024.0*1024.0*1024.0),
        params_70b * 2.0 / (1024.0*1024.0*1024.0),
        params_70b * (double)native_bpp / (1024.0*1024.0*1024.0),
        params_70b * (double)best_bpp / (1024.0*1024.0*1024.0));

    free(W_decoded);
    free(y_dense);
}


/* ═══════════════════════════════════════════════════════════════════
 * Phase 7: FPQx Operator Fusion — zero-copy sequential matvec chain
 *
 * Purpose: eliminates intermediate heap allocations when chaining
 * multiple SLI matvec operations (e.g., attention → projection).
 *
 * Without fusion:
 *   float *tmp = malloc(rows_0 * sizeof(float));
 *   fpqx_sli_matvec(ctx0, x, tmp);          // write tmp
 *   fpqx_sli_matvec(ctx1, tmp, out);         // read tmp, write out
 *   free(tmp);
 *
 * With fusion:
 *   fpqx_fuse_chain_t *ch = fpqx_fuse_chain_create(tensors, 2);
 *   fpqx_fuse_chain_matvec(ch, x, out);     // single scratch buffer,
 *                                            // owned by chain, reused
 *   fpqx_fuse_chain_free(ch);
 *
 * The scratch buffer is sized max(rows[i]) across all stages and
 * pingponged between stages. Zero extra malloc/free per inference call.
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct fpqx_fuse_chain_t {
    fpqx_sli_ctx_t **stages;   /* prepared SLI contexts, one per layer */
    size_t           n_stages;
    float           *scratch_a; /* ping buffer: max(rows) elements */
    float           *scratch_b; /* pong buffer: max(rows) elements */
    size_t           scratch_cap; /* capacity of each scratch buffer */
} fpqx_fuse_chain_t;

/*
 * fpqx_fuse_chain_create — prepare a multi-stage chain.
 *
 * tensors[]  : array of n encoded tensors, applied in order
 * n          : number of tensors
 *
 * Returns NULL on allocation failure.
 */
fpqx_fuse_chain_t *fpqx_fuse_chain_create(const fpqx_tensor_t **tensors,
                                           int n) {
    if (!tensors || n <= 0) return NULL;

    fpqx_fuse_chain_t *ch = (fpqx_fuse_chain_t *)calloc(1, sizeof(*ch));
    if (!ch) return NULL;

    ch->stages = (fpqx_sli_ctx_t **)calloc((size_t)n, sizeof(*ch->stages));
    if (!ch->stages) { free(ch); return NULL; }
    ch->n_stages = (size_t)n;

    size_t max_rows = 0;
    for (int i = 0; i < n; i++) {
        ch->stages[i] = fpqx_sli_prepare(tensors[i]);
        if (!ch->stages[i]) {
            /* rollback already-prepared stages */
            for (int j = 0; j < i; j++) fpqx_sli_free(ch->stages[j]);
            free(ch->stages);
            free(ch);
            return NULL;
        }
        if (tensors[i]->rows > max_rows) max_rows = tensors[i]->rows;
    }

    ch->scratch_cap = max_rows;
    ch->scratch_a = (float *)calloc(max_rows, sizeof(float));
    ch->scratch_b = (float *)calloc(max_rows, sizeof(float));
    if (!ch->scratch_a || !ch->scratch_b) {
        for (size_t i = 0; i < ch->n_stages; i++) fpqx_sli_free(ch->stages[i]);
        free(ch->stages);
        free(ch->scratch_a);
        free(ch->scratch_b);
        free(ch);
        return NULL;
    }
    return ch;
}

/*
 * fpqx_fuse_chain_matvec — execute all stages with zero intermediate allocations.
 *
 * x_in  : input vector (length = tensors[0]->cols)
 * y_out : output vector (length = tensors[n-1]->rows) — caller allocates
 *
 * Internal scratch buffers are pingponged: no malloc/free per call.
 */
int fpqx_fuse_chain_matvec(fpqx_fuse_chain_t *ch,
                            const float *x_in,
                            float       *y_out) {
    if (!ch || !x_in || !y_out) return -1;

    const float *src = x_in;
    float       *dst;

    for (size_t i = 0; i < ch->n_stages; i++) {
        /* Last stage writes directly to caller's y_out — no copy */
        if (i == ch->n_stages - 1) {
            dst = y_out;
        } else {
            /* Pingpong: even stages → scratch_a, odd → scratch_b */
            dst = (i & 1) ? ch->scratch_b : ch->scratch_a;
        }

        /* Zero the destination before accumulating */
        size_t rows = ch->stages[i]->rows;
        for (size_t j = 0; j < rows; j++) dst[j] = 0.0f;

        int rc = fpqx_sli_matvec(ch->stages[i], src, dst);
        if (rc != 0) return rc;

        src = dst; /* next stage reads this stage's output */
    }
    return 0;
}

/*
 * fpqx_fuse_chain_free — release all resources.
 */
void fpqx_fuse_chain_free(fpqx_fuse_chain_t *ch) {
    if (!ch) return;
    for (size_t i = 0; i < ch->n_stages; i++) fpqx_sli_free(ch->stages[i]);
    free(ch->stages);
    free(ch->scratch_a);
    free(ch->scratch_b);
    free(ch);
}

/*
 * fpqx_fuse_chain_get_stage — return the prepared SLI context for stage i.
 * Returns NULL if ch is NULL or i is out of range.
 * Useful for benchmarks that want to compare fused vs. staged (unfused) using
 * exactly the same SLI contexts, without re-encoding the tensor.
 */
fpqx_sli_ctx_t *fpqx_fuse_chain_get_stage(fpqx_fuse_chain_t *ch, int i) {
    if (!ch || i < 0 || (size_t)i >= ch->n_stages) return NULL;
    return ch->stages[i];
}
