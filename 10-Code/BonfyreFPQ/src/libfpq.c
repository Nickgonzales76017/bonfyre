/*
 * libfpq.c — BonfyreFPQ Library Implementation
 *
 * Wires fpq_native_read_compressed() → fpqx_sli_prepare/matvec
 * into the 5-function public API defined in libfpq.h.
 */
#include "libfpq.h"
#include "fpq.h"
#include "fpqx.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* ═══════════════════════════════════════════════════════════════════
 * Internal model structure
 * ═══════════════════════════════════════════════════════════════════ */

/* One loaded tensor (SLI-capable or passthrough) */
typedef struct {
    fpq_tensor_info_t  info;        /* public metadata */
    fpq_tensor_t      *compressed;  /* compressed v9 data (owned) */
    fpqx_tensor_t     *fpqx;       /* FPQ-X wrapper for SLI (owned) */
    fpqx_sli_ctx_t    *sli;        /* SLI context (owned, NULL for passthrough) */
    float             *passthrough; /* raw fp32 for small tensors (borrowed from compressed) */
} fpq_loaded_tensor_t;

struct fpq_model {
    char                 *path;
    size_t                n_tensors;
    fpq_loaded_tensor_t  *tensors;
    fpq_info_t            cached_info;
};


/* ═══════════════════════════════════════════════════════════════════
 * fpq_open — Load .fpq, build SLI contexts
 * ═══════════════════════════════════════════════════════════════════ */

fpq_model_t *fpq_open(const char *path) {
    if (!path) return NULL;

    /* Read compressed tensors */
    size_t n_tensors = 0;
    fpq_tensor_t **raw = fpq_native_read_compressed(path, &n_tensors);
    if (!raw || n_tensors == 0) {
        fprintf(stderr, "fpq_open: failed to load %s\n", path);
        return NULL;
    }

    fpq_model_t *m = (fpq_model_t *)calloc(1, sizeof(fpq_model_t));
    m->path = strdup(path);
    m->n_tensors = n_tensors;
    m->tensors = (fpq_loaded_tensor_t *)calloc(n_tensors, sizeof(fpq_loaded_tensor_t));

    size_t n_sli = 0, n_pass = 0;
    size_t total_params = 0;

    for (size_t i = 0; i < n_tensors; i++) {
        fpq_loaded_tensor_t *lt = &m->tensors[i];
        fpq_tensor_t *t = raw[i];
        lt->compressed = t;

        size_t rows = t->original_rows;
        size_t cols = t->original_cols;

        /* Fill public info */
        lt->info.name = t->name;
        lt->info.rows = rows;
        lt->info.cols = cols;
        total_params += rows * cols;

        if (t->n_blocks == 0) {
            /* Passthrough tensor (small/1D) */
            lt->info.has_sli = 0;
            lt->info.bpw = 16.0f; /* stored as fp16 */
            lt->passthrough = t->coord_scales; /* borrowed pointer */
            n_pass++;
        } else {
            /* SLI-capable weight matrix */
            lt->info.has_sli = 1;

            /* Wrap in fpqx_tensor_t for SLI */
            fpqx_tensor_t *fx = (fpqx_tensor_t *)calloc(1, sizeof(fpqx_tensor_t));
            strncpy(fx->name, t->name, sizeof(fx->name) - 1);
            fx->rows = rows;
            fx->cols = cols;
            fx->active_ops = FPQX_OP_ADDITIVE;
            fx->additive = t;

            lt->fpqx = fx;

            /* Build SLI context */
            lt->sli = fpqx_sli_prepare(fx);
            if (!lt->sli) {
                fprintf(stderr, "fpq_open: SLI prepare failed for %s\n", t->name);
                lt->info.has_sli = 0;
                n_pass++;
            } else {
                n_sli++;
            }

            /* Approximate bpw from file data */
            size_t n = rows * cols;
            if (n > 0) {
                /* Size estimate: LR(INT8) + E8(~7bit) + tile(6bit) + QJL(64bit) + meta */
                size_t lr_bits = (rows + cols) * (size_t)(int)t->sbb_scale_delta[0] * 8;
                size_t block_bits = t->n_blocks * (size_t)(7 * 256 + 6 * 16 + 64);
                lt->info.bpw = (float)(lr_bits + block_bits) / (float)n;
            }
        }
    }

    m->cached_info.n_tensors = n_tensors;
    m->cached_info.n_sli_tensors = n_sli;
    m->cached_info.n_passthrough = n_pass;
    m->cached_info.total_params = total_params;
    m->cached_info.format_version = 12;

    free(raw); /* free the pointer array, not the contents (owned by tensors) */

    fprintf(stderr, "fpq_open: %s — %zu tensors (%zu SLI, %zu passthrough), %zuM params\n",
            path, n_tensors, n_sli, n_pass, total_params / 1000000);

    return m;
}


/* ═══════════════════════════════════════════════════════════════════
 * fpq_matmul — y = W @ x via SLI
 * ═══════════════════════════════════════════════════════════════════ */

/* Simple hash lookup — linear scan (fine for <1000 tensors) */
static fpq_loaded_tensor_t *find_tensor(fpq_model_t *m, const char *name) {
    for (size_t i = 0; i < m->n_tensors; i++) {
        if (strcmp(m->tensors[i].info.name, name) == 0)
            return &m->tensors[i];
    }
    return NULL;
}

int fpq_matmul(fpq_model_t *m, const char *tensor_name,
               const float *x, float *y) {
    if (!m || !tensor_name || !x || !y) return -1;

    fpq_loaded_tensor_t *lt = find_tensor(m, tensor_name);
    if (!lt) {
        fprintf(stderr, "fpq_matmul: tensor '%s' not found\n", tensor_name);
        return -1;
    }

    size_t rows = lt->info.rows, cols = lt->info.cols;

    /* Fast path: SLI-capable tensor with dimensions large enough.
     * Set FPQ_DENSE_FALLBACK=1 to bypass SLI and use full decode (for testing). */
    if (lt->sli && rows >= 256 && cols >= 256 && !getenv("FPQ_DENSE_FALLBACK")) {
        return fpqx_sli_matvec(lt->sli, x, y);
    }

    /* Dense fallback for: passthrough tensors, small tensors, or SLI-prep failures.
     * Uses passthrough fp32 data if available, otherwise decodes from compressed. */
    if (lt->passthrough) {
        for (size_t r = 0; r < rows; r++) {
            float sum = 0.0f;
            const float *w = lt->passthrough + r * cols;
            for (size_t c = 0; c < cols; c++) sum += w[c] * x[c];
            y[r] = sum;
        }
        return 0;
    }

    if (lt->compressed) {
        float *w = (float *)malloc(rows * cols * sizeof(float));
        if (!w) return -1;
        if (lt->compressed->pid_alpha == -9.0f)
            fpq_decode_tensor_v9(lt->compressed, w);
        else if (lt->compressed->sbb_scale_delta)
            fpq_decode_tensor_v8(lt->compressed, w);
        else
            fpq_decode_tensor_v4(lt->compressed, w);
        for (size_t r = 0; r < rows; r++) {
            float sum = 0.0f;
            for (size_t c = 0; c < cols; c++)
                sum += w[r * cols + c] * x[c];
            y[r] = sum;
        }
        free(w);
        return 0;
    }

    fprintf(stderr, "fpq_matmul: tensor '%s' — no SLI, passthrough, or compressed data\n",
            tensor_name);
    return -1;
}


/* ═══════════════════════════════════════════════════════════════════
 * fpq_close — Free everything
 * ═══════════════════════════════════════════════════════════════════ */

void fpq_close(fpq_model_t *m) {
    if (!m) return;

    for (size_t i = 0; i < m->n_tensors; i++) {
        fpq_loaded_tensor_t *lt = &m->tensors[i];
        if (lt->sli) fpqx_sli_free(lt->sli);
        if (lt->fpqx) {
            lt->fpqx->additive = NULL; /* don't double-free */
            free(lt->fpqx);
        }
        if (lt->compressed) fpq_tensor_free(lt->compressed);
    }

    free(m->tensors);
    free(m->path);
    free(m);
}


/* ═══════════════════════════════════════════════════════════════════
 * fpq_decode_one — Decode single tensor to FP32
 * ═══════════════════════════════════════════════════════════════════ */

/* Decode an SLI-prepped tensor back to spatial domain using z vectors.
 * After fpqx_sli_prepare(), the E8 region stores z = H_raw(s⊙W)/N so
 * fpq_decode_tensor_v9 would read corrupted data.  Recover via:
 *   W_block ≈ s ⊙ IFWHT(z_stored × sqrt(N))
 * and add the LR component. */
static int fpq_decode_one_from_z(fpq_loaded_tensor_t *lt, float *out) {
    fpq_tensor_t    *t   = lt->compressed;
    fpqx_sli_ctx_t  *ctx = lt->sli;
    size_t rows   = lt->info.rows;
    size_t cols   = lt->info.cols;
    size_t n_blk  = t->n_blocks;
    size_t bpr    = ctx->blocks_per_row;  /* blocks per row = ceil(cols/256) */
    int    lr_rank = ctx->lr_rank;
    size_t us_off  = ctx->us_off;
    size_t vt_off  = ctx->vt_off;

    /* Step 1: Reconstruct LR base into output (LR region not corrupted) */
    for (size_t i = 0; i < rows; i++) {
        for (size_t j = 0; j < cols; j++) {
            double val = 0.0;
            for (int r = 0; r < lr_rank; r++)
                val += (double)t->sbb_scale_delta[us_off + i * (size_t)lr_rank + r] *
                       (double)t->sbb_scale_delta[vt_off + r * cols + j];
            out[i * cols + j] = (float)val;
        }
    }

    /* Step 2: Recover spatial residuals from z vectors.
     * Legacy layout: z_stored = z' where inference used z'^T·FWHT_raw(s⊙x)
     *   Recovery: z' * sqrt(N) -> IFWHT -> undo signs.
     *
     * New layout (perf path): z_stored = FWHT_raw(z') pre-applied at prepare.
     *   Recovery simplifies to: undo signs directly.
     */
    float sqrt_n = sqrtf((float)256);  /* sqrt(SLI_BLOCK_DIM) = 16 */

    for (size_t b = 0; b < n_blk; b++) {
        size_t row       = b / bpr;
        size_t block_col = b % bpr;
        size_t col_off   = block_col * 256;
        size_t col_end   = col_off + 256;
        if (col_end > cols) col_end = cols;
        size_t this_dim  = col_end - col_off;

        const float *z_stored = ctx->z_data + b * 256;

        float block_data[256];
        if (ctx->z_fwht_preapplied) {
            memcpy(block_data, z_stored, 256 * sizeof(float));
        } else {
            for (int i = 0; i < 256; i++)
                block_data[i] = z_stored[i] * sqrt_n;  /* undo 1/sqrt(N) folding */
            fpq_fwht_inverse(block_data, 256);  /* IFWHT → s⊙W_b */
        }

        fpq_random_signs_inverse(block_data, 256,
                                 t->haar_seed ^ (uint64_t)b); /* undo signs → W_b */

        for (size_t i = 0; i < this_dim; i++)
            out[row * cols + col_off + i] += block_data[i];
    }

    return 0;
}

int fpq_decode_one(fpq_model_t *m, const char *tensor_name, float *out) {
    if (!m || !tensor_name || !out) return -1;

    fpq_loaded_tensor_t *lt = find_tensor(m, tensor_name);
    if (!lt) return -1;

    size_t n = lt->info.rows * lt->info.cols;

    /* Fast path: passthrough (1D norms, small tensors) */
    if (!lt->info.has_sli && lt->passthrough) {
        memcpy(out, lt->passthrough, n * sizeof(float));
        return 0;
    }

    /* SLI tensor: E8 region was overwritten by fpqx_sli_prepare.
     * Recover spatial weights from the precomputed z vectors. */
    if (lt->sli && lt->sli->z_precomputed) {
        return fpq_decode_one_from_z(lt, out);
    }

    /* Full block-by-block decode via raw compressed tensor — dispatch by version */
    if (lt->compressed) {
        if (lt->compressed->pid_alpha == -9.0f)
            fpq_decode_tensor_v9(lt->compressed, out);
        else if (lt->compressed->sbb_scale_delta)
            fpq_decode_tensor_v8(lt->compressed, out);
        else
            fpq_decode_tensor_v4(lt->compressed, out);
        return 0;
    }

    fprintf(stderr, "fpq_decode_one: '%s' — no data available\n", tensor_name);
    return -1;
}


/* ═══════════════════════════════════════════════════════════════════
 * fpq_decode_all — Decode to safetensors (uses fpq_native_read)
 * ═══════════════════════════════════════════════════════════════════ */

int fpq_decode_all(fpq_model_t *m, const char *out_path) {
    if (!m || !out_path) return -1;

    /* Use the standard decoder to get fp32 tensors */
    size_t n_tensors = 0;
    fpq_raw_tensor_t *raw = fpq_native_read(m->path, &n_tensors);
    if (!raw) return -1;

    /* Write as safetensors */
    int rc = fpq_safetensors_write(out_path, raw, n_tensors);

    for (size_t i = 0; i < n_tensors; i++)
        free(raw[i].data);
    free(raw);

    return rc;
}


/* ═══════════════════════════════════════════════════════════════════
 * Query functions
 * ═══════════════════════════════════════════════════════════════════ */

fpq_info_t fpq_info(fpq_model_t *m) {
    if (!m) {
        fpq_info_t empty = {0};
        return empty;
    }
    return m->cached_info;
}

const fpq_tensor_info_t *fpq_tensor_at(fpq_model_t *m, size_t index) {
    if (!m || index >= m->n_tensors) return NULL;
    return &m->tensors[index].info;
}

const fpq_tensor_info_t *fpq_tensor_find(fpq_model_t *m, const char *name) {
    fpq_loaded_tensor_t *lt = find_tensor(m, name);
    return lt ? &lt->info : NULL;
}

const float *fpq_get_passthrough(fpq_model_t *m, const char *tensor_name) {
    if (!m || !tensor_name) return NULL;
    fpq_loaded_tensor_t *lt = find_tensor(m, tensor_name);
    if (!lt || lt->info.has_sli) return NULL;
    return lt->passthrough;
}
