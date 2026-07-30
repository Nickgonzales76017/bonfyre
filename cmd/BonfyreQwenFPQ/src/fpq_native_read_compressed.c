#include "fpq.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct __attribute__((packed)) {
    uint16_t name_len;
    uint32_t rows;
    uint32_t cols;
    uint16_t lr_rank;
    uint8_t coord_bits;
    uint8_t has_ghost;
    uint32_t n_blocks;
    uint16_t effective_k;
    uint64_t data_offset;
    uint64_t data_size;
} fpq_native_tensor_header_local_t;

typedef struct __attribute__((packed)) {
    uint16_t name_len;
    uint32_t rows;
    uint32_t cols;
    uint16_t lr_rank;
    uint8_t coord_bits;
    uint8_t has_ghost;
    uint16_t n_blocks;
    uint16_t effective_k;
    uint64_t data_offset;
    uint64_t data_size;
} fpq_native_tensor_header_v10_local_t;

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t n_tensors;
    uint32_t flags;
    uint64_t tensor_table_offset;
} fpq_native_file_header_local_t;

static float native_fp16_to_float(uint16_t h) {
    uint32_t sign = ((uint32_t)h & 0x8000) << 16;
    uint32_t exp = (h >> 10) & 0x1F;
    uint32_t frac = h & 0x03FF;

    if (exp == 0) {
        if (frac == 0) {
            union { uint32_t u; float f; } v = { .u = sign };
            return v.f;
        }
        int32_t e = -14;
        while (!(frac & 0x0400)) {
            frac <<= 1;
            e--;
        }
        frac &= ~0x0400;
        union { uint32_t u; float f; } v = {
            .u = sign | (uint32_t)((e + 127) << 23) | (frac << 13)
        };
        return v.f;
    } else if (exp == 31) {
        union { uint32_t u; float f; } v = { .u = sign | 0x7F800000 | (frac << 13) };
        return v.f;
    }

    uint32_t result = sign | ((exp + 112) << 23) | (frac << 13);
    union { uint32_t u; float f; } v = { .u = result };
    return v.f;
}

static int read_exact(FILE *fp, void *dst, size_t n) {
    return fread(dst, 1, n, fp) == n ? 0 : -1;
}

fpq_tensor_t **fpq_native_read_compressed(const char *path, size_t *n_tensors) {
    FILE *fp = NULL;
    fpq_native_file_header_local_t fh;
    fpq_native_tensor_header_local_t *headers = NULL;
    char **names = NULL;
    fpq_tensor_t **out = NULL;
    uint32_t nt, i;

    if (n_tensors) *n_tensors = 0;
    fp = fopen(path, "rb");
    if (!fp) return NULL;

    if (read_exact(fp, &fh, sizeof(fh)) != 0 || fh.magic != FPQ_NATIVE_MAGIC) {
        fclose(fp);
        return NULL;
    }
    if (fh.version > FPQ_NATIVE_VERSION) {
        fclose(fp);
        return NULL;
    }

    nt = fh.n_tensors;
    headers = (fpq_native_tensor_header_local_t *)calloc(nt, sizeof(*headers));
    names = (char **)calloc(nt, sizeof(char *));
    out = (fpq_tensor_t **)calloc(nt, sizeof(fpq_tensor_t *));
    if (!headers || !names || !out) goto fail;

    for (i = 0; i < nt; i++) {
        if (fh.version <= 10) {
            fpq_native_tensor_header_v10_local_t oldh;
            if (read_exact(fp, &oldh, sizeof(oldh)) != 0) goto fail;
            headers[i].name_len = oldh.name_len;
            headers[i].rows = oldh.rows;
            headers[i].cols = oldh.cols;
            headers[i].lr_rank = oldh.lr_rank;
            headers[i].coord_bits = oldh.coord_bits;
            headers[i].has_ghost = oldh.has_ghost;
            headers[i].n_blocks = oldh.n_blocks;
            headers[i].effective_k = oldh.effective_k;
            headers[i].data_offset = oldh.data_offset;
            headers[i].data_size = oldh.data_size;
        } else {
            if (read_exact(fp, &headers[i], sizeof(headers[i])) != 0) goto fail;
        }
        names[i] = (char *)calloc((size_t)headers[i].name_len + 1, 1);
        if (!names[i]) goto fail;
        if (read_exact(fp, names[i], headers[i].name_len) != 0) goto fail;
    }

    for (i = 0; i < nt; i++) {
        fpq_native_tensor_header_local_t *h = &headers[i];
        size_t rows = h->rows, cols = h->cols;
        size_t n = rows * cols;
        size_t n_blocks = h->n_blocks;
        size_t expected_n_blocks = (n + FPQ_BLOCK_DIM - 1) / FPQ_BLOCK_DIM;
        fpq_tensor_t *t = (fpq_tensor_t *)calloc(1, sizeof(fpq_tensor_t));
        if (!t) goto fail;
        out[i] = t;

        if (h->lr_rank != 0 && h->coord_bits != 0 && n_blocks != expected_n_blocks) {
            fprintf(stderr,
                    "fpq_native_read_compressed: correcting legacy n_blocks for %s rows=%zu cols=%zu stored=%zu expected=%zu version=%u\n",
                    names[i], rows, cols, n_blocks, expected_n_blocks, fh.version);
            n_blocks = expected_n_blocks;
        }

        strncpy(t->name, names[i], sizeof(t->name) - 1);
        t->original_rows = rows;
        t->original_cols = cols;
        t->n_blocks = n_blocks;
        t->coord_bits = h->coord_bits;
        t->sbb_group_id = -1;
        t->haar_seed = 0;

        if (fseek(fp, (long)h->data_offset, SEEK_SET) != 0) goto fail;

        if (h->lr_rank == 0 && h->coord_bits == 0) {
            size_t j;
            t->coord_scales = (float *)calloc(n ? n : 1, sizeof(float));
            if (!t->coord_scales) goto fail;
            for (j = 0; j < n; j++) {
                uint16_t hv;
                if (read_exact(fp, &hv, sizeof(hv)) != 0) goto fail;
                t->coord_scales[j] = native_fp16_to_float(hv);
            }
            continue;
        }

        {
            int lr_rank = h->lr_rank;
            int effective_k = h->effective_k > 0 ? h->effective_k : 256;
            size_t lr_us_size = rows * (size_t)lr_rank;
            size_t lr_vt_size = (size_t)lr_rank * cols;
            size_t padded = FPQ_BLOCK_DIM;
            size_t v8_base = 2 + lr_us_size + lr_vt_size;
            size_t e8_off = v8_base + n_blocks;
            size_t e8_flat_size = n_blocks * padded;
            size_t tile_cb_off = e8_off + e8_flat_size;
            size_t tile_cb_size = (size_t)effective_k * 16;
            size_t tile_idx_off = tile_cb_off + tile_cb_size;
            size_t tile_idx_size = n_blocks * 16;
            size_t sbb_total = tile_idx_off + tile_idx_size + 1;
            size_t b;

            t->pid_alpha = -9.0f;
            t->coord_scales = (float *)calloc(n_blocks ? n_blocks : 1, sizeof(float));
            t->coord_residual_norms = (float *)calloc(n_blocks ? n_blocks : 1, sizeof(float));
            t->qjl = (fpq_qjl_t **)calloc(n_blocks ? n_blocks : 1, sizeof(fpq_qjl_t *));
            t->sbb_scale_delta = (float *)calloc(sbb_total, sizeof(float));
            if (!t->coord_scales || !t->coord_residual_norms || !t->qjl || !t->sbb_scale_delta) goto fail;

            t->sbb_scale_delta[0] = (float)lr_rank;
            t->sbb_scale_delta[1] = (float)lr_rank;

            for (int r = 0; r < lr_rank; r++) {
                uint16_t scale_h;
                float sc;
                if (read_exact(fp, &scale_h, sizeof(scale_h)) != 0) goto fail;
                sc = native_fp16_to_float(scale_h);
                for (size_t row = 0; row < rows; row++) {
                    int8_t qb;
                    if (read_exact(fp, &qb, sizeof(qb)) != 0) goto fail;
                    t->sbb_scale_delta[2 + row * (size_t)lr_rank + (size_t)r] = (float)qb * sc;
                }
            }

            for (int r = 0; r < lr_rank; r++) {
                uint16_t scale_h;
                float sc;
                if (read_exact(fp, &scale_h, sizeof(scale_h)) != 0) goto fail;
                sc = native_fp16_to_float(scale_h);
                for (size_t col = 0; col < cols; col++) {
                    int8_t qb;
                    if (read_exact(fp, &qb, sizeof(qb)) != 0) goto fail;
                    t->sbb_scale_delta[2 + lr_us_size + (size_t)r * cols + col] = (float)qb * sc;
                }
            }

            for (b = 0; b < n_blocks; b++) {
                uint16_t hv;
                if (read_exact(fp, &hv, sizeof(hv)) != 0) goto fail;
                t->coord_scales[b] = native_fp16_to_float(hv);
            }
            for (b = 0; b < n_blocks; b++) {
                uint16_t hv;
                if (read_exact(fp, &hv, sizeof(hv)) != 0) goto fail;
                t->sbb_scale_delta[v8_base + b] = native_fp16_to_float(hv);
            }
            {
                uint16_t rn_scale_h;
                float rn_sc;
                if (read_exact(fp, &rn_scale_h, sizeof(rn_scale_h)) != 0) goto fail;
                rn_sc = native_fp16_to_float(rn_scale_h);
                for (b = 0; b < n_blocks; b++) {
                    uint8_t qb;
                    if (read_exact(fp, &qb, sizeof(qb)) != 0) goto fail;
                    t->coord_residual_norms[b] = (float)qb * rn_sc;
                }
            }

            for (b = 0; b < e8_flat_size; b++) {
                int8_t v;
                if (read_exact(fp, &v, sizeof(v)) != 0) goto fail;
                t->sbb_scale_delta[e8_off + b] = (float)v;
            }
            for (b = 0; b < tile_cb_size; b++) {
                uint16_t hv;
                if (read_exact(fp, &hv, sizeof(hv)) != 0) goto fail;
                t->sbb_scale_delta[tile_cb_off + b] = native_fp16_to_float(hv);
            }
            for (b = 0; b < tile_idx_size; b++) {
                uint8_t idx;
                if (read_exact(fp, &idx, sizeof(idx)) != 0) goto fail;
                t->sbb_scale_delta[tile_idx_off + b] = (float)idx;
            }
            t->sbb_scale_delta[tile_idx_off + tile_idx_size] = (float)effective_k;

            for (b = 0; b < n_blocks; b++) {
                uint64_t bits_word;
                fpq_qjl_t *qjl = (fpq_qjl_t *)calloc(1, sizeof(fpq_qjl_t));
                if (!qjl) goto fail;
                qjl->bits = (uint64_t *)calloc(1, sizeof(uint64_t));
                if (!qjl->bits) goto fail;
                if (read_exact(fp, &bits_word, sizeof(bits_word)) != 0) goto fail;
                qjl->bits[0] = bits_word;
                qjl->n_projections = FPQ_QJL_PROJECTIONS;
                qjl->n_elements = padded;
                qjl->proj_seed = t->haar_seed ^ (uint64_t)b ^ 0xC00DULL;
                t->qjl[b] = qjl;
            }

            if (h->has_ghost) {
                uint16_t sigma_h, u_scale_h, v_scale_h;
                float u_sc, v_sc;
                t->ghost = (fpq_ghost_t *)calloc(1, sizeof(fpq_ghost_t));
                if (!t->ghost) goto fail;
                t->ghost->rows = rows;
                t->ghost->cols = cols;
                t->ghost->u = (float *)calloc(rows ? rows : 1, sizeof(float));
                t->ghost->v = (float *)calloc(cols ? cols : 1, sizeof(float));
                if (!t->ghost->u || !t->ghost->v) goto fail;
                if (read_exact(fp, &sigma_h, sizeof(sigma_h)) != 0) goto fail;
                if (read_exact(fp, &u_scale_h, sizeof(u_scale_h)) != 0) goto fail;
                t->ghost->sigma = native_fp16_to_float(sigma_h);
                u_sc = native_fp16_to_float(u_scale_h);
                for (size_t row = 0; row < rows; row++) {
                    int8_t qb;
                    if (read_exact(fp, &qb, sizeof(qb)) != 0) goto fail;
                    t->ghost->u[row] = (float)qb * u_sc;
                }
                if (read_exact(fp, &v_scale_h, sizeof(v_scale_h)) != 0) goto fail;
                v_sc = native_fp16_to_float(v_scale_h);
                for (size_t col = 0; col < cols; col++) {
                    int8_t qb;
                    if (read_exact(fp, &qb, sizeof(qb)) != 0) goto fail;
                    t->ghost->v[col] = (float)qb * v_sc;
                }
            }

            if (read_exact(fp, &t->haar_seed, sizeof(t->haar_seed)) != 0) goto fail;
            for (b = 0; b < n_blocks; b++) {
                if (t->qjl[b]) t->qjl[b]->proj_seed = t->haar_seed ^ (uint64_t)b ^ 0xC00DULL;
            }
        }
    }

    for (i = 0; i < nt; i++) free(names[i]);
    free(names);
    free(headers);
    fclose(fp);
    if (n_tensors) *n_tensors = nt;
    fprintf(stderr, "fpq_native_read_compressed: loaded %u tensors from %s\n", nt, path);
    return out;

fail:
    if (out) {
        for (i = 0; i < nt; i++) {
            if (out[i]) fpq_tensor_free(out[i]);
        }
    }
    free(out);
    if (names) {
        for (i = 0; i < nt; i++) free(names[i]);
    }
    free(names);
    free(headers);
    if (fp) fclose(fp);
    if (n_tensors) *n_tensors = 0;
    return NULL;
}
