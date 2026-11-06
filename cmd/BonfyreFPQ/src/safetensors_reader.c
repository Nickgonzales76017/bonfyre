/*
 * safetensors_reader.c — Read HuggingFace safetensors model files
 *
 * Safetensors format:
 *   [8 bytes]  header_size (little-endian uint64)
 *   [header_size bytes]  JSON header with tensor metadata
 *   [remaining]  raw tensor data (contiguous, aligned)
 *
 * JSON header maps tensor names to:
 *   { "dtype": "BF16"|"F16"|"F32", "shape": [d0, d1, ...],
 *     "data_offsets": [start, end] }
 *
 * We read all tensors as fp32 (converting bf16/fp16 as needed).
 * Supports sharded files: pass a directory containing multiple
 * .safetensors files and all are concatenated.
 *
 * Minimal JSON parser included (no external dependencies).
 */
#include "fpq.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <dirent.h>
#include <sys/stat.h>

#include "bf_lex.h"
/* ── bf16 → fp32 conversion ── */
static float bf16_to_fp32(uint16_t h) {
    union { uint32_t u; float f; } v;
    v.u = (uint32_t)h << 16;
    return v.f;
}

/* ── fp8 e4m3 → fp32 conversion ── */
/* FP8 E4M3FN: 1 sign + 4 exponent (bias=7) + 3 mantissa, no inf, NaN=0x7F/0xFF */
static float fp8_e4m3fn_to_fp32(uint8_t v) {
    uint32_t sign = (uint32_t)(v >> 7) << 31;
    uint32_t exp  = (v >> 3) & 0xF;
    uint32_t mant = v & 0x7;

    if (exp == 0xF && mant == 0x7) {
        /* NaN */
        union { uint32_t u; float f; } u = { sign | 0x7FC00000u };
        return u.f;
    }
    if (exp == 0) {
        if (mant == 0) {
            union { uint32_t u; float f; } u = { sign };
            return u.f;
        }
        /* Subnormal: value = (-1)^s * 2^(-6) * (0.mant) = (-1)^s * mant * 2^(-9) */
        float val = (float)mant / 512.0f;  /* mant * 2^-9 */
        union { uint32_t u; float f; } u;
        u.f = val;
        u.u |= sign;
        return u.f;
    }
    /* Normal: value = (-1)^s * 2^(exp-7) * (1 + mant/8) */
    /* Map to FP32: exp32 = exp - 7 + 127 = exp + 120, mant32 = mant << 20 */
    uint32_t exp32 = exp + 120;
    uint32_t mant32 = mant << 20;
    union { uint32_t u; float f; } u = { sign | (exp32 << 23) | mant32 };
    return u.f;
}

/* ── fp16 → fp32 conversion ── */
static float fp16_to_fp32_st(uint16_t h) {
    uint32_t sign = (uint32_t)(h >> 15) << 31;
    uint32_t exp  = (h >> 10) & 0x1F;
    uint32_t mant = h & 0x3FF;

    if (exp == 0) {
        if (mant == 0) {
            union { uint32_t u; float f; } u = { sign };
            return u.f;
        }
        /* Denormalized → normalize */
        while (!(mant & 0x400)) { mant <<= 1; exp--; }
        exp++; mant &= ~0x400;
        union { uint32_t u; float f; } u = {
            sign | ((exp + 112) << 23) | (mant << 13)
        };
        return u.f;
    }
    if (exp == 31) {
        union { uint32_t u; float f; } u = {
            sign | 0x7F800000u | (mant << 13)
        };
        return u.f;
    }
    union { uint32_t u; float f; } u = {
        sign | ((exp + 112) << 23) | (mant << 13)
    };
    return u.f;
}

/* ── Minimal JSON tokenizer for safetensors header ── */

typedef struct {
    char name[256];
    char dtype[16];     /* "F32", "F16", "BF16", "F64", "I32", "I64", etc. */
    size_t shape[8];
    int n_dims;
    size_t data_start;
    size_t data_end;
} st_tensor_meta_t;

/* Skip whitespace */
static const char *skip_ws(const char *p) {
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    return p;
}

/* Parse a JSON string (between quotes), return pointer past closing quote */
/* Delegates to the one implementation. This copy sent \\t, \\r and \\uXXXX
 * through its default branch, so a tab arrived as the letter t and an escaped
 * codepoint as its four hex digits -- the same defect libbonfyre's reader had,
 * written independently. */
static const char *parse_json_string(const char *p, char *out, size_t max) {
    const char *end;
    if (!p || *p != '"') return NULL;
    bf_lex_json_string_at(p + 1, out, max, &end);
    return end;
}

/* Parse a JSON integer */
static const char *parse_json_int(const char *p, size_t *out) {
    *out = 0;
    while (*p >= '0' && *p <= '9') {
        *out = *out * 10 + (size_t)(*p - '0');
        p++;
    }
    return p;
}

/*
 * Parse the safetensors JSON header.
 * Returns array of st_tensor_meta_t, sets *n_out.
 * Caller must free the result.
 */
static st_tensor_meta_t *parse_st_header(const char *json, size_t json_len,
                                          int *n_out) {
    int capacity = 256;
    int count = 0;
    st_tensor_meta_t *metas = (st_tensor_meta_t *)calloc(
        (size_t)capacity, sizeof(st_tensor_meta_t));

    const char *p = json;
    const char *end = json + json_len;

    p = skip_ws(p);
    if (*p != '{') { *n_out = 0; return metas; }
    p++;

    while (p < end) {
        p = skip_ws(p);
        if (*p == '}') break;
        if (*p == ',') { p++; continue; }

        /* Parse tensor name (key) */
        char name[256] = {0};
        p = parse_json_string(p, name, sizeof(name));
        if (!p) break;

        p = skip_ws(p);
        if (*p != ':') break;
        p++;
        p = skip_ws(p);

        /* Skip __metadata__ key */
        if (strcmp(name, "__metadata__") == 0) {
            /* Skip the value (could be object or simple) */
            int depth = 0;
            if (*p == '{') {
                depth = 1; p++;
                while (p < end && depth > 0) {
                    if (*p == '{') depth++;
                    else if (*p == '}') depth--;
                    else if (*p == '"') {
                        p++;
                        while (p < end && *p != '"') {
                            if (*p == '\\') p++;
                            p++;
                        }
                    }
                    p++;
                }
            }
            continue;
        }

        /* Parse value object: { "dtype": ..., "shape": [...], "data_offsets": [...] } */
        if (*p != '{') break;
        p++;

        st_tensor_meta_t m = {0};
        strncpy(m.name, name, sizeof(m.name) - 1);

        while (p < end) {
            p = skip_ws(p);
            if (*p == '}') { p++; break; }
            if (*p == ',') { p++; continue; }

            char key[64] = {0};
            p = parse_json_string(p, key, sizeof(key));
            if (!p) break;
            p = skip_ws(p);
            if (*p != ':') break;
            p++;
            p = skip_ws(p);

            if (strcmp(key, "dtype") == 0) {
                p = parse_json_string(p, m.dtype, sizeof(m.dtype));
                if (!p) break;
            } else if (strcmp(key, "shape") == 0) {
                if (*p != '[') break;
                p++;
                m.n_dims = 0;
                while (p < end && *p != ']') {
                    p = skip_ws(p);
                    if (*p == ',') { p++; continue; }
                    if (*p >= '0' && *p <= '9') {
                        size_t val;
                        p = parse_json_int(p, &val);
                        if (m.n_dims < 8) m.shape[m.n_dims++] = val;
                    } else break;
                }
                if (*p == ']') p++;
            } else if (strcmp(key, "data_offsets") == 0) {
                if (*p != '[') break;
                p++;
                p = skip_ws(p);
                p = parse_json_int(p, &m.data_start);
                p = skip_ws(p);
                if (*p == ',') p++;
                p = skip_ws(p);
                p = parse_json_int(p, &m.data_end);
                p = skip_ws(p);
                if (*p == ']') p++;
            } else {
                /* Skip unknown value */
                if (*p == '"') {
                    char tmp[512];
                    p = parse_json_string(p, tmp, sizeof(tmp));
                } else if (*p == '[') {
                    int d = 1; p++;
                    while (p < end && d > 0) {
                        if (*p == '[') d++; else if (*p == ']') d--;
                        p++;
                    }
                } else if (*p == '{') {
                    int d = 1; p++;
                    while (p < end && d > 0) {
                        if (*p == '{') d++; else if (*p == '}') d--;
                        p++;
                    }
                } else {
                    while (p < end && *p != ',' && *p != '}') p++;
                }
            }
        }

        if (count >= capacity) {
            capacity *= 2;
            metas = (st_tensor_meta_t *)realloc(
                metas, (size_t)capacity * sizeof(st_tensor_meta_t));
        }
        metas[count++] = m;
    }

    *n_out = count;
    return metas;
}

/* ── Read a single safetensors file ── */
static int read_single_safetensors(const char *path,
                                    fpq_raw_tensor_t **out_tensors,
                                    size_t *out_count,
                                    size_t *out_capacity) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "FPQ: Cannot open %s\n", path);
        return -1;
    }

    /* Read header size (8 bytes, little-endian) */
    uint64_t header_size;
    if (fread(&header_size, 8, 1, f) != 1) {
        fprintf(stderr, "FPQ: Failed to read header size from %s\n", path);
        fclose(f);
        return -1;
    }

    if (header_size > 100 * 1024 * 1024) {  /* sanity: 100 MB max header */
        fprintf(stderr, "FPQ: Header too large (%llu bytes) in %s\n",
                (unsigned long long)header_size, path);
        fclose(f);
        return -1;
    }

    /* Read JSON header */
    char *header = (char *)malloc((size_t)header_size + 1);
    if (fread(header, 1, (size_t)header_size, f) != (size_t)header_size) {
        fprintf(stderr, "FPQ: Failed to read header from %s\n", path);
        free(header);
        fclose(f);
        return -1;
    }
    header[header_size] = '\0';

    size_t data_base_offset = 8 + (size_t)header_size;

    /* Parse header */
    int n_metas = 0;
    st_tensor_meta_t *metas = parse_st_header(header, (size_t)header_size, &n_metas);
    free(header);

    fprintf(stderr, "FPQ: %s — %d tensors\n", path, n_metas);

    for (int t = 0; t < n_metas; t++) {
        st_tensor_meta_t *m = &metas[t];

        /* Compute element count */
        size_t n_elements = 1;
        for (int d = 0; d < m->n_dims; d++)
            n_elements *= m->shape[d];

        if (n_elements == 0) continue;

        /* Determine bytes per element */
        size_t bytes_per_el = 0;
        int is_bf16 = 0, is_fp16 = 0, is_fp32 = 0, is_fp8_e4m3 = 0;

        if (strcmp(m->dtype, "BF16") == 0) {
            bytes_per_el = 2; is_bf16 = 1;
        } else if (strcmp(m->dtype, "F16") == 0) {
            bytes_per_el = 2; is_fp16 = 1;
        } else if (strcmp(m->dtype, "F32") == 0) {
            bytes_per_el = 4; is_fp32 = 1;
        } else if (strcmp(m->dtype, "F8_E4M3") == 0) {
            bytes_per_el = 1; is_fp8_e4m3 = 1;
        } else {
            fprintf(stderr, "  [SKIP] %s: unsupported dtype %s\n",
                    m->name, m->dtype);
            continue;
        }

        size_t expected_bytes = n_elements * bytes_per_el;
        size_t actual_bytes = m->data_end - m->data_start;
        if (actual_bytes < expected_bytes) {
            fprintf(stderr, "  [SKIP] %s: data size mismatch (%zu < %zu)\n",
                    m->name, actual_bytes, expected_bytes);
            continue;
        }

        /* Seek to tensor data */
        if (fseek(f, (long)(data_base_offset + m->data_start), SEEK_SET) != 0) {
            fprintf(stderr, "  [SKIP] %s: seek failed\n", m->name);
            continue;
        }

        /* Grow output array if needed */
        if (*out_count >= *out_capacity) {
            *out_capacity = (*out_capacity == 0) ? 256 : *out_capacity * 2;
            *out_tensors = (fpq_raw_tensor_t *)realloc(
                *out_tensors, *out_capacity * sizeof(fpq_raw_tensor_t));
        }

        fpq_raw_tensor_t *rt = &(*out_tensors)[*out_count];
        memset(rt, 0, sizeof(*rt));
        strncpy(rt->name, m->name, sizeof(rt->name) - 1);
        rt->n_dims = (uint32_t)m->n_dims;
        rt->n_elements = n_elements;
        rt->ggml_type = 0;  /* we convert to fp32 */

        /* Interpret shape: safetensors uses [rows, cols] for 2D */
        if (m->n_dims >= 2) {
            rt->rows = m->shape[0];
            rt->cols = m->shape[1];
            /* For higher-dim tensors, flatten trailing dims into cols */
            for (int d = 2; d < m->n_dims; d++)
                rt->cols *= m->shape[d];
        } else if (m->n_dims == 1) {
            rt->rows = 1;
            rt->cols = m->shape[0];
        } else {
            rt->rows = 1;
            rt->cols = n_elements;
        }

        /* Read and convert to fp32 */
        rt->data = (float *)malloc(n_elements * sizeof(float));
        if (!rt->data) {
            fprintf(stderr, "  [SKIP] %s: malloc failed for %zu elements\n",
                    m->name, n_elements);
            continue;
        }

        if (is_fp32) {
            if (fread(rt->data, 4, n_elements, f) != n_elements) {
                fprintf(stderr, "  [SKIP] %s: read failed\n", m->name);
                free(rt->data); rt->data = NULL;
                continue;
            }
        } else if (is_fp8_e4m3) {
            /* fp8 e4m3 — read as uint8, convert to fp32 */
            size_t chunk_size = 131072;
            uint8_t *buf = (uint8_t *)malloc(chunk_size);
            size_t remaining = n_elements;
            size_t offset = 0;

            while (remaining > 0) {
                size_t batch = (remaining < chunk_size) ? remaining : chunk_size;
                if (fread(buf, 1, batch, f) != batch) {
                    fprintf(stderr, "  [SKIP] %s: read failed at offset %zu\n",
                            m->name, offset);
                    free(rt->data); rt->data = NULL;
                    break;
                }
                for (size_t i = 0; i < batch; i++)
                    rt->data[offset + i] = fp8_e4m3fn_to_fp32(buf[i]);
                offset += batch;
                remaining -= batch;
            }
            free(buf);

            if (!rt->data) continue;
        } else {
            /* bf16 or fp16 — read as uint16, convert */
            size_t chunk_size = 65536;
            uint16_t *buf = (uint16_t *)malloc(chunk_size * sizeof(uint16_t));
            size_t remaining = n_elements;
            size_t offset = 0;

            while (remaining > 0) {
                size_t batch = (remaining < chunk_size) ? remaining : chunk_size;
                if (fread(buf, 2, batch, f) != batch) {
                    fprintf(stderr, "  [SKIP] %s: read failed at offset %zu\n",
                            m->name, offset);
                    free(rt->data); rt->data = NULL;
                    break;
                }
                if (is_bf16) {
                    for (size_t i = 0; i < batch; i++)
                        rt->data[offset + i] = bf16_to_fp32(buf[i]);
                } else {
                    for (size_t i = 0; i < batch; i++)
                        rt->data[offset + i] = fp16_to_fp32_st(buf[i]);
                }
                offset += batch;
                remaining -= batch;
            }
            free(buf);

            if (!rt->data) continue;
        }

        (*out_count)++;
    }

    free(metas);
    fclose(f);
    return 0;
}

/* ── Compare strings for qsort ── */
static int cmp_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

/*
 * Public API: Read safetensors file(s).
 * `path` can be:
 *   - A single .safetensors file
 *   - A directory containing .safetensors files (sharded model)
 */
fpq_raw_tensor_t *fpq_safetensors_read(const char *path, size_t *n_tensors) {
    struct stat st;
    if (stat(path, &st) != 0) {
        fprintf(stderr, "FPQ: Cannot stat %s\n", path);
        *n_tensors = 0;
        return NULL;
    }

    fpq_raw_tensor_t *tensors = NULL;
    size_t count = 0;
    size_t capacity = 0;

    if (S_ISDIR(st.st_mode)) {
        /* Directory: read all .safetensors files */
        DIR *dir = opendir(path);
        if (!dir) {
            fprintf(stderr, "FPQ: Cannot open directory %s\n", path);
            *n_tensors = 0;
            return NULL;
        }

        /* Collect and sort filenames for deterministic ordering */
        char **files = NULL;
        int n_files = 0;
        int file_cap = 32;
        files = (char **)malloc((size_t)file_cap * sizeof(char *));

        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            size_t len = strlen(ent->d_name);
            if (len > 12 &&
                strcmp(ent->d_name + len - 12, ".safetensors") == 0) {
                if (n_files >= file_cap) {
                    file_cap *= 2;
                    files = (char **)realloc(files, (size_t)file_cap * sizeof(char *));
                }
                size_t plen = strlen(path) + 1 + len + 1;
                files[n_files] = (char *)malloc(plen);
                snprintf(files[n_files], plen, "%s/%s", path, ent->d_name);
                n_files++;
            }
        }
        closedir(dir);

        qsort(files, (size_t)n_files, sizeof(char *), cmp_strings);

        fprintf(stderr, "FPQ: Reading %d safetensors files from %s\n",
                n_files, path);

        for (int i = 0; i < n_files; i++) {
            read_single_safetensors(files[i], &tensors, &count, &capacity);
            free(files[i]);
        }
        free(files);
    } else {
        /* Single file */
        read_single_safetensors(path, &tensors, &count, &capacity);
    }

    /* Print summary */
    fprintf(stderr, "FPQ: Read %zu tensors total\n", count);
    for (size_t i = 0; i < count; i++) {
        const char *shape_str;
        char buf[64];
        if (tensors[i].n_dims >= 2) {
            snprintf(buf, sizeof(buf), "%zu×%zu",
                     tensors[i].rows, tensors[i].cols);
            shape_str = buf;
        } else {
            snprintf(buf, sizeof(buf), "[%zu]", tensors[i].n_elements);
            shape_str = buf;
        }
        fprintf(stderr, "  [%zu] %-50s %s n=%zu\n",
                i, tensors[i].name, shape_str, tensors[i].n_elements);
    }

    /* ── FP8 dequantization: apply _scale_inv tensors ──
     * Models like GLM-5.1-FP8 store weights as FP8 E4M3 with companion
     * _scale_inv tensors (BF16) that hold per-block inverse scales.
     * For each weight tensor "foo", if "foo_scale_inv" exists, multiply
     * the weight data by the scale factors (block size = cols / scale_cols,
     * typically 128). After applying, remove the _scale_inv tensors. */
    {
        size_t n_applied = 0;
        for (size_t i = 0; i < count; i++) {
            /* Check if this tensor has a matching _scale_inv */
            char scale_name[512];
            snprintf(scale_name, sizeof(scale_name), "%s_scale_inv", tensors[i].name);

            /* Skip if this IS a _scale_inv tensor */
            size_t nlen = strlen(tensors[i].name);
            if (nlen > 10 && strcmp(tensors[i].name + nlen - 10, "_scale_inv") == 0)
                continue;

            /* Find matching scale tensor */
            fpq_raw_tensor_t *scale_t = NULL;
            size_t scale_idx = 0;
            for (size_t j = 0; j < count; j++) {
                if (strcmp(tensors[j].name, scale_name) == 0) {
                    scale_t = &tensors[j];
                    scale_idx = j;
                    break;
                }
            }
            if (!scale_t || !scale_t->data || !tensors[i].data) continue;

            /* Determine block size from shape relationship:
             * Weight: [rows, cols], Scale: [scale_rows, scale_cols]
             * Block size per row = cols / scale_cols (typically 128)
             * Rows must match or scale_rows divides rows */
            size_t w_rows = tensors[i].rows;
            size_t w_cols = tensors[i].cols;
            size_t s_rows = scale_t->rows;
            size_t s_cols = scale_t->cols;

            if (s_rows == 0 || s_cols == 0) continue;

            size_t block_c = (s_cols > 0 && w_cols >= s_cols) ? w_cols / s_cols : 1;
            size_t block_r = (s_rows > 0 && w_rows >= s_rows) ? w_rows / s_rows : 1;

            fprintf(stderr, "  FP8 dequant: %s × %s (block %zux%zu)\n",
                    tensors[i].name, scale_name, block_r, block_c);

            /* Apply scales: weight[r][c] *= scale[r/block_r][c/block_c] */
            for (size_t r = 0; r < w_rows; r++) {
                size_t sr = r / block_r;
                if (sr >= s_rows) sr = s_rows - 1;
                for (size_t c = 0; c < w_cols; c++) {
                    size_t sc = c / block_c;
                    if (sc >= s_cols) sc = s_cols - 1;
                    float s = scale_t->data[sr * s_cols + sc];
                    tensors[i].data[r * w_cols + c] *= s;
                }
            }
            n_applied++;
            (void)scale_idx;
        }

        if (n_applied > 0) {
            fprintf(stderr, "FPQ: Applied %zu FP8 scale_inv dequantizations\n", n_applied);

            /* Remove _scale_inv tensors — they're no longer needed */
            size_t new_count = 0;
            for (size_t i = 0; i < count; i++) {
                size_t nlen = strlen(tensors[i].name);
                if (nlen > 10 && strcmp(tensors[i].name + nlen - 10, "_scale_inv") == 0) {
                    free(tensors[i].data);
                    tensors[i].data = NULL;
                } else {
                    if (new_count != i)
                        tensors[new_count] = tensors[i];
                    new_count++;
                }
            }
            count = new_count;
            fprintf(stderr, "FPQ: %zu tensors after removing scale_inv\n", count);
        }
    }

    *n_tensors = count;
    return tensors;
}

/* ── fp32 → bf16 conversion ── */
static uint16_t fp32_to_bf16(float f) {
    union { uint32_t u; float f; } v;
    v.f = f;
    /* Round to nearest even */
    uint32_t rounding_bias = ((v.u >> 16) & 1) + 0x7FFFu;
    return (uint16_t)((v.u + rounding_bias) >> 16);
}

/*
 * Write tensors as a BF16 safetensors file.
 * Input tensors have fp32 data which is converted to bf16 on write.
 */
int fpq_safetensors_write(const char *path, const fpq_raw_tensor_t *tensors,
                          size_t n_tensors) {
    /* Compute data offsets */
    size_t total_data = 0;
    size_t *offsets = (size_t *)malloc(n_tensors * sizeof(size_t));
    for (size_t i = 0; i < n_tensors; i++) {
        offsets[i] = total_data;
        total_data += tensors[i].n_elements * 2; /* bf16 = 2 bytes */
    }

    /* Build JSON header */
    size_t json_cap = n_tensors * 256 + 256;
    char *json = (char *)malloc(json_cap);
    size_t json_len = 0;
    json[json_len++] = '{';

    for (size_t i = 0; i < n_tensors; i++) {
        if (i > 0) json[json_len++] = ',';

        int written;
        if (tensors[i].n_dims >= 2) {
            written = snprintf(json + json_len, json_cap - json_len,
                "\"%s\":{\"dtype\":\"BF16\",\"shape\":[%zu,%zu],"
                "\"data_offsets\":[%zu,%zu]}",
                tensors[i].name,
                tensors[i].rows, tensors[i].cols,
                offsets[i], offsets[i] + tensors[i].n_elements * 2);
        } else {
            written = snprintf(json + json_len, json_cap - json_len,
                "\"%s\":{\"dtype\":\"BF16\",\"shape\":[%zu],"
                "\"data_offsets\":[%zu,%zu]}",
                tensors[i].name,
                tensors[i].n_elements,
                offsets[i], offsets[i] + tensors[i].n_elements * 2);
        }
        json_len += (size_t)written;
        if (json_len >= json_cap - 128) {
            json_cap *= 2;
            json = (char *)realloc(json, json_cap);
        }
    }
    json[json_len++] = '}';
    json[json_len] = '\0';

    /* Pad header to 8-byte alignment */
    while (json_len % 8 != 0) json[json_len++] = ' ';
    json[json_len] = '\0';

    FILE *f = fopen(path, "wb");
    if (!f) {
        fprintf(stderr, "Failed to open %s for writing\n", path);
        free(json); free(offsets);
        return -1;
    }

    /* 8-byte header length (little-endian) */
    uint64_t header_len = (uint64_t)json_len;
    fwrite(&header_len, 8, 1, f);
    fwrite(json, 1, json_len, f);

    /* Write tensor data as BF16, converting in chunks */
    size_t bf16_buf_size = 4096;
    uint16_t *bf16_buf = (uint16_t *)malloc(bf16_buf_size * sizeof(uint16_t));

    for (size_t i = 0; i < n_tensors; i++) {
        size_t n = tensors[i].n_elements;
        const float *src = tensors[i].data;
        size_t done = 0;
        while (done < n) {
            size_t chunk = n - done;
            if (chunk > bf16_buf_size) chunk = bf16_buf_size;
            for (size_t j = 0; j < chunk; j++)
                bf16_buf[j] = fp32_to_bf16(src[done + j]);
            fwrite(bf16_buf, 2, chunk, f);
            done += chunk;
        }
    }

    fclose(f);
    free(bf16_buf);
    free(json);
    free(offsets);

    fprintf(stderr, "FPQ: Wrote %zu tensors to %s (%.1f MB BF16)\n",
            n_tensors, path, (double)total_data / (1024.0 * 1024.0));
    return 0;
}
