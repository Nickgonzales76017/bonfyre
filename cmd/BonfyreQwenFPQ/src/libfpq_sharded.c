#include <sys/mman.h>

#define _POSIX_C_SOURCE 200809L

/*
 * libfpq_sharded.c — lazy native 0QPF runtime for BonfyreQwenFPQ.
 *
 * No full model decode. No fpq_native_read_compressed() bridge.
 * - fpq_open(): indexes native 0QPF shard headers/tables only
 * - fpq_matmul(): direct compact matvec over LR + optional residual + ghost
 * - fpq_decode_row(): single-row decode for embeddings
 * - fpq_decode_one(): passthrough tensors or slow one-tensor escape hatch
 *
 * Set BONFYRE_NATIVE_LR_ONLY=1 for first smoke: uses LR+ghost only and skips
 * residual E8/RVQ/QJL blocks so we can prove Qwen prompt execution path fast.
 */

#include "libfpq.h"
#include "fpq.h"
#include "fpqx.h"
#include "fpq_active_cache.h"
#include "fpq_kernels_neon.h"
#include <bonfyre.h>

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#ifdef __APPLE__
#include <Accelerate/Accelerate.h>
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>


#ifndef MAP_ANONYMOUS
#ifdef MAP_ANON
#define MAP_ANONYMOUS MAP_ANON
#elif defined(__APPLE__)
#define MAP_ANONYMOUS 0x1000
#endif
#endif

typedef struct bonfyre_sbb_mmap_rec_t {
    void *ptr;
    size_t bytes;
    struct bonfyre_sbb_mmap_rec_t *next;
} bonfyre_sbb_mmap_rec_t;

typedef enum {
    BONFYRE_TENSOR_STATE_INDEXED = 0,
    BONFYRE_TENSOR_STATE_RUNTIME_LOADED = 1,
    BONFYRE_TENSOR_STATE_SLI_PREPARED = 2,
    BONFYRE_TENSOR_STATE_EVICTED = 3,
    BONFYRE_TENSOR_STATE_RELOADABLE = 4,
    BONFYRE_TENSOR_STATE_CLOSED = 5
} bonfyre_tensor_state_t;

static bonfyre_sbb_mmap_rec_t *bonfyre_sbb_mmap_head = NULL;
static fpq_runtime_stats_t bonfyre_runtime_stats = {0};

static void bonfyre_sbb_mmap_register(void *ptr, size_t bytes) {
    if (!ptr || !bytes) return;
    bonfyre_sbb_mmap_rec_t *r = (bonfyre_sbb_mmap_rec_t *)calloc(1, sizeof(*r));
    if (!r) return;
    r->ptr = ptr;
    r->bytes = bytes;
    r->next = bonfyre_sbb_mmap_head;
    bonfyre_sbb_mmap_head = r;
}

static size_t bonfyre_sbb_mmap_unregister(void *ptr) {
    bonfyre_sbb_mmap_rec_t **pp = &bonfyre_sbb_mmap_head;
    while (*pp) {
        bonfyre_sbb_mmap_rec_t *r = *pp;
        if (r->ptr == ptr) {
            size_t bytes = r->bytes;
            *pp = r->next;
            free(r);
            return bytes;
        }
        pp = &r->next;
    }
    return 0;
}

static void *bonfyre_sbb_big_alloc(size_t bytes) {
    if (bytes == 0) bytes = 1;

    const char *disable = getenv("BONFYRE_QWEN_SBB_MMAP");
    int use_mmap = !(disable && *disable && strcmp(disable, "0") == 0);

#ifdef MAP_ANONYMOUS
    if (use_mmap) {
        long ps_l = sysconf(_SC_PAGESIZE);
        size_t ps = ps_l > 0 ? (size_t)ps_l : 4096u;
        size_t rounded = (bytes + ps - 1u) & ~(ps - 1u);

        void *mem = mmap(NULL,
                         rounded,
                         PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS,
                         -1,
                         0);
        if (mem != MAP_FAILED) {
            bonfyre_sbb_mmap_register(mem, rounded);
            bonfyre_runtime_stats.mmap_count++;
            bonfyre_runtime_stats.mmap_bytes += (uint64_t)rounded;

            const char *dbg = getenv("BONFYRE_QWEN_LOG_SBB_MMAP");
            if (dbg && *dbg && strcmp(dbg, "0") != 0) {
                fprintf(stderr,
                        "qwen_sbb_mmap_alloc bytes=%.1fMB mapped=%.1fMB\n",
                        (double)bytes / 1048576.0,
                        (double)rounded / 1048576.0);
                fflush(stderr);
            }

            return mem;
        }
    }
#endif

    return calloc(1, bytes);
}

static void bonfyre_sbb_big_free(void *ptr) {
    if (!ptr) return;

    size_t mapped = bonfyre_sbb_mmap_unregister(ptr);
    if (mapped > 0) {
        bonfyre_runtime_stats.munmap_count++;
        const char *dbg = getenv("BONFYRE_QWEN_LOG_SBB_MMAP");
        if (dbg && *dbg && strcmp(dbg, "0") != 0) {
            fprintf(stderr,
                    "qwen_sbb_mmap_free mapped=%.1fMB\n",
                    (double)mapped / 1048576.0);
            fflush(stderr);
        }
        munmap(ptr, mapped);
        return;
    }

    free(ptr);
}

void fpq_runtime_stats_reset(void) {
    memset(&bonfyre_runtime_stats, 0, sizeof(bonfyre_runtime_stats));
}

fpq_runtime_stats_t fpq_runtime_stats_get(void) {
    return bonfyre_runtime_stats;
}

static void bonfyre_v9_tensor_free(fpq_tensor_t *enc) {
    if (!enc) return;
    if (enc->seeds) {
        for (size_t i = 0; i < enc->n_blocks; i++) {
            fpq_seed_free(enc->seeds[i]);
        }
        free(enc->seeds);
        enc->seeds = NULL;
    }
    if (enc->qjl) {
        for (size_t i = 0; i < enc->n_blocks; i++) {
            fpq_qjl_free(enc->qjl[i]);
        }
        free(enc->qjl);
        enc->qjl = NULL;
    }
    if (enc->coord_quants) {
        for (size_t i = 0; i < enc->n_blocks; i++) {
            free(enc->coord_quants[i]);
        }
        free(enc->coord_quants);
        enc->coord_quants = NULL;
    }
    fpq_seed_free(enc->base_seed);
    enc->base_seed = NULL;
    free(enc->radii);
    enc->radii = NULL;
    free(enc->coord_scales);
    enc->coord_scales = NULL;
    free(enc->coord_residual_norms);
    enc->coord_residual_norms = NULL;
    free(enc->chaos_r_idx);
    enc->chaos_r_idx = NULL;
    if (enc->ghost) {
        free(enc->ghost->u);
        free(enc->ghost->v);
        free(enc->ghost);
        enc->ghost = NULL;
    }
    if (enc->sbb_scale_delta) {
        bonfyre_sbb_big_free(enc->sbb_scale_delta);
        enc->sbb_scale_delta = NULL;
    }
    free(enc);
}


#endif

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define FPQ_NATIVE_MAGIC_LE 0x46505130u
#define FPQ_NATIVE_MAGIC_BE 0x30515046u
#define FPQ_NATIVE_VERSION_EXPECTED 11u
#define FPQ_NATIVE_BLOCK_DIM 256u

#define FPQ_PASSTHROUGH_PRELOAD_LIMIT 1048576u
#define FPQ_TIED_EMBEDDING_PRELOAD_HARD_MAX_BYTES (1024ULL * 1024ULL * 1024ULL)

typedef struct __attribute__((packed)) {
    uint16_t name_len;
    uint32_t rows;
    uint32_t cols;
    uint16_t lr_rank;
    uint8_t  coord_bits;
    uint8_t  has_ghost;
    uint32_t n_blocks;
    uint16_t effective_k;
    uint64_t data_offset;
    uint64_t data_size;
} native_tensor_header_disk_t;

typedef struct __attribute__((packed)) {
    uint16_t name_len;
    uint32_t rows;
    uint32_t cols;
    uint16_t lr_rank;
    uint8_t  coord_bits;
    uint8_t  has_ghost;
    uint16_t n_blocks;
    uint16_t effective_k;
    uint64_t data_offset;
    uint64_t data_size;
} native_tensor_header_v10_disk_t;

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t n_tensors;
    uint32_t flags;
    uint64_t tensor_table_offset;
} native_file_header_disk_t;

typedef enum { NT_FP16 = 0, NT_V9 = 1 } native_kind_t;

typedef struct {
    fpq_tensor_info_t info;
    char *name;
    char *path;
    native_kind_t kind;
    uint32_t rows, cols;
    uint16_t lr_rank;
    uint8_t coord_bits, has_ghost;
    uint16_t effective_k;
    size_t n_blocks;
    uint64_t data_offset, data_size;
    uint64_t off_us, off_vt, off_coord_scales, off_warp_norms;
    uint64_t off_rn_scale, off_rn_q, off_e8, off_tile_cb, off_tile_idx;
    uint64_t off_qjl, off_ghost, off_haar;
    float *passthrough;
    size_t passthrough_len;
    float *tile_cb_cache;
    fpq_tensor_t *compressed;
    fpqx_tensor_t *fpqx;
    fpqx_sli_ctx_t *sli;
    uint64_t haar_seed_cache;
    int haar_seed_loaded;
    bonfyre_tensor_state_t state;
} native_tensor_t;

struct fpq_model {
    char *path;
    native_tensor_t *tensors;
    size_t n_tensors;
    fpq_info_t cached_info;
    void *active_cache;
};

static int fpq_truthy_env_local(const char *name) {
    const char *v = getenv(name);
    return v && v[0] && strcmp(v, "0") != 0 &&
           strcasecmp(v, "false") != 0 &&
           strcasecmp(v, "off") != 0;
}

/* The sidecar may request a tied-embedding preload, but this runtime never
 * turns that into an unbounded allocation.  A lower per-profile ceiling is
 * permitted; an attempted larger ceiling is clamped to the hard limit. */
static uint64_t fpq_tied_embedding_preload_max_bytes(void) {
    const char *text = getenv("BONFYRE_QWEN_PRELOAD_TIED_EMBEDDINGS_MAX_BYTES");
    char *end = NULL;
    unsigned long long parsed;

    if (!text || !text[0]) return FPQ_TIED_EMBEDDING_PRELOAD_HARD_MAX_BYTES;
    errno = 0;
    parsed = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || parsed == 0) {
        return FPQ_TIED_EMBEDDING_PRELOAD_HARD_MAX_BYTES;
    }
    if (parsed > FPQ_TIED_EMBEDDING_PRELOAD_HARD_MAX_BYTES) {
        return FPQ_TIED_EMBEDDING_PRELOAD_HARD_MAX_BYTES;
    }
    return (uint64_t)parsed;
}

static const char *fpq_tied_embedding_tensor_name(void) {
    const char *name = getenv("BONFYRE_QWEN_TIED_EMBEDDING_TENSOR");
    return (name && name[0]) ? name : "model.embed_tokens.weight";
}

static int fpq_active_lr_path_enabled(void) {
    if (fpq_truthy_env_local("BONFYRE_QWEN_DISABLE_ACTIVE_LR")) return 0;
    return fpq_truthy_env_local("BONFYRE_QWEN_ACTIVE_LR_FIRST") ||
           fpq_truthy_env_local("BONFYRE_NATIVE_LR_ONLY");
}

static const char *bonfyre_tensor_state_name(bonfyre_tensor_state_t state) {
    switch (state) {
        case BONFYRE_TENSOR_STATE_INDEXED: return "indexed";
        case BONFYRE_TENSOR_STATE_RUNTIME_LOADED: return "runtime_loaded";
        case BONFYRE_TENSOR_STATE_SLI_PREPARED: return "sli_prepared";
        case BONFYRE_TENSOR_STATE_EVICTED: return "evicted";
        case BONFYRE_TENSOR_STATE_RELOADABLE: return "reloadable";
        case BONFYRE_TENSOR_STATE_CLOSED: return "closed";
        default: return "unknown";
    }
}

static void bonfyre_tensor_set_state(native_tensor_t *t, bonfyre_tensor_state_t state) {
    if (!t) return;
    t->state = state;
}

static void bonfyre_tensor_clear_runtime(native_tensor_t *t) {
    if (!t) return;
    if (t->sli) {
        fpqx_sli_free(t->sli);
        t->sli = NULL;
    }
    if (t->fpqx) {
        t->fpqx->additive = NULL;
        free(t->fpqx);
        t->fpqx = NULL;
    }
    if (t->compressed) {
        bonfyre_v9_tensor_free(t->compressed);
        t->compressed = NULL;
    }
}

static void bonfyre_tensor_destroy(native_tensor_t *t) {
    if (!t || t->state == BONFYRE_TENSOR_STATE_CLOSED) return;
    bonfyre_tensor_clear_runtime(t);
    bonfyre_tensor_set_state(t, BONFYRE_TENSOR_STATE_CLOSED);
}

static void bonfyre_tensor_mark_reloadable(native_tensor_t *t) {
    if (!t) return;
    bonfyre_tensor_set_state(t, BONFYRE_TENSOR_STATE_EVICTED);
    bonfyre_tensor_set_state(t, BONFYRE_TENSOR_STATE_RELOADABLE);
}


static double fpq_prepare_elapsed_seconds_local(const struct timespec *a,
                                                const struct timespec *b) {
    return (double)(b->tv_sec - a->tv_sec) +
           (double)(b->tv_nsec - a->tv_nsec) / 1000000000.0;
}

static int fpq_prepare_breakdown_enabled(void) {
    const char *p = getenv("BONFYRE_QWEN_PREPARE_BREAKDOWN_JSONL");
    return p && p[0];
}

static void fpq_prepare_breakdown_emit(const char *kind,
                                       const char *tensor_name,
                                       double seconds,
                                       const char *extra_json) {
    const char *path = getenv("BONFYRE_QWEN_PREPARE_BREAKDOWN_JSONL");
    FILE *fp;
    if (!path || !path[0] || !kind) return;
    if (bf_ensure_parent_dir(path) != 0) return;
    fp = fopen(path, "a");
    if (!fp) return;
    fprintf(fp,
            "{\"kind\":\"%s\",\"tensor\":",
            kind);
    if (tensor_name && tensor_name[0]) {
        fprintf(fp, "\"");
        for (const char *p = tensor_name; *p; p++) {
            if (*p == '"' || *p == '\\') fputc('\\', fp);
            fputc(*p, fp);
        }
        fprintf(fp, "\"");
    } else {
        fprintf(fp, "null");
    }
    fprintf(fp, ",\"seconds\":%.9f", seconds);
    if (extra_json && extra_json[0]) fprintf(fp, ",%s", extra_json);
    fprintf(fp, "}\n");
    fclose(fp);
}

static float fp16_to_float_local(uint16_t h) {
    uint32_t sign = ((uint32_t)h & 0x8000u) << 16;
    uint32_t exp  = ((uint32_t)h >> 10) & 0x1fu;
    uint32_t frac = (uint32_t)h & 0x03ffu;
    if (exp == 0) {
        if (frac == 0) { union { uint32_t u; float f; } z = { sign }; return z.f; }
        int32_t e = -14;
        while ((frac & 0x0400u) == 0) { frac <<= 1; e--; }
        frac &= ~0x0400u;
        union { uint32_t u; float f; } z = { sign | (uint32_t)((e + 127) << 23) | (frac << 13) };
        return z.f;
    } else if (exp == 31) {
        union { uint32_t u; float f; } z = { sign | 0x7f800000u | (frac << 13) };
        return z.f;
    }
    union { uint32_t u; float f; } z = { sign | ((exp + 112u) << 23) | (frac << 13) };
    return z.f;
}

static int read_exact(FILE *fp, void *dst, size_t n) { return fread(dst, 1, n, fp) == n ? 0 : -1; }

static int read_at(const char *path, uint64_t off, void *dst, size_t n) {
    FILE *fp = fopen(path, "rb"); if (!fp) return -1;
    int rc = 0;
    if (fseek(fp, (long)off, SEEK_SET) != 0) rc = -1; else rc = read_exact(fp, dst, n);
    fclose(fp); return rc;
}

static FILE *open_seek(const char *path, uint64_t off) {
    FILE *fp = fopen(path, "rb"); if (!fp) return NULL;
    if (fseek(fp, (long)off, SEEK_SET) != 0) { fclose(fp); return NULL; }
    return fp;
}

static int ends_with_local(const char *s, const char *suffix) {
    size_t a = strlen(s), b = strlen(suffix);
    return a >= b && strcmp(s + a - b, suffix) == 0;
}
static int path_cmp(const void *a, const void *b) {
    const char *pa = *(const char * const *)a, *pb = *(const char * const *)b;
    return strcmp(pa, pb);
}

static void infer_parts_dir(const char *path, char *out, size_t n) {
    struct stat st;
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) { snprintf(out, n, "%s", path); return; }
    snprintf(out, n, "%s", path);
    char *slash = strrchr(out, '/');
    char dir[PATH_MAX] = ".", base[PATH_MAX] = {0};
    if (slash) { *slash = 0; snprintf(dir, sizeof(dir), "%s", out); snprintf(base, sizeof(base), "%s", slash + 1); }
    else snprintf(base, sizeof(base), "%s", path);
    char *p = strstr(base, ".fpq-pack.json"); if (p) *p = 0;
    p = strstr(base, ".fpq2-pack.json"); if (p) *p = 0;
    snprintf(out, n, "%s/%s.parts", dir, base);
}

static size_t infer_blocks(uint32_t rows, uint32_t cols, uint16_t hdr_blocks) {
    uint64_t n = (uint64_t)rows * (uint64_t)cols;
    if (n < FPQ_NATIVE_BLOCK_DIM * 2ull) return 0;
    uint64_t expect = (n + FPQ_NATIVE_BLOCK_DIM - 1ull) / FPQ_NATIVE_BLOCK_DIM;
    if (expect > 65535ull) return (size_t)expect;
    return hdr_blocks ? (size_t)hdr_blocks : (size_t)expect;
}

static uint64_t ghost_bytes_for(const native_tensor_t *t) {
    return t->has_ghost ? (2ull + 2ull + (uint64_t)t->rows + 2ull + (uint64_t)t->cols) : 0;
}

static void compute_offsets(native_tensor_t *t) {
    if (t->kind == NT_FP16) return;
    uint64_t off = t->data_offset, r = t->lr_rank, rows = t->rows, cols = t->cols;
    uint64_t nb = t->n_blocks, ek = t->effective_k ? t->effective_k : 256u;
    t->off_us = off; off += r * (2ull + rows);
    t->off_vt = off; off += r * (2ull + cols);
    t->off_coord_scales = off; off += nb * 2ull;
    t->off_warp_norms = off; off += nb * 2ull;
    t->off_rn_scale = off; off += 2ull;
    t->off_rn_q = off; off += nb;
    t->off_e8 = off; off += nb * 256ull;
    t->off_tile_cb = off; off += ek * 16ull * 2ull;
    t->off_tile_idx = off; off += nb * 16ull;
    t->off_qjl = off; off += nb * 8ull;
    t->off_ghost = off; off += ghost_bytes_for(t);
    t->off_haar = off;
}

static native_tensor_t *find_tensor(fpq_model_t *m, const char *name) {
    if (!m || !name) return NULL;
    for (size_t i = 0; i < m->n_tensors; i++)
        if (m->tensors[i].name && strcmp(m->tensors[i].name, name) == 0) return &m->tensors[i];
    return NULL;
}

/* SLI scores residual blocks as blocks_per_row segments.  Older native packs
 * are flat row-major and require a 256-aligned width; current Qwen FPQ packs
 * explicitly pad every row to the next SLI block.  Detect that structural
 * contract from the encoded block count instead of rejecting e.g. Qwen's
 * 896-wide projections and falling back to per-file residual streaming. */
static int native_sli_layout_safe(const native_tensor_t *t) {
    uint64_t blocks_per_row;
    uint64_t required_blocks;
    if (!t || t->kind != NT_V9 || t->rows == 0 || t->cols == 0) return 0;
    if ((t->cols % FPQ_NATIVE_BLOCK_DIM) == 0) return 1;
    blocks_per_row = ((uint64_t)t->cols + FPQ_NATIVE_BLOCK_DIM - 1u) /
                     FPQ_NATIVE_BLOCK_DIM;
    if ((uint64_t)t->rows > UINT64_MAX / blocks_per_row) return 0;
    required_blocks = (uint64_t)t->rows * blocks_per_row;
    /* The exact row-padded GEMV is an explicit prewarmed lane while its
     * prepared representation is being moved into a persistent cache.  The
     * default preserves the established low-memory streaming behavior. */
    return fpq_truthy_env_local("BONFYRE_QWEN_LINEAR_SLI") &&
           (uint64_t)t->n_blocks == required_blocks;
}

static int append_tensor(native_tensor_t **arr, size_t *n, const native_tensor_t *src) {
    native_tensor_t *g = (native_tensor_t *)realloc(*arr, (*n + 1) * sizeof(native_tensor_t));
    if (!g) return -1; *arr = g; (*arr)[*n] = *src; (*n)++; return 0;
}

static int load_passthrough(native_tensor_t *t,
                            const char *tied_embedding_tensor,
                            int preload_tied_embeddings) {
    uint64_t n = (uint64_t)t->rows * (uint64_t)t->cols;
    uint64_t tied_preload_bytes = 0;
    int force_hot_fp16 =
        t &&
        t->name &&
        fpq_truthy_env_local("BONFYRE_QWEN_PRELOAD_HOT_FP16") &&
        (strstr(t->name, ".self_attn.v_proj.weight") != NULL ||
         strstr(t->name, ".mlp.up_proj.weight") != NULL);
    int force_lm_head =
        t && t->name &&
        strstr(t->name, "lm_head") != NULL &&
        (fpq_truthy_env_local("BONFYRE_QWEN_PRELOAD_LM_HEAD") ||
         n <= (uint64_t)192 * 1024 * 1024);
    int force_tied_embeddings = 0;

    if (t && t->name && tied_embedding_tensor &&
        strcmp(t->name, tied_embedding_tensor) == 0 && preload_tied_embeddings &&
        n <= UINT64_MAX / sizeof(float)) {
        tied_preload_bytes = n * sizeof(float);
        if (tied_preload_bytes <= fpq_tied_embedding_preload_max_bytes()) {
            force_tied_embeddings = 1;
        } else {
            fprintf(stderr,
                    "fpq_open native: tied embedding preload skipped tensor=%s bytes=%.3f GiB budget=%.3f GiB\n",
                    t->name,
                    (double)tied_preload_bytes / 1073741824.0,
                    (double)fpq_tied_embedding_preload_max_bytes() / 1073741824.0);
            fflush(stderr);
        }
    }

    if (n > FPQ_PASSTHROUGH_PRELOAD_LIMIT && !force_lm_head && !force_tied_embeddings && !force_hot_fp16) return 0;
    if (force_lm_head || force_tied_embeddings) {
        fprintf(stderr,
                "fpq_open native: force-preload passthrough tensor=%s rows=%u cols=%u bytes=%.3f GiB\n",
                t->name ? t->name : "(null)",
                t->rows,
                t->cols,
                ((double)n * (double)sizeof(float)) / 1073741824.0);
        fflush(stderr);
    } else if (force_hot_fp16) {
        fprintf(stderr,
                "fpq_open native: hot-preload passthrough tensor=%s rows=%u cols=%u bytes=%.3f MiB\n",
                t->name ? t->name : "(null)",
                t->rows,
                t->cols,
                ((double)n * (double)sizeof(float)) / 1048576.0);
        fflush(stderr);
    }
    if (n == 0 || n > (uint64_t)((size_t)-1) / sizeof(float)) return -1;
    t->passthrough = (float *)calloc((size_t)n, sizeof(float));
    if (!t->passthrough) return -1;
    t->passthrough_len = (size_t)n;
    FILE *fp = open_seek(t->path, t->data_offset);
    if (!fp) {
        free(t->passthrough);
        t->passthrough = NULL;
        t->passthrough_len = 0;
        return -1;
    }
    for (uint64_t i = 0; i < n; i++) {
        uint16_t hv = 0;
        if (read_exact(fp, &hv, 2) != 0) {
            fclose(fp);
            free(t->passthrough);
            t->passthrough = NULL;
            t->passthrough_len = 0;
            return -1;
        }
        t->passthrough[i] = fp16_to_float_local(hv);
    }
    fclose(fp); return 0;
}

static int load_tile_codebook(native_tensor_t *t) {
    if (!t || t->kind != NT_V9) return 0;
    if (t->tile_cb_cache) return 0;
    size_t n = (size_t)t->effective_k * 16u;
    if (n == 0 || n > (size_t)-1 / sizeof(float)) return -1;
    t->tile_cb_cache = (float *)calloc(n, sizeof(float));
    if (!t->tile_cb_cache) return -1;
    FILE *fp = open_seek(t->path, t->off_tile_cb);
    if (!fp) {
        free(t->tile_cb_cache);
        t->tile_cb_cache = NULL;
        return -1;
    }
    for (size_t i = 0; i < n; i++) {
        uint16_t hv = 0;
        if (read_exact(fp, &hv, 2) != 0) {
            fclose(fp);
            free(t->tile_cb_cache);
            t->tile_cb_cache = NULL;
            return -1;
        }
        t->tile_cb_cache[i] = fp16_to_float_local(hv);
    }
    fclose(fp);
    return 0;
}

static int load_v9_runtime(native_tensor_t *t) {
    if (!t || t->kind != NT_V9) return 0;

    fpq_tensor_t *enc = (fpq_tensor_t *)calloc(1, sizeof(fpq_tensor_t));
    fpqx_tensor_t *fx = NULL;
    uint8_t *blob = NULL;
    if (!enc) return -1;

    size_t rows = t->rows, cols = t->cols, n_blocks = t->n_blocks;
    int lr_rank = t->lr_rank;
    int effective_k = t->effective_k > 0 ? t->effective_k : 256;
    size_t lr_us_size = rows * (size_t)lr_rank;
    size_t lr_vt_size = (size_t)lr_rank * cols;
    size_t padded = FPQ_BLOCK_DIM;
    size_t v8_base = 2 + lr_us_size + lr_vt_size;
    size_t e8_off = v8_base + n_blocks;
    size_t e8_flat_size = n_blocks * padded;
    size_t tile_cb_off = e8_off + e8_flat_size;
    size_t tile_cb_size = (size_t)effective_k * 16u;
    size_t tile_idx_off = tile_cb_off + tile_cb_size;
    size_t tile_idx_size = n_blocks * 16u;
    size_t sbb_total = tile_idx_off + tile_idx_size + 1u;
    size_t blob_size = (size_t)t->data_size;
    const uint8_t *p = NULL;
    const uint8_t *end = NULL;

    strncpy(enc->name, t->name, sizeof(enc->name) - 1);
    enc->original_rows = rows;
    enc->original_cols = cols;
    enc->n_blocks = n_blocks;
    enc->coord_bits = t->coord_bits;
    enc->pid_alpha = -9.0f;
    enc->sbb_group_id = -1;

    enc->coord_scales = (float *)calloc(n_blocks ? n_blocks : 1u, sizeof(float));
    enc->coord_residual_norms = (float *)calloc(n_blocks ? n_blocks : 1u, sizeof(float));
    enc->qjl = (fpq_qjl_t **)calloc(n_blocks ? n_blocks : 1u, sizeof(fpq_qjl_t *));
    enc->sbb_scale_delta = (float *)bonfyre_sbb_big_alloc((sbb_total ? sbb_total : 1u) * sizeof(float));
    if (!enc->coord_scales || !enc->coord_residual_norms || !enc->qjl || !enc->sbb_scale_delta) {
        bonfyre_v9_tensor_free(enc);
        return -1;
    }

    if (blob_size == 0) {
        bonfyre_v9_tensor_free(enc);
        return -1;
    }
    blob = (uint8_t *)malloc(blob_size);
    if (!blob) {
        bonfyre_v9_tensor_free(enc);
        return -1;
    }
    if (read_at(t->path, t->data_offset, blob, blob_size) != 0) {
        free(blob);
        bonfyre_v9_tensor_free(enc);
        return -1;
    }
    p = blob;
    end = blob + blob_size;

#define NEED_BYTES(nbytes) do { if ((size_t)(end - p) < (size_t)(nbytes)) goto fail_blob; } while (0)
#define READ_U8(dst) do { NEED_BYTES(1); (dst) = *p++; } while (0)
#define READ_I8(dst) do { NEED_BYTES(1); memcpy(&(dst), p, 1); p += 1; } while (0)
#define READ_U16(dst) do { NEED_BYTES(2); memcpy(&(dst), p, 2); p += 2; } while (0)
#define READ_U64(dst) do { NEED_BYTES(8); memcpy(&(dst), p, 8); p += 8; } while (0)

    enc->sbb_scale_delta[0] = (float)lr_rank;
    enc->sbb_scale_delta[1] = (float)lr_rank;

    for (int r = 0; r < lr_rank; r++) {
        uint16_t scale_h = 0;
        READ_U16(scale_h);
        float sc = fp16_to_float_local(scale_h);
        for (size_t row = 0; row < rows; row++) {
            int8_t qb = 0;
            READ_I8(qb);
            enc->sbb_scale_delta[2 + row * (size_t)lr_rank + (size_t)r] = (float)qb * sc;
        }
    }

    for (int r = 0; r < lr_rank; r++) {
        uint16_t scale_h = 0;
        READ_U16(scale_h);
        float sc = fp16_to_float_local(scale_h);
        for (size_t col = 0; col < cols; col++) {
            int8_t qb = 0;
            READ_I8(qb);
            enc->sbb_scale_delta[2 + lr_us_size + (size_t)r * cols + col] = (float)qb * sc;
        }
    }

    for (size_t b = 0; b < n_blocks; b++) {
        uint16_t hv = 0;
        READ_U16(hv);
        enc->coord_scales[b] = fp16_to_float_local(hv);
    }
    for (size_t b = 0; b < n_blocks; b++) {
        uint16_t hv = 0;
        READ_U16(hv);
        enc->sbb_scale_delta[v8_base + b] = fp16_to_float_local(hv);
    }
    {
        uint16_t rn_scale_h = 0;
        READ_U16(rn_scale_h);
        float rn_sc = fp16_to_float_local(rn_scale_h);
        for (size_t b = 0; b < n_blocks; b++) {
            uint8_t qb = 0;
            READ_U8(qb);
            enc->coord_residual_norms[b] = (float)qb * rn_sc;
        }
    }

    for (size_t b = 0; b < e8_flat_size; b++) {
        int8_t v = 0;
        READ_I8(v);
        enc->sbb_scale_delta[e8_off + b] = (float)v;
    }
    for (size_t b = 0; b < tile_cb_size; b++) {
        uint16_t hv = 0;
        READ_U16(hv);
        enc->sbb_scale_delta[tile_cb_off + b] = fp16_to_float_local(hv);
    }
    for (size_t b = 0; b < tile_idx_size; b++) {
        uint8_t idx = 0;
        READ_U8(idx);
        enc->sbb_scale_delta[tile_idx_off + b] = (float)idx;
    }
    enc->sbb_scale_delta[tile_idx_off + tile_idx_size] = (float)effective_k;

    for (size_t b = 0; b < n_blocks; b++) {
        uint64_t bits_word = 0;
        fpq_qjl_t *qjl = (fpq_qjl_t *)calloc(1, sizeof(fpq_qjl_t));
        if (!qjl) goto fail_blob;
        qjl->bits = (uint64_t *)calloc(1, sizeof(uint64_t));
        if (!qjl->bits) {
            free(qjl);
            goto fail_blob;
        }
        READ_U64(bits_word);
        qjl->bits[0] = bits_word;
        qjl->n_projections = FPQ_QJL_PROJECTIONS;
        qjl->n_elements = padded;
        enc->qjl[b] = qjl;
    }

    if (t->has_ghost) {
        uint16_t sigma_h = 0, u_scale_h = 0, v_scale_h = 0;
        enc->ghost = (fpq_ghost_t *)calloc(1, sizeof(fpq_ghost_t));
        if (!enc->ghost) goto fail_blob;
        enc->ghost->rows = rows;
        enc->ghost->cols = cols;
        enc->ghost->u = (float *)calloc(rows ? rows : 1u, sizeof(float));
        enc->ghost->v = (float *)calloc(cols ? cols : 1u, sizeof(float));
        if (!enc->ghost->u || !enc->ghost->v) goto fail_blob;
        READ_U16(sigma_h);
        READ_U16(u_scale_h);
        enc->ghost->sigma = fp16_to_float_local(sigma_h);
        {
            float u_sc = fp16_to_float_local(u_scale_h);
            for (size_t row = 0; row < rows; row++) {
                int8_t qb = 0;
                READ_I8(qb);
                enc->ghost->u[row] = (float)qb * u_sc;
            }
        }
        READ_U16(v_scale_h);
        {
            float v_sc = fp16_to_float_local(v_scale_h);
            for (size_t col = 0; col < cols; col++) {
                int8_t qb = 0;
                READ_I8(qb);
                enc->ghost->v[col] = (float)qb * v_sc;
            }
        }
    }

    READ_U64(enc->haar_seed);
    free(blob);
    blob = NULL;

#undef NEED_BYTES
#undef READ_U8
#undef READ_I8
#undef READ_U16
#undef READ_U64

    for (size_t b = 0; b < n_blocks; b++) {
        if (enc->qjl[b]) enc->qjl[b]->proj_seed = enc->haar_seed ^ (uint64_t)b ^ 0xC00DULL;
    }

    fx = (fpqx_tensor_t *)calloc(1, sizeof(fpqx_tensor_t));
    if (!fx) { bonfyre_v9_tensor_free(enc); return -1; }
    strncpy(fx->name, t->name, sizeof(fx->name) - 1);
    fx->rows = rows;
    fx->cols = cols;
    fx->active_ops = FPQX_OP_ADDITIVE;
    fx->additive = enc;

    t->compressed = enc;
    t->fpqx = fx;
    t->sli = NULL;
    bonfyre_tensor_set_state(t, BONFYRE_TENSOR_STATE_RUNTIME_LOADED);
    return 0;

fail_blob:
#undef NEED_BYTES
#undef READ_U8
#undef READ_I8
#undef READ_U16
#undef READ_U64
    free(blob);
    bonfyre_v9_tensor_free(enc);
    return -1;
}

static int ensure_sli_ready(native_tensor_t *t) {
    fpq_tensor_t *enc;
    const char *probe_tensor;
    const char *probe_shared_qkv;
    int preserve_probe_metadata = 0;
    struct timespec t0 = {0};
    struct timespec t1 = {0};
    struct timespec s0 = {0};
    struct timespec s1 = {0};
    double total_seconds = 0.0;
    double load_runtime_seconds = 0.0;
    double sli_prepare_seconds = 0.0;
    int reloaded = 0;
    int state_before = 0;
    if (!t || t->kind != NT_V9) return -1;
    if (t->sli) {
        if (fpq_prepare_breakdown_enabled()) {
            fpq_prepare_breakdown_emit("ensure_sli_ready_hit", t->name, 0.0, "\"prepared\":true");
        }
        return 0;
    }
    state_before = (int)t->state;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    if (t->state == BONFYRE_TENSOR_STATE_EVICTED ||
        t->state == BONFYRE_TENSOR_STATE_RELOADABLE) {
        bonfyre_tensor_clear_runtime(t);
        bonfyre_tensor_set_state(t, BONFYRE_TENSOR_STATE_INDEXED);
        reloaded = 1;
    }
    if (t->compressed && !t->compressed->sbb_scale_delta) {
        bonfyre_tensor_mark_reloadable(t);
        bonfyre_tensor_clear_runtime(t);
        bonfyre_tensor_set_state(t, BONFYRE_TENSOR_STATE_INDEXED);
        reloaded = 1;
    }
    if (!t->fpqx || !t->compressed) {
        clock_gettime(CLOCK_MONOTONIC, &s0);
        if (load_v9_runtime(t) != 0) return -1;
        clock_gettime(CLOCK_MONOTONIC, &s1);
        load_runtime_seconds += fpq_prepare_elapsed_seconds_local(&s0, &s1);
    }
    if (!t->fpqx || !t->compressed) return -1;
    clock_gettime(CLOCK_MONOTONIC, &s0);
    t->sli = fpqx_sli_prepare(t->fpqx);
    clock_gettime(CLOCK_MONOTONIC, &s1);
    sli_prepare_seconds += fpq_prepare_elapsed_seconds_local(&s0, &s1);
    if (!t->sli) return -1;
    bonfyre_tensor_set_state(t, BONFYRE_TENSOR_STATE_SLI_PREPARED);
    enc = t->compressed;
    probe_tensor = getenv("BONFYRE_QWEN_SLI_SCORE_PROBE_TENSOR");
    probe_shared_qkv = getenv("BONFYRE_QWEN_SLI_SCORE_PROBE_SHARED_QKV");
    if (getenv("BONFYRE_QWEN_SLI_SCORE_PROBE") &&
        strcmp(getenv("BONFYRE_QWEN_SLI_SCORE_PROBE"), "0") != 0 &&
        t->name && probe_tensor && *probe_tensor) {
        if (strcmp(t->name, probe_tensor) == 0) {
            preserve_probe_metadata = 1;
        } else if (probe_shared_qkv && *probe_shared_qkv && strcmp(probe_shared_qkv, "0") != 0) {
            const char *lhs_q = strstr(probe_tensor, ".self_attn.q_proj.");
            const char *lhs_k = strstr(probe_tensor, ".self_attn.k_proj.");
            const char *lhs_v = strstr(probe_tensor, ".self_attn.v_proj.");
            const char *rhs_q = strstr(t->name, ".self_attn.q_proj.");
            const char *rhs_k = strstr(t->name, ".self_attn.k_proj.");
            const char *rhs_v = strstr(t->name, ".self_attn.v_proj.");
            const char *lhs_pos = lhs_q ? lhs_q : (lhs_k ? lhs_k : lhs_v);
            const char *rhs_pos = rhs_q ? rhs_q : (rhs_k ? rhs_k : rhs_v);
            if (lhs_pos && rhs_pos) {
                size_t lhs_prefix = (size_t)(lhs_pos - probe_tensor);
                size_t rhs_prefix = (size_t)(rhs_pos - t->name);
                if (lhs_prefix == rhs_prefix && strncmp(probe_tensor, t->name, lhs_prefix) == 0) {
                    preserve_probe_metadata = 1;
                }
            }
        }
    }
    if (!preserve_probe_metadata && enc->qjl) {
        for (size_t b = 0; b < enc->n_blocks; b++) {
            if (enc->qjl[b]) {
                free(enc->qjl[b]->bits);
                free(enc->qjl[b]);
            }
        }
        free(enc->qjl);
        enc->qjl = NULL;
    }
    if (!preserve_probe_metadata) {
        free(enc->coord_scales);
        enc->coord_scales = NULL;
        free(enc->coord_residual_norms);
        enc->coord_residual_norms = NULL;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    total_seconds = fpq_prepare_elapsed_seconds_local(&t0, &t1);
    if (fpq_prepare_breakdown_enabled()) {
        char extra[512];
        snprintf(extra, sizeof(extra),
                 "\"state_before\":%d,\"state_after\":%d,\"reloaded\":%s,"
                 "\"load_runtime_seconds\":%.9f,\"sli_prepare_seconds\":%.9f,"
                 "\"rows\":%u,\"cols\":%u,\"blocks\":%zu",
                 state_before, (int)t->state, reloaded ? "true" : "false",
                 load_runtime_seconds, sli_prepare_seconds,
                 t->rows, t->cols, t->n_blocks);
        fpq_prepare_breakdown_emit("ensure_sli_ready_build", t->name, total_seconds, extra);
    }
    return 0;
}

static int index_one_native_file(const char *path, native_tensor_t **out, size_t *n_out,
                                 const char *tied_embedding_tensor,
                                 int preload_tied_embeddings) {
    FILE *fp = fopen(path, "rb");
    if (!fp) { fprintf(stderr, "fpq_open native: cannot open %s: %s\n", path, strerror(errno)); return -1; }
    native_file_header_disk_t fh;
    if (read_exact(fp, &fh, sizeof(fh)) != 0) { fprintf(stderr, "fpq_open native: short header %s\n", path); fclose(fp); return -1; }
    if (fh.magic != FPQ_NATIVE_MAGIC_LE && fh.magic != FPQ_NATIVE_MAGIC_BE) { fprintf(stderr, "fpq_open native: bad magic 0x%08x in %s\n", fh.magic, path); fclose(fp); return -1; }
    if (fh.version < 10u || fh.version > FPQ_NATIVE_VERSION_EXPECTED) { fprintf(stderr, "fpq_open native: unsupported version %u in %s\n", fh.version, path); fclose(fp); return -1; }
    if (fseek(fp, (long)fh.tensor_table_offset, SEEK_SET) != 0) { fclose(fp); return -1; }
    size_t loaded = 0;
    for (uint32_t i = 0; i < fh.n_tensors; i++) {
        native_tensor_header_disk_t h;
        memset(&h, 0, sizeof(h));
        if (fh.version <= 10u) {
            native_tensor_header_v10_disk_t oldh;
            if (read_exact(fp, &oldh, sizeof(oldh)) != 0) { fclose(fp); return -1; }
            h.name_len = oldh.name_len;
            h.rows = oldh.rows;
            h.cols = oldh.cols;
            h.lr_rank = oldh.lr_rank;
            h.coord_bits = oldh.coord_bits;
            h.has_ghost = oldh.has_ghost;
            h.n_blocks = oldh.n_blocks;
            h.effective_k = oldh.effective_k;
            h.data_offset = oldh.data_offset;
            h.data_size = oldh.data_size;
        } else if (read_exact(fp, &h, sizeof(h)) != 0) { fclose(fp); return -1; }
        char *name = (char *)calloc((size_t)h.name_len + 1, 1); if (!name) { fclose(fp); return -1; }
        if (read_exact(fp, name, h.name_len) != 0) { free(name); fclose(fp); return -1; }
        native_tensor_t t; memset(&t, 0, sizeof(t));
        t.name = name; t.path = strdup(path); t.rows = h.rows; t.cols = h.cols; t.lr_rank = h.lr_rank; t.coord_bits = h.coord_bits;
        t.has_ghost = h.has_ghost; t.effective_k = h.effective_k ? h.effective_k : 256u; t.n_blocks = infer_blocks(h.rows, h.cols, h.n_blocks);
        t.data_offset = h.data_offset; t.data_size = h.data_size;
        /*
         * Rank-zero tensors can still carry a valid residual/block-coded V9 body.
         * Classifying them as FP16 only because lr_rank==0 forces them into the
         * slow passthrough/fallback lane and blocks direct resident SLI.
         */
        t.kind = (h.coord_bits == 0 || t.n_blocks == 0) ? NT_FP16 : NT_V9;
        t.state = BONFYRE_TENSOR_STATE_INDEXED;
        compute_offsets(&t);
        t.info.name = t.name; t.info.rows = t.rows; t.info.cols = t.cols; t.info.has_sli = (t.kind == NT_V9);
        t.info.bpw = (t.rows && t.cols) ? (float)(((double)t.data_size * 8.0) / ((double)t.rows * (double)t.cols)) : 0.0f;
        if (t.kind == NT_FP16 &&
            load_passthrough(&t, tied_embedding_tensor, preload_tied_embeddings) != 0) {
            fprintf(stderr,
                    "fpq_open native: passthrough load failed path=%s tensor=%s rows=%u cols=%u data_offset=%llu data_size=%llu version=%u\n",
                    path,
                    t.name ? t.name : "(null)",
                    t.rows, t.cols,
                    (unsigned long long)t.data_offset,
                    (unsigned long long)t.data_size,
                    fh.version);
            free(t.name); free(t.path); fclose(fp); return -1;
        }
        if (append_tensor(out, n_out, &t) != 0) { free(t.passthrough); free(t.name); free(t.path); fclose(fp); return -1; }
        loaded++;
    }
    fclose(fp);
    fprintf(stderr, "fpq_open native-index: %s — %zu tensors\n", path, loaded);
    return 0;
}

static int load_native_paths(const char *path, native_tensor_t **out, size_t *n_out,
                             const char *tied_embedding_tensor,
                             int preload_tied_embeddings) {
    char parts_dir[PATH_MAX]; infer_parts_dir(path, parts_dir, sizeof(parts_dir));
    DIR *d = opendir(parts_dir);
    if (!d) return index_one_native_file(path, out, n_out,
                                         tied_embedding_tensor, preload_tied_embeddings);
    char **paths = NULL; size_t n_paths = 0; struct dirent *ent;
    int prefer_legacy = fpq_truthy_env_local("BONFYRE_QWEN_PREFER_LEGACY_FPQ");
    /*
     * An FPQ2 conversion is emitted next to its legacy .fpq compatibility
     * shard.  Prefer the complete FPQ2 set: loading both duplicates every
     * tensor, while selecting the legacy shard first silently routes all of
     * those tensors through the FP16 passthrough path.
     */
    if (!prefer_legacy) {
        while ((ent = readdir(d)) != NULL) {
            if (!ends_with_local(ent->d_name, ".fpq2")) continue;
            char full[PATH_MAX]; snprintf(full, sizeof(full), "%s/%s", parts_dir, ent->d_name);
            char **g = (char **)realloc(paths, (n_paths + 1) * sizeof(char *)); if (!g) { closedir(d); return -1; }
            paths = g; paths[n_paths++] = strdup(full);
        }
    }
    if (n_paths > 0) {
        size_t legacy_count = 0;
        rewinddir(d);
        while ((ent = readdir(d)) != NULL) {
            if (ends_with_local(ent->d_name, ".fpq")) legacy_count++;
        }
        /* A side-by-side conversion is eligible only when every legacy shard
         * has an FPQ2 peer.  A partial conversion must preserve the known-good
         * legacy set instead of producing an incomplete model. */
        if (legacy_count > 0 && legacy_count != n_paths) {
            for (size_t i = 0; i < n_paths; i++) free(paths[i]);
            free(paths);
            paths = NULL;
            n_paths = 0;
        }
    }
    if (n_paths == 0) {
        rewinddir(d);
        while ((ent = readdir(d)) != NULL) {
            if (!ends_with_local(ent->d_name, ".fpq")) continue;
            char full[PATH_MAX]; snprintf(full, sizeof(full), "%s/%s", parts_dir, ent->d_name);
            char **g = (char **)realloc(paths, (n_paths + 1) * sizeof(char *)); if (!g) { closedir(d); return -1; }
            paths = g; paths[n_paths++] = strdup(full);
        }
    }
    closedir(d); qsort(paths, n_paths, sizeof(char *), path_cmp);
    int rc = 0;
    for (size_t i = 0; i < n_paths; i++) {
        if (index_one_native_file(paths[i], out, n_out,
                                  tied_embedding_tensor, preload_tied_embeddings) != 0) rc = -1;
        free(paths[i]);
        if (rc) break;
    }
    free(paths); return rc;
}

static int read_fp16_at(const native_tensor_t *t, uint64_t off, float *out) {
    uint16_t hv = 0; if (read_at(t->path, off, &hv, 2) != 0) return -1; *out = fp16_to_float_local(hv); return 0;
}

static int read_at_fd(int fd, uint64_t off, void *dst, size_t n) {
    ssize_t got = pread(fd, dst, n, (off_t)off);
    return got == (ssize_t)n ? 0 : -1;
}

static int read_fp16_from_fd(int fd, uint64_t off, float *out) {
    uint16_t hv = 0;
    if (read_at_fd(fd, off, &hv, 2) != 0) return -1;
    *out = fp16_to_float_local(hv);
    return 0;
}

static int read_fp16_from_file(FILE *fp, uint64_t off, float *out) {
    uint16_t hv = 0;
    if (fseek(fp, (long)off, SEEK_SET) != 0) return -1;
    if (read_exact(fp, &hv, 2) != 0) return -1;
    *out = fp16_to_float_local(hv);
    return 0;
}
static int read_haar_seed(native_tensor_t *t, uint64_t *seed_out) {
    if (t->haar_seed_loaded) { *seed_out = t->haar_seed_cache; return 0; }
    if (read_at(t->path, t->off_haar, &t->haar_seed_cache, 8) != 0) return -1;
    t->haar_seed_loaded = 1; *seed_out = t->haar_seed_cache; return 0;
}

static float warp_inverse_local(float y, float beta) {
    /* Must be the inverse of v7_warp_forward in v4_optimizations.c:
     * sign(x) * log(1 + beta * |x|) / log(1 + beta).
     * The previous cubic Newton solve inverted an unrelated warp and made
     * every persisted native residual diverge from its encoder value. */
    float magnitude = expm1f(fabsf(y) * log1pf(beta)) / beta;
    return y < 0.0f ? -magnitude : magnitude;
}

static void warp_inverse_block_local(const float corrected[256], float lattice_scale,
                                     float warp_norm, float coord_scale, float beta,
                                     float out[256]) {
    for (int i = 0; i < 256; i++) {
        float lat_val = corrected[i] / lattice_scale * warp_norm;
        float unwarp = warp_inverse_local(lat_val, beta);
        out[i] = unwarp * coord_scale;
    }
}

static int decode_residual_block_fd(native_tensor_t *t, int fd, size_t b, float block[256]) {
    if (!t || t->kind != NT_V9 || b >= t->n_blocks) return -1;
    if (getenv("BONFYRE_NATIVE_LR_ONLY")) { memset(block, 0, 256 * sizeof(float)); return 0; }
    if (!t->tile_cb_cache && load_tile_codebook(t) != 0) return -1;
    float coord_scale = 0, warp_norm = 1, rn_scale = 0;
    uint8_t rn_q = 0;
    uint64_t qjl_bits = 0, seed = 0;
    float beta = 8.0f;
    float lattice_scale = 8.0f * (float)t->coord_bits;
    if (read_fp16_from_fd(fd, t->off_coord_scales + (uint64_t)b * 2ull, &coord_scale) != 0 ||
        read_fp16_from_fd(fd, t->off_warp_norms + (uint64_t)b * 2ull, &warp_norm) != 0 ||
        read_fp16_from_fd(fd, t->off_rn_scale, &rn_scale) != 0 ||
        read_at_fd(fd, t->off_rn_q + (uint64_t)b, &rn_q, 1) != 0 ||
        read_at_fd(fd, t->off_qjl + (uint64_t)b * 8ull, &qjl_bits, 8) != 0) {
        return -1;
    }
    if (read_haar_seed(t, &seed) != 0) seed = 0x12345678ull;
    int8_t e8[256];
    uint8_t idx[16];
    if (read_at_fd(fd, t->off_e8 + (uint64_t)b * 256ull, e8, sizeof(e8)) != 0 ||
        read_at_fd(fd, t->off_tile_idx + (uint64_t)b * 16ull, idx, sizeof(idx)) != 0) {
        return -1;
    }
    float corrected[256];
    for (int i = 0; i < 256; i++) corrected[i] = (float)e8[i];
    for (int tile = 0; tile < 16; tile++) {
        uint8_t code = idx[tile]; if (code >= t->effective_k) code = 0;
        for (int j = 0; j < 16; j++) {
            float v = t->tile_cb_cache ? t->tile_cb_cache[(size_t)code * 16u + (size_t)j] : 0.0f;
            corrected[tile * 16 + j] += v;
        }
    }
    float z[256];
    warp_inverse_block_local(corrected, lattice_scale, warp_norm, coord_scale, beta, z);
    float residual_norm = rn_scale * (float)rn_q;
    if (residual_norm > 1e-20f && qjl_bits) {
        /* The 64 persisted bits are signs of 64 seeded 256-D projections,
         * not 64 coordinate values.  Reconstruct in that same projection
         * basis; applying them to z[0..63] corrupts every compressed block. */
        fpq_qjl_t qjl = {
            .n_projections = FPQ_QJL_PROJECTIONS,
            .n_elements = 256,
            .proj_seed = seed ^ (uint64_t)b ^ 0xC00DULL,
            .bits = &qjl_bits
        };
        float qjl_residual[256];
        fpq_qjl_reconstruct(&qjl, residual_norm, qjl_residual);
        for (int i = 0; i < 256; i++) z[i] += qjl_residual[i];
    }
    fpq_fwht_inverse(z, 256); fpq_random_signs_inverse(z, 256, seed ^ (uint64_t)b);
    for (int i = 0; i < 256; i++) block[i] = z[i];
    return 0;
}

fpq_model_t *fpq_open_with_tied_embedding(const char *path,
                                          const char *tied_embedding_tensor,
                                          int preload_tied_embeddings) {
    if (!path) return NULL;
    fpq_model_t *m = (fpq_model_t *)calloc(1, sizeof(fpq_model_t)); if (!m) return NULL;
    m->path = strdup(path);
    if (load_native_paths(path, &m->tensors, &m->n_tensors,
                          tied_embedding_tensor, preload_tied_embeddings) != 0 ||
        m->n_tensors == 0) { fprintf(stderr, "fpq_open lazy-native: failed to load %s\n", path); fpq_close(m); return NULL; }
    size_t n_sli = 0, n_pass = 0, total = 0;
    for (size_t i = 0; i < m->n_tensors; i++) { native_tensor_t *t = &m->tensors[i]; total += (size_t)t->rows * (size_t)t->cols; if (t->kind == NT_V9) n_sli++; else n_pass++; }
    m->cached_info.n_tensors = m->n_tensors; m->cached_info.n_sli_tensors = n_sli; m->cached_info.n_passthrough = n_pass; m->cached_info.total_params = total; m->cached_info.format_version = FPQ_NATIVE_VERSION_EXPECTED;
    fprintf(stderr, "fpq_open lazy-native: %s — %zu tensors (%zu native-matvec, %zu passthrough), %zuM params\n", path, m->n_tensors, n_sli, n_pass, total / 1000000);
    return m;
}

fpq_model_t *fpq_open(const char *path) {
    return fpq_open_with_tied_embedding(
        path,
        fpq_tied_embedding_tensor_name(),
        fpq_truthy_env_local("BONFYRE_QWEN_PRELOAD_TIED_EMBEDDINGS"));
}

void fpq_close(fpq_model_t *m) {
    if (!m) return;
    for (size_t i = 0; i < m->n_tensors; i++) {
        free(m->tensors[i].name);
        free(m->tensors[i].path);
        free(m->tensors[i].passthrough);
        free(m->tensors[i].tile_cb_cache);
        bonfyre_tensor_destroy(&m->tensors[i]);
    }
    free(m->tensors); free(m->path); free(m);
}

int fpq_decode_row_impl(fpq_model_t *m, const char *tensor_name, size_t row, float *out) {
    native_tensor_t *t = find_tensor(m, tensor_name); if (!t || !out || row >= t->rows) return -1;
    if (t->kind == NT_FP16) {
        if (t->passthrough) {
            memcpy(out, t->passthrough + row * (size_t)t->cols, (size_t)t->cols * sizeof(float));
            return 0;
        }
        FILE *fp = open_seek(t->path, t->data_offset + (uint64_t)row * (uint64_t)t->cols * 2ull);
        if (!fp) return -1;
        for (size_t c = 0; c < t->cols; c++) {
            uint16_t hv = 0;
            if (read_exact(fp, &hv, 2) != 0) { fclose(fp); return -1; }
            out[c] = fp16_to_float_local(hv);
        }
        fclose(fp);
        return 0;
    }
    memset(out, 0, (size_t)t->cols * sizeof(float));
    FILE *fp = fopen(t->path, "rb"); if (!fp) return -1;
    float *urow = (float *)calloc(t->lr_rank ? t->lr_rank : 1, sizeof(float)); if (!urow) { fclose(fp); return -1; }
    for (size_t r = 0; r < t->lr_rank; r++) {
        uint64_t uoff = t->off_us + (uint64_t)r * (2ull + (uint64_t)t->rows);
        if (fseek(fp, (long)uoff, SEEK_SET) != 0) continue; uint16_t sh = 0; if (read_exact(fp, &sh, 2) != 0) continue; float sc = fp16_to_float_local(sh);
        if (fseek(fp, (long)(uoff + 2ull + (uint64_t)row), SEEK_SET) != 0) continue; int8_t q = 0; if (read_exact(fp, &q, 1) == 0) urow[r] = (float)q * sc;
    }
    for (size_t r = 0; r < t->lr_rank; r++) {
        if (fabsf(urow[r]) < 1e-30f) continue;
        uint64_t voff = t->off_vt + (uint64_t)r * (2ull + (uint64_t)t->cols);
        if (fseek(fp, (long)voff, SEEK_SET) != 0) continue; uint16_t sh = 0; if (read_exact(fp, &sh, 2) != 0) continue; float sc = fp16_to_float_local(sh);
        for (size_t c = 0; c < t->cols; c++) { int8_t q = 0; if (read_exact(fp, &q, 1) != 0) break; out[c] += urow[r] * (float)q * sc; }
    }
    free(urow); fclose(fp);
    if (!getenv("BONFYRE_NATIVE_LR_ONLY")) {
        int fd = open(t->path, O_RDONLY);
        if (fd < 0) return -1;
        size_t start = row * (size_t)t->cols, end = start + (size_t)t->cols, b0 = start / 256u, b1 = (end + 255u) / 256u;
        for (size_t b = b0; b < b1 && b < t->n_blocks; b++) { float block[256]; if (decode_residual_block_fd(t, fd, b, block) != 0) continue; size_t base = b * 256u; for (int k = 0; k < 256; k++) { size_t flat = base + (size_t)k; if (flat >= start && flat < end) out[flat - start] += block[k]; } }
        close(fd);
    }
    return 0;
}

int fpq_matmul(fpq_model_t *m, const char *tensor_name, const float *x, float *y) {
    native_tensor_t *t = find_tensor(m, tensor_name); if (!t || !x || !y) { fprintf(stderr, "fpq_matmul lazy-native: tensor '%s' not found\n", tensor_name ? tensor_name : "(null)"); return -1; }
    memset(y, 0, (size_t)t->rows * sizeof(float));
    if (t->kind == NT_FP16) {
        if (t->passthrough) {
#ifdef __APPLE__
            cblas_sgemv(CblasRowMajor,
                        CblasNoTrans,
                        (int)t->rows,
                        (int)t->cols,
                        1.0f,
                        t->passthrough,
                        (int)t->cols,
                        x,
                        1,
                        0.0f,
                        y,
                        1);
            return 0;
#else
            for (size_t r = 0; r < t->rows; r++) { float acc = 0; const float *w = t->passthrough + r * (size_t)t->cols; for (size_t c = 0; c < t->cols; c++) acc += w[c] * x[c]; y[r] = acc; }
            return 0;
#endif
        }
        /* Large FP16 tensors such as tied token embeddings intentionally stay
         * off the resident heap.  Decode them sequentially from their native
         * payload instead of reopening the pack once per vocabulary row. */
        uint16_t *rowbuf = (uint16_t *)malloc((size_t)t->cols * sizeof(uint16_t));
        FILE *fp = rowbuf ? open_seek(t->path, t->data_offset) : NULL;
        if (!fp) { free(rowbuf); return -1; }
        for (size_t r = 0; r < t->rows; r++) {
            if (read_exact(fp, rowbuf, (size_t)t->cols * sizeof(uint16_t)) != 0) {
                fclose(fp);
                free(rowbuf);
                return -1;
            }
            float acc = 0.0f;
            for (size_t c = 0; c < t->cols; c++) acc += fp16_to_float_local(rowbuf[c]) * x[c];
            y[r] = acc;
        }
        fclose(fp);
        free(rowbuf);
        return 0;
    }
    if (fpq_active_lr_path_enabled() && m && m->active_cache && t->lr_rank > 0) {
        fpq_active_cache_t *cache = (fpq_active_cache_t *)m->active_cache;
        fpq_active_entry_t *entry = fpq_active_cache_get(
            cache,
            t->name,
            t->rows,
            t->cols,
            t->path,
            t->data_offset,
            "v9-lr-active",
            FPQ_NATIVE_VERSION_EXPECTED);
        if (entry) {
            if (!entry->ready) {
                (void)fpq_active_materialize_v9_file(entry,
                                                     t->path,
                                                     t->off_us,
                                                     t->off_vt,
                                                     t->lr_rank,
                                                     t->off_ghost,
                                                     t->has_ghost);
            }
            if (entry->ready) {
                struct timespec mv_t0 = {0};
                struct timespec mv_t1 = {0};
                int mv_rc = -1;
                double mv_sec = 0.0;
                clock_gettime(CLOCK_MONOTONIC, &mv_t0);
                mv_rc = fpq_active_matvec(entry, x, y);
                clock_gettime(CLOCK_MONOTONIC, &mv_t1);
                mv_sec = (double)(mv_t1.tv_sec - mv_t0.tv_sec) +
                         (double)(mv_t1.tv_nsec - mv_t0.tv_nsec) / 1000000000.0;
                if (mv_rc == 0) {
                    if (fpq_prepare_breakdown_enabled()) {
                        fpq_prepare_breakdown_emit("active_lr_matvec_hit",
                                                   t->name,
                                                   mv_sec,
                                                   "\"returned_before_sli\":true");
                    }
                    return 0;
                }
            }
        }
    }
    if (fpq_active_lr_path_enabled() &&
        fpq_truthy_env_local("BONFYRE_NATIVE_LR_ONLY") &&
        t->lr_rank > 0) {
        if (fpq_prepare_breakdown_enabled()) {
            fpq_prepare_breakdown_emit("active_lr_native_only_blocked_sli",
                                       t->name,
                                       0.0,
                                       "\"native_lr_only\":true,\"blocked_sli\":true,\"rank_positive\":true");
        }
        fprintf(stderr,
                "fpq_matmul: active LR miss in BONFYRE_NATIVE_LR_ONLY tensor=%s before_sli=true rank=%u\n",
                t->name ? t->name : "(null)",
                (unsigned)t->lr_rank);
        return -1;
    }

    if (fpq_active_lr_path_enabled() &&
        fpq_truthy_env_local("BONFYRE_NATIVE_LR_ONLY") &&
        t->lr_rank == 0 &&
        t->rows >= 256u &&
        t->cols >= 256u &&
        !getenv("FPQ_DENSE_FALLBACK")) {
        if (fpq_prepare_breakdown_enabled()) {
            fpq_prepare_breakdown_emit("stage4b_rankzero_sli_allowed",
                                       t->name,
                                       0.0,
                                       "\"native_lr_only\":true,\"rank_zero\":true,\"resident_sli_fallback\":true");
        }
    }

    if (t->rows >= 256u && t->cols >= 256u && native_sli_layout_safe(t) &&
        !getenv("FPQ_DENSE_FALLBACK")) {
        if (!t->sli && ensure_sli_ready(t) != 0) {
            fprintf(stderr, "fpq_matmul lazy-native: SLI prepare failed for tensor '%s'\n", tensor_name ? tensor_name : "(null)");
            return -1;
        }
        return fpqx_sli_matvec(t->sli, x, y);
    }
    FILE *fp = fopen(t->path, "rb"); if (!fp) return -1;
    float *tmp = (float *)calloc(t->lr_rank ? t->lr_rank : 1, sizeof(float)); if (!tmp) { fclose(fp); return -1; }
    for (size_t r = 0; r < t->lr_rank; r++) {
        uint64_t voff = t->off_vt + (uint64_t)r * (2ull + (uint64_t)t->cols);
        if (fseek(fp, (long)voff, SEEK_SET) != 0) continue; uint16_t sh = 0; if (read_exact(fp, &sh, 2) != 0) continue; float sc = fp16_to_float_local(sh);
        float acc = 0; for (size_t c = 0; c < t->cols; c++) { int8_t q = 0; if (read_exact(fp, &q, 1) != 0) break; acc += ((float)q * sc) * x[c]; } tmp[r] = acc;
    }
    for (size_t r = 0; r < t->lr_rank; r++) {
        if (fabsf(tmp[r]) < 1e-30f) continue;
        uint64_t uoff = t->off_us + (uint64_t)r * (2ull + (uint64_t)t->rows);
        if (fseek(fp, (long)uoff, SEEK_SET) != 0) continue; uint16_t sh = 0; if (read_exact(fp, &sh, 2) != 0) continue; float sc = fp16_to_float_local(sh);
        for (size_t row = 0; row < t->rows; row++) { int8_t q = 0; if (read_exact(fp, &q, 1) != 0) break; y[row] += ((float)q * sc) * tmp[r]; }
    }
    free(tmp); fclose(fp);
    if (!getenv("BONFYRE_NATIVE_LR_ONLY")) {
        int fd = open(t->path, O_RDONLY);
        if (fd < 0) return -1;
        for (size_t b = 0; b < t->n_blocks; b++) { float block[256]; if (decode_residual_block_fd(t, fd, b, block) != 0) continue; size_t base = b * 256u; for (int k = 0; k < 256; k++) { size_t flat = base + (size_t)k; size_t row = flat / (size_t)t->cols, col = flat - row * (size_t)t->cols; if (row < t->rows && col < t->cols) y[row] += block[k] * x[col]; } }
        close(fd);
    }
    if (t->has_ghost) {
        float sigma = 0, u_sc = 0, v_sc = 0; uint16_t h = 0; uint64_t off = t->off_ghost;
        if (read_at(t->path, off, &h, 2) == 0) sigma = fp16_to_float_local(h); off += 2;
        if (read_at(t->path, off, &h, 2) == 0) u_sc = fp16_to_float_local(h); off += 2; uint64_t u_off = off; off += (uint64_t)t->rows;
        if (read_at(t->path, off, &h, 2) == 0) v_sc = fp16_to_float_local(h); off += 2; uint64_t v_off = off;
        float gv = 0; FILE *vfp = open_seek(t->path, v_off); if (vfp) { for (size_t c = 0; c < t->cols; c++) { int8_t q = 0; if (read_exact(vfp, &q, 1) != 0) break; gv += ((float)q * v_sc) * x[c]; } fclose(vfp); }
        FILE *ufp = open_seek(t->path, u_off); if (ufp) { for (size_t row = 0; row < t->rows; row++) { int8_t q = 0; if (read_exact(ufp, &q, 1) != 0) break; y[row] += sigma * ((float)q * u_sc) * gv; } fclose(ufp); }
    }
    return 0;
}

int fpq_native_block_reference(fpq_model_t *m, const char *tensor_name,
                               size_t row, const float *x,
                               size_t block_index,
                               float *out_block_score,
                               float *out_row_total,
                               size_t *out_col_start,
                               size_t *out_col_end) {
    native_tensor_t *t = find_tensor(m, tensor_name);
    float *wrow = NULL;
    float block_acc = 0.0f;
    float row_acc = 0.0f;
    size_t col_start = 0;
    size_t col_end = 0;

    if (!t || !x || !out_block_score || !out_row_total) return -1;
    if (row >= t->rows) return -1;

    wrow = (float *)malloc((size_t)t->cols * sizeof(float));
    if (!wrow) return -1;
    if (fpq_decode_row_impl(m, tensor_name, row, wrow) != 0) {
        free(wrow);
        return -1;
    }

    col_start = block_index * 256u;
    if (col_start >= (size_t)t->cols) {
        free(wrow);
        return -1;
    }
    col_end = col_start + 256u;
    if (col_end > (size_t)t->cols) col_end = (size_t)t->cols;

    for (size_t c = 0; c < (size_t)t->cols; c++) {
        float contrib = wrow[c] * x[c];
        row_acc += contrib;
        if (c >= col_start && c < col_end) block_acc += contrib;
    }
    free(wrow);

    *out_block_score = block_acc;
    *out_row_total = row_acc;
    if (out_col_start) *out_col_start = col_start;
    if (out_col_end) *out_col_end = col_end;
    return 0;
}

int fpq_native_residual_block_reference(fpq_model_t *m, const char *tensor_name,
                                        size_t row, const float *x,
                                        size_t block_index,
                                        float *out_block_score,
                                        size_t *out_col_start,
                                        size_t *out_col_end) {
    native_tensor_t *t = find_tensor(m, tensor_name);
    int fd = -1;
    float block[256];
    float block_acc = 0.0f;
    size_t col_start = 0;
    size_t col_end = 0;
    size_t flat_start = 0;
    size_t global_block = 0;

    if (!t || !x || !out_block_score) return -1;
    if (row >= t->rows) return -1;

    col_start = block_index * 256u;
    if (col_start >= (size_t)t->cols) return -1;
    col_end = col_start + 256u;
    if (col_end > (size_t)t->cols) col_end = (size_t)t->cols;

    flat_start = row * (size_t)t->cols + col_start;
    global_block = flat_start / 256u;
    if (global_block >= t->n_blocks) return -1;

    fd = open(t->path, O_RDONLY);
    if (fd < 0) return -1;
    if (decode_residual_block_fd(t, fd, global_block, block) != 0) {
        close(fd);
        return -1;
    }
    close(fd);

    for (size_t c = col_start; c < col_end; c++) {
        size_t local = c - col_start;
        block_acc += block[local] * x[c];
    }

    *out_block_score = block_acc;
    if (out_col_start) *out_col_start = col_start;
    if (out_col_end) *out_col_end = col_end;
    return 0;
}



static void fpq_shared_active_diag(const char *reason,
                                   const char *tensor,
                                   size_t tensor_index,
                                   size_t n_tensors,
                                   uint32_t rows,
                                   uint32_t cols,
                                   uint32_t lr_rank) {
    char extra[384];
    snprintf(extra,
             sizeof(extra),
             "\"reason\":\"%s\",\"tensor_index\":%zu,\"n_tensors\":%zu,"
             "\"rows\":%u,\"cols\":%u,\"lr_rank\":%u",
             reason ? reason : "unknown",
             tensor_index,
             n_tensors,
             rows,
             cols,
             lr_rank);

    if (fpq_prepare_breakdown_enabled()) {
        fpq_prepare_breakdown_emit("active_lr_shared_fail",
                                   tensor ? tensor : "(null)",
                                   0.0,
                                   extra);
    }

    if (fpq_truthy_env_local("BONFYRE_QWEN_LOG_ACTIVE_SHARED_FAIL")) {
        fprintf(stderr,
                "fpq_matmul_shared_active_lr: fail reason=%s tensor_index=%zu n_tensors=%zu tensor=%s rows=%u cols=%u lr_rank=%u\n",
                reason ? reason : "unknown",
                tensor_index,
                n_tensors,
                tensor ? tensor : "(null)",
                rows,
                cols,
                lr_rank);
        fflush(stderr);
    }
}



static int fpq_shared_active_direct_matvec(fpq_active_entry_t *entry,
                                           uint16_t expected_lr_rank,
                                           const float *x,
                                           float *y,
                                           char *reason,
                                           size_t reason_size) {
    if (reason && reason_size) reason[0] = '\0';

#define BF_SHARED_DIRECT_REASON(msg) do { \
        if (reason && reason_size) snprintf(reason, reason_size, "%s", (msg)); \
    } while (0)

    if (!entry) {
        BF_SHARED_DIRECT_REASON("direct_entry_null");
        return -1;
    }
    if (!x) {
        BF_SHARED_DIRECT_REASON("direct_x_null");
        return -1;
    }
    if (!y) {
        BF_SHARED_DIRECT_REASON("direct_y_null");
        return -1;
    }
    if (entry->disabled) {
        BF_SHARED_DIRECT_REASON("direct_entry_disabled");
        return -1;
    }
    if (!entry->loaded) {
        BF_SHARED_DIRECT_REASON("direct_entry_not_loaded");
        return -1;
    }
    /*
     * Persistent/reused active entries can carry stale entry->lr_rank even when
     * the native tensor metadata is correct. Trust the native tensor rank for
     * this call, then let the payload checks below report the real missing field.
     */
    if (expected_lr_rank > 0 && entry->lr_rank != expected_lr_rank) {
        entry->lr_rank = expected_lr_rank;
    }

    if (entry->lr_rank == 0) {
        BF_SHARED_DIRECT_REASON("direct_lr_rank_zero");
        return -1;
    }
    if (!entry->active_u_q) {
        BF_SHARED_DIRECT_REASON("direct_missing_active_u_q");
        return -1;
    }
    if (!entry->active_vt_q) {
        BF_SHARED_DIRECT_REASON("direct_missing_active_vt_q");
        return -1;
    }
    if (!entry->active_u_scales) {
        BF_SHARED_DIRECT_REASON("direct_missing_active_u_scales");
        return -1;
    }
    if (!entry->active_vt_scales) {
        BF_SHARED_DIRECT_REASON("direct_missing_active_vt_scales");
        return -1;
    }

    /*
     * Some persistent/reused active entries have complete payload pointers but
     * stale ready/data/payload bookkeeping. Finalize locally before rejecting.
     */
    if (!entry->ready) {
        size_t rank_fix = (size_t)entry->lr_rank;
        size_t u_count_fix = rank_fix * (size_t)entry->rows;
        size_t vt_count_fix = rank_fix * (size_t)entry->cols;

        entry->qweight_size = u_count_fix + vt_count_fix;
        entry->scales_size = rank_fix * 2u * sizeof(float);
        if (entry->has_ghost) {
            entry->scales_size += 3u * sizeof(float);
        }

        entry->total_bytes = entry->qweight_size + entry->scales_size;
        if (entry->has_ghost) {
            entry->total_bytes += (size_t)entry->rows + (size_t)entry->cols;
        }

        entry->payload_bytes = entry->total_bytes;
        entry->data = entry->active_u_q;
        entry->ready = (entry->data != NULL &&
                        entry->payload_bytes > 0 &&
                        entry->active_vt_q != NULL &&
                        entry->active_u_scales != NULL &&
                        entry->active_vt_scales != NULL);
    }

    if (!entry->ready) {
        BF_SHARED_DIRECT_REASON("direct_entry_not_ready");
        return -1;
    }

    if (entry->rows == 0 || entry->cols == 0) {
        BF_SHARED_DIRECT_REASON("direct_bad_shape");
        return -1;
    }

    memset(y, 0, (size_t)entry->rows * sizeof(float));

    size_t rank = (size_t)entry->lr_rank;
    float tmp_stack[64];
    float *tmp = NULL;

    if (rank <= (sizeof(tmp_stack) / sizeof(tmp_stack[0]))) {
        tmp = tmp_stack;
        memset(tmp_stack, 0, sizeof(tmp_stack));
    } else {
        tmp = (float *)calloc(rank, sizeof(float));
        if (!tmp) {
            BF_SHARED_DIRECT_REASON("direct_tmp_alloc_failed");
            return -1;
        }
    }

    for (size_t r = 0; r < rank; r++) {
        const int8_t *vt = entry->active_vt_q + r * (size_t)entry->cols;
        tmp[r] = fpq_i8_f32_dot_neon(vt, x, entry->cols) * entry->active_vt_scales[r];
    }

    for (size_t r = 0; r < rank; r++) {
        const int8_t *u = entry->active_u_q + r * (size_t)entry->rows;
        float coeff = tmp[r] * entry->active_u_scales[r];
        if (fabsf(coeff) < 1e-30f) continue;
        fpq_i8_f32_axpy_neon(u, coeff, y, entry->rows);
    }

    if (entry->has_ghost) {
        if (!entry->ghost_u_q || !entry->ghost_v_q) {
            if (tmp != tmp_stack) free(tmp);
            BF_SHARED_DIRECT_REASON("direct_missing_ghost_payload");
            return -1;
        }

        float gv = fpq_i8_f32_dot_neon(entry->ghost_v_q, x, entry->cols) * entry->ghost_v_scale;
        float coeff = entry->ghost_sigma * gv * entry->ghost_u_scale;
        if (fabsf(coeff) >= 1e-30f) {
            fpq_i8_f32_axpy_neon(entry->ghost_u_q, coeff, y, entry->rows);
        }
    }

    if (tmp != tmp_stack) free(tmp);
    BF_SHARED_DIRECT_REASON("ok");
    return 0;

#undef BF_SHARED_DIRECT_REASON
}


static int fpq_matmul_shared_active_lr(fpq_model_t *m,
                                       size_t n_tensors,
                                       const char *const *tensor_names,
                                       const float *x,
                                       float **outputs) {
    native_tensor_t *ts[3] = {0};
    fpq_active_entry_t *entries[3] = {0};
    fpq_active_cache_t *cache = NULL;
    uint32_t cols0 = 0;
    struct timespec t0 = {0};
    struct timespec t1 = {0};
    double sec = 0.0;
    char extra[256];

    if (!m || !tensor_names || !x || !outputs) {
        fpq_shared_active_diag("bad_args", NULL, 0, n_tensors, 0, 0, 0);
        return -1;
    }
    if (!m->active_cache) {
        fpq_shared_active_diag("no_active_cache", NULL, 0, n_tensors, 0, 0, 0);
        return -1;
    }
    if (n_tensors < 2 || n_tensors > 3) {
        fpq_shared_active_diag("bad_tensor_count", NULL, 0, n_tensors, 0, 0, 0);
        return -1;
    }

    cache = (fpq_active_cache_t *)m->active_cache;

    for (size_t i = 0; i < n_tensors; i++) {
        native_tensor_t *t = find_tensor(m, tensor_names[i]);
        if (!t) {
            fpq_shared_active_diag("tensor_missing",
                                   tensor_names && tensor_names[i] ? tensor_names[i] : NULL,
                                   i,
                                   n_tensors,
                                   0,
                                   0,
                                   0);
            return -1;
        }
        if (!outputs[i]) {
            fpq_shared_active_diag("output_missing", t->name, i, n_tensors, t->rows, t->cols, t->lr_rank);
            return -1;
        }
        if (t->lr_rank == 0) {
            fpq_shared_active_diag("lr_rank_zero", t->name, i, n_tensors, t->rows, t->cols, t->lr_rank);

            /*
             * Stage 4E quality bridge:
             * Keep the shared group in the active path. For rank-zero tensors,
             * compute only that tensor through fpq_matmul() once, instead of
             * returning -2 and forcing the entire K/V or gate/up group through
             * the slow fallback path.
             */
            if (fpq_truthy_env_local("BONFYRE_QWEN_RANKZERO_DIRECT_SLI") && outputs[i]) {
                struct timespec rz_t0 = {0};
                struct timespec rz_t1 = {0};
                int rz_rc = 0;
                if (fpq_prepare_breakdown_enabled()) {
                    fpq_prepare_breakdown_emit("stage4f_rankzero_direct_sli_enter",
                                               t->name,
                                               0.0,
                                               "\"rank_zero\":true,\"direct_sli\":true");
                }
                clock_gettime(CLOCK_MONOTONIC, &rz_t0);

                if (t->kind != NT_V9 || t->rows < 256u || t->cols < 256u ||
                    getenv("FPQ_DENSE_FALLBACK")) {
                    rz_rc = -2;
                } else {
                    if (!t->sli && ensure_sli_ready(t) != 0) {
                        rz_rc = -1;
                    } else {
                        rz_rc = fpqx_sli_matvec(t->sli, x, outputs[i]);
                    }
                }

                clock_gettime(CLOCK_MONOTONIC, &rz_t1);

                if (fpq_prepare_breakdown_enabled()) {
                    char rz_extra[320];
                    snprintf(rz_extra,
                             sizeof(rz_extra),
                             "\"rank_zero\":true,\"direct_sli\":true,\"rc\":%d,\"has_sli\":%s",
                             rz_rc,
                             t->sli ? "true" : "false");
                    fpq_prepare_breakdown_emit("stage4f_rankzero_direct_sli",
                                               t->name,
                                               fpq_prepare_elapsed_seconds_local(&rz_t0, &rz_t1),
                                               rz_extra);
                }

                if (rz_rc != 0) {
                    fpq_shared_active_diag("rankzero_direct_sli_failed",
                                           t->name,
                                           i,
                                           n_tensors,
                                           t->rows,
                                           t->cols,
                                           t->lr_rank);
                    return -2;
                }

                ts[i] = t;
                entries[i] = NULL;
                continue;
            }

            /*
             * REDLINE lane from Stage 4C: speed ceiling only, not final quality.
             */
            if (fpq_truthy_env_local("BONFYRE_QWEN_RANKZERO_ZERO_OUT") && outputs[i]) {
                memset(outputs[i], 0, (size_t)t->rows * sizeof(float));
                if (fpq_prepare_breakdown_enabled()) {
                    fpq_prepare_breakdown_emit("stage4c_rankzero_zero_out",
                                               t->name,
                                               0.0,
                                               "\"rank_zero\":true,\"zero_out\":true,\"redline\":true");
                }
                ts[i] = t;
                entries[i] = NULL;
                continue;
            }

            return -2; /* expected unsupported shared-active path; allow fallback under active-only guard */
        }

        if (i == 0) {
            cols0 = t->cols;
        } else if (t->cols != cols0) {
            fpq_shared_active_diag("cols_mismatch", t->name, i, n_tensors, t->rows, t->cols, t->lr_rank);
            return -1;
        }

        fpq_active_entry_t *entry = fpq_active_cache_get(
            cache,
            t->name,
            t->rows,
            t->cols,
            t->path,
            t->data_offset,
            "v9-lr-active",
            FPQ_NATIVE_VERSION_EXPECTED);

        if (!entry) {
            fpq_shared_active_diag("cache_get_failed", t->name, i, n_tensors, t->rows, t->cols, t->lr_rank);
            return -1;
        }

        /*
         * Do not trust entry->ready alone. Persistent/reused entries can be marked
         * ready while the typed active payload pointers are missing/stale. For
         * active-capable tensors, force materialization whenever payload fields
         * are incomplete or rank bookkeeping disagrees with native metadata.
         */
        int active_payload_missing =
            (t->lr_rank > 0 &&
             (!entry->active_u_q ||
              !entry->active_vt_q ||
              !entry->active_u_scales ||
              !entry->active_vt_scales ||
              entry->lr_rank == 0 ||
              entry->lr_rank != t->lr_rank));

        if (!entry->ready || active_payload_missing) {
            if (fpq_prepare_breakdown_enabled()) {
                char repair_extra[320];
                snprintf(repair_extra,
                         sizeof(repair_extra),
                         "\"ready_before\":%s,\"payload_missing\":%s,"
                         "\"entry_rank\":%u,\"tensor_rank\":%u,"
                         "\"has_u\":%s,\"has_vt\":%s,\"has_us\":%s,\"has_vts\":%s",
                         entry->ready ? "true" : "false",
                         active_payload_missing ? "true" : "false",
                         (unsigned)entry->lr_rank,
                         (unsigned)t->lr_rank,
                         entry->active_u_q ? "true" : "false",
                         entry->active_vt_q ? "true" : "false",
                         entry->active_u_scales ? "true" : "false",
                         entry->active_vt_scales ? "true" : "false");
                fpq_prepare_breakdown_emit("active_lr_shared_payload_repair",
                                           t->name,
                                           0.0,
                                           repair_extra);
            }

            (void)fpq_active_materialize_v9_file(entry,
                                                 t->path,
                                                 t->off_us,
                                                 t->off_vt,
                                                 t->lr_rank,
                                                 t->off_ghost,
                                                 t->has_ghost);
        }

        if (!entry->ready) {
            fpq_shared_active_diag("materialize_failed", t->name, i, n_tensors, t->rows, t->cols, t->lr_rank);
            return -1;
        }

        if (t->lr_rank > 0 &&
            (!entry->active_u_q ||
             !entry->active_vt_q ||
             !entry->active_u_scales ||
             !entry->active_vt_scales)) {
            fpq_shared_active_diag("active_payload_missing_after_materialize",
                                   t->name,
                                   i,
                                   n_tensors,
                                   t->rows,
                                   t->cols,
                                   t->lr_rank);
            return -1;
        }

        ts[i] = t;
        entries[i] = entry;
    }

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (size_t i = 0; i < n_tensors; i++) {
        if ((fpq_truthy_env_local("BONFYRE_QWEN_RANKZERO_DIRECT_SLI") ||
             fpq_truthy_env_local("BONFYRE_QWEN_RANKZERO_SINGLE_MATMUL") ||
             fpq_truthy_env_local("BONFYRE_QWEN_RANKZERO_ZERO_OUT")) &&
            ts[i] && ts[i]->lr_rank == 0) {
            continue;
        }
        if (fpq_truthy_env_local("BONFYRE_QWEN_ACTIVE_SHARED_DIRECT")) {
            char direct_reason[96];
            if (fpq_shared_active_direct_matvec(entries[i],
                                                ts[i] ? ts[i]->lr_rank : 0,
                                                x,
                                                outputs[i],
                                                direct_reason,
                                                sizeof(direct_reason)) != 0) {
                fpq_shared_active_diag(direct_reason[0] ? direct_reason : "direct_matvec_failed",
                                       ts[i] && ts[i]->name ? ts[i]->name : NULL,
                                       i,
                                       n_tensors,
                                       ts[i] ? ts[i]->rows : 0,
                                       ts[i] ? ts[i]->cols : 0,
                                       ts[i] ? ts[i]->lr_rank : 0);
                return -1;
            }
        } else if (fpq_active_matvec(entries[i], x, outputs[i]) != 0) {
            fpq_shared_active_diag("matvec_failed",
                                   ts[i] && ts[i]->name ? ts[i]->name : NULL,
                                   i,
                                   n_tensors,
                                   ts[i] ? ts[i]->rows : 0,
                                   ts[i] ? ts[i]->cols : 0,
                                   ts[i] ? ts[i]->lr_rank : 0);
            return -1;
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    sec = (double)(t1.tv_sec - t0.tv_sec) +
          (double)(t1.tv_nsec - t0.tv_nsec) / 1000000000.0;

    if (fpq_prepare_breakdown_enabled()) {
        snprintf(extra, sizeof(extra),
                 "\"returned_before_sli\":true,\"shared\":true,\"n_tensors\":%zu,"
                 "\"tensor_b\":\"%s\",\"tensor_c\":\"%s\"",
                 n_tensors,
                 ts[1] && ts[1]->name ? ts[1]->name : "",
                 (n_tensors > 2 && ts[2] && ts[2]->name) ? ts[2]->name : "");

        fpq_prepare_breakdown_emit(n_tensors == 2
                                       ? "active_lr_shared_matvec_2"
                                       : "active_lr_shared_matvec_3",
                                   ts[0] && ts[0]->name ? ts[0]->name : "(null)",
                                   sec,
                                   extra);
    }

    return 0;
}


int fpq_matmul_shared(fpq_model_t *m,
                      size_t n_tensors,
                      const char *const *tensor_names,
                      const float *x,
                      float **outputs) {
    fpqx_sli_ctx_t **ctxs = NULL;
    fpqx_sli_shared_spectral_t *grp = NULL;
    size_t rows0 = 0, cols0 = 0;
    int rc = -1;
    if (!m || !tensor_names || !x || !outputs || n_tensors < 2) return -1;
    if (fpq_active_lr_path_enabled()) {
        int shared_rc = -1;
        if (fpq_truthy_env_local("BONFYRE_QWEN_ACTIVE_SHARED")) {
            shared_rc = fpq_matmul_shared_active_lr(m, n_tensors, tensor_names, x, outputs);
            if (shared_rc == 0) {
                return 0;
            }
        }

        /*
         * Active-only guard means: fail only when an active-capable tensor fails.
         * lr_rank_zero is an expected non-active tensor and may fall back.
         */
        if (shared_rc == -2 &&
            fpq_truthy_env_local("BONFYRE_QWEN_SHARED_FALLBACK_ABORT_ACTIVE_ONLY")) {
            goto fallback;
        }

        if (fpq_truthy_env_local("BONFYRE_QWEN_SHARED_FALLBACK_ABORT")) {
            fprintf(stderr,
                    "fpq_matmul_shared: active LR shared fallback blocked n_tensors=%zu rc=%d first=%s second=%s third=%s\n",
                    n_tensors,
                    shared_rc,
                    tensor_names && n_tensors > 0 && tensor_names[0] ? tensor_names[0] : "",
                    tensor_names && n_tensors > 1 && tensor_names[1] ? tensor_names[1] : "",
                    tensor_names && n_tensors > 2 && tensor_names[2] ? tensor_names[2] : "");
            fflush(stderr);
            return -1;
        }
        goto fallback;
    }
    ctxs = (fpqx_sli_ctx_t **)calloc(n_tensors, sizeof(*ctxs));
    if (!ctxs) return -1;
    for (size_t i = 0; i < n_tensors; i++) {
        native_tensor_t *t = find_tensor(m, tensor_names[i]);
        if (!t || !outputs[i]) goto fallback;
        if (t->kind != NT_V9 || t->rows < 256u || t->cols < 256u ||
            !native_sli_layout_safe(t) ||
            getenv("FPQ_DENSE_FALLBACK")) goto fallback;
        if (ensure_sli_ready(t) != 0 || !t->sli) goto fallback;
        if (i == 0) {
            rows0 = (size_t)t->rows;
            cols0 = (size_t)t->cols;
        } else if ((size_t)t->rows != rows0 || (size_t)t->cols != cols0) {
            goto fallback;
        }
        ctxs[i] = t->sli;
    }
    grp = fpqx_sli_group_qkv(ctxs, (int)n_tensors);
    if (!grp) goto fallback;
    rc = fpqx_sli_matvec_shared(grp, x, outputs);
    fpqx_sli_shared_free(grp);
    free(ctxs);
    return rc;

fallback:
    if (grp) fpqx_sli_shared_free(grp);
    free(ctxs);
    for (size_t i = 0; i < n_tensors; i++) {
        if (fpq_matmul(m, tensor_names[i], x, outputs[i]) != 0) return -1;
    }
    return 0;
}

int fpq_prepare_tensor(fpq_model_t *m, const char *tensor_name) {
    struct timespec t0 = {0};
    struct timespec t1 = {0};
    int log_prepare = 0;

    if (!m || !tensor_name) return -1;

    log_prepare = getenv("BONFYRE_QWEN_LOG_PREPARE_DETAIL") &&
                  strcmp(getenv("BONFYRE_QWEN_LOG_PREPARE_DETAIL"), "0") != 0;

    native_tensor_t *t = find_tensor(m, tensor_name);
    if (!t) {
        if (log_prepare) {
            fprintf(stderr, "fpq_prepare_tensor detail missing tensor=%s\n", tensor_name);
            fflush(stderr);
        }
        return -1;
    }

    if (t->kind == NT_FP16) {
        /* Native FP16 weights are normally decoded lazily to keep the model
         * cold footprint small.  A prefill layer, however, reuses each matrix
         * for many prompt activations.  Materialize it once here so a bounded
         * layer-major prefill never reopens and decodes the same tensor for
         * every token.  The runtime releases this cache at its layer boundary.
         */
        if (!t->passthrough) {
            if (load_passthrough(t, t->name, 1) != 0 || !t->passthrough) {
                fprintf(stderr,
                        "fpq_prepare_tensor native-fp16: failed tensor=%s rows=%u cols=%u\\n",
                        tensor_name, t->rows, t->cols);
                return -1;
            }
        }
        if (log_prepare) {
            fprintf(stderr,
                    "fpq_prepare_tensor detail native-fp16-ready tensor=%s rows=%u cols=%u\n",
                    tensor_name, t->rows, t->cols);
            fflush(stderr);
        }
        return 0;
    }

    /* The native residual stream is flat 256-value blocks.  Qwen's 896-wide
     * matrices cross block boundaries inside rows, so an SLI cache cannot be
     * used for them and preparation would be pure allocation overhead. */
    if (!native_sli_layout_safe(t)) {
        if (log_prepare) {
            fprintf(stderr,
                    "fpq_prepare_tensor detail bypass-unaligned tensor=%s rows=%u cols=%u\n",
                    tensor_name, t->rows, t->cols);
            fflush(stderr);
        }
        return 0;
    }

    if (t->sli) {
        if (log_prepare) {
            fprintf(stderr,
                    "fpq_prepare_tensor detail hit tensor=%s rows=%u cols=%u blocks=%zu\n",
                    tensor_name, t->rows, t->cols, t->n_blocks);
            fflush(stderr);
        }
        return 0;
    }

    if (log_prepare) clock_gettime(CLOCK_MONOTONIC, &t0);
    int rc = ensure_sli_ready(t);
    if (log_prepare) {
        clock_gettime(CLOCK_MONOTONIC, &t1);
        fprintf(stderr,
                "fpq_prepare_tensor detail build tensor=%s rows=%u cols=%u blocks=%zu rc=%d sec=%.3f\n",
                tensor_name, t->rows, t->cols, t->n_blocks, rc,
                fpq_prepare_elapsed_seconds_local(&t0, &t1));
        fflush(stderr);
    }
    return rc;
}

int fpq_tensor_is_prepared(fpq_model_t *m, const char *tensor_name) {
    native_tensor_t *t = find_tensor(m, tensor_name);
    if (!t) return 0;
    return t->kind == NT_FP16 ? t->passthrough != NULL : t->sli != NULL;
}

int fpq_release_tensor(fpq_model_t *m, const char *tensor_name) {
    if (!m || !tensor_name) return -1;

    native_tensor_t *t = find_tensor(m, tensor_name);
    if (!t) return -1;

    int released = 0;
    uint64_t est_sli_bytes = (uint64_t)t->n_blocks * 256ull * 4ull;

    if (t->kind == NT_FP16 && t->passthrough) {
        /* FP16 prefill preparation owns this transient expanded cache.  Do
         * not retain a whole model merely because prefill touched a layer. */
        free(t->passthrough);
        t->passthrough = NULL;
        t->passthrough_len = 0;
        bonfyre_tensor_set_state(t, BONFYRE_TENSOR_STATE_INDEXED);
        released = 1;
    }

    if (t->sli) {
        fpqx_sli_free(t->sli);
        t->sli = NULL;
        bonfyre_tensor_set_state(t, BONFYRE_TENSOR_STATE_RUNTIME_LOADED);
        released = 1;
    }

    const char *deep = getenv("BONFYRE_QWEN_RELEASE_RUNTIME_AFTER_PREFILL");
    int deep_on = (deep && *deep && strcmp(deep, "0") != 0);

    if (deep_on && t->compressed && t->compressed->sbb_scale_delta) {
        bonfyre_sbb_big_free(t->compressed->sbb_scale_delta);
        t->compressed->sbb_scale_delta = NULL;
        released = 1;
        bonfyre_tensor_set_state(t, BONFYRE_TENSOR_STATE_EVICTED);
        bonfyre_tensor_mark_reloadable(t);
    }

    const char *dbg = getenv("BONFYRE_QWEN_LOG_RELEASE");
    if (dbg && *dbg && strcmp(dbg, "0") != 0 && released) {
        fprintf(stderr,
                "qwen_release tensor=%s blocks=%llu est_sli_mb=%.1f deep_sbb_only=%d state=%s\n",
                t->name ? t->name : tensor_name,
                (unsigned long long)t->n_blocks,
                (double)est_sli_bytes / 1048576.0,
                deep_on ? 1 : 0,
                bonfyre_tensor_state_name(t->state));
        fflush(stderr);
    }

    return released;
}





int fpq_decode_one(fpq_model_t *m, const char *tensor_name, float *out) {
    native_tensor_t *t = find_tensor(m, tensor_name); if (!t || !out) return -1;
    if (t->kind == NT_FP16) {
        if (t->passthrough) {
            memcpy(out, t->passthrough, t->passthrough_len * sizeof(float));
            return 0;
        }
        for (size_t r = 0; r < t->rows; r++) {
            if (fpq_decode_row_impl(m, tensor_name, r, out + r * (size_t)t->cols) != 0) return -1;
        }
        return 0;
    }
    for (size_t r = 0; r < t->rows; r++) if (fpq_decode_row_impl(m, tensor_name, r, out + r * (size_t)t->cols) != 0) return -1;
    return 0;
}
int fpq_decode_all(fpq_model_t *m, const char *out_path) { (void)m; (void)out_path; fprintf(stderr, "fpq_decode_all lazy-native: disabled; no-full-decode runtime\n"); return -1; }
fpq_info_t fpq_info(fpq_model_t *m) { fpq_info_t z; memset(&z, 0, sizeof(z)); return m ? m->cached_info : z; }
const fpq_tensor_info_t *fpq_tensor_at(fpq_model_t *m, size_t index) { if (!m || index >= m->n_tensors) return NULL; return &m->tensors[index].info; }
const fpq_tensor_info_t *fpq_tensor_find(fpq_model_t *m, const char *name) { native_tensor_t *t = find_tensor(m, name); return t ? &t->info : NULL; }
fpq_tensor_storage_t fpq_tensor_storage(fpq_model_t *m, const char *name) {
    native_tensor_t *t = find_tensor(m, name);
    if (!t) return FPQ_TENSOR_STORAGE_UNKNOWN;
    return t->kind == NT_FP16 ? FPQ_TENSOR_STORAGE_NATIVE_FP16 : FPQ_TENSOR_STORAGE_COMPRESSED_V9;
}
const float *fpq_get_passthrough(fpq_model_t *m, const char *tensor_name) { native_tensor_t *t = find_tensor(m, tensor_name); return (t && t->kind == NT_FP16) ? t->passthrough : NULL; }
void fpq_model_set_active_cache(fpq_model_t *m, void *cache) { if (m) m->active_cache = cache; }
void *fpq_model_get_active_cache(fpq_model_t *m) { return m ? m->active_cache : NULL; }
