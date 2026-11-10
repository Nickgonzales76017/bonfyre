/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * fpq_active_cache.c — Active packed weight cache implementation
 */
#include "fpq_active_cache.h"
#include "fpq_kernels_neon.h"
#include <bonfyre.h>
#include <bf_jsonw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <time.h>

#define FPQ_ACTIVE_CACHE_VERSION 2
#define FPQ_ACTIVE_MAGIC 0x54434141u /* "AACT" */
#define FPQ_ACTIVE_ENTRY_CAPACITY 2048u
#define FPQ_ACTIVE_FLAG_GHOST 0x1u

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t rank;
    uint16_t flags;
    uint16_t reserved;
    uint32_t rows;
    uint32_t cols;
    uint64_t payload_bytes;
} fpq_active_disk_header_t;

static float fp16_to_float_local(uint16_t h) {
    uint32_t sign = ((uint32_t)h & 0x8000u) << 16;
    uint32_t exp  = ((uint32_t)h >> 10) & 0x1fu;
    uint32_t frac = (uint32_t)h & 0x03ffu;
    if (exp == 0) {
        if (frac == 0) { union { uint32_t u; float f; } z = { sign }; return z.f; }
        while ((frac & 0x0400u) == 0) { frac <<= 1; exp--; }
        exp++; frac &= ~0x0400u;
    } else if (exp == 31) {
        union { uint32_t u; float f; } z = { sign | 0x7f800000u | (frac << 13) };
        return z.f;
    }
    union { uint32_t u; float f; } z = { sign | ((exp + 112u) << 23) | (frac << 13) };
    return z.f;
}

static uint64_t fnv1a64_local(const char *s) {
    uint64_t h = 1469598103934665603ULL;
    for (const unsigned char *p = (const unsigned char *)s; p && *p; p++) {
        h ^= (uint64_t)(*p);
        h *= 1099511628211ULL;
    }
    return h;
}

static int profiling_enabled(void) {
    const char *p = getenv("BONFYRE_PROFILE_QWEN");
    return (p && *p && strcmp(p, "0") != 0);
}

static int debug_enabled(void) {
    const char *p = getenv("BONFYRE_QWEN_DEBUG");
    return (p && *p && strcmp(p, "0") != 0);
}

static int persist_enabled(void) {
    const char *p = getenv("BONFYRE_ACTIVE_CACHE_PERSIST");
    if (p && *p) return strcmp(p, "0") != 0;
    p = getenv("BONFYRE_QWEN_SPEED_MODE");
    if (p && *p && strcmp(p, "0") != 0) return 0;
    return 1;
}

static double monotonic_seconds_now_local(void) {
    struct timespec ts = {0};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void prepare_breakdown_emit(const char *kind,
                                   const char *tensor_name,
                                   double seconds,
                                   const char *extra_json) {
    const char *path = getenv("BONFYRE_QWEN_PREPARE_BREAKDOWN_JSONL");
    FILE *fp;
    if (!path || !path[0] || !kind) return;
    fp = fopen(path, "a");
    if (!fp) return;
    fprintf(fp, "{\"kind\":\"%s\",\"tensor\":", kind);
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

static void safe_name(const char *in, char *out, size_t n) {
    if (!in || !out || n == 0) return;
    size_t k = 0;
    for (size_t i = 0; in[i] && k + 1 < n; i++) {
        char c = in[i];
        if ((c >= 'a' && c <= 'z') ||
            (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') ||
            c == '_' || c == '-') out[k++] = c;
        else out[k++] = '_';
    }
    out[k] = '\0';
}

static void ensure_parent_dir(const char *dir) {
    if (!dir || !*dir) return;
    mkdir(dir, 0755);
}

static void fpq_active_digest_bytes(const void *data, size_t bytes, char out[65]) {
    static const uint8_t empty = 0;
    const uint8_t *p = data ? (const uint8_t *)data : &empty;
    bf_sha256_hex(p, bytes, out);
}

static void fpq_active_payload_digest(fpq_active_entry_t *entry) {
    char u_hash[65], vt_hash[65], us_hash[65], vts_hash[65];
    char gu_hash[65], gv_hash[65], gm_hash[65];
    char factor_descriptor[768], exception_descriptor[512], payload_descriptor[256];
    float ghost_meta[3] = {0.0f, 0.0f, 0.0f};
    size_t rank, u_count, vt_count;

    if (!entry || !entry->ready || entry->lr_rank == 0 ||
        !entry->active_u_q || !entry->active_vt_q ||
        !entry->active_u_scales || !entry->active_vt_scales) {
        if (entry) {
            entry->factor_sha256[0] = '\0';
            entry->exception_sha256[0] = '\0';
            entry->payload_sha256[0] = '\0';
        }
        return;
    }

    rank = (size_t)entry->lr_rank;
    u_count = rank * (size_t)entry->rows;
    vt_count = rank * (size_t)entry->cols;
    fpq_active_digest_bytes(entry->active_u_q, u_count, u_hash);
    fpq_active_digest_bytes(entry->active_vt_q, vt_count, vt_hash);
    fpq_active_digest_bytes(entry->active_u_scales, rank * sizeof(float), us_hash);
    fpq_active_digest_bytes(entry->active_vt_scales, rank * sizeof(float), vts_hash);

    snprintf(factor_descriptor, sizeof(factor_descriptor),
             "fpq-active-factor-v1|rows=%u|cols=%u|rank=%u|u=%s|vt=%s|us=%s|vts=%s",
             entry->rows, entry->cols, (unsigned)entry->lr_rank,
             u_hash, vt_hash, us_hash, vts_hash);
    bf_sha256_hex((const uint8_t *)factor_descriptor, strlen(factor_descriptor),
                  entry->factor_sha256);

    if (entry->has_ghost && (!entry->ghost_u_q || !entry->ghost_v_q)) {
        entry->exception_sha256[0] = '\0';
        entry->payload_sha256[0] = '\0';
        return;
    }

    if (entry->has_ghost) {
        ghost_meta[0] = entry->ghost_sigma;
        ghost_meta[1] = entry->ghost_u_scale;
        ghost_meta[2] = entry->ghost_v_scale;
        fpq_active_digest_bytes(entry->ghost_u_q, (size_t)entry->rows, gu_hash);
        fpq_active_digest_bytes(entry->ghost_v_q, (size_t)entry->cols, gv_hash);
        fpq_active_digest_bytes(ghost_meta, sizeof(ghost_meta), gm_hash);
    } else {
        fpq_active_digest_bytes(NULL, 0, gu_hash);
        fpq_active_digest_bytes(NULL, 0, gv_hash);
        fpq_active_digest_bytes(NULL, 0, gm_hash);
    }

    snprintf(exception_descriptor, sizeof(exception_descriptor),
             "fpq-active-exception-v1|rows=%u|cols=%u|ghost=%u|gu=%s|gv=%s|gm=%s",
             entry->rows, entry->cols, (unsigned)entry->has_ghost,
             gu_hash, gv_hash, gm_hash);
    bf_sha256_hex((const uint8_t *)exception_descriptor, strlen(exception_descriptor),
                  entry->exception_sha256);
    snprintf(payload_descriptor, sizeof(payload_descriptor),
             "fpq-active-payload-v2|factor=%s|exception=%s",
             entry->factor_sha256, entry->exception_sha256);
    bf_sha256_hex((const uint8_t *)payload_descriptor, strlen(payload_descriptor),
                  entry->payload_sha256);
}



static int fpq_active_finalize_lr_payload(fpq_active_entry_t *entry,
                                          uint16_t lr_rank,
                                          const char *quant_mode,
                                          int allow_ghost) {
    if (!entry || lr_rank == 0) return -1;
    if (!entry->active_u_q || !entry->active_vt_q ||
        !entry->active_u_scales || !entry->active_vt_scales) {
        entry->data = NULL;
        entry->payload_bytes = 0;
        entry->ready = 0;
        return -1;
    }

    size_t rank = (size_t)lr_rank;
    size_t u_count = rank * (size_t)entry->rows;
    size_t vt_count = rank * (size_t)entry->cols;

    entry->lr_rank = lr_rank;
    entry->qweight_size = u_count + vt_count;
    entry->scales_size = rank * 2u * sizeof(float);

    if (allow_ghost && entry->has_ghost) {
        entry->scales_size += 3u * sizeof(float);
    }

    entry->total_bytes = entry->qweight_size + entry->scales_size;

    if (allow_ghost && entry->has_ghost) {
        entry->total_bytes += (size_t)entry->rows + (size_t)entry->cols;
    }

    entry->payload_bytes = entry->total_bytes;
    entry->data = entry->active_u_q;
    entry->ready = (entry->data != NULL &&
                    entry->payload_bytes > 0 &&
                    entry->active_vt_q != NULL &&
                    entry->active_u_scales != NULL &&
                    entry->active_vt_scales != NULL);

    if (quant_mode && quant_mode[0]) {
        snprintf(entry->quant_mode, sizeof(entry->quant_mode), "%s", quant_mode);
    }
    if (entry->ready) {
        fpq_active_payload_digest(entry);
    } else {
        entry->factor_sha256[0] = '\0';
        entry->exception_sha256[0] = '\0';
        entry->payload_sha256[0] = '\0';
    }

    return entry->ready ? 0 : -1;
}


static void clear_materialized_buffers(fpq_active_entry_t *e) {
    if (!e) return;
    free(e->active_u_q); e->active_u_q = NULL;
    free(e->active_vt_q); e->active_vt_q = NULL;
    free(e->active_u_scales); e->active_u_scales = NULL;
    free(e->active_vt_scales); e->active_vt_scales = NULL;
    free(e->ghost_u_q); e->ghost_u_q = NULL;
    free(e->ghost_v_q); e->ghost_v_q = NULL;
    e->lr_rank = 0;
    e->ghost_sigma = 0.0f;
    e->ghost_u_scale = 0.0f;
    e->ghost_v_scale = 0.0f;
    e->has_ghost = 0;
    e->qweight_size = 0;
    e->scales_size = 0;
    e->total_bytes = 0;
    e->payload_bytes = 0;
    e->factor_sha256[0] = '\0';
    e->exception_sha256[0] = '\0';
    e->payload_sha256[0] = '\0';
    e->data = NULL;
    e->ready = 0;
    e->materialized_from_artifact = 0;
}


static int load_v9_artifact(fpq_active_entry_t *entry) {
    if (!entry || !entry->cache_file) return -1;
    FILE *fp = fopen(entry->cache_file, "rb");
    if (!fp) return -1;

    fpq_active_disk_header_t h;
    if (fread(&h, sizeof(h), 1, fp) != 1) { fclose(fp); return -1; }
    if (h.magic != FPQ_ACTIVE_MAGIC ||
        h.version != FPQ_ACTIVE_CACHE_VERSION ||
        h.rows != entry->rows ||
        h.cols != entry->cols ||
        h.rank == 0) {
        fclose(fp);
        return -1;
    }

    const size_t rank = (size_t)h.rank;
    const size_t u_count = rank * (size_t)h.rows;
    const size_t vt_count = rank * (size_t)h.cols;
    const int has_ghost = ((h.flags & FPQ_ACTIVE_FLAG_GHOST) != 0);

    clear_materialized_buffers(entry);

    entry->active_u_scales = (float *)malloc(rank * sizeof(float));
    entry->active_vt_scales = (float *)malloc(rank * sizeof(float));
    entry->active_u_q = (int8_t *)malloc(u_count);
    entry->active_vt_q = (int8_t *)malloc(vt_count);
    if (has_ghost) {
        entry->ghost_u_q = (int8_t *)malloc((size_t)h.rows);
        entry->ghost_v_q = (int8_t *)malloc((size_t)h.cols);
    }

    if (!entry->active_u_scales || !entry->active_vt_scales ||
        !entry->active_u_q || !entry->active_vt_q ||
        (has_ghost && (!entry->ghost_u_q || !entry->ghost_v_q))) {
        fclose(fp);
        clear_materialized_buffers(entry);
        return -1;
    }

    if (fread(entry->active_u_scales, sizeof(float), rank, fp) != rank ||
        fread(entry->active_vt_scales, sizeof(float), rank, fp) != rank) {
        fclose(fp);
        clear_materialized_buffers(entry);
        return -1;
    }

    long payload_pos = ftell(fp);
    if (payload_pos < 0) {
        fclose(fp);
        clear_materialized_buffers(entry);
        return -1;
    }

    /* Canonical layout: header, scales, active_u_q, active_vt_q, optional ghost. */
    int ok = 0;
    entry->has_ghost = 0;
    if (fread(entry->active_u_q, 1, u_count, fp) == u_count &&
        fread(entry->active_vt_q, 1, vt_count, fp) == vt_count) {
        if (!has_ghost) {
            ok = 1;
        } else if (fread(&entry->ghost_sigma, sizeof(float), 1, fp) == 1 &&
                   fread(&entry->ghost_u_scale, sizeof(float), 1, fp) == 1 &&
                   fread(&entry->ghost_v_scale, sizeof(float), 1, fp) == 1 &&
                   fread(entry->ghost_u_q, 1, (size_t)h.rows, fp) == (size_t)h.rows &&
                   fread(entry->ghost_v_q, 1, (size_t)h.cols, fp) == (size_t)h.cols) {
            entry->has_ghost = 1;
            ok = 1;
        }
    }

    /* Legacy fallback: header, scales, optional ghost, active_u_q, active_vt_q. */
    if (!ok) {
        if (fseek(fp, payload_pos, SEEK_SET) != 0) {
            fclose(fp);
            clear_materialized_buffers(entry);
            return -1;
        }
        entry->has_ghost = 0;
        if (has_ghost) {
            if (fread(&entry->ghost_sigma, sizeof(float), 1, fp) != 1 ||
                fread(&entry->ghost_u_scale, sizeof(float), 1, fp) != 1 ||
                fread(&entry->ghost_v_scale, sizeof(float), 1, fp) != 1 ||
                fread(entry->ghost_u_q, 1, (size_t)h.rows, fp) != (size_t)h.rows ||
                fread(entry->ghost_v_q, 1, (size_t)h.cols, fp) != (size_t)h.cols) {
                fclose(fp);
                clear_materialized_buffers(entry);
                return -1;
            }
            entry->has_ghost = 1;
        }
        if (fread(entry->active_u_q, 1, u_count, fp) != u_count ||
            fread(entry->active_vt_q, 1, vt_count, fp) != vt_count) {
            fclose(fp);
            clear_materialized_buffers(entry);
            return -1;
        }
        ok = 1;
    }

    fclose(fp);

    if (!ok || fpq_active_finalize_lr_payload(entry, h.rank, "v9-lr-active-artifact", 1) != 0) {
        clear_materialized_buffers(entry);
        return -1;
    }

    entry->materialized_from_artifact = 1;
    return 0;
}


static int save_v9_artifact(const fpq_active_entry_t *entry) {
    if (!entry || !entry->cache_file ||
        !entry->active_u_q || !entry->active_vt_q ||
        !entry->active_u_scales || !entry->active_vt_scales ||
        entry->lr_rank == 0) {
        return -1;
    }

    FILE *fp = fopen(entry->cache_file, "wb");
    if (!fp) return -1;

    fpq_active_disk_header_t h;
    memset(&h, 0, sizeof(h));
    h.magic = FPQ_ACTIVE_MAGIC;
    h.version = FPQ_ACTIVE_CACHE_VERSION;
    h.rows = entry->rows;
    h.cols = entry->cols;
    h.rank = entry->lr_rank;
    h.flags = entry->has_ghost ? FPQ_ACTIVE_FLAG_GHOST : 0;

    const size_t rank = (size_t)entry->lr_rank;
    const size_t u_count = rank * (size_t)entry->rows;
    const size_t vt_count = rank * (size_t)entry->cols;

    int ok = 1;
    ok = ok && fwrite(&h, sizeof(h), 1, fp) == 1;
    ok = ok && fwrite(entry->active_u_scales, sizeof(float), rank, fp) == rank;
    ok = ok && fwrite(entry->active_vt_scales, sizeof(float), rank, fp) == rank;

    /* Canonical layout matching load_v9_artifact. */
    ok = ok && fwrite(entry->active_u_q, 1, u_count, fp) == u_count;
    ok = ok && fwrite(entry->active_vt_q, 1, vt_count, fp) == vt_count;

    if (entry->has_ghost) {
        ok = ok && entry->ghost_u_q && entry->ghost_v_q;
        ok = ok && fwrite(&entry->ghost_sigma, sizeof(float), 1, fp) == 1;
        ok = ok && fwrite(&entry->ghost_u_scale, sizeof(float), 1, fp) == 1;
        ok = ok && fwrite(&entry->ghost_v_scale, sizeof(float), 1, fp) == 1;
        ok = ok && fwrite(entry->ghost_u_q, 1, (size_t)entry->rows, fp) == (size_t)entry->rows;
        ok = ok && fwrite(entry->ghost_v_q, 1, (size_t)entry->cols, fp) == (size_t)entry->cols;
    }

    if (fclose(fp) != 0) ok = 0;
    if (!ok) {
        unlink(entry->cache_file);
        return -1;
    }
    return 0;
}

fpq_active_cache_t *fpq_active_cache_init(const char *cache_dir, size_t max_mb) {
    fpq_active_cache_t *cache = (fpq_active_cache_t *)calloc(1, sizeof(*cache));
    if (!cache) return NULL;
    pthread_mutex_init(&cache->mu, NULL);

    cache->entries_capacity = FPQ_ACTIVE_ENTRY_CAPACITY;
    cache->entries = (fpq_active_entry_t *)calloc(cache->entries_capacity,
                                                  sizeof(fpq_active_entry_t));
    if (!cache->entries) {
        pthread_mutex_destroy(&cache->mu);
        free(cache);
        return NULL;
    }

    cache->cache_dir = strdup(cache_dir ? cache_dir :
        "/Users/nickgonzales/BonfyreModels/cache/fpq-active");
    cache->max_bytes = max_mb * 1024 * 1024;
    {
        const char *aw = getenv("BONFYRE_ACTIVE_WEIGHT_CACHE");
        cache->enabled = (aw && strcmp(aw, "0") != 0);
    }

    if (cache->enabled) {
        /* Create cache directory if needed */
        mkdir(cache->cache_dir, 0755);
        fprintf(stderr, "fpq_active_cache: enabled, dir=%s, max=%zu MB\n",
                cache->cache_dir, max_mb);
    }

    return cache;
}

void fpq_active_cache_free(fpq_active_cache_t *cache) {
    if (!cache) return;
    pthread_mutex_lock(&cache->mu);
    for (size_t i = 0; i < cache->n_entries; i++) {
        fpq_active_entry_t *e = &cache->entries[i];
        free(e->tensor_name);
        free(e->cache_key);
        free(e->cache_file);
        free(e->active_qweight);
        free(e->active_scales);
        free(e->active_u_q);
        free(e->active_vt_q);
        free(e->active_u_scales);
        free(e->active_vt_scales);
        free(e->ghost_u_q);
        free(e->ghost_v_q);
        free(e->active_residuals);
        free(e->active_tile_meta);
        free(e->row_group_offsets);
        if (e->materialize_mu_inited) {
            pthread_mutex_destroy(&e->materialize_mu);
            e->materialize_mu_inited = 0;
        }
    }

    free(cache->entries);
    free(cache->cache_dir);
    pthread_mutex_unlock(&cache->mu);
    pthread_mutex_destroy(&cache->mu);
    free(cache);
}

void fpq_active_cache_density(fpq_active_cache_t *cache, fpq_active_density_t *out) {
    if (!out) return;
    memset(out, 0, sizeof(*out));
    if (!cache) return;

    pthread_mutex_lock(&cache->mu);
    for (size_t i = 0; i < cache->n_entries; i++) {
        const fpq_active_entry_t *entry = &cache->entries[i];
        int factor_duplicate = 0;
        int payload_duplicate = 0;
        size_t factor_bytes = 0;
        if (!entry->ready || entry->payload_bytes == 0 ||
            entry->factor_sha256[0] == '\0' ||
            entry->payload_sha256[0] == '\0') {
            continue;
        }

        out->ready_entries++;
        out->resident_payload_bytes += entry->payload_bytes;
        factor_bytes = entry->qweight_size +
                       (size_t)entry->lr_rank * 2u * sizeof(float);
        out->resident_factor_bytes += factor_bytes;
        for (size_t j = 0; j < i; j++) {
            const fpq_active_entry_t *prior = &cache->entries[j];
            if (!prior->ready || prior->payload_bytes == 0 ||
                prior->factor_sha256[0] == '\0' ||
                prior->payload_sha256[0] == '\0') {
                continue;
            }
            if (strcmp(prior->factor_sha256, entry->factor_sha256) == 0)
                factor_duplicate = 1;
            if (prior->payload_bytes == entry->payload_bytes &&
                strcmp(prior->payload_sha256, entry->payload_sha256) == 0)
                payload_duplicate = 1;
        }

        if (factor_duplicate) {
            out->duplicate_factors++;
            out->duplicate_factor_bytes += factor_bytes;
        } else {
            out->unique_factors++;
            out->unique_factor_bytes += factor_bytes;
        }
        if (payload_duplicate) {
            out->duplicate_payloads++;
            out->duplicate_payload_bytes += entry->payload_bytes;
        } else {
            out->unique_payloads++;
            out->unique_payload_bytes += entry->payload_bytes;
        }
    }
    pthread_mutex_unlock(&cache->mu);
}

int fpq_active_cache_write_passports(fpq_active_cache_t *cache,
                                     const char *jsonl_path) {
    char tmp_path[PATH_MAX];
    FILE *fp = NULL;
    int count = 0;
    int failed = 0;

    if (!cache || !jsonl_path || !jsonl_path[0]) return -1;
    if (snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.%ld",
                 jsonl_path, (long)getpid()) >= (int)sizeof(tmp_path)) {
        return -1;
    }
    fp = fopen(tmp_path, "w");
    if (!fp) return -1;

    pthread_mutex_lock(&cache->mu);
    for (size_t i = 0; i < cache->n_entries; i++) {
        const fpq_active_entry_t *entry = &cache->entries[i];
        char subject_descriptor[4096];
        char binding_descriptor[4096];
        char subject_hash[65], binding_hash[65];
        char identity[256], basis[80], binding[80], exception[80];
        char physical_digest[80], physical_body_ref[128];
        char lift_ref[1200], version[32];
        bf_jsonw w;

        if (!entry->ready || entry->lr_rank == 0 || entry->payload_bytes == 0 ||
            !entry->tensor_name || !entry->cache_key ||
            entry->factor_sha256[0] == '\0' ||
            entry->exception_sha256[0] == '\0' ||
            entry->payload_sha256[0] == '\0') {
            continue;
        }

        if (snprintf(subject_descriptor, sizeof(subject_descriptor),
                     "fpq-active-subject-v1|key=%s|tensor=%s|rows=%u|cols=%u",
                     entry->cache_key, entry->tensor_name,
                     entry->rows, entry->cols) >= (int)sizeof(subject_descriptor)) {
            failed = 1;
            break;
        }
        bf_sha256_hex((const uint8_t *)subject_descriptor,
                      strlen(subject_descriptor), subject_hash);

        if (snprintf(binding_descriptor, sizeof(binding_descriptor),
                     "fpq-active-binding-v1|subject=%s|tensor=%s|factor=%s",
                     subject_hash, entry->tensor_name,
                     entry->factor_sha256) >= (int)sizeof(binding_descriptor)) {
            failed = 1;
            break;
        }
        bf_sha256_hex((const uint8_t *)binding_descriptor,
                      strlen(binding_descriptor), binding_hash);

        snprintf(identity, sizeof(identity),
                 "fpq-active-tensor:%s:%s", subject_hash, binding_hash);
        snprintf(basis, sizeof(basis), "sha256:%s", entry->factor_sha256);
        snprintf(binding, sizeof(binding), "sha256:%s", binding_hash);
        if (entry->has_ghost)
            snprintf(exception, sizeof(exception), "sha256:%s", entry->exception_sha256);
        else
            exception[0] = '\0';
        snprintf(physical_digest, sizeof(physical_digest),
                 "sha256:%s", entry->payload_sha256);
        snprintf(physical_body_ref, sizeof(physical_body_ref),
                 "memory:fpq-active:%s", entry->payload_sha256);
        if (snprintf(lift_ref, sizeof(lift_ref),
                     "fpq-active-source:%s", entry->cache_key) >= (int)sizeof(lift_ref)) {
            failed = 1;
            break;
        }
        snprintf(version, sizeof(version), "%u", entry->format_version);

        bf_jsonw_init(&w, 0);
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "identity"); bf_jsonw_str(&w, identity);
        bf_jsonw_key(&w, "subject_family"); bf_jsonw_str(&w, "fpq-active-tensor");
        bf_jsonw_key(&w, "semantic_digest");
        {
            char semantic_digest[80];
            snprintf(semantic_digest, sizeof(semantic_digest), "sha256:%s", subject_hash);
            bf_jsonw_str(&w, semantic_digest);
        }
        bf_jsonw_key(&w, "representation_family"); bf_jsonw_str(&w, "fpq-active-low-rank");
        bf_jsonw_key(&w, "representation_version"); bf_jsonw_str(&w, version);
        bf_jsonw_key(&w, "exactness_class"); bf_jsonw_str(&w, "exact");
        bf_jsonw_key(&w, "quality_loss_budget"); bf_jsonw_num(&w, 0.0);
        bf_jsonw_key(&w, "measurement_state"); bf_jsonw_str(&w, "measured");
        bf_jsonw_key(&w, "shared_basis"); bf_jsonw_str(&w, basis);
        bf_jsonw_key(&w, "binding_digest"); bf_jsonw_str(&w, binding);
        bf_jsonw_key(&w, "exception_digest"); bf_jsonw_str(&w, exception);
        bf_jsonw_key(&w, "physical_body_ref"); bf_jsonw_str(&w, physical_body_ref);
        bf_jsonw_key(&w, "physical_digest"); bf_jsonw_str(&w, physical_digest);
        bf_jsonw_key(&w, "supports");
        bf_jsonw_arr(&w);
        bf_jsonw_str(&w, "matvec");
        bf_jsonw_str(&w, "row_decode");
        bf_jsonw_str(&w, "direct_execution");
        bf_jsonw_arr_end(&w);
        bf_jsonw_key(&w, "lift_ref"); bf_jsonw_str(&w, lift_ref);
        bf_jsonw_key(&w, "bytes"); bf_jsonw_int(&w, (int64_t)entry->payload_bytes);
        bf_jsonw_obj_end(&w);

        if (w.oom || !w.buf ||
            fwrite(w.buf, 1, w.len, fp) != w.len ||
            fputc('\n', fp) == EOF) {
            bf_jsonw_free(&w);
            failed = 1;
            break;
        }
        bf_jsonw_free(&w);
        count++;
    }
    pthread_mutex_unlock(&cache->mu);

    if (!failed && count > 0 && fflush(fp) == 0) {
        int fd = fileno(fp);
        if (fd < 0 || fsync(fd) != 0) failed = 1;
    } else {
        failed = 1;
    }
    if (fclose(fp) != 0) failed = 1;

    if (failed || rename(tmp_path, jsonl_path) != 0) {
        unlink(tmp_path);
        return -1;
    }
    return count;
}


void fpq_active_cache_key(const char *pack_path,
                          const char *tensor_name,
                          uint32_t rows,
                          uint32_t cols,
                          char *key_out,
                          size_t key_size) {
    snprintf(key_out, key_size, "%s|%s|%u|%u|v%d",
             pack_path ? pack_path : "pack",
             tensor_name ? tensor_name : "tensor",
             rows, cols, FPQ_ACTIVE_CACHE_VERSION);

    /* Replace unsafe chars */
    for (char *p = key_out; *p; p++) {
        if (*p == '/' || *p == '.') *p = '_';
    }
}

static fpq_active_entry_t *find_entry(fpq_active_cache_t *cache, const char *cache_key) {
    for (size_t i = 0; i < cache->n_entries; i++) {
        if (cache->entries[i].cache_key && strcmp(cache->entries[i].cache_key, cache_key) == 0) {
            return &cache->entries[i];
        }
    }
    return NULL;
}

fpq_active_entry_t *fpq_active_cache_get(fpq_active_cache_t *cache,
                                         const char *tensor_name,
                                         uint32_t rows,
                                         uint32_t cols,
                                         const char *shard_path,
                                         uint64_t tensor_offset,
                                         const char *quant_mode,
                                         uint32_t format_version) {
    char key[1024];
    struct stat st;
    uint64_t mtime = 0;
    double t0 = monotonic_seconds_now_local();
    double t1 = 0.0;

    if (!cache || !cache->enabled || !tensor_name) return NULL;
    if (shard_path && stat(shard_path, &st) == 0) mtime = (uint64_t)st.st_mtime;
    snprintf(key, sizeof(key), "%s|%" PRIu64 "|%s|%u|%u|%s|%u|%" PRIu64,
             shard_path ? shard_path : "shard",
             mtime,
             tensor_name,
             rows, cols,
             quant_mode ? quant_mode : "unknown",
             format_version,
             tensor_offset);

    pthread_mutex_lock(&cache->mu);

    fpq_active_entry_t *existing = find_entry(cache, key);
    if (existing && existing->loaded) {
        cache->hits++;
        if (profiling_enabled() && !existing->hit_logged) {
            fprintf(stderr, "active_cache HIT tensor=%s key=%s\n", tensor_name, key);
            existing->hit_logged = 1;
        }
        t1 = monotonic_seconds_now_local();
        {
            char extra[256];
            snprintf(extra, sizeof(extra),
                     "\"hit\":true,\"ready\":%s,\"rows\":%u,\"cols\":%u",
                     existing->ready ? "true" : "false", rows, cols);
            prepare_breakdown_emit("active_cache_lookup", tensor_name, t1 - t0, extra);
        }
        pthread_mutex_unlock(&cache->mu);
        return existing;
    }

    uint64_t h = fnv1a64_local(key);
    char safe[256];
    safe_name(tensor_name, safe, sizeof(safe));

    const char *dir = getenv("BONFYRE_ACTIVE_CACHE_DIR");
    if (!dir || !*dir) dir = cache->cache_dir;
    ensure_parent_dir(dir);

    char cpath[PATH_MAX];
    snprintf(cpath, sizeof(cpath), "%s/%s.%016" PRIx64 ".active", dir, safe, h);

    fpq_active_entry_t entry;
    memset(&entry, 0, sizeof(entry));
    entry.tensor_name = strdup(tensor_name);
    entry.cache_key = strdup(key);
    entry.cache_file = strdup(cpath);
    entry.rows = rows;
    entry.cols = cols;
    entry.format_version = format_version;
    snprintf(entry.quant_mode, sizeof(entry.quant_mode), "%s", quant_mode ? quant_mode : "unknown");

    if (persist_enabled() && access(cpath, F_OK) == 0) {
        entry.loaded = 1;
        entry.total_bytes = 0;
        if (cache->n_entries >= cache->entries_capacity) {
            free(entry.tensor_name); free(entry.cache_key); free(entry.cache_file);
            pthread_mutex_unlock(&cache->mu);
            return NULL;
        }
        cache->entries[cache->n_entries] = entry;
        if (pthread_mutex_init(&cache->entries[cache->n_entries].materialize_mu, NULL) == 0) {
            cache->entries[cache->n_entries].materialize_mu_inited = 1;
        } else {
            cache->entries[cache->n_entries].materialize_mu_inited = 0;
        }
        cache->n_entries++;
        if (load_v9_artifact(&cache->entries[cache->n_entries - 1]) != 0) {
            cache->entries[cache->n_entries - 1].ready = 0;
            cache->entries[cache->n_entries - 1].payload_bytes = 0;
            cache->entries[cache->n_entries - 1].data = NULL;
        }
        cache->hits++;
        if (profiling_enabled() && !cache->entries[cache->n_entries - 1].hit_logged) {
            fprintf(stderr, "active_cache HIT tensor=%s\n", tensor_name);
            cache->entries[cache->n_entries - 1].hit_logged = 1;
        }
        t1 = monotonic_seconds_now_local();
        {
            char extra[256];
            snprintf(extra, sizeof(extra),
                     "\"hit\":true,\"artifact\":true,\"ready\":%s,\"rows\":%u,\"cols\":%u",
                     cache->entries[cache->n_entries - 1].ready ? "true" : "false", rows, cols);
            prepare_breakdown_emit("active_cache_lookup", tensor_name, t1 - t0, extra);
        }
        pthread_mutex_unlock(&cache->mu);
        return &cache->entries[cache->n_entries - 1];
    }

    /* MISS: materialize a visible artifact and register entry. */
    if (persist_enabled()) {
        FILE *wf = fopen(cpath, "wb");
        if (wf) {
            /* Keep artifact compact but informative. */
            fprintf(wf, "tensor=%s\nrows=%u\ncols=%u\nquant_mode=%s\nformat=%u\nkey=%s\n",
                    tensor_name, rows, cols,
                    quant_mode ? quant_mode : "unknown",
                    format_version,
                    key);
            fclose(wf);
        }
    }

    entry.loaded = 1;
    entry.total_bytes = (size_t)rows * (size_t)cols;
    entry.ready = 0;
    entry.payload_bytes = 0;
    entry.data = NULL;

    if (cache->n_entries >= cache->entries_capacity) {
        if (profiling_enabled()) {
            fprintf(stderr,
                    "active_cache CAPACITY_REACHED tensor=%s cap=%zu\n",
                    tensor_name ? tensor_name : "(null)",
                    cache->entries_capacity);
        }
        free(entry.tensor_name);
        free(entry.cache_key);
        free(entry.cache_file);
        pthread_mutex_unlock(&cache->mu);
        return NULL;
    }

    cache->entries[cache->n_entries] = entry;
    if (pthread_mutex_init(&cache->entries[cache->n_entries].materialize_mu, NULL) == 0) {
        cache->entries[cache->n_entries].materialize_mu_inited = 1;
    } else {
        cache->entries[cache->n_entries].materialize_mu_inited = 0;
    }
    cache->n_entries++;

    cache->misses++;
    cache->writes++;
    if (profiling_enabled()) {
        fprintf(stderr,
                "active_cache MISS tensor=%s rows=%u cols=%u bytes=%zu path=%s\n",
                tensor_name, rows, cols, entry.total_bytes, cpath);
    }

    t1 = monotonic_seconds_now_local();
    {
        char extra[256];
        snprintf(extra, sizeof(extra),
                 "\"hit\":false,\"ready\":false,\"rows\":%u,\"cols\":%u,\"path\":\"%s\"",
                 rows, cols, cpath);
        prepare_breakdown_emit("active_cache_lookup", tensor_name, t1 - t0, extra);
    }
    pthread_mutex_unlock(&cache->mu);
    return &cache->entries[cache->n_entries - 1];
}

int fpq_active_matvec(const fpq_active_entry_t *entry,
                     const float *x,
                     float *y) {
    if (!entry || !entry->loaded || !x || !y) return -1;
    if (entry->disabled) return -1;
    if (!entry->ready || entry->payload_bytes == 0 || !entry->data) return -1;
    if (entry->active_u_q && entry->active_vt_q &&
        entry->active_u_scales && entry->active_vt_scales &&
        entry->lr_rank > 0) {
        size_t rank = (size_t)entry->lr_rank;
        float *tmp = (float *)calloc(rank, sizeof(float));
        if (!tmp) return -1;

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
        if (entry->has_ghost && entry->ghost_u_q && entry->ghost_v_q) {
            float gv = fpq_i8_f32_dot_neon(entry->ghost_v_q, x, entry->cols) * entry->ghost_v_scale;
            float coeff = entry->ghost_sigma * gv * entry->ghost_u_scale;
            if (fabsf(coeff) >= 1e-30f) {
                fpq_i8_f32_axpy_neon(entry->ghost_u_q, coeff, y, entry->rows);
            }
        }
        free(tmp);
        return 0;
    }
    if (!entry->active_qweight || !entry->active_scales) return -1;

    /* Use active packed data for optimized matvec */
    fpq_i8_f32_gemv_neon(entry->active_qweight, entry->active_scales,
                        x, y, entry->rows, entry->cols);

    /* Merge residuals if present */
    if (entry->active_residuals) {
        fpq_residual_merge_neon(y, entry->active_residuals, entry->rows);
    }

    return 0;
}

int fpq_active_decode_row(const fpq_active_entry_t *entry,
                          size_t row,
                          float *out) {
    if (!entry || !entry->loaded || row >= entry->rows || !out) return -1;
    if (entry->disabled) return -1;
    if (!entry->ready || entry->payload_bytes == 0 || !entry->data) return -1;
    if (entry->active_u_q && entry->active_vt_q &&
        entry->active_u_scales && entry->active_vt_scales &&
        entry->lr_rank > 0) {
        memset(out, 0, (size_t)entry->cols * sizeof(float));
        for (size_t r = 0; r < (size_t)entry->lr_rank; r++) {
            const int8_t *u = entry->active_u_q + r * (size_t)entry->rows;
            const int8_t *vt = entry->active_vt_q + r * (size_t)entry->cols;
            float coeff = (float)u[row] * entry->active_u_scales[r] * entry->active_vt_scales[r];
            if (fabsf(coeff) < 1e-30f) continue;
            fpq_i8_f32_axpy_neon(vt, coeff, out, entry->cols);
        }
        return 0;
    }
    if (!entry->active_qweight || !entry->active_scales) return -1;

    /* Decode one row from active packed data */
    const int8_t *w_row = entry->active_qweight + row * entry->cols;
    float scale = entry->active_scales[row];

    for (size_t c = 0; c < entry->cols; c++) {
        out[c] = (float)w_row[c] * scale;
    }

    /* Add residual correction if present */
    if (entry->active_residuals) {
        const float *res_row = entry->active_residuals + row * entry->cols;
        for (size_t c = 0; c < entry->cols; c++) {
            out[c] += res_row[c];
        }
    }

    return 0;
}

int fpq_active_materialize_v9(fpq_active_entry_t *entry,
                              const uint8_t *shard_base,
                              size_t shard_size,
                              uint64_t off_us,
                              uint64_t off_vt,
                              uint16_t lr_rank) {
    double t0 = monotonic_seconds_now_local();
    if (!entry || !shard_base || shard_size == 0 || lr_rank == 0) return -1;
    if (entry->disabled || entry->materialize_failed) return -1;
    if (entry->materialize_mu_inited) {
        pthread_mutex_lock(&entry->materialize_mu);
    }
    if (entry->disabled || entry->materialize_failed) {
        prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                               monotonic_seconds_now_local() - t0,
                               "\"reused\":false,\"ok\":false,\"phase\":\"disabled\"");
        if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
        return -1;
    }
    if (entry->active_u_q && entry->active_vt_q &&
        entry->active_u_scales && entry->active_vt_scales &&
        entry->lr_rank == lr_rank) {
        int frc = fpq_active_finalize_lr_payload(entry, lr_rank, "v9-lr-active", 0);
        prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                               monotonic_seconds_now_local() - t0,
                               frc == 0
                                   ? "\"reused\":true,\"ok\":true,\"finalized\":true,\"phase\":\"memory\""
                                   : "\"reused\":true,\"ok\":false,\"finalized\":false,\"phase\":\"memory\"");
        if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
        return frc;
    }

    size_t rank = (size_t)lr_rank;
    size_t u_count = rank * (size_t)entry->rows;
    size_t vt_count = rank * (size_t)entry->cols;

    clear_materialized_buffers(entry);

    entry->active_u_scales = (float *)malloc(rank * sizeof(float));
    entry->active_vt_scales = (float *)malloc(rank * sizeof(float));
    entry->active_u_q = (int8_t *)malloc(u_count);
    entry->active_vt_q = (int8_t *)malloc(vt_count);
    if (!entry->active_u_scales || !entry->active_vt_scales || !entry->active_u_q || !entry->active_vt_q) {
        clear_materialized_buffers(entry);
        entry->materialize_failed = 1;
        entry->failure_count++;
        prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                               monotonic_seconds_now_local() - t0,
                               "\"reused\":false,\"ok\":false,\"phase\":\"alloc\"");
        if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
        return -1;
    }

    for (size_t r = 0; r < rank; r++) {
        uint64_t uoff = off_us + (uint64_t)r * (2ull + (uint64_t)entry->rows);
        uint64_t voff = off_vt + (uint64_t)r * (2ull + (uint64_t)entry->cols);
        if (uoff + 2ull + (uint64_t)entry->rows > (uint64_t)shard_size ||
            voff + 2ull + (uint64_t)entry->cols > (uint64_t)shard_size) {
            clear_materialized_buffers(entry);
            entry->materialize_failed = 1;
            entry->failure_count++;
            prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                                   monotonic_seconds_now_local() - t0,
                                   "\"reused\":false,\"ok\":false,\"phase\":\"bounds\"");
            if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
            return -1;
        }

        uint16_t u_h, vt_h;
        memcpy(&u_h, shard_base + uoff, 2);
        memcpy(&vt_h, shard_base + voff, 2);
        entry->active_u_scales[r] = fp16_to_float_local(u_h);
        entry->active_vt_scales[r] = fp16_to_float_local(vt_h);
        memcpy(entry->active_u_q + r * (size_t)entry->rows, shard_base + uoff + 2ull, (size_t)entry->rows);
        memcpy(entry->active_vt_q + r * (size_t)entry->cols, shard_base + voff + 2ull, (size_t)entry->cols);
    }

    if (fpq_active_finalize_lr_payload(entry, lr_rank, "v9-lr-active", 0) != 0) {
        clear_materialized_buffers(entry);
        entry->materialize_failed = 1;
        entry->failure_count++;
        prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                               monotonic_seconds_now_local() - t0,
                               "\"reused\":false,\"ok\":false,\"phase\":\"finalize_memory\"");
        if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
        return -1;
    }
    if (persist_enabled()) (void)save_v9_artifact(entry);
    if (profiling_enabled()) {
        fprintf(stderr,
                "active_cache MATERIALIZED tensor=%s rank=%u rows=%u cols=%u bytes=%zu\n",
                entry->tensor_name ? entry->tensor_name : "(null)",
                (unsigned)entry->lr_rank,
                entry->rows,
                entry->cols,
                entry->total_bytes);
    }
    {
        char extra[256];
        snprintf(extra, sizeof(extra),
                 "\"reused\":false,\"ok\":true,\"rank\":%u,\"rows\":%u,\"cols\":%u,\"bytes\":%zu",
                 (unsigned)entry->lr_rank, entry->rows, entry->cols, entry->total_bytes);
        prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                               monotonic_seconds_now_local() - t0, extra);
    }
    if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
    return 0;
}

int fpq_active_materialize_v9_file(fpq_active_entry_t *entry,
                                   const char *path,
                                   uint64_t off_us,
                                   uint64_t off_vt,
                                   uint16_t lr_rank,
                                   uint64_t off_ghost,
                                   int has_ghost) {
    double t0 = monotonic_seconds_now_local();
    if (!entry || !path || !path[0] || lr_rank == 0) return -1;
    if (entry->disabled || entry->materialize_failed) return -1;
    if (entry->materialize_mu_inited) {
        pthread_mutex_lock(&entry->materialize_mu);
    }
    if (entry->disabled || entry->materialize_failed) {
        prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                               monotonic_seconds_now_local() - t0,
                               "\"reused\":false,\"ok\":false,\"phase\":\"disabled_file\"");
        if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
        return -1;
    }
    if (entry->active_u_q && entry->active_vt_q &&
        entry->active_u_scales && entry->active_vt_scales &&
        entry->lr_rank == lr_rank) {
        int frc = fpq_active_finalize_lr_payload(entry,
                                                 lr_rank,
                                                 "v9-lr-active-file",
                                                 entry->has_ghost);
        prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                               monotonic_seconds_now_local() - t0,
                               frc == 0
                                   ? "\"reused\":true,\"ok\":true,\"phase\":\"file\",\"finalized\":true"
                                   : "\"reused\":true,\"ok\":false,\"phase\":\"file\",\"finalized\":false");
        if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
        return frc;
    }

    size_t rank = (size_t)lr_rank;
    size_t u_count = rank * (size_t)entry->rows;
    size_t vt_count = rank * (size_t)entry->cols;
    clear_materialized_buffers(entry);

    entry->active_u_scales = (float *)malloc(rank * sizeof(float));
    entry->active_vt_scales = (float *)malloc(rank * sizeof(float));
    entry->active_u_q = (int8_t *)malloc(u_count);
    entry->active_vt_q = (int8_t *)malloc(vt_count);
    if (has_ghost) {
        entry->ghost_u_q = (int8_t *)malloc((size_t)entry->rows);
        entry->ghost_v_q = (int8_t *)malloc((size_t)entry->cols);
    }
    if (!entry->active_u_scales || !entry->active_vt_scales || !entry->active_u_q || !entry->active_vt_q ||
        (has_ghost && (!entry->ghost_u_q || !entry->ghost_v_q))) {
        clear_materialized_buffers(entry);
        entry->materialize_failed = 1;
        entry->failure_count++;
        prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                               monotonic_seconds_now_local() - t0,
                               "\"reused\":false,\"ok\":false,\"phase\":\"alloc_file\"");
        if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
        return -1;
    }

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        clear_materialized_buffers(entry);
        entry->materialize_failed = 1;
        entry->failure_count++;
        prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                               monotonic_seconds_now_local() - t0,
                               "\"reused\":false,\"ok\":false,\"phase\":\"open_file\"");
        if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
        return -1;
    }

    for (size_t r = 0; r < rank; r++) {
        uint64_t uoff = off_us + (uint64_t)r * (2ull + (uint64_t)entry->rows);
        uint64_t voff = off_vt + (uint64_t)r * (2ull + (uint64_t)entry->cols);
        uint16_t u_h = 0;
        uint16_t vt_h = 0;
        if (pread(fd, &u_h, 2, (off_t)uoff) != 2 ||
            pread(fd, &vt_h, 2, (off_t)voff) != 2 ||
            pread(fd, entry->active_u_q + r * (size_t)entry->rows,
                  (size_t)entry->rows, (off_t)(uoff + 2ull)) != (ssize_t)entry->rows ||
            pread(fd, entry->active_vt_q + r * (size_t)entry->cols,
                  (size_t)entry->cols, (off_t)(voff + 2ull)) != (ssize_t)entry->cols) {
            close(fd);
            clear_materialized_buffers(entry);
            entry->materialize_failed = 1;
            entry->failure_count++;
            prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                                   monotonic_seconds_now_local() - t0,
                                   "\"reused\":false,\"ok\":false,\"phase\":\"pread_file\"");
            if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
            return -1;
        }
        entry->active_u_scales[r] = fp16_to_float_local(u_h);
        entry->active_vt_scales[r] = fp16_to_float_local(vt_h);
    }
    if (has_ghost) {
        uint16_t sigma_h = 0, u_sc_h = 0, v_sc_h = 0;
        uint64_t off = off_ghost;
        if (pread(fd, &sigma_h, 2, (off_t)off) != 2 ||
            pread(fd, &u_sc_h, 2, (off_t)(off + 2ull)) != 2 ||
            pread(fd, entry->ghost_u_q, (size_t)entry->rows, (off_t)(off + 4ull)) != (ssize_t)entry->rows ||
            pread(fd, &v_sc_h, 2, (off_t)(off + 4ull + (uint64_t)entry->rows)) != 2 ||
            pread(fd, entry->ghost_v_q, (size_t)entry->cols,
                  (off_t)(off + 6ull + (uint64_t)entry->rows)) != (ssize_t)entry->cols) {
            close(fd);
            clear_materialized_buffers(entry);
            entry->materialize_failed = 1;
            entry->failure_count++;
            prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                                   monotonic_seconds_now_local() - t0,
                                   "\"reused\":false,\"ok\":false,\"phase\":\"pread_ghost_file\"");
            if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
            return -1;
        }
        entry->ghost_sigma = fp16_to_float_local(sigma_h);
        entry->ghost_u_scale = fp16_to_float_local(u_sc_h);
        entry->ghost_v_scale = fp16_to_float_local(v_sc_h);
        entry->has_ghost = 1;
    }
    close(fd);

    if (fpq_active_finalize_lr_payload(entry, lr_rank, "v9-lr-active-file", 1) != 0) {
        clear_materialized_buffers(entry);
        entry->materialize_failed = 1;
        entry->failure_count++;
        prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                               monotonic_seconds_now_local() - t0,
                               "\"reused\":false,\"ok\":false,\"phase\":\"finalize_file\"");
        if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
        return -1;
    }
    if (persist_enabled()) (void)save_v9_artifact(entry);
    prepare_breakdown_emit("active_cache_materialize", entry->tensor_name,
                           monotonic_seconds_now_local() - t0,
                           "\"reused\":false,\"ok\":true,\"phase\":\"file\"");
    if (entry->materialize_mu_inited) pthread_mutex_unlock(&entry->materialize_mu);
    return 0;
}

void fpq_active_entry_disable(fpq_active_entry_t *entry) {
    if (!entry) return;
    entry->disabled = 1;
    entry->materialize_failed = 1;
    entry->failure_count++;
    clear_materialized_buffers(entry);
    if (debug_enabled() || profiling_enabled()) {
        fprintf(stderr,
                "active_cache DISABLED tensor=%s failures=%u\n",
                entry->tensor_name ? entry->tensor_name : "(null)",
                entry->failure_count);
    }
}
