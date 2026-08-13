/*
 * fpq_active_cache.h — Active packed weight cache
 *
 * Caches pre-decoded/pre-packed tensor data to avoid repeated
 * residual/codebook decode during prefill and generation.
 *
 * Cache directory: /Users/nickgonzales/BonfyreModels/cache/fpq-active/
 * Cache key: pack_hash + shard_mtime + tensor_name + shape + quant_mode + version
 */
#pragma once

#include "libfpq.h"
#include <stdint.h>
#include <stddef.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Cache entry for one tensor */
typedef struct {
    char *tensor_name;
    char *cache_key;
    char *cache_file;
    uint32_t rows;
    uint32_t cols;

    /* Active packed weights */
    int8_t *active_qweight;      /* quantized weights ready for SIMD */
    float *active_scales;        /* per-block scales */
    int8_t *active_u_q;          /* low-rank U quantized columns [rank, rows] */
    int8_t *active_vt_q;         /* low-rank V^T quantized rows [rank, cols] */
    float *active_u_scales;      /* low-rank U scales [rank] */
    float *active_vt_scales;     /* low-rank V^T scales [rank] */
    uint16_t lr_rank;
    int8_t *ghost_u_q;           /* cached ghost U quantized vector [rows] */
    int8_t *ghost_v_q;           /* cached ghost V quantized vector [cols] */
    float ghost_sigma;
    float ghost_u_scale;
    float ghost_v_scale;
    uint8_t has_ghost;
    float *active_residuals;     /* pre-decoded residual corrections */
    uint8_t *active_tile_meta;   /* tile codebook metadata */
    uint64_t *row_group_offsets; /* for batched access */
    void *data;                  /* non-NULL only when real payload is materialized */
    size_t payload_bytes;        /* payload bytes currently materialized */
    int ready;                   /* 1 when payload is usable for compute */

    size_t qweight_size;
    size_t scales_size;
    size_t residuals_size;
    size_t tile_meta_size;
    size_t total_bytes;
    size_t n_blocks;
    uint32_t format_version;
    char quant_mode[32];
    int hit_logged;
    int materialized_from_artifact;
    int disabled;
    int materialize_failed;
    unsigned failure_count;
    pthread_mutex_t materialize_mu;
    int materialize_mu_inited;

    int loaded;
} fpq_active_entry_t;

/* Active cache manager */
typedef struct {
    char *cache_dir;
    fpq_active_entry_t *entries;
    size_t n_entries;
    size_t entries_capacity;
    size_t total_bytes;
    size_t max_bytes;
    size_t hits;
    size_t misses;
    size_t writes;
    pthread_mutex_t mu;
    int enabled;
} fpq_active_cache_t;

/* Initialize active cache */
fpq_active_cache_t *fpq_active_cache_init(const char *cache_dir, size_t max_mb);

/* Free active cache */
void fpq_active_cache_free(fpq_active_cache_t *cache);

/* Load or create cache entry for tensor */
fpq_active_entry_t *fpq_active_cache_get(fpq_active_cache_t *cache,
                                         const char *tensor_name,
                                         uint32_t rows,
                                         uint32_t cols,
                                         const char *shard_path,
                                         uint64_t tensor_offset,
                                         const char *quant_mode,
                                         uint32_t format_version);

/* Compute cache key for tensor */
void fpq_active_cache_key(const char *pack_path,
                          const char *tensor_name,
                          uint32_t rows,
                          uint32_t cols,
                          char *key_out,
                          size_t key_size);

/* Matvec using active cache entry */
int fpq_active_matvec(const fpq_active_entry_t *entry,
                     const float *x,
                     float *y);

/* Decode row using active cache entry */
int fpq_active_decode_row(const fpq_active_entry_t *entry,
                          size_t row,
                          float *out);

/* Materialize low-rank V9 tensor data into active packed arrays. */
int fpq_active_materialize_v9(fpq_active_entry_t *entry,
                              const uint8_t *shard_base,
                              size_t shard_size,
                              uint64_t off_us,
                              uint64_t off_vt,
                              uint16_t lr_rank);

int fpq_active_materialize_v9_file(fpq_active_entry_t *entry,
                                   const char *path,
                                   uint64_t off_us,
                                   uint64_t off_vt,
                                   uint16_t lr_rank,
                                   uint64_t off_ghost,
                                   int has_ghost);

void fpq_active_entry_disable(fpq_active_entry_t *entry);

#ifdef __cplusplus
}
#endif
