/*
 * cbf_internal.h — Internal types shared across libcolibri-bonfyre implementation
 *
 * This header contains internal structure definitions not exposed in the public API.
 * Used by cbf_engine.c, cbf_forward.c, cbf_quic_expert.c, etc.
 */

#ifndef CBF_INTERNAL_H
#define CBF_INTERNAL_H

#include "colibri_bonfyre.h"
#include <pthread.h>
#include <stdatomic.h>

/* Bonfyre dependencies */
#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations for Bonfyre types */
typedef struct bf_quic_ctx bf_quic_ctx_t;
typedef struct bf_quic_conn bf_quic_conn_t;
typedef struct bf_fragment_store bf_fragment_store_t;

#ifdef __cplusplus
}
#endif

/* ═══════════════════════════════════════════════════════════════════
 * Constants
 * ═══════════════════════════════════════════════════════════════════ */

#define MAX_EXPERTS_TOTAL    20000  /* GLM-5.2 has 19,456 */
#define MAX_PEERS            64
#define MAX_IO_THREADS       32
#define EXPERT_CACHE_ENTRIES 512    /* Per-layer LRU size */

/* ═══════════════════════════════════════════════════════════════════
 * Internal structures
 * ═══════════════════════════════════════════════════════════════════ */

/* Expert weight blob (single expert = gate, up, down matrices at int4) */
typedef struct {
    uint8_t *data;           /* Packed int4 weights */
    size_t   size;           /* Bytes */
    uint32_t layer_idx;
    uint32_t expert_idx;
    cbf_tier_t tier;         /* Where it's currently placed */
    uint64_t last_used_ms;   /* For LRU eviction */
    float    routing_heat;   /* Exponential decay frequency */
    char     host[64];       /* If tier == NETWORK */
    atomic_uint refcount;    /* For async I/O safety */
} expert_blob_t;

/* Per-layer expert cache (LRU) */
typedef struct {
    expert_blob_t *entries[EXPERT_CACHE_ENTRIES];
    uint32_t count;
    pthread_rwlock_t lock;
} expert_cache_t;

/* Async I/O request */
typedef struct {
    uint32_t layer_idx;
    uint32_t expert_idx;
    cbf_tier_t tier;
    expert_blob_t *result;  /* Filled by I/O thread */
    atomic_bool done;
} io_request_t;

/* I/O thread pool */
typedef struct {
    pthread_t threads[MAX_IO_THREADS];
    uint32_t  n_threads;
    io_request_t *queue;    /* Ring buffer */
    uint32_t  queue_size;
    atomic_uint queue_head;
    atomic_uint queue_tail;
    atomic_bool shutdown;
    pthread_mutex_t queue_lock;
    pthread_cond_t  queue_cond;
} io_pool_t;

/* Dense component storage (always resident) */
typedef struct {
    char model_path[512];    /* Base path to model directory */
    float *embeddings;       /* [n_vocab × d_model] */
    float *attn_dense;       /* All attention projections, concatenated */
    float *shared_experts;   /* Dense MLP that runs every token */
    float *output_proj;      /* Final projection to vocab */
    size_t total_bytes;
} dense_storage_t;

/* Router lookahead (prefetch next layer's experts) */
typedef struct {
    pthread_t thread;
    atomic_bool active;
    uint32_t current_layer;
    uint32_t prefetch_depth; /* How many layers ahead */
} router_lookahead_t;

/* Network peer tracking */
typedef struct {
    cbf_peer_t info;
    bf_quic_conn_t *conn;    /* Persistent QUIC connection */
    atomic_bool connected;
} peer_slot_t;

/* Main engine state */
struct cbf_engine {
    /* Model configuration */
    cbf_model_shape_t shape;
    cbf_memory_config_t memory;
    char model_path[1024];
    
    /* Storage */
    dense_storage_t dense;
    expert_cache_t *layer_caches;   /* Array of per-layer caches */
    expert_blob_t **expert_index;   /* [layer][expert] lookup */
    
    /* I/O */
    io_pool_t io_pool;
    router_lookahead_t lookahead;
    pthread_t lookahead_thread;
    atomic_bool lookahead_stop;
    
    /* Network tier */
    bf_quic_ctx_t *quic_ctx;
    peer_slot_t peers[MAX_PEERS];
    uint32_t n_peers;
    pthread_rwlock_t peers_lock;
    
    /* Fragment integration */
    bf_fragment_store_t *frag_store;
    char router_cache_id[73];
    
    /* Metrics */
    cbf_metrics_t metrics;
    atomic_uint_fast64_t total_tokens;
    atomic_uint_fast64_t total_experts_routed;
    
    /* State */
    atomic_bool loaded;
    pthread_mutex_t forward_lock;  /* Single forward at a time */
};

#endif /* CBF_INTERNAL_H */
