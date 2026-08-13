/*
 * colibri_bonfyre.h — Expert-streaming MoE inference for Bonfyre
 *
 * Ports Colibri's (github.com/JustVugg/colibri) proven expert-streaming
 * architecture into Bonfyre's distributed infrastructure and extends it with:
 *
 * 1. Expert placement across QUIC-connected nodes (VRAM/RAM/disk/network tiers)
 * 2. Fragment-based router learning with perspective-aware caching
 * 3. Multi-host expert pinning with latency-aware scheduling
 * 4. Compressed KV state persisted to lambda-tensor families
 * 5. Distributed speculative decoding with cross-node draft verification
 *
 * Core principles from Colibri maintained:
 *   - Placement only affects speed, never precision or routing semantics
 *   - Dense components stay resident, routed experts stream on-demand
 *   - Learning cache that optimizes over time (.coli_usage → fragments)
 *   - Per-layer LRU + async I/O + router lookahead prefetch
 *   - MLA compressed attention (576 floats/token vs 32,768)
 *   - Faithful model execution, token-exact validated
 *
 * Bonfyre extensions:
 *   - Network tier: QUIC-stream experts from remote nodes (layer-prioritized)
 *   - Fragment cache: router decisions + expert heat stored as fragments
 *   - Distributed pinning: hot experts replicated across fleet with continuity
 *   - NUMA-aware: multi-socket hosts interleave resident weights
 *   - Swarm inference: batch positions across multiple hosts
 *
 * Dependencies: libbonfyre, libfragment, libquic-transport, liblambda-tensors
 * Thread safety: Single writer per engine, multi-reader safe after init
 */

#ifndef COLIBRI_BONFYRE_H
#define COLIBRI_BONFYRE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ═══════════════════════════════════════════════════════════════════
 * Model shape & configuration
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    /* Architecture */
    uint32_t n_vocab;          /* Vocabulary size */
    uint32_t d_model;          /* Hidden dimension */
    uint32_t n_layers;         /* Number of transformer layers */
    uint32_t n_heads;          /* Attention heads */
    uint32_t n_kv_heads;       /* KV heads (GQA) */
    uint32_t head_dim;         /* Dimension per head */
    uint32_t d_ffn;            /* FFN intermediate dimension */

    /* MoE topology */
    uint32_t n_experts_per_layer;   /* Routed experts per layer (e.g., 256) */
    uint32_t n_experts_active;      /* Active experts per token (e.g., 2-8) */
    uint32_t n_shared_experts;      /* Dense shared experts (always active) */
    bool     has_mtp_head;          /* Multi-token prediction head for spec decoding */

    /* Memory layout */
    uint32_t expert_size_mb;        /* Size of one expert at int4 (~19 MB for GLM-5.2) */
    uint32_t dense_size_mb;         /* Resident dense components */
    uint32_t kv_bytes_per_token;    /* MLA compressed KV (576 floats = 2304 bytes) */

    /* Context */
    uint32_t max_seq_len;           /* Maximum sequence length */
    float    rope_theta;            /* RoPE base frequency */
} cbf_model_shape_t;

/* ═══════════════════════════════════════════════════════════════════
 * Memory hierarchy & placement policy
 * ═══════════════════════════════════════════════════════════════════ */

/* Storage tier for expert placement */
typedef enum {
    CBF_TIER_VRAM      = 0,   /* GPU memory (fastest) */
    CBF_TIER_RAM       = 1,   /* System RAM */
    CBF_TIER_DISK      = 2,   /* NVMe/SSD via async I/O */
    CBF_TIER_NETWORK   = 3,   /* QUIC-streamed from remote node */
    CBF_TIER_COUNT     = 4
} cbf_tier_t;

/* Per-tier budget and configuration */
typedef struct {
    uint64_t vram_gb;          /* GPU memory budget (0 = auto-detect) */
    uint64_t ram_gb;           /* System RAM budget (0 = auto-detect) */
    const char *disk_path;     /* Expert storage directory */
    bool     enable_network;   /* Allow QUIC expert streaming */

    /* Pinning policy */
    uint32_t pin_hot_experts;  /* Number of hot experts to pin resident (0 = auto) */
    float    pin_heat_threshold; /* Minimum routing frequency for pinning (0.0-1.0) */

    /* Async I/O */
    uint32_t io_threads;       /* Async I/O pool size (0 = auto, typ. 4-8) */
    uint32_t prefetch_layers;  /* Router lookahead depth (0 = disable, typ. 1) */

    /* NUMA */
    bool     numa_interleave;  /* Interleave weights across NUMA nodes */
} cbf_memory_config_t;

/* Placement plan (output of planning phase) */
typedef struct {
    uint32_t experts_vram;     /* Experts pinned in VRAM */
    uint32_t experts_ram;      /* Experts pinned in RAM */
    uint32_t experts_disk;     /* Experts streamed from disk */
    uint32_t experts_network;  /* Experts streamed from network */

    uint64_t vram_used_mb;
    uint64_t ram_used_mb;
    uint64_t disk_used_mb;

    float    cold_tok_per_sec; /* Estimated speed (cold cache) */
    float    warm_tok_per_sec; /* Estimated speed (warm cache) */
} cbf_placement_plan_t;

/* ═══════════════════════════════════════════════════════════════════
 * Expert metadata & routing statistics
 * ═══════════════════════════════════════════════════════════════════ */

/* Per-expert metadata (stored as fragments) */
typedef struct {
    uint32_t layer_idx;        /* Which layer (0..n_layers-1) */
    uint32_t expert_idx;       /* Expert index within layer */
    char     topic[64];        /* Measured topic affinity (e.g., "poetry", "SQL") */
    float    routing_heat;     /* Frequency routed (0.0-1.0, exponential decay) */
    uint64_t route_count;      /* Total times routed */
    uint64_t last_routed_ms;   /* Unix epoch milliseconds */
    cbf_tier_t tier;           /* Current placement tier */
    char     host[64];         /* Hostname if tier == NETWORK */
} cbf_expert_meta_t;

/* Router learning cache (persisted as fragments, updated every turn) */
typedef struct {
    char    fragment_id[73];   /* Fragment ID for this cache entry */
    char    workload_tag[64];  /* Workload identifier (e.g., "code_review") */
    uint32_t expert_count;     /* Number of hot experts tracked */
    cbf_expert_meta_t *experts; /* Sorted by routing_heat DESC */
    uint64_t updated_at;       /* Unix epoch ms */
} cbf_router_cache_t;

/* ═══════════════════════════════════════════════════════════════════
 * Inference engine
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct cbf_engine cbf_engine_t;

/* Create a new inference engine.
 * model_path: Directory containing expert weights + dense components
 * memory: Memory tier configuration
 * Returns NULL on failure (check stderr for diagnostics) */
cbf_engine_t *cbf_engine_new(const char *model_path,
                              const cbf_model_shape_t *shape,
                              const cbf_memory_config_t *memory);

/* Destroy engine and free all resources */
void cbf_engine_free(cbf_engine_t *engine);

/* Plan expert placement without loading weights (fast, read-only) */
int cbf_engine_plan(const char *model_path,
                    const cbf_model_shape_t *shape,
                    const cbf_memory_config_t *memory,
                    cbf_placement_plan_t *out_plan);

/* Load model weights and initialize runtime (blocking, may take 10-60s) */
int cbf_engine_load(cbf_engine_t *engine);

/* Readiness check (validates model files, memory, network peers) */
int cbf_engine_doctor(const cbf_engine_t *engine);

/* ═══════════════════════════════════════════════════════════════════
 * Forward pass & generation
 * ═══════════════════════════════════════════════════════════════════ */

/* KV cache handle (persistent across turns, byte-identical on restore) */
typedef struct cbf_kv_cache cbf_kv_cache_t;

/* Create a new KV cache (max_tokens = max_seq_len) */
cbf_kv_cache_t *cbf_kv_cache_new(const cbf_engine_t *engine, uint32_t max_tokens);

/* Destroy KV cache */
void cbf_kv_cache_free(cbf_kv_cache_t *cache);

/* Save KV cache to lambda-tensor family (compressed) */
int cbf_kv_cache_save(const cbf_kv_cache_t *cache, const char *path);

/* Load KV cache from lambda-tensor family (restore conversation warm) */
cbf_kv_cache_t *cbf_kv_cache_load(const cbf_engine_t *engine, const char *path);

/* Forward pass (single token or batch)
 * tokens: Input token IDs
 * n_tokens: Number of tokens
 * cache: KV cache (NULL for stateless single-shot)
 * logits_out: Output logits [n_vocab], caller-owned buffer
 * Returns 0 on success, <0 on error */
int cbf_forward(cbf_engine_t *engine,
                const uint32_t *tokens,
                uint32_t n_tokens,
                cbf_kv_cache_t *cache,
                float *logits_out);

/* Speculative decoding: draft + verify in one forward
 * Returns number of tokens accepted (1 = no speedup, 2-4 typical) */
int cbf_forward_speculative(cbf_engine_t *engine,
                            uint32_t prompt_token,
                            cbf_kv_cache_t *cache,
                            uint32_t max_draft,
                            uint32_t *out_tokens,
                            uint32_t *out_count);

/* ═══════════════════════════════════════════════════════════════════
 * Distributed expert placement (Bonfyre extension)
 * ═══════════════════════════════════════════════════════════════════ */

/* Network peer (remote node with expert capacity) */
typedef struct {
    char     host[64];         /* Hostname or IP */
    uint16_t port;             /* QUIC port */
    uint64_t vram_available_mb;
    uint64_t ram_available_mb;
    float    latency_ms;       /* Measured round-trip latency */
    bool     online;
} cbf_peer_t;

/* Register a remote peer for expert streaming */
int cbf_engine_add_peer(cbf_engine_t *engine, const cbf_peer_t *peer);

/* Remove a peer */
int cbf_engine_remove_peer(cbf_engine_t *engine, const char *host);

/* List current peers */
int cbf_engine_list_peers(const cbf_engine_t *engine,
                          cbf_peer_t **out_peers,
                          uint32_t *out_count);

/* Replicate a hot expert to a remote peer (async, fire-and-forget) */
int cbf_expert_replicate(cbf_engine_t *engine,
                         uint32_t layer_idx,
                         uint32_t expert_idx,
                         const char *target_host);

/* Pull expert from remote peer (blocks until transfer complete) */
int cbf_expert_pull(cbf_engine_t *engine,
                    uint32_t layer_idx,
                    uint32_t expert_idx,
                    const char *source_host,
                    cbf_tier_t target_tier);

/* Distributed pinning: analyze router cache and replicate hot experts
 * across fleet with latency-aware placement */
int cbf_rebalance_experts(cbf_engine_t *engine);

/* ═══════════════════════════════════════════════════════════════════
 * Router learning & fragment integration (Bonfyre extension)
 * ═══════════════════════════════════════════════════════════════════ */

/* Update router cache from current turn's routing decisions
 * Creates/updates fragments with routing heat, topic affinity, etc. */
int cbf_router_cache_update(cbf_engine_t *engine, const char *workload_tag);

/* Load router cache from fragment store (restores learned pinning) */
int cbf_router_cache_load(cbf_engine_t *engine, const char *workload_tag);

/* Export measured expert atlas as fragment set (3D galaxy visualization) */
int cbf_expert_atlas_export(const cbf_engine_t *engine, const char *out_path);

/* ═══════════════════════════════════════════════════════════════════
 * Observability & metrics
 * ═══════════════════════════════════════════════════════════════════ */

/* Real-time inference metrics */
typedef struct {
    /* Token throughput */
    float    tok_per_sec;      /* Current decode speed */
    float    ttft_ms;          /* Time to first token */
    uint64_t tokens_generated; /* Total tokens this session */

    /* Expert routing */
    uint64_t experts_routed;   /* Experts used this turn */
    uint64_t cache_hits[CBF_TIER_COUNT];  /* Per-tier cache hits */
    uint64_t cache_misses[CBF_TIER_COUNT]; /* Per-tier cache misses */

    /* Speculative decoding */
    uint64_t drafts_attempted;
    uint64_t drafts_accepted;  /* Tokens accepted (acceptance rate) */
    float    spec_speedup;     /* Effective speedup (2.2-2.8x typical) */

    /* Memory residency */
    uint32_t experts_resident[CBF_TIER_COUNT];
    uint64_t bytes_streamed;   /* Total bytes loaded from disk/network */

    /* Network */
    uint64_t network_latency_us; /* Average expert fetch latency */
    uint64_t network_bytes_rx;

    /* Prefetch effectiveness */
    float    prefetch_hit_rate; /* Fraction of prefetches that hit */
} cbf_metrics_t;

/* Get current metrics snapshot */
void cbf_engine_get_metrics(const cbf_engine_t *engine, cbf_metrics_t *out);

/* Reset metrics counters */
void cbf_engine_reset_metrics(cbf_engine_t *engine);

/* Dump metrics as JSON to FILE* */
void cbf_metrics_dump_json(const cbf_metrics_t *metrics, FILE *out);

/* ═══════════════════════════════════════════════════════════════════
 * Error handling
 * ═══════════════════════════════════════════════════════════════════ */

#define CBF_OK                0
#define CBF_ERR_MEMORY       -1   /* Allocation failure */
#define CBF_ERR_IO           -2   /* File I/O error */
#define CBF_ERR_NETWORK      -3   /* QUIC transport failure */
#define CBF_ERR_MODEL        -4   /* Model file corrupt/missing */
#define CBF_ERR_SHAPE        -5   /* Invalid model shape */
#define CBF_ERR_BUDGET       -6   /* Insufficient memory budget */
#define CBF_ERR_EXPERT       -7   /* Expert load/route failure */
#define CBF_ERR_INVALID      -8   /* Invalid argument */

/* Get human-readable error string for last operation */
const char *cbf_strerror(int code);

/* ═══════════════════════════════════════════════════════════════════
 * Tokenizer API
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct cbf_tokenizer cbf_tokenizer_t;

/* Load tokenizer from vocabulary file */
cbf_tokenizer_t *cbf_tokenizer_load(const char *vocab_path);

/* Create a simple fallback tokenizer (char-level) */
cbf_tokenizer_t *cbf_tokenizer_create_fallback(uint32_t vocab_size);

/* Free tokenizer resources */
void cbf_tokenizer_free(cbf_tokenizer_t *tok);

/* Encode text to tokens */
int cbf_tokenizer_encode(cbf_tokenizer_t *tok, const char *text,
                         uint32_t *out_tokens, uint32_t max_tokens,
                         uint32_t *out_count);

/* Decode tokens to text */
int cbf_tokenizer_decode(cbf_tokenizer_t *tok, const uint32_t *tokens,
                         uint32_t n_tokens, char *out_text, size_t max_len);

/* Decode single token */
const char *cbf_tokenizer_decode_token(cbf_tokenizer_t *tok, uint32_t token_id);

/* Get special token IDs */
uint32_t cbf_tokenizer_bos(cbf_tokenizer_t *tok);
uint32_t cbf_tokenizer_eos(cbf_tokenizer_t *tok);
uint32_t cbf_tokenizer_vocab_size(cbf_tokenizer_t *tok);

/* ═══════════════════════════════════════════════════════════════════
 * Architecture Registry API
 * ═══════════════════════════════════════════════════════════════════ */

/* Look up architecture template by name (e.g., "mixtral", "qwen-moe-2.7b") */
const cbf_model_shape_t *cbf_arch_lookup(const char *arch_name);

/* Load architecture from model_config.json in model directory */
int cbf_arch_load_from_config(const char *model_path, cbf_model_shape_t *out_shape);

/* Auto-detect architecture from model_config.json or directory name */
int cbf_arch_auto_detect(const char *model_path, cbf_model_shape_t *out_shape);

/* Save configuration to model_config.json */
int cbf_arch_save_config(const char *model_path, const char *arch_name,
                         const cbf_model_shape_t *shape);

/* List all known architectures */
void cbf_arch_list_all(void);

#ifdef __cplusplus
}
#endif

#endif /* COLIBRI_BONFYRE_H */
