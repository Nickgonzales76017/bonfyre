/*
 * bonfyre.h — canonical runtime contract for all Bonfyre binaries.
 *
 * Every binary in the system either reads or writes BfArtifact manifests.
 * This header defines that contract, plus shared utilities that every
 * binary needs (dir creation, timestamps, CLI parsing, JSON extraction).
 *
 * Link with: -lbonfyre (lib/libbonfyre/libbonfyre.a)
 */
#ifndef BONFYRE_H
#define BONFYRE_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ================================================================
 * Artifact Contract
 *
 * This is THE canonical data structure in Bonfyre. Every binary
 * that produces output writes a BfArtifact manifest. Every binary
 * that consumes input reads one.
 *
 * The artifact_id is content-addressed (SHA-256 of canonical form).
 * The family_key groups structurally equivalent artifacts.
 * The canonical_key distinguishes different signatures within a family.
 *
 * Artifacts are pure data — they never contain behavior.
 * ================================================================ */

typedef struct {
    char artifact_id[128];     /* content-addressed ID (SHA-256 hex=64)  */
    char artifact_type[128];   /* "transcript", "brief", "proof", etc.   */
    char source_system[128];   /* "BonfyreTranscribe", etc.              */
    char created_at[32];       /* ISO-8601 UTC "YYYY-MM-DDTHH:MM:SSZ"   */
    char root_hash[68];        /* SHA-256 hex = 64 chars + NUL           */
    char family_key[17];       /* FNV-1a-64 hex: type + system           */
    char canonical_key[17];    /* FNV-1a-64 hex: type + system + counts  */
    int  atoms_count;          /* number of atom sub-objects              */
    int  operators_count;      /* number of operator sub-objects          */
    int  realizations_count;   /* number of realization sub-objects       */
    int  component_total;      /* atoms + operators + realizations        */
} BfArtifact;

/* Initialize all fields to zero. */
void bf_artifact_init(BfArtifact *a);

/* Parse artifact fields from a JSON string.
 * Extracts: artifact_id, artifact_type, source_system, created_at,
 * root_hash, and counts atoms/operators/realizations arrays.
 * Computes family_key and canonical_key automatically. */
void bf_artifact_parse(BfArtifact *a, const char *json);

/* Compute family_key and canonical_key from current fields.
 * Called automatically by bf_artifact_parse, but exposed for
 * code that builds artifacts field-by-field. */
void bf_artifact_compute_keys(BfArtifact *a);

/* Write a BfArtifact as JSON to a file. Returns 0 on success. */
int bf_artifact_write_json(const BfArtifact *a, const char *path);

/* Write a BfArtifact as JSON to a buffer.
 * Returns bytes written (excluding NUL), or -1 on overflow. */
int bf_artifact_to_json(const BfArtifact *a, char *buf, size_t buf_sz);

/* ================================================================
 * Artifact Cache (binary fast path)
 *
 * .bfsum — text cache: magic + BfArtifact
 * .bfrec — binary cache: magic + file_size + file_mtime + BfArtifact
 * ================================================================ */

#define BF_CACHE_MAGIC  "BFSM01"
#define BF_BINARY_MAGIC "BFAR01"
#define BF_MAGIC_LEN    6       /* strlen of both magic strings */

typedef struct {
    char magic[8];
    BfArtifact artifact;
} BfCacheRecord;

typedef struct {
    char       magic[8];
    long long  json_size;
    long long  json_mtime;
    BfArtifact artifact;
} BfBinaryRecord;

/* Load cached artifact if cache is fresh (returns 1), else 0. */
int bf_cache_load(const char *json_path, BfArtifact *a);

/* Save artifact to cache files. */
void bf_cache_save(const char *json_path, const BfArtifact *a);

/* ================================================================
 * Operator Descriptors
 *
 * Every transform in the system declares what it accepts, what it
 * produces, and its behavioral class. This drives:
 *   - pipeline composition and validation
 *   - dependency graph generation
 *   - cost modeling for realization policies
 *   - automated documentation
 *
 * A binary is either a PURE transform (stateless, cacheable) or a
 * STATEFUL service (owns state, not cacheable). Never half-both.
 * ================================================================ */

#define BF_OP_PURE       0x01  /* stateless: same inputs → same outputs      */
#define BF_OP_STATEFUL   0x02  /* owns mutable state (SQLite, files)         */
#define BF_OP_CACHEABLE  0x04  /* output can be cached by (op, params, hash) */
#define BF_OP_REVERSIBLE 0x08  /* output → input reconstruction possible     */
#define BF_OP_IDEMPOTENT 0x10  /* running twice = running once               */
#define BF_OP_STREAMING  0x20  /* can process incrementally                  */

/* Exactness classes for transform outputs */
typedef enum {
    BF_EXACT_BYTE  = 0,  /* byte-for-byte identical on replay           */
    BF_EXACT_CANON = 1,  /* identical after canonicalization             */
    BF_EXACT_LOSSY = 2   /* derived but not perfectly reconstructable   */
} BfExactness;

#define BF_MAX_TYPES  8

typedef struct {
    const char  *name;                   /* e.g. "transcribe"               */
    const char  *binary;                 /* e.g. "bonfyre-transcribe"       */
    const char  *description;            /* one-line purpose                */
    const char  *input_types[BF_MAX_TYPES];  /* accepted artifact types     */
    const char  *output_types[BF_MAX_TYPES]; /* produced artifact types     */
    int          input_count;
    int          output_count;
    uint32_t     flags;                  /* BF_OP_* flags                   */
    BfExactness  exactness;              /* output exactness class          */
    const char  *version;                /* semantic version                */
    const char  *layer;                  /* "substrate" or "surface"        */
    const char  *group;                  /* "ingest", "transform", etc.     */
} BfOperator;

typedef struct {
    double cost;             /* normalized execution/storage cost         */
    double latency;          /* normalized latency burden                 */
    double confidence;       /* normalized confidence / replay stability  */
    double reversibility;    /* normalized reversibility / rollback ease  */
    double utility;          /* normalized expected contribution          */
    double information_gain; /* normalized expected branch-value gain     */
} BfOperatorProfile;

/* Built-in operator registry — all Bonfyre binaries. */
extern const BfOperator BF_OPERATORS[];
extern const int        BF_OPERATOR_COUNT;

/* Look up an operator by binary name. Returns NULL if not found. */
const BfOperator *bf_operator_find(const char *binary_name);

/* Look up an operator by logical name. Returns NULL if not found. */
const BfOperator *bf_operator_find_by_name(const char *name);

/* Derived control profile for orchestration, planning, and policy search. */
BfOperatorProfile bf_operator_profile(const BfOperator *op);

/* ================================================================
 * Binary Layer Model
 *
 * Substrate (cold, formal, stable):
 *   ingest, hash, index, compress, stitch, graph, runtime, queue, sync
 *
 * Transform (pure, cacheable):
 *   transcribe, transcript-clean, paragraph, brief, proof, embed,
 *   media-prep, narrate, render, emit, mfa-dict, weaviate-index
 *
 * Surface (product-facing, stateful):
 *   cms, api, auth, pipeline, cli, transcript-family
 *
 * Value (monetization, metering):
 *   offer, gate, meter, ledger, finance, outreach, pay, distribute, pack
 *
 * Library:
 *   liblambda-tensors
 * ================================================================ */

typedef enum {
    BF_LAYER_SUBSTRATE = 0,
    BF_LAYER_TRANSFORM = 1,
    BF_LAYER_SURFACE   = 2,
    BF_LAYER_VALUE     = 3,
    BF_LAYER_LIBRARY   = 4
} BfLayer;

/* ================================================================
 * SHA-256 (FIPS 180-4)
 *
 * Inline implementation with no external dependencies.
 * Used for content addressing throughout the system.
 * ================================================================ */

typedef struct {
    uint32_t h[8];
    uint8_t  buf[64];
    uint64_t total;
} BfSha256;

void   bf_sha256_init(BfSha256 *ctx);
void   bf_sha256_update(BfSha256 *ctx, const uint8_t *data, size_t len);
void   bf_sha256_final(BfSha256 *ctx, uint8_t hash[32]);

/* Convenience: hash data and write hex string (65 bytes including NUL). */
void   bf_sha256_hex(const uint8_t *data, size_t len, char hex[65]);

/* Convenience: hash a file and write hex string. Returns 0 on success. */
int    bf_sha256_file(const char *path, char hex[65]);

/* Convenience: format a pre-computed 32-byte digest as a 64-char hex string. */
void   bf_sha256_digest_hex(const uint8_t hash[32], char hex[65]);

/* ================================================================
 * FNV-1a-64
 *
 * Used for family and canonical key computation.
 * ================================================================ */

#define BF_FNV1A_INIT 1469598103934665603ULL

uint64_t bf_fnv1a64(uint64_t h, const void *data, size_t len);

/* Normalize a string for equivalence hashing:
 * lowercase, collapse non-alnum to single dash, strip leading/trailing dash.
 * Writes to dst (must be at least dst_sz bytes). */
void bf_normalize_token(char *dst, size_t dst_sz, const char *src);

/* ================================================================
 * Common Utilities
 *
 * These were previously duplicated across every binary.
 * ================================================================ */

/* Create directory and all parents. Returns 0 on success. */
int  bf_ensure_dir(const char *path);
int  bf_ensure_parent_dir(const char *filepath);  /* create parent of a file path */

/* Write ISO-8601 UTC timestamp to buf. */
void bf_iso_timestamp(char *buf, size_t sz);

/* Write ISO-8601 UTC timestamp offset by days_offset days. */
void bf_iso_timestamp_future(char *buf, size_t sz, int days_offset);

/* Check if a file exists. */
int  bf_file_exists(const char *path);

/* Get file size in bytes (-1 on error). */
long bf_file_size(const char *path);

/* Read entire file into malloc'd buffer. Caller frees.
 * Sets *out_len if non-NULL. Returns NULL on error. */
char *bf_read_file(const char *path, size_t *out_len);

/* Simple CLI argument check: returns 1 if --flag present. */
int  bf_arg_has(int argc, char **argv, const char *flag);

/* Get value after --key. Returns NULL if not found. */
const char *bf_arg_value(int argc, char **argv, const char *key);

/* ================================================================
 * Lightweight JSON extraction
 *
 * Not a full parser — extracts top-level string/int/double values
 * from flat JSON objects. Sufficient for manifest parsing.
 * ================================================================ */

/* Extract a string value for a top-level key. Returns 1 if found. */
int  bf_json_str(const char *json, const char *key, char *out, size_t out_sz);

/* Extract an integer value for a top-level key. Returns 1 if found. */
int  bf_json_int(const char *json, const char *key, int *out);

/* Extract a double value for a top-level key. Returns 1 if found. */
int  bf_json_double(const char *json, const char *key, double *out);

/* ================================================================
 * SIMD-accelerated primitives  (bf_simd.c)
 *
 * bf_json_scan_* — drop-in replacements for bf_json_* with a
 *   SIMD inner loop.  The json_len parameter enables bounded scan
 *   and lets the SIMD engine process 16–32 bytes per cycle instead
 *   of the byte-by-byte strstr path.  4–8× faster on manifests.
 *
 * bf_utf8_validate — 16-byte batch UTF-8 check.
 *   ASCII fast path: ceil(len/16) comparisons, NEON vmaxvq_u8.
 *
 * bf_base64_{encode,decode} — RFC 4648 with SIMD inner loop.
 *   NEON: 12 input bytes → 16 output chars per iteration (vld3/vst4).
 *   AVX2: 24 input bytes → 32 output chars per iteration.
 *
 * bf_csv_next_field — SIMD scan for ',' / '\n' delimiters.
 *   find_char2_simd skips field content 16–32 bytes/cycle.
 * ================================================================ */

/* SIMD JSON field extraction. Equivalent to bf_json_str/int/double
 * but uses SIMD to scan for '"' bytes 16–32 bytes/cycle.         */
int  bf_json_scan_str(const char *json, size_t json_len,
                      const char *key,  char *out, size_t out_sz);
int  bf_json_scan_int(const char *json, size_t json_len,
                      const char *key,  int *out);
int  bf_json_scan_double(const char *json, size_t json_len,
                         const char *key,  double *out);

/* UTF-8 batch validator.  Returns 1 if valid, 0 if not.
 * Processes 16 bytes per cycle on NEON/SSE2 (ASCII fast path).   */
int  bf_utf8_validate(const uint8_t *buf, size_t len);

/* Base64 encode/decode (RFC 4648).  Returns bytes written, -1 on error.
 * Processes 12–32 bytes per cycle depending on ISA.               */
int  bf_base64_encode(char *dst, size_t dst_sz,
                      const uint8_t *src, size_t src_len);
int  bf_base64_decode(uint8_t *dst, size_t dst_sz,
                      const char *src,    size_t src_len);

/* CSV SIMD field scanner.  Finds next ',' or '\n' in [p, end).
 * Returns pointer past the delimiter. Sets *field_start and *field_end. */
const char *bf_csv_next_field(const char *p,    const char *end,
                               const char **field_start,
                               const char **field_end);

/* ================================================================
 * Zero-copy mmap layer  (bf_mmap.c)
 *
 * bf_lmdb reads are pointer casts, not memcpy.
 * bf_bfrec_mmap returns a pointer directly into the mmap'd .bfrec
 * page — zero allocation, zero copy on the hot manifest-read path.
 * ================================================================ */

typedef struct {
    void   *ptr;  /* mmap base — cast directly, never copy */
    size_t  len;  /* file length in bytes                  */
    int     fd;   /* underlying fd (valid until close)     */
} BfMmapFile;

/* mmap a file read-only.  Returns 0 on success.
 * Caller must bf_mmap_close() when done.                          */
int  bf_mmap_open(BfMmapFile *m, const char *path);

/* Unmap and close.  Safe to call on a zeroed BfMmapFile.          */
void bf_mmap_close(BfMmapFile *m);

/* Zero-copy .bfrec read: mmap the record file, validate magic, and
 * return a typed pointer DIRECTLY into the mmap'd page.  No heap
 * allocation.  NULL on absent/corrupt file.  Caller must
 * bf_mmap_close(m) when done — pointer is invalid after that.     */
const BfBinaryRecord *bf_bfrec_mmap(const char *path, BfMmapFile *m);

/* Issue MADV_WILLNEED for each path to prefault pages asynchronously.
 * Call during pipeline setup before stages that access those files.
 * Returns number of paths successfully advised.                    */
int  bf_mmap_prefetch(const char * const *paths, int n);

/* ================================================================
 * Version
 * ================================================================ */

#define BONFYRE_VERSION_MAJOR 0
#define BONFYRE_VERSION_MINOR 1
#define BONFYRE_VERSION_PATCH 0
#define BONFYRE_VERSION "0.1.0"

/* ================================================================
 * SQLite helpers — link with -lsqlite3 to use
 * ================================================================ */
/* Forward declaration — compatible with sqlite3.h's own typedef.
 * C11 §6.7.8 allows identical typedef redeclarations; this is an
 * incomplete-struct pointer so no ABI conflict arises. */
#ifndef BONFYRE_SQLITE3_FWD_
#define BONFYRE_SQLITE3_FWD_
typedef struct sqlite3 sqlite3;
#endif

/* Open or create a SQLite database with the full Bonfyre PRAGMA bundle:
 *   journal_mode=WAL, synchronous=NORMAL, cache_size=-65536 (64 MB),
 *   mmap_size=268435456 (256 MB), temp_store=MEMORY.
 * Drop-in replacement for sqlite3_open(); same return codes. */
int bf_sqlite3_open(const char *path, sqlite3 **db);

/* Read-only open with cache/mmap/temp_store PRAGMAs.
 * Drop-in replacement for sqlite3_open_v2(...SQLITE_OPEN_READONLY...). */
int bf_sqlite3_open_ro(const char *path, sqlite3 **db);

/* Shared LayerArtifact runtime */
int bf_layer_resolve_root(const char *root, char *buf, size_t sz, char *attempted, size_t attempted_sz);
int bf_layer_state_db_path(const char *root, const char *db_name, char *buf, size_t sz);
int bf_layer_load_json(const char *root, const char *artifact_id, char **json_out);
int bf_layer_report_md(const char *artifact_json, char **out_md);
int bf_layer_auth_source_json(const char *artifact_json, char **out_json);
int bf_layer_gate_json(const char *artifact_json, const char *operation, char **out_json);
int bf_layer_tier_json(const char *artifact_json, char **out_json);
double bf_layer_estimated_cost(const char *operation);
int bf_layer_economy_json(const char *artifact_json, const char *operation, char **out_json);
int bf_layer_finance_json(const char *root, const char *artifact_id, char **out_json);
int bf_layer_pay_json(const char *artifact_id, const char *operation, char **out_json);
int bf_layer_moq_json(const char *artifact_json, char **out_json);
int bf_layer_rebuild_index(const char *root);
int bf_layer_query_json(const char *root,
                        const char *family,
                        const char *workflow,
                        const char *source,
                        const char *status,
                        const char *kind,
                        int bridge_required,
                        char **out_json);
int bf_layer_rebuild_graph(const char *root);
int bf_layer_graph_edges_json(const char *root, const char *artifact_id, char **out_json);
int bf_layer_graph_plan_json(const char *root, const char *plan_path, char **out_json);
int bf_layer_bridge_query_json(const char *root, const char *bridge_family, char **out_json);
int bf_layer_family_relations_json(const char *family_filter, char **out_json);
int bf_layer_compat_json(const char *root, const char *layer_a, const char *layer_b, char **out_json);
int bf_layer_compose_json(const char *root, const char *layer_a, const char *layer_b, int dry_run, char **out_json);
int bf_layer_queue_job_json(const char *root, const char *queue_cmd, const char *artifact_id, int priority, char **out_json);
int bf_layer_queue_plan_json(const char *root, const char *queue_cmd, const char *plan_path, int priority, char **out_json);
int bf_layer_queue_bridge_plan_json(const char *root, const char *queue_cmd, const char *plan_path, int priority, char **out_json);
int bf_layer_stitch_plan_json(const char *root, const char *layer_a, const char *layer_b, char **out_json);
int bf_layer_stitch_validate_json(const char *plan_json, char **out_json);
int bf_layer_stitch_validate_file(const char *plan_path, char **out_json);
int bf_layer_stitch_resolve_bridges_json(const char *root, const char *plan_path, char **out_json);
int bf_layer_stitch_composite_json(const char *virtual_composite_id, const char *out_dir, char **out_json);

/* Shared metadata catalog for first-class command surfaces. */
void bf_catalog_default_db_path(char *buf, size_t sz);
int bf_catalog_find_repo_root(char *buf, size_t sz);
int bf_catalog_sync_repo(const char *db_path, const char *repo_root);
int bf_catalog_sync_default(const char *db_path);
int bf_catalog_record_run_manifest(const char *db_path, const char *manifest_path);
int bf_catalog_projection_rules_json(char **out_json);
int bf_catalog_capability_tagging_rules_json(const char *filter, char **out_json);

/* ── Embedding cache (bf_embed_cache.c) ──────────────────────────────────── *
 *
 * Content-addressable store for float32 embedding vectors.
 * Keyed by SHA-256 of the input text. Every binary that links libbonfyre
 * shares the same on-disk store — the OS page cache handles cross-process
 * sharing. No daemon, no IPC, no env vars.
 *
 * bf_embed_lookup: returns 0 on hit (*out malloc'd by callee, caller frees,
 *                  *out_dim set). Returns -1 on miss.
 * bf_embed_store:  atomic rename-on-write. Idempotent — skips if already stored.
 */
int  bf_embed_lookup(const uint8_t hash[32], float **out, uint32_t *out_dim);
void bf_embed_store (const uint8_t hash[32], const float *vec, uint32_t dim);

/* ── KV-cache object store (bf_embed_cache.c) ─────────────────────────────── *
 *
 * Persists compressed KV blobs from bonfyre-kvcache. Keyed by
 * (model_hash, ctx_hash) so the same context can be stored under
 * different quantized model versions independently.
 *
 * bf_kvcache_store: 0 on success, -1 on I/O error. Idempotent.
 * bf_kvcache_fetch: 0 on hit (*out_data malloc'd, caller frees). -1 on miss.
 */
int bf_kvcache_store(const uint8_t model_hash[32], const uint8_t ctx_hash[32],
                     const void *data, size_t len);
int bf_kvcache_fetch(const uint8_t model_hash[32], const uint8_t ctx_hash[32],
                     void **out_data, size_t *out_len);

/* ── Context membrane planner (bf_context_plan.c) ───────────────────────── *
 *
 * Planner-only MVP for hybrid context selection.
 * Produces a deterministic JSON plan artifact without requiring model kernels.
 */

typedef struct {
    const char *mode;                    /* dense|compressed|membrane|state|hybrid */
    uint32_t dense_window_tokens;
    uint32_t sq_token_budget_blocks;
    uint32_t sq_kv_budget_mb;
    uint32_t compressed_kv_budget_mb;
    uint32_t state_atom_budget;
    uint32_t required_witnesses;
    int witness_required;
    int fail_on_missing_witness;
    double residual_drift_threshold;
    double residual_delta_estimate;
    int continuity_fail_events;
    int continuity_drift_events;
} BfContextPlanConfig;

typedef struct {
    char mode[24];
    uint32_t dense_window;
    uint32_t selected_token_blocks;
    uint32_t selected_state_atoms;
    uint32_t compressed_memory_atoms;
    uint32_t required_witnesses;
    uint32_t estimated_kv_mb;
    double residual_delta_estimate;
    char continuity_verdict[24];
} BfContextPlanSummary;

void bf_context_plan_defaults(BfContextPlanConfig *cfg);
int  bf_context_plan_summary(const BfContextPlanConfig *cfg, BfContextPlanSummary *out);
int  bf_context_plan_json(const BfContextPlanConfig *cfg, char **out_json);

/* ── Context KV registry (bf_context_registry.c) ────────────────────────── *
 *
 * Planner Phase 2: deterministic block-level KV registry artifact.
 */

typedef struct {
    const char *mode;            /* dense|compressed|membrane|state|hybrid */
    uint32_t layers;
    uint32_t heads;
    uint32_t seq_tokens;
    uint32_t block_tokens;
    uint32_t top_blocks;
    uint32_t required_witnesses;
    double base_residual;
    double continuity_fail_rate; /* 0..1 expected fraction of fail blocks */
} BfContextKVRegistryConfig;

void bf_context_kv_registry_defaults(BfContextKVRegistryConfig *cfg);
int  bf_context_kv_registry_json(const BfContextKVRegistryConfig *cfg, char **out_json);

/* ── KV commit chain (bf_embed_cache.c) ─────────────────────────────────── *
 *
 * A Merkle DAG of KV states: each state's hash transitively depends on
 * all its ancestors (like a git commit graph). This makes KV provenance
 * cryptographically verifiable.
 *
 * new_ctx_hash = SHA-256(model_hash || parent_ctx_hash || data)
 * parent_ctx_hash = {0x00 * 32} for the root of a new sequence.
 *
 * bf_kvcache_chain:     store + compute new_ctx_hash. Idempotent.
 * bf_kvcache_ancestry:  walk parent chain up to max_depth hashes.
 *                       Fills hashes[] oldest-first. Returns actual depth.
 */
int bf_kvcache_chain(const uint8_t model_hash[32],
                     const uint8_t parent_ctx_hash[32],
                     const void *data, size_t len,
                     uint8_t new_ctx_hash[32]);
int bf_kvcache_ancestry(const uint8_t model_hash[32],
                        const uint8_t ctx_hash[32],
                        uint8_t (*hashes)[32], int max_depth);

/* ── Embed pack (bf_embed_pack.c) ──────────────────────────────────────── *
 *
 * Consolidates loose .bfembed files into a single mmap-able pack file.
 * O(log n) binary search. All processes sharing the file path share the
 * same physical pages via the kernel page cache.
 *
 * Typical usage:
 *   bf_embed_pack_build(path, &n)  — pack all loose objects once
 *   bf_embed_pack_open(&pack, path) — mmap at process start
 *   bf_embed_lookup_fast(hash, &pack, &vec, &dim) — pack-first lookup
 *   bf_embed_pack_close(&pack)     — munmap at exit
 */
typedef struct BfEmbedPack {
    int           fd;
    void         *base;
    size_t        map_size;
    uint32_t      n;
    uint32_t      dim;
    const uint8_t *index_base;
    const float   *data_base;
} BfEmbedPack;

int          bf_embed_pack_build   (const char *pack_path, uint32_t *out_n);
int          bf_embed_pack_build_q8(const char *pack_path, uint32_t *out_n);
int          bf_embed_pack_open    (BfEmbedPack *pack, const char *pack_path);
const float *bf_embed_pack_lookup  (const BfEmbedPack *pack, const uint8_t hash[32]);
int          bf_embed_pack_get     (const BfEmbedPack *pack, const uint8_t hash[32],
                                    float **out, uint32_t *out_dim);
void         bf_embed_pack_close   (BfEmbedPack *pack);
int          bf_embed_lookup_fast  (const uint8_t hash[32], const BfEmbedPack *pack,
                                    float **out, uint32_t *out_dim);

/* Index-based accessors — the ONLY correct way to read pack entries.
 * BVH and Physics MUST use these instead of manual index_base arithmetic.
 *
 *   bf_embed_pack_vec_at  — fill float[pack->dim] for entry i.  0=ok, -1=err.
 *   bf_embed_pack_hash_at — return pointer to hash[32] for entry i (in mmap).
 */
int             bf_embed_pack_vec_at  (const BfEmbedPack *pack, uint32_t idx,
                                       float *out);
const uint8_t  *bf_embed_pack_hash_at (const BfEmbedPack *pack, uint32_t idx);

/* ── Steering vectors (bf_embed_steer.c) ───────────────────────────────── */
int bf_embed_steer_add  (const char *name, const float *delta, uint32_t dim);
int bf_embed_steer_apply(float *vec, uint32_t dim,
                         const char **names, const float *alphas, int n);
int bf_embed_steer_list (char ***out_names, int *out_count);

/* ── IVF-flat semantic index (bf_embed_index.c) ──────────────────────────── *
 *
 * ANN search via kmeans clustering + inverted file index. 16× faster
 * than brute-force at ~94% recall with k=64, n_probe=4.
 *
 * Build once after each pack rebuild; search in O(n_probe × avg_list × dim).
 * Stale index (pack grew) still gives partial recall; rebuild to restore.
 *
 * BfEmbedIndex is an mmap-backed struct; members are direct pointers into
 * the mapped region — zero copy, zero heap after open.
 */
typedef struct {
    int              fd;
    void            *base;
    size_t           map_size;
    uint32_t         n_centroids;
    uint32_t         dim;
    uint64_t         n_vectors;
    uint64_t         pack_n;          /* pack.n at build time */
    const float     *centroids;       /* float32[n_centroids × dim] */
    const uint32_t  *list_sizes;      /* uint32[n_centroids] */
    const uint8_t   *lists_base;      /* packed hash[32] per entry */
    uint64_t        *list_offsets;    /* prefix-sum table (malloc'd on open) */
} BfEmbedIndex;

typedef struct {
    uint8_t hash[32];
    float   score;        /* cosine similarity (inner product on L2-norm) */
} BfEmbedSearchResult;

int  bf_embed_index_build  (const char *pack_path, uint32_t k,
                             const char *index_path);
int  bf_embed_index_open   (BfEmbedIndex *idx, const char *index_path);
void bf_embed_index_close  (BfEmbedIndex *idx);
int  bf_embed_index_search (const BfEmbedIndex *idx, const BfEmbedPack *pack,
                             const float *query, uint32_t dim,
                             int top_k, int n_probe,
                             BfEmbedSearchResult *out, int *out_count);
int  bf_embed_brute_search (const BfEmbedPack *pack, const float *query,
                             uint32_t dim, int top_k,
                             BfEmbedSearchResult *out, int *out_count);

/* ── Named refs + reflog (bf_embed_refs.c) ──────────────────────────────── *
 *
 * git-style named refs and append-only reflog for the embed object store.
 *
 * refs/   → hash aliases, atomic rename-on-write
 * reflog  → chronological history of every embed stored
 */
typedef struct {
    char    timestamp[32];
    uint8_t hash[32];
    char    message[256];
} BfEmbedReflogEntry;

int bf_embed_ref_write    (const char *name, const uint8_t hash[32]);
int bf_embed_ref_read     (const char *name, uint8_t hash[32]);
int bf_embed_ref_delete   (const char *name);
int bf_embed_ref_list     (char ***out_names, int *out_count);
int bf_embed_reflog_append(const uint8_t hash[32], const char *message);
int bf_embed_reflog_read  (BfEmbedReflogEntry **out, int *out_count);
int bf_embed_reflog_trim  (int max_entries);

/* ── KV-cache pack (bf_kvcache_pack.c) ─────────────────────────────────── *
 *
 * Parallel to embed pack, but for variable-length KV blobs.
 * Keyed by (model_hash || ctx_hash). O(log n) binary search.
 * Zero I/O after open — returns pointer into mmap'd data.
 */
typedef struct {
    int           fd;
    void         *base;
    size_t        map_size;
    uint32_t      n;
    const uint8_t *index_base;  /* 80B per entry */
    const uint8_t *data_base;
} BfKVCachePack;

int          bf_kvcache_pack_build  (const char *pack_path, uint32_t *out_n);
int          bf_kvcache_pack_open   (BfKVCachePack *pack, const char *path);
const void  *bf_kvcache_pack_lookup (const BfKVCachePack *pack,
                                     const uint8_t model_hash[32],
                                     const uint8_t ctx_hash[32],
                                     uint64_t *out_len);
void         bf_kvcache_pack_close  (BfKVCachePack *pack);
int          bf_kvcache_pack_gc     (const BfKVCachePack *pack);

/* ── Ball-tree BVH (bf_embed_bvh.c) ─────────────────────────────────────── *
 *
 * Tier-1: 256-bit ternary sign sketch — POPCOUNT agreement check prunes
 *         entire BVH subtrees in O(1) before touching any float data.
 * Tier-2: INT8/float KDE gradient computation for nodes passing Tier-1.
 *
 * KDE potential:  V(q)  = −Σᵢ exp(−‖q−kᵢ‖²/2σ²)
 * KDE gradient:  ∇V(q)  = (1/σ²) Σᵢ (q−kᵢ) exp(−‖q−kᵢ‖²/2σ²)
 */
typedef struct {
    int              fd;
    void            *base;
    size_t           map_size;
    uint32_t         n_nodes;
    uint32_t         dim;
    uint64_t         n_vecs;
    const void      *nodes_base;    /* BVHNodeDisk array (opaque to callers) */
    const float     *centers;       /* float32[n_nodes × dim]                */
    const uint32_t  *idx_base;      /* pack indices in BVH traversal order   */
} BfEmbedBVH;

int  bf_embed_bvh_build    (const char *pack_path, const char *bvh_path);
int  bf_embed_bvh_open     (BfEmbedBVH *bvh, const char *path);
void bf_embed_bvh_close    (BfEmbedBVH *bvh);
int  bf_embed_bvh_gradient (const BfEmbedBVH *bvh, const BfEmbedPack *pack,
                             const float *q, uint32_t dim, float sigma,
                             float *out_grad, float *out_potential);
int  bf_embed_bvh_collide  (const BfEmbedBVH *bvh, const float *q,
                             uint32_t dim, uint32_t *out_indices,
                             int max_out, int *out_count);

/* ── Hamiltonian Leapfrog integrator (bf_physics.c) ─────────────────────── *
 *
 * Phase-space state (q, p) where q = current position on embedding manifold,
 * p = momentum (direction + speed of the "thought ball").
 *
 * Deterministic: same (q₀, p₀, σ, dt) always produces the same trajectory.
 * Symplectic: Leapfrog conserves Hamiltonian H = ½‖p‖² + V(q) up to O(dt²).
 *
 * Topological gap: bf_physics_step() returns +1 when ‖∇V(q)‖ ≈ 0, meaning
 * the query has left the populated region → caller should mount a new
 * sub-cache via bf_kvcache_mount_auto().
 */
typedef struct {
    float    *q;          /* position (embedding dim floats)   */
    float    *p;          /* momentum (embedding dim floats)   */
    float    *grad_buf;   /* internal scratch buffer           */
    uint32_t  dim;
    float     sigma;      /* KDE bandwidth                     */
    float     dt;         /* integration timestep              */
    uint64_t  step;       /* total steps taken                 */
    uint64_t  committed_epoch; /* last tran epoch that advanced physics;
                                * prevents double-advance in Newton iterations */
} BfPhysicsState;

BfPhysicsState *bf_physics_state_alloc       (uint32_t dim, float sigma, float dt);
void            bf_physics_state_free        (BfPhysicsState *s);
int             bf_physics_state_save        (const BfPhysicsState *s,
                                              const char *path);
BfPhysicsState *bf_physics_state_load        (const char *path);
int             bf_physics_init_from_embed   (BfPhysicsState *s,
                                              const float *embed, uint32_t dim);
int             bf_physics_kick              (BfPhysicsState *s,
                                              const float *impulse, float scale);
float           bf_physics_hamiltonian       (const BfPhysicsState *s,
                                              const BfEmbedBVH *bvh,
                                              const BfEmbedPack *pack);
int             bf_physics_step              (BfPhysicsState *s,
                                              const BfEmbedBVH *bvh,
                                              const BfEmbedPack *pack);
int             bf_physics_run               (BfPhysicsState *s,
                                              const BfEmbedBVH *bvh,
                                              const BfEmbedPack *pack,
                                              int max_steps, int *out_steps);
int             bf_physics_nearest           (const BfPhysicsState *s,
                                              const BfEmbedBVH *bvh,
                                              const BfEmbedPack *pack,
                                              int top_k,
                                              BfEmbedSearchResult *out,
                                              int *out_count);

/* ── KV sub-cache mounting (bf_kvcache_mount.c) ─────────────────────────── *
 *
 * VFS-style lazy mount of a foreign model's KV pack into the current process.
 * Mounted packs are read-only (MAP_SHARED|PROT_READ) — zero-copy checkout.
 * The Hamiltonian integrator treats mounted vectors as additional "mass"
 * in the potential field: hot-plug knowledge mid-inference.
 *
 * Path resolution order:
 *   ~/.local/share/bonfyre/kvcache/<hex>/pack.bfkvpack
 *   ~/.local/share/bonfyre/embeds/<hex>.bfpack
 *   ~/.local/share/bonfyre/embeds/pack.bfpack  (if hash == zeros)
 */
typedef struct {
    int      fd;
    void    *base;
    size_t   map_size;
    uint8_t  hash[32];   /* model_hash used as mount key */
    int      readonly;   /* always 1 for mounted packs   */
} BfKVMount;

int          bf_kvcache_mount        (const uint8_t hash[32], BfKVMount *out);
int          bf_kvcache_umount       (BfKVMount *mount);
void         bf_kvcache_umount_all   (void);
int          bf_kvcache_mount_list   (BfKVMount *out, int max_out, int *out_count);
const void  *bf_kvcache_mount_lookup (const uint8_t hash[32], size_t *out_size);
int          bf_kvcache_mount_auto   (const uint8_t model_hash[32],
                                      const uint8_t ctx_hash[32],
                                      BfKVMount *out_mount);

/* ── Entropy trace (bf_entropy_trace.c) ─────────────────────────────────── *
 *
 * Append-only JSONL trace of every Leapfrog step.  One JSON object per line:
 *   {"step":N,"q_hash":"<64hex>","H":F,"K":F,"V":F,"grad_norm":F,
 *    "gap":0|1,"mounted":[...],"candidates":N,"collisions":N,
 *    "nearest":[...],"entropy":F}
 *
 * Traces are replayable (same physics run → same file content).
 * Used for: trace-summary, diff, cherry-pick, rebase, branch-at.
 */
#define BF_TRACE_MAX_MOUNTS  8
#define BF_TRACE_MAX_NEAREST 8

typedef struct {
    uint64_t     step;
    const uint8_t *q_hash;          /* SHA-256 of quantized q [32] */
    float        H, K, V;           /* Hamiltonian, kinetic, potential */
    float        grad_norm;
    int          gap;
    const char  *mounted[BF_TRACE_MAX_MOUNTS];
    int          n_mounted;
    int          candidates;
    int          collisions;
    const char  *nearest[BF_TRACE_MAX_NEAREST];
    int          n_nearest;
    float        entropy;
} BfTraceEvent;

typedef struct {
    FILE    *fp;
    char     path[4096];
    uint64_t events;
} BfEntropyTrace;

BfEntropyTrace *bf_trace_open      (const char *path, int append);
void            bf_trace_close     (BfEntropyTrace *t);
int             bf_trace_write     (BfEntropyTrace *t, const BfTraceEvent *ev);
int             bf_trace_summary   (const char *path, FILE *out);
int             bf_trace_iterate   (const char *path,
                                    int (*cb)(const char *line, void *ctx),
                                    void *ctx);
uint64_t        bf_trace_gap_step  (const char *path, int gap_n);

/* ── Trajectory entropy accumulator (bf_trajectory_entropy.c) ───────────── *
 *
 * S_runtime = λ1·|H_t−H_0| + λ2·gap_count + λ3·mean_gap_dur
 *           + λ4·log(mean_cand) + λ5·branch_div + λ6·mounts + λ7·target_dist
 *
 * A clean run scores near 0. A chaotic run scores > 7.
 * Entropy is billable: chaotic runs cost more in bonfyre-meter.
 */
typedef struct {
    float    H_0;              /* initial Hamiltonian */
    float    H_last;           /* H at last update */
    float    H_drift;          /* max |H - H_0| seen */
    uint32_t gap_count;        /* number of gap events */
    uint32_t gap_steps_total;  /* cumulative steps inside gaps */
    int      in_gap;           /* currently in a gap? */
    uint64_t gap_start_step;   /* step when current gap began */
    uint32_t mount_count;      /* sub-cache mounts performed */
    uint32_t branch_count;     /* branches forked */
    double   log_cand_sum;     /* Σ log(candidates+1) */
    uint32_t log_cand_n;       /* events counted */
    float    target_distance;  /* distance to target basin (set externally) */
    uint32_t step_count;       /* total steps processed */
    float    lam[7];           /* λ1..λ7 */
} BfEntropyAccum;

void  bf_entropy_init          (BfEntropyAccum *a, float H0);
void  bf_entropy_set_lambdas   (BfEntropyAccum *a, const float lam[7]);
void  bf_entropy_update_step   (BfEntropyAccum *a, float H, int gap,
                                int new_mounts, int candidates);
void  bf_entropy_update_branch (BfEntropyAccum *a, int new_branches);
void  bf_entropy_set_target    (BfEntropyAccum *a, float target_distance);
float bf_entropy_score         (const BfEntropyAccum *a);
int   bf_entropy_report        (const BfEntropyAccum *a, FILE *out);

/* ═══════════════════════════════════════════════════════════════════════
 * NODAL CIRCUIT RUNTIME
 * Mixed-signal inference runtime.
 *
 *   Digital side : hashes, refs, contracts, mounts, events
 *   Analog side  : q, p, V(q), H, entropy, conductance, tolerance
 *
 * Bonfyre binaries are not pipeline stages.  They are components in a
 * mixed-signal circuit.  The KV cache is the substrate.  The netlist
 * is the circuit.  The attention ball is the signal.
 * ═══════════════════════════════════════════════════════════════════════ */

/* ── Pin types ──────────────────────────────────────────────────────── */
typedef enum {
    BF_PIN_SIGNAL  = 0,   /* live audio / event / embedding signal   */
    BF_PIN_STATE   = 1,   /* phase-space state (q,p,H)               */
    BF_PIN_MEMORY  = 2,   /* mounted KV pack                         */
    BF_PIN_COST    = 3,   /* entropy / meter reading                 */
    BF_PIN_PROOF   = 4,   /* content hash / commit hash              */
    BF_PIN_RULE    = 5,   /* DisCIPL rule set                        */
    BF_PIN_VALUE   = 6,   /* ledger value event                      */
    BF_PIN_STREAM  = 7    /* MoQ / live byte stream                  */
} BfPinKind;

#define BF_PIN_FLAG_OPTIONAL  0x01u   /* net may leave pin unconnected */
#define BF_PIN_FLAG_BUFFERED  0x02u   /* signal is buffered / latched  */
#define BF_PIN_FLAG_BROADCAST 0x04u   /* fan-out to all connected pins  */

typedef struct {
    char      name[64];
    BfPinKind kind;
    uint32_t  dim;          /* vector dimension (0 = scalar / event) */
    float     impedance;    /* analog: source/load impedance         */
    float     tolerance;    /* noise / uncertainty floor             */
    uint64_t  flags;
} BfPin;

/* ── Component descriptor ───────────────────────────────────────────── */
#define BF_COMPONENT_MAX_PINS 16

/* ── Typed signal payload ──────────────────────────────────────────── *
 *
 * Every net edge in the circuit carries a BfSignal.  The payload union
 * lets a signal carry more than a float pointer: a memory field
 * (KV mount + BVH + pack), a physics state, a HE-SLI result, etc.
 *
 * Rules:
 *   - payload_kind == BF_PAYLOAD_NONE  →  data/dim are the canonical value
 *   - payload_kind == BF_PAYLOAD_MEMORY_FIELD  →  payload → BfMemoryField*
 *   - payload_kind == BF_PAYLOAD_PHYSICS_STATE →  payload → BfPhysicsState*
 *   - payload_kind == BF_PAYLOAD_HESLI_RESULT  →  payload → BfHeSliResult*
 * Ownership: payload pointer is BORROWED (lifetime = current eval frame).
 * ─────────────────────────────────────────────────────────────────── */
typedef enum {
    BF_PAYLOAD_NONE          = 0,
    BF_PAYLOAD_SCALAR        = 1,
    BF_PAYLOAD_VEC_F32       = 2,
    BF_PAYLOAD_HASH          = 3,
    BF_PAYLOAD_MEMORY_FIELD  = 4,
    BF_PAYLOAD_PHYSICS_STATE = 5,
    BF_PAYLOAD_HESLI_RESULT  = 6,
    BF_PAYLOAD_TRACE_EVENT   = 7,
    BF_PAYLOAD_METER_EVENT   = 8
} BfPayloadKind;

/* ── Payload ownership flags ──────────────────────────────────────────
 *
 * BfSignal.payload_flags answers: who owns this pointer and how long
 * is it valid? Every component that emits a payload MUST set these.
 *
 *   BF_PAYLOAD_F_BORROWED  — pointer is borrowed from the emitting
 *                            component's priv. Do NOT free. Do NOT cache
 *                            past the current eval frame without copying.
 *
 *   BF_PAYLOAD_F_OWNED     — this signal transfered ownership. The
 *                            receiving component MUST call the appropriate
 *                            destructor when done (e.g. free(payload) or
 *                            bf_physics_state_free(payload)).
 *
 *   BF_PAYLOAD_F_MMAP      — pointer into a memory-mapped region (pack,
 *                            BVH). Do NOT free. Valid as long as the
 *                            BfEmbedPack/BfEmbedBVH handle is open.
 *
 *   BF_PAYLOAD_F_READONLY  — caller must not mutate payload through
 *                            this pointer. Cast-away-const is a bug.
 *
 *   BF_PAYLOAD_F_FRAME     — valid ONLY during the current bf_spice_eval
 *                            call. A component must NOT cache this pointer
 *                            into st->priv or any persistent storage.
 *
 * Default (flags == 0) == BF_PAYLOAD_F_BORROWED | BF_PAYLOAD_F_FRAME:
 *   treat as read-only borrowed pointer valid for one frame.
 * ─────────────────────────────────────────────────────────────────── */
#define BF_PAYLOAD_F_BORROWED  0x01u
#define BF_PAYLOAD_F_OWNED     0x02u
#define BF_PAYLOAD_F_MMAP      0x04u
#define BF_PAYLOAD_F_READONLY  0x08u
#define BF_PAYLOAD_F_FRAME     0x10u

/* ── Payload lifetime ─────────────────────────────────────────────────
 *
 * Orthogonal to flags: the MAXIMUM scope for which the pointer is valid.
 *   FRAME   — only during the current bf_spice_eval() call
 *   STEP    — until the next bf_spice_eval() call
 *   CIRCUIT — as long as the owning BfCircuit is alive
 *   STATIC  — global / mmap-backed, never freed at runtime
 * ─────────────────────────────────────────────────────────────────── */
typedef enum {
    BF_PAYLOAD_LIFE_FRAME   = 0,
    BF_PAYLOAD_LIFE_STEP    = 1,
    BF_PAYLOAD_LIFE_CIRCUIT = 2,
    BF_PAYLOAD_LIFE_STATIC  = 3
} BfPayloadLifetime;

typedef struct {
    float  *data;          /* float32 vector (dim floats) or NULL          */
    uint32_t dim;          /* vector dimension                             */
    BfPinKind kind;        /* pin semantic kind (BF_PIN_*)                 */
    uint8_t  hash[32];     /* content hash — PROOF / MEMORY pins           */
    float    scalar;       /* convenience scalar (COST / VALUE / analog)   */
    int      event;        /* 1 = digital event present this step          */
    void    *payload;      /* typed payload pointer (see BfPayloadKind)    */
    int      payload_kind; /* BF_PAYLOAD_* tag (0 = none)                  */
    uint8_t  payload_flags;/* BF_PAYLOAD_F_* bitmask (0 = borrowed+frame)  */
    uint8_t  payload_life; /* BfPayloadLifetime (0 = FRAME)                */
    uint32_t payload_size; /* sizeof(*payload); 0 = unknown/variable       */
} BfSignal;

typedef struct {
    void  *priv;           /* component private state pointer           */
    float  conductance;    /* how easily signal flows through           */
    float  capacitance;    /* state storage scale                       */
    float  inductance;     /* momentum / history inertia                */
    float  tolerance;      /* noise floor                               */
    float  aging_rate;     /* conductance decay per step                */
    float  trust;          /* source confidence (0..1)                  */
    uint64_t step;         /* current step count                        */
    uint64_t last_touched; /* ns timestamp of last activation           */
} BfComponentState;

/* \u2500\u2500 Memory field \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500 *
 *
 * BfMemoryField is the typed payload emitted by BonfyreKVCache.\n * It is transmitted via BfSignal.payload (payload_kind = BF_PAYLOAD_MEMORY_FIELD).
 * BonfyrePhysics reads in[1].payload to get bvh + pack for bf_physics_step.
 *
 * Lifetime: owned by the BonfyreKVCache component (st->priv).
 * Downstream nodes borrow it for one eval frame.
 * \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500 */
typedef struct {
    uint8_t       ctx_hash[32];    /* SHA-256 of context key                */
    uint8_t       model_hash[32];  /* SHA-256 of model identifier           */
    BfEmbedPack  *embed_pack;      /* live pack pointer (NOT owned)         */
    BfEmbedBVH   *bvh;             /* live BVH  pointer (NOT owned)         */
    uint32_t      n_mounts;        /* number of active KV mounts            */
    int           readonly;        /* 1 = field is read-only this frame     */
} BfMemoryField;

/* Transfer function prototype \u2014 every component exposes exactly one.
 * Returns BF_SPICE_* code. */
typedef int (*BfTransferFn)(
    const BfSignal *inputs,  size_t n_inputs,
    BfSignal       *outputs, size_t n_outputs,
    BfComponentState *state
);

typedef struct {
    char          name[64];       /* e.g. "BonfyrePhysics"          */
    uint32_t      n_inputs;
    uint32_t      n_outputs;
    BfPin         inputs [BF_COMPONENT_MAX_PINS];
    BfPin         outputs[BF_COMPONENT_MAX_PINS];
    BfTransferFn  transfer;       /* NULL = stub / passthrough        */
} BfComponentDef;

/* ── BfKvElectricalMeta — component aging ───────────────────────────── *
 * A KV commit has electrical properties:                                *
 *   fresh context     → high conductance                               *
 *   old context       → low conductance                                *
 *   frequently useful → reinforced conductance                         *
 *   stale branch      → higher resistance                              *
 *   trusted source    → lower noise                                    *
 *   uncertain source  → higher tolerance                               */
typedef struct {
    uint8_t  ctx_hash   [32];
    uint8_t  parent_hash[32];

    float    conductance;    /* attention flows through this commit      */
    float    capacitance;    /* state it stores                          */
    float    inductance;     /* momentum / history it preserves          */
    float    tolerance;      /* noise / uncertainty floor                */
    float    aging_rate;     /* conductance decay per step               */
    float    trust;          /* source confidence                        */

    uint64_t created_ns;
    uint64_t last_touched_ns;
} BfKvElectricalMeta;

/* ── Netlist (.bfnet) ───────────────────────────────────────────────── */
#define BF_NETLIST_MAX_COMPONENTS 64
#define BF_NETLIST_MAX_NETS       256
#define BF_NETLIST_MAX_PROBES     32

typedef struct {
    char   instance[64];    /* e.g. "phy0"            */
    char   type    [64];    /* e.g. "BonfyrePhysics"  */
    char   params  [512];   /* raw key=value string   */
} BfNetComponent;

typedef struct {
    char   src[128];        /* "tel0.signal"          */
    char   dst[128];        /* "emb0.input"           */
    char   net_type[32];    /* "signal","vector",etc  */
} BfNetWire;

typedef struct {
    char   world[64];
    uint32_t n_components;
    uint32_t n_wires;
    uint32_t n_probes;
    BfNetComponent components[BF_NETLIST_MAX_COMPONENTS];
    BfNetWire      wires     [BF_NETLIST_MAX_NETS];
    char           probes    [BF_NETLIST_MAX_PROBES][128];
    /* .TRAN params */
    uint64_t tran_start;
    uint64_t tran_end;
    float    tran_dt;
    /* .ON_GAP / .ON_FAIL */
    char     on_gap [128];
    char     on_fail[128];
} BfNetlist;

int  bf_netlist_parse  (const char *path, BfNetlist *out);
int  bf_netlist_check  (const BfNetlist *nl, FILE *err);
void bf_netlist_print  (const BfNetlist *nl, FILE *fp);

/* ── Compiled circuit (.bfcircuit) ──────────────────────────────────── */
#define BF_CIRCUIT_MAGIC 0x54524943u   /* "CIRC" */

/* Per-instance runtime state slot */
typedef struct {
    char             instance[64];
    char             type    [64];
    BfComponentState state;
    BfSignal         input_buf [BF_COMPONENT_MAX_PINS];
    BfSignal         output_buf[BF_COMPONENT_MAX_PINS];
    uint32_t         n_inputs;
    uint32_t         n_outputs;
} BfCircuitNode;

/* A net: connects one output pin to one or more input pins */
typedef struct {
    char   src_instance[64];
    uint8_t src_pin;
    char   dst_instance[64];
    uint8_t dst_pin;
    char   net_type[32];
} BfCircuitEdge;

typedef struct {
    uint32_t       magic;
    char           world[64];
    uint32_t       n_nodes;
    uint32_t       n_edges;
    BfCircuitNode *nodes;
    BfCircuitEdge *edges;
    /* probe list */
    uint32_t       n_probes;
    char           probe_names[BF_NETLIST_MAX_PROBES][128];
} BfCircuit;

BfCircuit *bf_circuit_compile (const BfNetlist *nl);
BfCircuit *bf_circuit_load    (const char *path);
int        bf_circuit_save    (const BfCircuit *c, const char *path);
void       bf_circuit_free    (BfCircuit *c);
void       bf_circuit_print   (const BfCircuit *c, FILE *fp);

/* ── Transient state ────────────────────────────────────────────────── */
typedef struct {
    uint64_t step;
    float    t;
    float    dt;
    uint64_t tran_end;
    int      converged;
    /* Two-buffer iterative scheme: prev holds last committed step,
     * next accumulates current iteration output.  Swapped per iteration.
     * Convergence: max|next[i] - prev[i]| < tolerance across all pins. */
    float   *analog;      /* n_nodes × BF_COMPONENT_MAX_PINS (current/next) */
    float   *prev_analog; /* n_nodes × BF_COMPONENT_MAX_PINS (previous)     */
    uint32_t n_nodes;
    int      n_iters;     /* Newton iterations taken last step              */
} BfTranState;

BfTranState *bf_tran_state_alloc (const BfCircuit *c, float dt, uint64_t end);
void         bf_tran_state_free  (BfTranState *s);

/* ── Probe frame ────────────────────────────────────────────────────── */
typedef struct {
    uint64_t  step;
    float     t;
    uint32_t  n_probes;
    char      names [BF_NETLIST_MAX_PROBES][128];
    float     values[BF_NETLIST_MAX_PROBES];
    uint8_t   hashes[BF_NETLIST_MAX_PROBES][32];
    uint8_t   is_hash[BF_NETLIST_MAX_PROBES]; /* 1 = hash field valid */
    int       status;     /* BF_SPICE_* */
} BfProbeFrame;

/* ── Input pulse ────────────────────────────────────────────────────── */
typedef struct {
    const float *data;    /* signal vector (or NULL for event-only)   */
    uint32_t     dim;
    uint8_t      hash[32];
    int          event;
} BfInputPulse;

/* ── SPICE eval return codes ────────────────────────────────────────── */
#define BF_SPICE_OK              0
#define BF_SPICE_NOT_CONVERGED   1
#define BF_SPICE_TOPO_GAP        2
#define BF_SPICE_MOUNTED         3
#define BF_SPICE_CONTRACT_BLOCK  4
#define BF_SPICE_NUMERIC_FAULT   5

/* ── Main transient step ─────────────────────────────────────────────
 *
 * Evaluates one step of the global circuit:
 *   1. Propagate digital events
 *   2. Resolve mounts / refs / contracts
 *   3. Assemble sparse active analog matrix
 *   4. Apply source current / query
 *   5. Solve active nodal state (Newton / leapfrog / MNA)
 *   6. Emit probe frame
 *   7. Commit changed state
 *   8. Meter / ledger / hash
 * ─────────────────────────────────────────────────────────────────── */
int bf_spice_eval (BfCircuit *c, BfTranState *s,
                   const BfInputPulse *input, BfProbeFrame *out);

/* ── Component registry ─────────────────────────────────────────────── */
int  bf_component_register (const BfComponentDef *def);
const BfComponentDef *bf_component_lookup (const char *type_name);
void bf_component_registry_init (void);  /* register all built-in types */
int  bf_component_registry_list (const BfComponentDef **out, int max_n);

/* ═══════════════════════════════════════════════════════════════════════
 * HE-SLI: DIELECTRIC ISOLATION LAYER
 *
 * HE-SLI provides privacy-preserving field coupling between Bonfyre
 * components.  Private memory, sealed expert circuits, and partner-owned
 * policy subcircuits can influence global trajectory, routing, and
 * convergence without exposing their underlying state.
 *
 *   V_total(q) = V_public(q) + V_private_encrypted(q) + V_policy_sealed(q)
 *
 * In circuit terms: HE-SLI is a dielectric.
 * It allows coupling without leakage.
 *
 * Four isolation levels:
 *   HASH_ONLY     — only content addresses cross the boundary
 *   SKETCH        — 256-bit ternary sign sketch, no raw floats
 *   HE_VECTOR     — simulated encrypted vector evaluation
 *   LOCAL_ENCLAVE — full local eval, output-filtered by policy
 *
 * Marketable sealed subcircuits (.hebfsubckt):
 *   mount legal-safe-v3.hebfsubckt
 *   mount tax-code-2026.hebfsubckt
 *   mount medical-triage.hebfsubckt
 *   mount brand-voice-private.hebfsubckt
 * ═══════════════════════════════════════════════════════════════════════ */

typedef enum {
    BF_HESLI_HASH_ONLY     = 0, /* only content addresses cross boundary  */
    BF_HESLI_SKETCH        = 1, /* 256-bit ternary sign sketch, no floats */
    BF_HESLI_HE_VECTOR     = 2, /* protected vector eval, sanitized output*/
    BF_HESLI_LOCAL_ENCLAVE = 3  /* full local eval, output-filtered       */
} BfHeSliLevel;

/* Output kinds a sealed component is allowed to emit across boundary */
#define BF_HESLI_OUT_GATE          0x01u  /* allowed/denied (1 bit)            */
#define BF_HESLI_OUT_BASIN         0x02u  /* nearest attractor (32-byte hash)  */
#define BF_HESLI_OUT_DISTANCE      0x04u  /* distance bucket 0-7               */
#define BF_HESLI_OUT_RISK          0x08u  /* risk score 0.0-1.0                */
#define BF_HESLI_OUT_ENTROPY_DELTA 0x10u  /* signed entropy change             */
#define BF_HESLI_OUT_MOUNT         0x20u  /* mount yes/no + opaque handle      */
#define BF_HESLI_OUT_PROOF         0x40u  /* commitment/proof token (32 bytes) */
#define BF_HESLI_OUT_METER         0x80u  /* billable units this eval          */

/* Default safe output mask: gate + distance bucket + entropy */
#define BF_HESLI_OUT_DEFAULT \
    (BF_HESLI_OUT_GATE | BF_HESLI_OUT_DISTANCE | BF_HESLI_OUT_ENTROPY_DELTA)

typedef struct {
    BfHeSliLevel  level;
    uint32_t      allowed_outputs; /* BF_HESLI_OUT_* bitmask              */
    float         meter_rate;      /* billable units per eval call         */
    char          seal_key[64];    /* HMAC key handle (empty = no HMAC)   */
    char          name[64];
} BfHeSliPolicy;

/* Observable result that crosses the HE-SLI boundary.
 *
 * HE-SLI is a dielectric: it allows coupling without leakage.
 * The FORCE outputs let the private field actually BEND the trajectory
 * without exposing the underlying embedding vectors.
 *
 *   force_bucket      — quantized force magnitude (0=repulsive … 7=attract)
 *   potential_delta   — signed change in V(q) from this private field
 *   projected_force[] — 8-dim low-rank force projection for Physics.
 *                        Basis is canonical; raw q is never recoverable.
 */
typedef struct {
    int      gate;             /* 1 = allowed, 0 = denied                  */
    uint8_t  basin_id[32];     /* nearest attractor (content address)       */
    uint8_t  distance_bucket;  /* 0 = nearest … 7 = farthest               */
    float    risk_score;       /* 0.0 safe → 1.0 high-risk                 */
    float    entropy_delta;    /* signed change in trajectory entropy       */
    int      mount_yes;        /* 1 = private field can fill a gap         */
    char     mount_handle[128];/* opaque handle for bf_kvcache_mount_auto   */
    uint8_t  proof[32];        /* commitment token (HMAC of inner eval)     */
    float    meter_units;      /* usage to bill this step                   */
    /* ── Physics coupling outputs (cross boundary, do NOT leak raw q) ── */
    uint8_t  force_bucket;     /* 0=strongly repulsive … 7=strongly attract */
    float    potential_delta;  /* ΔV(q) from this private field             */
    float    projected_force[8]; /* low-rank force: 8-dim canonical projection */
} BfHeSliResult;

/* On-disk sealed subcircuit header (.hebfsubckt)
 *
 *   [BfHebfSubckt header] [inner circuit blob (size = inner_size)]
 *
 *   seal_hash = SHA-256(inner_blob)
 *   hmac      = HMAC-SHA256(seal_key, header || inner_blob)
 *             = 0x00…00 when seal_key is empty
 */
#define BF_HEBFSUBCKT_MAGIC   0x544B4253u /* "SBKT" little-endian             */
#define BF_HEBFSUBCKT_VERSION 1

typedef struct {
    uint32_t     magic;
    uint32_t     version;
    uint32_t     level;           /* BfHeSliLevel                          */
    uint32_t     allowed_outputs; /* BF_HESLI_OUT_* bitmask                */
    uint8_t      seal_hash[32];   /* SHA-256 of appended inner blob        */
    uint8_t      hmac[32];        /* HMAC-SHA256(seal_key, hdr||blob)      */
    uint32_t     inner_size;      /* byte size of appended inner blob      */
    float        meter_rate;
    char         name[64];
    char         description[256];
} BfHebfSubckt;

/* Loaded sealed subcircuit (in-memory, not persisted) */
typedef struct {
    BfHebfSubckt  header;
    uint8_t      *inner_blob; /* raw inner .bfcircuit bytes (opaque)       */
    size_t        inner_size;
    BfCircuit    *inner;      /* decoded inner circuit (lazy, NULLable)    */
} BfHeSliSubckt;

/* ── HE-SLI API ─────────────────────────────────────────────────────── */

/* Core boundary evaluation.
 * Applies the HE-SLI isolation at the given level.  Returns BF_SPICE_OK
 * (gate=1) or BF_SPICE_CONTRACT_BLOCK (gate=0). */
int  bf_hesli_eval   (const float *q, uint32_t dim,
                      const BfHeSliPolicy *policy,
                      BfHeSliResult *out);

/* Convert a BfHeSliResult into a BfSignal for circuit injection. */
void bf_hesli_result_to_signal (const BfHeSliResult *r,
                                 uint32_t allowed_outputs,
                                 BfSignal *out_signal);

/* Seal a compiled circuit as a .hebfsubckt file.
 * Returns 0 on success. */
int  bf_hesli_seal   (const BfCircuit *c,
                      const BfHeSliPolicy *policy,
                      const char *description,
                      const char *out_path);

/* Load an unsealed subcircuit (no HMAC key).  Rejects files that were
 * sealed with a key — use bf_hesli_load_keyed() for those.
 * Caller must bf_hesli_free() it. */
BfHeSliSubckt *bf_hesli_load       (const char *path);

/* Load a key-sealed subcircuit.  Verifies HMAC-SHA256(key, hdr||blob)
 * in constant time before accepting the file.  Caller must
 * bf_hesli_free() it. */
BfHeSliSubckt *bf_hesli_load_keyed (const char *path, const char *seal_key);

/* Evaluate a sealed subcircuit.
 * Decodes inner circuit on first call (lazy).
 * Returns BF_SPICE_OK or BF_SPICE_CONTRACT_BLOCK. */
int  bf_hesli_subckt_eval (BfHeSliSubckt *s,
                            const BfInputPulse *input,
                            BfHeSliResult *out);

/* Ask a sealed subcircuit whether it can fill a topological gap at q.
 * Returns 1 if the private field has relevant curvature, 0 if not. */
int  bf_hesli_gap_query (BfHeSliSubckt *s,
                          const float *q, uint32_t dim,
                          BfHeSliResult *out);

void bf_hesli_free (BfHeSliSubckt *s);

#include "bonfyre/bf_discipl.h"

#ifdef __cplusplus
}
#endif

#endif /* BONFYRE_H */
