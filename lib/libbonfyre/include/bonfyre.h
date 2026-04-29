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

#include "bonfyre/bf_discipl.h"

#ifdef __cplusplus
}
#endif

#endif /* BONFYRE_H */
