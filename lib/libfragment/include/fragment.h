/*
 * fragment.h — Bonfyre Fragment System
 *
 * A Fragment is the atomic unit of perspective in Bonfyre. Any piece of
 * knowledge — a claim, a transcript segment, a detection, a hypothesis —
 * is stored as a Fragment with:
 *
 *   - A unique content-addressed ID  (SHA-256 of canonical JSON)
 *   - A kind                         (claim, event, entity, hypothesis, ...)
 *   - A perspective tag              (source system, author, model, ...)
 *   - A confidence score             [0.0, 1.0]
 *   - A payload                      (arbitrary JSON blob)
 *   - Parent linkage                 (derived-from relationships)
 *   - Temporal bounds                (start_ms, end_ms for media)
 *
 * Operations:
 *   create  — intern a new fragment, return its ID
 *   get     — retrieve fragment by ID
 *   merge   — union two perspective sets, resolve conflicts
 *   diff    — enumerate disagreements between two perspective sets
 *   query   — filter + rank fragments by kind/confidence/tag/time
 *
 * Storage:
 *   SQLite + WAL, one row per fragment, JSONB payload.
 *   Index: (kind, perspective, confidence DESC), (start_ms, end_ms)
 *
 * Thread safety: each bf_fragment_store_t is single-writer (caller serialises).
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Constants ──────────────────────────────────────────────────── */

#define BF_FRAGMENT_ID_LEN   73   /* "sha256:" (7) + 64 hex chars + '\0' = 72, +1 safety */
#define BF_FRAGMENT_MAX_KIND 32
#define BF_FRAGMENT_MAX_PERSP 128
#define BF_FRAGMENT_MAX_PARENTS 8

/* Fragment kinds (open set — use string constants) */
#define BFK_CLAIM       "claim"
#define BFK_EVENT       "event"
#define BFK_ENTITY      "entity"
#define BFK_HYPOTHESIS  "hypothesis"
#define BFK_DETECTION   "detection"
#define BFK_SEGMENT     "segment"
#define BFK_STATEMENT   "statement"
#define BFK_ANNOTATION  "annotation"

/* ── Core types ─────────────────────────────────────────────────── */

typedef struct bf_fragment_store bf_fragment_store_t;

/* Immutable fragment descriptor (returned by query, never mutated) */
typedef struct {
    char    id[BF_FRAGMENT_ID_LEN];          /* content-addressed SHA-256 */
    char    kind[BF_FRAGMENT_MAX_KIND];      /* claim | event | entity | … */
    char    perspective[BF_FRAGMENT_MAX_PERSP]; /* whisper-base | user:alice | … */
    float   confidence;                       /* 0.0 – 1.0 */
    int64_t start_ms;                         /* media start (−1 = none) */
    int64_t end_ms;                           /* media end  (−1 = none) */
    int64_t created_at;                       /* Unix epoch seconds */
    char    parent_ids[BF_FRAGMENT_MAX_PARENTS][BF_FRAGMENT_ID_LEN];
    int     parent_count;
    char   *payload_json;                     /* heap-allocated, caller frees */
} bf_fragment_t;

/* Query filter — zero-value = "any" */
typedef struct {
    const char *kind;         /* NULL = any kind */
    const char *perspective;  /* NULL = any perspective */
    float  min_confidence;    /* 0.0 = any */
    int64_t start_after_ms;   /* −1 = any */
    int64_t end_before_ms;    /* −1 = any */
    int     limit;            /* 0 = unlimited */
    int     offset;           /* pagination */
} bf_fragment_query_t;

/* Diff record — represents a disagreement between two perspective sets */
typedef struct {
    char    kind[BF_FRAGMENT_MAX_KIND];
    char    perspective_a[BF_FRAGMENT_MAX_PERSP];
    char    perspective_b[BF_FRAGMENT_MAX_PERSP];
    char    id_a[BF_FRAGMENT_ID_LEN];  /* empty if only in B */
    char    id_b[BF_FRAGMENT_ID_LEN];  /* empty if only in A */
    float   confidence_a;
    float   confidence_b;
    float   conflict_score;  /* 0 = same, 1 = completely opposite */
    char   *summary;         /* heap-allocated human-readable summary */
} bf_fragment_diff_t;

/* Merge result */
typedef struct {
    int fragments_added;      /* new fragments ingested from source */
    int conflicts_detected;   /* fragments with same kind+time but different payload */
    int conflicts_resolved;   /* resolved by confidence-weighted merge */
    int conflicts_deferred;   /* kept as competing perspectives */
} bf_merge_result_t;

/* ── Store lifecycle ────────────────────────────────────────────── */

/* Open (or create) a fragment store at path.
 * Returns NULL on failure; call bf_fragment_store_errmsg() for details. */
bf_fragment_store_t *bf_fragment_store_open(const char *path);

/* Close and free. */
void bf_fragment_store_close(bf_fragment_store_t *store);

/* Human-readable error from last failed call. */
const char *bf_fragment_store_errmsg(const bf_fragment_store_t *store);

/* ── CRUD ───────────────────────────────────────────────────────── */

/* Intern a fragment. If an identical content-hash already exists, returns its
 * existing ID (idempotent). Fills id_out (must be BF_FRAGMENT_ID_LEN bytes).
 * Returns 0 on success, −1 on error. */
int bf_fragment_create(bf_fragment_store_t *store,
                       const char *kind,
                       const char *perspective,
                       float confidence,
                       int64_t start_ms,
                       int64_t end_ms,
                       const char *payload_json,
                       const char **parent_ids,
                       int parent_count,
                       char id_out[BF_FRAGMENT_ID_LEN]);

/* Retrieve one fragment by ID. Returns NULL if not found.
 * Caller must call bf_fragment_free() on the result. */
bf_fragment_t *bf_fragment_get(bf_fragment_store_t *store, const char *id);

/* Free a fragment returned by bf_fragment_get() or query. */
void bf_fragment_free(bf_fragment_t *frag);

/* ── Query ──────────────────────────────────────────────────────── */

/* Run a filter query. Results ranked by confidence DESC.
 * Returns array of bf_fragment_t* (NULL-terminated); caller frees each
 * element and the array itself.
 * Returns NULL on error; count set to −1. */
bf_fragment_t **bf_fragment_query(bf_fragment_store_t *store,
                                  const bf_fragment_query_t *filter,
                                  int *count_out);

/* ── Merge ──────────────────────────────────────────────────────── */

/* Merge all fragments from src into dst.
 * Conflict resolution:
 *   - If both stores have a fragment of same kind at overlapping time:
 *       * confidence delta < 0.1  → keep both as competing perspectives
 *       * confidence delta ≥ 0.1  → keep higher-confidence, tag lower as "superseded"
 * Returns 0 on success. */
int bf_fragment_merge(bf_fragment_store_t *dst,
                      bf_fragment_store_t *src,
                      bf_merge_result_t *result_out);

/* ── Diff ───────────────────────────────────────────────────────── */

/* Compute the diff between two stores for a given kind (NULL = all kinds).
 * Returns array of bf_fragment_diff_t (count_out items).
 * Caller frees each diff's summary and the array itself.
 * Returns NULL on error. */
bf_fragment_diff_t *bf_fragment_diff(bf_fragment_store_t *a,
                                     bf_fragment_store_t *b,
                                     const char *kind,
                                     int *count_out);

void bf_fragment_diff_free(bf_fragment_diff_t *diffs, int count);

/* ── Statistics ─────────────────────────────────────────────────── */

typedef struct {
    int total_fragments;
    int unique_kinds;
    int unique_perspectives;
    float mean_confidence;
    int64_t earliest_ms;
    int64_t latest_ms;
} bf_fragment_stats_t;

int bf_fragment_stats(bf_fragment_store_t *store, bf_fragment_stats_t *out);

#ifdef __cplusplus
}
#endif
