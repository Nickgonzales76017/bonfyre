/*
 * bf_lmdb.h — LMDB zero-copy artifact cache
 *
 * Split architecture:
 *   LMDB  → read-hot path (O(1) mmap'd lookups, zero deserialization)
 *   SQLite → write-behind (FTS5, joins, aggregation, complex queries)
 *
 * Content-addressed: key = SHA-256 hex, value = raw artifact bytes.
 * Lookups are pointer casts into mmap'd pages — no allocation.
 *
 * Write path: bf_lmdb_put() writes to LMDB first (fast), then
 * optionally queues a SQLite writeback for indexing.
 *
 * Read path: bf_lmdb_get() returns a direct pointer into the mmap —
 * valid until the transaction ends (or the next resize).
 */

#ifndef BF_LMDB_H
#define BF_LMDB_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Configuration ───────────────────────────────────────────── */

#define BF_LMDB_MAP_SIZE   (512ULL * 1024 * 1024)  /* 512MB default   */
#define BF_LMDB_MAX_DBS    4                         /* Named databases */

/* Database names */
#define BF_LMDB_DB_ARTIFACTS   "artifacts"    /* key=sha256, val=manifest  */
#define BF_LMDB_DB_FAMILIES    "families"     /* key=family_key, val=list  */
#define BF_LMDB_DB_BLOBS       "blobs"        /* key=sha256, val=raw data  */
#define BF_LMDB_DB_META        "meta"         /* key=string, val=string    */

/* ── Handle ──────────────────────────────────────────────────── */

typedef struct bf_lmdb bf_lmdb_t;

/* ── Read transaction (zero-copy) ────────────────────────────── */

typedef struct bf_lmdb_txn bf_lmdb_txn_t;

/* ── Cursor for iteration ────────────────────────────────────── */

typedef struct bf_lmdb_cursor bf_lmdb_cursor_t;

/* ── Lifecycle ───────────────────────────────────────────────── */

/*
 * Open or create an LMDB environment.
 *   path:     directory for LMDB files (created if needed)
 *   map_size: max mmap size (0 = default 512MB)
 *   readonly: 1 = open read-only (multiple processes can read)
 */
bf_lmdb_t *bf_lmdb_open(const char *path, size_t map_size, int readonly);

void bf_lmdb_close(bf_lmdb_t *db);

/* ── Zero-copy read ──────────────────────────────────────────── */

/*
 * Begin a read transaction.
 * Returned pointer is valid until bf_lmdb_txn_end().
 * Multiple read transactions can be active concurrently.
 */
bf_lmdb_txn_t *bf_lmdb_txn_begin(bf_lmdb_t *db);
void bf_lmdb_txn_end(bf_lmdb_txn_t *txn);

/*
 * Zero-copy get: returns pointer directly into mmap.
 *   db_name: which named DB (BF_LMDB_DB_*)
 *   key/key_len: lookup key
 *   val_out: receives pointer to mmap'd data (DO NOT FREE)
 *   val_len_out: receives data length
 *
 * Returns 0 on success, -1 if not found.
 * Pointer is valid until bf_lmdb_txn_end().
 */
int bf_lmdb_get(bf_lmdb_txn_t *txn, const char *db_name,
                 const void *key, size_t key_len,
                 const void **val_out, size_t *val_len_out);

/* ── Write ───────────────────────────────────────────────────── */

/*
 * Put a key-value pair.  Creates a write transaction, commits immediately.
 * For bulk writes, use bf_lmdb_batch_begin/put/commit.
 */
int bf_lmdb_put(bf_lmdb_t *db, const char *db_name,
                 const void *key, size_t key_len,
                 const void *val, size_t val_len);

/* Batch write interface */
typedef struct bf_lmdb_writer bf_lmdb_writer_t;

bf_lmdb_writer_t *bf_lmdb_batch_begin(bf_lmdb_t *db);

int bf_lmdb_batch_put(bf_lmdb_writer_t *w, const char *db_name,
                       const void *key, size_t key_len,
                       const void *val, size_t val_len);

int bf_lmdb_batch_commit(bf_lmdb_writer_t *w);
void bf_lmdb_batch_abort(bf_lmdb_writer_t *w);

/* ── Delete ──────────────────────────────────────────────────── */

int bf_lmdb_del(bf_lmdb_t *db, const char *db_name,
                 const void *key, size_t key_len);

/* ── Iteration ───────────────────────────────────────────────── */

bf_lmdb_cursor_t *bf_lmdb_cursor_open(bf_lmdb_txn_t *txn, const char *db_name);

/*
 * Advance cursor. Returns 0 on success, -1 at end.
 * key_out/val_out point into mmap (zero-copy).
 */
int bf_lmdb_cursor_next(bf_lmdb_cursor_t *cur,
                          const void **key_out, size_t *key_len_out,
                          const void **val_out, size_t *val_len_out);

/* Seek to key >= target */
int bf_lmdb_cursor_seek(bf_lmdb_cursor_t *cur,
                          const void *target, size_t target_len);

void bf_lmdb_cursor_close(bf_lmdb_cursor_t *cur);

/* ── Stats ───────────────────────────────────────────────────── */

typedef struct {
    size_t  map_size;
    size_t  pages_used;
    size_t  page_size;
    int     entries;        /* Total entries across all DBs */
} bf_lmdb_stat_t;

bf_lmdb_stat_t bf_lmdb_stats(bf_lmdb_t *db);

#ifdef __cplusplus
}
#endif

#endif /* BF_LMDB_H */
