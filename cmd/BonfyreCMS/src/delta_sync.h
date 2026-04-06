/*
 * BonfyreGraph — Upgrade IV: Differential Delta Sync
 *
 * Compact delta computation + shipping for multi-replica sync.
 * Three-phase protocol:
 *   1. Divergence detection — find the fork point between two op logs
 *   2. Delta extraction — collect only ops since divergence
 *   3. Three-way merge — base + local + remote with conflict detection
 *
 * Operates at the op log level (Flagship I) and tensor level (Flagship IV).
 * Delta format includes Merkle-linked verification hashes.
 */
#ifndef DELTA_SYNC_H
#define DELTA_SYNC_H

#include <sqlite3.h>

/* Delta entry for sync */
typedef struct {
    long long op_id;
    char      op_type[16];
    char      target[256];
    long long target_id;
    char      ns[64];
    char      payload[8192];
    char      hash[17];
    char      prev_hash[17];
    char      ts[64];
} DeltaOp;

/* Bootstrap _sync_state table. */
int delta_bootstrap(sqlite3 *db);

/* Compute divergence point between local and remote databases.
   Returns the hash of the last common op, or empty string if fully diverged.
   common_hash must be >= 17 bytes. */
int delta_find_fork(sqlite3 *local_db, sqlite3 *remote_db,
                    const char *ns, char *common_hash);

/* Extract delta: all ops since a given hash.
   Returns array of DeltaOps (caller must free).
   *count receives the number of ops. */
DeltaOp *delta_extract(sqlite3 *db, const char *ns,
                       const char *since_hash, int *count);

/* Export delta as compact JSON.
   Caller frees out_json. Returns op count. */
int delta_export_json(sqlite3 *db, const char *ns,
                      const char *since_hash, char **out_json);

/* Import delta from compact JSON into local database.
   Performs three-way merge. Returns count of ops applied.
   *conflicts receives count of conflicting ops (resolved by last-write-wins). */
int delta_import_json(sqlite3 *db, const char *delta_json, int *conflicts);

/* Compute delta at the tensor level: which bindings changed.
   Returns a compact diff of changed cells only.
   Caller frees out_json. */
int delta_tensor_diff(sqlite3 *db, const char *content_type,
                      const char *since_ts, char **out_json);

/* Apply a tensor-level delta to bring local cells up to date.
   Returns count of cells updated. */
int delta_tensor_apply(sqlite3 *db, const char *diff_json);

/* Record sync checkpoint: store the latest sync hash for a remote.
   remote_id is an opaque label for the peer. */
int delta_checkpoint(sqlite3 *db, const char *remote_id,
                     const char *ns, const char *hash);

/* Get last sync hash for a remote peer. Returns 0 if found. */
int delta_last_sync(sqlite3 *db, const char *remote_id,
                    const char *ns, char *hash_out);

#endif /* DELTA_SYNC_H */
