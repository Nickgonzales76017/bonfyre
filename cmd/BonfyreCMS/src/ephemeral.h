/*
 * BonfyreGraph — Flagship I: Ephemeral Reconstruction
 *
 * Append-only operation log as ground truth.
 * State reconstructed on demand via deterministic fold.
 * Merkle-linked DAG for content-addressed history.
 */
#ifndef EPHEMERAL_H
#define EPHEMERAL_H

#include <sqlite3.h>

/* Op types */
#define OP_CREATE   "create"
#define OP_UPDATE   "update"
#define OP_DELETE   "delete"
#define OP_RELATE   "relate"
#define OP_UNRELATE "unrelate"

/* Bootstrap the _ops table. Call during DB init. */
int ops_bootstrap(sqlite3 *db);

/* Append an operation to the log. Returns 0 on success.
   hash_out (if non-NULL) receives the hex hash of the new op (must be >=17 bytes). */
int ops_append(sqlite3 *db, const char *op_type, const char *target,
               long long target_id, const char *ns, const char *payload,
               const char *actor, char *hash_out);

/* Get the HEAD hash (latest op) for a (target, target_id). Returns 0 if found. */
int ops_head(sqlite3 *db, const char *target, long long target_id, char *hash_out);

/* Reconstruct current state of an entity by replaying its op log.
   Writes reconstructed JSON to *out_json (caller must free).
   Returns 0 on success, -1 if entity not found or deleted. */
int ops_reconstruct(sqlite3 *db, const char *target, long long target_id,
                    char **out_json);

/* Get full op history for an entity as a JSON array.
   Writes to *out_json (caller must free). Returns op count. */
int ops_history(sqlite3 *db, const char *target, long long target_id,
                char **out_json);

/* Merge ops from a remote database. Inserts any ops whose hashes are
   not already present. Returns count of new ops merged. */
int ops_merge(sqlite3 *db, sqlite3 *remote_db, const char *ns);

/* Get namespace HEAD — the hash of the latest op in a namespace. */
int ops_namespace_head(sqlite3 *db, const char *ns, char *hash_out);

/* Count ops for an entity. */
int ops_count(sqlite3 *db, const char *target, long long target_id);

#endif /* EPHEMERAL_H */
