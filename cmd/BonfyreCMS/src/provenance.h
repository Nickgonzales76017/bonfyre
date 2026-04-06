/*
 * BonfyreGraph — Upgrade IX: Provenance & Merkle Proofs
 *
 * Merkle tree over the operation log for tamper-evident audit.
 * Compact proofs verify entry state without replaying full history.
 * Compaction manifests record Merkle root + generator hash +
 * eigenvalue snapshot for verifiable compression.
 */
#ifndef PROVENANCE_H
#define PROVENANCE_H

#include <sqlite3.h>
#include <stdint.h>

/* A Merkle node */
typedef struct {
    uint64_t hash;
    int64_t  left_id;
    int64_t  right_id;
    int      depth;
} MerkleNode;

/* Bootstrap _merkle_tree and _compaction_manifests tables. */
int prov_bootstrap(sqlite3 *db);

/* Build/rebuild the Merkle tree from the _ops table.
   Returns root hash via out_root. */
int prov_build_tree(sqlite3 *db, const char *ns, uint64_t *out_root);

/* Generate a compact inclusion proof for a specific op.
   out_proof is an array of sibling hashes, out_count = proof length.
   Caller frees out_proof. */
int prov_prove(sqlite3 *db, const char *ns, int64_t op_id,
               uint64_t **out_proof, int *out_count);

/* Verify an inclusion proof against a known root.
   Returns 1 if valid, 0 if invalid. */
int prov_verify(uint64_t leaf_hash, const uint64_t *proof, int proof_len,
                const int *directions, uint64_t expected_root);

/* Get the current Merkle root for a namespace. */
int prov_root(sqlite3 *db, const char *ns, uint64_t *out_root);

/* Create a compaction manifest: snapshot of Merkle root, generator hashes,
   and eigenvalue summaries at a point in time.
   Returns manifest_id. */
int prov_manifest(sqlite3 *db, const char *content_type, int64_t *out_manifest_id);

/* Verify a compaction manifest: check that current state is consistent
   with the recorded manifest. Returns 1 if valid, 0 if diverged. */
int prov_verify_manifest(sqlite3 *db, int64_t manifest_id);

/* Get provenance chain as JSON: sequence of Merkle roots and manifests.
   Caller frees out_json. */
int prov_chain(sqlite3 *db, const char *ns, char **out_json);

/* Provenance statistics JSON. Caller frees out_json. */
int prov_stats(sqlite3 *db, char **out_json);

#endif /* PROVENANCE_H */
