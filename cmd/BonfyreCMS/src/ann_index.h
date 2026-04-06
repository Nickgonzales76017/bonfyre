/*
 * BonfyreGraph — Upgrade III: ANN Index over Tensor Bindings
 *
 * Locality-Sensitive Hashing (LSH) for approximate nearest-neighbor
 * search in the compressed binding domain. No decompression needed.
 *
 * Hash family: random hyperplane (cosine similarity) for numeric,
 * MinHash (Jaccard similarity) for categorical positions.
 *
 * Multiple hash tables with independent projections for sub-linear lookup.
 */
#ifndef ANN_INDEX_H
#define ANN_INDEX_H

#include <sqlite3.h>

/* Configuration */
#define ANN_NUM_TABLES   8   /* Number of independent hash tables */
#define ANN_HASH_BITS   12   /* Bits per hash (4096 buckets per table) */
#define ANN_MAX_DIM    128   /* Maximum binding vector dimension */

/* Bootstrap _ann_buckets and _ann_projections tables. */
int ann_bootstrap(sqlite3 *db);

/* Build or rebuild ANN index for a content type.
   Reads all family bindings, hashes into LSH buckets.
   Returns number of vectors indexed. */
int ann_build_index(sqlite3 *db, const char *content_type);

/* Incrementally index a single member's bindings.
   Call after inserting a new tensor cell set. */
int ann_index_member(sqlite3 *db, int family_id, int target_id);

/* k-NN query: find the k nearest neighbors to a query vector.
   query_json is a JSON object (used as the binding vector).
   Returns target_ids and distances as JSON array. Caller frees. */
int ann_knn_query(sqlite3 *db, const char *content_type,
                  const char *query_json, int k, char **out_json);

/* Quantized k-NN query: same candidate lookup, but reranks using a
   TurboQuant-inspired rotated 4-bit sidecar with residual sign sketch. */
int ann_knn_query_quantized(sqlite3 *db, const char *content_type,
                            const char *query_json, int k, char **out_json);

/* k-NN by entry ID: uses stored tensor cells directly as query vector.
   Bypasses JSON → bindings extraction. Ideal for self-recall benchmarks. */
int ann_knn_by_entry(sqlite3 *db, const char *content_type,
                     int query_target_id, int k, char **out_json);

/* Quantized k-NN by entry ID. Uses exact query vector, but reranks against
   the stored quantized sidecar instead of the raw tensor cells. */
int ann_knn_by_entry_quantized(sqlite3 *db, const char *content_type,
                               int query_target_id, int k, char **out_json);

/* Similarity query: find all members within distance threshold.
   Returns matching target_ids as JSON array. Caller frees. */
int ann_range_query(sqlite3 *db, const char *content_type,
                    const char *query_json, double threshold,
                    char **out_json);

/* Delete a member from the ANN index. */
int ann_remove_member(sqlite3 *db, int family_id, int target_id);

/* Get index statistics as JSON. */
int ann_stats(sqlite3 *db, const char *content_type, char **out_json);

/* Focused quality benchmark for the quantized ANN sidecar. */
int ann_quant_bench(sqlite3 *db, const char *content_type,
                    int query_limit, int k, char **out_json);

#endif /* ANN_INDEX_H */
