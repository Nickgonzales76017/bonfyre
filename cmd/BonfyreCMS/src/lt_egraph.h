/*
 * BonfyreGraph — Flagship III: LT-EGraph Unified Framework
 *
 * Unified multi-representation store:
 *   - Equivalence classes (e-classes) per entity
 *   - Multiple representation types per class (row, curve, oplog, tensor)
 *   - Cost-based extraction (cheapest materialized repr per query)
 *   - Lambda Tensors compaction (factor shared structure across families)
 */
#ifndef LT_EGRAPH_H
#define LT_EGRAPH_H

#include <sqlite3.h>

/* Representation types */
#define REPR_ROW    "row"
#define REPR_OPLOG  "oplog"
#define REPR_TENSOR "tensor"
#define REPR_CURVE  "curve"
#define REPR_PITCH  "pitch"

/* Bootstrap _equivalences table. */
int egraph_bootstrap(sqlite3 *db);

/* Insert or update a representation in the e-graph.
   class_id groups equivalent reprs. Returns 0 on success. */
int egraph_insert(sqlite3 *db, int class_id, const char *repr_type,
                  const char *repr_hash, const char *target_type,
                  int target_id, double cost, int materialized,
                  const char *data);

/* Extract the best (lowest cost, materialized) representation for an entity.
   If preferred_type is non-NULL, tries that type first.
   Writes repr data to *out_data (caller frees). Returns 0 if found. */
int egraph_extract(sqlite3 *db, const char *target_type, int target_id,
                   const char *preferred_type, char **out_data);

/* Get e-class ID for an entity. Returns class_id, or -1 if not found. */
int egraph_class_id(sqlite3 *db, const char *target_type, int target_id);

/* List all representations for an entity as JSON array.
   Writes to *out_json (caller frees). Returns repr count. */
int egraph_list_reprs(sqlite3 *db, const char *target_type, int target_id,
                      char **out_json);

/* Mark a representation as stale (not materialized).
   Used when a new op arrives. */
int egraph_invalidate(sqlite3 *db, const char *target_type, int target_id);

/* Lambda Tensors compaction: discover structural families,
   factor generators + bindings, store tensor representations.
   Returns number of families compacted. */
int lt_compact(sqlite3 *db, const char *content_type);

/* Get compaction statistics for a content type. */
int lt_compact_stats(sqlite3 *db, const char *content_type,
                     int *family_count, int *member_count,
                     int *repr_count);

/* Allocate a new e-class ID. */
int egraph_new_class(sqlite3 *db);

#endif /* LT_EGRAPH_H */
