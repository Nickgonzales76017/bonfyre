/*
 * BonfyreGraph — Upgrade VI: Hierarchical Meta-Generators
 *
 * Level-2+ abstraction: generators-of-generators.
 * Meta-family discovery groups families by structural similarity
 * of their generators, then factors out shared generator structure.
 *
 * Multi-level compaction hierarchy:
 *   Level 0: raw entries
 *   Level 1: generator + bindings (current Lambda Tensors)
 *   Level 2: meta-generator + generator-bindings
 *   Level N: N-th order meta-generator
 *
 * Each level reduces the total stored bytes further.
 */
#ifndef META_GEN_H
#define META_GEN_H

#include <sqlite3.h>

/* Bootstrap _meta_generators and _meta_families tables. */
int meta_bootstrap(sqlite3 *db);

/* Discover meta-families by grouping structurally similar generators.
   Returns number of meta-families found. */
int meta_discover(sqlite3 *db, const char *content_type);

/* Extract the meta-generator for a meta-family.
   Returns the meta-level structural template.
   Caller frees out_meta_gen. */
int meta_extract(sqlite3 *db, int meta_family_id, char **out_meta_gen);

/* Multi-level compaction: run L1 (tensor_abstract), then L2 (meta_discover).
   Returns total compression ratio improvement. */
double meta_compact(sqlite3 *db, const char *content_type);

/* Get hierarchy statistics as JSON.
   Shows level counts, byte savings, and meta-family sizes.
   Caller frees out_json. */
int meta_stats(sqlite3 *db, const char *content_type, char **out_json);

/* Decompress from meta-level: given a meta-generator and meta-bindings,
   produce the L1 generator. Then β-reduce to get original JSON.
   Caller frees out_json. Returns 0 on success. */
int meta_reduce(sqlite3 *db, int meta_family_id, int family_id,
                int target_id, char **out_json);

#endif /* META_GEN_H */
