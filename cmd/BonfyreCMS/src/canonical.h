/*
 * BonfyreGraph — Flagship II: Canonical Functional Normalization
 *
 * Three-level normalization pipeline:
 *   L1: Syntactic (sorted keys, trimmed whitespace, canonical JSON)
 *   L2: Structural (shape hash — ignores values, detects families)
 *   L3: Semantic (schema-aware field alias mapping)
 */
#ifndef CANONICAL_H
#define CANONICAL_H

#include <sqlite3.h>

/* L1: Canonical JSON — sorted keys, normalized whitespace.
   Returns newly allocated canonical JSON string. Caller frees. */
char *canonical_l1(const char *json);

/* L1 hash: FNV-1a of the L1 canonical form.
   Writes 16-char hex hash to hash_out (must be >= 17 bytes). */
void canonical_l1_hash(const char *json, char *hash_out);

/* L2: Structural signature — shape hash ignoring values.
   Two objects with the same L2 hash share a generator (Lambda Tensors).
   Writes 16-char hex hash to hash_out (must be >= 17 bytes). */
void structural_hash(const char *json, char *hash_out);

/* L2: Extract structural signature as a readable string.
   Returns newly allocated string. Caller frees. */
char *structural_signature(const char *json);

/* Family discovery: group entries by structural hash.
   Returns count of distinct families found.
   Writes results to the _families table. */
int family_discover(sqlite3 *db, const char *content_type);

/* Incremental family maintenance for a single entry.
   Recomputes the entry's family membership from current row state. */
int family_refresh_entry(sqlite3 *db, const char *content_type, int target_id);
int family_rebind_entry(sqlite3 *db, const char *content_type, int target_id);
int family_count_orphans(sqlite3 *db, const char *content_type);

/* Remove a single entry from family tracking. */
int family_remove_entry(sqlite3 *db, const char *content_type, int target_id);

/* Bootstrap _families table. */
int families_bootstrap(sqlite3 *db);

/* Get the family_id (structural hash) for a given JSON object.
   Writes 16-char hex to hash_out. */
void family_id(const char *json, char *hash_out);

/* Extract bindings: given a JSON object, return just the values
   in canonical field order. Returns newly allocated JSON array string.
   Caller frees. */
char *extract_bindings(const char *json);

/* Extract generator: given a JSON object, return the structural
   template (keys + types, no values). Returns newly allocated JSON.
   Caller frees. */
char *extract_generator(const char *json);

#endif /* CANONICAL_H */
