/*
 * BonfyreGraph — Upgrade II: Generator Synthesis Engine
 *
 * Heuristic + anti-unification based generator proposal.
 * Multi-pass pipeline:
 *   1. Structural alignment  — find common key sets
 *   2. Type inference         — constrain each position's type
 *   3. Value pattern detection — enum sets, format patterns, ranges
 *   4. Generator ranking      — score by compression ratio × coverage
 *   5. Anti-unification       — most specific generalization of two JSON objects
 */
#ifndef SYNTHESIS_H
#define SYNTHESIS_H

#include <sqlite3.h>

/* Bootstrap _synthesis_candidates table. */
int synthesis_bootstrap(sqlite3 *db);

/* Synthesize generator candidates for a content type.
   Analyzes all entries, proposes generators ranked by quality.
   Returns number of candidates proposed. */
int synthesis_propose(sqlite3 *db, const char *content_type);

/* Anti-unify two JSON objects: compute the least general generalization.
   Returns the generator (structural template) covering both.
   out_gen: newly allocated generator JSON. Caller frees.
   out_bindings_a, out_bindings_b: bindings for a and b. Caller frees. */
int synthesis_anti_unify(const char *json_a, const char *json_b,
                         char **out_gen, char **out_bindings_a,
                         char **out_bindings_b);

/* Score a candidate generator: how many entries it covers and compression ratio.
   Returns score (0.0-1.0). Higher is better. */
double synthesis_score(sqlite3 *db, const char *content_type,
                       const char *generator);

/* Get top-N generator candidates as JSON array.
   Returns count written. Caller frees out_json. */
int synthesis_candidates(sqlite3 *db, const char *content_type,
                         int top_n, char **out_json);

/* Refine a generator using pattern analysis on its bindings.
   Detects enum constraints, format patterns, numeric ranges.
   Returns a refined generator with type annotations.
   out_refined: newly allocated JSON. Caller frees. */
int synthesis_refine(sqlite3 *db, int family_id, char **out_refined);

#endif /* SYNTHESIS_H */
