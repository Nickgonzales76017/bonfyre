/*
 * BonfyreGraph — Upgrade I: Incremental λ-Abstraction
 *
 * Online/streaming eigenvalue updates via Welford's running variance.
 * Rank-1 updates when members join/leave/change — O(k) per update
 * instead of O(n·k) full recompute.
 *
 * Constant absorption: when eigenvalue ≈ 0, absorb position into
 * the generator body, rewriting the lambda to reduce arity.
 *
 * Divergence-based family split detection.
 */
#ifndef INCREMENTAL_H
#define INCREMENTAL_H

#include <sqlite3.h>

/* Bootstrap _incr_eigen table for running statistics. */
int incr_bootstrap(sqlite3 *db);

/* Incrementally update eigenvalues when a new member joins a family.
   bindings_json is the member's binding array.
   Updates running mean/variance/count per position in O(arity). */
int incr_add_member(sqlite3 *db, int family_id, const char *bindings_json);

/* Incrementally update eigenvalues when a member leaves.
   De-accumulates running statistics in O(arity). */
int incr_remove_member(sqlite3 *db, int family_id, const char *bindings_json);

/* Incrementally update eigenvalues when a member's binding changes.
   old_bindings and new_bindings are the before/after arrays. */
int incr_update_member(sqlite3 *db, int family_id,
                       const char *old_bindings, const char *new_bindings);

/* Absorb near-zero eigenvalue positions into the generator body.
   Rewrites the generator lambda to inline constants, reducing arity.
   threshold: positions with eigenvalue < threshold are absorbed.
   Returns count of positions absorbed. */
int incr_absorb_constants(sqlite3 *db, int family_id, double threshold);

/* Online compaction pass for a content type:
   1. Refresh incremental eigenvalue stats
   2. Absorb near-constant positions (eigenvalue < threshold)
   3. Detect and split divergent families (coefficient of variation > split_cv)
   Returns total structural refinements made. */
int incr_compact_online(sqlite3 *db, const char *content_type,
                        double absorb_threshold, double split_cv);

/* Get incremental eigenvalue statistics as JSON.
   Returns running mean, variance, count per position. */
int incr_eigen_stats(sqlite3 *db, int family_id, char **out_json);

#endif /* INCREMENTAL_H */
