/*
 * BonfyreGraph — Upgrade VIII: Hybrid Reduction Strategies
 *
 * Cost-model based strategy selection for tensor_reduce(),
 * choosing between FULL, PARTIAL, and SYMBOLIC reduction modes
 * based on family characteristics.
 *
 * The cost model considers:
 *   - arity (lower → FULL is cheap)
 *   - member_count (higher → PARTIAL is better for queries)
 *   - selectivity (high → SYMBOLIC for targeted reads)
 *   - eigenvalue profile (low-rank → FULL, high-rank → PARTIAL)
 *   - available memory budget
 *
 * Also provides memoized reduction results to avoid redundant work.
 */
#ifndef HYBRID_REDUCE_H
#define HYBRID_REDUCE_H

#include <sqlite3.h>

/* Strategy codes — match tensor_ops.h reduce modes */
#define STRATEGY_FULL     0
#define STRATEGY_PARTIAL  1
#define STRATEGY_SYMBOLIC 2
#define STRATEGY_AUTO     3  /* let cost model decide */

/* Cost model feature vector */
typedef struct {
    int    arity;
    int    member_count;
    double mean_eigenvalue;
    double max_eigenvalue;
    double selectivity;     /* 0.0–1.0: fraction of rows a typical query touches */
    int    target_positions; /* how many positions the query needs */
    int    total_positions;
} ReduceFeatures;

/* Cost model output */
typedef struct {
    int    recommended_strategy;
    double estimated_cost_full;
    double estimated_cost_partial;
    double estimated_cost_symbolic;
    double confidence;
} ReduceCost;

/* Bootstrap _reduce_memo and _reduce_profiles tables. */
int hybrid_bootstrap(sqlite3 *db);

/* Compute features from a family for cost estimation. */
int hybrid_features(sqlite3 *db, const char *family_id, ReduceFeatures *out);

/* Run cost model. Returns recommended strategy. */
int hybrid_estimate(const ReduceFeatures *features, ReduceCost *out);

/* Auto-reduce: pick best strategy and execute.
   Returns reconstructed JSON. Caller frees out_json. */
int hybrid_reduce(sqlite3 *db, const char *family_id, int target_id,
                  const int *positions, int npos,
                  char **out_json);

/* Store a reduction result in the memo cache. */
int hybrid_memo_store(sqlite3 *db, const char *family_id, int target_id,
                      int strategy, const char *result);

/* Look up a cached reduction. Returns 0 if found, -1 if miss.
   Caller frees out_result. */
int hybrid_memo_lookup(sqlite3 *db, const char *family_id, int target_id,
                       char **out_result);

/* Invalidate memo entries for a family (after mutations). */
int hybrid_memo_invalidate(sqlite3 *db, const char *family_id);

/* Profile a family: run all three strategies, measure timings,
   store results. Returns JSON report. Caller frees out_json. */
int hybrid_profile(sqlite3 *db, const char *family_id, int target_id,
                   char **out_json);

/* Statistics JSON for memo cache. Caller frees out_json. */
int hybrid_stats(sqlite3 *db, char **out_json);

#endif /* HYBRID_REDUCE_H */
