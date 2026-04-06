/*
 * BonfyreGraph — Tensor Operations: Compressed-Domain Lambda Calculus
 *
 * Operates on Lambda Tensor representations without decompression.
 * The generator is a lambda expression; operations are reduction strategies.
 *
 * Four primitives:
 *   tensor_abstract  — λ-abstraction: discover generator, extract bindings, compute eigenvalues
 *   tensor_reduce    — β/partial/symbolic reduction chosen by e-graph cost model
 *   tensor_discover  — re-analyze family, find novel sub-functions from eigenvalue patterns
 *   tensor_eigen     — compute/update spectral profile per family
 *
 * Compressed-domain operations (query, update, aggregate, project, insert, delete)
 * are implemented as specific reduction strategies over the tensor form.
 */
#ifndef TENSOR_OPS_H
#define TENSOR_OPS_H

#include <sqlite3.h>

/* ================================================================
 * Reduction strategies
 * ================================================================ */
#define REDUCE_FULL      0   /* Full β-reduction — reconstruct original */
#define REDUCE_PARTIAL   1   /* Partial application — project subset of fields */
#define REDUCE_SYMBOLIC  2   /* Symbolic rewrite — modify binding in-place */
#define REDUCE_AUTO      3   /* Cost-model driven — hybrid_reduce selects strategy */

/* ================================================================
 * Eigenvalue / spectral profile
 * ================================================================ */
typedef struct {
    int    family_id;
    int    arity;            /* number of binding positions */
    int    member_count;     /* family size */
    double *eigenvalues;     /* eigenvalue per position (descending) */
    double *variance;        /* variance per position */
    double total_variance;   /* sum of all variances */
    double structural_ratio; /* top-k / total — compression quality metric */
    int    optimal_rank;     /* positions with meaningful variance */
} TensorEigen;

/* ================================================================
 * Bootstrap tables for tensor operations
 * ================================================================ */
int tensor_ops_bootstrap(sqlite3 *db);

/* ================================================================
 * Primitive 1: tensor_abstract
 *
 * Given a content type, discover families by structural hash,
 * store λ-expressions (generators) and bindings per member,
 * compute and store eigenvalue profiles.
 * Returns number of families abstracted.
 * ================================================================ */
int tensor_abstract(sqlite3 *db, const char *content_type);

/* ================================================================
 * Primitive 2: tensor_reduce
 *
 * Apply a reduction strategy to an entity's tensor representation.
 *   REDUCE_FULL:     β-reduce → original JSON (decompression)
 *   REDUCE_PARTIAL:  Apply with field subset → projected JSON
 *   REDUCE_SYMBOLIC: Rewrite a binding in-place → update without decompression
 *
 * For REDUCE_PARTIAL: fields is comma-separated field names
 * For REDUCE_SYMBOLIC: fields is "field_name", value is new value string
 * out_json receives result (caller frees). Returns 0 on success.
 * ================================================================ */
int tensor_reduce(sqlite3 *db, const char *target_type, int target_id,
                  int strategy, const char *fields, const char *value,
                  char **out_json);

/* ================================================================
 * Primitive 3: tensor_discover
 *
 * Re-analyze a family. Detect:
 *   - near-zero eigenvalue positions → absorb into generator (reduce arity)
 *   - correlated binding positions → factor inner lambda
 *   - divergent members → split family
 * Returns count of structural refinements made.
 * ================================================================ */
int tensor_discover(sqlite3 *db, const char *content_type, int family_id);

/* ================================================================
 * Primitive 4: tensor_eigen
 *
 * Compute spectral profile for a family.
 * Stores results in _tensor_eigen table.
 * Fills eigen_out if non-NULL (caller must free eigenvalues/variance arrays).
 * Returns 0 on success.
 * ================================================================ */
int tensor_eigen(sqlite3 *db, int family_id, TensorEigen *eigen_out);

/* ================================================================
 * Compressed-domain operations (built on reduce)
 * ================================================================ */

/* Query: filter family members on a field condition.
   op is one of "=", "!=", ">", "<", ">=", "<=".
   Returns matching member IDs as JSON array. Caller frees. */
int tensor_query(sqlite3 *db, const char *content_type, const char *field,
                 const char *op, const char *value, char **out_json);

/* Update: modify a single field in compressed form (symbolic reduction).
   Does NOT decompress. Logs to op log. Returns 0 on success. */
int tensor_update(sqlite3 *db, const char *content_type, int target_id,
                  const char *field, const char *value);

/* Aggregate: SUM/AVG/MIN/MAX/COUNT over a field across a family.
   agg_fn is one of "sum", "avg", "min", "max", "count".
   Returns result as JSON. Caller frees. */
int tensor_aggregate(sqlite3 *db, const char *content_type, const char *field,
                     const char *agg_fn, char **out_json);

/* Project: select specific fields from all family members.
   fields is comma-separated. Returns JSON array. Caller frees. */
int tensor_project(sqlite3 *db, const char *content_type,
                   const char *fields, char **out_json);

/* Eigen stats: get eigenvalue profile for a content type as JSON.
   Returns JSON with per-family eigenvalue data. Caller frees. */
int tensor_eigen_stats(sqlite3 *db, const char *content_type, char **out_json);

/* Free a TensorEigen struct's heap arrays. */
void tensor_eigen_free(TensorEigen *e);

#endif /* TENSOR_OPS_H */
