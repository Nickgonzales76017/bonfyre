/*
 * BonfyreGraph — Upgrade VII: Vectorized Columnar Engine
 *
 * SIMD-friendly columnar layout for binding data with vectorized
 * scan, aggregation, and multi-column sort operations.
 *
 * Organises _tensor_cells data into packed column arrays for
 * cache-efficient batch processing. Supports predicate pushdown,
 * projection, and columnar aggregation.
 */
#ifndef COLUMNAR_H
#define COLUMNAR_H

#include <sqlite3.h>
#include <stdint.h>

/* Column data types */
#define COL_TYPE_TEXT    0
#define COL_TYPE_INT     1
#define COL_TYPE_DOUBLE  2
#define COL_TYPE_NULL    3

/* Aggregation operations */
#define COL_AGG_COUNT  0
#define COL_AGG_SUM    1
#define COL_AGG_AVG    2
#define COL_AGG_MIN    3
#define COL_AGG_MAX    4

/* Maximum columns and rows per materialized batch */
#define COL_MAX_COLS   128
#define COL_MAX_ROWS   65536

/* A single materialised column vector */
typedef struct {
    int       col_type;
    int       position;             /* binding position this came from */
    int       nrows;
    /* data arrays — exactly one is non-NULL based on col_type */
    int64_t  *ints;
    double   *doubles;
    char    **texts;                /* array of owned strings */
    uint8_t  *nulls;               /* 1 = NULL for that row */
} ColVector;

/* A materialised columnar batch */
typedef struct {
    ColVector  cols[COL_MAX_COLS];
    int        ncols;
    int        nrows;
    int       *target_ids;          /* row → target_id mapping */
    char      *family_id;           /* family this batch came from */
} ColBatch;

/* Bootstrap (no persistent tables; operates over _tensor_cells). */
int col_bootstrap(sqlite3 *db);

/* Materialise a columnar batch for a family.
   Positions array (positions, npos) selects which columns to load.
   Pass positions=NULL, npos=0 for all positions.
   Caller must col_batch_free() the result. */
int col_materialize(sqlite3 *db, const char *family_id,
                    const int *positions, int npos,
                    ColBatch *out);

/* Vectorized scan: apply a predicate on a column and return matching row indices.
   pred_op: "=","!=","<",">","<=",">=","like","between"
   Returns count of matching rows, fills match_indices (must have nrows capacity). */
int col_scan(const ColBatch *batch, int col_idx,
             const char *pred_op, const char *value,
             const char *value2,  /* for 'between' only, else NULL */
             int *match_indices);

/* Vectorized aggregation over a column, optionally filtered by match_indices.
   match_indices=NULL means aggregate all rows.
   Result returned in out_result (double for numeric, count for text). */
int col_aggregate(const ColBatch *batch, int col_idx, int agg_op,
                  const int *match_indices, int match_count,
                  double *out_result);

/* Multi-column sort in-place.
   sort_cols[i] = column index, ascending[i] = 1 for ASC, 0 for DESC.
   Modifies batch row order. */
int col_sort(ColBatch *batch, const int *sort_cols,
             const int *ascending, int nsort);

/* Project a subset of columns, producing a new batch.
   Caller must col_batch_free() the result. */
int col_project(const ColBatch *src, const int *col_indices, int ncols,
                ColBatch *out);

/* Free all memory in a ColBatch. */
void col_batch_free(ColBatch *batch);

/* Statistics JSON for a materialised batch.
   Caller frees out_json. */
int col_stats(const ColBatch *batch, char **out_json);

#endif /* COLUMNAR_H */
