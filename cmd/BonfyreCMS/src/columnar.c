/*
 * BonfyreGraph — Upgrade VII: Vectorized Columnar Engine
 *
 * Batch columnar materialisation from _tensor_cells with
 * vectorized scan, aggregation, and sort.
 */
#include "columnar.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <float.h>

extern uint64_t fnv1a64(const void *data, size_t len);
extern int      db_exec(sqlite3 *db, const char *sql);

/* ------------------------------------------------------------------ */
/*  Bootstrap — no persistent tables needed                           */
/* ------------------------------------------------------------------ */
int col_bootstrap(sqlite3 *db)
{
    (void)db;
    return 0; /* columnar engine operates over existing _tensor_cells */
}

/* ------------------------------------------------------------------ */
/*  Internal: detect column type from a sample of values              */
/* ------------------------------------------------------------------ */
static int detect_type(const char *val)
{
    if (!val || *val == '\0') return COL_TYPE_NULL;

    /* try integer */
    const char *p = val;
    if (*p == '-' || *p == '+') p++;
    if (*p >= '0' && *p <= '9') {
        while (*p >= '0' && *p <= '9') p++;
        if (*p == '\0') return COL_TYPE_INT;
        /* try double */
        if (*p == '.') {
            p++;
            while (*p >= '0' && *p <= '9') p++;
            if (*p == '\0' || *p == 'e' || *p == 'E') return COL_TYPE_DOUBLE;
        }
        if (*p == 'e' || *p == 'E') {
            p++;
            if (*p == '+' || *p == '-') p++;
            while (*p >= '0' && *p <= '9') p++;
            if (*p == '\0') return COL_TYPE_DOUBLE;
        }
    }
    return COL_TYPE_TEXT;
}

/* ------------------------------------------------------------------ */
/*  col_materialize                                                    */
/* ------------------------------------------------------------------ */
int col_materialize(sqlite3 *db, const char *family_id,
                    const int *positions, int npos,
                    ColBatch *out)
{
    if (!db || !family_id || !out) return -1;
    memset(out, 0, sizeof(ColBatch));
    out->family_id = strdup(family_id);

    int fid_int = atoi(family_id); /* _tensor_cells.family_id is INTEGER */

    /* Phase 1: determine which positions to load */
    int all_positions[COL_MAX_COLS];
    int actual_npos = 0;

    if (!positions || npos <= 0) {
        /* discover positions */
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT DISTINCT position FROM _tensor_cells "
            "WHERE family_id = ?1 ORDER BY position LIMIT ?2";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
        sqlite3_bind_int(st, 1, fid_int);
        sqlite3_bind_int(st, 2, COL_MAX_COLS);
        while (sqlite3_step(st) == SQLITE_ROW && actual_npos < COL_MAX_COLS) {
            all_positions[actual_npos++] = sqlite3_column_int(st, 0);
        }
        sqlite3_finalize(st);
    } else {
        actual_npos = npos < COL_MAX_COLS ? npos : COL_MAX_COLS;
        memcpy(all_positions, positions, actual_npos * sizeof(int));
    }

    if (actual_npos == 0) return 0;

    /* Phase 2: discover distinct target_ids and allocate row mapping */
    int *tids = malloc(COL_MAX_ROWS * sizeof(int));
    if (!tids) return -1;
    int nrows = 0;
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT DISTINCT target_id FROM _tensor_cells "
            "WHERE family_id = ?1 ORDER BY target_id LIMIT ?2";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
            free(tids);
            return -1;
        }
        sqlite3_bind_int(st, 1, fid_int);
        sqlite3_bind_int(st, 2, COL_MAX_ROWS);
        while (sqlite3_step(st) == SQLITE_ROW && nrows < COL_MAX_ROWS) {
            tids[nrows++] = sqlite3_column_int(st, 0);
        }
        sqlite3_finalize(st);
    }

    if (nrows == 0) { free(tids); return 0; }

    out->target_ids = tids;
    out->nrows = nrows;
    out->ncols = actual_npos;

    /* Build target_id → row index lookup.
       Use a simple linear scan since nrows ≤ 65536. */

    /* Phase 3: for each position, load all values into a column vector */
    for (int c = 0; c < actual_npos; c++) {
        ColVector *cv = &out->cols[c];
        cv->position = all_positions[c];
        cv->nrows = nrows;
        cv->nulls = calloc(nrows, sizeof(uint8_t));
        /* initially all null */
        if (cv->nulls) memset(cv->nulls, 1, nrows);

        /* First pass: load raw text values to determine type */
        char **raw = calloc(nrows, sizeof(char *));
        if (!raw) continue;

        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT target_id, COALESCE(val_text, CAST(val_num AS TEXT)) "
            "FROM _tensor_cells "
            "WHERE family_id = ?1 AND position = ?2 ORDER BY target_id";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
            free(raw);
            continue;
        }
        sqlite3_bind_int(st, 1, fid_int);
        sqlite3_bind_int(st, 2, all_positions[c]);

        while (sqlite3_step(st) == SQLITE_ROW) {
            int tid = sqlite3_column_int(st, 0);
            const char *val = (const char *)sqlite3_column_text(st, 1);
            /* find row index for this target_id */
            for (int r = 0; r < nrows; r++) {
                if (tids[r] == tid) {
                    if (val) raw[r] = strdup(val);
                    if (cv->nulls) cv->nulls[r] = (val == NULL) ? 1 : 0;
                    break;
                }
            }
        }
        sqlite3_finalize(st);

        /* detect dominant type */
        int counts[4] = {0, 0, 0, 0};
        for (int r = 0; r < nrows; r++) {
            if (cv->nulls && cv->nulls[r]) { counts[COL_TYPE_NULL]++; continue; }
            counts[detect_type(raw[r])]++;
        }
        /* pick majority non-null type */
        int best_type = COL_TYPE_TEXT;
        int best_count = 0;
        for (int t = 0; t < 3; t++) {
            if (counts[t] > best_count) { best_count = counts[t]; best_type = t; }
        }
        cv->col_type = best_type;

        /* convert to typed arrays */
        switch (best_type) {
        case COL_TYPE_INT:
            cv->ints = calloc(nrows, sizeof(int64_t));
            for (int r = 0; r < nrows; r++) {
                if (!cv->nulls[r] && raw[r])
                    cv->ints[r] = strtoll(raw[r], NULL, 10);
            }
            break;
        case COL_TYPE_DOUBLE:
            cv->doubles = calloc(nrows, sizeof(double));
            for (int r = 0; r < nrows; r++) {
                if (!cv->nulls[r] && raw[r])
                    cv->doubles[r] = strtod(raw[r], NULL);
            }
            break;
        default: /* TEXT */
            cv->texts = calloc(nrows, sizeof(char *));
            for (int r = 0; r < nrows; r++) {
                if (!cv->nulls[r] && raw[r])
                    cv->texts[r] = strdup(raw[r]);
            }
            break;
        }

        /* free raw */
        for (int r = 0; r < nrows; r++) free(raw[r]);
        free(raw);
    }

    return 0;
}

/* ------------------------------------------------------------------ */
/*  col_scan — vectorized predicate evaluation                        */
/* ------------------------------------------------------------------ */
static int cmp_text(const char *a, const char *b)
{
    if (!a && !b) return 0;
    if (!a) return -1;
    if (!b) return 1;
    return strcmp(a, b);
}

static int wildcard_match(const char *pattern, const char *text)
{
    /* simple SQL LIKE: % = any sequence, _ = any single char */
    if (!pattern || !text) return 0;
    const char *pp = pattern, *tp = text;
    const char *star_p = NULL, *star_t = NULL;
    while (*tp) {
        if (*pp == '%') {
            star_p = pp++;
            star_t = tp;
        } else if (*pp == '_' || (*pp && *pp == *tp)) {
            pp++; tp++;
        } else if (star_p) {
            pp = star_p + 1;
            tp = ++star_t;
        } else {
            return 0;
        }
    }
    while (*pp == '%') pp++;
    return *pp == '\0';
}

int col_scan(const ColBatch *batch, int col_idx,
             const char *pred_op, const char *value,
             const char *value2,
             int *match_indices)
{
    if (!batch || col_idx < 0 || col_idx >= batch->ncols || !pred_op || !match_indices)
        return 0;

    const ColVector *cv = &batch->cols[col_idx];
    int count = 0;

    for (int r = 0; r < cv->nrows; r++) {
        if (cv->nulls && cv->nulls[r]) continue;
        int hit = 0;

        switch (cv->col_type) {
        case COL_TYPE_INT: {
            int64_t v = strtoll(value ? value : "0", NULL, 10);
            int64_t v2 = value2 ? strtoll(value2, NULL, 10) : 0;
            int64_t cell = cv->ints[r];
            if      (strcmp(pred_op, "=") == 0)       hit = (cell == v);
            else if (strcmp(pred_op, "!=") == 0)      hit = (cell != v);
            else if (strcmp(pred_op, "<") == 0)       hit = (cell < v);
            else if (strcmp(pred_op, ">") == 0)       hit = (cell > v);
            else if (strcmp(pred_op, "<=") == 0)      hit = (cell <= v);
            else if (strcmp(pred_op, ">=") == 0)      hit = (cell >= v);
            else if (strcmp(pred_op, "between") == 0) hit = (cell >= v && cell <= v2);
            break;
        }
        case COL_TYPE_DOUBLE: {
            double v = strtod(value ? value : "0", NULL);
            double v2d = value2 ? strtod(value2, NULL) : 0;
            double cell = cv->doubles[r];
            if      (strcmp(pred_op, "=") == 0)       hit = (fabs(cell - v) < 1e-12);
            else if (strcmp(pred_op, "!=") == 0)      hit = (fabs(cell - v) >= 1e-12);
            else if (strcmp(pred_op, "<") == 0)       hit = (cell < v);
            else if (strcmp(pred_op, ">") == 0)       hit = (cell > v);
            else if (strcmp(pred_op, "<=") == 0)      hit = (cell <= v);
            else if (strcmp(pred_op, ">=") == 0)      hit = (cell >= v);
            else if (strcmp(pred_op, "between") == 0) hit = (cell >= v && cell <= v2d);
            break;
        }
        default: { /* TEXT */
            const char *cell = cv->texts ? cv->texts[r] : NULL;
            if (!cell) break;
            if      (strcmp(pred_op, "=") == 0)  hit = (cmp_text(cell, value) == 0);
            else if (strcmp(pred_op, "!=") == 0) hit = (cmp_text(cell, value) != 0);
            else if (strcmp(pred_op, "<") == 0)  hit = (cmp_text(cell, value) < 0);
            else if (strcmp(pred_op, ">") == 0)  hit = (cmp_text(cell, value) > 0);
            else if (strcmp(pred_op, "<=") == 0) hit = (cmp_text(cell, value) <= 0);
            else if (strcmp(pred_op, ">=") == 0) hit = (cmp_text(cell, value) >= 0);
            else if (strcmp(pred_op, "like") == 0) hit = wildcard_match(value, cell);
            break;
        }
        }

        if (hit) match_indices[count++] = r;
    }
    return count;
}

/* ------------------------------------------------------------------ */
/*  col_aggregate — vectorized aggregation                            */
/* ------------------------------------------------------------------ */
int col_aggregate(const ColBatch *batch, int col_idx, int agg_op,
                  const int *match_indices, int match_count,
                  double *out_result)
{
    if (!batch || col_idx < 0 || col_idx >= batch->ncols || !out_result) return -1;
    const ColVector *cv = &batch->cols[col_idx];

    double result = 0.0;
    int    n = 0;
    double minv = DBL_MAX, maxv = -DBL_MAX, sum = 0.0;

    int use_filter = (match_indices != NULL && match_count > 0);
    int total = use_filter ? match_count : cv->nrows;

    for (int i = 0; i < total; i++) {
        int r = use_filter ? match_indices[i] : i;
        if (r < 0 || r >= cv->nrows) continue;
        if (cv->nulls && cv->nulls[r]) continue;

        double val = 0.0;
        switch (cv->col_type) {
        case COL_TYPE_INT:    val = (double)cv->ints[r]; break;
        case COL_TYPE_DOUBLE: val = cv->doubles[r]; break;
        default:
            /* for text columns, COUNT is the only meaningful agg */
            if (agg_op == COL_AGG_COUNT) { n++; continue; }
            continue;
        }

        n++;
        sum += val;
        if (val < minv) minv = val;
        if (val > maxv) maxv = val;
    }

    switch (agg_op) {
    case COL_AGG_COUNT: result = (double)n; break;
    case COL_AGG_SUM:   result = sum; break;
    case COL_AGG_AVG:   result = n > 0 ? sum / n : 0.0; break;
    case COL_AGG_MIN:   result = n > 0 ? minv : 0.0; break;
    case COL_AGG_MAX:   result = n > 0 ? maxv : 0.0; break;
    default: return -1;
    }

    *out_result = result;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  col_sort — multi-column sort with row permutation                 */
/* ------------------------------------------------------------------ */
typedef struct {
    const ColBatch *batch;
    const int      *sort_cols;
    const int      *ascending;
    int             nsort;
} SortCtx;

/* thread-local sort context for qsort comparison */
static _Thread_local SortCtx *g_sort_ctx = NULL;

static int sort_cmp(const void *a, const void *b)
{
    int ra = *(const int *)a;
    int rb = *(const int *)b;
    const SortCtx *ctx = g_sort_ctx;

    for (int s = 0; s < ctx->nsort; s++) {
        int ci = ctx->sort_cols[s];
        int asc = ctx->ascending[s];
        const ColVector *cv = &ctx->batch->cols[ci];

        int na = (cv->nulls && cv->nulls[ra]);
        int nb = (cv->nulls && cv->nulls[rb]);
        if (na && nb) continue;
        if (na) return asc ? 1 : -1;
        if (nb) return asc ? -1 : 1;

        int cmp = 0;
        switch (cv->col_type) {
        case COL_TYPE_INT:
            if (cv->ints[ra] < cv->ints[rb]) cmp = -1;
            else if (cv->ints[ra] > cv->ints[rb]) cmp = 1;
            break;
        case COL_TYPE_DOUBLE:
            if (cv->doubles[ra] < cv->doubles[rb]) cmp = -1;
            else if (cv->doubles[ra] > cv->doubles[rb]) cmp = 1;
            break;
        default:
            cmp = cmp_text(cv->texts ? cv->texts[ra] : NULL,
                          cv->texts ? cv->texts[rb] : NULL);
            break;
        }
        if (cmp != 0) return asc ? cmp : -cmp;
    }
    return 0;
}

int col_sort(ColBatch *batch, const int *sort_cols,
             const int *ascending, int nsort)
{
    if (!batch || !sort_cols || !ascending || nsort <= 0) return -1;
    if (batch->nrows <= 1) return 0;

    /* create permutation array */
    int *perm = malloc(batch->nrows * sizeof(int));
    if (!perm) return -1;
    for (int i = 0; i < batch->nrows; i++) perm[i] = i;

    SortCtx ctx = { batch, sort_cols, ascending, nsort };
    g_sort_ctx = &ctx;
    qsort(perm, batch->nrows, sizeof(int), sort_cmp);
    g_sort_ctx = NULL;

    /* apply permutation to all columns and target_ids */
    int nrows = batch->nrows;

    /* target_ids */
    if (batch->target_ids) {
        int *new_tids = malloc(nrows * sizeof(int));
        if (new_tids) {
            for (int i = 0; i < nrows; i++) new_tids[i] = batch->target_ids[perm[i]];
            free(batch->target_ids);
            batch->target_ids = new_tids;
        }
    }

    for (int c = 0; c < batch->ncols; c++) {
        ColVector *cv = &batch->cols[c];

        /* permute nulls */
        if (cv->nulls) {
            uint8_t *nn = malloc(nrows);
            if (nn) {
                for (int i = 0; i < nrows; i++) nn[i] = cv->nulls[perm[i]];
                free(cv->nulls);
                cv->nulls = nn;
            }
        }

        switch (cv->col_type) {
        case COL_TYPE_INT:
            if (cv->ints) {
                int64_t *ni = malloc(nrows * sizeof(int64_t));
                if (ni) {
                    for (int i = 0; i < nrows; i++) ni[i] = cv->ints[perm[i]];
                    free(cv->ints);
                    cv->ints = ni;
                }
            }
            break;
        case COL_TYPE_DOUBLE:
            if (cv->doubles) {
                double *nd = malloc(nrows * sizeof(double));
                if (nd) {
                    for (int i = 0; i < nrows; i++) nd[i] = cv->doubles[perm[i]];
                    free(cv->doubles);
                    cv->doubles = nd;
                }
            }
            break;
        default:
            if (cv->texts) {
                char **nt = malloc(nrows * sizeof(char *));
                if (nt) {
                    for (int i = 0; i < nrows; i++) nt[i] = cv->texts[perm[i]];
                    free(cv->texts);
                    cv->texts = nt;
                }
            }
            break;
        }
    }

    free(perm);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  col_project — column subset projection                            */
/* ------------------------------------------------------------------ */
int col_project(const ColBatch *src, const int *col_indices, int ncols,
                ColBatch *out)
{
    if (!src || !col_indices || !out || ncols <= 0) return -1;
    memset(out, 0, sizeof(ColBatch));
    out->nrows = src->nrows;
    out->ncols = ncols;
    out->family_id = src->family_id ? strdup(src->family_id) : NULL;
    out->target_ids = malloc(src->nrows * sizeof(int));
    if (out->target_ids && src->target_ids)
        memcpy(out->target_ids, src->target_ids, src->nrows * sizeof(int));

    for (int i = 0; i < ncols && i < COL_MAX_COLS; i++) {
        int ci = col_indices[i];
        if (ci < 0 || ci >= src->ncols) continue;
        const ColVector *sv = &src->cols[ci];
        ColVector *dv = &out->cols[i];
        dv->col_type = sv->col_type;
        dv->position = sv->position;
        dv->nrows = sv->nrows;

        dv->nulls = malloc(sv->nrows);
        if (dv->nulls && sv->nulls)
            memcpy(dv->nulls, sv->nulls, sv->nrows);

        switch (sv->col_type) {
        case COL_TYPE_INT:
            dv->ints = malloc(sv->nrows * sizeof(int64_t));
            if (dv->ints && sv->ints)
                memcpy(dv->ints, sv->ints, sv->nrows * sizeof(int64_t));
            break;
        case COL_TYPE_DOUBLE:
            dv->doubles = malloc(sv->nrows * sizeof(double));
            if (dv->doubles && sv->doubles)
                memcpy(dv->doubles, sv->doubles, sv->nrows * sizeof(double));
            break;
        default:
            dv->texts = calloc(sv->nrows, sizeof(char *));
            if (dv->texts && sv->texts) {
                for (int r = 0; r < sv->nrows; r++)
                    dv->texts[r] = sv->texts[r] ? strdup(sv->texts[r]) : NULL;
            }
            break;
        }
    }

    return 0;
}

/* ------------------------------------------------------------------ */
/*  col_batch_free                                                     */
/* ------------------------------------------------------------------ */
void col_batch_free(ColBatch *batch)
{
    if (!batch) return;
    free(batch->family_id);
    free(batch->target_ids);
    for (int c = 0; c < batch->ncols; c++) {
        ColVector *cv = &batch->cols[c];
        free(cv->nulls);
        free(cv->ints);
        free(cv->doubles);
        if (cv->texts) {
            for (int r = 0; r < cv->nrows; r++) free(cv->texts[r]);
            free(cv->texts);
        }
    }
    memset(batch, 0, sizeof(ColBatch));
}

/* ------------------------------------------------------------------ */
/*  col_stats — JSON summary of a materialised batch                  */
/* ------------------------------------------------------------------ */
int col_stats(const ColBatch *batch, char **out_json)
{
    if (!batch || !out_json) return -1;

    size_t cap = 2048;
    char *buf = malloc(cap);
    if (!buf) return -1;
    int off = 0;

    off += snprintf(buf + off, cap - off,
        "{\"family_id\":\"%s\",\"nrows\":%d,\"ncols\":%d,\"columns\":[",
        batch->family_id ? batch->family_id : "", batch->nrows, batch->ncols);

    for (int c = 0; c < batch->ncols; c++) {
        const ColVector *cv = &batch->cols[c];
        if (c > 0) buf[off++] = ',';

        int nulls = 0;
        for (int r = 0; r < cv->nrows; r++)
            if (cv->nulls && cv->nulls[r]) nulls++;

        const char *type_str = "text";
        if (cv->col_type == COL_TYPE_INT) type_str = "int";
        else if (cv->col_type == COL_TYPE_DOUBLE) type_str = "double";
        else if (cv->col_type == COL_TYPE_NULL) type_str = "null";

        size_t need = 256;
        while (off + need >= cap) {
            cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); return -1; }
            buf = nb;
        }

        off += snprintf(buf + off, cap - off,
            "{\"position\":%d,\"type\":\"%s\",\"nrows\":%d,\"nulls\":%d}",
            cv->position, type_str, cv->nrows, nulls);
    }

    while (off + 64 >= (int)cap) {
        cap *= 2;
        char *nb = realloc(buf, cap);
        if (!nb) { free(buf); return -1; }
        buf = nb;
    }

    /* memory estimate */
    size_t mem = 0;
    for (int c = 0; c < batch->ncols; c++) {
        const ColVector *cv = &batch->cols[c];
        mem += cv->nrows; /* nulls */
        if (cv->col_type == COL_TYPE_INT)    mem += cv->nrows * sizeof(int64_t);
        if (cv->col_type == COL_TYPE_DOUBLE) mem += cv->nrows * sizeof(double);
        if (cv->col_type == COL_TYPE_TEXT && cv->texts) {
            for (int r = 0; r < cv->nrows; r++)
                if (cv->texts[r]) mem += strlen(cv->texts[r]) + 1;
        }
    }

    off += snprintf(buf + off, cap - off,
        "],\"memory_bytes\":%zu}", mem);

    *out_json = buf;
    return 0;
}
