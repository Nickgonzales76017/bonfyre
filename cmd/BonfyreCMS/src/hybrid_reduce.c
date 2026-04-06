/*
 * BonfyreGraph — Upgrade VIII: Hybrid Reduction Strategies
 *
 * Cost-model driven strategy selection with memoized results.
 */
#include "hybrid_reduce.h"
#include "bench_metrics.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <time.h>

extern uint64_t fnv1a64(const void *data, size_t len);
extern void     iso_timestamp(char *buf, size_t sz);
extern int      db_exec(sqlite3 *db, const char *sql);

/* ------------------------------------------------------------------ */
/*  tensor_reduce declaration (from tensor_ops.h)                     */
/* ------------------------------------------------------------------ */
extern int tensor_reduce(sqlite3 *db, const char *target_type, int target_id,
                         int strategy, const char *fields, const char *value,
                         char **out_json);

/* ------------------------------------------------------------------ */
/*  Bootstrap                                                          */
/* ------------------------------------------------------------------ */
int hybrid_bootstrap(sqlite3 *db)
{
    const char *ddl =
        "CREATE TABLE IF NOT EXISTS _reduce_memo ("
        "  family_id  TEXT NOT NULL,"
        "  target_id  INTEGER NOT NULL,"
        "  strategy   INTEGER NOT NULL,"
        "  result     TEXT NOT NULL,"
        "  created_at TEXT NOT NULL,"
        "  PRIMARY KEY (family_id, target_id)"
        ");"
        "CREATE TABLE IF NOT EXISTS _reduce_profiles ("
        "  family_id       TEXT NOT NULL,"
        "  strategy        INTEGER NOT NULL,"
        "  elapsed_us      INTEGER NOT NULL,"
        "  result_bytes    INTEGER NOT NULL,"
        "  profiled_at     TEXT NOT NULL,"
        "  PRIMARY KEY (family_id, strategy)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_memo_family "
        "  ON _reduce_memo(family_id);";
    return db_exec(db, ddl);
}

/* ------------------------------------------------------------------ */
/*  hybrid_features — extract cost model inputs from a family         */
/* ------------------------------------------------------------------ */
int hybrid_features(sqlite3 *db, const char *family_id, ReduceFeatures *out)
{
    if (!db || !family_id || !out) return -1;
    memset(out, 0, sizeof(ReduceFeatures));

    /* arity: count distinct positions in _tensor_cells */
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT COUNT(DISTINCT position) FROM _tensor_cells "
            "WHERE family_id = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW)
                out->arity = sqlite3_column_int(st, 0);
            sqlite3_finalize(st);
        }
    }
    out->total_positions = out->arity;
    out->target_positions = out->arity; /* default: need all */

    /* member_count */
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT COUNT(DISTINCT target_id) FROM _tensor_cells "
            "WHERE family_id = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW)
                out->member_count = sqlite3_column_int(st, 0);
            sqlite3_finalize(st);
        }
    }

    /* eigenvalue statistics from _tensor_eigen */
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT AVG(eigenvalue), MAX(eigenvalue) FROM _tensor_eigen "
            "WHERE family_id = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out->mean_eigenvalue = sqlite3_column_double(st, 0);
                out->max_eigenvalue = sqlite3_column_double(st, 1);
            }
            sqlite3_finalize(st);
        }
    }

    /* selectivity estimate: use coefficient of variation as proxy */
    out->selectivity = 0.5; /* default: moderate selectivity */
    if (out->mean_eigenvalue > 0 && out->max_eigenvalue > 0) {
        double ratio = out->mean_eigenvalue / out->max_eigenvalue;
        out->selectivity = 1.0 - ratio; /* high variance → more selective queries */
    }

    return 0;
}

/* ------------------------------------------------------------------ */
/*  hybrid_estimate — pure cost model, no DB access                   */
/* ------------------------------------------------------------------ */
int hybrid_estimate(const ReduceFeatures *f, ReduceCost *out)
{
    if (!f || !out) return -1;
    memset(out, 0, sizeof(ReduceCost));

    /*
     * Cost model (microsecond estimates):
     *
     * FULL reduce: must reconstruct entire JSON from generator + all bindings
     *   cost = arity × member_count × 0.5µs (binding lookups)
     *        + member_count × 2µs (JSON assembly)
     *
     * PARTIAL reduce: reconstruct only requested positions
     *   cost = target_positions × member_count × 0.5µs
     *        + member_count × 1µs (partial JSON)
     *
     * SYMBOLIC reduce: return generator template + eigenvalue summary
     *   cost = arity × 0.1µs (eigenvalue reads)
     *        + 5µs (template format)
     *   But: result is not materialised JSON, needs further processing
     */
    double a = (double)f->arity;
    double m = (double)f->member_count;
    double tp = (double)f->target_positions;
    double sel = f->selectivity;

    out->estimated_cost_full     = a * m * 0.5 + m * 2.0;
    out->estimated_cost_partial  = tp * m * 0.5 + m * 1.0;
    out->estimated_cost_symbolic = a * 0.1 + 5.0;

    /* Adjust for selectivity: if highly selective, SYMBOLIC saves work
       because the caller will filter most rows anyway */
    if (sel > 0.8) {
        out->estimated_cost_symbolic *= 0.5; /* symbolic wins more */
    }

    /* Decision logic */
    if (a <= 4 && m <= 100) {
        /* small family: FULL is fine */
        out->recommended_strategy = STRATEGY_FULL;
        out->confidence = 0.9;
    } else if (tp < a * 0.5) {
        /* query touches < half the positions: PARTIAL */
        out->recommended_strategy = STRATEGY_PARTIAL;
        out->confidence = 0.85;
    } else if (sel > 0.8 || (f->mean_eigenvalue < 0.01 && a > 16)) {
        /* very selective or very low-rank: SYMBOLIC */
        out->recommended_strategy = STRATEGY_SYMBOLIC;
        out->confidence = 0.75;
    } else if (m > 1000 && a > 8) {
        /* large family, moderate arity: PARTIAL */
        out->recommended_strategy = STRATEGY_PARTIAL;
        out->confidence = 0.80;
    } else {
        /* default to whichever has lowest estimated cost */
        if (out->estimated_cost_full <= out->estimated_cost_partial &&
            out->estimated_cost_full <= out->estimated_cost_symbolic) {
            out->recommended_strategy = STRATEGY_FULL;
        } else if (out->estimated_cost_partial <= out->estimated_cost_symbolic) {
            out->recommended_strategy = STRATEGY_PARTIAL;
        } else {
            out->recommended_strategy = STRATEGY_SYMBOLIC;
        }
        out->confidence = 0.65;
    }

    return 0;
}

/* ------------------------------------------------------------------ */
/*  hybrid_reduce — auto-select strategy and execute                  */
/* ------------------------------------------------------------------ */
int hybrid_reduce(sqlite3 *db, const char *family_id, int target_id,
                  const int *positions, int npos,
                  char **out_json)
{
    if (!db || !family_id || !out_json) return -1;

    /* check memo cache first */
    if (hybrid_memo_lookup(db, family_id, target_id, out_json) == 0) {
        bench_metrics_record_hybrid_memo_hit();
        return 0;
    }

    /* compute features */
    ReduceFeatures feat;
    hybrid_features(db, family_id, &feat);
    if (positions && npos > 0)
        feat.target_positions = npos;

    /* estimate best strategy */
    ReduceCost cost;
    hybrid_estimate(&feat, &cost);

    /* execute tensor_reduce with recommended strategy */
    int strategy = cost.recommended_strategy;
    bench_metrics_record_hybrid_strategy(strategy);

    /* PARTIAL/SYMBOLIC without field context degrades to FULL */
    if ((strategy == STRATEGY_PARTIAL || strategy == STRATEGY_SYMBOLIC) &&
        (!positions || npos <= 0))
        strategy = STRATEGY_FULL;

    /* Resolve content_type from family_id for tensor_reduce */
    char content_type[256] = {0};
    {
        sqlite3_stmt *ct_q = NULL;
        if (sqlite3_prepare_v2(db,
            "SELECT content_type FROM _families WHERE id = CAST(?1 AS INTEGER)",
            -1, &ct_q, NULL) == SQLITE_OK) {
            sqlite3_bind_text(ct_q, 1, family_id, -1, SQLITE_STATIC);
            if (sqlite3_step(ct_q) == SQLITE_ROW) {
                const char *ct = (const char *)sqlite3_column_text(ct_q, 0);
                if (ct) strncpy(content_type, ct, sizeof(content_type) - 1);
            }
            sqlite3_finalize(ct_q);
        }
    }
    if (content_type[0] == '\0') return -1;

    int rc = tensor_reduce(db, content_type, target_id, strategy,
                           NULL, NULL, out_json);

    /* cache result */
    if (rc == 0 && *out_json) {
        hybrid_memo_store(db, family_id, target_id, strategy, *out_json);
    }

    return rc;
}

/* ------------------------------------------------------------------ */
/*  Memo cache operations                                              */
/* ------------------------------------------------------------------ */
int hybrid_memo_store(sqlite3 *db, const char *family_id, int target_id,
                      int strategy, const char *result)
{
    if (!db || !family_id || !result) return -1;

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    sqlite3_stmt *st = NULL;
    const char *sql =
        "INSERT OR REPLACE INTO _reduce_memo "
        "(family_id, target_id, strategy, result, created_at) "
        "VALUES (?1, ?2, ?3, ?4, ?5)";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 2, target_id);
    sqlite3_bind_int(st, 3, strategy);
    sqlite3_bind_text(st, 4, result, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 5, ts, -1, SQLITE_STATIC);
    int rc = (sqlite3_step(st) == SQLITE_DONE) ? 0 : -1;
    sqlite3_finalize(st);
    return rc;
}

int hybrid_memo_lookup(sqlite3 *db, const char *family_id, int target_id,
                       char **out_result)
{
    if (!db || !family_id || !out_result) return -1;

    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT result FROM _reduce_memo "
        "WHERE family_id = ?1 AND target_id = ?2";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 2, target_id);
    int rc = -1;
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *r = (const char *)sqlite3_column_text(st, 0);
        if (r) { *out_result = strdup(r); rc = 0; }
    }
    sqlite3_finalize(st);
    return rc;
}

int hybrid_memo_invalidate(sqlite3 *db, const char *family_id)
{
    if (!db || !family_id) return -1;
    sqlite3_stmt *st = NULL;
    const char *sql = "DELETE FROM _reduce_memo WHERE family_id = ?1";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);
    int rc = (sqlite3_step(st) == SQLITE_DONE) ? 0 : -1;
    sqlite3_finalize(st);
    return rc;
}

/* ------------------------------------------------------------------ */
/*  hybrid_profile — benchmark all strategies for a target            */
/* ------------------------------------------------------------------ */
static int64_t now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

int hybrid_profile(sqlite3 *db, const char *family_id, int target_id,
                   char **out_json)
{
    if (!db || !family_id || !out_json) return -1;

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    /* Resolve content_type from family_id for tensor_reduce */
    char content_type[256] = {0};
    {
        sqlite3_stmt *ct_q = NULL;
        if (sqlite3_prepare_v2(db,
            "SELECT content_type FROM _families WHERE id = CAST(?1 AS INTEGER)",
            -1, &ct_q, NULL) == SQLITE_OK) {
            sqlite3_bind_text(ct_q, 1, family_id, -1, SQLITE_STATIC);
            if (sqlite3_step(ct_q) == SQLITE_ROW) {
                const char *ct = (const char *)sqlite3_column_text(ct_q, 0);
                if (ct) strncpy(content_type, ct, sizeof(content_type) - 1);
            }
            sqlite3_finalize(ct_q);
        }
    }
    if (content_type[0] == '\0') return -1;

    int strategies[3] = { STRATEGY_FULL, STRATEGY_PARTIAL, STRATEGY_SYMBOLIC };
    const char *names[3] = { "full", "partial", "symbolic" };
    int64_t elapsed[3] = {0, 0, 0};
    int result_bytes[3] = {0, 0, 0};

    for (int s = 0; s < 3; s++) {
        char *result = NULL;
        int64_t t0 = now_us();
        int rc = tensor_reduce(db, content_type, target_id, strategies[s],
                               NULL, NULL, &result);
        int64_t t1 = now_us();

        elapsed[s] = t1 - t0;
        if (rc == 0 && result) {
            result_bytes[s] = (int)strlen(result);
            free(result);
        }

        /* store profile */
        sqlite3_stmt *st = NULL;
        const char *sql =
            "INSERT OR REPLACE INTO _reduce_profiles "
            "(family_id, strategy, elapsed_us, result_bytes, profiled_at) "
            "VALUES (?1, ?2, ?3, ?4, ?5)";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);
            sqlite3_bind_int(st, 2, strategies[s]);
            sqlite3_bind_int64(st, 3, elapsed[s]);
            sqlite3_bind_int(st, 4, result_bytes[s]);
            sqlite3_bind_text(st, 5, ts, -1, SQLITE_STATIC);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    /* build JSON report */
    size_t cap = 1024;
    char *buf = malloc(cap);
    if (!buf) return -1;
    int off = 0;

    off += snprintf(buf + off, cap - off,
        "{\"family_id\":\"%s\",\"target_id\":%d,\"strategies\":[",
        family_id, target_id);

    for (int s = 0; s < 3; s++) {
        if (s > 0) buf[off++] = ',';
        off += snprintf(buf + off, cap - off,
            "{\"name\":\"%s\",\"elapsed_us\":%lld,\"result_bytes\":%d}",
            names[s], (long long)elapsed[s], result_bytes[s]);
    }

    /* recommendation */
    ReduceFeatures feat;
    hybrid_features(db, family_id, &feat);
    ReduceCost cost;
    hybrid_estimate(&feat, &cost);

    off += snprintf(buf + off, cap - off,
        "],\"recommended\":\"%s\",\"confidence\":%.2f,"
        "\"features\":{\"arity\":%d,\"member_count\":%d,"
        "\"mean_eigenvalue\":%.6f,\"selectivity\":%.2f}}",
        names[cost.recommended_strategy], cost.confidence,
        feat.arity, feat.member_count,
        feat.mean_eigenvalue, feat.selectivity);

    *out_json = buf;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  hybrid_stats — memo cache statistics                              */
/* ------------------------------------------------------------------ */
int hybrid_stats(sqlite3 *db, char **out_json)
{
    if (!db || !out_json) return -1;

    int total = 0, full_ct = 0, partial_ct = 0, symbolic_ct = 0;
    int64_t total_bytes = 0;

    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT strategy, COUNT(*), SUM(LENGTH(result)) FROM _reduce_memo "
            "GROUP BY strategy";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                int strat = sqlite3_column_int(st, 0);
                int cnt = sqlite3_column_int(st, 1);
                int64_t bytes = sqlite3_column_int64(st, 2);
                total += cnt;
                total_bytes += bytes;
                if (strat == STRATEGY_FULL) full_ct = cnt;
                else if (strat == STRATEGY_PARTIAL) partial_ct = cnt;
                else if (strat == STRATEGY_SYMBOLIC) symbolic_ct = cnt;
            }
            sqlite3_finalize(st);
        }
    }

    int profiles = 0;
    {
        sqlite3_stmt *st = NULL;
        const char *sql = "SELECT COUNT(*) FROM _reduce_profiles";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW)
                profiles = sqlite3_column_int(st, 0);
            sqlite3_finalize(st);
        }
    }

    size_t cap = 512;
    char *buf = malloc(cap);
    if (!buf) return -1;
    snprintf(buf, cap,
        "{\"memo_entries\":%d,\"memo_bytes\":%lld,"
        "\"by_strategy\":{\"full\":%d,\"partial\":%d,\"symbolic\":%d},"
        "\"profiles\":%d}",
        total, (long long)total_bytes,
        full_ct, partial_ct, symbolic_ct, profiles);

    *out_json = buf;
    return 0;
}
