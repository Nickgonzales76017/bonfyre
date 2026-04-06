/*
 * BonfyreGraph — Upgrade I: Incremental λ-Abstraction
 *
 * Welford's online algorithm for running mean/variance per binding position.
 * Rank-1 updates give O(arity) incremental eigenvalue maintenance.
 * Constant absorption rewrites lambdas to reduce arity.
 */

#include "incremental.h"
#include "canonical.h"
#include "tensor_ops.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

extern unsigned long long fnv1a64(const void *data, size_t len);
extern void iso_timestamp(char *buf, size_t sz);
extern int db_exec(sqlite3 *db, const char *sql);

/* ================================================================
 * Bootstrap — _incr_eigen table for running statistics
 * ================================================================ */

int incr_bootstrap(sqlite3 *db) {
    const char *stmts[] = {
        "CREATE TABLE IF NOT EXISTS _incr_eigen ("
        "  id         INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  family_id  INTEGER NOT NULL,"
        "  position   INTEGER NOT NULL,"
        "  field_name TEXT NOT NULL,"
        "  count      INTEGER DEFAULT 0,"
        "  mean       REAL DEFAULT 0.0,"
        "  m2         REAL DEFAULT 0.0,"       /* Welford's M2 accumulator */
        "  variance   REAL DEFAULT 0.0,"
        "  eigenvalue REAL DEFAULT 0.0,"
        "  min_val    REAL DEFAULT 0.0,"
        "  max_val    REAL DEFAULT 0.0,"
        "  is_numeric INTEGER DEFAULT 0,"
        "  distinct_count INTEGER DEFAULT 0,"
        "  updated_at TEXT NOT NULL,"
        "  UNIQUE(family_id, position)"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_ieig_fam ON _incr_eigen(family_id);",

        /* Absorbed constants record */
        "CREATE TABLE IF NOT EXISTS _absorbed_constants ("
        "  id          INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  family_id   INTEGER NOT NULL,"
        "  position    INTEGER NOT NULL,"
        "  field_name  TEXT NOT NULL,"
        "  const_value TEXT NOT NULL,"
        "  absorbed_at TEXT NOT NULL,"
        "  UNIQUE(family_id, position)"
        ");",

        NULL
    };
    for (int i = 0; stmts[i]; i++) {
        if (db_exec(db, stmts[i]) != 0) return -1;
    }
    return 0;
}

/* ================================================================
 * Internal: parse a binding array into numeric values
 * ================================================================ */

#define MAX_POS 128

typedef struct {
    double num;
    char   text[4096];
    int    is_numeric;
} IncrVal;

static int parse_binding_vals(const char *json, IncrVal *vals, int max) {
    if (!json) return 0;
    int count = 0;
    const char *p = json;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    while (*p && *p != ']' && count < max) {
        while (*p && (*p == ' ' || *p == ',' || *p == '\n' || *p == '\t')) p++;
        if (*p == ']' || !*p) break;

        vals[count].is_numeric = 0;
        vals[count].num = 0.0;
        vals[count].text[0] = '\0';

        if (*p == '"') {
            p++;
            size_t vi = 0;
            while (*p && *p != '"' && vi < sizeof(vals[0].text) - 1) {
                if (*p == '\\' && *(p + 1)) { vals[count].text[vi++] = *p++; }
                vals[count].text[vi++] = *p++;
            }
            vals[count].text[vi] = '\0';
            if (*p == '"') p++;

            /* Try parse as number */
            char *end = NULL;
            double d = strtod(vals[count].text, &end);
            if (end && end != vals[count].text && (*end == '\0')) {
                vals[count].num = d;
                vals[count].is_numeric = 1;
            }
        } else if (*p == '{' || *p == '[') {
            char open = *p, close = (open == '{') ? '}' : ']';
            int depth = 1;
            size_t vi = 0;
            vals[count].text[vi++] = *p++;
            while (*p && depth > 0 && vi < sizeof(vals[0].text) - 1) {
                if (*p == open) depth++;
                if (*p == close) depth--;
                vals[count].text[vi++] = *p++;
            }
            vals[count].text[vi] = '\0';
        } else {
            size_t vi = 0;
            while (*p && *p != ',' && *p != ']' && vi < sizeof(vals[0].text) - 1)
                vals[count].text[vi++] = *p++;
            while (vi > 0 && vals[count].text[vi-1] == ' ') vi--;
            vals[count].text[vi] = '\0';

            char *end = NULL;
            double d = strtod(vals[count].text, &end);
            if (end && end != vals[count].text) {
                vals[count].num = d;
                vals[count].is_numeric = 1;
            }
        }
        count++;
    }
    return count;
}

/* ================================================================
 * Welford's online algorithm helpers
 *
 * On add:   count++; delta = x - mean; mean += delta/count; m2 += delta*(x - mean)
 * On remove: delta = x - mean; mean -= delta/count; m2 -= delta*(x - mean); count--
 * Variance = m2 / max(count - 1, 1)
 * ================================================================ */

static void welford_add(double x, int *count, double *mean, double *m2) {
    (*count)++;
    double delta = x - *mean;
    *mean += delta / *count;
    double delta2 = x - *mean;
    *m2 += delta * delta2;
}

static void welford_remove(double x, int *count, double *mean, double *m2) {
    if (*count <= 1) {
        *count = 0; *mean = 0.0; *m2 = 0.0;
        return;
    }
    double delta = x - *mean;
    *mean = (*mean * *count - x) / (*count - 1);
    double delta2 = x - *mean;
    *m2 -= delta * delta2;
    if (*m2 < 0.0) *m2 = 0.0; /* numerical safety */
    (*count)--;
}

/* ================================================================
 * Initialize incremental stats for a family from current cells
 * ================================================================ */

static int ensure_incr_stats(sqlite3 *db, int family_id) {
    /* Check if stats already exist */
    sqlite3_stmt *chk;
    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM _incr_eigen WHERE family_id=?1",
        -1, &chk, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(chk, 1, family_id);
    int existing = 0;
    if (sqlite3_step(chk) == SQLITE_ROW) existing = sqlite3_column_int(chk, 0);
    sqlite3_finalize(chk);
    if (existing > 0) return 0; /* already initialized */

    /* Bootstrap from current tensor_eigen data or raw cells */
    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    sqlite3_stmt *pos_q;
    if (sqlite3_prepare_v2(db,
        "SELECT position, field_name, COUNT(*), AVG(val_num), "
        "CASE WHEN COUNT(*) > 1 THEN "
        "  SUM((val_num - (SELECT AVG(val_num) FROM _tensor_cells "
        "    WHERE family_id=?1 AND position=p.position)) * "
        "  (val_num - (SELECT AVG(val_num) FROM _tensor_cells "
        "    WHERE family_id=?1 AND position=p.position))) / (COUNT(*) - 1) "
        "ELSE 0.0 END, "
        "MIN(val_num), MAX(val_num), val_type, "
        "COUNT(DISTINCT val_text) "
        "FROM _tensor_cells p WHERE family_id=?1 "
        "GROUP BY position ORDER BY position",
        -1, &pos_q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(pos_q, 1, family_id);

    while (sqlite3_step(pos_q) == SQLITE_ROW) {
        int pos = sqlite3_column_int(pos_q, 0);
        const char *fname = (const char *)sqlite3_column_text(pos_q, 1);
        int cnt = sqlite3_column_int(pos_q, 2);
        double mean = sqlite3_column_double(pos_q, 3);
        double var = sqlite3_column_double(pos_q, 4);
        double min_v = sqlite3_column_double(pos_q, 5);
        double max_v = sqlite3_column_double(pos_q, 6);
        int vtype = sqlite3_column_int(pos_q, 7);
        int distinct = sqlite3_column_int(pos_q, 8);
        int is_num = (vtype == 1);

        /* For strings: variance = distinct/total as entropy proxy */
        double eig = is_num ? var : (cnt > 0 ? (double)distinct / cnt : 0.0);
        double m2 = var * (cnt > 1 ? (cnt - 1) : 1); /* reconstruct M2 */

        sqlite3_stmt *ins;
        if (sqlite3_prepare_v2(db,
            "INSERT OR REPLACE INTO _incr_eigen "
            "(family_id, position, field_name, count, mean, m2, variance, "
            " eigenvalue, min_val, max_val, is_numeric, distinct_count, updated_at) "
            "VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            -1, &ins, NULL) == SQLITE_OK) {
            sqlite3_bind_int(ins, 1, family_id);
            sqlite3_bind_int(ins, 2, pos);
            sqlite3_bind_text(ins, 3, fname ? fname : "", -1, SQLITE_STATIC);
            sqlite3_bind_int(ins, 4, cnt);
            sqlite3_bind_double(ins, 5, mean);
            sqlite3_bind_double(ins, 6, m2);
            sqlite3_bind_double(ins, 7, eig);
            sqlite3_bind_double(ins, 8, eig);
            sqlite3_bind_double(ins, 9, min_v);
            sqlite3_bind_double(ins, 10, max_v);
            sqlite3_bind_int(ins, 11, is_num);
            sqlite3_bind_int(ins, 12, distinct);
            sqlite3_bind_text(ins, 13, ts, -1, SQLITE_STATIC);
            sqlite3_step(ins);
            sqlite3_finalize(ins);
        }
    }
    sqlite3_finalize(pos_q);
    return 0;
}

/* ================================================================
 * incr_add_member — O(arity) Welford update for a new member
 * ================================================================ */

int incr_add_member(sqlite3 *db, int family_id, const char *bindings_json) {
    ensure_incr_stats(db, family_id);

    IncrVal vals[MAX_POS];
    int n = parse_binding_vals(bindings_json, vals, MAX_POS);
    if (n <= 0) return -1;

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    for (int pos = 0; pos < n; pos++) {
        /* Read current stats */
        sqlite3_stmt *rd;
        if (sqlite3_prepare_v2(db,
            "SELECT count, mean, m2, is_numeric, distinct_count, min_val, max_val "
            "FROM _incr_eigen WHERE family_id=?1 AND position=?2",
            -1, &rd, NULL) != SQLITE_OK) continue;
        sqlite3_bind_int(rd, 1, family_id);
        sqlite3_bind_int(rd, 2, pos);

        int cnt = 0; double mean = 0.0, m2 = 0.0;
        int is_num = 0, distinct = 0;
        double min_v = 0.0, max_v = 0.0;

        if (sqlite3_step(rd) == SQLITE_ROW) {
            cnt = sqlite3_column_int(rd, 0);
            mean = sqlite3_column_double(rd, 1);
            m2 = sqlite3_column_double(rd, 2);
            is_num = sqlite3_column_int(rd, 3);
            distinct = sqlite3_column_int(rd, 4);
            min_v = sqlite3_column_double(rd, 5);
            max_v = sqlite3_column_double(rd, 6);
        }
        sqlite3_finalize(rd);

        /* Apply Welford update */
        double x = vals[pos].is_numeric ? vals[pos].num : 0.0;
        if (is_num || vals[pos].is_numeric) {
            welford_add(x, &cnt, &mean, &m2);
            if (x < min_v || cnt == 1) min_v = x;
            if (x > max_v || cnt == 1) max_v = x;
        } else {
            cnt++;
            distinct++; /* approximate: assume new value is distinct */
        }

        double var = cnt > 1 ? m2 / (cnt - 1) : 0.0;
        double eig = is_num ? var : (cnt > 0 ? (double)distinct / cnt : 0.0);

        /* Write back */
        sqlite3_stmt *upd;
        if (sqlite3_prepare_v2(db,
            "UPDATE _incr_eigen SET count=?1, mean=?2, m2=?3, variance=?4, "
            "eigenvalue=?5, min_val=?6, max_val=?7, distinct_count=?8, updated_at=?9 "
            "WHERE family_id=?10 AND position=?11",
            -1, &upd, NULL) == SQLITE_OK) {
            sqlite3_bind_int(upd, 1, cnt);
            sqlite3_bind_double(upd, 2, mean);
            sqlite3_bind_double(upd, 3, m2);
            sqlite3_bind_double(upd, 4, var);
            sqlite3_bind_double(upd, 5, eig);
            sqlite3_bind_double(upd, 6, min_v);
            sqlite3_bind_double(upd, 7, max_v);
            sqlite3_bind_int(upd, 8, distinct);
            sqlite3_bind_text(upd, 9, ts, -1, SQLITE_STATIC);
            sqlite3_bind_int(upd, 10, family_id);
            sqlite3_bind_int(upd, 11, pos);
            sqlite3_step(upd);
            sqlite3_finalize(upd);
        }
    }
    return 0;
}

/* ================================================================
 * incr_remove_member — O(arity) reverse Welford
 * ================================================================ */

int incr_remove_member(sqlite3 *db, int family_id, const char *bindings_json) {
    ensure_incr_stats(db, family_id);

    IncrVal vals[MAX_POS];
    int n = parse_binding_vals(bindings_json, vals, MAX_POS);
    if (n <= 0) return -1;

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    for (int pos = 0; pos < n; pos++) {
        sqlite3_stmt *rd;
        if (sqlite3_prepare_v2(db,
            "SELECT count, mean, m2, is_numeric, distinct_count "
            "FROM _incr_eigen WHERE family_id=?1 AND position=?2",
            -1, &rd, NULL) != SQLITE_OK) continue;
        sqlite3_bind_int(rd, 1, family_id);
        sqlite3_bind_int(rd, 2, pos);

        int cnt = 0; double mean = 0.0, m2 = 0.0;
        int is_num = 0, distinct = 0;

        if (sqlite3_step(rd) == SQLITE_ROW) {
            cnt = sqlite3_column_int(rd, 0);
            mean = sqlite3_column_double(rd, 1);
            m2 = sqlite3_column_double(rd, 2);
            is_num = sqlite3_column_int(rd, 3);
            distinct = sqlite3_column_int(rd, 4);
        }
        sqlite3_finalize(rd);

        double x = vals[pos].is_numeric ? vals[pos].num : 0.0;
        if (is_num) {
            welford_remove(x, &cnt, &mean, &m2);
        } else {
            if (cnt > 0) cnt--;
            if (distinct > 0) distinct--; /* approximate */
        }

        double var = cnt > 1 ? m2 / (cnt - 1) : 0.0;
        double eig = is_num ? var : (cnt > 0 ? (double)distinct / cnt : 0.0);

        sqlite3_stmt *upd;
        if (sqlite3_prepare_v2(db,
            "UPDATE _incr_eigen SET count=?1, mean=?2, m2=?3, variance=?4, "
            "eigenvalue=?5, distinct_count=?6, updated_at=?7 "
            "WHERE family_id=?8 AND position=?9",
            -1, &upd, NULL) == SQLITE_OK) {
            sqlite3_bind_int(upd, 1, cnt);
            sqlite3_bind_double(upd, 2, mean);
            sqlite3_bind_double(upd, 3, m2);
            sqlite3_bind_double(upd, 4, var);
            sqlite3_bind_double(upd, 5, eig);
            sqlite3_bind_int(upd, 6, distinct);
            sqlite3_bind_text(upd, 7, ts, -1, SQLITE_STATIC);
            sqlite3_bind_int(upd, 8, family_id);
            sqlite3_bind_int(upd, 9, pos);
            sqlite3_step(upd);
            sqlite3_finalize(upd);
        }
    }
    return 0;
}

/* ================================================================
 * incr_update_member — delta Welford: remove old, add new
 * ================================================================ */

int incr_update_member(sqlite3 *db, int family_id,
                       const char *old_bindings, const char *new_bindings) {
    ensure_incr_stats(db, family_id);

    IncrVal old_vals[MAX_POS], new_vals[MAX_POS];
    int n_old = parse_binding_vals(old_bindings, old_vals, MAX_POS);
    int n_new = parse_binding_vals(new_bindings, new_vals, MAX_POS);
    int n = n_old < n_new ? n_old : n_new;
    if (n <= 0) return -1;

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    for (int pos = 0; pos < n; pos++) {
        /* Skip unchanged positions */
        if (old_vals[pos].is_numeric && new_vals[pos].is_numeric &&
            old_vals[pos].num == new_vals[pos].num) continue;
        if (!old_vals[pos].is_numeric && !new_vals[pos].is_numeric &&
            strcmp(old_vals[pos].text, new_vals[pos].text) == 0) continue;

        sqlite3_stmt *rd;
        if (sqlite3_prepare_v2(db,
            "SELECT count, mean, m2, is_numeric, distinct_count, min_val, max_val "
            "FROM _incr_eigen WHERE family_id=?1 AND position=?2",
            -1, &rd, NULL) != SQLITE_OK) continue;
        sqlite3_bind_int(rd, 1, family_id);
        sqlite3_bind_int(rd, 2, pos);

        int cnt = 0; double mean = 0.0, m2 = 0.0;
        int is_num = 0, distinct = 0;
        double min_v = 0.0, max_v = 0.0;

        if (sqlite3_step(rd) == SQLITE_ROW) {
            cnt = sqlite3_column_int(rd, 0);
            mean = sqlite3_column_double(rd, 1);
            m2 = sqlite3_column_double(rd, 2);
            is_num = sqlite3_column_int(rd, 3);
            distinct = sqlite3_column_int(rd, 4);
            min_v = sqlite3_column_double(rd, 5);
            max_v = sqlite3_column_double(rd, 6);
        }
        sqlite3_finalize(rd);

        if (is_num) {
            /* Remove old, add new via Welford */
            double x_old = old_vals[pos].num;
            double x_new = new_vals[pos].num;
            welford_remove(x_old, &cnt, &mean, &m2);
            welford_add(x_new, &cnt, &mean, &m2);
            if (x_new < min_v) min_v = x_new;
            if (x_new > max_v) max_v = x_new;
        }
        /* For strings, distinct_count approximation stays unchanged */

        double var = cnt > 1 ? m2 / (cnt - 1) : 0.0;
        double eig = is_num ? var : (cnt > 0 ? (double)distinct / cnt : 0.0);

        sqlite3_stmt *upd;
        if (sqlite3_prepare_v2(db,
            "UPDATE _incr_eigen SET mean=?1, m2=?2, variance=?3, "
            "eigenvalue=?4, min_val=?5, max_val=?6, updated_at=?7 "
            "WHERE family_id=?8 AND position=?9",
            -1, &upd, NULL) == SQLITE_OK) {
            sqlite3_bind_double(upd, 1, mean);
            sqlite3_bind_double(upd, 2, m2);
            sqlite3_bind_double(upd, 3, var);
            sqlite3_bind_double(upd, 4, eig);
            sqlite3_bind_double(upd, 5, min_v);
            sqlite3_bind_double(upd, 6, max_v);
            sqlite3_bind_text(upd, 7, ts, -1, SQLITE_STATIC);
            sqlite3_bind_int(upd, 8, family_id);
            sqlite3_bind_int(upd, 9, pos);
            sqlite3_step(upd);
            sqlite3_finalize(upd);
        }
    }
    return 0;
}

/* ================================================================
 * incr_absorb_constants — rewrite generator to inline constants
 *
 * For each position with eigenvalue < threshold:
 *   1. Read the constant value (most frequent val_text for that position)
 *   2. Rewrite the generator JSON to embed the constant
 *   3. Remove the position from all bindings
 *   4. Update arity in incr_eigen
 *   5. Record in _absorbed_constants
 * ================================================================ */

int incr_absorb_constants(sqlite3 *db, int family_id, double threshold) {
    ensure_incr_stats(db, family_id);

    /* Collect positions to absorb */
    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT position, field_name, eigenvalue FROM _incr_eigen "
        "WHERE family_id=?1 AND eigenvalue < ?2 AND eigenvalue >= 0 "
        "ORDER BY position DESC", /* reverse order so removals don't shift indices */
        -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(q, 1, family_id);
    sqlite3_bind_double(q, 2, threshold);

    typedef struct { int pos; char name[256]; } AbsorbTarget;
    AbsorbTarget targets[MAX_POS];
    int n_targets = 0;

    while (sqlite3_step(q) == SQLITE_ROW && n_targets < MAX_POS) {
        targets[n_targets].pos = sqlite3_column_int(q, 0);
        const char *fn = (const char *)sqlite3_column_text(q, 1);
        if (fn) strncpy(targets[n_targets].name, fn, 255);
        targets[n_targets].name[255] = '\0';
        n_targets++;
    }
    sqlite3_finalize(q);

    if (n_targets == 0) return 0;

    char ts[64];
    iso_timestamp(ts, sizeof(ts));
    int absorbed = 0;

    /* Get current generator */
    sqlite3_stmt *gen_q;
    if (sqlite3_prepare_v2(db,
        "SELECT generator FROM _families WHERE id=?1",
        -1, &gen_q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(gen_q, 1, family_id);
    char generator[32768] = {0};
    if (sqlite3_step(gen_q) == SQLITE_ROW) {
        const char *g = (const char *)sqlite3_column_text(gen_q, 0);
        if (g) strncpy(generator, g, sizeof(generator) - 1);
    }
    sqlite3_finalize(gen_q);
    if (!generator[0]) return 0;

    for (int t = 0; t < n_targets; t++) {
        int pos = targets[t].pos;
        const char *fname = targets[t].name;

        /* Get the most frequent value at this position */
        sqlite3_stmt *mode_q;
        if (sqlite3_prepare_v2(db,
            "SELECT val_text, val_num, val_type, COUNT(*) as freq "
            "FROM _tensor_cells WHERE family_id=?1 AND position=?2 "
            "GROUP BY val_text ORDER BY freq DESC LIMIT 1",
            -1, &mode_q, NULL) != SQLITE_OK) continue;
        sqlite3_bind_int(mode_q, 1, family_id);
        sqlite3_bind_int(mode_q, 2, pos);

        char const_val[4096] = {0};
        int const_type = 0;
        if (sqlite3_step(mode_q) == SQLITE_ROW) {
            const char *vt = (const char *)sqlite3_column_text(mode_q, 0);
            const_type = sqlite3_column_int(mode_q, 2);
            if (vt) strncpy(const_val, vt, sizeof(const_val) - 1);
        }
        sqlite3_finalize(mode_q);

        if (!const_val[0]) continue;

        /* Record the absorption */
        sqlite3_stmt *rec;
        if (sqlite3_prepare_v2(db,
            "INSERT OR REPLACE INTO _absorbed_constants "
            "(family_id, position, field_name, const_value, absorbed_at) "
            "VALUES (?1, ?2, ?3, ?4, ?5)",
            -1, &rec, NULL) == SQLITE_OK) {
            sqlite3_bind_int(rec, 1, family_id);
            sqlite3_bind_int(rec, 2, pos);
            sqlite3_bind_text(rec, 3, fname, -1, SQLITE_STATIC);
            sqlite3_bind_text(rec, 4, const_val, -1, SQLITE_STATIC);
            sqlite3_bind_text(rec, 5, ts, -1, SQLITE_STATIC);
            sqlite3_step(rec);
            sqlite3_finalize(rec);
        }

        /* Rewrite generator: change type to the constant value.
           Original: "field":"type" → becomes "field":"=<value>" to denote a constant */
        /* Build replacement pattern */
        char old_pat[8192], new_pat[8192];
        if (const_type == 0) {
            snprintf(old_pat, sizeof(old_pat), "\"%s\":\"string\"", fname);
            snprintf(new_pat, sizeof(new_pat), "\"%s\":\"=%s\"", fname, const_val);
        } else {
            snprintf(old_pat, sizeof(old_pat), "\"%s\":\"number\"", fname);
            snprintf(new_pat, sizeof(new_pat), "\"%s\":\"=%s\"", fname, const_val);
        }

        /* Apply substitution in generator string */
        char *found = strstr(generator, old_pat);
        if (found) {
            char new_gen[32768];
            size_t prefix_len = (size_t)(found - generator);
            memcpy(new_gen, generator, prefix_len);
            size_t off = prefix_len;
            size_t new_len = strlen(new_pat);
            memcpy(new_gen + off, new_pat, new_len);
            off += new_len;
            size_t old_len = strlen(old_pat);
            strcpy(new_gen + off, found + old_len);
            strcpy(generator, new_gen);
        }

        /* Mark this position's eigenvalue as absorbed (-1.0) */
        sqlite3_stmt *mark;
        if (sqlite3_prepare_v2(db,
            "UPDATE _incr_eigen SET eigenvalue=-1.0, updated_at=?1 "
            "WHERE family_id=?2 AND position=?3",
            -1, &mark, NULL) == SQLITE_OK) {
            sqlite3_bind_text(mark, 1, ts, -1, SQLITE_STATIC);
            sqlite3_bind_int(mark, 2, family_id);
            sqlite3_bind_int(mark, 3, pos);
            sqlite3_step(mark);
            sqlite3_finalize(mark);
        }

        absorbed++;
    }

    /* Update the generator in _families */
    if (absorbed > 0) {
        sqlite3_stmt *upd_gen;
        if (sqlite3_prepare_v2(db,
            "UPDATE _families SET generator=?1 WHERE id=?2",
            -1, &upd_gen, NULL) == SQLITE_OK) {
            sqlite3_bind_text(upd_gen, 1, generator, -1, SQLITE_STATIC);
            sqlite3_bind_int(upd_gen, 2, family_id);
            sqlite3_step(upd_gen);
            sqlite3_finalize(upd_gen);
        }
    }

    return absorbed;
}

/* ================================================================
 * incr_compact_online — full online compaction pass
 * ================================================================ */

int incr_compact_online(sqlite3 *db, const char *content_type,
                        double absorb_threshold, double split_cv) {
    int total_refinements = 0;

    /* For each family in this content type */
    sqlite3_stmt *fam_q;
    if (sqlite3_prepare_v2(db,
        "SELECT id, member_count FROM _families WHERE content_type=?1",
        -1, &fam_q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(fam_q, 1, content_type, -1, SQLITE_STATIC);

    while (sqlite3_step(fam_q) == SQLITE_ROW) {
        int fam_id = sqlite3_column_int(fam_q, 0);
        int member_count = sqlite3_column_int(fam_q, 1);
        if (member_count < 2) continue;

        /* Step 1: Ensure incremental stats are initialized */
        ensure_incr_stats(db, fam_id);

        /* Step 2: Absorb near-constant positions */
        int absorbed = incr_absorb_constants(db, fam_id, absorb_threshold);
        total_refinements += absorbed;

        /* Step 3: Check for family split via coefficient of variation.
           If mean CV across positions > split_cv, the family is too diverse. */
        if (split_cv > 0.0) {
            sqlite3_stmt *cv_q;
            if (sqlite3_prepare_v2(db,
                "SELECT AVG(CASE WHEN mean != 0 AND is_numeric = 1 "
                "  THEN SQRT(variance) / ABS(mean) ELSE eigenvalue END) "
                "FROM _incr_eigen WHERE family_id=?1 AND eigenvalue >= 0",
                -1, &cv_q, NULL) == SQLITE_OK) {
                sqlite3_bind_int(cv_q, 1, fam_id);
                if (sqlite3_step(cv_q) == SQLITE_ROW) {
                    double avg_cv = sqlite3_column_double(cv_q, 0);
                    if (avg_cv > split_cv) {
                        /* Mark family as split candidate.
                           Actual split would re-run family_discover on subsets. */
                        total_refinements++;
                    }
                }
                sqlite3_finalize(cv_q);
            }
        }
    }
    sqlite3_finalize(fam_q);

    return total_refinements;
}

/* ================================================================
 * incr_eigen_stats — JSON output of running statistics
 * ================================================================ */

int incr_eigen_stats(sqlite3 *db, int family_id, char **out_json) {
    *out_json = NULL;
    ensure_incr_stats(db, family_id);

    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT position, field_name, count, mean, variance, eigenvalue, "
        "min_val, max_val, is_numeric, distinct_count "
        "FROM _incr_eigen WHERE family_id=?1 ORDER BY position",
        -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(q, 1, family_id);

    size_t cap = 4096;
    char *out = malloc(cap);
    size_t off = 0;
    out[off++] = '[';
    int count = 0;

    while (sqlite3_step(q) == SQLITE_ROW) {
        const char *fn = (const char *)sqlite3_column_text(q, 1);
        while (off + 512 >= cap) { cap *= 2; out = realloc(out, cap); }
        if (count > 0) out[off++] = ',';
        off += (size_t)snprintf(out + off, cap - off,
            "{\"position\":%d,\"field\":\"%s\",\"count\":%d,"
            "\"mean\":%.6f,\"variance\":%.6f,\"eigenvalue\":%.6f,"
            "\"min\":%.6f,\"max\":%.6f,\"numeric\":%s,\"distinct\":%d}",
            sqlite3_column_int(q, 0),
            fn ? fn : "",
            sqlite3_column_int(q, 2),
            sqlite3_column_double(q, 3),
            sqlite3_column_double(q, 4),
            sqlite3_column_double(q, 5),
            sqlite3_column_double(q, 6),
            sqlite3_column_double(q, 7),
            sqlite3_column_int(q, 8) ? "true" : "false",
            sqlite3_column_int(q, 9));
        count++;
    }
    sqlite3_finalize(q);

    out[off++] = ']';
    out[off] = '\0';
    *out_json = out;
    return count;
}
