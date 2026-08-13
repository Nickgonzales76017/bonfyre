/*
 * fragment.c — Bonfyre Fragment System implementation
 *
 * SQLite-backed store for content-addressed perspective fragments.
 * See fragment.h for full API documentation.
 */

#include "fragment.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include <sqlite3.h>
#include <openssl/sha.h>

/* ─────────────────────────────────────────────────────────────────
 * Internal types
 * ───────────────────────────────────────────────────────────────── */

struct bf_fragment_store {
    sqlite3    *db;
    char        errmsg[512];
};

/* ─────────────────────────────────────────────────────────────────
 * Utility: SHA-256 → hex string ("sha256:<64 hex chars>")
 * ───────────────────────────────────────────────────────────────── */

static void sha256_hex(const char *data, size_t len,
                        char out[BF_FRAGMENT_ID_LEN]) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256((const unsigned char *)data, len, digest);

    out[0] = '\0';
    strcat(out, "sha256:");
    char *p = out + 7;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        sprintf(p + i * 2, "%02x", digest[i]);
    }
}

/* ─────────────────────────────────────────────────────────────────
 * Utility: ensure directory exists
 * ───────────────────────────────────────────────────────────────── */

static int ensure_dir(const char *path) {
    char tmp[4096];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
            *p = '/';
        }
    }
    return 0;
}

/* ─────────────────────────────────────────────────────────────────
 * Schema
 * ───────────────────────────────────────────────────────────────── */

static const char *SCHEMA = 
    "CREATE TABLE IF NOT EXISTS fragments ("
    "  id           TEXT PRIMARY KEY,"
    "  kind         TEXT NOT NULL,"
    "  perspective  TEXT NOT NULL,"
    "  confidence   REAL NOT NULL DEFAULT 0.5,"
    "  start_ms     INTEGER NOT NULL DEFAULT -1,"
    "  end_ms       INTEGER NOT NULL DEFAULT -1,"
    "  created_at   INTEGER NOT NULL,"
    "  payload_json TEXT NOT NULL,"
    "  superseded   INTEGER NOT NULL DEFAULT 0"
    ");"

    "CREATE TABLE IF NOT EXISTS fragment_parents ("
    "  child_id   TEXT NOT NULL,"
    "  parent_id  TEXT NOT NULL,"
    "  PRIMARY KEY (child_id, parent_id),"
    "  FOREIGN KEY (child_id)  REFERENCES fragments(id),"
    "  FOREIGN KEY (parent_id) REFERENCES fragments(id)"
    ");"

    "CREATE INDEX IF NOT EXISTS idx_fragments_kind_conf"
    "  ON fragments(kind, confidence DESC);"
    "CREATE INDEX IF NOT EXISTS idx_fragments_perspective"
    "  ON fragments(perspective);"
    "CREATE INDEX IF NOT EXISTS idx_fragments_time"
    "  ON fragments(start_ms, end_ms);"
    "CREATE VIRTUAL TABLE IF NOT EXISTS fragments_fts"
    "  USING fts5(id UNINDEXED, kind, perspective, payload_json);";

/* ─────────────────────────────────────────────────────────────────
 * Store lifecycle
 * ───────────────────────────────────────────────────────────────── */

bf_fragment_store_t *bf_fragment_store_open(const char *path) {
    bf_fragment_store_t *store = calloc(1, sizeof(*store));
    if (!store) return NULL;

    /* Ensure parent directory exists */
    char dir[4096];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) { *slash = '\0'; ensure_dir(dir); }

    int rc = sqlite3_open(path, &store->db);
    if (rc != SQLITE_OK) {
        snprintf(store->errmsg, sizeof(store->errmsg),
                 "sqlite3_open: %s", sqlite3_errmsg(store->db));
        sqlite3_close(store->db);
        free(store);
        return NULL;
    }

    /* WAL mode + foreign keys */
    sqlite3_exec(store->db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(store->db, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);

    /* Create schema */
    char *errmsg = NULL;
    rc = sqlite3_exec(store->db, SCHEMA, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
        snprintf(store->errmsg, sizeof(store->errmsg),
                 "schema: %s", errmsg ? errmsg : "unknown");
        sqlite3_free(errmsg);
        sqlite3_close(store->db);
        free(store);
        return NULL;
    }

    return store;
}

void bf_fragment_store_close(bf_fragment_store_t *store) {
    if (!store) return;
    sqlite3_close(store->db);
    free(store);
}

const char *bf_fragment_store_errmsg(const bf_fragment_store_t *store) {
    return store ? store->errmsg : "null store";
}

/* ─────────────────────────────────────────────────────────────────
 * Create
 * ───────────────────────────────────────────────────────────────── */

int bf_fragment_create(bf_fragment_store_t *store,
                        const char *kind,
                        const char *perspective,
                        float confidence,
                        int64_t start_ms,
                        int64_t end_ms,
                        const char *payload_json,
                        const char **parent_ids,
                        int parent_count,
                        char id_out[BF_FRAGMENT_ID_LEN]) {
    if (!store || !kind || !perspective || !payload_json) return -1;

    /* Build canonical string for content-addressing */
    char canonical[65536];
    snprintf(canonical, sizeof(canonical),
             "{\"kind\":\"%s\",\"perspective\":\"%s\","
             "\"confidence\":%.6f,\"start_ms\":%lld,\"end_ms\":%lld,"
             "\"payload\":%s}",
             kind, perspective, (double)confidence,
             (long long)start_ms, (long long)end_ms,
             payload_json);

    sha256_hex(canonical, strlen(canonical), id_out);

    /* Insert (ignore if already exists — idempotent) */
    const char *sql =
        "INSERT OR IGNORE INTO fragments"
        " (id, kind, perspective, confidence, start_ms, end_ms,"
        "  created_at, payload_json)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(store->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        snprintf(store->errmsg, sizeof(store->errmsg),
                 "prepare insert: %s", sqlite3_errmsg(store->db));
        return -1;
    }

    sqlite3_bind_text(stmt, 1, id_out,       -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, kind,          -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, perspective,   -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 4, (double)confidence);
    sqlite3_bind_int64(stmt, 5, start_ms);
    sqlite3_bind_int64(stmt, 6, end_ms);
    sqlite3_bind_int64(stmt, 7, (int64_t)time(NULL));
    sqlite3_bind_text(stmt, 8, payload_json, -1, SQLITE_STATIC);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
        snprintf(store->errmsg, sizeof(store->errmsg),
                 "insert fragment: %s", sqlite3_errmsg(store->db));
        return -1;
    }

    /* Insert parent links */
    for (int i = 0; i < parent_count && parent_ids && parent_ids[i]; i++) {
        const char *psql =
            "INSERT OR IGNORE INTO fragment_parents (child_id, parent_id)"
            " VALUES (?, ?)";
        sqlite3_stmt *pstmt;
        if (sqlite3_prepare_v2(store->db, psql, -1, &pstmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(pstmt, 1, id_out,          -1, SQLITE_STATIC);
            sqlite3_bind_text(pstmt, 2, parent_ids[i],   -1, SQLITE_STATIC);
            sqlite3_step(pstmt);
            sqlite3_finalize(pstmt);
        }
    }

    /* FTS insert (ignore errors — FTS is auxiliary) */
    const char *fts_sql =
        "INSERT OR IGNORE INTO fragments_fts (id, kind, perspective, payload_json)"
        " VALUES (?, ?, ?, ?)";
    sqlite3_stmt *fstmt;
    if (sqlite3_prepare_v2(store->db, fts_sql, -1, &fstmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(fstmt, 1, id_out,       -1, SQLITE_STATIC);
        sqlite3_bind_text(fstmt, 2, kind,          -1, SQLITE_STATIC);
        sqlite3_bind_text(fstmt, 3, perspective,   -1, SQLITE_STATIC);
        sqlite3_bind_text(fstmt, 4, payload_json,  -1, SQLITE_STATIC);
        sqlite3_step(fstmt);
        sqlite3_finalize(fstmt);
    }

    return 0;
}

/* ─────────────────────────────────────────────────────────────────
 * Get
 * ───────────────────────────────────────────────────────────────── */

static bf_fragment_t *row_to_fragment(sqlite3_stmt *stmt) {
    bf_fragment_t *f = calloc(1, sizeof(*f));
    if (!f) return NULL;

    const char *id = (const char *)sqlite3_column_text(stmt, 0);
    const char *kind = (const char *)sqlite3_column_text(stmt, 1);
    const char *persp = (const char *)sqlite3_column_text(stmt, 2);
    const char *payload = (const char *)sqlite3_column_text(stmt, 7);

    if (id)    strncpy(f->id,          id,    sizeof(f->id) - 1);
    if (kind)  strncpy(f->kind,        kind,  sizeof(f->kind) - 1);
    if (persp) strncpy(f->perspective, persp, sizeof(f->perspective) - 1);

    f->confidence = (float)sqlite3_column_double(stmt, 3);
    f->start_ms   = sqlite3_column_int64(stmt, 4);
    f->end_ms     = sqlite3_column_int64(stmt, 5);
    f->created_at = sqlite3_column_int64(stmt, 6);
    f->payload_json = payload ? strdup(payload) : strdup("{}");

    return f;
}

bf_fragment_t *bf_fragment_get(bf_fragment_store_t *store, const char *id) {
    if (!store || !id) return NULL;

    const char *sql =
        "SELECT id, kind, perspective, confidence, start_ms, end_ms,"
        "       created_at, payload_json"
        " FROM fragments WHERE id = ?";

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(store->db, sql, -1, &stmt, NULL) != SQLITE_OK)
        return NULL;

    sqlite3_bind_text(stmt, 1, id, -1, SQLITE_STATIC);

    bf_fragment_t *frag = NULL;
    if (sqlite3_step(stmt) == SQLITE_ROW)
        frag = row_to_fragment(stmt);

    sqlite3_finalize(stmt);
    return frag;
}

void bf_fragment_free(bf_fragment_t *frag) {
    if (!frag) return;
    free(frag->payload_json);
    free(frag);
}

/* ─────────────────────────────────────────────────────────────────
 * Query
 * ───────────────────────────────────────────────────────────────── */

bf_fragment_t **bf_fragment_query(bf_fragment_store_t *store,
                                   const bf_fragment_query_t *filter,
                                   int *count_out) {
    if (!store || !filter) { if (count_out) *count_out = -1; return NULL; }

    /* Build WHERE clause dynamically */
    char where[2048] = "WHERE superseded = 0";
    char limit_clause[64] = "";

    if (filter->kind && filter->kind[0]) {
        char esc[128];
        snprintf(esc, sizeof(esc), " AND kind = '%s'", filter->kind);
        strncat(where, esc, sizeof(where) - strlen(where) - 1);
    }
    if (filter->perspective && filter->perspective[0]) {
        char esc[256];
        snprintf(esc, sizeof(esc), " AND perspective = '%s'", filter->perspective);
        strncat(where, esc, sizeof(where) - strlen(where) - 1);
    }
    if (filter->min_confidence > 0.0f) {
        char esc[64];
        snprintf(esc, sizeof(esc), " AND confidence >= %.4f", (double)filter->min_confidence);
        strncat(where, esc, sizeof(where) - strlen(where) - 1);
    }
    if (filter->start_after_ms >= 0) {
        char esc[64];
        snprintf(esc, sizeof(esc), " AND start_ms >= %lld", (long long)filter->start_after_ms);
        strncat(where, esc, sizeof(where) - strlen(where) - 1);
    }
    if (filter->end_before_ms >= 0) {
        char esc[64];
        snprintf(esc, sizeof(esc), " AND end_ms <= %lld", (long long)filter->end_before_ms);
        strncat(where, esc, sizeof(where) - strlen(where) - 1);
    }
    if (filter->limit > 0) {
        snprintf(limit_clause, sizeof(limit_clause),
                 " LIMIT %d OFFSET %d", filter->limit, filter->offset);
    }

    char sql[4096];
    snprintf(sql, sizeof(sql),
             "SELECT id, kind, perspective, confidence, start_ms, end_ms,"
             "       created_at, payload_json"
             " FROM fragments %s ORDER BY confidence DESC%s",
             where, limit_clause);

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(store->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        if (count_out) *count_out = -1;
        return NULL;
    }

    /* Collect into dynamic array */
    bf_fragment_t **results = NULL;
    int count = 0, cap = 32;
    results = malloc(cap * sizeof(bf_fragment_t *));
    if (!results) { sqlite3_finalize(stmt); if (count_out) *count_out = -1; return NULL; }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (count >= cap - 1) {
            cap *= 2;
            bf_fragment_t **tmp = realloc(results, cap * sizeof(bf_fragment_t *));
            if (!tmp) break;
            results = tmp;
        }
        results[count++] = row_to_fragment(stmt);
    }
    results[count] = NULL;  /* NULL-terminate */

    sqlite3_finalize(stmt);
    if (count_out) *count_out = count;
    return results;
}

/* ─────────────────────────────────────────────────────────────────
 * Merge
 * ───────────────────────────────────────────────────────────────── */

int bf_fragment_merge(bf_fragment_store_t *dst,
                       bf_fragment_store_t *src,
                       bf_merge_result_t *result_out) {
    if (!dst || !src) return -1;

    bf_merge_result_t result = {0};

    /* Walk all non-superseded fragments in src */
    const char *sql =
        "SELECT id, kind, perspective, confidence, start_ms, end_ms,"
        "       created_at, payload_json"
        " FROM fragments WHERE superseded = 0";

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(src->db, sql, -1, &stmt, NULL) != SQLITE_OK)
        return -1;

    sqlite3_exec(dst->db, "BEGIN;", NULL, NULL, NULL);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        bf_fragment_t *frag = row_to_fragment(stmt);
        if (!frag) continue;

        /* Check for conflict: same kind, overlapping time, different payload */
        const char *conflict_sql =
            "SELECT id, confidence FROM fragments"
            " WHERE kind = ? AND superseded = 0"
            "   AND start_ms <= ? AND end_ms >= ?"
            "   AND id != ?"
            " ORDER BY confidence DESC LIMIT 1";

        sqlite3_stmt *cstmt;
        int conflict = 0;
        if (sqlite3_prepare_v2(dst->db, conflict_sql, -1, &cstmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(cstmt, 1, frag->kind, -1, SQLITE_STATIC);
            sqlite3_bind_int64(cstmt, 2, frag->end_ms);
            sqlite3_bind_int64(cstmt, 3, frag->start_ms);
            sqlite3_bind_text(cstmt, 4, frag->id, -1, SQLITE_STATIC);

            if (frag->start_ms >= 0 && frag->end_ms >= 0 &&
                sqlite3_step(cstmt) == SQLITE_ROW) {
                float existing_conf = (float)sqlite3_column_double(cstmt, 1);
                float delta = frag->confidence - existing_conf;

                if (delta < 0.0f) delta = -delta;

                if (delta >= 0.1f) {
                    /* Replace lower-confidence with superseded tag */
                    const char *existing_id = (const char *)sqlite3_column_text(cstmt, 0);
                    float existing_c = (float)sqlite3_column_double(cstmt, 1);

                    if (frag->confidence > existing_c) {
                        /* Supersede existing */
                        char supersede_sql[256];
                        snprintf(supersede_sql, sizeof(supersede_sql),
                                 "UPDATE fragments SET superseded=1 WHERE id='%s'",
                                 existing_id);
                        sqlite3_exec(dst->db, supersede_sql, NULL, NULL, NULL);
                        result.conflicts_resolved++;
                    } else {
                        /* src fragment is weaker — still add as competing */
                        conflict = 1;
                        result.conflicts_deferred++;
                    }
                } else {
                    /* Close confidence — keep both as competing perspectives */
                    conflict = 1;
                    result.conflicts_detected++;
                    result.conflicts_deferred++;
                }
            }
            sqlite3_finalize(cstmt);
        }
        (void)conflict;  /* Always insert; conflict only controls superseding */

        /* Insert into dst */
        char id_out[BF_FRAGMENT_ID_LEN];
        int rc = bf_fragment_create(dst,
                                     frag->kind, frag->perspective,
                                     frag->confidence,
                                     frag->start_ms, frag->end_ms,
                                     frag->payload_json,
                                     NULL, 0,
                                     id_out);
        if (rc == 0) result.fragments_added++;

        bf_fragment_free(frag);
    }

    sqlite3_finalize(stmt);
    sqlite3_exec(dst->db, "COMMIT;", NULL, NULL, NULL);

    if (result_out) *result_out = result;
    return 0;
}

/* ─────────────────────────────────────────────────────────────────
 * Diff
 * ───────────────────────────────────────────────────────────────── */

bf_fragment_diff_t *bf_fragment_diff(bf_fragment_store_t *a,
                                      bf_fragment_store_t *b,
                                      const char *kind,
                                      int *count_out) {
    if (!a || !b) { if (count_out) *count_out = 0; return NULL; }

    char kind_filter_a[256] = "";
    char kind_filter_b[256] = "";
    if (kind && kind[0]) {
        snprintf(kind_filter_a, sizeof(kind_filter_a), " AND kind = '%s'", kind);
        snprintf(kind_filter_b, sizeof(kind_filter_b), " AND kind = '%s'", kind);
    }

    /* Fragments in A but not in B (by ID) */
    char sql_a[1024];
    snprintf(sql_a, sizeof(sql_a),
             "SELECT id, kind, perspective, confidence, start_ms, end_ms,"
             "       created_at, payload_json"
             " FROM fragments WHERE superseded=0%s", kind_filter_a);

    char sql_b[1024];
    snprintf(sql_b, sizeof(sql_b),
             "SELECT id, kind, perspective, confidence FROM fragments"
             " WHERE superseded=0%s", kind_filter_b);

    sqlite3_stmt *stmt_a, *stmt_b;
    if (sqlite3_prepare_v2(a->db, sql_a, -1, &stmt_a, NULL) != SQLITE_OK) {
        if (count_out) *count_out = 0; return NULL;
    }

    int cap = 64, count = 0;
    bf_fragment_diff_t *diffs = malloc(cap * sizeof(*diffs));
    if (!diffs) { sqlite3_finalize(stmt_a); if (count_out)*count_out=0; return NULL; }

    while (sqlite3_step(stmt_a) == SQLITE_ROW) {
        const char *id_a    = (const char *)sqlite3_column_text(stmt_a, 0);
        const char *kind_a  = (const char *)sqlite3_column_text(stmt_a, 1);
        const char *persp_a = (const char *)sqlite3_column_text(stmt_a, 2);
        float conf_a        = (float)sqlite3_column_double(stmt_a, 3);
        int64_t s_ms        = sqlite3_column_int64(stmt_a, 4);
        int64_t e_ms        = sqlite3_column_int64(stmt_a, 5);

        /* Look for same kind + overlapping time in B */
        char lookup[1024];
        snprintf(lookup, sizeof(lookup),
                 "SELECT id, perspective, confidence FROM fragments"
                 " WHERE kind='%s' AND superseded=0"
                 "   AND start_ms <= %lld AND end_ms >= %lld AND id != '%s'"
                 " ORDER BY confidence DESC LIMIT 1",
                 kind_a, (long long)e_ms, (long long)s_ms, id_a);

        if (sqlite3_prepare_v2(b->db, lookup, -1, &stmt_b, NULL) != SQLITE_OK)
            continue;

        if (count >= cap - 1) {
            cap *= 2;
            bf_fragment_diff_t *tmp = realloc(diffs, cap * sizeof(*diffs));
            if (!tmp) { sqlite3_finalize(stmt_b); break; }
            diffs = tmp;
        }

        bf_fragment_diff_t *d = &diffs[count];
        memset(d, 0, sizeof(*d));
        strncpy(d->kind,           kind_a  ? kind_a  : "", sizeof(d->kind) - 1);
        strncpy(d->perspective_a,  persp_a ? persp_a : "", sizeof(d->perspective_a) - 1);
        strncpy(d->id_a,           id_a    ? id_a    : "", sizeof(d->id_a) - 1);
        d->confidence_a = conf_a;

        if (sqlite3_step(stmt_b) == SQLITE_ROW) {
            const char *id_b    = (const char *)sqlite3_column_text(stmt_b, 0);
            const char *persp_b = (const char *)sqlite3_column_text(stmt_b, 1);
            float conf_b        = (float)sqlite3_column_double(stmt_b, 2);

            strncpy(d->id_b,           id_b    ? id_b    : "", sizeof(d->id_b) - 1);
            strncpy(d->perspective_b,  persp_b ? persp_b : "", sizeof(d->perspective_b) - 1);
            d->confidence_b = conf_b;

            float delta = conf_a - conf_b;
            if (delta < 0.0f) delta = -delta;
            d->conflict_score = 1.0f - delta;  /* 1.0 = identical confidence */

            char summary[512];
            snprintf(summary, sizeof(summary),
                     "%s: %s(%.2f) vs %s(%.2f) [conflict=%.2f]",
                     kind_a, persp_a, (double)conf_a,
                     persp_b, (double)conf_b, (double)d->conflict_score);
            d->summary = strdup(summary);
        } else {
            /* Only in A */
            d->conflict_score = 1.0f;
            char summary[512];
            snprintf(summary, sizeof(summary),
                     "%s: only in A (%s conf=%.2f)", kind_a, persp_a, (double)conf_a);
            d->summary = strdup(summary);
        }

        sqlite3_finalize(stmt_b);
        count++;
    }

    sqlite3_finalize(stmt_a);
    if (count_out) *count_out = count;
    return diffs;
}

void bf_fragment_diff_free(bf_fragment_diff_t *diffs, int count) {
    if (!diffs) return;
    for (int i = 0; i < count; i++) free(diffs[i].summary);
    free(diffs);
}

/* ─────────────────────────────────────────────────────────────────
 * Stats
 * ───────────────────────────────────────────────────────────────── */

int bf_fragment_stats(bf_fragment_store_t *store, bf_fragment_stats_t *out) {
    if (!store || !out) return -1;
    memset(out, 0, sizeof(*out));

    sqlite3_stmt *stmt;

    const char *sql =
        "SELECT COUNT(*), COUNT(DISTINCT kind), COUNT(DISTINCT perspective),"
        "       AVG(confidence), MIN(start_ms), MAX(end_ms)"
        " FROM fragments WHERE superseded = 0";

    if (sqlite3_prepare_v2(store->db, sql, -1, &stmt, NULL) != SQLITE_OK)
        return -1;

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        out->total_fragments      = sqlite3_column_int(stmt, 0);
        out->unique_kinds         = sqlite3_column_int(stmt, 1);
        out->unique_perspectives  = sqlite3_column_int(stmt, 2);
        out->mean_confidence      = (float)sqlite3_column_double(stmt, 3);
        out->earliest_ms          = sqlite3_column_int64(stmt, 4);
        out->latest_ms            = sqlite3_column_int64(stmt, 5);
    }

    sqlite3_finalize(stmt);
    return 0;
}
