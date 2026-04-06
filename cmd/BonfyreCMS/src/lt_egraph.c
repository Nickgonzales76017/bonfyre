/*
 * BonfyreGraph — Flagship III: LT-EGraph Unified Framework
 *
 * E-graph equivalence classes + Lambda Tensors structural compaction.
 * See: Research - Flagship Papers (LT-EGraph Synthesis)
 */

#include "lt_egraph.h"
#include "canonical.h"
#include "ephemeral.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Declared in main.c */
extern unsigned long long fnv1a64(const void *data, size_t len);
extern void iso_timestamp(char *buf, size_t sz);
extern int db_exec(sqlite3 *db, const char *sql);

/* ================================================================
 * Bootstrap
 * ================================================================ */

int egraph_bootstrap(sqlite3 *db) {
    const char *stmts[] = {
        "CREATE TABLE IF NOT EXISTS _equivalences ("
        "  id            INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  class_id      INTEGER NOT NULL,"
        "  repr_type     TEXT NOT NULL,"
        "  repr_hash     TEXT NOT NULL,"
        "  target_type   TEXT NOT NULL,"
        "  target_id     INTEGER NOT NULL,"
        "  materialized  INTEGER DEFAULT 0,"
        "  cost          REAL DEFAULT 1.0,"
        "  data          TEXT,"
        "  ts            TEXT NOT NULL"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_eq_class ON _equivalences(class_id);",
        "CREATE INDEX IF NOT EXISTS idx_eq_target ON _equivalences(target_type, target_id);",
        "CREATE INDEX IF NOT EXISTS idx_eq_repr ON _equivalences(target_type, target_id, repr_type);",
        "CREATE INDEX IF NOT EXISTS idx_eq_mat ON _equivalences(materialized, cost);",

        /* Class counter table */
        "CREATE TABLE IF NOT EXISTS _egraph_meta ("
        "  key   TEXT PRIMARY KEY,"
        "  value TEXT NOT NULL"
        ");",
        "INSERT OR IGNORE INTO _egraph_meta (key, value) VALUES ('next_class_id', '1');",

        NULL
    };
    for (int i = 0; stmts[i]; i++) {
        if (db_exec(db, stmts[i]) != 0) return -1;
    }
    return 0;
}

/* ================================================================
 * Class ID management
 * ================================================================ */

int egraph_new_class(sqlite3 *db) {
    sqlite3_stmt *stmt;
    int class_id = -1;

    /* Get and increment the counter atomically */
    if (sqlite3_prepare_v2(db,
        "UPDATE _egraph_meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) "
        "WHERE key='next_class_id' RETURNING CAST(value AS INTEGER) - 1",
        -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            class_id = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    return class_id;
}

int egraph_class_id(sqlite3 *db, const char *target_type, int target_id) {
    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT class_id FROM _equivalences "
        "WHERE target_type=?1 AND target_id=?2 LIMIT 1";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;

    sqlite3_bind_text(stmt, 1, target_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, target_id);

    int cid = -1;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        cid = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return cid;
}

static int egraph_class_id_with_stmt(sqlite3_stmt *stmt, const char *target_type, int target_id) {
    int cid = -1;
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
    sqlite3_bind_text(stmt, 1, target_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, target_id);
    if (sqlite3_step(stmt) == SQLITE_ROW) cid = sqlite3_column_int(stmt, 0);
    return cid;
}

/* ================================================================
 * Insert representation
 * ================================================================ */

int egraph_insert(sqlite3 *db, int class_id, const char *repr_type,
                  const char *repr_hash, const char *target_type,
                  int target_id, double cost, int materialized,
                  const char *data)
{
    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    /* Check if this exact repr already exists */
    sqlite3_stmt *chk;
    const char *chk_sql =
        "SELECT id FROM _equivalences "
        "WHERE target_type=?1 AND target_id=?2 AND repr_type=?3";
    if (sqlite3_prepare_v2(db, chk_sql, -1, &chk, NULL) == SQLITE_OK) {
        sqlite3_bind_text(chk, 1, target_type, -1, SQLITE_STATIC);
        sqlite3_bind_int(chk, 2, target_id);
        sqlite3_bind_text(chk, 3, repr_type, -1, SQLITE_STATIC);

        if (sqlite3_step(chk) == SQLITE_ROW) {
            /* Update existing */
            int row_id = sqlite3_column_int(chk, 0);
            sqlite3_finalize(chk);

            sqlite3_stmt *upd;
            const char *upd_sql =
                "UPDATE _equivalences SET repr_hash=?1, cost=?2, "
                "materialized=?3, data=?4, ts=?5, class_id=?6 WHERE id=?7";
            if (sqlite3_prepare_v2(db, upd_sql, -1, &upd, NULL) != SQLITE_OK)
                return -1;
            sqlite3_bind_text(upd, 1, repr_hash, -1, SQLITE_STATIC);
            sqlite3_bind_double(upd, 2, cost);
            sqlite3_bind_int(upd, 3, materialized);
            sqlite3_bind_text(upd, 4, data, -1, SQLITE_STATIC);
            sqlite3_bind_text(upd, 5, ts, -1, SQLITE_STATIC);
            sqlite3_bind_int(upd, 6, class_id);
            sqlite3_bind_int(upd, 7, row_id);
            int rc = sqlite3_step(upd);
            sqlite3_finalize(upd);
            return rc == SQLITE_DONE ? 0 : -1;
        }
        sqlite3_finalize(chk);
    }

    /* Insert new */
    sqlite3_stmt *stmt;
    const char *sql =
        "INSERT INTO _equivalences "
        "(class_id, repr_type, repr_hash, target_type, target_id, "
        " materialized, cost, data, ts) "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[egraph] insert: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    sqlite3_bind_int(stmt, 1, class_id);
    sqlite3_bind_text(stmt, 2, repr_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, repr_hash, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, target_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 5, target_id);
    sqlite3_bind_int(stmt, 6, materialized);
    sqlite3_bind_double(stmt, 7, cost);
    sqlite3_bind_text(stmt, 8, data, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 9, ts, -1, SQLITE_STATIC);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

static int egraph_insert_with_stmts(sqlite3_stmt *chk, sqlite3_stmt *upd, sqlite3_stmt *ins,
                                    int class_id, const char *repr_type,
                                    const char *repr_hash, const char *target_type,
                                    int target_id, double cost, int materialized,
                                    const char *data) {
    char ts[64];
    int row_id = -1;
    iso_timestamp(ts, sizeof(ts));

    sqlite3_reset(chk);
    sqlite3_clear_bindings(chk);
    sqlite3_bind_text(chk, 1, target_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(chk, 2, target_id);
    sqlite3_bind_text(chk, 3, repr_type, -1, SQLITE_STATIC);
    if (sqlite3_step(chk) == SQLITE_ROW) row_id = sqlite3_column_int(chk, 0);

    if (row_id >= 0) {
        sqlite3_reset(upd);
        sqlite3_clear_bindings(upd);
        sqlite3_bind_text(upd, 1, repr_hash, -1, SQLITE_STATIC);
        sqlite3_bind_double(upd, 2, cost);
        sqlite3_bind_int(upd, 3, materialized);
        sqlite3_bind_text(upd, 4, data, -1, SQLITE_STATIC);
        sqlite3_bind_text(upd, 5, ts, -1, SQLITE_STATIC);
        sqlite3_bind_int(upd, 6, class_id);
        sqlite3_bind_int(upd, 7, row_id);
        return sqlite3_step(upd) == SQLITE_DONE ? 0 : -1;
    }

    sqlite3_reset(ins);
    sqlite3_clear_bindings(ins);
    sqlite3_bind_int(ins, 1, class_id);
    sqlite3_bind_text(ins, 2, repr_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 3, repr_hash, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 4, target_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(ins, 5, target_id);
    sqlite3_bind_int(ins, 6, materialized);
    sqlite3_bind_double(ins, 7, cost);
    sqlite3_bind_text(ins, 8, data, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 9, ts, -1, SQLITE_STATIC);
    return sqlite3_step(ins) == SQLITE_DONE ? 0 : -1;
}

/* ================================================================
 * Extract — cost-based retrieval
 * ================================================================ */

int egraph_extract(sqlite3 *db, const char *target_type, int target_id,
                   const char *preferred_type, char **out_data)
{
    *out_data = NULL;
    sqlite3_stmt *stmt;

    /* Strategy:
       1. Try preferred_type if materialized
       2. Try any materialized representation (cheapest)
       3. Fall back to op log reconstruction */

    if (preferred_type) {
        const char *sql =
            "SELECT data FROM _equivalences "
            "WHERE target_type=?1 AND target_id=?2 AND repr_type=?3 "
            "AND materialized=1 LIMIT 1";
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, target_type, -1, SQLITE_STATIC);
            sqlite3_bind_int(stmt, 2, target_id);
            sqlite3_bind_text(stmt, 3, preferred_type, -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                const char *d = (const char *)sqlite3_column_text(stmt, 0);
                if (d) *out_data = strdup(d);
                sqlite3_finalize(stmt);
                return 0;
            }
            sqlite3_finalize(stmt);
        }
    }

    /* Try cheapest materialized */
    const char *any_sql =
        "SELECT data, repr_type FROM _equivalences "
        "WHERE target_type=?1 AND target_id=?2 AND materialized=1 "
        "ORDER BY cost ASC LIMIT 1";
    if (sqlite3_prepare_v2(db, any_sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, target_type, -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 2, target_id);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *d = (const char *)sqlite3_column_text(stmt, 0);
            if (d) *out_data = strdup(d);
            sqlite3_finalize(stmt);
            return 0;
        }
        sqlite3_finalize(stmt);
    }

    /* Fallback: reconstruct from op log (Flagship I) */
    char *reconstructed = NULL;
    if (ops_reconstruct(db, target_type, (long long)target_id, &reconstructed) == 0) {
        *out_data = reconstructed;

        /* Cache it as a row representation for next time */
        int cid = egraph_class_id(db, target_type, target_id);
        if (cid >= 0) {
            char hash[17];
            canonical_l1_hash(reconstructed, hash);
            egraph_insert(db, cid, REPR_ROW, hash, target_type, target_id,
                          1.0, 1, reconstructed);
        }
        return 0;
    }

    return -1;
}

/* ================================================================
 * List representations
 * ================================================================ */

int egraph_list_reprs(sqlite3 *db, const char *target_type, int target_id,
                      char **out_json)
{
    *out_json = NULL;
    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT class_id, repr_type, repr_hash, materialized, cost, ts "
        "FROM _equivalences "
        "WHERE target_type=?1 AND target_id=?2 ORDER BY cost ASC";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return 0;

    sqlite3_bind_text(stmt, 1, target_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, target_id);

    size_t cap = 2048;
    char *buf = malloc(cap);
    if (!buf) { sqlite3_finalize(stmt); return 0; }
    size_t off = 0;
    buf[off++] = '[';
    int count = 0;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int cid = sqlite3_column_int(stmt, 0);
        const char *rtype = (const char *)sqlite3_column_text(stmt, 1);
        const char *rhash = (const char *)sqlite3_column_text(stmt, 2);
        int mat = sqlite3_column_int(stmt, 3);
        double cost = sqlite3_column_double(stmt, 4);
        const char *ts = (const char *)sqlite3_column_text(stmt, 5);

        while (off + 512 >= cap) { cap *= 2; buf = realloc(buf, cap); }
        if (count > 0) buf[off++] = ',';

        off += (size_t)snprintf(buf + off, cap - off,
            "{\"class_id\":%d,\"type\":\"%s\",\"hash\":\"%s\","
            "\"materialized\":%s,\"cost\":%.2f,\"ts\":\"%s\"}",
            cid, rtype ? rtype : "", rhash ? rhash : "",
            mat ? "true" : "false", cost, ts ? ts : "");
        count++;
    }
    sqlite3_finalize(stmt);

    buf[off++] = ']';
    buf[off] = '\0';
    *out_json = buf;
    return count;
}

/* ================================================================
 * Invalidate — mark all reprs for an entity as stale
 * ================================================================ */

int egraph_invalidate(sqlite3 *db, const char *target_type, int target_id) {
    sqlite3_stmt *stmt;
    const char *sql =
        "UPDATE _equivalences SET materialized=0 "
        "WHERE target_type=?1 AND target_id=?2";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, target_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, target_id);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

/* ================================================================
 * Lambda Tensors Compaction
 *
 * For a content type:
 * 1. Run family_discover() to group entries by structural hash
 * 2. For each family with >1 members:
 *    a. Store generator once
 *    b. Store bindings per member
 *    c. Create tensor representation in e-graph for each member
 *    d. Set tensor cost = 1/family_size (amortized)
 * ================================================================ */

int lt_compact(sqlite3 *db, const char *content_type) {
    /* Step 1: discover families (Flagship II) */
    int families_found = family_discover(db, content_type);

    if (families_found <= 0) return 0;

    /* Step 2: for each family, create tensor representations */
    sqlite3_stmt *fam_stmt;
    const char *fam_sql =
        "SELECT id, family_hash, generator, member_count "
        "FROM _families WHERE content_type=?1 AND member_count > 0";
    if (sqlite3_prepare_v2(db, fam_sql, -1, &fam_stmt, NULL) != SQLITE_OK)
        return -1;
    sqlite3_bind_text(fam_stmt, 1, content_type, -1, SQLITE_STATIC);

    int compacted = 0;
    sqlite3_stmt *mem_stmt = NULL;
    sqlite3_stmt *class_stmt = NULL;
    sqlite3_stmt *chk_stmt = NULL;
    sqlite3_stmt *upd_stmt = NULL;
    sqlite3_stmt *ins_stmt = NULL;

    if (sqlite3_prepare_v2(db,
        "SELECT target_id, bindings FROM _family_members WHERE family_id=?1 AND target_type=?2",
        -1, &mem_stmt, NULL) != SQLITE_OK) {
        sqlite3_finalize(fam_stmt);
        return -1;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT class_id FROM _equivalences WHERE target_type=?1 AND target_id=?2 LIMIT 1",
        -1, &class_stmt, NULL) != SQLITE_OK) {
        sqlite3_finalize(mem_stmt);
        sqlite3_finalize(fam_stmt);
        return -1;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT id FROM _equivalences WHERE target_type=?1 AND target_id=?2 AND repr_type=?3",
        -1, &chk_stmt, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db,
        "UPDATE _equivalences SET repr_hash=?1, cost=?2, materialized=?3, data=?4, ts=?5, class_id=?6 WHERE id=?7",
        -1, &upd_stmt, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db,
        "INSERT INTO _equivalences (class_id, repr_type, repr_hash, target_type, target_id,  materialized, cost, data, ts) "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        -1, &ins_stmt, NULL) != SQLITE_OK) {
        sqlite3_finalize(ins_stmt);
        sqlite3_finalize(upd_stmt);
        sqlite3_finalize(chk_stmt);
        sqlite3_finalize(class_stmt);
        sqlite3_finalize(mem_stmt);
        sqlite3_finalize(fam_stmt);
        return -1;
    }

    while (sqlite3_step(fam_stmt) == SQLITE_ROW) {
        int fam_id = sqlite3_column_int(fam_stmt, 0);
        const char *fhash = (const char *)sqlite3_column_text(fam_stmt, 1);
        const char *generator = (const char *)sqlite3_column_text(fam_stmt, 2);
        int member_count = sqlite3_column_int(fam_stmt, 3);

        double tensor_cost = (member_count > 0) ? 1.0 / member_count : 1.0;

        /* For each member, create tensor representation */
        sqlite3_reset(mem_stmt);
        sqlite3_clear_bindings(mem_stmt);
        sqlite3_bind_int(mem_stmt, 1, fam_id);
        sqlite3_bind_text(mem_stmt, 2, content_type, -1, SQLITE_STATIC);

        while (sqlite3_step(mem_stmt) == SQLITE_ROW) {
            int target_id = sqlite3_column_int(mem_stmt, 0);
            const char *bindings = (const char *)sqlite3_column_text(mem_stmt, 1);

            /* Build tensor data: {"generator": ..., "bindings": ..., "family": ...} */
            size_t gen_len = generator ? strlen(generator) : 2;
            size_t bind_len = bindings ? strlen(bindings) : 2;
            size_t data_cap = gen_len + bind_len + 256;
            char *tensor_data = malloc(data_cap);
            if (!tensor_data) continue;

            snprintf(tensor_data, data_cap,
                "{\"family\":\"%s\",\"generator\":%s,\"bindings\":%s,\"members\":%d}",
                fhash ? fhash : "",
                generator ? generator : "{}",
                bindings ? bindings : "[]",
                member_count);

            /* Hash the tensor data */
            char thash[17];
            unsigned long long h = fnv1a64(tensor_data, strlen(tensor_data));
            snprintf(thash, 17, "%016llx", h);

            /* Get or create e-class for this entity */
            int class_id = egraph_class_id_with_stmt(class_stmt, content_type, target_id);
            if (class_id < 0) {
                class_id = egraph_new_class(db);
            }

            /* Insert tensor repr */
            egraph_insert_with_stmts(chk_stmt, upd_stmt, ins_stmt,
                                     class_id, REPR_TENSOR, thash,
                                     content_type, target_id,
                                     tensor_cost, 1, tensor_data);

            free(tensor_data);
        }
        compacted++;
    }
    sqlite3_finalize(ins_stmt);
    sqlite3_finalize(upd_stmt);
    sqlite3_finalize(chk_stmt);
    sqlite3_finalize(class_stmt);
    sqlite3_finalize(mem_stmt);
    sqlite3_finalize(fam_stmt);

    return compacted;
}

/* ================================================================
 * Compaction statistics
 * ================================================================ */

int lt_compact_stats(sqlite3 *db, const char *content_type,
                     int *family_count, int *member_count,
                     int *repr_count)
{
    *family_count = 0;
    *member_count = 0;
    *repr_count = 0;

    sqlite3_stmt *stmt;

    /* Family count */
    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM _families WHERE content_type=?1",
        -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, content_type, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW)
            *family_count = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }

    /* Member count */
    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM _family_members WHERE target_type=?1",
        -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, content_type, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW)
            *member_count = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }

    /* Repr count */
    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM _equivalences WHERE target_type=?1",
        -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, content_type, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW)
            *repr_count = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }

    return 0;
}
