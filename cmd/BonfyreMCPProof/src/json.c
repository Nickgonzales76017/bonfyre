#include "proof.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

sqlite3 *bf_json_db(void) {
    sqlite3 *db = NULL;
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) return NULL;
    return db;
}

int bf_jtext(sqlite3 *db, const char *json, const char *path,
             char *out, size_t cap) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (!db || !json || !path || !out || !cap ||
        sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) != SQLITE_NULL) {
        const unsigned char *v = sqlite3_column_text(st, 0);
        snprintf(out, cap, "%s", v ? (const char *)v : "");
        found = 1;
    }
    sqlite3_finalize(st);
    return found;
}

int bf_jint(sqlite3 *db, const char *json, const char *path, long long *out) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) != SQLITE_NULL) {
        *out = sqlite3_column_int64(st, 0);
        found = 1;
    }
    sqlite3_finalize(st);
    return found;
}

int bf_jtype(sqlite3 *db, const char *json, const char *path,
             char *out, size_t cap) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (sqlite3_prepare_v2(db, "SELECT json_type(?1,?2)", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) {
        snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0));
        found = 1;
    }
    sqlite3_finalize(st);
    return found;
}

char *bf_jraw(sqlite3 *db, const char *json, const char *path) {
    sqlite3_stmt *st = NULL;
    char *copy = NULL;
    if (sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)", -1, &st, NULL) != SQLITE_OK)
        return NULL;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        copy = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return copy;
}

char *bf_array_object_by(sqlite3 *db, const char *json, const char *array_path,
                         const char *field_path, const char *value) {
    sqlite3_stmt *st = NULL;
    char *copy = NULL;
    const char *sql =
        "SELECT value FROM json_each(?1,?2) WHERE json_extract(value,?3)=?4 LIMIT 1";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, array_path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, field_path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, value, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        copy = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return copy;
}

int bf_array_has_two(sqlite3 *db, const char *json, const char *array_path,
                     const char *p1, const char *v1,
                     const char *p2, const char *v2) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    const char *sql =
        "SELECT 1 FROM json_each(?1,?2) WHERE json_extract(value,?3)=?4 "
        "AND json_extract(value,?5)=?6 LIMIT 1";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, array_path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, p1, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, v1, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 5, p2, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 6, v2, -1, SQLITE_TRANSIENT);
    found = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return found;
}

int bf_json_tree_equal(sqlite3 *db, const char *a, const char *b) {
    sqlite3_stmt *sa = NULL, *sb = NULL;
    const char *sql =
        "SELECT fullkey,type,COALESCE(CAST(atom AS TEXT),'') "
        "FROM json_tree(?1) ORDER BY fullkey,type,COALESCE(CAST(atom AS TEXT),'')";
    int equal = 1;
    if (sqlite3_prepare_v2(db, sql, -1, &sa, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, sql, -1, &sb, NULL) != SQLITE_OK) {
        equal = 0;
        goto done;
    }
    sqlite3_bind_text(sa, 1, a, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(sb, 1, b, -1, SQLITE_TRANSIENT);
    for (;;) {
        int ra = sqlite3_step(sa), rb = sqlite3_step(sb);
        if (ra != rb) { equal = 0; break; }
        if (ra == SQLITE_DONE) break;
        if (ra != SQLITE_ROW) { equal = 0; break; }
        for (int col = 0; col < 3; col++) {
            const char *va = (const char *)sqlite3_column_text(sa, col);
            const char *vb = (const char *)sqlite3_column_text(sb, col);
            if (strcmp(va ? va : "", vb ? vb : "")) {
                equal = 0;
                break;
            }
        }
        if (!equal) break;
    }
done:
    if (sa) sqlite3_finalize(sa);
    if (sb) sqlite3_finalize(sb);
    return equal;
}
