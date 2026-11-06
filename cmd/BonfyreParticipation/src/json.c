#define _POSIX_C_SOURCE 200809L
#include "json.h"

#include <stdlib.h>
#include <string.h>

static char *query_text(sqlite3 *db, const char *sql, const char *json, const char *path) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    if (!db || !sql || !json || sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    if (path) sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        out = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return out;
}

char *bf_part_json_text(sqlite3 *db, const char *json, const char *path) {
    return query_text(db, "SELECT json_extract(?1,?2)", json, path);
}

char *bf_part_json_raw(sqlite3 *db, const char *json, const char *path) {
    return query_text(db, "SELECT json_extract(?1,?2)", json, path);
}

char *bf_part_json_quote(sqlite3 *db, const char *text) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    if (!db || sqlite3_prepare_v2(db, "SELECT json_quote(?1)", -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, text ? text : "", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        out = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return out;
}

int bf_part_json_int(sqlite3 *db, const char *json, const char *path, long long *out) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (!db || !json || !path || !out ||
        sqlite3_prepare_v2(db,
            "SELECT json_extract(?1,?2) WHERE json_type(?1,?2)='integer'", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) { *out = sqlite3_column_int64(st, 0); ok = 1; }
    sqlite3_finalize(st);
    return ok;
}

int bf_part_json_true(sqlite3 *db, const char *json, const char *path) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (!db || !json || !path ||
        sqlite3_prepare_v2(db, "SELECT json_type(?1,?2)='true'", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) ok = sqlite3_column_int(st, 0) == 1;
    sqlite3_finalize(st);
    return ok;
}

int bf_part_strings_has(const BfPartStrings *values, const char *value) {
    if (!values || !value) return 0;
    for (size_t i = 0; i < values->count; ++i) if (!strcmp(values->items[i], value)) return 1;
    return 0;
}

int bf_part_strings_add(BfPartStrings *values, const char *value) {
    char **next;
    if (!values || !value || !*value || bf_part_strings_has(values, value)) return 1;
    next = realloc(values->items, (values->count + 1) * sizeof(*next));
    if (!next) return 0;
    values->items = next;
    values->items[values->count] = strdup(value);
    if (!values->items[values->count]) return 0;
    values->count++;
    return 1;
}

static int string_cmp(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

void bf_part_strings_sort(BfPartStrings *values) {
    if (values && values->count > 1) qsort(values->items, values->count, sizeof(*values->items), string_cmp);
}

void bf_part_strings_free(BfPartStrings *values) {
    if (!values) return;
    for (size_t i = 0; i < values->count; ++i) free(values->items[i]);
    free(values->items);
    values->items = NULL;
    values->count = 0;
}

int bf_part_json_array(sqlite3 *db, const char *json, const char *path, BfPartStrings *out) {
    sqlite3_stmt *st = NULL;
    if (!db || !json || !path || !out) return 0;
    memset(out, 0, sizeof(*out));
    if (sqlite3_prepare_v2(db,
        "SELECT value FROM json_each(?1,?2) ORDER BY CAST(key AS INTEGER)", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *value = (const char *)sqlite3_column_text(st, 0);
        if (!value || !bf_part_strings_add(out, value)) { sqlite3_finalize(st); bf_part_strings_free(out); return 0; }
    }
    sqlite3_finalize(st);
    return 1;
}
