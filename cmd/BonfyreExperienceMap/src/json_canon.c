#include "json_canon.h"

#include <bonfyre.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} BfBuf;

static int buf_reserve(BfBuf *b, size_t extra) {
    size_t need = b->len + extra + 1;
    size_t cap;
    char *next;
    if (need <= b->cap) return 1;
    cap = b->cap ? b->cap : 256;
    while (cap < need) {
        if (cap > ((size_t)-1) / 2) return 0;
        cap *= 2;
    }
    next = (char *)realloc(b->data, cap);
    if (!next) return 0;
    b->data = next;
    b->cap = cap;
    return 1;
}

static int buf_putn(BfBuf *b, const char *s, size_t n) {
    if (!buf_reserve(b, n)) return 0;
    memcpy(b->data + b->len, s, n);
    b->len += n;
    b->data[b->len] = '\0';
    return 1;
}

static int buf_puts(BfBuf *b, const char *s) { return buf_putn(b, s, strlen(s)); }
static int buf_putc(BfBuf *b, char c) { return buf_putn(b, &c, 1); }

static char *json_quote(sqlite3 *db, const char *text) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    const unsigned char *v;
    if (sqlite3_prepare_v2(db, "SELECT json_quote(?1)", -1, &st, NULL) != SQLITE_OK)
        return NULL;
    sqlite3_bind_text(st, 1, text ? text : "", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && (v = sqlite3_column_text(st, 0)))
        out = strdup((const char *)v);
    sqlite3_finalize(st);
    return out;
}

static int append_scalar(sqlite3 *db, BfBuf *out, sqlite3_stmt *rows,
                         int value_col, int type_col) {
    const char *type = (const char *)sqlite3_column_text(rows, type_col);
    const unsigned char *v = sqlite3_column_text(rows, value_col);
    char num[128];
    char *quoted;
    if (!type) return 0;
    if (!strcmp(type, "null")) return buf_puts(out, "null");
    if (!strcmp(type, "true")) return buf_puts(out, "true");
    if (!strcmp(type, "false")) return buf_puts(out, "false");
    if (!strcmp(type, "text")) {
        quoted = json_quote(db, v ? (const char *)v : "");
        if (!quoted) return 0;
        if (!buf_puts(out, quoted)) { free(quoted); return 0; }
        free(quoted);
        return 1;
    }
    if (!strcmp(type, "integer")) {
        snprintf(num, sizeof(num), "%lld", sqlite3_column_int64(rows, value_col));
        return buf_puts(out, num);
    }
    if (!strcmp(type, "real")) {
        snprintf(num, sizeof(num), "%.17g", sqlite3_column_double(rows, value_col));
        return buf_puts(out, num);
    }
    return 0;
}

static int canonical_value(sqlite3 *db, const char *json, BfBuf *out) {
    sqlite3_stmt *type_st = NULL;
    sqlite3_stmt *rows = NULL;
    char *type = NULL;
    int first = 1;
    int ok = 0;

    if (sqlite3_prepare_v2(db, "SELECT json_type(?1,'$')", -1, &type_st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(type_st, 1, json, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(type_st) == SQLITE_ROW && sqlite3_column_text(type_st, 0))
        type = strdup((const char *)sqlite3_column_text(type_st, 0));
    sqlite3_finalize(type_st);
    if (!type) return 0;

    if (!strcmp(type, "object")) {
        if (!buf_putc(out, '{')) goto done;
        if (sqlite3_prepare_v2(db,
                "SELECT key,value,type FROM json_each(?1) ORDER BY key COLLATE BINARY",
                -1, &rows, NULL) != SQLITE_OK) goto done;
        sqlite3_bind_text(rows, 1, json, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(rows) == SQLITE_ROW) {
            const char *key = (const char *)sqlite3_column_text(rows, 0);
            const char *child_type = (const char *)sqlite3_column_text(rows, 2);
            const char *child = (const char *)sqlite3_column_text(rows, 1);
            char *quoted;
            if (!first && !buf_putc(out, ',')) goto done;
            first = 0;
            quoted = json_quote(db, key ? key : "");
            if (!quoted) goto done;
            if (!buf_puts(out, quoted) || !buf_putc(out, ':')) {
                free(quoted);
                goto done;
            }
            free(quoted);
            if (child_type && (!strcmp(child_type, "object") || !strcmp(child_type, "array"))) {
                if (!child || !canonical_value(db, child, out)) goto done;
            } else if (!append_scalar(db, out, rows, 1, 2)) goto done;
        }
        if (!buf_putc(out, '}')) goto done;
        ok = 1;
    } else if (!strcmp(type, "array")) {
        if (!buf_putc(out, '[')) goto done;
        if (sqlite3_prepare_v2(db,
                "SELECT value,type FROM json_each(?1) ORDER BY CAST(key AS INTEGER)",
                -1, &rows, NULL) != SQLITE_OK) goto done;
        sqlite3_bind_text(rows, 1, json, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(rows) == SQLITE_ROW) {
            const char *child_type = (const char *)sqlite3_column_text(rows, 1);
            const char *child = (const char *)sqlite3_column_text(rows, 0);
            if (!first && !buf_putc(out, ',')) goto done;
            first = 0;
            if (child_type && (!strcmp(child_type, "object") || !strcmp(child_type, "array"))) {
                if (!child || !canonical_value(db, child, out)) goto done;
            } else if (!append_scalar(db, out, rows, 0, 1)) goto done;
        }
        if (!buf_putc(out, ']')) goto done;
        ok = 1;
    } else {
        if (sqlite3_prepare_v2(db,
                "SELECT json_extract(?1,'$'),json_type(?1,'$')", -1, &rows, NULL) != SQLITE_OK)
            goto done;
        sqlite3_bind_text(rows, 1, json, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(rows) != SQLITE_ROW || !append_scalar(db, out, rows, 0, 1)) goto done;
        ok = 1;
    }

done:
    sqlite3_finalize(rows);
    free(type);
    return ok;
}

char *bf_experience_json_canonical(sqlite3 *db, const char *json) {
    BfBuf out = {0};
    if (!db || !json || !canonical_value(db, json, &out)) {
        free(out.data);
        return NULL;
    }
    if (!out.data) out.data = strdup("");
    return out.data;
}

int bf_experience_json_sha256(sqlite3 *db, const char *json, char out[65]) {
    char *canonical = bf_experience_json_canonical(db, json);
    if (!canonical) return 0;
    bf_sha256_hex((const uint8_t *)canonical, strlen(canonical), out);
    free(canonical);
    return 1;
}

int bf_experience_json_text(sqlite3 *db, const char *json, const char *path,
                            char *out, size_t out_size) {
    sqlite3_stmt *st = NULL;
    const unsigned char *v;
    int ok = 0;
    if (!db || !json || !path || !out || !out_size) return 0;
    out[0] = '\0';
    if (sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && (v = sqlite3_column_text(st, 0))) {
        snprintf(out, out_size, "%s", (const char *)v);
        ok = 1;
    }
    sqlite3_finalize(st);
    return ok;
}

int bf_experience_json_int(sqlite3 *db, const char *json, const char *path,
                           long long *out) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (!db || !json || !path || !out) return 0;
    if (sqlite3_prepare_v2(db,
            "SELECT json_extract(?1,?2) WHERE json_type(?1,?2)='integer'",
            -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        *out = sqlite3_column_int64(st, 0);
        ok = 1;
    }
    sqlite3_finalize(st);
    return ok;
}

int bf_experience_json_true(sqlite3 *db, const char *json, const char *path) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (!db || !json || !path) return 0;
    if (sqlite3_prepare_v2(db, "SELECT json_type(?1,?2)='true'", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) ok = sqlite3_column_int(st, 0) == 1;
    sqlite3_finalize(st);
    return ok;
}
