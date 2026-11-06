#define _POSIX_C_SOURCE 200809L
#include "json_ascii.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} Buf;

static int reserve(Buf *b, size_t extra) {
    size_t need = b->len + extra + 1, cap;
    char *next;
    if (need <= b->cap) return 1;
    cap = b->cap ? b->cap : 256;
    while (cap < need) cap *= 2;
    next = realloc(b->data, cap);
    if (!next) return 0;
    b->data = next; b->cap = cap;
    return 1;
}
static int putn(Buf *b, const char *s, size_t n) {
    if (!reserve(b, n)) return 0;
    memcpy(b->data + b->len, s, n); b->len += n; b->data[b->len] = '\0'; return 1;
}
static int putsb(Buf *b, const char *s) { return putn(b, s, strlen(s)); }
static int putc1(Buf *b, char c) { return putn(b, &c, 1); }

static int utf8_next(const unsigned char **pp, unsigned long *cp) {
    const unsigned char *p = *pp;
    if (*p < 0x80) { *cp = *p; *pp = p + 1; return 1; }
    if ((*p & 0xe0) == 0xc0 && (p[1] & 0xc0) == 0x80) {
        *cp = ((unsigned long)(p[0] & 0x1f) << 6) | (p[1] & 0x3f); *pp = p + 2; return *cp >= 0x80;
    }
    if ((*p & 0xf0) == 0xe0 && (p[1] & 0xc0) == 0x80 && (p[2] & 0xc0) == 0x80) {
        *cp = ((unsigned long)(p[0] & 0x0f) << 12) | ((unsigned long)(p[1] & 0x3f) << 6) | (p[2] & 0x3f);
        *pp = p + 3; return *cp >= 0x800 && !(*cp >= 0xd800 && *cp <= 0xdfff);
    }
    if ((*p & 0xf8) == 0xf0 && (p[1] & 0xc0) == 0x80 && (p[2] & 0xc0) == 0x80 && (p[3] & 0xc0) == 0x80) {
        *cp = ((unsigned long)(p[0] & 0x07) << 18) | ((unsigned long)(p[1] & 0x3f) << 12) |
              ((unsigned long)(p[2] & 0x3f) << 6) | (p[3] & 0x3f);
        *pp = p + 4; return *cp >= 0x10000 && *cp <= 0x10ffff;
    }
    return 0;
}
static int quote(Buf *b, const char *text) {
    const unsigned char *p = (const unsigned char *)(text ? text : "");
    char esc[16];
    if (!putc1(b, '"')) return 0;
    while (*p) {
        unsigned long cp;
        if (!utf8_next(&p, &cp)) return 0;
        if (cp == '"') { if (!putsb(b, "\\\"")) return 0; }
        else if (cp == '\\') { if (!putsb(b, "\\\\")) return 0; }
        else if (cp == '\b') { if (!putsb(b, "\\b")) return 0; }
        else if (cp == '\f') { if (!putsb(b, "\\f")) return 0; }
        else if (cp == '\n') { if (!putsb(b, "\\n")) return 0; }
        else if (cp == '\r') { if (!putsb(b, "\\r")) return 0; }
        else if (cp == '\t') { if (!putsb(b, "\\t")) return 0; }
        else if (cp < 0x20 || cp >= 0x80) {
            if (cp <= 0xffff) {
                snprintf(esc, sizeof(esc), "\\u%04lx", cp);
                if (!putsb(b, esc)) return 0;
            } else {
                unsigned long v = cp - 0x10000;
                unsigned long hi = 0xd800 + (v >> 10), lo = 0xdc00 + (v & 0x3ff);
                snprintf(esc, sizeof(esc), "\\u%04lx\\u%04lx", hi, lo);
                if (!putsb(b, esc)) return 0;
            }
        } else if (!putc1(b, (char)cp)) return 0;
    }
    return putc1(b, '"');
}

static int canonical(sqlite3 *db, const char *json, Buf *out);

static int scalar(Buf *out, sqlite3_stmt *rows, int value_col, int type_col) {
    const char *type = (const char *)sqlite3_column_text(rows, type_col);
    const char *v = (const char *)sqlite3_column_text(rows, value_col);
    char num[128];
    if (!type) return 0;
    if (!strcmp(type, "null")) return putsb(out, "null");
    if (!strcmp(type, "true")) return putsb(out, "true");
    if (!strcmp(type, "false")) return putsb(out, "false");
    if (!strcmp(type, "text")) return quote(out, v ? v : "");
    if (!strcmp(type, "integer")) {
        snprintf(num, sizeof(num), "%lld", sqlite3_column_int64(rows, value_col));
        return putsb(out, num);
    }
    if (!strcmp(type, "real")) {
        snprintf(num, sizeof(num), "%.17g", sqlite3_column_double(rows, value_col));
        return putsb(out, num);
    }
    return 0;
}

static int canonical(sqlite3 *db, const char *json, Buf *out) {
    sqlite3_stmt *type_st = NULL, *rows = NULL;
    char *type = NULL;
    int first = 1, ok = 0;
    if (sqlite3_prepare_v2(db, "SELECT json_type(?1,'$')", -1, &type_st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(type_st, 1, json, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(type_st) == SQLITE_ROW && sqlite3_column_text(type_st, 0))
        type = strdup((const char *)sqlite3_column_text(type_st, 0));
    sqlite3_finalize(type_st);
    if (!type) return 0;

    if (!strcmp(type, "object")) {
        if (!putc1(out, '{') ||
            sqlite3_prepare_v2(db, "SELECT key,value,type FROM json_each(?1) ORDER BY key COLLATE BINARY",
                               -1, &rows, NULL) != SQLITE_OK) goto done;
        sqlite3_bind_text(rows, 1, json, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(rows) == SQLITE_ROW) {
            const char *key = (const char *)sqlite3_column_text(rows, 0);
            const char *child = (const char *)sqlite3_column_text(rows, 1);
            const char *child_type = (const char *)sqlite3_column_text(rows, 2);
            if ((!first && !putc1(out, ',')) || !quote(out, key ? key : "") || !putc1(out, ':')) goto done;
            first = 0;
            if (child_type && (!strcmp(child_type, "object") || !strcmp(child_type, "array"))) {
                if (!child || !canonical(db, child, out)) goto done;
            } else if (!scalar(out, rows, 1, 2)) goto done;
        }
        ok = putc1(out, '}');
    } else if (!strcmp(type, "array")) {
        if (!putc1(out, '[') ||
            sqlite3_prepare_v2(db, "SELECT value,type FROM json_each(?1) ORDER BY CAST(key AS INTEGER)",
                               -1, &rows, NULL) != SQLITE_OK) goto done;
        sqlite3_bind_text(rows, 1, json, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(rows) == SQLITE_ROW) {
            const char *child = (const char *)sqlite3_column_text(rows, 0);
            const char *child_type = (const char *)sqlite3_column_text(rows, 1);
            if (!first && !putc1(out, ',')) goto done;
            first = 0;
            if (child_type && (!strcmp(child_type, "object") || !strcmp(child_type, "array"))) {
                if (!child || !canonical(db, child, out)) goto done;
            } else if (!scalar(out, rows, 0, 1)) goto done;
        }
        ok = putc1(out, ']');
    } else {
        if (sqlite3_prepare_v2(db, "SELECT json_extract(?1,'$'),json_type(?1,'$')",
                               -1, &rows, NULL) != SQLITE_OK) goto done;
        sqlite3_bind_text(rows, 1, json, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(rows) == SQLITE_ROW) ok = scalar(out, rows, 0, 1);
    }

done:
    sqlite3_finalize(rows);
    free(type);
    return ok;
}

char *bf_stage_json_ascii_canonical(sqlite3 *db, const char *json) {
    Buf out = {0};
    if (!db || !json || !canonical(db, json, &out)) {
        free(out.data);
        return NULL;
    }
    if (!out.data) out.data = strdup("");
    return out.data;
}
