/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * BonfyreGraph — Flagship II: Canonical Functional Normalization
 *
 * Three-level canonicalization + structural family discovery.
 * See: Research - Flagship Papers (Canonical Functional Normalization)
 */

#include "canonical.h"
#include "incremental.h"
#include "hybrid_reduce.h"
#include "ann_index.h"
#include "compact_bindings.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* Declared in main.c */
extern unsigned long long fnv1a64(const void *data, size_t len);
extern int db_exec(sqlite3 *db, const char *sql);

/* ================================================================
 * JSON key-value parser (flat + one level nesting)
 * ================================================================ */

#define CAN_MAX_KEYS 128
#define CAN_MAX_KEY  256
#define CAN_MAX_VAL  8192

typedef struct {
    char key[CAN_MAX_KEY];
    char raw_val[CAN_MAX_VAL]; /* value as-is from JSON */
    int  val_type;             /* 0=string, 1=number, 2=bool, 3=null, 4=object, 5=array */
} CanKV;

/* Compare function for qsort — sort by key name */
static int kv_cmp(const void *a, const void *b) {
    return strcmp(((const CanKV *)a)->key, ((const CanKV *)b)->key);
}

static int append_char(char **buf, size_t *len, size_t *cap, char ch) {
    if (*len + 2 > *cap) {
        size_t next = *cap ? *cap * 2 : 4096;
        char *tmp = realloc(*buf, next);
        if (!tmp) return -1;
        *buf = tmp;
        *cap = next;
    }
    (*buf)[(*len)++] = ch;
    (*buf)[*len] = '\0';
    return 0;
}

static int append_bytes(char **buf, size_t *len, size_t *cap, const char *src, size_t src_len) {
    if (*len + src_len + 1 > *cap) {
        size_t next = *cap ? *cap : 4096;
        while (*len + src_len + 1 > next) next *= 2;
        char *tmp = realloc(*buf, next);
        if (!tmp) return -1;
        *buf = tmp;
        *cap = next;
    }
    memcpy(*buf + *len, src, src_len);
    *len += src_len;
    (*buf)[*len] = '\0';
    return 0;
}

static int append_cstr(char **buf, size_t *len, size_t *cap, const char *src) {
    return append_bytes(buf, len, cap, src, strlen(src));
}

static int append_json_escaped(char **buf, size_t *len, size_t *cap, const char *src) {
    const unsigned char *p = (const unsigned char *)src;
    while (*p) {
        switch (*p) {
            case '\\':
                if (append_cstr(buf, len, cap, "\\\\") != 0) return -1;
                break;
            case '"':
                if (append_cstr(buf, len, cap, "\\\"") != 0) return -1;
                break;
            case '\n':
                if (append_cstr(buf, len, cap, "\\n") != 0) return -1;
                break;
            case '\r':
                if (append_cstr(buf, len, cap, "\\r") != 0) return -1;
                break;
            case '\t':
                if (append_cstr(buf, len, cap, "\\t") != 0) return -1;
                break;
            default:
                if (append_char(buf, len, cap, (char)*p) != 0) return -1;
                break;
        }
        p++;
    }
    return 0;
}

static char *build_row_json_from_stmt(sqlite3_stmt *stmt) {
    int col_count = sqlite3_column_count(stmt);
    size_t cap = 4096;
    size_t len = 0;
    char *json = malloc(cap);
    if (!json) return NULL;
    json[0] = '\0';

    if (append_char(&json, &len, &cap, '{') != 0) { free(json); return NULL; }

    int emitted = 0;
    for (int c = 0; c < col_count; c++) {
        const char *col_name = sqlite3_column_name(stmt, c);
        if (!col_name || strcmp(col_name, "id") == 0) continue;

        if (emitted++ > 0 && append_char(&json, &len, &cap, ',') != 0) {
            free(json);
            return NULL;
        }
        if (append_char(&json, &len, &cap, '"') != 0 ||
            append_json_escaped(&json, &len, &cap, col_name) != 0 ||
            append_cstr(&json, &len, &cap, "\":") != 0) {
            free(json);
            return NULL;
        }

        switch (sqlite3_column_type(stmt, c)) {
            case SQLITE_INTEGER: {
                char tmp[64];
                snprintf(tmp, sizeof(tmp), "%lld", sqlite3_column_int64(stmt, c));
                if (append_cstr(&json, &len, &cap, tmp) != 0) { free(json); return NULL; }
                break;
            }
            case SQLITE_FLOAT: {
                char tmp[64];
                snprintf(tmp, sizeof(tmp), "%.6f", sqlite3_column_double(stmt, c));
                if (append_cstr(&json, &len, &cap, tmp) != 0) { free(json); return NULL; }
                break;
            }
            case SQLITE_TEXT: {
                const char *text = (const char *)sqlite3_column_text(stmt, c);
                if (!text) {
                    if (append_cstr(&json, &len, &cap, "null") != 0) { free(json); return NULL; }
                    break;
                }
                if (append_char(&json, &len, &cap, '"') != 0 ||
                    append_json_escaped(&json, &len, &cap, text) != 0 ||
                    append_char(&json, &len, &cap, '"') != 0) {
                    free(json);
                    return NULL;
                }
                break;
            }
            case SQLITE_NULL:
            default:
                if (append_cstr(&json, &len, &cap, "null") != 0) { free(json); return NULL; }
                break;
        }
    }

    if (append_char(&json, &len, &cap, '}') != 0) {
        free(json);
        return NULL;
    }
    return json;
}

static int family_remove_entry_internal(sqlite3 *db, const char *content_type, int target_id,
                                        int *old_family_id_out) {
    sqlite3_stmt *stmt = NULL;
    int old_family_id = 0;
    char *old_bindings = NULL;

    /* Fetch old family_id AND bindings before removal (for incremental stats) */
    if (sqlite3_prepare_v2(db,
        "SELECT family_id, bindings FROM _family_members WHERE target_type=?1 AND target_id=?2",
        -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, target_id);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        old_family_id = sqlite3_column_int(stmt, 0);
        const char *b = (const char *)sqlite3_column_text(stmt, 1);
        if (b) old_bindings = strdup(b);
    }
    sqlite3_finalize(stmt);

    if (old_family_id_out) *old_family_id_out = old_family_id;
    if (old_family_id == 0) { free(old_bindings); return 0; }

    /* Incrementally de-accumulate eigenvalue stats before membership removal */
    if (old_bindings) {
        incr_remove_member(db, old_family_id, old_bindings);
        free(old_bindings);
    }

    /* Invalidate memoized reductions for this family */
    {
        char fid_str[32];
        snprintf(fid_str, sizeof(fid_str), "%d", old_family_id);
        hybrid_memo_invalidate(db, fid_str);
    }

    /* Remove from ANN index before membership deletion */
    ann_remove_member(db, old_family_id, target_id);

    if (sqlite3_prepare_v2(db,
        "DELETE FROM _family_members WHERE target_type=?1 AND target_id=?2",
        -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, target_id);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (sqlite3_prepare_v2(db,
        "UPDATE _families SET member_count = CASE WHEN member_count > 0 THEN member_count - 1 ELSE 0 END WHERE id=?1",
        -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_int(stmt, 1, old_family_id);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (sqlite3_prepare_v2(db,
        "DELETE FROM _families WHERE id=?1 AND member_count <= 0",
        -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_int(stmt, 1, old_family_id);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return 0;
}

static int family_upsert_membership_with_stmts(sqlite3_stmt *upsert_family,
                                               sqlite3_stmt *select_family_id,
                                               sqlite3_stmt *upsert_member,
                                               const char *content_type,
                                               int target_id,
                                               const char *fhash,
                                               const char *gen,
                                               const char *bind,
                                               const char *ts,
                                               int *family_row_id_out) {
    int family_row_id = 0;

    sqlite3_reset(upsert_family);
    sqlite3_clear_bindings(upsert_family);
    sqlite3_bind_text(upsert_family, 1, fhash, -1, SQLITE_STATIC);
    sqlite3_bind_text(upsert_family, 2, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(upsert_family, 3, gen, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(upsert_family, 4, ts, -1, SQLITE_STATIC);
    if (sqlite3_step(upsert_family) != SQLITE_DONE) return -1;

    sqlite3_reset(select_family_id);
    sqlite3_clear_bindings(select_family_id);
    sqlite3_bind_text(select_family_id, 1, fhash, -1, SQLITE_STATIC);
    sqlite3_bind_text(select_family_id, 2, content_type, -1, SQLITE_STATIC);
    if (sqlite3_step(select_family_id) == SQLITE_ROW) {
        family_row_id = sqlite3_column_int(select_family_id, 0);
    }
    if (family_row_id == 0) return -1;

    sqlite3_reset(upsert_member);
    sqlite3_clear_bindings(upsert_member);
    sqlite3_bind_int(upsert_member, 1, family_row_id);
    sqlite3_bind_text(upsert_member, 2, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(upsert_member, 3, target_id);
    sqlite3_bind_text(upsert_member, 4, bind, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(upsert_member) != SQLITE_DONE) return -1;

    if (family_row_id_out) *family_row_id_out = family_row_id;
    return 0;
}

/* Parse flat JSON into key-value array. Returns count. */
static int parse_flat_json(const char *json, CanKV *kvs, int max_kvs) {
    int count = 0;
    const char *p = json;
    while (*p && *p != '{') p++;
    if (*p == '{') p++;

    while (*p && *p != '}' && count < max_kvs) {
        while (*p && (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t' || *p == ',')) p++;
        if (*p == '}' || !*p) break;

        /* key */
        if (*p != '"') { p++; continue; }
        p++;
        size_t ki = 0;
        while (*p && *p != '"' && ki < CAN_MAX_KEY - 1) {
            if (*p == '\\' && *(p+1)) { kvs[count].key[ki++] = *(p+1); p += 2; continue; }
            kvs[count].key[ki++] = *p++;
        }
        kvs[count].key[ki] = '\0';
        if (*p == '"') p++;

        /* skip colon */
        while (*p && *p != ':') p++;
        if (*p == ':') p++;
        while (*p && (*p == ' ' || *p == '\t')) p++;

        /* value */
        size_t vi = 0;
        if (*p == '"') {
            /* string */
            kvs[count].val_type = 0;
            p++;
            while (*p && *p != '"' && vi < CAN_MAX_VAL - 1) {
                if (*p == '\\' && *(p+1)) {
                    kvs[count].raw_val[vi++] = *p++;
                    if (vi < CAN_MAX_VAL - 1) kvs[count].raw_val[vi++] = *p++;
                    continue;
                }
                kvs[count].raw_val[vi++] = *p++;
            }
            if (*p == '"') p++;
        } else if (*p == '{') {
            /* nested object */
            kvs[count].val_type = 4;
            int depth = 1;
            kvs[count].raw_val[vi++] = *p++;
            while (*p && depth > 0 && vi < CAN_MAX_VAL - 1) {
                if (*p == '"') {
                    kvs[count].raw_val[vi++] = *p++;
                    while (*p && *p != '"' && vi < CAN_MAX_VAL - 1) {
                        if (*p == '\\' && *(p+1) && vi < CAN_MAX_VAL - 2)
                            kvs[count].raw_val[vi++] = *p++;
                        kvs[count].raw_val[vi++] = *p++;
                    }
                    if (*p == '"' && vi < CAN_MAX_VAL - 1) kvs[count].raw_val[vi++] = *p++;
                    continue;
                }
                if (*p == '{') depth++;
                if (*p == '}') depth--;
                kvs[count].raw_val[vi++] = *p++;
            }
        } else if (*p == '[') {
            /* array */
            kvs[count].val_type = 5;
            int depth = 1;
            kvs[count].raw_val[vi++] = *p++;
            while (*p && depth > 0 && vi < CAN_MAX_VAL - 1) {
                if (*p == '"') {
                    kvs[count].raw_val[vi++] = *p++;
                    while (*p && *p != '"' && vi < CAN_MAX_VAL - 1) {
                        if (*p == '\\' && *(p+1) && vi < CAN_MAX_VAL - 2)
                            kvs[count].raw_val[vi++] = *p++;
                        kvs[count].raw_val[vi++] = *p++;
                    }
                    if (*p == '"' && vi < CAN_MAX_VAL - 1) kvs[count].raw_val[vi++] = *p++;
                    continue;
                }
                if (*p == '[') depth++;
                if (*p == ']') depth--;
                kvs[count].raw_val[vi++] = *p++;
            }
        } else if (strncmp(p, "null", 4) == 0) {
            kvs[count].val_type = 3;
            memcpy(kvs[count].raw_val, "null", 4); vi = 4;
            p += 4;
        } else if (strncmp(p, "true", 4) == 0) {
            kvs[count].val_type = 2;
            memcpy(kvs[count].raw_val, "true", 4); vi = 4;
            p += 4;
        } else if (strncmp(p, "false", 5) == 0) {
            kvs[count].val_type = 2;
            memcpy(kvs[count].raw_val, "false", 5); vi = 5;
            p += 5;
        } else {
            /* number */
            kvs[count].val_type = 1;
            while (*p && *p != ',' && *p != '}' && *p != '\n' &&
                   *p != ' ' && *p != '\t' && vi < CAN_MAX_VAL - 1)
                kvs[count].raw_val[vi++] = *p++;
        }
        kvs[count].raw_val[vi] = '\0';
        count++;
    }
    return count;
}

/* ================================================================
 * Level 1: Syntactic Normalization
 *
 * - Sorted keys (lexicographic, recursive)
 * - Minified (no extra whitespace)
 * - Integers stay integers, strings trimmed
 * ================================================================ */

char *canonical_l1(const char *json) {
    if (!json) return NULL;

    CanKV *kvs = calloc(CAN_MAX_KEYS, sizeof(CanKV));
    if (!kvs) return NULL;
    int count = parse_flat_json(json, kvs, CAN_MAX_KEYS);
    if (count == 0) { free(kvs); return strdup("{}"); }

    /* Sort by key */
    qsort(kvs, (size_t)count, sizeof(CanKV), kv_cmp);

    /* Build canonical output */
    size_t cap = 4096;
    char *out = malloc(cap);
    if (!out) return NULL;
    size_t off = 0;
    out[off++] = '{';

    for (int i = 0; i < count; i++) {
        if (i > 0) out[off++] = ',';

        /* Ensure capacity */
        size_t needed = strlen(kvs[i].key) + strlen(kvs[i].raw_val) + 8;
        while (off + needed >= cap) {
            cap *= 2;
            char *tmp = realloc(out, cap);
            if (!tmp) { free(out); free(kvs); return NULL; }
            out = tmp;
        }

        /* Key always quoted */
        out[off++] = '"';
        size_t klen = strlen(kvs[i].key);
        memcpy(out + off, kvs[i].key, klen);
        off += klen;
        out[off++] = '"';
        out[off++] = ':';

        /* Value */
        if (kvs[i].val_type == 0) {
            /* string: re-quote */
            out[off++] = '"';
            size_t vlen = strlen(kvs[i].raw_val);
            /* Trim leading/trailing whitespace from string values */
            const char *vs = kvs[i].raw_val;
            const char *ve = vs + vlen;
            while (vs < ve && (*vs == ' ' || *vs == '\t')) vs++;
            while (ve > vs && (*(ve-1) == ' ' || *(ve-1) == '\t')) ve--;
            vlen = (size_t)(ve - vs);
            while (off + vlen + 4 >= cap) {
                cap *= 2;
                char *tmp = realloc(out, cap);
                if (!tmp) { free(out); return NULL; }
                out = tmp;
            }
            memcpy(out + off, vs, vlen);
            off += vlen;
            out[off++] = '"';
        } else if (kvs[i].val_type == 4 || kvs[i].val_type == 5) {
            /* nested object or array — recursively canonicalize if object */
            if (kvs[i].val_type == 4) {
                char *nested = canonical_l1(kvs[i].raw_val);
                if (nested) {
                    size_t nlen = strlen(nested);
                    while (off + nlen + 2 >= cap) {
                        cap *= 2;
                        char *tmp = realloc(out, cap);
                    if (!tmp) { free(out); free(nested); free(kvs); return NULL; }
                    out = tmp;
                }
                memcpy(out + off, nested, nlen);
                    off += nlen;
                    free(nested);
                }
            } else {
                /* array — copy as-is for now (TODO: canonicalize elements) */
                size_t vlen = strlen(kvs[i].raw_val);
                while (off + vlen + 2 >= cap) {
                    cap *= 2;
                    char *tmp = realloc(out, cap);
                    if (!tmp) { free(out); free(kvs); return NULL; }
                    out = tmp;
                }
                memcpy(out + off, kvs[i].raw_val, vlen);
                off += vlen;
            }
        } else {
            /* number, bool, null — copy raw */
            size_t vlen = strlen(kvs[i].raw_val);
            while (off + vlen + 2 >= cap) {
                cap *= 2;
                char *tmp = realloc(out, cap);
                if (!tmp) { free(out); free(kvs); return NULL; }
                out = tmp;
            }
            memcpy(out + off, kvs[i].raw_val, vlen);
            off += vlen;
        }
    }

    out[off++] = '}';
    out[off] = '\0';
    free(kvs);
    return out;
}

void canonical_l1_hash(const char *json, char *hash_out) {
    char *c = canonical_l1(json);
    if (!c) { memset(hash_out, '0', 16); hash_out[16] = '\0'; return; }
    unsigned long long h = fnv1a64(c, strlen(c));
    snprintf(hash_out, 17, "%016llx", h);
    free(c);
}

/* ================================================================
 * Level 2: Structural Signature
 *
 * Hashes the SHAPE of the JSON — field names + value types — not values.
 * Two objects with the same structural hash share a Lambda Tensors generator.
 *
 * sig(leaf) = H(type_tag)
 * sig(object) = H("obj" || sort([H(key || sig(value)) for each field]))
 * ================================================================ */

static const char *type_tag(int val_type) {
    switch (val_type) {
        case 0: return "str";
        case 1: return "num";
        case 2: return "bool";
        case 3: return "null";
        case 4: return "obj";
        case 5: return "arr";
    }
    return "unk";
}

void structural_hash(const char *json, char *hash_out) {
    if (!json) { memset(hash_out, '0', 16); hash_out[16] = '\0'; return; }

    CanKV *kvs = calloc(CAN_MAX_KEYS, sizeof(CanKV));
    if (!kvs) { memset(hash_out, '0', 16); hash_out[16] = '\0'; return; }
    int count = parse_flat_json(json, kvs, CAN_MAX_KEYS);
    if (count == 0) {
        unsigned long long h = fnv1a64("obj|empty", 9);
        snprintf(hash_out, 17, "%016llx", h);
        free(kvs);
        return;
    }

    /* Sort keys for determinism */
    qsort(kvs, (size_t)count, sizeof(CanKV), kv_cmp);

    /* Build structural descriptor:
       "obj|key1:type1|key2:type2|..." */
    char desc[32768];
    int off = snprintf(desc, sizeof(desc), "obj");

    for (int i = 0; i < count; i++) {
        if (kvs[i].val_type == 4) {
            /* Nested object: recursive structural hash */
            char nested_hash[17];
            structural_hash(kvs[i].raw_val, nested_hash);
            off += snprintf(desc + off, sizeof(desc) - (size_t)off,
                "|%s:obj:%s", kvs[i].key, nested_hash);
        } else {
            off += snprintf(desc + off, sizeof(desc) - (size_t)off,
                "|%s:%s", kvs[i].key, type_tag(kvs[i].val_type));
        }
    }

    unsigned long long h = fnv1a64(desc, (size_t)off);
    snprintf(hash_out, 17, "%016llx", h);
    free(kvs);
}

char *structural_signature(const char *json) {
    if (!json) return strdup("{}");

    CanKV *kvs = calloc(CAN_MAX_KEYS, sizeof(CanKV));
    if (!kvs) return NULL;
    int count = parse_flat_json(json, kvs, CAN_MAX_KEYS);
    if (count == 0) { free(kvs); return strdup("{}"); }

    qsort(kvs, (size_t)count, sizeof(CanKV), kv_cmp);

    size_t cap = 2048;
    char *out = malloc(cap);
    if (!out) { free(kvs); return NULL; }
    size_t off = 0;
    out[off++] = '{';

    for (int i = 0; i < count; i++) {
        if (i > 0) out[off++] = ',';
        size_t needed = strlen(kvs[i].key) + 32;
        while (off + needed >= cap) { cap *= 2; out = realloc(out, cap); }
        off += (size_t)snprintf(out + off, cap - off,
            "\"%s\":\"%s\"", kvs[i].key, type_tag(kvs[i].val_type));
    }

    out[off++] = '}';
    out[off] = '\0';
    free(kvs);
    return out;
}

/* ================================================================
 * Family discovery
 * ================================================================ */

int families_bootstrap(sqlite3 *db) {
    const char *stmts[] = {
        "CREATE TABLE IF NOT EXISTS _families ("
        "  id            INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  family_hash   TEXT NOT NULL,"
        "  content_type  TEXT NOT NULL,"
        "  generator     TEXT,"
        "  member_count  INTEGER DEFAULT 0,"
        "  created_at    TEXT NOT NULL,"
        "  UNIQUE(family_hash, content_type)"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_families_hash ON _families(family_hash);",
        "CREATE INDEX IF NOT EXISTS idx_families_type ON _families(content_type);",

        "CREATE TABLE IF NOT EXISTS _family_members ("
        "  family_id     INTEGER NOT NULL,"
        "  target_type   TEXT NOT NULL,"
        "  target_id     INTEGER NOT NULL,"
        "  bindings      TEXT,"
        "  PRIMARY KEY(target_type, target_id),"
        "  FOREIGN KEY(family_id) REFERENCES _families(id)"
        ");",
        NULL
    };
    for (int i = 0; stmts[i]; i++) {
        if (db_exec(db, stmts[i]) != 0) return -1;
    }
    return 0;
}

void family_id(const char *json, char *hash_out) {
    structural_hash(json, hash_out);
}

int family_remove_entry(sqlite3 *db, const char *content_type, int target_id) {
    int old_family_id = 0;
    int rc = family_remove_entry_internal(db, content_type, target_id, &old_family_id);
    if (rc == 0 && old_family_id > 0 &&
        compact_pack_family(db, old_family_id, content_type) != 0)
        return -1;
    return rc;
}

/* Light-weight rebind: update _family_members bindings without re-hashing family.
   Assumes structural shape (fields/types) hasn't changed — only values. */
static int tensor_cells_from_bindings(sqlite3 *db, int family_id,
                                      const char *content_type, int target_id,
                                      const char *bindings);

int family_rebind_entry(sqlite3 *db, const char *content_type, int target_id) {
    char sql[1024];
    sqlite3_stmt *stmt = NULL;
    char *json = NULL;
    char *bind = NULL;
    int family_id = 0;

    snprintf(sql, sizeof(sql), "SELECT id, * FROM \"%s\" WHERE id=?1", content_type);
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(stmt, 1, target_id);
    if (sqlite3_step(stmt) != SQLITE_ROW) { sqlite3_finalize(stmt); return 0; }
    json = build_row_json_from_stmt(stmt);
    sqlite3_finalize(stmt);
    if (!json) return -1;

    bind = extract_bindings(json);
    free(json);
    if (!bind) return -1;

    if (sqlite3_prepare_v2(db,
        "SELECT family_id FROM _family_members WHERE target_type=?1 AND target_id=?2",
        -1, &stmt, NULL) != SQLITE_OK) { free(bind); return -1; }
    sqlite3_bind_text(stmt, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, target_id);
    if (sqlite3_step(stmt) == SQLITE_ROW)
        family_id = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    stmt = NULL;

    sqlite3_stmt *upd;
    if (sqlite3_prepare_v2(db,
        "UPDATE _family_members SET bindings=?1 WHERE target_type=?2 AND target_id=?3",
        -1, &upd, NULL) != SQLITE_OK) { free(bind); return -1; }
    sqlite3_bind_text(upd, 1, bind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(upd, 2, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(upd, 3, target_id);
    int rc = (sqlite3_step(upd) == SQLITE_DONE) ? 0 : -1;
    sqlite3_finalize(upd);
    /* A value-only update still changes the binding vector, so the cells the
     * index reads and the index itself both have to follow. Rebinding the
     * membership and leaving the index on the old vector is how a store ends up
     * returning confident neighbours for content that no longer says that. */
    if (rc == 0 && family_id > 0) {
        tensor_cells_from_bindings(db, family_id, content_type, target_id, bind);
        ann_index_member(db, family_id, target_id);
    }
    if (rc == 0 && family_id > 0 &&
        compact_pack_family(db, family_id, content_type) != 0)
        rc = -1;
    free(bind);
    return rc;
}

/* Count entries in content_type table that have no _family_members row. */
int family_count_orphans(sqlite3 *db, const char *content_type) {
    char sql[512];
    snprintf(sql, sizeof(sql),
        "SELECT COUNT(*) FROM \"%s\" e "
        "LEFT JOIN _family_members fm ON fm.target_type='%s' AND fm.target_id=e.id "
        "WHERE fm.target_id IS NULL",
        content_type, content_type);
    sqlite3_stmt *st;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    int count = 0;
    if (sqlite3_step(st) == SQLITE_ROW) count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return count;
}

/* Materialize tensor cells from the bindings we just computed.
 *
 * The create path was fully wired -- membership, eigenvalue accumulation, memo
 * invalidation, ANN indexing, compaction -- and indexed nothing, because
 * ann_index_member reads _tensor_cells and only delta_tensor_apply ever wrote
 * them. Cells were populated by the sync path and never by authoring, so an
 * estate that had only ever been authored had 61 family members, a 12,288-row
 * projection matrix, and zero indexed vectors.
 *
 * The vector already exists here: bindings is the row's values in field order,
 * which is exactly what extract_vector reads back out. This writes it in the
 * shape the indexer looks for, at the moment it is known.
 *
 * Cannot infer: an indexed member -> a semantically embedded one. Strings are
 * hashed to a coordinate, not embedded; this makes structural recall work, and
 * says nothing about meaning. */
static int tensor_cells_from_bindings(sqlite3 *db, int family_id,
                                      const char *content_type, int target_id,
                                      const char *bindings) {
    sqlite3_stmt *ins = NULL;
    const char *p = bindings;
    int position = 0;
    int written = 0;
    char field[32];

    if (!db || !bindings || family_id <= 0) return 0;
    while (*p && *p != '[') p++;
    if (!*p) return 0;
    p++;

    if (sqlite3_prepare_v2(db,
            "INSERT OR REPLACE INTO _tensor_cells "
            "(family_id, target_type, target_id, position, field_name, "
            " val_text, val_num, val_type) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            -1, &ins, NULL) != SQLITE_OK) return -1;

    while (*p && *p != ']') {
        while (*p == ' ' || *p == ',' || *p == '\n') p++;
        if (!*p || *p == ']') break;
        sqlite3_reset(ins);
        sqlite3_clear_bindings(ins);
        sqlite3_bind_int(ins, 1, family_id);
        sqlite3_bind_text(ins, 2, content_type, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(ins, 3, target_id);
        sqlite3_bind_int(ins, 4, position);
        /* field_name is NOT NULL. The binding vector is positional -- the field
         * it came from is recoverable from the content type's attribute order --
         * so name it for what it is rather than leaving it null and losing every
         * insert to a constraint. */
        snprintf(field, sizeof(field), "bind:%d", position);
        sqlite3_bind_text(ins, 5, field, -1, SQLITE_TRANSIENT);
        if (*p == '"') {
            const char *start = ++p;
            char *value;
            size_t len;
            while (*p && !(*p == '"' && p[-1] != '\\')) p++;
            len = (size_t)(p - start);
            value = malloc(len + 1);
            if (!value) break;
            memcpy(value, start, len);
            value[len] = '\0';
            sqlite3_bind_text(ins, 6, value, -1, SQLITE_TRANSIENT);
            sqlite3_bind_null(ins, 7);
            sqlite3_bind_int(ins, 8, 0);
            if (sqlite3_step(ins) == SQLITE_DONE) written++;
            free(value);
            if (*p == '"') p++;
        } else {
            char *end = NULL;
            double number = strtod(p, &end);
            if (end == p) { while (*p && *p != ',' && *p != ']') p++; continue; }
            sqlite3_bind_null(ins, 6);
            sqlite3_bind_double(ins, 7, number);
            sqlite3_bind_int(ins, 8, 1);
            if (sqlite3_step(ins) == SQLITE_DONE) written++;
            p = end;
        }
        position++;
    }
    sqlite3_finalize(ins);
    /* Report what was written, not what was attempted. The first version bound
     * a NULL field_name, lost every row to the constraint, and returned a happy
     * count because nothing checked the step result. */
    if (written < position) {
        fprintf(stderr, "[canonical] tensor cells: wrote %d of %d for %s/%d: %s\n",
                written, position, content_type, target_id, sqlite3_errmsg(db));
    }
    return written;
}

/* bf_cms_backfill_cells -- derive missing tensor cells from existing bindings.
 *
 * The create and rebind paths now write cells as content is authored, but that
 * only helps content authored after the fix. Anything already in the store had
 * bindings and no cells: a member the indexer skips silently, because a member
 * with no vector is indistinguishable from one that has not been reached yet.
 *
 * Rather than a one-off migration script, this is called by ann_build_index, so
 * a build repairs what it is about to index. Content imported by any path that
 * writes bindings without cells -- a bulk load, a restore, a future sync -- is
 * healed by the next build rather than stranding until someone notices the
 * count is short.
 *
 * Cannot infer: a backfilled cell -> a cell equal to what the create path would
 * have written at authoring time. It is derived from the bindings as they stand
 * now, which is the same source, read later. */
/* bf_cms_prune_orphan_cells -- the other half of the reconciliation.
 *
 * Deleting an entry removes the row and its family membership, and left its
 * tensor cells behind. The index is built from cells rather than from
 * membership, so a deleted door stayed indexed: `ann stats` reported eight
 * members over seven rows, and the vector remained eligible to be returned as
 * a neighbour of something real. A lookup layer that can answer with content
 * that no longer exists is worse than one that cannot answer.
 *
 * Backfilling missing cells and pruning orphaned ones are the same invariant
 * read in two directions -- cells and membership describe the same set -- so
 * both run before a build. Neither is a migration; both are reconciliation,
 * and a store that is already correct is left untouched by both.
 *
 * Cannot infer: a pruned cell -> a deleted entry. A cell is pruned when no
 * membership claims it, which is also what a rebind looks like mid-flight;
 * the following backfill re-derives anything still owed. */
int bf_cms_prune_orphan_cells(sqlite3 *db, const char *content_type) {
    sqlite3_stmt *del = NULL;
    int pruned = 0;

    if (!db) return 0;
    if (sqlite3_prepare_v2(db,
            "DELETE FROM _tensor_cells WHERE id IN ("
            "  SELECT c.id FROM _tensor_cells c JOIN _families f ON f.id = c.family_id "
            "  WHERE (?1 = 'all' OR f.content_type = ?1) "
            "    AND NOT EXISTS (SELECT 1 FROM _family_members m "
            "                    WHERE m.family_id = c.family_id "
            "                      AND m.target_type = c.target_type "
            "                      AND m.target_id = c.target_id))",
            -1, &del, NULL) != SQLITE_OK) {
        fprintf(stderr, "[canonical] prune: %s\n", sqlite3_errmsg(db));
        return 0;
    }
    sqlite3_bind_text(del, 1, content_type ? content_type : "all", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(del) == SQLITE_DONE) pruned = sqlite3_changes(db);
    sqlite3_finalize(del);
    if (pruned)
        fprintf(stderr, "[canonical] pruned %d orphaned cell(s)\n", pruned);
    return pruned;
}

int bf_cms_backfill_cells(sqlite3 *db, const char *content_type) {
    sqlite3_stmt *scan = NULL;
    int repaired = 0;

    if (!db) return 0;
    /* Members holding a vector that the cell table has no row for. The NOT
     * EXISTS is the whole point: never rewrite cells for a member that already
     * has them, so a build stays idempotent and cannot clobber a cell the
     * create path wrote from a binding that has since changed. */
    if (sqlite3_prepare_v2(db,
            "SELECT m.family_id, f.content_type, m.target_id, m.bindings "
            "FROM _family_members m JOIN _families f ON f.id = m.family_id "
            "WHERE m.bindings IS NOT NULL AND m.bindings != '' "
            "  AND (?1 = 'all' OR f.content_type = ?1) "
            "  AND NOT EXISTS (SELECT 1 FROM _tensor_cells c "
            "                  WHERE c.family_id = m.family_id "
            "                    AND c.target_type = m.target_type "
            "                    AND c.target_id = m.target_id)",
            -1, &scan, NULL) != SQLITE_OK) {
        fprintf(stderr, "[canonical] backfill: %s\n", sqlite3_errmsg(db));
        return 0;
    }
    sqlite3_bind_text(scan, 1, content_type ? content_type : "all", -1, SQLITE_TRANSIENT);

    while (sqlite3_step(scan) == SQLITE_ROW) {
        int family_id = sqlite3_column_int(scan, 0);
        const char *type = (const char *)sqlite3_column_text(scan, 1);
        int target_id = sqlite3_column_int(scan, 2);
        const char *bind = (const char *)sqlite3_column_text(scan, 3);
        if (tensor_cells_from_bindings(db, family_id, type, target_id, bind) > 0)
            repaired++;
    }
    sqlite3_finalize(scan);
    if (repaired)
        fprintf(stderr, "[canonical] backfilled cells for %d member(s)\n", repaired);
    return repaired;
}

int family_refresh_entry(sqlite3 *db, const char *content_type, int target_id) {
    char sql[1024];
    sqlite3_stmt *stmt = NULL, *upsert_family = NULL, *select_family = NULL, *upsert_member = NULL;
    char *json = NULL;
    char *gen = NULL;
    char *bind = NULL;
    int rc = -1;
    int family_row_id = 0;
    int old_family_id = 0;
    char fhash[17];
    char ts[64];

    snprintf(sql, sizeof(sql), "SELECT id, * FROM \"%s\" WHERE id=?1", content_type);
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[canonical] refresh prepare: %s\n", sqlite3_errmsg(db));
        return -1;
    }
    sqlite3_bind_int(stmt, 1, target_id);
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        rc = family_remove_entry_internal(db, content_type, target_id, &old_family_id);
        if (rc == 0 && old_family_id > 0 &&
            compact_pack_family(db, old_family_id, content_type) != 0)
            rc = -1;
        return rc;
    }

    json = build_row_json_from_stmt(stmt);
    sqlite3_finalize(stmt);
    stmt = NULL;
    if (!json) return -1;

    structural_hash(json, fhash);
    gen = extract_generator(json);
    bind = extract_bindings(json);
    if (!gen || !bind) goto cleanup;

    if (family_remove_entry_internal(db, content_type, target_id, &old_family_id) != 0) goto cleanup;

    extern void iso_timestamp(char *, size_t);
    iso_timestamp(ts, sizeof(ts));

    if (sqlite3_prepare_v2(db,
        "INSERT INTO _families (family_hash, content_type, generator, member_count, created_at) "
        "VALUES (?1, ?2, ?3, 1, ?4) "
        "ON CONFLICT(family_hash, content_type) DO UPDATE SET member_count = member_count + 1",
        -1, &upsert_family, NULL) != SQLITE_OK) goto cleanup;
    if (sqlite3_prepare_v2(db,
        "SELECT id FROM _families WHERE family_hash=?1 AND content_type=?2",
        -1, &select_family, NULL) != SQLITE_OK) goto cleanup;
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO _family_members (family_id, target_type, target_id, bindings) "
        "VALUES (?1, ?2, ?3, ?4)",
        -1, &upsert_member, NULL) != SQLITE_OK) goto cleanup;

    if (family_upsert_membership_with_stmts(upsert_family, select_family, upsert_member,
        content_type, target_id, fhash, gen, bind, ts, &family_row_id) != 0) goto cleanup;

    /* Incrementally accumulate eigenvalue stats for the new membership */
    if (family_row_id > 0 && bind) {
        incr_add_member(db, family_row_id, bind);

        /* Invalidate memoized reductions for this family */
        char fid_str[32];
        snprintf(fid_str, sizeof(fid_str), "%d", family_row_id);
        hybrid_memo_invalidate(db, fid_str);

        /* Cells first: the indexer reads them, and nothing else writes them
         * on the authoring path. */
        tensor_cells_from_bindings(db, family_row_id, content_type, target_id, bind);

        /* Incrementally index new member in ANN */
        ann_index_member(db, family_row_id, target_id);
    }

    if (old_family_id > 0 && old_family_id != family_row_id &&
        compact_pack_family(db, old_family_id, content_type) != 0)
        goto cleanup;
    if (family_row_id > 0 &&
        compact_pack_family(db, family_row_id, content_type) != 0)
        goto cleanup;

    rc = 0;

cleanup:
    if (stmt) sqlite3_finalize(stmt);
    if (upsert_family) sqlite3_finalize(upsert_family);
    if (select_family) sqlite3_finalize(select_family);
    if (upsert_member) sqlite3_finalize(upsert_member);
    free(json);
    free(gen);
    free(bind);
    return rc;
}

/* Scan all entries of a content type, compute structural hashes,
   group into families, store generators + bindings. */
int family_discover(sqlite3 *db, const char *content_type) {
    /* Rebuild the content-type family tables from scratch for honest batch discovery. */
    sqlite3_stmt *wipe_members = NULL;
    sqlite3_stmt *wipe_families = NULL;
    if (sqlite3_prepare_v2(db, "DELETE FROM _family_members WHERE target_type=?1", -1, &wipe_members, NULL) == SQLITE_OK) {
        sqlite3_bind_text(wipe_members, 1, content_type, -1, SQLITE_STATIC);
        sqlite3_step(wipe_members);
        sqlite3_finalize(wipe_members);
    }
    if (sqlite3_prepare_v2(db, "DELETE FROM _families WHERE content_type=?1", -1, &wipe_families, NULL) == SQLITE_OK) {
        sqlite3_bind_text(wipe_families, 1, content_type, -1, SQLITE_STATIC);
        sqlite3_step(wipe_families);
        sqlite3_finalize(wipe_families);
    }

    /* Read all entries as JSON-ish, compute structural hash, group */
    char sql[1024];
    snprintf(sql, sizeof(sql),
        "SELECT id, * FROM \"%s\"", content_type);

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[canonical] discover: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    int families_found = 0;
    sqlite3_stmt *upsert_family = NULL, *select_family = NULL, *upsert_member = NULL;
    char ts[64];
    extern void iso_timestamp(char *, size_t);
    iso_timestamp(ts, sizeof(ts));

    if (sqlite3_prepare_v2(db,
        "INSERT INTO _families (family_hash, content_type, generator, member_count, created_at) "
        "VALUES (?1, ?2, ?3, 1, ?4) "
        "ON CONFLICT(family_hash, content_type) DO UPDATE SET member_count = member_count + 1",
        -1, &upsert_family, NULL) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return -1;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT id FROM _families WHERE family_hash=?1 AND content_type=?2",
        -1, &select_family, NULL) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_finalize(upsert_family);
        return -1;
    }
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO _family_members (family_id, target_type, target_id, bindings) "
        "VALUES (?1, ?2, ?3, ?4)",
        -1, &upsert_member, NULL) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_finalize(upsert_family);
        sqlite3_finalize(select_family);
        return -1;
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int entry_id = sqlite3_column_int(stmt, 0);
        char *json = build_row_json_from_stmt(stmt);
        char *gen = NULL;
        char *bind = NULL;
        char fhash[17];
        if (!json) {
            sqlite3_finalize(stmt);
            sqlite3_finalize(upsert_family);
            sqlite3_finalize(select_family);
            sqlite3_finalize(upsert_member);
            return -1;
        }
        structural_hash(json, fhash);
        gen = extract_generator(json);
        bind = extract_bindings(json);
        free(json);
        if (!gen || !bind ||
            family_upsert_membership_with_stmts(upsert_family, select_family, upsert_member,
                content_type, entry_id, fhash, gen, bind, ts, NULL) != 0) {
            free(gen);
            free(bind);
            sqlite3_finalize(stmt);
            sqlite3_finalize(upsert_family);
            sqlite3_finalize(select_family);
            sqlite3_finalize(upsert_member);
            return -1;
        }
        free(gen);
        free(bind);
    }
    sqlite3_finalize(stmt);
    sqlite3_finalize(upsert_family);
    sqlite3_finalize(select_family);
    sqlite3_finalize(upsert_member);

    /* Count actual distinct families */
    sqlite3_stmt *cnt;
    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM _families WHERE content_type=?1",
        -1, &cnt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(cnt, 1, content_type, -1, SQLITE_STATIC);
        if (sqlite3_step(cnt) == SQLITE_ROW)
            families_found = sqlite3_column_int(cnt, 0);
        sqlite3_finalize(cnt);
    }

    if (compact_pack_content_type(db, content_type) != 0)
        return -1;

    return families_found;
}

/* ================================================================
 * Generator + Bindings extraction (Lambda Tensors factorization)
 * ================================================================ */

/* Generator: the template. Keys + type tags. */
char *extract_generator(const char *json) {
    return structural_signature(json);
}

/* Bindings: the values in canonical key order. */
char *extract_bindings(const char *json) {
    if (!json) return strdup("[]");

    CanKV *kvs = calloc(CAN_MAX_KEYS, sizeof(CanKV));
    if (!kvs) return NULL;
    int count = parse_flat_json(json, kvs, CAN_MAX_KEYS);
    if (count == 0) { free(kvs); return strdup("[]"); }

    qsort(kvs, (size_t)count, sizeof(CanKV), kv_cmp);

    size_t cap = 4096;
    char *out = malloc(cap);
    if (!out) { free(kvs); return NULL; }
    size_t off = 0;
    out[off++] = '[';

    for (int i = 0; i < count; i++) {
        if (i > 0) out[off++] = ',';
        size_t needed = strlen(kvs[i].raw_val) + 8;
        while (off + needed >= cap) {
            cap *= 2;
            char *tmp = realloc(out, cap);
            if (!tmp) { free(out); free(kvs); return NULL; }
            out = tmp;
        }

        if (kvs[i].val_type == 0) {
            /* string value */
            off += (size_t)snprintf(out + off, cap - off,
                "\"%s\"", kvs[i].raw_val);
        } else {
            /* non-string value */
            size_t vlen = strlen(kvs[i].raw_val);
            memcpy(out + off, kvs[i].raw_val, vlen);
            off += vlen;
        }
    }

    out[off++] = ']';
    out[off] = '\0';
    free(kvs);
    return out;
}
