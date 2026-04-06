/*
 * BonfyreGraph — Flagship I: Ephemeral Reconstruction
 *
 * Operation log, content addressing, deterministic reconstruction.
 * See: Research - Flagship Papers (Ephemeral CRDT Reconstruction)
 */

#include "ephemeral.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ================================================================
 * Internal helpers (defined in main.c, declared here for linkage)
 * ================================================================ */
extern unsigned long long fnv1a64(const void *data, size_t len);
extern void iso_timestamp(char *buf, size_t sz);
extern int db_exec(sqlite3 *db, const char *sql);

/* ================================================================
 * Bootstrap
 * ================================================================ */

int ops_bootstrap(sqlite3 *db) {
    const char *stmts[] = {
        "CREATE TABLE IF NOT EXISTS _ops ("
        "  id        INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  op_type   TEXT NOT NULL,"
        "  target    TEXT NOT NULL,"
        "  target_id INTEGER,"
        "  namespace TEXT NOT NULL DEFAULT 'root',"
        "  payload   TEXT NOT NULL,"
        "  hash      TEXT NOT NULL,"
        "  parent    TEXT,"
        "  ts        TEXT NOT NULL,"
        "  actor     TEXT DEFAULT ''"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_ops_target ON _ops(target, target_id);",
        "CREATE INDEX IF NOT EXISTS idx_ops_hash ON _ops(hash);",
        "CREATE INDEX IF NOT EXISTS idx_ops_ns ON _ops(namespace);",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_ops_hash_uniq ON _ops(hash);",
        NULL
    };
    for (int i = 0; stmts[i]; i++) {
        if (db_exec(db, stmts[i]) != 0) return -1;
    }
    return 0;
}

/* ================================================================
 * Hash computation — FNV-1a over op fields
 * ================================================================ */

static void compute_op_hash(const char *op_type, const char *target,
                            long long target_id, const char *payload,
                            const char *parent, const char *ts,
                            const char *actor, char *hash_out)
{
    /* Build canonical string to hash:
       op_type|target|target_id|payload|parent|ts|actor */
    char buf[65536];
    int len = snprintf(buf, sizeof(buf), "%s|%s|%lld|%s|%s|%s|%s",
        op_type, target, target_id,
        payload ? payload : "",
        parent ? parent : "",
        ts, actor ? actor : "");
    if (len < 0) len = 0;
    if ((size_t)len >= sizeof(buf)) len = (int)sizeof(buf) - 1;

    unsigned long long h = fnv1a64(buf, (size_t)len);
    snprintf(hash_out, 17, "%016llx", h);
}

/* ================================================================
 * Append
 * ================================================================ */

int ops_append(sqlite3 *db, const char *op_type, const char *target,
               long long target_id, const char *ns, const char *payload,
               const char *actor, char *hash_out)
{
    /* Get parent: latest op hash for this (target, target_id) */
    char parent[17] = {0};
    ops_head(db, target, target_id, parent);

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    /* Compute content hash */
    char hash[17];
    compute_op_hash(op_type, target, target_id, payload,
                    parent[0] ? parent : NULL, ts, actor, hash);

    /* Insert */
    sqlite3_stmt *stmt;
    const char *sql =
        "INSERT OR IGNORE INTO _ops (op_type, target, target_id, namespace, "
        "payload, hash, parent, ts, actor) "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[ops] prepare: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    sqlite3_bind_text(stmt, 1, op_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, target, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 3, target_id);
    sqlite3_bind_text(stmt, 4, ns ? ns : "root", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 5, payload, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 6, hash, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 7, parent[0] ? parent : NULL, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 8, ts, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 9, actor ? actor : "", -1, SQLITE_STATIC);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        fprintf(stderr, "[ops] insert: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    if (hash_out) memcpy(hash_out, hash, 17);
    return 0;
}

/* ================================================================
 * Head — latest op hash for an entity
 * ================================================================ */

int ops_head(sqlite3 *db, const char *target, long long target_id, char *hash_out) {
    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT hash FROM _ops WHERE target=?1 AND target_id=?2 "
        "ORDER BY id DESC LIMIT 1";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;

    sqlite3_bind_text(stmt, 1, target, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 2, target_id);

    int found = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *h = (const char *)sqlite3_column_text(stmt, 0);
        if (h) { strncpy(hash_out, h, 16); hash_out[16] = '\0'; found = 1; }
    }
    sqlite3_finalize(stmt);
    return found ? 0 : -1;
}

/* ================================================================
 * Namespace head
 * ================================================================ */

int ops_namespace_head(sqlite3 *db, const char *ns, char *hash_out) {
    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT hash FROM _ops WHERE namespace=?1 ORDER BY id DESC LIMIT 1";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;

    sqlite3_bind_text(stmt, 1, ns ? ns : "root", -1, SQLITE_STATIC);

    int found = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *h = (const char *)sqlite3_column_text(stmt, 0);
        if (h) { strncpy(hash_out, h, 16); hash_out[16] = '\0'; found = 1; }
    }
    sqlite3_finalize(stmt);
    return found ? 0 : -1;
}

/* ================================================================
 * Op count
 * ================================================================ */

int ops_count(sqlite3 *db, const char *target, long long target_id) {
    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT COUNT(*) FROM _ops WHERE target=?1 AND target_id=?2";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return 0;

    sqlite3_bind_text(stmt, 1, target, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 2, target_id);

    int count = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

/* ================================================================
 * Reconstruct — deterministic fold over op log
 *
 * Apply ops in topological order (id order for linear history,
 * ts+hash tiebreaker for concurrent branches).
 *
 * create: payload becomes initial state
 * update: merge fields from payload into state
 * delete: state becomes NULL (returns -1)
 *
 * The reconstruct function produces a JSON object by folding
 * all ops in order — this is the ephemeral reconstruction primitive.
 * ================================================================ */

/* Merge two JSON objects (flat, single level). Updates fields from
   patch into base. Both are flat JSON strings.
   Returns newly allocated merged JSON. Caller frees. */
static char *json_merge(const char *base, const char *patch) {
    /* Strategy: collect all key-value pairs from base, then overwrite
       with pairs from patch. Simple and correct for flat JSON. */

    /* We'll build the result incrementally.
       For structured content entries (which are flat JSON), this is sufficient. */

    /* Parse base keys into a simple key=value store */
    #define MERGE_MAX_KEYS 128
    #define MERGE_MAX_KEY  256
    #define MERGE_MAX_VAL  8192

    struct merge_kv { char key[MERGE_MAX_KEY]; char val[MERGE_MAX_VAL]; int is_string; };
    struct merge_kv *kvs = calloc(MERGE_MAX_KEYS, sizeof(struct merge_kv));
    if (!kvs) return NULL;
    int count = 0;

    /* Extract from a JSON string — finds "key": value pairs.
       Defined as a macro to avoid GCC nested function extension. */
    #define MERGE_EXTRACT(json_src) do { \
        const char *p_ = (json_src); \
        while (*p_ && *p_ != '{') p_++; \
        if (*p_ == '{') p_++; \
        while (*p_ && *p_ != '}' && count < MERGE_MAX_KEYS) { \
            while (*p_ && (*p_ == ' ' || *p_ == '\n' || *p_ == '\r' || *p_ == '\t' || *p_ == ',')) p_++; \
            if (*p_ == '}' || !*p_) break; \
            if (*p_ != '"') { p_++; continue; } \
            p_++; \
            char key_[MERGE_MAX_KEY]; memset(key_, 0, sizeof(key_)); \
            size_t ki_ = 0; \
            while (*p_ && *p_ != '"' && ki_ < MERGE_MAX_KEY - 1) { \
                if (*p_ == '\\' && *(p_+1)) { key_[ki_++] = *(p_+1); p_ += 2; continue; } \
                key_[ki_++] = *p_++; \
            } \
            key_[ki_] = '\0'; \
            if (*p_ == '"') p_++; \
            while (*p_ && *p_ != ':') p_++; \
            if (*p_ == ':') p_++; \
            while (*p_ && (*p_ == ' ' || *p_ == '\t')) p_++; \
            char val_[MERGE_MAX_VAL]; memset(val_, 0, sizeof(val_)); \
            int is_str_ = 0; \
            if (*p_ == '"') { \
                is_str_ = 1; p_++; \
                size_t vi_ = 0; \
                while (*p_ && *p_ != '"' && vi_ < MERGE_MAX_VAL - 1) { \
                    if (*p_ == '\\' && *(p_+1)) { \
                        val_[vi_++] = *p_++; \
                        if (vi_ < MERGE_MAX_VAL - 1) val_[vi_++] = *p_++; \
                        continue; \
                    } \
                    val_[vi_++] = *p_++; \
                } \
                val_[vi_] = '\0'; \
                if (*p_ == '"') p_++; \
            } else if (*p_ == '{' || *p_ == '[') { \
                char o_ = *p_, c_ = (o_ == '{') ? '}' : ']'; \
                int d_ = 1; size_t vi_ = 0; \
                val_[vi_++] = *p_++; \
                while (*p_ && d_ > 0 && vi_ < MERGE_MAX_VAL - 1) { \
                    if (*p_ == '"') { \
                        val_[vi_++] = *p_++; \
                        while (*p_ && *p_ != '"' && vi_ < MERGE_MAX_VAL - 1) { \
                            if (*p_ == '\\' && *(p_+1) && vi_ < MERGE_MAX_VAL - 2) \
                                { val_[vi_++] = *p_++; } \
                            val_[vi_++] = *p_++; \
                        } \
                        if (*p_ == '"' && vi_ < MERGE_MAX_VAL - 1) val_[vi_++] = *p_++; \
                        continue; \
                    } \
                    if (*p_ == o_) d_++; \
                    if (*p_ == c_) d_--; \
                    val_[vi_++] = *p_++; \
                } \
                val_[vi_] = '\0'; \
            } else { \
                size_t vi_ = 0; \
                while (*p_ && *p_ != ',' && *p_ != '}' && *p_ != '\n' && vi_ < MERGE_MAX_VAL - 1) \
                    val_[vi_++] = *p_++; \
                while (vi_ > 0 && (val_[vi_-1] == ' ' || val_[vi_-1] == '\t' || val_[vi_-1] == '\r')) \
                    vi_--; \
                val_[vi_] = '\0'; \
            } \
            int idx_ = -1; \
            for (int i_ = 0; i_ < count; i_++) { \
                if (strcmp(kvs[i_].key, key_) == 0) { idx_ = i_; break; } \
            } \
            if (idx_ >= 0) { \
                strncpy(kvs[idx_].val, val_, MERGE_MAX_VAL - 1); \
                kvs[idx_].is_string = is_str_; \
            } else { \
                strncpy(kvs[count].key, key_, MERGE_MAX_KEY - 1); \
                strncpy(kvs[count].val, val_, MERGE_MAX_VAL - 1); \
                kvs[count].is_string = is_str_; \
                count++; \
            } \
        } \
    } while(0)

    if (base) { MERGE_EXTRACT(base); }
    if (patch) { MERGE_EXTRACT(patch); }
    #undef MERGE_EXTRACT

    /* Build output JSON */
    size_t out_cap = 4096;
    char *out = malloc(out_cap);
    if (!out) { free(kvs); return NULL; }
    size_t off = 0;
    out[off++] = '{';

    for (int i = 0; i < count; i++) {
        if (i > 0) { out[off++] = ','; }

        /* Ensure capacity */
        size_t needed = strlen(kvs[i].key) + strlen(kvs[i].val) + 8;
        while (off + needed >= out_cap) {
            out_cap *= 2;
            char *tmp = realloc(out, out_cap);
            if (!tmp) { free(out); free(kvs); return NULL; }
            out = tmp;
        }

        if (kvs[i].is_string) {
            off += (size_t)snprintf(out + off, out_cap - off,
                "\"%s\":\"%s\"", kvs[i].key, kvs[i].val);
        } else {
            off += (size_t)snprintf(out + off, out_cap - off,
                "\"%s\":%s", kvs[i].key, kvs[i].val);
        }
    }
    out[off++] = '}';
    out[off] = '\0';

    free(kvs);
    return out;

    #undef MERGE_MAX_KEYS
    #undef MERGE_MAX_KEY
    #undef MERGE_MAX_VAL
}

int ops_reconstruct(sqlite3 *db, const char *target, long long target_id,
                    char **out_json)
{
    *out_json = NULL;

    /* Replay ops in order. For concurrent branches (same ts), use hash as
       tiebreaker for deterministic ordering. */
    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT op_type, payload FROM _ops "
        "WHERE target=?1 AND target_id=?2 "
        "ORDER BY ts ASC, hash ASC";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[ops] reconstruct prepare: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    sqlite3_bind_text(stmt, 1, target, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 2, target_id);

    char *state = NULL;
    int op_count = 0;
    int deleted = 0;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *op = (const char *)sqlite3_column_text(stmt, 0);
        const char *payload = (const char *)sqlite3_column_text(stmt, 1);
        if (!op || !payload) continue;

        op_count++;

        if (strcmp(op, OP_CREATE) == 0) {
            free(state);
            state = strdup(payload);
            deleted = 0;
        } else if (strcmp(op, OP_UPDATE) == 0) {
            if (state && !deleted) {
                char *merged = json_merge(state, payload);
                free(state);
                state = merged;
            }
        } else if (strcmp(op, OP_DELETE) == 0) {
            free(state);
            state = NULL;
            deleted = 1;
        }
    }
    sqlite3_finalize(stmt);

    if (op_count == 0 || deleted || !state) {
        free(state);
        return -1;
    }

    *out_json = state;
    return 0;
}

/* ================================================================
 * History — full op log as JSON array
 * ================================================================ */

int ops_history(sqlite3 *db, const char *target, long long target_id,
                char **out_json)
{
    *out_json = NULL;

    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT op_type, payload, hash, parent, ts, actor FROM _ops "
        "WHERE target=?1 AND target_id=?2 "
        "ORDER BY ts ASC, hash ASC";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;

    sqlite3_bind_text(stmt, 1, target, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 2, target_id);

    size_t cap = 4096;
    char *buf = malloc(cap);
    if (!buf) { sqlite3_finalize(stmt); return 0; }
    size_t off = 0;
    buf[off++] = '[';

    int count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *op     = (const char *)sqlite3_column_text(stmt, 0);
        const char *pay    = (const char *)sqlite3_column_text(stmt, 1);
        const char *hash   = (const char *)sqlite3_column_text(stmt, 2);
        const char *parent = (const char *)sqlite3_column_text(stmt, 3);
        const char *ts     = (const char *)sqlite3_column_text(stmt, 4);
        const char *actor  = (const char *)sqlite3_column_text(stmt, 5);

        /* Ensure capacity */
        size_t needed = 512 + (pay ? strlen(pay) : 0);
        while (off + needed >= cap) { cap *= 2; buf = realloc(buf, cap); }

        if (count > 0) buf[off++] = ',';

        off += (size_t)snprintf(buf + off, cap - off,
            "{\"op\":\"%s\",\"hash\":\"%s\"",
            op ? op : "", hash ? hash : "");

        if (parent && parent[0])
            off += (size_t)snprintf(buf + off, cap - off, ",\"parent\":\"%s\"", parent);
        else
            off += (size_t)snprintf(buf + off, cap - off, ",\"parent\":null");

        off += (size_t)snprintf(buf + off, cap - off,
            ",\"ts\":\"%s\",\"actor\":\"%s\",\"payload\":%s}",
            ts ? ts : "", actor ? actor : "", pay ? pay : "{}");

        count++;
    }
    sqlite3_finalize(stmt);

    buf[off++] = ']';
    buf[off] = '\0';

    *out_json = buf;
    return count;
}

/* ================================================================
 * Merge — import missing ops from a remote database
 * ================================================================ */

int ops_merge(sqlite3 *db, sqlite3 *remote_db, const char *ns) {
    /* Select all ops from remote namespace that we don't already have (by hash) */
    sqlite3_stmt *remote_stmt;
    const char *remote_sql =
        "SELECT op_type, target, target_id, namespace, payload, hash, parent, ts, actor "
        "FROM _ops WHERE namespace=?1 ORDER BY id ASC";
    if (sqlite3_prepare_v2(remote_db, remote_sql, -1, &remote_stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[ops] merge: cannot query remote: %s\n", sqlite3_errmsg(remote_db));
        return -1;
    }
    sqlite3_bind_text(remote_stmt, 1, ns ? ns : "root", -1, SQLITE_STATIC);

    /* Prepare insert for local */
    sqlite3_stmt *insert_stmt;
    const char *insert_sql =
        "INSERT OR IGNORE INTO _ops "
        "(op_type, target, target_id, namespace, payload, hash, parent, ts, actor) "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)";
    if (sqlite3_prepare_v2(db, insert_sql, -1, &insert_stmt, NULL) != SQLITE_OK) {
        sqlite3_finalize(remote_stmt);
        return -1;
    }

    int merged = 0;
    db_exec(db, "BEGIN TRANSACTION");

    while (sqlite3_step(remote_stmt) == SQLITE_ROW) {
        const char *hash = (const char *)sqlite3_column_text(remote_stmt, 5);

        sqlite3_reset(insert_stmt);
        sqlite3_bind_text(insert_stmt,  1, (const char *)sqlite3_column_text(remote_stmt, 0), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(insert_stmt,  2, (const char *)sqlite3_column_text(remote_stmt, 1), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(insert_stmt, 3, sqlite3_column_int64(remote_stmt, 2));
        sqlite3_bind_text(insert_stmt,  4, (const char *)sqlite3_column_text(remote_stmt, 3), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(insert_stmt,  5, (const char *)sqlite3_column_text(remote_stmt, 4), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(insert_stmt,  6, hash, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(insert_stmt,  7, (const char *)sqlite3_column_text(remote_stmt, 6), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(insert_stmt,  8, (const char *)sqlite3_column_text(remote_stmt, 7), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(insert_stmt,  9, (const char *)sqlite3_column_text(remote_stmt, 8), -1, SQLITE_TRANSIENT);

        if (sqlite3_step(insert_stmt) == SQLITE_DONE) {
            if (sqlite3_changes(db) > 0) merged++;
        }
    }

    db_exec(db, "COMMIT");
    sqlite3_finalize(remote_stmt);
    sqlite3_finalize(insert_stmt);

    return merged;
}
