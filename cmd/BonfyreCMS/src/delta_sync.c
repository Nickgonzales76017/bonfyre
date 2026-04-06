/*
 * BonfyreGraph — Upgrade IV: Differential Delta Sync
 *
 * Hash-chain based divergence detection over the op log.
 * Compact delta extraction and three-way merge with LWW conflict resolution.
 * Tensor-level cell diffs for efficient compressed-domain sync.
 */

#include "delta_sync.h"
#include "ephemeral.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern unsigned long long fnv1a64(const void *data, size_t len);
extern void iso_timestamp(char *buf, size_t sz);
extern int db_exec(sqlite3 *db, const char *sql);

/* ================================================================
 * Bootstrap
 * ================================================================ */

int delta_bootstrap(sqlite3 *db) {
    const char *stmts[] = {
        "CREATE TABLE IF NOT EXISTS _sync_state ("
        "  id          INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  remote_id   TEXT NOT NULL,"
        "  ns          TEXT NOT NULL,"
        "  last_hash   TEXT NOT NULL,"
        "  last_op_id  INTEGER DEFAULT 0,"
        "  synced_at   TEXT NOT NULL,"
        "  UNIQUE(remote_id, ns)"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_sync_remote ON _sync_state(remote_id, ns);",

        NULL
    };
    for (int i = 0; stmts[i]; i++) {
        if (db_exec(db, stmts[i]) != 0) return -1;
    }
    return 0;
}

/* ================================================================
 * delta_find_fork — binary search for divergence point
 *
 * Walk the local and remote op logs from head backwards.
 * The fork point is the last hash present in both logs.
 * ================================================================ */

int delta_find_fork(sqlite3 *local_db, sqlite3 *remote_db,
                    const char *ns, char *common_hash) {
    common_hash[0] = '\0';

    /* Collect local hashes in order */
    sqlite3_stmt *lq;
    if (sqlite3_prepare_v2(local_db,
        "SELECT hash FROM _ops WHERE ns=?1 ORDER BY id DESC LIMIT 10000",
        -1, &lq, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(lq, 1, ns, -1, SQLITE_STATIC);

    while (sqlite3_step(lq) == SQLITE_ROW) {
        const char *lhash = (const char *)sqlite3_column_text(lq, 0);
        if (!lhash) continue;

        /* Check if this hash exists in remote */
        sqlite3_stmt *rq;
        if (sqlite3_prepare_v2(remote_db,
            "SELECT 1 FROM _ops WHERE hash=?1 AND ns=?2 LIMIT 1",
            -1, &rq, NULL) == SQLITE_OK) {
            sqlite3_bind_text(rq, 1, lhash, -1, SQLITE_STATIC);
            sqlite3_bind_text(rq, 2, ns, -1, SQLITE_STATIC);
            if (sqlite3_step(rq) == SQLITE_ROW) {
                strncpy(common_hash, lhash, 16);
                common_hash[16] = '\0';
                sqlite3_finalize(rq);
                sqlite3_finalize(lq);
                return 0;
            }
            sqlite3_finalize(rq);
        }
    }
    sqlite3_finalize(lq);
    return -1; /* fully diverged */
}

/* ================================================================
 * delta_extract — get all ops since a hash
 * ================================================================ */

DeltaOp *delta_extract(sqlite3 *db, const char *ns,
                       const char *since_hash, int *count) {
    *count = 0;

    /* Find the op_id of the since_hash */
    long long since_id = 0;
    if (since_hash && since_hash[0]) {
        sqlite3_stmt *hq;
        if (sqlite3_prepare_v2(db,
            "SELECT id FROM _ops WHERE hash=?1 AND ns=?2 LIMIT 1",
            -1, &hq, NULL) == SQLITE_OK) {
            sqlite3_bind_text(hq, 1, since_hash, -1, SQLITE_STATIC);
            sqlite3_bind_text(hq, 2, ns, -1, SQLITE_STATIC);
            if (sqlite3_step(hq) == SQLITE_ROW)
                since_id = sqlite3_column_int64(hq, 0);
            sqlite3_finalize(hq);
        }
    }

    /* Count ops after since_id */
    int total = 0;
    {
        sqlite3_stmt *cq;
        if (sqlite3_prepare_v2(db,
            "SELECT COUNT(*) FROM _ops WHERE ns=?1 AND id > ?2",
            -1, &cq, NULL) == SQLITE_OK) {
            sqlite3_bind_text(cq, 1, ns, -1, SQLITE_STATIC);
            sqlite3_bind_int64(cq, 2, since_id);
            if (sqlite3_step(cq) == SQLITE_ROW)
                total = sqlite3_column_int(cq, 0);
            sqlite3_finalize(cq);
        }
    }
    if (total <= 0) return NULL;

    DeltaOp *ops = calloc((size_t)total, sizeof(DeltaOp));

    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT id, op_type, target, target_id, ns, payload, hash, prev_hash, ts "
        "FROM _ops WHERE ns=?1 AND id > ?2 ORDER BY id ASC",
        -1, &q, NULL) != SQLITE_OK) {
        free(ops);
        return NULL;
    }
    sqlite3_bind_text(q, 1, ns, -1, SQLITE_STATIC);
    sqlite3_bind_int64(q, 2, since_id);

    int idx = 0;
    while (sqlite3_step(q) == SQLITE_ROW && idx < total) {
        ops[idx].op_id = sqlite3_column_int64(q, 0);
        const char *s;
        s = (const char *)sqlite3_column_text(q, 1);
        if (s) strncpy(ops[idx].op_type, s, 15);
        s = (const char *)sqlite3_column_text(q, 2);
        if (s) strncpy(ops[idx].target, s, 255);
        ops[idx].target_id = sqlite3_column_int64(q, 3);
        s = (const char *)sqlite3_column_text(q, 4);
        if (s) strncpy(ops[idx].ns, s, 63);
        s = (const char *)sqlite3_column_text(q, 5);
        if (s) strncpy(ops[idx].payload, s, sizeof(ops[idx].payload) - 1);
        s = (const char *)sqlite3_column_text(q, 6);
        if (s) strncpy(ops[idx].hash, s, 16);
        s = (const char *)sqlite3_column_text(q, 7);
        if (s) strncpy(ops[idx].prev_hash, s, 16);
        s = (const char *)sqlite3_column_text(q, 8);
        if (s) strncpy(ops[idx].ts, s, 63);
        idx++;
    }
    sqlite3_finalize(q);

    *count = idx;
    return ops;
}

/* ================================================================
 * delta_export_json — compact serialization
 * ================================================================ */

int delta_export_json(sqlite3 *db, const char *ns,
                      const char *since_hash, char **out_json) {
    *out_json = NULL;

    int count = 0;
    DeltaOp *ops = delta_extract(db, ns, since_hash, &count);
    if (count <= 0 || !ops) {
        *out_json = strdup("{\"ops\":[],\"count\":0}");
        free(ops);
        return 0;
    }

    size_t cap = (size_t)count * 512 + 256;
    char *out = malloc(cap);
    size_t off = 0;

    off += (size_t)snprintf(out, cap,
        "{\"since\":\"%s\",\"ns\":\"%s\",\"count\":%d,\"ops\":[",
        since_hash ? since_hash : "", ns, count);

    for (int i = 0; i < count; i++) {
        while (off + 8192 + 256 >= cap) { cap *= 2; out = realloc(out, cap); }
        if (i > 0) out[off++] = ',';
        off += (size_t)snprintf(out + off, cap - off,
            "{\"op\":\"%s\",\"target\":\"%s\",\"tid\":%lld,"
            "\"payload\":%s,\"hash\":\"%s\",\"prev\":\"%s\",\"ts\":\"%s\"}",
            ops[i].op_type, ops[i].target, ops[i].target_id,
            ops[i].payload[0] ? ops[i].payload : "{}",
            ops[i].hash, ops[i].prev_hash, ops[i].ts);
    }

    off += (size_t)snprintf(out + off, cap - off, "]}");
    *out_json = out;
    free(ops);
    return count;
}

/* ================================================================
 * delta_import_json — three-way merge with LWW resolution
 * ================================================================ */

int delta_import_json(sqlite3 *db, const char *delta_json, int *conflicts) {
    if (conflicts) *conflicts = 0;
    if (!delta_json) return 0;

    /* Parse: find "ops":[...] array */
    const char *ops_start = strstr(delta_json, "\"ops\":[");
    if (!ops_start) return 0;
    ops_start += 7; /* skip "ops":[ */

    int applied = 0;

    /* Parse each op object */
    const char *p = ops_start;
    while (*p && *p != ']') {
        while (*p && *p != '{') p++;
        if (!*p || *p == ']') break;
        p++; /* skip { */

        char op_type[16] = {0}, target[256] = {0}, hash[17] = {0};
        char payload[8192] = {0}, ts[64] = {0};
        long long tid = 0;

        /* Simple key-value extraction within this object */
        int depth = 1;
        const char *obj_start = p - 1;
        while (*p && depth > 0) {
            if (*p == '{') depth++;
            if (*p == '}') depth--;
            if (depth <= 0) break;
            p++;
        }
        if (*p == '}') p++;

        /* Extract fields using strstr within the object bounds */
        size_t obj_len = (size_t)(p - obj_start);
        char *obj = malloc(obj_len + 1);
        memcpy(obj, obj_start, obj_len);
        obj[obj_len] = '\0';

        /* Parse op type */
        const char *f;
        f = strstr(obj, "\"op\":\"");
        if (f) { f += 6; size_t i = 0; while (f[i] && f[i] != '"' && i < 15) { op_type[i] = f[i]; i++; } }

        f = strstr(obj, "\"target\":\"");
        if (f) { f += 10; size_t i = 0; while (f[i] && f[i] != '"' && i < 255) { target[i] = f[i]; i++; } }

        f = strstr(obj, "\"tid\":");
        if (f) { f += 6; tid = atoll(f); }

        f = strstr(obj, "\"hash\":\"");
        if (f) { f += 8; size_t i = 0; while (f[i] && f[i] != '"' && i < 16) { hash[i] = f[i]; i++; } }

        f = strstr(obj, "\"ts\":\"");
        if (f) { f += 6; size_t i = 0; while (f[i] && f[i] != '"' && i < 63) { ts[i] = f[i]; i++; } }

        /* Extract payload (JSON object) */
        f = strstr(obj, "\"payload\":");
        if (f) {
            f += 10;
            while (*f == ' ') f++;
            if (*f == '{') {
                int pd = 1;
                size_t pi = 0;
                payload[pi++] = *f++;
                while (*f && pd > 0 && pi < sizeof(payload) - 1) {
                    if (*f == '{') pd++;
                    if (*f == '}') pd--;
                    payload[pi++] = *f++;
                }
                payload[pi] = '\0';
            }
        }

        free(obj);

        if (!op_type[0] || !target[0]) continue;

        /* Check for conflict: does this hash already exist? */
        sqlite3_stmt *chk;
        int exists = 0;
        if (sqlite3_prepare_v2(db,
            "SELECT 1 FROM _ops WHERE hash=?1 LIMIT 1",
            -1, &chk, NULL) == SQLITE_OK) {
            sqlite3_bind_text(chk, 1, hash, -1, SQLITE_STATIC);
            if (sqlite3_step(chk) == SQLITE_ROW) exists = 1;
            sqlite3_finalize(chk);
        }

        if (exists) {
            /* Already have this op — skip */
            continue;
        }

        /* Check for LWW conflict: same (target, target_id) with newer local op */
        int conflict = 0;
        if (ts[0]) {
            sqlite3_stmt *cq;
            if (sqlite3_prepare_v2(db,
                "SELECT 1 FROM _ops WHERE target=?1 AND target_id=?2 AND ts > ?3 LIMIT 1",
                -1, &cq, NULL) == SQLITE_OK) {
                sqlite3_bind_text(cq, 1, target, -1, SQLITE_STATIC);
                sqlite3_bind_int64(cq, 2, tid);
                sqlite3_bind_text(cq, 3, ts, -1, SQLITE_STATIC);
                if (sqlite3_step(cq) == SQLITE_ROW) conflict = 1;
                sqlite3_finalize(cq);
            }
        }

        if (conflict) {
            if (conflicts) (*conflicts)++;
            /* LWW: skip this op since local is newer */
            continue;
        }

        /* Apply the op */
        ops_append(db, op_type, target, tid, "root",
                   payload[0] ? payload : "{}", "delta_sync", NULL);
        applied++;
    }

    return applied;
}

/* ================================================================
 * delta_tensor_diff — compact cell-level diff since timestamp
 * ================================================================ */

int delta_tensor_diff(sqlite3 *db, const char *content_type,
                      const char *since_ts, char **out_json) {
    *out_json = NULL;

    /* Find ops since timestamp and extract affected target_ids */
    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT DISTINCT c.family_id, c.target_id, c.position, "
        "c.field_name, c.val_text, c.val_num, c.val_type "
        "FROM _tensor_cells c "
        "JOIN _families f ON f.id = c.family_id "
        "JOIN _ops o ON o.target = f.content_type AND o.target_id = c.target_id "
        "WHERE f.content_type = ?1 AND o.ts > ?2 "
        "ORDER BY c.family_id, c.target_id, c.position",
        -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(q, 2, since_ts, -1, SQLITE_STATIC);

    size_t cap = 4096;
    char *out = malloc(cap);
    size_t off = 0;
    off += (size_t)snprintf(out, cap,
        "{\"content_type\":\"%s\",\"since\":\"%s\",\"cells\":[",
        content_type, since_ts);
    int count = 0;

    while (sqlite3_step(q) == SQLITE_ROW) {
        int fid = sqlite3_column_int(q, 0);
        int tid = sqlite3_column_int(q, 1);
        int pos = sqlite3_column_int(q, 2);
        const char *fname = (const char *)sqlite3_column_text(q, 3);
        const char *vtext = (const char *)sqlite3_column_text(q, 4);
        double vnum = sqlite3_column_double(q, 5);
        int vtype = sqlite3_column_int(q, 6);

        while (off + 1024 >= cap) { cap *= 2; out = realloc(out, cap); }
        if (count > 0) out[off++] = ',';

        if (vtype == 0) {
            off += (size_t)snprintf(out + off, cap - off,
                "{\"f\":%d,\"t\":%d,\"p\":%d,\"n\":\"%s\",\"v\":\"%s\",\"y\":0}",
                fid, tid, pos, fname ? fname : "", vtext ? vtext : "");
        } else {
            off += (size_t)snprintf(out + off, cap - off,
                "{\"f\":%d,\"t\":%d,\"p\":%d,\"n\":\"%s\",\"v\":%.6f,\"y\":%d}",
                fid, tid, pos, fname ? fname : "", vnum, vtype);
        }
        count++;
    }
    sqlite3_finalize(q);

    while (off + 32 >= cap) { cap *= 2; out = realloc(out, cap); }
    off += (size_t)snprintf(out + off, cap - off, "],\"count\":%d}", count);
    *out_json = out;
    return count;
}

/* ================================================================
 * delta_tensor_apply — apply cell-level diff
 * ================================================================ */

int delta_tensor_apply(sqlite3 *db, const char *diff_json) {
    if (!diff_json) return 0;

    /* Find cells array */
    const char *cells_start = strstr(diff_json, "\"cells\":[");
    if (!cells_start) return 0;
    cells_start += 9;

    sqlite3_stmt *ins;
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO _tensor_cells "
        "(family_id, target_type, target_id, position, field_name, "
        " val_text, val_num, val_type) "
        "VALUES (?1, (SELECT content_type FROM _families WHERE id=?1), "
        "?2, ?3, ?4, ?5, ?6, ?7)",
        -1, &ins, NULL) != SQLITE_OK) return -1;

    int applied = 0;
    const char *p = cells_start;

    while (*p && *p != ']') {
        while (*p && *p != '{') p++;
        if (!*p || *p == ']') break;

        /* Parse compact cell: {"f":N,"t":N,"p":N,"n":"...","v":...,"y":N} */
        int fid = 0, tid = 0, pos = 0, vtype = 0;
        char fname[256] = {0};
        char vtext[4096] = {0};
        double vnum = 0.0;

        const char *f;
        f = strstr(p, "\"f\":");
        if (f) fid = atoi(f + 4);
        f = strstr(p, "\"t\":");
        if (f) tid = atoi(f + 4);
        f = strstr(p, "\"p\":");
        if (f) pos = atoi(f + 4);
        f = strstr(p, "\"y\":");
        if (f) vtype = atoi(f + 4);

        f = strstr(p, "\"n\":\"");
        if (f) {
            f += 5;
            size_t i = 0;
            while (f[i] && f[i] != '"' && i < 255) { fname[i] = f[i]; i++; }
        }

        f = strstr(p, "\"v\":");
        if (f) {
            f += 4;
            if (*f == '"') {
                f++;
                size_t i = 0;
                while (f[i] && f[i] != '"' && i < 4095) { vtext[i] = f[i]; i++; }
            } else {
                vnum = strtod(f, NULL);
                snprintf(vtext, sizeof(vtext), "%.6f", vnum);
            }
        }

        /* Skip to end of this object */
        int depth = 1;
        p++;
        while (*p && depth > 0) {
            if (*p == '{') depth++;
            if (*p == '}') depth--;
            p++;
        }

        if (fid > 0 && tid > 0) {
            sqlite3_reset(ins);
            sqlite3_bind_int(ins, 1, fid);
            sqlite3_bind_int(ins, 2, tid);
            sqlite3_bind_int(ins, 3, pos);
            sqlite3_bind_text(ins, 4, fname, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 5, vtext, -1, SQLITE_TRANSIENT);
            sqlite3_bind_double(ins, 6, vnum);
            sqlite3_bind_int(ins, 7, vtype);
            if (sqlite3_step(ins) == SQLITE_DONE) applied++;
        }
    }
    sqlite3_finalize(ins);
    return applied;
}

/* ================================================================
 * delta_checkpoint / delta_last_sync
 * ================================================================ */

int delta_checkpoint(sqlite3 *db, const char *remote_id,
                     const char *ns, const char *hash) {
    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    sqlite3_stmt *ins;
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO _sync_state "
        "(remote_id, ns, last_hash, synced_at) VALUES (?1, ?2, ?3, ?4)",
        -1, &ins, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(ins, 1, remote_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 2, ns, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 3, hash, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 4, ts, -1, SQLITE_STATIC);
    int rc = sqlite3_step(ins);
    sqlite3_finalize(ins);
    return rc == SQLITE_DONE ? 0 : -1;
}

int delta_last_sync(sqlite3 *db, const char *remote_id,
                    const char *ns, char *hash_out) {
    hash_out[0] = '\0';

    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT last_hash FROM _sync_state WHERE remote_id=?1 AND ns=?2",
        -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(q, 1, remote_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(q, 2, ns, -1, SQLITE_STATIC);

    int found = -1;
    if (sqlite3_step(q) == SQLITE_ROW) {
        const char *h = (const char *)sqlite3_column_text(q, 0);
        if (h) { strncpy(hash_out, h, 16); hash_out[16] = '\0'; found = 0; }
    }
    sqlite3_finalize(q);
    return found;
}
