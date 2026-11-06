#include "public_projection_cache.h"

#include "bonfyre.h"

#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int bf_public_projection_response_cacheable(const void *response, size_t length) {
    const unsigned char *p = response;
    if (!p || length < 13) return 0;
    return (!memcmp(p, "HTTP/1.1 200", 12) || !memcmp(p, "HTTP/1.0 200", 12)) &&
           (p[12] == ' ' || p[12] == '\r' || p[12] == '\n');
}

int bf_public_projection_forward(FILE *capture, FILE *out) {
    unsigned char buffer[65536]; size_t n;
    if (!capture || !out || fseek(capture, 0, SEEK_SET) != 0) return 0;
    clearerr(capture);
    while ((n = fread(buffer, 1, sizeof(buffer), capture)) > 0)
        if (fwrite(buffer, 1, n, out) != n) return 0;
    return !ferror(capture);
}

static sqlite3 *open_cache(const char *path, int writable, int busy_ms) {
    sqlite3 *db = NULL;
    int flags = writable ? SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE : SQLITE_OPEN_READONLY;
    if (!path || !*path || sqlite3_open_v2(path, &db, flags, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return NULL;
    }
    sqlite3_busy_timeout(db, busy_ms);
    if (writable && sqlite3_exec(db,
        "PRAGMA journal_mode=WAL;"
        "CREATE TABLE IF NOT EXISTS public_projection("
        " closure_key TEXT PRIMARY KEY, source_fingerprint TEXT NOT NULL,"
        " response BLOB NOT NULL, response_digest TEXT NOT NULL,"
        " compiled_at INTEGER NOT NULL);"
        "CREATE TABLE IF NOT EXISTS public_projection_dependency("
        " closure_key TEXT NOT NULL REFERENCES public_projection(closure_key) ON DELETE CASCADE,"
        " dependency TEXT NOT NULL, PRIMARY KEY(closure_key,dependency));"
        "CREATE INDEX IF NOT EXISTS public_projection_dependency_lookup"
        " ON public_projection_dependency(dependency);", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    return db;
}

int bf_public_projection_cache_read(const char *path, const char *key,
                                    const char *fingerprint, FILE *out) {
    sqlite3 *db;
    sqlite3_stmt *st = NULL;
    const void *body;
    int bytes;
    int result = 0;
    if (!path || !*path || !key || !*key || !fingerprint || !out) return 0;
    db = open_cache(path, 0, 2000);
    if (!db) return -1;
    if (sqlite3_prepare_v2(db,
        /* A lowerer handoff retires a body immediately.  This short source-TTL
         * is the independent backstop: a dropped handoff may delay a public
         * refresh, but cannot make a derived response permanently current. */
        "SELECT response FROM public_projection WHERE closure_key=?1 AND source_fingerprint=?2"
        " AND compiled_at>=?3",
        -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, fingerprint, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 3, (sqlite3_int64)time(NULL) - 60);
        if (sqlite3_step(st) == SQLITE_ROW) {
            body = sqlite3_column_blob(st, 0);
            bytes = sqlite3_column_bytes(st, 0);
            if (body && bytes > 0 && fwrite(body, 1, (size_t)bytes, out) == (size_t)bytes)
                result = 1;
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    return result;
}

int bf_public_projection_cache_write(const char *path, const char *key,
                                     const char *fingerprint,
                                     const void *response, size_t response_len,
                                     const char *const *dependencies,
                                     size_t dependency_count,
                                     char digest_out[65]) {
    sqlite3 *db;
    sqlite3_stmt *st = NULL;
    char digest[65];
    int ok = 0;
    if (!path || !*path || !key || !*key || !fingerprint || !response || !response_len ||
        response_len > 16U * 1024U * 1024U) return 0;
    bf_sha256_hex((const uint8_t *)response, response_len, digest);
    /* This disposable optimization must not delay delivery of an already
     * composed response behind another writer. Invalidation still waits. */
    db = open_cache(path, 1, 0);
    if (!db || sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) goto out;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO public_projection(closure_key,source_fingerprint,response,response_digest,compiled_at)"
        " VALUES(?1,?2,?3,?4,?5) ON CONFLICT(closure_key) DO UPDATE SET"
        " source_fingerprint=excluded.source_fingerprint,response=excluded.response,"
        " response_digest=excluded.response_digest,compiled_at=excluded.compiled_at",
        -1, &st, NULL) != SQLITE_OK) goto rollback;
    sqlite3_bind_text(st, 1, key, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, fingerprint, -1, SQLITE_TRANSIENT);
    sqlite3_bind_blob(st, 3, response, (int)response_len, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 5, (sqlite3_int64)time(NULL));
    if (sqlite3_step(st) != SQLITE_DONE) goto rollback;
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_prepare_v2(db, "DELETE FROM public_projection_dependency WHERE closure_key=?1", -1, &st, NULL) != SQLITE_OK) goto rollback;
    sqlite3_bind_text(st, 1, key, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) goto rollback;
    sqlite3_finalize(st); st = NULL;
    if (dependency_count && sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO public_projection_dependency(closure_key,dependency) VALUES(?1,?2)",
        -1, &st, NULL) != SQLITE_OK) goto rollback;
    for (size_t i = 0; i < dependency_count; i++) {
        if (!dependencies[i] || !*dependencies[i]) continue;
        sqlite3_reset(st);
        sqlite3_clear_bindings(st);
        sqlite3_bind_text(st, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, dependencies[i], -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) != SQLITE_DONE) goto rollback;
    }
    if (st) { sqlite3_finalize(st); st = NULL; }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto rollback;
    if (digest_out) memcpy(digest_out, digest, sizeof(digest));
    ok = 1;
    goto out;
rollback:
    if (st) sqlite3_finalize(st);
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
out:
    if (db) sqlite3_close(db);
    return ok;
}

int bf_public_projection_cache_invalidate(const char *path, const char *dependency,
                                          int *invalidated_out) {
    sqlite3 *db;
    sqlite3_stmt *st = NULL;
    int changed = 0;
    if (invalidated_out) *invalidated_out = 0;
    if (!path || !*path || !dependency || !*dependency) return 0;
    db = open_cache(path, 1, 2000);
    if (!db || sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) goto out;
    if (sqlite3_prepare_v2(db,
        "DELETE FROM public_projection WHERE closure_key IN ("
        "SELECT closure_key FROM public_projection_dependency WHERE dependency=?1)",
        -1, &st, NULL) != SQLITE_OK) goto rollback;
    sqlite3_bind_text(st, 1, dependency, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) goto rollback;
    changed = sqlite3_changes(db);
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto rollback;
    if (invalidated_out) *invalidated_out = changed;
    sqlite3_close(db);
    return 1;
rollback:
    if (st) sqlite3_finalize(st);
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
out:
    if (db) sqlite3_close(db);
    return 0;
}
