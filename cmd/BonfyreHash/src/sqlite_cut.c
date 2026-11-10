#define _POSIX_C_SOURCE 200809L
#include "sqlite_cut.h"
#include "bonfyre.h"
#include <sqlite3.h>
#include <float.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

/* BFSQL1 binds DDL, visible columns and each storage-class value in the native
 * table scan. It is a cursor-order cut, not unordered relational equivalence.
 * Byte spans are consumed before sqlite3_step invalidates their storage. */
static void cut_u64(BfSha256 *h, uint64_t n) {
    unsigned char b[8];
    for (int i = 7; i >= 0; --i) { b[i] = (unsigned char)n; n >>= 8; }
    bf_sha256_update(h, b, sizeof b);
}
static void cut_span(BfSha256 *h, const void *p, size_t n) {
    cut_u64(h, (uint64_t)n);
    if (n) bf_sha256_update(h, p, n);
}
static int cut_value(BfSha256 *h, sqlite3_stmt *q, int i) {
    sqlite3 *db = sqlite3_db_handle(q);
    int type = sqlite3_column_type(q, i); /* Before any conversion. */
    unsigned char tag = (unsigned char)type;
    bf_sha256_update(h, &tag, 1);
    switch (type) {
    case SQLITE_NULL: return 1;
    case SQLITE_INTEGER:
        cut_u64(h, (uint64_t)sqlite3_column_int64(q, i)); return 1;
    case SQLITE_FLOAT: {
        double value = sqlite3_column_double(q, i); uint64_t bits;
        memcpy(&bits, &value, sizeof bits); cut_u64(h, bits); return 1;
    }
    case SQLITE_TEXT: case SQLITE_BLOB: {
        const void *p = type == SQLITE_TEXT
            ? (const void *)sqlite3_column_text(q, i) : sqlite3_column_blob(q, i);
        if (!p && sqlite3_errcode(db) == SQLITE_NOMEM) return 0;
        int n = sqlite3_column_bytes(q, i);
        if (sqlite3_errcode(db) == SQLITE_NOMEM || n < 0 || (n && !p)) return 0;
        cut_span(h, p, (size_t)n); return 1;
    }
    default: return 0;
    }
}
int bf_hash_sqlite_table(const char *path, const char *table) {
    sqlite3 *db = NULL; sqlite3_stmt *q = NULL; char *sql = NULL;
    struct stat st; BfSha256 hash; unsigned char digest[32]; char hex[65];
    uint64_t rows = 0; int columns = 0, code, ok = 0;
    const char *failure = "table cut refused";
    if (!path || !*path || !table || !*table || strlen(table) > 512 ||
        lstat(path, &st) != 0 || !S_ISREG(st.st_mode)) goto done;
    if (sizeof(double) != 8 || DBL_MANT_DIG != 53 || DBL_MAX_EXP != 1024 ||
        sqlite3_libversion_number() < 3037000) {
        failure = "unsupported native SQLite or floating-point ABI"; goto done;
    }
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOFOLLOW,
                        NULL) != SQLITE_OK) goto done;
    if (sqlite3_db_readonly(db, "main") != 1) goto done;
    sqlite3_busy_timeout(db, 2000);
    sqlite3_limit(db, SQLITE_LIMIT_LENGTH, 64 * 1024 * 1024);
    if (sqlite3_exec(db, "PRAGMA query_only=ON; PRAGMA trusted_schema=OFF; "
                        "PRAGMA cache_size=-4096; BEGIN;", NULL, NULL, NULL)
        != SQLITE_OK) goto done;
    /* Views, virtual tables and extension shadow tables are not native cuts. */
    if (sqlite3_prepare_v2(db, "SELECT s.sql FROM main.sqlite_schema s "
        "JOIN pragma_table_list t ON t.schema='main' AND t.name=s.name "
        "WHERE s.name=?1 AND s.type='table' AND t.type='table' "
        "AND s.name NOT LIKE 'sqlite_%'", -1, &q, NULL) != SQLITE_OK) goto done;
    if (sqlite3_bind_text(q, 1, table, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_step(q) != SQLITE_ROW || sqlite3_column_type(q, 0) != SQLITE_TEXT)
        goto done;
    bf_sha256_init(&hash);
    cut_span(&hash, "bonfyre.sqlite-table-cut.v1", sizeof("bonfyre.sqlite-table-cut.v1") - 1);
    if (!cut_value(&hash, q, 0) || sqlite3_step(q) != SQLITE_DONE) goto done;
    code = sqlite3_finalize(q); q = NULL; if (code != SQLITE_OK) goto done;
    /* NOT INDEXED prevents covering-index choice from changing the cut order.
     * Ordinary rowid tables and WITHOUT ROWID primary-key scans are supported. */
    sql = sqlite3_mprintf("SELECT * FROM main.\"%w\" NOT INDEXED", table);
    if (!sql || sqlite3_prepare_v2(db, sql, -1, &q, NULL) != SQLITE_OK) goto done;
    sqlite3_free(sql); sql = NULL;
    columns = sqlite3_column_count(q); if (columns < 1) goto done;
    cut_u64(&hash, (uint64_t)columns);
    for (int i = 0; i < columns; ++i) {
        const char *name = sqlite3_column_name(q, i);
        const char *decl = sqlite3_column_decltype(q, i);
        if (!name || sqlite3_errcode(db) == SQLITE_NOMEM) goto done;
        cut_span(&hash, name, strlen(name));
        cut_span(&hash, decl ? decl : "", decl ? strlen(decl) : 0);
    }
    while ((code = sqlite3_step(q)) == SQLITE_ROW) {
        const unsigned char marker = 'R';
        bf_sha256_update(&hash, &marker, 1);
        if (rows == UINT64_MAX) goto done;
        for (int i = 0; i < columns; ++i) if (!cut_value(&hash, q, i)) goto done;
        ++rows;
    }
    if (code != SQLITE_DONE) goto done;
    code = sqlite3_finalize(q); q = NULL; if (code != SQLITE_OK) goto done;
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto done;
    if (sqlite3_close(db) != SQLITE_OK) goto done;
    db = NULL;
    const unsigned char marker = 'E';
    bf_sha256_update(&hash, &marker, 1); cut_u64(&hash, rows);
    bf_sha256_final(&hash, digest); bf_sha256_digest_hex(digest, hex);
    /* No source values, path or table name are emitted. Failure emits no cut. */
    if (printf("BFSQL1\t%s\t%" PRIu64 "\t%d\n", hex, rows, columns) < 0 ||
        fflush(stdout) != 0) goto done;
    ok = 1;
done:
    sqlite3_free(sql);
    if (q) sqlite3_finalize(q);
    if (db) sqlite3_close(db);
    if (!ok) fprintf(stderr, "bonfyre-hash: %s\n", failure);
    return ok ? 0 : 1;
}
