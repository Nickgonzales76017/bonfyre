/*
 * Cross-references the discovery-index catalog (catalog.db: models/
 * surfaces/transports/families/recipes -- no generation concept of its
 * own) with a specific compiled fabric's catalog_generation hash (a
 * fabric.db's fabric_meta table -- the governed executable-dispatch
 * catalog's real source of truth, see catalog_bindings/
 * operator_contract_bindings in engine/core).
 *
 * The two catalogs describe the same underlying estate yaff/tsv source
 * files but live at different paths (catalog.db is a stable global path;
 * fabric.db is ephemeral, per BONFYRE_STATE_DIR) -- this is the explicit
 * bridge between them, letting a caller ask "does this discovery index
 * reflect the executable catalog I just compiled?" without requiring
 * either database to know about the other's location by default.
 *
 * Deliberately self-contained -- no dependency on bf_catalog.c's fts5/json
 * machinery, and no dependency on bf_sqlite.c/bf_common.c either -- so
 * engine/core, which links only a handful of individual libbonfyre object
 * files rather than the whole library, can pull this one in standalone
 * the same way it already pulls in bf_sha256.o. Uses plain sqlite3_open_v2
 * instead of bf_sqlite3_open's tuned-PRAGMA wrapper: this runs once per
 * `catalog stamp-generation` invocation, not a hot path.
 */
#include <errno.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define BF_CATALOG_DB_SUBPATH "/.local/share/bonfyre/catalog.db"

static void default_catalog_db_path(char *buf, size_t sz) {
    const char *env = getenv("BONFYRE_CATALOG_DB");
    const char *home = getenv("HOME");
    if (env && env[0]) {
        snprintf(buf, sz, "%s", env);
        return;
    }
    if (!home) home = "/tmp";
    snprintf(buf, sz, "%s%s", home, BF_CATALOG_DB_SUBPATH);
}

/* Minimal `mkdir -p` -- avoids pulling in bf_common.c's bf_ensure_dir for
 * one call site. */
static int ensure_dir(const char *path) {
    char tmp[PATH_MAX];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    memcpy(tmp, path, len + 1);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    return mkdir(tmp, 0755) == 0 || errno == EEXIST ? 0 : 1;
}

int bf_catalog_stamp_generation(const char *db_path, const char *fabric_db_path) {
    sqlite3 *fabric_db = NULL;
    sqlite3 *catalog_db = NULL;
    sqlite3_stmt *statement = NULL;
    char resolved_db[PATH_MAX];
    char db_dir[PATH_MAX];
    char *slash;
    char generation[128] = "";
    char timestamp[32];
    int ok = 0;

    if (!fabric_db_path || !fabric_db_path[0]) return 1;
    if (sqlite3_open_v2(fabric_db_path, &fabric_db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(fabric_db);
        return 1;
    }
    if (sqlite3_prepare_v2(fabric_db,
            "SELECT value FROM fabric_meta WHERE key='catalog_generation'",
            -1, &statement, NULL) == SQLITE_OK) {
        if (sqlite3_step(statement) == SQLITE_ROW) {
            const char *value = (const char *)sqlite3_column_text(statement, 0);
            if (value) snprintf(generation, sizeof(generation), "%s", value);
        }
    }
    sqlite3_finalize(statement);
    sqlite3_close(fabric_db);
    if (!generation[0]) return 1;

    if (db_path && db_path[0]) snprintf(resolved_db, sizeof(resolved_db), "%s", db_path);
    else default_catalog_db_path(resolved_db, sizeof(resolved_db));
    snprintf(db_dir, sizeof(db_dir), "%s", resolved_db);
    slash = strrchr(db_dir, '/');
    if (slash) {
        *slash = '\0';
        if (ensure_dir(db_dir) != 0) return 1;
    }
    if (sqlite3_open_v2(resolved_db, &catalog_db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
        sqlite3_close(catalog_db);
        return 1;
    }
    if (sqlite3_exec(catalog_db,
            "CREATE TABLE IF NOT EXISTS catalog_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
            NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(catalog_db);
        return 1;
    }
    snprintf(timestamp, sizeof(timestamp), "%ld", (long)time(NULL));
    statement = NULL;
    if (sqlite3_prepare_v2(catalog_db,
            "INSERT INTO catalog_meta(key,value) VALUES('fabric_catalog_generation',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, generation, -1, SQLITE_TRANSIENT);
        ok = sqlite3_step(statement) == SQLITE_DONE;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (ok && sqlite3_prepare_v2(catalog_db,
            "INSERT INTO catalog_meta(key,value) VALUES('fabric_catalog_generation_stamped_at',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, timestamp, -1, SQLITE_TRANSIENT);
        ok = sqlite3_step(statement) == SQLITE_DONE;
    }
    sqlite3_finalize(statement);
    sqlite3_close(catalog_db);
    return ok ? 0 : 1;
}
