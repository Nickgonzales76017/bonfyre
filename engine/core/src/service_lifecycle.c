#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "bf_service_lifecycle.h"

#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct BfServiceLifecycle {
    sqlite3 *db;
    int owns_db;
    FILE *err;
};

static const char *SERVICE_LIFECYCLE_SCHEMA =
    "CREATE TABLE IF NOT EXISTS service_bindings("
    " resource_id TEXT PRIMARY KEY,"
    " service_id TEXT NOT NULL,"
    " actor TEXT NOT NULL,"
    " state TEXT NOT NULL CHECK(state IN ('bound','released')),"
    " evidence TEXT NOT NULL,"
    " bound_at TEXT NOT NULL,"
    " released_at TEXT"
    ");"
    "CREATE INDEX IF NOT EXISTS service_bindings_actor_state "
    "ON service_bindings(actor,state);";

static void now_iso(char out[40]) {
    time_t t = time(NULL);
    struct tm utc;
    gmtime_r(&t, &utc);
    if (!strftime(out, 40, "%Y-%m-%dT%H:%M:%S+00:00", &utc))
        snprintf(out, 40, "1970-01-01T00:00:00+00:00");
}

static int apply_schema(sqlite3 *db, FILE *err) {
    char *message = NULL;
    if (sqlite3_exec(db, SERVICE_LIFECYCLE_SCHEMA, NULL, NULL, &message) != SQLITE_OK) {
        if (err) fprintf(err, "service-lifecycle: schema failed: %s\n", message ? message : "?");
        sqlite3_free(message);
        return -1;
    }
    return 0;
}

int bf_service_lifecycle_open(const char *database_path, BfServiceLifecycle **out,
                              FILE *err) {
    sqlite3 *db = NULL;
    BfServiceLifecycle *lifecycle;
    if (!database_path || !out || sqlite3_open(database_path, &db) != SQLITE_OK) {
        if (err) fprintf(err, "service-lifecycle: cannot open database\n");
        sqlite3_close(db);
        return -1;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
    if (apply_schema(db, err) != 0) { sqlite3_close(db); return -1; }
    lifecycle = calloc(1, sizeof(*lifecycle));
    if (!lifecycle) { sqlite3_close(db); return -1; }
    lifecycle->db = db;
    lifecycle->owns_db = 1;
    lifecycle->err = err;
    *out = lifecycle;
    return 0;
}

int bf_service_lifecycle_open_database(void *sqlite_database,
                                       BfServiceLifecycle **out, FILE *err) {
    BfServiceLifecycle *lifecycle;
    if (!sqlite_database || !out || apply_schema((sqlite3 *)sqlite_database, err) != 0) return -1;
    lifecycle = calloc(1, sizeof(*lifecycle));
    if (!lifecycle) return -1;
    lifecycle->db = sqlite_database;
    lifecycle->err = err;
    *out = lifecycle;
    return 0;
}

void bf_service_lifecycle_close(BfServiceLifecycle *lifecycle) {
    if (!lifecycle) return;
    if (lifecycle->owns_db) sqlite3_close(lifecycle->db);
    free(lifecycle);
}

int bf_service_lifecycle_bind(BfServiceLifecycle *lifecycle, BfAuthority *authority,
                              const BfServiceBinding *binding, const char *at_iso) {
    sqlite3_stmt *statement = NULL;
    char stamp[40];
    int rc;
    if (!lifecycle || !authority || !binding || !binding->resource_id || !binding->resource_id[0] ||
        !binding->service_id || !binding->service_id[0] || !binding->actor || !binding->actor[0] ||
        !binding->evidence || !binding->evidence[0]) return -1;
    if (bf_authority_has(authority, binding->actor, "bind", binding->service_id, at_iso,
                         "service-binding") != 1) return 0;
    if (!at_iso || !at_iso[0]) { now_iso(stamp); at_iso = stamp; }
    if (sqlite3_prepare_v2(lifecycle->db,
            "INSERT INTO service_bindings(resource_id,service_id,actor,state,evidence,bound_at,released_at)"
            " VALUES(?,?,?,'bound',?,?,NULL)"
            " ON CONFLICT(resource_id) DO UPDATE SET service_id=excluded.service_id,"
            " actor=excluded.actor,state='bound',evidence=excluded.evidence,bound_at=excluded.bound_at,"
            " released_at=NULL", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, binding->resource_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, binding->service_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, binding->actor, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, binding->evidence, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, at_iso, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(statement) == SQLITE_DONE ? 1 : -1;
    sqlite3_finalize(statement);
    return rc;
}

int bf_service_lifecycle_release(BfServiceLifecycle *lifecycle, const char *resource_id,
                                 const char *actor, const char *at_iso) {
    sqlite3_stmt *statement = NULL;
    char stamp[40];
    int changed;
    if (!lifecycle || !resource_id || !actor) return -1;
    if (!at_iso || !at_iso[0]) { now_iso(stamp); at_iso = stamp; }
    if (sqlite3_prepare_v2(lifecycle->db,
            "UPDATE service_bindings SET state='released',released_at=? "
            "WHERE resource_id=? AND actor=? AND state='bound'", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, at_iso, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, resource_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, actor, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); return -1; }
    changed = sqlite3_changes(lifecycle->db);
    sqlite3_finalize(statement);
    return changed;
}

int bf_service_lifecycle_is_bound(BfServiceLifecycle *lifecycle,
                                  const char *resource_id, const char *actor) {
    sqlite3_stmt *statement = NULL;
    int result = -1;
    if (!lifecycle || !resource_id || !actor) return -1;
    if (sqlite3_prepare_v2(lifecycle->db,
            "SELECT 1 FROM service_bindings WHERE resource_id=? AND actor=? AND state='bound'",
            -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, resource_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, actor, -1, SQLITE_TRANSIENT);
    result = sqlite3_step(statement) == SQLITE_ROW ? 1 : 0;
    sqlite3_finalize(statement);
    return result;
}
