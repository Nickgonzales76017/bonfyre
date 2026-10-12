/*
 * Native corroboration ledger. Shares the actor_corroborations store with the
 * Python reference (verification.py); independence by distinct source.
 */

#include "bf_corroboration.h"

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>
#include <time.h>

struct BfCorroboration {
    sqlite3 *db;
    int owns_db;
    FILE *err;
};

/* Must match verification.py SCHEMA exactly. */
static const char *CORROBORATION_SCHEMA =
    "CREATE TABLE IF NOT EXISTS actor_corroborations("
    "  actor_id TEXT NOT NULL REFERENCES actor_nodes(actor_id),"
    "  source TEXT NOT NULL,"
    "  evidence_ref TEXT NOT NULL DEFAULT '',"
    "  note TEXT NOT NULL DEFAULT '',"
    "  corroborated_at TEXT NOT NULL,"
    "  PRIMARY KEY(actor_id, source)"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_corroboration_actor ON actor_corroborations(actor_id);";

static void now_iso(char out[40]) {
    time_t t = time(NULL);
    struct tm utc;
#if defined(_WIN32)
    gmtime_s(&utc, &t);
#else
    gmtime_r(&t, &utc);
#endif
    if (strftime(out, 40, "%Y-%m-%dT%H:%M:%S+00:00", &utc) == 0) {
        snprintf(out, 40, "1970-01-01T00:00:00+00:00");
    }
}

static int apply_schema(sqlite3 *db, FILE *err) {
    char *errmsg = NULL;
    if (sqlite3_exec(db, CORROBORATION_SCHEMA, NULL, NULL, &errmsg) != SQLITE_OK) {
        if (err) {
            fprintf(err, "corroboration: schema failed: %s\n", errmsg ? errmsg : "?");
        }
        sqlite3_free(errmsg);
        return -1;
    }
    return 0;
}

int bf_corroboration_open(const char *database_path, BfCorroboration **out, FILE *err) {
    if (!database_path || !out) {
        return -1;
    }
    sqlite3 *db = NULL;
    if (sqlite3_open(database_path, &db) != SQLITE_OK) {
        if (err) {
            fprintf(err, "corroboration: cannot open %s: %s\n", database_path,
                    db ? sqlite3_errmsg(db) : "?");
        }
        sqlite3_close(db);
        return -1;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);
    if (apply_schema(db, err) != 0) {
        sqlite3_close(db);
        return -1;
    }
    BfCorroboration *ledger = calloc(1, sizeof(*ledger));
    if (!ledger) {
        sqlite3_close(db);
        return -1;
    }
    ledger->db = db;
    ledger->owns_db = 1;
    ledger->err = err;
    *out = ledger;
    return 0;
}

int bf_corroboration_open_database(void *sqlite_database, BfCorroboration **out, FILE *err) {
    if (!sqlite_database || !out) {
        return -1;
    }
    sqlite3 *db = (sqlite3 *)sqlite_database;
    if (apply_schema(db, err) != 0) {
        return -1;
    }
    BfCorroboration *ledger = calloc(1, sizeof(*ledger));
    if (!ledger) {
        return -1;
    }
    ledger->db = db;
    ledger->owns_db = 0;
    ledger->err = err;
    *out = ledger;
    return 0;
}

void bf_corroboration_close(BfCorroboration *ledger) {
    if (!ledger) {
        return;
    }
    if (ledger->owns_db && ledger->db) {
        sqlite3_close(ledger->db);
    }
    free(ledger);
}

int bf_corroboration_record(BfCorroboration *ledger, const char *actor_id,
                            const char *source, const char *evidence_ref,
                            const char *note, const char *now) {
    if (!ledger || !actor_id || actor_id[0] == '\0' || !source || source[0] == '\0') {
        return -1;
    }
    /* Was this (actor, source) already present? A repeat is not independent. */
    int existed = 0;
    sqlite3_stmt *check = NULL;
    if (sqlite3_prepare_v2(ledger->db,
                           "SELECT 1 FROM actor_corroborations WHERE actor_id=? AND source=?",
                           -1, &check, NULL) == SQLITE_OK) {
        sqlite3_bind_text(check, 1, actor_id, -1, SQLITE_STATIC);
        sqlite3_bind_text(check, 2, source, -1, SQLITE_STATIC);
        existed = (sqlite3_step(check) == SQLITE_ROW);
    }
    sqlite3_finalize(check);

    char stamp[40];
    if (!now || now[0] == '\0') {
        now_iso(stamp);
        now = stamp;
    }
    sqlite3_stmt *ins = NULL;
    if (sqlite3_prepare_v2(
            ledger->db,
            "INSERT INTO actor_corroborations(actor_id,source,evidence_ref,note,corroborated_at)"
            " VALUES(?,?,?,?,?)"
            " ON CONFLICT(actor_id,source) DO UPDATE SET"
            "  evidence_ref=excluded.evidence_ref, note=excluded.note",
            -1, &ins, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(ins, 1, actor_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 2, source, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 3, evidence_ref ? evidence_ref : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 4, note ? note : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 5, now, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(ins);
    sqlite3_finalize(ins);
    if (rc != SQLITE_DONE) {
        return -1;
    }
    return existed ? 0 : 1;
}

int64_t bf_corroboration_independent_sources(BfCorroboration *ledger, const char *actor_id) {
    if (!ledger || !actor_id) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    int64_t n = -1;
    if (sqlite3_prepare_v2(
            ledger->db,
            "SELECT COUNT(DISTINCT source) FROM actor_corroborations WHERE actor_id=?",
            -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, actor_id, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            n = sqlite3_column_int64(stmt, 0);
        }
    }
    sqlite3_finalize(stmt);
    return n;
}
