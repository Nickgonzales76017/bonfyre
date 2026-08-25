/*
 * Native resource-activation state machine. Shares the resource_candidates store
 * with resource_activation.py; the ordered state derivation matches exactly.
 */

#include "bf_activation.h"

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>
#include <time.h>

struct BfActivation {
    sqlite3 *db;
    int owns_db;
    FILE *err;
};

/* Must match resource_activation.py SCHEMA exactly. */
static const char *ACTIVATION_SCHEMA =
    "CREATE TABLE IF NOT EXISTS resource_candidates("
    "  resource_id TEXT PRIMARY KEY,"
    "  mechanism TEXT NOT NULL DEFAULT 'service',"
    "  qualified INTEGER NOT NULL DEFAULT 0,"
    "  eligible INTEGER NOT NULL DEFAULT 0,"
    "  activate_actor TEXT NOT NULL DEFAULT '',"
    "  detail TEXT NOT NULL DEFAULT '',"
    "  updated_at TEXT NOT NULL"
    ");";

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
    if (sqlite3_exec(db, ACTIVATION_SCHEMA, NULL, NULL, &errmsg) != SQLITE_OK) {
        if (err) {
            fprintf(err, "activation: schema failed: %s\n", errmsg ? errmsg : "?");
        }
        sqlite3_free(errmsg);
        return -1;
    }
    return 0;
}

int bf_activation_open(const char *database_path, BfActivation **out, FILE *err) {
    if (!database_path || !out) {
        return -1;
    }
    sqlite3 *db = NULL;
    if (sqlite3_open(database_path, &db) != SQLITE_OK) {
        if (err) {
            fprintf(err, "activation: cannot open %s: %s\n", database_path,
                    db ? sqlite3_errmsg(db) : "?");
        }
        sqlite3_close(db);
        return -1;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (apply_schema(db, err) != 0) {
        sqlite3_close(db);
        return -1;
    }
    BfActivation *act = calloc(1, sizeof(*act));
    if (!act) {
        sqlite3_close(db);
        return -1;
    }
    act->db = db;
    act->owns_db = 1;
    act->err = err;
    *out = act;
    return 0;
}

int bf_activation_open_database(void *sqlite_database, BfActivation **out, FILE *err) {
    if (!sqlite_database || !out) {
        return -1;
    }
    sqlite3 *db = (sqlite3 *)sqlite_database;
    if (apply_schema(db, err) != 0) {
        return -1;
    }
    BfActivation *act = calloc(1, sizeof(*act));
    if (!act) {
        return -1;
    }
    act->db = db;
    act->owns_db = 0;
    act->err = err;
    *out = act;
    return 0;
}

void bf_activation_close(BfActivation *act) {
    if (!act) {
        return;
    }
    if (act->owns_db && act->db) {
        sqlite3_close(act->db);
    }
    free(act);
}

int bf_activation_record(BfActivation *act, const BfActivationCandidate *cand, const char *now) {
    if (!act || !cand || !cand->resource_id || cand->resource_id[0] == '\0') {
        return -1;
    }
    char stamp[40];
    if (!now || now[0] == '\0') {
        now_iso(stamp);
        now = stamp;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            act->db,
            "INSERT INTO resource_candidates"
            "(resource_id,mechanism,qualified,eligible,activate_actor,detail,updated_at)"
            " VALUES(?,?,?,?,?,?,?)"
            " ON CONFLICT(resource_id) DO UPDATE SET mechanism=excluded.mechanism,"
            "  qualified=excluded.qualified, eligible=excluded.eligible,"
            "  activate_actor=excluded.activate_actor, detail=excluded.detail,"
            "  updated_at=excluded.updated_at",
            -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, cand->resource_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, cand->mechanism ? cand->mechanism : "service", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 3, cand->qualified ? 1 : 0);
    sqlite3_bind_int(stmt, 4, cand->eligible ? 1 : 0);
    sqlite3_bind_text(stmt, 5, cand->activate_actor ? cand->activate_actor : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, cand->detail ? cand->detail : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 7, now, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

BfActivationState bf_activation_state(BfActivation *act, const char *resource_id,
                                      int has_authority, int is_bound) {
    if (!act || !resource_id) {
        return BF_ACT_GAP;
    }
    sqlite3_stmt *stmt = NULL;
    int found = 0, qualified = 0, eligible = 0;
    const char *actor = NULL;
    char actor_buf[256] = {0};
    if (sqlite3_prepare_v2(
            act->db,
            "SELECT qualified,eligible,activate_actor FROM resource_candidates WHERE resource_id=?",
            -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, resource_id, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            found = 1;
            qualified = sqlite3_column_int(stmt, 0);
            eligible = sqlite3_column_int(stmt, 1);
            actor = (const char *)sqlite3_column_text(stmt, 2);
            if (actor) {
                snprintf(actor_buf, sizeof(actor_buf), "%s", actor);
            }
        }
    }
    sqlite3_finalize(stmt);

    if (!found) {
        return BF_ACT_GAP;
    }
    if (!qualified) {
        return BF_ACT_CANDIDATE;
    }
    if (!eligible) {
        return BF_ACT_QUALIFIED;
    }
    /* the activate-authority gate: needs a named actor AND authority over it.
     * has_authority is the caller's authority-check result. */
    if (!(actor_buf[0] != '\0' && has_authority)) {
        return BF_ACT_ELIGIBLE;
    }
    if (!is_bound) {
        return BF_ACT_AUTHORIZED;
    }
    return BF_ACT_ACTIVATED;
}

int bf_activation_is_activated(BfActivation *act, const char *resource_id,
                               int has_authority, int is_bound) {
    if (!act || !resource_id) {
        return -1;
    }
    return bf_activation_state(act, resource_id, has_authority, is_bound) == BF_ACT_ACTIVATED ? 1 : 0;
}

const char *bf_activation_state_name(BfActivationState state) {
    switch (state) {
        case BF_ACT_GAP: return "gap";
        case BF_ACT_CANDIDATE: return "candidate";
        case BF_ACT_QUALIFIED: return "qualified";
        case BF_ACT_ELIGIBLE: return "eligible";
        case BF_ACT_AUTHORIZED: return "authorized";
        case BF_ACT_ACTIVATED: return "activated";
    }
    return "unknown";
}
