/*
 * Native proof frontier. Shares frontier_layers / solved_invariants /
 * known_noncauses with proof_frontier.py. Preserves the truth-plane discipline
 * (measured != proven) and challenge-retraction.
 */

#include "bf_proof.h"

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>
#include <time.h>

struct BfProof {
    sqlite3 *db;
    int owns_db;
    FILE *err;
};

/* Must match proof_frontier.py SCHEMA exactly. */
static const char *PROOF_SCHEMA =
    "CREATE TABLE IF NOT EXISTS solved_invariants("
    "  invariant_id TEXT PRIMARY KEY,"
    "  subject_resource TEXT NOT NULL,"
    "  subject_profile TEXT NOT NULL DEFAULT '',"
    "  subject_hashes TEXT NOT NULL DEFAULT '[]',"
    "  layer TEXT NOT NULL,"
    "  statement TEXT NOT NULL,"
    "  truth_plane TEXT NOT NULL DEFAULT 'measured',"
    "  status TEXT NOT NULL DEFAULT 'cooled',"
    "  proof_refs TEXT NOT NULL DEFAULT '[]',"
    "  reheat_conditions TEXT NOT NULL DEFAULT '[]',"
    "  proven_at TEXT,"
    "  updated_at TEXT NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS known_noncauses("
    "  noncause_id TEXT PRIMARY KEY,"
    "  hypothesis TEXT NOT NULL,"
    "  subject_scope TEXT NOT NULL,"
    "  experiment TEXT NOT NULL,"
    "  witness_ref TEXT NOT NULL DEFAULT '',"
    "  invalidation_conditions TEXT NOT NULL DEFAULT '[]',"
    "  recorded_at TEXT NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS frontier_layers("
    "  subject_resource TEXT NOT NULL,"
    "  subject_profile TEXT NOT NULL DEFAULT '',"
    "  ordinal INTEGER NOT NULL,"
    "  layer TEXT NOT NULL,"
    "  status TEXT NOT NULL DEFAULT 'untested',"
    "  witness_ref TEXT NOT NULL DEFAULT '',"
    "  detail TEXT NOT NULL DEFAULT '',"
    "  PRIMARY KEY(subject_resource, subject_profile, layer)"
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
    if (sqlite3_exec(db, PROOF_SCHEMA, NULL, NULL, &errmsg) != SQLITE_OK) {
        if (err) {
            fprintf(err, "proof: schema failed: %s\n", errmsg ? errmsg : "?");
        }
        sqlite3_free(errmsg);
        return -1;
    }
    return 0;
}

int bf_proof_open(const char *database_path, BfProof **out, FILE *err) {
    if (!database_path || !out) {
        return -1;
    }
    sqlite3 *db = NULL;
    if (sqlite3_open(database_path, &db) != SQLITE_OK) {
        if (err) {
            fprintf(err, "proof: cannot open %s: %s\n", database_path,
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
    BfProof *proof = calloc(1, sizeof(*proof));
    if (!proof) {
        sqlite3_close(db);
        return -1;
    }
    proof->db = db;
    proof->owns_db = 1;
    proof->err = err;
    *out = proof;
    return 0;
}

int bf_proof_open_database(void *sqlite_database, BfProof **out, FILE *err) {
    if (!sqlite_database || !out) {
        return -1;
    }
    sqlite3 *db = (sqlite3 *)sqlite_database;
    if (apply_schema(db, err) != 0) {
        return -1;
    }
    BfProof *proof = calloc(1, sizeof(*proof));
    if (!proof) {
        return -1;
    }
    proof->db = db;
    proof->owns_db = 0;
    proof->err = err;
    *out = proof;
    return 0;
}

void bf_proof_close(BfProof *proof) {
    if (!proof) {
        return;
    }
    if (proof->owns_db && proof->db) {
        sqlite3_close(proof->db);
    }
    free(proof);
}

int bf_proof_record_layer(BfProof *proof, const char *subject_resource,
                          const char *subject_profile, int ordinal, const char *layer,
                          const char *status, const char *witness_ref) {
    if (!proof || !subject_resource || subject_resource[0] == '\0' ||
        !layer || layer[0] == '\0' || !status || status[0] == '\0') {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            proof->db,
            "INSERT INTO frontier_layers"
            "(subject_resource,subject_profile,ordinal,layer,status,witness_ref)"
            " VALUES(?,?,?,?,?,?)"
            " ON CONFLICT(subject_resource,subject_profile,layer) DO UPDATE SET"
            "  ordinal=excluded.ordinal, status=excluded.status,"
            "  witness_ref=excluded.witness_ref",
            -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, subject_resource, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, subject_profile ? subject_profile : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 3, ordinal);
    sqlite3_bind_text(stmt, 4, layer, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, status, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, witness_ref ? witness_ref : "", -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

int bf_proof_layer_is_proven(BfProof *proof, const char *subject_resource,
                             const char *subject_profile, const char *layer) {
    if (!proof || !subject_resource || !layer) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    int proven = 0;
    if (sqlite3_prepare_v2(
            proof->db,
            "SELECT status FROM frontier_layers"
            " WHERE subject_resource=? AND subject_profile=? AND layer=?",
            -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, subject_resource, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, subject_profile ? subject_profile : "", -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 3, layer, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *status = (const char *)sqlite3_column_text(stmt, 0);
            proven = (status && strcmp(status, "proven") == 0) ? 1 : 0;
        }
    }
    sqlite3_finalize(stmt);
    return proven;
}

int bf_proof_record_invariant(BfProof *proof, const char *invariant_id,
                              const char *subject_resource, const char *subject_profile,
                              const char *layer, const char *statement,
                              const char *truth_plane, const char *status, const char *now) {
    if (!proof || !invariant_id || invariant_id[0] == '\0' ||
        !subject_resource || !layer || !statement) {
        return -1;
    }
    char stamp[40];
    if (!now || now[0] == '\0') {
        now_iso(stamp);
        now = stamp;
    }
    const char *tp = (truth_plane && truth_plane[0]) ? truth_plane : "measured";
    const char *st = (status && status[0]) ? status : "cooled";
    const char *proven_at = strcmp(tp, "proven") == 0 ? now : NULL;
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            proof->db,
            "INSERT INTO solved_invariants"
            "(invariant_id,subject_resource,subject_profile,layer,statement,truth_plane,"
            " status,proven_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(invariant_id) DO UPDATE SET truth_plane=excluded.truth_plane,"
            "  status=excluded.status, proven_at=excluded.proven_at, updated_at=excluded.updated_at",
            -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, invariant_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, subject_resource, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, subject_profile ? subject_profile : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, layer, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, statement, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, tp, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 7, st, -1, SQLITE_TRANSIENT);
    if (proven_at) {
        sqlite3_bind_text(stmt, 8, proven_at, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(stmt, 8);
    }
    sqlite3_bind_text(stmt, 9, now, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

int bf_proof_challenge_invariant(BfProof *proof, const char *invariant_id, const char *now) {
    if (!proof || !invariant_id) {
        return -1;
    }
    char stamp[40];
    if (!now || now[0] == '\0') {
        now_iso(stamp);
        now = stamp;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            proof->db,
            "UPDATE solved_invariants SET status='challenged', updated_at=?"
            " WHERE invariant_id=? AND status!='challenged'",
            -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, now, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, invariant_id, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    int changed = sqlite3_changes(proof->db);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return -1;
    }
    return changed > 0 ? 1 : 0;
}

int bf_proof_invariant_is_proven(BfProof *proof, const char *invariant_id) {
    if (!proof || !invariant_id) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    int proven = 0;
    if (sqlite3_prepare_v2(
            proof->db,
            "SELECT truth_plane,status FROM solved_invariants WHERE invariant_id=?",
            -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, invariant_id, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *tp = (const char *)sqlite3_column_text(stmt, 0);
            const char *st = (const char *)sqlite3_column_text(stmt, 1);
            /* the discipline: proven requires the proven truth plane AND not
             * challenged/invalidated. A measured result is never proven. */
            int tp_proven = tp && strcmp(tp, "proven") == 0;
            int retracted = st && (strcmp(st, "challenged") == 0 ||
                                   strcmp(st, "invalidated") == 0);
            proven = (tp_proven && !retracted) ? 1 : 0;
        }
    }
    sqlite3_finalize(stmt);
    return proven;
}

int bf_proof_record_noncause(BfProof *proof, const char *noncause_id,
                             const char *hypothesis, const char *subject_scope,
                             const char *experiment, const char *witness_ref, const char *now) {
    if (!proof || !noncause_id || noncause_id[0] == '\0' ||
        !hypothesis || hypothesis[0] == '\0' || !subject_scope || !experiment) {
        return -1;
    }
    char stamp[40];
    if (!now || now[0] == '\0') {
        now_iso(stamp);
        now = stamp;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            proof->db,
            "INSERT INTO known_noncauses"
            "(noncause_id,hypothesis,subject_scope,experiment,witness_ref,recorded_at)"
            " VALUES(?,?,?,?,?,?)"
            " ON CONFLICT(noncause_id) DO UPDATE SET witness_ref=excluded.witness_ref",
            -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, noncause_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, hypothesis, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, subject_scope, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, experiment, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, witness_ref ? witness_ref : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, now, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

int bf_proof_is_blackholed(BfProof *proof, const char *hypothesis, const char *subject_scope) {
    if (!proof || !hypothesis || !subject_scope) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    int found = 0;
    if (sqlite3_prepare_v2(
            proof->db,
            "SELECT 1 FROM known_noncauses WHERE hypothesis=? AND subject_scope=?",
            -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, hypothesis, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, subject_scope, -1, SQLITE_STATIC);
        found = (sqlite3_step(stmt) == SQLITE_ROW) ? 1 : 0;
    }
    sqlite3_finalize(stmt);
    return found;
}
