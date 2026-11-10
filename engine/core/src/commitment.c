/*
 * Native owner for CommitmentGraph.  The archived Python reference remains a
 * readable oracle, but durable promise state and its event-log transition law
 * live here.  Keep ProtocolContract callers moving toward this API: no new
 * store-backed commitment mutation belongs in Python.
 */
#include "bf_commitment.h"

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>

struct BfCommitment { sqlite3 *db; int owns_db; FILE *err; };

static const char *COMMITMENT_SCHEMA =
    "CREATE TABLE IF NOT EXISTS commitments("
    "commitment_id TEXT PRIMARY KEY,committer TEXT NOT NULL,beneficiary TEXT NOT NULL,"
    "resource TEXT NOT NULL,conditions TEXT NOT NULL DEFAULT '',expires_at TEXT NOT NULL DEFAULT '',"
    "authority TEXT NOT NULL DEFAULT '',origin TEXT NOT NULL DEFAULT '',enactment TEXT NOT NULL DEFAULT '');"
    "CREATE TABLE IF NOT EXISTS commitment_events("
    "seq INTEGER PRIMARY KEY AUTOINCREMENT,commitment_id TEXT NOT NULL,state TEXT NOT NULL,"
    "actor TEXT NOT NULL DEFAULT '',detail TEXT NOT NULL DEFAULT '',at TEXT NOT NULL DEFAULT '');"
    "CREATE INDEX IF NOT EXISTS commitment_events_by_id ON commitment_events(commitment_id,seq);";

static int present(const char *value) { return value && value[0]; }

static BfCommitmentStatus apply_schema(sqlite3 *db, FILE *err) {
    char *message = NULL;
    if (sqlite3_exec(db, COMMITMENT_SCHEMA, NULL, NULL, &message) != SQLITE_OK) {
        if (err) fprintf(err, "commitment: schema failed: %s\n", message ? message : "?");
        sqlite3_free(message);
        return BF_COMMITMENT_STORAGE;
    }
    return BF_COMMITMENT_OK;
}

static BfCommitmentStatus copy_text(char *out, size_t out_size, const char *value) {
    size_t size;
    if (!out || out_size == 0) return BF_COMMITMENT_INVALID;
    value = value ? value : "";
    size = strlen(value);
    if (size >= out_size) return BF_COMMITMENT_CAPACITY;
    memcpy(out, value, size + 1);
    return BF_COMMITMENT_OK;
}

const char *bf_commitment_state_name(BfCommitmentState state) {
    switch (state) {
    case BF_COMMITMENT_OPEN: return "open";
    case BF_COMMITMENT_DISCHARGED: return "discharged";
    case BF_COMMITMENT_DELEGATED: return "delegated";
    case BF_COMMITMENT_VIOLATED: return "violated";
    case BF_COMMITMENT_COMPENSATED: return "compensated";
    }
    return NULL;
}

const char *bf_commitment_status_name(BfCommitmentStatus status) {
    switch (status) {
    case BF_COMMITMENT_OK: return "ok";
    case BF_COMMITMENT_INVALID: return "invalid";
    case BF_COMMITMENT_NOT_FOUND: return "not_found";
    case BF_COMMITMENT_EXISTS: return "exists";
    case BF_COMMITMENT_TRANSITION: return "transition";
    case BF_COMMITMENT_STORAGE: return "storage";
    case BF_COMMITMENT_CAPACITY: return "capacity";
    }
    return "unknown";
}

int bf_commitment_open(const char *database_path, BfCommitment **out, FILE *err) {
    sqlite3 *db = NULL;
    BfCommitment *commitment;
    if (!present(database_path) || !out || sqlite3_open(database_path, &db) != SQLITE_OK) {
        sqlite3_close(db); return BF_COMMITMENT_STORAGE;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (apply_schema(db, err) != BF_COMMITMENT_OK) { sqlite3_close(db); return BF_COMMITMENT_STORAGE; }
    commitment = calloc(1, sizeof(*commitment));
    if (!commitment) { sqlite3_close(db); return BF_COMMITMENT_STORAGE; }
    commitment->db = db; commitment->owns_db = 1; commitment->err = err; *out = commitment;
    return BF_COMMITMENT_OK;
}

int bf_commitment_open_database(void *sqlite_database, BfCommitment **out, FILE *err) {
    BfCommitment *commitment;
    if (!sqlite_database || !out || apply_schema(sqlite_database, err) != BF_COMMITMENT_OK) return BF_COMMITMENT_STORAGE;
    commitment = calloc(1, sizeof(*commitment));
    if (!commitment) return BF_COMMITMENT_STORAGE;
    commitment->db = sqlite_database; commitment->err = err; *out = commitment;
    return BF_COMMITMENT_OK;
}

void bf_commitment_close(BfCommitment *commitment) {
    if (!commitment) return;
    if (commitment->owns_db) sqlite3_close(commitment->db);
    free(commitment);
}

static BfCommitmentStatus append_event(BfCommitment *commitment, const char *commitment_id,
                                       BfCommitmentState state, const char *actor,
                                       const char *detail, const char *at) {
    sqlite3_stmt *stmt = NULL;
    const char *state_name = bf_commitment_state_name(state);
    if (!state_name || sqlite3_prepare_v2(commitment->db,
        "INSERT INTO commitment_events(commitment_id,state,actor,detail,at) VALUES(?,?,?,?,?)",
        -1, &stmt, NULL) != SQLITE_OK) return BF_COMMITMENT_STORAGE;
    sqlite3_bind_text(stmt, 1, commitment_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, state_name, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, actor ? actor : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, detail ? detail : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, at ? at : "", -1, SQLITE_TRANSIENT);
    int done = sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    return done ? BF_COMMITMENT_OK : BF_COMMITMENT_STORAGE;
}

BfCommitmentStatus bf_commitment_record(BfCommitment *commitment, const BfCommitmentSpec *spec) {
    sqlite3_stmt *stmt = NULL;
    BfCommitmentStatus status;
    if (!commitment || !spec || !present(spec->commitment_id) || !present(spec->committer) ||
        !present(spec->beneficiary) || !present(spec->resource)) return BF_COMMITMENT_INVALID;
    if (sqlite3_prepare_v2(commitment->db,
        "INSERT INTO commitments(commitment_id,committer,beneficiary,resource,conditions,expires_at,authority,origin,enactment) VALUES(?,?,?,?,?,?,?,?,?)",
        -1, &stmt, NULL) != SQLITE_OK) return BF_COMMITMENT_STORAGE;
    sqlite3_bind_text(stmt, 1, spec->commitment_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, spec->committer, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, spec->beneficiary, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, spec->resource, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, spec->conditions ? spec->conditions : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, spec->expires_at ? spec->expires_at : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 7, spec->authority ? spec->authority : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 8, present(spec->origin) ? spec->origin : spec->committer, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 9, spec->enactment ? spec->enactment : "", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        int duplicate = sqlite3_errcode(commitment->db) == SQLITE_CONSTRAINT;
        sqlite3_finalize(stmt); return duplicate ? BF_COMMITMENT_EXISTS : BF_COMMITMENT_STORAGE;
    }
    sqlite3_finalize(stmt);
    status = append_event(commitment, spec->commitment_id, BF_COMMITMENT_OPEN,
                          spec->committer, spec->resource, spec->at);
    return status;
}

static BfCommitmentState state_from_name(const char *state) {
    BfCommitmentState value;
    for (value = BF_COMMITMENT_OPEN; value <= BF_COMMITMENT_COMPENSATED; value++)
        if (strcmp(state, bf_commitment_state_name(value)) == 0) return value;
    return (BfCommitmentState)-1;
}

BfCommitmentStatus bf_commitment_status(BfCommitment *commitment, const char *commitment_id,
                                        BfCommitmentState *out, int *out_found) {
    sqlite3_stmt *stmt = NULL;
    if (!commitment || !present(commitment_id) || !out || !out_found) return BF_COMMITMENT_INVALID;
    *out_found = 0;
    if (sqlite3_prepare_v2(commitment->db,
        "SELECT state FROM commitment_events WHERE commitment_id=? ORDER BY seq DESC LIMIT 1",
        -1, &stmt, NULL) != SQLITE_OK) return BF_COMMITMENT_STORAGE;
    sqlite3_bind_text(stmt, 1, commitment_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        *out = state_from_name((const char *)sqlite3_column_text(stmt, 0));
        *out_found = 1;
    }
    sqlite3_finalize(stmt);
    return BF_COMMITMENT_OK;
}

BfCommitmentStatus bf_commitment_was_ever(BfCommitment *commitment, const char *commitment_id,
                                          BfCommitmentState state, int *out) {
    sqlite3_stmt *stmt = NULL;
    const char *name = bf_commitment_state_name(state);
    if (!commitment || !present(commitment_id) || !name || !out) return BF_COMMITMENT_INVALID;
    if (sqlite3_prepare_v2(commitment->db,
        "SELECT 1 FROM commitment_events WHERE commitment_id=? AND state=? LIMIT 1", -1, &stmt, NULL) != SQLITE_OK) return BF_COMMITMENT_STORAGE;
    sqlite3_bind_text(stmt, 1, commitment_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, name, -1, SQLITE_STATIC);
    *out = sqlite3_step(stmt) == SQLITE_ROW; sqlite3_finalize(stmt); return BF_COMMITMENT_OK;
}

BfCommitmentStatus bf_commitment_delivered(BfCommitment *commitment, const char *commitment_id, int *out) {
    BfCommitmentState state = BF_COMMITMENT_OPEN; int found = 0;
    BfCommitmentStatus status;
    if (!out) return BF_COMMITMENT_INVALID;
    status = bf_commitment_status(commitment, commitment_id, &state, &found);
    if (status != BF_COMMITMENT_OK) return status;
    *out = found && state == BF_COMMITMENT_DISCHARGED;
    return BF_COMMITMENT_OK;
}

static int allowed(BfCommitmentState from, BfCommitmentState to) {
    return (from == BF_COMMITMENT_OPEN && (to == BF_COMMITMENT_DISCHARGED || to == BF_COMMITMENT_DELEGATED || to == BF_COMMITMENT_VIOLATED)) ||
           (from == BF_COMMITMENT_DELEGATED && (to == BF_COMMITMENT_DISCHARGED || to == BF_COMMITMENT_VIOLATED)) ||
           (from == BF_COMMITMENT_VIOLATED && (to == BF_COMMITMENT_DISCHARGED || to == BF_COMMITMENT_COMPENSATED));
}

BfCommitmentStatus bf_commitment_transition(BfCommitment *commitment, const char *commitment_id,
                                            BfCommitmentState to, const char *actor,
                                            const char *detail, const char *at) {
    BfCommitmentState from = BF_COMMITMENT_OPEN; int found = 0;
    BfCommitmentStatus status;
    if (!bf_commitment_state_name(to)) return BF_COMMITMENT_INVALID;
    status = bf_commitment_status(commitment, commitment_id, &from, &found);
    if (status != BF_COMMITMENT_OK) return status;
    if (!found) return BF_COMMITMENT_NOT_FOUND;
    if (!allowed(from, to)) return BF_COMMITMENT_TRANSITION;
    return append_event(commitment, commitment_id, to, actor, detail, at);
}

BfCommitmentStatus bf_commitment_discharge(BfCommitment *c, const char *id, const char *actor, const char *detail, const char *at) { return bf_commitment_transition(c, id, BF_COMMITMENT_DISCHARGED, actor, detail, at); }
BfCommitmentStatus bf_commitment_violate(BfCommitment *c, const char *id, const char *actor, const char *detail, const char *at) { return bf_commitment_transition(c, id, BF_COMMITMENT_VIOLATED, actor, detail, at); }
BfCommitmentStatus bf_commitment_compensate(BfCommitment *c, const char *id, const char *actor, const char *detail, const char *at) { return bf_commitment_transition(c, id, BF_COMMITMENT_COMPENSATED, actor, detail, at); }

BfCommitmentStatus bf_commitment_delegate(BfCommitment *commitment, const char *commitment_id,
                                          const char *to_committer, const char *delegated_id,
                                          const char *actor, const char *at) {
    sqlite3_stmt *stmt = NULL;
    BfCommitmentSpec child = {0};
    char beneficiary[BF_COMMITMENT_ID_MAX], resource[BF_COMMITMENT_TEXT_MAX], conditions[BF_COMMITMENT_TEXT_MAX], expires_at[64], authority[BF_COMMITMENT_ID_MAX], origin[BF_COMMITMENT_ID_MAX], enactment[BF_COMMITMENT_ID_MAX];
    BfCommitmentStatus status;
    if (!commitment || !present(commitment_id) || !present(to_committer) || !present(delegated_id)) return BF_COMMITMENT_INVALID;
    if (sqlite3_prepare_v2(commitment->db,
        "SELECT beneficiary,resource,conditions,expires_at,authority,origin,enactment FROM commitments WHERE commitment_id=?", -1, &stmt, NULL) != SQLITE_OK) return BF_COMMITMENT_STORAGE;
    sqlite3_bind_text(stmt, 1, commitment_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW) { sqlite3_finalize(stmt); return BF_COMMITMENT_NOT_FOUND; }
    status = copy_text(beneficiary, sizeof(beneficiary), (const char *)sqlite3_column_text(stmt, 0));
    if (status == BF_COMMITMENT_OK) status = copy_text(resource, sizeof(resource), (const char *)sqlite3_column_text(stmt, 1));
    if (status == BF_COMMITMENT_OK) status = copy_text(conditions, sizeof(conditions), (const char *)sqlite3_column_text(stmt, 2));
    if (status == BF_COMMITMENT_OK) status = copy_text(expires_at, sizeof(expires_at), (const char *)sqlite3_column_text(stmt, 3));
    if (status == BF_COMMITMENT_OK) status = copy_text(authority, sizeof(authority), (const char *)sqlite3_column_text(stmt, 4));
    if (status == BF_COMMITMENT_OK) status = copy_text(origin, sizeof(origin), (const char *)sqlite3_column_text(stmt, 5));
    if (status == BF_COMMITMENT_OK) status = copy_text(enactment, sizeof(enactment), (const char *)sqlite3_column_text(stmt, 6));
    sqlite3_finalize(stmt);
    if (status != BF_COMMITMENT_OK) return status;
    status = bf_commitment_transition(commitment, commitment_id, BF_COMMITMENT_DELEGATED,
                                      present(actor) ? actor : to_committer, delegated_id, at);
    if (status != BF_COMMITMENT_OK) return status;
    child.commitment_id = delegated_id; child.committer = to_committer; child.beneficiary = beneficiary;
    child.resource = resource; child.conditions = conditions; child.expires_at = expires_at;
    child.authority = authority; child.origin = origin; child.enactment = enactment; child.at = at;
    return bf_commitment_record(commitment, &child);
}

BfCommitmentStatus bf_commitment_delegation_chain(BfCommitment *commitment, const char *commitment_id,
                                                   char origin[BF_COMMITMENT_ID_MAX], char committer[BF_COMMITMENT_ID_MAX],
                                                   int *out_has_delegate) {
    sqlite3_stmt *stmt = NULL; BfCommitmentStatus status;
    if (!commitment || !present(commitment_id) || !origin || !committer || !out_has_delegate) return BF_COMMITMENT_INVALID;
    if (sqlite3_prepare_v2(commitment->db, "SELECT origin,committer FROM commitments WHERE commitment_id=?", -1, &stmt, NULL) != SQLITE_OK) return BF_COMMITMENT_STORAGE;
    sqlite3_bind_text(stmt, 1, commitment_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW) { sqlite3_finalize(stmt); return BF_COMMITMENT_NOT_FOUND; }
    status = copy_text(origin, BF_COMMITMENT_ID_MAX, (const char *)sqlite3_column_text(stmt, 0));
    if (status == BF_COMMITMENT_OK) status = copy_text(committer, BF_COMMITMENT_ID_MAX, (const char *)sqlite3_column_text(stmt, 1));
    sqlite3_finalize(stmt); if (status != BF_COMMITMENT_OK) return status;
    *out_has_delegate = strcmp(origin, committer) != 0; return BF_COMMITMENT_OK;
}

static BfCommitmentStatus append_list(BfCommitmentList *out, const char *id, BfCommitmentState state) {
    if (out->count == BF_COMMITMENT_LIST_MAX) return BF_COMMITMENT_CAPACITY;
    BfCommitmentStatus status = copy_text(out->ids[out->count], BF_COMMITMENT_ID_MAX, id);
    if (status != BF_COMMITMENT_OK) return status;
    out->states[out->count++] = state; return BF_COMMITMENT_OK;
}

BfCommitmentStatus bf_commitment_violable(BfCommitment *commitment, const char *now, BfCommitmentList *out) {
    sqlite3_stmt *stmt = NULL; BfCommitmentStatus status = BF_COMMITMENT_OK;
    if (!commitment || !present(now) || !out) return BF_COMMITMENT_INVALID;
    memset(out, 0, sizeof(*out));
    if (sqlite3_prepare_v2(commitment->db,
        "SELECT c.commitment_id,e.state FROM commitments c JOIN commitment_events e ON e.commitment_id=c.commitment_id WHERE c.expires_at != '' AND c.expires_at < ? AND e.seq=(SELECT MAX(e2.seq) FROM commitment_events e2 WHERE e2.commitment_id=c.commitment_id) AND e.state IN ('open','delegated') ORDER BY c.commitment_id", -1, &stmt, NULL) != SQLITE_OK) return BF_COMMITMENT_STORAGE;
    sqlite3_bind_text(stmt, 1, now, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW && status == BF_COMMITMENT_OK)
        status = append_list(out, (const char *)sqlite3_column_text(stmt, 0), state_from_name((const char *)sqlite3_column_text(stmt, 1)));
    sqlite3_finalize(stmt); return status;
}

BfCommitmentStatus bf_commitment_outstanding(BfCommitment *commitment, const char *beneficiary, BfCommitmentList *out) {
    sqlite3_stmt *stmt = NULL; BfCommitmentStatus status = BF_COMMITMENT_OK;
    if (!commitment || !present(beneficiary) || !out) return BF_COMMITMENT_INVALID;
    memset(out, 0, sizeof(*out));
    if (sqlite3_prepare_v2(commitment->db,
        "SELECT c.commitment_id,e.state FROM commitments c JOIN commitment_events e ON e.commitment_id=c.commitment_id WHERE c.beneficiary=? AND e.seq=(SELECT MAX(e2.seq) FROM commitment_events e2 WHERE e2.commitment_id=c.commitment_id) AND e.state NOT IN ('discharged','compensated') ORDER BY c.commitment_id", -1, &stmt, NULL) != SQLITE_OK) return BF_COMMITMENT_STORAGE;
    sqlite3_bind_text(stmt, 1, beneficiary, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW && status == BF_COMMITMENT_OK)
        status = append_list(out, (const char *)sqlite3_column_text(stmt, 0), state_from_name((const char *)sqlite3_column_text(stmt, 1)));
    sqlite3_finalize(stmt); return status;
}

BfCommitmentStatus bf_commitment_history(BfCommitment *commitment, const char *commitment_id, BfCommitmentEvents *out) {
    sqlite3_stmt *stmt = NULL; BfCommitmentStatus status = BF_COMMITMENT_OK;
    if (!commitment || !present(commitment_id) || !out) return BF_COMMITMENT_INVALID;
    memset(out, 0, sizeof(*out));
    if (sqlite3_prepare_v2(commitment->db, "SELECT state,actor,detail,at FROM commitment_events WHERE commitment_id=? ORDER BY seq", -1, &stmt, NULL) != SQLITE_OK) return BF_COMMITMENT_STORAGE;
    sqlite3_bind_text(stmt, 1, commitment_id, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW && status == BF_COMMITMENT_OK) {
        BfCommitmentEvent *event;
        if (out->count == BF_COMMITMENT_LIST_MAX) { status = BF_COMMITMENT_CAPACITY; break; }
        event = &out->events[out->count];
        status = copy_text(event->state, sizeof(event->state), (const char *)sqlite3_column_text(stmt, 0));
        if (status == BF_COMMITMENT_OK) status = copy_text(event->actor, sizeof(event->actor), (const char *)sqlite3_column_text(stmt, 1));
        if (status == BF_COMMITMENT_OK) status = copy_text(event->detail, sizeof(event->detail), (const char *)sqlite3_column_text(stmt, 2));
        if (status == BF_COMMITMENT_OK) status = copy_text(event->at, sizeof(event->at), (const char *)sqlite3_column_text(stmt, 3));
        if (status == BF_COMMITMENT_OK) out->count++;
    }
    sqlite3_finalize(stmt); return status;
}
