#include "bf_lifecycle.h"
#include "bonfyre.h"

#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static int present(const char *value) {
    return value != NULL && value[0] != '\0';
}

static int64_t now_ms(void) {
    struct timespec value;
    if (timespec_get(&value, TIME_UTC) != TIME_UTC) return 0;
    return (int64_t)value.tv_sec * 1000 + value.tv_nsec / 1000000;
}

static BfLifecycleResult result(BfLifecycleStatus status, const char *identity,
                                const char *state, const char *message) {
    BfLifecycleResult out;
    memset(&out, 0, sizeof(out));
    out.status = status;
    if (identity != NULL) snprintf(out.identity, sizeof(out.identity), "%s", identity);
    if (state != NULL) snprintf(out.state, sizeof(out.state), "%s", state);
    if (message != NULL) snprintf(out.error_message, sizeof(out.error_message), "%s", message);
    return out;
}

static void make_event_id(char output[65], const char *domain, const char *identity,
                          const char *key, const char *operation) {
    char material[768];
    snprintf(material, sizeof(material), "%s\n%s\n%s\n%s", domain, identity, key,
             operation);
    bf_sha256_hex((const uint8_t *)material, strlen(material), output);
}

static int execute(sqlite3 *db, const char *sql, FILE *err) {
    char *message = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &message);
    if (rc != SQLITE_OK) {
        if (err != NULL) fprintf(err, "lifecycle migration: %s\n",
                                 message != NULL ? message : sqlite3_errmsg(db));
        sqlite3_free(message);
        return -1;
    }
    return 0;
}

static int column_exists(sqlite3 *db, const char *table, const char *column) {
    sqlite3_stmt *statement = NULL;
    char sql[192];
    int found = 0;
    if (snprintf(sql, sizeof(sql), "PRAGMA table_info(%s)", table) >=
        (int)sizeof(sql) || sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) {
        return 0;
    }
    while (sqlite3_step(statement) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(statement, 1);
        if (name != NULL && strcmp(name, column) == 0) {
            found = 1;
            break;
        }
    }
    sqlite3_finalize(statement);
    return found;
}

int bf_lifecycle_migrate_database(void *sqlite_database, FILE *err) {
    sqlite3 *db = sqlite_database;
    const char *schema =
        "CREATE TABLE IF NOT EXISTS worker_runs("
        " run_id TEXT PRIMARY KEY,worker_id TEXT NOT NULL,mission_id TEXT NOT NULL,"
        " protocol_instance_id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,"
        " created_at_ms INTEGER NOT NULL,updated_at_ms INTEGER NOT NULL,terminal_receipt_ref TEXT);"
        "CREATE TABLE IF NOT EXISTS worker_run_transitions("
        " sequence INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT NOT NULL,"
        " idempotency_key TEXT NOT NULL,operation TEXT NOT NULL,from_state TEXT NOT NULL,"
        " to_state TEXT NOT NULL,receipt_ref TEXT,event_id TEXT NOT NULL UNIQUE,"
        " created_at_ms INTEGER NOT NULL,UNIQUE(run_id,idempotency_key),"
        " FOREIGN KEY(run_id) REFERENCES worker_runs(run_id));"
        "CREATE TABLE IF NOT EXISTS protocol_machines("
        " protocol_id TEXT PRIMARY KEY,initial_state TEXT NOT NULL,terminal_state TEXT NOT NULL,"
        " definition_hash TEXT NOT NULL,registered_at_ms INTEGER NOT NULL);"
        "CREATE TABLE IF NOT EXISTS protocol_machine_transitions("
        " protocol_id TEXT NOT NULL,source_state TEXT NOT NULL,operation TEXT NOT NULL,"
        " target_state TEXT NOT NULL,idempotent INTEGER NOT NULL DEFAULT 0,"
        " acknowledgement_required INTEGER NOT NULL DEFAULT 0,"
        " PRIMARY KEY(protocol_id,source_state,operation),"
        " FOREIGN KEY(protocol_id) REFERENCES protocol_machines(protocol_id));"
        "CREATE TABLE IF NOT EXISTS protocol_instances("
        " instance_id TEXT PRIMARY KEY,protocol_id TEXT NOT NULL,worker_run_id TEXT NOT NULL UNIQUE,"
        " state TEXT NOT NULL,created_at_ms INTEGER NOT NULL,updated_at_ms INTEGER NOT NULL,"
        " initial_receipt_ref TEXT,last_receipt_ref TEXT,"
        " FOREIGN KEY(protocol_id) REFERENCES protocol_machines(protocol_id),"
        " FOREIGN KEY(worker_run_id) REFERENCES worker_runs(run_id));"
        "CREATE TABLE IF NOT EXISTS protocol_instance_transitions("
        " sequence INTEGER PRIMARY KEY AUTOINCREMENT,instance_id TEXT NOT NULL,"
        " idempotency_key TEXT NOT NULL,operation TEXT NOT NULL,from_state TEXT NOT NULL,"
        " to_state TEXT NOT NULL,receipt_ref TEXT,event_id TEXT NOT NULL UNIQUE,"
        " created_at_ms INTEGER NOT NULL,UNIQUE(instance_id,idempotency_key),"
        " FOREIGN KEY(instance_id) REFERENCES protocol_instances(instance_id));"
        "CREATE INDEX IF NOT EXISTS idx_worker_runs_mission ON worker_runs(mission_id,state);"
        "CREATE INDEX IF NOT EXISTS idx_protocol_instances_state ON protocol_instances(protocol_id,state);";
    if (db == NULL) return -1;
    if (execute(db, schema, err) != 0) return -1;
    if (!column_exists(db, "protocol_instances", "last_receipt_ref")) {
        if (execute(db, "ALTER TABLE protocol_instances ADD COLUMN last_receipt_ref TEXT", err) != 0)
            return -1;
    }
    if (!column_exists(db, "protocol_instances", "initial_receipt_ref")) {
        return execute(db,
            "ALTER TABLE protocol_instances ADD COLUMN initial_receipt_ref TEXT", err);
    }
    return 0;
}

static int bind_text(sqlite3_stmt *statement, int index, const char *value) {
    return sqlite3_bind_text(statement, index, value, -1, SQLITE_TRANSIENT);
}

static BfLifecycleResult register_protocol(sqlite3 *db, const char *protocol_id,
                                           const char *initial_state,
                                           const char *terminal_state,
                                           const char *definition_hash) {
    sqlite3_stmt *statement = NULL;
    BfLifecycleResult out = result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL,
                                   "protocol registration failed");
    int rc = sqlite3_prepare_v2(db,
        "SELECT initial_state,terminal_state,definition_hash FROM protocol_machines WHERE protocol_id=?",
        -1, &statement, NULL);
    if (rc != SQLITE_OK) return out;
    bind_text(statement, 1, protocol_id);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        const char *existing_initial = (const char *)sqlite3_column_text(statement, 0);
        const char *existing_terminal = (const char *)sqlite3_column_text(statement, 1);
        const char *existing_hash = (const char *)sqlite3_column_text(statement, 2);
        if (strcmp(existing_initial, initial_state) != 0 ||
            strcmp(existing_terminal, terminal_state) != 0 ||
            strcmp(existing_hash, definition_hash) != 0) {
            sqlite3_finalize(statement);
            return result(BF_LIFECYCLE_CONFLICT, protocol_id, NULL,
                          "protocol identity is already bound to another definition");
        }
        sqlite3_finalize(statement);
        out = result(BF_LIFECYCLE_OK, protocol_id, initial_state, NULL);
        out.replayed = 1;
        return out;
    }
    sqlite3_finalize(statement);
    rc = sqlite3_prepare_v2(db,
        "INSERT INTO protocol_machines(protocol_id,initial_state,terminal_state,definition_hash,registered_at_ms) VALUES(?,?,?,?,?)",
        -1, &statement, NULL);
    if (rc != SQLITE_OK) return out;
    bind_text(statement, 1, protocol_id);
    bind_text(statement, 2, initial_state);
    bind_text(statement, 3, terminal_state);
    bind_text(statement, 4, definition_hash);
    sqlite3_bind_int64(statement, 5, now_ms());
    rc = sqlite3_step(statement);
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE ? result(BF_LIFECYCLE_OK, protocol_id, initial_state, NULL) : out;
}

static int insert_protocol_transition(sqlite3 *db, const char *protocol_id,
                                      const char *source, const char *operation,
                                      const char *target, int idempotent,
                                      int acknowledgement_required) {
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO protocol_machine_transitions(protocol_id,source_state,operation,target_state,idempotent,acknowledgement_required) VALUES(?,?,?,?,?,?)",
        -1, &statement, NULL);
    if (rc != SQLITE_OK) return -1;
    bind_text(statement, 1, protocol_id);
    bind_text(statement, 2, source);
    bind_text(statement, 3, operation);
    bind_text(statement, 4, target);
    sqlite3_bind_int(statement, 5, idempotent);
    sqlite3_bind_int(statement, 6, acknowledgement_required);
    rc = sqlite3_step(statement);
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE ? 0 : -1;
}

BfLifecycleResult bf_lifecycle_register_restate(void *sqlite_database) {
    sqlite3 *db = sqlite_database;
    static const char *protocol_id = "restate-workgraph-v1";
    static const char *definition_hash =
        "db250fbf24313e0b2aa9f2b1587183aa5f9268aa13a9956369b3496a34d8f9e1";
    BfLifecycleResult out;
    if (db == NULL || bf_lifecycle_migrate_database(db, NULL) != 0) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL, "schema unavailable");
    }
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL, "cannot begin registration");
    }
    out = register_protocol(db, protocol_id, "registered", "completed", definition_hash);
    if (out.status != BF_LIFECYCLE_OK ||
        insert_protocol_transition(db, protocol_id, "registered", "invoke", "running-a", 1, 0) != 0 ||
        insert_protocol_transition(db, protocol_id, "running-a", "node-a-complete", "waiting", 1, 1) != 0 ||
        insert_protocol_transition(db, protocol_id, "waiting", "resume", "running-b", 1, 1) != 0 ||
        insert_protocol_transition(db, protocol_id, "running-b", "node-b-complete", "completed", 1, 1) != 0) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        if (out.status == BF_LIFECYCLE_OK) {
            out = result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL,
                         "cannot register protocol transitions");
        }
        return out;
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        return result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL, "cannot commit registration");
    }
    return out;
}

BfLifecycleResult bf_lifecycle_register_capability_fulfillment(void *sqlite_database) {
    sqlite3 *db = sqlite_database;
    static const char *protocol_id = "capability-fulfillment-v2";
    static const char *definition_hash =
        "a3d3216efdd45fb24d214418456cd43845352d8bc91e6f837361d7401527815c";
    BfLifecycleResult out;
    if (db == NULL || bf_lifecycle_migrate_database(db, NULL) != 0) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL, "schema unavailable");
    }
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL, "cannot begin registration");
    }
    /* The machine records; it never verifies.
     *
     * "verify-payment" is deliberately not an event here, and a test asserts it
     * stays illegal: a generic lifecycle that could verify its own payment would
     * settle a forged receipt. What was missing is the other half -- with no
     * event past payment-presented at all, a rail that *had* verified externally
     * had nowhere to say so, so financials could not be exercised end to end
     * even with real funds.
     *
     * The three events below are issued by a rail adapter and each carries that
     * adapter's receipt. The machine's job is to record a verification that
     * already happened somewhere it can name, which is the same shape as every
     * other receipt in this system.
     *
     * Cannot infer: a recorded rail-verified event -> that this machine checked
     * the money. It checked nothing. It recorded who did. */
    out = register_protocol(db, protocol_id, "quoted", "payment-presented", definition_hash);
    if (out.status != BF_LIFECYCLE_OK ||
        insert_protocol_transition(db, protocol_id, "quoted", "present-payment",
                                   "payment-presented", 1, 1) != 0) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        if (out.status == BF_LIFECYCLE_OK) {
            out = result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL,
                         "cannot register capability fulfillment transitions");
        }
        return out;
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        return result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL, "cannot commit registration");
    }
    return out;
}

/* v3: the same quote, carried through to delivery.
 *
 * v2 stops at payment-presented and stays there, which is why financials could
 * not be exercised end to end even with real funds: a rail that had verified
 * externally had no event to record. It is not edited here -- instances are
 * bound to their definition and the registration guard refuses to redefine one
 * that is in use, which is the correct answer. This is a version beside it.
 *
 * "verify-payment" is still not an event in either definition, and the test that
 * a generic lifecycle refuses forged verification still passes against v2. The
 * events below are issued by a rail adapter and each carries that adapter's
 * receipt: the machine records a verification that happened somewhere it can
 * name, exactly as it records every other receipt.
 *
 * Cannot infer: a recorded rail-verified event -> that this machine checked the
 * money. It checked nothing. It recorded who did, and the receipt says who. */
BfLifecycleResult bf_lifecycle_register_capability_fulfillment_rail(void *sqlite_database) {
    sqlite3 *db = sqlite_database;
    static const char *protocol_id = "capability-fulfillment-rail-v3";
    static const char *definition_hash =
        "b7f1c0e94a2d6835bb1e47f0c3a95d2e6081fb47cc90a5d3e21f6b8c4d70a915";
    BfLifecycleResult out;
    if (db == NULL || bf_lifecycle_migrate_database(db, NULL) != 0) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL, "schema unavailable");
    }
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL, "cannot begin registration");
    }
    out = register_protocol(db, protocol_id, "quoted", "delivered", definition_hash);
    if (out.status != BF_LIFECYCLE_OK ||
        insert_protocol_transition(db, protocol_id, "quoted", "present-payment",
                                   "payment-presented", 1, 1) != 0 ||
        insert_protocol_transition(db, protocol_id, "payment-presented", "rail-verified",
                                   "settled", 1, 1) != 0 ||
        insert_protocol_transition(db, protocol_id, "settled", "execute",
                                   "executed", 1, 1) != 0 ||
        insert_protocol_transition(db, protocol_id, "executed", "deliver",
                                   "delivered", 1, 1) != 0) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        if (out.status == BF_LIFECYCLE_OK) {
            out = result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL,
                         "cannot register rail fulfillment transitions");
        }
        return out;
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        return result(BF_LIFECYCLE_STORAGE_ERROR, protocol_id, NULL, "cannot commit registration");
    }
    return out;
}

static BfLifecycleResult start_identity(sqlite3 *db, const char *table,
                                        const char *identity_column, const char *identity,
                                        const char *select_sql, const char *insert_sql,
                                        const char *a, const char *b, const char *c,
                                        const char *initial_state) {
    sqlite3_stmt *statement = NULL;
    BfLifecycleResult out;
    int rc;
    (void)table;
    (void)identity_column;
    rc = sqlite3_prepare_v2(db, select_sql, -1, &statement, NULL);
    if (rc != SQLITE_OK) return result(BF_LIFECYCLE_STORAGE_ERROR, identity, NULL, sqlite3_errmsg(db));
    bind_text(statement, 1, identity);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        const char *existing_a = (const char *)sqlite3_column_text(statement, 0);
        const char *existing_b = (const char *)sqlite3_column_text(statement, 1);
        const char *existing_c = (const char *)sqlite3_column_text(statement, 2);
        const char *state = (const char *)sqlite3_column_text(statement, 3);
        int matches = strcmp(existing_a, a) == 0 && strcmp(existing_b, b) == 0 &&
                      strcmp(existing_c, c) == 0;
        out = result(matches ? BF_LIFECYCLE_OK : BF_LIFECYCLE_CONFLICT,
                     identity, state, matches ? NULL :
                     "stable identity is already bound to different semantic fields");
        out.replayed = matches;
        sqlite3_finalize(statement);
        return out;
    }
    sqlite3_finalize(statement);
    rc = sqlite3_prepare_v2(db, insert_sql, -1, &statement, NULL);
    if (rc != SQLITE_OK) return result(BF_LIFECYCLE_STORAGE_ERROR, identity, NULL, sqlite3_errmsg(db));
    bind_text(statement, 1, identity);
    bind_text(statement, 2, a);
    bind_text(statement, 3, b);
    bind_text(statement, 4, c);
    bind_text(statement, 5, initial_state);
    sqlite3_bind_int64(statement, 6, now_ms());
    sqlite3_bind_int64(statement, 7, now_ms());
    rc = sqlite3_step(statement);
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE ? result(BF_LIFECYCLE_OK, identity, initial_state, NULL) :
        result(BF_LIFECYCLE_STORAGE_ERROR, identity, NULL, sqlite3_errmsg(db));
}

BfLifecycleResult bf_worker_run_start(void *sqlite_database,
                                      const BfWorkerRunSpec *spec) {
    sqlite3 *db = sqlite_database;
    if (db == NULL || spec == NULL || !present(spec->run_id) ||
        !present(spec->worker_id) || !present(spec->mission_id) ||
        !present(spec->protocol_instance_id)) {
        return result(BF_LIFECYCLE_INVALID, spec != NULL ? spec->run_id : NULL,
                      NULL, "WorkerRun requires stable run, worker, mission and protocol identities");
    }
    if (bf_lifecycle_migrate_database(db, NULL) != 0) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, spec->run_id, NULL, "schema unavailable");
    }
    return start_identity(db, "worker_runs", "run_id", spec->run_id,
        "SELECT worker_id,mission_id,protocol_instance_id,state FROM worker_runs WHERE run_id=?",
        "INSERT INTO worker_runs(run_id,worker_id,mission_id,protocol_instance_id,state,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?,?)",
        spec->worker_id, spec->mission_id, spec->protocol_instance_id, "running");
}

BfLifecycleResult bf_protocol_instance_start(
    void *sqlite_database, const BfProtocolInstanceSpec *spec) {
    sqlite3 *db = sqlite_database;
    sqlite3_stmt *statement = NULL;
    char initial_state[64];
    BfLifecycleResult out;
    int rc;
    if (db == NULL || spec == NULL || !present(spec->instance_id) ||
        !present(spec->protocol_id) || !present(spec->worker_run_id)) {
        return result(BF_LIFECYCLE_INVALID, spec != NULL ? spec->instance_id : NULL,
                      NULL, "ProtocolMachine requires stable instance, definition and WorkerRun identities");
    }
    if (bf_lifecycle_migrate_database(db, NULL) != 0) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, spec->instance_id, NULL, "schema unavailable");
    }
    if (sqlite3_prepare_v2(db, "SELECT initial_state FROM protocol_machines WHERE protocol_id=?",
                           -1, &statement, NULL) != SQLITE_OK) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, spec->instance_id, NULL, sqlite3_errmsg(db));
    }
    bind_text(statement, 1, spec->protocol_id);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        snprintf(initial_state, sizeof(initial_state), "%s", sqlite3_column_text(statement, 0));
    } else {
        sqlite3_finalize(statement);
        return result(BF_LIFECYCLE_NOT_FOUND, spec->instance_id, NULL,
                      "protocol definition is not registered");
    }
    sqlite3_finalize(statement);
    if (sqlite3_prepare_v2(db,
        "SELECT protocol_id,worker_run_id,state,COALESCE(initial_receipt_ref,'') FROM protocol_instances WHERE instance_id=?",
        -1, &statement, NULL) != SQLITE_OK) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, spec->instance_id, NULL, sqlite3_errmsg(db));
    }
    bind_text(statement, 1, spec->instance_id);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        const char *existing_protocol = (const char *)sqlite3_column_text(statement, 0);
        const char *existing_run = (const char *)sqlite3_column_text(statement, 1);
        const char *state = (const char *)sqlite3_column_text(statement, 2);
        const char *existing_receipt = (const char *)sqlite3_column_text(statement, 3);
        const char *requested_receipt = spec->initial_receipt_ref ? spec->initial_receipt_ref : "";
        int matches = strcmp(existing_protocol, spec->protocol_id) == 0 &&
                      strcmp(existing_run, spec->worker_run_id) == 0 &&
                      strcmp(existing_receipt, requested_receipt) == 0;
        out = result(matches ? BF_LIFECYCLE_OK : BF_LIFECYCLE_CONFLICT,
                     spec->instance_id, state, matches ? NULL :
                     "stable protocol identity is already bound to different semantic fields");
        out.replayed = matches;
        sqlite3_finalize(statement);
        return out;
    }
    sqlite3_finalize(statement);
    if (sqlite3_prepare_v2(db,
        "INSERT INTO protocol_instances(instance_id,protocol_id,worker_run_id,state,initial_receipt_ref,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?,?)",
        -1, &statement, NULL) != SQLITE_OK) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, spec->instance_id, NULL, sqlite3_errmsg(db));
    }
    bind_text(statement, 1, spec->instance_id);
    bind_text(statement, 2, spec->protocol_id);
    bind_text(statement, 3, spec->worker_run_id);
    bind_text(statement, 4, initial_state);
    if (spec->initial_receipt_ref != NULL) bind_text(statement, 5, spec->initial_receipt_ref);
    else sqlite3_bind_null(statement, 5);
    sqlite3_bind_int64(statement, 6, now_ms());
    sqlite3_bind_int64(statement, 7, now_ms());
    rc = sqlite3_step(statement);
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE ? result(BF_LIFECYCLE_OK, spec->instance_id, initial_state, NULL) :
        result(BF_LIFECYCLE_STORAGE_ERROR, spec->instance_id, NULL, sqlite3_errmsg(db));
}

static BfLifecycleResult status_row(sqlite3 *db, const char *sql, const char *identity) {
    sqlite3_stmt *statement = NULL;
    BfLifecycleResult out;
    if (!present(identity)) return result(BF_LIFECYCLE_INVALID, identity, NULL, "identity required");
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, identity, NULL, sqlite3_errmsg(db));
    }
    bind_text(statement, 1, identity);
    if (sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        return result(BF_LIFECYCLE_NOT_FOUND, identity, NULL, "identity not found");
    }
    out = result(BF_LIFECYCLE_OK, identity,
                 (const char *)sqlite3_column_text(statement, 0), NULL);
    sqlite3_finalize(statement);
    return out;
}

BfLifecycleResult bf_worker_run_status(void *sqlite_database, const char *run_id) {
    return status_row(sqlite_database, "SELECT state FROM worker_runs WHERE run_id=?", run_id);
}

BfLifecycleResult bf_protocol_instance_status(void *sqlite_database,
                                               const char *instance_id) {
    return status_row(sqlite_database,
                      "SELECT state FROM protocol_instances WHERE instance_id=?",
                      instance_id);
}

static const char *worker_target(const char *state, const char *operation) {
    if (strcmp(state, "running") == 0 && strcmp(operation, "wait") == 0) return "waiting";
    if (strcmp(state, "waiting") == 0 && strcmp(operation, "resume") == 0) return "running";
    if (strcmp(state, "running") == 0 && strcmp(operation, "complete") == 0) return "completed";
    if ((strcmp(state, "running") == 0 || strcmp(state, "waiting") == 0) &&
        strcmp(operation, "fail") == 0) return "failed";
    return NULL;
}

static BfLifecycleResult replay_transition(sqlite3 *db, const char *sql,
                                           const char *identity, const char *key,
                                           const char *operation) {
    sqlite3_stmt *statement = NULL;
    BfLifecycleResult out = result(BF_LIFECYCLE_NOT_FOUND, identity, NULL, NULL);
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, identity, NULL, sqlite3_errmsg(db));
    }
    bind_text(statement, 1, identity);
    bind_text(statement, 2, key);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        const char *existing_operation = (const char *)sqlite3_column_text(statement, 0);
        const char *to_state = (const char *)sqlite3_column_text(statement, 1);
        const char *event_id = (const char *)sqlite3_column_text(statement, 2);
        out = result(strcmp(existing_operation, operation) == 0 ? BF_LIFECYCLE_OK :
                     BF_LIFECYCLE_CONFLICT, identity, to_state,
                     strcmp(existing_operation, operation) == 0 ? NULL :
                     "idempotency key is already bound to another operation");
        if (out.status == BF_LIFECYCLE_OK) {
            out.replayed = 1;
            snprintf(out.event_id, sizeof(out.event_id), "%s", event_id);
        }
    }
    sqlite3_finalize(statement);
    return out;
}

static BfLifecycleResult apply_transition(sqlite3 *db, const char *identity,
    const char *operation, const char *key, const char *receipt_ref,
    const char *status_sql, const char *replay_sql, const char *transition_sql,
    const char *update_sql, const char *domain, const char *protocol_id) {
    sqlite3_stmt *statement = NULL;
    BfLifecycleResult replay;
    BfLifecycleResult out;
    char current[64];
    char target_buffer[64];
    const char *target = NULL;
    int acknowledgement_required = 0;
    int rc;
    if (!present(identity) || !present(operation) || !present(key)) {
        return result(BF_LIFECYCLE_INVALID, identity, NULL,
                      "identity, operation and idempotency key are required");
    }
    if (protocol_id == NULL && strcmp(operation, "complete") == 0 &&
        !present(receipt_ref)) {
        return result(BF_LIFECYCLE_INVALID, identity, NULL,
                      "WorkerRun completion requires a receipt");
    }
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, identity, NULL,
                      "cannot begin transition");
    }
    replay = replay_transition(db, replay_sql, identity, key, operation);
    if (replay.status != BF_LIFECYCLE_NOT_FOUND) {
        sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
        return replay;
    }
    if (sqlite3_prepare_v2(db, status_sql, -1, &statement, NULL) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        return result(BF_LIFECYCLE_STORAGE_ERROR, identity, NULL, sqlite3_errmsg(db));
    }
    bind_text(statement, 1, identity);
    if (sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        return result(BF_LIFECYCLE_NOT_FOUND, identity, NULL, "identity not found");
    }
    snprintf(current, sizeof(current), "%s", sqlite3_column_text(statement, 0));
    sqlite3_finalize(statement);
    if (protocol_id == NULL) {
        target = worker_target(current, operation);
    } else {
        if (sqlite3_prepare_v2(db,
            "SELECT target_state,acknowledgement_required FROM protocol_machine_transitions WHERE protocol_id=? AND source_state=? AND operation=?",
            -1, &statement, NULL) != SQLITE_OK) {
            sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
            return result(BF_LIFECYCLE_STORAGE_ERROR, identity, current, sqlite3_errmsg(db));
        }
        bind_text(statement, 1, protocol_id);
        bind_text(statement, 2, current);
        bind_text(statement, 3, operation);
        if (sqlite3_step(statement) == SQLITE_ROW) {
            snprintf(target_buffer, sizeof(target_buffer), "%s", sqlite3_column_text(statement, 0));
            target = target_buffer;
            acknowledgement_required = sqlite3_column_int(statement, 1);
        }
        sqlite3_finalize(statement);
    }
    if (target == NULL) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        return result(BF_LIFECYCLE_ILLEGAL_TRANSITION, identity, current,
                      "operation is illegal in the current state");
    }
    if (acknowledgement_required && !present(receipt_ref)) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        return result(BF_LIFECYCLE_INVALID, identity, current,
                      "protocol transition requires an acknowledgement receipt");
    }
    out = result(BF_LIFECYCLE_OK, identity, target, NULL);
    make_event_id(out.event_id, domain, identity, key, operation);
    if (sqlite3_prepare_v2(db, transition_sql, -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    bind_text(statement, 1, identity);
    bind_text(statement, 2, key);
    bind_text(statement, 3, operation);
    bind_text(statement, 4, current);
    bind_text(statement, 5, target);
    if (receipt_ref != NULL) bind_text(statement, 6, receipt_ref); else sqlite3_bind_null(statement, 6);
    bind_text(statement, 7, out.event_id);
    sqlite3_bind_int64(statement, 8, now_ms());
    rc = sqlite3_step(statement);
    sqlite3_finalize(statement);
    statement = NULL;
    if (rc != SQLITE_DONE) goto storage_failure;
    if (sqlite3_prepare_v2(db, update_sql, -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    bind_text(statement, 1, target);
    sqlite3_bind_int64(statement, 2, now_ms());
    if (receipt_ref != NULL) bind_text(statement, 3, receipt_ref); else sqlite3_bind_null(statement, 3);
    bind_text(statement, 4, identity);
    rc = sqlite3_step(statement);
    sqlite3_finalize(statement);
    statement = NULL;
    if (rc != SQLITE_DONE || sqlite3_changes(db) != 1) goto storage_failure;
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto storage_failure;
    return out;

storage_failure:
    if (statement != NULL) sqlite3_finalize(statement);
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    return result(BF_LIFECYCLE_STORAGE_ERROR, identity, current, sqlite3_errmsg(db));
}

BfLifecycleResult bf_worker_run_step(void *sqlite_database, const char *run_id,
                                     const char *operation,
                                     const char *idempotency_key,
                                     const char *receipt_ref) {
    sqlite3 *db = sqlite_database;
    if (db == NULL) return result(BF_LIFECYCLE_INVALID, run_id, NULL, "database required");
    return apply_transition(db, run_id, operation, idempotency_key, receipt_ref,
        "SELECT state FROM worker_runs WHERE run_id=?",
        "SELECT operation,to_state,event_id FROM worker_run_transitions WHERE run_id=? AND idempotency_key=?",
        "INSERT INTO worker_run_transitions(run_id,idempotency_key,operation,from_state,to_state,receipt_ref,event_id,created_at_ms) VALUES(?,?,?,?,?,?,?,?)",
        "UPDATE worker_runs SET state=?,updated_at_ms=?,terminal_receipt_ref=COALESCE(?,terminal_receipt_ref) WHERE run_id=?",
        "worker-run", NULL);
}

BfLifecycleResult bf_protocol_instance_step(
    void *sqlite_database, const char *instance_id, const char *operation,
    const char *idempotency_key, const char *receipt_ref) {
    sqlite3 *db = sqlite_database;
    sqlite3_stmt *statement = NULL;
    char protocol_id[128];
    if (db == NULL) return result(BF_LIFECYCLE_INVALID, instance_id, NULL, "database required");
    if (sqlite3_prepare_v2(db, "SELECT protocol_id FROM protocol_instances WHERE instance_id=?",
                           -1, &statement, NULL) != SQLITE_OK) {
        return result(BF_LIFECYCLE_STORAGE_ERROR, instance_id, NULL, sqlite3_errmsg(db));
    }
    bind_text(statement, 1, instance_id);
    if (sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        return result(BF_LIFECYCLE_NOT_FOUND, instance_id, NULL, "protocol instance not found");
    }
    snprintf(protocol_id, sizeof(protocol_id), "%s", sqlite3_column_text(statement, 0));
    sqlite3_finalize(statement);
    return apply_transition(db, instance_id, operation, idempotency_key, receipt_ref,
        "SELECT state FROM protocol_instances WHERE instance_id=?",
        "SELECT operation,to_state,event_id FROM protocol_instance_transitions WHERE instance_id=? AND idempotency_key=?",
        "INSERT INTO protocol_instance_transitions(instance_id,idempotency_key,operation,from_state,to_state,receipt_ref,event_id,created_at_ms) VALUES(?,?,?,?,?,?,?,?)",
        "UPDATE protocol_instances SET state=?,updated_at_ms=?,last_receipt_ref=COALESCE(?,last_receipt_ref) WHERE instance_id=?",
        "protocol-machine", protocol_id);
}

const char *bf_lifecycle_status_name(BfLifecycleStatus status) {
    switch (status) {
    case BF_LIFECYCLE_OK: return "ok";
    case BF_LIFECYCLE_NOT_FOUND: return "not_found";
    case BF_LIFECYCLE_CONFLICT: return "conflict";
    case BF_LIFECYCLE_ILLEGAL_TRANSITION: return "illegal_transition";
    case BF_LIFECYCLE_INVALID: return "invalid";
    case BF_LIFECYCLE_STORAGE_ERROR: return "storage_error";
    }
    return "unknown";
}
