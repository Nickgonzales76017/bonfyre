#include "workgraph_internal.h"

#include <stdio.h>
#include <string.h>

#define BF_WORKGRAPH_SCHEMA_VERSION 8

static int table_exists(sqlite3 *db, const char *table_name) {
    sqlite3_stmt *statement = NULL;
    int found = 0;

    if (sqlite3_prepare_v2(db,
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            -1, &statement, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(statement, 1, table_name, -1, SQLITE_TRANSIENT);
    found = sqlite3_step(statement) == SQLITE_ROW;
    sqlite3_finalize(statement);
    return found;
}

static int view_exists(sqlite3 *db, const char *view_name) {
    sqlite3_stmt *statement = NULL;
    int found = 0;

    if (sqlite3_prepare_v2(db,
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name=?",
            -1, &statement, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(statement, 1, view_name, -1, SQLITE_TRANSIENT);
    found = sqlite3_step(statement) == SQLITE_ROW;
    sqlite3_finalize(statement);
    return found;
}

static int column_exists(sqlite3 *db, const char *table_name, const char *column_name) {
    sqlite3_stmt *statement = NULL;
    char sql[256];
    int found = 0;

    if (snprintf(sql, sizeof(sql), "PRAGMA table_info(%s)", table_name) >= (int)sizeof(sql)) {
        return 0;
    }
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) {
        return 0;
    }
    while (sqlite3_step(statement) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(statement, 1);
        if (name != NULL && strcmp(name, column_name) == 0) {
            found = 1;
            break;
        }
    }
    sqlite3_finalize(statement);
    return found;
}

static int index_exists(sqlite3 *db, const char *index_name) {
    sqlite3_stmt *statement = NULL;
    int found = 0;

    if (sqlite3_prepare_v2(db,
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
            -1, &statement, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(statement, 1, index_name, -1, SQLITE_TRANSIENT);
    found = sqlite3_step(statement) == SQLITE_ROW;
    sqlite3_finalize(statement);
    return found;
}

static int checked_exec(sqlite3 *db, const char *sql, FILE *err) {
    char *message = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &message);

    if (rc != SQLITE_OK) {
        fprintf(err, "workgraph migration: %s\n", message != NULL ? message : sqlite3_errmsg(db));
        sqlite3_free(message);
        return -1;
    }
    return 0;
}

static int add_column(sqlite3 *db, const char *table_name, const char *column_name,
                      const char *definition, FILE *err) {
    char sql[512];

    if (column_exists(db, table_name, column_name)) {
        return 0;
    }
    if (snprintf(sql, sizeof(sql), "ALTER TABLE %s ADD COLUMN %s %s",
                 table_name, column_name, definition) >= (int)sizeof(sql)) {
        fprintf(err, "workgraph migration: column definition is too long\n");
        return -1;
    }
    return checked_exec(db, sql, err);
}

static int verify_base_schema(sqlite3 *db, FILE *err) {
    static const char *required_node_columns[] = {
        "mission_id", "node_id", "operator_id", "status", "attempt",
        "retry_limit", "timeout_seconds", NULL
    };

    if (!table_exists(db, "missions") || !table_exists(db, "workgraph_nodes") ||
        !table_exists(db, "events") || !table_exists(db, "receipts")) {
        fprintf(err, "workgraph migration: version-3 base tables are missing\n");
        return -1;
    }
    for (size_t index = 0; required_node_columns[index] != NULL; ++index) {
        if (!column_exists(db, "workgraph_nodes", required_node_columns[index])) {
            fprintf(err, "workgraph migration: workgraph_nodes.%s is missing\n",
                    required_node_columns[index]);
            return -1;
        }
    }
    return 0;
}

static int apply_version_four(sqlite3 *db, FILE *err) {
    static const struct {
        const char *name;
        const char *definition;
    } node_columns[] = {
        {"input_uri", "TEXT"},
        {"depends_on", "TEXT NOT NULL DEFAULT '[]'"},
        {"output_uri", "TEXT"},
        {"lease_owner", "TEXT"},
        {"claim_token_hash", "TEXT"},
        {"claim_acquired_at_ms", "INTEGER"},
        {"lease_expires_at_ms", "INTEGER"},
        {"visibility_deadline_ms", "INTEGER"},
        {"next_attempt_at_ms", "INTEGER"},
        {"last_heartbeat_at_ms", "INTEGER"},
        {"backoff_policy", "TEXT NOT NULL DEFAULT 'exponential'"},
        {"backoff_base_ms", "INTEGER NOT NULL DEFAULT 1000"},
        {"backoff_multiplier", "REAL NOT NULL DEFAULT 2.0"},
        {"backoff_max_ms", "INTEGER NOT NULL DEFAULT 60000"},
        {"backoff_jitter_percent", "INTEGER NOT NULL DEFAULT 0"},
        {"failure_class", "TEXT"},
        {"failure_message", "TEXT"},
        {"cancellation_state", "TEXT NOT NULL DEFAULT 'none'"},
        {"compensation_operator", "TEXT"},
        {"parent_node", "TEXT"},
        {"subgraph_id", "TEXT"},
        {"fanout_group", "TEXT"},
        {"fanin_required", "INTEGER NOT NULL DEFAULT 0"},
        {"family", "TEXT NOT NULL DEFAULT 'default'"},
        {"priority", "INTEGER NOT NULL DEFAULT 100"},
        {"ready_at_ms", "INTEGER"},
        {"created_at_ms", "INTEGER"},
        {"updated_at_ms", "INTEGER"},
        {"terminal_at_ms", "INTEGER"},
        {"receipt_id", "TEXT"},
        {"effect_state", "TEXT"},
        {NULL, NULL}
    };
    static const char *normalized_schema =
        "CREATE TABLE workgraph_dependencies("
        " mission_id TEXT NOT NULL,node_id TEXT NOT NULL,depends_on_node_id TEXT NOT NULL,"
        " dependency_policy TEXT NOT NULL DEFAULT 'require_success',"
        " PRIMARY KEY(mission_id,node_id,depends_on_node_id),"
        " FOREIGN KEY(mission_id,node_id) REFERENCES workgraph_nodes(mission_id,node_id),"
        " FOREIGN KEY(mission_id,depends_on_node_id) REFERENCES workgraph_nodes(mission_id,node_id));"
        "CREATE TABLE workgraph_fanout_groups("
        " mission_id TEXT NOT NULL,group_id TEXT NOT NULL,parent_node_id TEXT NOT NULL,"
        " child_count INTEGER NOT NULL,failure_policy TEXT NOT NULL,state TEXT NOT NULL,"
        " created_at_ms INTEGER NOT NULL,updated_at_ms INTEGER NOT NULL,"
        " PRIMARY KEY(mission_id,group_id));"
        "CREATE TABLE workgraph_attempts("
        " mission_id TEXT NOT NULL,node_id TEXT NOT NULL,attempt INTEGER NOT NULL,"
        " worker_id TEXT NOT NULL,claim_token_digest TEXT NOT NULL,started_at_ms INTEGER NOT NULL,"
        " lease_expires_at_ms INTEGER NOT NULL,finished_at_ms INTEGER,outcome TEXT,error_code TEXT,"
        " PRIMARY KEY(mission_id,node_id,attempt));"
        "CREATE TABLE workgraph_transitions("
        " sequence INTEGER PRIMARY KEY AUTOINCREMENT,transition_id TEXT NOT NULL UNIQUE,"
        " mission_id TEXT NOT NULL,node_id TEXT,attempt INTEGER NOT NULL,from_status TEXT,"
        " to_status TEXT NOT NULL,actor TEXT NOT NULL,event_id TEXT NOT NULL UNIQUE,"
        " receipt_id TEXT NOT NULL UNIQUE,created_at_ms INTEGER NOT NULL);"
        "CREATE TABLE workgraph_compensations("
        " mission_id TEXT NOT NULL,node_id TEXT NOT NULL,effect_id TEXT NOT NULL,"
        " state TEXT NOT NULL,attempt INTEGER NOT NULL DEFAULT 0,last_error TEXT,"
        " created_at_ms INTEGER NOT NULL,updated_at_ms INTEGER NOT NULL,receipt_id TEXT,"
        " PRIMARY KEY(mission_id,node_id,effect_id));"
        "CREATE UNIQUE INDEX idx_workgraph_claim_digest ON workgraph_attempts(claim_token_digest);"
        "CREATE INDEX idx_workgraph_due ON workgraph_nodes(status,next_attempt_at_ms);"
        "CREATE INDEX idx_workgraph_lease ON workgraph_nodes(status,lease_expires_at_ms);"
        "CREATE INDEX idx_workgraph_mission_status ON workgraph_nodes(mission_id,status);"
        "CREATE INDEX idx_workgraph_mission_node ON workgraph_nodes(mission_id,node_id);"
        "CREATE INDEX idx_workgraph_dependency_parent ON workgraph_dependencies(mission_id,depends_on_node_id);"
        "CREATE INDEX idx_workgraph_dependency_child ON workgraph_dependencies(mission_id,node_id);"
        "CREATE INDEX idx_workgraph_fanout_group ON workgraph_nodes(mission_id,fanout_group);"
        "CREATE INDEX idx_workgraph_parent_node ON workgraph_nodes(mission_id,parent_node);"
        "CREATE INDEX idx_workgraph_cancellation ON workgraph_nodes(cancellation_state,status);";

    if (verify_base_schema(db, err) != 0) {
        return -1;
    }
    for (size_t index = 0; node_columns[index].name != NULL; ++index) {
        if (add_column(db, "workgraph_nodes", node_columns[index].name,
                       node_columns[index].definition, err) != 0) {
            return -1;
        }
    }
    if (add_column(db, "receipts", "previous_receipt_id", "TEXT", err) != 0 ||
        add_column(db, "receipts", "chain_hash", "TEXT", err) != 0) {
        return -1;
    }
    if (checked_exec(db, normalized_schema, err) != 0) {
        return -1;
    }
    return 0;
}

static int verify_version_four(sqlite3 *db, FILE *err) {
    static const char *tables[] = {
        "workgraph_dependencies", "workgraph_fanout_groups", "workgraph_attempts",
        "workgraph_transitions", "workgraph_compensations", NULL
    };
    static const char *indexes[] = {
        "idx_workgraph_due", "idx_workgraph_lease", "idx_workgraph_mission_status",
        "idx_workgraph_mission_node", "idx_workgraph_dependency_parent",
        "idx_workgraph_dependency_child", "idx_workgraph_fanout_group",
        "idx_workgraph_parent_node", "idx_workgraph_cancellation", NULL
    };
    static const char *dependency_columns[] = {
        "mission_id", "node_id", "depends_on_node_id", "dependency_policy", NULL
    };

    for (size_t index = 0; tables[index] != NULL; ++index) {
        if (!table_exists(db, tables[index])) {
            fprintf(err, "workgraph migration: required table %s is missing\n", tables[index]);
            return -1;
        }
    }
    for (size_t index = 0; indexes[index] != NULL; ++index) {
        if (!index_exists(db, indexes[index])) {
            fprintf(err, "workgraph migration: required index %s is missing\n", indexes[index]);
            return -1;
        }
    }
    for (size_t index = 0; dependency_columns[index] != NULL; ++index) {
        if (!column_exists(db, "workgraph_dependencies", dependency_columns[index])) {
            fprintf(err, "workgraph migration: dependency column %s is missing\n",
                    dependency_columns[index]);
            return -1;
        }
    }
    return column_exists(db, "receipts", "previous_receipt_id") &&
           column_exists(db, "receipts", "chain_hash") ? 0 : -1;
}

static int apply_version_five(sqlite3 *db, FILE *err) {
    static const struct {
        const char *name;
        const char *definition;
    } effect_columns[] = {
        {"adapter_id", "TEXT NOT NULL DEFAULT 'derive-file'"},
        {"target_uri", "TEXT NOT NULL DEFAULT ''"},
        {"input_artifact_uri", "TEXT"},
        {"prepared_state", "TEXT NOT NULL DEFAULT ''"},
        {"verification_policy", "TEXT NOT NULL DEFAULT 'sha256'"},
        {"rollback_contract", "TEXT NOT NULL DEFAULT 'remove-created-target'"},
        {"authority_identity", "TEXT NOT NULL DEFAULT 'mission-worker'"},
        {"simulation", "TEXT NOT NULL DEFAULT ''"},
        {"recovery_action", "TEXT NOT NULL DEFAULT 'rollback'"},
        {NULL, NULL}
    };
    static const char *attempt_schema =
        "CREATE TABLE workgraph_compensation_attempts("
        " mission_id TEXT NOT NULL,node_id TEXT NOT NULL,effect_id TEXT NOT NULL,"
        " attempt INTEGER NOT NULL,worker_id TEXT NOT NULL,claim_token_digest TEXT NOT NULL,"
        " started_at_ms INTEGER NOT NULL,lease_expires_at_ms INTEGER NOT NULL,"
        " finished_at_ms INTEGER,outcome TEXT,error_code TEXT,"
        " PRIMARY KEY(mission_id,node_id,effect_id,attempt),"
        " FOREIGN KEY(mission_id,node_id,effect_id) REFERENCES "
        " workgraph_compensations(mission_id,node_id,effect_id));"
        "CREATE UNIQUE INDEX idx_workgraph_compensation_claim_digest "
        "ON workgraph_compensation_attempts(claim_token_digest);"
        "CREATE INDEX idx_workgraph_compensation_due "
        "ON workgraph_compensations(state,updated_at_ms);"
        "CREATE INDEX idx_workgraph_compensation_lease "
        "ON workgraph_compensation_attempts(lease_expires_at_ms,finished_at_ms);";

    if (!table_exists(db, "workgraph_compensations")) {
        fprintf(err, "workgraph migration: version-4 compensation table is missing\n");
        return -1;
    }
    for (size_t index = 0; effect_columns[index].name != NULL; ++index) {
        if (add_column(db, "workgraph_compensations", effect_columns[index].name,
                       effect_columns[index].definition, err) != 0) {
            return -1;
        }
    }
    return checked_exec(db, attempt_schema, err);
}

static int verify_version_five(sqlite3 *db, FILE *err) {
    static const char *effect_columns[] = {
        "adapter_id", "target_uri", "input_artifact_uri", "prepared_state",
        "verification_policy", "rollback_contract", "authority_identity", "simulation",
        "recovery_action", NULL
    };
    static const char *indexes[] = {
        "idx_workgraph_compensation_claim_digest", "idx_workgraph_compensation_due",
        "idx_workgraph_compensation_lease", NULL
    };

    if (!table_exists(db, "workgraph_compensation_attempts")) {
        fprintf(err, "workgraph migration: compensation attempt table is missing\n");
        return -1;
    }
    for (size_t index = 0; effect_columns[index] != NULL; ++index) {
        if (!column_exists(db, "workgraph_compensations", effect_columns[index])) {
            fprintf(err, "workgraph migration: compensation column %s is missing\n",
                    effect_columns[index]);
            return -1;
        }
    }
    for (size_t index = 0; indexes[index] != NULL; ++index) {
        if (!index_exists(db, indexes[index])) {
            fprintf(err, "workgraph migration: required index %s is missing\n", indexes[index]);
            return -1;
        }
    }
    return 0;
}

static int apply_version_six(sqlite3 *db, FILE *err) {
    if (!table_exists(db, "workgraph_transitions") || !table_exists(db, "events")) {
        fprintf(err, "workgraph migration: version-5 evidence tables are missing\n");
        return -1;
    }
    if (add_column(db, "workgraph_transitions", "transition_domain",
                   "TEXT NOT NULL DEFAULT 'node'", err) != 0 ||
        add_column(db, "workgraph_transitions", "execution_attempt", "INTEGER", err) != 0 ||
        add_column(db, "workgraph_transitions", "compensation_attempt", "INTEGER", err) != 0 ||
        add_column(db, "events", "transition_domain", "TEXT NOT NULL DEFAULT 'node'", err) != 0 ||
        add_column(db, "events", "execution_attempt", "INTEGER", err) != 0 ||
        add_column(db, "events", "compensation_attempt", "INTEGER", err) != 0) {
        return -1;
    }
    return checked_exec(db,
        "CREATE INDEX idx_workgraph_transition_domain "
        "ON workgraph_transitions(mission_id,node_id,transition_domain);",
        err);
}

static int verify_version_six(sqlite3 *db, FILE *err) {
    static const char *transition_columns[] = {
        "transition_domain", "execution_attempt", "compensation_attempt", NULL
    };
    static const char *event_columns[] = {
        "transition_domain", "execution_attempt", "compensation_attempt", NULL
    };

    for (size_t index = 0; transition_columns[index] != NULL; ++index) {
        if (!column_exists(db, "workgraph_transitions", transition_columns[index])) {
            fprintf(err, "workgraph migration: workgraph_transitions.%s is missing\n",
                    transition_columns[index]);
            return -1;
        }
    }
    for (size_t index = 0; event_columns[index] != NULL; ++index) {
        if (!column_exists(db, "events", event_columns[index])) {
            fprintf(err, "workgraph migration: events.%s is missing\n", event_columns[index]);
            return -1;
        }
    }
    if (!index_exists(db, "idx_workgraph_transition_domain")) {
        fprintf(err, "workgraph migration: required index idx_workgraph_transition_domain is missing\n");
        return -1;
    }
    return 0;
}

static int apply_version_seven(sqlite3 *db, FILE *err) {
    static const struct {
        const char *name;
        const char *definition;
    } commit_columns[] = {
        {"commit_nonce", "TEXT"},
        {"commit_started_at_ms", "INTEGER"},
        {"verified_at_ms", "INTEGER"},
        {"expected_postcondition", "TEXT"},
        {NULL, NULL}
    };

    if (!table_exists(db, "workgraph_compensations")) {
        fprintf(err, "workgraph migration: version-4 compensation table is missing\n");
        return -1;
    }
    for (size_t index = 0; commit_columns[index].name != NULL; ++index) {
        if (add_column(db, "workgraph_compensations", commit_columns[index].name,
                       commit_columns[index].definition, err) != 0) {
            return -1;
        }
    }
    return 0;
}

static int verify_version_seven(sqlite3 *db, FILE *err) {
    static const char *commit_columns[] = {
        "commit_nonce", "commit_started_at_ms", "verified_at_ms",
        "expected_postcondition", NULL
    };

    for (size_t index = 0; commit_columns[index] != NULL; ++index) {
        if (!column_exists(db, "workgraph_compensations", commit_columns[index])) {
            fprintf(err, "workgraph migration: workgraph_compensations.%s is missing\n",
                    commit_columns[index]);
            return -1;
        }
    }
    return 0;
}

/*
 * Deterministic Feldera adapter boundary: real SQL views computing the
 * readiness contract (capability/effect-backlog/compensation-backlog/lease-
 * pressure/mission rollup) live from the same tables the fabric already
 * writes -- events, effects, workgraph_nodes, workgraph_compensations,
 * catalog_bindings. Recomputed on every query (SQLite views are not
 * cached), so this is genuinely live, not a stale snapshot.
 *
 * When a real Feldera instance is available, only the computation engine
 * changes (SQL pipelines submitted to Feldera instead of SQLite views);
 * the output contract (these view/column names) is what downstream readers
 * -- and, eventually, Feldera's own materialized views -- are expected to
 * match, so nothing downstream needs to change at that point.
 *
 * Each view is only created if its underlying tables already exist, so a
 * minimal workgraph-only database (used by narrower unit tests) migrates
 * cleanly without the full fabric.c schema present.
 */
static int apply_version_eight(sqlite3 *db, FILE *err) {
    if (table_exists(db, "workgraph_nodes")) {
        if (checked_exec(db,
            "CREATE VIEW IF NOT EXISTS bf_readiness_mission AS "
            "SELECT mission_id, count(*) AS total_nodes, "
            "sum(CASE WHEN status='complete' THEN 1 ELSE 0 END) AS complete_nodes, "
            "sum(CASE WHEN status='blocked' THEN 1 ELSE 0 END) AS blocked_nodes, "
            "sum(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_nodes, "
            "sum(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running_nodes "
            "FROM workgraph_nodes GROUP BY mission_id", err) != 0) {
            return -1;
        }
        if (checked_exec(db,
            "CREATE VIEW IF NOT EXISTS bf_readiness_lease_pressure AS "
            "SELECT mission_id, node_id, lease_expires_at_ms, "
            "CAST(strftime('%s','now') AS INTEGER)*1000 AS observed_at_ms, "
            "CASE WHEN lease_expires_at_ms < CAST(strftime('%s','now') AS INTEGER)*1000 "
            "THEN 1 ELSE 0 END AS expired "
            "FROM workgraph_nodes WHERE status='running' AND lease_expires_at_ms IS NOT NULL",
            err) != 0) {
            return -1;
        }
    }
    if (table_exists(db, "effects")) {
        if (checked_exec(db,
            "CREATE VIEW IF NOT EXISTS bf_readiness_effect_backlog AS "
            "SELECT mission_id, count(*) AS backlog_count "
            "FROM effects WHERE state NOT IN ('committed','compensated') "
            "GROUP BY mission_id", err) != 0) {
            return -1;
        }
    }
    if (table_exists(db, "workgraph_compensations")) {
        if (checked_exec(db,
            "CREATE VIEW IF NOT EXISTS bf_readiness_compensation_backlog AS "
            "SELECT mission_id, count(*) AS backlog_count "
            "FROM workgraph_compensations WHERE state NOT IN ('compensated','rolled_back') "
            "GROUP BY mission_id", err) != 0) {
            return -1;
        }
    }
    if (table_exists(db, "catalog_bindings") && table_exists(db, "fabric_meta")) {
        if (checked_exec(db,
            "CREATE VIEW IF NOT EXISTS bf_readiness_capability AS "
            "SELECT b.operator_id, b.binding_state, "
            "(SELECT value FROM fabric_meta WHERE key='catalog_generation') AS catalog_generation "
            "FROM catalog_bindings b WHERE b.operator_id LIKE 'command.%'", err) != 0) {
            return -1;
        }
    }
    return 0;
}

static int verify_version_eight(sqlite3 *db, FILE *err) {
    if (table_exists(db, "workgraph_nodes") &&
        (!view_exists(db, "bf_readiness_mission") ||
         !view_exists(db, "bf_readiness_lease_pressure"))) {
        fprintf(err, "workgraph migration: readiness views over workgraph_nodes are missing\n");
        return -1;
    }
    if (table_exists(db, "effects") && !view_exists(db, "bf_readiness_effect_backlog")) {
        fprintf(err, "workgraph migration: bf_readiness_effect_backlog is missing\n");
        return -1;
    }
    if (table_exists(db, "workgraph_compensations") &&
        !view_exists(db, "bf_readiness_compensation_backlog")) {
        fprintf(err, "workgraph migration: bf_readiness_compensation_backlog is missing\n");
        return -1;
    }
    if (table_exists(db, "catalog_bindings") && table_exists(db, "fabric_meta") &&
        !view_exists(db, "bf_readiness_capability")) {
        fprintf(err, "workgraph migration: bf_readiness_capability is missing\n");
        return -1;
    }
    return 0;
}

static int migration_record(sqlite3 *db, int version, FILE *err) {
    sqlite3_stmt *statement = NULL;
    char version_text[32];
    char timestamp[32];

    bf_workgraph_timestamp(timestamp);
    if (sqlite3_prepare_v2(db,
            "INSERT INTO schema_migrations(version,applied_at) VALUES(?,?)",
            -1, &statement, NULL) != SQLITE_OK) {
        fprintf(err, "workgraph migration: %s\n", sqlite3_errmsg(db));
        return -1;
    }
    sqlite3_bind_int(statement, 1, version);
    sqlite3_bind_text(statement, 2, timestamp, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        fprintf(err, "workgraph migration: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(statement);
        return -1;
    }
    sqlite3_finalize(statement);
    snprintf(version_text, sizeof(version_text), "%d", version);
    if (sqlite3_prepare_v2(db,
            "INSERT INTO fabric_meta(key,value) VALUES('schema_version',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, version_text, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        return -1;
    }
    sqlite3_finalize(statement);
    return 0;
}

static int current_version(sqlite3 *db) {
    sqlite3_stmt *statement = NULL;
    int version = 0;

    if (sqlite3_prepare_v2(db, "SELECT COALESCE(MAX(version),0) FROM schema_migrations",
                           -1, &statement, NULL) == SQLITE_OK &&
        sqlite3_step(statement) == SQLITE_ROW) {
        version = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return version;
}

int bf_workgraph_migrate_database(void *sqlite_database, FILE *err) {
    sqlite3 *db = sqlite_database;
    int version;

    if (db == NULL || err == NULL) {
        return -1;
    }
    version = current_version(db);
    if (version > BF_WORKGRAPH_SCHEMA_VERSION) {
        fprintf(err, "workgraph migration: database version %d is newer than runtime\n", version);
        return -1;
    }
    for (int target = version + 1; target <= BF_WORKGRAPH_SCHEMA_VERSION; ++target) {
        if (checked_exec(db, "BEGIN IMMEDIATE", err) != 0) {
            return -1;
        }
        if ((target == 4 &&
             (apply_version_four(db, err) != 0 || verify_version_four(db, err) != 0)) ||
            (target == 5 &&
             (apply_version_five(db, err) != 0 || verify_version_five(db, err) != 0)) ||
            (target == 6 &&
             (apply_version_six(db, err) != 0 || verify_version_six(db, err) != 0)) ||
            (target == 7 &&
             (apply_version_seven(db, err) != 0 || verify_version_seven(db, err) != 0)) ||
            (target == 8 &&
             (apply_version_eight(db, err) != 0 || verify_version_eight(db, err) != 0)) ||
            migration_record(db, target, err) != 0 ||
            checked_exec(db, "COMMIT", err) != 0) {
            sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
            return -1;
        }
    }
    if (current_version(db) != BF_WORKGRAPH_SCHEMA_VERSION) {
        fprintf(err, "workgraph migration: schema version verification failed\n");
        return -1;
    }
    if (verify_version_four(db, err) != 0) {
        return -1;
    }
    if (verify_version_five(db, err) != 0) {
        return -1;
    }
    if (verify_version_six(db, err) != 0) {
        return -1;
    }
    return verify_version_seven(db, err);
}
