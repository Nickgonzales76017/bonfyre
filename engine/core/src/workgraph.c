#define _POSIX_C_SOURCE 200809L
#include "workgraph_internal.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>

int64_t bf_workgraph_now_ms(void) {
    struct timespec value;

    if (clock_gettime(CLOCK_REALTIME, &value) != 0) {
        return (int64_t)time(NULL) * 1000;
    }
    return (int64_t)value.tv_sec * 1000 + value.tv_nsec / 1000000;
}

void bf_workgraph_timestamp(char output[32]) {
    time_t value = time(NULL);
    struct tm utc;

    if (gmtime_r(&value, &utc) == NULL ||
        strftime(output, 32, "%Y-%m-%dT%H:%M:%SZ", &utc) == 0) {
        snprintf(output, 32, "1970-01-01T00:00:00Z");
    }
}

BfWorkgraphResult bf_workgraph_result(BfWorkgraphStatus status, const char *mission_id,
                                      const char *node_id, const char *error_code,
                                      const char *error_message) {
    BfWorkgraphResult result;

    memset(&result, 0, sizeof(result));
    result.status = status;
    if (mission_id != NULL) {
        snprintf(result.mission_id, sizeof(result.mission_id), "%s", mission_id);
    }
    if (node_id != NULL) {
        snprintf(result.node_id, sizeof(result.node_id), "%s", node_id);
    }
    if (error_code != NULL) {
        snprintf(result.error_code, sizeof(result.error_code), "%s", error_code);
    }
    if (error_message != NULL) {
        snprintf(result.error_message, sizeof(result.error_message), "%s", error_message);
    }
    return result;
}

const char *bf_workgraph_status_name(BfWorkgraphStatus status) {
    switch (status) {
        case BF_WORKGRAPH_OK: return "ok";
        case BF_WORKGRAPH_NOT_FOUND: return "not_found";
        case BF_WORKGRAPH_NOT_ELIGIBLE: return "not_eligible";
        case BF_WORKGRAPH_STALE_CLAIM: return "stale_claim";
        case BF_WORKGRAPH_CONFLICT: return "conflict";
        case BF_WORKGRAPH_INVALID: return "invalid";
        case BF_WORKGRAPH_STORAGE_ERROR: return "storage_error";
    }
    return "unknown";
}

int bf_workgraph_open_database(void *sqlite_database, BfWorkgraph **out, FILE *err) {
    BfWorkgraph *graph;

    if (sqlite_database == NULL || out == NULL || err == NULL) {
        return -1;
    }
    if (bf_workgraph_migrate_database(sqlite_database, err) != 0) {
        return -1;
    }
    graph = calloc(1, sizeof(*graph));
    if (graph == NULL) {
        return -1;
    }
    graph->db = sqlite_database;
    graph->err = err;
    graph->owns_database = 0;
    sqlite3_busy_timeout(graph->db, 5000);
    *out = graph;
    return 0;
}

int bf_workgraph_open(const char *database_path, BfWorkgraph **out, FILE *err) {
    sqlite3 *database = NULL;
    BfWorkgraph *graph = NULL;

    if (database_path == NULL || out == NULL || err == NULL) {
        return -1;
    }
    if (sqlite3_open_v2(database_path, &database,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
        fprintf(err, "workgraph: cannot open database: %s\n",
                database != NULL ? sqlite3_errmsg(database) : "unknown error");
        sqlite3_close(database);
        return -1;
    }
    sqlite3_busy_timeout(database, 5000);
    if (sqlite3_exec(database,
            "PRAGMA foreign_keys=ON;"
            "CREATE TABLE IF NOT EXISTS fabric_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);"
            "CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY,applied_at TEXT NOT NULL);"
            "CREATE TABLE IF NOT EXISTS missions("
            "id TEXT PRIMARY KEY,status TEXT NOT NULL,context_generation TEXT NOT NULL,"
            "catalog_generation TEXT NOT NULL,provider_generation TEXT NOT NULL,input_snapshot TEXT NOT NULL,"
            "artifact_root TEXT NOT NULL,workgraph_cursor TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);"
            "CREATE TABLE IF NOT EXISTS workgraph_nodes("
            "mission_id TEXT NOT NULL,node_id TEXT NOT NULL,operator_id TEXT NOT NULL,status TEXT NOT NULL,"
            "attempt INTEGER NOT NULL DEFAULT 0,retry_limit INTEGER NOT NULL DEFAULT 0,timeout_seconds INTEGER NOT NULL,"
            "PRIMARY KEY(mission_id,node_id),FOREIGN KEY(mission_id) REFERENCES missions(id));"
            "CREATE TABLE IF NOT EXISTS events("
            "id TEXT PRIMARY KEY,mission_id TEXT,task_id TEXT,attempt INTEGER NOT NULL,actor TEXT NOT NULL,"
            "operator_id TEXT,provider_id TEXT,model_id TEXT,start_at TEXT NOT NULL,end_at TEXT,duration_ms INTEGER,"
            "input_uri TEXT,output_uri TEXT,effect_class TEXT NOT NULL,status TEXT NOT NULL,error_code TEXT,receipt_id TEXT);"
            "CREATE TABLE IF NOT EXISTS receipts("
            "id TEXT PRIMARY KEY,subject_kind TEXT NOT NULL,subject_id TEXT NOT NULL,content_hash TEXT NOT NULL,"
            "payload TEXT NOT NULL,created_at TEXT NOT NULL);",
            NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(err, "workgraph: base schema initialization failed: %s\n", sqlite3_errmsg(database));
        sqlite3_close(database);
        return -1;
    }
    if (bf_workgraph_open_database(database, &graph, err) != 0) {
        sqlite3_close(database);
        return -1;
    }
    graph->owns_database = 1;
    *out = graph;
    return 0;
}

BfWorkgraphResult bf_workgraph_create_mission(BfWorkgraph *graph, const char *mission_id) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, NULL, NULL, NULL);
    sqlite3_stmt *statement = NULL;
    char timestamp[32];

    if (graph == NULL || mission_id == NULL || mission_id[0] == '\0') {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, NULL,
                                   "invalid_mission", "mission identifier is required");
    }
    bf_workgraph_timestamp(timestamp);
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (sqlite3_prepare_v2(graph->db,
            "INSERT INTO missions(id,status,context_generation,catalog_generation,provider_generation,"
            "input_snapshot,artifact_root,workgraph_cursor,created_at,updated_at) "
            "VALUES(?,'defined','context-1','workgraph-4','providers-1','{}','','',?,?)",
            -1, &statement, NULL) != SQLITE_OK) goto failure;
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, timestamp, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, timestamp, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        result.status = BF_WORKGRAPH_CONFLICT;
        snprintf(result.error_code, sizeof(result.error_code), "mission_exists");
        snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
        bf_workgraph_rollback(graph);
        return result;
    }
    sqlite3_finalize(statement);
    if (bf_workgraph_write_evidence(graph, &result, "mission_created", "scheduler", NULL) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) goto failure;
    return result;

failure:
    if (statement != NULL) sqlite3_finalize(statement);
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "mission_create_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

void bf_workgraph_close(BfWorkgraph *graph) {
    if (graph == NULL) {
        return;
    }
    if (graph->owns_database) {
        sqlite3_close(graph->db);
    }
    free(graph);
}

int bf_workgraph_exec(BfWorkgraph *graph, const char *sql, BfWorkgraphResult *result) {
    char *message = NULL;

    if (sqlite3_exec(graph->db, sql, NULL, NULL, &message) != SQLITE_OK) {
        result->status = BF_WORKGRAPH_STORAGE_ERROR;
        snprintf(result->error_code, sizeof(result->error_code), "storage_error");
        snprintf(result->error_message, sizeof(result->error_message), "%s",
                 message != NULL ? message : sqlite3_errmsg(graph->db));
        sqlite3_free(message);
        return -1;
    }
    return 0;
}

int bf_workgraph_begin(BfWorkgraph *graph, BfWorkgraphResult *result) {
    return bf_workgraph_exec(graph, "BEGIN IMMEDIATE", result);
}

int bf_workgraph_commit(BfWorkgraph *graph, BfWorkgraphResult *result) {
    return bf_workgraph_exec(graph, "COMMIT", result);
}

void bf_workgraph_rollback(BfWorkgraph *graph) {
    sqlite3_exec(graph->db, "ROLLBACK", NULL, NULL, NULL);
}

void bf_workgraph_hash_token(const char *token, char digest[65]) {
    if (token == NULL) {
        digest[0] = '\0';
        return;
    }
    bf_sha256_hex((const uint8_t *)token, strlen(token), digest);
}

int bf_workgraph_generate_token(char token[65], char digest[65]) {
    unsigned char bytes[32];
    static const char hex[] = "0123456789abcdef";
    FILE *random = fopen("/dev/urandom", "rb");

    if (random == NULL || fread(bytes, 1, sizeof(bytes), random) != sizeof(bytes)) {
        if (random != NULL) {
            fclose(random);
        }
        return -1;
    }
    fclose(random);
    for (size_t index = 0; index < sizeof(bytes); ++index) {
        token[index * 2] = hex[bytes[index] >> 4];
        token[index * 2 + 1] = hex[bytes[index] & 15];
    }
    token[64] = '\0';
    bf_workgraph_hash_token(token, digest);
    return 0;
}

int bf_workgraph_backoff_ms(const char *mission_id, const char *node_id, int attempt,
                            int64_t base_ms, double multiplier, int64_t maximum_ms,
                            int jitter_percent, int64_t *backoff_ms) {
    char seed[384];
    char digest[65];
    double delay;
    int64_t bounded;

    if (base_ms < 0 || multiplier < 1.0 || maximum_ms < base_ms ||
        jitter_percent < 0 || jitter_percent > 100 || backoff_ms == NULL) {
        return -1;
    }
    delay = (double)base_ms;
    for (int index = 1; index < attempt && delay < (double)maximum_ms; ++index) {
        delay *= multiplier;
    }
    bounded = delay > (double)maximum_ms ? maximum_ms : (int64_t)delay;
    if (jitter_percent > 0 && bounded > 0) {
        unsigned long value;
        int64_t range = bounded * jitter_percent / 100;
        snprintf(seed, sizeof(seed), "%s|%s|%d", mission_id, node_id, attempt);
        bf_sha256_hex((const uint8_t *)seed, strlen(seed), digest);
        value = strtoul(digest, NULL, 16);
        bounded = bounded - range + (int64_t)(value % (unsigned long)(range * 2 + 1));
    }
    *backoff_ms = bounded;
    return 0;
}

int bf_workgraph_load_result(BfWorkgraph *graph, BfWorkgraphResult *result) {
    sqlite3_stmt *statement = NULL;

    if (sqlite3_prepare_v2(graph->db,
            "SELECT attempt,COALESCE(lease_owner,''),COALESCE(lease_expires_at_ms,0),"
            "COALESCE(next_attempt_at_ms,0),status FROM workgraph_nodes "
            "WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, result->mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, result->node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        result->status = BF_WORKGRAPH_NOT_FOUND;
        snprintf(result->error_code, sizeof(result->error_code), "node_not_found");
        snprintf(result->error_message, sizeof(result->error_message), "work node was not found");
        return -1;
    }
    result->attempt = sqlite3_column_int(statement, 0);
    snprintf(result->worker_id, sizeof(result->worker_id), "%s", sqlite3_column_text(statement, 1));
    result->lease_expires_at_ms = sqlite3_column_int64(statement, 2);
    result->next_attempt_at_ms = sqlite3_column_int64(statement, 3);
    snprintf(result->node_status, sizeof(result->node_status), "%s", sqlite3_column_text(statement, 4));
    sqlite3_finalize(statement);
    return 0;
}

int bf_workgraph_promote_dependents(BfWorkgraph *graph, const char *mission_id,
                                    BfWorkgraphResult *result) {
    sqlite3_stmt *statement = NULL;
    int64_t now = bf_workgraph_now_ms();
    const char *sql =
        "UPDATE workgraph_nodes AS child SET status='ready',ready_at_ms=?,updated_at_ms=? "
        "WHERE child.mission_id=? AND child.status IN ('defined','blocked') "
        "AND child.cancellation_state='none' "
        "AND NOT EXISTS (SELECT 1 FROM workgraph_dependencies d "
        "JOIN workgraph_nodes parent ON parent.mission_id=d.mission_id "
        "AND parent.node_id=d.depends_on_node_id "
        "WHERE d.mission_id=child.mission_id AND d.node_id=child.node_id "
        "AND ((d.dependency_policy='require_success' AND parent.status!='complete') "
        "OR (d.dependency_policy='continue' AND parent.status NOT IN "
        "('complete','dead_letter','cancelled'))))";

    if (sqlite3_prepare_v2(graph->db, sql, -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_int64(statement, 1, now);
    sqlite3_bind_int64(statement, 2, now);
    sqlite3_bind_text(statement, 3, mission_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        result->status = BF_WORKGRAPH_STORAGE_ERROR;
        return -1;
    }
    sqlite3_finalize(statement);
    return 0;
}

int bf_workgraph_update_mission(BfWorkgraph *graph, const char *mission_id,
                                BfWorkgraphResult *result) {
    sqlite3_stmt *statement = NULL;
    char timestamp[32];
    const char *sql =
        "UPDATE missions SET status=CASE "
        "WHEN EXISTS(SELECT 1 FROM workgraph_nodes WHERE mission_id=? AND status='dead_letter') "
        "AND NOT EXISTS(SELECT 1 FROM workgraph_nodes WHERE mission_id=? AND status NOT IN "
        "('complete','dead_letter','cancelled')) THEN 'failed' "
        "WHEN EXISTS(SELECT 1 FROM workgraph_nodes WHERE mission_id=? AND status='cancelled') "
        "AND NOT EXISTS(SELECT 1 FROM workgraph_nodes WHERE mission_id=? AND status NOT IN "
        "('complete','dead_letter','cancelled')) THEN 'cancelled' "
        "WHEN EXISTS(SELECT 1 FROM workgraph_nodes WHERE mission_id=?) "
        "AND NOT EXISTS(SELECT 1 FROM workgraph_nodes WHERE mission_id=? AND status!='complete') "
        "THEN 'complete' ELSE 'running' END,"
        "workgraph_cursor=CASE WHEN NOT EXISTS(SELECT 1 FROM workgraph_nodes WHERE mission_id=? "
        "AND status NOT IN ('complete','dead_letter','cancelled')) THEN 'terminal' ELSE 'active' END,"
        "updated_at=? WHERE id=?";

    bf_workgraph_timestamp(timestamp);
    if (sqlite3_prepare_v2(graph->db, sql, -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    for (int index = 1; index <= 7; ++index) {
        sqlite3_bind_text(statement, index, mission_id, -1, SQLITE_TRANSIENT);
    }
    sqlite3_bind_text(statement, 8, timestamp, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 9, mission_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        result->status = BF_WORKGRAPH_STORAGE_ERROR;
        return -1;
    }
    sqlite3_finalize(statement);
    return 0;
}

BfWorkgraphResult bf_workgraph_add_node(BfWorkgraph *graph, const BfWorkgraphNodeSpec *spec) {
    BfWorkgraphResult result;
    sqlite3_stmt *statement = NULL;
    int64_t now = bf_workgraph_now_ms();

    if (graph == NULL || spec == NULL || spec->mission_id == NULL || spec->node_id == NULL ||
        spec->operator_id == NULL) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, NULL, NULL,
                                   "invalid_argument", "node specification is incomplete");
    }
    result = bf_workgraph_result(BF_WORKGRAPH_OK, spec->mission_id, spec->node_id, NULL, NULL);
    if (bf_workgraph_begin(graph, &result) != 0) {
        return result;
    }
    if (sqlite3_prepare_v2(graph->db,
            "INSERT INTO workgraph_nodes(mission_id,node_id,operator_id,input_uri,status,attempt,"
            "retry_limit,timeout_seconds,depends_on,backoff_policy,backoff_base_ms,"
            "backoff_multiplier,backoff_max_ms,backoff_jitter_percent,cancellation_state,"
            "compensation_operator,parent_node,fanout_group,fanin_required,family,priority,"
            "ready_at_ms,created_at_ms,updated_at_ms) "
            "VALUES(?,?,?,?,'ready',0,?,?,'[]','exponential',?,?,?,?, 'none',?,?,?,?,?,?,?, ?,?)",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, spec->mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, spec->node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, spec->operator_id, -1, SQLITE_TRANSIENT);
    if (spec->input_uri != NULL) sqlite3_bind_text(statement, 4, spec->input_uri, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(statement, 4);
    sqlite3_bind_int(statement, 5, spec->retry_limit >= 0 ? spec->retry_limit : 0);
    sqlite3_bind_int(statement, 6, spec->timeout_seconds > 0 ? spec->timeout_seconds : 30);
    sqlite3_bind_int64(statement, 7, spec->backoff_base_ms >= 0 ? spec->backoff_base_ms : 1000);
    sqlite3_bind_double(statement, 8, spec->backoff_multiplier >= 1.0 ? spec->backoff_multiplier : 2.0);
    sqlite3_bind_int64(statement, 9, spec->backoff_max_ms > 0 ? spec->backoff_max_ms : 60000);
    sqlite3_bind_int(statement, 10, spec->jitter_percent);
    if (spec->compensation_operator != NULL) sqlite3_bind_text(statement, 11, spec->compensation_operator, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(statement, 11);
    if (spec->parent_node != NULL) sqlite3_bind_text(statement, 12, spec->parent_node, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(statement, 12);
    if (spec->fanout_group != NULL) sqlite3_bind_text(statement, 13, spec->fanout_group, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(statement, 13);
    sqlite3_bind_int(statement, 14, spec->fanin_required);
    sqlite3_bind_text(statement, 15, spec->family != NULL ? spec->family : "default", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 16, spec->priority != 0 ? spec->priority : 100);
    sqlite3_bind_int64(statement, 17, now);
    sqlite3_bind_int64(statement, 18, now);
    sqlite3_bind_int64(statement, 19, now);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        result.status = sqlite3_extended_errcode(graph->db) == SQLITE_CONSTRAINT_PRIMARYKEY ?
            BF_WORKGRAPH_CONFLICT : BF_WORKGRAPH_STORAGE_ERROR;
        snprintf(result.error_code, sizeof(result.error_code), "node_insert_failed");
        snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
        bf_workgraph_rollback(graph);
        return result;
    }
    sqlite3_finalize(statement);
    if (bf_workgraph_write_evidence(graph, &result, "node_added", "scheduler", NULL) != 0 ||
        bf_workgraph_update_mission(graph, spec->mission_id, &result) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) {
        bf_workgraph_rollback(graph);
        result.status = BF_WORKGRAPH_STORAGE_ERROR;
    }
    return result;

storage_failure:
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "storage_error");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    bf_workgraph_rollback(graph);
    return result;
}

BfWorkgraphResult bf_workgraph_add_dependency(BfWorkgraph *graph, const char *mission_id,
                                               const char *node_id, const char *depends_on_node_id,
                                               const char *dependency_policy) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    sqlite3_stmt *statement = NULL;

    if (graph == NULL || mission_id == NULL || node_id == NULL || depends_on_node_id == NULL ||
        strcmp(node_id, depends_on_node_id) == 0) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id,
                                   "invalid_dependency", "dependency is invalid");
    }
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (sqlite3_prepare_v2(graph->db,
            "INSERT INTO workgraph_dependencies(mission_id,node_id,depends_on_node_id,dependency_policy) "
            "VALUES(?,?,?,?)",
            -1, &statement, NULL) != SQLITE_OK) {
        goto failure;
    }
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, depends_on_node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, dependency_policy != NULL ? dependency_policy : "require_success",
                      -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        goto failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_nodes SET status='blocked',ready_at_ms=NULL,updated_at_ms=? "
            "WHERE mission_id=? AND node_id=? AND status IN ('ready','defined','blocked')",
            -1, &statement, NULL) != SQLITE_OK) {
        goto failure;
    }
    sqlite3_bind_int64(statement, 1, bf_workgraph_now_ms());
    sqlite3_bind_text(statement, 2, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        goto failure;
    }
    sqlite3_finalize(statement);
    if (bf_workgraph_promote_dependents(graph, mission_id, &result) != 0 ||
        bf_workgraph_write_evidence(graph, &result, "dependency_added", "scheduler", NULL) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) {
        goto failure;
    }
    return result;

failure:
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "dependency_insert_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

BfWorkgraphResult bf_workgraph_status(BfWorkgraph *graph, const char *mission_id,
                                      const char *node_id) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);

    if (graph == NULL || mission_id == NULL || node_id == NULL ||
        bf_workgraph_load_result(graph, &result) != 0) {
        if (result.status == BF_WORKGRAPH_OK) result.status = BF_WORKGRAPH_INVALID;
    }
    return result;
}

BfWorkgraphResult bf_workgraph_resume(BfWorkgraph *graph, const char *mission_id) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, NULL, NULL, NULL);

    if (graph == NULL || mission_id == NULL) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, NULL,
                                   "invalid_argument", "mission is required");
    }
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (bf_workgraph_promote_dependents(graph, mission_id, &result) != 0 ||
        bf_workgraph_update_mission(graph, mission_id, &result) != 0 ||
        bf_workgraph_write_evidence(graph, &result, "mission_resumed", "scheduler", NULL) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) {
        bf_workgraph_rollback(graph);
        result.status = BF_WORKGRAPH_STORAGE_ERROR;
    }
    return result;
}

BfWorkgraphResult bf_workgraph_cancel_mission(BfWorkgraph *graph, const char *mission_id) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, NULL, NULL, NULL);
    sqlite3_stmt *statement = NULL;
    int64_t now = bf_workgraph_now_ms();

    if (graph == NULL || mission_id == NULL) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, NULL,
                                   "invalid_argument", "mission is required");
    }
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_nodes SET cancellation_state='requested',"
            "status=CASE WHEN status='running' THEN 'cancel_requested' ELSE 'cancelled' END,"
            "terminal_at_ms=CASE WHEN status='running' THEN NULL ELSE ? END,updated_at_ms=? "
            "WHERE mission_id=? AND status NOT IN ('complete','dead_letter','cancelled')",
            -1, &statement, NULL) != SQLITE_OK) {
        goto failure;
    }
    sqlite3_bind_int64(statement, 1, now);
    sqlite3_bind_int64(statement, 2, now);
    sqlite3_bind_text(statement, 3, mission_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        goto failure;
    }
    sqlite3_finalize(statement);
    if (bf_workgraph_update_mission(graph, mission_id, &result) != 0 ||
        bf_workgraph_write_evidence(graph, &result, "mission_cancelled", "scheduler", NULL) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) {
        goto failure;
    }
    return result;

failure:
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "mission_cancel_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}
