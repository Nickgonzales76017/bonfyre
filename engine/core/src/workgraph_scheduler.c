#include "workgraph_internal.h"

#include <stdio.h>
#include <string.h>

static BfWorkgraphResult claim_selected(BfWorkgraph *graph, const char *mission_id,
                                        const char *node_id, const BfWorkgraphClaimSpec *spec) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    sqlite3_stmt *statement = NULL;
    char token_digest[65];
    int64_t now = bf_workgraph_now_ms();
    int64_t lease_ms = spec->lease_ms > 0 ? spec->lease_ms : 30000;

    if (bf_workgraph_generate_token(result.claim_token, token_digest) != 0) {
        result.status = BF_WORKGRAPH_STORAGE_ERROR;
        snprintf(result.error_code, sizeof(result.error_code), "random_source_failed");
        snprintf(result.error_message, sizeof(result.error_message),
                 "cryptographic random source is unavailable");
        return result;
    }
    result.lease_expires_at_ms = now + lease_ms;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_nodes SET status='running',attempt=attempt+1,lease_owner=?,"
            "claim_token_hash=?,claim_acquired_at_ms=?,lease_expires_at_ms=?,"
            "visibility_deadline_ms=?,last_heartbeat_at_ms=?,next_attempt_at_ms=NULL,updated_at_ms=? "
            "WHERE mission_id=? AND node_id=? AND status IN ('ready','retry_wait') "
            "AND cancellation_state='none' AND (next_attempt_at_ms IS NULL OR next_attempt_at_ms<=?)",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, spec->worker_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, token_digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 3, now);
    sqlite3_bind_int64(statement, 4, result.lease_expires_at_ms);
    sqlite3_bind_int64(statement, 5, result.lease_expires_at_ms);
    sqlite3_bind_int64(statement, 6, now);
    sqlite3_bind_int64(statement, 7, now);
    sqlite3_bind_text(statement, 8, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 9, node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 10, now);
    if (sqlite3_step(statement) != SQLITE_DONE || sqlite3_changes(graph->db) != 1) {
        sqlite3_finalize(statement);
        result.status = BF_WORKGRAPH_NOT_ELIGIBLE;
        snprintf(result.error_code, sizeof(result.error_code), "node_not_claimable");
        snprintf(result.error_message, sizeof(result.error_message), "work node is not claimable");
        result.claim_token[0] = '\0';
        return result;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (bf_workgraph_load_result(graph, &result) != 0) {
        goto storage_failure;
    }
    snprintf(result.worker_id, sizeof(result.worker_id), "%s", spec->worker_id);
    if (sqlite3_prepare_v2(graph->db,
            "INSERT INTO workgraph_attempts(mission_id,node_id,attempt,worker_id,"
            "claim_token_digest,started_at_ms,lease_expires_at_ms) VALUES(?,?,?,?,?,?,?)",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 3, result.attempt);
    sqlite3_bind_text(statement, 4, spec->worker_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, token_digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 6, now);
    sqlite3_bind_int64(statement, 7, result.lease_expires_at_ms);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    if (bf_workgraph_write_evidence(graph, &result, "claimed", spec->worker_id, NULL) != 0 ||
        bf_workgraph_update_mission(graph, mission_id, &result) != 0) {
        goto storage_failure;
    }
    return result;

storage_failure:
    if (statement != NULL) sqlite3_finalize(statement);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "claim_storage_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    result.claim_token[0] = '\0';
    return result;
}

static BfWorkgraphResult claim(BfWorkgraph *graph, const char *mission_filter,
                               const char *node_filter, const BfWorkgraphClaimSpec *spec) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK,
                                                   mission_filter, node_filter, NULL, NULL);
    sqlite3_stmt *statement = NULL;
    const char *sql =
        "SELECT n.mission_id,n.node_id FROM workgraph_nodes n "
        "JOIN missions m ON m.id=n.mission_id "
        "WHERE n.status IN ('ready','retry_wait') AND n.cancellation_state='none' "
        "AND m.status NOT IN ('complete','failed','cancelled') "
        "AND (n.next_attempt_at_ms IS NULL OR n.next_attempt_at_ms<=?) "
        "AND n.attempt<=n.retry_limit "
        "AND (? IS NULL OR n.family=?) AND (? IS NULL OR n.mission_id=?) "
        "AND (? IS NULL OR n.node_id=?) "
        "AND NOT EXISTS(SELECT 1 FROM workgraph_dependencies d "
        "JOIN workgraph_nodes parent ON parent.mission_id=d.mission_id "
        "AND parent.node_id=d.depends_on_node_id "
        "WHERE d.mission_id=n.mission_id AND d.node_id=n.node_id "
        "AND ((d.dependency_policy='require_success' AND parent.status!='complete') "
        "OR (d.dependency_policy='continue' AND parent.status NOT IN "
        "('complete','dead_letter','cancelled')))) "
        "ORDER BY n.priority DESC,COALESCE(n.ready_at_ms,n.created_at_ms),m.created_at,"
        "n.created_at_ms,n.mission_id,n.node_id LIMIT 1";
    char selected_mission[128];
    char selected_node[128];

    if (graph == NULL || spec == NULL || spec->worker_id == NULL || spec->worker_id[0] == '\0') {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_filter, node_filter,
                                   "invalid_worker", "worker identity is required");
    }
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (sqlite3_prepare_v2(graph->db, sql, -1, &statement, NULL) != SQLITE_OK) {
        goto failure;
    }
    sqlite3_bind_int64(statement, 1, bf_workgraph_now_ms());
    if (spec->family != NULL) {
        sqlite3_bind_text(statement, 2, spec->family, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 3, spec->family, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(statement, 2);
        sqlite3_bind_null(statement, 3);
    }
    if (mission_filter != NULL) {
        sqlite3_bind_text(statement, 4, mission_filter, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 5, mission_filter, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(statement, 4);
        sqlite3_bind_null(statement, 5);
    }
    if (node_filter != NULL) {
        sqlite3_bind_text(statement, 6, node_filter, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 7, node_filter, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(statement, 6);
        sqlite3_bind_null(statement, 7);
    }
    if (sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        bf_workgraph_rollback(graph);
        return bf_workgraph_result(BF_WORKGRAPH_NOT_ELIGIBLE, mission_filter, node_filter,
                                   "no_eligible_node", "no eligible work node is available");
    }
    snprintf(selected_mission, sizeof(selected_mission), "%s", sqlite3_column_text(statement, 0));
    snprintf(selected_node, sizeof(selected_node), "%s", sqlite3_column_text(statement, 1));
    sqlite3_finalize(statement);
    result = claim_selected(graph, selected_mission, selected_node, spec);
    if (result.status != BF_WORKGRAPH_OK || bf_workgraph_commit(graph, &result) != 0) {
        bf_workgraph_rollback(graph);
        result.claim_token[0] = '\0';
    }
    return result;

failure:
    if (statement != NULL) sqlite3_finalize(statement);
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "claim_query_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

BfWorkgraphResult bf_workgraph_claim_next(BfWorkgraph *graph, const BfWorkgraphClaimSpec *spec) {
    return claim(graph, NULL, NULL, spec);
}

BfWorkgraphResult bf_workgraph_claim_node(BfWorkgraph *graph, const char *mission_id,
                                          const char *node_id, const BfWorkgraphClaimSpec *spec) {
    return claim(graph, mission_id, node_id, spec);
}

int bf_workgraph_validate_claim(BfWorkgraph *graph, BfWorkgraphResult *result,
                                const char *worker_id, const char *claim_token,
                                int allow_cancel_requested) {
    sqlite3_stmt *statement = NULL;
    char supplied_digest[65];
    const char *status;
    const char *owner;
    const char *stored_digest;
    const char *cancellation;
    int valid = 0;

    if (worker_id == NULL || claim_token == NULL || strlen(claim_token) != 64) {
        goto stale;
    }
    bf_workgraph_hash_token(claim_token, supplied_digest);
    if (sqlite3_prepare_v2(graph->db,
            "SELECT status,COALESCE(lease_owner,''),COALESCE(claim_token_hash,''),"
            "COALESCE(lease_expires_at_ms,0),attempt,cancellation_state "
            "FROM workgraph_nodes WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, result->mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, result->node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        status = (const char *)sqlite3_column_text(statement, 0);
        owner = (const char *)sqlite3_column_text(statement, 1);
        stored_digest = (const char *)sqlite3_column_text(statement, 2);
        result->lease_expires_at_ms = sqlite3_column_int64(statement, 3);
        result->attempt = sqlite3_column_int(statement, 4);
        cancellation = (const char *)sqlite3_column_text(statement, 5);
        valid = ((!strcmp(status, "running") && !strcmp(cancellation, "none")) ||
                 (allow_cancel_requested && !strcmp(status, "cancel_requested") &&
                  !strcmp(cancellation, "requested"))) &&
                !strcmp(owner, worker_id) && !strcmp(stored_digest, supplied_digest) &&
                result->lease_expires_at_ms > bf_workgraph_now_ms();
    }
    sqlite3_finalize(statement);
    if (valid) {
        snprintf(result->worker_id, sizeof(result->worker_id), "%s", worker_id);
        return 0;
    }

stale:
    result->status = BF_WORKGRAPH_STALE_CLAIM;
    snprintf(result->error_code, sizeof(result->error_code), "stale_claim");
    snprintf(result->error_message, sizeof(result->error_message),
             "claim is stale, expired, cancelled, or owned by another worker");
    return -1;
}

BfWorkgraphResult bf_workgraph_renew(BfWorkgraph *graph, const char *mission_id,
                                     const char *node_id, const char *worker_id,
                                     const char *claim_token, int64_t lease_ms) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    sqlite3_stmt *statement = NULL;
    int64_t now = bf_workgraph_now_ms();

    if (graph == NULL || lease_ms <= 0) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id,
                                   "invalid_lease", "lease duration must be positive");
    }
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (bf_workgraph_validate_claim(graph, &result, worker_id, claim_token, 0) != 0) goto failure;
    result.lease_expires_at_ms = now + lease_ms;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_nodes SET lease_expires_at_ms=?,visibility_deadline_ms=?,"
            "last_heartbeat_at_ms=?,updated_at_ms=? WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    sqlite3_bind_int64(statement, 1, result.lease_expires_at_ms);
    sqlite3_bind_int64(statement, 2, result.lease_expires_at_ms);
    sqlite3_bind_int64(statement, 3, now);
    sqlite3_bind_int64(statement, 4, now);
    sqlite3_bind_text(statement, 5, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); goto storage_failure; }
    sqlite3_finalize(statement);
    statement = NULL;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_attempts SET lease_expires_at_ms=? "
            "WHERE mission_id=? AND node_id=? AND attempt=?",
            -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    sqlite3_bind_int64(statement, 1, result.lease_expires_at_ms);
    sqlite3_bind_text(statement, 2, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 4, result.attempt);
    if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); goto storage_failure; }
    sqlite3_finalize(statement);
    if (bf_workgraph_write_evidence(graph, &result, "lease_renewed", worker_id, NULL) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) goto storage_failure;
    return result;

failure:
    bf_workgraph_rollback(graph);
    return result;
storage_failure:
    if (statement != NULL) sqlite3_finalize(statement);
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "renew_storage_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

static int finish_attempt(BfWorkgraph *graph, const BfWorkgraphResult *result,
                          const char *outcome, const char *error_code, int64_t now) {
    sqlite3_stmt *statement = NULL;

    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_attempts SET finished_at_ms=?,outcome=?,error_code=? "
            "WHERE mission_id=? AND node_id=? AND attempt=?",
            -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int64(statement, 1, now);
    sqlite3_bind_text(statement, 2, outcome, -1, SQLITE_TRANSIENT);
    if (error_code != NULL) sqlite3_bind_text(statement, 3, error_code, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(statement, 3);
    sqlite3_bind_text(statement, 4, result->mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, result->node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 6, result->attempt);
    if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); return -1; }
    sqlite3_finalize(statement);
    return 0;
}

BfWorkgraphResult bf_workgraph_complete(BfWorkgraph *graph, const char *mission_id,
                                        const char *node_id, const char *worker_id,
                                        const char *claim_token, const char *output_uri) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    sqlite3_stmt *statement = NULL;
    int64_t now = bf_workgraph_now_ms();

    if (graph == NULL) return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id, "invalid_argument", "graph is required");
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (bf_workgraph_validate_claim(graph, &result, worker_id, claim_token, 0) != 0) goto failure;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_nodes SET status='complete',output_uri=?,terminal_at_ms=?,updated_at_ms=?,"
            "lease_owner=NULL,claim_token_hash=NULL,lease_expires_at_ms=NULL,visibility_deadline_ms=NULL "
            "WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    if (output_uri != NULL) sqlite3_bind_text(statement, 1, output_uri, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(statement, 1);
    sqlite3_bind_int64(statement, 2, now);
    sqlite3_bind_int64(statement, 3, now);
    sqlite3_bind_text(statement, 4, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); goto storage_failure; }
    sqlite3_finalize(statement);
    if (finish_attempt(graph, &result, "complete", NULL, now) != 0 ||
        bf_workgraph_promote_dependents(graph, mission_id, &result) != 0 ||
        bf_workgraph_update_mission(graph, mission_id, &result) != 0 ||
        bf_workgraph_write_evidence(graph, &result, "completed", worker_id, NULL) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) goto storage_failure;
    return result;

failure:
    bf_workgraph_rollback(graph);
    return result;
storage_failure:
    if (statement != NULL) sqlite3_finalize(statement);
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "complete_storage_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

BfWorkgraphResult bf_workgraph_fail(BfWorkgraph *graph, const char *mission_id,
                                    const char *node_id, const char *worker_id,
                                    const char *claim_token, const BfWorkgraphFailure *failure) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    sqlite3_stmt *statement = NULL;
    char failure_class[32];
    int retry_limit;
    int64_t base_ms;
    double multiplier;
    int64_t maximum_ms;
    int jitter_percent;
    int retryable;
    int64_t delay = 0;
    int64_t now = bf_workgraph_now_ms();
    const char *new_status;

    if (graph == NULL || failure == NULL || failure->failure_class == NULL) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id,
                                   "invalid_failure", "failure class is required");
    }
    snprintf(failure_class, sizeof(failure_class), "%s", failure->failure_class);
    retryable = !strcmp(failure_class, "transient") || !strcmp(failure_class, "timeout") ||
                !strcmp(failure_class, "resource");
    if (!retryable && strcmp(failure_class, "permanent") && strcmp(failure_class, "validation") &&
        strcmp(failure_class, "dependency")) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id,
                                   "invalid_failure_class", "failure class is not supported");
    }
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (bf_workgraph_validate_claim(graph, &result, worker_id, claim_token, 0) != 0) goto failure;
    if (sqlite3_prepare_v2(graph->db,
            "SELECT retry_limit,backoff_base_ms,backoff_multiplier,backoff_max_ms,"
            "backoff_jitter_percent FROM workgraph_nodes WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) { sqlite3_finalize(statement); goto storage_failure; }
    retry_limit = sqlite3_column_int(statement, 0);
    base_ms = sqlite3_column_int64(statement, 1);
    multiplier = sqlite3_column_double(statement, 2);
    maximum_ms = sqlite3_column_int64(statement, 3);
    jitter_percent = sqlite3_column_int(statement, 4);
    sqlite3_finalize(statement);
    statement = NULL;
    retryable = retryable && result.attempt <= retry_limit;
    new_status = retryable ? "retry_wait" : "dead_letter";
    if (retryable && bf_workgraph_backoff_ms(mission_id, node_id, result.attempt, base_ms,
                                             multiplier, maximum_ms, jitter_percent, &delay) != 0) {
        goto storage_failure;
    }
    result.next_attempt_at_ms = retryable ? now + delay : 0;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_nodes SET status=?,next_attempt_at_ms=?,failure_class=?,"
            "failure_message=?,lease_owner=NULL,claim_token_hash=NULL,lease_expires_at_ms=NULL,"
            "visibility_deadline_ms=NULL,updated_at_ms=?,terminal_at_ms=? "
            "WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    sqlite3_bind_text(statement, 1, new_status, -1, SQLITE_TRANSIENT);
    if (retryable) sqlite3_bind_int64(statement, 2, result.next_attempt_at_ms);
    else sqlite3_bind_null(statement, 2);
    sqlite3_bind_text(statement, 3, failure_class, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, failure->message != NULL ? failure->message : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 5, now);
    if (retryable) sqlite3_bind_null(statement, 6);
    else sqlite3_bind_int64(statement, 6, now);
    sqlite3_bind_text(statement, 7, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 8, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); goto storage_failure; }
    sqlite3_finalize(statement);
    if (finish_attempt(graph, &result, new_status, failure_class, now) != 0 ||
        bf_workgraph_promote_dependents(graph, mission_id, &result) != 0 ||
        bf_workgraph_update_mission(graph, mission_id, &result) != 0 ||
        bf_workgraph_write_evidence(graph, &result, retryable ? "retry_scheduled" : "dead_lettered",
                                    worker_id, failure_class) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) goto storage_failure;
    return result;

failure:
    bf_workgraph_rollback(graph);
    return result;
storage_failure:
    if (statement != NULL) sqlite3_finalize(statement);
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "fail_storage_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

BfWorkgraphResult bf_workgraph_cancel_node(BfWorkgraph *graph, const char *mission_id,
                                           const char *node_id, const char *worker_id,
                                           const char *claim_token) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    sqlite3_stmt *statement = NULL;
    char status[32];
    char effect_state[32];
    int64_t now = bf_workgraph_now_ms();
    int acknowledgment = worker_id != NULL && claim_token != NULL;

    if (graph == NULL) return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id, "invalid_argument", "graph is required");
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (sqlite3_prepare_v2(graph->db,
            "SELECT status,COALESCE(effect_state,'') FROM workgraph_nodes WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        bf_workgraph_rollback(graph);
        return bf_workgraph_result(BF_WORKGRAPH_NOT_FOUND, mission_id, node_id, "node_not_found", "work node was not found");
    }
    snprintf(status, sizeof(status), "%s", sqlite3_column_text(statement, 0));
    snprintf(effect_state, sizeof(effect_state), "%s", sqlite3_column_text(statement, 1));
    sqlite3_finalize(statement);
    statement = NULL;
    if (!strcmp(status, "running")) {
        if (acknowledgment) {
            result.status = BF_WORKGRAPH_INVALID;
            snprintf(result.error_code, sizeof(result.error_code), "cancellation_not_requested");
            snprintf(result.error_message, sizeof(result.error_message), "cancellation must be requested before acknowledgment");
            goto failure;
        }
        status[0] = '\0';
        snprintf(status, sizeof(status), "cancel_requested");
        if (!strcmp(effect_state, "prepared")) {
            snprintf(effect_state, sizeof(effect_state), "rollback_required");
        } else if (!strcmp(effect_state, "committed")) {
            snprintf(effect_state, sizeof(effect_state), "compensation_required");
        }
    } else if (!strcmp(status, "cancel_requested")) {
        if (!acknowledgment || bf_workgraph_validate_claim(graph, &result, worker_id, claim_token, 1) != 0) goto failure;
        if (!strcmp(effect_state, "prepared")) snprintf(effect_state, sizeof(effect_state), "rollback_required");
        else if (!strcmp(effect_state, "committed")) snprintf(effect_state, sizeof(effect_state), "compensation_required");
        else if (strcmp(effect_state, "rollback_required") &&
                 strcmp(effect_state, "compensation_required")) {
            snprintf(status, sizeof(status), "cancelled");
        }
    } else if (!strcmp(status, "ready") || !strcmp(status, "retry_wait") ||
               !strcmp(status, "defined") || !strcmp(status, "blocked")) {
        snprintf(status, sizeof(status), "cancelled");
    } else {
        result.status = BF_WORKGRAPH_NOT_ELIGIBLE;
        snprintf(result.error_code, sizeof(result.error_code), "node_terminal");
        snprintf(result.error_message, sizeof(result.error_message), "terminal node cannot be cancelled");
        goto failure;
    }
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_nodes SET status=?,cancellation_state='requested',effect_state=?,"
            "terminal_at_ms=?,updated_at_ms=?,lease_owner=CASE WHEN ?='cancelled' THEN NULL ELSE lease_owner END,"
            "claim_token_hash=CASE WHEN ?='cancelled' THEN NULL ELSE claim_token_hash END "
            "WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    sqlite3_bind_text(statement, 1, status, -1, SQLITE_TRANSIENT);
    if (effect_state[0]) sqlite3_bind_text(statement, 2, effect_state, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(statement, 2);
    if (!strcmp(status, "cancelled")) sqlite3_bind_int64(statement, 3, now);
    else sqlite3_bind_null(statement, 3);
    sqlite3_bind_int64(statement, 4, now);
    sqlite3_bind_text(statement, 5, status, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, status, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 7, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 8, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); goto storage_failure; }
    sqlite3_finalize(statement);
    statement = NULL;
    if (!strcmp(effect_state, "rollback_required") ||
        !strcmp(effect_state, "compensation_required")) {
        if (sqlite3_prepare_v2(graph->db,
                "UPDATE workgraph_compensations SET state=?,recovery_action=?,updated_at_ms=? "
                "WHERE mission_id=? AND node_id=? AND state IN "
                "('prepared','committed','rollback_required','compensation_required')",
                -1, &statement, NULL) != SQLITE_OK) {
            goto storage_failure;
        }
        sqlite3_bind_text(statement, 1, effect_state, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2,
                          !strcmp(effect_state, "rollback_required") ? "rollback" :
                          "compensation", -1, SQLITE_STATIC);
        sqlite3_bind_int64(statement, 3, now);
        sqlite3_bind_text(statement, 4, mission_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 5, node_id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(statement) != SQLITE_DONE) {
            sqlite3_finalize(statement);
            statement = NULL;
            goto storage_failure;
        }
        sqlite3_finalize(statement);
        statement = NULL;
    }
    if (!strcmp(status, "cancelled") && acknowledgment && finish_attempt(graph, &result, "cancelled", NULL, now) != 0) goto storage_failure;
    if (bf_workgraph_update_mission(graph, mission_id, &result) != 0 ||
        bf_workgraph_write_evidence(graph, &result, !strcmp(status, "cancelled") ? "cancelled" : "cancellation_requested",
                                    acknowledgment ? worker_id : "scheduler", NULL) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) goto storage_failure;
    return result;

failure:
    bf_workgraph_rollback(graph);
    return result;
storage_failure:
    if (statement != NULL) sqlite3_finalize(statement);
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "cancel_storage_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

BfWorkgraphResult bf_workgraph_reap_expired(BfWorkgraph *graph, const char *mission_id) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, NULL, NULL, NULL);
    sqlite3_stmt *read = NULL;
    sqlite3_stmt *update = NULL;
    int64_t now = bf_workgraph_now_ms();
    int reaped = 0;

    if (graph == NULL) return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, NULL, "invalid_argument", "graph is required");
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (sqlite3_prepare_v2(graph->db,
            "SELECT mission_id,node_id,attempt,retry_limit,status FROM workgraph_nodes "
            "WHERE status IN ('running','cancel_requested') AND lease_expires_at_ms<=? "
            "AND (? IS NULL OR mission_id=?) ORDER BY mission_id,node_id",
            -1, &read, NULL) != SQLITE_OK) goto storage_failure;
    sqlite3_bind_int64(read, 1, now);
    if (mission_id != NULL) {
        sqlite3_bind_text(read, 2, mission_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(read, 3, mission_id, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(read, 2); sqlite3_bind_null(read, 3);
    }
    while (sqlite3_step(read) == SQLITE_ROW) {
        const char *selected_mission = (const char *)sqlite3_column_text(read, 0);
        const char *selected_node = (const char *)sqlite3_column_text(read, 1);
        int attempt = sqlite3_column_int(read, 2);
        int retry_limit = sqlite3_column_int(read, 3);
        const char *old_status = (const char *)sqlite3_column_text(read, 4);
        int retry = !strcmp(old_status, "running") && attempt <= retry_limit;
        snprintf(result.mission_id, sizeof(result.mission_id), "%s", selected_mission);
        snprintf(result.node_id, sizeof(result.node_id), "%s", selected_node);
        result.attempt = attempt;
        result.next_attempt_at_ms = retry ? now : 0;
        if (sqlite3_prepare_v2(graph->db,
                "UPDATE workgraph_nodes SET status=?,next_attempt_at_ms=?,failure_class='timeout',"
                "failure_message='lease expired',lease_owner=NULL,claim_token_hash=NULL,"
                "lease_expires_at_ms=NULL,visibility_deadline_ms=NULL,updated_at_ms=?,terminal_at_ms=? "
                "WHERE mission_id=? AND node_id=?",
                -1, &update, NULL) != SQLITE_OK) goto storage_failure;
        sqlite3_bind_text(update, 1, retry ? "retry_wait" : (!strcmp(old_status, "cancel_requested") ? "cancelled" : "dead_letter"), -1, SQLITE_TRANSIENT);
        if (retry) sqlite3_bind_int64(update, 2, now); else sqlite3_bind_null(update, 2);
        sqlite3_bind_int64(update, 3, now);
        if (retry) sqlite3_bind_null(update, 4); else sqlite3_bind_int64(update, 4, now);
        sqlite3_bind_text(update, 5, selected_mission, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(update, 6, selected_node, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(update) != SQLITE_DONE) { sqlite3_finalize(update); update = NULL; goto storage_failure; }
        sqlite3_finalize(update); update = NULL;
        if (finish_attempt(graph, &result, "lease_expired", "timeout", now) != 0 ||
            bf_workgraph_write_evidence(graph, &result, "lease_expired", "scheduler", "timeout") != 0 ||
            bf_workgraph_promote_dependents(graph, selected_mission, &result) != 0 ||
            bf_workgraph_update_mission(graph, selected_mission, &result) != 0) {
            goto storage_failure;
        }
        ++reaped;
    }
    sqlite3_finalize(read); read = NULL;
    if (reaped == 0) {
        bf_workgraph_rollback(graph);
        return bf_workgraph_result(BF_WORKGRAPH_NOT_ELIGIBLE, mission_id, NULL,
                                   "no_expired_leases", "no expired leases were found");
    }
    if (bf_workgraph_commit(graph, &result) != 0) goto storage_failure;
    return result;

storage_failure:
    if (read != NULL) sqlite3_finalize(read);
    if (update != NULL) sqlite3_finalize(update);
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "reap_storage_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}
