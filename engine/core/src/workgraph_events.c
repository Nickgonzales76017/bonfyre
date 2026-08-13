#include "workgraph_internal.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

static int random_identifier(const char *prefix, char *output, size_t output_size) {
    unsigned char bytes[16];
    char material[33];
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
        material[index * 2] = hex[bytes[index] >> 4];
        material[index * 2 + 1] = hex[bytes[index] & 15];
    }
    material[32] = '\0';
    if (snprintf(output, output_size, "%s-%s", prefix, material) >= (int)output_size) {
        return -1;
    }
    return 0;
}

static int previous_receipt(BfWorkgraph *graph, const char *mission_id,
                            char receipt_id[64], char chain_hash[65]) {
    sqlite3_stmt *statement = NULL;
    int found = 0;

    receipt_id[0] = '\0';
    chain_hash[0] = '\0';
    if (sqlite3_prepare_v2(graph->db,
            "SELECT t.receipt_id,COALESCE(r.chain_hash,r.content_hash) "
            "FROM workgraph_transitions t JOIN receipts r ON r.id=t.receipt_id "
            "WHERE t.mission_id=? ORDER BY t.sequence DESC LIMIT 1",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        snprintf(receipt_id, 64, "%s", sqlite3_column_text(statement, 0));
        snprintf(chain_hash, 65, "%s", sqlite3_column_text(statement, 1));
        found = 1;
    }
    sqlite3_finalize(statement);
    return found;
}

static int previous_node_status(BfWorkgraph *graph, const char *mission_id,
                                const char *node_id, char status[32]) {
    sqlite3_stmt *statement = NULL;
    int found = 0;

    status[0] = '\0';
    if (node_id == NULL) {
        snprintf(status, 32, "mission");
        return 1;
    }
    if (sqlite3_prepare_v2(graph->db,
            "SELECT to_status FROM workgraph_transitions "
            "WHERE mission_id=? AND node_id=? ORDER BY sequence DESC LIMIT 1",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        snprintf(status, 32, "%s", sqlite3_column_text(statement, 0));
        found = 1;
    }
    sqlite3_finalize(statement);
    if (!found) {
        snprintf(status, 32, "absent");
    }
    return found;
}

int bf_workgraph_write_evidence(BfWorkgraph *graph, BfWorkgraphResult *result,
                                const char *transition, const char *actor,
                                const char *error_code) {
    BfWorkgraphEvidence evidence = {
        .transition_domain = BF_WORKGRAPH_TRANSITION_NODE,
        .execution_attempt = -1,
        .compensation_attempt = -1,
        .from_state = NULL,
        .to_state = transition,
    };

    if (!strcmp(transition, "effect_prepared") || !strcmp(transition, "effect_committed")) {
        evidence.transition_domain = BF_WORKGRAPH_TRANSITION_EFFECT;
    }
    return bf_workgraph_write_evidence_ex(graph, result, &evidence, actor, error_code);
}

static const char *transition_domain_name(BfWorkgraphTransitionDomain domain) {
    switch (domain) {
        case BF_WORKGRAPH_TRANSITION_EFFECT: return "effect";
        case BF_WORKGRAPH_TRANSITION_COMPENSATION: return "compensation";
        default: return "node";
    }
}

int bf_workgraph_write_evidence_ex(BfWorkgraph *graph, BfWorkgraphResult *result,
                                   const BfWorkgraphEvidence *evidence,
                                   const char *actor, const char *error_code) {
    sqlite3_stmt *statement = NULL;
    char timestamp[32];
    char transition_id[64];
    char previous_id[64];
    char previous_hash[65];
    char payload[1024];
    char payload_hash[65];
    char chain_material[256];
    char chain_hash[65];
    char from_status[32];
    char node_status[32] = "mission";
    char operator_id[160] = "workgraph.scheduler";
    const char *node_value = result->node_id[0] != '\0' ? result->node_id : NULL;
    const char *transition = evidence->to_state;
    const char *domain_name = transition_domain_name(evidence->transition_domain);
    int is_compensation = evidence->transition_domain == BF_WORKGRAPH_TRANSITION_COMPENSATION;
    int node_attempt = -1;
    int execution_attempt;
    int64_t now = bf_workgraph_now_ms();

    if (random_identifier("evt", result->event_id, sizeof(result->event_id)) != 0 ||
        random_identifier("rcpt", result->receipt_id, sizeof(result->receipt_id)) != 0 ||
        random_identifier("trn", transition_id, sizeof(transition_id)) != 0) {
        return -1;
    }
    if (node_value != NULL && sqlite3_prepare_v2(graph->db,
            "SELECT status,operator_id,attempt,COALESCE(lease_owner,''),"
            "COALESCE(lease_expires_at_ms,0),COALESCE(next_attempt_at_ms,0) "
            "FROM workgraph_nodes WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, result->mission_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, result->node_id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(statement) == SQLITE_ROW) {
            snprintf(node_status, sizeof(node_status), "%s", sqlite3_column_text(statement, 0));
            snprintf(operator_id, sizeof(operator_id), "%s", sqlite3_column_text(statement, 1));
            node_attempt = sqlite3_column_int(statement, 2);
            if (!is_compensation) {
                /* Compensation attempts are tracked independently of the node's
                 * ordinary execution attempt; never let this lookup clobber a
                 * caller-supplied compensation attempt. */
                result->attempt = node_attempt;
                snprintf(result->worker_id, sizeof(result->worker_id), "%s",
                         sqlite3_column_text(statement, 3));
                result->lease_expires_at_ms = sqlite3_column_int64(statement, 4);
                result->next_attempt_at_ms = sqlite3_column_int64(statement, 5);
            }
        }
        sqlite3_finalize(statement);
        statement = NULL;
    }
    if (previous_receipt(graph, result->mission_id, previous_id, previous_hash) < 0) {
        return -1;
    }
    if (evidence->from_state != NULL) {
        snprintf(from_status, sizeof(from_status), "%s", evidence->from_state);
    } else if (previous_node_status(graph, result->mission_id, node_value, from_status) < 0) {
        return -1;
    }
    snprintf(result->node_status, sizeof(result->node_status), "%s", node_status);
    execution_attempt = evidence->execution_attempt >= 0 ? evidence->execution_attempt : node_attempt;
    if (is_compensation) {
        /* result->attempt carries the compensation attempt for this domain. */
        result->attempt = evidence->compensation_attempt;
    }
    snprintf(payload, sizeof(payload),
             "{\"mission_id\":\"%s\",\"node_id\":\"%s\","
             "\"attempt\":%d,\"transition\":\"%s\",\"transition_domain\":\"%s\","
             "\"execution_attempt\":%d,\"compensation_attempt\":%d,"
             "\"from_status\":\"%s\",\"to_status\":\"%s\",\"status\":\"%s\","
             "\"actor\":\"%s\",\"lease_expires_at_ms\":%lld,"
             "\"next_attempt_at_ms\":%lld,\"error_code\":\"%s\"}",
             result->mission_id, result->node_id, result->attempt, transition, domain_name,
             execution_attempt, evidence->compensation_attempt,
             from_status, is_compensation ? transition : node_status, node_status,
             actor != NULL ? actor : "scheduler",
             (long long)result->lease_expires_at_ms,
             (long long)result->next_attempt_at_ms,
             error_code != NULL ? error_code : "");
    bf_sha256_hex((const uint8_t *)payload, strlen(payload), payload_hash);
    snprintf(chain_material, sizeof(chain_material), "%s|%s", previous_hash, payload_hash);
    bf_sha256_hex((const uint8_t *)chain_material, strlen(chain_material), chain_hash);
    bf_workgraph_timestamp(timestamp);

    if (sqlite3_prepare_v2(graph->db,
            "INSERT INTO receipts(id,subject_kind,subject_id,content_hash,payload,created_at,"
            "previous_receipt_id,chain_hash) VALUES(?,?,?,?,?,?,?,?)",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, result->receipt_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, "workgraph-transition", -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 3, node_value != NULL ? node_value : result->mission_id,
                      -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, payload_hash, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, payload, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, timestamp, -1, SQLITE_TRANSIENT);
    if (previous_id[0] != '\0') {
        sqlite3_bind_text(statement, 7, previous_id, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(statement, 7);
    }
    sqlite3_bind_text(statement, 8, chain_hash, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        return -1;
    }
    sqlite3_finalize(statement);

    if (sqlite3_prepare_v2(graph->db,
            "INSERT INTO events(id,mission_id,task_id,attempt,actor,operator_id,provider_id,"
            "model_id,start_at,end_at,duration_ms,input_uri,output_uri,effect_class,status,"
            "error_code,receipt_id,transition_domain,execution_attempt,compensation_attempt) "
            "VALUES(?,?,?,?,?,?,?,NULL,?,?,0,NULL,NULL,?,?,?,?,?,?,?)",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, result->event_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, result->mission_id, -1, SQLITE_TRANSIENT);
    if (node_value != NULL) {
        sqlite3_bind_text(statement, 3, node_value, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(statement, 3);
    }
    sqlite3_bind_int(statement, 4, result->attempt);
    sqlite3_bind_text(statement, 5, actor != NULL ? actor : "scheduler", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, operator_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 7, "native", -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 8, timestamp, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 9, timestamp, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 10, "governed-workgraph", -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 11, transition, -1, SQLITE_TRANSIENT);
    if (error_code != NULL && error_code[0] != '\0') {
        sqlite3_bind_text(statement, 12, error_code, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(statement, 12);
    }
    sqlite3_bind_text(statement, 13, result->receipt_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 14, domain_name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 15, execution_attempt);
    if (is_compensation) {
        sqlite3_bind_int(statement, 16, evidence->compensation_attempt);
    } else {
        sqlite3_bind_null(statement, 16);
    }
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        return -1;
    }
    sqlite3_finalize(statement);

    if (sqlite3_prepare_v2(graph->db,
            "INSERT INTO workgraph_transitions(transition_id,mission_id,node_id,attempt,"
            "from_status,to_status,actor,event_id,receipt_id,created_at_ms,"
            "transition_domain,execution_attempt,compensation_attempt) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, transition_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, result->mission_id, -1, SQLITE_TRANSIENT);
    if (node_value != NULL) {
        sqlite3_bind_text(statement, 3, node_value, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(statement, 3);
    }
    sqlite3_bind_int(statement, 4, result->attempt);
    sqlite3_bind_text(statement, 5, from_status, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, is_compensation ? transition : node_status, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 7, actor != NULL ? actor : "scheduler", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 8, result->event_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 9, result->receipt_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 10, now);
    sqlite3_bind_text(statement, 11, domain_name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 12, execution_attempt);
    if (is_compensation) {
        sqlite3_bind_int(statement, 13, evidence->compensation_attempt);
    } else {
        sqlite3_bind_null(statement, 13);
    }
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        return -1;
    }
    sqlite3_finalize(statement);
    return 0;
}

BfWorkgraphResult bf_workgraph_list_transitions(BfWorkgraph *graph,
                                                const char *mission_id,
                                                const char *node_id,
                                                BfWorkgraphTransition **transitions,
                                                size_t *transition_count) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id,
                                                    node_id, NULL, NULL);
    BfWorkgraphTransition *items = NULL;
    sqlite3_stmt *statement = NULL;
    size_t count = 0;
    size_t index = 0;

    if (graph == NULL || mission_id == NULL || node_id == NULL ||
        transitions == NULL || transition_count == NULL) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id,
                                   "invalid_argument",
                                   "graph, mission, node, and output pointers are required");
    }
    *transitions = NULL;
    *transition_count = 0;
    if (sqlite3_prepare_v2(graph->db,
            "SELECT count(*) FROM workgraph_transitions "
            "WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) {
        goto storage_failure;
    }
    count = (size_t)sqlite3_column_int64(statement, 0);
    sqlite3_finalize(statement);
    statement = NULL;
    if (count == 0) {
        return bf_workgraph_result(BF_WORKGRAPH_NOT_FOUND, mission_id, node_id,
                                   "transition_history_not_found",
                                   "no transitions were found for the node");
    }
    items = calloc(count, sizeof(*items));
    if (items == NULL) {
        return bf_workgraph_result(BF_WORKGRAPH_STORAGE_ERROR, mission_id, node_id,
                                   "allocation_failed",
                                   "transition history allocation failed");
    }
    if (sqlite3_prepare_v2(graph->db,
            "SELECT sequence,attempt,from_status,to_status,actor,event_id,receipt_id,created_at_ms "
            "FROM workgraph_transitions WHERE mission_id=? AND node_id=? ORDER BY sequence",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    while (index < count && sqlite3_step(statement) == SQLITE_ROW) {
        items[index].sequence = sqlite3_column_int64(statement, 0);
        items[index].attempt = sqlite3_column_int(statement, 1);
        snprintf(items[index].from_status, sizeof(items[index].from_status), "%s",
                 sqlite3_column_text(statement, 2));
        snprintf(items[index].to_status, sizeof(items[index].to_status), "%s",
                 sqlite3_column_text(statement, 3));
        snprintf(items[index].actor, sizeof(items[index].actor), "%s",
                 sqlite3_column_text(statement, 4));
        snprintf(items[index].event_id, sizeof(items[index].event_id), "%s",
                 sqlite3_column_text(statement, 5));
        snprintf(items[index].receipt_id, sizeof(items[index].receipt_id), "%s",
                 sqlite3_column_text(statement, 6));
        items[index].created_at_ms = sqlite3_column_int64(statement, 7);
        ++index;
    }
    sqlite3_finalize(statement);
    if (index != count) {
        free(items);
        return bf_workgraph_result(BF_WORKGRAPH_STORAGE_ERROR, mission_id, node_id,
                                   "transition_history_changed",
                                   "transition history changed while it was read");
    }
    *transitions = items;
    *transition_count = count;
    return result;

storage_failure:
    if (statement != NULL) {
        sqlite3_finalize(statement);
    }
    free(items);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "transition_history_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s",
             sqlite3_errmsg(graph->db));
    return result;
}

void bf_workgraph_free_transitions(BfWorkgraphTransition *transitions) {
    free(transitions);
}
