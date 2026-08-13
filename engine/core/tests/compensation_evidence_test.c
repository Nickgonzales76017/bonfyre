/*
 * Proves that execution attempts and compensation attempts stay distinguishable
 * in events, transitions, and receipts after a claim -> prepare -> commit ->
 * cancel -> compensate(fail) -> compensate(succeed) sequence.
 */
#include "../include/bf_workgraph.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_failures = 0;

static void fail(const char *what) {
    fprintf(stderr, "FAIL: %s\n", what);
    g_failures++;
}

static void require_ok(BfWorkgraphResult *result, const char *what) {
    if (result->status != BF_WORKGRAPH_OK) {
        fprintf(stderr, "FAIL: %s status=%s error=%s (%s)\n", what,
                bf_workgraph_status_name(result->status), result->error_code,
                result->error_message);
        g_failures++;
    }
}

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (!f) {
        fprintf(stderr, "FAIL: could not create fixture %s\n", path);
        exit(1);
    }
    fputs(content, f);
    fclose(f);
}

/* Verification queries run over an independent read connection so we never
 * observe partially-committed state from the workgraph's own transactions. */
static sqlite3 *open_reader(const char *db_path) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fprintf(stderr, "FAIL: could not open verification connection: %s\n",
                sqlite3_errmsg(db));
        exit(1);
    }
    return db;
}

typedef struct Row {
    char to_status[64];
    char from_status[64];
    char domain[32];
    int attempt;
    int execution_attempt;
    int has_execution_attempt;
    int compensation_attempt;
    int has_compensation_attempt;
} Row;

static int fetch_transitions(sqlite3 *db, const char *mission_id, const char *node_id,
                             const char *domain_filter, Row *rows, int max_rows) {
    sqlite3_stmt *statement = NULL;
    int count = 0;

    if (sqlite3_prepare_v2(db,
            "SELECT to_status,from_status,transition_domain,attempt,execution_attempt,"
            "compensation_attempt FROM workgraph_transitions "
            "WHERE mission_id=? AND node_id=? AND "
            "(? IS NULL OR transition_domain=?) ORDER BY sequence",
            -1, &statement, NULL) != SQLITE_OK) {
        fprintf(stderr, "FAIL: prepare transitions query: %s\n", sqlite3_errmsg(db));
        exit(1);
    }
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    if (domain_filter != NULL) {
        sqlite3_bind_text(statement, 3, domain_filter, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 4, domain_filter, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(statement, 3);
        sqlite3_bind_null(statement, 4);
    }
    while (count < max_rows && sqlite3_step(statement) == SQLITE_ROW) {
        Row *row = &rows[count];
        snprintf(row->to_status, sizeof(row->to_status), "%s", sqlite3_column_text(statement, 0));
        snprintf(row->from_status, sizeof(row->from_status), "%s", sqlite3_column_text(statement, 1));
        snprintf(row->domain, sizeof(row->domain), "%s", sqlite3_column_text(statement, 2));
        row->attempt = sqlite3_column_int(statement, 3);
        row->has_execution_attempt = sqlite3_column_type(statement, 4) != SQLITE_NULL;
        row->execution_attempt = sqlite3_column_int(statement, 4);
        row->has_compensation_attempt = sqlite3_column_type(statement, 5) != SQLITE_NULL;
        row->compensation_attempt = sqlite3_column_int(statement, 5);
        ++count;
    }
    sqlite3_finalize(statement);
    return count;
}

static int events_count_matching(sqlite3 *db, const char *mission_id, const char *node_id,
                                 const char *domain, int compensation_attempt) {
    sqlite3_stmt *statement = NULL;
    int count = 0;

    if (sqlite3_prepare_v2(db,
            "SELECT count(*) FROM events WHERE mission_id=? AND task_id=? AND "
            "transition_domain=? AND compensation_attempt=?",
            -1, &statement, NULL) != SQLITE_OK) {
        fprintf(stderr, "FAIL: prepare events query: %s\n", sqlite3_errmsg(db));
        exit(1);
    }
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, domain, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 4, compensation_attempt);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        count = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return count;
}

static int receipts_count_matching(sqlite3 *db, const char *needle) {
    sqlite3_stmt *statement = NULL;
    int count = 0;

    if (sqlite3_prepare_v2(db,
            "SELECT count(*) FROM receipts WHERE payload LIKE '%' || ? || '%'",
            -1, &statement, NULL) != SQLITE_OK) {
        fprintf(stderr, "FAIL: prepare receipts query: %s\n", sqlite3_errmsg(db));
        exit(1);
    }
    sqlite3_bind_text(statement, 1, needle, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        count = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return count;
}

int main(void) {
    const char *db_path = "/tmp/bonfyre-compensation-evidence-test.sqlite";
    const char *input_path = "/tmp/bonfyre-compensation-evidence-input.txt";
    const char *target_path = "/tmp/bonfyre-compensation-evidence-target.txt";
    BfWorkgraph *graph = NULL;
    BfWorkgraphResult result;
    char claim_token[65];
    char compensation_token[65];

    remove(db_path);
    remove(input_path);
    remove(target_path);
    write_file(input_path, "compensation evidence fixture\n");

    if (bf_workgraph_open(db_path, &graph, stderr) != 0) {
        fprintf(stderr, "FAIL: could not open workgraph database\n");
        return 1;
    }

    result = bf_workgraph_create_mission(graph, "m1");
    require_ok(&result, "create_mission");

    {
        BfWorkgraphNodeSpec spec = {
            .mission_id = "m1", .node_id = "n1", .operator_id = "op.test",
            .family = "default", .priority = 100, .retry_limit = 0,
            .timeout_seconds = 60, .backoff_base_ms = 1000, .backoff_multiplier = 2.0,
            .backoff_max_ms = 60000, .jitter_percent = 0, .fanin_required = 0,
        };
        result = bf_workgraph_add_node(graph, &spec);
        require_ok(&result, "add_node");
    }

    {
        BfWorkgraphClaimSpec spec = { .worker_id = "worker-exec", .lease_ms = 60000 };
        result = bf_workgraph_claim_node(graph, "m1", "n1", &spec);
        require_ok(&result, "claim_node");
        if (result.attempt != 1) fail("expected execution attempt 1 on first claim");
        snprintf(claim_token, sizeof(claim_token), "%s", result.claim_token);
    }

    {
        char target_uri[256];
        char input_uri[256];
        BfWorkgraphEffectSpec spec;

        snprintf(target_uri, sizeof(target_uri), "file://%s", target_path);
        snprintf(input_uri, sizeof(input_uri), "file://%s", input_path);
        memset(&spec, 0, sizeof(spec));
        spec.effect_id = "e1";
        spec.adapter_id = "derive-file";
        spec.target_uri = target_uri;
        spec.input_artifact_uri = input_uri;
        spec.verification_policy = "sha256";
        spec.rollback_contract = "remove-created-target";
        spec.authority_identity = "tester";
        result = bf_workgraph_prepare_effect(graph, "m1", "n1", "worker-exec", claim_token, &spec);
        require_ok(&result, "prepare_effect");
    }

    result = bf_workgraph_commit_effect(graph, "m1", "n1", "worker-exec", claim_token, "e1");
    require_ok(&result, "commit_effect");

    /* Node is still "running" from the caller's perspective; cancelling it
     * with a committed effect makes the compensation claimable. */
    result = bf_workgraph_cancel_node(graph, "m1", "n1", NULL, NULL);
    require_ok(&result, "cancel_node (request)");

    /* Compensation attempt 1: force a failure so a second attempt is required. */
    {
        BfWorkgraphClaimSpec spec = { .worker_id = "worker-comp", .lease_ms = 60000 };
        result = bf_workgraph_claim_compensation(graph, "m1", &spec);
        require_ok(&result, "claim_compensation (attempt 1)");
        if (result.attempt != 1) fail("expected compensation attempt 1 on first claim");
        snprintf(compensation_token, sizeof(compensation_token), "%s", result.claim_token);
    }
    result = bf_workgraph_compensate(graph, "m1", "n1", "worker-comp", compensation_token, "e1", 0);
    require_ok(&result, "compensate (attempt 1, forced failure)");

    /* Compensation attempt 2: succeed. */
    {
        BfWorkgraphClaimSpec spec = { .worker_id = "worker-comp-2", .lease_ms = 60000 };
        result = bf_workgraph_claim_compensation(graph, "m1", &spec);
        require_ok(&result, "claim_compensation (attempt 2)");
        if (result.attempt != 2) fail("expected compensation attempt 2 on second claim");
        snprintf(compensation_token, sizeof(compensation_token), "%s", result.claim_token);
    }
    result = bf_workgraph_compensate(graph, "m1", "n1", "worker-comp-2", compensation_token, "e1", 1);
    require_ok(&result, "compensate (attempt 2, success)");
    if (result.attempt != 2) fail("expected compensation attempt to remain 2 after completion");

    bf_workgraph_close(graph);

    /* Verify persisted evidence distinguishes execution attempt 1 from
     * compensation attempts 1 and 2 -- the bug this test guards against
     * clobbered all of these down to the node's execution attempt. */
    {
        sqlite3 *reader = open_reader(db_path);
        Row rows[8];
        int count = fetch_transitions(reader, "m1", "n1", "compensation", rows, 8);

        if (count != 4) {
            fprintf(stderr, "FAIL: expected 4 compensation transitions, got %d\n", count);
            g_failures++;
        } else {
            static const struct { const char *to_status; int compensation_attempt; } expected[4] = {
                {"compensation_claimed", 1},
                {"compensation_failed", 1},
                {"compensation_claimed", 2},
                {"compensated", 2},
            };
            for (int i = 0; i < 4; ++i) {
                if (strcmp(rows[i].to_status, expected[i].to_status) != 0) {
                    fprintf(stderr, "FAIL: transition %d to_status=%s want=%s\n",
                            i, rows[i].to_status, expected[i].to_status);
                    g_failures++;
                }
                if (!rows[i].has_compensation_attempt ||
                    rows[i].compensation_attempt != expected[i].compensation_attempt) {
                    fprintf(stderr, "FAIL: transition %d compensation_attempt=%d want=%d\n",
                            i, rows[i].compensation_attempt, expected[i].compensation_attempt);
                    g_failures++;
                }
                if (rows[i].attempt != expected[i].compensation_attempt) {
                    fprintf(stderr,
                            "FAIL: transition %d legacy attempt column=%d want=%d "
                            "(compensation attempt clobbered by execution attempt)\n",
                            i, rows[i].attempt, expected[i].compensation_attempt);
                    g_failures++;
                }
                if (!rows[i].has_execution_attempt || rows[i].execution_attempt != 1) {
                    fprintf(stderr,
                            "FAIL: transition %d execution_attempt=%d want=1 "
                            "(node was only ever claimed once for execution)\n",
                            i, rows[i].execution_attempt);
                    g_failures++;
                }
                if (strcmp(rows[i].domain, "compensation") != 0) {
                    fprintf(stderr, "FAIL: transition %d domain=%s want=compensation\n",
                            i, rows[i].domain);
                    g_failures++;
                }
            }
        }

        {
            Row node_rows[8];
            int node_count = fetch_transitions(reader, "m1", "n1", "node", node_rows, 8);
            int saw_claimed_execution_attempt_1 = 0;

            for (int i = 0; i < node_count; ++i) {
                if (strcmp(node_rows[i].to_status, "running") == 0 && node_rows[i].attempt == 1) {
                    saw_claimed_execution_attempt_1 = 1;
                }
                if (node_rows[i].has_compensation_attempt) {
                    fprintf(stderr, "FAIL: node-domain transition %d unexpectedly has compensation_attempt\n", i);
                    g_failures++;
                }
            }
            if (!saw_claimed_execution_attempt_1) {
                fail("expected a node-domain 'claimed' transition with execution attempt 1");
            }
        }

        if (events_count_matching(reader, "m1", "n1", "compensation", 1) != 2) {
            fail("expected exactly 2 compensation events for attempt 1 (claim + failure)");
        }
        if (events_count_matching(reader, "m1", "n1", "compensation", 2) != 2) {
            fail("expected exactly 2 compensation events for attempt 2 (claim + success)");
        }

        if (receipts_count_matching(reader, "\"compensation_attempt\":1") != 2) {
            fail("expected 2 receipts recording compensation_attempt 1");
        }
        if (receipts_count_matching(reader, "\"compensation_attempt\":2") != 2) {
            fail("expected 2 receipts recording compensation_attempt 2");
        }
        if (receipts_count_matching(reader, "\"transition_domain\":\"compensation\"") != 4) {
            fail("expected 4 receipts tagged with transition_domain=compensation");
        }

        sqlite3_close(reader);
    }

    remove(db_path);
    remove(input_path);
    remove(target_path);

    if (g_failures) {
        fprintf(stderr, "\n%d compensation evidence check(s) failed\n", g_failures);
        return 1;
    }
    printf("all compensation evidence checks passed\n");
    return 0;
}
