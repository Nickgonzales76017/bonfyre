#define _POSIX_C_SOURCE 200809L
#include "bf_workgraph.h"
#include "bf_command_lookup.h"
#include "account_activity.h"
#include "account_inbox.h"
#include "account_work.h"
#include <sqlite3.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "../../../lib/libbonfyre/include/bf_help.h"

#define QUEUE_DEFAULT_FAMILY "queue"


#define VERSION "3.0.0"

static const char *option_value(int argc, char **argv, const char *name) {
    for (int index = 1; index + 1 < argc; ++index) {
        if (strcmp(argv[index], name) == 0) return argv[index + 1];
    }
    return NULL;
}

static int option_present(int argc, char **argv, const char *name) {
    for (int index = 1; index < argc; ++index) {
        if (strcmp(argv[index], name) == 0) return 1;
    }
    return 0;
}

static long long integer_option(int argc, char **argv, const char *name, long long fallback) {
    const char *value = option_value(argc, argv, name);
    char *end = NULL;
    long long parsed;

    if (value == NULL) return fallback;
    parsed = strtoll(value, &end, 10);
    return end != NULL && *end == '\0' ? parsed : fallback;
}

static int64_t epoch_ms(void) {
    struct timespec value;
    clock_gettime(CLOCK_REALTIME, &value);
    return (int64_t)value.tv_sec * 1000 + value.tv_nsec / 1000000;
}

static int result_ok(BfWorkgraphResult result, const char *operation) {
    if (result.status == BF_WORKGRAPH_OK) return 1;
    fprintf(stderr, "bonfyre-queue: %s failed: %s: %s\n", operation,
            result.error_code, result.error_message);
    return 0;
}

/* Which families currently hold claimable work. Read-only, and only consulted
 * when a filtered claim came back empty, so the common path pays nothing. */
static void report_ready_families(const char *database_path, const char *want_family) {
    sqlite3 *db = NULL;
    sqlite3_stmt *q = NULL;
    int found = 0;
    /* Its own read-only handle rather than a new library accessor: this is a
     * diagnostic on a path that already failed, and it should not be able to
     * touch the queue it is describing. */
    if (sqlite3_open_v2(database_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return;
    }
    if (sqlite3_prepare_v2(db,
            "SELECT family, COUNT(*) FROM workgraph_nodes "
            "WHERE lower(status) IN ('ready','pending') GROUP BY family ORDER BY 2 DESC",
            -1, &q, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return;
    }
    while (sqlite3_step(q) == SQLITE_ROW) {
        const unsigned char *fam = sqlite3_column_text(q, 0);
        if (!found) {
            fprintf(stderr,
                    "bonfyre-queue: no claimable node in family '%s'. "
                    "Families with work waiting:\n", want_family);
            found = 1;
        }
        fprintf(stderr, "  %-24s %d node(s)\n",
                fam ? (const char *)fam : "(none)", sqlite3_column_int(q, 1));
    }
    sqlite3_finalize(q);
    sqlite3_close(db);
    if (found) {
        fprintf(stderr,
                "  enqueue files jobs under '%s' unless --family is given; "
                "TYPE names the operator, not the family.\n", QUEUE_DEFAULT_FAMILY);
    }
}

/* Follow an MCP/WorkGraph mission when the caller knows the mission id but not
 * its internal node ids. This is read-only: inspection grants no queue power. */
/* Read-only family depth for standing workers.  Keeping this in the queue
 * owner lets producers maintain a bounded ready window without scraping the
 * WorkGraph schema or re-enqueueing thousands of already-ready missions every
 * interval. */
static int queue_depth(const char *database_path, const char *family) {
    sqlite3 *db = NULL;
    sqlite3_stmt *q = NULL;
    const char *sql =
        "SELECT COUNT(*),"
        "SUM(status IN ('ready','retry_wait')),SUM(status='running'),"
        "SUM(status='complete'),SUM(status='dead_letter') "
        "FROM workgraph_nodes WHERE (?1 IS NULL OR family=?1)";
    if (sqlite3_open_v2(database_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 1;
    }
    sqlite3_busy_timeout(db, 2000);
    if (sqlite3_prepare_v2(db, sql, -1, &q, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    if (family) sqlite3_bind_text(q, 1, family, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(q, 1);
    if (sqlite3_step(q) != SQLITE_ROW) {
        sqlite3_finalize(q); sqlite3_close(db); return 1;
    }
    printf("{\"family\":");
    if (family) printf("\"%s\"", family); else printf("null");
    printf(",\"total\":%d,\"claimable\":%d,\"running\":%d,\"complete\":%d,\"dead_letter\":%d}\n",
           sqlite3_column_int(q, 0), sqlite3_column_int(q, 1), sqlite3_column_int(q, 2),
           sqlite3_column_int(q, 3), sqlite3_column_int(q, 4));
    sqlite3_finalize(q);
    sqlite3_close(db);
    return 0;
}

static int inspect_mission(const char *database_path, const char *mission_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *q = NULL;
    int rows = 0;
    if (sqlite3_open_v2(database_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 1;
    }
    if (sqlite3_prepare_v2(db,
            "SELECT json_object('mission_id',mission_id,'node_id',node_id,"
            "'operator_id',operator_id,'status',status,'attempt',attempt,"
            "'family',COALESCE(family,''),'input_uri',COALESCE(input_uri,''),"
            "'output_uri',COALESCE(output_uri,''),'receipt_id',COALESCE("
            "(SELECT t.receipt_id FROM workgraph_transitions t JOIN receipts r ON r.id=t.receipt_id "
            "WHERE t.mission_id=workgraph_nodes.mission_id AND t.node_id=workgraph_nodes.node_id "
            "AND t.to_status=workgraph_nodes.status ORDER BY t.sequence DESC LIMIT 1),"
            "NULLIF(receipt_id,''),''),'effect_state',COALESCE(effect_state,'')) "
            "FROM workgraph_nodes WHERE mission_id=? ORDER BY node_id",
            -1, &q, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_text(q, 1, mission_id, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(q) == SQLITE_ROW) {
        const unsigned char *json = sqlite3_column_text(q, 0);
        if (json) puts((const char *)json);
        rows++;
    }
    sqlite3_finalize(q);
    sqlite3_close(db);
    return rows ? 0 : 1;
}

static void print_result(const BfWorkgraphResult *result) {
    printf("{\"result\":\"%s\",\"mission_id\":\"%s\",\"node_id\":\"%s\","
           "\"status\":\"%s\",\"attempt\":%d,\"worker_id\":\"%s\","
           "\"lease_expires_at_ms\":%lld,\"next_attempt_at_ms\":%lld,"
           "\"event_id\":\"%s\",\"receipt_id\":\"%s\","
           "\"error_code\":\"%s\",\"error_message\":\"%s\"",
           bf_workgraph_status_name(result->status), result->mission_id, result->node_id,
           result->node_status, result->attempt, result->worker_id,
           (long long)result->lease_expires_at_ms, (long long)result->next_attempt_at_ms,
           result->event_id, result->receipt_id, result->error_code, result->error_message);
    if (result->status == BF_WORKGRAPH_OK && result->claim_token[0] != '\0') {
        printf(",\"claim_token\":\"%s\"", result->claim_token);
    }
    printf("}\n");
}

static void checkpoint_string(const char *value) {
    putchar('"');
    for (const unsigned char *p = (const unsigned char *)value; *p; ++p) {
        if (*p == '"' || *p == '\\') { putchar('\\'); putchar(*p); }
        else if (*p < 32) printf("\\u%04x", *p);
        else putchar(*p);
    }
    putchar('"');
}

static void print_checkpoint(const BfWorkgraphCheckpoint *record) {
    printf("{\"revision\":%lld,\"created_at_ms\":%lld,\"artifact_uri\":",
        (long long)record->revision, (long long)record->created_at_ms);
    checkpoint_string(record->artifact_uri);
    printf(",\"worker_id\":"); checkpoint_string(record->worker_id); puts("}");
}

/* The family a job is filed under, and why it is not the TYPE.
 *
 * `enqueue TYPE` names the operator: it writes operator_id "queue.<TYPE>" and
 * files every job under the single family "queue". `claim-next --family F`
 * filters on that family column. So the only family this CLI ever wrote was
 * "queue", and `--family local_fallback` matched nothing however many
 * local_fallback jobs were waiting -- claim-next answered no_eligible_node while
 * a bare claim-next took the job immediately.
 *
 * The two are genuinely different axes: an operator says what runs, a family
 * says which pool of workers may take it, and a queue can legitimately want one
 * worker pool serving several operators. Collapsing TYPE into family would fix
 * the symptom by deleting the distinction. Making family settable, and defaulted,
 * keeps both and stops the CLI from advertising a filter it could not satisfy. */

static BfWorkgraphNodeSpec node_spec(const char *mission, const char *node,
                                     const char *operator_id, const char *input,
                                     int retry_limit, int64_t backoff_ms,
                                     const char *family, int priority) {
    BfWorkgraphNodeSpec spec;
    memset(&spec, 0, sizeof(spec));
    spec.mission_id = mission;
    spec.node_id = node;
    spec.operator_id = operator_id;
    spec.input_uri = input;
    spec.family = family ? family : QUEUE_DEFAULT_FAMILY;
    spec.priority = priority != 0 ? priority : 100;
    spec.retry_limit = retry_limit;
    spec.timeout_seconds = 30;
    spec.backoff_base_ms = backoff_ms;
    spec.backoff_multiplier = 2.0;
    spec.backoff_max_ms = backoff_ms > 0 ? backoff_ms : 60000;
    return spec;
}

static int create_job(BfWorkgraph *graph, const char *mission, const char *type,
                      const char *input, int retries, int64_t backoff_ms,
                      const char *family, int priority, int if_absent) {
    char operator_id[160];
    const char *resolved_family = family ? family : QUEUE_DEFAULT_FAMILY;
    BfWorkgraphNodeSpec spec;
    BfWorkgraphResult created;
    BfWorkgraphResult existing;

    created = bf_workgraph_register_family(graph, resolved_family);
    if (created.status != BF_WORKGRAPH_OK && created.status != BF_WORKGRAPH_CONFLICT) {
        return result_ok(created, "register family");
    }
    created = bf_workgraph_create_mission(graph, mission);
    if (created.status == BF_WORKGRAPH_CONFLICT && if_absent) {
        existing = bf_workgraph_status(graph, mission, "job");
        if (existing.status == BF_WORKGRAPH_OK) return 1;
    } else if (created.status != BF_WORKGRAPH_OK) {
        return result_ok(created, "create mission");
    }
    snprintf(operator_id, sizeof(operator_id), "queue.%s", type);
    spec = node_spec(mission, "job", operator_id, input, retries, backoff_ms,
                     resolved_family, priority);
    existing = bf_workgraph_add_node(graph, &spec);
    if (existing.status == BF_WORKGRAPH_CONFLICT && if_absent) return 1;
    return result_ok(existing, "enqueue");
}

static int fixture(const char *database_path) {
    BfWorkgraph *graph = NULL;
    BfWorkgraphResult claim;
    BfWorkgraphResult result;
    BfWorkgraphClaimSpec claim_spec = {"fixture-worker", "queue", 30000};
    BfWorkgraphFailure transient_failure = {"transient", "fixture transient failure"};
    BfWorkgraphFailure permanent_failure = {"permanent", "fixture permanent failure"};
    int ok = bf_workgraph_open(database_path, &graph, stderr) == 0;

    if (!ok) return 1;
    ok &= create_job(graph, "queue-success", "success", "fixture://success", 1, 20, QUEUE_DEFAULT_FAMILY, 100, 0);
    claim = bf_workgraph_claim_next(graph, &claim_spec);
    ok &= result_ok(claim, "claim success");
    result = bf_workgraph_renew(graph, claim.mission_id, claim.node_id, claim.worker_id,
                                claim.claim_token, 30000);
    ok &= result_ok(result, "renew success");
    result = bf_workgraph_complete(graph, claim.mission_id, claim.node_id, claim.worker_id,
                                   claim.claim_token, "fixture://completed");
    ok &= result_ok(result, "ack success");

    ok &= create_job(graph, "queue-retry", "transient", "fixture://retry", 1, 20, QUEUE_DEFAULT_FAMILY, 100, 0);
    claim = bf_workgraph_claim_next(graph, &claim_spec);
    ok &= result_ok(claim, "claim retry");
    result = bf_workgraph_fail(graph, claim.mission_id, claim.node_id, claim.worker_id,
                               claim.claim_token, &transient_failure);
    ok &= result_ok(result, "schedule retry");
    while (epoch_ms() < result.next_attempt_at_ms) { }
    claim_spec.worker_id = "fixture-retry-worker";
    claim = bf_workgraph_claim_next(graph, &claim_spec);
    ok &= result_ok(claim, "claim retried job");
    result = bf_workgraph_complete(graph, claim.mission_id, claim.node_id, claim.worker_id,
                                   claim.claim_token, "fixture://retried");
    ok &= result_ok(result, "ack retried job");

    ok &= create_job(graph, "queue-dead", "permanent", "fixture://dead", 3, 20, QUEUE_DEFAULT_FAMILY, 100, 0);
    claim_spec.worker_id = "fixture-dead-worker";
    claim = bf_workgraph_claim_next(graph, &claim_spec);
    ok &= result_ok(claim, "claim dead-letter job");
    result = bf_workgraph_fail(graph, claim.mission_id, claim.node_id, claim.worker_id,
                               claim.claim_token, &permanent_failure);
    ok &= result_ok(result, "dead letter");
    ok &= strcmp(result.node_status, "dead_letter") == 0;

    ok &= create_job(graph, "queue-expiry", "expiry", "fixture://expiry", 1, 20, QUEUE_DEFAULT_FAMILY, 100, 0);
    claim_spec.worker_id = "fixture-expired-worker";
    claim_spec.lease_ms = 40;
    claim = bf_workgraph_claim_next(graph, &claim_spec);
    ok &= result_ok(claim, "claim expiring job");
    while (epoch_ms() <= claim.lease_expires_at_ms) { }
    result = bf_workgraph_reap_expired(graph, "queue-expiry");
    ok &= result_ok(result, "reap expired job");
    claim_spec.worker_id = "fixture-reclaimed-worker";
    claim_spec.lease_ms = 30000;
    claim = bf_workgraph_claim_next(graph, &claim_spec);
    ok &= result_ok(claim, "reclaim expired job");
    result = bf_workgraph_complete(graph, claim.mission_id, claim.node_id, claim.worker_id,
                                   claim.claim_token, "fixture://reclaimed");
    ok &= result_ok(result, "ack reclaimed job");

    ok &= create_job(graph, "queue-cancel", "cancel", "fixture://cancel", 0, 20, QUEUE_DEFAULT_FAMILY, 100, 0);
    result = bf_workgraph_cancel_node(graph, "queue-cancel", "job", NULL, NULL);
    ok &= result_ok(result, "cancel queued job") && strcmp(result.node_status, "cancelled") == 0;

    ok &= create_job(graph, "queue-fan", "parent", "fixture://fan", 0, 20, QUEUE_DEFAULT_FAMILY, 100, 0);
    claim_spec.worker_id = "fixture-fan-worker";
    claim = bf_workgraph_claim_next(graph, &claim_spec);
    ok &= result_ok(claim, "claim fan parent");
    result = bf_workgraph_complete(graph, claim.mission_id, claim.node_id, claim.worker_id,
                                   claim.claim_token, "fixture://fan-parent");
    ok &= result_ok(result, "complete fan parent");
    {
        BfWorkgraphFanoutSpec fanout = {"queue-fan", "job", "group", "queue.child",
                                        "queue", "child", 2, "fail"};
        BfWorkgraphNodeSpec join = node_spec("queue-fan", "join", "queue.join", NULL, 0, 20, QUEUE_DEFAULT_FAMILY, 100);
        join.fanout_group = "group";
        join.fanin_required = 1;
        ok &= result_ok(bf_workgraph_create_fanout(graph, &fanout), "fan out");
        ok &= result_ok(bf_workgraph_add_node(graph, &join), "add fan in");
        result = bf_workgraph_evaluate_fanin(graph, "queue-fan", "join");
        ok &= result_ok(result, "block fan in") && strcmp(result.node_status, "blocked") == 0;
        for (int index = 1; index <= 2; ++index) {
            char child[32];
            snprintf(child, sizeof(child), "child-%d", index);
            claim = bf_workgraph_claim_node(graph, "queue-fan", child, &claim_spec);
            ok &= result_ok(claim, "claim child");
            result = bf_workgraph_complete(graph, claim.mission_id, claim.node_id,
                                           claim.worker_id, claim.claim_token, "fixture://child");
            ok &= result_ok(result, "complete child");
        }
        result = bf_workgraph_evaluate_fanin(graph, "queue-fan", "join");
        ok &= result_ok(result, "release fan in") && strcmp(result.node_status, "ready") == 0;
    }

    ok &= create_job(graph, "queue-restart", "restart", "fixture://restart", 1, 20, QUEUE_DEFAULT_FAMILY, 100, 0);
    claim_spec.worker_id = "fixture-restart-worker";
    claim = bf_workgraph_claim_node(graph, "queue-restart", "job", &claim_spec);
    ok &= result_ok(claim, "claim restart job");
    bf_workgraph_close(graph);
    graph = NULL;
    ok &= bf_workgraph_open(database_path, &graph, stderr) == 0;
    result = bf_workgraph_status(graph, "queue-restart", "job");
    ok &= result_ok(result, "restart status") && strcmp(result.node_status, "running") == 0;
    result = bf_workgraph_complete(graph, claim.mission_id, claim.node_id, claim.worker_id,
                                   claim.claim_token, "fixture://restart-complete");
    ok &= result_ok(result, "restart ack");

    bf_workgraph_close(graph);
    printf("{\"enqueue\":%s,\"claim\":%s,\"lease\":%s,\"lease_renewal\":%s,"
           "\"ack\":%s,\"lease_expiry\":%s,\"retry\":%s,\"backoff\":%s,"
           "\"dead_letter\":%s,\"cancellation\":%s,\"fan_out\":%s,"
           "\"fan_in\":%s,\"pipeline_continuation\":%s,\"restart_resume\":%s}\n",
           ok ? "true" : "false", ok ? "true" : "false", ok ? "true" : "false",
           ok ? "true" : "false", ok ? "true" : "false", ok ? "true" : "false",
           ok ? "true" : "false", ok ? "true" : "false", ok ? "true" : "false",
           ok ? "true" : "false", ok ? "true" : "false", ok ? "true" : "false",
           ok ? "true" : "false", ok ? "true" : "false");
    return ok ? 0 : 1;
}

static int enqueue_batch(BfWorkgraph *graph, const char *path) {
    FILE *in = NULL;
    char *line = NULL;
    size_t capacity = 0;
    ssize_t length;
    int accepted = 0, failed = 0;

    if (path == NULL) return 0;
    const int owns_stream = strcmp(path, "-") != 0;
    in = owns_stream ? fopen(path, "r") : stdin;
    if (in == NULL) {
        fprintf(stderr, "bonfyre-queue: cannot open batch %s\n", path);
        return 0;
    }
    while ((length = getline(&line, &capacity, in)) >= 0) {
        char *save = NULL;
        char *mission, *type, *input, *family, *priority_text, *retries_text, *backoff_text;
        int priority = 100, retries = 3;
        int64_t backoff_ms = 1000;
        if (length && line[length - 1] == '\n') line[--length] = '\0';
        if (!length || line[0] == '#') continue;
        mission = strtok_r(line, "\t", &save);
        type = strtok_r(NULL, "\t", &save);
        input = strtok_r(NULL, "\t", &save);
        family = strtok_r(NULL, "\t", &save);
        priority_text = strtok_r(NULL, "\t", &save);
        retries_text = strtok_r(NULL, "\t", &save);
        backoff_text = strtok_r(NULL, "\t", &save);
        if (!mission || !type || !input || !family) { failed++; continue; }
        if (priority_text && *priority_text) priority = atoi(priority_text);
        if (retries_text && *retries_text) retries = atoi(retries_text);
        if (backoff_text && *backoff_text) backoff_ms = strtoll(backoff_text, NULL, 10);
        if (create_job(graph, mission, type, input, retries, backoff_ms, family, priority, 1)) accepted++;
        else failed++;
    }
    free(line);
    if (owns_stream) fclose(in);
    printf("{\"accepted\":%d,\"failed\":%d,\"idempotent\":true}\n", accepted, failed);
    return failed == 0;
}

static int claim_batch(BfWorkgraph *graph, const char *worker, const char *family,
                       int count, int64_t lease_ms) {
    BfWorkgraphClaimSpec spec = {worker, family, lease_ms};
    int claimed = 0;
    while (claimed < count) {
        BfWorkgraphResult result = bf_workgraph_claim_next(graph, &spec);
        if (result.status == BF_WORKGRAPH_NOT_ELIGIBLE) break;
        if (result.status != BF_WORKGRAPH_OK) {
            print_result(&result);
            return 0;
        }
        print_result(&result);
        claimed++;
    }
    return 1;
}

static int finish_batch(BfWorkgraph *graph, const char *path, int fail_mode,
                        const char *failure_class, const char *message) {
    FILE *in = NULL;
    char *line = NULL;
    size_t capacity = 0;
    ssize_t length;
    int finished = 0, failures = 0;
    if (path == NULL) return 0;
    const int owns_stream = strcmp(path, "-") != 0;
    in = owns_stream ? fopen(path, "r") : stdin;
    if (in == NULL) {
        fprintf(stderr, "bonfyre-queue: cannot open completion batch %s\n", path);
        return 0;
    }
    while ((length = getline(&line, &capacity, in)) >= 0) {
        char *save = NULL;
        char *mission, *node, *worker, *token, *output;
        BfWorkgraphResult result;
        if (length && line[length - 1] == '\n') line[--length] = '\0';
        if (!length || line[0] == '#') continue;
        mission = strtok_r(line, "\t", &save);
        node = strtok_r(NULL, "\t", &save);
        worker = strtok_r(NULL, "\t", &save);
        token = strtok_r(NULL, "\t", &save);
        output = strtok_r(NULL, "\t", &save);
        if (!mission || !node || !worker || !token) { failures++; continue; }
        if (fail_mode) {
            BfWorkgraphFailure failure = {failure_class ? failure_class : "transient",
                                          message ? message : "batch worker failed"};
            result = bf_workgraph_fail(graph, mission, node, worker, token, &failure);
        } else {
            result = bf_workgraph_complete(graph, mission, node, worker, token,
                                           output && *output ? output : NULL);
        }
        if (result.status == BF_WORKGRAPH_OK) finished++;
        else { failures++; print_result(&result); }
    }
    free(line);
    if (owns_stream) fclose(in);
    printf("{\"finished\":%d,\"failed\":%d,\"mode\":\"%s\"}\n",
           finished, failures, fail_mode ? "fail" : "ack");
    return failures == 0;
}

static void usage(void) {
    fprintf(stderr,
            "BonfyreQueue v%s — WorkGraph-backed durable queue\n"
            "usage: bonfyre-queue fixture --db FILE\n"
            "       bonfyre-queue enqueue TYPE INPUT [--mission ID] [--priority N] [--if-absent] [--db FILE] [--retries N] [--family F]\n"
            "           account admission: --subject SUBJECT --scope self|institution|household --title TITLE --continuation PATH\n"
            "       bonfyre-queue activity --subject SUBJECT --scope SCOPE [--mission ID] [--cursor N] --db FILE  read-only MessagePack\n"
            "       bonfyre-queue account-work index|read|usage|audit|agents --db INDEX --subject SUBJECT --scope SCOPE [--source KIND=PATH]... [--mission ID] [--cursor N]\n"
            "       bonfyre-queue account-inbox list|set|receipt --subject SUBJECT --scope SCOPE --actor user:ID --db FILE [options]\n"
            "       bonfyre-queue enqueue-batch TSV [--db FILE]\n"
            "       bonfyre-queue claim-next WORKER [--family FAMILY] [--lease-ms N] [--db FILE]\n"
            "       bonfyre-queue claim MISSION NODE WORKER [--family FAMILY] [--lease-ms N] [--db FILE]\n"
            "       bonfyre-queue depth [--family FAMILY] [--db FILE]\n"
            "       bonfyre-queue claim-batch WORKER [--count N] [--family FAMILY] [--lease-ms N] [--db FILE]\n"
            "       bonfyre-queue ack-batch|fail-batch TSV [options] [--db FILE]\n"
            "       bonfyre-queue renew|ack|fail MISSION NODE WORKER TOKEN [options] [--db FILE]\n"
            "       bonfyre-queue cancel MISSION NODE [WORKER TOKEN] [--db FILE]\n"
            "       bonfyre-queue reap-expired [MISSION] [--db FILE]\n"
            "       bonfyre-queue mission MISSION [--db FILE]  read-only node inspection\n"
            "       bonfyre-queue checkpoint-read MISSION NODE --db FILE\n"
            "       bonfyre-queue checkpoint-save MISSION NODE WORKER TOKEN --expected-revision N --artifact-uri URI --db FILE\n"
            "       bonfyre-queue ack MISSION NODE WORKER TOKEN --checkpoint-revision N --output-uri URI --db FILE\n"
            "       bonfyre-queue run-attest MISSION NODE WORKER TOKEN --agent claude|codex --session REF [--model ID] [--handoff sha256:HEX] --db FILE\n",
            VERSION);
}

#define QUEUE_COMMANDS(X) \
    X("account-inbox", QC_ACCOUNT_INBOX, 3) \
    X("account-work", QC_ACCOUNT_WORK, 3) \
    X("ack", QC_ACK, 6) \
    X("ack-batch", QC_ACK_BATCH, 3) \
    X("activity", QC_ACTIVITY, 2) \
    X("cancel", QC_CANCEL, 4) \
    X("checkpoint-read", QC_CHECKPOINT_READ, 4) \
    X("checkpoint-save", QC_CHECKPOINT_SAVE, 6) \
    X("claim", QC_CLAIM, 5) \
    X("claim-batch", QC_CLAIM_BATCH, 3) \
    X("claim-next", QC_CLAIM_NEXT, 3) \
    X("depth", QC_DEPTH, 2) \
    X("enqueue", QC_ENQUEUE, 4) \
    X("enqueue-batch", QC_ENQUEUE_BATCH, 3) \
    X("fail", QC_FAIL, 6) \
    X("fail-batch", QC_FAIL_BATCH, 3) \
    X("fixture", QC_FIXTURE, 2) \
    X("mission", QC_MISSION, 3) \
    X("reap-expired", QC_REAP_EXPIRED, 2) \
    X("renew", QC_RENEW, 6) \
    X("run-attest", QC_RUN_ATTEST, 6) \
    X("status", QC_STATUS, 4)

enum {
#define QUEUE_ENUM(name, id, minimum_argc) id,
    QUEUE_COMMANDS(QUEUE_ENUM)
#undef QUEUE_ENUM
};
static const BfCommandEntry queue_commands[] = {
#define QUEUE_ENTRY(name, id, minimum_argc) {name, id, minimum_argc, NULL},
    QUEUE_COMMANDS(QUEUE_ENTRY)
#undef QUEUE_ENTRY
};

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(queue_commands,
        sizeof(queue_commands) / sizeof(queue_commands[0]), argv[1]);
    if (!command || argc < command->mode) {
        usage();
        return 2;
    }
    if (command->id == QC_ACCOUNT_INBOX) return qbox_cli(argc, argv);
    if (command->id == QC_ACCOUNT_WORK) return awk_cli(argc, argv);
    const char *database_path = option_value(argc, argv, "--db");
    BfWorkgraph *graph = NULL;
    BfWorkgraphResult result;
    if (database_path == NULL) {
        usage();
        return 2;
    }
    if (command->id == QC_FIXTURE) return fixture(database_path);
    if (command->id == QC_MISSION) return inspect_mission(database_path, argv[2]);
    if (command->id == QC_DEPTH) return queue_depth(database_path, option_value(argc, argv, "--family"));
    if (command->id == QC_ACTIVITY) {
        const char *cursor = option_value(argc, argv, "--cursor");
        int64_t value = 0;
        if (cursor) {
            if (!*cursor || strlen(cursor) > 16) return 2;
            for (const char *p = cursor; *p; p++) {
                if (*p < '0' || *p > '9') return 2;
                value = value * 10 + (*p - '0');
                if (value > QACT_MAX_TIME) return 2;
            }
        }
        return qact_read(database_path, option_value(argc, argv, "--subject"),
            option_value(argc, argv, "--scope"), option_value(argc, argv, "--mission"), value, stdout);
    }
    if (bf_workgraph_open(database_path, &graph, stderr) != 0) return 1;
    if (command->id == QC_CHECKPOINT_READ || command->id == QC_CHECKPOINT_SAVE) {
        BfWorkgraphCheckpoint record;
        result = command->id == QC_CHECKPOINT_READ
            ? bf_workgraph_checkpoint_read(graph, argv[2], argv[3], &record)
            : bf_workgraph_checkpoint_save(graph, argv[2], argv[3], argv[4], argv[5],
                integer_option(argc, argv, "--expected-revision", -1),
                option_value(argc, argv, "--artifact-uri"), &record);
        bf_workgraph_close(graph);
        if (result.status == BF_WORKGRAPH_OK) print_checkpoint(&record);
        else print_result(&result);
        return result.status == BF_WORKGRAPH_OK ? 0 : 1;
    }
    if (command->id == QC_RUN_ATTEST) {
        /* The harness session that runs this claimed attempt, bound into the
         * node's chained receipts; the account Work index projects it as a run. */
        BfWorkgraphRunEvidence run = {option_value(argc, argv, "--agent"), option_value(argc, argv, "--session"),
            option_value(argc, argv, "--model"), option_value(argc, argv, "--handoff")};
        result = bf_workgraph_attest_run(graph, argv[2], argv[3], argv[4], argv[5], &run);
        bf_workgraph_close(graph);
        print_result(&result);
        return result.status == BF_WORKGRAPH_OK ? 0 : 1;
    }
    switch (command->id) {
    case QC_ENQUEUE: {
        char generated_mission[128];
        const char *subject = option_value(argc, argv, "--subject");
        const char *scope = option_value(argc, argv, "--scope");
        const char *title = option_value(argc, argv, "--title");
        const char *continuation = option_value(argc, argv, "--continuation");
        int scoped = option_present(argc, argv, "--subject") || option_present(argc, argv, "--scope") ||
            option_present(argc, argv, "--title") || option_present(argc, argv, "--continuation");
        const char *mission = option_value(argc, argv, "--mission");
        if (mission == NULL) {
            snprintf(generated_mission, sizeof(generated_mission), "%squeue-%lld-%ld",
                     scoped ? "account-" : "", (long long)epoch_ms(), (long)getpid());
            mission = generated_mission;
        }
        if (scoped) {
            char operator_id[128];
            const char *family = option_value(argc, argv, "--family");
            int n = snprintf(operator_id, sizeof(operator_id), "queue.%s", argv[2]);
            QactScope admission = {mission, subject, scope, title, continuation, operator_id, argv[3], family ? family : QUEUE_DEFAULT_FAMILY};
            if (n < 0 || n >= (int)sizeof(operator_id) || qact_reserve(database_path, &admission) != 1) {
                fprintf(stderr, "bonfyre-queue: account admission is unavailable or conflicts with existing work\n");
                bf_workgraph_close(graph);
                return 1;
            }
        }
        if (!create_job(graph, mission, argv[2], argv[3],
                        (int)integer_option(argc, argv, "--retries", 3),
                        integer_option(argc, argv, "--backoff-ms", 1000),
                        option_value(argc, argv, "--family"),
                        (int)integer_option(argc, argv, "--priority", 100),
                        option_present(argc, argv, "--if-absent"))) {
            bf_workgraph_close(graph);
            return 1;
        }
        result = bf_workgraph_status(graph, mission, "job");
        if (scoped) {
            sqlite3 *check = NULL;
            char version[65];
            int valid = sqlite3_open_v2(database_path, &check, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
                sqlite3_exec(check, "BEGIN", NULL, NULL, NULL) == SQLITE_OK &&
                qact_version(check, subject, scope, mission, version) == 1;
            if (check) { sqlite3_exec(check, "ROLLBACK", NULL, NULL, NULL); sqlite3_close(check); }
            if (!valid) {
                fprintf(stderr, "bonfyre-queue: the native mission does not match its account admission\n");
                bf_workgraph_close(graph);
                return 1;
            }
        }
        break;
    }
    case QC_ENQUEUE_BATCH: {
        int ok = enqueue_batch(graph, argv[2]);
        bf_workgraph_close(graph);
        return ok ? 0 : 1;
    }
    case QC_CLAIM_BATCH: {
        int ok = claim_batch(graph, argv[2], option_value(argc, argv, "--family"),
                             (int)integer_option(argc, argv, "--count", 1),
                             integer_option(argc, argv, "--lease-ms", 30000));
        bf_workgraph_close(graph);
        return ok ? 0 : 1;
    }
    case QC_ACK_BATCH: {
        int ok = finish_batch(graph, argv[2], 0, NULL, NULL);
        bf_workgraph_close(graph);
        return ok ? 0 : 1;
    }
    case QC_FAIL_BATCH: {
        int ok = finish_batch(graph, argv[2], 1, option_value(argc, argv, "--class"),
                              option_value(argc, argv, "--message"));
        bf_workgraph_close(graph);
        return ok ? 0 : 1;
    }
    case QC_CLAIM: {
        BfWorkgraphClaimSpec spec = {argv[4], option_value(argc, argv, "--family"),
                                     integer_option(argc, argv, "--lease-ms", 30000)};
        result = bf_workgraph_claim_node(graph, argv[2], argv[3], &spec);
        break;
    }
    case QC_CLAIM_NEXT: {
        const char *want_family = option_value(argc, argv, "--family");
        BfWorkgraphClaimSpec spec = {argv[2], want_family,
                                     integer_option(argc, argv, "--lease-ms", 30000)};
        result = bf_workgraph_claim_next(graph, &spec);
        /* An empty answer under a family filter has two very different causes:
         * there is no work, or there is work filed under a family nobody asked
         * for. They read identically as no_eligible_node, and the second one
         * cost an afternoon -- claim-next --family local_fallback found nothing
         * while a bare claim-next took the job on the next line. Naming the
         * families that do hold ready work separates the two in one line. */
        if (result.status != BF_WORKGRAPH_OK && want_family) {
            report_ready_families(database_path, want_family);
        }
        break;
    }
    case QC_RENEW:
        result = bf_workgraph_renew(graph, argv[2], argv[3], argv[4], argv[5],
                                    integer_option(argc, argv, "--lease-ms", 30000));
        break;
    case QC_ACK:
        result = option_present(argc, argv, "--checkpoint-revision")
            ? bf_workgraph_complete_checkpoint(graph, argv[2], argv[3], argv[4], argv[5],
                option_value(argc, argv, "--output-uri"), integer_option(argc, argv, "--checkpoint-revision", -1))
            : bf_workgraph_complete(graph, argv[2], argv[3], argv[4], argv[5],
                option_value(argc, argv, "--output-uri"));
        break;
    case QC_FAIL: {
        BfWorkgraphFailure failure = {option_value(argc, argv, "--class"),
                                      option_value(argc, argv, "--message")};
        result = bf_workgraph_fail(graph, argv[2], argv[3], argv[4], argv[5], &failure);
        break;
    }
    case QC_CANCEL:
        result = bf_workgraph_cancel_node(graph, argv[2], argv[3],
                                          argc > 4 && argv[4][0] != '-' ? argv[4] : NULL,
                                          argc > 5 && argv[5][0] != '-' ? argv[5] : NULL);
        break;
    case QC_REAP_EXPIRED:
        result = bf_workgraph_reap_expired(graph, argc > 2 && argv[2][0] != '-' ? argv[2] : NULL);
        break;
    case QC_STATUS:
        result = bf_workgraph_status(graph, argv[2], argv[3]);
        break;
    default:
        bf_workgraph_close(graph);
        usage();
        return 2;
    }
    bf_workgraph_close(graph);
    print_result(&result);
    return result.status == BF_WORKGRAPH_OK ? 0 : 1;
}
