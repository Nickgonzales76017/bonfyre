#define _POSIX_C_SOURCE 200809L
#include "bf_workgraph.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define VERSION "3.0.0"

static const char *option_value(int argc, char **argv, const char *name) {
    for (int index = 1; index + 1 < argc; ++index) {
        if (strcmp(argv[index], name) == 0) return argv[index + 1];
    }
    return NULL;
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

static BfWorkgraphNodeSpec node_spec(const char *mission, const char *node,
                                     const char *operator_id, const char *input,
                                     int retry_limit, int64_t backoff_ms) {
    BfWorkgraphNodeSpec spec;
    memset(&spec, 0, sizeof(spec));
    spec.mission_id = mission;
    spec.node_id = node;
    spec.operator_id = operator_id;
    spec.input_uri = input;
    spec.family = "queue";
    spec.priority = 100;
    spec.retry_limit = retry_limit;
    spec.timeout_seconds = 30;
    spec.backoff_base_ms = backoff_ms;
    spec.backoff_multiplier = 2.0;
    spec.backoff_max_ms = backoff_ms > 0 ? backoff_ms : 60000;
    return spec;
}

static int create_job(BfWorkgraph *graph, const char *mission, const char *type,
                      const char *input, int retries, int64_t backoff_ms) {
    char operator_id[160];
    BfWorkgraphNodeSpec spec;

    if (!result_ok(bf_workgraph_create_mission(graph, mission), "create mission")) return 0;
    snprintf(operator_id, sizeof(operator_id), "queue.%s", type);
    spec = node_spec(mission, "job", operator_id, input, retries, backoff_ms);
    return result_ok(bf_workgraph_add_node(graph, &spec), "enqueue");
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
    ok &= create_job(graph, "queue-success", "success", "fixture://success", 1, 20);
    claim = bf_workgraph_claim_next(graph, &claim_spec);
    ok &= result_ok(claim, "claim success");
    result = bf_workgraph_renew(graph, claim.mission_id, claim.node_id, claim.worker_id,
                                claim.claim_token, 30000);
    ok &= result_ok(result, "renew success");
    result = bf_workgraph_complete(graph, claim.mission_id, claim.node_id, claim.worker_id,
                                   claim.claim_token, "fixture://completed");
    ok &= result_ok(result, "ack success");

    ok &= create_job(graph, "queue-retry", "transient", "fixture://retry", 1, 20);
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

    ok &= create_job(graph, "queue-dead", "permanent", "fixture://dead", 3, 20);
    claim_spec.worker_id = "fixture-dead-worker";
    claim = bf_workgraph_claim_next(graph, &claim_spec);
    ok &= result_ok(claim, "claim dead-letter job");
    result = bf_workgraph_fail(graph, claim.mission_id, claim.node_id, claim.worker_id,
                               claim.claim_token, &permanent_failure);
    ok &= result_ok(result, "dead letter");
    ok &= strcmp(result.node_status, "dead_letter") == 0;

    ok &= create_job(graph, "queue-expiry", "expiry", "fixture://expiry", 1, 20);
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

    ok &= create_job(graph, "queue-cancel", "cancel", "fixture://cancel", 0, 20);
    result = bf_workgraph_cancel_node(graph, "queue-cancel", "job", NULL, NULL);
    ok &= result_ok(result, "cancel queued job") && strcmp(result.node_status, "cancelled") == 0;

    ok &= create_job(graph, "queue-fan", "parent", "fixture://fan", 0, 20);
    claim_spec.worker_id = "fixture-fan-worker";
    claim = bf_workgraph_claim_next(graph, &claim_spec);
    ok &= result_ok(claim, "claim fan parent");
    result = bf_workgraph_complete(graph, claim.mission_id, claim.node_id, claim.worker_id,
                                   claim.claim_token, "fixture://fan-parent");
    ok &= result_ok(result, "complete fan parent");
    {
        BfWorkgraphFanoutSpec fanout = {"queue-fan", "job", "group", "queue.child",
                                        "queue", "child", 2, "fail"};
        BfWorkgraphNodeSpec join = node_spec("queue-fan", "join", "queue.join", NULL, 0, 20);
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

    ok &= create_job(graph, "queue-restart", "restart", "fixture://restart", 1, 20);
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

static void usage(void) {
    fprintf(stderr,
            "BonfyreQueue v%s — WorkGraph-backed durable queue\n"
            "usage: bonfyre-queue fixture --db FILE\n"
            "       bonfyre-queue enqueue TYPE INPUT [--db FILE] [--retries N]\n"
            "       bonfyre-queue claim-next WORKER [--family FAMILY] [--lease-ms N] [--db FILE]\n"
            "       bonfyre-queue renew|ack|fail MISSION NODE WORKER TOKEN [options] [--db FILE]\n"
            "       bonfyre-queue cancel MISSION NODE [WORKER TOKEN] [--db FILE]\n"
            "       bonfyre-queue reap-expired [MISSION] [--db FILE]\n",
            VERSION);
}

int main(int argc, char **argv) {
    const char *database_path = option_value(argc, argv, "--db");
    BfWorkgraph *graph = NULL;
    BfWorkgraphResult result;

    if (argc < 2 || database_path == NULL) {
        usage();
        return 2;
    }
    if (strcmp(argv[1], "fixture") == 0) return fixture(database_path);
    if (bf_workgraph_open(database_path, &graph, stderr) != 0) return 1;
    if (strcmp(argv[1], "enqueue") == 0 && argc >= 4) {
        char mission[128];
        snprintf(mission, sizeof(mission), "queue-%lld-%ld", (long long)epoch_ms(), (long)getpid());
        if (!create_job(graph, mission, argv[2], argv[3],
                        (int)integer_option(argc, argv, "--retries", 3),
                        integer_option(argc, argv, "--backoff-ms", 1000))) {
            bf_workgraph_close(graph);
            return 1;
        }
        result = bf_workgraph_status(graph, mission, "job");
    } else if (strcmp(argv[1], "claim-next") == 0 && argc >= 3) {
        BfWorkgraphClaimSpec spec = {argv[2], option_value(argc, argv, "--family"),
                                     integer_option(argc, argv, "--lease-ms", 30000)};
        result = bf_workgraph_claim_next(graph, &spec);
    } else if (strcmp(argv[1], "renew") == 0 && argc >= 6) {
        result = bf_workgraph_renew(graph, argv[2], argv[3], argv[4], argv[5],
                                    integer_option(argc, argv, "--lease-ms", 30000));
    } else if (strcmp(argv[1], "ack") == 0 && argc >= 6) {
        result = bf_workgraph_complete(graph, argv[2], argv[3], argv[4], argv[5],
                                       option_value(argc, argv, "--output-uri"));
    } else if (strcmp(argv[1], "fail") == 0 && argc >= 6) {
        BfWorkgraphFailure failure = {option_value(argc, argv, "--class"),
                                      option_value(argc, argv, "--message")};
        result = bf_workgraph_fail(graph, argv[2], argv[3], argv[4], argv[5], &failure);
    } else if (strcmp(argv[1], "cancel") == 0 && argc >= 4) {
        result = bf_workgraph_cancel_node(graph, argv[2], argv[3],
                                          argc > 4 && argv[4][0] != '-' ? argv[4] : NULL,
                                          argc > 5 && argv[5][0] != '-' ? argv[5] : NULL);
    } else if (strcmp(argv[1], "reap-expired") == 0) {
        result = bf_workgraph_reap_expired(graph, argc > 2 && argv[2][0] != '-' ? argv[2] : NULL);
    } else if (strcmp(argv[1], "status") == 0 && argc >= 4) {
        result = bf_workgraph_status(graph, argv[2], argv[3]);
    } else {
        bf_workgraph_close(graph);
        usage();
        return 2;
    }
    bf_workgraph_close(graph);
    print_result(&result);
    return result.status == BF_WORKGRAPH_OK ? 0 : 1;
}
