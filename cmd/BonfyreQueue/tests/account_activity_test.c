#define _POSIX_C_SOURCE 200809L
#include "../src/account_activity.h"
#include "bf_workgraph.h"
#include "bf_lifecycle.h"
#include <assert.h>
#include <unistd.h>

static BfWorkgraphNodeSpec spec(const char *mission, const char *node, const char *parent) {
    BfWorkgraphNodeSpec s = {0};
    s.mission_id = mission; s.node_id = node; s.parent_node = parent;
    s.operator_id = "queue.fixture"; s.input_uri = "private-input-secret"; s.family = "queue";
    s.priority = 100; s.timeout_seconds = 30; s.backoff_base_ms = 1000;
    s.backoff_multiplier = 2; s.backoff_max_ms = 1000;
    return s;
}

static void native_root(BfWorkgraph *graph, const char *mission) {
    assert(bf_workgraph_create_mission(graph, mission).status == BF_WORKGRAPH_OK);
    BfWorkgraphNodeSpec s = spec(mission, "job", NULL);
    assert(bf_workgraph_add_node(graph, &s).status == BF_WORKGRAPH_OK);
}

static void admit(const char *path, BfWorkgraph *graph, const char *mission, const char *subject, const char *scope) {
    char continuation[128];
    snprintf(continuation, sizeof(continuation), "/apps?context=%s", !strcmp(scope, "self") ? "self" : subject);
    QactScope s = {mission, subject, scope, "Fixture task", continuation, "queue.fixture", "private-input-secret", "queue"};
    assert(qact_reserve(path, &s) == 1);
    native_root(graph, mission);
}

static void complete(BfWorkgraph *graph, const char *mission, const char *node, const char *output) {
    BfWorkgraphClaimSpec claim = {"fixture-worker", "queue", 30000};
    BfWorkgraphResult r = bf_workgraph_claim_node(graph, mission, node, &claim);
    assert(r.status == BF_WORKGRAPH_OK);
    assert(bf_workgraph_complete(graph, mission, node, claim.worker_id, r.claim_token, output).status == BF_WORKGRAPH_OK);
}

static int bytes_contain(const uint8_t *p, size_t n, const char *needle) {
    size_t m = strlen(needle);
    for (size_t i = 0; i + m <= n; i++) if (!memcmp(p + i, needle, m)) return 1;
    return 0;
}

static void packet_is_bounded_private(const char *path, const char *mission) {
    FILE *f = tmpfile();
    assert(f && qact_read(path, "user:7", "self", mission, 0, f) == 0);
    assert(!fseek(f, 0, SEEK_END));
    long n = ftell(f);
    assert(n > 0 && n < (long)QACT_PACKET_LIMIT);
    rewind(f);
    uint8_t *bytes = malloc((size_t)n);
    assert(bytes && fread(bytes, 1, (size_t)n, f) == (size_t)n);
    assert(!bytes_contain(bytes, (size_t)n, "private-input-secret"));
    assert(!bytes_contain(bytes, (size_t)n, "file:///private"));
    assert(!bytes_contain(bytes, (size_t)n, "claim_token"));
    assert(!bytes_contain(bytes, (size_t)n, "protocol-private"));
    bf_mp_reader_t r;
    bf_mp_reader_init(&r, bytes, (size_t)n);
    assert(bf_mp_skip(&r) == 0 && !bf_mp_reader_remaining(&r));
    free(bytes); fclose(f);
}

int main(void) {
    char directory[] = "/tmp/bonfyre-account-activity-XXXXXX";
    assert(mkdtemp(directory));
    char path[256];
    snprintf(path, sizeof(path), "%s/queue.db", directory);
    BfWorkgraph *graph = NULL;
    assert(bf_workgraph_open(path, &graph, stderr) == 0);
    assert(bf_workgraph_register_family(graph, "queue").status == BF_WORKGRAPH_OK);
    sqlite3 *db = NULL;
    assert(sqlite3_open(path, &db) == SQLITE_OK);
    assert(sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL) == SQLITE_OK);
    QactTask task;
    QactNotice notice;
    char ids[51][128], version[65], next[65];
    int64_t sequences[51];
    size_t count = 0;

    /* Existing work cannot be claimed by a later account association. */
    native_root(graph, "account-legacy");
    QactScope legacy = {"account-legacy", "user:7", "self", "Fixture task", "/apps?context=self", "queue.fixture", "private-input-secret", "queue"};
    assert(qact_reserve(path, &legacy) == 0);
    assert(qact_notice_ids(db, "user:7", "self", 0, ids, sequences, &count) == 1 && count == 0);
    assert(qact_notice(db, "user:7", "self", "account-legacy", &notice) == 0);

    QactScope reserved = {"account-resume", "user:7", "self", "Fixture task", "/apps?context=self", "queue.fixture", "private-input-secret", "queue"};
    QactScope unreserved_name = reserved; unreserved_name.mission = "unscoped-name";
    assert(qact_reserve(path, &unreserved_name) == 0);
    assert(qact_reserve(path, &reserved) == 1 && qact_reserve(path, &reserved) == 1);
    assert(qact_notice(db, "user:7", "self", "account-resume", &notice) == 0);
    assert(qact_notice_ids(db, "user:7", "self", 0, ids, sequences, &count) == 1 && count == 0);
    QactScope collision = reserved; collision.subject = "user:8";
    assert(qact_reserve(path, &collision) == 0);
    collision = reserved; collision.input_uri = "different-input";
    assert(qact_reserve(path, &collision) == 0);
    native_root(graph, "account-resume");
    assert(qact_reserve(path, &reserved) == 1);
    assert(qact_load(db, "user:7", "self", "account-resume", &task) == 1);
    assert(!strcmp(task.notice.phase, "pending") && task.total_nodes == 1 && task.node_count == 1);
    assert(!strcmp(task.nodes[0].id, "job") && !task.nodes[0].parent[0]);
    assert(qact_notice(db, "user:8", "self", "account-resume", &notice) == 0);
    assert(qact_notice(db, "institution:acme", "institution", "account-resume", &notice) == 0);
    assert(qact_notice(db, "user:7", "institution", "account-resume", &notice) == -1);
    assert(sqlite3_exec(db, "UPDATE queue_activity_scopes SET subject='user:8'", NULL, NULL, NULL) != SQLITE_OK);
    assert(sqlite3_exec(db, "DELETE FROM queue_activity_scopes", NULL, NULL, NULL) != SQLITE_OK);

    assert(qact_version(db, "user:7", "self", "account-resume", version) == 1);
    BfWorkgraphNodeSpec child = spec("account-resume", "child", "job");
    assert(bf_workgraph_add_node(graph, &child).status == BF_WORKGRAPH_OK);
    assert(qact_version(db, "user:7", "self", "account-resume", next) == 1 && strcmp(version, next));
    memcpy(version, next, 65);
    assert(bf_workgraph_add_dependency(graph, "account-resume", "child", "job", "success").status == BF_WORKGRAPH_OK);
    assert(qact_version(db, "user:7", "self", "account-resume", next) == 1 && strcmp(version, next));
    complete(graph, "account-resume", "job", "file:///private/result-output");
    assert(qact_load(db, "user:7", "self", "account-resume", &task) == 1);
    assert(!strcmp(task.state, "complete") && !strcmp(task.notice.phase, "pending"));
    assert(task.total_nodes == 2 && task.total_outputs == 1 && task.output_count == 1);
    assert(!strcmp(task.nodes[1].parent, "job") && !strcmp(task.outputs[0].node, "job"));
    packet_is_bounded_private(path, "account-resume");
    BfWorkgraphClaimSpec claim = {"fixture-worker", "queue", 30000};
    BfWorkgraphResult active = bf_workgraph_claim_node(graph, "account-resume", "child", &claim);
    assert(active.status == BF_WORKGRAPH_OK);
    assert(qact_load(db, "user:7", "self", "account-resume", &task) == 1 && !strcmp(task.notice.phase, "active"));
    BfWorkgraphFailure failure = {"permanent", "private failure details"};
    assert(bf_workgraph_fail(graph, "account-resume", "child", claim.worker_id, active.claim_token, &failure).status == BF_WORKGRAPH_OK);
    assert(qact_load(db, "user:7", "self", "account-resume", &task) == 1 && !strcmp(task.notice.phase, "attention"));

    /* WorkerRun start has no transition row. Its admission must still create a
     * new version and keep a completed root from hiding active native workers. */
    admit(path, graph, "account-worker-task", "user:7", "self");
    complete(graph, "account-worker-task", "job", NULL);
    assert(qact_version(db, "user:7", "self", "account-worker-task", version) == 1);
    BfWorkerRunSpec worker = {"worker-run-1", "research-worker", "account-worker-task", "protocol-private-1"};
    assert(bf_worker_run_start(db, &worker).status == BF_LIFECYCLE_OK);
    assert(qact_version(db, "user:7", "self", "account-worker-task", next) == 1 && strcmp(next, version));
    assert(qact_load(db, "user:7", "self", "account-worker-task", &task) == 1);
    assert(!strcmp(task.notice.phase, "active") && task.total_workers == 1 && task.worker_count == 1);
    assert(!strcmp(task.workers[0].worker, "research-worker"));
    assert(bf_worker_run_step(db, worker.run_id, "wait", "wait-1", NULL).status == BF_LIFECYCLE_OK);
    assert(qact_load(db, "user:7", "self", "account-worker-task", &task) == 1 && !strcmp(task.notice.phase, "pending"));
    assert(bf_worker_run_step(db, worker.run_id, "resume", "resume-1", NULL).status == BF_LIFECYCLE_OK);
    assert(bf_worker_run_step(db, worker.run_id, "complete", "complete-1", "retained-receipt").status == BF_LIFECYCLE_OK);
    assert(qact_load(db, "user:7", "self", "account-worker-task", &task) == 1 && !strcmp(task.notice.phase, "completed"));
    packet_is_bounded_private(path, "account-worker-task");

    /* Counts and versions include work beyond the visible child/worker arrays. */
    admit(path, graph, "account-many", "user:7", "self");
    for (int i = 0; i < 35; i++) {
        char node[32]; snprintf(node, sizeof(node), "child-%02d", i);
        BfWorkgraphNodeSpec s = spec("account-many", node, "job");
        assert(bf_workgraph_add_node(graph, &s).status == BF_WORKGRAPH_OK);
    }
    for (int i = 0; i < 18; i++) {
        char run[32], protocol[32];
        snprintf(run, sizeof(run), "many-run-%02d", i); snprintf(protocol, sizeof(protocol), "many-protocol-%02d", i);
        BfWorkerRunSpec s = {run, "fixture-worker", "account-many", protocol};
        assert(bf_worker_run_start(db, &s).status == BF_LIFECYCLE_OK);
    }
    assert(qact_load(db, "user:7", "self", "account-many", &task) == 1);
    assert(task.total_nodes == 36 && task.node_count == 32 && task.total_workers == 18 && task.worker_count == 16 && task.active_workers == 18);
    memcpy(version, task.notice.version, 65);
    assert(bf_worker_run_step(db, "many-run-00", "fail", "hidden-worker-fail", NULL).status == BF_LIFECYCLE_OK);
    assert(qact_load(db, "user:7", "self", "account-many", &task) == 1 && strcmp(version, task.notice.version) && !strcmp(task.notice.phase, "attention") && task.active_workers == 17);
    packet_is_bounded_private(path, "account-many");

    /* A page's snapshot cannot mix versions from before/after concurrent work. */
    assert(sqlite3_exec(db, "BEGIN", NULL, NULL, NULL) == SQLITE_OK);
    assert(qact_version(db, "user:7", "self", "account-many", version) == 1);
    BfWorkgraphNodeSpec late = spec("account-many", "late-child", "job");
    assert(bf_workgraph_add_node(graph, &late).status == BF_WORKGRAPH_OK);
    assert(qact_version(db, "user:7", "self", "account-many", next) == 1 && !strcmp(version, next));
    assert(sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) == SQLITE_OK);
    assert(qact_version(db, "user:7", "self", "account-many", next) == 1 && strcmp(version, next));

    admit(path, graph, "account-org-only", "institution:acme", "institution");
    admit(path, graph, "account-other-user", "user:8", "self");
    for (int i = 0; i < 23; i++) {
        char mission[32]; snprintf(mission, sizeof(mission), "account-page-%02d", i);
        admit(path, graph, mission, "user:7", "self");
    }
    assert(qact_notice_ids(db, "user:7", "self", 0, ids, sequences, &count) == 1 && count == 26);
    for (size_t i = 0; i < count; i++) { assert(strcmp(ids[i], "account-org-only") && strcmp(ids[i], "account-other-user") && strcmp(ids[i], "account-legacy")); if (i) assert(sequences[i] < sequences[i - 1]); }
    int64_t after = sequences[19];
    assert(qact_notice_ids(db, "user:7", "self", after, ids, sequences, &count) == 1 && count == 6);
    assert(qact_notice_ids(db, "institution:acme", "institution", 0, ids, sequences, &count) == 1 && count == 1 && !strcmp(ids[0], "account-org-only"));
    packet_is_bounded_private(path, NULL);

    /* Exact root mutation cannot make a reservation expose a different job. */
    assert(sqlite3_exec(db, "UPDATE workgraph_nodes SET input_uri='replacement-private-input' WHERE mission_id='account-resume' AND node_id='job'", NULL, NULL, NULL) == SQLITE_OK);
    assert(qact_notice(db, "user:7", "self", "account-resume", &notice) == -1);
    QactScope bad = {"account-bad", "institution:acme", "institution", "Fixture", "/apps?context=institution:other", "queue.fixture", "private-input-secret", "queue"};
    assert(qact_reserve(path, &bad) == 0);
    bad.subject = "user:7"; bad.scope = "self"; bad.continuation = "//external.example";
    assert(qact_reserve(path, &bad) == 0);
    bad.continuation = "/apps/%2e%2e/auth"; assert(qact_reserve(path, &bad) == 0);
    bad.continuation = "/apps?context=self&next=//external.example"; assert(qact_reserve(path, &bad) == 0);
    const char *invalid_routes[] = {"/fleming", "/fleming?context=self", "/apps/", "/apps/example/?context=self", "/apps//example?context=self", "/sources/web/?context=self"};
    for (size_t i = 0; i < sizeof(invalid_routes) / sizeof(invalid_routes[0]); i++) {
        bad.continuation = invalid_routes[i]; assert(qact_reserve(path, &bad) == 0);
    }
    const char *valid_routes[] = {"/apps", "/apps?context=self", "/apps/example/next_step-1?context=self", "/sources/web?context=self"};
    for (size_t i = 0; i < sizeof(valid_routes) / sizeof(valid_routes[0]); i++)
        assert(qact_continuation(valid_routes[i], "user:7", "self"));
    assert(!qact_scope_valid("user:9223372036854775808", "self"));

    /* Resource failure is closed and emits no partial task packet. */
    assert(sqlite3_exec(db,
        "WITH RECURSIVE rows(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM rows WHERE n<4097) "
        "INSERT INTO workgraph_nodes(mission_id,node_id,operator_id,status,attempt,retry_limit,timeout_seconds,"
        "input_uri,family,priority,created_at_ms,updated_at_ms) "
        "SELECT 'account-many','overflow-'||n,'queue.fixture','ready',0,0,30,'','queue',100,0,0 FROM rows", NULL, NULL, NULL) == SQLITE_OK);
    assert(qact_load(db, "user:7", "self", "account-many", &task) == -1);
    FILE *limited = tmpfile(); assert(limited && qact_read(path, "user:7", "self", "account-many", 0, limited) == 1 && ftell(limited) == 0); fclose(limited);

    char missing[256]; snprintf(missing, sizeof(missing), "%s/missing.db", directory);
    FILE *sink = tmpfile();
    assert(sink && qact_read(missing, "user:7", "self", NULL, 0, sink) == 1 && access(missing, F_OK) != 0);
    fclose(sink);
    sqlite3_close(db); bf_workgraph_close(graph);
    unlink(path); rmdir(directory);
    puts("account activity: scoped admission, real hierarchy/lifecycle, private bounded packets and stable pages passed");
    return 0;
}
