#define _POSIX_C_SOURCE 200809L
#include "../src/account_inbox.h"
#include "bf_workgraph.h"
#include "bf_lifecycle.h"
#include <assert.h>
#include <sys/wait.h>
#include <unistd.h>

static BfWorkgraphNodeSpec node_spec(const char *mission, const char *node) {
    BfWorkgraphNodeSpec s = {0};
    s.mission_id = mission; s.node_id = node; s.operator_id = "queue.fixture";
    s.input_uri = "private-inbox-input"; s.family = "queue"; s.timeout_seconds = 30;
    s.priority = 100; s.backoff_base_ms = 1000; s.backoff_multiplier = 2; s.backoff_max_ms = 1000;
    return s;
}

static void create_task(const char *path, BfWorkgraph *graph, const char *id, const char *subject, const char *scope) {
    char href[160];
    snprintf(href, sizeof(href), "/apps?context=%s", !strcmp(scope, "self") ? "self" : subject);
    QactScope admission = {id, subject, scope, "Inbox fixture", href, "queue.fixture", "private-inbox-input", "queue"};
    assert(qact_reserve(path, &admission) == 1);
    assert(bf_workgraph_create_mission(graph, id).status == BF_WORKGRAPH_OK);
    BfWorkgraphNodeSpec s = node_spec(id, "job");
    assert(bf_workgraph_add_node(graph, &s).status == BF_WORKGRAPH_OK);
}

static int64_t scalar(sqlite3 *db, const char *sql) {
    sqlite3_stmt *q = NULL;
    assert(sqlite3_prepare_v2(db, sql, -1, &q, NULL) == SQLITE_OK && sqlite3_step(q) == SQLITE_ROW);
    int64_t value = sqlite3_column_int64(q, 0);
    sqlite3_finalize(q); return value;
}

static int contains(const unsigned char *p, size_t n, const char *s) {
    size_t m = strlen(s);
    for (size_t i = 0; i + m <= n; i++) if (!memcmp(p + i, s, m)) return 1;
    return 0;
}

static void reply(FILE *f, int okay, const char *code) {
    assert(!fseek(f, 0, SEEK_END)); long n = ftell(f);
    assert(n > 0 && n <= (long)QACT_PACKET_LIMIT); rewind(f);
    unsigned char *bytes = malloc((size_t)n);
    assert(bytes && fread(bytes, 1, (size_t)n, f) == (size_t)n);
    assert(!contains(bytes, (size_t)n, "private-inbox-input"));
    bf_mp_reader_t r; bf_mp_value_t v;
    bf_mp_reader_init(&r, bytes, (size_t)n);
    assert(bf_mp_read_next(&r, &v) == BF_MP_ARRAY && v.v.count == 3);
    bf_mp_type_t t = bf_mp_read_next(&r, &v);
    assert((t == BF_MP_UINT && v.v.u == 1) || (t == BF_MP_INT && v.v.i == 1));
    assert(bf_mp_read_next(&r, &v) == BF_MP_BOOL && v.v.b == okay);
    assert(!bf_mp_skip(&r) && !bf_mp_reader_remaining(&r));
    if (code) assert(contains(bytes, (size_t)n, code));
    free(bytes); fclose(f);
}

static QboxChange change_for(sqlite3 *db, const char *subject, const char *scope, const char *actor,
                              const char *id, const char *action, const char *request) {
    QboxNotice notice; QboxChange change = {0};
    assert(qbox_notice(db, subject, scope, actor, id, &notice) == 1);
    snprintf(change.id, sizeof(change.id), "%s", id);
    snprintf(change.version, sizeof(change.version), "%s", notice.task.version);
    snprintf(change.action, sizeof(change.action), "%s", action);
    snprintf(change.request, sizeof(change.request), "%s", request);
    change.revision = notice.revision;
    return change;
}

static void update(const char *path, const char *subject, const char *scope, const char *actor,
                    const QboxChange *change, int okay, const char *code) {
    FILE *f = tmpfile(); assert(f);
    assert(qbox_update(path, subject, scope, actor, change, change->request, f) == 0);
    reply(f, okay, code);
}

static void parser_cases(const QboxChange *change) {
    for (int mode = 0; mode < 8; mode++) {
        bf_mp_writer_t w; bf_mp_writer_init(&w, 256);
        assert(!bf_mp_pack_array(&w, 2) && !bf_mp_pack_int(&w, 1) && !bf_mp_pack_map(&w, 5));
        assert(!bf_mp_pack_kv_str(&w, "id", change->id));
        assert(!bf_mp_pack_kv_str(&w, "version", mode == 6 ? "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" : change->version));
        assert(!bf_mp_pack_kv_int(&w, "revision", mode == 3 ? -1 : change->revision));
        assert(!bf_mp_pack_kv_str(&w, mode == 1 ? "id" : mode == 2 ? "unknown" : "action", mode == 4 ? "cancel" : change->action));
        assert(!bf_mp_pack_str_cstr(&w, "requestId"));
        if (mode == 7) assert(!bf_mp_pack_str(&w, "request\0hidden", 14));
        else assert(!bf_mp_pack_str_cstr(&w, mode == 5 ? "short" : change->request));
        QboxChange parsed;
        assert(qbox_parse_change(bf_mp_writer_data(&w), bf_mp_writer_size(&w), &parsed) == (mode == 0));
        if (!mode) {
            assert(!strcmp(parsed.id, change->id) && !strcmp(parsed.version, change->version) && parsed.revision == change->revision);
            assert(!bf_mp_pack_nil(&w));
            assert(!qbox_parse_change(bf_mp_writer_data(&w), bf_mp_writer_size(&w), &parsed));
        }
        bf_mp_writer_free(&w);
    }
    unsigned char oversized[QBOX_INPUT_MAX + 1] = {0}; QboxChange parsed;
    assert(!qbox_parse_change(oversized, sizeof(oversized), &parsed));
}

int main(void) {
    char directory[] = "/tmp/bonfyre-account-inbox-XXXXXX"; assert(mkdtemp(directory));
    char path[256]; snprintf(path, sizeof(path), "%s/queue.db", directory);
    BfWorkgraph *graph = NULL; assert(!bf_workgraph_open(path, &graph, stderr));
    assert(bf_workgraph_register_family(graph, "queue").status == BF_WORKGRAPH_OK);
    create_task(path, graph, "account-personal", "user:7", "self");
    create_task(path, graph, "account-org", "institution:acme", "institution");
    sqlite3 *db = NULL; assert(sqlite3_open(path, &db) == SQLITE_OK);
    QboxNotice notice, other;
    assert(qact_table(db, "queue_inbox_states") == 0 && qact_table(db, "queue_inbox_operations") == 0);
    FILE *f = tmpfile(); assert(f && !qbox_list(path, "user:7", "self", "user:7", NULL, 0, f)); reply(f, 1, NULL);
    f = tmpfile(); assert(f && !qbox_update(path, "user:7", "self", "user:7", NULL, "missing-request", f)); reply(f, 0, "receipt_pending");
    assert(qact_table(db, "queue_inbox_states") == 0 && qact_table(db, "queue_inbox_operations") == 0);
    assert(qbox_notice(db, "user:7", "self", "user:8", "account-personal", &notice) == -1);
    assert(qbox_notice(db, "user:8", "self", "user:8", "account-personal", &notice) == 0);
    assert(qbox_notice(db, "institution:other", "institution", "user:7", "account-org", &notice) == 0);
    int64_t work_before = scalar(db, "SELECT COUNT(*) FROM workgraph_transitions");

    QboxChange mark = change_for(db, "user:7", "self", "user:7", "account-personal", "read", "personal-read-1");
    parser_cases(&mark);
    update(path, "user:7", "self", "user:8", &mark, 0, "unavailable");
    assert(qact_table(db, "queue_inbox_states") == 0);
    update(path, "user:7", "self", "user:7", &mark, 1, NULL);
    assert(qbox_notice(db, "user:7", "self", "user:7", mark.id, &notice) == 1 && notice.read && !notice.dismissed && notice.revision == 1);
    update(path, "user:7", "self", "user:7", &mark, 1, NULL);
    assert(scalar(db, "SELECT COUNT(*) FROM queue_inbox_operations") == 1);
    assert(qbox_notice(db, "user:7", "self", "user:7", mark.id, &notice) == 1 && notice.revision == 1);
    QboxChange conflict = mark; snprintf(conflict.action, sizeof(conflict.action), "dismiss");
    update(path, "user:7", "self", "user:7", &conflict, 0, "request_conflict");
    snprintf(conflict.request, sizeof(conflict.request), "stale-revision");
    update(path, "user:7", "self", "user:7", &conflict, 0, "version_conflict");
    assert(scalar(db, "SELECT COUNT(*) FROM queue_inbox_operations") == 1);

    QboxChange dismiss = change_for(db, "user:7", "self", "user:7", mark.id, "dismiss", "personal-dismiss-2");
    update(path, "user:7", "self", "user:7", &dismiss, 1, NULL);
    assert(qbox_notice(db, "user:7", "self", "user:7", mark.id, &notice) == 1 && notice.read && notice.dismissed && notice.revision == 2);
    QboxChange restore = change_for(db, "user:7", "self", "user:7", mark.id, "restore", "personal-restore-3");
    update(path, "user:7", "self", "user:7", &restore, 1, NULL);
    assert(qbox_notice(db, "user:7", "self", "user:7", mark.id, &notice) == 1 && notice.read && !notice.dismissed && notice.revision == 3);
    QboxChange unread = change_for(db, "user:7", "self", "user:7", mark.id, "unread", "personal-unread-4");
    update(path, "user:7", "self", "user:7", &unread, 1, NULL);
    assert(qbox_notice(db, "user:7", "self", "user:7", mark.id, &notice) == 1 && !notice.read && !notice.dismissed && notice.revision == 4);
    dismiss = change_for(db, "user:7", "self", "user:7", mark.id, "dismiss", "personal-dismiss-5");
    update(path, "user:7", "self", "user:7", &dismiss, 1, NULL);
    assert(scalar(db, "SELECT COUNT(*) FROM workgraph_transitions") == work_before);
    assert(!strcmp(bf_workgraph_status(graph, mark.id, "job").node_status, "ready"));

    /* One organization audience, separate recipient dispositions. */
    QboxChange org = change_for(db, "institution:acme", "institution", "user:7", "account-org", "dismiss", "org-user7-dismiss");
    update(path, "institution:acme", "institution", "user:7", &org, 1, NULL);
    assert(qbox_notice(db, "institution:acme", "institution", "user:7", org.id, &notice) == 1 && notice.dismissed && notice.revision == 1);
    assert(qbox_notice(db, "institution:acme", "institution", "user:8", org.id, &other) == 1 && !other.dismissed && !other.read && other.revision == 0);
    QboxChange org_other = change_for(db, "institution:acme", "institution", "user:8", org.id, "read", "org-user8-read");
    update(path, "institution:acme", "institution", "user:8", &org_other, 1, NULL);
    assert(qbox_notice(db, "institution:acme", "institution", "user:7", org.id, &notice) == 1 && !notice.read && notice.dismissed);
    assert(qbox_notice(db, "institution:acme", "institution", "user:8", org.id, &other) == 1 && other.read && !other.dismissed);

    /* Lease heartbeats refresh activity without reopening a dismissed notice.
     * A real expiry, child, worker start or retained output does reopen it. */
    create_task(path, graph, "account-heartbeat", "user:7", "self");
    BfWorkgraphClaimSpec heartbeat_claim = {"heartbeat-worker", "queue", 30000};
    BfWorkgraphResult heartbeat = bf_workgraph_claim_node(graph, "account-heartbeat", "job", &heartbeat_claim);
    assert(heartbeat.status == BF_WORKGRAPH_OK);
    QboxChange heartbeat_dismiss = change_for(db, "user:7", "self", "user:7", "account-heartbeat", "dismiss", "heartbeat-dismiss");
    update(path, "user:7", "self", "user:7", &heartbeat_dismiss, 1, NULL);
    QactTask snapshot;
    assert(qact_load(db, "user:7", "self", "account-heartbeat", &snapshot) == 1);
    char full_version[65]; memcpy(full_version, snapshot.notice.version, 65);
    assert(bf_workgraph_renew(graph, "account-heartbeat", "job", heartbeat_claim.worker_id, heartbeat.claim_token, 60000).status == BF_WORKGRAPH_OK);
    assert(qact_load(db, "user:7", "self", "account-heartbeat", &snapshot) == 1 && strcmp(full_version, snapshot.notice.version));
    assert(qbox_notice(db, "user:7", "self", "user:7", "account-heartbeat", &notice) == 1 && notice.dismissed && !strcmp(notice.task.version, heartbeat_dismiss.version));
    BfWorkgraphNodeSpec heartbeat_child = node_spec("account-heartbeat", "child"); heartbeat_child.parent_node = "job";
    assert(bf_workgraph_add_node(graph, &heartbeat_child).status == BF_WORKGRAPH_OK);
    assert(qbox_notice(db, "user:7", "self", "user:7", "account-heartbeat", &notice) == 1 && !notice.dismissed && strcmp(notice.task.version, heartbeat_dismiss.version));
    heartbeat_dismiss = change_for(db, "user:7", "self", "user:7", "account-heartbeat", "dismiss", "heartbeat-dismiss-child");
    update(path, "user:7", "self", "user:7", &heartbeat_dismiss, 1, NULL);
    BfWorkerRunSpec native_worker = {"inbox-worker-run", "research-worker", "account-heartbeat", "inbox-protocol"};
    assert(bf_worker_run_start(db, &native_worker).status == BF_LIFECYCLE_OK);
    assert(qbox_notice(db, "user:7", "self", "user:7", "account-heartbeat", &notice) == 1 && !notice.dismissed && strcmp(notice.task.version, heartbeat_dismiss.version));
    heartbeat_dismiss = change_for(db, "user:7", "self", "user:7", "account-heartbeat", "dismiss", "heartbeat-dismiss-worker");
    update(path, "user:7", "self", "user:7", &heartbeat_dismiss, 1, NULL);
    assert(bf_workgraph_complete(graph, "account-heartbeat", "job", heartbeat_claim.worker_id, heartbeat.claim_token, "file:///private/retained-output").status == BF_WORKGRAPH_OK);
    assert(qbox_notice(db, "user:7", "self", "user:7", "account-heartbeat", &notice) == 1 && !notice.dismissed && strcmp(notice.task.version, heartbeat_dismiss.version));
    assert(bf_workgraph_claim_node(graph, "account-heartbeat", "child", &heartbeat_claim).status == BF_WORKGRAPH_OK);
    heartbeat_dismiss = change_for(db, "user:7", "self", "user:7", "account-heartbeat", "dismiss", "heartbeat-before-expiry");
    update(path, "user:7", "self", "user:7", &heartbeat_dismiss, 1, NULL);
    /* Controlled expiry fixture: leave native state/timestamp unchanged, move
     * only the already-issued lease deadline into the past. No reaper runs. */
    assert(sqlite3_exec(db, "UPDATE workgraph_nodes SET lease_expires_at_ms=1 WHERE mission_id='account-heartbeat' AND node_id='child'", NULL, NULL, NULL) == SQLITE_OK);
    assert(qbox_notice(db, "user:7", "self", "user:7", "account-heartbeat", &notice) == 1 && !notice.dismissed && !strcmp(notice.task.phase, "attention") && strcmp(notice.task.version, heartbeat_dismiss.version));

    /* A native completion produces a fresh notice, while the old request's
     * receipt remains replayable without applying that dismissal again. */
    QboxChange stale = change_for(db, "user:7", "self", "user:7", mark.id, "read", "stale-source-version");
    BfWorkgraphClaimSpec claim = {"fixture-worker", "queue", 30000};
    BfWorkgraphResult active = bf_workgraph_claim_node(graph, mark.id, "job", &claim); assert(active.status == BF_WORKGRAPH_OK);
    assert(bf_workgraph_complete(graph, mark.id, "job", claim.worker_id, active.claim_token, "file:///private/inbox-result").status == BF_WORKGRAPH_OK);
    assert(qbox_notice(db, "user:7", "self", "user:7", mark.id, &notice) == 1 && !notice.read && !notice.dismissed && notice.revision == 5 && strcmp(stale.version, notice.task.version));
    int64_t operations = scalar(db, "SELECT COUNT(*) FROM queue_inbox_operations");
    update(path, "user:7", "self", "user:7", &stale, 0, "version_conflict");
    assert(scalar(db, "SELECT COUNT(*) FROM queue_inbox_operations") == operations);
    update(path, "user:7", "self", "user:7", &dismiss, 1, NULL);
    assert(qbox_notice(db, "user:7", "self", "user:7", mark.id, &notice) == 1 && !notice.dismissed && notice.revision == 5);
    f = tmpfile(); assert(f && !qbox_update(path, "user:7", "self", "user:7", NULL, dismiss.request, f)); reply(f, 1, NULL);
    QboxReceipt receipt; assert(qbox_receipt(db, "user:7", "self", "user:7", dismiss.request, &receipt) == 1 && !strcmp(receipt.version, dismiss.version) && receipt.applied == 5);
    assert(qbox_receipt(db, "user:7", "self", "user:8", dismiss.request, &receipt) == 0);

    /* Concurrent delivery of an identical operation commits one state change. */
    QboxChange concurrent = change_for(db, "institution:acme", "institution", "user:8", org.id, "dismiss", "concurrent-exact-request");
    sqlite3_close(db); bf_workgraph_close(graph); db = NULL; graph = NULL;
    int barrier[2]; assert(!pipe(barrier)); pid_t children[2];
    for (int i = 0; i < 2; i++) {
        children[i] = fork(); assert(children[i] >= 0);
        if (!children[i]) {
            close(barrier[1]); char ready; assert(read(barrier[0], &ready, 1) == 1); close(barrier[0]);
            update(path, "institution:acme", "institution", "user:8", &concurrent, 1, NULL);
            _exit(0);
        }
    }
    close(barrier[0]); assert(write(barrier[1], "go", 2) == 2); close(barrier[1]);
    for (int i = 0; i < 2; i++) { int status; assert(waitpid(children[i], &status, 0) == children[i] && WIFEXITED(status) && WEXITSTATUS(status) == 0); }
    assert(sqlite3_open(path, &db) == SQLITE_OK);
    assert(qbox_notice(db, "institution:acme", "institution", "user:8", org.id, &notice) == 1 && notice.dismissed && notice.read && notice.revision == 2);
    assert(scalar(db, "SELECT COUNT(*) FROM queue_inbox_operations WHERE request_id='concurrent-exact-request'") == 1);
    sqlite3_close(db); unlink(path); rmdir(directory);
    puts("account inbox: recipient isolation, durable dispositions, exact CAS/replay, concurrent delivery and strict packets passed");
    return 0;
}
