#ifndef BONFYRE_QUEUE_ACCOUNT_ACTIVITY_H
#define BONFYRE_QUEUE_ACCOUNT_ACTIVITY_H

/* Queue owns admission; WorkGraph and WorkerRun retain execution truth. The HTTP
 * boundary must derive subject/scope from current Auth grants on every call.
 * These helpers neither authorize browser input nor dispatch work. */
#include "bonfyre.h"
#include "bf_msgpack.h"
#include "bf_utf8.h"
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define QACT_PAGE 20
#define QACT_NODE_LIMIT 32
#define QACT_WORKER_LIMIT 16
#define QACT_OUTPUT_LIMIT 32
#define QACT_SCAN_NODES 4096
#define QACT_SCAN_WORKERS 512
#define QACT_PACKET_LIMIT (512u * 1024u)
#define QACT_MAX_TIME INT64_C(8640000000000000)

typedef struct {
    const char *mission, *subject, *scope, *title, *continuation;
    const char *operator_id, *input_uri, *family;
} QactScope;

typedef struct {
    char id[128], title[161], attention[241], version[65], phase[16], continuation[513];
    int64_t created, updated, sequence;
} QactNotice;

typedef struct {
    char id[128], parent[128], operator_id[128], family[128], state[32], phase[16];
    int64_t attempt, updated;
    int has_output;
} QactNode;

typedef struct {
    char id[128], worker[128], state[32], phase[16];
    int64_t created, updated;
} QactWorker;

typedef struct {
    char node[128], label[161], reference[65];
    int64_t updated;
} QactOutput;

typedef struct {
    /* Activity reads use the full snapshot; recipient dispositions use the
     * semantic version so lease heartbeats do not reopen a dismissed notice. */
    QactNotice notice;
    char notice_version[65];
    char state[32];
    QactNode nodes[QACT_NODE_LIMIT];
    QactWorker workers[QACT_WORKER_LIMIT];
    QactOutput outputs[QACT_OUTPUT_LIMIT];
    size_t node_count, worker_count, output_count;
    int64_t total_nodes, total_workers, active_workers, total_outputs, phases[5];
    /* Derived account work coverage; the generic Queue packet stays unchanged. */
    int has_account_usage; int64_t usage_runs, usage_reported_runs, usage_input_tokens, usage_output_tokens, usage_priced_runs, usage_estimated_micro_usd;
} QactTask;

static inline int64_t qact_now(void) {
    struct timespec ts;
    return clock_gettime(CLOCK_REALTIME, &ts) == 0 ? (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000 : 0;
}

static inline int qact_text(const char *s, size_t max, int empty) {
    size_t n = s ? strlen(s) : 0;
    if (!s || n > max || (!empty && !n) || !bf_utf8_valid(s, n, NULL)) return 0;
    for (size_t i = 0; i < n; i++) if ((unsigned char)s[i] < 32 || (unsigned char)s[i] == 127) return 0;
    return 1;
}

static inline int qact_atom(const char *s, size_t max) {
    if (!s || !*s || strlen(s) > max) return 0;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++)
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
              (*p >= '0' && *p <= '9') || strchr("._:-", *p))) return 0;
    return 1;
}

static inline int qact_scope_valid(const char *subject, const char *scope) {
    if (!subject || !scope) return 0;
    if (!strcmp(scope, "self")) {
        if (strncmp(subject, "user:", 5) || subject[5] < '1' || subject[5] > '9' || strlen(subject) > 24) return 0;
        int64_t value = 0;
        for (const char *p = subject + 5; *p; p++) {
            if (*p < '0' || *p > '9' || value > (INT64_MAX - (*p - '0')) / 10) return 0;
            value = value * 10 + (*p - '0');
        }
        return 1;
    }
    if (strcmp(scope, "institution") && strcmp(scope, "household")) return 0;
    size_t n = strlen(scope);
    if (strncmp(subject, scope, n) || subject[n] != ':' || !subject[n + 1] || strlen(subject + n + 1) > 63) return 0;
    for (const char *p = subject + n + 1; *p; p++) if (!((*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9') || *p == '-')) return 0;
    return 1;
}

static inline int qact_continuation(const char *s, const char *subject, const char *scope) {
    if (!qact_text(s, 512, 1)) return 0;
    if (!*s) return 1;
    const char *query = strchr(s, '?');
    size_t n = query ? (size_t)(query - s) : strlen(s);
    if (!((n == 5 && !strncmp(s, "/apps", n)) ||
          (n == 12 && !strncmp(s, "/sources/web", n)) || (n > 6 && !strncmp(s, "/apps/", 6)))) return 0;
    if (s[n - 1] == '/') return 0;
    for (size_t i = 1; i < n; i++) if (!((s[i] >= 'a' && s[i] <= 'z') || (s[i] >= '0' && s[i] <= '9') || s[i] == '-' || s[i] == '/' || s[i] == '_')) return 0;
    if (strstr(s, "//")) return 0;
    if (!query) return !strcmp(scope, "self");
    return !strncmp(query, "?context=", 9) && !strcmp(query + 9, !strcmp(scope, "self") ? "self" : subject);
}

static inline int qact_table(sqlite3 *db, const char *name) {
    sqlite3_stmt *q = NULL;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(q, 1, name, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(q), found = rc == SQLITE_ROW ? 1 : rc == SQLITE_DONE ? 0 : -1;
    sqlite3_finalize(q);
    return found;
}

/* Length-prefixed MessagePack cells make the digest independent of delimiters,
 * pointer identity, host endianness and SQLite's presentation of SQL rows. */
static inline int qact_hash_cells(BfSha256 *hash, sqlite3_stmt *q, const unsigned *indices, size_t selected) {
    bf_mp_writer_t w;
    bf_mp_writer_init(&w, 128);
    int columns = indices ? (int)selected : sqlite3_column_count(q), ok = bf_mp_pack_array(&w, (uint32_t)columns) == 0;
    for (int i = 0; ok && i < columns; i++) {
        int col = indices ? (int)indices[i] : i;
        int type = sqlite3_column_type(q, col);
        if (type == SQLITE_NULL) ok = bf_mp_pack_nil(&w) == 0;
        else if (type == SQLITE_INTEGER) ok = bf_mp_pack_int(&w, sqlite3_column_int64(q, col)) == 0;
        else if (type == SQLITE_TEXT) {
            int n = sqlite3_column_bytes(q, col);
            ok = n >= 0 && n <= 8192 && bf_mp_pack_str(&w, (const char *)sqlite3_column_text(q, col), (uint32_t)n) == 0;
        } else ok = 0;
    }
    if (ok) bf_sha256_update(hash, bf_mp_writer_data(&w), bf_mp_writer_size(&w));
    bf_mp_writer_free(&w);
    return ok;
}

static inline int qact_hash_row(BfSha256 *hash, sqlite3_stmt *q) {
    return qact_hash_cells(hash, q, NULL, 0);
}

static inline int qact_root_hash(const char *operator_id, const char *input, const char *family, char out[65]) {
    if (!qact_text(operator_id, 127, 0) || !input || strlen(input) > 8192 || !qact_text(family, 127, 0)) return 0;
    bf_mp_writer_t w;
    bf_mp_writer_init(&w, 128);
    int ok = !bf_mp_pack_array(&w, 3) && !bf_mp_pack_str_cstr(&w, operator_id) &&
        !bf_mp_pack_str_cstr(&w, input) && !bf_mp_pack_str_cstr(&w, family);
    if (ok) bf_sha256_hex(bf_mp_writer_data(&w), bf_mp_writer_size(&w), out);
    bf_mp_writer_free(&w);
    return ok;
}

static inline int qact_schema(sqlite3 *db) {
    return sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS queue_activity_scopes("
        "sequence INTEGER PRIMARY KEY AUTOINCREMENT,mission_id TEXT NOT NULL UNIQUE,"
        "subject TEXT NOT NULL,scope TEXT NOT NULL,title TEXT NOT NULL,continuation TEXT NOT NULL,"
        "root_sha256 TEXT NOT NULL,created_at_ms INTEGER NOT NULL);"
        "CREATE INDEX IF NOT EXISTS queue_activity_scope_page ON queue_activity_scopes(subject,scope,sequence DESC);"
        "CREATE TRIGGER IF NOT EXISTS queue_activity_scope_immutable_update BEFORE UPDATE ON queue_activity_scopes "
        "BEGIN SELECT RAISE(ABORT,'queue activity admission is immutable'); END;"
        "CREATE TRIGGER IF NOT EXISTS queue_activity_scope_immutable_delete BEFORE DELETE ON queue_activity_scopes "
        "BEGIN SELECT RAISE(ABORT,'queue activity admission is immutable'); END;", NULL, NULL, NULL) == SQLITE_OK;
}

/* Reserve before creating the native mission. A crash leaves an invisible
 * reservation that the identical trusted enqueue can resume. Never adopt an
 * existing unscoped mission. The account- namespace is a public-reader exclusion
 * guard; authority still requires the full audience/root binding below.
 * Returns 1 admitted/replayed, 0 conflict, -1 storage. */
static inline int qact_reserve(const char *path, const QactScope *s) {
    char digest[65];
    if (!s || !qact_atom(s->mission, 127) || strncmp(s->mission, "account-", 8) || !qact_scope_valid(s->subject, s->scope) ||
        !qact_text(s->title, 160, 0) || strspn(s->title, " ") == strlen(s->title) ||
        !qact_continuation(s->continuation, s->subject, s->scope) ||
        !qact_root_hash(s->operator_id, s->input_uri, s->family, digest)) return 0;
    sqlite3 *db = NULL;
    sqlite3_stmt *q = NULL;
    int answer = -1, rc;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK) goto done;
    sqlite3_busy_timeout(db, 2500);
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK || !qact_schema(db)) goto done;
    if (sqlite3_prepare_v2(db, "SELECT subject,scope,title,continuation,root_sha256 FROM queue_activity_scopes WHERE mission_id=?", -1, &q, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(q, 1, s->mission, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(q);
    if (rc == SQLITE_ROW) {
        const char *values[] = {s->subject, s->scope, s->title, s->continuation, digest};
        answer = 1;
        for (int i = 0; i < 5; i++) if (sqlite3_column_type(q, i) != SQLITE_TEXT || strcmp((const char *)sqlite3_column_text(q, i), values[i])) answer = 0;
        goto finish;
    }
    if (rc != SQLITE_DONE) goto done;
    sqlite3_finalize(q); q = NULL;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM missions WHERE id=? UNION ALL SELECT 1 FROM workgraph_nodes WHERE mission_id=? LIMIT 1", -1, &q, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(q, 1, s->mission, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(q, 2, s->mission, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(q);
    if (rc == SQLITE_ROW) { answer = 0; goto finish; }
    if (rc != SQLITE_DONE) goto done;
    sqlite3_finalize(q); q = NULL;
    if (sqlite3_prepare_v2(db, "INSERT INTO queue_activity_scopes(mission_id,subject,scope,title,continuation,root_sha256,created_at_ms) VALUES(?,?,?,?,?,?,?)", -1, &q, NULL) != SQLITE_OK) goto done;
    const char *values[] = {s->mission, s->subject, s->scope, s->title, s->continuation, digest};
    for (int i = 0; i < 6; i++) sqlite3_bind_text(q, i + 1, values[i], -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(q, 7, qact_now());
    if (sqlite3_step(q) != SQLITE_DONE) goto done;
    answer = 1;
finish:
    sqlite3_finalize(q); q = NULL;
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) answer = -1;
done:
    sqlite3_finalize(q);
    if (db) { if (!sqlite3_get_autocommit(db)) sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); sqlite3_close(db); }
    return answer;
}

static inline int qact_cell(sqlite3_stmt *q, int column, char *out, size_t cap, int empty) {
    const char *s = (const char *)sqlite3_column_text(q, column);
    int n = sqlite3_column_bytes(q, column);
    if (!s || n < 0 || (size_t)n >= cap || strlen(s) != (size_t)n || !qact_text(s, cap - 1, empty)) return 0;
    memcpy(out, s, (size_t)n + 1);
    return 1;
}

static inline int qact_time(sqlite3_stmt *q, int col, int64_t *out) {
    *out = sqlite3_column_int64(q, col);
    return sqlite3_column_type(q, col) == SQLITE_INTEGER && *out >= 0 && *out <= QACT_MAX_TIME;
}

static inline int qact_phase(const char *state, int worker, int expired) {
    if (!strcmp(state, "running")) return expired ? 2 : 1;
    if (!strcmp(state, worker ? "completed" : "complete")) return 3;
    if (!strcmp(state, worker ? "failed" : "dead_letter")) return 2;
    if (worker) return !strcmp(state, "waiting") ? 0 : -1;
    if (!strcmp(state, "cancelled")) return 4;
    if (!strcmp(state, "cancel_requested")) return 2;
    if (!strcmp(state, "ready") || !strcmp(state, "defined") || !strcmp(state, "blocked") || !strcmp(state, "retry_wait")) return 0;
    return -1;
}

static inline const char *qact_phase_name(int phase) {
    static const char *names[] = {"pending", "active", "attention", "completed", "cancelled"};
    return phase >= 0 && phase < 5 ? names[phase] : "attention";
}

/* Caller owns the SQLite transaction. Returns 1 for an exact authorized native
 * mission, 0 for absent/invisible work, -1 for malformed or unavailable storage.
 * Scans are bounded independently from the smaller arrays sent to the browser. */
static inline int qact_load(sqlite3 *db, const char *subject, const char *scope, const char *mission, QactTask *task) {
    sqlite3_stmt *q = NULL;
    int answer = -1, rc, presence;
    BfSha256 hash, semantic;
    static const unsigned admission_cells[] = {0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11};
    static const unsigned node_cells[] = {0, 1, 2, 3, 4, 5, 7};
    static const unsigned worker_cells[] = {0, 1, 2, 5, 6};
    char expected[65], actual[65], root_operator[128], root_family[128];
    int64_t now = qact_now(), combined[5] = {0};
    memset(task, 0, sizeof(*task));
    if (!qact_scope_valid(subject, scope) || !qact_atom(mission, 127)) return -1;
    if (strncmp(mission, "account-", 8)) return 0;
    presence = qact_table(db, "queue_activity_scopes");
    if (presence <= 0) return presence;
    if (sqlite3_prepare_v2(db,
        "SELECT a.sequence,a.mission_id,a.title,a.continuation,a.created_at_ms,a.root_sha256,"
        "n.operator_id,COALESCE(n.input_uri,''),COALESCE(n.family,''),n.status,a.subject,a.scope "
        "FROM queue_activity_scopes a JOIN missions m ON m.id=a.mission_id "
        "JOIN workgraph_nodes n ON n.mission_id=a.mission_id AND n.node_id='job' "
        "WHERE a.subject=? AND a.scope=? AND a.mission_id=?", -1, &q, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(q, 2, scope, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(q, 3, mission, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(q);
    if (rc == SQLITE_DONE) { answer = 0; goto done; }
    if (rc != SQLITE_ROW || !qact_time(q, 0, &task->notice.sequence) || task->notice.sequence < 1 ||
        !qact_cell(q, 1, task->notice.id, sizeof(task->notice.id), 0) ||
        !qact_cell(q, 2, task->notice.title, sizeof(task->notice.title), 0) ||
        !qact_cell(q, 3, task->notice.continuation, sizeof(task->notice.continuation), 1) ||
        !qact_continuation(task->notice.continuation, subject, scope) ||
        !qact_time(q, 4, &task->notice.created) || !qact_cell(q, 5, expected, sizeof(expected), 0) ||
        !qact_cell(q, 6, root_operator, sizeof(root_operator), 0) ||
        !qact_cell(q, 8, root_family, sizeof(root_family), 0) ||
        !qact_cell(q, 9, task->state, sizeof(task->state), 0)) goto done;
    const char *input = (const char *)sqlite3_column_text(q, 7);
    if (!input || sqlite3_column_bytes(q, 7) != (int)strlen(input) || !qact_root_hash(root_operator, input, root_family, actual) || strcmp(actual, expected)) goto done;
    task->notice.updated = task->notice.created;
    bf_sha256_init(&hash);
    bf_sha256_update(&hash, (const uint8_t *)"queue-account-task-v1", 21);
    bf_sha256_init(&semantic);
    bf_sha256_update(&semantic, (const uint8_t *)"queue-account-notice-v1", 23);
    if (!qact_hash_row(&hash, q) || !qact_hash_cells(&semantic, q, admission_cells, 11)) goto done;
    sqlite3_finalize(q); q = NULL;
    if (sqlite3_prepare_v2(db,
        "SELECT node_id,COALESCE(parent_node,''),operator_id,COALESCE(family,''),status,attempt,updated_at_ms,"
        "COALESCE(output_uri,''),COALESCE(lease_expires_at_ms,0),created_at_ms "
        "FROM workgraph_nodes WHERE mission_id=? ORDER BY CASE WHEN node_id='job' THEN 0 ELSE 1 END,node_id LIMIT 4097",
        -1, &q, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(q, 1, mission, -1, SQLITE_TRANSIENT);
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
        QactNode node = {0};
        int64_t created = 0;
        if (++task->total_nodes > QACT_SCAN_NODES || !qact_hash_row(&hash, q) || !qact_hash_cells(&semantic, q, node_cells, 7) ||
            !qact_cell(q, 0, node.id, sizeof(node.id), 0) || !qact_cell(q, 1, node.parent, sizeof(node.parent), 1) ||
            !qact_cell(q, 2, node.operator_id, sizeof(node.operator_id), 0) || !qact_cell(q, 3, node.family, sizeof(node.family), 1) ||
            !qact_cell(q, 4, node.state, sizeof(node.state), 0) || !qact_time(q, 5, &node.attempt) ||
            !qact_time(q, 6, &node.updated) || !qact_time(q, 9, &created) || node.updated < created) goto done;
        int phase = qact_phase(node.state, 0, sqlite3_column_int64(q, 8) <= now);
        if (phase < 0) goto done;
        uint8_t phase_byte = (uint8_t)phase;
        bf_sha256_update(&hash, &phase_byte, 1);
        bf_sha256_update(&semantic, &phase_byte, 1);
        snprintf(node.phase, sizeof(node.phase), "%s", qact_phase_name(phase));
        task->phases[phase]++; combined[phase]++;
        if (node.updated > task->notice.updated) task->notice.updated = node.updated;
        const char *output = (const char *)sqlite3_column_text(q, 7);
        node.has_output = output && *output && !strcmp(node.state, "complete");
        if (node.has_output) {
            task->total_outputs++;
            if (task->output_count < QACT_OUTPUT_LIMIT) {
                QactOutput *o = &task->outputs[task->output_count++];
                snprintf(o->node, sizeof(o->node), "%s", node.id);
                snprintf(o->label, sizeof(o->label), "%s output", node.operator_id);
                bf_sha256_hex((const uint8_t *)output, (size_t)sqlite3_column_bytes(q, 7), o->reference);
                o->updated = node.updated;
            }
        }
        if (task->node_count < QACT_NODE_LIMIT) task->nodes[task->node_count++] = node;
    }
    if (rc != SQLITE_DONE || !task->total_nodes) goto done;
    sqlite3_finalize(q); q = NULL;
    /* Dependency rows participate in the version even when neither endpoint is
     * among the first visible nodes. Their strings are never rendered as links. */
    if (sqlite3_prepare_v2(db, "SELECT node_id,depends_on_node_id,dependency_policy FROM workgraph_dependencies WHERE mission_id=? ORDER BY node_id,depends_on_node_id LIMIT 8193", -1, &q, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(q, 1, mission, -1, SQLITE_TRANSIENT);
    int dependencies = 0;
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) if (++dependencies > 8192 || !qact_hash_row(&hash, q) || !qact_hash_row(&semantic, q)) goto done;
    if (rc != SQLITE_DONE) goto done;
    sqlite3_finalize(q); q = NULL;
    presence = qact_table(db, "worker_runs");
    if (presence < 0) goto done;
    if (presence) {
        if (sqlite3_prepare_v2(db, "SELECT run_id,worker_id,state,created_at_ms,updated_at_ms,protocol_instance_id,terminal_receipt_ref FROM worker_runs WHERE mission_id=? ORDER BY created_at_ms DESC,run_id LIMIT 513", -1, &q, NULL) != SQLITE_OK) goto done;
        sqlite3_bind_text(q, 1, mission, -1, SQLITE_TRANSIENT);
        while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
            QactWorker worker = {0};
            if (++task->total_workers > QACT_SCAN_WORKERS || !qact_hash_row(&hash, q) || !qact_hash_cells(&semantic, q, worker_cells, 5) ||
                !qact_cell(q, 0, worker.id, sizeof(worker.id), 0) || !qact_cell(q, 1, worker.worker, sizeof(worker.worker), 0) ||
                !qact_cell(q, 2, worker.state, sizeof(worker.state), 0) || !qact_time(q, 3, &worker.created) ||
                !qact_time(q, 4, &worker.updated) || worker.updated < worker.created) goto done;
            int phase = qact_phase(worker.state, 1, 0);
            if (phase < 0) goto done;
            snprintf(worker.phase, sizeof(worker.phase), "%s", qact_phase_name(phase));
            combined[phase]++;
            if (phase == 1) task->active_workers++;
            if (worker.updated > task->notice.updated) task->notice.updated = worker.updated;
            if (task->worker_count < QACT_WORKER_LIMIT) task->workers[task->worker_count++] = worker;
        }
        if (rc != SQLITE_DONE) goto done;
    }
    int phase = combined[2] ? 2 : combined[1] ? 1 : combined[0] ? 0 : combined[4] ? 4 : 3;
    snprintf(task->notice.phase, sizeof(task->notice.phase), "%s", qact_phase_name(phase));
    uint8_t digest[32];
    bf_sha256_final(&hash, digest);
    bf_sha256_digest_hex(digest, task->notice.version);
    bf_sha256_final(&semantic, digest);
    bf_sha256_digest_hex(digest, task->notice_version);
    answer = 1;
done:
    sqlite3_finalize(q);
    return answer;
}

static inline int qact_notice(sqlite3 *db, const char *subject, const char *scope, const char *mission, QactNotice *notice) {
    QactTask *task = calloc(1, sizeof(*task));
    if (!task) return -1;
    int rc = qact_load(db, subject, scope, mission, task);
    if (rc == 1) { *notice = task->notice; memcpy(notice->version, task->notice_version, 65); }
    free(task);
    return rc;
}

static inline int qact_version(sqlite3 *db, const char *subject, const char *scope, const char *mission, char out[65]) {
    QactNotice notice;
    int rc = qact_notice(db, subject, scope, mission, &notice);
    out[0] = '\0';
    if (rc == 1) memcpy(out, notice.version, 65);
    return rc;
}

/* One stable admission page, including one lookahead for a 50-notice Inbox.
 * An orphan reservation has no native root job and is not listed. */
static inline int qact_notice_ids(sqlite3 *db, const char *subject, const char *scope, int64_t cursor,
                                  char ids[51][128], int64_t sequences[51], size_t *count) {
    sqlite3_stmt *q = NULL;
    *count = 0;
    if (!qact_scope_valid(subject, scope) || cursor < 0 || cursor > QACT_MAX_TIME) return -1;
    int exists = qact_table(db, "queue_activity_scopes");
    if (exists <= 0) return exists < 0 ? -1 : 1;
    if (sqlite3_prepare_v2(db,
        "SELECT a.mission_id,a.sequence FROM queue_activity_scopes a "
        "JOIN missions m ON m.id=a.mission_id JOIN workgraph_nodes n ON n.mission_id=a.mission_id AND n.node_id='job' "
        "WHERE a.subject=? AND a.scope=? AND a.sequence<? ORDER BY a.sequence DESC LIMIT 51", -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(q, 2, scope, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(q, 3, cursor ? cursor : INT64_MAX);
    int rc;
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
        size_t i = *count;
        if (i >= 51 || !qact_cell(q, 0, ids[i], 128, 0) || !qact_atom(ids[i], 127) ||
            !qact_time(q, 1, &sequences[i]) || sequences[i] < 1) { sqlite3_finalize(q); return -1; }
        (*count)++;
    }
    sqlite3_finalize(q);
    return rc == SQLITE_DONE ? 1 : -1;
}

#define QACT_MP(call) do { if ((call) != 0) return 0; } while (0)
static inline int qact_pack_task(bf_mp_writer_t *w, const QactTask *t) {
    const QactNotice *n = &t->notice;
    QACT_MP(bf_mp_pack_map(w, t->has_account_usage ? 17 : 16));
    QACT_MP(bf_mp_pack_kv_str(w, "id", n->id)); QACT_MP(bf_mp_pack_kv_int(w, "seq", n->sequence));
    QACT_MP(bf_mp_pack_kv_str(w, "title", n->title)); QACT_MP(bf_mp_pack_kv_str(w, "state", t->state));
    QACT_MP(bf_mp_pack_kv_str(w, "phase", n->phase)); QACT_MP(bf_mp_pack_kv_str(w, "version", n->version));
    QACT_MP(bf_mp_pack_kv_int(w, "created", n->created)); QACT_MP(bf_mp_pack_kv_int(w, "updated", n->updated));
    QACT_MP(bf_mp_pack_kv_str(w, "continuation", n->continuation));
    QACT_MP(bf_mp_pack_str_cstr(w, "nodes")); QACT_MP(bf_mp_pack_array(w, (uint32_t)t->node_count));
    for (size_t i = 0; i < t->node_count; i++) {
        const QactNode *v = &t->nodes[i];
        QACT_MP(bf_mp_pack_map(w, 9)); QACT_MP(bf_mp_pack_kv_str(w, "id", v->id));
        QACT_MP(bf_mp_pack_str_cstr(w, "parentId"));
        if (*v->parent) QACT_MP(bf_mp_pack_str_cstr(w, v->parent)); else QACT_MP(bf_mp_pack_nil(w));
        QACT_MP(bf_mp_pack_kv_str(w, "operatorId", v->operator_id)); QACT_MP(bf_mp_pack_kv_str(w, "family", v->family));
        QACT_MP(bf_mp_pack_kv_str(w, "state", v->state)); QACT_MP(bf_mp_pack_kv_str(w, "phase", v->phase));
        QACT_MP(bf_mp_pack_kv_int(w, "attempt", v->attempt)); QACT_MP(bf_mp_pack_kv_int(w, "updated", v->updated));
        QACT_MP(bf_mp_pack_kv_bool(w, "hasOutput", v->has_output));
    }
    QACT_MP(bf_mp_pack_str_cstr(w, "workers")); QACT_MP(bf_mp_pack_array(w, (uint32_t)t->worker_count));
    for (size_t i = 0; i < t->worker_count; i++) {
        const QactWorker *v = &t->workers[i];
        QACT_MP(bf_mp_pack_map(w, 6)); QACT_MP(bf_mp_pack_kv_str(w, "id", v->id));
        QACT_MP(bf_mp_pack_kv_str(w, "workerId", v->worker)); QACT_MP(bf_mp_pack_kv_str(w, "state", v->state));
        QACT_MP(bf_mp_pack_kv_str(w, "phase", v->phase)); QACT_MP(bf_mp_pack_kv_int(w, "created", v->created));
        QACT_MP(bf_mp_pack_kv_int(w, "updated", v->updated));
    }
    QACT_MP(bf_mp_pack_str_cstr(w, "outputs")); QACT_MP(bf_mp_pack_array(w, (uint32_t)t->output_count));
    for (size_t i = 0; i < t->output_count; i++) {
        const QactOutput *v = &t->outputs[i];
        QACT_MP(bf_mp_pack_map(w, 6)); QACT_MP(bf_mp_pack_kv_str(w, "id", v->node));
        QACT_MP(bf_mp_pack_kv_str(w, "nodeId", v->node)); QACT_MP(bf_mp_pack_kv_str(w, "label", v->label));
        QACT_MP(bf_mp_pack_kv_str(w, "kind", "declared-output")); QACT_MP(bf_mp_pack_kv_str(w, "reference", v->reference));
        QACT_MP(bf_mp_pack_kv_int(w, "updated", v->updated));
    }
    QACT_MP(bf_mp_pack_str_cstr(w, "counts")); QACT_MP(bf_mp_pack_map(w, 9));
    QACT_MP(bf_mp_pack_kv_int(w, "nodes", t->total_nodes)); QACT_MP(bf_mp_pack_kv_int(w, "workers", t->total_workers));
    QACT_MP(bf_mp_pack_kv_int(w, "activeWorkers", t->active_workers));
    QACT_MP(bf_mp_pack_kv_int(w, "outputs", t->total_outputs));
    for (int i = 0; i < 5; i++) QACT_MP(bf_mp_pack_kv_int(w, qact_phase_name(i), t->phases[i]));
    QACT_MP(bf_mp_pack_kv_bool(w, "nodesMore", t->total_nodes > (int64_t)t->node_count));
    if (t->has_account_usage) {
        QACT_MP(bf_mp_pack_str_cstr(w, "usage")); QACT_MP(bf_mp_pack_map(w, 7));
        QACT_MP(bf_mp_pack_kv_int(w, "runs", t->usage_runs));
        QACT_MP(bf_mp_pack_kv_int(w, "reportedRuns", t->usage_reported_runs));
        QACT_MP(bf_mp_pack_kv_str(w, "costStatus", t->usage_priced_runs ? "estimate" : "unknown"));
        QACT_MP(bf_mp_pack_kv_int(w, "pricedRuns", t->usage_priced_runs));
        QACT_MP(bf_mp_pack_str_cstr(w, "estimatedMicroUsd"));
        if (t->usage_priced_runs) QACT_MP(bf_mp_pack_int(w, t->usage_estimated_micro_usd)); else QACT_MP(bf_mp_pack_nil(w));
        QACT_MP(bf_mp_pack_str_cstr(w, "inputTokens"));
        if (t->usage_reported_runs) QACT_MP(bf_mp_pack_int(w, t->usage_input_tokens)); else QACT_MP(bf_mp_pack_nil(w));
        QACT_MP(bf_mp_pack_str_cstr(w, "outputTokens"));
        if (t->usage_reported_runs) QACT_MP(bf_mp_pack_int(w, t->usage_output_tokens)); else QACT_MP(bf_mp_pack_nil(w));
    }
    QACT_MP(bf_mp_pack_kv_bool(w, "workersMore", t->total_workers > (int64_t)t->worker_count));
    QACT_MP(bf_mp_pack_kv_bool(w, "outputsMore", t->total_outputs > (int64_t)t->output_count));
    return 1;
}
#undef QACT_MP

static inline int qact_budget(void *p) { return --*(int *)p <= 0; }

/* Emits one complete MessagePack packet; failure never emits a partial prefix.
 * Read-only open and snapshot avoid creating/migrating databases on a GET. */
static inline int qact_read(const char *path, const char *subject, const char *scope,
                             const char *mission, int64_t cursor, FILE *out) {
    sqlite3 *db = NULL;
    bf_mp_writer_t w;
    bf_mp_writer_init(&w, 4096);
    char ids[51][128] = {{0}};
    int64_t sequences[51] = {0};
    size_t count = 0;
    int ok = 0, budget = 30000;
    QactTask *task = calloc(1, sizeof(*task));
    if (!task || !qact_scope_valid(subject, scope) || cursor < 0 || (mission && *mission && (!qact_atom(mission, 127) || cursor))) goto done;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) goto done;
    sqlite3_busy_timeout(db, 1000);
    sqlite3_progress_handler(db, 1000, qact_budget, &budget);
    if (sqlite3_exec(db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) goto done;
    if (mission && *mission) {
        if (qact_load(db, subject, scope, mission, task) != 1) goto done;
        snprintf(ids[0], sizeof(ids[0]), "%s", mission); count = 1;
    } else if (qact_notice_ids(db, subject, scope, cursor, ids, sequences, &count) != 1) goto done;
    int more = count > QACT_PAGE;
    if (count > QACT_PAGE) count = QACT_PAGE;
    if (bf_mp_pack_array(&w, 3) || bf_mp_pack_int(&w, 1) || bf_mp_pack_bool(&w, 1) ||
        bf_mp_pack_map(&w, 3) || bf_mp_pack_str_cstr(&w, "tasks") || bf_mp_pack_array(&w, (uint32_t)count)) goto done;
    for (size_t i = 0; i < count; i++) {
        if (qact_load(db, subject, scope, ids[i], task) != 1 || !qact_pack_task(&w, task) || bf_mp_writer_size(&w) > QACT_PACKET_LIMIT) goto done;
    }
    if (bf_mp_pack_kv_bool(&w, "more", more) || bf_mp_pack_kv_int(&w, "cursor", more ? sequences[count - 1] : 0) ||
        sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto done;
    ok = fwrite(bf_mp_writer_data(&w), 1, bf_mp_writer_size(&w), out) == bf_mp_writer_size(&w) && !ferror(out);
done:
    if (db) { sqlite3_progress_handler(db, 0, NULL, NULL); if (!sqlite3_get_autocommit(db)) sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); sqlite3_close(db); }
    bf_mp_writer_free(&w);
    free(task);
    return ok ? 0 : 1;
}
#endif
