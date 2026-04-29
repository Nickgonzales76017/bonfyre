/*
 * BonfyreQueue v2 — SQLite-backed job queue with built-in worker daemon.
 *
 * Architecture:
 *   - SQLite WAL mode for concurrent API writes + worker reads
 *   - Worker pool: N threads, each polls → claim → fork+exec → complete/fail
 *   - Event log: every state change emits a row (consumed by API SSE)
 *   - Webhooks: POST notification on job completion/failure
 *   - Retry: exponential backoff with max_retries per job
 *   - FTS5: full-text search over job results
 *
 * Commands:
 *   bonfyre-queue enqueue <type> <input> [--priority N] [--source S] [--db FILE]
 *   bonfyre-queue work [--workers N] [--db FILE]
 *   bonfyre-queue list [--status S] [--limit N] [--db FILE]
 *   bonfyre-queue stats [--db FILE]
 *   bonfyre-queue events [--since ID] [--follow] [--db FILE]
 *   bonfyre-queue webhook add|list|remove ... [--db FILE]
 *   bonfyre-queue retry <job-id> [--db FILE]
 *   bonfyre-queue purge [--older-than DAYS] [--db FILE]
 */
#define _GNU_SOURCE
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define VERSION        "2.0.0"
#define MAX_WORKERS    16
#define MAX_RESULT     (2*1024*1024)  /* 2 MB stdout capture */
#define MAX_RETRIES    3
#define POLL_BASE_MS   50
#define POLL_MAX_MS    2000
#define WEBHOOK_TIMEOUT 5  /* seconds */

static volatile int g_running = 1;
static atomic_int g_active_workers = 0;
static pthread_mutex_t g_db_mutex = PTHREAD_MUTEX_INITIALIZER;

static const char *arg_val(int argc, char **argv, const char *flag);

/* ── Timestamps ───────────────────────────────────────────────────── */

static void iso_now(char *buf, size_t sz) {
    time_t t = time(NULL);
    struct tm tm;
    gmtime_r(&t, &tm);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

static double monotonic_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* ── Database ─────────────────────────────────────────────────────── */

static const char *SCHEMA_SQL =
    "PRAGMA journal_mode=WAL;"
    "PRAGMA busy_timeout=5000;"
    "PRAGMA synchronous=NORMAL;"

    "CREATE TABLE IF NOT EXISTS jobs ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  type TEXT NOT NULL,"
    "  status TEXT NOT NULL DEFAULT 'queued',"
    "  priority INTEGER NOT NULL DEFAULT 100,"
    "  input_path TEXT,"
    "  output_path TEXT,"
    "  source TEXT DEFAULT 'cli',"
    "  api_key TEXT,"
    "  worker TEXT,"
    "  attempts INTEGER DEFAULT 0,"
    "  max_retries INTEGER DEFAULT 3,"
    "  created_at TEXT NOT NULL,"
    "  claimed_at TEXT,"
    "  completed_at TEXT,"
    "  error TEXT,"
    "  result_json TEXT,"
    "  wall_ms REAL,"
    "  exit_code INTEGER"
    ");"

    "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);"
    "CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority, id);"
    "CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(type);"

    "CREATE TABLE IF NOT EXISTS events ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  job_id INTEGER NOT NULL,"
    "  event TEXT NOT NULL,"
    "  data TEXT,"
    "  created_at TEXT NOT NULL,"
    "  FOREIGN KEY(job_id) REFERENCES jobs(id)"
    ");"

    "CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id);"

    "CREATE TABLE IF NOT EXISTS webhooks ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  url TEXT NOT NULL,"
    "  events TEXT DEFAULT 'completed,failed',"
    "  active INTEGER DEFAULT 1,"
    "  created_at TEXT NOT NULL"
    ");"

    /* FTS5 virtual table for full-text search over job results */
    "CREATE VIRTUAL TABLE IF NOT EXISTS jobs_fts USING fts5("
    "  type, source, result_json, error,"
    "  content=jobs, content_rowid=id"
    ");";

static sqlite3 *open_db(const char *path) {
    sqlite3 *db;
    if (sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "[queue] cannot open %s: %s\n", path, sqlite3_errmsg(db));
        return NULL;
    }
    char *err = NULL;
    if (sqlite3_exec(db, SCHEMA_SQL, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "[queue] schema error: %s\n", err);
        sqlite3_free(err);
        sqlite3_close(db);
        return NULL;
    }
    return db;
}

static const char *default_db_path(void) {
    static char p[2048];
    const char *h = getenv("HOME");
    snprintf(p, sizeof(p), "%s/.local/share/bonfyre/queue.db", h ? h : ".");
    /* Ensure parent directory */
    char d[2048];
    snprintf(d, sizeof(d), "%s/.local/share/bonfyre", h ? h : ".");
    mkdir(d, 0755);
    return p;
}

/* ── Event emission ───────────────────────────────────────────────── */

static void emit_event(sqlite3 *db, int job_id, const char *event, const char *data) {
    char ts[64];
    iso_now(ts, sizeof(ts));
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "INSERT INTO events (job_id, event, data, created_at) VALUES (?,?,?,?)",
        -1, &st, NULL);
    sqlite3_bind_int(st, 1, job_id);
    sqlite3_bind_text(st, 2, event, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, data ? data : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, ts, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
}

/* ── FTS sync (update search index after job completion) ──────────── */

static void fts_sync_job(sqlite3 *db, int job_id) {
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    /* Delete old entry */
    sqlite3_prepare_v2(db,
        "INSERT INTO jobs_fts(jobs_fts, rowid, type, source, result_json, error) "
        "SELECT 'delete', id, type, source, COALESCE(result_json,''), COALESCE(error,'') "
        "FROM jobs WHERE id=?", -1, &st, NULL);
    sqlite3_bind_int(st, 1, job_id);
    sqlite3_step(st);
    sqlite3_finalize(st);
    /* Insert fresh */
    sqlite3_prepare_v2(db,
        "INSERT INTO jobs_fts(rowid, type, source, result_json, error) "
        "SELECT id, type, source, COALESCE(result_json,''), COALESCE(error,'') "
        "FROM jobs WHERE id=?", -1, &st, NULL);
    sqlite3_bind_int(st, 1, job_id);
    sqlite3_step(st);
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
}

/* ── Webhook dispatch ─────────────────────────────────────────────── */

static void dispatch_webhooks(sqlite3 *db, int job_id, const char *event) {
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT id, url FROM webhooks WHERE active=1 AND events LIKE '%' || ? || '%'",
        -1, &st, NULL);
    sqlite3_bind_text(st, 1, event, -1, SQLITE_STATIC);

    /* Collect URLs (can't hold mutex during HTTP calls) */
    char urls[32][1024];
    int n = 0;
    while (sqlite3_step(st) == SQLITE_ROW && n < 32) {
        const char *u = (const char *)sqlite3_column_text(st, 1);
        if (u) snprintf(urls[n++], sizeof(urls[0]), "%s", u);
    }
    sqlite3_finalize(st);

    /* Build payload */
    char payload[4096];
    sqlite3_prepare_v2(db,
        "SELECT type, status, input_path, output_path, wall_ms, exit_code "
        "FROM jobs WHERE id=?", -1, &st, NULL);
    sqlite3_bind_int(st, 1, job_id);
    int plen = 0;
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *type = (const char *)sqlite3_column_text(st, 0);
        const char *status = (const char *)sqlite3_column_text(st, 1);
        const char *inp = (const char *)sqlite3_column_text(st, 2);
        const char *out = (const char *)sqlite3_column_text(st, 3);
        double wall = sqlite3_column_double(st, 4);
        int code = sqlite3_column_int(st, 5);
        plen = snprintf(payload, sizeof(payload),
            "{\"event\":\"%s\",\"job\":{\"id\":%d,\"type\":\"%s\",\"status\":\"%s\","
            "\"input\":\"%s\",\"output\":\"%s\",\"wall_ms\":%.1f,\"exit_code\":%d}}",
            event, job_id,
            type ? type : "", status ? status : "",
            inp ? inp : "", out ? out : "",
            wall, code);
    }
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);

    if (plen <= 0 || n == 0) return;

    /* Fire-and-forget HTTP POST to each webhook URL via fork+curl */
    for (int i = 0; i < n; i++) {
        pid_t pid = fork();
        if (pid == 0) {
            char timeout_str[16];
            snprintf(timeout_str, sizeof(timeout_str), "%d", WEBHOOK_TIMEOUT);
            execlp("curl", "curl", "-s", "-X", "POST",
                "-H", "Content-Type: application/json",
                "--max-time", timeout_str,
                "-d", payload,
                urls[i], (char *)NULL);
            _exit(127);
        }
        if (pid > 0) {
            int wstatus;
            waitpid(pid, &wstatus, WNOHANG);
        }
    }
}

/* ── Binary execution ─────────────────────────────────────────────── */

static int run_binary(const char *type, const char *input, const char *outdir,
                      char *result, size_t result_sz) {
    char bin[128];
    snprintf(bin, sizeof(bin), "bonfyre-%s", type);

    int pipefd[2];
    if (pipe(pipefd) < 0) return -1;

    pid_t pid = fork();
    if (pid < 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);

        if (outdir && outdir[0]) {
            char *argv[] = {bin, (char *)input, "--out", (char *)outdir, NULL};
            execvp(bin, argv);
        } else {
            char *argv[] = {bin, (char *)input, NULL};
            execvp(bin, argv);
        }
        _exit(127);
    }

    close(pipefd[1]);
    size_t total = 0;
    while (total < result_sz - 1) {
        ssize_t nr = read(pipefd[0], result + total, result_sz - total - 1);
        if (nr <= 0) break;
        total += (size_t)nr;
    }
    result[total] = '\0';
    close(pipefd[0]);

    int status;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

static int cmd_layer_queue_native(int argc, char **argv, const char *layer_cmd) {
    const char *root = arg_val(argc, argv, "--root");
    const char *target = NULL;
    int priority = 100;
    char *json = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], layer_cmd) == 0 && i + 1 < argc) {
            target = argv[i + 1];
        } else if (strcmp(argv[i], "--priority") == 0 && i + 1 < argc) {
            priority = atoi(argv[i + 1]);
        }
    }
    if (!target) {
        fprintf(stderr, "%s requires <%s>\n", layer_cmd,
                (strcmp(layer_cmd, "layer-compose") == 0 || strcmp(layer_cmd, "layer-bridge") == 0) ? "plan.json" : "artifact_id");
        return 1;
    }
    if (strcmp(layer_cmd, "layer-compose") == 0) {
        if (bf_layer_queue_plan_json(root, layer_cmd, target, priority, &json) != 0) {
            fprintf(stderr, "bonfyre-queue: failed to queue %s plan\n", layer_cmd);
            return 1;
        }
    } else if (strcmp(layer_cmd, "layer-bridge") == 0) {
        if (bf_layer_queue_bridge_plan_json(root, layer_cmd, target, priority, &json) != 0) {
            fprintf(stderr, "bonfyre-queue: failed to queue %s plan\n", layer_cmd);
            return 1;
        }
    } else if (bf_layer_queue_job_json(root, layer_cmd, target, priority, &json) != 0) {
        fprintf(stderr, "bonfyre-queue: failed to queue %s\n", layer_cmd);
        return 1;
    }
    puts(json);
    free(json);
    return 0;
}

/* ── Worker thread ────────────────────────────────────────────────── */

typedef struct {
    sqlite3 *db;
    int worker_id;
} WorkerCtx;

static void *worker_thread(void *arg) {
    WorkerCtx *ctx = (WorkerCtx *)arg;
    char worker_name[32];
    snprintf(worker_name, sizeof(worker_name), "worker-%d", ctx->worker_id);
    int backoff_ms = POLL_BASE_MS;

    fprintf(stderr, "[queue] %s started\n", worker_name);

    while (g_running) {
        /* Claim next job: highest priority (lowest number), oldest first */
        pthread_mutex_lock(&g_db_mutex);
        sqlite3_stmt *st;
        int job_id = 0;
        char type[64] = "", input[2048] = "", outdir[2048] = "";
        int attempts = 0, max_retries = MAX_RETRIES;

        sqlite3_prepare_v2(ctx->db,
            "SELECT id, type, input_path, output_path, attempts, max_retries "
            "FROM jobs WHERE status='queued' "
            "ORDER BY priority ASC, id ASC LIMIT 1",
            -1, &st, NULL);

        if (sqlite3_step(st) == SQLITE_ROW) {
            job_id = sqlite3_column_int(st, 0);
            const char *t = (const char *)sqlite3_column_text(st, 1);
            const char *i = (const char *)sqlite3_column_text(st, 2);
            const char *o = (const char *)sqlite3_column_text(st, 3);
            attempts = sqlite3_column_int(st, 4);
            max_retries = sqlite3_column_int(st, 5);
            if (t) snprintf(type, sizeof(type), "%s", t);
            if (i) snprintf(input, sizeof(input), "%s", i);
            if (o) snprintf(outdir, sizeof(outdir), "%s", o);
        }
        sqlite3_finalize(st);

        if (job_id > 0) {
            /* Claim atomically */
            char ts[64];
            iso_now(ts, sizeof(ts));
            sqlite3_prepare_v2(ctx->db,
                "UPDATE jobs SET status='running', worker=?, claimed_at=?, "
                "attempts=attempts+1 WHERE id=? AND status='queued'",
                -1, &st, NULL);
            sqlite3_bind_text(st, 1, worker_name, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, ts, -1, SQLITE_STATIC);
            sqlite3_bind_int(st, 3, job_id);
            int rc = sqlite3_step(st);
            int claimed = (rc == SQLITE_DONE && sqlite3_changes(ctx->db) > 0);
            sqlite3_finalize(st);
            pthread_mutex_unlock(&g_db_mutex);

            if (!claimed) continue;  /* Another worker got it */

            backoff_ms = POLL_BASE_MS;
            atomic_fetch_add(&g_active_workers, 1);

            char evdata[256];
            snprintf(evdata, sizeof(evdata),
                "{\"worker\":\"%s\",\"type\":\"%s\",\"attempt\":%d}",
                worker_name, type, attempts + 1);
            emit_event(ctx->db, job_id, "running", evdata);

            fprintf(stderr, "[queue] %s executing job %d: bonfyre-%s %s\n",
                worker_name, job_id, type, input);

            double t0 = monotonic_ms();
            char *result = malloc(MAX_RESULT);
            int exit_code = -1;

            if (result) {
                if (outdir[0]) mkdir(outdir, 0755);
                exit_code = run_binary(type, input, outdir, result, MAX_RESULT);
            }

            double wall_ms = monotonic_ms() - t0;
            char done_ts[64];
            iso_now(done_ts, sizeof(done_ts));

            pthread_mutex_lock(&g_db_mutex);
            if (exit_code == 0) {
                sqlite3_prepare_v2(ctx->db,
                    "UPDATE jobs SET status='completed', completed_at=?, "
                    "result_json=?, wall_ms=?, exit_code=0 WHERE id=?",
                    -1, &st, NULL);
                sqlite3_bind_text(st, 1, done_ts, -1, SQLITE_STATIC);
                sqlite3_bind_text(st, 2, result ? result : "", -1, SQLITE_STATIC);
                sqlite3_bind_double(st, 3, wall_ms);
                sqlite3_bind_int(st, 4, job_id);
                sqlite3_step(st);
                sqlite3_finalize(st);
                pthread_mutex_unlock(&g_db_mutex);

                fprintf(stderr, "[queue] %s job %d completed in %.0fms\n",
                    worker_name, job_id, wall_ms);

                char cdata[256];
                snprintf(cdata, sizeof(cdata),
                    "{\"wall_ms\":%.1f,\"exit_code\":0}", wall_ms);
                emit_event(ctx->db, job_id, "completed", cdata);
                fts_sync_job(ctx->db, job_id);
                dispatch_webhooks(ctx->db, job_id, "completed");

            } else {
                int should_retry = (attempts + 1 < max_retries) && (exit_code != 127);
                const char *new_status = should_retry ? "queued" : "failed";

                sqlite3_prepare_v2(ctx->db,
                    "UPDATE jobs SET status=?, completed_at=?, "
                    "error=?, wall_ms=?, exit_code=? WHERE id=?",
                    -1, &st, NULL);
                sqlite3_bind_text(st, 1, new_status, -1, SQLITE_STATIC);
                sqlite3_bind_text(st, 2, done_ts, -1, SQLITE_STATIC);
                sqlite3_bind_text(st, 3, result ? result : "unknown error", -1, SQLITE_STATIC);
                sqlite3_bind_double(st, 4, wall_ms);
                sqlite3_bind_int(st, 5, exit_code);
                sqlite3_bind_int(st, 6, job_id);
                sqlite3_step(st);
                sqlite3_finalize(st);
                pthread_mutex_unlock(&g_db_mutex);

                if (should_retry) {
                    fprintf(stderr, "[queue] %s job %d failed (exit %d), retrying (%d/%d)\n",
                        worker_name, job_id, exit_code, attempts + 1, max_retries);
                    char rdata[256];
                    snprintf(rdata, sizeof(rdata),
                        "{\"exit_code\":%d,\"attempt\":%d,\"max_retries\":%d}",
                        exit_code, attempts + 1, max_retries);
                    emit_event(ctx->db, job_id, "retry", rdata);
                } else {
                    fprintf(stderr, "[queue] %s job %d failed (exit %d), no more retries\n",
                        worker_name, job_id, exit_code);
                    char fdata[256];
                    snprintf(fdata, sizeof(fdata),
                        "{\"exit_code\":%d,\"wall_ms\":%.1f}", exit_code, wall_ms);
                    emit_event(ctx->db, job_id, "failed", fdata);
                    fts_sync_job(ctx->db, job_id);
                    dispatch_webhooks(ctx->db, job_id, "failed");
                }
            }

            free(result);
            atomic_fetch_sub(&g_active_workers, 1);

        } else {
            pthread_mutex_unlock(&g_db_mutex);

            /* Exponential backoff when queue is empty */
            struct timespec ts_sleep;
            ts_sleep.tv_sec = backoff_ms / 1000;
            ts_sleep.tv_nsec = (backoff_ms % 1000) * 1000000L;
            nanosleep(&ts_sleep, NULL);
            if (backoff_ms < POLL_MAX_MS) backoff_ms = backoff_ms * 3 / 2;
        }
    }

    fprintf(stderr, "[queue] %s shutting down\n", worker_name);
    free(arg);
    return NULL;
}

/* ── CLI helpers ──────────────────────────────────────────────────── */

static const char *arg_val(int argc, char **argv, const char *flag) {
    for (int i = 0; i < argc - 1; i++)
        if (strcmp(argv[i], flag) == 0) return argv[i + 1];
    return NULL;
}

static int arg_has(int argc, char **argv, const char *flag) {
    for (int i = 0; i < argc; i++)
        if (strcmp(argv[i], flag) == 0) return 1;
    return 0;
}

/* ── Commands ─────────────────────────────────────────────────────── */

static int cmd_enqueue(sqlite3 *db, int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: bonfyre-queue enqueue <type> <input> [--priority N] [--source S]\n");
        return 1;
    }
    const char *type = argv[2];
    const char *input = argv[3];
    int priority = 100;
    const char *source = "cli";
    const char *outdir = "";

    const char *p = arg_val(argc, argv, "--priority");
    if (p) priority = atoi(p);
    const char *s = arg_val(argc, argv, "--source");
    if (s) source = s;
    const char *o = arg_val(argc, argv, "--output");
    if (o) outdir = o;

    char ts[64];
    iso_now(ts, sizeof(ts));

    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "INSERT INTO jobs (type, status, priority, input_path, output_path, source, created_at, max_retries) "
        "VALUES (?,?,?,?,?,?,?,?)",
        -1, &st, NULL);
    sqlite3_bind_text(st, 1, type, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, "queued", -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 3, priority);
    sqlite3_bind_text(st, 4, input, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 5, outdir, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 6, source, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 7, ts, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 8, MAX_RETRIES);
    sqlite3_step(st);
    sqlite3_finalize(st);
    int job_id = (int)sqlite3_last_insert_rowid(db);
    pthread_mutex_unlock(&g_db_mutex);

    emit_event(db, job_id, "queued", "{\"source\":\"cli\"}");

    printf("{\"id\":%d,\"type\":\"%s\",\"status\":\"queued\",\"priority\":%d}\n",
        job_id, type, priority);
    return 0;
}

static int cmd_work(sqlite3 *db, int argc, char **argv) {
    int num_workers = 4;
    const char *w = arg_val(argc, argv, "--workers");
    if (w) num_workers = atoi(w);
    if (num_workers < 1) num_workers = 1;
    if (num_workers > MAX_WORKERS) num_workers = MAX_WORKERS;

    fprintf(stderr,
        "[queue] v%s starting %d worker%s\n"
        "[queue] db: polling for jobs...\n"
        "[queue] Ctrl-C to stop\n",
        VERSION, num_workers, num_workers > 1 ? "s" : "");

    pthread_t threads[MAX_WORKERS];
    for (int i = 0; i < num_workers; i++) {
        WorkerCtx *ctx = malloc(sizeof(WorkerCtx));
        ctx->db = db;
        ctx->worker_id = i;
        pthread_create(&threads[i], NULL, worker_thread, ctx);
    }

    while (g_running) sleep(1);

    fprintf(stderr, "[queue] shutting down, waiting for %d active job%s...\n",
        atomic_load(&g_active_workers),
        atomic_load(&g_active_workers) != 1 ? "s" : "");

    for (int i = 0; i < num_workers; i++)
        pthread_join(threads[i], NULL);

    fprintf(stderr, "[queue] clean shutdown\n");
    return 0;
}

static int cmd_list(sqlite3 *db, int argc, char **argv) {
    const char *status_filter = arg_val(argc, argv, "--status");
    int limit = 50;
    const char *l = arg_val(argc, argv, "--limit");
    if (l) limit = atoi(l);

    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    if (status_filter) {
        sqlite3_prepare_v2(db,
            "SELECT id, type, status, priority, input_path, output_path, source, "
            "worker, attempts, created_at, claimed_at, completed_at, wall_ms, exit_code "
            "FROM jobs WHERE status=? ORDER BY id DESC LIMIT ?",
            -1, &st, NULL);
        sqlite3_bind_text(st, 1, status_filter, -1, SQLITE_STATIC);
        sqlite3_bind_int(st, 2, limit);
    } else {
        sqlite3_prepare_v2(db,
            "SELECT id, type, status, priority, input_path, output_path, source, "
            "worker, attempts, created_at, claimed_at, completed_at, wall_ms, exit_code "
            "FROM jobs ORDER BY id DESC LIMIT ?",
            -1, &st, NULL);
        sqlite3_bind_int(st, 1, limit);
    }

    printf("[");
    int i = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (i > 0) printf(",");
        printf("\n  {\"id\":%d,\"type\":\"%s\",\"status\":\"%s\",\"priority\":%d,"
               "\"input\":\"%s\",\"output\":\"%s\",\"source\":\"%s\","
               "\"worker\":\"%s\",\"attempts\":%d,"
               "\"created_at\":\"%s\",\"claimed_at\":\"%s\",\"completed_at\":\"%s\","
               "\"wall_ms\":%.1f,\"exit_code\":%d}",
            sqlite3_column_int(st, 0),
            sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : "",
            sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "",
            sqlite3_column_int(st, 3),
            sqlite3_column_text(st, 4) ? (const char *)sqlite3_column_text(st, 4) : "",
            sqlite3_column_text(st, 5) ? (const char *)sqlite3_column_text(st, 5) : "",
            sqlite3_column_text(st, 6) ? (const char *)sqlite3_column_text(st, 6) : "",
            sqlite3_column_text(st, 7) ? (const char *)sqlite3_column_text(st, 7) : "",
            sqlite3_column_int(st, 8),
            sqlite3_column_text(st, 9) ? (const char *)sqlite3_column_text(st, 9) : "",
            sqlite3_column_text(st, 10) ? (const char *)sqlite3_column_text(st, 10) : "",
            sqlite3_column_text(st, 11) ? (const char *)sqlite3_column_text(st, 11) : "",
            sqlite3_column_double(st, 12),
            sqlite3_column_int(st, 13));
        i++;
    }
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
    printf("\n]\n");
    return 0;
}

static int cmd_stats(sqlite3 *db) {
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;

    int total = 0, queued = 0, running = 0, completed = 0, failed = 0;
    double avg_wall = 0;
    int with_wall = 0;

    sqlite3_prepare_v2(db,
        "SELECT status, COUNT(*) FROM jobs GROUP BY status", -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *s = (const char *)sqlite3_column_text(st, 0);
        int c = sqlite3_column_int(st, 1);
        total += c;
        if (s && strcmp(s, "queued") == 0) queued = c;
        else if (s && strcmp(s, "running") == 0) running = c;
        else if (s && strcmp(s, "completed") == 0) completed = c;
        else if (s && strcmp(s, "failed") == 0) failed = c;
    }
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,
        "SELECT AVG(wall_ms), COUNT(*) FROM jobs WHERE wall_ms > 0", -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) {
        avg_wall = sqlite3_column_double(st, 0);
        with_wall = sqlite3_column_int(st, 1);
    }
    sqlite3_finalize(st);

    int event_count = 0;
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM events", -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) event_count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    int webhook_count = 0;
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM webhooks WHERE active=1", -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) webhook_count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    pthread_mutex_unlock(&g_db_mutex);

    printf("{\"version\":\"%s\",\"total\":%d,\"queued\":%d,\"running\":%d,"
           "\"completed\":%d,\"failed\":%d,"
           "\"avg_wall_ms\":%.1f,\"timed_jobs\":%d,"
           "\"events\":%d,\"webhooks\":%d,"
           "\"active_workers\":%d}\n",
        VERSION, total, queued, running, completed, failed,
        avg_wall, with_wall,
        event_count, webhook_count,
        atomic_load(&g_active_workers));
    return 0;
}

static int cmd_events(sqlite3 *db, int argc, char **argv) {
    int since = 0;
    const char *s = arg_val(argc, argv, "--since");
    if (s) since = atoi(s);
    int follow = arg_has(argc, argv, "--follow");

    do {
        pthread_mutex_lock(&g_db_mutex);
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,
            "SELECT id, job_id, event, data, created_at FROM events "
            "WHERE id > ? ORDER BY id ASC LIMIT 100",
            -1, &st, NULL);
        sqlite3_bind_int(st, 1, since);

        int got = 0;
        while (sqlite3_step(st) == SQLITE_ROW) {
            int eid = sqlite3_column_int(st, 0);
            printf("{\"id\":%d,\"job_id\":%d,\"event\":\"%s\",\"data\":%s,\"created_at\":\"%s\"}\n",
                eid,
                sqlite3_column_int(st, 1),
                sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "",
                sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "{}",
                sqlite3_column_text(st, 4) ? (const char *)sqlite3_column_text(st, 4) : "");
            since = eid;
            got++;
        }
        sqlite3_finalize(st);
        pthread_mutex_unlock(&g_db_mutex);

        if (follow && got == 0) usleep(500000);
    } while (follow && g_running);

    return 0;
}

static int cmd_webhook(sqlite3 *db, int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: bonfyre-queue webhook add|list|remove ...\n");
        return 1;
    }

    const char *sub = argv[2];

    if (strcmp(sub, "add") == 0) {
        if (argc < 4) {
            fprintf(stderr, "Usage: bonfyre-queue webhook add <url>\n");
            return 1;
        }
        const char *url = argv[3];
        char ts[64];
        iso_now(ts, sizeof(ts));

        pthread_mutex_lock(&g_db_mutex);
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,
            "INSERT INTO webhooks (url, events, active, created_at) VALUES (?,?,1,?)",
            -1, &st, NULL);
        sqlite3_bind_text(st, 1, url, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, "completed,failed", -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 3, ts, -1, SQLITE_STATIC);
        sqlite3_step(st);
        sqlite3_finalize(st);
        int wid = (int)sqlite3_last_insert_rowid(db);
        pthread_mutex_unlock(&g_db_mutex);

        printf("{\"id\":%d,\"url\":\"%s\",\"status\":\"active\"}\n", wid, url);
        return 0;

    } else if (strcmp(sub, "list") == 0) {
        pthread_mutex_lock(&g_db_mutex);
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,
            "SELECT id, url, events, active, created_at FROM webhooks ORDER BY id",
            -1, &st, NULL);
        printf("[");
        int i = 0;
        while (sqlite3_step(st) == SQLITE_ROW) {
            if (i > 0) printf(",");
            printf("\n  {\"id\":%d,\"url\":\"%s\",\"events\":\"%s\",\"active\":%d,\"created_at\":\"%s\"}",
                sqlite3_column_int(st, 0),
                sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : "",
                sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "",
                sqlite3_column_int(st, 3),
                sqlite3_column_text(st, 4) ? (const char *)sqlite3_column_text(st, 4) : "");
            i++;
        }
        sqlite3_finalize(st);
        pthread_mutex_unlock(&g_db_mutex);
        printf("\n]\n");
        return 0;

    } else if (strcmp(sub, "remove") == 0) {
        if (argc < 4) {
            fprintf(stderr, "Usage: bonfyre-queue webhook remove <id>\n");
            return 1;
        }
        int wid = atoi(argv[3]);
        pthread_mutex_lock(&g_db_mutex);
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,
            "UPDATE webhooks SET active=0 WHERE id=?", -1, &st, NULL);
        sqlite3_bind_int(st, 1, wid);
        sqlite3_step(st);
        sqlite3_finalize(st);
        pthread_mutex_unlock(&g_db_mutex);
        printf("{\"id\":%d,\"status\":\"removed\"}\n", wid);
        return 0;
    }

    fprintf(stderr, "Unknown webhook subcommand: %s\n", sub);
    return 1;
}

static int cmd_retry(sqlite3 *db, int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: bonfyre-queue retry <job-id>\n");
        return 1;
    }
    int job_id = atoi(argv[2]);

    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "UPDATE jobs SET status='queued', error=NULL, completed_at=NULL, "
        "claimed_at=NULL, worker=NULL WHERE id=? AND status IN ('failed','completed')",
        -1, &st, NULL);
    sqlite3_bind_int(st, 1, job_id);
    sqlite3_step(st);
    int changed = sqlite3_changes(db);
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);

    if (changed > 0) {
        emit_event(db, job_id, "retry", "{\"source\":\"manual\"}");
        printf("{\"id\":%d,\"status\":\"queued\",\"action\":\"retry\"}\n", job_id);
    } else {
        fprintf(stderr, "Job %d not found or not in retryable state\n", job_id);
        return 1;
    }
    return 0;
}

static int cmd_purge(sqlite3 *db, int argc, char **argv) {
    int days = 30;
    const char *d = arg_val(argc, argv, "--older-than");
    if (d) days = atoi(d);

    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "DELETE FROM events WHERE job_id IN "
        "(SELECT id FROM jobs WHERE status IN ('completed','failed') "
        "AND completed_at < datetime('now', '-' || ? || ' days'))",
        -1, &st, NULL);
    sqlite3_bind_int(st, 1, days);
    sqlite3_step(st);
    int events_purged = sqlite3_changes(db);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,
        "DELETE FROM jobs WHERE status IN ('completed','failed') "
        "AND completed_at < datetime('now', '-' || ? || ' days')",
        -1, &st, NULL);
    sqlite3_bind_int(st, 1, days);
    sqlite3_step(st);
    int jobs_purged = sqlite3_changes(db);
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);

    printf("{\"jobs_purged\":%d,\"events_purged\":%d,\"older_than_days\":%d}\n",
        jobs_purged, events_purged, days);
    return 0;
}

/* ── Main ─────────────────────────────────────────────────────────── */

static void usage(void) {
    fprintf(stderr,
        "BonfyreQueue v%s — SQLite-backed job queue with worker daemon\n\n"
        "Usage:\n"
        "  bonfyre-queue layer-verify <artifact_id> [--priority N] [--root DIR]\n"
        "  bonfyre-queue layer-materialize <artifact_id> [--priority N] [--root DIR]\n"
        "  bonfyre-queue layer-compose <plan.json> [--priority N] [--root DIR]\n"
        "  bonfyre-queue layer-bridge <resolved-plan.json> [--priority N] [--root DIR]\n"
        "  bonfyre-queue layer-sync <artifact_id> [--priority N] [--root DIR]\n"
        "  bonfyre-queue enqueue <type> <input> [--priority N] [--source S] [--output DIR]\n"
        "  bonfyre-queue work [--workers N]\n"
        "  bonfyre-queue list [--status S] [--limit N]\n"
        "  bonfyre-queue stats\n"
        "  bonfyre-queue events [--since ID] [--follow]\n"
        "  bonfyre-queue webhook add|list|remove ...\n"
        "  bonfyre-queue retry <job-id>\n"
        "  bonfyre-queue purge [--older-than DAYS]\n\n"
        "Options:\n"
        "  --db FILE    SQLite database path (default: ~/.local/share/bonfyre/queue.db)\n\n"
        "Worker mode polls for queued jobs, forks bonfyre-<type> binaries,\n"
        "captures results, emits events, and dispatches webhooks.\n",
        VERSION);
}

static void handle_signal(int sig) {
    (void)sig;
    g_running = 0;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        usage();
        return 1;
    }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGPIPE, SIG_IGN);

    const char *db_path = arg_val(argc, argv, "--db");
    if (!db_path) db_path = default_db_path();

    /* Find command (first non-flag argument) */
    const char *cmd = NULL;
    for (int i = 1; i < argc; i++) {
        if (argv[i][0] == '-') {
            /* Skip flag values */
            if (strcmp(argv[i], "--db") == 0 || strcmp(argv[i], "--workers") == 0 ||
                strcmp(argv[i], "--priority") == 0 || strcmp(argv[i], "--source") == 0 ||
                strcmp(argv[i], "--output") == 0 || strcmp(argv[i], "--status") == 0 ||
                strcmp(argv[i], "--limit") == 0 || strcmp(argv[i], "--since") == 0 ||
                strcmp(argv[i], "--older-than") == 0)
                i++;
            continue;
        }
        cmd = argv[i];
        break;
    }

    if (!cmd) { usage(); return 1; }

    if (strcmp(cmd, "layer-verify") == 0) return cmd_layer_queue_native(argc, argv, "layer-verify");
    if (strcmp(cmd, "layer-materialize") == 0) return cmd_layer_queue_native(argc, argv, "layer-materialize");
    if (strcmp(cmd, "layer-compose") == 0) return cmd_layer_queue_native(argc, argv, "layer-compose");
    if (strcmp(cmd, "layer-bridge") == 0) return cmd_layer_queue_native(argc, argv, "layer-bridge");
    if (strcmp(cmd, "layer-sync") == 0) return cmd_layer_queue_native(argc, argv, "layer-sync");

    sqlite3 *db = open_db(db_path);
    if (!db) return 1;

    int rc = 1;
    if (strcmp(cmd, "enqueue") == 0)       rc = cmd_enqueue(db, argc, argv);
    else if (strcmp(cmd, "work") == 0)     rc = cmd_work(db, argc, argv);
    else if (strcmp(cmd, "list") == 0)     rc = cmd_list(db, argc, argv);
    else if (strcmp(cmd, "stats") == 0)    rc = cmd_stats(db);
    else if (strcmp(cmd, "events") == 0)   rc = cmd_events(db, argc, argv);
    else if (strcmp(cmd, "webhook") == 0)  rc = cmd_webhook(db, argc, argv);
    else if (strcmp(cmd, "retry") == 0)    rc = cmd_retry(db, argc, argv);
    else if (strcmp(cmd, "purge") == 0)    rc = cmd_purge(db, argc, argv);
    else { fprintf(stderr, "Unknown command: %s\n", cmd); usage(); }

    sqlite3_close(db);
    return rc;
}
