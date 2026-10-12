/*
 * BonfyreAPI v2 — Async HTTP gateway for the Bonfyre binary family.
 *
 * Shares a single SQLite WAL database with BonfyreQueue.  Jobs are enqueued
 * (INSERT) and return immediately; the queue worker daemon picks them up.
 *
 * New in v2:
 *   - GET  /api/events          Server-Sent Events stream (real-time job updates)
 *   - GET  /api/search?q=term   FTS5 full-text search over job results
 *   - POST /api/webhooks        Register webhook callback URLs
 *   - GET  /api/webhooks        List registered webhooks
 *   - API-key auth middleware    (api_keys table; header Authorization: Bearer <key>)
 *   - In-memory rate limiting    (token bucket per API key, 120 req/min)
 *
 * Retained from v1:
 *   - POST /api/upload           File upload
 *   - POST /api/jobs             Submit job (now async)
 *   - GET  /api/jobs[/:id]       Job status/list
 *   - *    /api/binaries/:name   Proxy to any binary
 *   - GET  /api/health           Health check
 *   - GET  /api/status           Dashboard
 *   - GET  /                     Static files / SPA fallback
 *
 * Usage:
 *   bonfyre-api serve [--port 8080] [--db FILE] [--static DIR] [--uploads DIR]
 */
#define _GNU_SOURCE
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
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

#define VERSION       "2.0.0"
#define MAX_BODY      (4*1024*1024)
#define MAX_PATH_SEGS 16
#define MAX_PATH_LEN  2048
#define MAX_RESULT    (1024*1024)
#define MAX_THREADS   64
#define THREAD_STACK  (256*1024)

/* Rate limiting: token bucket */
#define RL_CAPACITY   120      /* tokens (requests) */
#define RL_REFILL     2.0      /* tokens per second */
#define RL_MAX_KEYS   256

static volatile int g_running = 1;
static atomic_int g_thread_count = 0;
static sqlite3 *g_db = NULL;
static pthread_mutex_t g_db_mutex = PTHREAD_MUTEX_INITIALIZER;
static char g_static_dir[MAX_PATH_LEN] = "";
static char g_upload_dir[MAX_PATH_LEN] = "";

static const char *arg_get(int argc, char **argv, const char *flag);

static int delegate_layer_api(int argc, char **argv) {
    if (argc < 4) return 1;
    const char *root = arg_get(argc, argv, "--root");
    if (strcmp(argv[2], "get") == 0) {
        char *json = NULL;
        if (bf_layer_load_json(root, argv[3], &json) != 0) return 1;
        puts(json);
        free(json);
        return 0;
    } else if (strcmp(argv[2], "lineage") == 0) {
        char *json = NULL;
        if (bf_layer_graph_edges_json(root, argv[3], &json) != 0) return 1;
        puts(json);
        free(json);
        return 0;
    } else if (strcmp(argv[2], "compat") == 0 && argc >= 5) {
        char *json = NULL;
        if (bf_layer_compat_json(root, argv[3], argv[4], &json) != 0) return 1;
        puts(json);
        free(json);
        return 0;
    } else if (strcmp(argv[2], "compose") == 0 && argc >= 5) {
        char *json = NULL;
        int dry_run = 0;
        for (int i = 5; i < argc; i++) {
            if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
        }
        if (bf_layer_compose_json(root, argv[3], argv[4], dry_run, &json) != 0) return 1;
        puts(json);
        free(json);
        return 0;
    } else {
        return 1;
    }
    return 1;
}

/* ── Rate limiter (in-memory token bucket per API key) ────────────── */

typedef struct {
    char key[256];
    double tokens;
    double last_refill;  /* monotonic seconds */
} RateBucket;

static RateBucket g_buckets[RL_MAX_KEYS];
static int g_nbuckets = 0;
static pthread_mutex_t g_rl_mutex = PTHREAD_MUTEX_INITIALIZER;

static double mono_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static int rate_limit_check(const char *key) {
    if (!key || !key[0]) return 1;  /* No key = anonymous = allow */

    pthread_mutex_lock(&g_rl_mutex);
    double now = mono_sec();
    RateBucket *b = NULL;

    for (int i = 0; i < g_nbuckets; i++) {
        if (strcmp(g_buckets[i].key, key) == 0) { b = &g_buckets[i]; break; }
    }

    if (!b) {
        if (g_nbuckets >= RL_MAX_KEYS) {
            /* Evict oldest (simplistic) */
            b = &g_buckets[0];
        } else {
            b = &g_buckets[g_nbuckets++];
        }
        strncpy(b->key, key, sizeof(b->key) - 1);
        b->tokens = RL_CAPACITY;
        b->last_refill = now;
    }

    /* Refill tokens */
    double elapsed = now - b->last_refill;
    b->tokens += elapsed * RL_REFILL;
    if (b->tokens > RL_CAPACITY) b->tokens = RL_CAPACITY;
    b->last_refill = now;

    int allowed = 0;
    if (b->tokens >= 1.0) {
        b->tokens -= 1.0;
        allowed = 1;
    }

    pthread_mutex_unlock(&g_rl_mutex);
    return allowed;
}

/* ── HTTP primitives ──────────────────────────────────────────────── */

typedef struct {
    char method[16];
    char path[MAX_PATH_LEN];
    char query[MAX_PATH_LEN];
    char *body;
    int body_len;
    char auth_token[256];
    int content_length;
    char content_type[256];
} HttpRequest;

typedef struct {
    int fd;
    char *buf;
    size_t buf_len;
    int status;
    char content_type[128];
} HttpResponse;

static void http_resp_init(HttpResponse *r, int fd) {
    memset(r, 0, sizeof(*r));
    r->fd = fd; r->status = 200;
    strcpy(r->content_type, "application/json");
}
static void http_resp_free(HttpResponse *r) { free(r->buf); r->buf = NULL; }

static void http_resp_send(HttpResponse *r) {
    const char *st = "OK";
    if (r->status == 201) st = "Created";
    else if (r->status == 204) st = "No Content";
    else if (r->status == 400) st = "Bad Request";
    else if (r->status == 401) st = "Unauthorized";
    else if (r->status == 404) st = "Not Found";
    else if (r->status == 405) st = "Method Not Allowed";
    else if (r->status == 429) st = "Too Many Requests";
    else if (r->status == 500) st = "Internal Server Error";
    else if (r->status == 503) st = "Service Unavailable";

    char hdr[1024];
    int hlen = snprintf(hdr, sizeof(hdr),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"
        "Access-Control-Allow-Headers: Authorization, Content-Type\r\n"
        "Connection: close\r\n\r\n",
        r->status, st, r->content_type, r->buf_len);
    write(r->fd, hdr, (size_t)hlen);
    if (r->buf_len > 0 && r->buf) write(r->fd, r->buf, r->buf_len);
}

static void http_resp_json(HttpResponse *r, int status, const char *fmt, ...) {
    r->status = status;
    free(r->buf);
    va_list ap; va_start(ap, fmt);
    int n = vasprintf(&r->buf, fmt, ap);
    va_end(ap);
    r->buf_len = (size_t)(n > 0 ? n : 0);
}

static int parse_http_request(int fd, HttpRequest *req) {
    memset(req, 0, sizeof(*req));
    req->body = NULL;

    char hdr[8192];
    ssize_t total = 0;
    while (total < (ssize_t)sizeof(hdr) - 1) {
        ssize_t n = read(fd, hdr + total, sizeof(hdr) - (size_t)total - 1);
        if (n <= 0) break;
        total += n; hdr[total] = '\0';
        if (strstr(hdr, "\r\n\r\n")) break;
    }
    if (total <= 0) return -1;
    hdr[total] = '\0';

    char *body_marker = strstr(hdr, "\r\n\r\n");
    size_t header_end = body_marker ? (size_t)(body_marker - hdr) : (size_t)total;

    char *line_end = strstr(hdr, "\r\n");
    if (!line_end) return -1;
    *line_end = '\0';
    sscanf(hdr, "%15s %2047s", req->method, req->path);

    char *qm = strchr(req->path, '?');
    if (qm) { *qm = '\0'; strncpy(req->query, qm + 1, sizeof(req->query) - 1); }

    char *hp = line_end + 2;
    while (hp < hdr + header_end) {
        char *he = strstr(hp, "\r\n");
        if (!he || he == hp) break;
        *he = '\0';
        if (strncasecmp(hp, "Content-Length:", 15) == 0)
            req->content_length = atoi(hp + 15);
        if (strncasecmp(hp, "Content-Type:", 13) == 0) {
            const char *v = hp + 13; while (*v == ' ') v++;
            strncpy(req->content_type, v, sizeof(req->content_type) - 1);
        }
        if (strncasecmp(hp, "Authorization:", 14) == 0) {
            const char *av = hp + 14; while (*av == ' ') av++;
            if (strncasecmp(av, "Bearer ", 7) == 0) av += 7;
            strncpy(req->auth_token, av, sizeof(req->auth_token) - 1);
        }
        hp = he + 2;
    }

    if (body_marker && req->content_length > 0) {
        int cap = req->content_length < MAX_BODY ? req->content_length : MAX_BODY;
        req->body = malloc((size_t)cap + 1);
        char *body_start = body_marker + 4;
        size_t in_hdr = (size_t)(total - (body_start - hdr));
        if (in_hdr > 0) {
            size_t cp = in_hdr < (size_t)cap ? in_hdr : (size_t)cap;
            memcpy(req->body, body_start, cp);
            req->body_len = (int)cp;
        }
        while (req->body_len < cap) {
            ssize_t n = read(fd, req->body + req->body_len, (size_t)(cap - req->body_len));
            if (n <= 0) break;
            req->body_len += (int)n;
        }
        req->body[req->body_len] = '\0';
    }
    return 0;
}

static void free_request(HttpRequest *req) { free(req->body); }

/* ── Utilities ────────────────────────────────────────────────────── */

static void iso_now(char *buf, size_t sz) { bf_iso_timestamp(buf, sz); }

static int path_segments(const char *path, char segs[][256], int max) {
    int c = 0; const char *p = path;
    while (*p == '/') p++;
    while (*p && c < max) {
        size_t i = 0;
        while (*p && *p != '/' && i < 255) segs[c][i++] = *p++;
        segs[c][i] = '\0'; c++;
        while (*p == '/') p++;
    }
    return c;
}

static const char *query_param(const char *query, const char *key, char *buf, size_t sz) {
    size_t klen = strlen(key);
    const char *p = query;
    while (p && *p) {
        if (strncmp(p, key, klen) == 0 && p[klen] == '=') {
            const char *v = p + klen + 1;
            size_t i = 0;
            while (*v && *v != '&' && i < sz - 1) buf[i++] = *v++;
            buf[i] = '\0';
            return buf;
        }
        p = strchr(p, '&');
        if (p) p++;
    }
    buf[0] = '\0';
    return NULL;
}

static int run_binary(const char *bin, char *const argv[], char *out, size_t out_sz) {
    int pipefd[2];
    if (pipe(pipefd) < 0) return -1;
    pid_t pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return -1; }
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);
        execvp(bin, argv);
        _exit(127);
    }
    close(pipefd[1]);
    size_t total = 0;
    while (total < out_sz - 1) {
        ssize_t n = read(pipefd[0], out + total, out_sz - total - 1);
        if (n <= 0) break;
        total += (size_t)n;
    }
    out[total] = '\0';
    close(pipefd[0]);
    int status; waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

/* ── Database ─────────────────────────────────────────────────────── */

/*
 * Shared schema: the API creates queue tables if the queue hasn't yet,
 * and adds its own tables (uploads, api_keys, request_log).  When both
 * API and Queue point at the same DB file, they share the jobs/events
 * tables via WAL mode.
 */
static const char *SCHEMA_SQL =
    "PRAGMA journal_mode=WAL;"
    "PRAGMA busy_timeout=5000;"
    "PRAGMA synchronous=NORMAL;"

    /* Queue tables (shared with BonfyreQueue) */
    "CREATE TABLE IF NOT EXISTS jobs ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  type TEXT NOT NULL,"
    "  status TEXT NOT NULL DEFAULT 'queued',"
    "  priority INTEGER NOT NULL DEFAULT 100,"
    "  input_path TEXT,"
    "  output_path TEXT,"
    "  source TEXT DEFAULT 'api',"
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

    "CREATE VIRTUAL TABLE IF NOT EXISTS jobs_fts USING fts5("
    "  type, source, result_json, error,"
    "  content=jobs, content_rowid=id"
    ");"

    /* API-only tables */
    "CREATE TABLE IF NOT EXISTS uploads ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  filename TEXT NOT NULL,"
    "  path TEXT NOT NULL,"
    "  size_bytes INTEGER,"
    "  content_type TEXT,"
    "  uploaded_at TEXT NOT NULL,"
    "  api_key TEXT"
    ");"

    "CREATE TABLE IF NOT EXISTS api_keys ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  key TEXT NOT NULL UNIQUE,"
    "  label TEXT,"
    "  scopes TEXT DEFAULT '*',"
    "  active INTEGER DEFAULT 1,"
    "  created_at TEXT NOT NULL"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_api_keys_key ON api_keys(key);"

    "CREATE TABLE IF NOT EXISTS request_log ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  method TEXT,"
    "  path TEXT,"
    "  api_key TEXT,"
    "  status INTEGER,"
    "  wall_ms REAL,"
    "  created_at TEXT NOT NULL"
    ");";

static sqlite3 *open_db(const char *path) {
    sqlite3 *db;
    if (sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "[api] cannot open %s: %s\n", path, sqlite3_errmsg(db));
        return NULL;
    }
    char *err = NULL;
    if (sqlite3_exec(db, SCHEMA_SQL, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "[api] schema error: %s\n", err);
        sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    return db;
}

static const char *default_db(void) {
    static char p[MAX_PATH_LEN];
    const char *h = getenv("HOME");
    snprintf(p, sizeof(p), "%s/.local/share/bonfyre/queue.db", h ? h : ".");
    char d[MAX_PATH_LEN];
    snprintf(d, sizeof(d), "%s/.local/share/bonfyre", h ? h : ".");
    mkdir(d, 0755);
    return p;
}

/* ── Auth middleware ──────────────────────────────────────────────── */

static int validate_api_key(const char *key) {
    if (!key || !key[0]) return 1;  /* No key = anonymous access allowed */

    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "SELECT active FROM api_keys WHERE key=?", -1, &st, NULL);
    sqlite3_bind_text(st, 1, key, -1, SQLITE_STATIC);
    int valid = 0;
    if (sqlite3_step(st) == SQLITE_ROW)
        valid = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
    return valid;
}

/* ── Request logging ──────────────────────────────────────────────── */

static void log_request(const char *method, const char *path,
                        const char *key, int status, double wall_ms) {
    char ts[64]; iso_now(ts, sizeof(ts));
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "INSERT INTO request_log (method,path,api_key,status,wall_ms,created_at) "
        "VALUES (?,?,?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st, 1, method, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, key && key[0] ? key : "anon", -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 4, status);
    sqlite3_bind_double(st, 5, wall_ms);
    sqlite3_bind_text(st, 6, ts, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
}

/* ── Event emission (shared with queue) ───────────────────────────── */

static void emit_event(int job_id, const char *event, const char *data) {
    char ts[64]; iso_now(ts, sizeof(ts));
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
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

/* ── Static file serving ──────────────────────────────────────────── */

static const char *mime_for_ext(const char *path) {
    const char *dot = strrchr(path, '.');
    if (!dot) return "application/octet-stream";
    if (strcasecmp(dot, ".html") == 0 || strcasecmp(dot, ".htm") == 0) return "text/html";
    if (strcasecmp(dot, ".css") == 0) return "text/css";
    if (strcasecmp(dot, ".js") == 0) return "application/javascript";
    if (strcasecmp(dot, ".json") == 0) return "application/json";
    if (strcasecmp(dot, ".png") == 0) return "image/png";
    if (strcasecmp(dot, ".jpg") == 0 || strcasecmp(dot, ".jpeg") == 0) return "image/jpeg";
    if (strcasecmp(dot, ".gif") == 0) return "image/gif";
    if (strcasecmp(dot, ".svg") == 0) return "image/svg+xml";
    if (strcasecmp(dot, ".ico") == 0) return "image/x-icon";
    if (strcasecmp(dot, ".woff2") == 0) return "font/woff2";
    if (strcasecmp(dot, ".woff") == 0) return "font/woff";
    return "application/octet-stream";
}

static int serve_static(HttpResponse *resp, const char *url_path) {
    if (!g_static_dir[0]) return 0;
    if (strstr(url_path, "..")) return 0;

    char fpath[MAX_PATH_LEN];
    if (strcmp(url_path, "/") == 0 || strcmp(url_path, "") == 0)
        snprintf(fpath, sizeof(fpath), "%s/index.html", g_static_dir);
    else
        snprintf(fpath, sizeof(fpath), "%s%s", g_static_dir, url_path);

    char resolved[MAX_PATH_LEN];
    if (!realpath(fpath, resolved)) return 0;
    char resolved_root[MAX_PATH_LEN];
    if (!realpath(g_static_dir, resolved_root)) return 0;
    if (strncmp(resolved, resolved_root, strlen(resolved_root)) != 0) return 0;

    FILE *f = fopen(resolved, "rb");
    if (!f) {
        snprintf(fpath, sizeof(fpath), "%s/index.html", g_static_dir);
        f = fopen(fpath, "rb");
        if (!f) return 0;
    }
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz <= 0 || sz > 10 * 1024 * 1024) { fclose(f); return 0; }

    free(resp->buf);
    resp->buf = malloc((size_t)sz);
    resp->buf_len = fread(resp->buf, 1, (size_t)sz, f);
    fclose(f);
    resp->status = 200;
    strncpy(resp->content_type, mime_for_ext(resolved), sizeof(resp->content_type) - 1);
    return 1;
}

/* ── API routes ───────────────────────────────────────────────────── */

/* GET /api/health */
static void handle_health(HttpResponse *resp) {
    http_resp_json(resp, 200,
        "{\"status\":\"ok\",\"version\":\"%s\",\"service\":\"bonfyre-api\"}", VERSION);
}

/* GET /api/status — system dashboard */
static void handle_status(HttpResponse *resp) {
    pthread_mutex_lock(&g_db_mutex);

    int total_jobs = 0, completed = 0, failed = 0, queued = 0, running = 0;
    int total_uploads = 0;
    long total_bytes = 0;
    double avg_wall = 0;
    int event_count = 0, webhook_count = 0, key_count = 0;
    int req_count = 0;

    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db, "SELECT status, COUNT(*) FROM jobs GROUP BY status",
        -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *s = (const char *)sqlite3_column_text(st, 0);
        int c = sqlite3_column_int(st, 1);
        total_jobs += c;
        if (s && strcmp(s, "completed") == 0) completed = c;
        else if (s && strcmp(s, "failed") == 0) failed = c;
        else if (s && strcmp(s, "queued") == 0) queued = c;
        else if (s && strcmp(s, "running") == 0) running = c;
    }
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT AVG(wall_ms) FROM jobs WHERE wall_ms>0",
        -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) avg_wall = sqlite3_column_double(st, 0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT COUNT(*), COALESCE(SUM(size_bytes),0) FROM uploads",
        -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) {
        total_uploads = sqlite3_column_int(st, 0);
        total_bytes = (long)sqlite3_column_int64(st, 1);
    }
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM events", -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) event_count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM webhooks WHERE active=1",
        -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) webhook_count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM api_keys WHERE active=1",
        -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) key_count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM request_log", -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) req_count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    pthread_mutex_unlock(&g_db_mutex);

    http_resp_json(resp, 200,
        "{\"status\":\"ok\",\"version\":\"%s\","
        "\"jobs\":{\"total\":%d,\"queued\":%d,\"running\":%d,\"completed\":%d,\"failed\":%d,\"avg_wall_ms\":%.1f},"
        "\"uploads\":{\"total\":%d,\"bytes\":%ld},"
        "\"events\":%d,\"webhooks\":%d,\"api_keys\":%d,\"requests_logged\":%d,"
        "\"active_threads\":%d}",
        VERSION, total_jobs, queued, running, completed, failed, avg_wall,
        total_uploads, total_bytes,
        event_count, webhook_count, key_count, req_count,
        atomic_load(&g_thread_count));
}

/* POST /api/upload */
static void handle_upload(HttpRequest *req, HttpResponse *resp) {
    if (!req->body || req->body_len <= 0) {
        http_resp_json(resp, 400, "{\"error\":\"empty body\"}"); return;
    }

    char ts[64]; iso_now(ts, sizeof(ts));
    char fname[256];
    snprintf(fname, sizeof(fname), "upload_%ld_%d", (long)time(NULL), getpid());

    char *fn_param = strstr(req->query, "filename=");
    if (fn_param) {
        fn_param += 9;
        char safe[128]; int si = 0;
        while (*fn_param && *fn_param != '&' && si < (int)sizeof(safe) - 1) {
            char c = *fn_param++;
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.')
                safe[si++] = c;
        }
        safe[si] = '\0';
        if (si > 0) snprintf(fname, sizeof(fname), "%ld_%s", (long)time(NULL), safe);
    }

    char fpath[MAX_PATH_LEN];
    snprintf(fpath, sizeof(fpath), "%s/%s", g_upload_dir, fname);

    FILE *f = fopen(fpath, "wb");
    if (!f) { http_resp_json(resp, 500, "{\"error\":\"cannot write file\"}"); return; }
    fwrite(req->body, 1, (size_t)req->body_len, f);
    fclose(f);

    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "INSERT INTO uploads (filename,path,size_bytes,content_type,uploaded_at,api_key) "
        "VALUES (?,?,?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st, 1, fname, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, fpath, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 3, req->body_len);
    sqlite3_bind_text(st, 4, req->content_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 5, ts, -1, SQLITE_STATIC);
    if (req->auth_token[0]) sqlite3_bind_text(st, 6, req->auth_token, -1, SQLITE_STATIC);
    else sqlite3_bind_null(st, 6);
    sqlite3_step(st); sqlite3_finalize(st);
    int upload_id = (int)sqlite3_last_insert_rowid(g_db);
    pthread_mutex_unlock(&g_db_mutex);

    http_resp_json(resp, 201,
        "{\"data\":{\"id\":%d,\"filename\":\"%s\",\"size\":%d,\"path\":\"%s\"},"
        "\"meta\":{\"uploaded\":true}}",
        upload_id, fname, req->body_len, fpath);
}

/* POST /api/jobs — async: enqueue and return immediately */
static void handle_job_submit(HttpRequest *req, HttpResponse *resp) {
    if (!req->body || req->body_len <= 0) {
        http_resp_json(resp, 400, "{\"error\":\"empty body\"}"); return;
    }

    /* Parse JSON: {"type":"transcribe","input":"/path","priority":100} */
    char type[64] = "", input[MAX_PATH_LEN] = "";
    int priority = 100;
    const char *p = req->body;

    const char *tf = strstr(p, "\"type\"");
    if (tf) {
        tf = strchr(tf + 6, '"');
        if (tf) { tf++;
            int i = 0; while (*tf && *tf != '"' && i < 63) type[i++] = *tf++;
            type[i] = '\0';
        }
    }
    const char *inf = strstr(p, "\"input\"");
    if (inf) {
        inf = strchr(inf + 7, '"');
        if (inf) { inf++;
            int i = 0; while (*inf && *inf != '"' && i < (int)sizeof(input) - 1) input[i++] = *inf++;
            input[i] = '\0';
        }
    }
    const char *pf = strstr(p, "\"priority\"");
    if (pf) {
        pf += 10; while (*pf && (*pf == ':' || *pf == ' ')) pf++;
        if (*pf >= '0' && *pf <= '9') priority = atoi(pf);
    }

    if (!type[0]) { http_resp_json(resp, 400, "{\"error\":\"missing type\"}"); return; }

    /* Create output directory */
    char outdir[MAX_PATH_LEN];
    snprintf(outdir, sizeof(outdir), "%s/job_%ld_%d", g_upload_dir, (long)time(NULL), getpid());
    mkdir(outdir, 0755);

    char ts[64]; iso_now(ts, sizeof(ts));

    /* INSERT into shared queue — the queue worker daemon will pick it up */
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "INSERT INTO jobs (type,status,priority,input_path,output_path,source,api_key,created_at,max_retries) "
        "VALUES (?,?,?,?,?,?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st, 1, type, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, "queued", -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 3, priority);
    sqlite3_bind_text(st, 4, input, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 5, outdir, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 6, "api", -1, SQLITE_STATIC);
    if (req->auth_token[0]) sqlite3_bind_text(st, 7, req->auth_token, -1, SQLITE_STATIC);
    else sqlite3_bind_null(st, 7);
    sqlite3_bind_text(st, 8, ts, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 9, 3);
    sqlite3_step(st); sqlite3_finalize(st);
    int job_id = (int)sqlite3_last_insert_rowid(g_db);
    pthread_mutex_unlock(&g_db_mutex);

    /* Emit enqueue event */
    char evdata[256];
    snprintf(evdata, sizeof(evdata),
        "{\"source\":\"api\",\"priority\":%d}", priority);
    emit_event(job_id, "queued", evdata);

    /* Return immediately — the worker will execute the binary */
    http_resp_json(resp, 202,
        "{\"data\":{\"id\":%d,\"type\":\"%s\",\"status\":\"queued\",\"priority\":%d},"
        "\"meta\":{\"async\":true,\"poll\":\"/api/jobs/%d\",\"stream\":\"/api/events?job_id=%d\"}}",
        job_id, type, priority, job_id, job_id);
}

/* GET /api/jobs[/:id] */
static void handle_job_get(HttpResponse *resp, int job_id) {
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    if (job_id > 0) {
        sqlite3_prepare_v2(g_db,
            "SELECT id,type,status,priority,input_path,output_path,source,worker,"
            "attempts,created_at,claimed_at,completed_at,wall_ms,exit_code,error "
            "FROM jobs WHERE id=?", -1, &st, NULL);
        sqlite3_bind_int(st, 1, job_id);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *col_text[15];
            for (int i = 0; i < 15; i++)
                col_text[i] = sqlite3_column_text(st, i) ? (const char *)sqlite3_column_text(st, i) : "";

            http_resp_json(resp, 200,
                "{\"data\":{\"id\":%d,\"type\":\"%s\",\"status\":\"%s\",\"priority\":%d,"
                "\"input\":\"%s\",\"output\":\"%s\",\"source\":\"%s\",\"worker\":\"%s\","
                "\"attempts\":%d,\"created_at\":\"%s\",\"claimed_at\":\"%s\","
                "\"completed_at\":\"%s\",\"wall_ms\":%.1f,\"exit_code\":%d"
                "%s%s%s}}",
                sqlite3_column_int(st, 0), col_text[1], col_text[2], sqlite3_column_int(st, 3),
                col_text[4], col_text[5], col_text[6], col_text[7],
                sqlite3_column_int(st, 8), col_text[9], col_text[10],
                col_text[11], sqlite3_column_double(st, 12), sqlite3_column_int(st, 13),
                col_text[14][0] ? ",\"error\":\"" : "",
                col_text[14][0] ? col_text[14] : "",
                col_text[14][0] ? "\"" : "");
        } else {
            http_resp_json(resp, 404, "{\"error\":\"job not found\"}");
        }
        sqlite3_finalize(st);
    } else {
        sqlite3_prepare_v2(g_db,
            "SELECT id,type,status,priority,source,worker,attempts,"
            "created_at,completed_at,wall_ms,exit_code FROM jobs ORDER BY id DESC LIMIT 50",
            -1, &st, NULL);
        free(resp->buf); resp->buf = NULL; resp->buf_len = 0;
        FILE *mem = open_memstream(&resp->buf, &resp->buf_len);
        fprintf(mem, "{\"data\":[");
        int i = 0;
        while (sqlite3_step(st) == SQLITE_ROW) {
            if (i > 0) fprintf(mem, ",");
            const char *t = sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : "";
            const char *s = sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "";
            const char *src = sqlite3_column_text(st, 4) ? (const char *)sqlite3_column_text(st, 4) : "";
            const char *w = sqlite3_column_text(st, 5) ? (const char *)sqlite3_column_text(st, 5) : "";
            const char *cr = sqlite3_column_text(st, 7) ? (const char *)sqlite3_column_text(st, 7) : "";
            const char *co = sqlite3_column_text(st, 8) ? (const char *)sqlite3_column_text(st, 8) : "";
            fprintf(mem,
                "{\"id\":%d,\"type\":\"%s\",\"status\":\"%s\",\"priority\":%d,"
                "\"source\":\"%s\",\"worker\":\"%s\",\"attempts\":%d,"
                "\"created_at\":\"%s\",\"completed_at\":\"%s\","
                "\"wall_ms\":%.1f,\"exit_code\":%d}",
                sqlite3_column_int(st, 0), t, s, sqlite3_column_int(st, 3),
                src, w, sqlite3_column_int(st, 6), cr, co,
                sqlite3_column_double(st, 9), sqlite3_column_int(st, 10));
            i++;
        }
        fprintf(mem, "],\"meta\":{\"total\":%d}}", i);
        fclose(mem);
        resp->status = 200;
        sqlite3_finalize(st);
    }
    pthread_mutex_unlock(&g_db_mutex);
}

/* ── SSE: GET /api/events ─────────────────────────────────────────── */

static void handle_events_sse(HttpRequest *req, int fd) {
    /* Send SSE headers directly — this is a streaming response */
    const char *sse_hdr =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/event-stream\r\n"
        "Cache-Control: no-cache\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Connection: keep-alive\r\n\r\n";
    write(fd, sse_hdr, strlen(sse_hdr));

    /* Optional filter by job_id */
    char jid_buf[32]; int filter_job = 0;
    if (query_param(req->query, "job_id", jid_buf, sizeof(jid_buf)))
        filter_job = atoi(jid_buf);

    /* Start from last_event_id or latest */
    char since_buf[32]; int since = 0;
    if (query_param(req->query, "since", since_buf, sizeof(since_buf)))
        since = atoi(since_buf);

    while (g_running) {
        pthread_mutex_lock(&g_db_mutex);
        sqlite3_stmt *st;

        if (filter_job > 0) {
            sqlite3_prepare_v2(g_db,
                "SELECT id, job_id, event, data, created_at FROM events "
                "WHERE id > ? AND job_id = ? ORDER BY id ASC LIMIT 50",
                -1, &st, NULL);
            sqlite3_bind_int(st, 1, since);
            sqlite3_bind_int(st, 2, filter_job);
        } else {
            sqlite3_prepare_v2(g_db,
                "SELECT id, job_id, event, data, created_at FROM events "
                "WHERE id > ? ORDER BY id ASC LIMIT 50",
                -1, &st, NULL);
            sqlite3_bind_int(st, 1, since);
        }

        int sent = 0;
        while (sqlite3_step(st) == SQLITE_ROW) {
            int eid = sqlite3_column_int(st, 0);
            int jid = sqlite3_column_int(st, 1);
            const char *evt = sqlite3_column_text(st, 2) ?
                (const char *)sqlite3_column_text(st, 2) : "";
            const char *data = sqlite3_column_text(st, 3) ?
                (const char *)sqlite3_column_text(st, 3) : "{}";
            const char *ts = sqlite3_column_text(st, 4) ?
                (const char *)sqlite3_column_text(st, 4) : "";

            char msg[4096];
            int mlen = snprintf(msg, sizeof(msg),
                "id: %d\n"
                "event: %s\n"
                "data: {\"job_id\":%d,\"event\":\"%s\",\"data\":%s,\"ts\":\"%s\"}\n\n",
                eid, evt, jid, evt, data, ts);

            ssize_t wr = write(fd, msg, (size_t)mlen);
            if (wr <= 0) {
                /* Client disconnected */
                sqlite3_finalize(st);
                pthread_mutex_unlock(&g_db_mutex);
                return;
            }
            since = eid;
            sent++;
        }
        sqlite3_finalize(st);
        pthread_mutex_unlock(&g_db_mutex);

        if (sent == 0) usleep(500000);  /* 500ms poll */
    }
}

/* ── FTS5 search: GET /api/search?q=... ───────────────────────────── */

static void handle_search(HttpRequest *req, HttpResponse *resp) {
    char q[512];
    if (!query_param(req->query, "q", q, sizeof(q)) || !q[0]) {
        http_resp_json(resp, 400, "{\"error\":\"missing ?q=term\"}"); return;
    }

    int limit = 20;
    char lbuf[16];
    if (query_param(req->query, "limit", lbuf, sizeof(lbuf)))
        limit = atoi(lbuf);
    if (limit < 1) limit = 1;
    if (limit > 100) limit = 100;

    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "SELECT j.id, j.type, j.status, j.source, j.created_at, j.wall_ms, "
        "snippet(jobs_fts, 2, '<b>', '</b>', '...', 32) AS snippet, "
        "rank "
        "FROM jobs_fts f "
        "JOIN jobs j ON j.id = f.rowid "
        "WHERE jobs_fts MATCH ? "
        "ORDER BY rank "
        "LIMIT ?",
        -1, &st, NULL);
    sqlite3_bind_text(st, 1, q, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 2, limit);

    free(resp->buf); resp->buf = NULL; resp->buf_len = 0;
    FILE *mem = open_memstream(&resp->buf, &resp->buf_len);
    fprintf(mem, "{\"query\":\"%s\",\"results\":[", q);
    int i = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (i > 0) fprintf(mem, ",");
        const char *type_v = sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : "";
        const char *stat_v = sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "";
        const char *src_v = sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "";
        const char *cre_v = sqlite3_column_text(st, 4) ? (const char *)sqlite3_column_text(st, 4) : "";
        const char *snip = sqlite3_column_text(st, 6) ? (const char *)sqlite3_column_text(st, 6) : "";
        fprintf(mem,
            "{\"id\":%d,\"type\":\"%s\",\"status\":\"%s\",\"source\":\"%s\","
            "\"created_at\":\"%s\",\"wall_ms\":%.1f,\"snippet\":\"%s\",\"rank\":%.4f}",
            sqlite3_column_int(st, 0), type_v, stat_v, src_v, cre_v,
            sqlite3_column_double(st, 5), snip, sqlite3_column_double(st, 7));
        i++;
    }
    fprintf(mem, "],\"total\":%d}", i);
    fclose(mem);
    resp->status = 200;
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
}

/* ── Webhooks: POST/GET /api/webhooks ─────────────────────────────── */

static void handle_webhook_list(HttpResponse *resp) {
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "SELECT id, url, events, active, created_at FROM webhooks ORDER BY id",
        -1, &st, NULL);

    free(resp->buf); resp->buf = NULL; resp->buf_len = 0;
    FILE *mem = open_memstream(&resp->buf, &resp->buf_len);
    fprintf(mem, "{\"data\":[");
    int i = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (i > 0) fprintf(mem, ",");
        fprintf(mem,
            "{\"id\":%d,\"url\":\"%s\",\"events\":\"%s\",\"active\":%d,\"created_at\":\"%s\"}",
            sqlite3_column_int(st, 0),
            sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : "",
            sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "",
            sqlite3_column_int(st, 3),
            sqlite3_column_text(st, 4) ? (const char *)sqlite3_column_text(st, 4) : "");
        i++;
    }
    fprintf(mem, "],\"total\":%d}", i);
    fclose(mem);
    resp->status = 200;
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
}

static void handle_webhook_create(HttpRequest *req, HttpResponse *resp) {
    if (!req->body || req->body_len <= 0) {
        http_resp_json(resp, 400, "{\"error\":\"empty body\"}"); return;
    }

    char url[1024] = "", events[256] = "completed,failed";
    const char *p = req->body;
    const char *uf = strstr(p, "\"url\"");
    if (uf) {
        uf = strchr(uf + 5, '"');
        if (uf) { uf++;
            int i = 0; while (*uf && *uf != '"' && i < (int)sizeof(url) - 1) url[i++] = *uf++;
            url[i] = '\0';
        }
    }
    const char *ef = strstr(p, "\"events\"");
    if (ef) {
        ef = strchr(ef + 8, '"');
        if (ef) { ef++;
            int i = 0; while (*ef && *ef != '"' && i < (int)sizeof(events) - 1) events[i++] = *ef++;
            events[i] = '\0';
        }
    }

    if (!url[0]) { http_resp_json(resp, 400, "{\"error\":\"missing url\"}"); return; }

    char ts[64]; iso_now(ts, sizeof(ts));
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "INSERT INTO webhooks (url, events, active, created_at) VALUES (?,?,1,?)",
        -1, &st, NULL);
    sqlite3_bind_text(st, 1, url, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, events, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, ts, -1, SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    int wid = (int)sqlite3_last_insert_rowid(g_db);
    pthread_mutex_unlock(&g_db_mutex);

    http_resp_json(resp, 201,
        "{\"data\":{\"id\":%d,\"url\":\"%s\",\"events\":\"%s\",\"active\":true}}",
        wid, url, events);
}

/* Proxy: run a Bonfyre command and return its output as JSON. */
static void handle_proxy(HttpResponse *resp, const char *binary, int argc, char **argv) {
    char *run_argv[64];
    run_argv[0] = (char *)binary;
    for (int i = 0; i < argc && i < 62; i++) run_argv[i + 1] = argv[i];
    run_argv[argc + 1] = NULL;

    char result[MAX_RESULT];
    int rc = run_binary(binary, run_argv, result, sizeof(result));
    if (rc == 0) {
        const char *p = result; while (*p == ' ' || *p == '\n' || *p == '\t') p++;
        if (*p == '{' || *p == '[') {
            free(resp->buf);
            resp->buf_len = strlen(result);
            resp->buf = malloc(resp->buf_len + 1);
            memcpy(resp->buf, result, resp->buf_len + 1);
            resp->status = 200;
        } else {
            http_resp_json(resp, 200, "{\"data\":\"%.*s\"}",
                (int)(strlen(result) > 4096 ? 4096 : strlen(result)), result);
        }
    } else {
        http_resp_json(resp, 500, "{\"error\":\"binary failed\",\"code\":%d}", rc);
    }
}

/* GET /api/binaries/:name/... — proxy to any bonfyre binary */
static void handle_binary_proxy(HttpRequest *req, HttpResponse *resp,
                                const char *binary_name, int nseg,
                                char segs[][256]) {
    char bin[64]; snprintf(bin, sizeof(bin), "bonfyre-%s", binary_name);
    char *args[32]; int ac = 0;
    for (int i = 3; i < nseg && ac < 30; i++) args[ac++] = segs[i];
    if (req->body && req->body_len > 0 && ac < 30) args[ac++] = req->body;
    handle_proxy(resp, bin, ac, args);
}

/* ── Main router ──────────────────────────────────────────────────── */

static void route_request(HttpRequest *req, HttpResponse *resp) {
    char segs[MAX_PATH_SEGS][256];
    memset(segs, 0, sizeof(segs));
    int nseg = path_segments(req->path, segs, MAX_PATH_SEGS);

    /* CORS preflight */
    if (strcmp(req->method, "OPTIONS") == 0) {
        http_resp_json(resp, 204, ""); return;
    }

    /* /api/ routes */
    if (nseg >= 2 && strcmp(segs[0], "api") == 0) {
        const char *ep = segs[1];

        /* Auth check for non-health endpoints */
        if (strcmp(ep, "health") != 0) {
            if (req->auth_token[0] && !validate_api_key(req->auth_token)) {
                http_resp_json(resp, 401, "{\"error\":\"invalid API key\"}"); return;
            }
            if (!rate_limit_check(req->auth_token)) {
                http_resp_json(resp, 429,
                    "{\"error\":\"rate limit exceeded\",\"limit\":%d,\"window\":\"per minute\"}",
                    RL_CAPACITY); return;
            }
        }

        if (strcmp(ep, "health") == 0)  { handle_health(resp); return; }
        if (strcmp(ep, "status") == 0)  { handle_status(resp); return; }

        if (strcmp(ep, "upload") == 0 && strcmp(req->method, "POST") == 0) {
            handle_upload(req, resp); return;
        }

        if (strcmp(ep, "jobs") == 0) {
            if (strcmp(req->method, "POST") == 0) {
                handle_job_submit(req, resp); return;
            }
            int jid = nseg >= 3 ? atoi(segs[2]) : 0;
            handle_job_get(resp, jid); return;
        }

        /* SSE stream — handled specially (does not use HttpResponse) */
        if (strcmp(ep, "events") == 0 && strcmp(req->method, "GET") == 0) {
            handle_events_sse(req, resp->fd);
            /* SSE writes directly to fd, set status to -1 to skip normal send */
            resp->status = -1;
            return;
        }

        if (strcmp(ep, "search") == 0 && strcmp(req->method, "GET") == 0) {
            handle_search(req, resp); return;
        }

        if (strcmp(ep, "webhooks") == 0) {
            if (strcmp(req->method, "POST") == 0) {
                handle_webhook_create(req, resp); return;
            }
            handle_webhook_list(resp); return;
        }

        if (strcmp(ep, "binaries") == 0 && nseg >= 3) {
            handle_binary_proxy(req, resp, segs[2], nseg, segs); return;
        }

        http_resp_json(resp, 404, "{\"error\":\"unknown endpoint\"}");
        return;
    }

    if (serve_static(resp, req->path)) return;
    http_resp_json(resp, 404, "{\"error\":\"not found\"}");
}

/* ── Server ───────────────────────────────────────────────────────── */

static void *connection_handler(void *arg) {
    int fd = *(int *)arg; free(arg);
    double t0 = mono_sec();

    HttpRequest *req = malloc(sizeof(HttpRequest));
    if (!req) { close(fd); atomic_fetch_sub(&g_thread_count, 1); return NULL; }
    if (parse_http_request(fd, req) != 0) {
        free(req); close(fd); atomic_fetch_sub(&g_thread_count, 1); return NULL;
    }

    HttpResponse resp;
    http_resp_init(&resp, fd);
    route_request(req, &resp);

    if (resp.status >= 0) http_resp_send(&resp);

    double wall_ms = (mono_sec() - t0) * 1000.0;
    log_request(req->method, req->path, req->auth_token, resp.status, wall_ms);

    http_resp_free(&resp);
    free_request(req);
    free(req);
    close(fd);
    atomic_fetch_sub(&g_thread_count, 1);
    return NULL;
}

static int start_server(int port) {
    signal(SIGPIPE, SIG_IGN);

    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); return -1; }
    int opt = 1; setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons((uint16_t)port);

    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(sfd); return -1;
    }
    if (listen(sfd, 128) < 0) { perror("listen"); close(sfd); return -1; }

    fprintf(stderr,
        "[bonfyre-api] v%s listening on http://0.0.0.0:%d\n"
        "[bonfyre-api] static: %s\n"
        "[bonfyre-api] uploads: %s\n"
        "[bonfyre-api] endpoints:\n"
        "  GET  /api/health              Health check\n"
        "  GET  /api/status              System dashboard\n"
        "  POST /api/upload              File upload\n"
        "  POST /api/jobs                Submit async job\n"
        "  GET  /api/jobs[/:id]          Job status/list\n"
        "  GET  /api/events              SSE stream (real-time)\n"
        "  GET  /api/search?q=term       FTS5 full-text search\n"
        "  POST /api/webhooks            Register webhook\n"
        "  GET  /api/webhooks            List webhooks\n"
        "  *    /api/binaries/:name/...  Proxy to any binary\n"
        "  GET  /*                       Static files / SPA\n",
        VERSION, port,
        g_static_dir[0] ? g_static_dir : "(none)",
        g_upload_dir);

    while (g_running) {
        struct sockaddr_in ca; socklen_t cl = sizeof(ca);
        int cfd = accept(sfd, (struct sockaddr *)&ca, &cl);
        if (cfd < 0) { if (!g_running) break; continue; }

        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        if (atomic_load(&g_thread_count) >= MAX_THREADS) {
            const char *busy = "HTTP/1.1 503 Service Unavailable\r\n"
                "Content-Length: 0\r\nConnection: close\r\n\r\n";
            write(cfd, busy, strlen(busy));
            close(cfd);
            continue;
        }
        atomic_fetch_add(&g_thread_count, 1);

        int *pfd = malloc(sizeof(int)); *pfd = cfd;
        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_attr_setstacksize(&attr, THREAD_STACK);
        pthread_create(&tid, &attr, connection_handler, pfd);
        pthread_attr_destroy(&attr);
    }
    close(sfd);
    return 0;
}

static void handle_signal(int sig) { (void)sig; g_running = 0; }

/* ── CLI ──────────────────────────────────────────────────────────── */

static const char *arg_get(int argc, char **argv, const char *flag) {
    for (int i = 0; i < argc - 1; i++)
        if (strcmp(argv[i], flag) == 0) return argv[i + 1];
    return NULL;
}

static void usage(void) {
    fprintf(stderr,
        "BonfyreAPI v%s — Async HTTP gateway with SSE, search, webhooks\n\n"
        "Usage:\n"
        "  bonfyre-api serve [--port 8080] [--db FILE] [--static DIR] [--uploads DIR]\n"
        "  bonfyre-api status\n"
        "  bonfyre-api layer get <artifact_id> [--root DIR]\n"
        "  bonfyre-api layer lineage <artifact_id> [--root DIR]\n"
        "  bonfyre-api layer compat <artifact_a> <artifact_b> [--root DIR]\n"
        "  bonfyre-api layer compose <artifact_a> <artifact_b> [--dry-run] [--root DIR]\n\n"
        "Shares SQLite WAL database with BonfyreQueue (default: ~/.local/share/bonfyre/queue.db).\n"
        "Jobs are enqueued asynchronously — run `bonfyre-queue work` to process them.\n",
        VERSION);
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(); return 1; }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    const char *cmd = NULL;
    for (int i = 1; i < argc; i++) {
        if (argv[i][0] != '-') { cmd = argv[i]; break; }
        if (strcmp(argv[i], "--db") == 0 || strcmp(argv[i], "--port") == 0 ||
            strcmp(argv[i], "--static") == 0 || strcmp(argv[i], "--uploads") == 0) { i++; continue; }
    }
    if (!cmd) { usage(); return 1; }

    if (strcmp(cmd, "layer") == 0) {
        return delegate_layer_api(argc, argv);
    }

    if (strcmp(cmd, "serve") == 0) {
        int port = 8080;
        const char *p = arg_get(argc, argv, "--port");
        if (p) port = atoi(p);

        const char *db_path = arg_get(argc, argv, "--db");
        if (!db_path) db_path = default_db();

        const char *sd = arg_get(argc, argv, "--static");
        if (sd) strncpy(g_static_dir, sd, sizeof(g_static_dir) - 1);

        const char *ud = arg_get(argc, argv, "--uploads");
        if (!ud) {
            const char *h = getenv("HOME");
            snprintf(g_upload_dir, sizeof(g_upload_dir), "%s/.local/share/bonfyre/uploads", h ? h : ".");
        } else {
            strncpy(g_upload_dir, ud, sizeof(g_upload_dir) - 1);
        }
        mkdir(g_upload_dir, 0755);

        g_db = open_db(db_path);
        if (!g_db) return 1;

        int rc = start_server(port);
        sqlite3_close(g_db);
        return rc;

    } else if (strcmp(cmd, "status") == 0) {
        printf("BonfyreAPI v%s\n", VERSION);
        printf("Use 'bonfyre-api serve' to start, then GET /api/status for live dashboard.\n");
        return 0;

    } else {
        usage(); return 1;
    }
}
