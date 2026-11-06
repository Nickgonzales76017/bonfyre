/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
/* The POSIX pin above hides BSD interfaces on Darwin -- strcasecmp, ru_maxrss,
 * MSG_DONTWAIT, INADDR_LOOPBACK -- so this file did not compile on the macOS
 * control plane at all. _DARWIN_C_SOURCE restores them without loosening the
 * POSIX baseline the Linux deploy host needs. */
#ifdef __APPLE__
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE 1
#endif
#endif

/*
 * BonfyreTel — FreeSWITCH Event Socket telephony adapter.
 *
 * Connects to FreeSWITCH via Event Socket (plain TCP on localhost:8021),
 * listens for call/SMS events, and triggers Bonfyre pipeline binaries.
 *
 * No Twilio dependency. Works with any SIP trunk (Telnyx, Bandwidth, etc.)
 *
 * Usage:
 *   bonfyre-tel listen  [--host HOST] [--port PORT] [--password PW] [--db FILE]
 *   bonfyre-tel send-sms --from NUM --to NUM --body TEXT [--db FILE]
 *   bonfyre-tel send-mms --from NUM --to NUM --body TEXT --media FILE [--db FILE]
 *   bonfyre-tel call     --from NUM --to NUM [--record] [--db FILE]
 *   bonfyre-tel hangup   --uuid UUID
 *   bonfyre-tel status   [--db FILE]
 *   bonfyre-tel version
 */
#include <sys/time.h>  /* struct timeval; implicit on macOS, not on glibc */
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
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
#include "../../../lib/libbonfyre/include/bf_help.h"
#define VERSION         "1.0.0"
#define DEFAULT_HOST    "127.0.0.1"
#define DEFAULT_PORT    8021
#define DEFAULT_PASS    "ClueCon"
#define BUF_SIZE        (64 * 1024)
#define MAX_HEADERS     128
#define RECV_TIMEOUT_S  1

static volatile int g_running = 1;

static const char *layeros_binary(void) {
    return "layeros/bin/bonfyre-layeros";
}

/* ── SQLite schema ─────────────────────────────────────────────────── */

static const char *SCHEMA_SQL =
    "CREATE TABLE IF NOT EXISTS calls ("
    "  id         INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  uuid       TEXT NOT NULL,"
    "  direction  TEXT NOT NULL,"   /* inbound | outbound */
    "  caller     TEXT,"
    "  callee     TEXT,"
    "  started_at TEXT NOT NULL,"
    "  ended_at   TEXT,"
    "  duration_s REAL,"
    "  recording  TEXT,"           /* path to .wav */
    "  pipeline   TEXT,"           /* pipeline job id */
    "  status     TEXT DEFAULT 'active'"
    ");"
    "CREATE TABLE IF NOT EXISTS messages ("
    "  id          INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  direction   TEXT NOT NULL,"
    "  from_number TEXT,"
    "  to_number   TEXT,"
    "  body        TEXT,"
    "  media_url   TEXT,"
    "  sent_at     TEXT NOT NULL,"
    "  status      TEXT DEFAULT 'sent'"
    ");"
    "CREATE TABLE IF NOT EXISTS verify_codes ("
    "  id         INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  phone      TEXT NOT NULL,"
    "  code       TEXT NOT NULL,"
    "  created_at TEXT NOT NULL,"
    "  expires_at TEXT NOT NULL,"
    "  verified   INTEGER DEFAULT 0,"
    "  attempts   INTEGER DEFAULT 0"
    ");";

/* ── Utility ───────────────────────────────────────────────────────── */

static void iso_timestamp(char *buf, size_t sz) { bf_iso_timestamp(buf, sz); }

static const char *arg_get(int argc, char **argv, const char *flag) {
    for (int i = 1; i < argc - 1; i++)
        if (strcmp(argv[i], flag) == 0) return argv[i + 1];
    return NULL;
}

static int arg_has(int argc, char **argv, const char *flag) {
    for (int i = 1; i < argc; i++)
        if (strcmp(argv[i], flag) == 0) return 1;
    return 0;
}

static const char *default_db(void) {
    static char path[1024];
    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    snprintf(path, sizeof(path), "%s/.local/share/bonfyre", home);
    mkdir(path, 0755);
    snprintf(path, sizeof(path), "%s/.local/share/bonfyre/tel.db", home);
    return path;
}

static sqlite3 *open_db(const char *path) {
    sqlite3 *db = NULL;
    /* Durable tier -- calls, messages and verification codes: a record of what was said and sent. */
    if (bf_sqlite3_open_tier(path, &db, BF_SQLITE_DURABLE) != SQLITE_OK) {
        fprintf(stderr, "tel: cannot open %s: %s\n", path, sqlite3_errmsg(db));
        sqlite3_close(db);
        return NULL;
    }
    char *err = NULL;
    sqlite3_exec(db, SCHEMA_SQL, NULL, NULL, &err);
    if (err) { fprintf(stderr, "tel: schema: %s\n", err); sqlite3_free(err); }
    return db;
}

static void sigint_handler(int sig) {
    (void)sig;
    g_running = 0;
}

/* ── Fork + exec (same pattern as BonfyreMediaPrep) ───────────────── */

static int run_process(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) {
        execvp(argv[0], argv);
        perror("execvp");
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) { perror("waitpid"); return 1; }
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return 1;
}

static int delegate_layeros_tel(int argc, char **argv) {
    char *exec_argv[16];
    int n = 0;
    exec_argv[n++] = (char *)layeros_binary();
    const char *root = NULL;
    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--root") == 0) {
            root = argv[i + 1];
            break;
        }
    }
    if (root) {
        exec_argv[n++] = "--root";
        exec_argv[n++] = (char *)root;
    }
    exec_argv[n++] = "tel";
    exec_argv[n++] = argv[2];
    exec_argv[n] = NULL;
    return run_process(exec_argv);
}

/* Non-blocking fork — don't wait for child (pipeline may take minutes) */
static pid_t run_process_async(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return -1; }
    if (pid == 0) {
        setsid(); /* detach from parent session */
        execvp(argv[0], argv);
        perror("execvp");
        _exit(127);
    }
    return pid;
}

/* ── ESL connection ────────────────────────────────────────────────── */

typedef struct {
    int    fd;
    char   buf[BUF_SIZE];
    size_t buf_len;
    size_t scan_off;  /* P4: avoid re-scanning for \n\n from byte 0 */
} EslConn;

static int esl_connect(EslConn *c, const char *host, int port) {
    memset(c, 0, sizeof(*c));

    struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
    struct addrinfo *res = NULL;
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);

    if (getaddrinfo(host, port_str, &hints, &res) != 0 || !res) {
        fprintf(stderr, "tel: cannot resolve %s:%d\n", host, port);
        return -1;
    }

    c->fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (c->fd < 0) {
        perror("socket");
        freeaddrinfo(res);
        return -1;
    }

    if (connect(c->fd, res->ai_addr, res->ai_addrlen) < 0) {
        fprintf(stderr, "tel: cannot connect to FreeSWITCH at %s:%d: %s\n",
                host, port, strerror(errno));
        close(c->fd);
        c->fd = -1;
        freeaddrinfo(res);
        return -1;
    }

    freeaddrinfo(res);

    /* P4: TCP_NODELAY eliminates 40ms Nagle delay on small ESL commands */
    int one = 1;
    setsockopt(c->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    setsockopt(c->fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));

    /* Set receive timeout so event loop can check g_running */
    struct timeval tv = {.tv_sec = RECV_TIMEOUT_S, .tv_usec = 0};
    setsockopt(c->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    return 0;
}

static void esl_close(EslConn *c) {
    if (c->fd >= 0) { close(c->fd); c->fd = -1; }
}

/* Send a raw ESL command (terminated with \n\n) */
static int esl_send(EslConn *c, const char *fmt, ...) {
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(cmd, sizeof(cmd) - 2, fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= sizeof(cmd) - 2) return -1;
    /* Ensure double newline terminator */
    if (n > 0 && cmd[n - 1] != '\n') cmd[n++] = '\n';
    cmd[n++] = '\n';
    cmd[n] = '\0';

    ssize_t sent = 0;
    while (sent < n) {
        ssize_t w = write(c->fd, cmd + sent, (size_t)(n - sent));
        if (w < 0) {
            if (errno == EINTR) continue;
            perror("write");
            return -1;
        }
        sent += w;
    }
    return 0;
}

/* Read until we get a full ESL event (double newline delimited).
 * Returns number of bytes in event, or 0 on timeout, -1 on error. */
static int esl_recv_event(EslConn *c, char *out, size_t out_sz) {
    for (;;) {
        /* P4: scan only from where we left off, not byte 0 */
        char *start = c->buf + (c->scan_off > 0 ? c->scan_off - 1 : 0);
        char *end = strstr(start, "\n\n");
        if (end) {
            size_t event_len = (size_t)(end - c->buf + 2);
            if (event_len >= out_sz) event_len = out_sz - 1;
            memcpy(out, c->buf, event_len);
            out[event_len] = '\0';
            /* Shift remaining data */
            size_t remain = c->buf_len - (size_t)(end - c->buf + 2);
            if (remain > 0) memmove(c->buf, end + 2, remain);
            c->buf_len = remain;
            c->buf[c->buf_len] = '\0';
            c->scan_off = 0;
            return (int)event_len;
        }
        c->scan_off = c->buf_len > 0 ? c->buf_len : 0;

        if (c->buf_len >= BUF_SIZE - 1) {
            /* Buffer full without complete event — discard */
            c->buf_len = 0;
            c->buf[0] = '\0';
            c->scan_off = 0;
        }

        ssize_t n = recv(c->fd, c->buf + c->buf_len, BUF_SIZE - 1 - c->buf_len, 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return 0; /* timeout */
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1; /* connection closed */
        c->buf_len += (size_t)n;
        c->buf[c->buf_len] = '\0';
    }
}

/* Reads the switch's answer to an api command: the header block, then the
 * body its Content-Length promises, which is where "+OK ..." or "-ERR ..."
 * is. A command such as originate does not answer until the call is set up,
 * so this waits through several receive timeouts before giving up. Returns
 * the body's length, 0 if no answer came in time, -1 on error. */
static int esl_api_reply(EslConn *c, char *body, size_t body_sz) {
    char headers[BUF_SIZE];
    int waited = 0;
    for (;;) {
        int n = esl_recv_event(c, headers, sizeof(headers));
        if (n < 0) return -1;
        if (n == 0) { if (++waited > 20) return 0; continue; }
        if (strstr(headers, "Content-Type: api/response")) break;
    }
    const char *length_header = strstr(headers, "Content-Length:");
    size_t want = length_header ? (size_t)strtoul(length_header + 15, NULL, 10) : 0;
    if (want >= body_sz) want = body_sz - 1;
    waited = 0;
    while (c->buf_len < want) {
        ssize_t n = recv(c->fd, c->buf + c->buf_len, BUF_SIZE - 1 - c->buf_len, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            if ((errno == EAGAIN || errno == EWOULDBLOCK) && ++waited <= 20) continue;
            return -1;
        }
        if (n == 0) return -1;
        c->buf_len += (size_t)n;
        c->buf[c->buf_len] = '\0';
    }
    memcpy(body, c->buf, want);
    body[want] = '\0';
    if (c->buf_len > want) memmove(c->buf, c->buf + want, c->buf_len - want);
    c->buf_len -= want;
    c->buf[c->buf_len] = '\0';
    c->scan_off = 0;
    return (int)want;
}

/* ── ESL event parsing ─────────────────────────────────────────────── */

typedef struct {
    char key[128];
    char value[1024];
} EslHeader;

typedef struct {
    EslHeader headers[MAX_HEADERS];
    int       count;
} EslEvent;

static void esl_parse_event(const char *raw, EslEvent *evt) {
    evt->count = 0;
    const char *p = raw;
    while (*p && evt->count < MAX_HEADERS) {
        const char *colon = strchr(p, ':');
        const char *nl = strchr(p, '\n');
        if (!colon || (nl && nl < colon)) {
            if (nl) { p = nl + 1; continue; }
            break;
        }
        size_t klen = (size_t)(colon - p);
        if (klen >= sizeof(evt->headers[0].key)) klen = sizeof(evt->headers[0].key) - 1;
        memcpy(evt->headers[evt->count].key, p, klen);
        evt->headers[evt->count].key[klen] = '\0';

        const char *vstart = colon + 1;
        while (*vstart == ' ') vstart++;
        const char *vend = nl ? nl : vstart + strlen(vstart);
        size_t vlen = (size_t)(vend - vstart);
        if (vlen >= sizeof(evt->headers[0].value)) vlen = sizeof(evt->headers[0].value) - 1;
        memcpy(evt->headers[evt->count].value, vstart, vlen);
        evt->headers[evt->count].value[vlen] = '\0';

        evt->count++;
        p = nl ? nl + 1 : vend;
    }
}

static const char *esl_get(const EslEvent *evt, const char *key) {
    for (int i = 0; i < evt->count; i++)
        if (strcasecmp(evt->headers[i].key, key) == 0)
            return evt->headers[i].value;
    return NULL;
}

/* ── Event handlers ────────────────────────────────────────────────── */

static void handle_recording_done(sqlite3 *db, const EslEvent *evt) {
    const char *uuid       = esl_get(evt, "Unique-ID");
    const char *rec_path   = esl_get(evt, "variable_record_file_path");
    const char *caller     = esl_get(evt, "Caller-Caller-ID-Number");
    const char *callee     = esl_get(evt, "Caller-Destination-Number");
    const char *duration   = esl_get(evt, "variable_record_seconds");

    if (!rec_path || !uuid) return;

    char ts[32];
    iso_timestamp(ts, sizeof(ts));
    fprintf(stderr, "tel: [%s] recording done: %s (%s → %s, %ss)\n",
            ts, rec_path, caller ? caller : "?", callee ? callee : "?",
            duration ? duration : "?");

    /* Log to DB */
    if (db) {
        const char *sql = "INSERT INTO calls "
            "(uuid, direction, caller, callee, started_at, recording, status) "
            "VALUES (?1, 'inbound', ?2, ?3, ?4, ?5, 'recorded')";
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, uuid, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, caller ? caller : "", -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 3, callee ? callee : "", -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 4, ts, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 5, rec_path, -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }

    /* Trigger Bonfyre pipeline asynchronously */
    {
        fprintf(stderr, "tel: triggering pipeline for %s\n", rec_path);
        char *const argv[] = {
            "bonfyre-pipeline", "run", (char *)rec_path, NULL
        };
        run_process_async(argv);
    }
}

static void handle_sms_recv(sqlite3 *db, const EslEvent *evt) {
    const char *from = esl_get(evt, "from");
    const char *to   = esl_get(evt, "to");
    const char *body = esl_get(evt, "body");

    if (!from || !to) return;

    char ts[32];
    iso_timestamp(ts, sizeof(ts));
    fprintf(stderr, "tel: [%s] SMS received: %s → %s: %.80s\n",
            ts, from, to, body ? body : "(empty)");

    /* Log to DB */
    if (db) {
        const char *sql = "INSERT INTO messages "
            "(direction, from_number, to_number, body, sent_at, status) "
            "VALUES ('inbound', ?1, ?2, ?3, ?4, 'received')";
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, from, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, to, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 3, body ? body : "", -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 4, ts, -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }

    /* Ingest the message. The ingest organ takes a file and a directory, so
     * the body is written to one under BONFYRE_TEL_INBOUND (default
     * ~/.local/share/bonfyre/tel-inbound) and ingested beside it. The call
     * that stood here passed flags the ingest organ has never had. */
    if (body && body[0]) {
        char dir[1024], file[1100], out[1100];
        const char *base = getenv("BONFYRE_TEL_INBOUND"), *home = getenv("HOME");
        if (base && base[0]) snprintf(dir, sizeof(dir), "%s", base);
        else snprintf(dir, sizeof(dir), "%s/.local/share/bonfyre/tel-inbound", home ? home : "/tmp");
        long long row = db ? (long long)sqlite3_last_insert_rowid(db) : 0;
        snprintf(out, sizeof(out), "%s/message-%lld-%ld", dir, row, (long)time(NULL));
        snprintf(file, sizeof(file), "%s/body.txt", out);
        if (bf_ensure_dir(out) != 0) {
            fprintf(stderr, "tel: cannot create %s; the message is logged but was not ingested\n", out);
            return;
        }
        FILE *f = fopen(file, "w");
        if (!f || fprintf(f, "%s\n", body) < 0 || fclose(f) != 0) {
            fprintf(stderr, "tel: cannot write %s; the message is logged but was not ingested\n", file);
            return;
        }
        static char file_arg[1100], out_arg[1100];
        snprintf(file_arg, sizeof(file_arg), "%s", file);
        snprintf(out_arg, sizeof(out_arg), "%s", out);
        char *const argv[] = { "bonfyre-ingest", file_arg, out_arg, "--type", "text", NULL };
        run_process_async(argv);
    }
}

static void handle_channel_hangup(sqlite3 *db, const EslEvent *evt) {
    const char *uuid     = esl_get(evt, "Unique-ID");
    const char *duration = esl_get(evt, "variable_billsec");

    if (!uuid) return;

    char ts[32];
    iso_timestamp(ts, sizeof(ts));
    fprintf(stderr, "tel: [%s] hangup: %s (duration: %ss)\n",
            ts, uuid, duration ? duration : "?");

    if (db) {
        const char *sql =
            "UPDATE calls SET ended_at=?1, duration_s=?2, status='completed' "
            "WHERE uuid=?3 AND ended_at IS NULL";
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, ts, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, duration ? duration : "0", -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 3, uuid, -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }
}

/* ── ESL authenticate + subscribe ──────────────────────────────────── */

static int esl_auth(EslConn *c, const char *password) {
    /* FreeSWITCH sends "Content-Type: auth/request" on connect */
    char buf[BUF_SIZE];
    int n = esl_recv_event(c, buf, sizeof(buf));
    if (n <= 0) {
        fprintf(stderr, "tel: no auth prompt from FreeSWITCH\n");
        return -1;
    }
    if (!strstr(buf, "auth/request")) {
        fprintf(stderr, "tel: unexpected greeting: %.80s\n", buf);
        return -1;
    }

    esl_send(c, "auth %s", password);

    n = esl_recv_event(c, buf, sizeof(buf));
    if (n <= 0 || !strstr(buf, "command/reply")) {
        fprintf(stderr, "tel: auth failed\n");
        return -1;
    }
    if (strstr(buf, "-ERR")) {
        fprintf(stderr, "tel: auth rejected (bad password?)\n");
        return -1;
    }

    fprintf(stderr, "tel: authenticated with FreeSWITCH\n");
    return 0;
}

static int esl_subscribe(EslConn *c) {
    /* Subscribe to events we care about */
    esl_send(c, "event plain CHANNEL_EXECUTE_COMPLETE CHANNEL_HANGUP_COMPLETE");

    char buf[BUF_SIZE];
    int n = esl_recv_event(c, buf, sizeof(buf));
    if (n <= 0 || strstr(buf, "-ERR")) {
        fprintf(stderr, "tel: event subscription failed\n");
        return -1;
    }

    /* Also subscribe to custom SMS events */
    esl_send(c, "event plain CUSTOM sms::recv");
    n = esl_recv_event(c, buf, sizeof(buf));
    /* sms::recv may not exist until SMS traffic arrives — not fatal */

    fprintf(stderr, "tel: subscribed to call + SMS events\n");
    return 0;
}

/* ── Commands ──────────────────────────────────────────────────────── */

/* Start bonfyre-transcribe serve in the background if the socket isn't already present.
 * The daemon loads the Whisper model once; all pipeline calls reuse it instead of
 * paying the ~800 ms model-load cost per recording. */
static void start_transcribe_daemon_bg(const char *model) {
    const char *sock_path = getenv("BONFYRE_TRANSCRIBE_SOCKET");
    if (!sock_path) sock_path = "/tmp/bonfyre-transcribe.sock";

    if (access(sock_path, F_OK) == 0) {
        fprintf(stderr, "tel: transcribe daemon already running at %s\n", sock_path);
        return;
    }

    fprintf(stderr, "tel: starting bonfyre-transcribe serve --model %s\n", model);
    char *const argv[] = {
        "bonfyre-transcribe", "serve", "--model", (char *)model, NULL
    };
    run_process_async(argv);
    /* Daemon starts in background; the model loads asynchronously.
     * First bonfyre-pipeline call will retry connect until the socket appears. */
}

static int cmd_listen(const char *host, int port, const char *password,
                      sqlite3 *db) {
    EslConn conn;

    /* Start the transcribe daemon eagerly so the model is warm before first call */
    start_transcribe_daemon_bg("base");

    fprintf(stderr, "tel: connecting to FreeSWITCH at %s:%d...\n", host, port);

    if (esl_connect(&conn, host, port) < 0) return 1;
    if (esl_auth(&conn, password) < 0) { esl_close(&conn); return 1; }
    if (esl_subscribe(&conn) < 0) { esl_close(&conn); return 1; }

    fprintf(stderr, "tel: listening for events (Ctrl-C to stop)...\n");

    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);
    signal(SIGCHLD, SIG_IGN); /* auto-reap async children */

    char event_buf[BUF_SIZE];
    while (g_running) {
        int n = esl_recv_event(&conn, event_buf, sizeof(event_buf));
        if (n < 0) {
            fprintf(stderr, "tel: connection lost, reconnecting in 3s...\n");
            esl_close(&conn);
            sleep(3);
            if (esl_connect(&conn, host, port) < 0) continue;
            if (esl_auth(&conn, password) < 0) { esl_close(&conn); continue; }
            if (esl_subscribe(&conn) < 0) { esl_close(&conn); continue; }
            continue;
        }
        if (n == 0) continue; /* timeout — check g_running */

        EslEvent evt;
        esl_parse_event(event_buf, &evt);

        const char *event_name = esl_get(&evt, "Event-Name");
        if (!event_name) continue;

        if (strcmp(event_name, "CHANNEL_EXECUTE_COMPLETE") == 0) {
            const char *app = esl_get(&evt, "Application");
            if (app && strcmp(app, "record") == 0) {
                handle_recording_done(db, &evt);
            }
        } else if (strcmp(event_name, "CHANNEL_HANGUP_COMPLETE") == 0) {
            handle_channel_hangup(db, &evt);
        } else if (strcmp(event_name, "CUSTOM") == 0) {
            const char *subclass = esl_get(&evt, "Event-Subclass");
            if (subclass && strcmp(subclass, "sms::recv") == 0) {
                handle_sms_recv(db, &evt);
            }
        }
    }

    fprintf(stderr, "\ntel: shutting down\n");
    esl_close(&conn);
    return 0;
}

static int cmd_send_sms(const char *host, int port, const char *password,
                        const char *from, const char *to, const char *body,
                        sqlite3 *db) {
    if (!from || !to || !body) {
        fprintf(stderr, "tel: send-sms requires --from, --to, --body\n");
        return 1;
    }

    EslConn conn;
    if (esl_connect(&conn, host, port) < 0) return 1;
    if (esl_auth(&conn, password) < 0) { esl_close(&conn); return 1; }

    /* Use FreeSWITCH chat API to send SMS via SIP MESSAGE */
    esl_send(&conn,
        "api chat sip|%s|%s|%s|text/plain",
        from, to, body);

    char buf[BUF_SIZE];
    int n = esl_recv_event(&conn, buf, sizeof(buf));
    int ok = (n > 0 && !strstr(buf, "-ERR"));

    if (ok)
        fprintf(stderr, "tel: SMS sent: %s → %s\n", from, to);
    else
        fprintf(stderr, "tel: SMS send failed\n");

    /* Log */
    if (db) {
        char ts[32];
        iso_timestamp(ts, sizeof(ts));
        const char *sql = "INSERT INTO messages "
            "(direction, from_number, to_number, body, sent_at, status) "
            "VALUES ('outbound', ?1, ?2, ?3, ?4, ?5)";
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, from, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, to, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 3, body, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 4, ts, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 5, ok ? "sent" : "failed", -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }

    esl_close(&conn);
    return ok ? 0 : 1;
}

static int cmd_send_mms(const char *host, int port, const char *password,
                        const char *from, const char *to, const char *body,
                        const char *media, sqlite3 *db) {
    (void)host; (void)port; (void)password; /* MMS uses carrier REST API */
    if (!from || !to || !media) {
        fprintf(stderr, "tel: send-mms requires --from, --to, --media\n");
        return 1;
    }

    /*
     * MMS via SIP is carrier-dependent. Most SIP trunks (Telnyx, Bandwidth)
     * expose MMS via their REST API. We shell out to curl for portability.
     * The BONFYRE_TEL_MMS_ENDPOINT env var should point to the carrier's
     * MMS endpoint (e.g. https://api.telnyx.com/v2/messages).
     */
    const char *endpoint = getenv("BONFYRE_TEL_MMS_ENDPOINT");
    const char *api_key  = getenv("BONFYRE_TEL_API_KEY");

    if (!endpoint || !api_key) {
        fprintf(stderr, "tel: MMS requires BONFYRE_TEL_MMS_ENDPOINT and "
                "BONFYRE_TEL_API_KEY environment variables\n");
        return 1;
    }

    /* Build JSON payload */
    char payload[4096];
    snprintf(payload, sizeof(payload),
        "{\"from\":\"%s\",\"to\":\"%s\",\"text\":\"%s\","
        "\"media_urls\":[\"%s\"]}",
        from, to, body ? body : "", media);

    char auth_header[256];
    snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s", api_key);

    char *const argv[] = {
        "curl", "-s", "-X", "POST",
        "-H", "Content-Type: application/json",
        "-H", auth_header,
        "-d", payload,
        (char *)endpoint,
        NULL
    };

    fprintf(stderr, "tel: sending MMS %s → %s (media: %s)\n", from, to, media);
    int rc = run_process(argv);

    if (db) {
        char ts[32];
        iso_timestamp(ts, sizeof(ts));
        const char *sql = "INSERT INTO messages "
            "(direction, from_number, to_number, body, media_url, sent_at, status) "
            "VALUES ('outbound', ?1, ?2, ?3, ?4, ?5, ?6)";
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, from, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, to, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 3, body ? body : "", -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 4, media, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 5, ts, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 6, rc == 0 ? "sent" : "failed", -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }

    return rc;
}

static int cmd_call(const char *host, int port, const char *password,
                    const char *from, const char *to, int do_record,
                    sqlite3 *db) {
    if (!from || !to) {
        fprintf(stderr, "tel: call requires --from and --to\n");
        return 1;
    }

    EslConn conn;
    if (esl_connect(&conn, host, port) < 0) return 1;
    if (esl_auth(&conn, password) < 0) { esl_close(&conn); return 1; }

    /*
     * Originate a call via FreeSWITCH.
     * The dialstring uses the default SIP profile — carrier config lives
     * in FreeSWITCH sip_profiles/, not in this binary.
     */
    /* Where the call goes is the switch's routing, named by --route or
     * BONFYRE_TEL_ROUTE with {to} standing for the number. Left unset it is
     * the outside trunk; a route such as loopback/{to}/default keeps the call
     * inside the switch, which is how a switch with no trunk is exercised. */
    char dial[512];
    const char *route = getenv("BONFYRE_TEL_ROUTE");
    const char *slot = route && *route ? strstr(route, "{to}") : NULL;
    if (slot)
        snprintf(dial, sizeof(dial), "%.*s%s%s", (int)(slot - route), route, to, slot + 4);
    else if (route && *route) {
        fprintf(stderr, "tel: BONFYRE_TEL_ROUTE must contain {to}\n");
        esl_close(&conn);
        return 1;
    } else
        snprintf(dial, sizeof(dial), "sofia/external/%s@${default_provider}", to);

    if (do_record) {
        esl_send(&conn,
            "api originate {origination_caller_id_number=%s,"
            "execute_on_answer='record_session $${recordings_dir}/%s_%s_${strftime(%%Y%%m%%d_%%H%%M%%S)}.wav'}"
            "%s &park()",
            from, from, to, dial);
    } else {
        esl_send(&conn,
            "api originate {origination_caller_id_number=%s}"
            "%s &park()",
            from, dial);
    }

    char buf[BUF_SIZE];
    int n = esl_api_reply(&conn, buf, sizeof(buf));
    int ok = (n > 0 && !strncmp(buf, "+OK", 3));

    /* The switch answers "+OK <uuid>" for a call it set up. That uuid is the
     * call's identity; it was being thrown away and the word "pending"
     * stored in its place, which left hangup nothing to name. */
    char uuid[64] = "";
    const char *plus = ok ? strstr(buf, "+OK ") : NULL;
    if (plus) {
        size_t i = 0;
        for (plus += 4; *plus && *plus != '\n' && *plus != '\r' && *plus != ' ' && i + 1 < sizeof(uuid); plus++)
            uuid[i++] = *plus;
        uuid[i] = '\0';
    }
    if (ok && !uuid[0]) ok = 0;

    if (ok) {
        fprintf(stderr, "tel: call originated: %s → %s%s\n",
                from, to, do_record ? " (recording)" : "");
        printf("%s\n", uuid);
    } else
        fprintf(stderr, "tel: call failed: %.200s\n", n > 0 ? buf : "(no response)");

    if (db) {
        char ts[32];
        iso_timestamp(ts, sizeof(ts));
        const char *sql = "INSERT INTO calls "
            "(uuid, direction, caller, callee, started_at, status) "
            "VALUES (?5, 'outbound', ?1, ?2, ?3, ?4)";
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 5, uuid[0] ? uuid : "none", -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 1, from, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, to, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 3, ts, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 4, ok ? "active" : "failed", -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }

    esl_close(&conn);
    return ok ? 0 : 1;
}

static int cmd_hangup(const char *host, int port, const char *password,
                      const char *uuid) {
    if (!uuid) {
        fprintf(stderr, "tel: hangup requires --uuid\n");
        return 1;
    }

    EslConn conn;
    if (esl_connect(&conn, host, port) < 0) return 1;
    if (esl_auth(&conn, password) < 0) { esl_close(&conn); return 1; }

    esl_send(&conn, "api uuid_kill %s", uuid);

    char buf[BUF_SIZE];
    int n = esl_api_reply(&conn, buf, sizeof(buf));
    int ok = (n > 0 && !strncmp(buf, "+OK", 3));

    fprintf(stderr, "tel: hangup %s: %s\n", uuid, ok ? "OK" : "failed");
    esl_close(&conn);
    return ok ? 0 : 1;
}

static int cmd_status(sqlite3 *db) {
    if (!db) {
        fprintf(stderr, "tel: no database\n");
        return 1;
    }

    printf("=== BonfyreTel Status ===\n\n");

    /* Call summary */
    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT status, COUNT(*) FROM calls GROUP BY status";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        printf("Calls:\n");
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            printf("  %-12s %d\n",
                   sqlite3_column_text(stmt, 0),
                   sqlite3_column_int(stmt, 1));
        }
        sqlite3_finalize(stmt);
    }

    /* Message summary */
    sql = "SELECT direction, COUNT(*) FROM messages GROUP BY direction";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        printf("\nMessages:\n");
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            printf("  %-12s %d\n",
                   sqlite3_column_text(stmt, 0),
                   sqlite3_column_int(stmt, 1));
        }
        sqlite3_finalize(stmt);
    }

    /* Recent calls */
    sql = "SELECT uuid, direction, caller, callee, started_at, status "
          "FROM calls ORDER BY id DESC LIMIT 5";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        printf("\nRecent calls:\n");
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            printf("  [%s] %s %s → %s (%s) %s\n",
                   sqlite3_column_text(stmt, 4),
                   sqlite3_column_text(stmt, 1),
                   sqlite3_column_text(stmt, 2),
                   sqlite3_column_text(stmt, 3),
                   sqlite3_column_text(stmt, 5),
                   sqlite3_column_text(stmt, 0));
        }
        sqlite3_finalize(stmt);
    }

    /* Recent messages */
    sql = "SELECT direction, from_number, to_number, "
          "SUBSTR(body, 1, 50), sent_at "
          "FROM messages ORDER BY id DESC LIMIT 5";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        printf("\nRecent messages:\n");
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            printf("  [%s] %s %s → %s: %s\n",
                   sqlite3_column_text(stmt, 4),
                   sqlite3_column_text(stmt, 0),
                   sqlite3_column_text(stmt, 1),
                   sqlite3_column_text(stmt, 2),
                   sqlite3_column_text(stmt, 3));
        }
        sqlite3_finalize(stmt);
    }

    return 0;
}

/* ── Verify (Twilio Verify replacement) ────────────────────────────── */

static int cmd_verify_send(const char *host, int port, const char *password,
                           const char *to, sqlite3 *db) {
    if (!to) {
        fprintf(stderr, "tel: verify-send requires --to\n");
        return 1;
    }
    if (!db) {
        fprintf(stderr, "tel: verify-send requires database\n");
        return 1;
    }

    /* P4: arc4random is CSPRNG — srand/rand is predictable and exploitable */
    int code = 100000 + (int)(arc4random_uniform(900000));
    char code_str[8];
    snprintf(code_str, sizeof(code_str), "%d", code);

    char ts[32], expires[32];
    iso_timestamp(ts, sizeof(ts));
    time_t exp_time = time(NULL) + 600; /* 10 min TTL */
    struct tm exp_tm;
    gmtime_r(&exp_time, &exp_tm);
    strftime(expires, sizeof(expires), "%Y-%m-%dT%H:%M:%SZ", &exp_tm);

    /* A code is valid only if it was sent. It used to be stored before the
     * send, and under --dry-run stored and printed without one, so a number
     * could be "verified" with a code that never left the machine. */
    char body[128];
    snprintf(body, sizeof(body),
        "Your Bonfyre verification code is: %s", code_str);
    if (cmd_send_sms(host, port, password, "bonfyre-verify", to, body, db) != 0) {
        fprintf(stderr, "tel: the code was not sent, so none was recorded\n");
        return 1;
    }

    const char *sql = "INSERT INTO verify_codes "
        "(phone, code, created_at, expires_at) VALUES (?1, ?2, ?3, ?4)";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(stmt, 1, to, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, code_str, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, ts, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, expires, -1, SQLITE_TRANSIENT);
    int stored = sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    return stored ? 0 : 1;
}

static int cmd_verify_check(const char *phone, const char *code, sqlite3 *db) {
    if (!phone || !code) {
        fprintf(stderr, "tel: verify-check requires --phone and --code\n");
        return 1;
    }
    if (!db) {
        fprintf(stderr, "tel: verify-check requires database\n");
        return 1;
    }

    char ts[32];
    iso_timestamp(ts, sizeof(ts));

    /* Find valid, unexpired, unverified code */
    const char *sql =
        "SELECT id, attempts FROM verify_codes "
        "WHERE phone=?1 AND code=?2 AND verified=0 AND expires_at>?3 "
        "ORDER BY id DESC LIMIT 1";
    sqlite3_stmt *stmt = NULL;
    int found = 0;
    int64_t row_id = 0;
    int attempts = 0;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, phone, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, code, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, ts, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            found = 1;
            row_id = sqlite3_column_int64(stmt, 0);
            attempts = sqlite3_column_int(stmt, 1);
        }
        sqlite3_finalize(stmt);
    }

    if (found && attempts < 5) {
        /* Mark verified */
        const char *upd = "UPDATE verify_codes SET verified=1 WHERE id=?1";
        if (sqlite3_prepare_v2(db, upd, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_int64(stmt, 1, row_id);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
        printf("verified\n");
        return 0;
    }

    /* Wrong code: increment attempts on latest code for this phone */
    const char *inc =
        "UPDATE verify_codes SET attempts=attempts+1 "
        "WHERE id=(SELECT MAX(id) FROM verify_codes "
        "WHERE phone=?1 AND verified=0)";
    if (sqlite3_prepare_v2(db, inc, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, phone, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    printf("denied\n");
    return 1;
}

/* ── Main ──────────────────────────────────────────────────────────── */

static void usage(void) {
    fprintf(stderr,
        "BonfyreTel %s — FreeSWITCH telephony adapter\n"
        "\n"
        "Production:\n"
        "  bonfyre-tel listen      [--host H] [--port P] [--password PW]\n"
        "  bonfyre-tel send-sms    --from NUM --to NUM --body TEXT\n"
        "  bonfyre-tel send-mms    --from NUM --to NUM --body TEXT --media FILE\n"
        "  bonfyre-tel call        --from NUM --to NUM [--record]\n"
        "  bonfyre-tel hangup      --uuid UUID\n"
        "  bonfyre-tel status\n"
        "\n"
        "Verify (Twilio Verify replacement):\n"
        "  bonfyre-tel verify-send  --to NUM               Send 6-digit code via SMS\n"
        "  bonfyre-tel verify-check --phone NUM --code NUM  Validate code\n"
        "  bonfyre-tel layer-trace <artifact_id> [--root DIR]\n"
        "\n"
        "  bonfyre-tel version\n"
        "\n"
        "Flags:\n"
        "  --db FILE    SQLite database path (default: ~/.local/share/bonfyre/tel.db)\n",
        VERSION);
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    if (argc < 2) { usage(); return 1; }

    /* Parse global options */
    const char *db_path  = arg_get(argc, argv, "--db");
    const char *host     = arg_get(argc, argv, "--host");
    const char *port_str = arg_get(argc, argv, "--port");
    const char *password = arg_get(argc, argv, "--password");
    const char *from     = arg_get(argc, argv, "--from");
    const char *to       = arg_get(argc, argv, "--to");
    const char *body     = arg_get(argc, argv, "--body");
    const char *media    = arg_get(argc, argv, "--media");
    const char *uuid     = arg_get(argc, argv, "--uuid");
    const char *phone    = arg_get(argc, argv, "--phone");
    const char *code     = arg_get(argc, argv, "--code");
    int record           = arg_has(argc, argv, "--record");

    if (!db_path)  db_path  = default_db();
    if (!host)     host     = DEFAULT_HOST;
    if (!password) password = DEFAULT_PASS;
    int port = port_str ? atoi(port_str) : DEFAULT_PORT;

    const char *cmd = argv[1];

    if (strcmp(cmd, "version") == 0) {
        printf("bonfyre-tel %s\n", VERSION);
        return 0;
    }
    if (strcmp(cmd, "layer-trace") == 0 && argc >= 3) {
        return delegate_layeros_tel(argc, argv);
    }

    /* Commands that don't need a DB */

    sqlite3 *db = open_db(db_path);
    int rc = 1;

    if (strcmp(cmd, "listen") == 0) {
        rc = cmd_listen(host, port, password, db);
    } else if (strcmp(cmd, "send-sms") == 0) {
        rc = cmd_send_sms(host, port, password, from, to, body, db);
    } else if (strcmp(cmd, "send-mms") == 0) {
        rc = cmd_send_mms(host, port, password, from, to, body, media, db);
    } else if (strcmp(cmd, "call") == 0) {
        rc = cmd_call(host, port, password, from, to, record, db);
    } else if (strcmp(cmd, "hangup") == 0) {
        rc = cmd_hangup(host, port, password, uuid);
    } else if (strcmp(cmd, "status") == 0) {
        rc = cmd_status(db);
    } else if (strcmp(cmd, "verify-send") == 0) {
        rc = cmd_verify_send(host, port, password, to, db);
    } else if (strcmp(cmd, "verify-check") == 0) {
        rc = cmd_verify_check(phone, code, db);
    } else {
        fprintf(stderr, "tel: unknown command: %s\n", cmd);
        usage();
        rc = 1;
    }

    if (db) sqlite3_close(db);
    return rc;
}
