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

#define VERSION         "1.0.0"
#define DEFAULT_HOST    "127.0.0.1"
#define DEFAULT_PORT    8021
#define DEFAULT_PASS    "ClueCon"
#define BUF_SIZE        (64 * 1024)
#define MAX_HEADERS     128
#define RECV_TIMEOUT_S  1

static volatile int g_running = 1;
static int g_dry_run = 0;

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

static void iso_timestamp(char *buf, size_t len) {
    time_t t = time(NULL);
    struct tm tm;
    gmtime_r(&t, &tm);
    strftime(buf, len, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

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
    if (sqlite3_open(path, &db) != SQLITE_OK) {
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
    if (g_dry_run) {
        fprintf(stderr, "tel: [DRY-RUN] would trigger: bonfyre-pipeline run %s\n", rec_path);
    } else {
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

    /* Trigger ingest — feed the message body to the pipeline */
    if (body && body[0]) {
        if (g_dry_run) {
            fprintf(stderr, "tel: [DRY-RUN] would trigger: bonfyre-ingest --text \"%s\"\n", body);
        } else {
            char *const argv[] = {
                "bonfyre-ingest", "--text", (char *)body,
                "--meta", "channel=sms",
                "--meta", (char *)(from ? from : "unknown"),
                NULL
            };
            run_process_async(argv);
        }
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

static int cmd_listen(const char *host, int port, const char *password,
                      sqlite3 *db) {
    EslConn conn;

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
    if (do_record) {
        esl_send(&conn,
            "api originate {origination_caller_id_number=%s,"
            "execute_on_answer='record_session $${recordings_dir}/%s_%s_${strftime(%%Y%%m%%d_%%H%%M%%S)}.wav'}"
            "sofia/external/%s@${default_provider} &park()",
            from, from, to, to);
    } else {
        esl_send(&conn,
            "api originate {origination_caller_id_number=%s}"
            "sofia/external/%s@${default_provider} &park()",
            from, to);
    }

    char buf[BUF_SIZE];
    int n = esl_recv_event(&conn, buf, sizeof(buf));
    int ok = (n > 0 && !strstr(buf, "-ERR"));

    if (ok)
        fprintf(stderr, "tel: call originated: %s → %s%s\n",
                from, to, do_record ? " (recording)" : "");
    else
        fprintf(stderr, "tel: call failed: %.200s\n", n > 0 ? buf : "(no response)");

    if (db) {
        char ts[32];
        iso_timestamp(ts, sizeof(ts));
        const char *sql = "INSERT INTO calls "
            "(uuid, direction, caller, callee, started_at, status) "
            "VALUES ('pending', 'outbound', ?1, ?2, ?3, ?4)";
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
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
    int n = esl_recv_event(&conn, buf, sizeof(buf));
    int ok = (n > 0 && !strstr(buf, "-ERR"));

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

/* ── Mock ESL Server (zero-cost testing) ───────────────────────────── */

#define MAX_MOCK_CLIENTS 16
#define MOCK_BUF_SIZE    4096

typedef struct {
    int    fd;
    int    authed;
    int    subscribed;
    char   buf[MOCK_BUF_SIZE];
    size_t buf_len;
} MockClient;

static void mock_send(int fd, const char *fmt, ...) {
    char msg[4096];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    if (n > 0) write(fd, msg, (size_t)n);
}

static void mock_broadcast(MockClient *clients, int n,
                           const char *data, size_t len) {
    for (int i = 0; i < n; i++)
        if (clients[i].fd >= 0 && clients[i].subscribed)
            write(clients[i].fd, data, len);
}

static void mock_gen_call_event(char *out, size_t sz,
                                const char *caller, const char *callee) {
    char ts[32];
    iso_timestamp(ts, sizeof(ts));
    snprintf(out, sz,
        "Event-Name: CHANNEL_EXECUTE_COMPLETE\n"
        "Application: record\n"
        "Unique-ID: sim-%08x\n"
        "Caller-Caller-ID-Number: %s\n"
        "Caller-Destination-Number: %s\n"
        "variable_record_file_path: /tmp/bonfyre-sim/rec_%s.wav\n"
        "variable_record_seconds: 45\n\n",
        (unsigned)time(NULL), caller, callee, ts);
}

static void mock_gen_sms_event(char *out, size_t sz,
                               const char *from, const char *to,
                               const char *body) {
    snprintf(out, sz,
        "Event-Name: CUSTOM\n"
        "Event-Subclass: sms::recv\n"
        "from: %s\n"
        "to: %s\n"
        "body: %s\n\n",
        from, to, body);
}

static void mock_gen_hangup_event(char *out, size_t sz, const char *uuid) {
    snprintf(out, sz,
        "Event-Name: CHANNEL_HANGUP_COMPLETE\n"
        "Unique-ID: %s\n"
        "variable_billsec: 45\n\n",
        uuid);
}

static void mock_handle_data(MockClient *c, MockClient *all, int n_all,
                             const char *password) {
    char *end;
    while ((end = strstr(c->buf, "\n\n")) != NULL) {
        *end = '\0';
        char *line = c->buf;

        if (strncmp(line, "auth ", 5) == 0) {
            if (strcmp(line + 5, password) == 0) {
                c->authed = 1;
                mock_send(c->fd,
                    "Content-Type: command/reply\n"
                    "Reply-Text: +OK accepted\n\n");
                fprintf(stderr, "mock: client authed\n");
            } else {
                mock_send(c->fd,
                    "Content-Type: command/reply\n"
                    "Reply-Text: -ERR invalid\n\n");
            }
        } else if (strncmp(line, "event ", 6) == 0) {
            c->subscribed = 1;
            mock_send(c->fd,
                "Content-Type: command/reply\n"
                "Reply-Text: +OK event listener enabled plain\n\n");
            fprintf(stderr, "mock: client subscribed to events\n");
        } else if (strncmp(line, "api sim_call ", 13) == 0) {
            char caller[64] = "+15551234567", callee[64] = "+15559876543";
            sscanf(line + 13, "%63s %63s", caller, callee);

            char event[2048];
            mock_gen_call_event(event, sizeof(event), caller, callee);
            mock_broadcast(all, n_all, event, strlen(event));

            /* Also generate a hangup event */
            char hangup[1024], uuid[32];
            snprintf(uuid, sizeof(uuid), "sim-%08x", (unsigned)time(NULL));
            mock_gen_hangup_event(hangup, sizeof(hangup), uuid);
            mock_broadcast(all, n_all, hangup, strlen(hangup));

            /* Touch a fake recording so pipeline can see a file */
            char wav_path[256], ts[32];
            iso_timestamp(ts, sizeof(ts));
            snprintf(wav_path, sizeof(wav_path),
                "/tmp/bonfyre-sim/rec_%s.wav", ts);
            FILE *f = fopen(wav_path, "w");
            if (f) {
                unsigned char hdr[44] = {
                    'R','I','F','F', 36,0,0,0, 'W','A','V','E',
                    'f','m','t',' ', 16,0,0,0, 1,0, 1,0,
                    0x80,0x3e,0,0, 0x80,0x3e,0,0, 1,0, 8,0,
                    'd','a','t','a', 0,0,0,0
                };
                fwrite(hdr, 1, 44, f);
                fclose(f);
            }

            mock_send(c->fd,
                "Content-Type: api/response\n"
                "Reply-Text: +OK sim_call sent\n\n");
            fprintf(stderr, "mock: injected call event %s → %s\n",
                    caller, callee);
        } else if (strncmp(line, "api sim_sms ", 12) == 0) {
            char from_num[64] = "+15551234567", to_num[64] = "+15559876543";
            char body_buf[1024] = "Test message from bonfyre-tel mock";
            char *p = line + 12;
            if (sscanf(p, "%63s %63s", from_num, to_num) >= 2) {
                char *bp = strstr(p + 1, to_num);
                if (bp) {
                    bp += strlen(to_num);
                    while (*bp == ' ') bp++;
                    if (*bp) snprintf(body_buf, sizeof(body_buf), "%s", bp);
                }
            }

            char event[2048];
            mock_gen_sms_event(event, sizeof(event),
                               from_num, to_num, body_buf);
            mock_broadcast(all, n_all, event, strlen(event));

            mock_send(c->fd,
                "Content-Type: api/response\n"
                "Reply-Text: +OK sim_sms sent\n\n");
            fprintf(stderr, "mock: injected SMS %s → %s: %.40s\n",
                    from_num, to_num, body_buf);
        } else if (strncmp(line, "api ", 4) == 0) {
            mock_send(c->fd,
                "Content-Type: api/response\n"
                "Reply-Text: +OK\n\n");
        }

        size_t consumed = (size_t)(end - c->buf + 2);
        size_t remain = c->buf_len - consumed;
        if (remain > 0) memmove(c->buf, end + 2, remain);
        c->buf_len = remain;
        c->buf[c->buf_len] = '\0';
    }
}

static int cmd_mock(int port, const char *password, int auto_mode) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port   = htons((uint16_t)port),
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK)
    };

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "mock: cannot bind port %d: %s\n",
                port, strerror(errno));
        close(server_fd);
        return 1;
    }

    listen(server_fd, 5);

    fprintf(stderr,
        "\n"
        "  ┌─────────────────────────────────────────────────────────┐\n"
        "  │  BonfyreTel Mock ESL Server — 127.0.0.1:%-5d          │\n"
        "  │                                                         │\n"
        "  │  No FreeSWITCH. No SIP trunk. No phone number.         │\n"
        "  │  Full pipeline testing at zero cost.                    │\n"
        "  │                                                         │\n"
        "  │  Terminal 1:  bonfyre-tel mock                          │\n"
        "  │  Terminal 2:  bonfyre-tel listen --dry-run              │\n"
        "  │  Terminal 3:  bonfyre-tel sim-call                      │\n"
        "  │               bonfyre-tel sim-sms --body \"hello\"        │\n",
        port);
    if (auto_mode)
        fprintf(stderr,
        "  │                                                         │\n"
        "  │  Auto-mode: generating events every 5 seconds           │\n");
    fprintf(stderr,
        "  └─────────────────────────────────────────────────────────┘\n\n");

    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);

    MockClient clients[MAX_MOCK_CLIENTS];
    for (int i = 0; i < MAX_MOCK_CLIENTS; i++) {
        clients[i].fd = -1;
        clients[i].authed = 0;
        clients[i].subscribed = 0;
        clients[i].buf_len = 0;
    }

    time_t last_auto = time(NULL);
    int auto_counter = 0;

    mkdir("/tmp/bonfyre-sim", 0755);

    while (g_running) {
        struct pollfd fds[MAX_MOCK_CLIENTS + 1];
        fds[0].fd = server_fd;
        fds[0].events = POLLIN;
        for (int i = 0; i < MAX_MOCK_CLIENTS; i++) {
            fds[i + 1].fd = clients[i].fd;
            fds[i + 1].events = POLLIN;
        }

        int ready = poll(fds, MAX_MOCK_CLIENTS + 1, 1000);
        if (ready < 0) {
            if (errno == EINTR) continue;
            break;
        }

        /* Accept new connections */
        if (fds[0].revents & POLLIN) {
            int cfd = accept(server_fd, NULL, NULL);
            if (cfd >= 0) {
                int slot = -1;
                for (int i = 0; i < MAX_MOCK_CLIENTS; i++)
                    if (clients[i].fd < 0) { slot = i; break; }
                if (slot >= 0) {
                    clients[slot].fd = cfd;
                    clients[slot].authed = 0;
                    clients[slot].subscribed = 0;
                    clients[slot].buf_len = 0;
                    clients[slot].buf[0] = '\0';
                    mock_send(cfd, "Content-Type: auth/request\n\n");
                    fprintf(stderr, "mock: client connected (slot %d)\n", slot);
                } else {
                    close(cfd);
                }
            }
        }

        /* Process client data */
        for (int i = 0; i < MAX_MOCK_CLIENTS; i++) {
            if (clients[i].fd < 0) continue;
            if (!(fds[i + 1].revents & POLLIN)) continue;

            ssize_t n = recv(clients[i].fd,
                             clients[i].buf + clients[i].buf_len,
                             MOCK_BUF_SIZE - 1 - clients[i].buf_len, 0);
            if (n <= 0) {
                close(clients[i].fd);
                clients[i].fd = -1;
                fprintf(stderr, "mock: client disconnected (slot %d)\n", i);
            } else {
                clients[i].buf_len += (size_t)n;
                clients[i].buf[clients[i].buf_len] = '\0';
                mock_handle_data(&clients[i], clients,
                                 MAX_MOCK_CLIENTS, password);
            }
        }

        /* Auto-mode: periodic event generation */
        if (auto_mode && time(NULL) - last_auto >= 5) {
            last_auto = time(NULL);
            auto_counter++;
            char event[2048];

            if (auto_counter % 3 == 0) {
                char body[128];
                snprintf(body, sizeof(body),
                    "Auto test message #%d", auto_counter);
                mock_gen_sms_event(event, sizeof(event),
                    "+15551234567", "+15559876543", body);
                fprintf(stderr, "mock: [auto] SMS event #%d\n", auto_counter);
            } else {
                mock_gen_call_event(event, sizeof(event),
                    "+15551234567", "+15559876543");
                fprintf(stderr, "mock: [auto] call event #%d\n", auto_counter);
            }

            mock_broadcast(clients, MAX_MOCK_CLIENTS,
                           event, strlen(event));
        }
    }

    for (int i = 0; i < MAX_MOCK_CLIENTS; i++)
        if (clients[i].fd >= 0) close(clients[i].fd);
    close(server_fd);
    fprintf(stderr, "\nmock: shut down\n");
    return 0;
}

/* ── Sim Commands ──────────────────────────────────────────────────── */

static int cmd_sim_call(const char *host, int port, const char *password,
                        const char *from, const char *to) {
    if (!from) from = "+15551234567";
    if (!to)   to   = "+15559876543";

    EslConn conn;
    if (esl_connect(&conn, host, port) < 0) {
        fprintf(stderr,
            "tel: cannot connect to mock at %s:%d\n"
            "     Start the mock first: bonfyre-tel mock\n", host, port);
        return 1;
    }
    if (esl_auth(&conn, password) < 0) { esl_close(&conn); return 1; }

    esl_send(&conn, "api sim_call %s %s", from, to);

    char buf[BUF_SIZE];
    int n = esl_recv_event(&conn, buf, sizeof(buf));
    int ok = (n > 0 && strstr(buf, "+OK"));

    if (ok)
        fprintf(stderr, "tel: simulated call: %s → %s\n", from, to);
    else
        fprintf(stderr, "tel: sim-call failed\n");

    esl_close(&conn);
    return ok ? 0 : 1;
}

static int cmd_sim_sms(const char *host, int port, const char *password,
                       const char *from, const char *to, const char *body) {
    if (!from) from = "+15551234567";
    if (!to)   to   = "+15559876543";
    if (!body) body = "Test message from bonfyre-tel simulator";

    EslConn conn;
    if (esl_connect(&conn, host, port) < 0) {
        fprintf(stderr,
            "tel: cannot connect to mock at %s:%d\n"
            "     Start the mock first: bonfyre-tel mock\n", host, port);
        return 1;
    }
    if (esl_auth(&conn, password) < 0) { esl_close(&conn); return 1; }

    esl_send(&conn, "api sim_sms %s %s %s", from, to, body);

    char buf[BUF_SIZE];
    int n = esl_recv_event(&conn, buf, sizeof(buf));
    int ok = (n > 0 && strstr(buf, "+OK"));

    if (ok)
        fprintf(stderr, "tel: simulated SMS: %s → %s: %s\n", from, to, body);
    else
        fprintf(stderr, "tel: sim-sms failed\n");

    esl_close(&conn);
    return ok ? 0 : 1;
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

    /* Store in DB */
    const char *sql = "INSERT INTO verify_codes "
        "(phone, code, created_at, expires_at) VALUES (?1, ?2, ?3, ?4)";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, to, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, code_str, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, ts, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 4, expires, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    if (g_dry_run) {
        printf("verify: [DRY-RUN] code for %s: %s (expires %s)\n",
               to, code_str, expires);
        return 0;
    }

    /* Send code via SMS */
    char body[128];
    snprintf(body, sizeof(body),
        "Your Bonfyre verification code is: %s", code_str);
    return cmd_send_sms(host, port, password, "bonfyre-verify", to, body, db);
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
        "  bonfyre-tel listen      [--host H] [--port P] [--password PW] [--dry-run]\n"
        "  bonfyre-tel send-sms    --from NUM --to NUM --body TEXT\n"
        "  bonfyre-tel send-mms    --from NUM --to NUM --body TEXT --media FILE\n"
        "  bonfyre-tel call        --from NUM --to NUM [--record]\n"
        "  bonfyre-tel hangup      --uuid UUID\n"
        "  bonfyre-tel status\n"
        "\n"
        "Testing (zero cost, no FreeSWITCH needed):\n"
        "  bonfyre-tel mock        [--port P] [--auto]     Start mock ESL server\n"
        "  bonfyre-tel sim-call    [--from N] [--to N]     Inject fake call event\n"
        "  bonfyre-tel sim-sms     [--from N] [--to N] [--body T]  Inject fake SMS\n"
        "\n"
        "Verify (Twilio Verify replacement):\n"
        "  bonfyre-tel verify-send  --to NUM               Send 6-digit code via SMS\n"
        "  bonfyre-tel verify-check --phone NUM --code NUM  Validate code\n"
        "\n"
        "  bonfyre-tel version\n"
        "\n"
        "Flags:\n"
        "  --dry-run    Log pipeline triggers without actually forking\n"
        "  --auto       (mock) Generate events every 5 seconds automatically\n"
        "  --db FILE    SQLite database path (default: ~/.local/share/bonfyre/tel.db)\n"
        "\n"
        "Zero-cost test flow:\n"
        "  Terminal 1:  bonfyre-tel mock\n"
        "  Terminal 2:  bonfyre-tel listen --dry-run\n"
        "  Terminal 3:  bonfyre-tel sim-call\n"
        "               bonfyre-tel sim-sms --body \"hello world\"\n"
        "               bonfyre-tel verify-send --to +15559876543 --dry-run\n"
        "               bonfyre-tel verify-check --phone +15559876543 --code 123456\n",
        VERSION);
}

int main(int argc, char **argv) {
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
    int auto_mode        = arg_has(argc, argv, "--auto");
    g_dry_run            = arg_has(argc, argv, "--dry-run");

    if (!db_path)  db_path  = default_db();
    if (!host)     host     = DEFAULT_HOST;
    if (!password) password = DEFAULT_PASS;
    int port = port_str ? atoi(port_str) : DEFAULT_PORT;

    const char *cmd = argv[1];

    if (strcmp(cmd, "version") == 0) {
        printf("bonfyre-tel %s\n", VERSION);
        return 0;
    }

    /* Commands that don't need a DB */
    if (strcmp(cmd, "mock") == 0)
        return cmd_mock(port, password, auto_mode);
    if (strcmp(cmd, "sim-call") == 0)
        return cmd_sim_call(host, port, password, from, to);
    if (strcmp(cmd, "sim-sms") == 0)
        return cmd_sim_sms(host, port, password, from, to, body);

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
