/*
 * BonfyreProxy — OpenAI-compatible API shim for Bonfyre binaries.
 *
 * Drop-in replacement for OpenAI API endpoints:
 *   POST /v1/audio/transcriptions   → bonfyre-transcribe (or hcp-whisper)
 *   POST /v1/chat/completions       → bonfyre-brief (summarize input)
 *   GET  /v1/models                 → list available local models
 *   GET  /health                    → health check
 *
 * Usage:
 *   bonfyre-proxy serve [--port 8787] [--whisper-bin PATH] [--hcp-bin PATH]
 *   bonfyre-proxy status
 *
 * Environment:
 *   BONFYRE_PROXY_PORT   — port (default 8787)
 *   HCP_WHISPER_BIN      — path to hcp-whisper binary
 *   BONFYRE_TRANSCRIBE   — path to bonfyre-transcribe binary
 *
 * Clients set:
 *   OPENAI_API_BASE=http://localhost:8787
 *   OPENAI_API_KEY=anything    # accepted but ignored
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
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
#include <fcntl.h>

#include "bf_lex.h"
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"
#define VERSION       "1.0.0"
#define MAX_BODY      (50*1024*1024)  /* 50MB — audio files can be large */
#define MAX_PATH_LEN  2048
#define MAX_RESULT    (4*1024*1024)   /* 4MB result buffer */
#define MAX_THREADS   32
#define THREAD_STACK  (512*1024)
#define TEMP_DIR      "/tmp/bonfyre-proxy"

static volatile int g_running = 1;
static atomic_int g_thread_count = 0;
static char g_hcp_bin[MAX_PATH_LEN] = "";
static char g_transcribe_bin[MAX_PATH_LEN] = "";
static char g_brief_bin[MAX_PATH_LEN] = "";
static int g_port = 8787;

/* ── Logging ─────────────────────────────────────────────────────── */

static void logmsg(const char *level, const char *fmt, ...) {
    time_t now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);
    char ts[32];
    strftime(ts, sizeof(ts), "%H:%M:%S", &tm);
    fprintf(stderr, "[%s] %s ", ts, level);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, "\n");
}

#define LOG_INFO(...)  logmsg("INFO", __VA_ARGS__)
#define LOG_ERR(...)   logmsg("ERR ", __VA_ARGS__)

/* ── HTTP primitives ─────────────────────────────────────────────── */

typedef struct {
    char method[16];
    char path[MAX_PATH_LEN];
    char *body;
    int  body_len;
    int  content_length;
    char content_type[512];
    char boundary[256];
    char auth[256];
} HttpRequest;

typedef struct {
    int fd;
    char *buf;
    size_t len;
    int status;
} HttpResponse;

static void resp_init(HttpResponse *r, int fd) {
    memset(r, 0, sizeof(*r));
    r->fd = fd;
    r->status = 200;
}
static void resp_free(HttpResponse *r) { free(r->buf); r->buf = NULL; }

static void resp_json(HttpResponse *r, int status, const char *fmt, ...) {
    r->status = status;
    free(r->buf);
    va_list ap;
    va_start(ap, fmt);
    int n = vasprintf(&r->buf, fmt, ap);
    va_end(ap);
    r->len = (size_t)(n > 0 ? n : 0);
}

static void resp_send(HttpResponse *r) {
    const char *st = "OK";
    switch (r->status) {
        case 200: st = "OK"; break;
        case 400: st = "Bad Request"; break;
        case 404: st = "Not Found"; break;
        case 405: st = "Method Not Allowed"; break;
        case 413: st = "Payload Too Large"; break;
        case 500: st = "Internal Server Error"; break;
    }
    char hdr[1024];
    int hlen = snprintf(hdr, sizeof(hdr),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %zu\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
        "Access-Control-Allow-Headers: Authorization, Content-Type\r\n"
        "Connection: close\r\n\r\n",
        r->status, st, r->len);
    write(r->fd, hdr, (size_t)hlen);
    if (r->len > 0 && r->buf) write(r->fd, r->buf, r->len);
}

static int parse_request(int fd, HttpRequest *req) {
    memset(req, 0, sizeof(*req));

    char hdr[16384];
    ssize_t total = 0;
    while (total < (ssize_t)sizeof(hdr) - 1) {
        ssize_t n = read(fd, hdr + total, sizeof(hdr) - (size_t)total - 1);
        if (n <= 0) return -1;
        total += n;
        hdr[total] = '\0';
        if (strstr(hdr, "\r\n\r\n")) break;
    }

    /* Method + path */
    char *sp = strchr(hdr, ' ');
    if (!sp) return -1;
    size_t mlen = (size_t)(sp - hdr);
    if (mlen >= sizeof(req->method)) mlen = sizeof(req->method) - 1;
    memcpy(req->method, hdr, mlen);
    req->method[mlen] = '\0';

    char *sp2 = strchr(sp + 1, ' ');
    if (!sp2) return -1;
    size_t plen = (size_t)(sp2 - sp - 1);
    if (plen >= sizeof(req->path)) plen = sizeof(req->path) - 1;
    memcpy(req->path, sp + 1, plen);
    req->path[plen] = '\0';

    /* Content-Length */
    const char *cl = strcasestr(hdr, "Content-Length:");
    if (cl) req->content_length = atoi(cl + 15);

    /* Content-Type + boundary */
    const char *ct = strcasestr(hdr, "Content-Type:");
    if (ct) {
        ct += 13;
        while (*ct == ' ') ct++;
        const char *end = strpbrk(ct, "\r\n");
        if (end) {
            size_t ctlen = (size_t)(end - ct);
            if (ctlen >= sizeof(req->content_type)) ctlen = sizeof(req->content_type) - 1;
            memcpy(req->content_type, ct, ctlen);
            req->content_type[ctlen] = '\0';
        }
        const char *b = strstr(req->content_type, "boundary=");
        if (b) {
            b += 9;
            if (*b == '"') b++;
            size_t blen = 0;
            while (b[blen] && b[blen] != '"' && b[blen] != ';' && b[blen] != '\r' && blen < sizeof(req->boundary) - 1)
                blen++;
            memcpy(req->boundary, b, blen);
            req->boundary[blen] = '\0';
        }
    }

    /* Authorization (accepted but not validated — local-only service) */
    const char *auth = strcasestr(hdr, "Authorization:");
    if (auth) {
        auth += 14;
        while (*auth == ' ') auth++;
        const char *aend = strpbrk(auth, "\r\n");
        if (aend) {
            size_t alen = (size_t)(aend - auth);
            if (alen >= sizeof(req->auth)) alen = sizeof(req->auth) - 1;
            memcpy(req->auth, auth, alen);
        }
    }

    /* Body */
    char *body_start = strstr(hdr, "\r\n\r\n");
    if (!body_start) return -1;
    body_start += 4;
    int hdr_consumed = (int)(body_start - hdr);
    int body_in_header = (int)(total - hdr_consumed);

    if (req->content_length > MAX_BODY) return -1;
    if (req->content_length > 0) {
        req->body = malloc((size_t)req->content_length + 1);
        if (!req->body) return -1;
        if (body_in_header > 0) {
            int copy = body_in_header;
            if (copy > req->content_length) copy = req->content_length;
            memcpy(req->body, body_start, (size_t)copy);
            req->body_len = copy;
        }
        while (req->body_len < req->content_length) {
            ssize_t n = read(fd, req->body + req->body_len,
                             (size_t)(req->content_length - req->body_len));
            if (n <= 0) break;
            req->body_len += (int)n;
        }
        req->body[req->body_len] = '\0';
    }
    return 0;
}

/* ── Multipart parser (extract "file" part to disk) ──────────────── */

static int extract_multipart_file(const HttpRequest *req, char *out_path, size_t out_path_len) {
    if (!req->boundary[0] || !req->body) return -1;

    char delim[300];
    snprintf(delim, sizeof(delim), "--%s", req->boundary);
    size_t dlen = strlen(delim);

    char *p = req->body;
    char *end = req->body + req->body_len;

    while (p < end) {
        char *part = memmem(p, (size_t)(end - p), delim, dlen);
        if (!part) break;
        part += dlen;
        if (part[0] == '-' && part[1] == '-') break; /* final boundary */
        if (part[0] == '\r') part++;
        if (part[0] == '\n') part++;

        /* Find headers end */
        char *hdr_end = memmem(part, (size_t)(end - part), "\r\n\r\n", 4);
        if (!hdr_end) break;

        /* Check if this part has name="file" */
        char hdr_buf[2048];
        size_t hdr_len = (size_t)(hdr_end - part);
        if (hdr_len >= sizeof(hdr_buf)) hdr_len = sizeof(hdr_buf) - 1;
        memcpy(hdr_buf, part, hdr_len);
        hdr_buf[hdr_len] = '\0';

        if (strstr(hdr_buf, "name=\"file\"")) {
            char *data = hdr_end + 4;

            /* Find next boundary */
            char *next = memmem(data, (size_t)(end - data), delim, dlen);
            if (!next) break;

            /* Strip trailing \r\n before boundary */
            size_t data_len = (size_t)(next - data);
            if (data_len >= 2 && data[data_len - 2] == '\r' && data[data_len - 1] == '\n')
                data_len -= 2;

            /* Determine extension from filename */
            const char *fn = strstr(hdr_buf, "filename=\"");
            const char *ext = ".wav";
            if (fn) {
                fn += 10;
                const char *dot = strrchr(fn, '.');
                if (dot) {
                    static char ext_buf[16];
                    size_t elen = 0;
                    while (dot[elen] && dot[elen] != '"' && elen < sizeof(ext_buf) - 1)
                        ext_buf[elen] = dot[elen], elen++;
                    ext_buf[elen] = '\0';
                    ext = ext_buf;
                }
            }

            /* Write to temp file */
            pid_t pid = getpid();
            snprintf(out_path, out_path_len, "%s/upload_%d%s", TEMP_DIR, pid, ext);
            FILE *fp = fopen(out_path, "wb");
            if (!fp) return -1;
            fwrite(data, 1, data_len, fp);
            fclose(fp);
            return 0;
        }

        p = hdr_end + 4;
    }
    return -1;
}

/* ── Extract multipart form field value ──────────────────────────── */

static int extract_multipart_field(const HttpRequest *req, const char *name,
                                   char *out, size_t out_len) {
    if (!req->boundary[0] || !req->body) return -1;

    char needle[300];
    snprintf(needle, sizeof(needle), "name=\"%s\"", name);

    char delim[300];
    snprintf(delim, sizeof(delim), "--%s", req->boundary);
    size_t dlen = strlen(delim);

    char *p = req->body;
    char *end = req->body + req->body_len;

    while (p < end) {
        char *part = memmem(p, (size_t)(end - p), delim, dlen);
        if (!part) break;
        part += dlen;
        if (part[0] == '-' && part[1] == '-') break;
        if (part[0] == '\r') part++;
        if (part[0] == '\n') part++;

        char *hdr_end = memmem(part, (size_t)(end - part), "\r\n\r\n", 4);
        if (!hdr_end) break;

        char hdr_buf[2048];
        size_t hdr_len = (size_t)(hdr_end - part);
        if (hdr_len >= sizeof(hdr_buf)) hdr_len = sizeof(hdr_buf) - 1;
        memcpy(hdr_buf, part, hdr_len);
        hdr_buf[hdr_len] = '\0';

        if (strstr(hdr_buf, needle) && !strstr(hdr_buf, "filename=")) {
            char *data = hdr_end + 4;
            char *next = memmem(data, (size_t)(end - data), delim, dlen);
            if (!next) break;
            size_t data_len = (size_t)(next - data);
            if (data_len >= 2 && data[data_len - 2] == '\r' && data[data_len - 1] == '\n')
                data_len -= 2;
            if (data_len >= out_len) data_len = out_len - 1;
            memcpy(out, data, data_len);
            out[data_len] = '\0';
            return 0;
        }
        p = hdr_end + 4;
    }
    return -1;
}

/* ── Run a binary and capture stdout ─────────────────────────────── */

static int run_binary(const char *bin, char *const argv[], char *out, size_t out_len, size_t *out_actual) {
    int pipefd[2];
    if (pipe(pipefd) < 0) return -1;

    pid_t pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return -1; }

    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);
        /* Redirect stderr to /dev/null for clean JSON output */
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) { dup2(devnull, STDERR_FILENO); close(devnull); }
        execvp(bin, argv);
        _exit(127);
    }

    close(pipefd[1]);
    size_t total = 0;
    while (total < out_len - 1) {
        ssize_t n = read(pipefd[0], out + total, out_len - total - 1);
        if (n <= 0) break;
        total += (size_t)n;
    }
    out[total] = '\0';
    if (out_actual) *out_actual = total;
    close(pipefd[0]);

    int status;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

/* ── Escape JSON string ──────────────────────────────────────────── */

/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 */
static char *json_escape(const char *s, size_t len) {
    return bf_lex_json_escape_alloc(s, len);
}

/* ── GET /v1/models ──────────────────────────────────────────────── */

static void handle_models(HttpResponse *r) {
    /* Check which backends are available */
    int has_hcp = (g_hcp_bin[0] && access(g_hcp_bin, X_OK) == 0);
    int has_transcribe = (g_transcribe_bin[0] && access(g_transcribe_bin, X_OK) == 0);
    int has_brief = (g_brief_bin[0] && access(g_brief_bin, X_OK) == 0);

    char buf[4096];
    int off = 0;
    off += snprintf(buf + off, sizeof(buf) - (size_t)off,
        "{\"object\":\"list\",\"data\":[");

    int first = 1;

    if (has_hcp) {
        off += snprintf(buf + off, sizeof(buf) - (size_t)off,
            "%s{\"id\":\"whisper-1\",\"object\":\"model\",\"owned_by\":\"bonfyre\",\"permission\":[]}"
            , first ? "" : ",");
        first = 0;
    }
    if (has_transcribe) {
        off += snprintf(buf + off, sizeof(buf) - (size_t)off,
            "%s{\"id\":\"bonfyre-transcribe\",\"object\":\"model\",\"owned_by\":\"bonfyre\",\"permission\":[]}"
            , first ? "" : ",");
        first = 0;
    }
    if (has_brief) {
        off += snprintf(buf + off, sizeof(buf) - (size_t)off,
            "%s{\"id\":\"bonfyre-brief\",\"object\":\"model\",\"owned_by\":\"bonfyre\",\"permission\":[]}"
            , first ? "" : ",");
        first = 0;
    }

    (void)first;
    snprintf(buf + off, sizeof(buf) - (size_t)off, "]}");
    resp_json(r, 200, "%s", buf);
}

/* ── POST /v1/audio/transcriptions ───────────────────────────────── */

/* Remove a directory this process made and the plain files a tool wrote in it. */
static void remove_flat_dir(const char *dir) {
    DIR *d = opendir(dir);
    if (d) {
        struct dirent *e; char path[MAX_PATH_LEN];
        while ((e = readdir(d))) {
            if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
            snprintf(path, sizeof(path), "%s/%s", dir, e->d_name);
            unlink(path);
        }
        closedir(d);
    }
    rmdir(dir);
}

static void handle_transcription_into(HttpRequest *req, HttpResponse *r, char *result) {
    /* Extract uploaded audio file */
    char audio_path[MAX_PATH_LEN];
    if (extract_multipart_file(req, audio_path, sizeof(audio_path)) < 0) {
        resp_json(r, 400,
            "{\"error\":{\"message\":\"Missing 'file' in multipart form data\","
            "\"type\":\"invalid_request_error\",\"code\":\"missing_file\"}}");
        return;
    }

    /* Extract optional fields */
    char model[128] = "whisper-1";
    char language[16] = "en";
    char response_format[32] = "json";
    extract_multipart_field(req, "model", model, sizeof(model));
    extract_multipart_field(req, "language", language, sizeof(language));
    extract_multipart_field(req, "response_format", response_format, sizeof(response_format));

    LOG_INFO("transcribe: file=%s model=%s lang=%s fmt=%s", audio_path, model, language, response_format);

    size_t result_len = 0;
    int rc;

    /* Prefer hcp-whisper if available */
    if (g_hcp_bin[0] && access(g_hcp_bin, X_OK) == 0) {
        char lang_flag[32];
        snprintf(lang_flag, sizeof(lang_flag), "-l");

        /* Determine output format flag */
        const char *fmt_flag = "--output-json";
        if (strcmp(response_format, "srt") == 0) fmt_flag = "--output-srt";
        else if (strcmp(response_format, "vtt") == 0) fmt_flag = "--output-vtt";
        else if (strcmp(response_format, "text") == 0) fmt_flag = "--output-txt";

        char *argv[] = {
            (char *)g_hcp_bin,
            "-f", audio_path,
            lang_flag, (char *)language,
            (char *)fmt_flag,
            NULL
        };
        rc = run_binary(g_hcp_bin, argv, result, MAX_RESULT, &result_len);
    } else if (g_transcribe_bin[0] && access(g_transcribe_bin, X_OK) == 0) {
        /* bonfyre-transcribe <input-audio> <output-dir> --language L writes
         * transcript.txt/.srt/.vtt (and working files) into the directory. */
        char out_dir[MAX_PATH_LEN];
        snprintf(out_dir, sizeof(out_dir), "%s/asr_%d_%lx.XXXXXX", TEMP_DIR, getpid(), (unsigned long)pthread_self());
        if (!mkdtemp(out_dir)) { unlink(audio_path); resp_json(r, 500, "{\"error\":{\"message\":\"Failed to create output directory\"}}"); return; }
        char *argv[] = { (char *)g_transcribe_bin, audio_path, out_dir, "--language", language, NULL };
        rc = run_binary(g_transcribe_bin, argv, result, MAX_RESULT, &result_len);
        if (rc == 0) {
            const char *want = !strcmp(response_format, "srt") ? "transcript.srt"
                             : !strcmp(response_format, "vtt") ? "transcript.vtt" : "transcript.txt";
            char path[MAX_PATH_LEN];
            snprintf(path, sizeof(path), "%s/%s", out_dir, want);
            FILE *tf = fopen(path, "rb");
            if (!tf) rc = 1;
            else {
                result_len = fread(result, 1, MAX_RESULT - 1, tf); fclose(tf);
                if (!strcmp(want, "transcript.txt")) {
                    size_t a = 0; while (a < result_len && (result[a] == ' ' || result[a] == '\n')) a++;
                    while (result_len > a && (result[result_len - 1] == ' ' || result[result_len - 1] == '\n')) result_len--;
                    memmove(result, result + a, result_len - a); result_len -= a;
                }
                result[result_len] = '\0';
            }
        }
        remove_flat_dir(out_dir);
    } else {
        resp_json(r, 500,
            "{\"error\":{\"message\":\"No transcription backend available. "
            "Install hcp-whisper or bonfyre-transcribe.\","
            "\"type\":\"server_error\",\"code\":\"no_backend\"}}");
        unlink(audio_path);
        return;
    }

    /* Clean up temp file */
    unlink(audio_path);

    if (rc != 0) {
        resp_json(r, 500,
            "{\"error\":{\"message\":\"Transcription failed (exit code %d)\","
            "\"type\":\"server_error\",\"code\":\"transcription_failed\"}}", rc);
        return;
    }

    /* Format response in OpenAI style */
    if (strcmp(response_format, "json") == 0 || strcmp(response_format, "verbose_json") == 0) {
        /* Try to extract text from JSON output */
        const char *text_key = "\"text\"";
        char *text_start = strstr(result, text_key);
        if (text_start) {
            /* Already JSON — wrap in OpenAI envelope if needed */
            char *colon = strchr(text_start + strlen(text_key), ':');
            if (colon) {
                colon++;
                while (*colon == ' ') colon++;
                if (*colon == '"') {
                    colon++;
                    char *text_end = colon;
                    while (*text_end && !(*text_end == '"' && *(text_end - 1) != '\\'))
                        text_end++;
                    size_t tlen = (size_t)(text_end - colon);
                    char *escaped = json_escape(colon, tlen);
                    resp_json(r, 200, "{\"text\":\"%s\"}", escaped ? escaped : "");
                    free(escaped);
                    return;
                }
            }
        }
        /* Fallback: wrap raw output as text */
        char *escaped = json_escape(result, result_len);
        resp_json(r, 200, "{\"text\":\"%s\"}", escaped ? escaped : "");
        free(escaped);
    } else {
        /* text/srt/vtt — return raw */
        r->status = 200;
        free(r->buf);
        r->buf = malloc(result_len + 1);
        if (r->buf) {
            memcpy(r->buf, result, result_len);
            r->buf[result_len] = '\0';
            r->len = result_len;
        }
    }
}

/* ── POST /v1/chat/completions ───────────────────────────────────── */

static void handle_chat_into(HttpRequest *req, HttpResponse *r, char *result) {
    if (!g_brief_bin[0] || access(g_brief_bin, X_OK) != 0) {
        resp_json(r, 500,
            "{\"error\":{\"message\":\"bonfyre-brief not available\","
            "\"type\":\"server_error\",\"code\":\"no_backend\"}}");
        return;
    }

    /* Extract last user message from JSON body (simple parser) */
    const char *content_key = "\"content\"";
    char *last_content = NULL;
    char *search = req->body;
    while (search && (search = strstr(search, content_key))) {
        last_content = search;
        search += strlen(content_key);
    }

    if (!last_content) {
        resp_json(r, 400,
            "{\"error\":{\"message\":\"No message content found\","
            "\"type\":\"invalid_request_error\",\"code\":\"missing_content\"}}");
        return;
    }

    /* Extract content value */
    char *colon = strchr(last_content + strlen(content_key), ':');
    if (!colon) { resp_json(r, 400, "{\"error\":{\"message\":\"Malformed JSON\"}}"); return; }
    colon++;
    while (*colon == ' ') colon++;

    char input_text[32768];
    if (*colon == '"') {
        colon++;
        size_t i = 0;
        while (*colon && i < sizeof(input_text) - 1) {
            if (*colon == '\\' && *(colon + 1)) {
                colon++;
                switch (*colon) {
                    case 'n': input_text[i++] = '\n'; break;
                    case 'r': input_text[i++] = '\r'; break;
                    case 't': input_text[i++] = '\t'; break;
                    case '"': input_text[i++] = '"'; break;
                    case '\\': input_text[i++] = '\\'; break;
                    default: input_text[i++] = *colon; break;
                }
            } else if (*colon == '"') {
                break;
            } else {
                input_text[i++] = *colon;
            }
            colon++;
        }
        input_text[i] = '\0';
    } else {
        resp_json(r, 400, "{\"error\":{\"message\":\"Content must be a string\"}}");
        return;
    }

    /* Write input to temp file */
    char input_path[MAX_PATH_LEN];
    snprintf(input_path, sizeof(input_path), "%s/chat_%d_%lx.txt", TEMP_DIR, getpid(), (unsigned long)pthread_self());
    FILE *fp = fopen(input_path, "w");
    if (!fp) { resp_json(r, 500, "{\"error\":{\"message\":\"Failed to create temp file\"}}"); return; }
    fputs(input_text, fp);
    fclose(fp);

    /* Run bonfyre-brief <transcript-file> <output-dir>; it writes brief.md,
     * brief-meta.json and artifact.json there. The reply is brief.md. */
    size_t result_len = 0;
    char out_dir[MAX_PATH_LEN];
    snprintf(out_dir, sizeof(out_dir), "%s/chat_%d_%lx.XXXXXX", TEMP_DIR, getpid(), (unsigned long)pthread_self());
    if (!mkdtemp(out_dir)) { unlink(input_path); resp_json(r, 500, "{\"error\":{\"message\":\"Failed to create output directory\"}}"); return; }
    char *argv[] = { (char *)g_brief_bin, input_path, out_dir, NULL };
    int rc = run_binary(g_brief_bin, argv, result, MAX_RESULT, &result_len);
    unlink(input_path);
    if (rc == 0) {
        char brief_path[MAX_PATH_LEN];
        snprintf(brief_path, sizeof(brief_path), "%s/brief.md", out_dir);
        FILE *bf = fopen(brief_path, "rb");
        if (!bf) rc = 1;
        else { result_len = fread(result, 1, MAX_RESULT - 1, bf); result[result_len] = '\0'; fclose(bf); }
    }
    remove_flat_dir(out_dir);

    if (rc != 0) {
        resp_json(r, 500,
            "{\"error\":{\"message\":\"Brief generation failed (exit %d)\","
            "\"type\":\"server_error\"}}", rc);
        return;
    }

    /* Format as OpenAI chat completion response */
    char *escaped = json_escape(result, result_len);
    char id_buf[64];
    snprintf(id_buf, sizeof(id_buf), "chatcmpl-bf%ld", (long)time(NULL));

    resp_json(r, 200,
        "{"
        "\"id\":\"%s\","
        "\"object\":\"chat.completion\","
        "\"created\":%ld,"
        "\"model\":\"bonfyre-brief\","
        "\"choices\":[{"
            "\"index\":0,"
            "\"message\":{\"role\":\"assistant\",\"content\":\"%s\"},"
            "\"finish_reason\":\"stop\""
        "}],"
        "\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0,\"total_tokens\":0}"
        "}",
        id_buf, (long)time(NULL), escaped ? escaped : "");
    free(escaped);
}

/* The result buffer is MAX_RESULT (4 MB) and each connection runs on a
 * THREAD_STACK (512 KB) thread, so it lives on the heap: on the stack every
 * chat or transcription request overflowed it and the server died (SIGBUS). */
static void with_result(void (*handler)(HttpRequest *, HttpResponse *, char *), HttpRequest *req, HttpResponse *r) {
    char *result = malloc(MAX_RESULT);
    if (!result) { resp_json(r, 500, "{\"error\":{\"message\":\"out of memory\",\"type\":\"server_error\"}}"); return; }
    handler(req, r, result);
    free(result);
}

/* ── Request router ──────────────────────────────────────────────── */

static void handle_request(int fd) {
    HttpRequest req;
    HttpResponse resp;
    resp_init(&resp, fd);

    if (parse_request(fd, &req) < 0) {
        resp_json(&resp, 400, "{\"error\":{\"message\":\"Bad request\"}}");
        resp_send(&resp);
        resp_free(&resp);
        return;
    }

    /* CORS preflight */
    if (strcmp(req.method, "OPTIONS") == 0) {
        resp_json(&resp, 200, "{}");
        resp_send(&resp);
        free(req.body);
        resp_free(&resp);
        return;
    }

    LOG_INFO("%s %s", req.method, req.path);

    if (strcmp(req.path, "/health") == 0) {
        resp_json(&resp, 200,
            "{\"status\":\"ok\",\"version\":\"%s\","
            "\"hcp_available\":%s,\"transcribe_available\":%s,\"brief_available\":%s}",
            VERSION,
            (g_hcp_bin[0] && access(g_hcp_bin, X_OK) == 0) ? "true" : "false",
            (g_transcribe_bin[0] && access(g_transcribe_bin, X_OK) == 0) ? "true" : "false",
            (g_brief_bin[0] && access(g_brief_bin, X_OK) == 0) ? "true" : "false");
    }
    else if (strcmp(req.path, "/v1/models") == 0 && strcmp(req.method, "GET") == 0) {
        handle_models(&resp);
    }
    else if (strcmp(req.path, "/v1/audio/transcriptions") == 0 && strcmp(req.method, "POST") == 0) {
        with_result(handle_transcription_into, &req, &resp);
    }
    else if (strcmp(req.path, "/v1/chat/completions") == 0 && strcmp(req.method, "POST") == 0) {
        with_result(handle_chat_into, &req, &resp);
    }
    else if (strncmp(req.path, "/v1/", 4) == 0) {
        resp_json(&resp, 404,
            "{\"error\":{\"message\":\"Unknown endpoint: %s\","
            "\"type\":\"invalid_request_error\"}}", req.path);
    }
    else {
        resp_json(&resp, 404, "{\"error\":{\"message\":\"Not found\"}}");
    }

    resp_send(&resp);
    free(req.body);
    resp_free(&resp);
}

/* ── Connection thread ───────────────────────────────────────────── */

static void *conn_thread(void *arg) {
    int fd = *(int *)arg;
    free(arg);
    atomic_fetch_add(&g_thread_count, 1);
    handle_request(fd);
    close(fd);
    atomic_fetch_sub(&g_thread_count, 1);
    return NULL;
}

/* ── Find binaries ───────────────────────────────────────────────── */

static void find_binary(const char *name, char *out, size_t out_len) {
    /* Check common locations */
    const char *paths[] = {
        "%s/.local/bin/%s",
        "/usr/local/bin/%s",
        "/usr/bin/%s",
        NULL
    };
    const char *home = getenv("HOME");

    for (int i = 0; paths[i]; i++) {
        char path[MAX_PATH_LEN];
        if (strstr(paths[i], "%s/.local")) {
            if (!home) continue;
            snprintf(path, sizeof(path), paths[i], home, name);
        } else {
            snprintf(path, sizeof(path), paths[i], name);
        }
        if (access(path, X_OK) == 0) {
            snprintf(out, out_len, "%s", path);
            return;
        }
    }

    /* Try PATH */
    char cmd[MAX_PATH_LEN];
    snprintf(cmd, sizeof(cmd), "which %s 2>/dev/null", name);
    FILE *fp = popen(cmd, "r");
    if (fp) {
        if (fgets(out, (int)out_len, fp)) {
            size_t len = strlen(out);
            while (len > 0 && (out[len - 1] == '\n' || out[len - 1] == '\r'))
                out[--len] = '\0';
        }
        pclose(fp);
    }
}

/* ── Signal handler ──────────────────────────────────────────────── */

static void sig_handler(int sig) {
    (void)sig;
    g_running = 0;
}

/* ── Status command ──────────────────────────────────────────────── */

static int cmd_status(void) {
    printf("{\"binary\":\"bonfyre-proxy\",\"version\":\"%s\",\"status\":\"available\"}\n", VERSION);
    return 0;
}

/* ── Serve command ───────────────────────────────────────────────── */

static int cmd_serve(int argc, char **argv) {
    /* Parse args */
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            g_port = atoi(argv[++i]);
            if (g_port < 1 || g_port > 65535) { fprintf(stderr, "Invalid port\n"); return 1; }
        } else if (strcmp(argv[i], "--hcp-bin") == 0 && i + 1 < argc) {
            snprintf(g_hcp_bin, sizeof(g_hcp_bin), "%s", argv[++i]);
        } else if (strcmp(argv[i], "--whisper-bin") == 0 && i + 1 < argc) {
            snprintf(g_hcp_bin, sizeof(g_hcp_bin), "%s", argv[++i]);
        } else if (strcmp(argv[i], "--transcribe-bin") == 0 && i + 1 < argc) {
            snprintf(g_transcribe_bin, sizeof(g_transcribe_bin), "%s", argv[++i]);
        } else if (strcmp(argv[i], "--brief-bin") == 0 && i + 1 < argc) {
            snprintf(g_brief_bin, sizeof(g_brief_bin), "%s", argv[++i]);
        }
    }

    /* Environment overrides */
    const char *env;
    if ((env = getenv("BONFYRE_PROXY_PORT"))) {
        int p = atoi(env);
        if (p > 0 && p <= 65535) g_port = p;
    }
    if ((env = getenv("HCP_WHISPER_BIN"))) snprintf(g_hcp_bin, sizeof(g_hcp_bin), "%s", env);
    if ((env = getenv("BONFYRE_TRANSCRIBE"))) snprintf(g_transcribe_bin, sizeof(g_transcribe_bin), "%s", env);

    /* Auto-discover binaries if not specified */
    if (!g_hcp_bin[0]) find_binary("hcp-whisper", g_hcp_bin, sizeof(g_hcp_bin));
    if (!g_transcribe_bin[0]) find_binary("bonfyre-transcribe", g_transcribe_bin, sizeof(g_transcribe_bin));
    if (!g_brief_bin[0]) find_binary("bonfyre-brief", g_brief_bin, sizeof(g_brief_bin));

    /* Create temp dir */
    mkdir(TEMP_DIR, 0700);

    /* Log discovered backends */
    LOG_INFO("bonfyre-proxy v%s starting on port %d", VERSION, g_port);
    LOG_INFO("  hcp-whisper:       %s", g_hcp_bin[0] ? g_hcp_bin : "(not found)");
    LOG_INFO("  bonfyre-transcribe: %s", g_transcribe_bin[0] ? g_transcribe_bin : "(not found)");
    LOG_INFO("  bonfyre-brief:      %s", g_brief_bin[0] ? g_brief_bin : "(not found)");
    LOG_INFO("  OpenAI compat:     http://localhost:%d/v1/", g_port);

    /* Setup socket */
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGPIPE, SIG_IGN);

    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons((uint16_t)g_port),
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK)  /* localhost only for security */
    };

    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(sfd);
        return 1;
    }

    if (listen(sfd, 64) < 0) {
        perror("listen");
        close(sfd);
        return 1;
    }

    LOG_INFO("Listening on http://127.0.0.1:%d (localhost only)", g_port);
    LOG_INFO("Set OPENAI_API_BASE=http://localhost:%d to use as drop-in OpenAI replacement", g_port);

    /* Accept loop */
    while (g_running) {
        struct sockaddr_in client;
        socklen_t clen = sizeof(client);
        int cfd = accept(sfd, (struct sockaddr *)&client, &clen);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            break;
        }

        if (atomic_load(&g_thread_count) >= MAX_THREADS) {
            const char *busy = "HTTP/1.1 503 Busy\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            write(cfd, busy, strlen(busy));
            close(cfd);
            continue;
        }

        int tcp_nodelay = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &tcp_nodelay, sizeof(tcp_nodelay));

        int *fd_ptr = malloc(sizeof(int));
        if (!fd_ptr) { close(cfd); continue; }
        *fd_ptr = cfd;

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setstacksize(&attr, THREAD_STACK);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&tid, &attr, conn_thread, fd_ptr) != 0) {
            close(cfd);
            free(fd_ptr);
        }
        pthread_attr_destroy(&attr);
    }

    close(sfd);
    LOG_INFO("Shutting down.");
    return 0;
}

/* ── Main ────────────────────────────────────────────────────────── */

static void usage(void) {
    fprintf(stderr,
            "bonfyre-proxy — OpenAI-compatible API shim\n\n"
            "Usage:\n"
            "  bonfyre-proxy serve [--port 8787] [--hcp-bin PATH]\n"
            "  bonfyre-proxy status\n\n"
            "Endpoints:\n"
            "  POST /v1/audio/transcriptions   Whisper-compatible transcription\n"
            "  POST /v1/chat/completions        Chat completion via bonfyre-brief\n"
            "  GET  /v1/models                  List available models\n"
            "  GET  /health                     Health check\n\n"
            "Set OPENAI_API_BASE=http://localhost:8787 for drop-in OpenAI replacement.\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    if (argc < 2) {
        usage();
        return 1;
    }

    enum { PX_SERVE, PX_STATUS };
    static const BfCommandEntry proxy_commands[] = {
        {"serve", PX_SERVE, 0, NULL},
        {"status", PX_STATUS, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(proxy_commands,
        sizeof(proxy_commands) / sizeof(proxy_commands[0]), argv[1]);
    if (command && command->id == PX_STATUS) return cmd_status();
    if (command && command->id == PX_SERVE) return cmd_serve(argc - 2, argv + 2);

    fprintf(stderr, "Unknown command: %s\n", argv[1]);
    return 1;
}
