/* The owner wire: WHATWG URL subset, one bounded HTTP/1.1 POST, token files. */
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>
#include "bf_picohttpparser.h"
#include "mcp.h"

/* ── URL ────────────────────────────────────────────────────── */
static int ipv4_shorthand(const char *h, char *out, size_t n) {
    struct in_addr a;
    const char *dot = strrchr(h, '.'), *last = dot ? dot + 1 : h;
    if (!*last) return 0;
    for (const char *c = last; *c; c++) if (!isxdigit((unsigned char)*c) && *c != 'x' && *c != 'X') return 0;
    if (!inet_aton(h, &a)) return -1;   /* ends in a number but is not IPv4 */
    snprintf(out, n, "%s", inet_ntoa(a));
    return 1;
}

int url_parse(const char *text, js_url *u) {
    memset(u, 0, sizeof *u);
    while (*text && (unsigned char)*text <= ' ') text++;   /* leading C0/space is stripped */
    const char *c = strchr(text, ':');
    if (!c || c == text || c - text >= 8) return -1;
    for (const char *p = text; p < c; p++) {
        if (!(isalnum((unsigned char)*p) || ((*p == '+' || *p == '-' || *p == '.') && p > text))) return -1;
        u->scheme[p - text] = (char)tolower((unsigned char)*p);
    }
    if (!isalpha((unsigned char)text[0])) return -1;
    /* Non-special schemes are valid URLs but the callers refuse them anyway. */
    if (strcmp(u->scheme, "http") && strcmp(u->scheme, "https")) { strcpy(u->path, "/"); strcpy(u->host, "x"); return 0; }
    const char *p = c + 1;
    while (*p == '/' || *p == '\\') p++;
    const char *end = p + strcspn(p, "/\\?#");
    const char *at = NULL;
    for (const char *q = p; q < end; q++) if (*q == '@') at = q;
    if (at) { if (at > p) u->has_user = 1; p = at + 1; }
    const char *hostend = end, *colon = NULL;
    if (*p == '[') { const char *rb = memchr(p, ']', (size_t)(end - p)); if (!rb) return -1; colon = rb + 1 < end && rb[1] == ':' ? rb + 1 : NULL; if (rb + 1 < end && !colon) return -1; hostend = rb + 1; }
    else { for (const char *q = p; q < end; q++) if (*q == ':') { colon = q; break; } if (colon) hostend = colon; }
    size_t hl = (size_t)(hostend - p);
    if (!hl || hl >= sizeof u->host) return -1;
    for (size_t i = 0; i < hl; i++) u->host[i] = (char)tolower((unsigned char)p[i]);
    if (u->host[0] == '[') {
        char inner[64]; struct in6_addr a6;
        if (hl - 2 >= sizeof inner) return -1;
        memcpy(inner, u->host + 1, hl - 2); inner[hl - 2] = 0;
        if (inet_pton(AF_INET6, inner, &a6) != 1) return -1;
        char pr[64]; inet_ntop(AF_INET6, &a6, pr, sizeof pr);
        snprintf(u->host, sizeof u->host, "[%s]", pr);
    } else {
        for (size_t i = 0; i < hl; i++) if (strchr(" #%/:<>?@[\\]^|", u->host[i])) return -1;
        char v4[32]; int r = ipv4_shorthand(u->host, v4, sizeof v4);
        if (r < 0) return -1;
        if (r > 0) snprintf(u->host, sizeof u->host, "%s", v4);
    }
    if (colon) {
        const char *q = colon + 1; long port = 0;
        if (q < end) {
            for (; q < end; q++) { if (!isdigit((unsigned char)*q)) return -1; port = port * 10 + (*q - '0'); if (port > 65535) return -1; }
            int dflt = !strcmp(u->scheme, "http") ? 80 : 443;
            if (port != dflt) snprintf(u->port, sizeof u->port, "%ld", port);
        }
    }
    p = end;
    size_t pl = strcspn(p, "?#");
    if (!pl) strcpy(u->path, "/");
    else {
        if (pl >= sizeof u->path) return -1;
        for (size_t i = 0; i < pl; i++) u->path[i] = p[i] == '\\' ? '/' : p[i];
    }
    p += pl;
    if (*p == '?') { size_t ql = strcspn(p + 1, "#"); u->has_search = ql > 0; p += 1 + ql; }
    if (*p == '#') u->has_hash = p[1] != 0;
    return 0;
}
void url_href(const js_url *u, char *out, size_t n) {
    snprintf(out, n, "%s://%s%s%s%s", u->scheme, u->host, *u->port ? ":" : "", u->port, u->path);
}
int url_loopback(const js_url *u) { return !strcmp(u->host, "127.0.0.1") || !strcmp(u->host, "localhost") || !strcmp(u->host, "[::1]"); }

/* ── files ──────────────────────────────────────────────────── */
int read_text_file(const char *path, char **out, size_t *len, char *err) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        const char *code = errno == ENOENT ? "ENOENT: no such file or directory" : errno == EACCES ? "EACCES: permission denied" : NULL;
        if (code) snprintf(err, ERR_MAX, "%s, open '%s'", code, path);
        else snprintf(err, ERR_MAX, "%s, open '%s'", strerror(errno), path);
        return -1;
    }
    struct stat st;
    if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) { close(fd); snprintf(err, ERR_MAX, "EISDIR: illegal operation on a directory, read"); return -1; }
    size_t cap = 4096, n = 0; char *b = malloc(cap);
    for (ssize_t r; (r = read(fd, b + n, cap - n - 1)) > 0; ) { n += (size_t)r; if (cap - n < 2) b = realloc(b, cap *= 2); }
    close(fd); b[n] = 0; *out = b; if (len) *len = n;
    return 0;
}
void random_hex(char *out, size_t bytes) {
    unsigned char buf[64];
#if defined(__APPLE__) || defined(__OpenBSD__) || defined(__FreeBSD__)
    arc4random_buf(buf, bytes);
#else
    FILE *u = fopen("/dev/urandom", "rb"); if (!u || fread(buf, 1, bytes, u) != bytes) abort(); fclose(u);
#endif
    for (size_t i = 0; i < bytes; i++) sprintf(out + 2 * i, "%02x", buf[i]);
}

/* ── HTTP ───────────────────────────────────────────────────── */
static long now_ms(void) { struct timeval t; gettimeofday(&t, NULL); return t.tv_sec * 1000L + t.tv_usec / 1000; }
static int timed_out(long deadline, char *err) {
    if (now_ms() < deadline) return 0;
    snprintf(err, ERR_MAX, "The operation timed out."); return 1;
}
static int wait_fd(int fd, short ev, long deadline, char *err) {
    for (;;) {
        long left = deadline - now_ms();
        if (left <= 0) { snprintf(err, ERR_MAX, "The operation timed out."); return -1; }
        struct pollfd p = { fd, ev, 0 };
        int r = poll(&p, 1, (int)(left > 1000000 ? 1000000 : left));
        if (r > 0) return 0;
        if (r < 0 && errno != EINTR) { snprintf(err, ERR_MAX, "%s", strerror(errno)); return -1; }
    }
}

void http_reply_free(http_reply *r) { free(r->body); memset(r, 0, sizeof *r); }

/* Parse a complete or close-delimited response held in buf. */
static int parse_reply(char *buf, size_t n, size_t max_bytes, http_reply *r, int *complete) {
    int minor, status; const char *msg; size_t msglen, nh = 64; struct phr_header h[64];
    int hl = phr_parse_response(buf, n, &minor, &status, &msg, &msglen, h, &nh, 0);
    *complete = 0;
    if (hl == -2) return 0;
    if (hl < 0) return -1;
    r->status = status; r->content_length = -1;
    int chunked = 0;
    for (size_t i = 0; i < nh; i++) {
        if (h[i].name_len == 14 && !strncasecmp(h[i].name, "content-length", 14)) {
            r->content_length = 0;
            for (size_t k = 0; k < h[i].value_len; k++) {
                if (!isdigit((unsigned char)h[i].value[k])) { r->content_length = -2; break; }
                if (r->content_length < 1000000000000L) r->content_length = r->content_length * 10 + (h[i].value[k] - '0');
            }
            if (!h[i].value_len) r->content_length = -2;
        }
        if (h[i].name_len == 17 && !strncasecmp(h[i].name, "transfer-encoding", 17) &&
            h[i].value_len >= 7 && !strncasecmp(h[i].value + h[i].value_len - 7, "chunked", 7)) chunked = 1;
    }
    size_t have = n - (size_t)hl;
    r->has_body = !(status == 204 || status == 304 || (status >= 100 && status < 200));
    if (!r->has_body) { *complete = 1; return 0; }
    if (chunked) {
        /* Decode size-line/data pairs; incomplete input waits for more bytes. */
        const char *s = buf + hl, *e = buf + n;
        char *copy = malloc(have + 1); size_t sz = 0; int finished = 0;
        while (s < e) {
            const char *nl = memchr(s, '\n', (size_t)(e - s));
            if (!nl) break;
            char *endp; unsigned long len = strtoul(s, &endp, 16);
            if (endp == s) { free(copy); return -1; }
            if (len == 0) { finished = 1; break; }
            if ((size_t)(e - (nl + 1)) < len + 2) break;
            memcpy(copy + sz, nl + 1, len); sz += len; s = nl + 1 + len + 2;
            if (sz > max_bytes) { r->over_budget = 1; break; }
        }
        if (finished || r->over_budget) { free(r->body); r->body = copy; r->body_len = sz; *complete = 1; return 0; }
        free(copy); return 0;
    }
    if (r->content_length >= 0) {
        if ((size_t)r->content_length > max_bytes) { *complete = 1; return 0; }
        if (have >= (size_t)r->content_length) {
            free(r->body); r->body = malloc((size_t)r->content_length + 1);
            memcpy(r->body, buf + hl, (size_t)r->content_length); r->body_len = (size_t)r->content_length; *complete = 1;
        }
        return 0;
    }
    if (have > max_bytes) { r->over_budget = 1; *complete = 1; }
    free(r->body); r->body = malloc(have + 1); memcpy(r->body, buf + hl, have); r->body_len = have;
    return 0;
}

static int connect_to(const js_url *u, long deadline, char *err) {
    char host[256]; snprintf(host, sizeof host, "%s", u->host);
    if (host[0] == '[') { memmove(host, host + 1, strlen(host)); host[strlen(host) - 1] = 0; }
    struct addrinfo hints = { 0 }, *res = NULL; hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, *u->port ? u->port : "80", &hints, &res) != 0) { snprintf(err, ERR_MAX, "Unable to connect. Is the computer able to access the url?"); return -1; }
    int fd = -1;
    for (struct addrinfo *a = res; a; a = a->ai_next) {
        fd = socket(a->ai_family, a->ai_socktype | 0, a->ai_protocol);
        if (fd < 0) continue;
        fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
#ifdef SO_NOSIGPIPE
        int on = 1; setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof on);
#endif
        if (connect(fd, a->ai_addr, a->ai_addrlen) == 0) break;
        if (errno == EINPROGRESS) {
            if (wait_fd(fd, POLLOUT, deadline, err)) { close(fd); freeaddrinfo(res); return -2; }
            int so = 0; socklen_t sl = sizeof so; getsockopt(fd, SOL_SOCKET, SO_ERROR, &so, &sl);
            if (!so) break;
        }
        close(fd); fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) snprintf(err, ERR_MAX, "Unable to connect. Is the computer able to access the url?");
    return fd;
}

static int http_plain(const js_url *u, const char *token, const char *body, size_t body_len,
                      long timeout_ms, size_t max_bytes, http_reply *r, char *err) {
    long deadline = now_ms() + timeout_ms;
    int fd = connect_to(u, deadline, err);
    if (fd < 0) return -1;
    size_t tl = strlen(token);
    size_t hcap = tl + strlen(u->host) + strlen(u->path) + 512;
    char *req = malloc(hcap + body_len);
    int hn = snprintf(req, hcap, "POST %s HTTP/1.1\r\nHost: %s%s%s\r\nauthorization: Bearer %s\r\ncontent-type: application/json\r\n"
        "MCP-Protocol-Version: 2025-06-18\r\nAccept: */*\r\nConnection: close\r\nContent-Length: %zu\r\n\r\n",
        u->path, u->host, *u->port ? ":" : "", u->port, token, body_len);
    memcpy(req + hn, body, body_len);
    size_t total = (size_t)hn + body_len, sent = 0;
    while (sent < total) {
        ssize_t w = write(fd, req + sent, total - sent);
        if (w > 0) { sent += (size_t)w; continue; }
        if (w < 0 && (errno == EAGAIN || errno == EINTR)) { if (wait_fd(fd, POLLOUT, deadline, err)) goto fail; continue; }
        snprintf(err, ERR_MAX, "The socket connection was closed unexpectedly. For more information, pass `verbose: true` in the second argument to fetch()");
        goto fail;
    }
    free(req); req = NULL;
    size_t cap = 65536, n = 0; char *buf = malloc(cap); int complete = 0;
    for (;;) {
        if (wait_fd(fd, POLLIN, deadline, err)) { free(buf); goto fail; }
        if (cap - n < 4096) { if (cap > max_bytes + 1048576) { r->over_budget = 1; break; } buf = realloc(buf, cap *= 2); }
        ssize_t got = read(fd, buf + n, cap - n);
        if (got < 0 && (errno == EAGAIN || errno == EINTR)) continue;
        if (got <= 0) break;
        n += (size_t)got;
        if (parse_reply(buf, n, max_bytes, r, &complete) < 0) { free(buf); snprintf(err, ERR_MAX, "Malformed HTTP response"); goto fail; }
        if (complete) break;
    }
    if (!complete && !r->over_budget) {
        if (n == 0 || parse_reply(buf, n, max_bytes, r, &complete) < 0 || r->status == 0) {
            free(buf); snprintf(err, ERR_MAX, "The socket connection was closed unexpectedly. For more information, pass `verbose: true` in the second argument to fetch()"); goto fail;
        }
    }
    free(buf); close(fd);
    if (timed_out(deadline + 1, err)) return -1;
    return 0;
fail:
    free(req); close(fd); return -1;
}

/* HTTPS owners go through the system curl; the token never reaches argv. */
static int http_curl(const js_url *u, const char *token, const char *body, size_t body_len,
                     long timeout_ms, size_t max_bytes, http_reply *r, char *err) {
    char dir[] = "/tmp/bonfyre-mcp-XXXXXX";
    const char *tmp = getenv("TMPDIR"); char tdir[1024];
    snprintf(tdir, sizeof tdir, "%s/bonfyre-mcp-XXXXXX", tmp && *tmp ? tmp : "/tmp");
    char *d = mkdtemp(tdir); if (!d) d = mkdtemp(dir);
    if (!d) { snprintf(err, ERR_MAX, "%s", strerror(errno)); return -1; }
    char hpath[1100], bpath[1100], opath[1100], url[3000];
    snprintf(hpath, sizeof hpath, "%s/h", d); snprintf(bpath, sizeof bpath, "%s/b", d); snprintf(opath, sizeof opath, "%s/o", d);
    url_href(u, url, sizeof url);
    FILE *f = fopen(hpath, "w"); fprintf(f, "authorization: Bearer %s\ncontent-type: application/json\nMCP-Protocol-Version: 2025-06-18\n", token); fclose(f);
    f = fopen(bpath, "w"); fwrite(body, 1, body_len, f); fclose(f);
    char maxt[32], hat[1200], dat[1200]; snprintf(maxt, sizeof maxt, "%.3f", timeout_ms / 1000.0);
    snprintf(hat, sizeof hat, "@%s", hpath); snprintf(dat, sizeof dat, "@%s", bpath);
    int rc = -1; pid_t pid = fork();
    if (pid == 0) {
        int o = open(opath, O_WRONLY | O_CREAT | O_TRUNC, 0600); dup2(o, 1);
        int nul = open("/dev/null", O_WRONLY); dup2(nul, 2);
        execl("/usr/bin/curl", "curl", "-sS", "-i", "--proto", "=https", "--max-redirs", "0", "--max-time", maxt,
              "-X", "POST", "-H", hat, "--data-binary", dat, url, (char *)NULL);
        _exit(127);
    }
    int st = 0; if (pid > 0) waitpid(pid, &st, 0);
    char *out = NULL; size_t on = 0, complete = 0; char e2[ERR_MAX];
    if (pid > 0 && WIFEXITED(st) && WEXITSTATUS(st) == 28) snprintf(err, ERR_MAX, "The operation timed out.");
    else if (pid > 0 && WIFEXITED(st) && WEXITSTATUS(st) == 0 && read_text_file(opath, &out, &on, e2) == 0) {
        int c = 0;
        if (parse_reply(out, on, max_bytes, r, &c) == 0 && r->status) { rc = 0; complete = 1; }
        else snprintf(err, ERR_MAX, "Malformed HTTP response");
    } else snprintf(err, ERR_MAX, "Unable to connect. Is the computer able to access the url?");
    (void)complete;
    free(out); unlink(hpath); unlink(bpath); unlink(opath); rmdir(d);
    return rc;
}

int http_post(const js_url *u, const char *token, const char *body, size_t body_len,
              long timeout_ms, size_t max_bytes, http_reply *r, char *err) {
    memset(r, 0, sizeof *r);
    signal(SIGPIPE, SIG_IGN);
    int rc = !strcmp(u->scheme, "https") ? http_curl(u, token, body, body_len, timeout_ms, max_bytes, r, err)
                                         : http_plain(u, token, body, body_len, timeout_ms, max_bytes, r, err);
    /* redirect: "error" refuses every redirect status, with or without Location. */
    if (!rc && (r->status == 301 || r->status == 302 || r->status == 303 || r->status == 307 || r->status == 308)) {
        char href[4096]; url_href(u, href, sizeof href);
        snprintf(err, ERR_MAX, "UnexpectedRedirect fetching \"%s\". For more information, pass `verbose: true` in the second argument to fetch()", href);
        http_reply_free(r); return -1;
    }
    return rc;
}
