/*
 * dp_util.c — what the products share: product-cli.mjs flags, files, JS
 * string helpers, and a loopback HTTP/1.1 client for the local model front.
 */
#include "dp.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

/* ── flags ──────────────────────────────────────────────────────────────── */

void dp_flags(DpFlags *fl, int argc, char **argv, const char *const *repeatable) {
    memset(fl, 0, sizeof *fl);
    fl->f = calloc((size_t)argc + 1, sizeof *fl->f);
    fl->rest = calloc((size_t)argc + 1, sizeof *fl->rest);
    for (int i = 0; i < argc; i++) {
        const char *arg = argv[i];
        if (strncmp(arg, "--", 2)) { fl->rest[fl->nrest++] = argv[i]; continue; }
        const char *key = arg + 2;
        const char *value = i + 1 < argc && strncmp(argv[i + 1], "--", 2) ? argv[++i] : DP_TRUE;
        int rep = 0;
        for (int r = 0; repeatable && repeatable[r]; r++) if (!strcmp(repeatable[r], key)) rep = 1;
        DpFlag *f = NULL;
        for (int k = 0; k < fl->n; k++) if (!strcmp(fl->f[k].key, key)) f = &fl->f[k];
        if (!f) { f = &fl->f[fl->n++]; f->key = strdup(key); }
        if (rep) { f->vals = realloc(f->vals, sizeof *f->vals * (size_t)(f->n + 1)); f->vals[f->n++] = strdup(value); }
        else { if (!f->vals) f->vals = malloc(sizeof *f->vals); else free(f->vals[0]); f->vals[0] = strdup(value); f->n = 1; }
    }
}
const char *dp_flag(const DpFlags *fl, const char *key) {
    for (int k = 0; k < fl->n; k++) if (!strcmp(fl->f[k].key, key)) return fl->f[k].n ? fl->f[k].vals[fl->f[k].n - 1] : NULL;
    return NULL;
}
int dp_flag_list(const DpFlags *fl, const char *key, char ***vals) {
    for (int k = 0; k < fl->n; k++) if (!strcmp(fl->f[k].key, key)) { *vals = fl->f[k].vals; return fl->f[k].n; }
    *vals = NULL; return 0;
}
int dp_flag_is_true(const DpFlags *fl, const char *key) { const char *v = dp_flag(fl, key); return v && !strcmp(v, DP_TRUE); }

/* ── files and strings ──────────────────────────────────────────────────── */

char *dp_read_file(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    size_t cap = 1 << 16, n = 0; char *b = malloc(cap);
    for (size_t got; (got = fread(b + n, 1, cap - n - 1, f)) > 0;) { n += got; if (n + 1 >= cap) b = realloc(b, cap *= 2); }
    fclose(f);
    b[n] = 0; if (len) *len = n;
    return b;
}

void dp_write_file(const char *path, const char *data, size_t len) {
    FILE *f = fopen(path, "wb");
    if (!f) dp_fail("ENOENT: no such file or directory, open '%s'", path);
    fwrite(data, 1, len, f); fclose(f);
}

static int js_space(const unsigned char *s, size_t *adv) {
    *adv = 1;
    if (*s == ' ' || (*s >= 9 && *s <= 13)) return 1;
    if (s[0] == 0xC2 && s[1] == 0xA0) { *adv = 2; return 1; }                         /* NBSP */
    if (s[0] == 0xEF && s[1] == 0xBB && s[2] == 0xBF) { *adv = 3; return 1; }          /* BOM */
    if (s[0] == 0xE2 && s[1] == 0x80 && (s[2] == 0xA8 || s[2] == 0xA9 || (s[2] >= 0x80 && s[2] <= 0x8A) || s[2] == 0xAF)) { *adv = 3; return 1; }
    if (s[0] == 0xE2 && s[1] == 0x81 && s[2] == 0x9F) { *adv = 3; return 1; }
    if (s[0] == 0xE3 && s[1] == 0x80 && s[2] == 0x80) { *adv = 3; return 1; }
    if (s[0] == 0xE1 && s[1] == 0x9A && s[2] == 0x80) { *adv = 3; return 1; }
    return 0;
}
/* String.prototype.trim */
char *dp_js_trim(const char *s) {
    const unsigned char *p = (const unsigned char *)s; size_t adv;
    while (*p && js_space(p, &adv)) p += adv;
    const unsigned char *end = p + strlen((const char *)p), *last = p;
    for (const unsigned char *q = p; q < end;) {
        if (js_space(q, &adv)) q += adv;
        else { q++; while (q < end && (*q & 0xC0) == 0x80) q++; last = q; }
    }
    size_t n = (size_t)(last - p);
    char *o = malloc(n + 1); memcpy(o, p, n); o[n] = 0;
    return o;
}

/* Number.prototype.toFixed: exact decimal value, ties away from zero. */
char *dp_to_fixed(double v, int digits) {
    if (v != v) return strdup("NaN");
    if (v >= 1e21 || v <= -1e21) { char b[32]; bf_js_number(v, b); return strdup(b); }
    int neg = v < 0; if (neg) v = -v;
    char exact[1200];
    snprintf(exact, sizeof exact, "%.1100f", v);
    char *dot = strchr(exact, '.');
    size_t ip = (size_t)(dot - exact);
    /* digits kept: integer part + `digits` fractional digits */
    char kept[600]; size_t k = 0;
    memcpy(kept, exact, ip); k = ip;
    for (int i = 0; i < digits; i++) kept[k++] = dot[1 + i];
    const char *rest = dot + 1 + digits;
    int up = *rest >= '5';   /* above half, or a tie: away from zero */
    if (up) {
        long i = (long)k - 1;
        while (i >= 0) { if (kept[i] == '9') { kept[i] = '0'; i--; } else { kept[i]++; break; } }
        if (i < 0) { memmove(kept + 1, kept, k); kept[0] = '1'; k++; ip++; }
    }
    char out[640]; size_t o = 0;
    int nonzero = 0; for (size_t i = 0; i < k; i++) if (kept[i] != '0') nonzero = 1;
    (void)nonzero;
    if (neg) out[o++] = '-';   /* (-0.001).toFixed(2) is "-0.00" */
    memcpy(out + o, kept, ip); o += ip;
    if (digits) { out[o++] = '.'; memcpy(out + o, kept + ip, (size_t)digits); o += (size_t)digits; }
    out[o] = 0;
    return strdup(out);
}

/* ── loopback HTTP ──────────────────────────────────────────────────────── */

static int parse_origin(const char *origin, char *host, size_t hn, char *port, size_t pn) {
    const char *p = strstr(origin, "://"); p = p ? p + 3 : origin;
    if (*p == '[') { const char *e = strchr(p, ']'); if (!e) return -1; snprintf(host, hn, "%.*s", (int)(e - p - 1), p + 1); p = e + 1; }
    else { size_t n = strcspn(p, ":/"); snprintf(host, hn, "%.*s", (int)n, p); p += n; }
    if (*p == ':') { p++; size_t n = strcspn(p, "/"); snprintf(port, pn, "%.*s", (int)n, p); } else snprintf(port, pn, "80");
    return 0;
}

int dp_http(const char *origin, const char *method, const char *path, const char *body, double timeout_ms, DpHttp *out) {
    memset(out, 0, sizeof *out);
    char host[256], port[16];
    if (parse_origin(origin, host, sizeof host, port, sizeof port)) { snprintf(out->error, sizeof out->error, "bad origin"); return -1; }
    struct addrinfo hints = {0}, *res = NULL;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port, &hints, &res) || !res) { snprintf(out->error, sizeof out->error, "Unable to connect. Is the computer able to access the url?"); return -1; }
    int fd = -1;
    for (struct addrinfo *a = res; a; a = a->ai_next) {
        fd = socket(a->ai_family, a->ai_socktype, a->ai_protocol);
        if (fd < 0) continue;
        struct timeval tv = {(time_t)(timeout_ms / 1000), (suseconds_t)((long)timeout_ms % 1000) * 1000};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
        if (!connect(fd, a->ai_addr, a->ai_addrlen)) break;
        close(fd); fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) { snprintf(out->error, sizeof out->error, "Unable to connect. Is the computer able to access the url?"); return -1; }
    size_t blen = body ? strlen(body) : 0;
    char *req = dp_fmt("%s %s HTTP/1.1\r\nHost: %s:%s\r\nConnection: close\r\nAccept: */*\r\n%sContent-Length: %zu\r\n\r\n",
                       method, path, host, port, body ? "Content-Type: application/json\r\n" : "", blen);
    int ok = send(fd, req, strlen(req), 0) >= 0 && (!blen || send(fd, body, blen, 0) >= 0);
    free(req);
    size_t cap = 1 << 16, n = 0; char *buf = malloc(cap);
    for (ssize_t got; ok && (got = recv(fd, buf + n, cap - n - 1, 0)) > 0;) { n += (size_t)got; if (n + 1 >= cap) buf = realloc(buf, cap *= 2); }
    close(fd);
    buf[n] = 0;
    char *head_end = strstr(buf, "\r\n\r\n");
    if (!ok || !head_end || sscanf(buf, "HTTP/%*s %d", &out->status) != 1) {
        free(buf); snprintf(out->error, sizeof out->error, "The socket connection was closed unexpectedly."); return -1;
    }
    *head_end = 0;
    char *payload = head_end + 4; size_t plen = n - (size_t)(payload - buf);
    int chunked = 0;
    for (char *h = buf; (h = strchr(h, '\n')); h++) if (!strncasecmp(h + 1, "transfer-encoding:", 18) && strstr(h, "chunked")) chunked = 1;
    if (chunked) {
        char *o = malloc(plen + 1); size_t on = 0; char *p = payload, *end = payload + plen;
        while (p < end) {
            size_t sz = strtoul(p, NULL, 16);
            char *nl = strstr(p, "\r\n"); if (!nl || !sz) break;
            p = nl + 2; if (p + sz > end) sz = (size_t)(end - p);
            memcpy(o + on, p, sz); on += sz; p += sz + 2;
        }
        o[on] = 0; out->body = o; out->len = on;
    } else { out->body = malloc(plen + 1); memcpy(out->body, payload, plen); out->body[plen] = 0; out->len = plen; }
    free(buf);
    return 0;
}

void dp_http_free(DpHttp *h) { free(h->body); h->body = NULL; }
