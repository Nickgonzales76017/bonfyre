/* fixture-serve: a scripted loopback owner for golden vectors. It records what
 * a client sent (request id normalised to @ID@) and answers from a script, so
 * the retired JS client and the native client can be compared byte for byte.
 *
 *   bonfyre-mcp fixture-serve PORTFILE SCRIPT RECORD
 * SCRIPT lines: STATUS<TAB>CONTENT-LENGTH|-<TAB>BODY   (@ID@ in BODY = request id)
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include "bf_jsonw.h"
#include "bf_picohttpparser.h"
#include "mcp.h"

static char *replace_all(const char *s, const char *from, const char *to) {
    size_t fl = strlen(from), tl = strlen(to), n = 0;
    for (const char *p = s; (p = strstr(p, from)); p += fl) n++;
    char *o = malloc(strlen(s) + n * (tl + 1) + 1), *w = o;
    for (const char *p; (p = strstr(s, from)); s = p + fl) { memcpy(w, s, (size_t)(p - s)); w += p - s; memcpy(w, to, tl); w += tl; }
    strcpy(w, s);
    return o;
}

int fixture_serve(int argc, char **argv) {
    if (argc != 3) { fprintf(stderr, "usage: bonfyre-mcp fixture-serve PORTFILE SCRIPT RECORD\n"); return 2; }
    char err[ERR_MAX], *script; size_t sl;
    if (read_text_file(argv[1], &script, &sl, err)) { fprintf(stderr, "%s\n", err); return 1; }
    int fd = socket(AF_INET, SOCK_STREAM, 0), on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on);
    struct sockaddr_in a = { 0 }; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd, (struct sockaddr *)&a, sizeof a) || listen(fd, 16)) { perror("bind"); return 1; }
    socklen_t al = sizeof a; getsockname(fd, (struct sockaddr *)&a, &al);
    FILE *pf = fopen(argv[0], "w"); fprintf(pf, "%d\n", ntohs(a.sin_port)); fclose(pf);
    FILE *rec = fopen(argv[2], "a");
    char *line = script;
    while (line && *line) {
        char *next = strchr(line, '\n'); if (next) *next++ = 0;
        if (!*line) { line = next; continue; }
        struct pollfd p = { fd, POLLIN, 0 };
        if (poll(&p, 1, 15000) <= 0) break;
        int c = accept(fd, NULL, NULL); if (c < 0) continue;
        size_t cap = 1 << 20, n = 0; char *buf = malloc(cap); int done = 0; size_t hl = 0, clen = 0;
        const char *method, *path; size_t ml, pl, nh = 64; int minor; struct phr_header h[64];
        while (!done) {
            struct pollfd q = { c, POLLIN, 0 }; if (poll(&q, 1, 10000) <= 0) break;
            ssize_t r = read(c, buf + n, cap - n - 1); if (r <= 0) break; n += (size_t)r;
            if (n + 1 >= cap) buf = realloc(buf, cap *= 2);
            nh = 64;
            int ret = phr_parse_request(buf, n, &method, &ml, &path, &pl, &minor, h, &nh, 0);
            if (ret > 0) {
                hl = (size_t)ret; clen = 0;
                for (size_t i = 0; i < nh; i++) if (h[i].name_len == 14 && !strncasecmp(h[i].name, "content-length", 14)) clen = strtoul(h[i].value, NULL, 10);
                if (n >= hl + clen) done = 1;
            }
        }
        bf_jv *entry = bf_jv_obj(), *body = NULL; char id[256] = "";
        if (done) {
            char tmp[2048]; snprintf(tmp, sizeof tmp, "%.*s %.*s", (int)ml, method, (int)pl, path);
            bf_jv_set(entry, "request", bf_jv_str(tmp));
            const char *names[] = { "authorization", "content-type", "mcp-protocol-version" };
            for (int k = 0; k < 3; k++) for (size_t i = 0; i < nh; i++)
                if (h[i].name_len == strlen(names[k]) && !strncasecmp(h[i].name, names[k], h[i].name_len)) bf_jv_set(entry, names[k], bf_jv_strn(h[i].value, h[i].value_len));
            body = jsx_parse(buf + hl, clen, err);
            const bf_jv *rid = jsx_get(body, "id");
            if (rid && rid->type == BF_JV_STR) { snprintf(id, sizeof id, "%s", rid->s); bf_jv_set(body, "id", bf_jv_str("@ID@")); }
            bf_jv_set(entry, "body", body ? body : bf_jv_strn(buf + hl, clen));
            const bf_jv *fpath = jsx_get(jsx_get(jsx_get(body, "params"), "arguments"), "path");
            char *content; size_t cl;
            if (fpath && fpath->type == BF_JV_STR && read_text_file(fpath->s, &content, &cl, err) == 0) { bf_jv_set(entry, "file", bf_jv_strn(content, cl)); free(content); }
        }
        bf_jsonw w; bf_jsonw_init(&w, 0); bf_jv_write(&w, entry, BF_JV_SORT_BYTES);
        fprintf(rec, "%s\n", w.buf); fflush(rec); bf_jsonw_free(&w); bf_jv_free(entry);
        char *t1 = strchr(line, '\t'), *t2 = t1 ? strchr(t1 + 1, '\t') : NULL;
        int status = atoi(line); const char *cl = t1 ? t1 + 1 : "-";
        char *payload = replace_all(t2 ? t2 + 1 : "", "@ID@", id);
        char head[512]; size_t bl = strlen(payload);
        int hn = cl[0] == '-' ? snprintf(head, sizeof head, "HTTP/1.1 %d X\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n", status, bl)
                             : snprintf(head, sizeof head, "HTTP/1.1 %d X\r\nContent-Type: application/json\r\nContent-Length: %.*s\r\nConnection: close\r\n\r\n", status, (int)(t2 - cl), cl);
        if (write(c, head, (size_t)hn) < 0 || write(c, payload, bl) < 0) { /* the client may refuse early */ }
        free(payload); free(buf); shutdown(c, SHUT_WR); close(c);
        line = next;
    }
    fclose(rec); close(fd); free(script);
    return 0;
}
