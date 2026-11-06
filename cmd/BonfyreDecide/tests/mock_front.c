/*
 * mock_front — a deterministic stand-in for the bonfyre-moe model front, for
 * parity tests of the DecisionPlane products. Every answer is a function of
 * the request text (sha256), so two implementations asking the same questions
 * get the same distributions. Tokens are one per byte (id = byte + 1000).
 *
 *   mock_front PORT MODEL_PATH        serves until killed
 */
#include "bf_jv.h"
#include "bonfyre.h"

#include <arpa/inet.h>
#include <math.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static const char *model_path;

static void reply(int fd, int status, const char *body) {
    char head[256];
    snprintf(head, sizeof head, "HTTP/1.1 %d X\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n", status, strlen(body));
    send(fd, head, strlen(head), 0); send(fd, body, strlen(body), 0);
}

/* logprob for item i of a request: spread from the request digest */
static double lp(const char *text, int i) {
    char hex[65], *k = malloc(strlen(text) + 16); sprintf(k, "%s#%d", text, i);
    bf_sha256_hex((const uint8_t *)k, strlen(k), hex); free(k);
    unsigned v = (unsigned)strtoul((char[9]){hex[0], hex[1], hex[2], hex[3], hex[4], hex[5], hex[6], hex[7], 0}, NULL, 16);
    return -0.05 - (double)(v % 600000) / 100000.0;   /* -0.05 .. -6.05 */
}

int main(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: mock_front PORT MODEL_PATH\n"); return 2; }
    model_path = argv[2];
    int s = socket(AF_INET, SOCK_STREAM, 0), one = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a = {0}; a.sin_family = AF_INET; a.sin_port = htons((uint16_t)atoi(argv[1])); a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(s, (struct sockaddr *)&a, sizeof a) || listen(s, 16)) { perror("mock_front"); return 1; }
    const char *slash = strrchr(model_path, '/');
    char model[256]; snprintf(model, sizeof model, "%s", slash ? slash + 1 : model_path);
    char *dot = strstr(model, ".gguf"); if (dot) *dot = 0;
    for (;;) {
        int fd = accept(s, NULL, NULL);
        if (fd < 0) continue;
        size_t cap = 1 << 20, n = 0; char *buf = malloc(cap);
        size_t need = 0;
        for (ssize_t got; (got = recv(fd, buf + n, cap - n - 1, 0)) > 0;) {
            n += (size_t)got; buf[n] = 0;
            char *he = strstr(buf, "\r\n\r\n");
            if (he && !need) { char *cl = strcasestr(buf, "Content-Length:"); need = (size_t)(he - buf) + 4 + (cl ? strtoul(cl + 15, NULL, 10) : 0); }
            if (need && n >= need) break;
            if (n + 1 >= cap) buf = realloc(buf, cap *= 2);
        }
        buf[n] = 0;
        char method[8] = "", path[1024] = "";
        sscanf(buf, "%7s %1023s", method, path);
        char *body = strstr(buf, "\r\n\r\n"); body = body ? body + 4 : buf + n;
        char out[1 << 16];
        if (!strncmp(path, "/bonfyre/lease", 14)) {
            if (strstr(path, "release=")) reply(fd, 200, "{\"lease\":7,\"released\":true}");
            else if (strstr(path, "renew=")) reply(fd, 200, "{\"lease\":7,\"renewed\":true}");
            else reply(fd, 200, "{\"lease\":7}");
        } else if (!strcmp(path, "/props")) {
            snprintf(out, sizeof out, "{\"model_path\":\"%s\"}", model_path); reply(fd, 200, out);
        } else if (!strcmp(path, "/v1/chat/completions")) {
            bf_jv *req = bf_jv_parse(body, strlen(body), NULL, 0);
            const char *content = bf_jv_s(bf_jv_get(bf_jv_at(bf_jv_get(req, "messages"), 1), "content"));
            int o = snprintf(out, sizeof out, "{\"model\":\"%s\",\"choices\":[{\"logprobs\":{\"content\":[{\"top_logprobs\":[", model);
            const char *toks[8] = {"A", " B", "C.", "D", "E", "Hello", "F", "x"};
            for (int i = 0; i < 8; i++) o += snprintf(out + o, sizeof out - (size_t)o, "%s{\"token\":\"%s\",\"logprob\":%.17g}", i ? "," : "", toks[i], lp(content ? content : "", i) - (i >= 5 ? 9 : 0));
            snprintf(out + o, sizeof out - (size_t)o, "]}]}}]}");
            reply(fd, 200, out); bf_jv_free(req);
        } else if (!strcmp(path, "/tokenize")) {
            bf_jv *req = bf_jv_parse(body, strlen(body), NULL, 0);
            const char *c = bf_jv_s(bf_jv_get(req, "content"));
            int o = snprintf(out, sizeof out, "{\"tokens\":[");
            for (const unsigned char *p = (const unsigned char *)(c ? c : ""); *p; p++) o += snprintf(out + o, sizeof out - (size_t)o, "%s%d", p == (const unsigned char *)c ? "" : ",", *p + 1000);
            snprintf(out + o, sizeof out - (size_t)o, "]}");
            reply(fd, 200, out); bf_jv_free(req);
        } else if (!strcmp(path, "/apply-template")) {
            bf_jv *req = bf_jv_parse(body, strlen(body), NULL, 0);
            char *joined = bf_jv_dump(bf_jv_get(req, "messages"), 0, 0, NULL);
            bf_jv *r = bf_jv_obj(); bf_jv_set(r, "prompt", bf_jv_str(joined));
            char *t = bf_jv_dump(r, 0, 0, NULL); reply(fd, 200, t);
            free(t); free(joined); bf_jv_free(r); bf_jv_free(req);
        } else if (!strcmp(path, "/completion")) {
            char *key = strdup(body);
            int o = snprintf(out, sizeof out, "{\"model\":\"%s.gguf\",\"completion_probabilities\":[{\"top_logprobs\":[", model);
            for (int b = 32; b < 127; b++) o += snprintf(out + o, sizeof out - (size_t)o, "%s{\"id\":%d,\"logprob\":%.17g}", b == 32 ? "" : ",", b + 1000, lp(key, b) - 1.5);
            snprintf(out + o, sizeof out - (size_t)o, "]}]}");
            reply(fd, 200, out); free(key);
        } else reply(fd, 404, "{\"error\":{\"message\":\"no route\"}}");
        free(buf);
        close(fd);
    }
}
