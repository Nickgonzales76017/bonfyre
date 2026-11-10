#define _POSIX_C_SOURCE 200809L
#include "util.h"
#include <bonfyre.h>
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

char *bf_fm_read_file(const char *path) {
    FILE *f = fopen(path, "rb");
    long n;
    char *out;
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) || (n = ftell(f)) < 0 || fseek(f, 0, SEEK_SET)) { fclose(f); return NULL; }
    out = malloc((size_t)n + 1);
    if (!out) { fclose(f); return NULL; }
    if (n && fread(out, 1, (size_t)n, f) != (size_t)n) { free(out); fclose(f); return NULL; }
    out[n] = '\0';
    fclose(f);
    return out;
}

int bf_fm_ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) == 0) return S_ISDIR(st.st_mode) ? 0 : -1;
    return mkdir(path, 0755);
}

void bf_fm_json_string(FILE *out, const char *value) {
    const unsigned char *p = (const unsigned char *)(value ? value : "");
    fputc('"', out);
    for (; *p; ++p) {
        if (*p == '"' || *p == '\\') { fputc('\\', out); fputc(*p, out); }
        else if (*p == '\n') fputs("\\n", out);
        else if (*p == '\r') fputs("\\r", out);
        else if (*p == '\t') fputs("\\t", out);
        else if (*p < 0x20) fprintf(out, "\\u%04x", (unsigned int)*p);
        else fputc(*p, out);
    }
    fputc('"', out);
}

void bf_fm_atom(const char *value, char *out, size_t cap, size_t limit) {
    size_t w = 0;
    int dash = 0;
    const unsigned char *p = (const unsigned char *)(value ? value : "");
    while (*p && isspace(*p)) ++p;
    for (; *p && w + 1 < cap && w < limit; ++p) {
        int safe = isalnum(*p) || *p == '.' || *p == '_' || *p == ':' || *p == '-';
        if (safe) { out[w++] = (char)*p; dash = 0; }
        else if (!dash && w && w < limit) { out[w++] = '-'; dash = 1; }
    }
    while (w && out[w - 1] == '-') --w;
    if (!w) {
        const char *fallback = "fragment";
        size_t n = strlen(fallback);
        if (n >= cap) n = cap - 1;
        memcpy(out, fallback, n); w = n;
    }
    out[w] = '\0';
}

char *bf_fm_normalize_space(const char *value) {
    size_t n = strlen(value ? value : ""), w = 0;
    char *out = malloc(n + 1);
    int pending = 0;
    if (!out) return NULL;
    for (size_t i = 0; i < n; ++i) {
        unsigned char c = (unsigned char)value[i];
        if (isspace(c)) { if (w) pending = 1; continue; }
        if (pending) out[w++] = ' ';
        pending = 0;
        out[w++] = (char)c;
    }
    out[w] = '\0';
    return out;
}

void bf_fm_digest_parts(const char *const *parts, size_t count, char hex[65]) {
    BfSha256 ctx;
    uint8_t digest[32];
    const uint8_t zero = 0;
    bf_sha256_init(&ctx);
    for (size_t i = 0; i < count; ++i) {
        const char *part = parts[i] ? parts[i] : "";
        bf_sha256_update(&ctx, (const uint8_t *)part, strlen(part));
        bf_sha256_update(&ctx, &zero, 1);
    }
    bf_sha256_final(&ctx, digest);
    bf_sha256_digest_hex(digest, hex);
}

int bf_fm_safe_ident(const char *value) {
    size_t n;
    if (!value || !(n = strlen(value)) || n > 192 || !isalnum((unsigned char)value[0])) return 0;
    for (size_t i = 1; i < n; ++i) {
        unsigned char c = (unsigned char)value[i];
        if (!(isalnum(c) || c == '.' || c == '_' || c == ':' || c == '-')) return 0;
    }
    return 1;
}

int bf_fm_run(char *const argv[]) {
    pid_t pid = fork();
    int status = 0;
    if (pid < 0) return -1;
    if (pid == 0) { execvp(argv[0], argv); _exit(127); }
    if (waitpid(pid, &status, 0) < 0 || !WIFEXITED(status)) return -1;
    return WEXITSTATUS(status);
}

char *bf_fm_path_join(const char *dir, const char *leaf) {
    size_t a = strlen(dir), b = strlen(leaf);
    char *out = malloc(a + b + 2);
    if (!out) return NULL;
    memcpy(out, dir, a);
    if (a && dir[a - 1] != '/') out[a++] = '/';
    memcpy(out + a, leaf, b + 1);
    return out;
}
