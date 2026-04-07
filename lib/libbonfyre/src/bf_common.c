/*
 * bf_common.c — shared utilities extracted from all 38 binaries.
 *
 * Previously duplicated ~200 LOC per binary. Now shared.
 */
#include "bonfyre.h"
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* ----------------------------------------------------------------
 * Filesystem
 * ---------------------------------------------------------------- */

int bf_ensure_dir(const char *path) {
    char tmp[PATH_MAX];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    memcpy(tmp, path, len + 1);
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] == '/') {
            tmp[i] = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 1;
            tmp[i] = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 1;
    return 0;
}

int bf_file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

long bf_file_size(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 ? st.st_size : -1;
}

char *bf_read_file(const char *path, size_t *out_len) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return NULL;
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < 0) { close(fd); return NULL; }
    size_t len = (size_t)st.st_size;
    char *buf = malloc(len + 1);
    if (!buf) { close(fd); return NULL; }
    size_t rd = 0;
    while (rd < len) {
        ssize_t n = read(fd, buf + rd, len - rd);
        if (n <= 0) break;
        rd += (size_t)n;
    }
    close(fd);
    buf[rd] = '\0';
    if (out_len) *out_len = rd;
    return buf;
}

/* ----------------------------------------------------------------
 * Timestamps
 * ---------------------------------------------------------------- */

void bf_iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL);
    struct tm t;
    gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
}

/* ----------------------------------------------------------------
 * CLI arguments
 * ---------------------------------------------------------------- */

int bf_arg_has(int argc, char **argv, const char *flag) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], flag) == 0) return 1;
    }
    return 0;
}

const char *bf_arg_value(int argc, char **argv, const char *key) {
    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], key) == 0) return argv[i + 1];
    }
    return NULL;
}

/* ----------------------------------------------------------------
 * Lightweight JSON extraction
 * ---------------------------------------------------------------- */

int bf_json_str(const char *json, const char *key, char *out, size_t out_sz) {
    if (!json || !key || !out || out_sz == 0) return 0;

    /* Build search pattern: "key" */
    char pattern[256];
    int plen = snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    if (plen < 0 || (size_t)plen >= sizeof(pattern)) return 0;

    const char *p = strstr(json, pattern);
    if (!p) return 0;
    p += plen;

    /* Skip whitespace and colon */
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (*p != ':') return 0;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;

    if (*p != '"') return 0;
    p++;

    size_t j = 0;
    int escape = 0;
    for (; *p && j + 1 < out_sz; p++) {
        if (escape) { out[j++] = *p; escape = 0; continue; }
        if (*p == '\\') { escape = 1; continue; }
        if (*p == '"') break;
        out[j++] = *p;
    }
    out[j] = '\0';
    return 1;
}

int bf_json_int(const char *json, const char *key, int *out) {
    char pattern[256];
    int plen = snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    if (plen < 0 || (size_t)plen >= sizeof(pattern)) return 0;

    const char *p = strstr(json, pattern);
    if (!p) return 0;
    p += plen;

    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (*p != ':') return 0;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;

    char *end;
    long val = strtol(p, &end, 10);
    if (end == p) return 0;
    *out = (int)val;
    return 1;
}

int bf_json_double(const char *json, const char *key, double *out) {
    char pattern[256];
    int plen = snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    if (plen < 0 || (size_t)plen >= sizeof(pattern)) return 0;

    const char *p = strstr(json, pattern);
    if (!p) return 0;
    p += plen;

    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (*p != ':') return 0;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;

    char *end;
    double val = strtod(p, &end);
    if (end == p) return 0;
    *out = val;
    return 1;
}

/* ----------------------------------------------------------------
 * FNV-1a-64
 * ---------------------------------------------------------------- */

uint64_t bf_fnv1a64(uint64_t h, const void *data, size_t len) {
    const unsigned char *p = (const unsigned char *)data;
    for (size_t i = 0; i < len; i++) {
        h ^= (uint64_t)p[i];
        h *= 1099511628211ULL;
    }
    return h;
}
