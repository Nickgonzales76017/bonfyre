/*
 * bf_common.c — shared utilities extracted from all 38 binaries.
 *
 * Previously duplicated ~200 LOC per binary. Now shared.
 */
#include <stdint.h>
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

#include "bf_lex.h"
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* ----------------------------------------------------------------
 * Filesystem
 * ---------------------------------------------------------------- */

/* A directory that already exists. A state root is declared by the operator or
 * created by the installer; an organ that mkdir -p's its own root would just be
 * inventing a store one level higher up. */
static int bf_dir_present(const char *path) {
    struct stat st;
    return path && *path && stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

static int bf_parent_present(const char *file_path) {
    char dir[PATH_MAX];
    const char *slash = strrchr(file_path, '/');
    if (!slash || (size_t)(slash - file_path) >= sizeof(dir)) return 0;
    memcpy(dir, file_path, (size_t)(slash - file_path));
    dir[slash - file_path] = '\0';
    return bf_dir_present(dir);
}

const char *bf_state_path(char *buf, size_t n, const char *env_name,
                          const char *relative, const char *explicit_path) {
    const char *env, *home;

    if (explicit_path && *explicit_path) return explicit_path;
    if (!buf || n == 0 || !relative || !*relative) return NULL;

    if (env_name && *env_name) {
        env = getenv(env_name);
        if (env && *env) { snprintf(buf, n, "%s", env); return buf; }
    }

    env = getenv("BONFYRE_STATE_ROOT");
    if (env && *env) {
        snprintf(buf, n, "%s/%s", env, relative);
        if (bf_parent_present(buf)) return buf;
    }

    snprintf(buf, n, "/var/lib/bonfyre/%s", relative);
    if (bf_parent_present(buf)) return buf;

    home = getenv("HOME");
    if (home && *home) {
        snprintf(buf, n, "%s/.local/share/bonfyre/%s", home, relative);
        if (bf_parent_present(buf)) return buf;
    }
    return NULL;
}

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

/* Create only the parent directory of a file path.
 * e.g. "/a/b/c.db" -> creates "/a/b"
 * Safe to call when the path has no directory component. */
int bf_ensure_parent_dir(const char *filepath) {
    char tmp[PATH_MAX];
    size_t len = strlen(filepath);
    if (len == 0 || len >= sizeof(tmp)) return 0;
    memcpy(tmp, filepath, len + 1);
    char *slash = strrchr(tmp, '/');
    if (!slash || slash == tmp) return 0;  /* no parent to create */
    *slash = '\0';
    return bf_ensure_dir(tmp);
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
    struct tm *utc = gmtime(&now);
    if (utc) {
        t = *utc;
    } else {
        memset(&t, 0, sizeof(t));
    }
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

/* Delegates to the one implementation.
 *
 * This was the estate's other shared JSON reader, with 47 callers, and it
 * carried three defects at once. Its unescaping was `if (escape) out[j++] =
 * *p`, so \n arrived as the letter n, \t as t and \u00e9 as u00e9. It
 * searched for the key with strstr, so it matched inside a value and at any
 * depth. And on a value too long for the buffer it wrote a prefix and returned
 * success, so a digest truncated by one character was indistinguishable from a
 * whole one.
 *
 * Two shared readers that disagree are not a shared reader. */
int bf_json_str(const char *json, const char *key, char *out, size_t out_sz) {
    return bf_lex_json_string(json, key, out, out_sz);
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
