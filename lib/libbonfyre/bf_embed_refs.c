/*
 * bf_embed_refs.c — Named refs and reflog for the embed object store
 *
 * git has refs (HEAD, branches, tags) and the reflog. We have both.
 *
 * Named refs let you alias specific embedding hashes to human names:
 *   "best-product-embed"  → <64-hex-hash>
 *   "baseline-2026-04"    → <64-hex-hash>
 *
 * The reflog is an append-only chronological log of every embedding that
 * was stored, with a human message. It's `git reflog` for your semantic
 * knowledge graph: you can always trace when a concept was introduced.
 *
 * Filesystem layout:
 *   ~/.local/share/bonfyre/refs/<name>   — one file per named ref
 *                                           contains: "<64-hex>\n"
 *   ~/.local/share/bonfyre/reflog        — append-only event log
 *                                           format per line:
 *                                           "<ISO-UTC> <64-hex> <message>\n"
 *
 * Name rules for refs: [a-zA-Z0-9_/:\-.] only. / allowed for namespacing
 * (e.g. "heads/main", "tags/v1.0"). No path traversal: ".." is rejected.
 *
 * Thread safety: writes use atomic rename (refs) or O_APPEND (reflog).
 * Two concurrent appends to the reflog may produce interleaved bytes on
 * some filesystems; the risk is acceptable for a debug log.
 */
#define _DEFAULT_SOURCE
#include "include/bonfyre.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <ctype.h>
#include <errno.h>

#define MAX_REFPATH 4096

/* ── helpers ─────────────────────────────────────────────────── */

static const char *ref_home_(void) {
    const char *h = getenv("HOME");
    return h ? h : "/tmp";
}

static void refs_base_(char *buf, size_t sz) {
    snprintf(buf, sz, "%s/.local/share/bonfyre/refs", ref_home_());
}

static void reflog_path_(char *buf, size_t sz) {
    snprintf(buf, sz, "%s/.local/share/bonfyre/reflog", ref_home_());
}

/* Validate a ref name: allow [a-zA-Z0-9_/:\-.], reject ".." */
static int ref_name_ok_(const char *name) {
    if (!name || !*name || strlen(name) > 256) return 0;
    if (strstr(name, "..")) return 0;  /* no parent traversal */
    for (const char *p = name; *p; p++) {
        if (!isalnum((unsigned char)*p) &&
            *p != '_' && *p != '/' && *p != ':' && *p != '-' && *p != '.') return 0;
    }
    return 1;
}

static void ref_file_path_(const char *name, char *buf, size_t sz) {
    char base[MAX_REFPATH];
    refs_base_(base, sizeof(base));
    snprintf(buf, sz, "%s/%s", base, name);
}

static void hash_to_hex_r_(const uint8_t hash[32], char hex[65]) {
    static const char hc[] = "0123456789abcdef";
    for (int i = 0; i < 32; i++) {
        hex[i*2]   = hc[hash[i] >> 4];
        hex[i*2+1] = hc[hash[i] & 0xf];
    }
    hex[64] = '\0';
}

static int hex_to_hash_r_(const char *hex, uint8_t out[32]) {
    if (strlen(hex) < 64) return -1;
    for (int i = 0; i < 32; i++) {
        unsigned v;
        if (sscanf(hex + i*2, "%02x", &v) != 1) return -1;
        out[i] = (uint8_t)v;
    }
    return 0;
}

/* Recursively ensure all parent dirs of filepath exist */
static void mkparents_(const char *filepath) {
    char dir[MAX_REFPATH];
    snprintf(dir, sizeof(dir), "%s", filepath);
    char *sl = strrchr(dir, '/');
    if (sl) { *sl = '\0'; bf_ensure_dir(dir); }
}

/* ── refs API ────────────────────────────────────────────────── */

/*
 * bf_embed_ref_write — create or update a named ref.
 *
 * Writes <name> → hash atomically. Will create parent directories
 * under refs/ for namespaced names like "heads/main".
 * Returns 0 on success, -1 on error.
 */
int bf_embed_ref_write(const char *name, const uint8_t hash[32]) {
    if (!ref_name_ok_(name)) return -1;

    char base[MAX_REFPATH];
    refs_base_(base, sizeof(base));
    bf_ensure_dir(base);

    char path[MAX_REFPATH], tmp[MAX_REFPATH + 4];
    ref_file_path_(name, path, sizeof(path));
    mkparents_(path);
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);

    char hex[65];
    hash_to_hex_r_(hash, hex);

    FILE *f = fopen(tmp, "wb");
    if (!f) return -1;
    fprintf(f, "%s\n", hex);
    fclose(f);
    rename(tmp, path);
    return 0;
}

/*
 * bf_embed_ref_read — resolve a named ref to a hash.
 *
 * Returns 0 on success (hash filled), -1 if ref not found.
 */
int bf_embed_ref_read(const char *name, uint8_t hash[32]) {
    if (!ref_name_ok_(name)) return -1;
    char path[MAX_REFPATH];
    ref_file_path_(name, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    char line[80];
    int ok = (fgets(line, sizeof(line), f) != NULL);
    fclose(f);
    if (!ok) return -1;

    size_t l = strlen(line);
    while (l > 0 && (line[l-1] == '\n' || line[l-1] == '\r')) line[--l] = '\0';
    return hex_to_hash_r_(line, hash);
}

/*
 * bf_embed_ref_delete — remove a named ref.
 * Returns 0 on success (or if ref didn't exist), -1 on error.
 */
int bf_embed_ref_delete(const char *name) {
    if (!ref_name_ok_(name)) return -1;
    char path[MAX_REFPATH];
    ref_file_path_(name, path, sizeof(path));
    int rc = unlink(path);
    return (rc == 0 || errno == ENOENT) ? 0 : -1;
}

/*
 * bf_embed_ref_list — enumerate all stored refs.
 *
 * Recursively walks refs/ directory. Each ref's name is relative to
 * the refs/ base (e.g. "heads/main", "tags/v1.0").
 * *out_names is malloc'd array of malloc'd strings; caller frees each + array.
 * Returns count on success, -1 on error.
 */
static int ref_list_dir_(const char *dir, const char *prefix,
                         char ***names, size_t *n, size_t *cap) {
    DIR *d = opendir(dir);
    if (!d) return 0;
    struct dirent *de;
    while ((de = readdir(d))) {
        if (de->d_name[0] == '.') continue;
        char subpath[MAX_REFPATH];
        snprintf(subpath, sizeof(subpath), "%s/%s", dir, de->d_name);
        char fullname[MAX_REFPATH];
        if (prefix && prefix[0])
            snprintf(fullname, sizeof(fullname), "%s/%s", prefix, de->d_name);
        else
            snprintf(fullname, sizeof(fullname), "%s", de->d_name);

        struct stat st;
        if (stat(subpath, &st) != 0) continue;
        if (S_ISDIR(st.st_mode)) {
            ref_list_dir_(subpath, fullname, names, n, cap);
        } else {
            if (*n == *cap) {
                *cap *= 2;
                char **tmp = realloc(*names, *cap * sizeof(char *));
                if (!tmp) { closedir(d); return -1; }
                *names = tmp;
            }
            (*names)[(*n)++] = strdup(fullname);
        }
    }
    closedir(d);
    return 0;
}

int bf_embed_ref_list(char ***out_names, int *out_count) {
    char base[MAX_REFPATH];
    refs_base_(base, sizeof(base));

    size_t n = 0, cap = 16;
    char **names = malloc(cap * sizeof(char *));
    if (!names) return -1;

    ref_list_dir_(base, "", &names, &n, &cap);
    *out_names  = names;
    *out_count  = (int)n;
    return (int)n;
}

/* ── reflog API ──────────────────────────────────────────────── */

/*
 * bf_embed_reflog_append — write one line to the reflog.
 *
 * Format: "<ISO-8601-UTC> <64-hex> <message>\n"
 * Uses O_APPEND for atomic appends (per POSIX, single writes ≤ PIPE_BUF
 * are atomic on local filesystems).
 * Returns 0 on success, -1 on error.
 */
int bf_embed_reflog_append(const uint8_t hash[32], const char *message) {
    char base[MAX_REFPATH];
    snprintf(base, sizeof(base), "%s/.local/share/bonfyre", ref_home_());
    bf_ensure_dir(base);

    char path[MAX_REFPATH];
    reflog_path_(path, sizeof(path));

    /* ISO-8601 UTC timestamp */
    time_t now = time(NULL);
    struct tm *tm = gmtime(&now);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%SZ", tm);

    char hex[65];
    hash_to_hex_r_(hash, hex);

    /* Sanitize message: remove newlines */
    char msg[256] = "(none)";
    if (message && message[0]) {
        size_t ml = strlen(message);
        if (ml > 255) ml = 255;
        memcpy(msg, message, ml);
        msg[ml] = '\0';
        for (char *p = msg; *p; p++) if (*p == '\n' || *p == '\r') *p = ' ';
    }

    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) return -1;

    char line[512];
    int len = snprintf(line, sizeof(line), "%s %s %s\n", ts, hex, msg);
    if (len > 0 && len < (int)sizeof(line)) write(fd, line, (size_t)len);
    close(fd);
    return 0;
}

/*
 * bf_embed_reflog_read — load all reflog entries into memory.
 *
 * *out is malloc'd BfEmbedReflogEntry[*out_count]; caller frees.
 * Returns count on success, 0 if log is empty or missing, -1 on error.
 */
int bf_embed_reflog_read(BfEmbedReflogEntry **out, int *out_count) {
    char path[MAX_REFPATH];
    reflog_path_(path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) { *out = NULL; *out_count = 0; return 0; }

    size_t cap = 64, n = 0;
    BfEmbedReflogEntry *ents = malloc(cap * sizeof(BfEmbedReflogEntry));
    if (!ents) { fclose(f); return -1; }

    char line[600];
    while (fgets(line, sizeof(line), f)) {
        /* Format: "<ts> <64-hex> <message...>\n" */
        char ts[32], hex[70];
        char msg[512] = "";
        if (sscanf(line, "%31s %69s %511[^\n]", ts, hex, msg) < 2) continue;
        if (strlen(hex) != 64) continue;

        uint8_t hash[32];
        if (hex_to_hash_r_(hex, hash) != 0) continue;

        if (n == cap) {
            cap *= 2;
            BfEmbedReflogEntry *tmp = realloc(ents, cap * sizeof(*tmp));
            if (!tmp) break;
            ents = tmp;
        }

        memcpy(ents[n].hash, hash, 32);
        snprintf(ents[n].timestamp, sizeof(ents[n].timestamp), "%s", ts);
        snprintf(ents[n].message,   sizeof(ents[n].message),   "%s", msg);
        n++;
    }
    fclose(f);
    *out       = ents;
    *out_count = (int)n;
    return (int)n;
}

/*
 * bf_embed_reflog_trim — keep only the last max_entries lines.
 *
 * Rewrites the reflog atomically. Used to bound log growth.
 * Returns 0 on success, -1 on error.
 */
int bf_embed_reflog_trim(int max_entries) {
    BfEmbedReflogEntry *ents = NULL;
    int count = 0;
    if (bf_embed_reflog_read(&ents, &count) < 0) return -1;
    if (count <= max_entries) { free(ents); return 0; }

    char path[MAX_REFPATH], tmp[MAX_REFPATH + 4];
    reflog_path_(path, sizeof(path));
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);

    FILE *f = fopen(tmp, "wb");
    if (!f) { free(ents); return -1; }

    /* Write newest max_entries (entries are oldest-first; keep tail) */
    int start = count - max_entries;
    for (int i = start; i < count; i++) {
        char hex[65];
        hash_to_hex_r_(ents[i].hash, hex);
        fprintf(f, "%s %s %s\n", ents[i].timestamp, hex, ents[i].message);
    }
    fclose(f);
    free(ents);
    rename(tmp, path);
    return 0;
}
