/*
 * BonfyreHash — content-addressing + Merkle DAG hashing engine.
 *
 * Owns every hash in the system. The single source of truth for:
 *   - File content hashes (SHA-256)
 *   - Operator node hashes (DAG hashing)
 *   - Merkle roots (family integrity)
 *   - Dedup detection (same hash = same content, pay once)
 *
 * Usage:
 *   bonfyre-hash file <path>                     — SHA-256 of file
 *   bonfyre-hash node <op> <version> <params_json> <child_hash,...>  — operator node hash
 *   bonfyre-hash merkle <artifact.json>           — compute/update root_hash
 *   bonfyre-hash verify <artifact.json>           — verify all hashes
 *   bonfyre-hash dedup <dir>                      — find duplicate files by hash
 */
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include "bonfyre.h"

#define MAX_LINE 65536
#define MAX_CHILDREN 256
#define MAX_FILES 4096
#define HASH_LEN 65

/* ---------- SHA-256 (delegates to lib/libbonfyre) ---------- */

typedef BfSha256 SHA256_CTX;

static void sha256_init(SHA256_CTX *c) { bf_sha256_init(c); }
static void sha256_update(SHA256_CTX *c, const unsigned char *data, size_t len) { bf_sha256_update(c, data, len); }
static void sha256_final(SHA256_CTX *c, unsigned char out[32]) { bf_sha256_final(c, out); }
static void sha256_hex(const unsigned char hash[32], char hex[65]) { bf_sha256_digest_hex(hash, hex); }

/* ---------- commands ---------- */

static int cmd_file(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) { fprintf(stderr, "Cannot open: %s\n", path); return 1; }
    SHA256_CTX ctx;
    sha256_init(&ctx);
    unsigned char buf[8192];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0)
        sha256_update(&ctx, buf, n);
    fclose(fp);
    unsigned char h[32];
    sha256_final(&ctx, h);
    char hex[65];
    sha256_hex(h, hex);
    printf("%s  %s\n", hex, path);
    return 0;
}

/* #8: qsort comparator for child hash pointers */
static int cmp_str_ptrs(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

static int cmd_node(const char *op, const char *version, const char *params_json, const char *children_csv) {
    /* Canonical: sort children, concatenate op + params + children hashes */
    char *children[MAX_CHILDREN];
    int nchildren = 0;
    char *csv_copy = strdup(children_csv);
    char *tok = strtok(csv_copy, ",");
    while (tok && nchildren < MAX_CHILDREN) {
        while (*tok == ' ') tok++;
        children[nchildren++] = strdup(tok);
        tok = strtok(NULL, ",");
    }
    free(csv_copy);

    /* #8: qsort instead of bubble sort — O(n log n) */
    qsort(children, (size_t)nchildren, sizeof(char *), cmp_str_ptrs);

    /* Build canonical string */
    SHA256_CTX ctx;
    sha256_init(&ctx);
    sha256_update(&ctx, (const unsigned char *)op, strlen(op));
    sha256_update(&ctx, (const unsigned char *)"|", 1);
    sha256_update(&ctx, (const unsigned char *)version, strlen(version));
    sha256_update(&ctx, (const unsigned char *)"|", 1);
    sha256_update(&ctx, (const unsigned char *)params_json, strlen(params_json));
    sha256_update(&ctx, (const unsigned char *)"|", 1);
    for (int i = 0; i < nchildren; i++) {
        sha256_update(&ctx, (const unsigned char *)children[i], strlen(children[i]));
        if (i < nchildren - 1)
            sha256_update(&ctx, (const unsigned char *)",", 1);
        free(children[i]);
    }
    unsigned char h[32];
    sha256_final(&ctx, h);
    char hex[65];
    sha256_hex(h, hex);
    printf("%s\n", hex);
    return 0;
}

/* #6: qsort comparator for dedup entries (sort by hash) */
static int cmp_entry_hash(const void *a, const void *b) {
    typedef struct { char hash[65]; char path[PATH_MAX]; unsigned long size; } Entry;
    return strcmp(((const Entry *)a)->hash, ((const Entry *)b)->hash);
}

static int cmd_dedup(const char *dir) {
    /* Walk directory, hash all files, report duplicates */
    typedef struct { char hash[65]; char path[PATH_MAX]; unsigned long size; } Entry;

    /* #7: Dynamic growth instead of fixed MAX_FILES calloc (saves ~4.3MB) */
    int cap = 128;
    int count = 0;
    Entry *entries = malloc((size_t)cap * sizeof(Entry));
    if (!entries) return 1;

    DIR *d = opendir(dir);
    if (!d) { fprintf(stderr, "Cannot open: %s\n", dir); free(entries); return 1; }
    struct dirent *ent;
    while ((ent = readdir(d))) {
        if (ent->d_name[0] == '.') continue;
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name);
        struct stat st;
        if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) continue;

        /* Grow if needed */
        if (count >= cap) {
            cap *= 2;
            Entry *tmp = realloc(entries, (size_t)cap * sizeof(Entry));
            if (!tmp) { free(entries); closedir(d); return 1; }
            entries = tmp;
        }

        FILE *fp = fopen(path, "rb");
        if (!fp) continue;
        SHA256_CTX ctx;
        sha256_init(&ctx);
        unsigned char buf[8192];
        size_t n;
        while ((n = fread(buf, 1, sizeof(buf), fp)) > 0)
            sha256_update(&ctx, buf, n);
        fclose(fp);
        unsigned char h[32];
        sha256_final(&ctx, h);
        sha256_hex(h, entries[count].hash);
        snprintf(entries[count].path, PATH_MAX, "%s", path);
        entries[count].size = (unsigned long)st.st_size;
        count++;
    }
    closedir(d);

    /* #6: Sort by hash, then linear scan for duplicates — O(n log n) */
    qsort(entries, (size_t)count, sizeof(Entry), cmp_entry_hash);

    unsigned long wasted = 0;
    int dupes = 0;
    for (int i = 0; i < count - 1; i++) {
        if (strcmp(entries[i].hash, entries[i+1].hash) == 0) {
            printf("DUP %s  %s  (%lu bytes)\n", entries[i].path, entries[i+1].path, entries[i].size);
            wasted += entries[i+1].size;
            dupes++;
        }
    }
    if (dupes == 0) printf("No duplicates found in %d files.\n", count);
    else printf("\n%d duplicate pairs, %lu bytes reclaimable.\n", dupes, wasted);

    free(entries);
    return 0;
}

/* ---------- #10: Inline Merkle (replaces system() + Python) ---------- */

/* Extract quoted string values matching "content_hash" from artifact.json */
static int extract_content_hashes(const char *json, char hashes[][65], int max_hashes) {
    const char *needle = "\"content_hash\"";
    size_t needle_len = strlen(needle);
    int count = 0;
    const char *p = json;
    while ((p = strstr(p, needle)) && count < max_hashes) {
        p += needle_len;
        while (*p && (*p == ' ' || *p == ':' || *p == '\t')) p++;
        if (*p != '"') continue;
        p++;
        int i = 0;
        while (*p && *p != '"' && i < 64) hashes[count][i++] = *p++;
        hashes[count][i] = '\0';
        if (i == 64) count++;
    }
    return count;
}

static int cmp_hash_strings(const void *a, const void *b) {
    return strcmp((const char *)a, (const char *)b);
}

static int cmd_merkle(const char *artifact_path, int verify_only) {
    /* Read artifact.json */
    FILE *f = fopen(artifact_path, "rb");
    if (!f) { fprintf(stderr, "Cannot open: %s\n", artifact_path); return 1; }
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz <= 0 || sz > 1048576) { fclose(f); return 1; }
    char *json = malloc((size_t)sz + 1);
    if (!json) { fclose(f); return 1; }
    fread(json, 1, (size_t)sz, f);
    json[sz] = '\0';
    fclose(f);

    /* Extract all content_hash values from atoms */
    char (*hashes)[65] = malloc(4096 * 65);
    if (!hashes) { free(json); return 1; }
    int nhashes = extract_content_hashes(json, hashes, 4096);
    if (nhashes == 0) {
        fprintf(stderr, "No content_hash entries found\n");
        free(hashes); free(json);
        return 1;
    }

    /* Sort hashes for canonical ordering */
    qsort(hashes, (size_t)nhashes, 65, cmp_hash_strings);

    /* Build Merkle root: iteratively pair-hash until one remains */
    while (nhashes > 1) {
        int next = 0;
        for (int i = 0; i < nhashes; i += 2) {
            SHA256_CTX ctx;
            sha256_init(&ctx);
            sha256_update(&ctx, (const unsigned char *)hashes[i], strlen(hashes[i]));
            if (i + 1 < nhashes)
                sha256_update(&ctx, (const unsigned char *)hashes[i+1], strlen(hashes[i+1]));
            unsigned char h[32];
            sha256_final(&ctx, h);
            sha256_hex(h, hashes[next]);
            next++;
        }
        nhashes = next;
    }

    char *root_hash = hashes[0];

    if (verify_only) {
        /* Check if existing root_hash matches */
        const char *rh = strstr(json, "\"root_hash\"");
        if (rh) {
            rh += 11;
            while (*rh && (*rh == ' ' || *rh == ':' || *rh == '\t' || *rh == '"')) rh++;
            if (strncmp(rh, root_hash, 64) == 0)
                printf("VERIFIED: root_hash matches (%s)\n", root_hash);
            else
                printf("MISMATCH: computed=%s\n", root_hash);
        } else {
            printf("No root_hash field found; computed=%s\n", root_hash);
        }
    } else {
        printf("%s\n", root_hash);
    }

    free(hashes);
    free(json);
    return 0;
}

static char *read_text_file(const char *path, long *sz_out) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz <= 0 || sz > (16 * 1024 * 1024)) {
        fclose(f);
        return NULL;
    }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t n = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    if ((long)n != sz) {
        free(buf);
        return NULL;
    }
    buf[sz] = '\0';
    if (sz_out) *sz_out = sz;
    return buf;
}

static int json_extract_string_key(const char *json, const char *key, char *out, size_t out_sz) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p = strchr(p + (int)strlen(needle), ':');
    if (!p) return 0;
    p++;
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != '"') return 0;
    p++;
    size_t i = 0;
    while (*p && *p != '"' && i + 1 < out_sz) out[i++] = *p++;
    out[i] = '\0';
    return i > 0;
}

static int json_extract_int_key(const char *json, const char *key, int *out) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p = strchr(p + (int)strlen(needle), ':');
    if (!p) return 0;
    p++;
    while (*p && isspace((unsigned char)*p)) p++;
    *out = atoi(p);
    return 1;
}

static int cmp_int_asc(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    return (ia > ib) - (ia < ib);
}

static int extract_feature_ids(const char *json, int *ids, int max_ids) {
    const char *needle = "\"feature_id\"";
    int n = 0;
    const char *p = json;
    while ((p = strstr(p, needle)) && n < max_ids) {
        p += strlen(needle);
        p = strchr(p, ':');
        if (!p) break;
        p++;
        while (*p && isspace((unsigned char)*p)) p++;
        ids[n++] = atoi(p);
    }
    return n;
}

static int cmd_feature(const char *manifest_path) {
    long sz = 0;
    char *json = read_text_file(manifest_path, &sz);
    if (!json) {
        fprintf(stderr, "Cannot open feature manifest: %s\n", manifest_path);
        return 1;
    }

    char model[128] = "unknown";
    int layer = 0;
    (void)json_extract_string_key(json, "model_family", model, sizeof(model));
    (void)json_extract_int_key(json, "layer", &layer);

    int ids[4096];
    int n = extract_feature_ids(json, ids, 4096);
    if (n <= 0) {
        fprintf(stderr, "No feature_id entries found in: %s\n", manifest_path);
        free(json);
        return 1;
    }

    qsort(ids, (size_t)n, sizeof(int), cmp_int_asc);
    int uniq = 0;
    for (int i = 0; i < n; i++) {
        if (i == 0 || ids[i] != ids[i - 1]) ids[uniq++] = ids[i];
    }

    SHA256_CTX ctx;
    sha256_init(&ctx);
    sha256_update(&ctx, (const unsigned char *)model, strlen(model));
    sha256_update(&ctx, (const unsigned char *)"|", 1);
    char lbuf[32];
    snprintf(lbuf, sizeof(lbuf), "%d", layer);
    sha256_update(&ctx, (const unsigned char *)lbuf, strlen(lbuf));
    sha256_update(&ctx, (const unsigned char *)"|", 1);
    for (int i = 0; i < uniq; i++) {
        char ibuf[32];
        snprintf(ibuf, sizeof(ibuf), "%d", ids[i]);
        sha256_update(&ctx, (const unsigned char *)ibuf, strlen(ibuf));
        if (i + 1 < uniq)
            sha256_update(&ctx, (const unsigned char *)",", 1);
    }

    unsigned char h[32];
    char hex[65];
    sha256_final(&ctx, h);
    sha256_hex(h, hex);

    printf("bfh:feature:%s:l%d:%.16s\n", model, layer, hex);
    free(json);
    return 0;
}

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
    if (argc < 2) goto usage;

    if (strcmp(argv[1], "file") == 0 && argc >= 3)
        return cmd_file(argv[2]);

    if (strcmp(argv[1], "node") == 0 && argc >= 6)
        return cmd_node(argv[2], argv[3], argv[4], argv[5]);

    if (strcmp(argv[1], "merkle") == 0 && argc >= 3)
        return cmd_merkle(argv[2], 0);

    if (strcmp(argv[1], "verify") == 0 && argc >= 3)
        return cmd_merkle(argv[2], 1);

    if (strcmp(argv[1], "dedup") == 0 && argc >= 3)
        return cmd_dedup(argv[2]);

    if (strcmp(argv[1], "feature") == 0 && argc >= 3)
        return cmd_feature(argv[2]);

usage:
    fprintf(stderr,
        "BonfyreHash — content-addressing engine\n\n"
        "Usage:\n"
        "  bonfyre-hash file <path>                              SHA-256 of file\n"
        "  bonfyre-hash node <op> <ver> <params_json> <h1,h2>    operator node hash\n"
        "  bonfyre-hash merkle <artifact.json>                    compute/update root_hash\n"
        "  bonfyre-hash verify <artifact.json>                    verify all hashes\n"
        "  bonfyre-hash dedup <dir>                               find duplicate files\n"
        "  bonfyre-hash feature <features.json>                   stable SAE feature hash URI\n"
    );
    return 1;
}
