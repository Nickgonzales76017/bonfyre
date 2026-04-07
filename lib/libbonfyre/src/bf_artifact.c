/*
 * bf_artifact.c — canonical artifact contract implementation.
 *
 * Extracts and unifies the ManifestSummary code that was previously
 * embedded only in BonfyrePipeline.
 */
#include "bonfyre.h"
#include <ctype.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* ----------------------------------------------------------------
 * Init / parse / keys
 * ---------------------------------------------------------------- */

void bf_artifact_init(BfArtifact *a) {
    memset(a, 0, sizeof(*a));
}

void bf_normalize_token(char *dst, size_t dst_sz, const char *src) {
    size_t j = 0;
    int last_dash = 0;
    if (dst_sz == 0) return;
    if (!src || !src[0]) { snprintf(dst, dst_sz, "unknown"); return; }
    for (size_t i = 0; src[i] && j + 1 < dst_sz; ++i) {
        unsigned char ch = (unsigned char)src[i];
        if (isalnum(ch)) {
            dst[j++] = (char)tolower(ch);
            last_dash = 0;
        } else if (!last_dash && j > 0) {
            dst[j++] = '-';
            last_dash = 1;
        }
    }
    while (j > 0 && dst[j - 1] == '-') j--;
    if (j == 0) { snprintf(dst, dst_sz, "unknown"); return; }
    dst[j] = '\0';
}

void bf_artifact_compute_keys(BfArtifact *a) {
    char type_norm[128], system_norm[128], counts[96];
    bf_normalize_token(type_norm, sizeof(type_norm), a->artifact_type);
    bf_normalize_token(system_norm, sizeof(system_norm), a->source_system);
    snprintf(counts, sizeof(counts), "%d|%d|%d",
             a->atoms_count, a->operators_count, a->realizations_count);

    size_t type_len = strlen(type_norm);
    size_t sys_len  = strlen(system_norm);
    size_t cnt_len  = strlen(counts);

    /* family_key: type + system (groups structurally equivalent artifacts) */
    uint64_t fh = BF_FNV1A_INIT;
    fh = bf_fnv1a64(fh, type_norm, type_len);
    fh = bf_fnv1a64(fh, "|", 1);
    fh = bf_fnv1a64(fh, system_norm, sys_len);

    /* canonical_key: type + system + cardinalities (distinguishes signatures) */
    uint64_t ch = BF_FNV1A_INIT;
    ch = bf_fnv1a64(ch, type_norm, type_len);
    ch = bf_fnv1a64(ch, "|", 1);
    ch = bf_fnv1a64(ch, system_norm, sys_len);
    ch = bf_fnv1a64(ch, "|", 1);
    ch = bf_fnv1a64(ch, counts, cnt_len);

    a->component_total = a->atoms_count + a->operators_count + a->realizations_count;
    snprintf(a->family_key, sizeof(a->family_key), "%016llx", (unsigned long long)fh);
    snprintf(a->canonical_key, sizeof(a->canonical_key), "%016llx", (unsigned long long)ch);
}

static void copy_token(char *dst, size_t dst_sz, const char *start, size_t len) {
    if (dst_sz == 0) return;
    size_t n = len < dst_sz - 1 ? len : dst_sz - 1;
    memcpy(dst, start, n);
    dst[n] = '\0';
}

void bf_artifact_parse(BfArtifact *a, const char *json) {
    bf_artifact_init(a);
    int object_depth = 0, array_depth = 0, in_string = 0, escape = 0;
    int pending_key = 0;
    const char *str_start = NULL;
    size_t str_len = 0;
    char key[64] = {0};
    enum { A_NONE, A_ATOMS, A_OPS, A_REAL } active = A_NONE;

    for (const char *p = json; *p; ++p) {
        char c = *p;
        if (in_string) {
            if (escape) { escape = 0; continue; }
            if (c == '\\') { escape = 1; continue; }
            if (c == '"') {
                in_string = 0;
                const char *look = p + 1;
                while (*look == ' ' || *look == '\n' || *look == '\r' || *look == '\t') look++;
                if (object_depth == 1 && array_depth == 0 && *look == ':') {
                    copy_token(key, sizeof(key), str_start, str_len);
                    pending_key = 1;
                } else if (pending_key && object_depth == 1 && array_depth == 0) {
                    if      (strcmp(key, "artifact_id") == 0)   copy_token(a->artifact_id, sizeof(a->artifact_id), str_start, str_len);
                    else if (strcmp(key, "artifact_type") == 0)  copy_token(a->artifact_type, sizeof(a->artifact_type), str_start, str_len);
                    else if (strcmp(key, "source_system") == 0)  copy_token(a->source_system, sizeof(a->source_system), str_start, str_len);
                    else if (strcmp(key, "created_at") == 0)     copy_token(a->created_at, sizeof(a->created_at), str_start, str_len);
                    else if (strcmp(key, "root_hash") == 0)      copy_token(a->root_hash, sizeof(a->root_hash), str_start, str_len);
                    pending_key = 0; key[0] = '\0';
                }
                continue;
            }
            str_len++;
            continue;
        }
        if (c == '"') { in_string = 1; str_start = p + 1; str_len = 0; continue; }
        if (pending_key && object_depth == 1 && array_depth == 0 && c == '[') {
            if      (strcmp(key, "atoms") == 0)          active = A_ATOMS;
            else if (strcmp(key, "operators") == 0)       active = A_OPS;
            else if (strcmp(key, "realizations") == 0)    active = A_REAL;
        }
        if (c == '{') {
            if (active != A_NONE && array_depth == 1) {
                if      (active == A_ATOMS) a->atoms_count++;
                else if (active == A_OPS)   a->operators_count++;
                else if (active == A_REAL)   a->realizations_count++;
            }
            object_depth++;
        } else if (c == '}') {
            if (object_depth > 0) object_depth--;
        } else if (c == '[') {
            array_depth++;
        } else if (c == ']') {
            if (array_depth > 0) array_depth--;
            if (array_depth == 0) { active = A_NONE; pending_key = 0; key[0] = '\0'; }
        } else if (pending_key && object_depth == 1 && array_depth == 0 &&
                   c != ' ' && c != '\n' && c != '\r' && c != '\t' && c != ':') {
            pending_key = 0; key[0] = '\0';
        }
    }
    bf_artifact_compute_keys(a);
}

/* ----------------------------------------------------------------
 * Serialization
 * ---------------------------------------------------------------- */

int bf_artifact_to_json(const BfArtifact *a, char *buf, size_t buf_sz) {
    return snprintf(buf, buf_sz,
        "{\n"
        "  \"artifact_id\": \"%s\",\n"
        "  \"artifact_type\": \"%s\",\n"
        "  \"source_system\": \"%s\",\n"
        "  \"created_at\": \"%s\",\n"
        "  \"root_hash\": \"%s\",\n"
        "  \"family_key\": \"%s\",\n"
        "  \"canonical_key\": \"%s\",\n"
        "  \"atoms_count\": %d,\n"
        "  \"operators_count\": %d,\n"
        "  \"realizations_count\": %d,\n"
        "  \"component_total\": %d\n"
        "}",
        a->artifact_id, a->artifact_type, a->source_system,
        a->created_at, a->root_hash, a->family_key, a->canonical_key,
        a->atoms_count, a->operators_count, a->realizations_count,
        a->component_total);
}

int bf_artifact_write_json(const BfArtifact *a, const char *path) {
    char buf[4096];
    int n = bf_artifact_to_json(a, buf, sizeof(buf));
    if (n < 0 || (size_t)n >= sizeof(buf)) return -1;
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fwrite(buf, 1, (size_t)n, f);
    fputc('\n', f);
    fclose(f);
    return 0;
}

/* ----------------------------------------------------------------
 * Cache (binary fast path for manifest loading)
 * ---------------------------------------------------------------- */

int bf_cache_load(const char *json_path, BfArtifact *a) {
    /* Check .bfrec first (binary record with size/mtime validation) */
    char rec_path[PATH_MAX];
    snprintf(rec_path, sizeof(rec_path), "%s.bfrec", json_path);

    struct stat json_st, rec_st;
    if (stat(json_path, &json_st) != 0) return 0;
    if (stat(rec_path, &rec_st) != 0) return 0;
    if (rec_st.st_mtime < json_st.st_mtime) return 0;

    FILE *f = fopen(rec_path, "rb");
    if (!f) return 0;

    BfBinaryRecord rec;
    if (fread(&rec, sizeof(rec), 1, f) != 1) { fclose(f); return 0; }
    fclose(f);

    if (memcmp(rec.magic, BF_BINARY_MAGIC, 6) != 0) return 0;
    if (rec.json_size != (long long)json_st.st_size) return 0;
    if (rec.json_mtime != (long long)json_st.st_mtime) return 0;

    *a = rec.artifact;
    if (a->canonical_key[0] == '\0') bf_artifact_compute_keys(a);
    return 1;
}

void bf_cache_save(const char *json_path, const BfArtifact *a) {
    struct stat json_st;
    if (stat(json_path, &json_st) != 0) return;

    /* Write .bfrec */
    char rec_path[PATH_MAX];
    snprintf(rec_path, sizeof(rec_path), "%s.bfrec", json_path);

    BfBinaryRecord rec;
    memset(&rec, 0, offsetof(BfBinaryRecord, artifact));
    memcpy(rec.magic, BF_BINARY_MAGIC, 6);
    rec.json_size = (long long)json_st.st_size;
    rec.json_mtime = (long long)json_st.st_mtime;
    rec.artifact = *a;

    FILE *f = fopen(rec_path, "wb");
    if (!f) return;
    fwrite(&rec, sizeof(rec), 1, f);
    fclose(f);

    /* Write .bfsum (text cache) */
    char sum_path[PATH_MAX];
    snprintf(sum_path, sizeof(sum_path), "%s.bfsum", json_path);
    f = fopen(sum_path, "wb");
    if (!f) return;
    BfCacheRecord cache;
    memset(&cache, 0, offsetof(BfCacheRecord, artifact));
    memcpy(cache.magic, BF_CACHE_MAGIC, 6);
    cache.artifact = *a;
    fwrite(&cache, sizeof(cache), 1, f);
    fclose(f);
}
