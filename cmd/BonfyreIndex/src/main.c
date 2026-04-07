/*
 * BonfyreIndex — artifact family indexer and search engine.
 *
 * Crawls directories of artifact.json manifests and builds a SQLite index.
 * Query by hash, type, date, tag. Find shared operators across families.
 * Discover reuse opportunities (same atom hash = same content).
 *
 * Usage:
 *   bonfyre-index build <artifacts_dir> [--db index.db]
 *   bonfyre-index search <query> [--db index.db] [--type TYPE] [--limit N]
 *   bonfyre-index reuse [--db index.db]
 *   bonfyre-index stats [--db index.db]
 */
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>

#define MAX_LINE 65536

static void iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL); struct tm t; gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
}

static long long monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static long current_max_rss_kb(void) {
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) != 0) return 0;
#ifdef __APPLE__
    return usage.ru_maxrss / 1024;
#else
    return usage.ru_maxrss;
#endif
}

typedef struct {
    char artifact_id[128];
    char artifact_type[128];
    char source_system[128];
    char created_at[32];
    char root_hash[68];
    char family_key[17];
    char canonical_key[17];
    int atoms_count;
    int operators_count;
    int realizations_count;
    int component_total;
} ManifestSummary;

typedef struct {
    char magic[8];
    ManifestSummary summary;
} ManifestCacheRecord;

typedef struct {
    char magic[8];
    long long json_size;
    long long json_mtime;
    ManifestSummary summary;
} ManifestBinaryRecord;

#define MANIFEST_CACHE_MAGIC "BFSM01"
#define MANIFEST_BINARY_MAGIC "BFAR01"

static unsigned long long fnv1a64_update(unsigned long long h, const void *data, size_t len) {
    const unsigned char *p = (const unsigned char *)data;
    for (size_t i = 0; i < len; ++i) {
        h ^= (unsigned long long)p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static void normalize_equivalence_token(char *dst, size_t dst_sz, const char *src) {
    size_t j = 0;
    int last_dash = 0;
    if (dst_sz == 0) return;
    if (!src || !src[0]) {
        snprintf(dst, dst_sz, "unknown");
        return;
    }
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
    if (j == 0) {
        snprintf(dst, dst_sz, "unknown");
        return;
    }
    dst[j] = '\0';
}

static void compute_canonical_key(ManifestSummary *summary) {
    unsigned long long h = 1469598103934665603ULL;
    unsigned long long family_h = 1469598103934665603ULL;
    char counts[96];
    char type_norm[128];
    char system_norm[128];
    snprintf(
        counts, sizeof(counts), "%d|%d|%d",
        summary->atoms_count,
        summary->operators_count,
        summary->realizations_count
    );
    normalize_equivalence_token(type_norm, sizeof(type_norm), summary->artifact_type);
    normalize_equivalence_token(system_norm, sizeof(system_norm), summary->source_system);
    family_h = fnv1a64_update(family_h, type_norm, strlen(type_norm));
    family_h = fnv1a64_update(family_h, "|", 1);
    family_h = fnv1a64_update(family_h, system_norm, strlen(system_norm));
    h = fnv1a64_update(h, type_norm, strlen(type_norm));
    h = fnv1a64_update(h, "|", 1);
    h = fnv1a64_update(h, system_norm, strlen(system_norm));
    h = fnv1a64_update(h, "|", 1);
    h = fnv1a64_update(h, counts, strlen(counts));
    summary->component_total = summary->atoms_count + summary->operators_count + summary->realizations_count;
    snprintf(summary->family_key, sizeof(summary->family_key), "%016llx", family_h);
    snprintf(summary->canonical_key, sizeof(summary->canonical_key), "%016llx", h);
}

static void copy_json_token(char *dst, size_t dst_sz, const char *start, size_t len) {
    if (dst_sz == 0) return;
    size_t n = len < dst_sz - 1 ? len : dst_sz - 1;
    memcpy(dst, start, n);
    dst[n] = '\0';
}

static void manifest_summary_init(ManifestSummary *summary) {
    memset(summary, 0, sizeof(*summary));
}

static void scan_manifest_summary(const char *json, ManifestSummary *summary) {
    manifest_summary_init(summary);
    int object_depth = 0;
    int array_depth = 0;
    int in_string = 0;
    int escape = 0;
    int pending_top_level_key = 0;
    const char *string_start = NULL;
    size_t string_len = 0;
    char current_key[64] = {0};
    enum { ARRAY_NONE, ARRAY_ATOMS, ARRAY_OPERATORS, ARRAY_REALIZATIONS } active_array = ARRAY_NONE;

    for (const char *p = json; *p; ++p) {
        char ch = *p;
        if (in_string) {
            if (escape) {
                escape = 0;
                continue;
            }
            if (ch == '\\') {
                escape = 1;
                continue;
            }
            if (ch == '"') {
                in_string = 0;
                const char *look = p + 1;
                while (*look == ' ' || *look == '\n' || *look == '\r' || *look == '\t') look++;
                if (object_depth == 1 && array_depth == 0 && *look == ':') {
                    copy_json_token(current_key, sizeof(current_key), string_start, string_len);
                    pending_top_level_key = 1;
                } else if (pending_top_level_key && object_depth == 1 && array_depth == 0) {
                    if (strcmp(current_key, "artifact_id") == 0) {
                        copy_json_token(summary->artifact_id, sizeof(summary->artifact_id), string_start, string_len);
                    } else if (strcmp(current_key, "artifact_type") == 0) {
                        copy_json_token(summary->artifact_type, sizeof(summary->artifact_type), string_start, string_len);
                    } else if (strcmp(current_key, "source_system") == 0) {
                        copy_json_token(summary->source_system, sizeof(summary->source_system), string_start, string_len);
                    } else if (strcmp(current_key, "created_at") == 0) {
                        copy_json_token(summary->created_at, sizeof(summary->created_at), string_start, string_len);
                    } else if (strcmp(current_key, "root_hash") == 0) {
                        copy_json_token(summary->root_hash, sizeof(summary->root_hash), string_start, string_len);
                    }
                    pending_top_level_key = 0;
                    current_key[0] = '\0';
                }
                continue;
            }
            string_len++;
            continue;
        }

        if (ch == '"') {
            in_string = 1;
            string_start = p + 1;
            string_len = 0;
            continue;
        }

        if (pending_top_level_key && object_depth == 1 && array_depth == 0 && ch == '[') {
            if (strcmp(current_key, "atoms") == 0) active_array = ARRAY_ATOMS;
            else if (strcmp(current_key, "operators") == 0) active_array = ARRAY_OPERATORS;
            else if (strcmp(current_key, "realizations") == 0) active_array = ARRAY_REALIZATIONS;
        }

        if (ch == '{') {
            if (active_array != ARRAY_NONE && array_depth == 1) {
                if (active_array == ARRAY_ATOMS) summary->atoms_count++;
                else if (active_array == ARRAY_OPERATORS) summary->operators_count++;
                else if (active_array == ARRAY_REALIZATIONS) summary->realizations_count++;
            }
            object_depth++;
        } else if (ch == '}') {
            if (object_depth > 0) object_depth--;
        } else if (ch == '[') {
            array_depth++;
        } else if (ch == ']') {
            if (array_depth > 0) array_depth--;
            if (array_depth == 0) {
                active_array = ARRAY_NONE;
                pending_top_level_key = 0;
                current_key[0] = '\0';
            }
        } else if (pending_top_level_key && object_depth == 1 && array_depth == 0 && ch != ' ' && ch != '\n' && ch != '\r' && ch != '\t' && ch != ':') {
            pending_top_level_key = 0;
            current_key[0] = '\0';
        }
    }
    compute_canonical_key(summary);
}

typedef struct {
    sqlite3_stmt *stmt;
    const char *indexed_at;
    int count;
    long long bytes_scanned;
    char *scratch;
    size_t scratch_cap;
    int cache_hits;
    int cache_misses;
} BuildCtx;

typedef struct {
    char **items;
    size_t count;
    size_t cap;
    char *arena;
    size_t arena_len;
    size_t arena_cap;
} DirStack;

static void dirstack_free(DirStack *stack) {
    free(stack->items);
    free(stack->arena);
    memset(stack, 0, sizeof(*stack));
}

static char *dirstack_arena_copy(DirStack *stack, const char *src, size_t len) {
    size_t needed = stack->arena_len + len + 1;
    if (needed > stack->arena_cap) {
        size_t next_cap = stack->arena_cap ? stack->arena_cap : 4096;
        while (next_cap < needed) next_cap *= 2;
        char *next = realloc(stack->arena, next_cap);
        if (!next) return NULL;
        stack->arena = next;
        stack->arena_cap = next_cap;
    }
    char *dst = stack->arena + stack->arena_len;
    memcpy(dst, src, len);
    dst[len] = '\0';
    stack->arena_len += len + 1;
    return dst;
}

static int dirstack_push_len(DirStack *stack, const char *path, size_t len) {
    if (stack->count == stack->cap) {
        size_t next_cap = stack->cap ? stack->cap * 2 : 64;
        char **next = realloc(stack->items, next_cap * sizeof(char *));
        if (!next) return 1;
        stack->items = next;
        stack->cap = next_cap;
    }
    char *stored = dirstack_arena_copy(stack, path, len);
    if (!stored) return 1;
    stack->items[stack->count++] = stored;
    return 0;
}

static int dirstack_push(DirStack *stack, const char *path) {
    return dirstack_push_len(stack, path, strlen(path));
}

static char *dirstack_pop(DirStack *stack) {
    if (stack->count == 0) return NULL;
    return stack->items[--stack->count];
}

static char *scratch_reserve(char **scratch, size_t *cap, size_t needed) {
    if (*cap >= needed) return *scratch;
    size_t next_cap = *cap ? *cap : 4096;
    while (next_cap < needed) next_cap *= 2;
    char *next = realloc(*scratch, next_cap);
    if (!next) return NULL;
    *scratch = next;
    *cap = next_cap;
    return next;
}

static void manifest_cache_path(const char *json_path, char *out, size_t out_sz) {
    snprintf(out, out_sz, "%s.bfsum", json_path);
}

static void manifest_binary_path(const char *json_path, char *out, size_t out_sz) {
    const char *slash = strrchr(json_path, '/');
    if (slash && strcmp(slash + 1, "artifact.json") == 0) {
        size_t prefix_len = (size_t)(slash - json_path + 1);
        if (prefix_len + sizeof("artifact.bfrec") <= out_sz) {
            memcpy(out, json_path, prefix_len);
            memcpy(out + prefix_len, "artifact.bfrec", sizeof("artifact.bfrec"));
            return;
        }
    }
    snprintf(out, out_sz, "%s.bfrec", json_path);
}

static int load_manifest_binary_if_fresh(const char *json_path, const struct stat *json_st, ManifestSummary *summary) {
    char record_path[PATH_MAX];
    manifest_binary_path(json_path, record_path, sizeof(record_path));

    FILE *f = fopen(record_path, "rb");
    if (!f) return 0;
    ManifestBinaryRecord record;
    size_t n = fread(&record, 1, sizeof(record), f);
    fclose(f);
    if (n != sizeof(record)) return 0;
    if (memcmp(record.magic, MANIFEST_BINARY_MAGIC, 6) != 0) return 0;
    if (record.json_size != (long long)json_st->st_size) return 0;
    if (record.json_mtime != (long long)json_st->st_mtime) return 0;
    *summary = record.summary;
    if (summary->canonical_key[0] == '\0') compute_canonical_key(summary);
    return 1;
}

static void save_manifest_binary(const char *json_path, const struct stat *json_st, const ManifestSummary *summary) {
    char record_path[PATH_MAX];
    manifest_binary_path(json_path, record_path, sizeof(record_path));
    FILE *rf = fopen(record_path, "wb");
    if (!rf) return;
    ManifestBinaryRecord binary;
    memset(&binary, 0, offsetof(ManifestBinaryRecord, summary));
    memcpy(binary.magic, MANIFEST_BINARY_MAGIC, 6);
    binary.json_size = (long long)json_st->st_size;
    binary.json_mtime = (long long)json_st->st_mtime;
    binary.summary = *summary;
    fwrite(&binary, 1, sizeof(binary), rf);
    fclose(rf);
}

static int load_manifest_cache_if_fresh(const char *json_path, ManifestSummary *summary) {
    struct stat json_st, cache_st;
    char cache_path[PATH_MAX];
    if (stat(json_path, &json_st) != 0) return 0;
    if (load_manifest_binary_if_fresh(json_path, &json_st, summary)) return 1;

    manifest_cache_path(json_path, cache_path, sizeof(cache_path));
    if (stat(cache_path, &cache_st) != 0) return 0;
    if (cache_st.st_mtime < json_st.st_mtime) return 0;

    FILE *f = fopen(cache_path, "rb");
    if (!f) return 0;
    ManifestCacheRecord record;
    size_t n = fread(&record, 1, sizeof(record), f);
    fclose(f);
    if (n != sizeof(record)) return 0;
    if (memcmp(record.magic, MANIFEST_CACHE_MAGIC, 6) != 0) return 0;
    *summary = record.summary;
    if (summary->canonical_key[0] == '\0') compute_canonical_key(summary);
    save_manifest_binary(json_path, &json_st, summary);
    return 1;
}

static void save_manifest_cache(const char *json_path, const ManifestSummary *summary) {
    struct stat json_st;
    char cache_path[PATH_MAX];
    manifest_cache_path(json_path, cache_path, sizeof(cache_path));

    if (stat(json_path, &json_st) == 0) {
        save_manifest_binary(json_path, &json_st, summary);
    }

    FILE *f = fopen(cache_path, "wb");
    if (!f) return;
    ManifestCacheRecord record;
    memset(&record, 0, offsetof(ManifestCacheRecord, summary));
    memcpy(record.magic, MANIFEST_CACHE_MAGIC, 6);
    record.summary = *summary;
    fwrite(&record, 1, sizeof(record), f);
    fclose(f);
}

static void index_artifact_file(BuildCtx *ctx, const char *path) {
    ManifestSummary summary;
    if (load_manifest_cache_if_fresh(path, &summary)) {
        ctx->cache_hits++;
    } else {
        FILE *fp = fopen(path, "rb");
        if (!fp) return;
        fseek(fp, 0, SEEK_END);
        long sz = ftell(fp);
        fseek(fp, 0, SEEK_SET);
        if (sz < 0) { fclose(fp); return; }
        char *json = scratch_reserve(&ctx->scratch, &ctx->scratch_cap, (size_t)sz + 1);
        if (!json) { fclose(fp); return; }
        fread(json, 1, (size_t)sz, fp);
        json[sz] = '\0';
        fclose(fp);
        ctx->bytes_scanned += sz;
        scan_manifest_summary(json, &summary);
        save_manifest_cache(path, &summary);
        ctx->cache_misses++;
    }

    /* #5: SQLITE_STATIC — strings live in scratch buffer through sqlite3_step */
    sqlite3_bind_text(ctx->stmt, 1, summary.artifact_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(ctx->stmt, 2, summary.artifact_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(ctx->stmt, 3, summary.source_system, -1, SQLITE_STATIC);
    sqlite3_bind_text(ctx->stmt, 4, summary.created_at, -1, SQLITE_STATIC);
    sqlite3_bind_text(ctx->stmt, 5, summary.root_hash, -1, SQLITE_STATIC);
    sqlite3_bind_text(ctx->stmt, 6, summary.family_key, -1, SQLITE_STATIC);
    sqlite3_bind_text(ctx->stmt, 7, summary.canonical_key, -1, SQLITE_STATIC);
    sqlite3_bind_text(ctx->stmt, 8, path, -1, SQLITE_STATIC);
    sqlite3_bind_int(ctx->stmt, 9, summary.atoms_count);
    sqlite3_bind_int(ctx->stmt, 10, summary.operators_count);
    sqlite3_bind_int(ctx->stmt, 11, summary.realizations_count);
    sqlite3_bind_int(ctx->stmt, 12, summary.component_total);
    sqlite3_bind_text(ctx->stmt, 13, ctx->indexed_at, -1, SQLITE_STATIC);
    sqlite3_step(ctx->stmt);
    sqlite3_reset(ctx->stmt);
    sqlite3_clear_bindings(ctx->stmt);

    fprintf(stderr, "  [index] %s (%s) a=%d o=%d r=%d\n",
        summary.artifact_id,
        summary.artifact_type,
        summary.atoms_count,
        summary.operators_count,
        summary.realizations_count);
    ctx->count++;
}

static int entry_is_dir(const char *path, const struct dirent *ent) {
#ifdef DT_DIR
    if (ent->d_type == DT_DIR) return 1;
#endif
#ifdef DT_REG
    if (ent->d_type == DT_REG) return 0;
#endif
#ifdef DT_UNKNOWN
    if (ent->d_type != DT_UNKNOWN) return 0;
#endif
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return S_ISDIR(st.st_mode);
}

static void walk_artifacts_build(const char *root, BuildCtx *ctx) {
    DirStack stack = {0};
    if (dirstack_push(&stack, root) != 0) return;

    for (;;) {
        char *dir_path = dirstack_pop(&stack);
        if (!dir_path) break;

        DIR *d = opendir(dir_path);
        if (!d) continue;
        struct dirent *ent;
        while ((ent = readdir(d))) {
            if (ent->d_name[0] == '.') continue;

            size_t dir_len = strlen(dir_path);
            size_t name_len = strlen(ent->d_name);
            size_t full_len = dir_len + 1 + name_len;
            if (full_len >= PATH_MAX) continue;

            char fp[PATH_MAX];
            memcpy(fp, dir_path, dir_len);
            fp[dir_len] = '/';
            memcpy(fp + dir_len + 1, ent->d_name, name_len + 1);

            if (entry_is_dir(fp, ent)) {
                dirstack_push_len(&stack, fp, full_len);
            } else if (strcmp(ent->d_name, "artifact.json") == 0) {
                index_artifact_file(ctx, fp);
            }
        }
        closedir(d);
    }

    dirstack_free(&stack);
}

/* ---------- SQLite print callback ---------- */

static int print_row_cb(void *data, int ncols, char **vals, char **names) {
    int *first = (int *)data;
    if (*first) {
        for (int i = 0; i < ncols; i++) printf("%-15s", names[i]);
        printf("\n");
        for (int i = 0; i < ncols; i++) printf("%-15s", "-------------");
        printf("\n");
        *first = 0;
    }
    for (int i = 0; i < ncols; i++) printf("%-15s", vals[i] ? vals[i] : "");
    printf("\n");
    return 0;
}

static void fprint_json_escaped(FILE *out, const unsigned char *text) {
    fputc('"', out);
    if (text) {
        for (const unsigned char *p = text; *p; ++p) {
            switch (*p) {
                case '\\': fputs("\\\\", out); break;
                case '"': fputs("\\\"", out); break;
                case '\n': fputs("\\n", out); break;
                case '\r': fputs("\\r", out); break;
                case '\t': fputs("\\t", out); break;
                default:
                    if (*p < 0x20) fprintf(out, "\\u%04x", *p);
                    else fputc(*p, out);
            }
        }
    }
    fputc('"', out);
}

static void print_json_escaped(const unsigned char *text) {
    fprint_json_escaped(stdout, text);
}

static void fprint_json_array_from_csv(FILE *out, const unsigned char *text) {
    fprintf(out, "[");
    if (text && *text) {
        const unsigned char *start = text;
        int first = 1;
        for (const unsigned char *p = text;; ++p) {
            if (*p == ',' || *p == '\0') {
                if (!first) fprintf(out, ",");
                fputc('"', out);
                for (const unsigned char *q = start; q < p; ++q) {
                    switch (*q) {
                        case '\\': fputs("\\\\", out); break;
                        case '"': fputs("\\\"", out); break;
                        case '\n': fputs("\\n", out); break;
                        case '\r': fputs("\\r", out); break;
                        case '\t': fputs("\\t", out); break;
                        default:
                            if (*q < 0x20) fprintf(out, "\\u%04x", *q);
                            else fputc(*q, out);
                    }
                }
                fputc('"', out);
                first = 0;
                if (*p == '\0') break;
                start = p + 1;
            }
        }
    }
    fprintf(out, "]");
}

static void print_json_array_from_csv(const unsigned char *text) {
    fprint_json_array_from_csv(stdout, text);
}

static char *read_text_file(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long sz = ftell(f);
    if (sz < 0) {
        fclose(f);
        return NULL;
    }
    if (fseek(f, 0, SEEK_SET) != 0) {
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
    buf[n] = '\0';
    if (out_len) *out_len = n;
    return buf;
}

static const char *skip_ws(const char *p) {
    while (p && *p && (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t')) p++;
    return p;
}

static int json_extract_string_field(const char *obj, const char *key, char *out, size_t out_sz) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(obj, needle);
    if (!p) return 0;
    p += strlen(needle);
    p = strchr(p, ':');
    if (!p) return 0;
    p = skip_ws(p + 1);
    if (!p || *p != '"') return 0;
    p++;
    size_t n = 0;
    while (*p && *p != '"' && n + 1 < out_sz) {
        if (*p == '\\' && p[1]) p++;
        out[n++] = *p++;
    }
    out[n] = '\0';
    return *p == '"';
}

static int json_extract_int_field(const char *obj, const char *key, long long *value) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(obj, needle);
    if (!p) return 0;
    p += strlen(needle);
    p = strchr(p, ':');
    if (!p) return 0;
    p = skip_ws(p + 1);
    if (!p) return 0;
    char *end = NULL;
    long long v = strtoll(p, &end, 10);
    if (end == p) return 0;
    *value = v;
    return 1;
}

static int json_extract_array_raw(const char *obj, const char *key, const char **start_out, const char **end_out) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(obj, needle);
    if (!p) return 0;
    p += strlen(needle);
    p = strchr(p, ':');
    if (!p) return 0;
    p = skip_ws(p + 1);
    if (!p || *p != '[') return 0;
    const char *start = p;
    int depth = 0;
    int in_string = 0;
    int escape = 0;
    for (; *p; ++p) {
        if (in_string) {
            if (escape) escape = 0;
            else if (*p == '\\') escape = 1;
            else if (*p == '"') in_string = 0;
            continue;
        }
        if (*p == '"') in_string = 1;
        else if (*p == '[') depth++;
        else if (*p == ']') {
            depth--;
            if (depth == 0) {
                *start_out = start;
                *end_out = p;
                return 1;
            }
        }
    }
    return 0;
}

static int json_array_count_strings(const char *start, const char *end) {
    int count = 0;
    int in_string = 0;
    int escape = 0;
    for (const char *p = start; p && p <= end; ++p) {
        if (in_string) {
            if (escape) escape = 0;
            else if (*p == '\\') escape = 1;
            else if (*p == '"') in_string = 0;
            continue;
        }
        if (*p == '"') {
            in_string = 1;
            count++;
        }
    }
    return count;
}

static void slugify_token(char *dst, size_t dst_sz, const char *src) {
    size_t j = 0;
    int last_dash = 0;
    if (dst_sz == 0) return;
    for (size_t i = 0; src && src[i] && j + 1 < dst_sz; ++i) {
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
    if (j == 0) snprintf(dst, dst_sz, "bundle");
    else dst[j] = '\0';
}

static int json_has_string_field(const char *obj, const char *key) {
    char buf[PATH_MAX];
    return json_extract_string_field(obj, key, buf, sizeof(buf));
}

static int json_has_array_field(const char *obj, const char *key) {
    const char *start = NULL, *end = NULL;
    return json_extract_array_raw(obj, key, &start, &end);
}

static int path_exists(const char *path) {
    struct stat st;
    return path && path[0] && stat(path, &st) == 0;
}

static long long query_single_int64(sqlite3 *db, const char *sql) {
    sqlite3_stmt *stmt = NULL;
    long long value = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) value = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return value;
}

/* ---------- build index ---------- */

static int cmd_build(const char *dir, const char *db_path) {
    long long started = monotonic_ns();
    sqlite3 *db;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        fprintf(stderr, "Cannot open db: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    sqlite3_exec(db,
        "PRAGMA temp_store=FILE;"
        "PRAGMA cache_size=-256;"
        "PRAGMA mmap_size=0;",
        NULL, NULL, NULL);

    char *err = NULL;
    sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS families ("
        "  id INTEGER PRIMARY KEY,"
        "  artifact_id TEXT UNIQUE,"
        "  artifact_type TEXT,"
        "  source_system TEXT,"
        "  created_at TEXT,"
        "  root_hash TEXT,"
        "  family_key TEXT,"
        "  canonical_key TEXT,"
        "  path TEXT,"
        "  n_atoms INTEGER,"
        "  n_operators INTEGER,"
        "  n_realizations INTEGER,"
        "  component_total INTEGER,"
        "  indexed_at TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS atoms ("
        "  id INTEGER PRIMARY KEY,"
        "  family_id INTEGER,"
        "  atom_id TEXT,"
        "  content_hash TEXT,"
        "  media_type TEXT,"
        "  byte_size INTEGER,"
        "  FOREIGN KEY(family_id) REFERENCES families(id)"
        ");"
        "CREATE TABLE IF NOT EXISTS operators ("
        "  id INTEGER PRIMARY KEY,"
        "  family_id INTEGER,"
        "  operator_id TEXT,"
        "  op TEXT,"
        "  node_hash TEXT,"
        "  version TEXT,"
        "  FOREIGN KEY(family_id) REFERENCES families(id)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_atoms_hash ON atoms(content_hash);"
        "CREATE INDEX IF NOT EXISTS idx_ops_hash ON operators(node_hash);"
        "CREATE INDEX IF NOT EXISTS idx_ops_type ON operators(op);"
        "CREATE INDEX IF NOT EXISTS idx_families_type ON families(artifact_type);"
        "CREATE INDEX IF NOT EXISTS idx_families_family_key ON families(family_key);"
        "CREATE INDEX IF NOT EXISTS idx_families_canonical ON families(canonical_key);",
        NULL, NULL, &err);
    if (err) { fprintf(stderr, "Schema error: %s\n", err); sqlite3_free(err); }
    sqlite3_exec(db, "ALTER TABLE families ADD COLUMN family_key TEXT;", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE families ADD COLUMN canonical_key TEXT;", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE families ADD COLUMN component_total INTEGER;", NULL, NULL, NULL);
    sqlite3_exec(db, "CREATE INDEX IF NOT EXISTS idx_families_family_key ON families(family_key);", NULL, NULL, NULL);
    sqlite3_exec(db, "CREATE INDEX IF NOT EXISTS idx_families_canonical ON families(canonical_key);", NULL, NULL, NULL);

    char ts[64]; iso_timestamp(ts, sizeof(ts));

    /* Batch all inserts in a single transaction */
    sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, NULL);

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO families (artifact_id, artifact_type, source_system, "
        "created_at, root_hash, family_key, canonical_key, path, n_atoms, n_operators, n_realizations, component_total, indexed_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);", -1, &stmt, NULL);

    BuildCtx ctx = {
        .stmt = stmt,
        .indexed_at = ts,
        .count = 0,
        .bytes_scanned = 0,
        .scratch = NULL,
        .scratch_cap = 0,
        .cache_hits = 0,
        .cache_misses = 0,
    };
    walk_artifacts_build(dir, &ctx);

    sqlite3_finalize(stmt);
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);

    if (ctx.count == 0) {
        fprintf(stderr, "[index] No artifact.json files found in %s\n", dir);
        sqlite3_close(db);
        return 1;
    }

    fprintf(stderr, "[index] Indexed %d artifact families into %s\n", ctx.count, db_path);

    long long canonical_groups = query_single_int64(db, "SELECT COUNT(DISTINCT canonical_key) FROM families;");
    long long equivalent_families = query_single_int64(db, "SELECT COALESCE(SUM(c),0) FROM (SELECT COUNT(*) AS c FROM families GROUP BY canonical_key HAVING c > 1);");
    long long max_equivalence_group = query_single_int64(db, "SELECT COALESCE(MAX(c),0) FROM (SELECT COUNT(*) AS c FROM families GROUP BY canonical_key);");

    char telemetry_path[PATH_MAX];
    snprintf(telemetry_path, sizeof(telemetry_path), "%s.telemetry.json", db_path);
    FILE *tf = fopen(telemetry_path, "w");
    if (tf) {
        char ts2[64];
        iso_timestamp(ts2, sizeof(ts2));
        fprintf(
            tf,
            "{\n"
            "  \"recorded_at\": \"%s\",\n"
            "  \"source_system\": \"BonfyreIndex\",\n"
            "  \"artifact_count\": %d,\n"
            "  \"canonical_groups\": %lld,\n"
            "  \"equivalent_families\": %lld,\n"
            "  \"max_equivalence_group\": %lld,\n"
            "  \"bytes_scanned\": %lld,\n"
            "  \"cache_hits\": %d,\n"
            "  \"cache_misses\": %d,\n"
            "  \"max_rss_kb\": %ld,\n"
            "  \"build_ms\": %.3f\n"
            "}\n",
            ts2,
            ctx.count,
            canonical_groups,
            equivalent_families,
            max_equivalence_group,
            ctx.bytes_scanned,
            ctx.cache_hits,
            ctx.cache_misses,
            current_max_rss_kb(),
            (monotonic_ns() - started) / 1000000.0
        );
        fclose(tf);
    }

    free(ctx.scratch);
    sqlite3_close(db);
    return 0;
}

static int cmd_search(const char *query, const char *db_path, const char *type, int limit) {
    sqlite3 *db;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fprintf(stderr, "Cannot open db: %s\n", db_path);
        return 1;
    }

    const char *sql;
    if (type && type[0]) {
        sql = "SELECT artifact_id, artifact_type, source_system, n_atoms, n_operators, n_realizations, path "
              "FROM families WHERE artifact_type=?1 AND (artifact_id LIKE ?2 OR root_hash LIKE ?2) "
              "ORDER BY created_at DESC LIMIT ?3;";
    } else {
        sql = "SELECT artifact_id, artifact_type, source_system, n_atoms, n_operators, n_realizations, path "
              "FROM families WHERE artifact_id LIKE ?1 OR root_hash LIKE ?1 OR artifact_type LIKE ?1 "
              "ORDER BY created_at DESC LIMIT ?2;";
    }

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);

    char like[512];
    snprintf(like, sizeof(like), "%%%s%%", query);
    if (type && type[0]) {
        sqlite3_bind_text(stmt, 1, type, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, like, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 3, limit);
    } else {
        sqlite3_bind_text(stmt, 1, like, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 2, limit);
    }

    int ncols = sqlite3_column_count(stmt);
    int first = 1;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (first) {
            for (int i = 0; i < ncols; i++) printf("%-15s", sqlite3_column_name(stmt, i));
            printf("\n");
            for (int i = 0; i < ncols; i++) printf("%-15s", "-------------");
            printf("\n");
            first = 0;
        }
        for (int i = 0; i < ncols; i++) {
            const char *v = (const char *)sqlite3_column_text(stmt, i);
            printf("%-15s", v ? v : "");
        }
        printf("\n");
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}

static int cmd_reuse(const char *db_path) {
    sqlite3 *db;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return 1;

    int first;
    printf("=== Shared Atoms (same content, multiple families) ===\n");
    first = 1;
    sqlite3_exec(db,
        "SELECT content_hash, COUNT(*) as n, GROUP_CONCAT(family_id) "
        "FROM atoms GROUP BY content_hash HAVING n > 1 ORDER BY n DESC LIMIT 20;",
        print_row_cb, &first, NULL);
    printf("\n=== Shared Operators (same node_hash, multiple families) ===\n");
    first = 1;
    sqlite3_exec(db,
        "SELECT op, node_hash, COUNT(*) as n "
        "FROM operators GROUP BY node_hash HAVING n > 1 ORDER BY n DESC LIMIT 20;",
        print_row_cb, &first, NULL);

    sqlite3_close(db);
    return 0;
}

static int cmd_equivalence(const char *db_path, int limit, int json_mode) {
    sqlite3 *db;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return 1;

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "SELECT canonical_key, COUNT(*) AS families, GROUP_CONCAT(artifact_id) "
        "FROM families "
        "GROUP BY canonical_key "
        "HAVING COUNT(*) > 1 "
        "ORDER BY families DESC, canonical_key "
        "LIMIT ?1;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_int(stmt, 1, limit);

    int first = 1;
    int first_json = 1;
    if (json_mode) printf("[\n");
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (json_mode) {
            if (!first_json) printf(",\n");
            printf("  {\"canonical_key\":");
            print_json_escaped(sqlite3_column_text(stmt, 0));
            printf(",\"families\":%d,\"artifact_ids\":", sqlite3_column_int(stmt, 1));
            print_json_array_from_csv(sqlite3_column_text(stmt, 2));
            printf("}");
            first_json = 0;
            continue;
        }
        if (first) {
            printf("%-18s%-12s%s\n", "canonical_key", "families", "artifact_ids");
            printf("%-18s%-12s%s\n", "----------------", "----------", "------------");
            first = 0;
        }
        printf(
            "%-18s%-12d%s\n",
            sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0) : "",
            sqlite3_column_int(stmt, 1),
            sqlite3_column_text(stmt, 2) ? (const char *)sqlite3_column_text(stmt, 2) : ""
        );
    }
    sqlite3_finalize(stmt);
    if (json_mode) printf("\n]\n");
    sqlite3_close(db);
    return 0;
}

static int cmd_collapse_preview(const char *db_path, int limit, int json_mode) {
    sqlite3 *db;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return 1;

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "SELECT canonical_key, "
        "       MIN(artifact_id) AS representative_id, "
        "       COUNT(*) AS families, "
        "       COUNT(*) - 1 AS collapsible, "
        "       GROUP_CONCAT(artifact_id) AS artifact_ids "
        "FROM families "
        "GROUP BY canonical_key "
        "HAVING COUNT(*) > 1 "
        "ORDER BY collapsible DESC, representative_id "
        "LIMIT ?1;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_int(stmt, 1, limit);

    int first = 1;
    int first_json = 1;
    long long groups = 0;
    long long collapsible_total = 0;
    if (json_mode) printf("{\"groups\":[\n");
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (json_mode) {
            if (!first_json) printf(",\n");
            printf("  {\"canonical_key\":");
            print_json_escaped(sqlite3_column_text(stmt, 0));
            printf(",\"representative_id\":");
            print_json_escaped(sqlite3_column_text(stmt, 1));
            printf(",\"families\":%d,\"collapsible\":%d,\"artifact_ids\":", sqlite3_column_int(stmt, 2), sqlite3_column_int(stmt, 3));
            print_json_array_from_csv(sqlite3_column_text(stmt, 4));
            printf("}");
            first_json = 0;
            groups++;
            collapsible_total += sqlite3_column_int(stmt, 3);
            continue;
        }
        if (first) {
            printf("%-18s%-22s%-10s%-12s%s\n", "canonical_key", "representative", "families", "collapsible", "artifact_ids");
            printf("%-18s%-22s%-10s%-12s%s\n", "----------------", "----------------------", "--------", "----------", "------------");
            first = 0;
        }
        int families = sqlite3_column_int(stmt, 2);
        int collapsible = sqlite3_column_int(stmt, 3);
        printf(
            "%-18s%-22s%-10d%-12d%s\n",
            sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0) : "",
            sqlite3_column_text(stmt, 1) ? (const char *)sqlite3_column_text(stmt, 1) : "",
            families,
            collapsible,
            sqlite3_column_text(stmt, 4) ? (const char *)sqlite3_column_text(stmt, 4) : ""
        );
        groups++;
        collapsible_total += collapsible;
    }
    sqlite3_finalize(stmt);
    if (json_mode) {
        printf("\n],\"preview_groups\":%lld,\"collapsible_families\":%lld}\n", groups, collapsible_total);
    } else {
        printf("\npreview_groups=%lld collapsible_families=%lld\n", groups, collapsible_total);
    }
    sqlite3_close(db);
    return 0;
}

static int cmd_confidence_preview(const char *db_path, int limit, int json_mode) {
    sqlite3 *db;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return 1;

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "WITH family_groups AS ("
        "  SELECT family_key, "
        "         COUNT(*) AS families, "
        "         COUNT(DISTINCT canonical_key) AS canonical_variants, "
        "         MIN(component_total) AS min_components, "
        "         MAX(component_total) AS max_components, "
        "         MIN(artifact_id) AS representative_id, "
        "         GROUP_CONCAT(artifact_id) AS artifact_ids "
        "  FROM families "
        "  GROUP BY family_key "
        "  HAVING COUNT(*) > 1"
        ") "
        "SELECT family_key, representative_id, families, "
        "       CASE "
        "         WHEN canonical_variants = 1 THEN 'exact-shape' "
        "         WHEN (max_components - min_components) <= 2 THEN 'probable' "
        "         ELSE 'related' "
        "       END AS confidence, "
        "       artifact_ids "
        "FROM family_groups "
        "ORDER BY families DESC, representative_id "
        "LIMIT ?1;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_int(stmt, 1, limit);

    int first = 1;
    int first_json = 1;
    if (json_mode) printf("[\n");
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (json_mode) {
            if (!first_json) printf(",\n");
            printf("  {\"family_key\":");
            print_json_escaped(sqlite3_column_text(stmt, 0));
            printf(",\"representative_id\":");
            print_json_escaped(sqlite3_column_text(stmt, 1));
            printf(",\"families\":%d,\"confidence\":", sqlite3_column_int(stmt, 2));
            print_json_escaped(sqlite3_column_text(stmt, 3));
            printf(",\"artifact_ids\":");
            print_json_array_from_csv(sqlite3_column_text(stmt, 4));
            printf("}");
            first_json = 0;
            continue;
        }
        if (first) {
            printf("%-18s%-22s%-10s%-14s%s\n", "family_key", "representative", "families", "confidence", "artifact_ids");
            printf("%-18s%-22s%-10s%-14s%s\n", "----------------", "----------------------", "--------", "------------", "------------");
            first = 0;
        }
        printf(
            "%-18s%-22s%-10d%-14s%s\n",
            sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0) : "",
            sqlite3_column_text(stmt, 1) ? (const char *)sqlite3_column_text(stmt, 1) : "",
            sqlite3_column_int(stmt, 2),
            sqlite3_column_text(stmt, 3) ? (const char *)sqlite3_column_text(stmt, 3) : "",
            sqlite3_column_text(stmt, 4) ? (const char *)sqlite3_column_text(stmt, 4) : ""
        );
    }
    sqlite3_finalize(stmt);
    if (json_mode) printf("\n]\n");
    sqlite3_close(db);
    return 0;
}

static int cmd_collapse_plan(const char *db_path, int limit, int json_mode) {
    sqlite3 *db;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return 1;

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "WITH family_groups AS ("
        "  SELECT family_key, "
        "         COUNT(*) AS families, "
        "         COUNT(DISTINCT canonical_key) AS canonical_variants, "
        "         MIN(component_total) AS min_components, "
        "         MAX(component_total) AS max_components "
        "  FROM families "
        "  GROUP BY family_key"
        "), ranked AS ("
        "  SELECT canonical_key, family_key, artifact_id, path, component_total, created_at, "
        "         ROW_NUMBER() OVER ("
        "           PARTITION BY canonical_key "
        "           ORDER BY component_total DESC, created_at ASC, artifact_id ASC"
        "         ) AS rn, "
        "         COUNT(*) OVER (PARTITION BY canonical_key) AS group_size "
        "  FROM families"
        "), grouped AS ("
        "  SELECT r.canonical_key, r.family_key, "
        "         MAX(CASE WHEN rn = 1 THEN artifact_id END) AS keep_artifact_id, "
        "         MAX(CASE WHEN rn = 1 THEN path END) AS keep_path, "
        "         MAX(group_size) AS families, "
        "         MAX(group_size) - 1 AS collapsible, "
        "         GROUP_CONCAT(CASE WHEN rn > 1 THEN artifact_id END) AS fold_artifact_ids, "
        "         GROUP_CONCAT(CASE WHEN rn > 1 THEN path END) AS fold_paths "
        "  FROM ranked r "
        "  GROUP BY r.canonical_key, r.family_key "
        "  HAVING MAX(group_size) > 1"
        ") "
        "SELECT g.canonical_key, g.family_key, g.keep_artifact_id, g.families, g.collapsible, g.keep_path, g.fold_artifact_ids, g.fold_paths, "
        "       CASE "
        "         WHEN fg.canonical_variants = 1 THEN 'exact-shape' "
        "         WHEN (fg.max_components - fg.min_components) <= 2 THEN 'probable' "
        "         ELSE 'related' "
        "       END AS confidence "
        "FROM grouped g "
        "JOIN family_groups fg ON fg.family_key = g.family_key "
        "ORDER BY collapsible DESC, keep_artifact_id "
        "LIMIT ?1;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_int(stmt, 1, limit);

    int first = 1;
    int first_json = 1;
    long long groups = 0;
    long long collapsible_total = 0;
    if (json_mode) printf("{\"groups\":[\n");
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (json_mode) {
            if (!first_json) printf(",\n");
            printf("  {\"canonical_key\":");
            print_json_escaped(sqlite3_column_text(stmt, 0));
            printf(",\"family_key\":");
            print_json_escaped(sqlite3_column_text(stmt, 1));
            printf(",\"confidence\":");
            print_json_escaped(sqlite3_column_text(stmt, 8));
            printf(",\"keep_artifact_id\":");
            print_json_escaped(sqlite3_column_text(stmt, 2));
            printf(",\"families\":%d,\"planned_folds\":%d,\"keep_path\":", sqlite3_column_int(stmt, 3), sqlite3_column_int(stmt, 4));
            print_json_escaped(sqlite3_column_text(stmt, 5));
            printf(",\"fold_artifact_ids\":");
            print_json_array_from_csv(sqlite3_column_text(stmt, 6));
            printf(",\"fold_paths\":");
            print_json_array_from_csv(sqlite3_column_text(stmt, 7));
            printf("}");
            first_json = 0;
            groups++;
            collapsible_total += sqlite3_column_int(stmt, 4);
            continue;
        }
        if (first) {
            printf("%-18s %-18s %-12s %-22s %-10s %-12s %-40s %s\n", "canonical_key", "family_key", "confidence", "keep", "families", "fold", "keep_path", "fold_artifact_ids");
            printf("%-18s %-18s %-12s %-22s %-10s %-12s %-40s %s\n", "----------------", "----------------", "------------", "----------------------", "--------", "----------", "----------------------------------------", "----------------");
            first = 0;
        }
        int families = sqlite3_column_int(stmt, 3);
        int collapsible = sqlite3_column_int(stmt, 4);
        printf(
            "%-18s %-18s %-12s %-22s %-10d %-12d %-40s %s\n",
            sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0) : "",
            sqlite3_column_text(stmt, 1) ? (const char *)sqlite3_column_text(stmt, 1) : "",
            sqlite3_column_text(stmt, 8) ? (const char *)sqlite3_column_text(stmt, 8) : "",
            sqlite3_column_text(stmt, 2) ? (const char *)sqlite3_column_text(stmt, 2) : "",
            families,
            collapsible,
            sqlite3_column_text(stmt, 5) ? (const char *)sqlite3_column_text(stmt, 5) : "",
            sqlite3_column_text(stmt, 6) ? (const char *)sqlite3_column_text(stmt, 6) : ""
        );
        groups++;
        collapsible_total += collapsible;
    }
    sqlite3_finalize(stmt);
    if (json_mode) {
        printf("\n],\"plan_groups\":%lld,\"planned_folds\":%lld}\n", groups, collapsible_total);
    } else {
        printf("\nplan_groups=%lld planned_folds=%lld\n", groups, collapsible_total);
    }
    sqlite3_close(db);
    return 0;
}

static int cmd_collapse_ledger(const char *db_path, const char *out_path, int limit) {
    sqlite3 *db;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return 1;

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "WITH family_groups AS ("
        "  SELECT family_key, "
        "         COUNT(*) AS families, "
        "         COUNT(DISTINCT canonical_key) AS canonical_variants, "
        "         MIN(component_total) AS min_components, "
        "         MAX(component_total) AS max_components "
        "  FROM families "
        "  GROUP BY family_key"
        "), ranked AS ("
        "  SELECT canonical_key, family_key, artifact_id, path, component_total, created_at, "
        "         ROW_NUMBER() OVER ("
        "           PARTITION BY canonical_key "
        "           ORDER BY component_total DESC, created_at ASC, artifact_id ASC"
        "         ) AS rn, "
        "         COUNT(*) OVER (PARTITION BY canonical_key) AS group_size "
        "  FROM families"
        "), grouped AS ("
        "  SELECT r.canonical_key, r.family_key, "
        "         MAX(CASE WHEN rn = 1 THEN artifact_id END) AS keep_artifact_id, "
        "         MAX(CASE WHEN rn = 1 THEN path END) AS keep_path, "
        "         MAX(group_size) AS families, "
        "         MAX(group_size) - 1 AS planned_folds, "
        "         GROUP_CONCAT(CASE WHEN rn > 1 THEN artifact_id END) AS fold_artifact_ids, "
        "         GROUP_CONCAT(CASE WHEN rn > 1 THEN path END) AS fold_paths "
        "  FROM ranked r "
        "  GROUP BY r.canonical_key, r.family_key "
        "  HAVING MAX(group_size) > 1"
        ") "
        "SELECT g.canonical_key, g.family_key, "
        "       CASE "
        "         WHEN fg.canonical_variants = 1 THEN 'exact-shape' "
        "         WHEN (fg.max_components - fg.min_components) <= 2 THEN 'probable' "
        "         ELSE 'related' "
        "       END AS confidence, "
        "       g.keep_artifact_id, g.keep_path, g.families, g.planned_folds, g.fold_artifact_ids, g.fold_paths "
        "FROM grouped g "
        "JOIN family_groups fg ON fg.family_key = g.family_key "
        "ORDER BY planned_folds DESC, keep_artifact_id "
        "LIMIT ?1;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_int(stmt, 1, limit);

    FILE *out = fopen(out_path, "w");
    if (!out) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return 1;
    }

    char ts[64];
    iso_timestamp(ts, sizeof(ts));
    long long groups = 0;
    long long planned_folds_total = 0;
    fprintf(out, "{\n  \"recorded_at\": ");
    fprint_json_escaped(out, (const unsigned char *)ts);
    fprintf(out, ",\n  \"source_system\": \"BonfyreIndex\",\n  \"db_path\": ");
    fprint_json_escaped(out, (const unsigned char *)db_path);
    fprintf(out, ",\n  \"actions\": [\n");

    int first = 1;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (!first) fprintf(out, ",\n");
        fprintf(out, "    {\"action\":\"collapse-family\",\"canonical_key\":");
        fprint_json_escaped(out, sqlite3_column_text(stmt, 0));
        fprintf(out, ",\"family_key\":");
        fprint_json_escaped(out, sqlite3_column_text(stmt, 1));
        fprintf(out, ",\"confidence\":");
        fprint_json_escaped(out, sqlite3_column_text(stmt, 2));
        fprintf(out, ",\"keep_artifact_id\":");
        fprint_json_escaped(out, sqlite3_column_text(stmt, 3));
        fprintf(out, ",\"keep_path\":");
        fprint_json_escaped(out, sqlite3_column_text(stmt, 4));
        fprintf(out, ",\"families\":%d,\"planned_folds\":%d,\"fold_artifact_ids\":",
                sqlite3_column_int(stmt, 5), sqlite3_column_int(stmt, 6));
        fprint_json_array_from_csv(out, sqlite3_column_text(stmt, 7));
        fprintf(out, ",\"fold_paths\":");
        fprint_json_array_from_csv(out, sqlite3_column_text(stmt, 8));
        fprintf(out, "}");
        first = 0;
        groups++;
        planned_folds_total += sqlite3_column_int(stmt, 6);
    }

    fprintf(out, "\n  ],\n  \"plan_groups\": %lld,\n  \"planned_folds\": %lld\n}\n",
            groups, planned_folds_total);
    fclose(out);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    printf("%s\n", out_path);
    return 0;
}

static int cmd_merge_manifest(const char *db_path, const char *out_path, int limit) {
    sqlite3 *db;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return 1;

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "WITH family_groups AS ("
        "  SELECT family_key, "
        "         COUNT(*) AS families, "
        "         COUNT(DISTINCT canonical_key) AS canonical_variants, "
        "         MIN(component_total) AS min_components, "
        "         MAX(component_total) AS max_components "
        "  FROM families "
        "  GROUP BY family_key"
        "), ranked AS ("
        "  SELECT canonical_key, family_key, artifact_id, path, component_total, created_at, "
        "         ROW_NUMBER() OVER ("
        "           PARTITION BY canonical_key "
        "           ORDER BY component_total DESC, created_at ASC, artifact_id ASC"
        "         ) AS rn, "
        "         COUNT(*) OVER (PARTITION BY canonical_key) AS group_size "
        "  FROM families"
        "), grouped AS ("
        "  SELECT r.canonical_key, r.family_key, "
        "         MAX(CASE WHEN rn = 1 THEN artifact_id END) AS keep_artifact_id, "
        "         MAX(CASE WHEN rn = 1 THEN path END) AS keep_path, "
        "         MAX(group_size) AS families, "
        "         MAX(group_size) - 1 AS planned_folds, "
        "         GROUP_CONCAT(CASE WHEN rn > 1 THEN artifact_id END) AS fold_artifact_ids, "
        "         GROUP_CONCAT(CASE WHEN rn > 1 THEN path END) AS fold_paths "
        "  FROM ranked r "
        "  GROUP BY r.canonical_key, r.family_key "
        "  HAVING MAX(group_size) > 1"
        ") "
        "SELECT g.canonical_key, g.family_key, "
        "       CASE "
        "         WHEN fg.canonical_variants = 1 THEN 'exact-shape' "
        "         WHEN (fg.max_components - fg.min_components) <= 2 THEN 'probable' "
        "         ELSE 'related' "
        "       END AS confidence, "
        "       g.keep_artifact_id, g.keep_path, g.families, g.planned_folds, g.fold_artifact_ids, g.fold_paths "
        "FROM grouped g "
        "JOIN family_groups fg ON fg.family_key = g.family_key "
        "ORDER BY planned_folds DESC, keep_artifact_id "
        "LIMIT ?1;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_int(stmt, 1, limit);

    FILE *out = fopen(out_path, "w");
    if (!out) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return 1;
    }

    char ts[64];
    iso_timestamp(ts, sizeof(ts));
    long long groups = 0;
    long long planned_folds_total = 0;
    long long families_before = 0;
    long long families_after = 0;
    long long exact_shape_groups = 0;
    long long probable_groups = 0;
    long long related_groups = 0;

    fprintf(out, "{\n");
    fprintf(out, "  \"artifact_type\": \"merge-manifest\",\n");
    fprintf(out, "  \"source_system\": \"BonfyreIndex\",\n");
    fprintf(out, "  \"mode\": \"preview-only\",\n");
    fprintf(out, "  \"generated_at\": ");
    fprint_json_escaped(out, (const unsigned char *)ts);
    fprintf(out, ",\n  \"db_path\": ");
    fprint_json_escaped(out, (const unsigned char *)db_path);
    fprintf(out, ",\n  \"merges\": [\n");

    int first = 1;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char *confidence = sqlite3_column_text(stmt, 2);
        int families = sqlite3_column_int(stmt, 5);
        int planned_folds = sqlite3_column_int(stmt, 6);
        if (!first) fprintf(out, ",\n");
        fprintf(out, "    {\"canonical_key\":");
        fprint_json_escaped(out, sqlite3_column_text(stmt, 0));
        fprintf(out, ",\"family_key\":");
        fprint_json_escaped(out, sqlite3_column_text(stmt, 1));
        fprintf(out, ",\"confidence\":");
        fprint_json_escaped(out, confidence);
        fprintf(out, ",\"keep_artifact_id\":");
        fprint_json_escaped(out, sqlite3_column_text(stmt, 3));
        fprintf(out, ",\"keep_path\":");
        fprint_json_escaped(out, sqlite3_column_text(stmt, 4));
        fprintf(out, ",\"families\":%d,\"planned_folds\":%d,\"fold_artifact_ids\":", families, planned_folds);
        fprint_json_array_from_csv(out, sqlite3_column_text(stmt, 7));
        fprintf(out, ",\"fold_paths\":");
        fprint_json_array_from_csv(out, sqlite3_column_text(stmt, 8));
        fprintf(out, "}");
        first = 0;

        groups++;
        planned_folds_total += planned_folds;
        families_before += families;
        families_after += 1;
        if (confidence && strcmp((const char *)confidence, "exact-shape") == 0) exact_shape_groups++;
        else if (confidence && strcmp((const char *)confidence, "probable") == 0) probable_groups++;
        else related_groups++;
    }

    fprintf(out, "\n  ],\n");
    fprintf(out, "  \"summary\": {\n");
    fprintf(out, "    \"merge_groups\": %lld,\n", groups);
    fprintf(out, "    \"families_before\": %lld,\n", families_before);
    fprintf(out, "    \"families_after\": %lld,\n", families_after);
    fprintf(out, "    \"planned_folds\": %lld,\n", planned_folds_total);
    fprintf(out, "    \"exact_shape_groups\": %lld,\n", exact_shape_groups);
    fprintf(out, "    \"probable_groups\": %lld,\n", probable_groups);
    fprintf(out, "    \"related_groups\": %lld\n", related_groups);
    fprintf(out, "  }\n}\n");

    fclose(out);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    printf("%s\n", out_path);
    return 0;
}

static int cmd_bundle_layout(const char *merge_manifest_path, const char *out_path) {
    size_t len = 0;
    char *json = read_text_file(merge_manifest_path, &len);
    if (!json || len == 0) {
        free(json);
        return 1;
    }

    FILE *out = fopen(out_path, "w");
    if (!out) {
        free(json);
        return 1;
    }

    char generated_at[128] = {0};
    json_extract_string_field(json, "generated_at", generated_at, sizeof(generated_at));

    long long merge_groups = 0;
    long long families_before = 0;
    long long families_after = 0;
    long long planned_folds = 0;
    const char *summary = strstr(json, "\"summary\"");
    if (summary) {
        json_extract_int_field(summary, "merge_groups", &merge_groups);
        json_extract_int_field(summary, "families_before", &families_before);
        json_extract_int_field(summary, "families_after", &families_after);
        json_extract_int_field(summary, "planned_folds", &planned_folds);
    }

    fprintf(out, "{\n");
    fprintf(out, "  \"artifact_type\": \"canonical-bundle-layout\",\n");
    fprintf(out, "  \"source_system\": \"BonfyreIndex\",\n");
    fprintf(out, "  \"mode\": \"preview-only\",\n");
    fprintf(out, "  \"merge_manifest_path\": ");
    fprint_json_escaped(out, (const unsigned char *)merge_manifest_path);
    fprintf(out, ",\n  \"generated_at\": ");
    fprint_json_escaped(out, (const unsigned char *)(generated_at[0] ? generated_at : ""));
    fprintf(out, ",\n  \"bundles\": [\n");

    const char *merges = strstr(json, "\"merges\"");
    int first = 1;
    long long bundle_count = 0;
    if (merges) {
        const char *p = strchr(merges, '[');
        if (p) {
            p++;
            for (;;) {
                p = skip_ws(p);
                if (!p || !*p || *p == ']') break;
                if (*p != '{') { p++; continue; }
                const char *obj_start = p;
                int depth = 0;
                int in_string = 0;
                int escape = 0;
                for (; *p; ++p) {
                    if (in_string) {
                        if (escape) escape = 0;
                        else if (*p == '\\') escape = 1;
                        else if (*p == '"') in_string = 0;
                        continue;
                    }
                    if (*p == '"') in_string = 1;
                    else if (*p == '{') depth++;
                    else if (*p == '}') {
                        depth--;
                        if (depth == 0) {
                            size_t obj_len = (size_t)(p - obj_start + 1);
                            char *obj = malloc(obj_len + 1);
                            if (!obj) break;
                            memcpy(obj, obj_start, obj_len);
                            obj[obj_len] = '\0';

                            char canonical_key[64] = {0};
                            char family_key[64] = {0};
                            char confidence[64] = {0};
                            char keep_artifact_id[128] = {0};
                            char keep_path[PATH_MAX] = {0};
                            long long families = 0;
                            long long item_planned_folds = 0;
                            const char *fold_start = NULL, *fold_end = NULL;

                            json_extract_string_field(obj, "canonical_key", canonical_key, sizeof(canonical_key));
                            json_extract_string_field(obj, "family_key", family_key, sizeof(family_key));
                            json_extract_string_field(obj, "confidence", confidence, sizeof(confidence));
                            json_extract_string_field(obj, "keep_artifact_id", keep_artifact_id, sizeof(keep_artifact_id));
                            json_extract_string_field(obj, "keep_path", keep_path, sizeof(keep_path));
                            json_extract_int_field(obj, "families", &families);
                            json_extract_int_field(obj, "planned_folds", &item_planned_folds);
                            json_extract_array_raw(obj, "fold_paths", &fold_start, &fold_end);

                            char bundle_slug[256];
                            slugify_token(bundle_slug, sizeof(bundle_slug), keep_artifact_id);

                            if (!first) fprintf(out, ",\n");
                            fprintf(out, "    {\"bundle_slug\":");
                            fprint_json_escaped(out, (const unsigned char *)bundle_slug);
                            fprintf(out, ",\"canonical_key\":");
                            fprint_json_escaped(out, (const unsigned char *)canonical_key);
                            fprintf(out, ",\"family_key\":");
                            fprint_json_escaped(out, (const unsigned char *)family_key);
                            fprintf(out, ",\"confidence\":");
                            fprint_json_escaped(out, (const unsigned char *)confidence);
                            fprintf(out, ",\"keep_artifact_id\":");
                            fprint_json_escaped(out, (const unsigned char *)keep_artifact_id);
                            fprintf(out, ",\"keep_path\":");
                            fprint_json_escaped(out, (const unsigned char *)keep_path);
                            fprintf(out, ",\"target_dir\":");
                            fprintf(out, "\"bundles/%s\"", bundle_slug);
                            fprintf(out, ",\"families\":%lld,\"planned_folds\":%lld,\"fold_count\":%d",
                                    families, item_planned_folds,
                                    (fold_start && fold_end) ? json_array_count_strings(fold_start, fold_end) : 0);
                            fprintf(out, ",\"planned_members\":{");
                            fprintf(out, "\"canonical_manifest\":\"bundles/%s/artifact.json\",", bundle_slug);
                            fprintf(out, "\"runtime_record\":\"bundles/%s/artifact.bfrec\",", bundle_slug);
                            fprintf(out, "\"merge_manifest\":\"bundles/%s/merge.manifest.json\"", bundle_slug);
                            fprintf(out, "},\"fold_paths\":");
                            if (fold_start && fold_end) {
                                fwrite(fold_start, 1, (size_t)(fold_end - fold_start + 1), out);
                            } else {
                                fprintf(out, "[]");
                            }
                            fprintf(out, "}");
                            first = 0;
                            bundle_count++;
                            free(obj);
                            p++;
                            break;
                        }
                    }
                }
            }
        }
    }

    fprintf(out, "\n  ],\n");
    fprintf(out, "  \"summary\": {\n");
    fprintf(out, "    \"bundle_count\": %lld,\n", bundle_count);
    fprintf(out, "    \"merge_groups\": %lld,\n", merge_groups);
    fprintf(out, "    \"families_before\": %lld,\n", families_before);
    fprintf(out, "    \"families_after\": %lld,\n", families_after);
    fprintf(out, "    \"planned_folds\": %lld\n", planned_folds);
    fprintf(out, "  }\n}\n");

    fclose(out);
    free(json);
    printf("%s\n", out_path);
    return 0;
}

static int cmd_validate_preview_artifact(const char *path) {
    size_t len = 0;
    char *json = read_text_file(path, &len);
    if (!json || len == 0) {
        free(json);
        fprintf(stderr, "invalid: unreadable file\n");
        return 1;
    }

    char artifact_type[128] = {0};
    if (!json_extract_string_field(json, "artifact_type", artifact_type, sizeof(artifact_type))) {
        fprintf(stderr, "invalid: missing artifact_type\n");
        free(json);
        return 1;
    }

    int ok = 1;
    if (strcmp(artifact_type, "merge-manifest") == 0) {
        const char *merges = strstr(json, "\"merges\"");
        long long merge_groups = 0, families_before = 0, families_after = 0, planned_folds = 0;
        long long actual_groups = 0, actual_families_before = 0, actual_families_after = 0, actual_planned_folds = 0;
        const char *summary = strstr(json, "\"summary\"");
        if (!json_has_string_field(json, "source_system") || !json_has_string_field(json, "mode") ||
            !json_has_string_field(json, "generated_at") || !json_has_string_field(json, "db_path") || !merges || !summary) {
            fprintf(stderr, "invalid: merge-manifest missing required top-level fields\n");
            free(json);
            return 1;
        }
        json_extract_int_field(summary, "merge_groups", &merge_groups);
        json_extract_int_field(summary, "families_before", &families_before);
        json_extract_int_field(summary, "families_after", &families_after);
        json_extract_int_field(summary, "planned_folds", &planned_folds);

        const char *p = strchr(merges, '[');
        if (p) {
            p++;
            for (;;) {
                p = skip_ws(p);
                if (!p || !*p || *p == ']') break;
                if (*p != '{') { p++; continue; }
                const char *obj_start = p;
                int depth = 0, in_string = 0, escape = 0;
                for (; *p; ++p) {
                    if (in_string) {
                        if (escape) escape = 0;
                        else if (*p == '\\') escape = 1;
                        else if (*p == '"') in_string = 0;
                        continue;
                    }
                    if (*p == '"') in_string = 1;
                    else if (*p == '{') depth++;
                    else if (*p == '}') {
                        depth--;
                        if (depth == 0) {
                            size_t obj_len = (size_t)(p - obj_start + 1);
                            char *obj = malloc(obj_len + 1);
                            long long families = 0, item_folds = 0;
                            if (!obj) { ok = 0; break; }
                            memcpy(obj, obj_start, obj_len);
                            obj[obj_len] = '\0';
                            if (!json_has_string_field(obj, "canonical_key") ||
                                !json_has_string_field(obj, "family_key") ||
                                !json_has_string_field(obj, "confidence") ||
                                !json_has_string_field(obj, "keep_artifact_id") ||
                                !json_has_string_field(obj, "keep_path") ||
                                !json_has_array_field(obj, "fold_artifact_ids") ||
                                !json_has_array_field(obj, "fold_paths") ||
                                !json_extract_int_field(obj, "families", &families) ||
                                !json_extract_int_field(obj, "planned_folds", &item_folds)) {
                                ok = 0;
                            } else {
                                actual_groups++;
                                actual_families_before += families;
                                actual_families_after += 1;
                                actual_planned_folds += item_folds;
                            }
                            free(obj);
                            p++;
                            break;
                        }
                    }
                }
                if (!ok) break;
            }
        }
        if (!ok || actual_groups != merge_groups || actual_families_before != families_before ||
            actual_families_after != families_after || actual_planned_folds != planned_folds) {
            fprintf(stderr, "invalid: merge-manifest summary mismatch or missing merge fields\n");
            free(json);
            return 1;
        }
        printf("valid merge-manifest groups=%lld families_before=%lld families_after=%lld planned_folds=%lld\n",
               actual_groups, actual_families_before, actual_families_after, actual_planned_folds);
        free(json);
        return 0;
    }

    if (strcmp(artifact_type, "canonical-bundle-layout") == 0) {
        const char *bundles = strstr(json, "\"bundles\"");
        long long bundle_count = 0, merge_groups = 0, families_before = 0, families_after = 0, planned_folds = 0;
        long long actual_bundle_count = 0, actual_families_before = 0, actual_families_after = 0, actual_planned_folds = 0;
        const char *summary = strstr(json, "\"summary\"");
        if (!json_has_string_field(json, "source_system") || !json_has_string_field(json, "mode") ||
            !json_has_string_field(json, "generated_at") || !json_has_string_field(json, "merge_manifest_path") || !bundles || !summary) {
            fprintf(stderr, "invalid: canonical-bundle-layout missing required top-level fields\n");
            free(json);
            return 1;
        }
        json_extract_int_field(summary, "bundle_count", &bundle_count);
        json_extract_int_field(summary, "merge_groups", &merge_groups);
        json_extract_int_field(summary, "families_before", &families_before);
        json_extract_int_field(summary, "families_after", &families_after);
        json_extract_int_field(summary, "planned_folds", &planned_folds);

        const char *p = strchr(bundles, '[');
        if (p) {
            p++;
            for (;;) {
                p = skip_ws(p);
                if (!p || !*p || *p == ']') break;
                if (*p != '{') { p++; continue; }
                const char *obj_start = p;
                int depth = 0, in_string = 0, escape = 0;
                for (; *p; ++p) {
                    if (in_string) {
                        if (escape) escape = 0;
                        else if (*p == '\\') escape = 1;
                        else if (*p == '"') in_string = 0;
                        continue;
                    }
                    if (*p == '"') in_string = 1;
                    else if (*p == '{') depth++;
                    else if (*p == '}') {
                        depth--;
                        if (depth == 0) {
                            size_t obj_len = (size_t)(p - obj_start + 1);
                            char *obj = malloc(obj_len + 1);
                            long long families = 0, item_folds = 0, fold_count = 0;
                            if (!obj) { ok = 0; break; }
                            memcpy(obj, obj_start, obj_len);
                            obj[obj_len] = '\0';
                            if (!json_has_string_field(obj, "bundle_slug") ||
                                !json_has_string_field(obj, "canonical_key") ||
                                !json_has_string_field(obj, "family_key") ||
                                !json_has_string_field(obj, "confidence") ||
                                !json_has_string_field(obj, "keep_artifact_id") ||
                                !json_has_string_field(obj, "keep_path") ||
                                !json_has_string_field(obj, "target_dir") ||
                                !json_has_array_field(obj, "fold_paths") ||
                                !json_extract_int_field(obj, "families", &families) ||
                                !json_extract_int_field(obj, "planned_folds", &item_folds) ||
                                !json_extract_int_field(obj, "fold_count", &fold_count) ||
                                !strstr(obj, "\"planned_members\"") ) {
                                ok = 0;
                            } else {
                                actual_bundle_count++;
                                actual_families_before += families;
                                actual_families_after += 1;
                                actual_planned_folds += item_folds;
                            }
                            free(obj);
                            p++;
                            break;
                        }
                    }
                }
                if (!ok) break;
            }
        }
        if (!ok || actual_bundle_count != bundle_count || actual_bundle_count != merge_groups ||
            actual_families_before != families_before || actual_families_after != families_after ||
            actual_planned_folds != planned_folds) {
            fprintf(stderr, "invalid: canonical-bundle-layout summary mismatch or missing bundle fields\n");
            free(json);
            return 1;
        }
        printf("valid canonical-bundle-layout bundles=%lld families_before=%lld families_after=%lld planned_folds=%lld\n",
               actual_bundle_count, actual_families_before, actual_families_after, actual_planned_folds);
        free(json);
        return 0;
    }

    fprintf(stderr, "invalid: unsupported artifact_type %s\n", artifact_type);
    free(json);
    return 1;
}

static int cmd_bundle_diff(const char *bundle_layout_path, const char *out_path) {
    size_t len = 0;
    char *json = read_text_file(bundle_layout_path, &len);
    if (!json || len == 0) {
        free(json);
        return 1;
    }

    FILE *out = fopen(out_path, "w");
    if (!out) {
        free(json);
        return 1;
    }

    char generated_at[128] = {0};
    json_extract_string_field(json, "generated_at", generated_at, sizeof(generated_at));

    long long bundle_count = 0;
    long long families_before = 0;
    long long families_after = 0;
    long long planned_folds = 0;
    const char *summary = strstr(json, "\"summary\"");
    if (summary) {
        json_extract_int_field(summary, "bundle_count", &bundle_count);
        json_extract_int_field(summary, "families_before", &families_before);
        json_extract_int_field(summary, "families_after", &families_after);
        json_extract_int_field(summary, "planned_folds", &planned_folds);
    }

    fprintf(out, "{\n");
    fprintf(out, "  \"artifact_type\": \"canonical-bundle-diff\",\n");
    fprintf(out, "  \"source_system\": \"BonfyreIndex\",\n");
    fprintf(out, "  \"mode\": \"preview-only\",\n");
    fprintf(out, "  \"bundle_layout_path\": ");
    fprint_json_escaped(out, (const unsigned char *)bundle_layout_path);
    fprintf(out, ",\n  \"generated_at\": ");
    fprint_json_escaped(out, (const unsigned char *)(generated_at[0] ? generated_at : ""));
    fprintf(out, ",\n  \"bundles\": [\n");

    const char *bundles = strstr(json, "\"bundles\"");
    long long actual_bundle_count = 0;
    long long keep_missing = 0;
    long long source_runtime_missing = 0;
    long long target_paths_existing = 0;
    long long target_paths_missing = 0;
    long long fold_paths_existing = 0;
    long long fold_paths_missing = 0;
    int first = 1;

    if (bundles) {
        const char *p = strchr(bundles, '[');
        if (p) {
            p++;
            for (;;) {
                p = skip_ws(p);
                if (!p || !*p || *p == ']') break;
                if (*p != '{') { p++; continue; }
                const char *obj_start = p;
                int depth = 0, in_string = 0, escape = 0;
                for (; *p; ++p) {
                    if (in_string) {
                        if (escape) escape = 0;
                        else if (*p == '\\') escape = 1;
                        else if (*p == '"') in_string = 0;
                        continue;
                    }
                    if (*p == '"') in_string = 1;
                    else if (*p == '{') depth++;
                    else if (*p == '}') {
                        depth--;
                        if (depth == 0) {
                            size_t obj_len = (size_t)(p - obj_start + 1);
                            char *obj = malloc(obj_len + 1);
                            if (!obj) break;
                            memcpy(obj, obj_start, obj_len);
                            obj[obj_len] = '\0';

                            char bundle_slug[256] = {0};
                            char confidence[64] = {0};
                            char keep_path[PATH_MAX] = {0};
                            char canonical_manifest[PATH_MAX] = {0};
                            char runtime_record[PATH_MAX] = {0};
                            char merge_manifest[PATH_MAX] = {0};
                            const char *fold_start = NULL, *fold_end = NULL;

                            json_extract_string_field(obj, "bundle_slug", bundle_slug, sizeof(bundle_slug));
                            json_extract_string_field(obj, "confidence", confidence, sizeof(confidence));
                            json_extract_string_field(obj, "keep_path", keep_path, sizeof(keep_path));
                            json_extract_string_field(obj, "canonical_manifest", canonical_manifest, sizeof(canonical_manifest));
                            json_extract_string_field(obj, "runtime_record", runtime_record, sizeof(runtime_record));
                            json_extract_string_field(obj, "merge_manifest", merge_manifest, sizeof(merge_manifest));
                            json_extract_array_raw(obj, "fold_paths", &fold_start, &fold_end);

                            char source_runtime[PATH_MAX] = {0};
                            manifest_binary_path(keep_path, source_runtime, sizeof(source_runtime));
                            int keep_exists_now = path_exists(keep_path);
                            int source_runtime_exists_now = path_exists(source_runtime);
                            int target_manifest_exists = path_exists(canonical_manifest);
                            int target_runtime_exists = path_exists(runtime_record);
                            int target_merge_exists = path_exists(merge_manifest);
                            int fold_existing = (fold_start && fold_end) ? json_array_count_strings(fold_start, fold_end) : 0;

                            if (fold_start && fold_end) {
                                int in_str = 0, esc = 0;
                                char item[PATH_MAX];
                                size_t item_len = 0;
                                int count_existing = 0, count_total = 0;
                                for (const char *q = fold_start; q <= fold_end; ++q) {
                                    if (in_str) {
                                        if (esc) {
                                            esc = 0;
                                            if (item_len + 1 < sizeof(item)) item[item_len++] = *q;
                                        } else if (*q == '\\') {
                                            esc = 1;
                                        } else if (*q == '"') {
                                            item[item_len] = '\0';
                                            if (path_exists(item)) count_existing++;
                                            count_total++;
                                            item_len = 0;
                                            in_str = 0;
                                        } else if (item_len + 1 < sizeof(item)) {
                                            item[item_len++] = *q;
                                        }
                                    } else if (*q == '"') {
                                        in_str = 1;
                                        item_len = 0;
                                    }
                                }
                                fold_existing = count_existing;
                                fold_paths_existing += count_existing;
                                fold_paths_missing += (count_total - count_existing);
                            }

                            if (!keep_exists_now) keep_missing++;
                            if (!source_runtime_exists_now) source_runtime_missing++;
                            target_paths_existing += target_manifest_exists + target_runtime_exists + target_merge_exists;
                            target_paths_missing += 3 - (target_manifest_exists + target_runtime_exists + target_merge_exists);

                            if (!first) fprintf(out, ",\n");
                            fprintf(out, "    {\"bundle_slug\":");
                            fprint_json_escaped(out, (const unsigned char *)bundle_slug);
                            fprintf(out, ",\"confidence\":");
                            fprint_json_escaped(out, (const unsigned char *)confidence);
                            fprintf(out, ",\"current\":{");
                            fprintf(out, "\"keep_path\":");
                            fprint_json_escaped(out, (const unsigned char *)keep_path);
                            fprintf(out, ",\"keep_exists\":%s,\"source_runtime_record\":", keep_exists_now ? "true" : "false");
                            fprint_json_escaped(out, (const unsigned char *)source_runtime);
                            fprintf(out, ",\"source_runtime_exists\":%s,\"fold_paths_existing\":%d", source_runtime_exists_now ? "true" : "false", fold_existing);
                            fprintf(out, "},\"projected\":{");
                            fprintf(out, "\"canonical_manifest\":");
                            fprint_json_escaped(out, (const unsigned char *)canonical_manifest);
                            fprintf(out, ",\"canonical_manifest_exists\":%s,\"runtime_record\":", target_manifest_exists ? "true" : "false");
                            fprint_json_escaped(out, (const unsigned char *)runtime_record);
                            fprintf(out, ",\"runtime_record_exists\":%s,\"merge_manifest\":", target_runtime_exists ? "true" : "false");
                            fprint_json_escaped(out, (const unsigned char *)merge_manifest);
                            fprintf(out, ",\"merge_manifest_exists\":%s", target_merge_exists ? "true" : "false");
                            fprintf(out, "},\"actions\":[");
                            fprintf(out, "\"reference-keep-manifest\",\"reference-fold-families\"");
                            if (!source_runtime_exists_now) fprintf(out, ",\"materialize-runtime-record\"");
                            if (!target_manifest_exists) fprintf(out, ",\"write-canonical-manifest\"");
                            if (!target_runtime_exists) fprintf(out, ",\"write-runtime-record\"");
                            if (!target_merge_exists) fprintf(out, ",\"write-merge-manifest\"");
                            fprintf(out, "]}");

                            first = 0;
                            actual_bundle_count++;
                            free(obj);
                            p++;
                            break;
                        }
                    }
                }
            }
        }
    }

    fprintf(out, "\n  ],\n");
    fprintf(out, "  \"summary\": {\n");
    fprintf(out, "    \"bundle_count\": %lld,\n", actual_bundle_count);
    fprintf(out, "    \"families_before\": %lld,\n", families_before);
    fprintf(out, "    \"families_after\": %lld,\n", families_after);
    fprintf(out, "    \"planned_folds\": %lld,\n", planned_folds);
    fprintf(out, "    \"keep_missing\": %lld,\n", keep_missing);
    fprintf(out, "    \"source_runtime_missing\": %lld,\n", source_runtime_missing);
    fprintf(out, "    \"target_paths_existing\": %lld,\n", target_paths_existing);
    fprintf(out, "    \"target_paths_missing\": %lld,\n", target_paths_missing);
    fprintf(out, "    \"fold_paths_existing\": %lld,\n", fold_paths_existing);
    fprintf(out, "    \"fold_paths_missing\": %lld\n", fold_paths_missing);
    fprintf(out, "  }\n}\n");

    fclose(out);
    free(json);
    printf("%s\n", out_path);
    return 0;
}

static int cmd_stats(const char *db_path) {
    sqlite3 *db;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fprintf(stderr, "Cannot open db: %s\n", db_path);
        return 1;
    }

    int first;
    printf("=== BonfyreIndex Stats ===\n");
    first = 1;
    sqlite3_exec(db,
        "SELECT artifact_type, COUNT(*) as families, SUM(n_atoms) as atoms, "
        "SUM(n_operators) as ops, SUM(n_realizations) as realizations "
        "FROM families GROUP BY artifact_type;",
        print_row_cb, &first, NULL);
    printf("\n");
    first = 1;
    sqlite3_exec(db,
        "SELECT COUNT(*) as total_families, SUM(n_atoms) as total_atoms, "
        "SUM(n_operators) as total_ops, SUM(n_realizations) as total_realizations FROM families;",
        print_row_cb, &first, NULL);
    printf("\n=== Canonical Equivalence ===\n");
    first = 1;
    sqlite3_exec(db,
        "SELECT COUNT(DISTINCT canonical_key) as canonical_groups, "
        "COALESCE(SUM(c),0) as equivalent_families, "
        "COALESCE(MAX(c),0) as max_group_size "
        "FROM (SELECT canonical_key, COUNT(*) as c FROM families GROUP BY canonical_key);",
        print_row_cb, &first, NULL);

    sqlite3_close(db);
    return 0;
}

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
    const char *db = "bonfyre-index.db";
    int json_mode = 0;

    /* parse --db */
    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--db") == 0) { db = argv[i+1]; break; }
    }
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--json") == 0) { json_mode = 1; break; }
    }

    if (argc >= 3 && strcmp(argv[1], "build") == 0)
        return cmd_build(argv[2], db);

    if (argc >= 3 && strcmp(argv[1], "search") == 0) {
        const char *type = NULL;
        int limit = 20;
        for (int i = 3; i < argc - 1; i++) {
            if (strcmp(argv[i], "--type") == 0) type = argv[i+1];
            if (strcmp(argv[i], "--limit") == 0) limit = atoi(argv[i+1]);
        }
        return cmd_search(argv[2], db, type, limit);
    }

    if (argc >= 2 && strcmp(argv[1], "reuse") == 0)
        return cmd_reuse(db);

    if (argc >= 2 && strcmp(argv[1], "equivalence") == 0) {
        int limit = 20;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--limit") == 0) limit = atoi(argv[i+1]);
        }
        return cmd_equivalence(db, limit, json_mode);
    }

    if (argc >= 2 && strcmp(argv[1], "collapse-preview") == 0) {
        int limit = 20;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--limit") == 0) limit = atoi(argv[i+1]);
        }
        return cmd_collapse_preview(db, limit, json_mode);
    }

    if (argc >= 2 && strcmp(argv[1], "confidence-preview") == 0) {
        int limit = 20;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--limit") == 0) limit = atoi(argv[i+1]);
        }
        return cmd_confidence_preview(db, limit, json_mode);
    }

    if (argc >= 2 && strcmp(argv[1], "collapse-plan") == 0) {
        int limit = 20;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--limit") == 0) limit = atoi(argv[i+1]);
        }
        return cmd_collapse_plan(db, limit, json_mode);
    }

    if (argc >= 3 && strcmp(argv[1], "collapse-ledger") == 0) {
        int limit = 20;
        for (int i = 3; i < argc - 1; i++) {
            if (strcmp(argv[i], "--limit") == 0) limit = atoi(argv[i+1]);
        }
        return cmd_collapse_ledger(db, argv[2], limit);
    }

    if (argc >= 3 && strcmp(argv[1], "merge-manifest") == 0) {
        int limit = 20;
        for (int i = 3; i < argc - 1; i++) {
            if (strcmp(argv[i], "--limit") == 0) limit = atoi(argv[i+1]);
        }
        return cmd_merge_manifest(db, argv[2], limit);
    }

    if (argc >= 4 && strcmp(argv[1], "bundle-layout") == 0) {
        return cmd_bundle_layout(argv[2], argv[3]);
    }

    if (argc >= 3 && strcmp(argv[1], "validate-preview") == 0) {
        return cmd_validate_preview_artifact(argv[2]);
    }

    if (argc >= 4 && strcmp(argv[1], "bundle-diff") == 0) {
        return cmd_bundle_diff(argv[2], argv[3]);
    }

    if (argc >= 2 && strcmp(argv[1], "stats") == 0)
        return cmd_stats(db);

    fprintf(stderr,
        "BonfyreIndex — artifact family index & search\n\n"
        "Usage:\n"
        "  bonfyre-index build <dir> [--db F]      Crawl & index artifact.json files\n"
        "  bonfyre-index search <q> [--type T]      Search families\n"
        "  bonfyre-index reuse [--db F]             Find shared atoms/operators\n"
        "  bonfyre-index equivalence [--db F]       Show canonical-equivalent families\n"
        "  bonfyre-index collapse-preview [--db F]  Preview safe equivalence collapse groups\n"
        "  bonfyre-index confidence-preview [--db F] Preview exact/probable/related family groups\n"
        "  bonfyre-index collapse-plan [--db F]     Choose deterministic keep/fold representatives\n"
        "  bonfyre-index collapse-ledger <out> [--db F] Write non-destructive collapse action ledger\n"
        "  bonfyre-index merge-manifest <out> [--db F] Write preview-only merge manifest artifact\n"
        "  bonfyre-index bundle-layout <merge> <out> Build preview-only canonical bundle layout\n"
        "  bonfyre-index validate-preview <path>    Validate preview-only merge/layout artifact\n"
        "  bonfyre-index bundle-diff <layout> <out> Compare current disk state to projected bundle layout\n"
        "  Add --json to the preview commands for machine-readable output\n"
        "  bonfyre-index stats [--db F]             Summary statistics\n"
    );
    return 1;
}
