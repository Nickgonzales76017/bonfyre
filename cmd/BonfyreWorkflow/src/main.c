/*
 * bonfyre-workflow - workflow profile browser.
 *
 * Workflow profiles are operator-facing suites that describe what kind of
 * outcome Bonfyre should produce. They are not the same thing as concrete
 * executable recipes.
 *
 * Commands:
 *   bonfyre-workflow help
 *   bonfyre-workflow list [term] [--json] [--compact]
 *   bonfyre-workflow show <WORKFLOW_ID | path/to/workflow.json>
 */

#include <ctype.h>
#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sqlite3.h>
#include <bonfyre.h>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#define MAX_PATH_LEN   4096
#define MAX_JSON_BYTES 2097152
#define MAX_WORKFLOWS  256

typedef struct {
    int json;
    int compact;
    char query[256];
} ListOptions;

typedef struct {
    char id[64];
    char name[256];
    char version[64];
    char category[128];
    char description[1024];
    char tags[256];
    char path[MAX_PATH_LEN];
    int inputs;
    int outputs;
    int stages;
    int models;
} WorkflowSummary;

typedef struct {
    char id[64];
    char name[256];
    char operator[256];
    char description[1024];
    int parallel;
} StepSummary;

typedef struct {
    char capability_role[64];
    char stage_class[64];
    char artifact_out[64];
    char layer_attachment[160];
} StepBindingInference;

static int is_json_file(const char *name) {
    size_t len = strlen(name);
    return len > 5 && strcmp(name + len - 5, ".json") == 0;
}

static int is_dir(const char *path) {
    struct stat st;
    return path && stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

static int contains_ci(const char *haystack, const char *needle) {
    size_t needle_len;
    if (!needle || !needle[0]) return 1;
    if (!haystack) return 0;

    needle_len = strlen(needle);
    if (needle_len == 0) return 1;

    for (size_t i = 0; haystack[i]; i++) {
        size_t j = 0;
        while (needle[j] && haystack[i + j]
               && tolower((unsigned char)haystack[i + j]) == tolower((unsigned char)needle[j])) {
            j++;
        }
        if (j == needle_len) return 1;
    }
    return 0;
}

static void append_query_token(char *dst, size_t size, const char *token) {
    size_t len;
    if (!dst || size == 0 || !token || !token[0]) return;
    len = strlen(dst);
    if (len > 0 && len + 1 < size) {
        dst[len++] = ' ';
        dst[len] = '\0';
    }
    if (len + 1 < size)
        snprintf(dst + len, size - len, "%s", token);
}

static ListOptions parse_list_options(int argc, char *argv[]) {
    ListOptions opts;
    memset(&opts, 0, sizeof(opts));

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--json") == 0) {
            opts.json = 1;
            continue;
        }
        if (strcmp(argv[i], "--compact") == 0 || strcmp(argv[i], "-c") == 0) {
            opts.compact = 1;
            continue;
        }
        append_query_token(opts.query, sizeof(opts.query), argv[i]);
    }
    return opts;
}

static void get_self_dir(char *buf, size_t sz) {
    char self[PATH_MAX];
    memset(self, 0, sizeof(self));
#ifdef __APPLE__
    uint32_t bsz = sizeof(self);
    if (_NSGetExecutablePath(self, &bsz) != 0) self[0] = '\0';
#elif defined(__linux__)
    ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
    if (n > 0) self[n] = '\0'; else self[0] = '\0';
#else
    self[0] = '\0';
#endif
    if (self[0]) {
        char *last = strrchr(self, '/');
        if (last) {
            *last = '\0';
            snprintf(buf, sz, "%s", self);
            return;
        }
    }
    buf[0] = '\0';
}

static int __attribute__((unused)) find_workflow_dir(char *out, size_t out_sz) {
    static const char *cwd_candidates[] = {
        "docs/recipes",
        "../docs/recipes",
        "../../docs/recipes",
        NULL
    };
    char self_dir[PATH_MAX];
    char candidate[MAX_PATH_LEN];
    const char *env = getenv("BONFYRE_WORKFLOW_DIR");

    if (env && is_dir(env)) {
        snprintf(out, out_sz, "%s", env);
        return 1;
    }

    for (int i = 0; cwd_candidates[i]; i++) {
        if (is_dir(cwd_candidates[i])) {
            snprintf(out, out_sz, "%s", cwd_candidates[i]);
            return 1;
        }
    }

    get_self_dir(self_dir, sizeof(self_dir));
    if (!self_dir[0]) return 0;

    snprintf(candidate, sizeof(candidate), "%s/../docs/recipes", self_dir);
    if (is_dir(candidate)) { snprintf(out, out_sz, "%s", candidate); return 1; }
    snprintf(candidate, sizeof(candidate), "%s/../../docs/recipes", self_dir);
    if (is_dir(candidate)) { snprintf(out, out_sz, "%s", candidate); return 1; }
    snprintf(candidate, sizeof(candidate), "%s/../../../docs/recipes", self_dir);
    if (is_dir(candidate)) { snprintf(out, out_sz, "%s", candidate); return 1; }
    snprintf(candidate, sizeof(candidate), "%s/../../../../docs/recipes", self_dir);
    if (is_dir(candidate)) { snprintf(out, out_sz, "%s", candidate); return 1; }

    return 0;
}

static char *read_file(const char *path, size_t *out_size) {
    size_t sz = 0;
    char *buf = bf_read_file(path, &sz);
    if (buf && sz > MAX_JSON_BYTES) {
        free(buf);
        return NULL;
    }
    if (out_size) *out_size = sz;
    return buf;
}

static void json_escape(FILE *out, const char *s) {
    fputc('"', out);
    for (; *s; s++) {
        unsigned char ch = (unsigned char)*s;
        switch (ch) {
            case '"': fputs("\\\"", out); break;
            case '\\': fputs("\\\\", out); break;
            case '\b': fputs("\\b", out); break;
            case '\f': fputs("\\f", out); break;
            case '\n': fputs("\\n", out); break;
            case '\r': fputs("\\r", out); break;
            case '\t': fputs("\\t", out); break;
            default:
                if (ch < 0x20) fprintf(out, "\\u%04x", ch);
                else fputc((int)ch, out);
        }
    }
    fputc('"', out);
}

static int json_extract_string_from_span(const char *start, const char *end,
                                         const char *key, char *out, size_t out_sz) {
    char needle[128];
    const char *p;

    if (!start || !key || !out || out_sz == 0) return 0;
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    p = start;
    while ((p = strstr(p, needle)) != NULL) {
        const char *q;
        const char *value_start;
        size_t len = 0;
        if (end && p >= end) break;
        q = p + strlen(needle);
        while ((!end || q < end) && *q && *q != ':') q++;
        if ((!end || q < end) && *q == ':') q++;
        while ((!end || q < end) && *q && isspace((unsigned char)*q)) q++;
        if (!*q || (end && q >= end) || *q != '"') {
            p += strlen(needle);
            continue;
        }
        value_start = ++q;
        while ((!end || q < end) && *q) {
            if (*q == '"' && *(q - 1) != '\\') break;
            q++;
        }
        if (!*q || (end && q > end)) break;
        len = (size_t)(q - value_start);
        if (len >= out_sz) len = out_sz - 1;
        memcpy(out, value_start, len);
        out[len] = '\0';
        return 1;
    }
    return 0;
}

static int json_find_array_span(const char *json, const char *key,
                                const char **array_start, const char **array_end) {
    char needle[128];
    const char *p;
    int depth = 0;
    int in_string = 0;
    int escape = 0;

    if (!json || !key || !array_start || !array_end) return 0;
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    p = strstr(json, needle);
    if (!p) return 0;

    p += strlen(needle);
    while (*p && *p != ':') p++;
    if (*p != ':') return 0;
    p++;
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != '[') return 0;

    *array_start = p;
    for (; *p; p++) {
        char ch = *p;
        if (in_string) {
            if (escape) {
                escape = 0;
            } else if (ch == '\\') {
                escape = 1;
            } else if (ch == '"') {
                in_string = 0;
            }
            continue;
        }
        if (ch == '"') {
            in_string = 1;
            continue;
        }
        if (ch == '[') depth++;
        if (ch == ']') {
            depth--;
            if (depth == 0) {
                *array_end = p;
                return 1;
            }
        }
    }

    return 0;
}

static int json_count_array_objects(const char *json, const char *key) {
    const char *start;
    const char *end;
    int count = 0;
    int depth = 0;
    int in_string = 0;
    int escape = 0;

    if (!json_find_array_span(json, key, &start, &end)) return 0;

    for (const char *p = start + 1; p < end; p++) {
        char ch = *p;
        if (in_string) {
            if (escape) {
                escape = 0;
            } else if (ch == '\\') {
                escape = 1;
            } else if (ch == '"') {
                in_string = 0;
            }
            continue;
        }
        if (ch == '"') {
            in_string = 1;
            continue;
        }
        if (ch == '{') {
            if (depth == 0) count++;
            depth++;
        } else if (ch == '}' && depth > 0) {
            depth--;
        }
    }

    return count;
}

static int json_extract_string_array_preview(const char *json, const char *key,
                                             char *out, size_t out_sz) {
    const char *start;
    const char *end;
    size_t used = 0;
    int emitted = 0;
    int in_string = 0;
    int escape = 0;
    const char *value_start = NULL;

    if (!out || out_sz == 0) return 0;
    out[0] = '\0';
    if (!json_find_array_span(json, key, &start, &end)) return 0;

    for (const char *p = start + 1; p < end; p++) {
        char ch = *p;
        if (!in_string) {
            if (ch == '"') {
                in_string = 1;
                escape = 0;
                value_start = p + 1;
            }
            continue;
        }
        if (escape) {
            escape = 0;
            continue;
        }
        if (ch == '\\') {
            escape = 1;
            continue;
        }
        if (ch == '"' && value_start) {
            size_t len = (size_t)(p - value_start);
            if (emitted > 0 && used + 2 < out_sz) {
                out[used++] = ',';
                out[used++] = ' ';
            }
            if (used + len >= out_sz) len = out_sz - used - 1;
            memcpy(out + used, value_start, len);
            used += len;
            out[used] = '\0';
            emitted++;
            in_string = 0;
            value_start = NULL;
        }
    }

    return emitted > 0;
}

static int json_extract_int_from_span(const char *start, const char *end,
                                      const char *key, int *out_value) {
    char needle[128];
    const char *p;

    if (!start || !key || !out_value) return 0;
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    p = start;

    while ((p = strstr(p, needle)) != NULL) {
        const char *q;
        char *endptr;
        long value;

        if (end && p >= end) break;
        q = p + strlen(needle);
        while ((!end || q < end) && *q && *q != ':') q++;
        if ((!end || q < end) && *q == ':') q++;
        while ((!end || q < end) && *q && isspace((unsigned char)*q)) q++;
        if (!*q || (end && q >= end) || (!isdigit((unsigned char)*q) && *q != '-')) {
            p += strlen(needle);
            continue;
        }

        value = strtol(q, &endptr, 10);
        if (endptr == q) {
            p += strlen(needle);
            continue;
        }
        if (end && endptr > end) break;
        *out_value = (int)value;
        return 1;
    }

    return 0;
}

static int json_find_array_span_in_span(const char *start, const char *end,
                                        const char *key,
                                        const char **array_start, const char **array_end) {
    char needle[128];
    const char *p;
    int depth = 0;
    int in_string = 0;
    int escape = 0;

    if (!start || !key || !array_start || !array_end) return 0;
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    p = start;

    while ((p = strstr(p, needle)) != NULL) {
        if (end && p >= end) break;
        p += strlen(needle);
        while ((!end || p < end) && *p && *p != ':') p++;
        if ((end && p >= end) || *p != ':') break;
        p++;
        while ((!end || p < end) && *p && isspace((unsigned char)*p)) p++;
        if ((end && p >= end) || *p != '[') continue;

        *array_start = p;
        for (; (!end || p < end) && *p; p++) {
            char ch = *p;
            if (in_string) {
                if (escape) {
                    escape = 0;
                } else if (ch == '\\') {
                    escape = 1;
                } else if (ch == '"') {
                    in_string = 0;
                }
                continue;
            }
            if (ch == '"') {
                in_string = 1;
                continue;
            }
            if (ch == '[') depth++;
            if (ch == ']') {
                depth--;
                if (depth == 0) {
                    *array_end = p;
                    return 1;
                }
            }
        }
        break;
    }

    return 0;
}

static int json_extract_first_string_value_from_span(const char *start, const char *end,
                                                     char *out, size_t out_sz) {
    const char *p;
    if (!start || !out || out_sz == 0) return 0;

    for (p = start; (!end || p < end) && *p; p++) {
        const char *value_start;
        size_t len;
        if (*p != ':') continue;
        p++;
        while ((!end || p < end) && *p && isspace((unsigned char)*p)) p++;
        if (!*p || (end && p >= end) || *p != '"') continue;
        value_start = ++p;
        while ((!end || p < end) && *p) {
            if (*p == '"' && *(p - 1) != '\\') break;
            p++;
        }
        if (!*p || (end && p > end)) break;
        len = (size_t)(p - value_start);
        if (len >= out_sz) len = out_sz - 1;
        memcpy(out, value_start, len);
        out[len] = '\0';
        return 1;
    }

    return 0;
}

static int args_has_token(const char *obj_start, const char *obj_end, const char *token) {
    const char *start;
    const char *end;
    const char *value_start = NULL;
    int in_string = 0;
    int escape = 0;

    if (!obj_start || !token || !token[0]) return 0;
    if (!json_find_array_span_in_span(obj_start, obj_end, "args", &start, &end))
        return 0;

    for (const char *p = start + 1; p < end; p++) {
        char ch = *p;
        if (!in_string) {
            if (ch == '"') {
                in_string = 1;
                escape = 0;
                value_start = p + 1;
            }
            continue;
        }
        if (escape) {
            escape = 0;
            continue;
        }
        if (ch == '\\') {
            escape = 1;
            continue;
        }
        if (ch == '"' && value_start) {
            size_t len = (size_t)(p - value_start);
            if (strlen(token) == len && strncmp(value_start, token, len) == 0)
                return 1;
            in_string = 0;
            value_start = NULL;
        }
    }

    return 0;
}

static int json_extract_string_array_preview_from_span(const char *start, const char *end,
                                                       const char *key, char *out, size_t out_sz) {
    const char *array_start;
    const char *array_end;
    size_t used = 0;
    int emitted = 0;
    int in_string = 0;
    int escape = 0;
    const char *value_start = NULL;

    if (!out || out_sz == 0) return 0;
    out[0] = '\0';
    if (!json_find_array_span_in_span(start, end, key, &array_start, &array_end)) return 0;

    for (const char *p = array_start + 1; p < array_end; p++) {
        char ch = *p;
        if (!in_string) {
            if (ch == '"') {
                in_string = 1;
                escape = 0;
                value_start = p + 1;
            }
            continue;
        }
        if (escape) {
            escape = 0;
            continue;
        }
        if (ch == '\\') {
            escape = 1;
            continue;
        }
        if (ch == '"' && value_start) {
            size_t len = (size_t)(p - value_start);
            if (emitted > 0 && used + 2 < out_sz) {
                out[used++] = ',';
                out[used++] = ' ';
            }
            if (used + len >= out_sz) len = out_sz - used - 1;
            memcpy(out + used, value_start, len);
            used += len;
            out[used] = '\0';
            emitted++;
            in_string = 0;
            value_start = NULL;
        }
    }

    return emitted > 0;
}

static int collect_flag_values_from_args(const char *obj_start, const char *obj_end,
                                         const char *needle, char *out, size_t out_sz) {
    const char *start;
    const char *end;
    const char *value_start = NULL;
    char pending_flag[128];
    size_t used = 0;
    int in_string = 0;
    int escape = 0;
    int count = 0;

    if (!out || out_sz == 0 || !needle || !needle[0]) return 0;
    out[0] = '\0';
    pending_flag[0] = '\0';
    if (!json_find_array_span_in_span(obj_start, obj_end, "args", &start, &end))
        return 0;

    for (const char *p = start + 1; p < end; p++) {
        char ch = *p;
        if (!in_string) {
            if (ch == '"') {
                in_string = 1;
                escape = 0;
                value_start = p + 1;
            }
            continue;
        }
        if (escape) {
            escape = 0;
            continue;
        }
        if (ch == '\\') {
            escape = 1;
            continue;
        }
        if (ch == '"' && value_start) {
            size_t len = (size_t)(p - value_start);
            char token[256];
            size_t copy_len = len < sizeof(token) - 1 ? len : sizeof(token) - 1;

            memcpy(token, value_start, copy_len);
            token[copy_len] = '\0';

            if (pending_flag[0] && token[0] != '-') {
                int n;
                if (count > 0 && used + 2 < out_sz) {
                    out[used++] = ',';
                    out[used++] = ' ';
                }
                n = snprintf(out + used, out_sz - used, "%s=%s", pending_flag, token);
                if (n > 0 && (size_t)n < out_sz - used) {
                    used += (size_t)n;
                    count++;
                }
                pending_flag[0] = '\0';
            } else if (token[0] == '-' && strstr(token, needle) != NULL) {
                const char *eq = strchr(token, '=');
                if (eq && *(eq + 1)) {
                    int n;
                    if (count > 0 && used + 2 < out_sz) {
                        out[used++] = ',';
                        out[used++] = ' ';
                    }
                    n = snprintf(out + used, out_sz - used, "%s", token);
                    if (n > 0 && (size_t)n < out_sz - used) {
                        used += (size_t)n;
                        count++;
                    }
                    pending_flag[0] = '\0';
                } else {
                    snprintf(pending_flag, sizeof(pending_flag), "%s", token);
                }
            } else if (token[0] == '-') {
                pending_flag[0] = '\0';
            }

            in_string = 0;
            value_start = NULL;
        }
    }

    return count;
}

static int text_has_layer_signal(const char *text) {
    return text && (
        contains_ci(text, "fpq") ||
        contains_ci(text, "fpqx") ||
        contains_ci(text, "sli") ||
        contains_ci(text, "layer") ||
        contains_ci(text, "e8") ||
        contains_ci(text, "shared-qk") ||
        contains_ci(text, "parallel-residual")
    );
}

static int json_next_object_in_array(const char *array_start, const char *array_end,
                                     const char **cursor,
                                     const char **obj_start, const char **obj_end) {
    const char *p = *cursor ? *cursor : array_start + 1;
    const char *start = NULL;
    int depth = 0;
    int in_string = 0;
    int escape = 0;

    for (; p < array_end; p++) {
        char ch = *p;
        if (in_string) {
            if (escape) {
                escape = 0;
            } else if (ch == '\\') {
                escape = 1;
            } else if (ch == '"') {
                in_string = 0;
            }
            continue;
        }
        if (ch == '"') {
            in_string = 1;
            continue;
        }
        if (ch == '{') {
            if (depth == 0) start = p;
            depth++;
            continue;
        }
        if (ch == '}' && depth > 0) {
            depth--;
            if (depth == 0 && start) {
                *obj_start = start;
                *obj_end = p;
                *cursor = p + 1;
                return 1;
            }
        }
    }

    return 0;
}

static int load_workflow_summary(const char *path, WorkflowSummary *wf) {
    char *json;
    size_t size = 0;
    if (!wf) return 0;

    memset(wf, 0, sizeof(*wf));
    json = read_file(path, &size);
    if (!json || size == 0) {
        free(json);
        return 0;
    }

    if (!json_extract_string_from_span(json, json + size, "recipe_id", wf->id, sizeof(wf->id)))
        snprintf(wf->id, sizeof(wf->id), "%s", "unknown");
    json_extract_string_from_span(json, json + size, "name", wf->name, sizeof(wf->name));
    json_extract_string_from_span(json, json + size, "version", wf->version, sizeof(wf->version));
    json_extract_string_from_span(json, json + size, "category", wf->category, sizeof(wf->category));
    json_extract_string_from_span(json, json + size, "description", wf->description, sizeof(wf->description));
    json_extract_string_array_preview(json, "tags", wf->tags, sizeof(wf->tags));
    snprintf(wf->path, sizeof(wf->path), "%s", path);

    wf->inputs = json_count_array_objects(json, "inputs");
    wf->outputs = json_count_array_objects(json, "outputs");
    wf->stages = json_count_array_objects(json, "stages");
    wf->models = json_count_array_objects(json, "models");

    free(json);
    return 1;
}

static int workflow_matches(const WorkflowSummary *wf, const char *query) {
    return contains_ci(wf->id, query)
        || contains_ci(wf->name, query)
        || contains_ci(wf->description, query)
        || contains_ci(wf->category, query)
        || contains_ci(wf->tags, query);
}

static int cmp_workflow_summary(const void *lhs, const void *rhs) {
    const WorkflowSummary *a = lhs;
    const WorkflowSummary *b = rhs;
    return strcmp(a->id, b->id);
}

static int load_workflows(const char *dir, WorkflowSummary *items, int max_items) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int count = 0;
    if (!dir || !items || max_items <= 0) return 0;
    if (bf_catalog_sync_default(dir) != 0) return 0;
    if (bf_sqlite3_open_ro(dir, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT external_id, name, category, summary, source_path, json_data "
        "FROM catalog_nodes WHERE kind='workflow' ORDER BY external_id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }

    while (sqlite3_step(st) == SQLITE_ROW && count < max_items) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *category = (const char *)sqlite3_column_text(st, 2);
        const char *summary = (const char *)sqlite3_column_text(st, 3);
        const char *source_path = (const char *)sqlite3_column_text(st, 4);
        const char *json = (const char *)sqlite3_column_text(st, 5);

        memset(&items[count], 0, sizeof(items[count]));
        snprintf(items[count].id, sizeof(items[count].id), "%s", id ? id : "");
        snprintf(items[count].name, sizeof(items[count].name), "%s", name ? name : "");
        snprintf(items[count].category, sizeof(items[count].category), "%s", category ? category : "");
        snprintf(items[count].description, sizeof(items[count].description), "%s", summary ? summary : "");
        snprintf(items[count].path, sizeof(items[count].path), "%s", source_path ? source_path : "");
        if (json) {
            json_extract_string_from_span(json, json + strlen(json), "version", items[count].version, sizeof(items[count].version));
            json_extract_string_array_preview(json, "tags", items[count].tags, sizeof(items[count].tags));
            items[count].inputs = json_count_array_objects(json, "inputs");
            items[count].outputs = json_count_array_objects(json, "outputs");
            items[count].stages = json_count_array_objects(json, "stages");
            items[count].models = json_count_array_objects(json, "models");
        }
        count++;
    }

    sqlite3_finalize(st);
    sqlite3_close(db);
    qsort(items, (size_t)count, sizeof(items[0]), cmp_workflow_summary);
    return count;
}

static int print_named_objects(const char *json, const char *key,
                               const char *kind, const char *secondary_key) {
    const char *start;
    const char *end;
    const char *cursor = NULL;
    int count = 0;

    if (!json_find_array_span(json, key, &start, &end)) return 0;

    while (1) {
        const char *obj_start;
        const char *obj_end;
        char name[256];
        char secondary[512];

        if (!json_next_object_in_array(start, end, &cursor, &obj_start, &obj_end))
            break;

        name[0] = '\0';
        secondary[0] = '\0';
        json_extract_string_from_span(obj_start, obj_end, "name", name, sizeof(name));
        if (!name[0])
            json_extract_string_from_span(obj_start, obj_end, "id", name, sizeof(name));
        if (!name[0])
            json_extract_first_string_value_from_span(obj_start, obj_end, name, sizeof(name));
        if (secondary_key)
            json_extract_string_from_span(obj_start, obj_end, secondary_key, secondary, sizeof(secondary));

        if (secondary_key && secondary[0]) {
            printf("  - %-20s  %s\n", name[0] ? name : kind, secondary);
        } else if (name[0]) {
            printf("  - %s\n", name);
        } else {
            printf("  - %s\n", kind);
        }
        count++;
    }

    return count;
}

static int print_stage_rows(const char *json) {
    const char *start;
    const char *end;
    const char *cursor = NULL;
    int count = 0;

    if (!json_find_array_span(json, "stages", &start, &end)) return 0;

    while (1) {
        const char *obj_start;
        const char *obj_end;
        char id[64];
        char name[256];
        char op[256];

        if (!json_next_object_in_array(start, end, &cursor, &obj_start, &obj_end))
            break;

        id[0] = '\0';
        name[0] = '\0';
        op[0] = '\0';
        json_extract_string_from_span(obj_start, obj_end, "id", id, sizeof(id));
        json_extract_string_from_span(obj_start, obj_end, "name", name, sizeof(name));
        json_extract_string_from_span(obj_start, obj_end, "operator", op, sizeof(op));

        printf("  - %-4s  %-30s  %s\n",
               id[0] ? id : "--",
               name[0] ? name : "(unnamed stage)",
               op[0] ? op : "(operator not declared)");
        count++;
    }

    return count;
}

static int load_step_summary(const char *obj_start, const char *obj_end, StepSummary *step) {
    if (!obj_start || !obj_end || !step) return 0;

    memset(step, 0, sizeof(*step));
    json_extract_string_from_span(obj_start, obj_end, "id", step->id, sizeof(step->id));
    json_extract_string_from_span(obj_start, obj_end, "name", step->name, sizeof(step->name));
    json_extract_string_from_span(obj_start, obj_end, "operator", step->operator, sizeof(step->operator));
    json_extract_string_from_span(obj_start, obj_end, "description", step->description, sizeof(step->description));
    json_extract_int_from_span(obj_start, obj_end, "parallel", &step->parallel);
    return 1;
}

static int print_string_array_field(const char *obj_start, const char *obj_end, const char *key) {
    const char *start;
    const char *end;
    const char *value_start = NULL;
    int in_string = 0;
    int escape = 0;
    int count = 0;

    if (!json_find_array_span_in_span(obj_start, obj_end, key, &start, &end))
        return 0;

    for (const char *p = start + 1; p < end; p++) {
        char ch = *p;
        if (!in_string) {
            if (ch == '"') {
                in_string = 1;
                escape = 0;
                value_start = p + 1;
            }
            continue;
        }
        if (escape) {
            escape = 0;
            continue;
        }
        if (ch == '\\') {
            escape = 1;
            continue;
        }
        if (ch == '"' && value_start) {
            printf("  - %.*s\n", (int)(p - value_start), value_start);
            count++;
            in_string = 0;
            value_start = NULL;
        }
    }

    return count;
}

static int find_stage_match(const char *json, const char *selector,
                            const char **match_start, const char **match_end,
                            StepSummary *matched_step) {
    const char *stages_start;
    const char *stages_end;
    const char *cursor = NULL;
    const char *partial_start = NULL;
    const char *partial_end = NULL;
    StepSummary partial_step;
    int found_partial = 0;

    if (!json || !selector || !selector[0]) return 0;
    if (!json_find_array_span(json, "stages", &stages_start, &stages_end)) return 0;

    while (1) {
        const char *obj_start;
        const char *obj_end;
        StepSummary step;

        if (!json_next_object_in_array(stages_start, stages_end, &cursor, &obj_start, &obj_end))
            break;

        load_step_summary(obj_start, obj_end, &step);

        if ((step.id[0] && strcmp(step.id, selector) == 0) ||
            (step.name[0] && strcmp(step.name, selector) == 0)) {
            if (match_start) *match_start = obj_start;
            if (match_end) *match_end = obj_end;
            if (matched_step) *matched_step = step;
            return 1;
        }

        if (!found_partial &&
            (contains_ci(step.id, selector) || contains_ci(step.name, selector) || contains_ci(step.operator, selector))) {
            partial_start = obj_start;
            partial_end = obj_end;
            partial_step = step;
            found_partial = 1;
        }
    }

    if (found_partial) {
        if (match_start) *match_start = partial_start;
        if (match_end) *match_end = partial_end;
        if (matched_step) *matched_step = partial_step;
        return 1;
    }

    return 0;
}

static int print_related_families_for_step(const char *workflow_id, const char *step_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[MAX_PATH_LEN];
    char step_node_id[256];
    int shown = 0;

    if (!workflow_id || !workflow_id[0] || !step_id || !step_id[0]) return 0;
    bf_catalog_default_db_path(db_path, sizeof(db_path));
    if (bf_catalog_sync_default(db_path) != 0) return 0;
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT fn.external_id "
        "FROM catalog_edges e "
        "JOIN catalog_nodes fn ON fn.node_id = e.dst_node_id "
        "WHERE e.src_node_id = ? AND e.rel = 'specialized_by_family' "
        "ORDER BY fn.external_id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }

    snprintf(step_node_id, sizeof(step_node_id), "workflow-step:%s:%s", workflow_id, step_id);
    sqlite3_bind_text(st, 1, step_node_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *family_id = (const char *)sqlite3_column_text(st, 0);
        if (shown == 0) printf("\nFamilies\n");
        printf("  - %s\n", family_id ? family_id : "");
        shown++;
    }

    sqlite3_finalize(st);
    sqlite3_close(db);
    return shown;
}

static void infer_step_binding(const StepSummary *step, const char *obj_start, const char *obj_end,
                               const char *declared_model_ref, const char *declared_layer_refs,
                               const char *observed_model_flags, const char *observed_layer_flags,
                               StepBindingInference *inference) {
    const char *op;
    int explicit_layers;
    int implicit_layer_signal;

    if (!step || !inference) return;

    memset(inference, 0, sizeof(*inference));
    op = step->operator;

    if (strcmp(op, "BonfyreTranscribe") == 0) {
        if (args_has_token(obj_start, obj_end, "--clean") || contains_ci(step->name, "clean")) {
            snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "clean");
            snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "transform");
            snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "clean-transcript");
        } else {
            snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "transcribe");
            snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "ingest");
            snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "transcript");
        }
    } else if (strcmp(op, "BonfyreSpeechLoop") == 0 || strcmp(op, "BonfyreSegment") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "segment");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "transform");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "segments");
    } else if (strcmp(op, "BonfyreBrief") == 0 || strcmp(op, "BonfyreCodeBrief") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "brief");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "transform");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "brief");
    } else if (strcmp(op, "BonfyreProof") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "proof");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "score");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "proof");
    } else if (strcmp(op, "BonfyreOffer") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "offer");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "transform");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "offer");
    } else if (strcmp(op, "BonfyreNarrate") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "narrate");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "emit");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "narration");
    } else if (strcmp(op, "BonfyrePack") == 0 || strcmp(op, "BonfyrePackage") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "pack");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "emit");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "bundle");
    } else if (strcmp(op, "BonfyreDistribute") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "distribute");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "emit");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "distribution");
    } else if (strcmp(op, "BonfyreEmbed") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "embed");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "transform");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "embeddings");
    } else if (strcmp(op, "BonfyreModel") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "model");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "registry");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "model-ref");
    } else if (strcmp(op, "BonfyreLayer") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "layer");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "registry");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "layer-spec");
    } else if (strcmp(op, "BonfyreSLI") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "sli");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "optimization");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "routed-family");
    } else if (strcmp(op, "BonfyreFPQ") == 0 || strcmp(op, "BonfyreFPQx") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "fpq");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "optimization");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "fpq-pack");
    } else if (strcmp(op, "BonfyreEntity") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "entity");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "transform");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "entities");
    } else if (strcmp(op, "BonfyreCanon") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "canon");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "transform");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "canon");
    } else if (strcmp(op, "BonfyreGraph") == 0 || strcmp(op, "BonfyreCodeGraph") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "graph");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "transform");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "graph");
    } else if (strcmp(op, "BonfyreClaims") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "claims");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "transform");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "claims");
    } else if (strcmp(op, "BonfyreHypothesis") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "hypothesis");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "transform");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "hypotheses");
    } else if (strcmp(op, "BonfyreConvergence") == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", "convergence");
        snprintf(inference->stage_class, sizeof(inference->stage_class), "%s", "score");
        snprintf(inference->artifact_out, sizeof(inference->artifact_out), "%s", "convergence");
    } else if (strncmp(op, "Bonfyre", 7) == 0) {
        snprintf(inference->capability_role, sizeof(inference->capability_role), "%s", op + 7);
    }

    explicit_layers = (declared_layer_refs && declared_layer_refs[0]) || (observed_layer_flags && observed_layer_flags[0]);
    implicit_layer_signal = text_has_layer_signal(op)
        || text_has_layer_signal(declared_model_ref)
        || text_has_layer_signal(observed_model_flags)
        || text_has_layer_signal(step->name);

    if (explicit_layers) {
        snprintf(inference->layer_attachment, sizeof(inference->layer_attachment),
                 "%s", "explicit layer binding declared on this step");
    } else if (implicit_layer_signal) {
        snprintf(inference->layer_attachment, sizeof(inference->layer_attachment),
                 "%s", "layer-sensitive or model-bound, but no explicit layer_refs are declared");
    } else {
        snprintf(inference->layer_attachment, sizeof(inference->layer_attachment),
                 "%s", "no explicit layer binding is declared for this step");
    }
}

static void cmd_help(void) {
    printf(
        "bonfyre-workflow - workflow profile browser\n\n"
        "Workflow profiles are operator-facing suites. They describe the shape\n"
        "of work Bonfyre should perform, but they are not concrete executable\n"
        "recipes or step capabilities.\n\n"
        "Usage:\n"
        "  bonfyre-workflow list [term] [--json] [--compact]\n"
        "  bonfyre-workflow show <WORKFLOW_ID | path/to/workflow.json>\n"
        "  bonfyre-workflow steps <WORKFLOW_ID | path/to/workflow.json>\n"
        "  bonfyre-workflow step <WORKFLOW_ID | path/to/workflow.json> <STEP_ID | name>\n"
        "  bonfyre-workflow help\n\n"
        "Examples:\n"
        "  bonfyre workflow list\n"
        "  bonfyre workflow list podcast\n"
        "  bonfyre workflow show A3\n"
        "  bonfyre workflow steps A3\n"
        "  bonfyre workflow step A3 s02\n\n"
        "Related surfaces:\n"
        "  bonfyre recipe list          concrete executable manifests\n"
        "  bonfyre capabilities help   step traits and matching registry\n"
        "  bonfyre model --help        model families and routing\n"
        "  bonfyre layer --help        layer bindings and pack operations\n"
    );
}

static void cmd_list_table(const char *root, const WorkflowSummary *items, int count, const char *query) {
    int shown = 0;
    printf("bonfyre-workflow  operator-facing workflow profiles\n");
    printf("catalog   %s\n", root);
    if (query && query[0]) printf("filter    %s\n", query);
    printf("note      Profiles describe workflow intent. Use bonfyre recipe/run for concrete manifests.\n\n");
    printf("%-6s %-6s %-6s %-7s %-18s %s\n",
           "ID", "Steps", "In", "Out", "Category", "Workflow");

    for (int i = 0; i < count; i++) {
        if (!workflow_matches(&items[i], query)) continue;
        printf("%-6s %-6d %-6d %-7d %-18s %s\n",
               items[i].id[0] ? items[i].id : "-",
               items[i].stages,
               items[i].inputs,
               items[i].outputs,
               items[i].category[0] ? items[i].category : "-",
               items[i].name[0] ? items[i].name : "(unnamed workflow)");
        shown++;
    }

    if (shown == 0)
        printf("No workflow profiles matched that filter.\n");
}

static void cmd_list_compact(const WorkflowSummary *items, int count, const char *query) {
    int shown = 0;
    printf("bonfyre-workflow  compact\n");
    if (query && query[0]) printf("filter   %s\n", query);

    for (int i = 0; i < count; i++) {
        if (!workflow_matches(&items[i], query)) continue;
        printf("%s  %s  [%d steps]\n",
               items[i].id[0] ? items[i].id : "-",
               items[i].name[0] ? items[i].name : "(unnamed workflow)",
               items[i].stages);
        shown++;
    }

    if (shown == 0)
        printf("No workflow profiles matched that filter.\n");
}

static void cmd_list_json(const char *root, const WorkflowSummary *items, int count, const char *query) {
    int first = 1;
    int shown = 0;
    printf("{\n  \"catalog\": ");
    json_escape(stdout, root);
    printf(",\n  \"query\": ");
    if (query && query[0]) json_escape(stdout, query);
    else printf("null");
    printf(",\n  \"items\": [\n");

    for (int i = 0; i < count; i++) {
        if (!workflow_matches(&items[i], query)) continue;
        if (!first) printf(",\n");
        first = 0;
        shown++;
        printf("    {\"id\": ");
        json_escape(stdout, items[i].id);
        printf(", \"name\": ");
        json_escape(stdout, items[i].name);
        printf(", \"category\": ");
        json_escape(stdout, items[i].category[0] ? items[i].category : "");
        printf(", \"description\": ");
        json_escape(stdout, items[i].description[0] ? items[i].description : "");
        printf(", \"steps\": %d, \"inputs\": %d, \"outputs\": %d, \"path\": ",
               items[i].stages, items[i].inputs, items[i].outputs);
        json_escape(stdout, items[i].path);
        printf("}");
    }

    printf("\n  ],\n  \"count\": %d\n}\n", shown);
}

static int path_exists(const char *path) {
    return path && access(path, R_OK) == 0;
}

static char *fetch_workflow_json_from_catalog(const char *workflow_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[MAX_PATH_LEN];
    char *copy = NULL;

    bf_catalog_default_db_path(db_path, sizeof(db_path));
    if (bf_catalog_sync_default(db_path) != 0) return NULL;
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT json_data FROM catalog_nodes WHERE kind='workflow' AND external_id=?",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    sqlite3_bind_text(st, 1, workflow_id, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *json = (const char *)sqlite3_column_text(st, 0);
        if (json) copy = strdup(json);
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return copy;
}

static int resolve_workflow_target(const char *arg, const WorkflowSummary *items, int count,
                                   char *target_path, size_t target_path_sz,
                                   WorkflowSummary *resolved_wf) {
    int found = -1;

    if (!arg || !arg[0] || !target_path || target_path_sz == 0) return 0;

    if (strchr(arg, '/') || is_json_file(arg)) {
        if (!path_exists(arg))
            return 0;
        if (resolved_wf && !load_workflow_summary(arg, resolved_wf))
            return 0;
        snprintf(target_path, target_path_sz, "%s", arg);
        return 1;
    }

    for (int i = 0; i < count; i++) {
        if (strcmp(items[i].id, arg) == 0) {
            found = i;
            break;
        }
    }
    if (found < 0)
        return 0;

    snprintf(target_path, target_path_sz, "%s", items[found].path);
    if (resolved_wf)
        *resolved_wf = items[found];
    return 1;
}

static void print_workflow_header(const WorkflowSummary *wf) {
    printf("workflow   %s\n", wf->id[0] ? wf->id : "-");
    printf("name       %s\n", wf->name[0] ? wf->name : "(unnamed workflow)");
    printf("version    %s\n", wf->version[0] ? wf->version : "-");
    printf("category   %s\n", wf->category[0] ? wf->category : "-");
    printf("source     %s\n", wf->path);
    printf("kind       workflow_profile\n");
    printf("note       Operator-facing suite. Use bonfyre recipe/run for concrete manifests.\n");
    if (wf->description[0]) printf("summary    %s\n", wf->description);
    printf("counts     %d steps  %d inputs  %d outputs  %d models\n",
           wf->stages, wf->inputs, wf->outputs, wf->models);
    if (wf->tags[0]) printf("tags       %s\n", wf->tags);
}

static int cmd_show(const char *arg, const char *root, const WorkflowSummary *items, int count) {
    WorkflowSummary wf;
    char *json;

    if (!arg || !arg[0]) {
        fprintf(stderr, "bonfyre-workflow: show requires a workflow id or path\n");
        return 1;
    }

    if (!resolve_workflow_target(arg, items, count, wf.path, sizeof(wf.path), &wf)) {
        fprintf(stderr, "bonfyre-workflow: unknown workflow '%s'\n", arg);
        fprintf(stderr, "Run 'bonfyre workflow list' to inspect available workflow profiles.\n");
        return 1;
    }

    json = (strchr(arg, '/') || is_json_file(arg)) ? read_file(arg, NULL) : fetch_workflow_json_from_catalog(wf.id);
    if (!json || json[0] == '\0') {
        free(json);
        fprintf(stderr, "bonfyre-workflow: failed to load workflow payload for %s\n", wf.id);
        return 1;
    }

    print_workflow_header(&wf);

    printf("\nInputs\n");
    if (!print_named_objects(json, "inputs", "input", "type"))
        printf("  - none declared\n");

    printf("\nOutputs\n");
    if (!print_named_objects(json, "outputs", "output", "path"))
        printf("  - none declared\n");

    printf("\nStages\n");
    if (!print_stage_rows(json))
        printf("  - none declared\n");

    if (json_count_array_objects(json, "models") > 0) {
        printf("\nModels\n");
        if (!print_named_objects(json, "models", "model", NULL))
            printf("  - none declared\n");
    }

    printf("\nRelated\n");
    printf("  bonfyre recipe list\n");
    printf("  bonfyre capabilities help\n");
    printf("  bonfyre model --help\n");
    printf("  bonfyre layer --help\n");

    free(json);
    (void)root;
    return 0;
}

static int cmd_steps(const char *arg, const WorkflowSummary *items, int count) {
    WorkflowSummary wf;
    char *json;
    const char *stages_start;
    const char *stages_end;
    const char *cursor = NULL;

    if (!arg || !arg[0]) {
        fprintf(stderr, "bonfyre-workflow: steps requires a workflow id or path\n");
        return 1;
    }

    if (!resolve_workflow_target(arg, items, count, wf.path, sizeof(wf.path), &wf)) {
        fprintf(stderr, "bonfyre-workflow: unknown workflow '%s'\n", arg);
        fprintf(stderr, "Run 'bonfyre workflow list' to inspect available workflow profiles.\n");
        return 1;
    }

    json = (strchr(arg, '/') || is_json_file(arg)) ? read_file(arg, NULL) : fetch_workflow_json_from_catalog(wf.id);
    if (!json || json[0] == '\0') {
        free(json);
        fprintf(stderr, "bonfyre-workflow: failed to load workflow payload for %s\n", wf.id);
        return 1;
    }

    printf("workflow   %s\n", wf.id[0] ? wf.id : "-");
    printf("name       %s\n", wf.name[0] ? wf.name : "(unnamed workflow)");
    printf("steps      %d\n", wf.stages);
    printf("note       Use 'bonfyre workflow step %s <step-id>' for one-step detail.\n\n",
           wf.id[0] ? wf.id : arg);
    printf("%-4s %-30s %-24s %-8s %s\n",
           "ID", "Step", "Operator", "Parallel", "Depends On");

    if (!json_find_array_span(json, "stages", &stages_start, &stages_end)) {
        printf("No stages declared.\n");
        free(json);
        return 0;
    }

    while (1) {
        const char *obj_start;
        const char *obj_end;
        StepSummary step;
        char depends[256];

        if (!json_next_object_in_array(stages_start, stages_end, &cursor, &obj_start, &obj_end))
            break;

        load_step_summary(obj_start, obj_end, &step);
        depends[0] = '\0';
        json_extract_string_array_preview_from_span(obj_start, obj_end, "depends_on",
                                                    depends, sizeof(depends));

        printf("%-4s %-30s %-24s %-8d %s\n",
               step.id[0] ? step.id : "--",
               step.name[0] ? step.name : "(unnamed step)",
               step.operator[0] ? step.operator : "(operator not declared)",
               step.parallel > 0 ? step.parallel : 1,
               depends[0] ? depends : "-");
    }

    free(json);
    return 0;
}

static int cmd_step(const char *workflow_arg, const char *step_selector,
                    const WorkflowSummary *items, int count) {
    WorkflowSummary wf;
    char *json;
    const char *obj_start = NULL;
    const char *obj_end = NULL;
    StepSummary step;
    StepBindingInference inference;
    char declared_variant[128];
    char declared_capabilities[256];
    char declared_model_ref[256];
    char declared_layer_refs[256];
    char observed_model_flags[256];
    char observed_layer_flags[256];

    if (!workflow_arg || !workflow_arg[0] || !step_selector || !step_selector[0]) {
        fprintf(stderr, "bonfyre-workflow: step requires a workflow id/path and a step id/name\n");
        return 1;
    }

    if (!resolve_workflow_target(workflow_arg, items, count, wf.path, sizeof(wf.path), &wf)) {
        fprintf(stderr, "bonfyre-workflow: unknown workflow '%s'\n", workflow_arg);
        fprintf(stderr, "Run 'bonfyre workflow list' to inspect available workflow profiles.\n");
        return 1;
    }

    json = (strchr(workflow_arg, '/') || is_json_file(workflow_arg))
         ? read_file(workflow_arg, NULL)
         : fetch_workflow_json_from_catalog(wf.id);
    if (!json || json[0] == '\0') {
        free(json);
        fprintf(stderr, "bonfyre-workflow: failed to load workflow payload for %s\n", wf.id);
        return 1;
    }

    if (!find_stage_match(json, step_selector, &obj_start, &obj_end, &step)) {
        free(json);
        fprintf(stderr, "bonfyre-workflow: no step matched '%s' in workflow %s\n",
                step_selector, wf.id[0] ? wf.id : workflow_arg);
        fprintf(stderr, "Run 'bonfyre workflow steps %s' to inspect declared steps.\n",
                wf.id[0] ? wf.id : workflow_arg);
        return 1;
    }

    printf("workflow   %s\n", wf.id[0] ? wf.id : "-");
    printf("step       %s\n", step.id[0] ? step.id : "-");
    printf("name       %s\n", step.name[0] ? step.name : "(unnamed step)");
    printf("operator   %s\n", step.operator[0] ? step.operator : "(operator not declared)");
    printf("parallel   %d\n", step.parallel > 0 ? step.parallel : 1);
    printf("kind       workflow_step\n");
    if (step.description[0]) printf("summary    %s\n", step.description);

    declared_variant[0] = '\0';
    declared_capabilities[0] = '\0';
    declared_model_ref[0] = '\0';
    declared_layer_refs[0] = '\0';
    observed_model_flags[0] = '\0';
    observed_layer_flags[0] = '\0';

    json_extract_string_from_span(obj_start, obj_end, "variant",
                                  declared_variant, sizeof(declared_variant));
    json_extract_string_from_span(obj_start, obj_end, "model_ref",
                                  declared_model_ref, sizeof(declared_model_ref));
    json_extract_string_array_preview_from_span(obj_start, obj_end, "capabilities",
                                                declared_capabilities, sizeof(declared_capabilities));
    json_extract_string_array_preview_from_span(obj_start, obj_end, "layer_refs",
                                                declared_layer_refs, sizeof(declared_layer_refs));
    collect_flag_values_from_args(obj_start, obj_end, "model",
                                  observed_model_flags, sizeof(observed_model_flags));
    collect_flag_values_from_args(obj_start, obj_end, "layer",
                                  observed_layer_flags, sizeof(observed_layer_flags));

    infer_step_binding(&step, obj_start, obj_end,
                       declared_model_ref, declared_layer_refs,
                       observed_model_flags, observed_layer_flags,
                       &inference);

    printf("\nBindings\n");
    printf("  variant        %s\n", declared_variant[0] ? declared_variant : "none declared");
    printf("  capabilities   %s\n", declared_capabilities[0] ? declared_capabilities : "none declared");
    printf("  inferred role  %s\n", inference.capability_role[0] ? inference.capability_role : "not inferred");
    printf("  stage class    %s\n", inference.stage_class[0] ? inference.stage_class : "not inferred");
    printf("  artifact out   %s\n", inference.artifact_out[0] ? inference.artifact_out : "not inferred");
    printf("  model_ref      %s\n", declared_model_ref[0] ? declared_model_ref : "none declared");
    printf("  model flags    %s\n", observed_model_flags[0] ? observed_model_flags : "none observed in args");
    printf("  layer_refs     %s\n", declared_layer_refs[0] ? declared_layer_refs : "none declared");
    printf("  layer flags    %s\n", observed_layer_flags[0] ? observed_layer_flags : "none observed in args");
    printf("  layer attach   %s\n", inference.layer_attachment[0] ? inference.layer_attachment : "not inferred");

    printf("\nArgs\n");
    if (!print_string_array_field(obj_start, obj_end, "args"))
        printf("  - none declared\n");

    printf("\nInputs\n");
    if (!print_string_array_field(obj_start, obj_end, "inputs"))
        printf("  - none declared\n");

    printf("\nOutputs\n");
    if (!print_string_array_field(obj_start, obj_end, "outputs"))
        printf("  - none declared\n");

    printf("\nDepends On\n");
    if (!print_string_array_field(obj_start, obj_end, "depends_on"))
        printf("  - none\n");

    print_related_families_for_step(wf.id, step.id);

    printf("\nRelated\n");
    printf("  bonfyre workflow steps %s\n", wf.id[0] ? wf.id : workflow_arg);
    printf("  bonfyre workflow show %s\n", wf.id[0] ? wf.id : workflow_arg);
    printf("  bonfyre capabilities help\n");
    printf("  bonfyre model --help\n");
    printf("  bonfyre layer --help\n");

    free(json);
    return 0;
}

int main(int argc, char *argv[]) {
    char root[MAX_PATH_LEN];
    WorkflowSummary items[MAX_WORKFLOWS];
    int count;
    ListOptions opts;

    bf_catalog_default_db_path(root, sizeof(root));

    count = load_workflows(root, items, MAX_WORKFLOWS);
    if (argc < 2 || strcmp(argv[1], "list") == 0) {
        opts = parse_list_options(argc, argv);
        if (opts.json) cmd_list_json(root, items, count, opts.query[0] ? opts.query : NULL);
        else if (opts.compact) cmd_list_compact(items, count, opts.query[0] ? opts.query : NULL);
        else cmd_list_table(root, items, count, opts.query[0] ? opts.query : NULL);
        return 0;
    }

    if (strcmp(argv[1], "help") == 0 || strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0) {
        cmd_help();
        return 0;
    }

    if (strcmp(argv[1], "show") == 0) {
        return cmd_show(argc >= 3 ? argv[2] : NULL, root, items, count);
    }

    if (strcmp(argv[1], "steps") == 0) {
        return cmd_steps(argc >= 3 ? argv[2] : NULL, items, count);
    }

    if (strcmp(argv[1], "step") == 0) {
        return cmd_step(argc >= 3 ? argv[2] : NULL,
                        argc >= 4 ? argv[3] : NULL,
                        items, count);
    }

    fprintf(stderr, "bonfyre-workflow: unknown command '%s'\n", argv[1]);
    fprintf(stderr, "Run 'bonfyre-workflow help' for usage.\n");
    return 1;
}
