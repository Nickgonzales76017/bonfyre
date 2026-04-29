/*
 * bonfyre-family - conceptual family browser for Bonfyre.
 *
 * Families are the specialization taxonomy that sits beside workflows,
 * recipes, capabilities, models, and layers. They are not executable
 * commands by themselves; they describe how Bonfyre can be specialized.
 *
 * Commands:
 *   bonfyre-family help
 *   bonfyre-family list [term] [--json] [--compact]
 *   bonfyre-family show <family>
 *   bonfyre-family explain <family>
 *   bonfyre-family related <family>
 *   bonfyre-family where <family>
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

#define MAX_PATH_LEN    4096
#define MAX_JSON_BYTES  524288
#define MAX_FAMILIES    256
#define MAX_WORKFLOWS   256
#define MAX_MATCHES     8

typedef struct {
    int json;
    int compact;
    char query[256];
} ListOptions;

typedef struct {
    char id[96];
    char category_key[64];
    char category_name[64];
} FamilySummary;

typedef struct {
    char id[64];
    char name[256];
    char path[MAX_PATH_LEN];
} WorkflowSummary;

typedef struct {
    char id[64];
    char name[256];
    char operator[256];
} StepSummary;

typedef struct {
    char workflow_id[64];
    char step_id[64];
    char step_name[256];
    char operator[256];
    int score;
} StepMatch;

typedef struct {
    const char *id;
    const char *meaning;
    const char *attach_to;
    const char *fields;
    const char *commands;
    const char *related;
    const char *workflow_examples;
} FamilyHint;

static const FamilyHint FAMILY_HINTS[] = {
    {
        "T_CASEOPS",
        "Case normalization and reversible case encoding for text cleanup, canonicalization, and deterministic transforms.",
        "Step-local transform trait on cleanup, canonicalization, paragraph, hashing, or compression stages.",
        "variant, capabilities, params",
        "clean, paragraph, canon, hash, compress",
        "T_WHITESPACE_PACK, T_MARKER_STREAM",
        "A3|s03|Transcript Cleanup|BonfyreTranscribe\nA3|s05|Canonicalization|BonfyreCanon"
    },
    {
        "T_WHITESPACE_PACK",
        "Whitespace normalization and entropy-oriented packing for compact, stable text representations.",
        "Step-local transform trait on cleanup, paragraph recovery, hashing, and packing stages.",
        "variant, capabilities, params",
        "clean, paragraph, pack, hash, compress",
        "T_CASEOPS, T_BYTE_REMAP",
        "A3|s03|Transcript Cleanup|BonfyreTranscribe\nA1|s03|Brief Generation|BonfyreBrief"
    },
    {
        "T_MARKER_STREAM",
        "Structural marker insertion to preserve boundaries, provenance, or typed segments across transforms.",
        "Step-local transform trait on segmentation, canon, graph, and structured transform stages.",
        "variant, capabilities, params",
        "segment, clean, canon, graph, claims",
        "T_CASEOPS, T_BYTE_REMAP",
        "A3|s01|VAD Segmentation|BonfyreSpeechLoop\nA3|s06|Graph Construction|BonfyreGraph"
    },
    {
        "T_BYTE_REMAP",
        "Byte-domain remapping and canonical encoding transforms for packing, hashing, and compression-oriented pipelines.",
        "Low-level transform trait on hashing, compression, indexing, and pack stages.",
        "variant, capabilities, params",
        "hash, compress, index, pack",
        "T_WHITESPACE_PACK, T_MARKER_STREAM",
        "A3|s06|Graph Construction|BonfyreGraph\nA1|s03|Brief Generation|BonfyreBrief"
    },
    {
        "T_RECUR_LATE",
        "Late-stage recurrent model structure, where recurrence is concentrated deeper in the stack.",
        "Model/layer structural binding on model manifests and layer specs.",
        "model_ref, layer_refs",
        "model, layer, sli, fpq",
        "T_RECUR_EARLY, T_RECUR_PROGRESSIVE",
        "A1|s03|Brief Generation|BonfyreBrief\nA3|s11|Brief Generation|BonfyreBrief"
    },
    {
        "T_RECUR_EARLY",
        "Early-stage recurrent model structure, where recurrence shapes lower layers first.",
        "Model/layer structural binding on model manifests and layer specs.",
        "model_ref, layer_refs",
        "model, layer, sli, fpq",
        "T_RECUR_LATE, T_RECUR_PROGRESSIVE",
        "A1|s03|Brief Generation|BonfyreBrief\nA3|s11|Brief Generation|BonfyreBrief"
    },
    {
        "T_RECUR_PROGRESSIVE",
        "Progressive recurrence across the stack, useful when recurrence deepens or widens stage by stage.",
        "Model/layer structural binding on model manifests and layer specs.",
        "model_ref, layer_refs",
        "model, layer, sli, fpq",
        "T_RECUR_LATE, T_RECUR_EARLY",
        "A1|s03|Brief Generation|BonfyreBrief\nA3|s11|Brief Generation|BonfyreBrief"
    },
    {
        "T_SHARED_QK",
        "Shared query/key attention structure; a model or layer topology choice rather than a simple step option.",
        "Model/layer structural binding on model manifests, layer specs, and packed transform artifacts.",
        "model_ref, layer_refs",
        "model, layer, sli, fpq, fpqx",
        "T_PARALLEL_RESIDUAL, T_RECUR_PROGRESSIVE",
        "A1|s03|Brief Generation|BonfyreBrief\nA3|s11|Brief Generation|BonfyreBrief"
    },
    {
        "T_PARALLEL_RESIDUAL",
        "Parallel residual topology in the model or layer stack, affecting how blocks compose and route activations.",
        "Model/layer structural binding on model manifests, layer specs, and packed transform artifacts.",
        "model_ref, layer_refs",
        "model, layer, sli, fpq, fpqx",
        "T_SHARED_QK, T_RECUR_PROGRESSIVE",
        "A1|s03|Brief Generation|BonfyreBrief\nA3|s11|Brief Generation|BonfyreBrief"
    },
    { NULL, NULL, NULL, NULL, NULL, NULL, NULL }
};

static int __attribute__((unused)) is_file(const char *path) {
    struct stat st;
    return path && stat(path, &st) == 0 && S_ISREG(st.st_mode);
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

static char *read_file(const char *path, size_t *out_size) {
    return bf_read_file(path, out_size);
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
        q++;
        value_start = q;
        while ((!end || q < end) && *q) {
            if (*q == '"' && q > value_start && q[-1] != '\\') break;
            q++;
        }
        len = (size_t)(q - value_start);
        if (len >= out_sz) len = out_sz - 1;
        memcpy(out, value_start, len);
        out[len] = '\0';
        return 1;
    }
    out[0] = '\0';
    return 0;
}

static int json_find_array_span(const char *json, const char *key,
                                const char **array_start, const char **array_end) {
    char needle[128];
    const char *p;
    int depth = 0;

    if (!json || !key || !array_start || !array_end) return 0;
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    p = strstr(json, needle);
    if (!p) return 0;
    p = strchr(p, '[');
    if (!p) return 0;
    *array_start = p + 1;
    for (; *p; p++) {
        if (*p == '[') depth++;
        else if (*p == ']') {
            depth--;
            if (depth == 0) {
                *array_end = p;
                return 1;
            }
        }
    }
    return 0;
}

static int json_next_object_in_array(const char *array_start, const char *array_end,
                                     const char **cursor, const char **obj_start, const char **obj_end) {
    const char *p = *cursor ? *cursor : array_start;
    int depth = 0;

    while (p && p < array_end && *p && *p != '{') p++;
    if (!p || p >= array_end || *p != '{') return 0;
    *obj_start = p;
    for (; p < array_end && *p; p++) {
        if (*p == '{') depth++;
        else if (*p == '}') {
            depth--;
            if (depth == 0) {
                *obj_end = p + 1;
                *cursor = p + 1;
                return 1;
            }
        }
    }
    return 0;
}

static int find_workflow_dir(char *out, size_t out_sz) {
    static const char *cwd_candidates[] = {
        "docs/recipes",
        "../docs/recipes",
        "../../docs/recipes",
        NULL
    };
    char self_dir[PATH_MAX];
    char candidate[MAX_PATH_LEN];
    const char *env = getenv("BONFYRE_WORKFLOW_DIR");
    struct stat st;

    if (env && stat(env, &st) == 0 && S_ISDIR(st.st_mode)) {
        snprintf(out, out_sz, "%s", env);
        return 1;
    }

    for (int i = 0; cwd_candidates[i]; i++) {
        if (stat(cwd_candidates[i], &st) == 0 && S_ISDIR(st.st_mode)) {
            snprintf(out, out_sz, "%s", cwd_candidates[i]);
            return 1;
        }
    }

    get_self_dir(self_dir, sizeof(self_dir));
    if (!self_dir[0]) return 0;

    snprintf(candidate, sizeof(candidate), "%s/../docs/recipes", self_dir);
    if (stat(candidate, &st) == 0 && S_ISDIR(st.st_mode)) { snprintf(out, out_sz, "%s", candidate); return 1; }
    snprintf(candidate, sizeof(candidate), "%s/../../docs/recipes", self_dir);
    if (stat(candidate, &st) == 0 && S_ISDIR(st.st_mode)) { snprintf(out, out_sz, "%s", candidate); return 1; }
    snprintf(candidate, sizeof(candidate), "%s/../../../docs/recipes", self_dir);
    if (stat(candidate, &st) == 0 && S_ISDIR(st.st_mode)) { snprintf(out, out_sz, "%s", candidate); return 1; }
    snprintf(candidate, sizeof(candidate), "%s/../../../../docs/recipes", self_dir);
    if (stat(candidate, &st) == 0 && S_ISDIR(st.st_mode)) { snprintf(out, out_sz, "%s", candidate); return 1; }
    return 0;
}

static int load_workflow_summary(const char *path, WorkflowSummary *wf) {
    size_t size = 0;
    char *json = read_file(path, &size);
    if (!json || size == 0) {
        free(json);
        return 0;
    }
    memset(wf, 0, sizeof(*wf));
    json_extract_string_from_span(json, NULL, "id", wf->id, sizeof(wf->id));
    json_extract_string_from_span(json, NULL, "name", wf->name, sizeof(wf->name));
    snprintf(wf->path, sizeof(wf->path), "%s", path);
    free(json);
    return 1;
}

static int load_workflows(const char *dir, WorkflowSummary *items, int max_items) {
    DIR *dp;
    struct dirent *de;
    int count = 0;

    dp = opendir(dir);
    if (!dp) return 0;
    while ((de = readdir(dp)) != NULL && count < max_items) {
        char path[MAX_PATH_LEN];
        size_t len = strlen(de->d_name);
        if (len < 6 || strcmp(de->d_name + len - 5, ".json") != 0) continue;
        snprintf(path, sizeof(path), "%s/%s", dir, de->d_name);
        if (load_workflow_summary(path, &items[count])) count++;
    }
    closedir(dp);
    return count;
}

static void load_step_summary(const char *obj_start, const char *obj_end, StepSummary *step) {
    memset(step, 0, sizeof(*step));
    json_extract_string_from_span(obj_start, obj_end, "id", step->id, sizeof(step->id));
    json_extract_string_from_span(obj_start, obj_end, "name", step->name, sizeof(step->name));
    json_extract_string_from_span(obj_start, obj_end, "operator", step->operator, sizeof(step->operator));
}

static int tokenize_csv(const char *csv, char tokens[][64], int max_tokens) {
    int count = 0;
    const char *p = csv;
    while (p && *p && count < max_tokens) {
        while (*p == ' ' || *p == ',') p++;
        if (!*p) break;
        {
            const char *start = p;
            size_t len = 0;
            while (p[len] && p[len] != ',') len++;
            while (len > 0 && isspace((unsigned char)start[len - 1])) len--;
            if (len >= 64) len = 63;
            memcpy(tokens[count], start, len);
            tokens[count][len] = '\0';
            count++;
            p = start + len;
            while (*p && *p != ',') p++;
        }
    }
    return count;
}

static int score_step_match(const FamilyHint *hint, const StepSummary *step) {
    char tokens[16][64];
    int token_count = 0;
    int score = 0;

    if (!hint) return 0;
    token_count = tokenize_csv(hint->commands, tokens, 16);
    for (int i = 0; i < token_count; i++) {
        if (contains_ci(step->id, tokens[i])) score += 3;
        if (contains_ci(step->name, tokens[i])) score += 3;
        if (contains_ci(step->operator, tokens[i])) score += 2;
    }

    if (contains_ci(hint->attach_to, "step") && (step->id[0] || step->name[0])) score += 1;
    if (contains_ci(hint->attach_to, "model") && contains_ci(step->operator, "model")) score += 2;
    if (contains_ci(hint->attach_to, "layer") &&
        (contains_ci(step->operator, "layer") || contains_ci(step->operator, "sli") || contains_ci(step->operator, "fpq")))
        score += 2;

    return score;
}

static void sort_matches(StepMatch *matches, int count) {
    for (int i = 0; i < count - 1; i++) {
        for (int j = i + 1; j < count; j++) {
            if (matches[j].score > matches[i].score) {
                StepMatch tmp = matches[i];
                matches[i] = matches[j];
                matches[j] = tmp;
            }
        }
    }
}

static void __attribute__((unused)) print_family_workflow_matches(const FamilyHint *hint) {
    WorkflowSummary workflows[MAX_WORKFLOWS];
    StepMatch matches[MAX_MATCHES];
    char root[MAX_PATH_LEN];
    int workflow_count;
    int match_count = 0;

    if (!hint) return;
    if (!find_workflow_dir(root, sizeof(root))) return;
    workflow_count = load_workflows(root, workflows, MAX_WORKFLOWS);
    if (workflow_count <= 0) return;

    for (int i = 0; i < workflow_count; i++) {
        size_t size = 0;
        char *json = read_file(workflows[i].path, &size);
        const char *stages_start = NULL;
        const char *stages_end = NULL;
        const char *cursor = NULL;

        if (!json || size == 0) {
            free(json);
            continue;
        }
        if (!json_find_array_span(json, "stages", &stages_start, &stages_end)) {
            free(json);
            continue;
        }

        while (1) {
            const char *obj_start;
            const char *obj_end;
            StepSummary step;
            int score;

            if (!json_next_object_in_array(stages_start, stages_end, &cursor, &obj_start, &obj_end))
                break;

            load_step_summary(obj_start, obj_end, &step);
            score = score_step_match(hint, &step);
            if (score <= 0) continue;

            if (match_count < MAX_MATCHES) {
                snprintf(matches[match_count].workflow_id, sizeof(matches[match_count].workflow_id),
                         "%s", workflows[i].id[0] ? workflows[i].id : "(path)");
                snprintf(matches[match_count].step_id, sizeof(matches[match_count].step_id),
                         "%s", step.id[0] ? step.id : "-");
                snprintf(matches[match_count].step_name, sizeof(matches[match_count].step_name),
                         "%s", step.name[0] ? step.name : "(unnamed step)");
                snprintf(matches[match_count].operator, sizeof(matches[match_count].operator),
                         "%s", step.operator[0] ? step.operator : "(operator not declared)");
                matches[match_count].score = score;
                match_count++;
            } else {
                int worst = 0;
                for (int j = 1; j < MAX_MATCHES; j++) {
                    if (matches[j].score < matches[worst].score) worst = j;
                }
                if (score > matches[worst].score) {
                    snprintf(matches[worst].workflow_id, sizeof(matches[worst].workflow_id),
                             "%s", workflows[i].id[0] ? workflows[i].id : "(path)");
                    snprintf(matches[worst].step_id, sizeof(matches[worst].step_id),
                             "%s", step.id[0] ? step.id : "-");
                    snprintf(matches[worst].step_name, sizeof(matches[worst].step_name),
                             "%s", step.name[0] ? step.name : "(unnamed step)");
                    snprintf(matches[worst].operator, sizeof(matches[worst].operator),
                             "%s", step.operator[0] ? step.operator : "(operator not declared)");
                    matches[worst].score = score;
                }
            }
        }
        free(json);
    }

    if (match_count == 0) return;
    sort_matches(matches, match_count);

    printf("\nSuggested Workflow Steps\n");
    for (int i = 0; i < match_count; i++) {
        printf("  - %s %s  %s  (%s)\n",
               matches[i].workflow_id,
               matches[i].step_id,
               matches[i].step_name,
               matches[i].operator);
    }

    printf("\nTry\n");
    for (int i = 0; i < match_count && i < 3; i++) {
        printf("  bonfyre workflow step %s %s\n",
               matches[i].workflow_id,
               matches[i].step_id);
    }
}

static void print_curated_workflow_examples(const FamilyHint *hint) {
    char *copy;
    char *line;
    char *save = NULL;
    if (!hint || !hint->workflow_examples || !hint->workflow_examples[0]) return;

    copy = strdup(hint->workflow_examples);
    if (!copy) return;

    printf("\nSuggested Workflow Steps\n");
    for (line = strtok_r(copy, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
        char *field_save = NULL;
        char *workflow = strtok_r(line, "|", &field_save);
        char *step = strtok_r(NULL, "|", &field_save);
        char *name = strtok_r(NULL, "|", &field_save);
        char *op = strtok_r(NULL, "|", &field_save);
        if (!workflow || !step || !name || !op) continue;
        printf("  - %s %s  %s  (%s)\n", workflow, step, name, op);
    }

    printf("\nTry\n");
    snprintf(copy, strlen(hint->workflow_examples) + 1, "%s", hint->workflow_examples);
    save = NULL;
    for (line = strtok_r(copy, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
        char *field_save = NULL;
        char *workflow = strtok_r(line, "|", &field_save);
        char *step = strtok_r(NULL, "|", &field_save);
        if (!workflow || !step) continue;
        printf("  bonfyre workflow step %s %s\n", workflow, step);
    }

    free(copy);
}

static int print_catalog_related_families(const char *index_path, const char *family_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char family_node_id[160];
    int shown = 0;

    if (!index_path || !family_id) return 0;
    if (bf_sqlite3_open_ro(index_path, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT fn.external_id "
        "FROM catalog_edges e "
        "JOIN catalog_nodes fn ON fn.node_id = e.dst_node_id "
        "WHERE e.src_node_id = ? AND e.rel = 'related_family' "
        "ORDER BY fn.external_id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }

    snprintf(family_node_id, sizeof(family_node_id), "family:%s", family_id);
    sqlite3_bind_text(st, 1, family_node_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        printf("  - %s\n", id ? id : "");
        shown++;
    }

    sqlite3_finalize(st);
    sqlite3_close(db);
    return shown;
}

static int print_catalog_workflow_examples(const char *index_path, const char *family_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char family_node_id[160];
    int shown = 0;

    if (!index_path || !family_id) return 0;
    if (bf_sqlite3_open_ro(index_path, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT ws.external_id, ws.name, ws.category "
        "FROM catalog_edges e "
        "JOIN catalog_nodes ws ON ws.node_id = e.dst_node_id "
        "WHERE e.src_node_id = ? AND e.rel = 'investigate_in_workflow_step' "
        "ORDER BY ws.external_id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }

    snprintf(family_node_id, sizeof(family_node_id), "family:%s", family_id);
    sqlite3_bind_text(st, 1, family_node_id, -1, SQLITE_STATIC);

    printf("\nSuggested Workflow Steps\n");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *external_id = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *operator = (const char *)sqlite3_column_text(st, 2);
        char workflow_id[64];
        char step_id[64];
        const char *colon;

        workflow_id[0] = '\0';
        step_id[0] = '\0';
        colon = external_id ? strchr(external_id, ':') : NULL;
        if (colon) {
            size_t wlen = (size_t)(colon - external_id);
            size_t slen = strlen(colon + 1);
            if (wlen >= sizeof(workflow_id)) wlen = sizeof(workflow_id) - 1;
            if (slen >= sizeof(step_id)) slen = sizeof(step_id) - 1;
            memcpy(workflow_id, external_id, wlen);
            workflow_id[wlen] = '\0';
            memcpy(step_id, colon + 1, slen);
            step_id[slen] = '\0';
        } else {
            snprintf(workflow_id, sizeof(workflow_id), "%s", external_id ? external_id : "");
        }

        printf("  - %s %s  %s  (%s)\n",
               workflow_id[0] ? workflow_id : "?",
               step_id[0] ? step_id : "--",
               name ? name : "(unnamed step)",
               operator ? operator : "(operator not declared)");
        shown++;
    }

    if (shown > 0) {
        sqlite3_reset(st);
        sqlite3_clear_bindings(st);
        sqlite3_bind_text(st, 1, family_node_id, -1, SQLITE_STATIC);
        printf("\nTry\n");
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *external_id = (const char *)sqlite3_column_text(st, 0);
            const char *colon = external_id ? strchr(external_id, ':') : NULL;
            if (!colon) continue;
            printf("  bonfyre workflow step %.*s %s\n",
                   (int)(colon - external_id),
                   external_id,
                   colon + 1);
        }
    } else {
        printf("  - none mapped yet\n");
    }

    sqlite3_finalize(st);
    sqlite3_close(db);
    return shown;
}

static int print_catalog_models_for_family(const char *index_path, const char *family_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char family_node_id[160];
    int shown = 0;

    if (!index_path || !family_id) return 0;
    if (bf_sqlite3_open_ro(index_path, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT m.external_id, m.name "
        "FROM catalog_edges e "
        "JOIN catalog_nodes m ON m.node_id = e.dst_node_id "
        "WHERE e.src_node_id = ? AND e.rel = 'has_model' "
        "ORDER BY m.external_id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    snprintf(family_node_id, sizeof(family_node_id), "family:%s", family_id);
    sqlite3_bind_text(st, 1, family_node_id, -1, SQLITE_STATIC);
    printf("\nRelated Models\n");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        printf("  - %s  %s\n", id ? id : "-", name ? name : "(unnamed model)");
        shown++;
    }
    if (shown == 0) printf("  - none indexed yet\n");
    sqlite3_finalize(st);
    sqlite3_close(db);
    return shown;
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

static const FamilyHint *find_hint(const char *id) {
    for (const FamilyHint *hint = FAMILY_HINTS; hint->id; hint++) {
        if (strcmp(hint->id, id) == 0) return hint;
    }
    return NULL;
}

static const char *category_display_name(const char *key) {
    if (strcmp(key, "transform_families") == 0) return "Transform Families";
    if (strcmp(key, "model_structure_families") == 0) return "Model Structure Families";
    if (strcmp(key, "hybrid_families") == 0) return "Hybrid Families";
    if (strcmp(key, "ttt_families") == 0) return "TTT Families";
    if (strcmp(key, "quant_pack_families") == 0) return "Quant / Pack Families";
    if (strcmp(key, "pipeline_families") == 0) return "Pipeline Families";
    if (strcmp(key, "control_families") == 0) return "Control Families";
    if (strcmp(key, "meta_recipe_families") == 0) return "Meta Families";
    return "Families";
}

static const char *default_meaning_for_category(const char *key) {
    if (strcmp(key, "transform_families") == 0)
        return "Step-local transform specialization for how text or artifacts are normalized, packed, marked, or remapped.";
    if (strcmp(key, "model_structure_families") == 0)
        return "Model or layer structural specialization for how the stack is wired, shared, or made recurrent.";
    if (strcmp(key, "hybrid_families") == 0)
        return "Hybrid retrieval or symbolic sidecar specialization that blends more than one inference or memory mode.";
    if (strcmp(key, "ttt_families") == 0)
        return "Adaptation and tuning-time specialization used to steer how a model is tuned or conditioned.";
    if (strcmp(key, "quant_pack_families") == 0)
        return "Quantization and packing specialization for model compression, layout, and deployable artifacts.";
    if (strcmp(key, "pipeline_families") == 0)
        return "Pipeline-shape specialization that changes how steps are arranged or tuned across an execution graph.";
    if (strcmp(key, "control_families") == 0)
        return "Control-plane specialization for gating, replay, latency, healing, or cost-aware orchestration.";
    if (strcmp(key, "meta_recipe_families") == 0)
        return "Meta-level tooling families for generating, evaluating, or evolving system patterns.";
    return "Bonfyre specialization family.";
}

static const char *default_attach_for_category(const char *key) {
    if (strcmp(key, "transform_families") == 0) return "workflow step / recipe step";
    if (strcmp(key, "model_structure_families") == 0) return "model manifest / layer spec";
    if (strcmp(key, "hybrid_families") == 0) return "model manifest / route policy / recipe step";
    if (strcmp(key, "ttt_families") == 0) return "model manifest / training program";
    if (strcmp(key, "quant_pack_families") == 0) return "model manifest / packed artifact / layer spec";
    if (strcmp(key, "pipeline_families") == 0) return "recipe template / workflow profile / orchestration plan";
    if (strcmp(key, "control_families") == 0) return "runtime policy / orchestration plan";
    if (strcmp(key, "meta_recipe_families") == 0) return "tooling / generator / evaluator";
    return "registry metadata";
}

static const char *default_fields_for_category(const char *key) {
    if (strcmp(key, "transform_families") == 0) return "variant, capabilities, params";
    if (strcmp(key, "model_structure_families") == 0) return "model_ref, layer_refs";
    if (strcmp(key, "hybrid_families") == 0) return "capabilities, model_ref, params";
    if (strcmp(key, "ttt_families") == 0) return "model_ref, params";
    if (strcmp(key, "quant_pack_families") == 0) return "model_ref, layer_refs, packed artifact metadata";
    if (strcmp(key, "pipeline_families") == 0) return "recipe/workflow metadata";
    if (strcmp(key, "control_families") == 0) return "runtime policy metadata";
    if (strcmp(key, "meta_recipe_families") == 0) return "tooling metadata";
    return "-";
}

static const char *default_commands_for_category(const char *key) {
    if (strcmp(key, "transform_families") == 0) return "workflow, recipe, run, clean, paragraph, canon";
    if (strcmp(key, "model_structure_families") == 0) return "model, layer, sli, fpq, fpqx";
    if (strcmp(key, "hybrid_families") == 0) return "model, capabilities, workflow step";
    if (strcmp(key, "ttt_families") == 0) return "model, learn";
    if (strcmp(key, "quant_pack_families") == 0) return "model, fpq, fpqx, layer";
    if (strcmp(key, "pipeline_families") == 0) return "workflow, recipe, runtime";
    if (strcmp(key, "control_families") == 0) return "runtime, control, orchestrate";
    if (strcmp(key, "meta_recipe_families") == 0) return "family, capability, model";
    return "family";
}

static int __attribute__((unused)) extract_array_items(const char *json, const char *key,
                               FamilySummary *items, int start_idx, int max_items) {
    char needle[128];
    const char *p;
    int count = start_idx;

    snprintf(needle, sizeof(needle), "\"%s\"", key);
    p = strstr(json, needle);
    if (!p) return count;
    p = strchr(p, '[');
    if (!p) return count;
    p++;

    while (*p && *p != ']' && count < max_items) {
        while (*p && *p != '"' && *p != ']') p++;
        if (*p == ']') break;
        if (*p != '"') break;
        p++;

        {
            const char *start = p;
            size_t len = 0;
            while (*p && *p != '"') {
                p++;
                len++;
            }
            if (*p != '"') break;
            if (len >= sizeof(items[count].id)) len = sizeof(items[count].id) - 1;
            memcpy(items[count].id, start, len);
            items[count].id[len] = '\0';
            snprintf(items[count].category_key, sizeof(items[count].category_key), "%s", key);
            snprintf(items[count].category_name, sizeof(items[count].category_name), "%s",
                     category_display_name(key));
            count++;
            p++;
        }
    }

    return count;
}

static int cmp_family_summary(const void *lhs, const void *rhs) {
    const FamilySummary *a = lhs;
    const FamilySummary *b = rhs;
    int cat_cmp = strcmp(a->category_name, b->category_name);
    if (cat_cmp != 0) return cat_cmp;
    return strcmp(a->id, b->id);
}

static int load_families(const char *index_path, FamilySummary *items, int max_items) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int count = 0;
    (void)bf_catalog_sync_default(index_path);
    if (bf_sqlite3_open_ro(index_path, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT external_id, category FROM catalog_nodes WHERE kind='family' ORDER BY category, external_id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    while (sqlite3_step(st) == SQLITE_ROW && count < max_items) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *category = (const char *)sqlite3_column_text(st, 1);
        snprintf(items[count].id, sizeof(items[count].id), "%s", id ? id : "");
        snprintf(items[count].category_name, sizeof(items[count].category_name), "%s", category ? category : "Families");
        if (strcmp(items[count].category_name, "Transform Families") == 0)
            snprintf(items[count].category_key, sizeof(items[count].category_key), "%s", "transform_families");
        else if (strcmp(items[count].category_name, "Model Structure Families") == 0)
            snprintf(items[count].category_key, sizeof(items[count].category_key), "%s", "model_structure_families");
        else if (strcmp(items[count].category_name, "Hybrid Families") == 0)
            snprintf(items[count].category_key, sizeof(items[count].category_key), "%s", "hybrid_families");
        else if (strcmp(items[count].category_name, "TTT Families") == 0)
            snprintf(items[count].category_key, sizeof(items[count].category_key), "%s", "ttt_families");
        else if (strcmp(items[count].category_name, "Quant / Pack Families") == 0)
            snprintf(items[count].category_key, sizeof(items[count].category_key), "%s", "quant_pack_families");
        else if (strcmp(items[count].category_name, "Pipeline Families") == 0)
            snprintf(items[count].category_key, sizeof(items[count].category_key), "%s", "pipeline_families");
        else if (strcmp(items[count].category_name, "Control Families") == 0)
            snprintf(items[count].category_key, sizeof(items[count].category_key), "%s", "control_families");
        else if (strcmp(items[count].category_name, "Meta Families") == 0)
            snprintf(items[count].category_key, sizeof(items[count].category_key), "%s", "meta_recipe_families");
        count++;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    qsort(items, (size_t)count, sizeof(items[0]), cmp_family_summary);
    return count;
}

static int family_matches(const FamilySummary *fam, const char *query) {
    const FamilyHint *hint = find_hint(fam->id);
    return contains_ci(fam->id, query)
        || contains_ci(fam->category_name, query)
        || (hint && contains_ci(hint->meaning, query))
        || (hint && contains_ci(hint->attach_to, query))
        || (hint && contains_ci(hint->commands, query));
}

static const FamilySummary *find_family(const FamilySummary *items, int count, const char *id) {
    for (int i = 0; i < count; i++) {
        if (strcmp(items[i].id, id) == 0) return &items[i];
    }
    return NULL;
}

static void print_help(void) {
    printf(
        "bonfyre-family - conceptual family browser\n\n"
        "Families are Bonfyre's specialization taxonomy. They are not executable\n"
        "recipes or standalone workflows; they describe how steps, models, layers,\n"
        "and runtime policies can be specialized.\n\n"
        "Usage:\n"
        "  bonfyre-family list [term] [--json] [--compact]\n"
        "  bonfyre-family show <family>\n"
        "  bonfyre-family explain <family>\n"
        "  bonfyre-family related <family>\n"
        "  bonfyre-family where <family>\n"
        "  bonfyre-family help\n\n"
        "Examples:\n"
        "  bonfyre family list\n"
        "  bonfyre family list recur\n"
        "  bonfyre family show T_CASEOPS\n"
        "  bonfyre family explain T_SHARED_QK\n"
        "  bonfyre family where T_PARALLEL_RESIDUAL\n\n"
        "Related:\n"
        "  bonfyre workflow step A3 s02\n"
        "  bonfyre capabilities help\n"
        "  bonfyre model --help\n"
        "  bonfyre layer --help\n"
    );
}

static void cmd_list_text(const FamilySummary *items, int count, const char *query) {
    char last_category[64] = "";
    int shown = 0;

    printf("bonfyre-family  conceptual taxonomy\n");
    if (query && query[0]) printf("filter          %s\n", query);
    printf("note            Families are attachment classes, not recipes.\n\n");

    for (int i = 0; i < count; i++) {
        if (!family_matches(&items[i], query)) continue;
        if (strcmp(last_category, items[i].category_name) != 0) {
            if (shown > 0) printf("\n");
            printf("%s\n", items[i].category_name);
            snprintf(last_category, sizeof(last_category), "%s", items[i].category_name);
        }

        const FamilyHint *hint = find_hint(items[i].id);
        printf("  %-24s  %s\n",
               items[i].id,
               hint ? hint->attach_to : default_attach_for_category(items[i].category_key));
        shown++;
    }

    if (shown == 0)
        printf("No families matched that filter.\n");
}

static void cmd_list_compact(const FamilySummary *items, int count, const char *query) {
    int shown = 0;
    printf("bonfyre-family  compact\n");
    if (query && query[0]) printf("filter          %s\n", query);
    for (int i = 0; i < count; i++) {
        if (!family_matches(&items[i], query)) continue;
        printf("%s [%s]\n", items[i].id, items[i].category_name);
        shown++;
    }
    if (shown == 0) printf("No families matched that filter.\n");
}

static void cmd_list_json(const FamilySummary *items, int count, const char *query, const char *index_path) {
    int shown = 0;
    printf("{\n  \"index\": ");
    json_escape(stdout, index_path);
    printf(",\n  \"families\": [\n");
    for (int i = 0; i < count; i++) {
        const FamilyHint *hint;
        if (!family_matches(&items[i], query)) continue;
        hint = find_hint(items[i].id);
        if (shown > 0) printf(",\n");
        printf("    {\"id\": ");
        json_escape(stdout, items[i].id);
        printf(", \"category\": ");
        json_escape(stdout, items[i].category_name);
        printf(", \"attach_to\": ");
        json_escape(stdout, hint ? hint->attach_to : default_attach_for_category(items[i].category_key));
        printf("}");
        shown++;
    }
    printf("\n  ]\n}\n");
}

static void print_family_record(const FamilySummary *fam, const char *index_path) {
    const FamilyHint *hint = find_hint(fam->id);

    printf("family      %s\n", fam->id);
    printf("category    %s\n", fam->category_name);
    printf("source      %s\n", index_path);
    printf("kind        conceptual_family\n");
    printf("meaning     %s\n", hint ? hint->meaning : default_meaning_for_category(fam->category_key));
    printf("attach_to   %s\n", hint ? hint->attach_to : default_attach_for_category(fam->category_key));
    printf("fields      %s\n", hint ? hint->fields : default_fields_for_category(fam->category_key));
    printf("commands    %s\n", hint ? hint->commands : default_commands_for_category(fam->category_key));
    printf("related     %s\n", hint ? hint->related : "same-category neighbors");
}

static int cmd_show_like(const FamilySummary *items, int count, const char *id,
                         const char *index_path, const char *mode) {
    const FamilySummary *fam = find_family(items, count, id);
    if (!fam) {
        fprintf(stderr, "bonfyre-family: unknown family '%s'\n", id);
        fprintf(stderr, "Run 'bonfyre family list' to inspect available families.\n");
        return 1;
    }

    if (strcmp(mode, "show") == 0) {
        print_family_record(fam, index_path);
        return 0;
    }

    if (strcmp(mode, "explain") == 0) {
        const FamilyHint *hint = find_hint(fam->id);
        printf("family      %s\n", fam->id);
        printf("category    %s\n", fam->category_name);
        printf("explanation %s\n", hint ? hint->meaning : default_meaning_for_category(fam->category_key));
        printf("note        This family is not a recipe or workflow. It describes how Bonfyre is specialized.\n");
        return 0;
    }

    if (strcmp(mode, "related") == 0) {
        printf("family      %s\n", fam->id);
        printf("category    %s\n", fam->category_name);
        printf("related     %s\n", find_hint(fam->id) ? find_hint(fam->id)->related : "same-category neighbors");
        printf("\nNeighbors\n");
        if (print_catalog_related_families(index_path, fam->id) == 0) {
            for (int i = 0; i < count; i++) {
                if (strcmp(items[i].category_key, fam->category_key) != 0) continue;
                if (strcmp(items[i].id, fam->id) == 0) continue;
                printf("  - %s\n", items[i].id);
            }
        }
        return 0;
    }

    if (strcmp(mode, "where") == 0) {
        const FamilyHint *hint = find_hint(fam->id);
        printf("family      %s\n", fam->id);
        printf("attach_to   %s\n", hint ? hint->attach_to : default_attach_for_category(fam->category_key));
        printf("fields      %s\n", hint ? hint->fields : default_fields_for_category(fam->category_key));
        printf("commands    %s\n", hint ? hint->commands : default_commands_for_category(fam->category_key));
        printf("note        Investigate with workflow step bindings, model records, and layer surfaces before pipeline resolution.\n");
        print_catalog_models_for_family(index_path, fam->id);
        if (print_catalog_workflow_examples(index_path, fam->id) == 0)
            print_curated_workflow_examples(hint);
        return 0;
    }

    return 1;
}

int main(int argc, char *argv[]) {
    FamilySummary items[MAX_FAMILIES];
    ListOptions opts;
    char index_path[MAX_PATH_LEN];
    int count;

    bf_catalog_default_db_path(index_path, sizeof(index_path));

    count = load_families(index_path, items, MAX_FAMILIES);
    if (count <= 0) {
        fprintf(stderr, "bonfyre-family: failed to load family inventory from catalog %s\n", index_path);
        return 1;
    }

    if (argc < 2 || strcmp(argv[1], "help") == 0 || strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0) {
        print_help();
        return 0;
    }

    if (strcmp(argv[1], "list") == 0) {
        opts = parse_list_options(argc, argv);
        if (opts.json) cmd_list_json(items, count, opts.query[0] ? opts.query : NULL, index_path);
        else if (opts.compact) cmd_list_compact(items, count, opts.query[0] ? opts.query : NULL);
        else cmd_list_text(items, count, opts.query[0] ? opts.query : NULL);
        return 0;
    }

    if ((strcmp(argv[1], "show") == 0 || strcmp(argv[1], "explain") == 0
         || strcmp(argv[1], "related") == 0 || strcmp(argv[1], "where") == 0) && argc < 3) {
        fprintf(stderr, "bonfyre-family: %s requires a family id\n", argv[1]);
        return 1;
    }

    if (strcmp(argv[1], "show") == 0) return cmd_show_like(items, count, argv[2], index_path, "show");
    if (strcmp(argv[1], "explain") == 0) return cmd_show_like(items, count, argv[2], index_path, "explain");
    if (strcmp(argv[1], "related") == 0) return cmd_show_like(items, count, argv[2], index_path, "related");
    if (strcmp(argv[1], "where") == 0) return cmd_show_like(items, count, argv[2], index_path, "where");

    fprintf(stderr, "bonfyre-family: unknown command '%s'\n", argv[1]);
    fprintf(stderr, "Run 'bonfyre family help' for usage.\n");
    return 1;
}
