/*
 * BonfyreRun — Recipe Executor
 *
 * Executes pipeline recipes from the registry with:
 * - DAG dependency resolution (topological sort)
 * - Level-parallel execution (up to 8 stages per level)
 * - Variable substitution ({input}, {out}, {input_repo})
 * - Run manifest generation
 *
 * Usage:
 *   bonfyre-run <RECIPE_ID> --input <FILE> --out <DIR> [OPTIONS]
 *   bonfyre-run history [RECIPE_ID]
 *   bonfyre-run show <RUN_ID>
 *
 * Options:
 *   --dry-run           Show execution plan without running
 *   --resume            Resume from last failed stage (TODO)
 *   --from-stage ID     Start from specific stage (TODO)
 *   --to-stage ID       Stop at specific stage (TODO)
 *   --tier local        Execution tier (TODO)
 *   --batch             Batch mode (TODO)
 *   --db PATH           Custom recipe database path
 *   --input-repo PATH   Optional repository input for composed recipes
 */

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define MAX_STAGES 64
#define MAX_ARGS 128
#define MAX_DEPS 16
#define MAX_SKIP_VARS 8
#define MAX_PATH 4096
#define MAX_PARALLEL 8

typedef struct {
    char id[64];
    char name[256];
    char operator[256];
    char *args[MAX_ARGS];
    int argc;
    char *inputs[MAX_ARGS];
    int input_count;
    char *outputs[MAX_ARGS];
    int output_count;
    int parallel;
    int depends_on_count;
    char depends_on[MAX_DEPS][64];
    int skip_if_null_count;
    char skip_if_null[MAX_SKIP_VARS][64];

    int level;
    int executed;
    int skipped;
    int exit_code;
    pid_t pid;
    time_t started_at;
    time_t completed_at;
} Stage;

typedef struct {
    char recipe_id[64];
    char name[256];
    char version[32];
    char hash[128];

    char input_path[MAX_PATH];
    char input_repo[MAX_PATH];
    char output_dir[MAX_PATH];

    Stage stages[MAX_STAGES];
    int stage_count;
    int max_level;

    time_t started_at;
    time_t completed_at;
    char status[32];
} ExecutionContext;

static char *xstrdup(const char *s) {
    if (!s) return NULL;
    size_t len = strlen(s) + 1;
    char *copy = malloc(len);
    if (!copy) return NULL;
    memcpy(copy, s, len);
    return copy;
}

static char *get_db_path(const char *custom_path) {
    static char path[MAX_PATH];
    if (custom_path) {
        snprintf(path, sizeof(path), "%s", custom_path);
        return path;
    }
    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    snprintf(path, sizeof(path), "%s/.bonfyre/recipes.db", home);
    return path;
}

static int ensure_dir(const char *path) {
    char tmp[MAX_PATH];

    snprintf(tmp, sizeof(tmp), "%s", path);

    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
            *p = '/';
        }
    }

    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
    return 0;
}

static const char *skip_ws(const char *p, const char *limit) {
    while (p < limit && isspace((unsigned char)*p)) p++;
    return p;
}

static const char *find_matching(const char *start, const char *limit,
                                 char open_ch, char close_ch) {
    int depth = 0;
    int in_string = 0;
    int escape = 0;

    for (const char *p = start; p < limit; p++) {
        char c = *p;

        if (in_string) {
            if (escape) {
                escape = 0;
            } else if (c == '\\') {
                escape = 1;
            } else if (c == '"') {
                in_string = 0;
            }
            continue;
        }

        if (c == '"') {
            in_string = 1;
            continue;
        }

        if (c == open_ch) {
            depth++;
        } else if (c == close_ch) {
            depth--;
            if (depth == 0) return p;
        }
    }

    return NULL;
}

static const char *find_key_value(const char *json, const char *limit,
                                  const char *key) {
    char pattern[128];
    snprintf(pattern, sizeof(pattern), "\"%s\"", key);

    const char *p = json;
    while (p < limit) {
        const char *match = strstr(p, pattern);
        if (!match || match >= limit) return NULL;

        const char *cursor = match + strlen(pattern);
        cursor = skip_ws(cursor, limit);
        if (cursor >= limit || *cursor != ':') {
            p = match + 1;
            continue;
        }

        cursor++;
        return skip_ws(cursor, limit);
    }

    return NULL;
}

static char *dup_json_string_value(const char *value, const char *limit) {
    if (!value || value >= limit || *value != '"') return NULL;

    size_t capacity = 128;
    size_t len = 0;
    char *out = malloc(capacity);
    if (!out) return NULL;

    const char *p = value + 1;
    int escape = 0;

    while (p < limit) {
        char c = *p++;
        if (escape) {
            switch (c) {
                case 'n': c = '\n'; break;
                case 'r': c = '\r'; break;
                case 't': c = '\t'; break;
                case '"': break;
                case '\\': break;
                default: break;
            }
            escape = 0;
        } else if (c == '\\') {
            escape = 1;
            continue;
        } else if (c == '"') {
            out[len] = '\0';
            return out;
        }

        if (len + 2 > capacity) {
            capacity *= 2;
            char *grown = realloc(out, capacity);
            if (!grown) {
                free(out);
                return NULL;
            }
            out = grown;
        }
        out[len++] = c;
    }

    free(out);
    return NULL;
}

static int extract_string_field(const char *json, const char *limit,
                                const char *key, char *out, size_t out_size) {
    const char *value = find_key_value(json, limit, key);
    if (!value) return 0;

    char *dup = dup_json_string_value(value, limit);
    if (!dup) return 0;

    snprintf(out, out_size, "%s", dup);
    free(dup);
    return 1;
}

static int extract_int_field(const char *json, const char *limit,
                             const char *key, int default_value) {
    const char *value = find_key_value(json, limit, key);
    if (!value) return default_value;
    return (int)strtol(value, NULL, 10);
}

static int extract_string_array_field(const char *json, const char *limit,
                                      const char *key, char **out,
                                      int max_items) {
    const char *value = find_key_value(json, limit, key);
    if (!value || value >= limit || *value != '[') return 0;

    const char *array_end = find_matching(value, limit, '[', ']');
    if (!array_end) return 0;

    int count = 0;
    const char *p = value + 1;

    while (p < array_end && count < max_items) {
        p = skip_ws(p, array_end);
        if (p >= array_end) break;
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p != '"') break;

        char *item = dup_json_string_value(p, array_end);
        if (!item) break;
        out[count++] = item;

        p++;
        int escape = 0;
        while (p < array_end) {
            if (escape) {
                escape = 0;
            } else if (*p == '\\') {
                escape = 1;
            } else if (*p == '"') {
                p++;
                break;
            }
            p++;
        }
    }

    return count;
}

static int extract_small_string_array_field(const char *json, const char *limit,
                                            const char *key,
                                            char out[][64], int max_items) {
    char *items[MAX_ARGS] = {0};
    int count = extract_string_array_field(json, limit, key, items, max_items);
    for (int i = 0; i < count; i++) {
        snprintf(out[i], 64, "%s", items[i]);
        free(items[i]);
    }
    return count;
}

static void free_stage(Stage *stage) {
    for (int i = 0; i < stage->argc; i++) {
        free(stage->args[i]);
        stage->args[i] = NULL;
    }
    for (int i = 0; i < stage->input_count; i++) {
        free(stage->inputs[i]);
        stage->inputs[i] = NULL;
    }
    for (int i = 0; i < stage->output_count; i++) {
        free(stage->outputs[i]);
        stage->outputs[i] = NULL;
    }
}

static void free_context(ExecutionContext *ctx) {
    for (int i = 0; i < ctx->stage_count; i++) free_stage(&ctx->stages[i]);
}

static void substitute_vars(const char *template, char *output, size_t output_size,
                            const ExecutionContext *ctx) {
    const char *p = template;
    char *out = output;
    size_t remaining = output_size - 1;

    while (*p && remaining > 0) {
        const char *replacement = NULL;

        if (strncmp(p, "{input}", 7) == 0) {
            replacement = ctx->input_path;
            p += 7;
        } else if (strncmp(p, "{out}", 5) == 0) {
            replacement = ctx->output_dir;
            p += 5;
        } else if (strncmp(p, "{input_repo}", 12) == 0) {
            replacement = ctx->input_repo;
            p += 12;
        }

        if (replacement) {
            int written = snprintf(out, remaining + 1, "%s", replacement);
            if (written < 0) break;
            if ((size_t)written > remaining) written = (int)remaining;
            out += written;
            remaining -= (size_t)written;
            continue;
        }

        *out++ = *p++;
        remaining--;
    }

    *out = '\0';
}

static void camel_to_kebab(const char *input, char *output, size_t output_size) {
    size_t j = 0;
    for (size_t i = 0; input[i] != '\0' && j + 1 < output_size; i++) {
        unsigned char c = (unsigned char)input[i];
        if (isupper(c)) {
            if (i > 0 && j + 2 < output_size) output[j++] = '-';
            output[j++] = (char)tolower(c);
        } else {
            output[j++] = (char)c;
        }
    }
    output[j] = '\0';
}

static void operator_to_compact(const char *operator_name,
                                char *output, size_t output_size) {
    if (strncmp(operator_name, "Bonfyre", 7) == 0 && operator_name[7] != '\0') {
        snprintf(output, output_size, "bonfyre-");
        size_t used = strlen(output);
        for (const char *p = operator_name + 7; *p && used + 1 < output_size; p++) {
            output[used++] = (char)tolower((unsigned char)*p);
        }
        output[used] = '\0';
        return;
    }
    camel_to_kebab(operator_name, output, output_size);
}

static int file_exists_executable(const char *path) {
    return access(path, X_OK) == 0;
}

static int try_operator_candidate(const char *command,
                                  const char *operator_name,
                                  char *resolved, size_t resolved_size) {
    if (strchr(command, '/')) {
        snprintf(resolved, resolved_size, "%s", command);
        return file_exists_executable(resolved) ? 0 : -1;
    }

    const char *path_env = getenv("PATH");
    if (path_env) {
        char *paths = xstrdup(path_env);
        if (!paths) return -1;

        char *saveptr = NULL;
        for (char *dir = strtok_r(paths, ":", &saveptr);
             dir;
             dir = strtok_r(NULL, ":", &saveptr)) {
            char candidate[MAX_PATH];
            snprintf(candidate, sizeof(candidate), "%s/%s", dir, command);
            if (file_exists_executable(candidate)) {
                snprintf(resolved, resolved_size, "%s", candidate);
                free(paths);
                return 0;
            }
        }
        free(paths);
    }

    char cwd[MAX_PATH];
    if (!getcwd(cwd, sizeof(cwd))) return -1;

    char candidate[MAX_PATH];
    snprintf(candidate, sizeof(candidate), "%s/cmd/%s/build/%s", cwd, operator_name, command);
    if (file_exists_executable(candidate)) {
        snprintf(resolved, resolved_size, "%s", candidate);
        return 0;
    }

    snprintf(candidate, sizeof(candidate), "%s/cmd/%s/%s", cwd, operator_name, command);
    if (file_exists_executable(candidate)) {
        snprintf(resolved, resolved_size, "%s", candidate);
        return 0;
    }

    snprintf(candidate, sizeof(candidate), "%s/../%s/build/%s", cwd, operator_name, command);
    if (file_exists_executable(candidate)) {
        snprintf(resolved, resolved_size, "%s", candidate);
        return 0;
    }

    snprintf(candidate, sizeof(candidate), "%s/../%s/%s", cwd, operator_name, command);
    if (file_exists_executable(candidate)) {
        snprintf(resolved, resolved_size, "%s", candidate);
        return 0;
    }

    snprintf(candidate, sizeof(candidate), "%s/../../cmd/%s/build/%s", cwd, operator_name, command);
    if (file_exists_executable(candidate)) {
        snprintf(resolved, resolved_size, "%s", candidate);
        return 0;
    }

    snprintf(candidate, sizeof(candidate), "%s/../../cmd/%s/%s", cwd, operator_name, command);
    if (file_exists_executable(candidate)) {
        snprintf(resolved, resolved_size, "%s", candidate);
        return 0;
    }

    return -1;
}

static int find_operator_path(const char *operator_name,
                              char *resolved, size_t resolved_size) {
    char kebab[256];
    char compact[256];

    if (strchr(operator_name, '/')) {
        snprintf(kebab, sizeof(kebab), "%s", operator_name);
        return try_operator_candidate(kebab, operator_name, resolved, resolved_size);
    }

    camel_to_kebab(operator_name, kebab, sizeof(kebab));
    if (try_operator_candidate(kebab, operator_name, resolved, resolved_size) == 0) return 0;

    operator_to_compact(operator_name, compact, sizeof(compact));
    if (strcmp(compact, kebab) != 0 &&
        try_operator_candidate(compact, operator_name, resolved, resolved_size) == 0) {
        return 0;
    }

    return -1;
}

static int load_recipe(const char *db_path, const char *recipe_id, char **json_out) {
    char catalog_db[MAX_PATH];
    sqlite3 *catalog = NULL;
    sqlite3_stmt *catalog_stmt = NULL;

    bf_catalog_default_db_path(catalog_db, sizeof(catalog_db));
    if (bf_catalog_sync_default(catalog_db) == 0 &&
        bf_sqlite3_open_ro(catalog_db, &catalog) == SQLITE_OK &&
        sqlite3_prepare_v2(catalog,
            "SELECT json_data FROM catalog_nodes WHERE kind='recipe' AND external_id=?",
            -1, &catalog_stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(catalog_stmt, 1, recipe_id, -1, SQLITE_STATIC);
        if (sqlite3_step(catalog_stmt) == SQLITE_ROW) {
            const char *json = (const char *)sqlite3_column_text(catalog_stmt, 0);
            if (json) {
                *json_out = xstrdup(json);
                sqlite3_finalize(catalog_stmt);
                sqlite3_close(catalog);
                return *json_out ? 0 : 1;
            }
        }
    }
    if (catalog_stmt) sqlite3_finalize(catalog_stmt);
    if (catalog) sqlite3_close(catalog);

    sqlite3 *db;
    int rc = sqlite3_open(db_path, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }

    const char *sql = "SELECT json_data FROM recipes WHERE recipe_id = ?";
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }

    sqlite3_bind_text(stmt, 1, recipe_id, -1, SQLITE_STATIC);

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *json = (const char *)sqlite3_column_text(stmt, 0);
        *json_out = xstrdup(json);
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return *json_out ? 0 : -1;
    }

    fprintf(stderr, "Recipe not found: %s\n", recipe_id);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return -1;
}

static int parse_stages(const char *json, ExecutionContext *ctx) {
    const char *doc_end = json + strlen(json);
    const char *stages_value = find_key_value(json, doc_end, "stages");
    if (!stages_value) stages_value = find_key_value(json, doc_end, "steps");
    if (!stages_value || *stages_value != '[') return -1;

    const char *stages_end = find_matching(stages_value, doc_end, '[', ']');
    if (!stages_end) return -1;

    const char *p = stages_value + 1;
    int stage_idx = 0;

    while (p < stages_end && stage_idx < MAX_STAGES) {
        p = skip_ws(p, stages_end);
        if (p >= stages_end) break;
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p != '{') break;

        const char *obj_end = find_matching(p, stages_end, '{', '}');
        if (!obj_end) return -1;

        Stage *s = &ctx->stages[stage_idx];
        memset(s, 0, sizeof(*s));
        s->parallel = 1;
        s->level = -1;
        s->exit_code = -1;

        extract_string_field(p, obj_end + 1, "id", s->id, sizeof(s->id));
        extract_string_field(p, obj_end + 1, "name", s->name, sizeof(s->name));
        extract_string_field(p, obj_end + 1, "operator", s->operator, sizeof(s->operator));
        if (s->operator[0] == '\0') {
            extract_string_field(p, obj_end + 1, "uses", s->operator, sizeof(s->operator));
        }
        if (s->operator[0] == '\0') {
            extract_string_field(p, obj_end + 1, "bin", s->operator, sizeof(s->operator));
        }
        s->parallel = extract_int_field(p, obj_end + 1, "parallel", 1);
        if (s->parallel < 1) s->parallel = 1;

        s->argc = extract_string_array_field(p, obj_end + 1, "args", s->args, MAX_ARGS - 1);
        s->input_count = extract_string_array_field(p, obj_end + 1, "inputs", s->inputs, MAX_ARGS);
        s->output_count = extract_string_array_field(p, obj_end + 1, "outputs", s->outputs, MAX_ARGS);
        s->depends_on_count = extract_small_string_array_field(
            p, obj_end + 1, "depends_on", s->depends_on, MAX_DEPS);
        s->skip_if_null_count = extract_small_string_array_field(
            p, obj_end + 1, "skip_if_null", s->skip_if_null, MAX_SKIP_VARS);

        if (s->id[0] == '\0' || s->operator[0] == '\0') {
            fprintf(stderr, "Invalid stage: missing id/operator/uses/bin\n");
            return -1;
        }

        stage_idx++;
        p = obj_end + 1;
    }

    ctx->stage_count = stage_idx;
    return stage_idx > 0 ? 0 : -1;
}

static int stage_index_by_id(const ExecutionContext *ctx, const char *stage_id) {
    for (int i = 0; i < ctx->stage_count; i++) {
        if (strcmp(ctx->stages[i].id, stage_id) == 0) return i;
    }
    return -1;
}

static int assign_levels(ExecutionContext *ctx) {
    int assigned = 0;

    while (assigned < ctx->stage_count) {
        int progress = 0;

        for (int i = 0; i < ctx->stage_count; i++) {
            Stage *stage = &ctx->stages[i];
            if (stage->level >= 0) continue;

            int ready = 1;
            int level = 0;

            for (int d = 0; d < stage->depends_on_count; d++) {
                int dep_idx = stage_index_by_id(ctx, stage->depends_on[d]);
                if (dep_idx < 0) {
                    fprintf(stderr, "Unknown dependency '%s' in stage %s\n",
                            stage->depends_on[d], stage->id);
                    return -1;
                }
                if (ctx->stages[dep_idx].level < 0) {
                    ready = 0;
                    break;
                }
                if (ctx->stages[dep_idx].level + 1 > level) {
                    level = ctx->stages[dep_idx].level + 1;
                }
            }

            if (!ready) continue;

            stage->level = level;
            if (level > ctx->max_level) ctx->max_level = level;
            assigned++;
            progress = 1;
        }

        if (!progress) {
            fprintf(stderr, "Cyclic dependency detected in recipe\n");
            return -1;
        }
    }

    return 0;
}

static int should_skip_stage(const Stage *stage, const ExecutionContext *ctx) {
    for (int i = 0; i < stage->skip_if_null_count; i++) {
        const char *name = stage->skip_if_null[i];
        if (strcmp(name, "input_repo") == 0 && ctx->input_repo[0] == '\0') return 1;
        if (strcmp(name, "input") == 0 && ctx->input_path[0] == '\0') return 1;
    }
    return 0;
}

static int dependency_was_skipped(const Stage *stage, const ExecutionContext *ctx) {
    for (int i = 0; i < stage->depends_on_count; i++) {
        int dep_idx = stage_index_by_id(ctx, stage->depends_on[i]);
        if (dep_idx >= 0 && ctx->stages[dep_idx].skipped) return 1;
    }
    return 0;
}

static void print_stage_command(char *const argv[]) {
    printf("    Command:");
    for (int i = 0; argv[i]; i++) printf(" %s", argv[i]);
    printf("\n");
}

static int build_stage_argv(const Stage *stage, const ExecutionContext *ctx,
                            char *resolved_cmd, size_t resolved_size,
                            char substituted[][MAX_PATH], char *argv_out[]) {
    if (find_operator_path(stage->operator, resolved_cmd, resolved_size) != 0) {
        fprintf(stderr, "    Operator not found: %s\n", stage->operator);
        return -1;
    }

    argv_out[0] = resolved_cmd;
    for (int i = 0; i < stage->argc; i++) {
        substitute_vars(stage->args[i], substituted[i], sizeof(substituted[i]), ctx);
        argv_out[i + 1] = substituted[i];
    }
    argv_out[stage->argc + 1] = NULL;
    return 0;
}

static int spawn_stage(Stage *stage, const ExecutionContext *ctx) {
    char resolved_cmd[MAX_PATH];
    char substituted[MAX_ARGS][MAX_PATH];
    char *argv[MAX_ARGS + 2];

    printf("  [%s] %s\n", stage->id, stage->name[0] ? stage->name : stage->id);
    printf("    Operator: %s\n", stage->operator);

    if (dependency_was_skipped(stage, ctx)) {
        printf("    Status: skipped (dependency skipped)\n");
        stage->executed = 1;
        stage->skipped = 1;
        stage->exit_code = 0;
        stage->started_at = time(NULL);
        stage->completed_at = stage->started_at;
        return 1;
    }

    if (should_skip_stage(stage, ctx)) {
        printf("    Status: skipped (missing optional input)\n");
        stage->executed = 1;
        stage->skipped = 1;
        stage->exit_code = 0;
        stage->started_at = time(NULL);
        stage->completed_at = stage->started_at;
        return 1;
    }

    if (build_stage_argv(stage, ctx, resolved_cmd, sizeof(resolved_cmd),
                         substituted, argv) != 0) {
        stage->exit_code = 127;
        return -1;
    }

    print_stage_command(argv);

    stage->started_at = time(NULL);
    stage->pid = fork();
    if (stage->pid < 0) {
        perror("fork");
        stage->exit_code = errno;
        return -1;
    }

    if (stage->pid == 0) {
        execv(resolved_cmd, argv);
        perror("execv");
        _exit(127);
    }

    return 0;
}

static int finalize_stage(Stage *stage, int status) {
    stage->completed_at = time(NULL);
    stage->executed = 1;
    stage->pid = 0;

    if (WIFEXITED(status)) {
        stage->exit_code = WEXITSTATUS(status);
        return stage->exit_code == 0 ? 0 : -1;
    }
    if (WIFSIGNALED(status)) {
        stage->exit_code = 128 + WTERMSIG(status);
        return -1;
    }

    stage->exit_code = status;
    return -1;
}

static int execute_level(ExecutionContext *ctx, int level) {
    Stage *level_stages[MAX_STAGES];
    int count = 0;

    for (int i = 0; i < ctx->stage_count; i++) {
        if (ctx->stages[i].level == level && !ctx->stages[i].executed) {
            level_stages[count++] = &ctx->stages[i];
        }
    }

    if (count == 0) return 0;

    printf("\nLevel %d (%d stage%s):\n", level, count, count == 1 ? "" : "s");

    int launched = 0;
    for (int i = 0; i < count; i++) {
        int rc = spawn_stage(level_stages[i], ctx);
        if (rc < 0) return -1;
        if (rc == 0) launched++;
    }

    for (int i = 0; i < count; i++) {
        Stage *stage = level_stages[i];
        if (stage->skipped) continue;

        int status = 0;
        if (waitpid(stage->pid, &status, 0) < 0) {
            perror("waitpid");
            return -1;
        }

        if (finalize_stage(stage, status) != 0) {
            printf("    Result: failed (exit %d)\n", stage->exit_code);
            return -1;
        }

        printf("    Result: success\n");
    }

    return launched >= 0 ? 0 : -1;
}

static void write_json_string(FILE *fp, const char *s) {
    fputc('"', fp);
    for (; s && *s; s++) {
        unsigned char ch = (unsigned char)*s;
        if (ch == '"') fputs("\\\"", fp);
        else if (ch == '\\') fputs("\\\\", fp);
        else if (ch == '\n') fputs("\\n", fp);
        else if (ch == '\r') fputs("\\r", fp);
        else if (ch == '\t') fputs("\\t", fp);
        else if (ch < 0x20) fprintf(fp, "\\u%04x", ch);
        else fputc(ch, fp);
    }
    fputc('"', fp);
}

static int make_manifest_path(const ExecutionContext *ctx,
                              const char *name,
                              char *out,
                              size_t out_size) {
    if (!name || !name[0]) name = "recipe.json";
    if (strchr(name, '/')) snprintf(out, out_size, "%s", name);
    else snprintf(out, out_size, "%s/%s", ctx->output_dir, name);
    return 0;
}

static int write_manifest_file(ExecutionContext *ctx, const char *path, const char *kind) {
    char manifest_path[MAX_PATH];
    snprintf(manifest_path, sizeof(manifest_path), "%s", path);

    FILE *fp = fopen(manifest_path, "w");
    if (!fp) {
        fprintf(stderr, "Cannot write manifest: %s\n", manifest_path);
        return -1;
    }

    fprintf(fp, "{\n");
    fprintf(fp, "  \"kind\": ");
    write_json_string(fp, kind);
    fprintf(fp, ",\n");
    fprintf(fp, "  \"manifest_version\": \"1.0.0\",\n");
    fprintf(fp, "  \"recipe_id\": ");
    write_json_string(fp, ctx->recipe_id);
    fprintf(fp, ",\n");
    fprintf(fp, "  \"recipe_hash\": ");
    write_json_string(fp, ctx->hash);
    fprintf(fp, ",\n");
    fprintf(fp, "  \"started_at\": %ld,\n", ctx->started_at);
    fprintf(fp, "  \"completed_at\": %ld,\n", ctx->completed_at);
    fprintf(fp, "  \"status\": ");
    write_json_string(fp, ctx->status);
    fprintf(fp, ",\n");
    fprintf(fp, "  \"stages\": [\n");

    for (int i = 0; i < ctx->stage_count; i++) {
        Stage *s = &ctx->stages[i];
        long duration = 0;
        if (s->started_at > 0 && s->completed_at >= s->started_at) {
            duration = s->completed_at - s->started_at;
        }
        fprintf(fp, "    {\n");
        fprintf(fp, "      \"id\": ");
        write_json_string(fp, s->id);
        fprintf(fp, ",\n");
        fprintf(fp, "      \"name\": ");
        write_json_string(fp, s->name);
        fprintf(fp, ",\n");
        fprintf(fp, "      \"operator\": ");
        write_json_string(fp, s->operator);
        fprintf(fp, ",\n");
        fprintf(fp, "      \"skipped\": %s,\n", s->skipped ? "true" : "false");
        fprintf(fp, "      \"started_at\": %ld,\n", s->started_at);
        fprintf(fp, "      \"completed_at\": %ld,\n", s->completed_at);
        fprintf(fp, "      \"duration_seconds\": %ld,\n", duration);
        fprintf(fp, "      \"exit_code\": %d\n", s->exit_code);
        fprintf(fp, "    }%s\n", (i < ctx->stage_count - 1) ? "," : "");
    }

    fprintf(fp, "  ]\n");
    fprintf(fp, "}\n");

    fclose(fp);
    printf("\n✓ Manifest: %s\n", manifest_path);
    {
        char catalog_db[MAX_PATH];
        bf_catalog_default_db_path(catalog_db, sizeof(catalog_db));
        bf_catalog_record_run_manifest(catalog_db, manifest_path);
    }
    return 0;
}

static int write_manifests(ExecutionContext *ctx) {
    char manifest_path[MAX_PATH];
    make_manifest_path(ctx, "recipe.json", manifest_path, sizeof(manifest_path));
    return write_manifest_file(ctx, manifest_path, "bonfyre.recipe-run.v1");
}

static int cmd_history(const char *recipe_id_filter) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *st = NULL;
    char catalog_db[MAX_PATH];

    bf_catalog_default_db_path(catalog_db, sizeof(catalog_db));
    if (bf_catalog_sync_default(catalog_db) != 0 ||
        bf_sqlite3_open_ro(catalog_db, &catalog) != SQLITE_OK) {
        sqlite3_close(catalog);
        fprintf(stderr, "Unable to open Bonfyre catalog: %s\n", catalog_db);
        return 1;
    }

    if (recipe_id_filter && recipe_id_filter[0]) {
        sqlite3_prepare_v2(catalog,
            "SELECT external_id, name, category, json_data, source_path "
            "FROM catalog_nodes WHERE kind='run_manifest' AND name=? "
            "ORDER BY updated_at DESC",
            -1, &st, NULL);
        sqlite3_bind_text(st, 1, recipe_id_filter, -1, SQLITE_STATIC);
    } else {
        sqlite3_prepare_v2(catalog,
            "SELECT external_id, name, category, json_data, source_path "
            "FROM catalog_nodes WHERE kind='run_manifest' "
            "ORDER BY updated_at DESC",
            -1, &st, NULL);
    }

    printf("bonfyre-run history\n");
    printf("catalog  %s\n\n", catalog_db);
    printf("%-24s %-14s %-10s %-12s %s\n",
           "RUN ID", "RECIPE", "STATUS", "STARTED", "MANIFEST");

    int count = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *run_id = (const char *)sqlite3_column_text(st, 0);
        const char *recipe = (const char *)sqlite3_column_text(st, 1);
        const char *status = (const char *)sqlite3_column_text(st, 2);
        const char *json = (const char *)sqlite3_column_text(st, 3);
        const char *path = (const char *)sqlite3_column_text(st, 4);
        int started_at = 0;
        if (json) bf_json_int(json, "started_at", &started_at);
        printf("%-24s %-14s %-10s %-12d %s\n",
               run_id ? run_id : "",
               recipe ? recipe : "",
               status ? status : "",
               started_at,
               path ? path : "");
        count++;
    }
    sqlite3_finalize(st);
    sqlite3_close(catalog);

    if (count == 0) {
        if (recipe_id_filter && recipe_id_filter[0])
            printf("no run manifests indexed for recipe '%s'\n", recipe_id_filter);
        else
            printf("no run manifests indexed yet\n");
    }
    return 0;
}

static int cmd_show_run(const char *run_id) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *st = NULL;
    char catalog_db[MAX_PATH];

    bf_catalog_default_db_path(catalog_db, sizeof(catalog_db));
    if (bf_catalog_sync_default(catalog_db) != 0 ||
        bf_sqlite3_open_ro(catalog_db, &catalog) != SQLITE_OK) {
        sqlite3_close(catalog);
        fprintf(stderr, "Unable to open Bonfyre catalog: %s\n", catalog_db);
        return 1;
    }

    sqlite3_prepare_v2(catalog,
        "SELECT json_data FROM catalog_nodes "
        "WHERE kind='run_manifest' AND (external_id=? OR node_id=?)",
        -1, &st, NULL);
    sqlite3_bind_text(st, 1, run_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, run_id, -1, SQLITE_STATIC);

    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *json = (const char *)sqlite3_column_text(st, 0);
        if (json) printf("%s\n", json);
        sqlite3_finalize(st);
        sqlite3_close(catalog);
        return 0;
    }

    fprintf(stderr, "Run manifest not found: %s\n", run_id);
    sqlite3_finalize(st);
    sqlite3_close(catalog);
    return 1;
}

static int cmd_run(const char *db_path, const char *recipe_id,
                   const char *input_path, const char *input_repo,
                   const char *output_dir, int dry_run) {
    printf("═══════════════════════════════════════════════════════\n");
    printf("BonfyreRun — Recipe Executor\n");
    printf("═══════════════════════════════════════════════════════\n\n");

    char *json = NULL;
    if (load_recipe(db_path, recipe_id, &json) != 0) return 1;

    ExecutionContext ctx;
    memset(&ctx, 0, sizeof(ctx));
    snprintf(ctx.recipe_id, sizeof(ctx.recipe_id), "%s", recipe_id);
    snprintf(ctx.input_path, sizeof(ctx.input_path), "%s", input_path ? input_path : "");
    snprintf(ctx.input_repo, sizeof(ctx.input_repo), "%s", input_repo ? input_repo : "");
    snprintf(ctx.output_dir, sizeof(ctx.output_dir), "%s", output_dir);

    const char *doc_end = json + strlen(json);
    extract_string_field(json, doc_end, "name", ctx.name, sizeof(ctx.name));
    extract_string_field(json, doc_end, "version", ctx.version, sizeof(ctx.version));
    extract_string_field(json, doc_end, "hash", ctx.hash, sizeof(ctx.hash));

    printf("Recipe: %s (%s) v%s\n", recipe_id, ctx.name, ctx.version);
    printf("Input:  %s\n", ctx.input_path);
    if (ctx.input_repo[0] != '\0') printf("Repo:   %s\n", ctx.input_repo);
    printf("Output: %s\n\n", output_dir);

    if (parse_stages(json, &ctx) != 0) {
        fprintf(stderr, "Failed to parse stages\n");
        free(json);
        return 1;
    }

    printf("Loaded %d stages\n", ctx.stage_count);

    if (assign_levels(&ctx) != 0) {
        free_context(&ctx);
        free(json);
        return 1;
    }

    if (!dry_run && ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output directory: %s\n", output_dir);
        free_context(&ctx);
        free(json);
        return 1;
    }

    if (dry_run) {
        printf("\n[DRY RUN] Execution plan:\n");
        for (int level = 0; level <= ctx.max_level; level++) {
            printf("\nLevel %d:\n", level);
            for (int i = 0; i < ctx.stage_count; i++) {
                Stage *stage = &ctx.stages[i];
                if (stage->level != level) continue;
                printf("  [%s] %s (%s)\n",
                       stage->id,
                       stage->name[0] ? stage->name : stage->id,
                       stage->operator);
                if (stage->depends_on_count > 0) {
                    printf("    depends_on:");
                    for (int d = 0; d < stage->depends_on_count; d++) {
                        printf(" %s", stage->depends_on[d]);
                    }
                    printf("\n");
                }
                if (stage->skip_if_null_count > 0) {
                    printf("    skip_if_null:");
                    for (int s = 0; s < stage->skip_if_null_count; s++) {
                        printf(" %s", stage->skip_if_null[s]);
                    }
                    printf("\n");
                }
            }
        }
        free_context(&ctx);
        free(json);
        return 0;
    }

    ctx.started_at = time(NULL);
    snprintf(ctx.status, sizeof(ctx.status), "running");

    printf("═══════════════════════════════════════════════════════\n");
    printf("Executing Pipeline\n");
    printf("═══════════════════════════════════════════════════════\n");

    for (int level = 0; level <= ctx.max_level; level++) {
        if (execute_level(&ctx, level) != 0) {
            snprintf(ctx.status, sizeof(ctx.status), "failed");
            ctx.completed_at = time(NULL);
            write_manifests(&ctx);
            free_context(&ctx);
            free(json);
            return 1;
        }
    }

    ctx.completed_at = time(NULL);
    snprintf(ctx.status, sizeof(ctx.status), "success");

    printf("\n═══════════════════════════════════════════════════════\n");
    printf("✓ Pipeline Completed\n");
    printf("═══════════════════════════════════════════════════════\n");
    printf("Duration: %ld seconds\n", ctx.completed_at - ctx.started_at);
    printf("Status:   %s\n", ctx.status);

    write_manifests(&ctx);

    free_context(&ctx);
    free(json);
    return 0;
}

static void print_usage(void) {
    fprintf(stderr,
            "BonfyreRun — Recipe Executor\n\n"
            "Usage:\n"
            "  bonfyre-run <RECIPE_ID> --input <FILE> --out <DIR> [OPTIONS]\n\n"
            "  bonfyre-run history [RECIPE_ID]\n"
            "  bonfyre-run show <RUN_ID>\n\n"
            "Options:\n"
            "  --dry-run           Show execution plan\n"
            "  --db PATH           Custom recipe database\n"
            "  --input-repo PATH   Optional repository input\n"
            "  --resume            Resume from failed stage (TODO)\n"
            "  --from-stage ID     Start from stage (TODO)\n"
            "  --to-stage ID       Stop at stage (TODO)\n"
            "  --batch             Batch mode (TODO)\n");
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }
    if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "help") == 0 ||
        strcmp(argv[1], "-h") == 0) {
        print_usage();
        return 0;
    }
    if (strcmp(argv[1], "history") == 0) {
        return cmd_history(argc >= 3 ? argv[2] : NULL);
    }
    if (strcmp(argv[1], "show") == 0) {
        if (argc < 3) {
            fprintf(stderr, "usage: bonfyre-run show <RUN_ID>\n");
            return 1;
        }
        return cmd_show_run(argv[2]);
    }

    const char *recipe_id = argv[1];
    const char *input_path = NULL;
    const char *input_repo = NULL;
    const char *output_dir = NULL;
    const char *db_path_override = NULL;
    int dry_run = 0;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) {
            input_path = argv[++i];
        } else if (strcmp(argv[i], "--input-repo") == 0 && i + 1 < argc) {
            input_repo = argv[++i];
        } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
            output_dir = argv[++i];
        } else if (strcmp(argv[i], "--db") == 0 && i + 1 < argc) {
            db_path_override = argv[++i];
        } else if (strcmp(argv[i], "--dry-run") == 0) {
            dry_run = 1;
        }
    }

    if (!input_path || !output_dir) {
        fprintf(stderr, "Error: --input and --out are required\n\n");
        print_usage();
        return 1;
    }

    return cmd_run(get_db_path(db_path_override), recipe_id,
                   input_path, input_repo, output_dir, dry_run);
}
