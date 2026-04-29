#include <dirent.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>

#define MAX_RUNTIME_TOKEN 64

typedef struct {
    char token[MAX_RUNTIME_TOKEN];
} RuntimeToken;

typedef struct {
    char id[128];
    char kind[32];
    char name[256];
    char summary[1024];
    char json[4096];
    double score;
} CatalogMatch;
static void path_join(char *buffer, size_t size, const char *left, const char *right) {
    snprintf(buffer, size, "%s/%s", left, right);
}

static void resolve_executable_sibling(char *buffer, size_t size, const char *argv0, const char *sibling_dir, const char *binary_name) {
    if (argv0 && argv0[0] == '/') snprintf(buffer, size, "%s", argv0);
    else if (argv0 && strstr(argv0, "/")) {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd))) snprintf(buffer, size, "%s/%s", cwd, argv0);
        else snprintf(buffer, size, "%s", argv0);
    } else {
        buffer[0] = '\0';
        return;
    }
    char *last = strrchr(buffer, '/');
    if (!last) { buffer[0] = '\0'; return; }
    *last = '\0';
    last = strrchr(buffer, '/');
    if (!last) { buffer[0] = '\0'; return; }
    *last = '\0';
    snprintf(buffer, size, "%s/%s/%s", buffer, sibling_dir, binary_name);
}

static const char *default_binary(const char *env_name, const char *argv0, char *resolved, size_t resolved_size, const char *dir, const char *name, const char *fallback) {
    const char *env = getenv(env_name);
    if (env && env[0] != '\0') return env;
    /* Try top-level sibling path: ../SiblingDir/binary */
    resolve_executable_sibling(resolved, resolved_size, argv0, dir, name);
    if (resolved[0] != '\0' && access(resolved, X_OK) == 0) return resolved;
    /* Try build/ subdirectory: ../SiblingDir/build/binary */
    char build_path[PATH_MAX];
    resolve_executable_sibling(build_path, sizeof(build_path), argv0, dir, name);
    if (build_path[0] != '\0') {
        /* Replace trailing /binary with /build/binary */
        char *last_slash = strrchr(build_path, '/');
        if (last_slash) {
            *last_slash = '\0';
            char candidate[PATH_MAX];
            snprintf(candidate, sizeof(candidate), "%s/build/%s", build_path, name);
            if (access(candidate, X_OK) == 0) {
                snprintf(resolved, resolved_size, "%s", candidate);
                return resolved;
            }
        }
    }
    return fallback;
}

static int run_command(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return 1;
    if (pid == 0) {
        if (strchr(argv[0], '/')) execv(argv[0], argv);
        else execvp(argv[0], argv);
        perror(argv[0]);
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) return 1;
    if (!WIFEXITED(status)) return 1;
    return WEXITSTATUS(status);
}

static int run_command_to_file(char *const argv[], const char *output_path) {
    pid_t pid = fork();
    if (pid < 0) return 1;
    if (pid == 0) {
        FILE *fp = fopen(output_path, "w");
        if (!fp) _exit(127);
        if (dup2(fileno(fp), STDOUT_FILENO) < 0) _exit(127);
        fclose(fp);
        if (strchr(argv[0], '/')) execv(argv[0], argv);
        else execvp(argv[0], argv);
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) return 1;
    if (!WIFEXITED(status)) return 1;
    return WEXITSTATUS(status);
}

static int env_truthy(const char *name, int default_value) {
    const char *v = getenv(name);
    if (!v || !v[0]) return default_value;
    if (strcmp(v, "0") == 0 || strcmp(v, "false") == 0 || strcmp(v, "FALSE") == 0 ||
        strcmp(v, "no") == 0 || strcmp(v, "NO") == 0)
        return 0;
    return 1;
}

static int preflight_enabled(void) {
    return env_truthy("BONFYRE_RUNTIME_PREFLIGHT", 1);
}

static int preflight_strict(void) {
    return env_truthy("BONFYRE_RUNTIME_PREFLIGHT_STRICT", 0);
}

static int preflight_entropy(const char *control_bin, const char *input) {
    if (!preflight_enabled()) return 0;
    if (!input || access(input, R_OK) != 0) return 0;

    const char *thr = getenv("BONFYRE_RUNTIME_ENTROPY_THRESHOLD");
    int rc = 0;
    if (thr && thr[0]) {
        char *argv[] = { (char *)control_bin, "entropy-check", (char *)input, (char *)thr, NULL };
        rc = run_command(argv);
    } else {
        char *argv[] = { (char *)control_bin, "entropy-check", (char *)input, NULL };
        rc = run_command(argv);
    }

    if (rc == 0) return 0;
    if (rc == 2) {
        fprintf(stderr, "preflight: entropy gate failed for %s (rc=2)\n", input);
        return 2;
    }
    if (preflight_strict()) return rc;
    fprintf(stderr, "preflight: entropy probe unavailable (rc=%d), continuing\n", rc);
    return 0;
}

static int preflight_route(const char *control_bin, const char *recipe, const char *input) {
    if (!preflight_enabled()) return 0;
    if (!recipe || !recipe[0] || !input || !input[0]) return 0;

    char *argv[] = { (char *)control_bin, "route", (char *)recipe, (char *)input, NULL };
    int rc = run_command(argv);
    if (rc == 0) return 0;
    if (preflight_strict()) return rc;
    fprintf(stderr, "preflight: control route unavailable (rc=%d), continuing\n", rc);
    return 0;
}

static const char *find_flag_value(int argc, char **argv, const char *flag) {
    for (int i = 0; i < argc - 1; i++) {
        if (strcmp(argv[i], flag) == 0) return argv[i + 1];
    }
    return NULL;
}

static int parse_target_from_makefile(const char *path, char *out, size_t out_sz) {
    FILE *fp = fopen(path, "r");
    if (!fp) return 0;
    char line[1024];
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "TARGET", 6) != 0) continue;
        char *eq = strchr(line, '=');
        if (!eq) continue;
        eq++;
        while (*eq == ' ' || *eq == '\t') eq++;
        size_t n = 0;
        while (eq[n] && eq[n] != '\n' && eq[n] != '\r' && eq[n] != ' ' && eq[n] != '\t') n++;
        if (n > 0 && n < out_sz) {
            memcpy(out, eq, n);
            out[n] = '\0';
            fclose(fp);
            return 1;
        }
    }
    fclose(fp);
    return 0;
}

static int open_cmd_root(const char *argv0, DIR **dir_out, char *root_out, size_t root_sz) {
    const char *candidates[] = { "cmd", "../cmd", "../../cmd", NULL };
    for (int i = 0; candidates[i]; i++) {
        DIR *d = opendir(candidates[i]);
        if (d) {
            snprintf(root_out, root_sz, "%s", candidates[i]);
            *dir_out = d;
            return 1;
        }
    }

    if (argv0 && strchr(argv0, '/')) {
        char abs[PATH_MAX];
        if (argv0[0] == '/') snprintf(abs, sizeof(abs), "%s", argv0);
        else {
            char cwd[PATH_MAX];
            if (!getcwd(cwd, sizeof(cwd))) return 0;
            snprintf(abs, sizeof(abs), "%s/%s", cwd, argv0);
        }
        char *last = strrchr(abs, '/');
        if (!last) return 0;
        *last = '\0';
        snprintf(root_out, root_sz, "%s/../../cmd", abs);
        DIR *d = opendir(root_out);
        if (d) {
            *dir_out = d;
            return 1;
        }
    }
    return 0;
}

static int is_safe_cmd_token(const char *s) {
    if (!s || !s[0]) return 0;
    for (const char *p = s; *p; p++) {
        char c = *p;
        int ok = (c >= 'a' && c <= 'z') ||
                 (c >= 'A' && c <= 'Z') ||
                 (c >= '0' && c <= '9') ||
                 c == '-' || c == '_';
        if (!ok) return 0;
    }
    return 1;
}

static int open_catalog(sqlite3 **db_out, char *path, size_t path_sz) {
    bf_catalog_default_db_path(path, path_sz);
    if (bf_catalog_sync_default(path) != 0) return 0;
    if (bf_sqlite3_open_ro(path, db_out) != SQLITE_OK) {
        sqlite3_close(*db_out);
        *db_out = NULL;
        return 0;
    }
    return 1;
}

static void default_state_path(const char *env_name, const char *subpath, char *buf, size_t sz) {
    const char *env = getenv(env_name);
    const char *home = getenv("HOME");
    if (env && env[0]) {
        snprintf(buf, sz, "%s", env);
        return;
    }
    if (!home) home = "/tmp";
    snprintf(buf, sz, "%s%s", home, subpath);
}

static int path_ready_or_parent_writable(const char *path) {
    if (!path || !path[0]) return 0;
    if (access(path, R_OK | W_OK) == 0) return 1;
    char parent[PATH_MAX];
    snprintf(parent, sizeof(parent), "%s", path);
    char *slash = strrchr(parent, '/');
    if (!slash) return access(".", W_OK) == 0;
    if (slash == parent) slash[1] = '\0';
    else *slash = '\0';
    return access(parent, W_OK) == 0;
}

static int contains_ci(const char *haystack, const char *needle) {
    if (!needle || !needle[0]) return 1;
    if (!haystack || !haystack[0]) return 0;
    size_t needle_len = strlen(needle);
    for (size_t i = 0; haystack[i]; i++) {
        size_t j = 0;
        while (needle[j] && haystack[i + j] &&
               tolower((unsigned char)haystack[i + j]) == tolower((unsigned char)needle[j])) {
            j++;
        }
        if (j == needle_len) return 1;
    }
    return 0;
}

static int tokenize_intent(const char *text, RuntimeToken *tokens, int cap) {
    int count = 0;
    char buf[MAX_RUNTIME_TOKEN];
    int len = 0;
    if (!text) return 0;
    for (const char *p = text; ; p++) {
        unsigned char c = (unsigned char)*p;
        if (isalnum(c) || c == '-' || c == '_') {
            if (len + 1 < (int)sizeof(buf)) buf[len++] = (char)tolower(c);
        } else {
            if (len >= 3 && count < cap) {
                buf[len] = '\0';
                int dup = 0;
                for (int i = 0; i < count; i++) {
                    if (strcmp(tokens[i].token, buf) == 0) { dup = 1; break; }
                }
                if (!dup) {
                    snprintf(tokens[count].token, sizeof(tokens[count].token), "%s", buf);
                    count++;
                }
            }
            len = 0;
            if (c == '\0') break;
        }
    }
    return count;
}

static double score_text_match(const RuntimeToken *tokens, int ntokens,
                               const char *a, const char *b, const char *c, const char *d) {
    double score = 0.0;
    if (ntokens <= 0) return 0.0;
    for (int i = 0; i < ntokens; i++) {
        int hits = 0;
        if (contains_ci(a, tokens[i].token)) hits += 4;
        if (contains_ci(b, tokens[i].token)) hits += 3;
        if (contains_ci(c, tokens[i].token)) hits += 2;
        if (contains_ci(d, tokens[i].token)) hits += 1;
        score += (double)hits;
    }
    return score;
}

static int fetch_best_catalog_match(sqlite3 *db, const char *kind, const char *intent,
                                    CatalogMatch *out_match) {
    sqlite3_stmt *st = NULL;
    RuntimeToken tokens[32];
    int ntokens = tokenize_intent(intent, tokens, 32);
    double best = -1.0;
    memset(out_match, 0, sizeof(*out_match));

    if (sqlite3_prepare_v2(db,
        "SELECT external_id,name,summary,json_data FROM catalog_nodes WHERE kind=?",
        -1, &st, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(st, 1, kind, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *summary = (const char *)sqlite3_column_text(st, 2);
        const char *json = (const char *)sqlite3_column_text(st, 3);
        double score = score_text_match(tokens, ntokens, id, name, summary, json);
        if (intent && id && strcasecmp(intent, id) == 0) score += 1000.0;
        if (intent) {
            if (strcmp(kind, "workflow") == 0) {
                if (contains_ci(intent, "workflow")) score += 12.0;
                if (contains_ci(intent, "pipeline")) score += 12.0;
                if (contains_ci(intent, "investigation")) score += 10.0;
                if (contains_ci(intent, "suite")) score += 8.0;
            } else if (strcmp(kind, "recipe") == 0) {
                if (contains_ci(intent, "recipe")) score += 10.0;
                if (contains_ci(intent, "run")) score += 4.0;
                if (contains_ci(intent, "calibration")) score += 4.0;
                if (contains_ci(intent, "collapse")) score += 4.0;
            } else if (strcmp(kind, "capability") == 0) {
                if (contains_ci(intent, "capability")) score += 8.0;
            }
        }
        if (score > best) {
            best = score;
            snprintf(out_match->id, sizeof(out_match->id), "%s", id ? id : "");
            snprintf(out_match->kind, sizeof(out_match->kind), "%s", kind);
            snprintf(out_match->name, sizeof(out_match->name), "%s", name ? name : "");
            snprintf(out_match->summary, sizeof(out_match->summary), "%s", summary ? summary : "");
            snprintf(out_match->json, sizeof(out_match->json), "%s", json ? json : "");
            out_match->score = score;
        }
    }
    sqlite3_finalize(st);
    return best > 0.0;
}

static int intent_prefers_workflow(const char *intent) {
    if (!intent) return 0;
    return contains_ci(intent, "workflow") ||
           contains_ci(intent, "pipeline") ||
           contains_ci(intent, "investigation") ||
           contains_ci(intent, "suite");
}

static int write_text_file(const char *path, const char *text) {
    FILE *fp = fopen(path, "w");
    if (!fp) return 0;
    fputs(text ? text : "", fp);
    fclose(fp);
    return 1;
}

static int write_autowire_resolution(const char *path,
                                     const char *intent,
                                     const CatalogMatch *recipe,
                                     const CatalogMatch *workflow,
                                     const CatalogMatch *capability) {
    FILE *fp = fopen(path, "w");
    if (!fp) return 0;
    fprintf(fp, "{\n");
    fprintf(fp, "  \"intent\": \"%s\",\n", intent ? intent : "");
    if (recipe && recipe->id[0]) fprintf(fp, "  \"recipe\": \"%s\",\n", recipe->id);
    else fprintf(fp, "  \"recipe\": null,\n");
    if (workflow && workflow->id[0]) fprintf(fp, "  \"workflow\": \"%s\",\n", workflow->id);
    else fprintf(fp, "  \"workflow\": null,\n");
    if (capability && capability->id[0]) fprintf(fp, "  \"capability\": \"%s\"\n", capability->id);
    else fprintf(fp, "  \"capability\": null\n");
    fprintf(fp, "}\n");
    fclose(fp);
    return 1;
}

static int find_binary_in_cmd_tree(const char *argv0, const char *binary, char *out, size_t out_sz) {
    DIR *root = NULL;
    char cmd_root[PATH_MAX];
    if (!open_cmd_root(argv0, &root, cmd_root, sizeof(cmd_root))) return 0;

    struct dirent *de;
    while ((de = readdir(root)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char candidate[PATH_MAX];
        snprintf(candidate, sizeof(candidate), "%s/%s/%s", cmd_root, de->d_name, binary);
        if (access(candidate, X_OK) == 0) {
            snprintf(out, out_sz, "%s", candidate);
            closedir(root);
            return 1;
        }
    }
    closedir(root);
    return 0;
}

static int cmd_dynamic_passthrough(const char *argv0, const char *cmd, int argc, char **argv) {
    if (!is_safe_cmd_token(cmd)) return -1;

    char binary[256];
    if (strncmp(cmd, "bonfyre-", 8) == 0) snprintf(binary, sizeof(binary), "%s", cmd);
    else snprintf(binary, sizeof(binary), "bonfyre-%s", cmd);

    char resolved[PATH_MAX];
    const char *target = binary;
    if (find_binary_in_cmd_tree(argv0, binary, resolved, sizeof(resolved))) {
        target = resolved;
    }

    char **child = calloc((size_t)argc + 2, sizeof(char *));
    if (!child) return 1;
    child[0] = (char *)target;
    for (int i = 0; i < argc; i++) child[i + 1] = argv[i];
    child[argc + 1] = NULL;

    int rc = run_command(child);
    free(child);

    if (rc == 127 && strcmp(target, binary) == 0) return -1;
    return rc;
}

typedef struct {
    char dir[256];
    char target[256];
    int has_target;
    int has_main;
} CapabilityEntry;

typedef struct {
    const char *name;
    const char *path;
} BinarySpec;

static int cmp_capability_entry(const void *a, const void *b) {
    const CapabilityEntry *ea = (const CapabilityEntry *)a;
    const CapabilityEntry *eb = (const CapabilityEntry *)b;
    return strcmp(ea->dir, eb->dir);
}

static int resolve_in_path(const char *name, char *out, size_t out_sz) {
    if (!name || !name[0]) return 0;
    if (strchr(name, '/')) {
        if (access(name, X_OK) == 0) {
            snprintf(out, out_sz, "%s", name);
            return 1;
        }
        return 0;
    }

    const char *path = getenv("PATH");
    if (!path || !path[0]) return 0;

    char *copy = strdup(path);
    if (!copy) return 0;

    int found = 0;
    for (char *tok = strtok(copy, ":"); tok; tok = strtok(NULL, ":")) {
        char cand[PATH_MAX];
        snprintf(cand, sizeof(cand), "%s/%s", tok, name);
        if (access(cand, X_OK) == 0) {
            snprintf(out, out_sz, "%s", cand);
            found = 1;
            break;
        }
    }
    free(copy);
    return found;
}

static int cmd_capabilities(const char *argv0) {
    (void)argv0;
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char path[PATH_MAX];
    if (open_catalog(&db, path, sizeof(path))) {
        printf("{\n");
        printf("  \"generator\":\"bonfyre-runtime capabilities\",\n");
        printf("  \"catalog\":\"%s\",\n", path);

        printf("  \"surfaces\":[\n");
        if (sqlite3_prepare_v2(db,
            "SELECT kind, COUNT(*) FROM catalog_nodes "
            "WHERE kind IN ('workflow','family','capability','model','layer','recipe','run_manifest') "
            "GROUP BY kind ORDER BY kind",
            -1, &st, NULL) == SQLITE_OK) {
            int first = 1;
            while (sqlite3_step(st) == SQLITE_ROW) {
                if (!first) printf(",\n");
                first = 0;
                printf("    {\"kind\":\"%s\",\"count\":%d}",
                       sqlite3_column_text(st, 0) ? (const char *)sqlite3_column_text(st, 0) : "",
                       sqlite3_column_int(st, 1));
            }
            sqlite3_finalize(st);
        }
        printf("\n  ],\n");

        printf("  \"capabilities\":[\n");
        if (sqlite3_prepare_v2(db,
            "SELECT external_id, name, category, json_data FROM catalog_nodes "
            "WHERE kind='capability' ORDER BY external_id",
            -1, &st, NULL) == SQLITE_OK) {
            int first = 1;
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *json = (const char *)sqlite3_column_text(st, 3);
                char binary[128] = "", command[64] = "", artifact[128] = "";
                if (json) {
                    bf_json_str(json, "binary", binary, sizeof(binary));
                    bf_json_str(json, "command", command, sizeof(command));
                    bf_json_str(json, "artifact_out", artifact, sizeof(artifact));
                }
                if (!first) printf(",\n");
                first = 0;
                printf("    {\"id\":\"%s\",\"name\":\"%s\",\"stage\":\"%s\",\"binary\":\"%s\",\"command\":\"%s\",\"artifact\":\"%s\"}",
                       sqlite3_column_text(st, 0) ? (const char *)sqlite3_column_text(st, 0) : "",
                       sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : "",
                       sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "",
                       binary, command, artifact);
            }
            sqlite3_finalize(st);
        }
        printf("\n  ]\n}\n");
        sqlite3_close(db);
        return 0;
    }

    DIR *root = NULL;
    char cmd_root[PATH_MAX];
    if (!open_cmd_root(argv0, &root, cmd_root, sizeof(cmd_root))) {
        fprintf(stderr, "capabilities: unable to locate cmd/ tree\n");
        return 1;
    }

    size_t cap = 64;
    size_t len = 0;
    CapabilityEntry *entries = calloc(cap, sizeof(CapabilityEntry));
    if (!entries) {
        closedir(root);
        fprintf(stderr, "capabilities: out of memory\n");
        return 1;
    }

    struct dirent *de;
    while ((de = readdir(root)) != NULL) {
        if (de->d_name[0] == '.') continue;

        char makefile[PATH_MAX], main_c[PATH_MAX];
        snprintf(makefile, sizeof(makefile), "%s/%s/Makefile", cmd_root, de->d_name);
        snprintf(main_c, sizeof(main_c), "%s/%s/src/main.c", cmd_root, de->d_name);
        if (access(makefile, F_OK) != 0) continue;

        if (len == cap) {
            cap *= 2;
            CapabilityEntry *next = realloc(entries, cap * sizeof(CapabilityEntry));
            if (!next) {
                free(entries);
                closedir(root);
                fprintf(stderr, "capabilities: out of memory\n");
                return 1;
            }
            entries = next;
        }

        CapabilityEntry *e = &entries[len++];
        memset(e, 0, sizeof(*e));
        snprintf(e->dir, sizeof(e->dir), "%s", de->d_name);
        e->has_target = parse_target_from_makefile(makefile, e->target, sizeof(e->target));
        e->has_main = access(main_c, F_OK) == 0;
    }
    closedir(root);

    qsort(entries, len, sizeof(CapabilityEntry), cmp_capability_entry);

    printf("{\n  \"generator\":\"bonfyre-runtime capabilities\",\n");
    printf("  \"cmd_root\":\"%s\",\n", cmd_root);
    printf("  \"commands\":[\n");

    for (size_t i = 0; i < len; i++) {
        if (i > 0) printf(",\n");
        printf("    {\"dir\":\"%s\",\"target\":\"%s\",\"has_target\":%s,\"has_main\":%s}",
               entries[i].dir,
               entries[i].has_target ? entries[i].target : "",
               entries[i].has_target ? "true" : "false",
               entries[i].has_main ? "true" : "false");
    }

    printf("\n  ],\n  \"total\":%zu\n}\n", len);
    free(entries);
    return 0;
}

static int cmd_doctor(int as_json, int preflight_on, int preflight_is_strict, const BinarySpec *bins, size_t bins_len) {
    size_t ready = 0;
    sqlite3 *catalog = NULL;
    sqlite3_stmt *st = NULL;
    char catalog_path[PATH_MAX];
    int catalog_ok = open_catalog(&catalog, catalog_path, sizeof(catalog_path));
    int workflow_count = 0, family_count = 0, capability_count = 0, model_count = 0, layer_count = 0, recipe_count = 0;
    char capability_db[PATH_MAX], model_db[PATH_MAX], layer_db[PATH_MAX];
    default_state_path("BONFYRE_CAPABILITY_DB", "/.local/share/bonfyre/capability.db", capability_db, sizeof(capability_db));
    default_state_path("BONFYRE_MODEL_DB", "/.local/share/bonfyre/models.db", model_db, sizeof(model_db));
    default_state_path("BONFYRE_LAYER_DB", "/.local/share/bonfyre/layers.db", layer_db, sizeof(layer_db));

    if (catalog_ok && sqlite3_prepare_v2(catalog,
        "SELECT kind, COUNT(*) FROM catalog_nodes "
        "WHERE kind IN ('workflow','family','capability','model','layer','recipe') "
        "GROUP BY kind",
        -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *kind = (const char *)sqlite3_column_text(st, 0);
            int count = sqlite3_column_int(st, 1);
            if (kind && strcmp(kind, "workflow") == 0) workflow_count = count;
            else if (kind && strcmp(kind, "family") == 0) family_count = count;
            else if (kind && strcmp(kind, "capability") == 0) capability_count = count;
            else if (kind && strcmp(kind, "model") == 0) model_count = count;
            else if (kind && strcmp(kind, "layer") == 0) layer_count = count;
            else if (kind && strcmp(kind, "recipe") == 0) recipe_count = count;
        }
        sqlite3_finalize(st);
    }

    if (as_json) {
        printf("{\n");
        printf("  \"preflight\":{\"enabled\":%s,\"strict\":%s},\n",
               preflight_on ? "true" : "false",
               preflight_is_strict ? "true" : "false");
        printf("  \"catalog\":{\"ok\":%s,\"path\":\"%s\",\"workflow\":%d,\"family\":%d,\"capability\":%d,\"model\":%d,\"layer\":%d,\"recipe\":%d},\n",
               catalog_ok ? "true" : "false",
               catalog_ok ? catalog_path : "",
               workflow_count, family_count, capability_count, model_count, layer_count, recipe_count);
        printf("  \"registries\":{\"capability\":{\"path\":\"%s\",\"ready\":%s},\"model\":{\"path\":\"%s\",\"ready\":%s},\"layer\":{\"path\":\"%s\",\"ready\":%s}},\n",
               capability_db, path_ready_or_parent_writable(capability_db) ? "true" : "false",
               model_db, path_ready_or_parent_writable(model_db) ? "true" : "false",
               layer_db, path_ready_or_parent_writable(layer_db) ? "true" : "false");
        printf("  \"binaries\":[\n");
    } else {
        printf("bonfyre-runtime doctor\n");
        printf("preflight: enabled=%s strict=%s\n",
               preflight_on ? "true" : "false",
               preflight_is_strict ? "true" : "false");
        printf("catalog:   %s%s\n",
               catalog_ok ? "OK  " : "FAIL",
               catalog_ok ? catalog_path : "");
        if (catalog_ok) {
            printf("surfaces:  workflow=%d family=%d capability=%d model=%d layer=%d recipe=%d\n",
                   workflow_count, family_count, capability_count, model_count, layer_count, recipe_count);
        }
        printf("registries:\n");
        printf("  capability  %s  %s\n", path_ready_or_parent_writable(capability_db) ? "READY" : "MISSING", capability_db);
        printf("  model       %s  %s\n", path_ready_or_parent_writable(model_db) ? "READY" : "MISSING", model_db);
        printf("  layer       %s  %s\n", path_ready_or_parent_writable(layer_db) ? "READY" : "MISSING", layer_db);
        printf("binaries:\n");
    }

    for (size_t i = 0; i < bins_len; i++) {
        char resolved[PATH_MAX] = "";
        int ok = resolve_in_path(bins[i].path, resolved, sizeof(resolved));
        if (ok) ready++;

        if (as_json) {
            if (i > 0) printf(",\n");
            printf("    {\"name\":\"%s\",\"configured\":\"%s\",\"resolved\":\"%s\",\"ok\":%s}",
                   bins[i].name,
                   bins[i].path ? bins[i].path : "",
                   ok ? resolved : "",
                   ok ? "true" : "false");
        } else {
            printf("  %-12s %s\n",
                   bins[i].name,
                   ok ? resolved : "MISSING");
        }
    }

    if (as_json) {
        printf("\n  ],\n  \"summary\":{\"ready\":%zu,\"total\":%zu}\n}\n", ready, bins_len);
    } else {
        printf("summary: %zu/%zu binaries resolvable\n", ready, bins_len);
        if (!catalog_ok || ready != bins_len) {
            printf("hint: install missing binaries or set BONFYRE_*_BINARY overrides\n");
            if (catalog) sqlite3_close(catalog);
            return 2;
        }
    }
    if (catalog) sqlite3_close(catalog);
    return 0;
}

static void mkdir_p(const char *path) {
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    mkdir(tmp, 0755);
}

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-runtime\n\n"
            "Usage:\n"
            "  bonfyre-runtime run <input> [pipeline args...]\n"
            "  bonfyre-runtime run-ledger <input> [pipeline args...]\n"
            "  bonfyre-runtime queue <queue args...>\n"
            "  bonfyre-runtime ledger <ledger args...>\n"
            "  bonfyre-runtime loop <N> <binary> [args...]\n"
            "  bonfyre-runtime parallel [-- cmd args...]...\n"
            "  bonfyre-runtime pipeline [-- cmd args...]...\n"
            "  bonfyre-runtime gen <gen args...>\n"
            "  bonfyre-runtime swarm <swarm args...>\n"
            "  bonfyre-runtime control <control args...>\n"
            "  bonfyre-runtime recipe <recipe args...>\n"
            "  bonfyre-runtime model <model args...>\n"
            "  bonfyre-runtime stitch <stitch args...>\n"
            "  bonfyre-runtime proxy <proxy args...>\n"
            "  bonfyre-runtime sli <sli args...>\n"
            "  bonfyre-runtime run-recipe <code> <input> [bonfyre-run args...]\n"
            "  bonfyre-runtime service <proxy|moq|swarm-worker> [args...]\n"
            "  bonfyre-runtime capabilities\n"
            "      print machine-readable cmd capability index (JSON)\n"
            "  bonfyre-runtime doctor [--json]\n"
            "      validate runtime dependency binaries and preflight toggles\n"
            "  bonfyre-runtime conference [video-relay args...]\n"
            "  bonfyre-runtime autowire <input> --intent <text> --out <dir> [--mode local|swarm] [--nodes N]\n"
            "  bonfyre-runtime <cmd> [args...]\n"
            "      dynamic pass-through to bonfyre-<cmd> for recovered/orphaned modules\n"
            "\n"
            "  loop:     runs <binary> N times; passes previous artifact.json as --in\n"
            "            to each subsequent iteration.\n"
            "  parallel: forks all '-- cmd args...' groups concurrently; waits for\n"
            "            all to finish; returns 0 only if every child exited 0.\n"
            "            Separate independent pipeline stages with '--'.\n"
            "  pipeline: chains '-- cmd args...' groups via OS pipes: stdout of\n"
            "            stage N is streamed directly into stdin of stage N+1.\n"
            "            Eliminates bonfyre-space round-trip I/O for intermediate data.\n"
            "            All stages run concurrently; waits for the final stage.\n"
            "  gen/swarm/control/recipe/model/stitch/proxy/sli: pass-through wrappers so\n"
            "            runtime can orchestrate broad cmd-tree capabilities from one entry point.\n"
            "  run-recipe: canonical bridge to bonfyre-run recipe execution semantics.\n"
            "  service: standard launcher for long-running infra (proxy, moq relay, swarm worker).\n"
            "  autowire: resolves existing recipe / workflow / capability matches from the\n"
            "            shared catalog first; optional generator fallback is disabled by default.\n");}

static void print_usage(void);
static int  cmd_parallel(int argc, char **argv);
static int  cmd_pipeline(int argc, char **argv);

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

    char queue_resolved[PATH_MAX];
    char pipeline_resolved[PATH_MAX];
    char ledger_resolved[PATH_MAX];
    char gen_resolved[PATH_MAX];
    char swarm_resolved[PATH_MAX];
    char control_resolved[PATH_MAX];
    char moq_resolved[PATH_MAX];
    char run_resolved[PATH_MAX];
    char recipe_resolved[PATH_MAX];
    char model_resolved[PATH_MAX];
    char stitch_resolved[PATH_MAX];
    char proxy_resolved[PATH_MAX];
    char sli_resolved[PATH_MAX];
    char orchestrate_resolved[PATH_MAX];
    char project_resolved[PATH_MAX];
    char flow_resolved[PATH_MAX];
    const char *queue_bin = default_binary("BONFYRE_QUEUE_BINARY", argv[0], queue_resolved, sizeof(queue_resolved), "BonfyreQueue", "bonfyre-queue", "../BonfyreQueue/bonfyre-queue");
    const char *pipeline_bin = default_binary("BONFYRE_PIPELINE_BINARY", argv[0], pipeline_resolved, sizeof(pipeline_resolved), "BonfyrePipeline", "bonfyre-pipeline", "../BonfyrePipeline/bonfyre-pipeline");
    const char *ledger_bin = default_binary("BONFYRE_LEDGER_BINARY", argv[0], ledger_resolved, sizeof(ledger_resolved), "BonfyreLedger", "bonfyre-ledger", "../BonfyreLedger/bonfyre-ledger");
    const char *gen_bin = default_binary("BONFYRE_GEN_BINARY", argv[0], gen_resolved, sizeof(gen_resolved), "BonfyreGen", "bonfyre-gen", "../BonfyreGen/bonfyre-gen");
    const char *swarm_bin = default_binary("BONFYRE_SWARM_BINARY", argv[0], swarm_resolved, sizeof(swarm_resolved), "BonfyreSwarm", "bonfyre-swarm", "../BonfyreSwarm/bonfyre-swarm");
    const char *control_bin = default_binary("BONFYRE_CONTROL_BINARY", argv[0], control_resolved, sizeof(control_resolved), "BonfyreControl", "bonfyre-control", "../BonfyreControl/bonfyre-control");
    const char *moq_bin = default_binary("BONFYRE_MOQ_BINARY", argv[0], moq_resolved, sizeof(moq_resolved), "BonfyreMoQ", "bonfyre-moq", "../BonfyreMoQ/bonfyre-moq");
    const char *run_bin = default_binary("BONFYRE_RUN_BINARY", argv[0], run_resolved, sizeof(run_resolved), "BonfyreRun", "bonfyre-run", "../BonfyreRun/bonfyre-run");
    const char *recipe_bin = default_binary("BONFYRE_RECIPE_BINARY", argv[0], recipe_resolved, sizeof(recipe_resolved), "BonfyreRecipe", "bonfyre-recipe", "../BonfyreRecipe/bonfyre-recipe");
    const char *model_bin = default_binary("BONFYRE_MODEL_BINARY", argv[0], model_resolved, sizeof(model_resolved), "BonfyreModel", "bonfyre-model", "../BonfyreModel/bonfyre-model");
    const char *stitch_bin = default_binary("BONFYRE_STITCH_BINARY", argv[0], stitch_resolved, sizeof(stitch_resolved), "BonfyreStitch", "bonfyre-stitch", "../BonfyreStitch/bonfyre-stitch");
    const char *proxy_bin = default_binary("BONFYRE_PROXY_BINARY", argv[0], proxy_resolved, sizeof(proxy_resolved), "BonfyreProxy", "bonfyre-proxy", "../BonfyreProxy/bonfyre-proxy");
    const char *sli_bin = default_binary("BONFYRE_SLI_BINARY", argv[0], sli_resolved, sizeof(sli_resolved), "BonfyreSLI", "bonfyre-sli", "../BonfyreSLI/bonfyre-sli");
    const char *orchestrate_bin = default_binary("BONFYRE_ORCHESTRATE_BINARY", argv[0], orchestrate_resolved, sizeof(orchestrate_resolved), "BonfyreOrchestrate", "bonfyre-orchestrate", "../BonfyreOrchestrate/bonfyre-orchestrate");
    const char *project_bin = default_binary("BONFYRE_PROJECT_BINARY", argv[0], project_resolved, sizeof(project_resolved), "BonfyreProject", "bonfyre-project", "../BonfyreProject/bonfyre-project");
    const char *flow_bin = default_binary("BONFYRE_FLOW_BINARY", argv[0], flow_resolved, sizeof(flow_resolved), "BonfyreFlow", "bonfyre-flow", "../BonfyreFlow/bonfyre-flow");
    const BinarySpec runtime_bins[] = {
        {"queue", queue_bin},
        {"pipeline", pipeline_bin},
        {"ledger", ledger_bin},
        {"gen", gen_bin},
        {"swarm", swarm_bin},
        {"control", control_bin},
        {"moq", moq_bin},
        {"run", run_bin},
        {"recipe", recipe_bin},
        {"model", model_bin},
        {"stitch", stitch_bin},
        {"proxy", proxy_bin},
        {"sli", sli_bin},
        {"orchestrate", orchestrate_bin},
        {"project", project_bin},
        {"flow", flow_bin}
    };

    if (strcmp(argv[1], "capabilities") == 0) {
        return cmd_capabilities(argv[0]);
    }

    if (strcmp(argv[1], "doctor") == 0) {
        int as_json = (argc >= 3 && strcmp(argv[2], "--json") == 0);
        return cmd_doctor(as_json,
                          preflight_enabled(),
                          preflight_strict(),
                          runtime_bins,
                          sizeof(runtime_bins) / sizeof(runtime_bins[0]));
    }

    if (strcmp(argv[1], "gen") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)gen_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "swarm") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)swarm_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "control") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)control_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "recipe") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)recipe_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "model") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)model_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "stitch") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)stitch_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "proxy") == 0) {
        char **child = calloc((size_t)argc + 1, sizeof(char *));
        if (!child) return 1;
        int c = 0;
        child[c++] = (char *)proxy_bin;
        if (argc >= 3 &&
            (strcmp(argv[2], "--help") == 0 || strcmp(argv[2], "help") == 0 || strcmp(argv[2], "-h") == 0)) {
            /* bonfyre-proxy prints usage when run without a subcommand */
        } else {
            for (int i = 2; i < argc; i++) child[c++] = argv[i];
        }
        child[c] = NULL;
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "sli") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)sli_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "orchestrate") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)orchestrate_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "project") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)project_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "flow") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)flow_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "run-recipe") == 0) {
        if (argc < 4) {
            fprintf(stderr, "usage: bonfyre-runtime run-recipe <code> <input> [bonfyre-run args...]\n");
            return 1;
        }

        {
            int prc = preflight_entropy(control_bin, argv[3]);
            if (prc != 0) return prc;
            prc = preflight_route(control_bin, argv[2], argv[3]);
            if (prc != 0) return prc;
        }

        int has_input_flag = 0;
        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--input") == 0) {
                has_input_flag = 1;
                break;
            }
        }

        char **child = calloc((size_t)argc + 3, sizeof(char *));
        if (!child) return 1;
        int c = 0;
        child[c++] = (char *)run_bin;
        child[c++] = argv[2];
        if (!has_input_flag) {
            child[c++] = "--input";
            child[c++] = argv[3];
        }
        for (int i = 4; i < argc; i++) child[c++] = argv[i];
        child[c] = NULL;
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "service") == 0) {
        if (argc < 3) {
            fprintf(stderr, "usage: bonfyre-runtime service <proxy|moq|swarm-worker> [args...]\n");
            return 1;
        }
        if (strcmp(argv[2], "proxy") == 0) {
            char **child = calloc((size_t)argc + 2, sizeof(char *));
            if (!child) return 1;
            int c = 0;
            child[c++] = (char *)proxy_bin;
            child[c++] = "serve";
            for (int i = 3; i < argc; i++) child[c++] = argv[i];
            child[c] = NULL;
            int rc = run_command(child);
            free(child);
            return rc;
        }
        if (strcmp(argv[2], "moq") == 0) {
            char **child = calloc((size_t)argc + 2, sizeof(char *));
            if (!child) return 1;
            int c = 0;
            child[c++] = (char *)moq_bin;
            child[c++] = "video-relay";
            for (int i = 3; i < argc; i++) child[c++] = argv[i];
            child[c] = NULL;
            int rc = run_command(child);
            free(child);
            return rc;
        }
        if (strcmp(argv[2], "swarm-worker") == 0) {
            char **child = calloc((size_t)argc + 2, sizeof(char *));
            if (!child) return 1;
            int c = 0;
            child[c++] = (char *)swarm_bin;
            child[c++] = "worker";
            for (int i = 3; i < argc; i++) child[c++] = argv[i];
            child[c] = NULL;
            int rc = run_command(child);
            free(child);
            return rc;
        }
        fprintf(stderr, "unknown service type: %s\n", argv[2]);
        return 1;
    }

    if (strcmp(argv[1], "conference") == 0) {
        char **child = calloc((size_t)argc + 2, sizeof(char *));
        if (!child) return 1;
        int c = 0;
        child[c++] = (char *)moq_bin;
        if (argc >= 3 &&
            (strcmp(argv[2], "help") == 0 || strcmp(argv[2], "--help") == 0)) {
            child[c++] = "help";
        } else {
            child[c++] = "video-relay";
            for (int i = 2; i < argc; i++) child[c++] = argv[i];
        }
        child[c] = NULL;
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "autowire") == 0) {
        if (argc < 3) {
            fprintf(stderr, "usage: bonfyre-runtime autowire <input> --intent <text> --out <dir> [--mode local|swarm] [--nodes N]\n");
            return 1;
        }
        const char *input = argv[2];
        const char *intent = NULL;
        const char *out_dir = NULL;
        const char *mode = "local";
        const char *nodes = NULL;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--intent") == 0 && i + 1 < argc) intent = argv[++i];
            else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out_dir = argv[++i];
            else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) mode = argv[++i];
            else if (strcmp(argv[i], "--nodes") == 0 && i + 1 < argc) nodes = argv[++i];
        }
        if (!intent || !out_dir) {
            fprintf(stderr, "autowire requires --intent and --out\n");
            return 1;
        }

        {
            int prc = preflight_entropy(control_bin, input);
            if (prc != 0) return prc;
            prc = preflight_route(control_bin, intent, input);
            if (prc != 0) return prc;
        }

        mkdir_p(out_dir);

        {
            sqlite3 *catalog = NULL;
            char catalog_path[PATH_MAX];
            CatalogMatch recipe_match;
            CatalogMatch workflow_match;
            CatalogMatch capability_match;
            memset(&recipe_match, 0, sizeof(recipe_match));
            memset(&workflow_match, 0, sizeof(workflow_match));
            memset(&capability_match, 0, sizeof(capability_match));

            if (open_catalog(&catalog, catalog_path, sizeof(catalog_path))) {
                int have_recipe = fetch_best_catalog_match(catalog, "recipe", intent, &recipe_match);
                int have_workflow = fetch_best_catalog_match(catalog, "workflow", intent, &workflow_match);
                int have_capability = fetch_best_catalog_match(catalog, "capability", intent, &capability_match);
                sqlite3_close(catalog);

                char resolution_path[PATH_MAX];
                path_join(resolution_path, sizeof(resolution_path), out_dir, "autowire-resolution.json");
                write_autowire_resolution(resolution_path, intent,
                                          have_recipe ? &recipe_match : NULL,
                                          have_workflow ? &workflow_match : NULL,
                                          have_capability ? &capability_match : NULL);

                if (have_workflow &&
                    (!have_recipe ||
                     workflow_match.score >= recipe_match.score ||
                     (intent_prefers_workflow(intent) && workflow_match.score > 0.0))) {
                    char workflow_path[PATH_MAX];
                    path_join(workflow_path, sizeof(workflow_path), out_dir, "workflow.resolved.json");
                    write_text_file(workflow_path, workflow_match.json);
                    fprintf(stderr,
                            "autowire: resolved workflow %s from catalog\n  input:  %s\n  workflow: %s\n  resolution: %s\n",
                            workflow_match.id, input, workflow_path, resolution_path);
                    return 0;
                }

                if (have_recipe) {
                    if (strcmp(mode, "swarm") == 0) {
                        fprintf(stderr,
                                "autowire: resolved recipe %s from catalog; local execution remains canonical in this pass, running locally\n",
                                recipe_match.id);
                    }
                    char *run_argv[] = {
                        (char *)run_bin,
                        recipe_match.id,
                        "--input", (char *)input,
                        "--out", (char *)out_dir,
                        NULL
                    };
                    int rc = run_command(run_argv);
                    if (rc != 0) {
                        fprintf(stderr, "autowire: catalog recipe run failed (rc=%d)\n", rc);
                        return rc;
                    }
                    fprintf(stderr,
                            "autowire: complete\n  input:  %s\n  recipe: %s\n  mode:   local\n  resolution: %s\n",
                            input, recipe_match.id, resolution_path);
                    return 0;
                }

                if (have_capability) {
                    fprintf(stderr,
                            "autowire: resolved capability %s from catalog\n  input:  %s\n  resolution: %s\n",
                            capability_match.id, input, resolution_path);
                    return 0;
                }
            }
        }

        if (!env_truthy("BONFYRE_RUNTIME_AUTOWIRE_GEN_FALLBACK", 0)) {
            fprintf(stderr, "autowire: no catalog-backed recipe/workflow/capability match and generator fallback is disabled\n");
            return 1;
        }

        char recipe_path[PATH_MAX];
        char artifact_path[PATH_MAX];
        path_join(recipe_path, sizeof(recipe_path), out_dir, "recipe.generated.yaml");
        path_join(artifact_path, sizeof(artifact_path), out_dir, "artifact.json");

        char *gen_argv[] = { (char *)gen_bin, (char *)intent, NULL };
        int rc = run_command_to_file(gen_argv, recipe_path);
        if (rc != 0) {
            fprintf(stderr, "autowire: bonfyre-gen failed (rc=%d)\n", rc);
            return rc;
        }

        if (strcmp(mode, "swarm") == 0) {
            char *swarm_argv_nodes[] = {
                (char *)swarm_bin, "dispatch", recipe_path, (char *)input,
                "--nodes", (char *)(nodes ? nodes : "8"), NULL
            };
            rc = run_command(swarm_argv_nodes);
            if (rc != 0) {
                fprintf(stderr, "autowire: bonfyre-swarm dispatch failed (rc=%d)\n", rc);
                return rc;
            }
        } else {
            char *pipe_argv[] = {
                (char *)pipeline_bin, "run", (char *)input,
                "--recipe", recipe_path,
                "--out", (char *)out_dir,
                NULL
            };
            rc = run_command(pipe_argv);
            if (rc != 0) {
                fprintf(stderr, "autowire: bonfyre-pipeline run failed (rc=%d)\n", rc);
                return rc;
            }
        }

        if (access(artifact_path, F_OK) == 0) {
            char *score_argv[] = { (char *)control_bin, "score", artifact_path, NULL };
            run_command(score_argv);
        }
        {
            char *ops_argv[] = { (char *)control_bin, "ops", NULL };
            run_command(ops_argv);
        }

        fprintf(stderr, "autowire: complete\n  input:  %s\n  recipe: %s\n  mode:   %s\n", input, recipe_path, mode);
        return 0;
    }

    if (strcmp(argv[1], "queue") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)queue_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "ledger") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)ledger_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "run") == 0 || strcmp(argv[1], "run-ledger") == 0) {
        if (argc < 3) {
            print_usage();
            return 1;
        }
        const int with_ledger = (strcmp(argv[1], "run-ledger") == 0);
        const char *input = argv[2];
        const char *recipe_hint = find_flag_value(argc - 3, argv + 3, "--recipe");
        const char *out_dir = NULL;
        for (int i = 3; i < argc - 1; i++) {
            if (strcmp(argv[i], "--out") == 0) out_dir = argv[i + 1];
        }
        if (!out_dir) {
            fprintf(stderr, "run and run-ledger require --out DIR\n");
            return 1;
        }

        {
            int prc = preflight_entropy(control_bin, input);
            if (prc != 0) return prc;
            prc = preflight_route(control_bin, recipe_hint, input);
            if (prc != 0) return prc;
        }

        char **pipeline_argv = calloc((size_t)argc + 2, sizeof(char *));
        if (!pipeline_argv) return 1;
        int p = 0;
        pipeline_argv[p++] = (char *)pipeline_bin;
        pipeline_argv[p++] = "run";
        pipeline_argv[p++] = (char *)input;
        for (int i = 3; i < argc; i++) pipeline_argv[p++] = argv[i];
        pipeline_argv[p] = NULL;
        int rc = run_command(pipeline_argv);
        free(pipeline_argv);
        if (rc != 0 || !with_ledger) return rc;

        char artifact_path[PATH_MAX];
        char ledger_json[PATH_MAX];
        path_join(artifact_path, sizeof(artifact_path), out_dir, "artifact.json");
        path_join(ledger_json, sizeof(ledger_json), out_dir, "ledger-assessment.json");
        char *ledger_argv[] = {
            (char *)ledger_bin,
            "assess-json",
            artifact_path,
            NULL
        };
        return run_command_to_file(ledger_argv, ledger_json);
    }

    if (strcmp(argv[1], "loop") == 0) {
        if (argc < 4) {
            fprintf(stderr, "usage: bonfyre-runtime loop <N> <binary> [args...]\n");
            return 1;
        }
        int n_iters = atoi(argv[2]);
        if (n_iters <= 0 || n_iters > 1000) {
            fprintf(stderr, "loop: N must be 1..1000 (got %s)\n", argv[2]);
            return 1;
        }
        const char *binary = argv[3];

        /* Find --out DIR in user args, if any */
        const char *base_out = NULL;
        for (int i = 4; i < argc - 1; i++) {
            if (strcmp(argv[i], "--out") == 0) { base_out = argv[i + 1]; break; }
        }

        char prev_artifact[PATH_MAX];
        prev_artifact[0] = '\0';

        pid_t self_pid = getpid();

        for (int iter = 1; iter <= n_iters; iter++) {
            /* Build output dir for this iteration */
            char iter_out[PATH_MAX];
            if (base_out) {
                snprintf(iter_out, sizeof(iter_out), "%s-%d", base_out, iter);
            } else {
                snprintf(iter_out, sizeof(iter_out),
                         "/tmp/bonfyre-loop-%d-%d", (int)self_pid, iter);
            }

            /* mkdir -p iter_out */
            {
                char tmp[PATH_MAX];
                snprintf(tmp, sizeof(tmp), "%s", iter_out);
                for (char *p = tmp + 1; *p; p++) {
                    if (*p == '/') {
                        *p = '\0';
                        mkdir(tmp, 0755);
                        *p = '/';
                    }
                }
                mkdir(tmp, 0755);
            }

            /* Build child argv:
             *   binary [original args, with --out replaced by iter_out]
             *   [--in prev_artifact  if iter > 1]
             */
            int extra = (iter > 1) ? 2 : 0;  /* --in <path> */
            char **child = (char **)calloc((size_t)(argc - 4 + 3 + extra + 2),
                                           sizeof(char *));
            if (!child) return 1;
            int ci = 0;
            child[ci++] = (char *)binary;

            int skip_next = 0;
            for (int i = 4; i < argc; i++) {
                if (skip_next) { skip_next = 0; continue; }
                if (strcmp(argv[i], "--out") == 0) {
                    child[ci++] = "--out";
                    child[ci++] = iter_out;
                    skip_next = 1; /* skip original DIR */
                } else {
                    child[ci++] = argv[i];
                }
            }
            if (!base_out) {
                child[ci++] = "--out";
                child[ci++] = iter_out;
            }
            if (iter > 1 && prev_artifact[0]) {
                child[ci++] = "--in";
                child[ci++] = prev_artifact;
            }
            child[ci] = NULL;

            fprintf(stderr, "bonfyre-runtime loop [%d/%d]: %s\n",
                    iter, n_iters, binary);

            int rc = run_command(child);
            free(child);
            if (rc != 0) {
                fprintf(stderr, "bonfyre-runtime loop: iteration %d failed (rc=%d)\n",
                        iter, rc);
                return rc;
            }

            /* Next iteration's --in = this iteration's artifact.json */
            path_join(prev_artifact, sizeof(prev_artifact),
                      iter_out, "artifact.json");
            /* If no artifact.json was written, clear so --in is not passed */
            if (access(prev_artifact, F_OK) != 0) prev_artifact[0] = '\0';
        }

        fprintf(stderr, "bonfyre-runtime loop: completed %d iterations\n", n_iters);
        return 0;
    }

    if (strcmp(argv[1], "parallel") == 0) {
        return cmd_parallel(argc - 2, argv + 2);
    }

    if (strcmp(argv[1], "pipeline") == 0) {
        return cmd_pipeline(argc - 2, argv + 2);
    }

    {
        int dynamic_rc = cmd_dynamic_passthrough(argv[0], argv[1], argc - 2, argv + 2);
        if (dynamic_rc >= 0) return dynamic_rc;
    }

    print_usage();
    return 1;
}

/* ================================================================
 * parallel subcommand
 *
 * bonfyre-runtime parallel [-- binary arg...] [-- binary arg...] ...
 *
 * Parses the argv into groups delimited by "--".  Forks each group
 * simultaneously, then collects all exit codes.  Returns 0 only if
 * every child exited with status 0.  This lets independent pipeline
 * stages (transcription, hashing, ledger update) overlap their I/O
 * and CPU work.
 *
 * Max groups: 64 (enough for any realistic pipeline fan-out).
 * ================================================================ */
#define PAR_MAX_GROUPS 64

static int cmd_parallel(int argc, char **argv) {
    /* Collect group start indices in remaining argv (after "parallel") */
    int group_starts[PAR_MAX_GROUPS];
    int group_count = 0;

    int i = 0;
    while (i < argc) {
        if (strcmp(argv[i], "--") == 0) {
            i++;
            if (i < argc && group_count < PAR_MAX_GROUPS) {
                group_starts[group_count++] = i;
            }
            continue;
        }
        /* First argument without a leading "--" also starts an implicit group */
        if (group_count == 0 && group_count < PAR_MAX_GROUPS) {
            group_starts[group_count++] = i;
        }
        i++;
    }

    if (group_count == 0) {
        fprintf(stderr, "bonfyre-runtime parallel: no commands given\n");
        return 1;
    }

    pid_t pids[PAR_MAX_GROUPS];

    /* Compute extent of each group (from its start to the next "--" or end) */
    for (int g = 0; g < group_count; g++) {
        int start = group_starts[g];
        /* Find end: next "--" marker */
        int end = argc;
        for (int j = start; j < argc; j++) {
            if (strcmp(argv[j], "--") == 0) { end = j; break; }
        }
        int len = end - start;
        if (len <= 0) { pids[g] = -1; continue; }

        /* Build null-terminated argv for execv */
        char **cargv = calloc((size_t)(len + 1), sizeof(char *));
        if (!cargv) {
            fprintf(stderr, "bonfyre-runtime parallel: OOM\n");
            /* Kill already-forked children */
            for (int k = 0; k < g; k++) if (pids[k] > 0) kill(pids[k], SIGTERM);
            return 1;
        }
        for (int j = 0; j < len; j++) cargv[j] = argv[start + j];
        cargv[len] = NULL;

        pid_t pid = fork();
        if (pid < 0) {
            perror("fork");
            free(cargv);
            for (int k = 0; k < g; k++) if (pids[k] > 0) kill(pids[k], SIGTERM);
            return 1;
        }
        if (pid == 0) {
            execv(cargv[0], cargv);
            perror(cargv[0]);
            _exit(127);
        }
        free(cargv);
        pids[g] = pid;
    }

    /* Collect all children */
    int overall = 0;
    for (int g = 0; g < group_count; g++) {
        if (pids[g] <= 0) continue;
        int st = 0;
        waitpid(pids[g], &st, 0);
        int code = WIFEXITED(st) ? WEXITSTATUS(st) : 1;
        if (code != 0) {
            fprintf(stderr, "bonfyre-runtime parallel: group %d exited %d\n", g, code);
            overall = code;
        }
    }
    return overall;
}

/* ================================================================
 * pipeline subcommand — cross-binary stdout→stdin chaining
 *
 * bonfyre-runtime pipeline [-- binary arg...] [-- binary arg...] ...
 *
 * Each group is connected to the next via an OS pipe:
 *   stdout(stage 0) → stdin(stage 1) → stdin(stage 2) → ... → stdout(last) → terminal
 *
 * This eliminates bonfyre-space I/O for intermediate data — the output of
 * one binary flows directly into the next binary's stdin as a byte stream,
 * reducing latency by the cost of writing + reading intermediate files.
 *
 * All stages are forked simultaneously (like parallel) but with their
 * stdio connected in a chain.  The parent waits for all children; if any
 * stage fails, the downstream stages receive EOF and terminate naturally.
 *
 * Max stages: PAR_MAX_GROUPS (64).
 *
 * Example (Ingest → Transcribe → Clean as a single low-latency stream):
 *   bonfyre-runtime pipeline \
 *     -- bonfyre-ingest  --input audio.mp3 \
 *     -- bonfyre-transcribe --model whisper-medium \
 *     -- bonfyre-clean   --out /artifacts/job-001/
 * ================================================================ */
static int cmd_pipeline(int argc, char **argv) {
    /* Parse groups (same logic as cmd_parallel) */
    int group_starts[PAR_MAX_GROUPS];
    int group_count = 0;
    int i = 0;
    while (i < argc) {
        if (strcmp(argv[i], "--") == 0) {
            i++;
            if (i < argc && group_count < PAR_MAX_GROUPS)
                group_starts[group_count++] = i;
            continue;
        }
        if (group_count == 0 && group_count < PAR_MAX_GROUPS)
            group_starts[group_count++] = i;
        i++;
    }
    if (group_count == 0) {
        fprintf(stderr, "bonfyre-runtime pipeline: no stages given\n");
        return 1;
    }

    /* Create N-1 pipes connecting adjacent stages */
    int pipes[PAR_MAX_GROUPS][2];
    for (int g = 0; g < group_count - 1; g++) {
        if (pipe(pipes[g]) != 0) {
            perror("pipe");
            return 1;
        }
    }

    pid_t pids[PAR_MAX_GROUPS];

    for (int g = 0; g < group_count; g++) {
        /* Determine argv extent for this group */
        int start = group_starts[g];
        int end   = argc;
        for (int j = start; j < argc; j++) {
            if (strcmp(argv[j], "--") == 0) { end = j; break; }
        }
        int len = end - start;
        if (len <= 0) { pids[g] = -1; continue; }

        char **cargv = calloc((size_t)(len + 1), sizeof(char *));
        if (!cargv) { perror("calloc"); return 1; }
        for (int j = 0; j < len; j++) cargv[j] = argv[start + j];
        cargv[len] = NULL;

        pid_t pid = fork();
        if (pid < 0) { perror("fork"); free(cargv); return 1; }

        if (pid == 0) {
            /* Child: wire stdin from previous pipe, stdout to next pipe */

            /* stdin: read end of pipe from previous stage */
            if (g > 0) {
                if (dup2(pipes[g-1][0], STDIN_FILENO) < 0) _exit(127);
            }
            /* stdout: write end of pipe to next stage */
            if (g < group_count - 1) {
                if (dup2(pipes[g][1], STDOUT_FILENO) < 0) _exit(127);
            }

            /* Close all pipe ends in child — only the dup'd ones matter */
            for (int k = 0; k < group_count - 1; k++) {
                close(pipes[k][0]);
                close(pipes[k][1]);
            }

            execv(cargv[0], cargv);
            perror(cargv[0]);
            _exit(127);
        }

        free(cargv);
        pids[g] = pid;
    }

    /* Parent: close all pipe ends (children have their own copies) */
    for (int g = 0; g < group_count - 1; g++) {
        close(pipes[g][0]);
        close(pipes[g][1]);
    }

    /* Wait for all children */
    int overall = 0;
    for (int g = 0; g < group_count; g++) {
        if (pids[g] <= 0) continue;
        int st = 0;
        waitpid(pids[g], &st, 0);
        int code = WIFEXITED(st) ? WEXITSTATUS(st) : 1;
        if (code != 0) {
            fprintf(stderr, "bonfyre-runtime pipeline: stage %d exited %d\n", g, code);
            if (overall == 0) overall = code;
        }
    }
    return overall;
}
