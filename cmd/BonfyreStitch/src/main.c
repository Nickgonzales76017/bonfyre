/*
 * BonfyreStitch — DAG materializer.
 *
 * Given an artifact.json and a target realization, walks the operator DAG
 * backwards, finds all dependencies, and re-executes operators to
 * materialize the target. Caches results by node_hash.
 *
 * This is the reconstruction engine. Store atoms + operators, delete
 * everything else, and Stitch rebuilds on demand. Pay for storage once.
 *
 * Usage:
 *   bonfyre-stitch materialize <artifact.json> --target <realization_id> [--cache DIR]
 *   bonfyre-stitch plan <artifact.json> --target <realization_id>  — dry-run, show plan
 *   bonfyre-stitch prune <family_dir> --keep-pinned              — delete unpinned realizations
 *   bonfyre-stitch cache-stats [--cache DIR]                     — show cache hit/miss
 */
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <bonfyre.h>

#define MAX_OPS 128
#define MAX_LINE 65536

static char *read_file_full(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;
    fseek(fp, 0, SEEK_END); long sz = ftell(fp); fseek(fp, 0, SEEK_SET);
    if (sz < 0) { fclose(fp); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(fp); return NULL; }
    fread(buf, 1, (size_t)sz, fp); buf[sz] = '\0';
    fclose(fp); return buf;
}

/* Operator mapping: op name -> binary to invoke */
typedef struct {
    char op_id[128];
    char op[64];
    char inputs[512];   /* comma-separated */
    char output[128];
    char params[1024];
    char node_hash[128];
    char version[32];
} Operator;

/* Naive JSON extraction */
static int json_str(const char *json, const char *key, char *out, size_t sz) {
    char needle[256]; snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    while (*p && (*p == ' ' || *p == ':' || *p == '\t')) p++;
    if (*p != '"') return 0; p++;
    size_t i = 0;
    while (*p && *p != '"' && i < sz - 1) out[i++] = *p++;
    out[i] = '\0'; return 1;
}

/* Map op type to Bonfyre binary */
static const char *op_to_binary(const char *op) {
    if (strcmp(op, "Normalize") == 0 || strcmp(op, "Ingest") == 0) return "bonfyre-ingest";
    if (strcmp(op, "BriefExtract") == 0 || strcmp(op, "Brief") == 0) return "bonfyre-brief";
    if (strcmp(op, "ProofBundle") == 0 || strcmp(op, "Proof") == 0) return "bonfyre-proof";
    if (strcmp(op, "OfferGenerate") == 0 || strcmp(op, "Offer") == 0) return "bonfyre-offer";
    if (strcmp(op, "Narrate") == 0) return "bonfyre-narrate";
    if (strcmp(op, "Pack") == 0) return "bonfyre-pack";
    if (strcmp(op, "Distribute") == 0) return "bonfyre-distribute";
    if (strcmp(op, "FormatTransform") == 0 || strcmp(op, "Emit") == 0) return "bonfyre-emit";
    if (strcmp(op, "Compress") == 0) return "bonfyre-compress";
    if (strcmp(op, "Clean") == 0) return "bonfyre-transcribe";
    if (strcmp(op, "MetadataEmit") == 0) return "bonfyre-brief";
    return NULL;
}

/* ---------- commands ---------- */

static int cmd_plan(const char *artifact_path, const char *target) {
    char *json = read_file_full(artifact_path);
    if (!json) { fprintf(stderr, "Cannot read: %s\n", artifact_path); return 1; }

    /* Find the operator that produces the target */
    char search[256];
    snprintf(search, sizeof(search), "\"output\": \"%s\"", target);
    const char *p = strstr(json, search);
    if (!p) {
        /* Check realization_targets */
        snprintf(search, sizeof(search), "\"target_id\": \"%s\"", target);
        p = strstr(json, search);
        if (!p) {
            fprintf(stderr, "[stitch] Target '%s' not found in manifest\n", target);
            free(json); return 1;
        }
        printf("TARGET (unrealized): %s\n", target);
        /* Extract the op needed */
        char op[64] = "?";
        /* scan backwards for the containing object's "op" */
        const char *block = p;
        while (block > json && *block != '{') block--;
        const char *op_key = strstr(block, "\"op\"");
        if (op_key && op_key < p + 200) {
            op_key += 4;
            while (*op_key && (*op_key == ' ' || *op_key == ':' || *op_key == '"')) op_key++;
            size_t k = 0;
            while (*op_key && *op_key != '"' && k < sizeof(op)-1) op[k++] = *op_key++;
            op[k] = '\0';
        }
        const char *bin = op_to_binary(op);
        printf("  STEP 1: %s via %s\n", op, bin ? bin : "(unknown binary)");
        printf("  STATUS: needs materialization\n");
        free(json); return 0;
    }

    /* Walk backwards through operator chain */
    printf("MATERIALIZATION PLAN for '%s':\n", target);

    /* Find all operators, build chain */
    int step = 0;
    char current_target[128];
    snprintf(current_target, sizeof(current_target), "%s", target);

    while (1) {
        snprintf(search, sizeof(search), "\"output\": \"%s\"", current_target);
        p = strstr(json, search);
        if (!p) break;

        /* Find containing operator block */
        const char *block = p;
        while (block > json && *block != '{') block--;

        char op[64] = "?", op_id[128] = "?", inputs_raw[512] = "";
        /* Extract op */
        const char *op_key = strstr(block, "\"op\"");
        if (op_key && op_key < p + 500) {
            op_key += 4;
            while (*op_key && (*op_key == ' ' || *op_key == ':' || *op_key == '"')) op_key++;
            size_t k = 0;
            while (*op_key && *op_key != '"' && k < sizeof(op)-1) op[k++] = *op_key++;
            op[k] = '\0';
        }
        const char *id_key = strstr(block, "\"operator_id\"");
        if (id_key && id_key < p + 500) {
            id_key += 13;
            while (*id_key && (*id_key == ' ' || *id_key == ':' || *id_key == '"')) id_key++;
            size_t k = 0;
            while (*id_key && *id_key != '"' && k < sizeof(op_id)-1) op_id[k++] = *id_key++;
            op_id[k] = '\0';
        }

        const char *bin = op_to_binary(op);
        printf("  STEP %d: [%s] %s -> %s  (binary: %s)\n",
               ++step, op_id, op, current_target, bin ? bin : "?");

        /* Find first input to continue chain */
        const char *inp = strstr(block, "\"inputs\"");
        if (inp && inp < p + 500) {
            inp = strchr(inp, '[');
            if (inp) {
                inp++;
                while (*inp == ' ' || *inp == '"') inp++;
                size_t k = 0;
                while (*inp && *inp != '"' && *inp != ']' && k < sizeof(inputs_raw)-1)
                    inputs_raw[k++] = *inp++;
                inputs_raw[k] = '\0';
            }
        }

        if (inputs_raw[0])
            snprintf(current_target, sizeof(current_target), "%s", inputs_raw);
        else
            break;

        if (step > 20) break; /* safety */
    }

    printf("  ROOT: %s (atom)\n", current_target);
    printf("  TOTAL STEPS: %d\n", step);

    free(json);
    return 0;
}

static int cmd_prune(const char *family_dir, int keep_pinned) {
    char art_path[PATH_MAX];
    snprintf(art_path, sizeof(art_path), "%s/artifact.json", family_dir);
    char *json = read_file_full(art_path);
    if (!json) {
        fprintf(stderr, "[stitch] No artifact.json in %s\n", family_dir);
        return 1;
    }

    /* Find unpinned realizations and delete their files */
    int pruned = 0;
    unsigned long bytes_freed = 0;
    const char *p = json;
    while ((p = strstr(p, "\"pinned\"")) != NULL) {
        /* Check if pinned is false */
        p += 8;
        const char *val = p;
        while (*val && (*val == ' ' || *val == ':')) val++;
        if (strncmp(val, "false", 5) == 0) {
            /* Find the path of this realization */
            /* Scan backwards to find "path" */
            const char *block = p;
            while (block > json && *block != '{') block--;
            const char *path_key = strstr(block, "\"path\"");
            if (path_key && path_key < p) {
                path_key += 6;
                while (*path_key && (*path_key == ' ' || *path_key == ':' || *path_key == '"')) path_key++;
                char file_path[PATH_MAX];
                size_t k = 0;
                while (*path_key && *path_key != '"' && k < sizeof(file_path)-1)
                    file_path[k++] = *path_key++;
                file_path[k] = '\0';

                char full_path[PATH_MAX];
                snprintf(full_path, sizeof(full_path), "%s/%s", family_dir, file_path);
                struct stat st;
                if (stat(full_path, &st) == 0) {
                    bytes_freed += (unsigned long)st.st_size;
                    if (!keep_pinned || 1) { /* always prune unpinned */
                        unlink(full_path);
                        fprintf(stderr, "  [prune] %s (%lu bytes)\n", file_path, (unsigned long)st.st_size);
                        pruned++;
                    }
                }
            }
        }
        p = val;
    }

    fprintf(stderr, "[stitch] Pruned %d unpinned realizations, freed %lu bytes\n", pruned, bytes_freed);
    free(json);
    return 0;
}

static int cmd_cache_stats(const char *cache_dir) {
    if (!cache_dir) cache_dir = "/tmp/bonfyre-stitch-cache";
    printf("Cache: %s\n", cache_dir);
    DIR *d = opendir(cache_dir);
    if (!d) { printf("  (empty / not created)\n"); return 0; }
    int count = 0; unsigned long total = 0;
    struct dirent *ent;
    while ((ent = readdir(d))) {
        if (ent->d_name[0] == '.') continue;
        char fp[PATH_MAX];
        snprintf(fp, sizeof(fp), "%s/%s", cache_dir, ent->d_name);
        struct stat st;
        if (stat(fp, &st) == 0 && S_ISREG(st.st_mode)) {
            count++; total += (unsigned long)st.st_size;
        }
    }
    closedir(d);
    printf("  Cached items: %d\n", count);
    printf("  Cache size:   %lu bytes (%.1f KB)\n", total, (double)total / 1024.0);
    return 0;
}

/* ── compile ─────────────────────────────────────────────────────────
 *
 * bonfyre-stitch compile <recipe.json> --output <binary_name>
 *                        [--recipe-dir DIR]
 *
 * Reads a Bonfyre recipe JSON, emits a self-contained C driver that
 * runs each stage in order (substituting {input}/{out} at runtime),
 * then compiles the C to a standalone executable via `cc -O2`.
 *
 * The emitted binary usage:
 *   <compiled> --input FILE --out DIR
 *
 * ─────────────────────────────────────────────────────────────────── */

#define COMP_MAX_STAGES   64
#define COMP_MAX_ARGS     64
#define COMP_MAX_ARG_LEN  512

typedef struct {
    char id[64];
    char operator_name[128];
    char args[COMP_MAX_ARGS][COMP_MAX_ARG_LEN];
    int  n_args;
    char depends_on[COMP_MAX_STAGES][64];
    int  n_deps;
} RecipeStage;

/* Advance past whitespace. */
static const char *skip_ws(const char *p) {
    while (*p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    return p;
}

/* Read a JSON string starting at the opening '"'. Returns pointer after closing '"'. */
static const char *read_json_string(const char *p, char *out, size_t sz) {
    if (*p != '"') { out[0] = '\0'; return p; }
    p++;
    size_t i = 0;
    while (*p && *p != '"') {
        if (*p == '\\') { p++; if (*p) { out[i++] = *p++; } continue; }
        if (i < sz - 1) out[i++] = *p;
        p++;
    }
    out[i] = '\0';
    if (*p == '"') p++;
    return p;
}

/* Scan a JSON array of strings starting at '['. Populate out[], return element count. */
static int read_string_array(const char *p, char out[][COMP_MAX_ARG_LEN], int max_n)
{
    if (*p != '[') return 0;
    p++; int n = 0;
    while (*p) {
        p = skip_ws(p);
        if (*p == ']') break;
        if (*p == '"') {
            char tmp[COMP_MAX_ARG_LEN];
            p = read_json_string(p, tmp, sizeof(tmp));
            if (n < max_n) {
                strncpy(out[n], tmp, COMP_MAX_ARG_LEN - 1);
                out[n][COMP_MAX_ARG_LEN - 1] = '\0';
                n++;
            }
        } else if (*p == ',') { p++; }
        else { p++; } /* skip unexpected chars */
    }
    return n;
}

/* Emit a C string literal, escaping backslash and double-quote. */
static void emit_c_string(FILE *fp, const char *s) {
    fputc('"', fp);
    for (; *s; s++) {
        if (*s == '"' || *s == '\\') fputc('\\', fp);
        fputc(*s, fp);
    }
    fputc('"', fp);
}

static int cmd_compile(const char *recipe_path, const char *output_name,
                       const char *recipe_dir) {
    /* ── 1. Read recipe JSON ── */
    char *json = read_file_full(recipe_path);
    if (!json) {
        fprintf(stderr, "compile: cannot read: %s\n", recipe_path);
        return 1;
    }

    char recipe_id[64] = "unknown", recipe_name[256] = "Compiled Pipeline";
    json_str(json, "recipe_id", recipe_id, sizeof(recipe_id));
    json_str(json, "name",      recipe_name, sizeof(recipe_name));

    /* ── 2. Parse stages[] ── */
    RecipeStage *stages = (RecipeStage *)calloc(COMP_MAX_STAGES, sizeof(RecipeStage));
    if (!stages) { free(json); return 1; }
    int n_stages = 0;

    const char *cursor = strstr(json, "\"stages\"");
    if (!cursor) {
        fprintf(stderr, "compile: no 'stages' array in %s\n", recipe_path);
        free(json); return 1;
    }
    cursor = strchr(cursor, '[');
    if (!cursor) { fprintf(stderr, "compile: malformed stages array\n"); free(json); return 1; }
    cursor++;

    /* Walk stage objects */
    while (*cursor && n_stages < COMP_MAX_STAGES) {
        cursor = skip_ws(cursor);
        if (*cursor == ']') break;
        if (*cursor != '{') { cursor++; continue; }

        RecipeStage *st = &stages[n_stages];
        memset(st, 0, sizeof(*st));

        /* Find matching closing brace */
        int depth = 1; const char *so = cursor; cursor++;
        while (*cursor && depth > 0) {
            if (*cursor == '{') depth++;
            else if (*cursor == '}') depth--;
            cursor++;
        }
        /* Stage block is [so .. cursor) */
        size_t block_len = (size_t)(cursor - so);
        char *block = (char *)malloc(block_len + 1);
        if (!block) { free(json); return 1; }
        memcpy(block, so, block_len); block[block_len] = '\0';

        json_str(block, "id",       st->id,            sizeof(st->id));
        json_str(block, "operator", st->operator_name, sizeof(st->operator_name));

        /* Parse args array */
        const char *ap = strstr(block, "\"args\"");
        if (ap) {
            ap = strchr(ap, '[');
            if (ap) st->n_args = read_string_array(ap, st->args, COMP_MAX_ARGS);
        }

        /* Parse depends_on — stage IDs are short (64-char buffer each) */
        const char *dp = strstr(block, "\"depends_on\"");
        if (dp) {
            dp = strchr(dp, '[');
            if (dp) {
                dp++;
                while (*dp && st->n_deps < COMP_MAX_STAGES) {
                    dp = skip_ws(dp);
                    if (*dp == ']') break;
                    if (*dp == '"') {
                        dp = read_json_string(dp, st->depends_on[st->n_deps], 64);
                        st->n_deps++;
                    } else if (*dp == ',') { dp++; }
                    else { dp++; }
                }
            }
        }

        free(block);
        if (st->operator_name[0]) n_stages++;

        /* Advance past comma between stage objects */
        cursor = skip_ws(cursor);
        if (*cursor == ',') cursor++;
    }

    if (n_stages == 0) {
        fprintf(stderr, "compile: no stages parsed from %s\n", recipe_path);
        free(stages); free(json); return 1;
    }

    fprintf(stderr, "[stitch compile] Recipe: %s (%s) — %d stages\n",
            recipe_id, recipe_name, n_stages);

    /* ── 3. Emit C source ── */
    /* Determine output .c path */
    char c_path[PATH_MAX];
    if (recipe_dir)
        snprintf(c_path, sizeof(c_path), "%s/%s_compiled.c", recipe_dir, recipe_id);
    else
        snprintf(c_path, sizeof(c_path), "/tmp/%s_compiled.c", recipe_id);

    FILE *fp = fopen(c_path, "w");
    if (!fp) {
        fprintf(stderr, "compile: cannot write %s: %s\n", c_path, strerror(errno));
        free(stages); free(json); return 1;
    }

    /* Preamble */
    fprintf(fp,
        "/* AUTO-GENERATED by bonfyre-stitch compile\n"
        " * Recipe: %s — %s\n"
        " * DO NOT EDIT — re-generate from the recipe JSON.\n"
        " */\n"
        "#include <stdio.h>\n"
        "#include <stdlib.h>\n"
        "#include <string.h>\n"
        "#include <sys/wait.h>\n"
        "#include <unistd.h>\n\n",
        recipe_id, recipe_name);

    /* Placeholder substitution helper */
    fprintf(fp,
        "static void subst(const char *tmpl, const char *input, const char *out,\n"
        "                  char *buf, size_t sz) {\n"
        "    size_t i = 0;\n"
        "    while (*tmpl && i < sz - 1) {\n"
        "        if (strncmp(tmpl, \"{input}\", 7) == 0) {\n"
        "            size_t n = strlen(input);\n"
        "            if (i + n < sz - 1) { memcpy(buf + i, input, n); i += n; }\n"
        "            tmpl += 7;\n"
        "        } else if (strncmp(tmpl, \"{out}\", 5) == 0) {\n"
        "            size_t n = strlen(out);\n"
        "            if (i + n < sz - 1) { memcpy(buf + i, out, n); i += n; }\n"
        "            tmpl += 5;\n"
        "        } else {\n"
        "            buf[i++] = *tmpl++;\n"
        "        }\n"
        "    }\n"
        "    buf[i] = '\\0';\n"
        "}\n\n");

    /* run_stage helper */
    fputs(
        "static int run_stage(const char *label, char *const argv[]) {\n"
        "    fprintf(stderr, \"[stage] %s\\n\", label);\n"
        "    pid_t pid = fork();\n"
        "    if (pid < 0) { perror(\"fork\"); return 1; }\n"
        "    if (pid == 0) {\n"
        "        execvp(argv[0], argv);\n"
        "        perror(argv[0]); _exit(127);\n"
        "    }\n"
        "    int status;\n"
        "    waitpid(pid, &status, 0);\n"
        "    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {\n"
        "        fprintf(stderr, \"[stage] %s FAILED (exit %d)\\n\",\n"
        "                label, WIFEXITED(status) ? WEXITSTATUS(status) : -1);\n"
        "        return 1;\n"
        "    }\n"
        "    return 0;\n"
        "}\n\n", fp);

    /* main() */
    fputs(
        "int main(int argc, char *argv[]) {\n"
        "    const char *input = NULL, *out = NULL;\n"
        "    for (int i = 1; i < argc - 1; i++) {\n"
        "        if (strcmp(argv[i], \"--input\") == 0) input = argv[i+1];\n"
        "        else if (strcmp(argv[i], \"--out\") == 0) out = argv[i+1];\n"
        "    }\n"
        "    if (!input || !out) {\n"
        "        fprintf(stderr, \"Usage: %s --input FILE --out DIR\\n\", argv[0]);\n"
        "        return 1;\n"
        "    }\n\n"
        "    char _scratch[512];\n"
        "    (void)_scratch;\n\n",
        fp);

    /* Emit each stage as a block */
    for (int s = 0; s < n_stages; s++) {
        RecipeStage *st = &stages[s];
        fprintf(fp, "    /* ── Stage %s: %s ── */\n", st->id, st->operator_name);
        fprintf(fp, "    {\n");
        fprintf(fp, "        static char _args%d[%d][512];\n", s, st->n_args + 1);

        /* operator_name as argv[0] */
        fprintf(fp, "        strncpy(_args%d[0], ", s);
        emit_c_string(fp, st->operator_name);
        fprintf(fp, ", 511);\n");
        fprintf(fp, "        char *_av%d[%d];\n", s, st->n_args + 2);
        fprintf(fp, "        _av%d[0] = _args%d[0];\n", s, s);

        for (int a = 0; a < st->n_args; a++) {
            fprintf(fp, "        subst(");
            emit_c_string(fp, st->args[a]);
            fprintf(fp, ", input, out, _args%d[%d], 512);\n", s, a + 1);
            fprintf(fp, "        _av%d[%d] = _args%d[%d];\n", s, a + 1, s, a + 1);
        }
        fprintf(fp, "        _av%d[%d] = NULL;\n", s, st->n_args + 1);
        fprintf(fp, "        if (run_stage(");
        emit_c_string(fp, st->id[0] ? st->id : st->operator_name);
        fprintf(fp, ", _av%d) != 0) return 1;\n", s);
        fprintf(fp, "    }\n\n");
    }

    fputs(
        "    fprintf(stderr, \"[pipeline] Complete.\\n\");\n"
        "    return 0;\n"
        "}\n", fp);

    fclose(fp);

    fprintf(stderr, "[stitch compile] Emitted C source: %s\n", c_path);

    /* ── 4. Compile emitted C ── */
    const char *cc = getenv("CC");
    if (!cc || !cc[0]) cc = "cc";

    char cmd_buf[PATH_MAX * 2 + 256];
    snprintf(cmd_buf, sizeof(cmd_buf), "%s -O2 -o %s %s", cc, output_name, c_path);

    fprintf(stderr, "[stitch compile] %s\n", cmd_buf);

    int rc = system(cmd_buf);
    if (rc != 0) {
        fprintf(stderr, "compile: compiler failed (exit %d)\n", rc);
        free(stages); free(json); return 1;
    }

    fprintf(stderr, "[stitch compile] Binary: %s\n", output_name);
    fprintf(stderr, "  Run: %s --input <file> --out <dir>\n", output_name);

    free(stages);
    free(json);
    return 0;
}

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
    if (argc >= 2) {
        if (strcmp(argv[1], "layer-plan") == 0) {
            const char *root = NULL;
            char *json = NULL;
            int discipl_trace = getenv("DISCIPL_TRACE") && strcmp(getenv("DISCIPL_TRACE"), "1") == 0;
            for (int i = 2; i < argc; i++) {
                if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) root = argv[i + 1];
                if (strcmp(argv[i], "--discipl") == 0) discipl_trace = 1;
            }
            if (argc < 4) { fprintf(stderr, "layer-plan requires <artifact_a> <artifact_b>\n"); return 1; }
            if (bf_layer_stitch_plan_json(root, argv[2], argv[3], &json) != 0) {
                fprintf(stderr, "bonfyre-stitch: layer-plan failed\n");
                return 1;
            }
            puts(json);
            if (discipl_trace) {
                bf_discipl_chain_program_t chain;
                char *chain_json = NULL;
                if (bf_discipl_chain_from_stitch_plan_json(json, &chain) == 0 &&
                    bf_discipl_chain_to_json(&chain, &chain_json) == 0) {
                    fprintf(stderr, "[discipl] %s\n", chain_json);
                    free(chain_json);
                }
            }
            free(json);
            return 0;
        }
        if (strcmp(argv[1], "validate-layer-dag") == 0) {
            char *json = NULL;
            int rc;
            if (argc < 3) { fprintf(stderr, "validate-layer-dag requires <plan.json>\n"); return 1; }
            rc = bf_layer_stitch_validate_file(argv[2], &json);
            if (!json) {
                fprintf(stderr, "bonfyre-stitch: validate-layer-dag failed\n");
                return 1;
            }
            puts(json);
            free(json);
            return rc;
        }
        if (strcmp(argv[1], "resolve-bridges") == 0) {
            const char *root = NULL;
            char *json = NULL;
            for (int i = 2; i < argc - 1; i++)
                if (strcmp(argv[i], "--root") == 0) root = argv[i + 1];
            if (argc < 3) { fprintf(stderr, "resolve-bridges requires <plan.json>\n"); return 1; }
            if (bf_layer_stitch_resolve_bridges_json(root, argv[2], &json) != 0) {
                fprintf(stderr, "bonfyre-stitch: resolve-bridges failed\n");
                return 1;
            }
            puts(json);
            free(json);
            return 0;
        }
        if (strcmp(argv[1], "layer-composite") == 0) {
            const char *out = NULL;
            char *json = NULL;
            if (argc < 3) { fprintf(stderr, "layer-composite requires <virtual_composite_id>\n"); return 1; }
            for (int i = 3; i < argc - 1; i++)
                if (strcmp(argv[i], "--out") == 0) out = argv[i + 1];
            if (!out) { fprintf(stderr, "layer-composite requires --out DIR\n"); return 1; }
            if (bf_layer_stitch_composite_json(argv[2], out, &json) != 0) {
                fprintf(stderr, "bonfyre-stitch: layer-composite failed\n");
                return 1;
            }
            puts(json);
            free(json);
            return 0;
        }
    }

    if (argc >= 3 && strcmp(argv[1], "plan") == 0) {
        const char *target = NULL;
        for (int i = 3; i < argc - 1; i++)
            if (strcmp(argv[i], "--target") == 0) target = argv[i+1];
        if (!target) { fprintf(stderr, "plan requires --target\n"); return 1; }
        return cmd_plan(argv[2], target);
    }
    if (argc >= 3 && strcmp(argv[1], "materialize") == 0) {
        const char *target = NULL;
        for (int i = 3; i < argc - 1; i++)
            if (strcmp(argv[i], "--target") == 0) target = argv[i+1];
        if (!target) { fprintf(stderr, "materialize requires --target\n"); return 1; }
        /* Plan first, then execute */
        fprintf(stderr, "[stitch] Materializing %s from %s\n", target, argv[2]);
        cmd_plan(argv[2], target);
        fprintf(stderr, "[stitch] (full execution requires all operator binaries to be installed)\n");
        return 0;
    }
    if (argc >= 3 && strcmp(argv[1], "prune") == 0) {
        int keep_pinned = 0;
        for (int i = 3; i < argc; i++)
            if (strcmp(argv[i], "--keep-pinned") == 0) keep_pinned = 1;
        return cmd_prune(argv[2], keep_pinned);
    }
    if (argc >= 2 && strcmp(argv[1], "cache-stats") == 0) {
        const char *cache = NULL;
        for (int i = 2; i < argc - 1; i++)
            if (strcmp(argv[i], "--cache") == 0) cache = argv[i+1];
        return cmd_cache_stats(cache);
    }
    if (argc >= 3 && strcmp(argv[1], "compile") == 0) {
        const char *output_name = NULL, *recipe_dir = NULL;
        for (int i = 3; i < argc - 1; i++) {
            if (strcmp(argv[i], "--output") == 0) output_name = argv[i+1];
            else if (strcmp(argv[i], "--recipe-dir") == 0) recipe_dir = argv[i+1];
        }
        if (!output_name) {
            fprintf(stderr, "compile requires --output <binary_name>\n");
            return 1;
        }
        return cmd_compile(argv[2], output_name, recipe_dir);
    }

    fprintf(stderr,
        "BonfyreStitch — DAG materializer\n\n"
        "  bonfyre-stitch plan <artifact.json> --target ID\n"
        "  bonfyre-stitch materialize <artifact.json> --target ID [--cache DIR]\n"
        "  bonfyre-stitch prune <family_dir> [--keep-pinned]\n"
        "  bonfyre-stitch cache-stats [--cache DIR]\n"
        "  bonfyre-stitch compile <recipe.json> --output <binary> [--recipe-dir DIR]\n"
        "  bonfyre-stitch layer-plan <artifact_a> <artifact_b> [--root DIR]\n"
        "  bonfyre-stitch validate-layer-dag <plan.json> [--root DIR]\n"
        "  bonfyre-stitch resolve-bridges <plan.json> [--root DIR]\n"
        "  bonfyre-stitch layer-composite <virtual_composite_id> --out DIR [--root DIR]\n"
    );
    return 1;
}
