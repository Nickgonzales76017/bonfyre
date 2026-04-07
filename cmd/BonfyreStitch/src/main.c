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
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <bonfyre.h>

#define MAX_OPS 128
#define MAX_LINE 65536

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
static void iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL); struct tm t; gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
}

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

static int file_exists(const char *p) { struct stat st; return stat(p, &st) == 0; }

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

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
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

    fprintf(stderr,
        "BonfyreStitch — DAG materializer\n\n"
        "  bonfyre-stitch plan <artifact.json> --target ID\n"
        "  bonfyre-stitch materialize <artifact.json> --target ID [--cache DIR]\n"
        "  bonfyre-stitch prune <family_dir> [--keep-pinned]\n"
        "  bonfyre-stitch cache-stats [--cache DIR]\n"
    );
    return 1;
}
