/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * BonfyreLedger — value accounting engine.
 *
 * Tracks accumulated value of every artifact family:
 *   - atom count, operator count, realization count
 *   - total bytes (raw & compressed)
 *   - compression savings
 *   - metered revenue
 *   - reuse multiplier (how many families share the same atoms/ops)
 *   - estimated replacement cost
 *
 * Rolls up into a portfolio-level ledger: "the vault is worth $X."
 *
 * Usage:
 *   bonfyre-ledger assess <artifact.json>         — value a single family
 *   bonfyre-ledger portfolio <root_dir>            — roll up everything
 *   bonfyre-ledger delta <artifact.json> [--since DATE]  — value added since date
 *   bonfyre-ledger export <root_dir> [--format json|csv] — full export
 */
#include <errno.h>
#include "bf_lex.h"
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "bonfyre.h"
#include <dirent.h>
#include <sqlite3.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static const char *layeros_binary(void) {
    return "layeros/bin/bonfyre-layeros";
}

static int run_execvp(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return 1;
    }
    if (pid == 0) {
        execvp(argv[0], argv);
        perror("execvp");
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        perror("waitpid");
        return 1;
    }
    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}

static int delegate_layeros_ledger(int argc, char *argv[]) {
    char *exec_argv[16];
    int n = 0;
    exec_argv[n++] = (char *)layeros_binary();
    const char *root = NULL;
    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--root") == 0) {
            root = argv[i + 1];
            break;
        }
    }
    if (root) {
        exec_argv[n++] = "--root";
        exec_argv[n++] = (char *)root;
    }
    exec_argv[n++] = "ledger";
    exec_argv[n++] = argv[2];
    exec_argv[n] = NULL;
    return run_execvp(exec_argv);
}

/* What a family cost is whatever cost was observed and recorded for the recipe
 * that made it. This organ used to price a family from constants of its own --
 * a dollar a megabyte, fifty cents an operator, double for being packaged --
 * and report the product as what the vault was worth. A figure nobody observed
 * is not a cost, so with no recorded cost the answer is unknown. */
static const char *g_recipe = NULL;       /* --recipe: the recipe whose costs are this family's */
static const char *g_economy_db = NULL;   /* --economy-db, else BONFYRE_ECONOMY_DB, else the home store */

typedef struct {
    int atom_count;
    int op_count;
    int realization_count;
    int realization_target_count;
    unsigned long raw_bytes;
    unsigned long compressed_bytes;
    double metered_revenue;
    double observed_cost;   /* sum of recorded costs for the named recipe */
    int    cost_known;      /* 0 when no recipe was named or no cost was recorded */
    long   cost_records;
    char family_id[256];
} FamilyValue;

static char *read_file_full(const char *path) {
    return bf_read_file(path, NULL);
}

/* Count occurrences of a substring */
static int count_str(const char *hay, const char *needle) {
    int c = 0; const char *p = hay;
    size_t nlen = strlen(needle);
    while ((p = strstr(p, needle))) { c++; p += nlen; }
    return c;
}

/* Sum file sizes under a directory */
static unsigned long dir_size(const char *path) {
    unsigned long total = 0;
    DIR *d = opendir(path);
    if (!d) return 0;
    struct dirent *ent;
    while ((ent = readdir(d))) {
        if (ent->d_name[0] == '.') continue;
        char fp[PATH_MAX];
        snprintf(fp, sizeof(fp), "%s/%s", path, ent->d_name);
        struct stat st;
        if (stat(fp, &st) == 0) {
            if (S_ISREG(st.st_mode)) total += (unsigned long)st.st_size;
            else if (S_ISDIR(st.st_mode)) total += dir_size(fp);
        }
    }
    closedir(d);
    return total;
}

static int json_str(const char *json, const char *key, char *out, size_t sz) {
    /* Delegates to the one implementation. This copy searched for "key" rather
     * than "key": so it matched inside values, and stopped at the first quote
     * so an escaped one truncated the result -- the bug that silently broke
     * conditional re-observation elsewhere. Six binaries carried it; a seventh
     * had been fixed locally and the fix never reached the others. */
    return bf_lex_json_string(json, key, out, sz);
}

/* Sum the costs the economy organ recorded for the named recipe at or after
 * `since` (seconds; 0 for all). Leaves the count at zero when there is no
 * recipe, no store, or no record. */
static void observed_cost_since(long long since, double *usd, long *records) {
    *usd = 0.0; *records = 0;
    if (!g_recipe || !g_recipe[0]) return;
    char path[PATH_MAX];
    const char *named = g_economy_db ? g_economy_db : getenv("BONFYRE_ECONOMY_DB");
    if (named && named[0]) snprintf(path, sizeof(path), "%s", named);
    else {
        const char *home = getenv("HOME");
        if (!home) return;
        snprintf(path, sizeof(path), "%s/.local/share/bonfyre/economy.db", home);
    }
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { if (db) sqlite3_close(db); return; }
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT COALESCE(SUM(usd),0.0), COUNT(*) FROM costs WHERE recipe=?1 AND ts>=?2",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, g_recipe, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, since);
        if (sqlite3_step(st) == SQLITE_ROW) { *usd = sqlite3_column_double(st, 0); *records = (long)sqlite3_column_int64(st, 1); }
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
}

static void print_cost_line(const FamilyValue *fv) {
    if (fv->cost_known)
        printf("│ Replacement cost:  $%.4f observed (%ld records, recipe %s)\n", fv->observed_cost, fv->cost_records, g_recipe);
    else
        printf("│ Replacement cost:  unknown -- %s\n", (g_recipe && g_recipe[0])
               ? "no cost is recorded for that recipe" : "name the recipe with --recipe");
    printf("│ Portfolio value:   not computed -- nothing here evidences a price\n");
}

static FamilyValue assess_family(const char *artifact_path) {
    FamilyValue fv;
    memset(&fv, 0, sizeof(fv));

    char *json = read_file_full(artifact_path);
    if (!json) return fv;

    json_str(json, "artifact_id", fv.family_id, sizeof(fv.family_id));

    fv.atom_count = count_str(json, "\"atom_id\"");
    fv.op_count = count_str(json, "\"operator_id\"");
    fv.realization_count = count_str(json, "\"realization_id\"");
    fv.realization_target_count = count_str(json, "\"target_id\"");

    /* Calculate raw bytes from parent directory */
    char dir[PATH_MAX];
    strncpy(dir, artifact_path, sizeof(dir) - 1);
    dir[sizeof(dir) - 1] = '\0';
    char *last = strrchr(dir, '/');
    if (last) *last = '\0';
    fv.raw_bytes = dir_size(dir);

    /* Check for compressed variants */
    char zst[PATH_MAX];
    snprintf(zst, sizeof(zst), "%s.tar.zst", dir);
    struct stat st;
    if (stat(zst, &st) == 0) fv.compressed_bytes = (unsigned long)st.st_size;

    observed_cost_since(0, &fv.observed_cost, &fv.cost_records);
    fv.cost_known = fv.cost_records > 0;

    free(json);
    return fv;
}

static void print_family_report(const FamilyValue *fv) {
    printf("┌─────────────────────────────────────────┐\n");
    printf("│ FAMILY: %-32s│\n", fv->family_id[0] ? fv->family_id : "(unknown)");
    printf("├─────────────────────────────────────────┤\n");
    printf("│ Atoms:         %5d                     │\n", fv->atom_count);
    printf("│ Operators:     %5d                     │\n", fv->op_count);
    printf("│ Realizations:  %5d                     │\n", fv->realization_count);
    printf("│ Unrealized:    %5d                     │\n", fv->realization_target_count);
    printf("│ Raw bytes:     %12lu              │\n", fv->raw_bytes);
    if (fv->compressed_bytes > 0) {
        double ratio = (double)fv->compressed_bytes / (double)fv->raw_bytes * 100.0;
        printf("│ Compressed:    %12lu (%.1f%%)       │\n", fv->compressed_bytes, ratio);
        printf("│ Savings:       %12lu bytes         │\n", fv->raw_bytes - fv->compressed_bytes);
    }
    printf("├─────────────────────────────────────────┤\n");
    print_cost_line(fv);
    printf("└─────────────────────────────────────────┘\n");
}

static void print_family_json(const FamilyValue *fv) {
    printf("{\n");
    printf("  \"family_id\": \"%s\",\n", fv->family_id);
    printf("  \"atoms\": %d,\n", fv->atom_count);
    printf("  \"operators\": %d,\n", fv->op_count);
    printf("  \"realizations\": %d,\n", fv->realization_count);
    printf("  \"unrealized_targets\": %d,\n", fv->realization_target_count);
    printf("  \"raw_bytes\": %lu,\n", fv->raw_bytes);
    printf("  \"compressed_bytes\": %lu,\n", fv->compressed_bytes);
    if (fv->cost_known) printf("  \"replacement_cost_usd\": %.6f,\n", fv->observed_cost);
    else printf("  \"replacement_cost_usd\": null,\n");
    printf("  \"cost_records\": %ld,\n", fv->cost_records);
    printf("  \"cost_basis\": \"%s\",\n", fv->cost_known ? "costs recorded by the economy organ for the named recipe" : "none");
    printf("  \"portfolio_value_usd\": null\n");
    printf("}\n");
}

/* Recursively find all artifact.json files */
typedef struct { char paths[1024][PATH_MAX]; int count; } ArtifactPaths;
static void find_artifacts(const char *root, ArtifactPaths *ap) {
    DIR *d = opendir(root);
    if (!d) return;
    struct dirent *ent;
    while ((ent = readdir(d))) {
        if (ent->d_name[0] == '.') continue;
        char fp[PATH_MAX];
        snprintf(fp, sizeof(fp), "%s/%s", root, ent->d_name);
        struct stat st;
        if (stat(fp, &st) != 0) continue;
        if (S_ISDIR(st.st_mode)) find_artifacts(fp, ap);
        else if (strcmp(ent->d_name, "artifact.json") == 0 && ap->count < 1024) {
            strncpy(ap->paths[ap->count], fp, PATH_MAX - 1);
            ap->paths[ap->count][PATH_MAX - 1] = '\0';
            ap->count++;
        }
    }
    closedir(d);
}

static int cmd_portfolio(const char *root) {
    ArtifactPaths *ap = calloc(1, sizeof(ArtifactPaths));
    if (!ap) { fprintf(stderr, "malloc fail\n"); return 1; }
    find_artifacts(root, ap);

    if (ap->count == 0) {
        fprintf(stderr, "[ledger] No artifact.json files found under %s\n", root);
        free(ap); return 1;
    }

    int total_atoms = 0, total_ops = 0, total_real = 0, total_unreal = 0;
    unsigned long total_raw = 0, total_comp = 0;

    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║              BONFYRE PORTFOLIO LEDGER                        ║\n");
    printf("╠═══════════════════════════════════════════════════════════════╣\n");

    for (int i = 0; i < ap->count; i++) {
        FamilyValue fv = assess_family(ap->paths[i]);
        printf("║ %-30s %3dA %3dO %3dR  %10lu B ║\n",
               fv.family_id[0] ? fv.family_id : "(unknown)",
               fv.atom_count, fv.op_count, fv.realization_count,
               fv.raw_bytes);
        total_atoms += fv.atom_count;
        total_ops += fv.op_count;
        total_real += fv.realization_count;
        total_unreal += fv.realization_target_count;
        total_raw += fv.raw_bytes;
        total_comp += fv.compressed_bytes;
    }

    printf("╠═══════════════════════════════════════════════════════════════╣\n");
    printf("║ FAMILIES:     %4d                                           ║\n", ap->count);
    printf("║ ATOMS:        %4d   OPERATORS: %4d                         ║\n", total_atoms, total_ops);
    printf("║ REALIZATIONS: %4d   UNREALIZED: %4d                        ║\n", total_real, total_unreal);
    printf("║ RAW STORAGE:  %12lu bytes                                ║\n", total_raw);
    if (total_comp > 0)
    printf("║ COMPRESSED:   %12lu bytes (saved %lu)                    ║\n", total_comp, total_raw - total_comp);
    printf("╠═══════════════════════════════════════════════════════════════╣\n");
    {
        double usd = 0.0; long records = 0;
        observed_cost_since(0, &usd, &records);
        if (records > 0) printf("║ REPLACEMENT COST:    $%.4f observed (%ld records, recipe %s)\n", usd, records, g_recipe);
        else printf("║ REPLACEMENT COST:    unknown -- no observed cost is bound to these families\n");
        printf("║ PORTFOLIO VALUE:     not computed -- nothing here evidences a price\n");
    }
    printf("╚═══════════════════════════════════════════════════════════════╝\n");

    free(ap);
    return 0;
}

static int cmd_export(const char *root, const char *fmt) {
    ArtifactPaths *ap = calloc(1, sizeof(ArtifactPaths));
    if (!ap) { fprintf(stderr, "malloc fail\n"); return 1; }
    find_artifacts(root, ap);

    int is_csv = (fmt && strcmp(fmt, "csv") == 0);
    if (is_csv) {
        printf("family_id,atoms,operators,realizations,unrealized,raw_bytes,compressed_bytes\n");
        for (int i = 0; i < ap->count; i++) {
            FamilyValue fv = assess_family(ap->paths[i]);
            printf("%s,%d,%d,%d,%d,%lu,%lu\n",
                   fv.family_id, fv.atom_count, fv.op_count,
                   fv.realization_count, fv.realization_target_count,
                   fv.raw_bytes, fv.compressed_bytes);
        }
    } else {
        printf("[\n");
        for (int i = 0; i < ap->count; i++) {
            FamilyValue fv = assess_family(ap->paths[i]);
            printf("  {\"family_id\":\"%s\",\"atoms\":%d,\"operators\":%d,"
                   "\"realizations\":%d,\"unrealized\":%d,\"raw_bytes\":%lu,"
                   "\"compressed_bytes\":%lu,\"replacement_cost\":null,"
                   "\"portfolio_value\":null}%s\n",
                   fv.family_id, fv.atom_count, fv.op_count,
                   fv.realization_count, fv.realization_target_count,
                   fv.raw_bytes, fv.compressed_bytes,
                   (i < ap->count - 1) ? "," : "");
        }
        printf("]\n");
    }

    free(ap);
    return 0;
}

static void usage(void) {
    fprintf(stderr,
        "BonfyreLedger — value accounting engine\n\n"
        "  bonfyre-ledger assess <artifact.json>          detailed family report\n"
        "  bonfyre-ledger assess-json <artifact.json>     JSON output\n"
        "  bonfyre-ledger portfolio <root_dir>             full portfolio roll-up\n"
        "  bonfyre-ledger delta <artifact.json> --recipe NAME --since YYYY-MM-DD\n"
        "  --recipe NAME [--economy-db FILE]   bind observed costs; without it cost is unknown\n"
        "  bonfyre-ledger export <root_dir> [--format json|csv]\n"
        "  bonfyre-ledger layer <artifact_id> [--root DIR]\n"
    );
}

int main(int argc, char *argv[]) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    for (int i = 2; i < argc - 1; i++) {
        if (strcmp(argv[i], "--recipe") == 0) g_recipe = argv[i + 1];
        if (strcmp(argv[i], "--economy-db") == 0) g_economy_db = argv[i + 1];
    }
    enum {
        LG_ASSESS, LG_ASSESS_JSON, LG_DELTA, LG_EXPORT, LG_LAYER, LG_PORTFOLIO,
    };
    static const BfCommandEntry ledger_commands[] = {
        {"assess", LG_ASSESS, 0, NULL},
        {"assess-json", LG_ASSESS_JSON, 0, NULL},
        {"delta", LG_DELTA, 0, NULL},
        {"export", LG_EXPORT, 0, NULL},
        {"layer", LG_LAYER, 0, NULL},
        {"portfolio", LG_PORTFOLIO, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(ledger_commands,
        sizeof(ledger_commands) / sizeof(ledger_commands[0]), argv[1]);
    if (argc >= 3 && command && command->id == LG_LAYER) {
        return delegate_layeros_ledger(argc, argv);
    }

    if (argc >= 3 && command && command->id == LG_ASSESS) {
        FamilyValue fv = assess_family(argv[2]);
        if (!fv.family_id[0]) { fprintf(stderr, "[ledger] not a readable artifact family: %s\n", argv[2]); return 1; }
        print_family_report(&fv);
        return 0;
    }
    if (argc >= 3 && command && command->id == LG_ASSESS_JSON) {
        FamilyValue fv = assess_family(argv[2]);
        if (!fv.family_id[0]) { fprintf(stderr, "[ledger] not a readable artifact family: %s\n", argv[2]); return 1; }
        print_family_json(&fv);
        return 0;
    }
    if (argc >= 3 && command && command->id == LG_PORTFOLIO) {
        return cmd_portfolio(argv[2]);
    }
    if (argc >= 3 && command && command->id == LG_DELTA) {
        /* What changed is the cost recorded since a date. It used to print
         * the current snapshot under the heading DELTA. */
        const char *since = NULL;
        for (int i = 3; i < argc - 1; i++) if (strcmp(argv[i], "--since") == 0) since = argv[i + 1];
        int y = 0, mo = 0, d = 0;
        long long since_epoch = -1;
        if (since && sscanf(since, "%4d-%2d-%2d", &y, &mo, &d) == 3 && y >= 1970 && mo >= 1 && mo <= 12 && d >= 1 && d <= 31) {
            /* days from the civil date, then seconds; no library call that a
             * strict C11 build hides */
            int yy = mo <= 2 ? y - 1 : y;
            long era = (yy >= 0 ? yy : yy - 399) / 400;
            long yoe = yy - era * 400;
            long doy = (153 * (mo + (mo > 2 ? -3 : 9)) + 2) / 5 + d - 1;
            long doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
            since_epoch = (long long)(era * 146097 + doe - 719468) * 86400;
        }
        if (!g_recipe || since_epoch < 0) {
            fprintf(stderr, "bonfyre-ledger delta <artifact.json> --recipe NAME --since YYYY-MM-DD\n"
                            "  a delta is the cost recorded for a recipe since a date; both are required\n");
            return 1;
        }
        double usd = 0.0; long records = 0;
        observed_cost_since(since_epoch, &usd, &records);
        printf("recipe %s since %s: $%.6f observed in %ld cost records\n", g_recipe, since, usd, records);
        return 0;
    }
    if (argc >= 3 && command && command->id == LG_EXPORT) {
        const char *fmt = "json";
        for (int i = 3; i < argc - 1; i++)
            if (strcmp(argv[i], "--format") == 0) fmt = argv[i+1];
        return cmd_export(argv[2], fmt);
    }

    usage();
    return 1;
}
