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
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>

/* Per-operation cost-to-replace estimates (USD).
 * These are what it would cost to recreate from scratch. */
#define COST_PER_ATOM_BYTE  0.000001   /* $1/MB raw content */
#define COST_PER_OPERATOR   0.50       /* avg compute cost per operator run */
#define COST_PER_REALIZATION 0.10      /* marginal cost of each output form */
#define COST_REUSE_MULTIPLIER 1.5      /* shared atoms are worth more */
#define COST_BRAND_PREMIUM   2.0       /* packaged, gated, metered = 2x */

typedef struct {
    int atom_count;
    int op_count;
    int realization_count;
    int realization_target_count;
    unsigned long raw_bytes;
    unsigned long compressed_bytes;
    double metered_revenue;
    double replacement_cost;
    double portfolio_value;
    char family_id[256];
} FamilyValue;

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

    /* Replacement cost */
    fv.replacement_cost =
        (double)fv.raw_bytes * COST_PER_ATOM_BYTE +
        (double)fv.op_count * COST_PER_OPERATOR +
        (double)fv.realization_count * COST_PER_REALIZATION;

    /* Value = replacement cost × brand premium */
    fv.portfolio_value = fv.replacement_cost * COST_BRAND_PREMIUM;

    /* Unrealized targets add potential value */
    fv.portfolio_value += (double)fv.realization_target_count * COST_PER_OPERATOR * 0.5;

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
    printf("│ Replacement cost:  $%12.2f          │\n", fv->replacement_cost);
    printf("│ Portfolio value:   $%12.2f          │\n", fv->portfolio_value);
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
    printf("  \"replacement_cost_usd\": %.4f,\n", fv->replacement_cost);
    printf("  \"portfolio_value_usd\": %.4f\n", fv->portfolio_value);
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
    double total_replace = 0, total_value = 0;

    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║              BONFYRE PORTFOLIO LEDGER                        ║\n");
    printf("╠═══════════════════════════════════════════════════════════════╣\n");

    for (int i = 0; i < ap->count; i++) {
        FamilyValue fv = assess_family(ap->paths[i]);
        printf("║ %-30s %3dA %3dO %3dR  $%8.2f ║\n",
               fv.family_id[0] ? fv.family_id : "(unknown)",
               fv.atom_count, fv.op_count, fv.realization_count,
               fv.portfolio_value);
        total_atoms += fv.atom_count;
        total_ops += fv.op_count;
        total_real += fv.realization_count;
        total_unreal += fv.realization_target_count;
        total_raw += fv.raw_bytes;
        total_comp += fv.compressed_bytes;
        total_replace += fv.replacement_cost;
        total_value += fv.portfolio_value;
    }

    printf("╠═══════════════════════════════════════════════════════════════╣\n");
    printf("║ FAMILIES:     %4d                                           ║\n", ap->count);
    printf("║ ATOMS:        %4d   OPERATORS: %4d                         ║\n", total_atoms, total_ops);
    printf("║ REALIZATIONS: %4d   UNREALIZED: %4d                        ║\n", total_real, total_unreal);
    printf("║ RAW STORAGE:  %12lu bytes                                ║\n", total_raw);
    if (total_comp > 0)
    printf("║ COMPRESSED:   %12lu bytes (saved %lu)                    ║\n", total_comp, total_raw - total_comp);
    printf("╠═══════════════════════════════════════════════════════════════╣\n");
    printf("║ REPLACEMENT COST:    $%12.2f                              ║\n", total_replace);
    printf("║ ★ PORTFOLIO VALUE:   $%12.2f                              ║\n", total_value);
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
        printf("family_id,atoms,operators,realizations,unrealized,raw_bytes,compressed_bytes,replacement_cost,portfolio_value\n");
        for (int i = 0; i < ap->count; i++) {
            FamilyValue fv = assess_family(ap->paths[i]);
            printf("%s,%d,%d,%d,%d,%lu,%lu,%.4f,%.4f\n",
                   fv.family_id, fv.atom_count, fv.op_count,
                   fv.realization_count, fv.realization_target_count,
                   fv.raw_bytes, fv.compressed_bytes,
                   fv.replacement_cost, fv.portfolio_value);
        }
    } else {
        printf("[\n");
        for (int i = 0; i < ap->count; i++) {
            FamilyValue fv = assess_family(ap->paths[i]);
            printf("  {\"family_id\":\"%s\",\"atoms\":%d,\"operators\":%d,"
                   "\"realizations\":%d,\"unrealized\":%d,\"raw_bytes\":%lu,"
                   "\"compressed_bytes\":%lu,\"replacement_cost\":%.4f,"
                   "\"portfolio_value\":%.4f}%s\n",
                   fv.family_id, fv.atom_count, fv.op_count,
                   fv.realization_count, fv.realization_target_count,
                   fv.raw_bytes, fv.compressed_bytes,
                   fv.replacement_cost, fv.portfolio_value,
                   (i < ap->count - 1) ? "," : "");
        }
        printf("]\n");
    }

    free(ap);
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc >= 3 && strcmp(argv[1], "assess") == 0) {
        FamilyValue fv = assess_family(argv[2]);
        print_family_report(&fv);
        return 0;
    }
    if (argc >= 3 && strcmp(argv[1], "assess-json") == 0) {
        FamilyValue fv = assess_family(argv[2]);
        print_family_json(&fv);
        return 0;
    }
    if (argc >= 3 && strcmp(argv[1], "portfolio") == 0) {
        return cmd_portfolio(argv[2]);
    }
    if (argc >= 3 && strcmp(argv[1], "delta") == 0) {
        /* For now, delta simply reports current values */
        FamilyValue fv = assess_family(argv[2]);
        printf("DELTA (current snapshot — historical tracking coming):\n");
        print_family_report(&fv);
        return 0;
    }
    if (argc >= 3 && strcmp(argv[1], "export") == 0) {
        const char *fmt = "json";
        for (int i = 3; i < argc - 1; i++)
            if (strcmp(argv[i], "--format") == 0) fmt = argv[i+1];
        return cmd_export(argv[2], fmt);
    }

    fprintf(stderr,
        "BonfyreLedger — value accounting engine\n\n"
        "  bonfyre-ledger assess <artifact.json>          detailed family report\n"
        "  bonfyre-ledger assess-json <artifact.json>     JSON output\n"
        "  bonfyre-ledger portfolio <root_dir>             full portfolio roll-up\n"
        "  bonfyre-ledger delta <artifact.json>            value changes\n"
        "  bonfyre-ledger export <root_dir> [--format json|csv]\n"
    );
    return 1;
}
