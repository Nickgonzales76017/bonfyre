/**
 * main.c
 *
 * BonfyreFrappeCompiler — Universal Uplift Runtime Compiler
 *
 * Compiles Frappe applications into dependent, stratified, proof-carrying
 * business computation runtime.
 */

#include "bonfyre_frappe.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>

static void print_usage(const char *prog) {
    printf("BonfyreFrappeCompiler — Universal Uplift Runtime Compiler\n\n");
    printf("Usage: %s [options] <app_path>\n\n", prog);
    printf("Options:\n");
    printf("  -o, --output DIR         Output directory (default: ./output)\n");
    printf("  -r, --root DIR           Bonfyre root directory (default: $BONFYRE_ROOT or ..)\n");
    printf("  --dry-run                Parse and analyze only, don't generate output\n");
    printf("  --emit-schema-ir FILE    Emit schema intermediate representation JSON\n");
    printf("  --emit-bindings FILE     Emit binding derivations JSON\n");
    printf("  --emit-rule-universes FILE  Emit rule universes JSON\n");
    printf("  --emit-pack FILE         Emit complete app pack\n");
    printf("  --phase PHASE            Run up to specified phase (0-12)\n");
    printf("  -v, --verbose            Verbose output\n");
    printf("  -h, --help               Show this help\n");
    printf("\n");
    printf("Phases:\n");
    printf("  0  - Inventory\n");
    printf("  1  - Schema graph build\n");
    printf("  2  - Dependent type elaboration\n");
    printf("  3  - Signal extraction\n");
    printf("  4  - Binding derivation (Datalog)\n");
    printf("  5  - Type elaboration\n");
    printf("  6  - Proof elaboration\n");
    printf("  7  - Rule universe derivation\n");
    printf("  8  - Rule checks\n");
    printf("  9  - Route/surface generation\n");
    printf("  10 - Storage optimization\n");
    printf("  11 - Semantic substrate build\n");
    printf("  12 - Coverage and theorem audit\n");
    printf("\n");
    printf("Example:\n");
    printf("  %s -o ./pack_output ~/frappe-bench/apps/erpnext\n", prog);
    printf("  %s --phase 4 --emit-bindings bindings.json ~/frappe-bench/apps/frappe\n", prog);
    printf("\n");
}

int main(int argc, char **argv) {
    char *app_path = NULL;
    char *output_dir = "output";
    char *bonfyre_root = NULL;
    char *emit_schema_ir = NULL;
    char *emit_bindings = NULL;
    char *emit_rule_universes = NULL;
    char *emit_pack = NULL;
    int max_phase = 12;
    bool dry_run = false;
    bool verbose __attribute__((unused)) = false;

    /* Parse options */
    static struct option long_options[] = {
        {"output", required_argument, 0, 'o'},
        {"root", required_argument, 0, 'r'},
        {"dry-run", no_argument, 0, 1},
        {"emit-schema-ir", required_argument, 0, 2},
        {"emit-bindings", required_argument, 0, 3},
        {"emit-rule-universes", required_argument, 0, 4},
        {"emit-pack", required_argument, 0, 5},
        {"phase", required_argument, 0, 6},
        {"verbose", no_argument, 0, 'v'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    int option_index = 0;

    while ((opt = getopt_long(argc, argv, "o:r:vh", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'o':
                output_dir = optarg;
                break;
            case 'r':
                bonfyre_root = optarg;
                break;
            case 'v':
                verbose = true;
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            case 1:  /* --dry-run */
                dry_run = true;
                break;
            case 2:  /* --emit-schema-ir */
                emit_schema_ir = optarg;
                break;
            case 3:  /* --emit-bindings */
                emit_bindings = optarg;
                break;
            case 4:  /* --emit-rule-universes */
                emit_rule_universes = optarg;
                break;
            case 5:  /* --emit-pack */
                emit_pack = optarg;
                break;
            case 6:  /* --phase */
                max_phase = atoi(optarg);
                if (max_phase < 0 || max_phase > 12) {
                    fprintf(stderr, "Error: Phase must be 0-12\n");
                    return 1;
                }
                break;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    /* Get app path */
    if (optind >= argc) {
        fprintf(stderr, "Error: Missing app_path argument\n\n");
        print_usage(argv[0]);
        return 1;
    }

    app_path = argv[optind];

    /* Determine bonfyre_root */
    if (!bonfyre_root) {
        bonfyre_root = getenv("BONFYRE_ROOT");
        if (!bonfyre_root) {
            bonfyre_root = "..";
        }
    }

    /* Print configuration */
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("  BonfyreFrappeCompiler — Universal Uplift Runtime\n");
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("\n");
    printf("Configuration:\n");
    printf("  App path:      %s\n", app_path);
    printf("  Bonfyre root:  %s\n", bonfyre_root);
    printf("  Output dir:    %s\n", output_dir);
    printf("  Max phase:     %d\n", max_phase);
    printf("  Dry run:       %s\n", dry_run ? "yes" : "no");
    printf("\n");

    /* Check app path exists */
    if (access(app_path, R_OK) != 0) {
        fprintf(stderr, "Error: Cannot access app path: %s\n", app_path);
        return 1;
    }

    /* Run compilation */
    setenv("BONFYRE_FRAPPE_OUTPUT_DIR", output_dir, 1);
    int error_code = 0;
    BfAppPack *pack = bf_compile_app_to_phase(app_path, bonfyre_root, max_phase, &error_code);

    if (error_code != 0) {
        fprintf(stderr, "\n✗ Compilation failed with error code: %d\n", error_code);
        return 1;
    }

    if (!pack) {
        fprintf(stderr, "\n✗ Compilation failed: pack is NULL\n");
        return 1;
    }

    /* Emit outputs if requested */
    if (emit_schema_ir) {
        printf("\nEmitting schema IR to: %s\n", emit_schema_ir);
        /* TODO: Serialize schema to JSON */
    }

    if (emit_bindings) {
        printf("\nEmitting bindings to: %s\n", emit_bindings);
        /* TODO: Serialize bindings to JSON */
    }

    if (emit_rule_universes) {
        printf("\nEmitting rule universes to: %s\n", emit_rule_universes);
        /* TODO: Serialize rule universes to JSON */
    }

    if (emit_pack) {
        printf("\nEmitting app pack to: %s\n", emit_pack);
        /* TODO: Serialize complete pack */
    }

    /* Success */
    printf("\n═══════════════════════════════════════════════════════════════\n");
    printf("  ✓ Compilation Complete\n");
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("\n");
    printf("Summary:\n");
    printf("  App:           %s\n", pack->schema ? pack->schema->app_name : "unknown");
    printf("  DocTypes:      %zu\n", pack->schema ? pack->schema->doctype_count : 0);
    printf("  Bindings:      %zu\n", pack->binding_count);
    printf("  Families:      %zu\n", pack->family_count);
    printf("  Relations:     %zu\n", pack->relation_count);
    printf("  Rule universes: %zu\n", pack->rule_universe_count);
    printf("  Routes:        %zu\n", pack->route_count);
    printf("  Surfaces:      %zu\n", pack->surface_count);
    printf("  Coverage:      %.1f%%\n", pack->coverage_score * 100.0);
    printf("\n");

    /* Cleanup */
    bf_app_pack_free(pack);

    return 0;
}
