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

static void json_string(FILE *file, const char *value) {
    fputc('"', file);
    for (const unsigned char *cursor = (const unsigned char *)(value ? value : ""); *cursor; cursor++) {
        if (*cursor == '"' || *cursor == '\\') fputc('\\', file);
        if (*cursor >= 0x20) fputc(*cursor, file);
    }
    fputc('"', file);
}

static void json_pack_revision(FILE *file, const BfAppPack *pack) {
    static const char digits[] = "0123456789abcdef";
    char revision[65];
    for (size_t index = 0; index < 32; ++index) {
        revision[index * 2] = digits[pack->pack_hash[index] >> 4];
        revision[index * 2 + 1] = digits[pack->pack_hash[index] & 15];
    }
    revision[64] = '\0';
    json_string(file, revision);
}

static void emit_schema_graph(FILE *file, const BfAppPack *pack) {
    fprintf(file, "\"schema\":{\"app\":"); json_string(file, pack->schema ? pack->schema->app_name : pack->app_name);
    fprintf(file, ",\"doctypes\":[");
    for (size_t i = 0; pack->schema && i < pack->schema->doctype_count; i++) {
        BfDocType *doctype = pack->schema->doctypes[i];
        if (i) fputc(',', file);
        fprintf(file, "{\"name\":"); json_string(file, doctype->name);
        fprintf(file, ",\"module\":"); json_string(file, doctype->module);
        fprintf(file, ",\"controller\":"); json_string(file, doctype->controller_path);
        fprintf(file, ",\"fields\":[");
        for (size_t j = 0; j < doctype->field_count; j++) {
            BfField *field = &doctype->fields[j];
            if (j) fputc(',', file);
            fprintf(file, "{\"name\":"); json_string(file, field->fieldname);
            fprintf(file, ",\"label\":"); json_string(file, field->label);
            fprintf(file, ",\"type\":%d,\"required\":%s,\"options\":", (int)field->fieldtype, field->reqd ? "true" : "false");
            json_string(file, field->options); fputc('}', file);
        }
        fprintf(file, "],\"relations\":[");
        for (size_t j = 0; j < doctype->relation_count; j++) {
            BfRelation *relation = &doctype->relations[j];
            if (j) fputc(',', file);
            fprintf(file, "{\"type\":%d,\"source\":", (int)relation->type); json_string(file, relation->source_doctype);
            fprintf(file, ",\"field\":"); json_string(file, relation->source_field);
            fprintf(file, ",\"target\":"); json_string(file, relation->target_doctype); fputc('}', file);
        }
        fprintf(file, "]}");
    }
    fprintf(file, "]}");
}

static void emit_bindings(FILE *file, const BfAppPack *pack) {
    fprintf(file, "\"bindings\":[");
    for (size_t i = 0; i < pack->binding_count; i++) {
        BfBinding *binding = pack->bindings[i];
        if (i) fputc(',', file);
        fprintf(file, "{\"doctype\":"); json_string(file, binding->doctype);
        fprintf(file, ",\"capabilities\":[");
        for (size_t j = 0; j < binding->capability_count; j++) { if (j) fputc(',', file); json_string(file, binding->capability_ids[j]); }
        fprintf(file, "]}");
    }
    fprintf(file, "]");
}

static void emit_rule_universes(FILE *file, const BfAppPack *pack) {
    fprintf(file, "\"rule_universes\":[");
    for (size_t i = 0; i < pack->rule_universe_count; i++) {
        BfRuleUniverse *rules = pack->rule_universes[i];
        if (i) fputc(',', file);
        fprintf(file, "{\"doctype\":"); json_string(file, rules->doctype);
        fprintf(file, ",\"proof_policy\":"); json_string(file, rules->proof_policy);
        fprintf(file, ",\"activation_policy\":"); json_string(file, rules->activation_policy);
        fprintf(file, ",\"operators\":[");
        for (size_t j = 0; j < rules->operator_count; j++) { if (j) fputc(',', file); json_string(file, rules->operators[j]); }
        fprintf(file, "]}");
    }
    fprintf(file, "]");
}

static int emit_projection(const char *path, const char *kind, const BfAppPack *pack) {
    FILE *file = fopen(path, "wb");
    if (!file) {
        fprintf(stderr, "Error: cannot emit %s to %s\n", kind, path);
        return -1;
    }
    fprintf(file, "{\"kind\":"); json_string(file, kind);
    fprintf(file, ",\"app\":"); json_string(file, pack->app_name);
    fprintf(file, ",\"version\":"); json_string(file, pack->app_version);
    fprintf(file, ",\"source_revision\":"); json_pack_revision(file, pack);
    fputc(',', file);
    if (!strcmp(kind, "BfSchemaGraph")) emit_schema_graph(file, pack);
    else if (!strcmp(kind, "CapabilityBindings")) emit_bindings(file, pack);
    else if (!strcmp(kind, "RuleUniverses")) emit_rule_universes(file, pack);
    else { emit_schema_graph(file, pack); fputc(',', file); emit_bindings(file, pack); fputc(',', file); emit_rule_universes(file, pack); }
    fprintf(file, ",\"routes\":[");
    for (size_t i = 0; i < pack->route_count; i++) { if (i) fputc(',', file); json_string(file, pack->api_routes[i]); }
    fprintf(file, "],\"surfaces\":[");
    for (size_t i = 0; i < pack->surface_count; i++) { if (i) fputc(',', file); json_string(file, pack->surfaces[i]); }
    fprintf(file, "],\"migrations\":[");
    for (size_t i = 0; i < pack->migration_count; i++) { if (i) fputc(',', file); json_string(file, pack->migrations[i]); }
    fprintf(file, "],\"coverage_score\":%.6f}\n", pack->coverage_score);
    int ok = ferror(file) ? -1 : 0;
    if (fclose(file) != 0) ok = -1;
    if (ok != 0) fprintf(stderr, "Error: failed writing %s to %s\n", kind, path);
    return ok;
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

    if (dry_run && (emit_schema_ir || emit_bindings || emit_rule_universes || emit_pack)) {
        fprintf(stderr, "Error: --dry-run cannot be combined with --emit-* because it must not write artifacts\n");
        return 1;
    }

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
        if (emit_projection(emit_schema_ir, "BfSchemaGraph", pack) != 0) { bf_app_pack_free(pack); return 1; }
    }

    if (emit_bindings) {
        printf("\nEmitting bindings to: %s\n", emit_bindings);
        if (emit_projection(emit_bindings, "CapabilityBindings", pack) != 0) { bf_app_pack_free(pack); return 1; }
    }

    if (emit_rule_universes) {
        printf("\nEmitting rule universes to: %s\n", emit_rule_universes);
        if (emit_projection(emit_rule_universes, "RuleUniverses", pack) != 0) { bf_app_pack_free(pack); return 1; }
    }

    if (emit_pack) {
        printf("\nEmitting app pack to: %s\n", emit_pack);
        if (emit_projection(emit_pack, "AppPack", pack) != 0) { bf_app_pack_free(pack); return 1; }
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
