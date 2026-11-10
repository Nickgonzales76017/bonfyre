#define _POSIX_C_SOURCE 200809L
#include "compiler.h"
#include "portfolio.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static const char *opt(int argc, char **argv, const char *name, const char *fallback) {
    for (int i = 2; i + 1 < argc; ++i) if (!strcmp(argv[i], name)) return argv[i + 1];
    return fallback;
}

static void usage(FILE *out) {
    fprintf(out,
        "Usage:\n"
        "  bonfyre-experience-stage portfolio audit|order|cards|queue-plan|show ID [--contracts FILE --root DIR]\\n"
        "  bonfyre-experience-stage compile [options]\n\n"
        "Options:\n"
        "  --chains FILE             experience_chains.tsv\n"
        "  --external-bindings FILE  compiled external bindings JSONL\n"
        "  --external-raw FILE       observed ExternalEstate JSONL\n"
        "  --ghost-bindings FILE     compiled Ghost bindings JSONL\n"
        "  --app-bindings FILE       compiled app/reference bindings JSONL\n"
        "  --output FILE             stage bindings JSONL\n"
        "  --manifest FILE           stage manifest JSON\n");
}

int main(int argc, char **argv) {
    BfStageCompileOptions o;
    enum {
        ES_HELP, ES_COMPILE, ES_PORTFOLIO,
    };
    static const BfCommandEntry stage_commands[] = {
        {"--help", ES_HELP, 0, NULL},
        {"compile", ES_COMPILE, 0, NULL},
        {"help", ES_HELP, 0, NULL},
        {"portfolio", ES_PORTFOLIO, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(stage_commands,
        sizeof(stage_commands) / sizeof(stage_commands[0]), argv[1]);
    if (argc < 2 || (command && command->id == ES_HELP)) {
        usage(stdout);
        return argc < 2 ? 1 : 0;
    }
    if (command && command->id == ES_PORTFOLIO) return bf_portfolio_main(argc, argv);
    if (!command || command->id != ES_COMPILE) {
        usage(stderr);
        return 1;
    }
    (void)mkdir("generated", 0755);
    o.chains_tsv = opt(argc, argv, "--chains", "../BonfyreExperienceMap/data/experience_chains.tsv");
    o.external_bindings = opt(argc, argv, "--external-bindings", "../BonfyreExperienceMap/generated/external_bindings.jsonl");
    o.external_raw = opt(argc, argv, "--external-raw", "../../estate/generated/external-estate-v4/observed-external-estate.jsonl");
    o.ghost_bindings = opt(argc, argv, "--ghost-bindings", "../BonfyreExperienceMap/generated/ghost_bindings.jsonl");
    o.app_bindings = opt(argc, argv, "--app-bindings", "../BonfyreExperienceMap/generated/app_bindings.jsonl");
    o.output_jsonl = opt(argc, argv, "--output", "generated/stage_bindings.jsonl");
    o.manifest_json = opt(argc, argv, "--manifest", "generated/stage_manifest.json");
    return bf_stage_compile(&o);
}
