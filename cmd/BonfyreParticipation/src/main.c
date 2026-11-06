#include "activation.h"
#include "evidence.h"
#include "projection.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *opt(int argc, char **argv, const char *name, const char *fallback) {
    for (int i = 2; i + 1 < argc; ++i) if (!strcmp(argv[i], name)) return argv[i + 1];
    return fallback;
}

static void usage(FILE *out) {
    fprintf(out,
        "Usage:\n"
        "  bonfyre-participation compile [options]\n\n"
        "Options:\n"
        "  --external-bindings FILE  ExternalEstate compiled bindings JSONL\n"
        "  --evidence PATH           evidence JSONL file or recursive directory\n"
        "  --manifest FILE           output diagnostic manifest\n");
}

int main(int argc, char **argv) {
    sqlite3 *db = NULL;
    long long raw_rows = 0;
    int rc = 1;
    const char *external, *evidence, *manifest;
    enum {
        PA_HELP, PA_COMPILE,
    };
    static const BfCommandEntry participation_commands[] = {
        {"--help", PA_HELP, 0, NULL},
        {"compile", PA_COMPILE, 0, NULL},
        {"help", PA_HELP, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(participation_commands,
        sizeof(participation_commands) / sizeof(participation_commands[0]), argv[1]);
    if (argc < 2 || (command && command->id == PA_HELP)) {
        usage(stdout);
        return argc < 2 ? 1 : 0;
    }
    if (!command || command->id != PA_COMPILE) { usage(stderr); return 1; }
    external = opt(argc, argv, "--external-bindings",
                   "../BonfyreExperienceMap/generated/external_bindings.jsonl");
    evidence = opt(argc, argv, "--evidence",
                   "../BonfyreExperienceMap/generated/participation_evidence.jsonl");
    manifest = opt(argc, argv, "--manifest", "generated/participation_manifest.json");
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) goto done;
    if (!bf_part_evidence_init(db)) goto done;
    if (!bf_part_evidence_load(db, evidence, &raw_rows)) goto done;
    rc = bf_part_project_manifest(db, external, raw_rows, manifest);
done:
    if (db) sqlite3_close(db);
    return rc;
}
