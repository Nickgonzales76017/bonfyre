#define _POSIX_C_SOURCE 200809L
#include "domain_binding.h"
#include "../../BonfyreExperienceMap/src/protocol_binding.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static const char *flag_value(int argc, char **argv, const char *flag) {
    int i;
    for (i = 3; i + 1 < argc; i++)
        if (!strcmp(argv[i], flag)) return argv[i + 1];
    return NULL;
}

static void usage(FILE *out) {
    fputs("BonfyreExperienceBinding 0.2.0\n"
          "  bonfyre-experience-binding domain OCCURRENCE "
          "--observation-receipts FILE --ingest-dir DIR [--output FILE]\n"
          "  bonfyre-experience-binding protocol OCCURRENCE "
          "--evidence FILE --native-receipt FILE [--output FILE]\n"
          "Bindings validate owner receipts; they do not enact provider or external effects.\n",
          out);
}

int main(int argc, char **argv) {
    sqlite3 *db = NULL;
    const char *output;
    int rc;

    enum {
        EB_HELP, EB_DOMAIN, EB_PROTOCOL,
    };
    static const BfCommandEntry binding_commands[] = {
        {"--help", EB_HELP, 0, NULL},
        {"domain", EB_DOMAIN, 0, NULL},
        {"help", EB_HELP, 0, NULL},
        {"protocol", EB_PROTOCOL, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(binding_commands,
        sizeof(binding_commands) / sizeof(binding_commands[0]), argv[1]);

    if (argc == 2 && command && command->id == EB_HELP) {
        usage(stdout);
        return 0;
    }
    if (argc < 3) {
        usage(stderr);
        return 2;
    }
    output = flag_value(argc, argv, "--output");
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 2;
    }

    if (command && command->id == EB_DOMAIN) {
        const char *receipts = flag_value(argc, argv, "--observation-receipts");
        const char *ingest_dir = flag_value(argc, argv, "--ingest-dir");
        if (!receipts || !ingest_dir) {
            usage(stderr);
            sqlite3_close(db);
            return 2;
        }
        rc = bf_experience_bind_domain(db, argv[2], receipts, ingest_dir, output);
    } else if (command && command->id == EB_PROTOCOL) {
        const char *evidence = flag_value(argc, argv, "--evidence");
        const char *native_receipt = flag_value(argc, argv, "--native-receipt");
        if (!evidence || !native_receipt) {
            usage(stderr);
            sqlite3_close(db);
            return 2;
        }
        rc = bf_experience_bind_protocol(db, argv[2], evidence, native_receipt, output);
    } else {
        usage(stderr);
        rc = 2;
    }

    sqlite3_close(db);
    return rc;
}
