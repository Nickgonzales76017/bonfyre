#include "estate_evidence.h"
#include <stdio.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#ifdef main
#undef main
#endif

int bf_inverse_main(int argc, char **argv);

static const char *entry_option(int argc, char **argv, const char *name) {
    for (int i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

int main(int argc, char **argv) {
    enum { SO_ESTATE_EVIDENCE_STATUS, SO_ESTATE_EVIDENCE_SYNC };
    static const BfCommandEntry entry_commands[] = {
        {"estate-evidence-status", SO_ESTATE_EVIDENCE_STATUS, 0, NULL},
        {"estate-evidence-sync", SO_ESTATE_EVIDENCE_SYNC, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(entry_commands,
        sizeof(entry_commands) / sizeof(entry_commands[0]), argv[1]);
    if (argc >= 2 && command && command->id == SO_ESTATE_EVIDENCE_SYNC)
        return bf_estate_evidence_sync(entry_option(argc, argv, "--db"),
            entry_option(argc, argv, "--cms"), entry_option(argc, argv, "--foreign-root"),
            entry_option(argc, argv, "--generation"), stderr);
    if (argc >= 2 && command && command->id == SO_ESTATE_EVIDENCE_STATUS)
        return bf_estate_evidence_status(entry_option(argc, argv, "--db"),
            entry_option(argc, argv, "--cms"), stderr);
    return bf_inverse_main(argc, argv);
}
