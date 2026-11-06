/*
 * bonfyre-decide — one program for the DecisionPlane products. Installed as
 * bonfyre-books, bonfyre-monitor and bonfyre-manifest (links to this file);
 * the name it was started under picks the product, or the first argument
 * when started as bonfyre-decide.
 */
#include "dp.h"
#include <stdio.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

int main(int argc, char **argv) {
    const char *self = strrchr(argv[0], '/'); self = self ? self + 1 : argv[0];
    int (*run)(int, char **) = NULL;
    if (strstr(self, "books")) run = books_main;
    else if (strstr(self, "monitor")) run = monitor_main;
    else if (strstr(self, "manifest")) run = manifest_main;
    else if (argc > 1) {
        enum { DE_BOOKS, DE_MANIFEST, DE_MONITOR };
        static const BfCommandEntry decide_commands[] = {
            {"books", DE_BOOKS, 0, NULL},
            {"manifest", DE_MANIFEST, 0, NULL},
            {"monitor", DE_MONITOR, 0, NULL},
        };
        const BfCommandEntry *command = bf_command_lookup(decide_commands,
            sizeof(decide_commands) / sizeof(decide_commands[0]), argv[1]);
        if (command && command->id == DE_BOOKS) run = books_main;
        else if (command && command->id == DE_MONITOR) run = monitor_main;
        else if (command && command->id == DE_MANIFEST) run = manifest_main;
        if (run) { argc--; argv++; }
    }
    if (!run) { fputs("usage: bonfyre-decide books|monitor|manifest ARGS (or run as bonfyre-books, -monitor, -manifest)\n", stderr); return 2; }
    return run(argc - 1, argv + 1);
}
