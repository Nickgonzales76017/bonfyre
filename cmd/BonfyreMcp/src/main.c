/* bonfyre-mcp: native MCP organ. Multi-call: the first argument, or the
 * program name (bonfyre-spines, bonfyre-canary, ...), selects the owner. */
#include <stdio.h>
#include <string.h>
#include "mcp.h"

static int usage(void) {
    fputs("usage: bonfyre-mcp spines|canary|recursive|v6 [arguments]\n"
          "       bonfyre-mcp fixture-serve PORTFILE SCRIPT RECORD\n", stderr);
    return 2;
}

int main(int argc, char **argv) {
    const char *self = strrchr(argv[0], '/'); self = self ? self + 1 : argv[0];
    static const struct { const char *name; int (*run)(int, char **); } owners[] = {
        { "spines", cli_spines }, { "canary", cli_canary }, { "recursive", cli_recursive }, { "v6", cli_v6 },
        { "fixture-serve", fixture_serve },
    };
    for (size_t i = 0; i < sizeof owners / sizeof *owners; i++) {
        char alias[64]; snprintf(alias, sizeof alias, "bonfyre-%s", owners[i].name);
        if (!strcmp(self, alias)) return owners[i].run(argc - 1, argv + 1);
    }
    if (argc < 2) return usage();
    for (size_t i = 0; i < sizeof owners / sizeof *owners; i++)
        if (!strcmp(argv[1], owners[i].name)) return owners[i].run(argc - 2, argv + 2);
    return usage();
}
