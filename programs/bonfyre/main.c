#include "bonfyre_fabric.h"

#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
    const char *program = strrchr(argv[0], '/');

    program = program ? program + 1 : argv[0];
    if (argc < 2) return bf_fabric_dispatch(1, (char *[]){"help"}, stdout, stderr);
    /* Reached through the front door as `bonfyre fabric <verb>`, the verbs
     * that belong to Fabric itself arrive without their own first word. */
    if (!strcmp(program, "bonfyre-fabric") && argc < 30 &&
        (!strcmp(argv[1], "compile") || !strcmp(argv[1], "status") || !strcmp(argv[1], "init"))) {
        char *named[32] = {"fabric"};
        for (int index = 1; index < argc; ++index) named[index] = argv[index];
        return bf_fabric_dispatch(argc, named, stdout, stderr);
    }
    return bf_fabric_dispatch(argc - 1, argv + 1, stdout, stderr);
}
