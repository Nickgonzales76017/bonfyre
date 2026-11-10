#include "bf_authority.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum { OP_CASCADE, OP_DELEGATE, OP_GRANT, OP_HAS, OP_REVOKE } Operation;
typedef struct { const char *name; Operation op; int minimum; } Command;
static const Command COMMANDS[] = {
    {"cascade", OP_CASCADE, 1}, {"delegate", OP_DELEGATE, 3},
    {"grant", OP_GRANT, 4}, {"has", OP_HAS, 3}, {"revoke", OP_REVOKE, 1},
};
static int compare_command(const void *key, const void *item) {
    return strcmp((const char *)key, ((const Command *)item)->name);
}
static const char *optional(int argc, char **argv, int index) {
    return index < argc && argv[index][0] ? argv[index] : NULL;
}
int main(int argc, char **argv) {
    if (argc < 4) return 2;
    const Command *command = bsearch(argv[2], COMMANDS,
        sizeof(COMMANDS) / sizeof(COMMANDS[0]), sizeof(COMMANDS[0]), compare_command);
    if (!command || argc < command->minimum + 3) return 2;
    BfAuthority *graph = NULL;
    if (bf_authority_open(argv[1], &graph, stderr) != 0) return 2;
    int result = -1;
    switch (command->op) {
    case OP_GRANT: {
        BfAuthorityEdge edge = {0};
        edge.edge_id = argv[3]; edge.actor = argv[4]; edge.permission = argv[5]; edge.subject = argv[6];
        edge.purpose = optional(argc, argv, 7); edge.scope = optional(argc, argv, 8);
        edge.effective_from = optional(argc, argv, 9); edge.expires_at = optional(argc, argv, 10);
        edge.delegable = optional(argc, argv, 11) && strcmp(argv[11], "0") != 0;
        edge.evidence = optional(argc, argv, 12); edge.originating_authority = optional(argc, argv, 13);
        result = bf_authority_grant(graph, &edge, optional(argc, argv, 14));
        break;
    }
    case OP_HAS:
        result = bf_authority_has(graph, argv[3], argv[4], argv[5],
            optional(argc, argv, 6), optional(argc, argv, 7));
        break;
    case OP_REVOKE: result = bf_authority_revoke(graph, argv[3]); break;
    case OP_DELEGATE:
        result = bf_authority_delegate(graph, argv[3], argv[5], argv[4],
            optional(argc, argv, 6), optional(argc, argv, 7));
        break;
    case OP_CASCADE: result = bf_authority_revoke_cascade(graph, argv[3]); break;
    }
    bf_authority_close(graph);
    printf("%d\n", result);
    return result < 0 ? 1 : 0;
}
