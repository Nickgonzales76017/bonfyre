#include "bf_commitment.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum { OP_COMPENSATE, OP_DELEGATE, OP_DELIVERED, OP_DISCHARGE,
    OP_HISTORY, OP_OUTSTANDING, OP_RECORD, OP_STATUS, OP_VIOLABLE,
    OP_VIOLATE, OP_WAS_EVER, OP_CHAIN } Operation;
typedef struct { const char *name; Operation op; int arguments; } Command;
static const Command COMMANDS[] = {
    {"chain", OP_CHAIN, 1}, {"compensate", OP_COMPENSATE, 1}, {"delegate", OP_DELEGATE, 3},
    {"delivered", OP_DELIVERED, 1}, {"discharge", OP_DISCHARGE, 1},
    {"history", OP_HISTORY, 1}, {"outstanding", OP_OUTSTANDING, 1},
    {"record", OP_RECORD, 4}, {"status", OP_STATUS, 1},
    {"violable", OP_VIOLABLE, 1}, {"violate", OP_VIOLATE, 1},
    {"was-ever", OP_WAS_EVER, 2},
};

static void json_string(const char *value) {
    const unsigned char *p = (const unsigned char *)(value ? value : "");
    putchar('"');
    for (; *p; ++p) {
        switch (*p) {
        case '"': fputs("\\\"", stdout); break;
        case '\\': fputs("\\\\", stdout); break;
        case '\n': fputs("\\n", stdout); break;
        case '\r': fputs("\\r", stdout); break;
        case '\t': fputs("\\t", stdout); break;
        default:
            if (*p < 0x20) {
                static const char hex[] = "0123456789abcdef";
                fputs("\\u00", stdout);
                putchar(hex[*p >> 4]); putchar(hex[*p & 15]);
            } else putchar(*p);
        }
    }
    putchar('"');
}
static int compare_command(const void *key, const void *item) {
    return strcmp((const char *)key, ((const Command *)item)->name);
}
static int report(BfCommitmentStatus status) {
    if (status == BF_COMMITMENT_OK) return 0;
    fputs("{\"error\":", stdout);
    json_string(bf_commitment_status_name(status));
    fputs("}\n", stdout);
    return 1;
}
static BfCommitmentState state_from_name(const char *name) {
    for (int value = BF_COMMITMENT_OPEN; value <= BF_COMMITMENT_COMPENSATED; ++value)
        if (strcmp(name, bf_commitment_state_name((BfCommitmentState)value)) == 0)
            return (BfCommitmentState)value;
    return (BfCommitmentState)-1;
}
static const char *arg(int argc, char **argv, int index) {
    return index < argc ? argv[index] : "";
}
static void print_list(const BfCommitmentList *list, int with_state) {
    putchar('[');
    for (size_t i = 0; i < list->count; ++i) {
        if (i) putchar(',');
        if (with_state) putchar('[');
        json_string(list->ids[i]);
        if (with_state) {
            putchar(','); json_string(bf_commitment_state_name(list->states[i]));
            putchar(']');
        }
    }
    fputs("]\n", stdout);
}

int main(int argc, char **argv) {
    if (argc < 4) return 2;
    const Command *command = bsearch(argv[2], COMMANDS,
        sizeof(COMMANDS) / sizeof(COMMANDS[0]), sizeof(COMMANDS[0]), compare_command);
    if (!command || argc < command->arguments + 3) return 2;
    BfCommitment *graph = NULL;
    if (bf_commitment_open(argv[1], &graph, stderr) != 0) return 2;
    BfCommitmentStatus result = BF_COMMITMENT_OK;
    switch (command->op) {
    case OP_RECORD: {
        BfCommitmentSpec spec = {0};
        spec.commitment_id = argv[3]; spec.committer = argv[4];
        spec.beneficiary = argv[5]; spec.resource = argv[6];
        spec.conditions = arg(argc, argv, 7); spec.expires_at = arg(argc, argv, 8);
        spec.authority = arg(argc, argv, 9); spec.origin = arg(argc, argv, 10);
        spec.enactment = arg(argc, argv, 11); spec.at = arg(argc, argv, 12);
        result = bf_commitment_record(graph, &spec);
        if (result == BF_COMMITMENT_OK) fputs("true\n", stdout);
        break;
    }
    case OP_STATUS: {
        BfCommitmentState state; int found = 0;
        result = bf_commitment_status(graph, argv[3], &state, &found);
        if (result == BF_COMMITMENT_OK) {
            if (found) json_string(bf_commitment_state_name(state));
            else fputs("null", stdout);
            putchar('\n');
        }
        break;
    }
    case OP_DELIVERED: {
        int delivered = 0;
        result = bf_commitment_delivered(graph, argv[3], &delivered);
        if (result == BF_COMMITMENT_OK) fputs(delivered ? "true\n" : "false\n", stdout);
        break;
    }
    case OP_HISTORY: {
        BfCommitmentEvents events = {0};
        result = bf_commitment_history(graph, argv[3], &events);
        if (result == BF_COMMITMENT_OK) {
            putchar('[');
            for (size_t i = 0; i < events.count; ++i) {
                if (i) putchar(',');
                putchar('['); json_string(events.events[i].state);
                putchar(','); json_string(events.events[i].actor);
                putchar(','); json_string(events.events[i].detail);
                putchar(','); json_string(events.events[i].at); putchar(']');
            }
            fputs("]\n", stdout);
        }
        break;
    }
    case OP_WAS_EVER: {
        const BfCommitmentState state = state_from_name(argv[4]);
        if ((int)state < 0) result = BF_COMMITMENT_INVALID;
        else {
            int present = 0;
            result = bf_commitment_was_ever(graph, argv[3], state, &present);
            if (result == BF_COMMITMENT_OK) fputs(present ? "true\n" : "false\n", stdout);
        }
        break;
    }
    case OP_DISCHARGE:
        result = bf_commitment_discharge(graph, argv[3], arg(argc, argv, 4),
            arg(argc, argv, 5), arg(argc, argv, 6));
        if (result == BF_COMMITMENT_OK) fputs("true\n", stdout);
        break;
    case OP_VIOLATE:
        result = bf_commitment_violate(graph, argv[3], arg(argc, argv, 4),
            arg(argc, argv, 5), arg(argc, argv, 6));
        if (result == BF_COMMITMENT_OK) fputs("true\n", stdout);
        break;
    case OP_COMPENSATE:
        result = bf_commitment_compensate(graph, argv[3], arg(argc, argv, 4),
            arg(argc, argv, 5), arg(argc, argv, 6));
        if (result == BF_COMMITMENT_OK) fputs("true\n", stdout);
        break;
    case OP_DELEGATE:
        result = bf_commitment_delegate(graph, argv[3], argv[4], argv[5],
            arg(argc, argv, 6), arg(argc, argv, 7));
        if (result == BF_COMMITMENT_OK) { json_string(argv[5]); putchar('\n'); }
        break;
    case OP_CHAIN: {
        char origin[BF_COMMITMENT_ID_MAX], committer[BF_COMMITMENT_ID_MAX];
        int delegated = 0;
        result = bf_commitment_delegation_chain(graph, argv[3], origin, committer, &delegated);
        if (result == BF_COMMITMENT_OK) {
            putchar('['); json_string(origin);
            if (delegated) { putchar(','); json_string(committer); }
            fputs("]\n", stdout);
        }
        break;
    }
    case OP_VIOLABLE:
    case OP_OUTSTANDING: {
        BfCommitmentList list = {0};
        result = command->op == OP_VIOLABLE
            ? bf_commitment_violable(graph, argv[3], &list)
            : bf_commitment_outstanding(graph, argv[3], &list);
        if (result == BF_COMMITMENT_OK) print_list(&list, command->op == OP_OUTSTANDING);
        break;
    }
    }
    bf_commitment_close(graph);
    return report(result);
}
