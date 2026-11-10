#include "procurement.h"
#include "effect.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_INPUT (1024u * 1024u)

static char *read_stdin(size_t *length) {
    size_t cap = 16384, used = 0;
    char *body = malloc(cap + 1);
    if (!body) return NULL;
    for (;;) {
        if (used == cap) {
            if (cap >= MAX_INPUT) { free(body); return NULL; }
            size_t next = cap * 2;
            if (next > MAX_INPUT) next = MAX_INPUT;
            char *grown = realloc(body, next + 1);
            if (!grown) { free(body); return NULL; }
            body = grown; cap = next;
        }
        size_t got = fread(body + used, 1, cap - used, stdin);
        used += got;
        if (!got) {
            if (ferror(stdin)) { free(body); return NULL; }
            break;
        }
    }
    body[used] = 0;
    *length = used;
    return body;
}

static int usage(void) {
    fputs(
        "bonfyre-procurement — procurement continuation and authority-gated enactment\n\n"
        "  bonfyre-procurement rfq < input.json\n"
        "  bonfyre-procurement quote < input.json\n"
        "  bonfyre-procurement compare [--as-of YYYY-MM-DDTHH:MM:SSZ] < input.json\n"
        "  bonfyre-procurement allocate < input.json\n"
        "  bonfyre-procurement approval-request < input.json\n"
        "  bonfyre-procurement order-intent < input.json\n"
        "  bonfyre-procurement contract-preflight < input.json\n"
        "  bonfyre-procurement enact --commitments-db DB --authority-db DB --exchange ID --order-intent-digest HEX --quote-id ID --quote-digest HEX --supplier-id ed25519:PUBLIC --capability REF --buyer ACTOR --seller ed25519:PUBLIC --settlement ACTOR --deliverer ACTOR --amount-cents N --currency USD --signature HEX --at UTC\n\n"
        "Pure commands derive preparation records only. enact is native-only: it verifies exact supplier acceptance, rechecks role authority and atomically opens ProtocolContract commitments; it still does not create a purchase order, payable, payment, settlement or shipment.\n",
        stderr);
    return 2;
}

int main(int argc, char **argv) {
    if (argc < 2) return usage();
    enum {
        PC_ALLOCATE, PC_APPROVAL_REQUEST, PC_COMPARE, PC_CONTRACT_PREFLIGHT,
        PC_ENACT, PC_ORDER_INTENT, PC_QUOTE, PC_RFQ,
    };
    static const BfCommandEntry procurement_commands[] = {
        {"allocate", PC_ALLOCATE, 0, NULL},
        {"approval-request", PC_APPROVAL_REQUEST, 0, NULL},
        {"compare", PC_COMPARE, 0, NULL},
        {"contract-preflight", PC_CONTRACT_PREFLIGHT, 0, NULL},
        {"enact", PC_ENACT, 0, NULL},
        {"order-intent", PC_ORDER_INTENT, 0, NULL},
        {"quote", PC_QUOTE, 0, NULL},
        {"rfq", PC_RFQ, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(procurement_commands,
        sizeof(procurement_commands) / sizeof(procurement_commands[0]), argv[1]);
    if (command && command->id == PC_ENACT) return bf_procurement_effect_main(argc, argv);
    const char *as_of = NULL;
    if (command && command->id == PC_COMPARE) {
        for (int i = 2; i < argc; i++) {
            if (!strcmp(argv[i], "--as-of") && i + 1 < argc) as_of = argv[++i];
            else return usage();
        }
    } else if (argc != 2 || !command || (command->id != PC_RFQ && command->id != PC_QUOTE && command->id != PC_ALLOCATE && command->id != PC_APPROVAL_REQUEST && command->id != PC_ORDER_INTENT && command->id != PC_CONTRACT_PREFLIGHT)) {
        return usage();
    }
    size_t len = 0;
    char *input = read_stdin(&len), *output = NULL, error[256] = {0};
    if (!input) { fputs("input exceeds 1 MiB or could not be read\n", stderr); return 1; }
    int ok = command->id == PC_RFQ ? bf_procurement_rfq(input, len, &output, error, sizeof error)
           : command->id == PC_QUOTE ? bf_procurement_quote(input, len, &output, error, sizeof error)
           : command->id == PC_ALLOCATE ? bf_procurement_allocate(input, len, &output, error, sizeof error)
           : command->id == PC_APPROVAL_REQUEST ? bf_procurement_approval_request(input, len, &output, error, sizeof error)
           : command->id == PC_ORDER_INTENT ? bf_procurement_order_intent(input, len, &output, error, sizeof error)
           : command->id == PC_CONTRACT_PREFLIGHT ? bf_procurement_contract_preflight(input, len, &output, error, sizeof error)
           : bf_procurement_compare(input, len, as_of, &output, error, sizeof error);
    free(input);
    if (!ok) { fprintf(stderr, "refused: %s\n", error[0] ? error : "invalid procurement input"); return 1; }
    fputs(output, stdout); fputc('\n', stdout); free(output);
    return 0;
}
