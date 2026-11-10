/* bonfyre-lifecycle — native ProtocolMachine carrier.
 *
 * This command adds no lifecycle semantics of its own. It exposes the
 * Fabric-owned lifecycle store so every carrier (MCP, x402, MPP, A2A,
 * CloudEvents and commerce projections) can point at the same durable
 * instance and idempotency transitions.
 */
#include "bonfyre_fabric.h"

#include <stdio.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static void usage(FILE *stream) {
    fputs(
        "bonfyre-lifecycle — native durable protocol carrier\n\n"
        "USAGE\n"
        "  bonfyre-lifecycle lifecycle register-capability-fulfillment\n"
        "  bonfyre-lifecycle worker-run start <run> <worker> <mission> <protocol-instance>\n"
        "  bonfyre-lifecycle protocol-machine start <instance> <definition> <worker-run>\n"
        "  bonfyre-lifecycle protocol-machine step <instance> <operation> <idempotency-key> [--receipt <ref>]\n"
        "  bonfyre-lifecycle protocol-machine status <instance>\n\n"
        "CAPABILITY FULFILLMENT\n"
        "  quoted --present-payment--> payment-presented\n\n"
        "Every transition needs a receipted acknowledgement. This binary never\n"
        "treats a quote as payment, a payment claim as verification, or a\n"
        "protocol state as external execution authority. A payment-presented\n"
        "instance stays there until a verified rail-specific adapter exists.\n",
        stream);
}

int main(int argc, char **argv) {
    enum { LC_HELP };
    static const BfCommandEntry lifecycle_help_command[] = {
        {"--help", LC_HELP, 0, NULL},
        {"help", LC_HELP, 0, NULL},
    };
    const BfCommandEntry *help_command = argc < 2 ? NULL : bf_command_lookup(lifecycle_help_command,
        sizeof(lifecycle_help_command) / sizeof(lifecycle_help_command[0]), argv[1]);
    if (argc < 2 || (help_command && help_command->id == LC_HELP)) {
        usage(argc < 2 ? stderr : stdout);
        return argc < 2 ? 2 : 0;
    }
    return bf_fabric_dispatch(argc - 1, argv + 1, stdout, stderr);
}
