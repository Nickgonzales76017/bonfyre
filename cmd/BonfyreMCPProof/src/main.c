#include "proof.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

int main(int argc, char **argv) {
    char repo[PATH_MAX];
    char *receipt;
    sqlite3 *db;
    BfProofContext ctx;
    BfProofChecks checks;
    int passed;

    enum { MP_GENERATION };
    static const BfCommandEntry proof_commands[] = {
        {"generation", MP_GENERATION, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(proof_commands,
        sizeof(proof_commands) / sizeof(proof_commands[0]), argv[1]);
    if (argc != 3 || !command || command->id != MP_GENERATION) {
        fprintf(stderr,
                "BonfyreMCPProof %s\n"
                "usage: bonfyre-mcp-proof generation <control-plane-receipt.json>\n",
                BF_MCP_PROOF_VERSION);
        return 2;
    }

    receipt = bf_proof_read_file(argv[2]);
    db = bf_json_db();
    if (!receipt || !db) {
        fputs("{\"schema\":\"bonfyre-mcp-generation-proof.v1\","
              "\"passed\":false,\"error\":\"receipt_unreadable\"}\n", stdout);
        free(receipt);
        if (db) sqlite3_close(db);
        return 1;
    }

    bf_init_repo_root(argv[0], repo, sizeof(repo));
    bf_load_context(db, receipt, argv[2], repo, &ctx, &checks);
    bf_verify_cms(db, receipt, &ctx, &checks);
    bf_verify_graph(db, &ctx, &checks);
    bf_verify_flow(db, &ctx, &checks);
    bf_verify_discipl(db, &ctx, &checks);
    bf_emit_report(&ctx, &checks);

    passed = checks.receipt_schema && checks.receipt_status && checks.manifest &&
             checks.cms_entry && checks.cms_hash && checks.cms_declared &&
             checks.cms_schema && checks.graph_atom && checks.graph_hash &&
             checks.graph_operator && checks.flow_record &&
             checks.flow_definition && checks.discipl_contract;
    free(receipt);
    sqlite3_close(db);
    return passed ? 0 : 1;
}
