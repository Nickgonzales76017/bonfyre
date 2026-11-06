#define _POSIX_C_SOURCE 200809L
#include "proof.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int bf_verify_graph(sqlite3 *db, BfProofContext *ctx, BfProofChecks *checks) {
    char binbuf[PATH_MAX], out[8192], err[8192];
    const char *graph = bf_owner_bin("BONFYRE_GRAPH_BINARY", "bonfyre-graph",
                                     binbuf, sizeof(binbuf));
    char artifact[] = "/tmp/bonfyre-mcp-proof.XXXXXX";
    char *doc = NULL, *atom = NULL, *op = NULL;
    int artifact_fd;

    artifact_fd = mkstemp(artifact);
    if (artifact_fd < 0) return 0;
    close(artifact_fd);
    {
        char *const av[] = {
            (char *)graph, "export", ctx->graph_db, "--out", artifact, NULL
        };
        if (bf_run_capture(av, NULL, NULL, out, sizeof(out), err, sizeof(err)) == 0)
            doc = bf_proof_read_file(artifact);
    }
    unlink(artifact);
    if (!doc) return 0;

    atom = bf_array_object_by(db, doc, "$.atoms", "$.atom_id",
                              ctx->graph_output_atom);
    op = bf_array_object_by(db, doc, "$.operators", "$.operator_id",
                            ctx->graph_operator);
    if (atom) {
        char actual[80] = "";
        checks->graph_atom = 1;
        bf_jtext(db, atom, "$.content_hash", actual, sizeof(actual));
        checks->graph_hash = ctx->expected_hash[0] &&
                             !strcmp(actual, ctx->expected_hash);
    }
    if (op) {
        char output[256] = "";
        bf_jtext(db, op, "$.output", output, sizeof(output));
        checks->graph_operator = !strcmp(output, ctx->graph_output_atom);
    }
    free(atom);
    free(op);
    free(doc);
    return checks->graph_atom && checks->graph_hash && checks->graph_operator;
}
