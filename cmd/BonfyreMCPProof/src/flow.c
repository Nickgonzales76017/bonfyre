#include "proof.h"

#include <stdlib.h>
#include <string.h>

int bf_verify_flow(sqlite3 *db, BfProofContext *ctx, BfProofChecks *checks) {
    char binbuf[PATH_MAX], out[262144], err[8192], actual_id[256] = "";
    const char *flow = bf_owner_bin("BONFYRE_FLOW_BINARY", "bonfyre-flow",
                                    binbuf, sizeof(binbuf));
    char *definition = NULL;
    char *const av[] = {(char *)flow, "export", ctx->flow_id, NULL};

    if (bf_run_capture(av, "BONFYRE_FLOW_DB", ctx->flow_db,
                       out, sizeof(out), err, sizeof(err)) != 0 || !out[0])
        return 0;
    checks->flow_record =
        bf_jtext(db, out, "$.id", actual_id, sizeof(actual_id)) &&
        !strcmp(actual_id, ctx->flow_id);
    definition = bf_proof_read_file(ctx->flow_definition);
    if (definition)
        checks->flow_definition = bf_json_tree_equal(db, out, definition);
    free(definition);
    return checks->flow_record && checks->flow_definition;
}
