#include "proof.h"

int bf_verify_discipl(sqlite3 *db, BfProofContext *ctx, BfProofChecks *checks) {
    char binbuf[PATH_MAX], out[262144], err[8192];
    const char *discipl = bf_owner_bin("BONFYRE_DISCIPL_BINARY", "bonfyre-discipl",
                                       binbuf, sizeof(binbuf));
    char *const av[] = {
        (char *)discipl, "contracts", "list", "--family", "T_MOE_ROUTER",
        "--root", ctx->actor_root, NULL
    };
    if (bf_run_capture(av, NULL, NULL, out, sizeof(out), err, sizeof(err)) != 0)
        return 0;
    checks->discipl_contract =
        bf_array_has_two(db, out, "$.contracts",
                         "$.src_family", "T_MOE_ROUTER",
                         "$.dst_family", "T_MOE_EXPERT");
    return checks->discipl_contract;
}
