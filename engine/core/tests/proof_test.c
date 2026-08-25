/* Native conformance for the proof frontier: shared stores, and the load-bearing
 * discipline -- a MEASURED invariant is never PROVEN (the FPQ lesson), a challenge
 * RETRACTS proof, and a KnownNonCause is a durable negative. Prints DB for a
 * Python shared-store read. */

#include "bf_proof.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

int main(int argc, char **argv) {
    char default_path[] = "/tmp/bf_proof_XXXXXX";
    const char *db_path = default_path;
    if (argc > 1) {
        db_path = argv[1];
    } else {
        int fd = mkstemp(default_path);
        if (fd >= 0) close(fd);
        unlink(default_path);
    }

    BfProof *p = NULL;
    if (bf_proof_open(db_path, &p, stderr) != 0) {
        return fail("open");
    }

    /* frontier layer: proven vs open */
    if (bf_proof_record_layer(p, "res:1", "", 0, "identity", "proven", "w1") != 0) {
        return fail("record proven layer");
    }
    if (bf_proof_record_layer(p, "res:1", "", 1, "generation", "open", "") != 0) {
        return fail("record open layer");
    }
    if (bf_proof_layer_is_proven(p, "res:1", "", "identity") != 1) {
        return fail("identity layer should be proven");
    }
    if (bf_proof_layer_is_proven(p, "res:1", "", "generation") != 0) {
        return fail("generation layer is open, not proven");
    }

    /* THE discipline: a measured invariant is NOT proven */
    if (bf_proof_record_invariant(p, "inv:measured", "res:1", "", "generation",
                                  "recon error low", "measured", "cooled", NULL) != 0) {
        return fail("record measured invariant");
    }
    if (bf_proof_invariant_is_proven(p, "inv:measured") != 0) {
        return fail("a MEASURED invariant must never read as proven (FPQ lesson)");
    }

    /* a proven-plane invariant IS proven */
    if (bf_proof_record_invariant(p, "inv:proven", "res:1", "", "identity",
                                  "identity continuity", "proven", "cooled", NULL) != 0) {
        return fail("record proven invariant");
    }
    if (bf_proof_invariant_is_proven(p, "inv:proven") != 1) {
        return fail("a proven-plane invariant should be proven");
    }

    /* a challenge RETRACTS proof */
    if (bf_proof_challenge_invariant(p, "inv:proven", NULL) != 1) {
        return fail("challenge should change status");
    }
    if (bf_proof_invariant_is_proven(p, "inv:proven") != 0) {
        return fail("a challenged invariant is no longer proven (retraction)");
    }

    /* KnownNonCause: a durable negative */
    if (bf_proof_record_noncause(p, "nc:1", "gpu-oom", "res:1", "exp-42", "w", NULL) != 0) {
        return fail("record noncause");
    }
    if (bf_proof_is_blackholed(p, "gpu-oom", "res:1") != 1) {
        return fail("hypothesis should be blackholed");
    }
    if (bf_proof_is_blackholed(p, "gpu-oom", "res:other") != 0) {
        return fail("blackhole is scoped to its subject");
    }

    bf_proof_close(p);
    printf("DB=%s\n", db_path);
    printf("proof_test: PASS\n");
    return 0;
}
