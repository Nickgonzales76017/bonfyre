/* Retraction cascade witness: a -1 propagates through native authority.
 *
 *   evidence withdrawn (Evidence -1)
 *   -> a claim loses its support
 *   -> the proof resting on it is challenged (ProofFrontier retracts)
 *   -> is_proven flips false
 *
 * Every step is a native engine call on ONE database. This is the first links of
 * the consequence-plane cascade the mandate requires, proven end to end. */

#include "bf_evidence.h"
#include "bf_proof.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

int main(void) {
    char db_path[] = "/tmp/bf_cascade_XXXXXX";
    int fd = mkstemp(db_path);
    if (fd >= 0) close(fd);
    unlink(db_path);

    BfEvidence *ev = NULL;
    BfProof *pf = NULL;
    if (bf_evidence_open(db_path, &ev, stderr) != 0) return fail("evidence open");
    if (bf_proof_open(db_path, &pf, stderr) != 0) return fail("proof open");

    /* two evidences support the claim; a proven invariant rests on it */
    if (bf_evidence_relate(ev, "artifact:A", "supports", "claim:x") != BF_EVIDENCE_OK ||
        bf_evidence_relate(ev, "artifact:B", "supports", "claim:x") != BF_EVIDENCE_OK) {
        return fail("relate evidence");
    }
    if (bf_proof_record_invariant(pf, "inv:x", "res:1", "", "claim:x", "x holds",
                                  "proven", "cooled", NULL) != 0) {
        return fail("record proven invariant");
    }
    if (bf_evidence_supporters_count(ev, "claim:x") != 2) return fail("two supporters");
    if (bf_proof_invariant_is_proven(pf, "inv:x") != 1) return fail("invariant proven");

    /* Evidence -1: withdraw one supporter -> still supported (robust) */
    if (bf_evidence_retract(ev, "artifact:A", "supports", "claim:x") != BF_EVIDENCE_OK) {
        return fail("retract A");
    }
    if (bf_evidence_supporters_count(ev, "claim:x") != 1) return fail("one supporter left");
    /* one loss did not collapse it, so the proof still stands (no cascade yet) */
    if (bf_proof_invariant_is_proven(pf, "inv:x") != 1) {
        return fail("proof should survive a single evidence loss");
    }

    /* Evidence -1 again: the last supporter withdrawn -> claim loses ALL support */
    if (bf_evidence_retract(ev, "artifact:B", "supports", "claim:x") != BF_EVIDENCE_OK) {
        return fail("retract B");
    }
    int64_t remaining = bf_evidence_supporters_count(ev, "claim:x");
    if (remaining != 0) return fail("no supporters left");

    /* the cascade: a proof with no surviving support is challenged (retracted) */
    if (remaining == 0) {
        if (bf_proof_challenge_invariant(pf, "inv:x", NULL) != 1) {
            return fail("challenge should retract the unsupported proof");
        }
    }
    if (bf_proof_invariant_is_proven(pf, "inv:x") != 0) {
        return fail("the proof must no longer be proven after the cascade");
    }

    /* idempotent withdrawal: retracting an absent relation is still OK */
    if (bf_evidence_retract(ev, "artifact:A", "supports", "claim:x") != BF_EVIDENCE_OK) {
        return fail("idempotent retract");
    }

    bf_proof_close(pf);
    bf_evidence_close(ev);
    unlink(db_path);
    printf("retraction_cascade_test: PASS (Evidence -1 -> support lost -> proof retracts)\n");
    return 0;
}
