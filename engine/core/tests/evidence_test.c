/* Native conformance for evidence relations: shared evidence_relations store,
 * declared kinds, and DIRECTIONALITY (supports(A,B) does not imply supports(B,A)).
 * The db path is printed so a companion harness can reopen it from Python. */

#include "bf_evidence.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

int main(int argc, char **argv) {
    char default_path[] = "/tmp/bf_evidence_XXXXXX";
    const char *db_path = default_path;
    if (argc > 1) {
        db_path = argv[1];
    } else {
        int fd = mkstemp(default_path);
        if (fd >= 0) close(fd);
        unlink(default_path);
    }

    BfEvidence *g = NULL;
    if (bf_evidence_open(db_path, &g, stderr) != 0) {
        return fail("open");
    }

    /* a supporting relation A -> claim1 */
    if (bf_evidence_relate(g, "artifact:A", "supports", "claim:1") != BF_EVIDENCE_OK) {
        return fail("relate supports");
    }
    if (bf_evidence_supports(g, "artifact:A", "claim:1") != 1) {
        return fail("A should support claim:1");
    }
    /* directionality: claim:1 does not support artifact:A */
    if (bf_evidence_supports(g, "claim:1", "artifact:A") != 0) {
        return fail("support must be directional");
    }
    /* a second distinct supporter */
    if (bf_evidence_relate(g, "artifact:B", "supports", "claim:1") != BF_EVIDENCE_OK) {
        return fail("relate second supporter");
    }
    if (bf_evidence_supporters_count(g, "claim:1") != 2) {
        return fail("claim:1 should have 2 supporters");
    }
    /* a contradiction is not a support */
    if (bf_evidence_relate(g, "artifact:C", "contradicts", "claim:1") != BF_EVIDENCE_OK) {
        return fail("relate contradicts");
    }
    if (bf_evidence_supports(g, "artifact:C", "claim:1") != 0) {
        return fail("a contradiction must not read as support");
    }
    if (bf_evidence_supporters_count(g, "claim:1") != 2) {
        return fail("supporters count must ignore the contradiction");
    }
    {
        BfEvidenceVerdict verdict;
        if (bf_evidence_verdict(g, "claim:1", &verdict) != BF_EVIDENCE_OK ||
            verdict != BF_EVIDENCE_CONTESTED || bf_evidence_stands(g, "claim:1") != 0) {
            return fail("contradiction must outrank support");
        }
        if (bf_evidence_relate(g, "artifact:D", "falsifies", "claim:1") != BF_EVIDENCE_OK ||
            bf_evidence_relate(g, "artifact:E", "reproduces", "claim:1") != BF_EVIDENCE_OK ||
            bf_evidence_verdict(g, "claim:1", &verdict) != BF_EVIDENCE_OK ||
            verdict != BF_EVIDENCE_FALSIFIED || bf_evidence_stands(g, "claim:1") != 0) {
            return fail("falsification must remain terminal");
        }
        if (bf_evidence_relate(g, "artifact:R", "reproduces", "claim:2") != BF_EVIDENCE_OK ||
            bf_evidence_verdict(g, "claim:2", &verdict) != BF_EVIDENCE_OK ||
            verdict != BF_EVIDENCE_REPRODUCED || bf_evidence_stands(g, "claim:2") != 1) {
            return fail("reproduction must stand when uncontested");
        }
        if (bf_evidence_verdict(g, "claim:none", &verdict) != BF_EVIDENCE_OK ||
            verdict != BF_EVIDENCE_UNEVIDENCED || bf_evidence_stands(g, "claim:none") != 0) {
            return fail("missing relation must be unevidenced");
        }
    }
    /* an undeclared kind is rejected */
    if (bf_evidence_relate(g, "x", "vibes_with", "claim:1") != BF_EVIDENCE_INVALID_KIND) {
        return fail("undeclared kind should be rejected");
    }
    /* a missing field is rejected */
    if (bf_evidence_relate(g, "", "supports", "claim:1") != BF_EVIDENCE_INVALID) {
        return fail("empty evidence should be rejected");
    }

    bf_evidence_close(g);
    printf("DB=%s\n", db_path);
    printf("evidence_test: PASS\n");
    return 0;
}
