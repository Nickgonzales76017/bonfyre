/* Native conformance for the OccurrenceSpine: shared external_event_log store,
 * observation-identity dedup, the two-clock invariant (event time != ingest
 * time), event-kind gating, and projection lifecycle. The digest-parity line is
 * checked against the Python reference by the harness (occurrence_parity.sh). */

#include "bf_occurrence.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

int main(void) {
    char db_path[] = "/tmp/bf_occ_XXXXXX";
    int fd = mkstemp(db_path);
    if (fd >= 0) {
        close(fd);
    }
    unlink(db_path);

    BfOccurrenceSpine *spine = NULL;
    if (bf_occurrence_open(db_path, &spine, stderr) != 0) {
        return fail("open");
    }

    BfOccurrenceSpec spec;
    memset(&spec, 0, sizeof(spec));
    spec.source = "github";
    spec.actor = "octocat";
    spec.event_kind = "inbound_reply";
    spec.subject_ref = "issue#42";
    spec.observed_at = "2026-08-16T12:00:00+00:00";
    spec.recorded_at = "2026-08-16T12:00:05+00:00";

    int64_t id1 = 0;
    if (bf_occurrence_observe(spine, &spec, &id1) != BF_OCCURRENCE_OK || id1 <= 0) {
        return fail("first observe should succeed");
    }
    if (bf_occurrence_unprojected_count(spine) != 1) {
        return fail("count should be 1 after first observe");
    }

    /* Same observation, later ingest time -- dedup by observation identity, not
     * by when we noticed. This is the two-clock invariant: a re-read must not
     * create a second row. */
    spec.recorded_at = "2026-08-16T18:30:00+00:00";
    int64_t id_dup = 0;
    if (bf_occurrence_observe(spine, &spec, &id_dup) != BF_OCCURRENCE_DUPLICATE) {
        return fail("re-observe should be DUPLICATE");
    }
    if (id_dup != id1) {
        return fail("duplicate should return the existing id");
    }
    if (bf_occurrence_unprojected_count(spine) != 1) {
        return fail("count must stay 1 after duplicate");
    }

    /* Different event time -> a genuinely different occurrence. */
    spec.observed_at = "2026-08-16T13:00:00+00:00";
    int64_t id2 = 0;
    if (bf_occurrence_observe(spine, &spec, &id2) != BF_OCCURRENCE_OK || id2 == id1) {
        return fail("different observed_at should be a new occurrence");
    }
    if (bf_occurrence_unprojected_count(spine) != 2) {
        return fail("count should be 2");
    }

    /* An undeclared kind is rejected, never silently folded. */
    BfOccurrenceSpec bad = spec;
    bad.event_kind = "coordinator_assigned";
    if (bf_occurrence_observe(spine, &bad, NULL) != BF_OCCURRENCE_INVALID_KIND) {
        return fail("undeclared kind should be rejected");
    }
    /* A missing required field is rejected. */
    BfOccurrenceSpec empty = spec;
    empty.actor = "";
    if (bf_occurrence_observe(spine, &empty, NULL) != BF_OCCURRENCE_INVALID) {
        return fail("empty actor should be invalid");
    }

    /* Projection folds an occurrence out of the pending set (not a delete). */
    if (bf_occurrence_mark_projected(spine, id1, NULL) != BF_OCCURRENCE_OK) {
        return fail("mark_projected should succeed");
    }
    if (bf_occurrence_unprojected_count(spine) != 1) {
        return fail("count should be 1 after projecting one");
    }
    if (bf_occurrence_mark_projected(spine, 999999, NULL) != BF_OCCURRENCE_INVALID) {
        return fail("projecting a missing id should be invalid");
    }

    bf_occurrence_close(spine);
    unlink(db_path);

    /* Digest-parity line: the harness compares this to the Python reference's
     * digest_for() for the identical tuple. */
    char digest[33];
    bf_occurrence_digest("github", "octocat", "inbound_reply", "issue#42",
                         "2026-08-16T12:00:00+00:00", digest);
    printf("DIGEST=%s\n", digest);
    printf("occurrence_spine_test: PASS\n");
    return 0;
}
