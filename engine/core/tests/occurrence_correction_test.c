/* Native occurrence correction/retraction: a correction supersedes a prior
 * occurrence (the -1), and the fold withdraws the stale status while the
 * correction's status stands. Append-only and auditable -- nothing is deleted. */

#include "bf_occurrence.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

int main(void) {
    char db_path[] = "/tmp/bf_occ_corr_XXXXXX";
    int fd = mkstemp(db_path);
    if (fd >= 0) close(fd);
    unlink(db_path);

    BfOccurrenceSpine *spine = NULL;
    if (bf_occurrence_open(db_path, &spine, stderr) != 0) return fail("open");

    /* an original observation: an inbound reply (projects to 'replied') */
    BfOccurrenceSpec orig;
    memset(&orig, 0, sizeof(orig));
    orig.source = "github";
    orig.actor = "octocat";
    orig.event_kind = "inbound_reply";
    orig.subject_ref = "issue#7";
    orig.observed_at = "2026-08-16T09:00:00+00:00";
    int64_t oid = 0;
    if (bf_occurrence_observe(spine, &orig, &oid) != BF_OCCURRENCE_OK) return fail("observe original");
    if (bf_occurrence_is_superseded(spine, oid) != 0) return fail("not yet superseded");

    /* it was misread: correct it with a state_changed occurrence */
    BfOccurrenceSpec corr;
    memset(&corr, 0, sizeof(corr));
    corr.source = "github";
    corr.actor = "octocat";
    corr.event_kind = "state_changed";
    corr.subject_ref = "issue#7";
    corr.observed_at = "2026-08-16T09:30:00+00:00";
    int64_t cid = 0;
    if (bf_occurrence_correct(spine, oid, &corr, "misclassified as a reply", &cid)
        != BF_OCCURRENCE_OK || cid <= oid) {
        return fail("correct");
    }
    if (bf_occurrence_is_superseded(spine, oid) != 1) return fail("original should be superseded");
    if (bf_occurrence_is_superseded(spine, cid) != 0) return fail("correction is not superseded");

    /* correcting a nonexistent occurrence is rejected */
    if (bf_occurrence_correct(spine, 999999, &corr, "x", NULL) != BF_OCCURRENCE_INVALID) {
        return fail("correcting a missing original should be invalid");
    }

    /* fold: two pending (original + correction), both fold, but the original is
     * retracted (no 'replied' status) -- only the correction's status stands. */
    if (bf_occurrence_unprojected_count(spine) != 2) return fail("two pending");
    if (bf_occurrence_project(spine, "2026-08-16T10:00:00+00:00") != 2) return fail("fold both");
    if (bf_occurrence_unprojected_count(spine) != 0) return fail("all folded");

    sqlite3 *db = NULL;
    sqlite3_open(db_path, &db);
    /* the retracted original wrote NO projection */
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM occurrence_projection WHERE event_id=?", -1, &s, NULL);
    sqlite3_bind_int64(s, 1, oid);
    sqlite3_step(s);
    int orig_rows = sqlite3_column_int(s, 0);
    sqlite3_finalize(s);
    /* the correction's status stands */
    sqlite3_prepare_v2(db, "SELECT status FROM occurrence_projection WHERE event_id=?", -1, &s, NULL);
    sqlite3_bind_int64(s, 1, cid);
    int have_corr = (sqlite3_step(s) == SQLITE_ROW);
    char corr_status[32] = {0};
    if (have_corr) snprintf(corr_status, sizeof(corr_status), "%s", sqlite3_column_text(s, 0));
    sqlite3_finalize(s);
    sqlite3_close(db);

    if (orig_rows != 0) return fail("the retracted original must not project a stale status");
    if (!have_corr || strcmp(corr_status, "state_changed") != 0) {
        return fail("the correction's status must stand");
    }

    bf_occurrence_close(spine);
    unlink(db_path);
    printf("occurrence_correction_test: PASS\n");
    return 0;
}
