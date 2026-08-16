/* Native conformance for the resource-admission controller: the three-verdict
 * decision kernel (admit/defer/reject, with "never" never collapsing into "not
 * now"), committed-bytes over-commit prevention, and the grant ledger on the
 * shared resource_grants store. The CASE= lines are compared verdict-for-verdict
 * against the Python decide() by the companion harness; the DB= line lets Python
 * reopen the ledger this test wrote. */

#include "bf_admission.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define GIB ((int64_t)1 << 30)

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

/* Shared battery: {free, committed, plane_committed, estimate} in GiB. Kept
 * numerically identical in the Python cross-check. */
static const struct {
    int64_t free_g, committed_g, plane_g, est_g;
    BfAdmissionVerdict expect;
} CASES[] = {
    {100, 0, 0, 0, BF_ADMISSION_REJECT},   /* non-positive estimate */
    {100, 0, 0, 50, BF_ADMISSION_REJECT},  /* over single-grant ceiling (40G) */
    {100, 0, 19, 5, BF_ADMISSION_DEFER},   /* would exceed 20G per-plane quota */
    {100, 0, 0, 5, BF_ADMISSION_ADMIT},    /* fits above the floor */
    {15, 0, 0, 8, BF_ADMISSION_REJECT},    /* cannot fit even if drained (never) */
    {30, 15, 0, 8, BF_ADMISSION_DEFER},    /* not now: committed holds the space */
    {60, 40, 0, 15, BF_ADMISSION_DEFER},   /* over-commit: committed subtracted (15<=40 ceiling, >spendable 10, fits if drained) */
};

int main(int argc, char **argv) {
    size_t n = sizeof(CASES) / sizeof(CASES[0]);
    for (size_t i = 0; i < n; i++) {
        BfResourceRequest req;
        memset(&req, 0, sizeof(req));
        req.plane = "test";
        req.kind = "scratch";
        req.volume = "/data";
        req.estimated_bytes = CASES[i].est_g * GIB;
        BfAdmissionDecision d;
        BfAdmissionVerdict v = bf_admission_decide(
            &req, NULL, CASES[i].free_g * GIB, CASES[i].committed_g * GIB,
            CASES[i].plane_g * GIB, &d);
        if (v != CASES[i].expect) {
            fprintf(stderr, "case %zu: got %s\n", i, bf_admission_verdict_name(v));
            return fail("decide verdict mismatch");
        }
        printf("CASE=%zu:%s\n", i, bf_admission_verdict_name(v));
    }

    /* Ledger on the shared store. */
    char default_path[] = "/tmp/bf_admit_XXXXXX";
    const char *db_path = default_path;
    if (argc > 1) {
        db_path = argv[1];
    } else {
        int fd = mkstemp(default_path);
        if (fd >= 0) close(fd);
        unlink(default_path);
    }

    BfAdmissionGrants *grants = NULL;
    if (bf_admission_open(db_path, &grants, stderr) != 0) {
        return fail("open");
    }

    /* Grants stay under the 20G per-plane quota and 40G ceiling. */
    BfResourceRequest a;
    memset(&a, 0, sizeof(a));
    a.plane = "feldera";
    a.kind = "circuit-state";
    a.volume = "/data";
    a.estimated_bytes = 8 * GIB;

    int64_t id1 = 0;
    if (bf_admission_request_grant(grants, &a, NULL, 100 * GIB, "2026-01-01T00:00:00+00:00",
                                   NULL, &id1) != BF_ADMISSION_ADMIT || id1 <= 0) {
        return fail("first grant should be admitted");
    }
    if (bf_admission_committed_bytes(grants, "/data", NULL) != 8 * GIB) {
        return fail("committed should be 8G");
    }

    BfResourceRequest b = a;
    b.plane = "habitat";
    b.estimated_bytes = 8 * GIB;
    if (bf_admission_request_grant(grants, &b, NULL, 100 * GIB, "2026-01-01T05:00:00+00:00",
                                   NULL, NULL) != BF_ADMISSION_ADMIT) {
        return fail("second grant should be admitted");
    }
    if (bf_admission_committed_bytes(grants, "/data", NULL) != 16 * GIB) {
        return fail("committed should be 16G after two grants");
    }

    /* Over-commit prevention: with 16G committed and only 30G free, spendable is
     * 30-10-16 = 4G. An 8G request (within quota and ceiling) exceeds spendable
     * but would fit if grants were drained -> DEFER, never admitted against
     * already-committed space, and no ledger row is written. */
    BfResourceRequest c = a;
    c.plane = "market";
    c.estimated_bytes = 8 * GIB;
    if (bf_admission_request_grant(grants, &c, NULL, 30 * GIB, NULL, NULL, NULL)
        != BF_ADMISSION_DEFER) {
        return fail("8G against 4G spendable should defer");
    }
    if (bf_admission_committed_bytes(grants, "/data", NULL) != 16 * GIB) {
        return fail("a deferred request must not touch the ledger");
    }

    /* Release the first grant; committed drops to just the second. */
    if (bf_admission_release_grant(grants, id1, "2026-01-01T06:00:00+00:00") != 0) {
        return fail("release");
    }
    if (bf_admission_committed_bytes(grants, "/data", NULL) != 8 * GIB) {
        return fail("committed should be 8G after release");
    }

    /* A grant a crashed plane never returned: dated 2020, reaped as stale while a
     * fresh grant (dated 05:00) survives the cutoff. */
    BfResourceRequest old = a;
    old.plane = "stale";
    old.estimated_bytes = 5 * GIB;
    if (bf_admission_request_grant(grants, &old, NULL, 100 * GIB, "2020-01-01T00:00:00+00:00",
                                   NULL, NULL) != BF_ADMISSION_ADMIT) {
        return fail("old grant admit");
    }
    /* cutoff = 05:30 - 1h = 04:30: the 2020 grant is stale; the 05:00 grant survives. */
    int64_t reaped = bf_admission_reap_expired(grants, 3600, "2026-01-01T05:30:00+00:00");
    if (reaped != 1) {
        return fail("exactly the 2020 grant should be reaped");
    }
    if (bf_admission_committed_bytes(grants, "/data", NULL) != 8 * GIB) {
        return fail("committed should be 8G after reaping the stale grant");
    }

    bf_admission_close(grants);
    printf("DB=%s\n", db_path);
    printf("admission_test: PASS\n");
    return 0;
}
