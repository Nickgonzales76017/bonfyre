/* P0 exit witness: the principal loop runs natively end to end, on ONE database,
 * with no Python-owned semantic transition:
 *
 *   external observation -> native Occurrence -> native WorkGraph (routable) ->
 *   claim -> complete -> native Receipt -> a follow-on native Occurrence carrying
 *   that receipt -> native History (projection).
 *
 * Every step is a native engine call. Python is not in this path. */

#include "bf_occurrence.h"
#include "bf_workgraph.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

int main(void) {
    char db_path[] = "/tmp/bf_p0loop_XXXXXX";
    int fd = mkstemp(db_path);
    if (fd >= 0) close(fd);
    unlink(db_path);

    BfWorkgraph *wg = NULL;
    BfOccurrenceSpine *spine = NULL;
    if (bf_workgraph_open(db_path, &wg, stderr) != 0) return fail("workgraph open");
    if (bf_occurrence_open(db_path, &spine, stderr) != 0) return fail("occurrence open");

    /* 1. external observation -> native Occurrence */
    BfOccurrenceSpec occ;
    memset(&occ, 0, sizeof(occ));
    occ.source = "github";
    occ.actor = "sipyourdrink-ltd/bernstein";
    occ.event_kind = "inbound_reply";
    occ.subject_ref = "bernstein#3903";
    occ.observed_at = "2026-08-16T09:00:00+00:00";
    int64_t occ_id = 0;
    if (bf_occurrence_observe(spine, &occ, &occ_id) != BF_OCCURRENCE_OK || occ_id <= 0) {
        return fail("observe external reply");
    }

    /* 2. canonical work in the native WorkGraph -- a routable family only */
    if (bf_workgraph_register_family(wg, "T_REPLY").status != BF_WORKGRAPH_OK) {
        return fail("register family");
    }
    if (bf_workgraph_create_mission(wg, "m1").status != BF_WORKGRAPH_OK) {
        return fail("create mission");
    }
    BfWorkgraphNodeSpec node;
    memset(&node, 0, sizeof(node));
    node.mission_id = "m1";
    node.node_id = "n1";
    node.operator_id = "process-reply";
    node.family = "T_REPLY";
    if (bf_workgraph_add_node(wg, &node).status != BF_WORKGRAPH_OK) {
        return fail("add node");
    }
    /* an unroutable family is still rejected at insertion (no free-string target) */
    BfWorkgraphNodeSpec bad = node;
    bad.node_id = "nbad";
    bad.family = "coordinator";
    if (bf_workgraph_add_node(wg, &bad).status != BF_WORKGRAPH_INVALID) {
        return fail("unroutable family must be rejected at insertion");
    }

    /* 3. claim -> complete -> native Receipt */
    BfWorkgraphClaimSpec claim;
    memset(&claim, 0, sizeof(claim));
    claim.worker_id = "worker-1";
    claim.family = "T_REPLY";
    claim.lease_ms = 60000;
    BfWorkgraphResult claimed = bf_workgraph_claim_next(wg, &claim);
    if (claimed.status != BF_WORKGRAPH_OK) {
        return fail("claim_next");
    }
    BfWorkgraphResult done = bf_workgraph_complete(wg, "m1", claimed.node_id, "worker-1",
                                                   claimed.claim_token,
                                                   "bonfyre://artifact/reply-processed");
    if (done.status != BF_WORKGRAPH_OK) {
        return fail("complete");
    }
    if (done.receipt_id[0] == '\0') {
        return fail("completion must produce a receipt");
    }

    /* 4. Effect/Receipt produces a follow-on native Occurrence (the loop closes) */
    BfOccurrenceSpec back;
    memset(&back, 0, sizeof(back));
    back.source = "workgraph";
    back.actor = "sipyourdrink-ltd/bernstein";
    back.event_kind = "state_changed";
    back.subject_ref = "bernstein#3903";
    back.observed_at = "2026-08-16T09:05:00+00:00";
    back.evidence_ref = done.receipt_id; /* the receipt is the evidence */
    int64_t occ2 = 0;
    if (bf_occurrence_observe(spine, &back, &occ2) != BF_OCCURRENCE_OK || occ2 <= occ_id) {
        return fail("receipt should produce a new occurrence");
    }

    /* 5. native History: both occurrences pending, then folded */
    if (bf_occurrence_unprojected_count(spine) != 2) {
        return fail("two occurrences should be pending");
    }
    if (bf_occurrence_project(spine, "2026-08-16T09:06:00+00:00") != 2) {
        return fail("project should fold both");
    }
    if (bf_occurrence_unprojected_count(spine) != 0) {
        return fail("history should be fully folded");
    }

    bf_occurrence_close(spine);
    bf_workgraph_close(wg);
    unlink(db_path);
    printf("p0_loop_test: PASS (native observation -> work -> receipt -> occurrence -> history)\n");
    return 0;
}
