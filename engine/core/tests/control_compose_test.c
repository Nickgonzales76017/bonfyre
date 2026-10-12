/* Native composition proof: the OccurrenceSpine, ActorGraph and admission
 * controller all co-inhabit ONE control-plane sqlite database and drive a single
 * native tick end to end -- observe an external occurrence, record the actor it
 * concerns, admit a resource grant to process it, then fold the occurrence out
 * of the pending set. This is the substrate a supervisor calls; the companion
 * harness reopens the same db from Python and confirms every store is consistent.
 */

#include "bf_occurrence.h"
#include "bf_actor.h"
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

int main(int argc, char **argv) {
    char default_path[] = "/tmp/bf_control_XXXXXX";
    const char *db_path = default_path;
    if (argc > 1) {
        db_path = argv[1];
    } else {
        int fd = mkstemp(default_path);
        if (fd >= 0) close(fd);
        unlink(default_path);
    }

    /* All three subsystems open the SAME database -- one control-plane store. */
    BfOccurrenceSpine *spine = NULL;
    BfActorGraph *actors = NULL;
    BfAdmissionGrants *grants = NULL;
    if (bf_occurrence_open(db_path, &spine, stderr) != 0) return fail("occurrence open");
    if (bf_actor_open(db_path, &actors, stderr) != 0) return fail("actor open");
    if (bf_admission_open(db_path, &grants, stderr) != 0) return fail("admission open");

    /* 1. An external reply is observed. */
    BfOccurrenceSpec occ;
    memset(&occ, 0, sizeof(occ));
    occ.source = "github";
    occ.actor = "person:alex";
    occ.event_kind = "inbound_reply";
    occ.subject_ref = "bernstein#3903";
    occ.observed_at = "2026-08-16T09:00:00+00:00";
    int64_t occ_id = 0;
    if (bf_occurrence_observe(spine, &occ, &occ_id) != BF_OCCURRENCE_OK || occ_id <= 0) {
        return fail("observe");
    }

    /* 2. The actor the occurrence concerns is recorded (asserted -- an inbound
     *    reply is someone's claim, not yet human-verified). */
    BfActorNodeSpec who;
    memset(&who, 0, sizeof(who));
    who.actor_id = "person:alex";
    who.node_kind = "person";
    who.display_name = "Alex";
    who.confidence = "asserted";
    who.provenance = "github reply on bernstein#3903";
    if (bf_actor_upsert(actors, &who, NULL) != BF_ACTOR_OK) {
        return fail("upsert actor");
    }

    /* 3. A resource grant is admitted to process the occurrence. */
    BfResourceRequest req;
    memset(&req, 0, sizeof(req));
    req.plane = "control";
    req.kind = "reply-processing";
    req.volume = "/data";
    req.estimated_bytes = 2 * GIB;
    int64_t grant_id = 0;
    if (bf_admission_request_grant(grants, &req, NULL, 100 * GIB, "2026-08-16T09:00:01+00:00",
                                   NULL, &grant_id) != BF_ADMISSION_ADMIT || grant_id <= 0) {
        return fail("admit grant");
    }

    /* 4. The occurrence is folded out of the pending set; the grant is released. */
    if (bf_occurrence_mark_projected(spine, occ_id, "2026-08-16T09:00:02+00:00") != BF_OCCURRENCE_OK) {
        return fail("mark projected");
    }
    if (bf_admission_release_grant(grants, grant_id, "2026-08-16T09:00:03+00:00") != 0) {
        return fail("release grant");
    }

    /* Post-tick invariants, all read from the one store. */
    if (bf_occurrence_unprojected_count(spine) != 0) return fail("occurrence should be projected");
    if (bf_actor_unverified_count(actors) != 1) return fail("alex should be unverified");
    if (bf_admission_committed_bytes(grants, "/data", NULL) != 0) return fail("grant should be released");

    bf_admission_close(grants);
    bf_actor_close(actors);
    bf_occurrence_close(spine);

    printf("DB=%s\n", db_path);
    printf("control_compose_test: PASS\n");
    return 0;
}
