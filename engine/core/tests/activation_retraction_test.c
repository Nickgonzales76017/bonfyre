/* Cascade witness (SS20): loss of activation retracts reachability.
 *
 *   authority granted -> resource reaches ACTIVATED (usable)
 *   authority REVOKED (Authority -1)
 *   -> the activate gate no longer holds
 *   -> activation drops ACTIVATED -> ELIGIBLE (no longer usable)
 *
 * Native authority + native activation on one db; no Python. */

#include "bf_authority.h"
#include "bf_activation.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

int main(void) {
    char db_path[] = "/tmp/bf_actret_XXXXXX";
    int fd = mkstemp(db_path);
    if (fd >= 0) close(fd);
    unlink(db_path);

    BfAuthority *auth = NULL;
    BfActivation *act = NULL;
    if (bf_authority_open(db_path, &auth, stderr) != 0) return fail("auth open");
    if (bf_activation_open(db_path, &act, stderr) != 0) return fail("act open");

    const char *actor = "person:approver";
    const char *res = "res:gpu-lease";

    /* grant activate authority; record a qualified+eligible candidate with an actor */
    BfAuthorityEdge e;
    memset(&e, 0, sizeof(e));
    e.edge_id = "grant1";
    e.actor = actor;
    e.permission = "activate";
    e.subject = res;
    if (bf_authority_grant(auth, &e, NULL) != 0) return fail("grant");

    BfActivationCandidate c;
    memset(&c, 0, sizeof(c));
    c.resource_id = res;
    c.mechanism = "gpu_lease";
    c.qualified = 1;
    c.eligible = 1;
    c.activate_actor = actor;
    if (bf_activation_record(act, &c, NULL) != 0) return fail("record candidate");

    /* the activate gate reads authority; with it granted and bound -> ACTIVATED */
    int has = bf_authority_has(auth, actor, "activate", res, NULL, NULL);
    if (has != 1) return fail("authority should hold");
    if (bf_activation_state(act, res, has, 1) != BF_ACT_ACTIVATED) {
        return fail("resource should be activated (usable)");
    }

    /* Authority -1: revoke the grant */
    if (bf_authority_revoke(auth, "grant1") != 1) return fail("revoke");
    has = bf_authority_has(auth, actor, "activate", res, NULL, NULL);
    if (has != 0) return fail("authority must be gone after revoke");

    /* the cascade: activation drops out of ACTIVATED -- reachability retracted */
    BfActivationState after = bf_activation_state(act, res, has, 1);
    if (after == BF_ACT_ACTIVATED) {
        return fail("loss of authority must retract activation");
    }
    if (after != BF_ACT_ELIGIBLE) {
        return fail("without authority the resource falls back to ELIGIBLE");
    }
    if (bf_activation_is_activated(act, res, has, 1) != 0) {
        return fail("resource must no longer be usable");
    }

    bf_activation_close(act);
    bf_authority_close(auth);
    unlink(db_path);
    printf("activation_retraction_test: PASS (Authority -1 -> activation retracts -> not usable)\n");
    return 0;
}
