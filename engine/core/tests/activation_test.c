/* Native conformance for the activation state machine: shared resource_candidates
 * store, the ordered gap<candidate<qualified<eligible<authorized<activated states,
 * and the un-short-circuitable activate-authority gate. Prints DB for a Python
 * shared-store read. */

#include "bf_activation.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

static BfActivationCandidate cand(const char *rid, int qualified, int eligible,
                                  const char *actor) {
    BfActivationCandidate c;
    memset(&c, 0, sizeof(c));
    c.resource_id = rid;
    c.mechanism = "service";
    c.qualified = qualified;
    c.eligible = eligible;
    c.activate_actor = actor;
    return c;
}

int main(int argc, char **argv) {
    char default_path[] = "/tmp/bf_act_XXXXXX";
    const char *db_path = default_path;
    if (argc > 1) {
        db_path = argv[1];
    } else {
        int fd = mkstemp(default_path);
        if (fd >= 0) close(fd);
        unlink(default_path);
    }

    BfActivation *act = NULL;
    if (bf_activation_open(db_path, &act, stderr) != 0) {
        return fail("open");
    }

    /* no candidate -> gap */
    if (bf_activation_state(act, "r:unknown", 1, 1) != BF_ACT_GAP) {
        return fail("unknown resource should be gap");
    }
    /* recorded but not qualified -> candidate */
    BfActivationCandidate c = cand("r:1", 0, 0, "");
    if (bf_activation_record(act, &c, NULL) != 0) return fail("record");
    if (bf_activation_state(act, "r:1", 1, 1) != BF_ACT_CANDIDATE) {
        return fail("not qualified -> candidate");
    }
    /* qualified, not eligible -> qualified */
    c = cand("r:1", 1, 0, "");
    bf_activation_record(act, &c, NULL);
    if (bf_activation_state(act, "r:1", 1, 1) != BF_ACT_QUALIFIED) {
        return fail("qualified, not eligible -> qualified");
    }
    /* eligible but NO actor -> eligible (authority gate not passable) */
    c = cand("r:1", 1, 1, "");
    bf_activation_record(act, &c, NULL);
    if (bf_activation_state(act, "r:1", 1, 1) != BF_ACT_ELIGIBLE) {
        return fail("no actor -> eligible even with has_authority");
    }
    /* eligible, actor set, but has_authority=0 -> eligible (cannot short-circuit) */
    c = cand("r:1", 1, 1, "person:approver");
    bf_activation_record(act, &c, NULL);
    if (bf_activation_state(act, "r:1", 0, 1) != BF_ACT_ELIGIBLE) {
        return fail("authority gate must not be short-circuited");
    }
    /* actor set AND has_authority but not bound -> authorized */
    if (bf_activation_state(act, "r:1", 1, 0) != BF_ACT_AUTHORIZED) {
        return fail("authorized but unbound -> authorized");
    }
    /* everything -> activated (usable) */
    if (bf_activation_state(act, "r:1", 1, 1) != BF_ACT_ACTIVATED) {
        return fail("fully satisfied -> activated");
    }
    if (bf_activation_is_activated(act, "r:1", 1, 1) != 1) {
        return fail("is_activated should be 1");
    }
    if (bf_activation_is_activated(act, "r:1", 1, 0) != 0) {
        return fail("unbound is not activated");
    }

    bf_activation_close(act);
    printf("DB=%s\n", db_path);
    printf("activation_test: PASS\n");
    return 0;
}
