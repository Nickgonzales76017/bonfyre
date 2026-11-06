#include "bf_target_obligation.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "target obligation check failed: %s\n", #x); return 1; } } while (0)

int main(void) {
    char path[256];
    BfTargetObligation *owner = NULL;
    BfTargetObligationRecord target;
    BfTargetPredicateRecord *predicates = NULL;
    size_t count = 0;
    BfTargetObligationSpec spec = {
        "target:arbitrary-42", "event:arbitrary-42/readiness", "contract:event-readiness.v1",
        "proof-frontier:seed", "resilient"
    };
    BfTargetPredicateSpec refs[] = {
        {"venue", "CommitmentGraph", "event:arbitrary-42/venue:river", "commitment", "discharged",
         "2026-10-01T18:00:00Z", "2026-10-02T02:00:00Z", "venue-receipt", "", 1, 1, 0},
        {"capacity", "ResourceReservation", "event:arbitrary-42/equipment:pa", "capacity", "reserved",
         "2026-10-01T18:00:00Z", "2026-10-02T02:00:00Z", "", "reserve:equipment", 7, 7, 0},
    };
    snprintf(path, sizeof(path), "/tmp/bonfyre-target-obligation-%ld.db", (long)getpid());
    unlink(path);
    CHECK(bf_target_obligation_open(path, &owner, stderr) == 0);
    CHECK(bf_target_obligation_require(owner, &spec, refs, 2) == 0);
    CHECK(bf_target_obligation_get(owner, spec.target_id, &target) == 0);
    CHECK(target.state == BF_TARGET_OPEN);
    CHECK(!strcmp(target.target, "event:arbitrary-42/readiness"));
    CHECK(bf_target_obligation_list_predicates(owner, spec.target_id, &predicates, &count) == 0);
    CHECK(count == 2);
    CHECK(!strcmp(predicates[0].predicate_id, "capacity") || !strcmp(predicates[1].predicate_id, "capacity"));
    bf_target_obligation_free_predicates(predicates);
    /* A frontier being green is not a proof receipt and cannot terminate it. */
    CHECK(bf_target_obligation_get(owner, spec.target_id, &target) == 0);
    CHECK(target.state == BF_TARGET_OPEN);
    CHECK(bf_target_obligation_prove(owner, spec.target_id, "proof:independent-42") == 0);
    CHECK(bf_target_obligation_get(owner, spec.target_id, &target) == 0);
    CHECK(target.state == BF_TARGET_PROVEN);
    CHECK(bf_target_obligation_require(owner, &spec, refs, 2) == -2);
    bf_target_obligation_close(owner);
    unlink(path);
    puts("target obligation: PASS");
    return 0;
}
