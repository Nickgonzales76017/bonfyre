#include "bf_commitment.h"

#include <stdio.h>
#include <string.h>

#define CHECK(condition) do { if (!(condition)) { fprintf(stderr, "commitment_test:%d: %s\n", __LINE__, #condition); return 1; } } while (0)

static BfCommitmentSpec open_spec(const char *id, const char *committer, const char *beneficiary, const char *expiry) {
    BfCommitmentSpec spec = {0};
    spec.commitment_id = id; spec.committer = committer; spec.beneficiary = beneficiary;
    spec.resource = "room-on-friday"; spec.expires_at = expiry; spec.enactment = "quote-accept";
    return spec;
}

int main(void) {
    BfCommitment *commitment = NULL;
    BfCommitmentSpec spec = open_spec("c1", "venue", "organizer", "2026-01-01T00:00:00Z");
    BfCommitmentState state; BfCommitmentList list; BfCommitmentEvents history;
    char origin[BF_COMMITMENT_ID_MAX], committer[BF_COMMITMENT_ID_MAX]; int found, delivered, ever, delegated;
    CHECK(bf_commitment_open(":memory:", &commitment, stderr) == BF_COMMITMENT_OK);
    CHECK(bf_commitment_record(commitment, &spec) == BF_COMMITMENT_OK);
    CHECK(bf_commitment_status(commitment, "c1", &state, &found) == BF_COMMITMENT_OK && found && state == BF_COMMITMENT_OPEN);
    CHECK(bf_commitment_delivered(commitment, "c1", &delivered) == BF_COMMITMENT_OK && !delivered);
    CHECK(bf_commitment_violable(commitment, "2026-06-01T00:00:00Z", &list) == BF_COMMITMENT_OK && list.count == 1 && !strcmp(list.ids[0], "c1"));
    CHECK(bf_commitment_status(commitment, "c1", &state, &found) == BF_COMMITMENT_OK && state == BF_COMMITMENT_OPEN);
    CHECK(bf_commitment_delegate(commitment, "c1", "sub-venue", "c1-d", NULL, "2026-01-02T00:00:00Z") == BF_COMMITMENT_OK);
    CHECK(bf_commitment_status(commitment, "c1", &state, &found) == BF_COMMITMENT_OK && state == BF_COMMITMENT_DELEGATED);
    CHECK(bf_commitment_status(commitment, "c1-d", &state, &found) == BF_COMMITMENT_OK && state == BF_COMMITMENT_OPEN);
    CHECK(bf_commitment_delegation_chain(commitment, "c1-d", origin, committer, &delegated) == BF_COMMITMENT_OK && delegated && !strcmp(origin, "venue") && !strcmp(committer, "sub-venue"));
    CHECK(bf_commitment_outstanding(commitment, "organizer", &list) == BF_COMMITMENT_OK && list.count == 2);
    CHECK(bf_commitment_violate(commitment, "c1", "organizer", "sub-venue never showed", "") == BF_COMMITMENT_OK);
    CHECK(bf_commitment_discharge(commitment, "c1", "venue", "late delivery", "") == BF_COMMITMENT_OK);
    CHECK(bf_commitment_was_ever(commitment, "c1", BF_COMMITMENT_VIOLATED, &ever) == BF_COMMITMENT_OK && ever);
    CHECK(bf_commitment_delivered(commitment, "c1", &delivered) == BF_COMMITMENT_OK && delivered);
    CHECK(bf_commitment_outstanding(commitment, "organizer", &list) == BF_COMMITMENT_OK && list.count == 1 && !strcmp(list.ids[0], "c1-d"));
    CHECK(bf_commitment_compensate(commitment, "c1-d", "venue", "refund", "") == BF_COMMITMENT_TRANSITION);
    CHECK(bf_commitment_violate(commitment, "c1-d", "organizer", "missed", "") == BF_COMMITMENT_OK);
    CHECK(bf_commitment_compensate(commitment, "c1-d", "venue", "refund", "") == BF_COMMITMENT_OK);
    CHECK(bf_commitment_discharge(commitment, "c1-d", "venue", "late", "") == BF_COMMITMENT_TRANSITION);
    CHECK(bf_commitment_history(commitment, "c1", &history) == BF_COMMITMENT_OK && history.count == 4);
    CHECK(!strcmp(history.events[0].state, "open") && !strcmp(history.events[1].state, "delegated") && !strcmp(history.events[2].state, "violated"));
    /* The late discharge remains the fourth append-only event. */
    CHECK(!strcmp(history.events[3].state, "discharged"));
    CHECK(bf_commitment_record(commitment, &spec) == BF_COMMITMENT_EXISTS);
    bf_commitment_close(commitment);
    puts("commitment_test: PASS"); return 0;
}
