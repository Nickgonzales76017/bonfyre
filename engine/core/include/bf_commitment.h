#ifndef BF_COMMITMENT_H
#define BF_COMMITMENT_H

/*
 * CommitmentGraph's native institutional boundary.
 *
 * This owns the `commitments` and append-only `commitment_events` SQLite
 * relations formerly owned only by commitment_graph.py.  A promise is never a
 * delivery; delegation never releases its original obligor; and a clock may
 * nominate an expired promise for review but may not record a violation.
 */

#include <stddef.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BF_COMMITMENT_ID_MAX 256
#define BF_COMMITMENT_TEXT_MAX 512
#define BF_COMMITMENT_LIST_MAX 256

typedef struct BfCommitment BfCommitment;

typedef enum BfCommitmentStatus {
    BF_COMMITMENT_OK = 0,
    BF_COMMITMENT_INVALID,
    BF_COMMITMENT_NOT_FOUND,
    BF_COMMITMENT_EXISTS,
    BF_COMMITMENT_TRANSITION,
    BF_COMMITMENT_STORAGE,
    BF_COMMITMENT_CAPACITY
} BfCommitmentStatus;

typedef enum BfCommitmentState {
    BF_COMMITMENT_OPEN,
    BF_COMMITMENT_DISCHARGED,
    BF_COMMITMENT_DELEGATED,
    BF_COMMITMENT_VIOLATED,
    BF_COMMITMENT_COMPENSATED
} BfCommitmentState;

typedef struct BfCommitmentSpec {
    const char *commitment_id;
    const char *committer;
    const char *beneficiary;
    const char *resource;
    const char *conditions;
    const char *expires_at;
    const char *authority;
    const char *origin;
    const char *enactment;
    const char *at;
} BfCommitmentSpec;

typedef struct BfCommitmentEvent {
    char state[32];
    char actor[BF_COMMITMENT_ID_MAX];
    char detail[BF_COMMITMENT_TEXT_MAX];
    char at[64];
} BfCommitmentEvent;

typedef struct BfCommitmentEvents {
    size_t count;
    BfCommitmentEvent events[BF_COMMITMENT_LIST_MAX];
} BfCommitmentEvents;

typedef struct BfCommitmentList {
    size_t count;
    char ids[BF_COMMITMENT_LIST_MAX][BF_COMMITMENT_ID_MAX];
    BfCommitmentState states[BF_COMMITMENT_LIST_MAX];
} BfCommitmentList;

int bf_commitment_open(const char *database_path, BfCommitment **out, FILE *err);
int bf_commitment_open_database(void *sqlite_database, BfCommitment **out, FILE *err);
void bf_commitment_close(BfCommitment *commitment);

BfCommitmentStatus bf_commitment_record(BfCommitment *commitment,
                                        const BfCommitmentSpec *spec);
BfCommitmentStatus bf_commitment_status(BfCommitment *commitment, const char *commitment_id,
                                        BfCommitmentState *out, int *out_found);
BfCommitmentStatus bf_commitment_history(BfCommitment *commitment, const char *commitment_id,
                                         BfCommitmentEvents *out);
BfCommitmentStatus bf_commitment_was_ever(BfCommitment *commitment, const char *commitment_id,
                                          BfCommitmentState state, int *out);
/* Delivery requires an explicit discharge event; recording is never delivery. */
BfCommitmentStatus bf_commitment_delivered(BfCommitment *commitment, const char *commitment_id,
                                           int *out);

BfCommitmentStatus bf_commitment_transition(BfCommitment *commitment, const char *commitment_id,
                                            BfCommitmentState to, const char *actor,
                                            const char *detail, const char *at);
BfCommitmentStatus bf_commitment_discharge(BfCommitment *commitment, const char *commitment_id,
                                           const char *actor, const char *detail, const char *at);
BfCommitmentStatus bf_commitment_violate(BfCommitment *commitment, const char *commitment_id,
                                         const char *actor, const char *detail, const char *at);
BfCommitmentStatus bf_commitment_compensate(BfCommitment *commitment, const char *commitment_id,
                                            const char *actor, const char *detail, const char *at);

/* Creates a separately open delegated promise and leaves the original live in
 * `delegated`; both continue to be owed to the same beneficiary. */
BfCommitmentStatus bf_commitment_delegate(BfCommitment *commitment,
                                          const char *commitment_id,
                                          const char *to_committer,
                                          const char *delegated_id,
                                          const char *actor, const char *at);
BfCommitmentStatus bf_commitment_delegation_chain(BfCommitment *commitment,
                                                   const char *commitment_id,
                                                   char origin[BF_COMMITMENT_ID_MAX],
                                                   char committer[BF_COMMITMENT_ID_MAX],
                                                   int *out_has_delegate);

/* `violable` is a candidate query only. Neither it nor `outstanding` mutates
 * the event log. Results are ordered by commitment id and bounded. */
BfCommitmentStatus bf_commitment_violable(BfCommitment *commitment, const char *now,
                                          BfCommitmentList *out);
BfCommitmentStatus bf_commitment_outstanding(BfCommitment *commitment,
                                             const char *beneficiary,
                                             BfCommitmentList *out);

const char *bf_commitment_state_name(BfCommitmentState state);
const char *bf_commitment_status_name(BfCommitmentStatus status);

#ifdef __cplusplus
}
#endif

#endif
