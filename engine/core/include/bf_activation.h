#ifndef BF_ACTIVATION_H
#define BF_ACTIVATION_H

#include <stdint.h>
#include <stdio.h>

/*
 * Native resource-activation state machine -- native peer of resource_activation.py.
 * Writes the SAME resource_candidates store, so native and Python agree on the
 * furthest activation state a resource has reached.
 *
 * The ordered states are gap < candidate < qualified < eligible < authorized <
 * activated. A resource is usable ONLY at activated, and the activate-authority
 * gate cannot be short-circuited: the ordering makes authorized unreachable
 * unless authority holds. Authority itself is determined elsewhere (its own
 * fact); this module takes the has_authority result as an input so the state
 * machine stays decoupled from the authority calculus.
 */

typedef struct BfActivation BfActivation;

typedef enum BfActivationState {
    BF_ACT_GAP = 0,       /* no candidate recorded */
    BF_ACT_CANDIDATE,     /* recorded, not yet qualified */
    BF_ACT_QUALIFIED,     /* qualified, not yet eligible */
    BF_ACT_ELIGIBLE,      /* eligible, activate authority missing */
    BF_ACT_AUTHORIZED,    /* authorized, mechanism not yet bound */
    BF_ACT_ACTIVATED      /* usable */
} BfActivationState;

typedef struct BfActivationCandidate {
    const char *resource_id;   /* required */
    const char *mechanism;     /* optional (default "service") */
    int qualified;
    int eligible;
    const char *activate_actor; /* optional (default "") */
    const char *detail;        /* optional (default "") */
} BfActivationCandidate;

int bf_activation_open(const char *database_path, BfActivation **out, FILE *err);
int bf_activation_open_database(void *sqlite_database, BfActivation **out, FILE *err);
void bf_activation_close(BfActivation *act);

/* Insert or update a candidate by resource_id. now may be NULL. Returns 0/-1. */
int bf_activation_record(BfActivation *act, const BfActivationCandidate *cand, const char *now);

/*
 * The furthest state a resource has reached. has_authority is the result of the
 * separate authority check (the activate-authority gate); is_bound is whether the
 * activation mechanism is bound. Returns the state; BF_ACT_GAP if no candidate.
 */
BfActivationState bf_activation_state(BfActivation *act, const char *resource_id,
                                      int has_authority, int is_bound);

/* Convenience: is the resource usable (state == activated)? 1/0, -1 on error. */
int bf_activation_is_activated(BfActivation *act, const char *resource_id,
                               int has_authority, int is_bound);

const char *bf_activation_state_name(BfActivationState state);

#endif
