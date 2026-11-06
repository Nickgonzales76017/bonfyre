#ifndef BF_ACTIVATION_H
#define BF_ACTIVATION_H

#include <stdint.h>
#include <stdio.h>

#include "bf_authority.h"
#include "bf_service_lifecycle.h"

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
 * Project an independently-derived eligibility fact without overwriting
 * activation authority/binding metadata already attached to the resource.
 *
 * This is the safe bridge for SupplyMatch/DemandBook-style projections:
 * - qualified is asserted because eligibility is downstream of qualification
 * - eligible is set/retracted from the supplied fact
 * - mechanism and activate_actor are preserved on an existing row
 * - no authority or binding is inferred
 */
int bf_activation_project_eligibility(BfActivation *act, const char *resource_id,
                                      int eligible, const char *detail, const char *now);

/* Record an actor's explicit selection of an activation mechanism. This neither
 * grants authority nor changes independently projected qualification/eligibility. */
int bf_activation_select_actor(BfActivation *act, const char *resource_id,
                               const char *actor, const char *mechanism,
                               const char *detail, const char *now);

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

/*
 * Read the two independent owner facts required to make a candidate usable:
 * AuthorityGraph's `activate` grant over the resource and ServiceLifecycle's
 * current binding by the candidate's elected actor. This function never grants,
 * elects, binds, or writes an activation state.
 */
BfActivationState bf_activation_reconcile(BfActivation *act, BfAuthority *authority,
                                          BfServiceLifecycle *lifecycle,
                                          const char *resource_id, const char *at_iso);

const char *bf_activation_state_name(BfActivationState state);

#endif
