#ifndef BF_PROOF_H
#define BF_PROOF_H

#include <stdint.h>
#include <stdio.h>

/*
 * Native proof frontier -- native peer of proof_frontier.py. Writes the SAME
 * frontier_layers / solved_invariants / known_noncauses stores.
 *
 * The load-bearing discipline is the FPQ lesson, the atlas's central rule: a
 * MEASURED result is not a PROVEN one. is_proven_invariant is true only when the
 * truth plane is 'proven' AND the invariant has not been challenged/invalidated.
 * A challenge RETRACTS proof (the -1 delta): what was proven stops being proven.
 * A KnownNonCause is a durable negative -- a blackholed hypothesis.
 */

typedef struct BfProof BfProof;

int bf_proof_open(const char *database_path, BfProof **out, FILE *err);
int bf_proof_open_database(void *sqlite_database, BfProof **out, FILE *err);
void bf_proof_close(BfProof *proof);

/* --- frontier layers: is a proof-frontier layer proven? --- */
int bf_proof_record_layer(BfProof *proof, const char *subject_resource,
                          const char *subject_profile, int ordinal, const char *layer,
                          const char *status, const char *witness_ref);
/* Returns 1 if the layer's status is 'proven', 0 otherwise, -1 on error. */
int bf_proof_layer_is_proven(BfProof *proof, const char *subject_resource,
                             const char *subject_profile, const char *layer);

/* --- solved invariants: proven requires truth_plane 'proven', not 'measured' --- */
int bf_proof_record_invariant(BfProof *proof, const char *invariant_id,
                              const char *subject_resource, const char *subject_profile,
                              const char *layer, const char *statement,
                              const char *truth_plane, const char *status, const char *now);
/* Challenge (retract) an invariant: status -> 'challenged'. 1 if changed, 0, -1. */
int bf_proof_challenge_invariant(BfProof *proof, const char *invariant_id, const char *now);
/*
 * Is the invariant PROVEN authority? True only if truth_plane == 'proven' AND
 * status is not challenged/invalidated. A 'measured' invariant is never proven.
 * Returns 1 / 0 / -1.
 */
int bf_proof_invariant_is_proven(BfProof *proof, const char *invariant_id);

/* --- known non-causes: durable negatives (blackholes) --- */
int bf_proof_record_noncause(BfProof *proof, const char *noncause_id,
                             const char *hypothesis, const char *subject_scope,
                             const char *experiment, const char *witness_ref, const char *now);
/* Is (hypothesis, subject_scope) a recorded KnownNonCause? 1 / 0 / -1. */
int bf_proof_is_blackholed(BfProof *proof, const char *hypothesis, const char *subject_scope);

#endif
