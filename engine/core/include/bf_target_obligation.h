#ifndef BF_TARGET_OBLIGATION_H
#define BF_TARGET_OBLIGATION_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

/* TargetObligation is persistent semantic purpose.  It deliberately stores
 * references to other owners rather than their asserted state: the inverse
 * compiler reevaluates each reference at compilation/effect time. */
typedef struct BfTargetObligation BfTargetObligation;

typedef enum BfTargetState {
    BF_TARGET_OPEN = 0,
    BF_TARGET_WAITING_EXTERNAL_PREDICATE,
    BF_TARGET_PROVEN,
    BF_TARGET_SUPERSEDED
} BfTargetState;

typedef struct BfTargetObligationSpec {
    const char *target_id;
    const char *target;
    const char *semantic_contract;
    const char *source_frontier;
    const char *preference_policy;
} BfTargetObligationSpec;

/* adapter/native_state/owner_query are the generated OwnerPredicateGrammar
 * ABI.  The Target owner persists them verbatim; it does not choose an owner
 * opcode from a string or independently interpret expected state. */
typedef struct BfTargetPredicateSpec {
    const char *predicate_id;
    const char *owner;
    const char *subject;
    const char *predicate;
    const char *expected_state;
    const char *valid_start;
    const char *valid_end;
    const char *evidence_requirement;
    const char *authority_requirement;
    uint8_t adapter;
    uint8_t native_state;
    uint16_t owner_query;
} BfTargetPredicateSpec;

typedef struct BfTargetObligationRecord {
    char target_id[128];
    char target[512];
    char semantic_contract[512];
    char source_frontier[256];
    char preference_policy[96];
    BfTargetState state;
    char terminal_reason[512];
} BfTargetObligationRecord;

typedef struct BfTargetPredicateRecord {
    char predicate_id[128];
    char owner[96];
    char subject[512];
    char predicate[128];
    char expected_state[64];
    char valid_start[64];
    char valid_end[64];
    char evidence_requirement[256];
    char authority_requirement[256];
    uint8_t adapter;
    uint8_t native_state;
    uint16_t owner_query;
} BfTargetPredicateRecord;

int bf_target_obligation_open(const char *database_path, BfTargetObligation **out, FILE *err);
void bf_target_obligation_close(BfTargetObligation *owner);
int bf_target_obligation_require(BfTargetObligation *owner, const BfTargetObligationSpec *spec,
                                 const BfTargetPredicateSpec *predicates, size_t predicate_count);
int bf_target_obligation_get(BfTargetObligation *owner, const char *target_id,
                             BfTargetObligationRecord *out);
/* Caller owns returned rows and frees them with bf_target_obligation_free_predicates. */
int bf_target_obligation_list_predicates(BfTargetObligation *owner, const char *target_id,
                                         BfTargetPredicateRecord **out, size_t *count);
void bf_target_obligation_free_predicates(BfTargetPredicateRecord *predicates);
/* Only an independently named proof/evidence owner may call this boundary.
 * The inverse compiler must never call it merely because its frontier is green. */
int bf_target_obligation_prove(BfTargetObligation *owner, const char *target_id,
                               const char *proof_receipt);
int bf_target_obligation_supersede(BfTargetObligation *owner, const char *target_id,
                                   const char *reason);
const char *bf_target_obligation_state_name(BfTargetState state);

#endif
