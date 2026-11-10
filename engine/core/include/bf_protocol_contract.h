#ifndef BF_PROTOCOL_CONTRACT_H
#define BF_PROTOCOL_CONTRACT_H

/* Native ProtocolContract compiler. A contract declares information causality;
 * this derives concurrent waves, detects liveness/safety/integrity defects,
 * then opens its obligations through the native CommitmentGraph owner. */

#include "bf_commitment.h"
#include "bf_authority.h"
#include "bf_evidence.h"
#include "bf_information_policy.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BF_PROTOCOL_ID_MAX 128
#define BF_PROTOCOL_MAX_ROLES 64
#define BF_PROTOCOL_MAX_PARAMETERS 64
#define BF_PROTOCOL_MAX_COMMITMENTS 64
#define BF_PROTOCOL_MAX_EVIDENCE_REQUIREMENTS 64

typedef enum BfProtocolStatus { BF_PROTOCOL_OK = 0, BF_PROTOCOL_INVALID, BF_PROTOCOL_EXISTS, BF_PROTOCOL_NOT_FOUND, BF_PROTOCOL_CAPACITY, BF_PROTOCOL_NONCONFORMANT, BF_PROTOCOL_STORAGE } BfProtocolStatus;

typedef struct BfProtocolRoleSpec {
    const char *name;
    const char *authority;
    const char *const *requires;
    size_t requires_count;
    const char *const *produces;
    size_t produces_count;
    /* NULL means reads defaults exactly to requires. */
    const char *const *reads;
    size_t reads_count;
} BfProtocolRoleSpec;

typedef struct BfProtocolCommitmentSpec { const char *key; const char *committer; const char *beneficiary; const char *resource; const char *discharged_by; } BfProtocolCommitmentSpec;
typedef struct BfProtocolActor { const char *role; const char *actor; } BfProtocolActor;

typedef struct BfProtocolRole { char name[BF_PROTOCOL_ID_MAX]; char authority[BF_PROTOCOL_ID_MAX]; uint64_t requires; uint64_t produces; uint64_t reads; } BfProtocolRole;
typedef struct BfProtocolCommitment { char key[BF_PROTOCOL_ID_MAX]; size_t committer; size_t beneficiary; char resource[BF_PROTOCOL_ID_MAX]; char discharged_by[BF_PROTOCOL_ID_MAX]; } BfProtocolCommitment;
typedef struct BfProtocolContract {
    char name[BF_PROTOCOL_ID_MAX];
    char parameters[BF_PROTOCOL_MAX_PARAMETERS][BF_PROTOCOL_ID_MAX];
    size_t parameter_count;
    uint64_t given;
    uint64_t keys;
    BfProtocolRole roles[BF_PROTOCOL_MAX_ROLES];
    size_t role_count;
    BfProtocolCommitment commitments[BF_PROTOCOL_MAX_COMMITMENTS];
    size_t commitment_count;
    char evidence_required[BF_PROTOCOL_MAX_EVIDENCE_REQUIREMENTS][BF_PROTOCOL_ID_MAX];
    size_t evidence_required_count;
} BfProtocolContract;

typedef struct BfProtocolConformance { uint64_t liveness_roles; uint64_t safety_parameters; uint64_t integrity_roles; } BfProtocolConformance;
typedef struct BfProtocolWaves { size_t wave_count; size_t counts[BF_PROTOCOL_MAX_ROLES]; size_t role_indices[BF_PROTOCOL_MAX_ROLES][BF_PROTOCOL_MAX_ROLES]; } BfProtocolWaves;
typedef struct BfProtocolEnactment { size_t opened_count; char commitment_ids[BF_PROTOCOL_MAX_COMMITMENTS][BF_COMMITMENT_ID_MAX]; } BfProtocolEnactment;
typedef struct BfProtocolClosure {
    int obligations_terminal;
    int evidence_closed;
    BfProtocolEnactment outstanding;
    size_t unevidenced_count;
    char unevidenced_claims[BF_PROTOCOL_MAX_EVIDENCE_REQUIREMENTS][BF_PROTOCOL_ID_MAX];
    BfEvidenceVerdict evidence_verdicts[BF_PROTOCOL_MAX_EVIDENCE_REQUIREMENTS];
} BfProtocolClosure;
typedef struct BfProtocolAuthorityRefusal { char role[BF_PROTOCOL_ID_MAX]; char actor[BF_PROTOCOL_ID_MAX]; char permission[BF_PROTOCOL_ID_MAX]; } BfProtocolAuthorityRefusal;

void bf_protocol_contract_init(BfProtocolContract *contract, const char *name);
BfProtocolStatus bf_protocol_contract_given(BfProtocolContract *contract, const char *parameter);
BfProtocolStatus bf_protocol_contract_key(BfProtocolContract *contract, const char *parameter);
BfProtocolStatus bf_protocol_contract_evidence_required(BfProtocolContract *contract, const char *claim);
BfProtocolStatus bf_protocol_contract_role(BfProtocolContract *contract, const BfProtocolRoleSpec *spec);
BfProtocolStatus bf_protocol_contract_commit(BfProtocolContract *contract, const BfProtocolCommitmentSpec *spec);
BfProtocolStatus bf_protocol_contract_conformance(const BfProtocolContract *contract, const BfInformationPolicy *policy, BfDisclosureAudience audience, BfProtocolConformance *out);
BfProtocolStatus bf_protocol_contract_waves(const BfProtocolContract *contract, BfProtocolWaves *out);
BfProtocolStatus bf_protocol_contract_enact(const BfProtocolContract *contract, void *sqlite_database, const char *enactment_id, const BfProtocolActor *actors, size_t actor_count, const char *at, BfProtocolEnactment *out);
/* Conformance proves contract shape; this separately proves that each role with
 * a declared authority holds it over this addressed subject at effect time. */
BfProtocolStatus bf_protocol_contract_authorize(const BfProtocolContract *contract, void *authority_database, const BfProtocolActor *actors, size_t actor_count, const char *subject, const char *at_iso, const char *purpose, BfProtocolAuthorityRefusal *out_refusal);
BfProtocolStatus bf_protocol_contract_enact_authorized(const BfProtocolContract *contract, void *sqlite_database, void *authority_database, const char *enactment_id, const BfProtocolActor *actors, size_t actor_count, const char *subject, const char *at_iso, const char *purpose, const char *at, BfProtocolEnactment *out);
BfProtocolStatus bf_protocol_contract_outstanding(const BfProtocolContract *contract, void *sqlite_database, const char *enactment_id, BfProtocolEnactment *out);
/* A completed exchange closes only when its obligations are terminal AND each
 * required claim presently stands.  This reports both axes without claiming
 * either one is a proof-frontier verdict. */
BfProtocolStatus bf_protocol_contract_close(const BfProtocolContract *contract, void *commitment_database, void *evidence_database, const char *enactment_id, BfProtocolClosure *out);
const char *bf_protocol_status_name(BfProtocolStatus status);

#ifdef __cplusplus
}
#endif

#endif
