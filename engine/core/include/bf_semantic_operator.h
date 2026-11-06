#ifndef BF_SEMANTIC_OPERATOR_H
#define BF_SEMANTIC_OPERATOR_H

/*
 * Read-only lowering over the durable household projections.
 *
 * This absorbed semantic_operator.py.archive contract deliberately accepts
 * already decoded projection fields rather than opening a second household
 * store: the food and housing flows remain the only writers.  A caller must
 * bind a projection before it can be addressed, lowered, reconciled, or
 * explained.  Binding is the source boundary: declared identity, canonical
 * source body, and a nonempty evidence reference are all required.
 */

#include <stddef.h>
#include <stdint.h>

#define BF_HOUSEHOLD_ADDRESS_MAX 384
#define BF_HOUSEHOLD_SOURCE_REF_MAX 512
#define BF_HOUSEHOLD_READ_LIMIT 16

typedef enum BfHouseholdStatus {
    BF_HOUSEHOLD_OK = 0,
    BF_HOUSEHOLD_INVALID,
    BF_HOUSEHOLD_UNSUPPORTED_DOMAIN,
    BF_HOUSEHOLD_UNSUPPORTED_IDENTITY,
    BF_HOUSEHOLD_MISSING_SOURCE,
    BF_HOUSEHOLD_DUPLICATE_ADDRESS,
    BF_HOUSEHOLD_TOO_MANY_READS
} BfHouseholdStatus;

/* The flags describe observations carried by the projection, never facts. */
typedef enum BfHouseholdObservedClaim {
    BF_HOUSEHOLD_OBSERVED_AVAILABILITY = 1u << 0,
    BF_HOUSEHOLD_OBSERVED_DOG_ACCESS = 1u << 1,
    BF_HOUSEHOLD_OBSERVED_EFFECTIVE_COST = 1u << 2,
    BF_HOUSEHOLD_OBSERVED_PRICE = 1u << 3
} BfHouseholdObservedClaim;

typedef struct BfHouseholdProjection {
    const char *domain;             /* food or housing */
    const char *offer_id;           /* required for food */
    const char *listing_id;         /* housing listing identity */
    const char *candidate_id;       /* housing observation identity */
    const char *source_file;        /* one of the durable projection files */
    const char *source_ref;         /* retained-body receipt / source URL */
    const char *canonical_document; /* exact decoded projection JSON */
    uint32_t observed_claims;
    const char *property_id;        /* optional, for reconciliation only */
    const char *unit_identity;      /* optional, for reconciliation only */
} BfHouseholdProjection;

typedef struct BfHouseholdRecord {
    char address[BF_HOUSEHOLD_ADDRESS_MAX];
    char kind[32];                  /* food.offer, housing.listing, ... */
    char content_sha256[65];
    char source_ref[BF_HOUSEHOLD_SOURCE_REF_MAX];
    uint32_t observed_claims;
    const char *property_id;
    const char *unit_identity;
} BfHouseholdRecord;

typedef enum BfSemanticOperator {
    BF_SEMANTIC_SELECT = 0,
    BF_SEMANTIC_DIFF,
    BF_SEMANTIC_COMPARE,
    BF_SEMANTIC_WHY,
    BF_SEMANTIC_PROVE,
    BF_SEMANTIC_PROJECT,
    BF_SEMANTIC_RECONCILE
} BfSemanticOperator;

typedef struct BfSemanticProgram {
    BfSemanticOperator operator_kind;
    char program_id[128];
    char digest[65];
    size_t read_count;
    int writes;       /* always zero: native household lowering is read-only */
    int admitted;     /* lowerer admits only a complete, bounded read closure */
} BfSemanticProgram;

typedef enum BfHouseholdProofClaim {
    BF_HOUSEHOLD_PROVE_AVAILABILITY = 0,
    BF_HOUSEHOLD_PROVE_DOG_ACCESS,
    BF_HOUSEHOLD_PROVE_EFFECTIVE_COST,
    BF_HOUSEHOLD_PROVE_PRICE
} BfHouseholdProofClaim;

typedef struct BfHouseholdProofVerdict {
    int observed;
    int proven;       /* always zero: observation never promotes itself */
    char source_ref[BF_HOUSEHOLD_SOURCE_REF_MAX];
    char reason[256];
} BfHouseholdProofVerdict;

typedef enum BfHouseholdReconciliation {
    BF_HOUSEHOLD_IDENTICAL_ADDRESS = 0,
    BF_HOUSEHOLD_COEXISTING_DISTINCT_UNITS,
    BF_HOUSEHOLD_NO_CANONICAL_JOIN
} BfHouseholdReconciliation;

typedef struct BfHouseholdReconciliationResult {
    BfHouseholdReconciliation disposition;
    int merge_prohibited;
    char reason[192];
} BfHouseholdReconciliationResult;

/* Validate and bind a decoded projection to a deterministic source address. */
BfHouseholdStatus bf_household_projection_bind(const BfHouseholdProjection *projection,
                                               BfHouseholdRecord *out);

/* Lower a bounded, duplicate-free read set.  No storage or external effect. */
BfHouseholdStatus bf_semantic_program_lower(BfSemanticOperator operator_kind,
                                            const BfHouseholdRecord *records,
                                            size_t record_count,
                                            BfSemanticProgram *out);

/* A proof query can report an observed field but can never report it as proven. */
BfHouseholdStatus bf_household_prove(const BfHouseholdRecord *record,
                                     BfHouseholdProofClaim claim,
                                     BfHouseholdProofVerdict *out);

/* Preserve distinct units; no identity relation means no merge. */
BfHouseholdStatus bf_household_reconcile(const BfHouseholdRecord *left,
                                         const BfHouseholdRecord *right,
                                         BfHouseholdReconciliationResult *out);

const char *bf_household_status_name(BfHouseholdStatus status);
const char *bf_semantic_operator_name(BfSemanticOperator operator_kind);

#endif
