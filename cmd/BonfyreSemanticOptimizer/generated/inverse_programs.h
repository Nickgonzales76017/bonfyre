/* Generated from YAFF/CUE TransformationAlgebra. Do not edit. */
#ifndef BF_INVERSE_PROGRAMS_H
#define BF_INVERSE_PROGRAMS_H
#include <stddef.h>
#include <stdint.h>
typedef enum { BF_INVERSE_ADAPTER_NONE=0,BF_INVERSE_ADAPTER_COMMITMENT,BF_INVERSE_ADAPTER_AUTHORITY,BF_INVERSE_ADAPTER_POLICY,BF_INVERSE_ADAPTER_EVIDENCE,BF_INVERSE_ADAPTER_PROOF,BF_INVERSE_ADAPTER_ACTIVATION,BF_INVERSE_ADAPTER_RESERVATION,BF_INVERSE_ADAPTER_FOREIGN,BF_INVERSE_ADAPTER_WORK,BF_INVERSE_ADAPTER_COMMERCE } BfInverseAdapter;
typedef enum { BF_INVERSE_STATE_NONE=0,BF_INVERSE_STATE_COMMITMENT_OPEN,BF_INVERSE_STATE_DISCHARGED,BF_INVERSE_STATE_DELEGATED,BF_INVERSE_STATE_VIOLATED,BF_INVERSE_STATE_COMPENSATED,BF_INVERSE_STATE_VALID,BF_INVERSE_STATE_PUBLIC,BF_INVERSE_STATE_POLICY_PUBLIC,BF_INVERSE_STATE_STANDS,BF_INVERSE_STATE_PROVEN,BF_INVERSE_STATE_ACTIVATED,BF_INVERSE_STATE_RESERVED,BF_INVERSE_STATE_OBSERVED_PUBLICATION,BF_INVERSE_STATE_OBSERVED_DELIVERY,BF_INVERSE_STATE_OBSERVED_RESULT,BF_INVERSE_STATE_COMPLETED,BF_INVERSE_STATE_PRODUCT,BF_INVERSE_STATE_LISTING,BF_INVERSE_STATE_INVENTORY,BF_INVERSE_STATE_AVAILABILITY,BF_INVERSE_STATE_OFFER,BF_INVERSE_STATE_QUOTE,BF_INVERSE_STATE_CART,BF_INVERSE_STATE_CHECKOUT,BF_INVERSE_STATE_COMMERCE_COMMITMENT,BF_INVERSE_STATE_PAYMENT,BF_INVERSE_STATE_SETTLEMENT,BF_INVERSE_STATE_FULFILMENT,BF_INVERSE_STATE_DELIVERY,BF_INVERSE_STATE_SATISFACTION,BF_INVERSE_STATE_OWNERSHIP } BfInverseNativeState;
typedef enum { BF_COST_ELAPSED,BF_COST_CASH,BF_COST_ACCRUED,BF_COST_RESOURCE,BF_COST_RIGHTS,BF_COST_DILUTION,BF_COST_REVERSIBILITY,BF_COST_COMPENSATION,BF_COST_SUCCESS,BF_COST_EVIDENCE,BF_COST_PROOF_DISTANCE,BF_COST_AUTHORITY_DISTANCE,BF_COST_DISCLOSURE,BF_COST_PROVIDER,BF_COST_FAILURE_DOMAIN,BF_COST_FRESHNESS,BF_COST_ATTENTION,BF_COST_ROUNDTRIP,BF_COST_SEMANTIC_LOSS } BfInverseCostDimension;
typedef struct { const char *owner,*predicate,*expected; uint8_t adapter,native_state; uint16_t owner_query; } BfInversePredicatePattern;
typedef struct { const char *owner,*predicate,*expected; uint8_t adapter,native_state; uint16_t owner_query; } BfInverseOwnerGrammar;
typedef struct { int64_t elapsed_minutes,cash_delta_micros,accrued_cost_micros,resource_consumption,rights_conceded,ownership_dilution_bps,reversibility_frontier,compensation_cost_micros,success_probability_ppm,evidence_strength,proof_distance,authority_distance,information_disclosure,provider_dependence,failure_domain_concentration,freshness_exposure,human_attention_minutes,roundtrip_observability,semantic_loss; } BfInverseSemanticCost;
typedef struct { const char *id; const BfInversePredicatePattern *requires,*establishes,*retracts; size_t requires_count,establishes_count,retracts_count; const char *consumes,*produces,*creates_commitments,*discharges_commitments,*grants_rights,*concedes_rights,*conflicts_with_rights,*obligations,*protocol_pattern,*resource_pattern,*effect_kind,*reversibility; BfInverseSemanticCost cost; uint16_t eclass; } BfInverseTransformationDescriptor;
typedef struct { const char *id; const BfInverseCostDimension *dimensions; size_t dimension_count; } BfInversePreferencePolicy;
static const BfInverseOwnerGrammar bf_inverse_owner_grammar[] = {
{"CommitmentGraph","commitment","open",1,1,0},
{"CommitmentGraph","commitment","discharged",1,2,1},
{"CommitmentGraph","commitment","delegated",1,3,2},
{"CommitmentGraph","commitment","violated",1,4,3},
{"CommitmentGraph","commitment","compensated",1,5,4},
{"AuthorityGraph","permit","valid",2,6,1},
{"AuthorityGraph","publish:public","valid",2,7,1},
{"InformationPolicy","public_projection","valid",3,8,1},
{"EvidenceGraph","claim","valid",4,9,1},
{"ProofFrontier","invariant","proven",5,10,1},
{"ResourceActivation","capacity","activated",6,11,5},
{"ResourceReservation","capacity","reserved",7,12,1},
{"ForeignTwin","publication_roundtrip","observed",8,13,1},
{"ForeignTwin","delivery","observed",8,14,1},
{"ForeignTwin","result","observed",8,15,1},
{"WorkGraph","node","done",9,16,1},
{"CommerceChain","product","product",10,17,0},
{"CommerceChain","listing","listing",10,18,1},
{"CommerceChain","inventory","inventory",10,19,2},
{"CommerceChain","availability","availability",10,20,3},
{"CommerceChain","offer","offer",10,21,4},
{"CommerceChain","quote","valid",10,22,5},
{"CommerceChain","cart","cart",10,23,6},
{"CommerceChain","checkout","checkout",10,24,7},
{"CommerceChain","commitment","commitment",10,25,8},
{"CommerceChain","payment","payment",10,26,9},
{"CommerceChain","payment","settled",10,27,10},
{"CommerceChain","fulfilment","fulfilment",10,28,11},
{"CommerceChain","delivery","delivery",10,29,12},
{"CommerceChain","satisfaction","satisfaction",10,30,13},
{"CommerceChain","ownership","ownership",10,31,14},
};
static const size_t bf_inverse_owner_grammar_count=sizeof(bf_inverse_owner_grammar)/sizeof(*bf_inverse_owner_grammar);
static const BfInversePredicatePattern bf_inverse_requires_reserve_capacity[] = {
{"","","",0,0,0},
};
static const BfInversePredicatePattern bf_inverse_requires_enact_protocol_commitment[] = {
{"","","",0,0,0},
};
static const BfInversePredicatePattern bf_inverse_requires_settle_commerce[] = {
{"CommerceChain","quote","valid",10,22,5},
};
static const BfInversePredicatePattern bf_inverse_requires_independent_roundtrip_observe[] = {
{"","","",0,0,0},
};
static const BfInversePredicatePattern bf_inverse_requires_prove_invariant[] = {
{"ForeignTwin","result","observed",8,15,1},
};
static const BfInversePredicatePattern bf_inverse_establishes_reserve_capacity[] = {
{"ResourceReservation","capacity","reserved",7,12,1},
};
static const BfInversePredicatePattern bf_inverse_establishes_enact_protocol_commitment[] = {
{"CommitmentGraph","commitment","discharged",1,2,1},
};
static const BfInversePredicatePattern bf_inverse_establishes_settle_commerce[] = {
{"CommerceChain","payment","settled",10,27,10},
};
static const BfInversePredicatePattern bf_inverse_establishes_independent_roundtrip_observe[] = {
{"ForeignTwin","result","observed",8,15,1},
};
static const BfInversePredicatePattern bf_inverse_establishes_prove_invariant[] = {
{"ProofFrontier","invariant","proven",5,10,1},
};
static const BfInversePredicatePattern bf_inverse_retracts_reserve_capacity[] = {
{"","","",0,0,0},
};
static const BfInversePredicatePattern bf_inverse_retracts_enact_protocol_commitment[] = {
{"","","",0,0,0},
};
static const BfInversePredicatePattern bf_inverse_retracts_settle_commerce[] = {
{"","","",0,0,0},
};
static const BfInversePredicatePattern bf_inverse_retracts_independent_roundtrip_observe[] = {
{"","","",0,0,0},
};
static const BfInversePredicatePattern bf_inverse_retracts_prove_invariant[] = {
{"","","",0,0,0},
};
static const BfInverseCostDimension bf_inverse_policy_resilient[]={14,10,11,4,6,17,8,0,1};
static const BfInverseCostDimension bf_inverse_policy_urgent[]={0,8,11,17,1,14};
static const BfInverseCostDimension bf_inverse_policy_frugal[]={1,2,3,4,18,0};
static const BfInverseTransformationDescriptor bf_inverse_transformations[] = {
{"reserve_capacity",bf_inverse_requires_reserve_capacity,bf_inverse_establishes_reserve_capacity,bf_inverse_retracts_reserve_capacity,0,1,0,"temporal-capacity","reservation-hold","reservation-hold","","","","","authority-recheck-at-effect-time,independent-roundtrip-observation","resource-reservation","temporal-capacity","reservation","compensatable",{20,0,0,1,0,0,1,0,900000,2,1,1,0,1,1,1,5,2,0},0},
{"enact_protocol_commitment",bf_inverse_requires_enact_protocol_commitment,bf_inverse_establishes_enact_protocol_commitment,bf_inverse_retracts_enact_protocol_commitment,0,1,0,"counterparty-capacity","commitment-receipt","counterparty-commitment","counterparty-commitment","","","","protocol-conformance,evidence-receipt","commitment-enactment","counterparty-capacity","protocol","compensatable",{30,0,0,1,0,0,1,0,850000,3,1,1,1,1,1,1,10,2,0},0},
{"settle_commerce",bf_inverse_requires_settle_commerce,bf_inverse_establishes_settle_commerce,bf_inverse_retracts_settle_commerce,1,1,0,"cash,quote-validity-window","settlement-receipt","settlement-obligation","settlement-obligation","","payment-control","","payment-authority-recheck,delivery-roundtrip","commerce-settlement","funding-source","payment","irreversible",{5,1000000,0,1,1,0,2,0,950000,3,2,2,1,1,1,2,2,3,0},2},
{"independent_roundtrip_observe",bf_inverse_requires_independent_roundtrip_observe,bf_inverse_establishes_independent_roundtrip_observe,bf_inverse_retracts_independent_roundtrip_observe,0,1,0,"observation-budget","foreign-twin-receipt","","","","","","independent-observation","foreign-twin-observation","observation-budget","observe","reversible",{2,0,0,1,0,0,0,0,800000,3,1,0,0,1,1,2,1,3,0},3},
{"prove_invariant",bf_inverse_requires_prove_invariant,bf_inverse_establishes_prove_invariant,bf_inverse_retracts_prove_invariant,1,1,0,"independent-evidence","proof-receipt","","","","","","proof-frontier-validation","proof-frontier","independent-evidence","prove","reversible",{60,0,0,2,0,0,0,0,700000,5,0,0,0,0,0,1,30,3,0},4},
};
static const size_t bf_inverse_transformation_count=sizeof(bf_inverse_transformations)/sizeof(*bf_inverse_transformations);
static const BfInversePreferencePolicy bf_inverse_policies[]={{"resilient",bf_inverse_policy_resilient,sizeof(bf_inverse_policy_resilient)/sizeof(*bf_inverse_policy_resilient)},{"urgent",bf_inverse_policy_urgent,sizeof(bf_inverse_policy_urgent)/sizeof(*bf_inverse_policy_urgent)},{"frugal",bf_inverse_policy_frugal,sizeof(bf_inverse_policy_frugal)/sizeof(*bf_inverse_policy_frugal)},};
static const size_t bf_inverse_policy_count=sizeof(bf_inverse_policies)/sizeof(*bf_inverse_policies);
#endif
