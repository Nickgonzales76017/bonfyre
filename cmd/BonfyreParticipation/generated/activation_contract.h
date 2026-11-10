/* Generated from activation-contract.cue. Do not edit. */
#ifndef BONFYRE_ACTIVATION_CONTRACT_H
#define BONFYRE_ACTIVATION_CONTRACT_H
#include <stddef.h>
typedef struct { const char *name; int bonus; const char *require_role_class; const char *forbid_role_class; const char *require_candidate_plane; int join_power_min; const char *or_source_kind; const char *or_protocol; const char *or_role_class; } BfActivationMode;
typedef struct { const char *name; const char *candidate_plane; const char *test_axis; int require_unknown; int depth_lt; const char *owner; const char *action; const char *why; } BfActivationNextRule;
typedef struct { int investigation_join_power_weight; int investigation_chain_span_cap; int investigation_candidate_plane_weight; int investigation_unknown_axis_weight; int investigation_braid_depth_weight; int investigation_branch_weight; int investigation_rejoin_weight; int investigation_braid_excess_weight; int activation_join_power_weight; int activation_chain_span_cap; int activation_braid_depth_weight; } BfActivationScore;

static const BfActivationMode bf_activation_modes[] = {
  {"cross_world_service", 12, "service", "", "", 3, "", "", ""},
  {"effect_adapter", 6, "action", "", "", 0, "", "", ""},
  {"publication_roundtrip", 0, "carrier", "", "round_trip", 0, "", "", ""},
  {"paid_machine_market", 5, "", "", "economic", 0, "x402", "x402", ""},
  {"semantic_interchange", 8, "", "", "", 0, "machine_protocol", "", "standard"},
  {"toolchain", 0, "compiler", "", "", 0, "", "", ""},
  {"verification_bridge", 0, "verification", "", "", 0, "", "", ""},
  {"sensor_material", 0, "material", "service", "", 0, "", "", ""},
};
#define BF_ACTIVATION_MODE_COUNT (sizeof(bf_activation_modes)/sizeof(bf_activation_modes[0]))
static const char *const bf_activation_default_mode = "general_candidate";

static const BfActivationNextRule bf_activation_next_rules[] = {
  {"observe_exact_candidate", "observation", "observation", 1, 0, "ForeignTwin", "observe_exact_candidate", "establish policy/transport/body evidence before deeper admission"},
  {"resolve_observation_boundary", "observation", "observation", 0, 3, "ForeignTwin", "resolve_observation_boundary", "policy or transport was measured but no source body was observed"},
  {"retain_exact_observation", "semantic", "observation", 0, 4, "ForeignTwin", "retain_exact_observation", "semantic binding must read retained bytes that still hash to their receipt"},
  {"bind_semantic_profile", "semantic", "semantic", 1, 0, "DomainProfile", "bind_semantic_profile", "turn candidate domain material into shared domain meaning"},
  {"bind_or_enact_protocol", "protocol", "protocol", 1, 0, "ProtocolContract", "bind_or_enact_protocol", "carrier fit is not yet a lawful interaction"},
  {"qualify_candidate_resource", "capability", "capability", 1, 0, "ResourceActivationGraph", "qualify_candidate_resource", "candidate capability must become qualified, eligible, authorized and activated"},
  {"establish_quote_or_service_book_entry", "economic", "economic", 1, 0, "Finance", "establish_quote_or_service_book_entry", "economic vocabulary is not a quote, commitment, payment or settlement"},
  {"close_projection_round_trip", "round_trip", "round_trip", 1, 0, "RoundTripTwin", "close_projection_round_trip", "a projection is not verified until independently observed"},
};
#define BF_ACTIVATION_NEXT_RULE_COUNT (sizeof(bf_activation_next_rules)/sizeof(bf_activation_next_rules[0]))
static const char *const bf_activation_default_owner = "Opportunity";
static const char *const bf_activation_default_action = "inspect_blockers_and_joins";
static const char *const bf_activation_default_why = "all candidate participation axes with known potential already have owner evidence";
static const BfActivationScore bf_activation_score = {12,12,2,1,-5,-4,-4,-2,4,10,-8};
#endif
