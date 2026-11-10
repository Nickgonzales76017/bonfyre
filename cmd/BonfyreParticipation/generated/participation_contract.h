/* Generated from participation-contract.cue. Do not edit. */
#ifndef BONFYRE_PARTICIPATION_CONTRACT_H
#define BONFYRE_PARTICIPATION_CONTRACT_H
#include <stddef.h>
typedef struct { const char *name; const char *const *members; size_t member_count; } BfPartSet;
typedef struct { const char *axis; const char *state; int depth; const char *depth_name; } BfPartRung;

static const char *const bf_part_axes[] = {
  "discovery",
  "observation",
  "semantic",
  "protocol",
  "capability",
  "economic",
  "round_trip",
};
#define bf_part_axes_COUNT (sizeof(bf_part_axes)/sizeof(bf_part_axes[0]))

static const char *const bf_part_generic_human[] = {
  "discover",
};
#define bf_part_generic_human_COUNT (sizeof(bf_part_generic_human)/sizeof(bf_part_generic_human[0]))

static const char *const bf_part_generic_protocol[] = {
  "machine-generic",
};
#define bf_part_generic_protocol_COUNT (sizeof(bf_part_generic_protocol)/sizeof(bf_part_generic_protocol[0]))

static const char *const bf_part_capability_roles[] = {
  "action_adapter",
  "authoring_adapter",
  "compiler_toolchain",
  "external_service_candidate",
  "verification_tool",
  "media_service",
};
#define bf_part_capability_roles_COUNT (sizeof(bf_part_capability_roles)/sizeof(bf_part_capability_roles[0]))

static const char *const bf_part_roundtrip_roles[] = {
  "action_adapter",
  "authoring_adapter",
  "publication_carrier",
  "verification_tool",
};
#define bf_part_roundtrip_roles_COUNT (sizeof(bf_part_roundtrip_roles)/sizeof(bf_part_roundtrip_roles[0]))

static const char *const bf_part_roundtrip_human[] = {
  "creator-publish",
  "publication",
};
#define bf_part_roundtrip_human_COUNT (sizeof(bf_part_roundtrip_human)/sizeof(bf_part_roundtrip_human[0]))

static const char *const bf_part_roundtrip_protocols[] = {
  "activitypub",
  "atproto",
  "nostr",
  "webmention",
  "websub",
};
#define bf_part_roundtrip_protocols_COUNT (sizeof(bf_part_roundtrip_protocols)/sizeof(bf_part_roundtrip_protocols[0]))

static const char *const bf_part_economic_terms[] = {
  "commerce",
  "finance",
  "investing",
  "trade",
  "x402",
  "web-payment",
  "interledger",
  "iso20022",
  "payment-insurance-interchange",
  "agent-commerce",
};
#define bf_part_economic_terms_COUNT (sizeof(bf_part_economic_terms)/sizeof(bf_part_economic_terms[0]))

static const char *const bf_part_role_class_0_members[] = {
  "material_source",
  "media_service",
};
static const char *const bf_part_role_class_1_members[] = {
  "action_adapter",
  "authoring_adapter",
};
static const char *const bf_part_role_class_2_members[] = {
  "interaction_reference",
};
static const char *const bf_part_role_class_3_members[] = {
  "design_reference",
};
static const char *const bf_part_role_class_4_members[] = {
  "compiler_toolchain",
};
static const char *const bf_part_role_class_5_members[] = {
  "transport_carrier",
  "publication_carrier",
};
static const char *const bf_part_role_class_6_members[] = {
  "verification_tool",
};
static const char *const bf_part_role_class_7_members[] = {
  "external_service_candidate",
};
static const char *const bf_part_role_class_8_members[] = {
  "reference_standard",
};
static const BfPartSet bf_part_role_classes[] = {
  {"material", bf_part_role_class_0_members, sizeof(bf_part_role_class_0_members)/sizeof(bf_part_role_class_0_members[0])},
  {"action", bf_part_role_class_1_members, sizeof(bf_part_role_class_1_members)/sizeof(bf_part_role_class_1_members[0])},
  {"interaction_reference", bf_part_role_class_2_members, sizeof(bf_part_role_class_2_members)/sizeof(bf_part_role_class_2_members[0])},
  {"design_reference", bf_part_role_class_3_members, sizeof(bf_part_role_class_3_members)/sizeof(bf_part_role_class_3_members[0])},
  {"compiler", bf_part_role_class_4_members, sizeof(bf_part_role_class_4_members)/sizeof(bf_part_role_class_4_members[0])},
  {"carrier", bf_part_role_class_5_members, sizeof(bf_part_role_class_5_members)/sizeof(bf_part_role_class_5_members[0])},
  {"verification", bf_part_role_class_6_members, sizeof(bf_part_role_class_6_members)/sizeof(bf_part_role_class_6_members[0])},
  {"service", bf_part_role_class_7_members, sizeof(bf_part_role_class_7_members)/sizeof(bf_part_role_class_7_members[0])},
  {"standard", bf_part_role_class_8_members, sizeof(bf_part_role_class_8_members)/sizeof(bf_part_role_class_8_members[0])},
};
#define bf_part_role_classes_COUNT (sizeof(bf_part_role_classes)/sizeof(bf_part_role_classes[0]))

static const char *const bf_part_join_family_0_members[] = {
  "commerce",
  "finance",
  "investing",
  "enterprise-gtm",
  "resource-market",
  "insurance",
};
static const char *const bf_part_join_family_1_members[] = {
  "agriculture",
  "energy",
  "food",
  "housing",
  "logistics",
  "manufacturing",
  "mobility",
  "place",
  "property",
  "trip",
  "utility",
  "vehicle",
};
static const char *const bf_part_join_family_2_members[] = {
  "care",
  "community",
  "dating",
  "hiring",
  "household",
  "professional",
  "relationship",
};
static const char *const bf_part_join_family_3_members[] = {
  "ai-work",
  "knowledge",
  "learning",
  "research",
  "science",
  "software-work",
};
static const char *const bf_part_join_family_4_members[] = {
  "audience-growth",
  "creator-publish",
  "listen",
  "live-event",
  "media-production",
  "publication",
  "sports",
  "watch",
};
static const char *const bf_part_join_family_5_members[] = {
  "civic",
  "emergency",
  "grant-funding",
  "healthcare",
  "identity-access",
  "service-case",
};
static const char *const bf_part_join_family_6_members[] = {
  "collaboration",
  "mission",
  "project",
  "support",
};
static const BfPartSet bf_part_join_families[] = {
  {"economic", bf_part_join_family_0_members, sizeof(bf_part_join_family_0_members)/sizeof(bf_part_join_family_0_members[0])},
  {"physical", bf_part_join_family_1_members, sizeof(bf_part_join_family_1_members)/sizeof(bf_part_join_family_1_members[0])},
  {"social", bf_part_join_family_2_members, sizeof(bf_part_join_family_2_members)/sizeof(bf_part_join_family_2_members[0])},
  {"knowledge_compute", bf_part_join_family_3_members, sizeof(bf_part_join_family_3_members)/sizeof(bf_part_join_family_3_members[0])},
  {"media_attention", bf_part_join_family_4_members, sizeof(bf_part_join_family_4_members)/sizeof(bf_part_join_family_4_members[0])},
  {"civic_regulated", bf_part_join_family_5_members, sizeof(bf_part_join_family_5_members)/sizeof(bf_part_join_family_5_members[0])},
  {"coordination", bf_part_join_family_6_members, sizeof(bf_part_join_family_6_members)/sizeof(bf_part_join_family_6_members[0])},
};
#define bf_part_join_families_COUNT (sizeof(bf_part_join_families)/sizeof(bf_part_join_families[0]))

static const BfPartRung bf_part_ladders[] = {
  {"discovery", "indexed", 1, "indexed"},
  {"discovery", "routable", 2, "owner-routable"},
  {"discovery", "maintained", 3, "maintained"},
  {"observation", "policy_excluded", 1, "policy-preflighted"},
  {"observation", "policy_indeterminate", 1, "policy-preflighted"},
  {"observation", "boundary_response", 2, "transport-observed"},
  {"observation", "source_observed", 3, "body-observed"},
  {"observation", "source_retained", 4, "body-retained"},
  {"observation", "repeat_observed", 5, "repeat-observed"},
  {"semantic", "interpreted", 1, "interpreted"},
  {"semantic", "profile_bound", 2, "profile-bound"},
  {"semantic", "carrier_conformant", 3, "carrier-conformant"},
  {"protocol", "contract_bound", 1, "contract-bound"},
  {"protocol", "compiled", 2, "compiled"},
  {"protocol", "enacted", 3, "enacted"},
  {"protocol", "obligations_closed", 4, "obligations-closed"},
  {"capability", "qualified", 1, "qualified"},
  {"capability", "eligible", 2, "eligible"},
  {"capability", "authorized", 3, "authorized"},
  {"capability", "activated", 4, "activated"},
  {"economic", "quoted", 1, "quoted"},
  {"economic", "committed", 2, "committed"},
  {"economic", "paid", 3, "paid"},
  {"economic", "settled", 4, "settled"},
  {"economic", "reconciled", 5, "reconciled"},
  {"round_trip", "projected", 1, "projected"},
  {"round_trip", "observed", 2, "independently-observed"},
  {"round_trip", "faithful", 3, "closed-faithful"},
  {"round_trip", "drifted", 3, "closed-drifted"},
  {"round_trip", "unreachable", 3, "closed-unreachable"},
};
#define bf_part_ladders_COUNT (sizeof(bf_part_ladders)/sizeof(bf_part_ladders[0]))
#endif
