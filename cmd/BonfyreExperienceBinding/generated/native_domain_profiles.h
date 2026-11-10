/* Generated from BonfyreExperienceBinding/domain-profile.cue. Do not edit. */
#ifndef BONFYRE_EXPERIENCE_BINDING_DOMAIN_PROFILES_H
#define BONFYRE_EXPERIENCE_BINDING_DOMAIN_PROFILES_H
#include <stddef.h>
#include <stdint.h>
typedef struct { const char *name; uint32_t audience_mask; } BfBindingDomainField;
typedef struct { const char *semantic; const char *carrier; } BfBindingCarrierField;
typedef struct { const char *carrier; const char *standard; const BfBindingCarrierField *fields; size_t field_count; } BfBindingProjection;
typedef struct { const char *name; const char *const *evidence_requirements; size_t evidence_requirement_count; const BfBindingDomainField *fields; size_t field_count; const BfBindingProjection *projections; size_t projection_count; } BfBindingDomainProfile;

static const char *const bf_binding_domain_reference_standard_evidence[] = {
  "foreign_observation_receipt",
  "ingest_artifact",
};
static const BfBindingDomainField bf_binding_domain_reference_standard_fields[] = {
  {"authority", 0x7f},
  {"description", 0x7f},
  {"name", 0x7f},
  {"source_url", 0x7f},
  {"updated_at", 0x7f},
};
static const BfBindingCarrierField bf_binding_domain_reference_standard_semantic_json[] = {
  {"authority", "authority"},
  {"description", "description"},
  {"name", "name"},
  {"source_url", "source_url"},
  {"updated_at", "updated_at"},
};
static const BfBindingProjection bf_binding_domain_reference_standard_projections[] = {
  {"semantic-json", "Bonfyre semantic record", bf_binding_domain_reference_standard_semantic_json, sizeof(bf_binding_domain_reference_standard_semantic_json) / sizeof(bf_binding_domain_reference_standard_semantic_json[0])},
};

static const BfBindingDomainProfile bf_binding_domain_profiles[] = {
  {"reference-standard", bf_binding_domain_reference_standard_evidence, sizeof(bf_binding_domain_reference_standard_evidence) / sizeof(bf_binding_domain_reference_standard_evidence[0]), bf_binding_domain_reference_standard_fields, sizeof(bf_binding_domain_reference_standard_fields) / sizeof(bf_binding_domain_reference_standard_fields[0]), bf_binding_domain_reference_standard_projections, sizeof(bf_binding_domain_reference_standard_projections) / sizeof(bf_binding_domain_reference_standard_projections[0])},
};
#define BF_BINDING_DOMAIN_PROFILE_COUNT (sizeof(bf_binding_domain_profiles) / sizeof(bf_binding_domain_profiles[0]))
#endif
