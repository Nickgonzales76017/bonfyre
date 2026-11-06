/* Generated from readiness-contract.cue. Do not edit. */
#ifndef BONFYRE_BRIDGE_READINESS_CONTRACT_H
#define BONFYRE_BRIDGE_READINESS_CONTRACT_H
#include <stddef.h>
typedef struct { const char *schema; const char *axis; const char *owner; const char *from_owner; const char *to_owner; const char *state; int require_validated; long long minimum_programs; } BfBridgeBinding;
typedef struct { const char *schema; int require_registered; int require_scoped; int require_targeted_invalidation; int require_atomic; long long minimum_enactments; long long minimum_programs; } BfBridgeWitness;
static const char *const bf_bridge_readiness_schema = "bonfyre.bridge-readiness.v2";
static const BfBridgeBinding bf_bridge_domain_binding = {"bonfyre-domain-binding.v1", "semantic", "DomainProfile", "ForeignTwin", "DomainProfile", "profile_bound", 1, 0};
static const char *const bf_bridge_domain_binding_hash_fields[] = {
  "foreign_receipt_sha256",
  "ingest_manifest_sha256",
  "normalized_content_sha256",
  "semantic_record_sha256",
  "source_content_sha256",
};
#define bf_bridge_domain_binding_HASH_FIELD_COUNT (sizeof(bf_bridge_domain_binding_hash_fields)/sizeof(bf_bridge_domain_binding_hash_fields[0]))
static const BfBridgeBinding bf_bridge_protocol_binding = {"bonfyre-protocol-binding.v1", "protocol", "ProtocolContract", "DomainProfile", "ProtocolContract", "compiled", 1, 1};
static const char *const bf_bridge_protocol_binding_hash_fields[] = {
  "domain_binding_sha256",
  "native_registration_receipt_sha256",
  "semantic_record_sha256",
};
#define bf_bridge_protocol_binding_HASH_FIELD_COUNT (sizeof(bf_bridge_protocol_binding_hash_fields)/sizeof(bf_bridge_protocol_binding_hash_fields[0]))
static const BfBridgeWitness bf_bridge_derivation_witness = {"bonfyre-protocol-derivation-bridge.v1", 1, 1, 1, 1, 2, 2};
#endif
