/* Generated from schemas/control/domain-profile.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_DOMAIN_PROFILES_H
#define BONFYRE_NATIVE_DOMAIN_PROFILES_H

#include <stddef.h>
#include <stdint.h>

typedef struct { const char *semantic; const char *carrier; } BfDomainCarrierField;
typedef struct { const char *carrier; const char *standard; const char *container; const BfDomainCarrierField *fields; size_t field_count; } BfDomainCarrierProjection;
typedef struct { const char *name; uint32_t audience_mask; const char *withhold_reason; } BfDomainFieldRule;
typedef struct { const char *name; const char *evidence_requirement; const BfDomainFieldRule *fields; size_t field_count; const BfDomainCarrierProjection *projections; size_t projection_count; } BfNativeDomainProfile;

static const BfDomainCarrierField bf_domain_0_0_fields[] = {
  {"account", "account"},
  {"address", "address"},
  {"city", "city"},
  {"postal", "postal"},
  {"owner", "owner"},
  {"year_built", "year_built"},
  {"living_area", "living_area"},
  {"lot_sqft", "lot_sqft"},
  {"land_value", "land_value"},
  {"total_value", "total_value"},
  {"beds", "beds"},
  {"baths", "baths"},
  {"source_id", "source_id"},
  {"territory", "territory"},
};
static const BfDomainCarrierField bf_domain_0_1_fields[] = {
  {"account", "ListingKey"},
  {"address", "UnparsedAddress"},
  {"city", "City"},
  {"postal", "PostalCode"},
  {"owner", "OwnerName"},
  {"year_built", "YearBuilt"},
  {"beds", "BedroomsTotal"},
  {"baths", "BathroomsTotalInteger"},
  {"living_area", "LivingArea"},
  {"lot_sqft", "LotSizeSquareFeet"},
  {"land_value", "LandValue"},
  {"total_value", "TaxAssessedValue"},
};
static const BfDomainCarrierField bf_domain_0_2_fields[] = {
  {"account", "account"},
  {"address", "address"},
  {"city", "city"},
  {"postal", "postal"},
  {"year_built", "year_built"},
  {"living_area", "living_area"},
  {"land_value", "land_value"},
  {"total_value", "total_value"},
};
static const BfDomainFieldRule bf_domain_0_rules[] = {
  {"account", 0x70, NULL},
  {"address", 0x70, NULL},
  {"city", 0x70, NULL},
  {"postal", 0x70, NULL},
  {"owner", 0x70, NULL},
  {"year_built", 0x70, NULL},
  {"living_area", 0x70, NULL},
  {"lot_sqft", 0x70, NULL},
  {"land_value", 0x70, NULL},
  {"total_value", 0x70, NULL},
  {"beds", 0x70, NULL},
  {"baths", 0x70, NULL},
  {"source_id", 0x70, NULL},
  {"territory", 0x70, NULL},
  {"owner_mailing_address", 0x41, "a mailing address is contactable and is not released with the assessment roll"},
};
static const BfDomainCarrierProjection bf_domain_0_projections[] = {
  {"ogc", "OGC API Features", "FeatureCollection", bf_domain_0_0_fields, sizeof(bf_domain_0_0_fields) / sizeof(bf_domain_0_0_fields[0])},
  {"reso", "RESO Web API", NULL, bf_domain_0_1_fields, sizeof(bf_domain_0_1_fields) / sizeof(bf_domain_0_1_fields[0])},
  {"mismo", "MISMO 3.4", "MESSAGE.DEAL_SETS.SUBJECT_PROPERTIES", bf_domain_0_2_fields, sizeof(bf_domain_0_2_fields) / sizeof(bf_domain_0_2_fields[0])},
};

static const BfDomainCarrierField bf_domain_1_0_fields[] = {
  {"occurrence", "subject"},
  {"kind", "type"},
  {"value", "data"},
  {"at_ms", "time"},
  {"source", "source"},
};
static const BfDomainCarrierField bf_domain_1_1_fields[] = {
  {"occurrence", "context"},
  {"value", "content"},
  {"speaker", "attributedTo"},
  {"kind", "type"},
};
static const BfDomainCarrierField bf_domain_1_2_fields[] = {
  {"occurrence", "subject"},
  {"kind", "$type"},
  {"value", "text"},
  {"speaker", "did"},
  {"at_ms", "createdAt"},
};
static const BfDomainCarrierField bf_domain_1_3_fields[] = {
  {"occurrence", "resource"},
  {"kind", "kind"},
  {"value", "value"},
  {"at_ms", "at_ms"},
  {"speaker", "speaker"},
  {"source", "source"},
};
static const BfDomainFieldRule bf_domain_1_rules[] = {
  {"occurrence", 0x70, NULL},
  {"at_ms", 0x70, NULL},
  {"kind", 0x70, NULL},
  {"value", 0x70, NULL},
  {"speaker", 0x70, NULL},
  {"source", 0x70, NULL},
};
static const BfDomainCarrierProjection bf_domain_1_projections[] = {
  {"cloudevents", "CloudEvents 1.0", NULL, bf_domain_1_0_fields, sizeof(bf_domain_1_0_fields) / sizeof(bf_domain_1_0_fields[0])},
  {"activitypub", "ActivityPub", NULL, bf_domain_1_1_fields, sizeof(bf_domain_1_1_fields) / sizeof(bf_domain_1_1_fields[0])},
  {"atproto", "ATProto", NULL, bf_domain_1_2_fields, sizeof(bf_domain_1_2_fields) / sizeof(bf_domain_1_2_fields[0])},
  {"mcp", "MCP", NULL, bf_domain_1_3_fields, sizeof(bf_domain_1_3_fields) / sizeof(bf_domain_1_3_fields[0])},
};

static const BfDomainCarrierField bf_domain_2_0_fields[] = {
  {"subject", "context"},
  {"body", "content"},
  {"creator", "attributedTo"},
  {"url", "url"},
  {"published_at", "published"},
};
static const BfDomainCarrierField bf_domain_2_1_fields[] = {
  {"subject", "subject"},
  {"body", "text"},
  {"creator", "did"},
  {"url", "uri"},
  {"published_at", "createdAt"},
};
static const BfDomainCarrierField bf_domain_2_2_fields[] = {
  {"title", "caption"},
  {"url", "video_url"},
  {"creator", "handle"},
};
static const BfDomainFieldRule bf_domain_2_rules[] = {
  {"subject", 0x70, NULL},
  {"title", 0x70, NULL},
  {"body", 0x70, NULL},
  {"creator", 0x70, NULL},
  {"url", 0x70, NULL},
  {"published_at", 0x70, NULL},
  {"location", 0x70, NULL},
};
static const BfDomainCarrierProjection bf_domain_2_projections[] = {
  {"activitypub", "ActivityPub", NULL, bf_domain_2_0_fields, sizeof(bf_domain_2_0_fields) / sizeof(bf_domain_2_0_fields[0])},
  {"atproto", "ATProto", NULL, bf_domain_2_1_fields, sizeof(bf_domain_2_1_fields) / sizeof(bf_domain_2_1_fields[0])},
  {"shortvideo", "Short video", NULL, bf_domain_2_2_fields, sizeof(bf_domain_2_2_fields) / sizeof(bf_domain_2_2_fields[0])},
};

static const BfNativeDomainProfile bf_native_domain_profiles[] = {
  {"parcel", "assessment_roll_receipt", bf_domain_0_rules, sizeof(bf_domain_0_rules) / sizeof(bf_domain_0_rules[0]), bf_domain_0_projections, sizeof(bf_domain_0_projections) / sizeof(bf_domain_0_projections[0])},
  {"live-moment", "media_clock_receipt", bf_domain_1_rules, sizeof(bf_domain_1_rules) / sizeof(bf_domain_1_rules[0]), bf_domain_1_projections, sizeof(bf_domain_1_projections) / sizeof(bf_domain_1_projections[0])},
  {"social-clip", "publication_receipt", bf_domain_2_rules, sizeof(bf_domain_2_rules) / sizeof(bf_domain_2_rules[0]), bf_domain_2_projections, sizeof(bf_domain_2_projections) / sizeof(bf_domain_2_projections[0])},
};
#define BF_NATIVE_DOMAIN_PROFILE_COUNT (sizeof(bf_native_domain_profiles) / sizeof(bf_native_domain_profiles[0]))
#endif
