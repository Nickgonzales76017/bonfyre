#include "bf_semantic_operator.h"

#include "bonfyre.h"

#include <stdio.h>
#include <string.h>

static int present(const char *value)
{
    return value != NULL && value[0] != '\0';
}

static int source_file_matches(const char *domain, const char *source_file)
{
    if (strcmp(domain, "food") == 0) {
        return strcmp(source_file, "household/food/current-offers.jsonl") == 0;
    }
    return strcmp(source_file, "household/housing/current-listings.jsonl") == 0;
}

const char *bf_household_status_name(BfHouseholdStatus status)
{
    switch (status) {
    case BF_HOUSEHOLD_OK: return "ok";
    case BF_HOUSEHOLD_INVALID: return "invalid";
    case BF_HOUSEHOLD_UNSUPPORTED_DOMAIN: return "unsupported_domain";
    case BF_HOUSEHOLD_UNSUPPORTED_IDENTITY: return "unsupported_identity";
    case BF_HOUSEHOLD_MISSING_SOURCE: return "missing_source";
    case BF_HOUSEHOLD_DUPLICATE_ADDRESS: return "duplicate_address";
    case BF_HOUSEHOLD_TOO_MANY_READS: return "too_many_reads";
    }
    return "unknown";
}

const char *bf_semantic_operator_name(BfSemanticOperator operator_kind)
{
    switch (operator_kind) {
    case BF_SEMANTIC_SELECT: return "SELECT";
    case BF_SEMANTIC_DIFF: return "DIFF";
    case BF_SEMANTIC_COMPARE: return "COMPARE";
    case BF_SEMANTIC_WHY: return "WHY";
    case BF_SEMANTIC_PROVE: return "PROVE";
    case BF_SEMANTIC_PROJECT: return "PROJECT";
    case BF_SEMANTIC_RECONCILE: return "RECONCILE";
    }
    return NULL;
}

BfHouseholdStatus bf_household_projection_bind(const BfHouseholdProjection *projection,
                                               BfHouseholdRecord *out)
{
    const char *kind = NULL;
    const char *identity = NULL;

    if (projection == NULL || out == NULL || !present(projection->domain) ||
        !present(projection->source_file) || !present(projection->canonical_document)) {
        return BF_HOUSEHOLD_INVALID;
    }
    if (strcmp(projection->domain, "food") != 0 && strcmp(projection->domain, "housing") != 0) {
        return BF_HOUSEHOLD_UNSUPPORTED_DOMAIN;
    }
    if (!source_file_matches(projection->domain, projection->source_file)) {
        return BF_HOUSEHOLD_INVALID;
    }
    if (!present(projection->source_ref)) {
        return BF_HOUSEHOLD_MISSING_SOURCE;
    }
    if (strcmp(projection->domain, "food") == 0) {
        if (!present(projection->offer_id) || present(projection->listing_id) ||
            present(projection->candidate_id)) {
            return BF_HOUSEHOLD_UNSUPPORTED_IDENTITY;
        }
        kind = "offer";
        identity = projection->offer_id;
    } else if (present(projection->listing_id) && !present(projection->candidate_id) &&
               !present(projection->offer_id)) {
        kind = "listing";
        identity = projection->listing_id;
    } else if (present(projection->candidate_id) && !present(projection->listing_id) &&
               !present(projection->offer_id)) {
        kind = "observation";
        identity = projection->candidate_id;
    } else {
        return BF_HOUSEHOLD_UNSUPPORTED_IDENTITY;
    }

    memset(out, 0, sizeof(*out));
    if (snprintf(out->address, sizeof(out->address), "household:%s:%s:%s",
                 projection->domain, kind, identity) >= (int)sizeof(out->address) ||
        snprintf(out->kind, sizeof(out->kind), "%s.%s", projection->domain, kind) >=
            (int)sizeof(out->kind) ||
        snprintf(out->source_ref, sizeof(out->source_ref), "%s", projection->source_ref) >=
            (int)sizeof(out->source_ref)) {
        return BF_HOUSEHOLD_INVALID;
    }
    bf_sha256_hex((const uint8_t *)projection->canonical_document,
                  strlen(projection->canonical_document), out->content_sha256);
    out->observed_claims = projection->observed_claims;
    out->property_id = projection->property_id;
    out->unit_identity = projection->unit_identity;
    return BF_HOUSEHOLD_OK;
}

static int record_is_bound(const BfHouseholdRecord *record)
{
    return record != NULL && present(record->address) && present(record->kind) &&
           present(record->source_ref) && strlen(record->content_sha256) == 64;
}

static void hash_text(BfSha256 *hash, const char *text)
{
    bf_sha256_update(hash, (const uint8_t *)text, strlen(text));
    bf_sha256_update(hash, (const uint8_t *)"\n", 1);
}

BfHouseholdStatus bf_semantic_program_lower(BfSemanticOperator operator_kind,
                                            const BfHouseholdRecord *records,
                                            size_t record_count,
                                            BfSemanticProgram *out)
{
    const char *operator_name = bf_semantic_operator_name(operator_kind);
    const BfHouseholdRecord *ordered[BF_HOUSEHOLD_READ_LIMIT];
    BfSha256 hash;

    if (out == NULL || operator_name == NULL || records == NULL || record_count == 0) {
        return BF_HOUSEHOLD_INVALID;
    }
    if (record_count > BF_HOUSEHOLD_READ_LIMIT) {
        return BF_HOUSEHOLD_TOO_MANY_READS;
    }
    for (size_t index = 0; index < record_count; index++) {
        if (!record_is_bound(&records[index])) return BF_HOUSEHOLD_INVALID;
        ordered[index] = &records[index];
    }
    for (size_t left = 0; left < record_count; left++) {
        for (size_t right = left + 1; right < record_count; right++) {
            if (strcmp(ordered[left]->address, ordered[right]->address) > 0) {
                const BfHouseholdRecord *swap = ordered[left];
                ordered[left] = ordered[right];
                ordered[right] = swap;
            }
        }
        if (left > 0 && strcmp(ordered[left - 1]->address, ordered[left]->address) == 0) {
            return BF_HOUSEHOLD_DUPLICATE_ADDRESS;
        }
    }
    bf_sha256_init(&hash);
    hash_text(&hash, operator_name);
    for (size_t index = 0; index < record_count; index++) hash_text(&hash, ordered[index]->address);

    memset(out, 0, sizeof(*out));
    out->operator_kind = operator_kind;
    bf_sha256_digest_hex((uint8_t[32]){0}, out->digest);
    uint8_t digest[32];
    bf_sha256_final(&hash, digest);
    bf_sha256_digest_hex(digest, out->digest);
    if (snprintf(out->program_id, sizeof(out->program_id), "household.%s.%.16s",
                 operator_name, out->digest) >= (int)sizeof(out->program_id)) {
        return BF_HOUSEHOLD_INVALID;
    }
    out->read_count = record_count;
    out->writes = 0;
    out->admitted = 1;
    return BF_HOUSEHOLD_OK;
}

BfHouseholdStatus bf_household_prove(const BfHouseholdRecord *record,
                                     BfHouseholdProofClaim claim,
                                     BfHouseholdProofVerdict *out)
{
    uint32_t flag;
    if (!record_is_bound(record) || out == NULL) return BF_HOUSEHOLD_INVALID;
    switch (claim) {
    case BF_HOUSEHOLD_PROVE_AVAILABILITY: flag = BF_HOUSEHOLD_OBSERVED_AVAILABILITY; break;
    case BF_HOUSEHOLD_PROVE_DOG_ACCESS: flag = BF_HOUSEHOLD_OBSERVED_DOG_ACCESS; break;
    case BF_HOUSEHOLD_PROVE_EFFECTIVE_COST: flag = BF_HOUSEHOLD_OBSERVED_EFFECTIVE_COST; break;
    case BF_HOUSEHOLD_PROVE_PRICE: flag = BF_HOUSEHOLD_OBSERVED_PRICE; break;
    default: return BF_HOUSEHOLD_INVALID;
    }
    memset(out, 0, sizeof(*out));
    out->observed = (record->observed_claims & flag) != 0;
    out->proven = 0;
    snprintf(out->source_ref, sizeof(out->source_ref), "%s", record->source_ref);
    snprintf(out->reason, sizeof(out->reason),
             "A source-bound projection preserves an observation but does not independently prove current availability, eligibility, pet acceptance, or cost.");
    return BF_HOUSEHOLD_OK;
}

BfHouseholdStatus bf_household_reconcile(const BfHouseholdRecord *left,
                                         const BfHouseholdRecord *right,
                                         BfHouseholdReconciliationResult *out)
{
    const char *left_property;
    const char *right_property;
    const char *left_unit;
    const char *right_unit;
    if (!record_is_bound(left) || !record_is_bound(right) || out == NULL) {
        return BF_HOUSEHOLD_INVALID;
    }
    memset(out, 0, sizeof(*out));
    if (strcmp(left->address, right->address) == 0) {
        out->disposition = BF_HOUSEHOLD_IDENTICAL_ADDRESS;
        snprintf(out->reason, sizeof(out->reason), "Both operands resolve to the same address.");
        return BF_HOUSEHOLD_OK;
    }
    left_property = left->property_id ? left->property_id : "";
    right_property = right->property_id ? right->property_id : "";
    left_unit = left->unit_identity ? left->unit_identity : "";
    right_unit = right->unit_identity ? right->unit_identity : "";
    if (present(left_property) && strcmp(left_property, right_property) == 0 &&
        strcmp(left_unit, right_unit) != 0) {
        out->disposition = BF_HOUSEHOLD_COEXISTING_DISTINCT_UNITS;
        out->merge_prohibited = 1;
        snprintf(out->reason, sizeof(out->reason),
                 "Shared property identity with distinct unit identities is not a duplicate or price conflict.");
    } else {
        out->disposition = BF_HOUSEHOLD_NO_CANONICAL_JOIN;
        out->merge_prohibited = 1;
        snprintf(out->reason, sizeof(out->reason),
                 "No supported identity relation permits merging these source-bound records.");
    }
    return BF_HOUSEHOLD_OK;
}
