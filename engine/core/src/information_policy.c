/* Native information-policy calculation.  The CUE declaration accepts the
 * authored shape; this file makes default-deny, field/audience exactness and
 * aggregation floors executable for every carrier that binds the ABI. */
#include "bf_information_policy.h"

#include <stdio.h>
#include <string.h>

static int present(const char *value) { return value && value[0]; }

static BfInformationPolicyStatus copy_text(char *out, const char *value) {
    size_t size;
    if (!out || !present(value)) return BF_INFORMATION_POLICY_INVALID;
    size = strlen(value); if (size >= BF_INFORMATION_POLICY_TEXT_MAX) return BF_INFORMATION_POLICY_CAPACITY;
    memcpy(out, value, size + 1); return BF_INFORMATION_POLICY_OK;
}

void bf_information_policy_init(BfInformationPolicy *policy) { if (policy) memset(policy, 0, sizeof(*policy)); }

unsigned int bf_information_policy_audience_mask(BfDisclosureAudience audience) {
    return audience >= BF_AUDIENCE_SELF && audience <= BF_AUDIENCE_INSTITUTION ? 1u << (unsigned int)audience : 0u;
}

const char *bf_information_policy_audience_name(BfDisclosureAudience audience) {
    static const char *const names[] = {"self", "household", "group", "community", "public", "market", "institution"};
    return audience >= BF_AUDIENCE_SELF && audience <= BF_AUDIENCE_INSTITUTION ? names[audience] : NULL;
}

const char *bf_information_policy_status_name(BfInformationPolicyStatus status) {
    switch (status) {
    case BF_INFORMATION_POLICY_OK: return "ok";
    case BF_INFORMATION_POLICY_INVALID: return "invalid";
    case BF_INFORMATION_POLICY_EXISTS: return "exists";
    case BF_INFORMATION_POLICY_CAPACITY: return "capacity";
    }
    return "unknown";
}

/* An explicit empty audience set is a valid deny-to-all declaration. CUE and
 * the Bun projection allow it; rejecting it here would make carrier outcomes
 * disagree for the same authored policy. */
static int valid_mask(unsigned int mask) { return !(mask & ~((1u << BF_INFORMATION_POLICY_MAX_AUDIENCES) - 1u)); }

BfInformationPolicyStatus bf_information_policy_disclose(BfInformationPolicy *policy, const char *field, unsigned int audience_mask, const char *withhold_reason) {
    size_t index; BfInformationPolicyStatus status;
    if (!policy || !present(field) || !valid_mask(audience_mask)) return BF_INFORMATION_POLICY_INVALID;
    for (index = 0; index < policy->field_count; index++) if (strcmp(policy->fields[index].field, field) == 0) return BF_INFORMATION_POLICY_EXISTS;
    if (policy->field_count == BF_INFORMATION_POLICY_MAX_RULES) return BF_INFORMATION_POLICY_CAPACITY;
    status = copy_text(policy->fields[policy->field_count].field, field); if (status != BF_INFORMATION_POLICY_OK) return status;
    status = copy_text(policy->fields[policy->field_count].reason, present(withhold_reason) ? withhold_reason : "not disclosed to this audience"); if (status != BF_INFORMATION_POLICY_OK) return status;
    policy->fields[policy->field_count].audience_mask = audience_mask; policy->field_count++; return BF_INFORMATION_POLICY_OK;
}

BfInformationPolicyStatus bf_information_policy_floor(BfInformationPolicy *policy, const char *statistic, int minimum_population, unsigned int audience_mask) {
    size_t index; BfInformationPolicyStatus status;
    if (!policy || !present(statistic) || minimum_population < 2 || !valid_mask(audience_mask)) return BF_INFORMATION_POLICY_INVALID;
    for (index = 0; index < policy->floor_count; index++) if (strcmp(policy->floors[index].statistic, statistic) == 0) return BF_INFORMATION_POLICY_EXISTS;
    if (policy->floor_count == BF_INFORMATION_POLICY_MAX_RULES) return BF_INFORMATION_POLICY_CAPACITY;
    status = copy_text(policy->floors[policy->floor_count].statistic, statistic); if (status != BF_INFORMATION_POLICY_OK) return status;
    policy->floors[policy->floor_count].minimum_population = minimum_population; policy->floors[policy->floor_count].audience_mask = audience_mask; policy->floor_count++; return BF_INFORMATION_POLICY_OK;
}

BfInformationPolicyStatus bf_information_policy_release_evidence(BfInformationPolicy *policy, const char *witness_kind) {
    size_t index; BfInformationPolicyStatus status;
    if (!policy || !present(witness_kind)) return BF_INFORMATION_POLICY_INVALID;
    for (index = 0; index < policy->evidence_release_count; index++) if (strcmp(policy->evidence_release[index], witness_kind) == 0) return BF_INFORMATION_POLICY_EXISTS;
    if (policy->evidence_release_count == BF_INFORMATION_POLICY_MAX_RULES) return BF_INFORMATION_POLICY_CAPACITY;
    status = copy_text(policy->evidence_release[policy->evidence_release_count], witness_kind); if (status != BF_INFORMATION_POLICY_OK) return status;
    policy->evidence_release_count++; return BF_INFORMATION_POLICY_OK;
}

BfInformationPolicyStatus bf_information_policy_field_decision(const BfInformationPolicy *policy, const char *field, BfDisclosureAudience audience, BfDisclosureDecision *out) {
    size_t index; unsigned int mask;
    if (!policy || !present(field) || !out || !(mask = bf_information_policy_audience_mask(audience))) return BF_INFORMATION_POLICY_INVALID;
    memset(out, 0, sizeof(*out));
    for (index = 0; index < policy->field_count; index++) if (strcmp(policy->fields[index].field, field) == 0) {
        out->allowed = (policy->fields[index].audience_mask & mask) != 0;
        if (!out->allowed) snprintf(out->reason, sizeof(out->reason), "%s", policy->fields[index].reason);
        return BF_INFORMATION_POLICY_OK;
    }
    snprintf(out->reason, sizeof(out->reason), "no disclosure rule declared for this field"); return BF_INFORMATION_POLICY_OK;
}

BfInformationPolicyStatus bf_information_policy_project_fields(const BfInformationPolicy *policy,
    const char *const *fields, size_t field_count, BfDisclosureAudience audience,
    BfDisclosureDecision *decisions) {
    size_t index;
    if (!policy || (field_count && (!fields || !decisions)) ||
        !bf_information_policy_audience_mask(audience)) return BF_INFORMATION_POLICY_INVALID;
    /* Reject the whole projection before touching its output; an invalid
     * later field must not leave a misleading partial disclosure decision. */
    for (index = 0; index < field_count; index++)
        if (!present(fields[index])) return BF_INFORMATION_POLICY_INVALID;
    for (index = 0; index < field_count; index++) {
        BfInformationPolicyStatus status = bf_information_policy_field_decision(policy, fields[index], audience, &decisions[index]);
        if (status != BF_INFORMATION_POLICY_OK) return status;
    }
    return BF_INFORMATION_POLICY_OK;
}

BfInformationPolicyStatus bf_information_policy_aggregate_decision(const BfInformationPolicy *policy, const char *statistic, int population, BfDisclosureAudience audience, BfAggregateDecision *out) {
    size_t index; unsigned int mask;
    if (!policy || !present(statistic) || population < 0 || !out || !(mask = bf_information_policy_audience_mask(audience))) return BF_INFORMATION_POLICY_INVALID;
    memset(out, 0, sizeof(*out));
    for (index = 0; index < policy->floor_count; index++) if (strcmp(policy->floors[index].statistic, statistic) == 0) {
        out->minimum_population = policy->floors[index].minimum_population;
        if (!(policy->floors[index].audience_mask & mask)) { snprintf(out->reason, sizeof(out->reason), "this audience may not receive this aggregate"); return BF_INFORMATION_POLICY_OK; }
        if (population < out->minimum_population) { snprintf(out->reason, sizeof(out->reason), "below the disclosure floor of %d", out->minimum_population); return BF_INFORMATION_POLICY_OK; }
        out->allowed = 1; return BF_INFORMATION_POLICY_OK;
    }
    snprintf(out->reason, sizeof(out->reason), "no aggregation floor declared for this statistic"); return BF_INFORMATION_POLICY_OK;
}

int bf_information_policy_evidence_visible(const BfInformationPolicy *policy, const char *witness_kind) {
    size_t index;
    if (!policy || !present(witness_kind)) return 0;
    for (index = 0; index < policy->evidence_release_count; index++) if (strcmp(policy->evidence_release[index], witness_kind) == 0) return 1;
    return 0;
}
