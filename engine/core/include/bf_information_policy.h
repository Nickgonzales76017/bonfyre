#ifndef BF_INFORMATION_POLICY_H
#define BF_INFORMATION_POLICY_H

/* Native declaration/evaluation ABI for InformationPolicy.  Authority to act
 * and entitlement to read remain separate questions; this answers only the
 * latter, per field and exact audience. */

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BF_INFORMATION_POLICY_MAX_RULES 256
#define BF_INFORMATION_POLICY_MAX_AUDIENCES 7
#define BF_INFORMATION_POLICY_TEXT_MAX 256

typedef struct BfInformationPolicy BfInformationPolicy;

typedef enum BfInformationPolicyStatus {
    BF_INFORMATION_POLICY_OK = 0,
    BF_INFORMATION_POLICY_INVALID,
    BF_INFORMATION_POLICY_EXISTS,
    BF_INFORMATION_POLICY_CAPACITY
} BfInformationPolicyStatus;

typedef enum BfDisclosureAudience {
    BF_AUDIENCE_SELF,
    BF_AUDIENCE_HOUSEHOLD,
    BF_AUDIENCE_GROUP,
    BF_AUDIENCE_COMMUNITY,
    BF_AUDIENCE_PUBLIC,
    BF_AUDIENCE_MARKET,
    BF_AUDIENCE_INSTITUTION
} BfDisclosureAudience;

typedef struct BfDisclosureDecision {
    int allowed;
    char reason[BF_INFORMATION_POLICY_TEXT_MAX];
} BfDisclosureDecision;

typedef struct BfAggregateDecision {
    int allowed;
    int minimum_population;
    char reason[BF_INFORMATION_POLICY_TEXT_MAX];
} BfAggregateDecision;

void bf_information_policy_init(BfInformationPolicy *policy);
/* Storage is caller-owned so a compiled policy can be embedded in a generated
 * contract without making the policy a second database. */
struct BfInformationPolicy {
    struct {
        char field[BF_INFORMATION_POLICY_TEXT_MAX];
        unsigned int audience_mask;
        char reason[BF_INFORMATION_POLICY_TEXT_MAX];
    } fields[BF_INFORMATION_POLICY_MAX_RULES];
    size_t field_count;
    struct {
        char statistic[BF_INFORMATION_POLICY_TEXT_MAX];
        int minimum_population;
        unsigned int audience_mask;
    } floors[BF_INFORMATION_POLICY_MAX_RULES];
    size_t floor_count;
    char evidence_release[BF_INFORMATION_POLICY_MAX_RULES][BF_INFORMATION_POLICY_TEXT_MAX];
    size_t evidence_release_count;
};

BfInformationPolicyStatus bf_information_policy_disclose(BfInformationPolicy *policy,
    const char *field, unsigned int audience_mask, const char *withhold_reason);
BfInformationPolicyStatus bf_information_policy_floor(BfInformationPolicy *policy,
    const char *statistic, int minimum_population, unsigned int audience_mask);
BfInformationPolicyStatus bf_information_policy_release_evidence(BfInformationPolicy *policy,
    const char *witness_kind);
BfInformationPolicyStatus bf_information_policy_field_decision(const BfInformationPolicy *policy,
    const char *field, BfDisclosureAudience audience, BfDisclosureDecision *out);
/* Every input field gets one decision in the same slot; a denied field is
 * explicitly withheld, never silently omitted from the projection. */
BfInformationPolicyStatus bf_information_policy_project_fields(const BfInformationPolicy *policy,
    const char *const *fields, size_t field_count, BfDisclosureAudience audience,
    BfDisclosureDecision *decisions);
BfInformationPolicyStatus bf_information_policy_aggregate_decision(const BfInformationPolicy *policy,
    const char *statistic, int population, BfDisclosureAudience audience, BfAggregateDecision *out);
int bf_information_policy_evidence_visible(const BfInformationPolicy *policy, const char *witness_kind);

unsigned int bf_information_policy_audience_mask(BfDisclosureAudience audience);
const char *bf_information_policy_audience_name(BfDisclosureAudience audience);
const char *bf_information_policy_status_name(BfInformationPolicyStatus status);

#ifdef __cplusplus
}
#endif

#endif
