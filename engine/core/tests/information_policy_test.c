#include "bf_information_policy.h"

#include <stdio.h>
#include <string.h>

#define CHECK(condition) do { if (!(condition)) { fprintf(stderr, "information_policy_test:%d: %s\n", __LINE__, #condition); return 1; } } while (0)

int main(void) {
    BfInformationPolicy policy; BfDisclosureDecision field; BfAggregateDecision aggregate;
    const char *projected_fields[] = {"address", "owner", "undeclared"};
    const char *invalid_fields[] = {"address", ""};
    BfDisclosureDecision projected[3];
    unsigned int public = bf_information_policy_audience_mask(BF_AUDIENCE_PUBLIC);
    unsigned int institution = bf_information_policy_audience_mask(BF_AUDIENCE_INSTITUTION);
    bf_information_policy_init(&policy);
    CHECK(bf_information_policy_disclose(&policy, "address", public | institution, "private address") == BF_INFORMATION_POLICY_OK);
    CHECK(bf_information_policy_disclose(&policy, "owner", institution, "owner is restricted") == BF_INFORMATION_POLICY_OK);
    CHECK(bf_information_policy_disclose(&policy, "sealed", 0, "sealed to every audience") == BF_INFORMATION_POLICY_OK);
    CHECK(bf_information_policy_field_decision(&policy, "owner", BF_AUDIENCE_PUBLIC, &field) == BF_INFORMATION_POLICY_OK && !field.allowed && !strcmp(field.reason, "owner is restricted"));
    CHECK(bf_information_policy_field_decision(&policy, "undeclared", BF_AUDIENCE_PUBLIC, &field) == BF_INFORMATION_POLICY_OK && !field.allowed && strstr(field.reason, "no disclosure rule"));
    CHECK(bf_information_policy_field_decision(&policy, "address", BF_AUDIENCE_PUBLIC, &field) == BF_INFORMATION_POLICY_OK && field.allowed);
    CHECK(bf_information_policy_field_decision(&policy, "sealed", BF_AUDIENCE_INSTITUTION, &field) == BF_INFORMATION_POLICY_OK && !field.allowed && !strcmp(field.reason, "sealed to every audience"));
    CHECK(bf_information_policy_project_fields(&policy, projected_fields, 3, BF_AUDIENCE_PUBLIC, projected) == BF_INFORMATION_POLICY_OK);
    CHECK(projected[0].allowed && !projected[0].reason[0]);
    CHECK(!projected[1].allowed && !strcmp(projected[1].reason, "owner is restricted"));
    CHECK(!projected[2].allowed && strstr(projected[2].reason, "no disclosure rule"));
    CHECK(bf_information_policy_project_fields(&policy, NULL, 0, BF_AUDIENCE_PUBLIC, NULL) == BF_INFORMATION_POLICY_OK);
    CHECK(bf_information_policy_project_fields(&policy, projected_fields, 3, (BfDisclosureAudience)99, projected) == BF_INFORMATION_POLICY_INVALID);
    CHECK(bf_information_policy_project_fields(&policy, projected_fields, 3, BF_AUDIENCE_PUBLIC, NULL) == BF_INFORMATION_POLICY_INVALID);
    memset(projected, 0x5a, sizeof(projected));
    CHECK(bf_information_policy_project_fields(&policy, invalid_fields, 2, BF_AUDIENCE_PUBLIC, projected) == BF_INFORMATION_POLICY_INVALID);
    CHECK(((unsigned char *)&projected[0])[0] == 0x5a);
    CHECK(bf_information_policy_floor(&policy, "median_value", 3, public | institution) == BF_INFORMATION_POLICY_OK);
    CHECK(bf_information_policy_floor(&policy, "sealed_count", 2, 0) == BF_INFORMATION_POLICY_OK);
    CHECK(bf_information_policy_aggregate_decision(&policy, "sealed_count", 100, BF_AUDIENCE_INSTITUTION, &aggregate) == BF_INFORMATION_POLICY_OK && !aggregate.allowed);
    CHECK(bf_information_policy_floor(&policy, "single", 1, public) == BF_INFORMATION_POLICY_INVALID);
    CHECK(bf_information_policy_aggregate_decision(&policy, "median_value", 2, BF_AUDIENCE_PUBLIC, &aggregate) == BF_INFORMATION_POLICY_OK && !aggregate.allowed && aggregate.minimum_population == 3 && !strcmp(aggregate.reason, "below the disclosure floor of 3"));
    CHECK(bf_information_policy_aggregate_decision(&policy, "median_value", 3, BF_AUDIENCE_PUBLIC, &aggregate) == BF_INFORMATION_POLICY_OK && aggregate.allowed);
    CHECK(bf_information_policy_release_evidence(&policy, "assessment_roll_receipt") == BF_INFORMATION_POLICY_OK);
    CHECK(bf_information_policy_evidence_visible(&policy, "assessment_roll_receipt") && !bf_information_policy_evidence_visible(&policy, "private_note"));
    puts("information_policy_test: PASS"); return 0;
}
