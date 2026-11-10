#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "domain_profile_projection.h"

int main(void) {
    char select_list[4096];
    assert(bf_domain_profile_select_list(select_list, sizeof(select_list), "parcel", "reso", BF_AUDIENCE_PUBLIC));
    assert(strstr(select_list, "account AS ListingKey"));
    assert(strstr(select_list, "total_value AS TaxAssessedValue"));
    assert(strstr(select_list, "owner AS OwnerName"));
    assert(!strstr(select_list, "owner_mailing_address"));
    assert(!strcmp(bf_domain_profile_standard("parcel", "ogc"), "OGC API Features"));
    assert(!strcmp(bf_domain_profile_container("parcel", "mismo"), "MESSAGE.DEAL_SETS.SUBJECT_PROPERTIES"));
    assert(!bf_domain_profile_select_list(select_list, sizeof(select_list), "parcel", "zillow", BF_AUDIENCE_PUBLIC));
    assert(!bf_domain_profile_select_list(select_list, sizeof(select_list), "parcel", "reso", (BfDisclosureAudience)99));
    puts("domain profile projection: PASS");
    return 0;
}
