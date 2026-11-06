#ifndef BONFYRE_DOMAIN_PROFILE_PROJECTION_H
#define BONFYRE_DOMAIN_PROFILE_PROJECTION_H

#include <stddef.h>
#include "bf_information_policy.h"

/* Finds a generated domain profile/carrier and lowers its lawful field mapping
 * into a SQL select list over an already-owned semantic relation.  It does not
 * query, retain, or invent facts. */
int bf_domain_profile_select_list(char *out, size_t out_size, const char *profile,
    const char *carrier, BfDisclosureAudience audience);
const char *bf_domain_profile_standard(const char *profile, const char *carrier);
const char *bf_domain_profile_container(const char *profile, const char *carrier);

#endif
