#include "domain_profile_projection.h"

#include <stdio.h>
#include <string.h>

#include "../generated/native_domain_profiles.h"

static const BfNativeDomainProfile *profile_for(const char *name) {
    size_t i;
    for (i = 0; i < BF_NATIVE_DOMAIN_PROFILE_COUNT; ++i)
        if (!strcmp(bf_native_domain_profiles[i].name, name)) return &bf_native_domain_profiles[i];
    return NULL;
}

static const BfDomainCarrierProjection *projection_for(const BfNativeDomainProfile *profile, const char *carrier) {
    size_t i;
    if (!profile) return NULL;
    for (i = 0; i < profile->projection_count; ++i)
        if (!strcmp(profile->projections[i].carrier, carrier)) return &profile->projections[i];
    return NULL;
}

static const BfDomainFieldRule *rule_for(const BfNativeDomainProfile *profile, const char *semantic) {
    size_t i;
    for (i = 0; i < profile->field_count; ++i)
        if (!strcmp(profile->fields[i].name, semantic)) return &profile->fields[i];
    return NULL;
}

int bf_domain_profile_select_list(char *out, size_t out_size, const char *profile_name,
    const char *carrier, BfDisclosureAudience audience) {
    const BfNativeDomainProfile *profile = profile_for(profile_name);
    const BfDomainCarrierProjection *projection = projection_for(profile, carrier);
    size_t i, used = 0;
    if (!out || !out_size || !profile || !projection || !bf_information_policy_audience_mask(audience)) return 0;
    out[0] = '\0';
    for (i = 0; i < projection->field_count; ++i) {
        const BfDomainFieldRule *rule = rule_for(profile, projection->fields[i].semantic);
        int written;
        if (!rule) return 0; /* generated declaration cannot be bypassed */
        if (!(rule->audience_mask & bf_information_policy_audience_mask(audience))) continue;
        written = snprintf(out + used, out_size - used, "%s%s AS %s",
            used ? ", " : "", projection->fields[i].semantic, projection->fields[i].carrier);
        if (written < 0 || (size_t)written >= out_size - used) return 0;
        used += (size_t)written;
    }
    return used != 0;
}

const char *bf_domain_profile_standard(const char *profile_name, const char *carrier) {
    const BfDomainCarrierProjection *projection = projection_for(profile_for(profile_name), carrier);
    return projection ? projection->standard : NULL;
}

const char *bf_domain_profile_container(const char *profile_name, const char *carrier) {
    const BfDomainCarrierProjection *projection = projection_for(profile_for(profile_name), carrier);
    return projection ? projection->container : NULL;
}
