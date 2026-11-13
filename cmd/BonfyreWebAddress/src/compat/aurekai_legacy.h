#ifndef BONFYRE_AUREKAI_LEGACY_H
#define BONFYRE_AUREKAI_LEGACY_H
/*
 * DEPRECATED HOST COMPATIBILITY, not an engine default or institution grant.
 * This is historical Aurekai deployment material retained only for existing
 * releases until its business-owned adapter has migrated to aurekai/aurekai.
 *
 * Callers MUST consult the tenant-owned host map first. An explicitly
 * configured but invalid map fails closed; it must never select this adapter.
 */
#include <string.h>

static inline const char *bf_legacy_aurekai_public_host(const char *territory) {
    if (!territory) return NULL;
    if (!strcmp(territory, "work")) return "aurekai.work";
    if (!strcmp(territory, "life")) return "aurekai.life";
    if (!strcmp(territory, "live")) return "aurekai.live";
    if (!strcmp(territory, "shop")) return "aurekai.shop";
    return NULL;
}
static inline const char *bf_legacy_aurekai_account_host(const char *territory) {
    if (!territory) return NULL;
    if (!strcmp(territory, "work")) return "signin.aurekai.work";
    if (!strcmp(territory, "life")) return "auth.aurekai.life";
    if (!strcmp(territory, "live")) return "auth.aurekai.live";
    if (!strcmp(territory, "shop")) return "auth.aurekai.shop";
    return NULL;
}
static inline const char *bf_legacy_aurekai_territory(const char *host, int include_accounts) {
    if (!host) return NULL;
    const char *const territories[]={"work","life","live","shop"};
    for (size_t i=0;i<4;i++) {
        if (!strcmp(host,bf_legacy_aurekai_public_host(territories[i])) ||
            (include_accounts && !strcmp(host,bf_legacy_aurekai_account_host(territories[i]))))
            return territories[i];
    }
    return NULL;
}
static inline int bf_legacy_aurekai_account_host_is(const char *host) {
    if (!host) return 0;
    const char *const territories[]={"work","life","live","shop"};
    for (size_t i=0;i<4;i++) if (!strcmp(host,bf_legacy_aurekai_account_host(territories[i]))) return 1;
    return 0;
}
/* The four downstream client redirects are explicit historical allowlist
 * members, NOT inferred just because their names end in .aurekai.work. */
static inline int bf_legacy_aurekai_redirect_destination(const char *host) {
    return host && (!strcmp(host,"upper90.aurekai.work") ||
                    !strcmp(host,"jrss.aurekai.work") ||
                    !strcmp(host,"ai-loss.aurekai.work") ||
                    !strcmp(host,"leads.aurekai.work"));
}
static inline int bf_legacy_aurekai_subhost(const char *host) {
    const char *const roots[]={".aurekai.work",".aurekai.life",".aurekai.live",".aurekai.shop"};
    size_t n=host?strlen(host):0;
    if (!host) return 0;
    for(size_t i=0;i<4;i++) {
        size_t k=strlen(roots[i]);
        if(n>k && !strcmp(host+n-k,roots[i])) return 1;
    }
    return 0;
}
#endif
