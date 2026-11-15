/* Generated from schemas/control/estate-hosts.cue by tools/compile_estate_hosts.sh. Do not edit. */
#ifndef BONFYRE_NATIVE_ESTATE_HOSTS_H
#define BONFYRE_NATIVE_ESTATE_HOSTS_H
#include <stdio.h>
#include <string.h>
#define BF_ORIGIN_WORK "https://aurekai.work"
#define BF_ORIGIN_LIFE "https://aurekai.life"
#define BF_ORIGIN_LIVE "https://aurekai.live"
#define BF_ORIGIN_SHOP "https://aurekai.shop"
#define BF_ORIGIN_SIGNIN "https://signin.aurekai.work"
#define BF_ORIGIN_DOCS "https://docs.aurekai.work"
#define BF_ORIGIN_INTERNET "https://internet.aurekai.work"
#define BF_ORIGIN_NIGHTGRID "https://nightgrid.aurekai.live"
typedef struct { const char *territory, *host, *origin; int territory_role; } BfEstateHost;
static const BfEstateHost bf_estate_hosts[] = {
    {"work", "aurekai.work", BF_ORIGIN_WORK, 1},
    {"life", "aurekai.life", BF_ORIGIN_LIFE, 1},
    {"live", "aurekai.live", BF_ORIGIN_LIVE, 1},
    {"shop", "aurekai.shop", BF_ORIGIN_SHOP, 1},
    {"signin", "signin.aurekai.work", BF_ORIGIN_SIGNIN, 0},
    {"docs", "docs.aurekai.work", BF_ORIGIN_DOCS, 0},
    {"internet", "internet.aurekai.work", BF_ORIGIN_INTERNET, 0},
    {"nightgrid", "nightgrid.aurekai.live", BF_ORIGIN_NIGHTGRID, 0},
};
/* The origin (https://host, no trailing slash) a territory answers on, or NULL. */
static inline const char *bf_estate_origin(const char *territory) {
    for (size_t i = 0; territory && i < sizeof(bf_estate_hosts) / sizeof(bf_estate_hosts[0]); i++)
        if (!strcmp(bf_estate_hosts[i].territory, territory)) return bf_estate_hosts[i].origin;
    return NULL;
}
/* Print a territory URL: origin then path, as written. Returns 0 for an unknown territory. */
static inline int bf_estate_url_put(FILE *out, const char *territory, const char *path) {
    const char *origin = bf_estate_origin(territory);
    if (!out || !origin) return 0;
    fputs(origin, out);
    if (path) fputs(path, out);
    return 1;
}
#endif
