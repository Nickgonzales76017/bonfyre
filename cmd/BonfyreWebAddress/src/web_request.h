#ifndef BONFYRE_WEB_REQUEST_H
#define BONFYRE_WEB_REQUEST_H
/* One request, one shape. A route owner reads what it needs from here instead
 * of main.c handing each owner its own argument list; which owner answers
 * which path is declared in schemas/control/web-routes.cue and compiled into
 * generated/native_web_routes.h, never written as another branch in main.c. */
#include <string.h>
struct BfMatrix;
/* What is true of this request, computed once by the server. A route row
 * names the facts it needs in `when`; the dispatcher asks its owner only if
 * every one of them holds. */
enum {
    BF_WHEN_GET = 1u << 0,                /* method is GET */
    BF_WHEN_SITE_ROOT = 1u << 1,          /* the server was given a site root */
    BF_WHEN_HTML = 1u << 2,               /* Accept names text/html */
    BF_WHEN_NOT_JSON = 1u << 3,           /* not a JSON-only Accept */
    BF_WHEN_NOT_TERRITORY_APEX = 1u << 4, /* host is not a territory apex */
    BF_WHEN_JSON = 1u << 5,               /* Accept names JSON */
};
typedef struct {
    const char *method, *target, *query, *body, *raw, *host, *bearer, *auth_db, *accept;
    struct BfMatrix *m;
    const char *root, *origin, *site_bearer, *site_client, *if_none_match, *payment, *origin_method, *institution;
    unsigned facts;
    /* The connection itself, for the transport-level answers (a health probe,
     * a refused method) that are written before any page framing. */
    int socket;
    /* The name the matched route row gives this path (`as` in the CUE), so an
     * owner reasons about which of its declared paths was asked, never about
     * the path's bytes. NULL for a row without one. */
    const char *as;
} BfRequest;
static inline int bf_request_is(const BfRequest *r, const char *as) {
    return r && r->as && as && !strcmp(r->as, as);
}
/* The query an owner reads as absent when nothing was asked. */
static inline const char *bf_request_query_opt(const BfRequest *r) {
    return r && r->query && r->query[0] ? r->query : NULL;
}
typedef enum { BF_ROUTE_EXACT = 0, BF_ROUTE_PREFIX = 1, BF_ROUTE_STRICT = 2, BF_ROUTE_ANY = 3 } BfRouteMatch;
typedef int (*BfRouteOwner)(const BfRequest *);
typedef struct { const char *path; BfRouteMatch match; BfRouteOwner owner; unsigned when; const char *as; } BfRoute;
/* Exact paths also answer with one trailing slash, strict paths only with
 * their own bytes; prefixes match by bytes; any asks the owner every time. */
static inline int bf_route_matches(const BfRoute *route, const char *target) {
    size_t n;
    if (!route || !target) return 0;
    if (route->match == BF_ROUTE_ANY) return 1;
    if (!route->path) return 0;
    n = strlen(route->path);
    if (route->match == BF_ROUTE_PREFIX) return !strncmp(target, route->path, n);
    if (route->match == BF_ROUTE_STRICT) return !strcmp(target, route->path);
    return !strncmp(target, route->path, n) && (!target[n] || (target[n] == '/' && !target[n + 1]));
}
/* Each owner still checks its own sub-paths and method; the table only
 * decides who is asked, in declared order, and stops at the first answer.
 * An owner already asked for this request is not asked again. */
static inline int bf_route_dispatch(const BfRoute *routes, size_t count, const BfRequest *request) {
    BfRouteOwner asked[256]; size_t n_asked = 0;
    for (size_t i = 0; request && request->target && i < count; i++) {
        if ((request->facts & routes[i].when) != routes[i].when) continue;
        if (!bf_route_matches(&routes[i], request->target)) continue;
        int seen = 0;
        for (size_t k = 0; k < n_asked; k++) seen |= asked[k] == routes[i].owner;
        if (seen) continue;
        if (n_asked < sizeof(asked) / sizeof(asked[0])) asked[n_asked++] = routes[i].owner;
        BfRequest named = *request;
        named.as = routes[i].as;
        if (routes[i].owner(&named)) return 1;
    }
    return 0;
}
/* A caller that already chose its owner (the static export) asks it through
 * the owner's own first matching row, so the owner sees the same `as`. An
 * owner with no row for this path is not asked. */
static inline int bf_route_ask(const BfRoute *routes, size_t count, BfRouteOwner owner, const BfRequest *request) {
    for (size_t i = 0; request && request->target && i < count; i++) {
        if (routes[i].owner != owner || !bf_route_matches(&routes[i], request->target)) continue;
        BfRequest named = *request;
        named.as = routes[i].as;
        return owner(&named);
    }
    return 0;
}
#endif
