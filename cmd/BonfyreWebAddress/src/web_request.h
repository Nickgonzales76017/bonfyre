#ifndef BONFYRE_WEB_REQUEST_H
#define BONFYRE_WEB_REQUEST_H
/* One request, one shape. A route owner reads what it needs from here instead
 * of main.c handing each owner its own argument list; which owner answers
 * which path is declared in schemas/control/web-routes.cue and compiled into
 * generated/native_web_routes.h, never written as another branch in main.c. */
#include <string.h>
typedef struct {
    const char *method, *target, *query, *body, *raw, *host, *bearer, *auth_db, *accept;
} BfRequest;
typedef enum { BF_ROUTE_EXACT = 0, BF_ROUTE_PREFIX = 1 } BfRouteMatch;
typedef int (*BfRouteOwner)(const BfRequest *);
typedef struct { const char *path; BfRouteMatch match; BfRouteOwner owner; } BfRoute;
/* Exact paths also answer with one trailing slash; prefixes match by bytes. */
static inline int bf_route_matches(const BfRoute *route, const char *target) {
    size_t n;
    if (!route || !route->path || !target) return 0;
    n = strlen(route->path);
    if (route->match == BF_ROUTE_PREFIX) return !strncmp(target, route->path, n);
    return !strncmp(target, route->path, n) && (!target[n] || (target[n] == '/' && !target[n + 1]));
}
/* Each owner still checks its own exact paths and method; the table only
 * decides who is asked, in declared order, and stops at the first answer. */
static inline int bf_route_dispatch(const BfRoute *routes, size_t count, const BfRequest *request) {
    BfRouteOwner asked[64]; size_t n_asked = 0;
    for (size_t i = 0; request && request->target && i < count; i++) {
        if (!bf_route_matches(&routes[i], request->target)) continue;
        int seen = 0;
        for (size_t k = 0; k < n_asked; k++) seen |= asked[k] == routes[i].owner;
        if (seen) continue;
        if (n_asked < sizeof(asked) / sizeof(asked[0])) asked[n_asked++] = routes[i].owner;
        if (routes[i].owner(request)) return 1;
    }
    return 0;
}
#endif
