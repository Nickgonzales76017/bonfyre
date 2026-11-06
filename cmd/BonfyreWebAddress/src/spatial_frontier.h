#ifndef BONFYRE_SPATIAL_FRONTIER_H
#define BONFYRE_SPATIAL_FRONTIER_H

/* Serves /w/<world>/spatial when the request carries a frontier.
 *
 * Returns 1 when it answered, 0 when the route is not its business -- a
 * /w/<world>/spatial request with no `at` is the existing spatial surface and
 * falls through untouched.
 */
int serve_spatial_frontier(const char *method, const char *target,
                           const char *query, const char *accept);

#endif
