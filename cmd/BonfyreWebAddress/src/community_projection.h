#ifndef BONFYRE_COMMUNITY_PROJECTION_H
#define BONFYRE_COMMUNITY_PROJECTION_H

/* Returns 1 when the route was handled, 0 when it was not this module's route. */
int bf_serve_human_community(const char *method, const char *target,
                             const char *query);

#endif
