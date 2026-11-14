#ifndef BF_ROUTE_RANKER_H
#define BF_ROUTE_RANKER_H

/*
 * Ternary route ranker after the AddressPlane funnel.  Weights in {-1,0,+1}
 * are fit from the sign of each feature's success/failure mean difference; a
 * difference under the threshold is 0, so a weak signal never pretends to
 * matter.  It only REORDERS eligible survivors: it never changes eligibility,
 * grants authority or proves anything, and the exact closure still verifies
 * the top choice.  Ported from components/BonfyreControlPlane/route_ranker.py.archive.
 */
#include <stddef.h>

#define BF_ROUTE_FEATURES 3 /* normalized priority, has target plane, retry free */

typedef struct { double f[BF_ROUTE_FEATURES]; int success; } BfRouteRecord;
typedef struct { int w[BF_ROUTE_FEATURES]; size_t trained_on; } BfRouteRanker;
typedef struct { const char *id; double f[BF_ROUTE_FEATURES]; double score; } BfRouteCandidate;

void bf_route_train(const BfRouteRecord *records, size_t n, double threshold, BfRouteRanker *out);
double bf_route_score(const BfRouteRanker *r, const double f[BF_ROUTE_FEATURES]);
/* Scores and sorts in place: best first, ties by id. */
void bf_route_rank(const BfRouteRanker *r, BfRouteCandidate *c, size_t n);
/* Records from a work_items table (priority, target_plane, [attempt,] state);
 * satisfied/effected is success.  Returns the count written (<= cap), 0 when
 * the table is absent, -1 on error. */
long bf_route_records_from_work(void *sqlite_database, BfRouteRecord *out, size_t cap);

#endif
