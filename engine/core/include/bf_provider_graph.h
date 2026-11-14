#ifndef BF_PROVIDER_GRAPH_H
#define BF_PROVIDER_GRAPH_H

/*
 * Provider topology above the provider state fold.
 *   ProviderGraph       capability bids as a metric VECTOR, never one scalar
 *                       in storage; a tournament ranks under a weight policy,
 *                       with each dimension's polarity part of its meaning
 *                       (cost, latency, semantic_loss are spent: more is worse)
 *   PlacementGraph      where a selected realization resides
 *   RuntimeParityGraph  carrier pairs PROVEN equivalent; a pair that only ran
 *                       is not parity
 * Forbidden: available -> authorized (a bid without authority is not
 * selectable), cheapest -> valid (a ranking is an order of candidates).
 * Ported from components/BonfyreControlPlane/provider_graphs.py.archive (same tables).
 */
#include <stddef.h>
#include <stdint.h>

#define BF_PROVIDER_DIMS 8
extern const char *const BF_PROVIDER_DIM_NAMES[BF_PROVIDER_DIMS];
/* cost quality latency trust authority locality semantic_loss availability */

typedef struct { const char *provider; double score; } BfProviderRank;

int bf_provider_graph_open(void *sqlite_database);
/* metric: one value per BF_PROVIDER_DIM_NAMES; present[i]=0 leaves a dimension absent. */
int bf_provider_record_bid(void *db, const char *capability, const char *provider,
                           const double metric[BF_PROVIDER_DIMS], const int present[BF_PROVIDER_DIMS]);
/* +1 where more is better, -1 where more is worse. */
int bf_provider_polarity(const char *dim);
/* Rank bids (weights NULL = all 1; weights are importances >= 0).  Writes up to
 * cap ranks with provider strings owned by the caller (free each).  Returns the
 * number ranked or -1.  require_authority drops bids whose authority <= 0. */
int bf_provider_tournament(void *db, const char *capability, const double *weights,
                           int require_authority, BfProviderRank *out, size_t cap);
int bf_provider_record_placement(void *db, const char *resource, const char *location);
int bf_provider_placement_of(void *db, const char *resource, char *out, size_t cap); /* 1/0/-1 */
int bf_provider_record_parity(void *db, const char *capability, const char *carrier_a, const char *carrier_b, int proven);
/* Proven pairs only, each ordered (a < b).  Count or -1. */
int64_t bf_provider_parity_group(void *db, const char *capability,
                                 int (*visit)(const char *a, const char *b, void *ctx), void *ctx);

#endif
