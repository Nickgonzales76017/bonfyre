#ifndef BF_LATENT_H
#define BF_LATENT_H

/*
 * Governed learned coordinates attached to exact AddressPlane subjects.
 *
 * A vector means something only inside its LatentSpacePassport (space,
 * encoder digest, dimension, metric).  Two projections compare only when the
 * passports are the same space, or a ComparabilityContract names both spaces
 * with a transform digest; otherwise similarity is refused, never computed.
 * A trajectory never crosses subjects, and a watch only signals -- it never
 * changes semantic truth.  Ported from
 * components/BonfyreControlPlane/latent_fabric.py.archive.
 */
#include <stddef.h>

typedef struct {
    const char *space_id, *encoder_digest, *metric, *feature_closure_digest, *information_policy_ref;
    size_t dimension;
} BfLatentPassport;

typedef struct {
    const char *left_space, *right_space, *transform_digest; /* transform may be NULL */
} BfLatentContract;

typedef struct {
    const char *subject_ref, *profile_id, *causal_frontier, *source_digest;
    const double *vector; size_t dimension;
} BfLatentProjection;

typedef enum {
    BF_LATENT_OK = 0,
    BF_LATENT_DIMENSION,        /* vector length violates the passport */
    BF_LATENT_IDENTITY,         /* subject or source digest missing */
    BF_LATENT_INCOMPARABLE,     /* no shared space and no contract with a transform */
    BF_LATENT_METRIC,           /* only declared cosine spaces compare */
    BF_LATENT_CROSS_SUBJECT     /* a trajectory may not cross subjects */
} BfLatentStatus;

/* Validate a projection against its passport (dimension, exact identity). */
BfLatentStatus bf_latent_project(const BfLatentPassport *passport, const BfLatentProjection *projection);

int bf_latent_contract_permits(const BfLatentContract *contract, const BfLatentPassport *left, const BfLatentPassport *right);

/* Cosine similarity, only under a permitting contract and cosine spaces. */
BfLatentStatus bf_latent_similarity(const BfLatentProjection *left, const BfLatentProjection *right,
                                    const BfLatentPassport *left_passport, const BfLatentPassport *right_passport,
                                    const BfLatentContract *contract, double *out);

/* Euclidean distance between two frontiers of one subject. */
BfLatentStatus bf_latent_trajectory_delta(const BfLatentProjection *previous, const BfLatentProjection *current, double *distance);

/* An observation signal only: 1 when the delta reaches the threshold. */
int bf_latent_watch(double distance, double threshold);

const char *bf_latent_status_name(BfLatentStatus status);

#endif
