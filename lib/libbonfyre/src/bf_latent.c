/* Governed latent coordinates.  See bf_latent.h. */
#include "bf_latent.h"

#include <math.h>
#include <string.h>

static int same(const char *a, const char *b) { return a && b && strcmp(a, b) == 0; }
static int present(const char *s) { return s && *s; }

BfLatentStatus bf_latent_project(const BfLatentPassport *p, const BfLatentProjection *x) {
    if (!p || !x || x->dimension != p->dimension || !x->vector) return BF_LATENT_DIMENSION;
    if (!present(x->subject_ref) || !present(x->source_digest)) return BF_LATENT_IDENTITY;
    return BF_LATENT_OK;
}

int bf_latent_contract_permits(const BfLatentContract *c, const BfLatentPassport *l, const BfLatentPassport *r) {
    if (!l || !r) return 0;
    if (same(l->space_id, r->space_id) && same(l->encoder_digest, r->encoder_digest) &&
        l->dimension == r->dimension && same(l->metric, r->metric)) return 1;
    return c && same(c->left_space, l->space_id) && same(c->right_space, r->space_id) && present(c->transform_digest);
}

BfLatentStatus bf_latent_similarity(const BfLatentProjection *a, const BfLatentProjection *b,
                                    const BfLatentPassport *pa, const BfLatentPassport *pb,
                                    const BfLatentContract *c, double *out) {
    if (!a || !b || !out) return BF_LATENT_IDENTITY;
    if (!bf_latent_contract_permits(c, pa, pb)) return BF_LATENT_INCOMPARABLE;
    if (!same(pa->metric, "cosine") || !same(pb->metric, "cosine")) return BF_LATENT_METRIC;
    if (a->dimension != b->dimension) return BF_LATENT_DIMENSION;
    double dot = 0, la = 0, lb = 0;
    for (size_t i = 0; i < a->dimension; i++) { dot += a->vector[i] * b->vector[i]; la += a->vector[i] * a->vector[i]; lb += b->vector[i] * b->vector[i]; }
    *out = (la > 0 && lb > 0) ? dot / (sqrt(la) * sqrt(lb)) : 0.0;
    return BF_LATENT_OK;
}

BfLatentStatus bf_latent_trajectory_delta(const BfLatentProjection *p, const BfLatentProjection *c, double *d) {
    if (!p || !c || !d) return BF_LATENT_IDENTITY;
    if (!same(p->subject_ref, c->subject_ref)) return BF_LATENT_CROSS_SUBJECT;
    if (p->dimension != c->dimension) return BF_LATENT_DIMENSION;
    double s = 0;
    for (size_t i = 0; i < p->dimension; i++) { double e = p->vector[i] - c->vector[i]; s += e * e; }
    *d = sqrt(s);
    return BF_LATENT_OK;
}

int bf_latent_watch(double distance, double threshold) { return distance >= threshold; }

const char *bf_latent_status_name(BfLatentStatus s) {
    switch (s) {
    case BF_LATENT_OK: return "ok";
    case BF_LATENT_DIMENSION: return "dimension";
    case BF_LATENT_IDENTITY: return "identity";
    case BF_LATENT_INCOMPARABLE: return "incomparable";
    case BF_LATENT_METRIC: return "metric";
    case BF_LATENT_CROSS_SUBJECT: return "cross_subject";
    }
    return "unknown";
}
