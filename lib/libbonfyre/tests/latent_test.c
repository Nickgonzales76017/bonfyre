/* Latent coordinates compare only inside a passport or under a contract. */
#include "bf_latent.h"

#include <math.h>
#include <stdio.h>

static int failures = 0;
#define CHECK(c, m) do { if (!(c)) { fprintf(stderr, "FAIL: %s (line %d)\n", m, __LINE__); failures++; } } while (0)

int main(void) {
    BfLatentPassport a = {"space:nomic-128", "sha256:enc1", "cosine", "sha256:fc", "policy:public", 3};
    BfLatentPassport a2 = a;
    BfLatentPassport b = {"space:other", "sha256:enc2", "cosine", "sha256:fc", "policy:public", 3};
    BfLatentPassport e = {"space:euclid", "sha256:enc3", "l2", "sha256:fc", "policy:public", 3};
    double v1[] = {1, 0, 0}, v2[] = {1, 1, 0}, v3[] = {0, 1, 0};
    BfLatentProjection p1 = {"subject:a", "profile", "f1", "sha256:s1", v1, 3};
    BfLatentProjection p2 = {"subject:b", "profile", "f1", "sha256:s2", v2, 3};
    BfLatentProjection p3 = {"subject:a", "profile", "f2", "sha256:s3", v3, 3};
    BfLatentProjection bad_dim = {"subject:a", "profile", "f1", "sha256:s1", v1, 2};
    BfLatentProjection no_src = {"subject:a", "profile", "f1", "", v1, 3};
    BfLatentContract none = {0}, with_t = {"space:nomic-128", "space:other", "sha256:transform"},
                     no_t = {"space:nomic-128", "space:other", NULL};
    double s = -9, d = -9;

    CHECK(bf_latent_project(&a, &p1) == BF_LATENT_OK, "valid projection");
    CHECK(bf_latent_project(&a, &bad_dim) == BF_LATENT_DIMENSION, "dimension violates passport");
    CHECK(bf_latent_project(&a, &no_src) == BF_LATENT_IDENTITY, "projection needs source identity");

    CHECK(bf_latent_similarity(&p1, &p2, &a, &a2, &none, &s) == BF_LATENT_OK && fabs(s - 0.70710678) < 1e-6, "same space compares");
    s = -9;
    CHECK(bf_latent_similarity(&p1, &p2, &a, &b, &none, &s) == BF_LATENT_INCOMPARABLE && s == -9, "different spaces refused, nothing computed");
    CHECK(bf_latent_similarity(&p1, &p2, &a, &b, &no_t, &s) == BF_LATENT_INCOMPARABLE, "a contract without a transform does not permit");
    CHECK(bf_latent_similarity(&p1, &p2, &a, &b, &with_t, &s) == BF_LATENT_OK, "a contract with a transform permits");
    CHECK(bf_latent_similarity(&p1, &p2, &e, &e, &none, &s) == BF_LATENT_METRIC, "only cosine spaces compare");

    CHECK(bf_latent_trajectory_delta(&p1, &p3, &d) == BF_LATENT_OK && fabs(d - sqrt(2.0)) < 1e-9, "trajectory distance");
    CHECK(bf_latent_trajectory_delta(&p1, &p2, &d) == BF_LATENT_CROSS_SUBJECT, "trajectory cannot cross subjects");
    CHECK(bf_latent_watch(sqrt(2.0), 1.0) == 1 && bf_latent_watch(0.5, 1.0) == 0, "watch signals at threshold");

    if (failures) return 1;
    puts("latent fabric passports and contracts: PASS");
    return 0;
}
