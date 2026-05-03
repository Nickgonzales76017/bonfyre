#ifndef FPQ_NEON_H
#define FPQ_NEON_H

#include <stddef.h>
#include <stdint.h>

float fpq_neon_score_block(
    const float *e8_pts,
    const float *tile_data,
    const float *tile_indices,
    const float *x_spectral,
    float rms,
    float wnorm,
    float lattice_scale,
    int effective_k
);

float fpq_neon_qjl_score(
    const float *x_spectral,
    uint64_t qjl_bits,
    uint64_t proj_seed,
    float residual_norm,
    size_t n_projections
);

void fpq_neon_fwht_256(float *x);
void fpq_neon_random_signs_256(float *x, uint64_t seed);

/* Fast 256-element dot product (4-accumulator NEON pipeline) */
float fpq_neon_dot256(const float *a, const float *b);

#endif /* FPQ_NEON_H */
