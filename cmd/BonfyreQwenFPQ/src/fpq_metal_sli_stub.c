#include "fpqx.h"

int fpqx_metal_sli_try_matvec(fpqx_sli_ctx_t *ctx,
                              const float *x,
                              float *output,
                              uint64_t haar_seed) {
    (void)ctx; (void)x; (void)output; (void)haar_seed;
    return -1;
}

void fpqx_metal_sli_free(fpqx_sli_ctx_t *ctx) {
    (void)ctx;
}
