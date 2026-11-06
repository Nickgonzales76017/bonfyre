#ifndef AUREKAI_CREATOR_MIX_KERNEL_H
#define AUREKAI_CREATOR_MIX_KERNEL_H
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
/* Caller owns source/destination and lifetime. This pure, allocation-free
 * ordered f32 operation is shared by native offline and browser WASM paths. */
void aurekai_mix_f32(float *dst, const float *src, float gain, size_t count);
#ifdef __cplusplus
}
#endif
#endif
