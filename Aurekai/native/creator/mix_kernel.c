#include "mix_kernel.h"
void aurekai_mix_f32(float *dst, const float *src, float gain, size_t count) {
  for (size_t i = 0; i < count; ++i) dst[i] += src[i] * gain;
}
