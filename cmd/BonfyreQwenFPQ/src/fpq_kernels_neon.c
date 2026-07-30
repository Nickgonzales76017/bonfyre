/*
 * fpq_kernels_neon.c — Fused NEON kernels for Qwen inference
 */
#include "fpq_kernels_neon.h"
#include <math.h>
#include <string.h>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>

float fpq_i8_f32_dot_neon(const int8_t *w, const float *x, size_t n) {
    float32x4_t s0 = vdupq_n_f32(0.f), s1 = vdupq_n_f32(0.f);
    float32x4_t s2 = vdupq_n_f32(0.f), s3 = vdupq_n_f32(0.f);
    size_t i = 0;

    for (; i + 16 <= n; i += 16) {
        int8x16_t wi = vld1q_s8(w + i);
        int16x8_t lo = vmovl_s8(vget_low_s8(wi));
        int16x8_t hi = vmovl_s8(vget_high_s8(wi));

        s0 = vfmaq_f32(s0, vcvtq_f32_s32(vmovl_s16(vget_low_s16(lo))), vld1q_f32(x + i));
        s1 = vfmaq_f32(s1, vcvtq_f32_s32(vmovl_s16(vget_high_s16(lo))), vld1q_f32(x + i + 4));
        s2 = vfmaq_f32(s2, vcvtq_f32_s32(vmovl_s16(vget_low_s16(hi))), vld1q_f32(x + i + 8));
        s3 = vfmaq_f32(s3, vcvtq_f32_s32(vmovl_s16(vget_high_s16(hi))), vld1q_f32(x + i + 12));
    }

    float acc = vaddvq_f32(vaddq_f32(vaddq_f32(s0, s1), vaddq_f32(s2, s3)));
    for (; i < n; i++) acc += (float)w[i] * x[i];
    return acc;
}

void fpq_i8_f32_gemv_neon(const int8_t *W, const float *scales,
                          const float *x, float *y,
                          size_t rows, size_t cols) {
    for (size_t r = 0; r < rows; r++) {
        float scale = scales[r];
        float dot = fpq_i8_f32_dot_neon(W + r * cols, x, cols);
        y[r] = dot * scale;
    }
}

void fpq_i8_f32_axpy_neon(const int8_t *w, float coeff, float *y, size_t n) {
    float32x4_t vc = vdupq_n_f32(coeff);
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        int8x16_t wi = vld1q_s8(w + i);
        int16x8_t lo = vmovl_s8(vget_low_s8(wi));
        int16x8_t hi = vmovl_s8(vget_high_s8(wi));
        vst1q_f32(y + i,
                  vfmaq_f32(vld1q_f32(y + i),
                            vcvtq_f32_s32(vmovl_s16(vget_low_s16(lo))),
                            vc));
        vst1q_f32(y + i + 4,
                  vfmaq_f32(vld1q_f32(y + i + 4),
                            vcvtq_f32_s32(vmovl_s16(vget_high_s16(lo))),
                            vc));
        vst1q_f32(y + i + 8,
                  vfmaq_f32(vld1q_f32(y + i + 8),
                            vcvtq_f32_s32(vmovl_s16(vget_low_s16(hi))),
                            vc));
        vst1q_f32(y + i + 12,
                  vfmaq_f32(vld1q_f32(y + i + 12),
                            vcvtq_f32_s32(vmovl_s16(vget_high_s16(hi))),
                            vc));
    }
    for (; i < n; i++) y[i] += (float)w[i] * coeff;
}

void fpq_residual_merge_neon(float *out, const float *residuals, size_t n) {
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        float32x4_t o = vld1q_f32(out + i);
        float32x4_t r = vld1q_f32(residuals + i);
        vst1q_f32(out + i, vaddq_f32(o, r));
    }
    for (; i < n; i++) out[i] += residuals[i];
}

void fpq_rmsnorm_neon(float *out, const float *x, const float *w,
                      size_t n, float eps) {
    float32x4_t ss = vdupq_n_f32(0.f);
    size_t i = 0;

    for (; i + 4 <= n; i += 4) {
        float32x4_t xi = vld1q_f32(x + i);
        ss = vfmaq_f32(ss, xi, xi);
    }

    float sum = vaddvq_f32(ss);
    for (; i < n; i++) sum += x[i] * x[i];

    float scale = 1.0f / sqrtf(sum / (float)n + eps);
    float32x4_t sc = vdupq_n_f32(scale);

    i = 0;
    for (; i + 4 <= n; i += 4) {
        float32x4_t xi = vld1q_f32(x + i);
        float32x4_t wi = vld1q_f32(w + i);
        vst1q_f32(out + i, vmulq_f32(vmulq_f32(xi, sc), wi));
    }
    for (; i < n; i++) out[i] = w[i] * (scale * x[i]);
}

void fpq_silu_mul_neon(float *gate, const float *up, size_t n) {
    size_t i = 0;
    float32x4_t one = vdupq_n_f32(1.0f);

    for (; i + 4 <= n; i += 4) {
        float32x4_t g = vld1q_f32(gate + i);
        float32x4_t u = vld1q_f32(up + i);

        /* sigmoid(g) = 1 / (1 + exp(-g)) */
        float32x4_t neg_g = vnegq_f32(g);
        float32x4_t exp_neg_g = vdupq_n_f32(0.f);
        for (int j = 0; j < 4; j++) {
            exp_neg_g[j] = expf(neg_g[j]);
        }
        float32x4_t sig = vdivq_f32(one, vaddq_f32(one, exp_neg_g));

        /* silu(g) = g * sig */
        float32x4_t silu = vmulq_f32(g, sig);
        vst1q_f32(gate + i, vmulq_f32(silu, u));
    }

    for (; i < n; i++) {
        float g = gate[i];
        float sig = 1.0f / (1.0f + expf(-g));
        gate[i] = g * sig * up[i];
    }
}

void fpq_rope_neon(float *head, size_t head_dim, int pos, float theta) {
    for (size_t i = 0; i < head_dim / 2; i++) {
        float freq = 1.0f / powf(theta, (float)(2 * i) / (float)head_dim);
        float angle = (float)pos * freq;
        float cos_a = cosf(angle);
        float sin_a = sinf(angle);
        float q0 = head[2 * i];
        float q1 = head[2 * i + 1];
        head[2 * i] = q0 * cos_a - q1 * sin_a;
        head[2 * i + 1] = q0 * sin_a + q1 * cos_a;
    }
}

void fpq_prefill_gemm_neon(const int8_t *W, const float *scales,
                           const float *X, float *Y,
                           size_t rows, size_t cols, size_t batch) {
    /* Batched matrix-matrix multiply for prefill */
    for (size_t b = 0; b < batch; b++) {
        fpq_i8_f32_gemv_neon(W, scales, X + b * cols, Y + b * rows, rows, cols);
    }
}

#else
/* Non-NEON fallback */
float fpq_i8_f32_dot_neon(const int8_t *w, const float *x, size_t n) {
    float acc = 0.f;
    for (size_t i = 0; i < n; i++) acc += (float)w[i] * x[i];
    return acc;
}

void fpq_i8_f32_gemv_neon(const int8_t *W, const float *scales,
                          const float *x, float *y,
                          size_t rows, size_t cols) {
    for (size_t r = 0; r < rows; r++) {
        float dot = fpq_i8_f32_dot_neon(W + r * cols, x, cols);
        y[r] = dot * scales[r];
    }
}

void fpq_i8_f32_axpy_neon(const int8_t *w, float coeff, float *y, size_t n) {
    for (size_t i = 0; i < n; i++) y[i] += (float)w[i] * coeff;
}

void fpq_residual_merge_neon(float *out, const float *residuals, size_t n) {
    for (size_t i = 0; i < n; i++) out[i] += residuals[i];
}

void fpq_rmsnorm_neon(float *out, const float *x, const float *w,
                      size_t n, float eps) {
    float sum = 0.f;
    for (size_t i = 0; i < n; i++) sum += x[i] * x[i];
    float scale = 1.0f / sqrtf(sum / (float)n + eps);
    for (size_t i = 0; i < n; i++) out[i] = w[i] * (scale * x[i]);
}

void fpq_silu_mul_neon(float *gate, const float *up, size_t n) {
    for (size_t i = 0; i < n; i++) {
        float g = gate[i];
        float sig = 1.0f / (1.0f + expf(-g));
        gate[i] = g * sig * up[i];
    }
}

void fpq_rope_neon(float *head, size_t head_dim, int pos, float theta) {
    for (size_t i = 0; i < head_dim / 2; i++) {
        float freq = 1.0f / powf(theta, (float)(2 * i) / (float)head_dim);
        float angle = (float)pos * freq;
        float cos_a = cosf(angle);
        float sin_a = sinf(angle);
        float q0 = head[2 * i];
        float q1 = head[2 * i + 1];
        head[2 * i] = q0 * cos_a - q1 * sin_a;
        head[2 * i + 1] = q0 * sin_a + q1 * cos_a;
    }
}

void fpq_prefill_gemm_neon(const int8_t *W, const float *scales,
                           const float *X, float *Y,
                           size_t rows, size_t cols, size_t batch) {
    for (size_t b = 0; b < batch; b++) {
        fpq_i8_f32_gemv_neon(W, scales, X + b * cols, Y + b * rows, rows, cols);
    }
}
#endif
