/*
 * fpq_kernels_neon.h — Fused NEON kernels for Qwen inference
 *
 * Optimized SIMD implementations for Apple Silicon / ARM NEON.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Fused dequant + dot product: int8 weights * float32 input */
float fpq_i8_f32_dot_neon(const int8_t *w, const float *x, size_t n);

/* Fused dequant + GEMV: y = W*x (int8 weights, float32 input/output) */
void fpq_i8_f32_gemv_neon(const int8_t *W, const float *scales,
                          const float *x, float *y,
                          size_t rows, size_t cols);

/* AXPY-style scatter: y += coeff * int8_vector */
void fpq_i8_f32_axpy_neon(const int8_t *w, float coeff, float *y, size_t n);

/* Merge residual corrections into output */
void fpq_residual_merge_neon(float *out, const float *residuals, size_t n);

/* RMS normalization */
void fpq_rmsnorm_neon(float *out, const float *x, const float *w,
                      size_t n, float eps);

/* SiLU activation with Hadamard product: gate = silu(gate) * up */
void fpq_silu_mul_neon(float *gate, const float *up, size_t n);

/* RoPE rotation */
void fpq_rope_neon(float *head, size_t head_dim, int pos, float theta);

/* Batched prefill: matrix-matrix multiply for prompt processing */
void fpq_prefill_gemm_neon(const int8_t *W, const float *scales,
                           const float *X, float *Y,
                           size_t rows, size_t cols, size_t batch);

#ifdef __cplusplus
}
#endif
