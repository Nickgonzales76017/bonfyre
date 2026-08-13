/*
 * simd_bp128.h — SIMD-BP128 batch integer decoder for FPQ E8 coordinates
 *
 * Replaces serial rANS decode for E8 INT7 coords with SIMD batch unpack.
 * Groups of 128 integers packed at their bit-width, decoded in one shot
 * using NEON (ARM) or SSE2/AVX2 (x86).
 *
 * Why: rANS is serial (byte-at-a-time state machine). For SLI inference
 * where we stream blocks, decode latency matters. BP128 decodes 128
 * integers at memory bandwidth — no serial dependency chain.
 *
 * E8 coords are INT7 (7-bit, range [-64, 64]). BP128 at 7 bits
 * packs 128 values into 112 bytes (vs 128 bytes for raw INT8).
 * The win is decode speed, not size.
 *
 * Integration: plug into fpq_native_read() as alternative to
 * rans_decode() + unpack_int7() when FPQ_FLAG_BP128 is set.
 */
#ifndef SIMD_BP128_H
#define SIMD_BP128_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Flag for BP128 encoding in .fpq native format */
#define FPQ_FLAG_BP128  0x10

/* ── Encoder ─────────────────────────────────────────────────── */

/* Pack 128 signed 7-bit integers into BP128 format.
 * Input:  128 int8_t values in [-64, 64] (biased to [0, 128] internally)
 * Output: packed bytes, returns number of bytes written.
 * bit_width is auto-detected from the actual range of values. */
size_t bp128_encode_block(const int8_t *in, uint8_t *out);

/* Pack N integers (must be multiple of 128).
 * Returns total bytes written. */
size_t bp128_encode(const int8_t *in, size_t n, uint8_t *out);

/* ── Decoder (SIMD-accelerated) ──────────────────────────────── */

/* Decode 128 signed integers from BP128 format.
 * Returns number of bytes consumed from input. */
size_t bp128_decode_block(const uint8_t *in, int8_t *out);

/* Decode N integers (must be multiple of 128).
 * Returns total bytes consumed. */
size_t bp128_decode(const uint8_t *in, size_t n, int8_t *out);

/* ── Benchmark ───────────────────────────────────────────────── */

/* Benchmark BP128 vs rANS decode for n_blocks of 256 E8 coords each.
 * Prints results to stderr. Returns BP128 throughput in GB/s. */
double bp128_bench_vs_rans(int n_blocks);

#ifdef __cplusplus
}
#endif

#endif /* SIMD_BP128_H */
