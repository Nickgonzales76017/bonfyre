/*
 * bf_lz4.h — In-process LZ4 block compression
 *
 * Self-contained LZ4 implementation (block format).
 * Replaces fork+exec of external compression tools.
 *
 * Features:
 *   - Single-shot compress / decompress
 *   - Streaming encoder / decoder
 *   - Bounded output size calculation
 */

#ifndef BF_LZ4_H
#define BF_LZ4_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Single-shot API ─────────────────────── */

/* Maximum compressed size for `src_len` bytes of input. */
size_t bf_lz4_bound(size_t src_len);

/*
 * Compress `src` (src_len bytes) into `dst` (dst_cap bytes).
 * Returns compressed size, or 0 on failure (dst_cap too small).
 */
size_t bf_lz4_compress(const void *src, size_t src_len,
                       void *dst, size_t dst_cap);

/*
 * Decompress `src` (src_len bytes) into `dst` (dst_cap bytes).
 * `original_size` must be the exact uncompressed size (known from framing).
 * Returns bytes written, or 0 on corruption / overflow.
 */
size_t bf_lz4_decompress(const void *src, size_t src_len,
                         void *dst, size_t original_size);

/* ── Streaming compression ───────────────── */

typedef struct bf_lz4_stream bf_lz4_stream_t;

bf_lz4_stream_t *bf_lz4_stream_new(void);
void             bf_lz4_stream_free(bf_lz4_stream_t *s);

/*
 * Feed a block to the streaming compressor.
 * `src` must remain valid until the next call or stream_free.
 * Returns compressed size written to `dst`, or 0 on error.
 */
size_t bf_lz4_stream_compress(bf_lz4_stream_t *s,
                              const void *src, size_t src_len,
                              void *dst, size_t dst_cap);

/* Reset streaming state (start a new independent stream). */
void bf_lz4_stream_reset(bf_lz4_stream_t *s);

#ifdef __cplusplus
}
#endif

#endif /* BF_LZ4_H */
