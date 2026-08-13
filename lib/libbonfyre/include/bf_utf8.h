/*
 * bf_utf8.h — SIMD-accelerated UTF-8 processing
 *
 * Features:
 *   - Validation (SSE4.2 PCMPISTRI / NEON / scalar fallback)
 *   - Codepoint iterator
 *   - Case folding (ASCII fast path + basic Latin/Greek/Cyrillic)
 *   - NFC normalization (canonical decomposition + reorder + compose)
 *
 * Critical for transcript text integrity — Whisper outputs can contain
 * malformed UTF-8, mixed encodings, and non-normalized forms.
 */

#ifndef BF_UTF8_H
#define BF_UTF8_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Validation ──────────────────────────── */

/*
 * Validate a UTF-8 string. Returns 1 if valid, 0 if invalid.
 * On invalid, *err_pos (if non-NULL) is set to the byte offset of
 * the first bad byte.
 */
int bf_utf8_valid(const char *data, size_t len, size_t *err_pos);

/* Convenience: NUL-terminated string */
int bf_utf8_valid_str(const char *str, size_t *err_pos);

/* ── Codepoint iteration ─────────────────── */

typedef struct {
    const uint8_t *ptr;
    const uint8_t *end;
} bf_utf8_iter_t;

/* Initialize an iterator over [data, data+len). */
void bf_utf8_iter_init(bf_utf8_iter_t *it, const char *data, size_t len);

/*
 * Decode the next codepoint. Returns the codepoint (0-0x10FFFF),
 * or -1 at end of string, or 0xFFFD on invalid sequence (advances 1 byte).
 */
int32_t bf_utf8_iter_next(bf_utf8_iter_t *it);

/* Peek without advancing. */
int32_t bf_utf8_iter_peek(const bf_utf8_iter_t *it);

/* ── Encoding ────────────────────────────── */

/*
 * Encode a single codepoint to UTF-8.
 * `buf` must have room for at least 4 bytes.
 * Returns the number of bytes written (1-4), or 0 on invalid codepoint.
 */
int bf_utf8_encode(uint8_t *buf, int32_t cp);

/* ── String metrics ──────────────────────── */

/* Count UTF-8 codepoints in [data, data+len). Invalid bytes count as 1 each. */
size_t bf_utf8_cp_count(const char *data, size_t len);

/* Byte length of the codepoint starting at `data`. Returns 1-4, or 1 on invalid. */
int bf_utf8_cp_len(const uint8_t *data);

/* ── Case folding ────────────────────────── */

/*
 * Fold `src` to lowercase into `dst`. `dst_cap` is the buffer size.
 * Returns bytes written (excluding NUL terminator), or required size
 * if dst_cap is too small.
 * Handles ASCII + Latin-1 Supplement + basic Latin Extended + Greek + Cyrillic.
 */
size_t bf_utf8_fold(char *dst, size_t dst_cap, const char *src, size_t src_len);

/* ── Sanitization ────────────────────────── */

/*
 * Replace invalid UTF-8 sequences with U+FFFD (3 bytes each).
 * Returns bytes written. `dst` must be at least src_len*3 bytes
 * (worst case: every byte is invalid).
 */
size_t bf_utf8_sanitize(char *dst, size_t dst_cap,
                        const char *src, size_t src_len);

/*
 * Strip NUL bytes and C0 control chars (except \\n \\r \\t) in-place.
 * Returns the new length.
 */
size_t bf_utf8_strip_control(char *data, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* BF_UTF8_H */
