/*
 * bf_picohttpparser.h — Embedded PicoHTTPParser (SIMD-accelerated HTTP/1.x)
 *
 * Originally from H2O project (MIT License).
 * Embedded here to avoid external dependency.
 *
 * SIMD acceleration:
 *   - SSE4.2 on x86_64 (PCMPESTRI for header scanning)
 *   - NEON on ARM64 (vtbl for range checks)
 *   - Scalar fallback for other architectures
 *
 * Performance: ~2.5 GB/s HTTP parse throughput (vs ~200 MB/s hand-rolled)
 */

#ifndef BF_PICOHTTPPARSER_H
#define BF_PICOHTTPPARSER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Parsed header ───────────────────────────────────────────── */

struct phr_header {
    const char *name;
    size_t      name_len;
    const char *value;
    size_t      value_len;
};

/* ── Parse functions ─────────────────────────────────────────── */

/*
 * Parse an HTTP request.
 *
 * Returns: number of bytes consumed, or
 *   -1 on error
 *   -2 if request is incomplete (need more data)
 *
 * `method`, `path` point into `buf` (zero-copy).
 * `num_headers` is input/output: pass max capacity, receives actual count.
 * `last_len`: bytes parsed in previous call (for incremental parsing), 0 for first call.
 */
int phr_parse_request(const char *buf, size_t len,
                       const char **method, size_t *method_len,
                       const char **path,   size_t *path_len,
                       int *minor_version,
                       struct phr_header *headers, size_t *num_headers,
                       size_t last_len);

/*
 * Parse an HTTP response.
 */
int phr_parse_response(const char *buf, size_t len,
                        int *minor_version,
                        int *status,
                        const char **msg, size_t *msg_len,
                        struct phr_header *headers, size_t *num_headers,
                        size_t last_len);

/*
 * Parse headers only (for trailers, etc.)
 */
int phr_parse_headers(const char *buf, size_t len,
                       struct phr_header *headers, size_t *num_headers,
                       size_t last_len);

#ifdef __cplusplus
}
#endif

#endif /* BF_PICOHTTPPARSER_H */
