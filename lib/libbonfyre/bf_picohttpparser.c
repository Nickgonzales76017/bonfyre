/*
 * bf_picohttpparser.c — Embedded PicoHTTPParser with SIMD acceleration
 *
 * Based on picohttpparser by Kazuho Oku (H2O project), MIT License.
 * Adapted for Bonfyre: stripped content-length auto-parse,
 * added ARM NEON path alongside SSE4.2.
 *
 * Key optimization: uses SIMD to scan for delimiters (\r\n, :, space)
 * across 16 bytes at a time instead of byte-by-byte.
 */

#include "bf_picohttpparser.h"

#include <string.h>
#include <stdint.h>

/* ── SIMD detection ──────────────────────────────────────────── */

#if defined(__SSE4_2__) && defined(__x86_64__)
#define PHR_USE_SSE42 1
#include <x86intrin.h>
#elif defined(__aarch64__)
#define PHR_USE_NEON 1
#include <arm_neon.h>
#endif

/* ── Token tables ────────────────────────────────────────────── */

/* Characters valid in HTTP tokens (RFC 7230 §3.2.6) */
static const char token_char_map[256] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  /* 0x00-0x0F */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  /* 0x10-0x1F */
    0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 1, 1, 0,  /* 0x20-0x2F */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0,  /* 0x30-0x3F */
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  /* 0x40-0x4F */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1,  /* 0x50-0x5F */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  /* 0x60-0x6F */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 0,  /* 0x70-0x7F */
};

static inline int is_token(char c) {
    return (unsigned char)c < 128 && token_char_map[(unsigned char)c];
}

/* ── SIMD scanning helpers ───────────────────────────────────── */

#ifdef PHR_USE_SSE42

/* Find first occurrence of any char in `ranges` using SSE4.2 PCMPESTRI */
static inline size_t find_char_sse42(const char *buf, size_t len,
                                       const char *ranges, int ranges_len) {
    __m128i r = _mm_loadu_si128((const __m128i *)ranges);
    size_t i = 0;
    for (; i + 16 <= len; i += 16) {
        __m128i b = _mm_loadu_si128((const __m128i *)(buf + i));
        int idx = _mm_cmpestri(r, ranges_len, b, 16,
                                _SIDD_UBYTE_OPS | _SIDD_CMP_RANGES |
                                _SIDD_LEAST_SIGNIFICANT);
        if (idx != 16) return i + (size_t)idx;
    }
    /* Scalar tail */
    for (; i < len; i++) {
        unsigned char c = (unsigned char)buf[i];
        for (int j = 0; j < ranges_len; j += 2) {
            if (c >= (unsigned char)ranges[j] && c <= (unsigned char)ranges[j+1])
                return i;
        }
    }
    return len;
}

#elif defined(PHR_USE_NEON)

static inline size_t find_cr_or_lf_neon(const char *buf, size_t len) {
    uint8x16_t cr = vdupq_n_u8('\r');
    uint8x16_t lf = vdupq_n_u8('\n');
    size_t i = 0;
    for (; i + 16 <= len; i += 16) {
        uint8x16_t v = vld1q_u8((const uint8_t *)(buf + i));
        uint8x16_t eq_cr = vceqq_u8(v, cr);
        uint8x16_t eq_lf = vceqq_u8(v, lf);
        uint8x16_t eq = vorrq_u8(eq_cr, eq_lf);
        /* Check if any match */
        uint64x2_t eq64 = vreinterpretq_u64_u8(eq);
        if (vgetq_lane_u64(eq64, 0) | vgetq_lane_u64(eq64, 1)) {
            for (size_t j = 0; j < 16 && i + j < len; j++) {
                if (buf[i + j] == '\r' || buf[i + j] == '\n') return i + j;
            }
        }
    }
    for (; i < len; i++) {
        if (buf[i] == '\r' || buf[i] == '\n') return i;
    }
    return len;
}

#endif

/* ── Scalar helpers ──────────────────────────────────────────── */

static inline const char *find_crlf(const char *buf, size_t len) {
#ifdef PHR_USE_SSE42
    static const char ranges[] = "\r\r\n\n";
    size_t pos = find_char_sse42(buf, len, ranges, 4);
    if (pos < len && buf[pos] == '\r' && pos + 1 < len && buf[pos + 1] == '\n')
        return buf + pos;
    /* Might have found \n first, scan forward */
    for (size_t i = pos; i + 1 < len; i++) {
        if (buf[i] == '\r' && buf[i + 1] == '\n') return buf + i;
    }
    return NULL;
#elif defined(PHR_USE_NEON)
    size_t pos = find_cr_or_lf_neon(buf, len);
    for (size_t i = pos; i + 1 < len; i++) {
        if (buf[i] == '\r' && buf[i + 1] == '\n') return buf + i;
    }
    return NULL;
#else
    return (const char *)memmem(buf, len, "\r\n", 2);
#endif
}

static inline const char *find_ch(const char *buf, size_t len, char target) {
    return (const char *)memchr(buf, target, len);
}

/* ── Request parser ──────────────────────────────────────────── */

int phr_parse_request(const char *buf, size_t len,
                       const char **method, size_t *method_len,
                       const char **path,   size_t *path_len,
                       int *minor_version,
                       struct phr_header *headers, size_t *num_headers,
                       size_t last_len) {
    const char *p = buf + (last_len > 3 ? last_len - 3 : 0);
    const char *end = buf + len;
    size_t max_headers = *num_headers;
    *num_headers = 0;

    /* Find end of request line */
    const char *line_end = find_crlf(p, (size_t)(end - p));
    if (!line_end) return -2; /* Incomplete */

    p = buf; /* Reset to start for parsing */

    /* Method */
    const char *sp = find_ch(p, (size_t)(line_end - p), ' ');
    if (!sp) return -1;
    *method = p;
    *method_len = (size_t)(sp - p);

    /* Path */
    p = sp + 1;
    sp = find_ch(p, (size_t)(line_end - p), ' ');
    if (!sp) return -1;
    *path = p;
    *path_len = (size_t)(sp - p);

    /* Version: HTTP/1.x */
    p = sp + 1;
    if (line_end - p < 8 || memcmp(p, "HTTP/1.", 7) != 0)
        return -1;
    *minor_version = p[7] - '0';

    p = line_end + 2; /* Skip \r\n */

    /* Headers */
    while (p < end && *num_headers < max_headers) {
        /* Empty line = end of headers */
        if (p + 1 < end && p[0] == '\r' && p[1] == '\n') {
            p += 2;
            return (int)(p - buf);
        }

        /* Header name */
        const char *colon = find_ch(p, (size_t)(end - p), ':');
        if (!colon) return -2;

        headers[*num_headers].name = p;
        headers[*num_headers].name_len = (size_t)(colon - p);

        /* Skip OWS after colon */
        p = colon + 1;
        while (p < end && (*p == ' ' || *p == '\t')) p++;

        /* Header value (until CRLF) */
        const char *val_end = find_crlf(p, (size_t)(end - p));
        if (!val_end) return -2;

        headers[*num_headers].value = p;
        headers[*num_headers].value_len = (size_t)(val_end - p);

        /* Trim trailing OWS */
        while (headers[*num_headers].value_len > 0) {
            char c = headers[*num_headers].value[headers[*num_headers].value_len - 1];
            if (c == ' ' || c == '\t') headers[*num_headers].value_len--;
            else break;
        }

        (*num_headers)++;
        p = val_end + 2;
    }

    return -2; /* Headers incomplete or too many */
}

/* ── Response parser ─────────────────────────────────────────── */

int phr_parse_response(const char *buf, size_t len,
                        int *minor_version,
                        int *status,
                        const char **msg, size_t *msg_len,
                        struct phr_header *headers, size_t *num_headers,
                        size_t last_len) {
    const char *p = buf;
    const char *end = buf + len;
    size_t max_headers = *num_headers;
    *num_headers = 0;
    (void)last_len;

    /* Status line: HTTP/1.x SSS Reason\r\n */
    const char *line_end = find_crlf(p, (size_t)(end - p));
    if (!line_end) return -2;

    if (line_end - p < 12 || memcmp(p, "HTTP/1.", 7) != 0)
        return -1;
    *minor_version = p[7] - '0';

    /* Status code */
    if (p[8] != ' ') return -1;
    *status = (p[9] - '0') * 100 + (p[10] - '0') * 10 + (p[11] - '0');

    /* Reason phrase */
    if (p[12] == ' ') {
        *msg = p + 13;
        *msg_len = (size_t)(line_end - p - 13);
    } else {
        *msg = "";
        *msg_len = 0;
    }

    p = line_end + 2;

    /* Headers (same as request) */
    while (p < end && *num_headers < max_headers) {
        if (p + 1 < end && p[0] == '\r' && p[1] == '\n') {
            p += 2;
            return (int)(p - buf);
        }

        const char *colon = find_ch(p, (size_t)(end - p), ':');
        if (!colon) return -2;

        headers[*num_headers].name = p;
        headers[*num_headers].name_len = (size_t)(colon - p);

        p = colon + 1;
        while (p < end && (*p == ' ' || *p == '\t')) p++;

        const char *val_end = find_crlf(p, (size_t)(end - p));
        if (!val_end) return -2;

        headers[*num_headers].value = p;
        headers[*num_headers].value_len = (size_t)(val_end - p);

        while (headers[*num_headers].value_len > 0) {
            char c = headers[*num_headers].value[headers[*num_headers].value_len - 1];
            if (c == ' ' || c == '\t') headers[*num_headers].value_len--;
            else break;
        }

        (*num_headers)++;
        p = val_end + 2;
    }

    return -2;
}

/* ── Headers-only parser ─────────────────────────────────────── */

int phr_parse_headers(const char *buf, size_t len,
                       struct phr_header *headers, size_t *num_headers,
                       size_t last_len) {
    const char *p = buf;
    const char *end = buf + len;
    size_t max_headers = *num_headers;
    *num_headers = 0;
    (void)last_len;

    while (p < end && *num_headers < max_headers) {
        if (p + 1 < end && p[0] == '\r' && p[1] == '\n') {
            p += 2;
            return (int)(p - buf);
        }

        const char *colon = find_ch(p, (size_t)(end - p), ':');
        if (!colon) return -2;

        headers[*num_headers].name = p;
        headers[*num_headers].name_len = (size_t)(colon - p);

        p = colon + 1;
        while (p < end && (*p == ' ' || *p == '\t')) p++;

        const char *val_end = find_crlf(p, (size_t)(end - p));
        if (!val_end) return -2;

        headers[*num_headers].value = p;
        headers[*num_headers].value_len = (size_t)(val_end - p);

        while (headers[*num_headers].value_len > 0) {
            char c = headers[*num_headers].value[headers[*num_headers].value_len - 1];
            if (c == ' ' || c == '\t') headers[*num_headers].value_len--;
            else break;
        }

        (*num_headers)++;
        p = val_end + 2;
    }

    return -2;
}
