/*
 * bf_lz4.c — Self-contained LZ4 block compression
 *
 * Implements the LZ4 block format (compatible with the reference decoder).
 * Hash-table-based greedy match finder, 4-byte minimum match length.
 *
 * Reference: https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md
 */

#include "bf_lz4.h"

#include <stdlib.h>
#include <string.h>

/* ── Constants ───────────────────────────── */

#define LZ4_MIN_MATCH      4
#define LZ4_HASH_LOG       16
#define LZ4_HASH_SIZE      (1 << LZ4_HASH_LOG)
#define LZ4_SKIP_TRIGGER   6
#define LZ4_MAX_INPUT_SIZE  0x7E000000  /* ~2 GB */
#define LZ4_LAST_LITERALS  5
#define LZ4_MF_LIMIT       12   /* min bytes to keep for last-literal safety */
#define ML_BITS            4
#define ML_MASK            ((1 << ML_BITS) - 1)
#define RUN_BITS           (8 - ML_BITS)
#define RUN_MASK           ((1 << RUN_BITS) - 1)

/* ── Portability helpers ─────────────────── */

static inline uint32_t read32(const void *p)
{
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}

static inline uint16_t read16(const void *p)
{
    uint16_t v;
    memcpy(&v, p, 2);
    return v;
}

static inline void write16(void *p, uint16_t v)
{
    memcpy(p, &v, 2);
}

/* Count bytes that match between `p` and `q`, up to `max` bytes. */
static size_t match_length(const uint8_t *p, const uint8_t *q, const uint8_t *limit)
{
    const uint8_t *start = p;
    while (p < limit - 7) {
        uint64_t diff;
        uint64_t v1, v2;
        memcpy(&v1, p, 8);
        memcpy(&v2, q, 8);
        diff = v1 ^ v2;
        if (diff) {
            /* find first differing byte — little-endian CTZ */
#if defined(__GNUC__) || defined(__clang__)
            p += __builtin_ctzll(diff) >> 3;
#else
            /* portable fallback */
            for (int i = 0; i < 8; i++) {
                if (((diff >> (i * 8)) & 0xFF) != 0) { p += i; break; }
            }
#endif
            return (size_t)(p - start);
        }
        p += 8; q += 8;
    }
    while (p < limit && *p == *q) { p++; q++; }
    return (size_t)(p - start);
}

/* ── Hash function ───────────────────────── */

static inline uint32_t lz4_hash(uint32_t v)
{
    return (v * 2654435761U) >> (32 - LZ4_HASH_LOG);
}

/* ── Bound ───────────────────────────────── */

size_t bf_lz4_bound(size_t src_len)
{
    if (src_len > LZ4_MAX_INPUT_SIZE) return 0;
    return src_len + (src_len / 255) + 16;
}

/* ── Write variable-length count ─────────── */

static uint8_t *write_count(uint8_t *dst, size_t count)
{
    while (count >= 255) {
        *dst++ = 255;
        count -= 255;
    }
    *dst++ = (uint8_t)count;
    return dst;
}

/* ── Compress ────────────────────────────── */

size_t bf_lz4_compress(const void *src, size_t src_len,
                       void *dst, size_t dst_cap)
{
    if (!src || !dst || src_len == 0) return 0;
    if (src_len > LZ4_MAX_INPUT_SIZE) return 0;
    if (dst_cap < bf_lz4_bound(src_len)) {
        /* might still fit, keep going — but watch bounds below */
    }

    const uint8_t *ip   = (const uint8_t *)src;
    const uint8_t *base = ip;
    const uint8_t *iend = ip + src_len;
    const uint8_t *mf_limit = iend - LZ4_MF_LIMIT;
    const uint8_t *match_limit = iend - LZ4_LAST_LITERALS;
    const uint8_t *anchor = ip;

    uint8_t *op    = (uint8_t *)dst;
    uint8_t *olimit = op + dst_cap;

    uint32_t htable[LZ4_HASH_SIZE];
    memset(htable, 0, sizeof(htable));

    if (src_len < LZ4_MF_LIMIT) goto _last_literals;

    ip++; /* first byte can't be a back-ref anchor */

    /* main loop */
    for (;;) {
        const uint8_t *match;
        uint8_t *token;

        /* find a match */
        {
            const uint8_t *forwardIp = ip;
            unsigned step = 1;
            unsigned search_limit = 1 << LZ4_SKIP_TRIGGER;

            do {
                uint32_t h = lz4_hash(read32(forwardIp));
                ip = forwardIp;
                forwardIp += step++;
                if (step > search_limit) step = search_limit;

                if (forwardIp > mf_limit) goto _last_literals;

                match = base + htable[h];
                htable[h] = (uint32_t)(ip - base);
            } while (read32(match) != read32(ip) ||
                     match + 0xFFFF < ip);  /* offset must fit in 16 bits */
        }

        /* emit literals since anchor */
        {
            size_t lit_len = (size_t)(ip - anchor);
            if (op + 1 + lit_len + (lit_len >= 15 ? lit_len/255 + 1 : 0) + 2 > olimit)
                return 0;

            token = op++;
            if (lit_len >= 15) {
                *token = (15 << ML_BITS);
                op = write_count(op, lit_len - 15);
            } else {
                *token = (uint8_t)(lit_len << ML_BITS);
            }
            memcpy(op, anchor, lit_len);
            op += lit_len;
        }

_next_match:
        /* emit offset */
        {
            uint16_t offset = (uint16_t)(ip - match);
            if (op + 2 > olimit) return 0;
            write16(op, offset);
            op += 2;
        }

        /* count match length */
        {
            size_t ml = match_length(ip + LZ4_MIN_MATCH,
                                     match + LZ4_MIN_MATCH,
                                     match_limit) + LZ4_MIN_MATCH;
            if (op + (ml >= 19 ? (ml - 19)/255 + 1 : 0) + 1 > olimit)
                return 0;

            if (ml >= 19) {
                *token |= (uint8_t)ML_MASK;
                op = write_count(op, ml - 19);
            } else {
                *token |= (uint8_t)(ml - LZ4_MIN_MATCH);
            }
            ip += ml;
        }

        anchor = ip;
        if (ip > mf_limit) goto _last_literals;

        /* test next position */
        {
            uint32_t h = lz4_hash(read32(ip - 2));
            htable[h] = (uint32_t)(ip - 2 - base);

            h = lz4_hash(read32(ip));
            match = base + htable[h];
            htable[h] = (uint32_t)(ip - base);

            if (read32(match) == read32(ip) && match + 0xFFFF >= ip) {
                token = op++;
                *token = 0;
                goto _next_match;
            }
        }

        ip++;
    }

_last_literals:
    {
        size_t last = (size_t)(iend - anchor);
        if (op + 1 + last + (last >= 15 ? last/255 + 1 : 0) > olimit)
            return 0;

        uint8_t *token = op++;
        if (last >= 15) {
            *token = (uint8_t)(15 << ML_BITS);
            op = write_count(op, last - 15);
        } else {
            *token = (uint8_t)(last << ML_BITS);
        }
        memcpy(op, anchor, last);
        op += last;
    }

    return (size_t)(op - (uint8_t *)dst);
}

/* ── Decompress ──────────────────────────── */

size_t bf_lz4_decompress(const void *src, size_t src_len,
                         void *dst, size_t original_size)
{
    if (!src || !dst || original_size == 0) return 0;

    const uint8_t *ip   = (const uint8_t *)src;
    const uint8_t *iend = ip + src_len;

    uint8_t *op    = (uint8_t *)dst;
    uint8_t *oend  = op + original_size;

    while (1) {
        if (ip >= iend) return 0;

        /* token */
        uint8_t tok = *ip++;
        size_t lit_len = tok >> ML_BITS;

        /* literal length */
        if (lit_len == 15) {
            uint8_t s;
            do {
                if (ip >= iend) return 0;
                s = *ip++;
                lit_len += s;
            } while (s == 255);
        }

        /* copy literals */
        if (op + lit_len > oend) return 0;
        if (ip + lit_len > iend) return 0;
        memcpy(op, ip, lit_len);
        op += lit_len;
        ip += lit_len;

        if (op >= oend) break;  /* done — all output consumed */

        /* match offset */
        if (ip + 2 > iend) return 0;
        size_t offset = read16(ip);
        ip += 2;
        if (offset == 0) return 0;  /* invalid offset */
        uint8_t *match = op - offset;
        if (match < (uint8_t *)dst) return 0;  /* underflow */

        /* match length */
        size_t ml = (tok & ML_MASK) + LZ4_MIN_MATCH;
        if (ml == LZ4_MIN_MATCH + ML_MASK) {
            uint8_t s;
            do {
                if (ip >= iend) return 0;
                s = *ip++;
                ml += s;
            } while (s == 255);
        }

        if (op + ml > oend) return 0;

        /* copy match — may overlap (offset < ml) */
        if (offset >= 8) {
            /* fast non-overlapping copy */
            uint8_t *cp_end = op + ml;
            while (op < cp_end) {
                size_t chunk = (size_t)(cp_end - op);
                if (chunk > offset) chunk = offset;
                memcpy(op, match, chunk);
                op += chunk;
            }
        } else {
            /* byte-by-byte for small overlapping offsets */
            for (size_t i = 0; i < ml; i++)
                op[i] = match[i];
            op += ml;
        }
    }

    return (size_t)(op - (uint8_t *)dst);
}

/* ── Streaming compression ───────────────── */

struct bf_lz4_stream {
    uint32_t htable[LZ4_HASH_SIZE];
    const uint8_t *prev_block;
    size_t         prev_len;
    uint8_t       dict[64 * 1024];  /* rolling dictionary */
    size_t         dict_len;
};

bf_lz4_stream_t *bf_lz4_stream_new(void)
{
    bf_lz4_stream_t *s = calloc(1, sizeof(*s));
    return s;
}

void bf_lz4_stream_free(bf_lz4_stream_t *s)
{
    free(s);
}

void bf_lz4_stream_reset(bf_lz4_stream_t *s)
{
    if (!s) return;
    memset(s->htable, 0, sizeof(s->htable));
    s->prev_block = NULL;
    s->prev_len   = 0;
    s->dict_len   = 0;
}

size_t bf_lz4_stream_compress(bf_lz4_stream_t *s,
                              const void *src, size_t src_len,
                              void *dst, size_t dst_cap)
{
    if (!s) return 0;

    /* For streaming, we save the dictionary window from previous blocks.
     * For simplicity, fall back to independent block compression
     * while maintaining the dictionary. */
    size_t result = bf_lz4_compress(src, src_len, dst, dst_cap);

    /* Update dict: keep last 64KB for future blocks */
    if (src_len <= sizeof(s->dict)) {
        memcpy(s->dict, src, src_len);
        s->dict_len = src_len;
    } else {
        memcpy(s->dict, (const uint8_t *)src + src_len - sizeof(s->dict),
               sizeof(s->dict));
        s->dict_len = sizeof(s->dict);
    }
    s->prev_block = s->dict;
    s->prev_len = s->dict_len;

    return result;
}
