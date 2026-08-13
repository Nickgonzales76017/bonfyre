/*
 * bf_msgpack.c — Minimal MessagePack encoder/decoder
 *
 * Implements the MessagePack specification (https://msgpack.org/):
 *   - fixint, int8/16/32/64, uint8/16/32/64
 *   - nil, true, false
 *   - float32, float64
 *   - fixstr, str8/16/32
 *   - bin8/16/32
 *   - fixarray, array16/32
 *   - fixmap, map16/32
 */

#include "bf_msgpack.h"

#include <stdlib.h>
#include <string.h>

/* ── Encoder helpers ─────────────────────────────────────────── */

static int ensure(bf_mp_writer_t *w, size_t need) {
    if (w->len + need <= w->cap) return 0;
    size_t newcap = w->cap * 2;
    if (newcap < w->len + need) newcap = w->len + need;
    if (newcap < 256) newcap = 256;
    uint8_t *nb = realloc(w->buf, newcap);
    if (!nb) return -1;
    w->buf = nb;
    w->cap = newcap;
    return 0;
}

static inline void w8(bf_mp_writer_t *w, uint8_t v) {
    w->buf[w->len++] = v;
}

static inline void w16(bf_mp_writer_t *w, uint16_t v) {
    w->buf[w->len++] = (uint8_t)(v >> 8);
    w->buf[w->len++] = (uint8_t)(v);
}

static inline void w32(bf_mp_writer_t *w, uint32_t v) {
    w->buf[w->len++] = (uint8_t)(v >> 24);
    w->buf[w->len++] = (uint8_t)(v >> 16);
    w->buf[w->len++] = (uint8_t)(v >> 8);
    w->buf[w->len++] = (uint8_t)(v);
}

static inline void w64(bf_mp_writer_t *w, uint64_t v) {
    w32(w, (uint32_t)(v >> 32));
    w32(w, (uint32_t)(v));
}

/* ── Writer lifecycle ────────────────────────────────────────── */

void bf_mp_writer_init(bf_mp_writer_t *w, size_t initial_cap) {
    if (initial_cap == 0) initial_cap = 256;
    w->buf = malloc(initial_cap);
    w->len = 0;
    w->cap = w->buf ? initial_cap : 0;
}

void bf_mp_writer_free(bf_mp_writer_t *w) {
    free(w->buf);
    w->buf = NULL;
    w->len = 0;
    w->cap = 0;
}

void bf_mp_writer_reset(bf_mp_writer_t *w) {
    w->len = 0;
}

const uint8_t *bf_mp_writer_data(const bf_mp_writer_t *w) {
    return w->buf;
}

size_t bf_mp_writer_size(const bf_mp_writer_t *w) {
    return w->len;
}

/* ── Pack: nil / bool ────────────────────────────────────────── */

int bf_mp_pack_nil(bf_mp_writer_t *w) {
    if (ensure(w, 1)) return -1;
    w8(w, 0xc0);
    return 0;
}

int bf_mp_pack_bool(bf_mp_writer_t *w, int val) {
    if (ensure(w, 1)) return -1;
    w8(w, val ? 0xc3 : 0xc2);
    return 0;
}

/* ── Pack: integers ──────────────────────────────────────────── */

int bf_mp_pack_uint(bf_mp_writer_t *w, uint64_t val) {
    if (val <= 0x7f) {
        if (ensure(w, 1)) return -1;
        w8(w, (uint8_t)val);
    } else if (val <= 0xff) {
        if (ensure(w, 2)) return -1;
        w8(w, 0xcc); w8(w, (uint8_t)val);
    } else if (val <= 0xffff) {
        if (ensure(w, 3)) return -1;
        w8(w, 0xcd); w16(w, (uint16_t)val);
    } else if (val <= 0xffffffff) {
        if (ensure(w, 5)) return -1;
        w8(w, 0xce); w32(w, (uint32_t)val);
    } else {
        if (ensure(w, 9)) return -1;
        w8(w, 0xcf); w64(w, val);
    }
    return 0;
}

int bf_mp_pack_int(bf_mp_writer_t *w, int64_t val) {
    if (val >= 0) return bf_mp_pack_uint(w, (uint64_t)val);

    if (val >= -32) {
        if (ensure(w, 1)) return -1;
        w8(w, (uint8_t)(int8_t)val);
    } else if (val >= -128) {
        if (ensure(w, 2)) return -1;
        w8(w, 0xd0); w8(w, (uint8_t)(int8_t)val);
    } else if (val >= -32768) {
        if (ensure(w, 3)) return -1;
        w8(w, 0xd1); w16(w, (uint16_t)(int16_t)val);
    } else if (val >= -2147483648LL) {
        if (ensure(w, 5)) return -1;
        w8(w, 0xd2); w32(w, (uint32_t)(int32_t)val);
    } else {
        if (ensure(w, 9)) return -1;
        w8(w, 0xd3); w64(w, (uint64_t)val);
    }
    return 0;
}

/* ── Pack: float / double ────────────────────────────────────── */

int bf_mp_pack_float(bf_mp_writer_t *w, float val) {
    if (ensure(w, 5)) return -1;
    w8(w, 0xca);
    uint32_t u;
    memcpy(&u, &val, 4);
    w32(w, u);
    return 0;
}

int bf_mp_pack_double(bf_mp_writer_t *w, double val) {
    if (ensure(w, 9)) return -1;
    w8(w, 0xcb);
    uint64_t u;
    memcpy(&u, &val, 8);
    w64(w, u);
    return 0;
}

/* ── Pack: string ────────────────────────────────────────────── */

int bf_mp_pack_str(bf_mp_writer_t *w, const char *s, uint32_t len) {
    if (len <= 31) {
        if (ensure(w, 1 + len)) return -1;
        w8(w, (uint8_t)(0xa0 | len));
    } else if (len <= 0xff) {
        if (ensure(w, 2 + len)) return -1;
        w8(w, 0xd9); w8(w, (uint8_t)len);
    } else if (len <= 0xffff) {
        if (ensure(w, 3 + len)) return -1;
        w8(w, 0xda); w16(w, (uint16_t)len);
    } else {
        if (ensure(w, 5 + len)) return -1;
        w8(w, 0xdb); w32(w, len);
    }
    memcpy(w->buf + w->len, s, len);
    w->len += len;
    return 0;
}

int bf_mp_pack_str_cstr(bf_mp_writer_t *w, const char *s) {
    return bf_mp_pack_str(w, s, s ? (uint32_t)strlen(s) : 0);
}

/* ── Pack: binary ────────────────────────────────────────────── */

int bf_mp_pack_bin(bf_mp_writer_t *w, const void *data, uint32_t len) {
    if (len <= 0xff) {
        if (ensure(w, 2 + len)) return -1;
        w8(w, 0xc4); w8(w, (uint8_t)len);
    } else if (len <= 0xffff) {
        if (ensure(w, 3 + len)) return -1;
        w8(w, 0xc5); w16(w, (uint16_t)len);
    } else {
        if (ensure(w, 5 + len)) return -1;
        w8(w, 0xc6); w32(w, len);
    }
    memcpy(w->buf + w->len, data, len);
    w->len += len;
    return 0;
}

/* ── Pack: containers ────────────────────────────────────────── */

int bf_mp_pack_array(bf_mp_writer_t *w, uint32_t count) {
    if (count <= 15) {
        if (ensure(w, 1)) return -1;
        w8(w, (uint8_t)(0x90 | count));
    } else if (count <= 0xffff) {
        if (ensure(w, 3)) return -1;
        w8(w, 0xdc); w16(w, (uint16_t)count);
    } else {
        if (ensure(w, 5)) return -1;
        w8(w, 0xdd); w32(w, count);
    }
    return 0;
}

int bf_mp_pack_map(bf_mp_writer_t *w, uint32_t count) {
    if (count <= 15) {
        if (ensure(w, 1)) return -1;
        w8(w, (uint8_t)(0x80 | count));
    } else if (count <= 0xffff) {
        if (ensure(w, 3)) return -1;
        w8(w, 0xde); w16(w, (uint16_t)count);
    } else {
        if (ensure(w, 5)) return -1;
        w8(w, 0xdf); w32(w, count);
    }
    return 0;
}

/* ── Pack: key-value convenience ─────────────────────────────── */

int bf_mp_pack_kv_str(bf_mp_writer_t *w, const char *key, const char *val) {
    if (bf_mp_pack_str_cstr(w, key)) return -1;
    return bf_mp_pack_str_cstr(w, val);
}

int bf_mp_pack_kv_int(bf_mp_writer_t *w, const char *key, int64_t val) {
    if (bf_mp_pack_str_cstr(w, key)) return -1;
    return bf_mp_pack_int(w, val);
}

int bf_mp_pack_kv_double(bf_mp_writer_t *w, const char *key, double val) {
    if (bf_mp_pack_str_cstr(w, key)) return -1;
    return bf_mp_pack_double(w, val);
}

int bf_mp_pack_kv_bool(bf_mp_writer_t *w, const char *key, int val) {
    if (bf_mp_pack_str_cstr(w, key)) return -1;
    return bf_mp_pack_bool(w, val);
}

/* ── Decoder helpers ─────────────────────────────────────────── */

static inline int has(const bf_mp_reader_t *r, size_t n) {
    return r->pos + n <= r->len;
}

static inline uint8_t r8(bf_mp_reader_t *r) {
    return r->buf[r->pos++];
}

static inline uint16_t r16(bf_mp_reader_t *r) {
    uint16_t v = ((uint16_t)r->buf[r->pos] << 8) | r->buf[r->pos + 1];
    r->pos += 2;
    return v;
}

static inline uint32_t r32(bf_mp_reader_t *r) {
    uint32_t v = ((uint32_t)r->buf[r->pos] << 24) |
                  ((uint32_t)r->buf[r->pos+1] << 16) |
                  ((uint32_t)r->buf[r->pos+2] << 8) |
                  r->buf[r->pos+3];
    r->pos += 4;
    return v;
}

static inline uint64_t r64(bf_mp_reader_t *r) {
    uint64_t hi = r32(r);
    uint64_t lo = r32(r);
    return (hi << 32) | lo;
}

/* ── Reader lifecycle ────────────────────────────────────────── */

void bf_mp_reader_init(bf_mp_reader_t *r, const void *data, size_t len) {
    r->buf = (const uint8_t *)data;
    r->len = len;
    r->pos = 0;
}

size_t bf_mp_reader_remaining(const bf_mp_reader_t *r) {
    return r->len - r->pos;
}

/* ── Decode next value ───────────────────────────────────────── */

bf_mp_type_t bf_mp_read_next(bf_mp_reader_t *r, bf_mp_value_t *out) {
    memset(out, 0, sizeof(*out));

    if (!has(r, 1)) { out->type = BF_MP_EOF; return BF_MP_EOF; }

    uint8_t tag = r8(r);

    /* Positive fixint: 0x00-0x7f */
    if (tag <= 0x7f) {
        out->type = BF_MP_UINT;
        out->v.u = tag;
        return BF_MP_UINT;
    }

    /* Negative fixint: 0xe0-0xff */
    if (tag >= 0xe0) {
        out->type = BF_MP_INT;
        out->v.i = (int8_t)tag;
        return BF_MP_INT;
    }

    /* Fixmap: 0x80-0x8f */
    if ((tag & 0xf0) == 0x80) {
        out->type = BF_MP_MAP;
        out->v.count = tag & 0x0f;
        return BF_MP_MAP;
    }

    /* Fixarray: 0x90-0x9f */
    if ((tag & 0xf0) == 0x90) {
        out->type = BF_MP_ARRAY;
        out->v.count = tag & 0x0f;
        return BF_MP_ARRAY;
    }

    /* Fixstr: 0xa0-0xbf */
    if ((tag & 0xe0) == 0xa0) {
        uint32_t len = tag & 0x1f;
        if (!has(r, len)) { out->type = BF_MP_ERROR; return BF_MP_ERROR; }
        out->type = BF_MP_STR;
        out->v.raw.ptr = r->buf + r->pos;
        out->v.raw.len = len;
        r->pos += len;
        return BF_MP_STR;
    }

    switch (tag) {
    case 0xc0: out->type = BF_MP_NIL; return BF_MP_NIL;
    case 0xc2: out->type = BF_MP_BOOL; out->v.b = 0; return BF_MP_BOOL;
    case 0xc3: out->type = BF_MP_BOOL; out->v.b = 1; return BF_MP_BOOL;

    /* bin8/16/32 */
    case 0xc4: {
        if (!has(r, 1)) goto err;
        uint32_t len = r8(r);
        if (!has(r, len)) goto err;
        out->type = BF_MP_BIN; out->v.raw.ptr = r->buf + r->pos; out->v.raw.len = len;
        r->pos += len; return BF_MP_BIN;
    }
    case 0xc5: {
        if (!has(r, 2)) goto err;
        uint32_t len = r16(r);
        if (!has(r, len)) goto err;
        out->type = BF_MP_BIN; out->v.raw.ptr = r->buf + r->pos; out->v.raw.len = len;
        r->pos += len; return BF_MP_BIN;
    }
    case 0xc6: {
        if (!has(r, 4)) goto err;
        uint32_t len = r32(r);
        if (!has(r, len)) goto err;
        out->type = BF_MP_BIN; out->v.raw.ptr = r->buf + r->pos; out->v.raw.len = len;
        r->pos += len; return BF_MP_BIN;
    }

    /* float32 */
    case 0xca: {
        if (!has(r, 4)) goto err;
        uint32_t u = r32(r);
        float f; memcpy(&f, &u, 4);
        out->type = BF_MP_FLOAT; out->v.f = f; return BF_MP_FLOAT;
    }
    /* float64 */
    case 0xcb: {
        if (!has(r, 8)) goto err;
        uint64_t u = r64(r);
        double d; memcpy(&d, &u, 8);
        out->type = BF_MP_DOUBLE; out->v.d = d; return BF_MP_DOUBLE;
    }

    /* uint8/16/32/64 */
    case 0xcc: if (!has(r, 1)) goto err; out->type = BF_MP_UINT; out->v.u = r8(r); return BF_MP_UINT;
    case 0xcd: if (!has(r, 2)) goto err; out->type = BF_MP_UINT; out->v.u = r16(r); return BF_MP_UINT;
    case 0xce: if (!has(r, 4)) goto err; out->type = BF_MP_UINT; out->v.u = r32(r); return BF_MP_UINT;
    case 0xcf: if (!has(r, 8)) goto err; out->type = BF_MP_UINT; out->v.u = r64(r); return BF_MP_UINT;

    /* int8/16/32/64 */
    case 0xd0: if (!has(r, 1)) goto err; out->type = BF_MP_INT; out->v.i = (int8_t)r8(r); return BF_MP_INT;
    case 0xd1: if (!has(r, 2)) goto err; out->type = BF_MP_INT; out->v.i = (int16_t)r16(r); return BF_MP_INT;
    case 0xd2: if (!has(r, 4)) goto err; out->type = BF_MP_INT; out->v.i = (int32_t)r32(r); return BF_MP_INT;
    case 0xd3: if (!has(r, 8)) goto err; out->type = BF_MP_INT; out->v.i = (int64_t)r64(r); return BF_MP_INT;

    /* str8/16/32 */
    case 0xd9: {
        if (!has(r, 1)) goto err;
        uint32_t len = r8(r);
        if (!has(r, len)) goto err;
        out->type = BF_MP_STR; out->v.raw.ptr = r->buf + r->pos; out->v.raw.len = len;
        r->pos += len; return BF_MP_STR;
    }
    case 0xda: {
        if (!has(r, 2)) goto err;
        uint32_t len = r16(r);
        if (!has(r, len)) goto err;
        out->type = BF_MP_STR; out->v.raw.ptr = r->buf + r->pos; out->v.raw.len = len;
        r->pos += len; return BF_MP_STR;
    }
    case 0xdb: {
        if (!has(r, 4)) goto err;
        uint32_t len = r32(r);
        if (!has(r, len)) goto err;
        out->type = BF_MP_STR; out->v.raw.ptr = r->buf + r->pos; out->v.raw.len = len;
        r->pos += len; return BF_MP_STR;
    }

    /* array16/32 */
    case 0xdc: if (!has(r, 2)) goto err; out->type = BF_MP_ARRAY; out->v.count = r16(r); return BF_MP_ARRAY;
    case 0xdd: if (!has(r, 4)) goto err; out->type = BF_MP_ARRAY; out->v.count = r32(r); return BF_MP_ARRAY;

    /* map16/32 */
    case 0xde: if (!has(r, 2)) goto err; out->type = BF_MP_MAP; out->v.count = r16(r); return BF_MP_MAP;
    case 0xdf: if (!has(r, 4)) goto err; out->type = BF_MP_MAP; out->v.count = r32(r); return BF_MP_MAP;

    default: goto err;
    }

err:
    out->type = BF_MP_ERROR;
    return BF_MP_ERROR;
}

/* ── Skip ────────────────────────────────────────────────────── */

int bf_mp_skip(bf_mp_reader_t *r) {
    bf_mp_value_t val;
    bf_mp_type_t t = bf_mp_read_next(r, &val);

    switch (t) {
    case BF_MP_NIL:
    case BF_MP_BOOL:
    case BF_MP_INT:
    case BF_MP_UINT:
    case BF_MP_FLOAT:
    case BF_MP_DOUBLE:
    case BF_MP_STR:
    case BF_MP_BIN:
        return 0; /* Already consumed */

    case BF_MP_ARRAY:
        for (uint32_t i = 0; i < val.v.count; i++) {
            if (bf_mp_skip(r) != 0) return -1;
        }
        return 0;

    case BF_MP_MAP:
        for (uint32_t i = 0; i < val.v.count; i++) {
            if (bf_mp_skip(r) != 0) return -1; /* key */
            if (bf_mp_skip(r) != 0) return -1; /* value */
        }
        return 0;

    default:
        return -1;
    }
}
