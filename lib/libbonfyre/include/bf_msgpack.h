/*
 * bf_msgpack.h — Minimal MessagePack encoder/decoder
 *
 * Compact binary serialization for event streams.
 * Replaces fprintf JSONL with ~40% smaller wire format.
 *
 * Encoder: builder pattern — pack values into a growable buffer.
 * Decoder: pull-based iterator over a byte stream.
 */

#ifndef BF_MSGPACK_H
#define BF_MSGPACK_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Types ───────────────────────────────────────────────────── */

typedef enum {
    BF_MP_NIL = 0,
    BF_MP_BOOL,
    BF_MP_INT,
    BF_MP_UINT,
    BF_MP_FLOAT,
    BF_MP_DOUBLE,
    BF_MP_STR,
    BF_MP_BIN,
    BF_MP_ARRAY,
    BF_MP_MAP,
    BF_MP_EXT,
    BF_MP_EOF,
    BF_MP_ERROR
} bf_mp_type_t;

/* ── Encoder ─────────────────────────────────────────────────── */

typedef struct {
    uint8_t *buf;
    size_t   len;
    size_t   cap;
} bf_mp_writer_t;

/* Initialize a writer. initial_cap=0 for default (256). */
void bf_mp_writer_init(bf_mp_writer_t *w, size_t initial_cap);

/* Free the writer's buffer. */
void bf_mp_writer_free(bf_mp_writer_t *w);

/* Reset writer for reuse (keeps buffer). */
void bf_mp_writer_reset(bf_mp_writer_t *w);

/* Get the encoded bytes. Valid until next pack call or free. */
const uint8_t *bf_mp_writer_data(const bf_mp_writer_t *w);
size_t bf_mp_writer_size(const bf_mp_writer_t *w);

/* ── Pack functions ──────────────────────────────────────────── */

int bf_mp_pack_nil(bf_mp_writer_t *w);
int bf_mp_pack_bool(bf_mp_writer_t *w, int val);
int bf_mp_pack_int(bf_mp_writer_t *w, int64_t val);
int bf_mp_pack_uint(bf_mp_writer_t *w, uint64_t val);
int bf_mp_pack_float(bf_mp_writer_t *w, float val);
int bf_mp_pack_double(bf_mp_writer_t *w, double val);
int bf_mp_pack_str(bf_mp_writer_t *w, const char *s, uint32_t len);
int bf_mp_pack_str_cstr(bf_mp_writer_t *w, const char *s);
int bf_mp_pack_bin(bf_mp_writer_t *w, const void *data, uint32_t len);
int bf_mp_pack_array(bf_mp_writer_t *w, uint32_t count);
int bf_mp_pack_map(bf_mp_writer_t *w, uint32_t count);

/* Convenience: pack a map entry (string key + typed value). */
int bf_mp_pack_kv_str(bf_mp_writer_t *w, const char *key, const char *val);
int bf_mp_pack_kv_int(bf_mp_writer_t *w, const char *key, int64_t val);
int bf_mp_pack_kv_double(bf_mp_writer_t *w, const char *key, double val);
int bf_mp_pack_kv_bool(bf_mp_writer_t *w, const char *key, int val);

/* ── Decoder ─────────────────────────────────────────────────── */

typedef struct {
    const uint8_t *buf;
    size_t         len;
    size_t         pos;
} bf_mp_reader_t;

/* Decoded value. */
typedef struct {
    bf_mp_type_t type;
    union {
        int         b;        /* BF_MP_BOOL */
        int64_t     i;        /* BF_MP_INT */
        uint64_t    u;        /* BF_MP_UINT */
        float       f;        /* BF_MP_FLOAT */
        double      d;        /* BF_MP_DOUBLE */
        struct {              /* BF_MP_STR, BF_MP_BIN */
            const uint8_t *ptr;
            uint32_t       len;
        } raw;
        uint32_t    count;    /* BF_MP_ARRAY, BF_MP_MAP (number of items/pairs) */
    } v;
} bf_mp_value_t;

/* Initialize reader over a buffer. */
void bf_mp_reader_init(bf_mp_reader_t *r, const void *data, size_t len);

/* Read next value. Returns type. BF_MP_EOF when done, BF_MP_ERROR on malformed data. */
bf_mp_type_t bf_mp_read_next(bf_mp_reader_t *r, bf_mp_value_t *out);

/* Skip one value (including nested containers). */
int bf_mp_skip(bf_mp_reader_t *r);

/* Remaining bytes. */
size_t bf_mp_reader_remaining(const bf_mp_reader_t *r);

#ifdef __cplusplus
}
#endif

#endif /* BF_MSGPACK_H */
