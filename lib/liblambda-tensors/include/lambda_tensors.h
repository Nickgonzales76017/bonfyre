#ifndef LAMBDA_TENSORS_H
#define LAMBDA_TENSORS_H

/*
 * liblambda-tensors — Family-aware structural compression for JSON
 *
 * Given a "family" of structurally similar JSON records (same keys,
 * different values), this library compresses them to ~13-15% of raw
 * JSON size while preserving O(1) per-field random access.
 *
 * Five encoding tiers (each subsumes the previous):
 *   V1:        Type tags + varint + zigzag + bitmask delta
 *   V2:        Small-int/float32/empty-str optimizations + LZ77 string refs
 *   Interned:  Cross-member string deduplication via family string table
 *   Huffman:   Per-position canonical Huffman with cost-model pruning
 *   Arithmetic: Fractional-bit entropy measurement (Shannon limit proof)
 *
 * Dependencies: libc only (stdlib, string, math, stdio).
 *               No SQLite. No zlib. No external deps.
 *
 * Thread safety: All state is in caller-owned structs.
 *                No global mutable state.
 */

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ================================================================
 * Value type tags in the binary format
 * ================================================================ */

#define LT_NULL      0
#define LT_BOOL_F    1
#define LT_BOOL_T    2
#define LT_INT       3   /* signed varint (zigzag encoded) */
#define LT_DOUBLE    4   /* 8-byte IEEE 754 */
#define LT_STRING    5   /* varint length + UTF-8 bytes */
#define LT_NESTED    6   /* varint length + raw JSON bytes */
#define LT_STRREF    7   /* varint index: back-reference to earlier string */
#define LT_SMALL_INT 8   /* 1 value byte: signed int in [-64..63] */
#define LT_FLOAT32   9   /* 4-byte IEEE 754 single precision */
#define LT_EMPTY_STR 10  /* zero-length string, tag only */
#define LT_FAMSTR    11  /* varint index: family string table reference */

/* Stored member blob tags */
#define LT_STORED_DELTA  0
#define LT_STORED_PACKED 1

/* ================================================================
 * Family String Table
 *
 * Shared across all members of a family. Cross-member string
 * deduplication: strings appearing in 2+ members get a compact
 * index instead of being stored inline.
 * ================================================================ */

typedef struct {
    char **strings;
    size_t *lengths;
    int count;
    int capacity;
} LtStringTable;

void lt_strtab_init(LtStringTable *t);
void lt_strtab_free(LtStringTable *t);

/* Ingest context — accumulates string frequencies during family scan.
 * Must be created before ingesting, freed after finalize. */
typedef struct LtStrtabCtx LtStrtabCtx;

LtStrtabCtx *lt_strtab_ctx_new(void);
void lt_strtab_ctx_free(LtStrtabCtx *ctx);

/* Feed a JSON binding array (e.g. [17,null,"hello",3.14]) to the context */
int lt_strtab_ingest(LtStrtabCtx *ctx, const char *json_bindings);

/* Finalize: sort by frequency, apply cost model, populate table */
void lt_strtab_finalize(LtStrtabCtx *ctx, LtStringTable *t);

/* Serialized header size in bytes */
int lt_strtab_header_size(const LtStringTable *t);

/* Encode/decode the string table header for storage */
int lt_strtab_encode_header(const LtStringTable *t,
                            unsigned char **out, size_t *out_len);
int lt_strtab_decode_header(const unsigned char *header, size_t header_len,
                            LtStringTable *t);

/* ================================================================
 * Huffman Codebook (per-position canonical Huffman)
 *
 * Each binding position gets its own codebook derived from the
 * family's value frequency distribution. Canonical coding means
 * only code lengths are stored in the header.
 * ================================================================ */

typedef struct {
    int type;          /* LT_* tag */
    char *val;         /* string representation of value */
    int freq;          /* frequency across family members */
    int code_len;      /* Huffman code length in bits */
    uint32_t code;     /* canonical Huffman code */
} LtHuffSymbol;

typedef struct {
    LtHuffSymbol *entries;
    int count;
    int capacity;
    int use_huffman;   /* 1 if Huffman beats raw cost, 0 = pass-through */
    int total_freq;    /* sum of all entry frequencies (for arithmetic) */
} LtHuffPosition;

typedef struct {
    LtHuffPosition *positions;
    int num_positions;
} LtHuffTable;

void lt_huff_init(LtHuffTable *t);
void lt_huff_free(LtHuffTable *t);

/* Feed a JSON binding array — accumulates per-position value PMF */
int lt_huff_ingest(LtHuffTable *t, const char *json_bindings);

/* Build Huffman trees + canonical codes, apply cost-model pruning */
void lt_huff_finalize(LtHuffTable *t);

/* Codebook header size (only Huffman-enabled positions counted) */
int lt_huff_header_size(const LtHuffTable *t);

/* ================================================================
 * Encoding / Decoding — V1 (base format)
 * ================================================================ */

/* Encode JSON binding array → compact binary. Caller frees *out. */
int lt_encode_v1(const char *json_bindings,
                 unsigned char **out, size_t *out_len);

/* Decode compact binary → JSON binding array. Caller frees *out_json. */
int lt_decode_v1(const unsigned char *packed, size_t packed_len,
                 char **out_json);

/* Delta encode: reference + target → delta blob. Caller frees *delta. */
int lt_delta_encode_v1(const unsigned char *ref, size_t ref_len,
                       const unsigned char *target, size_t target_len,
                       unsigned char **delta, size_t *delta_len);

/* Delta decode: reference + delta → reconstructed target. Caller frees *out. */
int lt_delta_decode_v1(const unsigned char *ref, size_t ref_len,
                       const unsigned char *delta, size_t delta_len,
                       unsigned char **out, size_t *out_len);

/* Measure without allocating */
int lt_measure_v1(const char *json_bindings);
int lt_delta_measure_v1(const char *ref_json, const char *target_json);

/* ================================================================
 * Encoding / Decoding — V2 (enhanced: small values, LZ77 strings,
 *                            sparse delta, hot symbols)
 * ================================================================ */

int lt_encode_v2(const char *json_bindings,
                 unsigned char **out, size_t *out_len);

int lt_decode_v2(const unsigned char *packed, size_t packed_len,
                 char **out_json);

int lt_delta_encode_v2(const unsigned char *ref, size_t ref_len,
                       const unsigned char *target, size_t target_len,
                       unsigned char **delta, size_t *delta_len);

int lt_delta_decode_v2(const unsigned char *ref, size_t ref_len,
                       const unsigned char *delta, size_t delta_len,
                       unsigned char **out, size_t *out_len);

int lt_measure_v2(const char *json_bindings);
int lt_delta_measure_v2(const char *ref_json, const char *target_json);

/* ================================================================
 * Encoding / Decoding — Interned (V2 + family string table)
 * ================================================================ */

int lt_encode_v2_interned(const char *json_bindings,
                          const LtStringTable *strtab,
                          unsigned char **out, size_t *out_len);

int lt_decode_v2_interned(const unsigned char *packed, size_t packed_len,
                          const LtStringTable *strtab,
                          char **out_json);

int lt_measure_v2_interned(const char *json_bindings,
                           const LtStringTable *strtab);

int lt_delta_measure_v2_interned(const char *ref_json, const char *target_json,
                                 const LtStringTable *strtab);

/* Decode a stored member (delta or packed) against a reference */
int lt_decode_member_v2_interned(const unsigned char *ref_packed, size_t ref_len,
                                 const unsigned char *stored, size_t stored_len,
                                 const LtStringTable *strtab,
                                 char **out_json);

/* ================================================================
 * Measurement — Huffman (V2 + per-position canonical Huffman)
 * ================================================================ */

int lt_measure_v2_huffman(const char *json_bindings,
                          const LtHuffTable *htab);

int lt_delta_measure_v2_huffman(const char *ref_json,
                                const char *target_json,
                                const LtHuffTable *htab);

/* ================================================================
 * Measurement — Arithmetic (fractional-bit entropy, Shannon limit)
 * ================================================================ */

int lt_measure_v2_arithmetic(const char *json_bindings,
                             const LtHuffTable *htab);

int lt_delta_measure_v2_arithmetic(const char *ref_json,
                                   const char *target_json,
                                   const LtHuffTable *htab);

#ifdef __cplusplus
}
#endif

#endif /* LAMBDA_TENSORS_H */
