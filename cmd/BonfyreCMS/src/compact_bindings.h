#ifndef COMPACT_BINDINGS_H
#define COMPACT_BINDINGS_H

/*
 * Compact Binary Binding Format (Upgrade XI)
 *
 * This header is now a thin compatibility wrapper around liblambda-tensors.
 * All compression logic lives in the standalone library.
 * This file provides:
 *   1. CB_* aliases for the LT_* type tags
 *   2. compact_* aliases for the lt_* functions
 *   3. SQLite storage functions (bootstrap, pack_family, pack_content_type)
 */

#include <sqlite3.h>
#include "lambda_tensors.h"

/* ---- Tag aliases ---- */
#define CB_NULL      LT_NULL
#define CB_BOOL_F    LT_BOOL_F
#define CB_BOOL_T    LT_BOOL_T
#define CB_INT       LT_INT
#define CB_DOUBLE    LT_DOUBLE
#define CB_STRING    LT_STRING
#define CB_NESTED    LT_NESTED
#define CB_STRREF    LT_STRREF
#define CB_SMALL_INT LT_SMALL_INT
#define CB_FLOAT32   LT_FLOAT32
#define CB_EMPTY_STR LT_EMPTY_STR
#define CB_FAMSTR    LT_FAMSTR
#define CB_STORED_DELTA  LT_STORED_DELTA
#define CB_STORED_PACKED LT_STORED_PACKED

#define COMPACT_CODEC_V2_PACKED   "v2_packed"
#define COMPACT_CODEC_V2_INTERNED "v2_interned"

/* ---- Type aliases ---- */
typedef LtStringTable   FamilyStringTable;
typedef LtHuffSymbol    HuffSymEntry;
typedef LtHuffPosition  HuffPosCodebook;
typedef LtHuffTable     FamilyHuffTable;

/* ---- Function aliases: encode/decode ---- */
#define compact_encode            lt_encode_v1
#define compact_decode            lt_decode_v1
#define compact_delta_encode      lt_delta_encode_v1
#define compact_delta_decode      lt_delta_decode_v1
#define compact_measure           lt_measure_v1
#define compact_delta_measure     lt_delta_measure_v1

#define compact_encode_v2         lt_encode_v2
#define compact_decode_v2         lt_decode_v2
#define compact_delta_encode_v2   lt_delta_encode_v2
#define compact_delta_decode_v2   lt_delta_decode_v2
#define compact_measure_v2        lt_measure_v2
#define compact_delta_measure_v2  lt_delta_measure_v2

#define compact_encode_v2_interned       lt_encode_v2_interned
#define compact_decode_v2_interned       lt_decode_v2_interned
#define compact_measure_v2_interned      lt_measure_v2_interned
#define compact_delta_measure_v2_interned lt_delta_measure_v2_interned
#define compact_decode_member_v2_interned lt_decode_member_v2_interned

#define compact_measure_v2_huffman       lt_measure_v2_huffman
#define compact_delta_measure_v2_huffman lt_delta_measure_v2_huffman
#define compact_measure_v2_arithmetic    lt_measure_v2_arithmetic
#define compact_delta_measure_v2_arithmetic lt_delta_measure_v2_arithmetic

/* ---- Function aliases: family string table ---- */
#define family_strtab_init           lt_strtab_init
#define family_strtab_free           lt_strtab_free
#define family_strtab_header_size    lt_strtab_header_size
#define family_strtab_encode_header  lt_strtab_encode_header
#define family_strtab_decode_header  lt_strtab_decode_header

/* family_strtab_ingest/finalize: BonfyreCMS still uses the old signature.
 * Provide inline wrappers that use a module-level context. */
static LtStrtabCtx *_cb_strtab_ctx = NULL;

static inline int family_strtab_ingest(FamilyStringTable *t, const char *json) {
    (void)t;
    if (!_cb_strtab_ctx) _cb_strtab_ctx = lt_strtab_ctx_new();
    return lt_strtab_ingest(_cb_strtab_ctx, json);
}

static inline void family_strtab_finalize(FamilyStringTable *t) {
    if (_cb_strtab_ctx) {
        lt_strtab_finalize(_cb_strtab_ctx, t);
        lt_strtab_ctx_free(_cb_strtab_ctx);
        _cb_strtab_ctx = NULL;
    }
}

/* ---- Function aliases: Huffman ---- */
#define family_huff_init        lt_huff_init
#define family_huff_free        lt_huff_free
#define family_huff_ingest      lt_huff_ingest
#define family_huff_finalize    lt_huff_finalize
#define family_huff_header_size lt_huff_header_size

/* ---- SQLite storage layer (implemented in compact_bindings.c) ---- */
int compact_bindings_bootstrap(sqlite3 *db);
int compact_pack_family(sqlite3 *db, int family_id, const char *content_type);
int compact_pack_content_type(sqlite3 *db, const char *content_type);

#endif /* COMPACT_BINDINGS_H */
