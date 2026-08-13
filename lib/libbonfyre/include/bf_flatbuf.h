/*
 * bf_flatbuf.h — FlatBuffers zero-copy artifact manifests
 *
 * Replaces ad-hoc JSON parsing of artifact.json / intake-manifest.json
 * with zero-copy FlatBuffer reads. Benefits:
 *   - No parsing: mmap the file, cast pointers
 *   - Schema-validated at build time (flatcc compiler)
 *   - 10-100x faster than JSON parse for large manifests
 *   - Wire-compatible: same format for disk, network, mmap
 *
 * Two modes:
 *   1. Schema-generated (requires flatcc) — full type safety
 *   2. Hand-rolled reader (this file) — no build dependency,
 *      reads standard FlatBuffer wire format
 *
 * Wire layout (FlatBuffer):
 *   [4B root table offset] [vtable...] [table fields...]
 *   All offsets are relative (position + value) → relocatable
 *   Strings are [4B len][data...][NUL] → zero-copy C strings
 */

#ifndef BF_FLATBUF_H
#define BF_FLATBUF_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Error codes ─────────────────────────────────────────────── */

#define BF_FB_OK              0
#define BF_FB_ERR_NULL       -1
#define BF_FB_ERR_TOO_SMALL  -2
#define BF_FB_ERR_BAD_MAGIC  -3
#define BF_FB_ERR_BAD_OFFSET -4
#define BF_FB_ERR_OVERFLOW   -5

/* ── Artifact manifest schema (field IDs mirror artifact.json) ── */

/*
 * Table BfArtifact {
 *   id:         string;     // 0 — SHA-256 hex
 *   type:       string;     // 1 — "transcript", "summary", etc.
 *   source:     string;     // 2 — source system or binary
 *   family:     string;     // 3 — family key (FNV-1a)
 *   canonical:  string;     // 4 — canonical key
 *   created_at: int64;      // 5 — Unix epoch ns
 *   size_bytes: int64;      // 6
 *   hash:       string;     // 7 — SHA-256 of content
 *   status:     uint8;      // 8 — 0=draft, 1=ready, 2=published
 *   tags:       [string];   // 9 — vector of tag strings
 *   components: [BfComponent]; // 10 — sub-components
 * }
 *
 * Table BfComponent {
 *   name:       string;     // 0
 *   mime_type:  string;     // 1
 *   offset:     int64;      // 2 — byte offset in blob
 *   length:     int64;      // 3
 * }
 *
 * Table BfManifest {
 *   version:    uint16;     // 0
 *   artifacts:  [BfArtifact]; // 1
 *   created_at: int64;      // 2
 * }
 *
 * root_type BfManifest;
 * file_identifier "BFMF";
 */

/* ── Zero-copy reader (hand-rolled, no flatcc dependency) ───── */

typedef struct bf_fb_buf {
    const uint8_t *data;
    size_t         size;
} bf_fb_buf_t;

/*
 * Verify buffer has valid FlatBuffer structure + "BFMF" identifier.
 * Call before any read operations.
 */
int bf_fb_verify(const bf_fb_buf_t *buf);

/*
 * Load a FlatBuffer from an mmap'd file or memory region.
 * Does NOT copy data — reads directly from the pointer.
 */
bf_fb_buf_t bf_fb_wrap(const void *data, size_t size);

/* ── Primitive readers ───────────────────────────────────────── */

/* Read table field at vtable slot `field_id` */
static inline uint32_t bf_fb_field_offset(const uint8_t *table, int field_id) {
    int32_t vt_off = *(const int32_t *)table;           /* soffset to vtable */
    const uint8_t *vtable = table - vt_off;
    uint16_t vt_size = *(const uint16_t *)vtable;
    int slot = 4 + field_id * 2;                         /* vtable header is 4B */
    if (slot + 2 > (int)vt_size) return 0;              /* field absent */
    return *(const uint16_t *)(vtable + slot);
}

/* Read string at field_id (returns NUL-terminated pointer into buffer) */
static inline const char *bf_fb_string(const uint8_t *table, int field_id) {
    uint32_t off = bf_fb_field_offset(table, field_id);
    if (!off) return NULL;
    const uint8_t *field = table + off;
    uint32_t str_off = *(const uint32_t *)field;
    const uint8_t *str = field + str_off;
    /* String layout: [4B len][chars...][NUL] */
    return (const char *)(str + 4);
}

static inline size_t bf_fb_string_len(const uint8_t *table, int field_id) {
    uint32_t off = bf_fb_field_offset(table, field_id);
    if (!off) return 0;
    const uint8_t *field = table + off;
    uint32_t str_off = *(const uint32_t *)field;
    const uint8_t *str = field + str_off;
    return *(const uint32_t *)str;
}

static inline int64_t bf_fb_int64(const uint8_t *table, int field_id) {
    uint32_t off = bf_fb_field_offset(table, field_id);
    if (!off) return 0;
    return *(const int64_t *)(table + off);
}

static inline uint16_t bf_fb_uint16(const uint8_t *table, int field_id) {
    uint32_t off = bf_fb_field_offset(table, field_id);
    if (!off) return 0;
    return *(const uint16_t *)(table + off);
}

static inline uint8_t bf_fb_uint8(const uint8_t *table, int field_id) {
    uint32_t off = bf_fb_field_offset(table, field_id);
    if (!off) return 0;
    return *(const uint8_t *)(table + off);
}

/* ── Vector readers ──────────────────────────────────────────── */

static inline int bf_fb_vec_len(const uint8_t *table, int field_id) {
    uint32_t off = bf_fb_field_offset(table, field_id);
    if (!off) return 0;
    const uint8_t *field = table + off;
    uint32_t vec_off = *(const uint32_t *)field;
    const uint8_t *vec = field + vec_off;
    return (int)(*(const uint32_t *)vec);
}

/* Get pointer to table at vector[i] (for object vectors) */
static inline const uint8_t *bf_fb_vec_at(const uint8_t *table,
                                            int field_id, int idx) {
    uint32_t off = bf_fb_field_offset(table, field_id);
    if (!off) return NULL;
    const uint8_t *field = table + off;
    uint32_t vec_off = *(const uint32_t *)field;
    const uint8_t *vec = field + vec_off;
    int count = (int)(*(const uint32_t *)vec);
    if (idx < 0 || idx >= count) return NULL;
    const uint8_t *elems = vec + 4;
    uint32_t elem_off = *(const uint32_t *)(elems + idx * 4);
    return elems + idx * 4 + elem_off;
}

/* Get string at vector[i] (for string vectors like tags) */
static inline const char *bf_fb_vec_string_at(const uint8_t *table,
                                                int field_id, int idx) {
    uint32_t off = bf_fb_field_offset(table, field_id);
    if (!off) return NULL;
    const uint8_t *field = table + off;
    uint32_t vec_off = *(const uint32_t *)field;
    const uint8_t *vec = field + vec_off;
    int count = (int)(*(const uint32_t *)vec);
    if (idx < 0 || idx >= count) return NULL;
    const uint8_t *elems = vec + 4;
    uint32_t str_off = *(const uint32_t *)(elems + idx * 4);
    const uint8_t *str = elems + idx * 4 + str_off;
    return (const char *)(str + 4);
}

/* ── Builder (minimal, for writing manifests) ────────────────── */

typedef struct bf_fb_builder bf_fb_builder_t;

bf_fb_builder_t *bf_fb_builder_new(size_t initial_cap);
void bf_fb_builder_free(bf_fb_builder_t *b);

/* Build a BfArtifact table */
int bf_fb_add_artifact(bf_fb_builder_t *b,
                        const char *id,
                        const char *type,
                        const char *source,
                        const char *family,
                        const char *canonical,
                        int64_t     created_at,
                        int64_t     size_bytes,
                        const char *hash,
                        uint8_t     status);

/* Finalize and get the buffer. Caller owns the returned buffer. */
int bf_fb_finish(bf_fb_builder_t *b, uint8_t **out, size_t *out_size);

/* ── JSON → FlatBuffer converter ─────────────────────────────── */

/*
 * Convert artifact.json text to BfManifest FlatBuffer.
 * Uses minimal JSON tokenizer internally.
 * Returns 0 on success, populates out/out_size.
 */
int bf_fb_from_json(const char *json, size_t json_len,
                     uint8_t **out, size_t *out_size);

#ifdef __cplusplus
}
#endif

#endif /* BF_FLATBUF_H */
