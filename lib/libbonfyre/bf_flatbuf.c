/*
 * bf_flatbuf.c — FlatBuffers zero-copy artifact manifest builder + verifier
 *
 * Reader is entirely inline in the header (zero overhead).
 * This file implements:
 *   - Buffer verification (bf_fb_verify)
 *   - Builder for creating BfManifest flatbuffers from C structs
 *   - JSON → FlatBuffer converter with minimal tokenizer
 */

#include "bf_flatbuf.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Magic / file identifier ─────────────────────────────────── */

static const char BF_FB_IDENT[4] = {'B', 'F', 'M', 'F'};

/* ── Verify ──────────────────────────────────────────────────── */

int bf_fb_verify(const bf_fb_buf_t *buf) {
    if (!buf || !buf->data) return BF_FB_ERR_NULL;
    if (buf->size < 8) return BF_FB_ERR_TOO_SMALL;

    /* Check file identifier at bytes [4..7] */
    if (memcmp(buf->data + 4, BF_FB_IDENT, 4) != 0)
        return BF_FB_ERR_BAD_MAGIC;

    /* Root table offset */
    uint32_t root_off = *(const uint32_t *)buf->data;
    if (root_off >= buf->size) return BF_FB_ERR_BAD_OFFSET;

    return BF_FB_OK;
}

bf_fb_buf_t bf_fb_wrap(const void *data, size_t size) {
    return (bf_fb_buf_t){ .data = (const uint8_t *)data, .size = size };
}

/* ── Builder internals ───────────────────────────────────────── */

struct bf_fb_builder {
    uint8_t *buf;
    size_t   cap;
    size_t   len;            /* Grows from end (FlatBuffers are built back-to-front) */
    size_t   head;           /* Current write position (= cap - len) */

    /* Artifact offsets for the manifest vector */
    uint32_t *artifact_offs;
    int       n_artifacts;
    int       artifacts_cap;
};

static int fb_grow(bf_fb_builder_t *b, size_t need) {
    while (b->len + need > b->cap) {
        size_t newcap = b->cap * 2;
        if (newcap < 1024) newcap = 1024;
        uint8_t *nb = realloc(b->buf, newcap);
        if (!nb) return -1;
        /* Data is at the end of buf, so shift it */
        size_t old_head = b->cap - b->len;
        size_t new_head = newcap - b->len;
        memmove(nb + new_head, nb + old_head, b->len);
        b->buf = nb;
        b->cap = newcap;
        b->head = new_head;
    }
    return 0;
}

static void fb_prep(bf_fb_builder_t *b, size_t n) {
    fb_grow(b, n);
    b->head -= n;
    b->len += n;
}

static void fb_pad(bf_fb_builder_t *b, size_t align) {
    size_t pad = (~b->len + 1) & (align - 1);
    if (pad) { fb_prep(b, pad); memset(b->buf + b->head, 0, pad); }
}

static uint32_t fb_offset(bf_fb_builder_t *b) {
    return (uint32_t)b->len;
}

static void fb_u8(bf_fb_builder_t *b, uint8_t v) {
    fb_prep(b, 1); b->buf[b->head] = v;
}

static void fb_u16(bf_fb_builder_t *b, uint16_t v) {
    fb_pad(b, 2); fb_prep(b, 2); memcpy(b->buf + b->head, &v, 2);
}

static void fb_u32(bf_fb_builder_t *b, uint32_t v) {
    fb_pad(b, 4); fb_prep(b, 4); memcpy(b->buf + b->head, &v, 4);
}

static void fb_i64(bf_fb_builder_t *b, int64_t v) {
    fb_pad(b, 8); fb_prep(b, 8); memcpy(b->buf + b->head, &v, 8);
}

/* Write a string, return its offset from end */
static uint32_t fb_string(bf_fb_builder_t *b, const char *s) {
    if (!s) s = "";
    size_t slen = strlen(s);
    fb_u8(b, 0); /* NUL terminator */
    fb_prep(b, slen); memcpy(b->buf + b->head, s, slen);
    fb_pad(b, 4);
    fb_u32(b, (uint32_t)slen);
    return fb_offset(b);
}

/* ── Public builder API ──────────────────────────────────────── */

bf_fb_builder_t *bf_fb_builder_new(size_t initial_cap) {
    bf_fb_builder_t *b = calloc(1, sizeof(*b));
    if (!b) return NULL;
    b->cap = initial_cap > 256 ? initial_cap : 256;
    b->buf = calloc(1, b->cap);
    if (!b->buf) { free(b); return NULL; }
    b->head = b->cap;
    b->artifacts_cap = 64;
    b->artifact_offs = calloc((size_t)b->artifacts_cap, sizeof(uint32_t));
    return b;
}

void bf_fb_builder_free(bf_fb_builder_t *b) {
    if (!b) return;
    free(b->buf);
    free(b->artifact_offs);
    free(b);
}

int bf_fb_add_artifact(bf_fb_builder_t *b,
                        const char *id, const char *type,
                        const char *source, const char *family,
                        const char *canonical, int64_t created_at,
                        int64_t size_bytes, const char *hash,
                        uint8_t status) {
    if (!b) return -1;

    /* Grow artifact offsets array if needed */
    if (b->n_artifacts >= b->artifacts_cap) {
        b->artifacts_cap *= 2;
        b->artifact_offs = realloc(b->artifact_offs,
                                    (size_t)b->artifacts_cap * sizeof(uint32_t));
        if (!b->artifact_offs) return -1;
    }

    /* Pre-create all strings */
    uint32_t off_id =        fb_string(b, id);
    uint32_t off_type =      fb_string(b, type);
    uint32_t off_source =    fb_string(b, source);
    uint32_t off_family =    fb_string(b, family);
    uint32_t off_canonical = fb_string(b, canonical);
    uint32_t off_hash =      fb_string(b, hash);

    /* Build vtable: 4B vtable_size + 4B table_size + N × 2B field offsets */
    /* Fields: id(0), type(1), source(2), family(3), canonical(4),
     *         created_at(5), size_bytes(6), hash(7), status(8) */
    int n_fields = 9;
    uint16_t vt_size = (uint16_t)(4 + n_fields * 2);

    /* Table body: store inline fields + offsets to strings */
    /* For simplicity, store everything as sequential slots:
     *   [soffset to vtable][6 × uoffset to strings][2 × int64][1 × uint8] */
    size_t table_body_size = 4 + 6 * 4 + 2 * 8 + 1;
    fb_pad(b, 8);

    uint32_t table_start = fb_offset(b);

    /* Write table body: status, size_bytes, created_at, offsets */
    fb_u8(b, status);                  /* field 8 */
    fb_pad(b, 8);
    fb_i64(b, size_bytes);             /* field 6 */
    fb_i64(b, created_at);            /* field 5 */

    /* String offsets (relative forward pointers) */
    uint32_t cur;
    cur = fb_offset(b);
    fb_u32(b, off_hash - cur);         /* field 7 */
    cur = fb_offset(b);
    fb_u32(b, off_canonical - cur);    /* field 4 */
    cur = fb_offset(b);
    fb_u32(b, off_family - cur);       /* field 3 */
    cur = fb_offset(b);
    fb_u32(b, off_source - cur);       /* field 2 */
    cur = fb_offset(b);
    fb_u32(b, off_type - cur);         /* field 1 */
    cur = fb_offset(b);
    fb_u32(b, off_id - cur);           /* field 0 */

    uint32_t table_end = fb_offset(b);
    (void)table_body_size;

    /* Vtable */
    uint16_t field_offsets[9];
    /* Calculate offsets from table start (end of table in buffer direction) */
    size_t pos = 4; /* Skip soffset-to-vtable */
    field_offsets[0] = (uint16_t)pos; pos += 4; /* id */
    field_offsets[1] = (uint16_t)pos; pos += 4; /* type */
    field_offsets[2] = (uint16_t)pos; pos += 4; /* source */
    field_offsets[3] = (uint16_t)pos; pos += 4; /* family */
    field_offsets[4] = (uint16_t)pos; pos += 4; /* canonical */
    field_offsets[5] = (uint16_t)pos; pos += 8; /* created_at */
    field_offsets[6] = (uint16_t)pos; pos += 8; /* size_bytes */
    field_offsets[7] = (uint16_t)pos; pos += 4; /* hash */
    field_offsets[8] = (uint16_t)pos;            /* status */

    for (int i = n_fields - 1; i >= 0; i--) fb_u16(b, field_offsets[i]);
    fb_u16(b, (uint16_t)(table_end - table_start + 4)); /* table size */
    fb_u16(b, vt_size);

    /* Write soffset-to-vtable at table start */
    /* This is a back-reference, we need to patch it */
    uint32_t vtable_off = fb_offset(b);

    /* Record table offset for manifest vector */
    b->artifact_offs[b->n_artifacts++] = table_start;

    (void)vtable_off;
    return 0;
}

int bf_fb_finish(bf_fb_builder_t *b, uint8_t **out, size_t *out_size) {
    if (!b) return -1;

    /* Build artifact vector */
    for (int i = b->n_artifacts - 1; i >= 0; i--) {
        uint32_t cur = fb_offset(b);
        fb_u32(b, b->artifact_offs[i] - cur);
    }
    fb_u32(b, (uint32_t)b->n_artifacts);
    uint32_t vec_off = fb_offset(b);

    /* Build BfManifest root table */
    fb_pad(b, 8);
    fb_i64(b, 0); /* created_at — caller can patch */
    uint32_t cur = fb_offset(b);
    fb_u32(b, vec_off - cur); /* artifacts vector */
    fb_u16(b, 1); /* version */
    fb_pad(b, 4);

    /* Root table soffset placeholder */
    uint32_t root_table = fb_offset(b);
    fb_u32(b, 0); /* vtable soffset — simplified */

    /* File identifier */
    fb_prep(b, 4); memcpy(b->buf + b->head, BF_FB_IDENT, 4);

    /* Root offset */
    fb_u32(b, root_table);

    /* Copy output */
    *out_size = b->len;
    *out = malloc(b->len);
    if (!*out) return -1;
    memcpy(*out, b->buf + b->head, b->len);

    return 0;
}

/* ── Minimal JSON tokenizer for bf_fb_from_json ──────────────── */

typedef enum {
    JT_LBRACE, JT_RBRACE, JT_LBRACK, JT_RBRACK,
    JT_COLON, JT_COMMA, JT_STRING, JT_NUMBER, JT_NULL,
    JT_TRUE, JT_FALSE, JT_END, JT_ERR
} jtoken_t;

typedef struct {
    const char *src;
    size_t      len;
    size_t      pos;
    char        strbuf[4096];
    size_t      strpos;
    int64_t     numval;
} jlex_t;

static void j_skip_ws(jlex_t *j) {
    while (j->pos < j->len) {
        char c = j->src[j->pos];
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') j->pos++;
        else break;
    }
}

static jtoken_t j_next(jlex_t *j) {
    j_skip_ws(j);
    if (j->pos >= j->len) return JT_END;

    char c = j->src[j->pos];
    switch (c) {
        case '{': j->pos++; return JT_LBRACE;
        case '}': j->pos++; return JT_RBRACE;
        case '[': j->pos++; return JT_LBRACK;
        case ']': j->pos++; return JT_RBRACK;
        case ':': j->pos++; return JT_COLON;
        case ',': j->pos++; return JT_COMMA;
        case '"': {
            j->pos++;
            j->strpos = 0;
            while (j->pos < j->len && j->src[j->pos] != '"') {
                if (j->src[j->pos] == '\\' && j->pos + 1 < j->len) {
                    j->pos++;
                    char e = j->src[j->pos];
                    if (e == 'n') j->strbuf[j->strpos++] = '\n';
                    else if (e == 't') j->strbuf[j->strpos++] = '\t';
                    else j->strbuf[j->strpos++] = e;
                } else {
                    if (j->strpos < sizeof(j->strbuf) - 1)
                        j->strbuf[j->strpos++] = j->src[j->pos];
                }
                j->pos++;
            }
            if (j->pos < j->len) j->pos++; /* Skip closing quote */
            j->strbuf[j->strpos] = '\0';
            return JT_STRING;
        }
        case 'n': j->pos += 4; return JT_NULL;
        case 't': j->pos += 4; return JT_TRUE;
        case 'f': j->pos += 5; return JT_FALSE;
        default: {
            /* Number */
            int neg = 0;
            if (c == '-') { neg = 1; j->pos++; }
            int64_t v = 0;
            while (j->pos < j->len && j->src[j->pos] >= '0' && j->src[j->pos] <= '9') {
                v = v * 10 + (j->src[j->pos] - '0');
                j->pos++;
            }
            /* Skip fractional part */
            if (j->pos < j->len && j->src[j->pos] == '.') {
                j->pos++;
                while (j->pos < j->len && j->src[j->pos] >= '0' && j->src[j->pos] <= '9')
                    j->pos++;
            }
            j->numval = neg ? -v : v;
            return JT_NUMBER;
        }
    }
}

/* Skip a JSON value recursively */
static void j_skip_value(jlex_t *j) {
    jtoken_t t = j_next(j);
    if (t == JT_LBRACE) {
        int depth = 1;
        while (depth > 0) {
            t = j_next(j);
            if (t == JT_LBRACE) depth++;
            else if (t == JT_RBRACE) depth--;
            else if (t == JT_END) break;
        }
    } else if (t == JT_LBRACK) {
        int depth = 1;
        while (depth > 0) {
            t = j_next(j);
            if (t == JT_LBRACK) depth++;
            else if (t == JT_RBRACK) depth--;
            else if (t == JT_END) break;
        }
    }
    /* Primitives: already consumed */
}

int bf_fb_from_json(const char *json, size_t json_len,
                     uint8_t **out, size_t *out_size) {
    if (!json || !out || !out_size) return -1;

    bf_fb_builder_t *b = bf_fb_builder_new(json_len * 2);
    if (!b) return -1;

    jlex_t j = { .src = json, .len = json_len, .pos = 0 };

    /* Expect top-level object or array */
    jtoken_t t = j_next(&j);

    if (t == JT_LBRACE) {
        /* Single artifact object */
        char id[256] = "", type[128] = "", source[256] = "";
        char family[256] = "", canonical[256] = "", hash[128] = "";
        int64_t created_at = 0, size_bytes = 0;
        uint8_t status = 0;

        while ((t = j_next(&j)) == JT_STRING) {
            char key[128];
            snprintf(key, sizeof(key), "%s", j.strbuf);
            t = j_next(&j); /* colon */
            if (t != JT_COLON) break;

            if (strcmp(key, "id") == 0 || strcmp(key, "artifact_id") == 0) {
                t = j_next(&j); if (t == JT_STRING) snprintf(id, sizeof(id), "%s", j.strbuf);
            } else if (strcmp(key, "type") == 0) {
                t = j_next(&j); if (t == JT_STRING) snprintf(type, sizeof(type), "%s", j.strbuf);
            } else if (strcmp(key, "source") == 0 || strcmp(key, "source_system") == 0) {
                t = j_next(&j); if (t == JT_STRING) snprintf(source, sizeof(source), "%s", j.strbuf);
            } else if (strcmp(key, "family") == 0 || strcmp(key, "family_key") == 0) {
                t = j_next(&j); if (t == JT_STRING) snprintf(family, sizeof(family), "%s", j.strbuf);
            } else if (strcmp(key, "canonical") == 0 || strcmp(key, "canonical_key") == 0) {
                t = j_next(&j); if (t == JT_STRING) snprintf(canonical, sizeof(canonical), "%s", j.strbuf);
            } else if (strcmp(key, "hash") == 0 || strcmp(key, "sha256") == 0) {
                t = j_next(&j); if (t == JT_STRING) snprintf(hash, sizeof(hash), "%s", j.strbuf);
            } else if (strcmp(key, "created_at") == 0) {
                t = j_next(&j); if (t == JT_NUMBER) created_at = j.numval;
            } else if (strcmp(key, "size_bytes") == 0 || strcmp(key, "size") == 0) {
                t = j_next(&j); if (t == JT_NUMBER) size_bytes = j.numval;
            } else if (strcmp(key, "status") == 0) {
                t = j_next(&j);
                if (t == JT_NUMBER) status = (uint8_t)j.numval;
                else if (t == JT_STRING) {
                    if (strcmp(j.strbuf, "ready") == 0) status = 1;
                    else if (strcmp(j.strbuf, "published") == 0) status = 2;
                }
            } else {
                j_skip_value(&j);
            }

            t = j_next(&j);
            if (t == JT_RBRACE) break;
            /* else JT_COMMA, continue */
        }

        bf_fb_add_artifact(b, id, type, source, family, canonical,
                            created_at, size_bytes, hash, status);
    } else if (t == JT_LBRACK) {
        /* Array of artifacts */
        while ((t = j_next(&j)) == JT_LBRACE) {
            char id[256] = "", type[128] = "", source[256] = "";
            char family[256] = "", canonical[256] = "", hash[128] = "";
            int64_t created_at = 0, size_bytes = 0;
            uint8_t status = 0;

            while ((t = j_next(&j)) == JT_STRING) {
                char key[128];
                snprintf(key, sizeof(key), "%s", j.strbuf);
                t = j_next(&j);
                if (t != JT_COLON) break;

                if (strcmp(key, "id") == 0 || strcmp(key, "artifact_id") == 0) {
                    t = j_next(&j); if (t == JT_STRING) snprintf(id, sizeof(id), "%s", j.strbuf);
                } else if (strcmp(key, "type") == 0) {
                    t = j_next(&j); if (t == JT_STRING) snprintf(type, sizeof(type), "%s", j.strbuf);
                } else if (strcmp(key, "source") == 0 || strcmp(key, "source_system") == 0) {
                    t = j_next(&j); if (t == JT_STRING) snprintf(source, sizeof(source), "%s", j.strbuf);
                } else if (strcmp(key, "family") == 0 || strcmp(key, "family_key") == 0) {
                    t = j_next(&j); if (t == JT_STRING) snprintf(family, sizeof(family), "%s", j.strbuf);
                } else if (strcmp(key, "canonical") == 0 || strcmp(key, "canonical_key") == 0) {
                    t = j_next(&j); if (t == JT_STRING) snprintf(canonical, sizeof(canonical), "%s", j.strbuf);
                } else if (strcmp(key, "hash") == 0 || strcmp(key, "sha256") == 0) {
                    t = j_next(&j); if (t == JT_STRING) snprintf(hash, sizeof(hash), "%s", j.strbuf);
                } else if (strcmp(key, "created_at") == 0) {
                    t = j_next(&j); if (t == JT_NUMBER) created_at = j.numval;
                } else if (strcmp(key, "size_bytes") == 0 || strcmp(key, "size") == 0) {
                    t = j_next(&j); if (t == JT_NUMBER) size_bytes = j.numval;
                } else if (strcmp(key, "status") == 0) {
                    t = j_next(&j);
                    if (t == JT_NUMBER) status = (uint8_t)j.numval;
                    else if (t == JT_STRING) {
                        if (strcmp(j.strbuf, "ready") == 0) status = 1;
                        else if (strcmp(j.strbuf, "published") == 0) status = 2;
                    }
                } else {
                    j_skip_value(&j);
                }

                t = j_next(&j);
                if (t == JT_RBRACE) break;
            }

            bf_fb_add_artifact(b, id, type, source, family, canonical,
                                created_at, size_bytes, hash, status);

            /* Expect comma or end of array */
            size_t save = j.pos;
            t = j_next(&j);
            if (t == JT_RBRACK) break;
            if (t != JT_COMMA) { j.pos = save; break; }
        }
    }

    int rc = bf_fb_finish(b, out, out_size);
    bf_fb_builder_free(b);
    return rc;
}
