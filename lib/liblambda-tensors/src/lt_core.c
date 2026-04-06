/* lt_core.c — liblambda-tensors: Family-aware structural JSON compression
 *
 * Five encoding tiers:
 *   V1:        Type tags + varint + zigzag + bitmask delta
 *   V2:        Small-int/float32/empty-str + LZ77 strings + sparse delta
 *   Interned:  Cross-member string deduplication via family string table
 *   Huffman:   Per-position canonical Huffman + cost-model pruning
 *   Arithmetic: Fractional-bit entropy measurement (Shannon limit proof)
 *
 * See lambda_tensors.h for public API.
 * No SQLite. No external dependencies beyond libc + libm.
 */

#include "lambda_tensors.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdio.h>

#define DELTA_OP_LITERAL    0
#define DELTA_OP_WINDOW     1
#define DELTA_OP_REF_FIELD  2
#define DELTA_OP_PRIMED     3
#define DELTA_OP_FREQ_BASE  16
#define DELTA_FREQ_SLOTS    8

#define DELTA_WINDOW_SLOTS  16
#define PRIMED_DICT_MAX     32

#define DELTA_MASK_RAW      0
#define DELTA_MASK_SPARSE   1

typedef struct {
    const unsigned char *ptr;
    size_t len;
} EncodedValue;

typedef struct {
    const unsigned char *ptr;
    size_t len;
    int freq;
    int src_index;
} PrimedEntry;

typedef struct {
    size_t off;
    size_t len;
} WindowEntry;

/* ================================================================
 * Varint encoding (unsigned, 7-bit continuation, little-endian)
 * ================================================================ */

static int varint_encode(unsigned long long val, unsigned char *buf, size_t max) {
    int i = 0;
    while (val >= 0x80 && (size_t)i < max - 1) {
        buf[i++] = (unsigned char)((val & 0x7F) | 0x80);
        val >>= 7;
    }
    if ((size_t)i < max) buf[i++] = (unsigned char)(val & 0x7F);
    return i;
}

static int varint_decode(const unsigned char *buf, size_t max,
                         unsigned long long *val) {
    *val = 0;
    int shift = 0;
    int i = 0;
    while ((size_t)i < max) {
        unsigned long long b = buf[i];
        *val |= (b & 0x7F) << shift;
        i++;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    return i;
}

/* Signed varint: zigzag encoding */
static unsigned long long zigzag_encode(long long val) {
    return (unsigned long long)((val << 1) ^ (val >> 63));
}

static long long zigzag_decode(unsigned long long val) {
    return (long long)((val >> 1) ^ -(long long)(val & 1));
}

/* ================================================================
 * Layer 1+2: JSON binding array → compact binary
 * ================================================================
 *
 * Format: [count:varint] { [type:1byte] [data...] }*
 *
 * Type bytes: LT_NULL(0), LT_BOOL_F(1), LT_BOOL_T(2), LT_INT(3),
 *             LT_DOUBLE(4), LT_STRING(5), LT_NESTED(6)
 */

static int grow_text_buf(char **buf, size_t *cap, size_t need) {
    if (!buf || !cap) return -1;
    while (need + 1 > *cap) {
        size_t next = (*cap == 0) ? 64 : (*cap * 2);
        char *tmp = realloc(*buf, next);
        if (!tmp) return -1;
        *buf = tmp;
        *cap = next;
    }
    return 0;
}

static int append_text_char(char **buf, size_t *len, size_t *cap, char ch) {
    if (grow_text_buf(buf, cap, *len + 1) != 0) return -1;
    (*buf)[(*len)++] = ch;
    (*buf)[*len] = '\0';
    return 0;
}

static int append_text_bytes(char **buf, size_t *len, size_t *cap,
                             const char *src, size_t src_len) {
    if (grow_text_buf(buf, cap, *len + src_len) != 0) return -1;
    memcpy(*buf + *len, src, src_len);
    *len += src_len;
    (*buf)[*len] = '\0';
    return 0;
}

/* Internal parser: extract next JSON value from position p.
 * Returns type tag and allocates raw value text.
 * Advances *pp past the consumed value. */
static int parse_next_value(const char **pp, char **out_val) {
    const char *p = *pp;
    char *buf = NULL;
    size_t len = 0, cap = 0;

    if (!out_val) return -1;
    *out_val = NULL;

    /* skip whitespace/commas */
    while (*p && (*p == ' ' || *p == ',' || *p == '\n' || *p == '\t')) p++;
    if (!*p || *p == ']') { *pp = p; return -1; }

    if (*p == 'n' && strncmp(p, "null", 4) == 0) {
        *pp = p + 4;
        *out_val = strdup("");
        return LT_NULL;
    }
    if (*p == 't' && strncmp(p, "true", 4) == 0) {
        *pp = p + 4;
        *out_val = strdup("");
        return LT_BOOL_T;
    }
    if (*p == 'f' && strncmp(p, "false", 5) == 0) {
        *pp = p + 5;
        *out_val = strdup("");
        return LT_BOOL_F;
    }
    if (*p == '"') {
        /* String */
        p++;
        while (*p && *p != '"') {
            if (*p == '\\' && *(p + 1)) {
                if (append_text_char(&buf, &len, &cap, *p++) != 0) { free(buf); return -1; }
                if (append_text_char(&buf, &len, &cap, *p++) != 0) { free(buf); return -1; }
                continue;
            }
            if (append_text_char(&buf, &len, &cap, *p++) != 0) { free(buf); return -1; }
        }
        if (*p == '"') p++;
        *pp = p;
        if (!buf) {
            buf = strdup("");
            if (!buf) return -1;
        }
        *out_val = buf;
        return LT_STRING;
    }
    if (*p == '{' || *p == '[') {
        /* Nested object or array */
        char open = *p, close = (open == '{') ? '}' : ']';
        int depth = 1;
        if (append_text_char(&buf, &len, &cap, *p++) != 0) { free(buf); return -1; }
        while (*p && depth > 0) {
            if (*p == '"') {
                if (append_text_char(&buf, &len, &cap, *p++) != 0) { free(buf); return -1; }
                while (*p && *p != '"') {
                    if (*p == '\\' && *(p + 1)) {
                        if (append_text_char(&buf, &len, &cap, *p++) != 0) { free(buf); return -1; }
                    }
                    if (append_text_char(&buf, &len, &cap, *p++) != 0) { free(buf); return -1; }
                }
                if (*p == '"' && append_text_char(&buf, &len, &cap, *p++) != 0) { free(buf); return -1; }
                continue;
            }
            if (*p == open) depth++;
            if (*p == close) depth--;
            if (append_text_char(&buf, &len, &cap, *p++) != 0) { free(buf); return -1; }
        }
        *pp = p;
        if (!buf) return -1;
        *out_val = buf;
        return LT_NESTED;
    }

    /* Number: integer or float */
    {
        const char *start = p;
        int has_dot = 0, has_e = 0;
        while (*p && *p != ',' && *p != ']' && *p != ' ') {
            if (*p == '.') has_dot = 1;
            if (*p == 'e' || *p == 'E') has_e = 1;
            p++;
        }
        if (append_text_bytes(&buf, &len, &cap, start, (size_t)(p - start)) != 0) { free(buf); return -1; }
        *pp = p;
        *out_val = buf;

        if (has_dot || has_e) return LT_DOUBLE;
        return LT_INT;
    }
}

/* Ensure output buffer has room for `need` more bytes */
static int ensure_cap(unsigned char **buf, size_t *cap, size_t used, size_t need) {
    while (used + need >= *cap) {
        *cap *= 2;
        unsigned char *tmp = realloc(*buf, *cap);
        if (!tmp) return -1;
        *buf = tmp;
    }
    return 0;
}

int lt_encode_v1(const char *json_bindings,
                   unsigned char **out, size_t *out_len) {
    if (!json_bindings || !out || !out_len) return -1;

    const char *p = json_bindings;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    /* First pass: parse values into temp storage */
    typedef struct { int type; char *val; } TmpVal;
    int max_vals = 256;
    TmpVal *vals = calloc((size_t)max_vals, sizeof(TmpVal));
    if (!vals) return -1;
    int count = 0;

    while (*p && *p != ']') {
        if (count >= max_vals) {
            max_vals *= 2;
            TmpVal *nv = realloc(vals, (size_t)max_vals * sizeof(TmpVal));
            if (!nv) { free(vals); return -1; }
            vals = nv;
        }
        int t = parse_next_value(&p, &vals[count].val);
        if (t < 0) break;
        vals[count].type = t;
        count++;
    }

    /* Second pass: encode to binary */
    size_t cap = 1024;
    unsigned char *buf = malloc(cap);
    if (!buf) {
        for (int i = 0; i < count; i++) free(vals[i].val);
        free(vals);
        return -1;
    }
    size_t off = 0;

    /* Header: field count as varint */
    unsigned char vbuf[16];
    int vlen = varint_encode((unsigned long long)count, vbuf, sizeof(vbuf));
    if (ensure_cap(&buf, &cap, off, (size_t)vlen) < 0) {
        free(buf);
        for (int i = 0; i < count; i++) free(vals[i].val);
        free(vals);
        return -1;
    }
    memcpy(buf + off, vbuf, (size_t)vlen);
    off += (size_t)vlen;

    for (int i = 0; i < count; i++) {
        int t = vals[i].type;

        switch (t) {
        case LT_NULL:
        case LT_BOOL_F:
        case LT_BOOL_T:
            /* Just the type byte */
            if (ensure_cap(&buf, &cap, off, 1) < 0) {
                free(buf);
                for (int j = 0; j < count; j++) free(vals[j].val);
                free(vals);
                return -1;
            }
            buf[off++] = (unsigned char)t;
            break;

        case LT_INT: {
            long long iv = strtoll(vals[i].val, NULL, 10);
            unsigned long long zz = zigzag_encode(iv);
            vlen = varint_encode(zz, vbuf, sizeof(vbuf));
            if (ensure_cap(&buf, &cap, off, 1 + (size_t)vlen) < 0) {
                free(buf);
                for (int j = 0; j < count; j++) free(vals[j].val);
                free(vals);
                return -1;
            }
            buf[off++] = LT_INT;
            memcpy(buf + off, vbuf, (size_t)vlen);
            off += (size_t)vlen;
            break;
        }

        case LT_DOUBLE: {
            double dv = strtod(vals[i].val, NULL);
            if (ensure_cap(&buf, &cap, off, 9) < 0) {
                free(buf);
                for (int j = 0; j < count; j++) free(vals[j].val);
                free(vals);
                return -1;
            }
            buf[off++] = LT_DOUBLE;
            memcpy(buf + off, &dv, 8);
            off += 8;
            break;
        }

        case LT_STRING:
        case LT_NESTED: {
            size_t slen = strlen(vals[i].val);
            vlen = varint_encode((unsigned long long)slen, vbuf, sizeof(vbuf));
            if (ensure_cap(&buf, &cap, off, 1 + (size_t)vlen + slen) < 0) {
                free(buf);
                for (int j = 0; j < count; j++) free(vals[j].val);
                free(vals);
                return -1;
            }
            buf[off++] = (unsigned char)t;
            memcpy(buf + off, vbuf, (size_t)vlen);
            off += (size_t)vlen;
            memcpy(buf + off, vals[i].val, slen);
            off += slen;
            break;
        }
        }
    }

    for (int i = 0; i < count; i++) free(vals[i].val);
    free(vals);
    *out = buf;
    *out_len = off;
    return count;
}

int lt_decode_v1(const unsigned char *packed, size_t packed_len,
                   char **out_json) {
    if (!packed || !out_json) return -1;

    size_t pos = 0;
    unsigned long long count_ull;
    int vlen = varint_decode(packed + pos, packed_len - pos, &count_ull);
    pos += (size_t)vlen;
    int count = (int)count_ull;

    size_t cap = 4096;
    char *out = malloc(cap);
    if (!out) return -1;
    size_t off = 0;
    out[off++] = '[';

    for (int i = 0; i < count && pos < packed_len; i++) {
        if (i > 0) {
            while (off + 2 >= cap) { cap *= 2; out = realloc(out, cap); }
            out[off++] = ',';
        }

        unsigned char t = packed[pos++];

        switch (t) {
        case LT_NULL:
            while (off + 5 >= cap) { cap *= 2; out = realloc(out, cap); }
            memcpy(out + off, "null", 4); off += 4;
            break;

        case LT_BOOL_F:
            while (off + 6 >= cap) { cap *= 2; out = realloc(out, cap); }
            memcpy(out + off, "false", 5); off += 5;
            break;

        case LT_BOOL_T:
            while (off + 5 >= cap) { cap *= 2; out = realloc(out, cap); }
            memcpy(out + off, "true", 4); off += 4;
            break;

        case LT_INT: {
            unsigned long long zz;
            vlen = varint_decode(packed + pos, packed_len - pos, &zz);
            pos += (size_t)vlen;
            long long iv = zigzag_decode(zz);
            while (off + 24 >= cap) { cap *= 2; out = realloc(out, cap); }
            off += (size_t)snprintf(out + off, cap - off, "%lld", iv);
            break;
        }

        case LT_DOUBLE: {
            if (pos + 8 > packed_len) { free(out); return -1; }
            double dv;
            memcpy(&dv, packed + pos, 8);
            pos += 8;
            while (off + 32 >= cap) { cap *= 2; out = realloc(out, cap); }
            if (dv == (double)(long long)dv)
                off += (size_t)snprintf(out + off, cap - off, "%.1f", dv);
            else
                off += (size_t)snprintf(out + off, cap - off, "%.6f", dv);
            break;
        }

        case LT_STRING: {
            unsigned long long slen_ull;
            vlen = varint_decode(packed + pos, packed_len - pos, &slen_ull);
            pos += (size_t)vlen;
            size_t slen = (size_t)slen_ull;
            if (pos + slen > packed_len) { free(out); return -1; }
            while (off + slen + 4 >= cap) { cap *= 2; out = realloc(out, cap); }
            out[off++] = '"';
            memcpy(out + off, packed + pos, slen);
            off += slen;
            out[off++] = '"';
            pos += slen;
            break;
        }

        case LT_NESTED: {
            unsigned long long slen_ull;
            vlen = varint_decode(packed + pos, packed_len - pos, &slen_ull);
            pos += (size_t)vlen;
            size_t slen = (size_t)slen_ull;
            if (pos + slen > packed_len) { free(out); return -1; }
            while (off + slen + 2 >= cap) { cap *= 2; out = realloc(out, cap); }
            memcpy(out + off, packed + pos, slen);
            off += slen;
            pos += slen;
            break;
        }

        default:
            free(out);
            return -1;
        }
    }

    while (off + 2 >= cap) { cap *= 2; out = realloc(out, cap); }
    out[off++] = ']';
    out[off] = '\0';
    *out_json = out;
    return count;
}

/* ================================================================
 * Layer 3: Delta encoding
 * ================================================================
 *
 * Delta format:
 *   [count:varint]                     — total field count
 *   [bitmask: ceil(count/8) bytes]     — 1=changed, 0=same as ref
 *   For each set bit: [type:1][data...]  (same encoding as Layer 1+2)
 *
 * Comparison is type-aware: same type + same bytes = unchanged.
 */

/* Skip one encoded value at position pos, return bytes consumed */
static size_t skip_value(const unsigned char *buf, size_t len, size_t pos) {
    if (pos >= len) return 0;
    unsigned char t = buf[pos];
    size_t start = pos;
    pos++;

    switch (t) {
    case LT_NULL:
    case LT_BOOL_F:
    case LT_BOOL_T:
        break;

    case LT_INT: {
        while (pos < len && (buf[pos - 1] & 0x80)) pos++;
        /* varint_decode consumes until high bit clear */
        unsigned long long dummy;
        int vl = varint_decode(buf + start + 1, len - start - 1, &dummy);
        pos = start + 1 + (size_t)vl;
        break;
    }

    case LT_DOUBLE:
        pos += 8;
        break;

    case LT_STRING:
    case LT_NESTED: {
        unsigned long long slen;
        int vl = varint_decode(buf + pos, len - pos, &slen);
        pos += (size_t)vl + (size_t)slen;
        break;
    }

    case LT_STRREF: {
        unsigned long long idx;
        int vl = varint_decode(buf + pos, len - pos, &idx);
        (void)idx;
        pos += (size_t)vl;
        break;
    }

    case LT_FAMSTR: {
        unsigned long long idx;
        int vl = varint_decode(buf + pos, len - pos, &idx);
        (void)idx;
        pos += (size_t)vl;
        break;
    }

    case LT_SMALL_INT:
        pos += 1;
        break;

    case LT_FLOAT32:
        pos += 4;
        break;

    case LT_EMPTY_STR:
        break;
    }
    return pos - start;
}

static int varint_size(unsigned long long val) {
    int n = 1;
    while (val >= 0x80) {
        val >>= 7;
        n++;
    }
    return n;
}

static int extract_values(const unsigned char *buf, size_t buf_len,
                          EncodedValue **out_vals, int *out_count) {
    if (!buf || !out_vals || !out_count) return -1;

    unsigned long long count_ull = 0;
    int hdr = varint_decode(buf, buf_len, &count_ull);
    if (hdr <= 0) return -1;

    int count = (int)count_ull;
    EncodedValue *vals = calloc((size_t)(count > 0 ? count : 1), sizeof(*vals));
    if (!vals) return -1;

    size_t pos = (size_t)hdr;
    for (int i = 0; i < count; i++) {
        if (pos >= buf_len) {
            free(vals);
            return -1;
        }
        size_t sv = skip_value(buf, buf_len, pos);
        if (sv == 0 || pos + sv > buf_len) {
            free(vals);
            return -1;
        }
        vals[i].ptr = buf + pos;
        vals[i].len = sv;
        pos += sv;
    }

    *out_vals = vals;
    *out_count = count;
    return 0;
}

static int is_primable_value(const EncodedValue *v) {
    if (!v || !v->ptr || v->len == 0) return 0;
    return v->ptr[0] == LT_STRING || v->ptr[0] == LT_NESTED;
}

static int same_value(const EncodedValue *a, const EncodedValue *b) {
    if (!a || !b || !a->ptr || !b->ptr) return 0;
    return a->len == b->len && memcmp(a->ptr, b->ptr, a->len) == 0;
}

static int build_primed_dict(const EncodedValue *ref_vals, int ref_count,
                             PrimedEntry *dict, int max_dict) {
    int n = 0;

    for (int i = 0; i < ref_count; i++) {
        if (!is_primable_value(&ref_vals[i])) continue;

        int found = -1;
        for (int j = 0; j < n; j++) {
            if (dict[j].len == ref_vals[i].len &&
                memcmp(dict[j].ptr, ref_vals[i].ptr, ref_vals[i].len) == 0) {
                found = j;
                break;
            }
        }

        if (found >= 0) {
            dict[found].freq++;
            continue;
        }

        if (n < max_dict) {
            dict[n].ptr = ref_vals[i].ptr;
            dict[n].len = ref_vals[i].len;
            dict[n].freq = 1;
            dict[n].src_index = i;
            n++;
            continue;
        }

        int min_idx = 0;
        for (int j = 1; j < n; j++) {
            if (dict[j].freq < dict[min_idx].freq) min_idx = j;
        }
        if (dict[min_idx].freq < 1) {
            dict[min_idx].ptr = ref_vals[i].ptr;
            dict[min_idx].len = ref_vals[i].len;
            dict[min_idx].freq = 1;
            dict[min_idx].src_index = i;
        }
    }

    for (int i = 0; i < n; i++) {
        for (int j = i + 1; j < n; j++) {
            if (dict[j].freq > dict[i].freq) {
                PrimedEntry tmp = dict[i];
                dict[i] = dict[j];
                dict[j] = tmp;
            }
        }
    }

    return n;
}

static int find_ref_match(const EncodedValue *ref_vals, int ref_count,
                          const EncodedValue *target, int cur_idx) {
    if (!target || !target->ptr) return -1;
    for (int i = 0; i < ref_count; i++) {
        if (i == cur_idx) continue;
        if (same_value(&ref_vals[i], target)) return i;
    }
    return -1;
}

static int find_primed_match(const PrimedEntry *dict, int dict_count,
                             const EncodedValue *target) {
    if (!target || !target->ptr) return -1;
    for (int i = 0; i < dict_count; i++) {
        if (dict[i].len == target->len &&
            memcmp(dict[i].ptr, target->ptr, target->len) == 0) {
            return i;
        }
    }
    return -1;
}

static int build_hot_refs(const EncodedValue *ref_vals, int ref_count,
                          PrimedEntry *dict, int max_dict) {
    int n = 0;

    for (int i = 0; i < ref_count; i++) {
        int found = -1;
        if (!ref_vals[i].ptr || ref_vals[i].len == 0) continue;

        for (int j = 0; j < n; j++) {
            if (dict[j].len == ref_vals[i].len &&
                memcmp(dict[j].ptr, ref_vals[i].ptr, ref_vals[i].len) == 0) {
                found = j;
                break;
            }
        }

        if (found >= 0) {
            dict[found].freq++;
            continue;
        }

        if (n < max_dict) {
            dict[n].ptr = ref_vals[i].ptr;
            dict[n].len = ref_vals[i].len;
            dict[n].freq = 1;
            dict[n].src_index = i;
            n++;
        }
    }

    for (int i = 0; i < n; i++) {
        for (int j = i + 1; j < n; j++) {
            if (dict[j].freq > dict[i].freq ||
                (dict[j].freq == dict[i].freq && dict[j].len > dict[i].len)) {
                PrimedEntry tmp = dict[i];
                dict[i] = dict[j];
                dict[j] = tmp;
            }
        }
    }

    {
        int out = 0;
        for (int i = 0; i < n; i++) {
            if (dict[i].freq < 2) continue;
            if (out != i) dict[out] = dict[i];
            out++;
        }
        if (out > max_dict) out = max_dict;
        return out;
    }
}

int lt_delta_encode_v1(const unsigned char *ref, size_t ref_len,
                         const unsigned char *target, size_t target_len,
                         unsigned char **delta, size_t *delta_len) {
    if (!ref || !target || !delta || !delta_len) return -1;

    EncodedValue *ref_vals = NULL, *tgt_vals = NULL;
    int ref_count = 0, tgt_count = 0;
    if (extract_values(ref, ref_len, &ref_vals, &ref_count) != 0) return -1;
    if (extract_values(target, target_len, &tgt_vals, &tgt_count) != 0) {
        free(ref_vals);
        return -1;
    }

    int count = tgt_count;
    int bitmask_bytes = (count + 7) / 8;
    PrimedEntry primed[PRIMED_DICT_MAX];
    int primed_count = build_primed_dict(ref_vals, ref_count, primed, PRIMED_DICT_MAX);

    /* Build bitmask and collect changed values */
    unsigned char *mask = calloc((size_t)bitmask_bytes, 1);
    if (!mask) { free(ref_vals); free(tgt_vals); return -1; }

    /* Temp buffer for changed values */
    size_t chg_cap = 1024;
    unsigned char *chg = malloc(chg_cap);
    if (!chg) { free(mask); free(ref_vals); free(tgt_vals); return -1; }
    size_t chg_off = 0;
    EncodedValue window[DELTA_WINDOW_SLOTS];
    int window_count = 0;

    for (int i = 0; i < DELTA_WINDOW_SLOTS; i++) {
        window[i].ptr = NULL;
        window[i].len = 0;
    }

    for (int i = 0; i < count; i++) {
        EncodedValue rv = {0}, tv = {0};
        if (i < ref_count) rv = ref_vals[i];
        if (i < tgt_count) tv = tgt_vals[i];

        int same = same_value(&rv, &tv);

        if (!same) {
            unsigned char op = DELTA_OP_LITERAL;
            unsigned long long operand = 0;
            size_t payload_len = tv.len;
            int best_size = 1 + (int)payload_len;

            for (int w = 0; w < window_count; w++) {
                if (same_value(&window[w], &tv)) {
                    int cand = 1 + varint_size((unsigned long long)(w + 1));
                    if (cand < best_size) {
                        best_size = cand;
                        op = DELTA_OP_WINDOW;
                        operand = (unsigned long long)(w + 1);
                    }
                    break;
                }
            }

            int ref_match = find_ref_match(ref_vals, ref_count, &tv, i);
            if (ref_match >= 0) {
                int cand = 1 + varint_size((unsigned long long)ref_match);
                if (cand < best_size) {
                    best_size = cand;
                    op = DELTA_OP_REF_FIELD;
                    operand = (unsigned long long)ref_match;
                }
            }

            int primed_match = find_primed_match(primed, primed_count, &tv);
            if (primed_match >= 0) {
                int cand = 1 + varint_size((unsigned long long)primed_match);
                if (cand < best_size) {
                    best_size = cand;
                    op = DELTA_OP_PRIMED;
                    operand = (unsigned long long)primed_match;
                }
            }

            mask[i / 8] |= (unsigned char)(1 << (i % 8));
            if (op == DELTA_OP_LITERAL) {
                while (chg_off + 1 + payload_len >= chg_cap) {
                    chg_cap *= 2;
                    unsigned char *nc = realloc(chg, chg_cap);
                    if (!nc) { free(mask); free(chg); free(ref_vals); free(tgt_vals); return -1; }
                    chg = nc;
                }
                chg[chg_off++] = op;
                memcpy(chg + chg_off, tv.ptr, payload_len);
                chg_off += payload_len;
            } else {
                unsigned char opbuf[16];
                int oplen = varint_encode(operand, opbuf, sizeof(opbuf));
                while (chg_off + 1 + (size_t)oplen >= chg_cap) {
                    chg_cap *= 2;
                    unsigned char *nc = realloc(chg, chg_cap);
                    if (!nc) { free(mask); free(chg); free(ref_vals); free(tgt_vals); return -1; }
                    chg = nc;
                }
                chg[chg_off++] = op;
                memcpy(chg + chg_off, opbuf, (size_t)oplen);
                chg_off += (size_t)oplen;
            }
        }

        if (tv.ptr && tv.len > 0) {
            if (window_count < DELTA_WINDOW_SLOTS) {
                for (int w = window_count; w > 0; w--) window[w] = window[w - 1];
                window[0] = tv;
                window_count++;
            } else {
                for (int w = DELTA_WINDOW_SLOTS - 1; w > 0; w--) window[w] = window[w - 1];
                window[0] = tv;
            }
        }
    }

    /* Assemble delta: count varint + bitmask + changed values */
    size_t out_cap = 16 + (size_t)bitmask_bytes + chg_off;
    unsigned char *out = malloc(out_cap);
    if (!out) { free(mask); free(chg); free(ref_vals); free(tgt_vals); return -1; }
    size_t off = 0;

    unsigned char vbuf[16];
    int vl = varint_encode((unsigned long long)count, vbuf, sizeof(vbuf));
    memcpy(out + off, vbuf, (size_t)vl);
    off += (size_t)vl;

    memcpy(out + off, mask, (size_t)bitmask_bytes);
    off += (size_t)bitmask_bytes;

    memcpy(out + off, chg, chg_off);
    off += chg_off;

    free(mask);
    free(chg);
    free(ref_vals);
    free(tgt_vals);

    *delta = out;
    *delta_len = off;
    return count;
}

int lt_delta_decode_v1(const unsigned char *ref, size_t ref_len,
                         const unsigned char *delta, size_t delta_len,
                         unsigned char **out, size_t *out_len) {
    if (!ref || !delta || !out || !out_len) return -1;

    EncodedValue *ref_vals = NULL;
    int ref_count = 0;
    if (extract_values(ref, ref_len, &ref_vals, &ref_count) != 0) return -1;
    PrimedEntry primed[PRIMED_DICT_MAX];
    int primed_count = build_primed_dict(ref_vals, ref_count, primed, PRIMED_DICT_MAX);

    /* Read count from delta */
    size_t dpos = 0;
    unsigned long long count_ull;
    int vl = varint_decode(delta + dpos, delta_len - dpos, &count_ull);
    dpos += (size_t)vl;
    int count = (int)count_ull;
    int bitmask_bytes = (count + 7) / 8;

    if (dpos + (size_t)bitmask_bytes > delta_len) { free(ref_vals); return -1; }
    const unsigned char *mask = delta + dpos;
    dpos += (size_t)bitmask_bytes;

    /* Build output: start with header count */
    size_t cap = ref_len + delta_len;
    unsigned char *buf = malloc(cap);
    if (!buf) { free(ref_vals); return -1; }
    size_t off = 0;
    WindowEntry window[DELTA_WINDOW_SLOTS];
    int window_count = 0;

    for (int i = 0; i < DELTA_WINDOW_SLOTS; i++) {
        window[i].off = 0;
        window[i].len = 0;
    }

    unsigned char vbuf[16];
    vl = varint_encode((unsigned long long)count, vbuf, sizeof(vbuf));
    memcpy(buf + off, vbuf, (size_t)vl);
    off += (size_t)vl;

    for (int i = 0; i < count; i++) {
        int changed = (mask[i / 8] >> (i % 8)) & 1;
        size_t value_off = off;
        size_t value_len = 0;

        if (changed) {
            if (dpos >= delta_len) { free(buf); free(ref_vals); return -1; }
            unsigned char op = delta[dpos++];

            if (op == DELTA_OP_LITERAL) {
                size_t sv = skip_value(delta, delta_len, dpos);
                if (sv == 0) { free(buf); free(ref_vals); return -1; }
                while (off + sv >= cap) {
                    cap *= 2;
                    buf = realloc(buf, cap);
                    if (!buf) { free(ref_vals); return -1; }
                }
                memcpy(buf + off, delta + dpos, sv);
                off += sv;
                dpos += sv;
                value_len = sv;
            } else if (op == DELTA_OP_WINDOW) {
                unsigned long long dist = 0;
                int vv = varint_decode(delta + dpos, delta_len - dpos, &dist);
                dpos += (size_t)vv;
                if (dist == 0 || dist > (unsigned long long)window_count) { free(buf); free(ref_vals); return -1; }
                WindowEntry src = window[(int)dist - 1];
                while (off + src.len >= cap) {
                    cap *= 2;
                    buf = realloc(buf, cap);
                    if (!buf) { free(ref_vals); return -1; }
                }
                memcpy(buf + off, buf + src.off, src.len);
                off += src.len;
                value_len = src.len;
            } else if (op == DELTA_OP_REF_FIELD) {
                unsigned long long idx = 0;
                int vv = varint_decode(delta + dpos, delta_len - dpos, &idx);
                dpos += (size_t)vv;
                if (idx >= (unsigned long long)ref_count || !ref_vals[idx].ptr) { free(buf); free(ref_vals); return -1; }
                while (off + ref_vals[idx].len >= cap) {
                    cap *= 2;
                    buf = realloc(buf, cap);
                    if (!buf) { free(ref_vals); return -1; }
                }
                memcpy(buf + off, ref_vals[idx].ptr, ref_vals[idx].len);
                off += ref_vals[idx].len;
                value_len = ref_vals[idx].len;
            } else if (op == DELTA_OP_PRIMED) {
                unsigned long long idx = 0;
                int vv = varint_decode(delta + dpos, delta_len - dpos, &idx);
                dpos += (size_t)vv;
                if (idx >= (unsigned long long)primed_count || !primed[idx].ptr) { free(buf); free(ref_vals); return -1; }
                while (off + primed[idx].len >= cap) {
                    cap *= 2;
                    buf = realloc(buf, cap);
                    if (!buf) { free(ref_vals); return -1; }
                }
                memcpy(buf + off, primed[idx].ptr, primed[idx].len);
                off += primed[idx].len;
                value_len = primed[idx].len;
            } else {
                free(buf);
                free(ref_vals);
                return -1;
            }
        } else {
            /* Copy value from reference */
            if (i < ref_count && ref_vals[i].ptr && ref_vals[i].len > 0) {
                while (off + ref_vals[i].len >= cap) {
                    cap *= 2;
                    buf = realloc(buf, cap);
                    if (!buf) { free(ref_vals); return -1; }
                }
                memcpy(buf + off, ref_vals[i].ptr, ref_vals[i].len);
                off += ref_vals[i].len;
                value_len = ref_vals[i].len;
            }
        }

        if (value_len > 0) {
            if (window_count < DELTA_WINDOW_SLOTS) {
                for (int w = window_count; w > 0; w--) window[w] = window[w - 1];
                window[0].off = value_off;
                window[0].len = value_len;
                window_count++;
            } else {
                for (int w = DELTA_WINDOW_SLOTS - 1; w > 0; w--) window[w] = window[w - 1];
                window[0].off = value_off;
                window[0].len = value_len;
            }
        }
    }

    free(ref_vals);
    *out = buf;
    *out_len = off;
    return count;
}

/* ================================================================
 * V2: Enhanced encoding / delta coding
 * ================================================================ */

int lt_encode_v2(const char *json_bindings,
                      unsigned char **out, size_t *out_len) {
    if (!json_bindings || !out || !out_len) return -1;

    const char *p = json_bindings;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    typedef struct { int type; char *val; } TmpVal;
    int max_vals = 256;
    TmpVal *vals = calloc((size_t)max_vals, sizeof(TmpVal));
    const char **seen_strings = NULL;
    int seen_count = 0;
    int count = 0;
    size_t cap = 1024;
    unsigned char *buf = NULL;
    size_t off = 0;

    if (!vals) return -1;

    while (*p && *p != ']') {
        if (count >= max_vals) {
            max_vals *= 2;
            TmpVal *nv = realloc(vals, (size_t)max_vals * sizeof(TmpVal));
            if (!nv) { free(vals); return -1; }
            vals = nv;
        }
        {
            int t = parse_next_value(&p, &vals[count].val);
            if (t < 0) break;
            vals[count].type = t;
            count++;
        }
    }

    seen_strings = calloc((size_t)(count > 0 ? count : 1), sizeof(*seen_strings));
    buf = malloc(cap);
    if (!seen_strings || !buf) {
        for (int i = 0; i < count; i++) free(vals[i].val);
        free(vals);
        free(seen_strings);
        free(buf);
        return -1;
    }

    {
        unsigned char vbuf[16];
        int vlen = varint_encode((unsigned long long)count, vbuf, sizeof(vbuf));
        if (ensure_cap(&buf, &cap, off, (size_t)vlen) < 0) {
            for (int i = 0; i < count; i++) free(vals[i].val);
            free(vals);
            free(seen_strings);
            free(buf);
            return -1;
        }
        memcpy(buf + off, vbuf, (size_t)vlen);
        off += (size_t)vlen;
    }

    for (int i = 0; i < count; i++) {
        int t = vals[i].type;

        switch (t) {
        case LT_NULL:
        case LT_BOOL_F:
        case LT_BOOL_T:
            if (ensure_cap(&buf, &cap, off, 1) < 0) {
                for (int j = 0; j < count; j++) free(vals[j].val);
                free(vals); free(seen_strings); free(buf); return -1;
            }
            buf[off++] = (unsigned char)t;
            break;

        case LT_INT: {
            long long iv = strtoll(vals[i].val, NULL, 10);
            if (iv >= -64 && iv <= 63) {
                if (ensure_cap(&buf, &cap, off, 2) < 0) {
                    for (int j = 0; j < count; j++) free(vals[j].val);
                    free(vals); free(seen_strings); free(buf); return -1;
                }
                buf[off++] = LT_SMALL_INT;
                buf[off++] = (unsigned char)(iv + 64);
            } else {
                unsigned char vbuf[16];
                unsigned long long zz = zigzag_encode(iv);
                int vlen = varint_encode(zz, vbuf, sizeof(vbuf));
                if (ensure_cap(&buf, &cap, off, 1 + (size_t)vlen) < 0) {
                    for (int j = 0; j < count; j++) free(vals[j].val);
                    free(vals); free(seen_strings); free(buf); return -1;
                }
                buf[off++] = LT_INT;
                memcpy(buf + off, vbuf, (size_t)vlen);
                off += (size_t)vlen;
            }
            break;
        }

        case LT_DOUBLE: {
            double dv = strtod(vals[i].val, NULL);
            float fv = (float)dv;
            if ((double)fv == dv) {
                if (ensure_cap(&buf, &cap, off, 5) < 0) {
                    for (int j = 0; j < count; j++) free(vals[j].val);
                    free(vals); free(seen_strings); free(buf); return -1;
                }
                buf[off++] = LT_FLOAT32;
                memcpy(buf + off, &fv, 4);
                off += 4;
            } else {
                if (ensure_cap(&buf, &cap, off, 9) < 0) {
                    for (int j = 0; j < count; j++) free(vals[j].val);
                    free(vals); free(seen_strings); free(buf); return -1;
                }
                buf[off++] = LT_DOUBLE;
                memcpy(buf + off, &dv, 8);
                off += 8;
            }
            break;
        }

        case LT_STRING: {
            size_t slen = strlen(vals[i].val);
            if (slen == 0) {
                if (ensure_cap(&buf, &cap, off, 1) < 0) {
                    for (int j = 0; j < count; j++) free(vals[j].val);
                    free(vals); free(seen_strings); free(buf); return -1;
                }
                buf[off++] = LT_EMPTY_STR;
                break;
            }

            {
                int seen_idx = -1;
                for (int s = 0; s < seen_count; s++) {
                    if (strcmp(seen_strings[s], vals[i].val) == 0) {
                        seen_idx = s;
                        break;
                    }
                }

                if (seen_idx >= 0) {
                    unsigned char vbuf[16];
                    int vlen = varint_encode((unsigned long long)seen_idx, vbuf, sizeof(vbuf));
                    if (ensure_cap(&buf, &cap, off, 1 + (size_t)vlen) < 0) {
                        for (int j = 0; j < count; j++) free(vals[j].val);
                        free(vals); free(seen_strings); free(buf); return -1;
                    }
                    buf[off++] = LT_STRREF;
                    memcpy(buf + off, vbuf, (size_t)vlen);
                    off += (size_t)vlen;
                } else {
                    unsigned char vbuf[16];
                    int vlen = varint_encode((unsigned long long)slen, vbuf, sizeof(vbuf));
                    if (ensure_cap(&buf, &cap, off, 1 + (size_t)vlen + slen) < 0) {
                        for (int j = 0; j < count; j++) free(vals[j].val);
                        free(vals); free(seen_strings); free(buf); return -1;
                    }
                    buf[off++] = LT_STRING;
                    memcpy(buf + off, vbuf, (size_t)vlen);
                    off += (size_t)vlen;
                    memcpy(buf + off, vals[i].val, slen);
                    off += slen;
                    seen_strings[seen_count++] = vals[i].val;
                }
            }
            break;
        }

        case LT_NESTED: {
            size_t slen = strlen(vals[i].val);
            unsigned char vbuf[16];
            int vlen = varint_encode((unsigned long long)slen, vbuf, sizeof(vbuf));
            if (ensure_cap(&buf, &cap, off, 1 + (size_t)vlen + slen) < 0) {
                for (int j = 0; j < count; j++) free(vals[j].val);
                free(vals); free(seen_strings); free(buf); return -1;
            }
            buf[off++] = LT_NESTED;
            memcpy(buf + off, vbuf, (size_t)vlen);
            off += (size_t)vlen;
            memcpy(buf + off, vals[i].val, slen);
            off += slen;
            break;
        }
        }
    }

    for (int i = 0; i < count; i++) free(vals[i].val);
    free(vals);
    free(seen_strings);
    *out = buf;
    *out_len = off;
    return count;
}

static int lt_decode_v2_impl(const unsigned char *packed, size_t packed_len,
                                  const LtStringTable *strtab,
                                  char **out_json) {
    if (!packed || !out_json) return -1;

    size_t pos = 0;
    unsigned long long count_ull = 0;
    int vlen = varint_decode(packed + pos, packed_len - pos, &count_ull);
    int count = (int)count_ull;
    size_t cap = 4096;
    size_t off = 0;
    char *out = NULL;
    char **seen_strings = NULL;
    int seen_count = 0;

    pos += (size_t)vlen;
    out = malloc(cap);
    seen_strings = calloc((size_t)(count > 0 ? count : 1), sizeof(*seen_strings));
    if (!out || !seen_strings) {
        free(out);
        free(seen_strings);
        return -1;
    }

    out[off++] = '[';

    for (int i = 0; i < count && pos < packed_len; i++) {
        if (i > 0) {
            while (off + 2 >= cap) {
                cap *= 2;
                out = realloc(out, cap);
                if (!out) goto fail;
            }
            out[off++] = ',';
        }

        switch (packed[pos++]) {
        case LT_NULL:
            while (off + 5 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            memcpy(out + off, "null", 4); off += 4;
            break;

        case LT_BOOL_F:
            while (off + 6 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            memcpy(out + off, "false", 5); off += 5;
            break;

        case LT_BOOL_T:
            while (off + 5 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            memcpy(out + off, "true", 4); off += 4;
            break;

        case LT_SMALL_INT: {
            int iv = (int)packed[pos++] - 64;
            while (off + 24 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            off += (size_t)snprintf(out + off, cap - off, "%d", iv);
            break;
        }

        case LT_INT: {
            unsigned long long zz = 0;
            vlen = varint_decode(packed + pos, packed_len - pos, &zz);
            pos += (size_t)vlen;
            while (off + 24 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            off += (size_t)snprintf(out + off, cap - off, "%lld", zigzag_decode(zz));
            break;
        }

        case LT_FLOAT32: {
            float fv = 0.0f;
            if (pos + 4 > packed_len) goto fail;
            memcpy(&fv, packed + pos, 4);
            pos += 4;
            while (off + 32 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            off += (size_t)snprintf(out + off, cap - off, "%.9g", fv);
            break;
        }

        case LT_DOUBLE: {
            double dv = 0.0;
            if (pos + 8 > packed_len) goto fail;
            memcpy(&dv, packed + pos, 8);
            pos += 8;
            while (off + 32 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            if (dv == (double)(long long)dv)
                off += (size_t)snprintf(out + off, cap - off, "%.1f", dv);
            else
                off += (size_t)snprintf(out + off, cap - off, "%.6f", dv);
            break;
        }

        case LT_EMPTY_STR:
            while (off + 3 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            out[off++] = '"';
            out[off++] = '"';
            break;

        case LT_STRING: {
            unsigned long long slen_ull = 0;
            size_t slen;
            char *copy;
            vlen = varint_decode(packed + pos, packed_len - pos, &slen_ull);
            pos += (size_t)vlen;
            slen = (size_t)slen_ull;
            if (pos + slen > packed_len) goto fail;
            copy = malloc(slen + 1);
            if (!copy) goto fail;
            memcpy(copy, packed + pos, slen);
            copy[slen] = '\0';
            seen_strings[seen_count++] = copy;
            while (off + slen + 4 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            out[off++] = '"';
            memcpy(out + off, packed + pos, slen);
            off += slen;
            out[off++] = '"';
            pos += slen;
            break;
        }

        case LT_STRREF: {
            unsigned long long idx = 0;
            const char *s;
            size_t slen;
            vlen = varint_decode(packed + pos, packed_len - pos, &idx);
            pos += (size_t)vlen;
            if (idx >= (unsigned long long)seen_count || !seen_strings[idx]) goto fail;
            s = seen_strings[idx];
            slen = strlen(s);
            while (off + slen + 4 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            out[off++] = '"';
            memcpy(out + off, s, slen);
            off += slen;
            out[off++] = '"';
            break;
        }

        case LT_FAMSTR: {
            unsigned long long idx = 0;
            const char *s;
            size_t slen;
            vlen = varint_decode(packed + pos, packed_len - pos, &idx);
            pos += (size_t)vlen;
            if (!strtab || idx >= (unsigned long long)strtab->count ||
                !strtab->strings[idx]) goto fail;
            s = strtab->strings[idx];
            slen = strtab->lengths[idx];
            while (off + slen + 4 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            out[off++] = '"';
            memcpy(out + off, s, slen);
            off += slen;
            out[off++] = '"';
            break;
        }

        case LT_NESTED: {
            unsigned long long slen_ull = 0;
            size_t slen;
            vlen = varint_decode(packed + pos, packed_len - pos, &slen_ull);
            pos += (size_t)vlen;
            slen = (size_t)slen_ull;
            if (pos + slen > packed_len) goto fail;
            while (off + slen + 2 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
            memcpy(out + off, packed + pos, slen);
            off += slen;
            pos += slen;
            break;
        }

        default:
            goto fail;
        }
    }

    while (off + 2 >= cap) { cap *= 2; out = realloc(out, cap); if (!out) goto fail; }
    out[off++] = ']';
    out[off] = '\0';
    for (int i = 0; i < seen_count; i++) free(seen_strings[i]);
    free(seen_strings);
    *out_json = out;
    return count;

fail:
    if (seen_strings) {
        for (int i = 0; i < seen_count; i++) free(seen_strings[i]);
        free(seen_strings);
    }
    free(out);
    return -1;
}

int lt_decode_v2(const unsigned char *packed, size_t packed_len,
                      char **out_json) {
    return lt_decode_v2_impl(packed, packed_len, NULL, out_json);
}

int lt_decode_v2_interned(const unsigned char *packed, size_t packed_len,
                               const LtStringTable *strtab,
                               char **out_json) {
    return lt_decode_v2_impl(packed, packed_len, strtab, out_json);
}

int lt_delta_encode_v2(const unsigned char *ref, size_t ref_len,
                            const unsigned char *target, size_t target_len,
                            unsigned char **delta, size_t *delta_len) {
    if (!ref || !target || !delta || !delta_len) return -1;

    EncodedValue *ref_vals = NULL, *tgt_vals = NULL;
    int ref_count = 0, tgt_count = 0;
    PrimedEntry primed[PRIMED_DICT_MAX];
    PrimedEntry hot_refs[DELTA_FREQ_SLOTS];
    int primed_count, hot_count;
    unsigned char *mask = NULL;
    unsigned char *chg = NULL;
    EncodedValue window[DELTA_WINDOW_SLOTS];
    size_t chg_cap = 1024, chg_off = 0;
    int count, bitmask_bytes, changed_count = 0, window_count = 0;

    if (extract_values(ref, ref_len, &ref_vals, &ref_count) != 0) return -1;
    if (extract_values(target, target_len, &tgt_vals, &tgt_count) != 0) {
        free(ref_vals);
        return -1;
    }

    count = tgt_count;
    bitmask_bytes = (count + 7) / 8;
    primed_count = build_primed_dict(ref_vals, ref_count, primed, PRIMED_DICT_MAX);
    hot_count = build_hot_refs(ref_vals, ref_count, hot_refs, DELTA_FREQ_SLOTS);

    mask = calloc((size_t)(bitmask_bytes > 0 ? bitmask_bytes : 1), 1);
    chg = malloc(chg_cap);
    if (!mask || !chg) {
        free(ref_vals); free(tgt_vals); free(mask); free(chg); return -1;
    }

    for (int i = 0; i < DELTA_WINDOW_SLOTS; i++) {
        window[i].ptr = NULL;
        window[i].len = 0;
    }

    for (int i = 0; i < count; i++) {
        EncodedValue rv = {0}, tv = {0};
        if (i < ref_count) rv = ref_vals[i];
        if (i < tgt_count) tv = tgt_vals[i];

        if (!same_value(&rv, &tv)) {
            unsigned char op = DELTA_OP_LITERAL;
            unsigned long long operand = 0;
            size_t payload_len = tv.len;
            int best_size = 1 + (int)payload_len;

            for (int h = 0; h < hot_count; h++) {
                if (tv.ptr && hot_refs[h].len == tv.len &&
                    memcmp(hot_refs[h].ptr, tv.ptr, tv.len) == 0) {
                    op = (unsigned char)(DELTA_OP_FREQ_BASE + h);
                    best_size = 1;
                    break;
                }
            }

            if (best_size > 1) {
                for (int w = 0; w < window_count; w++) {
                    if (same_value(&window[w], &tv)) {
                        int cand = 1 + varint_size((unsigned long long)(w + 1));
                        if (cand < best_size) {
                            best_size = cand;
                            op = DELTA_OP_WINDOW;
                            operand = (unsigned long long)(w + 1);
                        }
                        break;
                    }
                }
            }

            {
                int ref_match = find_ref_match(ref_vals, ref_count, &tv, i);
                if (ref_match >= 0) {
                    int cand = 1 + varint_size((unsigned long long)ref_match);
                    if (cand < best_size) {
                        best_size = cand;
                        op = DELTA_OP_REF_FIELD;
                        operand = (unsigned long long)ref_match;
                    }
                }
            }

            {
                int primed_match = find_primed_match(primed, primed_count, &tv);
                if (primed_match >= 0) {
                    int cand = 1 + varint_size((unsigned long long)primed_match);
                    if (cand < best_size) {
                        best_size = cand;
                        op = DELTA_OP_PRIMED;
                        operand = (unsigned long long)primed_match;
                    }
                }
            }

            if (bitmask_bytes > 0) mask[i / 8] |= (unsigned char)(1 << (i % 8));
            changed_count++;

            if (op == DELTA_OP_LITERAL) {
                while (chg_off + 1 + payload_len >= chg_cap) {
                    chg_cap *= 2;
                    chg = realloc(chg, chg_cap);
                    if (!chg) { free(ref_vals); free(tgt_vals); free(mask); return -1; }
                }
                chg[chg_off++] = op;
                memcpy(chg + chg_off, tv.ptr, payload_len);
                chg_off += payload_len;
            } else if (op >= DELTA_OP_FREQ_BASE &&
                       op < (unsigned char)(DELTA_OP_FREQ_BASE + DELTA_FREQ_SLOTS)) {
                while (chg_off + 1 >= chg_cap) {
                    chg_cap *= 2;
                    chg = realloc(chg, chg_cap);
                    if (!chg) { free(ref_vals); free(tgt_vals); free(mask); return -1; }
                }
                chg[chg_off++] = op;
            } else {
                unsigned char vbuf[16];
                int vlen = varint_encode(operand, vbuf, sizeof(vbuf));
                while (chg_off + 1 + (size_t)vlen >= chg_cap) {
                    chg_cap *= 2;
                    chg = realloc(chg, chg_cap);
                    if (!chg) { free(ref_vals); free(tgt_vals); free(mask); return -1; }
                }
                chg[chg_off++] = op;
                memcpy(chg + chg_off, vbuf, (size_t)vlen);
                chg_off += (size_t)vlen;
            }
        }

        if (tv.ptr && tv.len > 0) {
            if (window_count < DELTA_WINDOW_SLOTS) {
                for (int w = window_count; w > 0; w--) window[w] = window[w - 1];
                window[0] = tv;
                window_count++;
            } else {
                for (int w = DELTA_WINDOW_SLOTS - 1; w > 0; w--) window[w] = window[w - 1];
                window[0] = tv;
            }
        }
    }

    {
        size_t raw_mask_sz = 1 + (size_t)bitmask_bytes;
        size_t sparse_mask_sz = 1 + (size_t)varint_size((unsigned long long)changed_count);
        unsigned char *out;
        size_t out_cap, off = 0;
        int use_sparse = 0;

        for (int i = 0; i < count; i++) {
            if (bitmask_bytes > 0 && (mask[i / 8] >> (i % 8) & 1))
                sparse_mask_sz += (size_t)varint_size((unsigned long long)i);
        }
        if (sparse_mask_sz < raw_mask_sz) use_sparse = 1;

        out_cap = 16 + (use_sparse ? sparse_mask_sz : raw_mask_sz) + chg_off;
        out = malloc(out_cap);
        if (!out) {
            free(ref_vals); free(tgt_vals); free(mask); free(chg); return -1;
        }

        {
            unsigned char vbuf[16];
            int vlen = varint_encode((unsigned long long)count, vbuf, sizeof(vbuf));
            memcpy(out + off, vbuf, (size_t)vlen);
            off += (size_t)vlen;
        }

        out[off++] = (unsigned char)(use_sparse ? DELTA_MASK_SPARSE : DELTA_MASK_RAW);
        if (use_sparse) {
            unsigned char vbuf[16];
            int vlen = varint_encode((unsigned long long)changed_count, vbuf, sizeof(vbuf));
            memcpy(out + off, vbuf, (size_t)vlen);
            off += (size_t)vlen;
            for (int i = 0; i < count; i++) {
                if (bitmask_bytes > 0 && (mask[i / 8] >> (i % 8) & 1)) {
                    vlen = varint_encode((unsigned long long)i, vbuf, sizeof(vbuf));
                    memcpy(out + off, vbuf, (size_t)vlen);
                    off += (size_t)vlen;
                }
            }
        } else if (bitmask_bytes > 0) {
            memcpy(out + off, mask, (size_t)bitmask_bytes);
            off += (size_t)bitmask_bytes;
        }

        memcpy(out + off, chg, chg_off);
        off += chg_off;

        free(ref_vals);
        free(tgt_vals);
        free(mask);
        free(chg);
        *delta = out;
        *delta_len = off;
    }

    return count;
}

int lt_delta_decode_v2(const unsigned char *ref, size_t ref_len,
                            const unsigned char *delta, size_t delta_len,
                            unsigned char **out, size_t *out_len) {
    if (!ref || !delta || !out || !out_len) return -1;

    EncodedValue *ref_vals = NULL;
    int ref_count = 0;
    PrimedEntry primed[PRIMED_DICT_MAX];
    PrimedEntry hot_refs[DELTA_FREQ_SLOTS];
    int primed_count, hot_count;
    size_t dpos = 0, off = 0, cap;
    unsigned long long count_ull = 0;
    int count, window_count = 0;
    unsigned char *buf = NULL;
    unsigned char *changed = NULL;
    WindowEntry window[DELTA_WINDOW_SLOTS];
    int vl;

    if (extract_values(ref, ref_len, &ref_vals, &ref_count) != 0) return -1;
    primed_count = build_primed_dict(ref_vals, ref_count, primed, PRIMED_DICT_MAX);
    hot_count = build_hot_refs(ref_vals, ref_count, hot_refs, DELTA_FREQ_SLOTS);

    vl = varint_decode(delta + dpos, delta_len - dpos, &count_ull);
    dpos += (size_t)vl;
    count = (int)count_ull;
    if (dpos >= delta_len) { free(ref_vals); return -1; }

    changed = calloc((size_t)(count > 0 ? count : 1), 1);
    cap = ref_len + delta_len + 32;
    buf = malloc(cap);
    if (!changed || !buf) {
        free(ref_vals); free(changed); free(buf); return -1;
    }

    {
        unsigned char mask_mode = delta[dpos++];
        if (mask_mode == DELTA_MASK_RAW) {
            int bitmask_bytes = (count + 7) / 8;
            if (dpos + (size_t)bitmask_bytes > delta_len) goto fail_v2_decode;
            for (int i = 0; i < count; i++) {
                changed[i] = (unsigned char)((delta[dpos + (size_t)(i / 8)] >> (i % 8)) & 1);
            }
            dpos += (size_t)bitmask_bytes;
        } else if (mask_mode == DELTA_MASK_SPARSE) {
            unsigned long long nchanged = 0;
            vl = varint_decode(delta + dpos, delta_len - dpos, &nchanged);
            dpos += (size_t)vl;
            for (unsigned long long i = 0; i < nchanged; i++) {
                unsigned long long idx = 0;
                vl = varint_decode(delta + dpos, delta_len - dpos, &idx);
                dpos += (size_t)vl;
                if (idx >= (unsigned long long)count) goto fail_v2_decode;
                changed[idx] = 1;
            }
        } else {
            goto fail_v2_decode;
        }
    }

    for (int i = 0; i < DELTA_WINDOW_SLOTS; i++) {
        window[i].off = 0;
        window[i].len = 0;
    }

    {
        unsigned char vbuf[16];
        int vlen = varint_encode((unsigned long long)count, vbuf, sizeof(vbuf));
        memcpy(buf + off, vbuf, (size_t)vlen);
        off += (size_t)vlen;
    }

    for (int i = 0; i < count; i++) {
        size_t value_off = off;
        size_t value_len = 0;

        if (changed[i]) {
            unsigned char op;
            if (dpos >= delta_len) goto fail_v2_decode;
            op = delta[dpos++];

            if (op == DELTA_OP_LITERAL) {
                size_t sv = skip_value(delta, delta_len, dpos);
                if (sv == 0) goto fail_v2_decode;
                while (off + sv >= cap) {
                    cap *= 2;
                    buf = realloc(buf, cap);
                    if (!buf) goto fail_v2_decode;
                }
                memcpy(buf + off, delta + dpos, sv);
                off += sv;
                dpos += sv;
                value_len = sv;
            } else if (op == DELTA_OP_WINDOW) {
                unsigned long long dist = 0;
                WindowEntry src;
                vl = varint_decode(delta + dpos, delta_len - dpos, &dist);
                dpos += (size_t)vl;
                if (dist == 0 || dist > (unsigned long long)window_count) goto fail_v2_decode;
                src = window[(int)dist - 1];
                while (off + src.len >= cap) {
                    cap *= 2;
                    buf = realloc(buf, cap);
                    if (!buf) goto fail_v2_decode;
                }
                memcpy(buf + off, buf + src.off, src.len);
                off += src.len;
                value_len = src.len;
            } else if (op == DELTA_OP_REF_FIELD) {
                unsigned long long idx = 0;
                vl = varint_decode(delta + dpos, delta_len - dpos, &idx);
                dpos += (size_t)vl;
                if (idx >= (unsigned long long)ref_count || !ref_vals[idx].ptr) goto fail_v2_decode;
                while (off + ref_vals[idx].len >= cap) {
                    cap *= 2;
                    buf = realloc(buf, cap);
                    if (!buf) goto fail_v2_decode;
                }
                memcpy(buf + off, ref_vals[idx].ptr, ref_vals[idx].len);
                off += ref_vals[idx].len;
                value_len = ref_vals[idx].len;
            } else if (op == DELTA_OP_PRIMED) {
                unsigned long long idx = 0;
                vl = varint_decode(delta + dpos, delta_len - dpos, &idx);
                dpos += (size_t)vl;
                if (idx >= (unsigned long long)primed_count || !primed[idx].ptr) goto fail_v2_decode;
                while (off + primed[idx].len >= cap) {
                    cap *= 2;
                    buf = realloc(buf, cap);
                    if (!buf) goto fail_v2_decode;
                }
                memcpy(buf + off, primed[idx].ptr, primed[idx].len);
                off += primed[idx].len;
                value_len = primed[idx].len;
            } else if (op >= DELTA_OP_FREQ_BASE &&
                       op < (unsigned char)(DELTA_OP_FREQ_BASE + hot_count)) {
                int idx = (int)(op - DELTA_OP_FREQ_BASE);
                if (!hot_refs[idx].ptr) goto fail_v2_decode;
                while (off + hot_refs[idx].len >= cap) {
                    cap *= 2;
                    buf = realloc(buf, cap);
                    if (!buf) goto fail_v2_decode;
                }
                memcpy(buf + off, hot_refs[idx].ptr, hot_refs[idx].len);
                off += hot_refs[idx].len;
                value_len = hot_refs[idx].len;
            } else {
                goto fail_v2_decode;
            }
        } else if (i < ref_count && ref_vals[i].ptr && ref_vals[i].len > 0) {
            while (off + ref_vals[i].len >= cap) {
                cap *= 2;
                buf = realloc(buf, cap);
                if (!buf) goto fail_v2_decode;
            }
            memcpy(buf + off, ref_vals[i].ptr, ref_vals[i].len);
            off += ref_vals[i].len;
            value_len = ref_vals[i].len;
        }

        if (value_len > 0) {
            if (window_count < DELTA_WINDOW_SLOTS) {
                for (int w = window_count; w > 0; w--) window[w] = window[w - 1];
                window[0].off = value_off;
                window[0].len = value_len;
                window_count++;
            } else {
                for (int w = DELTA_WINDOW_SLOTS - 1; w > 0; w--) window[w] = window[w - 1];
                window[0].off = value_off;
                window[0].len = value_len;
            }
        }
    }

    free(ref_vals);
    free(changed);
    *out = buf;
    *out_len = off;
    return count;

fail_v2_decode:
    free(ref_vals);
    free(changed);
    free(buf);
    return -1;
}

int lt_measure_v2(const char *json_bindings) {
    unsigned char *packed = NULL;
    size_t packed_len = 0;
    int n = lt_encode_v2(json_bindings, &packed, &packed_len);
    if (n < 0) return -1;
    free(packed);
    return (int)packed_len;
}

int lt_delta_measure_v2(const char *ref_json, const char *target_json) {
    unsigned char *ref_packed = NULL, *tgt_packed = NULL, *delta = NULL;
    size_t ref_len = 0, tgt_len = 0, delta_len = 0;

    if (lt_encode_v2(ref_json, &ref_packed, &ref_len) < 0) return -1;
    if (lt_encode_v2(target_json, &tgt_packed, &tgt_len) < 0) {
        free(ref_packed);
        return -1;
    }
    if (lt_delta_encode_v2(ref_packed, ref_len, tgt_packed, tgt_len,
                                &delta, &delta_len) < 0) {
        free(ref_packed);
        free(tgt_packed);
        return -1;
    }

    free(ref_packed);
    free(tgt_packed);
    free(delta);
    return (int)delta_len;
}

/* ================================================================
 * Convenience: measure sizes without allocating persistent output
 * ================================================================ */

int lt_measure_v1(const char *json_bindings) {
    unsigned char *packed = NULL;
    size_t packed_len = 0;
    int n = lt_encode_v1(json_bindings, &packed, &packed_len);
    if (n < 0) return -1;
    int result = (int)packed_len;
    free(packed);
    return result;
}

int lt_delta_measure_v1(const char *ref_json, const char *target_json) {
    unsigned char *ref_packed = NULL, *tgt_packed = NULL, *delta = NULL;
    size_t ref_len = 0, tgt_len = 0, delta_len = 0;

    if (lt_encode_v1(ref_json, &ref_packed, &ref_len) < 0) return -1;
    if (lt_encode_v1(target_json, &tgt_packed, &tgt_len) < 0) {
        free(ref_packed); return -1;
    }
    if (lt_delta_encode_v1(ref_packed, ref_len, tgt_packed, tgt_len,
                             &delta, &delta_len) < 0) {
        free(ref_packed); free(tgt_packed); return -1;
    }

    int result = (int)delta_len;
    free(ref_packed);
    free(tgt_packed);
    free(delta);
    return result;
}

/* ================================================================
 * Family String Table: cross-member string interning
 * ================================================================
 *
 * Scans all bindings in a family to build a frequency-sorted string table.
 * Strings that appear in 2+ members get an index. During encoding,
 * LT_FAMSTR + varint(index) replaces LT_STRING + length + bytes.
 *
 * Table overhead: stored once per family.
 * Per-member savings: every interned string costs 1+varint instead of
 *   1+varint+strlen bytes.
 */

typedef struct {
    char *str;
    size_t len;
    int freq;
} FamStrEntry;

/* ---- Strtab ingest context (replaces static globals for thread safety) ---- */

struct LtStrtabCtx {
    FamStrEntry *entries;
    int count;
    int capacity;
};

LtStrtabCtx *lt_strtab_ctx_new(void) {
    LtStrtabCtx *ctx = calloc(1, sizeof(LtStrtabCtx));
    return ctx;
}

void lt_strtab_ctx_free(LtStrtabCtx *ctx) {
    if (!ctx) return;
    for (int i = 0; i < ctx->count; i++) free(ctx->entries[i].str);
    free(ctx->entries);
    free(ctx);
}

void lt_strtab_init(LtStringTable *t) {
    t->strings = NULL;
    t->lengths = NULL;
    t->count = 0;
    t->capacity = 0;
}

void lt_strtab_free(LtStringTable *t) {
    for (int i = 0; i < t->count; i++) free(t->strings[i]);
    free(t->strings);
    free(t->lengths);
    t->strings = NULL;
    t->lengths = NULL;
    t->count = 0;
    t->capacity = 0;
}

static void ctx_add(LtStrtabCtx *ctx, const char *s, size_t len) {
    for (int i = 0; i < ctx->count; i++) {
        if (ctx->entries[i].len == len &&
            memcmp(ctx->entries[i].str, s, len) == 0) {
            ctx->entries[i].freq++;
            return;
        }
    }
    if (ctx->count >= ctx->capacity) {
        ctx->capacity = ctx->capacity ? ctx->capacity * 2 : 64;
        ctx->entries = realloc(ctx->entries,
                               (size_t)ctx->capacity * sizeof(FamStrEntry));
    }
    ctx->entries[ctx->count].str = malloc(len + 1);
    memcpy(ctx->entries[ctx->count].str, s, len);
    ctx->entries[ctx->count].str[len] = '\0';
    ctx->entries[ctx->count].len = len;
    ctx->entries[ctx->count].freq = 1;
    ctx->count++;
}

int lt_strtab_ingest(LtStrtabCtx *ctx, const char *json_bindings) {
    if (!ctx || !json_bindings) return -1;
    const char *p = json_bindings;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    while (*p && *p != ']') {
        char *val = NULL;
        int ty = parse_next_value(&p, &val);
        if (ty < 0) { free(val); break; }
        if (ty == LT_STRING && val) {
            ctx_add(ctx, val, strlen(val));
        }
        free(val);
    }
    return 0;
}

void lt_strtab_finalize(LtStrtabCtx *ctx, LtStringTable *t) {
    if (!ctx || !t) return;

    /* Sort by frequency descending */
    for (int i = 0; i < ctx->count; i++) {
        for (int j = i + 1; j < ctx->count; j++) {
            if (ctx->entries[j].freq > ctx->entries[i].freq) {
                FamStrEntry tmp = ctx->entries[i];
                ctx->entries[i] = ctx->entries[j];
                ctx->entries[j] = tmp;
            }
        }
    }

    /* Only keep strings where interning saves bytes */
    int n = 0;
    for (int i = 0; i < ctx->count; i++) {
        size_t slen = ctx->entries[i].len;
        int idx_cost = varint_size((unsigned long long)n);
        int str_cost_inline = 1 + varint_size((unsigned long long)slen) + (int)slen;
        int str_cost_interned = 1 + idx_cost;
        int per_occurrence_savings = str_cost_inline - str_cost_interned;
        int header_cost = varint_size((unsigned long long)slen) + (int)slen;
        int total_savings = ctx->entries[i].freq * per_occurrence_savings - header_cost;
        if (total_savings > 0) n++;
        else break;
    }

    t->count = n;
    t->capacity = n;
    t->strings = calloc((size_t)(n > 0 ? n : 1), sizeof(char *));
    t->lengths = calloc((size_t)(n > 0 ? n : 1), sizeof(size_t));
    for (int i = 0; i < n; i++) {
        t->strings[i] = ctx->entries[i].str;
        t->lengths[i] = ctx->entries[i].len;
        ctx->entries[i].str = NULL; /* transferred ownership */
    }
}

int lt_strtab_header_size(const LtStringTable *t) {
    /* Header format: [count:varint] { [len:varint] [bytes] }* */
    unsigned char tmp[16];
    int sz = varint_encode((unsigned long long)t->count, tmp, sizeof(tmp));
    for (int i = 0; i < t->count; i++) {
        sz += varint_encode((unsigned long long)t->lengths[i], tmp, sizeof(tmp));
        sz += (int)t->lengths[i];
    }
    return sz;
}

int lt_strtab_encode_header(const LtStringTable *t,
                                unsigned char **out, size_t *out_len) {
    unsigned char *buf = NULL;
    size_t cap, off = 0;
    unsigned char tmp[16];

    if (!t || !out || !out_len) return -1;

    cap = (size_t)(lt_strtab_header_size(t) > 0 ? lt_strtab_header_size(t) : 1);
    buf = malloc(cap);
    if (!buf) return -1;

    {
        int vlen = varint_encode((unsigned long long)t->count, tmp, sizeof(tmp));
        memcpy(buf + off, tmp, (size_t)vlen);
        off += (size_t)vlen;
    }

    for (int i = 0; i < t->count; i++) {
        int vlen = varint_encode((unsigned long long)t->lengths[i], tmp, sizeof(tmp));
        memcpy(buf + off, tmp, (size_t)vlen);
        off += (size_t)vlen;
        memcpy(buf + off, t->strings[i], t->lengths[i]);
        off += t->lengths[i];
    }

    *out = buf;
    *out_len = off;
    return 0;
}

int lt_strtab_decode_header(const unsigned char *header, size_t header_len,
                                LtStringTable *t) {
    size_t pos = 0;
    unsigned long long count_ull = 0;
    int hdr;

    if (!header || !t) return -1;
    lt_strtab_init(t);

    hdr = varint_decode(header + pos, header_len - pos, &count_ull);
    if (hdr <= 0) return -1;
    pos += (size_t)hdr;

    t->count = (int)count_ull;
    t->capacity = t->count;
    t->strings = calloc((size_t)(t->count > 0 ? t->count : 1), sizeof(char *));
    t->lengths = calloc((size_t)(t->count > 0 ? t->count : 1), sizeof(size_t));
    if (!t->strings || !t->lengths) {
        lt_strtab_free(t);
        return -1;
    }

    for (int i = 0; i < t->count; i++) {
        unsigned long long slen_ull = 0;
        size_t slen;
        int vlen = varint_decode(header + pos, header_len - pos, &slen_ull);
        if (vlen <= 0) {
            lt_strtab_free(t);
            return -1;
        }
        pos += (size_t)vlen;
        slen = (size_t)slen_ull;
        if (pos + slen > header_len) {
            lt_strtab_free(t);
            return -1;
        }

        t->strings[i] = malloc(slen + 1);
        if (!t->strings[i]) {
            lt_strtab_free(t);
            return -1;
        }
        memcpy(t->strings[i], header + pos, slen);
        t->strings[i][slen] = '\0';
        t->lengths[i] = slen;
        pos += slen;
    }

    return 0;
}

static int strtab_lookup(const LtStringTable *t, const char *s, size_t len) {
    for (int i = 0; i < t->count; i++) {
        if (t->lengths[i] == len && memcmp(t->strings[i], s, len) == 0)
            return i;
    }
    return -1;
}

int lt_encode_v2_interned(const char *json_bindings,
                               const LtStringTable *strtab,
                               unsigned char **out, size_t *out_len) {
    if (!json_bindings || !strtab || !out || !out_len) return -1;

    const char *p = json_bindings;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    typedef struct { int type; char *val; } TmpVal;
    int max_vals = 256, count = 0;
    TmpVal *vals = calloc((size_t)max_vals, sizeof(TmpVal));
    if (!vals) return -1;

    while (*p && *p != ']') {
        if (count >= max_vals) {
            max_vals *= 2;
            TmpVal *nv = realloc(vals, (size_t)max_vals * sizeof(TmpVal));
            if (!nv) { for (int i = 0; i < count; i++) free(vals[i].val); free(vals); return -1; }
            vals = nv;
        }
        int t = parse_next_value(&p, &vals[count].val);
        if (t < 0) break;
        vals[count].type = t;
        count++;
    }

    /* String table for in-array backrefs */
    int seen_cap = count > 0 ? count : 1;
    char **seen_strings = calloc((size_t)seen_cap, sizeof(char *));
    int seen_count = 0;

    size_t cap = 1024;
    unsigned char *buf = malloc(cap);
    if (!buf) {
        for (int i = 0; i < count; i++) free(vals[i].val);
        free(vals); free(seen_strings); return -1;
    }
    size_t off = 0;

    unsigned char vbuf[16];
    int vlen = varint_encode((unsigned long long)count, vbuf, sizeof(vbuf));
    if (ensure_cap(&buf, &cap, off, (size_t)vlen) < 0) goto interned_fail;
    memcpy(buf + off, vbuf, (size_t)vlen);
    off += (size_t)vlen;

    for (int i = 0; i < count; i++) {
        int t = vals[i].type;

        switch (t) {
        case LT_NULL: case LT_BOOL_F: case LT_BOOL_T:
            if (ensure_cap(&buf, &cap, off, 1) < 0) goto interned_fail;
            buf[off++] = (unsigned char)t;
            break;

        case LT_INT: {
            long long iv = strtoll(vals[i].val, NULL, 10);
            if (iv >= -64 && iv <= 63) {
                if (ensure_cap(&buf, &cap, off, 2) < 0) goto interned_fail;
                buf[off++] = LT_SMALL_INT;
                buf[off++] = (unsigned char)((int)iv + 64);
            } else {
                unsigned long long zz = zigzag_encode(iv);
                vlen = varint_encode(zz, vbuf, sizeof(vbuf));
                if (ensure_cap(&buf, &cap, off, 1 + (size_t)vlen) < 0) goto interned_fail;
                buf[off++] = LT_INT;
                memcpy(buf + off, vbuf, (size_t)vlen);
                off += (size_t)vlen;
            }
            break;
        }

        case LT_DOUBLE: {
            double dv = strtod(vals[i].val, NULL);
            float fv = (float)dv;
            if ((double)fv == dv) {
                if (ensure_cap(&buf, &cap, off, 5) < 0) goto interned_fail;
                buf[off++] = LT_FLOAT32;
                memcpy(buf + off, &fv, 4);
                off += 4;
            } else {
                if (ensure_cap(&buf, &cap, off, 9) < 0) goto interned_fail;
                buf[off++] = LT_DOUBLE;
                memcpy(buf + off, &dv, 8);
                off += 8;
            }
            break;
        }

        case LT_STRING: {
            size_t slen = strlen(vals[i].val);
            if (slen == 0) {
                if (ensure_cap(&buf, &cap, off, 1) < 0) goto interned_fail;
                buf[off++] = LT_EMPTY_STR;
                break;
            }

            /* Check family string table first (cross-member dedup) */
            int fam_idx = strtab_lookup(strtab, vals[i].val, slen);
            if (fam_idx >= 0) {
                vlen = varint_encode((unsigned long long)fam_idx, vbuf, sizeof(vbuf));
                if (ensure_cap(&buf, &cap, off, 1 + (size_t)vlen) < 0) goto interned_fail;
                buf[off++] = LT_FAMSTR;
                memcpy(buf + off, vbuf, (size_t)vlen);
                off += (size_t)vlen;
                break;
            }

            /* In-array string back-reference */
            int seen_idx = -1;
            for (int s = 0; s < seen_count; s++) {
                if (strcmp(seen_strings[s], vals[i].val) == 0) {
                    seen_idx = s;
                    break;
                }
            }

            if (seen_idx >= 0) {
                vlen = varint_encode((unsigned long long)seen_idx, vbuf, sizeof(vbuf));
                if (ensure_cap(&buf, &cap, off, 1 + (size_t)vlen) < 0) goto interned_fail;
                buf[off++] = LT_STRREF;
                memcpy(buf + off, vbuf, (size_t)vlen);
                off += (size_t)vlen;
            } else {
                vlen = varint_encode((unsigned long long)slen, vbuf, sizeof(vbuf));
                if (ensure_cap(&buf, &cap, off, 1 + (size_t)vlen + slen) < 0) goto interned_fail;
                buf[off++] = LT_STRING;
                memcpy(buf + off, vbuf, (size_t)vlen);
                off += (size_t)vlen;
                memcpy(buf + off, vals[i].val, slen);
                off += slen;
                seen_strings[seen_count++] = vals[i].val;
            }
            break;
        }

        case LT_NESTED: {
            size_t slen = strlen(vals[i].val);
            vlen = varint_encode((unsigned long long)slen, vbuf, sizeof(vbuf));
            if (ensure_cap(&buf, &cap, off, 1 + (size_t)vlen + slen) < 0) goto interned_fail;
            buf[off++] = LT_NESTED;
            memcpy(buf + off, vbuf, (size_t)vlen);
            off += (size_t)vlen;
            memcpy(buf + off, vals[i].val, slen);
            off += slen;
            break;
        }
        }
    }

    for (int i = 0; i < count; i++) free(vals[i].val);
    free(vals);
    free(seen_strings);
    *out = buf;
    *out_len = off;
    return count;

interned_fail:
    for (int i = 0; i < count; i++) free(vals[i].val);
    free(vals);
    free(seen_strings);
    free(buf);
    return -1;
}

int lt_measure_v2_interned(const char *json_bindings,
                                const LtStringTable *strtab) {
    unsigned char *packed = NULL;
    size_t packed_len = 0;
    int n = lt_encode_v2_interned(json_bindings, strtab, &packed, &packed_len);
    if (n < 0) return -1;
    free(packed);
    return (int)packed_len;
}

int lt_delta_measure_v2_interned(const char *ref_json, const char *target_json,
                                      const LtStringTable *strtab) {
    unsigned char *ref_packed = NULL, *tgt_packed = NULL, *delta = NULL;
    size_t ref_len = 0, tgt_len = 0, delta_len = 0;

    if (lt_encode_v2_interned(ref_json, strtab, &ref_packed, &ref_len) < 0) return -1;
    if (lt_encode_v2_interned(target_json, strtab, &tgt_packed, &tgt_len) < 0) {
        free(ref_packed); return -1;
    }
    if (lt_delta_encode_v2(ref_packed, ref_len, tgt_packed, tgt_len,
                                &delta, &delta_len) < 0) {
        free(ref_packed); free(tgt_packed); return -1;
    }

    int result = (int)delta_len;
    free(ref_packed);
    free(tgt_packed);
    free(delta);
    return result;
}

/* ================================================================
 * Canonical Huffman Coding — Per-Position Entropy Encoding
 * ================================================================
 *
 * For each binding position in a family, builds a canonical Huffman
 * codebook from the value frequency distribution (the PMF derived
 * from the binding tensor).  Values that appear frequently at a
 * position get shorter codes; constant positions cost 0 bits.
 *
 * Properties:
 *   - Single-stage: PMF comes from the tensor, no per-member codebook
 *   - Canonical: codes determined by code lengths alone (minimal header)
 *   - Per-position: each binding position gets its own codebook
 *   - Subsumes string interning: all value types compete on frequency
 *
 * References:
 *   arXiv:2601.10673 — Single-Stage Huffman via PMF from tensors
 *   Canonical Huffman — code lengths + sorted symbols = full codebook
 */

void lt_huff_init(LtHuffTable *t) {
    t->positions = NULL;
    t->num_positions = 0;
}

void lt_huff_free(LtHuffTable *t) {
    for (int i = 0; i < t->num_positions; i++) {
        for (int j = 0; j < t->positions[i].count; j++)
            free(t->positions[i].entries[j].val);
        free(t->positions[i].entries);
    }
    free(t->positions);
    t->positions = NULL;
    t->num_positions = 0;
}

/* Raw V2 encoding size for a typed value (bytes, no strtab) */
static int raw_value_size(int type, const char *val) {
    switch (type) {
    case LT_NULL: case LT_BOOL_F: case LT_BOOL_T: return 1;
    case LT_INT: {
        long long iv = strtoll(val, NULL, 10);
        if (iv >= -64 && iv <= 63) return 2;
        return 1 + varint_size(zigzag_encode(iv));
    }
    case LT_DOUBLE: {
        double dv = strtod(val, NULL);
        float fv = (float)dv;
        return ((double)fv == dv) ? 5 : 9;
    }
    case LT_STRING: {
        if (!val || strlen(val) == 0) return 1;
        size_t slen = strlen(val);
        return 1 + varint_size((unsigned long long)slen) + (int)slen;
    }
    case LT_NESTED: {
        size_t slen = val ? strlen(val) : 0;
        return 1 + varint_size((unsigned long long)slen) + (int)slen;
    }
    }
    return 1;
}

static void huff_pos_add(LtHuffPosition *cb, int type, const char *val) {
    for (int i = 0; i < cb->count; i++) {
        if (cb->entries[i].type != type) continue;
        if (type == LT_NULL || type == LT_BOOL_F || type == LT_BOOL_T) {
            cb->entries[i].freq++;
            return;
        }
        if (val && cb->entries[i].val && strcmp(cb->entries[i].val, val) == 0) {
            cb->entries[i].freq++;
            return;
        }
    }
    if (cb->count >= cb->capacity) {
        cb->capacity = cb->capacity ? cb->capacity * 2 : 16;
        cb->entries = realloc(cb->entries,
                              (size_t)cb->capacity * sizeof(LtHuffSymbol));
    }
    LtHuffSymbol *e = &cb->entries[cb->count++];
    e->type = type;
    e->val = val ? strdup(val) : NULL;
    e->freq = 1;
    e->code_len = 0;
    e->code = 0;
}

int lt_huff_ingest(LtHuffTable *t, const char *json_bindings) {
    if (!json_bindings) return -1;
    const char *p = json_bindings;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    int pos = 0;
    while (*p && *p != ']') {
        char *val = NULL;
        int ty = parse_next_value(&p, &val);
        if (ty < 0) { free(val); break; }

        if (pos >= t->num_positions) {
            int nc = pos + 1;
            t->positions = realloc(t->positions,
                                   (size_t)nc * sizeof(LtHuffPosition));
            for (int i = t->num_positions; i < nc; i++) {
                t->positions[i].entries = NULL;
                t->positions[i].count = 0;
                t->positions[i].capacity = 0;
                t->positions[i].use_huffman = 0;
                t->positions[i].total_freq = 0;
            }
            t->num_positions = nc;
        }

        huff_pos_add(&t->positions[pos], ty, val);
        free(val);
        pos++;
    }
    return 0;
}

/* Build canonical Huffman codes for one position's codebook */
static void build_position_huffman(LtHuffPosition *cb) {
    int n = cb->count;
    if (n == 0) return;
    if (n == 1) {
        /* Single symbol: 0 bits — value is implicit from codebook */
        cb->entries[0].code_len = 0;
        cb->entries[0].code = 0;
        return;
    }

    /* Huffman tree via repeated merge of two lightest nodes — O(n²), n small */
    int total = 2 * n - 1;
    int *nf = calloc((size_t)total, sizeof(int));
    int *np = malloc((size_t)total * sizeof(int));
    for (int i = 0; i < total; i++) np[i] = -1;
    for (int i = 0; i < n; i++) nf[i] = cb->entries[i].freq;

    int next = n;
    for (int step = 0; step < n - 1; step++) {
        int m1 = -1, m2 = -1;
        for (int i = 0; i < next; i++) {
            if (np[i] != -1) continue;
            if (m1 == -1 || nf[i] < nf[m1]) { m2 = m1; m1 = i; }
            else if (m2 == -1 || nf[i] < nf[m2]) { m2 = i; }
        }
        nf[next] = nf[m1] + nf[m2];
        np[m1] = next;
        np[m2] = next;
        next++;
    }

    /* Extract code lengths from tree depth */
    for (int i = 0; i < n; i++) {
        int depth = 0, node = i;
        while (np[node] != -1) { depth++; node = np[node]; }
        cb->entries[i].code_len = depth;
    }
    free(nf);
    free(np);

    /* Canonical code assignment: sort by (code_len asc, index asc) */
    int *order = malloc((size_t)n * sizeof(int));
    for (int i = 0; i < n; i++) order[i] = i;
    for (int i = 1; i < n; i++) {
        int k = order[i], j = i - 1;
        while (j >= 0 &&
               (cb->entries[order[j]].code_len > cb->entries[k].code_len ||
                (cb->entries[order[j]].code_len == cb->entries[k].code_len &&
                 order[j] > k))) {
            order[j + 1] = order[j];
            j--;
        }
        order[j + 1] = k;
    }

    uint32_t code = 0;
    int prev_len = cb->entries[order[0]].code_len;
    cb->entries[order[0]].code = 0;
    for (int i = 1; i < n; i++) {
        code++;
        int cl = cb->entries[order[i]].code_len;
        if (cl > prev_len) { code <<= (cl - prev_len); prev_len = cl; }
        cb->entries[order[i]].code = code;
    }
    free(order);
}

void lt_huff_finalize(LtHuffTable *t) {
    for (int i = 0; i < t->num_positions; i++) {
        LtHuffPosition *cb = &t->positions[i];
        build_position_huffman(cb);

        /* Cost-model: only use Huffman if bitstream + header < raw bytes */
        int huff_total_bits = 0;
        int raw_total_bytes = 0;
        int header_bytes = varint_size((unsigned long long)cb->count);
        for (int j = 0; j < cb->count; j++) {
            huff_total_bits += cb->entries[j].freq * cb->entries[j].code_len;
            int rsz = raw_value_size(cb->entries[j].type, cb->entries[j].val);
            raw_total_bytes += cb->entries[j].freq * rsz;
            header_bytes += 1 + rsz; /* code_len byte + value encoding */
        }
        int huff_cost = (huff_total_bits + 7) / 8 + header_bytes;
        cb->use_huffman = (huff_cost < raw_total_bytes) ? 1 : 0;

        /* Compute total frequency for arithmetic coding probabilities */
        int tf = 0;
        for (int j = 0; j < cb->count; j++) tf += cb->entries[j].freq;
        cb->total_freq = tf;
    }
}

/* Header size: what the codebook costs to store per family.
 * Format: [num_positions:varint] per position: [num_syms:varint]
 *         per sym (canonical order): [code_len:1B] [type:1B] [data...] */
int lt_huff_header_size(const LtHuffTable *t) {
    /* Header: [num_positions:varint] [huffman_mask: ceil(num_positions/8) bytes]
     *         per Huffman-enabled position: [num_syms:varint]
     *         per sym: [code_len:1B] [value_type:1B] [data...] */
    int sz = varint_size((unsigned long long)t->num_positions);
    sz += (t->num_positions + 7) / 8; /* bitmask of which positions use Huffman */
    for (int i = 0; i < t->num_positions; i++) {
        const LtHuffPosition *cb = &t->positions[i];
        if (!cb->use_huffman) continue; /* pass-through: no header cost */
        sz += varint_size((unsigned long long)cb->count);
        for (int j = 0; j < cb->count; j++) {
            sz += 1; /* code_len byte */
            sz += raw_value_size(cb->entries[j].type, cb->entries[j].val);
        }
    }
    return sz;
}

/* Look up a value in a position's codebook, return code_len or -1 */
static int huff_lookup(const LtHuffPosition *cb, int type, const char *val) {
    for (int i = 0; i < cb->count; i++) {
        if (cb->entries[i].type != type) continue;
        if (type == LT_NULL || type == LT_BOOL_F || type == LT_BOOL_T)
            return cb->entries[i].code_len;
        if (val && cb->entries[i].val && strcmp(cb->entries[i].val, val) == 0)
            return cb->entries[i].code_len;
    }
    return -1;
}

/* Measure Huffman-coded packed size for one member (bytes).
 * Hybrid: Huffman positions contribute bits, pass-through positions contribute raw bytes. */
int lt_measure_v2_huffman(const char *json_bindings,
                               const LtHuffTable *htab) {
    if (!json_bindings || !htab) return -1;
    const char *p = json_bindings;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    int huff_bits = 0, raw_bytes = 0, pos = 0;
    while (*p && *p != ']') {
        char *val = NULL;
        int ty = parse_next_value(&p, &val);
        if (ty < 0) { free(val); break; }

        if (pos < htab->num_positions && htab->positions[pos].use_huffman) {
            int cl = huff_lookup(&htab->positions[pos], ty, val);
            if (cl >= 0) huff_bits += cl;
            else raw_bytes += raw_value_size(ty, val); /* unknown symbol: raw */
        } else {
            raw_bytes += raw_value_size(ty, val);
        }
        free(val);
        pos++;
    }

    return raw_bytes + (huff_bits + 7) / 8;
}

/* Measure Huffman-coded delta size for one member against reference (bytes) */
int lt_delta_measure_v2_huffman(const char *ref_json,
                                     const char *target_json,
                                     const LtHuffTable *htab) {
    if (!ref_json || !target_json || !htab) return -1;

    typedef struct { int type; char *val; } TV;
    int rc = 0, tc = 0, rcap = 64, tcap = 64;
    TV *rv = calloc((size_t)rcap, sizeof(TV));
    TV *tv = calloc((size_t)tcap, sizeof(TV));
    const char *p;

    p = ref_json;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;
    while (*p && *p != ']') {
        if (rc >= rcap) { rcap *= 2; rv = realloc(rv, (size_t)rcap * sizeof(TV)); }
        rv[rc].type = parse_next_value(&p, &rv[rc].val);
        if (rv[rc].type < 0) { free(rv[rc].val); break; }
        rc++;
    }

    p = target_json;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;
    while (*p && *p != ']') {
        if (tc >= tcap) { tcap *= 2; tv = realloc(tv, (size_t)tcap * sizeof(TV)); }
        tv[tc].type = parse_next_value(&p, &tv[tc].val);
        if (tv[tc].type < 0) { free(tv[tc].val); break; }
        tc++;
    }

    int n = rc < tc ? rc : tc;

    /* Identify changed positions, accumulate Huffman bits */
    int changed_count = 0, changed_bits = 0, changed_raw = 0;
    int *is_changed = calloc((size_t)(n > 0 ? n : 1), sizeof(int));

    for (int i = 0; i < n; i++) {
        int same = 0;
        if (rv[i].type == tv[i].type) {
            if (rv[i].type == LT_NULL || rv[i].type == LT_BOOL_F ||
                rv[i].type == LT_BOOL_T)
                same = 1;
            else if (rv[i].val && tv[i].val &&
                     strcmp(rv[i].val, tv[i].val) == 0)
                same = 1;
        }
        if (!same) {
            is_changed[i] = 1;
            changed_count++;
            if (i < htab->num_positions && htab->positions[i].use_huffman) {
                int cl = huff_lookup(&htab->positions[i], tv[i].type, tv[i].val);
                if (cl >= 0) changed_bits += cl;
                else changed_raw += raw_value_size(tv[i].type, tv[i].val);
            } else {
                changed_raw += raw_value_size(tv[i].type, tv[i].val);
            }
        }
    }

    /* Bitmask cost: min(raw, sparse) — same model as V2 delta */
    int raw_sz = 1 + (n + 7) / 8;
    int sparse_sz = 1 + varint_size((unsigned long long)changed_count);
    for (int i = 0; i < n; i++) {
        if (is_changed[i])
            sparse_sz += varint_size((unsigned long long)i);
    }
    int mask_bytes = (sparse_sz < raw_sz) ? sparse_sz : raw_sz;

    free(is_changed);
    for (int i = 0; i < rc; i++) free(rv[i].val);
    for (int i = 0; i < tc; i++) free(tv[i].val);
    free(rv);
    free(tv);

    return mask_bytes + changed_raw + (changed_bits + 7) / 8;
}

/* ================================================================
 * Hybrid Arithmetic-Huffman: fractional-bit entropy coding
 * ================================================================
 *
 * At Huffman-enabled positions, replace integer code_len with
 * -log2(freq / total_freq) fractional bits.  Accumulate as double,
 * byte-align once per member.  Pass-through positions use raw V2.
 *
 * This measures what a table-driven arithmetic coder achieves
 * using the same per-position PMF as the Huffman tier.
 * ================================================================ */

static double arith_lookup(const LtHuffPosition *cb, int type, const char *val) {
    for (int i = 0; i < cb->count; i++) {
        if (cb->entries[i].type != type) continue;
        if (type == LT_NULL || type == LT_BOOL_F || type == LT_BOOL_T) {
            return -log2((double)cb->entries[i].freq / (double)cb->total_freq);
        }
        if (val && cb->entries[i].val && strcmp(cb->entries[i].val, val) == 0) {
            return -log2((double)cb->entries[i].freq / (double)cb->total_freq);
        }
    }
    return -1.0;
}

int lt_measure_v2_arithmetic(const char *json_bindings,
                                   const LtHuffTable *htab) {
    if (!json_bindings || !htab) return -1;
    const char *p = json_bindings;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    double arith_bits = 0.0;
    int raw_bytes = 0, pos = 0;
    while (*p && *p != ']') {
        char *val = NULL;
        int ty = parse_next_value(&p, &val);
        if (ty < 0) { free(val); break; }

        if (pos < htab->num_positions && htab->positions[pos].use_huffman) {
            double bits = arith_lookup(&htab->positions[pos], ty, val);
            if (bits >= 0.0) arith_bits += bits;
            else raw_bytes += raw_value_size(ty, val);
        } else {
            raw_bytes += raw_value_size(ty, val);
        }
        free(val);
        pos++;
    }

    return raw_bytes + (int)ceil(arith_bits / 8.0);
}

int lt_delta_measure_v2_arithmetic(const char *ref_json,
                                         const char *target_json,
                                         const LtHuffTable *htab) {
    if (!ref_json || !target_json || !htab) return -1;

    typedef struct { int type; char *val; } TV;
    int rc = 0, tc = 0, rcap = 64, tcap = 64;
    TV *rv = calloc((size_t)rcap, sizeof(TV));
    TV *tv = calloc((size_t)tcap, sizeof(TV));
    const char *p;

    p = ref_json;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;
    while (*p && *p != ']') {
        if (rc >= rcap) { rcap *= 2; rv = realloc(rv, (size_t)rcap * sizeof(TV)); }
        rv[rc].type = parse_next_value(&p, &rv[rc].val);
        if (rv[rc].type < 0) { free(rv[rc].val); break; }
        rc++;
    }

    p = target_json;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;
    while (*p && *p != ']') {
        if (tc >= tcap) { tcap *= 2; tv = realloc(tv, (size_t)tcap * sizeof(TV)); }
        tv[tc].type = parse_next_value(&p, &tv[tc].val);
        if (tv[tc].type < 0) { free(tv[tc].val); break; }
        tc++;
    }

    int n = rc < tc ? rc : tc;
    int changed_count = 0, changed_raw = 0;
    double changed_arith_bits = 0.0;
    int *is_changed = calloc((size_t)(n > 0 ? n : 1), sizeof(int));

    for (int i = 0; i < n; i++) {
        int same = 0;
        if (rv[i].type == tv[i].type) {
            if (rv[i].type == LT_NULL || rv[i].type == LT_BOOL_F ||
                rv[i].type == LT_BOOL_T)
                same = 1;
            else if (rv[i].val && tv[i].val &&
                     strcmp(rv[i].val, tv[i].val) == 0)
                same = 1;
        }
        if (!same) {
            is_changed[i] = 1;
            changed_count++;
            if (i < htab->num_positions && htab->positions[i].use_huffman) {
                double bits = arith_lookup(&htab->positions[i], tv[i].type, tv[i].val);
                if (bits >= 0.0) changed_arith_bits += bits;
                else changed_raw += raw_value_size(tv[i].type, tv[i].val);
            } else {
                changed_raw += raw_value_size(tv[i].type, tv[i].val);
            }
        }
    }

    int raw_sz = 1 + (n + 7) / 8;
    int sparse_sz = 1 + varint_size((unsigned long long)changed_count);
    for (int i = 0; i < n; i++) {
        if (is_changed[i])
            sparse_sz += varint_size((unsigned long long)i);
    }
    int mask_bytes = (sparse_sz < raw_sz) ? sparse_sz : raw_sz;

    free(is_changed);
    for (int i = 0; i < rc; i++) free(rv[i].val);
    for (int i = 0; i < tc; i++) free(tv[i].val);
    free(rv);
    free(tv);

    return mask_bytes + changed_raw + (int)ceil(changed_arith_bits / 8.0);
}

int lt_decode_member_v2_interned(const unsigned char *ref_packed, size_t ref_len,
                                      const unsigned char *stored, size_t stored_len,
                                      const LtStringTable *strtab,
                                      char **out_json) {
    unsigned char *packed = NULL;
    size_t packed_len = 0;
    int rc = -1;

    if (!stored || stored_len == 0 || !strtab || !out_json) return -1;

    switch (stored[0]) {
    case LT_STORED_DELTA:
        if (!ref_packed || ref_len == 0) return -1;
        if (lt_delta_decode_v2(ref_packed, ref_len,
                                    stored + 1, stored_len - 1,
                                    &packed, &packed_len) < 0)
            return -1;
        rc = lt_decode_v2_interned(packed, packed_len, strtab, out_json);
        free(packed);
        return rc;

    case LT_STORED_PACKED:
        return lt_decode_v2_interned(stored + 1, stored_len - 1, strtab, out_json);

    default:
        return -1;
    }
}
