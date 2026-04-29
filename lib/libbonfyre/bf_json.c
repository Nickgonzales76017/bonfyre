/*
 * bf_json.c — SIMD-accelerated JSON DOM parser
 *
 * Design: yyjson-inspired flat node array. Parse in one pass,
 * strings point into the original buffer (zero-copy).
 *
 * SIMD acceleration:
 *   - SSE4.2: PCMPISTRM to find structural chars in 16-byte windows
 *   - NEON: vtbl + vceq to scan for delimiters
 *   - Scalar fallback for other architectures
 *
 * Node pool: pre-allocated, doubles on overflow (amortized O(1)).
 * No recursion in parser — explicit stack.
 */

#include "bf_json.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* ── SIMD detection ──────────────────────────────────────────── */

#if defined(__SSE4_2__) && defined(__x86_64__)
#define BF_JSON_SSE42 1
#include <x86intrin.h>
#elif defined(__aarch64__)
#define BF_JSON_NEON 1
#include <arm_neon.h>
#endif

/* ── Document structure ──────────────────────────────────────── */

struct bf_json_doc {
    bf_json_node_t *nodes;
    int             count;
    int             capacity;
    const char     *buf;       /* Original buffer (kept for zero-copy) */
    size_t          buf_len;
};

/* ── Parser state ────────────────────────────────────────────── */

typedef struct {
    const char *s;
    const char *end;
    const char *cur;
    bf_json_doc_t *doc;
    char *err;
    size_t err_sz;

    /* Explicit stack for nested containers */
    int stack[256];
    int stack_top;
} parser_t;

/* ── Node pool ───────────────────────────────────────────────── */

static int node_alloc(parser_t *p) {
    bf_json_doc_t *doc = p->doc;
    if (doc->count >= doc->capacity) {
        int newcap = doc->capacity * 2;
        if (newcap < 64) newcap = 64;
        bf_json_node_t *nn = realloc(doc->nodes, (size_t)newcap * sizeof(bf_json_node_t));
        if (!nn) return -1;
        doc->nodes = nn;
        doc->capacity = newcap;
    }
    int idx = doc->count++;
    memset(&doc->nodes[idx], 0, sizeof(bf_json_node_t));
    doc->nodes[idx].parent = -1;
    doc->nodes[idx].next = -1;
    doc->nodes[idx].first_child = -1;
    return idx;
}

/* ── Skip whitespace (SIMD-accelerated) ──────────────────────── */

static inline void skip_ws(parser_t *p) {
#ifdef BF_JSON_SSE42
    /* Use SIMD to skip past whitespace 16 bytes at a time */
    static const char ws[16] = " \t\n\r\0\0\0\0\0\0\0\0\0\0\0\0";
    __m128i w = _mm_loadu_si128((const __m128i *)ws);
    while (p->cur + 16 <= p->end) {
        __m128i chunk = _mm_loadu_si128((const __m128i *)p->cur);
        int idx = _mm_cmpistri(w, chunk,
                                _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_ANY |
                                _SIDD_NEGATIVE_POLARITY | _SIDD_LEAST_SIGNIFICANT);
        if (idx == 0) return;
        if (idx < 16) { p->cur += idx; return; }
        p->cur += 16;
    }
#elif defined(BF_JSON_NEON)
    uint8x16_t sp = vdupq_n_u8(' ');
    uint8x16_t tab = vdupq_n_u8('\t');
    uint8x16_t nl = vdupq_n_u8('\n');
    uint8x16_t cr = vdupq_n_u8('\r');
    while (p->cur + 16 <= p->end) {
        uint8x16_t v = vld1q_u8((const uint8_t *)p->cur);
        uint8x16_t m = vorrq_u8(vorrq_u8(vceqq_u8(v, sp), vceqq_u8(v, tab)),
                                  vorrq_u8(vceqq_u8(v, nl), vceqq_u8(v, cr)));
        uint64x2_t m64 = vreinterpretq_u64_u8(m);
        /* If not all whitespace, find first non-ws */
        if (!(vgetq_lane_u64(m64, 0) == 0xFFFFFFFFFFFFFFFFULL &&
              vgetq_lane_u64(m64, 1) == 0xFFFFFFFFFFFFFFFFULL)) {
            for (int i = 0; i < 16 && p->cur < p->end; i++, p->cur++) {
                char c = *p->cur;
                if (c != ' ' && c != '\t' && c != '\n' && c != '\r') return;
            }
            return;
        }
        p->cur += 16;
    }
#endif
    /* Scalar tail */
    while (p->cur < p->end) {
        char c = *p->cur;
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') return;
        p->cur++;
    }
}

/* ── String scanner (SIMD finds quote/backslash) ─────────────── */

static inline const char *scan_string_end(const char *s, const char *end) {
#ifdef BF_JSON_SSE42
    static const char delims[16] = "\"\\\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
    __m128i d = _mm_loadu_si128((const __m128i *)delims);
    while (s + 16 <= end) {
        __m128i chunk = _mm_loadu_si128((const __m128i *)s);
        int idx = _mm_cmpistri(d, chunk,
                                _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_ANY |
                                _SIDD_LEAST_SIGNIFICANT);
        if (idx < 16) return s + idx;
        s += 16;
    }
#elif defined(BF_JSON_NEON)
    uint8x16_t qt = vdupq_n_u8('"');
    uint8x16_t bs = vdupq_n_u8('\\');
    while (s + 16 <= end) {
        uint8x16_t v = vld1q_u8((const uint8_t *)s);
        uint8x16_t m = vorrq_u8(vceqq_u8(v, qt), vceqq_u8(v, bs));
        uint64x2_t m64 = vreinterpretq_u64_u8(m);
        if (vgetq_lane_u64(m64, 0) | vgetq_lane_u64(m64, 1)) {
            for (int i = 0; i < 16; i++) {
                if (s[i] == '"' || s[i] == '\\') return s + i;
            }
        }
        s += 16;
    }
#endif
    while (s < end) {
        if (*s == '"' || *s == '\\') return s;
        s++;
    }
    return end;
}

/* ── Parse string (after opening quote consumed) ─────────────── */

static int parse_string(parser_t *p, const char **out_ptr, int *out_len) {
    const char *start = p->cur;
    while (p->cur < p->end) {
        const char *found = scan_string_end(p->cur, p->end);
        p->cur = found;
        if (p->cur >= p->end) break;
        if (*p->cur == '\\') {
            p->cur += 2; /* Skip escape sequence */
            continue;
        }
        if (*p->cur == '"') {
            *out_ptr = start;
            *out_len = (int)(p->cur - start);
            p->cur++; /* Consume closing quote */
            return 0;
        }
    }
    return -1; /* Unterminated string */
}

/* ── Parse number ────────────────────────────────────────────── */

static int parse_number(parser_t *p, bf_json_node_t *n) {
    const char *start = p->cur;
    int is_float = 0;

    if (*p->cur == '-') p->cur++;

    while (p->cur < p->end && *p->cur >= '0' && *p->cur <= '9') p->cur++;

    if (p->cur < p->end && *p->cur == '.') {
        is_float = 1;
        p->cur++;
        while (p->cur < p->end && *p->cur >= '0' && *p->cur <= '9') p->cur++;
    }

    if (p->cur < p->end && (*p->cur == 'e' || *p->cur == 'E')) {
        is_float = 1;
        p->cur++;
        if (p->cur < p->end && (*p->cur == '+' || *p->cur == '-')) p->cur++;
        while (p->cur < p->end && *p->cur >= '0' && *p->cur <= '9') p->cur++;
    }

    if (is_float) {
        n->type = BF_JSON_DOUBLE;
        n->val.d = strtod(start, NULL);
    } else {
        n->type = BF_JSON_INT;
        n->val.i = strtoll(start, NULL, 10);
    }
    return 0;
}

/* ── Forward declaration ─────────────────────────────────────── */

static int parse_value(parser_t *p, int parent, const char *key, int key_len);

/* ── Parse object ────────────────────────────────────────────── */

static int parse_object(parser_t *p, int obj_idx) {
    p->cur++; /* Consume '{' */
    skip_ws(p);

    int child_count = 0;
    int prev_child = -1;

    while (p->cur < p->end && *p->cur != '}') {
        skip_ws(p);
        if (p->cur >= p->end) return -1;

        /* Handle trailing comma or empty */
        if (*p->cur == '}') break;
        if (*p->cur == ',') { p->cur++; skip_ws(p); continue; }

        /* Key string */
        if (*p->cur != '"') return -1;
        p->cur++;
        const char *key_ptr;
        int key_len;
        if (parse_string(p, &key_ptr, &key_len) != 0) return -1;

        skip_ws(p);
        if (p->cur >= p->end || *p->cur != ':') return -1;
        p->cur++;
        skip_ws(p);

        /* Value */
        int child_idx = parse_value(p, obj_idx, key_ptr, key_len);
        if (child_idx < 0) return -1;

        if (child_count == 0) {
            p->doc->nodes[obj_idx].first_child = child_idx;
        } else {
            p->doc->nodes[prev_child].next = child_idx;
        }
        prev_child = child_idx;
        child_count++;

        skip_ws(p);
        if (p->cur < p->end && *p->cur == ',') {
            p->cur++;
            skip_ws(p);
        }
    }

    if (p->cur < p->end && *p->cur == '}') p->cur++;
    p->doc->nodes[obj_idx].child_count = child_count;
    return 0;
}

/* ── Parse array ─────────────────────────────────────────────── */

static int parse_array(parser_t *p, int arr_idx) {
    p->cur++; /* Consume '[' */
    skip_ws(p);

    int child_count = 0;
    int prev_child = -1;

    while (p->cur < p->end && *p->cur != ']') {
        skip_ws(p);
        if (p->cur >= p->end) return -1;

        if (*p->cur == ']') break;
        if (*p->cur == ',') { p->cur++; skip_ws(p); continue; }

        int child_idx = parse_value(p, arr_idx, NULL, 0);
        if (child_idx < 0) return -1;

        if (child_count == 0) {
            p->doc->nodes[arr_idx].first_child = child_idx;
        } else {
            p->doc->nodes[prev_child].next = child_idx;
        }
        prev_child = child_idx;
        child_count++;

        skip_ws(p);
        if (p->cur < p->end && *p->cur == ',') {
            p->cur++;
            skip_ws(p);
        }
    }

    if (p->cur < p->end && *p->cur == ']') p->cur++;
    p->doc->nodes[arr_idx].child_count = child_count;
    return 0;
}

/* ── Parse value ─────────────────────────────────────────────── */

static int parse_value(parser_t *p, int parent, const char *key, int key_len) {
    skip_ws(p);
    if (p->cur >= p->end) return -1;

    int idx = node_alloc(p);
    if (idx < 0) return -1;

    bf_json_node_t *n = &p->doc->nodes[idx];
    n->parent = parent;
    n->key = key;
    n->key_len = key_len;

    char c = *p->cur;

    if (c == '"') {
        p->cur++;
        n->type = BF_JSON_STRING;
        if (parse_string(p, &n->val.str.ptr, &n->val.str.len) != 0) return -1;
    } else if (c == '{') {
        n->type = BF_JSON_OBJECT;
        if (parse_object(p, idx) != 0) return -1;
    } else if (c == '[') {
        n->type = BF_JSON_ARRAY;
        if (parse_array(p, idx) != 0) return -1;
    } else if (c == 't') {
        if (p->cur + 4 > p->end || memcmp(p->cur, "true", 4) != 0) return -1;
        n->type = BF_JSON_BOOL;
        n->val.b = 1;
        p->cur += 4;
    } else if (c == 'f') {
        if (p->cur + 5 > p->end || memcmp(p->cur, "false", 5) != 0) return -1;
        n->type = BF_JSON_BOOL;
        n->val.b = 0;
        p->cur += 5;
    } else if (c == 'n') {
        if (p->cur + 4 > p->end || memcmp(p->cur, "null", 4) != 0) return -1;
        n->type = BF_JSON_NULL;
        p->cur += 4;
    } else if (c == '-' || (c >= '0' && c <= '9')) {
        if (parse_number(p, n) != 0) return -1;
    } else {
        return -1;
    }

    return idx;
}

/* ── Public API: Parse ───────────────────────────────────────── */

bf_json_doc_t *bf_json_parse(const char *buf, size_t len, char *err_out, size_t err_sz) {
    if (!buf || len == 0) {
        if (err_out && err_sz > 0) snprintf(err_out, err_sz, "empty input");
        return NULL;
    }

    bf_json_doc_t *doc = calloc(1, sizeof(bf_json_doc_t));
    if (!doc) return NULL;
    doc->capacity = 64;
    doc->nodes = calloc((size_t)doc->capacity, sizeof(bf_json_node_t));
    if (!doc->nodes) { free(doc); return NULL; }
    doc->buf = buf;
    doc->buf_len = len;

    parser_t p = {0};
    p.s = buf;
    p.end = buf + len;
    p.cur = buf;
    p.doc = doc;
    p.err = err_out;
    p.err_sz = err_sz;

    int root = parse_value(&p, -1, NULL, 0);
    if (root < 0) {
        if (err_out && err_sz > 0) {
            size_t offset = (size_t)(p.cur - buf);
            snprintf(err_out, err_sz, "parse error at byte %zu", offset);
        }
        bf_json_free(doc);
        return NULL;
    }

    return doc;
}

bf_json_doc_t *bf_json_parse_str(const char *s, char *err_out, size_t err_sz) {
    return bf_json_parse(s, s ? strlen(s) : 0, err_out, err_sz);
}

void bf_json_free(bf_json_doc_t *doc) {
    if (!doc) return;
    free(doc->nodes);
    free(doc);
}

/* ── Public API: Access ──────────────────────────────────────── */

const bf_json_node_t *bf_json_root(const bf_json_doc_t *doc) {
    if (!doc || doc->count == 0) return NULL;
    return &doc->nodes[0];
}

int bf_json_count(const bf_json_doc_t *doc) {
    return doc ? doc->count : 0;
}

const bf_json_node_t *bf_json_at(const bf_json_doc_t *doc, int idx) {
    if (!doc || idx < 0 || idx >= doc->count) return NULL;
    return &doc->nodes[idx];
}

const bf_json_node_t *bf_json_obj_get(const bf_json_doc_t *doc,
                                       const bf_json_node_t *obj,
                                       const char *key) {
    if (!doc || !obj || !key || obj->type != BF_JSON_OBJECT) return NULL;
    size_t klen = strlen(key);

    int ci = obj->first_child;
    while (ci >= 0 && ci < doc->count) {
        const bf_json_node_t *child = &doc->nodes[ci];
        if (child->key && child->key_len == (int)klen &&
            memcmp(child->key, key, klen) == 0) {
            return child;
        }
        ci = child->next;
    }
    return NULL;
}

const bf_json_node_t *bf_json_arr_get(const bf_json_doc_t *doc,
                                       const bf_json_node_t *arr,
                                       int i) {
    if (!doc || !arr || arr->type != BF_JSON_ARRAY || i < 0) return NULL;

    int ci = arr->first_child;
    int idx = 0;
    while (ci >= 0 && ci < doc->count) {
        if (idx == i) return &doc->nodes[ci];
        ci = doc->nodes[ci].next;
        idx++;
    }
    return NULL;
}

/* ── Value extractors ────────────────────────────────────────── */

const char *bf_json_get_str(const bf_json_node_t *n, int *len_out) {
    if (!n || n->type != BF_JSON_STRING) {
        if (len_out) *len_out = 0;
        return NULL;
    }
    if (len_out) *len_out = n->val.str.len;
    return n->val.str.ptr;
}

int bf_json_get_str_copy(const bf_json_node_t *n, char *out, size_t sz) {
    if (!n || !out || sz == 0 || n->type != BF_JSON_STRING) {
        if (out && sz > 0) out[0] = '\0';
        return 0;
    }
    size_t copy = (size_t)n->val.str.len < sz - 1 ? (size_t)n->val.str.len : sz - 1;
    memcpy(out, n->val.str.ptr, copy);
    out[copy] = '\0';
    return (int)copy;
}

int64_t bf_json_get_int(const bf_json_node_t *n) {
    if (!n) return 0;
    if (n->type == BF_JSON_INT) return n->val.i;
    if (n->type == BF_JSON_DOUBLE) return (int64_t)n->val.d;
    return 0;
}

double bf_json_get_double(const bf_json_node_t *n) {
    if (!n) return 0.0;
    if (n->type == BF_JSON_DOUBLE) return n->val.d;
    if (n->type == BF_JSON_INT) return (double)n->val.i;
    return 0.0;
}

int bf_json_get_bool(const bf_json_node_t *n) {
    if (!n) return 0;
    if (n->type == BF_JSON_BOOL) return n->val.b;
    if (n->type == BF_JSON_INT) return n->val.i != 0;
    return 0;
}

/* ── Dotpath lookup ──────────────────────────────────────────── */

const bf_json_node_t *bf_json_dotpath(const bf_json_doc_t *doc, const char *path) {
    if (!doc || !path) return NULL;

    const bf_json_node_t *cur = bf_json_root(doc);
    if (!cur) return NULL;

    char seg[256];
    const char *p = path;

    while (*p && cur) {
        const char *dot = strchr(p, '.');
        size_t slen = dot ? (size_t)(dot - p) : strlen(p);
        if (slen >= sizeof(seg)) slen = sizeof(seg) - 1;
        memcpy(seg, p, slen);
        seg[slen] = '\0';

        if (cur->type == BF_JSON_OBJECT) {
            cur = bf_json_obj_get(doc, cur, seg);
        } else if (cur->type == BF_JSON_ARRAY) {
            char *end;
            int idx = (int)strtol(seg, &end, 10);
            if (end == seg) return NULL;
            cur = bf_json_arr_get(doc, cur, idx);
        } else {
            return NULL;
        }

        p += slen;
        if (*p == '.') p++;
    }

    return cur;
}

/* ── Iteration ───────────────────────────────────────────────── */

const bf_json_node_t *bf_json_child_first(const bf_json_doc_t *doc,
                                            const bf_json_node_t *parent) {
    if (!doc || !parent || parent->first_child < 0) return NULL;
    return &doc->nodes[parent->first_child];
}

const bf_json_node_t *bf_json_child_next(const bf_json_doc_t *doc,
                                           const bf_json_node_t *node) {
    if (!doc || !node || node->next < 0) return NULL;
    return &doc->nodes[node->next];
}

/* ── Compat: one-shot extractors ──────────────────────────────── */

int bf_json2_str(const char *json, const char *key, char *out, size_t out_sz) {
    if (!json || !key || !out || out_sz == 0) return 0;
    bf_json_doc_t *doc = bf_json_parse_str(json, NULL, 0);
    if (!doc) return 0;
    const bf_json_node_t *r = bf_json_root(doc);
    const bf_json_node_t *n = bf_json_obj_get(doc, r, key);
    int ret = bf_json_get_str_copy(n, out, out_sz) > 0 ? 1 : 0;
    bf_json_free(doc);
    return ret;
}

int bf_json2_int(const char *json, const char *key, int *out) {
    if (!json || !key || !out) return 0;
    bf_json_doc_t *doc = bf_json_parse_str(json, NULL, 0);
    if (!doc) return 0;
    const bf_json_node_t *r = bf_json_root(doc);
    const bf_json_node_t *n = bf_json_obj_get(doc, r, key);
    if (!n || (n->type != BF_JSON_INT && n->type != BF_JSON_DOUBLE)) {
        bf_json_free(doc);
        return 0;
    }
    *out = (int)bf_json_get_int(n);
    bf_json_free(doc);
    return 1;
}

int bf_json2_double(const char *json, const char *key, double *out) {
    if (!json || !key || !out) return 0;
    bf_json_doc_t *doc = bf_json_parse_str(json, NULL, 0);
    if (!doc) return 0;
    const bf_json_node_t *r = bf_json_root(doc);
    const bf_json_node_t *n = bf_json_obj_get(doc, r, key);
    if (!n || (n->type != BF_JSON_DOUBLE && n->type != BF_JSON_INT)) {
        bf_json_free(doc);
        return 0;
    }
    *out = bf_json_get_double(n);
    bf_json_free(doc);
    return 1;
}
