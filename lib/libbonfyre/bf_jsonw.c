/*
 * bf_jsonw.c — JSON writer matching JavaScript's JSON.stringify. See bf_jsonw.h.
 */
#include "bf_jsonw.h"
#include "bonfyre.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void put(bf_jsonw *w, const char *s, size_t n) {
    if (w->oom) return;
    if (w->len + n + 1 > w->cap) {
        size_t cap = w->cap ? w->cap : 256;
        while (cap < w->len + n + 1) cap *= 2;
        char *b = realloc(w->buf, cap);
        if (!b) { w->oom = 1; return; }
        w->buf = b; w->cap = cap;
    }
    memcpy(w->buf + w->len, s, n);
    w->len += n;
    w->buf[w->len] = 0;
}
static void putc1(bf_jsonw *w, char c) { put(w, &c, 1); }

void bf_jsonw_init(bf_jsonw *w, int indent) {
    memset(w, 0, sizeof *w);
    w->indent = indent;
    w->first[0] = 1;
}
void bf_jsonw_free(bf_jsonw *w) { free(w->buf); memset(w, 0, sizeof *w); }
void bf_jsonw_reset(bf_jsonw *w) {
    int indent = w->indent; char *b = w->buf; size_t cap = w->cap;
    memset(w, 0, sizeof *w);
    w->buf = b; w->cap = cap; w->indent = indent; w->first[0] = 1;
    if (b) b[0] = 0;
}

static void newline(bf_jsonw *w) {
    if (!w->indent) return;
    putc1(w, '\n');
    for (int i = 0; i < w->depth * w->indent; i++) putc1(w, ' ');
}

/* Before any value: separator and indentation, unless it follows a key. */
static void pre(bf_jsonw *w) {
    if (w->after_key) { w->after_key = 0; return; }
    if (w->depth == 0) return;
    if (!w->first[w->depth]) putc1(w, ',');
    w->first[w->depth] = 0;
    newline(w);
}

static void wopen(bf_jsonw *w, char c) {
    pre(w); putc1(w, c);
    if (w->depth < 63) w->depth++;
    w->first[w->depth] = 1;
}
static void close_(bf_jsonw *w, char c) {
    int empty = w->first[w->depth];
    if (w->depth > 0) w->depth--;
    if (!empty) newline(w);
    putc1(w, c);
}
void bf_jsonw_obj(bf_jsonw *w) { wopen(w, '{'); }
void bf_jsonw_obj_end(bf_jsonw *w) { close_(w, '}'); }
void bf_jsonw_arr(bf_jsonw *w) { wopen(w, '['); }
void bf_jsonw_arr_end(bf_jsonw *w) { close_(w, ']'); }

static void quote(bf_jsonw *w, const char *s, size_t n) {
    static const char hex[] = "0123456789abcdef";
    putc1(w, '"');
    size_t run = 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c >= 0x20 && c != '"' && c != '\\') continue;   /* the common case: copied in one run */
        const char *esc = NULL; char u[7];
        if (c == '"') esc = "\\\"";
        else if (c == '\\') esc = "\\\\";
        else if (c < 0x20) {
            switch (c) {
            case '\b': esc = "\\b"; break; case '\f': esc = "\\f"; break;
            case '\n': esc = "\\n"; break; case '\r': esc = "\\r"; break;
            case '\t': esc = "\\t"; break;
            default:
                u[0] = '\\'; u[1] = 'u'; u[2] = '0'; u[3] = '0';
                u[4] = hex[c >> 4]; u[5] = hex[c & 15]; u[6] = 0; esc = u;
            }
        }
        if (esc) { put(w, s + run, i - run); put(w, esc, strlen(esc)); run = i + 1; }
    }
    put(w, s + run, n - run);
    putc1(w, '"');
}

void bf_jsonw_keyn(bf_jsonw *w, const char *key, size_t len) {
    pre(w);
    quote(w, key, len);
    putc1(w, ':');
    if (w->indent) putc1(w, ' ');
    w->after_key = 1;
}
void bf_jsonw_key(bf_jsonw *w, const char *key) { bf_jsonw_keyn(w, key, strlen(key)); }
void bf_jsonw_strn(bf_jsonw *w, const char *s, size_t len) { pre(w); quote(w, s, len); }
void bf_jsonw_str(bf_jsonw *w, const char *s) { bf_jsonw_strn(w, s, strlen(s)); }
void bf_jsonw_bool(bf_jsonw *w, int b) { pre(w); put(w, b ? "true" : "false", b ? 4 : 5); }
void bf_jsonw_null(bf_jsonw *w) { pre(w); put(w, "null", 4); }

int bf_js_number(double d, char out[32]) {
    if (!isfinite(d)) return snprintf(out, 32, "null");
    if (d == 0) return snprintf(out, 32, "0");
    if (d > -9007199254740992.0 && d < 9007199254740992.0 && d == (double)(long long)d)
        return snprintf(out, 32, "%lld", (long long)d);   /* integers print as themselves */
    /* Shortest round-trip digits: if 15 significant digits round-trip they
     * are already the shortest form (trailing zeros dropped below), else 16,
     * else 17 always do. */
    /* Exact fast path (Clinger): for k decimals, N = round(d * 10^k) with
     * |N| < 10^15. Both N and 10^k (k <= 22) are exact doubles, so N / 10^k is
     * the correctly rounded quotient; equal to d means N * 10^-k round-trips,
     * and the smallest such k is the unique shortest form (decimal spacing
     * exceeds an ulp below 10^15). */
    static const double p10[23] = {1e0,1e1,1e2,1e3,1e4,1e5,1e6,1e7,1e8,1e9,1e10,1e11,1e12,1e13,1e14,1e15,1e16,1e17,1e18,1e19,1e20,1e21,1e22};
    double ad = fabs(d);
    if (ad > 1e-7 && ad < 1e15) {
        for (int k = 1; k <= 22; k++) {
            double m = d * p10[k];
            if (fabs(m) >= 1e15) break;
            long long N = llround(m);
            if ((double)N / p10[k] != d) continue;
            char dg[24]; int nd = snprintf(dg, sizeof dg, "%lld", N < 0 ? -N : N);
            int n = nd - k, o = 0, kk = nd;   /* decimal point position; trim trailing zeros */
            while (kk > 1 && dg[kk - 1] == '0') kk--;
            if (d < 0) out[o++] = '-';
            if (n > 0) { memcpy(out + o, dg, (size_t)n); o += n; if (kk > n) { out[o++] = '.'; memcpy(out + o, dg + n, (size_t)(kk - n)); o += kk - n; } }
            else if (n > -6) { out[o++] = '0'; out[o++] = '.'; for (int z = 0; z < -n; z++) out[o++] = '0'; memcpy(out + o, dg, (size_t)kk); o += kk; }
            else break;   /* exponent form: leave to the general path */
            out[o] = 0;
            return o;
        }
    }
    char e[40]; int p;
    if (fabs(d) < 2.2250738585072014e-308) {   /* subnormals carry fewer digits: search all */
        for (p = 1; p <= 17; p++) { snprintf(e, sizeof e, "%.*e", p - 1, d); if (p == 17 || strtod(e, NULL) == d) break; }
    } else {
        /* One exact 17-digit conversion; 15 and 16 digits are its roundings,
         * each verified by round trip. A decimal tie in the dropped digits
         * could double-round, so ties take the exact conversion instead. */
        char e17[40];
        snprintf(e17, sizeof e17, "%.16e", d);
        p = 17; memcpy(e, e17, sizeof e17);
        const char *m = e17 + (e17[0] == '-'), *ex = strchr(m, 'e');
        char dig[18]; int nd = 0;
        for (const char *q = m; q < ex; q++) if (*q != '.') dig[nd++] = *q;
        int exp10 = atoi(ex + 1);
        for (int want = 15; want <= 16; want++) {
            const char *rest = dig + want; int tail = 17 - want;
            int tie = rest[0] == '5'; for (int k = 1; k < tail; k++) if (rest[k] != '0') tie = 0;
            char cand[40];
            if (tie) snprintf(cand, sizeof cand, "%.*e", want - 1, d);
            else {
                char r[18]; memcpy(r, dig, (size_t)want); int e2 = exp10, i = want - 1;
                if (rest[0] >= '5') { while (i >= 0 && r[i] == '9') r[i--] = '0'; if (i >= 0) r[i]++; else { memmove(r + 1, r, (size_t)want - 1); r[0] = '1'; e2++; } }
                int o = 0; if (d < 0) cand[o++] = '-';
                cand[o++] = r[0]; cand[o++] = '.'; memcpy(cand + o, r + 1, (size_t)want - 1); o += want - 1;
                snprintf(cand + o, sizeof cand - (size_t)o, "e%+03d", e2);
            }
            if (strtod(cand, NULL) == d) { memcpy(e, cand, sizeof cand); p = want; break; }
        }
    }
    /* e = [-]D[.DDD]e[+-]XX */
    char digits[24]; int k = 0, neg = 0; const char *c = e;
    if (*c == '-') { neg = 1; c++; }
    for (; *c && *c != 'e'; c++) if (*c != '.') digits[k++] = *c;
    while (k > 1 && digits[k - 1] == '0') k--;
    digits[k] = 0;
    int n = atoi(c + 1) + 1, o = 0;
    if (neg) out[o++] = '-';
    if (k <= n && n <= 21) {
        memcpy(out + o, digits, k); o += k;
        for (int i = k; i < n; i++) out[o++] = '0';
    } else if (0 < n && n <= 21) {
        memcpy(out + o, digits, n); o += n; out[o++] = '.';
        memcpy(out + o, digits + n, k - n); o += k - n;
    } else if (-6 < n && n <= 0) {
        out[o++] = '0'; out[o++] = '.';
        for (int i = 0; i < -n; i++) out[o++] = '0';
        memcpy(out + o, digits, k); o += k;
    } else {
        out[o++] = digits[0];
        if (k > 1) { out[o++] = '.'; memcpy(out + o, digits + 1, k - 1); o += k - 1; }
        o += snprintf(out + o, 32 - o, "e%c%d", n - 1 >= 0 ? '+' : '-', abs(n - 1));
    }
    out[o] = 0;
    return o;
}

void bf_jsonw_num(bf_jsonw *w, double d) {
    char b[32]; int n = bf_js_number(d, b);
    pre(w); put(w, b, (size_t)n);
}
void bf_jsonw_int(bf_jsonw *w, int64_t i) { bf_jsonw_num(w, (double)i); }

static int hexval(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}
static unsigned u4(const char *s) {
    unsigned v = 0;
    for (int i = 0; i < 4; i++) { int h = hexval(s[i]); if (h < 0) return 0xFFFD; v = v * 16 + (unsigned)h; }
    return v;
}
static int utf8(unsigned cp, char *o) {
    if (cp < 0x80) { o[0] = (char)cp; return 1; }
    if (cp < 0x800) { o[0] = (char)(0xC0 | cp >> 6); o[1] = (char)(0x80 | (cp & 63)); return 2; }
    if (cp < 0x10000) { o[0] = (char)(0xE0 | cp >> 12); o[1] = (char)(0x80 | (cp >> 6 & 63)); o[2] = (char)(0x80 | (cp & 63)); return 3; }
    o[0] = (char)(0xF0 | cp >> 18); o[1] = (char)(0x80 | (cp >> 12 & 63));
    o[2] = (char)(0x80 | (cp >> 6 & 63)); o[3] = (char)(0x80 | (cp & 63)); return 4;
}

char *bf_json_unescape(const char *raw, int len, size_t *out_len) {
    char *o = malloc((size_t)len + 1); size_t n = 0;
    if (!o) return NULL;
    for (int i = 0; i < len; i++) {
        if (raw[i] != '\\' || i + 1 >= len) { o[n++] = raw[i]; continue; }
        char c = raw[++i];
        switch (c) {
        case 'b': o[n++] = '\b'; break; case 'f': o[n++] = '\f'; break;
        case 'n': o[n++] = '\n'; break; case 'r': o[n++] = '\r'; break;
        case 't': o[n++] = '\t'; break;
        case 'u':
            if (i + 4 < len) {
                unsigned cp = u4(raw + i + 1); i += 4;
                if (cp >= 0xD800 && cp < 0xDC00 && i + 6 < len && raw[i + 1] == '\\' && raw[i + 2] == 'u') {
                    unsigned lo = u4(raw + i + 3);
                    if (lo >= 0xDC00 && lo < 0xE000) { cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00); i += 6; }
                }
                n += (size_t)utf8(cp, o + n);   /* \uXXXX is 6 bytes in, at most 4 out */
            }
            break;
        default: o[n++] = c;
        }
    }
    o[n] = 0;
    if (out_len) *out_len = n;
    return o;
}

int bf_json_rawcmp(const char *a, int alen, const char *b, int blen) {
    if (!memchr(a, '\\', (size_t)alen) && !memchr(b, '\\', (size_t)blen)) {
        int r = memcmp(a, b, (size_t)(alen < blen ? alen : blen));
        return r ? r : (alen > blen) - (alen < blen);
    }
    size_t an, bn; char *x = bf_json_unescape(a, alen, &an), *y = bf_json_unescape(b, blen, &bn);
    int r = memcmp(x, y, an < bn ? an : bn);
    if (!r) r = (an > bn) - (an < bn);
    free(x); free(y);
    return r;
}

static int cmp_keys(const void *pa, const void *pb) {
    const bf_json_node_t *a = *(const bf_json_node_t *const *)pa, *b = *(const bf_json_node_t *const *)pb;
    return bf_json_rawcmp(a->key, a->key_len, b->key, b->key_len);
}

/* JS objects list array-index keys ("0", "7", "42") first, ascending. */
static long index_key(const char *k, int len) {
    if (len < 1 || len > 10 || (len > 1 && k[0] == '0')) return -1;
    long v = 0;
    for (int i = 0; i < len; i++) { if (k[i] < '0' || k[i] > '9') return -1; v = v * 10 + (k[i] - '0'); }
    return v < 4294967295L ? v : -1;
}
static int cmp_source(const void *pa, const void *pb) {
    const bf_json_node_t *a = *(const bf_json_node_t *const *)pa, *b = *(const bf_json_node_t *const *)pb;
    long x = index_key(a->key, a->key_len), y = index_key(b->key, b->key_len);
    if (x >= 0 && y >= 0) return (x > y) - (x < y);
    if (x >= 0) return -1;
    if (y >= 0) return 1;
    return (a > b) - (a < b);   /* nodes are stored in source order */
}

static void raw_string(bf_jsonw *w, const char *raw, int len, int key) {
    if (!memchr(raw, '\\', (size_t)len)) {
        if (key) bf_jsonw_keyn(w, raw, (size_t)len); else bf_jsonw_strn(w, raw, (size_t)len);
        return;
    }
    size_t n; char *s = bf_json_unescape(raw, len, &n);
    if (key) bf_jsonw_keyn(w, s, n); else bf_jsonw_strn(w, s, n);
    free(s);
}

void bf_jsonw_node(bf_jsonw *w, const bf_json_doc_t *doc, const bf_json_node_t *n, int sorted) {
    if (!n) { bf_jsonw_null(w); return; }
    switch (n->type) {
    case BF_JSON_NULL: bf_jsonw_null(w); break;
    case BF_JSON_BOOL: bf_jsonw_bool(w, n->val.b); break;
    case BF_JSON_INT: bf_jsonw_num(w, (double)n->val.i); break;
    case BF_JSON_DOUBLE: bf_jsonw_num(w, n->val.d); break;
    case BF_JSON_STRING: raw_string(w, n->val.str.ptr, n->val.str.len, 0); break;
    case BF_JSON_ARRAY:
        bf_jsonw_arr(w);
        for (const bf_json_node_t *c = bf_json_child_first(doc, n); c; c = bf_json_child_next(doc, c))
            bf_jsonw_node(w, doc, c, sorted);
        bf_jsonw_arr_end(w);
        break;
    case BF_JSON_OBJECT: {
        bf_jsonw_obj(w);
        int count = n->child_count, k = 0;
        const bf_json_node_t **kids = malloc(sizeof *kids * (size_t)(count ? count : 1));
        for (const bf_json_node_t *c = bf_json_child_first(doc, n); c && k < count; c = bf_json_child_next(doc, c)) kids[k++] = c;
        qsort(kids, (size_t)k, sizeof *kids, sorted ? cmp_keys : cmp_source);
        for (int i = 0; i < k; i++) {
            raw_string(w, kids[i]->key, kids[i]->key_len, 1);
            bf_jsonw_node(w, doc, kids[i], sorted);
        }
        free(kids);
        bf_jsonw_obj_end(w);
        break;
    }
    }
}

void bf_jsonw_digest(const bf_json_doc_t *doc, const bf_json_node_t *n, char hex[65]) {
    bf_jsonw w; bf_jsonw_init(&w, 0);
    bf_jsonw_node(&w, doc, n, 1);
    bf_sha256_hex((const uint8_t *)(w.buf ? w.buf : ""), w.len, hex);
    bf_jsonw_free(&w);
}

/* String.prototype.localeCompare under the root locale, for ASCII text:
 * punctuation before digits before letters, letters compared without case
 * first, then lower case before upper case. Other bytes sort after ASCII. */
static const char js_primary_order[] =
    " _-,;:!?.'\"()[]{}@*/\\&#%`^+<=>|~$0123456789abcdefghijklmnopqrstuvwxyz";
static int primary(unsigned char c) {
    if (c >= 'A' && c <= 'Z') c = (unsigned char)(c - 'A' + 'a');
    const char *p = c ? strchr(js_primary_order, c) : NULL;
    return p ? (int)(p - js_primary_order) + 1 : 256 + c;
}
int bf_js_locale_compare(const char *a, const char *b) {
    size_t i;
    for (i = 0; a[i] && b[i]; i++) {
        int x = primary((unsigned char)a[i]), y = primary((unsigned char)b[i]);
        if (x != y) return x < y ? -1 : 1;
    }
    if (a[i] || b[i]) return a[i] ? 1 : -1;
    for (i = 0; a[i]; i++) {
        int x = a[i] >= 'A' && a[i] <= 'Z', y = b[i] >= 'A' && b[i] <= 'Z';
        if (x != y) return x ? 1 : -1;
    }
    return 0;
}
