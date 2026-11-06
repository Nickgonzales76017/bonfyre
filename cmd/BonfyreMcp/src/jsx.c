/* JavaScript semantics the retired CLIs relied on, over bf_jv. */
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf_jsonw.h"
#include "mcp.h"

/* ── JSON.parse ─────────────────────────────────────────────── */
typedef struct { const unsigned char *s, *e; const char *fail; int depth; } P;

static int utf8_valid(const unsigned char *s, size_t n) {
    size_t i = 0;
    while (i < n) {
        unsigned c = s[i];
        if (c < 0x80) { i++; continue; }
        size_t k; unsigned cp, min;
        if ((c & 0xE0) == 0xC0) { k = 1; cp = c & 0x1F; min = 0x80; }
        else if ((c & 0xF0) == 0xE0) { k = 2; cp = c & 0x0F; min = 0x800; }
        else if ((c & 0xF8) == 0xF0) { k = 3; cp = c & 0x07; min = 0x10000; }
        else return 0;
        for (size_t j = 1; j <= k; j++) {
            if (i + j >= n || (s[i + j] & 0xC0) != 0x80) return 0;
            cp = cp << 6 | (s[i + j] & 0x3F);
        }
        if (cp < min || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) return 0;
        i += k + 1;
    }
    return 1;
}

static void ws(P *p) { while (p->s < p->e && (*p->s == ' ' || *p->s == '\t' || *p->s == '\n' || *p->s == '\r')) p->s++; }
static int hex4(const unsigned char *s, unsigned *v) {
    *v = 0;
    for (int i = 0; i < 4; i++) {
        unsigned c = s[i], d;
        if (c >= '0' && c <= '9') d = c - '0'; else if (c >= 'a' && c <= 'f') d = c - 'a' + 10;
        else if (c >= 'A' && c <= 'F') d = c - 'A' + 10; else return 0;
        *v = *v << 4 | d;
    }
    return 1;
}
static size_t put8(unsigned cp, char *o) {
    if (cp < 0x80) { o[0] = (char)cp; return 1; }
    if (cp < 0x800) { o[0] = (char)(0xC0 | cp >> 6); o[1] = (char)(0x80 | (cp & 63)); return 2; }
    if (cp < 0x10000) { o[0] = (char)(0xE0 | cp >> 12); o[1] = (char)(0x80 | (cp >> 6 & 63)); o[2] = (char)(0x80 | (cp & 63)); return 3; }
    o[0] = (char)(0xF0 | cp >> 18); o[1] = (char)(0x80 | (cp >> 12 & 63)); o[2] = (char)(0x80 | (cp >> 6 & 63)); o[3] = (char)(0x80 | (cp & 63)); return 4;
}
static char *pstring(P *p, size_t *len) {
    p->s++;   /* opening quote */
    char *o = malloc((size_t)(p->e - p->s) + 1); size_t n = 0;
    if (!o) { p->fail = "Out of memory"; return NULL; }
    while (p->s < p->e) {
        unsigned c = *p->s;
        if (c == '"') { p->s++; o[n] = 0; *len = n; return o; }
        if (c < 0x20) break;
        if (c != '\\') { o[n++] = (char)c; p->s++; continue; }
        if (++p->s >= p->e) break;
        c = *p->s++;
        switch (c) {
        case '"': o[n++] = '"'; break; case '\\': o[n++] = '\\'; break; case '/': o[n++] = '/'; break;
        case 'b': o[n++] = '\b'; break; case 'f': o[n++] = '\f'; break; case 'n': o[n++] = '\n'; break;
        case 'r': o[n++] = '\r'; break; case 't': o[n++] = '\t'; break;
        case 'u': {
            unsigned cp, lo;
            if (p->e - p->s < 4 || !hex4(p->s, &cp)) goto bad;
            p->s += 4;
            if (cp >= 0xD800 && cp < 0xDC00 && p->e - p->s >= 6 && p->s[0] == '\\' && p->s[1] == 'u' && hex4(p->s + 2, &lo) && lo >= 0xDC00 && lo < 0xE000) {
                cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00); p->s += 6;
            }
            /* A lone surrogate keeps its 3-byte form; see known gaps in the atlas. */
            n += put8(cp, o + n);
            break;
        }
        default: goto bad;
        }
    }
bad:
    free(o); p->fail = "Unterminated string"; return NULL;
}
static bf_jv *pvalue(P *p);
static bf_jv *pnumber(P *p) {
    const unsigned char *a = p->s;
    if (p->s < p->e && *p->s == '-') p->s++;
    if (p->s >= p->e) goto bad;
    if (*p->s == '0') p->s++;
    else if (*p->s >= '1' && *p->s <= '9') while (p->s < p->e && *p->s >= '0' && *p->s <= '9') p->s++;
    else goto bad;
    if (p->s < p->e && *p->s == '.') {
        p->s++; if (p->s >= p->e || *p->s < '0' || *p->s > '9') goto bad;
        while (p->s < p->e && *p->s >= '0' && *p->s <= '9') p->s++;
    }
    if (p->s < p->e && (*p->s == 'e' || *p->s == 'E')) {
        p->s++; if (p->s < p->e && (*p->s == '+' || *p->s == '-')) p->s++;
        if (p->s >= p->e || *p->s < '0' || *p->s > '9') goto bad;
        while (p->s < p->e && *p->s >= '0' && *p->s <= '9') p->s++;
    }
    {
        size_t n = (size_t)(p->s - a); char buf[512]; char *t = n < sizeof buf ? buf : malloc(n + 1);
        memcpy(t, a, n); t[n] = 0; double d = strtod(t, NULL); if (t != buf) free(t);
        return bf_jv_num(d);
    }
bad:
    p->fail = "Unrecognized token"; return NULL;
}
static int lit(P *p, const char *w) {
    size_t n = strlen(w);
    if ((size_t)(p->e - p->s) < n || memcmp(p->s, w, n)) { p->fail = "Unrecognized token"; return 0; }
    p->s += n; return 1;
}
static bf_jv *pvalue(P *p) {
    ws(p);
    if (p->s >= p->e) { p->fail = "Unexpected EOF"; return NULL; }
    if (++p->depth > 4096) { p->fail = "Exceeded maximum depth"; return NULL; }
    bf_jv *v = NULL;
    switch (*p->s) {
    case '{': {
        p->s++; v = bf_jv_obj(); ws(p);
        if (p->s < p->e && *p->s == '}') { p->s++; break; }
        for (;;) {
            ws(p);
            if (p->s >= p->e || *p->s != '"') { p->fail = "Expected '}'"; goto fail; }
            size_t kl; char *k = pstring(p, &kl); if (!k) goto fail;
            ws(p);
            if (p->s >= p->e || *p->s != ':') { free(k); p->fail = "Expected ':' before value in object property definition"; goto fail; }
            p->s++;
            bf_jv *c = pvalue(p); if (!c) { free(k); goto fail; }
            bf_jv_set(v, k, c); free(k);
            ws(p);
            if (p->s < p->e && *p->s == ',') { p->s++; continue; }
            if (p->s < p->e && *p->s == '}') { p->s++; break; }
            p->fail = "Expected '}'"; goto fail;
        }
        break;
    }
    case '[': {
        p->s++; v = bf_jv_arr(); ws(p);
        if (p->s < p->e && *p->s == ']') { p->s++; break; }
        for (;;) {
            bf_jv *c = pvalue(p); if (!c) goto fail;
            bf_jv_push(v, c); ws(p);
            if (p->s < p->e && *p->s == ',') { p->s++; continue; }
            if (p->s < p->e && *p->s == ']') { p->s++; break; }
            p->fail = "Expected ']'"; goto fail;
        }
        break;
    }
    case '"': { size_t l; char *s = pstring(p, &l); if (!s) return NULL; v = bf_jv_strn(s, l); free(s); break; }
    case 't': if (!lit(p, "true")) return NULL; v = bf_jv_bool(1); break;
    case 'f': if (!lit(p, "false")) return NULL; v = bf_jv_bool(0); break;
    case 'n': if (!lit(p, "null")) return NULL; v = bf_jv_null(); break;
    default: v = pnumber(p); if (!v) return NULL;
    }
    p->depth--;
    return v;
fail:
    bf_jv_free(v); return NULL;
}

bf_jv *jsx_parse(const char *text, size_t len, char *err) {
    const unsigned char *s = (const unsigned char *)text;
    if (!utf8_valid(s, len)) { snprintf(err, ERR_MAX, "Invalid byte sequence"); return NULL; }
    if (len >= 3 && s[0] == 0xEF && s[1] == 0xBB && s[2] == 0xBF) { s += 3; len -= 3; }
    P p = { s, s + len, NULL, 0 };
    bf_jv *v = pvalue(&p);
    if (v) { ws(&p); if (p.s != p.e) { bf_jv_free(v); v = NULL; p.fail = "Unable to parse JSON string"; } }
    if (!v) snprintf(err, ERR_MAX, "JSON Parse error: %s", p.fail ? p.fail : "Unexpected EOF");
    return v;
}

/* ── small JS helpers ───────────────────────────────────────── */
int jsx_truthy(const bf_jv *v) {
    if (!v) return 0;
    switch (v->type) {
    case BF_JV_NULL: return 0;
    case BF_JV_BOOL: return v->b;
    case BF_JV_NUM: return v->n != 0 && v->n == v->n;
    case BF_JV_STR: return v->len > 0;
    default: return 1;
    }
}
bf_jv *jsx_get(const bf_jv *v, const char *key) { return v && v->type == BF_JV_OBJ ? bf_jv_get(v, key) : NULL; }
void jsx_set_opt(bf_jv *obj, const char *key, const bf_jv *value) { if (value) bf_jv_set(obj, key, bf_jv_clone(value)); }
int jsx_str_is(const bf_jv *v, const char *s) { return v && v->type == BF_JV_STR && v->len == strlen(s) && !memcmp(v->s, s, v->len); }

/* Length in bytes of a JS WhiteSpace/LineTerminator at s, else 0. */
static size_t js_space(const unsigned char *s, size_t n) {
    if (!n) return 0;
    if (s[0] == ' ' || (s[0] >= 0x09 && s[0] <= 0x0D)) return 1;
    if (n >= 2 && s[0] == 0xC2 && s[1] == 0xA0) return 2;
    if (n >= 3) {
        unsigned cp = (unsigned)(s[0] & 0x0F) << 12 | (unsigned)(s[1] & 0x3F) << 6 | (s[2] & 0x3F);
        if ((s[0] & 0xF0) == 0xE0 && (cp == 0x1680 || (cp >= 0x2000 && cp <= 0x200A) || cp == 0x2028 || cp == 0x2029 ||
            cp == 0x202F || cp == 0x205F || cp == 0x3000 || cp == 0xFEFF)) return 3;
    }
    return 0;
}
char *jsx_trim(const char *s, size_t len) {
    const unsigned char *a = (const unsigned char *)s, *e = a + len;
    for (size_t k; a < e && (k = js_space(a, (size_t)(e - a))); ) a += k;
    for (;;) {   /* trailing: test every candidate width */
        if (e > a && js_space(e - 1, 1) == 1) { e--; continue; }
        if (e - a >= 2 && js_space(e - 2, 2) == 2) { e -= 2; continue; }
        if (e - a >= 3 && js_space(e - 3, 3) == 3) { e -= 3; continue; }
        break;
    }
    char *o = malloc((size_t)(e - a) + 1);
    memcpy(o, a, (size_t)(e - a)); o[e - a] = 0;
    return o;
}

/* Number(string): StringToNumber of ECMA-262. */
double jsx_to_number(const char *s) {
    char *t = jsx_trim(s, strlen(s)); const char *p = t; double r = NAN;
    if (!*p) { r = 0; goto out; }
    if (p[0] == '0' && (p[1] == 'x' || p[1] == 'X' || p[1] == 'o' || p[1] == 'O' || p[1] == 'b' || p[1] == 'B')) {
        int base = (p[1] | 32) == 'x' ? 16 : (p[1] | 32) == 'o' ? 8 : 2; const char *q = p + 2; double v = 0;
        if (!*q) goto out;
        for (; *q; q++) {
            int d = *q >= '0' && *q <= '9' ? *q - '0' : (*q | 32) >= 'a' && (*q | 32) <= 'f' ? (*q | 32) - 'a' + 10 : 99;
            if (d >= base) goto out;
            v = v * base + d;
        }
        if (base == 16) v = strtod(p, NULL);   /* exact rounding for long hex */
        r = v; goto out;
    }
    {
        const char *q = p; int digits = 0;
        if (*q == '+' || *q == '-') q++;
        if (!strcmp(q, "Infinity")) { r = *p == '-' ? -INFINITY : INFINITY; goto out; }
        while (*q >= '0' && *q <= '9') { q++; digits++; }
        if (*q == '.') { q++; while (*q >= '0' && *q <= '9') { q++; digits++; } }
        if (!digits) goto out;
        if (*q == 'e' || *q == 'E') {
            q++; if (*q == '+' || *q == '-') q++;
            if (*q < '0' || *q > '9') goto out;
            while (*q >= '0' && *q <= '9') q++;
        }
        if (*q) goto out;
        r = strtod(p, NULL);
    }
out:
    free(t); return r;
}
int jsx_safe_integer(double d) { return isfinite(d) && d == floor(d) && fabs(d) <= 9007199254740991.0; }

void jsx_redact(char *msg, const char *secret, const char *mark, size_t units) {
    if (secret && *secret) {
        size_t sl = strlen(secret), ml = strlen(mark);
        char *o = malloc(ERR_MAX * 2), *w = o; const char *r = msg;
        for (const char *f; (f = strstr(r, secret)); r = f + sl) {
            memcpy(w, r, (size_t)(f - r)); w += f - r; memcpy(w, mark, ml); w += ml;
        }
        size_t rest = strlen(r); memcpy(w, r, rest); w[rest] = 0;
        size_t n = strlen(o); if (n >= ERR_MAX) n = ERR_MAX - 1;
        memcpy(msg, o, n); msg[n] = 0; free(o);
    }
    /* slice(0, units) counts UTF-16 code units; a split pair leaves a lone
     * high surrogate, which a stream write encodes as U+FFFD. */
    size_t u = 0, i = 0, n = strlen(msg);
    while (i < n) {
        unsigned c = (unsigned char)msg[i];
        size_t k = c < 0x80 ? 1 : (c & 0xE0) == 0xC0 ? 2 : (c & 0xF0) == 0xE0 ? 3 : 4;
        size_t w = k == 4 ? 2 : 1;
        if (u + w > units) {
            if (w == 2 && u + 1 == units && i + 3 < ERR_MAX) { msg[i] = (char)0xEF; msg[i + 1] = (char)0xBF; msg[i + 2] = (char)0xBD; i += 3; }
            break;
        }
        u += w; i += k;
    }
    msg[i] = 0;
}

void jsx_print(FILE *out, const bf_jv *v) {
    bf_jsonw w; bf_jsonw_init(&w, 2);
    bf_jv_write(&w, v, BF_JV_SORT_NONE);
    fwrite(w.buf ? w.buf : "", 1, w.len, out); fputc('\n', out);
    bf_jsonw_free(&w);
}
