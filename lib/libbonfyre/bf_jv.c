/*
 * bf_jv.c — mutable JSON values. See bf_jv.h.
 */
#include "bf_jv.h"
#include "bonfyre.h"

#include <stdlib.h>
#include <string.h>

/* ── allocation ─────────────────────────────────────────────────────────
 * Ports build and drop millions of small values; the system allocator was
 * half of their run time. Blocks up to 256 bytes come from per-size-class
 * free lists carved from 64 KiB slabs, with the class in an 8-byte header.
 * Not thread-safe: a bf_jv tree belongs to one thread. */
#define JV_CLASSES 5
static const size_t jv_class_size[JV_CLASSES] = {16, 32, 64, 128, 256};
static void *jv_free_list[JV_CLASSES];
static char *jv_slab; static size_t jv_slab_left;

static int jv_class(size_t n) { for (int c = 0; c < JV_CLASSES; c++) if (n <= jv_class_size[c]) return c; return -1; }

static void *jv_alloc(size_t n) {
    int c = jv_class(n);
    if (c < 0) { size_t *b = malloc(n + 8); if (!b) return NULL; b[0] = JV_CLASSES + n; return b + 1; }   /* large: header holds the size */
    void *p = jv_free_list[c];
    if (p) { jv_free_list[c] = *(void **)p; return p; }
    size_t need = jv_class_size[c] + 8;
    if (jv_slab_left < need) { jv_slab = malloc(1 << 16); if (!jv_slab) return NULL; jv_slab_left = 1 << 16; }
    size_t *b = (size_t *)jv_slab; jv_slab += need; jv_slab_left -= need;
    b[0] = (size_t)c;
    return b + 1;
}
static void jv_release(void *p) {
    if (!p) return;
    size_t *b = (size_t *)p - 1;
    if (b[0] >= JV_CLASSES) { free(b); return; }
    *(void **)p = jv_free_list[b[0]]; jv_free_list[b[0]] = p;
}
static size_t jv_capacity(void *p) { size_t *b = (size_t *)p - 1; return b[0] < JV_CLASSES ? jv_class_size[b[0]] : b[0] - JV_CLASSES; }
static void *jv_realloc(void *p, size_t old, size_t n) {
    if (p && n <= jv_capacity(p)) return p;
    void *q = jv_alloc(n);
    if (q && p) memcpy(q, p, old < n ? old : n);
    jv_release(p);
    return q;
}
static char *jv_strdup(const char *s) { size_t n = strlen(s) + 1; char *d = jv_alloc(n); if (d) memcpy(d, s, n); return d; }

static bf_jv *make(bf_jv_type t) {
    bf_jv *v = jv_alloc(sizeof *v);
    if (v) { memset(v, 0, sizeof *v); v->type = t; }
    return v;
}
bf_jv *bf_jv_null(void) { return make(BF_JV_NULL); }
bf_jv *bf_jv_bool(int b) { bf_jv *v = make(BF_JV_BOOL); if (v) v->b = !!b; return v; }
bf_jv *bf_jv_num(double n) { bf_jv *v = make(BF_JV_NUM); if (v) v->n = n; return v; }
bf_jv *bf_jv_strn(const char *s, size_t len) {
    bf_jv *v = make(BF_JV_STR);
    if (!v) return NULL;
    v->s = jv_alloc(len + 1);
    if (len) memcpy(v->s, s, len);
    v->s[len] = 0; v->len = len;
    return v;
}
bf_jv *bf_jv_str(const char *s) { return bf_jv_strn(s ? s : "", s ? strlen(s) : 0); }
bf_jv *bf_jv_arr(void) { return make(BF_JV_ARR); }
bf_jv *bf_jv_obj(void) { return make(BF_JV_OBJ); }

void bf_jv_free(bf_jv *v) {
    if (!v) return;
    for (size_t i = 0; i < v->len && v->items; i++) {
        bf_jv_free(v->items[i]);
        if (v->keys) jv_release(v->keys[i]);
    }
    free(v->ht);
    jv_release(v->items); jv_release(v->keys); jv_release(v->s); jv_release(v);
}

static void grow(bf_jv *v) {
    if (v->len < v->cap) return;
    size_t old = v->cap;
    v->cap = v->cap ? v->cap * 2 : 4;
    v->items = jv_realloc(v->items, sizeof *v->items * old, sizeof *v->items * v->cap);
    if (v->type == BF_JV_OBJ) v->keys = jv_realloc(v->keys, sizeof *v->keys * old, sizeof *v->keys * v->cap);
}

void bf_jv_push(bf_jv *arr, bf_jv *item) {
    if (!arr || arr->type != BF_JV_ARR) { bf_jv_free(item); return; }
    grow(arr);
    arr->items[arr->len++] = item ? item : bf_jv_null();
}
bf_jv *bf_jv_at(const bf_jv *arr, size_t i) {
    return arr && arr->type == BF_JV_ARR && i < arr->len ? arr->items[i] : NULL;
}

static size_t key_hash(const char *s) { size_t h = 1469598103934665603ULL; for (; *s; s++) h = (h ^ (unsigned char)*s) * 1099511628211ULL; return h; }
#define HT_MIN 16
static void ht_rebuild(bf_jv *o) {
    free(o->ht); o->ht = NULL; o->htcap = 0;
    if (o->len < HT_MIN) return;
    size_t cap = 64; while (cap < o->len * 2) cap *= 2;
    o->ht = malloc(sizeof *o->ht * cap); o->htcap = cap;
    for (size_t i = 0; i < cap; i++) o->ht[i] = -1;
    for (size_t i = 0; i < o->len; i++) {
        size_t h = key_hash(o->keys[i]) & (cap - 1);
        while (o->ht[h] >= 0) h = (h + 1) & (cap - 1);
        o->ht[h] = (int)i;
    }
}
static long find(const bf_jv *obj, const char *key) {
    if (!obj || obj->type != BF_JV_OBJ) return -1;
    if (obj->ht) {
        for (size_t h = key_hash(key) & (obj->htcap - 1);; h = (h + 1) & (obj->htcap - 1)) {
            int i = obj->ht[h];
            if (i < 0) return -1;
            if (!strcmp(obj->keys[i], key)) return i;
        }
    }
    char k0 = key[0];
    for (size_t i = 0; i < obj->len; i++) if (obj->keys[i][0] == k0 && !strcmp(obj->keys[i], key)) return (long)i;
    return -1;
}
/* JS property order: array-index keys ascending, then insertion order. */
static long index_key(const char *k) {
    size_t n = strlen(k);
    if (n < 1 || n > 10 || (n > 1 && k[0] == '0')) return -1;
    long v = 0;
    for (size_t i = 0; i < n; i++) { if (k[i] < '0' || k[i] > '9') return -1; v = v * 10 + (k[i] - '0'); }
    return v < 4294967295L ? v : -1;
}
/* Stored order is JS property order, so iteration and emission agree with JS. */
void bf_jv_set(bf_jv *obj, const char *key, bf_jv *value) {
    if (!obj || obj->type != BF_JV_OBJ) { bf_jv_free(value); return; }
    if (!value) value = bf_jv_null();
    long at = find(obj, key);
    if (at >= 0) { bf_jv_free(obj->items[at]); obj->items[at] = value; return; }
    grow(obj);
    size_t pos = obj->len;
    long k = index_key(key);
    if (k >= 0) {
        pos = 0;
        while (pos < obj->len && index_key(obj->keys[pos]) >= 0 && index_key(obj->keys[pos]) < k) pos++;
        memmove(obj->keys + pos + 1, obj->keys + pos, sizeof *obj->keys * (obj->len - pos));
        memmove(obj->items + pos + 1, obj->items + pos, sizeof *obj->items * (obj->len - pos));
    }
    obj->keys[pos] = jv_strdup(key);
    obj->items[pos] = value;
    obj->len++;
    if (pos != obj->len - 1 || !obj->ht || obj->len * 2 > obj->htcap) ht_rebuild(obj);   /* insert shifted indexes, or table full */
    else {
        size_t h = key_hash(key) & (obj->htcap - 1);
        while (obj->ht[h] >= 0) h = (h + 1) & (obj->htcap - 1);
        obj->ht[h] = (int)pos;
    }
}
bf_jv *bf_jv_get(const bf_jv *obj, const char *key) { long at = find(obj, key); return at >= 0 ? obj->items[at] : NULL; }
int bf_jv_has(const bf_jv *obj, const char *key) { return find(obj, key) >= 0; }
int bf_jv_del(bf_jv *obj, const char *key) {
    long at = find(obj, key);
    if (at < 0) return 0;
    bf_jv_free(obj->items[at]); jv_release(obj->keys[at]);
    memmove(obj->items + at, obj->items + at + 1, sizeof *obj->items * (obj->len - (size_t)at - 1));
    memmove(obj->keys + at, obj->keys + at + 1, sizeof *obj->keys * (obj->len - (size_t)at - 1));
    obj->len--;
    if (obj->ht) ht_rebuild(obj);
    return 1;
}
bf_jv *bf_jv_path(const bf_jv *v, const char *dotted) {
    char part[256]; const char *p = dotted;
    while (v && *p) {
        size_t n = strcspn(p, ".");
        if (n >= sizeof part) return NULL;
        memcpy(part, p, n); part[n] = 0;
        v = bf_jv_get(v, part);
        p += n; if (*p == '.') p++;
    }
    return (bf_jv *)v;
}

const char *bf_jv_s(const bf_jv *v) { return v && v->type == BF_JV_STR ? v->s : NULL; }
double bf_jv_n(const bf_jv *v, double fallback) { return v && v->type == BF_JV_NUM ? v->n : fallback; }
int bf_jv_is(const bf_jv *v, bf_jv_type t) { return v && v->type == t; }

bf_jv *bf_jv_clone(const bf_jv *v) {
    if (!v) return NULL;
    switch (v->type) {
    case BF_JV_NULL: return bf_jv_null();
    case BF_JV_BOOL: return bf_jv_bool(v->b);
    case BF_JV_NUM: return bf_jv_num(v->n);
    case BF_JV_STR: return bf_jv_strn(v->s, v->len);
    case BF_JV_ARR: {
        bf_jv *a = bf_jv_arr();
        for (size_t i = 0; i < v->len; i++) bf_jv_push(a, bf_jv_clone(v->items[i]));
        return a;
    }
    case BF_JV_OBJ: {
        bf_jv *o = bf_jv_obj();
        for (size_t i = 0; i < v->len; i++) bf_jv_set(o, v->keys[i], bf_jv_clone(v->items[i]));
        return o;
    }
    }
    return NULL;
}

bf_jv *bf_jv_from_node(const bf_json_doc_t *doc, const bf_json_node_t *n) {
    if (!n) return NULL;
    switch (n->type) {
    case BF_JSON_NULL: return bf_jv_null();
    case BF_JSON_BOOL: return bf_jv_bool(n->val.b);
    case BF_JSON_INT: return bf_jv_num((double)n->val.i);
    case BF_JSON_DOUBLE: return bf_jv_num(n->val.d);
    case BF_JSON_STRING: {
        size_t len; char *s = bf_json_unescape(n->val.str.ptr, n->val.str.len, &len);
        bf_jv *v = bf_jv_strn(s, len); free(s); return v;
    }
    case BF_JSON_ARRAY: {
        bf_jv *a = bf_jv_arr();
        for (const bf_json_node_t *c = bf_json_child_first(doc, n); c; c = bf_json_child_next(doc, c))
            bf_jv_push(a, bf_jv_from_node(doc, c));
        return a;
    }
    case BF_JSON_OBJECT: {
        bf_jv *o = bf_jv_obj();
        for (const bf_json_node_t *c = bf_json_child_first(doc, n); c; c = bf_json_child_next(doc, c)) {
            char *k = bf_json_unescape(c->key, c->key_len, NULL);
            bf_jv_set(o, k, bf_jv_from_node(doc, c));
            free(k);
        }
        return o;
    }
    }
    return NULL;
}

bf_jv *bf_jv_parse(const char *text, size_t len, char *err, size_t err_sz) {
    bf_json_doc_t *doc = bf_json_parse(text, len, err, err_sz);
    if (!doc) return NULL;
    bf_jv *v = bf_jv_from_node(doc, bf_json_root(doc));
    bf_json_free(doc);
    return v;
}

int bf_jv_equal(const bf_jv *a, const bf_jv *b) {
    if (!a || !b) return a == b;
    if (a->type != b->type) return 0;
    switch (a->type) {
    case BF_JV_NULL: return 1;
    case BF_JV_BOOL: return a->b == b->b;
    case BF_JV_NUM: return a->n == b->n || (a->n != a->n && b->n != b->n);
    case BF_JV_STR: return a->len == b->len && !memcmp(a->s, b->s, a->len);
    case BF_JV_ARR:
        if (a->len != b->len) return 0;
        for (size_t i = 0; i < a->len; i++) if (!bf_jv_equal(a->items[i], b->items[i])) return 0;
        return 1;
    case BF_JV_OBJ:
        if (a->len != b->len) return 0;
        for (size_t i = 0; i < a->len; i++) if (!bf_jv_equal(a->items[i], bf_jv_get(b, a->keys[i]))) return 0;
        return 1;
    }
    return 0;
}

typedef struct { const char *key; size_t pos; long idx; } Slot;
static int by_bytes(const void *pa, const void *pb) { return strcmp(((const Slot *)pa)->key, ((const Slot *)pb)->key); }
static int by_js_sorted(const void *pa, const void *pb) {
    const Slot *a = pa, *b = pb;
    if (a->idx >= 0 && b->idx >= 0) return (a->idx > b->idx) - (a->idx < b->idx);
    if (a->idx >= 0) return -1;
    if (b->idx >= 0) return 1;
    return strcmp(a->key, b->key);
}

void bf_jv_write(bf_jsonw *w, const bf_jv *v, int sorted) {
    if (!v) { bf_jsonw_null(w); return; }
    switch (v->type) {
    case BF_JV_NULL: bf_jsonw_null(w); break;
    case BF_JV_BOOL: bf_jsonw_bool(w, v->b); break;
    case BF_JV_NUM: bf_jsonw_num(w, v->n); break;
    case BF_JV_STR: bf_jsonw_strn(w, v->s, v->len); break;
    case BF_JV_ARR:
        bf_jsonw_arr(w);
        for (size_t i = 0; i < v->len; i++) bf_jv_write(w, v->items[i], sorted);
        bf_jsonw_arr_end(w);
        break;
    case BF_JV_OBJ: {
        bf_jsonw_obj(w);
        if (sorted == BF_JV_SORT_NONE) {   /* stored order is JS order */
            for (size_t i = 0; i < v->len; i++) { bf_jsonw_key(w, v->keys[i]); bf_jv_write(w, v->items[i], sorted); }
            bf_jsonw_obj_end(w);
            break;
        }
        Slot local[32];
        Slot *slots = v->len <= 32 ? local : malloc(sizeof *slots * v->len);
        for (size_t i = 0; i < v->len; i++) { slots[i].key = v->keys[i]; slots[i].pos = i; slots[i].idx = sorted == BF_JV_SORT_JS ? index_key(v->keys[i]) : -1; }
        int (*cmp)(const void *, const void *) = sorted == BF_JV_SORT_BYTES ? by_bytes : by_js_sorted;
        if (v->len <= 32) {
            for (size_t i = 1; i < v->len; i++) {
                Slot t = slots[i]; size_t j = i;
                while (j > 0 && cmp(&slots[j - 1], &t) > 0) { slots[j] = slots[j - 1]; j--; }
                slots[j] = t;
            }
        } else qsort(slots, v->len, sizeof *slots, cmp);
        for (size_t i = 0; i < v->len; i++) {
            bf_jsonw_key(w, slots[i].key);
            bf_jv_write(w, v->items[slots[i].pos], sorted);
        }
        if (slots != local) free(slots);
        bf_jsonw_obj_end(w);
        break;
    }
    }
}

char *bf_jv_dump(const bf_jv *v, int indent, int sorted, size_t *len_out) {
    bf_jsonw w; bf_jsonw_init(&w, indent);
    bf_jv_write(&w, v, sorted);
    if (!w.buf) { w.buf = calloc(1, 1); w.len = 0; }
    if (len_out) *len_out = w.len;
    return w.buf;   /* caller frees */
}

void bf_jv_digest(const bf_jv *v, char hex[65]) { bf_jv_digest_mode(v, BF_JV_SORT_BYTES, hex); }
void bf_jv_digest_members(int mode, int n, const char *const *keys, const bf_jv *const *values, char hex[65]) {
    /* {keys[0]: values[0], ...} digested without building the object; keys must be distinct */
    int order[64]; if (n > 64) n = 64;
    for (int i = 0; i < n; i++) order[i] = i;
    for (int i = 1; i < n; i++) for (int j = i; j > 0 && strcmp(keys[order[j - 1]], keys[order[j]]) > 0; j--) { int t = order[j]; order[j] = order[j - 1]; order[j - 1] = t; }
    bf_jsonw w; bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    for (int i = 0; i < n; i++) { bf_jsonw_key(&w, keys[order[i]]); bf_jv_write(&w, values[order[i]], mode); }
    bf_jsonw_obj_end(&w);
    bf_sha256_hex((const uint8_t *)(w.buf ? w.buf : ""), w.len, hex);
    bf_jsonw_free(&w);
}

void bf_jv_digest_mode(const bf_jv *v, int mode, char hex[65]) {
    size_t n; char *s = bf_jv_dump(v, 0, mode, &n);
    bf_sha256_hex((const uint8_t *)s, n, hex);
    free(s);
}

static void sort_keys(bf_jv *v, int mode) {
    if (!v) return;
    if (v->type == BF_JV_ARR) { for (size_t i = 0; i < v->len; i++) sort_keys(v->items[i], mode); return; }
    if (v->type != BF_JV_OBJ) return;
    Slot *slots = malloc(sizeof *slots * (v->len ? v->len : 1));
    for (size_t i = 0; i < v->len; i++) { slots[i].key = v->keys[i]; slots[i].pos = i; slots[i].idx = index_key(v->keys[i]); }
    qsort(slots, v->len, sizeof *slots, mode == BF_JV_SORT_BYTES ? by_bytes : by_js_sorted);
    char **keys = malloc(sizeof *keys * (v->len ? v->len : 1)); bf_jv **items = malloc(sizeof *items * (v->len ? v->len : 1));
    for (size_t i = 0; i < v->len; i++) { keys[i] = v->keys[slots[i].pos]; items[i] = v->items[slots[i].pos]; sort_keys(items[i], mode); }
    memcpy(v->keys, keys, sizeof *keys * v->len); memcpy(v->items, items, sizeof *items * v->len);
    free(keys); free(items); free(slots);
    if (v->ht) ht_rebuild(v);
}
void bf_jv_sort_keys(bf_jv *v, int mode) { sort_keys(v, mode); }
