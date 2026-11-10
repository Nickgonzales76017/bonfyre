/* bf_factor: exact variable elimination over Boolean factor graphs, with
 * per-component structural digests shared across problems. See bf_factor.h. */
#include "bf_factor.h"
#include "bonfyre.h"

#include <stdlib.h>
#include <string.h>

typedef struct {
    uint32_t k;
    uint32_t *scope;
    uint64_t *table;           /* 2^k entries */
} Factor;

struct bf_factor_graph {
    uint32_t nvars;
    Factor *f;
    size_t n, cap;
};

/* ---------- graph ---------- */

bf_factor_graph *bf_fg_new(uint32_t nvars) {
    bf_factor_graph *g = calloc(1, sizeof *g);
    if (g) g->nvars = nvars;
    return g;
}

void bf_fg_free(bf_factor_graph *g) {
    if (!g) return;
    for (size_t i = 0; i < g->n; i++) { free(g->f[i].scope); free(g->f[i].table); }
    free(g->f); free(g);
}

uint32_t bf_fg_nvars(const bf_factor_graph *g) { return g ? g->nvars : 0; }

int bf_fg_add_factor(bf_factor_graph *g, const uint32_t *scope, uint32_t k,
                     const uint64_t *table) {
    if (!g || k > BF_FG_MAX_ARITY || (k && !scope) || !table) return BF_FG_EINVAL;
    for (uint32_t i = 0; i < k; i++) {
        if (scope[i] >= g->nvars) return BF_FG_EINVAL;
        for (uint32_t j = 0; j < i; j++) if (scope[j] == scope[i]) return BF_FG_EINVAL;
    }
    if (g->n == g->cap) {
        size_t cap = g->cap ? g->cap * 2 : 16;
        Factor *f = realloc(g->f, cap * sizeof *f);
        if (!f) return BF_FG_ENOMEM;
        g->f = f; g->cap = cap;
    }
    size_t rows = (size_t)1 << k;
    Factor nf = { k, k ? malloc(k * sizeof(uint32_t)) : NULL, malloc(rows * sizeof(uint64_t)) };
    if ((k && !nf.scope) || !nf.table) { free(nf.scope); free(nf.table); return BF_FG_ENOMEM; }
    if (k) memcpy(nf.scope, scope, k * sizeof(uint32_t));
    memcpy(nf.table, table, rows * sizeof(uint64_t));
    g->f[g->n++] = nf;
    return BF_FG_OK;
}

int bf_fg_add_clause(bf_factor_graph *g, const int32_t *lits, uint32_t n) {
    if (!g || (n && !lits)) return BF_FG_EINVAL;
    uint32_t vars[BF_FG_MAX_ARITY]; int sign[BF_FG_MAX_ARITY]; uint32_t k = 0;
    for (uint32_t i = 0; i < n; i++) {
        int32_t l = lits[i];
        if (l == 0 || l == INT32_MIN) return BF_FG_EINVAL;
        uint32_t v = (uint32_t)(l < 0 ? -l : l) - 1; int s = l > 0;
        if (v >= g->nvars) return BF_FG_EINVAL;
        uint32_t j = 0;
        while (j < k && vars[j] != v) j++;
        if (j < k) { if (sign[j] != s) return BF_FG_OK; continue; }  /* tautology / repeat */
        if (k == BF_FG_MAX_ARITY) return BF_FG_EINVAL;
        vars[k] = v; sign[k] = s; k++;
    }
    uint64_t *table = malloc(((size_t)1 << k) * sizeof(uint64_t));
    if (!table) return BF_FG_ENOMEM;
    size_t rows = (size_t)1 << k, falsifying = 0;
    for (uint32_t i = 0; i < k; i++) if (!sign[i]) falsifying |= (size_t)1 << i;
    for (size_t r = 0; r < rows; r++) table[r] = r != falsifying;
    int rc = bf_fg_add_factor(g, vars, k, table);
    free(table);
    return rc;
}

/* ---------- exact arithmetic ---------- */

static const bf_fg_count_t U128_MAX = ~(bf_fg_count_t)0;
static int mul_ok(bf_fg_count_t a, bf_fg_count_t b, bf_fg_count_t *out) {
    if (a && b > U128_MAX / a) return 0;
    *out = a * b; return 1;
}
static int add_ok(bf_fg_count_t a, bf_fg_count_t b, bf_fg_count_t *out) {
    if (b > U128_MAX - a) return 0;
    *out = a + b; return 1;
}

char *bf_fg_count_str(bf_fg_count_t v, char out[40]) {
    char tmp[40]; int n = 0;
    do { tmp[n++] = (char)('0' + (int)(v % 10)); v /= 10; } while (v);
    for (int i = 0; i < n; i++) out[i] = tmp[n - 1 - i];
    out[n] = 0;
    return out;
}

/* ---------- shared cache: digest -> count ---------- */

typedef struct { uint8_t key[32]; bf_fg_count_t value; int used; } Slot;
struct bf_fg_cache { Slot *slot; size_t cap, n; };

bf_fg_cache *bf_fg_cache_new(void) {
    bf_fg_cache *c = calloc(1, sizeof *c);
    if (!c) return NULL;
    c->cap = 256; c->slot = calloc(c->cap, sizeof(Slot));
    if (!c->slot) { free(c); return NULL; }
    return c;
}
void bf_fg_cache_free(bf_fg_cache *c) { if (c) { free(c->slot); free(c); } }
size_t bf_fg_cache_size(const bf_fg_cache *c) { return c ? c->n : 0; }

static size_t slot_of(const bf_fg_cache *c, const uint8_t key[32]) {
    size_t h; memcpy(&h, key, sizeof h);
    size_t i = h & (c->cap - 1);
    while (c->slot[i].used && memcmp(c->slot[i].key, key, 32)) i = (i + 1) & (c->cap - 1);
    return i;
}
static int cache_get(const bf_fg_cache *c, const uint8_t key[32], bf_fg_count_t *v) {
    if (!c) return 0;
    size_t i = slot_of(c, key);
    if (!c->slot[i].used) return 0;
    *v = c->slot[i].value; return 1;
}
static int cache_put(bf_fg_cache *c, const uint8_t key[32], bf_fg_count_t v) {
    if (!c) return 1;
    if ((c->n + 1) * 2 > c->cap) {
        size_t cap = c->cap * 2; Slot *old = c->slot, *s = calloc(cap, sizeof(Slot));
        if (!s) return 0;
        size_t oldcap = c->cap; c->slot = s; c->cap = cap;
        for (size_t i = 0; i < oldcap; i++) if (old[i].used) c->slot[slot_of(c, old[i].key)] = old[i];
        free(old);
    }
    size_t i = slot_of(c, key);
    if (!c->slot[i].used) { c->slot[i].used = 1; memcpy(c->slot[i].key, key, 32); c->n++; }
    c->slot[i].value = v;
    return 1;
}

/* ---------- elimination over one component ---------- */

typedef struct {
    uint32_t k;
    uint32_t scope[BF_FG_MAX_WIDTH];
    bf_fg_count_t *t;          /* 2^k entries */
    int live;
} Work;

static int component_count(const bf_factor_graph *g, const size_t *fids, size_t nf,
                           uint32_t max_width, bf_fg_count_t *out, bf_fg_stats *st) {
    int rc = BF_FG_ENOMEM;
    size_t cap = nf * 2 + 4, n = 0;
    Work *w = calloc(cap, sizeof *w);
    if (!w) return rc;
    for (size_t i = 0; i < nf; i++) {
        const Factor *f = &g->f[fids[i]];
        size_t rows = (size_t)1 << f->k;
        w[n].k = f->k; memcpy(w[n].scope, f->scope, f->k * sizeof(uint32_t));
        if (!(w[n].t = malloc(rows * sizeof(bf_fg_count_t)))) goto done;
        for (size_t r = 0; r < rows; r++) w[n].t[r] = f->table[r];
        w[n].live = 1; n++;
    }
    bf_fg_count_t product = 1;
    for (;;) {
        /* Pick the variable whose eliminated union scope is smallest. */
        uint32_t best = UINT32_MAX, best_width = UINT32_MAX;
        for (size_t i = 0; i < n; i++) {
            if (!w[i].live) continue;
            for (uint32_t s = 0; s < w[i].k; s++) {
                uint32_t v = w[i].scope[s], u[2 * BF_FG_MAX_WIDTH], un = 0, too_wide = 0;
                for (size_t j = 0; j < n && !too_wide; j++) {
                    if (!w[j].live) continue;
                    int has = 0;
                    for (uint32_t t = 0; t < w[j].k; t++) if (w[j].scope[t] == v) has = 1;
                    if (!has) continue;
                    for (uint32_t t = 0; t < w[j].k; t++) {
                        uint32_t x = w[j].scope[t], seen = 0;
                        for (uint32_t q = 0; q < un; q++) if (u[q] == x) seen = 1;
                        if (!seen) { if (un == 2 * BF_FG_MAX_WIDTH) { too_wide = 1; break; } u[un++] = x; }
                    }
                }
                uint32_t width = too_wide ? UINT32_MAX - 1 : un;
                if (width < best_width) { best_width = width; best = v; }
            }
        }
        if (best == UINT32_MAX) break;                  /* only constants remain */
        if (best_width > max_width) { rc = BF_FG_EWIDTH; goto done; }
        if (st && best_width > st->max_width) st->max_width = best_width;

        /* Multiply every factor holding best over U, then sum best out. */
        uint32_t u[BF_FG_MAX_WIDTH], un = 0;
        size_t members[64], nm = 0; Work *more = NULL;
        size_t *mem = members, memcap = 64;
        for (size_t j = 0; j < n; j++) {
            if (!w[j].live) continue;
            int has = 0;
            for (uint32_t t = 0; t < w[j].k; t++) if (w[j].scope[t] == best) has = 1;
            if (!has) continue;
            if (nm == memcap) {
                size_t *grown = malloc(memcap * 2 * sizeof *grown);
                if (!grown) goto done;
                memcpy(grown, mem, nm * sizeof *mem); if (mem != members) free(mem);
                mem = grown; memcap *= 2;
            }
            mem[nm++] = j;
            for (uint32_t t = 0; t < w[j].k; t++) {
                uint32_t x = w[j].scope[t], seen = 0;
                for (uint32_t q = 0; q < un; q++) if (u[q] == x) seen = 1;
                if (!seen) u[un++] = x;
            }
        }
        /* Put best last so summing it out pairs rows r and r | top. */
        for (uint32_t q = 0; q < un; q++) if (u[q] == best) { u[q] = u[un - 1]; u[un - 1] = best; break; }
        size_t rows = (size_t)1 << un, half = rows >> 1;
        bf_fg_count_t *joint = malloc(half * sizeof *joint);
        uint32_t (*pos)[BF_FG_MAX_WIDTH] = malloc(nm * sizeof *pos);
        if (!joint || !pos) { free(joint); free(pos); if (mem != members) free(mem); goto done; }
        for (size_t m = 0; m < nm; m++)
            for (uint32_t t = 0; t < w[mem[m]].k; t++)
                for (uint32_t q = 0; q < un; q++) if (u[q] == w[mem[m]].scope[t]) pos[m][t] = q;
        int overflow = 0;
        for (size_t r = 0; r < half && !overflow; r++) {
            bf_fg_count_t sum = 0;
            for (size_t b = 0; b < 2 && !overflow; b++) {
                size_t a = r | (b << (un - 1));
                bf_fg_count_t p = 1;
                for (size_t m = 0; m < nm && p; m++) {
                    size_t idx = 0;
                    for (uint32_t t = 0; t < w[mem[m]].k; t++) idx |= ((a >> pos[m][t]) & 1u) << t;
                    if (!mul_ok(p, w[mem[m]].t[idx], &p)) overflow = 1;
                }
                if (!overflow && !add_ok(sum, p, &sum)) overflow = 1;
            }
            joint[r] = sum;
        }
        free(pos);
        for (size_t m = 0; m < nm; m++) { free(w[mem[m]].t); w[mem[m]].t = NULL; w[mem[m]].live = 0; }
        if (mem != members) free(mem);
        if (overflow) { free(joint); rc = BF_FG_EOVERFLOW; goto done; }
        if (st) st->eliminations++;
        if (un == 1) {                                   /* reduced to a constant */
            if (!mul_ok(product, joint[0], &product)) { free(joint); rc = BF_FG_EOVERFLOW; goto done; }
            free(joint);
            continue;
        }
        if (n == cap) {
            more = realloc(w, cap * 2 * sizeof *w);
            if (!more) { free(joint); goto done; }
            memset(more + cap, 0, cap * sizeof *more); w = more; cap *= 2;
        }
        w[n].k = un - 1; memcpy(w[n].scope, u, (un - 1) * sizeof(uint32_t));
        w[n].t = joint; w[n].live = 1; n++;
    }
    /* Remaining live factors are constants (k == 0). */
    for (size_t i = 0; i < n; i++)
        if (w[i].live && !mul_ok(product, w[i].t[0], &product)) { rc = BF_FG_EOVERFLOW; goto done; }
    *out = product; rc = BF_FG_OK;
done:
    for (size_t i = 0; i < n; i++) free(w[i].t);
    free(w);
    return rc;
}

/* ---------- components and digests ---------- */

static uint32_t find(uint32_t *p, uint32_t x) { while (p[x] != x) { p[x] = p[p[x]]; x = p[x]; } return x; }

static void digest_component(const bf_factor_graph *g, const size_t *fids, size_t nf,
                             uint32_t *rename, uint8_t out[32]) {
    BfSha256 h; bf_sha256_init(&h);
    uint32_t next = 0;
    const uint8_t tag[] = "bf.factor.component.v1";
    bf_sha256_update(&h, tag, sizeof tag);
    for (size_t i = 0; i < nf; i++) {
        const Factor *f = &g->f[fids[i]];
        uint8_t b[8];
        for (int s = 0; s < 4; s++) b[s] = (uint8_t)(f->k >> (8 * s));
        bf_sha256_update(&h, b, 4);
        for (uint32_t t = 0; t < f->k; t++) {
            uint32_t v = f->scope[t];
            if (rename[v] == UINT32_MAX) rename[v] = next++;
            for (int s = 0; s < 4; s++) b[s] = (uint8_t)(rename[v] >> (8 * s));
            bf_sha256_update(&h, b, 4);
        }
        size_t rows = (size_t)1 << f->k;
        for (size_t r = 0; r < rows; r++) {
            for (int s = 0; s < 8; s++) b[s] = (uint8_t)(f->table[r] >> (8 * s));
            bf_sha256_update(&h, b, 8);
        }
    }
    for (size_t i = 0; i < nf; i++)                     /* leave rename clean */
        for (uint32_t t = 0; t < g->f[fids[i]].k; t++) rename[g->f[fids[i]].scope[t]] = UINT32_MAX;
    bf_sha256_final(&h, out);
}

int bf_fg_count(const bf_factor_graph *g, bf_fg_cache *cache,
                const bf_fg_limits *limits, bf_fg_count_t *out, bf_fg_stats *stats) {
    if (!g || !out) return BF_FG_EINVAL;
    bf_fg_stats st = {0};
    uint32_t max_width = limits && limits->max_width ? limits->max_width : BF_FG_MAX_WIDTH;
    if (max_width > BF_FG_MAX_WIDTH) max_width = BF_FG_MAX_WIDTH;
    int rc = BF_FG_ENOMEM;
    uint32_t nv = g->nvars;
    uint32_t *parent = malloc((nv ? nv : 1) * sizeof *parent);
    uint32_t *rename = malloc((nv ? nv : 1) * sizeof *rename);
    uint8_t *used = calloc(nv ? nv : 1, 1);
    size_t *order = malloc((g->n ? g->n : 1) * sizeof *order);
    uint32_t *root_of = malloc((g->n ? g->n : 1) * sizeof *root_of);
    if (!parent || !rename || !used || !order || !root_of) goto done;
    for (uint32_t v = 0; v < nv; v++) { parent[v] = v; rename[v] = UINT32_MAX; }
    bf_fg_count_t z = 1;
    for (size_t i = 0; i < g->n; i++) {
        const Factor *f = &g->f[i];
        if (!f->k) { if (!mul_ok(z, f->table[0], &z)) { rc = BF_FG_EOVERFLOW; goto done; } continue; }
        used[f->scope[0]] = 1;
        for (uint32_t t = 1; t < f->k; t++) {
            used[f->scope[t]] = 1;
            uint32_t a = find(parent, f->scope[0]), b = find(parent, f->scope[t]);
            if (a != b) parent[b] = a;
        }
    }
    for (uint32_t v = 0; v < nv; v++) if (!used[v]) {
        st.free_variables++;
        if (!mul_ok(z, 2, &z)) { rc = BF_FG_EOVERFLOW; goto done; }
    }
    /* Group factors by component, keeping insertion order inside each. */
    size_t nk = 0;
    for (size_t i = 0; i < g->n; i++) if (g->f[i].k) { root_of[nk] = find(parent, g->f[i].scope[0]); order[nk++] = i; }
    uint8_t *done_mark = calloc(nk ? nk : 1, 1);
    size_t *members = malloc((nk ? nk : 1) * sizeof *members);
    if (!done_mark || !members) { free(done_mark); free(members); goto done; }
    for (size_t i = 0; i < nk; i++) {
        if (done_mark[i]) continue;
        size_t nm = 0;
        for (size_t j = i; j < nk; j++) if (!done_mark[j] && root_of[j] == root_of[i]) { members[nm++] = order[j]; done_mark[j] = 1; }
        st.components++;
        uint8_t key[32]; bf_fg_count_t c;
        digest_component(g, members, nm, rename, key);
        if (cache_get(cache, key, &c)) st.cache_hits++;
        else {
            int r = component_count(g, members, nm, max_width, &c, &st);
            if (r) { rc = r; free(done_mark); free(members); goto done; }
            st.cache_misses++;
            if (!cache_put(cache, key, c)) { free(done_mark); free(members); goto done; }
        }
        if (!mul_ok(z, c, &z)) { rc = BF_FG_EOVERFLOW; free(done_mark); free(members); goto done; }
    }
    free(done_mark); free(members);
    *out = z; rc = BF_FG_OK;
done:
    free(parent); free(rename); free(used); free(order); free(root_of);
    if (stats) *stats = st;
    return rc;
}
