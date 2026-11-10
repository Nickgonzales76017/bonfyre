/* bf_factor against brute-force enumeration. Every count asserted here is
 * also computed by summing the factor product over all 2^n assignments. */
#include "bf_factor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int failures, checks;
#define CHECK(c, ...) do { checks++; if (!(c)) { failures++; fprintf(stderr, "FAIL %s:%d ", __func__, __LINE__); fprintf(stderr, __VA_ARGS__); fputc('\n', stderr); } } while (0)

static uint64_t rng = 0x9e3779b97f4a7c15ull;
static uint64_t next(void) { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; }

/* A mirror of every factor added, for brute force. */
typedef struct { uint32_t k, scope[BF_FG_MAX_ARITY]; uint64_t *t; } Mirror;
typedef struct { uint32_t n; Mirror m[512]; size_t nm; } Problem;

static void mirror_add(Problem *p, const uint32_t *scope, uint32_t k, const uint64_t *t) {
    Mirror *m = &p->m[p->nm++];
    m->k = k; memcpy(m->scope, scope, k * sizeof *scope);
    m->t = malloc(((size_t)1 << k) * sizeof *t); memcpy(m->t, t, ((size_t)1 << k) * sizeof *t);
}
static void mirror_free(Problem *p) { for (size_t i = 0; i < p->nm; i++) free(p->m[i].t); p->nm = 0; }

static bf_fg_count_t brute(const Problem *p) {
    bf_fg_count_t z = 0;
    for (uint64_t a = 0; a < (1ull << p->n); a++) {
        bf_fg_count_t prod = 1;
        for (size_t i = 0; i < p->nm && prod; i++) {
            size_t idx = 0;
            for (uint32_t t = 0; t < p->m[i].k; t++) idx |= (size_t)((a >> p->m[i].scope[t]) & 1) << t;
            prod *= p->m[i].t[idx];
        }
        z += prod;
    }
    return z;
}

/* Clause as a mirrored 0/1 table over its distinct variables. */
static void clause_both(bf_factor_graph *g, Problem *p, const int32_t *lits, uint32_t n) {
    CHECK(bf_fg_add_clause(g, lits, n) == BF_FG_OK, "add clause");
    uint32_t vars[BF_FG_MAX_ARITY]; int sign[BF_FG_MAX_ARITY]; uint32_t k = 0;
    for (uint32_t i = 0; i < n; i++) {
        uint32_t v = (uint32_t)abs(lits[i]) - 1; int s = lits[i] > 0, j;
        for (j = 0; j < (int)k && vars[j] != v; j++) ;
        if (j < (int)k) { if (sign[j] != s) return; continue; }
        vars[k] = v; sign[k] = s; k++;
    }
    uint64_t t[1 << 8]; size_t rows = (size_t)1 << k;
    for (size_t r = 0; r < rows; r++) {
        int sat = 0;
        for (uint32_t i = 0; i < k; i++) if ((int)((r >> i) & 1) == sign[i]) sat = 1;
        t[r] = sat;
    }
    mirror_add(p, vars, k, t);
}

static void random_cnf(void) {
    for (int trial = 0; trial < 400; trial++) {
        Problem p = { .n = 4 + (uint32_t)(next() % 13) };
        bf_factor_graph *g = bf_fg_new(p.n);
        int clauses = (int)(next() % (p.n * 4 + 1));
        for (int c = 0; c < clauses; c++) {
            int32_t lits[3]; uint32_t len = 1 + (uint32_t)(next() % 3);
            for (uint32_t i = 0; i < len; i++) { int32_t v = 1 + (int32_t)(next() % p.n); lits[i] = (next() & 1) ? v : -v; }
            clause_both(g, &p, lits, len);
        }
        bf_fg_count_t z; bf_fg_stats st;
        int rc = bf_fg_count(g, NULL, NULL, &z, &st);
        bf_fg_count_t want = brute(&p);
        char a[40], b[40];
        CHECK(rc == BF_FG_OK && z == want, "cnf trial %d n=%u got %s want %s rc %d", trial, p.n, bf_fg_count_str(z, a), bf_fg_count_str(want, b), rc);
        bf_fg_free(g); mirror_free(&p);
    }
}

static void random_weighted(void) {
    for (int trial = 0; trial < 300; trial++) {
        Problem p = { .n = 3 + (uint32_t)(next() % 12) };
        bf_factor_graph *g = bf_fg_new(p.n);
        int factors = 1 + (int)(next() % (p.n * 2));
        for (int f = 0; f < factors; f++) {
            uint32_t k = (uint32_t)(next() % 4), scope[4]; uint64_t t[16];
            for (uint32_t i = 0; i < k; i++) {
                uint32_t v; int dup;
                do { v = (uint32_t)(next() % p.n); dup = 0; for (uint32_t j = 0; j < i; j++) if (scope[j] == v) dup = 1; } while (dup);
                scope[i] = v;
            }
            if (k > p.n) k = p.n;
            for (size_t r = 0; r < ((size_t)1 << k); r++) t[r] = next() % 7;
            CHECK(bf_fg_add_factor(g, scope, k, t) == BF_FG_OK, "add factor");
            mirror_add(&p, scope, k, t);
        }
        bf_fg_count_t z; int rc = bf_fg_count(g, NULL, NULL, &z, NULL);
        CHECK(rc == BF_FG_OK && z == brute(&p), "weighted trial %d", trial);
        bf_fg_free(g); mirror_free(&p);
    }
}

static void clause_lowering(void) {
    bf_factor_graph *g = bf_fg_new(3); bf_fg_count_t z;
    int32_t taut[] = {1, -1, 2};
    CHECK(bf_fg_add_clause(g, taut, 3) == BF_FG_OK, "tautology accepted");
    CHECK(bf_fg_count(g, NULL, NULL, &z, NULL) == BF_FG_OK && z == 8, "tautology adds nothing");
    int32_t bad[] = {4};
    CHECK(bf_fg_add_clause(g, bad, 1) == BF_FG_EINVAL, "out-of-range literal refused");
    int32_t zero[] = {0};
    CHECK(bf_fg_add_clause(g, zero, 1) == BF_FG_EINVAL, "zero literal refused");
    CHECK(bf_fg_add_clause(g, NULL, 0) == BF_FG_OK, "empty clause accepted");
    CHECK(bf_fg_count(g, NULL, NULL, &z, NULL) == BF_FG_OK && z == 0, "empty clause makes Z zero");
    bf_fg_free(g);
    g = bf_fg_new(2); uint32_t dup[] = {0, 0}; uint64_t t[4] = {1, 1, 1, 1};
    CHECK(bf_fg_add_factor(g, dup, 2, t) == BF_FG_EINVAL, "repeated scope variable refused");
    bf_fg_free(g);
}

/* z = a AND b by three clauses: the auxiliary z is determined by a and b, so
 * the count over (a, b, z) equals the count over (a, b) alone. */
static void auxiliary_definitions_preserve_counts(void) {
    bf_factor_graph *g = bf_fg_new(4);  /* a=1 b=2 c=3 z=4 */
    int32_t c1[] = {-4, 1}, c2[] = {-4, 2}, c3[] = {4, -1, -2}, use[] = {4, 3};
    bf_fg_add_clause(g, c1, 2); bf_fg_add_clause(g, c2, 2); bf_fg_add_clause(g, c3, 3); bf_fg_add_clause(g, use, 2);
    bf_fg_count_t z; bf_fg_count(g, NULL, NULL, &z, NULL);
    /* (a AND b) OR c over a, b, c: 8 - |not(ab) and not c| = 8 - 3 = 5. */
    CHECK(z == 5, "auxiliary encoding preserves the original count (got %llu)", (unsigned long long)z);
    bf_fg_free(g);
}

/* One family of problems built from shared typed pieces: each piece is
 * eliminated once and then answered from the cache with identical results. */
static void shared_structure_is_computed_once(void) {
    enum { PIECES = 6, PROBLEMS = 2000, PER = 8, VARS = 10 };
    int32_t piece[PIECES][12][3]; int len[PIECES], width[PIECES][12];
    for (int p = 0; p < PIECES; p++) {
        len[p] = 6 + (int)(next() % 6);
        for (int c = 0; c < len[p]; c++) {
            width[p][c] = 1 + (int)(next() % 3);
            for (int i = 0; i < width[p][c]; i++) { int32_t v = 1 + (int32_t)(next() % VARS); piece[p][c][i] = (next() & 1) ? v : -v; }
        }
    }
    bf_fg_cache *cache = bf_fg_cache_new();
    uint64_t hits = 0, misses = 0, mismatches = 0;
    double shared_s = 0, fresh_s = 0;
    for (int q = 0; q < PROBLEMS; q++) {
        bf_factor_graph *g = bf_fg_new(PER * VARS);
        for (int slot = 0; slot < PER; slot++) {
            int p = (int)(next() % PIECES);
            for (int c = 0; c < len[p]; c++) {
                int32_t lits[3];
                for (int i = 0; i < width[p][c]; i++) {
                    int32_t v = abs(piece[p][c][i]) + slot * VARS;
                    lits[i] = piece[p][c][i] > 0 ? v : -v;
                }
                bf_fg_add_clause(g, lits, (uint32_t)width[p][c]);
            }
        }
        bf_fg_count_t shared, fresh; bf_fg_stats st;
        clock_t t0 = clock();
        CHECK(bf_fg_count(g, cache, NULL, &shared, &st) == BF_FG_OK, "shared count");
        clock_t t1 = clock();
        CHECK(bf_fg_count(g, NULL, NULL, &fresh, NULL) == BF_FG_OK, "fresh count");
        clock_t t2 = clock();
        shared_s += (double)(t1 - t0) / CLOCKS_PER_SEC; fresh_s += (double)(t2 - t1) / CLOCKS_PER_SEC;
        if (shared != fresh) mismatches++;
        hits += st.cache_hits; misses += st.cache_misses;
        bf_fg_free(g);
    }
    CHECK(mismatches == 0, "%llu shared answers differed from fresh elimination", (unsigned long long)mismatches);
    CHECK(misses <= (uint64_t)PIECES * 4 && hits > misses * 100, "reuse: %llu hits, %llu misses", (unsigned long long)hits, (unsigned long long)misses);
    printf("shared structure: %d problems, %llu components eliminated once, %llu answered from cache; %.3fs shared vs %.3fs fresh\n",
           PROBLEMS, (unsigned long long)misses, (unsigned long long)hits, shared_s, fresh_s);
    bf_fg_cache_free(cache);
}

/* The CMS-policy example from the 2026-11-07 research thread: publish needs
 * an approved author, public render needs publication and a licence.
 * Variables x=publish y=approved l=licensed r=rendered. Each implication
 * a -> b is a factor weighted 1 when satisfied and z when violated, so
 * Z(z) is the violation generating polynomial H(z). Evaluating H at three
 * integer points and interpolating recovers its coefficients exactly. */
static bf_fg_count_t policy_at(uint64_t z) {
    bf_factor_graph *g = bf_fg_new(4);
    /* bit 0 = a, bit 1 = b; violated only when a=1, b=0 (index 1) */
    uint64_t implies[4] = {1, z, 1, 1};
    uint32_t xy[2] = {0, 1}, rx[2] = {3, 0}, rl[2] = {3, 2};
    bf_fg_add_factor(g, xy, 2, implies); bf_fg_add_factor(g, rx, 2, implies); bf_fg_add_factor(g, rl, 2, implies);
    bf_fg_count_t v = 0; bf_fg_count(g, NULL, NULL, &v, NULL); bf_fg_free(g);
    return v;
}
static void policy_violation_polynomial_is_recovered_exactly(void) {
    CHECK(policy_at(0) == 7, "7 valid policy configurations (got %llu)", (unsigned long long)policy_at(0));
    CHECK(policy_at(1) == 16, "all 16 assignments at z=1");
    /* H(z) = c0 + c1 z + c2 z^2 from H(0), H(1), H(2) by exact differences. */
    long long h0 = (long long)policy_at(0), h1 = (long long)policy_at(1), h2 = (long long)policy_at(2);
    long long c0 = h0, c2 = (h2 - 2 * h1 + h0) / 2, c1 = h1 - h0 - c2;
    CHECK(c0 == 7 && c1 == 6 && c2 == 3, "H(z) = 7 + 6z + 3z^2 (got %lld + %lldz + %lldz^2)", c0, c1, c2);
    CHECK(policy_at(3) == (bf_fg_count_t)(7 + 6 * 3 + 3 * 9), "H(3) agrees with the recovered polynomial");
}

static void width_and_overflow_are_refused(void) {
    /* A 26-cycle of pairwise factors has width 3; a clique over 26 does not. */
    bf_factor_graph *g = bf_fg_new(26); uint64_t ones[4] = {1, 1, 1, 1};
    for (uint32_t i = 0; i < 26; i++) for (uint32_t j = i + 1; j < 26; j++) { uint32_t s[2] = {i, j}; bf_fg_add_factor(g, s, 2, ones); }
    bf_fg_count_t z; bf_fg_stats st;
    CHECK(bf_fg_count(g, NULL, NULL, &z, &st) == BF_FG_EWIDTH, "clique wider than the bound is refused");
    bf_fg_limits tight = { 4 };
    bf_fg_free(g); g = bf_fg_new(26);
    for (uint32_t i = 0; i < 26; i++) { uint32_t s[2] = {i, (i + 1) % 26}; bf_fg_add_factor(g, s, 2, ones); }
    CHECK(bf_fg_count(g, NULL, &tight, &z, &st) == BF_FG_OK && z == ((bf_fg_count_t)1 << 26) && st.max_width <= 3, "cycle fits width 3");
    bf_fg_free(g);
    g = bf_fg_new(127);
    CHECK(bf_fg_count(g, NULL, NULL, &z, NULL) == BF_FG_OK && z == ((bf_fg_count_t)1 << 127), "2^127 is exact");
    bf_fg_free(g); g = bf_fg_new(128);
    CHECK(bf_fg_count(g, NULL, NULL, &z, NULL) == BF_FG_EOVERFLOW, "2^128 is refused, not wrapped");
    bf_fg_free(g);
    char s[40];
    CHECK(!strcmp(bf_fg_count_str((bf_fg_count_t)1 << 127, s), "170141183460469231731687303715884105728"), "decimal text");
}

int main(void) {
    random_cnf(); random_weighted(); clause_lowering(); auxiliary_definitions_preserve_counts();
    shared_structure_is_computed_once(); policy_violation_polynomial_is_recovered_exactly(); width_and_overflow_are_refused();
    printf("bf_factor: %s (%d checks)\n", failures ? "FAIL" : "PASS", checks);
    return failures ? 1 : 0;
}
