/* bf_factor: exact sum-of-products over Boolean factor graphs.
 *
 *   Z = sum over x in {0,1}^n of prod_f phi_f(x restricted to scope(f))
 *
 * With 0/1 tables (clauses, exclusions, dependencies) Z is the exact number
 * of satisfying assignments; with integer weights it is an exact weighted
 * count. Evaluation is variable elimination per connected component with a
 * greedy min-degree order and an explicit width bound: a problem whose
 * elimination would exceed the bound is refused, never approximated.
 *
 * Shared structure is computed once. Each component is reduced to a
 * structural digest (variables renamed by first appearance, factors in
 * insertion order, tables included) and its count is kept in a cache that
 * outlives one problem, so a family of problems built from the same typed
 * pieces pays for each distinct piece once. The digest identifies a
 * component as built; two isomorphic components built in different orders
 * get different digests and are both computed, which costs time but never
 * correctness.
 *
 * Counts are exact unsigned 128-bit integers; any overflow is refused. */
#ifndef BF_FACTOR_H
#define BF_FACTOR_H

#include <stddef.h>
#include <stdint.h>

#define BF_FG_MAX_ARITY 20     /* variables in one factor's scope */
#define BF_FG_MAX_WIDTH 24     /* hard ceiling on an intermediate factor */

enum {
    BF_FG_OK = 0,
    BF_FG_EINVAL = -1,         /* bad scope, variable, arity or literal */
    BF_FG_ENOMEM = -2,
    BF_FG_EWIDTH = -3,         /* elimination would exceed the width bound */
    BF_FG_EOVERFLOW = -4       /* the exact count does not fit 128 bits */
};

typedef unsigned __int128 bf_fg_count_t;

typedef struct bf_factor_graph bf_factor_graph;
typedef struct bf_fg_cache bf_fg_cache;

typedef struct {
    uint32_t max_width;        /* 0 means BF_FG_MAX_WIDTH */
} bf_fg_limits;

typedef struct {
    uint64_t components;       /* connected components with at least one factor */
    uint64_t free_variables;   /* variables in no factor (each doubles Z) */
    uint64_t cache_hits;       /* components answered from the shared cache */
    uint64_t cache_misses;     /* components eliminated here */
    uint64_t eliminations;     /* variables summed out by this call */
    uint32_t max_width;        /* widest intermediate factor built */
} bf_fg_stats;

/* A graph over variables 0..nvars-1. */
bf_factor_graph *bf_fg_new(uint32_t nvars);
void bf_fg_free(bf_factor_graph *g);
uint32_t bf_fg_nvars(const bf_factor_graph *g);

/* table has 2^k entries; bit i of an entry's index is the value of scope[i].
 * Scope variables must be distinct and < nvars. The table is copied. */
int bf_fg_add_factor(bf_factor_graph *g, const uint32_t *scope, uint32_t k,
                     const uint64_t *table);

/* A disjunction of literals: +v means variable v-1 is true, -v means false
 * (1-based, as in DIMACS). Lowered to a 0/1 factor over its distinct
 * variables; a clause holding v and -v is always true and adds nothing. An
 * empty clause makes Z zero. */
int bf_fg_add_clause(bf_factor_graph *g, const int32_t *lits, uint32_t n);

/* A cache shared across problems; NULL disables reuse. */
bf_fg_cache *bf_fg_cache_new(void);
void bf_fg_cache_free(bf_fg_cache *c);
size_t bf_fg_cache_size(const bf_fg_cache *c);

/* Exact Z. limits and stats may be NULL. The graph is not modified. */
int bf_fg_count(const bf_factor_graph *g, bf_fg_cache *cache,
                const bf_fg_limits *limits, bf_fg_count_t *out,
                bf_fg_stats *stats);

/* Decimal text of a count; out needs at least 40 bytes. Returns out. */
char *bf_fg_count_str(bf_fg_count_t v, char out[40]);

#endif
