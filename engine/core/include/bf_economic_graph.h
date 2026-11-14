#ifndef BF_ECONOMIC_GRAPH_H
#define BF_ECONOMIC_GRAPH_H

/*
 * The economic estate as causal relations, beside the ledgers:
 *   MoneyGraph              funds / pays / owes / grants / subsidizes / costs / earns
 *   RecursiveValueGraph     non-cash value (unlocks, prefills, strengthens_evidence,
 *                           ...) reachable transitively and never converted to cash
 *   InstitutionalCostGraph  projected mission cost per resource; a projection is
 *                           never a booked entry, and nothing here books one
 * Commitments are bf_commitment's.  Ported from
 * components/BonfyreControlPlane/economic_graphs.py.archive (same tables).
 */
#include <stdint.h>

typedef enum { BF_ECON_OK = 0, BF_ECON_INVALID, BF_ECON_UNKNOWN_KIND, BF_ECON_STORAGE } BfEconStatus;
typedef int (*BfEconVisit)(const char *kind, const char *dst, double amount, int has_amount, void *ctx);

int bf_econ_open(void *sqlite_database);
/* has_amount 0 records a relation without a figure (an unpriced relation is not zero). */
BfEconStatus bf_econ_record_money(void *db, const char *src, const char *relation, const char *dst, double amount, int has_amount);
int64_t bf_econ_money_out(void *db, const char *src, BfEconVisit visit, void *ctx);
BfEconStatus bf_econ_record_value(void *db, const char *src, const char *kind, const char *dst);
/* Transitive, each destination once; never yields an amount. */
int64_t bf_econ_value_reachable(void *db, const char *src, BfEconVisit visit, void *ctx);
BfEconStatus bf_econ_record_cost(void *db, const char *mission, const char *resource, double amount);
/* Sum of projected costs; -1 on error (written to *out). */
int bf_econ_mission_cost(void *db, const char *mission, double *out);
/* 1 booked, 0 projected or unknown, -1 error. Projections are recorded unbooked. */
int bf_econ_cost_booked(void *db, const char *mission, const char *resource);

#endif
