/* Generated from architecture/apps/app-lineages.contract.yaff.
 *
 * Do not edit. Change the contract and re-run
 * cmd/BonfyreAtlas/bonfyre-atlas compile-contracts app-lineages.
 *
 * These were a C table inside the Frappe compiler, which made the set of
 * application grammars a fact only a binary could answer. `dialect` records
 * where each grammar was first extracted from; it is provenance, not a
 * dependency, and not the type of an application lineage.
 *
 * Cannot infer: a declared lineage -> a running application. None of these is
 * installed on the serving estate.
 */
#ifndef BF_APP_LINEAGES_H
#define BF_APP_LINEAGES_H

typedef struct BfAppLineage {
    const char *lineage;
    const char *app_dir;
    const char *flow_stage;
    const char *representative_doctype;
    const char *company_flow_after;
    const char *owns;
    const char *consumes;
    const char *publishes;
    const char *dialect;
} BfAppLineage;

#define BF_APP_LINEAGES { \
    {"alpha", "app", "platform", "Sales Order", "", "", "Input|State", "Output", "frappe"}, \
    {"second", "two", "customer", "Quoted Doc", "alpha", "", "", "Name\"Quoted", ""} \
}
#define BF_APP_LINEAGE_N 2

#endif /* BF_APP_LINEAGES_H */
