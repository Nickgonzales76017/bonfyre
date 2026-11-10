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
    {"frappe-core", "frappe", "platform", "DocType", "", "DocType", "Authority", "SurfaceGrammar|Policy", "frappe"}, \
    {"crm", "crm", "customer", "CRM Deal", "frappe-core", "", "Actor|Relation|OccurrenceProjection", "CommunicationEvent|DealState", "frappe"}, \
    {"helpdesk", "helpdesk", "support", "HD Ticket", "crm", "TicketState", "Actor|WorkState|OccurrenceProjection", "TicketState", "frappe"}, \
    {"erpnext", "erpnext", "commercial-fulfillment-accounting", "Sales Order", "helpdesk", "Value|Receipt", "WorkState|ProviderState", "Value|Receipt", "frappe"}, \
    {"hrms", "hrms", "people", "Attendance", "erpnext", "HumanCapacity", "Actor|WorkState", "HumanCapacity", "frappe"}, \
    {"lms", "lms", "capability", "LMS Course", "hrms", "Credential", "Actor", "Credential|Evidence", "frappe"}, \
    {"wiki", "wiki", "knowledge", "Wiki Document", "lms", "KnowledgeClaim", "Evidence|ProofFrontier", "KnowledgeClaim", "frappe"}, \
    {"insights", "insights", "analytics", "Insights Query", "wiki", "DerivedView", "ReachableCapacity|Value|OpportunityEligibility|ResourcePressure", "DerivedView", "frappe"}, \
    {"drive", "drive", "content", "Drive Permission", "insights", "", "Artifact|Authority|RouteAdvertisement|NamespaceProjection|QueryDirectory", "ShareGrant|RepresentationHistory", "frappe"} \
}
#define BF_APP_LINEAGE_N 9

#endif /* BF_APP_LINEAGES_H */
