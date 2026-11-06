#ifndef BONFYRE_OWNER_CONTRACT_CATALOG_H
#define BONFYRE_OWNER_CONTRACT_CATALOG_H

int catalog_audit(int argc, char **argv);

/* A compact physical-realization summary used by OwnerContractImage.  The
 * image references canonical capability rows; it does not copy a capability
 * definition or turn a realizable command into an admitted invocation. */
typedef struct {
 int capability_rows;
 int atlas_architectures;
 int canonical_realizations;
 int operation_aliases;
 int source_linked_realizations;
 int multi_owner_realizations;
 int archives;
 int declared_unrealized;
 int external_realizations;
 int realized_unmapped;
} BfOwnerCatalogSummary;

int catalog_summary(const char *atlas, const char *capbin, BfOwnerCatalogSummary *out);

#endif
