#ifndef BONFYRE_SEMANTIC_OPTIMIZER_ESTATE_EVIDENCE_H
#define BONFYRE_SEMANTIC_OPTIMIZER_ESTATE_EVIDENCE_H
#include <stdio.h>
int bf_estate_evidence_sync(const char *control_db,const char *cms_db,const char *foreign_root,const char *generation,FILE *err);
int bf_estate_evidence_status(const char *control_db,const char *cms_db,FILE *err);
#endif
