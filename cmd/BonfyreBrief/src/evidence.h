/* Transformation evidence (DatabaseBridge receipts) cited by a migration brief. */
#ifndef BRIEF_EVIDENCE_H
#define BRIEF_EVIDENCE_H
#include <stdio.h>
#include <bf_jsonw.h>

typedef void (*brief_exc_fn)(const char *kind, const char *ref, const char *detail);

/* Returns the number loaded, or -1 when an input must be refused. */
int brief_evidence_load(const char *const *paths, int n, const char *receipts, brief_exc_fn exc);
const char *brief_evidence_digest(void);
int brief_evidence_has(const char *kind);
void brief_evidence_refs(bf_jsonw *w, const char *kind);
void brief_evidence_json(bf_jsonw *w);
void brief_evidence_md(FILE *o);
void brief_evidence_receipt(bf_jsonw *w);
void brief_evidence_free(void);
#endif
