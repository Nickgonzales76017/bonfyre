#ifndef BF_PATTERN_H
#define BF_PATTERN_H

/*
 * PatternGraph: repeated observations kept apart from candidate primitives.
 * A cluster (statement, member traces, evidence) may be observed or
 * challenged; it may propose a candidate primitive, and that is all.  Proof
 * and promotion are separate effects this owner never performs: frequency is
 * neither correctness nor authority.  Ported from
 * components/BonfyreControlPlane/pattern_graph.py.archive (same tables).
 */
#include <stddef.h>
#include <stdint.h>

typedef enum {
    BF_PATTERN_OK = 0,
    BF_PATTERN_INVALID,        /* missing id, statement, traces or evidence */
    BF_PATTERN_STATUS,         /* a cluster may only be observed or challenged */
    BF_PATTERN_UNKNOWN_CLUSTER,
    BF_PATTERN_STORAGE
} BfPatternStatus;

typedef int (*BfPatternVisit)(const char *value, void *ctx);

/* sqlite3 handle; creates the tables when absent. */
int bf_pattern_open(void *sqlite_database);

BfPatternStatus bf_pattern_record_cluster(void *db, const char *cluster_id, const char *statement,
                                          const char *const *traces, size_t trace_count,
                                          const char *const *evidence, size_t evidence_count,
                                          const char *status);
/* Propose only: the candidate is recorded as "candidate" and never higher. */
BfPatternStatus bf_pattern_propose(void *db, const char *candidate_id, const char *cluster_id, const char *statement);
/* 1 and the status when known, 0 when not, -1 on error. */
int bf_pattern_candidate_status(void *db, const char *candidate_id, char *out, size_t cap);
int64_t bf_pattern_cluster_traces(void *db, const char *cluster_id, BfPatternVisit visit, void *ctx);

#endif
