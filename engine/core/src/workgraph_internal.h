#ifndef BF_WORKGRAPH_INTERNAL_H
#define BF_WORKGRAPH_INTERNAL_H

#include "bf_workgraph.h"
#include "bonfyre.h"

#include <sqlite3.h>

struct BfWorkgraph {
    sqlite3 *db;
    int owns_database;
    FILE *err;
};

int64_t bf_workgraph_now_ms(void);
void bf_workgraph_timestamp(char output[32]);
BfWorkgraphResult bf_workgraph_result(BfWorkgraphStatus status, const char *mission_id,
                                      const char *node_id, const char *error_code,
                                      const char *error_message);
int bf_workgraph_begin(BfWorkgraph *graph, BfWorkgraphResult *result);
int bf_workgraph_commit(BfWorkgraph *graph, BfWorkgraphResult *result);
void bf_workgraph_rollback(BfWorkgraph *graph);
int bf_workgraph_exec(BfWorkgraph *graph, const char *sql, BfWorkgraphResult *result);
int bf_workgraph_generate_token(char token[65], char digest[65]);
void bf_workgraph_hash_token(const char *token, char digest[65]);
typedef enum BfWorkgraphTransitionDomain {
    BF_WORKGRAPH_TRANSITION_NODE = 0,
    BF_WORKGRAPH_TRANSITION_EFFECT,
    BF_WORKGRAPH_TRANSITION_COMPENSATION
} BfWorkgraphTransitionDomain;

/*
 * Explicit evidence for a single transition. For BF_WORKGRAPH_TRANSITION_COMPENSATION,
 * compensation_attempt is authoritative and must not be overwritten by the node's
 * ordinary execution attempt; from_state/to_state are recorded verbatim rather than
 * derived from workgraph_nodes, since compensation state is tracked independently of
 * node execution status.
 */
typedef struct BfWorkgraphEvidence {
    BfWorkgraphTransitionDomain transition_domain;
    int execution_attempt;    /* -1 to derive from workgraph_nodes */
    int compensation_attempt; /* -1 when not applicable */
    const char *from_state;   /* NULL to derive from prior node transition history */
    const char *to_state;
} BfWorkgraphEvidence;

int bf_workgraph_write_evidence(BfWorkgraph *graph, BfWorkgraphResult *result,
                                const char *transition, const char *actor,
                                const char *error_code);
int bf_workgraph_write_evidence_ex(BfWorkgraph *graph, BfWorkgraphResult *result,
                                   const BfWorkgraphEvidence *evidence,
                                   const char *actor, const char *error_code);
int bf_workgraph_promote_dependents(BfWorkgraph *graph, const char *mission_id,
                                    BfWorkgraphResult *result);
int bf_workgraph_update_mission(BfWorkgraph *graph, const char *mission_id,
                                BfWorkgraphResult *result);
int bf_workgraph_load_result(BfWorkgraph *graph, BfWorkgraphResult *result);
int bf_workgraph_validate_claim(BfWorkgraph *graph, BfWorkgraphResult *result,
                                const char *worker_id, const char *claim_token,
                                int allow_cancel_requested);
int bf_workgraph_backoff_ms(const char *mission_id, const char *node_id, int attempt,
                            int64_t base_ms, double multiplier, int64_t maximum_ms,
                            int jitter_percent, int64_t *backoff_ms);

#endif
