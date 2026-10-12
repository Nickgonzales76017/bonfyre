#ifndef BF_WORKGRAPH_H
#define BF_WORKGRAPH_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>

typedef struct BfWorkgraph BfWorkgraph;

typedef enum BfWorkgraphStatus {
    BF_WORKGRAPH_OK = 0,
    BF_WORKGRAPH_NOT_FOUND,
    BF_WORKGRAPH_NOT_ELIGIBLE,
    BF_WORKGRAPH_STALE_CLAIM,
    BF_WORKGRAPH_CONFLICT,
    BF_WORKGRAPH_INVALID,
    BF_WORKGRAPH_STORAGE_ERROR
} BfWorkgraphStatus;

typedef struct BfWorkgraphResult {
    BfWorkgraphStatus status;
    char mission_id[128];
    char node_id[128];
    char effect_id[128];
    int attempt;
    char worker_id[128];
    char claim_token[65];
    int64_t lease_expires_at_ms;
    int64_t next_attempt_at_ms;
    char event_id[64];
    char receipt_id[64];
    char error_code[64];
    char error_message[256];
    char node_status[32];
} BfWorkgraphResult;

typedef struct BfWorkgraphNodeSpec {
    const char *mission_id;
    const char *node_id;
    const char *operator_id;
    const char *input_uri;
    const char *family;
    const char *parent_node;
    const char *fanout_group;
    const char *compensation_operator;
    int priority;
    int retry_limit;
    int timeout_seconds;
    int64_t backoff_base_ms;
    double backoff_multiplier;
    int64_t backoff_max_ms;
    int jitter_percent;
    int fanin_required;
} BfWorkgraphNodeSpec;

typedef struct BfWorkgraphClaimSpec {
    const char *worker_id;
    const char *family;
    int64_t lease_ms;
} BfWorkgraphClaimSpec;

typedef struct BfWorkgraphFailure {
    const char *failure_class;
    const char *message;
} BfWorkgraphFailure;

typedef struct BfWorkgraphFanoutSpec {
    const char *mission_id;
    const char *parent_node_id;
    const char *group_id;
    const char *operator_id;
    const char *family;
    const char *child_prefix;
    int child_count;
    const char *failure_policy;
} BfWorkgraphFanoutSpec;

typedef struct BfWorkgraphEffectSpec {
    const char *effect_id;
    const char *adapter_id;
    const char *target_uri;
    const char *input_artifact_uri;
    const char *verification_policy;
    const char *rollback_contract;
    const char *authority_identity;
} BfWorkgraphEffectSpec;

typedef struct BfWorkgraphEffectRecord {
    char effect_id[128];
    char adapter_id[64];
    char target_uri[1024];
    char input_artifact_uri[1024];
    char prepared_state[1024];
    char verification_policy[128];
    char rollback_contract[256];
    char authority_identity[128];
    char simulation[1024];
    char recovery_action[32];
    char state[32];
    int attempt;
    char receipt_id[64];
    char commit_nonce[65];
    int64_t commit_started_at_ms;
    int64_t verified_at_ms;
    char expected_postcondition[256];
} BfWorkgraphEffectRecord;

typedef struct BfWorkgraphTransition {
    int64_t sequence;
    int attempt;
    char from_status[32];
    char to_status[32];
    char actor[128];
    char event_id[64];
    char receipt_id[64];
    int64_t created_at_ms;
} BfWorkgraphTransition;

int bf_workgraph_open(const char *database_path, BfWorkgraph **out, FILE *err);
int bf_workgraph_open_database(void *sqlite_database, BfWorkgraph **out, FILE *err);
void bf_workgraph_close(BfWorkgraph *graph);
int bf_workgraph_migrate_database(void *sqlite_database, FILE *err);
BfWorkgraphResult bf_workgraph_create_mission(BfWorkgraph *graph, const char *mission_id);

/* Register a routable node family. Once any family is registered, bf_workgraph_add_node
 * rejects a node whose family is neither "default" nor registered -- closing the gap
 * where the native WorkGraph accepted an unroutable target (e.g. "coordinator") that
 * the Python reference rejects. With no families registered, behavior is unchanged. */
BfWorkgraphResult bf_workgraph_register_family(BfWorkgraph *graph, const char *family);

BfWorkgraphResult bf_workgraph_add_node(BfWorkgraph *graph, const BfWorkgraphNodeSpec *spec);
BfWorkgraphResult bf_workgraph_add_dependency(BfWorkgraph *graph, const char *mission_id,
                                               const char *node_id, const char *depends_on_node_id,
                                               const char *dependency_policy);
BfWorkgraphResult bf_workgraph_claim_next(BfWorkgraph *graph, const BfWorkgraphClaimSpec *spec);
BfWorkgraphResult bf_workgraph_claim_node(BfWorkgraph *graph, const char *mission_id,
                                          const char *node_id, const BfWorkgraphClaimSpec *spec);
BfWorkgraphResult bf_workgraph_renew(BfWorkgraph *graph, const char *mission_id,
                                     const char *node_id, const char *worker_id,
                                     const char *claim_token, int64_t lease_ms);
BfWorkgraphResult bf_workgraph_complete(BfWorkgraph *graph, const char *mission_id,
                                        const char *node_id, const char *worker_id,
                                        const char *claim_token, const char *output_uri);
BfWorkgraphResult bf_workgraph_fail(BfWorkgraph *graph, const char *mission_id,
                                    const char *node_id, const char *worker_id,
                                    const char *claim_token, const BfWorkgraphFailure *failure);
BfWorkgraphResult bf_workgraph_cancel_node(BfWorkgraph *graph, const char *mission_id,
                                           const char *node_id, const char *worker_id,
                                           const char *claim_token);
BfWorkgraphResult bf_workgraph_cancel_mission(BfWorkgraph *graph, const char *mission_id);
BfWorkgraphResult bf_workgraph_reap_expired(BfWorkgraph *graph, const char *mission_id);
BfWorkgraphResult bf_workgraph_create_fanout(BfWorkgraph *graph,
                                             const BfWorkgraphFanoutSpec *spec);
BfWorkgraphResult bf_workgraph_evaluate_fanin(BfWorkgraph *graph, const char *mission_id,
                                              const char *node_id);
BfWorkgraphResult bf_workgraph_prepare_effect(BfWorkgraph *graph, const char *mission_id,
                                              const char *node_id, const char *worker_id,
                                              const char *claim_token,
                                              const BfWorkgraphEffectSpec *spec);
BfWorkgraphResult bf_workgraph_commit_effect(BfWorkgraph *graph, const char *mission_id,
                                             const char *node_id, const char *worker_id,
                                             const char *claim_token, const char *effect_id);
BfWorkgraphResult bf_workgraph_compensate(BfWorkgraph *graph, const char *mission_id,
                                          const char *node_id, const char *worker_id,
                                          const char *claim_token, const char *effect_id,
                                          int succeeded);
BfWorkgraphResult bf_workgraph_claim_compensation(BfWorkgraph *graph,
                                                  const char *mission_id,
                                                  const BfWorkgraphClaimSpec *spec);
BfWorkgraphResult bf_workgraph_effect_status(BfWorkgraph *graph, const char *mission_id,
                                             const char *node_id, const char *effect_id,
                                             BfWorkgraphEffectRecord *record);
/*
 * Scans effects left in 'commit_started' (external mutation may or may not have
 * completed before a crash) and reconciles them: finalizes to 'committed' when the
 * postcondition verifies, safely rolls back when nothing external happened yet, or
 * marks 'reconciliation_required' to block further mutation when neither is provably
 * true. mission_id may be NULL to reconcile across all missions. Returns the number
 * of effects inspected, or -1 on storage failure.
 */
int bf_workgraph_reconcile_effects(BfWorkgraph *graph, const char *mission_id);
BfWorkgraphResult bf_workgraph_resume(BfWorkgraph *graph, const char *mission_id);
BfWorkgraphResult bf_workgraph_status(BfWorkgraph *graph, const char *mission_id,
                                      const char *node_id);
BfWorkgraphResult bf_workgraph_list_transitions(BfWorkgraph *graph,
                                                const char *mission_id,
                                                const char *node_id,
                                                BfWorkgraphTransition **transitions,
                                                size_t *transition_count);
void bf_workgraph_free_transitions(BfWorkgraphTransition *transitions);

const char *bf_workgraph_status_name(BfWorkgraphStatus status);

#endif
