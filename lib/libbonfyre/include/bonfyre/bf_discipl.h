/*
 * bf_discipl.h — Recursive DisCIPL substrate for Bonfyre.
 *
 * Metadata-first actor / contract / chain / loop runtime for
 * AI and non-AI systems.
 */
#ifndef BONFYRE_BF_DISCIPL_H
#define BONFYRE_BF_DISCIPL_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BF_DISCIPL_MAX_ITEMS 128
#define BF_DISCIPL_MAX_FOLLOWERS 64

typedef enum {
    BF_DISCIPL_ACTOR_LAYER = 0,
    BF_DISCIPL_ACTOR_COMMAND,
    BF_DISCIPL_ACTOR_PIPELINE_STAGE,
    BF_DISCIPL_ACTOR_BRIDGE,
    BF_DISCIPL_ACTOR_SERVICE,
    BF_DISCIPL_ACTOR_VALUE_ACTOR,
    BF_DISCIPL_ACTOR_GRAPH_ACTOR,
    BF_DISCIPL_ACTOR_QUEUE_ACTOR
} bf_discipl_actor_type_t;

typedef struct {
    int can_boss;
    int can_follow;
    int can_verify;
    int can_decompose;
    int can_route;
    int can_price;
    int can_execute;
} bf_discipl_role_affordances_t;

typedef struct {
    char id[256];
    bf_discipl_actor_type_t actor_type;
    char family[128];
    char domain[128];
    char modality[128];
    uint64_t capabilities_bitset;
    bf_discipl_role_affordances_t affordances;
    double confidence;
    double uncertainty;
    double cost;
    double latency;
    char source_artifact_id[256];
    char source_command[128];
} bf_discipl_actor_t;

typedef struct {
    char name[128];
    char src_family[128];
    char dst_family[128];
    char relationship[128];
    char directionality[64];
    char preconditions_json[1024];
    char postconditions_json[1024];
    char failure_modes_json[1024];
    char required_bridge[128];
    double success_prior;
    double transform_cost;
    double semantic_loss;
    int retryable;
    int decomposable;
    char verification_family[128];
} bf_discipl_contract_t;

typedef struct {
    char chain_id[256];
    char goal[256];
    char artifact_ids[BF_DISCIPL_MAX_ITEMS][256];
    char families[BF_DISCIPL_MAX_ITEMS][128];
    char bridge_requirements[BF_DISCIPL_MAX_ITEMS][128];
    char per_hop_status[BF_DISCIPL_MAX_ITEMS][64];
    double per_hop_confidence[BF_DISCIPL_MAX_ITEMS];
    int edge_count;
    double global_confidence;
    double accumulated_cost;
    double semantic_drift;
    char materialization_policy[64];
    char status[64];
} bf_discipl_chain_program_t;

typedef struct {
    char loop_id[256];
    char parent_loop_id[256];
    char goal[256];
    char boss_actor_id[256];
    char follower_actor_ids[BF_DISCIPL_MAX_FOLLOWERS][256];
    char candidate_chain_ids[BF_DISCIPL_MAX_FOLLOWERS][256];
    char active_chain_id[256];
    int follower_count;
    int candidate_chain_count;
    int recursion_depth;
    double convergence_score;
    double uncertainty;
    double cost_budget;
    char status[64];
    char created_at[32];
    char updated_at[32];
} bf_discipl_loop_t;

typedef double (*bf_discipl_llamppl_score_hook_t)(const bf_discipl_chain_program_t *chain,
                                                  const bf_discipl_contract_t *contracts,
                                                  int contract_count);

int bf_discipl_actor_from_layer_json(const char *layer_json, bf_discipl_actor_t *out_actor);
int bf_discipl_contract_from_family_relation(const char *src_family,
                                             const char *dst_family,
                                             bf_discipl_contract_t *out_contract);
int bf_discipl_chain_from_stitch_plan_json(const char *plan_json, bf_discipl_chain_program_t *out_chain);
double bf_discipl_chain_score(bf_discipl_chain_program_t *chain,
                              const bf_discipl_contract_t *contracts,
                              int contract_count);
double bf_discipl_llamppl_score_hook(const bf_discipl_chain_program_t *chain,
                                     const bf_discipl_contract_t *contracts,
                                     int contract_count);
int bf_discipl_loop_init(const char *goal, const bf_discipl_chain_program_t *seed_chain, bf_discipl_loop_t *out_loop);
int bf_discipl_loop_spawn_subloop(const bf_discipl_loop_t *parent,
                                  const char *goal,
                                  const bf_discipl_chain_program_t *seed_chain,
                                  bf_discipl_loop_t *out_loop);
int bf_discipl_loop_assign_roles(bf_discipl_loop_t *loop,
                                 const bf_discipl_actor_t *actors,
                                 int actor_count);
int bf_discipl_loop_verify(const bf_discipl_loop_t *loop,
                           const bf_discipl_chain_program_t *chains,
                           int chain_count,
                           char *out_json,
                           size_t out_json_sz);
int bf_discipl_loop_to_json(const bf_discipl_loop_t *loop, char **out_json);
int bf_discipl_chain_to_json(const bf_discipl_chain_program_t *chain, char **out_json);

int bf_discipl_init_db(const char *root);
int bf_discipl_tables_exist(const char *root);
int bf_discipl_contracts_json(const char *family_filter, char **out_json);
int bf_discipl_upsert_actor(const char *root, const bf_discipl_actor_t *actor);
int bf_discipl_upsert_contract(const char *root, const bf_discipl_contract_t *contract);
int bf_discipl_upsert_chain(const char *root, const bf_discipl_chain_program_t *chain);
int bf_discipl_upsert_loop(const char *root, const bf_discipl_loop_t *loop);
int bf_discipl_load_chain(const char *root, const char *chain_id, bf_discipl_chain_program_t *out_chain);
int bf_discipl_load_loop(const char *root, const char *loop_id, bf_discipl_loop_t *out_loop);

#ifdef __cplusplus
}
#endif

#endif
