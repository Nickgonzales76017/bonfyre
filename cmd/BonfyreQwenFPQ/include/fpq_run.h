/*
 * fpq_run.h — LLaMA transformer inference declarations for Bonfyre Ember
 */
#ifndef BONFYRE_FPQ_RUN_H
#define BONFYRE_FPQ_RUN_H

#include "libfpq.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    FPQ_RUN_ARCH_LLAMA   = 0,
    FPQ_RUN_ARCH_MISTRAL = 1,
    FPQ_RUN_ARCH_QWEN2   = 2,
} fpq_run_arch_t;

typedef enum {
    FPQ_RUN_TOKENIZER_POLICY_GENERIC = 0,
    FPQ_RUN_TOKENIZER_POLICY_CHATML = 1,
} fpq_run_tokenizer_policy_t;

#define FPQ_RUN_MAX_PREFILL_CHUNKS 512
#define FPQ_RUN_MAX_LAYERS 128
#define FPQ_RUN_MAX_FAMILIES 16

typedef struct {
    int            n_vocab;
    int            d_model;
    int            d_ffn;
    int            n_layers;
    int            n_heads;
    int            n_kv_heads;
    int            head_dim;
    float          rms_norm_eps;
    float          rope_theta;
    int            max_seq_len;
    fpq_run_arch_t arch;
    /* Generation params */
    int            max_new_tokens;
    float          temperature;
    float          top_p;
    int            greedy;
    int            prefill_chunk_size;
    /* Optional cap for sampling space; 0 means use n_vocab. */
    int            sample_n_vocab;
    /* Optional debug decode for logit tracing. */
    const void    *debug_decode_ctx;
    const char * (*debug_id_to_str)(const void *ctx, int token_id);
    char           model_family[32];
    char           embed_tensor_name[128];
    char           final_norm_tensor_name[128];
    char           lm_head_tensor_name[128];
    int            tie_word_embeddings;
    int            tokenizer_policy;
    int            stop_token_ids[16];
    int            n_stop_token_ids;
} fpq_run_config_t;

typedef struct fpq_run_state fpq_run_state_t;

typedef struct {
    int prompt_tokens;
    int generated_tokens;
    int total_tokens;
    int prefill_chunk_size;
    int prefill_chunk_count;
    int active_kv_window;
    double prefill_wall_seconds;
    double decode_wall_seconds;
    double prefill_layer_total_seconds[FPQ_RUN_MAX_LAYERS];
    double decode_layer_total_seconds[FPQ_RUN_MAX_LAYERS];
    double prefill_layer_prepare_seconds[FPQ_RUN_MAX_LAYERS];
    double decode_layer_prepare_seconds[FPQ_RUN_MAX_LAYERS];
    double decode_layer_qkv_seconds[FPQ_RUN_MAX_LAYERS];
    double decode_layer_attention_seconds[FPQ_RUN_MAX_LAYERS];
    double decode_layer_o_proj_seconds[FPQ_RUN_MAX_LAYERS];
    double decode_layer_norm_seconds[FPQ_RUN_MAX_LAYERS];
    double decode_layer_mlp_seconds[FPQ_RUN_MAX_LAYERS];
    double decode_layer_kv_write_seconds[FPQ_RUN_MAX_LAYERS];
    double decode_layer_release_seconds[FPQ_RUN_MAX_LAYERS];
    double family_seconds[FPQ_RUN_MAX_FAMILIES];
    uint64_t family_calls[FPQ_RUN_MAX_FAMILIES];
    double lm_head_seconds;
    double kv_write_seconds;
    double tensor_prepare_seconds;
    double release_seconds;
    double prefill_chunk_seconds[FPQ_RUN_MAX_PREFILL_CHUNKS];
    uint64_t prefill_chunk_cache_hits[FPQ_RUN_MAX_PREFILL_CHUNKS];
    int prefill_chunk_tokens[FPQ_RUN_MAX_PREFILL_CHUNKS];
} fpq_run_metrics_t;

/* Callback invoked per generated token. Return non-zero to stop early. */
typedef void (*fpq_run_token_cb)(int token_id, void *data);

/* Default config (TinyLlama-1.1B) */
fpq_run_config_t fpq_run_default_config(void);

/* Persistent session state for repeated generation calls. */
fpq_run_state_t *fpq_run_state_create(const fpq_run_config_t *cfg,
                                      float **k_cache,
                                      float **v_cache);
void fpq_run_state_free(fpq_run_state_t *state);
void fpq_run_state_reset(fpq_run_state_t *state);
const fpq_run_metrics_t *fpq_run_state_metrics(const fpq_run_state_t *state);
const char *fpq_run_metrics_family_name(int family);

/* Main generation loop. Returns number of tokens generated, -1 on error. */
int fpq_run_generate(
    fpq_model_t       *model,
    const float       *embed_table,
    const float       *norm_layers,
    const float       *final_norm,
    const int         *prompt_ids,
    int                prompt_len,
    const fpq_run_config_t *cfg,
    fpq_run_state_t   *state,
    fpq_run_token_cb   callback,
    void              *cb_data);

/* Load norm weights from model (allocates, caller must free) */
float *fpq_run_load_norms(fpq_model_t *model, const fpq_run_config_t *cfg);
float *fpq_run_load_final_norm(fpq_model_t *model, const fpq_run_config_t *cfg);
float *fpq_run_load_embeddings(fpq_model_t *model, const fpq_run_config_t *cfg);

/* Top-level CLI command: fpq run <model.fpq> "prompt" [options] */
int cmd_run(int argc, char **argv);

#ifdef __cplusplus
}
#endif
#endif /* BONFYRE_FPQ_RUN_H */
