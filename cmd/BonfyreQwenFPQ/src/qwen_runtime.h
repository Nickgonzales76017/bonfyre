/*
 * qwen_runtime.h — Qwen inference runtime core
 *
 * Manages model state, generation loop, and quality-safe output.
 */
#pragma once

#include "libfpq.h"
#include "tokenizer.h"
#include "fpq_active_cache.h"
#include "fpq_run.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Runtime configuration */
typedef struct {
    int n_vocab;
    int d_model;
    int d_ffn;
    int n_layers;
    int n_heads;
    int n_kv_heads;
    int head_dim;
    int max_seq_len;
    float rms_norm_eps;
    float rope_theta;
    int max_new_tokens;
    float temperature;
    float top_p;
    int greedy;
    int prefill_chunk_size;
    const char *mode;  /* "code", "blender", "json", "video" */
    fpq_run_arch_t arch;
    char model_family[32];
    char embed_tensor_name[128];
    char final_norm_tensor_name[128];
    char lm_head_tensor_name[128];
    int tie_word_embeddings;
    int tokenizer_policy;
    int stop_token_ids[16];
    int n_stop_token_ids;
} qwen_config_t;

/* KV cache state */
typedef struct {
    float **k_cache;  /* [n_layers][max_seq * n_kv_heads * head_dim] */
    float **v_cache;  /* [n_layers][max_seq * n_kv_heads * head_dim] */
    int cached_tokens;
    int max_seq_len;
    int n_layers;
    int n_kv_heads;
    int head_dim;
    uint64_t prefix_hash;
} qwen_kv_cache_t;

typedef enum {
    QWEN_BACKEND_CPU_SCALAR = 0,
    QWEN_BACKEND_CPU_NEON = 1,
    QWEN_BACKEND_CPU_NEON_FUSED = 2,
    QWEN_BACKEND_FLASHQLA_PREFILL = 3
} qwen_backend_t;

/* Runtime state */
typedef struct {
    fpq_model_t *model;
    tokenizer_t *tokenizer;
    qwen_config_t config;
    qwen_kv_cache_t *kv_cache;
    fpq_run_state_t *run_state;
    fpq_active_cache_t *active_cache;
    float *embeddings;    /* loader-owned embedding handle/sentinel */
    float *norm_layers;   /* [n_layers * 2 * d_model] */
    float *final_norm;    /* [d_model] */
    char *pack_path;
    char *tokenizer_path;
    char *model_id;
    char *tokenizer_id;
    qwen_backend_t backend;
    int model_validated;
    double model_open_seconds;
    double tokenizer_load_seconds;
    double tensor_prepare_seconds;
    double runtime_init_seconds;
    uint64_t resident_cache_hits;
    uint64_t resident_cache_misses;
    uint64_t resident_hot_tensors_retained;
    uint64_t resident_tensors_reprepared;
    uint64_t resident_reused_prompt_tokens;
    uint64_t resident_reused_prefix_kv;
    uint64_t resident_reused_rope;
    uint64_t resident_reused_mlp_prepare;
    uint64_t resident_reused_lm_head_prepare;
    char *last_prompt;
    int *last_prompt_ids;
    int last_prompt_len;
} qwen_runtime_t;

/* Generation callback */
typedef void (*qwen_token_cb)(int token_id, const char *text, void *user_data);

/* Stop condition callback */
typedef int (*qwen_stop_cb)(const int *tokens, int n_tokens, void *user_data);

/* Initialize runtime */
qwen_runtime_t *qwen_runtime_init(const char *pack_path,
                                   const char *tokenizer_path,
                                   const qwen_config_t *config);

/* Free runtime */
void qwen_runtime_free(qwen_runtime_t *rt);

/* Warm up (load model, populate active cache) */
int qwen_runtime_warm(qwen_runtime_t *rt);

/* Generate from prompt */
int qwen_runtime_generate(qwen_runtime_t *rt,
                          const char *prompt,
                          qwen_token_cb token_cb,
                          qwen_stop_cb stop_cb,
                          void *user_data);

/* Generate from prompt file */
int qwen_runtime_generate_file(qwen_runtime_t *rt,
                               const char *prompt_path,
                               const char *out_path,
                               qwen_token_cb token_cb,
                               void *user_data);

/* Build a JSON status object for service/status surfaces. Caller frees. */
char *qwen_runtime_status_json(const qwen_runtime_t *rt);

/* Get default config for Qwen2.5-Coder-14B-Instruct */
qwen_config_t qwen_default_config(void);

/* KV cache management */
qwen_kv_cache_t *qwen_kv_cache_create(const qwen_config_t *config);
void qwen_kv_cache_free(qwen_kv_cache_t *kv);
void qwen_kv_cache_reset(qwen_kv_cache_t *kv);

#ifdef __cplusplus
}
#endif
