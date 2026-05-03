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
} fpq_run_config_t;

/* Callback invoked per generated token. Return non-zero to stop early. */
typedef void (*fpq_run_token_cb)(int token_id, void *data);

/* Default config (TinyLlama-1.1B) */
fpq_run_config_t fpq_run_default_config(void);

/* Main generation loop. Returns number of tokens generated, -1 on error. */
int fpq_run_generate(
    fpq_model_t       *model,
    const float       *embed_table,
    const float       *norm_layers,
    const float       *final_norm,
    const int         *prompt_ids,
    int                prompt_len,
    const fpq_run_config_t *cfg,
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
