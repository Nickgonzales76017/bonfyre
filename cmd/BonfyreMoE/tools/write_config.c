/*
 * write_config — emits a correct model_config.json for a converted GGUF MoE
 * model using the real cbf_arch_save_config() function, so we don't have to
 * guess the JSON schema by hand.
 *
 * Needed because bonfyre-moe's built-in "qwen-moe-2.7b" arch template has
 * d_ffn=5632 (the model's *shared*-expert FFN size) baked in and applies it
 * uniformly to routed experts too, while Qwen1.5-MoE's routed experts are
 * actually d_ffn=1408. Passing --arch qwen-moe-2.7b makes cbf_forward size
 * its per-expert buffers off the wrong number and corrupt the heap. Writing
 * our own model_config.json with the real per-expert d_ffn lets
 * cbf_arch_auto_detect load correct numbers instead.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "colibri_bonfyre.h"

int main(int argc, char **argv) {
    if (argc < 2) { fprintf(stderr, "usage: %s <model_dir>\n", argv[0]); return 1; }
    const char *model_path = argv[1];

    cbf_model_shape_t shape = {0};
    shape.n_vocab = 151936;
    shape.d_model = 2048;
    shape.n_layers = 24;
    shape.n_heads = 16;
    shape.n_kv_heads = 16;
    shape.head_dim = 128;
    shape.d_ffn = 1408;              /* real routed-expert FFN size, not the 5632 shared-expert size */
    shape.n_experts_per_layer = 60;
    shape.n_experts_active = 4;
    shape.n_shared_experts = 1;
    shape.has_mtp_head = false;
    shape.expert_size_mb = 5;        /* ~4.25MB actual per-expert int4 file, rounded up */
    shape.dense_size_mb = 2400;      /* embeddings + output_proj + attention_dense */
    shape.kv_bytes_per_token = 2 * shape.n_kv_heads * shape.head_dim * 2 * shape.n_layers;
    shape.max_seq_len = 4096;
    shape.rope_theta = 1000000.0f;

    int rc = cbf_arch_save_config(model_path, "qwen-moe-2.7b-native", &shape);
    printf("cbf_arch_save_config -> %d\n", rc);
    return rc == 0 ? 0 : 1;
}
