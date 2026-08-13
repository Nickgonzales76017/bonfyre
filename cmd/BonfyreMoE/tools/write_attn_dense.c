/*
 * write_attn_dense — regenerates attention_dense.bin from the GGUF with a
 * different candidate 4-vector-per-layer composition, for empirically
 * testing which composition cbf_forward actually expects (no source
 * available; the byte COUNT is disassembly-verified, but which 4 real
 * tensors fill the 4 slots is not).
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "ggml.h"
#include "gguf.h"

static float *dequant_tensor(struct ggml_tensor *t) {
    int64_t n = ggml_nelements(t);
    float *out = malloc((size_t)n * sizeof(float));
    if (t->type == GGML_TYPE_F32) { memcpy(out, t->data, (size_t)n * sizeof(float)); return out; }
    const struct ggml_type_traits *tt = ggml_get_type_traits(t->type);
    tt->to_float(t->data, out, n);
    return out;
}

int main(int argc, char **argv) {
    if (argc < 4) { fprintf(stderr, "usage: %s <gguf> <out_dir> <slot0,slot1,slot2,slot3>\n", argv[0]); return 1; }
    const char *gguf_path = argv[1];
    const char *out_dir = argv[2];
    char keys_buf[512];
    snprintf(keys_buf, sizeof(keys_buf), "%s", argv[3]);
    char *keys[4] = {0};
    char *tok = strtok(keys_buf, ",");
    for (int i = 0; i < 4 && tok; i++) { keys[i] = tok; tok = strtok(NULL, ","); }

    struct ggml_context *ctx;
    struct gguf_init_params params = { .no_alloc = false, .ctx = &ctx };
    struct gguf_context *g = gguf_init_from_file(gguf_path, params);
    if (!g) { fprintf(stderr, "load failed\n"); return 1; }

    uint32_t n_layers = 24, d_model = 2048;

    char path[1024];
    snprintf(path, sizeof(path), "%s/attention_dense.bin", out_dir);
    FILE *f = fopen(path, "wb");
    for (uint32_t l = 0; l < n_layers; l++) {
        for (int k = 0; k < 4; k++) {
            char name[256];
            snprintf(name, sizeof(name), "blk.%u.%s", l, keys[k]);
            struct ggml_tensor *t = ggml_get_tensor(ctx, name);
            if (!t) { fprintf(stderr, "missing tensor %s\n", name); return 1; }
            float *v = dequant_tensor(t);
            fwrite(v, sizeof(float), d_model, f);
            free(v);
        }
    }
    fclose(f);
    printf("wrote %s with slots [%s,%s,%s,%s]\n", path, keys[0], keys[1], keys[2], keys[3]);
    return 0;
}
