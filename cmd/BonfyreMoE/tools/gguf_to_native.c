/*
 * gguf_to_native — converts a GGUF MoE model (Qwen1.5-MoE / qwen2moe arch)
 * into the on-disk layout bonfyre-moe / libcolibri-bonfyre expects:
 *
 *   embeddings.bin                              raw float32[n_vocab * d_model]
 *   attention_dense.bin                         raw float32[n_layers * d_model * 4]
 *   output_proj.bin                             raw float32[n_vocab * d_model]
 *   experts/layer_%02u/expert_%03u.int4          groupwise int4 (see below)
 *
 * The dense-file and expert-file byte layouts were recovered by disassembling
 * cbf_engine.o / cbf_forward.o in lib/libcolibri-bonfyre (no source available)
 * — not guessed. See notes inline at each writer.
 *
 * GGUF parsing and dequantization use the real libggml (Homebrew ggml package)
 * so every quant format (Q2_K/Q3_K/Q6_K/IQ4_NL/...) goes through ggml's own
 * verified dequant kernels rather than a reimplementation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <errno.h>
#include <sys/stat.h>

#include "ggml.h"
#include "gguf.h"

static struct gguf_context *g_gguf;
static struct ggml_context *g_ctx;

static uint32_t kv_u32(const char *key, uint32_t dflt) {
    int64_t id = gguf_find_key(g_gguf, key);
    if (id < 0) return dflt;
    return gguf_get_val_u32(g_gguf, id);
}

static float kv_f32(const char *key, float dflt) {
    int64_t id = gguf_find_key(g_gguf, key);
    if (id < 0) return dflt;
    return gguf_get_val_f32(g_gguf, id);
}

/* Dequantize an entire tensor to a freshly malloc'd float32 buffer. */
static float *dequant_tensor(struct ggml_tensor *t) {
    int64_t n = ggml_nelements(t);
    float *out = malloc((size_t)n * sizeof(float));
    if (!out) { fprintf(stderr, "malloc failed for %s (%lld elems)\n", t->name, (long long)n); exit(1); }
    if (t->type == GGML_TYPE_F32) {
        memcpy(out, t->data, (size_t)n * sizeof(float));
        return out;
    }
    const struct ggml_type_traits *tt = ggml_get_type_traits(t->type);
    if (!tt || !tt->to_float) {
        fprintf(stderr, "no dequant available for tensor %s (type %s)\n", t->name, ggml_type_name(t->type));
        exit(1);
    }
    tt->to_float(t->data, out, n);
    return out;
}

static float *get_dequant(const char *name) {
    struct ggml_tensor *t = ggml_get_tensor(g_ctx, name);
    if (!t) { fprintf(stderr, "tensor not found: %s\n", name); exit(1); }
    return dequant_tensor(t);
}

static void write_all(FILE *f, const void *buf, size_t n) {
    if (fwrite(buf, 1, n, f) != n) { fprintf(stderr, "short write\n"); exit(1); }
}

static void mkdir_p(const char *path) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    mkdir(tmp, 0755);
}

/*
 * Pack n_elements floats into the verified expert int4 format:
 *   int64 n_elements
 *   int32 group_size
 *   float32 scale[ceil(n_elements/group_size)]   -- max-abs per group / 7
 *   uint8 packed[ceil(n_elements/2)]              -- low nibble=elem[2k], high nibble=elem[2k+1], signed -8..7
 */
static void write_int4_file(const char *path, const float *data, int64_t n_elements, int32_t group_size) {
    int64_t n_groups = (n_elements + group_size - 1) / group_size;
    int64_t n_packed = (n_elements + 1) / 2;

    float *scale = malloc((size_t)n_groups * sizeof(float));
    uint8_t *packed = calloc((size_t)n_packed, 1);
    if (!scale || !packed) { fprintf(stderr, "oom writing %s\n", path); exit(1); }

    for (int64_t g = 0; g < n_groups; g++) {
        int64_t start = g * group_size;
        int64_t end = start + group_size;
        if (end > n_elements) end = n_elements;
        float maxabs = 1e-8f;
        for (int64_t i = start; i < end; i++) {
            float a = fabsf(data[i]);
            if (a > maxabs) maxabs = a;
        }
        scale[g] = maxabs / 7.0f;
    }

    for (int64_t i = 0; i < n_elements; i++) {
        int64_t g = i / group_size;
        float s = scale[g];
        int32_t q = s > 0 ? (int32_t)lroundf(data[i] / s) : 0;
        if (q > 7) q = 7;
        if (q < -8) q = -8;
        uint8_t nib = (uint8_t)(q & 0xF);
        int64_t byte_idx = i / 2;
        if ((i & 1) == 0) {
            packed[byte_idx] = (packed[byte_idx] & 0xF0) | nib;
        } else {
            packed[byte_idx] = (packed[byte_idx] & 0x0F) | (nib << 4);
        }
    }

    FILE *f = fopen(path, "wb");
    if (!f) { fprintf(stderr, "cannot open %s: %s\n", path, strerror(errno)); exit(1); }
    write_all(f, &n_elements, sizeof(int64_t));
    write_all(f, &group_size, sizeof(int32_t));
    write_all(f, scale, (size_t)n_groups * sizeof(float));
    write_all(f, packed, (size_t)n_packed);
    fclose(f);

    free(scale);
    free(packed);
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s <model.gguf> <output_model_dir> [group_size]\n", argv[0]);
        return 1;
    }
    const char *gguf_path = argv[1];
    const char *out_dir = argv[2];
    int32_t group_size = argc > 3 ? atoi(argv[3]) : 256;
    const char *order = argc > 4 ? argv[4] : "gud"; /* permutation of g(ate) u(p) d(own) */

    struct gguf_init_params params = { .no_alloc = false, .ctx = &g_ctx };
    g_gguf = gguf_init_from_file(gguf_path, params);
    if (!g_gguf) { fprintf(stderr, "failed to load %s\n", gguf_path); return 1; }

    uint32_t n_layers   = kv_u32("qwen2moe.block_count", 24);
    uint32_t d_model    = kv_u32("qwen2moe.embedding_length", 2048);
    uint32_t n_experts  = kv_u32("qwen2moe.expert_count", 60);
    uint32_t d_ffn_exp  = kv_u32("qwen2moe.expert_feed_forward_length", 1408);
    uint32_t n_vocab_kv = kv_u32("qwen2moe.vocab_size", 0);
    (void) kv_f32;

    printf("[gguf2native] n_layers=%u d_model=%u n_experts=%u d_ffn_expert=%u group_size=%d\n",
           n_layers, d_model, n_experts, d_ffn_exp, group_size);

    mkdir_p(out_dir);

    /* --- token_embd.weight -> embeddings.bin : float32[n_vocab * d_model] --- */
    {
        struct ggml_tensor *t = ggml_get_tensor(g_ctx, "token_embd.weight");
        if (!t) { fprintf(stderr, "token_embd.weight not found\n"); return 1; }
        int64_t n_vocab = t->ne[1];
        if (n_vocab_kv && (uint32_t)n_vocab != n_vocab_kv) {
            fprintf(stderr, "warn: vocab mismatch tensor=%lld kv=%u\n", (long long)n_vocab, n_vocab_kv);
        }
        float *emb = dequant_tensor(t);
        char path[1024];
        snprintf(path, sizeof(path), "%s/embeddings.bin", out_dir);
        FILE *f = fopen(path, "wb");
        if (!f) { fprintf(stderr, "cannot open %s\n", path); return 1; }
        write_all(f, emb, (size_t)n_vocab * d_model * sizeof(float));
        fclose(f);
        printf("[gguf2native] wrote embeddings.bin (%lld x %u floats)\n", (long long)n_vocab, d_model);
        free(emb);
    }

    /* --- output.weight -> output_proj.bin : float32[n_vocab * d_model] --- */
    {
        struct ggml_tensor *t = ggml_get_tensor(g_ctx, "output.weight");
        if (!t) { fprintf(stderr, "output.weight not found\n"); return 1; }
        int64_t n_vocab = t->ne[1];
        float *op = dequant_tensor(t);
        char path[1024];
        snprintf(path, sizeof(path), "%s/output_proj.bin", out_dir);
        FILE *f = fopen(path, "wb");
        if (!f) { fprintf(stderr, "cannot open %s\n", path); return 1; }
        write_all(f, op, (size_t)n_vocab * d_model * sizeof(float));
        fclose(f);
        printf("[gguf2native] wrote output_proj.bin (%lld x %u floats)\n", (long long)n_vocab, d_model);
        free(op);
    }

    /*
     * --- attention_dense.bin : float32[n_layers * d_model * 4] ---
     * Disassembly nails the total size (n_layers*d_model*4 floats) but not
     * which 4 per-layer d_model-vectors they are. Best-supported guess:
     * [attn_norm, attn_q.bias, attn_k.bias, attn_v.bias] per layer
     * (the attention-specific dense components; ffn_norm is treated as
     * part of the FFN/expert path and excluded). Unverified — refine via
     * generation-quality feedback from `bonfyre-moe chat`.
     */
    {
        char path[1024];
        snprintf(path, sizeof(path), "%s/attention_dense.bin", out_dir);
        FILE *f = fopen(path, "wb");
        if (!f) { fprintf(stderr, "cannot open %s\n", path); return 1; }
        for (uint32_t l = 0; l < n_layers; l++) {
            char name[256];
            const char *keys[4] = { "attn_norm.weight", "attn_q.bias", "attn_k.bias", "attn_v.bias" };
            for (int k = 0; k < 4; k++) {
                snprintf(name, sizeof(name), "blk.%u.%s", l, keys[k]);
                float *v = get_dequant(name);
                write_all(f, v, (size_t)d_model * sizeof(float));
                free(v);
            }
        }
        fclose(f);
        printf("[gguf2native] wrote attention_dense.bin (%u layers x 4 x %u floats)\n", n_layers, d_model);
    }

    /*
     * --- per-expert int4 files ---
     * gate_exps/up_exps/down_exps are stored in GGUF as stacked 3D tensors
     * [ne0,ne1,n_expert] with the expert index as the slowest-varying dim,
     * so expert E's slice is a contiguous run of ne0*ne1 elements starting
     * at offset E*ne0*ne1 in the fully-dequantized tensor.
     *
     * Flat layout written per expert (order is our own choice, matching
     * llama.cpp's w1/w3/w2 = gate/up/down convention):
     *   [gate_proj: d_ffn_exp * d_model][up_proj: d_ffn_exp * d_model][down_proj: d_model * d_ffn_exp]
     */
    for (uint32_t l = 0; l < n_layers; l++) {
        char name[256];
        snprintf(name, sizeof(name), "blk.%u.ffn_gate_exps.weight", l);
        float *gate_all = get_dequant(name);
        snprintf(name, sizeof(name), "blk.%u.ffn_up_exps.weight", l);
        float *up_all = get_dequant(name);
        snprintf(name, sizeof(name), "blk.%u.ffn_down_exps.weight", l);
        float *down_all = get_dequant(name);

        int64_t per_expert_gate = (int64_t)d_model * d_ffn_exp;
        int64_t per_expert_down = (int64_t)d_ffn_exp * d_model;
        int64_t n_elements = per_expert_gate * 2 + per_expert_down;

        float *flat = malloc((size_t)n_elements * sizeof(float));
        if (!flat) { fprintf(stderr, "oom\n"); return 1; }

        char dir[1024];
        snprintf(dir, sizeof(dir), "%s/experts/layer_%02u", out_dir, l);
        mkdir_p(dir);

        for (uint32_t e = 0; e < n_experts; e++) {
            size_t off = 0;
            for (const char *p = order; *p; p++) {
                if (*p == 'g') {
                    memcpy(flat + off, gate_all + (size_t)e * per_expert_gate, (size_t)per_expert_gate * sizeof(float));
                    off += per_expert_gate;
                } else if (*p == 'u') {
                    memcpy(flat + off, up_all + (size_t)e * per_expert_gate, (size_t)per_expert_gate * sizeof(float));
                    off += per_expert_gate;
                } else if (*p == 'd') {
                    memcpy(flat + off, down_all + (size_t)e * per_expert_down, (size_t)per_expert_down * sizeof(float));
                    off += per_expert_down;
                }
            }

            char path[1200];
            snprintf(path, sizeof(path), "%s/expert_%03u.int4", dir, e);
            write_int4_file(path, flat, n_elements, group_size);
        }

        free(flat);
        free(gate_all);
        free(up_all);
        free(down_all);
        printf("[gguf2native] layer %u: wrote %u expert files\n", l, n_experts);
    }

    printf("[gguf2native] done.\n");
    return 0;
}
