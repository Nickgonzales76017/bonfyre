/*
 * fpq_run.c — LLaMA-style transformer inference for Bonfyre Ember
 *
 * Supports: LLaMA-2, TinyLlama, Mistral (GQA), Qwen2 architectures.
 * All weight matmuls route through fpq_matmul() (SLI, zero decode).
 * Non-linear ops (rmsnorm, rope, silu, softmax, sampling) are ~200 LoC.
 *
 * Architecture params are read from a config JSON sidecar or specified
 * via fpq_run_config_t. A default config matches TinyLlama-1.1B.
 *
 * Usage (from fpq_cli.c):
 *   fpq run path/to/model.fpq [--tokenizer path/to/tokenizer.json]
 *              [--sys "You are..."] "User prompt"
 *              [--max-tokens N] [--temp F] [--top-p F] [--greedy]
 */
#include "fpq_run.h"
#include "libfpq.h"
#include "tokenizer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <stdint.h>

/* ═══════════════════════════════════════════════════════
 * Default config (TinyLlama-1.1B-Chat-v1.0)
 * ═══════════════════════════════════════════════════════ */

fpq_run_config_t fpq_run_default_config(void) {
    fpq_run_config_t c = {0};
    c.n_vocab        = 32000;
    c.d_model        = 2048;
    c.d_ffn          = 5632;
    c.n_layers       = 22;
    c.n_heads        = 32;
    c.n_kv_heads     = 4;
    c.rms_norm_eps   = 1e-5f;
    c.rope_theta     = 10000.0f;
    c.max_seq_len    = 2048;
    c.head_dim       = c.d_model / c.n_heads; /* 64 */
    c.arch           = FPQ_RUN_ARCH_LLAMA;
    c.max_new_tokens = 512;
    c.temperature    = 0.6f;
    c.top_p          = 0.9f;
    c.greedy         = 0;
    return c;
}

/* ═══════════════════════════════════════════════════════
 * Helpers: rmsnorm, rope, silu, softmax, sampling
 * ═══════════════════════════════════════════════════════ */

static void rms_norm(float *out, const float *x, const float *w,
                     int n, float eps) {
    float ss = 0.0f;
    for (int i = 0; i < n; i++) ss += x[i] * x[i];
    ss = 1.0f / sqrtf(ss / (float)n + eps);
    for (int i = 0; i < n; i++) out[i] = w[i] * (ss * x[i]);
}

static void silu_hadamard(float *gate, const float *up, int n) {
    /* gate[i] = silu(gate[i]) * up[i]  (replaces gate in-place) */
    for (int i = 0; i < n; i++) {
        float g = gate[i];
        float sig = 1.0f / (1.0f + expf(-g));
        gate[i] = g * sig * up[i];
    }
}

/* Apply RoPE to query/key head in-place.
 * head: float[head_dim], pos: token position */
static void rope_apply(float *head, int head_dim, int pos, float theta) {
    for (int i = 0; i < head_dim / 2; i++) {
        float freq = 1.0f / powf(theta, (float)(2 * i) / (float)head_dim);
        float angle = (float)pos * freq;
        float cos_a = cosf(angle);
        float sin_a = sinf(angle);
        float q0 = head[2 * i];
        float q1 = head[2 * i + 1];
        head[2 * i]     = q0 * cos_a - q1 * sin_a;
        head[2 * i + 1] = q0 * sin_a + q1 * cos_a;
    }
}

/* Grouped Multi-Head Attention (GQA).
 * q:         [n_heads * head_dim]
 * k_cache:   [n_kv_heads][max_seq][head_dim]
 * v_cache:   [n_kv_heads][max_seq][head_dim]
 * attn_out:  [n_heads * head_dim] (output)
 * Returns scratch allocated by caller: att_scratch [max_seq] */
static void gqa_attention(
        const float *q,
        const float *k_cache,     /* [max_seq * n_kv_heads * head_dim] */
        const float *v_cache,     /* [max_seq * n_kv_heads * head_dim] */
        float *attn_out,
        float *att_scratch,       /* [max_seq] scratch */
        int seq_len,
        int n_heads, int n_kv_heads, int head_dim, int max_seq_len) {

    int kv_group = n_heads / n_kv_heads; /* heads per KV head (GQA factor) */
    float scale = 1.0f / sqrtf((float)head_dim);

    for (int h = 0; h < n_heads; h++) {
        const float *qh = q + h * head_dim;
        int kv_h = h / kv_group;  /* map query head → KV head */

        /* Compute attention scores */
        float max_score = -1e30f;
        for (int t = 0; t <= seq_len; t++) {  /* seq_len inclusive (0-indexed) */
            const float *kh = k_cache + (t * n_kv_heads + kv_h) * head_dim;
            float score = 0.0f;
            for (int d = 0; d < head_dim; d++) score += qh[d] * kh[d];
            score *= scale;
            att_scratch[t] = score;
            if (score > max_score) max_score = score;
        }

        /* Softmax */
        float sum = 0.0f;
        for (int t = 0; t <= seq_len; t++) {
            att_scratch[t] = expf(att_scratch[t] - max_score);
            sum += att_scratch[t];
        }
        for (int t = 0; t <= seq_len; t++) att_scratch[t] /= sum;

        /* Weighted sum of values */
        float *outh = attn_out + h * head_dim;
        memset(outh, 0, (size_t)head_dim * sizeof(float));
        for (int t = 0; t <= seq_len; t++) {
            const float *vh = v_cache + (t * n_kv_heads + kv_h) * head_dim;
            for (int d = 0; d < head_dim; d++) outh[d] += att_scratch[t] * vh[d];
        }
    }
}

/* Comparator for qsort (descending probability) */
static int cmp_pi_desc(const void *a, const void *b) {
    const float pa = ((const struct { float p; int id; } *)a)->p;
    const float pb = ((const struct { float p; int id; } *)b)->p;
    return (pb > pa) ? 1 : (pb < pa) ? -1 : 0;
}

/* Top-p (nucleus) sampling */
static int sample_top_p(const float *probs, int vocab_size,
                        float top_p, float temperature, uint64_t *rng) {
    /* Temperature sampling: logits already softmaxed → probs */
    /* Simple top-p: sort by prob, include until cumsum >= top_p */

    /* We only need partial sort: find candidates above threshold */
    /* Use a small scratch sort on the stack isn't feasible for 32K vocab;
     * instead do two passes: find pivot, then sample */

    /* Fast path: greedy (temperature=0 handled by caller) */
    if (top_p >= 1.0f) {
        /* Sample proportionally */
        *rng ^= *rng << 13; *rng ^= *rng >> 7; *rng ^= *rng << 17;
        float r = (float)(*rng & 0xFFFFFF) / (float)0x1000000;
        float cdf = 0.0f;
        for (int i = 0; i < vocab_size; i++) {
            cdf += probs[i];
            if (r < cdf) return i;
        }
        return vocab_size - 1;
    }

    /* Top-p: collect candidates */
    /* Allocate index array (stack-friendly with malloc for large vocab) */
    typedef struct { float p; int id; } pi_t;
    pi_t *cands = (pi_t *)malloc((size_t)vocab_size * sizeof(pi_t));
    for (int i = 0; i < vocab_size; i++) { cands[i].p = probs[i]; cands[i].id = i; }

    qsort(cands, (size_t)vocab_size, sizeof(pi_t), cmp_pi_desc);

    /* Nucleus: keep tokens until cumulative prob >= top_p */
    float cum = 0.0f;
    int n_keep = 0;
    while (n_keep < vocab_size && cum < top_p) {
        cum += cands[n_keep].p;
        n_keep++;
    }
    if (n_keep == 0) n_keep = 1;

    /* Renormalize over kept tokens */
    float norm = 0.0f;
    for (int i = 0; i < n_keep; i++) norm += cands[i].p;

    *rng ^= *rng << 13; *rng ^= *rng >> 7; *rng ^= *rng << 17;
    float r = (float)(*rng & 0xFFFFFF) / (float)0x1000000 * norm;
    float c = 0.0f;
    int result = cands[0].id;
    for (int i = 0; i < n_keep; i++) {
        c += cands[i].p;
        if (r < c) { result = cands[i].id; break; }
    }
    free(cands);
    return result;
}

/* Softmax in-place */
static void softmax(float *x, int n) {
    float max = x[0];
    for (int i = 1; i < n; i++) if (x[i] > max) max = x[i];
    float sum = 0.0f;
    for (int i = 0; i < n; i++) { x[i] = expf(x[i] - max); sum += x[i]; }
    for (int i = 0; i < n; i++) x[i] /= sum;
}

/* ═══════════════════════════════════════════════════════
 * Tensor name helpers
 * ═══════════════════════════════════════════════════════ */

#define FPQ_RUN_NAME_BUF 128

static void tname(char *buf, fpq_run_arch_t arch, const char *suffix, int layer) {
    if (layer < 0) {
        snprintf(buf, FPQ_RUN_NAME_BUF, "%s", suffix);
        return;
    }
    switch (arch) {
        case FPQ_RUN_ARCH_LLAMA:
        case FPQ_RUN_ARCH_MISTRAL:
            snprintf(buf, FPQ_RUN_NAME_BUF, "model.layers.%d.%s", layer, suffix);
            break;
        case FPQ_RUN_ARCH_QWEN2:
            snprintf(buf, FPQ_RUN_NAME_BUF, "model.layers.%d.%s", layer, suffix);
            break;
    }
}

/* ═══════════════════════════════════════════════════════
 * fpq_run_generate — main inference loop
 * ═══════════════════════════════════════════════════════ */

int fpq_run_generate(
        fpq_model_t *model,
        const float *embed_table,  /* [n_vocab × d_model], row-major, pre-decoded */
        const float *norm_layers,  /* [n_layers × 2 × d_model] — input+post norms */
        const float *final_norm,   /* [d_model] */
        const int   *prompt_ids,
        int          prompt_len,
        const fpq_run_config_t *cfg,
        fpq_run_token_cb callback, /* called per generated token, NULL = print */
        void        *cb_data) {

    int d       = cfg->d_model;
    int d_ffn   = cfg->d_ffn;
    int n_lay   = cfg->n_layers;
    int n_h     = cfg->n_heads;
    int n_kv    = cfg->n_kv_heads;
    int hd      = cfg->head_dim;
    int max_seq = cfg->max_seq_len;
    int max_new = cfg->max_new_tokens;
    float eps   = cfg->rms_norm_eps;
    float theta = cfg->rope_theta;
    fpq_run_arch_t arch = cfg->arch;

    /* Allocate buffers */
    float *h       = (float *)calloc((size_t)d, sizeof(float));
    float *h_norm  = (float *)calloc((size_t)d, sizeof(float));
    float *q_buf   = (float *)calloc((size_t)(n_h * hd), sizeof(float));
    float *k_buf   = (float *)calloc((size_t)(n_kv * hd), sizeof(float));
    float *v_buf   = (float *)calloc((size_t)(n_kv * hd), sizeof(float));
    float *attn_out = (float *)calloc((size_t)(n_h * hd), sizeof(float));
    float *o_buf   = (float *)calloc((size_t)d, sizeof(float));
    float *gate_buf= (float *)calloc((size_t)d_ffn, sizeof(float));
    float *up_buf  = (float *)calloc((size_t)d_ffn, sizeof(float));
    float *ffn_out = (float *)calloc((size_t)d, sizeof(float));
    float *logits  = (float *)calloc((size_t)cfg->n_vocab, sizeof(float));
    float *att_scratch = (float *)calloc((size_t)max_seq, sizeof(float));

    /* Per-layer KV cache: [n_layers][max_seq][n_kv_heads][head_dim]
     * CRITICAL: each layer must have its own independent KV cache.
     * Using a single shared cache causes later layers to overwrite
     * the stored KV from earlier layers at the same sequence position. */
    size_t kv_size = (size_t)max_seq * n_kv * hd;
    float **k_caches = (float **)calloc((size_t)n_lay, sizeof(float *));
    float **v_caches = (float **)calloc((size_t)n_lay, sizeof(float *));
    for (int l = 0; l < n_lay; l++) {
        k_caches[l] = (float *)calloc(kv_size, sizeof(float));
        v_caches[l] = (float *)calloc(kv_size, sizeof(float));
    }

    if (!h || !h_norm || !q_buf || !k_buf || !v_buf || !attn_out ||
        !o_buf || !gate_buf || !up_buf || !ffn_out || !logits ||
        !att_scratch || !k_caches || !v_caches || !k_caches[0]) {
        fprintf(stderr, "fpq_run: OOM allocating buffers\n");
        return -1;
    }

    char name_buf[FPQ_RUN_NAME_BUF];
    uint64_t rng = (uint64_t)time(NULL) ^ 0xDEADBEEFCAFEBABEULL;

    /* Process prompt tokens, then generate */
    int total_pos = 0;          /* next KV cache position */
    int generated = 0;
    int next_token = -1;

    for (int step = 0; step < prompt_len + max_new; step++) {
        int token;
        if (step < prompt_len) {
            token = prompt_ids[step];
        } else {
            if (next_token < 0) break;
            token = next_token;
        }

        if (token < 0 || token >= cfg->n_vocab) break;

        /* Embed: h = embed_table[token] */
        memcpy(h, embed_table + (size_t)token * (size_t)d, (size_t)d * sizeof(float));

        /* Run through all transformer layers */
        for (int lay = 0; lay < n_lay; lay++) {
            const float *inp_norm_w = norm_layers + (size_t)lay * 2 * d;
            const float *post_norm_w = norm_layers + (size_t)lay * 2 * d + d;

            /* ── Self-attention ── */
            rms_norm(h_norm, h, inp_norm_w, d, eps);

            /* Q, K, V projections via SLI */
            tname(name_buf, arch, "self_attn.q_proj.weight", lay);
            fpq_matmul(model, name_buf, h_norm, q_buf);

            tname(name_buf, arch, "self_attn.k_proj.weight", lay);
            fpq_matmul(model, name_buf, h_norm, k_buf);

            tname(name_buf, arch, "self_attn.v_proj.weight", lay);
            fpq_matmul(model, name_buf, h_norm, v_buf);

            /* Apply RoPE to each query head */
            for (int hh = 0; hh < n_h; hh++)
                rope_apply(q_buf + hh * hd, hd, total_pos, theta);
            /* Apply RoPE to each KV head */
            for (int hh = 0; hh < n_kv; hh++)
                rope_apply(k_buf + hh * hd, hd, total_pos, theta);

            /* Store K, V in this layer's cache at position total_pos */
            int cache_off = total_pos * n_kv * hd;
            memcpy(k_caches[lay] + cache_off, k_buf, (size_t)(n_kv * hd) * sizeof(float));
            memcpy(v_caches[lay] + cache_off, v_buf, (size_t)(n_kv * hd) * sizeof(float));

            /* GQA attention over this layer's cache */
            memset(attn_out, 0, (size_t)(n_h * hd) * sizeof(float));
            gqa_attention(q_buf, k_caches[lay], v_caches[lay], attn_out, att_scratch,
                          total_pos, n_h, n_kv, hd, max_seq);

            /* Output projection */
            tname(name_buf, arch, "self_attn.o_proj.weight", lay);
            fpq_matmul(model, name_buf, attn_out, o_buf);

            /* Residual: h += o_buf */
            for (int i = 0; i < d; i++) h[i] += o_buf[i];

            /* ── Feed-forward (SwiGLU MLP) ── */
            rms_norm(h_norm, h, post_norm_w, d, eps);

            tname(name_buf, arch, "mlp.gate_proj.weight", lay);
            fpq_matmul(model, name_buf, h_norm, gate_buf);

            tname(name_buf, arch, "mlp.up_proj.weight", lay);
            fpq_matmul(model, name_buf, h_norm, up_buf);

            silu_hadamard(gate_buf, up_buf, d_ffn);  /* gate_buf = silu(gate)*up */

            tname(name_buf, arch, "mlp.down_proj.weight", lay);
            fpq_matmul(model, name_buf, gate_buf, ffn_out);

            /* Residual: h += ffn_out */
            for (int i = 0; i < d; i++) h[i] += ffn_out[i];
        }

        /* Final norm + LM head → logits */
        rms_norm(h_norm, h, final_norm, d, eps);

        tname(name_buf, arch, "lm_head.weight", -1);
        fpq_matmul(model, name_buf, h_norm, logits);

        /* ── Sampling ── */
        /* Apply temperature */
        if (cfg->temperature > 0.0f && !cfg->greedy) {
            for (int i = 0; i < cfg->n_vocab; i++)
                logits[i] /= cfg->temperature;
            softmax(logits, cfg->n_vocab);
            next_token = sample_top_p(logits, cfg->n_vocab,
                                      cfg->top_p, cfg->temperature, &rng);
        } else {
            /* Greedy: argmax */
            int best = 0;
            for (int i = 1; i < cfg->n_vocab; i++)
                if (logits[i] > logits[best]) best = i;
            next_token = best;
        }

        total_pos++;

        /* Emit generated tokens (not prompt tokens) */
        if (step >= prompt_len - 1) {
            if (next_token == 2 /* EOS */) break;  /* </s> */
            if (callback)
                callback(next_token, cb_data);
            generated++;
            if (generated >= max_new) break;
        }
    }

    free(h); free(h_norm); free(q_buf); free(k_buf); free(v_buf);
    free(attn_out); free(o_buf); free(gate_buf); free(up_buf); free(ffn_out);
    free(logits); free(att_scratch);
    for (int l = 0; l < n_lay; l++) { free(k_caches[l]); free(v_caches[l]); }
    free(k_caches); free(v_caches);

    return generated;
}

/* ═══════════════════════════════════════════════════════
 * fpq_run_load_norms — load RMS norm weights from model
 *
 * Fetches passthrough (1D) tensors from libfpq via decode.
 * Returns malloc'd array [n_layers × 2 × d_model].
 * ═══════════════════════════════════════════════════════ */

float *fpq_run_load_norms(fpq_model_t *model, const fpq_run_config_t *cfg) {
    int d = cfg->d_model;
    int n = cfg->n_layers;
    float *buf = (float *)calloc((size_t)n * 2 * d, sizeof(float));
    char name_buf[FPQ_RUN_NAME_BUF];

    for (int lay = 0; lay < n; lay++) {
        tname(name_buf, cfg->arch, "input_layernorm.weight", lay);
        fpq_decode_one(model, name_buf, buf + (size_t)lay * 2 * d);

        tname(name_buf, cfg->arch, "post_attention_layernorm.weight", lay);
        fpq_decode_one(model, name_buf, buf + (size_t)lay * 2 * d + d);
    }
    return buf;
}

float *fpq_run_load_final_norm(fpq_model_t *model, const fpq_run_config_t *cfg) {
    float *buf = (float *)calloc((size_t)cfg->d_model, sizeof(float));
    fpq_decode_one(model, "model.norm.weight", buf);
    return buf;
}

/* Load embedding table: decode embed_tokens → [n_vocab × d_model].
 * This is the only at-start decode; ~256 MB for 32K × 2048 fp32. */
float *fpq_run_load_embeddings(fpq_model_t *model, const fpq_run_config_t *cfg) {
    size_t n = (size_t)cfg->n_vocab * (size_t)cfg->d_model;
    float *buf = (float *)malloc(n * sizeof(float));
    if (!buf) { fprintf(stderr, "fpq_run: OOM for embed table\n"); return NULL; }
    fpq_decode_one(model, "model.embed_tokens.weight", buf);
    return buf;
}

/* ═══════════════════════════════════════════════════════
 * cmd_run — top-level entry point called from fpq_cli.c
 * ═══════════════════════════════════════════════════════ */

/* Token callback: stream to stdout */
static void stream_token(int token_id, void *data) {
    tokenizer_t *tok = (tokenizer_t *)data;
    const char *s = tok_id_to_str(tok, token_id);
    if (!s || !*s) return;

    /* Convert ▁ → space for display */
    while (*s) {
        if ((unsigned char)s[0] == 0xE2 &&
            (unsigned char)s[1] == 0x96 &&
            (unsigned char)s[2] == 0x81) {
            putchar(' ');
            s += 3;
        } else {
            putchar((unsigned char)*s);
            s++;
        }
    }
    fflush(stdout);
}

int cmd_run(int argc, char **argv) {
    /* Parse args:
     *   fpq run <model.fpq> "prompt"
     *            [--tokenizer <path>]
     *            [--sys "system prompt"]
     *            [--max-tokens N]
     *            [--temp F]
     *            [--top-p F]
     *            [--greedy]
     *            [--no-chat]   (skip chat template)
     */
    if (argc < 4) {
        fprintf(stderr,
            "Usage: fpq run <model.fpq> \"prompt\" [options]\n"
            "\n"
            "Options:\n"
            "  --tokenizer <path>   Path to tokenizer.json (default: model dir)\n"
            "  --sys \"text\"         System prompt\n"
            "  --max-tokens N       Max tokens to generate (default: 512)\n"
            "  --temp F             Temperature 0.0–2.0 (default: 0.6)\n"
            "  --top-p F            Top-p nucleus sampling (default: 0.9)\n"
            "  --greedy             Greedy decoding (overrides temp)\n"
            "  --no-chat            Don't apply chat template\n"
            "\n"
            "Example:\n"
            "  fpq run models/tinyllama-v12/model.fpq \"What is 2+2?\"\n");
        return 1;
    }

    const char *model_path = argv[2];
    const char *prompt     = NULL;
    const char *sys_prompt = "You are a helpful assistant.";
    const char *tok_path   = NULL;
    int max_new_tokens     = 512;
    float temperature      = 0.6f;
    float top_p            = 0.9f;
    int greedy             = 0;
    int no_chat            = 0;

    /* Parse positional prompt + options */
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--tokenizer") == 0 && i+1 < argc)
            tok_path = argv[++i];
        else if (strcmp(argv[i], "--sys") == 0 && i+1 < argc)
            sys_prompt = argv[++i];
        else if (strcmp(argv[i], "--max-tokens") == 0 && i+1 < argc)
            max_new_tokens = atoi(argv[++i]);
        else if (strcmp(argv[i], "--temp") == 0 && i+1 < argc)
            temperature = (float)atof(argv[++i]);
        else if (strcmp(argv[i], "--top-p") == 0 && i+1 < argc)
            top_p = (float)atof(argv[++i]);
        else if (strcmp(argv[i], "--greedy") == 0)
            greedy = 1;
        else if (strcmp(argv[i], "--no-chat") == 0)
            no_chat = 1;
        else if (argv[i][0] != '-')
            prompt = argv[i];
    }

    if (!prompt) {
        fprintf(stderr, "fpq run: no prompt provided\n");
        return 1;
    }

    /* Auto-discover tokenizer.json next to model.fpq */
    char auto_tok_path[2048];
    if (!tok_path) {
        /* Try same directory as model */
        strncpy(auto_tok_path, model_path, sizeof(auto_tok_path) - 32);
        char *slash = strrchr(auto_tok_path, '/');
        if (slash) {
            strcpy(slash + 1, "tokenizer.json");
        } else {
            strcpy(auto_tok_path, "tokenizer.json");
        }
        tok_path = auto_tok_path;
    }

    /* Load tokenizer */
    fprintf(stderr, "Loading tokenizer: %s\n", tok_path);
    tokenizer_t *tok = tok_load(tok_path);
    if (!tok) {
        fprintf(stderr, "fpq run: failed to load tokenizer from %s\n", tok_path);
        fprintf(stderr, "         Try: --tokenizer /path/to/tokenizer.json\n");
        return 1;
    }

    /* Load model */
    fprintf(stderr, "Loading model: %s\n", model_path);
    double t0 = (double)clock() / CLOCKS_PER_SEC;
    fpq_model_t *model = fpq_open(model_path);
    if (!model) { tok_free(tok); return 1; }
    double t_load = (double)clock() / CLOCKS_PER_SEC - t0;
    fprintf(stderr, "Model loaded in %.1fs\n", t_load);

    /* Build config (TODO: read from model metadata / config.json sidecar) */
    fpq_run_config_t cfg = fpq_run_default_config();
    cfg.max_new_tokens = max_new_tokens;
    cfg.temperature    = temperature;
    cfg.top_p          = top_p;
    cfg.greedy         = greedy;

    /* Load norm weights (passthrough tensors — fast) */
    fprintf(stderr, "Loading norms + embeddings...\n");
    float *norms     = fpq_run_load_norms(model, &cfg);
    float *final_norm= fpq_run_load_final_norm(model, &cfg);
    float *embeddings= fpq_run_load_embeddings(model, &cfg);

    if (!norms || !final_norm || !embeddings) {
        fprintf(stderr, "fpq run: failed to load model tensors\n");
        fpq_close(model); tok_free(tok);
        free(norms); free(final_norm); free(embeddings);
        return 1;
    }

    /* Build prompt with chat template (TinyLlama ChatML / im_start format) */
    char *full_prompt = NULL;
    if (!no_chat) {
        /* <|im_start|>system\n{sys}<|im_end|>\n<|im_start|>user\n{user}<|im_end|>\n<|im_start|>assistant\n */
        size_t len = strlen(sys_prompt) + strlen(prompt) + 128;
        full_prompt = (char *)malloc(len);
        snprintf(full_prompt, len,
            "<|im_start|>system\n%s<|im_end|>\n<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n",
            sys_prompt, prompt);
    } else {
        full_prompt = strdup(prompt);
    }

    /* Tokenize */
    int n_tokens = 0;
    int *ids = tok_encode(tok, full_prompt, 1 /* add BOS */, &n_tokens);
    free(full_prompt);

    if (!ids || n_tokens == 0) {
        fprintf(stderr, "fpq run: tokenization failed\n");
        fpq_close(model); tok_free(tok);
        free(norms); free(final_norm); free(embeddings);
        return 1;
    }

    fprintf(stderr, "Prompt: %d tokens → generating (max %d)...\n",
            n_tokens, max_new_tokens);

    /* Run generation */
    double t1 = (double)clock() / CLOCKS_PER_SEC;
    int generated = fpq_run_generate(
        model, embeddings, norms, final_norm,
        ids, n_tokens, &cfg,
        stream_token, tok);
    double t_gen = (double)clock() / CLOCKS_PER_SEC - t1;

    printf("\n");
    if (generated > 0 && t_gen > 0)
        fprintf(stderr, "\n%.1f tok/s (%d tokens in %.1fs)\n",
                (float)generated / t_gen, generated, t_gen);

    free(ids);
    free(norms);
    free(final_norm);
    free(embeddings);
    fpq_close(model);
    tok_free(tok);
    return 0;
}
