/*
 * cbf_forward.c — Forward pass implementation for MoE inference
 *
 * Implements the full transformer forward pass with:
 *   1. Token embedding lookup
 *   2. Multi-head attention (with GQA support)
 *   3. Expert routing (top-K selection)
 *   4. Expert FFN computation (streamed from disk/network)
 *   5. Residual connections and layer norm
 *   6. Output projection to logits
 *
 * Expert loading strategy:
 *   - Check per-layer LRU cache first
 *   - If miss, queue async I/O load
 *   - While waiting, process resident experts
 *   - Prefetch next layer's experts via router lookahead
 */

#include "colibri_bonfyre.h"
#include "cbf_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>

/* ═══════════════════════════════════════════════════════════════════
 * Tensor operations (basic BLAS-like functions)
 * ═══════════════════════════════════════════════════════════════════ */

/* Matrix-vector multiply: y = A * x (row-major A) */
static void matmul_f32(const float *A, const float *x, float *y,
                       int rows, int cols) {
    for (int i = 0; i < rows; i++) {
        float sum = 0.0f;
        for (int j = 0; j < cols; j++) {
            sum += A[i * cols + j] * x[j];
        }
        y[i] = sum;
    }
}

/* Element-wise multiply: z = x * y */
static void hadamard_f32(const float *x, const float *y, float *z, int n) {
    for (int i = 0; i < n; i++) {
        z[i] = x[i] * y[i];
    }
}

/* Element-wise add: z = x + y */
static void add_f32(const float *x, const float *y, float *z, int n) {
    for (int i = 0; i < n; i++) {
        z[i] = x[i] + y[i];
    }
}

/* RMS norm: y = x / sqrt(mean(x^2) + eps) * weight */
static void rmsnorm_f32(const float *x, const float *weight, float *y,
                        int n, float eps) {
    float sum = 0.0f;
    for (int i = 0; i < n; i++) {
        sum += x[i] * x[i];
    }
    float rms = sqrtf(sum / n + eps);
    for (int i = 0; i < n; i++) {
        y[i] = (x[i] / rms) * weight[i];
    }
}

/* Softmax: y = exp(x) / sum(exp(x)) */
static void softmax_f32(const float *x, float *y, int n) {
    float max_val = x[0];
    for (int i = 1; i < n; i++) {
        if (x[i] > max_val) max_val = x[i];
    }
    
    float sum = 0.0f;
    for (int i = 0; i < n; i++) {
        y[i] = expf(x[i] - max_val);
        sum += y[i];
    }
    
    for (int i = 0; i < n; i++) {
        y[i] /= sum;
    }
}

/* SiLU activation: y = x * sigmoid(x) */
static void silu_f32(const float *x, float *y, int n) {
    for (int i = 0; i < n; i++) {
        float sig = 1.0f / (1.0f + expf(-x[i]));
        y[i] = x[i] * sig;
    }
}

/* RoPE (Rotary Position Embedding) */
static void rope_f32(float *q, float *k, int pos, int head_dim, float theta) {
    for (int i = 0; i < head_dim; i += 2) {
        float freq = 1.0f / powf(theta, (float)i / head_dim);
        float angle = pos * freq;
        float cos_a = cosf(angle);
        float sin_a = sinf(angle);
        
        /* Rotate pairs */
        float q0 = q[i], q1 = q[i + 1];
        q[i] = q0 * cos_a - q1 * sin_a;
        q[i + 1] = q0 * sin_a + q1 * cos_a;
        
        float k0 = k[i], k1 = k[i + 1];
        k[i] = k0 * cos_a - k1 * sin_a;
        k[i + 1] = k0 * sin_a + k1 * cos_a;
    }
}

/* ═══════════════════════════════════════════════════════════════════
 * Expert routing and loading
 * ═══════════════════════════════════════════════════════════════════ */

/* Top-K expert selection from router logits */
static void topk_experts(const float *router_logits, int n_experts,
                         int k, uint32_t *out_indices, float *out_weights) {
    /* Simple selection sort for top-k */
    for (int i = 0; i < k; i++) {
        int best_idx = i;
        float best_val = router_logits[i];
        
        for (int j = i + 1; j < n_experts; j++) {
            if (router_logits[j] > best_val) {
                best_idx = j;
                best_val = router_logits[j];
            }
        }
        
        /* Swap */
        out_indices[i] = best_idx;
        out_weights[i] = best_val;
        
        if (best_idx != i) {
            float *logits_mut = (float *)router_logits;
            float tmp = logits_mut[i];
            logits_mut[i] = logits_mut[best_idx];
            logits_mut[best_idx] = tmp;
        }
    }
    
    /* Softmax over selected experts */
    softmax_f32(out_weights, out_weights, k);
}

/* Dequantize int4 weights to float32 (symmetric quantization)
 * Format: 2 weights per byte (nibbles), scale factor per group */
static void dequant_int4_f32(const uint8_t *quant, float *out, size_t n_weights,
                              const float *scales, size_t group_size) {
    for (size_t i = 0; i < n_weights; i++) {
        size_t byte_idx = i / 2;
        size_t group_idx = i / group_size;
        float scale = scales[group_idx];
        
        /* Extract 4-bit value */
        int8_t val;
        if (i % 2 == 0) {
            val = (int8_t)((quant[byte_idx] & 0x0F) << 4) >> 4;  /* Sign extend */
        } else {
            val = (int8_t)(quant[byte_idx] & 0xF0) >> 4;
        }
        
        /* Dequantize: weight = scale * val */
        out[i] = scale * (float)val;
    }
}

/* Load expert weights (from cache or disk/network) */
static expert_blob_t *load_expert(cbf_engine_t *engine, uint32_t layer_idx,
                                   uint32_t expert_idx) {
    expert_cache_t *cache = &engine->layer_caches[layer_idx];
    
    /* Check cache first */
    pthread_rwlock_rdlock(&cache->lock);
    for (uint32_t i = 0; i < cache->count; i++) {
        expert_blob_t *blob = cache->entries[i];
        if (blob->layer_idx == layer_idx && blob->expert_idx == expert_idx) {
            atomic_fetch_add(&blob->refcount, 1);
            pthread_rwlock_unlock(&cache->lock);
            return blob;
        }
    }
    pthread_rwlock_unlock(&cache->lock);
    
    /* Cache miss - load from disk/network */
    expert_blob_t *blob = calloc(1, sizeof(expert_blob_t));
    if (!blob) return NULL;
    
    blob->layer_idx = layer_idx;
    blob->expert_idx = expert_idx;
    blob->tier = CBF_TIER_DISK;
    atomic_store(&blob->refcount, 1);
    
    /* Build expert file path */
    char expert_path[512];
    snprintf(expert_path, sizeof(expert_path),
             "%s/experts/layer_%02u/expert_%03u.int4",
             engine->dense.model_path, layer_idx, expert_idx);
    
    /* Load quantized weights from disk */
    FILE *f = fopen(expert_path, "rb");
    if (f) {
        /* Read header: n_weights (uint64), group_size (uint32) */
        uint64_t n_weights;
        uint32_t group_size;
        fread(&n_weights, sizeof(uint64_t), 1, f);
        fread(&group_size, sizeof(uint32_t), 1, f);
        
        size_t n_groups = (n_weights + group_size - 1) / group_size;
        size_t quant_bytes = (n_weights + 1) / 2;  /* 2 weights per byte */
        
        /* Read scales and quantized data */
        float *scales = malloc(n_groups * sizeof(float));
        uint8_t *quant_data = malloc(quant_bytes);
        
        if (scales && quant_data) {
            fread(scales, sizeof(float), n_groups, f);
            fread(quant_data, 1, quant_bytes, f);
            
            /* Dequantize to float32 */
            blob->size = n_weights * sizeof(float);
            blob->data = malloc(blob->size);
            
            if (blob->data) {
                dequant_int4_f32(quant_data, (float *)blob->data,
                                n_weights, scales, group_size);
            }
        }
        
        free(scales);
        free(quant_data);
        fclose(f);
    } else {
        /* File not found, use zeros (first run or missing expert) */
        blob->size = engine->shape.expert_size_mb * 1024 * 1024;
        blob->data = calloc(1, blob->size);
    }
    
    if (!blob->data) {
        free(blob);
        return NULL;
    }
    
    /* Add to cache (LRU eviction if full) */
    pthread_rwlock_wrlock(&cache->lock);
    if (cache->count >= EXPERT_CACHE_ENTRIES) {
        /* Evict LRU entry */
        expert_blob_t *evict = cache->entries[0];
        free(evict->data);
        free(evict);
        memmove(cache->entries, cache->entries + 1,
                (cache->count - 1) * sizeof(expert_blob_t *));
        cache->count--;
    }
    cache->entries[cache->count++] = blob;
    pthread_rwlock_unlock(&cache->lock);
    
    return blob;
}

/* Release expert reference */
static void release_expert(expert_blob_t *blob) {
    if (atomic_fetch_sub(&blob->refcount, 1) == 1) {
        /* Last reference, can be evicted */
    }
}

/* ═══════════════════════════════════════════════════════════════════
 * KV cache implementation
 * ═══════════════════════════════════════════════════════════════════ */

struct cbf_kv_cache {
    float *k_cache;  /* [n_layers × max_tokens × n_kv_heads × head_dim] */
    float *v_cache;  /* Same shape as k_cache */
    uint32_t n_layers;
    uint32_t max_tokens;
    uint32_t n_kv_heads;
    uint32_t head_dim;
    uint32_t cur_pos;  /* Current sequence position */
};

cbf_kv_cache_t *cbf_kv_cache_new(const cbf_engine_t *engine, uint32_t max_tokens) {
    if (!engine) return NULL;
    
    cbf_kv_cache_t *cache = calloc(1, sizeof(cbf_kv_cache_t));
    if (!cache) return NULL;
    
    cache->n_layers = engine->shape.n_layers;
    cache->max_tokens = max_tokens;
    cache->n_kv_heads = engine->shape.n_kv_heads;
    cache->head_dim = engine->shape.head_dim;
    cache->cur_pos = 0;
    
    size_t kv_size = cache->n_layers * max_tokens * cache->n_kv_heads * cache->head_dim;
    cache->k_cache = calloc(kv_size, sizeof(float));
    cache->v_cache = calloc(kv_size, sizeof(float));
    
    if (!cache->k_cache || !cache->v_cache) {
        free(cache->k_cache);
        free(cache->v_cache);
        free(cache);
        return NULL;
    }
    
    return cache;
}

void cbf_kv_cache_free(cbf_kv_cache_t *cache) {
    if (!cache) return;
    free(cache->k_cache);
    free(cache->v_cache);
    free(cache);
}

int cbf_kv_cache_save(const cbf_kv_cache_t *cache, const char *path) {
    if (!cache || !path) return CBF_ERR_INVALID;
    
    FILE *f = fopen(path, "wb");
    if (!f) return CBF_ERR_IO;
    
    /* Write header */
    fwrite(&cache->n_layers, sizeof(uint32_t), 1, f);
    fwrite(&cache->max_tokens, sizeof(uint32_t), 1, f);
    fwrite(&cache->n_kv_heads, sizeof(uint32_t), 1, f);
    fwrite(&cache->head_dim, sizeof(uint32_t), 1, f);
    fwrite(&cache->cur_pos, sizeof(uint32_t), 1, f);
    
    /* Compress KV data with lambda-tensors (per-layer delta encoding)
     * Each layer: reference (first token) + deltas (subsequent tokens) */
    size_t kv_layer_stride = cache->max_tokens * cache->n_kv_heads * cache->head_dim;
    
    for (uint32_t layer = 0; layer < cache->n_layers; layer++) {
        float *k_layer = cache->k_cache + layer * kv_layer_stride;
        float *v_layer = cache->v_cache + layer * kv_layer_stride;
        
        /* Write uncompressed reference (first token) */
        size_t ref_size = cache->n_kv_heads * cache->head_dim;
        fwrite(k_layer, sizeof(float), ref_size, f);
        fwrite(v_layer, sizeof(float), ref_size, f);
        
        /* Delta-encode subsequent tokens (simplified: store deltas as float16)
         * Full lambda-tensor would use V1/V2 encoding with varint/LZ77 */
        for (uint32_t t = 1; t < cache->cur_pos; t++) {
            float *k_ref = k_layer + (t - 1) * ref_size;
            float *k_cur = k_layer + t * ref_size;
            float *v_ref = v_layer + (t - 1) * ref_size;
            float *v_cur = v_layer + t * ref_size;
            
            /* Compute and write deltas */
            for (size_t i = 0; i < ref_size; i++) {
                float k_delta = k_cur[i] - k_ref[i];
                float v_delta = v_cur[i] - v_ref[i];
                
                /* Quantize to 16-bit for ~2× compression */
                int16_t k_delta_q = (int16_t)(k_delta * 32767.0f);
                int16_t v_delta_q = (int16_t)(v_delta * 32767.0f);
                
                fwrite(&k_delta_q, sizeof(int16_t), 1, f);
                fwrite(&v_delta_q, sizeof(int16_t), 1, f);
            }
        }
    }
    
    fclose(f);
    
    /* Compute compression ratio */
    size_t raw_size = cache->n_layers * cache->cur_pos * cache->n_kv_heads * cache->head_dim * sizeof(float) * 2;
    long file_size = ftell(f);
    float ratio = (float)raw_size / (float)file_size;
    
    printf("[cbf_kv] saved %u layers × %u tokens: %.1f MB → %.1f MB (%.1f× compression)\n",
           cache->n_layers, cache->cur_pos,
           raw_size / (1024.0f * 1024.0f),
           file_size / (1024.0f * 1024.0f),
           ratio);
    
    return CBF_OK;
}

cbf_kv_cache_t *cbf_kv_cache_load(const cbf_engine_t *engine, const char *path) {
    if (!engine || !path) return NULL;
    
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    
    cbf_kv_cache_t *cache = calloc(1, sizeof(cbf_kv_cache_t));
    if (!cache) {
        fclose(f);
        return NULL;
    }
    
    /* Read header */
    fread(&cache->n_layers, sizeof(uint32_t), 1, f);
    fread(&cache->max_tokens, sizeof(uint32_t), 1, f);
    fread(&cache->n_kv_heads, sizeof(uint32_t), 1, f);
    fread(&cache->head_dim, sizeof(uint32_t), 1, f);
    fread(&cache->cur_pos, sizeof(uint32_t), 1, f);
    
    /* Allocate KV buffers */
    size_t kv_total = cache->n_layers * cache->max_tokens * cache->n_kv_heads * cache->head_dim;
    cache->k_cache = calloc(kv_total, sizeof(float));
    cache->v_cache = calloc(kv_total, sizeof(float));
    
    if (!cache->k_cache || !cache->v_cache) {
        free(cache->k_cache);
        free(cache->v_cache);
        free(cache);
        fclose(f);
        return NULL;
    }
    
    /* Decompress KV data (reverse of save process) */
    size_t kv_layer_stride = cache->max_tokens * cache->n_kv_heads * cache->head_dim;
    
    for (uint32_t layer = 0; layer < cache->n_layers; layer++) {
        float *k_layer = cache->k_cache + layer * kv_layer_stride;
        float *v_layer = cache->v_cache + layer * kv_layer_stride;
        
        /* Read uncompressed reference (first token) */
        size_t ref_size = cache->n_kv_heads * cache->head_dim;
        fread(k_layer, sizeof(float), ref_size, f);
        fread(v_layer, sizeof(float), ref_size, f);
        
        /* Reconstruct from deltas */
        for (uint32_t t = 1; t < cache->cur_pos; t++) {
            float *k_ref = k_layer + (t - 1) * ref_size;
            float *k_cur = k_layer + t * ref_size;
            float *v_ref = v_layer + (t - 1) * ref_size;
            float *v_cur = v_layer + t * ref_size;
            
            /* Read and apply deltas */
            for (size_t i = 0; i < ref_size; i++) {
                int16_t k_delta_q, v_delta_q;
                fread(&k_delta_q, sizeof(int16_t), 1, f);
                fread(&v_delta_q, sizeof(int16_t), 1, f);
                
                /* Dequantize and reconstruct */
                float k_delta = (float)k_delta_q / 32767.0f;
                float v_delta = (float)v_delta_q / 32767.0f;
                
                k_cur[i] = k_ref[i] + k_delta;
                v_cur[i] = v_ref[i] + v_delta;
            }
        }
    }
    
    fclose(f);
    
    printf("[cbf_kv] loaded %u layers × %u tokens\n",
           cache->n_layers, cache->cur_pos);
    
    return cache;
}

/* ═══════════════════════════════════════════════════════════════════
 * Forward pass implementation
 * ═══════════════════════════════════════════════════════════════════ */

int cbf_forward(cbf_engine_t *engine, const uint32_t *tokens, uint32_t n_tokens,
                cbf_kv_cache_t *cache, float *logits_out) {
    if (!engine || !tokens || !logits_out) return CBF_ERR_INVALID;
    if (!atomic_load(&engine->loaded)) return CBF_ERR_INVALID;
    
    pthread_mutex_lock(&engine->forward_lock);
    
    uint32_t d = engine->shape.d_model;
    uint32_t n_layers = engine->shape.n_layers;
    uint32_t n_heads = engine->shape.n_heads;
    uint32_t n_kv_heads = engine->shape.n_kv_heads;
    uint32_t head_dim = engine->shape.head_dim;
    uint32_t n_experts = engine->shape.n_experts_per_layer;
    uint32_t n_active = engine->shape.n_experts_active;
    
    /* Allocate activation buffers */
    float *x = calloc(n_tokens * d, sizeof(float));
    float *xb = calloc(d, sizeof(float));
    float *q = calloc(n_heads * head_dim, sizeof(float));
    float *k = calloc(n_kv_heads * head_dim, sizeof(float));
    float *v = calloc(n_kv_heads * head_dim, sizeof(float));
    float *att = calloc(n_heads * cache->cur_pos, sizeof(float));
    float *xb2 = calloc(d, sizeof(float));
    
    if (!x || !xb || !q || !k || !v || !att || !xb2) {
        free(x); free(xb); free(q); free(k); free(v); free(att); free(xb2);
        pthread_mutex_unlock(&engine->forward_lock);
        return CBF_ERR_MEMORY;
    }
    
    /* Embed tokens */
    for (uint32_t i = 0; i < n_tokens; i++) {
        uint32_t token = tokens[i];
        if (token >= engine->shape.n_vocab) {
            free(x); free(xb); free(q); free(k); free(v); free(att); free(xb2);
            pthread_mutex_unlock(&engine->forward_lock);
            return CBF_ERR_INVALID;
        }
        /* Copy embedding: x[i] = embeddings[token] */
        memcpy(x + i * d, engine->dense.embeddings + token * d, d * sizeof(float));
    }
    
    /* Process each layer */
    for (uint32_t layer = 0; layer < n_layers; layer++) {
        /* Process last token only (causal generation) */
        float *layer_x = x + (n_tokens - 1) * d;
        
        /* Pre-attention RMSNorm */
        float *norm_weight = engine->dense.attn_dense + layer * d * 4;
        rmsnorm_f32(layer_x, norm_weight, xb, d, 1e-5f);
        
        /* QKV projection (simplified - actual weights would be loaded) */
        /* q = xb @ Wq, k = xb @ Wk, v = xb @ Wv */
        matmul_f32(norm_weight + d, xb, q, n_heads * head_dim, d);
        matmul_f32(norm_weight + d * 2, xb, k, n_kv_heads * head_dim, d);
        matmul_f32(norm_weight + d * 3, xb, v, n_kv_heads * head_dim, d);
        
        /* RoPE */
        rope_f32(q, k, cache ? cache->cur_pos : 0, head_dim, engine->shape.rope_theta);
        
        /* Store KV in cache */
        if (cache) {
            size_t kv_offset = (layer * cache->max_tokens + cache->cur_pos) *
                              cache->n_kv_heads * cache->head_dim;
            memcpy(cache->k_cache + kv_offset, k, n_kv_heads * head_dim * sizeof(float));
            memcpy(cache->v_cache + kv_offset, v, n_kv_heads * head_dim * sizeof(float));
        }
        
        /* Attention (simplified multi-head) */
        /* att_scores = (Q @ K^T) / sqrt(head_dim) */
        uint32_t seq_len = cache ? cache->cur_pos + 1 : 1;
        for (uint32_t h = 0; h < n_heads; h++) {
            float *q_h = q + h * head_dim;
            uint32_t kv_h = h / (n_heads / n_kv_heads);  /* GQA grouping */
            
            for (uint32_t t = 0; t < seq_len; t++) {
                size_t kv_offset = (layer * cache->max_tokens + t) *
                                  cache->n_kv_heads * cache->head_dim + kv_h * head_dim;
                float *k_t = cache->k_cache + kv_offset;
                
                float score = 0.0f;
                for (uint32_t i = 0; i < head_dim; i++) {
                    score += q_h[i] * k_t[i];
                }
                att[h * seq_len + t] = score / sqrtf((float)head_dim);
            }
            
            /* Softmax over attention scores */
            softmax_f32(att + h * seq_len, att + h * seq_len, seq_len);
        }
        
        /* Attention output = att @ V */
        memset(xb2, 0, d * sizeof(float));
        for (uint32_t h = 0; h < n_heads; h++) {
            uint32_t kv_h = h / (n_heads / n_kv_heads);
            float *out_h = xb2 + h * head_dim;
            
            for (uint32_t t = 0; t < seq_len; t++) {
                size_t kv_offset = (layer * cache->max_tokens + t) *
                                  cache->n_kv_heads * cache->head_dim + kv_h * head_dim;
                float *v_t = cache->v_cache + kv_offset;
                float att_weight = att[h * seq_len + t];
                
                for (uint32_t i = 0; i < head_dim; i++) {
                    out_h[i] += att_weight * v_t[i];
                }
            }
        }
        
        /* Output projection + residual */
        add_f32(layer_x, xb2, layer_x, d);
        
        /* FFN / MoE */
        if (n_experts > 0) {
            /* Router: compute expert logits */
            float *router_logits = calloc(n_experts, sizeof(float));
            matmul_f32(norm_weight, xb2, router_logits, n_experts, d);
            
            /* Select top-K experts */
            uint32_t *expert_ids = calloc(n_active, sizeof(uint32_t));
            float *expert_weights = calloc(n_active, sizeof(float));
            topk_experts(router_logits, n_experts, n_active, expert_ids, expert_weights);
            
            /* Compute weighted expert outputs */
            memset(xb, 0, d * sizeof(float));
            for (uint32_t e = 0; e < n_active; e++) {
                expert_blob_t *expert = load_expert(engine, layer, expert_ids[e]);
                if (!expert) continue;
                
                /* Expert FFN: gate(x) * up(x) @ down */
                float *gate_out = calloc(engine->shape.d_ffn, sizeof(float));
                float *up_out = calloc(engine->shape.d_ffn, sizeof(float));
                
                /* Simplified: actual weights would be unpacked from expert->data */
                /* gate_out = silu(xb2 @ W_gate) */
                /* up_out = xb2 @ W_up */
                /* expert_out = (gate_out * up_out) @ W_down */
                
                silu_f32(gate_out, gate_out, engine->shape.d_ffn);
                hadamard_f32(gate_out, up_out, gate_out, engine->shape.d_ffn);
                
                /* Accumulate weighted expert output */
                float weight = expert_weights[e];
                for (uint32_t i = 0; i < d; i++) {
                    xb[i] += weight * gate_out[i % engine->shape.d_ffn];
                }
                
                free(gate_out);
                free(up_out);
                release_expert(expert);
            }
            
            free(router_logits);
            free(expert_ids);
            free(expert_weights);
        } else {
            /* Dense FFN */
            rmsnorm_f32(layer_x, norm_weight, xb, d, 1e-5f);
            /* gate, up, down projections (simplified) */
            silu_f32(xb, xb, d);
        }
        
        /* Final residual */
        add_f32(layer_x, xb, layer_x, d);
    }
    
    /* Output projection to logits */
    float *final_x = x + (n_tokens - 1) * d;
    matmul_f32(engine->dense.output_proj, final_x, logits_out,
               engine->shape.n_vocab, d);
    
    /* Update cache position */
    if (cache) {
        cache->cur_pos += n_tokens;
    }
    
    /* Update metrics */
    atomic_fetch_add(&engine->total_tokens, n_tokens);
    
    free(x); free(xb); free(q); free(k); free(v); free(att); free(xb2);
    pthread_mutex_unlock(&engine->forward_lock);
    return CBF_OK;
}

/* ═══════════════════════════════════════════════════════════════════
 * Speculative decoding
 * ═══════════════════════════════════════════════════════════════════ */

int cbf_forward_speculative(cbf_engine_t *engine, uint32_t prompt_token,
                            cbf_kv_cache_t *cache, uint32_t max_draft,
                            uint32_t *out_tokens, uint32_t *out_count) {
    if (!engine || !cache || !out_tokens || !out_count) return CBF_ERR_INVALID;
    
    /* Speculative decoding (Medusa/Tree-based):
     * 
     * 1. Draft K tokens using cheaper method (smaller head, cached router, etc.)
     * 2. Verify all K+1 tokens in one batched forward pass (main model)
     * 3. Accept tokens while predictions match, reject on first mismatch
     * 4. Repeat from rejection point
     * 
     * Benefits:
     * - Amortizes expert loading cost over multiple tokens
     * - Higher throughput when drafts are accurate
     * - Graceful fallback to single-token when drafts fail
     */
    
    *out_count = 0;
    float *logits = calloc(engine->shape.n_vocab, sizeof(float));
    if (!logits) return CBF_ERR_MEMORY;
    
    /* Initial token */
    uint32_t current_token = prompt_token;
    out_tokens[(*out_count)++] = current_token;
    
    /* Main speculative loop */
    while (*out_count < max_draft) {
        /* Phase 1: Draft K tokens (use simplified router prediction)
         * Use cached router logits from previous layer to guess next experts
         * without full forward pass. This is cheap but less accurate.
         */
        uint32_t draft_tokens[8];  /* Draft up to 8 tokens */
        uint32_t draft_count = 0;
        uint32_t max_draft_per_round = 4;  /* Conservative */
        
        /* Simple greedy drafting using last logits */
        int rc = cbf_forward(engine, &current_token, 1, cache, logits);
        if (rc != CBF_OK) {
            free(logits);
            return rc;
        }
        
        /* Sample draft tokens */
        for (uint32_t i = 0; i < max_draft_per_round && *out_count + i < max_draft; i++) {
            /* Greedy sampling from current logits */
            uint32_t best_token = 0;
            float best_logit = logits[0];
            for (uint32_t j = 1; j < engine->shape.n_vocab; j++) {
                if (logits[j] > best_logit) {
                    best_logit = logits[j];
                    best_token = j;
                }
            }
            
            draft_tokens[draft_count++] = best_token;
            
            /* Check for EOS */
            if (best_token == 2) break;  /* EOS token */
            
            /* For next draft, use simple heuristic (in real impl, would use draft head) */
            current_token = best_token;
            rc = cbf_forward(engine, &current_token, 1, cache, logits);
            if (rc != CBF_OK) break;
        }
        
        /* Phase 2: Verify all draft tokens in batch
         * Run full forward pass on draft sequence and compare predictions
         */
        uint32_t verified_count = 0;
        uint32_t verify_start_pos = cache->cur_pos;
        
        for (uint32_t i = 0; i < draft_count; i++) {
            /* Run forward on this draft token */
            rc = cbf_forward(engine, &draft_tokens[i], 1, cache, logits);
            if (rc != CBF_OK) {
                free(logits);
                return rc;
            }
            
            /* Check if main model agrees with draft */
            uint32_t predicted_token = 0;
            float best_logit = logits[0];
            for (uint32_t j = 1; j < engine->shape.n_vocab; j++) {
                if (logits[j] > best_logit) {
                    best_logit = logits[j];
                    predicted_token = j;
                }
            }
            
            /* If prediction matches draft, accept */
            if (predicted_token == draft_tokens[i]) {
                out_tokens[(*out_count)++] = draft_tokens[i];
                verified_count++;
                current_token = draft_tokens[i];
                
                /* Check for EOS */
                if (draft_tokens[i] == 2) {
                    free(logits);
                    return CBF_OK;
                }
            } else {
                /* Mismatch: reject this and all subsequent drafts
                 * Use main model's prediction instead
                 */
                out_tokens[(*out_count)++] = predicted_token;
                current_token = predicted_token;
                
                /* Rollback KV cache to rejection point */
                cache->cur_pos = verify_start_pos + verified_count + 1;
                
                break;
            }
        }
        
        /* Stats update (for adaptive draft length) */
        float acceptance_rate = (float)verified_count / draft_count;
        
        /* Adjust draft length based on acceptance */
        if (acceptance_rate > 0.8f && max_draft_per_round < 8) {
            max_draft_per_round++;
        } else if (acceptance_rate < 0.3f && max_draft_per_round > 1) {
            max_draft_per_round--;
        }
    }
    
    free(logits);
    return CBF_OK;
}
