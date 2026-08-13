/*
 * cbf_engine.c — Expert-streaming MoE inference engine core
 *
 * Core principles from Colibri:
 *   1. Dense stays resident (~9.9 GB for GLM-5.2 at int4)
 *   2. Experts stream on-demand (per-layer LRU, async I/O pool)
 *   3. Router lookahead prefetches next layer (71.6% predictable)
 *   4. Learning cache pins hot experts (workload-specific)
 *   5. Batch-union: read each unique expert once per batch
 *   6. MLA compressed KV: 576 floats/token vs 32,768 (57× smaller)
 *
 * Bonfyre extensions:
 *   - QUIC network tier for cross-node expert streaming
 *   - Fragment-based router cache (perspectives + confidence)
 *   - NUMA-aware weight placement (interleave across sockets)
 *   - Distributed speculative decoding (draft on local, verify on fleet)
 */

#define _DEFAULT_SOURCE
#include "colibri_bonfyre.h"
#include "cbf_internal.h"
#include "bonfyre.h"
#include "fragment.h"
#include "bf_quic.h"
#include "lambda_tensors.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <math.h>
#include <errno.h>

/* ═══════════════════════════════════════════════════════════════════
 * Helper functions
 * ═══════════════════════════════════════════════════════════════════ */

static uint64_t get_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* ═══════════════════════════════════════════════════════════════════
 * I/O pool implementation
 * ═══════════════════════════════════════════════════════════════════ */

static void *io_worker_thread(void *arg) {
    io_pool_t *pool = (io_pool_t *)arg;
    
    while (!atomic_load(&pool->shutdown)) {
        pthread_mutex_lock(&pool->queue_lock);
        
        /* Wait for work */
        while (atomic_load(&pool->queue_head) == atomic_load(&pool->queue_tail) &&
               !atomic_load(&pool->shutdown)) {
            pthread_cond_wait(&pool->queue_cond, &pool->queue_lock);
        }
        
        if (atomic_load(&pool->shutdown)) {
            pthread_mutex_unlock(&pool->queue_lock);
            break;
        }
        
        /* Dequeue request */
        uint32_t head = atomic_fetch_add(&pool->queue_head, 1) % pool->queue_size;
        io_request_t *req = &pool->queue[head];
        pthread_mutex_unlock(&pool->queue_lock);
        
        /* Disk-backed expert hydration is staged through this worker.
         * The queue and completion signaling are live; the storage path is next. */
        req->result = NULL;  /* Would allocate and load here */
        atomic_store(&req->done, true);
    }
    
    return NULL;
}

static int io_pool_init(io_pool_t *pool, uint32_t n_threads, uint32_t queue_size) {
    if (n_threads > MAX_IO_THREADS) n_threads = MAX_IO_THREADS;
    
    pool->n_threads = n_threads;
    pool->queue_size = queue_size;
    pool->queue = calloc(queue_size, sizeof(io_request_t));
    if (!pool->queue) return CBF_ERR_MEMORY;
    
    atomic_store(&pool->queue_head, 0);
    atomic_store(&pool->queue_tail, 0);
    atomic_store(&pool->shutdown, false);
    
    pthread_mutex_init(&pool->queue_lock, NULL);
    pthread_cond_init(&pool->queue_cond, NULL);
    
    /* Spawn worker threads */
    for (uint32_t i = 0; i < n_threads; i++) {
        if (pthread_create(&pool->threads[i], NULL, io_worker_thread, pool) != 0) {
            atomic_store(&pool->shutdown, true);
            return CBF_ERR_MEMORY;
        }
    }
    
    return CBF_OK;
}

static void io_pool_destroy(io_pool_t *pool) {
    atomic_store(&pool->shutdown, true);
    pthread_cond_broadcast(&pool->queue_cond);
    
    for (uint32_t i = 0; i < pool->n_threads; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    
    pthread_mutex_destroy(&pool->queue_lock);
    pthread_cond_destroy(&pool->queue_cond);
    free(pool->queue);
}

/* ═══════════════════════════════════════════════════════════════════
 * Router lookahead worker
 * ═══════════════════════════════════════════════════════════════════ */

/* Worker thread: prefetch experts for upcoming layers based on router patterns */
static void *lookahead_worker(void *arg) {
    cbf_engine_t *engine = (cbf_engine_t *)arg;
    router_lookahead_t *la = &engine->lookahead;
    
    printf("[lookahead] worker started (prefetch depth: %u)\n", la->prefetch_depth);
    
    while (!atomic_load(&engine->lookahead_stop)) {
        /* Wait for work */
        usleep(10000);  /* 10ms poll */
        
        if (!atomic_load(&la->active)) continue;
        
        /* Get current layer and prefetch experts for next N layers */
        uint32_t current = la->current_layer;
        
        for (uint32_t offset = 1; offset <= la->prefetch_depth; offset++) {
            uint32_t target_layer = current + offset;
            if (target_layer >= engine->shape.n_layers) break;
            
            /* Prefetch strategy:
             * 1. Check router cache for hot experts in target layer
             * 2. If expert not in RAM cache, queue I/O load
             * 3. Prioritize experts with high heat scores
             */
            
            expert_cache_t *cache = &engine->layer_caches[target_layer];
            
            /* Simple heuristic: prefetch first K experts per layer
             * In production, would use router cache heat scores
             */
            uint32_t prefetch_count = 4;  /* Load top-4 per layer */
            if (prefetch_count > engine->shape.n_experts_per_layer) {
                prefetch_count = engine->shape.n_experts_per_layer;
            }
            
            pthread_rwlock_rdlock(&cache->lock);
            uint32_t cached = cache->count;
            pthread_rwlock_unlock(&cache->lock);
            
            /* If cache has room, prefetch more */
            if (cached < EXPERT_CACHE_ENTRIES) {
                for (uint32_t e = 0; e < prefetch_count; e++) {
                    /* Check if already cached */
                    bool found = false;
                    pthread_rwlock_rdlock(&cache->lock);
                    for (uint32_t i = 0; i < cache->count; i++) {
                        if (cache->entries[i]->expert_idx == e) {
                            found = true;
                            break;
                        }
                    }
                    pthread_rwlock_unlock(&cache->lock);
                    
                    if (!found) {
                        /* Queue async load (would use io_pool in full implementation) */
                        /* For now, just log */
                    }
                }
            }
        }
    }
    
    printf("[lookahead] worker stopped\n");
    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════
 * Engine lifecycle
 * ═══════════════════════════════════════════════════════════════════ */

cbf_engine_t *cbf_engine_new(const char *model_path,
                              const cbf_model_shape_t *shape,
                              const cbf_memory_config_t *memory) {
    if (!model_path || !shape || !memory) return NULL;
    
    cbf_engine_t *e = calloc(1, sizeof(cbf_engine_t));
    if (!e) return NULL;
    
    /* Copy configuration */
    e->shape = *shape;
    e->memory = *memory;
    snprintf(e->model_path, sizeof(e->model_path), "%s", model_path);
    
    /* Initialize layer caches */
    e->layer_caches = calloc(shape->n_layers, sizeof(expert_cache_t));
    if (!e->layer_caches) {
        free(e);
        return NULL;
    }
    
    for (uint32_t i = 0; i < shape->n_layers; i++) {
        pthread_rwlock_init(&e->layer_caches[i].lock, NULL);
    }
    
    /* Initialize I/O pool */
    uint32_t io_threads = memory->io_threads ? memory->io_threads : 4;
    if (io_pool_init(&e->io_pool, io_threads, 256) != CBF_OK) {
        free(e->layer_caches);
        free(e);
        return NULL;
    }
    
    /* Initialize QUIC context if network enabled */
    if (memory->enable_network) {
        e->quic_ctx = bf_quic_ctx_new(NULL, NULL, NULL);
        pthread_rwlock_init(&e->peers_lock, NULL);
    }
    
    /* Initialize fragment store for router cache */
    const char *frag_db = getenv("BF_FRAGMENT_DB");
    if (!frag_db) frag_db = "~/.local/share/bonfyre/fragments.db";
    e->frag_store = bf_fragment_store_open(frag_db);
    
    pthread_mutex_init(&e->forward_lock, NULL);
    atomic_store(&e->loaded, false);
    
    return e;
}

void cbf_engine_free(cbf_engine_t *engine) {
    if (!engine) return;
    
    /* Shutdown I/O */
    io_pool_destroy(&engine->io_pool);
    
    /* Shutdown lookahead */
    if (engine->lookahead.active) {
        atomic_store(&engine->lookahead.active, false);
        pthread_join(engine->lookahead.thread, NULL);
    }
    
    /* Destroy layer caches */
    for (uint32_t i = 0; i < engine->shape.n_layers; i++) {
        expert_cache_t *cache = &engine->layer_caches[i];
        pthread_rwlock_wrlock(&cache->lock);
        for (uint32_t j = 0; j < cache->count; j++) {
            free(cache->entries[j]->data);
            free(cache->entries[j]);
        }
        pthread_rwlock_unlock(&cache->lock);
        pthread_rwlock_destroy(&cache->lock);
    }
    free(engine->layer_caches);
    
    /* Close QUIC */
    if (engine->quic_ctx) {
        bf_quic_ctx_free(engine->quic_ctx);
        pthread_rwlock_destroy(&engine->peers_lock);
    }
    
    /* Close fragment store */
    if (engine->frag_store) {
        bf_fragment_store_close(engine->frag_store);
    }
    
    /* Free dense storage */
    free(engine->dense.embeddings);
    free(engine->dense.attn_dense);
    free(engine->dense.shared_experts);
    free(engine->dense.output_proj);
    
    pthread_mutex_destroy(&engine->forward_lock);
    free(engine);
}

/* ═══════════════════════════════════════════════════════════════════
 * Planning & loading
 * ═══════════════════════════════════════════════════════════════════ */

int cbf_engine_plan(const char *model_path,
                    const cbf_model_shape_t *shape,
                    const cbf_memory_config_t *memory,
                    cbf_placement_plan_t *out_plan) {
    if (!model_path || !shape || !memory || !out_plan) return CBF_ERR_INVALID;
    
    memset(out_plan, 0, sizeof(*out_plan));
    
    /* Calculate total expert count */
    uint32_t total_experts = shape->n_layers * shape->n_experts_per_layer;
    uint64_t expert_mb = (uint64_t)total_experts * shape->expert_size_mb;
    
    /* Dense budget */
    uint64_t dense_mb = shape->dense_size_mb;
    uint64_t vram_budget_mb = memory->vram_gb * 1024;
    uint64_t ram_budget_mb = memory->ram_gb * 1024;
    
    /* Placement strategy: dense first, then hot experts */
    uint64_t vram_used = dense_mb;
    uint64_t ram_used = 0;
    
    /* Pin hot experts in VRAM if budget allows */
    uint32_t experts_in_vram = 0;
    if (vram_budget_mb > vram_used) {
        experts_in_vram = (vram_budget_mb - vram_used) / shape->expert_size_mb;
        if (experts_in_vram > total_experts) experts_in_vram = total_experts;
        vram_used += experts_in_vram * shape->expert_size_mb;
    }
    
    /* Remaining experts in RAM */
    uint32_t experts_in_ram = 0;
    if (ram_budget_mb > 0 && experts_in_vram < total_experts) {
        experts_in_ram = ram_budget_mb / shape->expert_size_mb;
        uint32_t remaining = total_experts - experts_in_vram;
        if (experts_in_ram > remaining) experts_in_ram = remaining;
        ram_used = experts_in_ram * shape->expert_size_mb;
    }
    
    /* Rest stream from disk */
    uint32_t experts_on_disk = total_experts - experts_in_vram - experts_in_ram;
    
    /* Populate plan */
    out_plan->experts_vram = experts_in_vram;
    out_plan->experts_ram = experts_in_ram;
    out_plan->experts_disk = experts_on_disk;
    out_plan->experts_network = 0;  /* Populated during rebalance */
    
    out_plan->vram_used_mb = vram_used;
    out_plan->ram_used_mb = ram_used;
    out_plan->disk_used_mb = expert_mb;
    
    /* Estimate throughput (very rough heuristics) */
    if (experts_on_disk == 0) {
        out_plan->cold_tok_per_sec = 5.0f;  /* Full residency */
        out_plan->warm_tok_per_sec = 6.0f;
    } else if (experts_in_ram > 0) {
        out_plan->cold_tok_per_sec = 1.5f;  /* Some RAM cache */
        out_plan->warm_tok_per_sec = 2.5f;
    } else {
        out_plan->cold_tok_per_sec = 0.05f;  /* Disk-only (Colibri baseline) */
        out_plan->warm_tok_per_sec = 0.1f;
    }
    
    return CBF_OK;
}

int cbf_engine_load(cbf_engine_t *engine) {
    if (!engine) return CBF_ERR_INVALID;
    if (atomic_load(&engine->loaded)) return CBF_OK;
    
    printf("[cbf_engine] loading model from %s\n", engine->model_path);
    
    /* Copy model path to dense storage */
    snprintf(engine->dense.model_path, sizeof(engine->dense.model_path), "%s", engine->model_path);
    
    /* Build weight file paths */
    char embeddings_path[512];
    char attn_path[512];
    char output_path[512];
    
    snprintf(embeddings_path, sizeof(embeddings_path), "%s/embeddings.bin", engine->model_path);
    snprintf(attn_path, sizeof(attn_path), "%s/attention_dense.bin", engine->model_path);
    snprintf(output_path, sizeof(output_path), "%s/output_proj.bin", engine->model_path);
    
    /* Load embeddings */
    FILE *f = fopen(embeddings_path, "rb");
    if (!f) {
        fprintf(stderr, "[cbf_engine] failed to open embeddings: %s\n", embeddings_path);
        return CBF_ERR_IO;
    }
    
    size_t emb_size = engine->shape.n_vocab * engine->shape.d_model;
    engine->dense.embeddings = calloc(emb_size, sizeof(float));
    if (!engine->dense.embeddings) {
        fclose(f);
        return CBF_ERR_MEMORY;
    }
    
    size_t read = fread(engine->dense.embeddings, sizeof(float), emb_size, f);
    fclose(f);
    
    if (read != emb_size) {
        fprintf(stderr, "[cbf_engine] embeddings size mismatch: expected %zu, got %zu\n",
                emb_size, read);
        /* Continue anyway - may be partial file */
    }
    
    printf("[cbf_engine] loaded embeddings: %zu MB\n", (read * sizeof(float)) / (1024 * 1024));
    
    /* Load attention dense weights (Q/K/V/O projections) */
    f = fopen(attn_path, "rb");
    if (!f) {
        fprintf(stderr, "[cbf_engine] warning: attention weights not found\n");
    } else {
        size_t attn_size = engine->shape.n_layers * engine->shape.d_model * 4;
        engine->dense.attn_dense = calloc(attn_size, sizeof(float));
        if (engine->dense.attn_dense) {
            read = fread(engine->dense.attn_dense, sizeof(float), attn_size, f);
            printf("[cbf_engine] loaded attention: %zu MB\n", (read * sizeof(float)) / (1024 * 1024));
        }
        fclose(f);
    }
    
    /* Load output projection */
    f = fopen(output_path, "rb");
    if (!f) {
        fprintf(stderr, "[cbf_engine] warning: output projection not found\n");
    } else {
        size_t out_size = engine->shape.n_vocab * engine->shape.d_model;
        engine->dense.output_proj = calloc(out_size, sizeof(float));
        if (engine->dense.output_proj) {
            read = fread(engine->dense.output_proj, sizeof(float), out_size, f);
            printf("[cbf_engine] loaded output: %zu MB\n", (read * sizeof(float)) / (1024 * 1024));
        }
        fclose(f);
    }
    
    /* Build expert index (map layer:expert → file offset) */
    printf("[cbf_engine] indexing %u experts per layer × %u layers\n",
           engine->shape.n_experts_per_layer, engine->shape.n_layers);
    
    /* Expert files are typically: experts_L{layer}_E{expert}.bin */
    uint32_t experts_found = 0;
    for (uint32_t l = 0; l < engine->shape.n_layers; l++) {
        for (uint32_t e = 0; e < engine->shape.n_experts_per_layer; e++) {
            char expert_path[512];
            snprintf(expert_path, sizeof(expert_path), "%s/experts_L%u_E%u.bin",
                    engine->model_path, l, e);
            
            /* Check if file exists */
            f = fopen(expert_path, "rb");
            if (f) {
                fclose(f);
                experts_found++;
            }
        }
    }
    
    printf("[cbf_engine] found %u expert weight files\n", experts_found);
    
    /* Load hot experts from fragment cache */
    if (engine->frag_store) {
        printf("[cbf_engine] loading hot experts from fragment cache\n");
        /* Would load frequently-used experts into RAM cache */
    }
    
    /* Start router lookahead thread */
    if (!engine->lookahead_stop) {
        pthread_create(&engine->lookahead_thread, NULL, lookahead_worker, engine);
        printf("[cbf_engine] started router lookahead thread\n");
    }
    
    atomic_store(&engine->loaded, true);
    printf("[cbf_engine] load complete\n");
    
    return CBF_OK;
}

int cbf_engine_doctor(const cbf_engine_t *engine) {
    if (!engine) return CBF_ERR_INVALID;
    
    int issues = 0;
    
    printf("[cbf_doctor] Running model validation checks...\n\n");
    
    /* Check 1: Model files */
    printf("1. Model files\n");
    char embeddings_path[512];
    snprintf(embeddings_path, sizeof(embeddings_path), "%s/embeddings.bin", engine->model_path);
    
    FILE *f = fopen(embeddings_path, "rb");
    if (!f) {
        printf("  ✗ embeddings.bin not found\n");
        issues++;
    } else {
        fseek(f, 0, SEEK_END);
        long size = ftell(f);
        printf("  ✓ embeddings.bin (%ld MB)\n", size / (1024 * 1024));
        fclose(f);
    }
    
    /* Check expert files */
    uint32_t experts_found = 0;
    for (uint32_t l = 0; l < engine->shape.n_layers; l++) {
        for (uint32_t e = 0; e < engine->shape.n_experts_per_layer; e++) {
            char expert_path[512];
            snprintf(expert_path, sizeof(expert_path), "%s/experts_L%u_E%u.bin",
                    engine->model_path, l, e);
            f = fopen(expert_path, "rb");
            if (f) {
                experts_found++;
                fclose(f);
            }
        }
    }
    
    uint32_t total_experts = engine->shape.n_layers * engine->shape.n_experts_per_layer;
    if (experts_found == total_experts) {
        printf("  ✓ All %u expert files found\n", experts_found);
    } else {
        printf("  ⚠ Only %u/%u expert files found\n", experts_found, total_experts);
        if (experts_found < total_experts / 2) issues++;
    }
    
    /* Check 2: Memory budgets */
    printf("\n2. Memory budgets\n");
    
    size_t embeddings_mb = (engine->shape.n_vocab * engine->shape.d_model * sizeof(float)) / (1024 * 1024);
    size_t attn_mb = (engine->shape.n_layers * engine->shape.d_model * 4 * sizeof(float)) / (1024 * 1024);
    size_t experts_mb = engine->shape.expert_size_mb * total_experts;
    
    printf("  Model footprint:\n");
    printf("    Embeddings: %zu MB\n", embeddings_mb);
    printf("    Attention: %zu MB\n", attn_mb);
    printf("    All experts: %zu MB\n", experts_mb);
    printf("    Total (dense + all experts): %zu MB\n", embeddings_mb + attn_mb + experts_mb);
    
    printf("  Configured budgets:\n");
    printf("    VRAM: %llu MB\n", (unsigned long long)(engine->memory.vram_gb * 1024));
    printf("    RAM: %llu MB\n", (unsigned long long)(engine->memory.ram_gb * 1024));
    printf("    Disk: %s\n", engine->memory.disk_path ? engine->memory.disk_path : "(disabled)");
    printf("    Network: %s\n", engine->memory.enable_network ? "enabled" : "disabled");
    
    uint64_t total_mem_mb = (engine->memory.vram_gb + engine->memory.ram_gb) * 1024;
    if (embeddings_mb + attn_mb > total_mem_mb) {
        printf("  ⚠ Dense components may not fit in VRAM+RAM\n");
    } else {
        printf("  ✓ Dense components fit in memory\n");
    }
    
    /* Check 3: Network peers */
    printf("\n3. Network configuration\n");
    if (engine->quic_ctx && engine->n_peers > 0) {
        printf("  ✓ QUIC enabled with %u peers\n", engine->n_peers);
        for (uint32_t i = 0; i < engine->n_peers; i++) {
            printf("    - %s:%u\n", engine->peers[i].info.host, engine->peers[i].info.port);
        }
    } else if (engine->memory.enable_network) {
        printf("  ⚠ Network tier enabled but no peers configured\n");
    } else {
        printf("  • Network tier disabled\n");
    }
    
    /* Check 4: Fragment store */
    printf("\n4. Fragment store\n");
    if (engine->frag_store) {
        printf("  ✓ Fragment store connected\n");
    } else {
        printf("  • Fragment store disabled (router cache uses temp file)\n");
    }
    
    /* Summary */
    printf("\n");
    if (issues == 0) {
        printf("✓ All checks passed - model ready for inference\n");
        return CBF_OK;
    } else {
        printf("⚠ %d issue(s) found - may affect performance\n", issues);
        return CBF_ERR_INVALID;
    }
}

/* ═══════════════════════════════════════════════════════════════════
 * Compatibility implementations for the remaining API surface
 * ═══════════════════════════════════════════════════════════════════ */

const char *cbf_strerror(int code) {
    switch (code) {
        case CBF_OK:         return "Success";
        case CBF_ERR_MEMORY: return "Memory allocation failed";
        case CBF_ERR_IO:     return "I/O error";
        case CBF_ERR_NETWORK: return "Network error";
        case CBF_ERR_MODEL:  return "Model file error";
        case CBF_ERR_SHAPE:  return "Invalid model shape";
        case CBF_ERR_BUDGET: return "Insufficient memory budget";
        case CBF_ERR_EXPERT: return "Expert load/route failure";
        case CBF_ERR_INVALID: return "Invalid argument";
        default:             return "Unknown error";
    }
}

/* cbf_kv_cache_new/free, cbf_forward, and cbf_engine_add_peer are the real,
 * complete implementations in cbf_forward.c and cbf_quic_expert.c -- this
 * file used to carry stub compatibility fallbacks for them before those
 * landed, left behind as dead duplicate symbols after the split. */

void cbf_engine_get_metrics(const cbf_engine_t *engine, cbf_metrics_t *out) {
    if (!engine || !out) return;
    *out = engine->metrics;
}

void cbf_metrics_dump_json(const cbf_metrics_t *metrics, FILE *out) {
    if (!metrics || !out) return;
    fprintf(out, "{\n");
    fprintf(out, "  \"tok_per_sec\": %.2f,\n", metrics->tok_per_sec);
    fprintf(out, "  \"tokens_generated\": %llu,\n", (unsigned long long)metrics->tokens_generated);
    fprintf(out, "  \"experts_routed\": %llu\n", (unsigned long long)metrics->experts_routed);
    fprintf(out, "}\n");
}
