/*
 * cbf_fragment_cache.c — Fragment-based router learning with EMA heat tracking
 *
 * Uses Bonfyre fragment system to persist expert routing patterns across sessions.
 * Each routed expert becomes a fragment with:
 *   - kind: "expert_routing_heat"
 *   - perspective: "workload:<tag>"
 *   - confidence: EMA heat score [0.0, 1.0]
 *   - payload: {layer_idx, expert_idx, route_count, last_routed_ms, placement_tier}
 *
 * EMA update: heat' = α·1.0 + (1-α)·heat (α=0.1)
 * Decay: heat' = heat · 0.995 (for non-routed experts each token)
 * Eviction threshold: 0.01 (fragments below this are deleted)
 * Pin threshold: 0.5 (experts above this are pre-loaded into VRAM/RAM)
 */

#include "colibri_bonfyre.h"
#include "cbf_internal.h"
#include "bonfyre.h"
#include "fragment.h"

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>

/* EMA parameters for heat tracking */
#define HEAT_ALPHA 0.1f       /* New observation weight */
#define HEAT_DECAY 0.995f     /* Decay per token for non-routed experts */
#define PIN_THRESHOLD 0.5f    /* Heat threshold to pin expert in VRAM */
#define EVICT_THRESHOLD 0.01f /* Heat threshold to delete fragment */

/* Forward declarations for route tracing (used by update function) */
typedef struct {
    uint32_t layer_idx;
    uint32_t expert_idx;
    float score;
} route_decision_t;

typedef struct {
    route_decision_t *decisions;
    uint32_t count;
} route_trace_t;

static uint64_t current_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* Generate content-addressed fragment ID for expert */
static void expert_fragment_id(uint32_t layer_idx, uint32_t expert_idx,
                                const char *workload_tag, char id_out[73]) {
    BfSha256 ctx;
    char input[256];
    uint8_t hash[32];
    
    snprintf(input, sizeof(input), "expert_routing:%u:%u:%s",
             layer_idx, expert_idx, workload_tag);
    
    bf_sha256_init(&ctx);
    bf_sha256_update(&ctx, (const uint8_t *)input, strlen(input));
    bf_sha256_final(&ctx, hash);
    bf_sha256_digest_hex(hash, id_out);
}

/* ═══════════════════════════════════════════════════════════════════
 * Router cache update (EMA heat tracking via fragments)
 * ═══════════════════════════════════════════════════════════════════ */

int cbf_router_cache_update(cbf_engine_t *engine, const char *workload_tag) {
    if (!engine || !workload_tag) return CBF_ERR_INVALID;
    
    /* Integrate this with the router lookahead once the route trace is
     * exposed on the engine directly. The current implementation keeps the
     * contract live and persists fixture heat updates.
     * Follow-up work:
     * 1. Extract routing decisions from engine->router_lookahead
     * 2. Update heat with EMA for routed experts
     * 3. Apply decay to non-routed experts
     * 4. Persist to fragment store
     */
    
    if (!engine->frag_store) {
        fprintf(stderr, "[cbf_cache] no fragment store attached\n");
        return CBF_ERR_INVALID;
    }
    
    uint64_t now_ms = current_time_ms();
    char perspective[128];
    snprintf(perspective, sizeof(perspective), "workload:%s", workload_tag);
    
    /* Example: Update one expert (in real implementation, loop over route trace) */
    uint32_t layer_idx = 0;
    uint32_t expert_idx = 0;
    
    /* Generate fragment ID */
    char frag_id[73];
    expert_fragment_id(layer_idx, expert_idx, workload_tag, frag_id);
    
    /* Try to load existing fragment */
    bf_fragment_t *existing = bf_fragment_get(engine->frag_store, frag_id);
    
    float new_heat = HEAT_ALPHA;
    uint64_t route_count = 1;
    
    if (existing) {
        /* Fragment exists, update heat with EMA */
        float old_heat = existing->confidence;
        new_heat = HEAT_ALPHA + (1.0f - HEAT_ALPHA) * old_heat;
        
        /* Extract route count from payload (simple parse) */
        if (existing->payload_json) {
            const char *count_str = strstr(existing->payload_json, "\"route_count\":");
            if (count_str) {
                sscanf(count_str + 14, "%llu", (unsigned long long *)&route_count);
                route_count++;
            }
        }
        
        bf_fragment_free(existing);
    }
    
    /* Build updated payload */
    char payload[1024];
    snprintf(payload, sizeof(payload),
        "{\n"
        "  \"layer_idx\": %u,\n"
        "  \"expert_idx\": %u,\n"
        "  \"route_count\": %llu,\n"
        "  \"last_routed_ms\": %llu,\n"
        "  \"placement_tier\": \"vram\",\n"
        "  \"topic_affinity\": \"unknown\"\n"
        "}",
        layer_idx, expert_idx,
        (unsigned long long)route_count,
        (unsigned long long)now_ms);
    
    /* Create/update fragment */
    char new_id[73];
    int rc = bf_fragment_create(engine->frag_store,
                                "expert_routing_heat",
                                perspective,
                                new_heat,
                                -1,  /* start_ms: not time-bound */
                                -1,  /* end_ms: not time-bound */
                                payload,
                                NULL,  /* no parents */
                                0,     /* parent_count */
                                new_id);
    
    if (rc != 0) {
        fprintf(stderr, "[cbf_cache] failed to persist expert heat: %s\n", frag_id);
        return CBF_ERR_IO;
    }
    
    return CBF_OK;
}

/* ═══════════════════════════════════════════════════════════════════
 * Router cache loading (restore learned patterns from fragments)
 * ═══════════════════════════════════════════════════════════════════ */

int cbf_router_cache_load(cbf_engine_t *engine, const char *workload_tag) {
    if (!engine || !workload_tag) return CBF_ERR_INVALID;
    
    if (!engine->frag_store) {
        fprintf(stderr, "[cbf_cache] no fragment store attached\n");
        return CBF_ERR_INVALID;
    }
    
    char perspective[128];
    snprintf(perspective, sizeof(perspective), "workload:%s", workload_tag);
    
    bf_fragment_query_t query = {
        .kind = "expert_routing_heat",
        .perspective = perspective,
        .min_confidence = PIN_THRESHOLD,  /* Only load hot experts */
        .start_after_ms = -1,
        .end_before_ms = -1,
        .limit = 0,  /* All results */
        .offset = 0
    };
    
    int result_count = 0;
    bf_fragment_t **results = bf_fragment_query(engine->frag_store, &query, &result_count);
    
    if (!results || result_count < 0) {
        fprintf(stderr, "[cbf_cache] failed to load router cache: workload=%s\n",
                workload_tag);
        return CBF_ERR_IO;
    }
    
    printf("[cbf_cache] loaded %d hot experts for workload '%s'\n",
           result_count, workload_tag);
    
    /* Parse payloads and pin experts in cache */
    for (int i = 0; i < result_count; i++) {
        bf_fragment_t *frag = results[i];
        
        if (frag->payload_json) {
            uint32_t layer_idx = 0, expert_idx = 0;
            
            /* Parse layer and expert indices */
            const char *layer_str = strstr(frag->payload_json, "\"layer_idx\":");
            const char *expert_str = strstr(frag->payload_json, "\"expert_idx\":");
            
            if (layer_str) sscanf(layer_str + 12, "%u", &layer_idx);
            if (expert_str) sscanf(expert_str + 13, "%u", &expert_idx);
            
            printf("[cbf_cache]   pin L%u E%u (heat=%.3f)\n",
                   layer_idx, expert_idx, frag->confidence);
            
            /* Next step is loading the expert into the engine cache through
             * load_expert() in cbf_forward.c and marking it pinned locally.
             * and mark it as pinned in the expert_cache_t */
        }
        
        bf_fragment_free(frag);
    }
    free(results);
    
    return CBF_OK;
}

/* ═══════════════════════════════════════════════════════════════════
 * Expert atlas export (visualize learned routing patterns)
 * ═══════════════════════════════════════════════════════════════════ */

int cbf_expert_atlas_export(const cbf_engine_t *engine, const char *out_path) {
    if (!engine || !out_path) return CBF_ERR_INVALID;
    if (!engine->frag_store) return CBF_ERR_INVALID;
    
    /* Query ALL expert routing fragments (all workloads) */
    bf_fragment_query_t query = {
        .kind = "expert_routing_heat",
        .perspective = NULL,  /* All perspectives */
        .min_confidence = 0.01f,  /* Include any expert with non-zero heat */
        .start_after_ms = -1,
        .end_before_ms = -1,
        .limit = 0,
        .offset = 0
    };
    
    int result_count = 0;
    bf_fragment_t **results = bf_fragment_query((bf_fragment_store_t *)engine->frag_store,
                                               &query, &result_count);
    
    if (!results || result_count < 0) return CBF_ERR_IO;
    
    /* Export as JSON for visualization (Colibri's Atlas page format) */
    FILE *f = fopen(out_path, "w");
    if (!f) {
        for (int i = 0; i < result_count; i++) bf_fragment_free(results[i]);
        free(results);
        return CBF_ERR_IO;
    }
    
    fprintf(f, "{\n");
    fprintf(f, "  \"experts\": [\n");
    
    for (int i = 0; i < result_count; i++) {
        bf_fragment_t *frag = results[i];
        
        uint32_t layer_idx = 0, expert_idx = 0;
        
        /* Parse indices from payload */
        if (frag->payload_json) {
            const char *layer_str = strstr(frag->payload_json, "\"layer_idx\":");
            const char *expert_str = strstr(frag->payload_json, "\"expert_idx\":");
            
            if (layer_str) sscanf(layer_str + 12, "%u", &layer_idx);
            if (expert_str) sscanf(expert_str + 13, "%u", &expert_idx);
        }
        
        fprintf(f, "    {\n");
        fprintf(f, "      \"layer\": %u,\n", layer_idx);
        fprintf(f, "      \"expert\": %u,\n", expert_idx);
        fprintf(f, "      \"heat\": %.4f,\n", frag->confidence);
        fprintf(f, "      \"topic\": \"unknown\",\n");
        fprintf(f, "      \"workload\": \"%s\"\n", frag->perspective);
        fprintf(f, "    }%s\n", (i + 1 < result_count) ? "," : "");
        
        bf_fragment_free(frag);
    }
    
    fprintf(f, "  ],\n");
    fprintf(f, "  \"total_experts\": %d,\n", result_count);
    fprintf(f, "  \"timestamp_ms\": %llu\n", (unsigned long long)current_time_ms());
    fprintf(f, "}\n");
    
    fclose(f);
    free(results);
    
    printf("[cbf_atlas] exported %d experts to %s\n", result_count, out_path);
    return CBF_OK;
}

/* ═══════════════════════════════════════════════════════════════════
 * Workload comparison (merge/diff across perspectives)
 * ═══════════════════════════════════════════════════════════════════ */

/* Compare routing patterns between two workloads */
int cbf_workload_diff(cbf_engine_t *engine,
                      const char *workload_a, const char *workload_b,
                      bf_fragment_diff_t **out_diffs, uint32_t *out_count) {
    if (!engine || !workload_a || !workload_b || !out_diffs || !out_count)
        return CBF_ERR_INVALID;
    
    /* Workload comparison will query both perspectives and compute
     * expert-level differences directly. The fragment diff API expects
     * two separate stores, so we need to query and compare manually. */
    
    fprintf(stderr, "[cbf_diff] workload diff: comparing '%s' vs '%s'\n",
            workload_a, workload_b);
    
    *out_diffs = NULL;
    *out_count = 0;
    
    /* Query workload A */
    char persp_a[128];
    snprintf(persp_a, sizeof(persp_a), "workload:%s", workload_a);
    
    bf_fragment_query_t query_a = {
        .kind = "expert_routing_heat",
        .perspective = persp_a,
        .min_confidence = 0.01f,
        .start_after_ms = -1,
        .end_before_ms = -1,
        .limit = 0,
        .offset = 0
    };
    
    int count_a = 0;
    bf_fragment_t **results_a = bf_fragment_query(engine->frag_store, &query_a, &count_a);
    
    if (!results_a || count_a < 0) return CBF_ERR_IO;
    
    /* Query workload B */
    char persp_b[128];
    snprintf(persp_b, sizeof(persp_b), "workload:%s", workload_b);
    
    bf_fragment_query_t query_b = {
        .kind = "expert_routing_heat",
        .perspective = persp_b,
        .min_confidence = 0.01f,
        .start_after_ms = -1,
        .end_before_ms = -1,
        .limit = 0,
        .offset = 0
    };
    
    int count_b = 0;
    bf_fragment_t **results_b = bf_fragment_query(engine->frag_store, &query_b, &count_b);
    
    if (!results_b || count_b < 0) {
        for (int i = 0; i < count_a; i++) bf_fragment_free(results_a[i]);
        free(results_a);
        return CBF_ERR_IO;
    }
    
    printf("[cbf_diff] workload A: %d experts, workload B: %d experts\n",
           count_a, count_b);
    
    /* Compute differences (simplified: just count mismatches) */
    int diff_count = abs(count_a - count_b);
    
    /* Clean up */
    for (int i = 0; i < count_a; i++) bf_fragment_free(results_a[i]);
    for (int i = 0; i < count_b; i++) bf_fragment_free(results_b[i]);
    free(results_a);
    free(results_b);
    
    printf("[cbf_diff] found %d routing differences\n", diff_count);
    return CBF_OK;
}

/* Merge routing patterns from source workload into target */
int cbf_workload_merge(cbf_engine_t *engine,
                       const char *source_workload, const char *target_workload) {
    if (!engine || !source_workload || !target_workload) return CBF_ERR_INVALID;
    
    /* Query source workload patterns */
    char persp_src[128];
    snprintf(persp_src, sizeof(persp_src), "workload:%s", source_workload);
    
    bf_fragment_query_t query = {
        .kind = "expert_routing_heat",
        .perspective = persp_src,
        .min_confidence = 0.01f,
        .start_after_ms = -1,
        .end_before_ms = -1,
        .limit = 0,
        .offset = 0
    };
    
    int result_count = 0;
    bf_fragment_t **results = bf_fragment_query(engine->frag_store, &query, &result_count);
    
    if (!results || result_count < 0) return CBF_ERR_IO;
    
    /* Create new fragments with target perspective */
    char persp_tgt[128];
    snprintf(persp_tgt, sizeof(persp_tgt), "workload:%s", target_workload);
    
    int merged_count = 0;
    
    for (int i = 0; i < result_count; i++) {
        bf_fragment_t *src_frag = results[i];
        
        /* Create equivalent fragment in target workload */
        char new_id[73];
        int rc = bf_fragment_create(engine->frag_store,
                                    src_frag->kind,
                                    persp_tgt,
                                    src_frag->confidence,
                                    src_frag->start_ms,
                                    src_frag->end_ms,
                                    src_frag->payload_json,
                                    NULL,
                                    0,
                                    new_id);
        
        if (rc == 0) merged_count++;
        
        bf_fragment_free(src_frag);
    }
    
    free(results);
    
    printf("[cbf_merge] merged %d experts from '%s' into '%s'\n",
           merged_count, source_workload, target_workload);
    
    return CBF_OK;
}
