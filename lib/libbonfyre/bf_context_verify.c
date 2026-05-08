#define _POSIX_C_SOURCE 200809L
#include "include/bonfyre.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── internal helpers ───────────────────────────────────────────────────── */

static uint32_t clamp_u32_v_(uint32_t v, uint32_t lo, uint32_t hi) {
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

static double clamp_f64_v_(double v, double lo, double hi) {
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

static const char *safe_mode_v_(const char *mode) {
    if (!mode || !mode[0]) return "hybrid";
    if (strcmp(mode, "dense") == 0)      return "dense";
    if (strcmp(mode, "compressed") == 0) return "compressed";
    if (strcmp(mode, "membrane") == 0)   return "membrane";
    if (strcmp(mode, "state") == 0)      return "state";
    if (strcmp(mode, "hybrid") == 0)     return "hybrid";
    return "hybrid";
}

typedef enum {
    VPOL_HOT_EXACT       = 0,
    VPOL_MEMBRANE_SELECT = 1,
    VPOL_COMPRESS        = 2,
    VPOL_EVICT           = 3,
} VPolicy;

typedef struct {
    uint32_t idx;
    double   score;
} VRankRow;

static int vrank_cmp_desc_(const void *a, const void *b) {
    const VRankRow *ra = (const VRankRow *)a;
    const VRankRow *rb = (const VRankRow *)b;
    if (ra->score < rb->score) return  1;
    if (ra->score > rb->score) return -1;
    if (ra->idx   > rb->idx)   return  1;
    if (ra->idx   < rb->idx)   return -1;
    return 0;
}

static void default_budgets_v_(
    const char *mode, uint32_t n,
    uint32_t *hot, uint32_t *membrane, uint32_t *compress, uint32_t *evict
) {
    if (strcmp(mode, "dense") == 0)      { *hot = n; *membrane = 0; *compress = 0; *evict = 0; return; }
    if (strcmp(mode, "compressed") == 0) { *hot = 0; *membrane = 0; *compress = n; *evict = 0; return; }
    if (strcmp(mode, "membrane") == 0) {
        *hot = n / 8u; *membrane = (n * 5u) / 8u; *compress = n / 8u;
        *evict = n - (*hot + *membrane + *compress); return;
    }
    if (strcmp(mode, "state") == 0) {
        *hot = n / 10u; *membrane = (n * 4u) / 10u; *compress = (n * 4u) / 10u;
        *evict = n - (*hot + *membrane + *compress); return;
    }
    /* hybrid */
    *hot = n / 5u; *membrane = (n * 9u) / 20u; *compress = (n * 3u) / 10u;
    *evict = n - (*hot + *membrane + *compress);
}

/* ── defaults ───────────────────────────────────────────────────────────── */

void bf_context_kv_verify_defaults(BfContextVerifyConfig *cfg) {
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->mode = "hybrid";
    cfg->layers = 24;
    cfg->heads = 16;
    cfg->seq_tokens = 131072;
    cfg->block_tokens = 128;
    cfg->top_blocks = 128;
    cfg->hot_exact_budget_blocks = 24;
    cfg->membrane_budget_blocks = 56;
    cfg->compress_budget_blocks = 32;
    cfg->evict_budget_blocks = 16;
    cfg->required_witnesses = 12;
    cfg->base_residual = 0.18;
    cfg->continuity_fail_rate = 0.05;
    cfg->objective_profile = "balanced";
    cfg->w_attention_predictor = 1.0;
    cfg->w_state_relevance = 0.8;
    cfg->w_witness_relevance = 1.2;
    cfg->w_continuity_risk = 1.0;
    cfg->w_layer_need = 0.5;
    cfg->w_recency = 0.4;
    cfg->w_objective_match = 0.7;
    cfg->w_memory_cost = 0.6;
    cfg->w_residual_error = 0.7;
    cfg->residual_drift_threshold = 0.35;
    cfg->kv_budget_mb = 512.0;
}

/* ── main entry ─────────────────────────────────────────────────────────── */

int bf_context_kv_verify_json(const BfContextVerifyConfig *cfg, char **out_json) {
    if (!cfg || !out_json) return -1;

    const char    *mode          = safe_mode_v_(cfg->mode);
    const uint32_t layers        = clamp_u32_v_(cfg->layers, 1, 256);
    const uint32_t heads         = clamp_u32_v_(cfg->heads, 1, 256);
    const uint32_t seq_tokens    = clamp_u32_v_(cfg->seq_tokens, 256, 1u << 30);
    const uint32_t block_tokens  = clamp_u32_v_(cfg->block_tokens, 16, 8192);
    const uint32_t top_blocks    = clamp_u32_v_(cfg->top_blocks, 1, 8192);
    const uint32_t req_witnesses = cfg->required_witnesses;
    const double   base_residual = clamp_f64_v_(cfg->base_residual, 0.0, 10.0);
    const double   fail_rate     = clamp_f64_v_(cfg->continuity_fail_rate, 0.0, 1.0);
    const char    *objective     = cfg->objective_profile ? cfg->objective_profile : "balanced";
    const double   drift_thresh  = clamp_f64_v_(cfg->residual_drift_threshold, 0.0, 100.0);
    const double   kv_budget_mb  = clamp_f64_v_(cfg->kv_budget_mb, 0.0, 1e9);

    const double w_attention  = clamp_f64_v_(cfg->w_attention_predictor, 0.0, 100.0);
    const double w_state      = clamp_f64_v_(cfg->w_state_relevance,     0.0, 100.0);
    const double w_witness    = clamp_f64_v_(cfg->w_witness_relevance,   0.0, 100.0);
    const double w_continuity = clamp_f64_v_(cfg->w_continuity_risk,     0.0, 100.0);
    const double w_layer      = clamp_f64_v_(cfg->w_layer_need,          0.0, 100.0);
    const double w_recency    = clamp_f64_v_(cfg->w_recency,             0.0, 100.0);
    const double w_objective  = clamp_f64_v_(cfg->w_objective_match,     0.0, 100.0);
    const double w_mem_cost   = clamp_f64_v_(cfg->w_memory_cost,         0.0, 100.0);
    const double w_residual   = clamp_f64_v_(cfg->w_residual_error,      0.0, 100.0);

    const uint32_t total_blocks = (seq_tokens + block_tokens - 1u) / block_tokens;
    uint32_t n_blocks = top_blocks;
    if (n_blocks > total_blocks) n_blocks = total_blocks;

    /* Resolve budgets */
    uint32_t bgt_hot = cfg->hot_exact_budget_blocks;
    uint32_t bgt_mem = cfg->membrane_budget_blocks;
    uint32_t bgt_cmp = cfg->compress_budget_blocks;
    uint32_t bgt_evi = cfg->evict_budget_blocks;

    if ((bgt_hot + bgt_mem + bgt_cmp + bgt_evi) == 0u) {
        default_budgets_v_(mode, n_blocks, &bgt_hot, &bgt_mem, &bgt_cmp, &bgt_evi);
    }
    {
        uint32_t rem = n_blocks;
        if (bgt_hot > rem) bgt_hot = rem; rem -= bgt_hot;
        if (bgt_mem > rem) bgt_mem = rem; rem -= bgt_mem;
        if (bgt_cmp > rem) bgt_cmp = rem; rem -= bgt_cmp;
        if (bgt_evi > rem) bgt_evi = rem; rem -= bgt_evi;
        bgt_cmp += rem;
    }

    /* Allocate per-block arrays */
    typedef struct {
        uint32_t layer;
        uint32_t token_start;
        double   residual_norm;
        double   attention_mass;
        double   continuity_risk;
        double   witness_relevance;
        double   state_relevance;
        double   layer_need;
        double   recency;
        double   objective_match;
        double   memory_cost;
        double   residual_error;
        double   selection_score;
        char     witness_hex[65];
        int      is_fail;
        int      witness_critical;
        const char *continuity;
        VPolicy  policy;
    } VBlock;

    VBlock   *blocks = (VBlock *)calloc(n_blocks, sizeof(VBlock));
    VRankRow *ranked = (VRankRow *)calloc(n_blocks, sizeof(VRankRow));
    if (!blocks || !ranked) { free(blocks); free(ranked); return -1; }

    uint32_t fail_stride = 0;
    if (fail_rate > 0.0) {
        fail_stride = (uint32_t)floor(1.0 / fail_rate);
        if (fail_stride == 0) fail_stride = 1;
    }

    /* Build blocks */
    for (uint32_t i = 0; i < n_blocks; i++) {
        VBlock *b = &blocks[i];
        b->layer       = i % layers;
        b->token_start = i * block_tokens;
        b->attention_mass  = 1.0 / (double)(i + 2u);
        if (b->attention_mass < 0.0005) b->attention_mass = 0.0005;
        b->residual_norm   = base_residual + ((double)(i % 17u) * 0.007);
        b->is_fail         = (fail_stride > 0u && (i % fail_stride) == 0u) ? 1 : 0;
        b->witness_critical = (req_witnesses > 0u && i < req_witnesses) ? 1 : 0;
        b->continuity      = b->is_fail ? "CONTINUITY_FAIL"
                           : ((b->residual_norm > base_residual + 0.06) ? "PASS_WITH_DRIFT" : "PASS");

        char seed[256];
        snprintf(seed, sizeof(seed), "%s|%u|%u|%u|%u|%u|%u",
            mode, b->layer, (i / layers) % heads, b->token_start,
            b->token_start + block_tokens - 1u, req_witnesses, i);
        bf_sha256_hex((const uint8_t *)seed, strlen(seed), b->witness_hex);

        b->attention_mass  = 1.0 / (double)(i + 2u);
        if (b->attention_mass < 0.0005) b->attention_mass = 0.0005;

        b->state_relevance   = clamp_f64_v_(1.0 - ((double)i / (double)(n_blocks + 1u)), 0.0, 1.0);
        b->witness_relevance = b->witness_critical ? 1.0 : 0.15;
        b->continuity_risk   = b->is_fail ? 1.0 : ((strcmp(b->continuity, "PASS_WITH_DRIFT") == 0) ? 0.5 : 0.1);
        b->layer_need        = clamp_f64_v_(1.0 - ((double)b->layer / (double)layers), 0.0, 1.0);
        b->recency           = clamp_f64_v_(1.0 - ((double)b->token_start / (double)(seq_tokens + 1u)), 0.0, 1.0);
        b->memory_cost       = (double)block_tokens / 1024.0;
        b->residual_error    = clamp_f64_v_(b->residual_norm / (base_residual + 0.2), 0.0, 4.0);

        if (strcmp(objective, "latency") == 0) {
            b->objective_match = clamp_f64_v_(0.7 * b->recency + 0.3 * (1.0 / (1.0 + b->memory_cost)), 0.0, 1.0);
        } else if (strcmp(objective, "continuity") == 0) {
            b->objective_match = clamp_f64_v_(0.7 * b->continuity_risk + 0.3 * b->witness_relevance, 0.0, 1.0);
        } else if (strcmp(objective, "fidelity") == 0) {
            b->objective_match = clamp_f64_v_(0.5 * b->attention_mass + 0.5 * b->state_relevance, 0.0, 1.0);
        } else {
            b->objective_match = clamp_f64_v_(0.34 * b->state_relevance + 0.33 * b->continuity_risk + 0.33 * b->witness_relevance, 0.0, 1.0);
        }

        b->selection_score =
            (w_attention  * b->attention_mass)     +
            (w_state      * b->state_relevance)    +
            (w_witness    * b->witness_relevance)  +
            (w_continuity * b->continuity_risk)    +
            (w_layer      * b->layer_need)         +
            (w_recency    * b->recency)            +
            (w_objective  * b->objective_match)    -
            (w_mem_cost   * b->memory_cost)        -
            (w_residual   * b->residual_error);

        b->policy = VPOL_EVICT;
        ranked[i].idx   = i;
        ranked[i].score = b->selection_score;
    }

    /* Rank + allocate */
    qsort(ranked, n_blocks, sizeof(VRankRow), vrank_cmp_desc_);

    uint32_t rem_hot = bgt_hot, rem_mem = bgt_mem, rem_cmp = bgt_cmp;
    for (uint32_t r = 0; r < n_blocks; r++) {
        VBlock *b = &blocks[ranked[r].idx];
        if (rem_hot > 0u) { b->policy = VPOL_HOT_EXACT;       rem_hot--; continue; }
        if (rem_mem > 0u) { b->policy = VPOL_MEMBRANE_SELECT; rem_mem--; continue; }
        if (rem_cmp > 0u) { b->policy = VPOL_COMPRESS;        rem_cmp--; continue; }
        b->policy = VPOL_EVICT;
    }
    /* guardrail: promote EVICT→MEMBRANE_SELECT for critical blocks */
    for (uint32_t i = 0; i < n_blocks; i++) {
        if ((blocks[i].is_fail || blocks[i].witness_critical) && blocks[i].policy == VPOL_EVICT) {
            blocks[i].policy = VPOL_MEMBRANE_SELECT;
        }
    }

    /* ── Compute verification signals ───────────────────────────────────── */

    /* 1. Witness coverage */
    uint32_t witness_retained = 0;
    for (uint32_t i = 0; i < n_blocks; i++) {
        if (blocks[i].witness_critical &&
            (blocks[i].policy == VPOL_HOT_EXACT || blocks[i].policy == VPOL_MEMBRANE_SELECT)) {
            witness_retained++;
        }
    }
    const int witness_pass = (witness_retained >= req_witnesses);

    /* 2. CONTINUITY_FAIL eviction check */
    uint32_t fail_total = 0, fail_evicted = 0;
    for (uint32_t i = 0; i < n_blocks; i++) {
        if (blocks[i].is_fail) {
            fail_total++;
            if (blocks[i].policy == VPOL_EVICT) fail_evicted++;
        }
    }
    const int fail_evict_pass = (fail_evicted == 0);

    /* 3. Residual bound check over selected blocks */
    double max_selected_residual = 0.0;
    uint32_t selected_over_threshold = 0;
    for (uint32_t i = 0; i < n_blocks; i++) {
        if (blocks[i].policy == VPOL_HOT_EXACT || blocks[i].policy == VPOL_MEMBRANE_SELECT) {
            if (blocks[i].residual_norm > max_selected_residual)
                max_selected_residual = blocks[i].residual_norm;
            if (blocks[i].residual_norm > drift_thresh)
                selected_over_threshold++;
        }
    }
    const char *residual_verdict =
        (max_selected_residual <= drift_thresh) ? "PASS" :
        (max_selected_residual <= drift_thresh * 1.5) ? "PASS_WITH_DRIFT" : "FAIL";

    /* 4. Budget compliance */
    uint32_t cnt_hot = 0, cnt_mem = 0, cnt_cmp = 0, cnt_evi = 0;
    for (uint32_t i = 0; i < n_blocks; i++) {
        switch (blocks[i].policy) {
            case VPOL_HOT_EXACT:       cnt_hot++; break;
            case VPOL_MEMBRANE_SELECT: cnt_mem++; break;
            case VPOL_COMPRESS:        cnt_cmp++; break;
            default:                   cnt_evi++; break;
        }
    }
    const double exact_mb_per   = ((double)block_tokens * (double)heads * 2.0 * 128.0) / (1024.0 * 1024.0);
    const double compr_mb_per   = exact_mb_per * 0.28;
    const double est_exact_mb   = (double)(cnt_hot + cnt_mem) * exact_mb_per;
    const double est_compr_mb   = (double)cnt_cmp * compr_mb_per;
    const double est_total_mb   = est_exact_mb + est_compr_mb;
    const int budget_pass       = (kv_budget_mb <= 0.0 || est_total_mb <= kv_budget_mb);

    /* 5. Overall verdict */
    const int overall_fail = (!witness_pass || !fail_evict_pass || !budget_pass ||
                              strcmp(residual_verdict, "FAIL") == 0);
    const int overall_drift = (!overall_fail &&
                               (strcmp(residual_verdict, "PASS_WITH_DRIFT") == 0 ||
                                selected_over_threshold > 0));
    const char *overall = overall_fail ? "FAIL" : (overall_drift ? "PASS_WITH_DRIFT" : "PASS");

    free(blocks);
    free(ranked);

    /* ── Build JSON ──────────────────────────────────────────────────────── */
    size_t cap = 4096u;
    char *json = (char *)malloc(cap);
    if (!json) return -1;

    int n = snprintf(
        json, cap,
        "{\n"
        "  \"schema_version\": \"akai.context.verify.v1\",\n"
        "  \"mode\": \"%s\",\n"
        "  \"objective_profile\": \"%s\",\n"
        "  \"considered_blocks\": %u,\n"
        "  \"checks\": {\n"
        "    \"witness_coverage\": {\n"
        "      \"required\": %u,\n"
        "      \"retained\": %u,\n"
        "      \"verdict\": \"%s\"\n"
        "    },\n"
        "    \"continuity_fail_evicted\": {\n"
        "      \"fail_blocks_total\": %u,\n"
        "      \"fail_blocks_evicted\": %u,\n"
        "      \"verdict\": \"%s\"\n"
        "    },\n"
        "    \"residual_bound\": {\n"
        "      \"threshold\": %.6f,\n"
        "      \"max_selected_residual\": %.6f,\n"
        "      \"selected_blocks_over_threshold\": %u,\n"
        "      \"verdict\": \"%s\"\n"
        "    },\n"
        "    \"budget_compliance\": {\n"
        "      \"kv_budget_mb\": %.6f,\n"
        "      \"estimated_exact_kv_mb\": %.6f,\n"
        "      \"estimated_compressed_kv_mb\": %.6f,\n"
        "      \"estimated_total_kv_mb\": %.6f,\n"
        "      \"verdict\": \"%s\"\n"
        "    }\n"
        "  },\n"
        "  \"policy_counts\": {\n"
        "    \"HOT_EXACT\": %u,\n"
        "    \"MEMBRANE_SELECT\": %u,\n"
        "    \"COMPRESS\": %u,\n"
        "    \"EVICT\": %u\n"
        "  },\n"
        "  \"overall_verdict\": \"%s\"\n"
        "}\n",
        mode,
        objective,
        n_blocks,
        req_witnesses, witness_retained, witness_pass ? "PASS" : "FAIL",
        fail_total, fail_evicted, fail_evict_pass ? "PASS" : "FAIL",
        drift_thresh, max_selected_residual, selected_over_threshold, residual_verdict,
        kv_budget_mb, est_exact_mb, est_compr_mb, est_total_mb, budget_pass ? "PASS" : "FAIL",
        cnt_hot, cnt_mem, cnt_cmp, cnt_evi,
        overall
    );

    if (n <= 0 || (size_t)n >= cap) { free(json); return -1; }

    *out_json = json;
    return 0;
}
