#define _POSIX_C_SOURCE 200809L
#include "include/bonfyre.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint32_t clamp_u32_(uint32_t value, uint32_t min_v, uint32_t max_v) {
    if (value < min_v) return min_v;
    if (value > max_v) return max_v;
    return value;
}

static double clamp_f64_(double value, double min_v, double max_v) {
    if (value < min_v) return min_v;
    if (value > max_v) return max_v;
    return value;
}

static const char *safe_mode_(const char *mode) {
    if (!mode || !mode[0]) return "hybrid";
    if (strcmp(mode, "dense") == 0) return "dense";
    if (strcmp(mode, "compressed") == 0) return "compressed";
    if (strcmp(mode, "membrane") == 0) return "membrane";
    if (strcmp(mode, "state") == 0) return "state";
    if (strcmp(mode, "hybrid") == 0) return "hybrid";
    return "hybrid";
}

typedef enum {
    POLICY_HOT_EXACT = 0,
    POLICY_MEMBRANE_SELECT = 1,
    POLICY_COMPRESS = 2,
    POLICY_EVICT = 3,
} BfPolicy;

typedef struct {
    uint32_t layer;
    uint32_t head;
    uint32_t token_start;
    uint32_t token_end;
    double attention_mass;
    double residual_norm;
    int e8[8];
    char witness_hex[65];
    const char *continuity;
    int is_fail;
    int witness_critical;
    double attention_predictor;
    double state_relevance;
    double witness_relevance;
    double continuity_risk;
    double layer_need;
    double recency;
    double objective_match;
    double memory_cost;
    double residual_error;
    double selection_score;
    uint32_t selection_rank;
    BfPolicy policy;
} BfBlockEval;

typedef struct {
    uint32_t idx;
    double score;
} BfRankRow;

static const char *policy_name_(BfPolicy policy) {
    switch (policy) {
        case POLICY_HOT_EXACT: return "HOT_EXACT";
        case POLICY_MEMBRANE_SELECT: return "MEMBRANE_SELECT";
        case POLICY_COMPRESS: return "COMPRESS";
        case POLICY_EVICT: return "EVICT";
        default: return "COMPRESS";
    }
}

static int rank_row_cmp_desc_(const void *a, const void *b) {
    const BfRankRow *ra = (const BfRankRow *)a;
    const BfRankRow *rb = (const BfRankRow *)b;
    if (ra->score < rb->score) return 1;
    if (ra->score > rb->score) return -1;
    if (ra->idx > rb->idx) return 1;
    if (ra->idx < rb->idx) return -1;
    return 0;
}

static void default_budgets_for_mode_(
    const char *mode,
    uint32_t considered_blocks,
    uint32_t *hot,
    uint32_t *membrane,
    uint32_t *compress,
    uint32_t *evict
) {
    if (strcmp(mode, "dense") == 0) {
        *hot = considered_blocks;
        *membrane = 0;
        *compress = 0;
        *evict = 0;
        return;
    }

    if (strcmp(mode, "compressed") == 0) {
        *hot = 0;
        *membrane = 0;
        *compress = considered_blocks;
        *evict = 0;
        return;
    }

    if (strcmp(mode, "membrane") == 0) {
        *hot = considered_blocks / 8u;
        *membrane = (considered_blocks * 5u) / 8u;
        *compress = considered_blocks / 8u;
        *evict = considered_blocks - (*hot + *membrane + *compress);
        return;
    }

    if (strcmp(mode, "state") == 0) {
        *hot = considered_blocks / 10u;
        *membrane = (considered_blocks * 4u) / 10u;
        *compress = (considered_blocks * 4u) / 10u;
        *evict = considered_blocks - (*hot + *membrane + *compress);
        return;
    }

    *hot = considered_blocks / 5u;
    *membrane = (considered_blocks * 9u) / 20u;
    *compress = (considered_blocks * 3u) / 10u;
    *evict = considered_blocks - (*hot + *membrane + *compress);
}

static void resolve_budgets_(
    const char *mode,
    uint32_t considered_blocks,
    const BfContextKVRegistryConfig *cfg,
    uint32_t *hot,
    uint32_t *membrane,
    uint32_t *compress,
    uint32_t *evict
) {
    *hot = cfg->hot_exact_budget_blocks;
    *membrane = cfg->membrane_budget_blocks;
    *compress = cfg->compress_budget_blocks;
    *evict = cfg->evict_budget_blocks;

    if ((*hot + *membrane + *compress + *evict) == 0u) {
        default_budgets_for_mode_(mode, considered_blocks, hot, membrane, compress, evict);
    }

    if (*hot > considered_blocks) *hot = considered_blocks;
    if (*membrane > considered_blocks) *membrane = considered_blocks;
    if (*compress > considered_blocks) *compress = considered_blocks;
    if (*evict > considered_blocks) *evict = considered_blocks;

    uint32_t remaining = considered_blocks;
    if (*hot > remaining) *hot = remaining;
    remaining -= *hot;

    if (*membrane > remaining) *membrane = remaining;
    remaining -= *membrane;

    if (*compress > remaining) *compress = remaining;
    remaining -= *compress;

    if (*evict > remaining) *evict = remaining;
    remaining -= *evict;

    *compress += remaining;
}

void bf_context_kv_registry_defaults(BfContextKVRegistryConfig *cfg) {
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
}

int bf_context_kv_registry_json(const BfContextKVRegistryConfig *cfg, char **out_json) {
    if (!cfg || !out_json) return -1;

    const char *mode = safe_mode_(cfg->mode);
    const uint32_t layers = clamp_u32_(cfg->layers, 1, 256);
    const uint32_t heads = clamp_u32_(cfg->heads, 1, 256);
    const uint32_t seq_tokens = clamp_u32_(cfg->seq_tokens, 256, 1u << 30);
    const uint32_t block_tokens = clamp_u32_(cfg->block_tokens, 16, 8192);
    const uint32_t top_blocks = clamp_u32_(cfg->top_blocks, 1, 8192);
    const uint32_t required_witnesses = cfg->required_witnesses;
    const double base_residual = clamp_f64_(cfg->base_residual, 0.0, 10.0);
    const double fail_rate = clamp_f64_(cfg->continuity_fail_rate, 0.0, 1.0);
    const char *objective = cfg->objective_profile ? cfg->objective_profile : "balanced";
    const double w_attention = clamp_f64_(cfg->w_attention_predictor, 0.0, 100.0);
    const double w_state = clamp_f64_(cfg->w_state_relevance, 0.0, 100.0);
    const double w_witness = clamp_f64_(cfg->w_witness_relevance, 0.0, 100.0);
    const double w_continuity = clamp_f64_(cfg->w_continuity_risk, 0.0, 100.0);
    const double w_layer = clamp_f64_(cfg->w_layer_need, 0.0, 100.0);
    const double w_recency = clamp_f64_(cfg->w_recency, 0.0, 100.0);
    const double w_objective = clamp_f64_(cfg->w_objective_match, 0.0, 100.0);
    const double w_mem_cost = clamp_f64_(cfg->w_memory_cost, 0.0, 100.0);
    const double w_residual = clamp_f64_(cfg->w_residual_error, 0.0, 100.0);

    const uint32_t total_blocks = (seq_tokens + block_tokens - 1u) / block_tokens;
    uint32_t considered_blocks = top_blocks;
    if (considered_blocks > total_blocks) considered_blocks = total_blocks;

    uint32_t budget_hot = 0;
    uint32_t budget_membrane = 0;
    uint32_t budget_compress = 0;
    uint32_t budget_evict = 0;
    resolve_budgets_(
        mode,
        considered_blocks,
        cfg,
        &budget_hot,
        &budget_membrane,
        &budget_compress,
        &budget_evict
    );

    size_t cap = (size_t)considered_blocks * 760u + 8192u;
    char *json = (char *)malloc(cap);
    if (!json) return -1;

    BfBlockEval *blocks = (BfBlockEval *)calloc(considered_blocks, sizeof(BfBlockEval));
    BfRankRow *ranked = (BfRankRow *)calloc(considered_blocks, sizeof(BfRankRow));
    if (!blocks || !ranked) {
        free(blocks);
        free(ranked);
        free(json);
        return -1;
    }

    size_t off = 0;
    int n = snprintf(
        json + off,
        cap - off,
        "{\n"
        "  \"schema_version\": \"akai.context.kv_registry.v1\",\n"
        "  \"mode\": \"%s\",\n"
        "  \"selection_basis\": \"score_ranked\",\n"
        "  \"objective_profile\": \"%s\",\n"
        "  \"layers\": %u,\n"
        "  \"heads\": %u,\n"
        "  \"seq_tokens\": %u,\n"
        "  \"block_tokens\": %u,\n"
        "  \"considered_blocks\": %u,\n"
        "  \"required_witnesses\": %u,\n"
        "  \"selection_weights\": {\n"
        "    \"attention_predictor\": %.6f,\n"
        "    \"state_relevance\": %.6f,\n"
        "    \"witness_relevance\": %.6f,\n"
        "    \"continuity_risk\": %.6f,\n"
        "    \"layer_need\": %.6f,\n"
        "    \"recency\": %.6f,\n"
        "    \"objective_match\": %.6f,\n"
        "    \"memory_cost\": %.6f,\n"
        "    \"residual_error\": %.6f\n"
        "  },\n"
        "  \"blocks\": [\n",
        mode,
        objective,
        layers,
        heads,
        seq_tokens,
        block_tokens,
        considered_blocks,
        required_witnesses,
        w_attention,
        w_state,
        w_witness,
        w_continuity,
        w_layer,
        w_recency,
        w_objective,
        w_mem_cost,
        w_residual
    );
    if (n <= 0 || (size_t)n >= cap - off) {
        free(blocks);
        free(ranked);
        free(json);
        return -1;
    }
    off += (size_t)n;

    uint32_t fail_stride = 0;
    if (fail_rate > 0.0) {
        fail_stride = (uint32_t)floor(1.0 / fail_rate);
        if (fail_stride == 0) fail_stride = 1;
    }

    for (uint32_t i = 0; i < considered_blocks; i++) {
        BfBlockEval *b = &blocks[i];

        b->layer = i % layers;
        b->head = (i / layers) % heads;
        b->token_start = i * block_tokens;
        b->token_end = b->token_start + block_tokens - 1u;
        if (b->token_end >= seq_tokens) b->token_end = seq_tokens - 1u;

        b->attention_mass = 1.0 / (double)(i + 2u);
        if (b->attention_mass < 0.0005) b->attention_mass = 0.0005;

        b->residual_norm = base_residual + ((double)(i % 17u) * 0.007);

        b->e8[0] = (int)((b->layer + b->head + i) % 9u) - 4;
        b->e8[1] = (int)((b->layer + 2u * i) % 9u) - 4;
        b->e8[2] = (int)((b->head + 3u * i) % 9u) - 4;
        b->e8[3] = (int)((b->layer + b->head + 5u * i) % 9u) - 4;
        b->e8[4] = (int)((2u * b->layer + i) % 9u) - 4;
        b->e8[5] = (int)((2u * b->head + i) % 9u) - 4;
        b->e8[6] = (int)((b->layer + 7u * i) % 9u) - 4;
        b->e8[7] = (int)((b->head + 11u * i) % 9u) - 4;

        char witness_seed[256];
        snprintf(
            witness_seed,
            sizeof(witness_seed),
            "%s|%u|%u|%u|%u|%u|%u",
            mode,
            b->layer,
            b->head,
            b->token_start,
            b->token_end,
            required_witnesses,
            i
        );
        bf_sha256_hex((const uint8_t *)witness_seed, strlen(witness_seed), b->witness_hex);

        b->is_fail = (fail_stride > 0u && (i % fail_stride) == 0u) ? 1 : 0;
        b->witness_critical = (required_witnesses > 0u && i < required_witnesses) ? 1 : 0;
        b->continuity = b->is_fail ? "CONTINUITY_FAIL" : ((b->residual_norm > base_residual + 0.06) ? "PASS_WITH_DRIFT" : "PASS");

        b->attention_predictor = b->attention_mass;
        b->state_relevance = clamp_f64_(1.0 - ((double)i / (double)(considered_blocks + 1u)), 0.0, 1.0);
        b->witness_relevance = b->witness_critical ? 1.0 : 0.15;
        b->continuity_risk = b->is_fail ? 1.0 : ((strcmp(b->continuity, "PASS_WITH_DRIFT") == 0) ? 0.5 : 0.1);
        b->layer_need = clamp_f64_(1.0 - ((double)b->layer / (double)layers), 0.0, 1.0);
        b->recency = clamp_f64_(1.0 - ((double)b->token_start / (double)(seq_tokens + 1u)), 0.0, 1.0);
        if (strcmp(objective, "latency") == 0) {
            b->objective_match = clamp_f64_(0.7 * b->recency + 0.3 * (1.0 / (1.0 + b->memory_cost)), 0.0, 1.0);
        } else if (strcmp(objective, "continuity") == 0) {
            b->objective_match = clamp_f64_(0.7 * b->continuity_risk + 0.3 * b->witness_relevance, 0.0, 1.0);
        } else if (strcmp(objective, "fidelity") == 0) {
            b->objective_match = clamp_f64_(0.5 * b->attention_predictor + 0.5 * b->state_relevance, 0.0, 1.0);
        } else {
            b->objective_match = clamp_f64_(0.34 * b->state_relevance + 0.33 * b->continuity_risk + 0.33 * b->witness_relevance, 0.0, 1.0);
        }
        b->memory_cost = (double)block_tokens / 1024.0;
        b->residual_error = clamp_f64_(b->residual_norm / (base_residual + 0.2), 0.0, 4.0);
        b->selection_score =
            (w_attention * b->attention_predictor) +
            (w_state * b->state_relevance) +
            (w_witness * b->witness_relevance) +
            (w_continuity * b->continuity_risk) +
            (w_layer * b->layer_need) +
            (w_recency * b->recency) +
            (w_objective * b->objective_match) -
            (w_mem_cost * b->memory_cost) -
            (w_residual * b->residual_error);

        b->policy = POLICY_EVICT;
        b->selection_rank = considered_blocks;

        ranked[i].idx = i;
        ranked[i].score = b->selection_score;
    }

    qsort(ranked, considered_blocks, sizeof(BfRankRow), rank_row_cmp_desc_);

    for (uint32_t r = 0; r < considered_blocks; r++) {
        blocks[ranked[r].idx].selection_rank = r;
    }

    uint32_t remaining_hot = budget_hot;
    uint32_t remaining_membrane = budget_membrane;
    uint32_t remaining_compress = budget_compress;

    for (uint32_t r = 0; r < considered_blocks; r++) {
        BfBlockEval *b = &blocks[ranked[r].idx];
        if (remaining_hot > 0u) {
            b->policy = POLICY_HOT_EXACT;
            remaining_hot--;
            continue;
        }
        if (remaining_membrane > 0u) {
            b->policy = POLICY_MEMBRANE_SELECT;
            remaining_membrane--;
            continue;
        }
        if (remaining_compress > 0u) {
            b->policy = POLICY_COMPRESS;
            remaining_compress--;
            continue;
        }
        b->policy = POLICY_EVICT;
    }

    for (uint32_t i = 0; i < considered_blocks; i++) {
        BfBlockEval *b = &blocks[i];
        if ((b->is_fail || b->witness_critical) && b->policy == POLICY_EVICT) {
            b->policy = POLICY_MEMBRANE_SELECT;
        }
    }

    uint32_t count_hot = 0;
    uint32_t count_membrane = 0;
    uint32_t count_compress = 0;
    uint32_t count_evict = 0;

    for (uint32_t i = 0; i < considered_blocks; i++) {
        const BfBlockEval *b = &blocks[i];
        if (b->policy == POLICY_HOT_EXACT) count_hot++;
        else if (b->policy == POLICY_MEMBRANE_SELECT) count_membrane++;
        else if (b->policy == POLICY_COMPRESS) count_compress++;
        else count_evict++;

        n = snprintf(
            json + off,
            cap - off,
            "    {\n"
            "      \"layer\": %u,\n"
            "      \"head\": %u,\n"
            "      \"token_start\": %u,\n"
            "      \"token_end\": %u,\n"
            "      \"attention_mass\": %.6f,\n"
            "      \"_e8\": {\n"
            "        \"cell_key\": \"%d,%d,%d,%d,%d,%d,%d,%d\",\n"
            "        \"residual_norm\": %.6f\n"
            "      },\n"
            "      \"witness_hash\": \"sha256:%s\",\n"
            "      \"continuity\": \"%s\",\n"
            "      \"selection_rank\": %u,\n"
            "      \"score_components\": {\n"
            "        \"attention_predictor\": %.6f,\n"
            "        \"state_relevance\": %.6f,\n"
            "        \"witness_relevance\": %.6f,\n"
            "        \"continuity_risk\": %.6f,\n"
            "        \"layer_need\": %.6f,\n"
            "        \"recency\": %.6f,\n"
            "        \"objective_match\": %.6f,\n"
            "        \"memory_cost\": %.6f,\n"
            "        \"residual_error\": %.6f\n"
            "      },\n"
            "      \"selection_score\": %.6f,\n"
            "      \"policy\": \"%s\"\n"
            "    }%s\n",
            b->layer,
            b->head,
            b->token_start,
            b->token_end,
            b->attention_mass,
            b->e8[0], b->e8[1], b->e8[2], b->e8[3], b->e8[4], b->e8[5], b->e8[6], b->e8[7],
            b->residual_norm,
            b->witness_hex,
            b->continuity,
            b->selection_rank,
            b->attention_predictor,
            b->state_relevance,
            b->witness_relevance,
            b->continuity_risk,
            b->layer_need,
            b->recency,
            b->objective_match,
            b->memory_cost,
            b->residual_error,
            b->selection_score,
            policy_name_(b->policy),
            (i + 1u < considered_blocks) ? "," : ""
        );
        if (n <= 0 || (size_t)n >= cap - off) {
            free(blocks);
            free(ranked);
            free(json);
            return -1;
        }
        off += (size_t)n;
    }

    const uint32_t selected_blocks = count_hot + count_membrane + count_compress;
    const double exact_kv_mb_per_block = ((double)block_tokens * (double)heads * 2.0 * 128.0) / (1024.0 * 1024.0);
    const double compressed_kv_mb_per_block = exact_kv_mb_per_block * 0.28;
    const double estimated_exact_kv_mb = (double)(count_hot + count_membrane) * exact_kv_mb_per_block;
    const double estimated_compressed_kv_mb = (double)count_compress * compressed_kv_mb_per_block;

    n = snprintf(
        json + off,
        cap - off,
        "  ],\n"
        "  \"selected_blocks\": %u,\n"
        "  \"policy_budget\": {\n"
        "    \"hot_exact_budget_blocks\": %u,\n"
        "    \"membrane_budget_blocks\": %u,\n"
        "    \"compress_budget_blocks\": %u,\n"
        "    \"evict_budget_blocks\": %u\n"
        "  },\n"
        "  \"policy_counts\": {\n"
        "    \"HOT_EXACT\": %u,\n"
        "    \"MEMBRANE_SELECT\": %u,\n"
        "    \"COMPRESS\": %u,\n"
        "    \"EVICT\": %u\n"
        "  },\n"
        "  \"budget_accounting\": {\n"
        "    \"estimated_exact_kv_mb\": %.6f,\n"
        "    \"estimated_compressed_kv_mb\": %.6f,\n"
        "    \"estimated_total_kv_mb\": %.6f\n"
        "  }\n"
        "}\n",
        selected_blocks,
        budget_hot,
        budget_membrane,
        budget_compress,
        budget_evict,
        count_hot,
        count_membrane,
        count_compress,
        count_evict,
        estimated_exact_kv_mb,
        estimated_compressed_kv_mb,
        estimated_exact_kv_mb + estimated_compressed_kv_mb
    );
    if (n <= 0 || (size_t)n >= cap - off) {
        free(blocks);
        free(ranked);
        free(json);
        return -1;
    }

    free(blocks);
    free(ranked);

    *out_json = json;
    return 0;
}
