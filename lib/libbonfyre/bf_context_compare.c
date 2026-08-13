#define _POSIX_C_SOURCE 200809L
#include "include/bonfyre.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── internal helpers (mirrors bf_context_registry.c) ──────────────────── */

static uint32_t clamp_u32_cmp_(uint32_t value, uint32_t min_v, uint32_t max_v) {
    if (value < min_v) return min_v;
    if (value > max_v) return max_v;
    return value;
}

static double clamp_f64_cmp_(double value, double min_v, double max_v) {
    if (value < min_v) return min_v;
    if (value > max_v) return max_v;
    return value;
}

static const char *safe_mode_cmp_(const char *mode) {
    if (!mode || !mode[0]) return "hybrid";
    if (strcmp(mode, "dense") == 0)      return "dense";
    if (strcmp(mode, "compressed") == 0) return "compressed";
    if (strcmp(mode, "membrane") == 0)   return "membrane";
    if (strcmp(mode, "state") == 0)      return "state";
    if (strcmp(mode, "hybrid") == 0)     return "hybrid";
    return "hybrid";
}

typedef enum {
    CPOL_HOT_EXACT       = 0,
    CPOL_MEMBRANE_SELECT = 1,
    CPOL_COMPRESS        = 2,
    CPOL_EVICT           = 3,
} CmpPolicy;

static const char *cpol_name_(CmpPolicy p) {
    switch (p) {
        case CPOL_HOT_EXACT:       return "HOT_EXACT";
        case CPOL_MEMBRANE_SELECT: return "MEMBRANE_SELECT";
        case CPOL_COMPRESS:        return "COMPRESS";
        case CPOL_EVICT:           return "EVICT";
        default:                   return "COMPRESS";
    }
}

typedef struct {
    uint32_t idx;
    double   score;
} CmpRankRow;

static int rank_row_cmp_desc_cmp_(const void *a, const void *b) {
    const CmpRankRow *ra = (const CmpRankRow *)a;
    const CmpRankRow *rb = (const CmpRankRow *)b;
    if (ra->score < rb->score) return  1;
    if (ra->score > rb->score) return -1;
    if (ra->idx   > rb->idx)   return  1;
    if (ra->idx   < rb->idx)   return -1;
    return 0;
}

static void default_budgets_cmp_(
    const char *mode,
    uint32_t    n,
    uint32_t   *hot,
    uint32_t   *membrane,
    uint32_t   *compress,
    uint32_t   *evict
) {
    if (strcmp(mode, "dense") == 0) {
        *hot = n; *membrane = 0; *compress = 0; *evict = 0; return;
    }
    if (strcmp(mode, "compressed") == 0) {
        *hot = 0; *membrane = 0; *compress = n; *evict = 0; return;
    }
    if (strcmp(mode, "membrane") == 0) {
        *hot = n / 8u;
        *membrane = (n * 5u) / 8u;
        *compress = n / 8u;
        *evict = n - (*hot + *membrane + *compress);
        return;
    }
    if (strcmp(mode, "state") == 0) {
        *hot = n / 10u;
        *membrane = (n * 4u) / 10u;
        *compress = (n * 4u) / 10u;
        *evict = n - (*hot + *membrane + *compress);
        return;
    }
    /* hybrid */
    *hot = n / 5u;
    *membrane = (n * 9u) / 20u;
    *compress = (n * 3u) / 10u;
    *evict = n - (*hot + *membrane + *compress);
}

/* Shared per-block structural data (constant across all objective profiles) */
typedef struct {
    uint32_t layer;
    uint32_t head;
    uint32_t token_start;
    uint32_t token_end;
    double   attention_mass;
    double   residual_norm;
    int      e8[8];
    char     witness_hex[65];
    const char *continuity;
    int      is_fail;
    int      witness_critical;
    /* invariant score components */
    double   attention_predictor;
    double   state_relevance;
    double   witness_relevance;
    double   continuity_risk;
    double   layer_need;
    double   recency;
    double   memory_cost;
    double   residual_error;
} CmpBlock;

/* Per-profile result for one block */
typedef struct {
    double      objective_match;
    double      selection_score;
    uint32_t    selection_rank;
    CmpPolicy   policy;
} CmpBlockResult;

/* Compute objective_match for a block under a given profile */
static double objective_match_for_(
    const CmpBlock *b,
    const char     *objective
) {
    if (strcmp(objective, "latency") == 0) {
        return clamp_f64_cmp_(0.7 * b->recency + 0.3 * (1.0 / (1.0 + b->memory_cost)), 0.0, 1.0);
    } else if (strcmp(objective, "continuity") == 0) {
        return clamp_f64_cmp_(0.7 * b->continuity_risk + 0.3 * b->witness_relevance, 0.0, 1.0);
    } else if (strcmp(objective, "fidelity") == 0) {
        return clamp_f64_cmp_(0.5 * b->attention_predictor + 0.5 * b->state_relevance, 0.0, 1.0);
    } else {
        /* balanced */
        return clamp_f64_cmp_(0.34 * b->state_relevance + 0.33 * b->continuity_risk + 0.33 * b->witness_relevance, 0.0, 1.0);
    }
}

/* Run one objective profile pass: fills results[0..n_blocks-1].
 * budgets are passed in (pre-resolved from mode). */
static void run_profile_pass_(
    const CmpBlock     *blocks,
    uint32_t            n_blocks,
    const char         *objective,
    const BfContextKVCompareConfig *cfg,
    uint32_t            budget_hot,
    uint32_t            budget_membrane,
    uint32_t            budget_compress,
    CmpBlockResult     *results
) {
    const double w_attention  = clamp_f64_cmp_(cfg->w_attention_predictor, 0.0, 100.0);
    const double w_state      = clamp_f64_cmp_(cfg->w_state_relevance,     0.0, 100.0);
    const double w_witness    = clamp_f64_cmp_(cfg->w_witness_relevance,   0.0, 100.0);
    const double w_continuity = clamp_f64_cmp_(cfg->w_continuity_risk,     0.0, 100.0);
    const double w_layer      = clamp_f64_cmp_(cfg->w_layer_need,          0.0, 100.0);
    const double w_recency    = clamp_f64_cmp_(cfg->w_recency,             0.0, 100.0);
    const double w_objective  = clamp_f64_cmp_(cfg->w_objective_match,     0.0, 100.0);
    const double w_mem_cost   = clamp_f64_cmp_(cfg->w_memory_cost,         0.0, 100.0);
    const double w_residual   = clamp_f64_cmp_(cfg->w_residual_error,      0.0, 100.0);

    CmpRankRow *ranked = (CmpRankRow *)calloc(n_blocks, sizeof(CmpRankRow));
    if (!ranked) return;

    for (uint32_t i = 0; i < n_blocks; i++) {
        const CmpBlock *b = &blocks[i];
        double obj = objective_match_for_(b, objective);
        double score =
            (w_attention  * b->attention_predictor) +
            (w_state      * b->state_relevance)     +
            (w_witness    * b->witness_relevance)   +
            (w_continuity * b->continuity_risk)     +
            (w_layer      * b->layer_need)          +
            (w_recency    * b->recency)             +
            (w_objective  * obj)                    -
            (w_mem_cost   * b->memory_cost)         -
            (w_residual   * b->residual_error);
        results[i].objective_match = obj;
        results[i].selection_score = score;
        results[i].policy = CPOL_EVICT;
        results[i].selection_rank = n_blocks;
        ranked[i].idx   = i;
        ranked[i].score = score;
    }

    qsort(ranked, n_blocks, sizeof(CmpRankRow), rank_row_cmp_desc_cmp_);
    for (uint32_t r = 0; r < n_blocks; r++) {
        results[ranked[r].idx].selection_rank = r;
    }

    uint32_t rem_hot      = budget_hot;
    uint32_t rem_membrane = budget_membrane;
    uint32_t rem_compress = budget_compress;

    for (uint32_t r = 0; r < n_blocks; r++) {
        CmpBlockResult *res = &results[ranked[r].idx];
        if (rem_hot > 0u)      { res->policy = CPOL_HOT_EXACT;       rem_hot--;      continue; }
        if (rem_membrane > 0u) { res->policy = CPOL_MEMBRANE_SELECT; rem_membrane--; continue; }
        if (rem_compress > 0u) { res->policy = CPOL_COMPRESS;        rem_compress--; continue; }
        res->policy = CPOL_EVICT;
    }

    /* guardrail: promote EVICT→MEMBRANE_SELECT for fail/witness-critical blocks */
    for (uint32_t i = 0; i < n_blocks; i++) {
        const CmpBlock *b = &blocks[i];
        if ((b->is_fail || b->witness_critical) && results[i].policy == CPOL_EVICT) {
            results[i].policy = CPOL_MEMBRANE_SELECT;
        }
    }

    free(ranked);
}

/* ── defaults + main entry ─────────────────────────────────────────────── */

void bf_context_kv_compare_defaults(BfContextKVCompareConfig *cfg) {
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

static const char *COMPARE_PROFILES[] = { "balanced", "latency", "continuity", "fidelity" };
#define NUM_PROFILES 4

int bf_context_kv_compare_json(const BfContextKVCompareConfig *cfg, char **out_json) {
    if (!cfg || !out_json) return -1;

    const char *mode       = safe_mode_cmp_(cfg->mode);
    const uint32_t layers  = clamp_u32_cmp_(cfg->layers,       1, 256);
    const uint32_t heads   = clamp_u32_cmp_(cfg->heads,        1, 256);
    const uint32_t seq_tokens   = clamp_u32_cmp_(cfg->seq_tokens,   256, 1u << 30);
    const uint32_t block_tokens = clamp_u32_cmp_(cfg->block_tokens, 16, 8192);
    const uint32_t top_blocks   = clamp_u32_cmp_(cfg->top_blocks,   1, 8192);
    const uint32_t required_witnesses = cfg->required_witnesses;
    const double base_residual = clamp_f64_cmp_(cfg->base_residual, 0.0, 10.0);
    const double fail_rate     = clamp_f64_cmp_(cfg->continuity_fail_rate, 0.0, 1.0);

    const uint32_t total_blocks = (seq_tokens + block_tokens - 1u) / block_tokens;
    uint32_t n_blocks = top_blocks;
    if (n_blocks > total_blocks) n_blocks = total_blocks;

    uint32_t budget_hot = 0, budget_membrane = 0, budget_compress = 0, budget_evict = 0;
    budget_hot      = cfg->hot_exact_budget_blocks;
    budget_membrane = cfg->membrane_budget_blocks;
    budget_compress = cfg->compress_budget_blocks;
    budget_evict    = cfg->evict_budget_blocks;

    if ((budget_hot + budget_membrane + budget_compress + budget_evict) == 0u) {
        default_budgets_cmp_(mode, n_blocks, &budget_hot, &budget_membrane, &budget_compress, &budget_evict);
    }
    if (budget_hot      > n_blocks) budget_hot      = n_blocks;
    if (budget_membrane > n_blocks) budget_membrane = n_blocks;
    if (budget_compress > n_blocks) budget_compress = n_blocks;
    if (budget_evict    > n_blocks) budget_evict    = n_blocks;
    {
        uint32_t rem = n_blocks;
        if (budget_hot      > rem) budget_hot      = rem; rem -= budget_hot;
        if (budget_membrane > rem) budget_membrane = rem; rem -= budget_membrane;
        if (budget_compress > rem) budget_compress = rem; rem -= budget_compress;
        if (budget_evict    > rem) budget_evict    = rem; rem -= budget_evict;
        budget_compress += rem;
    }

    /* Allocate block and result arrays */
    CmpBlock *blocks = (CmpBlock *)calloc(n_blocks, sizeof(CmpBlock));
    /* results[p][i] = result for profile p, block i */
    CmpBlockResult *all_results = (CmpBlockResult *)calloc(
        (size_t)NUM_PROFILES * (size_t)n_blocks, sizeof(CmpBlockResult)
    );
    if (!blocks || !all_results) {
        free(blocks);
        free(all_results);
        return -1;
    }

    /* Build shared block data (structural + invariant score components) */
    uint32_t fail_stride = 0;
    if (fail_rate > 0.0) {
        fail_stride = (uint32_t)floor(1.0 / fail_rate);
        if (fail_stride == 0) fail_stride = 1;
    }

    for (uint32_t i = 0; i < n_blocks; i++) {
        CmpBlock *b = &blocks[i];
        b->layer       = i % layers;
        b->head        = (i / layers) % heads;
        b->token_start = i * block_tokens;
        b->token_end   = b->token_start + block_tokens - 1u;
        if (b->token_end >= seq_tokens) b->token_end = seq_tokens - 1u;

        b->attention_mass = 1.0 / (double)(i + 2u);
        if (b->attention_mass < 0.0005) b->attention_mass = 0.0005;
        b->residual_norm = base_residual + ((double)(i % 17u) * 0.007);

        b->e8[0] = (int)((b->layer + b->head + i)         % 9u) - 4;
        b->e8[1] = (int)((b->layer + 2u * i)              % 9u) - 4;
        b->e8[2] = (int)((b->head  + 3u * i)              % 9u) - 4;
        b->e8[3] = (int)((b->layer + b->head + 5u * i)    % 9u) - 4;
        b->e8[4] = (int)((2u * b->layer + i)              % 9u) - 4;
        b->e8[5] = (int)((2u * b->head  + i)              % 9u) - 4;
        b->e8[6] = (int)((b->layer + 7u * i)              % 9u) - 4;
        b->e8[7] = (int)((b->head  + 11u * i)             % 9u) - 4;

        char witness_seed[256];
        snprintf(witness_seed, sizeof(witness_seed),
            "%s|%u|%u|%u|%u|%u|%u",
            mode, b->layer, b->head, b->token_start, b->token_end,
            required_witnesses, i);
        bf_sha256_hex((const uint8_t *)witness_seed, strlen(witness_seed), b->witness_hex);

        b->is_fail        = (fail_stride > 0u && (i % fail_stride) == 0u) ? 1 : 0;
        b->witness_critical = (required_witnesses > 0u && i < required_witnesses) ? 1 : 0;
        b->continuity     = b->is_fail ? "CONTINUITY_FAIL"
                          : ((b->residual_norm > base_residual + 0.06) ? "PASS_WITH_DRIFT" : "PASS");

        b->attention_predictor = b->attention_mass;
        b->state_relevance   = clamp_f64_cmp_(1.0 - ((double)i / (double)(n_blocks + 1u)), 0.0, 1.0);
        b->witness_relevance = b->witness_critical ? 1.0 : 0.15;
        b->continuity_risk   = b->is_fail ? 1.0 : ((strcmp(b->continuity, "PASS_WITH_DRIFT") == 0) ? 0.5 : 0.1);
        b->layer_need        = clamp_f64_cmp_(1.0 - ((double)b->layer / (double)layers), 0.0, 1.0);
        b->recency           = clamp_f64_cmp_(1.0 - ((double)b->token_start / (double)(seq_tokens + 1u)), 0.0, 1.0);
        b->memory_cost       = (double)block_tokens / 1024.0;
        b->residual_error    = clamp_f64_cmp_(b->residual_norm / (base_residual + 0.2), 0.0, 4.0);
    }

    /* Run each profile pass */
    for (int p = 0; p < NUM_PROFILES; p++) {
        CmpBlockResult *res = all_results + (size_t)p * (size_t)n_blocks;
        run_profile_pass_(blocks, n_blocks, COMPARE_PROFILES[p], cfg,
                          budget_hot, budget_membrane, budget_compress, res);
    }

    /* Compute per-profile summaries */
    uint32_t pol_counts[NUM_PROFILES][4];
    uint32_t witness_retained[NUM_PROFILES];
    memset(pol_counts, 0, sizeof(pol_counts));
    memset(witness_retained, 0, sizeof(witness_retained));

    const double exact_kv_mb_per  = ((double)block_tokens * (double)heads * 2.0 * 128.0) / (1024.0 * 1024.0);
    const double compr_kv_mb_per  = exact_kv_mb_per * 0.28;

    for (int p = 0; p < NUM_PROFILES; p++) {
        const CmpBlockResult *res = all_results + (size_t)p * (size_t)n_blocks;
        for (uint32_t i = 0; i < n_blocks; i++) {
            pol_counts[p][(int)res[i].policy]++;
            if (blocks[i].witness_critical &&
                (res[i].policy == CPOL_HOT_EXACT || res[i].policy == CPOL_MEMBRANE_SELECT)) {
                witness_retained[p]++;
            }
        }
    }

    /* Compute delta shifts vs baseline (profile 0 = balanced) */
    /* policy_changes[profile][from][to] */
    uint32_t policy_changes[NUM_PROFILES][4][4];
    memset(policy_changes, 0, sizeof(policy_changes));

    for (int p = 1; p < NUM_PROFILES; p++) {
        const CmpBlockResult *base = all_results;
        const CmpBlockResult *cmp  = all_results + (size_t)p * (size_t)n_blocks;
        for (uint32_t i = 0; i < n_blocks; i++) {
            int from = (int)base[i].policy;
            int to   = (int)cmp[i].policy;
            if (from != to) {
                policy_changes[p][from][to]++;
            }
        }
    }

    /* Find top-10 blocks with maximum profile disagreement */
    typedef struct { uint32_t idx; uint32_t disagree_count; } DisagreeRow;
    DisagreeRow *disagree = (DisagreeRow *)calloc(n_blocks, sizeof(DisagreeRow));
    if (!disagree) { free(blocks); free(all_results); return -1; }

    for (uint32_t i = 0; i < n_blocks; i++) {
        disagree[i].idx = i;
        /* count number of unique policies across profiles */
        int seen[4] = {0, 0, 0, 0};
        uint32_t unique = 0;
        for (int p = 0; p < NUM_PROFILES; p++) {
            int pol = (int)all_results[(size_t)p * n_blocks + i].policy;
            if (!seen[pol]) { seen[pol] = 1; unique++; }
        }
        disagree[i].disagree_count = unique;
    }
    /* partial sort: bubble top 10 to front (n_blocks is at most 8192 so O(n) is fine) */
    #define TOP_SHIFTS 10
    uint32_t top_n = n_blocks < TOP_SHIFTS ? n_blocks : TOP_SHIFTS;
    for (uint32_t t = 0; t < top_n; t++) {
        uint32_t best = t;
        for (uint32_t j = t + 1; j < n_blocks; j++) {
            if (disagree[j].disagree_count > disagree[best].disagree_count ||
                (disagree[j].disagree_count == disagree[best].disagree_count &&
                 disagree[j].idx < disagree[best].idx)) {
                best = j;
            }
        }
        if (best != t) {
            DisagreeRow tmp = disagree[t];
            disagree[t]     = disagree[best];
            disagree[best]  = tmp;
        }
    }

    /* ── Build JSON ──────────────────────────────────────────────────────── */
    size_t cap = 16384u + (size_t)n_blocks * 48u;
    char *json = (char *)malloc(cap);
    if (!json) { free(blocks); free(all_results); free(disagree); return -1; }

    size_t off = 0;
    int n;

#define JEMIT(fmt, ...) \
    do { \
        n = snprintf(json + off, cap - off, fmt, ##__VA_ARGS__); \
        if (n <= 0 || (size_t)n >= cap - off) goto oom; \
        off += (size_t)n; \
    } while (0)

    JEMIT(
        "{\n"
        "  \"schema_version\": \"akai.context.kv_compare.v1\",\n"
        "  \"mode\": \"%s\",\n"
        "  \"layers\": %u,\n"
        "  \"heads\": %u,\n"
        "  \"seq_tokens\": %u,\n"
        "  \"block_tokens\": %u,\n"
        "  \"considered_blocks\": %u,\n"
        "  \"required_witnesses\": %u,\n"
        "  \"profiles_compared\": [\"balanced\", \"latency\", \"continuity\", \"fidelity\"],\n"
        "  \"profiles\": [\n",
        mode, layers, heads, seq_tokens, block_tokens, n_blocks, required_witnesses
    );

    for (int p = 0; p < NUM_PROFILES; p++) {
        const uint32_t n_hot      = pol_counts[p][CPOL_HOT_EXACT];
        const uint32_t n_membrane = pol_counts[p][CPOL_MEMBRANE_SELECT];
        const uint32_t n_compress = pol_counts[p][CPOL_COMPRESS];
        const uint32_t n_evict    = pol_counts[p][CPOL_EVICT];
        const uint32_t selected   = n_hot + n_membrane + n_compress;
        const double   est_total  = (double)(n_hot + n_membrane) * exact_kv_mb_per
                                  + (double)n_compress * compr_kv_mb_per;
        JEMIT(
            "    {\n"
            "      \"objective\": \"%s\",\n"
            "      \"policy_counts\": {\n"
            "        \"HOT_EXACT\": %u,\n"
            "        \"MEMBRANE_SELECT\": %u,\n"
            "        \"COMPRESS\": %u,\n"
            "        \"EVICT\": %u\n"
            "      },\n"
            "      \"witness_retained\": %u,\n"
            "      \"selected_blocks\": %u,\n"
            "      \"estimated_total_kv_mb\": %.6f\n"
            "    }%s\n",
            COMPARE_PROFILES[p],
            n_hot, n_membrane, n_compress, n_evict,
            witness_retained[p], selected, est_total,
            (p + 1 < NUM_PROFILES) ? "," : ""
        );
    }

    JEMIT("  ],\n  \"delta\": {\n    \"baseline\": \"balanced\",\n    \"shifts_vs_baseline\": [\n");

    for (int p = 1; p < NUM_PROFILES; p++) {
        const CmpBlockResult *base_res = all_results;
        const CmpBlockResult *cmp_res  = all_results + (size_t)p * (size_t)n_blocks;
        uint32_t changed = 0;
        for (uint32_t i = 0; i < n_blocks; i++) {
            if (base_res[i].policy != cmp_res[i].policy) changed++;
        }
        int32_t wr_delta = (int32_t)witness_retained[p] - (int32_t)witness_retained[0];

        JEMIT(
            "      {\n"
            "        \"objective\": \"%s\",\n"
            "        \"blocks_changed\": %u,\n"
            "        \"witness_retention_delta\": %d,\n"
            "        \"policy_changes\": {\n",
            COMPARE_PROFILES[p], changed, wr_delta
        );

        const char *pol_names[4] = { "HOT_EXACT", "MEMBRANE_SELECT", "COMPRESS", "EVICT" };
        int first_change = 1;
        for (int from = 0; from < 4; from++) {
            for (int to = 0; to < 4; to++) {
                if (from == to) continue;
                uint32_t cnt = policy_changes[p][from][to];
                if (cnt == 0) continue;
                JEMIT(
                    "          %s\"%s_to_%s\": %u",
                    first_change ? "" : ",\n",
                    pol_names[from], pol_names[to], cnt
                );
                first_change = 0;
            }
        }
        if (!first_change) JEMIT("\n");
        JEMIT("        }\n      }%s\n", (p + 1 < NUM_PROFILES) ? "," : "");
    }

    JEMIT("    ],\n    \"top_shifts\": [\n");

    for (uint32_t t = 0; t < top_n; t++) {
        uint32_t bi = disagree[t].idx;
        const CmpBlock *b = &blocks[bi];
        JEMIT(
            "      {\n"
            "        \"block_idx\": %u,\n"
            "        \"layer\": %u,\n"
            "        \"token_start\": %u,\n"
            "        \"witness_critical\": %s,\n"
            "        \"is_fail\": %s,\n"
            "        \"disagree_profiles\": %u,\n"
            "        \"policies\": {\n"
            "          \"balanced\": \"%s\",\n"
            "          \"latency\": \"%s\",\n"
            "          \"continuity\": \"%s\",\n"
            "          \"fidelity\": \"%s\"\n"
            "        }\n"
            "      }%s\n",
            bi, b->layer, b->token_start,
            b->witness_critical ? "true" : "false",
            b->is_fail          ? "true" : "false",
            disagree[t].disagree_count,
            cpol_name_(all_results[0 * n_blocks + bi].policy),
            cpol_name_(all_results[1 * n_blocks + bi].policy),
            cpol_name_(all_results[2 * n_blocks + bi].policy),
            cpol_name_(all_results[3 * n_blocks + bi].policy),
            (t + 1u < top_n) ? "," : ""
        );
    }

    JEMIT("    ]\n  }\n}\n");

    free(blocks);
    free(all_results);
    free(disagree);
    *out_json = json;
    return 0;

oom:
    free(blocks);
    free(all_results);
    free(disagree);
    free(json);
    return -1;

#undef JEMIT
#undef TOP_SHIFTS
}
