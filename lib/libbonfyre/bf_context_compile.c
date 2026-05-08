#define _POSIX_C_SOURCE 200809L
#include "include/bonfyre.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint32_t clamp_u32_c_(uint32_t v, uint32_t lo, uint32_t hi) {
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

static double clamp_f64_c_(double v, double lo, double hi) {
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

static const char *safe_mode_c_(const char *mode) {
    if (!mode || !mode[0]) return "hybrid";
    if (strcmp(mode, "dense") == 0) return "dense";
    if (strcmp(mode, "compressed") == 0) return "compressed";
    if (strcmp(mode, "membrane") == 0) return "membrane";
    if (strcmp(mode, "state") == 0) return "state";
    if (strcmp(mode, "hybrid") == 0) return "hybrid";
    return "hybrid";
}

void bf_context_compile_defaults(BfContextCompileConfig *cfg) {
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->mode = "hybrid";
    cfg->heads = 16;
    cfg->seq_tokens = 131072;
    cfg->block_tokens = 128;
    cfg->token_index_blocks = 1024;
    cfg->kv_index_blocks = 1024;
    cfg->state_index_atoms = 256;
    cfg->witness_index_anchors = 128;
    cfg->token_budget_blocks = 256;
    cfg->kv_budget_blocks = 128;
    cfg->state_atom_budget = 128;
    cfg->required_witnesses = 12;
    cfg->kv_budget_mb = 512.0;
    cfg->latency_budget_ms = 120.0;
    cfg->residual_drift_threshold = 0.35;
    cfg->base_residual = 0.18;
    cfg->continuity_fail_rate = 0.05;
    cfg->objective_profile = "balanced";
    cfg->fail_on_missing_witness = 1;
}

int bf_context_compile_json(const BfContextCompileConfig *cfg, char **out_json) {
    if (!cfg || !out_json) return -1;

    const char *mode = safe_mode_c_(cfg->mode);
    const char *objective = cfg->objective_profile ? cfg->objective_profile : "balanced";

    const uint32_t heads = clamp_u32_c_(cfg->heads, 1, 256);
    const uint32_t seq_tokens = clamp_u32_c_(cfg->seq_tokens, 256, 1u << 30);
    const uint32_t block_tokens = clamp_u32_c_(cfg->block_tokens, 16, 8192);

    const uint32_t token_index_blocks = clamp_u32_c_(cfg->token_index_blocks, 1, 2000000u);
    const uint32_t kv_index_blocks = clamp_u32_c_(cfg->kv_index_blocks, 1, 2000000u);
    const uint32_t state_index_atoms = clamp_u32_c_(cfg->state_index_atoms, 1, 2000000u);
    const uint32_t witness_index_anchors = clamp_u32_c_(cfg->witness_index_anchors, 1, 2000000u);

    const uint32_t token_budget_blocks = clamp_u32_c_(cfg->token_budget_blocks, 1, 2000000u);
    const uint32_t kv_budget_blocks = clamp_u32_c_(cfg->kv_budget_blocks, 1, 2000000u);
    const uint32_t state_atom_budget = clamp_u32_c_(cfg->state_atom_budget, 1, 2000000u);
    const uint32_t required_witnesses = cfg->required_witnesses;

    const double kv_budget_mb = clamp_f64_c_(cfg->kv_budget_mb, 0.0, 1e9);
    const double latency_budget_ms = clamp_f64_c_(cfg->latency_budget_ms, 0.0, 1e9);
    const double residual_threshold = clamp_f64_c_(cfg->residual_drift_threshold, 0.0, 100.0);
    const double base_residual = clamp_f64_c_(cfg->base_residual, 0.0, 100.0);
    const double fail_rate = clamp_f64_c_(cfg->continuity_fail_rate, 0.0, 1.0);

    const uint32_t token_blocks_from_seq = (seq_tokens + block_tokens - 1u) / block_tokens;
    const uint32_t token_index_total = token_index_blocks < token_blocks_from_seq ? token_index_blocks : token_blocks_from_seq;

    uint32_t selected_tokens = token_budget_blocks < token_index_total ? token_budget_blocks : token_index_total;
    uint32_t selected_kv = kv_budget_blocks < kv_index_blocks ? kv_budget_blocks : kv_index_blocks;
    uint32_t selected_state = state_atom_budget < state_index_atoms ? state_atom_budget : state_index_atoms;

    uint32_t selected_witness = required_witnesses;
    if (selected_witness > witness_index_anchors) selected_witness = witness_index_anchors;

    const double exact_kv_mb_per_block = ((double)block_tokens * (double)heads * 2.0 * 128.0) / (1024.0 * 1024.0);
    const double compressed_kv_mb_per_block = exact_kv_mb_per_block * 0.28;

    double estimated_exact_kv_mb = (double)selected_kv * exact_kv_mb_per_block;
    double estimated_compressed_kv_mb = 0.0;

    if (strcmp(mode, "compressed") == 0) {
        estimated_compressed_kv_mb = (double)selected_kv * compressed_kv_mb_per_block;
        estimated_exact_kv_mb = 0.0;
    } else if (strcmp(mode, "hybrid") == 0 || strcmp(mode, "membrane") == 0) {
        estimated_compressed_kv_mb = (double)(selected_kv / 2u) * compressed_kv_mb_per_block;
    }

    const double estimated_total_kv_mb = estimated_exact_kv_mb + estimated_compressed_kv_mb;

    const double token_latency_ms = (double)selected_tokens * 0.015;
    const double kv_latency_ms = (double)selected_kv * 0.035;
    const double state_latency_ms = (double)selected_state * 0.010;
    const double witness_latency_ms = (double)selected_witness * 0.006;
    const double estimated_latency_ms = token_latency_ms + kv_latency_ms + state_latency_ms + witness_latency_ms;

    const uint32_t fail_events = (uint32_t)floor((double)kv_index_blocks * fail_rate);
    uint32_t captured_fail_events = selected_kv / 8u + selected_state / 16u;
    if (captured_fail_events > fail_events) captured_fail_events = fail_events;

    const int witness_ok = (selected_witness >= required_witnesses);
    const int budget_ok = (kv_budget_mb <= 0.0 || estimated_total_kv_mb <= kv_budget_mb);
    const int latency_ok = (latency_budget_ms <= 0.0 || estimated_latency_ms <= latency_budget_ms);

    double residual_delta_estimate = base_residual;
    if (fail_events > captured_fail_events) {
        const uint32_t uncovered = fail_events - captured_fail_events;
        residual_delta_estimate += 0.02 + (double)uncovered * 0.001;
    }
    if (!witness_ok) residual_delta_estimate += 0.08;
    if (!budget_ok) residual_delta_estimate += 0.06;
    residual_delta_estimate = clamp_f64_c_(residual_delta_estimate, 0.0, 4.0);

    const char *continuity = "PASS";
    if ((!witness_ok && cfg->fail_on_missing_witness) || residual_delta_estimate > residual_threshold * 1.5) {
        continuity = "FAIL";
    } else if (!witness_ok || residual_delta_estimate > residual_threshold) {
        continuity = "PASS_WITH_DRIFT";
    }

    size_t cap = 8192;
    char *json = (char *)malloc(cap);
    if (!json) return -1;

    int n = snprintf(
        json,
        cap,
        "{\n"
        "  \"schema_version\": \"akai.context.compile.v1\",\n"
        "  \"mode\": \"%s\",\n"
        "  \"objective_profile\": \"%s\",\n"
        "  \"indexes\": {\n"
        "    \"I_token\": {\"blocks\": %u},\n"
        "    \"I_kv\": {\"blocks\": %u},\n"
        "    \"I_state\": {\"atoms\": %u},\n"
        "    \"I_witness\": {\"anchors\": %u}\n"
        "  },\n"
        "  \"selection\": {\n"
        "    \"selected_token_blocks\": %u,\n"
        "    \"selected_kv_blocks\": %u,\n"
        "    \"selected_state_atoms\": %u,\n"
        "    \"selected_witness_anchors\": %u,\n"
        "    \"required_witnesses\": %u\n"
        "  },\n"
        "  \"constraints\": {\n"
        "    \"kv_budget_mb\": %.6f,\n"
        "    \"latency_budget_ms\": %.6f,\n"
        "    \"residual_drift_threshold\": %.6f,\n"
        "    \"witness_required\": true,\n"
        "    \"fail_on_missing_witness\": %s\n"
        "  },\n"
        "  \"estimates\": {\n"
        "    \"estimated_exact_kv_mb\": %.6f,\n"
        "    \"estimated_compressed_kv_mb\": %.6f,\n"
        "    \"estimated_total_kv_mb\": %.6f,\n"
        "    \"estimated_latency_ms\": %.6f,\n"
        "    \"residual_delta_estimate\": %.6f\n"
        "  },\n"
        "  \"witness_check\": {\n"
        "    \"verdict\": \"%s\"\n"
        "  },\n"
        "  \"budget_check\": {\n"
        "    \"kv_verdict\": \"%s\",\n"
        "    \"latency_verdict\": \"%s\"\n"
        "  },\n"
        "  \"continuity_fail_events\": {\n"
        "    \"total\": %u,\n"
        "    \"captured\": %u\n"
        "  },\n"
        "  \"continuity_verdict\": \"%s\"\n"
        "}\n",
        mode,
        objective,
        token_index_total,
        kv_index_blocks,
        state_index_atoms,
        witness_index_anchors,
        selected_tokens,
        selected_kv,
        selected_state,
        selected_witness,
        required_witnesses,
        kv_budget_mb,
        latency_budget_ms,
        residual_threshold,
        cfg->fail_on_missing_witness ? "true" : "false",
        estimated_exact_kv_mb,
        estimated_compressed_kv_mb,
        estimated_total_kv_mb,
        estimated_latency_ms,
        residual_delta_estimate,
        witness_ok ? "PASS" : "FAIL",
        budget_ok ? "PASS" : "FAIL",
        latency_ok ? "PASS" : "FAIL",
        fail_events,
        captured_fail_events,
        continuity
    );

    if (n <= 0 || (size_t)n >= cap) {
        free(json);
        return -1;
    }

    *out_json = json;
    return 0;
}
