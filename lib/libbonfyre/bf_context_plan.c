#define _POSIX_C_SOURCE 200809L
#include "include/bonfyre.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint32_t clamp_u32_(uint32_t value, uint32_t min_v, uint32_t max_v) {
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

void bf_context_plan_defaults(BfContextPlanConfig *cfg) {
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->mode = "hybrid";
    cfg->dense_window_tokens = 8192;
    cfg->sq_token_budget_blocks = 256;
    cfg->sq_kv_budget_mb = 512;
    cfg->compressed_kv_budget_mb = 256;
    cfg->state_atom_budget = 128;
    cfg->required_witnesses = 12;
    cfg->witness_required = 1;
    cfg->fail_on_missing_witness = 1;
    cfg->residual_drift_threshold = 0.35;
    cfg->residual_delta_estimate = 0.18;
    cfg->continuity_fail_events = 0;
    cfg->continuity_drift_events = 0;
}

int bf_context_plan_summary(const BfContextPlanConfig *cfg, BfContextPlanSummary *out) {
    if (!cfg || !out) return -1;

    const char *mode = safe_mode_(cfg->mode);
    memset(out, 0, sizeof(*out));
    snprintf(out->mode, sizeof(out->mode), "%s", mode);

    out->dense_window = clamp_u32_(cfg->dense_window_tokens, 512, 262144);
    out->required_witnesses = cfg->required_witnesses;
    out->residual_delta_estimate = cfg->residual_delta_estimate;

    if (strcmp(mode, "dense") == 0) {
        out->selected_token_blocks = clamp_u32_(cfg->sq_token_budget_blocks / 8, 0, 4096);
        out->selected_state_atoms = clamp_u32_(cfg->state_atom_budget / 8, 0, 4096);
        out->compressed_memory_atoms = clamp_u32_(cfg->compressed_kv_budget_mb / 8, 0, 8192);
        out->estimated_kv_mb = clamp_u32_(cfg->sq_kv_budget_mb + cfg->compressed_kv_budget_mb, 32, 16384);
    } else if (strcmp(mode, "compressed") == 0) {
        out->selected_token_blocks = clamp_u32_(cfg->sq_token_budget_blocks / 16, 0, 4096);
        out->selected_state_atoms = clamp_u32_(cfg->state_atom_budget / 6, 0, 4096);
        out->compressed_memory_atoms = clamp_u32_(cfg->compressed_kv_budget_mb, 16, 8192);
        out->estimated_kv_mb = clamp_u32_(cfg->compressed_kv_budget_mb, 16, 16384);
    } else if (strcmp(mode, "membrane") == 0) {
        out->selected_token_blocks = clamp_u32_(cfg->sq_token_budget_blocks, 8, 4096);
        out->selected_state_atoms = clamp_u32_(cfg->state_atom_budget / 3, 0, 4096);
        out->compressed_memory_atoms = clamp_u32_(cfg->compressed_kv_budget_mb / 2, 0, 8192);
        out->estimated_kv_mb = clamp_u32_(cfg->sq_kv_budget_mb + cfg->compressed_kv_budget_mb / 2, 32, 16384);
    } else if (strcmp(mode, "state") == 0) {
        out->selected_token_blocks = clamp_u32_(cfg->sq_token_budget_blocks / 5, 0, 4096);
        out->selected_state_atoms = clamp_u32_(cfg->state_atom_budget, 8, 4096);
        out->compressed_memory_atoms = clamp_u32_(cfg->compressed_kv_budget_mb / 3, 0, 8192);
        out->estimated_kv_mb = clamp_u32_(cfg->sq_kv_budget_mb / 2 + cfg->compressed_kv_budget_mb / 3, 32, 16384);
    } else {
        out->selected_token_blocks = clamp_u32_(cfg->sq_token_budget_blocks / 2, 8, 4096);
        out->selected_state_atoms = clamp_u32_(cfg->state_atom_budget / 2, 4, 4096);
        out->compressed_memory_atoms = clamp_u32_(cfg->compressed_kv_budget_mb / 2, 8, 8192);
        out->estimated_kv_mb = clamp_u32_(cfg->sq_kv_budget_mb + cfg->compressed_kv_budget_mb / 2, 32, 16384);
    }

    if (cfg->witness_required && cfg->required_witnesses == 0) {
        snprintf(out->continuity_verdict, sizeof(out->continuity_verdict), "%s", "FAIL");
    } else if (cfg->continuity_fail_events > 0 && cfg->fail_on_missing_witness) {
        snprintf(out->continuity_verdict, sizeof(out->continuity_verdict), "%s", "FAIL");
    } else if (cfg->continuity_fail_events > 0 ||
               cfg->continuity_drift_events > 0 ||
               cfg->residual_delta_estimate > cfg->residual_drift_threshold) {
        snprintf(out->continuity_verdict, sizeof(out->continuity_verdict), "%s", "PASS_WITH_DRIFT");
    } else {
        snprintf(out->continuity_verdict, sizeof(out->continuity_verdict), "%s", "PASS");
    }

    return 0;
}

int bf_context_plan_json(const BfContextPlanConfig *cfg, char **out_json) {
    if (!cfg || !out_json) return -1;

    BfContextPlanSummary summary;
    if (bf_context_plan_summary(cfg, &summary) != 0) return -1;

    size_t cap = 4096;
    char *json = (char *)malloc(cap);
    if (!json) return -1;

    int n = snprintf(
        json,
        cap,
        "{\n"
        "  \"schema_version\": \"akai.context.plan.v1\",\n"
        "  \"mode\": \"%s\",\n"
        "  \"dense_window\": %u,\n"
        "  \"selected_token_blocks\": %u,\n"
        "  \"selected_state_atoms\": %u,\n"
        "  \"compressed_memory_atoms\": %u,\n"
        "  \"required_witnesses\": %u,\n"
        "  \"estimated_kv_mb\": %u,\n"
        "  \"residual_delta_estimate\": %.6f,\n"
        "  \"continuity_verdict\": \"%s\",\n"
        "  \"policy\": {\n"
        "    \"witness_required\": %s,\n"
        "    \"fail_on_missing_witness\": %s,\n"
        "    \"residual_drift_threshold\": %.6f\n"
        "  }\n"
        "}\n",
        summary.mode,
        summary.dense_window,
        summary.selected_token_blocks,
        summary.selected_state_atoms,
        summary.compressed_memory_atoms,
        summary.required_witnesses,
        summary.estimated_kv_mb,
        summary.residual_delta_estimate,
        summary.continuity_verdict,
        cfg->witness_required ? "true" : "false",
        cfg->fail_on_missing_witness ? "true" : "false",
        cfg->residual_drift_threshold
    );

    if (n <= 0 || (size_t)n >= cap) {
        free(json);
        return -1;
    }

    *out_json = json;
    return 0;
}
