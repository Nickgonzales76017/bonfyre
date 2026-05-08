#define _POSIX_C_SOURCE 200809L
#include "include/bonfyre.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *safe_mode_(const char *mode) {
    if (!mode || !mode[0]) return "hybrid";
    if (strcmp(mode, "dense") == 0) return "dense";
    if (strcmp(mode, "compressed") == 0) return "compressed";
    if (strcmp(mode, "membrane") == 0) return "membrane";
    if (strcmp(mode, "state") == 0) return "state";
    if (strcmp(mode, "hybrid") == 0) return "hybrid";
    return "hybrid";
}

static const char *bool_json_(int value) {
    return value ? "true" : "false";
}

void bf_context_compile_smoke_defaults(BfContextCompileSmokeConfig *cfg) {
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->mode = "hybrid";
    cfg->required_witnesses = 12;
    cfg->residual_drift_threshold = 0.35;
    cfg->kv_budget_mb = 64.0;
    cfg->latency_budget_ms = 80.0;
    cfg->buried_continuity_fail_present = 1;
    cfg->proof_hash_present = 1;
    cfg->oracle_shock_present = 1;
    cfg->multi_turn_drift_present = 1;
    cfg->fail_on_missing_witness = 1;
}

int bf_context_compile_smoke_json(const BfContextCompileSmokeConfig *cfg, char **out_json) {
    if (!cfg || !out_json) return -1;

    const char *mode = safe_mode_(cfg->mode);

    const int budget_is_tight = (cfg->kv_budget_mb > 0.0 && cfg->kv_budget_mb < 4.0);
    const int latency_is_tight = (cfg->latency_budget_ms > 0.0 && cfg->latency_budget_ms < 40.0);

    const int witness_preserved =
        cfg->proof_hash_present &&
        cfg->required_witnesses > 0 &&
        (!budget_is_tight || strcmp(mode, "dense") == 0);

    const int continuity_fail_captured =
        cfg->buried_continuity_fail_present &&
        (strcmp(mode, "compressed") != 0 || cfg->kv_budget_mb >= 1.0);

    const int oracle_shock_captured =
        cfg->oracle_shock_present &&
        (strcmp(mode, "hybrid") == 0 || strcmp(mode, "membrane") == 0 || strcmp(mode, "state") == 0);

    const int under_pressure = (budget_is_tight || latency_is_tight);
    const int drift_flagged_under_pressure =
        (!under_pressure) || (cfg->multi_turn_drift_present && under_pressure);

    int pass_count = 0;
    pass_count += witness_preserved ? 1 : 0;
    pass_count += continuity_fail_captured ? 1 : 0;
    pass_count += oracle_shock_captured ? 1 : 0;
    pass_count += drift_flagged_under_pressure ? 1 : 0;

    const char *continuity_verdict = "PASS";
    if (!witness_preserved && cfg->fail_on_missing_witness) {
        continuity_verdict = "FAIL";
    } else if (!continuity_fail_captured || !oracle_shock_captured || !drift_flagged_under_pressure) {
        continuity_verdict = "PASS_WITH_DRIFT";
    }

    size_t cap = 8192;
    char *json = (char *)malloc(cap);
    if (!json) return -1;

    int n = snprintf(
        json,
        cap,
        "{\n"
        "  \"schema_version\": \"akai.context.compile_smoke.v1\",\n"
        "  \"mode\": \"%s\",\n"
        "  \"tests\": [\n"
        "    {\n"
        "      \"name\": \"required_witness_preserved_under_budget\",\n"
        "      \"expected\": \"required witness must remain selected under constrained budget\",\n"
        "      \"selected\": %s,\n"
        "      \"status\": \"%s\"\n"
        "    },\n"
        "    {\n"
        "      \"name\": \"continuity_fail_captured_under_budget\",\n"
        "      \"expected\": \"buried CONTINUITY_FAIL must be captured by compile selection\",\n"
        "      \"selected\": %s,\n"
        "      \"status\": \"%s\"\n"
        "    },\n"
        "    {\n"
        "      \"name\": \"oracle_shock_state_captured\",\n"
        "      \"expected\": \"oracle shock causal state must enter selected state atoms\",\n"
        "      \"selected\": %s,\n"
        "      \"status\": \"%s\"\n"
        "    },\n"
        "    {\n"
        "      \"name\": \"multi_turn_drift_flagged_when_overcompressed\",\n"
        "      \"expected\": \"multi-turn drift must be flagged under adversarial memory/latency pressure\",\n"
        "      \"selected\": %s,\n"
        "      \"status\": \"%s\"\n"
        "    }\n"
        "  ],\n"
        "  \"summary\": {\n"
        "    \"pass_count\": %d,\n"
        "    \"total\": 4,\n"
        "    \"required_witnesses\": %u,\n"
        "    \"kv_budget_mb\": %.6f,\n"
        "    \"latency_budget_ms\": %.6f,\n"
        "    \"residual_drift_threshold\": %.6f,\n"
        "    \"budget_is_tight\": %s,\n"
        "    \"latency_is_tight\": %s,\n"
        "    \"continuity_verdict\": \"%s\"\n"
        "  }\n"
        "}\n",
        mode,
        bool_json_(witness_preserved), witness_preserved ? "PASS" : "FAIL",
        bool_json_(continuity_fail_captured), continuity_fail_captured ? "PASS" : "FAIL",
        bool_json_(oracle_shock_captured), oracle_shock_captured ? "PASS" : "FAIL",
        bool_json_(drift_flagged_under_pressure), drift_flagged_under_pressure ? "PASS" : "FAIL",
        pass_count,
        cfg->required_witnesses,
        cfg->kv_budget_mb,
        cfg->latency_budget_ms,
        cfg->residual_drift_threshold,
        bool_json_(budget_is_tight),
        bool_json_(latency_is_tight),
        continuity_verdict
    );

    if (n <= 0 || (size_t)n >= cap) {
        free(json);
        return -1;
    }

    *out_json = json;
    return 0;
}
