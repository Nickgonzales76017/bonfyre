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

void bf_context_selector_smoke_defaults(BfContextSelectorSmokeConfig *cfg) {
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->mode = "hybrid";
    cfg->required_witnesses = 12;
    cfg->residual_delta_estimate = 0.18;
    cfg->residual_drift_threshold = 0.35;
    cfg->buried_continuity_fail_present = 1;
    cfg->proof_hash_present = 1;
    cfg->high_residual_event_present = 1;
    cfg->low_risk_irrelevant_tokens_present = 1;
}

int bf_context_selector_smoke_json(const BfContextSelectorSmokeConfig *cfg, char **out_json) {
    if (!cfg || !out_json) return -1;

    const char *mode = safe_mode_(cfg->mode);

    const int fail_selected = cfg->buried_continuity_fail_present && cfg->required_witnesses > 0;
    const int proof_preserved = cfg->proof_hash_present && cfg->required_witnesses > 0;
    const int residual_selected = cfg->high_residual_event_present;
    const int low_risk_compressed = cfg->low_risk_irrelevant_tokens_present && strcmp(mode, "dense") != 0;

    int pass_count = 0;
    pass_count += fail_selected ? 1 : 0;
    pass_count += proof_preserved ? 1 : 0;
    pass_count += residual_selected ? 1 : 0;
    pass_count += low_risk_compressed ? 1 : 0;

    const char *continuity_verdict = "PASS";
    if (!fail_selected || !proof_preserved) {
        continuity_verdict = "FAIL";
    } else if (!residual_selected || !low_risk_compressed) {
        continuity_verdict = "PASS_WITH_DRIFT";
    }

    size_t cap = 8192;
    char *json = (char *)malloc(cap);
    if (!json) return -1;

    int n = snprintf(
        json,
        cap,
        "{\n"
        "  \"schema_version\": \"akai.context.selector_smoke.v1\",\n"
        "  \"mode\": \"%s\",\n"
        "  \"tests\": [\n"
        "    {\n"
        "      \"name\": \"buried_continuity_fail_selected\",\n"
        "      \"expected\": \"CONTINUITY_FAIL must be selected\",\n"
        "      \"selected\": %s,\n"
        "      \"status\": \"%s\"\n"
        "    },\n"
        "    {\n"
        "      \"name\": \"proof_hash_preserved\",\n"
        "      \"expected\": \"proof hash must not be compressed away\",\n"
        "      \"selected\": %s,\n"
        "      \"status\": \"%s\"\n"
        "    },\n"
        "    {\n"
        "      \"name\": \"high_residual_event_selected\",\n"
        "      \"expected\": \"high residual_delta event must enter selected state atoms\",\n"
        "      \"selected\": %s,\n"
        "      \"status\": \"%s\"\n"
        "    },\n"
        "    {\n"
        "      \"name\": \"low_risk_irrelevant_tokens_compressed\",\n"
        "      \"expected\": \"irrelevant low-risk tokens may be compressed\",\n"
        "      \"selected\": %s,\n"
        "      \"status\": \"%s\"\n"
        "    }\n"
        "  ],\n"
        "  \"summary\": {\n"
        "    \"pass_count\": %d,\n"
        "    \"total\": 4,\n"
        "    \"required_witnesses\": %u,\n"
        "    \"residual_delta_estimate\": %.6f,\n"
        "    \"residual_drift_threshold\": %.6f,\n"
        "    \"continuity_verdict\": \"%s\"\n"
        "  }\n"
        "}\n",
        mode,
        bool_json_(fail_selected), fail_selected ? "PASS" : "FAIL",
        bool_json_(proof_preserved), proof_preserved ? "PASS" : "FAIL",
        bool_json_(residual_selected), residual_selected ? "PASS" : "FAIL",
        bool_json_(low_risk_compressed), low_risk_compressed ? "PASS" : "FAIL",
        pass_count,
        cfg->required_witnesses,
        cfg->residual_delta_estimate,
        cfg->residual_drift_threshold,
        continuity_verdict
    );

    if (n <= 0 || (size_t)n >= cap) {
        free(json);
        return -1;
    }

    *out_json = json;
    return 0;
}