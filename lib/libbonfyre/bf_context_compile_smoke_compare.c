#define _POSIX_C_SOURCE 200809L
#include "include/bonfyre.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    int witness_preserved;
    int continuity_fail_captured;
    int oracle_shock_captured;
    int drift_flagged_under_pressure;
    int pass_count;
    const char *continuity_verdict;
} SmokeEval;

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

static int verdict_rank_(const char *v) {
    if (strcmp(v, "FAIL") == 0) return 0;
    if (strcmp(v, "PASS_WITH_DRIFT") == 0) return 1;
    return 2;
}

static void eval_smoke_(const BfContextCompileSmokeConfig *cfg, SmokeEval *out) {
    const char *mode = safe_mode_(cfg->mode);

    const int budget_is_tight = (cfg->kv_budget_mb > 0.0 && cfg->kv_budget_mb < 4.0);
    const int latency_is_tight = (cfg->latency_budget_ms > 0.0 && cfg->latency_budget_ms < 40.0);

    out->witness_preserved =
        cfg->proof_hash_present &&
        cfg->required_witnesses > 0 &&
        (!budget_is_tight || strcmp(mode, "dense") == 0);

    out->continuity_fail_captured =
        cfg->buried_continuity_fail_present &&
        (strcmp(mode, "compressed") != 0 || cfg->kv_budget_mb >= 1.0);

    out->oracle_shock_captured =
        cfg->oracle_shock_present &&
        (strcmp(mode, "hybrid") == 0 || strcmp(mode, "membrane") == 0 || strcmp(mode, "state") == 0);

    const int under_pressure = (budget_is_tight || latency_is_tight);
    out->drift_flagged_under_pressure =
        (!under_pressure) || (cfg->multi_turn_drift_present && under_pressure);

    out->pass_count = 0;
    out->pass_count += out->witness_preserved ? 1 : 0;
    out->pass_count += out->continuity_fail_captured ? 1 : 0;
    out->pass_count += out->oracle_shock_captured ? 1 : 0;
    out->pass_count += out->drift_flagged_under_pressure ? 1 : 0;

    out->continuity_verdict = "PASS";
    if (!out->witness_preserved && cfg->fail_on_missing_witness) {
        out->continuity_verdict = "FAIL";
    } else if (!out->continuity_fail_captured || !out->oracle_shock_captured || !out->drift_flagged_under_pressure) {
        out->continuity_verdict = "PASS_WITH_DRIFT";
    }
}

void bf_context_compile_smoke_compare_defaults(BfContextCompileSmokeCompareConfig *cfg) {
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

int bf_context_compile_smoke_compare_json(const BfContextCompileSmokeCompareConfig *cfg, char **out_json) {
    if (!cfg || !out_json) return -1;

    const char *mode = safe_mode_(cfg->mode);

    BfContextCompileSmokeConfig scenarios[5];
    const char *names[5] = {
        "baseline",
        "budget_tight",
        "latency_tight",
        "combined_tight",
        "missing_proof_hash"
    };
    const char *descriptions[5] = {
        "baseline smoke policy",
        "adversarial low kv budget",
        "adversarial low latency budget",
        "adversarial low kv and latency budgets",
        "missing proof hash under witness-required policy"
    };

    for (int i = 0; i < 5; i++) {
        memset(&scenarios[i], 0, sizeof(BfContextCompileSmokeConfig));
        scenarios[i].mode = mode;
        scenarios[i].required_witnesses = cfg->required_witnesses;
        scenarios[i].residual_drift_threshold = cfg->residual_drift_threshold;
        scenarios[i].kv_budget_mb = cfg->kv_budget_mb;
        scenarios[i].latency_budget_ms = cfg->latency_budget_ms;
        scenarios[i].buried_continuity_fail_present = cfg->buried_continuity_fail_present;
        scenarios[i].proof_hash_present = cfg->proof_hash_present;
        scenarios[i].oracle_shock_present = cfg->oracle_shock_present;
        scenarios[i].multi_turn_drift_present = cfg->multi_turn_drift_present;
        scenarios[i].fail_on_missing_witness = cfg->fail_on_missing_witness;
    }

    scenarios[1].kv_budget_mb = (cfg->kv_budget_mb < 2.0) ? cfg->kv_budget_mb : 2.0;
    scenarios[2].latency_budget_ms = (cfg->latency_budget_ms < 20.0) ? cfg->latency_budget_ms : 20.0;
    scenarios[3].kv_budget_mb = (cfg->kv_budget_mb < 2.0) ? cfg->kv_budget_mb : 2.0;
    scenarios[3].latency_budget_ms = (cfg->latency_budget_ms < 20.0) ? cfg->latency_budget_ms : 20.0;
    scenarios[4].proof_hash_present = 0;

    SmokeEval evals[5];
    for (int i = 0; i < 5; i++) {
        eval_smoke_(&scenarios[i], &evals[i]);
    }

    size_t cap = 16384;
    char *json = (char *)malloc(cap);
    if (!json) return -1;

    size_t off = 0;
    int n = 0;

#define JEMIT(fmt, ...) \
    do { \
        n = snprintf(json + off, cap - off, fmt, ##__VA_ARGS__); \
        if (n <= 0 || (size_t)n >= cap - off) { free(json); return -1; } \
        off += (size_t)n; \
    } while (0)

    JEMIT(
        "{\n"
        "  \"schema_version\": \"akai.context.compile_smoke_compare.v1\",\n"
        "  \"mode\": \"%s\",\n"
        "  \"scenarios\": [\n",
        mode
    );

    for (int i = 0; i < 5; i++) {
        const SmokeEval *e = &evals[i];
        JEMIT(
            "    {\n"
            "      \"name\": \"%s\",\n"
            "      \"description\": \"%s\",\n"
            "      \"inputs\": {\n"
            "        \"required_witnesses\": %u,\n"
            "        \"kv_budget_mb\": %.6f,\n"
            "        \"latency_budget_ms\": %.6f,\n"
            "        \"proof_hash_present\": %s,\n"
            "        \"buried_continuity_fail_present\": %s\n"
            "      },\n"
            "      \"results\": {\n"
            "        \"required_witness_preserved_under_budget\": %s,\n"
            "        \"continuity_fail_captured_under_budget\": %s,\n"
            "        \"oracle_shock_state_captured\": %s,\n"
            "        \"multi_turn_drift_flagged_when_overcompressed\": %s,\n"
            "        \"pass_count\": %d,\n"
            "        \"continuity_verdict\": \"%s\"\n"
            "      }\n"
            "    }%s\n",
            names[i],
            descriptions[i],
            scenarios[i].required_witnesses,
            scenarios[i].kv_budget_mb,
            scenarios[i].latency_budget_ms,
            bool_json_(scenarios[i].proof_hash_present),
            bool_json_(scenarios[i].buried_continuity_fail_present),
            bool_json_(e->witness_preserved),
            bool_json_(e->continuity_fail_captured),
            bool_json_(e->oracle_shock_captured),
            bool_json_(e->drift_flagged_under_pressure),
            e->pass_count,
            e->continuity_verdict,
            (i < 4) ? "," : ""
        );
    }

    JEMIT("  ],\n  \"delta_vs_baseline\": [\n");

    for (int i = 1; i < 5; i++) {
        const SmokeEval *base = &evals[0];
        const SmokeEval *cur = &evals[i];
        int pass_delta = cur->pass_count - base->pass_count;

        const char *transition = "UNCHANGED";
        if (verdict_rank_(cur->continuity_verdict) > verdict_rank_(base->continuity_verdict)) transition = "IMPROVED";
        else if (verdict_rank_(cur->continuity_verdict) < verdict_rank_(base->continuity_verdict)) transition = "DEGRADED";

        JEMIT(
            "    {\n"
            "      \"scenario\": \"%s\",\n"
            "      \"pass_count_delta\": %d,\n"
            "      \"continuity_transition\": \"%s\",\n"
            "      \"verdict_before\": \"%s\",\n"
            "      \"verdict_after\": \"%s\"\n"
            "    }%s\n",
            names[i],
            pass_delta,
            transition,
            base->continuity_verdict,
            cur->continuity_verdict,
            (i < 4) ? "," : ""
        );
    }

    JEMIT("  ]\n}\n");

#undef JEMIT

    *out_json = json;
    return 0;
}
