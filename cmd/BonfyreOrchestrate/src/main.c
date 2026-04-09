#define _POSIX_C_SOURCE 200809L
#include "bonfyre.h"

#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sqlite3.h>

#define MAX_PLAN_STEPS 32
#define MAX_TEXT 128
#define MODEL_TEXT 96

typedef struct {
    char input_type[MAX_TEXT];
    char objective[MAX_TEXT];
    char latency_class[MAX_TEXT];
    char surface[MAX_TEXT];
    char artifact_path[256];
} OrchestrateRequest;

typedef struct {
    double exec;
    double artifact;
    double tensor;
    double cms;
    double retrieval;
    double value;
} BfFeedbackDomains;

typedef struct {
    double exec;
    double artifact;
    double tensor;
    double cms;
    double retrieval;
    double value;
} BfDomainWeights;

typedef struct {
    int modality_audio;
    int modality_artifact;
    int modality_text;
    int surface_pages;
    int surface_api;
    int surface_jobs;
    int latency_interactive;
    int latency_batch;
    int objective_publish;
    int objective_retrieval;
    int objective_value;
    int objective_cms;
    int artifact_local;
    int artifact_structured;
} OrchestrateStateVector;

typedef struct {
    int selected[MAX_PLAN_STEPS];
    int selected_count;
    int boosters[MAX_PLAN_STEPS];
    int booster_count;
    double booster_scores[MAX_PLAN_STEPS];
    int pre_gate_booster_count;
    const char *outputs[MAX_PLAN_STEPS];
    int output_count;
    const char *surfaces[8];
    int surface_count;
    char mode[24];
    char model[MODEL_TEXT];
    double predicted_cost;
    double predicted_latency;
    double predicted_confidence;
    double predicted_reversibility;
    double predicted_utility;
    double predicted_information_gain;
    double predicted_policy_score;
    double baseline_cost;
    double baseline_latency;
    double baseline_confidence;
    double baseline_reversibility;
    double baseline_utility;
    double baseline_information_gain;
    double baseline_policy_score;
    double uplift_policy_score;
    double uplift_latency;
    double uplift_cost;
    double uplift_confidence;
    double uplift_reversibility;
    double uplift_utility;
    double uplift_information_gain;
    char frontier_decision[32];
} OrchestratePlan;

typedef struct {
    int boosters[MAX_PLAN_STEPS];
    int count;
    char source[24];
} DistilledPriors;

typedef struct {
    double min_policy_gain;
    double max_latency_delta;
    double max_cost_delta;
    double min_utility_gain;
} UpliftGate;

static const char *DEFAULT_MODEL = "google/gemma-4-E4B";
static const char *DEFAULT_POLICY_DB = ".bonfyre/orchestrate.db";
static const char *SYSTEM_PROMPT =
    "Bonfyre Orchestrate. Machine-only. No user prompting. "
    "Choose only a small booster delta over the existing deterministic Bonfyre plan. "
    "Do not restate baseline stages. "
    "Only return JSON with key booster_binaries.";

static const char *objective_family(const OrchestrateRequest *req);
static int ensure_policy_db(sqlite3 **db);

static void usage(void) {
    fprintf(stderr,
            "bonfyre-orchestrate\n\n"
            "Usage:\n"
            "  bonfyre-orchestrate status\n"
            "  bonfyre-orchestrate plan <request.json>\n"
            "  bonfyre-orchestrate feedback <request.json> <quality_gain> <latency_delta>\n"
            "  bonfyre-orchestrate feedback <request.json> <feedback.json>\n\n"
            "Environment:\n"
            "  BONFYRE_ORCHESTRATE_ENDPOINT  OpenAI-compatible Gemma endpoint\n"
            "  BONFYRE_ORCHESTRATE_MODEL     Model name (default: google/gemma-4-E4B)\n"
            "  BONFYRE_ORCHESTRATE_API_KEY   Optional bearer token\n"
            "  BONFYRE_ORCHESTRATE_POLICY_DB Optional SQLite policy path\n");
}

static void copy_text(char *dst, size_t dst_sz, const char *src) {
    if (!dst || dst_sz == 0) return;
    snprintf(dst, dst_sz, "%s", src ? src : "");
}

static int icontains(const char *haystack, const char *needle) {
    if (!haystack || !needle || !needle[0]) return 0;
    size_t n = strlen(needle);
    for (const char *p = haystack; *p; ++p) {
        if (strncasecmp(p, needle, n) == 0) return 1;
    }
    return 0;
}

static double clamp01(double value) {
    if (value < 0.0) return 0.0;
    if (value > 1.0) return 1.0;
    return value;
}

static BfFeedbackDomains default_domains(double quality_gain, double latency_delta) {
    BfFeedbackDomains d;
    d.exec = clamp01(quality_gain - latency_delta + 0.5);
    d.artifact = clamp01(quality_gain);
    d.tensor = clamp01(quality_gain * 0.8);
    d.cms = clamp01(quality_gain * 0.75);
    d.retrieval = clamp01(quality_gain * 0.85);
    d.value = clamp01(quality_gain * 0.65);
    return d;
}

static BfDomainWeights default_weights(void) {
    BfDomainWeights w = {0.22, 0.18, 0.12, 0.16, 0.18, 0.14};
    return w;
}

static BfDomainWeights objective_weights(const OrchestrateRequest *req) {
    BfDomainWeights w = default_weights();

    if (icontains(req->objective, "cms") || icontains(req->surface, "cms") || icontains(req->objective, "publish")) {
        w.cms += 0.10;
        w.artifact += 0.05;
        w.retrieval -= 0.05;
        w.value -= 0.03;
    }
    if (icontains(req->objective, "search") || icontains(req->objective, "semantic") ||
        icontains(req->objective, "retrieval") || icontains(req->objective, "memory") ||
        icontains(req->objective, "atlas") || icontains(req->objective, "repo")) {
        w.retrieval += 0.10;
        w.tensor += 0.08;
        w.cms -= 0.05;
        w.value -= 0.03;
    }
    if (icontains(req->objective, "compress") || icontains(req->objective, "tensor") ||
        icontains(req->objective, "structure")) {
        w.tensor += 0.12;
        w.artifact += 0.04;
        w.exec -= 0.04;
    }
    if (icontains(req->objective, "sales") || icontains(req->objective, "grant") ||
        icontains(req->objective, "procurement") || icontains(req->objective, "offer") ||
        icontains(req->objective, "value")) {
        w.value += 0.12;
        w.cms += 0.04;
        w.tensor -= 0.04;
    }
    if (icontains(req->latency_class, "fast") || icontains(req->latency_class, "interactive")) {
        w.exec += 0.08;
        w.cms -= 0.02;
        w.value -= 0.02;
    }

    double sum = w.exec + w.artifact + w.tensor + w.cms + w.retrieval + w.value;
    if (sum <= 0.0) return default_weights();
    w.exec /= sum;
    w.artifact /= sum;
    w.tensor /= sum;
    w.cms /= sum;
    w.retrieval /= sum;
    w.value /= sum;
    return w;
}

static double domain_policy_score(BfFeedbackDomains d, BfDomainWeights w) {
    return
        d.exec * w.exec +
        d.artifact * w.artifact +
        d.tensor * w.tensor +
        d.cms * w.cms +
        d.retrieval * w.retrieval +
        d.value * w.value;
}

static const char *policy_source_for_mode(const char *mode) {
    if (!mode || !mode[0]) return "heuristic-baseline";
    if (strcmp(mode, "policy-memory") == 0) return "exact-policy-memory";
    if (strcmp(mode, "state-memory") == 0) return "state-policy-memory";
    if (strcmp(mode, "family-memory") == 0) return "family-policy-prior";
    if (strcmp(mode, "gemma4-delta") == 0) return "stability-gated-gemma-delta";
    return "heuristic-baseline";
}

static OrchestrateStateVector request_state_vector(const OrchestrateRequest *req) {
    OrchestrateStateVector v;
    memset(&v, 0, sizeof(v));
    v.modality_audio = icontains(req->input_type, "audio");
    v.modality_artifact = icontains(req->input_type, "artifact");
    v.modality_text = !v.modality_audio && !v.modality_artifact;
    v.surface_pages = icontains(req->surface, "pages");
    v.surface_api = icontains(req->surface, "api") || icontains(req->surface, "backend");
    v.surface_jobs = icontains(req->surface, "jobs") || icontains(req->surface, "queue") || icontains(req->surface, "actions");
    v.latency_interactive = icontains(req->latency_class, "interactive") || icontains(req->latency_class, "fast") || icontains(req->latency_class, "realtime");
    v.latency_batch = icontains(req->latency_class, "batch");
    v.objective_publish = icontains(req->objective, "publish") || icontains(req->objective, "podcast") || icontains(req->objective, "release") || icontains(req->objective, "radio");
    v.objective_retrieval = icontains(req->objective, "search") || icontains(req->objective, "semantic") || icontains(req->objective, "retrieval") || icontains(req->objective, "memory") || icontains(req->objective, "graph") || icontains(req->objective, "atlas");
    v.objective_value = icontains(req->objective, "sales") || icontains(req->objective, "grant") || icontains(req->objective, "procurement") || icontains(req->objective, "offer") || icontains(req->objective, "value");
    v.objective_cms = icontains(req->objective, "cms") || icontains(req->surface, "cms") || icontains(req->objective, "page") || icontains(req->objective, "content");
    v.artifact_local = req->artifact_path[0] && !icontains(req->artifact_path, "http://") && !icontains(req->artifact_path, "https://");
    v.artifact_structured = icontains(req->artifact_path, ".json") || icontains(req->artifact_path, ".md") || icontains(req->artifact_path, ".txt");
    return v;
}

static void build_state_key(const OrchestrateRequest *req, char *dst, size_t dst_sz) {
    OrchestrateStateVector v = request_state_vector(req);
    snprintf(dst, dst_sz, "m%d%d%d-s%d%d%d-l%d%d-o%d%d%d%d-a%d%d",
             v.modality_audio, v.modality_artifact, v.modality_text,
             v.surface_pages, v.surface_api, v.surface_jobs,
             v.latency_interactive, v.latency_batch,
             v.objective_publish, v.objective_retrieval, v.objective_value, v.objective_cms,
             v.artifact_local, v.artifact_structured);
}

static int json_string(const char *json, const char *key, char *dst, size_t dst_sz) {
    char needle[64];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != ':') return 0;
    p++;
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != '"') return 0;
    p++;
    size_t j = 0;
    while (*p && *p != '"' && j + 1 < dst_sz) {
        if (*p == '\\' && p[1]) {
            p++;
            dst[j++] = (*p == 'n') ? '\n' : *p;
        } else {
            dst[j++] = *p;
        }
        p++;
    }
    dst[j] = '\0';
    return j > 0;
}

static int json_double(const char *json, const char *key, double *value) {
    char needle[64];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != ':') return 0;
    p++;
    while (*p && isspace((unsigned char)*p)) p++;
    char *end = NULL;
    double parsed = strtod(p, &end);
    if (end == p) return 0;
    if (value) *value = parsed;
    return 1;
}

static void infer_defaults(OrchestrateRequest *req) {
    if (!req->input_type[0]) {
        if (icontains(req->artifact_path, ".wav") || icontains(req->artifact_path, ".mp3") ||
            icontains(req->artifact_path, ".m4a") || icontains(req->artifact_path, ".flac")) {
            copy_text(req->input_type, sizeof(req->input_type), "audio");
        } else if (icontains(req->artifact_path, "artifact.json")) {
            copy_text(req->input_type, sizeof(req->input_type), "artifact");
        } else {
            copy_text(req->input_type, sizeof(req->input_type), "text");
        }
    }
    if (!req->objective[0]) copy_text(req->objective, sizeof(req->objective), "boost-bonfyre-flow");
    if (!req->latency_class[0]) copy_text(req->latency_class, sizeof(req->latency_class), "interactive");
    if (!req->surface[0]) copy_text(req->surface, sizeof(req->surface), "pages");
}

static int load_request(const char *path, OrchestrateRequest *req) {
    memset(req, 0, sizeof(*req));
    copy_text(req->artifact_path, sizeof(req->artifact_path), path);
    char *json = bf_read_file(path, NULL);
    if (!json) return 1;
    json_string(json, "input_type", req->input_type, sizeof(req->input_type));
    json_string(json, "objective", req->objective, sizeof(req->objective));
    json_string(json, "latency_class", req->latency_class, sizeof(req->latency_class));
    json_string(json, "surface", req->surface, sizeof(req->surface));
    json_string(json, "artifact_path", req->artifact_path, sizeof(req->artifact_path));
    free(json);
    infer_defaults(req);
    return 0;
}

static int op_index(const char *name_or_binary) {
    const BfOperator *op = bf_operator_find(name_or_binary);
    if (!op) op = bf_operator_find_by_name(name_or_binary);
    return op ? (int)(op - BF_OPERATORS) : -1;
}

static int contains_idx(const int *items, int count, int idx) {
    for (int i = 0; i < count; ++i) {
        if (items[i] == idx) return 1;
    }
    return 0;
}

static void add_selected(OrchestratePlan *plan, const char *name_or_binary) {
    int idx = op_index(name_or_binary);
    if (idx < 0 || plan->selected_count >= MAX_PLAN_STEPS || contains_idx(plan->selected, plan->selected_count, idx)) return;
    plan->selected[plan->selected_count++] = idx;
}

static void add_booster(OrchestratePlan *plan, const char *name_or_binary) {
    int idx = op_index(name_or_binary);
    if (idx < 0 || plan->booster_count >= MAX_PLAN_STEPS ||
        contains_idx(plan->selected, plan->selected_count, idx) ||
        contains_idx(plan->boosters, plan->booster_count, idx)) return;
    plan->boosters[plan->booster_count++] = idx;
}

static void add_surface(OrchestratePlan *plan, const char *surface) {
    if (!surface || !surface[0] || plan->surface_count >= 8) return;
    for (int i = 0; i < plan->surface_count; ++i) {
        if (strcmp(plan->surfaces[i], surface) == 0) return;
    }
    plan->surfaces[plan->surface_count++] = surface;
}

static int priors_contains(const DistilledPriors *priors, int op_idx) {
    if (!priors) return 0;
    for (int i = 0; i < priors->count; ++i) {
        if (priors->boosters[i] == op_idx) return i + 1;
    }
    return 0;
}

static BfFeedbackDomains profile_domains(BfOperatorProfile profile) {
    BfFeedbackDomains d;
    d.exec = clamp01(1.0 - profile.latency);
    d.artifact = clamp01(profile.reversibility);
    d.tensor = clamp01((profile.information_gain + profile.reversibility) * 0.5);
    d.cms = clamp01((profile.utility + profile.confidence) * 0.5);
    d.retrieval = clamp01((profile.information_gain + profile.utility) * 0.5);
    d.value = clamp01((profile.utility + (1.0 - profile.cost)) * 0.5);
    return d;
}

static double booster_gain_score(const OrchestrateRequest *req, int op_idx, const DistilledPriors *priors) {
    BfOperatorProfile profile = bf_operator_profile(&BF_OPERATORS[op_idx]);
    BfFeedbackDomains d = profile_domains(profile);
    BfDomainWeights w = objective_weights(req);
    int fast = icontains(req->latency_class, "fast") || icontains(req->latency_class, "interactive") || icontains(req->latency_class, "realtime");
    double gain = domain_policy_score(d, w);
    double latency_penalty = fast ? 0.45 : 0.28;
    double cost_penalty = fast ? 0.25 : 0.18;
    double prior_bonus = 0.0;
    int rank = priors_contains(priors, op_idx);
    if (rank > 0) {
        prior_bonus = 0.08 - ((double)(rank - 1) * 0.01);
        if (prior_bonus < 0.02) prior_bonus = 0.02;
    }
    return gain - (profile.latency * latency_penalty) - (profile.cost * cost_penalty) + prior_bonus;
}

static void rebalance_boosters(const OrchestrateRequest *req, OrchestratePlan *plan, const DistilledPriors *priors) {
    if (plan->booster_count <= 1) return;
    int fast = icontains(req->latency_class, "fast") || icontains(req->latency_class, "interactive") || icontains(req->latency_class, "realtime");
    int max_boosters = fast ? 4 : 7;
    if (icontains(req->surface, "jobs") || icontains(req->surface, "queue") || icontains(req->surface, "actions")) {
        max_boosters += 1;
    }

    double scores[MAX_PLAN_STEPS];
    for (int i = 0; i < plan->booster_count; ++i) {
        scores[i] = booster_gain_score(req, plan->boosters[i], priors);
        plan->booster_scores[i] = scores[i];
    }

    for (int i = 0; i < plan->booster_count - 1; ++i) {
        int best = i;
        for (int j = i + 1; j < plan->booster_count; ++j) {
            if (scores[j] > scores[best]) best = j;
        }
        if (best != i) {
            double score_tmp = scores[i];
            int booster_tmp = plan->boosters[i];
            double contrib_tmp = plan->booster_scores[i];
            scores[i] = scores[best];
            plan->boosters[i] = plan->boosters[best];
            plan->booster_scores[i] = plan->booster_scores[best];
            scores[best] = score_tmp;
            plan->boosters[best] = booster_tmp;
            plan->booster_scores[best] = contrib_tmp;
        }
    }

    int keep = 0;
    for (int i = 0; i < plan->booster_count && keep < max_boosters; ++i) {
        if (scores[i] > 0.12 || keep == 0) {
            plan->boosters[keep] = plan->boosters[i];
            plan->booster_scores[keep] = plan->booster_scores[i];
            keep++;
        }
    }
    plan->booster_count = keep;
}

static void collect_outputs(OrchestratePlan *plan) {
    plan->output_count = 0;
    for (int pass = 0; pass < 2; ++pass) {
        const int *items = pass == 0 ? plan->selected : plan->boosters;
        int count = pass == 0 ? plan->selected_count : plan->booster_count;
        for (int i = 0; i < count; ++i) {
            const BfOperator *op = &BF_OPERATORS[items[i]];
            for (int j = 0; j < BF_MAX_TYPES && op->output_types[j]; ++j) {
                const char *out = op->output_types[j];
                int dup = 0;
                for (int k = 0; k < plan->output_count; ++k) {
                    if (strcmp(plan->outputs[k], out) == 0) {
                        dup = 1;
                        break;
                    }
                }
                if (!dup && plan->output_count < MAX_PLAN_STEPS) {
                    plan->outputs[plan->output_count++] = out;
                }
            }
        }
    }
}

static void compute_plan_metrics(const OrchestrateRequest *req, OrchestratePlan *plan) {
    double cost = 0.0;
    double latency = 0.0;
    double confidence = 0.0;
    double reversibility = 0.0;
    double utility = 0.0;
    double information_gain = 0.0;
    double base_cost = 0.0;
    double base_latency = 0.0;
    double base_confidence = 0.0;
    double base_reversibility = 0.0;
    double base_utility = 0.0;
    double base_information_gain = 0.0;
    int count = 0;
    int base_count = 0;

    for (int i = 0; i < plan->selected_count; ++i) {
        BfOperatorProfile profile = bf_operator_profile(&BF_OPERATORS[plan->selected[i]]);
        cost += profile.cost;
        latency += profile.latency;
        confidence += profile.confidence;
        reversibility += profile.reversibility;
        utility += profile.utility;
        information_gain += profile.information_gain;
        base_cost += profile.cost;
        base_latency += profile.latency;
        base_confidence += profile.confidence;
        base_reversibility += profile.reversibility;
        base_utility += profile.utility;
        base_information_gain += profile.information_gain;
        count++;
        base_count++;
    }
    for (int i = 0; i < plan->booster_count; ++i) {
        BfOperatorProfile profile = bf_operator_profile(&BF_OPERATORS[plan->boosters[i]]);
        cost += profile.cost * 0.45;
        latency += profile.latency * 0.45;
        confidence += profile.confidence * 0.45;
        reversibility += profile.reversibility * 0.45;
        utility += profile.utility * 0.60;
        information_gain += profile.information_gain * 0.75;
        count++;
    }

    if (count <= 0) count = 1;
    if (base_count <= 0) base_count = 1;
    plan->predicted_cost = cost / (double)count;
    plan->predicted_latency = latency / (double)count;
    plan->predicted_confidence = confidence / (double)count;
    plan->predicted_reversibility = reversibility / (double)count;
    plan->predicted_utility = utility / (double)count;
    plan->predicted_information_gain = information_gain / (double)count;
    plan->baseline_cost = base_cost / (double)base_count;
    plan->baseline_latency = base_latency / (double)base_count;
    plan->baseline_confidence = base_confidence / (double)base_count;
    plan->baseline_reversibility = base_reversibility / (double)base_count;
    plan->baseline_utility = base_utility / (double)base_count;
    plan->baseline_information_gain = base_information_gain / (double)base_count;
    BfFeedbackDomains as_domains;
    as_domains.exec = clamp01(1.0 - plan->predicted_latency);
    as_domains.artifact = clamp01(plan->predicted_reversibility);
    as_domains.tensor = clamp01((plan->predicted_information_gain + plan->predicted_reversibility) * 0.5);
    as_domains.cms = clamp01((plan->predicted_utility + plan->predicted_confidence) * 0.5);
    as_domains.retrieval = clamp01((plan->predicted_information_gain + plan->predicted_utility) * 0.5);
    as_domains.value = clamp01((plan->predicted_utility + (1.0 - plan->predicted_cost)) * 0.5);
    plan->predicted_policy_score = domain_policy_score(as_domains, objective_weights(req));
    as_domains.exec = clamp01(1.0 - plan->baseline_latency);
    as_domains.artifact = clamp01(plan->baseline_reversibility);
    as_domains.tensor = clamp01((plan->baseline_information_gain + plan->baseline_reversibility) * 0.5);
    as_domains.cms = clamp01((plan->baseline_utility + plan->baseline_confidence) * 0.5);
    as_domains.retrieval = clamp01((plan->baseline_information_gain + plan->baseline_utility) * 0.5);
    as_domains.value = clamp01((plan->baseline_utility + (1.0 - plan->baseline_cost)) * 0.5);
    plan->baseline_policy_score = domain_policy_score(as_domains, objective_weights(req));
    plan->uplift_policy_score = plan->predicted_policy_score - plan->baseline_policy_score;
    plan->uplift_latency = plan->predicted_latency - plan->baseline_latency;
    plan->uplift_cost = plan->predicted_cost - plan->baseline_cost;
    plan->uplift_confidence = plan->predicted_confidence - plan->baseline_confidence;
    plan->uplift_reversibility = plan->predicted_reversibility - plan->baseline_reversibility;
    plan->uplift_utility = plan->predicted_utility - plan->baseline_utility;
    plan->uplift_information_gain = plan->predicted_information_gain - plan->baseline_information_gain;
}

static UpliftGate default_uplift_gate(void) {
    UpliftGate gate = {0.015, 0.050, 0.050, 0.060};
    return gate;
}

static UpliftGate adaptive_uplift_gate(const OrchestrateRequest *req) {
    UpliftGate gate = default_uplift_gate();
    sqlite3 *db = NULL;
    if (ensure_policy_db(&db) != 0) return gate;
    char state_key[64];
    build_state_key(req, state_key, sizeof(state_key));
    const char *family = objective_family(req);
    sqlite3_stmt *stmt = NULL;
    double avg_regret = 0.0;
    double policy_score = 0.0;
    int samples = 0;

    const char *state_sql =
        "SELECT avg_regret, policy_score, samples FROM orchestration_policy "
        "WHERE state_key = ?1 ORDER BY samples DESC, updated_at DESC LIMIT 1;";
    if (sqlite3_prepare_v2(db, state_sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, state_key, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            avg_regret = sqlite3_column_double(stmt, 0);
            policy_score = sqlite3_column_double(stmt, 1);
            samples = sqlite3_column_int(stmt, 2);
        }
    }
    sqlite3_finalize(stmt);

    if (samples < 2) {
        const char *family_sql =
            "SELECT avg_regret, policy_score, max(samples) FROM orchestration_policy "
            "WHERE family = ?1 AND input_type = ?2 AND latency_class = ?3 AND surface = ?4;";
        if (sqlite3_prepare_v2(db, family_sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, family, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 2, req->input_type, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 3, req->latency_class, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 4, req->surface, -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                avg_regret = sqlite3_column_double(stmt, 0);
                policy_score = sqlite3_column_double(stmt, 1);
                samples = sqlite3_column_int(stmt, 2);
            }
        }
        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);
    if (samples < 2) return gate;
    if (avg_regret > 0.05) {
        gate.min_policy_gain += 0.020;
        gate.max_latency_delta -= 0.015;
        gate.max_cost_delta -= 0.015;
        gate.min_utility_gain += 0.030;
    } else if (avg_regret < -0.10 && policy_score > 0.60) {
        gate.min_policy_gain -= 0.007;
        gate.max_latency_delta += 0.010;
        gate.max_cost_delta += 0.010;
        gate.min_utility_gain -= 0.015;
    }
    if (gate.min_policy_gain < 0.005) gate.min_policy_gain = 0.005;
    if (gate.max_latency_delta < 0.020) gate.max_latency_delta = 0.020;
    if (gate.max_cost_delta < 0.020) gate.max_cost_delta = 0.020;
    if (gate.min_utility_gain < 0.020) gate.min_utility_gain = 0.020;
    return gate;
}

static int frontier_uplift_is_worth_it(const OrchestratePlan *plan, UpliftGate gate) {
    if (!plan) return 0;
    if (plan->booster_count <= 0) return 1;
    if (plan->uplift_policy_score >= gate.min_policy_gain) return 1;
    if (plan->uplift_utility >= gate.min_utility_gain &&
        plan->uplift_latency <= gate.max_latency_delta &&
        plan->uplift_cost <= gate.max_cost_delta) return 1;
    return 0;
}

static void apply_frontier_uplift_gate(const OrchestrateRequest *req, OrchestratePlan *plan) {
    if (!plan || plan->booster_count <= 0) return;
    plan->pre_gate_booster_count = plan->booster_count;
    UpliftGate gate = adaptive_uplift_gate(req);
    if (frontier_uplift_is_worth_it(plan, gate)) {
        copy_text(plan->frontier_decision, sizeof(plan->frontier_decision), "retained");
        return;
    }
    plan->booster_count = 0;
    collect_outputs(plan);
    compute_plan_metrics(req, plan);
    copy_text(plan->frontier_decision, sizeof(plan->frontier_decision), "collapsed-to-floor");
}

static void build_signature(const OrchestrateRequest *req, char *dst, size_t dst_sz) {
    snprintf(dst, dst_sz, "%s|%s|%s|%s",
             req->input_type, req->objective, req->latency_class, req->surface);
}

static const char *objective_family(const OrchestrateRequest *req) {
    if (icontains(req->objective, "podcast") || icontains(req->objective, "publish") ||
        icontains(req->objective, "release") || icontains(req->objective, "radio")) return "publish";
    if (icontains(req->objective, "memory") || icontains(req->objective, "search") ||
        icontains(req->objective, "semantic") || icontains(req->objective, "repo") ||
        icontains(req->objective, "atlas") || icontains(req->objective, "graph")) return "retrieval";
    if (icontains(req->objective, "legal") || icontains(req->objective, "evidence") ||
        icontains(req->objective, "sales") || icontains(req->objective, "grant") ||
        icontains(req->objective, "procurement") || icontains(req->objective, "offer") ||
        icontains(req->objective, "value")) return "value";
    if (icontains(req->objective, "shift") || icontains(req->objective, "handoff") ||
        icontains(req->objective, "live") || icontains(req->objective, "call")) return "live";
    if (icontains(req->objective, "cms") || icontains(req->objective, "page") ||
        icontains(req->objective, "content")) return "cms";
    return "general";
}

static const char *policy_db_path(void) {
    const char *path = getenv("BONFYRE_ORCHESTRATE_POLICY_DB");
    if (path && path[0]) return path;
    static char fallback[512];
    const char *home = getenv("HOME");
    snprintf(fallback, sizeof(fallback), "%s/%s", home && home[0] ? home : ".", DEFAULT_POLICY_DB);
    return fallback;
}

static int ensure_policy_db(sqlite3 **db) {
    if (!db) return 1;
    *db = NULL;
    const char *path = policy_db_path();
    char parent[512];
    snprintf(parent, sizeof(parent), "%s", path);
    char *slash = strrchr(parent, '/');
    if (slash) {
        *slash = '\0';
        if (parent[0]) bf_ensure_dir(parent);
    }
    if (sqlite3_open(path, db) != SQLITE_OK) return 1;
    const char *sql =
        "CREATE TABLE IF NOT EXISTS orchestration_policy ("
        "signature TEXT PRIMARY KEY,"
        "state_key TEXT NOT NULL DEFAULT '',"
        "family TEXT NOT NULL DEFAULT 'general',"
        "input_type TEXT NOT NULL DEFAULT '',"
        "latency_class TEXT NOT NULL DEFAULT '',"
        "surface TEXT NOT NULL DEFAULT '',"
        "booster_csv TEXT NOT NULL,"
        "predicted_confidence REAL NOT NULL,"
        "predicted_information_gain REAL NOT NULL,"
        "avg_quality_gain REAL NOT NULL DEFAULT 0,"
        "avg_latency_delta REAL NOT NULL DEFAULT 0,"
        "avg_regret REAL NOT NULL DEFAULT 0,"
        "exec_score REAL NOT NULL DEFAULT 0,"
        "artifact_score REAL NOT NULL DEFAULT 0,"
        "tensor_score REAL NOT NULL DEFAULT 0,"
        "cms_score REAL NOT NULL DEFAULT 0,"
        "retrieval_score REAL NOT NULL DEFAULT 0,"
        "value_score REAL NOT NULL DEFAULT 0,"
        "policy_score REAL NOT NULL DEFAULT 0,"
        "samples INTEGER NOT NULL DEFAULT 0,"
        "updated_at TEXT NOT NULL"
        ");";

    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN state_key TEXT NOT NULL DEFAULT '';", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN family TEXT NOT NULL DEFAULT 'general';", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN input_type TEXT NOT NULL DEFAULT '';", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN latency_class TEXT NOT NULL DEFAULT '';", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN surface TEXT NOT NULL DEFAULT '';", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN exec_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN artifact_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN tensor_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN cms_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN retrieval_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN value_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN policy_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    if (sqlite3_exec(*db, sql, NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(*db);
        *db = NULL;
        return 1;
    }
    return 0;
}

static void import_booster_csv(const OrchestrateRequest *req, OrchestratePlan *plan, const char *csv) {
    if (!csv || !csv[0]) return;
    char *copy = strdup(csv);
    if (!copy) return;
    for (char *save = NULL, *token = strtok_r(copy, ",", &save); token; token = strtok_r(NULL, ",", &save)) {
        add_booster(plan, token);
    }
    free(copy);
    collect_outputs(plan);
    compute_plan_metrics(req, plan);
}

static void parse_priors_csv(DistilledPriors *priors, const char *csv, const char *source) {
    if (!priors || !csv || !csv[0]) return;
    memset(priors, 0, sizeof(*priors));
    copy_text(priors->source, sizeof(priors->source), source);
    char *copy = strdup(csv);
    if (!copy) return;
    for (char *save = NULL, *token = strtok_r(copy, ",", &save);
         token && priors->count < MAX_PLAN_STEPS;
         token = strtok_r(NULL, ",", &save)) {
        int idx = op_index(token);
        if (idx >= 0 && !contains_idx(priors->boosters, priors->count, idx)) {
            priors->boosters[priors->count++] = idx;
        }
    }
    free(copy);
}

static int load_distilled_priors(const OrchestrateRequest *req, DistilledPriors *priors) {
    if (!priors) return 0;
    memset(priors, 0, sizeof(*priors));
    sqlite3 *db = NULL;
    if (ensure_policy_db(&db) != 0) return 0;
    char state_key[64];
    build_state_key(req, state_key, sizeof(state_key));
    const char *family = objective_family(req);
    sqlite3_stmt *stmt = NULL;
    int found = 0;

    const char *state_sql =
        "SELECT booster_csv FROM orchestration_policy "
        "WHERE state_key = ?1 AND samples >= 2 AND avg_regret <= 0.12 AND policy_score >= 0.30 "
        "ORDER BY policy_score DESC, samples DESC LIMIT 1;";
    if (sqlite3_prepare_v2(db, state_sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, state_key, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char *csv = sqlite3_column_text(stmt, 0);
            if (csv) {
                parse_priors_csv(priors, (const char *)csv, "state");
                found = priors->count > 0;
            }
        }
    }
    sqlite3_finalize(stmt);

    if (!found) {
        const char *family_sql =
            "SELECT booster_csv FROM orchestration_policy "
            "WHERE family = ?1 AND input_type = ?2 AND latency_class = ?3 AND surface = ?4 "
            "AND samples >= 2 AND avg_regret <= 0.10 AND policy_score >= 0.35 "
            "ORDER BY policy_score DESC, samples DESC LIMIT 1;";
        if (sqlite3_prepare_v2(db, family_sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, family, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 2, req->input_type, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 3, req->latency_class, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 4, req->surface, -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                const unsigned char *csv = sqlite3_column_text(stmt, 0);
                if (csv) {
                    parse_priors_csv(priors, (const char *)csv, "family");
                    found = priors->count > 0;
                }
            }
        }
        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);
    return found;
}

static int load_policy_memory(const OrchestrateRequest *req, OrchestratePlan *plan) {
    sqlite3 *db = NULL;
    if (ensure_policy_db(&db) != 0) return 0;
    char signature[512];
    char state_key[64];
    build_signature(req, signature, sizeof(signature));
    build_state_key(req, state_key, sizeof(state_key));
    const char *family = objective_family(req);

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "SELECT booster_csv, predicted_confidence, predicted_information_gain, avg_regret, samples, policy_score "
        "FROM orchestration_policy WHERE signature = ?1;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    sqlite3_bind_text(stmt, 1, signature, -1, SQLITE_STATIC);
    int found = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char *csv = sqlite3_column_text(stmt, 0);
        double cached_conf = sqlite3_column_double(stmt, 1);
        double cached_regret = sqlite3_column_double(stmt, 3);
        int samples = sqlite3_column_int(stmt, 4);
        double policy_score = sqlite3_column_double(stmt, 5);
        if (csv && cached_conf >= plan->predicted_confidence &&
            (samples < 3 || (cached_regret <= 0.15 && policy_score >= 0.25))) {
            import_booster_csv(req, plan, (const char *)csv);
            copy_text(plan->mode, sizeof(plan->mode), "policy-memory");
            found = 1;
        }
    }
    sqlite3_finalize(stmt);
    if (!found) {
        const char *state_sql =
            "SELECT booster_csv, predicted_confidence, avg_regret, samples, policy_score "
            "FROM orchestration_policy "
            "WHERE state_key = ?1 "
            "AND samples >= 2 AND avg_regret <= 0.12 AND policy_score >= 0.30 "
            "ORDER BY policy_score DESC, samples DESC LIMIT 1;";
        if (sqlite3_prepare_v2(db, state_sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, state_key, -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                const unsigned char *csv = sqlite3_column_text(stmt, 0);
                double cached_conf = sqlite3_column_double(stmt, 1);
                if (csv && cached_conf + 0.01 >= plan->predicted_confidence) {
                    import_booster_csv(req, plan, (const char *)csv);
                    copy_text(plan->mode, sizeof(plan->mode), "state-memory");
                    found = 1;
                }
            }
        }
        sqlite3_finalize(stmt);
    }
    if (!found) {
        const char *fallback_sql =
            "SELECT booster_csv, predicted_confidence, avg_regret, samples, policy_score "
            "FROM orchestration_policy "
            "WHERE family = ?1 AND input_type = ?2 AND latency_class = ?3 AND surface = ?4 "
            "AND samples >= 2 AND avg_regret <= 0.10 AND policy_score >= 0.35 "
            "ORDER BY policy_score DESC, samples DESC LIMIT 1;";
        if (sqlite3_prepare_v2(db, fallback_sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, family, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 2, req->input_type, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 3, req->latency_class, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 4, req->surface, -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                const unsigned char *csv = sqlite3_column_text(stmt, 0);
                double cached_conf = sqlite3_column_double(stmt, 1);
                if (csv && cached_conf + 0.02 >= plan->predicted_confidence) {
                    import_booster_csv(req, plan, (const char *)csv);
                    copy_text(plan->mode, sizeof(plan->mode), "family-memory");
                    found = 1;
                }
            }
        }
        sqlite3_finalize(stmt);
    }
    sqlite3_close(db);
    return found;
}

static void save_policy_memory(const OrchestrateRequest *req, const OrchestratePlan *plan) {
    sqlite3 *db = NULL;
    if (ensure_policy_db(&db) != 0) return;
    char signature[512];
    char state_key[64];
    char updated_at[32];
    build_signature(req, signature, sizeof(signature));
    build_state_key(req, state_key, sizeof(state_key));
    bf_iso_timestamp(updated_at, sizeof(updated_at));
    const char *family = objective_family(req);

    char booster_csv[1024];
    booster_csv[0] = '\0';
    for (int i = 0; i < plan->booster_count; ++i) {
        const char *binary = BF_OPERATORS[plan->boosters[i]].binary;
        if (i) strncat(booster_csv, ",", sizeof(booster_csv) - strlen(booster_csv) - 1);
        strncat(booster_csv, binary, sizeof(booster_csv) - strlen(booster_csv) - 1);
    }

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT INTO orchestration_policy(signature, state_key, family, input_type, latency_class, surface, booster_csv, predicted_confidence, predicted_information_gain, updated_at) "
        "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) "
        "ON CONFLICT(signature) DO UPDATE SET "
        "state_key=excluded.state_key, "
        "family=excluded.family, "
        "input_type=excluded.input_type, "
        "latency_class=excluded.latency_class, "
        "surface=excluded.surface, "
        "booster_csv=excluded.booster_csv, "
        "predicted_confidence=excluded.predicted_confidence, "
        "predicted_information_gain=excluded.predicted_information_gain, "
        "updated_at=excluded.updated_at;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, signature, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, state_key, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 3, family, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 4, req->input_type, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 5, req->latency_class, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 6, req->surface, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 7, booster_csv, -1, SQLITE_STATIC);
        sqlite3_bind_double(stmt, 8, plan->predicted_confidence);
        sqlite3_bind_double(stmt, 9, plan->predicted_information_gain);
        sqlite3_bind_text(stmt, 10, updated_at, -1, SQLITE_STATIC);
        sqlite3_step(stmt);
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

static int load_feedback_payload(const char *path, double *quality_gain, double *latency_delta, BfFeedbackDomains *domains, int *has_domain_override) {
    char *json = bf_read_file(path, NULL);
    if (!json) return 1;

    double q = 0.0;
    double l = 0.0;
    domains->exec = NAN;
    domains->artifact = NAN;
    domains->tensor = NAN;
    domains->cms = NAN;
    domains->retrieval = NAN;
    domains->value = NAN;
    int have_q = json_double(json, "quality_gain", &q);
    int have_l = json_double(json, "latency_delta", &l);
    int have_exec = json_double(json, "exec", &domains->exec);
    int have_artifact = json_double(json, "artifact", &domains->artifact);
    int have_tensor = json_double(json, "tensor", &domains->tensor);
    int have_cms = json_double(json, "cms", &domains->cms);
    int have_retrieval = json_double(json, "retrieval", &domains->retrieval);
    int have_value = json_double(json, "value", &domains->value);

    if (quality_gain) *quality_gain = have_q ? q : 0.0;
    if (latency_delta) *latency_delta = have_l ? l : 0.0;
    if (has_domain_override) {
        *has_domain_override = have_exec || have_artifact || have_tensor || have_cms || have_retrieval || have_value;
    }
    free(json);
    return 0;
}

static int command_feedback(const char *path, const char *feedback_arg, const char *latency_delta_text) {
    OrchestrateRequest req;
    if (load_request(path, &req) != 0) {
        fprintf(stderr, "Failed to read request file: %s\n", path);
        return 1;
    }

    double quality_gain = 0.0;
    double latency_delta = 0.0;
    BfFeedbackDomains domains;
    int has_domain_override = 0;

    if (latency_delta_text) {
        quality_gain = atof(feedback_arg);
        latency_delta = atof(latency_delta_text);
        domains = default_domains(quality_gain, latency_delta);
    } else {
        if (load_feedback_payload(feedback_arg, &quality_gain, &latency_delta, &domains, &has_domain_override) != 0) {
            fprintf(stderr, "Failed to read feedback file: %s\n", feedback_arg);
            return 1;
        }
        BfFeedbackDomains derived = default_domains(quality_gain, latency_delta);
        if (!has_domain_override) {
            domains = derived;
        } else {
            if (isnan(domains.exec)) domains.exec = derived.exec;
            if (isnan(domains.artifact)) domains.artifact = derived.artifact;
            if (isnan(domains.tensor)) domains.tensor = derived.tensor;
            if (isnan(domains.cms)) domains.cms = derived.cms;
            if (isnan(domains.retrieval)) domains.retrieval = derived.retrieval;
            if (isnan(domains.value)) domains.value = derived.value;
        }
    }

    double regret = latency_delta - quality_gain;
    double policy_score = domain_policy_score(domains, objective_weights(&req));

    sqlite3 *db = NULL;
    if (ensure_policy_db(&db) != 0) {
        fprintf(stderr, "Failed to open policy db\n");
        return 1;
    }

    char signature[512];
    char state_key[64];
    char updated_at[32];
    build_signature(&req, signature, sizeof(signature));
    build_state_key(&req, state_key, sizeof(state_key));
    bf_iso_timestamp(updated_at, sizeof(updated_at));
    const char *family = objective_family(&req);

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT INTO orchestration_policy(signature, state_key, family, input_type, latency_class, surface, booster_csv, predicted_confidence, predicted_information_gain, avg_quality_gain, avg_latency_delta, avg_regret, exec_score, artifact_score, tensor_score, cms_score, retrieval_score, value_score, policy_score, samples, updated_at) "
        "VALUES(?1, ?2, ?3, ?4, ?5, ?6, '', 0, 0, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, 1, ?17) "
        "ON CONFLICT(signature) DO UPDATE SET "
        "state_key=excluded.state_key, "
        "family=excluded.family, "
        "input_type=excluded.input_type, "
        "latency_class=excluded.latency_class, "
        "surface=excluded.surface, "
        "avg_quality_gain=((avg_quality_gain*samples)+excluded.avg_quality_gain)/(samples+1), "
        "avg_latency_delta=((avg_latency_delta*samples)+excluded.avg_latency_delta)/(samples+1), "
        "avg_regret=((avg_regret*samples)+excluded.avg_regret)/(samples+1), "
        "exec_score=((exec_score*samples)+excluded.exec_score)/(samples+1), "
        "artifact_score=((artifact_score*samples)+excluded.artifact_score)/(samples+1), "
        "tensor_score=((tensor_score*samples)+excluded.tensor_score)/(samples+1), "
        "cms_score=((cms_score*samples)+excluded.cms_score)/(samples+1), "
        "retrieval_score=((retrieval_score*samples)+excluded.retrieval_score)/(samples+1), "
        "value_score=((value_score*samples)+excluded.value_score)/(samples+1), "
        "policy_score=((policy_score*samples)+excluded.policy_score)/(samples+1), "
        "samples=samples+1, "
        "updated_at=excluded.updated_at;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        fprintf(stderr, "Failed to prepare feedback statement\n");
        return 1;
    }
    sqlite3_bind_text(stmt, 1, signature, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, state_key, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, family, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, req.input_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 5, req.latency_class, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 6, req.surface, -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 7, quality_gain);
    sqlite3_bind_double(stmt, 8, latency_delta);
    sqlite3_bind_double(stmt, 9, regret);
    sqlite3_bind_double(stmt, 10, domains.exec);
    sqlite3_bind_double(stmt, 11, domains.artifact);
    sqlite3_bind_double(stmt, 12, domains.tensor);
    sqlite3_bind_double(stmt, 13, domains.cms);
    sqlite3_bind_double(stmt, 14, domains.retrieval);
    sqlite3_bind_double(stmt, 15, domains.value);
    sqlite3_bind_double(stmt, 16, policy_score);
    sqlite3_bind_text(stmt, 17, updated_at, -1, SQLITE_STATIC);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    printf("{\"status\":\"ok\",\"signature\":\"%s\",\"quality_gain\":%.3f,\"latency_delta\":%.3f,\"regret\":%.3f,"
           "\"domains\":{\"exec\":%.3f,\"artifact\":%.3f,\"tensor\":%.3f,\"cms\":%.3f,\"retrieval\":%.3f,\"value\":%.3f},"
           "\"policy_score\":%.3f}\n",
           signature, quality_gain, latency_delta, regret,
           domains.exec, domains.artifact, domains.tensor, domains.cms, domains.retrieval, domains.value,
           policy_score);
    return 0;
}

static void init_plan(OrchestratePlan *plan, const char *model) {
    memset(plan, 0, sizeof(*plan));
    copy_text(plan->mode, sizeof(plan->mode), "heuristic");
    copy_text(plan->model, sizeof(plan->model), model && model[0] ? model : DEFAULT_MODEL);
    copy_text(plan->frontier_decision, sizeof(plan->frontier_decision), "floor-only");
}

static void heuristic_plan(const OrchestrateRequest *req, OrchestratePlan *plan) {
    int fast = icontains(req->latency_class, "fast") || icontains(req->latency_class, "interactive") || icontains(req->latency_class, "realtime");
    DistilledPriors priors;
    int have_priors = load_distilled_priors(req, &priors);

    if (icontains(req->input_type, "audio")) {
        add_selected(plan, "ingest");
        add_selected(plan, "media-prep");
        add_selected(plan, "transcribe");
        add_selected(plan, fast ? "brief" : "transcript-clean");
        if (fast) {
            add_booster(plan, "transcript-clean");
            add_booster(plan, "paragraph");
        } else {
            add_selected(plan, "paragraph");
            add_selected(plan, "brief");
        }
        add_booster(plan, "proof");
        add_booster(plan, "tag");
    } else if (icontains(req->input_type, "artifact")) {
        add_selected(plan, "hash");
        add_selected(plan, "canon");
        add_selected(plan, "render");
        add_booster(plan, "query");
        add_booster(plan, "graph");
    } else {
        add_selected(plan, "ingest");
        add_selected(plan, "canon");
        add_selected(plan, "brief");
        add_booster(plan, "tag");
        add_booster(plan, "render");
    }

    if (icontains(req->objective, "podcast") || icontains(req->objective, "publish") ||
        icontains(req->objective, "release") || icontains(req->objective, "radio")) {
        add_booster(plan, "narrate");
        add_booster(plan, "clips");
        add_booster(plan, "render");
        add_booster(plan, "emit");
        add_booster(plan, "pack");
        add_booster(plan, "distribute");
    }

    if (icontains(req->objective, "memory") || icontains(req->objective, "search") ||
        icontains(req->objective, "semantic") || icontains(req->objective, "repo") ||
        icontains(req->objective, "civic") || icontains(req->objective, "atlas")) {
        add_booster(plan, "embed");
        add_booster(plan, "index");
        add_booster(plan, "vec");
        add_booster(plan, "query");
        add_booster(plan, "graph");
    }

    if (icontains(req->objective, "legal") || icontains(req->objective, "evidence") ||
        icontains(req->objective, "sales") || icontains(req->objective, "grant") ||
        icontains(req->objective, "procurement") || icontains(req->objective, "consult")) {
        add_booster(plan, "offer");
        add_booster(plan, "ledger");
        add_booster(plan, "gate");
        add_booster(plan, "meter");
    }

    if (icontains(req->objective, "shift") || icontains(req->objective, "handoff") ||
        icontains(req->objective, "live") || icontains(req->objective, "call")) {
        add_booster(plan, "segment");
        add_booster(plan, "speechloop");
        add_booster(plan, "tone");
    }

    if (icontains(req->surface, "pages")) {
        add_surface(plan, "bonfyre-render");
        add_surface(plan, "bonfyre-emit");
    }
    if (icontains(req->surface, "api") || icontains(req->surface, "backend")) {
        add_surface(plan, "bonfyre-api");
        add_surface(plan, "bonfyre-auth");
    }
    if (icontains(req->surface, "jobs") || icontains(req->surface, "queue") || icontains(req->surface, "actions")) {
        add_surface(plan, "bonfyre-queue");
        add_surface(plan, "bonfyre-runtime");
    }
    if (!plan->surface_count) add_surface(plan, "bonfyre-runtime");

    rebalance_boosters(req, plan, have_priors ? &priors : NULL);
    collect_outputs(plan);
    compute_plan_metrics(req, plan);
    apply_frontier_uplift_gate(req, plan);
}

static void escape_json(FILE *fp, const char *text) {
    for (const char *p = text ? text : ""; *p; ++p) {
        if (*p == '\\') fputs("\\\\", fp);
        else if (*p == '"') fputs("\\\"", fp);
        else if (*p == '\n') fputs("\\n", fp);
        else fputc(*p, fp);
    }
}

static void write_registry(FILE *fp) {
    fputc('[', fp);
    for (int i = 0; i < BF_OPERATOR_COUNT; ++i) {
        const BfOperator *op = &BF_OPERATORS[i];
        if (i) fputc(',', fp);
        fprintf(fp, "{\"binary\":\"");
        escape_json(fp, op->binary);
        fprintf(fp, "\",\"layer\":\"");
        escape_json(fp, op->layer);
        fprintf(fp, "\",\"group\":\"");
        escape_json(fp, op->group);
        fprintf(fp, "\"}");
    }
    fputc(']', fp);
}

static void write_domain_weights(FILE *fp, BfDomainWeights w) {
    fprintf(fp,
            "{\"exec\":%.3f,\"artifact\":%.3f,\"tensor\":%.3f,\"cms\":%.3f,\"retrieval\":%.3f,\"value\":%.3f}",
            w.exec, w.artifact, w.tensor, w.cms, w.retrieval, w.value);
}

static void write_state_vector(FILE *fp, OrchestrateStateVector v) {
    fprintf(fp,
            "{\"modality_audio\":%s,\"modality_artifact\":%s,\"modality_text\":%s,"
            "\"surface_pages\":%s,\"surface_api\":%s,\"surface_jobs\":%s,"
            "\"latency_interactive\":%s,\"latency_batch\":%s,"
            "\"objective_publish\":%s,\"objective_retrieval\":%s,\"objective_value\":%s,\"objective_cms\":%s,"
            "\"artifact_local\":%s,\"artifact_structured\":%s}",
            v.modality_audio ? "true" : "false",
            v.modality_artifact ? "true" : "false",
            v.modality_text ? "true" : "false",
            v.surface_pages ? "true" : "false",
            v.surface_api ? "true" : "false",
            v.surface_jobs ? "true" : "false",
            v.latency_interactive ? "true" : "false",
            v.latency_batch ? "true" : "false",
            v.objective_publish ? "true" : "false",
            v.objective_retrieval ? "true" : "false",
            v.objective_value ? "true" : "false",
            v.objective_cms ? "true" : "false",
            v.artifact_local ? "true" : "false",
            v.artifact_structured ? "true" : "false");
}

static void write_baseline_frontier(FILE *fp, const OrchestratePlan *plan) {
    fprintf(fp, "{\"selected_binaries\":[");
    for (int i = 0; i < plan->selected_count; ++i) {
        if (i) fputc(',', fp);
        fprintf(fp, "\"");
        escape_json(fp, BF_OPERATORS[plan->selected[i]].binary);
        fprintf(fp, "\"");
    }
    fprintf(fp, "],\"booster_binaries\":[");
    for (int i = 0; i < plan->booster_count; ++i) {
        if (i) fputc(',', fp);
        fprintf(fp, "\"");
        escape_json(fp, BF_OPERATORS[plan->boosters[i]].binary);
        fprintf(fp, "\"");
    }
    fprintf(fp,
            "],\"predicted_cost\":%.3f,\"predicted_latency\":%.3f,\"predicted_confidence\":%.3f,"
            "\"predicted_reversibility\":%.3f,\"predicted_utility\":%.3f,\"predicted_information_gain\":%.3f,"
            "\"predicted_policy_score\":%.3f}",
            plan->predicted_cost, plan->predicted_latency, plan->predicted_confidence,
            plan->predicted_reversibility, plan->predicted_utility, plan->predicted_information_gain,
            plan->predicted_policy_score);
}

static char *slurp(FILE *fp) {
    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    if (!buf) return NULL;
    int ch;
    while ((ch = fgetc(fp)) != EOF) {
        if (len + 2 >= cap) {
            cap *= 2;
            char *next = realloc(buf, cap);
            if (!next) {
                free(buf);
                return NULL;
            }
            buf = next;
        }
        buf[len++] = (char)ch;
    }
    buf[len] = '\0';
    return buf;
}

static int shell_safe(const char *text) {
    if (!text) return 0;
    for (const char *p = text; *p; ++p) {
        if (!(isalnum((unsigned char)*p) || *p == ':' || *p == '/' || *p == '.' || *p == '-' || *p == '_' || *p == '?'
              || *p == '=' || *p == '&' || *p == '%')) return 0;
    }
    return 1;
}

static int plan_stable_improvement(const OrchestratePlan *baseline, const OrchestratePlan *candidate) {
    if (!baseline || !candidate) return 0;
    if (candidate->predicted_policy_score < baseline->predicted_policy_score + 0.015) return 0;
    if (candidate->predicted_latency > baseline->predicted_latency + 0.08) return 0;
    if (candidate->predicted_cost > baseline->predicted_cost + 0.08) return 0;
    if (candidate->predicted_confidence + 0.02 < baseline->predicted_confidence) return 0;
    if (candidate->predicted_reversibility + 0.03 < baseline->predicted_reversibility) return 0;
    return 1;
}

static void adopt_model_boosters(const OrchestrateRequest *req, OrchestratePlan *plan, const char *response) {
    if (!response) return;
    OrchestratePlan baseline = *plan;
    OrchestratePlan candidate = *plan;
    int added = 0;
    for (int i = 0; i < BF_OPERATOR_COUNT; ++i) {
        if (icontains(response, BF_OPERATORS[i].binary) || icontains(response, BF_OPERATORS[i].name)) {
            int before = candidate.booster_count;
            add_booster(&candidate, BF_OPERATORS[i].binary);
            if (candidate.booster_count != before) added = 1;
        }
    }
    if (!added) return;
    DistilledPriors priors;
    int have_priors = load_distilled_priors(req, &priors);
    rebalance_boosters(req, &candidate, have_priors ? &priors : NULL);
    collect_outputs(&candidate);
    compute_plan_metrics(req, &candidate);
    apply_frontier_uplift_gate(req, &candidate);
    if (!plan_stable_improvement(&baseline, &candidate)) return;
    *plan = candidate;
    copy_text(plan->mode, sizeof(plan->mode), "gemma4-delta");
}

static void maybe_call_model(const OrchestrateRequest *req, OrchestratePlan *plan) {
    const char *endpoint = getenv("BONFYRE_ORCHESTRATE_ENDPOINT");
    const char *api_key = getenv("BONFYRE_ORCHESTRATE_API_KEY");
    if (plan->predicted_information_gain < 0.45 || plan->predicted_confidence > 0.78) return;
    if (!endpoint || !endpoint[0] || !shell_safe(endpoint)) return;
    OrchestrateStateVector sv = request_state_vector(req);
    BfDomainWeights w = objective_weights(req);
    char state_key[64];
    build_state_key(req, state_key, sizeof(state_key));

    char request_path[] = "/tmp/bonfyre-orchestrate-XXXXXX";
    int fd = mkstemp(request_path);
    if (fd < 0) return;
    FILE *fp = fdopen(fd, "w");
    if (!fp) return;

    fprintf(fp, "{\"model\":\"");
    escape_json(fp, plan->model);
    fprintf(fp, "\",\"temperature\":0.1,\"response_format\":{\"type\":\"json_object\"},\"messages\":[");
    fprintf(fp, "{\"role\":\"system\",\"content\":\"");
    escape_json(fp, SYSTEM_PROMPT);
    fprintf(fp, "\"},{\"role\":\"user\",\"content\":\"state_key=");
    escape_json(fp, state_key);
    fprintf(fp, ";objective_family=");
    escape_json(fp, objective_family(req));
    fprintf(fp, ";state_vector=");
    write_state_vector(fp, sv);
    fprintf(fp, ";active_domain_weights=");
    write_domain_weights(fp, w);
    fprintf(fp, ";baseline_frontier=");
    write_baseline_frontier(fp, plan);
    fprintf(fp, ";stability_gate={\\\"min_policy_gain\\\":0.015,\\\"max_latency_delta\\\":0.080,\\\"max_cost_delta\\\":0.080,\\\"max_confidence_drop\\\":0.020,\\\"max_reversibility_drop\\\":0.030};operators=");
    write_registry(fp);
    fprintf(fp, "\"}]}");
    fclose(fp);

    char cmd[4096];
    if (api_key && api_key[0] && shell_safe(api_key)) {
        snprintf(cmd, sizeof(cmd),
                 "curl -sS -X POST '%s' -H 'Content-Type: application/json' -H 'Authorization: Bearer %s' --data-binary @%s",
                 endpoint, api_key, request_path);
    } else {
        snprintf(cmd, sizeof(cmd),
                 "curl -sS -X POST '%s' -H 'Content-Type: application/json' --data-binary @%s",
                 endpoint, request_path);
    }

    FILE *pipe = popen(cmd, "r");
    unlink(request_path);
    if (!pipe) return;
    char *response = slurp(pipe);
    pclose(pipe);
    if (response) {
        adopt_model_boosters(req, plan, response);
        free(response);
    }
}

static void print_plan(const OrchestrateRequest *req, const OrchestratePlan *plan) {
    BfDomainWeights w = objective_weights(req);
    OrchestrateStateVector sv = request_state_vector(req);
    UpliftGate gate = adaptive_uplift_gate(req);
    const char *family = objective_family(req);
    const char *policy_source = policy_source_for_mode(plan->mode);
    char state_key[64];
    build_state_key(req, state_key, sizeof(state_key));
    printf("{\n");
    printf("  \"mode\": \"%s\",\n", plan->mode);
    printf("  \"policy_source\": \"%s\",\n", policy_source);
    printf("  \"model\": \"%s\",\n", plan->model);
    printf("  \"input_type\": \"%s\",\n", req->input_type);
    printf("  \"objective_family\": \"%s\",\n", family);
    printf("  \"state_key\": \"%s\",\n", state_key);
    printf("  \"objective\": \"%s\",\n", req->objective);
    printf("  \"latency_class\": \"%s\",\n", req->latency_class);
    printf("  \"surface\": \"%s\",\n", req->surface);
    printf("  \"selected_binaries\": [");
    for (int i = 0; i < plan->selected_count; ++i) {
        if (i) printf(", ");
        printf("\"%s\"", BF_OPERATORS[plan->selected[i]].binary);
    }
    printf("],\n");
    printf("  \"booster_binaries\": [");
    for (int i = 0; i < plan->booster_count; ++i) {
        if (i) printf(", ");
        printf("\"%s\"", BF_OPERATORS[plan->boosters[i]].binary);
    }
    printf("],\n");
    printf("  \"booster_contributions\": [");
    for (int i = 0; i < plan->booster_count; ++i) {
        if (i) printf(", ");
        printf("{\"binary\":\"%s\",\"score\":%.3f}", BF_OPERATORS[plan->boosters[i]].binary, plan->booster_scores[i]);
    }
    printf("],\n");
    printf("  \"control_surfaces\": [");
    for (int i = 0; i < plan->surface_count; ++i) {
        if (i) printf(", ");
        printf("\"%s\"", plan->surfaces[i]);
    }
    printf("],\n");
    printf("  \"expected_outputs\": [");
    for (int i = 0; i < plan->output_count; ++i) {
        if (i) printf(", ");
        printf("\"%s\"", plan->outputs[i]);
    }
    printf("],\n");
    printf("  \"predicted_cost\": %.3f,\n", plan->predicted_cost);
    printf("  \"predicted_latency\": %.3f,\n", plan->predicted_latency);
    printf("  \"predicted_confidence\": %.3f,\n", plan->predicted_confidence);
    printf("  \"predicted_reversibility\": %.3f,\n", plan->predicted_reversibility);
    printf("  \"predicted_utility\": %.3f,\n", plan->predicted_utility);
    printf("  \"predicted_information_gain\": %.3f,\n", plan->predicted_information_gain);
    printf("  \"predicted_policy_score\": %.3f,\n", plan->predicted_policy_score);
    printf("  \"baseline_frontier_metrics\": {\n");
    printf("    \"cost\": %.3f,\n", plan->baseline_cost);
    printf("    \"latency\": %.3f,\n", plan->baseline_latency);
    printf("    \"confidence\": %.3f,\n", plan->baseline_confidence);
    printf("    \"reversibility\": %.3f,\n", plan->baseline_reversibility);
    printf("    \"utility\": %.3f,\n", plan->baseline_utility);
    printf("    \"information_gain\": %.3f,\n", plan->baseline_information_gain);
    printf("    \"policy_score\": %.3f\n", plan->baseline_policy_score);
    printf("  },\n");
    printf("  \"frontier_uplift\": {\n");
    printf("    \"policy_score\": %.3f,\n", plan->uplift_policy_score);
    printf("    \"latency\": %.3f,\n", plan->uplift_latency);
    printf("    \"cost\": %.3f,\n", plan->uplift_cost);
    printf("    \"confidence\": %.3f,\n", plan->uplift_confidence);
    printf("    \"reversibility\": %.3f,\n", plan->uplift_reversibility);
    printf("    \"utility\": %.3f,\n", plan->uplift_utility);
    printf("    \"information_gain\": %.3f\n", plan->uplift_information_gain);
    printf("  },\n");
    printf("  \"frontier_decision\": {\n");
    printf("    \"decision\": \"%s\",\n", plan->frontier_decision);
    printf("    \"pre_gate_boosters\": %d,\n", plan->pre_gate_booster_count);
    printf("    \"retained_boosters\": %d\n", plan->booster_count);
    printf("  },\n");
    printf("  \"active_domain_weights\": {\n");
    printf("    \"exec\": %.3f,\n", w.exec);
    printf("    \"artifact\": %.3f,\n", w.artifact);
    printf("    \"tensor\": %.3f,\n", w.tensor);
    printf("    \"cms\": %.3f,\n", w.cms);
    printf("    \"retrieval\": %.3f,\n", w.retrieval);
    printf("    \"value\": %.3f\n", w.value);
    printf("  },\n");
    printf("  \"state_vector\": {\n");
    printf("    \"modality_audio\": %s,\n", sv.modality_audio ? "true" : "false");
    printf("    \"modality_artifact\": %s,\n", sv.modality_artifact ? "true" : "false");
    printf("    \"modality_text\": %s,\n", sv.modality_text ? "true" : "false");
    printf("    \"surface_pages\": %s,\n", sv.surface_pages ? "true" : "false");
    printf("    \"surface_api\": %s,\n", sv.surface_api ? "true" : "false");
    printf("    \"surface_jobs\": %s,\n", sv.surface_jobs ? "true" : "false");
    printf("    \"latency_interactive\": %s,\n", sv.latency_interactive ? "true" : "false");
    printf("    \"latency_batch\": %s,\n", sv.latency_batch ? "true" : "false");
    printf("    \"objective_publish\": %s,\n", sv.objective_publish ? "true" : "false");
    printf("    \"objective_retrieval\": %s,\n", sv.objective_retrieval ? "true" : "false");
    printf("    \"objective_value\": %s,\n", sv.objective_value ? "true" : "false");
    printf("    \"objective_cms\": %s,\n", sv.objective_cms ? "true" : "false");
    printf("    \"artifact_local\": %s,\n", sv.artifact_local ? "true" : "false");
    printf("    \"artifact_structured\": %s\n", sv.artifact_structured ? "true" : "false");
    printf("  },\n");
    printf("  \"stability_gate\": {\n");
    printf("    \"min_policy_gain\": %.3f,\n", 0.015);
    printf("    \"max_latency_delta\": %.3f,\n", 0.080);
    printf("    \"max_cost_delta\": %.3f,\n", 0.080);
    printf("    \"max_confidence_drop\": %.3f,\n", 0.020);
    printf("    \"max_reversibility_drop\": %.3f\n", 0.030);
    printf("  },\n");
    printf("  \"uplift_gate\": {\n");
    printf("    \"min_policy_gain\": %.3f,\n", gate.min_policy_gain);
    printf("    \"max_latency_delta\": %.3f,\n", gate.max_latency_delta);
    printf("    \"max_cost_delta\": %.3f,\n", gate.max_cost_delta);
    printf("    \"min_utility_gain\": %.3f\n", gate.min_utility_gain);
    printf("  }\n");
    printf("}\n");
}

static int command_status(void) {
    const char *endpoint = getenv("BONFYRE_ORCHESTRATE_ENDPOINT");
    const char *model = getenv("BONFYRE_ORCHESTRATE_MODEL");
    printf("{\"status\":\"ok\",\"binary\":\"bonfyre-orchestrate\",\"operators\":%d,"
           "\"endpoint_configured\":%s,\"model\":\"%s\",\"machine_only\":true,\"human_prompting\":false}\n",
           BF_OPERATOR_COUNT,
           (endpoint && endpoint[0]) ? "true" : "false",
           (model && model[0]) ? model : DEFAULT_MODEL);
    return 0;
}

static int command_plan(const char *path) {
    OrchestrateRequest req;
    if (load_request(path, &req) != 0) {
        fprintf(stderr, "Failed to read request file: %s\n", path);
        return 1;
    }
    OrchestratePlan plan;
    init_plan(&plan, getenv("BONFYRE_ORCHESTRATE_MODEL"));
    heuristic_plan(&req, &plan);
    if (!load_policy_memory(&req, &plan)) {
        maybe_call_model(&req, &plan);
    }
    save_policy_memory(&req, &plan);
    print_plan(&req, &plan);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        usage();
        return 1;
    }
    if (strcmp(argv[1], "status") == 0) return command_status();
    if (strcmp(argv[1], "plan") == 0 && argc >= 3) return command_plan(argv[2]);
    if (strcmp(argv[1], "feedback") == 0 && argc >= 4) return command_feedback(argv[2], argv[3], argc >= 5 ? argv[4] : NULL);
    if (strcmp(argv[1], "help") == 0 || strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0) {
        usage();
        return 0;
    }
    usage();
    return 1;
}
