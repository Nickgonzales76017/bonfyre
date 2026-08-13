#include "bonfyre.h"
#include "bf_json.h"
#include "bonfyre/bf_discipl.h"

#include <sqlite3.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int json_appendf(char **buf, size_t *len, size_t *cap, const char *fmt, ...);
static char *json_quote(const char *s);

typedef struct {
    const char *src;
    const char *dst;
    const char *relationship;
    const char *directionality;
    const char *required_bridge;
    double success_prior;
    double transform_cost;
    double semantic_loss;
    int retryable;
    int decomposable;
    const char *verification_family;
    const char *preconditions_json;
    const char *postconditions_json;
    const char *failure_modes_json;
} DisciplContractSpec;

static const DisciplContractSpec BF_DISCIPL_CONTRACT_SPECS[] = {
    { "T_MOE_ROUTER", "T_MOE_EXPERT", "routes_to", "directed", "T_ROUTER_EXPERT_BINDING", 0.65, 0.20, 0.05, 1, 1, "T_VERIFY", "[\"router_present\"]", "[\"expert_dispatch_ready\"]", "[\"bridge_missing\",\"expert_not_found\"]" },
    { "T_PROJECTOR_BRIDGE", "T_VISION_PATCH", "projects_from", "directed", "projector_bridge", 0.60, 0.15, 0.06, 1, 1, "T_VERIFY", "[\"vision_tokens_present\"]", "[\"projectable_tokens\"]", "[\"projector_shape_mismatch\"]" },
    { "T_PROJECTOR_BRIDGE", "T_SHARED_QK", "projects_into", "directed", "projector_bridge", 0.60, 0.15, 0.06, 1, 1, "T_VERIFY", "[\"decoder_attention_ready\"]", "[\"decoder_inputs_bound\"]", "[\"attention_binding_failed\"]" },
    { "T_EMBED_POOL", "T_RETRIEVAL_HEAD", "pools_for", "directed", "T_EMBED_RETRIEVAL_BINDING", 0.60, 0.12, 0.04, 1, 1, "T_VERIFY", "[\"embedding_ready\"]", "[\"retrieval_head_bound\"]", "[\"pool_shape_mismatch\"]" },
    { "T_POLICY_ROUTE", "T_SAFETY_HEAD", "routes_to", "directed", "T_POLICY_SAFETY_BINDING", 0.58, 0.10, 0.05, 1, 1, "T_VERIFY", "[\"policy_signal_present\"]", "[\"safety_head_bound\"]", "[\"policy_missing\"]" },
    { "T_AUDIO_MODEL", "T_MODAL_FUSION", "feeds_fusion", "directed", "T_AUDIO_FUSION_BINDING", 0.55, 0.11, 0.05, 1, 1, "T_VERIFY", "[\"audio_latents_present\"]", "[\"fusion_ready\"]", "[\"audio_not_aligned\"]" },
    { "T_DIFFUSION_UNET", "T_TEXT_ENCODER", "conditioned_by", "directed", "T_DIFFUSION_TEXT_BINDING", 0.62, 0.18, 0.06, 1, 1, "T_VERIFY", "[\"text_condition_present\"]", "[\"unet_conditioned\"]", "[\"conditioning_missing\"]" },
    { "T_KV_CACHE", "T_SHARED_QK", "caches_for", "directed", "cache_layout_bridge", 0.52, 0.09, 0.04, 1, 1, "T_VERIFY", "[\"cache_layout_present\"]", "[\"attention_cache_bound\"]", "[\"cache_mismatch\"]" },
    { "T_LATENCY_ROUTE", "T_MOE_ROUTER", "constrains", "bidirectional", "T_LATENCY_ROUTER_BINDING", 0.50, 0.08, 0.04, 1, 1, "T_VERIFY", "[\"latency_profile_present\"]", "[\"router_constrained\"]", "[\"latency_budget_missed\"]" },

    { "T_AUDIO_MODEL", "T_AUDIO_GENERATOR", "generates_audio", "directed", "T_AUDIO_GENERATION_BINDING", 0.70, 0.10, 0.03, 1, 1, "T_VERIFY", "[]", "[]", "[\"generator_missing\"]" },
    { "T_AUDIO_GENERATOR", "T_SAMPLE_OUTPUT", "emits_samples", "directed", "T_SAMPLE_EMIT_BINDING", 0.72, 0.08, 0.02, 1, 1, "T_VERIFY", "[]", "[]", "[\"sample_output_missing\"]" },
    { "T_SAMPLE_OUTPUT", "T_LATENT_SPACE", "encodes_to_latent", "directed", "T_SAMPLE_LATENT_BINDING", 0.63, 0.12, 0.05, 1, 1, "T_VERIFY", "[]", "[]", "[\"latent_projection_missing\"]" },
    { "T_LATENT_SPACE", "T_DIFFUSION_UNET", "denoised_by", "directed", "T_LATENT_DIFFUSION_BINDING", 0.66, 0.15, 0.05, 1, 1, "T_VERIFY", "[]", "[]", "[\"unet_missing\"]" },
    { "T_DIFFUSION_UNET", "T_VIDEO_OUTPUT", "renders_video", "directed", "T_DIFFUSION_VIDEO_BINDING", 0.68, 0.16, 0.04, 1, 1, "T_VERIFY", "[]", "[]", "[\"video_output_missing\"]" },
    { "T_VISION_GROUNDING", "T_OBJECT_DETECTOR", "grounds_objects", "directed", "T_GROUNDING_DETECT_BINDING", 0.67, 0.11, 0.04, 1, 1, "T_VERIFY", "[]", "[]", "[\"detector_missing\"]" },
    { "T_GRAPH_STRUCTURE", "T_PLANNER", "plans_over", "directed", "T_GRAPH_PLAN_BINDING", 0.64, 0.09, 0.03, 1, 1, "T_VERIFY", "[]", "[]", "[\"planner_missing\"]" },
    { "T_PLANNER", "T_EXECUTION", "executes_as", "directed", "T_PLAN_EXEC_BINDING", 0.69, 0.13, 0.03, 1, 1, "T_VERIFY", "[]", "[]", "[\"execution_missing\"]" },

    { "T_QUEUE_JOB", "T_EXECUTION", "dispatches_to", "directed", "T_QUEUE_EXEC_BINDING", 0.74, 0.05, 0.01, 1, 1, "T_VERIFY", "[]", "[]", "[\"queue_worker_missing\"]" },
    { "T_EXECUTION", "T_LEDGER_EVENT", "records_value", "directed", "T_EXEC_LEDGER_BINDING", 0.71, 0.04, 0.01, 1, 1, "T_VERIFY", "[]", "[]", "[\"ledger_write_failed\"]" },
    { "T_METER_EVENT", "T_LEDGER_EVENT", "settles_into", "directed", "T_METER_LEDGER_BINDING", 0.73, 0.04, 0.01, 1, 1, "T_VERIFY", "[]", "[]", "[\"meter_event_missing\"]" },
    { "T_LEDGER_EVENT", "T_VALUE_CAPTURE", "realizes_value", "directed", "T_LEDGER_VALUE_BINDING", 0.76, 0.04, 0.01, 1, 1, "T_VERIFY", "[]", "[]", "[\"value_capture_missing\"]" },
    { "T_GRAPH_STRUCTURE", "T_VERIFY", "verifies_with", "directed", "T_GRAPH_VERIFY_BINDING", 0.69, 0.05, 0.02, 1, 1, "T_VERIFY", "[]", "[]", "[\"verification_path_missing\"]" },
    { "T_ALERT", "T_RESPONSE", "triggers", "directed", "T_ALERT_RESPONSE_BINDING", 0.68, 0.03, 0.01, 1, 1, "T_VERIFY", "[]", "[]", "[\"response_missing\"]" },
    { "T_WORKFLOW_STAGE", "T_QUEUE_JOB", "enqueues", "directed", "T_WORKFLOW_QUEUE_BINDING", 0.72, 0.04, 0.01, 1, 1, "T_VERIFY", "[]", "[]", "[\"queue_missing\"]" },
    { "T_PIPELINE_STAGE", "T_ARTIFACT_OUTPUT", "emits", "directed", "T_PIPELINE_OUTPUT_BINDING", 0.75, 0.03, 0.01, 1, 1, "T_VERIFY", "[]", "[]", "[\"output_missing\"]" },
    { "T_CAPABILITY", "T_ACTOR_SELECTION", "selects_actor", "directed", "T_CAPABILITY_ACTOR_BINDING", 0.70, 0.03, 0.01, 1, 1, "T_VERIFY", "[]", "[]", "[\"actor_selection_missing\"]" },
    { "T_TELEMETRY_EVENT", "T_DIAGNOSTIC", "diagnoses", "directed", "T_TELEMETRY_DIAGNOSTIC_BINDING", 0.70, 0.03, 0.01, 1, 1, "T_VERIFY", "[]", "[]", "[\"diagnostic_missing\"]" },

    { "T_S2_CELL", "T_RASTER_FEATURE", "rasterizes_to", "directed", "T_S2_RASTER_BINDING", 0.74, 0.06, 0.02, 1, 1, "T_VERIFY", "[]", "[]", "[\"s2_partition_missing\"]" },
    { "T_BUILT_ENVIRONMENT", "T_RASTER_FEATURE", "counted_into", "directed", "T_BUILT_ENV_RASTER_BINDING", 0.72, 0.05, 0.02, 1, 1, "T_VERIFY", "[]", "[]", "[\"built_env_missing\"]" },
    { "T_RASTER_FEATURE", "T_MASKED_AUTOENCODER", "encoded_by", "directed", "T_RASTER_MAE_BINDING", 0.73, 0.07, 0.03, 1, 1, "T_VERIFY", "[]", "[]", "[\"mae_missing\"]" },
    { "T_MASKED_AUTOENCODER", "T_GEOSPATIAL_EMBED", "emits_embedding", "directed", "T_MAE_GEO_EMBED_BINDING", 0.76, 0.08, 0.03, 1, 1, "T_VERIFY", "[]", "[]", "[\"embedding_missing\"]" },
    { "T_GEOSPATIAL_EMBED", "T_SOCIOECONOMIC_HEAD", "predicts_socioeconomic", "directed", "T_GEO_SOCIO_BINDING", 0.72, 0.05, 0.03, 1, 1, "T_VERIFY", "[]", "[]", "[\"socio_head_missing\"]" },
    { "T_GEOSPATIAL_EMBED", "T_ENVIRONMENTAL_HEAD", "predicts_environmental", "directed", "T_GEO_ENV_BINDING", 0.70, 0.05, 0.03, 1, 1, "T_VERIFY", "[]", "[]", "[\"env_head_missing\"]" },
    { "T_GEOSPATIAL_EMBED", "T_MULTIMODAL_FUSION", "fuses_with_imagery", "directed", "T_GEO_IMAGE_FUSION_BINDING", 0.68, 0.06, 0.04, 1, 1, "T_VERIFY", "[]", "[]", "[\"fusion_missing\"]" },
    { NULL, NULL, NULL, NULL, NULL, 0.0, 0.0, 0.0, 0, 0, NULL, NULL, NULL, NULL }
};

int bf_discipl_contracts_json(const char *family_filter, char **out_json) {
    char *buf = NULL;
    size_t len = 0, cap = 0;
    int count = 0;
    char *qfilter = NULL;
    if (!out_json) return 1;
    *out_json = NULL;
    qfilter = json_quote(family_filter ? family_filter : "");
    if (!qfilter) return 1;
    if (json_appendf(&buf, &len, &cap, "{\n  \"family_filter\": %s,\n  \"contracts\": [", qfilter) != 0) {
        free(qfilter);
        free(buf);
        return 1;
    }
    free(qfilter);
    for (int i = 0; BF_DISCIPL_CONTRACT_SPECS[i].src; i++) {
        const DisciplContractSpec *spec = &BF_DISCIPL_CONTRACT_SPECS[i];
        char *qsrc = NULL, *qdst = NULL, *qrel = NULL, *qdir = NULL, *qbridge = NULL, *qvf = NULL;
        if (family_filter && family_filter[0] &&
            strcmp(family_filter, spec->src) != 0 &&
            strcmp(family_filter, spec->dst) != 0 &&
            strcmp(family_filter, spec->required_bridge) != 0) {
            continue;
        }
        qsrc = json_quote(spec->src);
        qdst = json_quote(spec->dst);
        qrel = json_quote(spec->relationship);
        qdir = json_quote(spec->directionality);
        qbridge = json_quote(spec->required_bridge);
        qvf = json_quote(spec->verification_family ? spec->verification_family : "");
        if (!qsrc || !qdst || !qrel || !qdir || !qbridge || !qvf) {
            free(qsrc); free(qdst); free(qrel); free(qdir); free(qbridge); free(qvf); free(buf);
            return 1;
        }
        if (json_appendf(&buf, &len, &cap,
                         "%s\n    {\"src_family\":%s,\"dst_family\":%s,\"relationship\":%s,"
                         "\"directionality\":%s,\"required_bridge\":%s,\"success_prior\":%.2f,"
                         "\"transform_cost\":%.2f,\"semantic_loss\":%.2f,\"verification_family\":%s}",
                         count ? "," : "", qsrc, qdst, qrel, qdir, qbridge,
                         spec->success_prior, spec->transform_cost, spec->semantic_loss, qvf) != 0) {
            free(qsrc); free(qdst); free(qrel); free(qdir); free(qbridge); free(qvf); free(buf);
            return 1;
        }
        free(qsrc); free(qdst); free(qrel); free(qdir); free(qbridge); free(qvf);
        count++;
    }
    if (json_appendf(&buf, &len, &cap, "\n  ],\n  \"count\": %d\n}\n", count) != 0) {
        free(buf);
        return 1;
    }
    *out_json = buf;
    return 0;
}

static void discipl_now_iso(char out[32]) {
    time_t now = time(NULL);
    struct tm tmv;
    gmtime_r(&now, &tmv);
    strftime(out, 32, "%Y-%m-%dT%H:%M:%SZ", &tmv);
}

static int json_appendf(char **buf, size_t *len, size_t *cap, const char *fmt, ...) {
    va_list ap;
    int need;
    char *next;
    if (!buf || !len || !cap) return 1;
    if (!*buf) {
        *cap = 1024;
        *len = 0;
        *buf = (char *)malloc(*cap);
        if (!*buf) return 1;
        (*buf)[0] = '\0';
    }
    while (1) {
        va_start(ap, fmt);
        need = vsnprintf(*buf + *len, *cap - *len, fmt, ap);
        va_end(ap);
        if (need < 0) return 1;
        if ((size_t)need < *cap - *len) {
            *len += (size_t)need;
            return 0;
        }
        *cap = (*cap * 2) + (size_t)need + 32;
        next = (char *)realloc(*buf, *cap);
        if (!next) return 1;
        *buf = next;
    }
}

static char *json_quote(const char *s) {
    char *esc;
    char *out;
    size_t n;
    esc = sqlite3_mprintf("%q", s ? s : "");
    if (!esc) return NULL;
    n = strlen(esc);
    out = (char *)malloc(n + 3);
    if (!out) { sqlite3_free(esc); return NULL; }
    out[0] = '"';
    memcpy(out + 1, esc, n);
    out[n + 1] = '"';
    out[n + 2] = '\0';
    sqlite3_free(esc);
    return out;
}

static int json_copy_str(const bf_json_doc_t *doc, const bf_json_node_t *obj,
                         const char *key, char *out, size_t out_sz) {
    const bf_json_node_t *n = bf_json_obj_get(doc, obj, key);
    if (!n) {
        if (out_sz) out[0] = '\0';
        return 0;
    }
    return bf_json_get_str_copy(n, out, out_sz) > 0;
}

static int collect_array_items(const bf_json_doc_t *doc, const bf_json_node_t *arr,
                               char items[][256], int max_items) {
    int n = 0;
    for (const bf_json_node_t *child = arr ? bf_json_child_first(doc, arr) : NULL;
         child && n < max_items; child = bf_json_child_next(doc, child)) {
        if (bf_json_get_str_copy(child, items[n], sizeof(items[n])) > 0) n++;
    }
    return n;
}

static uint64_t capability_mask_for(const char *cap) {
    if (!cap || !cap[0]) return 0;
    if (strstr(cap, "embedding")) return 1ull << 0;
    if (strstr(cap, "reason")) return 1ull << 1;
    if (strstr(cap, "prediction")) return 1ull << 2;
    if (strstr(cap, "fusion")) return 1ull << 3;
    if (strstr(cap, "economic")) return 1ull << 4;
    if (strstr(cap, "queue")) return 1ull << 5;
    if (strstr(cap, "graph")) return 1ull << 6;
    if (strstr(cap, "verify")) return 1ull << 7;
    if (strstr(cap, "execute")) return 1ull << 8;
    if (strstr(cap, "diagnostic")) return 1ull << 9;
    return 1ull << 10;
}

static int discipl_open_db(const char *root, sqlite3 **out_db) {
    char path[4096];
    if (!out_db) return 1;
    *out_db = NULL;
    if (bf_layer_state_db_path(root, "discipl.db", path, sizeof(path)) != 0) return 1;
    return bf_sqlite3_open(path, out_db) == SQLITE_OK ? 0 : 1;
}

static int discipl_init_schema(sqlite3 *db) {
    const char *sql =
        "CREATE TABLE IF NOT EXISTS discipl_actors ("
        " actor_id TEXT PRIMARY KEY,"
        " actor_type TEXT,"
        " family TEXT,"
        " domain TEXT,"
        " modality TEXT,"
        " capabilities_json TEXT,"
        " role_affordances_json TEXT,"
        " confidence REAL,"
        " uncertainty REAL,"
        " cost REAL,"
        " latency REAL,"
        " source_ref TEXT,"
        " created_at TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS discipl_contracts ("
        " contract_id TEXT PRIMARY KEY,"
        " src_family TEXT,"
        " dst_family TEXT,"
        " relationship TEXT,"
        " directionality TEXT,"
        " required_bridge TEXT,"
        " preconditions_json TEXT,"
        " postconditions_json TEXT,"
        " failure_modes_json TEXT,"
        " success_prior REAL,"
        " transform_cost REAL,"
        " semantic_loss REAL,"
        " created_at TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS discipl_chains ("
        " chain_id TEXT PRIMARY KEY,"
        " goal TEXT,"
        " families_json TEXT,"
        " artifacts_json TEXT,"
        " bridges_json TEXT,"
        " materialization_order_json TEXT,"
        " score REAL,"
        " global_confidence REAL,"
        " semantic_drift REAL,"
        " status TEXT,"
        " created_at TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS discipl_loops ("
        " loop_id TEXT PRIMARY KEY,"
        " parent_loop_id TEXT,"
        " goal TEXT,"
        " boss_actor_id TEXT,"
        " followers_json TEXT,"
        " candidate_chains_json TEXT,"
        " active_chain_id TEXT,"
        " recursion_depth INTEGER,"
        " status TEXT,"
        " score REAL,"
        " uncertainty REAL,"
        " created_at TEXT,"
        " updated_at TEXT"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_discipl_actors_family ON discipl_actors(family);"
        "CREATE INDEX IF NOT EXISTS idx_discipl_contracts_srcdst ON discipl_contracts(src_family, dst_family);";
    return sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK ? 0 : 1;
}

static const DisciplContractSpec *discipl_find_spec(const char *src, const char *dst) {
    for (int i = 0; BF_DISCIPL_CONTRACT_SPECS[i].src; i++) {
        if (strcmp(BF_DISCIPL_CONTRACT_SPECS[i].src, src) == 0 &&
            strcmp(BF_DISCIPL_CONTRACT_SPECS[i].dst, dst) == 0) {
            return &BF_DISCIPL_CONTRACT_SPECS[i];
        }
    }
    return NULL;
}

static void actor_type_name(bf_discipl_actor_type_t t, char *out, size_t out_sz) {
    const char *name = "service";
    switch (t) {
        case BF_DISCIPL_ACTOR_LAYER: name = "layer"; break;
        case BF_DISCIPL_ACTOR_COMMAND: name = "command"; break;
        case BF_DISCIPL_ACTOR_PIPELINE_STAGE: name = "pipeline_stage"; break;
        case BF_DISCIPL_ACTOR_BRIDGE: name = "bridge"; break;
        case BF_DISCIPL_ACTOR_SERVICE: name = "service"; break;
        case BF_DISCIPL_ACTOR_VALUE_ACTOR: name = "value_actor"; break;
        case BF_DISCIPL_ACTOR_GRAPH_ACTOR: name = "graph_actor"; break;
        case BF_DISCIPL_ACTOR_QUEUE_ACTOR: name = "queue_actor"; break;
    }
    snprintf(out, out_sz, "%s", name);
}

int bf_discipl_actor_from_layer_json(const char *layer_json, bf_discipl_actor_t *out_actor) {
    char err[128], domain[128] = "ai", modality[128] = "generic";
    char family_items[16][256], caps[32][256];
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root = NULL;
    int n_caps;
    if (!layer_json || !out_actor) return 1;
    memset(out_actor, 0, sizeof(*out_actor));
    doc = bf_json_parse_str(layer_json, err, sizeof(err));
    if (!doc) return 1;
    root = bf_json_root(doc);
    json_copy_str(doc, root, "artifact_id", out_actor->id, sizeof(out_actor->id));
    json_copy_str(doc, root, "artifact_id", out_actor->source_artifact_id, sizeof(out_actor->source_artifact_id));
    out_actor->actor_type = BF_DISCIPL_ACTOR_LAYER;
    if (collect_array_items(doc, bf_json_obj_get(doc, root, "families"), family_items, 16) > 0) {
        snprintf(out_actor->family, sizeof(out_actor->family), "%s", family_items[0]);
    }
    if (strstr(out_actor->family, "GEO") || strstr(out_actor->family, "S2")) {
        snprintf(domain, sizeof(domain), "geospatial");
        snprintf(modality, sizeof(modality), "geospatial");
    } else if (strstr(out_actor->family, "AUDIO")) {
        snprintf(domain, sizeof(domain), "audio");
        snprintf(modality, sizeof(modality), "audio");
    } else if (strstr(out_actor->family, "VISION")) {
        snprintf(domain, sizeof(domain), "vision");
        snprintf(modality, sizeof(modality), "vision");
    } else if (strstr(out_actor->family, "LEDGER") || strstr(out_actor->family, "VALUE")) {
        snprintf(domain, sizeof(domain), "value");
        snprintf(modality, sizeof(modality), "economic");
    }
    snprintf(out_actor->domain, sizeof(out_actor->domain), "%s", domain);
    snprintf(out_actor->modality, sizeof(out_actor->modality), "%s", modality);
    n_caps = collect_array_items(doc, bf_json_obj_get(doc, root, "capabilities"), caps, 32);
    for (int i = 0; i < n_caps; i++) out_actor->capabilities_bitset |= capability_mask_for(caps[i]);
    out_actor->affordances.can_follow = 1;
    out_actor->affordances.can_verify = 1;
    out_actor->affordances.can_route = strstr(out_actor->family, "ROUTER") != NULL || strstr(out_actor->family, "ROUTE") != NULL;
    out_actor->affordances.can_execute = 1;
    out_actor->affordances.can_boss = out_actor->affordances.can_route || strstr(out_actor->family, "PLANNER") != NULL;
    out_actor->confidence = 0.60;
    out_actor->uncertainty = 0.25;
    out_actor->cost = 0.05;
    out_actor->latency = 0.10;
    bf_json_free(doc);
    return 0;
}

int bf_discipl_contract_from_family_relation(const char *src_family,
                                             const char *dst_family,
                                             bf_discipl_contract_t *out_contract) {
    const DisciplContractSpec *spec;
    if (!src_family || !dst_family || !out_contract) return 1;
    memset(out_contract, 0, sizeof(*out_contract));
    spec = discipl_find_spec(src_family, dst_family);
    if (!spec) return 1;
    snprintf(out_contract->name, sizeof(out_contract->name), "%s__%s", src_family, dst_family);
    snprintf(out_contract->src_family, sizeof(out_contract->src_family), "%s", spec->src);
    snprintf(out_contract->dst_family, sizeof(out_contract->dst_family), "%s", spec->dst);
    snprintf(out_contract->relationship, sizeof(out_contract->relationship), "%s", spec->relationship);
    snprintf(out_contract->directionality, sizeof(out_contract->directionality), "%s", spec->directionality);
    snprintf(out_contract->preconditions_json, sizeof(out_contract->preconditions_json), "%s", spec->preconditions_json);
    snprintf(out_contract->postconditions_json, sizeof(out_contract->postconditions_json), "%s", spec->postconditions_json);
    snprintf(out_contract->failure_modes_json, sizeof(out_contract->failure_modes_json), "%s", spec->failure_modes_json);
    snprintf(out_contract->required_bridge, sizeof(out_contract->required_bridge), "%s", spec->required_bridge);
    snprintf(out_contract->verification_family, sizeof(out_contract->verification_family), "%s", spec->verification_family);
    out_contract->success_prior = spec->success_prior;
    out_contract->transform_cost = spec->transform_cost;
    out_contract->semantic_loss = spec->semantic_loss;
    out_contract->retryable = spec->retryable;
    out_contract->decomposable = spec->decomposable;
    return 0;
}

int bf_discipl_chain_from_stitch_plan_json(const char *plan_json, bf_discipl_chain_program_t *out_chain) {
    char err[128], digest[65];
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root = NULL, *components = NULL, *bridges = NULL, *conf = NULL;
    if (!plan_json || !out_chain) return 1;
    memset(out_chain, 0, sizeof(*out_chain));
    doc = bf_json_parse_str(plan_json, err, sizeof(err));
    if (!doc) return 1;
    root = bf_json_root(doc);
    components = bf_json_obj_get(doc, root, "components");
    if (!components) {
        const bf_json_node_t *base = bf_json_obj_get(doc, root, "base_plan");
        if (base) {
            components = bf_json_obj_get(doc, base, "components");
            bridges = bf_json_obj_get(doc, base, "bridge_requirements");
            conf = bf_json_obj_get(doc, base, "confidence");
        }
    }
    if (!bridges) bridges = bf_json_obj_get(doc, root, "bridge_requirements");
    if (!conf) conf = bf_json_obj_get(doc, root, "confidence");
    bf_sha256_hex((const uint8_t *)plan_json, strlen(plan_json), digest);
    snprintf(out_chain->chain_id, sizeof(out_chain->chain_id), "discipl_chain:%.*s", 40, digest);
    snprintf(out_chain->goal, sizeof(out_chain->goal), "stitch-plan-chain");
    snprintf(out_chain->status, sizeof(out_chain->status), "proposed");
    snprintf(out_chain->materialization_policy, sizeof(out_chain->materialization_policy), "metadata-first");
    out_chain->global_confidence = conf ? bf_json_get_double(conf) : 0.50;
    for (const bf_json_node_t *child = components ? bf_json_child_first(doc, components) : NULL;
         child && out_chain->edge_count < BF_DISCIPL_MAX_ITEMS; child = bf_json_child_next(doc, child)) {
        if (bf_json_get_str_copy(child, out_chain->artifact_ids[out_chain->edge_count], sizeof(out_chain->artifact_ids[0])) > 0) {
            snprintf(out_chain->families[out_chain->edge_count], sizeof(out_chain->families[0]), "unknown");
            snprintf(out_chain->per_hop_status[out_chain->edge_count], sizeof(out_chain->per_hop_status[0]), "proposed");
            out_chain->per_hop_confidence[out_chain->edge_count] = out_chain->global_confidence;
            out_chain->edge_count++;
        }
    }
    for (int i = 0; i < BF_DISCIPL_MAX_ITEMS; i++) {
        const bf_json_node_t *child = NULL;
        int idx = 0;
        for (child = bridges ? bf_json_child_first(doc, bridges) : NULL;
             child; child = bf_json_child_next(doc, child), idx++) {
            if (idx == i && bf_json_get_str_copy(child, out_chain->bridge_requirements[i], sizeof(out_chain->bridge_requirements[0])) > 0)
                break;
        }
    }
    bf_json_free(doc);
    return 0;
}

double bf_discipl_llamppl_score_hook(const bf_discipl_chain_program_t *chain,
                                     const bf_discipl_contract_t *contracts,
                                     int contract_count) {
    (void)chain;
    (void)contracts;
    (void)contract_count;
    return 0.0;
}

double bf_discipl_chain_score(bf_discipl_chain_program_t *chain,
                              const bf_discipl_contract_t *contracts,
                              int contract_count) {
    double conf = 1.0;
    double drift = 0.0;
    double cost = 0.0;
    double floor = 0.35;
    int hops = chain ? chain->edge_count : 0;
    if (!chain) return 0.0;
    for (int i = 0; i < hops; i++) {
        double hop = chain->per_hop_confidence[i] > 0.0 ? chain->per_hop_confidence[i] : 0.5;
        conf *= hop;
    }
    if (conf < floor && hops > 0) conf = floor;
    for (int i = 0; i < contract_count; i++) {
        drift += contracts[i].semantic_loss;
        cost += contracts[i].transform_cost;
    }
    chain->global_confidence = conf;
    chain->semantic_drift = drift;
    chain->accumulated_cost = cost;
    return conf - drift - (cost / (1.0 + (double)hops)) + bf_discipl_llamppl_score_hook(chain, contracts, contract_count);
}

int bf_discipl_loop_init(const char *goal, const bf_discipl_chain_program_t *seed_chain, bf_discipl_loop_t *out_loop) {
    char seed[512], digest[65];
    if (!goal || !out_loop) return 1;
    memset(out_loop, 0, sizeof(*out_loop));
    snprintf(seed, sizeof(seed), "%s|%s", goal, seed_chain ? seed_chain->chain_id : "none");
    bf_sha256_hex((const uint8_t *)seed, strlen(seed), digest);
    snprintf(out_loop->loop_id, sizeof(out_loop->loop_id), "discipl_loop:%.*s", 40, digest);
    snprintf(out_loop->goal, sizeof(out_loop->goal), "%s", goal);
    snprintf(out_loop->status, sizeof(out_loop->status), "proposed");
    if (seed_chain && seed_chain->chain_id[0]) {
        snprintf(out_loop->candidate_chain_ids[0], sizeof(out_loop->candidate_chain_ids[0]), "%s", seed_chain->chain_id);
        snprintf(out_loop->active_chain_id, sizeof(out_loop->active_chain_id), "%s", seed_chain->chain_id);
        out_loop->candidate_chain_count = 1;
        out_loop->convergence_score = seed_chain->global_confidence;
        out_loop->uncertainty = 1.0 - seed_chain->global_confidence;
    }
    out_loop->cost_budget = 1.0;
    discipl_now_iso(out_loop->created_at);
    discipl_now_iso(out_loop->updated_at);
    return 0;
}

int bf_discipl_loop_spawn_subloop(const bf_discipl_loop_t *parent,
                                  const char *goal,
                                  const bf_discipl_chain_program_t *seed_chain,
                                  bf_discipl_loop_t *out_loop) {
    if (!parent || !goal || !out_loop) return 1;
    if (bf_discipl_loop_init(goal, seed_chain, out_loop) != 0) return 1;
    snprintf(out_loop->parent_loop_id, sizeof(out_loop->parent_loop_id), "%s", parent->loop_id);
    out_loop->recursion_depth = parent->recursion_depth + 1;
    snprintf(out_loop->status, sizeof(out_loop->status), "running");
    return 0;
}

int bf_discipl_loop_assign_roles(bf_discipl_loop_t *loop,
                                 const bf_discipl_actor_t *actors,
                                 int actor_count) {
    if (!loop || !actors || actor_count <= 0) return 1;
    for (int i = 0; i < actor_count; i++) {
        if (!loop->boss_actor_id[0] && actors[i].affordances.can_boss) {
            snprintf(loop->boss_actor_id, sizeof(loop->boss_actor_id), "%s", actors[i].id);
            continue;
        }
        if (loop->follower_count < BF_DISCIPL_MAX_FOLLOWERS && actors[i].affordances.can_follow) {
            snprintf(loop->follower_actor_ids[loop->follower_count],
                     sizeof(loop->follower_actor_ids[0]), "%s", actors[i].id);
            loop->follower_count++;
        }
    }
    if (!loop->boss_actor_id[0]) snprintf(loop->boss_actor_id, sizeof(loop->boss_actor_id), "%s", actors[0].id);
    if (!loop->status[0] || strcmp(loop->status, "proposed") == 0) snprintf(loop->status, sizeof(loop->status), "running");
    discipl_now_iso(loop->updated_at);
    return 0;
}

int bf_discipl_loop_verify(const bf_discipl_loop_t *loop,
                           const bf_discipl_chain_program_t *chains,
                           int chain_count,
                           char *out_json,
                           size_t out_json_sz) {
    int has_active = 0;
    if (!loop || !out_json || out_json_sz == 0) return 1;
    for (int i = 0; i < chain_count; i++) {
        if (strcmp(loop->active_chain_id, chains[i].chain_id) == 0) { has_active = 1; break; }
    }
    snprintf(out_json, out_json_sz,
             "{\n  \"loop_id\": \"%s\",\n  \"status\": \"%s\",\n  \"boss_actor_id\": \"%s\",\n  \"active_chain_present\": %s,\n  \"follower_count\": %d\n}",
             loop->loop_id, loop->status, loop->boss_actor_id, has_active ? "true" : "false", loop->follower_count);
    return 0;
}

int bf_discipl_loop_to_json(const bf_discipl_loop_t *loop, char **out_json) {
    char *buf = NULL;
    size_t len = 0, cap = 0;
    if (!loop || !out_json) return 1;
    if (json_appendf(&buf, &len, &cap,
                     "{\n  \"loop_id\": \"%s\",\n  \"parent_loop_id\": \"%s\",\n  \"goal\": \"%s\",\n  \"boss_actor_id\": \"%s\",\n  \"active_chain_id\": \"%s\",\n  \"recursion_depth\": %d,\n  \"convergence_score\": %.2f,\n  \"uncertainty\": %.2f,\n  \"cost_budget\": %.2f,\n  \"status\": \"%s\",\n  \"created_at\": \"%s\",\n  \"updated_at\": \"%s\"\n}",
                     loop->loop_id, loop->parent_loop_id, loop->goal, loop->boss_actor_id,
                     loop->active_chain_id, loop->recursion_depth, loop->convergence_score,
                     loop->uncertainty, loop->cost_budget, loop->status, loop->created_at, loop->updated_at) != 0) {
        free(buf);
        return 1;
    }
    *out_json = buf;
    return 0;
}

int bf_discipl_chain_to_json(const bf_discipl_chain_program_t *chain, char **out_json) {
    char *buf = NULL;
    size_t len = 0, cap = 0;
    if (!chain || !out_json) return 1;
    if (json_appendf(&buf, &len, &cap,
                     "{\n  \"chain_id\": \"%s\",\n  \"goal\": \"%s\",\n  \"edge_count\": %d,\n  \"global_confidence\": %.2f,\n  \"accumulated_cost\": %.2f,\n  \"semantic_drift\": %.2f,\n  \"materialization_policy\": \"%s\",\n  \"status\": \"%s\",\n  \"families\": [",
                     chain->chain_id, chain->goal, chain->edge_count, chain->global_confidence,
                     chain->accumulated_cost, chain->semantic_drift, chain->materialization_policy, chain->status) != 0) {
        free(buf);
        return 1;
    }
    for (int i = 0; i < chain->edge_count; i++) {
        char *q = json_quote(chain->families[i]);
        if (!q) { free(buf); return 1; }
        if (json_appendf(&buf, &len, &cap, "%s%s", i ? "," : "", q) != 0) { free(q); free(buf); return 1; }
        free(q);
    }
    if (json_appendf(&buf, &len, &cap, "],\n  \"artifact_ids\": [") != 0) { free(buf); return 1; }
    for (int i = 0; i < chain->edge_count; i++) {
        char *q = json_quote(chain->artifact_ids[i]);
        if (!q) { free(buf); return 1; }
        if (json_appendf(&buf, &len, &cap, "%s%s", i ? "," : "", q) != 0) { free(q); free(buf); return 1; }
        free(q);
    }
    if (json_appendf(&buf, &len, &cap, "],\n  \"bridge_requirements\": [") != 0) { free(buf); return 1; }
    for (int i = 0; i < chain->edge_count; i++) {
        char *q = json_quote(chain->bridge_requirements[i]);
        if (!q) { free(buf); return 1; }
        if (json_appendf(&buf, &len, &cap, "%s%s", i ? "," : "", q) != 0) { free(q); free(buf); return 1; }
        free(q);
    }
    if (json_appendf(&buf, &len, &cap, "],\n  \"materialization_order\": [") != 0) { free(buf); return 1; }
    for (int i = 0; i < chain->edge_count; i++) {
        char *q = json_quote(chain->artifact_ids[i]);
        if (!q) { free(buf); return 1; }
        if (json_appendf(&buf, &len, &cap, "%s%s", i ? "," : "", q) != 0) { free(q); free(buf); return 1; }
        free(q);
    }
    if (json_appendf(&buf, &len, &cap, "],\n  \"per_hop_status\": [") != 0) { free(buf); return 1; }
    for (int i = 0; i < chain->edge_count; i++) {
        char *q = json_quote(chain->per_hop_status[i]);
        if (!q) { free(buf); return 1; }
        if (json_appendf(&buf, &len, &cap, "%s%s", i ? "," : "", q) != 0) { free(q); free(buf); return 1; }
        free(q);
    }
    if (json_appendf(&buf, &len, &cap, "],\n  \"per_hop_confidence\": [") != 0) { free(buf); return 1; }
    for (int i = 0; i < chain->edge_count; i++) {
        if (json_appendf(&buf, &len, &cap, "%s%.2f", i ? "," : "", chain->per_hop_confidence[i]) != 0) { free(buf); return 1; }
    }
    if (json_appendf(&buf, &len, &cap, "]\n}") != 0) { free(buf); return 1; }
    *out_json = buf;
    return 0;
}

int bf_discipl_init_db(const char *root) {
    sqlite3 *db = NULL;
    if (discipl_open_db(root, &db) != 0) return 1;
    if (discipl_init_schema(db) != 0) { sqlite3_close(db); return 1; }
    sqlite3_close(db);
    return 0;
}

int bf_discipl_tables_exist(const char *root) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (discipl_open_db(root, &db) != 0) return 0;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('discipl_actors','discipl_contracts','discipl_chains','discipl_loops')", -1, &st, NULL) == SQLITE_OK) {
        if (sqlite3_step(st) == SQLITE_ROW) ok = sqlite3_column_int(st, 0) == 4;
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    return ok;
}

int bf_discipl_upsert_actor(const char *root, const bf_discipl_actor_t *actor) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char actor_type[64], caps_json[64], roles_json[256], source_ref[512], created_at[32];
    if (!actor) return 1;
    if (bf_discipl_init_db(root) != 0) return 1;
    if (discipl_open_db(root, &db) != 0) return 1;
    actor_type_name(actor->actor_type, actor_type, sizeof(actor_type));
    snprintf(caps_json, sizeof(caps_json), "[\"0x%llx\"]", (unsigned long long)actor->capabilities_bitset);
    snprintf(roles_json, sizeof(roles_json),
             "{\"can_boss\":%s,\"can_follow\":%s,\"can_verify\":%s,\"can_decompose\":%s,\"can_route\":%s,\"can_price\":%s,\"can_execute\":%s}",
             actor->affordances.can_boss ? "true" : "false",
             actor->affordances.can_follow ? "true" : "false",
             actor->affordances.can_verify ? "true" : "false",
             actor->affordances.can_decompose ? "true" : "false",
             actor->affordances.can_route ? "true" : "false",
             actor->affordances.can_price ? "true" : "false",
             actor->affordances.can_execute ? "true" : "false");
    snprintf(source_ref, sizeof(source_ref), "%s%s%s",
             actor->source_artifact_id,
             actor->source_artifact_id[0] && actor->source_command[0] ? " | " : "",
             actor->source_command);
    discipl_now_iso(created_at);
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO discipl_actors(actor_id,actor_type,family,domain,modality,capabilities_json,role_affordances_json,confidence,uncertainty,cost,latency,source_ref,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 1; }
    sqlite3_bind_text(st,1,actor->id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,actor_type,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,actor->family,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,4,actor->domain,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,5,actor->modality,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,6,caps_json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,7,roles_json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_double(st,8,actor->confidence);
    sqlite3_bind_double(st,9,actor->uncertainty);
    sqlite3_bind_double(st,10,actor->cost);
    sqlite3_bind_double(st,11,actor->latency);
    sqlite3_bind_text(st,12,source_ref,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,13,created_at,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) { sqlite3_finalize(st); sqlite3_close(db); return 1; }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

int bf_discipl_upsert_contract(const char *root, const bf_discipl_contract_t *contract) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char created_at[32], contract_id[256];
    if (!contract) return 1;
    if (bf_discipl_init_db(root) != 0) return 1;
    if (discipl_open_db(root, &db) != 0) return 1;
    snprintf(contract_id, sizeof(contract_id), "%s__%s", contract->src_family, contract->dst_family);
    discipl_now_iso(created_at);
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO discipl_contracts(contract_id,src_family,dst_family,relationship,directionality,required_bridge,preconditions_json,postconditions_json,failure_modes_json,success_prior,transform_cost,semantic_loss,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 1; }
    sqlite3_bind_text(st,1,contract_id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,contract->src_family,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,contract->dst_family,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,4,contract->relationship,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,5,contract->directionality,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,6,contract->required_bridge,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,7,contract->preconditions_json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,8,contract->postconditions_json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,9,contract->failure_modes_json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_double(st,10,contract->success_prior);
    sqlite3_bind_double(st,11,contract->transform_cost);
    sqlite3_bind_double(st,12,contract->semantic_loss);
    sqlite3_bind_text(st,13,created_at,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) { sqlite3_finalize(st); sqlite3_close(db); return 1; }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

int bf_discipl_upsert_chain(const char *root, const bf_discipl_chain_program_t *chain) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char *json = NULL;
    char created_at[32];
    if (!chain) return 1;
    if (bf_discipl_init_db(root) != 0) return 1;
    if (discipl_open_db(root, &db) != 0) return 1;
    if (bf_discipl_chain_to_json(chain, &json) != 0) { sqlite3_close(db); return 1; }
    discipl_now_iso(created_at);
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO discipl_chains(chain_id,goal,families_json,artifacts_json,bridges_json,materialization_order_json,score,global_confidence,semantic_drift,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        -1, &st, NULL) != SQLITE_OK) { free(json); sqlite3_close(db); return 1; }
    sqlite3_bind_text(st,1,chain->chain_id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,chain->goal,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,4,json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,5,json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,6,json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_double(st,7,chain->global_confidence - chain->semantic_drift - chain->accumulated_cost);
    sqlite3_bind_double(st,8,chain->global_confidence);
    sqlite3_bind_double(st,9,chain->semantic_drift);
    sqlite3_bind_text(st,10,chain->status,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,11,created_at,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) { free(json); sqlite3_finalize(st); sqlite3_close(db); return 1; }
    free(json);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

int bf_discipl_upsert_loop(const char *root, const bf_discipl_loop_t *loop) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char followers[4096] = "[", chains[4096] = "[";
    size_t lf = 1, cf = 1;
    if (!loop) return 1;
    if (bf_discipl_init_db(root) != 0) return 1;
    if (discipl_open_db(root, &db) != 0) return 1;
    for (int i = 0; i < loop->follower_count; i++) {
        char *q = json_quote(loop->follower_actor_ids[i]);
        if (!q) continue;
        snprintf(followers + lf, sizeof(followers) - lf, "%s%s", i ? "," : "", q);
        lf = strlen(followers);
        free(q);
    }
    snprintf(followers + lf, sizeof(followers) - lf, "]");
    for (int i = 0; i < loop->candidate_chain_count; i++) {
        char *q = json_quote(loop->candidate_chain_ids[i]);
        if (!q) continue;
        snprintf(chains + cf, sizeof(chains) - cf, "%s%s", i ? "," : "", q);
        cf = strlen(chains);
        free(q);
    }
    snprintf(chains + cf, sizeof(chains) - cf, "]");
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO discipl_loops(loop_id,parent_loop_id,goal,boss_actor_id,followers_json,candidate_chains_json,active_chain_id,recursion_depth,status,score,uncertainty,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 1; }
    sqlite3_bind_text(st,1,loop->loop_id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,loop->parent_loop_id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,loop->goal,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,4,loop->boss_actor_id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,5,followers,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,6,chains,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,7,loop->active_chain_id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(st,8,loop->recursion_depth);
    sqlite3_bind_text(st,9,loop->status,-1,SQLITE_TRANSIENT);
    sqlite3_bind_double(st,10,loop->convergence_score);
    sqlite3_bind_double(st,11,loop->uncertainty);
    sqlite3_bind_text(st,12,loop->created_at,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,13,loop->updated_at,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) { sqlite3_finalize(st); sqlite3_close(db); return 1; }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

int bf_discipl_load_chain(const char *root, const char *chain_id, bf_discipl_chain_program_t *out_chain) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    if (!chain_id || !out_chain) return 1;
    memset(out_chain, 0, sizeof(*out_chain));
    if (discipl_open_db(root, &db) != 0) return 1;
    if (sqlite3_prepare_v2(db, "SELECT goal,global_confidence,semantic_drift,status FROM discipl_chains WHERE chain_id=?", -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 1; }
    sqlite3_bind_text(st,1,chain_id,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); sqlite3_close(db); return 1; }
    snprintf(out_chain->chain_id, sizeof(out_chain->chain_id), "%s", chain_id);
    snprintf(out_chain->goal, sizeof(out_chain->goal), "%s", sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "");
    out_chain->global_confidence = sqlite3_column_double(st,1);
    out_chain->semantic_drift = sqlite3_column_double(st,2);
    snprintf(out_chain->status, sizeof(out_chain->status), "%s", sqlite3_column_text(st,3) ? (const char *)sqlite3_column_text(st,3) : "");
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

int bf_discipl_load_loop(const char *root, const char *loop_id, bf_discipl_loop_t *out_loop) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    if (!loop_id || !out_loop) return 1;
    memset(out_loop, 0, sizeof(*out_loop));
    if (discipl_open_db(root, &db) != 0) return 1;
    if (sqlite3_prepare_v2(db, "SELECT parent_loop_id,goal,boss_actor_id,active_chain_id,recursion_depth,status,score,uncertainty,created_at,updated_at FROM discipl_loops WHERE loop_id=?", -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 1; }
    sqlite3_bind_text(st,1,loop_id,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); sqlite3_close(db); return 1; }
    snprintf(out_loop->loop_id, sizeof(out_loop->loop_id), "%s", loop_id);
    snprintf(out_loop->parent_loop_id, sizeof(out_loop->parent_loop_id), "%s", sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "");
    snprintf(out_loop->goal, sizeof(out_loop->goal), "%s", sqlite3_column_text(st,1) ? (const char *)sqlite3_column_text(st,1) : "");
    snprintf(out_loop->boss_actor_id, sizeof(out_loop->boss_actor_id), "%s", sqlite3_column_text(st,2) ? (const char *)sqlite3_column_text(st,2) : "");
    snprintf(out_loop->active_chain_id, sizeof(out_loop->active_chain_id), "%s", sqlite3_column_text(st,3) ? (const char *)sqlite3_column_text(st,3) : "");
    out_loop->recursion_depth = sqlite3_column_int(st,4);
    snprintf(out_loop->status, sizeof(out_loop->status), "%s", sqlite3_column_text(st,5) ? (const char *)sqlite3_column_text(st,5) : "");
    out_loop->convergence_score = sqlite3_column_double(st,6);
    out_loop->uncertainty = sqlite3_column_double(st,7);
    snprintf(out_loop->created_at, sizeof(out_loop->created_at), "%s", sqlite3_column_text(st,8) ? (const char *)sqlite3_column_text(st,8) : "");
    snprintf(out_loop->updated_at, sizeof(out_loop->updated_at), "%s", sqlite3_column_text(st,9) ? (const char *)sqlite3_column_text(st,9) : "");
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}
