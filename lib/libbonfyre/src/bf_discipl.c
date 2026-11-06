#include <stdint.h>
#include "bonfyre.h"
#include "bf_json.h"
#include "bonfyre/bf_discipl.h"

#include <sqlite3.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

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

/* Command grammar used to be hand-written into DisCIPL by Python.  It belongs
 * beside the native contract calculator: the arrays are facts about command
 * input/output kinds and edges are derived by set intersection below. */
typedef struct {
    const char *name;
    const char *binary;
    const char *const *consumes;
    const char *const *produces;
    int can_verify;
} DisciplCommandSpec;

#define KINDS(name, ...) static const char *const name[] = { __VA_ARGS__, NULL }
KINDS(k_file_url_audio_text_image, "file", "url", "audio", "text", "image");
KINDS(k_artifact, "artifact"); KINDS(k_artifact_code, "artifact", "code");
KINDS(k_artifact_observation, "artifact", "observation"); KINDS(k_audio, "audio");
KINDS(k_transcript, "transcript"); KINDS(k_transcript_clean, "transcript", "clean_transcript");
KINDS(k_brief_text, "brief", "text"); KINDS(k_brief, "brief");
KINDS(k_brief_artifact, "brief", "artifact"); KINDS(k_artifact_brief_rendered, "artifact", "brief", "rendered");
KINDS(k_text_clean, "text", "clean_transcript"); KINDS(k_embedding, "embedding");
KINDS(k_index, "search_index", "vector_index"); KINDS(k_model_tensor, "model", "tensor");
KINDS(k_compressed, "compressed"); KINDS(k_execution, "execution");
KINDS(k_artifact_execution_deliverable, "artifact", "execution", "deliverable");
KINDS(k_meter, "meter_event"); KINDS(k_artifact_brief_deliverable, "artifact", "brief", "deliverable");
KINDS(p_artifact, "artifact"); KINDS(p_canonical_structural, "canonical", "structural_id");
KINDS(p_content_id, "content_id"); KINDS(p_lineage, "lineage"); KINDS(p_identity, "identity");
KINDS(p_transcript, "transcript"); KINDS(p_clean_transcript, "clean_transcript"); KINDS(p_segments, "segments");
KINDS(p_tone, "tone_profile"); KINDS(p_brief, "brief"); KINDS(p_audio, "audio"); KINDS(p_social, "social");
KINDS(p_rendered, "rendered"); KINDS(p_deliverable, "deliverable"); KINDS(p_embedding, "embedding");
KINDS(p_vector_index, "vector_index"); KINDS(p_search_index, "search_index"); KINDS(p_results, "results");
KINDS(p_tags, "tags"); KINDS(p_composite, "composite"); KINDS(p_pack, "pack"); KINDS(p_compressed, "compressed");
KINDS(p_execution, "execution"); KINDS(p_kv_state, "kv_state"); KINDS(p_meter_event, "meter_event");
KINDS(p_accounting, "accounting"); KINDS(p_proof, "proof");
#undef KINDS

static const DisciplCommandSpec BF_DISCIPL_COMMAND_SPECS[] = {
    {"Ingest", "bonfyre-ingest", k_file_url_audio_text_image, p_artifact, 0},
    {"Canon", "bonfyre-canon", k_artifact_code, p_canonical_structural, 1},
    {"Hash", "bonfyre-hash", k_artifact, p_content_id, 1},
    {"Graph", "bonfyre-graph", k_artifact, p_lineage, 0},
    {"Entity", "bonfyre-entity", k_artifact_observation, p_identity, 0},
    {"Transcribe", "bonfyre-transcribe", k_audio, p_transcript, 0},
    {"TranscriptClean", "bonfyre-transcript-clean", k_transcript, p_clean_transcript, 0},
    {"Segment", "bonfyre-segment", k_transcript_clean, p_segments, 0},
    {"Tone", "bonfyre-tone", k_audio, p_tone, 0},
    {"Brief", "bonfyre-brief", k_transcript_clean, p_brief, 0},
    {"Narrate", "bonfyre-narrate", k_brief_text, p_audio, 0},
    {"Repurpose", "bonfyre-repurpose", k_brief, p_social, 0},
    {"Render", "bonfyre-render", k_brief_artifact, p_rendered, 0},
    {"Emit", "bonfyre-emit", k_artifact_brief_rendered, p_deliverable, 0},
    {"Embed", "bonfyre-embed", k_text_clean, p_embedding, 0},
    {"Vec", "bonfyre-vec", k_embedding, p_vector_index, 0},
    {"Index", "bonfyre-index", k_artifact, p_search_index, 0},
    {"Query", "bonfyre-query", k_index, p_results, 0},
    {"Tag", "bonfyre-tag", k_text_clean, p_tags, 0},
    {"Stitch", "bonfyre-stitch", k_artifact, p_composite, 0},
    {"Pack", "bonfyre-pack", k_artifact, p_pack, 0},
    {"FPQ", "bonfyre-fpq", k_model_tensor, p_compressed, 0},
    {"SLI", "bonfyre-sli", k_compressed, p_execution, 0},
    {"KVCache", "bonfyre-kvcache", k_execution, p_kv_state, 0},
    {"Meter", "bonfyre-meter", k_artifact_execution_deliverable, p_meter_event, 0},
    {"Ledger", "bonfyre-ledger", k_meter, p_accounting, 0},
    {"Proof", "bonfyre-proof", k_artifact_brief_deliverable, p_proof, 1},
    {NULL, NULL, NULL, NULL, 0}
};

static int command_kinds_intersect(const char *const *produces, const char *const *consumes) {
    size_t i, j;
    for (i = 0; produces[i]; ++i)
        for (j = 0; consumes[j]; ++j)
            if (strcmp(produces[i], consumes[j]) == 0) return 1;
    return 0;
}

/* What modality a kind is carried in.
 *
 * Every edge in the command estate used to carry the same semantic_loss of
 * 0.03, which sounds like a small honest number and is in fact the absence of
 * one: when every edge loses the same amount, the chain score is a hop counter
 * wearing a probability's clothes, and the ranking cannot tell a good
 * composition from a long one.
 *
 * The measurement that exposed this is worth keeping. Asked for a chain from a
 * retained document to a tone profile, the search returned
 * text -> Narrate -> audio -> Transcribe -> transcript -> Tone: speak the feed
 * aloud, then listen to what you just said. Every edge is contracted, the
 * search is right, and the composition is worthless. Nothing in the weights
 * could say so.
 *
 * So one fact goes in, and only one: a transform that changes the modality its
 * material is carried in loses more than a transform that does not. Crossing
 * from text to audio is a synthesis and crossing back is a recognition, and
 * both throw away things the other cannot restore. Two tiers, not a
 * calibrated model -- nobody has measured these losses, and a finer-grained
 * table would be inventing precision rather than reporting it. */
typedef enum {
    BF_MODALITY_RECORD = 0,   /* digests, identities, ledgers: projections */
    BF_MODALITY_TEXT,
    BF_MODALITY_AUDIO,
    BF_MODALITY_IMAGE,
    BF_MODALITY_NUMERIC       /* embeddings, tensors: not invertible to text */
} bf_modality_t;

static bf_modality_t discipl_kind_modality(const char *kind)
{
    static const struct { const char *kind; bf_modality_t m; } K[] = {
        {"text", BF_MODALITY_TEXT}, {"transcript", BF_MODALITY_TEXT},
        {"clean_transcript", BF_MODALITY_TEXT}, {"brief", BF_MODALITY_TEXT},
        {"segments", BF_MODALITY_TEXT}, {"social", BF_MODALITY_TEXT},
        {"tags", BF_MODALITY_TEXT}, {"rendered", BF_MODALITY_TEXT},
        {"code", BF_MODALITY_TEXT}, {"deliverable", BF_MODALITY_TEXT},
        {"audio", BF_MODALITY_AUDIO}, {"tone_profile", BF_MODALITY_AUDIO},
        {"image", BF_MODALITY_IMAGE},
        {"embedding", BF_MODALITY_NUMERIC}, {"vector_index", BF_MODALITY_NUMERIC},
        {"model", BF_MODALITY_NUMERIC}, {"tensor", BF_MODALITY_NUMERIC},
        {"compressed", BF_MODALITY_NUMERIC}, {"kv_state", BF_MODALITY_NUMERIC},
        {NULL, BF_MODALITY_RECORD}
    };
    size_t i;
    for (i = 0; K[i].kind; ++i) if (strcmp(K[i].kind, kind) == 0) return K[i].m;
    /* artifact, content_id, lineage, identity, canonical, proof, search_index,
     * results, composite, pack, execution, meter_event, accounting: each is a
     * record about material rather than the material itself. */
    return BF_MODALITY_RECORD;
}

/* The modality a command moves its material into, across everything it
 * declares. A command whose outputs live in a different modality from its
 * inputs is a synthesis or a recognition, and the edge leaving it carries what
 * that costs. */
static double discipl_command_transition_loss(const char *const *consumes,
                                              const char *const *produces)
{
    int crossed = 0;
    size_t i, j;
    for (i = 0; produces[i]; ++i) {
        bf_modality_t out = discipl_kind_modality(produces[i]);
        if (out == BF_MODALITY_RECORD) continue;   /* a record about material */
        for (j = 0; consumes[j]; ++j) {
            bf_modality_t in = discipl_kind_modality(consumes[j]);
            if (in == BF_MODALITY_RECORD) continue;
            if (in != out) crossed = 1;
        }
    }
    return crossed ? 0.15 : 0.03;
}

/* Whether the command behind a family moves material from one modality into
 * another, and which two. A command that only makes a record about material,
 * or stays within one modality, crosses nothing. */
int bf_discipl_family_crossing(const char *family, int *from_modality, int *to_modality)
{
    size_t i, j, k;
    for (i = 0; BF_DISCIPL_COMMAND_SPECS[i].name; ++i) {
        const DisciplCommandSpec *spec = &BF_DISCIPL_COMMAND_SPECS[i];
        char name[128];
        snprintf(name, sizeof(name), "T_%s", spec->name);
        for (char *p = name; *p; ++p) if (*p >= 'a' && *p <= 'z') *p = (char)(*p - ('a' - 'A'));
        if (strcmp(name, family) != 0) continue;
        for (j = 0; spec->produces[j]; ++j) {
            bf_modality_t out = discipl_kind_modality(spec->produces[j]);
            if (out == BF_MODALITY_RECORD) continue;
            for (k = 0; spec->consumes[k]; ++k) {
                bf_modality_t in = discipl_kind_modality(spec->consumes[k]);
                if (in == BF_MODALITY_RECORD || in == out) continue;
                if (from_modality) *from_modality = (int)in;
                if (to_modality) *to_modality = (int)out;
                return 1;
            }
        }
        return 0;
    }
    return 0;
}

int bf_discipl_seed_command_estate(const char *root, unsigned *actor_count,
                                   unsigned *contract_count) {
    char repo_root[PATH_MAX];
    unsigned actors = 0, contracts = 0;
    size_t i, j;
    if (actor_count) *actor_count = 0;
    if (contract_count) *contract_count = 0;
    if (bf_discipl_init_db(root) != 0 || !bf_catalog_find_repo_root(repo_root, sizeof(repo_root))) return 1;
    for (i = 0; BF_DISCIPL_COMMAND_SPECS[i].name; ++i) {
        const DisciplCommandSpec *spec = &BF_DISCIPL_COMMAND_SPECS[i];
        bf_discipl_actor_t actor;
        memset(&actor, 0, sizeof(actor));
        snprintf(actor.id, sizeof(actor.id), "command:%s", spec->name);
        snprintf(actor.family, sizeof(actor.family), "T_%s", spec->name);
        for (char *p = actor.family; *p; ++p) if (*p >= 'a' && *p <= 'z') *p = (char)(*p - ('a' - 'A'));
        actor.actor_type = BF_DISCIPL_ACTOR_COMMAND;
        snprintf(actor.domain, sizeof(actor.domain), "command");
        snprintf(actor.modality, sizeof(actor.modality), "artifact");
        actor.capabilities_bitset = 1ull << 8;
        actor.affordances.can_execute = 1;
        actor.affordances.can_verify = spec->can_verify;
        actor.confidence = 0.8; actor.uncertainty = 0.1; actor.cost = 0.1; actor.latency = 0.1;
        if (snprintf(actor.source_command, sizeof(actor.source_command), "%s/cmd/Bonfyre%s/%s", repo_root, spec->name, spec->binary) >= (int)sizeof(actor.source_command)) return 1;
        if (bf_discipl_upsert_actor(root, &actor) != 0) return 1;
        actors++;
    }
    for (i = 0; BF_DISCIPL_COMMAND_SPECS[i].name; ++i) {
        const DisciplCommandSpec *src = &BF_DISCIPL_COMMAND_SPECS[i];
        for (j = 0; BF_DISCIPL_COMMAND_SPECS[j].name; ++j) {
            const DisciplCommandSpec *dst = &BF_DISCIPL_COMMAND_SPECS[j];
            bf_discipl_contract_t contract;
            if (i == j || !command_kinds_intersect(src->produces, dst->consumes)) continue;
            memset(&contract, 0, sizeof(contract));
            snprintf(contract.src_family, sizeof(contract.src_family), "T_%s", src->name);
            snprintf(contract.dst_family, sizeof(contract.dst_family), "T_%s", dst->name);
            for (char *p = contract.src_family; *p; ++p) if (*p >= 'a' && *p <= 'z') *p = (char)(*p - ('a' - 'A'));
            for (char *p = contract.dst_family; *p; ++p) if (*p >= 'a' && *p <= 'z') *p = (char)(*p - ('a' - 'A'));
            snprintf(contract.relationship, sizeof(contract.relationship), "feeds");
            snprintf(contract.directionality, sizeof(contract.directionality), "directed");
            snprintf(contract.preconditions_json, sizeof(contract.preconditions_json), "[]");
            snprintf(contract.postconditions_json, sizeof(contract.postconditions_json), "[]");
            snprintf(contract.failure_modes_json, sizeof(contract.failure_modes_json), "[]");
            contract.success_prior = 0.7; contract.transform_cost = 0.08;
            contract.semantic_loss = discipl_command_transition_loss(src->consumes, src->produces);
            contract.retryable = 1; contract.decomposable = 1;
            if (bf_discipl_upsert_contract(root, &contract) != 0) return 1;
            contracts++;
        }
    }
    if (actor_count) *actor_count = actors;
    if (contract_count) *contract_count = contracts;
    return 0;
}

/* ------------------------------------------- external estate -> lattice --
 *
 * The estate reads and then stops.
 *
 * Four thousand one hundred and seventy-one observations were attempted and
 * the bodies of the successful ones are on this disk -- a gigabyte of
 * lawfully retained representations, each one a file. Twenty-seven native
 * commands declare what kinds they consume, and the first of them, Ingest,
 * consumes `file`. Every one of those retained bodies has been a legal start
 * node for the whole command lattice since the moment it landed, and not one
 * of them has ever entered it. discipl_chains was empty for a reason that had
 * nothing to do with the chains.
 *
 * Two things were missing and both are structural rather than clever.
 *
 * The first is that a phenotype was never a family. `bonfyre-facet` says what
 * shape an occurrence is; DisCIPL says what transforms a family admits; they
 * share no vocabulary, so the compiler that knows the estate holds a
 * MEASUREMENT_STREAM cannot ask the compiler that knows how to transform
 * things what may be done with one.
 *
 * The second is subtler and is the reason this bridge has the shape it has.
 * A phenotype describes what the *source* offers. It does not describe what
 * this estate *holds*. What this estate holds, uniformly, is a retained body:
 * a file. Binding MEDIA_ASSET straight to `audio` would claim we hold audio
 * when what we hold is whatever the server actually returned, which for a
 * great many of these is an HTML landing page. So every phenotype crosses
 * into the lattice through one honest intermediate -- T_RETAINED_BODY, kind
 * `file` -- and specialisation past that is conformance's job, not this
 * bridge's. The bridge is narrow on purpose; a wider one would be a lie about
 * carriers.
 *
 * The prior on that crossing is measured, not declared. It is the fraction of
 * attempted observations whose body is present on disk *now*, checked by
 * stat() rather than believed from the evidence row that claims retention.
 * Those two numbers disagree -- 3,573 rows claim a retained body and 2,875 of
 * those files exist -- and the honest prior is the smaller one. An estate that
 * priced this edge off its own evidence would be quoting itself. */

typedef struct {
    const char *phenotype;
    const char *family;
} DisciplPhenotypeSpec;

static const DisciplPhenotypeSpec BF_DISCIPL_PHENOTYPE_SPECS[] = {
    {"VERSIONED_RECORD",     "T_PHENOTYPE_VERSIONED_RECORD"},
    {"EVENT",                "T_PHENOTYPE_EVENT"},
    {"LIVE_ENTITY",          "T_PHENOTYPE_LIVE_ENTITY"},
    {"MEASUREMENT_STREAM",   "T_PHENOTYPE_MEASUREMENT_STREAM"},
    {"FIELD_COVERAGE",       "T_PHENOTYPE_FIELD_COVERAGE"},
    {"DATASET",              "T_PHENOTYPE_DATASET"},
    {"MEDIA_ASSET",          "T_PHENOTYPE_MEDIA_ASSET"},
    {"COMMANDABLE_RESOURCE", "T_PHENOTYPE_COMMANDABLE_RESOURCE"},
    {"ECONOMIC_OFFER",       "T_PHENOTYPE_ECONOMIC_OFFER"},
    {"SUPPLY_CHAIN_OBJECT",  "T_PHENOTYPE_SUPPLY_CHAIN_OBJECT"},
    {"IDENTITY_CREDENTIAL",  "T_PHENOTYPE_IDENTITY_CREDENTIAL"},
    {"SERVICE_CAPABILITY",   "T_PHENOTYPE_SERVICE_CAPABILITY"},
    {"DOCUMENT",             "T_PHENOTYPE_DOCUMENT"},
    {"SPATIAL_RESOURCE",     "T_PHENOTYPE_SPATIAL_RESOURCE"},
    {"OBLIGATION",           "T_PHENOTYPE_OBLIGATION"},
    {NULL, NULL}
};

/* Where the command lattice rejoins the value lattice.
 *
 * These two graphs were built separately and never introduced. The command
 * side ends at T_LEDGER, whose binary produces `accounting`. The value side
 * begins at T_METER_EVENT and runs T_METER_EVENT -> T_LEDGER_EVENT ->
 * T_VALUE_CAPTURE. `bonfyre-meter` produces the kind `meter_event`, which is
 * the family T_METER_EVENT spelled in lower case, and nothing joined them, so
 * a chain could reach accounting and could reach value and could never reach
 * one from the other.
 *
 * Nothing here is invented: each row joins a declared output kind to the
 * already-declared family of the same name. */
static const struct { const char *kind; const char *family; } BF_DISCIPL_KIND_FAMILIES[] = {
    {"meter_event", "T_METER_EVENT"},
    {"execution",   "T_EXECUTION"},
    {"artifact",    "T_ARTIFACT_OUTPUT"},
    {NULL, NULL}
};

static int discipl_kinds_contain(const char *const *kinds, const char *want)
{
    size_t i;
    for (i = 0; kinds[i]; ++i)
        if (strcmp(kinds[i], want) == 0) return 1;
    return 0;
}

static int discipl_seed_edge(const char *root, const char *src, const char *dst,
                             const char *relationship, const char *bridge,
                             double prior, double cost, double loss,
                             unsigned *contracts)
{
    bf_discipl_contract_t contract;
    memset(&contract, 0, sizeof(contract));
    snprintf(contract.src_family, sizeof(contract.src_family), "%s", src);
    snprintf(contract.dst_family, sizeof(contract.dst_family), "%s", dst);
    snprintf(contract.relationship, sizeof(contract.relationship), "%s", relationship);
    snprintf(contract.directionality, sizeof(contract.directionality), "directed");
    snprintf(contract.required_bridge, sizeof(contract.required_bridge), "%s", bridge ? bridge : "");
    snprintf(contract.preconditions_json, sizeof(contract.preconditions_json), "[]");
    snprintf(contract.postconditions_json, sizeof(contract.postconditions_json), "[]");
    snprintf(contract.failure_modes_json, sizeof(contract.failure_modes_json), "[]");
    contract.success_prior = prior;
    contract.transform_cost = cost;
    contract.semantic_loss = loss;
    contract.retryable = 1;
    contract.decomposable = 1;
    if (bf_discipl_upsert_contract(root, &contract) != 0) return 1;
    if (contracts) (*contracts)++;
    return 0;
}

int bf_discipl_seed_estate_bridge(const char *root, double retention_rate,
                                  unsigned *actor_count, unsigned *contract_count)
{
    unsigned actors = 0, contracts = 0;
    size_t i;
    if (actor_count) *actor_count = 0;
    if (contract_count) *contract_count = 0;
    /* A measured rate or nothing. Refusing here rather than substituting a
     * default is the point: an unmeasured prior on this edge would make every
     * chain crossing it carry a number nobody took. */
    if (!(retention_rate > 0.0) || retention_rate > 1.0) return 2;
    if (bf_discipl_init_db(root) != 0) return 1;

    for (i = 0; BF_DISCIPL_PHENOTYPE_SPECS[i].phenotype; ++i) {
        bf_discipl_actor_t actor;
        memset(&actor, 0, sizeof(actor));
        snprintf(actor.id, sizeof(actor.id), "phenotype:%s", BF_DISCIPL_PHENOTYPE_SPECS[i].phenotype);
        snprintf(actor.family, sizeof(actor.family), "%s", BF_DISCIPL_PHENOTYPE_SPECS[i].family);
        actor.actor_type = BF_DISCIPL_ACTOR_BRIDGE;
        snprintf(actor.domain, sizeof(actor.domain), "external_estate");
        snprintf(actor.modality, sizeof(actor.modality), "observation");
        actor.affordances.can_follow = 1;
        actor.confidence = retention_rate;
        actor.uncertainty = 1.0 - retention_rate;
        actor.cost = 0.0;
        actor.latency = 0.0;
        snprintf(actor.source_command, sizeof(actor.source_command), "bonfyre-facet compile");
        if (bf_discipl_upsert_actor(root, &actor) != 0) return 1;
        actors++;
        if (discipl_seed_edge(root, BF_DISCIPL_PHENOTYPE_SPECS[i].family, "T_RETAINED_BODY",
                              "retained_as", "foreign_twin_observation",
                              retention_rate, 0.02, 0.00, &contracts) != 0) return 1;
    }

    {
        bf_discipl_actor_t actor;
        memset(&actor, 0, sizeof(actor));
        snprintf(actor.id, sizeof(actor.id), "estate:retained_body");
        snprintf(actor.family, sizeof(actor.family), "T_RETAINED_BODY");
        actor.actor_type = BF_DISCIPL_ACTOR_BRIDGE;
        snprintf(actor.domain, sizeof(actor.domain), "external_estate");
        snprintf(actor.modality, sizeof(actor.modality), "file");
        actor.affordances.can_follow = 1;
        actor.affordances.can_execute = 1;
        actor.confidence = retention_rate;
        actor.uncertainty = 1.0 - retention_rate;
        snprintf(actor.source_command, sizeof(actor.source_command), "bonfyre-foreign-twin observe");
        if (bf_discipl_upsert_actor(root, &actor) != 0) return 1;
        actors++;
    }

    /* A retained body is a file, so it enters wherever `file` is consumed.
     * Derived by the same intersection the command estate uses -- this does
     * not name Ingest, it asks which commands declared that they take one. */
    for (i = 0; BF_DISCIPL_COMMAND_SPECS[i].name; ++i) {
        char dst[128];
        size_t k;
        if (!discipl_kinds_contain(BF_DISCIPL_COMMAND_SPECS[i].consumes, "file")) continue;
        snprintf(dst, sizeof(dst), "T_%s", BF_DISCIPL_COMMAND_SPECS[i].name);
        for (k = 0; dst[k]; ++k) if (dst[k] >= 'a' && dst[k] <= 'z') dst[k] = (char)(dst[k] - ('a' - 'A'));
        /* 0.95, and declared rather than measured: handing a local file to a
         * local binary fails for reasons of encoding and size, not of network
         * or authority, and nobody has counted those yet. */
        if (discipl_seed_edge(root, "T_RETAINED_BODY", dst, "ingested_as", "",
                              0.95, 0.05, 0.02, &contracts) != 0) return 1;
    }

    /* Rejoin the command lattice to the value lattice through the kinds they
     * already share the names of. */
    for (i = 0; BF_DISCIPL_COMMAND_SPECS[i].name; ++i) {
        size_t j;
        for (j = 0; BF_DISCIPL_KIND_FAMILIES[j].kind; ++j) {
            char src[128];
            size_t k;
            if (!discipl_kinds_contain(BF_DISCIPL_COMMAND_SPECS[i].produces,
                                       BF_DISCIPL_KIND_FAMILIES[j].kind)) continue;
            snprintf(src, sizeof(src), "T_%s", BF_DISCIPL_COMMAND_SPECS[i].name);
            for (k = 0; src[k]; ++k) if (src[k] >= 'a' && src[k] <= 'z') src[k] = (char)(src[k] - ('a' - 'A'));
            if (strcmp(src, BF_DISCIPL_KIND_FAMILIES[j].family) == 0) continue;
            if (discipl_seed_edge(root, src, BF_DISCIPL_KIND_FAMILIES[j].family,
                                  "emits_kind", "", 0.90, 0.02, 0.01, &contracts) != 0) return 1;
        }
    }

    if (actor_count) *actor_count = actors;
    if (contract_count) *contract_count = contracts;
    return 0;
}

/* ------------------------------------------------ carrier specialisation --
 *
 * The retained-body bridge above is deliberately narrow, and the measurement
 * shows exactly what that narrowness costs: all fifteen phenotypes reach the
 * same eleven terminals, because every one of them funnels through a single
 * `file` and the phenotype contributes nothing to what follows. A bridge that
 * makes its own input irrelevant is honest about what it knows and useless as
 * a discriminator.
 *
 * What licenses a wider edge is not a better guess about the phenotype. It is
 * the bytes. `bonfyre-facet conformance` reads a retained body and reports the
 * carrier it actually carries -- the third derivation tier, the only one that
 * rests on what was served rather than on what was catalogued. Five hundred
 * and nineteen occurrences in this estate have been through it. Those are not
 * sources that might be feeds; they are bodies that parsed as feeds.
 *
 * So carriers enter the lattice as their own families, and only carriers with
 * a measured population enter at all. The nine probes below cover what the
 * conformance compiler can detect; a carrier nobody has observed gets no
 * family, no edge and no mention, because seeding one would be a prepared
 * answer wearing a measurement's clothes. */

typedef struct {
    const char *carrier;
    const char *const *kinds;
} DisciplCarrierSpec;

/* Every carrier the conformance probes can detect is a structured text
 * document. That is a fact about this particular set -- a feed, a spec, a
 * catalogue record and a sitemap are all text -- and not a claim that carriers
 * in general are. When a probe for a binary carrier is added, this table is
 * where it declares itself, and the intersection below does the rest. */
static const char *const bf_carrier_kinds_text[] = { "text", NULL };

static const DisciplCarrierSpec BF_DISCIPL_CARRIER_SPECS[] = {
    {"RSS",      bf_carrier_kinds_text},
    {"Atom",     bf_carrier_kinds_text},
    {"OpenAPI",  bf_carrier_kinds_text},
    {"STAC",     bf_carrier_kinds_text},
    {"DCAT",     bf_carrier_kinds_text},
    {"RO-Crate", bf_carrier_kinds_text},
    {"IIIF",     bf_carrier_kinds_text},
    {"Sitemap",  bf_carrier_kinds_text},
    {NULL, NULL}
};

/* How to actually invoke a command, for the commands whose invocation has been
 * checked by running them.
 *
 * Every edge in this lattice was a contract and none of them had ever run a
 * binary, which makes the whole graph a claim about what could happen. What
 * stops it being one is executing it, and executing it needs an argv.
 *
 * An argv cannot be derived from the family name. `bonfyre-hash` wants
 * `file <path>`, `bonfyre-ingest` wants `<input> <outdir>`, `bonfyre-tag` wants
 * a trained model it has no way to ask for. So this table holds only the forms
 * that were checked by invoking them and looking at what came back; everything
 * else is BF_INVOKE_UNBOUND and the runner stops there and says so. A guessed
 * argv that happens to exit zero is worse than a refusal, because it would let
 * the receipt claim a hop ran when nothing meaningful happened. */
/* Every command family and its binary, so a prober can walk them.
 *
 * bf_discipl_command_invocation answers for the four whose argv was checked by
 * hand. That is the wrong scale: 23 of 27 commands were unbound and the plan
 * was to check them one at a time, which is the sort of task that stays on a
 * list for a year. The commands already describe themselves -- every one of
 * them prints a usage block -- so the estate can derive its own invocations
 * and then prove them by running them. */
int bf_discipl_command_at(int index, char *family, size_t family_sz,
                          char *binary, size_t binary_sz)
{
    char repo_root[PATH_MAX];
    int n = 0;
    size_t k;
    while (BF_DISCIPL_COMMAND_SPECS[n].name) n++;
    if (index < 0 || index >= n) return 1;
    if (!bf_catalog_find_repo_root(repo_root, sizeof(repo_root))) return 1;
    if (snprintf(family, family_sz, "T_%s", BF_DISCIPL_COMMAND_SPECS[index].name) >= (int)family_sz)
        return 1;
    for (k = 0; family[k]; ++k)
        if (family[k] >= 'a' && family[k] <= 'z') family[k] = (char)(family[k] - ('a' - 'A'));
    if (snprintf(binary, binary_sz, "%s/cmd/Bonfyre%s/%s", repo_root,
                 BF_DISCIPL_COMMAND_SPECS[index].name,
                 BF_DISCIPL_COMMAND_SPECS[index].binary) >= (int)binary_sz)
        return 1;
    return 0;
}

/* What the command declares it produces, so a prober can prefer the call that
 * makes that rather than any call that exits zero. */
const char *bf_discipl_command_produces(int index, int kind_index)
{
    int n = 0, k;
    while (BF_DISCIPL_COMMAND_SPECS[n].name) n++;
    if (index < 0 || index >= n || kind_index < 0) return NULL;
    for (k = 0; BF_DISCIPL_COMMAND_SPECS[index].produces[k]; k++)
        if (k == kind_index) return BF_DISCIPL_COMMAND_SPECS[index].produces[k];
    return NULL;
}

int bf_discipl_command_total(void)
{
    int n = 0;
    while (BF_DISCIPL_COMMAND_SPECS[n].name) n++;
    return n;
}

int bf_discipl_command_invocation(const char *family, char *binary, size_t binary_sz)
{
    static const struct { const char *family; const char *bin; int form; } INVOCATIONS[] = {
        {"T_INGEST", "bonfyre-ingest", BF_INVOKE_INPUT_OUTDIR},
        {"T_HASH",   "bonfyre-hash",   BF_INVOKE_FILE_SUBCMD},
        {"T_CANON",  "bonfyre-canon",  BF_INVOKE_PARSE_OUTDIR},
        {"T_EMBED",  "bonfyre-embed",  BF_INVOKE_TEXT_OUT},
        {NULL, NULL, BF_INVOKE_UNBOUND}
    };
    char repo_root[PATH_MAX];
    size_t i;
    if (!family) return BF_INVOKE_UNBOUND;
    for (i = 0; INVOCATIONS[i].family; ++i) {
        if (strcmp(INVOCATIONS[i].family, family) != 0) continue;
        if (!binary || binary_sz == 0) return INVOCATIONS[i].form;
        if (!bf_catalog_find_repo_root(repo_root, sizeof(repo_root))) return BF_INVOKE_UNBOUND;
        {
            const char *name = INVOCATIONS[i].bin;
            const char *camel = family + 2;   /* T_INGEST -> INGEST */
            char dir[128];
            size_t k;
            snprintf(dir, sizeof(dir), "%s", camel);
            for (k = 1; dir[k]; ++k)
                if (dir[k] >= 'A' && dir[k] <= 'Z') dir[k] = (char)(dir[k] + ('a' - 'A'));
            if (snprintf(binary, binary_sz, "%s/cmd/Bonfyre%c%s/%s",
                         repo_root, dir[0], dir + 1, name) >= (int)binary_sz)
                return BF_INVOKE_UNBOUND;
        }
        return INVOCATIONS[i].form;
    }
    return BF_INVOKE_UNBOUND;
}

int bf_discipl_phenotype_count(void)
{
    int n = 0;
    while (BF_DISCIPL_PHENOTYPE_SPECS[n].phenotype) n++;
    return n;
}

const char *bf_discipl_phenotype_name(int index)
{
    if (index < 0 || index >= bf_discipl_phenotype_count()) return NULL;
    return BF_DISCIPL_PHENOTYPE_SPECS[index].phenotype;
}

int bf_discipl_phenotype_index(const char *phenotype)
{
    int i;
    if (!phenotype) return -1;
    for (i = 0; BF_DISCIPL_PHENOTYPE_SPECS[i].phenotype; i++)
        if (strcmp(BF_DISCIPL_PHENOTYPE_SPECS[i].phenotype, phenotype) == 0) return i;
    return -1;
}

int bf_discipl_seed_carrier_bridge(const char *root, const char *carrier,
                                   const char *phenotype,
                                   double detection_rate, unsigned long population,
                                   unsigned *actor_count, unsigned *contract_count)
{
    int pheno_idx = bf_discipl_phenotype_index(phenotype);
    const DisciplCarrierSpec *spec = NULL;
    bf_discipl_actor_t actor;
    char family[128];
    size_t i, k;
    unsigned actors = 0, contracts = 0;
    if (actor_count) *actor_count = 0;
    if (contract_count) *contract_count = 0;
    if (!carrier || !carrier[0]) return 2;
    /* A carrier nobody has observed does not get a family. */
    if (population == 0) return 2;
    if (!(detection_rate > 0.0) || detection_rate > 1.0) return 2;
    /* A carrier no phenotype declares has nothing to be conditional on. */
    if (pheno_idx < 0) return 2;
    for (i = 0; BF_DISCIPL_CARRIER_SPECS[i].carrier; ++i)
        if (strcmp(BF_DISCIPL_CARRIER_SPECS[i].carrier, carrier) == 0) spec = &BF_DISCIPL_CARRIER_SPECS[i];
    /* A carrier the lattice has no kinds for is reported, not invented. */
    if (!spec) return 3;
    if (bf_discipl_init_db(root) != 0) return 1;

    snprintf(family, sizeof(family), "T_CARRIER_%s", carrier);
    for (k = 0; family[k]; ++k) {
        if (family[k] >= 'a' && family[k] <= 'z') family[k] = (char)(family[k] - ('a' - 'A'));
        else if (family[k] == '-') family[k] = '_';
    }

    memset(&actor, 0, sizeof(actor));
    snprintf(actor.id, sizeof(actor.id), "carrier:%s", carrier);
    snprintf(actor.family, sizeof(actor.family), "%s", family);
    actor.actor_type = BF_DISCIPL_ACTOR_BRIDGE;
    snprintf(actor.domain, sizeof(actor.domain), "external_estate");
    snprintf(actor.modality, sizeof(actor.modality), "text");
    actor.affordances.can_follow = 1;
    actor.affordances.can_verify = 1;
    /* Conformance was checked against the bytes, so the carrier itself is not
     * in doubt. What is uncertain is how often a retained body turns out to be
     * this carrier, and that lives on the edge below, not here. */
    actor.confidence = 1.0;
    actor.uncertainty = 0.0;
    snprintf(actor.source_command, sizeof(actor.source_command),
             "bonfyre-facet conformance | population=%lu", population);
    if (bf_discipl_upsert_actor(root, &actor) != 0) return 1;
    actors++;

    /* The edge is from the phenotype, not from the retained body.
     *
     * Seeding it from T_RETAINED_BODY was the honest-looking version and it
     * made the phenotype irrelevant: all fifteen crossed into the lattice
     * through one file and every one of them reached the same fifteen
     * terminals. A bridge whose input cannot change its output is not a
     * bridge, it is a funnel.
     *
     * What makes the phenotype matter again is that conformance is recorded
     * per occurrence and attributed to the phenotype that declares the
     * carrier. EVENT occurrences with a body conform to RSS at 0.990; the
     * same measurement gives SERVICE_CAPABILITY -> OpenAPI 0.012, and gives
     * the other thirteen phenotypes no carrier at all. That spread is the
     * discriminator, and it is counted rather than assigned.
     *
     * These rates do not partition. An occurrence can conform to more than
     * one carrier -- 500 conformances across 479 EVENT bodies -- so the rates
     * on one phenotype's edges may sum past 1. They are per-edge likelihoods,
     * not a distribution over carriers. */
    if (discipl_seed_edge(root, BF_DISCIPL_PHENOTYPE_SPECS[pheno_idx].family, family,
                          "conforms_to", "carrier_conformance", detection_rate,
                          0.03, 0.00, &contracts) != 0) return 1;

    for (i = 0; BF_DISCIPL_COMMAND_SPECS[i].name; ++i) {
        char dst[128];
        size_t j;
        int matched = 0;
        for (j = 0; spec->kinds[j] && !matched; ++j)
            if (discipl_kinds_contain(BF_DISCIPL_COMMAND_SPECS[i].consumes, spec->kinds[j])) matched = 1;
        if (!matched) continue;
        snprintf(dst, sizeof(dst), "T_%s", BF_DISCIPL_COMMAND_SPECS[i].name);
        for (k = 0; dst[k]; ++k) if (dst[k] >= 'a' && dst[k] <= 'z') dst[k] = (char)(dst[k] - ('a' - 'A'));
        /* 0.95 and declared. A body that parsed as this carrier is text; what
         * remains uncertain is encoding and size, which nobody has counted. */
        if (discipl_seed_edge(root, family, dst, "parsed_as", "", 0.95, 0.04, 0.02,
                              &contracts) != 0) return 1;
    }

    if (actor_count) *actor_count = actors;
    if (contract_count) *contract_count = contracts;
    return 0;
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
