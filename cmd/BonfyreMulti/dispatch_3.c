/* BonfyreMulti bucket 3: busybox-style multi-call binary (WX plan, bucketed). */
#include <string.h>
#include <stdio.h>
#include <stddef.h>

extern int bf_f_p_q_entry(int argc, char **argv);
extern int bf_external_estate_entry(int argc, char **argv);
extern int bf_embed_entry(int argc, char **argv);
extern int bf_c_l_i_entry(int argc, char **argv);
extern int bf_bridge_entry(int argc, char **argv);
extern int bf_facet_entry(int argc, char **argv);
extern int bf_meter_entry(int argc, char **argv);
extern int bf_runtime_entry(int argc, char **argv);
extern int bf_orchestrate_entry(int argc, char **argv);
extern int bf_lowering_workers_entry(int argc, char **argv);
extern int bf_layer_entry(int argc, char **argv);
extern int bf_transcribe_entry(int argc, char **argv);
extern int bf_ingest_entry(int argc, char **argv);
extern int bf_swarm_entry(int argc, char **argv);
extern int bf_owner_contract_entry(int argc, char **argv);
extern int bf_web_sub_entry(int argc, char **argv);
extern int bf_canon_entry(int argc, char **argv);
extern int bf_outreach_entry(int argc, char **argv);
extern int bf_pay_entry(int argc, char **argv);
extern int bf_mo_q_entry(int argc, char **argv);
extern int bf_stitch_entry(int argc, char **argv);
extern int bf_reason_entry(int argc, char **argv);
extern int bf_gen_entry(int argc, char **argv);
extern int bf_semantic_operation_entry(int argc, char **argv);
extern int bf_offer_entry(int argc, char **argv);
extern int bf_channel_entry(int argc, char **argv);
extern int bf_ledger_entry(int argc, char **argv);
extern int bf_standing_entry(int argc, char **argv);
extern int bf_scoped_demand_entry(int argc, char **argv);
extern int bf_tier_entry(int argc, char **argv);
extern int bf_commerce_entry(int argc, char **argv);
extern int bf_world_compiler_entry(int argc, char **argv);
extern int bf_transcript_clean_entry(int argc, char **argv);
extern int bf_f_p_qx_entry(int argc, char **argv);
extern int bf_git_hub_watch_entry(int argc, char **argv);
extern int bf_scoped_respecialize_entry(int argc, char **argv);
extern int bf_leapfrog_entry(int argc, char **argv);
extern int bf_attention_entry(int argc, char **argv);
extern int bf_gate_entry(int argc, char **argv);
extern int bf_material_weather_reheat_entry(int argc, char **argv);
extern int bf_weaviate_index_entry(int argc, char **argv);
extern int bf_web_sub_lease_gate_entry(int argc, char **argv);
extern int bf_pack_condition_delta_entry(int argc, char **argv);
extern int bf_frame_extract_entry(int argc, char **argv);
extern int bf_video_demux_entry(int argc, char **argv);

typedef int (*organ_fn)(int, char **);

static const struct { const char *name; organ_fn fn; } ORGANS[] = {
    {"bonfyre-fpq", bf_f_p_q_entry},
    {"bonfyre-external-estate", bf_external_estate_entry},
    {"bonfyre-embed", bf_embed_entry},
    {"bonfyre", bf_c_l_i_entry},
    {"bonfyre-bridge", bf_bridge_entry},
    {"bonfyre-facet", bf_facet_entry},
    {"bonfyre-meter", bf_meter_entry},
    {"bonfyre-runtime", bf_runtime_entry},
    {"bonfyre-orchestrate", bf_orchestrate_entry},
    {"bonfyre-lowering-workers", bf_lowering_workers_entry},
    {"bonfyre-layer", bf_layer_entry},
    {"bonfyre-layer-c", bf_layer_entry},
    {"bonfyre-transcribe", bf_transcribe_entry},
    {"bonfyre-ingest", bf_ingest_entry},
    {"bonfyre-swarm", bf_swarm_entry},
    {"bonfyre-owner-contract", bf_owner_contract_entry},
    {"bonfyre-websub", bf_web_sub_entry},
    {"bonfyre-canon", bf_canon_entry},
    {"bonfyre-outreach", bf_outreach_entry},
    {"bonfyre-pay", bf_pay_entry},
    {"bonfyre-moq", bf_mo_q_entry},
    {"bonfyre-stitch", bf_stitch_entry},
    {"bonfyre-reason", bf_reason_entry},
    {"bonfyre-gen", bf_gen_entry},
    {"bonfyre-semantic-operation", bf_semantic_operation_entry},
    {"bonfyre-offer", bf_offer_entry},
    {"bonfyre-channel", bf_channel_entry},
    {"bonfyre-ledger", bf_ledger_entry},
    {"bonfyre-standing", bf_standing_entry},
    {"bonfyre-scoped-demand", bf_scoped_demand_entry},
    {"bonfyre-tier", bf_tier_entry},
    {"bonfyre-commerce", bf_commerce_entry},
    {"bonfyre-world-compiler", bf_world_compiler_entry},
    {"bonfyre-transcript-clean", bf_transcript_clean_entry},
    {"bonfyre-fpqx", bf_f_p_qx_entry},
    {"bonfyre-github-watch", bf_git_hub_watch_entry},
    {"bonfyre-scoped-respecialize", bf_scoped_respecialize_entry},
    {"bonfyre-leapfrog", bf_leapfrog_entry},
    {"bonfyre-attention", bf_attention_entry},
    {"bonfyre-gate", bf_gate_entry},
    {"bonfyre-material-weather-reheat", bf_material_weather_reheat_entry},
    {"bonfyre-weaviate-index", bf_weaviate_index_entry},
    {"bonfyre-websub-lease-gate", bf_web_sub_lease_gate_entry},
    {"bonfyre-pack-condition-delta", bf_pack_condition_delta_entry},
    {"bonfyre-frame-extract", bf_frame_extract_entry},
    {"bonfyre-video-demux", bf_video_demux_entry},
};
#define ORGAN_COUNT (sizeof(ORGANS) / sizeof(ORGANS[0]))

static const char *basename_of(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

int main(int argc, char **argv) {
    const char *base = argc > 0 ? basename_of(argv[0]) : "";
    size_t i;

    for (i = 0; i < ORGAN_COUNT; i++) {
        if (strcmp(base, ORGANS[i].name) == 0) return ORGANS[i].fn(argc, argv);
    }

    if (argc > 1) {
        char prefixed[64];
        snprintf(prefixed, sizeof(prefixed), "bonfyre-%s", argv[1]);
        for (i = 0; i < ORGAN_COUNT; i++) {
            if (strcmp(prefixed, ORGANS[i].name) == 0) return ORGANS[i].fn(argc - 1, argv + 1);
        }
    }

    fprintf(stderr, "bonfyre-multi-3: unknown organ '%s' (argv[0] basename); expected one of:\n", base);
    for (i = 0; i < ORGAN_COUNT; i++) fprintf(stderr, "  %s\n", ORGANS[i].name);
    return 2;
}
