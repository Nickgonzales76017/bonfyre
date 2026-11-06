/* BonfyreMulti bucket 2: busybox-style multi-call binary (WX plan, bucketed). */
#include <string.h>
#include <stdio.h>
#include <stddef.h>

extern int bf_decide_entry(int argc, char **argv);
extern int bf_queue_entry(int argc, char **argv);
extern int bf_brief_entry(int argc, char **argv);
extern int bf_credentials_entry(int argc, char **argv);
extern int bf_finance_entry(int argc, char **argv);
extern int bf_capability_entry(int argc, char **argv);
extern int bf_surface_maturity_entry(int argc, char **argv);
extern int bf_index_entry(int argc, char **argv);
extern int bf_provider_entry(int argc, char **argv);
extern int bf_surfacescene_entry(int argc, char **argv);
extern int bf_control_entry(int argc, char **argv);
extern int bf_operator_entry(int argc, char **argv);
extern int bf_economy_entry(int argc, char **argv);
extern int bf_bit_net_entry(int argc, char **argv);
extern int bf_a_p_i_entry(int argc, char **argv);
extern int bf_tel_entry(int argc, char **argv);
extern int bf_s_l_i_entry(int argc, char **argv);
extern int bf_pipeline_entry(int argc, char **argv);
extern int bf_tone_entry(int argc, char **argv);
extern int bf_retention_entry(int argc, char **argv);
extern int bf_quant_entry(int argc, char **argv);
extern int bf_s_a_e_entry(int argc, char **argv);
extern int bf_actors_entry(int argc, char **argv);
extern int bf_repurpose_entry(int argc, char **argv);
extern int bf_lex_entry(int argc, char **argv);
extern int bf_segment_entry(int argc, char **argv);
extern int bf_clips_entry(int argc, char **argv);
extern int bf_time_entry(int argc, char **argv);
extern int bf_material_weather_gate_entry(int argc, char **argv);
extern int bf_space_entry(int argc, char **argv);
extern int bf_origin_entry(int argc, char **argv);
extern int bf_scoped_frontier_entry(int argc, char **argv);
extern int bf_query_entry(int argc, char **argv);
extern int bf_watch_entry(int argc, char **argv);
extern int bf_braid_audit_entry(int argc, char **argv);
extern int bf_distribute_entry(int argc, char **argv);
extern int bf_agent_contract_entry(int argc, char **argv);
extern int bf_scenedetect_entry(int argc, char **argv);
extern int bf_scoped_lease_reheat_entry(int argc, char **argv);
extern int bf_admission_entry(int argc, char **argv);
extern int bf_sign_entry(int argc, char **argv);
extern int bf_pack_entry(int argc, char **argv);
extern int bf_surface_entry(int argc, char **argv);
extern int bf_transcript_family_entry(int argc, char **argv);
extern int bf_lifecycle_entry(int argc, char **argv);

typedef int (*organ_fn)(int, char **);

static const struct { const char *name; organ_fn fn; } ORGANS[] = {
    {"bonfyre-decide", bf_decide_entry},
    {"bonfyre-queue", bf_queue_entry},
    {"bonfyre-brief", bf_brief_entry},
    {"bonfyre-credentials", bf_credentials_entry},
    {"bonfyre-finance", bf_finance_entry},
    {"bonfyre-capability", bf_capability_entry},
    {"bonfyre-surface-maturity", bf_surface_maturity_entry},
    {"bonfyre-index", bf_index_entry},
    {"bonfyre-provider", bf_provider_entry},
    {"bonfyre-surface-scene", bf_surfacescene_entry},
    {"bonfyre-control", bf_control_entry},
    {"bonfyre-operator", bf_operator_entry},
    {"bonfyre-economy", bf_economy_entry},
    {"bonfyre-bitnet", bf_bit_net_entry},
    {"bonfyre-api", bf_a_p_i_entry},
    {"bonfyre-tel", bf_tel_entry},
    {"bonfyre-sli", bf_s_l_i_entry},
    {"bonfyre-pipeline", bf_pipeline_entry},
    {"bonfyre-tone", bf_tone_entry},
    {"bonfyre-retention", bf_retention_entry},
    {"bonfyre-quant", bf_quant_entry},
    {"bonfyre-sae", bf_s_a_e_entry},
    {"bonfyre-actors", bf_actors_entry},
    {"bonfyre-repurpose", bf_repurpose_entry},
    {"bonfyre-lex", bf_lex_entry},
    {"bonfyre-segment", bf_segment_entry},
    {"bonfyre-clips", bf_clips_entry},
    {"bonfyre-time", bf_time_entry},
    {"bonfyre-material-weather-gate", bf_material_weather_gate_entry},
    {"bonfyre-space", bf_space_entry},
    {"bonfyre-origin", bf_origin_entry},
    {"bonfyre-scoped-frontier", bf_scoped_frontier_entry},
    {"bonfyre-query", bf_query_entry},
    {"bonfyre-watch", bf_watch_entry},
    {"bonfyre-braid-audit", bf_braid_audit_entry},
    {"bonfyre-distribute", bf_distribute_entry},
    {"bonfyre-agent-contract", bf_agent_contract_entry},
    {"bonfyre-scene-detect", bf_scenedetect_entry},
    {"bonfyre-scoped-lease-reheat", bf_scoped_lease_reheat_entry},
    {"bonfyre-admission", bf_admission_entry},
    {"bonfyre-sign", bf_sign_entry},
    {"bonfyre-pack", bf_pack_entry},
    {"bonfyre-surface", bf_surface_entry},
    {"bonfyre-surface-ir", bf_surface_entry},
    {"bonfyre-transcript-family", bf_transcript_family_entry},
    {"bonfyre-lifecycle", bf_lifecycle_entry},
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

    fprintf(stderr, "bonfyre-multi-2: unknown organ '%s' (argv[0] basename); expected one of:\n", base);
    for (i = 0; i < ORGAN_COUNT; i++) fprintf(stderr, "  %s\n", ORGANS[i].name);
    return 2;
}
