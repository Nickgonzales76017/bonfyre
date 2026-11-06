/* BonfyreMulti bucket 1: busybox-style multi-call binary (WX plan, bucketed). */
#include <string.h>
#include <stdio.h>
#include <stddef.h>

extern int bf_c_m_s_entry(int argc, char **argv);
extern int bf_model_entry(int argc, char **argv);
extern int bf_auth_entry(int argc, char **argv);
extern int bf_wire_entry(int argc, char **argv);
extern int bf_narrate_entry(int argc, char **argv);
extern int bf_compress_entry(int argc, char **argv);
extern int bf_discipl_entry(int argc, char **argv);
extern int bf_mo_e_entry(int argc, char **argv);
extern int bf_kvcache_entry(int argc, char **argv);
extern int bf_physics_entry(int argc, char **argv);
extern int bf_vec_entry(int argc, char **argv);
extern int bf_media_library_entry(int argc, char **argv);
extern int bf_workflow_entry(int argc, char **argv);
extern int bf_net_entry(int argc, char **argv);
extern int bf_graph_entry(int argc, char **argv);
extern int bf_interaction_grammar_entry(int argc, char **argv);
extern int bf_family_entry(int argc, char **argv);
extern int bf_flash_q_l_a_entry(int argc, char **argv);
extern int bf_tag_entry(int argc, char **argv);
extern int bf_proxy_entry(int argc, char **argv);
extern int bf_experience_map_entry(int argc, char **argv);
extern int bf_provider_frontier_entry(int argc, char **argv);
extern int bf_entity_entry(int argc, char **argv);
extern int bf_media_delivery_entry(int argc, char **argv);
extern int bf_proof_entry(int argc, char **argv);
extern int bf_flow_entry(int argc, char **argv);
extern int bf_compete_entry(int argc, char **argv);
extern int bf_hash_entry(int argc, char **argv);
extern int bf_paragraph_entry(int argc, char **argv);
extern int bf_learn_entry(int argc, char **argv);
extern int bf_detect_objects_entry(int argc, char **argv);
extern int bf_media_prep_entry(int argc, char **argv);
extern int bf_sync_entry(int argc, char **argv);
extern int bf_emit_entry(int argc, char **argv);
extern int bf_violence_entry(int argc, char **argv);
extern int bf_speech_loop_entry(int argc, char **argv);
extern int bf_pack_condition_entry(int argc, char **argv);
extern int bf_f_s_entry(int argc, char **argv);
extern int bf_domain_entry(int argc, char **argv);
extern int bf_scoped_reheat_entry(int argc, char **argv);
extern int bf_resource_weather_refresh_entry(int argc, char **argv);
extern int bf_render_entry(int argc, char **argv);
extern int bf_m_f_a_dict_entry(int argc, char **argv);

typedef int (*organ_fn)(int, char **);

static const struct { const char *name; organ_fn fn; } ORGANS[] = {
    {"bonfyre-cms", bf_c_m_s_entry},
    {"bonfyre-model", bf_model_entry},
    {"bonfyre-auth", bf_auth_entry},
    {"bonfyre-wire", bf_wire_entry},
    {"bonfyre-narrate", bf_narrate_entry},
    {"bonfyre-compress", bf_compress_entry},
    {"bonfyre-discipl", bf_discipl_entry},
    {"bonfyre-moe", bf_mo_e_entry},
    {"bonfyre-kvcache", bf_kvcache_entry},
    {"bonfyre-physics", bf_physics_entry},
    {"bonfyre-vec", bf_vec_entry},
    {"bonfyre-media-library", bf_media_library_entry},
    {"bonfyre-workflow", bf_workflow_entry},
    {"bonfyre-net", bf_net_entry},
    {"bonfyre-graph", bf_graph_entry},
    {"bonfyre-interaction-grammar", bf_interaction_grammar_entry},
    {"bonfyre-family", bf_family_entry},
    {"bonfyre-flashqla", bf_flash_q_l_a_entry},
    {"bonfyre-tag", bf_tag_entry},
    {"bonfyre-proxy", bf_proxy_entry},
    {"bonfyre-experience-map", bf_experience_map_entry},
    {"bonfyre-provider-frontier", bf_provider_frontier_entry},
    {"bonfyre-entity", bf_entity_entry},
    {"bonfyre-media-delivery", bf_media_delivery_entry},
    {"bonfyre-proof", bf_proof_entry},
    {"bonfyre-flow", bf_flow_entry},
    {"bonfyre-compete", bf_compete_entry},
    {"bonfyre-hash", bf_hash_entry},
    {"bonfyre-paragraph", bf_paragraph_entry},
    {"bonfyre-learn", bf_learn_entry},
    {"bonfyre-detect-objects", bf_detect_objects_entry},
    {"bonfyre-media-prep", bf_media_prep_entry},
    {"bonfyre-sync", bf_sync_entry},
    {"bonfyre-emit", bf_emit_entry},
    {"bonfyre-violence", bf_violence_entry},
    {"bonfyre-speechloop", bf_speech_loop_entry},
    {"bonfyre-pack-condition", bf_pack_condition_entry},
    {"bonfyre-fs", bf_f_s_entry},
    {"bonfyre-domain", bf_domain_entry},
    {"bonfyre-scoped-reheat", bf_scoped_reheat_entry},
    {"bonfyre-resource-weather-refresh", bf_resource_weather_refresh_entry},
    {"bonfyre-render", bf_render_entry},
    {"bonfyre-mfa-dict", bf_m_f_a_dict_entry},
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

    fprintf(stderr, "bonfyre-multi-1: unknown organ '%s' (argv[0] basename); expected one of:\n", base);
    for (i = 0; i < ORGAN_COUNT; i++) fprintf(stderr, "  %s\n", ORGANS[i].name);
    return 2;
}
