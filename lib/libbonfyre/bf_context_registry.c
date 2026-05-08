#define _POSIX_C_SOURCE 200809L
#include "include/bonfyre.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint32_t clamp_u32_(uint32_t value, uint32_t min_v, uint32_t max_v) {
    if (value < min_v) return min_v;
    if (value > max_v) return max_v;
    return value;
}

static double clamp_f64_(double value, double min_v, double max_v) {
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

static const char *policy_for_(const char *mode, uint32_t idx, uint32_t top_blocks) {
    if (strcmp(mode, "dense") == 0) return "HOT_EXACT";
    if (strcmp(mode, "compressed") == 0) return "COMPRESS";
    if (strcmp(mode, "state") == 0) return (idx % 3 == 0) ? "MEMBRANE_SELECT" : "COMPRESS";
    if (strcmp(mode, "membrane") == 0) return (idx < top_blocks / 2) ? "MEMBRANE_SELECT" : "COMPRESS";
    return (idx < (top_blocks * 2) / 3) ? "MEMBRANE_SELECT" : "COMPRESS";
}

void bf_context_kv_registry_defaults(BfContextKVRegistryConfig *cfg) {
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->mode = "hybrid";
    cfg->layers = 24;
    cfg->heads = 16;
    cfg->seq_tokens = 131072;
    cfg->block_tokens = 128;
    cfg->top_blocks = 128;
    cfg->required_witnesses = 12;
    cfg->base_residual = 0.18;
    cfg->continuity_fail_rate = 0.05;
}

int bf_context_kv_registry_json(const BfContextKVRegistryConfig *cfg, char **out_json) {
    if (!cfg || !out_json) return -1;

    const char *mode = safe_mode_(cfg->mode);
    const uint32_t layers = clamp_u32_(cfg->layers, 1, 256);
    const uint32_t heads = clamp_u32_(cfg->heads, 1, 256);
    const uint32_t seq_tokens = clamp_u32_(cfg->seq_tokens, 256, 1u << 30);
    const uint32_t block_tokens = clamp_u32_(cfg->block_tokens, 16, 8192);
    const uint32_t top_blocks = clamp_u32_(cfg->top_blocks, 1, 8192);
    const uint32_t required_witnesses = cfg->required_witnesses;
    const double base_residual = clamp_f64_(cfg->base_residual, 0.0, 10.0);
    const double fail_rate = clamp_f64_(cfg->continuity_fail_rate, 0.0, 1.0);

    const uint32_t total_blocks = (seq_tokens + block_tokens - 1u) / block_tokens;
    uint32_t selected_blocks = top_blocks;
    if (selected_blocks > total_blocks) selected_blocks = total_blocks;

    size_t cap = (size_t)selected_blocks * 620u + 4096u;
    char *json = (char *)malloc(cap);
    if (!json) return -1;

    size_t off = 0;
    int n = snprintf(
        json + off,
        cap - off,
        "{\n"
        "  \"schema_version\": \"akai.context.kv_registry.v1\",\n"
        "  \"mode\": \"%s\",\n"
        "  \"layers\": %u,\n"
        "  \"heads\": %u,\n"
        "  \"seq_tokens\": %u,\n"
        "  \"block_tokens\": %u,\n"
        "  \"selected_blocks\": %u,\n"
        "  \"required_witnesses\": %u,\n"
        "  \"blocks\": [\n",
        mode,
        layers,
        heads,
        seq_tokens,
        block_tokens,
        selected_blocks,
        required_witnesses
    );
    if (n <= 0 || (size_t)n >= cap - off) { free(json); return -1; }
    off += (size_t)n;

    uint32_t fail_stride = 0;
    if (fail_rate > 0.0) {
        fail_stride = (uint32_t)floor(1.0 / fail_rate);
        if (fail_stride == 0) fail_stride = 1;
    }

    for (uint32_t i = 0; i < selected_blocks; i++) {
        const uint32_t layer = i % layers;
        const uint32_t head = (i / layers) % heads;
        const uint32_t token_start = i * block_tokens;
        uint32_t token_end = token_start + block_tokens - 1u;
        if (token_end >= seq_tokens) token_end = seq_tokens - 1u;

        double attention_mass = 1.0 / (double)(i + 2u);
        if (attention_mass < 0.0005) attention_mass = 0.0005;

        const double residual_norm = base_residual + ((double)(i % 17u) * 0.007);

        int e8_0 = (int)((layer + head + i) % 9u) - 4;
        int e8_1 = (int)((layer + 2u * i) % 9u) - 4;
        int e8_2 = (int)((head + 3u * i) % 9u) - 4;
        int e8_3 = (int)((layer + head + 5u * i) % 9u) - 4;
        int e8_4 = (int)((2u * layer + i) % 9u) - 4;
        int e8_5 = (int)((2u * head + i) % 9u) - 4;
        int e8_6 = (int)((layer + 7u * i) % 9u) - 4;
        int e8_7 = (int)((head + 11u * i) % 9u) - 4;

        char witness_seed[256];
        snprintf(
            witness_seed,
            sizeof(witness_seed),
            "%s|%u|%u|%u|%u|%u|%u",
            mode,
            layer,
            head,
            token_start,
            token_end,
            required_witnesses,
            i
        );
        char witness_hex[65];
        bf_sha256_hex((const uint8_t *)witness_seed, strlen(witness_seed), witness_hex);

        const int is_fail = (fail_stride > 0u && (i % fail_stride) == 0u);
        const char *continuity = is_fail ? "CONTINUITY_FAIL" : ((residual_norm > base_residual + 0.06) ? "PASS_WITH_DRIFT" : "PASS");
        const char *policy = policy_for_(mode, i, selected_blocks);

        n = snprintf(
            json + off,
            cap - off,
            "    {\n"
            "      \"layer\": %u,\n"
            "      \"head\": %u,\n"
            "      \"token_start\": %u,\n"
            "      \"token_end\": %u,\n"
            "      \"attention_mass\": %.6f,\n"
            "      \"_e8\": {\n"
            "        \"cell_key\": \"%d,%d,%d,%d,%d,%d,%d,%d\",\n"
            "        \"residual_norm\": %.6f\n"
            "      },\n"
            "      \"witness_hash\": \"sha256:%s\",\n"
            "      \"continuity\": \"%s\",\n"
            "      \"policy\": \"%s\"\n"
            "    }%s\n",
            layer,
            head,
            token_start,
            token_end,
            attention_mass,
            e8_0, e8_1, e8_2, e8_3, e8_4, e8_5, e8_6, e8_7,
            residual_norm,
            witness_hex,
            continuity,
            policy,
            (i + 1u < selected_blocks) ? "," : ""
        );
        if (n <= 0 || (size_t)n >= cap - off) {
            free(json);
            return -1;
        }
        off += (size_t)n;
    }

    n = snprintf(json + off, cap - off, "  ]\n}\n");
    if (n <= 0 || (size_t)n >= cap - off) { free(json); return -1; }

    *out_json = json;
    return 0;
}
