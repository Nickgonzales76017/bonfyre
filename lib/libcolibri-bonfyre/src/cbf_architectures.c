/*
 * cbf_architectures.c — MoE architecture registry and auto-detection
 *
 * Provides configuration templates for known MoE architectures:
 *   - Mixtral 8x7B / 8x22B (Mistral AI)
 *   - Qwen2-MoE 2.7B / 57B (Alibaba)
 *   - DeepSeek-V2 / V3 (DeepSeek AI)
 *   - GLM-5.2 744B (Tsinghua/Zhipu)
 *   - Generic MoE fallback
 *
 * Auto-detection from model_config.json or command-line flags.
 */

#include "colibri_bonfyre.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

/* ═══════════════════════════════════════════════════════════════════
 * Architecture templates
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    const char *name;
    const char *aliases[4];  /* Alternative names */
    cbf_model_shape_t shape;
} arch_template_t;

static const arch_template_t ARCH_TEMPLATES[] = {
    /* Mixtral 8x7B (Mistral AI) */
    {
        .name = "mixtral-8x7b",
        .aliases = {"mixtral", "mixtral-8x7", "Mixtral-8x7B-v0.1", NULL},
        .shape = {
            .n_vocab = 32000,
            .d_model = 4096,
            .n_layers = 32,
            .n_heads = 32,
            .n_kv_heads = 8,  /* GQA */
            .head_dim = 128,
            .d_ffn = 14336,
            .n_experts_per_layer = 8,
            .n_experts_active = 2,
            .n_shared_experts = 0,
            .has_mtp_head = false,
            .expert_size_mb = 150,  /* ~150 MB per expert (d_ffn=14336) */
            .dense_size_mb = 2048,
            .kv_bytes_per_token = 1024,
            .max_seq_len = 32768,
            .rope_theta = 1000000.0f,
        }
    },
    
    /* Mixtral 8x22B (Mistral AI) */
    {
        .name = "mixtral-8x22b",
        .aliases = {"mixtral-22b", "Mixtral-8x22B-v0.1", NULL},
        .shape = {
            .n_vocab = 32000,
            .d_model = 6144,
            .n_layers = 56,
            .n_heads = 48,
            .n_kv_heads = 8,
            .head_dim = 128,
            .d_ffn = 16384,
            .n_experts_per_layer = 8,
            .n_experts_active = 2,
            .n_shared_experts = 0,
            .has_mtp_head = false,
            .expert_size_mb = 450,  /* d_ffn=16384 */
            .dense_size_mb = 4096,
            .kv_bytes_per_token = 1024,
            .max_seq_len = 65536,
            .rope_theta = 1000000.0f,
        }
    },
    
    /* Qwen2-MoE 2.7B (Alibaba) */
    {
        .name = "qwen-moe-2.7b",
        .aliases = {"qwen-moe", "Qwen1.5-MoE-A2.7B", "qwen2-moe", NULL},
        .shape = {
            .n_vocab = 151936,
            .d_model = 2048,
            .n_layers = 24,
            .n_heads = 16,
            .n_kv_heads = 16,
            .head_dim = 128,
            .d_ffn = 5632,
            .n_experts_per_layer = 60,
            .n_experts_active = 4,
            .n_shared_experts = 0,
            .has_mtp_head = false,
            .expert_size_mb = 20,  /* d_ffn=5632 */
            .dense_size_mb = 512,
            .kv_bytes_per_token = 512,
            .max_seq_len = 32768,
            .rope_theta = 1000000.0f,
        }
    },
    
    /* Qwen2-MoE 57B (Alibaba) */
    {
        .name = "qwen-moe-57b",
        .aliases = {"Qwen1.5-MoE-A57B", NULL},
        .shape = {
            .n_vocab = 152064,
            .d_model = 3584,
            .n_layers = 28,
            .n_heads = 28,
            .n_kv_heads = 28,
            .head_dim = 128,
            .d_ffn = 18944,
            .n_experts_per_layer = 64,
            .n_experts_active = 8,
            .n_shared_experts = 0,
            .has_mtp_head = false,
            .expert_size_mb = 180,  /* d_ffn=18944 */
            .dense_size_mb = 2048,
            .kv_bytes_per_token = 896,
            .max_seq_len = 32768,
            .rope_theta = 1000000.0f,
        }
    },
    
    /* DeepSeek-V2 236B (DeepSeek AI) */
    {
        .name = "deepseek-v2",
        .aliases = {"DeepSeek-V2", "deepseek-236b", NULL},
        .shape = {
            .n_vocab = 102400,
            .d_model = 5120,
            .n_layers = 60,
            .n_heads = 128,
            .n_kv_heads = 16,  /* MLA */
            .head_dim = 128,
            .d_ffn = 12288,
            .n_experts_per_layer = 160,
            .n_experts_active = 6,
            .n_shared_experts = 2,
            .has_mtp_head = false,
            .expert_size_mb = 80,  /* d_ffn=12288 */
            .dense_size_mb = 4096,
            .kv_bytes_per_token = 2304,  /* MLA compressed */
            .max_seq_len = 163840,
            .rope_theta = 10000.0f,
        }
    },
    
    /* DeepSeek-V3 671B (DeepSeek AI) */
    {
        .name = "deepseek-v3",
        .aliases = {"DeepSeek-V3", "deepseek-671b", NULL},
        .shape = {
            .n_vocab = 129280,
            .d_model = 7168,
            .n_layers = 61,
            .n_heads = 128,
            .n_kv_heads = 16,  /* MLA */
            .head_dim = 128,
            .d_ffn = 18432,
            .n_experts_per_layer = 256,
            .n_experts_active = 8,
            .n_shared_experts = 1,
            .has_mtp_head = false,
            .expert_size_mb = 150,  /* d_ffn=18432 */
            .dense_size_mb = 5120,
            .kv_bytes_per_token = 2304,  /* MLA compressed */
            .max_seq_len = 163840,
            .rope_theta = 10000.0f,
        }
    },
    
    /* GLM-5.2 744B (Tsinghua/Zhipu) */
    {
        .name = "glm-5-2",
        .aliases = {"glm", "glm-5.2", "glm-744b", NULL},
        .shape = {
            .n_vocab = 151552,
            .d_model = 8192,
            .n_layers = 60,
            .n_heads = 64,
            .n_kv_heads = 8,  /* GQA */
            .head_dim = 128,
            .d_ffn = 49152,
            .n_experts_per_layer = 256,
            .n_experts_active = 2,
            .n_shared_experts = 2,
            .has_mtp_head = true,
            .expert_size_mb = 200,  /* d_ffn=49152 */
            .dense_size_mb = 9900,
            .kv_bytes_per_token = 1024,
            .max_seq_len = 8192,
            .rope_theta = 10000.0f,
        }
    },
    
    /* Sentinel */
    {NULL, {NULL}, {0}}
};

#define N_TEMPLATES (sizeof(ARCH_TEMPLATES) / sizeof(arch_template_t) - 1)

/* ═══════════════════════════════════════════════════════════════════
 * Architecture detection
 * ═══════════════════════════════════════════════════════════════════ */

/* Normalize architecture name for matching (lowercase, remove special chars) */
static void normalize_arch_name(const char *input, char *output, size_t max_len) {
    size_t j = 0;
    for (size_t i = 0; input[i] && j < max_len - 1; i++) {
        char c = input[i];
        if (isalnum(c)) {
            output[j++] = tolower(c);
        } else if (c == '-' || c == '_') {
            output[j++] = '-';
        }
    }
    output[j] = '\0';
}

/* Find architecture template by name or alias */
const cbf_model_shape_t *cbf_arch_lookup(const char *arch_name) {
    if (!arch_name) return NULL;
    
    char normalized[128];
    normalize_arch_name(arch_name, normalized, sizeof(normalized));
    
    for (size_t i = 0; i < N_TEMPLATES; i++) {
        const arch_template_t *tmpl = &ARCH_TEMPLATES[i];
        
        /* Check main name */
        char tmpl_name[128];
        normalize_arch_name(tmpl->name, tmpl_name, sizeof(tmpl_name));
        if (strcmp(normalized, tmpl_name) == 0) {
            return &tmpl->shape;
        }
        
        /* Check aliases */
        for (int j = 0; j < 4 && tmpl->aliases[j]; j++) {
            char alias_name[128];
            normalize_arch_name(tmpl->aliases[j], alias_name, sizeof(alias_name));
            if (strcmp(normalized, alias_name) == 0) {
                return &tmpl->shape;
            }
        }
    }
    
    return NULL;
}

/* Load architecture from model_config.json */
int cbf_arch_load_from_config(const char *model_path, cbf_model_shape_t *out_shape) {
    if (!model_path || !out_shape) return CBF_ERR_INVALID;
    
    char config_path[1024];
    snprintf(config_path, sizeof(config_path), "%s/model_config.json", model_path);
    
    FILE *f = fopen(config_path, "r");
    if (!f) {
        fprintf(stderr, "[cbf_arch] model_config.json not found in %s\n", model_path);
        return CBF_ERR_IO;
    }
    
    /* Read entire file */
    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    char *json_buf = malloc(file_size + 1);
    if (!json_buf) {
        fclose(f);
        return CBF_ERR_MEMORY;
    }
    
    fread(json_buf, 1, file_size, f);
    json_buf[file_size] = '\0';
    fclose(f);
    
    /* Parse JSON with the current minimal config reader.
     * Move to a shared parser when the model config schema broadens. */
    
    /* Extract architecture name */
    char arch_name[64] = {0};
    const char *arch_key = "\"architecture\":";
    char *arch_ptr = strstr(json_buf, arch_key);
    if (arch_ptr) {
        arch_ptr += strlen(arch_key);
        while (*arch_ptr && (*arch_ptr == ' ' || *arch_ptr == '"')) arch_ptr++;
        
        int i = 0;
        while (*arch_ptr && *arch_ptr != '"' && *arch_ptr != ',' && i < 63) {
            arch_name[i++] = *arch_ptr++;
        }
        arch_name[i] = '\0';
    }
    
    /* Try to find template by architecture name */
    const cbf_model_shape_t *tmpl = cbf_arch_lookup(arch_name);
    if (tmpl) {
        *out_shape = *tmpl;
        free(json_buf);
        
        printf("[cbf_arch] Detected architecture: %s\n", arch_name);
        return CBF_OK;
    }
    
    /* Fallback: parse config manually */
    /* Extract numeric fields from JSON */
    #define PARSE_INT_FIELD(field_name, target) do { \
        const char *key = "\"" field_name "\":"; \
        char *ptr = strstr(json_buf, key); \
        if (ptr) { \
            ptr += strlen(key); \
            while (*ptr && !isdigit(*ptr)) ptr++; \
            target = atoi(ptr); \
        } \
    } while(0)
    
    #define PARSE_FLOAT_FIELD(field_name, target) do { \
        const char *key = "\"" field_name "\":"; \
        char *ptr = strstr(json_buf, key); \
        if (ptr) { \
            ptr += strlen(key); \
            while (*ptr && !isdigit(*ptr) && *ptr != '.') ptr++; \
            target = atof(ptr); \
        } \
    } while(0)
    
    PARSE_INT_FIELD("n_vocab", out_shape->n_vocab);
    PARSE_INT_FIELD("d_model", out_shape->d_model);
    PARSE_INT_FIELD("n_layers", out_shape->n_layers);
    PARSE_INT_FIELD("n_heads", out_shape->n_heads);
    PARSE_INT_FIELD("n_kv_heads", out_shape->n_kv_heads);
    PARSE_INT_FIELD("head_dim", out_shape->head_dim);
    PARSE_INT_FIELD("d_ffn", out_shape->d_ffn);
    PARSE_INT_FIELD("n_experts_per_layer", out_shape->n_experts_per_layer);
    PARSE_INT_FIELD("n_experts_active", out_shape->n_experts_active);
    PARSE_INT_FIELD("n_shared_experts", out_shape->n_shared_experts);
    PARSE_INT_FIELD("expert_size_mb", out_shape->expert_size_mb);
    PARSE_INT_FIELD("dense_size_mb", out_shape->dense_size_mb);
    PARSE_INT_FIELD("kv_bytes_per_token", out_shape->kv_bytes_per_token);
    PARSE_INT_FIELD("max_seq_len", out_shape->max_seq_len);
    PARSE_FLOAT_FIELD("rope_theta", out_shape->rope_theta);
    
    /* has_mtp_head is a boolean */
    const char *mtp_key = "\"has_mtp_head\":";
    char *mtp_ptr = strstr(json_buf, mtp_key);
    if (mtp_ptr) {
        mtp_ptr += strlen(mtp_key);
        while (*mtp_ptr && (*mtp_ptr == ' ' || *mtp_ptr == '\t')) mtp_ptr++;
        out_shape->has_mtp_head = (strncmp(mtp_ptr, "true", 4) == 0);
    }
    
    free(json_buf);
    
    /* Validate required fields */
    if (out_shape->n_vocab == 0 || out_shape->d_model == 0 || out_shape->n_layers == 0) {
        fprintf(stderr, "[cbf_arch] Invalid config: missing required fields\n");
        return CBF_ERR_INVALID;
    }
    
    printf("[cbf_arch] Loaded custom configuration from model_config.json\n");
    printf("[cbf_arch] %u layers, %u experts/layer, top-%u routing\n",
           out_shape->n_layers, out_shape->n_experts_per_layer, out_shape->n_experts_active);
    
    return CBF_OK;
}

/* List all known architectures */
void cbf_arch_list_all(void) {
    printf("Known MoE Architectures:\n\n");
    
    for (size_t i = 0; i < N_TEMPLATES; i++) {
        const arch_template_t *tmpl = &ARCH_TEMPLATES[i];
        const cbf_model_shape_t *s = &tmpl->shape;
        
        printf("%-20s", tmpl->name);
        
        /* Calculate total params (rough estimate) */
        size_t dense_params = (size_t)s->n_vocab * s->d_model + 
                              (size_t)s->n_layers * s->d_model * s->d_model * 4;
        size_t expert_params = (size_t)s->n_layers * s->n_experts_per_layer * 
                               s->expert_size_mb * 1024 * 1024 / 4;  /* Assume int4 = 0.5 bytes */
        size_t total_params = dense_params + expert_params;
        
        printf("  %5.1f B params, %u layers, %ux%u experts\n",
               total_params / 1e9,
               s->n_layers,
               s->n_layers,
               s->n_experts_per_layer);
        
        /* Print aliases */
        if (tmpl->aliases[0]) {
            printf("                      Aliases: ");
            for (int j = 0; j < 4 && tmpl->aliases[j]; j++) {
                if (j > 0) printf(", ");
                printf("%s", tmpl->aliases[j]);
            }
            printf("\n");
        }
        printf("\n");
    }
}

/* Auto-detect architecture from model directory */
int cbf_arch_auto_detect(const char *model_path, cbf_model_shape_t *out_shape) {
    if (!model_path || !out_shape) return CBF_ERR_INVALID;
    
    /* Try loading from model_config.json first */
    int rc = cbf_arch_load_from_config(model_path, out_shape);
    if (rc == CBF_OK) {
        return CBF_OK;
    }
    
    /* Fallback: try to detect from directory name */
    const char *dir_name = strrchr(model_path, '/');
    if (dir_name) {
        dir_name++;  /* Skip slash */
        
        const cbf_model_shape_t *tmpl = cbf_arch_lookup(dir_name);
        if (tmpl) {
            *out_shape = *tmpl;
            printf("[cbf_arch] Auto-detected from directory name: %s\n", dir_name);
            return CBF_OK;
        }
    }
    
    fprintf(stderr, "[cbf_arch] Could not auto-detect architecture\n");
    fprintf(stderr, "[cbf_arch] Specify with --arch flag or create model_config.json\n");
    return CBF_ERR_INVALID;
}

/* Create model_config.json from template */
int cbf_arch_save_config(const char *model_path, const char *arch_name,
                         const cbf_model_shape_t *shape) {
    if (!model_path || !arch_name || !shape) return CBF_ERR_INVALID;
    
    char config_path[1024];
    snprintf(config_path, sizeof(config_path), "%s/model_config.json", model_path);
    
    FILE *f = fopen(config_path, "w");
    if (!f) {
        fprintf(stderr, "[cbf_arch] Failed to create %s\n", config_path);
        return CBF_ERR_IO;
    }
    
    fprintf(f, "{\n");
    fprintf(f, "  \"architecture\": \"%s\",\n", arch_name);
    fprintf(f, "  \"config\": {\n");
    fprintf(f, "    \"n_vocab\": %u,\n", shape->n_vocab);
    fprintf(f, "    \"d_model\": %u,\n", shape->d_model);
    fprintf(f, "    \"n_layers\": %u,\n", shape->n_layers);
    fprintf(f, "    \"n_heads\": %u,\n", shape->n_heads);
    fprintf(f, "    \"n_kv_heads\": %u,\n", shape->n_kv_heads);
    fprintf(f, "    \"head_dim\": %u,\n", shape->head_dim);
    fprintf(f, "    \"d_ffn\": %u,\n", shape->d_ffn);
    fprintf(f, "    \"n_experts_per_layer\": %u,\n", shape->n_experts_per_layer);
    fprintf(f, "    \"n_experts_active\": %u,\n", shape->n_experts_active);
    fprintf(f, "    \"n_shared_experts\": %u,\n", shape->n_shared_experts);
    fprintf(f, "    \"has_mtp_head\": %s,\n", shape->has_mtp_head ? "true" : "false");
    fprintf(f, "    \"expert_size_mb\": %u,\n", shape->expert_size_mb);
    fprintf(f, "    \"dense_size_mb\": %u,\n", shape->dense_size_mb);
    fprintf(f, "    \"kv_bytes_per_token\": %u,\n", shape->kv_bytes_per_token);
    fprintf(f, "    \"max_seq_len\": %u,\n", shape->max_seq_len);
    fprintf(f, "    \"rope_theta\": %.1f\n", shape->rope_theta);
    fprintf(f, "  }\n");
    fprintf(f, "}\n");
    
    fclose(f);
    
    printf("[cbf_arch] Saved configuration to %s\n", config_path);
    return CBF_OK;
}
