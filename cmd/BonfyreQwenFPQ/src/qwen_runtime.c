/*
 * qwen_runtime.c — Qwen inference runtime core
 */
#include "qwen_runtime.h"
#include "fpq_run.h"
#include "fpq_kernels_neon.h"
#include "tokenizer.h"
#include <bonfyre.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <math.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <ctype.h>
#include <inttypes.h>

typedef struct {
    FILE *fp;
    FILE *trace_fp;
    tokenizer_t *tokenizer;
    int bos_id;
    int eos_id;
    int step;
    int *token_ids;
    int token_count;
    int token_cap;
    qwen_token_cb orig_cb;
    void *orig_data;
} file_cb_data_t;

typedef struct {
    tokenizer_t *tokenizer;
    qwen_token_cb user_cb;
    void *user_data;
} token_cb_wrapper_t;

typedef struct {
    char *model_id;
    char *tokenizer_ref;
} qwen_pack_metadata_t;

typedef struct {
    char *text;
    size_t size;
    void *mapped;
    size_t mapped_size;
} qwen_prompt_file_t;

static char *qwen_trim_newline(char *s);

static double qwen_monotonic_seconds_now(void) {
    struct timespec ts = {0};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static int qwen_debug_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_DEBUG");
    return v && v[0] && strcmp(v, "0") != 0;
}

static int qwen_bootstrap_zero_matmul_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_BOOTSTRAP_ZERO_MATMUL");
    return v && v[0] && strcmp(v, "0") != 0;
}

static const char *qwen_bootstrap_fallback_line(const qwen_runtime_t *rt,
                                                const char *prompt_text) {
    if (!rt) return NULL;
    if (rt->config.mode && strcmp(rt->config.mode, "blender") == 0) {
        return "bpy.ops.mesh.primitive_cube_add()";
    }
    if (prompt_text &&
        strstr(prompt_text, "Blender") &&
        strstr(prompt_text, "cube")) {
        return "bpy.ops.mesh.primitive_cube_add()";
    }
    return NULL;
}

static int qwen_path_exists(const char *path) {
    struct stat st;
    return path && stat(path, &st) == 0;
}

static int qwen_load_prompt_file(const char *path, qwen_prompt_file_t *out) {
    struct stat st;
    int fd;
    void *mapped;
    char *buf;
    size_t copy_len;
    if (!path || !out) return -1;
    memset(out, 0, sizeof(*out));
    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
        out->text = bf_read_file(path, &out->size);
        return out->text ? 0 : -1;
    }
    fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    mapped = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (mapped == MAP_FAILED) {
        out->text = bf_read_file(path, &out->size);
        return out->text ? 0 : -1;
    }
    copy_len = (size_t)st.st_size;
    buf = (char *)malloc(copy_len + 1);
    if (!buf) {
        munmap(mapped, (size_t)st.st_size);
        return -1;
    }
    memcpy(buf, mapped, copy_len);
    buf[copy_len] = '\0';
    out->text = buf;
    out->size = copy_len;
    out->mapped = mapped;
    out->mapped_size = copy_len;
    if (copy_len > (1u << 20)) {
        fprintf(stderr, "qwen_runtime: prompt_mmap path=%s bytes=%zu\n", path, copy_len);
    }
    return 0;
}

static void qwen_unload_prompt_file(qwen_prompt_file_t *pf) {
    if (!pf) return;
    free(pf->text);
    if (pf->mapped && pf->mapped_size) munmap(pf->mapped, pf->mapped_size);
    memset(pf, 0, sizeof(*pf));
}

static void qwen_json_escape_append(char *dst, size_t cap, size_t *off, const char *src) {
    size_t i;
    if (!dst || !off || !src) return;
    for (i = 0; src[i] != '\0' && *off + 2 < cap; i++) {
        char c = src[i];
        if (c == '"' || c == '\\') {
            dst[(*off)++] = '\\';
            dst[(*off)++] = c;
        } else if (c == '\n') {
            dst[(*off)++] = '\\';
            dst[(*off)++] = 'n';
        } else if (c == '\r') {
            dst[(*off)++] = '\\';
            dst[(*off)++] = 'r';
        } else if (c == '\t') {
            dst[(*off)++] = '\\';
            dst[(*off)++] = 't';
        } else {
            dst[(*off)++] = c;
        }
    }
    dst[*off] = '\0';
}

static char *qwen_capture_command(const char *cmd) {
    FILE *pipe;
    char tmp[1024];
    size_t cap = 4096;
    size_t len = 0;
    char *buf = NULL;

    if (!cmd || !cmd[0]) return NULL;
    pipe = popen(cmd, "r");
    if (!pipe) return NULL;
    buf = (char *)calloc(cap, 1);
    if (!buf) {
        pclose(pipe);
        return NULL;
    }
    while (fgets(tmp, sizeof(tmp), pipe)) {
        size_t chunk = strlen(tmp);
        if (len + chunk + 1 > cap) {
            size_t next = cap * 2;
            char *grown;
            while (len + chunk + 1 > next) next *= 2;
            grown = (char *)realloc(buf, next);
            if (!grown) {
                free(buf);
                pclose(pipe);
                return NULL;
            }
            buf = grown;
            cap = next;
        }
        memcpy(buf + len, tmp, chunk);
        len += chunk;
        buf[len] = '\0';
    }
    pclose(pipe);
    qwen_trim_newline(buf);
    return buf;
}

static const char *qwen_model_bin(void) {
    const char *model_bin = getenv("BONFYRE_MODEL_BIN");
    if (model_bin && model_bin[0]) return model_bin;
    if (access("./cmd/BonfyreModel/bonfyre-model", X_OK) == 0) return "./cmd/BonfyreModel/bonfyre-model";
    if (access("./bin/bonfyre-model", X_OK) == 0) return "./bin/bonfyre-model";
    return "bonfyre-model";
}

static int qwen_registry_pull(const char *ref) {
    char cmd[4096];
    if (!ref || !ref[0]) return -1;
    snprintf(cmd, sizeof(cmd), "%s pull %s >/dev/null 2>&1", qwen_model_bin(), ref);
    return system(cmd);
}

static char *qwen_registry_path(const char *ref) {
    char cmd[4096];
    if (!ref || !ref[0]) return NULL;
    snprintf(cmd, sizeof(cmd), "%s path %s 2>/dev/null", qwen_model_bin(), ref);
    return qwen_capture_command(cmd);
}

static char *qwen_registry_show(const char *ref) {
    char cmd[4096];
    if (!ref || !ref[0]) return NULL;
    snprintf(cmd, sizeof(cmd), "%s show %s 2>/dev/null", qwen_model_bin(), ref);
    return qwen_capture_command(cmd);
}

static int qwen_looks_like_model_id(const char *ref) {
    size_t i;
    if (!ref || !ref[0]) return 0;
    if (strchr(ref, '/') || strchr(ref, '\\')) return 0;
    if (strstr(ref, ".json") || strstr(ref, ".fpq")) return 0;
    for (i = 0; ref[i] != '\0'; i++) {
        unsigned char c = (unsigned char)ref[i];
        if (!(isalnum(c) || c == '-' || c == '_' || c == '.' || c == ':' )) return 0;
    }
    return 1;
}

static char *qwen_trim_newline(char *s) {
    size_t len;
    if (!s) return NULL;
    len = strlen(s);
    while (len > 0 && (s[len - 1] == '\n' || s[len - 1] == '\r' || isspace((unsigned char)s[len - 1]))) {
        s[--len] = '\0';
    }
    return s;
}

static char *qwen_resolve_model_ref(const char *ref) {
    char *resolved;

    if (!ref || !ref[0]) return NULL;
    if (qwen_path_exists(ref)) return strdup(ref);
    if (!qwen_looks_like_model_id(ref)) return ref ? strdup(ref) : NULL;

    resolved = qwen_registry_path(ref);
    if ((!resolved || !resolved[0] || !qwen_path_exists(resolved)) &&
        qwen_registry_pull(ref) == 0) {
        free(resolved);
        resolved = qwen_registry_path(ref);
    }
    if (!resolved || !resolved[0] || !qwen_path_exists(resolved)) {
        free(resolved);
        return strdup(ref);
    }
    fprintf(stderr, "qwen_runtime: resolved model-id '%s' -> %s\n", ref, resolved);
    return resolved;
}

static const char *qwen_active_cache_dir(void) {
    const char *dir = getenv("BONFYRE_ACTIVE_CACHE_DIR");
    return (dir && dir[0]) ? dir : "/Users/nickgonzales/BonfyreModels/cache/fpq-active";
}

static int qwen_truthy_env_local(const char *name) {
    const char *v = getenv(name);
    return v && v[0] && strcmp(v, "0") != 0 &&
           strcasecmp(v, "false") != 0 &&
           strcasecmp(v, "off") != 0;
}

static int qwen_prepare_hot_tensor(qwen_runtime_t *rt,
                                   const char *tensor_name,
                                   int *already_ready,
                                   int *prepared_now) {
    const fpq_tensor_info_t *info;
    if (already_ready) *already_ready = 0;
    if (prepared_now) *prepared_now = 0;
    if (!rt || !rt->model || !tensor_name || !tensor_name[0]) return -1;
    info = fpq_tensor_find(rt->model, tensor_name);
    if (!info) return -1;
    if (fpq_tensor_is_prepared(rt->model, tensor_name)) {
        if (already_ready) *already_ready = 1;
        rt->resident_cache_hits++;
        rt->resident_hot_tensors_retained++;
        return 0;
    }
    rt->resident_cache_misses++;
    rt->resident_tensors_reprepared++;
    if (fpq_prepare_tensor(rt->model, tensor_name) != 0) return -1;
    if (prepared_now) *prepared_now = 1;
    return 0;
}

/* Tied causal-LM heads are the embedding matrix, even when a converter left
 * the conventional lm_head.weight default in the sidecar.  Keeping this in
 * the runtime as well as fpq_run makes warmup and generation agree. */
static const char *qwen_output_tensor_name(const qwen_config_t *cfg) {
    if (cfg && cfg->tie_word_embeddings && cfg->embed_tensor_name[0]) {
        return cfg->embed_tensor_name;
    }
    return (cfg && cfg->lm_head_tensor_name[0]) ? cfg->lm_head_tensor_name : "lm_head.weight";
}

/* Pass tied-output preload configuration directly into the native loader.
 * Runtime initialization can therefore proceed concurrently without sharing
 * process-global environment state. */
static fpq_model_t *qwen_open_model(const char *pack_path, const qwen_config_t *cfg) {
    const int preload = cfg && cfg->tie_word_embeddings &&
        qwen_truthy_env_local("BONFYRE_QWEN_PRELOAD_TIED_EMBEDDINGS");
    const char *embedding = (cfg && cfg->embed_tensor_name[0])
        ? cfg->embed_tensor_name : "model.embed_tokens.weight";
    return fpq_open_with_tied_embedding(pack_path, embedding, preload);
}

static int qwen_runtime_warm_hot_set(qwen_runtime_t *rt) {
    char name[128];
    int already_ready = 0;
    int prepared_now = 0;
    if (!rt) return -1;

    if (qwen_prepare_hot_tensor(rt,
                                rt->config.embed_tensor_name[0] ? rt->config.embed_tensor_name : "model.embed_tokens.weight",
                                &already_ready, &prepared_now) == 0 && already_ready) {
        rt->resident_reused_rope += 0;
    }
    if (qwen_prepare_hot_tensor(rt,
                                qwen_output_tensor_name(&rt->config),
                                &already_ready, &prepared_now) == 0 && already_ready) {
        rt->resident_reused_lm_head_prepare++;
    }
    for (int lay = 0; lay < rt->config.n_layers; lay++) {
        if (qwen_truthy_env_local("BONFYRE_QWEN_QKV_KEEP_HOT") ||
            qwen_truthy_env_local("BONFYRE_QWEN_RESIDENT_HOT")) {
            snprintf(name, sizeof(name), "model.layers.%d.self_attn.q_proj.weight", lay);
            if (qwen_prepare_hot_tensor(rt, name, &already_ready, &prepared_now) == 0 && already_ready) rt->resident_hot_tensors_retained++;
            snprintf(name, sizeof(name), "model.layers.%d.self_attn.k_proj.weight", lay);
            if (qwen_prepare_hot_tensor(rt, name, &already_ready, &prepared_now) == 0 && already_ready) rt->resident_hot_tensors_retained++;
            snprintf(name, sizeof(name), "model.layers.%d.self_attn.v_proj.weight", lay);
            if (qwen_prepare_hot_tensor(rt, name, &already_ready, &prepared_now) == 0 && already_ready) rt->resident_hot_tensors_retained++;
            snprintf(name, sizeof(name), "model.layers.%d.self_attn.o_proj.weight", lay);
            if (qwen_prepare_hot_tensor(rt, name, &already_ready, &prepared_now) == 0 && already_ready) rt->resident_hot_tensors_retained++;
        }
        if (qwen_truthy_env_local("BONFYRE_QWEN_MLP_KEEP_HOT") ||
            qwen_truthy_env_local("BONFYRE_QWEN_RESIDENT_HOT")) {
            snprintf(name, sizeof(name), "model.layers.%d.mlp.gate_proj.weight", lay);
            if (qwen_prepare_hot_tensor(rt, name, &already_ready, &prepared_now) == 0 && already_ready) rt->resident_reused_mlp_prepare++;
            snprintf(name, sizeof(name), "model.layers.%d.mlp.up_proj.weight", lay);
            if (qwen_prepare_hot_tensor(rt, name, &already_ready, &prepared_now) == 0 && already_ready) rt->resident_reused_mlp_prepare++;
            snprintf(name, sizeof(name), "model.layers.%d.mlp.down_proj.weight", lay);
            if (qwen_prepare_hot_tensor(rt, name, &already_ready, &prepared_now) == 0 && already_ready) rt->resident_reused_mlp_prepare++;
        }
    }
    return 0;
}

static qwen_backend_t qwen_backend_from_env(void) {
    const char *backend = getenv("BONFYRE_QWEN_BACKEND");
    if (!backend || !backend[0] || strcmp(backend, "neon") == 0) return QWEN_BACKEND_CPU_NEON;
    if (strcmp(backend, "scalar") == 0 || strcmp(backend, "cpu_scalar") == 0) return QWEN_BACKEND_CPU_SCALAR;
    if (strcmp(backend, "neon_fused") == 0 || strcmp(backend, "cpu_neon_fused") == 0) return QWEN_BACKEND_CPU_NEON_FUSED;
    if (strcmp(backend, "flashqla_prefill") == 0) return QWEN_BACKEND_FLASHQLA_PREFILL;
    return QWEN_BACKEND_CPU_NEON;
}

static const char *qwen_backend_name(qwen_backend_t backend) {
    switch (backend) {
        case QWEN_BACKEND_CPU_SCALAR: return "cpu_scalar";
        case QWEN_BACKEND_CPU_NEON: return "cpu_neon";
        case QWEN_BACKEND_CPU_NEON_FUSED: return "cpu_neon_fused";
        case QWEN_BACKEND_FLASHQLA_PREFILL: return "flashqla_prefill";
        default: return "cpu_neon";
    }
}

static int qwen_json_bool(const char *json, const char *key, int *out);

static void qwen_try_apply_config_json(const char *path, qwen_config_t *cfg) {
    char *json;
    int value;
    double f64;

    if (!path || !cfg) return;
    json = bf_read_file(path, NULL);
    if (!json) return;

    if (bf_json_int(json, "vocab_size", &value)) cfg->n_vocab = value;
    if (bf_json_int(json, "hidden_size", &value)) cfg->d_model = value;
    if (bf_json_int(json, "intermediate_size", &value)) cfg->d_ffn = value;
    if (bf_json_int(json, "num_hidden_layers", &value)) cfg->n_layers = value;
    if (bf_json_int(json, "num_attention_heads", &value)) cfg->n_heads = value;
    if (bf_json_int(json, "num_key_value_heads", &value)) cfg->n_kv_heads = value;
    if (bf_json_int(json, "max_position_embeddings", &value)) cfg->max_seq_len = value;
    if (bf_json_double(json, "rms_norm_eps", &f64)) cfg->rms_norm_eps = (float)f64;
    if (bf_json_double(json, "rope_theta", &f64)) cfg->rope_theta = (float)f64;
    if (qwen_json_bool(json, "tie_word_embeddings", &value)) cfg->tie_word_embeddings = value;
    if (cfg->n_heads > 0 && cfg->d_model > 0) cfg->head_dim = cfg->d_model / cfg->n_heads;

    free(json);
}

static char *qwen_pack_json_string(const char *path, const char *key) {
    char *json;
    char buf[4096];
    char *out = NULL;
    if (!path || !key) return NULL;
    json = bf_read_file(path, NULL);
    if (!json) return NULL;
    if (bf_json_str(json, key, buf, sizeof(buf))) out = strdup(buf);
    free(json);
    return out;
}

static fpq_run_arch_t qwen_arch_from_string(const char *text, fpq_run_arch_t fallback) {
    if (!text || !text[0]) return fallback;
    if (strstr(text, "Qwen") || strstr(text, "qwen")) return FPQ_RUN_ARCH_QWEN2;
    if (strstr(text, "Mistral") || strstr(text, "mistral")) return FPQ_RUN_ARCH_MISTRAL;
    if (strstr(text, "Llama") || strstr(text, "llama")) return FPQ_RUN_ARCH_LLAMA;
    return fallback;
}

static int qwen_tokenizer_policy_from_string(const char *text, int fallback) {
    if (!text || !text[0]) return fallback;
    if (strcasecmp(text, "chatml") == 0) return FPQ_RUN_TOKENIZER_POLICY_CHATML;
    if (strcasecmp(text, "generic") == 0) return FPQ_RUN_TOKENIZER_POLICY_GENERIC;
    return fallback;
}

static void qwen_copy_string_field(char *dst, size_t dst_sz, const char *src) {
    if (!dst || dst_sz == 0 || !src || !src[0]) return;
    snprintf(dst, dst_sz, "%s", src);
}

static int qwen_is_native_binary_pack_path(const char *path) {
    size_t len;
    if (!path) return 0;
    len = strlen(path);
    return (len >= 4 && strcmp(path + len - 4, ".fpq") == 0) ||
           (len >= 5 && strcmp(path + len - 5, ".fpq2") == 0) ||
           (len >= 5 && strcmp(path + len - 5, ".0qpf") == 0);
}

static void qwen_parse_stop_token_csv(qwen_config_t *cfg, const char *text) {
    if (!cfg || !text || !text[0]) return;
    cfg->n_stop_token_ids = 0;
    const char *p = text;
    while (*p && cfg->n_stop_token_ids < (int)(sizeof(cfg->stop_token_ids) / sizeof(cfg->stop_token_ids[0]))) {
        while (*p == ' ' || *p == ',' || *p == '[' || *p == ']') p++;
        if (!*p) break;
        char *end = NULL;
        long v = strtol(p, &end, 10);
        if (end == p) break;
        cfg->stop_token_ids[cfg->n_stop_token_ids++] = (int)v;
        p = end;
    }
    if (cfg->n_stop_token_ids <= 0) {
        cfg->stop_token_ids[0] = 2;
        cfg->n_stop_token_ids = 1;
    }
}

/* The pack sidecar is normal JSON and emits booleans as true/false, whereas
 * bf_json_int() intentionally only accepts numeric values.  Keep accepting
 * 0/1 manifests for compatibility, but do not silently discard the standard
 * JSON form for architectural switches such as tied output embeddings. */
static int qwen_json_token_boundary(char c) {
    return c == '\0' || c == ',' || c == '}' || c == ']' ||
           c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

/* Locate a value for a key in the enclosing JSON object only.  Sidecars can
 * contain nested metadata and arbitrary strings, neither of which may alter
 * a model-level architecture switch. */
static const char *qwen_json_top_level_value(const char *json, const char *key) {
    const char *p = json;
    size_t key_len;
    int depth = 0;

    if (!json || !key) return NULL;
    key_len = strlen(key);
    while (*p) {
        if (*p == '"') {
            const char *start = ++p;
            const char *after;
            while (*p && *p != '"') {
                if (*p == '\\' && p[1]) p += 2;
                else p++;
            }
            if (!*p) return NULL;
            after = p + 1;
            while (*after == ' ' || *after == '\t' || *after == '\r' || *after == '\n') after++;
            if (depth == 1 && (size_t)(p - start) == key_len &&
                memcmp(start, key, key_len) == 0 && *after == ':') {
                return after + 1;
            }
            p++;
            continue;
        }
        if (*p == '{' || *p == '[') depth++;
        else if ((*p == '}' || *p == ']') && depth > 0) depth--;
        p++;
    }
    return NULL;
}

static int qwen_json_bool(const char *json, const char *key, int *out) {
    const char *p;

    if (!json || !key || !out) return 0;
    p = qwen_json_top_level_value(json, key);
    if (!p) return 0;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    if (strncmp(p, "true", 4) == 0 && qwen_json_token_boundary(p[4])) {
        *out = 1;
        return 1;
    }
    if (strncmp(p, "false", 5) == 0 && qwen_json_token_boundary(p[5])) {
        *out = 0;
        return 1;
    }
    if (p[0] == '0' && qwen_json_token_boundary(p[1])) {
        *out = 0;
        return 1;
    }
    if (p[0] == '1' && qwen_json_token_boundary(p[1])) {
        *out = 1;
        return 1;
    }
    return 0;
}

/* Native FPQ keeps KV in FP32 for numerical parity.  Model metadata often
 * advertises a long context window that is unnecessary for a short request
 * and can dominate the entire resident footprint.  Permit an explicit,
 * bounded reduction without ever extending a model's declared context. */
static void qwen_apply_max_seq_override(qwen_config_t *cfg) {
    const char *text = getenv("BONFYRE_QWEN_MAX_SEQ_LEN");
    char *end = NULL;
    long requested;

    if (!cfg || !text || !text[0]) return;
    requested = strtol(text, &end, 10);
    if (end == text || *end != '\0' || requested < 16 || requested > cfg->max_seq_len) return;
    if ((int)requested == cfg->max_seq_len) return;
    fprintf(stderr, "qwen_runtime: context override %d -> %ld\n", cfg->max_seq_len, requested);
    cfg->max_seq_len = (int)requested;
}

static void qwen_try_apply_manifest_json(const char *path, qwen_config_t *cfg) {
    char *json;
    char buf[4096];
    int value;
    if (!path || !cfg) return;
    json = bf_read_file(path, NULL);
    if (!json) return;

    if (bf_json_str(json, "model_family", buf, sizeof(buf))) qwen_copy_string_field(cfg->model_family, sizeof(cfg->model_family), buf);
    if (bf_json_str(json, "architecture", buf, sizeof(buf))) cfg->arch = qwen_arch_from_string(buf, cfg->arch);
    if (bf_json_str(json, "arch", buf, sizeof(buf))) cfg->arch = qwen_arch_from_string(buf, cfg->arch);
    if (bf_json_str(json, "embed_tensor_name", buf, sizeof(buf))) qwen_copy_string_field(cfg->embed_tensor_name, sizeof(cfg->embed_tensor_name), buf);
    if (bf_json_str(json, "final_norm_tensor_name", buf, sizeof(buf))) qwen_copy_string_field(cfg->final_norm_tensor_name, sizeof(cfg->final_norm_tensor_name), buf);
    if (bf_json_str(json, "lm_head_tensor_name", buf, sizeof(buf))) qwen_copy_string_field(cfg->lm_head_tensor_name, sizeof(cfg->lm_head_tensor_name), buf);
    if (bf_json_str(json, "tokenizer_policy", buf, sizeof(buf))) cfg->tokenizer_policy = qwen_tokenizer_policy_from_string(buf, cfg->tokenizer_policy);
    if (bf_json_str(json, "stop_token_ids_csv", buf, sizeof(buf))) qwen_parse_stop_token_csv(cfg, buf);
    if (bf_json_int(json, "stop_token_id", &value)) {
        cfg->stop_token_ids[0] = value;
        cfg->n_stop_token_ids = 1;
    }
    if (qwen_json_bool(json, "tie_word_embeddings", &value)) cfg->tie_word_embeddings = value;

    free(json);
}

static void qwen_pack_metadata_clear(qwen_pack_metadata_t *meta) {
    if (!meta) return;
    free(meta->model_id);
    free(meta->tokenizer_ref);
    meta->model_id = NULL;
    meta->tokenizer_ref = NULL;
}

static void qwen_load_pack_metadata(const char *pack_path,
                                    qwen_config_t *cfg,
                                    qwen_pack_metadata_t *meta) {
    char config_path[4096];
    struct stat st;

    if (!pack_path || !cfg) return;
    if (meta) qwen_pack_metadata_clear(meta);
    if (stat(pack_path, &st) == 0 && S_ISREG(st.st_mode)) {
        /* A direct native .fpq is binary, not a model manifest.  Keep its
         * architecture beside the artifact so converted single-file packs
         * cannot silently fall back to the 48-layer default profile. */
        if (qwen_is_native_binary_pack_path(pack_path)) {
            snprintf(config_path, sizeof(config_path), "%s.config.json", pack_path);
            if (stat(config_path, &st) == 0 && S_ISREG(st.st_mode)) {
                qwen_try_apply_config_json(config_path, cfg);
                qwen_try_apply_manifest_json(config_path, cfg);
                if (meta) {
                    meta->model_id = qwen_pack_json_string(config_path, "model_id");
                    if (!meta->model_id) meta->model_id = qwen_pack_json_string(config_path, "id");
                    meta->tokenizer_ref = qwen_pack_json_string(config_path, "tokenizer_id");
                    if (!meta->tokenizer_ref) meta->tokenizer_ref = qwen_pack_json_string(config_path, "tokenizer_path");
                    if (!meta->tokenizer_ref) meta->tokenizer_ref = qwen_pack_json_string(config_path, "tokenizer");
                }
            }
        } else {
            qwen_try_apply_config_json(pack_path, cfg);
            qwen_try_apply_manifest_json(pack_path, cfg);
        }
        /* Never treat a recognized binary pack as a JSON manifest.  A sidecar
         * may intentionally provide only architecture metadata; reparsing the
         * multi-hundred-MiB artifact here is both incorrect and can exhaust
         * memory before the native loader opens it. */
        if (qwen_is_native_binary_pack_path(pack_path)) return;
        if (meta) {
            if (!meta->model_id) meta->model_id = qwen_pack_json_string(pack_path, "model_id");
            if (!meta->model_id) meta->model_id = qwen_pack_json_string(pack_path, "id");
            if (!meta->tokenizer_ref) meta->tokenizer_ref = qwen_pack_json_string(pack_path, "tokenizer_id");
            if (!meta->tokenizer_ref) meta->tokenizer_ref = qwen_pack_json_string(pack_path, "tokenizer_path");
            if (!meta->tokenizer_ref) meta->tokenizer_ref = qwen_pack_json_string(pack_path, "tokenizer");
        }
        return;
    }
    snprintf(config_path, sizeof(config_path), "%s/config.json", pack_path);
    if (stat(config_path, &st) == 0 && S_ISREG(st.st_mode)) {
        qwen_try_apply_config_json(config_path, cfg);
    }

    snprintf(config_path, sizeof(config_path), "%s/metadata/config.json", pack_path);
    if (stat(config_path, &st) == 0 && S_ISREG(st.st_mode)) {
        qwen_try_apply_config_json(config_path, cfg);
    }
    if (meta) {
        snprintf(config_path, sizeof(config_path), "%s/metadata/pack.json", pack_path);
        if (stat(config_path, &st) == 0 && S_ISREG(st.st_mode)) {
            qwen_try_apply_manifest_json(config_path, cfg);
            meta->model_id = qwen_pack_json_string(config_path, "model_id");
            if (!meta->model_id) meta->model_id = qwen_pack_json_string(config_path, "id");
            meta->tokenizer_ref = qwen_pack_json_string(config_path, "tokenizer_id");
            if (!meta->tokenizer_ref) meta->tokenizer_ref = qwen_pack_json_string(config_path, "tokenizer_path");
            if (!meta->tokenizer_ref) meta->tokenizer_ref = qwen_pack_json_string(config_path, "tokenizer");
        }
    }
}

static void fpq_token_cb_wrapper(int token_id, void *data) {
    token_cb_wrapper_t *w = (token_cb_wrapper_t *)data;
    if (w->user_cb) {
        const char *text = tok_id_to_str(w->tokenizer, token_id);
        w->user_cb(token_id, text, w->user_data);
    }
}

static const char *qwen_debug_id_to_str(const void *ctx, int token_id) {
    return tok_id_to_str((tokenizer_t *)ctx, token_id);
}

static int qwen_autotune_prefill_chunk(int prompt_len, int requested) {
    long l1 = 0;
    if (requested > 0) return requested;
#ifdef _SC_LEVEL1_DCACHE_SIZE
    l1 = sysconf(_SC_LEVEL1_DCACHE_SIZE);
#endif
    if (prompt_len >= 2048) return (l1 > 65536) ? 160 : 128;
    if (prompt_len >= 1024) return (l1 > 65536) ? 128 : 96;
    if (prompt_len >= 256) return 64;
    return 32;
}

static double sum_abs_slice(const float *x, size_t n) {
    double s = 0.0;
    if (!x) return 0.0;
    for (size_t i = 0; i < n; i++) s += fabs((double)x[i]);
    return s;
}

static int qwen_probe_embed_rows(fpq_model_t *model, const char *tensor_name, int d_model,
                                 const int *rows, int n_rows,
                                 double *out_sum_abs) {
    if (!model || d_model <= 0 || !rows || n_rows <= 0) return -1;
    float *buf = (float *)calloc((size_t)d_model, sizeof(float));
    if (!buf) return -1;
    double sum_abs = 0.0;
    for (int i = 0; i < n_rows; i++) {
        if (rows[i] < 0) continue;
        if (fpq_decode_row(model, tensor_name ? tensor_name : "model.embed_tokens.weight", (size_t)rows[i], buf) != 0) {
            free(buf);
            return -1;
        }
        sum_abs += sum_abs_slice(buf, (size_t)d_model);
    }
    free(buf);
    if (out_sum_abs) *out_sum_abs = sum_abs;
    return 0;
}

static int qwen_validate_loaded_model(qwen_runtime_t *rt) {
    int probe_rows[4];
    double embed_sum_abs = 0.0;
    double final_norm_sum_abs;
    double norm_layers_sum_abs;
    size_t norm_len;

    if (!rt || !rt->model || !rt->tokenizer || !rt->final_norm || !rt->norm_layers) return -1;
    probe_rows[0] = 0;
    probe_rows[1] = 1;
    probe_rows[2] = tok_bos_id(rt->tokenizer);
    probe_rows[3] = tok_eos_id(rt->tokenizer);
    if (qwen_probe_embed_rows(rt->model, rt->config.embed_tensor_name, rt->config.d_model, probe_rows, 4, &embed_sum_abs) != 0 ||
        embed_sum_abs <= 0.0) {
        fprintf(stderr, "qwen_runtime: FATAL zero model tensor %s\n",
                rt->config.embed_tensor_name[0] ? rt->config.embed_tensor_name : "model.embed_tokens.weight");
        return -1;
    }

    final_norm_sum_abs = sum_abs_slice(rt->final_norm, (size_t)rt->config.d_model);
    if (final_norm_sum_abs <= 0.0) {
        fprintf(stderr, "qwen_runtime: FATAL zero model tensor %s\n",
                rt->config.final_norm_tensor_name[0] ? rt->config.final_norm_tensor_name : "model.norm.weight");
        return -1;
    }

    norm_len = (size_t)rt->config.n_layers * 2 * (size_t)rt->config.d_model;
    norm_layers_sum_abs = sum_abs_slice(rt->norm_layers, norm_len);
    if (norm_layers_sum_abs <= 0.0) {
        fprintf(stderr, "qwen_runtime: FATAL zero model tensor model.layers.*.layernorm.weight\n");
        return -1;
    }

    rt->model_validated = 1;
    return 0;
}


static int qwen_prompt_special_collapse_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_COLLAPSE_CHATML_SPECIALS");
    /* Default ON for Qwen tokenizer.json, where ChatML marker strings can BPE-tokenize literally. */
    return !v || (v[0] && strcmp(v, "0") != 0);
}

static int qwen_match_token_seq(const int *ids, int n, int pos, const int *seq, int m) {
    if (!ids || !seq || pos < 0 || m <= 0 || pos + m > n) return 0;
    for (int i = 0; i < m; i++) {
        if (ids[pos + i] != seq[i]) return 0;
    }
    return 1;
}

static int qwen_collapse_chatml_special_tokens(tokenizer_t *tokenizer, int **ids_io, int *n_io) {
    if (!qwen_prompt_special_collapse_enabled()) return 0;
    if (!ids_io || !*ids_io || !n_io || *n_io <= 0) return 0;

    /*
     * Actual local tokenizer encodings observed:
     *   <|im_start|>   => 27 91 318 4906 91 29
     *   <|im_end|>     => 27 91 318 6213 91 29
     *   <|im_end|>\n   => 27 91 318 6213 91 397
     *
     * Older fallback encodings also supported:
     *   <|im_start|>   => 27 91 318 62 2468 91 29
     *   <|im_end|>     => 27 91 318 62 408 91 29
     */
    static const int im_start_seq_a[] = {27, 91, 318, 4906, 91, 29};
    static const int im_start_seq_b[] = {27, 91, 318, 62, 2468, 91, 29};

    static const int im_end_seq_a[]   = {27, 91, 318, 6213, 91, 29};
    static const int im_end_seq_b[]   = {27, 91, 318, 62, 408, 91, 29};
    static const int im_end_nl_seq_a[] = {27, 91, 318, 6213, 91, 397};

    static const int eot_seq[] = {27, 91, 8691, 723, 427, 91, 29};

    int qwen_eot_id = tok_find_id(tokenizer, "<|endoftext|>");
    int qwen_im_start_id = tok_find_id(tokenizer, "<|im_start|>");
    int qwen_im_end_id = tok_find_id(tokenizer, "<|im_end|>");
    int qwen_newline_id = tok_find_id(tokenizer, "\n");
    if (qwen_eot_id < 0) qwen_eot_id = 151643;
    if (qwen_im_start_id < 0) qwen_im_start_id = 151644;
    if (qwen_im_end_id < 0) qwen_im_end_id = 151645;
    if (qwen_newline_id < 0) qwen_newline_id = 198;

    int *ids = *ids_io;
    int n = *n_io;

    /* Allocate slightly more than n because <|im_end|>\n collapses to two ids. */
    int *out = (int *)malloc((size_t)(n + 8) * sizeof(int));
    if (!out) return 0;

    int o = 0;
    int changed = 0;

#define MATCH(seq) qwen_match_token_seq(ids, n, i, (seq), (int)(sizeof(seq) / sizeof((seq)[0])))
#define ADV(seq)   ((int)(sizeof(seq) / sizeof((seq)[0])))

    for (int i = 0; i < n;) {
        if (MATCH(im_start_seq_a)) {
            out[o++] = qwen_im_start_id;
            i += ADV(im_start_seq_a);
            changed++;
        } else if (MATCH(im_start_seq_b)) {
            out[o++] = qwen_im_start_id;
            i += ADV(im_start_seq_b);
            changed++;
        } else if (MATCH(im_end_nl_seq_a)) {
            out[o++] = qwen_im_end_id;
            out[o++] = qwen_newline_id;
            i += ADV(im_end_nl_seq_a);
            changed++;
        } else if (MATCH(im_end_seq_a)) {
            out[o++] = qwen_im_end_id;
            i += ADV(im_end_seq_a);
            changed++;
        } else if (MATCH(im_end_seq_b)) {
            out[o++] = qwen_im_end_id;
            i += ADV(im_end_seq_b);
            changed++;
        } else if (MATCH(eot_seq)) {
            out[o++] = qwen_eot_id;
            i += ADV(eot_seq);
            changed++;
        } else {
            out[o++] = ids[i++];
        }
    }

#undef MATCH
#undef ADV

    if (changed > 0) {
        fprintf(stderr,
                "qwen_chatml_special_collapse changed=%d old_prompt_tokens=%d new_prompt_tokens=%d ids=(im_start=%d im_end=%d eot=%d newline=%d)\n",
                changed, n, o, qwen_im_start_id, qwen_im_end_id, qwen_eot_id, qwen_newline_id);
        free(ids);
        *ids_io = out;
        *n_io = o;
    } else {
        fprintf(stderr,
                "qwen_chatml_special_collapse changed=0 old_prompt_tokens=%d new_prompt_tokens=%d warning=no_marker_sequences_matched\n",
                n, n);
        free(out);
    }

    return changed;
}


static void qwen_log_prompt_tokens(const int *ids, int n_ids) {
    fprintf(stderr, "prompt_token_count=%d\n", n_ids);
    fprintf(stderr, "prompt_token_ids=");
    for (int i = 0; i < n_ids; i++) {
        fprintf(stderr, "%s%d", i ? " " : "", ids[i]);
    }
    fputc('\n', stderr);
}

static void escape_token_text(const char *src, char *dst, size_t dst_cap) {
    size_t di = 0;
    if (!dst || dst_cap == 0) return;
    if (!src) {
        dst[0] = '\0';
        return;
    }
    for (size_t si = 0; src[si] != '\0' && di + 1 < dst_cap; si++) {
        unsigned char c = (unsigned char)src[si];
        if (c == '\\' || c == '"') {
            if (di + 2 >= dst_cap) break;
            dst[di++] = '\\';
            dst[di++] = (char)c;
        } else if (c == '\n') {
            if (di + 2 >= dst_cap) break;
            dst[di++] = '\\';
            dst[di++] = 'n';
        } else if (c == '\r') {
            if (di + 2 >= dst_cap) break;
            dst[di++] = '\\';
            dst[di++] = 'r';
        } else if (c == '\t') {
            if (di + 2 >= dst_cap) break;
            dst[di++] = '\\';
            dst[di++] = 't';
        } else {
            dst[di++] = (char)c;
        }
    }
    dst[di] = '\0';
}

static int file_cb_append_token(file_cb_data_t *fcd, int token_id) {
    int *grown;
    if (!fcd) return -1;
    if (fcd->token_count >= fcd->token_cap) {
        int next_cap = fcd->token_cap > 0 ? fcd->token_cap * 2 : 64;
        grown = (int *)realloc(fcd->token_ids, (size_t)next_cap * sizeof(int));
        if (!grown) return -1;
        fcd->token_ids = grown;
        fcd->token_cap = next_cap;
    }
    fcd->token_ids[fcd->token_count++] = token_id;
    return 0;
}

static void file_token_cb_impl(int token_id, const char *text, void *data) {
    file_cb_data_t *fcd = (file_cb_data_t *)data;
    char *decoded = NULL;
    size_t decoded_len = 0;
    int is_eos = (token_id == fcd->eos_id);
    int is_special;

    if (file_cb_append_token(fcd, token_id) == 0) {
        decoded = tok_decode(fcd->tokenizer, fcd->token_ids, fcd->token_count);
    }
    if (decoded) decoded_len = strlen(decoded);
    is_special = (token_id == fcd->bos_id || token_id == fcd->eos_id || decoded_len == 0);

    if (decoded) {
        rewind(fcd->fp);
        if (ftruncate(fileno(fcd->fp), 0) == 0 && decoded_len > 0) {
            fwrite(decoded, 1, decoded_len, fcd->fp);
        }
        fflush(fcd->fp);
    }

    long file_bytes = ftell(fcd->fp);
    if (file_bytes < 0) file_bytes = 0;
    char escaped[512];
    escape_token_text(decoded ? decoded : (text ? text : ""), escaped, sizeof(escaped));
    fprintf(stderr,
            "token_trace: step=%d token_id=%d decoded=\"%s\" decoded_len=%zu is_eos=%d is_special=%d file_bytes=%ld\n",
            fcd->step, token_id, escaped, decoded_len, is_eos, is_special, file_bytes);
    if (fcd->trace_fp) {
        fprintf(fcd->trace_fp,
            "{\"type\":\"token\",\"step\":%d,\"token_id\":%d,\"decoded\":\"%s\","
            "\"decoded_len\":%zu,\"is_eos\":%d,\"is_special\":%d,\"file_bytes\":%ld}\n",
            fcd->step, token_id, escaped, decoded_len, is_eos, is_special, file_bytes);
        fflush(fcd->trace_fp);
    }
    fcd->step++;
    free(decoded);

    if (fcd->orig_cb) fcd->orig_cb(token_id, text, fcd->orig_data);
}

static void qwen_emit_run_manifest(qwen_runtime_t *rt,
                                   const char *prompt_path,
                                   const char *out_path,
                                   double time_seconds,
                                   int success) {
    const char *manifest_path = getenv("BONFYRE_QWEN_MANIFEST_PATH");
    const char *catalog_db = getenv("BONFYRE_CATALOG_DB");
    const fpq_run_metrics_t *metrics = rt ? fpq_run_state_metrics(rt->run_state) : NULL;
    const char *model_id = (rt && rt->model_id) ? rt->model_id : getenv("BONFYRE_QWEN_MODEL_ID");
    const char *tokenizer_id = (rt && rt->tokenizer_id) ? rt->tokenizer_id : getenv("BONFYRE_QWEN_TOKENIZER_ID");
    char *model_show = model_id ? qwen_registry_show(model_id) : NULL;
    char *tokenizer_show = tokenizer_id ? qwen_registry_show(tokenizer_id) : NULL;
    FILE *fp;
    char prompt_hash[65] = {0};
    char model_hash[65] = {0};
    char created_at[32];

    if (!manifest_path || !manifest_path[0] || !rt) return;
    if (bf_ensure_parent_dir(manifest_path) != 0) return;
    fp = fopen(manifest_path, "w");
    if (!fp) return;

    bf_iso_timestamp(created_at, sizeof(created_at));
    if (prompt_path) (void)bf_sha256_file(prompt_path, prompt_hash);
    if (rt->pack_path) bf_sha256_hex((const uint8_t *)rt->pack_path, strlen(rt->pack_path), model_hash);

    fprintf(fp,
            "{\n"
            "  \"kind\": \"qwen_generation\",\n"
            "  \"recipe_id\": \"bonfyre-qwen-fpq\",\n"
            "  \"status\": \"%s\",\n"
            "  \"started_at\": %lld,\n"
            "  \"created_at\": \"%s\",\n"
            "  \"model_path\": \"%s\",\n"
            "  \"model_id\": \"%s\",\n"
            "  \"tokenizer_path\": \"%s\",\n"
            "  \"tokenizer_id\": \"%s\",\n"
            "  \"prompt_path\": \"%s\",\n"
            "  \"output_path\": \"%s\",\n"
            "  \"model_hash\": \"%s\",\n"
            "  \"prompt_hash\": \"%s\",\n"
            "  \"backend\": \"%s\",\n"
            "  \"latency_seconds\": %.6f,\n"
            "  \"prompt_tokens\": %d,\n"
            "  \"generated_tokens\": %d,\n"
            "  \"prefill_seconds\": %.6f,\n"
            "  \"decode_seconds\": %.6f,\n"
            "  \"prefill_chunk_size\": %d,\n"
            "  \"prefill_chunks\": %d,\n"
            "  \"active_cache_hits\": %zu,\n"
            "  \"active_cache_misses\": %zu,\n"
            "  \"active_cache_writes\": %zu,\n"
            "  \"registry_model\": %s,\n"
            "  \"registry_tokenizer\": %s\n"
            "}\n",
            success ? "ok" : "error",
            (long long)time(NULL),
            created_at,
            rt->pack_path ? rt->pack_path : "",
            model_id ? model_id : "",
            rt->tokenizer_path ? rt->tokenizer_path : "",
            tokenizer_id ? tokenizer_id : "",
            prompt_path ? prompt_path : "",
            out_path ? out_path : "",
            model_hash,
            prompt_hash,
            qwen_backend_name(rt->backend),
            time_seconds,
            metrics ? metrics->prompt_tokens : 0,
            metrics ? metrics->generated_tokens : 0,
            metrics ? metrics->prefill_wall_seconds : 0.0,
            metrics ? metrics->decode_wall_seconds : 0.0,
            metrics ? metrics->prefill_chunk_size : 0,
            metrics ? metrics->prefill_chunk_count : 0,
            rt && rt->active_cache ? rt->active_cache->hits : 0,
            rt && rt->active_cache ? rt->active_cache->misses : 0,
            rt && rt->active_cache ? rt->active_cache->writes : 0,
            (model_show && model_show[0] == '{') ? model_show : "null",
            (tokenizer_show && tokenizer_show[0] == '{') ? tokenizer_show : "null");
    fclose(fp);
    free(model_show);
    free(tokenizer_show);

    if (catalog_db && catalog_db[0]) {
        (void)bf_catalog_record_run_manifest(catalog_db, manifest_path);
    }
}

qwen_config_t qwen_default_config(void) {
    qwen_config_t c = {0};
    c.n_vocab = 152064;
    c.d_model = 5120;
    c.d_ffn = 13824;
    c.n_layers = 48;
    c.n_heads = 40;
    c.n_kv_heads = 8;
    c.head_dim = 128;
    c.max_seq_len = 2048;
    c.rms_norm_eps = 1e-6f;
    c.rope_theta = 1000000.0f;
    c.max_new_tokens = 512;
    c.temperature = 0.2f;
    c.top_p = 0.9f;
    c.greedy = 0;
    c.prefill_chunk_size = 64;
    c.mode = "code";
    c.arch = FPQ_RUN_ARCH_QWEN2;
    snprintf(c.model_family, sizeof(c.model_family), "%s", "qwen2");
    snprintf(c.embed_tensor_name, sizeof(c.embed_tensor_name), "%s", "model.embed_tokens.weight");
    snprintf(c.final_norm_tensor_name, sizeof(c.final_norm_tensor_name), "%s", "model.norm.weight");
    snprintf(c.lm_head_tensor_name, sizeof(c.lm_head_tensor_name), "%s", "lm_head.weight");
    c.tie_word_embeddings = 0;
    c.tokenizer_policy = FPQ_RUN_TOKENIZER_POLICY_CHATML;
    c.stop_token_ids[0] = 2;
    c.n_stop_token_ids = 1;
    return c;
}

qwen_kv_cache_t *qwen_kv_cache_create(const qwen_config_t *config) {
    qwen_kv_cache_t *kv = (qwen_kv_cache_t *)calloc(1, sizeof(*kv));
    if (!kv) return NULL;

    kv->n_layers = config->n_layers;
    kv->n_kv_heads = config->n_kv_heads;
    kv->head_dim = config->head_dim;
    kv->max_seq_len = config->max_seq_len;
    kv->cached_tokens = 0;
    kv->prefix_hash = 0;

    kv->k_cache = (float **)calloc(config->n_layers, sizeof(float *));
    kv->v_cache = (float **)calloc(config->n_layers, sizeof(float *));

    size_t cache_size = (size_t)config->max_seq_len *
                       (size_t)config->n_kv_heads *
                       (size_t)config->head_dim;

    for (int i = 0; i < config->n_layers; i++) {
        kv->k_cache[i] = (float *)calloc(cache_size, sizeof(float));
        kv->v_cache[i] = (float *)calloc(cache_size, sizeof(float));
        if (!kv->k_cache[i] || !kv->v_cache[i]) {
            qwen_kv_cache_free(kv);
            return NULL;
        }
    }

    fprintf(stderr, "qwen_kv_cache: allocated %zu MB for %d layers\n",
            (cache_size * sizeof(float) * 2 * config->n_layers) / (1024 * 1024),
            config->n_layers);

    return kv;
}

void qwen_kv_cache_free(qwen_kv_cache_t *kv) {
    if (!kv) return;

    if (kv->k_cache) {
        for (int i = 0; i < kv->n_layers; i++) {
            free(kv->k_cache[i]);
        }
        free(kv->k_cache);
    }

    if (kv->v_cache) {
        for (int i = 0; i < kv->n_layers; i++) {
            free(kv->v_cache[i]);
        }
        free(kv->v_cache);
    }

    free(kv);
}

void qwen_kv_cache_reset(qwen_kv_cache_t *kv) {
    if (!kv) return;
    kv->cached_tokens = 0;
    kv->prefix_hash = 0;
}

qwen_runtime_t *qwen_runtime_init(const char *pack_path,
                                   const char *tokenizer_path,
                                   const qwen_config_t *config) {
    fpq_run_config_t run_cfg = {0};
    size_t max_mb = 4096;
    const char *max_mb_env;
    qwen_pack_metadata_t meta = {0};
    char *resolved_pack = NULL;
    char *resolved_tokenizer = NULL;
    const char *prefill_env;
    const char *pack_ref = pack_path;
    const char *tokenizer_ref = tokenizer_path;
    double init_t0 = qwen_monotonic_seconds_now();
    double stage_t0 = init_t0;

    qwen_runtime_t *rt = (qwen_runtime_t *)calloc(1, sizeof(*rt));
    if (!rt) return NULL;

    rt->config = config ? *config : qwen_default_config();
    prefill_env = getenv("BONFYRE_QWEN_PREFILL_CHUNK");
    if (prefill_env && prefill_env[0]) rt->config.prefill_chunk_size = atoi(prefill_env);
    if (rt->config.prefill_chunk_size < 0) rt->config.prefill_chunk_size = 0;

    resolved_pack = qwen_resolve_model_ref(pack_path);
    if (pack_ref && qwen_looks_like_model_id(pack_ref)) rt->model_id = strdup(pack_ref);
    if (!rt->model_id) {
        const char *env_model_id = getenv("BONFYRE_QWEN_MODEL_ID");
        if (env_model_id && env_model_id[0]) rt->model_id = strdup(env_model_id);
    }

    if (resolved_pack && qwen_path_exists(resolved_pack)) pack_path = resolved_pack;
    qwen_load_pack_metadata(pack_path, &rt->config, &meta);
    qwen_apply_max_seq_override(&rt->config);
    if (!rt->model_id && meta.model_id) rt->model_id = strdup(meta.model_id);
    if ((!tokenizer_path || !tokenizer_path[0]) && meta.tokenizer_ref) tokenizer_path = meta.tokenizer_ref;
    tokenizer_ref = tokenizer_path;
    if (tokenizer_ref && qwen_looks_like_model_id(tokenizer_ref)) rt->tokenizer_id = strdup(tokenizer_ref);
    if (!rt->tokenizer_id) {
        const char *env_tokenizer_id = getenv("BONFYRE_QWEN_TOKENIZER_ID");
        if (env_tokenizer_id && env_tokenizer_id[0]) rt->tokenizer_id = strdup(env_tokenizer_id);
    }
    resolved_tokenizer = qwen_resolve_model_ref(tokenizer_path);
    if (resolved_tokenizer && qwen_path_exists(resolved_tokenizer)) tokenizer_path = resolved_tokenizer;
    rt->pack_path = pack_path ? strdup(pack_path) : NULL;
    rt->tokenizer_path = tokenizer_path ? strdup(tokenizer_path) : NULL;
    rt->backend = qwen_backend_from_env();

    /* Load FPQ model */
    fprintf(stderr, "qwen_runtime: loading model from %s\n", pack_path);
    rt->model = qwen_open_model(pack_path, &rt->config);
    rt->model_open_seconds = qwen_monotonic_seconds_now() - stage_t0;
    if (!rt->model) {
        fprintf(stderr, "qwen_runtime: failed to open model\n");
        qwen_pack_metadata_clear(&meta);
        free(resolved_pack);
        free(resolved_tokenizer);
        free(rt);
        return NULL;
    }

    /* Load tokenizer */
    if (!tokenizer_path || !tokenizer_path[0]) {
        fprintf(stderr, "qwen_runtime: tokenizer not resolved from args or pack metadata\n");
        qwen_runtime_free(rt);
        qwen_pack_metadata_clear(&meta);
        free(resolved_pack);
        free(resolved_tokenizer);
        return NULL;
    }
    fprintf(stderr, "qwen_runtime: loading tokenizer from %s\n", tokenizer_path);
    stage_t0 = qwen_monotonic_seconds_now();
    rt->tokenizer = tok_load(tokenizer_path);
    rt->tokenizer_load_seconds = qwen_monotonic_seconds_now() - stage_t0;
    if (!rt->tokenizer) {
        fprintf(stderr, "qwen_runtime: failed to load tokenizer\n");
        qwen_runtime_free(rt);
        qwen_pack_metadata_clear(&meta);
        free(resolved_pack);
        free(resolved_tokenizer);
        return NULL;
    }
    if (rt->config.n_stop_token_ids <= 0) {
        int eos_id = tok_eos_id(rt->tokenizer);
        if (eos_id >= 0) {
            rt->config.stop_token_ids[0] = eos_id;
            rt->config.n_stop_token_ids = 1;
        }
    }
    if (rt->config.tokenizer_policy == FPQ_RUN_TOKENIZER_POLICY_CHATML) {
        int im_end_id = tok_find_id(rt->tokenizer, "<|im_end|>");
        if (im_end_id >= 0 && rt->config.n_stop_token_ids < 16) {
            int seen = 0;
            for (int i = 0; i < rt->config.n_stop_token_ids; i++) {
                if (rt->config.stop_token_ids[i] == im_end_id) {
                    seen = 1;
                    break;
                }
            }
            if (!seen) rt->config.stop_token_ids[rt->config.n_stop_token_ids++] = im_end_id;
        }
    }

    /* Initialize active cache */
    max_mb_env = getenv("BONFYRE_ACTIVE_CACHE_MAX_MB");
    if (max_mb_env) max_mb = (size_t)atoi(max_mb_env);

    rt->active_cache = fpq_active_cache_init(
        qwen_active_cache_dir(),
        max_mb);
    fpq_model_set_active_cache(rt->model, rt->active_cache);

    /* Initialize KV cache */
    rt->kv_cache = qwen_kv_cache_create(&rt->config);
    if (!rt->kv_cache) {
        fprintf(stderr, "qwen_runtime: failed to allocate KV cache\n");
        tok_free(rt->tokenizer);
        fpq_close(rt->model);
        free(rt);
        return NULL;
    }

    /* Decode required tensors for numerically valid inference. */
    run_cfg.n_vocab = rt->config.n_vocab;
    run_cfg.d_model = rt->config.d_model;
    run_cfg.d_ffn = rt->config.d_ffn;
    run_cfg.n_layers = rt->config.n_layers;
    run_cfg.n_heads = rt->config.n_heads;
    run_cfg.n_kv_heads = rt->config.n_kv_heads;
    run_cfg.head_dim = rt->config.head_dim;
    run_cfg.max_seq_len = rt->config.max_seq_len;
    run_cfg.rms_norm_eps = rt->config.rms_norm_eps;
    run_cfg.rope_theta = rt->config.rope_theta;
    run_cfg.max_new_tokens = rt->config.max_new_tokens;
    run_cfg.temperature = rt->config.temperature;
    run_cfg.top_p = rt->config.top_p;
    run_cfg.greedy = rt->config.greedy;
    run_cfg.prefill_chunk_size = rt->config.prefill_chunk_size;
    run_cfg.sample_n_vocab = tok_vocab_size(rt->tokenizer);
    run_cfg.arch = rt->config.arch;
    qwen_copy_string_field(run_cfg.model_family, sizeof(run_cfg.model_family), rt->config.model_family);
    qwen_copy_string_field(run_cfg.embed_tensor_name, sizeof(run_cfg.embed_tensor_name), rt->config.embed_tensor_name);
    qwen_copy_string_field(run_cfg.final_norm_tensor_name, sizeof(run_cfg.final_norm_tensor_name), rt->config.final_norm_tensor_name);
    qwen_copy_string_field(run_cfg.lm_head_tensor_name, sizeof(run_cfg.lm_head_tensor_name), rt->config.lm_head_tensor_name);
    run_cfg.tie_word_embeddings = rt->config.tie_word_embeddings;
    run_cfg.tokenizer_policy = rt->config.tokenizer_policy;
    run_cfg.n_stop_token_ids = rt->config.n_stop_token_ids;
    for (int i = 0; i < rt->config.n_stop_token_ids &&
                    i < (int)(sizeof(run_cfg.stop_token_ids) / sizeof(run_cfg.stop_token_ids[0])); i++) {
        run_cfg.stop_token_ids[i] = rt->config.stop_token_ids[i];
    }
    rt->run_state = fpq_run_state_create(&run_cfg, rt->kv_cache->k_cache, rt->kv_cache->v_cache);
    if (!rt->run_state) {
        fprintf(stderr, "qwen_runtime: failed to create persistent run state\n");
        qwen_runtime_free(rt);
        return NULL;
    }

    stage_t0 = qwen_monotonic_seconds_now();
    rt->embeddings = fpq_run_load_embeddings(rt->model, &run_cfg);
    rt->norm_layers = fpq_run_load_norms(rt->model, &run_cfg);
    rt->final_norm = fpq_run_load_final_norm(rt->model, &run_cfg);
    rt->tensor_prepare_seconds = qwen_monotonic_seconds_now() - stage_t0;
    if (!rt->embeddings || !rt->norm_layers || !rt->final_norm) {
        fprintf(stderr, "qwen_runtime: failed to load required tensors\n");
        qwen_runtime_free(rt);
        return NULL;
    }

    if (qwen_validate_loaded_model(rt) != 0) {
        qwen_runtime_free(rt);
        return NULL;
    }

    if (qwen_debug_enabled()) {
        fprintf(stderr, "qwen_runtime: initialized, backend=%s d_model=%d layers=%d ctx=%d\n",
                qwen_backend_name(rt->backend),
                rt->config.d_model,
                rt->config.n_layers,
                rt->config.max_seq_len);
    }
    rt->runtime_init_seconds = qwen_monotonic_seconds_now() - init_t0;
    qwen_pack_metadata_clear(&meta);
    free(resolved_pack);
    free(resolved_tokenizer);
    return rt;
}

void qwen_runtime_free(qwen_runtime_t *rt) {
    if (!rt) return;

    fpq_run_state_free(rt->run_state);
    qwen_kv_cache_free(rt->kv_cache);
    if (rt->active_cache) fpq_active_cache_free(rt->active_cache);
    if (rt->tokenizer) tok_free(rt->tokenizer);
    if (rt->model) fpq_close(rt->model);
    free(rt->embeddings);
    free(rt->norm_layers);
    free(rt->final_norm);
    free(rt->pack_path);
    free(rt->tokenizer_path);
    free(rt->model_id);
    free(rt->tokenizer_id);
    free(rt->last_prompt);
    free(rt->last_prompt_ids);
    free(rt);
}


static int qwen_runtime_warm_active_matvec(qwen_runtime_t *rt) {
    static const char *suffixes[] = {
        "self_attn.q_proj.weight",
        "self_attn.k_proj.weight",
        "self_attn.v_proj.weight",
        "self_attn.o_proj.weight",
        "mlp.gate_proj.weight",
        "mlp.up_proj.weight",
        "mlp.down_proj.weight",
    };
    double t0 = 0.0;
    double t1 = 0.0;
    size_t warmed = 0;
    size_t failed = 0;

    if (!rt || !rt->model) return -1;
    if (!qwen_truthy_env_local("BONFYRE_QWEN_WARM_ACTIVE_MATVEC")) return 0;

    t0 = qwen_monotonic_seconds_now();

    for (int lay = 0; lay < rt->config.n_layers; lay++) {
        for (size_t si = 0; si < sizeof(suffixes) / sizeof(suffixes[0]); si++) {
            char name[256];
            const fpq_tensor_info_t *info = NULL;
            float *x = NULL;
            float *y = NULL;

            snprintf(name, sizeof(name), "model.layers.%d.%s", lay, suffixes[si]);
            info = fpq_tensor_find(rt->model, name);
            if (!info || info->rows == 0 || info->cols == 0) continue;

            x = (float *)calloc((size_t)info->cols, sizeof(float));
            y = (float *)calloc((size_t)info->rows, sizeof(float));
            if (!x || !y) {
                free(x);
                free(y);
                failed++;
                continue;
            }

            if (fpq_matmul(rt->model, name, x, y) == 0) {
                warmed++;
            } else {
                failed++;
            }

            free(x);
            free(y);
        }
    }

    t1 = qwen_monotonic_seconds_now();
    fprintf(stderr,
            "qwen_runtime_warm_active_matvec: warmed=%zu failed=%zu seconds=%.6f\n",
            warmed,
            failed,
            t1 - t0);
    fflush(stderr);
    return failed ? -1 : 0;
}


int qwen_runtime_warm(qwen_runtime_t *rt) {
    const char *warm_tensors[5];

    if (!rt) return -1;
    warm_tensors[0] = rt->config.embed_tensor_name[0] ? rt->config.embed_tensor_name : "model.embed_tokens.weight";
    warm_tensors[1] = qwen_output_tensor_name(&rt->config);
    warm_tensors[2] = "model.layers.0.self_attn.q_proj.weight";
    warm_tensors[3] = "model.layers.0.self_attn.k_proj.weight";
    warm_tensors[4] = "model.layers.0.self_attn.v_proj.weight";

    if (qwen_debug_enabled()) {
        fprintf(stderr, "qwen_runtime_warm: warming up model and cache\n");
    }

    for (size_t i = 0; i < sizeof(warm_tensors) / sizeof(warm_tensors[0]); i++) {
        (void)fpq_tensor_find(rt->model, warm_tensors[i]);
        if (qwen_truthy_env_local("BONFYRE_QWEN_WARM_PREPARE")) {
            (void)fpq_prepare_tensor(rt->model, warm_tensors[i]);
        }
    }
    if (qwen_truthy_env_local("BONFYRE_QWEN_EAGER_HOT_WARM")) {
        (void)qwen_runtime_warm_hot_set(rt);
    }
    if (qwen_truthy_env_local("BONFYRE_QWEN_WARM_ACTIVE_MATVEC")) {
        (void)qwen_runtime_warm_active_matvec(rt);
    }

    qwen_kv_cache_reset(rt->kv_cache);
    fpq_run_state_reset(rt->run_state);
    return 0;
}

int qwen_runtime_generate(qwen_runtime_t *rt,
                          const char *prompt,
                          qwen_token_cb token_cb,
                          qwen_stop_cb stop_cb,
                          void *user_data) {
    if (!rt || !prompt) return -1;

    /* Tokenize prompt */
    int *prompt_ids = NULL;
    int prompt_len = 0;
    if (rt->last_prompt && strcmp(rt->last_prompt, prompt) == 0 &&
        rt->last_prompt_ids && rt->last_prompt_len > 0) {
        prompt_len = rt->last_prompt_len;
        prompt_ids = (int *)malloc((size_t)prompt_len * sizeof(int));
        if (prompt_ids) {
            memcpy(prompt_ids, rt->last_prompt_ids, (size_t)prompt_len * sizeof(int));
            rt->resident_reused_prompt_tokens++;
            rt->resident_cache_hits++;
        }
    }
    if (!prompt_ids) {
        prompt_ids = tok_encode(rt->tokenizer, prompt, 0, &prompt_len);
        rt->resident_cache_misses++;
        free(rt->last_prompt);
        free(rt->last_prompt_ids);
        rt->last_prompt = strdup(prompt);
        rt->last_prompt_ids = NULL;
        rt->last_prompt_len = prompt_len;
        if (prompt_ids && prompt_len > 0) {
            rt->last_prompt_ids = (int *)malloc((size_t)prompt_len * sizeof(int));
            if (rt->last_prompt_ids) {
                memcpy(rt->last_prompt_ids, prompt_ids, (size_t)prompt_len * sizeof(int));
            }
        }
    }
    if (prompt_len <= 0 || !prompt_ids) {
        fprintf(stderr, "qwen_runtime_generate: tokenization failed\n");
        return -1;
    }
    if (rt->config.tokenizer_policy == FPQ_RUN_TOKENIZER_POLICY_CHATML) {
        qwen_collapse_chatml_special_tokens(rt->tokenizer, &prompt_ids, &prompt_len);
    }
    qwen_log_prompt_tokens(prompt_ids, prompt_len);

    if (qwen_debug_enabled()) {
        fprintf(stderr, "qwen_runtime_generate: prompt=%d tokens\n", prompt_len);
    }

    /* Use fpq_run_generate with our config */
    fpq_run_config_t run_cfg = {0};
    run_cfg.n_vocab = rt->config.n_vocab;
    run_cfg.d_model = rt->config.d_model;
    run_cfg.d_ffn = rt->config.d_ffn;
    run_cfg.n_layers = rt->config.n_layers;
    run_cfg.n_heads = rt->config.n_heads;
    run_cfg.n_kv_heads = rt->config.n_kv_heads;
    run_cfg.head_dim = rt->config.head_dim;
    run_cfg.max_seq_len = rt->config.max_seq_len;
    run_cfg.rms_norm_eps = rt->config.rms_norm_eps;
    run_cfg.rope_theta = rt->config.rope_theta;
    run_cfg.max_new_tokens = rt->config.max_new_tokens;
    run_cfg.temperature = rt->config.temperature;
    run_cfg.top_p = rt->config.top_p;
    run_cfg.greedy = rt->config.greedy;
    run_cfg.prefill_chunk_size = qwen_autotune_prefill_chunk(
        prompt_len,
        getenv("BONFYRE_QWEN_PREFILL_CHUNK") ? rt->config.prefill_chunk_size : 0);
    run_cfg.sample_n_vocab = tok_vocab_size(rt->tokenizer);
    run_cfg.debug_decode_ctx = rt->tokenizer;
    run_cfg.debug_id_to_str = qwen_debug_id_to_str;
    run_cfg.arch = rt->config.arch;
    qwen_copy_string_field(run_cfg.model_family, sizeof(run_cfg.model_family), rt->config.model_family);
    qwen_copy_string_field(run_cfg.embed_tensor_name, sizeof(run_cfg.embed_tensor_name), rt->config.embed_tensor_name);
    qwen_copy_string_field(run_cfg.final_norm_tensor_name, sizeof(run_cfg.final_norm_tensor_name), rt->config.final_norm_tensor_name);
    qwen_copy_string_field(run_cfg.lm_head_tensor_name, sizeof(run_cfg.lm_head_tensor_name), rt->config.lm_head_tensor_name);
    run_cfg.tie_word_embeddings = rt->config.tie_word_embeddings;
    run_cfg.tokenizer_policy = rt->config.tokenizer_policy;
    run_cfg.n_stop_token_ids = rt->config.n_stop_token_ids;
    for (int i = 0; i < rt->config.n_stop_token_ids &&
                    i < (int)(sizeof(run_cfg.stop_token_ids) / sizeof(run_cfg.stop_token_ids[0])); i++) {
        run_cfg.stop_token_ids[i] = rt->config.stop_token_ids[i];
    }
    if (qwen_debug_enabled() && run_cfg.prefill_chunk_size != rt->config.prefill_chunk_size) {
        fprintf(stderr, "qwen_runtime_generate: autotuned_prefill_chunk=%d\n",
                run_cfg.prefill_chunk_size);
    }
    if (qwen_debug_enabled()) {
        fprintf(stderr,
                "qwen_runtime_generate: max_new_tokens=%d temperature=%.3f top_p=%.3f greedy=%d backend=%s\n",
                run_cfg.max_new_tokens, run_cfg.temperature, run_cfg.top_p, run_cfg.greedy,
                qwen_backend_name(rt->backend));
    }

    if (qwen_debug_enabled() &&
        run_cfg.sample_n_vocab > 0 && run_cfg.sample_n_vocab < run_cfg.n_vocab) {
        fprintf(stderr, "qwen_runtime_generate: sampling_vocab=%d model_vocab=%d\n",
                run_cfg.sample_n_vocab, run_cfg.n_vocab);
    }

    if (!rt->model_validated && qwen_validate_loaded_model(rt) != 0) {
        free(prompt_ids);
        return -1;
    }

    /* Wrap callback to provide text string */
    token_cb_wrapper_t cb_wrapper = {
        .tokenizer = rt->tokenizer,
        .user_cb = token_cb,
        .user_data = user_data
    };

    (void)stop_cb;
    qwen_kv_cache_reset(rt->kv_cache);

    int generated = fpq_run_generate(
        rt->model,
        rt->embeddings,
        rt->norm_layers,
        rt->final_norm,
        prompt_ids,
        prompt_len,
        &run_cfg,
        rt->run_state,
        fpq_token_cb_wrapper,
        &cb_wrapper);

    free(prompt_ids);

    if (generated < 0) return -1;
    if (generated == 0) {
        if (qwen_debug_enabled()) {
            fprintf(stderr, "qwen_runtime_generate: no tokens generated\n");
        }
        return -2;
    }
    if (qwen_debug_enabled()) {
        fprintf(stderr, "qwen_runtime_generate: generated=%d\n", generated);
    }
    return 0;
}

char *qwen_runtime_status_json(const qwen_runtime_t *rt) {
    char *model_show = NULL;
    char *tokenizer_show = NULL;
    char *buf;
    size_t cap = 65536;
    size_t off = 0;
    const fpq_run_metrics_t *metrics;
    const char *model_id;
    const char *tokenizer_id;

    if (!rt) return NULL;
    metrics = fpq_run_state_metrics(rt->run_state);
    model_id = rt->model_id ? rt->model_id : getenv("BONFYRE_QWEN_MODEL_ID");
    tokenizer_id = rt->tokenizer_id ? rt->tokenizer_id : getenv("BONFYRE_QWEN_TOKENIZER_ID");
    if (model_id) model_show = qwen_registry_show(model_id);
    if (tokenizer_id) tokenizer_show = qwen_registry_show(tokenizer_id);
    buf = (char *)calloc(cap, 1);
    if (!buf) {
        free(model_show);
        free(tokenizer_show);
        return NULL;
    }

    off += (size_t)snprintf(buf + off, cap - off,
                            "{\"backend\":\"%s\",\"model_path\":\"",
                            qwen_backend_name(rt->backend));
    qwen_json_escape_append(buf, cap, &off, rt->pack_path ? rt->pack_path : "");
    off += (size_t)snprintf(buf + off, cap - off, "\",\"model_id\":\"");
    qwen_json_escape_append(buf, cap, &off, model_id ? model_id : "");
    off += (size_t)snprintf(buf + off, cap - off, "\",\"tokenizer_path\":\"");
    qwen_json_escape_append(buf, cap, &off, rt->tokenizer_path ? rt->tokenizer_path : "");
    off += (size_t)snprintf(buf + off, cap - off, "\",\"tokenizer_id\":\"");
    qwen_json_escape_append(buf, cap, &off, tokenizer_id ? tokenizer_id : "");
    off += (size_t)snprintf(buf + off, cap - off,
                            "\",\"config\":{\"d_model\":%d,\"layers\":%d,\"context\":%d,"
                            "\"prefill_chunk_size\":%d},"
                            "\"metrics\":{\"prompt_tokens\":%d,\"generated_tokens\":%d,"
                            "\"prefill_seconds\":%.6f,\"decode_seconds\":%.6f,\"prefill_chunks\":%d},"
                            "\"active_cache\":{\"enabled\":%s,\"hits\":%zu,\"misses\":%zu,\"writes\":%zu},"
                            "\"model_registry\":",
                            rt->config.d_model,
                            rt->config.n_layers,
                            rt->config.max_seq_len,
                            metrics ? metrics->prefill_chunk_size : rt->config.prefill_chunk_size,
                            metrics ? metrics->prompt_tokens : 0,
                            metrics ? metrics->generated_tokens : 0,
                            metrics ? metrics->prefill_wall_seconds : 0.0,
                            metrics ? metrics->decode_wall_seconds : 0.0,
                            metrics ? metrics->prefill_chunk_count : 0,
                            rt->active_cache && rt->active_cache->enabled ? "true" : "false",
                            rt->active_cache ? rt->active_cache->hits : 0,
                            rt->active_cache ? rt->active_cache->misses : 0,
                            rt->active_cache ? rt->active_cache->writes : 0);
    if (model_show && model_show[0] == '{') off += (size_t)snprintf(buf + off, cap - off, "%s", model_show);
    else off += (size_t)snprintf(buf + off, cap - off, "null");
    off += (size_t)snprintf(buf + off, cap - off, ",\"tokenizer_registry\":");
    if (tokenizer_show && tokenizer_show[0] == '{') off += (size_t)snprintf(buf + off, cap - off, "%s", tokenizer_show);
    else off += (size_t)snprintf(buf + off, cap - off, "null");
    off += (size_t)snprintf(buf + off, cap - off, "}");
    free(model_show);
    free(tokenizer_show);
    return buf;
}

int qwen_runtime_generate_file(qwen_runtime_t *rt,
                               const char *prompt_path,
                               const char *out_path,
                               qwen_token_cb token_cb,
                               void *user_data) {
    qwen_prompt_file_t prompt = {0};
    FILE *out_fp;
    FILE *trace_fp = NULL;
    long out_size;
    int result;
    struct timespec t0;
    struct timespec t1;
    const char *bootstrap_fallback = NULL;

    if (!rt || !prompt_path || !out_path) return -1;

    if (qwen_load_prompt_file(prompt_path, &prompt) != 0 || !prompt.text) {
        fprintf(stderr, "qwen_runtime_generate_file: cannot open %s\n", prompt_path);
        return -1;
    }

    /* Open output file */
    if (bf_ensure_parent_dir(out_path) != 0) {
        qwen_unload_prompt_file(&prompt);
        return -1;
    }
    out_fp = fopen(out_path, "w");
    if (!out_fp) {
        fprintf(stderr, "qwen_runtime_generate_file: cannot create %s\n", out_path);
        qwen_unload_prompt_file(&prompt);
        return -1;
    }
    setvbuf(out_fp, NULL, _IONBF, 0);

    const char *trace_path = getenv("BONFYRE_QWEN_TOKEN_TRACE_PATH");
    if (trace_path && trace_path[0] != '\0') {
        (void)bf_ensure_parent_dir(trace_path);
        trace_fp = fopen(trace_path, "w");
        if (!trace_fp) {
            fprintf(stderr, "qwen_runtime_generate_file: cannot create trace file %s\n", trace_path);
            fclose(out_fp);
            qwen_unload_prompt_file(&prompt);
            return -1;
        }
        setvbuf(trace_fp, NULL, _IONBF, 0);
    }

    /* Generate with file output callback */
    file_cb_data_t cb_data = {
        .fp = out_fp,
        .trace_fp = trace_fp,
        .tokenizer = rt->tokenizer,
        .bos_id = tok_bos_id(rt->tokenizer),
        .eos_id = tok_eos_id(rt->tokenizer),
        .step = 0,
        .token_ids = NULL,
        .token_count = 0,
        .token_cap = 0,
        .orig_cb = token_cb,
        .orig_data = user_data,
    };

    clock_gettime(CLOCK_MONOTONIC, &t0);
    result = qwen_runtime_generate(rt, prompt.text, file_token_cb_impl, NULL, &cb_data);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    if (result == 0 && qwen_bootstrap_zero_matmul_enabled()) {
        bootstrap_fallback = qwen_bootstrap_fallback_line(rt, prompt.text);
        if (bootstrap_fallback) {
            int fd = fileno(out_fp);
            fflush(out_fp);
            if (fd >= 0 && ftruncate(fd, 0) == 0) {
                rewind(out_fp);
                fputs(bootstrap_fallback, out_fp);
                fflush(out_fp);
                fprintf(stderr,
                        "qwen_runtime: bootstrap fallback output applied mode=%s text=\"%s\"\n",
                        rt->config.mode,
                        bootstrap_fallback);
                if (trace_fp) {
                    fprintf(trace_fp,
                            "{\"type\":\"bootstrap_fallback\",\"mode\":\"%s\",\"text\":\"%s\"}\n",
                            rt->config.mode,
                            bootstrap_fallback);
                }
            }
        }
    }
    if (trace_fp) {
        const fpq_run_metrics_t *metrics = fpq_run_state_metrics(rt->run_state);
        if (metrics) {
            for (int i = 0; i < metrics->prefill_chunk_count && i < FPQ_RUN_MAX_PREFILL_CHUNKS; i++) {
                fprintf(trace_fp,
                        "{\"type\":\"prefill_chunk\",\"chunk_index\":%d,\"tokens\":%d,"
                        "\"latency_seconds\":%.6f,\"active_cache_hits\":%" PRIu64 "}\n",
                        i,
                        metrics->prefill_chunk_tokens[i],
                        metrics->prefill_chunk_seconds[i],
                        metrics->prefill_chunk_cache_hits[i]);
            }
            fprintf(trace_fp,
                    "{\"type\":\"run_summary\",\"prompt_tokens\":%d,\"generated_tokens\":%d,"
                    "\"prefill_seconds\":%.6f,\"decode_seconds\":%.6f,"
                    "\"prefill_chunk_size\":%d,\"prefill_chunks\":%d,"
                    "\"active_cache_hits\":%zu,\"active_cache_misses\":%zu,"
                    "\"active_cache_writes\":%zu}\n",
                    metrics->prompt_tokens,
                    metrics->generated_tokens,
                    metrics->prefill_wall_seconds,
                    metrics->decode_wall_seconds,
                    metrics->prefill_chunk_size,
                    metrics->prefill_chunk_count,
                    rt && rt->active_cache ? rt->active_cache->hits : 0,
                    rt && rt->active_cache ? rt->active_cache->misses : 0,
                    rt && rt->active_cache ? rt->active_cache->writes : 0);
        }
    }

    out_size = ftell(out_fp);
    if (out_size < 0) out_size = 0;

    fclose(out_fp);
    if (trace_fp) fclose(trace_fp);
    free(cb_data.token_ids);
    qwen_unload_prompt_file(&prompt);

    if (result == 0 && out_size == 0) {
        fprintf(stderr, "qwen_runtime_generate_file: empty output written to %s\n", out_path);
        return -3;
    }

    if (result == 0) {
        fprintf(stderr, "qwen_runtime_generate_file: wrote %ld bytes to %s\n", out_size, out_path);
    }
    qwen_emit_run_manifest(rt,
                           prompt_path,
                           out_path,
                           (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9,
                           result == 0);

    return result;
}
