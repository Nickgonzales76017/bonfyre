#define _POSIX_C_SOURCE 200809L
#include <bonfyre.h>
#include "fpq_run.h"
#include "libfpq.h"
#include "qwen_runtime.h"
#include <math.h>
#include "qwen_serve.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <inttypes.h>
#include <limits.h>
#include <sys/stat.h>
#include <time.h>
#include <ctype.h>
#include <stdint.h>

#ifdef BF_HAS_QUIC
#include <bf_quic.h>
#endif

static char *read_text_file(const char *path, size_t *len_out);
static int recommend_chunk_size(int prompt_tokens);
static int whitespace_normalized_equal(const char *a, const char *b);
static void print_escaped_text(FILE *fp, const char *s);
static double monotonic_seconds_now(void);
static void emit_native_block_score_jsonl(const char *path, const char *tensor_name,
                                          int layer, const char *projection,
                                          size_t rows, size_t cols,
                                          size_t blocks_per_row,
                                          size_t row, size_t block,
                                          size_t col_offset, size_t this_dim,
                                          float native_block_score,
                                          float row_native_total,
                                          float row_runtime_total);
static int cmd_score_probe(const char *pack, const char *tensor_name,
                           const char *rows_csv, const char *probe_jsonl_path,
                           const char *summary_path, int shared_qkv,
                           int random_rows, uint32_t seed);

typedef struct {
    size_t *rows;
    size_t n_rows;
} qwen_row_list_t;

typedef struct {
    double start_seconds;
    double first_token_seconds;
    double last_token_seconds;
    int token_count;
    FILE *trace_fp;
} qwen_bench_cb_t;

typedef struct {
    const char *pack;
    const char *tokenizer_path;
    const char *prompt_path;
    const char *json_path;
    const char *trace_path;
    int max_new_tokens;
    float temperature;
    int greedy;
    int load_only;
    int warm_cache;
    int resident;
    int speed_mode;
    int reuse_prefix;
} qwen_bench_options_t;

static void qwen_row_list_free(qwen_row_list_t *list) {
    if (!list) return;
    free(list->rows);
    list->rows = NULL;
    list->n_rows = 0;
}

static int qwen_row_list_push(qwen_row_list_t *list, size_t row) {
    if (!list) return -1;
    for (size_t i = 0; i < list->n_rows; i++) {
        if (list->rows[i] == row) return 0;
    }
    size_t *next = (size_t *)realloc(list->rows, (list->n_rows + 1) * sizeof(*next));
    if (!next) return -1;
    list->rows = next;
    list->rows[list->n_rows++] = row;
    return 0;
}

static int qwen_parse_rows_csv(const char *csv, qwen_row_list_t *out) {
    char *copy = NULL;
    char *save = NULL;
    char *tok;
    if (!out) return -1;
    if (!csv || !csv[0]) return 0;
    copy = strdup(csv);
    if (!copy) return -1;
    for (tok = strtok_r(copy, ",", &save); tok; tok = strtok_r(NULL, ",", &save)) {
        char *end = NULL;
        unsigned long v = strtoul(tok, &end, 10);
        if (end == tok) continue;
        if (qwen_row_list_push(out, (size_t)v) != 0) {
            free(copy);
            return -1;
        }
    }
    free(copy);
    return 0;
}

static void qwen_add_default_probe_rows(qwen_row_list_t *rows, size_t row_count) {
    static const size_t defaults[] = {0, 1, 2, 3, 64, 127, 128, 255, 511, 1023};
    for (size_t i = 0; i < sizeof(defaults) / sizeof(defaults[0]); i++) {
        if (defaults[i] < row_count) (void)qwen_row_list_push(rows, defaults[i]);
    }
}

static void qwen_add_deterministic_random_rows(qwen_row_list_t *rows,
                                               size_t row_count,
                                               int random_rows,
                                               uint32_t seed) {
    uint32_t state = seed ? seed : 0xC0FFEEu;
    if (!rows || row_count == 0 || random_rows <= 0) return;
    for (int i = 0; i < random_rows; i++) {
        state = state * 1664525u + 1013904223u;
        (void)qwen_row_list_push(rows, (size_t)(state % (uint32_t)row_count));
    }
}

static int qwen_rows_to_csv(const qwen_row_list_t *rows, char *buf, size_t buf_sz) {
    size_t used = 0;
    if (!rows || !buf || buf_sz == 0) return -1;
    buf[0] = '\0';
    for (size_t i = 0; i < rows->n_rows; i++) {
        int wrote = snprintf(buf + used, buf_sz - used, "%s%zu", i ? "," : "", rows->rows[i]);
        if (wrote < 0 || (size_t)wrote >= buf_sz - used) return -1;
        used += (size_t)wrote;
    }
    return 0;
}

static int qwen_tensor_layer_from_name(const char *name) {
    const char *needle = ".layers.";
    const char *p;
    if (!name) return -1;
    p = strstr(name, needle);
    if (!p) return -1;
    return atoi(p + strlen(needle));
}

static const char *qwen_projection_from_name(const char *name) {
    if (!name) return "unknown";
    if (strstr(name, ".self_attn.q_proj.")) return "q_proj";
    if (strstr(name, ".self_attn.k_proj.")) return "k_proj";
    if (strstr(name, ".self_attn.v_proj.")) return "v_proj";
    if (strstr(name, ".self_attn.o_proj.")) return "o_proj";
    if (strstr(name, ".mlp.gate_proj.")) return "gate_proj";
    if (strstr(name, ".mlp.up_proj.")) return "up_proj";
    if (strstr(name, ".mlp.down_proj.")) return "down_proj";
    return "unknown";
}

static int qwen_derive_qkv_group(const char *tensor_name,
                                 char names[3][256],
                                 int *primary_index) {
    const char *suffixes[3] = {
        ".self_attn.q_proj.weight",
        ".self_attn.k_proj.weight",
        ".self_attn.v_proj.weight",
    };
    const char *matched = NULL;
    size_t prefix_len = 0;
    if (!tensor_name || !names || !primary_index) return -1;
    for (int i = 0; i < 3; i++) {
        const char *pos = strstr(tensor_name, suffixes[i]);
        if (pos && strcmp(pos, suffixes[i]) == 0) {
            matched = suffixes[i];
            prefix_len = (size_t)(pos - tensor_name);
            *primary_index = i;
            break;
        }
    }
    if (!matched) return -1;
    for (int i = 0; i < 3; i++) {
        if (prefix_len + strlen(suffixes[i]) + 1 > sizeof(names[i])) return -1;
        memcpy(names[i], tensor_name, prefix_len);
        names[i][prefix_len] = '\0';
        strncat(names[i], suffixes[i], sizeof(names[i]) - strlen(names[i]) - 1);
    }
    return 0;
}

static const char *model_bin_path(void) {
    const char *env = getenv("BONFYRE_MODEL_BIN");
    if (env && env[0]) return env;
    if (access("./cmd/BonfyreModel/bonfyre-model", X_OK) == 0) return "./cmd/BonfyreModel/bonfyre-model";
    if (access("./bin/bonfyre-model", X_OK) == 0) return "./bin/bonfyre-model";
    return "bonfyre-model";
}

static char *capture_one_line(const char *cmd) {
    FILE *pipe = popen(cmd, "r");
    char buf[4096];
    size_t len;
    char *out;
    if (!pipe) return NULL;
    if (!fgets(buf, sizeof(buf), pipe)) {
        pclose(pipe);
        return NULL;
    }
    pclose(pipe);
    len = strlen(buf);
    while (len > 0 && (buf[len - 1] == '\n' || buf[len - 1] == '\r')) buf[--len] = '\0';
    out = (char *)malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, buf, len + 1);
    return out;
}

static char *resolve_registry_ref(const char *ref) {
    char cmd[4096];
    char *path = NULL;
    if (!ref || !ref[0] || access(ref, F_OK) == 0) return ref ? strdup(ref) : NULL;
    snprintf(cmd, sizeof(cmd), "%s path %s 2>/dev/null", model_bin_path(), ref);
    path = capture_one_line(cmd);
    if (path && access(path, F_OK) == 0) return path;
    free(path);
    snprintf(cmd, sizeof(cmd), "%s pull %s >/dev/null 2>&1", model_bin_path(), ref);
    (void)system(cmd);
    snprintf(cmd, sizeof(cmd), "%s path %s 2>/dev/null", model_bin_path(), ref);
    path = capture_one_line(cmd);
    if (path && access(path, F_OK) == 0) return path;
    free(path);
    return ref ? strdup(ref) : NULL;
}

static const char *guess_tokenizer_id(const char *model_id) {
    static char guess[512];
    size_t len;
    if (!model_id || !model_id[0]) return NULL;
    len = strlen(model_id);
    if (len > 4 && strcmp(model_id + len - 4, "-fpq") == 0) {
        snprintf(guess, sizeof(guess), "%.*s-tokenizer", (int)(len - 4), model_id);
        return guess;
    }
    snprintf(guess, sizeof(guess), "%s-tokenizer", model_id);
    return guess;
}

static double monotonic_seconds_now(void) {
    struct timespec ts = {0};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static int qwen_truthy_env(const char *name) {
    const char *v = getenv(name);
    return v && v[0] && strcmp(v, "0") != 0 &&
           strcasecmp(v, "false") != 0 &&
           strcasecmp(v, "off") != 0;
}

static void qwen_apply_speed_mode_env(void) {
    (void)setenv("BONFYRE_QWEN_DEBUG", "0", 1);
    (void)setenv("BONFYRE_QWEN_LOG_PREFILL_TIMING", "0", 1);
    (void)setenv("BONFYRE_QWEN_LOG_MEM", "0", 1);
    (void)setenv("BONFYRE_QWEN_LOG_LAYER_PROGRESS", "0", 1);
    (void)unsetenv("BONFYRE_QWEN_GOLDEN_JSONL");
    (void)unsetenv("BONFYRE_QWEN_SLI_SCORE_PROBE");
    (void)unsetenv("BONFYRE_QWEN_SLI_SCORE_PROBE_ONLY_ROWS");
    (void)unsetenv("BONFYRE_QWEN_SLI_SCORE_PROBE_JSONL");
    (void)unsetenv("BONFYRE_QWEN_SLI_SCORE_PROBE_TENSOR");
    (void)unsetenv("BONFYRE_QWEN_SLI_SCORE_PROBE_ROWS");
    if (!qwen_truthy_env("BONFYRE_QWEN_SPEED_MODE")) {
        (void)setenv("BONFYRE_QWEN_SPEED_MODE", "1", 1);
    }
    if (!qwen_truthy_env("BONFYRE_QWEN_TRACE_TOKENS")) {
        (void)setenv("BONFYRE_QWEN_TRACE_TOKENS", "0", 1);
    }
    if (qwen_truthy_env("BONFYRE_QWEN_KEEP_PREFILL_PREPARED")) {
        (void)setenv("BONFYRE_QWEN_RELEASE_LAYER_AFTER_PREFILL", "0", 1);
        (void)setenv("BONFYRE_QWEN_RELEASE_RUNTIME_AFTER_PREFILL", "0", 1);
        (void)setenv("BONFYRE_QWEN_RELEASE_LAYER_AFTER_DECODE", "0", 1);
    } else {
        if (!qwen_truthy_env("BONFYRE_QWEN_RELEASE_LAYER_AFTER_PREFILL")) {
            (void)setenv("BONFYRE_QWEN_RELEASE_LAYER_AFTER_PREFILL", "1", 1);
        }
        if (!qwen_truthy_env("BONFYRE_QWEN_RELEASE_RUNTIME_AFTER_PREFILL")) {
            (void)setenv("BONFYRE_QWEN_RELEASE_RUNTIME_AFTER_PREFILL", "1", 1);
        }
        if (!getenv("BONFYRE_QWEN_RELEASE_LAYER_AFTER_DECODE")) {
            (void)setenv("BONFYRE_QWEN_RELEASE_LAYER_AFTER_DECODE", "0", 1);
        }
    }
    if (qwen_truthy_env("BONFYRE_QWEN_PREPARE_CACHE")) {
        (void)setenv("BONFYRE_ACTIVE_WEIGHT_CACHE", "1", 1);
    }
}

static const char *qwen_backend_name_local(qwen_backend_t backend) {
    switch (backend) {
        case QWEN_BACKEND_CPU_SCALAR: return "scalar";
        case QWEN_BACKEND_CPU_NEON: return "neon";
        case QWEN_BACKEND_CPU_NEON_FUSED: return "neon_fused";
        case QWEN_BACKEND_FLASHQLA_PREFILL: return "flashqla_prefill";
        default: return "unknown";
    }
}

static void qwen_bench_token_cb(int token_id, const char *text, void *user_data) {
    qwen_bench_cb_t *cb = (qwen_bench_cb_t *)user_data;
    double now = monotonic_seconds_now();
    (void)text;
    if (!cb) return;
    if (cb->token_count == 0) cb->first_token_seconds = now - cb->start_seconds;
    cb->last_token_seconds = now - cb->start_seconds;
    cb->token_count++;
    if (cb->trace_fp) {
        fprintf(cb->trace_fp,
                "{\"type\":\"token\",\"step\":%d,\"token_id\":%d,\"elapsed_seconds\":%.6f}\n",
                cb->token_count - 1,
                token_id,
                now - cb->start_seconds);
        fflush(cb->trace_fp);
    }
}

static void qwen_sibling_path(const char *base_path, const char *leaf,
                              char *out, size_t out_cap) {
    const char *slash;
    if (!out || out_cap == 0) return;
    out[0] = '\0';
    if (!base_path || !leaf) return;
    slash = strrchr(base_path, '/');
    if (!slash) {
        snprintf(out, out_cap, "%s", leaf);
        return;
    }
    snprintf(out, out_cap, "%.*s/%s", (int)(slash - base_path), base_path, leaf);
}

static void qwen_write_layer_timing_jsonl(const char *path,
                                          const fpq_run_metrics_t *metrics,
                                          int n_layers) {
    FILE *fp;
    if (!path || !metrics || n_layers <= 0) return;
    if (bf_ensure_parent_dir(path) != 0) return;
    fp = fopen(path, "w");
    if (!fp) return;
    for (int lay = 0; lay < n_layers && lay < FPQ_RUN_MAX_LAYERS; lay++) {
        fprintf(fp,
                "{\"layer\":%d,\"prefill_total_seconds\":%.6f,"
                "\"prefill_prepare_seconds\":%.6f,"
                "\"decode_total_seconds\":%.6f,"
                "\"decode_prepare_seconds\":%.6f,"
                "\"decode_qkv_seconds\":%.6f,"
                "\"decode_attention_seconds\":%.6f,"
                "\"decode_o_proj_seconds\":%.6f,"
                "\"decode_norm_seconds\":%.6f,"
                "\"decode_mlp_seconds\":%.6f,"
                "\"decode_kv_write_seconds\":%.6f,"
                "\"decode_release_seconds\":%.6f}\n",
                lay,
                metrics->prefill_layer_total_seconds[lay],
                metrics->prefill_layer_prepare_seconds[lay],
                metrics->decode_layer_total_seconds[lay],
                metrics->decode_layer_prepare_seconds[lay],
                metrics->decode_layer_qkv_seconds[lay],
                metrics->decode_layer_attention_seconds[lay],
                metrics->decode_layer_o_proj_seconds[lay],
                metrics->decode_layer_norm_seconds[lay],
                metrics->decode_layer_mlp_seconds[lay],
                metrics->decode_layer_kv_write_seconds[lay],
                metrics->decode_layer_release_seconds[lay]);
    }
    fclose(fp);
}

static void qwen_write_family_timing_json(const char *path,
                                          const fpq_run_metrics_t *metrics) {
    FILE *fp;
    int first = 1;
    if (!path || !metrics) return;
    if (bf_ensure_parent_dir(path) != 0) return;
    fp = fopen(path, "w");
    if (!fp) return;
    fprintf(fp, "{\n");
    fprintf(fp, "  \"schema_version\": \"bonfyre.qwen_family_timing.v1\",\n");
    fprintf(fp, "  \"families\": [\n");
    for (int i = 0; i < FPQ_RUN_MAX_FAMILIES; i++) {
        if (metrics->family_calls[i] == 0 && metrics->family_seconds[i] == 0.0) continue;
        fprintf(fp, "%s    {\"family\": ", first ? "" : ",\n");
        print_escaped_text(fp, fpq_run_metrics_family_name(i));
        fprintf(fp,
                ", \"seconds\": %.6f, \"calls\": %" PRIu64 "}",
                metrics->family_seconds[i],
                metrics->family_calls[i]);
        first = 0;
    }
    fprintf(fp, "\n  ]\n}\n");
    fclose(fp);
}

static int cmd_bench_generate(const qwen_bench_options_t *opts) {
    qwen_config_t config = qwen_default_config();
    qwen_runtime_t *rt = NULL;
    char *prompt = NULL;
    size_t prompt_bytes = 0;
    int *prompt_ids = NULL;
    int prompt_tokens = 0;
    int rc = 1;
    int gen_rc = -1;
    FILE *trace_fp = NULL;
    double init_t0;
    double init_t1;
    double warm_t0 = 0.0;
    double warm_t1 = 0.0;
    double gen_t0 = 0.0;
    double gen_t1 = 0.0;
    double repeat_total_seconds = 0.0;
    double repeat_min_first_token_seconds = 0.0;
    double repeat_last_first_token_seconds = 0.0;
    int repeat_count = 1;
    int repeat_success = 0;
    int repeat_total_tokens = 0;
    int effective_token_count = 0;
    qwen_bench_cb_t cb = {0};
    const fpq_run_metrics_t *metrics = NULL;
    double include_load_tps = 0.0;
    double exclude_load_tps = 0.0;
    double decode_only_tps = 0.0;
    long trace_bytes = 0;
    fpq_runtime_stats_t runtime_stats = {0};
    char layer_timing_path[PATH_MAX];
    char family_timing_path[PATH_MAX];

    if (!opts || !opts->pack || !opts->tokenizer_path || !opts->prompt_path || !opts->json_path) {
        fprintf(stderr, "bench-generate requires --pack, --tokenizer, --prompt, and --json\n");
        return 1;
    }
    if (opts->speed_mode) qwen_apply_speed_mode_env();
    {
        const char *repeat_env = getenv("BONFYRE_QWEN_BENCH_REPEAT");
        if (repeat_env && repeat_env[0]) {
            int parsed_repeat = atoi(repeat_env);
            if (parsed_repeat > 1 && parsed_repeat <= 64) {
                repeat_count = parsed_repeat;
            }
        }
    }
    fpq_runtime_stats_reset();
    if (opts->trace_path && opts->trace_path[0]) {
        if (bf_ensure_parent_dir(opts->trace_path) != 0) return 1;
        trace_fp = fopen(opts->trace_path, "w");
        if (!trace_fp) {
            fprintf(stderr, "bench-generate: cannot create trace %s\n", opts->trace_path);
            return 1;
        }
        setvbuf(trace_fp, NULL, _IONBF, 0);
    }

    config.max_new_tokens = opts->max_new_tokens;
    config.temperature = opts->temperature;
    config.greedy = opts->greedy;

    init_t0 = monotonic_seconds_now();
    rt = qwen_runtime_init(opts->pack, opts->tokenizer_path, &config);
    init_t1 = monotonic_seconds_now();
    if (!rt) goto cleanup;

    prompt = read_text_file(opts->prompt_path, &prompt_bytes);
    if (!prompt) {
        fprintf(stderr, "bench-generate: cannot read prompt %s\n", opts->prompt_path);
        goto cleanup;
    }
    prompt_ids = tok_encode(rt->tokenizer, prompt, 0, &prompt_tokens);
    free(prompt_ids);
    prompt_ids = NULL;

    if (opts->warm_cache || opts->resident || opts->reuse_prefix) {
        warm_t0 = monotonic_seconds_now();
        if (qwen_runtime_warm(rt) != 0) {
            fprintf(stderr, "bench-generate: warm failed\n");
            goto cleanup;
        }
        warm_t1 = monotonic_seconds_now();
    }

    if (!opts->load_only) {
        gen_t0 = monotonic_seconds_now();
        for (int repeat_i = 0; repeat_i < repeat_count; repeat_i++) {
            qwen_bench_cb_t iter_cb = {0};
            double iter_t0;
            double iter_t1;

            iter_cb.start_seconds = monotonic_seconds_now();
            iter_cb.trace_fp = trace_fp;
            iter_t0 = iter_cb.start_seconds;

            gen_rc = qwen_runtime_generate(rt, prompt, qwen_bench_token_cb, NULL, &iter_cb);
            iter_t1 = monotonic_seconds_now();

            if (gen_rc != 0) {
                cb = iter_cb;
                gen_t1 = iter_t1;
                goto cleanup;
            }

            repeat_success++;
            repeat_total_seconds += (iter_t1 > iter_t0) ? (iter_t1 - iter_t0) : 0.0;
            repeat_total_tokens += iter_cb.token_count;

            if (iter_cb.token_count > 0) {
                repeat_last_first_token_seconds = iter_cb.first_token_seconds;
                if (repeat_min_first_token_seconds <= 0.0 ||
                    iter_cb.first_token_seconds < repeat_min_first_token_seconds) {
                    repeat_min_first_token_seconds = iter_cb.first_token_seconds;
                }
            }

            cb = iter_cb;
        }
        gen_t1 = monotonic_seconds_now();
    } else {
        gen_rc = 0;
    }

    metrics = fpq_run_state_metrics(rt->run_state);
    effective_token_count = (repeat_total_tokens > 0) ? repeat_total_tokens : cb.token_count;
    if (effective_token_count > 0) {
        include_load_tps = (double)effective_token_count /
            ((init_t1 - init_t0) + (warm_t1 > warm_t0 ? warm_t1 - warm_t0 : 0.0) + (gen_t1 - gen_t0));
        exclude_load_tps = (double)effective_token_count / ((gen_t1 - gen_t0) > 1e-9 ? (gen_t1 - gen_t0) : 1e-9);
        decode_only_tps = (double)effective_token_count /
            ((gen_t1 - gen_t0) > 1e-9 ? (gen_t1 - gen_t0) : 1e-9);
    }
    if (trace_fp) {
        fflush(trace_fp);
        trace_bytes = ftell(trace_fp);
        if (trace_bytes < 0) trace_bytes = 0;
    }
    runtime_stats = fpq_runtime_stats_get();
    qwen_sibling_path(opts->json_path, "layer-timing.jsonl", layer_timing_path, sizeof(layer_timing_path));
    qwen_sibling_path(opts->json_path, "family-timing.json", family_timing_path, sizeof(family_timing_path));
    if (metrics) {
        qwen_write_layer_timing_jsonl(layer_timing_path, metrics, rt ? rt->config.n_layers : 0);
        qwen_write_family_timing_json(family_timing_path, metrics);
    }
    if (bf_ensure_parent_dir(opts->json_path) != 0) goto cleanup;
    {
        FILE *fp = fopen(opts->json_path, "w");
        if (!fp) goto cleanup;
        fprintf(fp,
                "{\n"
                "  \"schema_version\": \"bonfyre.qwen_fpq_bench_generate.v1\",\n"
                "  \"mode\": %s,\n"
                "  \"speed_mode\": %s,\n"
                "  \"resident\": %s,\n"
                "  \"warm_cache\": %s,\n"
                "  \"reuse_prefix\": %s,\n"
                "  \"backend\": ",
                opts->load_only ? "\"load-only\"" : "\"generate\"",
                opts->speed_mode ? "true" : "false",
                opts->resident ? "true" : "false",
                opts->warm_cache ? "true" : "false",
                opts->reuse_prefix ? "true" : "false");
        print_escaped_text(fp, qwen_backend_name_local(rt->backend));
        fprintf(fp, ",\n");
        fprintf(fp,
                "  \"pack\": ");
        print_escaped_text(fp, opts->pack);
        fprintf(fp, ",\n  \"tokenizer\": ");
        print_escaped_text(fp, opts->tokenizer_path);
        fprintf(fp,
                ",\n  \"prompt_path\": ");
        print_escaped_text(fp, opts->prompt_path);
        fprintf(fp,
                ",\n  \"prompt_bytes\": %zu,\n"
                "  \"prompt_tokens\": %d,\n"
                "  \"generated_tokens\": %d,\n"
                "  \"model_load_seconds\": %.6f,\n"
                "  \"tokenizer_load_seconds\": %.6f,\n"
                "  \"tensor_prepare_seconds\": %.6f,\n"
                "  \"runtime_init_seconds\": %.6f,\n"
                "  \"warm_seconds\": %.6f,\n"
                "  \"prefill_seconds\": %.6f,\n"
                "  \"first_token_seconds\": %.6f,\n"
                "  \"total_decode_seconds\": %.6f,\n"
                "  \"wall_generate_seconds\": %.6f,\n"
                "  \"tok_per_sec_including_load\": %.6f,\n"
                "  \"tok_per_sec_excluding_load\": %.6f,\n"
                "  \"tok_per_sec_decode_only\": %.6f,\n"
                "  \"active_cache_hits\": %zu,\n"
                "  \"active_cache_misses\": %zu,\n"
                "  \"active_cache_writes\": %zu,\n"
                "  \"mmap_count\": %" PRIu64 ",\n"
                "  \"munmap_count\": %" PRIu64 ",\n"
                "  \"mmap_bytes\": %" PRIu64 ",\n"
                "  \"lm_head_seconds\": %.6f,\n"
                "  \"kv_write_seconds\": %.6f,\n"
                "  \"release_seconds\": %.6f,\n"
                "  \"layer_timing_path\": ",
                prompt_bytes,
                prompt_tokens,
                effective_token_count,
                rt->model_open_seconds,
                rt->tokenizer_load_seconds,
                rt->tensor_prepare_seconds,
                rt->runtime_init_seconds,
                (warm_t1 > warm_t0) ? (warm_t1 - warm_t0) : 0.0,
                metrics ? metrics->prefill_wall_seconds : 0.0,
                cb.token_count > 0 ? cb.first_token_seconds : 0.0,
                metrics ? metrics->decode_wall_seconds : 0.0,
                gen_t1 > gen_t0 ? (gen_t1 - gen_t0) : 0.0,
                include_load_tps,
                exclude_load_tps,
                decode_only_tps,
                rt->active_cache ? rt->active_cache->hits : 0,
                rt->active_cache ? rt->active_cache->misses : 0,
                rt->active_cache ? rt->active_cache->writes : 0,
                runtime_stats.mmap_count,
                runtime_stats.munmap_count,
                runtime_stats.mmap_bytes,
                metrics ? metrics->lm_head_seconds : 0.0,
                metrics ? metrics->kv_write_seconds : 0.0,
                metrics ? metrics->release_seconds : 0.0);
        print_escaped_text(fp, layer_timing_path);
        fprintf(fp, ",\n  \"family_timing_path\": ");
        print_escaped_text(fp, family_timing_path);
        fprintf(fp,
                ",\n  \"bench_repeat_count\": %d,\n"
                "  \"bench_repeat_success\": %d,\n"
                "  \"bench_repeat_total_generated_tokens\": %d,\n"
                "  \"bench_repeat_total_seconds\": %.6f,\n"
                "  \"bench_repeat_min_first_token_seconds\": %.6f,\n"
                "  \"bench_repeat_last_first_token_seconds\": %.6f,\n"
                "  \"trace_bytes\": %ld,\n"
                "  \"rc\": %d\n"
                "}\n",
                repeat_count,
                repeat_success,
                repeat_total_tokens,
                repeat_total_seconds,
                repeat_min_first_token_seconds,
                repeat_last_first_token_seconds,
                trace_bytes,
                gen_rc);
        fclose(fp);
    }
    rc = 0;

cleanup:
    if (trace_fp) fclose(trace_fp);
    free(prompt);
    free(prompt_ids);
    qwen_runtime_free(rt);
    return rc;
}

static void emit_native_block_score_jsonl(const char *path, const char *tensor_name,
                                          int layer, const char *projection,
                                          size_t rows, size_t cols,
                                          size_t blocks_per_row,
                                          size_t row, size_t block,
                                          size_t col_offset, size_t this_dim,
                                          float native_block_score,
                                          float row_native_total,
                                          float row_runtime_total) {
    FILE *fp;
    if (!path || !tensor_name || !projection) return;
    fp = fopen(path, "a");
    if (!fp) return;
    fprintf(fp,
            "{\"kind\":\"native_block_score\",\"tensor\":");
    print_escaped_text(fp, tensor_name);
    fprintf(fp,
            ",\"row\":%zu,\"block\":%zu,\"layer\":%d,\"projection\":",
            row, block, layer);
    print_escaped_text(fp, projection);
    fprintf(fp,
            ",\"rows\":%zu,\"cols\":%zu,\"blocks_per_row\":%zu,"
            "\"block_range_start\":%zu,\"block_range_end\":%zu,"
            "\"native_block_score\":%.9g,\"row_native_total\":%.9g,"
            "\"row_runtime_total\":%.9g,\"block_fraction_of_row_runtime\":%.9g,"
            "\"runtime_path_name\":\"native-block-reference\"}\n",
            rows, cols, blocks_per_row,
            col_offset, col_offset + this_dim,
            native_block_score, row_native_total, row_runtime_total,
            fabsf(row_runtime_total) > 1e-20f ? native_block_score / row_runtime_total : 0.0f);
    fclose(fp);
}

static void emit_native_residual_block_score_jsonl(const char *path, const char *tensor_name,
                                                   int layer, const char *projection,
                                                   size_t rows, size_t cols,
                                                   size_t blocks_per_row,
                                                   size_t row, size_t block,
                                                   size_t col_offset, size_t this_dim,
                                                   float native_block_score) {
    FILE *fp;
    if (!path || !tensor_name || !projection) return;
    fp = fopen(path, "a");
    if (!fp) return;
    fprintf(fp,
            "{\"kind\":\"native_residual_block_score\",\"tensor\":");
    print_escaped_text(fp, tensor_name);
    fprintf(fp,
            ",\"row\":%zu,\"block\":%zu,\"layer\":%d,\"projection\":",
            row, block, layer);
    print_escaped_text(fp, projection);
    fprintf(fp,
            ",\"rows\":%zu,\"cols\":%zu,\"blocks_per_row\":%zu,"
            "\"block_range_start\":%zu,\"block_range_end\":%zu,"
            "\"native_block_score\":%.9g,"
            "\"runtime_path_name\":\"native-residual-block-reference\"}\n",
            rows, cols, blocks_per_row,
            col_offset, col_offset + this_dim,
            native_block_score);
    fclose(fp);
}

static void usage(void) {
    fprintf(stderr,
        "bonfyre-qwen-fpq — High-quality local generation for Blender, video, and code\n\n"
        "Commands:\n"
        "  serve                      Resident service (JSONL over stdin/stdout)\n"
        "  serve-local                Resident local service (alias of serve)\n"
        "  serve-quic                 Resident QUIC service (JSON over QUIC streams)\n"
        "  quic-client                Local QUIC request client for status/generate testing\n"
        "  generate                   Generate from prompt file\n"
        "  generate-blender-template  Generate Blender scene template\n"
        "  generate-video-script      Generate procedural video editing script\n"
        "  generate-hyperframes       Generate hyperframe metadata\n"
        "  generate-json              Generate structured JSON output\n\n"
        "  doctor                     Validate model, tokenizer, and runtime wiring\n"
        "  tokenizer-test             Validate tokenizer encode/decode roundtrip\n"
        "  prefill-bench              Benchmark prompt prefill only and emit chunk guidance\n"
        "  bench-generate             Benchmark real generation speed and latency\n"
        "  score-probe                Bounded FPQ-X / SLI row probe for one tensor\n"
        "  test-tensor                Run isolated bounded matmul test on one tensor\n\n"
        "Generate Options:\n"
        "  --pack PATH               FPQ model pack directory\n"
        "  --tokenizer PATH          Tokenizer JSON file\n"
        "  --model-id ID             Resolve pack path through BonfyreModel\n"
        "  --tokenizer-id ID         Resolve tokenizer path through BonfyreModel\n"
        "  --prompt PATH             Input prompt file\n"
        "  --out PATH                Output file\n"
        "  --quic                    Use QUIC service mode with `serve`\n"
        "  --bind ADDR               Bind address for QUIC service (default: 0.0.0.0)\n"
        "  --connect HOST:PORT       QUIC client connection target\n"
        "  --port N                  Port for QUIC service (default: 9443)\n"
        "  --cert PATH               TLS cert for QUIC server\n"
        "  --key PATH                TLS key for QUIC server\n"
        "  --max-new-tokens N        Max tokens to generate (default: 512)\n"
        "  --temperature F           Sampling temperature (default: 0.2)\n"
        "  --greedy                  Use greedy sampling\n\n"
        "  --prefill-chunk N         Override prompt prefill chunk size\n"
        "  --active-kv-window N      Bound active KV attention window\n"
        "  --manifest PATH           Write run manifest to PATH\n"
        "  --token-trace PATH        Write token trace JSONL to PATH\n"
        "  --json PATH               Write benchmark/client JSON to PATH\n"
        "  --golden-out PATH         Write golden compare JSONL during doctor/generate\n"
        "  --request-json JSON       QUIC client request payload\n"
        "  --timeout-ms N            QUIC client receive timeout in milliseconds\n\n"

        "Bench-Generate Options:\n"
        "  --json PATH               Benchmark output JSON\n"
        "  --token-trace PATH        Optional per-token latency JSONL\n"
        "  --load-only               Stop after runtime init/warm\n"
        "  --warm-cache              Warm decode-hot tensors before generate\n"
        "  --resident                Alias for warm-cache on one resident runtime\n"
        "  --reuse-prefix            Alias for warm-cache on one resident runtime\n"
        "  --speed-mode              Disable probe/golden/debug overhead and prefer decode-hot speed\n\n"
        "Tokenizer-Test Options:\n"
        "  --text TEXT               Input text to encode/decode\n\n"
        "Test-Tensor Options:\n"
        "  --tensor NAME             Tensor name to probe\n"
        "  --dump-fp16-rows          Dump raw FP16 row audit (299,300-378,379 + 41/42/43 row300)\n\n"
        "Score-Probe Options:\n"
        "  --tensor NAME             Tensor name to probe\n"
        "  --rows CSV                Explicit row list (default adds boundary rows)\n"
        "  --random-rows N           Add N deterministic random rows\n"
        "  --seed N                  Seed for deterministic random rows\n"
        "  --probe-jsonl PATH        Fast-vs-slow block/row probe output path\n"
        "  --summary PATH            Native/nonshared/shared row summary JSON\n"
        "  --shared-qkv              Also run shared Q/K/V path when tensor is q/k/v\n\n"
        "Environment:\n"
        "  BONFYRE_ACTIVE_WEIGHT_CACHE=1              Enable active packed weight cache\n"
        "  BONFYRE_ACTIVE_CACHE_MAX_MB=4096           Cache size limit (MB)\n"
        "  BONFYRE_ACTIVE_CACHE_DIR=/path             Cache directory override\n"
        "  BONFYRE_QWEN_BACKEND=scalar|neon|neon_fused|flashqla_prefill\n"
        "  BONFYRE_QWEN_PREFILL_CHUNK=64              Chunk size for FlashQLA-style prompt prefill\n"
        "  BONFYRE_QWEN_DEBUG=1                       Enable deep diagnostics\n");
}

static int cmd_serve(const char *pack, const char *tokenizer_path,
                     int quic_mode, const char *bind_addr, int port,
                     const char *cert_path, const char *key_path,
                     int warm_cache, int resident, int speed_mode) {
    qwen_config_t config = qwen_default_config();
    if (speed_mode) qwen_apply_speed_mode_env();
    qwen_runtime_t *rt = qwen_runtime_init(pack, tokenizer_path, &config);
    if (!rt) {
        fprintf(stderr, "Failed to initialize runtime\n");
        return 1;
    }
    if (warm_cache || resident) {
        if (qwen_runtime_warm(rt) != 0) {
            fprintf(stderr, "Failed to warm runtime\n");
            qwen_runtime_free(rt);
            return 1;
        }
    }
    int result = quic_mode
        ? qwen_serve_quic(rt, bind_addr, (uint16_t)port, cert_path, key_path)
        : qwen_serve(rt);

    qwen_runtime_free(rt);
    return result;
}

static int cmd_generate_common(const char *pack, const char *tokenizer_path,
                               const char *prompt_path, const char *out_path,
                               const char *mode, int max_tokens, float temp,
                               int greedy) {
    qwen_config_t config = qwen_default_config();
    config.mode = mode;
    config.max_new_tokens = max_tokens;
    config.temperature = temp;
    config.greedy = greedy;

    qwen_runtime_t *rt = qwen_runtime_init(pack, tokenizer_path, &config);
    if (!rt) {
        fprintf(stderr, "Failed to initialize runtime\n");
        return 1;
    }

    int result = qwen_runtime_generate_file(rt, prompt_path, out_path, NULL, NULL);

    qwen_runtime_free(rt);
    return result;
}

static int cmd_generate_blender(const char *pack, const char *tokenizer_path,
                                const char *prompt_path, const char *out_path,
                                int max_tokens, float temp, int greedy) {
    const char *default_out = "output/ffts-youtube/corpus_v6_4/out/model/qwen_blender_template.py";
    const char *resolved_out = out_path ? out_path : default_out;
    return cmd_generate_common(pack, tokenizer_path, prompt_path, resolved_out,
                              "blender", max_tokens, temp, greedy);
}

static int cmd_generate_video_script(const char *pack, const char *tokenizer_path,
                                     const char *prompt_path, const char *out_path) {
    return cmd_generate_common(pack, tokenizer_path, prompt_path, out_path,
                              "video", 1024, 0.2f, 0);
}

static int cmd_generate_hyperframes(const char *pack, const char *tokenizer_path,
                                    const char *prompt_path, const char *out_path) {
    return cmd_generate_common(pack, tokenizer_path, prompt_path, out_path,
                              "hyperframes", 512, 0.1f, 0);
}

static int cmd_generate_json(const char *pack, const char *tokenizer_path,
                             const char *prompt_path, const char *out_path) {
    return cmd_generate_common(pack, tokenizer_path, prompt_path, out_path,
                              "json", 512, 0.0f, 1);
}

static int cmd_doctor(const char *pack,
                      const char *tokenizer_path,
                      const char *prompt_path,
                      const char *golden_out_path,
                      const char *out_path) {
    qwen_config_t config = qwen_default_config();
    qwen_runtime_t *rt = qwen_runtime_init(pack, tokenizer_path, &config);
    if (!rt) return 1;
    if (prompt_path) {
        size_t len = 0;
        char *prompt = read_text_file(prompt_path, &len);
        if (prompt) {
            int n_ids = 0;
            int *ids = tok_encode(rt->tokenizer, prompt, 0, &n_ids);
            free(ids);
            fprintf(stderr,
                    "doctor: prompt_chars=%zu prompt_tokens=%d recommended_prefill_chunk=%d active_kv_window_hint=%d\n",
                    len, n_ids, recommend_chunk_size(n_ids), n_ids > 2048 ? 1024 : 0);
            free(prompt);
        }
    }
    if (qwen_runtime_warm(rt) != 0) {
        qwen_runtime_free(rt);
        return 1;
    }
    if (golden_out_path && golden_out_path[0]) {
        const char *doctor_out = out_path && out_path[0] ? out_path : "/tmp/bonfyre-qwen-golden.out.txt";
        FILE *golden_fp;
        if (!prompt_path) {
            fprintf(stderr, "doctor --golden-out requires --prompt\n");
            qwen_runtime_free(rt);
            return 1;
        }
        if (bf_ensure_parent_dir(golden_out_path) != 0 || bf_ensure_parent_dir(doctor_out) != 0) {
            qwen_runtime_free(rt);
            return 1;
        }
        golden_fp = fopen(golden_out_path, "w");
        if (!golden_fp) {
            fprintf(stderr, "doctor: cannot create golden artifact %s\n", golden_out_path);
            qwen_runtime_free(rt);
            return 1;
        }
        fclose(golden_fp);
        (void)setenv("BONFYRE_QWEN_GOLDEN_JSONL", golden_out_path, 1);
        config.max_new_tokens = 1;
        rt->config.max_new_tokens = 1;
        if (qwen_runtime_generate_file(rt, prompt_path, doctor_out, NULL, NULL) != 0) {
            qwen_runtime_free(rt);
            return 1;
        }
        fprintf(stderr, "doctor: golden_jsonl=%s output=%s\n", golden_out_path, doctor_out);
        unsetenv("BONFYRE_QWEN_GOLDEN_JSONL");
    }
    {
        char *status = qwen_runtime_status_json(rt);
        fprintf(stderr, "bonfyre-qwen-fpq doctor: runtime OK\n");
        if (status) {
            printf("%s\n", status);
            free(status);
        }
    }
    qwen_runtime_free(rt);
    return 0;
}

static char *read_text_file(const char *path, size_t *len_out) {
    FILE *fp;
    long sz;
    char *buf;
    size_t rd;
    if (!path) return NULL;
    fp = fopen(path, "rb");
    if (!fp) return NULL;
    if (fseek(fp, 0, SEEK_END) != 0) { fclose(fp); return NULL; }
    sz = ftell(fp);
    if (sz < 0) { fclose(fp); return NULL; }
    if (fseek(fp, 0, SEEK_SET) != 0) { fclose(fp); return NULL; }
    buf = (char *)malloc((size_t)sz + 1);
    if (!buf) { fclose(fp); return NULL; }
    rd = fread(buf, 1, (size_t)sz, fp);
    fclose(fp);
    buf[rd] = '\0';
    if (len_out) *len_out = rd;
    return buf;
}

static int recommend_chunk_size(int prompt_tokens) {
    if (prompt_tokens >= 4096) return 160;
    if (prompt_tokens >= 2048) return 128;
    if (prompt_tokens >= 1024) return 96;
    if (prompt_tokens >= 256) return 64;
    return 32;
}

static void print_escaped_text(FILE *fp, const char *s) {
    if (!fp) return;
    if (!s) {
        fputs("\"\"", fp);
        return;
    }
    fputc('"', fp);
    for (size_t i = 0; s[i] != '\0'; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '\\' || c == '"') {
            fputc('\\', fp);
            fputc((int)c, fp);
        } else if (c == '\n') {
            fputs("\\n", fp);
        } else if (c == '\r') {
            fputs("\\r", fp);
        } else if (c == '\t') {
            fputs("\\t", fp);
        } else {
            fputc((int)c, fp);
        }
    }
    fputc('"', fp);
}

static int whitespace_normalized_equal(const char *a, const char *b) {
    size_t ia = 0, ib = 0;
    if (!a || !b) return 0;
    while (a[ia] || b[ib]) {
        while (isspace((unsigned char)a[ia])) ia++;
        while (isspace((unsigned char)b[ib])) ib++;
        while (a[ia] && b[ib] &&
               !isspace((unsigned char)a[ia]) &&
               !isspace((unsigned char)b[ib])) {
            if (a[ia] != b[ib]) return 0;
            ia++;
            ib++;
        }
        if ((!a[ia] || isspace((unsigned char)a[ia])) &&
            (!b[ib] || isspace((unsigned char)b[ib]))) {
            continue;
        }
        return 0;
    }
    return 1;
}

static int cmd_tokenizer_test(const char *tokenizer_path, const char *text) {
    tokenizer_t *tok;
    int *ids;
    int n_ids = 0;
    char *decoded;
    int roundtrip_ok;
    if (!tokenizer_path || !text) {
        fprintf(stderr, "tokenizer-test requires --tokenizer and --text\n");
        return 1;
    }
    tok = tok_load(tokenizer_path);
    if (!tok) return 1;
    ids = tok_encode(tok, text, 0, &n_ids);
    if (!ids) {
        tok_free(tok);
        return 1;
    }
    decoded = tok_decode(tok, ids, n_ids);
    roundtrip_ok = decoded &&
                   (strcmp(text, decoded) == 0 || whitespace_normalized_equal(text, decoded));

    printf("input_text=");
    print_escaped_text(stdout, text);
    printf("\n");
    printf("token_count=%d\n", n_ids);
    printf("token_ids=");
    for (int i = 0; i < n_ids; i++) {
        printf("%s%d", i ? " " : "", ids[i]);
    }
    printf("\n");
    printf("raw_token_pieces=\n");
    for (int i = 0; i < n_ids; i++) {
        printf("  [%d] id=%d piece=", i, ids[i]);
        print_escaped_text(stdout, tok_id_to_str(tok, ids[i]));
        printf("\n");
    }
    printf("decoded_text=");
    print_escaped_text(stdout, decoded ? decoded : "");
    printf("\n");
    printf("roundtrip_ok=%d\n", roundtrip_ok ? 1 : 0);

    free(decoded);
    free(ids);
    tok_free(tok);
    return roundtrip_ok ? 0 : 2;
}

static int cmd_prefill_bench(const char *pack, const char *tokenizer_path,
                             const char *prompt_path, int chunk_size) {
    qwen_config_t config = qwen_default_config();
    qwen_runtime_t *rt;
    size_t len = 0;
    char *prompt;
    int *ids;
    int n_ids = 0;
    int rc;
    if (!prompt_path) {
        fprintf(stderr, "prefill-bench requires --prompt\n");
        return 1;
    }
    if (chunk_size > 0) {
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", chunk_size);
        (void)setenv("BONFYRE_QWEN_PREFILL_CHUNK", buf, 1);
    }
    config.max_new_tokens = 1;
    config.greedy = 1;
    config.temperature = 0.0f;
    rt = qwen_runtime_init(pack, tokenizer_path, &config);
    if (!rt) return 1;
    prompt = read_text_file(prompt_path, &len);
    if (!prompt) {
        qwen_runtime_free(rt);
        return 1;
    }
    ids = tok_encode(rt->tokenizer, prompt, 0, &n_ids);
    fprintf(stderr, "prefill-bench: prompt_chars=%zu prompt_tokens=%d recommended_chunk=%d\n",
            len, n_ids, recommend_chunk_size(n_ids));
    free(ids);
    rc = qwen_runtime_generate(rt, prompt, NULL, NULL, NULL);
    free(prompt);
    if (rc == 0) {
        char *status = qwen_runtime_status_json(rt);
        if (status) {
            printf("%s\n", status);
            free(status);
        }
    }
    qwen_runtime_free(rt);
    return rc == 0 ? 0 : 1;
}

static int cmd_score_probe(const char *pack, const char *tensor_name,
                           const char *rows_csv, const char *probe_jsonl_path,
                           const char *summary_path, int shared_qkv,
                           int random_rows, uint32_t seed) {
    fpq_model_t *model = NULL;
    const fpq_tensor_info_t *info = NULL;
    qwen_row_list_t rows = {0};
    float *x = NULL;
    float *y = NULL;
    float *shared_outputs[3] = {0};
    const char *shared_names_const[3] = {0};
    char shared_names[3][256];
    int primary_shared_index = -1;
    int rc = 1;
    char rows_env[2048];
    double t_stage0 = 0.0;
    double t_stage1 = 0.0;

    if (!pack || !tensor_name || !probe_jsonl_path || !summary_path) {
        fprintf(stderr, "score-probe requires --pack, --tensor, --probe-jsonl, and --summary\n");
        return 1;
    }
    if (bf_ensure_parent_dir(probe_jsonl_path) != 0 || bf_ensure_parent_dir(summary_path) != 0) {
        fprintf(stderr, "score-probe: failed to ensure output parent dir\n");
        return 1;
    }
    {
        FILE *fp = fopen(probe_jsonl_path, "w");
        if (!fp) {
            fprintf(stderr, "score-probe: cannot open %s\n", probe_jsonl_path);
            return 1;
        }
        fclose(fp);
    }

    model = fpq_open(pack);
    if (!model) {
        fprintf(stderr, "score-probe: failed to open pack %s\n", pack);
        return 1;
    }
    info = fpq_tensor_find(model, tensor_name);
    if (!info) {
        fprintf(stderr, "score-probe: tensor '%s' not found\n", tensor_name);
        goto cleanup;
    }
    if (qwen_parse_rows_csv(rows_csv, &rows) != 0) goto cleanup;
    qwen_add_default_probe_rows(&rows, info->rows);
    qwen_add_deterministic_random_rows(&rows, info->rows, random_rows, seed);
    if (rows.n_rows == 0) goto cleanup;
    if (qwen_rows_to_csv(&rows, rows_env, sizeof(rows_env)) != 0) goto cleanup;

    x = (float *)calloc(info->cols, sizeof(float));
    y = (float *)calloc(info->rows, sizeof(float));
    if (!x || !y) goto cleanup;
    for (size_t i = 0; i < info->cols; i++) x[i] = (float)(i % 256) / 128.0f - 1.0f;

    (void)setenv("BONFYRE_QWEN_SLI_SCORE_PROBE", "1", 1);
    (void)setenv("BONFYRE_QWEN_SLI_SCORE_PROBE_ONLY_ROWS", "1", 1);
    (void)setenv("BONFYRE_QWEN_SLI_SCORE_PROBE_JSONL", probe_jsonl_path, 1);
    (void)setenv("BONFYRE_QWEN_SLI_SCORE_PROBE_TENSOR", tensor_name, 1);
    (void)setenv("BONFYRE_QWEN_SLI_SCORE_PROBE_ROWS", rows_env, 1);
    if (shared_qkv) (void)setenv("BONFYRE_QWEN_SLI_SCORE_PROBE_SHARED_QKV", "1", 1);

    if (fpq_prepare_tensor(model, tensor_name) != 0) {
        fprintf(stderr, "score-probe: prepare failed for %s\n", tensor_name);
        goto cleanup;
    }
    t_stage0 = monotonic_seconds_now();
    fprintf(stderr, "score-probe stage=nonshared begin tensor=%s\n", tensor_name);
    fflush(stderr);
    if (fpq_matmul(model, tensor_name, x, y) != 0) {
        fprintf(stderr, "score-probe: nonshared matmul failed for %s\n", tensor_name);
        goto cleanup;
    }
    t_stage1 = monotonic_seconds_now();
    fprintf(stderr, "score-probe stage=nonshared end tensor=%s sec=%.3f\n",
            tensor_name, t_stage1 - t_stage0);
    fflush(stderr);

    if (shared_qkv && qwen_derive_qkv_group(tensor_name, shared_names, &primary_shared_index) == 0) {
        for (int i = 0; i < 3; i++) {
            const fpq_tensor_info_t *shared_info = fpq_tensor_find(model, shared_names[i]);
            if (!shared_info) {
                primary_shared_index = -1;
                break;
            }
            shared_outputs[i] = (float *)calloc(shared_info->rows, sizeof(float));
            if (!shared_outputs[i]) {
                primary_shared_index = -1;
                break;
            }
            shared_names_const[i] = shared_names[i];
        }
        if (primary_shared_index >= 0 &&
            (fprintf(stderr, "score-probe stage=shared begin tensor=%s\n", tensor_name), fflush(stderr), 1) &&
            fpq_matmul_shared(model, 3, shared_names_const, x, shared_outputs) != 0) {
            fprintf(stderr, "score-probe: shared qkv matmul failed for %s\n", tensor_name);
            primary_shared_index = -1;
        } else if (primary_shared_index >= 0) {
            double t_shared = monotonic_seconds_now();
            fprintf(stderr, "score-probe stage=shared end tensor=%s sec=%.3f\n",
                    tensor_name, t_shared - t_stage1);
            fflush(stderr);
        }
    }

    {
        FILE *summary = fopen(summary_path, "w");
        if (!summary) goto cleanup;
        fprintf(summary, "{\n");
        fprintf(summary, "  \"schema_version\": \"bonfyre.qwen_fpq_score_probe.v1\",\n");
        fprintf(summary, "  \"tensor\": ");
        print_escaped_text(summary, tensor_name);
        fprintf(summary, ",\n");
        fprintf(summary, "  \"layer\": %d,\n", qwen_tensor_layer_from_name(tensor_name));
        fprintf(summary, "  \"projection\": ");
        print_escaped_text(summary, qwen_projection_from_name(tensor_name));
        fprintf(summary, ",\n");
        fprintf(summary, "  \"rows\": [\n");
        fprintf(stderr, "score-probe stage=native_rows begin tensor=%s rows=%zu\n",
                tensor_name, rows.n_rows);
        fflush(stderr);
        t_stage0 = monotonic_seconds_now();
        for (size_t i = 0; i < rows.n_rows; i++) {
            size_t row = rows.rows[i];
            float native = 0.0f;
            double nonshared_delta = NAN;
            double shared_delta = NAN;
            int native_rc = fpq_matmul_row_native(model, tensor_name, row, x, &native);
            if (native_rc == 0) {
                nonshared_delta = (double)y[row] - (double)native;
                if (primary_shared_index >= 0 && shared_outputs[primary_shared_index]) {
                    shared_delta = (double)shared_outputs[primary_shared_index][row] - (double)native;
                }
                size_t blocks_per_row = (info->cols + 255u) / 256u;
                for (size_t bj = 0; bj < blocks_per_row; bj++) {
                    size_t col_start = 0;
                    size_t col_end = 0;
                    float native_block = 0.0f;
                    float native_row_total = 0.0f;
                    float native_residual_block = 0.0f;
                    if (fpq_native_block_reference(model, tensor_name, row, x, bj,
                                                   &native_block, &native_row_total,
                                                   &col_start, &col_end) != 0) {
                        continue;
                    }
                    emit_native_block_score_jsonl(probe_jsonl_path,
                                                  tensor_name,
                                                  qwen_tensor_layer_from_name(tensor_name),
                                                  qwen_projection_from_name(tensor_name),
                                                  info->rows,
                                                  info->cols,
                                                  blocks_per_row,
                                                  row,
                                                  row * blocks_per_row + bj,
                                                  col_start,
                                                  col_end - col_start,
                                                  native_block,
                                                  native_row_total,
                                                  y[row]);
                    if (fpq_native_residual_block_reference(model, tensor_name, row, x, bj,
                                                            &native_residual_block,
                                                            &col_start, &col_end) == 0) {
                        emit_native_residual_block_score_jsonl(probe_jsonl_path,
                                                               tensor_name,
                                                               qwen_tensor_layer_from_name(tensor_name),
                                                               qwen_projection_from_name(tensor_name),
                                                               info->rows,
                                                               info->cols,
                                                               blocks_per_row,
                                                               row,
                                                               row * blocks_per_row + bj,
                                                               col_start,
                                                               col_end - col_start,
                                                               native_residual_block);
                    }
                }
            }
            fprintf(summary,
                    "    {\"row\":%zu,\"native_rc\":%d,\"native\":%.9g,"
                    "\"nonshared\":%.9g,\"nonshared_native_delta\":",
                    row, native_rc, native, y[row]);
            if (native_rc == 0) fprintf(summary, "%.9g,", nonshared_delta);
            else fprintf(summary, "null,");
            fprintf(summary, "\"shared_enabled\":%s,\"shared\":",
                    primary_shared_index >= 0 ? "true" : "false");
            if (primary_shared_index >= 0 && shared_outputs[primary_shared_index]) {
                fprintf(summary, "%.9g,\"shared_native_delta\":", shared_outputs[primary_shared_index][row]);
                if (native_rc == 0) fprintf(summary, "%.9g}", shared_delta);
                else fprintf(summary, "null}");
            } else {
                fprintf(summary, "null,\"shared_native_delta\":null}");
            }
            fprintf(summary, "%s\n", (i + 1 < rows.n_rows) ? "," : "");
        }
        t_stage1 = monotonic_seconds_now();
        fprintf(stderr, "score-probe stage=native_rows end tensor=%s sec=%.3f\n",
                tensor_name, t_stage1 - t_stage0);
        fflush(stderr);
        fprintf(summary, "  ],\n");
        fprintf(summary, "  \"probe_jsonl\": ");
        print_escaped_text(summary, probe_jsonl_path);
        fprintf(summary, "\n}\n");
        fclose(summary);
    }
    rc = 0;

cleanup:
    for (int i = 0; i < 3; i++) free(shared_outputs[i]);
    free(x);
    free(y);
    qwen_row_list_free(&rows);
    if (model) fpq_close(model);
    return rc;
}

#ifdef BF_HAS_QUIC
typedef struct {
    int got_any;
    int token_events;
    int response_events;
    int token_streams_opened;
    double start_seconds;
    double first_token_seconds;
    double last_event_seconds;
    char last_response[4096];
} qwen_quic_client_ctx_t;

static void qwen_quic_client_recv(const char *family_key,
                                  const uint8_t *data, size_t len,
                                  int fin, void *user) {
    qwen_quic_client_ctx_t *ctx = (qwen_quic_client_ctx_t *)user;
    (void)fin;
    if (!data || len == 0) return;
    double now = monotonic_seconds_now();
    if (ctx) ctx->last_event_seconds = now - ctx->start_seconds;
    if (ctx && family_key && strcmp(family_key, "qwen-token") == 0) {
        ctx->token_events++;
        if (ctx->first_token_seconds <= 0.0) ctx->first_token_seconds = now - ctx->start_seconds;
    } else if (ctx && family_key && strcmp(family_key, "qwen-response") == 0) {
        ctx->response_events++;
        size_t copy_len = len < sizeof(ctx->last_response) - 1 ? len : sizeof(ctx->last_response) - 1;
        memcpy(ctx->last_response, data, copy_len);
        ctx->last_response[copy_len] = '\0';
        {
            const char *needle = "\"token_streams_opened\":";
            const char *p = strstr(ctx->last_response, needle);
            if (p) ctx->token_streams_opened = atoi(p + (int)strlen(needle));
        }
    }
    fwrite(data, 1, len, stdout);
    fputc('\n', stdout);
    fflush(stdout);
    if (ctx) ctx->got_any = 1;
}

static int cmd_quic_client(const char *host, int port,
                           const char *cert_path, const char *key_path,
                           const char *request_json,
                           const char *json_path,
                           int timeout_ms) {
    bf_quic_ctx_t *ctx;
    bf_quic_conn_t *conn;
    bf_quic_stream_meta_t meta = {0};
    bf_quic_stream_t *stream;
    qwen_quic_client_ctx_t recv_ctx = {0};
    int polls;
    int quiet_polls;
    (void)cert_path;
    (void)key_path;
    if (!request_json) request_json = "{\"type\":\"status\"}";
    recv_ctx.start_seconds = monotonic_seconds_now();
    if (timeout_ms <= 0) timeout_ms = 30000;
    polls = timeout_ms / 100;
    if (polls < 1) polls = 1;
    quiet_polls = polls;
    ctx = bf_quic_ctx_new(NULL, NULL, ".bonfyre-qwen-quic-ticket-client");
    if (!ctx) return 1;
    conn = bf_quic_connect(ctx, host ? host : "127.0.0.1", (uint16_t)port);
    if (!conn) {
        bf_quic_ctx_free(ctx);
        return 1;
    }
    (void)bf_quic_recv_start(conn, qwen_quic_client_recv, &recv_ctx);
    for (int i = 0; i < 10 && !recv_ctx.got_any; i++) {
        (void)bf_quic_recv_poll(conn, 100);
    }
    snprintf(meta.family_key, sizeof(meta.family_key), "%s", "qwen-request");
    meta.layer = BF_LAYER_SURFACE;
    meta.total_bytes = (uint64_t)strlen(request_json);
    stream = NULL;
    for (int attempt = 0; attempt < 50 && !stream; attempt++) {
        stream = bf_quic_stream_open(conn, &meta);
        if (!stream) {
            (void)bf_quic_recv_poll(conn, 100);
        }
    }
    if (!stream) {
        bf_quic_conn_close(conn);
        bf_quic_ctx_free(ctx);
        return 1;
    }
    (void)bf_quic_stream_write(stream, (const uint8_t *)request_json, strlen(request_json), 1);
    bf_quic_stream_close(stream);
    while (polls-- > 0 && !recv_ctx.got_any) {
        (void)bf_quic_recv_poll(conn, 100);
    }
    while (quiet_polls-- > 0 && recv_ctx.response_events == 0 && recv_ctx.token_events > 0) {
        (void)bf_quic_recv_poll(conn, 100);
    }
    if (json_path && json_path[0]) {
        FILE *fp;
        if (bf_ensure_parent_dir(json_path) == 0 && (fp = fopen(json_path, "w")) != NULL) {
            fprintf(fp,
                    "{\n"
                    "  \"schema_version\": \"bonfyre.qwen_fpq_quic_client_result.v1\",\n"
                    "  \"host\": ");
            print_escaped_text(fp, host ? host : "127.0.0.1");
            fprintf(fp,
                    ",\n  \"port\": %d,\n"
                    "  \"token_events\": %d,\n"
                    "  \"response_events\": %d,\n"
                    "  \"token_streams_opened\": %d,\n"
                    "  \"first_token_seconds\": %.6f,\n"
                    "  \"last_event_seconds\": %.6f,\n"
                    "  \"got_any\": %s,\n"
                    "  \"last_response\": ",
                    port,
                    recv_ctx.token_events,
                    recv_ctx.response_events,
                    recv_ctx.token_streams_opened,
                    recv_ctx.first_token_seconds,
                    recv_ctx.last_event_seconds,
                    recv_ctx.got_any ? "true" : "false");
            print_escaped_text(fp, recv_ctx.last_response);
            fprintf(fp, ",\n  \"request_json\": ");
            print_escaped_text(fp, request_json);
            fprintf(fp, "\n}\n");
            fclose(fp);
        }
    }
    bf_quic_conn_close(conn);
    bf_quic_ctx_free(ctx);
    return recv_ctx.got_any ? 0 : 1;
}
#else
static int cmd_quic_client(const char *host, int port,
                           const char *cert_path, const char *key_path,
                           const char *request_json,
                           const char *json_path,
                           int timeout_ms) {
    (void)host; (void)port; (void)cert_path; (void)key_path; (void)request_json; (void)json_path;
    (void)timeout_ms;
    fprintf(stderr, "QUIC support not built\n");
    return 1;
}
#endif

int main(int argc, char **argv) {
    int arg_start = 2;
    if (argc < 2) {
        usage();
        return 1;
    }

    const char *cmd = argv[1];
    if (strcmp(cmd, "help") == 0 || strcmp(cmd, "--help") == 0) {
        usage();
        return 0;
    }
    if (strcmp(cmd, "qwen-fpq") == 0 && argc >= 3) {
        cmd = argv[2];
        arg_start = 3;
        if (strcmp(cmd, "help") == 0 || strcmp(cmd, "--help") == 0) {
            usage();
            return 0;
        }
    }
    if (!getenv("BONFYRE_MODEL_BIN")) {
        (void)setenv("BONFYRE_MODEL_BIN", model_bin_path(), 0);
    }

    /* Default paths */
    const char *pack = getenv("BONFYRE_QWEN_PACK");
    const char *tokenizer = getenv("BONFYRE_QWEN_TOKENIZER");
    const char *model_id = getenv("BONFYRE_QWEN_MODEL_ID");
    const char *tokenizer_id = getenv("BONFYRE_QWEN_TOKENIZER_ID");
    const char *prompt_path = NULL;
    const char *out_path = NULL;
    const char *manifest_path = getenv("BONFYRE_QWEN_MANIFEST_PATH");
    const char *token_trace_path = getenv("BONFYRE_QWEN_TOKEN_TRACE_PATH");
    const char *golden_out_path = getenv("BONFYRE_QWEN_GOLDEN_JSONL");
    const char *json_path = NULL;
    const char *request_json = NULL;
    const char *text_arg = NULL;
    const char *bind_addr = "0.0.0.0";
    const char *connect_addr = NULL;
    const char *cert_path = getenv("BONFYRE_QWEN_QUIC_CERT");
    const char *key_path = getenv("BONFYRE_QWEN_QUIC_KEY");
    int max_new_tokens = 512;
    float temp = 0.2f;
    int greedy = 0;
    int quic_mode = 0;
    int port = 9443;
    int timeout_ms = 30000;
    int prefill_chunk = -1;
    int active_kv_window = -1;
    int bench_load_only = 0;
    int bench_warm_cache = 0;
    int bench_resident = 0;
    int bench_reuse_prefix = 0;
    int bench_speed_mode = 0;

    /* Parse common options */
    for (int i = arg_start; i < argc; i++) {
        if (strcmp(argv[i], "--pack") == 0 && i + 1 < argc) pack = argv[++i];
        else if (strcmp(argv[i], "--model-id") == 0 && i + 1 < argc) model_id = argv[++i];
        else if (strcmp(argv[i], "--tokenizer") == 0 && i + 1 < argc) tokenizer = argv[++i];
        else if (strcmp(argv[i], "--tokenizer-id") == 0 && i + 1 < argc) tokenizer_id = argv[++i];
        else if (strcmp(argv[i], "--prompt") == 0 && i + 1 < argc) prompt_path = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out_path = argv[++i];
        else if (strcmp(argv[i], "--manifest") == 0 && i + 1 < argc) manifest_path = argv[++i];
        else if (strcmp(argv[i], "--token-trace") == 0 && i + 1 < argc) token_trace_path = argv[++i];
        else if (strcmp(argv[i], "--json") == 0 && i + 1 < argc) json_path = argv[++i];
        else if (strcmp(argv[i], "--golden-out") == 0 && i + 1 < argc) golden_out_path = argv[++i];
        else if (strcmp(argv[i], "--request-json") == 0 && i + 1 < argc) request_json = argv[++i];
        else if (strcmp(argv[i], "--text") == 0 && i + 1 < argc) text_arg = argv[++i];
        else if (strcmp(argv[i], "--quic") == 0) quic_mode = 1;
        else if (strcmp(argv[i], "--bind") == 0 && i + 1 < argc) bind_addr = argv[++i];
        else if (strcmp(argv[i], "--connect") == 0 && i + 1 < argc) connect_addr = argv[++i];
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--timeout-ms") == 0 && i + 1 < argc) timeout_ms = atoi(argv[++i]);
        else if (strcmp(argv[i], "--cert") == 0 && i + 1 < argc) cert_path = argv[++i];
        else if (strcmp(argv[i], "--key") == 0 && i + 1 < argc) key_path = argv[++i];
        else if (strcmp(argv[i], "--max-new-tokens") == 0 && i + 1 < argc) max_new_tokens = atoi(argv[++i]);
        else if (strcmp(argv[i], "--temperature") == 0 && i + 1 < argc) temp = (float)atof(argv[++i]);
        else if (strcmp(argv[i], "--prefill-chunk") == 0 && i + 1 < argc) prefill_chunk = atoi(argv[++i]);
        else if (strcmp(argv[i], "--active-kv-window") == 0 && i + 1 < argc) active_kv_window = atoi(argv[++i]);
        else if (strcmp(argv[i], "--load-only") == 0) bench_load_only = 1;
        else if (strcmp(argv[i], "--warm-cache") == 0) bench_warm_cache = 1;
        else if (strcmp(argv[i], "--resident") == 0) bench_resident = 1;
        else if (strcmp(argv[i], "--reuse-prefix") == 0) bench_reuse_prefix = 1;
        else if (strcmp(argv[i], "--speed-mode") == 0) bench_speed_mode = 1;
        else if (strcmp(argv[i], "--greedy") == 0) greedy = 1;
    }
    if ((!pack || !pack[0]) && model_id) pack = model_id;
    if ((!tokenizer || !tokenizer[0]) && (!tokenizer_id || !tokenizer_id[0]) && model_id) {
        tokenizer_id = guess_tokenizer_id(model_id);
    }
    if ((!tokenizer || !tokenizer[0]) && tokenizer_id) tokenizer = tokenizer_id;
    if (model_id && model_id[0]) (void)setenv("BONFYRE_QWEN_MODEL_ID", model_id, 1);
    if (tokenizer_id && tokenizer_id[0]) (void)setenv("BONFYRE_QWEN_TOKENIZER_ID", tokenizer_id, 1);
    if (manifest_path && manifest_path[0]) (void)setenv("BONFYRE_QWEN_MANIFEST_PATH", manifest_path, 1);
    if (token_trace_path && token_trace_path[0]) (void)setenv("BONFYRE_QWEN_TOKEN_TRACE_PATH", token_trace_path, 1);
    if (golden_out_path && golden_out_path[0]) (void)setenv("BONFYRE_QWEN_GOLDEN_JSONL", golden_out_path, 1);
    if (prefill_chunk > 0) {
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", prefill_chunk);
        (void)setenv("BONFYRE_QWEN_PREFILL_CHUNK", buf, 1);
    }
    if (active_kv_window > 0) {
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", active_kv_window);
        (void)setenv("BONFYRE_ACTIVE_KV_WINDOW", buf, 1);
    }
    char *resolved_pack = resolve_registry_ref(pack);
    char *resolved_tokenizer = resolve_registry_ref(tokenizer);
    int needs_model = strcmp(cmd, "quic-client") != 0 && strcmp(cmd, "tokenizer-test") != 0;
    if (resolved_pack && resolved_pack[0]) pack = resolved_pack;
    if (resolved_tokenizer && resolved_tokenizer[0]) tokenizer = resolved_tokenizer;
    if (connect_addr && connect_addr[0]) {
        const char *colon = strrchr(connect_addr, ':');
        if (colon && colon[1]) {
            static char host_buf[256];
            size_t host_len = (size_t)(colon - connect_addr);
            if (host_len >= sizeof(host_buf)) host_len = sizeof(host_buf) - 1;
            memcpy(host_buf, connect_addr, host_len);
            host_buf[host_len] = '\0';
            bind_addr = host_buf;
            port = atoi(colon + 1);
        } else {
            bind_addr = connect_addr;
        }
    }

    if (needs_model && !pack) {
        fprintf(stderr, "Error: --pack required or set BONFYRE_QWEN_PACK\n");
        usage();
        free(resolved_pack);
        free(resolved_tokenizer);
        return 1;
    }

    /* Dispatch commands */
    if (strcmp(cmd, "serve") == 0) {
        int rc = cmd_serve(pack, tokenizer, quic_mode, bind_addr, port, cert_path, key_path,
                           bench_warm_cache, bench_resident, bench_speed_mode);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "serve-local") == 0) {
        int rc = cmd_serve(pack, tokenizer, 0, bind_addr, port, cert_path, key_path,
                           bench_warm_cache, bench_resident, bench_speed_mode);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "serve-quic") == 0) {
        int rc = cmd_serve(pack, tokenizer, 1, bind_addr, port, cert_path, key_path,
                           bench_warm_cache, bench_resident, bench_speed_mode);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "doctor") == 0) {
        int rc = cmd_doctor(pack, tokenizer, prompt_path, golden_out_path, out_path);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "prefill-bench") == 0) {
        int rc = cmd_prefill_bench(pack, tokenizer, prompt_path, prefill_chunk);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "bench-generate") == 0) {
        qwen_bench_options_t opts = {
            .pack = pack,
            .tokenizer_path = tokenizer,
            .prompt_path = prompt_path,
            .json_path = json_path,
            .trace_path = token_trace_path,
            .max_new_tokens = max_new_tokens,
            .temperature = temp,
            .greedy = greedy,
            .load_only = bench_load_only,
            .warm_cache = bench_warm_cache,
            .resident = bench_resident,
            .speed_mode = bench_speed_mode,
            .reuse_prefix = bench_reuse_prefix,
        };
        int rc = cmd_bench_generate(&opts);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "score-probe") == 0) {
        const char *tname = NULL;
        const char *rows_csv = NULL;
        const char *probe_jsonl_path = NULL;
        const char *summary_path = NULL;
        int shared_qkv = 0;
        int random_rows = 0;
        uint32_t probe_seed = 0xC0FFEEu;
        for (int i = arg_start; i < argc; i++) {
            if (strcmp(argv[i], "--tensor") == 0 && i + 1 < argc) tname = argv[++i];
            else if (strcmp(argv[i], "--rows") == 0 && i + 1 < argc) rows_csv = argv[++i];
            else if (strcmp(argv[i], "--probe-jsonl") == 0 && i + 1 < argc) probe_jsonl_path = argv[++i];
            else if (strcmp(argv[i], "--summary") == 0 && i + 1 < argc) summary_path = argv[++i];
            else if (strcmp(argv[i], "--random-rows") == 0 && i + 1 < argc) random_rows = atoi(argv[++i]);
            else if (strcmp(argv[i], "--seed") == 0 && i + 1 < argc) probe_seed = (uint32_t)strtoul(argv[++i], NULL, 10);
            else if (strcmp(argv[i], "--shared-qkv") == 0) shared_qkv = 1;
        }
        int rc = cmd_score_probe(pack, tname, rows_csv,
                                 probe_jsonl_path ? probe_jsonl_path : "/tmp/qwen-score-probe.jsonl",
                                 summary_path ? summary_path : "/tmp/qwen-score-probe-summary.json",
                                 shared_qkv, random_rows, probe_seed);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "quic-client") == 0) {
        const char *host = (bind_addr && strcmp(bind_addr, "0.0.0.0") == 0) ? "127.0.0.1" : bind_addr;
        int rc = cmd_quic_client(host, port, cert_path, key_path, request_json, json_path, timeout_ms);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "tokenizer-test") == 0) {
        int rc = cmd_tokenizer_test(tokenizer, text_arg);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "generate") == 0) {
        if (!prompt_path || !out_path) {
            fprintf(stderr, "Error: generate requires --prompt and --out\n");
            free(resolved_pack);
            free(resolved_tokenizer);
            return 1;
        }
        int rc = cmd_generate_common(pack, tokenizer, prompt_path, out_path,
                                     "code", max_new_tokens, temp, greedy);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "generate-blender-template") == 0) {
        if (!prompt_path) {
            fprintf(stderr, "Error: generate-blender-template requires --prompt\n");
            free(resolved_pack);
            free(resolved_tokenizer);
            return 1;
        }
        int rc = cmd_generate_blender(pack, tokenizer, prompt_path, out_path,
                                      max_new_tokens, temp, greedy);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "generate-video-script") == 0) {
        if (!prompt_path || !out_path) {
            fprintf(stderr, "Error: generate-video-script requires --prompt and --out\n");
            free(resolved_pack);
            free(resolved_tokenizer);
            return 1;
        }
        int rc = cmd_generate_video_script(pack, tokenizer, prompt_path, out_path);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "generate-hyperframes") == 0) {
        if (!prompt_path || !out_path) {
            fprintf(stderr, "Error: generate-hyperframes requires --prompt and --out\n");
            free(resolved_pack);
            free(resolved_tokenizer);
            return 1;
        }
        int rc = cmd_generate_hyperframes(pack, tokenizer, prompt_path, out_path);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else if (strcmp(cmd, "generate-json") == 0) {
        if (!prompt_path || !out_path) {
            fprintf(stderr, "Error: generate-json requires --prompt and --out\n");
            free(resolved_pack);
            free(resolved_tokenizer);
            return 1;
        }
        int rc = cmd_generate_json(pack, tokenizer, prompt_path, out_path);
        free(resolved_pack);
        free(resolved_tokenizer);
        return rc;
    }
    else

    if (strcmp(cmd, "test-load") == 0) {
        const char *pack = NULL;

        for (int i = 2; i < argc; i++) {
            if (strcmp(argv[i], "--pack") == 0 && i + 1 < argc) {
                pack = argv[++i];
            }
        }

        if (!pack) {
            fprintf(stderr, "usage: %s test-load --pack <fpq-pack.json>\n", argv[0]);
            return 2;
        }

        fpq_model_t *model = fpq_open(pack);
        if (!model) {
            fprintf(stderr, "test-load: failed to open model %s\n", pack);
            return 1;
        }

        fpq_info_t info = fpq_info(model);
        fprintf(stderr,
                "test-load: opened tensors=%zu sli=%zu passthrough=%zu params=%zu format=%u\n",
                info.n_tensors,
                info.n_sli_tensors,
                info.n_passthrough,
                info.total_params,
                info.format_version);

        return 0; /* load-only gate: let OS reclaim model */
    }

if (strcmp(cmd, "test-tensor") == 0) {
        const char *tname = "model.layers.42.self_attn.q_proj.weight";
        int dump_fp16_rows = 0;
        for (int i = 2; i < argc; i++) {
            if (strcmp(argv[i], "--tensor") == 0 && i + 1 < argc) tname = argv[++i];
            else if (strcmp(argv[i], "--dump-fp16-rows") == 0) dump_fp16_rows = 1;
        }
        fpq_model_t *tmodel = fpq_open(pack);
        if (!tmodel) {
            fprintf(stderr, "test-tensor: failed to open model %s\n", pack);
            free(resolved_pack);
            free(resolved_tokenizer);
            return 1;
        }
        const fpq_tensor_info_t *tinfo = fpq_tensor_find(tmodel, tname);
        if (!tinfo) {
            fprintf(stderr, "test-tensor: tensor '%s' not found\n", tname);
            fpq_close(tmodel);
            return 1;
        }
        size_t rows = (size_t)tinfo->rows;
        size_t cols = (size_t)tinfo->cols;
        fprintf(stderr,
                "test-tensor: tensor=%s rows=%zu cols=%zu has_sli=%d bpw=%.6f\n",
                tname, rows, cols, tinfo->has_sli, (double)tinfo->bpw);

        {
            uint64_t overrun_bytes = 0;
            int erc = fpq_debug_tensor_extent_overrun(tmodel, tname, &overrun_bytes);
            if (erc > 0) {
                fprintf(stderr,
                        "FATAL tensor extent exceeds shard tensor=%s overrun_bytes=%" PRIu64 "\n",
                        tname, overrun_bytes);
                fpq_close(tmodel);
                free(resolved_pack);
                free(resolved_tokenizer);
                return 1;
            }
        }

        if (dump_fp16_rows) {
            size_t probe_rows[86];
            size_t n_probe = 0;
            probe_rows[n_probe++] = 299;
            for (size_t r = 300; r <= 378; r++) probe_rows[n_probe++] = r;
            probe_rows[n_probe++] = 379;

            int dump_rc = fpq_debug_dump_fp16_rows(tmodel, tname, probe_rows, n_probe);
            fprintf(stderr,
                    "test-tensor: fp16-row-dump tensor=%s rc=%d rows_dumped=%zu\n",
                    tname, dump_rc, n_probe);

            {
                const char *cmp_tensors[3] = {
                    "model.layers.41.self_attn.v_proj.weight",
                    "model.layers.42.self_attn.v_proj.weight",
                    "model.layers.43.self_attn.v_proj.weight"
                };
                const size_t cmp_row = 300;
                for (size_t i = 0; i < 3; i++) {
                    int cmp_rc = fpq_debug_dump_fp16_rows(tmodel, cmp_tensors[i], &cmp_row, 1);
                    fprintf(stderr,
                            "test-tensor: fp16-row-compare tensor=%s row=%zu rc=%d\n",
                            cmp_tensors[i], cmp_row, cmp_rc);
                }
            }

            if (dump_rc != 0) {
                fprintf(stderr,
                        "test-tensor: FAIL tensor=%s reason=raw_fp16_row_audit_failed rc=%d\n",
                        tname, dump_rc);
                fpq_close(tmodel);
                return 1;
            }
        }

        float *x = (float *)calloc(cols, sizeof(float));
        float *y = (float *)malloc(rows * sizeof(float));
        if (!x || !y) {
            fprintf(stderr, "test-tensor: OOM\n");
            free(x); free(y); fpq_close(tmodel); return 1;
        }
        /* Deterministic finite input */
        for (size_t i = 0; i < cols; i++)
            x[i] = (float)(i % 256) / 128.0f - 1.0f;
        /* NaN sentinel: any unwritten element remains detectable */
        for (size_t i = 0; i < rows; i++) y[i] = NAN;
        int rc = fpq_matmul(tmodel, tname, x, y);
        int finite_n = 0, nan_n = 0, inf_n = 0;
        double sum_abs = 0.0;
        for (size_t i = 0; i < rows; i++) {
            if (isnan(y[i])) nan_n++;
            else if (isinf(y[i])) inf_n++;
            else { finite_n++; sum_abs += fabs((double)y[i]); }
        }
        fprintf(stderr,
                "test-tensor: tensor=%s rc=%d finite=%d nan=%d inf=%d sum_abs=%g\n",
                tname, rc, finite_n, nan_n, inf_n, sum_abs);
        int pass = (rc == 0 && finite_n > 0 && sum_abs > 0.0 && nan_n == 0);
        fprintf(stderr, "test-tensor: %s tensor=%s\n",
                pass ? "PASS" : "FAIL", tname);
        free(x); free(y);
        fpq_close(tmodel);
        return pass ? 0 : 1;
    }
    else {
        fprintf(stderr, "Error: unknown command '%s'\n", cmd);
        usage();
        return 1;
    }
}
