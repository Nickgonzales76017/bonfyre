#include <pthread.h>
#include <stdlib.h>
#include <limits.h>
#ifndef PTHREAD_STACK_MIN
#define PTHREAD_STACK_MIN 16384
#endif


static pthread_attr_t *qwen_small_pthread_attr(void) {
    static pthread_attr_t attr;
    static int initialized = 0;
    static int enabled = 0;

    if (!initialized) {
        initialized = 1;

        size_t kb = 256;
        const char *v = getenv("BONFYRE_QWEN_WORKER_STACK_KB");
        if (v && *v) {
            unsigned long parsed = strtoul(v, NULL, 10);
            if (parsed >= 64 && parsed <= 8192) kb = (size_t)parsed;
        }

        size_t bytes = kb * 1024u;
        if (bytes < (size_t)PTHREAD_STACK_MIN) bytes = (size_t)PTHREAD_STACK_MIN;

        if (pthread_attr_init(&attr) == 0 &&
            pthread_attr_setstacksize(&attr, bytes) == 0) {
            enabled = 1;
        } else {
            pthread_attr_destroy(&attr);
            enabled = 0;
        }
    }

    return enabled ? &attr : NULL;
}

#ifdef __APPLE__
extern unsigned long malloc_zone_pressure_relief(void *zone, unsigned long goal);
#endif

#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/task_info.h>
#endif

/*
 * fpq_run.c — LLaMA-style transformer inference for Bonfyre Ember
 *
 * Supports: LLaMA-2, TinyLlama, Mistral (GQA), Qwen2 architectures.
 * All weight matmuls route through fpq_matmul() (SLI, zero decode).
 * Non-linear ops (rmsnorm, rope, silu, softmax, sampling) are ~200 LoC.
 *
 * Architecture params are read from a config JSON sidecar or specified
 * via fpq_run_config_t. A default config matches TinyLlama-1.1B.
 *
 * Usage (from fpq_cli.c):
 *   fpq run path/to/model.fpq [--tokenizer path/to/tokenizer.json]
 *              [--sys "You are..."] "User prompt"
 *              [--max-tokens N] [--temp F] [--top-p F] [--greedy]
 */
#include "fpq_run.h"
#include <bonfyre.h>
#include "libfpq.h"
#include "fpq_active_cache.h"
#include "fpq_kernels_neon.h"
#include "tokenizer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <math.h>
#include <float.h>
#include <time.h>
#include <stdint.h>
#include <pthread.h>
#ifdef _OPENMP
#include <omp.h>
#endif

#define FPQ_RUN_SAMPLE_TOPK_DEFAULT 256

typedef struct fpq_worker fpq_worker_t;

struct fpq_run_state {
    fpq_run_config_t cfg;
    float *h;
    float *h_norm;
    float *q_buf;
    float *k_buf;
    float *v_buf;
    float *attn_out;
    float *o_buf;
    float *gate_buf;
    float *up_buf;
    float *ffn_out;
    float *logits;
    float *att_scratch;
    float **k_caches;
    float **v_caches;
    int owns_kv;
    int *sample_ids;
    float *sample_probs;
    int sample_topk;
    fpq_worker_t *worker;
    fpq_run_metrics_t metrics;
};

enum {
    FPQ_FAMILY_PREPARE = 0,
    FPQ_FAMILY_QKV = 1,
    FPQ_FAMILY_ATTENTION = 2,
    FPQ_FAMILY_O_PROJ = 3,
    FPQ_FAMILY_NORM = 4,
    FPQ_FAMILY_MLP = 5,
    FPQ_FAMILY_KV_WRITE = 6,
    FPQ_FAMILY_LM_HEAD = 7,
    FPQ_FAMILY_RELEASE = 8,
};

static const char *fpq_family_name(int family) {
    switch (family) {
        case FPQ_FAMILY_PREPARE: return "prepare";
        case FPQ_FAMILY_QKV: return "qkv";
        case FPQ_FAMILY_ATTENTION: return "attention";
        case FPQ_FAMILY_O_PROJ: return "o_proj";
        case FPQ_FAMILY_NORM: return "norm";
        case FPQ_FAMILY_MLP: return "mlp";
        case FPQ_FAMILY_KV_WRITE: return "kv_write";
        case FPQ_FAMILY_LM_HEAD: return "lm_head";
        case FPQ_FAMILY_RELEASE: return "release";
        default: return "unknown";
    }
}

const char *fpq_run_metrics_family_name(int family) {
    return fpq_family_name(family);
}

static void fpq_metrics_add_family(fpq_run_metrics_t *m, int family, double seconds) {
    if (!m || family < 0 || family >= FPQ_RUN_MAX_FAMILIES || seconds <= 0.0) return;
    m->family_seconds[family] += seconds;
    m->family_calls[family] += 1u;
}

static void fpq_breakdown_emit_jsonl(const char *env_name,
                                     const char *kind,
                                     int layer,
                                     int step,
                                     const char *tensor_name,
                                     double value_a,
                                     double value_b,
                                     double value_c,
                                     const char *extra_json) {
    const char *path = getenv(env_name);
    FILE *fp;
    if (!path || !path[0] || !kind) return;
    if (bf_ensure_parent_dir(path) != 0) return;
    fp = fopen(path, "a");
    if (!fp) return;
    fprintf(fp, "{\"kind\":\"%s\",\"layer\":%d,\"step\":%d,\"tensor\":", kind, layer, step);
    if (tensor_name && tensor_name[0]) {
        fprintf(fp, "\"");
        for (const char *p = tensor_name; *p; p++) {
            if (*p == '"' || *p == '\\') fputc('\\', fp);
            fputc(*p, fp);
        }
        fprintf(fp, "\"");
    } else {
        fprintf(fp, "null");
    }
    fprintf(fp, ",\"value_a\":%.9f,\"value_b\":%.9f,\"value_c\":%.9f",
            value_a, value_b, value_c);
    if (extra_json && extra_json[0]) fprintf(fp, ",%s", extra_json);
    fprintf(fp, "}\n");
    fclose(fp);
}

static int fpq_run_debug_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_DEBUG");
    return v && v[0] && strcmp(v, "0") != 0;
}

static double fpq_vec_l2_norm(const float *x, size_t n) {
    double sumsq = 0.0;
    for (size_t i = 0; i < n; i++) {
        double v = (double)x[i];
        sumsq += v * v;
    }
    return sqrt(sumsq);
}

static int fpq_layer_probe_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_LAYER_PROBE");
    return v && v[0] && strcmp(v, "0") != 0;
}

static int fpq_layer_internal_probe_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_LAYER_INTERNAL_PROBE");
    return v && v[0] && strcmp(v, "0") != 0;
}

static int fpq_norm_probe_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_NORM_PROBE");
    return v && v[0] && strcmp(v, "0") != 0;
}

static int fpq_should_probe_layer_hidden(int lay) {
    return lay == 0 || lay == 1 || lay == 20 || lay == 40 ||
           lay == 41 || lay == 42 || lay == 43;
}



static int qwen_log_mem_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_LOG_MEM");
    if (!v || !*v || strcmp(v, "0") == 0) return 0;
    return 1;
}

static void qwen_log_mem_layer(const char *tag, int lay, int total_layers) {
#ifdef __APPLE__
    task_basic_info_data_t info;
    mach_msg_type_number_t count = TASK_BASIC_INFO_COUNT;
    kern_return_t kr = task_info(mach_task_self(),
                                 TASK_BASIC_INFO,
                                 (task_info_t)&info,
                                 &count);
    if (kr == KERN_SUCCESS) {
        fprintf(stderr,
                "qwen_mem %s layer=%d/%d resident_mb=%.1f virtual_mb=%.1f\n",
                tag,
                lay + 1,
                total_layers,
                (double)info.resident_size / 1048576.0,
                (double)info.virtual_size / 1048576.0);
        fflush(stderr);
        return;
    }
#endif
    fprintf(stderr,
            "qwen_mem %s layer=%d/%d resident_mb=-1 virtual_mb=-1\n",
            tag,
            lay + 1,
            total_layers);
    fflush(stderr);
}


int fpq_release_tensor(fpq_model_t *m, const char *tensor_name);
static void qwen_release_prefill_layer_tensors(fpq_model_t *model, int lay);
static const char *fpq_cfg_lm_head_tensor_name(const fpq_run_config_t *cfg);


static int qwen_inline_prefill_threads_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_INLINE_PREFILL_THREADS");
    return v && v[0] && strcmp(v, "0") != 0 &&
           strcasecmp(v, "false") != 0 &&
           strcasecmp(v, "off") != 0 &&
           strcasecmp(v, "no") != 0;
}

static int qwen_skip_prepare_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_SKIP_PREPARE");
    return v && v[0] && strcmp(v, "0") != 0 &&
           strcasecmp(v, "false") != 0 &&
           strcasecmp(v, "off") != 0 &&
           strcasecmp(v, "no") != 0;
}

static int qwen_prepare_tensor_guarded(fpq_model_t *model, const char *tensor_name) {
    if (qwen_skip_prepare_enabled()) {
        if (getenv("BONFYRE_QWEN_LOG_PREPARE_DETAIL") &&
            strcmp(getenv("BONFYRE_QWEN_LOG_PREPARE_DETAIL"), "0") != 0) {
            fprintf(stderr, "qwen_prepare_skip tensor=%s\n",
                    tensor_name ? tensor_name : "(null)");
            fflush(stderr);
        }
        return 0;
    }
    return fpq_prepare_tensor(model, tensor_name);
}



static void qwen_allocator_pressure_relief(void) {
#ifdef __APPLE__
    (void)malloc_zone_pressure_relief((void *)0, 0);
#endif
}

static int qwen_release_heartbeat_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_RELEASE_HEARTBEAT");
    if (!v || !*v) return 1;
    return strcmp(v, "0") != 0;
}

static int qwen_release_heartbeat_print_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_RELEASE_HEARTBEAT_PRINT");
    if (!v || !*v || strcmp(v, "0") == 0) return 0;
    return 1;
}

static long qwen_release_heartbeat_ns(void) {
    const char *v = getenv("BONFYRE_QWEN_RELEASE_HEARTBEAT_NS");
    if (v && *v) {
        long ns = atol(v);
        if (ns < 0) ns = 0;
        return ns;
    }
    return 5000000L;
}


static void qwen_release_heartbeat_touch_memory(void) {
#if defined(__APPLE__)
    /*
     * qwen_log_mem_layer() made silent survival better than nanosleep alone.
     * Preserve the OS-touch side effect without printing telemetry.
     */
    struct mach_task_basic_info info;
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    kern_return_t kr = task_info(mach_task_self(),
                                 MACH_TASK_BASIC_INFO,
                                 (task_info_t)&info,
                                 &count);
    (void)kr;
#else
    struct rusage ru;
    (void)getrusage(RUSAGE_SELF, &ru);
#endif
}


static int qwen_noop_release_enabled_stage4d(void) {
    const char *v = getenv("BONFYRE_QWEN_NOOP_RELEASE");
    return v && v[0] && v[0] != '0' && v[0] != 'f' && v[0] != 'F' && v[0] != 'n' && v[0] != 'N';
}

static void qwen_release_heartbeat_pause(const char *phase, int lay, int total_layers) {
    /* stage4d no-op qwen_release_heartbeat_pause */
    if (qwen_noop_release_enabled_stage4d()) return;
    if (!qwen_release_heartbeat_enabled()) return;

    long ns = qwen_release_heartbeat_ns();
    if (qwen_release_heartbeat_print_enabled()) {
        fprintf(stderr,
                "qwen_release_heartbeat phase=%s layer=%d/%d ns=%ld\n",
                phase ? phase : "unknown",
                lay + 1,
                total_layers,
                ns);
        fflush(stderr);
    }
    qwen_release_heartbeat_touch_memory();
    if (ns <= 0) return;

    struct timespec ts;
    ts.tv_sec = ns / 1000000000L;
    ts.tv_nsec = ns % 1000000000L;
    nanosleep(&ts, NULL);
}



static long qwen_release_stabilize_ns(void) {
    const char *v = getenv("BONFYRE_QWEN_RELEASE_STABILIZE_NS");
    if (v && *v) {
        long ns = atol(v);
        if (ns < 0) ns = 0;
        return ns;
    }

    /*
     * The native streaming runtime munmaps very large per-layer FPQ backing
     * regions. qwen_mem logging accidentally gave the OS enough scheduling time.
     * Keep that stabilizing pause even when telemetry is disabled.
     */
    return 1000000L; /* 1ms default */
}

static void qwen_release_stabilize(void) {
    long ns = qwen_release_stabilize_ns();
    if (ns <= 0) return;

    struct timespec ts;
    ts.tv_sec = ns / 1000000000L;
    ts.tv_nsec = ns % 1000000000L;
    nanosleep(&ts, NULL);
}

static void qwen_release_post_eviction_pause(const char *phase, int lay, int total_layers) {
    qwen_release_heartbeat_pause(phase, lay, total_layers);
    qwen_release_stabilize();
}

static void qwen_runtime_heartbeat_tick(const char *phase, int lay, int total_layers) {
    qwen_release_heartbeat_pause(phase, lay, total_layers);
}

static void qwen_release_log_mem_if_enabled(const char *tag, int lay, int total_layers) {
    if (qwen_log_mem_enabled()) {
        qwen_log_mem_layer(tag, lay, total_layers);
    }
}

static void qwen_release_layer_runtime(fpq_model_t *model,
                                       int lay,
                                       int total_layers,
                                       const char *phase) {
    /* stage4d no-op qwen_release_layer_runtime */
    if (qwen_noop_release_enabled_stage4d()) return;
    qwen_release_prefill_layer_tensors(model, lay);
    qwen_allocator_pressure_relief();
    qwen_release_post_eviction_pause(phase, lay, total_layers);
}

static int qwen_release_layer_after_decode_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_RELEASE_LAYER_AFTER_DECODE");
    if (v && *v && strcmp(v, "0") != 0) return 1;

    /*
     * Compatibility: if the caller enabled runtime release globally for this
     * experiment, use it for decode too unless explicitly disabled.
     */
    const char *deep = getenv("BONFYRE_QWEN_RELEASE_RUNTIME_AFTER_PREFILL");
    return deep && *deep && strcmp(deep, "0") != 0;
}

static int qwen_release_layer_after_prefill_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_RELEASE_LAYER_AFTER_PREFILL");
    if (!v || !*v || strcmp(v, "0") == 0) return 0;
    return 1;
}

static int qwen_keep_attn_hot_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_KEEP_ATTN_HOT");
    return v && v[0] && strcmp(v, "0") != 0;
}

static int qwen_keep_mlp_hot_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_KEEP_MLP_HOT");
    return v && v[0] && strcmp(v, "0") != 0;
}

static int qwen_should_release_layer_tensor(const char *tensor_name) {
    if (!tensor_name || !*tensor_name) return 1;
    if (qwen_keep_attn_hot_enabled() &&
        strstr(tensor_name, ".self_attn.") != NULL) {
        return 0;
    }
    if (qwen_keep_mlp_hot_enabled() &&
        strstr(tensor_name, ".mlp.") != NULL) {
        return 0;
    }
    return 1;
}

static void qwen_release_prefill_layer_tensors(fpq_model_t *model, int lay) {
    char name[256];
    int released = 0;

    snprintf(name, sizeof(name), "model.layers.%d.self_attn.q_proj.weight", lay);
    if (qwen_should_release_layer_tensor(name))
        released += (fpq_release_tensor(model, name) > 0);

    snprintf(name, sizeof(name), "model.layers.%d.self_attn.k_proj.weight", lay);
    if (qwen_should_release_layer_tensor(name))
        released += (fpq_release_tensor(model, name) > 0);

    snprintf(name, sizeof(name), "model.layers.%d.self_attn.v_proj.weight", lay);
    if (qwen_should_release_layer_tensor(name))
        released += (fpq_release_tensor(model, name) > 0);

    snprintf(name, sizeof(name), "model.layers.%d.self_attn.o_proj.weight", lay);
    if (qwen_should_release_layer_tensor(name))
        released += (fpq_release_tensor(model, name) > 0);

    snprintf(name, sizeof(name), "model.layers.%d.mlp.gate_proj.weight", lay);
    if (qwen_should_release_layer_tensor(name))
        released += (fpq_release_tensor(model, name) > 0);

    snprintf(name, sizeof(name), "model.layers.%d.mlp.up_proj.weight", lay);
    if (qwen_should_release_layer_tensor(name))
        released += (fpq_release_tensor(model, name) > 0);

    snprintf(name, sizeof(name), "model.layers.%d.mlp.down_proj.weight", lay);
    if (qwen_should_release_layer_tensor(name))
        released += (fpq_release_tensor(model, name) > 0);

    const char *dbg = getenv("BONFYRE_QWEN_LOG_RELEASE");
    if (dbg && *dbg && strcmp(dbg, "0") != 0) {
        fprintf(stderr,
                "qwen_release_prefill_layer_tensors released=%d layer=%d\n",
                released,
                lay + 1);
        fflush(stderr);
    }
}

static int qwen_stop_after_layer_for_debug(void) {
    const char *v = getenv("BONFYRE_QWEN_STOP_AFTER_LAYER");
    if (!v || !*v || strcmp(v, "0") == 0) return 0;
    return atoi(v);
}

static int qwen_log_layer_progress_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_LOG_LAYER_PROGRESS");
    return v && *v && strcmp(v, "0") != 0;
}

static int qwen_log_prefill_timing_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_LOG_PREFILL_TIMING");
    return v && *v && strcmp(v, "0") != 0;
}

static double fpq_elapsed_seconds(const struct timespec *t0, const struct timespec *t1) {
    return (double)(t1->tv_sec - t0->tv_sec) + (double)(t1->tv_nsec - t0->tv_nsec) / 1e9;
}

static int fpq_should_probe_layer_internal(int lay) {
    return lay == 0 || lay == 1;
}

static void fpq_log_layer_hidden_probe(const char *label, int layer, const float *x, size_t n) {
    size_t nan_n = 0, inf_n = 0;
    float min_v = FLT_MAX, max_v = -FLT_MAX;
    size_t top_abs_idx = 0;
    double top_abs = -1.0;
    double sumsq = 0.0;
    for (size_t i = 0; i < n; i++) {
        float v = x[i];
        double av = fabs((double)v);
        if (isnan(v)) {
            nan_n++;
            continue;
        }
        if (isinf(v)) {
            inf_n++;
            continue;
        }
        if (v < min_v) min_v = v;
        if (v > max_v) max_v = v;
        if (av > top_abs) {
            top_abs = av;
            top_abs_idx = i;
        }
        sumsq += (double)v * (double)v;
    }
    if (top_abs < 0.0) {
        min_v = 0.0f;
        max_v = 0.0f;
        top_abs = 0.0;
        top_abs_idx = 0;
    }
    if (layer >= 0) {
        fprintf(stderr,
                "layer_hidden_probe layer=%d l2=%g min=%g max=%g nan=%zu inf=%zu top_abs_idx=%zu top_abs=%g label=%s\n",
                layer, sqrt(sumsq), (double)min_v, (double)max_v, nan_n, inf_n, top_abs_idx, top_abs, label);
    } else {
        fprintf(stderr,
                "layer_hidden_probe layer=%s l2=%g min=%g max=%g nan=%zu inf=%zu top_abs_idx=%zu top_abs=%g\n",
                label, sqrt(sumsq), (double)min_v, (double)max_v, nan_n, inf_n, top_abs_idx, top_abs);
    }
}

static void fpq_log_layer_internal_probe(int layer, const char *point, const float *x, size_t n) {
    size_t nan_n = 0, inf_n = 0;
    float min_v = FLT_MAX, max_v = -FLT_MAX;
    size_t top_abs_idx = 0;
    double top_abs = -1.0;
    double sumsq = 0.0;
    double sum_abs = 0.0;
    for (size_t i = 0; i < n; i++) {
        float v = x[i];
        double av = fabs((double)v);
        if (isnan(v)) {
            nan_n++;
            continue;
        }
        if (isinf(v)) {
            inf_n++;
            continue;
        }
        if (v < min_v) min_v = v;
        if (v > max_v) max_v = v;
        if (av > top_abs) {
            top_abs = av;
            top_abs_idx = i;
        }
        sumsq += (double)v * (double)v;
        sum_abs += av;
    }
    if (top_abs < 0.0) {
        min_v = 0.0f;
        max_v = 0.0f;
        top_abs = 0.0;
        top_abs_idx = 0;
    }
    fprintf(stderr,
            "layer_internal_probe layer=%d point=%s l2=%g min=%g max=%g nan=%zu inf=%zu top_abs_idx=%zu top_abs=%g mean_abs=%g sum_abs=%g\n",
            layer,
            point,
            sqrt(sumsq),
            (double)min_v,
            (double)max_v,
            nan_n,
            inf_n,
            top_abs_idx,
            top_abs,
            n ? (sum_abs / (double)n) : 0.0,
            sum_abs);
}

static void fpq_find_top_abs_elem(const float *x, size_t n, size_t *idx_out, double *abs_out,
                                  float *min_out, float *max_out, double *l2_out) {
    size_t top_idx = 0;
    double top_abs = -1.0;
    double sumsq = 0.0;
    float min_v = FLT_MAX;
    float max_v = -FLT_MAX;
    for (size_t i = 0; i < n; i++) {
        float v = x[i];
        double av;
        if (!isfinite(v)) continue;
        av = fabs((double)v);
        if (av > top_abs) {
            top_abs = av;
            top_idx = i;
        }
        if (v < min_v) min_v = v;
        if (v > max_v) max_v = v;
        sumsq += (double)v * (double)v;
    }
    if (top_abs < 0.0) {
        top_abs = 0.0;
        top_idx = 0;
        min_v = 0.0f;
        max_v = 0.0f;
    }
    if (idx_out) *idx_out = top_idx;
    if (abs_out) *abs_out = top_abs;
    if (min_out) *min_out = min_v;
    if (max_out) *max_out = max_v;
    if (l2_out) *l2_out = sqrt(sumsq);
}

static void fpq_log_norm_probe(int layer, const char *point,
                               const float *input, const float *weight, const float *output,
                               int n, float eps, const int *elem_ids, int n_elem_ids) {
    double sumsq = 0.0;
    double weight_l2_sumsq = 0.0;
    double weight_sum_abs = 0.0;
    double mean_square;
    double inv_rms;
    size_t input_top_idx = 0, output_top_idx = 0;
    double input_top_abs = 0.0, output_top_abs = 0.0;
    float input_min = 0.0f, input_max = 0.0f;
    float output_min = 0.0f, output_max = 0.0f;
    float weight_min = FLT_MAX, weight_max = -FLT_MAX;
    for (int i = 0; i < n; i++) {
        double xd = (double)input[i];
        sumsq += xd * xd;
        if (isfinite(weight[i])) {
            double wd = (double)weight[i];
            if (weight[i] < weight_min) weight_min = weight[i];
            if (weight[i] > weight_max) weight_max = weight[i];
            weight_l2_sumsq += wd * wd;
            weight_sum_abs += fabs(wd);
        }
    }
    if (weight_min == FLT_MAX) {
        weight_min = 0.0f;
        weight_max = 0.0f;
    }
    mean_square = sumsq / (double)n;
    inv_rms = 1.0 / sqrt(mean_square + (double)eps);
    fpq_find_top_abs_elem(input, (size_t)n, &input_top_idx, &input_top_abs, &input_min, &input_max, NULL);
    fpq_find_top_abs_elem(output, (size_t)n, &output_top_idx, &output_top_abs, &output_min, &output_max, NULL);
    fprintf(stderr,
            "norm_probe layer=%d point=%s input_l2=%g input_min=%g input_max=%g input_top_abs_idx=%zu input_top_abs=%g sumsq=%g mean_square=%g eps=%g inv_rms=%g weight_l2=%g weight_min=%g weight_max=%g weight_mean_abs=%g output_l2=%g output_min=%g output_max=%g output_top_abs_idx=%zu output_top_abs=%g\n",
            layer, point,
            sqrt(sumsq), (double)input_min, (double)input_max, input_top_idx, input_top_abs,
            sumsq, mean_square, (double)eps, inv_rms,
            sqrt(weight_l2_sumsq), (double)weight_min, (double)weight_max,
            n ? (weight_sum_abs / (double)n) : 0.0,
            fpq_vec_l2_norm(output, (size_t)n), (double)output_min, (double)output_max,
            output_top_idx, output_top_abs);
    for (int i = 0; i < n_elem_ids; i++) {
        int idx = elem_ids[i];
        if (idx < 0 || idx >= n) continue;
        fprintf(stderr,
                "norm_elem layer=%d point=%s idx=%d input=%g inv_rms=%g weight=%g output=%g\n",
                layer, point, idx,
                (double)input[idx], inv_rms, (double)weight[idx], (double)output[idx]);
    }
}

static void fpq_log_norm_weight_compare(const char *point,
                                        const float *w0, const float *w1, int n) {
    double l2_0 = 0.0, l2_1 = 0.0;
    double max_abs_delta = 0.0, sum_abs_delta = 0.0;
    float min0 = FLT_MAX, max0 = -FLT_MAX;
    float min1 = FLT_MAX, max1 = -FLT_MAX;
    for (int i = 0; i < n; i++) {
        double d0 = (double)w0[i];
        double d1 = (double)w1[i];
        double dd = fabs(d0 - d1);
        if (w0[i] < min0) min0 = w0[i];
        if (w0[i] > max0) max0 = w0[i];
        if (w1[i] < min1) min1 = w1[i];
        if (w1[i] > max1) max1 = w1[i];
        l2_0 += d0 * d0;
        l2_1 += d1 * d1;
        if (dd > max_abs_delta) max_abs_delta = dd;
        sum_abs_delta += dd;
    }
    fprintf(stderr,
            "norm_weight_compare point=%s layer0_vs_layer1 layer0_l2=%g layer1_l2=%g layer0_min=%g layer1_min=%g layer0_max=%g layer1_max=%g max_abs_delta=%g mean_abs_delta=%g\n",
            point, sqrt(l2_0), sqrt(l2_1), (double)min0, (double)min1,
            (double)max0, (double)max1, max_abs_delta, n ? (sum_abs_delta / (double)n) : 0.0);
}

static int fpq_run_use_fused_backend(void) {
    const char *backend = getenv("BONFYRE_QWEN_BACKEND");
    return backend &&
           (strcmp(backend, "neon_fused") == 0 ||
            strcmp(backend, "cpu_neon_fused") == 0);
}

static int fpq_run_use_flashqla_prefill(void) {
    const char *backend = getenv("BONFYRE_QWEN_BACKEND");
    return backend && strcmp(backend, "flashqla_prefill") == 0;
}

static int fpq_run_use_bulk_prepare(void) {
    const char *v = getenv("BONFYRE_QWEN_BULK_PREPARE");
    return v && v[0] && strcmp(v, "0") != 0;
}


static int qwen_attention_bias_v2_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_ATTENTION_BIAS");
    /* Qwen2 attention projections carry Q/K/V bias. Keep the architectural
     * default enabled; BONFYRE_QWEN_ATTENTION_BIAS=0 is an explicit
     * diagnostic opt-out for comparing legacy behavior. */
    return !v || strcmp(v, "0") != 0;
}

static int qwen_disable_sli_fast_score_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_DISABLE_SLI_FAST_SCORE");
    return v && v[0] && strcmp(v, "0") != 0;
}

static int qwen_matmul_native_rows(fpq_model_t *model,
                                   const char *tensor_name,
                                   const float *x,
                                   float *y,
                                   size_t rows) {
    if (!model || !tensor_name || !x || !y) return -1;
    for (size_t row = 0; row < rows; row++) {
        if (fpq_matmul_row_native(model, tensor_name, row, x, &y[row]) != 0) {
            return -1;
        }
    }
    return 0;
}

static int qwen_matmul_qkv_guarded(fpq_model_t *model,
                                   const char *tensor_name,
                                   const float *x,
                                   float *y,
                                   size_t rows) {
    if (!qwen_disable_sli_fast_score_enabled()) {
        return fpq_matmul(model, tensor_name, x, y);
    }
    return qwen_matmul_native_rows(model, tensor_name, x, y, rows);
}

static int qwen_matmul_qkv_shared_guarded(fpq_model_t *model,
                                          const char *const *tensor_names,
                                          const float *x,
                                          float **outputs,
                                          const size_t *rows,
                                          size_t n_tensors) {
    if (!qwen_disable_sli_fast_score_enabled()) {
        return fpq_matmul_shared(model, n_tensors, tensor_names, x, outputs);
    }
    if (!tensor_names || !outputs || !rows) return -1;
    for (size_t i = 0; i < n_tensors; i++) {
        if (qwen_matmul_native_rows(model, tensor_names[i], x, outputs[i], rows[i]) != 0) {
            return -1;
        }
    }
    return 0;
}

static void qwen_add_bias_v2(
        fpq_model_t *model,
        const char *tensor_name,
        float *out,
        int n,
        int layer,
        const char *label) {
    if (!qwen_attention_bias_v2_enabled()) return;
    if (!model || !tensor_name || !out || n <= 0) return;

    float bias_buf[5120];
    if (n > 5120) {
        if (getenv("BONFYRE_QWEN_BIAS_PROBE")) {
            fprintf(stderr, "qwen_bias_skip layer=%d label=%s tensor=%s reason=n_gt_5120 n=%d\n",
                    layer, label ? label : "", tensor_name, n);
        }
        return;
    }

    int rc = fpq_decode_row(model, tensor_name, 0, bias_buf);
    if (getenv("BONFYRE_QWEN_BIAS_PROBE")) {
        fprintf(stderr, "qwen_bias_lookup layer=%d label=%s tensor=%s n=%d rc=%d\n",
                layer, label ? label : "", tensor_name, n, rc);
    }
    if (rc != 0) return;

    double sum_abs = 0.0;
    float min_v = FLT_MAX, max_v = -FLT_MAX;

    for (int i = 0; i < n; i++) {
        float b = bias_buf[i];
        out[i] += b;
        if (b < min_v) min_v = b;
        if (b > max_v) max_v = b;
        sum_abs += fabs((double)b);
    }

    if (getenv("BONFYRE_QWEN_BIAS_PROBE")) {
        fprintf(stderr,
                "qwen_bias_applied layer=%d label=%s tensor=%s n=%d min=%g max=%g mean_abs=%g sum_abs=%g\n",
                layer, label ? label : "", tensor_name, n,
                (double)min_v, (double)max_v,
                sum_abs / (double)n, sum_abs);
    }
}

/* ═══════════════════════════════════════════════════════
 * Default config (TinyLlama-1.1B-Chat-v1.0)
 * ═══════════════════════════════════════════════════════ */

fpq_run_config_t fpq_run_default_config(void) {
    fpq_run_config_t c = {0};

    /*
     * Qwen2.5-Coder-14B-Instruct config.
     * This binary is specifically bonfyre-qwen-fpq, not the generic
     * TinyLlama default from legacy fpq_run.c.
     */
    c.n_vocab        = 152064;
    c.d_model        = 5120;
    c.d_ffn          = 13824;
    c.n_layers       = 48;
    c.n_heads        = 40;
    c.n_kv_heads     = 8;
    c.rms_norm_eps   = 1e-6f;
    c.rope_theta     = 1000000.0f;
    c.max_seq_len    = 2048;
    c.head_dim       = c.d_model / c.n_heads; /* 128 */
    c.arch           = FPQ_RUN_ARCH_QWEN2;
    c.max_new_tokens = 512;
    c.temperature    = 0.2f;
    c.top_p          = 0.9f;
    c.greedy         = 0;
    c.prefill_chunk_size = 64;
    c.debug_decode_ctx = NULL;
    c.debug_id_to_str = NULL;
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

/* ═══════════════════════════════════════════════════════
 * Helpers: rmsnorm, rope, silu, softmax, sampling
 * ═══════════════════════════════════════════════════════ */




static int qwen_direct_norm_weights_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_DIRECT_NORM_WEIGHTS");
    return v && v[0] && strcmp(v, "0") != 0;
}

static void qwen_log_norm_direct_compare(
        int layer,
        const char *point,
        const char *tensor,
        const float *slice,
        const float *direct,
        int n) {
    double max_delta = 0.0;
    double sum_delta = 0.0;
    int max_idx = 0;
    double slice_l2 = 0.0, direct_l2 = 0.0;
    float slice_min = FLT_MAX, slice_max = -FLT_MAX;
    float direct_min = FLT_MAX, direct_max = -FLT_MAX;

    for (int i = 0; i < n; i++) {
        float a = slice[i];
        float b = direct[i];
        double d = fabs((double)a - (double)b);
        if (d > max_delta) {
            max_delta = d;
            max_idx = i;
        }
        sum_delta += d;
        slice_l2 += (double)a * (double)a;
        direct_l2 += (double)b * (double)b;
        if (a < slice_min) slice_min = a;
        if (a > slice_max) slice_max = a;
        if (b < direct_min) direct_min = b;
        if (b > direct_max) direct_max = b;
    }

    fprintf(stderr,
            "norm_direct_compare layer=%d point=%s tensor=%s slice_l2=%g direct_l2=%g slice_min=%g slice_max=%g direct_min=%g direct_max=%g max_delta=%g mean_delta=%g max_delta_idx=%d slice_at_idx=%g direct_at_idx=%g\n",
            layer, point, tensor,
            sqrt(slice_l2), sqrt(direct_l2),
            (double)slice_min, (double)slice_max,
            (double)direct_min, (double)direct_max,
            max_delta, sum_delta / (double)n, max_idx,
            (double)slice[max_idx], (double)direct[max_idx]);
}

static int qwen_repair_layer_in_csv(int lay, const char *csv) {
    if (!csv || !csv[0]) return 0;
    if (!csv || !*csv) return 0;
    const char *p = csv;
    while (*p) {
        while (*p == ' ' || *p == ',') p++;
        if (!*p) break;
        char *end = NULL;
        long a = strtol(p, &end, 10);
        long b = a;
        if (end && *end == '-') {
            char *end2 = NULL;
            b = strtol(end + 1, &end2, 10);
            end = end2;
        }
        if (a <= lay && lay <= b) return 1;
        p = end ? end : p + 1;
    }
    return 0;
}

static int qwen_repair_l1_mlp_norm_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_REPAIR_L1_MLP_NORM");
    /* Temporary diagnostic repair: default OFF. Set to 1 to enable. */
    return v && v[0] && strcmp(v, "0") != 0;
}

static float qwen_repair_l1_mlp_norm_cap(void) {
    const char *v = getenv("BONFYRE_QWEN_REPAIR_L1_MLP_NORM_CAP");
    if (v && *v) {
        float f = strtof(v, NULL);
        if (isfinite(f) && f > 0.05f && f < 10.0f) return f;
    }
    return 1.0f;
}

static const float *qwen_repair_norm_weight_if_needed(
        int layer,
        const char *point,
        const float *w,
        int n,
        float *scratch) {
    if (!w || !scratch || n <= 0) return w;
    if (!qwen_repair_l1_mlp_norm_enabled()) return w;
    if (layer != 1 || !point || strcmp(point, "mlp_norm") != 0) return w;

    float cap = qwen_repair_l1_mlp_norm_cap();
    float max_abs = 0.0f;
    int clipped = 0;

    for (int i = 0; i < n; i++) {
        float a = fabsf(w[i]);
        if (a > max_abs) max_abs = a;
    }

    if (max_abs <= cap) return w;

    for (int i = 0; i < n; i++) {
        float v = w[i];
        if (v > cap) {
            v = cap;
            clipped++;
        } else if (v < -cap) {
            v = -cap;
            clipped++;
        }
        scratch[i] = v;
    }

    fprintf(stderr,
            "norm_repair_applied layer=%d point=%s cap=%g old_max_abs=%g clipped=%d\n",
            layer, point, (double)cap, (double)max_abs, clipped);
    return scratch;
}

static void rms_norm(float *out, const float *x, const float *w,
                     int n, float eps, int log_debug) {
    if (fpq_run_use_fused_backend()) {
        fpq_rmsnorm_neon(out, x, w, (size_t)n, eps);
    } else {
    double sumsq = 0.0;
        for (int i = 0; i < n; i++) {
            double xd = (double)x[i];
            sumsq += xd * xd;
        }
        double mean_square = sumsq / (double)n;
        double denom_before_rsqrt = mean_square + (double)eps;
        float inv_rms = 1.0f / sqrtf((float)denom_before_rsqrt);
        for (int i = 0; i < n; i++) out[i] = w[i] * (inv_rms * x[i]);
    }

    if (log_debug) {
        double sumsq = 0.0;
        double mean_square;
        double denom_before_rsqrt;
        float inv_rms;
        int lim = n < 8 ? n : 8;
        for (int i = 0; i < n; i++) {
            double xd = (double)x[i];
            sumsq += xd * xd;
        }
        mean_square = sumsq / (double)n;
        denom_before_rsqrt = mean_square + (double)eps;
        inv_rms = 1.0f / sqrtf((float)denom_before_rsqrt);
        fprintf(stderr,
                "rms_norm_debug: name=final_norm sumsq=%g mean_square=%g eps=%g denom_before_rsqrt=%g inv_rms=%g\n",
                sumsq, mean_square, (double)eps, denom_before_rsqrt, (double)inv_rms);
        fprintf(stderr, "rms_norm_debug: name=final_norm x_first8=");
        for (int i = 0; i < lim; i++) fprintf(stderr, "%s%g", (i == 0) ? "" : ",", (double)x[i]);
        fprintf(stderr, "\n");
        fprintf(stderr, "rms_norm_debug: name=final_norm w_first8=");
        for (int i = 0; i < lim; i++) fprintf(stderr, "%s%g", (i == 0) ? "" : ",", (double)w[i]);
        fprintf(stderr, "\n");
        fprintf(stderr, "rms_norm_debug: name=final_norm out_first8=");
        for (int i = 0; i < lim; i++) fprintf(stderr, "%s%g", (i == 0) ? "" : ",", (double)out[i]);
        fprintf(stderr, "\n");
    }
}

static void silu_hadamard(float *gate, const float *up, int n) {
    if (fpq_run_use_fused_backend()) {
        fpq_silu_mul_neon(gate, up, (size_t)n);
        return;
    }
    /* gate[i] = silu(gate[i]) * up[i]  (replaces gate in-place).
     * Numerically stable sigmoid: branch to avoid expf overflow for large |g|. */
    for (int i = 0; i < n; i++) {
        float g = gate[i];
        float sig;
        if (g >= 0.0f) {
            /* expf(-g) is in (0,1], no overflow */
            sig = 1.0f / (1.0f + expf(-g));
        } else {
            /* expf(g) is in (0,1], avoids expf(-g) overflow for g<<0 */
            float e = expf(g);
            sig = e / (1.0f + e);
        }
        gate[i] = g * sig * up[i];
    }
}

/* Apply RoPE to query/key head in-place.
 * head: float[head_dim], pos: token position */
static void rope_apply(float *head, int head_dim, int pos, float theta) {
    if (fpq_run_use_fused_backend()) {
        fpq_rope_neon(head, (size_t)head_dim, pos, theta);
        return;
    }
    for (int i = 0; i < head_dim / 2; i++) {
        float freq = 1.0f / powf(theta, (float)(2 * i) / (float)head_dim);
        float angle = (float)pos * freq;
        float cos_a = cosf(angle);
        float sin_a = sinf(angle);
        float q0 = head[2 * i];
        float q1 = head[2 * i + 1];
        head[2 * i]     = q0 * cos_a - q1 * sin_a;
        head[2 * i + 1] = q0 * sin_a + q1 * cos_a;
    }
}


/* Qwen2/HF LLaMA-style RoPE uses split-half rotate:
 * first half rotates against second half, not adjacent pairs.
 */
static void rope_apply_split_half(float *head, int head_dim, int pos, float theta) {
    int half = head_dim / 2;
    for (int i = 0; i < half; i++) {
        float freq = 1.0f / powf(theta, (float)i / (float)half);
        float angle = (float)pos * freq;
        float cos_a = cosf(angle);
        float sin_a = sinf(angle);

        float x0 = head[i];
        float x1 = head[i + half];

        head[i]        = x0 * cos_a - x1 * sin_a;
        head[i + half] = x0 * sin_a + x1 * cos_a;
    }
}

static void rope_apply_for_arch(float *head, int head_dim, int pos, float theta, fpq_run_arch_t arch) {
    if (arch == FPQ_RUN_ARCH_QWEN2) {
        rope_apply_split_half(head, head_dim, pos, theta);
    } else {
        rope_apply(head, head_dim, pos, theta);
    }
}

/* Grouped Multi-Head Attention (GQA).
 * q:         [n_heads * head_dim]
 * k_cache:   [n_kv_heads][max_seq][head_dim]
 * v_cache:   [n_kv_heads][max_seq][head_dim]
 * attn_out:  [n_heads * head_dim] (output)
 * Returns scratch allocated by caller: att_scratch [max_seq] */
static void gqa_attention(
        const float *q,
        const float *k_cache,     /* [max_seq * n_kv_heads * head_dim] */
        const float *v_cache,     /* [max_seq * n_kv_heads * head_dim] */
        float *attn_out,
        float *att_scratch,       /* [n_heads * max_seq] scratch */
        int seq_len,
        int n_heads, int n_kv_heads, int head_dim, int max_seq_len,
        int active_kv_window,
        int lay, int token_pos) {

    int kv_group = n_heads / n_kv_heads; /* heads per KV head (GQA factor) */
    float scale = 1.0f / sqrtf((float)head_dim);
    int start_t = 0;
    if (active_kv_window > 0 && active_kv_window < (seq_len + 1)) {
        start_t = seq_len + 1 - active_kv_window;
    }

#ifdef _OPENMP
#pragma omp parallel for schedule(static)
#endif
    for (int h = 0; h < n_heads; h++) {
        const float *qh = q + h * head_dim;
        int kv_h = h / kv_group;  /* map query head → KV head */
        float *hscratch = att_scratch + (size_t)h * (size_t)max_seq_len;

        /* Compute attention scores */
        float max_score = -1e30f;
        for (int t = start_t; t <= seq_len; t++) {  /* seq_len inclusive (0-indexed) */
            const float *kh = k_cache + (t * n_kv_heads + kv_h) * head_dim;
            float score = 0.0f;
            for (int d = 0; d < head_dim; d++) score += qh[d] * kh[d];
            score *= scale;
            hscratch[t] = score;
            if (score > max_score) max_score = score;
        }

        /* Softmax — requires finite max_score; guard NaN/Inf sum */
        float *outh = attn_out + h * head_dim;
        memset(outh, 0, (size_t)head_dim * sizeof(float));
        if (!isfinite(max_score)) {
            fprintf(stderr,
                    "attn_softmax_fatal: layer=%d token_pos=%d head=%d max_score=%g"
                    " (non-finite, zeroing head output)\n",
                    lay, token_pos, h, (double)max_score);
            memset(outh, 0, (size_t)head_dim * sizeof(float));
            continue;
        }
        float sum = 0.0f;
        for (int t = start_t; t <= seq_len; t++) {
            hscratch[t] = expf(hscratch[t] - max_score);
            sum += hscratch[t];
        }
        if (!isfinite(sum) || sum < 1e-30f) {
            fprintf(stderr,
                    "attn_softmax_fatal: layer=%d token_pos=%d head=%d sum=%g"
                    " (non-finite/zero, clamping to 1)\n",
                    lay, token_pos, h, (double)sum);
            sum = 1.0f;
        }
        for (int t = start_t; t <= seq_len; t++) hscratch[t] /= sum;

        /* Weighted sum of values */
        for (int t = start_t; t <= seq_len; t++) {
            const float *vh = v_cache + (t * n_kv_heads + kv_h) * head_dim;
            for (int d = 0; d < head_dim; d++) outh[d] += hscratch[t] * vh[d];
        }
    }
}

static int sample_top_p(const float *probs, int vocab_size,
                        float top_p, uint64_t *rng,
                        fpq_run_state_t *state) {
    int topk = state && state->sample_topk > 0 ? state->sample_topk : FPQ_RUN_SAMPLE_TOPK_DEFAULT;
    int n_keep = 0;
    float norm = 0.0f;
    int result;

    if (topk > vocab_size) topk = vocab_size;
    if (topk < 1) topk = vocab_size;

    for (int i = 0; i < topk; i++) {
        state->sample_ids[i] = -1;
        state->sample_probs[i] = -1.0f;
    }

    for (int i = 0; i < vocab_size; i++) {
        float p = probs[i];
        int pos = -1;
        if (p <= state->sample_probs[topk - 1]) continue;
        for (int j = 0; j < topk; j++) {
            if (p > state->sample_probs[j]) {
                pos = j;
                break;
            }
        }
        if (pos < 0) continue;
        for (int j = topk - 1; j > pos; j--) {
            state->sample_probs[j] = state->sample_probs[j - 1];
            state->sample_ids[j] = state->sample_ids[j - 1];
        }
        state->sample_probs[pos] = p;
        state->sample_ids[pos] = i;
    }

    while (n_keep < topk && state->sample_ids[n_keep] >= 0 &&
           (top_p >= 1.0f || norm < top_p)) {
        norm += state->sample_probs[n_keep];
        n_keep++;
    }
    if (n_keep == 0) n_keep = 1;
    if (norm <= 0.0f) norm = state->sample_probs[0];

    *rng ^= *rng << 13; *rng ^= *rng >> 7; *rng ^= *rng << 17;
    float r = (float)(*rng & 0xFFFFFF) / (float)0x1000000 * norm;
    float c = 0.0f;
    result = state->sample_ids[0];
    for (int i = 0; i < n_keep; i++) {
        c += state->sample_probs[i];
        if (r < c) {
            result = state->sample_ids[i];
            break;
        }
    }
    return result;
}

static void fpq_log_topk_logits(const float *logits, int vocab_size,
                                int k, const fpq_run_config_t *cfg,
                                int step_idx) {
    typedef struct { float v; int id; } top_t;
    top_t top[20];
    if (k > 20) k = 20;
    for (int i = 0; i < k; i++) {
        top[i].v = -INFINITY;
        top[i].id = -1;
    }
    for (int i = 0; i < vocab_size; i++) {
        float v = logits[i];
        int pos = -1;
        for (int j = 0; j < k; j++) {
            if (v > top[j].v) { pos = j; break; }
        }
        if (pos < 0) continue;
        for (int j = k - 1; j > pos; j--) top[j] = top[j - 1];
        top[pos].v = v;
        top[pos].id = i;
    }

    fprintf(stderr, "logit_trace: step=%d topk=%d\n", step_idx, k);
    for (int i = 0; i < k; i++) {
        if (top[i].id < 0) continue;
        const char *decoded = "";
        if (cfg->debug_id_to_str) {
            decoded = cfg->debug_id_to_str(cfg->debug_decode_ctx, top[i].id);
            if (!decoded) decoded = "";
        }
        fprintf(stderr, "logit_trace: step=%d rank=%d token_id=%d logit=%g decoded=\"%s\"\n",
                step_idx, i + 1, top[i].id, (double)top[i].v, decoded);
    }
}

static int fpq_collect_topk_ids(const float *logits, int vocab_size, int k, int *ids_out) {
    typedef struct { float v; int id; } top_t;
    top_t top[20];
    if (k > 20) k = 20;
    for (int i = 0; i < k; i++) {
        top[i].v = -INFINITY;
        top[i].id = -1;
    }
    for (int i = 0; i < vocab_size; i++) {
        float v = logits[i];
        int pos = -1;
        for (int j = 0; j < k; j++) {
            if (v > top[j].v) { pos = j; break; }
        }
        if (pos < 0) continue;
        for (int j = k - 1; j > pos; j--) top[j] = top[j - 1];
        top[pos].v = v;
        top[pos].id = i;
    }
    for (int i = 0; i < k; i++) ids_out[i] = top[i].id;
    return k;
}

static int fpq_lm_head_row_probe_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_LM_HEAD_ROW_PROBE");
    return v && v[0] && strcmp(v, "0") != 0;
}

static int fpq_mlp_row_probe_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_MLP_ROW_PROBE");
    return v && v[0] && strcmp(v, "0") != 0;
}

static int fpq_mlp_activation_probe_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_MLP_ACTIVATION_PROBE");
    return v && v[0] && strcmp(v, "0") != 0;
}

static int fpq_collect_top_abs_ids(const float *x, int n, int k, int *ids_out) {
    typedef struct { double av; int id; } top_t;
    top_t top[10];
    if (k > 10) k = 10;
    for (int i = 0; i < k; i++) {
        top[i].av = -1.0;
        top[i].id = -1;
    }
    for (int i = 0; i < n; i++) {
        double av = fabs((double)x[i]);
        int pos = -1;
        for (int j = 0; j < k; j++) {
            if (av > top[j].av) { pos = j; break; }
        }
        if (pos < 0) continue;
        for (int j = k - 1; j > pos; j--) top[j] = top[j - 1];
        top[pos].av = av;
        top[pos].id = i;
    }
    for (int i = 0; i < k; i++) ids_out[i] = top[i].id;
    return k;
}

static void fpq_log_mlp_activation_probe(int layer, const float *x, size_t n) {
    size_t finite = 0;
    size_t top_abs_idx = 0;
    double top_abs = -1.0;
    double sumsq = 0.0;
    double sum_abs = 0.0;
    float min_v = FLT_MAX;
    float max_v = -FLT_MAX;
    for (size_t i = 0; i < n; i++) {
        float v = x[i];
        if (!isfinite(v)) continue;
        double av = fabs((double)v);
        if (av > top_abs) {
            top_abs = av;
            top_abs_idx = i;
        }
        if (v < min_v) min_v = v;
        if (v > max_v) max_v = v;
        sumsq += (double)v * (double)v;
        sum_abs += av;
        finite++;
    }
    if (top_abs < 0.0) {
        top_abs = 0.0;
        top_abs_idx = 0;
        min_v = 0.0f;
        max_v = 0.0f;
    }
    fprintf(stderr,
            "mlp_activation_probe layer=%d l2=%g min=%g max=%g top_abs_idx=%zu top_abs=%g mean_abs=%g sum_abs=%g\n",
            layer,
            sqrt(sumsq),
            (double)min_v,
            (double)max_v,
            top_abs_idx,
            top_abs,
            finite ? (sum_abs / (double)finite) : 0.0,
            sum_abs);
}

static void fpq_append_unique_ids(const int *src, int n_src, int *dst, int *n_dst, int max_dst) {
    for (int i = 0; i < n_src; i++) {
        int id = src[i];
        int seen = 0;
        if (id < 0 || *n_dst >= max_dst) continue;
        for (int j = 0; j < *n_dst; j++) {
            if (dst[j] == id) {
                seen = 1;
                break;
            }
        }
        if (!seen) dst[(*n_dst)++] = id;
    }
}

static float fpq_silu_scalar(float g) {
    if (g >= 0.0f) {
        return g * (1.0f / (1.0f + expf(-g)));
    }
    {
        float e = expf(g);
        return g * (e / (1.0f + e));
    }
}

static void fpq_mlp_activation_value_probe(fpq_model_t *model, int layer,
                                           const float *h_norm, size_t h_norm_len,
                                           const float *gate_raw, const float *up_raw,
                                           const float *mlp_activation, size_t activation_len) {
    int gate_top[10];
    int up_top[10];
    int act_top[10];
    int probe_ids[33];
    int n_probe = 0;
    static const int fixed_ids[] = {9675, 549, 9971};
    if (!fpq_mlp_activation_probe_enabled()) return;
    fpq_collect_top_abs_ids(gate_raw, (int)activation_len, 10, gate_top);
    fpq_collect_top_abs_ids(up_raw, (int)activation_len, 10, up_top);
    fpq_collect_top_abs_ids(mlp_activation, (int)activation_len, 10, act_top);
    fpq_append_unique_ids(gate_top, 10, probe_ids, &n_probe, 33);
    fpq_append_unique_ids(up_top, 10, probe_ids, &n_probe, 33);
    fpq_append_unique_ids(act_top, 10, probe_ids, &n_probe, 33);
    fpq_append_unique_ids(fixed_ids, (int)(sizeof(fixed_ids) / sizeof(fixed_ids[0])), probe_ids, &n_probe, 33);
    for (int i = 0; i < n_probe; i++) {
        int idx = probe_ids[i];
        if (idx < 0 || (size_t)idx >= activation_len) continue;
        float gate = gate_raw[idx];
        float silu_gate = fpq_silu_scalar(gate);
        float up = up_raw[idx];
        float product = mlp_activation[idx];
        fprintf(stderr,
                "mlp_activation_elem layer=%d idx=%d gate=%g silu_gate=%g up=%g product=%g\n",
                layer, idx, (double)gate, (double)silu_gate, (double)up, (double)product);
    }
    (void)fpq_debug_tensor_rows(model,
                                "model.layers.1.mlp.gate_proj.weight",
                                probe_ids,
                                (size_t)n_probe,
                                h_norm,
                                h_norm_len,
                                gate_raw,
                                activation_len,
                                "mlp_gate",
                                layer);
    (void)fpq_debug_tensor_rows(model,
                                "model.layers.1.mlp.up_proj.weight",
                                probe_ids,
                                (size_t)n_probe,
                                h_norm,
                                h_norm_len,
                                up_raw,
                                activation_len,
                                "mlp_up",
                                layer);
}

static void fpq_mlp_down_row_probe(fpq_model_t *model, int layer,
                                   const float *mlp_activation, size_t activation_len,
                                   const float *down_proj_out, size_t down_proj_len) {
    int top_rows[10];
    int probe_rows[12];
    int n_probe = 0;
    static const int fixed_rows[] = {892, 654};
    if (!fpq_mlp_row_probe_enabled()) return;
    fpq_log_mlp_activation_probe(layer, mlp_activation, activation_len);
    fpq_collect_top_abs_ids(down_proj_out, (int)down_proj_len, 10, top_rows);
    for (int i = 0; i < 10; i++) {
        if (top_rows[i] < 0) continue;
        int seen = 0;
        for (int j = 0; j < n_probe; j++) if (probe_rows[j] == top_rows[i]) { seen = 1; break; }
        if (!seen) probe_rows[n_probe++] = top_rows[i];
    }
    for (size_t i = 0; i < sizeof(fixed_rows) / sizeof(fixed_rows[0]); i++) {
        int id = fixed_rows[i];
        if (id < 0 || (size_t)id >= down_proj_len) continue;
        int seen = 0;
        for (int j = 0; j < n_probe; j++) if (probe_rows[j] == id) { seen = 1; break; }
        if (!seen) probe_rows[n_probe++] = id;
    }
    (void)fpq_debug_tensor_rows(model,
                                "model.layers.1.mlp.down_proj.weight",
                                probe_rows,
                                (size_t)n_probe,
                                mlp_activation,
                                activation_len,
                                down_proj_out,
                                down_proj_len,
                                "mlp_down",
                                layer);
}

static void fpq_lm_head_row_probe(fpq_model_t *model,
                                  const float *h_norm,
                                  const float *active_logits,
                                  int vocab_size,
                                  const fpq_run_config_t *cfg,
                                  int step_idx) {
    int top_ids[20];
    static const int fixed_ids[] = {13, 198, 220, 264, 624, 13104, 78300};
    int probe_ids[27];
    int n_probe = 0;
    if (!fpq_lm_head_row_probe_enabled()) return;
    const char *lm_head_name = fpq_cfg_lm_head_tensor_name(cfg);
    fpq_collect_topk_ids(active_logits, vocab_size, 20, top_ids);
    for (int i = 0; i < 20; i++) {
        if (top_ids[i] < 0) continue;
        int seen = 0;
        for (int j = 0; j < n_probe; j++) if (probe_ids[j] == top_ids[i]) { seen = 1; break; }
        if (!seen) probe_ids[n_probe++] = top_ids[i];
    }
    for (size_t i = 0; i < sizeof(fixed_ids) / sizeof(fixed_ids[0]); i++) {
        int id = fixed_ids[i];
        if (id < 0 || id >= vocab_size) continue;
        int seen = 0;
        for (int j = 0; j < n_probe; j++) if (probe_ids[j] == id) { seen = 1; break; }
        if (!seen) probe_ids[n_probe++] = id;
    }
    for (int i = 0; i < n_probe; i++) {
        float native_logit = 0.0f;
        if (fpq_matmul_row_native(model, lm_head_name, (size_t)probe_ids[i], h_norm, &native_logit) != 0) {
            fprintf(stderr, "lm_head_row_probe: step=%d token_id=%d native_error=1 active=%g\n",
                    step_idx, probe_ids[i], (double)active_logits[probe_ids[i]]);
            continue;
        }
        fprintf(stderr, "lm_head_row_probe: step=%d token_id=%d active=%g native=%g delta=%g\n",
                step_idx, probe_ids[i],
                (double)active_logits[probe_ids[i]],
                (double)native_logit,
                (double)(active_logits[probe_ids[i]] - native_logit));
    }
    (void)fpq_debug_lm_head_rows(model, lm_head_name, probe_ids, (size_t)n_probe,
                                 h_norm, (size_t)cfg->d_model,
                                 active_logits, (size_t)vocab_size);
}

static int fpq_logits_collapsed(const float *logits, int vocab_size,
                                float *out_min, float *out_max,
                                int *out_non_finite) {
    float min_v = FLT_MAX;
    float max_v = -FLT_MAX;
    int non_finite = 0;
    int finite_count = 0;
    for (int i = 0; i < vocab_size; i++) {
        float v = logits[i];
        if (!isfinite(v)) {
            non_finite++;
            continue;
        }
        finite_count++;
        if (v < min_v) min_v = v;
        if (v > max_v) max_v = v;
    }
    if (out_min) *out_min = (finite_count > 0) ? min_v : 0.0f;
    if (out_max) *out_max = (finite_count > 0) ? max_v : 0.0f;
    if (out_non_finite) *out_non_finite = non_finite;
    if (finite_count == 0) return 1;
    if (non_finite > 0) return 1;
    if (fabsf(max_v - min_v) < 1e-8f) return 1;
    return 0;
}

static int fpq_bootstrap_zero_matmul_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_BOOTSTRAP_ZERO_MATMUL");
    return v && v[0] && strcmp(v, "0") != 0;
}

static const char *fpq_cfg_embed_tensor_name(const fpq_run_config_t *cfg) {
    return (cfg && cfg->embed_tensor_name[0]) ? cfg->embed_tensor_name : "model.embed_tokens.weight";
}

static const char *fpq_cfg_final_norm_tensor_name(const fpq_run_config_t *cfg) {
    return (cfg && cfg->final_norm_tensor_name[0]) ? cfg->final_norm_tensor_name : "model.norm.weight";
}

static const char *fpq_cfg_lm_head_tensor_name(const fpq_run_config_t *cfg) {
    return (cfg && cfg->lm_head_tensor_name[0]) ? cfg->lm_head_tensor_name : "lm_head.weight";
}

static int fpq_cfg_primary_stop_token(const fpq_run_config_t *cfg) {
    if (cfg && cfg->n_stop_token_ids > 0) return cfg->stop_token_ids[0];
    return 2;
}

static int fpq_cfg_is_stop_token(const fpq_run_config_t *cfg, int token_id) {
    if (cfg && cfg->n_stop_token_ids > 0) {
        for (int i = 0; i < cfg->n_stop_token_ids; i++) {
            if (cfg->stop_token_ids[i] == token_id) return 1;
        }
        return 0;
    }
    return token_id == 2;
}

/* Softmax in-place */
static void softmax(float *x, int n) {
    float max = x[0];
    for (int i = 1; i < n; i++) if (x[i] > max) max = x[i];
    float sum = 0.0f;
    for (int i = 0; i < n; i++) { x[i] = expf(x[i] - max); sum += x[i]; }
    for (int i = 0; i < n; i++) x[i] /= sum;
}

/* ═══════════════════════════════════════════════════════
 * Tensor name helpers
 * ═══════════════════════════════════════════════════════ */

#define FPQ_RUN_NAME_BUF 128

static void tname(char *buf, fpq_run_arch_t arch, const char *suffix, int layer) {
    if (layer < 0) {
        snprintf(buf, FPQ_RUN_NAME_BUF, "%s", suffix);
        return;
    }
    switch (arch) {
        case FPQ_RUN_ARCH_LLAMA:
        case FPQ_RUN_ARCH_MISTRAL:
            snprintf(buf, FPQ_RUN_NAME_BUF, "model.layers.%d.%s", layer, suffix);
            break;
        case FPQ_RUN_ARCH_QWEN2:
            snprintf(buf, FPQ_RUN_NAME_BUF, "model.layers.%d.%s", layer, suffix);
            break;
    }
}

/* ═══════════════════════════════════════════════════════
 * Parallel matmul helper — persistent worker thread
 * Uses macOS dispatch_semaphore (no sem_init deprecation).
 * ═══════════════════════════════════════════════════════ */
#ifdef __APPLE__
#include <dispatch/dispatch.h>
typedef dispatch_semaphore_t fpq_sem_t;
#define fpq_sem_init(s, v)  (*(s) = dispatch_semaphore_create(v))
#define fpq_sem_post(s)     dispatch_semaphore_signal(*(s))
#define fpq_sem_wait(s)     dispatch_semaphore_wait(*(s), DISPATCH_TIME_FOREVER)
#define fpq_sem_destroy(s)  dispatch_release(*(s))
#else
#include <semaphore.h>

typedef sem_t fpq_sem_t;
#define fpq_sem_init(s, v)  sem_init(s, 0, v)
#define fpq_sem_post(s)     sem_post(s)
#define fpq_sem_wait(s)     sem_wait(s)
#define fpq_sem_destroy(s)  sem_destroy(s)
#endif

typedef struct {
    fpq_model_t *model;
    const char *const *names;
    int *results;
} fpq_bulk_prepare_dispatch_t;

static void fpq_bulk_prepare_dispatch(void *context, size_t idx) {
    fpq_bulk_prepare_dispatch_t *d = (fpq_bulk_prepare_dispatch_t *)context;
    d->results[idx] = qwen_prepare_tensor_guarded(d->model, d->names[idx]);
}

static int fpq_run_bulk_prepare_model(fpq_model_t *model, const fpq_run_config_t *cfg) {
    if (!model || !cfg) return -1;
    const int n_layers = cfg->n_layers;
    const fpq_run_arch_t arch = cfg->arch;
    const int names_per_layer = 7;
    const size_t total = (size_t)n_layers * (size_t)names_per_layer;
    char (*names)[FPQ_RUN_NAME_BUF] = NULL;
    const char **name_ptrs = NULL;
    int *results = NULL;
    char *old_prepare_env = NULL;
    int rc = 0;

    names = (char (*)[FPQ_RUN_NAME_BUF])calloc(total, sizeof(*names));
    name_ptrs = (const char **)calloc(total, sizeof(*name_ptrs));
    results = (int *)calloc(total, sizeof(*results));
    if (!names || !name_ptrs || !results) {
        free(names);
        free(name_ptrs);
        free(results);
        return -1;
    }

    for (int lay = 0; lay < n_layers; lay++) {
        size_t base = (size_t)lay * (size_t)names_per_layer;
        tname(names[base + 0], arch, "self_attn.q_proj.weight", lay);
        tname(names[base + 1], arch, "self_attn.k_proj.weight", lay);
        tname(names[base + 2], arch, "self_attn.v_proj.weight", lay);
        tname(names[base + 3], arch, "self_attn.o_proj.weight", lay);
        tname(names[base + 4], arch, "mlp.gate_proj.weight", lay);
        tname(names[base + 5], arch, "mlp.up_proj.weight", lay);
        tname(names[base + 6], arch, "mlp.down_proj.weight", lay);
        for (int j = 0; j < names_per_layer; j++) name_ptrs[base + (size_t)j] = names[base + (size_t)j];
    }

    {
        const char *prev = getenv("BONFYRE_QWEN_PREPARE_THREADS");
        if (prev && *prev) old_prepare_env = strdup(prev);
        setenv("BONFYRE_QWEN_PREPARE_THREADS", "1", 1);
    }

#ifdef __APPLE__
    {
        fpq_bulk_prepare_dispatch_t d = {
            .model = model,
            .names = name_ptrs,
            .results = results,
        };
        dispatch_apply_f(total,
                         dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0),
                         &d,
                         fpq_bulk_prepare_dispatch);
    }
#else
    for (size_t i = 0; i < total; i++) results[i] = qwen_prepare_tensor_guarded(model, name_ptrs[i]);
#endif

    if (old_prepare_env) {
        setenv("BONFYRE_QWEN_PREPARE_THREADS", old_prepare_env, 1);
    } else {
        unsetenv("BONFYRE_QWEN_PREPARE_THREADS");
    }

    for (size_t i = 0; i < total; i++) {
        if (results[i] != 0) {
            rc = -1;
            break;
        }
    }
    free(old_prepare_env);
    free(names);
    free(name_ptrs);
    free(results);
    return rc;
}

struct fpq_worker {
    fpq_model_t  *model;
    const char   *tensor_name;
    const char   *tensor_names[2];
    const float  *x;
    float        *y;
    float        *ys[2];
    size_t        n_tensors;
    int           shared_mode;
    int           rc;
    fpq_sem_t     work_sem;
    fpq_sem_t     done_sem;
    int           stop;
    int           thread_started;
    pthread_t     thread;
};

typedef struct {
    fpq_model_t *model;
    fpq_run_arch_t arch;
    int lay;
    int token_pos;
    int hd;
    int n_h;
    int n_kv;
    float theta;
    const float *h_norm;
    float *q_buf;
    float *k_buf;
    float *v_buf;
    int rc;
} fpq_prefill_qkv_task_t;

typedef struct {
    fpq_model_t *model;
    fpq_run_arch_t arch;
    int lay;
    int d;
    const float *attn_in;
    float *o_buf;
    int rc;
} fpq_prefill_o_task_t;

typedef struct {
    fpq_model_t *model;
    fpq_run_arch_t arch;
    int lay;
    int d;
    int d_ffn;
    const float *h_norm;
    float *gate_buf;
    float *up_buf;
    float *ffn_out;
    int rc;
} fpq_prefill_mlp_task_t;

static void *fpq_worker_fn(void *arg) {
    fpq_worker_t *w = (fpq_worker_t *)arg;
    for (;;) {
        fpq_sem_wait(&w->work_sem);
        if (w->stop) break;
        if (w->shared_mode) {
            const char *names[2] = { w->tensor_names[0], w->tensor_names[1] };
            float *outs[2] = { w->ys[0], w->ys[1] };
            w->rc = fpq_matmul_shared(w->model, w->n_tensors, names, w->x, outs);
        } else {
            w->rc = fpq_matmul(w->model, w->tensor_name, w->x, w->y);
        }
        fpq_sem_post(&w->done_sem);
    }
    return NULL;
}

static fpq_worker_t *fpq_worker_start(void) {
    fpq_worker_t *w = (fpq_worker_t *)calloc(1, sizeof(*w));
    if (!w) return NULL;
    fpq_sem_init(&w->work_sem, 0);
    fpq_sem_init(&w->done_sem, 0);
    w->stop = 0;
    if (pthread_create(&w->thread, NULL, fpq_worker_fn, w) == 0) {
        w->thread_started = 1;
    } else {
        fpq_sem_destroy(&w->work_sem);
        fpq_sem_destroy(&w->done_sem);
        free(w);
        return NULL;
    }
    return w;
}

static inline void fpq_worker_submit(fpq_worker_t *w,
    fpq_model_t *m, const char *name, const float *x, float *y) {
    w->model = m; w->tensor_name = name; w->x = x; w->y = y; w->rc = -1;
    w->shared_mode = 0; w->n_tensors = 0;
    fpq_sem_post(&w->work_sem);
}

static inline void fpq_worker_submit_shared(fpq_worker_t *w,
    fpq_model_t *m, const char *name0, const char *name1, const float *x, float *y0, float *y1) {
    w->model = m; w->tensor_names[0] = name0; w->tensor_names[1] = name1;
    w->x = x; w->ys[0] = y0; w->ys[1] = y1; w->rc = -1;
    w->shared_mode = 1; w->n_tensors = 2;
    fpq_sem_post(&w->work_sem);
}

static inline int fpq_worker_wait(fpq_worker_t *w) {
    fpq_sem_wait(&w->done_sem);
    return w->rc;
}

static void fpq_worker_stop(fpq_worker_t *w) {
    if (!w) return;
    w->stop = 1;
    fpq_sem_post(&w->work_sem);
    if (w->thread_started) {
        pthread_join(w->thread, NULL);
    }
    fpq_sem_destroy(&w->work_sem);
    fpq_sem_destroy(&w->done_sem);
    free(w);
}

static void *fpq_prefill_qkv_thread_fn(void *arg) {
    fpq_prefill_qkv_task_t *t = (fpq_prefill_qkv_task_t *)arg;
    char q_name[FPQ_RUN_NAME_BUF];
    char k_name[FPQ_RUN_NAME_BUF];
    char v_name[FPQ_RUN_NAME_BUF];
    const char *kv_names[2];
    float *kv_outs[2];
    t->rc = 0;
    tname(q_name, t->arch, "self_attn.q_proj.weight", t->lay);
    tname(k_name, t->arch, "self_attn.k_proj.weight", t->lay);
    tname(v_name, t->arch, "self_attn.v_proj.weight", t->lay);
    kv_names[0] = k_name;
    kv_names[1] = v_name;
    kv_outs[0] = t->k_buf;
    kv_outs[1] = t->v_buf;
    if (fpq_matmul(t->model, q_name, t->h_norm, t->q_buf) != 0 ||
        fpq_matmul_shared(t->model, 2, kv_names, t->h_norm, kv_outs) != 0) {
        t->rc = -1;
        return NULL;
    }
    tname(q_name, t->arch, "self_attn.q_proj.bias", t->lay);
    qwen_add_bias_v2(t->model, q_name, t->q_buf, t->n_h * t->hd, t->lay, "q_proj");
    tname(q_name, t->arch, "self_attn.k_proj.bias", t->lay);
    qwen_add_bias_v2(t->model, q_name, t->k_buf, t->n_kv * t->hd, t->lay, "k_proj");
    tname(q_name, t->arch, "self_attn.v_proj.bias", t->lay);
    qwen_add_bias_v2(t->model, q_name, t->v_buf, t->n_kv * t->hd, t->lay, "v_proj");
    for (int hh = 0; hh < t->n_h; hh++) {
        rope_apply_for_arch(t->q_buf + hh * t->hd, t->hd, t->token_pos, t->theta, t->arch);
    }
    for (int hh = 0; hh < t->n_kv; hh++) {
        rope_apply_for_arch(t->k_buf + hh * t->hd, t->hd, t->token_pos, t->theta, t->arch);
    }
    return NULL;
}

static void *fpq_prefill_o_thread_fn(void *arg) {
    fpq_prefill_o_task_t *t = (fpq_prefill_o_task_t *)arg;
    char name[FPQ_RUN_NAME_BUF];
    t->rc = 0;
    tname(name, t->arch, "self_attn.o_proj.weight", t->lay);
    if (fpq_matmul(t->model, name, t->attn_in, t->o_buf) != 0) {
        t->rc = -1;
        return NULL;
    }
    tname(name, t->arch, "self_attn.o_proj.bias", t->lay);
    qwen_add_bias_v2(t->model, name, t->o_buf, t->d, t->lay, "o_proj");
    return NULL;
}

static void *fpq_prefill_mlp_thread_fn(void *arg) {
    fpq_prefill_mlp_task_t *t = (fpq_prefill_mlp_task_t *)arg;
    char gate_name[FPQ_RUN_NAME_BUF];
    char up_name[FPQ_RUN_NAME_BUF];
    char down_name[FPQ_RUN_NAME_BUF];
    const char *mlp_names[2];
    float *mlp_outs[2];
    t->rc = 0;
    tname(gate_name, t->arch, "mlp.gate_proj.weight", t->lay);
    tname(up_name, t->arch, "mlp.up_proj.weight", t->lay);
    mlp_names[0] = gate_name;
    mlp_names[1] = up_name;
    mlp_outs[0] = t->gate_buf;
    mlp_outs[1] = t->up_buf;
    if (fpq_matmul_shared(t->model, 2, mlp_names, t->h_norm, mlp_outs) != 0) {
        t->rc = -1;
        return NULL;
    }
    silu_hadamard(t->gate_buf, t->up_buf, t->d_ffn);
    tname(down_name, t->arch, "mlp.down_proj.weight", t->lay);
    if (fpq_matmul(t->model, down_name, t->gate_buf, t->ffn_out) != 0) {
        t->rc = -1;
        return NULL;
    }
    return NULL;
}

fpq_run_state_t *fpq_run_state_create(const fpq_run_config_t *cfg,
                                      float **k_cache,
                                      float **v_cache) {
    fpq_run_state_t *state;
    size_t kv_size;
    const char *topk_env;

    if (!cfg) return NULL;
    state = (fpq_run_state_t *)calloc(1, sizeof(*state));
    if (!state) return NULL;
    state->cfg = *cfg;

    state->h = (float *)calloc((size_t)cfg->d_model, sizeof(float));
    state->h_norm = (float *)calloc((size_t)cfg->d_model, sizeof(float));
    state->q_buf = (float *)calloc((size_t)(cfg->n_heads * cfg->head_dim), sizeof(float));
    state->k_buf = (float *)calloc((size_t)(cfg->n_kv_heads * cfg->head_dim), sizeof(float));
    state->v_buf = (float *)calloc((size_t)(cfg->n_kv_heads * cfg->head_dim), sizeof(float));
    state->attn_out = (float *)calloc((size_t)(cfg->n_heads * cfg->head_dim), sizeof(float));
    state->o_buf = (float *)calloc((size_t)cfg->d_model, sizeof(float));
    state->gate_buf = (float *)calloc((size_t)cfg->d_ffn, sizeof(float));
    state->up_buf = (float *)calloc((size_t)cfg->d_ffn, sizeof(float));
    state->ffn_out = (float *)calloc((size_t)cfg->d_model, sizeof(float));
    state->logits = (float *)calloc((size_t)cfg->n_vocab, sizeof(float));
    state->att_scratch = (float *)calloc((size_t)cfg->n_heads * (size_t)cfg->max_seq_len, sizeof(float));

    topk_env = getenv("BONFYRE_QWEN_SAMPLE_TOPK");
    state->sample_topk = topk_env ? atoi(topk_env) : FPQ_RUN_SAMPLE_TOPK_DEFAULT;
    if (state->sample_topk < 1) state->sample_topk = FPQ_RUN_SAMPLE_TOPK_DEFAULT;
    if (state->sample_topk > cfg->n_vocab) state->sample_topk = cfg->n_vocab;
    state->sample_ids = (int *)malloc((size_t)state->sample_topk * sizeof(int));
    state->sample_probs = (float *)malloc((size_t)state->sample_topk * sizeof(float));

    state->k_caches = k_cache;
    state->v_caches = v_cache;
    if (!state->k_caches || !state->v_caches) {
        state->owns_kv = 1;
        state->k_caches = (float **)calloc((size_t)cfg->n_layers, sizeof(float *));
        state->v_caches = (float **)calloc((size_t)cfg->n_layers, sizeof(float *));
        kv_size = (size_t)cfg->max_seq_len * (size_t)cfg->n_kv_heads * (size_t)cfg->head_dim;
        if (state->k_caches && state->v_caches) {
            for (int i = 0; i < cfg->n_layers; i++) {
                state->k_caches[i] = (float *)calloc(kv_size, sizeof(float));
                state->v_caches[i] = (float *)calloc(kv_size, sizeof(float));
            }
        }
    }

    state->worker = fpq_worker_start();
    if (!state->h || !state->h_norm || !state->q_buf || !state->k_buf || !state->v_buf ||
        !state->attn_out || !state->o_buf || !state->gate_buf || !state->up_buf ||
        !state->ffn_out || !state->logits || !state->att_scratch || !state->sample_ids ||
        !state->sample_probs || !state->k_caches || !state->v_caches || !state->worker) {
        fpq_run_state_free(state);
        return NULL;
    }
    return state;
}

void fpq_run_state_reset(fpq_run_state_t *state) {
    if (!state) return;
    memset(state->h, 0, (size_t)state->cfg.d_model * sizeof(float));
    memset(state->h_norm, 0, (size_t)state->cfg.d_model * sizeof(float));
    memset(state->q_buf, 0, (size_t)(state->cfg.n_heads * state->cfg.head_dim) * sizeof(float));
    memset(state->k_buf, 0, (size_t)(state->cfg.n_kv_heads * state->cfg.head_dim) * sizeof(float));
    memset(state->v_buf, 0, (size_t)(state->cfg.n_kv_heads * state->cfg.head_dim) * sizeof(float));
    memset(state->attn_out, 0, (size_t)(state->cfg.n_heads * state->cfg.head_dim) * sizeof(float));
    memset(state->o_buf, 0, (size_t)state->cfg.d_model * sizeof(float));
    memset(state->gate_buf, 0, (size_t)state->cfg.d_ffn * sizeof(float));
    memset(state->up_buf, 0, (size_t)state->cfg.d_ffn * sizeof(float));
    memset(state->ffn_out, 0, (size_t)state->cfg.d_model * sizeof(float));
    memset(state->logits, 0, (size_t)state->cfg.n_vocab * sizeof(float));
    memset(state->att_scratch, 0,
           (size_t)state->cfg.n_heads * (size_t)state->cfg.max_seq_len * sizeof(float));
    memset(&state->metrics, 0, sizeof(state->metrics));
    state->metrics.prefill_chunk_size = state->cfg.prefill_chunk_size;
}

const fpq_run_metrics_t *fpq_run_state_metrics(const fpq_run_state_t *state) {
    return state ? &state->metrics : NULL;
}

void fpq_run_state_free(fpq_run_state_t *state) {
    if (!state) return;
    fpq_worker_stop(state->worker);
    free(state->h);
    free(state->h_norm);
    free(state->q_buf);
    free(state->k_buf);
    free(state->v_buf);
    free(state->attn_out);
    free(state->o_buf);
    free(state->gate_buf);
    free(state->up_buf);
    free(state->ffn_out);
    free(state->logits);
    free(state->att_scratch);
    free(state->sample_ids);
    free(state->sample_probs);
    if (state->owns_kv) {
        for (int i = 0; i < state->cfg.n_layers; i++) {
            free(state->k_caches ? state->k_caches[i] : NULL);
            free(state->v_caches ? state->v_caches[i] : NULL);
        }
        free(state->k_caches);
        free(state->v_caches);
    }
    free(state);
}

typedef struct {
    size_t n;
    size_t finite;
    size_t nan;
    size_t inf;
    size_t zero;
    float min_v;
    float max_v;
    double mean;
    double sum_abs;
} fpq_vec_stats_t;

static fpq_vec_stats_t fpq_collect_vec_stats(const float *x, size_t n) {
    fpq_vec_stats_t s;
    memset(&s, 0, sizeof(s));
    s.n = n;
    s.min_v = FLT_MAX;
    s.max_v = -FLT_MAX;
    double sum = 0.0;
    for (size_t i = 0; i < n; i++) {
        float v = x[i];
        if (isnan(v)) { s.nan++; continue; }
        if (isinf(v)) { s.inf++; continue; }
        s.finite++;
        if (v == 0.0f) s.zero++;
        if (v < s.min_v) s.min_v = v;
        if (v > s.max_v) s.max_v = v;
        sum += (double)v;
        s.sum_abs += fabs((double)v);
    }
    if (s.finite == 0) {
        s.min_v = 0.0f;
        s.max_v = 0.0f;
        s.mean = 0.0;
    } else {
        s.mean = sum / (double)s.finite;
    }
    return s;
}

static void fpq_log_vec_stats(const char *name, const fpq_vec_stats_t *s) {
    fprintf(stderr,
            "vec_stats: name=%s n=%zu finite=%zu nan=%zu inf=%zu zero=%zu min=%g max=%g mean=%g sum_abs=%g\n",
            name,
            s->n,
            s->finite,
            s->nan,
            s->inf,
            s->zero,
            (double)s->min_v,
            (double)s->max_v,
            s->mean,
            s->sum_abs);
}

static const char *fpq_golden_jsonl_path(void) {
    const char *v = getenv("BONFYRE_QWEN_GOLDEN_JSONL");
    return (v && v[0]) ? v : NULL;
}

static int fpq_golden_enabled(void) {
    return fpq_golden_jsonl_path() != NULL;
}

static void fpq_golden_append_line(const char *line) {
    const char *path = fpq_golden_jsonl_path();
    if (!path || !line) return;
    FILE *fp = fopen(path, "a");
    if (!fp) return;
    fputs(line, fp);
    fputc('\n', fp);
    fclose(fp);
}

static void fpq_golden_emit_vec_stage(const char *stage,
                                      int layer,
                                      int step,
                                      const float *x,
                                      size_t n,
                                      const char *reference_kind) {
    if (!fpq_golden_enabled() || !stage || !x) return;
    fpq_vec_stats_t s = fpq_collect_vec_stats(x, n);
    char line[1024];
    snprintf(line, sizeof(line),
             "{\"kind\":\"golden_stage\",\"stage\":\"%s\",\"layer\":%d,\"step\":%d,"
             "\"reference_kind\":\"%s\",\"n\":%zu,\"finite\":%zu,\"nan\":%zu,\"inf\":%zu,"
             "\"min\":%.9g,\"max\":%.9g,\"mean\":%.9g,\"sum_abs\":%.9g}",
             stage, layer, step, reference_kind ? reference_kind : "active-only",
             n, s.finite, s.nan, s.inf, (double)s.min_v, (double)s.max_v, s.mean, s.sum_abs);
    fpq_golden_append_line(line);
}

static void fpq_golden_emit_matmul_probe(fpq_model_t *model,
                                         const char *stage,
                                         const char *tensor_name,
                                         const float *input,
                                         const float *active,
                                         size_t out_n,
                                         int layer,
                                         int step) {
    if (!fpq_golden_enabled() || !model || !stage || !tensor_name || !input || !active || out_n == 0) return;
    size_t probe_ids[5];
    size_t n_probe = 0;
    probe_ids[n_probe++] = 0;
    if (out_n > 1) probe_ids[n_probe++] = out_n / 4;
    if (out_n > 2) probe_ids[n_probe++] = out_n / 2;
    if (out_n > 3) probe_ids[n_probe++] = (out_n * 3) / 4;
    if (out_n > 1) probe_ids[n_probe++] = out_n - 1;

    double sum_abs_delta = 0.0;
    double max_abs_delta = 0.0;
    size_t ok = 0;
    for (size_t i = 0; i < n_probe; i++) {
        float ref = 0.0f;
        if (fpq_matmul_row_native(model, tensor_name, probe_ids[i], input, &ref) != 0) continue;
        double delta = fabs((double)active[probe_ids[i]] - (double)ref);
        sum_abs_delta += delta;
        if (delta > max_abs_delta) max_abs_delta = delta;
        ok++;
    }
    char line[1024];
    snprintf(line, sizeof(line),
             "{\"kind\":\"golden_probe\",\"stage\":\"%s\",\"layer\":%d,\"step\":%d,"
             "\"tensor\":\"%s\",\"reference_kind\":\"native-row\",\"probes\":%zu,"
             "\"mean_abs_delta\":%.9g,\"max_abs_delta\":%.9g}",
             stage, layer, step, tensor_name, ok,
             ok ? (sum_abs_delta / (double)ok) : -1.0, max_abs_delta);
    fpq_golden_append_line(line);
}

static void fpq_golden_emit_rope_probe(const char *stage,
                                       const float *pre,
                                       const float *post,
                                       int head_dim,
                                       int pos,
                                       float theta,
                                       fpq_run_arch_t arch,
                                       int layer,
                                       int step) {
    if (!fpq_golden_enabled() || !stage || !pre || !post || head_dim <= 0) return;
    float *scratch = (float *)malloc((size_t)head_dim * sizeof(float));
    if (!scratch) return;
    memcpy(scratch, pre, (size_t)head_dim * sizeof(float));
    rope_apply_for_arch(scratch, head_dim, pos, theta, arch);
    double sum_abs_delta = 0.0;
    double max_abs_delta = 0.0;
    for (int i = 0; i < head_dim; i++) {
        double delta = fabs((double)post[i] - (double)scratch[i]);
        sum_abs_delta += delta;
        if (delta > max_abs_delta) max_abs_delta = delta;
    }
    free(scratch);
    char line[1024];
    snprintf(line, sizeof(line),
             "{\"kind\":\"golden_probe\",\"stage\":\"%s\",\"layer\":%d,\"step\":%d,"
             "\"reference_kind\":\"scalar-rope\",\"probes\":%d,\"mean_abs_delta\":%.9g,\"max_abs_delta\":%.9g}",
             stage, layer, step, head_dim,
             head_dim > 0 ? (sum_abs_delta / (double)head_dim) : 0.0, max_abs_delta);
    fpq_golden_append_line(line);
}

static void fpq_golden_emit_logits_probe(fpq_model_t *model,
                                         const fpq_run_config_t *cfg,
                                         const float *h_norm,
                                         const float *logits,
                                         int vocab_size,
                                         int step) {
    if (!fpq_golden_enabled() || !model || !cfg || !h_norm || !logits || vocab_size <= 0) return;

    int top_ids[10];
    fpq_collect_topk_ids(logits, vocab_size, 10, top_ids);

    double sum_abs_delta = 0.0;
    double max_abs_delta = 0.0;
    int ok = 0;
    const char *lm_head_name = fpq_cfg_lm_head_tensor_name(cfg);
    for (int i = 0; i < 10; i++) {
        int token_id = top_ids[i];
        float ref = 0.0f;
        if (token_id < 0 || token_id >= vocab_size) continue;
        if (fpq_matmul_row_native(model, lm_head_name, (size_t)token_id, h_norm, &ref) != 0) continue;
        double delta = fabs((double)logits[token_id] - (double)ref);
        sum_abs_delta += delta;
        if (delta > max_abs_delta) max_abs_delta = delta;
        ok++;
    }

    char line[1024];
    snprintf(line, sizeof(line),
             "{\"kind\":\"golden_probe\",\"stage\":\"topk_logits\",\"layer\":-1,\"step\":%d,"
             "\"tensor\":\"%s\",\"reference_kind\":\"native-row-topk\",\"probes\":%d,"
             "\"mean_abs_delta\":%.9g,\"max_abs_delta\":%.9g}",
             step, lm_head_name, ok,
             ok ? (sum_abs_delta / (double)ok) : -1.0, max_abs_delta);
    fpq_golden_append_line(line);
}

static int fpq_check_vec_fatal(const char *name, const fpq_vec_stats_t *s, int require_nonzero_sum_abs) {
    if (s->finite == 0) {
        fprintf(stderr, "qwen_runtime: FATAL non-finite vector %s\n", name);
        return -1;
    }
    if (require_nonzero_sum_abs && s->sum_abs == 0.0) {
        fprintf(stderr, "qwen_runtime: FATAL zero vector %s\n", name);
        return -1;
    }
    return 0;
}

static int fpq_layer_probe_match(int lay, int n_layers) {
    static const int fixed[] = {0, 8, 16, 24, 32, 40};
    for (size_t i = 0; i < sizeof(fixed) / sizeof(fixed[0]); i++) {
        if (lay == fixed[i] && lay < n_layers) return 1;
    }
    /* Fine-grained scan: every layer from 41 to n_layers-1 */
    if (lay >= 41 && lay < n_layers) return 1;
    return 0;
}

/* ═══════════════════════════════════════════════════════
 * fpq_run_generate — main inference loop
 * ═══════════════════════════════════════════════════════ */


static int qwen_norm_inventory_enabled(void) {
    const char *v = getenv("BONFYRE_QWEN_NORM_INVENTORY");
    return v && v[0] && strcmp(v, "0") != 0;
}

static void qwen_log_norm_inventory_once(const float *norm_layers, int n_lay, int d) {
    static int done = 0;
    if (done || !qwen_norm_inventory_enabled() || !norm_layers || n_lay <= 0 || d <= 0) return;
    done = 1;

    for (int lay = 0; lay < n_lay; lay++) {
        for (int kind = 0; kind < 2; kind++) {
            const char *point = kind == 0 ? "attn_norm" : "mlp_norm";
            const float *w = norm_layers + (size_t)lay * 2 * d + (size_t)kind * d;
            float min_v = FLT_MAX, max_v = -FLT_MAX;
            double sumsq = 0.0, sum_abs = 0.0, max_abs = -1.0;
            size_t top_abs_idx = 0;
            size_t finite = 0, nan_n = 0, inf_n = 0;

            for (int i = 0; i < d; i++) {
                float v = w[i];
                if (isnan(v)) { nan_n++; continue; }
                if (isinf(v)) { inf_n++; continue; }
                double av = fabs((double)v);
                if (v < min_v) min_v = v;
                if (v > max_v) max_v = v;
                if (av > max_abs) {
                    max_abs = av;
                    top_abs_idx = (size_t)i;
                }
                sumsq += (double)v * (double)v;
                sum_abs += av;
                finite++;
            }

            if (finite == 0) {
                min_v = 0.0f;
                max_v = 0.0f;
                max_abs = 0.0;
                top_abs_idx = 0;
            }

            fprintf(stderr,
                    "norm_inventory layer=%d point=%s finite=%zu nan=%zu inf=%zu l2=%g min=%g max=%g mean_abs=%g max_abs=%g top_abs_idx=%zu top_abs=%g suspicious=%d\n",
                    lay, point, finite, nan_n, inf_n, sqrt(sumsq),
                    (double)min_v, (double)max_v,
                    finite ? sum_abs / (double)finite : 0.0,
                    max_abs, top_abs_idx, max_abs,
                    (max_abs > 1.0 || nan_n || inf_n) ? 1 : 0);
        }
    }
}

static int fpq_run_flash_prefill_prefix(
        fpq_model_t *model,
        const float *embed_table,
        const float *norm_layers,
        const int *prompt_ids,
        int prefix_len,
        const fpq_run_config_t *cfg,
        fpq_run_state_t *state,
        int active_kv_window) {
    const int d = cfg->d_model;
    const int d_ffn = cfg->d_ffn;
    const int n_lay = cfg->n_layers;
    const int n_h = cfg->n_heads;
    const int n_kv = cfg->n_kv_heads;
    const int hd = cfg->head_dim;
    const int max_seq = cfg->max_seq_len;
    const float eps = cfg->rms_norm_eps;
    const float theta = cfg->rope_theta;
    const fpq_run_arch_t arch = cfg->arch;
    if (!model || !prompt_ids || prefix_len <= 0 || !cfg || !state || !norm_layers) return 0;

    size_t token_stride_d = (size_t)d;
    size_t token_stride_ffn = (size_t)d_ffn;
    size_t token_stride_q = (size_t)(n_h * hd);
    size_t token_stride_kv = (size_t)(n_kv * hd);

    float *hs = (float *)calloc((size_t)prefix_len * token_stride_d, sizeof(float));
    float *h_norms = (float *)calloc((size_t)prefix_len * token_stride_d, sizeof(float));
    float *qs = (float *)calloc((size_t)prefix_len * token_stride_q, sizeof(float));
    float *ks = (float *)calloc((size_t)prefix_len * token_stride_kv, sizeof(float));
    float *vs = (float *)calloc((size_t)prefix_len * token_stride_kv, sizeof(float));
    float *attn_outs = (float *)calloc((size_t)prefix_len * token_stride_q, sizeof(float));
    float *os = (float *)calloc((size_t)prefix_len * token_stride_d, sizeof(float));
    float *gates = (float *)calloc((size_t)prefix_len * token_stride_ffn, sizeof(float));
    float *ups = (float *)calloc((size_t)prefix_len * token_stride_ffn, sizeof(float));
    float *ffns = (float *)calloc((size_t)prefix_len * token_stride_d, sizeof(float));
    float *attn_scratch = (float *)calloc((size_t)prefix_len * (size_t)n_h * (size_t)max_seq, sizeof(float));
    if (!hs || !h_norms || !qs || !ks || !vs || !attn_outs || !os || !gates || !ups || !ffns || !attn_scratch) {
        free(hs); free(h_norms); free(qs); free(ks); free(vs);
        free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
        return -1;
    }

    for (int t = 0; t < prefix_len; t++) {
        float *h = hs + (size_t)t * token_stride_d;
        int token = prompt_ids[t];
        if (embed_table && !isnan(*embed_table)) {
            memcpy(h, embed_table + (size_t)token * token_stride_d, token_stride_d * sizeof(float));
        } else if (fpq_decode_row(model, fpq_cfg_embed_tensor_name(cfg), (size_t)token, h) != 0) {
            free(hs); free(h_norms); free(qs); free(ks); free(vs);
            free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
            return -1;
        }
    }

    for (int lay = 0; lay < n_lay; lay++) {
        struct timespec lay_t0 = {0};
        struct timespec lay_t1 = {0};
        struct timespec seg_t0 = {0};
        struct timespec seg_t1 = {0};
        double norm1_seconds = 0.0;
        double prepare_seconds = 0.0;
        double qkv_seconds = 0.0;
        double attn_seconds = 0.0;
        double o_seconds = 0.0;
        double norm2_seconds = 0.0;
        double mlp_seconds = 0.0;
        clock_gettime(CLOCK_MONOTONIC, &lay_t0);
        if (qwen_log_layer_progress_enabled()) {
            fprintf(stderr,
                    "flash_prefill begin tokens=%d layer=%d/%d\n",
                    prefix_len, lay + 1, n_lay);
        }
        qwen_runtime_heartbeat_tick("prefill_layer_begin", lay, n_lay);
        const float *inp_norm_w = norm_layers + (size_t)lay * 2 * (size_t)d;
        const float *post_norm_w = inp_norm_w + d;
        char prewarm_name[FPQ_RUN_NAME_BUF];
        char prewarm_k[FPQ_RUN_NAME_BUF];
        char prewarm_v[FPQ_RUN_NAME_BUF];
        char prewarm_up[FPQ_RUN_NAME_BUF];
        char prewarm_gate[FPQ_RUN_NAME_BUF];

        clock_gettime(CLOCK_MONOTONIC, &seg_t0);
        for (int t = 0; t < prefix_len; t++) {
            rms_norm(h_norms + (size_t)t * token_stride_d,
                     hs + (size_t)t * token_stride_d,
                     inp_norm_w, d, eps, 0);
        }
        clock_gettime(CLOCK_MONOTONIC, &seg_t1);
        norm1_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
        fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_NORM, norm1_seconds);

        clock_gettime(CLOCK_MONOTONIC, &seg_t0);
        tname(prewarm_name, arch, "self_attn.q_proj.weight", lay);
        tname(prewarm_k, arch, "self_attn.k_proj.weight", lay);
        tname(prewarm_v, arch, "self_attn.v_proj.weight", lay);
        if (qwen_prepare_tensor_guarded(model, prewarm_name) != 0 ||
            qwen_prepare_tensor_guarded(model, prewarm_k) != 0 ||
            qwen_prepare_tensor_guarded(model, prewarm_v) != 0) {
            free(hs); free(h_norms); free(qs); free(ks); free(vs);
            free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
            return -1;
        }
        tname(prewarm_name, arch, "self_attn.o_proj.weight", lay);
        if (qwen_prepare_tensor_guarded(model, prewarm_name) != 0) {
            free(hs); free(h_norms); free(qs); free(ks); free(vs);
            free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
            return -1;
        }
        tname(prewarm_gate, arch, "mlp.gate_proj.weight", lay);
        tname(prewarm_up, arch, "mlp.up_proj.weight", lay);
        if (qwen_prepare_tensor_guarded(model, prewarm_gate) != 0 ||
            qwen_prepare_tensor_guarded(model, prewarm_up) != 0) {
            free(hs); free(h_norms); free(qs); free(ks); free(vs);
            free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
            return -1;
        }
        tname(prewarm_name, arch, "mlp.down_proj.weight", lay);
        if (qwen_prepare_tensor_guarded(model, prewarm_name) != 0) {
            free(hs); free(h_norms); free(qs); free(ks); free(vs);
            free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
            return -1;
        }
        clock_gettime(CLOCK_MONOTONIC, &seg_t1);
        prepare_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
        state->metrics.tensor_prepare_seconds += prepare_seconds;
        fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_PREPARE, prepare_seconds);

        {
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            pthread_t *threads = (pthread_t *)calloc((size_t)prefix_len, sizeof(*threads));
            fpq_prefill_qkv_task_t *tasks =
                (fpq_prefill_qkv_task_t *)calloc((size_t)prefix_len, sizeof(*tasks));
            if (!threads || !tasks) {
                free(threads); free(tasks);
                free(hs); free(h_norms); free(qs); free(ks); free(vs);
                free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
                return -1;
            }
            for (int t = 0; t < prefix_len; t++) {
                tasks[t].model = model;
                tasks[t].arch = arch;
                tasks[t].lay = lay;
                tasks[t].token_pos = t;
                tasks[t].hd = hd;
                tasks[t].n_h = n_h;
                tasks[t].n_kv = n_kv;
                tasks[t].theta = theta;
                tasks[t].h_norm = h_norms + (size_t)t * token_stride_d;
                tasks[t].q_buf = qs + (size_t)t * token_stride_q;
                tasks[t].k_buf = ks + (size_t)t * token_stride_kv;
                tasks[t].v_buf = vs + (size_t)t * token_stride_kv;
                if (qwen_inline_prefill_threads_enabled()) {
                    (void)fpq_prefill_qkv_thread_fn(&tasks[t]);
                } else if (pthread_create(&threads[t], qwen_small_pthread_attr(), fpq_prefill_qkv_thread_fn, &tasks[t]) != 0) {
                    tasks[t].rc = -1;
                }
            }
            for (int t = 0; t < prefix_len; t++) {
                if (!qwen_inline_prefill_threads_enabled()) {
                    pthread_join(threads[t], NULL);
                }
                if (tasks[t].rc != 0) {
                    free(threads); free(tasks);
                    free(hs); free(h_norms); free(qs); free(ks); free(vs);
                    free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
                    return -1;
                }
            }
            free(threads);
            free(tasks);
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            qkv_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_QKV, qkv_seconds);
        }

        clock_gettime(CLOCK_MONOTONIC, &seg_t0);
        for (int t = 0; t < prefix_len; t++) {
            int cache_off = t * n_kv * hd;
            memcpy(state->k_caches[lay] + cache_off,
                   ks + (size_t)t * token_stride_kv,
                   token_stride_kv * sizeof(float));
            memcpy(state->v_caches[lay] + cache_off,
                   vs + (size_t)t * token_stride_kv,
                   token_stride_kv * sizeof(float));
        }
        clock_gettime(CLOCK_MONOTONIC, &seg_t1);
        {
            double kv_write_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
            state->metrics.kv_write_seconds += kv_write_seconds;
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_KV_WRITE, kv_write_seconds);
        }

        clock_gettime(CLOCK_MONOTONIC, &seg_t0);
        for (int t = 0; t < prefix_len; t++) {
            memset(attn_outs + (size_t)t * token_stride_q, 0, token_stride_q * sizeof(float));
            gqa_attention(qs + (size_t)t * token_stride_q,
                          state->k_caches[lay],
                          state->v_caches[lay],
                          attn_outs + (size_t)t * token_stride_q,
                          attn_scratch + (size_t)t * (size_t)n_h * (size_t)max_seq,
                          t, n_h, n_kv, hd, max_seq, active_kv_window, lay, t);
        }
        clock_gettime(CLOCK_MONOTONIC, &seg_t1);
        attn_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
        fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_ATTENTION, attn_seconds);

        {
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            pthread_t *threads = (pthread_t *)calloc((size_t)prefix_len, sizeof(*threads));
            fpq_prefill_o_task_t *tasks =
                (fpq_prefill_o_task_t *)calloc((size_t)prefix_len, sizeof(*tasks));
            if (!threads || !tasks) {
                free(threads); free(tasks);
                free(hs); free(h_norms); free(qs); free(ks); free(vs);
                free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
                return -1;
            }
            for (int t = 0; t < prefix_len; t++) {
                tasks[t].model = model;
                tasks[t].arch = arch;
                tasks[t].lay = lay;
                tasks[t].d = d;
                tasks[t].attn_in = attn_outs + (size_t)t * token_stride_q;
                tasks[t].o_buf = os + (size_t)t * token_stride_d;
                if (qwen_inline_prefill_threads_enabled()) {
                    (void)fpq_prefill_o_thread_fn(&tasks[t]);
                } else if (pthread_create(&threads[t], qwen_small_pthread_attr(), fpq_prefill_o_thread_fn, &tasks[t]) != 0) {
                    tasks[t].rc = -1;
                }
            }
            for (int t = 0; t < prefix_len; t++) {
                if (!qwen_inline_prefill_threads_enabled()) {
                    pthread_join(threads[t], NULL);
                }
                if (tasks[t].rc != 0) {
                    free(threads); free(tasks);
                    free(hs); free(h_norms); free(qs); free(ks); free(vs);
                    free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
                    return -1;
                }
                float *h = hs + (size_t)t * token_stride_d;
                float *o = os + (size_t)t * token_stride_d;
                for (int i = 0; i < d; i++) h[i] += o[i];
            }
            free(threads);
            free(tasks);
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            o_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_O_PROJ, o_seconds);
        }

        clock_gettime(CLOCK_MONOTONIC, &seg_t0);
        for (int t = 0; t < prefix_len; t++) {
            rms_norm(h_norms + (size_t)t * token_stride_d,
                     hs + (size_t)t * token_stride_d,
                     post_norm_w, d, eps, 0);
        }
        clock_gettime(CLOCK_MONOTONIC, &seg_t1);
        norm2_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
        fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_NORM, norm2_seconds);

        {
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            pthread_t *threads = (pthread_t *)calloc((size_t)prefix_len, sizeof(*threads));
            fpq_prefill_mlp_task_t *tasks =
                (fpq_prefill_mlp_task_t *)calloc((size_t)prefix_len, sizeof(*tasks));
            if (!threads || !tasks) {
                free(threads); free(tasks);
                free(hs); free(h_norms); free(qs); free(ks); free(vs);
                free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
                return -1;
            }
            for (int t = 0; t < prefix_len; t++) {
                tasks[t].model = model;
                tasks[t].arch = arch;
                tasks[t].lay = lay;
                tasks[t].d = d;
                tasks[t].d_ffn = d_ffn;
                tasks[t].h_norm = h_norms + (size_t)t * token_stride_d;
                tasks[t].gate_buf = gates + (size_t)t * token_stride_ffn;
                tasks[t].up_buf = ups + (size_t)t * token_stride_ffn;
                tasks[t].ffn_out = ffns + (size_t)t * token_stride_d;
                if (qwen_inline_prefill_threads_enabled()) {
                    (void)fpq_prefill_mlp_thread_fn(&tasks[t]);
                } else if (pthread_create(&threads[t], qwen_small_pthread_attr(), fpq_prefill_mlp_thread_fn, &tasks[t]) != 0) {
                    tasks[t].rc = -1;
                }
            }
            for (int t = 0; t < prefix_len; t++) {
                if (!qwen_inline_prefill_threads_enabled()) {
                    pthread_join(threads[t], NULL);
                }
                if (tasks[t].rc != 0) {
                    free(threads); free(tasks);
                    free(hs); free(h_norms); free(qs); free(ks); free(vs);
                    free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
                    return -1;
                }
                float *h = hs + (size_t)t * token_stride_d;
                float *ffn = ffns + (size_t)t * token_stride_d;
                for (int i = 0; i < d; i++) h[i] += ffn[i];
            }
            free(threads);
            free(tasks);
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            mlp_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_MLP, mlp_seconds);
        }
        clock_gettime(CLOCK_MONOTONIC, &lay_t1);
        qwen_runtime_heartbeat_tick("prefill_layer_end", lay, cfg->n_layers);
        if (lay >= 0 && lay < FPQ_RUN_MAX_LAYERS) {
            state->metrics.prefill_layer_total_seconds[lay] += fpq_elapsed_seconds(&lay_t0, &lay_t1);
            state->metrics.prefill_layer_prepare_seconds[lay] += prepare_seconds;
        }
        if (qwen_release_layer_after_prefill_enabled()) {
            struct timespec release_t0 = {0};
            struct timespec release_t1 = {0};
            clock_gettime(CLOCK_MONOTONIC, &release_t0);
            qwen_release_layer_runtime(model, lay, cfg->n_layers, "prefill");
            clock_gettime(CLOCK_MONOTONIC, &release_t1);
            {
                double release_seconds = fpq_elapsed_seconds(&release_t0, &release_t1);
                state->metrics.release_seconds += release_seconds;
                fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_RELEASE, release_seconds);
            }
        }
        qwen_release_log_mem_if_enabled("after_prefill_layer", lay, cfg->n_layers);

        if (qwen_log_prefill_timing_enabled()) {
            fprintf(stderr,
                    "flash_prefill timing layer=%d/%d tokens=%d total=%.3f norm1=%.3f prepare=%.3f qkv=%.3f attn=%.3f o=%.3f norm2=%.3f mlp=%.3f\n",
                    lay + 1, n_lay, prefix_len,
                    fpq_elapsed_seconds(&lay_t0, &lay_t1),
                    norm1_seconds, prepare_seconds, qkv_seconds, attn_seconds,
                    o_seconds, norm2_seconds, mlp_seconds);

            {
                int stop_layer = qwen_stop_after_layer_for_debug();
                if (stop_layer > 0 && lay + 1 >= stop_layer) {
                    fprintf(stderr,
                            "flash_prefill timing debug stop_after_layer=%d reached at layer=%d/%d\n",
                            stop_layer, lay + 1, cfg->n_layers);
                    fflush(stderr);
                    exit(0);
                }
            }
        }
        if (qwen_log_layer_progress_enabled()) {
            fprintf(stderr,
                    "flash_prefill end tokens=%d layer=%d/%d\n",
                    prefix_len, lay + 1, n_lay);

        {
            int stop_layer = qwen_stop_after_layer_for_debug();
            if (stop_layer > 0 && lay + 1 >= stop_layer) {
                fprintf(stderr,
                        "flash_prefill debug stop_after_layer=%d reached at layer=%d/%d\n",
                        stop_layer, lay + 1, cfg->n_layers);
                fflush(stderr);
                exit(0);
            }
        }
        }
    }

    free(hs); free(h_norms); free(qs); free(ks); free(vs);
    free(attn_outs); free(os); free(gates); free(ups); free(ffns); free(attn_scratch);
    return 0;
}

int fpq_run_generate(
        fpq_model_t *model,
        const float *embed_table,  /* [n_vocab × d_model], row-major, pre-decoded */
        const float *norm_layers,  /* [n_layers × 2 × d_model] — input+post norms */
        const float *final_norm,   /* [d_model] */
        const int   *prompt_ids,
        int          prompt_len,
        const fpq_run_config_t *cfg,
        fpq_run_state_t *state,
        fpq_run_token_cb callback, /* called per generated token, NULL = print */
        void        *cb_data) {

    int d       = cfg->d_model;
    int d_ffn   = cfg->d_ffn;
    int n_lay   = cfg->n_layers;
    int n_h     = cfg->n_heads;
    int n_kv    = cfg->n_kv_heads;
    int hd      = cfg->head_dim;
    int max_seq = cfg->max_seq_len;
    int max_new = cfg->max_new_tokens;
    int sample_n_vocab = cfg->sample_n_vocab > 0 ? cfg->sample_n_vocab : cfg->n_vocab;
    if (sample_n_vocab > cfg->n_vocab) sample_n_vocab = cfg->n_vocab;
    if (sample_n_vocab < 1) sample_n_vocab = cfg->n_vocab;
    float eps   = cfg->rms_norm_eps;
    float theta = cfg->rope_theta;
    fpq_run_arch_t arch = cfg->arch;
    int active_kv_window = 0;
    int debug_enabled = fpq_run_debug_enabled();
    int layer_probe_enabled = fpq_layer_probe_enabled();
    int layer_internal_probe_enabled = fpq_layer_internal_probe_enabled();
    int norm_probe_enabled = fpq_norm_probe_enabled();
    int prefill_chunk_size = cfg->prefill_chunk_size;
    float *h;
    float *h_norm;
    float *q_buf;
    float *k_buf;
    float *v_buf;
    float *attn_out;
    float *o_buf;
    float *gate_buf;
    float *up_buf;
    float *ffn_out;
    float *logits;
    float *att_scratch;
    float **k_caches;
    float **v_caches;
    {
        const char *w = getenv("BONFYRE_ACTIVE_KV_WINDOW");
        if (w && *w) {
            int v = atoi(w);
            if (v > 0) active_kv_window = v;
        }
    }

    int generated = -1;
    if (!state) {
        fprintf(stderr, "fpq_run: missing persistent state\n");
        return -1;
    }
    if (prefill_chunk_size <= 0) prefill_chunk_size = 1;
    h = state->h;
    h_norm = state->h_norm;
    q_buf = state->q_buf;
    k_buf = state->k_buf;
    v_buf = state->v_buf;
    attn_out = state->attn_out;
    o_buf = state->o_buf;
    gate_buf = state->gate_buf;
    up_buf = state->up_buf;
    ffn_out = state->ffn_out;
    logits = state->logits;
    att_scratch = state->att_scratch;
    k_caches = state->k_caches;
    v_caches = state->v_caches;
    fpq_run_state_reset(state);
    state->metrics.prompt_tokens = prompt_len;
    state->metrics.prefill_chunk_size = prefill_chunk_size;
    state->metrics.active_kv_window = active_kv_window;
    qwen_log_norm_inventory_once(norm_layers, n_lay, d);

    char name_buf[FPQ_RUN_NAME_BUF];
    uint64_t rng = (uint64_t)time(NULL) ^ 0xDEADBEEFCAFEBABEULL;
    fpq_active_cache_t *active_cache = (fpq_active_cache_t *)fpq_model_get_active_cache(model);
    struct timespec chunk_t0 = {0};
    size_t chunk_cache_hits_base = 0;
    int chunk_start = -1;
    int start_step = 0;

    /* Process prompt tokens, then generate */
    int total_pos = 0;          /* next KV cache position */
    generated = 0;
    int next_token = -1;
    int golden_step_done = 0;

    if (fpq_run_use_flashqla_prefill() && prompt_len > 1 && !debug_enabled) {
        struct timespec prefill_t0 = {0};
        struct timespec prefill_t1 = {0};
        if (fpq_run_use_bulk_prepare()) {
            struct timespec bulk_t0 = {0};
            struct timespec bulk_t1 = {0};
            clock_gettime(CLOCK_MONOTONIC, &bulk_t0);
            if (fpq_run_bulk_prepare_model(model, cfg) != 0) {
                fprintf(stderr, "fpq_run: bulk prepare failed\n");
                return -1;
            }
            clock_gettime(CLOCK_MONOTONIC, &bulk_t1);
            if (qwen_log_prefill_timing_enabled()) {
                fprintf(stderr, "flash_prefill bulk_prepare total=%.3f\n",
                        fpq_elapsed_seconds(&bulk_t0, &bulk_t1));
            }
        }
        clock_gettime(CLOCK_MONOTONIC, &prefill_t0);
        if (fpq_run_flash_prefill_prefix(model, embed_table, norm_layers,
                                         prompt_ids, prompt_len - 1,
                                         cfg, state, active_kv_window) != 0) {
            fprintf(stderr, "fpq_run: flash prefill prefix failed\n");
            return -1;
        }
        clock_gettime(CLOCK_MONOTONIC, &prefill_t1);
        state->metrics.prefill_wall_seconds +=
            (prefill_t1.tv_sec - prefill_t0.tv_sec) +
            (prefill_t1.tv_nsec - prefill_t0.tv_nsec) / 1e9;
        total_pos = prompt_len - 1;
        start_step = prompt_len - 1;
    }

    for (int step = start_step; step < prompt_len + max_new; step++) {
        int in_prompt_phase = (step < prompt_len);
        struct timespec step_t0 = {0};
        struct timespec step_t1 = {0};
        clock_gettime(CLOCK_MONOTONIC, &step_t0);
        if (in_prompt_phase && prefill_chunk_size > 1 && (step % prefill_chunk_size) == 0) {
            int chunk_end = step + prefill_chunk_size;
            if (chunk_end > prompt_len) chunk_end = prompt_len;
            clock_gettime(CLOCK_MONOTONIC, &chunk_t0);
            chunk_cache_hits_base = active_cache ? active_cache->hits : 0;
            chunk_start = step;
            if (debug_enabled) {
                fprintf(stderr,
                        "flashqla_prefill_chunk: begin=%d end=%d size=%d backend=%s\n",
                        step,
                        chunk_end,
                        chunk_end - step,
                        fpq_run_use_flashqla_prefill() ? "flashqla_prefill" : "chunked_cpu");
            }
        }
        int token;
        if (step < prompt_len) {
            token = prompt_ids[step];
        } else {
            if (next_token < 0) break;
            token = next_token;
        }

        if (token < 0 || token >= cfg->n_vocab) break;

        if (embed_table && !isnan(*embed_table)) {
            memcpy(h, embed_table + (size_t)token * (size_t)d, (size_t)d * sizeof(float));
        } else if (fpq_decode_row(model, fpq_cfg_embed_tensor_name(cfg), (size_t)token, h) != 0) {
            fprintf(stderr, "fpq_run: embedding row decode failed for token %d\n", token);
            return -1;
        }
        int inspect_step = debug_enabled && (step >= prompt_len - 1 && generated == 0);
        int golden_step = fpq_golden_enabled() && !golden_step_done &&
                          step >= prompt_len - 1 && generated == 0;
        char q_name_buf[FPQ_RUN_NAME_BUF];
        char k_name_buf[FPQ_RUN_NAME_BUF];
        char v_name_buf[FPQ_RUN_NAME_BUF];
        char o_name_buf[FPQ_RUN_NAME_BUF];
        char up_name_buf[FPQ_RUN_NAME_BUF];
        char gate_name_buf[FPQ_RUN_NAME_BUF];
        char down_name_buf[FPQ_RUN_NAME_BUF];
        float q_pre_rope_probe[128];
        float k_pre_rope_probe[128];
        int q_pre_rope_probe_n = 0;
        int k_pre_rope_probe_n = 0;
        if (golden_step) {
            fpq_golden_emit_vec_stage("embedding", -1, step, h, (size_t)d, "native-row");
        }
        if (inspect_step) {
            fpq_vec_stats_t st = fpq_collect_vec_stats(h, (size_t)d);
            fpq_log_vec_stats("embed_h", &st);
            if (fpq_check_vec_fatal("embed_h", &st, 1) != 0) return -1;
        }

        /* Run through all transformer layers */
        for (int lay = 0; lay < n_lay; lay++) {
            struct timespec layer_t0 = {0};
            struct timespec layer_t1 = {0};
            struct timespec seg_t0 = {0};
            struct timespec seg_t1 = {0};
            double layer_norm_seconds = 0.0;
            double layer_qkv_seconds = 0.0;
            double layer_kv_write_seconds = 0.0;
            double layer_attn_seconds = 0.0;
            double layer_o_seconds = 0.0;
            double layer_mlp_seconds = 0.0;
            double layer_release_seconds = 0.0;
            clock_gettime(CLOCK_MONOTONIC, &layer_t0);
            if (qwen_log_layer_progress_enabled()) {
                fprintf(stderr,
                        "layer_progress begin step=%d/%d generated=%d layer=%d/%d total_pos=%d\n",
                        step + 1, prompt_len + max_new, generated, lay + 1, n_lay, total_pos);
            }
            qwen_runtime_heartbeat_tick("decode_layer_begin", lay, n_lay);
            const float *inp_norm_w = norm_layers + (size_t)lay * 2 * d;
            const float *post_norm_w = norm_layers + (size_t)lay * 2 * d + d;
            int internal_probe_step = layer_internal_probe_enabled &&
                                      step >= prompt_len - 1 &&
                                      generated == 0 &&
                                      fpq_should_probe_layer_internal(lay);
            int norm_probe_step = norm_probe_enabled &&
                                  step >= prompt_len - 1 &&
                                  generated == 0 &&
                                  (lay == 0 || lay == 1);

            /* ── Self-attention ── */
            if (internal_probe_step) {
                fpq_log_layer_internal_probe(lay, "residual_in", h, (size_t)d);
            }
            if (inspect_step && lay == 42) {
                fpq_vec_stats_t st = fpq_collect_vec_stats(h, (size_t)d);
                fpq_log_vec_stats("layer42_input_residual", &st);
                if (fpq_check_vec_fatal("layer42_input_residual", &st, 1) != 0) return -1;
            }
            const float *inp_norm_w_eff = inp_norm_w;
            float inp_norm_direct_buf[5120];
            if (qwen_direct_norm_weights_enabled() && d <= 5120) {
                tname(name_buf, arch, "input_layernorm.weight", lay);
                if (fpq_decode_row(model, name_buf, 0, inp_norm_direct_buf) == 0) {
                    qwen_log_norm_direct_compare(lay, "attn_norm", name_buf, inp_norm_w, inp_norm_direct_buf, d);
                    inp_norm_w_eff = inp_norm_direct_buf;
                } else {
                    fprintf(stderr, "norm_direct_decode_failed layer=%d point=attn_norm tensor=%s\n", lay, name_buf);
                }
            }
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            rms_norm(h_norm, h, inp_norm_w_eff, d, eps, 0);
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            layer_norm_seconds += fpq_elapsed_seconds(&seg_t0, &seg_t1);
            if (golden_step) {
                fpq_golden_emit_vec_stage("attn_norm_out", lay, step, h_norm, (size_t)d, "scalar-rmsnorm");
            }
            if (norm_probe_step) {
                int elem_ids[4];
                int n_elem_ids = 0;
                size_t input_top_idx = 0;
                double input_top_abs = 0.0;
                fpq_find_top_abs_elem(h, (size_t)d, &input_top_idx, &input_top_abs, NULL, NULL, NULL);
                elem_ids[n_elem_ids++] = (int)input_top_idx;
                if (lay == 1) {
                    elem_ids[n_elem_ids++] = 3094;
                    elem_ids[n_elem_ids++] = 2159;
                    elem_ids[n_elem_ids++] = 892;
                }
                fpq_log_norm_probe(lay, "attn_norm", h, inp_norm_w_eff, h_norm, d, eps, elem_ids, n_elem_ids);
            }
            if (internal_probe_step) {
                fpq_log_layer_internal_probe(lay, "attn_norm_out", h_norm, (size_t)d);
            }
            if (inspect_step && lay == 42) {
                fpq_vec_stats_t st = fpq_collect_vec_stats(h_norm, (size_t)d);
                fpq_log_vec_stats("layer42_input_rmsnorm", &st);
                if (fpq_check_vec_fatal("layer42_input_rmsnorm", &st, 1) != 0) return -1;
            }
            if (inspect_step && lay == 0) {
                fpq_vec_stats_t st = fpq_collect_vec_stats(h_norm, (size_t)d);
                fpq_log_vec_stats("layer0_input_rmsnorm", &st);
                if (fpq_check_vec_fatal("layer0_input_rmsnorm", &st, 1) != 0) return -1;
            }

            /* Q projection stands alone under GQA. K/V share input and shape, so
             * run them as one shared FPQ job on the worker while Q stays on main. */
            tname(k_name_buf, arch, "self_attn.k_proj.weight", lay);
            tname(v_name_buf, arch, "self_attn.v_proj.weight", lay);
            tname(q_name_buf, arch, "self_attn.q_proj.weight", lay);
            if (inspect_step && lay == 42) {
                for (int _i = 0; _i < n_h * hd; _i++) q_buf[_i] = NAN;
                for (int _i = 0; _i < n_kv * hd; _i++) k_buf[_i] = NAN;
                for (int _i = 0; _i < n_kv * hd; _i++) v_buf[_i] = NAN;
            }
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            if (qwen_disable_sli_fast_score_enabled()) {
                size_t kv_rows[2] = { (size_t)(n_kv * hd), (size_t)(n_kv * hd) };
                const char *kv_names[2] = { k_name_buf, v_name_buf };
                float *kv_outs[2] = { k_buf, v_buf };
                if (qwen_matmul_qkv_guarded(model, q_name_buf, h_norm, q_buf, (size_t)(n_h * hd)) != 0) {
                    fprintf(stderr, "qwen_runtime: FATAL native-row matmul failed tensor=%s layer=%d\n", q_name_buf, lay);
                    generated = -1;
                    goto cleanup;
                }
                if (qwen_matmul_qkv_shared_guarded(model, kv_names, h_norm, kv_outs, kv_rows, 2) != 0) {
                    fprintf(stderr, "qwen_runtime: FATAL native-row shared matmul failed layer=%d tensors=%s,%s\n",
                            lay, k_name_buf, v_name_buf);
                    generated = -1;
                    goto cleanup;
                }
            } else {
                fpq_worker_submit_shared(state->worker, model, k_name_buf, v_name_buf, h_norm, k_buf, v_buf);
                if (qwen_matmul_qkv_guarded(model, q_name_buf, h_norm, q_buf, (size_t)(n_h * hd)) != 0) {
                    fprintf(stderr, "qwen_runtime: FATAL matmul failed tensor=%s layer=%d\n", q_name_buf, lay);
                    generated = -1;
                    goto cleanup;
                }
                if (fpq_worker_wait(state->worker) != 0) {
                    fprintf(stderr, "qwen_runtime: FATAL shared matmul failed layer=%d tensors=%s,%s\n",
                            lay, k_name_buf, v_name_buf);
                    generated = -1;
                    goto cleanup;
                }
            }
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            layer_qkv_seconds += fpq_elapsed_seconds(&seg_t0, &seg_t1);
            tname(name_buf, arch, "self_attn.q_proj.bias", lay);
            qwen_add_bias_v2(model, name_buf, q_buf, n_h * hd, lay, "q_proj");
            tname(name_buf, arch, "self_attn.k_proj.bias", lay);
            qwen_add_bias_v2(model, name_buf, k_buf, n_kv * hd, lay, "k_proj");
            tname(name_buf, arch, "self_attn.v_proj.bias", lay);
            qwen_add_bias_v2(model, name_buf, v_buf, n_kv * hd, lay, "v_proj");
            if (golden_step) {
                fpq_golden_emit_matmul_probe(model, "q_proj", q_name_buf, h_norm, q_buf, (size_t)(n_h * hd), lay, step);
                fpq_golden_emit_matmul_probe(model, "k_proj", k_name_buf, h_norm, k_buf, (size_t)(n_kv * hd), lay, step);
                fpq_golden_emit_matmul_probe(model, "v_proj", v_name_buf, h_norm, v_buf, (size_t)(n_kv * hd), lay, step);
                q_pre_rope_probe_n = hd < (int)(sizeof(q_pre_rope_probe) / sizeof(q_pre_rope_probe[0]))
                    ? hd
                    : (int)(sizeof(q_pre_rope_probe) / sizeof(q_pre_rope_probe[0]));
                k_pre_rope_probe_n = hd < (int)(sizeof(k_pre_rope_probe) / sizeof(k_pre_rope_probe[0]))
                    ? hd
                    : (int)(sizeof(k_pre_rope_probe) / sizeof(k_pre_rope_probe[0]));
                memcpy(q_pre_rope_probe, q_buf, (size_t)q_pre_rope_probe_n * sizeof(float));
                memcpy(k_pre_rope_probe, k_buf, (size_t)k_pre_rope_probe_n * sizeof(float));
            }

            if (internal_probe_step) {
                fpq_log_layer_internal_probe(lay, "q_proj_out", q_buf, (size_t)(n_h * hd));
                fpq_log_layer_internal_probe(lay, "k_proj_out", k_buf, (size_t)(n_kv * hd));
                fpq_log_layer_internal_probe(lay, "v_proj_out", v_buf, (size_t)(n_kv * hd));
            }
            if (inspect_step && lay == 0) {
                fpq_vec_stats_t qst = fpq_collect_vec_stats(q_buf, (size_t)(n_h * hd));
                fpq_log_vec_stats("layer0_q_proj", &qst);
                if (fpq_check_vec_fatal("layer0_q_proj", &qst, 1) != 0) return -1;

                fpq_vec_stats_t kst = fpq_collect_vec_stats(k_buf, (size_t)(n_kv * hd));
                fpq_log_vec_stats("layer0_k_proj", &kst);
                if (fpq_check_vec_fatal("layer0_k_proj", &kst, 1) != 0) return -1;

                fpq_vec_stats_t vst = fpq_collect_vec_stats(v_buf, (size_t)(n_kv * hd));
                fpq_log_vec_stats("layer0_v_proj", &vst);
                if (fpq_check_vec_fatal("layer0_v_proj", &vst, 1) != 0) return -1;
            }
            if (inspect_step && lay == 42) {
                /* Check NaN sentinel overwrite completeness */
                int q_nan_remaining = 0;
                for (int _i = 0; _i < n_h * hd; _i++) if (isnan(q_buf[_i])) q_nan_remaining++;
                int k_nan_remaining = 0;
                for (int _i = 0; _i < n_kv * hd; _i++) if (isnan(k_buf[_i])) k_nan_remaining++;
                int v_nan_remaining = 0;
                for (int _i = 0; _i < n_kv * hd; _i++) if (isnan(v_buf[_i])) v_nan_remaining++;
                if (q_nan_remaining > 0 || k_nan_remaining > 0 || v_nan_remaining > 0)
                    fprintf(stderr, "layer42_proj_incomplete_write q_nan=%d k_nan=%d v_nan=%d\n",
                            q_nan_remaining, k_nan_remaining, v_nan_remaining);

                fpq_vec_stats_t qst = fpq_collect_vec_stats(q_buf, (size_t)(n_h * hd));
                fpq_log_vec_stats("layer42_q_proj", &qst);
                if (fpq_check_vec_fatal("layer42_q_proj", &qst, 1) != 0) return -1;

                fpq_vec_stats_t kst = fpq_collect_vec_stats(k_buf, (size_t)(n_kv * hd));
                fpq_log_vec_stats("layer42_k_proj", &kst);
                if (fpq_check_vec_fatal("layer42_k_proj", &kst, 1) != 0) return -1;

                fpq_vec_stats_t vst = fpq_collect_vec_stats(v_buf, (size_t)(n_kv * hd));
                fpq_log_vec_stats("layer42_v_proj", &vst);
                if (fpq_check_vec_fatal("layer42_v_proj", &vst, 1) != 0) return -1;
            }

            /* Apply RoPE to each query head */
            for (int hh = 0; hh < n_h; hh++)
                rope_apply_for_arch(q_buf + hh * hd, hd, total_pos, theta, arch);
            /* Apply RoPE to each KV head */
            for (int hh = 0; hh < n_kv; hh++)
                rope_apply_for_arch(k_buf + hh * hd, hd, total_pos, theta, arch);
            if (golden_step && q_pre_rope_probe_n > 0 && k_pre_rope_probe_n > 0) {
                fpq_golden_emit_rope_probe("q_post_rope", q_pre_rope_probe, q_buf,
                                           q_pre_rope_probe_n, total_pos, theta, arch, lay, step);
                fpq_golden_emit_rope_probe("k_post_rope", k_pre_rope_probe, k_buf,
                                           k_pre_rope_probe_n, total_pos, theta, arch, lay, step);
            }

            if (inspect_step && lay == 42) {
                /* Post-RoPE q stats — if RoPE introduces NaN the scores will be NaN */
                fpq_vec_stats_t qrst = fpq_collect_vec_stats(q_buf, (size_t)(n_h * hd));
                fpq_log_vec_stats("layer42_q_post_rope", &qrst);
                if (fpq_check_vec_fatal("layer42_q_post_rope", &qrst, 1) != 0) return -1;
                fpq_vec_stats_t krst = fpq_collect_vec_stats(k_buf, (size_t)(n_kv * hd));
                fpq_log_vec_stats("layer42_k_post_rope", &krst);
                if (fpq_check_vec_fatal("layer42_k_post_rope", &krst, 1) != 0) return -1;
            }

            /* Store K, V in this layer's cache at position total_pos */
            int cache_off = total_pos * n_kv * hd;
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            memcpy(k_caches[lay] + cache_off, k_buf, (size_t)(n_kv * hd) * sizeof(float));
            memcpy(v_caches[lay] + cache_off, v_buf, (size_t)(n_kv * hd) * sizeof(float));
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            layer_kv_write_seconds += fpq_elapsed_seconds(&seg_t0, &seg_t1);

            /* GQA attention over this layer's cache */
            memset(attn_out, 0, (size_t)(n_h * hd) * sizeof(float));
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            gqa_attention(q_buf, k_caches[lay], v_caches[lay], attn_out, att_scratch,
                          total_pos, n_h, n_kv, hd, max_seq, active_kv_window,
                          lay, total_pos);
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            layer_attn_seconds += fpq_elapsed_seconds(&seg_t0, &seg_t1);
            if (golden_step) {
                fpq_golden_emit_vec_stage("attention_context", lay, step, attn_out, (size_t)(n_h * hd), "active-only");
            }
            if (internal_probe_step) {
                fpq_log_layer_internal_probe(lay, "attn_context_before_o_proj", attn_out,
                                             (size_t)(n_h * hd));
            }
            if (inspect_step && lay == 0) {
                fpq_vec_stats_t st = fpq_collect_vec_stats(attn_out, (size_t)(n_h * hd));
                fpq_log_vec_stats("layer0_attn_out", &st);
                if (fpq_check_vec_fatal("layer0_attn_out", &st, 1) != 0) return -1;
            }
            if (inspect_step && lay == 42) {
                fpq_vec_stats_t st = fpq_collect_vec_stats(attn_out, (size_t)(n_h * hd));
                fpq_log_vec_stats("layer42_attn_context", &st);
                /* don't fatal here — may be zeroed by the softmax guard; just record */
            }

            /* Output projection */
            tname(o_name_buf, arch, "self_attn.o_proj.weight", lay);
            if (inspect_step && lay == 42) {
                for (int _i = 0; _i < d; _i++) o_buf[_i] = NAN;
            }
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            if (fpq_matmul(model, o_name_buf, attn_out, o_buf) != 0) {
                fprintf(stderr, "qwen_runtime: FATAL matmul failed tensor=%s layer=%d\n", o_name_buf, lay);
                generated = -1;
                goto cleanup;
            }
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            layer_o_seconds += fpq_elapsed_seconds(&seg_t0, &seg_t1);
            tname(name_buf, arch, "self_attn.o_proj.bias", lay);
            qwen_add_bias_v2(model, name_buf, o_buf, d, lay, "o_proj");
            if (golden_step) {
                fpq_golden_emit_matmul_probe(model, "o_proj", o_name_buf, attn_out, o_buf, (size_t)d, lay, step);
            }

            if (internal_probe_step) {
                fpq_log_layer_internal_probe(lay, "o_proj_out", o_buf, (size_t)d);
            }

            /* Residual: h += o_buf */
            for (int i = 0; i < d; i++) h[i] += o_buf[i];
            if (internal_probe_step) {
                fpq_log_layer_internal_probe(lay, "after_attn_residual", h, (size_t)d);
            }
            if (golden_step) {
                fpq_golden_emit_vec_stage("residual_after_attn", lay, step, h, (size_t)d, "active-only");
            }

            if (inspect_step && lay == 42) {
                int o_nan = 0;
                for (int _i = 0; _i < d; _i++) if (isnan(o_buf[_i])) o_nan++;
                if (o_nan > 0) fprintf(stderr, "layer42_o_proj_incomplete_write o_nan=%d\n", o_nan);
                fpq_vec_stats_t ost = fpq_collect_vec_stats(o_buf, (size_t)d);
                fpq_log_vec_stats("layer42_o_proj", &ost);
                fpq_vec_stats_t rst = fpq_collect_vec_stats(h, (size_t)d);
                fpq_log_vec_stats("layer42_residual_after_attn", &rst);
                /* don't fatal — may be NaN already from attn; note it and continue for MLP substage */
            }

            /* ── Feed-forward (SwiGLU MLP) — run gate and up in parallel ──
             * Both take h_norm as input and write to separate output buffers.
             * Safe to run concurrently: independent reads + independent writes. */
            const float *post_norm_w_eff = post_norm_w;
            float post_norm_repair_buf[5120];
            float post_norm_direct_buf[5120];
            int post_norm_direct_used = 0;
            if (qwen_direct_norm_weights_enabled() && d <= 5120) {
                tname(name_buf, arch, "post_attention_layernorm.weight", lay);
                if (fpq_decode_row(model, name_buf, 0, post_norm_direct_buf) == 0) {
                    qwen_log_norm_direct_compare(lay, "mlp_norm", name_buf, post_norm_w, post_norm_direct_buf, d);
                    post_norm_w_eff = post_norm_direct_buf;
                    post_norm_direct_used = 1;
                } else {
                    fprintf(stderr, "norm_direct_decode_failed layer=%d point=mlp_norm tensor=%s\n", lay, name_buf);
                }
            }
            const char *l1_norm_mode = getenv("BONFYRE_QWEN_REPAIR_L1_MLP_NORM_MODE");
            const char *multi_layers = getenv("BONFYRE_QWEN_REPAIR_MLP_NORM_LAYERS");
            if (!post_norm_direct_used &&
                ((lay == 1 && l1_norm_mode && strcmp(l1_norm_mode, "layer0") == 0) ||
                 qwen_repair_layer_in_csv(lay, multi_layers))) {
                post_norm_w_eff = norm_layers + (size_t)0 * 2 * d + d;
                fprintf(stderr,
                        "norm_repair_applied layer=%d point=mlp_norm mode=layer0_source source_layer=0\n",
                        lay);
            } else if (d <= 5120) {
                post_norm_w_eff = qwen_repair_norm_weight_if_needed(
                    lay, "mlp_norm", post_norm_w, d, post_norm_repair_buf);
            }
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            rms_norm(h_norm, h, post_norm_w_eff, d, eps, 0);
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            layer_norm_seconds += fpq_elapsed_seconds(&seg_t0, &seg_t1);
            if (golden_step) {
                fpq_golden_emit_vec_stage("mlp_norm_out", lay, step, h_norm, (size_t)d, "scalar-rmsnorm");
            }
            if (norm_probe_step) {
                int elem_ids[4];
                int n_elem_ids = 0;
                size_t input_top_idx = 0;
                double input_top_abs = 0.0;
                fpq_find_top_abs_elem(h, (size_t)d, &input_top_idx, &input_top_abs, NULL, NULL, NULL);
                elem_ids[n_elem_ids++] = (int)input_top_idx;
                if (lay == 1) {
                    elem_ids[n_elem_ids++] = 2159;
                    elem_ids[n_elem_ids++] = 3094;
                    elem_ids[n_elem_ids++] = 892;
                }
                fpq_log_norm_probe(lay, "mlp_norm", h, post_norm_w_eff, h_norm, d, eps, elem_ids, n_elem_ids);
                if (lay == 1) {
                    const float *layer0_post_norm_w = norm_layers + d;
                    fpq_log_norm_weight_compare("mlp_norm", layer0_post_norm_w, post_norm_w, d);
                }
            }
            if (internal_probe_step) {
                fpq_log_layer_internal_probe(lay, "mlp_norm_out", h_norm, (size_t)d);
            }
            if (inspect_step && lay == 0) {
                fpq_vec_stats_t st = fpq_collect_vec_stats(h_norm, (size_t)d);
                fpq_log_vec_stats("layer0_post_rmsnorm", &st);
                if (fpq_check_vec_fatal("layer0_post_rmsnorm", &st, 1) != 0) return -1;
            }
            if (inspect_step && lay == 42) {
                fpq_vec_stats_t st = fpq_collect_vec_stats(h_norm, (size_t)d);
                fpq_log_vec_stats("layer42_post_rmsnorm", &st);
                /* don't fatal — MLP path may still reveal something if residual is NaN */
            }

            float *gate_raw_probe = NULL;
            double mlp_gate_up_seconds = 0.0;
            double mlp_activation_seconds = 0.0;
            double mlp_down_seconds = 0.0;
            tname(up_name_buf, arch, "mlp.up_proj.weight", lay);
            tname(gate_name_buf, arch, "mlp.gate_proj.weight", lay);
            if (inspect_step && lay == 42) {
                for (int _i = 0; _i < d_ffn; _i++) up_buf[_i] = NAN;
                for (int _i = 0; _i < d_ffn; _i++) gate_buf[_i] = NAN;
            }
            {
                clock_gettime(CLOCK_MONOTONIC, &seg_t0);
                const char *mlp_names[2] = { gate_name_buf, up_name_buf };
                float *mlp_outs[2] = { gate_buf, up_buf };
                if (fpq_matmul_shared(model, 2, mlp_names, h_norm, mlp_outs) != 0) {
                    fprintf(stderr, "qwen_runtime: FATAL shared matmul failed layer=%d tensors=%s,%s\n",
                            lay, gate_name_buf, up_name_buf);
                    generated = -1;
                    goto cleanup;
                }
                clock_gettime(CLOCK_MONOTONIC, &seg_t1);
                mlp_gate_up_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
                layer_mlp_seconds += mlp_gate_up_seconds;
            }
            if (inspect_step && lay == 42) {
                fpq_log_layer_internal_probe(lay, "gate_proj_out", gate_buf, (size_t)d_ffn);
                fpq_log_layer_internal_probe(lay, "up_proj_out", up_buf, (size_t)d_ffn);
            }
            if (internal_probe_step) {
                fpq_log_layer_internal_probe(lay, "gate_proj_out", gate_buf, (size_t)d_ffn);
                fpq_log_layer_internal_probe(lay, "up_proj_out", up_buf, (size_t)d_ffn);
            }
            if (golden_step) {
                fpq_golden_emit_matmul_probe(model, "mlp_gate_proj", gate_name_buf, h_norm, gate_buf, (size_t)d_ffn, lay, step);
                fpq_golden_emit_matmul_probe(model, "mlp_up_proj", up_name_buf, h_norm, up_buf, (size_t)d_ffn, lay, step);
            }
            if (lay == 1 && step >= prompt_len - 1 && generated == 0 &&
                fpq_mlp_activation_probe_enabled()) {
                gate_raw_probe = (float *)malloc((size_t)d_ffn * sizeof(float));
                if (gate_raw_probe) {
                    memcpy(gate_raw_probe, gate_buf, (size_t)d_ffn * sizeof(float));
                }
            }
            if (inspect_step && lay == 0) {
                fpq_vec_stats_t gst = fpq_collect_vec_stats(gate_buf, (size_t)d_ffn);
                fpq_log_vec_stats("layer0_gate_proj", &gst);
                if (fpq_check_vec_fatal("layer0_gate_proj", &gst, 1) != 0) return -1;

                fpq_vec_stats_t ust = fpq_collect_vec_stats(up_buf, (size_t)d_ffn);
                fpq_log_vec_stats("layer0_up_proj", &ust);
                if (fpq_check_vec_fatal("layer0_up_proj", &ust, 1) != 0) return -1;
            }
            if (inspect_step && lay == 42) {
                int g_nan = 0, u_nan = 0;
                for (int _i = 0; _i < d_ffn; _i++) if (isnan(gate_buf[_i])) g_nan++;
                for (int _i = 0; _i < d_ffn; _i++) if (isnan(up_buf[_i])) u_nan++;
                if (g_nan > 0 || u_nan > 0)
                    fprintf(stderr, "layer42_mlp_incomplete_write gate_nan=%d up_nan=%d\n", g_nan, u_nan);
                fpq_vec_stats_t gst = fpq_collect_vec_stats(gate_buf, (size_t)d_ffn);
                fpq_log_vec_stats("layer42_gate_proj", &gst);
                fpq_vec_stats_t ust = fpq_collect_vec_stats(up_buf, (size_t)d_ffn);
                fpq_log_vec_stats("layer42_up_proj", &ust);
            }

            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            silu_hadamard(gate_buf, up_buf, d_ffn);  /* gate_buf = silu(gate)*up */
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            mlp_activation_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
            if (internal_probe_step) {
                fpq_log_layer_internal_probe(lay, "mlp_activation_out", gate_buf, (size_t)d_ffn);
            }
            if (golden_step) {
                fpq_golden_emit_vec_stage("mlp_activation", lay, step, gate_buf, (size_t)d_ffn, "active-only");
            }
            if (lay == 1 && step >= prompt_len - 1 && generated == 0 &&
                fpq_mlp_activation_probe_enabled() && gate_raw_probe) {
                fpq_mlp_activation_value_probe(model, lay, h_norm, (size_t)d,
                                               gate_raw_probe, up_buf, gate_buf, (size_t)d_ffn);
            }
            free(gate_raw_probe);
            gate_raw_probe = NULL;

            if (inspect_step && lay == 42) {
                fpq_vec_stats_t sst = fpq_collect_vec_stats(gate_buf, (size_t)d_ffn);
                fpq_log_vec_stats("layer42_silu_gate", &sst);
            }

            tname(down_name_buf, arch, "mlp.down_proj.weight", lay);
            if (inspect_step && lay == 42) {
                for (int _i = 0; _i < d; _i++) ffn_out[_i] = NAN;
            }
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            if (fpq_matmul(model, down_name_buf, gate_buf, ffn_out) != 0) {
                fprintf(stderr, "qwen_runtime: FATAL matmul failed tensor=%s layer=%d\n", down_name_buf, lay);
                generated = -1;
                goto cleanup;
            }
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            mlp_down_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
            layer_mlp_seconds += mlp_down_seconds;
            fpq_breakdown_emit_jsonl("BONFYRE_QWEN_MLP_BREAKDOWN_JSONL",
                                     "mlp_decode",
                                     lay,
                                     step,
                                     down_name_buf,
                                     mlp_gate_up_seconds,
                                     mlp_activation_seconds,
                                     mlp_down_seconds,
                                     "\"shared_gate_up\":true");
            if (golden_step) {
                fpq_golden_emit_matmul_probe(model, "mlp_down_proj", down_name_buf, gate_buf, ffn_out, (size_t)d, lay, step);
            }
            if (internal_probe_step) {
                fpq_log_layer_internal_probe(lay, "down_proj_out", ffn_out, (size_t)d);
            }
            if (lay == 1 && step >= prompt_len - 1 && generated == 0 && fpq_mlp_row_probe_enabled()) {
                fpq_mlp_down_row_probe(model, lay, gate_buf, (size_t)d_ffn, ffn_out, (size_t)d);
            }
            if (inspect_step && lay == 0) {
                fpq_vec_stats_t dst = fpq_collect_vec_stats(ffn_out, (size_t)d);
                fpq_log_vec_stats("layer0_down_proj", &dst);
                if (fpq_check_vec_fatal("layer0_down_proj", &dst, 1) != 0) return -1;
            }
            if (inspect_step && lay == 42) {
                int d_nan = 0;
                for (int _i = 0; _i < d; _i++) if (isnan(ffn_out[_i])) d_nan++;
                if (d_nan > 0) fprintf(stderr, "layer42_down_proj_incomplete_write down_nan=%d\n", d_nan);
                fpq_vec_stats_t dst = fpq_collect_vec_stats(ffn_out, (size_t)d);
                fpq_log_vec_stats("layer42_down_proj", &dst);
            }

            /* Residual: h += ffn_out */
            for (int i = 0; i < d; i++) h[i] += ffn_out[i];
            if (internal_probe_step) {
                fpq_log_layer_internal_probe(lay, "after_mlp_residual", h, (size_t)d);
            }
            if (golden_step) {
                fpq_golden_emit_vec_stage("residual_output", lay, step, h, (size_t)d, "active-only");
            }

            if (layer_probe_enabled && step >= prompt_len - 1 && generated == 0 &&
                fpq_should_probe_layer_hidden(lay)) {
                fpq_log_layer_hidden_probe("end_residual", lay, h, (size_t)d);
            }

            if (inspect_step && fpq_layer_probe_match(lay, n_lay)) {
                fpq_vec_stats_t rst = fpq_collect_vec_stats(h, (size_t)d);
                char rname[64];
                snprintf(rname, sizeof(rname), "layer%d_end_residual", lay);
                fpq_log_vec_stats(rname, &rst);
            }

            if (qwen_release_layer_after_decode_enabled()) {
                clock_gettime(CLOCK_MONOTONIC, &seg_t0);
                qwen_release_layer_runtime(model, lay, n_lay, "decode");
                clock_gettime(CLOCK_MONOTONIC, &seg_t1);
                layer_release_seconds += fpq_elapsed_seconds(&seg_t0, &seg_t1);
            }
            qwen_runtime_heartbeat_tick("decode_layer_end", lay, n_lay);
            qwen_release_log_mem_if_enabled("after_decode_layer", lay, n_lay);
            clock_gettime(CLOCK_MONOTONIC, &layer_t1);
            if (lay >= 0 && lay < FPQ_RUN_MAX_LAYERS) {
                state->metrics.decode_layer_total_seconds[lay] += fpq_elapsed_seconds(&layer_t0, &layer_t1);
                state->metrics.decode_layer_qkv_seconds[lay] += layer_qkv_seconds;
                state->metrics.decode_layer_attention_seconds[lay] += layer_attn_seconds;
                state->metrics.decode_layer_o_proj_seconds[lay] += layer_o_seconds;
                state->metrics.decode_layer_norm_seconds[lay] += layer_norm_seconds;
                state->metrics.decode_layer_mlp_seconds[lay] += layer_mlp_seconds;
                state->metrics.decode_layer_kv_write_seconds[lay] += layer_kv_write_seconds;
                state->metrics.decode_layer_release_seconds[lay] += layer_release_seconds;
            }
            state->metrics.kv_write_seconds += layer_kv_write_seconds;
            state->metrics.release_seconds += layer_release_seconds;
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_QKV, layer_qkv_seconds);
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_ATTENTION, layer_attn_seconds);
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_O_PROJ, layer_o_seconds);
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_NORM, layer_norm_seconds);
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_MLP, layer_mlp_seconds);
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_KV_WRITE, layer_kv_write_seconds);
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_RELEASE, layer_release_seconds);

if (qwen_log_layer_progress_enabled()) {
                fprintf(stderr,
                        "layer_progress end step=%d/%d generated=%d layer=%d/%d total_pos=%d\n",
                        step + 1, prompt_len + max_new, generated, lay + 1, n_lay, total_pos);
            }
        }

        total_pos++;
        if (in_prompt_phase && prefill_chunk_size > 1 &&
            (((step + 1) % prefill_chunk_size) == 0 || (step + 1) == prompt_len)) {
            if (chunk_start >= 0 &&
                state->metrics.prefill_chunk_count < FPQ_RUN_MAX_PREFILL_CHUNKS) {
                struct timespec chunk_t1;
                int idx = state->metrics.prefill_chunk_count++;
                clock_gettime(CLOCK_MONOTONIC, &chunk_t1);
                state->metrics.prefill_chunk_seconds[idx] =
                    (chunk_t1.tv_sec - chunk_t0.tv_sec) +
                    (chunk_t1.tv_nsec - chunk_t0.tv_nsec) / 1e9;
                state->metrics.prefill_chunk_cache_hits[idx] =
                    active_cache ? (uint64_t)(active_cache->hits - chunk_cache_hits_base) : 0;
                state->metrics.prefill_chunk_tokens[idx] = (step + 1) - chunk_start;
            }
            chunk_start = -1;
            if (debug_enabled) {
                fprintf(stderr,
                        "flashqla_prefill_chunk_commit: tokens=%d cached=%d\n",
                        step + 1,
                        total_pos);
            }
        }

        /* Skip lm_head during prompt prefill — only need logits for the
         * last prompt token and every generated token. */
        if (step < prompt_len - 1) {
            clock_gettime(CLOCK_MONOTONIC, &step_t1);
            state->metrics.prefill_wall_seconds +=
                (step_t1.tv_sec - step_t0.tv_sec) +
                (step_t1.tv_nsec - step_t0.tv_nsec) / 1e9;
            continue;
        }

        /* Final norm + LM head → logits */
        if (inspect_step) {
            fpq_vec_stats_t pre = fpq_collect_vec_stats(h, (size_t)d);
            fpq_log_vec_stats("pre_final_residual", &pre);

            fpq_vec_stats_t wst = fpq_collect_vec_stats(final_norm, (size_t)d);
            fpq_log_vec_stats("final_norm_weight", &wst);

            if (fpq_check_vec_fatal("pre_final_residual", &pre, 1) != 0) return -1;
            if (fpq_check_vec_fatal("final_norm_weight", &wst, 1) != 0) return -1;
        }
        if (step >= prompt_len - 1 && generated == 0) {
            if (layer_probe_enabled) {
                fpq_log_layer_hidden_probe("final_pre_norm", -1, h, (size_t)d);
            }
            fprintf(stderr, "hidden_norm_before_final_norm: step=%d l2=%g\n",
                    step, fpq_vec_l2_norm(h, (size_t)d));
        }

        {
            struct timespec seg_t0 = {0};
            struct timespec seg_t1 = {0};
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            rms_norm(h_norm, h, final_norm, d, eps, inspect_step ? 1 : 0);
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_NORM, fpq_elapsed_seconds(&seg_t0, &seg_t1));
        }
        if (golden_step) {
            fpq_golden_emit_vec_stage("final_norm", -1, step, h_norm, (size_t)d, "scalar-rmsnorm");
        }
        if (inspect_step) {
            fpq_vec_stats_t st = fpq_collect_vec_stats(h_norm, (size_t)d);
            fpq_log_vec_stats("post_final_norm", &st);
            if (fpq_check_vec_fatal("post_final_norm", &st, 1) != 0) return -1;
        }
        if (step >= prompt_len - 1 && generated == 0) {
            if (layer_probe_enabled) {
                fpq_log_layer_hidden_probe("final_post_norm", -1, h_norm, (size_t)d);
            }
            fprintf(stderr, "hidden_norm_after_final_norm: step=%d l2=%g\n",
                    step, fpq_vec_l2_norm(h_norm, (size_t)d));
        }

        snprintf(name_buf, sizeof(name_buf), "%s", fpq_cfg_lm_head_tensor_name(cfg));
        for (int i = 0; i < cfg->n_vocab; i++) logits[i] = NAN;
        double lm_head_matvec_seconds = 0.0;
        double lm_head_select_seconds = 0.0;
        {
            struct timespec seg_t0 = {0};
            struct timespec seg_t1 = {0};
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
            if (fpq_matmul(model, name_buf, h_norm, logits) != 0) {
                fprintf(stderr, "qwen_runtime: FATAL matmul failed tensor=%s layer=%d\n", name_buf, -1);
                generated = -1;
                goto cleanup;
            }
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            {
                lm_head_matvec_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
                state->metrics.lm_head_seconds += lm_head_matvec_seconds;
                fpq_metrics_add_family(&state->metrics, FPQ_FAMILY_LM_HEAD, lm_head_matvec_seconds);
            }
        }
        int remaining_sentinel = 0;
        for (int i = 0; i < cfg->n_vocab; i++) {
            if (isnan(logits[i])) remaining_sentinel++;
        }
        if (remaining_sentinel > 0) {
            int finite_logits = 0;
            for (int i = 0; i < cfg->n_vocab; i++) {
                if (isfinite(logits[i])) finite_logits++;
            }
            fprintf(stderr,
                    "qwen_runtime: FATAL lm_head incomplete write finite=%d n_vocab=%d\n",
                    finite_logits, cfg->n_vocab);
            return -1;
        }

        if (inspect_step) {
            fpq_vec_stats_t lst = fpq_collect_vec_stats(logits, (size_t)sample_n_vocab);
            fpq_log_vec_stats("lm_head_logits", &lst);
            if (lst.finite == 0) {
                fprintf(stderr, "qwen_runtime: FATAL logits finite==0\n");
                return -1;
            }
            if (fabs((double)lst.max_v - (double)lst.min_v) < 1e-8) {
                fprintf(stderr, "qwen_runtime: FATAL logits all equal\n");
                return -1;
            }
        }
        if (golden_step) {
            fpq_golden_emit_vec_stage("logits", -1, step, logits, (size_t)sample_n_vocab, "active-matmul");
            fpq_golden_emit_logits_probe(model, cfg, h_norm, logits, sample_n_vocab, step);
            golden_step_done = 1;
        }

        if (step >= prompt_len - 1 && generated == 0) {
            float min_logit = 0.0f;
            float max_logit = 0.0f;
            int non_finite = 0;
            fpq_log_topk_logits(logits, sample_n_vocab, 20, cfg, step);
            fpq_lm_head_row_probe(model, h_norm, logits, sample_n_vocab, cfg, step);
            if (fpq_logits_collapsed(logits, sample_n_vocab,
                                     &min_logit, &max_logit, &non_finite)) {
                if (fpq_bootstrap_zero_matmul_enabled()) {
                    fprintf(stderr,
                            "qwen_runtime: bootstrap collapsed logits tolerated (min=%g max=%g non_finite=%d)\n",
                            (double)min_logit, (double)max_logit, non_finite);
                } else {
                    fprintf(stderr,
                            "qwen_runtime: FATAL collapsed logits (min=%g max=%g non_finite=%d)\n",
                            (double)min_logit, (double)max_logit, non_finite);
                    return -1;
                }
            }
        }

        /* ── Sampling ── */
        {
            struct timespec seg_t0 = {0};
            struct timespec seg_t1 = {0};
            clock_gettime(CLOCK_MONOTONIC, &seg_t0);
        if (cfg->temperature > 0.0f && !cfg->greedy) {
            for (int i = 0; i < sample_n_vocab; i++)
                logits[i] /= cfg->temperature;
            softmax(logits, sample_n_vocab);
            next_token = sample_top_p(logits, sample_n_vocab,
                                      cfg->top_p, &rng, state);
        } else {
            int best = 0;
            for (int i = 1; i < sample_n_vocab; i++)
                if (logits[i] > logits[best]) best = i;
            next_token = best;
        }

        /* Avoid empty completions from immediate EOS on the first emitted token.
         * If EOS is the top token before anything is generated, fall back to the
         * best non-EOS token so file generations are not silently empty. */
        if (step >= prompt_len - 1 && generated == 0 &&
            next_token == fpq_cfg_primary_stop_token(cfg)) {
            int best_non_eos = -1;
            for (int i = 0; i < sample_n_vocab; i++) {
                if (fpq_cfg_is_stop_token(cfg, i)) continue;
                if (best_non_eos < 0 || logits[i] > logits[best_non_eos]) {
                    best_non_eos = i;
                }
            }
            if (best_non_eos >= 0) next_token = best_non_eos;
        }
            clock_gettime(CLOCK_MONOTONIC, &seg_t1);
            lm_head_select_seconds = fpq_elapsed_seconds(&seg_t0, &seg_t1);
        }
        fpq_breakdown_emit_jsonl("BONFYRE_QWEN_LM_HEAD_BREAKDOWN_JSONL",
                                 "lm_head",
                                 -1,
                                 step,
                                 name_buf,
                                 lm_head_matvec_seconds,
                                 lm_head_select_seconds,
                                 (double)remaining_sentinel,
                                 NULL);

        /* Emit generated tokens (not prompt tokens) */
        if (step >= prompt_len - 1) {
            if (generated == 0) {
                const char *decoded = "";
                if (cfg->debug_id_to_str) {
                    decoded = cfg->debug_id_to_str(cfg->debug_decode_ctx, next_token);
                    if (!decoded) decoded = "";
                }
                fprintf(stderr, "first_generated_token: token_id=%d decoded=\"%s\"\n",
                        next_token, decoded);
            }
            if (fpq_cfg_is_stop_token(cfg, next_token)) {
                clock_gettime(CLOCK_MONOTONIC, &step_t1);
                if (in_prompt_phase) {
                    state->metrics.prefill_wall_seconds +=
                        (step_t1.tv_sec - step_t0.tv_sec) +
                        (step_t1.tv_nsec - step_t0.tv_nsec) / 1e9;
                } else {
                    state->metrics.decode_wall_seconds +=
                        (step_t1.tv_sec - step_t0.tv_sec) +
                        (step_t1.tv_nsec - step_t0.tv_nsec) / 1e9;
                }
                break;  /* </s> */
            }
            if (callback)
                callback(next_token, cb_data);
            generated++;
            state->metrics.generated_tokens = generated;
            if (generated >= max_new) {
                clock_gettime(CLOCK_MONOTONIC, &step_t1);
                if (in_prompt_phase) {
                    state->metrics.prefill_wall_seconds +=
                        (step_t1.tv_sec - step_t0.tv_sec) +
                        (step_t1.tv_nsec - step_t0.tv_nsec) / 1e9;
                } else {
                    state->metrics.decode_wall_seconds +=
                        (step_t1.tv_sec - step_t0.tv_sec) +
                        (step_t1.tv_nsec - step_t0.tv_nsec) / 1e9;
                }
                break;
            }
        }

        clock_gettime(CLOCK_MONOTONIC, &step_t1);
        if (in_prompt_phase) {
            state->metrics.prefill_wall_seconds +=
                (step_t1.tv_sec - step_t0.tv_sec) +
                (step_t1.tv_nsec - step_t0.tv_nsec) / 1e9;
        } else {
            state->metrics.decode_wall_seconds +=
                (step_t1.tv_sec - step_t0.tv_sec) +
                (step_t1.tv_nsec - step_t0.tv_nsec) / 1e9;
        }
    }

cleanup:
    state->metrics.generated_tokens = generated > 0 ? generated : 0;
    state->metrics.total_tokens = prompt_len + (generated > 0 ? generated : 0);
    return generated;
}

/* ═══════════════════════════════════════════════════════
 * fpq_run_load_norms — load RMS norm weights from model
 *
 * Fetches passthrough (1D) tensors from libfpq via decode.
 * Returns malloc'd array [n_layers × 2 × d_model].
 * ═══════════════════════════════════════════════════════ */

float *fpq_run_load_norms(fpq_model_t *model, const fpq_run_config_t *cfg) {
    int d = cfg->d_model;
    int n = cfg->n_layers;
    float *buf = (float *)calloc((size_t)n * 2 * d, sizeof(float));
    char name_buf[FPQ_RUN_NAME_BUF];

    for (int lay = 0; lay < n; lay++) {
        tname(name_buf, cfg->arch, "input_layernorm.weight", lay);
        fpq_decode_one(model, name_buf, buf + (size_t)lay * 2 * d);

        tname(name_buf, cfg->arch, "post_attention_layernorm.weight", lay);
        fpq_decode_one(model, name_buf, buf + (size_t)lay * 2 * d + d);
    }
    return buf;
}

float *fpq_run_load_final_norm(fpq_model_t *model, const fpq_run_config_t *cfg) {
    float *buf = (float *)calloc((size_t)cfg->d_model, sizeof(float));
    fpq_decode_one(model, fpq_cfg_final_norm_tensor_name(cfg), buf);
    return buf;
}

/* Load embedding table: decode embed_tokens → [n_vocab × d_model].
 * This is the only at-start decode; ~256 MB for 32K × 2048 fp32. */
float *fpq_run_load_embeddings(fpq_model_t *model, const fpq_run_config_t *cfg) {
    (void)model;
    (void)cfg;

    /* Qwen 14B embeddings are 152064 × 5120. Do not full-decode them at startup.
     * Return a NAN sentinel so the generation loop uses fpq_decode_row() per token.
     */
    float *sentinel = (float *)malloc(sizeof(float));
    if (!sentinel) {
        fprintf(stderr, "fpq_run: OOM for lazy embedding sentinel\n");
        return NULL;
    }
    *sentinel = NAN;
    fprintf(stderr, "fpq_run: lazy embeddings enabled; skipped full %s decode\n",
            fpq_cfg_embed_tensor_name(cfg));
    return sentinel;
}


/* ═══════════════════════════════════════════════════════
 * cmd_run — top-level entry point called from fpq_cli.c
 * ═══════════════════════════════════════════════════════ */

/* Token callback: stream to stdout */
static void stream_token(int token_id, void *data) {
    tokenizer_t *tok = (tokenizer_t *)data;
    const char *s = tok_id_to_str(tok, token_id);
    if (!s || !*s) return;

    /* Convert ▁ → space for display */
    while (*s) {
        if ((unsigned char)s[0] == 0xE2 &&
            (unsigned char)s[1] == 0x96 &&
            (unsigned char)s[2] == 0x81) {
            putchar(' ');
            s += 3;
        } else {
            putchar((unsigned char)*s);
            s++;
        }
    }
    fflush(stdout);
}

int cmd_run(int argc, char **argv) {
#ifdef _OPENMP
    /* Use 4 performance cores; avoid over-subscribing with Accelerate threads */
    omp_set_num_threads(4);
    omp_set_dynamic(0);
#endif
    /* Parse args:
     *   fpq run <model.fpq> "prompt"
     *            [--tokenizer <path>]
     *            [--sys "system prompt"]
     *            [--max-tokens N]
     *            [--temp F]
     *            [--top-p F]
     *            [--greedy]
     *            [--no-chat]   (skip chat template)
     */
    if (argc < 4) {
        fprintf(stderr,
            "Usage: fpq run <model.fpq> \"prompt\" [options]\n"
            "\n"
            "Options:\n"
            "  --tokenizer <path>   Path to tokenizer.json (default: model dir)\n"
            "  --sys \"text\"         System prompt\n"
            "  --max-tokens N       Max tokens to generate (default: 512)\n"
            "  --temp F             Temperature 0.0–2.0 (default: 0.6)\n"
            "  --top-p F            Top-p nucleus sampling (default: 0.9)\n"
            "  --greedy             Greedy decoding (overrides temp)\n"
            "  --no-chat            Don't apply chat template\n"
            "\n"
            "Example:\n"
            "  fpq run models/tinyllama-v12/model.fpq \"What is 2+2?\"\n");
        return 1;
    }

    const char *model_path = argv[2];
    const char *prompt     = NULL;
    const char *sys_prompt = "";  /* empty = user/assistant only (avoids EOS from system </s>) */
    const char *tok_path   = NULL;
    int max_new_tokens     = 512;
    float temperature      = 0.6f;
    float top_p            = 0.9f;
    int greedy             = 0;
    int no_chat            = 0;

    /* Parse positional prompt + options */
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--tokenizer") == 0 && i+1 < argc)
            tok_path = argv[++i];
        else if (strcmp(argv[i], "--sys") == 0 && i+1 < argc)
            sys_prompt = argv[++i];
        else if (strcmp(argv[i], "--max-tokens") == 0 && i+1 < argc)
            max_new_tokens = atoi(argv[++i]);
        else if (strcmp(argv[i], "--temp") == 0 && i+1 < argc)
            temperature = (float)atof(argv[++i]);
        else if (strcmp(argv[i], "--top-p") == 0 && i+1 < argc)
            top_p = (float)atof(argv[++i]);
        else if (strcmp(argv[i], "--greedy") == 0)
            greedy = 1;
        else if (strcmp(argv[i], "--no-chat") == 0)
            no_chat = 1;
        else if (argv[i][0] != '-')
            prompt = argv[i];
    }

    if (!prompt) {
        fprintf(stderr, "fpq run: no prompt provided\n");
        return 1;
    }

    /* Auto-discover tokenizer.json next to model.fpq */
    char auto_tok_path[2048];
    if (!tok_path) {
        /* Try same directory as model */
        strncpy(auto_tok_path, model_path, sizeof(auto_tok_path) - 32);
        char *slash = strrchr(auto_tok_path, '/');
        if (slash) {
            strcpy(slash + 1, "tokenizer.json");
        } else {
            strcpy(auto_tok_path, "tokenizer.json");
        }
        tok_path = auto_tok_path;
    }

    /* Load tokenizer */
    fprintf(stderr, "Loading tokenizer: %s\n", tok_path);
    tokenizer_t *tok = tok_load(tok_path);
    if (!tok) {
        fprintf(stderr, "fpq run: failed to load tokenizer from %s\n", tok_path);
        fprintf(stderr, "         Try: --tokenizer /path/to/tokenizer.json\n");
        return 1;
    }

    /* Load model */
    fprintf(stderr, "Loading model: %s\n", model_path);
    double t0 = (double)clock() / CLOCKS_PER_SEC;
    fpq_model_t *model = fpq_open(model_path);
    if (!model) { tok_free(tok); return 1; }
    double t_load = (double)clock() / CLOCKS_PER_SEC - t0;
    fprintf(stderr, "Model loaded in %.1fs\n", t_load);

    /* Build config (TODO: read from model metadata / config.json sidecar) */
    fpq_run_config_t cfg = fpq_run_default_config();
    cfg.max_new_tokens = max_new_tokens;
    cfg.temperature    = temperature;
    cfg.top_p          = top_p;
    cfg.greedy         = greedy;

    /* Load norm weights (passthrough tensors — fast) */
    fprintf(stderr, "Loading norms + embeddings...\n");
    float *norms     = fpq_run_load_norms(model, &cfg);
    float *final_norm= fpq_run_load_final_norm(model, &cfg);
    float *embeddings= fpq_run_load_embeddings(model, &cfg);
    fpq_run_state_t *state = fpq_run_state_create(&cfg, NULL, NULL);

    if (!norms || !final_norm || !embeddings || !state) {
        fprintf(stderr, "fpq run: failed to load model tensors\n");
        fpq_close(model); tok_free(tok);
        free(norms); free(final_norm); free(embeddings); fpq_run_state_free(state);
        return 1;
    }

    /* Build prompt with chat template (TinyLlama-Chat-v1.0 Zephyr format)
     * Full template: <|system|>\n{sys}</s>\n<|user|>\n{user}</s>\n<|assistant|>\n
     * Without system: <|user|>\n{user}</s>\n<|assistant|>\n
     * Verified: tok.apply_chat_template(messages, add_generation_prompt=True)
     * Note: skip system turn when sys_prompt is empty to avoid spurious EOS. */
    char *full_prompt = NULL;
    if (!no_chat) {
        size_t len = strlen(sys_prompt) + strlen(prompt) + 64;
        full_prompt = (char *)malloc(len);
        if (sys_prompt[0]) {
            /* Full format with system message */
            snprintf(full_prompt, len,
                "<|system|>\n%s</s>\n<|user|>\n%s</s>\n<|assistant|>\n",
                sys_prompt, prompt);
        } else {
            /* No system message — user/assistant only */
            snprintf(full_prompt, len,
                "<|user|>\n%s</s>\n<|assistant|>\n",
                prompt);
        }
    } else {
        full_prompt = strdup(prompt);
    }

    /* Tokenize */
    int n_tokens = 0;
    int *ids = tok_encode(tok, full_prompt, 1 /* add BOS */, &n_tokens);
    free(full_prompt);

    if (!ids || n_tokens == 0) {
        fprintf(stderr, "fpq run: tokenization failed\n");
        fpq_close(model); tok_free(tok);
        free(norms); free(final_norm); free(embeddings);
        return 1;
    }

    fprintf(stderr, "Prompt: %d tokens → generating (max %d)...\n",
            n_tokens, max_new_tokens);

    /* Run generation */
    double t1 = (double)clock() / CLOCKS_PER_SEC;
    int generated = fpq_run_generate(
        model, embeddings, norms, final_norm,
        ids, n_tokens, &cfg, state,
        stream_token, tok);
    double t_gen = (double)clock() / CLOCKS_PER_SEC - t1;

    printf("\n");
    if (generated > 0 && t_gen > 0)
        fprintf(stderr, "\n%.1f tok/s (%d tokens in %.1fs)\n",
                (float)generated / t_gen, generated, t_gen);

    free(ids);
    fpq_run_state_free(state);
    free(norms);
    free(final_norm);
    free(embeddings);
    fpq_close(model);
    tok_free(tok);
    return 0;
}
