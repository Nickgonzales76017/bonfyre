#include "bench_metrics.h"

#include <stdlib.h>
#include <string.h>

static BenchMetrics g_bench_metrics;
static int g_bench_metrics_enabled = 0;

static void reset_latency_metric(BenchLatencyMetric *metric) {
    if (!metric) return;
    metric->count = 0;
    metric->total_ms = 0.0;
    metric->total_bytes = 0;
    metric->total_steps = 0;
    metric->sample_count = 0;
}

static void reset_counter_metric(BenchCounterMetric *metric) {
    if (!metric) return;
    metric->calls = 0;
    metric->total = 0;
}

static void append_sample(BenchLatencyMetric *metric, double elapsed_ms) {
    double *next;
    int new_cap;

    if (!metric) return;
    if (metric->sample_count >= metric->sample_cap) {
        new_cap = metric->sample_cap ? metric->sample_cap * 2 : 64;
        next = realloc(metric->samples_ms, (size_t)new_cap * sizeof(*next));
        if (!next) return;
        metric->samples_ms = next;
        metric->sample_cap = new_cap;
    }
    metric->samples_ms[metric->sample_count++] = elapsed_ms;
}

static int cmp_double_asc(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

void bench_metrics_enable(int enabled) {
    g_bench_metrics_enabled = enabled ? 1 : 0;
}

int bench_metrics_is_enabled(void) {
    return g_bench_metrics_enabled;
}

void bench_metrics_reset(void) {
    reset_latency_metric(&g_bench_metrics.point_get);
    reset_latency_metric(&g_bench_metrics.point_list);
    reset_counter_metric(&g_bench_metrics.reduce_full_cell_reads);
    reset_counter_metric(&g_bench_metrics.reduce_partial_cell_reads);

    g_bench_metrics.tensor_filter.calls = 0;
    g_bench_metrics.tensor_filter.result_total = 0;
    g_bench_metrics.tensor_filter.fullscan_steps_total = 0;

    reset_counter_metric(&g_bench_metrics.tensor_agg_vm_steps);

    g_bench_metrics.hybrid_auto.calls = 0;
    g_bench_metrics.hybrid_auto.memo_hits = 0;
    g_bench_metrics.hybrid_auto.recommended_total = 0;
    g_bench_metrics.hybrid_auto.recommended_partial = 0;

    g_bench_metrics.ann_exact.calls = 0;
    g_bench_metrics.ann_exact.candidate_count_total = 0;

    g_bench_metrics.ann_quant.calls = 0;
    g_bench_metrics.ann_quant.rerank_shortlist_total = 0;

    g_bench_metrics.symbolic_update.calls = 0;
    g_bench_metrics.symbolic_update.bindings_rebuilt_bytes_total = 0;

    g_bench_metrics.family_repack.families = 0;
    g_bench_metrics.family_repack.bytes_written_total = 0;
}

void bench_metrics_cleanup(void) {
    free(g_bench_metrics.point_get.samples_ms);
    free(g_bench_metrics.point_list.samples_ms);
    memset(&g_bench_metrics, 0, sizeof(g_bench_metrics));
}

const BenchMetrics *bench_metrics_current(void) {
    return &g_bench_metrics;
}

double bench_latency_metric_p95(const BenchLatencyMetric *metric) {
    double *copy;
    double value;
    int idx;

    if (!metric || metric->sample_count <= 0 || !metric->samples_ms) return 0.0;

    copy = malloc((size_t)metric->sample_count * sizeof(*copy));
    if (!copy) return 0.0;
    memcpy(copy, metric->samples_ms, (size_t)metric->sample_count * sizeof(*copy));
    qsort(copy, (size_t)metric->sample_count, sizeof(*copy), cmp_double_asc);

    idx = (int)((metric->sample_count - 1) * 0.95);
    if (idx < 0) idx = 0;
    if (idx >= metric->sample_count) idx = metric->sample_count - 1;
    value = copy[idx];
    free(copy);
    return value;
}

void bench_metrics_record_point_get(double elapsed_ms, sqlite3_int64 bytes_touched) {
    BenchLatencyMetric *metric;
    if (!g_bench_metrics_enabled) return;
    metric = &g_bench_metrics.point_get;
    metric->count++;
    metric->total_ms += elapsed_ms;
    metric->total_bytes += bytes_touched;
    append_sample(metric, elapsed_ms);
}

void bench_metrics_record_point_list(double elapsed_ms, sqlite3_int64 vm_steps) {
    BenchLatencyMetric *metric;
    if (!g_bench_metrics_enabled) return;
    metric = &g_bench_metrics.point_list;
    metric->count++;
    metric->total_ms += elapsed_ms;
    metric->total_steps += vm_steps;
    append_sample(metric, elapsed_ms);
}

void bench_metrics_record_reduce_full(int cell_reads) {
    if (!g_bench_metrics_enabled) return;
    g_bench_metrics.reduce_full_cell_reads.calls++;
    g_bench_metrics.reduce_full_cell_reads.total += cell_reads;
}

void bench_metrics_record_reduce_partial(int cell_reads) {
    if (!g_bench_metrics_enabled) return;
    g_bench_metrics.reduce_partial_cell_reads.calls++;
    g_bench_metrics.reduce_partial_cell_reads.total += cell_reads;
}

void bench_metrics_record_tensor_filter(int result_count, sqlite3_int64 fullscan_steps) {
    if (!g_bench_metrics_enabled) return;
    g_bench_metrics.tensor_filter.calls++;
    g_bench_metrics.tensor_filter.result_total += result_count;
    g_bench_metrics.tensor_filter.fullscan_steps_total += fullscan_steps;
}

void bench_metrics_record_tensor_agg(sqlite3_int64 vm_steps) {
    if (!g_bench_metrics_enabled) return;
    g_bench_metrics.tensor_agg_vm_steps.calls++;
    g_bench_metrics.tensor_agg_vm_steps.total += vm_steps;
}

void bench_metrics_record_hybrid_memo_hit(void) {
    if (!g_bench_metrics_enabled) return;
    g_bench_metrics.hybrid_auto.calls++;
    g_bench_metrics.hybrid_auto.memo_hits++;
}

void bench_metrics_record_hybrid_strategy(int recommended_strategy) {
    if (!g_bench_metrics_enabled) return;
    g_bench_metrics.hybrid_auto.calls++;
    g_bench_metrics.hybrid_auto.recommended_total++;
    if (recommended_strategy == 1) g_bench_metrics.hybrid_auto.recommended_partial++;
}

void bench_metrics_record_ann_exact(int candidate_count) {
    if (!g_bench_metrics_enabled) return;
    g_bench_metrics.ann_exact.calls++;
    g_bench_metrics.ann_exact.candidate_count_total += candidate_count;
}

void bench_metrics_record_ann_quant(int rerank_shortlist) {
    if (!g_bench_metrics_enabled) return;
    g_bench_metrics.ann_quant.calls++;
    g_bench_metrics.ann_quant.rerank_shortlist_total += rerank_shortlist;
}

void bench_metrics_record_symbolic_update(sqlite3_int64 rebuilt_bytes) {
    if (!g_bench_metrics_enabled) return;
    g_bench_metrics.symbolic_update.calls++;
    g_bench_metrics.symbolic_update.bindings_rebuilt_bytes_total += rebuilt_bytes;
}

void bench_metrics_record_family_repack(sqlite3_int64 bytes_written) {
    if (!g_bench_metrics_enabled) return;
    g_bench_metrics.family_repack.families++;
    g_bench_metrics.family_repack.bytes_written_total += bytes_written;
}
