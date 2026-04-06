#ifndef BENCH_METRICS_H
#define BENCH_METRICS_H

#include <sqlite3.h>

typedef struct {
    int count;
    double total_ms;
    sqlite3_int64 total_bytes;
    sqlite3_int64 total_steps;
    double *samples_ms;
    int sample_count;
    int sample_cap;
} BenchLatencyMetric;

typedef struct {
    int calls;
    sqlite3_int64 total;
} BenchCounterMetric;

typedef struct {
    int calls;
    sqlite3_int64 result_total;
    sqlite3_int64 fullscan_steps_total;
} BenchTensorFilterMetric;

typedef struct {
    int calls;
    int memo_hits;
    int recommended_total;
    int recommended_partial;
} BenchHybridMetric;

typedef struct {
    int calls;
    sqlite3_int64 candidate_count_total;
} BenchAnnExactMetric;

typedef struct {
    int calls;
    sqlite3_int64 rerank_shortlist_total;
} BenchAnnQuantMetric;

typedef struct {
    int calls;
    sqlite3_int64 bindings_rebuilt_bytes_total;
} BenchSymbolicUpdateMetric;

typedef struct {
    int families;
    sqlite3_int64 bytes_written_total;
} BenchFamilyRepackMetric;

typedef struct {
    BenchLatencyMetric point_get;
    BenchLatencyMetric point_list;
    BenchCounterMetric reduce_full_cell_reads;
    BenchCounterMetric reduce_partial_cell_reads;
    BenchTensorFilterMetric tensor_filter;
    BenchCounterMetric tensor_agg_vm_steps;
    BenchHybridMetric hybrid_auto;
    BenchAnnExactMetric ann_exact;
    BenchAnnQuantMetric ann_quant;
    BenchSymbolicUpdateMetric symbolic_update;
    BenchFamilyRepackMetric family_repack;
} BenchMetrics;

void bench_metrics_enable(int enabled);
int bench_metrics_is_enabled(void);
void bench_metrics_reset(void);
void bench_metrics_cleanup(void);

const BenchMetrics *bench_metrics_current(void);
double bench_latency_metric_p95(const BenchLatencyMetric *metric);

void bench_metrics_record_point_get(double elapsed_ms, sqlite3_int64 bytes_touched);
void bench_metrics_record_point_list(double elapsed_ms, sqlite3_int64 vm_steps);
void bench_metrics_record_reduce_full(int cell_reads);
void bench_metrics_record_reduce_partial(int cell_reads);
void bench_metrics_record_tensor_filter(int result_count, sqlite3_int64 fullscan_steps);
void bench_metrics_record_tensor_agg(sqlite3_int64 vm_steps);
void bench_metrics_record_hybrid_memo_hit(void);
void bench_metrics_record_hybrid_strategy(int recommended_strategy);
void bench_metrics_record_ann_exact(int candidate_count);
void bench_metrics_record_ann_quant(int rerank_shortlist);
void bench_metrics_record_symbolic_update(sqlite3_int64 rebuilt_bytes);
void bench_metrics_record_family_repack(sqlite3_int64 bytes_written);

#endif /* BENCH_METRICS_H */
