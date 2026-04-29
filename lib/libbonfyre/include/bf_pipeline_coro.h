/*
 * bf_pipeline_coro.h — libdill structured concurrency for pipeline stages
 *
 * Replaces fork()+waitpid() in BonfyrePipeline with lightweight
 * coroutines + typed channels. Benefits:
 *   - No OS process overhead (context-switch ~50ns vs ~5μs for fork)
 *   - Structured: parent bundles children, never leaks
 *   - Typed channels for stage→stage data flow
 *   - Built-in deadlines via libdill's deadline() API
 */

#ifndef BF_PIPELINE_CORO_H
#define BF_PIPELINE_CORO_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Stage result ──────────────────────────────────────────── */

typedef struct {
    int         rc;            /* 0 = success, >0 = error */
    int64_t     elapsed_ns;    /* Wall-clock nanoseconds */
    size_t      bytes_in;      /* Input bytes consumed */
    size_t      bytes_out;     /* Output bytes produced */
    const char *stage_name;    /* E.g. "ingest", "index", "meter" */
} bf_stage_result_t;

/* ── Stage function signature ─────────────────────────────── */

typedef bf_stage_result_t (*bf_stage_fn)(void *ctx);

/* ── Stage descriptor ─────────────────────────────────────── */

typedef struct {
    const char  *name;         /* Human-readable stage name */
    bf_stage_fn  fn;           /* Coroutine entry point */
    void        *ctx;          /* Stage-specific context */
    int64_t      deadline_ms;  /* Timeout (0 = no deadline) */
} bf_stage_desc_t;

/* ── Pipeline handle (opaque) ─────────────────────────────── */

typedef struct bf_pipeline bf_pipeline_t;

/* ── Lifecycle ────────────────────────────────────────────── */

/*
 * Create a pipeline from an array of stage descriptors.
 * Stages are logically grouped in a libdill bundle:
 *   - If any stage fails, the bundle can be cancelled.
 *   - channel_mode controls data passing between stages.
 *
 * channel_mode:
 *   0 = independent (stages run in parallel, no data flow)
 *   1 = sequential  (each stage starts after previous completes)
 *   2 = chain       (typed channel connects stage[i] → stage[i+1])
 */
bf_pipeline_t *bf_pipeline_new(const bf_stage_desc_t *stages, int n_stages,
                                int channel_mode);

/*
 * Launch all stages as coroutines, wait for completion.
 * Returns 0 if all stages succeeded, or the first non-zero rc.
 */
int bf_pipeline_run(bf_pipeline_t *p);

/*
 * Retrieve result for stage `i` (0-based).
 * Valid only after bf_pipeline_run() returns.
 */
const bf_stage_result_t *bf_pipeline_result(const bf_pipeline_t *p, int i);

/*
 * Free all pipeline resources.
 */
void bf_pipeline_free(bf_pipeline_t *p);

/* ── Channel helpers ──────────────────────────────────────── */

/*
 * Generic typed channel for stage→stage data passing.
 * Backed by libdill ch() or fallback pipe.
 *
 * bf_chan_t is opaque; use bf_chan_send/bf_chan_recv to move
 * fixed-size items between coroutines.
 */
typedef struct bf_chan bf_chan_t;

bf_chan_t *bf_chan_new(size_t item_size, int capacity);
int        bf_chan_send(bf_chan_t *ch, const void *item, int64_t deadline_ms);
int        bf_chan_recv(bf_chan_t *ch, void *item, int64_t deadline_ms);
void       bf_chan_close(bf_chan_t *ch);
void       bf_chan_free(bf_chan_t *ch);

/* ── Convenience: parallel map ────────────────────────────── */

/*
 * Run `n` instances of `fn` in parallel with different contexts.
 * Waits for all to finish (or timeout).
 * Results written to `results[n]`.
 */
int bf_pipeline_parallel(bf_stage_fn fn, void **contexts, int n,
                          int64_t deadline_ms, bf_stage_result_t *results);

#ifdef __cplusplus
}
#endif

#endif /* BF_PIPELINE_CORO_H */
