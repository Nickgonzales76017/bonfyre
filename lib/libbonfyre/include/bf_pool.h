/*
 * bf_pool.h — Work-stealing thread pool
 *
 * Lock-free per-worker deques with C11 atomics.
 * Idle workers steal from random peers.
 *
 * Usage:
 *   bf_pool_t *pool = bf_pool_create(0);    // 0 = auto-detect cores
 *   bf_pool_submit(pool, my_func, my_arg);
 *   bf_pool_map(pool, process_item, items, n, sizeof(items[0]));
 *   bf_pool_wait(pool);                     // block until all done
 *   bf_pool_destroy(pool);
 */

#ifndef BF_POOL_H
#define BF_POOL_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Types ───────────────────────────────── */

typedef struct bf_pool bf_pool_t;

/* Task function signature */
typedef void (*bf_pool_fn)(void *arg);

/* ── Lifecycle ───────────────────────────── */

/*
 * Create a thread pool with `n_threads` workers.
 * Pass 0 to auto-detect (sysconf or hw.ncpu, capped at 64).
 * Returns NULL on failure.
 */
bf_pool_t *bf_pool_create(int n_threads);

/*
 * Drain pending tasks and join all worker threads.
 * Blocks until everything is finished.
 */
void bf_pool_destroy(bf_pool_t *pool);

/* ── Submit work ─────────────────────────── */

/*
 * Submit a single task. Returns 0 on success, -1 if the pool is shutting down.
 * The task is pushed to the submitter-affine deque (or thread 0 if called
 * from a non-pool thread).
 */
int bf_pool_submit(bf_pool_t *pool, bf_pool_fn fn, void *arg);

/*
 * Parallel map: calls fn(base + i*stride) for i in [0, count).
 * Distributes evenly across workers, then waits for completion.
 */
void bf_pool_map(bf_pool_t *pool, bf_pool_fn fn,
                 void *base, size_t count, size_t stride);

/*
 * Block until all submitted tasks are complete.
 */
void bf_pool_wait(bf_pool_t *pool);

/* ── Info ────────────────────────────────── */

/* Returns the number of worker threads. */
int bf_pool_num_threads(const bf_pool_t *pool);

#ifdef __cplusplus
}
#endif

#endif /* BF_POOL_H */
