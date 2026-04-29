/*
 * bf_pool.c — Work-stealing thread pool
 *
 * Architecture:
 *   - N worker pthreads, each with a lock-free Chase-Lev deque
 *   - Submitter pushes to a round-robin chosen deque
 *   - Workers pop from their own deque (LIFO, cache-friendly)
 *   - Idle workers steal from a random peer (FIFO, load-balance)
 *   - Global atomic pending counter for bf_pool_wait()
 *   - Condition variable for idle wake-up
 */

#include "bf_pool.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ── Chase-Lev work-stealing deque ────────── */

#define DEQUE_INIT_CAP 1024

typedef struct {
    bf_pool_fn  fn;
    void       *arg;
} bf_task_t;

typedef struct {
    bf_task_t      *buf;
    _Atomic size_t  cap;     /* always power of 2 */
} deque_array_t;

typedef struct {
    _Atomic long    top;     /* steal end (FIFO) */
    _Atomic long    bottom;  /* owner end (LIFO) */
    _Atomic(deque_array_t *) array;
} deque_t;

static deque_array_t *deque_array_new(size_t cap)
{
    deque_array_t *a = malloc(sizeof(*a));
    if (!a) return NULL;
    a->buf = calloc(cap, sizeof(bf_task_t));
    if (!a->buf) { free(a); return NULL; }
    atomic_store(&a->cap, cap);
    return a;
}

static void deque_init(deque_t *d)
{
    atomic_store(&d->top, 0);
    atomic_store(&d->bottom, 0);
    atomic_store(&d->array, deque_array_new(DEQUE_INIT_CAP));
}

static void deque_destroy(deque_t *d)
{
    deque_array_t *a = atomic_load(&d->array);
    if (a) { free(a->buf); free(a); }
}

/* Owner push (bottom) */
static void deque_push(deque_t *d, bf_task_t task)
{
    long b = atomic_load_explicit(&d->bottom, memory_order_relaxed);
    long t = atomic_load_explicit(&d->top, memory_order_acquire);
    deque_array_t *a = atomic_load_explicit(&d->array, memory_order_relaxed);
    size_t cap = atomic_load_explicit(&a->cap, memory_order_relaxed);

    if ((size_t)(b - t) >= cap) {
        /* grow */
        size_t new_cap = cap * 2;
        deque_array_t *na = deque_array_new(new_cap);
        for (long i = t; i < b; i++)
            na->buf[i & (new_cap - 1)] = a->buf[i & (cap - 1)];
        atomic_store_explicit(&d->array, na, memory_order_release);
        a = na;
        cap = new_cap;
        /* old array leaked — acceptable for long-lived pool */
    }
    a->buf[b & (cap - 1)] = task;
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&d->bottom, b + 1, memory_order_relaxed);
}

/* Owner pop (bottom, LIFO) — returns 1 on success */
static int deque_pop(deque_t *d, bf_task_t *out)
{
    long b = atomic_load_explicit(&d->bottom, memory_order_relaxed) - 1;
    atomic_store_explicit(&d->bottom, b, memory_order_relaxed);
    deque_array_t *a = atomic_load_explicit(&d->array, memory_order_relaxed);
    size_t cap = atomic_load_explicit(&a->cap, memory_order_relaxed);

    atomic_thread_fence(memory_order_seq_cst);
    long t = atomic_load_explicit(&d->top, memory_order_relaxed);

    if (t <= b) {
        *out = a->buf[b & (cap - 1)];
        if (t == b) {
            /* last element — race with stealers */
            if (!atomic_compare_exchange_strong_explicit(
                    &d->top, &t, t + 1,
                    memory_order_seq_cst, memory_order_relaxed)) {
                /* lost to a stealer */
                atomic_store_explicit(&d->bottom, t + 1, memory_order_relaxed);
                return 0;
            }
            atomic_store_explicit(&d->bottom, t + 1, memory_order_relaxed);
        }
        return 1;
    }
    /* empty */
    atomic_store_explicit(&d->bottom, t, memory_order_relaxed);
    return 0;
}

/* Stealer take (top, FIFO) — returns 1 on success */
static int deque_steal(deque_t *d, bf_task_t *out)
{
    long t = atomic_load_explicit(&d->top, memory_order_acquire);
    atomic_thread_fence(memory_order_seq_cst);
    long b = atomic_load_explicit(&d->bottom, memory_order_acquire);

    if (t >= b) return 0; /* empty */

    deque_array_t *a = atomic_load_explicit(&d->array, memory_order_relaxed);
    size_t cap = atomic_load_explicit(&a->cap, memory_order_relaxed);
    *out = a->buf[t & (cap - 1)];

    if (!atomic_compare_exchange_strong_explicit(
            &d->top, &t, t + 1,
            memory_order_seq_cst, memory_order_relaxed))
        return 0; /* contention, retry later */

    return 1;
}

/* ── Pool structure ──────────────────────── */

typedef struct {
    pthread_t   thread;
    deque_t     deque;
    int         id;
    struct bf_pool *pool;
} worker_t;

struct bf_pool {
    worker_t       *workers;
    int             n_threads;
    _Atomic int     shutdown;
    _Atomic long    pending;       /* outstanding task count */

    /* wake-up mechanism */
    pthread_mutex_t wake_mutex;
    pthread_cond_t  wake_cond;

    /* wait mechanism */
    pthread_mutex_t wait_mutex;
    pthread_cond_t  wait_cond;

    /* round-robin submission index */
    _Atomic int     submit_idx;

    /* simple xorshift per-steal RNG seed */
    _Atomic unsigned int rng_state;
};

static unsigned int pool_rand(bf_pool_t *p)
{
    unsigned int x = atomic_fetch_add(&p->rng_state, 1) ^ 0xDEADBEEF;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    return x;
}

/* ── Worker loop ─────────────────────────── */

static void execute_task(bf_pool_t *pool, bf_task_t *t)
{
    t->fn(t->arg);
    long prev = atomic_fetch_sub_explicit(&pool->pending, 1, memory_order_release);
    if (prev == 1) {
        /* last task done — wake waiters */
        pthread_mutex_lock(&pool->wait_mutex);
        pthread_cond_broadcast(&pool->wait_cond);
        pthread_mutex_unlock(&pool->wait_mutex);
    }
}

static void *worker_main(void *arg)
{
    worker_t *w = (worker_t *)arg;
    bf_pool_t *pool = w->pool;
    bf_task_t task;
    int idle_spins = 0;

    while (!atomic_load_explicit(&pool->shutdown, memory_order_acquire)) {
        /* try own deque first */
        if (deque_pop(&w->deque, &task)) {
            idle_spins = 0;
            execute_task(pool, &task);
            continue;
        }

        /* try stealing from random peer */
        int victim = (int)(pool_rand(pool) % (unsigned)pool->n_threads);
        if (victim != w->id && deque_steal(&pool->workers[victim].deque, &task)) {
            idle_spins = 0;
            execute_task(pool, &task);
            continue;
        }

        /* spin a bit before sleeping */
        if (++idle_spins < 64) {
#if defined(__x86_64__) || defined(__i386__)
            __asm__ volatile("pause");
#elif defined(__aarch64__)
            __asm__ volatile("yield");
#endif
            continue;
        }

        /* sleep until woken */
        pthread_mutex_lock(&pool->wake_mutex);
        if (!atomic_load(&pool->shutdown) &&
            atomic_load(&pool->pending) == 0) {
            pthread_cond_wait(&pool->wake_cond, &pool->wake_mutex);
        }
        pthread_mutex_unlock(&pool->wake_mutex);
        idle_spins = 0;
    }

    /* drain remaining tasks on shutdown */
    while (deque_pop(&w->deque, &task))
        execute_task(pool, &task);

    return NULL;
}

/* ── Public API ──────────────────────────── */

bf_pool_t *bf_pool_create(int n_threads)
{
    if (n_threads <= 0) {
#ifdef _SC_NPROCESSORS_ONLN
        n_threads = (int)sysconf(_SC_NPROCESSORS_ONLN);
#else
        n_threads = 4;
#endif
        if (n_threads < 1) n_threads = 1;
        if (n_threads > 64) n_threads = 64;
    }

    bf_pool_t *p = calloc(1, sizeof(*p));
    if (!p) return NULL;

    p->n_threads = n_threads;
    atomic_store(&p->shutdown, 0);
    atomic_store(&p->pending, 0);
    atomic_store(&p->submit_idx, 0);
    atomic_store(&p->rng_state, 0x12345678);
    pthread_mutex_init(&p->wake_mutex, NULL);
    pthread_cond_init(&p->wake_cond, NULL);
    pthread_mutex_init(&p->wait_mutex, NULL);
    pthread_cond_init(&p->wait_cond, NULL);

    p->workers = calloc((size_t)n_threads, sizeof(worker_t));
    if (!p->workers) { free(p); return NULL; }

    for (int i = 0; i < n_threads; i++) {
        p->workers[i].id   = i;
        p->workers[i].pool = p;
        deque_init(&p->workers[i].deque);
    }

    for (int i = 0; i < n_threads; i++) {
        if (pthread_create(&p->workers[i].thread, NULL, worker_main,
                           &p->workers[i]) != 0) {
            /* partial creation — shut down what we have */
            atomic_store(&p->shutdown, 1);
            for (int j = 0; j < i; j++) {
                pthread_cond_broadcast(&p->wake_cond);
                pthread_join(p->workers[j].thread, NULL);
            }
            for (int j = 0; j < n_threads; j++)
                deque_destroy(&p->workers[j].deque);
            free(p->workers);
            free(p);
            return NULL;
        }
    }

    return p;
}

void bf_pool_destroy(bf_pool_t *pool)
{
    if (!pool) return;

    bf_pool_wait(pool);

    atomic_store_explicit(&pool->shutdown, 1, memory_order_release);

    /* wake all workers */
    pthread_mutex_lock(&pool->wake_mutex);
    pthread_cond_broadcast(&pool->wake_cond);
    pthread_mutex_unlock(&pool->wake_mutex);

    for (int i = 0; i < pool->n_threads; i++)
        pthread_join(pool->workers[i].thread, NULL);

    for (int i = 0; i < pool->n_threads; i++)
        deque_destroy(&pool->workers[i].deque);

    pthread_mutex_destroy(&pool->wake_mutex);
    pthread_cond_destroy(&pool->wake_cond);
    pthread_mutex_destroy(&pool->wait_mutex);
    pthread_cond_destroy(&pool->wait_cond);
    free(pool->workers);
    free(pool);
}

int bf_pool_submit(bf_pool_t *pool, bf_pool_fn fn, void *arg)
{
    if (!pool || !fn) return -1;
    if (atomic_load_explicit(&pool->shutdown, memory_order_acquire)) return -1;

    bf_task_t task = { .fn = fn, .arg = arg };
    int idx = atomic_fetch_add(&pool->submit_idx, 1) % pool->n_threads;

    atomic_fetch_add_explicit(&pool->pending, 1, memory_order_release);
    deque_push(&pool->workers[idx].deque, task);

    /* wake a sleeping worker */
    pthread_mutex_lock(&pool->wake_mutex);
    pthread_cond_signal(&pool->wake_cond);
    pthread_mutex_unlock(&pool->wake_mutex);

    return 0;
}

/* map helper — passed as task arg */
typedef struct {
    bf_pool_fn  fn;
    void       *item;
} map_item_t;

static void map_trampoline(void *arg)
{
    map_item_t *mi = (map_item_t *)arg;
    mi->fn(mi->item);
    /* mi is freed by caller after wait */
}

void bf_pool_map(bf_pool_t *pool, bf_pool_fn fn,
                 void *base, size_t count, size_t stride)
{
    if (!pool || !fn || count == 0) return;

    map_item_t *items = malloc(count * sizeof(map_item_t));
    if (!items) {
        /* fallback: serial execution */
        for (size_t i = 0; i < count; i++)
            fn((char *)base + i * stride);
        return;
    }

    for (size_t i = 0; i < count; i++) {
        items[i].fn   = fn;
        items[i].item = (char *)base + i * stride;
        bf_pool_submit(pool, map_trampoline, &items[i]);
    }

    bf_pool_wait(pool);
    free(items);
}

void bf_pool_wait(bf_pool_t *pool)
{
    if (!pool) return;

    while (atomic_load_explicit(&pool->pending, memory_order_acquire) > 0) {
        pthread_mutex_lock(&pool->wait_mutex);
        if (atomic_load(&pool->pending) > 0)
            pthread_cond_wait(&pool->wait_cond, &pool->wait_mutex);
        pthread_mutex_unlock(&pool->wait_mutex);
    }
}

int bf_pool_num_threads(const bf_pool_t *pool)
{
    return pool ? pool->n_threads : 0;
}
