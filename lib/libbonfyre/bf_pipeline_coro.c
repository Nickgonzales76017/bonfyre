/*
 * bf_pipeline_coro.c — libdill structured concurrency for pipeline stages
 *
 * When libdill is available:
 *   - Bundle-based coroutines (structured, never-leak)
 *   - Typed channels via libdill ch()
 *   - Deadlines via libdill deadline()
 *
 * Fallback (no libdill):
 *   - POSIX threads + pipe-based channels
 *   - pthread_create per stage, pthread_join for sync
 */

#ifdef __has_include
#if __has_include(<libdill.h>)
#define BF_HAS_DILL 1
#endif
#endif

#ifdef BF_HAS_DILL
#include <libdill.h>
#else
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#endif

#include "bf_pipeline_coro.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

/* ── Timing ──────────────────────────────────────────────────── */

static int64_t mono_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* ── Pipeline internals ──────────────────────────────────────── */

struct bf_pipeline {
    bf_stage_desc_t   *stages;    /* Copied stage descriptors */
    bf_stage_result_t *results;   /* Per-stage results */
    int                n_stages;
    int                channel_mode; /* 0=independent, 1=sequential, 2=chain */
#ifdef BF_HAS_DILL
    int                bundle;    /* libdill bundle handle */
#else
    pthread_t         *threads;
#endif
};

/* ── Channel internals ───────────────────────────────────────── */

struct bf_chan {
    size_t item_size;
#ifdef BF_HAS_DILL
    int    ch[2];               /* libdill channel pair */
#else
    int    pipe_fd[2];           /* Fallback: POSIX pipe */
#endif
};

/* ──────────────────────────────────────────────────────────────
 * libdill backend
 * ────────────────────────────────────────────────────────────── */

#ifdef BF_HAS_DILL

/* Coroutine wrapper */
typedef struct {
    bf_stage_desc_t   *desc;
    bf_stage_result_t *result;
} coro_arg_t;

coroutine static void stage_coro(coro_arg_t *arg) {
    int64_t t0 = mono_ns();

    if (arg->desc->deadline_ms > 0) {
        int64_t dl = now() + arg->desc->deadline_ms;
        int rc_dl = deadline(dl);
        (void)rc_dl;
    }

    bf_stage_result_t r = arg->desc->fn(arg->desc->ctx);
    r.elapsed_ns = mono_ns() - t0;
    r.stage_name = arg->desc->name;
    *arg->result = r;
}

bf_pipeline_t *bf_pipeline_new(const bf_stage_desc_t *stages, int n_stages,
                                int channel_mode) {
    if (!stages || n_stages <= 0) return NULL;

    bf_pipeline_t *p = calloc(1, sizeof(*p));
    if (!p) return NULL;

    p->n_stages = n_stages;
    p->channel_mode = channel_mode;
    p->stages = calloc((size_t)n_stages, sizeof(bf_stage_desc_t));
    p->results = calloc((size_t)n_stages, sizeof(bf_stage_result_t));
    if (!p->stages || !p->results) {
        free(p->stages); free(p->results); free(p);
        return NULL;
    }

    memcpy(p->stages, stages, (size_t)n_stages * sizeof(bf_stage_desc_t));
    p->bundle = -1;
    return p;
}

int bf_pipeline_run(bf_pipeline_t *p) {
    if (!p) return -1;

    coro_arg_t *args = calloc((size_t)p->n_stages, sizeof(coro_arg_t));
    if (!args) return -1;

    p->bundle = bundle();
    if (p->bundle < 0) { free(args); return -1; }

    for (int i = 0; i < p->n_stages; i++) {
        args[i].desc = &p->stages[i];
        args[i].result = &p->results[i];
    }

    if (p->channel_mode == 1) {
        /* Sequential: run one at a time */
        for (int i = 0; i < p->n_stages; i++) {
            int64_t t0 = mono_ns();
            if (p->stages[i].deadline_ms > 0) {
                int64_t dl = now() + p->stages[i].deadline_ms;
                deadline(dl);
            }
            bf_stage_result_t r = p->stages[i].fn(p->stages[i].ctx);
            r.elapsed_ns = mono_ns() - t0;
            r.stage_name = p->stages[i].name;
            p->results[i] = r;
            if (r.rc != 0) { free(args); return r.rc; }
        }
    } else {
        /* Independent or chain: launch all as coroutines */
        for (int i = 0; i < p->n_stages; i++) {
            int h = bundle_go(p->bundle, stage_coro(&args[i]));
            if (h < 0) {
                fprintf(stderr, "[bf_pipeline_coro] failed to launch stage '%s'\n",
                        p->stages[i].name);
            }
        }

        /* Wait for entire bundle */
        int rc = bundle_wait(p->bundle, -1);
        (void)rc;
    }

    hclose(p->bundle);
    p->bundle = -1;
    free(args);

    /* Check results */
    for (int i = 0; i < p->n_stages; i++) {
        if (p->results[i].rc != 0) return p->results[i].rc;
    }
    return 0;
}

/* Channel via libdill */

bf_chan_t *bf_chan_new(size_t item_size, int capacity) {
    bf_chan_t *ch = calloc(1, sizeof(*ch));
    if (!ch) return NULL;
    ch->item_size = item_size;

    int rc = chmake(ch->ch);
    (void)capacity; /* libdill channels are unbuffered by default */
    if (rc != 0) { free(ch); return NULL; }
    return ch;
}

int bf_chan_send(bf_chan_t *ch, const void *item, int64_t deadline_ms) {
    if (!ch) return -1;
    int64_t dl = deadline_ms > 0 ? now() + deadline_ms : -1;
    return chsend(ch->ch[0], item, ch->item_size, dl);
}

int bf_chan_recv(bf_chan_t *ch, void *item, int64_t deadline_ms) {
    if (!ch) return -1;
    int64_t dl = deadline_ms > 0 ? now() + deadline_ms : -1;
    return chrecv(ch->ch[1], item, ch->item_size, dl);
}

void bf_chan_close(bf_chan_t *ch) {
    if (!ch) return;
    chdone(ch->ch[0]);
}

void bf_chan_free(bf_chan_t *ch) {
    if (!ch) return;
    hclose(ch->ch[0]);
    hclose(ch->ch[1]);
    free(ch);
}

/* parallel map */
int bf_pipeline_parallel(bf_stage_fn fn, void **contexts, int n,
                          int64_t deadline_ms, bf_stage_result_t *results) {
    bf_stage_desc_t *descs = calloc((size_t)n, sizeof(bf_stage_desc_t));
    if (!descs) return -1;
    for (int i = 0; i < n; i++) {
        descs[i].name = "parallel";
        descs[i].fn = fn;
        descs[i].ctx = contexts[i];
        descs[i].deadline_ms = deadline_ms;
    }
    bf_pipeline_t *p = bf_pipeline_new(descs, n, 0);
    free(descs);
    if (!p) return -1;
    int rc = bf_pipeline_run(p);
    if (results) memcpy(results, p->results, (size_t)n * sizeof(bf_stage_result_t));
    bf_pipeline_free(p);
    return rc;
}

/* ──────────────────────────────────────────────────────────────
 * POSIX fallback (no libdill)
 * ────────────────────────────────────────────────────────────── */

#else /* !BF_HAS_DILL */

typedef struct {
    bf_stage_desc_t   *desc;
    bf_stage_result_t *result;
} thread_arg_t;

static void *stage_thread(void *arg) {
    thread_arg_t *ta = (thread_arg_t *)arg;
    int64_t t0 = mono_ns();
    bf_stage_result_t r = ta->desc->fn(ta->desc->ctx);
    r.elapsed_ns = mono_ns() - t0;
    r.stage_name = ta->desc->name;
    *ta->result = r;
    return NULL;
}

bf_pipeline_t *bf_pipeline_new(const bf_stage_desc_t *stages, int n_stages,
                                int channel_mode) {
    if (!stages || n_stages <= 0) return NULL;

    bf_pipeline_t *p = calloc(1, sizeof(*p));
    if (!p) return NULL;

    p->n_stages = n_stages;
    p->channel_mode = channel_mode;
    p->stages = calloc((size_t)n_stages, sizeof(bf_stage_desc_t));
    p->results = calloc((size_t)n_stages, sizeof(bf_stage_result_t));
    p->threads = calloc((size_t)n_stages, sizeof(pthread_t));
    if (!p->stages || !p->results || !p->threads) {
        free(p->stages); free(p->results); free(p->threads); free(p);
        return NULL;
    }

    memcpy(p->stages, stages, (size_t)n_stages * sizeof(bf_stage_desc_t));
    return p;
}

int bf_pipeline_run(bf_pipeline_t *p) {
    if (!p) return -1;

    thread_arg_t *args = calloc((size_t)p->n_stages, sizeof(thread_arg_t));
    if (!args) return -1;

    for (int i = 0; i < p->n_stages; i++) {
        args[i].desc = &p->stages[i];
        args[i].result = &p->results[i];
    }

    if (p->channel_mode == 1) {
        /* Sequential */
        for (int i = 0; i < p->n_stages; i++) {
            stage_thread(&args[i]);
            if (p->results[i].rc != 0) { free(args); return p->results[i].rc; }
        }
    } else {
        /* Parallel */
        for (int i = 0; i < p->n_stages; i++) {
            if (pthread_create(&p->threads[i], NULL, stage_thread, &args[i]) != 0) {
                fprintf(stderr, "[bf_pipeline_coro] failed to create thread for '%s'\n",
                        p->stages[i].name);
                p->results[i].rc = -1;
            }
        }

        for (int i = 0; i < p->n_stages; i++) {
            pthread_join(p->threads[i], NULL);
        }
    }

    free(args);

    for (int i = 0; i < p->n_stages; i++) {
        if (p->results[i].rc != 0) return p->results[i].rc;
    }
    return 0;
}

/* Channels via pipe() */

bf_chan_t *bf_chan_new(size_t item_size, int capacity) {
    (void)capacity;
    bf_chan_t *ch = calloc(1, sizeof(*ch));
    if (!ch) return NULL;
    ch->item_size = item_size;
    if (pipe(ch->pipe_fd) != 0) { free(ch); return NULL; }
    return ch;
}

int bf_chan_send(bf_chan_t *ch, const void *item, int64_t deadline_ms) {
    (void)deadline_ms;
    if (!ch) return -1;
    ssize_t n = write(ch->pipe_fd[1], item, ch->item_size);
    return n == (ssize_t)ch->item_size ? 0 : -1;
}

int bf_chan_recv(bf_chan_t *ch, void *item, int64_t deadline_ms) {
    (void)deadline_ms;
    if (!ch) return -1;
    ssize_t n = read(ch->pipe_fd[0], item, ch->item_size);
    return n == (ssize_t)ch->item_size ? 0 : -1;
}

void bf_chan_close(bf_chan_t *ch) {
    if (!ch) return;
    close(ch->pipe_fd[1]);
}

void bf_chan_free(bf_chan_t *ch) {
    if (!ch) return;
    close(ch->pipe_fd[0]);
    close(ch->pipe_fd[1]);
    free(ch);
}

int bf_pipeline_parallel(bf_stage_fn fn, void **contexts, int n,
                          int64_t deadline_ms, bf_stage_result_t *results) {
    bf_stage_desc_t *descs = calloc((size_t)n, sizeof(bf_stage_desc_t));
    if (!descs) return -1;
    for (int i = 0; i < n; i++) {
        descs[i].name = "parallel";
        descs[i].fn = fn;
        descs[i].ctx = contexts[i];
        descs[i].deadline_ms = deadline_ms;
    }
    bf_pipeline_t *p = bf_pipeline_new(descs, n, 0);
    free(descs);
    if (!p) return -1;
    int rc = bf_pipeline_run(p);
    if (results) memcpy(results, p->results, (size_t)n * sizeof(bf_stage_result_t));
    bf_pipeline_free(p);
    return rc;
}

#endif /* BF_HAS_DILL */

/* ── Shared ──────────────────────────────────────────────────── */

const bf_stage_result_t *bf_pipeline_result(const bf_pipeline_t *p, int i) {
    if (!p || i < 0 || i >= p->n_stages) return NULL;
    return &p->results[i];
}

void bf_pipeline_free(bf_pipeline_t *p) {
    if (!p) return;
    free(p->stages);
    free(p->results);
#ifndef BF_HAS_DILL
    free(p->threads);
#endif
    free(p);
}
