/*
 * bf_log.c — Structured ring-buffer logger implementation
 *
 * Architecture:
 *   - Each thread gets a thread-local ring buffer (4096 entries)
 *   - Ring buffer stores formatted log lines
 *   - Flush drains all thread-local buffers into configured sinks
 *   - File sink supports size-based rotation
 *
 * Lock-free design: writers use atomic CAS on per-thread head.
 * Only the flush path (reader) needs a lock for file I/O.
 */

#include "bf_log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>

/* ── Ring buffer ─────────────────────────────────────────────── */

#define RING_SIZE     4096
#define RING_MASK     (RING_SIZE - 1)
#define MSG_MAX_LEN   512

typedef struct {
    char msg[MSG_MAX_LEN];
} ring_entry_t;

typedef struct {
    ring_entry_t entries[RING_SIZE];
    _Atomic int  head;   /* Next write position */
    _Atomic int  tail;   /* Next read position */
} ring_buf_t;

/* ── Global state ────────────────────────────────────────────── */

static struct {
    _Atomic bf_log_level_t level;
    bf_log_config_t        config;
    FILE                  *log_file;
    size_t                 file_bytes;
    pthread_mutex_t        file_lock;
    int                    initialized;
} g_log;

/* ── Thread-local ring ───────────────────────────────────────── */

static __thread ring_buf_t *tl_ring = NULL;

/* Registry of all thread-local rings for flush */
#define MAX_THREADS 64
static ring_buf_t  *g_rings[MAX_THREADS];
static _Atomic int  g_ring_count;

static ring_buf_t *get_ring(void) {
    if (tl_ring) return tl_ring;

    ring_buf_t *r = calloc(1, sizeof(ring_buf_t));
    if (!r) return NULL;
    atomic_store(&r->head, 0);
    atomic_store(&r->tail, 0);

    int idx = atomic_fetch_add(&g_ring_count, 1);
    if (idx < MAX_THREADS) {
        g_rings[idx] = r;
    }
    tl_ring = r;
    return r;
}

/* ── Severity names ──────────────────────────────────────────── */

static const char *level_names[] = {
    "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"
};

/* ── Timestamp ───────────────────────────────────────────────── */

static void format_timestamp(char *buf, size_t sz) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm tm;
    gmtime_r(&tv.tv_sec, &tm);
    int n = snprintf(buf, sz, "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ",
                     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                     tm.tm_hour, tm.tm_min, tm.tm_sec,
                     (int)tv.tv_usec);
    (void)n;
}

/* ── File rotation ───────────────────────────────────────────── */

static void rotate_file(void) {
    if (!g_log.log_file || !g_log.config.file_path) return;

    fclose(g_log.log_file);
    g_log.log_file = NULL;

    int max = g_log.config.max_files > 0 ? g_log.config.max_files : 3;
    char old_path[512], new_path[512];

    /* Remove oldest */
    snprintf(old_path, sizeof(old_path), "%s.%d", g_log.config.file_path, max);
    unlink(old_path);

    /* Shift existing rotated files */
    for (int i = max - 1; i >= 1; i--) {
        snprintf(old_path, sizeof(old_path), "%s.%d", g_log.config.file_path, i);
        snprintf(new_path, sizeof(new_path), "%s.%d", g_log.config.file_path, i + 1);
        rename(old_path, new_path);
    }

    /* Rotate current */
    snprintf(new_path, sizeof(new_path), "%s.1", g_log.config.file_path);
    rename(g_log.config.file_path, new_path);

    g_log.log_file = fopen(g_log.config.file_path, "a");
    g_log.file_bytes = 0;
}

/* ── Sink dispatch ───────────────────────────────────────────── */

static void emit_line(const char *line, bf_log_level_t level,
                       const char *ts, const char *file, int lineno,
                       const char *user_msg) {
    /* stderr */
    fprintf(stderr, "%s\n", line);

    /* File sink */
    if (g_log.log_file) {
        pthread_mutex_lock(&g_log.file_lock);
        size_t n = (size_t)fprintf(g_log.log_file, "%s\n", line);
        g_log.file_bytes += n;
        if (g_log.config.max_file_bytes > 0 &&
            g_log.file_bytes >= g_log.config.max_file_bytes) {
            rotate_file();
        }
        pthread_mutex_unlock(&g_log.file_lock);
    }

    /* Custom sink */
    if (g_log.config.custom_sink) {
        g_log.config.custom_sink(level, ts, file, lineno, user_msg,
                                  g_log.config.sink_user_data);
    }
}

/* ── Lifecycle ───────────────────────────────────────────────── */

int bf_log_init(bf_log_level_t level, const bf_log_config_t *config) {
    atomic_store(&g_log.level, level);
    atomic_store(&g_ring_count, 0);

    if (config) {
        g_log.config = *config;
    } else {
        memset(&g_log.config, 0, sizeof(g_log.config));
    }

    g_log.log_file = NULL;
    g_log.file_bytes = 0;

    if (g_log.config.file_path) {
        g_log.log_file = fopen(g_log.config.file_path, "a");
        if (!g_log.log_file) return -1;
    }

    pthread_mutex_init(&g_log.file_lock, NULL);
    g_log.initialized = 1;
    return 0;
}

void bf_log_shutdown(void) {
    if (!g_log.initialized) return;

    bf_log_flush();

    if (g_log.log_file) {
        fclose(g_log.log_file);
        g_log.log_file = NULL;
    }

    int n = atomic_load(&g_ring_count);
    for (int i = 0; i < n && i < MAX_THREADS; i++) {
        free(g_rings[i]);
        g_rings[i] = NULL;
    }
    atomic_store(&g_ring_count, 0);

    pthread_mutex_destroy(&g_log.file_lock);
    g_log.initialized = 0;
}

void bf_log_set_level(bf_log_level_t level) {
    atomic_store(&g_log.level, level);
}

bf_log_level_t bf_log_get_level(void) {
    return atomic_load(&g_log.level);
}

/* ── Core write ──────────────────────────────────────────────── */

void bf_log_write(bf_log_level_t level, const char *file, int line,
                  const char *fmt, ...) {
    if (level < atomic_load(&g_log.level)) return;
    if (!g_log.initialized) return;

    /* Format timestamp */
    char ts[32];
    format_timestamp(ts, sizeof(ts));

    /* Format user message */
    char user_msg[384];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(user_msg, sizeof(user_msg), fmt, ap);
    va_end(ap);

    /* Shorten file path to basename */
    const char *base = file;
    const char *slash = strrchr(file, '/');
    if (slash) base = slash + 1;

    /* Format full line */
    char full[MSG_MAX_LEN];
    snprintf(full, sizeof(full), "%s [%-5s] %s:%d %s",
             ts, level_names[level], base, line, user_msg);

    /* Try ring buffer first */
    ring_buf_t *ring = get_ring();
    if (ring) {
        int h = atomic_fetch_add(&ring->head, 1) & RING_MASK;
        snprintf(ring->entries[h].msg, MSG_MAX_LEN, "%s", full);

        /* Auto-flush on FATAL or when ring is getting full */
        int t = atomic_load(&ring->tail);
        int used = (atomic_load(&ring->head) - t) & RING_MASK;
        if (level >= BF_LOG_LVL_FATAL || used > RING_SIZE * 3 / 4) {
            bf_log_flush();
        }
        return;
    }

    /* Fallback: direct emit */
    emit_line(full, level, ts, base, line, user_msg);
}

/* ── Flush ───────────────────────────────────────────────────── */

void bf_log_flush(void) {
    int count = atomic_load(&g_ring_count);
    if (count > MAX_THREADS) count = MAX_THREADS;

    for (int i = 0; i < count; i++) {
        ring_buf_t *ring = g_rings[i];
        if (!ring) continue;

        int tail = atomic_load(&ring->tail);
        int head = atomic_load(&ring->head);

        while (tail != head) {
            int idx = tail & RING_MASK;
            const char *msg = ring->entries[idx].msg;
            if (msg[0]) {
                /* Parse level from formatted line for emit_line */
                fprintf(stderr, "%s\n", msg);
                if (g_log.log_file) {
                    pthread_mutex_lock(&g_log.file_lock);
                    size_t n = (size_t)fprintf(g_log.log_file, "%s\n", msg);
                    g_log.file_bytes += n;
                    if (g_log.config.max_file_bytes > 0 &&
                        g_log.file_bytes >= g_log.config.max_file_bytes) {
                        rotate_file();
                    }
                    pthread_mutex_unlock(&g_log.file_lock);
                }
            }
            tail++;
        }
        atomic_store(&ring->tail, tail);
    }

    if (g_log.log_file) {
        fflush(g_log.log_file);
    }
    fflush(stderr);
}
