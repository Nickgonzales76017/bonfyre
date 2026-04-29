/*
 * bf_reactor.h — Shared-nothing async I/O reactor for BonfyreApi
 *
 * Replaces pthreads-per-connection with:
 *   macOS:  kqueue
 *   Linux:  io_uring (5.1+) with fallback to epoll
 *
 * Architecture:
 *   - Per-core reactor loop (no shared state between cores)
 *   - Zero-copy recv via registered buffers (io_uring) or kevent (kqueue)
 *   - Connection affinity: each connection bound to one reactor
 *   - SSE streams: level-triggered persistent read + timer for keepalive
 *
 * Inspired by Seastar's shared-nothing model but pure C, no C++ deps.
 * Each reactor owns its connections, its SQLite read handle, its rate limiter.
 *
 * Integration: BonfyreApi main() creates N reactors (one per core),
 * listener socket is shared via SO_REUSEPORT.
 */
#ifndef BF_REACTOR_H
#define BF_REACTOR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Configuration ───────────────────────────────────────────── */

#define BF_REACTOR_MAX_EVENTS   256
#define BF_REACTOR_MAX_CONNS    4096
#define BF_REACTOR_BUF_SIZE     (64 * 1024)  /* 64KB per recv buffer */
#define BF_REACTOR_TIMER_MS     100          /* SSE keepalive interval */

/* ── Error codes ─────────────────────────────────────────────── */

#define BF_REACTOR_OK            0
#define BF_REACTOR_ERR_INIT     -1
#define BF_REACTOR_ERR_BIND     -2
#define BF_REACTOR_ERR_SUBMIT   -3
#define BF_REACTOR_ERR_MEMORY   -4

/* ── Connection state ────────────────────────────────────────── */

typedef enum {
    BF_CONN_READING,        /* Waiting for request data */
    BF_CONN_PROCESSING,     /* Handler running */
    BF_CONN_WRITING,        /* Sending response */
    BF_CONN_SSE,            /* SSE stream (long-lived) */
    BF_CONN_CLOSING,        /* Shutdown in progress */
} bf_conn_state_t;

typedef struct bf_conn {
    int               fd;
    bf_conn_state_t   state;
    uint8_t          *recv_buf;
    size_t            recv_len;
    uint8_t          *send_buf;
    size_t            send_len;
    size_t            send_off;

    /* Rate limiting (per-connection, token bucket) */
    double            tokens;
    double            last_refill;

    /* SSE state */
    int64_t           sse_cursor;     /* Last event ID sent */
    int64_t           sse_job_id;     /* Filter by job (0 = all) */

    /* API key (from Authorization header) */
    char              api_key[64];

    /* Timing */
    uint64_t          connected_ns;
    uint64_t          last_active_ns;
} bf_conn_t;

/* ── Request handler callback ────────────────────────────────── */

typedef struct {
    const char *method;       /* "GET", "POST", etc. */
    const char *path;         /* "/api/jobs", "/api/events", etc. */
    const char *query;        /* Query string (after ?) */
    const char *body;         /* Request body (NULL if none) */
    size_t      body_len;
    const char *api_key;      /* From Authorization header */
    const char *content_type;
} bf_request_t;

typedef struct {
    int         status;       /* HTTP status code */
    const char *content_type; /* "application/json", "text/event-stream", etc. */
    uint8_t    *body;         /* Response body (reactor takes ownership) */
    size_t      body_len;
    int         is_sse;       /* If 1, connection enters SSE mode */
} bf_response_t;

/* Handler: process a request, fill response.
 * Called from reactor loop — must not block.
 * For SSE: set response.is_sse = 1, the reactor handles streaming. */
typedef void (*bf_handler_fn)(const bf_request_t *req,
                               bf_response_t *resp,
                               void *user_data);

/* SSE generator: called periodically to produce events.
 * Returns number of bytes written to out_buf, or 0 if no new events.
 * cursor_inout: last event ID (updated by generator). */
typedef size_t (*bf_sse_gen_fn)(int64_t *cursor_inout,
                                 int64_t job_id,
                                 char *out_buf, size_t out_cap,
                                 void *user_data);

/* ── Reactor ─────────────────────────────────────────────────── */

typedef struct bf_reactor bf_reactor_t;

typedef struct {
    uint16_t       port;
    const char    *bind_addr;       /* NULL = "0.0.0.0" */
    int            n_reactors;      /* 0 = auto-detect cores */
    bf_handler_fn  handler;
    bf_sse_gen_fn  sse_generator;   /* NULL if no SSE support */
    void          *user_data;       /* Passed to handler and sse_gen */

    /* Rate limiting */
    double         rate_limit;      /* Requests per second per key (0 = unlimited) */
    double         burst;           /* Token bucket burst size */
} bf_reactor_config_t;

/* Create and start reactor pool.
 * Each reactor binds to the same port via SO_REUSEPORT.
 * Returns array of n_reactors handles. */
bf_reactor_t **bf_reactor_start(const bf_reactor_config_t *config, int *count_out);

/* Stop all reactors and free resources */
void bf_reactor_stop(bf_reactor_t **reactors, int count);

/* Get stats for a reactor */
typedef struct {
    int      reactor_id;
    int      active_conns;
    int      sse_conns;
    uint64_t requests_handled;
    uint64_t bytes_recv;
    uint64_t bytes_sent;
    double   avg_latency_us;
} bf_reactor_stats_t;

bf_reactor_stats_t bf_reactor_get_stats(const bf_reactor_t *reactor);

#ifdef __cplusplus
}
#endif

#endif /* BF_REACTOR_H */
