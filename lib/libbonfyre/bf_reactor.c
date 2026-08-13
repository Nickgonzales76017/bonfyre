/*
 * bf_reactor.c — Shared-nothing async I/O reactor
 *
 * macOS: kqueue-based event loop
 * Linux: io_uring (if available) with epoll fallback
 *
 * Each reactor runs on one thread, owns its connections, never shares state.
 * Listener socket shared via SO_REUSEPORT → kernel load-balances.
 */

#include "bf_reactor.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#ifdef __APPLE__
#include <sys/event.h>
#include <sys/sysctl.h>
#define REACTOR_KQUEUE 1
#elif defined(__linux__)
#include <sys/epoll.h>
#ifdef __has_include
#if __has_include(<liburing.h>)
#include <liburing.h>
#define REACTOR_IOURING 1
#endif
#endif
#ifndef REACTOR_IOURING
#define REACTOR_EPOLL 1
#endif
#endif

/* ── mimalloc per-reactor heap arenas ────────────────────────── */

#ifdef __has_include
#if __has_include(<mimalloc.h>)
#define BF_HAS_MIMALLOC 1
#include <mimalloc.h>
#endif
#endif

#ifdef BF_HAS_MIMALLOC
#define bf_malloc(heap, sz)   mi_heap_malloc(heap, sz)
#define bf_free(heap, ptr)    mi_free(ptr)
#define bf_heap_new()         mi_heap_new()
#define bf_heap_destroy(h)    mi_heap_destroy(h)
typedef mi_heap_t* bf_heap_t;
#else
#define bf_malloc(heap, sz)   malloc(sz)
#define bf_free(heap, ptr)    free(ptr)
#define bf_heap_new()         ((void *)1)  /* Dummy non-NULL */
#define bf_heap_destroy(h)    ((void)0)
typedef void* bf_heap_t;
#endif

/* ── Timestamp ───────────────────────────────────────────────── */

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int get_cpu_count(void) {
#ifdef __APPLE__
    int count = 0;
    size_t len = sizeof(count);
    sysctlbyname("hw.logicalcpu", &count, &len, NULL, 0);
    return count > 0 ? count : 4;
#else
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? (int)n : 4;
#endif
}

/* ── Internal reactor structure ──────────────────────────────── */

struct bf_reactor {
    int                    id;
    int                    listen_fd;
    int                    event_fd;     /* kqueue or epoll fd */
#ifdef REACTOR_IOURING
    struct io_uring        ring;
#endif
    volatile int           running;

    bf_conn_t              conns[BF_REACTOR_MAX_CONNS];
    int                    conn_count;

    bf_handler_fn          handler;
    bf_sse_gen_fn          sse_gen;
    void                  *user_data;

    /* Rate limiting config */
    double                 rate_limit;
    double                 burst;

    /* Stats */
    uint64_t               requests_handled;
    uint64_t               bytes_recv;
    uint64_t               bytes_sent;
    double                 total_latency_ns;

    bf_heap_t              heap;  /* Per-reactor mimalloc heap (or dummy) */

    pthread_t              thread;
};

/* ── Connection management ───────────────────────────────────── */

static bf_conn_t *reactor_alloc_conn(bf_reactor_t *r, int fd) {
    if (r->conn_count >= BF_REACTOR_MAX_CONNS) return NULL;

    bf_conn_t *c = &r->conns[r->conn_count++];
    memset(c, 0, sizeof(*c));
    c->fd = fd;
    c->state = BF_CONN_READING;
    c->recv_buf = bf_malloc(r->heap, BF_REACTOR_BUF_SIZE);
    c->send_buf = bf_malloc(r->heap, BF_REACTOR_BUF_SIZE);
    c->tokens = r->burst > 0 ? r->burst : 120.0;
    c->last_refill = (double)now_ns() / 1e9;
    c->connected_ns = now_ns();
    c->last_active_ns = c->connected_ns;

    /* Non-blocking */
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    /* TCP_NODELAY for low-latency SSE */
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    return c;
}

static void reactor_close_conn(bf_reactor_t *r, bf_conn_t *c) {
    close(c->fd);
    bf_free(r->heap, c->recv_buf);
    bf_free(r->heap, c->send_buf);

    /* Remove from array (swap with last) */
    int idx = (int)(c - r->conns);
    if (idx < r->conn_count - 1) {
        r->conns[idx] = r->conns[r->conn_count - 1];
    }
    r->conn_count--;
}

/* ── Rate limiter ────────────────────────────────────────────── */

static int check_rate_limit(bf_reactor_t *r, bf_conn_t *c) {
    if (r->rate_limit <= 0) return 1;

    double now = (double)now_ns() / 1e9;
    double elapsed = now - c->last_refill;
    c->tokens += elapsed * r->rate_limit;
    if (c->tokens > r->burst) c->tokens = r->burst;
    c->last_refill = now;

    if (c->tokens < 1.0) return 0;
    c->tokens -= 1.0;
    return 1;
}

/* ── HTTP parser (SIMD-accelerated via PicoHTTPParser) ───────── */

#include "bf_picohttpparser.h"

static int parse_request(const uint8_t *buf, size_t len, bf_request_t *req) {
    const char *method, *path;
    size_t method_len, path_len;
    int minor_version;
    struct phr_header hdrs[32];
    size_t num_headers = 32;

    int parsed = phr_parse_request((const char *)buf, len,
                                    &method, &method_len,
                                    &path, &path_len,
                                    &minor_version,
                                    hdrs, &num_headers, 0);
    if (parsed < 0) return -1;

    req->method = method;
    req->path = path;

    /* Query string split */
    const char *qmark = memchr(path, '?', path_len);
    req->query = qmark ? qmark + 1 : NULL;

    /* Extract known headers from parsed array */
    req->api_key = NULL;
    req->content_type = NULL;

    for (size_t i = 0; i < num_headers; i++) {
        if (hdrs[i].name_len == 13 &&
            strncasecmp(hdrs[i].name, "authorization", 13) == 0) {
            /* Skip "Bearer " prefix */
            if (hdrs[i].value_len > 7 &&
                strncasecmp(hdrs[i].value, "bearer ", 7) == 0) {
                req->api_key = hdrs[i].value + 7;
            }
        } else if (hdrs[i].name_len == 12 &&
                   strncasecmp(hdrs[i].name, "content-type", 12) == 0) {
            req->content_type = hdrs[i].value;
        }
    }

    /* Body: everything after headers */
    req->body = (const char *)buf + parsed;
    req->body_len = len - (size_t)parsed;

    return 0;
}

/* ── HTTP response builder ───────────────────────────────────── */

static size_t build_response(bf_conn_t *c, const bf_response_t *resp) {
    const char *status_text = "OK";
    if (resp->status == 201) status_text = "Created";
    else if (resp->status == 202) status_text = "Accepted";
    else if (resp->status == 400) status_text = "Bad Request";
    else if (resp->status == 401) status_text = "Unauthorized";
    else if (resp->status == 404) status_text = "Not Found";
    else if (resp->status == 429) status_text = "Too Many Requests";
    else if (resp->status == 500) status_text = "Internal Server Error";

    int hdr_len;
    if (resp->is_sse) {
        hdr_len = snprintf((char *)c->send_buf, BF_REACTOR_BUF_SIZE,
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/event-stream\r\n"
            "Cache-Control: no-cache\r\n"
            "Connection: keep-alive\r\n"
            "X-Accel-Buffering: no\r\n"
            "\r\n");
    } else {
        hdr_len = snprintf((char *)c->send_buf, BF_REACTOR_BUF_SIZE,
            "HTTP/1.1 %d %s\r\n"
            "Content-Type: %s\r\n"
            "Content-Length: %zu\r\n"
            "Connection: close\r\n"
            "\r\n",
            resp->status, status_text,
            resp->content_type ? resp->content_type : "application/json",
            resp->body_len);
    }

    if (resp->body && resp->body_len > 0) {
        size_t space = BF_REACTOR_BUF_SIZE - (size_t)hdr_len;
        size_t copy = resp->body_len < space ? resp->body_len : space;
        memcpy(c->send_buf + hdr_len, resp->body, copy);
        return (size_t)hdr_len + copy;
    }

    return (size_t)hdr_len;
}

/* ── Platform-specific event registration ────────────────────── */

static int register_read(bf_reactor_t *r, int fd) {
#ifdef REACTOR_KQUEUE
    struct kevent ev;
    EV_SET(&ev, fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, NULL);
    return kevent(r->event_fd, &ev, 1, NULL, 0, NULL);
#elif defined(REACTOR_EPOLL) || defined(REACTOR_IOURING)
    struct epoll_event ev = { .events = EPOLLIN | EPOLLET, .data.fd = fd };
    return epoll_ctl(r->event_fd, EPOLL_CTL_ADD, fd, &ev);
#endif
    return -1;
}

static int register_write(bf_reactor_t *r, int fd) {
#ifdef REACTOR_KQUEUE
    struct kevent ev;
    EV_SET(&ev, fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, NULL);
    return kevent(r->event_fd, &ev, 1, NULL, 0, NULL);
#elif defined(REACTOR_EPOLL) || defined(REACTOR_IOURING)
    struct epoll_event ev = { .events = EPOLLOUT | EPOLLET, .data.fd = fd };
    return epoll_ctl(r->event_fd, EPOLL_CTL_MOD, fd, &ev);
#endif
    return -1;
}

/* ── Handle readable connection ──────────────────────────────── */

static void handle_read(bf_reactor_t *r, bf_conn_t *c) {
    ssize_t n = recv(c->fd, c->recv_buf + c->recv_len,
                      BF_REACTOR_BUF_SIZE - c->recv_len, 0);
    if (n <= 0) {
        c->state = BF_CONN_CLOSING;
        return;
    }

    c->recv_len += (size_t)n;
    c->last_active_ns = now_ns();
    r->bytes_recv += (uint64_t)n;

    /* Check for complete HTTP request (\r\n\r\n) */
    if (!memmem(c->recv_buf, c->recv_len, "\r\n\r\n", 4)) return;

    /* Rate limit check */
    if (!check_rate_limit(r, c)) {
        bf_response_t resp = {
            .status = 429,
            .content_type = "application/json",
            .body = (uint8_t *)"{\"error\":\"rate_limit_exceeded\"}",
            .body_len = 30
        };
        c->send_len = build_response(c, &resp);
        c->send_off = 0;
        c->state = BF_CONN_WRITING;
        register_write(r, c->fd);
        return;
    }

    /* Parse and dispatch */
    bf_request_t req = {0};
    if (parse_request(c->recv_buf, c->recv_len, &req) != 0) {
        c->state = BF_CONN_CLOSING;
        return;
    }

    if (req.api_key) {
        size_t klen = 0;
        const char *kend = strstr(req.api_key, "\r\n");
        klen = kend ? (size_t)(kend - req.api_key) : strlen(req.api_key);
        if (klen > sizeof(c->api_key) - 1) klen = sizeof(c->api_key) - 1;
        memcpy(c->api_key, req.api_key, klen);
        c->api_key[klen] = '\0';
    }

    bf_response_t resp = {0};
    uint64_t t0 = now_ns();

    if (r->handler) {
        c->state = BF_CONN_PROCESSING;
        r->handler(&req, &resp, r->user_data);
    } else {
        resp.status = 404;
        resp.content_type = "application/json";
        resp.body = (uint8_t *)"{\"error\":\"no_handler\"}";
        resp.body_len = 21;
    }

    uint64_t t1 = now_ns();
    r->total_latency_ns += (double)(t1 - t0);
    r->requests_handled++;

    if (resp.is_sse) {
        /* Enter SSE mode: send headers, keep connection open */
        c->send_len = build_response(c, &resp);
        c->send_off = 0;
        c->state = BF_CONN_SSE;
        /* Send headers immediately */
        send(c->fd, c->send_buf, c->send_len, 0);
        r->bytes_sent += c->send_len;
        c->send_len = 0;
    } else {
        c->send_len = build_response(c, &resp);
        c->send_off = 0;
        c->state = BF_CONN_WRITING;
        register_write(r, c->fd);
    }

    if (resp.body && !resp.is_sse) free(resp.body);

    /* Reset recv buffer for potential keep-alive */
    c->recv_len = 0;
}

/* ── Handle writable connection ──────────────────────────────── */

static void handle_write(bf_reactor_t *r, bf_conn_t *c) {
    if (c->send_off >= c->send_len) {
        if (c->state == BF_CONN_WRITING)
            c->state = BF_CONN_CLOSING;
        return;
    }

    ssize_t n = send(c->fd, c->send_buf + c->send_off,
                      c->send_len - c->send_off, 0);
    if (n <= 0) {
        c->state = BF_CONN_CLOSING;
        return;
    }

    c->send_off += (size_t)n;
    r->bytes_sent += (uint64_t)n;

    if (c->send_off >= c->send_len) {
        c->state = BF_CONN_CLOSING;
    }
}

/* ── SSE push ────────────────────────────────────────────────── */

static void handle_sse(bf_reactor_t *r, bf_conn_t *c) {
    if (!r->sse_gen) return;

    char buf[4096];
    size_t n = r->sse_gen(&c->sse_cursor, c->sse_job_id,
                           buf, sizeof(buf), r->user_data);
    if (n > 0) {
        ssize_t sent = send(c->fd, buf, n, MSG_NOSIGNAL);
        if (sent <= 0) {
            c->state = BF_CONN_CLOSING;
            return;
        }
        r->bytes_sent += (uint64_t)sent;
    }
}

/* ── Main event loop ─────────────────────────────────────────── */

static void *reactor_loop(void *arg) {
    bf_reactor_t *r = (bf_reactor_t *)arg;

    while (r->running) {
#ifdef REACTOR_KQUEUE
        struct kevent events[BF_REACTOR_MAX_EVENTS];
        struct timespec ts = { .tv_sec = 0, .tv_nsec = BF_REACTOR_TIMER_MS * 1000000L };
        int n = kevent(r->event_fd, NULL, 0, events, BF_REACTOR_MAX_EVENTS, &ts);

        for (int i = 0; i < n; i++) {
            int fd = (int)events[i].ident;

            if (fd == r->listen_fd) {
                /* Accept new connection */
                struct sockaddr_storage addr;
                socklen_t addrlen = sizeof(addr);
                int cfd = accept(r->listen_fd, (struct sockaddr *)&addr, &addrlen);
                if (cfd >= 0) {
                    bf_conn_t *c = reactor_alloc_conn(r, cfd);
                    if (c) {
                        register_read(r, cfd);
                    } else {
                        close(cfd);
                    }
                }
            } else {
                /* Find connection */
                for (int j = 0; j < r->conn_count; j++) {
                    if (r->conns[j].fd == fd) {
                        if (events[i].filter == EVFILT_READ)
                            handle_read(r, &r->conns[j]);
                        else if (events[i].filter == EVFILT_WRITE)
                            handle_write(r, &r->conns[j]);
                        break;
                    }
                }
            }
        }

#elif defined(REACTOR_EPOLL)
        struct epoll_event events[BF_REACTOR_MAX_EVENTS];
        int n = epoll_wait(r->event_fd, events, BF_REACTOR_MAX_EVENTS,
                            BF_REACTOR_TIMER_MS);

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;

            if (fd == r->listen_fd) {
                struct sockaddr_storage addr;
                socklen_t addrlen = sizeof(addr);
                int cfd = accept(r->listen_fd, (struct sockaddr *)&addr, &addrlen);
                if (cfd >= 0) {
                    bf_conn_t *c = reactor_alloc_conn(r, cfd);
                    if (c) {
                        register_read(r, cfd);
                    } else {
                        close(cfd);
                    }
                }
            } else {
                for (int j = 0; j < r->conn_count; j++) {
                    if (r->conns[j].fd == fd) {
                        if (events[i].events & EPOLLIN)
                            handle_read(r, &r->conns[j]);
                        if (events[i].events & EPOLLOUT)
                            handle_write(r, &r->conns[j]);
                        break;
                    }
                }
            }
        }
#endif

        /* SSE keepalive + push for all SSE connections */
        for (int j = 0; j < r->conn_count; j++) {
            if (r->conns[j].state == BF_CONN_SSE) {
                handle_sse(r, &r->conns[j]);
            }
        }

        /* Cleanup closing connections */
        for (int j = r->conn_count - 1; j >= 0; j--) {
            if (r->conns[j].state == BF_CONN_CLOSING) {
                reactor_close_conn(r, &r->conns[j]);
            }
        }
    }

    return NULL;
}

/* ── Public API ──────────────────────────────────────────────── */

bf_reactor_t **bf_reactor_start(const bf_reactor_config_t *config, int *count_out) {
    int n = config->n_reactors > 0 ? config->n_reactors : get_cpu_count();
    if (n > 64) n = 64;

    bf_reactor_t **reactors = calloc((size_t)n, sizeof(bf_reactor_t *));
    if (!reactors) return NULL;

    for (int i = 0; i < n; i++) {
        reactors[i] = calloc(1, sizeof(bf_reactor_t));
        bf_reactor_t *r = reactors[i];
        r->id = i;
        r->running = 1;
        r->handler = config->handler;
        r->sse_gen = config->sse_generator;
        r->user_data = config->user_data;
        r->rate_limit = config->rate_limit > 0 ? config->rate_limit : 2.0;
        r->burst = config->burst > 0 ? config->burst : 120.0;

        /* Per-reactor heap arena (mimalloc or dummy) */
        r->heap = bf_heap_new();

        /* Create listener socket with SO_REUSEPORT */
        r->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (r->listen_fd < 0) continue;

        int one = 1;
        setsockopt(r->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        setsockopt(r->listen_fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

        struct sockaddr_in addr = {
            .sin_family = AF_INET,
            .sin_port = htons(config->port),
            .sin_addr.s_addr = INADDR_ANY
        };
        if (config->bind_addr)
            inet_pton(AF_INET, config->bind_addr, &addr.sin_addr);

        if (bind(r->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            close(r->listen_fd);
            r->listen_fd = -1;
            continue;
        }

        listen(r->listen_fd, 1024);

        int flags = fcntl(r->listen_fd, F_GETFL, 0);
        fcntl(r->listen_fd, F_SETFL, flags | O_NONBLOCK);

        /* Create event poller */
#ifdef REACTOR_KQUEUE
        r->event_fd = kqueue();
        struct kevent ev;
        EV_SET(&ev, r->listen_fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
        kevent(r->event_fd, &ev, 1, NULL, 0, NULL);
#elif defined(REACTOR_EPOLL)
        r->event_fd = epoll_create1(0);
        struct epoll_event ev = { .events = EPOLLIN, .data.fd = r->listen_fd };
        epoll_ctl(r->event_fd, EPOLL_CTL_ADD, r->listen_fd, &ev);
#endif

        /* Start reactor thread */
        pthread_create(&r->thread, NULL, reactor_loop, r);
    }

    *count_out = n;
    return reactors;
}

void bf_reactor_stop(bf_reactor_t **reactors, int count) {
    if (!reactors) return;

    /* Signal all to stop */
    for (int i = 0; i < count; i++) {
        if (reactors[i]) reactors[i]->running = 0;
    }

    /* Join threads */
    for (int i = 0; i < count; i++) {
        if (!reactors[i]) continue;
        pthread_join(reactors[i]->thread, NULL);

        /* Close all connections */
        for (int j = 0; j < reactors[i]->conn_count; j++) {
            close(reactors[i]->conns[j].fd);
            bf_free(reactors[i]->heap, reactors[i]->conns[j].recv_buf);
            bf_free(reactors[i]->heap, reactors[i]->conns[j].send_buf);
        }

        if (reactors[i]->listen_fd >= 0) close(reactors[i]->listen_fd);
        if (reactors[i]->event_fd >= 0) close(reactors[i]->event_fd);
        bf_heap_destroy(reactors[i]->heap);
        free(reactors[i]);
    }
    free(reactors);
}

bf_reactor_stats_t bf_reactor_get_stats(const bf_reactor_t *r) {
    bf_reactor_stats_t s = {0};
    if (!r) return s;
    s.reactor_id = r->id;
    s.active_conns = r->conn_count;
    s.requests_handled = r->requests_handled;
    s.bytes_recv = r->bytes_recv;
    s.bytes_sent = r->bytes_sent;

    for (int i = 0; i < r->conn_count; i++) {
        if (r->conns[i].state == BF_CONN_SSE) s.sse_conns++;
    }

    if (r->requests_handled > 0) {
        s.avg_latency_us = r->total_latency_ns / (double)r->requests_handled / 1000.0;
    }

    return s;
}
