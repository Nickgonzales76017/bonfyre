/*
 * bf_dns.h — Async DNS resolver for Bonfyre reactor
 *
 * Wraps c-ares when available, falls back to a thread-pool
 * dispatched getaddrinfo() otherwise. Integrates with
 * bf_reactor's kqueue/epoll event loop via fd watches.
 */

#ifndef BF_DNS_H
#define BF_DNS_H

#include <stdint.h>
#include <netinet/in.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Result types ────────────────────────────────────────────── */

#define BF_DNS_MAX_ADDRS 8

typedef struct {
    int            family;  /* AF_INET or AF_INET6 */
    union {
        struct in_addr  v4;
        struct in6_addr v6;
    } addr;
    uint16_t       port;    /* Copied from original query */
} bf_dns_addr_t;

typedef struct {
    int           status;   /* 0=success, -1=error */
    int           count;    /* Number of addresses resolved */
    bf_dns_addr_t addrs[BF_DNS_MAX_ADDRS];
    const char   *error;    /* Error string on failure, NULL on success */
    void         *user_data;
} bf_dns_result_t;

/* Callback invoked when resolution completes (on reactor thread). */
typedef void (*bf_dns_cb)(const bf_dns_result_t *result);

/* ── Resolver handle ─────────────────────────────────────────── */

typedef struct bf_dns_resolver bf_dns_resolver_t;

/* ── Lifecycle ───────────────────────────────────────────────── */

/*
 * Create a resolver.
 * reactor_fd: kqueue/epoll fd for integrating c-ares sockets.
 *             Pass -1 to use internal thread pool instead.
 * Returns NULL on failure.
 */
bf_dns_resolver_t *bf_dns_create(int reactor_fd);

/* Destroy resolver and cancel pending queries. */
void bf_dns_destroy(bf_dns_resolver_t *r);

/* ── Resolution ──────────────────────────────────────────────── */

/*
 * Resolve hostname asynchronously.
 * family: AF_INET, AF_INET6, or AF_UNSPEC for both.
 * port: stored in result for convenience.
 * cb: called when complete (may be called synchronously on cached hits).
 * user_data: passed through to result.
 *
 * Returns 0 on successful enqueue, -1 on error.
 */
int bf_dns_resolve(bf_dns_resolver_t *r,
                   const char *hostname,
                   int family,
                   uint16_t port,
                   bf_dns_cb cb,
                   void *user_data);

/* ── Reactor integration ─────────────────────────────────────── */

/*
 * Process pending DNS events. Call this from your reactor when
 * a DNS fd becomes readable/writable.
 * fd: the file descriptor that triggered.
 * readable/writable: which events fired.
 */
void bf_dns_process(bf_dns_resolver_t *r, int fd, int readable, int writable);

/*
 * Get timeout for next DNS timer. Returns milliseconds, or -1 if none.
 * Use this to set your reactor's poll timeout.
 */
int bf_dns_timeout_ms(bf_dns_resolver_t *r);

/*
 * Process expired timers. Call this when your reactor timer fires.
 */
void bf_dns_timer_tick(bf_dns_resolver_t *r);

/* ── Convenience: blocking resolve ───────────────────────────── */

/*
 * Synchronous resolve (blocks). Useful for startup.
 * Fills result->addrs. Returns 0 on success.
 */
int bf_dns_resolve_sync(const char *hostname, int family, uint16_t port,
                        bf_dns_result_t *result);

#ifdef __cplusplus
}
#endif

#endif /* BF_DNS_H */
