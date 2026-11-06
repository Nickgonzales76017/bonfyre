#define _POSIX_C_SOURCE 200809L
#include "http_readiness.h"
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define PENDING_LIMIT 64
#define IDLE_MILLISECONDS 10000
#define REQUEST_MILLISECONDS 30000

typedef struct {
    int fd, flags;
    char *bytes;
    size_t used, capacity, expected, body_offset;
    int64_t accepted_at;
} Pending;
struct BfHttpReadiness {
    Pending peers[PENDING_LIMIT];
    size_t request_capacity, narrow_capacity;
    char wide_prefix[64];
    unsigned cursor;
};
static int64_t milliseconds(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now)) return -1;
    return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}
static void retire(Pending *p) {
    if (p->fd >= 0) close(p->fd);
    free(p->bytes);
    memset(p, 0, sizeof(*p)); p->fd = -1;
}
static void refuse(Pending *p, int code) {
    char response[160];
    const char *reason = code == 413 ? "Content Too Large" : code == 408 ? "Request Timeout" :
                         code == 503 ? "Service Unavailable" : "Bad Request";
    int n = snprintf(response, sizeof(response), "HTTP/1.1 %d %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", code, reason);
    if (p->fd >= 0 && n > 0 && (size_t)n < sizeof(response)) (void)send(p->fd, response, (size_t)n, 0);
    retire(p);
}
BfHttpReadiness *bf_http_readiness_create(size_t request_capacity) {
    if (request_capacity < 1024) return NULL;
    BfHttpReadiness *pool = calloc(1, sizeof(*pool));
    if (!pool) return NULL;
    pool->request_capacity = request_capacity;
    for (int i = 0; i < PENDING_LIMIT; i++) pool->peers[i].fd = -1;
    return pool;
}
int bf_http_readiness_narrow(BfHttpReadiness *pool, size_t narrow_capacity, const char *wide_prefix) {
    if (!pool || narrow_capacity < 1024 || narrow_capacity > pool->request_capacity || !wide_prefix ||
        !*wide_prefix || strlen(wide_prefix) >= sizeof(pool->wide_prefix)) return -1;
    pool->narrow_capacity = narrow_capacity;
    snprintf(pool->wide_prefix, sizeof(pool->wide_prefix), "%s", wide_prefix);
    return 0;
}
void bf_http_readiness_destroy(BfHttpReadiness *pool) {
    if (!pool) return;
    for (int i = 0; i < PENDING_LIMIT; i++) retire(&pool->peers[i]);
    free(pool);
}
/* A target outside the wide prefix is held to the narrow limit as soon as its
 * request line has arrived, so headers or a declared body it could never
 * accept are refused then rather than waited for. */
static size_t limit_for(const Pending *p, const BfHttpReadiness *pool) {
    if (!pool->narrow_capacity) return pool->request_capacity;
    const char *target = memchr(p->bytes, ' ', p->used);
    size_t prefix = strlen(pool->wide_prefix);
    if (!target || (size_t)(p->bytes + p->used - (target + 1)) < prefix) return pool->request_capacity;
    return strncmp(target + 1, pool->wide_prefix, prefix) ? pool->narrow_capacity : pool->request_capacity;
}
/* Parse the existing bounded Content-Length contract once, before dispatch.
 * Return 0 for incomplete headers, 1 for framed headers, or an HTTP refusal. */
static int frame(Pending *p, const BfHttpReadiness *pool) {
    size_t maximum = limit_for(p, pool);
    char *end = strstr(p->bytes, "\r\n\r\n");
    size_t separator = 4;
    if (!end) { end = strstr(p->bytes, "\n\n"); separator = 2; }
    if (!end) return memchr(p->bytes, '\0', p->used) ? 400 : 0;
    p->body_offset = (size_t)(end - p->bytes) + separator;
    size_t want = 0;
    int seen = 0;
    for (const char *line = strchr(p->bytes, '\n'); line && line < end; ) {
        line++;
        const char *line_end = strchr(line, '\n');
        if (!line_end) return 400;
        if (!strncasecmp(line, "Content-Length:", 15)) {
            const char *number = line + 15; char *after = NULL;
            while (number < line_end && (*number == ' ' || *number == '\t')) number++;
            errno = 0;
            unsigned long long value = strtoull(number, &after, 10);
            while (after < line_end && (*after == ' ' || *after == '\t' || *after == '\r')) after++;
            if (seen++ || number >= line_end || !isdigit((unsigned char)*number) ||
                errno || after != line_end || value > SIZE_MAX) return 400;
            want = (size_t)value;
        } else if (!strncasecmp(line, "Transfer-Encoding:", 18)) return 400;
        line = line_end;
    }
    if (p->body_offset >= maximum || want > maximum - 1 - p->body_offset) return 413;
    p->expected = p->body_offset + want;
    return 1;
}
static int fill(Pending *p, const BfHttpReadiness *pool) {
    size_t maximum = pool->request_capacity;
    if (p->capacity - p->used <= 1) {
        size_t capacity = p->capacity ? p->capacity * 2 : 4096;
        if (capacity > maximum) capacity = maximum;
        if (capacity <= p->capacity) return 413;
        char *bytes = realloc(p->bytes, capacity);
        if (!bytes) return 503;
        p->bytes = bytes; p->capacity = capacity;
    }
    ssize_t n = recv(p->fd, p->bytes + p->used, p->capacity - p->used - 1, 0);
    if (n < 0) return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR ? 0 : -1;
    if (!n) return -1;
    p->used += (size_t)n; p->bytes[p->used] = '\0';
    if (!p->expected) {
        int framed = frame(p, pool);
        if (framed != 1) return framed ? framed : p->used >= limit_for(p, pool) - 1 ? 413 : 0;
    }
    return p->used >= p->expected ? 1 : 0;
}
int bf_http_readiness_next(BfHttpReadiness *pool, int listener, char *request,
                          size_t *body_offset) {
    if (!pool || !request || !body_offset) return -1;
    int listener_flags = fcntl(listener, F_GETFL, 0);
    if (listener_flags < 0 || fcntl(listener, F_SETFL, listener_flags | O_NONBLOCK) < 0) return -1;
    for (;;) {
        struct pollfd watches[PENDING_LIMIT + 1];
        int64_t now = milliseconds();
        int timeout = -1;
        if (now < 0) return -1;
        watches[0] = (struct pollfd){listener, POLLIN, 0};
        for (int i = 0; i < PENDING_LIMIT; i++) {
            Pending *p = &pool->peers[i];
            if (p->fd >= 0) {
                int64_t remaining = p->accepted_at + (p->used ? REQUEST_MILLISECONDS : IDLE_MILLISECONDS) - now;
                if (remaining <= 0) refuse(p, 408);
                else if (timeout < 0 || remaining < timeout) timeout = (int)remaining;
            }
            watches[i + 1] = (struct pollfd){p->fd, POLLIN, 0};
        }
        int ready = poll(watches, PENDING_LIMIT + 1, timeout);
        if (ready < 0) { if (errno == EINTR) continue; return -1; }
        /* Rotate the first examined peer so repeated new connections cannot
         * keep an older complete request from reaching its owner. */
        for (unsigned offset = 0; offset < PENDING_LIMIT; offset++) {
            unsigned i = (pool->cursor + offset) % PENDING_LIMIT;
            Pending *p = &pool->peers[i];
            if (p->fd < 0 || !watches[i + 1].revents) continue;
            int state = fill(p, pool);
            if (state < 0) { retire(p); continue; }
            if (state > 1) { refuse(p, state); continue; }
            if (!state) continue;
            if (fcntl(p->fd, F_SETFL, p->flags & ~O_NONBLOCK) < 0) { retire(p); continue; }
            int connected = p->fd;
            memcpy(request, p->bytes, p->expected); request[p->expected] = '\0';
            *body_offset = p->body_offset;
            p->fd = -1; retire(p); pool->cursor = (i + 1) % PENDING_LIMIT;
            return connected;
        }
        if (watches[0].revents & (POLLERR | POLLHUP | POLLNVAL)) return -1;
        if (!(watches[0].revents & POLLIN)) continue;
        int connected = accept(listener, NULL, NULL);
        if (connected < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) continue;
            return -1;
        }
        int slot = -1;
        for (int i = 0; i < PENDING_LIMIT; i++) if (pool->peers[i].fd < 0) { slot = i; break; }
        if (slot < 0) {
            /* At the bound, replace the oldest incomplete connection. A
             * preconnect burst cannot reserve every slot indefinitely. */
            slot = 0;
            for (int i = 1; i < PENDING_LIMIT; i++)
                if (pool->peers[i].accepted_at < pool->peers[slot].accepted_at) slot = i;
            refuse(&pool->peers[slot], 503);
        }
        Pending *p = &pool->peers[slot];
        p->fd = connected; p->flags = fcntl(connected, F_GETFL, 0); p->accepted_at = milliseconds();
        if (p->flags < 0 || p->accepted_at < 0 || fcntl(connected, F_SETFL, p->flags | O_NONBLOCK) < 0 ||
            fcntl(connected, F_SETFD, FD_CLOEXEC) < 0) retire(p);
    }
}
