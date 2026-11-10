#ifndef BF_WEB_HTTP_READINESS_H
#define BF_WEB_HTTP_READINESS_H
#include <stddef.h>

typedef struct BfHttpReadiness BfHttpReadiness;
/* Each worker retains at most 64 incomplete requests, allocated as bytes arrive.
 * Complete requests alone cross into the existing synchronous owner handlers. */
BfHttpReadiness *bf_http_readiness_create(size_t request_capacity);
/* Targets that do not start with wide_prefix are framed against
 * narrow_capacity: a larger declared body is refused 413 when the headers
 * arrive. Returns -1 for an invalid configuration. */
int bf_http_readiness_narrow(BfHttpReadiness *pool, size_t narrow_capacity, const char *wide_prefix);
void bf_http_readiness_destroy(BfHttpReadiness *pool);
/* Returns a blocking connected socket and one complete, NUL-terminated request.
 * Malformed, expired and over-capacity connections never reach a handler.
 * The caller owns the returned socket. -1 means the listener cannot continue. */
int bf_http_readiness_next(BfHttpReadiness *pool, int listener, char *request,
                          size_t *body_offset);
#endif
