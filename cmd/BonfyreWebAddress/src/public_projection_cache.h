#ifndef BONFYRE_PUBLIC_PROJECTION_CACHE_H
#define BONFYRE_PUBLIC_PROJECTION_CACHE_H

#include <stddef.h>
#include <stdio.h>

/* A public projection cache is a disposable compiled body, never an owner.
 * The cache knows a closure key and its declared dependency coordinates; it
 * deliberately contains no actor, authority, evidence or effect state. */
int bf_public_projection_cache_read(const char *db_path, const char *closure_key,
                                    const char *source_fingerprint, FILE *out);
int bf_public_projection_cache_write(const char *db_path, const char *closure_key,
                                     const char *source_fingerprint,
                                     const void *response, size_t response_len,
                                     const char *const *dependencies,
                                     size_t dependency_count,
                                     char digest_out[65]);
/* Only successful HTTP responses belong in a reusable public projection. */
int bf_public_projection_response_cacheable(const void *response, size_t length);
/* Stream a captured response without a cache-size or allocation limit. */
int bf_public_projection_forward(FILE *capture, FILE *out);
int bf_public_projection_cache_invalidate(const char *db_path,
                                          const char *dependency,
                                          int *invalidated_out);

#endif
