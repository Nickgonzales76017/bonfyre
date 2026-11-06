#ifndef BONFYRE_ARROW_C_H
#define BONFYRE_ARROW_C_H

#include <arrow/c/abi.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Exports a lazy Parquet reader as the Arrow C Stream ABI. On success the
// caller owns out and must call out->release(out). No Python runtime or JSON
// materialization participates. Returns zero on success, EINVAL or EIO on
// failure; error_buffer receives a bounded diagnostic when supplied.
int bf_parquet_export_stream(const char *path, int64_t batch_size,
                             struct ArrowArrayStream *out,
                             char *error_buffer, size_t error_capacity);

// Manifold-bound variant: verifies the expected SHA-256 before exporting a
// stream from that same open file. Use this for digest-bearing representations.
int bf_parquet_export_verified_stream(const char *path, const char *sha256,
                                      int64_t batch_size, struct ArrowArrayStream *out,
                                      char *error_buffer, size_t error_capacity);

#ifdef __cplusplus
}
#endif

#endif
