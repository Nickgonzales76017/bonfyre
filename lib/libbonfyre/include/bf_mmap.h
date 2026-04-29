/*
 * bf_mmap.h — Memory-mapped file I/O for zero-copy reads
 *
 * Maps files read-only with MAP_PRIVATE + MADV_SEQUENTIAL.
 * Falls back to fread for non-mappable inputs (pipes, stdin).
 * Provides a unified interface regardless of backing.
 */

#ifndef BF_MMAP_H
#define BF_MMAP_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Mapped file handle ──────────────────────────────────────── */

typedef struct bf_mmap bf_mmap_t;

/* ── Lifecycle ───────────────────────────────────────────────── */

/*
 * Open and map a file for reading.
 * Returns NULL on failure.
 * If the file is not mappable (pipe, device), falls back to fread.
 */
bf_mmap_t *bf_mmap_open(const char *path);

/*
 * Map from an existing file descriptor.
 * fd is NOT owned — caller must close it after bf_mmap_close.
 * Useful for pipes/stdin fallback.
 */
bf_mmap_t *bf_mmap_open_fd(int fd, size_t hint_size);

/* Unmap and free resources. */
void bf_mmap_close(bf_mmap_t *m);

/* ── Access ──────────────────────────────────────────────────── */

/* Pointer to mapped data. Valid until bf_mmap_close. */
const void *bf_mmap_data(const bf_mmap_t *m);

/* Convenience: cast to const char*. */
const char *bf_mmap_str(const bf_mmap_t *m);

/* Size in bytes. */
size_t bf_mmap_size(const bf_mmap_t *m);

/* Whether the mapping is backed by mmap (vs fread fallback). */
int bf_mmap_is_mapped(const bf_mmap_t *m);

/* ── Advisory ────────────────────────────────────────────────── */

/* Hint that a region will be accessed soon. Calls madvise(MADV_WILLNEED). */
int bf_mmap_advise_need(bf_mmap_t *m, size_t offset, size_t len);

/* Hint that a region is no longer needed. Calls madvise(MADV_DONTNEED). */
int bf_mmap_advise_done(bf_mmap_t *m, size_t offset, size_t len);

/* ── Convenience: read entire file into a buffer ─────────────── */

/* Allocates and reads file into *out_buf. Caller must free().
 * Returns size or -1 on error. */
long bf_mmap_read_all(const char *path, char **out_buf);

#ifdef __cplusplus
}
#endif

#endif /* BF_MMAP_H */
