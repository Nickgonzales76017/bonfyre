/*
 * bf_mmap.c — Memory-mapped file I/O implementation
 *
 * Strategy:
 *   1. stat() the file to get size
 *   2. If regular file with size > 0: mmap MAP_SHARED, MADV_SEQUENTIAL
 *   3. Otherwise: FAIL CLOSED — returns NULL, no read() fallback.
 *      Non-regular files (pipes, devices, zero-size) are not supported
 *      in the deterministic pipeline path.  Callers that require pipe
 *      support must use bf_mmap_open_fd() explicitly.
 *
 * No fread() or read() paths remain in this module.  Every pointer
 * returned by bf_mmap_view_acquire() is backed by an mmap region and
 * carries a memory_order_acquire fence so the caller sees fully
 * committed file contents.
 */

/* On macOS madvise/MADV_* are in <sys/mman.h> but require
 * BSD namespace (not restricted by _POSIX_C_SOURCE). */
#if defined(__APPLE__)
#  define _DARWIN_C_SOURCE
#elif defined(__linux__)
#  define _GNU_SOURCE
#else
#  define _POSIX_C_SOURCE 200809L
#endif
#include "bf_mmap.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdatomic.h>
#include <errno.h>
#include <stdint.h>

/* ── Handle ────────────────────────────────────────────────────── */

struct bf_mmap {
    void  *data;
    size_t size;
    int    is_mmap;   /* always 1: MAP_SHARED only, no malloc fallback */
    int    owns_fd;
    int    fd;
};

/* ── Lifecycle ──────────────────────────────────────────────────── */

bf_mmap_t *bf_mmap_open(const char *path) {
    if (!path) return NULL;

    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return NULL;

    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size == 0) {
        /* Not a regular file or empty — FAIL CLOSED (no read fallback) */
        close(fd);
        return NULL;
    }

    size_t fsize = (size_t)st.st_size;
    /* MAP_SHARED: avoids CoW page faults; all readers share one physical
     * mapping.  PROT_READ enforces no accidental write-back. */
    void *mapped = mmap(NULL, fsize, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        /* mmap failed — FAIL CLOSED (no read fallback) */
        close(fd);
        return NULL;
    }

    madvise(mapped, fsize, MADV_SEQUENTIAL);

    bf_mmap_t *m = calloc(1, sizeof(bf_mmap_t));
    if (!m) {
        munmap(mapped, fsize);
        close(fd);
        return NULL;
    }

    m->data    = mapped;
    m->size    = fsize;
    m->is_mmap = 1;
    m->fd      = fd;
    m->owns_fd = 1;
    return m;
}

bf_mmap_t *bf_mmap_open_fd(int fd, size_t hint_size) {
    if (fd < 0) return NULL;

    struct stat st;
    if (fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0) {
        size_t fsize = (size_t)st.st_size;
        void *mapped = mmap(NULL, fsize, PROT_READ, MAP_SHARED, fd, 0);
        if (mapped != MAP_FAILED) {
            madvise(mapped, fsize, MADV_SEQUENTIAL);
            bf_mmap_t *m = calloc(1, sizeof(bf_mmap_t));
            if (m) {
                m->data    = mapped;
                m->size    = fsize;
                m->is_mmap = 1;
                m->fd      = fd;
                m->owns_fd = 0;
                return m;
            }
            munmap(mapped, fsize);
        }
    }
    (void)hint_size;  /* no fallback: fail closed */
    return NULL;
}

void bf_mmap_close(bf_mmap_t *m) {
    if (!m) return;
    if (m->is_mmap)
        munmap(m->data, m->size);
    if (m->owns_fd && m->fd >= 0)
        close(m->fd);
    free(m);
}

/* ── Access ──────────────────────────────────────────────────────── */

const void *bf_mmap_data(const bf_mmap_t *m) {
    return m ? m->data : NULL;
}

const char *bf_mmap_str(const bf_mmap_t *m) {
    return m ? (const char *)m->data : NULL;
}

size_t bf_mmap_size(const bf_mmap_t *m) {
    return m ? m->size : 0;
}

int bf_mmap_is_mapped(const bf_mmap_t *m) {
    return m ? m->is_mmap : 0;
}

/* ── view_acquire ────────────────────────────────────────────────
 *
 * Return a pointer directly into the mmap region at `offset` of
 * `len` bytes, with a memory_order_acquire fence so the caller
 * sees fully committed file content (no stale cache lines).
 *
 * Requirements enforced (fail closed):
 *   - m must be a real MAP_SHARED mapping (is_mmap == 1)
 *   - offset + len must be within [0, m->size]
 *   - The returned address must be 64-byte aligned; if the mmap
 *     base+offset is not 64-byte aligned, returns NULL rather
 *     than a misaligned pointer.
 *
 * Returns NULL on any violation.
 */
const void *bf_mmap_view_acquire(const bf_mmap_t *m,
                                  size_t offset, size_t len) {
    if (!m || !m->is_mmap)              return NULL;
    if (len == 0)                        return NULL;
    if (offset > m->size)                return NULL;
    if (m->size - offset < len)          return NULL;

    const char *ptr = (const char *)m->data + offset;

    /* Enforce 64-byte alignment of the returned view pointer */
    if ((uintptr_t)ptr % 64 != 0)       return NULL;

    /* acquire fence: caller sees all writes that completed before
     * the mmap was established */
    atomic_thread_fence(memory_order_acquire);

    return ptr;
}

/* ── Advisory ────────────────────────────────────────────────────── */

int bf_mmap_advise_need(bf_mmap_t *m, size_t offset, size_t len) {
    if (!m || !m->is_mmap) return -1;
    if (offset + len > m->size) len = m->size - offset;
    return madvise((char *)m->data + offset, len, MADV_WILLNEED);
}

int bf_mmap_advise_done(bf_mmap_t *m, size_t offset, size_t len) {
    if (!m || !m->is_mmap) return -1;
    if (offset + len > m->size) len = m->size - offset;
    return madvise((char *)m->data + offset, len, MADV_DONTNEED);
}

/* ── Convenience ─────────────────────────────────────────────────── */

long bf_mmap_read_all(const char *path, char **out_buf) {
    if (!path || !out_buf) return -1;

    bf_mmap_t *m = bf_mmap_open(path);
    if (!m) return -1;

    size_t sz = bf_mmap_size(m);
    char *buf = malloc(sz + 1);
    if (!buf) {
        bf_mmap_close(m);
        return -1;
    }

    memcpy(buf, bf_mmap_data(m), sz);
    buf[sz] = '\0';
    bf_mmap_close(m);

    *out_buf = buf;
    return (long)sz;
}
