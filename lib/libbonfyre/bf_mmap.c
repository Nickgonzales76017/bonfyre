/*
 * bf_mmap.c — Memory-mapped file I/O implementation
 *
 * Strategy:
 *   1. stat() the file to get size
 *   2. If regular file with size > 0: mmap MAP_PRIVATE, MADV_SEQUENTIAL
 *   3. Otherwise (pipe, zero-size, device): fread into malloc'd buffer
 */

#include "bf_mmap.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <errno.h>

/* ── Handle ──────────────────────────────────────────────────── */

struct bf_mmap {
    void  *data;
    size_t size;
    int    is_mmap;   /* 1 = mmap, 0 = malloc'd buffer */
    int    owns_fd;
    int    fd;
};

/* ── fread fallback ──────────────────────────────────────────── */

static bf_mmap_t *fallback_read_fd(int fd, size_t hint) {
    size_t cap = hint > 0 ? hint : 65536;
    size_t len = 0;
    char *buf = malloc(cap);
    if (!buf) return NULL;

    for (;;) {
        if (len >= cap) {
            cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); return NULL; }
            buf = nb;
        }
        ssize_t r = read(fd, buf + len, cap - len);
        if (r <= 0) break;
        len += (size_t)r;
    }

    /* Null-terminate for string convenience (extra byte) */
    char *final = realloc(buf, len + 1);
    if (final) {
        buf = final;
        buf[len] = '\0';
    }

    bf_mmap_t *m = calloc(1, sizeof(bf_mmap_t));
    if (!m) { free(buf); return NULL; }
    m->data = buf;
    m->size = len;
    m->is_mmap = 0;
    m->fd = -1;
    return m;
}

static bf_mmap_t *fallback_read_file(const char *path) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return NULL;
    bf_mmap_t *m = fallback_read_fd(fd, 0);
    close(fd);
    return m;
}

/* ── Lifecycle ───────────────────────────────────────────────── */

bf_mmap_t *bf_mmap_open(const char *path) {
    if (!path) return NULL;

    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return NULL;

    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size == 0) {
        /* Not a regular file or empty — fallback */
        bf_mmap_t *m = fallback_read_fd(fd, (size_t)st.st_size);
        close(fd);
        return m;
    }

    size_t fsize = (size_t)st.st_size;
    void *mapped = mmap(NULL, fsize, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        /* mmap failed — fallback to read */
        bf_mmap_t *m = fallback_read_fd(fd, fsize);
        close(fd);
        return m;
    }

    /* Advisory hint for sequential access */
    madvise(mapped, fsize, MADV_SEQUENTIAL);

    bf_mmap_t *m = calloc(1, sizeof(bf_mmap_t));
    if (!m) {
        munmap(mapped, fsize);
        close(fd);
        return NULL;
    }

    m->data = mapped;
    m->size = fsize;
    m->is_mmap = 1;
    m->fd = fd;
    m->owns_fd = 1;
    return m;
}

bf_mmap_t *bf_mmap_open_fd(int fd, size_t hint_size) {
    if (fd < 0) return NULL;

    struct stat st;
    if (fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0) {
        size_t fsize = (size_t)st.st_size;
        void *mapped = mmap(NULL, fsize, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapped != MAP_FAILED) {
            madvise(mapped, fsize, MADV_SEQUENTIAL);
            bf_mmap_t *m = calloc(1, sizeof(bf_mmap_t));
            if (m) {
                m->data = mapped;
                m->size = fsize;
                m->is_mmap = 1;
                m->fd = fd;
                m->owns_fd = 0;
                return m;
            }
            munmap(mapped, fsize);
        }
    }

    return fallback_read_fd(fd, hint_size);
}

void bf_mmap_close(bf_mmap_t *m) {
    if (!m) return;
    if (m->is_mmap) {
        munmap(m->data, m->size);
    } else {
        free(m->data);
    }
    if (m->owns_fd && m->fd >= 0) {
        close(m->fd);
    }
    free(m);
}

/* ── Access ──────────────────────────────────────────────────── */

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

/* ── Advisory ────────────────────────────────────────────────── */

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

/* ── Convenience ─────────────────────────────────────────────── */

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
