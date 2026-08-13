/* bf_kvcache_mount.c — Lazy KV sub-cache mounting (Git submodule analog).
 *
 * When bf_physics_step() returns +1 (Topological Gap — query has left the
 * populated manifold region), the caller mounts a new sub-cache pack for a
 * different model/domain.  The mounted vectors immediately become part of
 * the BVH's gravitational landscape for the NEXT integration step.
 *
 * Implementation:
 *   - Mount table: BF_MOUNT_MAX (16) simultaneous mounts.
 *   - Each mount is a MAP_SHARED | PROT_READ mmap of a pack file,
 *     identified by its model_hash (same 32-byte key used in kvcache_chain).
 *   - "Hot-plug" semantics: mount/umount at any time; BVH gradient calls
 *     check the mount table for additional vector sources.
 *   - Pack path resolution: ~/.local/share/bonfyre/kvcache/<hex>/pack.bfkvpack
 *     OR ~/.local/share/bonfyre/embeds/<hex>.bfpack (embed pack alias).
 *
 * Thread safety: none (single-threaded inference pipeline).
 */

#define _POSIX_C_SOURCE 200809L
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <limits.h>
#include "bonfyre.h"

#define BF_MOUNT_MAX 16

/* Internal mount registry */
static BfKVMount mount_table_[BF_MOUNT_MAX];
static int       mount_init_  = 0;

static void mount_table_init_(void) {
    if (mount_init_) return;
    memset(mount_table_, 0, sizeof(mount_table_));
    for (int i = 0; i < BF_MOUNT_MAX; i++) mount_table_[i].fd = -1;
    mount_init_ = 1;
}

/* ── path resolution ────────────────────────────────────────── */
static void hash_to_hex_(const uint8_t h[32], char out[65]) {
    static const char hc[] = "0123456789abcdef";
    for (int i = 0; i < 32; i++) { out[i*2]=hc[h[i]>>4]; out[i*2+1]=hc[h[i]&0xf]; }
    out[64] = '\0';
}

static int resolve_pack_path_(const uint8_t hash[32], char *path, size_t sz) {
    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    char hex[65]; hash_to_hex_(hash, hex);

    /* 1. kvcache pack: ~/.local/share/bonfyre/kvcache/<hex>/pack.bfkvpack */
    snprintf(path, sz, "%s/.local/share/bonfyre/kvcache/%s/pack.bfkvpack",
             home, hex);
    struct stat st;
    if (stat(path, &st) == 0) return 0;

    /* 2. embed pack alias: ~/.local/share/bonfyre/embeds/<hex>.bfpack */
    snprintf(path, sz, "%s/.local/share/bonfyre/embeds/%s.bfpack", home, hex);
    if (stat(path, &st) == 0) return 0;

    /* 3. global embed pack (hash = zeros → default pack) */
    int all_zero = 1;
    for (int i = 0; i < 32; i++) if (hash[i]) { all_zero = 0; break; }
    if (all_zero) {
        snprintf(path, sz, "%s/.local/share/bonfyre/embeds/pack.bfpack", home);
        if (stat(path, &st) == 0) return 0;
    }

    return -1;
}

/* ── bf_kvcache_mount ────────────────────────────────────────── */
int bf_kvcache_mount(const uint8_t hash[32], BfKVMount *out) {
    mount_table_init_();
    if (!hash || !out) return -1;

    /* Check if already mounted → return existing slot */
    for (int i = 0; i < BF_MOUNT_MAX; i++) {
        if (mount_table_[i].fd >= 0 &&
            memcmp(mount_table_[i].hash, hash, 32) == 0) {
            *out = mount_table_[i];
            return 0;
        }
    }

    /* Find a free slot */
    int slot = -1;
    for (int i = 0; i < BF_MOUNT_MAX; i++) {
        if (mount_table_[i].fd < 0) { slot = i; break; }
    }
    if (slot < 0) return -1;  /* mount table full */

    /* Resolve path */
    char path[PATH_MAX];
    if (resolve_pack_path_(hash, path, sizeof(path)) != 0) return -1;

    /* mmap MAP_SHARED PROT_READ */
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); return -1; }
    if (st.st_size == 0) { close(fd); return -1; }

    void *base = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) { close(fd); return -1; }

    BfKVMount *m = &mount_table_[slot];
    m->fd       = fd;
    m->base     = base;
    m->map_size = (size_t)st.st_size;
    m->readonly = 1;
    memcpy(m->hash, hash, 32);

    *out = *m;
    return 0;
}

/* ── bf_kvcache_umount ───────────────────────────────────────── */
int bf_kvcache_umount(BfKVMount *mount) {
    if (!mount || mount->fd < 0) return -1;
    if (mount->base && mount->base != MAP_FAILED)
        munmap(mount->base, mount->map_size);
    close(mount->fd);

    /* Clear from registry */
    for (int i = 0; i < BF_MOUNT_MAX; i++) {
        if (memcmp(mount_table_[i].hash, mount->hash, 32) == 0 &&
            mount_table_[i].fd == mount->fd) {
            memset(&mount_table_[i], 0, sizeof(mount_table_[i]));
            mount_table_[i].fd = -1;
            break;
        }
    }
    memset(mount, 0, sizeof(*mount));
    mount->fd = -1;
    return 0;
}

/* ── bf_kvcache_umount_all ───────────────────────────────────── */
void bf_kvcache_umount_all(void) {
    mount_table_init_();
    for (int i = 0; i < BF_MOUNT_MAX; i++) {
        if (mount_table_[i].fd >= 0) {
            if (mount_table_[i].base && mount_table_[i].base != MAP_FAILED)
                munmap(mount_table_[i].base, mount_table_[i].map_size);
            close(mount_table_[i].fd);
            memset(&mount_table_[i], 0, sizeof(mount_table_[i]));
            mount_table_[i].fd = -1;
        }
    }
}

/* ── bf_kvcache_mount_list ───────────────────────────────────── */
int bf_kvcache_mount_list(BfKVMount *out, int max_out, int *out_count) {
    mount_table_init_();
    if (!out || !out_count) return -1;
    *out_count = 0;
    for (int i = 0; i < BF_MOUNT_MAX && *out_count < max_out; i++) {
        if (mount_table_[i].fd >= 0)
            out[(*out_count)++] = mount_table_[i];
    }
    return 0;
}

/* ── bf_kvcache_mount_lookup ─────────────────────────────────── */
/* Look up a mounted sub-cache by hash, return its base pointer + size. */
const void *bf_kvcache_mount_lookup(const uint8_t hash[32], size_t *out_size) {
    mount_table_init_();
    if (!hash) return NULL;
    for (int i = 0; i < BF_MOUNT_MAX; i++) {
        if (mount_table_[i].fd >= 0 &&
            memcmp(mount_table_[i].hash, hash, 32) == 0) {
            if (out_size) *out_size = mount_table_[i].map_size;
            return mount_table_[i].base;
        }
    }
    return NULL;
}

/* ── bf_kvcache_mount_auto ───────────────────────────────────── */
/* Called on Topological Gap (bf_physics_step returns +1).
 * Walks the kvcache-chain ancestry of the given ctx_hash to find a related
 * model sub-cache, mounts it, returns 0 if a new cache was mounted. */
int bf_kvcache_mount_auto(const uint8_t model_hash[32],
                           const uint8_t ctx_hash[32],
                           BfKVMount *out_mount) {
    if (!model_hash || !ctx_hash || !out_mount) return -1;

    /* Walk ancestry up to 8 levels to find a related sub-cache hash */
    uint8_t chain[8][32];
    int depth = bf_kvcache_ancestry(model_hash, ctx_hash, chain, 8);
    if (depth <= 0) {
        /* No ancestry — try mounting default embed pack alias for model */
        return bf_kvcache_mount(model_hash, out_mount);
    }

    /* Try each ancestor's model hash as a sub-cache key */
    for (int i = 0; i < depth; i++) {
        if (bf_kvcache_mount(chain[i], out_mount) == 0) return 0;
    }
    return -1;
}
