/*
 * bf_kvcache_pack.c — Pack format for the KV-cache object store
 *
 * The embed store has pack files (bf_embed_pack.c). The KV-cache store
 * gets the same treatment here. After accumulating many loose .bfkv files
 * (one per (model, context) pair), consolidate into a single mmap-able
 * pack for O(log n) lookup with zero per-read syscalls.
 *
 * KV blobs are variable-length (unlike embed vectors which are all dim*4
 * bytes), so the index stores (offset, length) pairs alongside the key.
 *
 * On-disk format (.bfkvpack):
 *
 *   Header (64 bytes):
 *     [0-3]   uint32 magic   = BFKVPK_MAGIC (0x504B4642 "BFKP")
 *     [4-7]   uint32 version = 1
 *     [8-11]  uint32 n       — number of entries
 *     [12-15] uint32 pad
 *     [16-23] uint64 idx_off = 64
 *     [24-31] uint64 dat_off = 64 + n*80
 *     [32-63] uint8[32] reserved
 *
 *   Index (sorted by (model_hash||ctx_hash), n × 80 bytes):
 *     [0-31]  uint8[32] model_hash
 *     [32-63] uint8[32] ctx_hash
 *     [64-71] uint64    dat_offset  — byte offset within data section
 *     [72-79] uint64    dat_len     — byte length of blob
 *
 *   Data (packed, variable-length):
 *     raw KV blobs, each of dat_len bytes at its dat_offset
 *
 * Lookup: binary search on 64-byte key (model_hash || ctx_hash).
 * Returns pointer into mmap'd data (valid until pack close) + len.
 * Zero copy, zero syscalls after open.
 *
 * GC: after packing, loose .bfkv files present in the pack can be removed.
 * bf_kvcache_fetch() still works — the pack takes priority, loose files
 * act as overflow for entries not yet packed.
 */
#define _DEFAULT_SOURCE
#include "include/bonfyre.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>

#define BFKVPK_MAGIC   0x504B4642u   /* "BFKP" */
#define BFKVPK_VERSION 1u
#define BFKVPK_HDR_SZ  64u
#define BFKVPK_IDX_SZ  80u   /* 32+32+8+8 per entry */
#define MAX_KP         4096

/* ── helpers ─────────────────────────────────────────────────── */

static const char *kp_home_(void) {
    const char *h = getenv("HOME");
    return h ? h : "/tmp";
}

static void kvcache_dir_kp_(const char *model_hex, char *buf, size_t sz) {
    snprintf(buf, sz, "%s/.local/share/bonfyre/kvcache/%s",
             kp_home_(), model_hex);
}

static void kvcache_base_kp_(char *buf, size_t sz) {
    snprintf(buf, sz, "%s/.local/share/bonfyre/kvcache", kp_home_());
}

static void hash_to_hex_kp_(const uint8_t h[32], char hex[65]) {
    static const char hc[] = "0123456789abcdef";
    for (int i = 0; i < 32; i++) {
        hex[i*2]   = hc[h[i] >> 4];
        hex[i*2+1] = hc[h[i] & 0xf];
    }
    hex[64] = '\0';
}

static int hex_to_hash_kp_(const char *hex, uint8_t out[32]) {
    if (strlen(hex) < 64) return -1;
    for (int i = 0; i < 32; i++) {
        unsigned v;
        if (sscanf(hex + i*2, "%02x", &v) != 1) return -1;
        out[i] = (uint8_t)v;
    }
    return 0;
}

/* ── build ───────────────────────────────────────────────────── */

typedef struct {
    uint8_t  model_hash[32];
    uint8_t  ctx_hash[32];
    uint8_t *data;
    uint64_t len;
} KPEnt_;

static int cmp_kp_ent_(const void *a, const void *b) {
    const KPEnt_ *x = (const KPEnt_ *)a;
    const KPEnt_ *y = (const KPEnt_ *)b;
    int c = memcmp(x->model_hash, y->model_hash, 32);
    if (c != 0) return c;
    return memcmp(x->ctx_hash, y->ctx_hash, 32);
}

/*
 * bf_kvcache_pack_build — consolidate loose .bfkv files into a pack.
 *
 * Scans ~/.local/share/bonfyre/kvcache/<model_hex>/*.bfkv and loads each
 * blob (stripping the 12-byte KV_MAGIC+len header), then writes a sorted
 * pack file at pack_path.  *out_n = entries written.
 * Returns 0 on success, -1 on I/O error.
 */
int bf_kvcache_pack_build(const char *pack_path, uint32_t *out_n) {
    char base[MAX_KP];
    kvcache_base_kp_(base, sizeof(base));

    size_t cap = 64, n = 0;
    KPEnt_ *ents = malloc(cap * sizeof(KPEnt_));
    if (!ents) return -1;

    /* Walk kvcache/<model_hex>/<ctx_hex>.bfkv */
    DIR *mdir = opendir(base);
    if (!mdir) { *out_n = 0; free(ents); return 0; }

    struct dirent *mde;
    while ((mde = readdir(mdir))) {
        if (mde->d_name[0] == '.') continue;
        if (strlen(mde->d_name) != 64) continue;

        uint8_t model_hash[32];
        if (hex_to_hash_kp_(mde->d_name, model_hash) != 0) continue;

        char mpath[MAX_KP];
        kvcache_dir_kp_(mde->d_name, mpath, sizeof(mpath));

        DIR *cdir = opendir(mpath);
        if (!cdir) continue;

        struct dirent *cde;
        while ((cde = readdir(cdir))) {
            const char *nm = cde->d_name;
            size_t nl = strlen(nm);
            if (nl != 69 || strcmp(nm + 64, ".bfkv") != 0) continue;

            uint8_t ctx_hash[32];
            if (hex_to_hash_kp_(nm, ctx_hash) != 0) continue;

            char fpath[MAX_KP];
            snprintf(fpath, sizeof(fpath), "%s/%s", mpath, nm);

            FILE *f = fopen(fpath, "rb");
            if (!f) continue;

            /* Skip the .bfkv header (magic u32 + len u64 = 12 bytes) */
            uint32_t magic; uint64_t flen;
            if (fread(&magic, 4, 1, f) != 1 ||
                fread(&flen,  8, 1, f) != 1 || flen == 0 || flen > 256*1024*1024UL) {
                fclose(f); continue;
            }

            uint8_t *blob = malloc((size_t)flen);
            if (!blob || fread(blob, 1, (size_t)flen, f) != (size_t)flen) {
                free(blob); fclose(f); continue;
            }
            fclose(f);

            if (n == cap) {
                cap *= 2;
                KPEnt_ *tmp = realloc(ents, cap * sizeof(KPEnt_));
                if (!tmp) { free(blob); break; }
                ents = tmp;
            }
            memcpy(ents[n].model_hash, model_hash, 32);
            memcpy(ents[n].ctx_hash,   ctx_hash,   32);
            ents[n].data = blob;
            ents[n].len  = flen;
            n++;
        }
        closedir(cdir);
    }
    closedir(mdir);

    if (n == 0) { free(ents); *out_n = 0; return 0; }

    qsort(ents, n, sizeof(KPEnt_), cmp_kp_ent_);

    /* Compute data offsets */
    uint64_t dat_off = BFKVPK_HDR_SZ + (uint64_t)n * BFKVPK_IDX_SZ;
    uint64_t running = 0;

    char tmp[MAX_KP + 4];
    snprintf(tmp, sizeof(tmp), "%s.tmp", pack_path);
    FILE *pf = fopen(tmp, "wb");
    if (!pf) goto fail;

    /* Header */
    uint32_t magic   = BFKVPK_MAGIC;
    uint32_t version = BFKVPK_VERSION;
    uint32_t cnt     = (uint32_t)n;
    uint32_t pad     = 0;
    uint64_t idx_off = BFKVPK_HDR_SZ;
    uint8_t  resv[32] = {0};

    int ok = 1;
    ok &= (fwrite(&magic,   4,  1, pf) == 1);
    ok &= (fwrite(&version, 4,  1, pf) == 1);
    ok &= (fwrite(&cnt,     4,  1, pf) == 1);
    ok &= (fwrite(&pad,     4,  1, pf) == 1);
    ok &= (fwrite(&idx_off, 8,  1, pf) == 1);
    ok &= (fwrite(&dat_off, 8,  1, pf) == 1);
    ok &= (fwrite(resv,     32, 1, pf) == 1);  /* total = 64 */

    /* Index */
    for (size_t i = 0; i < n; i++) {
        uint64_t offset = running;
        uint64_t len    = ents[i].len;
        ok &= (fwrite(ents[i].model_hash, 1, 32, pf) == 32);
        ok &= (fwrite(ents[i].ctx_hash,   1, 32, pf) == 32);
        ok &= (fwrite(&offset,            8,  1, pf) == 1);
        ok &= (fwrite(&len,               8,  1, pf) == 1);
        running += len;
    }

    /* Data */
    for (size_t i = 0; i < n; i++) {
        ok &= (fwrite(ents[i].data, 1, (size_t)ents[i].len, pf) == ents[i].len);
        free(ents[i].data);
    }
    free(ents);

    int rc = (ferror(pf) == 0 && ok);
    fclose(pf);
    if (rc) { rename(tmp, pack_path); *out_n = (uint32_t)n; return 0; }
    unlink(tmp);
    return -1;

fail:
    for (size_t i = 0; i < n; i++) free(ents[i].data);
    free(ents);
    return -1;
}

/* ── open / close ────────────────────────────────────────────── */

int bf_kvcache_pack_open(BfKVCachePack *pack, const char *pack_path) {
    memset(pack, 0, sizeof(*pack));

    int fd = open(pack_path, O_RDONLY);
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < (off_t)BFKVPK_HDR_SZ) {
        close(fd); return -1;
    }

    void *base = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) { close(fd); return -1; }

    const uint8_t *b = (const uint8_t *)base;
    uint32_t magic, version, nn;
    uint64_t idx_off, dat_off;
    memcpy(&magic,   b,      4);
    memcpy(&version, b +  4, 4);
    memcpy(&nn,      b +  8, 4);
    memcpy(&idx_off, b + 16, 8);
    memcpy(&dat_off, b + 24, 8);

    if (magic != BFKVPK_MAGIC || version != BFKVPK_VERSION ||
        idx_off < BFKVPK_HDR_SZ ||
        dat_off < idx_off + (uint64_t)nn * BFKVPK_IDX_SZ ||
        (off_t)dat_off > st.st_size) {
        munmap(base, (size_t)st.st_size);
        close(fd);
        return -1;
    }

    pack->fd         = fd;
    pack->base       = base;
    pack->map_size   = (size_t)st.st_size;
    pack->n          = nn;
    pack->index_base = b + idx_off;
    pack->data_base  = b + dat_off;
    return 0;
}

void bf_kvcache_pack_close(BfKVCachePack *pack) {
    if (!pack) return;
    if (pack->base && pack->map_size) munmap(pack->base, pack->map_size);
    if (pack->fd > 0) close(pack->fd);
    memset(pack, 0, sizeof(*pack));
}

/* ── lookup ──────────────────────────────────────────────────── */

/*
 * bf_kvcache_pack_lookup — O(log n) binary search.
 *
 * Returns pointer into mmap'd data (valid until pack close) and sets
 * *out_len. Returns NULL on miss.  Do NOT free the returned pointer.
 */
const void *bf_kvcache_pack_lookup(const BfKVCachePack *pack,
                                    const uint8_t model_hash[32],
                                    const uint8_t ctx_hash[32],
                                    uint64_t *out_len) {
    if (!pack || !pack->base || pack->n == 0) return NULL;

    uint32_t lo = 0, hi = pack->n;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        const uint8_t *entry = pack->index_base + (size_t)mid * BFKVPK_IDX_SZ;
        int cm = memcmp(model_hash, entry, 32);
        int cc = (cm == 0) ? memcmp(ctx_hash, entry + 32, 32) : cm;
        if (cc == 0) {
            uint64_t offset, len;
            memcpy(&offset, entry + 64, 8);
            memcpy(&len,    entry + 72, 8);
            if (out_len) *out_len = len;
            return pack->data_base + offset;
        }
        if (cc < 0) hi  = mid;
        else        lo  = mid + 1;
    }
    return NULL;
}

/*
 * bf_kvcache_pack_gc — remove loose .bfkv files already in the pack.
 *
 * Walks the loose kvcache directory and unlinks any file whose
 * (model_hash, ctx_hash) key is present in the pack.
 * Returns number of files removed.
 */
int bf_kvcache_pack_gc(const BfKVCachePack *pack) {
    if (!pack || !pack->base) return 0;
    char base[MAX_KP];
    kvcache_base_kp_(base, sizeof(base));

    int removed = 0;
    DIR *mdir = opendir(base);
    if (!mdir) return 0;

    struct dirent *mde;
    while ((mde = readdir(mdir))) {
        if (mde->d_name[0] == '.' || strlen(mde->d_name) != 64) continue;
        uint8_t model_hash[32];
        if (hex_to_hash_kp_(mde->d_name, model_hash) != 0) continue;

        char mpath[MAX_KP];
        kvcache_dir_kp_(mde->d_name, mpath, sizeof(mpath));

        DIR *cdir = opendir(mpath);
        if (!cdir) continue;
        struct dirent *cde;
        while ((cde = readdir(cdir))) {
            const char *nm = cde->d_name;
            size_t nl = strlen(nm);
            if (nl != 69 || strcmp(nm + 64, ".bfkv") != 0) continue;
            uint8_t ctx_hash[32];
            if (hex_to_hash_kp_(nm, ctx_hash) != 0) continue;
            uint64_t dummy;
            if (bf_kvcache_pack_lookup(pack, model_hash, ctx_hash, &dummy)) {
                char fp[MAX_KP];
                snprintf(fp, sizeof(fp), "%s/%s", mpath, nm);
                unlink(fp);
                removed++;
            }
        }
        closedir(cdir);
    }
    closedir(mdir);
    return removed;
}
