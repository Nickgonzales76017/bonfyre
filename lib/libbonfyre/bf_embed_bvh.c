/* bf_embed_bvh.c — Ball-tree BVH over the embedding pack.
 *
 * Provides two services:
 *   1. KDE gradient  ∇V(q) = (1/σ²) Σ_i (q−kᵢ) exp(−‖q−kᵢ‖²/2σ²)
 *      used by bf_physics.c for Hamiltonian Leapfrog integration.
 *   2. Ternary sketch collision pre-filter (256-bit sign sketch, fast
 *      POPCOUNT agreement check) to skip entire subtrees in O(1).
 *
 * Binary format  (magic 0x48564246 "BFVH", version 1):
 *
 *   [Header  64B]
 *   [Node array: n_nodes × 64B]
 *   [Centers: n_nodes × dim × 4B (float32)]
 *   [Indices: n_vecs × 4B (uint32)]
 *
 * Header 64B:
 *   magic(4) + version(4) + n_nodes(4) + dim(4) + n_vecs(8) +
 *   leaf_size(4) + nodes_off(8) + centers_off(8) + indices_off(8) + pad(12)
 *
 * Node 64B:
 *   radius(4) + left(4) + right(4) + vec_start(4) + vec_count(4) +
 *   sketch[4×uint64=32] + pad(8)
 *   [center is at centers_base + node_idx * dim * 4]
 */

#define _POSIX_C_SOURCE 200809L
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <limits.h>
#include "bonfyre.h"

/* ── constants ──────────────────────────────────────────────── */
#define BVH_MAGIC       0x48564246u   /* "BFVH" */
#define BVH_VERSION     1u
#define BVH_LEAF_SIZE   16u
#define BVH_SKETCH_DIMS 256u          /* first 256 dims used for sketch */
#define BVH_PRUNE_SIGMA 4.0f          /* prune if dist(q,center)-r > n·σ */
#define BVH_SKETCH_THRESH 26          /* min agreeing bits out of 256 */

/* ── on-disk node (64B) ─────────────────────────────────────── */
typedef struct __attribute__((packed)) {
    float    radius;          /*  4 */
    int32_t  left;            /*  4  (-1 = leaf) */
    int32_t  right;           /*  4 */
    uint32_t vec_start;       /*  4  index into indices[] section */
    uint32_t vec_count;       /*  4 */
    uint64_t sketch[4];       /* 32  256-bit ternary sign sketch */
    uint8_t  pad[8];          /*  8 */
} BVHNodeDisk;                /* = 64 */

/* ── on-disk header (64B) ───────────────────────────────────── */
typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t n_nodes;
    uint32_t dim;
    uint64_t n_vecs;
    uint32_t leaf_size;
    uint64_t nodes_off;
    uint64_t centers_off;
    uint64_t indices_off;
    uint8_t  pad[12];
} BVHHeader;                  /* = 64 */

/* ── build-time helpers ─────────────────────────────────────── */

/* Read float vector for pack index i (works for v1 and v2 packs via
   data_base which always holds the canonical float32 data for v1; for v2
   we fall back to bf_embed_pack_get which dequantizes). */
static int pack_vec_(const BfEmbedPack *pack, uint32_t idx, float *out) {
    /* Use the canonical 40-byte-stride accessor.  All version logic lives
     * in bf_embed_pack_vec_at — the BVH must NOT duplicate that layout. */
    return bf_embed_pack_vec_at(pack, idx, out);
}

static void compute_sketch_(const float *center, uint32_t dim,
                             uint64_t sketch[4]) {
    memset(sketch, 0, 32);
    uint32_t lim = dim < BVH_SKETCH_DIMS ? dim : BVH_SKETCH_DIMS;
    for (uint32_t b = 0; b < lim; b++)
        if (center[b] > 0.0f)
            sketch[b >> 6] |= (1ULL << (b & 63u));
}

/* Recursive build — returns allocated node index, fills nodes[]/centers[] */
typedef struct {
    BVHNodeDisk *nodes;
    float       *centers;   /* n_alloc × dim floats */
    uint32_t    *indices;   /* BVH traversal order */
    uint32_t     node_count;
    uint32_t     n_alloc;
    uint32_t     dim;
    uint32_t     leaf_size;
    const BfEmbedPack *pack;
} BuildCtx;

static float *tmp_vecs_ = NULL; /* thread-unsafe scratch — single-threaded build */
static uint32_t tmp_vecs_n_ = 0;

static float *get_tmp_vec_(BuildCtx *ctx, uint32_t idx) {
    /* cache is flat: n_alloc × dim */
    if (!tmp_vecs_) {
        tmp_vecs_n_ = ctx->pack->n;
        tmp_vecs_ = calloc((size_t)tmp_vecs_n_ * ctx->dim, sizeof(float));
        if (!tmp_vecs_) return NULL;
    }
    float *p = tmp_vecs_ + (size_t)idx * ctx->dim;
    /* lazy populate: first float is 0.0f if dim>0 and not set — we mark
       with NaN sentinel at [0] when stored */
    if (isnan(p[0])) return NULL; /* already failed */
    if (p[0] == 0.0f && ctx->dim > 1) {
        if (pack_vec_(ctx->pack, idx, p) != 0) {
            p[0] = NAN; return NULL;
        }
    }
    return p;
}

static int32_t build_node_(BuildCtx *ctx, uint32_t *sub_idx, uint32_t n,
                            uint32_t vec_start_in_indices) {
    if (ctx->node_count >= ctx->n_alloc) return -2;
    uint32_t my = ctx->node_count++;
    BVHNodeDisk *nd = &ctx->nodes[my];
    float *ctr = ctx->centers + (size_t)my * ctx->dim;

    /* --- compute center (mean) --- */
    memset(ctr, 0, ctx->dim * sizeof(float));
    uint32_t valid = 0;
    for (uint32_t i = 0; i < n; i++) {
        float *v = get_tmp_vec_(ctx, sub_idx[i]);
        if (!v) continue;
        for (uint32_t d = 0; d < ctx->dim; d++) ctr[d] += v[d];
        valid++;
    }
    if (valid > 0) {
        float inv = 1.0f / (float)valid;
        for (uint32_t d = 0; d < ctx->dim; d++) ctr[d] *= inv;
    }

    /* --- compute radius --- */
    float radius = 0.0f;
    for (uint32_t i = 0; i < n; i++) {
        float *v = get_tmp_vec_(ctx, sub_idx[i]);
        if (!v) continue;
        float d2 = 0.0f;
        for (uint32_t d = 0; d < ctx->dim; d++) {
            float diff = v[d] - ctr[d]; d2 += diff * diff;
        }
        float dist = sqrtf(d2);
        if (dist > radius) radius = dist;
    }

    nd->radius    = radius;
    nd->vec_start = vec_start_in_indices;
    nd->vec_count = n;
    nd->left      = -1;
    nd->right     = -1;
    memset(nd->pad, 0, sizeof(nd->pad));
    compute_sketch_(ctr, ctx->dim, nd->sketch);

    if (n <= ctx->leaf_size) return (int32_t)my;

    /* --- split: max-variance dimension --- */
    uint32_t split_dim = 0; float best_var = -1.0f;
    /* estimate variance from center deviation — use random sample ≤128 */
    for (uint32_t d = 0; d < ctx->dim; d++) {
        float var = 0.0f;
        uint32_t lim = n < 128 ? n : 128;
        for (uint32_t i = 0; i < lim; i++) {
            float *v = get_tmp_vec_(ctx, sub_idx[i]);
            if (!v) continue;
            float diff = v[d] - ctr[d]; var += diff * diff;
        }
        if (var > best_var) { best_var = var; split_dim = d; }
    }

    /* sort sub_idx by split_dim (insertion sort — n≤few thousand) */
    for (uint32_t i = 1; i < n; i++) {
        uint32_t key = sub_idx[i];
        float *kv = get_tmp_vec_(ctx, key);
        float kval = kv ? kv[split_dim] : 0.0f;
        int32_t j = (int32_t)i - 1;
        while (j >= 0) {
            float *jv = get_tmp_vec_(ctx, sub_idx[j]);
            float jval = jv ? jv[split_dim] : 0.0f;
            if (jval <= kval) break;
            sub_idx[j+1] = sub_idx[j]; j--;
        }
        sub_idx[j+1] = key;
    }

    uint32_t mid = n / 2;
    int32_t lc = build_node_(ctx, sub_idx,       mid,   vec_start_in_indices);
    int32_t rc = build_node_(ctx, sub_idx + mid, n-mid, vec_start_in_indices + mid);
    ctx->nodes[my].left  = lc;
    ctx->nodes[my].right = rc;
    return (int32_t)my;
}

/* ── bf_embed_bvh_build ─────────────────────────────────────── */
int bf_embed_bvh_build(const char *pack_path, const char *bvh_path) {
    BfEmbedPack pack = {0};
    if (bf_embed_pack_open(&pack, pack_path) != 0) return -1;

    uint32_t n   = pack.n;
    uint32_t dim = pack.dim;
    if (n == 0 || dim == 0) { bf_embed_pack_close(&pack); return -1; }

    /* pre-populate vector cache */
    float *vecs = calloc((size_t)n * dim, sizeof(float));
    if (!vecs) { bf_embed_pack_close(&pack); return -1; }
    for (uint32_t i = 0; i < n; i++)
        pack_vec_(&pack, i, vecs + (size_t)i * dim);
    tmp_vecs_   = vecs;
    tmp_vecs_n_ = n;

    /* allocate node/center/index arrays (upper bound: 2*n/leaf + 4) */
    uint32_t max_nodes = 2u * (n / BVH_LEAF_SIZE + 2u) + 4u;
    BVHNodeDisk *nodes   = calloc(max_nodes, sizeof(BVHNodeDisk));
    float       *centers = calloc((size_t)max_nodes * dim, sizeof(float));
    uint32_t    *indices = malloc(n * sizeof(uint32_t));
    if (!nodes || !centers || !indices) goto fail;

    for (uint32_t i = 0; i < n; i++) indices[i] = i;

    BuildCtx ctx = {
        .nodes = nodes, .centers = centers, .indices = indices,
        .node_count = 0, .n_alloc = max_nodes,
        .dim = dim, .leaf_size = BVH_LEAF_SIZE, .pack = &pack
    };
    if (build_node_(&ctx, indices, n, 0) < 0) goto fail;
    uint32_t n_nodes = ctx.node_count;

    /* --- write file --- */
    uint64_t nodes_off   = sizeof(BVHHeader);
    uint64_t centers_off = nodes_off   + (uint64_t)n_nodes * sizeof(BVHNodeDisk);
    uint64_t indices_off = centers_off + (uint64_t)n_nodes * dim * sizeof(float);

    BVHHeader hdr = {
        .magic       = BVH_MAGIC,
        .version     = BVH_VERSION,
        .n_nodes     = n_nodes,
        .dim         = dim,
        .n_vecs      = n,
        .leaf_size   = BVH_LEAF_SIZE,
        .nodes_off   = nodes_off,
        .centers_off = centers_off,
        .indices_off = indices_off,
    };
    memset(hdr.pad, 0, sizeof(hdr.pad));

    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s.tmp.%d", bvh_path, (int)getpid());
    FILE *f = fopen(tmp, "wb");
    if (!f) goto fail;
    fwrite(&hdr,  1, sizeof(hdr),   f);
    fwrite(nodes, sizeof(BVHNodeDisk), n_nodes, f);
    fwrite(centers, sizeof(float), (size_t)n_nodes * dim, f);
    fwrite(indices, sizeof(uint32_t), n, f);
    fclose(f);
    if (rename(tmp, bvh_path) != 0) { unlink(tmp); goto fail; }

    free(nodes); free(centers); free(indices);
    tmp_vecs_ = NULL; free(vecs);
    bf_embed_pack_close(&pack);
    return 0;

fail:
    free(nodes); free(centers); free(indices);
    tmp_vecs_ = NULL; free(vecs);
    bf_embed_pack_close(&pack);
    return -1;
}

/* ── bf_embed_bvh_open / close ──────────────────────────────── */
int bf_embed_bvh_open(BfEmbedBVH *bvh, const char *path) {
    if (!bvh || !path) return -1;
    memset(bvh, 0, sizeof(*bvh));
    bvh->fd = open(path, O_RDONLY);
    if (bvh->fd < 0) return -1;
    struct stat st;
    if (fstat(bvh->fd, &st) != 0) goto fail;
    bvh->map_size = (size_t)st.st_size;
    bvh->base = mmap(NULL, bvh->map_size, PROT_READ, MAP_SHARED, bvh->fd, 0);
    if (bvh->base == MAP_FAILED) goto fail;

    const BVHHeader *h = (const BVHHeader *)bvh->base;
    if (h->magic != BVH_MAGIC || h->version != BVH_VERSION) goto fail2;

    bvh->n_nodes    = h->n_nodes;
    bvh->dim        = h->dim;
    bvh->n_vecs     = h->n_vecs;
    bvh->nodes_base = (const BVHNodeDisk *)((const uint8_t *)bvh->base + h->nodes_off);
    bvh->centers    = (const float *)((const uint8_t *)bvh->base + h->centers_off);
    bvh->idx_base   = (const uint32_t *)((const uint8_t *)bvh->base + h->indices_off);
    return 0;

fail2: munmap(bvh->base, bvh->map_size);
fail:  close(bvh->fd); memset(bvh, 0, sizeof(*bvh)); return -1;
}

void bf_embed_bvh_close(BfEmbedBVH *bvh) {
    if (!bvh) return;
    if (bvh->base && bvh->base != MAP_FAILED)
        munmap(bvh->base, bvh->map_size);
    if (bvh->fd >= 0) close(bvh->fd);
    memset(bvh, 0, sizeof(*bvh));
}

/* ── KDE gradient traversal ─────────────────────────────────── */

static int sketch_agreement_(const uint64_t a[4], const uint64_t b[4]) {
    /* count agreeing sign bits: popcount(~(a XOR b)) over 256 bits */
    int agree = 0;
    for (int w = 0; w < 4; w++) {
        uint64_t match = ~(a[w] ^ b[w]);
        /* portable popcount */
        match = match - ((match >> 1) & 0x5555555555555555ULL);
        match = (match & 0x3333333333333333ULL) + ((match >> 2) & 0x3333333333333333ULL);
        match = (match + (match >> 4)) & 0x0f0f0f0f0f0f0f0fULL;
        agree += (int)((match * 0x0101010101010101ULL) >> 56);
    }
    return agree;
}

/* Recursive gradient + potential accumulation.
 * grad[]      — dim-sized, accumulates ∇V = Σ w*(q-k)/σ²
 * potential   — scalar, accumulates V = -Σ exp(-‖q-k‖²/(2σ²)); may be NULL */
static void grad_traverse_(const BfEmbedBVH *bvh, const BfEmbedPack *pack,
                            uint32_t node_idx, const float *q,
                            float sigma_sq, float *grad, float *potential,
                            const uint64_t q_sketch[4]) {
    if (node_idx >= bvh->n_nodes) return;
    const BVHNodeDisk *nd = (const BVHNodeDisk *)bvh->nodes_base + node_idx;
    const float *ctr = bvh->centers + (size_t)node_idx * bvh->dim;

    /* Ternary pre-filter */
    int agree = sketch_agreement_(q_sketch, nd->sketch);
    if (agree < BVH_SKETCH_THRESH) return;

    /* BVH sphere prune: dist(q, center) - radius > BVH_PRUNE_SIGMA * sqrt(sigma_sq) */
    float dist2 = 0.0f;
    for (uint32_t d = 0; d < bvh->dim; d++) {
        float diff = q[d] - ctr[d]; dist2 += diff * diff;
    }
    float dist = sqrtf(dist2);
    float sigma = sqrtf(sigma_sq);
    if (dist - nd->radius > BVH_PRUNE_SIGMA * sigma) return;

    /* Leaf: accumulate exact KDE gradient (and potential) for each vector */
    if (nd->left == -1) {
        float kbuf[4096];
        for (uint32_t vi = 0; vi < nd->vec_count; vi++) {
            uint32_t pack_idx = bvh->idx_base[nd->vec_start + vi];
            if (pack_idx >= pack->n) continue;

            /* v1 fast path: direct pointer into mmap'd float array.
             * data_idx in the 40-byte index entry is a VECTOR INDEX,
             * not a byte offset.  Use the canonical accessor so we
             * never duplicate the stride logic here. */
            const float *k = NULL;
            if (((const uint32_t *)pack->base)[1] == 1u) {
                /* v1: can compute direct pointer safely */
                const uint8_t *entry = pack->index_base
                                       + (size_t)pack_idx * 40u; /* PACK_IDX_ENTRY */
                uint64_t data_idx; memcpy(&data_idx, entry + 32, 8);
                k = pack->data_base + data_idx * pack->dim;
            } else {
                /* v2 / unknown: copy through canonical accessor */
                uint32_t dim_chk = pack->dim < 4096u ? pack->dim : 4096u;
                if (bf_embed_pack_vec_at(pack, pack_idx, kbuf) == 0) {
                    (void)dim_chk; k = kbuf;
                }
            }
            if (!k) continue;
            float d2_ = 0.0f;
            for (uint32_t d = 0; d < bvh->dim; d++) {
                float diff = q[d] - k[d]; d2_ += diff * diff;
            }
            float w = expf(-d2_ / (2.0f * sigma_sq)); /* Gaussian kernel */
            for (uint32_t d = 0; d < bvh->dim; d++)
                grad[d] += (q[d] - k[d]) / sigma_sq * w;
            /* V(q) = -Σ K(q,k) so each leaf contributes -w to the potential */
            if (potential) *potential -= w;
        }
        return;
    }

    /* Internal node: recurse */
    if (nd->left  >= 0) grad_traverse_(bvh, pack, (uint32_t)nd->left,  q, sigma_sq, grad, potential, q_sketch);
    if (nd->right >= 0) grad_traverse_(bvh, pack, (uint32_t)nd->right, q, sigma_sq, grad, potential, q_sketch);
}

int bf_embed_bvh_gradient(const BfEmbedBVH *bvh, const BfEmbedPack *pack,
                           const float *q, uint32_t dim, float sigma,
                           float *out_grad, float *out_potential) {
    if (!bvh || !pack || !q || !out_grad || dim != bvh->dim) return -1;
    memset(out_grad, 0, dim * sizeof(float));
    if (out_potential) *out_potential = 0.0f;

    /* compute query sketch */
    uint64_t q_sketch[4];
    uint32_t lim = dim < BVH_SKETCH_DIMS ? dim : BVH_SKETCH_DIMS;
    memset(q_sketch, 0, 32);
    for (uint32_t b = 0; b < lim; b++)
        if (q[b] > 0.0f) q_sketch[b >> 6] |= (1ULL << (b & 63u));

    float sigma_sq = sigma * sigma;
    if (bvh->n_nodes > 0)
        grad_traverse_(bvh, pack, 0, q, sigma_sq, out_grad, out_potential, q_sketch);
    return 0;
}

/* ── ternary-only collision check (fast path, Tier 1) ────────── */
int bf_embed_bvh_collide(const BfEmbedBVH *bvh, const float *q, uint32_t dim,
                          uint32_t *out_indices, int max_out, int *out_count) {
    if (!bvh || !q || !out_indices || !out_count || dim != bvh->dim) return -1;
    *out_count = 0;

    uint64_t q_sketch[4];
    uint32_t lim = dim < BVH_SKETCH_DIMS ? dim : BVH_SKETCH_DIMS;
    memset(q_sketch, 0, 32);
    for (uint32_t b = 0; b < lim; b++)
        if (q[b] > 0.0f) q_sketch[b >> 6] |= (1ULL << (b & 63u));

    /* Walk tree; when leaf matches ternary threshold, emit pack indices */
    /* Iterative via explicit stack (depth ≤ 64) */
    uint32_t stack[64]; int top = 0;
    if (bvh->n_nodes == 0) return 0;
    stack[top++] = 0;
    while (top > 0 && *out_count < max_out) {
        uint32_t ni = stack[--top];
        const BVHNodeDisk *nd = (const BVHNodeDisk *)bvh->nodes_base + ni;
        int agree = sketch_agreement_(q_sketch, nd->sketch);
        if (agree < BVH_SKETCH_THRESH) continue;

        if (nd->left == -1) {
            /* leaf — emit indices */
            for (uint32_t vi = 0; vi < nd->vec_count && *out_count < max_out; vi++)
                out_indices[(*out_count)++] = bvh->idx_base[nd->vec_start + vi];
        } else {
            if (nd->left  >= 0 && top < 63) stack[top++] = (uint32_t)nd->left;
            if (nd->right >= 0 && top < 63) stack[top++] = (uint32_t)nd->right;
        }
    }
    return 0;
}
