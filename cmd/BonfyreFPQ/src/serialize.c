/* FPQ! seed-mode storage, little endian.
 * Header: magic/u32, version/u32, tensor_count/u32, flags/u32.
 * Versions 1-2 store block seeds; v3 adds mode/u8 and optional base seed.
 * Interim v4 files used the v3 layout with flags=0. V4 flags=1 explicitly
 * selects the dictionary layout; never infer the layout from block bytes.
 * Tensor: name length/u32, name bytes, rows/cols/blocks/haar/bits/nodes (u64),
 * mode/u8, has_base/u8, optional legacy seed (dimension/size/distortion/tree).
 * Dictionary: count/u32, programs (dimension/u32, size/u32, tree).
 * Block: radius/f32, program_index/u32, distortion/f32, QJL.
 * QJL: elements/u32, projections/u32, projection_seed/u64, packed words/u64.
 * Tree: op/u8, params/2xf32, iparam/i32, left/u8 [+tree], right/u8 [+tree].
 * Distortion belongs to each block, independently of exact program identity.
 * Coordinate and auxiliary payloads belong to their existing FPQ0 owner.
 */
#define _POSIX_C_SOURCE 200809L
#include "fpq.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define FPQ_FILE_MAGIC 0x21515046u
#define FPQ_FILE_VERSION 4u
#define FPQ_FLAG_DICTIONARY 1u
/* Bound recursive cleanup and allocation even for hostile dictionaries whose
 * tiny references would otherwise expand into arbitrarily large trees. */
#define FILE_TREE_DEPTH 128u
#define FILE_TREE_NODES (1u << 20)
#define LOAD_BUDGET ((size_t)1 << 30)

typedef struct { FILE *f; size_t remaining, allocated; } reader;
typedef struct {
    const fpq_seed_t **seeds;
    uint32_t *indices;
    uint32_t count;
} dictionary;

static int put(FILE *f, uint64_t v, size_t n) {
    unsigned char bytes[8];
    for (size_t i = 0; i < n; i++) { bytes[i] = (unsigned char)v; v >>= 8; }
    return fwrite(bytes, 1, n, f) == n;
}
static int put_float(FILE *f, const float *v) {
    uint32_t bits; memcpy(&bits, v, 4); return put(f, bits, 4);
}
static int take(reader *r, void *out, size_t n) {
    if (n > r->remaining || fread(out, 1, n, r->f) != n) return 0;
    r->remaining -= n; return 1;
}
static int get(reader *r, uint64_t *v, size_t n) {
    unsigned char bytes[8];
    if (!take(r, bytes, n)) return 0;
    *v = 0;
    for (size_t i = 0; i < n; i++) *v |= (uint64_t)bytes[i] << (8 * i);
    return 1;
}
static int get_float(reader *r, float *v) {
    uint64_t word; if (!get(r, &word, 4)) return 0;
    uint32_t bits = (uint32_t)word; memcpy(v, &bits, 4); return 1;
}
static void *allocate(reader *r, size_t n, size_t size) {
    if (size && n > (LOAD_BUDGET - r->allocated) / size) return NULL;
    size_t bytes = n * size;
    void *p = calloc(bytes ? bytes : 1, 1);
    if (p) r->allocated += bytes;
    return p;
}

static int valid_node(const fpq_node_t *n, unsigned depth, size_t *count) {
    if (!n) return 1;
    if (depth > FILE_TREE_DEPTH || ++*count > FILE_TREE_NODES ||
        n->op < FPQ_OP_ROT || n->op > FPQ_OP_DCT) return 0;
    return valid_node(n->left, depth + 1, count) && valid_node(n->right, depth + 1, count);
}
static int valid_seed(const fpq_seed_t *s) {
    size_t count = 0;
    return s && s->root && s->target_dim == FPQ_BLOCK_DIM - 1 &&
           valid_node(s->root, 0, &count) && count == s->tree_size;
}
static int write_node(FILE *f, const fpq_node_t *n) {
    return put(f, n->op, 1) && put_float(f, &n->param[0]) &&
           put_float(f, &n->param[1]) && put(f, (uint32_t)n->iparam, 4) &&
           put(f, n->left != NULL, 1) && (!n->left || write_node(f, n->left)) &&
           put(f, n->right != NULL, 1) && (!n->right || write_node(f, n->right));
}
static int write_seed(FILE *f, const fpq_seed_t *s, int distortion) {
    return put(f, s->target_dim, 4) && put(f, s->tree_size, 4) &&
           (!distortion || put_float(f, &s->distortion)) && write_node(f, s->root);
}
static fpq_node_t *read_node(reader *r, unsigned depth, size_t *left) {
    uint64_t op, ip, child;
    if (depth > FILE_TREE_DEPTH || !*left || !get(r, &op, 1) || op > FPQ_OP_DCT) return NULL;
    --*left;
    fpq_node_t *n = allocate(r, 1, sizeof(*n));
    if (!n) return NULL;
    n->op = (fpq_op_t)op;
    if (!get_float(r, &n->param[0]) || !get_float(r, &n->param[1]) || !get(r, &ip, 4)) goto fail;
    uint32_t bits = (uint32_t)ip; int32_t signed_ip; memcpy(&signed_ip, &bits, 4);
    n->iparam = signed_ip;
    if (!get(r, &child, 1) || child > 1) goto fail;
    if (child && !(n->left = read_node(r, depth + 1, left))) goto fail;
    if (!get(r, &child, 1) || child > 1) goto fail;
    if (child && !(n->right = read_node(r, depth + 1, left))) goto fail;
    return n;
fail:
    fpq_node_free(n); return NULL;
}
static fpq_seed_t *read_seed(reader *r, int distortion) {
    uint64_t dim, count;
    if (!get(r, &dim, 4) || dim != FPQ_BLOCK_DIM - 1 || !get(r, &count, 4) ||
        !count || count > FILE_TREE_NODES || count > r->remaining / 15) return NULL;
    fpq_seed_t *s = allocate(r, 1, sizeof(*s));
    if (!s) return NULL;
    s->target_dim = (size_t)dim; s->tree_size = (size_t)count;
    size_t left = (size_t)count;
    if ((distortion && !get_float(r, &s->distortion)) ||
        !(s->root = read_node(r, 0, &left)) || left) {
        fpq_seed_free(s); return NULL;
    }
    return s;
}
static fpq_node_t *clone_node(reader *r, const fpq_node_t *src) {
    if (!src) return NULL;
    fpq_node_t *n = allocate(r, 1, sizeof(*n));
    if (!n) return NULL;
    *n = *src; n->left = n->right = NULL;
    if ((src->left && !(n->left = clone_node(r, src->left))) ||
        (src->right && !(n->right = clone_node(r, src->right)))) {
        fpq_node_free(n); return NULL;
    }
    return n;
}
static fpq_seed_t *clone_seed(reader *r, const fpq_seed_t *src) {
    fpq_seed_t *s = allocate(r, 1, sizeof(*s));
    if (!s) return NULL;
    *s = *src;
    if (!(s->root = clone_node(r, src->root))) { free(s); return NULL; }
    return s;
}

static void free_dictionary(dictionary *d) { free(d->seeds); free(d->indices); }
static int build_dictionary(const fpq_tensor_t *t, dictionary *d) {
    if (t->n_blocks > UINT32_MAX || t->n_blocks > SIZE_MAX / (2 * sizeof(uint64_t))) return 0;
    size_t cap = 1;
    while (cap < t->n_blocks * 2) {
        if (cap > SIZE_MAX / 2) return 0;
        cap *= 2;
    }
    d->seeds = calloc(t->n_blocks ? t->n_blocks : 1, sizeof(*d->seeds));
    d->indices = calloc(t->n_blocks ? t->n_blocks : 1, sizeof(*d->indices));
    uint32_t *slots = calloc(cap, sizeof(*slots));
    uint64_t *hashes = calloc(cap, sizeof(*hashes));
    if (!d->seeds || !d->indices || !slots || !hashes) { free(slots); free(hashes); return 0; }
    int ok = 0;
    for (size_t b = 0; b < t->n_blocks; b++) {
        const fpq_seed_t *s = t->seeds[b];
        if (!valid_seed(s)) goto done;
        uint64_t h = fpq_seed_hash(s);
        size_t p = (size_t)h & (cap - 1);
        while (slots[p] && !(hashes[p] == h && fpq_seed_equal(d->seeds[slots[p] - 1], s)))
            p = (p + 1) & (cap - 1);
        if (!slots[p]) {
            d->seeds[d->count++] = s; slots[p] = d->count; hashes[p] = h;
        }
        d->indices[b] = slots[p] - 1;
    }
    ok = 1;
done:
    free(slots); free(hashes); return ok;
}
static int write_qjl(FILE *f, const fpq_qjl_t *q) {
    if (!q || q->n_elements != FPQ_BLOCK_DIM - 1 || q->n_projections > UINT32_MAX ||
        q->n_projections > SIZE_MAX - 63) return 0;
    size_t words = (q->n_projections + 63) / 64;
    if ((words && !q->bits) || !put(f, q->n_elements, 4) ||
        !put(f, q->n_projections, 4) || !put(f, q->proj_seed, 8)) return 0;
    for (size_t w = 0; w < words; w++) if (!put(f, q->bits[w], 8)) return 0;
    return 1;
}
static fpq_qjl_t *read_qjl(reader *r) {
    uint64_t dim, proj, seed;
    if (!get(r, &dim, 4) || dim != FPQ_BLOCK_DIM - 1 || !get(r, &proj, 4) ||
        !get(r, &seed, 8)) return NULL;
    size_t words = (size_t)((proj + 63) / 64);
    if (words > r->remaining / 8) return NULL;
    fpq_qjl_t *q = allocate(r, 1, sizeof(*q));
    if (!q) return NULL;
    q->n_elements = (size_t)dim; q->n_projections = (size_t)proj; q->proj_seed = seed;
    q->bits = allocate(r, words, sizeof(*q->bits));
    if (!q->bits) { free(q); return NULL; }
    for (size_t w = 0; w < words; w++) if (!get(r, &q->bits[w], 8)) { fpq_qjl_free(q); return NULL; }
    return q;
}
static int valid_shape(const fpq_tensor_t *t) {
    if (t->original_cols && t->original_rows > SIZE_MAX / t->original_cols) return 0;
    size_t total = t->original_rows * t->original_cols;
    return t->n_blocks == total / FPQ_BLOCK_DIM + (total % FPQ_BLOCK_DIM != 0);
}
static int write_tensor(FILE *f, const fpq_tensor_t *t) {
    dictionary d = {0}; int ok = 0;
    if (!t || !valid_shape(t) || t->mode > FPQ_MODE_DIRECT ||
        t->coord_scales || t->coord_quants || t->coord_residual_norms ||
        t->chaos_r_idx || t->ghost || t->sbb_scale_delta || t->pid_alpha != 0 ||
        (t->n_blocks && (!t->seeds || !t->qjl || !t->radii)) ||
        (t->base_seed && !valid_seed(t->base_seed))) return 0;
    size_t name_len = strnlen(t->name, sizeof(t->name));
    if (name_len == sizeof(t->name) || !build_dictionary(t, &d)) goto done;
    if (!put(f, name_len, 4) || fwrite(t->name, 1, name_len, f) != name_len ||
        !put(f, t->original_rows, 8) || !put(f, t->original_cols, 8) ||
        !put(f, t->n_blocks, 8) || !put(f, t->haar_seed, 8) ||
        !put(f, t->total_bits, 8) || !put(f, t->total_seed_nodes, 8) ||
        !put(f, t->mode, 1) || !put(f, t->base_seed != NULL, 1) ||
        (t->base_seed && !write_seed(f, t->base_seed, 1)) || !put(f, d.count, 4)) goto done;
    for (uint32_t i = 0; i < d.count; i++) if (!write_seed(f, d.seeds[i], 0)) goto done;
    for (size_t b = 0; b < t->n_blocks; b++) {
        if (!put_float(f, &t->radii[b]) || !put(f, d.indices[b], 4) ||
            !put_float(f, &t->seeds[b]->distortion) || !write_qjl(f, t->qjl[b])) goto done;
    }
    ok = 1;
done:
    free_dictionary(&d); return ok;
}

int fpq_save(const char *path, fpq_tensor_t **tensors, size_t n_tensors) {
    if (!path || (n_tensors && !tensors) || n_tensors > UINT32_MAX) return -1;
    size_t len = strlen(path);
    if (len > SIZE_MAX - 12) return -1;
    char *temp = malloc(len + 12);
    if (!temp) return -1;
    snprintf(temp, len + 12, "%s.tmp.XXXXXX", path);
    int fd = mkstemp(temp);
    if (fd < 0) { free(temp); return -1; }
    FILE *f = fdopen(fd, "wb");
    if (!f) { close(fd); unlink(temp); free(temp); return -1; }
    int ok = put(f, FPQ_FILE_MAGIC, 4) && put(f, FPQ_FILE_VERSION, 4) &&
             put(f, n_tensors, 4) && put(f, FPQ_FLAG_DICTIONARY, 4);
    for (size_t t = 0; ok && t < n_tensors; t++) ok = write_tensor(f, tensors[t]);
    if (fflush(f) != 0 || ferror(f) || fsync(fd) != 0) ok = 0;
    if (fclose(f) != 0) ok = 0;
    if (ok && rename(temp, path) != 0) ok = 0;
    if (!ok) unlink(temp);
    free(temp);
    return ok ? 0 : -1;
}

static fpq_tensor_t *read_tensor(reader *r, uint64_t version, int dict_layout) {
    fpq_tensor_t *t = allocate(r, 1, sizeof(*t));
    fpq_seed_t **dict = NULL; uint64_t count = 0, n, v;
    if (!t) return NULL;
    if (!get(r, &n, 4) || n >= sizeof(t->name) || !take(r, t->name, (size_t)n) ||
        memchr(t->name, 0, (size_t)n)) goto fail;
    if (!get(r, &v, 8) || v > SIZE_MAX) goto fail;
    t->original_rows = (size_t)v;
    if (!get(r, &v, 8) || v > SIZE_MAX) goto fail;
    t->original_cols = (size_t)v;
    if (!get(r, &v, 8) || v > UINT32_MAX || v > SIZE_MAX) goto fail;
    /* Leave n_blocks zero until arrays exist, so cleanup cannot walk a count
     * from an untrusted header after an allocation failure. */
    size_t blocks = (size_t)v;
    if (blocks > r->remaining / (dict_layout ? 28 : 33)) goto fail;
    if (!get(r, &t->haar_seed, 8) || !get(r, &v, 8) || v > SIZE_MAX) goto fail;
    t->total_bits = (size_t)v;
    if (!get(r, &v, 8) || v > SIZE_MAX) goto fail;
    t->total_seed_nodes = (size_t)v;
    if (version >= 3) {
        if (!get(r, &v, 1) || v > FPQ_MODE_DIRECT) goto fail;
        t->mode = (uint8_t)v;
        if (!get(r, &v, 1) || v > 1) goto fail;
        if (v && !(t->base_seed = read_seed(r, 1))) goto fail;
    }
    t->seeds = allocate(r, blocks, sizeof(*t->seeds));
    t->qjl = allocate(r, blocks, sizeof(*t->qjl));
    t->radii = allocate(r, blocks, sizeof(*t->radii));
    if (!t->seeds || !t->qjl || !t->radii) goto fail;
    t->n_blocks = blocks;
    if (!valid_shape(t)) goto fail;
    if (dict_layout) {
        if (!get(r, &count, 4) || count > blocks || (blocks && !count) ||
            count > r->remaining / 23) goto fail;
        dict = allocate(r, (size_t)count, sizeof(*dict));
        if (!dict) goto fail;
        for (size_t i = 0; i < count; i++) if (!(dict[i] = read_seed(r, 0))) goto fail;
    }
    double distortion = 0;
    for (size_t b = 0; b < blocks; b++) {
        if (!get_float(r, &t->radii[b])) goto fail;
        if (dict_layout) {
            if (!get(r, &v, 4) || v >= count || !(t->seeds[b] = clone_seed(r, dict[v])) ||
                !get_float(r, &t->seeds[b]->distortion)) goto fail;
        } else if (!(t->seeds[b] = read_seed(r, 1))) goto fail;
        if (!(t->qjl[b] = read_qjl(r))) goto fail;
        distortion += t->seeds[b]->distortion;
    }
    t->avg_distortion = blocks ? (float)(distortion / blocks) : 0;
    for (size_t i = 0; dict && i < count; i++) fpq_seed_free(dict[i]);
    free(dict); return t;
fail:
    for (size_t i = 0; dict && i < count; i++) fpq_seed_free(dict[i]);
    free(dict); fpq_tensor_free(t); return NULL;
}

fpq_tensor_t **fpq_load(const char *path, size_t *n_tensors) {
    if (!n_tensors) return NULL;
    *n_tensors = 0;
    if (!path) return NULL;
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fpq_tensor_t **tensors = NULL; size_t loaded = 0;
    if (fseek(f, 0, SEEK_END) != 0) goto fail;
    long length = ftell(f);
    if (length < 0 || (uintmax_t)length > SIZE_MAX || fseek(f, 0, SEEK_SET) != 0) goto fail;
    reader r = {f, (size_t)length, 0};
    uint64_t magic, version, count, flags;
    if (!get(&r, &magic, 4) || magic != FPQ_FILE_MAGIC || !get(&r, &version, 4) ||
        version < 1 || version > FPQ_FILE_VERSION || !get(&r, &count, 4) ||
        !get(&r, &flags, 4) || (flags && (version != 4 || flags != FPQ_FLAG_DICTIONARY)) ||
        count > r.remaining / 52) goto fail;
    tensors = allocate(&r, (size_t)count, sizeof(*tensors));
    if (!tensors) goto fail;
    for (; loaded < count; loaded++) {
        tensors[loaded] = read_tensor(&r, version, flags == FPQ_FLAG_DICTIONARY);
        if (!tensors[loaded]) goto fail;
    }
    if (r.remaining || ferror(f)) goto fail;
    if (fclose(f) != 0) { f = NULL; goto fail; }
    *n_tensors = (size_t)count;
    return tensors;
fail:
    if (f) fclose(f);
    for (size_t i = 0; i < loaded; i++) fpq_tensor_free(tensors[i]);
    free(tensors); return NULL;
}
