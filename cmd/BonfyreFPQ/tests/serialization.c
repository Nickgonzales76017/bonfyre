#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE
#include "fpq.h"
#include "bf_jsonw.h"
#include <assert.h>
#include <dirent.h>
#include <math.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static void release(fpq_tensor_t **t, size_t n) {
    for (size_t i = 0; i < n; i++) fpq_tensor_free(t[i]);
    free(t);
}
static unsigned char *bytes(const char *path, size_t *n) {
    FILE *f = fopen(path, "rb"); assert(f);
    assert(fseek(f, 0, SEEK_END) == 0);
    long size = ftell(f); assert(size >= 0);
    *n = (size_t)size; rewind(f);
    unsigned char *p = malloc(*n ? *n : 1); assert(p);
    assert(fread(p, 1, *n, f) == *n); assert(fclose(f) == 0);
    return p;
}
static void write_bytes(const char *path, const void *p, size_t n) {
    FILE *f = fopen(path, "wb"); assert(f);
    assert(fwrite(p, 1, n, f) == n); assert(fclose(f) == 0);
}
static uint32_t u32(const unsigned char *p) {
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}
static void set32(unsigned char *p, uint32_t v) {
    for (int i = 0; i < 4; i++) { p[i] = (unsigned char)v; v >>= 8; }
}
static void field(bf_jsonw *w, const char *key, size_t value) {
    bf_jsonw_key(w, key); bf_jsonw_int(w, (int64_t)value);
}
/* Test-only metadata projection of files accepted by the native reader. CUE
 * constrains these relations; it does not parse or validate binary FPQ bytes. */
static void contract_witness(bf_jsonw *w, const char *path) {
    size_t n, count = 0; unsigned char *p = bytes(path, &n);
    fpq_tensor_t **t = fpq_load(path, &count); assert(t && n >= 16);
    unsigned version = u32(p + 4), flags = u32(p + 12);
    bf_jsonw_obj(w);
    field(w, "magic", u32(p)); field(w, "version", version);
    field(w, "flags", flags); field(w, "tensor_count", u32(p + 8));
    bf_jsonw_key(w, "layout"); bf_jsonw_str(w, flags ? "dictionary" : "per-block");
    bf_jsonw_key(w, "tensors"); bf_jsonw_arr(w);
    size_t at = 16;
    for (size_t i = 0; i < count; i++) {
        fpq_tensor_t *v = t[i];
        assert(at + 4 <= n); at += 4 + u32(p + at) + 48;
        if (version >= 3) at += 2;
        if (v->base_seed) at += 12 + 15 * v->base_seed->tree_size;
        assert(at <= n);
        bf_jsonw_obj(w);
        field(w, "rows", v->original_rows); field(w, "cols", v->original_cols);
        field(w, "block_count", v->n_blocks); field(w, "mode", v->mode);
        bf_jsonw_key(w, "has_base"); bf_jsonw_bool(w, v->base_seed != NULL);
        size_t dictionaries = 0;
        if (flags) { assert(at + 4 <= n); dictionaries = u32(p + at); at += 4; }
        field(w, "dictionary_count", dictionaries);
        bf_jsonw_key(w, "dictionary"); bf_jsonw_arr(w);
        for (size_t d = 0; d < dictionaries; d++) {
            assert(at + 8 <= n);
            size_t nodes = u32(p + at + 4);
            bf_jsonw_obj(w); field(w, "target_dim", u32(p + at));
            field(w, "node_count", nodes); bf_jsonw_obj_end(w);
            at += 8 + 15 * nodes; assert(at <= n);
        }
        bf_jsonw_arr_end(w);
        bf_jsonw_key(w, "blocks"); bf_jsonw_arr(w);
        for (size_t b = 0; b < v->n_blocks; b++) {
            bf_jsonw_obj(w); at += 4; /* radius */
            if (flags) {
                assert(at + 8 <= n); field(w, "dictionary_index", u32(p + at));
                at += 8; /* dictionary reference and independent distortion */
            } else { at += 12 + 15 * v->seeds[b]->tree_size; }
            field(w, "target_dim", v->seeds[b]->target_dim);
            field(w, "node_count", v->seeds[b]->tree_size);
            assert(at + 16 <= n);
            field(w, "qjl_elements", u32(p + at));
            size_t projections = u32(p + at + 4);
            field(w, "qjl_projections", projections);
            field(w, "qjl_words", (projections + 63) / 64);
            at += 16 + 8 * ((projections + 63) / 64); assert(at <= n);
            bf_jsonw_obj_end(w);
        }
        bf_jsonw_arr_end(w); bf_jsonw_obj_end(w);
    }
    assert(at == n);
    bf_jsonw_arr_end(w); bf_jsonw_obj_end(w);
    free(p); release(t, count);
}
static fpq_seed_t *seed(float value) {
    fpq_seed_t *s = calloc(1, sizeof(*s)); assert(s);
    s->root = fpq_node_alloc(FPQ_OP_REFINE); assert(s->root);
    s->root->left = fpq_node_alloc(FPQ_OP_RAMP);
    s->root->right = fpq_node_alloc(FPQ_OP_ROT);
    assert(s->root->left && s->root->right);
    s->root->left->param[0] = value;
    s->root->left->param[1] = 0.25f;
    s->root->right->param[0] = 0.125f;
    s->root->iparam = -17;
    s->target_dim = FPQ_BLOCK_DIM - 1; s->tree_size = 3;
    return s;
}
static fpq_tensor_t *tensor(unsigned mode, int base, size_t blocks, int unique) {
    fpq_tensor_t *t = calloc(1, sizeof(*t)); assert(t);
    strcpy(t->name, "dictionary-test");
    t->original_rows = 1; t->original_cols = blocks * FPQ_BLOCK_DIM - 3;
    t->n_blocks = blocks; t->mode = (uint8_t)mode; t->haar_seed = 12345678;
    t->total_bits = 999; t->total_seed_nodes = 3 * blocks;
    t->radii = calloc(blocks, sizeof(*t->radii));
    t->seeds = calloc(blocks, sizeof(*t->seeds));
    t->qjl = calloc(blocks, sizeof(*t->qjl));
    assert(t->radii && t->seeds && t->qjl);
    if (base) { t->base_seed = seed(0.0625f); t->base_seed->distortion = 0.75f; }
    const size_t projections[] = {0, 1, 64, 65, 129};
    for (size_t b = 0; b < blocks; b++) {
        /* Equal programs carry different distortion; signed zero is distinct. */
        float value = unique ? (float)b / 64 : (b % 2 ? -0.0f : 0.0f);
        t->seeds[b] = seed(value);
        t->seeds[b]->distortion = (float)b / 32;
        t->radii[b] = (float)(b + 1) / 8;
        t->qjl[b] = calloc(1, sizeof(*t->qjl[b])); assert(t->qjl[b]);
        fpq_qjl_t *q = t->qjl[b];
        q->n_elements = FPQ_BLOCK_DIM - 1;
        q->n_projections = projections[b % 5]; q->proj_seed = UINT64_MAX - b;
        size_t words = (q->n_projections + 63) / 64;
        q->bits = calloc(words ? words : 1, sizeof(*q->bits)); assert(q->bits);
        for (size_t w = 0; w < words; w++) q->bits[w] = UINT64_C(0xc123456789abcdef) ^ (b + w);
    }
    return t;
}
static void same_tensor(const fpq_tensor_t *a, const fpq_tensor_t *b) {
    assert(strcmp(a->name, b->name) == 0);
    assert(a->mode == b->mode && a->haar_seed == b->haar_seed);
    assert(a->original_rows == b->original_rows && a->original_cols == b->original_cols);
    assert(a->n_blocks == b->n_blocks && a->total_bits == b->total_bits);
    assert(a->total_seed_nodes == b->total_seed_nodes);
    assert(fpq_seed_equal(a->base_seed, b->base_seed));
    if (a->base_seed) assert(memcmp(&a->base_seed->distortion, &b->base_seed->distortion, 4) == 0);
    for (size_t i = 0; i < a->n_blocks; i++) {
        assert(a->seeds[i] != b->seeds[i]);
        assert(fpq_seed_equal(a->seeds[i], b->seeds[i]));
        assert(a->seeds[i]->tree_size == b->seeds[i]->tree_size);
        assert(memcmp(&a->seeds[i]->distortion, &b->seeds[i]->distortion, 4) == 0);
        assert(memcmp(&a->radii[i], &b->radii[i], 4) == 0);
        fpq_qjl_t *x = a->qjl[i], *y = b->qjl[i];
        assert(x->n_elements == y->n_elements && x->n_projections == y->n_projections && x->proj_seed == y->proj_seed);
        assert(memcmp(x->bits, y->bits, ((x->n_projections + 63) / 64) * 8) == 0);
    }
    size_t n = a->original_rows * a->original_cols;
    float *x = calloc(n, sizeof(*x)), *y = calloc(n, sizeof(*y)); assert(x && y);
    fpq_decode_tensor(a, x); fpq_decode_tensor(b, y);
    assert(memcmp(x, y, n * sizeof(*x)) == 0);
    free(x); free(y);
}
static void refuses(const char *path, const unsigned char *p, size_t n) {
    write_bytes(path, p, n); size_t count = 42;
    assert(fpq_load(path, &count) == NULL && count == 0);
}
static void mutate(const char *path, const unsigned char *p, size_t n, size_t offset, uint32_t value) {
    unsigned char *copy = malloc(n); assert(copy && offset + 4 <= n);
    memcpy(copy, p, n); set32(copy + offset, value);
    refuses(path, copy, n); free(copy);
}
static void legacy(const char *dir, const char *out) {
    for (unsigned version = 1; version <= 4; version++) {
        char path[1024]; snprintf(path, sizeof(path), "%s/legacy-v%u.fpq", dir, version);
        size_t count = 0; fpq_tensor_t **t = fpq_load(path, &count);
        assert(t && count == 1);
        fpq_tensor_t *v = t[0];
        assert(strcmp(v->name, "legacy") == 0 && v->original_cols == 256);
        assert(v->mode == (version >= 3 ? FPQ_MODE_DIRECT : FPQ_MODE_FWHT));
        assert((v->base_seed != NULL) == (version >= 3));
        assert(v->haar_seed == 123 && v->total_bits == 456 && v->total_seed_nodes == 1);
        assert(v->seeds[0]->root->op == FPQ_OP_RAMP);
        assert(v->seeds[0]->root->param[0] == -0.25f && v->seeds[0]->root->param[1] == 0.5f);
        assert(v->seeds[0]->distortion == 0.75f && v->radii[0] == 1.25f);
        assert(v->qjl[0]->n_projections == 65 && v->qjl[0]->proj_seed == 42);
        assert(v->qjl[0]->bits[0] == UINT64_C(0x123456789abcdef0));
        assert(v->qjl[0]->bits[1] == UINT64_C(0xfedcba9876543210));
        assert(fpq_save(out, t, 1) == 0);
        fpq_tensor_t **restored = fpq_load(out, &count); assert(restored && count == 1);
        same_tensor(v, restored[0]); release(restored, count);
        size_t n; unsigned char *p = bytes(path, &n);
        for (size_t cut = 0; cut < n; cut++) refuses(out, p, cut);
        /* A legacy body relabeled as a dictionary must never be guessed back. */
        if (version == 4) mutate(out, p, n, 12, 1);
        free(p); release(t, 1);
    }
}

int main(int argc, char **argv) {
    assert(argc == 2 || argc == 3);
    char temp[] = "/tmp/bonfyre-fpq-serialization-XXXXXX"; assert(mkdtemp(temp));
    char path[1024], bad[1024], alt[1024];
    snprintf(path, sizeof(path), "%s/dictionary.fpq", temp);
    snprintf(bad, sizeof(bad), "%s/bad.fpq", temp);
    snprintf(alt, sizeof(alt), "%s/unique.fpq", temp);
    fpq_tensor_t *t[] = {tensor(FPQ_MODE_FWHT, 0, 12, 0), tensor(FPQ_MODE_DIRECT, 1, 12, 0)};
    assert(fpq_save(path, t, 2) == 0);
    if (argc == 3) {
        bf_jsonw w; bf_jsonw_init(&w, 2); bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "files"); bf_jsonw_arr(&w);
        contract_witness(&w, path);
        for (unsigned version = 1; version <= 4; version++) {
            char fixture[1024];
            snprintf(fixture, sizeof(fixture), "%s/legacy-v%u.fpq", argv[1], version);
            contract_witness(&w, fixture);
        }
        fpq_tensor_t empty = {.original_cols = FPQ_BLOCK_DIM};
        strcpy(empty.name, "empty"); fpq_tensor_t *empty_ptr = &empty;
        assert(fpq_save(alt, &empty_ptr, 1) == 0);
        contract_witness(&w, alt);
        bf_jsonw_arr_end(&w); bf_jsonw_obj_end(&w); assert(!w.oom);
        write_bytes(argv[2], w.buf, w.len); bf_jsonw_free(&w);
    }
    size_t count = 0, n;
    fpq_tensor_t **loaded = fpq_load(path, &count); assert(loaded && count == 2);
    for (size_t i = 0; i < count; i++) same_tensor(t[i], loaded[i]);
    /* Block seeds remain independently owned after load. */
    assert(loaded[0]->seeds[0]->root != loaded[0]->seeds[2]->root);
    release(loaded, count);
    unsigned char *p = bytes(path, &n);
    assert(u32(p) == 0x21515046 && u32(p + 4) == 4 && u32(p + 12) == 1);
    size_t offset = 16, dictionary_offset = 0, block_offset = 0;
    for (size_t i = 0; i < 2; i++) {
        offset += 4 + strlen(t[i]->name) + 48 + 2;
        if (t[i]->base_seed) offset += 12 + 3 * 15;
        if (!i) dictionary_offset = offset;
        assert(u32(p + offset) == 2); offset += 4 + 2 * (8 + 3 * 15);
        if (!i) block_offset = offset;
        for (size_t b = 0; b < t[i]->n_blocks; b++) {
            assert(u32(p + offset + 4) == b % 2);
            offset += 28 + 8 * ((t[i]->qjl[b]->n_projections + 63) / 64);
        }
    }
    assert(offset == n);
    fpq_tensor_t *unique = tensor(FPQ_MODE_FWHT, 0, 12, 1);
    assert(fpq_save(alt, &unique, 1) == 0);
    size_t unique_n, shared_n; unsigned char *q = bytes(alt, &unique_n); free(q);
    assert(fpq_save(alt, t, 1) == 0); q = bytes(alt, &shared_n); free(q);
    assert(unique_n - shared_n == (12 - 2) * (8 + 3 * 15));
    printf("FPQ! dictionary fixture: %zu bytes shared, %zu unique; 530 bytes factored\n", shared_n, unique_n);
    fpq_tensor_free(unique);
    for (size_t cut = 0; cut < n; cut++) refuses(bad, p, cut);
    mutate(bad, p, n, 4, 0); mutate(bad, p, n, 4, 5);
    mutate(bad, p, n, 8, UINT32_MAX); mutate(bad, p, n, 12, 2);
    mutate(bad, p, n, 12, 0); /* Tagged dictionary cannot decode as interim v4. */
    mutate(bad, p, n, 16, 128);
    mutate(bad, p, n, 20 + strlen(t[0]->name), UINT32_MAX);
    mutate(bad, p, n, 20 + strlen(t[0]->name) + 48, FPQ_MODE_COORD);
    mutate(bad, p, n, 20 + strlen(t[0]->name) + 49, 2); /* base-presence marker */
    mutate(bad, p, n, dictionary_offset, 0);
    mutate(bad, p, n, dictionary_offset, UINT32_MAX);
    mutate(bad, p, n, dictionary_offset + 4, 256); /* target dimension */
    mutate(bad, p, n, dictionary_offset + 8, 2); /* wrong tree size */
    mutate(bad, p, n, dictionary_offset + 8, 0);
    mutate(bad, p, n, dictionary_offset + 8, UINT32_MAX);
    mutate(bad, p, n, dictionary_offset + 12, 255); /* unknown opcode */
    mutate(bad, p, n, dictionary_offset + 25, 2); /* child marker */
    mutate(bad, p, n, block_offset + 4, 2); /* dictionary index */
    mutate(bad, p, n, block_offset + 12, 256); /* QJL dimension */
    mutate(bad, p, n, block_offset + 16, UINT32_MAX); /* QJL payload length */
    /* A correctly framed 130-node chain exceeds the parser's depth bound. */
    size_t deep_n = n - 45 + 130 * 15;
    unsigned char *deep = calloc(deep_n, 1); assert(deep);
    size_t start = dictionary_offset + 12, cursor = start;
    memcpy(deep, p, start); set32(deep + dictionary_offset + 8, 130);
    for (size_t i = 0; i < 130; i++) {
        deep[cursor] = FPQ_OP_SCALE;
        set32(deep + cursor + 1, 0x3f800000); /* scale 1 */
        deep[cursor + 13] = i + 1 < 130;
        cursor += 14;
    }
    cursor += 130; /* all right-child markers are zero */
    memcpy(deep + cursor, p + start + 45, n - start - 45);
    refuses(bad, deep, deep_n); free(deep);
    unsigned char *trailing = malloc(n + 1); assert(trailing);
    memcpy(trailing, p, n); trailing[n] = 0; refuses(bad, trailing, n + 1); free(trailing);
    /* Refused tensors and failed writes leave a previous file byte-identical. */
    t[0]->mode = FPQ_MODE_COORD; assert(fpq_save(path, t, 2) == -1); t[0]->mode = FPQ_MODE_FWHT;
    t[0]->seeds[0]->tree_size++; assert(fpq_save(path, t, 2) == -1); t[0]->seeds[0]->tree_size--;
    t[0]->seeds[0]->root->left->left = t[0]->seeds[0]->root;
    assert(fpq_save(path, t, 2) == -1); t[0]->seeds[0]->root->left->left = NULL;
    assert(fpq_save(temp, t, 2) == -1); /* rename onto a directory */
    pid_t pid = fork(); assert(pid >= 0);
    if (pid == 0) {
        signal(SIGXFSZ, SIG_IGN);
        struct rlimit limit = {64, 64}; assert(setrlimit(RLIMIT_FSIZE, &limit) == 0);
        _exit(fpq_save(path, t, 2) == -1 ? 0 : 1);
    }
    int status; assert(waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    size_t after_n; q = bytes(path, &after_n); assert(after_n == n && memcmp(p, q, n) == 0); free(q);
    legacy(argv[1], bad);
    /* Empty model is a valid empty list, distinguishable from load refusal. */
    assert(fpq_save(bad, NULL, 0) == 0); loaded = fpq_load(bad, &count);
    assert(loaded && count == 0); free(loaded);
    for (size_t i = 0; i < 2; i++) fpq_tensor_free(t[i]);
    free(p); unlink(path); unlink(bad); unlink(alt);
    assert(rmdir(temp) == 0); /* Includes proof that failed saves leave no temp files. */
    puts("FPQ! serialization: native parity, legacy v1-v4, truncation and write refusal PASS");
    return 0;
}
