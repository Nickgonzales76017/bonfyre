/* Inject allocation failures into the real serializer, including its clone
 * cleanup. The other codec objects retain their ordinary allocators. */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static size_t fail_at, calls;
static void *fault_calloc(size_t n, size_t size) {
    return ++calls == fail_at ? NULL : calloc(n, size);
}
static void *fault_malloc(size_t size) {
    return ++calls == fail_at ? NULL : malloc(size);
}
#define calloc fault_calloc
#define malloc fault_malloc
#include "../src/serialize.c"
#undef calloc
#undef malloc

static void drop(fpq_tensor_t **t, size_t n) {
    for (size_t i = 0; i < n; i++) fpq_tensor_free(t[i]);
    free(t);
}
static void load_faults(const char *path) {
    fail_at = calls = 0;
    size_t n = 0;
    fpq_tensor_t **t = fpq_load(path, &n); assert(t && n == 1);
    size_t points = calls; drop(t, n);
    for (size_t point = 1; point <= points; point++) {
        fail_at = point; calls = 0; n = 42;
        t = fpq_load(path, &n); assert(!t && n == 0);
    }
    printf("FPQ! load allocation refusal: %zu points PASS\n", points);
}
static size_t snapshot(const char *path, unsigned char *bytes, size_t cap) {
    FILE *f = fopen(path, "rb"); assert(f);
    size_t n = fread(bytes, 1, cap, f); assert(n < cap && feof(f));
    assert(fclose(f) == 0); return n;
}
int main(int argc, char **argv) {
    assert(argc == 2);
    char directory[] = "/tmp/bonfyre-fpq-allocation-XXXXXX";
    assert(mkdtemp(directory));
    char path[512]; snprintf(path, sizeof(path), "%s/dictionary.fpq", directory);
    load_faults(argv[1]); fail_at = calls = 0;
    size_t n = 0;
    fpq_tensor_t **t = fpq_load(argv[1], &n); assert(t && n == 1);
    /* Exercise failure after either nested child has been allocated. */
    t[0]->seeds[0]->root->op = FPQ_OP_PAIR;
    t[0]->seeds[0]->root->left = fpq_node_alloc(FPQ_OP_ROT);
    t[0]->seeds[0]->root->right = fpq_node_alloc(FPQ_OP_RAMP);
    assert(t[0]->seeds[0]->root->left && t[0]->seeds[0]->root->right);
    t[0]->seeds[0]->tree_size = 3;
    calls = 0; assert(fpq_save(path, t, n) == 0); size_t points = calls;
    unsigned char before[1024], after[1024];
    size_t len = snapshot(path, before, sizeof(before));
    for (size_t point = 1; point <= points; point++) {
        fail_at = point; calls = 0; assert(fpq_save(path, t, n) == -1);
        size_t size = snapshot(path, after, sizeof(after));
        assert(size == len && !memcmp(before, after, len));
    }
    printf("FPQ! save allocation refusal: %zu points PASS\n", points);
    drop(t, n); load_faults(path); unlink(path);
    assert(rmdir(directory) == 0);
    return 0;
}
