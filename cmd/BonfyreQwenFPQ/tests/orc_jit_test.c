/* The specialised kernel three ways, on the same data: the kernel's own
 * source compiled ordinarily with the shape passed at run time, the
 * temporary-C path it replaces (C written out, a compiler process, a shared
 * object loaded back), and the in-process ORC specialisation. All three must
 * return the same bits; what differs is what it costs to get the code. */
#define _DARWIN_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#include "fpq_orc_jit.h"
#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define BDIM 256
static double now_ms(void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6; }

/* The same arithmetic with the shape as arguments: what an unspecialised build runs. */
static void reference(const float *z, const float *x, float *out, unsigned long long seed, unsigned long cols, unsigned long rows, unsigned long bpr) {
    for (unsigned long i = 0; i < rows; i++) {
        float row_score = 0.0f;
        for (unsigned long bj = 0; bj < bpr; bj++) {
            unsigned long b = i * bpr + bj, col_off = bj * BDIM;
            unsigned long this_dim = (col_off + BDIM <= cols) ? BDIM : (cols - col_off);
            float xs[BDIM];
            memset(xs, 0, sizeof(xs));
            memcpy(xs, x + col_off, this_dim * sizeof(float));
            unsigned long long state = (seed ^ b) ? (seed ^ b) : 0x5DEECE66DULL;
            for (unsigned long si = 0; si < BDIM; si += 64) {
                state ^= state << 13; state ^= state >> 7; state ^= state << 17;
                for (unsigned long j = si; j < si + 64 && j < BDIM; j++) if ((state >> (j - si)) & 1ULL) xs[j] = -xs[j];
            }
            const float *z_b = z + b * BDIM;
            float dot = 0.0f;
            for (unsigned long k = 0; k < BDIM; k++) dot += z_b[k] * xs[k];
            row_score += dot;
        }
        out[i] += row_score;
    }
}

/* The path being replaced, reproduced: source text with the shape baked in, a compiler process, dlopen. */
static fpq_sli_kernel_fn temporary_c(const char *kernel_source, const char *dir, unsigned long rows, unsigned long bpr, double *ms) {
    char src[512], so[512], cmd[1400], line[512];
    double started = now_ms();
    FILE *in = fopen(kernel_source, "r"), *out;
    snprintf(src, sizeof(src), "%s/k_%lu_%lu.c", dir, rows, bpr);
    snprintf(so, sizeof(so), "%s/k_%lu_%lu.so", dir, rows, bpr);
    if (!in || !(out = fopen(src, "w"))) return NULL;
    while (fgets(line, sizeof(line), in)) {
        if (!strncmp(line, "#define N_ROWS", 14)) fprintf(out, "#define N_ROWS %luUL\n", rows);
        else if (!strncmp(line, "#define BPR", 11)) fprintf(out, "#define BPR %luUL\n", bpr);
        else fputs(line, out);
    }
    fclose(in); fclose(out);
    snprintf(cmd, sizeof(cmd), "cc -O3 -march=native -shared -fPIC -o '%s' '%s' 2>/dev/null", so, src);
    if (system(cmd) != 0) return NULL;
    void *handle = dlopen(so, RTLD_NOW | RTLD_LOCAL);
    if (!handle) return NULL;
    *ms = now_ms() - started;
    return (fpq_sli_kernel_fn)dlsym(handle, "sli_jit_kernel");
}

int main(int argc, char **argv) {
    const char *kernel_source = argc > 1 ? argv[1] : "src/sli_jit_kernel.c";
    char dir[] = "/tmp/bonfyre-orc-test-XXXXXX";
    static const unsigned long shapes[][2] = { { 896, 4 }, { 896, 19 }, { 4864, 4 }, { 151, 1 } };
    int failed = 0;
    if (!mkdtemp(dir)) return 2;
    for (size_t s = 0; s < sizeof(shapes) / sizeof(shapes[0]); s++) {
        unsigned long rows = shapes[s][0], bpr = shapes[s][1], cols = bpr * BDIM - (s == 1 ? 96 : 0);
        size_t nz = rows * bpr * BDIM;
        float *z = malloc(nz * sizeof(float)), *x = malloc(bpr * BDIM * sizeof(float));
        float *a = calloc(rows, sizeof(float)), *b = calloc(rows, sizeof(float)), *c = calloc(rows, sizeof(float));
        unsigned long long seed = 0x9E3779B97F4A7C15ULL + s, r = 88172645463325252ULL + s;
        double orc_ms = 0, cc_ms = 0, again_ms = -1, t;
        for (size_t i = 0; i < nz; i++) { r ^= r << 13; r ^= r >> 7; r ^= r << 17; z[i] = (float)((int)(r % 2001) - 1000) / 997.0f; }
        for (size_t i = 0; i < bpr * BDIM; i++) { r ^= r << 13; r ^= r >> 7; r ^= r << 17; x[i] = (float)((int)(r % 2001) - 1000) / 991.0f; }

        fpq_sli_kernel_fn orc = fpq_orc_sli_kernel(rows, bpr, &orc_ms);
        fpq_sli_kernel_fn again = fpq_orc_sli_kernel(rows, bpr, &again_ms);
        fpq_sli_kernel_fn old = temporary_c(kernel_source, dir, rows, bpr, &cc_ms);
        if (!orc) { printf("FAIL rows=%lu bpr=%lu: no ORC kernel\n", rows, bpr); failed = 1; continue; }
        if (again != orc || again_ms != 0.0) { printf("FAIL rows=%lu bpr=%lu: the same shape was compiled twice\n", rows, bpr); failed = 1; }

        const int reps = 20;
        t = now_ms(); for (int k = 0; k < reps; k++) reference(z, x, a, seed, cols, rows, bpr); double ref_run = (now_ms() - t) / reps;
        t = now_ms(); for (int k = 0; k < reps; k++) orc(z, x, b, seed, cols); double orc_run = (now_ms() - t) / reps;
        double old_run = -1;
        if (old) { t = now_ms(); for (int k = 0; k < reps; k++) old(z, x, c, seed, cols); old_run = (now_ms() - t) / reps; }

        /* Bit for bit: the kernel is strict floating point, so specialising it changes no result. */
        if (memcmp(a, b, rows * sizeof(float)) != 0) { printf("FAIL rows=%lu bpr=%lu: ORC differs from the reference\n", rows, bpr); failed = 1; }
        if (old && memcmp(a, c, rows * sizeof(float)) != 0) { printf("FAIL rows=%lu bpr=%lu: temporary-C differs from the reference\n", rows, bpr); failed = 1; }
        printf("rows=%lu bpr=%lu cols=%lu | get code: orc %.1f ms, temporary-C %s%.1f ms | run: reference %.3f ms, orc %.3f ms, temporary-C %.3f ms | identical %s\n",
               rows, bpr, cols, orc_ms, old ? "" : "(unavailable) ", cc_ms, ref_run, orc_run, old_run, memcmp(a, b, rows * sizeof(float)) ? "NO" : "yes");
        free(z); free(x); free(a); free(b); free(c);
    }
    size_t compiled, reused; fpq_orc_stats(&compiled, &reused);
    if (compiled != 4 || reused != 4) { printf("FAIL: compiled %zu reused %zu\n", compiled, reused); failed = 1; }
    char cleanup[600]; snprintf(cleanup, sizeof(cleanup), "rm -rf '%s'", dir); if (system(cleanup)) {}
    printf(failed ? "FAIL orc jit\n" : "PASS orc jit: 4 shapes, every result bit-identical three ways, each shape compiled once\n");
    return failed;
}
