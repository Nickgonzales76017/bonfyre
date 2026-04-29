/*
 * bonfyre-flashqla — BonfyreGDN Chunked Prefill (native C kernel).
 *
 * Portable, dependency-free implementation of GDN (Gated Delta Network)
 * linear attention.  This is the production Bonfyre kernel — no Python,
 * no TileLang, no PyTorch.
 *
 * Backend hierarchy:
 *   auto / tile  — gdn_fwd_tile: fused, head-major, SIMD-dispatched (DEFAULT)
 *   ref          — gdn_fwd_chunked: two-pass scalar (correctness oracle)
 *   avx2 / neon  — alias for tile (SIMD selected inside gdn_fwd_tile)
 *
 * Python FlashQLA (external comparison only):
 *   bonfyre-flashqla compare-external  --in t.bfgla --out ext.bfgdn
 *   Requires: SM90+, CUDA 12.8+, PyTorch 2.8+, flash_qla package.
 *   Not used in production.
 *
 * Usage:
 *   bonfyre-flashqla run    --in t.bfgla --out o.bfgdn [--backend auto]
 *                           [--chunk-size 64]
 *   bonfyre-flashqla gen    --B 1 --T 128 --H 8 --K 64 --V 64 --out t.bfgla
 *   bonfyre-flashqla verify --ref a.bfgdn --test b.bfgdn
 *   bonfyre-flashqla bench  [--backend auto|ref|tile] [--compare]
 *   bonfyre-flashqla compare-external  --in t.bfgla --out ext.bfgdn
 *   bonfyre-flashqla doctor
 *
 * Environment:
 *   BONFYRE_FLASHQLA_BACKEND=auto|ref|tile|avx2|neon  (overrides --backend)
 */
#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>

#include <bonfyre.h>
#include "gdn_ref.h"
#include "gdn_backend.h"

#define VERSION  "1.0.0"
#define MAX_PATH 4096

/* ═══════════════════════════════════════════════════════════════════
 * File I/O — GDN input / output formats
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    GdnShape shape;
    float    scale;
    int      has_h0;
    float   *q, *k, *v, *g, *beta, *h0;
} GdnInput;

static void gdn_input_free(GdnInput *in) {
    free(in->q);   free(in->k);   free(in->v);
    free(in->g);   free(in->beta); free(in->h0);
    memset(in, 0, sizeof(*in));
}

static int read_gdn_input(const char *path, GdnInput *in) {
    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); return 1; }

    uint32_t hdr[7];
    float    scale_val;
    uint32_t has_h0;

    if (fread(hdr, sizeof(uint32_t), 7, f) != 7) goto err_close;
    if (hdr[0] != GDN_INPUT_MAGIC) {
        fprintf(stderr, "flashqla: bad magic in '%s' (got 0x%08X, expected 0x%08X)\n",
                path, hdr[0], GDN_INPUT_MAGIC);
        goto err_close;
    }
    if (hdr[1] != GDN_FORMAT_VER) {
        fprintf(stderr, "flashqla: unsupported format version %u in '%s'\n", hdr[1], path);
        goto err_close;
    }
    if (fread(&scale_val, sizeof(float),    1, f) != 1) goto err_close;
    if (fread(&has_h0,    sizeof(uint32_t), 1, f) != 1) goto err_close;

    in->shape  = (GdnShape){(int)hdr[2], (int)hdr[3], (int)hdr[4], (int)hdr[5], (int)hdr[6]};
    in->scale  = scale_val;
    in->has_h0 = (int)(has_h0 != 0);

    {
        int     B = in->shape.B, T = in->shape.T, H = in->shape.H;
        int     K = in->shape.K, V = in->shape.V;
        size_t  n_qk = (size_t)B * T * H * K;
        size_t  n_v  = (size_t)B * T * H * V;
        size_t  n_g  = (size_t)B * T * H;
        size_t  n_h  = (size_t)B * H * K * V;

        in->q    = malloc(n_qk * sizeof(float));
        in->k    = malloc(n_qk * sizeof(float));
        in->v    = malloc(n_v  * sizeof(float));
        in->g    = malloc(n_g  * sizeof(float));
        in->beta = malloc(n_g  * sizeof(float));
        if (!in->q || !in->k || !in->v || !in->g || !in->beta) goto err_close;

        if (fread(in->q,    sizeof(float), n_qk, f) != n_qk) goto err_close;
        if (fread(in->k,    sizeof(float), n_qk, f) != n_qk) goto err_close;
        if (fread(in->v,    sizeof(float), n_v,  f) != n_v)  goto err_close;
        if (fread(in->g,    sizeof(float), n_g,  f) != n_g)  goto err_close;
        if (fread(in->beta, sizeof(float), n_g,  f) != n_g)  goto err_close;

        if (in->has_h0) {
            in->h0 = malloc(n_h * sizeof(float));
            if (!in->h0 || fread(in->h0, sizeof(float), n_h, f) != n_h) goto err_close;
        }
    }

    fclose(f);
    return 0;

err_close:
    fclose(f);
    gdn_input_free(in);
    return 1;
}

static int write_gdn_output(const char *path, const GdnShape *s,
                             const float *o, const float *h_final) {
    FILE *f = fopen(path, "wb");
    if (!f) { perror(path); return 1; }

    uint32_t hdr[7] = {
        GDN_OUTPUT_MAGIC, GDN_FORMAT_VER,
        (uint32_t)s->B, (uint32_t)s->T, (uint32_t)s->H,
        (uint32_t)s->K, (uint32_t)s->V
    };
    size_t n_o = (size_t)s->B * s->T * s->H * s->V;
    size_t n_h = (size_t)s->B * s->H * s->K * s->V;

    fwrite(hdr,     sizeof(uint32_t), 7,   f);
    fwrite(o,       sizeof(float),    n_o, f);
    fwrite(h_final, sizeof(float),    n_h, f);

    fclose(f);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Synthetic tensor generation (reproducible via xorshift64 PRNG)
 * ═══════════════════════════════════════════════════════════════════ */

static uint64_t xs64(uint64_t s) {
    s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s;
}

static float randf_xs(uint64_t *s) {
    *s = xs64(*s);
    return (float)(*s & 0xFFFFFFu) / (float)0x1000000u * 2.0f - 1.0f;
}

static int cmd_gen(int argc, char **argv) {
    const char *out = bf_arg_value(argc, argv, "--out");
    if (!out) { fprintf(stderr, "flashqla gen: --out required\n"); return 1; }

    int B = 1, T = 128, H = 8, K = 64, V = 64;
    const char *sv;
    if ((sv = bf_arg_value(argc, argv, "--B"))) B = atoi(sv);
    if ((sv = bf_arg_value(argc, argv, "--T"))) T = atoi(sv);
    if ((sv = bf_arg_value(argc, argv, "--H"))) H = atoi(sv);
    if ((sv = bf_arg_value(argc, argv, "--K"))) K = atoi(sv);
    if ((sv = bf_arg_value(argc, argv, "--V"))) V = atoi(sv);

    uint64_t seed = 42;
    if ((sv = bf_arg_value(argc, argv, "--seed")))
        seed = (uint64_t)strtoull(sv, NULL, 10);

    if (B <= 0 || T <= 0 || H <= 0 || K <= 0 || V <= 0) {
        fprintf(stderr, "flashqla gen: all dimensions must be positive\n"); return 1;
    }

    size_t n_qk = (size_t)B * T * H * K;
    size_t n_v  = (size_t)B * T * H * V;
    size_t n_g  = (size_t)B * T * H;
    float scale = 1.0f / sqrtf((float)K);

    float *q    = malloc(n_qk * sizeof(float));
    float *k    = malloc(n_qk * sizeof(float));
    float *v    = malloc(n_v  * sizeof(float));
    float *g    = malloc(n_g  * sizeof(float));
    float *beta = malloc(n_g  * sizeof(float));

    if (!q || !k || !v || !g || !beta) {
        free(q); free(k); free(v); free(g); free(beta);
        fprintf(stderr, "flashqla gen: out of memory\n"); return 1;
    }

    for (size_t i = 0; i < n_qk; i++) q[i]    = randf_xs(&seed);
    for (size_t i = 0; i < n_qk; i++) k[i]    = randf_xs(&seed);
    for (size_t i = 0; i < n_v;  i++) v[i]    = randf_xs(&seed);
    /* gate: clamp to (0.8, 0.99) — exponential decay regime */
    for (size_t i = 0; i < n_g;  i++) {
        g[i]    = 0.85f + 0.1f * (float)(xs64(seed + i) & 0xFF) / 255.0f;
        beta[i] = 0.05f + 0.1f * (float)(xs64(seed + i + n_g) & 0xFF) / 255.0f;
    }

    FILE *f = fopen(out, "wb");
    if (!f) { perror(out); free(q); free(k); free(v); free(g); free(beta); return 1; }

    uint32_t hdr[7] = {
        GDN_INPUT_MAGIC, GDN_FORMAT_VER,
        (uint32_t)B, (uint32_t)T, (uint32_t)H, (uint32_t)K, (uint32_t)V
    };
    uint32_t has_h0 = 0;

    fwrite(hdr,     sizeof(uint32_t), 7,   f);
    fwrite(&scale,  sizeof(float),    1,   f);
    fwrite(&has_h0, sizeof(uint32_t), 1,   f);
    fwrite(q,       sizeof(float),    n_qk, f);
    fwrite(k,       sizeof(float),    n_qk, f);
    fwrite(v,       sizeof(float),    n_v,  f);
    fwrite(g,       sizeof(float),    n_g,  f);
    fwrite(beta,    sizeof(float),    n_g,  f);
    fclose(f);

    printf("flashqla gen: B=%d T=%d H=%d K=%d V=%d scale=%.5f seed=%llu → %s\n",
           B, T, H, K, V, (double)scale, (unsigned long long)seed, out);

    free(q); free(k); free(v); free(g); free(beta);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Hardware detection helpers
 * ═══════════════════════════════════════════════════════════════════ */

/* Returns the SM major version of the first GPU, or 0 if not detectable. */
static int detect_sm_major(void) {
    FILE *f = popen(
        "nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null | head -1",
        "r");
    if (!f) return 0;
    char buf[32] = {0};
    int major = 0;
    if (fgets(buf, sizeof(buf), f)) major = atoi(buf);
    pclose(f);
    return major;
}

/*
 * Check if the flash_qla Python package is importable.
 * Uses fork/execvp — no shell command construction, no injection risk.
 */
static int check_flashqla_pkg(void) {
    pid_t pid = fork();
    if (pid < 0) return 0;
    if (pid == 0) {
        /* Silence stdout/stderr in the child */
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDOUT_FILENO);
            dup2(devnull, STDERR_FILENO);
            close(devnull);
        }
        char *const args[] = {
            "python3", "-c", "from flash_qla import chunk_gated_delta_rule", NULL
        };
        execvp("python3", args);
        _exit(127);
    }
    int status = 0;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

/* ─────────────────────────────────────────────────────────────────── */

/* forward declaration for use in doctor */
const char *gdn_tile_simd_name(void);

static int cmd_doctor(void) {
    printf("bonfyre-flashqla v" VERSION " — doctor\n\n");

    /* Native kernel */
    printf("  BonfyreGDN kernel:\n");
    printf("    tile backend:  ACTIVE  (fused update+readout, head-major tiling)\n");
    printf("    SIMD path:     %s\n", gdn_tile_simd_name());
    const char *benv = getenv("BONFYRE_FLASHQLA_BACKEND");
    if (benv && *benv)
        printf("    env override:  BONFYRE_FLASHQLA_BACKEND=%s\n", benv);
    else
        printf("    env override:  (none — using auto/tile)\n");

    /* CUDA / SM detection (compare-external only) */
    printf("\n  External FlashQLA (compare-external subcommand only):\n");
    int sm = detect_sm_major();
    if (sm == 0)
        printf("    CUDA:     not detected (nvidia-smi unavailable or no GPU)\n");
    else
        printf("    CUDA:     SM %d.x (%s)\n",
               sm, sm >= 9 ? "SM90+ — FlashQLA capable" : "below SM90");
    int fq = check_flashqla_pkg();
    printf("    flash_qla: %s\n",
           fq ? "AVAILABLE (flash_qla importable)"
              : "not installed  (pip install -v git+https://github.com/QwenLM/FlashQLA)");
    printf("    requirements: SM90+  CUDA 12.8+  PyTorch 2.8+\n");

    printf("\n  production dispatch path: BonfyreGDN tile kernel (zero deps)\n");
    printf("  to override:  BONFYRE_FLASHQLA_BACKEND=ref|tile|avx2|neon\n");
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Python / FlashQLA dispatch — compare-external only.
 * Not used in production.  fork/exec, no shell injection risk.
 * ═══════════════════════════════════════════════════════════════════ */

static int dispatch_to_python_flashqla(const char *in_path, const char *out_path) {
    char tmp_py[MAX_PATH];
    snprintf(tmp_py, sizeof(tmp_py), "/tmp/bonfyre_flashqla_%d.py", (int)getpid());

    FILE *py = fopen(tmp_py, "w");
    if (!py) { perror(tmp_py); return 1; }

    /* The script reads paths from argv[1]/argv[2] — no interpolation. */
    fprintf(py,
        "import sys, struct, torch\n"
        "from flash_qla import chunk_gated_delta_rule\n"
        "\n"
        "MAGIC_IN  = 0x414C4742\n"
        "MAGIC_OUT = 0x4F47444E\n"
        "\n"
        "in_path, out_path = sys.argv[1], sys.argv[2]\n"
        "\n"
        "with open(in_path, 'rb') as f:\n"
        "    magic, ver, B, T, H, K, V = struct.unpack('<7I', f.read(28))\n"
        "    assert magic == MAGIC_IN, f'bad magic {magic:#010x}'\n"
        "    scale, = struct.unpack('<f', f.read(4))\n"
        "    has_h0, = struct.unpack('<I', f.read(4))\n"
        "    def rd(n):\n"
        "        raw = f.read(n * 4)\n"
        "        return torch.frombuffer(bytearray(raw), dtype=torch.float32).clone()\n"
        "    q    = rd(B*T*H*K).reshape(B,T,H,K).to(torch.bfloat16).cuda()\n"
        "    k    = rd(B*T*H*K).reshape(B,T,H,K).to(torch.bfloat16).cuda()\n"
        "    v    = rd(B*T*H*V).reshape(B,T,H,V).to(torch.bfloat16).cuda()\n"
        "    g    = rd(B*T*H  ).reshape(B,T,H  ).to(torch.bfloat16).cuda()\n"
        "    beta = rd(B*T*H  ).reshape(B,T,H  ).to(torch.bfloat16).cuda()\n"
        "    h0   = None\n"
        "    if has_h0:\n"
        "        h0 = rd(B*H*K*V).reshape(B,H,K,V).to(torch.bfloat16).cuda()\n"
        "\n"
        "o, h_fin = chunk_gated_delta_rule(\n"
        "    q=q, k=k, v=v, g=g, beta=beta, scale=float(scale),\n"
        "    initial_state=h0, output_final_state=True)\n"
        "\n"
        "o_f32  = o.cpu().to(torch.float32).contiguous()\n"
        "hf_f32 = h_fin.cpu().to(torch.float32).contiguous()\n"
        "\n"
        "with open(out_path, 'wb') as f:\n"
        "    f.write(struct.pack('<7I', MAGIC_OUT, 1, B, T, H, K, V))\n"
        "    f.write(o_f32.numpy().tobytes())\n"
        "    f.write(hf_f32.numpy().tobytes())\n"
        "\n"
        "print(f'flashqla(py): B={B} T={T} H={H} K={K} V={V} -> {out_path}')\n"
    );
    fclose(py);

    pid_t pid = fork();
    if (pid < 0) { remove(tmp_py); return 1; }
    if (pid == 0) {
        /* Child: exec python3 directly — paths are argv, never shell-interpolated */
        char *const args[] = { "python3", tmp_py, (char *)in_path, (char *)out_path, NULL };
        execvp("python3", args);
        _exit(127);
    }
    int status = 0;
    waitpid(pid, &status, 0);
    remove(tmp_py);

    return (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 0 : 1;
}

/* ═══════════════════════════════════════════════════════════════════
 * run command
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_run(int argc, char **argv) {
    const char *in_path  = bf_arg_value(argc, argv, "--in");
    const char *out_path = bf_arg_value(argc, argv, "--out");
    const char *cs_str   = bf_arg_value(argc, argv, "--chunk-size");
    const char *be_str   = bf_arg_value(argc, argv, "--backend");
    int chunk_size = cs_str ? atoi(cs_str) : 64;

    if (!in_path)  { fprintf(stderr, "flashqla run: --in required\n");  return 1; }
    if (!out_path) { fprintf(stderr, "flashqla run: --out required\n"); return 1; }

    GdnBackend backend = gdn_backend_resolve(gdn_backend_from_str(be_str));
    GdnFwdFn   fn      = gdn_backend_fn(backend);

    GdnInput in;
    memset(&in, 0, sizeof(in));
    if (read_gdn_input(in_path, &in)) return 1;

    GdnShape s   = in.shape;
    size_t   n_o = (size_t)s.B * s.T * s.H * s.V;
    size_t   n_h = (size_t)s.B * s.H * s.K * s.V;

    float *o = malloc(n_o * sizeof(float));
    float *h = malloc(n_h * sizeof(float));
    if (!o || !h) {
        fprintf(stderr, "flashqla run: out of memory\n");
        free(o); free(h); gdn_input_free(&in); return 1;
    }
    if (in.has_h0) memcpy(h, in.h0, n_h * sizeof(float));
    else           memset(h, 0,       n_h * sizeof(float));

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    int rc = fn(in.q, in.k, in.v, in.g, in.beta, in.scale, h, o, s, chunk_size);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = ((t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) * 1e-9) * 1e3;

    if (rc) {
        fprintf(stderr, "flashqla run: kernel failed\n");
    } else {
        if (write_gdn_output(out_path, &s, o, h)) rc = 1;
        printf("flashqla run: B=%d T=%d H=%d K=%d V=%d  %.3f ms  backend=%s  chunk=%d  → %s\n",
               s.B, s.T, s.H, s.K, s.V, ms, gdn_backend_name(backend), chunk_size, out_path);
    }

    free(o); free(h); gdn_input_free(&in);
    return rc;
}

/* ═══════════════════════════════════════════════════════════════════
 * run_bench_backend — shared timing helper used by cmd_bench
 * ═══════════════════════════════════════════════════════════════════ */
static double run_bench_backend(
    GdnFwdFn fn,
    const float *q, const float *k, const float *v,
    const float *g, const float *beta, float scale,
    float *h, float *o,
    GdnShape s, int chunk_size, int iters, size_t n_h)
{
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int iter = 0; iter < iters; iter++) {
        memset(h, 0, n_h * sizeof(float));
        fn(q, k, v, g, beta, scale, h, o, s, chunk_size);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    return (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) * 1e-9;
}

/* ═══════════════════════════════════════════════════════════════════
 * bench command
 *   --backend ref|tile|auto   run a single backend (default: auto/tile)
 *   --compare                 run both ref and tile, report speedup ratio
 * ═══════════════════════════════════════════════════════════════════ */
static int cmd_bench(int argc, char **argv) {
    int B = 1, T = 256, H = 8, K = 64, V = 64, iters = 20, chunk_size = 64;
    const char *sv;
    if ((sv = bf_arg_value(argc, argv, "--B")))          B          = atoi(sv);
    if ((sv = bf_arg_value(argc, argv, "--T")))          T          = atoi(sv);
    if ((sv = bf_arg_value(argc, argv, "--H")))          H          = atoi(sv);
    if ((sv = bf_arg_value(argc, argv, "--K")))          K          = atoi(sv);
    if ((sv = bf_arg_value(argc, argv, "--V")))          V          = atoi(sv);
    if ((sv = bf_arg_value(argc, argv, "--iters")))      iters      = atoi(sv);
    if ((sv = bf_arg_value(argc, argv, "--chunk-size"))) chunk_size = atoi(sv);

    int compare_mode = bf_arg_has(argc, argv, "--compare");
    const char *be_str = bf_arg_value(argc, argv, "--backend");

    if (B<=0 || T<=0 || H<=0 || K<=0 || V<=0 || iters<=0) {
        fprintf(stderr, "flashqla bench: all dimensions and --iters must be positive\n");
        return 1;
    }

    GdnShape s     = {B, T, H, K, V};
    float    scale = 1.0f / sqrtf((float)K);
    size_t   n_qk  = (size_t)B * T * H * K;
    size_t   n_v   = (size_t)B * T * H * V;
    size_t   n_g   = (size_t)B * T * H;
    size_t   n_h   = (size_t)B * H * K * V;
    size_t   n_o   = (size_t)B * T * H * V;

    float *q    = calloc(n_qk, sizeof(float));
    float *k    = calloc(n_qk, sizeof(float));
    float *v    = calloc(n_v,  sizeof(float));
    float *g    = malloc(n_g  * sizeof(float));
    float *beta = malloc(n_g  * sizeof(float));
    float *hbuf = calloc(n_h,  sizeof(float));
    float *o    = malloc(n_o  * sizeof(float));

    if (!q || !k || !v || !g || !beta || !hbuf || !o) {
        fprintf(stderr, "flashqla bench: out of memory\n");
        free(q); free(k); free(v); free(g); free(beta); free(hbuf); free(o); return 1;
    }
    for (size_t i = 0; i < n_g; i++) { g[i] = 0.9f; beta[i] = 0.1f; }

    double flops_per_iter = 2.0 * B * T * H * (2.0 * K * V + K + V);
    int rc = 0;

    if (compare_mode) {
        /* ── compare mode: ref vs tile ──────────────────────────────── */
        printf("flashqla bench --compare: B=%d T=%d H=%d K=%d V=%d  iters=%d\n",
               B, T, H, K, V, iters);

        double ref_t  = run_bench_backend(gdn_fwd_chunked, q, k, v, g, beta, scale,
                                           hbuf, o, s, chunk_size, iters, n_h);
        double tile_t = run_bench_backend(gdn_fwd_tile,    q, k, v, g, beta, scale,
                                           hbuf, o, s, chunk_size, iters, n_h);

        double ref_per  = ref_t  / iters;
        double tile_per = tile_t / iters;
        double speedup  = ref_t / tile_t;

        printf("  ref  (gdn_fwd_chunked): %.3f ms/run  %.1f GFLOP/s\n",
               ref_per * 1e3, flops_per_iter * iters / ref_t / 1e9);
        printf("  tile (gdn_fwd_tile):    %.3f ms/run  %.1f GFLOP/s  simd=%s\n",
               tile_per * 1e3, flops_per_iter * iters / tile_t / 1e9,
               gdn_tile_simd_name());
        printf("  speedup: %.2fx (tile vs ref)\n", speedup);

        if (speedup >= 1.5)
            printf("  PASS: speedup %.2f >= 1.5x minimum\n", speedup);
        else if (speedup >= 1.1)
            printf("  WARN: speedup %.2f < 1.5x target (expected 1.5-2.5x on modern hardware)\n",
                   speedup);
        else {
            printf("  FAIL: speedup %.2f < 1.1x — tile is not faster than ref\n", speedup);
            rc = 1;
        }
    } else {
        /* ── single backend mode ────────────────────────────────────── */
        GdnBackend backend = gdn_backend_resolve(gdn_backend_from_str(be_str));
        GdnFwdFn   fn      = gdn_backend_fn(backend);

        printf("flashqla bench: B=%d T=%d H=%d K=%d V=%d  iters=%d  chunk=%d  backend=%s\n",
               B, T, H, K, V, iters, chunk_size, gdn_backend_name(backend));

        double total = run_bench_backend(fn, q, k, v, g, beta, scale,
                                          hbuf, o, s, chunk_size, iters, n_h);
        double per   = total / iters;
        double gflops = flops_per_iter * iters / total / 1e9;

        printf("  total:    %.3f s\n",   total);
        printf("  per run:  %.3f ms  (%.1f μs)\n", per * 1e3, per * 1e6);
        printf("  GFLOP/s:  %.2f\n",  gflops);
        printf("  tokens/s: %.0f\n",  (double)B * T * iters / total);
        if (backend != GDN_BACKEND_REF)
            printf("  simd:     %s\n", gdn_tile_simd_name());
    }

    free(q); free(k); free(v); free(g); free(beta); free(hbuf); free(o);
    return rc;
}

/* ═══════════════════════════════════════════════════════════════════
 * verify command — compare two .bfgdn output files.
 * Reports max_abs_err and mean_abs_err over the 'o' tensor.
 * Exits non-zero if max_abs_err >= 1e-4.
 * ═══════════════════════════════════════════════════════════════════ */
static int read_gdn_output_o(const char *path,
                               float **o_out, size_t *n_out,
                               GdnShape *shape_out)
{
    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); return 1; }
    uint32_t hdr[7];
    if (fread(hdr, sizeof(uint32_t), 7, f) != 7) { fclose(f); return 1; }
    if (hdr[0] != GDN_OUTPUT_MAGIC) {
        fprintf(stderr, "verify: bad magic in '%s' (0x%08X)\n", path, hdr[0]);
        fclose(f); return 1;
    }
    *shape_out = (GdnShape){(int)hdr[2], (int)hdr[3], (int)hdr[4],
                             (int)hdr[5], (int)hdr[6]};
    size_t n = (size_t)hdr[2] * hdr[3] * hdr[4] * hdr[6];  /* B*T*H*V */
    *n_out = n;
    *o_out = malloc(n * sizeof(float));
    if (!*o_out) { fclose(f); return 1; }
    if (fread(*o_out, sizeof(float), n, f) != n) {
        free(*o_out); fclose(f); return 1;
    }
    fclose(f);
    return 0;
}

static int cmd_verify(int argc, char **argv) {
    const char *ref_path  = bf_arg_value(argc, argv, "--ref");
    const char *test_path = bf_arg_value(argc, argv, "--test");
    if (!ref_path)  { fprintf(stderr, "flashqla verify: --ref required\n");  return 1; }
    if (!test_path) { fprintf(stderr, "flashqla verify: --test required\n"); return 1; }

    float   *o_ref = NULL, *o_test = NULL;
    size_t   n_ref = 0, n_test = 0;
    GdnShape sr, st;

    if (read_gdn_output_o(ref_path,  &o_ref,  &n_ref,  &sr)) return 1;
    if (read_gdn_output_o(test_path, &o_test, &n_test, &st)) {
        free(o_ref); return 1;
    }

    if (n_ref != n_test ||
        sr.B != st.B || sr.T != st.T || sr.H != st.H ||
        sr.K != st.K || sr.V != st.V) {
        fprintf(stderr, "verify: shape mismatch  ref=[%d,%d,%d,%d,%d] test=[%d,%d,%d,%d,%d]\n",
                sr.B,sr.T,sr.H,sr.K,sr.V, st.B,st.T,st.H,st.K,st.V);
        free(o_ref); free(o_test); return 1;
    }

    double max_err = 0.0, sum_err = 0.0;
    for (size_t i = 0; i < n_ref; i++) {
        double err = fabs((double)o_ref[i] - (double)o_test[i]);
        if (err > max_err) max_err = err;
        sum_err += err;
    }
    double mean_err = sum_err / (double)n_ref;

    printf("flashqla verify: B=%d T=%d H=%d K=%d V=%d  n=%zu\n",
           sr.B, sr.T, sr.H, sr.K, sr.V, n_ref);
    printf("  max_abs_err:  %.3e\n", max_err);
    printf("  mean_abs_err: %.3e\n", mean_err);

    int pass = (max_err < 1e-4);
    printf("  %s: max_abs_err %s 1e-4\n", pass ? "ok" : "FAIL",
           pass ? "<" : ">=");

    free(o_ref); free(o_test);
    return pass ? 0 : 1;
}

/* ═══════════════════════════════════════════════════════════════════
 * compare-external — run Python FlashQLA and compare with tile kernel.
 * For benchmarking against upstream.  Not used in production.
 * ═══════════════════════════════════════════════════════════════════ */
static int cmd_compare_external(int argc, char **argv) {
    const char *in_path  = bf_arg_value(argc, argv, "--in");
    const char *out_path = bf_arg_value(argc, argv, "--out");
    if (!in_path)  { fprintf(stderr, "flashqla compare-external: --in required\n");  return 1; }
    if (!out_path) { fprintf(stderr, "flashqla compare-external: --out required\n"); return 1; }

    int sm = detect_sm_major();
    if (sm < 9) {
        fprintf(stderr, "compare-external: SM%d < 90 — Python FlashQLA requires Hopper (H100/H200)\n",
                sm);
        return 1;
    }
    if (!check_flashqla_pkg()) {
        fprintf(stderr,
            "compare-external: flash_qla not importable.\n"
            "  pip install -v git+https://github.com/QwenLM/FlashQLA\n");
        return 1;
    }

    printf("flashqla compare-external: SM%d — running Python FlashQLA...\n", sm);
    return dispatch_to_python_flashqla(in_path, out_path);
}

/* ═══════════════════════════════════════════════════════════════════
 * usage / main
 * ═══════════════════════════════════════════════════════════════════ */

static void usage(void) {
    fprintf(stderr,
        "bonfyre-flashqla v" VERSION " — BonfyreGDN Chunked Prefill\n\n"
        "Usage:\n"
        "  bonfyre-flashqla run    --in t.bfgla --out o.bfgdn\n"
        "                          [--backend auto|ref|tile|avx2|neon] [--chunk-size 64]\n"
        "  bonfyre-flashqla gen    --B 1 --T 128 --H 8 --K 64 --V 64 --out t.bfgla\n"
        "                          [--seed N]\n"
        "  bonfyre-flashqla verify --ref a.bfgdn --test b.bfgdn\n"
        "  bonfyre-flashqla bench  [--B 1] [--T 256] [--H 8] [--K 64] [--V 64]\n"
        "                          [--iters 20] [--chunk-size 64]\n"
        "                          [--backend ref|tile|auto] [--compare]\n"
        "  bonfyre-flashqla compare-external --in t.bfgla --out ext.bfgdn\n"
        "  bonfyre-flashqla doctor\n\n"
        "Backends (--backend):\n"
        "  auto/tile  fused update+readout, head-major cache tiling, SIMD  [DEFAULT]\n"
        "  ref        two-pass scalar — correctness oracle, debug only\n"
        "  avx2/neon  alias for tile (SIMD selected internally)\n\n"
        "Environment:\n"
        "  BONFYRE_FLASHQLA_BACKEND=auto|ref|tile|avx2|neon  (overrides --backend)\n\n"
        "GDN recurrence (model-agnostic linear attention):\n"
        "  H_t = g_t * H_{t-1} + beta_t * (v_t ⊗ k_t)\n"
        "  o_t = H_t^T * q_t\n\n"
        "Input format (.bfgla):\n"
        "  [magic:BGLA][ver:1][B][T][H][K][V][scale][has_h0]\n"
        "  [q:B*T*H*K f32][k:B*T*H*K f32][v:B*T*H*V f32]\n"
        "  [g:B*T*H f32][beta:B*T*H f32][h0:B*H*K*V f32 if has_h0]\n\n"
        "Output format (.bfgdn):\n"
        "  [magic:NGDO][ver:1][B][T][H][K][V]\n"
        "  [o:B*T*H*V f32][h_final:B*H*K*V f32]\n"
    );
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(); return 1; }

    const char *cmd = argv[1];

    if (strcmp(cmd, "--help")   == 0 || strcmp(cmd, "-h") == 0) { usage(); return 0; }
    if (strcmp(cmd, "--version") == 0 || strcmp(cmd, "version") == 0) {
        printf("bonfyre-flashqla v" VERSION "\n"); return 0;
    }
    if (strcmp(cmd, "doctor")    == 0) return cmd_doctor();
    if (strcmp(cmd, "gen")       == 0) return cmd_gen(argc, argv);
    if (strcmp(cmd, "run")       == 0 ||
        strcmp(cmd, "fwd")       == 0) return cmd_run(argc, argv);
    if (strcmp(cmd, "verify")    == 0) return cmd_verify(argc, argv);
    if (strcmp(cmd, "bench")     == 0 ||
        strcmp(cmd, "benchmark") == 0) return cmd_bench(argc, argv);
    if (strcmp(cmd, "compare-external") == 0) return cmd_compare_external(argc, argv);

    fprintf(stderr, "bonfyre-flashqla: unknown command '%s'\n", cmd);
    usage();
    return 1;
}
