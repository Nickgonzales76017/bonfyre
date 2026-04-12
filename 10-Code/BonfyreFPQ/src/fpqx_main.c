/*
 * fpqx_main.c — bonfyre-fpqx CLI
 *
 * FPQ-X: Generalized Compression Algebra
 * Rate–distortion–execution compiler for neural network weights.
 *
 * Commands:
 *   bonfyre-fpqx compress  <input> <output> [opts]     — Full FPQ-X pipeline
 *   bonfyre-fpqx roundtrip <input> [opts]              — Encode+decode+measure
 *   bonfyre-fpqx profile   <input> [opts]              — Analyze compressibility
 *   bonfyre-fpqx distill   <cache.safetensors> <output> [opts]  — KV distillation
 *   bonfyre-fpqx pack      <input> <output> [opts]     — Hardware-align pack
 *
 * Inherits FPQ v10 (B+R+P) as the additive core, adds:
 *   M — multiplicative low-rank scaling manifold
 *   Π — predictive context-conditioned correction
 *   D — sequence-axis distillation
 *   Λ — adaptive per-tensor policy
 *   H — hardware-aligned packing
 */

#include "fpqx.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <sys/stat.h>
#include <sys/time.h>

/* ═══════════════════════════════════════════════════════════════════ */

/* Auto-detect model format and read tensors (shared with bonfyre-fpq) */
static fpq_raw_tensor_t *fpqx_read_model(const char *path, size_t *n_tensors) {
    struct stat st;
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode))
        return fpq_safetensors_read(path, n_tensors);
    size_t len = strlen(path);
    if (len > 12 && strcmp(path + len - 12, ".safetensors") == 0)
        return fpq_safetensors_read(path, n_tensors);
    return fpq_ggml_read(path, n_tensors);
}

static double wall_clock(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1e6;
}

static void fpqx_usage(void) {
    fprintf(stderr,
        "bonfyre-fpqx " FPQX_VERSION " — FPQ-X: Generalized Compression Algebra\n"
        "Rate–distortion–execution compiler for neural network weights.\n\n"
        "  FPQ-X = A + M + Π + D + Λ + H\n"
        "    A  Additive        (LR SVD + E8 + RVQ + ghost)  [inherited from FPQ v10]\n"
        "    M  Multiplicative  (low-rank scaling manifold)   [NEW]\n"
        "    Π  Predictive      (context-conditioned restore) [NEW]\n"
        "    D  Distilled       (sequence-axis compression)   [NEW]\n"
        "    Λ  Adaptive        (per-tensor policy selection) [NEW]\n"
        "    H  Hardware        (kernel-aligned packing)      [NEW]\n\n"
        "Commands:\n"
        "  compress  <input> <output> [opts]    Full A+M+Π pipeline, write output\n"
        "  roundtrip <input> [opts]             Encode+decode measure (no write)\n"
        "  profile   <input> [opts]             Analyze per-tensor compressibility\n"
        "  distill   <cache> <output> [opts]    KV cache distillation\n"
        "  pack      <input> <output> [opts]    Hardware-aligned repacking\n\n"
        "Options:\n"
        "  --bits <N>          Base bits per weight (2/3/4, default: 3)\n"
        "  --scale-rank <N>    Multiplicative manifold rank (default: auto)\n"
        "  --no-scale          Disable multiplicative correction\n"
        "  --no-predict        Disable predictive correction\n"
        "  --limit <N>         Only process first N tensors\n"
        "  --tensor <name>     Only process named tensor\n"
        "  --format fpq|safetensors  Output format (default: safetensors)\n"
        "  --group-size <N>    Pack group size (default: 128)\n"
        "  --atoms <N>         KV distillation target atoms (default: 256)\n\n"
        "References:\n"
        "  LoRDS  arXiv:2601.22716    WaterSIC  arXiv:2603.04956\n"
        "  EchoKV arXiv:2603.22910    KVSculpt  arXiv:2603.27819\n"
        "  KV-CoRE arXiv:2602.05929   InnerQ    arXiv:2602.23200\n"
        "  MoBiQuant arXiv:2602.20191 QMM       arXiv:2601.17187\n"
    );
}


/* ═══════════════════════════════════════════════════════════════════
 * Command: profile — per-tensor compressibility diagnostics
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_profile(const char *input_path, const char *tensor_filter,
                        size_t limit, int base_bits) {
    size_t n_raw;
    fpq_raw_tensor_t *raw = fpqx_read_model(input_path, &n_raw);
    if (!raw || n_raw == 0) {
        fprintf(stderr, "Failed to read: %s\n", input_path);
        return 1;
    }

    fprintf(stderr,
        "═══════════════════════════════════════════════════════════════════\n"
        " FPQ-X Compressibility Profile: %s\n"
        " Tensors: %zu   Base bits: %d\n"
        "═══════════════════════════════════════════════════════════════════\n\n",
        input_path, n_raw, base_bits);

    fprintf(stderr,
        "%-45s %7s %6s %7s %7s %5s %5s %5s  Ops\n",
        "Tensor", "Shape", "η_L", "Gap", "Kurt", "Bits", "ScRk", "Pred");
    fprintf(stderr,
        "─────────────────────────────────────────────────────────────────"
        "─────────────────────────────────\n");

    int n_scale = 0, n_pred = 0, n_processed = 0;
    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && !strstr(raw[i].name, tensor_filter)) continue;
        if (raw[i].rows <= 1 || raw[i].cols <= 1) continue;

        fpqx_policy_t pol = fpqx_profile(raw[i].data, raw[i].rows, raw[i].cols,
                                           raw[i].name, base_bits);

        char shape[20];
        snprintf(shape, sizeof(shape), "%zux%zu", raw[i].rows, raw[i].cols);

        char ops[16] = "A";
        if (pol.use_scale) { strcat(ops, "+M"); n_scale++; }
        if (pol.use_predictor) { strcat(ops, "+Π"); n_pred++; }

        fprintf(stderr,
            "%-45s %7s %6.3f %7.1f %7.1f %5d %5d %5d  %s\n",
            raw[i].name, shape, pol.eta_L, pol.spectral_gap,
            pol.kurtosis, pol.recommended_bits,
            pol.use_scale ? pol.scale_rank : 0,
            pol.use_predictor ? pol.pred_rank : 0,
            ops);

        n_processed++;
    }

    fprintf(stderr,
        "\n═══════════════════════════════════════════════════════════════════\n"
        " Summary: %d tensors profiled\n"
        "   A only:    %d\n"
        "   A+M:       %d (multiplicative manifold)\n"
        "   A+M+Π:     %d (+ predictive correction)\n"
        "═══════════════════════════════════════════════════════════════════\n",
        n_processed,
        n_processed - n_scale,
        n_scale - n_pred,
        n_pred);

    fpq_raw_tensor_free(raw, n_raw);
    return 0;
}


/* ═══════════════════════════════════════════════════════════════════
 * Command: roundtrip — encode + decode + measure (no output file)
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_roundtrip(const char *input_path, const char *tensor_filter,
                          size_t limit, int base_bits,
                          int enable_scale, int enable_predict) {
    size_t n_raw;
    fpq_raw_tensor_t *raw = fpqx_read_model(input_path, &n_raw);
    if (!raw || n_raw == 0) {
        fprintf(stderr, "Failed to read: %s\n", input_path);
        return 1;
    }

    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;

    fprintf(stderr,
        "═══════════════════════════════════════════════════════════════════\n"
        " FPQ-X Roundtrip: %s\n"
        " Tensors: %zu   Base bits: %d   Scale: %s   Predict: %s\n"
        "═══════════════════════════════════════════════════════════════════\n\n",
        input_path, n_process, base_bits,
        enable_scale ? "on" : "off",
        enable_predict ? "on" : "off");

    double sum_cos_a = 0.0, sum_cos_final = 0.0;
    float worst_cos = 1.0f;
    double sum_bpw = 0.0;
    int n_enc = 0, n_scale_used = 0, n_pred_used = 0;
    double t_start = wall_clock();

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && !strstr(raw[i].name, tensor_filter)) continue;
        size_t total = raw[i].rows * raw[i].cols;
        if (total < FPQ_BLOCK_DIM * 2 || raw[i].rows <= 1 || raw[i].cols <= 1)
            continue;

        fpqx_tensor_t *t = fpqx_encode(raw[i].data, raw[i].rows, raw[i].cols,
                                          raw[i].name, base_bits);

        /* Decode */
        float *decoded = (float *)malloc(total * sizeof(float));
        fpqx_decode(t, decoded);
        float cos_verify = fpq_cosine_sim(raw[i].data, decoded, total);

        int has_scale = (t->scale != NULL);
        int has_pred = (t->predictor != NULL && t->predictor->mode != FPQX_PREDICT_NONE);

        char ops[16] = "A";
        if (has_scale) { strcat(ops, "+M"); n_scale_used++; }
        if (has_pred) { strcat(ops, "+Π"); n_pred_used++; }

        if (n_enc < 30 || (n_enc % 50 == 0)) {
            float delta_cos = t->cosine_final - t->cosine_pre_scale;
            fprintf(stderr,
                "  [%zu] %-40s cos_A=%.6f cos_final=%.6f (Δ%+.6f) bpw=%.2f  %s\n",
                i, raw[i].name, t->cosine_pre_scale, cos_verify,
                delta_cos, t->bpw, ops);
        }

        sum_cos_a += t->cosine_pre_scale;
        sum_cos_final += cos_verify;
        if (cos_verify < worst_cos) worst_cos = cos_verify;
        sum_bpw += t->bpw;
        n_enc++;

        free(decoded);
        fpqx_tensor_free(t);
    }

    double elapsed = wall_clock() - t_start;

    fprintf(stderr,
        "\n═══════════════════════════════════════════════════════════════════\n"
        " FPQ-X ROUNDTRIP SUMMARY\n"
        "═══════════════════════════════════════════════════════════════════\n"
        "  Tensors encoded:     %d\n"
        "  Avg cosine (A only): %.6f\n"
        "  Avg cosine (FPQ-X):  %.6f\n"
        "  Improvement:         %+.6f\n"
        "  Worst cosine:        %.6f\n"
        "  Avg bpw:             %.2f\n"
        "  Scale (M) used:      %d/%d tensors\n"
        "  Predict (Π) used:    %d/%d tensors\n"
        "  Time:                %.1fs (%.1f tensors/s)\n"
        "═══════════════════════════════════════════════════════════════════\n",
        n_enc,
        n_enc > 0 ? (float)(sum_cos_a / n_enc) : 0.0f,
        n_enc > 0 ? (float)(sum_cos_final / n_enc) : 0.0f,
        n_enc > 0 ? (float)((sum_cos_final - sum_cos_a) / n_enc) : 0.0f,
        worst_cos,
        n_enc > 0 ? (float)(sum_bpw / n_enc) : 0.0f,
        n_scale_used, n_enc,
        n_pred_used, n_enc,
        elapsed, n_enc / (elapsed > 0.001 ? elapsed : 0.001));

    fpq_raw_tensor_free(raw, n_raw);
    return 0;
}


/* ═══════════════════════════════════════════════════════════════════
 * Command: compress — full FPQ-X pipeline with file output
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_compress(const char *input_path, const char *output_path,
                         int base_bits, int use_fpq_format,
                         const char *tensor_filter, size_t limit) {
    size_t n_raw;
    fpq_raw_tensor_t *raw = fpqx_read_model(input_path, &n_raw);
    if (!raw || n_raw == 0) {
        fprintf(stderr, "Failed to read: %s\n", input_path);
        return 1;
    }

    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;

    fprintf(stderr,
        "═══════════════════════════════════════════════════════════════════\n"
        " FPQ-X Compress: %s → %s\n"
        " Pipeline: A + M + Π + Λ (six-operator algebra)\n"
        " Tensors: %zu   Base bits: %d   Format: %s\n"
        "═══════════════════════════════════════════════════════════════════\n\n",
        input_path, output_path, n_process, base_bits,
        use_fpq_format ? "native .fpq" : "BF16 safetensors");

    int n_enc = 0, n_scale = 0, n_pred = 0, n_pass = 0;
    double sum_cos_a = 0.0, sum_cos_final = 0.0;
    float worst_cos = 1.0f;
    double t_start = wall_clock();

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && !strstr(raw[i].name, tensor_filter)) continue;

        size_t total = raw[i].rows * raw[i].cols;
        int skip = (total < FPQ_BLOCK_DIM * 2) ||
                   (raw[i].rows <= 1) || (raw[i].cols <= 1);

        if (skip) {
            n_pass++;
            continue;
        }

        /* Encode with full FPQ-X pipeline */
        fpqx_tensor_t *t = fpqx_encode(raw[i].data, raw[i].rows, raw[i].cols,
                                          raw[i].name, base_bits);

        /* Decode to get the compressed representation */
        float *decoded = (float *)malloc(total * sizeof(float));
        fpqx_decode(t, decoded);

        float cos_final = fpq_cosine_sim(raw[i].data, decoded, total);
        if (cos_final < worst_cos) worst_cos = cos_final;
        sum_cos_a += t->cosine_pre_scale;
        sum_cos_final += cos_final;

        int has_scale = (t->scale != NULL);
        int has_pred = (t->predictor != NULL && t->predictor->mode != FPQX_PREDICT_NONE);
        if (has_scale) n_scale++;
        if (has_pred) n_pred++;

        char ops[16] = "A";
        if (has_scale) strcat(ops, "+M");
        if (has_pred) strcat(ops, "+Π");

        if (n_enc < 30 || (n_enc % 50 == 0)) {
            fprintf(stderr,
                "  [%zu] %-40s η_L=%.3f @%db cos_A=%.6f cos_X=%.6f bpw=%.2f  %s\n",
                i, raw[i].name, t->policy.eta_L, t->policy.recommended_bits,
                t->cosine_pre_scale, cos_final, t->bpw, ops);
        }

        /* Replace raw data with decoded for output */
        memcpy(raw[i].data, decoded, total * sizeof(float));
        n_enc++;

        free(decoded);
        fpqx_tensor_free(t);
    }

    double elapsed = wall_clock() - t_start;

    fprintf(stderr,
        "\n═══════════════════════════════════════════════════════════════════\n"
        " FPQ-X COMPRESS SUMMARY\n"
        "═══════════════════════════════════════════════════════════════════\n"
        "  A (additive):    %d tensors\n"
        "  M (scale):       %d tensors\n"
        "  Π (predict):     %d tensors\n"
        "  Passthrough:     %d tensors\n"
        "  Avg cos (A):     %.6f\n"
        "  Avg cos (FPQ-X): %.6f\n"
        "  Improvement:     %+.6f\n"
        "  Worst cosine:    %.6f\n"
        "  Time:            %.1fs\n"
        "═══════════════════════════════════════════════════════════════════\n",
        n_enc, n_scale, n_pred, n_pass,
        n_enc > 0 ? (float)(sum_cos_a / n_enc) : 0.0f,
        n_enc > 0 ? (float)(sum_cos_final / n_enc) : 0.0f,
        n_enc > 0 ? (float)((sum_cos_final - sum_cos_a) / n_enc) : 0.0f,
        worst_cos, elapsed);

    /* Write output */
    int rc;
    if (use_fpq_format) {
        rc = fpq_native_write(output_path, raw, n_raw);
    } else {
        rc = fpq_safetensors_write(output_path, raw, n_raw);
    }

    fpq_raw_tensor_free(raw, n_raw);
    return rc;
}


/* ═══════════════════════════════════════════════════════════════════
 * Command: distill — KV cache sequence-axis compression
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_distill(const char *input_path, const char *output_path,
                        int target_atoms) {
    size_t n_raw;
    fpq_raw_tensor_t *raw = fpqx_read_model(input_path, &n_raw);
    if (!raw || n_raw == 0) {
        fprintf(stderr, "Failed to read: %s\n", input_path);
        return 1;
    }

    fprintf(stderr,
        "═══════════════════════════════════════════════════════════════════\n"
        " FPQ-X KV Distillation: %s → %s\n"
        " Target atoms: %d\n"
        "═══════════════════════════════════════════════════════════════════\n\n",
        input_path, output_path, target_atoms);

    for (size_t i = 0; i < n_raw; i++) {
        /* Treat each tensor as [seq_len × head_dim] */
        if (raw[i].rows <= 1) continue;
        size_t seq_len = raw[i].rows;
        size_t head_dim = raw[i].cols;

        int atoms = target_atoms;
        if (atoms >= (int)seq_len) atoms = (int)seq_len;

        fpqx_distilled_cache_t *dc = fpqx_distill(raw[i].data, seq_len, head_dim,
                                                     NULL, atoms);

        /* Compute distillation quality */
        float *recon = (float *)calloc(seq_len * head_dim, sizeof(float));
        /* Simple: assign each row to nearest atom */
        for (size_t s = 0; s < seq_len; s++) {
            float best_d = FLT_MAX;
            int best_a = 0;
            for (int a = 0; a < dc->n_atoms; a++) {
                float d = 0.0f;
                for (size_t d2 = 0; d2 < head_dim; d2++) {
                    float diff = raw[i].data[s * head_dim + d2] -
                                 dc->atoms[a * head_dim + d2];
                    d += diff * diff;
                }
                if (d < best_d) { best_d = d; best_a = a; }
            }
            memcpy(recon + s * head_dim,
                   dc->atoms + best_a * head_dim,
                   head_dim * sizeof(float));
        }

        float cos = fpq_cosine_sim(raw[i].data, recon,
                                    seq_len * head_dim);
        float ratio = (float)seq_len / atoms;

        fprintf(stderr, "  %-40s %zux%zu → %d atoms (%.1f×)  cos=%.6f\n",
                raw[i].name, seq_len, head_dim, atoms, ratio, cos);

        /* Replace data with distilled reconstruction for output */
        /* Reshape to [atoms × head_dim] */
        free(raw[i].data);
        raw[i].data = (float *)malloc((size_t)atoms * head_dim * sizeof(float));
        memcpy(raw[i].data, dc->atoms, (size_t)atoms * head_dim * sizeof(float));
        raw[i].rows = atoms;
        raw[i].n_elements = (size_t)atoms * head_dim;

        free(recon);
        fpqx_distill_free(dc);
    }

    int rc = fpq_safetensors_write(output_path, raw, n_raw);
    fpq_raw_tensor_free(raw, n_raw);
    return rc;
}


/* ═══════════════════════════════════════════════════════════════════
 * main
 * ═══════════════════════════════════════════════════════════════════ */

int main(int argc, char **argv) {
    if (argc < 2) {
        fpqx_usage();
        return 1;
    }

    const char *cmd = argv[1];

    /* Parse global options */
    int base_bits = 3;
    size_t limit = 0;
    const char *tensor_filter = NULL;
    int enable_scale = 1;
    int enable_predict = 1;
    int use_fpq_format = 0;
    int target_atoms = 256;
    size_t group_size = 128;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--bits") == 0 && i + 1 < argc)
            base_bits = atoi(argv[++i]);
        else if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc)
            limit = (size_t)atoi(argv[++i]);
        else if (strcmp(argv[i], "--tensor") == 0 && i + 1 < argc)
            tensor_filter = argv[++i];
        else if (strcmp(argv[i], "--no-scale") == 0)
            enable_scale = 0;
        else if (strcmp(argv[i], "--no-predict") == 0)
            enable_predict = 0;
        else if (strcmp(argv[i], "--format") == 0 && i + 1 < argc) {
            i++;
            if (strcmp(argv[i], "fpq") == 0) use_fpq_format = 1;
        }
        else if (strcmp(argv[i], "--atoms") == 0 && i + 1 < argc)
            target_atoms = atoi(argv[++i]);
        else if (strcmp(argv[i], "--group-size") == 0 && i + 1 < argc)
            group_size = (size_t)atoi(argv[++i]);
    }

    /* ── Command dispatch ── */

    if (strcmp(cmd, "compress") == 0) {
        if (argc < 4) {
            fprintf(stderr,
                "Usage: bonfyre-fpqx compress <input> <output> [opts]\n"
                "  --bits N         Base bits (2/3/4, default: 3)\n"
                "  --format fpq     Native .fpq output (default: safetensors)\n"
                "  --no-scale       Disable multiplicative manifold\n"
                "  --no-predict     Disable predictive correction\n"
                "  --limit N        Process first N tensors\n"
                "  --tensor <name>  Filter by tensor name\n");
            return 1;
        }
        return cmd_compress(argv[2], argv[3], base_bits, use_fpq_format,
                            tensor_filter, limit);

    } else if (strcmp(cmd, "roundtrip") == 0) {
        if (argc < 3) {
            fprintf(stderr,
                "Usage: bonfyre-fpqx roundtrip <input> [opts]\n"
                "  Encode + decode + measure (no output file)\n");
            return 1;
        }
        return cmd_roundtrip(argv[2], tensor_filter, limit, base_bits,
                             enable_scale, enable_predict);

    } else if (strcmp(cmd, "profile") == 0) {
        if (argc < 3) {
            fprintf(stderr,
                "Usage: bonfyre-fpqx profile <input> [opts]\n"
                "  Per-tensor compressibility analysis\n");
            return 1;
        }
        return cmd_profile(argv[2], tensor_filter, limit, base_bits);

    } else if (strcmp(cmd, "distill") == 0) {
        if (argc < 4) {
            fprintf(stderr,
                "Usage: bonfyre-fpqx distill <cache.safetensors> <output> [opts]\n"
                "  --atoms N  Target distilled entries (default: 256)\n");
            return 1;
        }
        return cmd_distill(argv[2], argv[3], target_atoms);

    } else if (strcmp(cmd, "pack") == 0) {
        if (argc < 4) {
            fprintf(stderr,
                "Usage: bonfyre-fpqx pack <input> <output> [opts]\n"
                "  --bits N         Bits per weight\n"
                "  --group-size N   Quantization group size (default: 128)\n");
            return 1;
        }
        /* Pack command: read, quantize-pack, write packed data report */
        size_t n_raw;
        fpq_raw_tensor_t *raw = fpqx_read_model(argv[2], &n_raw);
        if (!raw || n_raw == 0) return 1;

        fprintf(stderr,
            "═══════════════════════════════════════════════════════\n"
            " FPQ-X Hardware Pack: %s → %s\n"
            " Bits: %d   Group: %zu   Mode: inner-group\n"
            "═══════════════════════════════════════════════════════\n\n",
            argv[2], argv[3], base_bits, group_size);

        double total_orig = 0.0, total_packed = 0.0;
        for (size_t i = 0; i < n_raw; i++) {
            size_t total = raw[i].rows * raw[i].cols;
            if (total < 256) continue;

            fpqx_packed_t *pk = fpqx_pack(raw[i].data, raw[i].rows, raw[i].cols,
                                            base_bits, FPQX_PACK_INNER_GROUP,
                                            group_size);

            double orig_bytes = total * 4.0;
            double pack_bytes = pk->packed_bytes +
                                pk->n_groups * sizeof(float);
            total_orig += orig_bytes;
            total_packed += pack_bytes;

            fprintf(stderr, "  %-40s %.1f KB → %.1f KB (%.1f×)\n",
                    raw[i].name,
                    orig_bytes / 1024.0,
                    pack_bytes / 1024.0,
                    orig_bytes / pack_bytes);

            fpqx_pack_free(pk);
        }

        fprintf(stderr,
            "\n  Total: %.1f MB → %.1f MB (%.1f×)\n",
            total_orig / (1024.0 * 1024.0),
            total_packed / (1024.0 * 1024.0),
            total_orig / total_packed);

        fpq_raw_tensor_free(raw, n_raw);
        return 0;

    } else if (strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0 ||
               strcmp(cmd, "help") == 0) {
        fpqx_usage();
        return 0;

    } else {
        fprintf(stderr, "Unknown command: %s\n\n", cmd);
        fpqx_usage();
        return 1;
    }
}
