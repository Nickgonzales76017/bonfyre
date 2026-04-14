/*
 * fpq_cli.c — Unified `fpq` command-line tool
 *
 * One binary. Mirrors the Ollama UX:
 *   fpq info   <model.fpq>             — Print model metadata
 *   fpq bench  <model.fpq>             — SLI quality + speed benchmark
 *   fpq decode <model.fpq> <out.safetensors>  — Decode to safetensors
 *   fpq convert <in.safetensors> <out.fpq>    — Encode to .fpq
 *
 * Future:
 *   fpq run    <model.fpq> [input]     — Auto-detect modality, infer
 *   fpq serve  <model.fpq> [--port]    — OpenAI-compatible API
 *   fpq pull   <hf-repo>               — Download from HuggingFace
 *   fpq export-gguf <model.fpq> <out>  — Convert to GGUF for Ollama
 */

#include "libfpq.h"
#include "fpq.h"
#include "fpqx.h"
#include "fpq_run.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/time.h>

static double now(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1e6;
}

static void print_usage(void) {
    fprintf(stderr,
        "fpq — Bonfyre Ember inference engine\n\n"
        "Usage:\n"
        "  fpq run     <model.fpq> \"prompt\" [options]  Generate text (LLaMA/TinyLlama)\n"
        "  fpq info    <model.fpq>                      Show model metadata\n"
        "  fpq bench   <model.fpq> [--limit N]          SLI quality + speed benchmark\n"
        "  fpq decode  <model.fpq> <out.safetensors>    Decode to safetensors\n"
        "  fpq convert <in.safetensors> <out.fpq>       Encode to .fpq\n"
        "\n"
        "Run options:\n"
        "  --tokenizer <path>   tokenizer.json location (default: model dir)\n"
        "  --sys \"text\"         System prompt\n"
        "  --max-tokens N       Max tokens (default: 512)\n"
        "  --temp F             Temperature (default: 0.6)\n"
        "  --top-p F            Top-p (default: 0.9)\n"
        "  --greedy             Greedy decoding\n"
        "  --no-chat            Skip chat template\n"
        "\n"
        "Future:\n"
        "  fpq serve   <model.fpq> [--port 8080]        OpenAI-compatible API\n"
        "  fpq pull    <hf-repo>                        Download from HuggingFace\n"
        "  fpq export-gguf <model.fpq> <out.gguf>       Convert for Ollama\n"
        "\n");
}


/* ═══════════════════════════════════════════════════════════════════
 * fpq info — Print model metadata
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_info(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: fpq info <model.fpq>\n");
        return 1;
    }

    fpq_model_t *m = fpq_open(argv[2]);
    if (!m) return 1;

    fpq_info_t info = fpq_info(m);

    printf("Model:        %s\n", argv[2]);
    printf("Format:       FPQ v%u\n", info.format_version);
    printf("Tensors:      %zu total (%zu SLI, %zu passthrough)\n",
           info.n_tensors, info.n_sli_tensors, info.n_passthrough);
    printf("Parameters:   %zuM\n", info.total_params / 1000000);
    printf("\n");

    printf("%-50s  %8s  %8s  %5s\n", "TENSOR", "SHAPE", "PARAMS", "SLI");
    printf("%-50s  %8s  %8s  %5s\n",
           "--------------------------------------------------",
           "--------", "--------", "-----");

    for (size_t i = 0; i < info.n_tensors; i++) {
        const fpq_tensor_info_t *ti = fpq_tensor_at(m, i);
        if (!ti) continue;
        printf("%-50s  %4zux%-4zu  %7zuK  %s\n",
               ti->name, ti->rows, ti->cols,
               (ti->rows * ti->cols) / 1000,
               ti->has_sli ? " yes" : "  no");
    }

    fpq_close(m);
    return 0;
}


/* ═══════════════════════════════════════════════════════════════════
 * fpq bench — SLI quality + speed benchmark (via libfpq)
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_bench(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: fpq bench <model.fpq> [--limit N]\n");
        return 1;
    }

    int limit = 0;
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc)
            limit = atoi(argv[++i]);
    }

    /* Open model via libfpq (compressed load + SLI prepare) */
    double t0 = now();
    fpq_model_t *m = fpq_open(argv[2]);
    if (!m) return 1;
    double t_load = now() - t0;

    fpq_info_t info = fpq_info(m);

    printf("═══════════════════════════════════════════════════════════\n");
    printf(" FPQ Bench: %s\n", argv[2]);
    printf(" Tensors: %zu (%zu SLI) | Params: %zuM | Load: %.2fs\n",
           info.n_tensors, info.n_sli_tensors, info.total_params / 1000000,
           t_load);
    printf("═══════════════════════════════════════════════════════════\n\n");

    /* Also load via standard decoder for quality comparison */
    size_t n_raw = 0;
    fpq_raw_tensor_t *raw = fpq_native_read(argv[2], &n_raw);
    if (!raw) {
        fprintf(stderr, "bench: could not read decoded tensors for comparison\n");
        fpq_close(m);
        return 1;
    }

    int n_tested = 0;
    float worst_cos = 1.0f;
    double sum_cos = 0.0;
    double total_sli_us = 0.0;
    double total_dense_us = 0.0;

    for (size_t i = 0; i < n_raw; i++) {
        if (limit > 0 && n_tested >= limit) break;

        const fpq_tensor_info_t *ti = fpq_tensor_find(m, raw[i].name);
        if (!ti || !ti->has_sli) continue;

        size_t rows = raw[i].rows, cols = raw[i].cols;

        /* Random activation vector */
        float *x = (float *)malloc(cols * sizeof(float));
        uint64_t xseed = 0x42424242ULL ^ (uint64_t)i;
        for (size_t j = 0; j < cols; j++) {
            xseed ^= xseed << 13; xseed ^= xseed >> 7; xseed ^= xseed << 17;
            x[j] = ((float)(xseed & 0xFFFF) / 32768.0f) - 1.0f;
        }

        /* Dense reference: y_ref = W_decoded @ x */
        float *y_ref = (float *)calloc(rows, sizeof(float));
        double t1 = now();
        for (size_t r = 0; r < rows; r++) {
            float dot = 0.0f;
            for (size_t c = 0; c < cols; c++)
                dot += raw[i].data[r * cols + c] * x[c];
            y_ref[r] = dot;
        }
        double dense_us = (now() - t1) * 1e6;

        /* SLI: y_sli = W_compressed @ x */
        float *y_sli = (float *)calloc(rows, sizeof(float));
        double t2 = now();
        fpq_matmul(m, raw[i].name, x, y_sli);
        double sli_us = (now() - t2) * 1e6;

        /* Quality: cosine similarity */
        double dot_ab = 0.0, dot_aa = 0.0, dot_bb = 0.0;
        for (size_t r = 0; r < rows; r++) {
            dot_ab += (double)y_ref[r] * (double)y_sli[r];
            dot_aa += (double)y_ref[r] * (double)y_ref[r];
            dot_bb += (double)y_sli[r] * (double)y_sli[r];
        }
        float cos = (float)(dot_ab / (sqrt(dot_aa) * sqrt(dot_bb) + 1e-10));

        printf("  %-40s [%4zux%-4zu]  cos=%.8f  dense=%6.0fus  sli=%6.0fus  %.1fx\n",
               raw[i].name, rows, cols, cos, dense_us, sli_us,
               dense_us / (sli_us + 0.001));

        sum_cos += cos;
        if (cos < worst_cos) worst_cos = cos;
        total_sli_us += sli_us;
        total_dense_us += dense_us;
        n_tested++;

        free(x);
        free(y_ref);
        free(y_sli);
    }

    printf("\n═══════════════════════════════════════════════════════════\n");
    printf(" Results: %d tensors tested\n", n_tested);
    printf(" Mean cosine:   %.10f\n", n_tested > 0 ? sum_cos / n_tested : 0.0);
    printf(" Worst cosine:  %.10f\n", worst_cos);
    printf(" Total dense:   %.1f ms\n", total_dense_us / 1000.0);
    printf(" Total SLI:     %.1f ms\n", total_sli_us / 1000.0);
    printf(" Speedup:       %.1fx bandwidth reduction\n",
           total_dense_us / (total_sli_us + 0.001));
    printf("═══════════════════════════════════════════════════════════\n");

    /* Cleanup */
    for (size_t i = 0; i < n_raw; i++)
        free(raw[i].data);
    free(raw);
    fpq_close(m);

    return 0;
}


/* ═══════════════════════════════════════════════════════════════════
 * fpq decode — Decode .fpq to safetensors
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_decode(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: fpq decode <model.fpq> <output.safetensors>\n");
        return 1;
    }

    fpq_model_t *m = fpq_open(argv[2]);
    if (!m) return 1;

    printf("Decoding %s → %s...\n", argv[2], argv[3]);
    int rc = fpq_decode_all(m, argv[3]);
    if (rc == 0)
        printf("Done.\n");
    else
        fprintf(stderr, "Decode failed.\n");

    fpq_close(m);
    return rc;
}


/* ═══════════════════════════════════════════════════════════════════
 * fpq convert — Encode safetensors/GGUF to .fpq
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_convert(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: fpq convert <input.safetensors> <output.fpq> [--bits N]\n");
        return 1;
    }

    /* Parse bits flag */
    int bits = 3;
    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "--bits") == 0 && i + 1 < argc)
            bits = atoi(argv[++i]);
    }

    /* Read input model */
    size_t n_raw = 0;
    fpq_raw_tensor_t *raw = NULL;

    size_t path_len = strlen(argv[2]);
    if (path_len > 12 && strcmp(argv[2] + path_len - 12, ".safetensors") == 0)
        raw = fpq_safetensors_read(argv[2], &n_raw);
    else if (path_len > 4 && strcmp(argv[2] + path_len - 4, ".bin") == 0)
        raw = fpq_ggml_read(argv[2], &n_raw);
    else
        raw = fpq_safetensors_read(argv[2], &n_raw); /* default */

    if (!raw || n_raw == 0) {
        fprintf(stderr, "convert: failed to read %s\n", argv[2]);
        return 1;
    }

    printf("Converting %s (%zu tensors) → %s (bits=%d)\n",
           argv[2], n_raw, argv[3], bits);

    int rc = fpq_native_write(argv[3], raw, n_raw);

    for (size_t i = 0; i < n_raw; i++)
        free(raw[i].data);
    free(raw);

    if (rc == 0) printf("Done.\n");
    return rc;
}


/* ═══════════════════════════════════════════════════════════════════
 * main — dispatch to subcommand
 * ═══════════════════════════════════════════════════════════════════ */

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

    const char *cmd = argv[1];

    if (strcmp(cmd, "run") == 0)     return cmd_run(argc, argv);
    if (strcmp(cmd, "info") == 0)    return cmd_info(argc, argv);
    if (strcmp(cmd, "bench") == 0)   return cmd_bench(argc, argv);
    if (strcmp(cmd, "decode") == 0)  return cmd_decode(argc, argv);
    if (strcmp(cmd, "convert") == 0) return cmd_convert(argc, argv);

    /* Future commands */
    if (strcmp(cmd, "serve") == 0 ||
        strcmp(cmd, "pull") == 0 ||
        strcmp(cmd, "export-gguf") == 0) {
        fprintf(stderr, "fpq %s: not yet implemented\n", cmd);
        return 1;
    }

    if (strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0) {
        print_usage();
        return 0;
    }

    fprintf(stderr, "fpq: unknown command '%s'\n\n", cmd);
    print_usage();
    return 1;
}
