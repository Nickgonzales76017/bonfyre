/*
 * main.c — bonfyre-fpq CLI
 *
 * The tool that replaces weight files with seed programs.
 *
 * Usage:
 *   bonfyre-fpq compress <model.bin> <output.fpq> [options]
 *   bonfyre-fpq decompress <file.fpq> <output.bin>
 *   bonfyre-fpq inspect <file.fpq>
 *   bonfyre-fpq roundtrip <model.bin> [options]   (compress+decompress+measure)
 *   bonfyre-fpq roundtrip-v4 <model.bin> [options] (v4: chaos+ghost+SBB)
 *   bonfyre-fpq roundtrip-v5 <model.bin> [options] (v5: PID+chaos+ghost)
 *
 * Options:
 *   --max-nodes <N>     Max seed combinator nodes per block (default: 32)
 *   --tolerance <f>     Target MSE tolerance (default: 0.01)
 *   --report            Print per-tensor compression stats
 *   --tensor <name>     Only process named tensor (for testing)
 *   --limit <N>         Only process first N tensors
 *   --bits <N>          Force COORD bit depth (2/3/4, default: auto)
 */
#include "fpq.h"
#include "weight_algebra.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/stat.h>

/* Auto-detect model format and read tensors */
static fpq_raw_tensor_t *fpq_read_model(const char *path, size_t *n_tensors) {
    struct stat st;
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
        /* Directory → assume safetensors sharded model */
        return fpq_safetensors_read(path, n_tensors);
    }
    size_t len = strlen(path);
    if (len > 12 && strcmp(path + len - 12, ".safetensors") == 0) {
        return fpq_safetensors_read(path, n_tensors);
    }
    /* Default: GGUF/GGML */
    return fpq_ggml_read(path, n_tensors);
}

static void usage(void) {
    fprintf(stderr,
        "bonfyre-fpq — Functional Polar Quantization\n"
        "Store seed programs, not weights.\n\n"
        "Usage:\n"
        "  bonfyre-fpq compress  <model.bin> <output.fpq> [opts]\n"
        "  bonfyre-fpq decompress <file.fpq> <output.bin>\n"
        "  bonfyre-fpq inspect   <file.fpq>\n"
        "  bonfyre-fpq roundtrip <model.bin> [opts]\n\n"
        "Options:\n"
        "  --max-nodes <N>    Max combinator nodes/block (default: 32)\n"
        "  --tolerance <f>    MSE tolerance (default: 0.01)\n"
        "  --report           Print per-tensor stats\n"
        "  --tensor <name>    Only process named tensor\n"
        "  --limit <N>        Only process first N tensors\n"
    );
}

/* ── Command: compress ── */

static int cmd_compress(const char *input, const char *output,
                         size_t max_nodes, float tolerance,
                         int report, const char *tensor_filter,
                         size_t limit) {
    fprintf(stderr, "FPQ Compress: %s → %s\n", input, output);
    fprintf(stderr, "  max_nodes=%zu  tolerance=%.4f\n", max_nodes, tolerance);

    /* Read model */
    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_read_model(input, &n_raw);
    if (!raw || n_raw == 0) {
        fprintf(stderr, "Failed to read model from %s\n", input);
        return 1;
    }

    /* Encode each tensor */
    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;
    fpq_tensor_t **encoded = (fpq_tensor_t **)calloc(n_process, sizeof(fpq_tensor_t *));
    size_t n_encoded = 0;
    size_t total_original_bytes = 0;

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && strcmp(raw[i].name, tensor_filter) != 0) continue;

        fprintf(stderr, "\n[%zu/%zu] Encoding: %s (%zu params)\n",
                i + 1, n_process, raw[i].name, raw[i].n_elements);

        fpq_tensor_t *t = fpq_encode_tensor(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, max_nodes, tolerance);

        if (report) {
            fpq_report(t);
        }

        encoded[n_encoded++] = t;
        total_original_bytes += raw[i].n_elements * sizeof(float);
    }

    /* Save */
    fpq_save(output, encoded, n_encoded);

    /* Summary */
    size_t total_seed_bits = 0;
    for (size_t i = 0; i < n_encoded; i++) {
        total_seed_bits += encoded[i]->total_bits;
    }
    float orig_mb = (float)total_original_bytes / (1024.0f * 1024.0f);
    float comp_mb = (float)(total_seed_bits / 8) / (1024.0f * 1024.0f);

    fprintf(stderr,
        "\n═══ FPQ Compression Complete ═══\n"
        "  Tensors:    %zu\n"
        "  Original:   %.2f MB (fp32)\n"
        "  Compressed: %.2f MB (seed programs)\n"
        "  Ratio:      %.1fx\n"
        "  Avg bpw:    %.2f\n",
        n_encoded, orig_mb, comp_mb,
        comp_mb > 0 ? orig_mb / comp_mb : 0.0f,
        total_original_bytes > 0
            ? (float)total_seed_bits / (float)(total_original_bytes / sizeof(float))
            : 0.0f);

    /* Cleanup */
    for (size_t i = 0; i < n_encoded; i++) fpq_tensor_free(encoded[i]);
    free(encoded);
    fpq_raw_tensor_free(raw, n_raw);

    return 0;
}

/* ── Command: decompress ── */

static int cmd_decompress(const char *input, const char *output) {
    fprintf(stderr, "FPQ Decompress: %s → %s\n", input, output);

    size_t n_tensors;
    fpq_tensor_t **tensors = fpq_load(input, &n_tensors);
    if (!tensors) return 1;

    FILE *f = fopen(output, "wb");
    if (!f) {
        fprintf(stderr, "Cannot open %s for writing\n", output);
        return 1;
    }

    /* Write as raw f32 (for now — could output GGML format) */
    for (size_t t = 0; t < n_tensors; t++) {
        size_t n = tensors[t]->original_rows * tensors[t]->original_cols;
        float *weights = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor(tensors[t], weights);

        fprintf(stderr, "  Decoded: %s (%zu × %zu)\n",
                tensors[t]->name,
                tensors[t]->original_rows,
                tensors[t]->original_cols);

        fwrite(weights, sizeof(float), n, f);
        free(weights);
    }

    fclose(f);

    /* Cleanup */
    for (size_t i = 0; i < n_tensors; i++) fpq_tensor_free(tensors[i]);
    free(tensors);

    return 0;
}

/* ── Command: inspect ── */

static int cmd_inspect(const char *input) {
    fprintf(stderr, "FPQ Inspect: %s\n", input);

    size_t n_tensors;
    fpq_tensor_t **tensors = fpq_load(input, &n_tensors);
    if (!tensors) return 1;

    size_t grand_total_bits = 0;
    size_t grand_total_weights = 0;

    for (size_t t = 0; t < n_tensors; t++) {
        fpq_report(tensors[t]);
        grand_total_bits += tensors[t]->total_bits;
        grand_total_weights += tensors[t]->original_rows * tensors[t]->original_cols;
    }

    float total_mb = (float)(grand_total_bits / 8) / (1024.0f * 1024.0f);
    float orig_mb = (float)(grand_total_weights * sizeof(float)) / (1024.0f * 1024.0f);

    fprintf(stderr,
        "\n═══ FPQ Model Summary ═══\n"
        "  File:       %s\n"
        "  Tensors:    %zu\n"
        "  Weights:    %zu total\n"
        "  Seed size:  %.2f MB\n"
        "  fp32 size:  %.2f MB\n"
        "  Ratio:      %.1fx\n"
        "  Avg bpw:    %.2f\n",
        input, n_tensors, grand_total_weights,
        total_mb, orig_mb,
        total_mb > 0 ? orig_mb / total_mb : 0.0f,
        grand_total_weights > 0
            ? (float)grand_total_bits / (float)grand_total_weights
            : 0.0f);

    for (size_t i = 0; i < n_tensors; i++) fpq_tensor_free(tensors[i]);
    free(tensors);
    return 0;
}

/* ── Command: roundtrip ── */

static int cmd_roundtrip(const char *input,
                          size_t max_nodes, float tolerance,
                          const char *tensor_filter, size_t limit) {
    fprintf(stderr, "FPQ Roundtrip Test: %s\n", input);
    fprintf(stderr, "  max_nodes=%zu  tolerance=%.4f\n", max_nodes, tolerance);

    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_read_model(input, &n_raw);
    if (!raw || n_raw == 0) return 1;

    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;
    float worst_mse = 0.0f;
    float best_cosine = 1.0f;

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && strcmp(raw[i].name, tensor_filter) != 0) continue;

        fprintf(stderr, "\n[%zu/%zu] %s (%zu params)\n",
                i + 1, n_process, raw[i].name, raw[i].n_elements);

        /* Encode */
        fpq_tensor_t *t = fpq_encode_tensor(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, max_nodes, tolerance);

        /* Decode */
        size_t n = raw[i].rows * raw[i].cols;
        float *decoded = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor(t, decoded);

        /* Measure */
        float mse = fpq_mse(raw[i].data, decoded, n);
        float cosine = fpq_cosine_sim(raw[i].data, decoded, n);

        if (mse > worst_mse) worst_mse = mse;
        if (cosine < best_cosine) best_cosine = cosine;

        fpq_report(t);
        fprintf(stderr, "  Roundtrip MSE:    %.8f\n", mse);
        fprintf(stderr, "  Cosine sim:       %.6f\n", cosine);

        float bpw = fpq_bits_per_weight(t);
        fprintf(stderr, "  Status:           %s\n",
                cosine > 0.99f ? "EXCELLENT" :
                cosine > 0.95f ? "GOOD" :
                cosine > 0.90f ? "ACCEPTABLE" : "NEEDS TUNING");

        free(decoded);
        fpq_tensor_free(t);
    }

    fprintf(stderr,
        "\n═══ Roundtrip Summary ═══\n"
        "  Worst MSE:     %.8f\n"
        "  Best cosine:   %.6f (worst across tensors)\n"
        "  Verdict:       %s\n",
        worst_mse, best_cosine,
        best_cosine > 0.99f ? "HIGH FIDELITY" :
        best_cosine > 0.95f ? "PRODUCTION READY" :
        best_cosine > 0.90f ? "USABLE" : "INCREASE NODES/TOLERANCE");

    fpq_raw_tensor_free(raw, n_raw);
    return 0;
}

/* ── Command: roundtrip-v4 (chaos + ghost + SBB) ── */

/* Helper: detect layer group from tensor name.
 * Returns layer number, or -1 if not an attention/ffn weight.
 * Sets *role to identify Q/K/V/O/gate/up within the layer. */
static int parse_layer_group(const char *name, int *role) {
    *role = -1;
    int layer = -1;

    /* GGUF Gemma: blk.N.attn_q.weight, blk.N.attn_k.weight, etc. */
    if (sscanf(name, "blk.%d.", &layer) == 1) {
        if (strstr(name, "attn_q."))      *role = 0;
        else if (strstr(name, "attn_k.")) *role = 1;
        else if (strstr(name, "attn_v.")) *role = 2;
        else if (strstr(name, "attn_output.")) *role = 3;
        else return -1;
        return layer;
    }

    /* Whisper: encoder.blocks.N.attn.query.weight, etc. */
    if (sscanf(name, "encoder.blocks.%d.", &layer) == 1 ||
        sscanf(name, "decoder.blocks.%d.", &layer) == 1) {
        if (strstr(name, ".query."))     *role = 0;
        else if (strstr(name, ".key."))  *role = 1;
        else if (strstr(name, ".value.")) *role = 2;
        else if (strstr(name, ".out."))  *role = 3;
        else return -1;
        return layer;
    }

    return -1;
}

static int cmd_roundtrip_v4(const char *input,
                             const char *tensor_filter, size_t limit,
                             int force_bits) {
    fprintf(stderr,
        "═══════════════════════════════════════════════════════\n"
        " FPQ v4 Roundtrip: CHAOS + GHOST + SBB\n"
        " Model: %s\n"
        "═══════════════════════════════════════════════════════\n",
        input);

    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_read_model(input, &n_raw);
    if (!raw || n_raw == 0) return 1;

    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;

    /* ── Phase 1: Identify layer groups for SBB ── */
    /* Group attention Q/K/V/O tensors by layer number.
     * We need tensors with the SAME element count for SBB. */
    typedef struct {
        int layer;
        int tensor_indices[FPQ_SBB_MAX_GROUP]; /* indices into raw[] */
        int roles[FPQ_SBB_MAX_GROUP];
        int count;
    } layer_group_t;

    layer_group_t *groups = (layer_group_t *)calloc(256, sizeof(layer_group_t));
    int n_groups = 0;

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && strcmp(raw[i].name, tensor_filter) != 0) continue;
        if (raw[i].n_elements < FPQ_BLOCK_DIM) continue; /* skip tiny tensors */

        int role;
        int layer = parse_layer_group(raw[i].name, &role);
        if (layer < 0) continue;

        /* Find or create group for this layer */
        int found = -1;
        for (int g = 0; g < n_groups; g++) {
            if (groups[g].layer == layer) { found = g; break; }
        }
        if (found < 0) {
            found = n_groups++;
            groups[found].layer = layer;
            groups[found].count = 0;
        }
        if (groups[found].count < FPQ_SBB_MAX_GROUP) {
            groups[found].tensor_indices[groups[found].count] = (int)i;
            groups[found].roles[groups[found].count] = role;
            groups[found].count++;
        }
    }

    fprintf(stderr, "\nIdentified %d layer groups for SBB\n", n_groups);

    /* ── Phase 2: Process each tensor with v4 pipeline ── */
    float worst_mse_v3 = 0.0f, worst_mse_v4 = 0.0f;
    float best_cosine_v3 = 1.0f, best_cosine_v4 = 1.0f;
    int n_tested = 0;

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && strcmp(raw[i].name, tensor_filter) != 0) continue;
        if (raw[i].n_elements < FPQ_BLOCK_DIM) continue;
        /* Skip 1-D tensors (norms, biases) */
        if (raw[i].rows <= 1) continue;

        fprintf(stderr, "\n[%zu/%zu] %s (%zu params, %zu×%zu)\n",
                i + 1, n_process, raw[i].name, raw[i].n_elements,
                raw[i].rows, raw[i].cols);

        int cbits = force_bits > 0 ? force_bits : 3;

        /* ── v3 BASELINE: standard COORD encode ── */
        fpq_tensor_t *t_v3 = fpq_encode_tensor(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, 32, 0.01f);

        size_t n = raw[i].rows * raw[i].cols;
        float *decoded_v3 = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor(t_v3, decoded_v3);

        float mse_v3 = fpq_mse(raw[i].data, decoded_v3, n);
        float cos_v3 = fpq_cosine_sim(raw[i].data, decoded_v3, n);
        float bpw_v3 = fpq_bits_per_weight(t_v3);

        /* ── v4: CHAOS + GHOST (+ SBB if in a group) ── */

        /* Check if this tensor belongs to an SBB group */
        fpq_sbb_t *sbb = NULL;
        int sbb_idx = -1;

        for (int g = 0; g < n_groups; g++) {
            for (int k = 0; k < groups[g].count; k++) {
                if ((size_t)groups[g].tensor_indices[k] == i &&
                    groups[g].count >= 2) {
                    /* Build SBB for this group (lazy: compute on demand) */
                    /* Find all tensors with same n_elements in this group */
                    const float *group_ptrs[FPQ_SBB_MAX_GROUP];
                    int group_count = 0;
                    int my_idx = -1;
                    for (int m = 0; m < groups[g].count; m++) {
                        int ti = groups[g].tensor_indices[m];
                        if (raw[ti].n_elements == raw[i].n_elements) {
                            if (ti == (int)i) my_idx = group_count;
                            group_ptrs[group_count++] = raw[ti].data;
                        }
                    }
                    if (group_count >= 2 && my_idx >= 0) {
                        sbb = fpq_sbb_compute(group_ptrs, (size_t)group_count,
                                               raw[i].n_elements,
                                               0x12345678ULL);
                        sbb_idx = my_idx;
                    }
                    goto sbb_done;
                }
            }
        }
sbb_done:
        ;  /* empty statement after label */
        fpq_tensor_t *t_v4 = fpq_encode_tensor_v4(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits, sbb, sbb_idx);

        float *decoded_v4 = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor_v4(t_v4, decoded_v4);

        float mse_v4 = fpq_mse(raw[i].data, decoded_v4, n);
        float cos_v4 = fpq_cosine_sim(raw[i].data, decoded_v4, n);
        float bpw_v4 = (float)t_v4->total_bits / (float)n;

        /* Track worst cases */
        if (mse_v3 > worst_mse_v3) worst_mse_v3 = mse_v3;
        if (mse_v4 > worst_mse_v4) worst_mse_v4 = mse_v4;
        if (cos_v3 < best_cosine_v3) best_cosine_v3 = cos_v3;
        if (cos_v4 < best_cosine_v4) best_cosine_v4 = cos_v4;

        float cos_delta = cos_v4 - cos_v3;

        fprintf(stderr,
            "  ╔═══ v3 vs v4 COMPARISON ═══╗\n"
            "  ║ v3 COORD@%d+QJL:          ║\n"
            "  ║   MSE=%.8f  cos=%.6f ║\n"
            "  ║   bpw=%.2f                ║\n"
            "  ║ v4 CHAOS+GHOST%s:       ║\n"
            "  ║   MSE=%.8f  cos=%.6f ║\n"
            "  ║   bpw=%.2f                ║\n"
            "  ║ Δcos: %+.6f (%s)    ║\n"
            "  ╚════════════════════════════╝\n",
            t_v3->coord_bits,
            mse_v3, cos_v3, bpw_v3,
            sbb ? "+SBB" : "    ",
            mse_v4, cos_v4, bpw_v4,
            cos_delta,
            cos_delta > 0.001f ? "IMPROVED" :
            cos_delta > 0.0f   ? "marginal" : "REGRESSED");

        n_tested++;

        free(decoded_v3);
        free(decoded_v4);
        fpq_tensor_free(t_v3);
        fpq_tensor_free(t_v4);
        if (sbb) fpq_sbb_free(sbb);
    }

    fprintf(stderr,
        "\n═══════════════════════════════════════════════════════\n"
        " v4 ROUNDTRIP SUMMARY (%d tensors)\n"
        "═══════════════════════════════════════════════════════\n"
        "  v3 worst cosine: %.6f\n"
        "  v4 worst cosine: %.6f\n"
        "  v3 worst MSE:    %.8f\n"
        "  v4 worst MSE:    %.8f\n"
        "  Verdict:         %s\n"
        "═══════════════════════════════════════════════════════\n",
        n_tested,
        best_cosine_v3, best_cosine_v4,
        worst_mse_v3, worst_mse_v4,
        best_cosine_v4 > best_cosine_v3
            ? "v4 WINS" : "v3 baseline holds");

    free(groups);
    fpq_raw_tensor_free(raw, n_raw);
    return 0;
}


/* ── Command: lie-probe (measure correlation in multiple domains) ── */

static int cmd_lie_probe(const char *input,
                          const char *tensor_filter, size_t limit) {
    fprintf(stderr,
        "═══════════════════════════════════════════════════════\n"
        " LIE ALGEBRA CORRELATION PROBE\n"
        " Model: %s\n"
        "═══════════════════════════════════════════════════════\n",
        input);

    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_read_model(input, &n_raw);
    if (!raw || n_raw == 0) return 1;

    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;

    float best_lie_r = 0.0f;
    const char *best_name = "";
    int n_tested = 0;

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && strcmp(raw[i].name, tensor_filter) != 0) continue;
        if (raw[i].n_elements < FPQ_BLOCK_DIM * 3) continue;
        if (raw[i].rows <= 1) continue;

        fprintf(stderr, "\n[%zu/%zu] %s (%zu params, %zu×%zu)\n",
                i + 1, n_process, raw[i].name, raw[i].n_elements,
                raw[i].rows, raw[i].cols);

        float r = fpq_lie_probe(raw[i].data, raw[i].n_elements, raw[i].name);
        if (r > best_lie_r) {
            best_lie_r = r;
            best_name = raw[i].name;
        }
        n_tested++;
    }

    fprintf(stderr,
        "\n═══════════════════════════════════════════════════════\n"
        " LIE PROBE SUMMARY (%d tensors)\n"
        "═══════════════════════════════════════════════════════\n"
        "  Best Lie r:  %.4f  (%s)\n"
        "  Threshold:   0.5000  (for viable DPCM)\n"
        "  Status:      %s\n"
        "═══════════════════════════════════════════════════════\n",
        n_tested,
        best_lie_r, best_name,
        best_lie_r > 0.5f ? "BREAKTHROUGH — Lie DPCM is viable!" :
        best_lie_r > 0.3f ? "Promising — optimization may push past 0.5" :
        "Below threshold — need alternative approach");

    fpq_raw_tensor_free(raw, n_raw);
    return 0;
}


/* ── Command: roundtrip-v5  (PID + CHAOS + GHOST) ── */

static int cmd_roundtrip_v5(const char *input,
                             const char *tensor_filter, size_t limit,
                             int force_bits) {
    fprintf(stderr,
        "═══════════════════════════════════════════════════════\n"
        " FPQ v5 Roundtrip: PID + CHAOS + GHOST\n"
        " Model: %s\n"
        "═══════════════════════════════════════════════════════\n",
        input);

    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_read_model(input, &n_raw);
    if (!raw || n_raw == 0) return 1;

    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;

    float worst_cos_v3 = 1.0f, worst_cos_v4 = 1.0f, worst_cos_v5 = 1.0f;
    int n_tested = 0;

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && strcmp(raw[i].name, tensor_filter) != 0) continue;
        if (raw[i].n_elements < FPQ_BLOCK_DIM) continue;
        if (raw[i].rows <= 1) continue;

        fprintf(stderr, "\n[%zu/%zu] %s (%zu params, %zu×%zu)\n",
                i + 1, n_process, raw[i].name, raw[i].n_elements,
                raw[i].rows, raw[i].cols);

        int cbits = force_bits > 0 ? force_bits : 3;
        size_t n = raw[i].rows * raw[i].cols;

        /* ── v3 BASELINE ── */
        fpq_tensor_t *t_v3 = fpq_encode_tensor(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, 32, 0.01f);
        float *decoded_v3 = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor(t_v3, decoded_v3);
        float cos_v3 = fpq_cosine_sim(raw[i].data, decoded_v3, n);
        float bpw_v3 = fpq_bits_per_weight(t_v3);

        /* ── v4: CHAOS + GHOST (no SBB for fair comparison) ── */
        fpq_tensor_t *t_v4 = fpq_encode_tensor_v4(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits, NULL, -1);
        float *decoded_v4 = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor_v4(t_v4, decoded_v4);
        float cos_v4 = fpq_cosine_sim(raw[i].data, decoded_v4, n);
        float bpw_v4 = (float)t_v4->total_bits / (float)n;

        /* ── v5: PID + CHAOS + GHOST ── */
        fpq_tensor_t *t_v5 = fpq_encode_tensor_v5(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits, NULL, -1);
        float *decoded_v5 = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor_v5(t_v5, decoded_v5);
        float cos_v5 = fpq_cosine_sim(raw[i].data, decoded_v5, n);
        float bpw_v5 = (float)t_v5->total_bits / (float)n;

        if (cos_v3 < worst_cos_v3) worst_cos_v3 = cos_v3;
        if (cos_v4 < worst_cos_v4) worst_cos_v4 = cos_v4;
        if (cos_v5 < worst_cos_v5) worst_cos_v5 = cos_v5;

        fprintf(stderr,
            "  ╔═══ v3 vs v4 vs v5 COMPARISON ═══════════╗\n"
            "  ║ v3 COORD@auto+QJL:                      ║\n"
            "  ║   cos=%.6f    bpw=%.2f                 ║\n"
            "  ║ v4 CHAOS+GHOST@%d:                       ║\n"
            "  ║   cos=%.6f    bpw=%.2f                 ║\n"
            "  ║ v5 PID(α=%.3f)+CHAOS+GHOST@%d:          ║\n"
            "  ║   cos=%.6f    bpw=%.2f                 ║\n"
            "  ║ Δcos v5-v4: %+.6f                      ║\n"
            "  ║ Δcos v5-v3: %+.6f                      ║\n"
            "  ╚═════════════════════════════════════════╝\n",
            cos_v3, bpw_v3,
            cbits, cos_v4, bpw_v4,
            t_v5->pid_alpha, cbits, cos_v5, bpw_v5,
            cos_v5 - cos_v4,
            cos_v5 - cos_v3);

        n_tested++;

        free(decoded_v3);
        free(decoded_v4);
        free(decoded_v5);
        fpq_tensor_free(t_v3);
        fpq_tensor_free(t_v4);
        fpq_tensor_free(t_v5);
    }

    fprintf(stderr,
        "\n═══════════════════════════════════════════════════════\n"
        " v5 ROUNDTRIP SUMMARY (%d tensors)\n"
        "═══════════════════════════════════════════════════════\n"
        "  v3 worst cosine: %.6f\n"
        "  v4 worst cosine: %.6f\n"
        "  v5 worst cosine: %.6f\n"
        "  v5 vs v4 gain:   %+.6f\n"
        "  Verdict:         %s\n"
        "═══════════════════════════════════════════════════════\n",
        n_tested,
        worst_cos_v3, worst_cos_v4, worst_cos_v5,
        worst_cos_v5 - worst_cos_v4,
        worst_cos_v5 > worst_cos_v4 ? "v5 PID WINS" : "v4 baseline holds");

    fpq_raw_tensor_free(raw, n_raw);
    return 0;
}


/* ── Command: roundtrip-v6 (MASQ: Lie-Delta + Spectral Governor) ── */

static int cmd_roundtrip_v6(const char *input,
                             const char *tensor_filter, size_t limit,
                             int force_bits) {
    fprintf(stderr,
        "═══════════════════════════════════════════════════════\n"
        " FPQ v6 Roundtrip: MASQ (Lie-Delta + Spectral Governor)\n"
        " Model: %s\n"
        "═══════════════════════════════════════════════════════\n",
        input);

    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_read_model(input, &n_raw);
    if (!raw || n_raw == 0) return 1;

    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;

    float worst_cos_v4 = 1.0f, worst_cos_v6 = 1.0f;
    double sum_cos_v4 = 0.0, sum_cos_v6 = 0.0;
    int n_tested = 0;

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && strcmp(raw[i].name, tensor_filter) != 0) continue;
        if (raw[i].n_elements < FPQ_BLOCK_DIM * 2) continue;
        if (raw[i].rows <= 1) continue;

        fprintf(stderr, "\n[%zu/%zu] %s (%zu params, %zu×%zu)\n",
                i + 1, n_process, raw[i].name, raw[i].n_elements,
                raw[i].rows, raw[i].cols);

        int cbits = force_bits > 0 ? force_bits : 3;
        size_t n = raw[i].rows * raw[i].cols;

        /* ── v4 BASELINE: CHAOS + GHOST ── */
        fpq_tensor_t *t_v4 = fpq_encode_tensor_v4(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits, NULL, -1);
        float *decoded_v4 = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor_v4(t_v4, decoded_v4);
        float cos_v4 = fpq_cosine_sim(raw[i].data, decoded_v4, n);
        float bpw_v4 = (float)t_v4->total_bits / (float)n;

        /* ── v6: MASQ ── */
        fpq_tensor_t *t_v6 = fpq_encode_tensor_v6(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits);
        float *decoded_v6 = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor_v6(t_v6, decoded_v6);
        float cos_v6 = fpq_cosine_sim(raw[i].data, decoded_v6, n);
        float bpw_v6 = (float)t_v6->total_bits / (float)n;

        if (cos_v4 < worst_cos_v4) worst_cos_v4 = cos_v4;
        if (cos_v6 < worst_cos_v6) worst_cos_v6 = cos_v6;
        sum_cos_v4 += cos_v4;
        sum_cos_v6 += cos_v6;

        fprintf(stderr,
            "  ╔═══ v4 vs v6 MASQ COMPARISON ═══════════╗\n"
            "  ║ v4 CHAOS+GHOST@%d:                      ║\n"
            "  ║   cos=%.6f    bpw=%.2f                ║\n"
            "  ║ v6 MASQ@%d (Lie-Delta+Governor):        ║\n"
            "  ║   cos=%.6f    bpw=%.2f                ║\n"
            "  ║ Δcos v6-v4: %+.6f                     ║\n"
            "  ╚═════════════════════════════════════════╝\n",
            cbits, cos_v4, bpw_v4,
            cbits, cos_v6, bpw_v6,
            cos_v6 - cos_v4);

        n_tested++;

        free(decoded_v4);
        free(decoded_v6);
        fpq_tensor_free(t_v4);
        fpq_tensor_free(t_v6);
    }

    float avg_cos_v4 = n_tested > 0 ? (float)(sum_cos_v4 / n_tested) : 0.0f;
    float avg_cos_v6 = n_tested > 0 ? (float)(sum_cos_v6 / n_tested) : 0.0f;

    fprintf(stderr,
        "\n═══════════════════════════════════════════════════════\n"
        " v6 MASQ ROUNDTRIP SUMMARY (%d tensors)\n"
        "═══════════════════════════════════════════════════════\n"
        "  v4 worst cosine: %.6f  avg: %.6f\n"
        "  v6 worst cosine: %.6f  avg: %.6f\n"
        "  Δworst:          %+.6f\n"
        "  Δavg:            %+.6f\n"
        "  Verdict:         %s\n"
        "═══════════════════════════════════════════════════════\n",
        n_tested,
        worst_cos_v4, avg_cos_v4,
        worst_cos_v6, avg_cos_v6,
        worst_cos_v6 - worst_cos_v4,
        avg_cos_v6 - avg_cos_v4,
        avg_cos_v6 > avg_cos_v4 ? "MASQ WINS — Manifold Transport beats Euclidean" :
                                   "v4 baseline holds — tune governor parameters");

    fpq_raw_tensor_free(raw, n_raw);
    return 0;
}


/* ── Command: roundtrip-v7 (Holographic Lattice) ── */

static int cmd_roundtrip_v7(const char *input,
                             const char *tensor_filter, size_t limit,
                             int force_bits) {
    fprintf(stderr,
        "═══════════════════════════════════════════════════════\n"
        " FPQ v7 Roundtrip: Holographic Lattice (E8+Warp+RVQ+Trellis)\n"
        " Model: %s\n"
        "═══════════════════════════════════════════════════════\n",
        input);

    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_read_model(input, &n_raw);
    if (!raw || n_raw == 0) return 1;

    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;

    float worst_cos_v4 = 1.0f, worst_cos_v7 = 1.0f;
    double sum_cos_v4 = 0.0, sum_cos_v7 = 0.0;
    float worst_cos_v6 = 1.0f;
    double sum_cos_v6 = 0.0;
    int n_tested = 0;

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && strcmp(raw[i].name, tensor_filter) != 0) continue;
        if (raw[i].n_elements < FPQ_BLOCK_DIM * 2) continue;
        if (raw[i].rows <= 1) continue;

        fprintf(stderr, "\n[%zu/%zu] %s (%zu params, %zu×%zu)\n",
                i + 1, n_process, raw[i].name, raw[i].n_elements,
                raw[i].rows, raw[i].cols);

        int cbits = force_bits > 0 ? force_bits : 3;
        size_t n = raw[i].rows * raw[i].cols;

        /* ── v4 baseline ── */
        fpq_tensor_t *t_v4 = fpq_encode_tensor_v4(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits, NULL, -1);
        float *decoded_v4 = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor_v4(t_v4, decoded_v4);
        float cos_v4 = fpq_cosine_sim(raw[i].data, decoded_v4, n);
        float bpw_v4 = (float)t_v4->total_bits / (float)n;

        /* ── v6 two-pass ── */
        fpq_tensor_t *t_v6 = fpq_encode_tensor_v6(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits);
        float *decoded_v6 = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor_v6(t_v6, decoded_v6);
        float cos_v6 = fpq_cosine_sim(raw[i].data, decoded_v6, n);
        float bpw_v6 = (float)t_v6->total_bits / (float)n;

        /* ── v7 holographic lattice ── */
        fpq_tensor_t *t_v7 = fpq_encode_tensor_v7(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits);
        float *decoded_v7 = (float *)malloc(n * sizeof(float));
        fpq_decode_tensor_v7(t_v7, decoded_v7);
        float cos_v7 = fpq_cosine_sim(raw[i].data, decoded_v7, n);
        float bpw_v7 = (float)t_v7->total_bits / (float)n;

        if (cos_v4 < worst_cos_v4) worst_cos_v4 = cos_v4;
        if (cos_v6 < worst_cos_v6) worst_cos_v6 = cos_v6;
        if (cos_v7 < worst_cos_v7) worst_cos_v7 = cos_v7;
        sum_cos_v4 += cos_v4;
        sum_cos_v6 += cos_v6;
        sum_cos_v7 += cos_v7;

        fprintf(stderr,
            "  ╔═══ v4 vs v6 vs v7 COMPARISON ════════════════╗\n"
            "  ║ v4 CHAOS+GHOST@%d:                             ║\n"
            "  ║   cos=%.6f    bpw=%.2f                       ║\n"
            "  ║ v6 Two-Pass@%d:                                ║\n"
            "  ║   cos=%.6f    bpw=%.2f                       ║\n"
            "  ║ v7 Holographic@%d:                             ║\n"
            "  ║   cos=%.6f    bpw=%.2f                       ║\n"
            "  ║ Δcos v7-v4: %+.6f                            ║\n"
            "  ║ Δcos v7-v6: %+.6f                            ║\n"
            "  ╚═══════════════════════════════════════════════╝\n",
            cbits, cos_v4, bpw_v4,
            cbits, cos_v6, bpw_v6,
            cbits, cos_v7, bpw_v7,
            cos_v7 - cos_v4,
            cos_v7 - cos_v6);

        n_tested++;

        free(decoded_v4);
        free(decoded_v6);
        free(decoded_v7);
        fpq_tensor_free(t_v4);
        fpq_tensor_free(t_v6);
        fpq_tensor_free(t_v7);
    }

    float avg_cos_v4 = n_tested > 0 ? (float)(sum_cos_v4 / n_tested) : 0.0f;
    float avg_cos_v6 = n_tested > 0 ? (float)(sum_cos_v6 / n_tested) : 0.0f;
    float avg_cos_v7 = n_tested > 0 ? (float)(sum_cos_v7 / n_tested) : 0.0f;

    fprintf(stderr,
        "\n═══════════════════════════════════════════════════════\n"
        " v7 HOLOGRAPHIC LATTICE SUMMARY (%d tensors)\n"
        "═══════════════════════════════════════════════════════\n"
        "  v4 worst: %.6f  avg: %.6f\n"
        "  v6 worst: %.6f  avg: %.6f\n"
        "  v7 worst: %.6f  avg: %.6f\n"
        "  Δworst v7-v4: %+.6f\n"
        "  Δavg   v7-v4: %+.6f\n"
        "  Δavg   v7-v6: %+.6f\n"
        "  Verdict: %s\n"
        "═══════════════════════════════════════════════════════\n",
        n_tested,
        worst_cos_v4, avg_cos_v4,
        worst_cos_v6, avg_cos_v6,
        worst_cos_v7, avg_cos_v7,
        worst_cos_v7 - worst_cos_v4,
        avg_cos_v7 - avg_cos_v4,
        avg_cos_v7 - avg_cos_v6,
        avg_cos_v7 > avg_cos_v6 ? "v7 WINS — Holographic Lattice is the new champion" :
        avg_cos_v7 > avg_cos_v4 ? "v7 beats v4 but not v6" :
                                   "v4 baseline holds");

    fpq_raw_tensor_free(raw, n_raw);
    return 0;
}


/* ── Command: roundtrip-v8 (Recursive Lattice-Flow) ── */

static int cmd_roundtrip_v8(const char *input,
                             const char *tensor_filter, size_t limit,
                             int force_bits) {
    fprintf(stderr,
        "═══════════════════════════════════════════════════════\n"
        " FPQ v8 Roundtrip: Recursive Lattice-Flow (TCLQ+16D-RVQ)\n"
        " Model: %s\n"
        "═══════════════════════════════════════════════════════\n",
        input);

    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_read_model(input, &n_raw);
    if (!raw || n_raw == 0) return 1;

    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;

    float worst_cos_v8 = 1.0f;
    double sum_cos_v8 = 0.0;
    int n_tested = 0;

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && strcmp(raw[i].name, tensor_filter) != 0) continue;
        if (raw[i].n_elements < FPQ_BLOCK_DIM * 2) continue;
        if (raw[i].rows <= 1) continue;
        size_t n = raw[i].rows * raw[i].cols;
        if (n < FPQ_BLOCK_DIM * 2) continue;  /* skip 3D+ tensors with tiny 2D shape */

        fprintf(stderr, "\n[%zu/%zu] %s (%zu params, %zu×%zu)\n",
                i + 1, n_process, raw[i].name, raw[i].n_elements,
                raw[i].rows, raw[i].cols);

        int cbits = force_bits > 0 ? force_bits : 3;

        /* ── v8 recursive lattice-flow ── */
        fpq_tensor_t *t_v8 = fpq_encode_tensor_v8(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits);
        float *decoded_v8 = (float *)malloc(n * sizeof(float));
        /* Use correct decoder: v8 uses sbb_scale_delta, v4 fallback uses coord_quants */
        if (t_v8->sbb_scale_delta)
            fpq_decode_tensor_v8(t_v8, decoded_v8);
        else
            fpq_decode_tensor_v4(t_v8, decoded_v8);
        float cos_v8 = fpq_cosine_sim(raw[i].data, decoded_v8, n);
        float bpw_v8 = (float)t_v8->total_bits / (float)n;

        if (cos_v8 < worst_cos_v8) worst_cos_v8 = cos_v8;
        sum_cos_v8 += cos_v8;

        fprintf(stderr,
            "  v8 RLF@%d: cos=%.6f  bpw=%.2f\n",
            cbits, cos_v8, bpw_v8);

        n_tested++;

        free(decoded_v8);
        fpq_tensor_free(t_v8);
    }

    float avg_cos_v8 = n_tested > 0 ? (float)(sum_cos_v8 / n_tested) : 0.0f;

    fprintf(stderr,
        "\n═══════════════════════════════════════════════════════\n"
        " v8 RECURSIVE LATTICE-FLOW SUMMARY (%d tensors)\n"
        "═══════════════════════════════════════════════════════\n"
        "  v8 worst: %.6f  avg: %.6f\n"
        "═══════════════════════════════════════════════════════\n",
        n_tested,
        worst_cos_v8, avg_cos_v8);

    fpq_raw_tensor_free(raw, n_raw);
    return 0;
}

/* ── write-v9: roundtrip + write reconstructed safetensors ── */
static int cmd_write_v9(const char *input, const char *output,
                        int force_bits) {
    fprintf(stderr,
        "═══════════════════════════════════════════════════════\n"
        " FPQ v9 Write: Roundtrip → BF16 safetensors\n"
        " Input:  %s\n"
        " Output: %s\n"
        "═══════════════════════════════════════════════════════\n",
        input, output);

    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_read_model(input, &n_raw);
    if (!raw || n_raw == 0) return 1;

    int cbits = force_bits > 0 ? force_bits : 3;
    int n_encoded = 0, n_passthrough = 0;
    double sum_cos = 0.0;
    float worst_cos = 1.0f;

    for (size_t i = 0; i < n_raw; i++) {
        size_t n = raw[i].rows * raw[i].cols;
        int skip = (raw[i].n_elements < FPQ_BLOCK_DIM * 2) ||
                   (raw[i].rows <= 1) ||
                   (n < FPQ_BLOCK_DIM * 2);

        if (skip) {
            /* Pass through unchanged (biases, 1D, small tensors) */
            n_passthrough++;
            continue;
        }

        fprintf(stderr, "  [%zu/%zu] %s (%zu×%zu) ... ",
                i + 1, n_raw, raw[i].name, raw[i].rows, raw[i].cols);

        /* Encode + decode through v9 */
        fpq_tensor_t *t = fpq_encode_tensor_v9(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits);

        float *decoded = (float *)malloc(n * sizeof(float));
        if (t->pid_alpha == -9.0f)
            fpq_decode_tensor_v9(t, decoded);
        else if (t->sbb_scale_delta)
            fpq_decode_tensor_v8(t, decoded);
        else
            fpq_decode_tensor_v4(t, decoded);

        float cos = fpq_cosine_sim(raw[i].data, decoded, n);
        if (cos < worst_cos) worst_cos = cos;
        sum_cos += cos;
        n_encoded++;

        /* Replace original data with reconstructed */
        memcpy(raw[i].data, decoded, n * sizeof(float));

        fprintf(stderr, "cos=%.6f\n", cos);

        free(decoded);
        fpq_tensor_free(t);
    }

    fprintf(stderr,
        "\nEncoded: %d tensors (avg cos=%.6f, worst=%.6f)\n"
        "Passthrough: %d tensors (unchanged)\n",
        n_encoded, n_encoded > 0 ? (float)(sum_cos / n_encoded) : 0.0f,
        worst_cos, n_passthrough);

    /* Write the modified model */
    int rc = fpq_safetensors_write(output, raw, n_raw);

    fpq_raw_tensor_free(raw, n_raw);
    return rc;
}

static int cmd_roundtrip_v9(const char *input,
                             const char *tensor_filter, size_t limit,
                             int force_bits) {
    fprintf(stderr,
        "═══════════════════════════════════════════════════════\n"
        " FPQ v9 Roundtrip: Unified Multiscale (LR+E8+RVQ+QJL+Ghost)\n"
        " Model: %s\n"
        "═══════════════════════════════════════════════════════\n",
        input);

    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_read_model(input, &n_raw);
    if (!raw || n_raw == 0) return 1;

    size_t n_process = (limit > 0 && limit < n_raw) ? limit : n_raw;

    float worst_cos_v9 = 1.0f, worst_cos_v8 = 1.0f;
    double sum_cos_v9 = 0.0, sum_cos_v8 = 0.0;
    int n_tested = 0;

    for (size_t i = 0; i < n_process; i++) {
        if (tensor_filter && strcmp(raw[i].name, tensor_filter) != 0) continue;
        if (raw[i].n_elements < FPQ_BLOCK_DIM * 2) continue;
        if (raw[i].rows <= 1) continue;
        size_t n = raw[i].rows * raw[i].cols;
        if (n < FPQ_BLOCK_DIM * 2) continue;  /* skip 3D+ tensors with tiny 2D shape */

        fprintf(stderr, "\n[%zu/%zu] %s (%zu params, %zu×%zu)\n",
                i + 1, n_process, raw[i].name, raw[i].n_elements,
                raw[i].rows, raw[i].cols);

        int cbits = force_bits > 0 ? force_bits : 3;

        /* v8 baseline for comparison */
        fpq_tensor_t *t_v8 = fpq_encode_tensor_v8(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits);
        float *decoded_v8 = (float *)malloc(n * sizeof(float));
        if (t_v8->sbb_scale_delta)
            fpq_decode_tensor_v8(t_v8, decoded_v8);
        else
            fpq_decode_tensor_v4(t_v8, decoded_v8);
        float cos_v8 = fpq_cosine_sim(raw[i].data, decoded_v8, n);
        float bpw_v8 = (float)t_v8->total_bits / (float)n;
        free(decoded_v8);
        fpq_tensor_free(t_v8);

        /* v9 multiscale */
        fpq_tensor_t *t_v9 = fpq_encode_tensor_v9(
            raw[i].data, raw[i].rows, raw[i].cols,
            raw[i].name, cbits);
        float *decoded_v9 = (float *)malloc(n * sizeof(float));
        if (t_v9->pid_alpha == -9.0f)
            fpq_decode_tensor_v9(t_v9, decoded_v9);
        else if (t_v9->sbb_scale_delta)
            fpq_decode_tensor_v8(t_v9, decoded_v9);
        else
            fpq_decode_tensor_v4(t_v9, decoded_v9);
        float cos_v9 = fpq_cosine_sim(raw[i].data, decoded_v9, n);
        float bpw_v9 = (float)t_v9->total_bits / (float)n;
        free(decoded_v9);
        fpq_tensor_free(t_v9);

        if (cos_v8 < worst_cos_v8) worst_cos_v8 = cos_v8;
        if (cos_v9 < worst_cos_v9) worst_cos_v9 = cos_v9;
        sum_cos_v8 += cos_v8;
        sum_cos_v9 += cos_v9;

        const char *winner = cos_v9 >= cos_v8 ? "v9 ✓" : "v8 ✓";
        fprintf(stderr,
            "  v8: cos=%.6f bpw=%.2f | v9: cos=%.6f bpw=%.2f  [%s]\n",
            cos_v8, bpw_v8, cos_v9, bpw_v9, winner);

        n_tested++;
    }

    float avg_v8 = n_tested > 0 ? (float)(sum_cos_v8 / n_tested) : 0.0f;
    float avg_v9 = n_tested > 0 ? (float)(sum_cos_v9 / n_tested) : 0.0f;

    fprintf(stderr,
        "\n═══════════════════════════════════════════════════════\n"
        " v9 UNIFIED MULTISCALE SUMMARY (%d tensors)\n"
        "═══════════════════════════════════════════════════════\n"
        "  v8 worst: %.6f  avg: %.6f\n"
        "  v9 worst: %.6f  avg: %.6f\n"
        "═══════════════════════════════════════════════════════\n",
        n_tested,
        worst_cos_v8, avg_v8,
        worst_cos_v9, avg_v9);

    fpq_raw_tensor_free(raw, n_raw);
    return 0;
}

static int cmd_v9_analyze(const char *input,
                          const char *tensor_filter, int force_bits) {
    fprintf(stderr,
        "═══════════════════════════════════════════════════════\n"
        " FPQ v9 Deep Analysis\n"
        " Model: %s\n"
        "═══════════════════════════════════════════════════════\n",
        input);

    size_t n_raw;
    fpq_raw_tensor_t *raw = fpq_read_model(input, &n_raw);
    if (!raw || n_raw == 0) return 1;

    int cbits = force_bits > 0 ? force_bits : 3;
    int found = 0;

    for (size_t i = 0; i < n_raw; i++) {
        if (tensor_filter && strcmp(raw[i].name, tensor_filter) != 0) continue;
        if (raw[i].n_elements < FPQ_BLOCK_DIM * 2) continue;
        if (raw[i].rows <= 1) continue;

        fpq_v9_analyze(raw[i].data, raw[i].rows, raw[i].cols,
                       raw[i].name, cbits);
        found++;

        /* Analyze only the first matching 2D tensor unless --tensor filters */
        if (!tensor_filter) break;
    }

    if (found == 0)
        fprintf(stderr, "No eligible tensors found.\n");

    fpq_raw_tensor_free(raw, n_raw);
    return 0;
}

/* ── main ── */

int main(int argc, char **argv) {
    /* Ensure real-time output when redirected to file */
    setvbuf(stderr, NULL, _IONBF, 0);
    setvbuf(stdout, NULL, _IOLBF, 0);

    if (argc < 2) {
        usage();
        return 1;
    }

    const char *cmd = argv[1];

    /* Parse options */
    size_t max_nodes = 32;
    float tolerance = 0.01f;
    int report = 0;
    const char *tensor_filter = NULL;
    size_t limit = 0;
    int force_bits = 0;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--max-nodes") == 0 && i + 1 < argc) {
            max_nodes = (size_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--tolerance") == 0 && i + 1 < argc) {
            tolerance = (float)atof(argv[++i]);
        } else if (strcmp(argv[i], "--report") == 0) {
            report = 1;
        } else if (strcmp(argv[i], "--tensor") == 0 && i + 1 < argc) {
            tensor_filter = argv[++i];
        } else if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc) {
            limit = (size_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--bits") == 0 && i + 1 < argc) {
            force_bits = atoi(argv[++i]);
        }
    }

    if (strcmp(cmd, "compress") == 0) {
        if (argc < 4) { usage(); return 1; }
        return cmd_compress(argv[2], argv[3], max_nodes, tolerance,
                            report, tensor_filter, limit);
    } else if (strcmp(cmd, "decompress") == 0) {
        if (argc < 4) { usage(); return 1; }
        return cmd_decompress(argv[2], argv[3]);
    } else if (strcmp(cmd, "inspect") == 0) {
        if (argc < 3) { usage(); return 1; }
        return cmd_inspect(argv[2]);
    } else if (strcmp(cmd, "roundtrip") == 0) {
        if (argc < 3) { usage(); return 1; }
        return cmd_roundtrip(argv[2], max_nodes, tolerance,
                             tensor_filter, limit);
    } else if (strcmp(cmd, "roundtrip-v4") == 0) {
        if (argc < 3) { usage(); return 1; }
        return cmd_roundtrip_v4(argv[2], tensor_filter, limit, force_bits);
    } else if (strcmp(cmd, "roundtrip-v5") == 0) {
        if (argc < 3) { usage(); return 1; }
        return cmd_roundtrip_v5(argv[2], tensor_filter, limit, force_bits);
    } else if (strcmp(cmd, "lie-probe") == 0) {
        if (argc < 3) { usage(); return 1; }
        return cmd_lie_probe(argv[2], tensor_filter, limit);
    } else if (strcmp(cmd, "roundtrip-v6") == 0) {
        if (argc < 3) { usage(); return 1; }
        return cmd_roundtrip_v6(argv[2], tensor_filter, limit, force_bits);
    } else if (strcmp(cmd, "roundtrip-v7") == 0) {
        if (argc < 3) { usage(); return 1; }
        return cmd_roundtrip_v7(argv[2], tensor_filter, limit, force_bits);
    } else if (strcmp(cmd, "roundtrip-v8") == 0) {
        if (argc < 3) { usage(); return 1; }
        return cmd_roundtrip_v8(argv[2], tensor_filter, limit, force_bits);
    } else if (strcmp(cmd, "roundtrip-v9") == 0) {
        if (argc < 3) { usage(); return 1; }
        return cmd_roundtrip_v9(argv[2], tensor_filter, limit, force_bits);
    } else if (strcmp(cmd, "v9-analyze") == 0) {
        if (argc < 3) { usage(); return 1; }
        return cmd_v9_analyze(argv[2], tensor_filter, force_bits);
    } else if (strcmp(cmd, "write-v9") == 0) {
        if (argc < 4) {
            fprintf(stderr, "Usage: bonfyre-fpq write-v9 <input.safetensors> <output.safetensors> [--bits N]\n");
            return 1;
        }
        return cmd_write_v9(argv[2], argv[3], force_bits);
    } else if (strcmp(cmd, "quantize") == 0) {
        if (argc < 4) {
            fprintf(stderr,
                "Usage: bonfyre-fpq quantize <input> <output> [--bits N]\n"
                "\n"
                "  Compress a model with FPQ v9 and write a usable output file.\n"
                "  Input:  GGUF (.gguf) or safetensors (.safetensors)\n"
                "  Output: GGUF F16 (for llama.cpp) or BF16 safetensors\n"
                "\n"
                "  Format is auto-detected from file extensions.\n"
                "  GGUF input  → GGUF F16 output (preserves all metadata)\n"
                "  Safetensors → BF16 safetensors output\n"
            );
            return 1;
        }
        const char *in = argv[2];
        const char *out = argv[3];
        size_t ilen = strlen(in);
        /* Auto-detect: safetensors input → safetensors path */
        if (ilen > 12 && strcmp(in + ilen - 12, ".safetensors") == 0) {
            return cmd_write_v9(in, out, force_bits);
        }
        /* Default: GGUF path */
        return fpq_gguf_write_v9(in, out, force_bits);
    } else if (strcmp(cmd, "algebra-analyze") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: bonfyre-fpq algebra-analyze <model>\n");
            return 1;
        }
        bwa_analyze_model(argv[2]);
        return 0;
    } else if (strcmp(cmd, "algebra-prune") == 0) {
        if (argc < 4) {
            fprintf(stderr,
                "Usage: bonfyre-fpq algebra-prune <input> <output> [--keep-ratio F] [--mode raw|rank|hybrid]\n");
            return 1;
        }
        float keep_ratio = 0.75f;
        int prune_mode = BWA_PRUNE_HYBRID;
        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--keep-ratio") == 0 && i + 1 < argc)
                keep_ratio = (float)atof(argv[++i]);
            else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
                i++;
                if (strcmp(argv[i], "raw") == 0) prune_mode = BWA_PRUNE_RAW;
                else if (strcmp(argv[i], "rank") == 0) prune_mode = BWA_PRUNE_RANK;
                else prune_mode = BWA_PRUNE_HYBRID;
            }
        }
        size_t n_raw;
        fpq_raw_tensor_t *raw = fpq_read_model(argv[2], &n_raw);
        if (!raw || n_raw == 0) return 1;
        fprintf(stderr, "BWA Prune: %s → %s (keep=%.0f%%, mode=%d)\n",
                argv[2], argv[3], keep_ratio * 100, prune_mode);
        for (size_t i = 0; i < n_raw; i++) {
            if (raw[i].rows <= 1 || raw[i].cols <= 1 ||
                raw[i].rows * raw[i].cols < 512) continue;
            float *pruned = (float *)malloc(raw[i].rows * raw[i].cols * sizeof(float));
            bwa_prune(raw[i].data, raw[i].rows, raw[i].cols, raw[i].name,
                      keep_ratio, 0.25f, (bwa_prune_mode_t)prune_mode, pruned);
            float cos = fpq_cosine_sim(raw[i].data, pruned,
                                        raw[i].rows * raw[i].cols);
            fprintf(stderr, "  %-50s cos=%.6f\n", raw[i].name, cos);
            memcpy(raw[i].data, pruned, raw[i].rows * raw[i].cols * sizeof(float));
            free(pruned);
        }
        int rc = fpq_safetensors_write(argv[3], raw, n_raw);
        fpq_raw_tensor_free(raw, n_raw);
        return rc;
    } else if (strcmp(cmd, "algebra-merge") == 0) {
        if (argc < 5) {
            fprintf(stderr,
                "Usage: bonfyre-fpq algebra-merge <model-a> <model-b> <output> [--alpha F]\n");
            return 1;
        }
        float alpha = 0.5f;
        for (int i = 5; i < argc; i++)
            if (strcmp(argv[i], "--alpha") == 0 && i + 1 < argc)
                alpha = (float)atof(argv[++i]);
        size_t n_a, n_b;
        fpq_raw_tensor_t *raw_a = fpq_read_model(argv[2], &n_a);
        fpq_raw_tensor_t *raw_b = fpq_read_model(argv[3], &n_b);
        if (!raw_a || !raw_b) return 1;
        size_t n_out = n_a < n_b ? n_a : n_b;
        fprintf(stderr, "BWA Merge: %s + %s → %s (α=%.2f)\n",
                argv[2], argv[3], argv[4], alpha);
        for (size_t i = 0; i < n_out; i++) {
            if (raw_a[i].rows != raw_b[i].rows || raw_a[i].cols != raw_b[i].cols)
                continue;
            size_t total = raw_a[i].rows * raw_a[i].cols;
            if (raw_a[i].rows <= 1 || total < 512) {
                for (size_t j = 0; j < total; j++)
                    raw_a[i].data[j] = alpha * raw_a[i].data[j] +
                                       (1.0f - alpha) * raw_b[i].data[j];
                continue;
            }
            float *merged = (float *)malloc(total * sizeof(float));
            bwa_merge(raw_a[i].data, raw_b[i].data,
                      raw_a[i].rows, raw_a[i].cols, raw_a[i].name,
                      alpha, 0.25f, merged);
            float cos_raw = 0.0f, cos_struct = 0.0f;
            {
                float *raw_m = (float *)malloc(total * sizeof(float));
                for (size_t j = 0; j < total; j++)
                    raw_m[j] = alpha * raw_a[i].data[j] +
                               (1.0f - alpha) * raw_b[i].data[j];
                cos_raw = fpq_cosine_sim(raw_a[i].data, raw_m, total);
                cos_struct = fpq_cosine_sim(raw_a[i].data, merged, total);
                free(raw_m);
            }
            fprintf(stderr, "  %-50s raw_cos=%.6f struct_cos=%.6f\n",
                    raw_a[i].name, cos_raw, cos_struct);
            memcpy(raw_a[i].data, merged, total * sizeof(float));
            free(merged);
        }
        int rc = fpq_safetensors_write(argv[4], raw_a, n_out);
        fpq_raw_tensor_free(raw_a, n_a);
        fpq_raw_tensor_free(raw_b, n_b);
        return rc;
    } else if (strcmp(cmd, "algebra-compress") == 0) {
        if (argc < 4) {
            fprintf(stderr,
                "Usage: bonfyre-fpq algebra-compress <input> <output>\n"
                "       [--bits N] [--keep-ratio F] [--rank-ratio F]\n"
                "\n"
                "  Combined pipeline: decompose W=L+R → hybrid prune R →\n"
                "  curl+div correct → FPQ v9 encode → write safetensors\n"
                "\n"
                "  L stored as low-rank factors (~0.08 bpw)\n"
                "  Pruned R encoded at FPQ@N (~1.5 bpw at 3-bit)\n"
                "  Total ≈ 1.6 bpw = 20× from fp32\n");
            return 1;
        }
        float keep_ratio = 0.50f;
        float rank_ratio = 0.25f;
        int cbits = force_bits > 0 ? force_bits : 3;
        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--keep-ratio") == 0 && i + 1 < argc)
                keep_ratio = (float)atof(argv[++i]);
            else if (strcmp(argv[i], "--rank-ratio") == 0 && i + 1 < argc)
                rank_ratio = (float)atof(argv[++i]);
        }
        size_t n_raw;
        fpq_raw_tensor_t *raw = fpq_read_model(argv[2], &n_raw);
        if (!raw || n_raw == 0) return 1;

        fprintf(stderr,
            "═══════════════════════════════════════════════════════\n"
            " BWA Algebra-Compress (W=L+R → prune → FPQ v9)\n"
            " Input:      %s\n"
            " Output:     %s\n"
            " Bits:       %d\n"
            " Keep ratio: %.0f%%\n"
            " Rank ratio: %.0f%%\n"
            " Tensors:    %zu\n"
            "═══════════════════════════════════════════════════════\n",
            argv[2], argv[3], cbits, keep_ratio * 100, rank_ratio * 100, n_raw);

        /* Phase 1: Count eligible tensors */
        int n_eligible = 0;
        for (size_t i = 0; i < n_raw; i++) {
            if (raw[i].rows <= 1 || raw[i].cols <= 1 ||
                raw[i].rows * raw[i].cols < 512) continue;
            bwa_tensor_type_t tt = bwa_classify_tensor(raw[i].name);
            if (tt == BWA_TENSOR_NORM) continue;
            n_eligible++;
        }
        fprintf(stderr, "  Phase 1: %d eligible 2D tensors\n", n_eligible);

        /* Phase 2: Decompose + prune + FPQ encode */
        int n_algebra = 0, n_fpq_only = 0, n_pass = 0;
        double sum_cos_pre = 0.0, sum_cos_post = 0.0;
        float worst_cos = 1.0f;

        for (size_t i = 0; i < n_raw; i++) {
            size_t n = raw[i].rows * raw[i].cols;
            int skip = (n < FPQ_BLOCK_DIM * 2) ||
                       (raw[i].rows <= 1) ||
                       (raw[i].cols <= 1);

            if (skip) {
                n_pass++;
                continue;
            }

            bwa_tensor_type_t tt = bwa_classify_tensor(raw[i].name);

            if (tt != BWA_TENSOR_NORM && n >= 512 && raw[i].rows > 1 && raw[i].cols > 1) {
                /* Full algebra path: decompose → prune → correct → FPQ */
                float *pruned = (float *)malloc(n * sizeof(float));
                bwa_prune(raw[i].data, raw[i].rows, raw[i].cols,
                          raw[i].name, keep_ratio, rank_ratio,
                          BWA_PRUNE_HYBRID, pruned);

                float cos_prune = fpq_cosine_sim(raw[i].data, pruned, n);

                /* FPQ v9 encode the algebra-compressed tensor */
                fpq_tensor_t *t = fpq_encode_tensor_v9(
                    pruned, raw[i].rows, raw[i].cols,
                    raw[i].name, cbits);

                float *decoded = (float *)malloc(n * sizeof(float));
                if (t->pid_alpha == -9.0f)
                    fpq_decode_tensor_v9(t, decoded);
                else if (t->sbb_scale_delta)
                    fpq_decode_tensor_v8(t, decoded);
                else
                    fpq_decode_tensor_v4(t, decoded);

                float cos_final = fpq_cosine_sim(raw[i].data, decoded, n);
                if (cos_final < worst_cos) worst_cos = cos_final;
                sum_cos_pre += cos_prune;
                sum_cos_post += cos_final;

                memcpy(raw[i].data, decoded, n * sizeof(float));

                if (n_algebra < 20 || (n_algebra % 50 == 0))
                    fprintf(stderr, "  [%zu] %-45s prune=%.6f final=%.6f  (%s)\n",
                            i, raw[i].name, cos_prune, cos_final,
                            tt == BWA_TENSOR_FFN ? "FFN" :
                            tt == BWA_TENSOR_SELF_ATTN ? "attn" :
                            tt == BWA_TENSOR_CROSS_ATTN ? "xattn" : "embed");

                n_algebra++;
                free(pruned);
                free(decoded);
                fpq_tensor_free(t);
            } else {
                /* FPQ-only path (norm, small) */
                fpq_tensor_t *t = fpq_encode_tensor_v9(
                    raw[i].data, raw[i].rows, raw[i].cols,
                    raw[i].name, cbits);
                float *decoded = (float *)malloc(n * sizeof(float));
                if (t->pid_alpha == -9.0f)
                    fpq_decode_tensor_v9(t, decoded);
                else if (t->sbb_scale_delta)
                    fpq_decode_tensor_v8(t, decoded);
                else
                    fpq_decode_tensor_v4(t, decoded);

                float cos = fpq_cosine_sim(raw[i].data, decoded, n);
                if (cos < worst_cos) worst_cos = cos;
                sum_cos_post += cos;

                memcpy(raw[i].data, decoded, n * sizeof(float));
                n_fpq_only++;
                free(decoded);
                fpq_tensor_free(t);
            }
        }

        int n_total_enc = n_algebra + n_fpq_only;
        fprintf(stderr,
            "\n═══════════════════════════════════════════════════════\n"
            " ALGEBRA-COMPRESS SUMMARY\n"
            "═══════════════════════════════════════════════════════\n"
            "  Algebra path:  %d tensors (decompose+prune+FPQ)\n"
            "  FPQ-only path: %d tensors\n"
            "  Passthrough:   %d tensors\n"
            "  Avg prune cos: %.6f\n"
            "  Avg final cos: %.6f\n"
            "  Worst cos:     %.6f\n"
            "═══════════════════════════════════════════════════════\n",
            n_algebra, n_fpq_only, n_pass,
            n_algebra > 0 ? (float)(sum_cos_pre / n_algebra) : 0.0f,
            n_total_enc > 0 ? (float)(sum_cos_post / n_total_enc) : 0.0f,
            worst_cos);

        int rc = fpq_safetensors_write(argv[3], raw, n_raw);
        fpq_raw_tensor_free(raw, n_raw);
        return rc;
    } else if (strcmp(cmd, "algebra-edit") == 0) {
        if (argc < 5) {
            fprintf(stderr,
                "Usage: bonfyre-fpq algebra-edit <model> <delta> <output> "
                "[--scale F] [--mode raw|lr|residual|selective]\n");
            return 1;
        }
        float scale = 0.1f;
        int edit_mode = BWA_EDIT_LR_ONLY;
        for (int i = 5; i < argc; i++) {
            if (strcmp(argv[i], "--scale") == 0 && i + 1 < argc)
                scale = (float)atof(argv[++i]);
            else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
                i++;
                if (strcmp(argv[i], "raw") == 0) edit_mode = BWA_EDIT_RAW;
                else if (strcmp(argv[i], "lr") == 0) edit_mode = BWA_EDIT_LR_ONLY;
                else if (strcmp(argv[i], "residual") == 0) edit_mode = BWA_EDIT_RESIDUAL_ONLY;
                else edit_mode = BWA_EDIT_SELECTIVE;
            }
        }
        size_t n_m, n_d;
        fpq_raw_tensor_t *raw_m = fpq_read_model(argv[2], &n_m);
        fpq_raw_tensor_t *raw_d = fpq_read_model(argv[3], &n_d);
        if (!raw_m || !raw_d) return 1;
        size_t n_out = n_m < n_d ? n_m : n_d;
        fprintf(stderr, "BWA Edit: %s + %s → %s (scale=%.2f, mode=%d)\n",
                argv[2], argv[3], argv[4], scale, edit_mode);
        for (size_t i = 0; i < n_out; i++) {
            if (raw_m[i].rows != raw_d[i].rows || raw_m[i].cols != raw_d[i].cols)
                continue;
            size_t total = raw_m[i].rows * raw_m[i].cols;
            float *edited = (float *)malloc(total * sizeof(float));
            bwa_edit(raw_m[i].data, raw_d[i].data,
                     raw_m[i].rows, raw_m[i].cols, raw_m[i].name,
                     scale, (bwa_edit_mode_t)edit_mode, 1.0f, 0.1f, 0.25f, edited);
            float cos = fpq_cosine_sim(raw_m[i].data, edited, total);
            if (total >= 512 && raw_m[i].rows > 1)
                fprintf(stderr, "  %-50s cos=%.6f\n", raw_m[i].name, cos);
            memcpy(raw_m[i].data, edited, total * sizeof(float));
            free(edited);
        }
        int rc = fpq_safetensors_write(argv[4], raw_m, n_out);
        fpq_raw_tensor_free(raw_m, n_m);
        fpq_raw_tensor_free(raw_d, n_d);
        return rc;
    } else {
        fprintf(stderr, "Unknown command: %s\n", cmd);
        usage();
        return 1;
    }
}
