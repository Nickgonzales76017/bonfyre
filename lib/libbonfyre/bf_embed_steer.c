/*
 * bf_embed_steer.c — Steering vectors (embedding branches)
 *
 * A steering vector is a named direction in embedding space. Applying
 * one to an existing embedding shifts it toward a concept without
 * re-running inference.
 *
 * The "conceptual branch" from the design doc:
 *
 *   bonfyre embed branch add  tech      <tech_delta.vecf>
 *   bonfyre embed branch add  agri      <agri_delta.vecf>
 *   bonfyre embed branch delta <base.vecf> <target.vecf> medical
 *   bonfyre embed branch apply <embed.vecf> --branch tech:0.5 --branch agri:0.3 --out <out.vecf>
 *
 * The merge is: v_out[i] = v_base[i] + Σ alpha_k · (q_k[i] · scale_k)
 * This is a linear combination of concept branches — a "neural merge."
 *
 * File format (.bfsteer):
 *   [0-3]   uint32  magic = STEER_MAGIC (0x54534642 "BFST")
 *   [4-7]   uint32  dim
 *   [8-11]  float   scale  (dequant: float = int8 * scale)
 *   [12..]  int8[dim]  quantized delta (max_abs / 127)
 *
 * Quantizing to INT8 (384 bytes for MiniLM) means 2,730 steering
 * vectors fit in 1 MB — the full "branch namespace" for an edge device.
 */
#define _DEFAULT_SOURCE
#include "include/bonfyre.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <dirent.h>
#include <math.h>
#include <ctype.h>
#include <unistd.h>
#include <errno.h>

#define STEER_MAGIC 0x54534642u   /* "BFST" */
#define MAX_SP      4096

/* ── path helpers ─────────────────────────────────────────────── */

static const char *home_st_(void) {
    const char *h = getenv("HOME");
    return h ? h : "/tmp";
}

static void steers_dir_(char *buf, size_t sz) {
    snprintf(buf, sz, "%s/.local/share/bonfyre/steers", home_st_());
}

/* Only allow [a-zA-Z0-9_:\-.] in branch names — no path traversal. */
static int name_ok_(const char *name) {
    if (!name || !*name || strlen(name) > 128) return 0;
    for (const char *p = name; *p; p++) {
        if (!isalnum((unsigned char)*p) &&
            *p != '_' && *p != ':' && *p != '-' && *p != '.') return 0;
    }
    return 1;
}

static void steer_path_(const char *name, char *buf, size_t sz) {
    char dir[MAX_SP];
    steers_dir_(dir, sizeof(dir));
    snprintf(buf, sz, "%s/%s.bfsteer", dir, name);
}

/* ── API ──────────────────────────────────────────────────────── */

/*
 * bf_embed_steer_add — quantize a float32 delta to INT8 and persist.
 *
 * scale = max_abs(delta) / 127.0f
 * q[i]  = clamp(round(delta[i] / scale), -127, 127)
 *
 * Atomic rename-on-write. Idempotent (overwrites if name exists).
 * Returns 0 on success, -1 on error.
 */
int bf_embed_steer_add(const char *name, const float *delta, uint32_t dim) {
    if (!name_ok_(name) || !delta || dim == 0) return -1;

    /* Compute quantization scale */
    float max_abs = 0.0f;
    for (uint32_t i = 0; i < dim; i++) {
        float a = fabsf(delta[i]);
        if (a > max_abs) max_abs = a;
    }
    if (max_abs == 0.0f) return 0;   /* zero delta — nothing to store */
    float scale = max_abs / 127.0f;

    int8_t *q = malloc(dim);
    if (!q) return -1;
    for (uint32_t i = 0; i < dim; i++) {
        float v = delta[i] / scale;
        int   iv = (int)roundf(v);
        if (iv >  127) iv =  127;
        if (iv < -127) iv = -127;
        q[i] = (int8_t)iv;
    }

    char dir[MAX_SP];
    steers_dir_(dir, sizeof(dir));
    bf_ensure_dir(dir);

    char path[MAX_SP], tmp[MAX_SP + 4];
    steer_path_(name, path, sizeof(path));
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);

    FILE *f = fopen(tmp, "wb");
    if (!f) { free(q); return -1; }

    uint32_t magic = STEER_MAGIC;
    int ok = (fwrite(&magic, 4, 1, f) == 1 &&
              fwrite(&dim,   4, 1, f) == 1 &&
              fwrite(&scale, 4, 1, f) == 1 &&
              fwrite(q, 1, (size_t)dim, f) == (size_t)dim);
    fclose(f);
    free(q);

    if (!ok) { unlink(tmp); return -1; }
    rename(tmp, path);
    return 0;
}

/*
 * bf_embed_steer_apply — apply n named steering vectors in-place.
 *
 * v_out[i] = v[i] + Σ alphas[k] * int8_k[i] * scale_k
 *
 * Returns 0 on success, -1 if any named branch is not found or dim mismatch.
 * All branches must have the same dimension as vec.
 */
int bf_embed_steer_apply(float *vec, uint32_t dim,
                         const char **names, const float *alphas, int n) {
    for (int s = 0; s < n; s++) {
        if (!name_ok_(names[s])) return -1;
        char path[MAX_SP];
        steer_path_(names[s], path, sizeof(path));

        FILE *f = fopen(path, "rb");
        if (!f) return -1;

        uint32_t magic, sdim;
        float scale;
        if (fread(&magic, 4, 1, f) != 1 || magic != STEER_MAGIC ||
            fread(&sdim,  4, 1, f) != 1 || sdim != dim          ||
            fread(&scale, 4, 1, f) != 1) {
            fclose(f); return -1;
        }

        int8_t *q = malloc(dim);
        if (!q) { fclose(f); return -1; }
        if (fread(q, 1, (size_t)dim, f) != (size_t)dim) {
            free(q); fclose(f); return -1;
        }
        fclose(f);

        float alpha = alphas[s];
        for (uint32_t i = 0; i < dim; i++)
            vec[i] += alpha * (float)q[i] * scale;
        free(q);
    }
    return 0;
}

/*
 * bf_embed_steer_list — enumerate all stored branch names.
 *
 * Fills *out_names with malloc'd strings (caller frees each + the array).
 * Returns count on success, -1 on error.
 */
int bf_embed_steer_list(char ***out_names, int *out_count) {
    char dir[MAX_SP];
    steers_dir_(dir, sizeof(dir));

    DIR *d = opendir(dir);
    if (!d) { *out_names = NULL; *out_count = 0; return 0; }

    size_t cap = 16, n = 0;
    char **names = malloc(cap * sizeof(char *));
    if (!names) { closedir(d); return -1; }

    struct dirent *de;
    while ((de = readdir(d))) {
        const char *nm = de->d_name;
        size_t nlen = strlen(nm);
        if (nlen <= 8 || strcmp(nm + nlen - 8, ".bfsteer") != 0) continue;
        if (n == cap) {
            cap *= 2;
            char **tmp = realloc(names, cap * sizeof(char *));
            if (!tmp) break;
            names = tmp;
        }
        char stripped[MAX_SP];
        snprintf(stripped, sizeof(stripped), "%.*s", (int)(nlen - 8), nm);
        names[n] = strdup(stripped);
        if (names[n]) n++;
    }
    closedir(d);
    *out_names  = names;
    *out_count  = (int)n;
    return (int)n;
}
