/*
 * bf_sae.c — Bonfyre .bfsae runtime: mmap, top-k activation, feature hash,
 *             manifest JSON, danger gate, and synthetic dict writer.
 *
 * Encoder: ReLU( x · W_enc + b_enc ) — standard sparse-autoencoder forward pass.
 * The encoder matrix is stored row-major as float16[N][D]:
 *   feature_i activation = max(0, dot(residual, W_enc[i]) + bias[i])
 *
 * For the synthetic/test path the weights are float32; the fp16 path
 * converts on the fly using a portable bit-cast (no HW fp16 intrinsics needed).
 *
 * Limitation: this file handles the control-plane (manifest, gate, hash, inspect).
 * Heavy fp16 matrix multiply is done via bfsae_activate_f32() which accepts
 * a pre-dequantised residual.  Callers that receive fp16 residuals from a model
 * runtime should dequantise first or use bfsae_activate() which does it inline.
 */

#include "bf_sae.h"
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* ── fp16 ↔ fp32 portable helpers ─────────────────────────────────────────── */

static float fp16_to_f32(uint16_t h) {
    uint32_t sign  = (uint32_t)(h & 0x8000) << 16;
    uint32_t exp   = (h >> 10) & 0x1f;
    uint32_t mant  = (uint32_t)(h & 0x03ff);
    uint32_t bits;
    if (exp == 0) {
        /* subnormal */
        if (mant == 0) { bits = sign; }
        else {
            exp = 1;
            while (!(mant & 0x400)) { mant <<= 1; exp--; }
            mant &= 0x3ff;
            bits = sign | ((exp + 112) << 23) | (mant << 13);
        }
    } else if (exp == 31) {
        bits = sign | 0x7f800000 | (mant << 13);  /* inf/nan */
    } else {
        bits = sign | ((exp + 112) << 23) | (mant << 13);
    }
    float f; memcpy(&f, &bits, 4); return f;
}

/* ── mmap helpers ──────────────────────────────────────────────────────────── */

BfsaeDict *bfsae_open(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { fprintf(stderr, "bfsae_open: cannot open %s: %s\n", path, strerror(errno)); return NULL; }
    struct stat st; fstat(fd, &st);
    size_t sz = (size_t)st.st_size;
    if (sz < sizeof(BfsaeHeader)) { fprintf(stderr, "bfsae_open: %s too small\n", path); close(fd); return NULL; }
    void *base = mmap(NULL, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (base == MAP_FAILED) { fprintf(stderr, "bfsae_open: mmap failed: %s\n", strerror(errno)); close(fd); return NULL; }

    const BfsaeHeader *h = (const BfsaeHeader *)base;
    if (memcmp(h->magic, BFSAE_MAGIC, BFSAE_MAGIC_LEN) != 0) {
        fprintf(stderr, "bfsae_open: bad magic in %s\n", path);
        munmap(base, sz); close(fd); return NULL;
    }

    BfsaeDict *d = calloc(1, sizeof(BfsaeDict));
    d->header      = h;
    d->_mmap_base  = base;
    d->_fd         = fd;
    d->file_size   = sz;

    const uint8_t *b = (const uint8_t *)base;
    if (h->meta_offset   && h->meta_offset   + h->feature_count * sizeof(BfsaeFeatureMeta) <= sz)
        d->meta       = (const BfsaeFeatureMeta *)(b + h->meta_offset);
    if (h->label_offset  && h->label_offset  + h->feature_count * sizeof(BfsaeLabelEntry)  <= sz)
        d->labels     = (const BfsaeLabelEntry  *)(b + h->label_offset);
    if (h->label_heap_offset && h->label_heap_offset < sz)
        d->label_heap = (const char             *)(b + h->label_heap_offset);
    if (h->encoder_offset && h->encoder_offset < sz)
        d->encoder    = (const void             *)(b + h->encoder_offset);
    if (h->decoder_offset && h->decoder_offset < sz)
        d->decoder    = (const void             *)(b + h->decoder_offset);

    return d;
}

void bfsae_close(BfsaeDict *d) {
    if (!d) return;
    if (d->_mmap_base) munmap(d->_mmap_base, d->file_size);
    if (d->_fd >= 0) close(d->_fd);
    free(d);
}

/* ── label lookup ──────────────────────────────────────────────────────────── */

static const char *lookup_label(const BfsaeDict *d, uint32_t fid) {
    if (!d->labels || !d->label_heap) return "";
    /* labels may not be sorted; linear scan for safety (N ≤ 131072, fast in cache) */
    uint32_t N = d->header->feature_count;
    for (uint32_t i = 0; i < N; i++) {
        if (d->labels[i].feature_id == fid)
            return d->label_heap + d->labels[i].label_offset;
    }
    return "";
}

/* ── activation (f32 residual → top-k) ────────────────────────────────────── */

BfsaeActivation *bfsae_activate_f32(const BfsaeDict *d,
                                     const float     *residual,
                                     uint32_t         hidden_dim,
                                     uint32_t         top_k) {
    if (!d || !residual) return NULL;
    uint32_t N = d->header->feature_count;
    uint32_t D = d->header->hidden_dim;
    if (hidden_dim < D) D = hidden_dim;   /* use min to avoid OOB */
    if (top_k > N) top_k = N;

    /* Compute activations = ReLU(encoder × residual + bias) */
    float *acts = calloc(N, sizeof(float));
    if (!acts) return NULL;

    if (d->encoder) {
        const uint16_t *E = (const uint16_t *)d->encoder;  /* float16[N][D] */
        for (uint32_t i = 0; i < N; i++) {
            float dot = 0.0f;
            const uint16_t *row = E + (size_t)i * D;
            for (uint32_t j = 0; j < D; j++)
                dot += fp16_to_f32(row[j]) * residual[j];
            /* add per-feature bias from meta */
            if (d->meta) dot += d->meta[i].bias;
            acts[i] = dot > 0.0f ? dot : 0.0f;  /* ReLU */
        }
    } else {
        /* No encoder loaded (synthetic stub): random stable activations */
        for (uint32_t i = 0; i < N; i++) {
            /* deterministic: hash (i, residual[i % D]) */
            float v = residual[i % D] * (float)(((i * 2654435769u) >> 16) & 0xff) / 255.0f;
            acts[i] = v > 0.0f ? v : 0.0f;
        }
    }

    /* Partial sort: find top_k by activation descending */
    uint32_t *idx = malloc(N * sizeof(uint32_t));
    if (!idx) { free(acts); return NULL; }
    for (uint32_t i = 0; i < N; i++) idx[i] = i;

    /* selection-sort for small top_k; insertion sort otherwise */
    uint32_t lim = top_k < 64 ? top_k : 64;
    for (uint32_t i = 0; i < lim; i++) {
        uint32_t best = i;
        for (uint32_t j = i + 1; j < N; j++)
            if (acts[idx[j]] > acts[idx[best]]) best = j;
        uint32_t t = idx[i]; idx[i] = idx[best]; idx[best] = t;
    }
    if (top_k > 64) {
        /* insertion sort rest */
        for (uint32_t i = 64; i < top_k; i++) {
            uint32_t best = i;
            for (uint32_t j = i + 1; j < N; j++)
                if (acts[idx[j]] > acts[idx[best]]) best = j;
            uint32_t t = idx[i]; idx[i] = idx[best]; idx[best] = t;
        }
    }

    BfsaeActivation *res = malloc(sizeof(BfsaeActivation) + top_k * sizeof(BfsaeTopFeature));
    if (!res) { free(acts); free(idx); return NULL; }
    res->count  = top_k;
    res->top_k  = top_k;
    res->layer  = d->header->layer;
    res->header = d->header;

    for (uint32_t k = 0; k < top_k; k++) {
        uint32_t fid = idx[k];
        float    a   = acts[fid];
        float    std = (d->meta && d->meta[fid].std_activation > 0.0f)
                       ? d->meta[fid].std_activation : 1.0f;
        res->features[k].feature_id  = fid;
        res->features[k].activation  = a;
        res->features[k].normalised  = a / std;
        res->features[k].tags        = d->meta ? d->meta[fid].tags : BFSAE_TAG_NONE;
        res->features[k].label       = lookup_label(d, fid);
    }

    free(acts);
    free(idx);
    return res;
}

BfsaeActivation *bfsae_activate(const BfsaeDict *d,
                                 const void      *residual_fp16,
                                 uint32_t         hidden_dim,
                                 uint32_t         top_k) {
    /* dequantise fp16 → f32 then delegate */
    const uint16_t *src = (const uint16_t *)residual_fp16;
    float *f32 = malloc(hidden_dim * sizeof(float));
    if (!f32) return NULL;
    for (uint32_t i = 0; i < hidden_dim; i++) f32[i] = fp16_to_f32(src[i]);
    BfsaeActivation *res = bfsae_activate_f32(d, f32, hidden_dim, top_k);
    free(f32);
    return res;
}

void bfsae_activation_free(BfsaeActivation *act) { free(act); }

/* ── manifest JSON ────────────────────────────────────────────────────────── */

int bfsae_manifest_json(const BfsaeActivation *act,
                         const char            *artifact_id,
                         char                  *out,
                         size_t                 out_sz) {
    if (!act || !out || out_sz < 64) return -1;
    char ts[32]; time_t now = time(NULL);
    strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));

    int n = snprintf(out, out_sz,
        "{\n"
        "  \"artifact_type\": \"sae-feature-manifest\",\n"
        "  \"artifact_id\": \"%s\",\n"
        "  \"model_family\": \"%s\",\n"
        "  \"layer\": %u,\n"
        "  \"dictionary\": \"%s\",\n"
        "  \"top_k\": %u,\n"
        "  \"created_at\": \"%s\",\n"
        "  \"features\": [\n",
        artifact_id ? artifact_id : "",
        act->header->model_family,
        act->layer,
        act->header->model_family,
        act->top_k,
        ts);

    for (uint32_t k = 0; k < act->count && n < (int)out_sz - 2; k++) {
        const BfsaeTopFeature *f = &act->features[k];
        const char *comma = (k + 1 < act->count) ? "," : "";
        /* escape label */
        char safe_label[128]; size_t li = 0;
        for (const char *p = f->label; *p && li < sizeof(safe_label) - 2; p++) {
            if (*p == '"' || *p == '\\') safe_label[li++] = '\\';
            safe_label[li++] = *p;
        }
        safe_label[li] = '\0';
        n += snprintf(out + n, out_sz - n,
            "    { \"feature_id\": %u, \"activation\": %.6f,"
            " \"normalised\": %.4f, \"tags\": %u, \"label\": \"%s\" }%s\n",
            f->feature_id, f->activation, f->normalised, f->tags,
            safe_label, comma);
    }
    n += snprintf(out + n, out_sz - n, "  ]\n}\n");
    return (n < (int)out_sz) ? n : -1;
}

/* ── danger check ─────────────────────────────────────────────────────────── */

int bfsae_danger_check(const BfsaeActivation *act, float alpha) {
    if (!act) return -1;
    int found_danger = 0;
    for (uint32_t k = 0; k < act->count; k++) {
        if (act->features[k].tags & BFSAE_TAG_DANGER) {
            found_danger = 1;
            if (act->features[k].activation >= alpha) return 1;
        }
    }
    return found_danger ? 0 : -1;
}

/* ── feature hash ─────────────────────────────────────────────────────────── */

void bfsae_feature_hash(const BfsaeActivation *act, char *out_hex) {
    /* FNV-1a over (layer, feature_id_0, feature_id_1, ...) */
    uint64_t h = 14695981039346656037ULL;
    h ^= (uint64_t)act->layer;
    h *= 1099511628211ULL;
    for (uint32_t k = 0; k < act->count; k++) {
        uint32_t fid = act->features[k].feature_id;
        h ^= fid & 0xff;           h *= 1099511628211ULL;
        h ^= (fid >> 8) & 0xff;    h *= 1099511628211ULL;
        h ^= (fid >> 16) & 0xff;   h *= 1099511628211ULL;
        h ^= (fid >> 24) & 0xff;   h *= 1099511628211ULL;
    }
    snprintf(out_hex, 17, "%016llx", (unsigned long long)h);
}

/* ── inspect ─────────────────────────────────────────────────────────────── */

void bfsae_inspect(const BfsaeDict *d) {
    if (!d) return;
    const BfsaeHeader *h = d->header;
    static const char *site_names[] = {"residual","mlp","attention","embed"};
    static const char *dtype_names[] = {"fp32","fp16","bf16","int8","fpq"};
    printf("  model_family   : %s\n",  h->model_family);
    printf("  model_revision : %s\n",  h->model_revision[0] ? h->model_revision : "(none)");
    printf("  layer          : %u\n",  h->layer);
    printf("  site           : %s\n",  h->site < 4 ? site_names[h->site] : "?");
    printf("  hidden_dim     : %u\n",  h->hidden_dim);
    printf("  feature_count  : %u\n",  h->feature_count);
    printf("  dtype          : %s\n",  h->dtype < 5 ? dtype_names[h->dtype] : "?");
    printf("  file_size      : %.2f MB\n", (double)h->file_size / 1048576.0);
    printf("  encoder_offset : 0x%llx\n", (unsigned long long)h->encoder_offset);
    printf("  decoder_offset : 0x%llx\n", (unsigned long long)h->decoder_offset);
    printf("  meta_offset    : 0x%llx\n", (unsigned long long)h->meta_offset);
    printf("  training_notes : %s\n",  h->training_notes[0] ? h->training_notes : "(none)");
}

/* ── synthetic writer ────────────────────────────────────────────────────── */

int bfsae_write_synthetic(const char *path,
                           const char *model_family,
                           uint32_t    layer,
                           uint32_t    hidden_dim,
                           uint32_t    feature_count) {
    FILE *f = fopen(path, "wb");
    if (!f) { perror("bfsae_write_synthetic"); return -1; }

    size_t meta_sz  = (size_t)feature_count * sizeof(BfsaeFeatureMeta);
    size_t enc_sz   = (size_t)feature_count * hidden_dim * sizeof(uint16_t); /* fp16[N][D] */
    size_t total    = sizeof(BfsaeHeader) + meta_sz + enc_sz;

    BfsaeHeader hdr = {0};
    memcpy(hdr.magic, BFSAE_MAGIC, BFSAE_MAGIC_LEN);
    hdr.version       = BFSAE_VERSION;
    hdr.site          = BFSAE_SITE_RESIDUAL;
    hdr.dtype         = BFSAE_DTYPE_FP16;
    hdr.layer         = layer;
    hdr.hidden_dim    = hidden_dim;
    hdr.feature_count = feature_count;
    hdr.meta_offset   = sizeof(BfsaeHeader);
    hdr.encoder_offset= sizeof(BfsaeHeader) + meta_sz;
    hdr.decoder_offset= hdr.encoder_offset; /* no decoder in synthetic */
    hdr.file_size     = total;
    strncpy(hdr.model_family, model_family, sizeof(hdr.model_family) - 1);
    strncpy(hdr.training_notes, "synthetic", sizeof(hdr.training_notes) - 1);
    fwrite(&hdr, sizeof(hdr), 1, f);

    /* meta: random biases and thresholds */
    srand(layer * 31337 + feature_count);
    for (uint32_t i = 0; i < feature_count; i++) {
        BfsaeFeatureMeta m = {0};
        m.feature_id        = i;
        m.bias              = 0.0f;
        m.mean_activation   = (float)(rand() % 100) / 1000.0f;
        m.std_activation    = 0.1f + (float)(rand() % 50) / 100.0f;
        m.danger_threshold  = 0.7f;
        m.default_threshold = 0.3f;
        m.tags              = (i % 50 == 0) ? BFSAE_TAG_DANGER : BFSAE_TAG_SEMANTIC;
        fwrite(&m, sizeof(m), 1, f);
    }

    /* encoder: random unit-norm fp16 rows */
    for (uint32_t i = 0; i < feature_count; i++) {
        float norm = 0.0f;
        float row[4096]; /* max dim we'll handle in synth */
        uint32_t D = hidden_dim < 4096 ? hidden_dim : 4096;
        for (uint32_t j = 0; j < D; j++) {
            row[j] = (float)(rand() - RAND_MAX/2) / (float)(RAND_MAX/2);
            norm  += row[j] * row[j];
        }
        norm = sqrtf(norm > 0.0f ? norm : 1.0f);
        for (uint32_t j = 0; j < D; j++) {
            /* float32 → float16 (round-to-nearest) */
            float v = row[j] / norm;
            uint32_t bits; memcpy(&bits, &v, 4);
            uint32_t sign = (bits >> 31) & 1;
            int32_t  exp  = (int32_t)((bits >> 23) & 0xff) - 127 + 15;
            uint32_t mant = (bits >> 13) & 0x3ff;
            uint16_t h16;
            if (exp <= 0)       h16 = (uint16_t)(sign << 15);
            else if (exp >= 31) h16 = (uint16_t)((sign << 15) | 0x7c00);
            else                h16 = (uint16_t)((sign << 15) | (exp << 10) | mant);
            fwrite(&h16, sizeof(h16), 1, f);
        }
    }

    fclose(f);
    return 0;
}
