/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * BonfyreTag — instant intent/topic tagging via fastText (pure C).
 *
 * Zero interpreter dependencies. Loads .bin models natively.
 * 2ms, no GPU. Classify any text into topics, intents, or custom labels.
 * Training requires a separately bound native training organ.
 *
 * Usage:
 *   bonfyre-tag predict <model.bin> <text-file>     → tags.json
 *   bonfyre-tag batch <model.bin> <dir>             → tag all text files
 *   bonfyre-tag train <training-data> <model-out>   → train custom model
 *   bonfyre-tag detect-lang <text-file>             → language detection
 */
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <dirent.h>
#include <bonfyre.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

/* ── helpers ─────────────────────────────────────────────────── */

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }

static char *read_file_contents(const char *path, size_t *out_len) {
    return bf_read_file(path, out_len);
}

/* ── fastText binary model loader ────────────────────────────── */

#define FT_MAGIC    793712314
#define FT_VERSION  12
#define FT_HASH_SEED 2166136261u

typedef struct {
    int32_t dim;
    int32_t ws;
    int32_t epoch;
    int32_t min_count;
    int32_t neg;
    int32_t word_ngrams;
    int32_t loss;
    int32_t model_type;
    int32_t bucket;
    int32_t minn;
    int32_t maxn;
    int32_t lr_update_rate;
    double  t;
} FtArgs;

typedef struct {
    char   *word;
    int64_t count;
    int8_t  type;   /* 0=word, 1=label */
} FtEntry;

/* Huffman tree node for hierarchical softmax */
typedef struct {
    int32_t parent;
    int32_t left;
    int32_t right;
    int64_t count;
    int     binary;  /* 1 = left child, 0 = right child */
} HsNode;

typedef struct {
    FtArgs   args;
    /* dictionary */
    int32_t  nwords;
    int32_t  nlabels;
    int64_t  ntokens;
    int32_t  dict_size;
    FtEntry *entries;
    int32_t *word2id;     /* hash table: hash → entry index (-1 = empty) */
    int32_t  word2id_sz;
    /* matrices */
    float   *input;       /* (nwords + bucket) × dim */
    float   *output;      /* internal_nodes × dim (for HS) or nlabels × dim */
    int64_t  input_rows;
    int64_t  output_rows;
    /* label index (into entries) */
    int32_t *label_ids;   /* nlabels indices into entries[] */
    /* hierarchical softmax tree */
    int       loss_type;  /* 1=hs, 2=ns, 3=softmax */
    HsNode   *tree;       /* 2*nlabels-1 nodes */
    int32_t **paths;      /* path from each label to root */
    int      **codes;     /* binary codes along each path */
    int       *path_lens; /* length of each path */
} FtModel;

static uint32_t ft_hash(const char *str, size_t len) {
    /* fastText FNV-1a: XOR with int8_t (signed char) cast for consistency */
    uint32_t h = FT_HASH_SEED;
    for (size_t i = 0; i < len; i++) {
        h ^= (uint32_t)(int8_t)str[i];
        h *= 16777619u;
    }
    return h;
}

static int32_t ft_dict_find(const FtModel *m, const char *w, size_t wlen) {
    uint32_t h = ft_hash(w, wlen) % (uint32_t)m->word2id_sz;
    while (m->word2id[h] >= 0) {
        const FtEntry *e = &m->entries[m->word2id[h]];
        if (strlen(e->word) == wlen && memcmp(e->word, w, wlen) == 0)
            return m->word2id[h];
        h = (h + 1) % (uint32_t)m->word2id_sz;
    }
    return -1;
}

static int ft_model_load(FtModel *m, const char *path) {
    memset(m, 0, sizeof(*m));
    FILE *f = fopen(path, "rb");
    if (!f) { fprintf(stderr, "[tag] Cannot open model: %s\n", path); return -1; }

    /* Header */
    int32_t magic, version;
    if (fread(&magic, 4, 1, f) != 1 || fread(&version, 4, 1, f) != 1) goto fail;
    if (magic != FT_MAGIC) { fprintf(stderr, "[tag] Bad magic: %d (expected %d)\n", magic, FT_MAGIC); goto fail; }
    if (version > FT_VERSION) { fprintf(stderr, "[tag] Unsupported version: %d\n", version); goto fail; }

    if (fread(&m->args.dim, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.ws, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.epoch, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.min_count, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.neg, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.word_ngrams, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.loss, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.model_type, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.bucket, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.minn, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.maxn, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.lr_update_rate, 4, 1, f) != 1) goto fail;
    if (fread(&m->args.t, 8, 1, f) != 1) goto fail;

    /* Dictionary */
    if (fread(&m->dict_size, 4, 1, f) != 1) goto fail;
    if (fread(&m->nwords, 4, 1, f) != 1) goto fail;
    if (fread(&m->nlabels, 4, 1, f) != 1) goto fail;
    if (fread(&m->ntokens, 8, 1, f) != 1) goto fail;

    int64_t pruneidx_size = 0;
    if (version >= 12) { if (fread(&pruneidx_size, 8, 1, f) != 1) goto fail; }

    if (m->dict_size <= 0 || m->dict_size > 50000000) goto fail;
    m->entries = calloc((size_t)m->dict_size, sizeof(FtEntry));
    if (!m->entries) goto fail;

    for (int32_t i = 0; i < m->dict_size; i++) {
        /* Read null-terminated word */
        char buf[1024];
        int pos = 0;
        int ch;
        while ((ch = fgetc(f)) != EOF && ch != '\0' && pos < 1023)
            buf[pos++] = (char)ch;
        buf[pos] = '\0';
        m->entries[i].word = strdup(buf);
        if (fread(&m->entries[i].count, 8, 1, f) != 1) goto fail;
        if (fread(&m->entries[i].type, 1, 1, f) != 1) goto fail;
    }

    /* Read pruning index (skip) */
    if (pruneidx_size > 0) {
        for (int64_t i = 0; i < pruneidx_size; i++) {
            int32_t k; int32_t v;
            if (fread(&k, 4, 1, f) != 1 || fread(&v, 4, 1, f) != 1) goto fail;
        }
    }

    /* Build word2id hash table */
    m->word2id_sz = m->dict_size * 2 + 1;
    m->word2id = malloc(sizeof(int32_t) * (size_t)m->word2id_sz);
    if (!m->word2id) goto fail;
    for (int32_t i = 0; i < m->word2id_sz; i++) m->word2id[i] = -1;

    m->label_ids = malloc(sizeof(int32_t) * (size_t)m->nlabels);
    if (!m->label_ids) goto fail;
    int32_t li = 0;

    for (int32_t i = 0; i < m->dict_size; i++) {
        if (m->entries[i].type == 1 && li < m->nlabels) {
            m->label_ids[li++] = i;
        }
        uint32_t h = ft_hash(m->entries[i].word, strlen(m->entries[i].word)) % (uint32_t)m->word2id_sz;
        while (m->word2id[h] >= 0)
            h = (h + 1) % (uint32_t)m->word2id_sz;
        m->word2id[h] = i;
    }

    /* Quantization flag */
    int8_t quant = 0;
    if (fread(&quant, 1, 1, f) != 1) goto fail;
    if (quant) { fprintf(stderr, "[tag] Quantized models not supported\n"); goto fail; }

    /* Input matrix */
    int64_t im, in_;
    if (fread(&im, 8, 1, f) != 1 || fread(&in_, 8, 1, f) != 1) goto fail;
    if (in_ != m->args.dim) { fprintf(stderr, "[tag] Dim mismatch: %lld vs %d\n", (long long)in_, m->args.dim); goto fail; }
    m->input_rows = im;
    m->input = malloc(sizeof(float) * (size_t)(im * in_));
    if (!m->input) goto fail;
    if (fread(m->input, sizeof(float), (size_t)(im * in_), f) != (size_t)(im * in_)) goto fail;

    /* Quantization flag for output */
    int8_t quant2 = 0;
    if (fread(&quant2, 1, 1, f) != 1) goto fail;
    if (quant2) { fprintf(stderr, "[tag] Quantized output not supported\n"); goto fail; }

    /* Output matrix */
    int64_t om, on;
    if (fread(&om, 8, 1, f) != 1 || fread(&on, 8, 1, f) != 1) goto fail;
    m->output_rows = om;
    m->output = malloc(sizeof(float) * (size_t)(om * on));
    if (!m->output) goto fail;
    if (fread(m->output, sizeof(float), (size_t)(om * on), f) != (size_t)(om * on)) goto fail;

    fclose(f);
    m->loss_type = m->args.loss;  /* 1=hs, 2=ns, 3=softmax */
    m->tree = NULL; m->paths = NULL; m->codes = NULL; m->path_lens = NULL;
    fprintf(stderr, "[tag] Loaded model: %d words, %d labels, dim=%d, bucket=%d, loss=%d\n",
            m->nwords, m->nlabels, m->args.dim, m->args.bucket, m->loss_type);
    return 0;

fail:
    fclose(f);
    fprintf(stderr, "[tag] Failed to load model: %s\n", path);
    return -1;
}

static void ft_model_free(FtModel *m) {
    if (m->entries) {
        for (int32_t i = 0; i < m->dict_size; i++) free(m->entries[i].word);
        free(m->entries);
    }
    free(m->word2id);
    free(m->label_ids);
    free(m->input);
    free(m->output);
    if (m->paths) {
        for (int32_t i = 0; i < m->nlabels; i++) { free(m->paths[i]); free(m->codes[i]); }
        free(m->paths); free(m->codes); free(m->path_lens);
    }
    free(m->tree);
    memset(m, 0, sizeof(*m));
}

/* ── Hierarchical softmax (Huffman tree) ─────────────────────── */

static int ft_build_hs_tree(FtModel *m) {
    int32_t osz = m->nlabels;
    int32_t tree_sz = 2 * osz - 1;
    m->tree = calloc((size_t)tree_sz, sizeof(HsNode));
    if (!m->tree) return -1;

    /* Initialize nodes */
    for (int32_t i = 0; i < tree_sz; i++) {
        m->tree[i].parent = -1;
        m->tree[i].left = -1;
        m->tree[i].right = -1;
        m->tree[i].count = (int64_t)1e15;
        m->tree[i].binary = 0;
    }
    /* Leaf nodes: labels sorted by decreasing count in dictionary */
    for (int32_t i = 0; i < osz; i++) {
        m->tree[i].count = m->entries[m->label_ids[i]].count;
    }

    /* Build Huffman tree (identical to fastText's buildTree) */
    int32_t leaf = osz - 1;
    int32_t node = osz;
    for (int32_t i = osz; i < tree_sz; i++) {
        int32_t mini[2] = {0, 0};
        for (int j = 0; j < 2; j++) {
            if (leaf >= 0 && m->tree[leaf].count < m->tree[node].count) {
                mini[j] = leaf--;
            } else {
                mini[j] = node++;
            }
        }
        m->tree[i].left = mini[0];
        m->tree[i].right = mini[1];
        m->tree[i].count = m->tree[mini[0]].count + m->tree[mini[1]].count;
        m->tree[mini[0]].parent = i;
        m->tree[mini[1]].parent = i;
        m->tree[mini[0]].binary = 1;  /* left = true */
        m->tree[mini[1]].binary = 0;  /* right = false */
    }

    /* Compute path & code for each label (leaf → root) */
    m->paths = calloc((size_t)osz, sizeof(int32_t *));
    m->codes = calloc((size_t)osz, sizeof(int *));
    m->path_lens = calloc((size_t)osz, sizeof(int));
    if (!m->paths || !m->codes || !m->path_lens) return -1;

    for (int32_t i = 0; i < osz; i++) {
        /* Count path length */
        int len = 0;
        int32_t j = i;
        while (m->tree[j].parent != -1) { len++; j = m->tree[j].parent; }
        m->path_lens[i] = len;
        m->paths[i] = malloc(sizeof(int32_t) * (size_t)len);
        m->codes[i] = malloc(sizeof(int) * (size_t)len);
        if (!m->paths[i] || !m->codes[i]) return -1;

        j = i;
        for (int k = 0; k < len; k++) {
            m->paths[i][k] = m->tree[j].parent - osz;  /* output matrix row */
            m->codes[i][k] = m->tree[j].binary;
            j = m->tree[j].parent;
        }
    }

    fprintf(stderr, "[tag] Built Huffman tree: %d labels, %d nodes\n", osz, tree_sz);
    return 0;
}

/* ── fastText inference ──────────────────────────────────────── */

static void ft_add_ngrams(const FtModel *m, const char *word,
                          int32_t *ids, int *nids, int cap) {
    /* Wrap word in < > for character ngrams */
    size_t wlen = strlen(word);
    size_t blen = wlen + 2;
    char *buf = malloc(blen + 1);
    if (!buf) return;
    buf[0] = '<';
    memcpy(buf + 1, word, wlen);
    buf[blen - 1] = '>';
    buf[blen] = '\0';

    for (int32_t n = m->args.minn; n <= m->args.maxn && n <= (int32_t)blen; n++) {
        for (size_t i = 0; i + (size_t)n <= blen; i++) {
            uint32_t h = ft_hash(buf + i, (size_t)n);
            int32_t id = m->nwords + (int32_t)(h % (uint32_t)m->args.bucket);
            if (*nids < cap) ids[(*nids)++] = id;
        }
    }
    free(buf);
}

typedef struct { int32_t label_idx; float score; } FtPred;

static int ft_pred_cmp(const void *a, const void *b) {
    float sa = ((const FtPred *)a)->score;
    float sb = ((const FtPred *)b)->score;
    return (sb > sa) ? 1 : (sb < sa) ? -1 : 0;
}

static int ft_predict(const FtModel *m, const char *text, int top_k,
                      FtPred *out, int *nout) {
    int dim = m->args.dim;
    float *hidden = calloc((size_t)dim, sizeof(float));
    if (!hidden) return -1;

    /* Tokenize: split on whitespace (no lowercase — match fastText) */
    size_t tlen = strlen(text);
    char *tmp = malloc(tlen + 1);
    if (!tmp) { free(hidden); return -1; }
    memcpy(tmp, text, tlen + 1);

    int32_t *ids = malloc(sizeof(int32_t) * (tlen + 1) * 20);
    if (!ids) { free(tmp); free(hidden); return -1; }
    int nids = 0;
    int id_cap = (int)((tlen + 1) * 20);

    /* Split by whitespace and look up words */
    char *saveptr = NULL;
    char *tok = strtok_r(tmp, " \t\n\r", &saveptr);
    while (tok) {
        size_t toklen = strlen(tok);
        int32_t wid = ft_dict_find(m, tok, toklen);
        if (wid >= 0 && m->entries[wid].type == 0) {
            if (nids < id_cap) ids[nids++] = wid;
        }
        if (m->args.maxn > 0) {
            ft_add_ngrams(m, tok, ids, &nids, id_cap);
        }
        tok = strtok_r(NULL, " \t\n\r", &saveptr);
    }
    free(tmp);

    if (nids == 0) { free(ids); free(hidden); *nout = 0; return 0; }

    /* Average input vectors */
    for (int i = 0; i < nids; i++) {
        int32_t row = ids[i];
        if (row >= 0 && row < m->input_rows) {
            const float *v = m->input + (int64_t)row * dim;
            for (int d = 0; d < dim; d++) hidden[d] += v[d];
        }
    }
    float inv = 1.0f / (float)nids;
    for (int d = 0; d < dim; d++) hidden[d] *= inv;
    free(ids);

    if (m->loss_type == 1 && m->tree) {
        /* Hierarchical softmax: compute log-probability for each label
         * by traversing the Huffman tree path. */
        int nl = m->nlabels;
        FtPred *preds = malloc(sizeof(FtPred) * (size_t)nl);
        if (!preds) { free(hidden); return -1; }

        for (int l = 0; l < nl; l++) {
            double log_prob = 0.0;
            for (int k = 0; k < m->path_lens[l]; k++) {
                int32_t node_row = m->paths[l][k];
                if (node_row < 0 || node_row >= m->output_rows) continue;
                const float *ov = m->output + (int64_t)node_row * dim;
                float dot = 0;
                for (int d = 0; d < dim; d++) dot += ov[d] * hidden[d];
                float s = 1.0f / (1.0f + expf(-dot));  /* sigmoid */
                if (m->codes[l][k]) {
                    log_prob += log(1.0 - (double)s + 1e-10);
                } else {
                    log_prob += log((double)s + 1e-10);
                }
            }
            preds[l].label_idx = l;
            preds[l].score = (float)exp(log_prob);
        }
        free(hidden);

        qsort(preds, (size_t)nl, sizeof(FtPred), ft_pred_cmp);
        *nout = (top_k < nl) ? top_k : nl;
        memcpy(out, preds, sizeof(FtPred) * (size_t)*nout);
        free(preds);
    } else {
        /* Standard softmax */
        int nl = (int)m->output_rows;
        if (nl > m->nlabels) nl = m->nlabels;
        FtPred *preds = malloc(sizeof(FtPred) * (size_t)nl);
        if (!preds) { free(hidden); return -1; }

        for (int l = 0; l < nl; l++) {
            const float *ov = m->output + (int64_t)l * dim;
            float dot = 0;
            for (int d = 0; d < dim; d++) dot += ov[d] * hidden[d];
            preds[l].label_idx = l;
            preds[l].score = dot;
        }
        free(hidden);

        float maxs = preds[0].score;
        for (int l = 1; l < nl; l++) if (preds[l].score > maxs) maxs = preds[l].score;
        float sum = 0;
        for (int l = 0; l < nl; l++) {
            preds[l].score = expf(preds[l].score - maxs);
            sum += preds[l].score;
        }
        if (sum > 0) for (int l = 0; l < nl; l++) preds[l].score /= sum;

        qsort(preds, (size_t)nl, sizeof(FtPred), ft_pred_cmp);
        *nout = (top_k < nl) ? top_k : nl;
        memcpy(out, preds, sizeof(FtPred) * (size_t)*nout);
        free(preds);
    }
    return 0;
}

static const char *ft_label_name(const FtModel *m, int label_idx) {
    if (label_idx < 0 || label_idx >= m->nlabels) return "?";
    int32_t eid = m->label_ids[label_idx];
    const char *w = m->entries[eid].word;
    /* Strip __label__ prefix if present */
    if (strncmp(w, "__label__", 9) == 0) return w + 9;
    return w;
}

/* ── commands (pure C inference) ─────────────────────────────── */

static int cmd_predict(const char *model_path, const char *text_file,
                       const char *out_dir, int top_k, FtModel *model) {
    ensure_dir(out_dir);

    char json_out[PATH_MAX];
    snprintf(json_out, sizeof(json_out), "%s/tags.json", out_dir);

    size_t tlen = 0;
    char *content = read_file_contents(text_file, &tlen);
    if (!content) { fprintf(stderr, "[tag] Cannot read: %s\n", text_file); return 1; }

    /* Split into lines */
    FILE *out = fopen(json_out, "w");
    if (!out) { free(content); return 1; }

    fprintf(out, "{\n  \"type\": \"text-tags\",\n  \"model\": \"%s\",\n"
                 "  \"source\": \"%s\",\n", model_path, text_file);

    /* Count and process lines */
    int count = 0;
    char *line_start = content;
    /* First pass: count lines */
    for (char *p = content; ; p++) {
        if (*p == '\n' || *p == '\0') {
            size_t ll = (size_t)(p - line_start);
            if (ll > 0) count++;
            line_start = p + 1;
            if (*p == '\0') break;
        }
    }

    fprintf(out, "  \"count\": %d,\n  \"predictions\": [\n", count);

    line_start = content;
    int idx = 0;
    FtPred results[64];
    for (char *p = content; ; p++) {
        if (*p == '\n' || *p == '\0') {
            size_t ll = (size_t)(p - line_start);
            if (ll > 0) {
                char saved = *p; *p = '\0';

                int nresults = 0;
                ft_predict(model, line_start, top_k, results, &nresults);

                fprintf(out, "    {\"text\": \"");
                /* JSON-escape first 200 chars */
                int lim = (int)ll > 200 ? 200 : (int)ll;
                for (int i = 0; i < lim; i++) {
                    char c = line_start[i];
                    if (c == '"') fprintf(out, "\\\"");
                    else if (c == '\\') fprintf(out, "\\\\");
                    else if (c == '\t') fprintf(out, "\\t");
                    else if ((unsigned char)c >= 0x20) fputc(c, out);
                }
                fprintf(out, "\", \"tags\": [");
                for (int r = 0; r < nresults; r++) {
                    fprintf(out, "%s{\"tag\": \"%s\", \"confidence\": %.4f}",
                            r ? ", " : "",
                            ft_label_name(model, results[r].label_idx),
                            results[r].score);
                }
                fprintf(out, "]}%s\n", (idx + 1 < count) ? "," : "");
                idx++;
                *p = saved;
            }
            line_start = p + 1;
            if (*p == '\0') break;
        }
    }

    fprintf(out, "  ]\n}\n");
    fclose(out);
    free(content);
    fprintf(stderr, "[tag] %d lines tagged -> %s\n", count, json_out);
    return 0;
}

static int cmd_batch(const char *model_path, const char *dir,
                     const char *out_dir, FtModel *model) {
    ensure_dir(out_dir);
    fprintf(stderr, "[tag] Batch tagging all text files in %s\n", dir);

    DIR *d = opendir(dir);
    if (!d) { perror("opendir"); return 1; }
    struct dirent *ent;
    int count = 0;
    while ((ent = readdir(d))) {
        if (ent->d_name[0] == '.') continue;
        const char *ext = strrchr(ent->d_name, '.');
        if (!ext) continue;
        if (strcmp(ext, ".txt") != 0 && strcmp(ext, ".md") != 0) continue;

        char fp[PATH_MAX], out_sub[PATH_MAX];
        snprintf(fp, sizeof(fp), "%s/%s", dir, ent->d_name);

        char basename[256];
        strncpy(basename, ent->d_name, sizeof(basename) - 1);
        basename[sizeof(basename) - 1] = '\0';
        char *dot = strrchr(basename, '.');
        if (dot) *dot = '\0';
        snprintf(out_sub, sizeof(out_sub), "%s/%s", out_dir, basename);

        cmd_predict(model_path, fp, out_sub, 3, model);
        count++;
    }
    closedir(d);

    fprintf(stderr, "[tag] Batch complete: %d files tagged\n", count);
    return 0;
}

/* Training, bound natively.
 *
 * This organ could read a fastText model and never make one: train() refused
 * and told the caller to bind a native training organ. So the estate could
 * classify its material only with a model somebody else had trained on
 * somebody else's corpus, which is the opposite of what a tagger is for.
 *
 * What is implemented is the model this file already knows how to read: a
 * linear bag-of-words classifier, hidden state the mean of its features'
 * vectors, softmax over labels, plain SGD with a linearly decayed rate. No
 * hierarchical softmax and no character n-grams, because the reader treats
 * both as optional and a model that uses neither is a model it loads.
 *
 * Training data is one example per line, fastText's own shape:
 *   __label__sport  arsenal beat spurs in the north london derby
 */
#define TRAIN_DIM        64
#define TRAIN_EPOCHS     12
#define TRAIN_LR         0.35f
#define TRAIN_MIN_COUNT  2
#define TRAIN_BUCKET     1        /* no char n-grams, so no bucket is used */
#define TRAIN_MAX_FEAT   256

typedef struct { char *word; int64_t count; int8_t type; } TrainEntry;

typedef struct {
    TrainEntry *e; int32_t n, cap;
    int32_t *table; int32_t table_sz;
} TrainDict;

static void train_dict_init(TrainDict *d, int32_t table_sz) {
    d->cap = 4096; d->n = 0;
    d->e = calloc((size_t)d->cap, sizeof(TrainEntry));
    d->table_sz = table_sz;
    d->table = malloc(sizeof(int32_t) * (size_t)table_sz);
    for (int32_t i = 0; i < table_sz; i++) d->table[i] = -1;
}

static int32_t train_dict_get(TrainDict *d, const char *w, int8_t type, int add) {
    size_t wlen = strlen(w);
    uint32_t h = ft_hash(w, wlen) % (uint32_t)d->table_sz;
    while (d->table[h] >= 0) {
        if (strcmp(d->e[d->table[h]].word, w) == 0) return d->table[h];
        h = (h + 1) % (uint32_t)d->table_sz;
    }
    if (!add) return -1;
    if (d->n == d->cap) {
        d->cap *= 2;
        d->e = realloc(d->e, (size_t)d->cap * sizeof(TrainEntry));
        memset(d->e + d->n, 0, (size_t)(d->cap - d->n) * sizeof(TrainEntry));
    }
    d->e[d->n].word = strdup(w);
    d->e[d->n].count = 0;
    d->e[d->n].type = type;
    d->table[h] = d->n;
    return d->n++;
}

static void train_dict_free(TrainDict *d) {
    for (int32_t i = 0; i < d->n; i++) free(d->e[i].word);
    free(d->e); free(d->table);
}

static int cmd_train(const char *training_data, const char *model_out) {
    FILE *f = fopen(training_data, "r");
    if (!f) { fprintf(stderr, "[tag] Cannot read training data: %s\n", training_data); return 1; }

    /* First pass: what words and labels exist, and how often. */
    TrainDict raw; train_dict_init(&raw, 1 << 21);
    char line[8192];
    int64_t ntokens = 0, nlines = 0;
    while (fgets(line, sizeof(line), f)) {
        char *save = NULL, *tok = strtok_r(line, " \t\n\r", &save);
        int had_label = 0;
        while (tok) {
            int is_label = strncmp(tok, "__label__", 9) == 0;
            int32_t id = train_dict_get(&raw, tok, is_label ? 1 : 0, 1);
            raw.e[id].count++;
            if (is_label) had_label = 1; else ntokens++;
            tok = strtok_r(NULL, " \t\n\r", &save);
        }
        if (had_label) nlines++;
    }
    if (nlines == 0) {
        fprintf(stderr, "[tag] No labelled examples in %s\n", training_data);
        fclose(f); train_dict_free(&raw); return 1;
    }

    /* The reader indexes the input matrix by dictionary position for words, so
     * every word must come before every label. Rare words are dropped: a word
     * seen once teaches the classifier a coincidence. */
    TrainDict d; train_dict_init(&d, 1 << 21);
    for (int32_t i = 0; i < raw.n; i++)
        if (raw.e[i].type == 0 && raw.e[i].count >= TRAIN_MIN_COUNT) {
            int32_t id = train_dict_get(&d, raw.e[i].word, 0, 1);
            d.e[id].count = raw.e[i].count;
        }
    int32_t nwords = d.n;
    for (int32_t i = 0; i < raw.n; i++)
        if (raw.e[i].type == 1) {
            int32_t id = train_dict_get(&d, raw.e[i].word, 1, 1);
            d.e[id].count = raw.e[i].count;
        }
    int32_t nlabels = d.n - nwords;
    train_dict_free(&raw);
    if (nlabels < 2) {
        fprintf(stderr, "[tag] %d label(s): a classifier needs at least two\n", nlabels);
        fclose(f); train_dict_free(&d); return 1;
    }

    int dim = TRAIN_DIM;
    int64_t input_rows = (int64_t)nwords + TRAIN_BUCKET;
    float *input = malloc(sizeof(float) * (size_t)(input_rows * dim));
    float *output = calloc((size_t)((int64_t)nlabels * dim), sizeof(float));
    if (!input || !output) { fclose(f); train_dict_free(&d); free(input); free(output); return 1; }
    /* Small random input, zero output: the same start fastText uses. */
    srand(1);
    for (int64_t i = 0; i < input_rows * dim; i++)
        input[i] = ((float)rand() / (float)RAND_MAX - 0.5f) / (float)dim;

    float *hidden = calloc((size_t)dim, sizeof(float));
    float *grad   = calloc((size_t)dim, sizeof(float));
    float *scores = calloc((size_t)nlabels, sizeof(float));
    int32_t feat[TRAIN_MAX_FEAT];

    int64_t seen = 0, total = (int64_t)nlines * TRAIN_EPOCHS;
    for (int epoch = 0; epoch < TRAIN_EPOCHS; epoch++) {
        rewind(f);
        while (fgets(line, sizeof(line), f)) {
            int nfeat = 0, label = -1;
            char *save = NULL, *tok = strtok_r(line, " \t\n\r", &save);
            while (tok) {
                if (strncmp(tok, "__label__", 9) == 0) {
                    int32_t id = train_dict_get(&d, tok, 1, 0);
                    if (id >= nwords) label = id - nwords;
                } else {
                    int32_t id = train_dict_get(&d, tok, 0, 0);
                    if (id >= 0 && id < nwords && nfeat < TRAIN_MAX_FEAT) feat[nfeat++] = id;
                }
                tok = strtok_r(NULL, " \t\n\r", &save);
            }
            if (label < 0 || nfeat == 0) continue;

            /* The rate decays with progress, so late examples nudge rather
             * than overwrite what the early ones taught. */
            float lr = TRAIN_LR * (1.0f - (float)seen / (float)total);
            if (lr < TRAIN_LR * 0.01f) lr = TRAIN_LR * 0.01f;
            seen++;

            for (int k = 0; k < dim; k++) hidden[k] = 0.0f;
            for (int i = 0; i < nfeat; i++) {
                const float *row = input + (int64_t)feat[i] * dim;
                for (int k = 0; k < dim; k++) hidden[k] += row[k];
            }
            for (int k = 0; k < dim; k++) hidden[k] /= (float)nfeat;

            float max = -1e30f;
            for (int32_t l = 0; l < nlabels; l++) {
                const float *row = output + (int64_t)l * dim;
                float s = 0.0f;
                for (int k = 0; k < dim; k++) s += row[k] * hidden[k];
                scores[l] = s;
                if (s > max) max = s;
            }
            float sum = 0.0f;
            for (int32_t l = 0; l < nlabels; l++) { scores[l] = expf(scores[l] - max); sum += scores[l]; }
            for (int32_t l = 0; l < nlabels; l++) scores[l] /= sum;

            for (int k = 0; k < dim; k++) grad[k] = 0.0f;
            for (int32_t l = 0; l < nlabels; l++) {
                float err = scores[l] - (l == label ? 1.0f : 0.0f);
                float *row = output + (int64_t)l * dim;
                for (int k = 0; k < dim; k++) {
                    grad[k] += err * row[k];
                    row[k]  -= lr * err * hidden[k];
                }
            }
            /* The hidden state was a mean, so its gradient is shared out again. */
            for (int i = 0; i < nfeat; i++) {
                float *row = input + (int64_t)feat[i] * dim;
                for (int k = 0; k < dim; k++) row[k] -= lr * grad[k] / (float)nfeat;
            }
        }
        fprintf(stderr, "[tag] epoch %d/%d\n", epoch + 1, TRAIN_EPOCHS);
    }
    fclose(f);

    /* Written in the format this organ already reads, so predict needs no
     * knowledge that the model was made here. */
    FILE *o = fopen(model_out, "wb");
    if (!o) { fprintf(stderr, "[tag] Cannot write model: %s\n", model_out); return 1; }
    int32_t magic = FT_MAGIC, version = FT_VERSION;
    int32_t z = 0, one = 1, dim32 = dim, epochs = TRAIN_EPOCHS, minc = TRAIN_MIN_COUNT;
    int32_t loss = 3, model_type = 1, bucket = TRAIN_BUCKET, lr_upd = 100;
    double t = 1e-4;
    fwrite(&magic, 4, 1, o); fwrite(&version, 4, 1, o);
    fwrite(&dim32, 4, 1, o); fwrite(&one, 4, 1, o);       /* dim, ws */
    fwrite(&epochs, 4, 1, o); fwrite(&minc, 4, 1, o);
    fwrite(&z, 4, 1, o);                                   /* neg */
    fwrite(&one, 4, 1, o);                                 /* word_ngrams */
    fwrite(&loss, 4, 1, o); fwrite(&model_type, 4, 1, o);
    fwrite(&bucket, 4, 1, o);
    fwrite(&z, 4, 1, o); fwrite(&z, 4, 1, o);              /* minn, maxn */
    fwrite(&lr_upd, 4, 1, o); fwrite(&t, 8, 1, o);

    int32_t dict_size = d.n;
    fwrite(&dict_size, 4, 1, o); fwrite(&nwords, 4, 1, o); fwrite(&nlabels, 4, 1, o);
    fwrite(&ntokens, 8, 1, o);
    int64_t pruneidx = 0;
    if (version >= 12) fwrite(&pruneidx, 8, 1, o);
    for (int32_t i = 0; i < d.n; i++) {
        fwrite(d.e[i].word, 1, strlen(d.e[i].word) + 1, o);
        fwrite(&d.e[i].count, 8, 1, o);
        fwrite(&d.e[i].type, 1, 1, o);
    }
    int8_t quant = 0;
    int64_t in_cols = dim, out_rows = nlabels;
    fwrite(&quant, 1, 1, o);
    fwrite(&input_rows, 8, 1, o); fwrite(&in_cols, 8, 1, o);
    fwrite(input, sizeof(float), (size_t)(input_rows * dim), o);
    fwrite(&quant, 1, 1, o);
    fwrite(&out_rows, 8, 1, o); fwrite(&in_cols, 8, 1, o);
    fwrite(output, sizeof(float), (size_t)((int64_t)nlabels * dim), o);
    fclose(o);

    fprintf(stderr, "[tag] Trained: %lld examples, %d words, %d labels, dim=%d → %s\n",
            (long long)nlines, nwords, nlabels, dim, model_out);
    free(input); free(output); free(hidden); free(grad); free(scores);
    train_dict_free(&d);
    return 0;
}

static int cmd_detect_lang(const char *text_file, const char *out_dir) {
    /* Find language ID model */
    const char *env = getenv("BONFYRE_LANGID_MODEL");
    static const char *paths[] = {
        NULL, /* placeholder for env */
        NULL, NULL, NULL
    };
    char home_path[PATH_MAX];
    const char *home = getenv("HOME");
    if (home) {
        snprintf(home_path, sizeof(home_path), "%s/.bonfyre/models/lid.176.bin", home);
        paths[1] = home_path;
    }
    paths[2] = "lid.176.bin";
    paths[3] = "/tmp/lid.176.bin";
    if (env && env[0]) paths[0] = env;

    const char *model_path = NULL;
    struct stat st2;
    for (int i = 0; i < 4; i++) {
        if (paths[i] && stat(paths[i], &st2) == 0) { model_path = paths[i]; break; }
    }
    if (!model_path) {
        fprintf(stderr, "[tag] lid.176.bin not found. Download from fasttext.cc/docs/en/language-identification.html\n");
        return 1;
    }

    FtModel lang_model;
    if (ft_model_load(&lang_model, model_path) != 0) return 1;
    if (lang_model.loss_type == 1 && ft_build_hs_tree(&lang_model) != 0) {
        fprintf(stderr, "[tag] Failed to build HS tree\n");
        ft_model_free(&lang_model); return 1;
    }

    ensure_dir(out_dir);
    char json_out[PATH_MAX];
    snprintf(json_out, sizeof(json_out), "%s/lang.json", out_dir);

    size_t tlen = 0;
    char *text = read_file_contents(text_file, &tlen);
    if (!text) { ft_model_free(&lang_model); return 1; }

    /* ── Chunked aggregation for language detection ──
     * Split into ~500 char chunks on word boundaries.
     * Predict each chunk, accumulate confidence per language.
     * This avoids the problem of a 20KB single-line input confusing fastText.
     */
    #define MAX_LANGS 10
    typedef struct { char lang[16]; double total_conf; int votes; } LangAccum;
    LangAccum accum[MAX_LANGS] = {0};
    int n_langs = 0;
    int n_chunks = 0;

    size_t chunk_size = 500;
    size_t pos = 0;
    while (pos < tlen) {
        /* Find a chunk boundary (word boundary near chunk_size) */
        size_t end = pos + chunk_size;
        if (end > tlen) end = tlen;
        else {
            /* Back up to a space */
            while (end > pos && text[end] != ' ' && text[end] != '\n') end--;
            if (end == pos) end = pos + chunk_size; /* no space found, use hard cut */
            if (end > tlen) end = tlen;
        }

        /* Prepare chunk: replace newlines with spaces */
        size_t clen = end - pos;
        char *chunk = malloc(clen + 1);
        if (!chunk) break;
        memcpy(chunk, text + pos, clen);
        chunk[clen] = '\0';
        for (size_t i = 0; i < clen; i++)
            if (chunk[i] == '\n') chunk[i] = ' ';

        /* Skip near-empty chunks */
        if (clen > 10) {
            FtPred results[5];
            int nresults = 0;
            ft_predict(&lang_model, chunk, 5, results, &nresults);

            for (int r = 0; r < nresults; r++) {
                const char *lname = ft_label_name(&lang_model, results[r].label_idx);
                /* Find or add to accum */
                int found = -1;
                for (int a = 0; a < n_langs; a++) {
                    if (strcmp(accum[a].lang, lname) == 0) { found = a; break; }
                }
                if (found < 0 && n_langs < MAX_LANGS) {
                    found = n_langs++;
                    snprintf(accum[found].lang, sizeof(accum[found].lang), "%s", lname);
                    accum[found].total_conf = 0;
                    accum[found].votes = 0;
                }
                if (found >= 0) {
                    accum[found].total_conf += results[r].score;
                    accum[found].votes++;
                }
            }
            n_chunks++;
        }
        free(chunk);
        pos = end;
        while (pos < tlen && (text[pos] == ' ' || text[pos] == '\n')) pos++;
    }

    /* Sort by total confidence descending */
    for (int i = 0; i < n_langs - 1; i++) {
        for (int j = i + 1; j < n_langs; j++) {
            if (accum[j].total_conf > accum[i].total_conf) {
                LangAccum tmp = accum[i]; accum[i] = accum[j]; accum[j] = tmp;
            }
        }
    }
    int top_n = n_langs < 5 ? n_langs : 5;

    FILE *out = fopen(json_out, "w");
    if (out) {
        fprintf(out, "{\"type\": \"language-detection\", \"source\": \"%s\", \"chunks\": %d, \"languages\": [", text_file, n_chunks);
        for (int i = 0; i < top_n; i++) {
            double avg = n_chunks > 0 ? accum[i].total_conf / n_chunks : 0;
            fprintf(out, "%s{\"language\": \"%s\", \"confidence\": %.4f, \"votes\": %d}",
                    i ? ", " : "", accum[i].lang, avg, accum[i].votes);
        }
        fprintf(out, "]}\n");
        fclose(out);
    }

    printf("{\"type\": \"language-detection\", \"source\": \"%s\", \"chunks\": %d, \"languages\": [", text_file, n_chunks);
    for (int i = 0; i < top_n; i++) {
        double avg = n_chunks > 0 ? accum[i].total_conf / n_chunks : 0;
        printf("%s{\"language\": \"%s\", \"confidence\": %.4f, \"votes\": %d}",
               i ? ", " : "", accum[i].lang, avg, accum[i].votes);
    }
    printf("]}\n");

    free(text);
    ft_model_free(&lang_model);
    return 0;
}

/* ── main ───────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-tag — instant topic/intent tagging (fastText, pure C)\n\n"
        "Usage:\n"
        "  bonfyre-tag predict <model.bin> <text-file> [output-dir] [--top N]\n"
        "  bonfyre-tag batch <model.bin> <dir> [output-dir]\n"
        "  bonfyre-tag train <training-data> <model-out>\n"
        "  bonfyre-tag detect-lang <text-file> [output-dir]\n"
        "  bonfyre-tag status\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { print_usage(); return 0; }
    enum { TG_STATUS };
    static const BfCommandEntry tag_commands[] = {
        {"status", TG_STATUS, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(tag_commands,
        sizeof(tag_commands) / sizeof(tag_commands[0]), argv[1]);
    if (command && command->id == TG_STATUS) {
        printf("{\"binary\":\"bonfyre-tag\",\"status\":\"ok\",\"version\":\"2.0.0\"}\n");
        return 0;
    }

    if (argc < 3) { print_usage(); return 1; }

    const char *cmd = argv[1];

    if (strcmp(cmd, "predict") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        const char *model_path = argv[2];
        const char *text = argv[3];
        const char *out = (argc > 4 && argv[4][0] != '-') ? argv[4] : "output";
        int top_k = 3;
        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--top") == 0 && i + 1 < argc)
                top_k = atoi(argv[++i]);
        }
        FtModel model;
        if (ft_model_load(&model, model_path) != 0) return 1;
        if (model.loss_type == 1 && ft_build_hs_tree(&model) != 0) {
            ft_model_free(&model); return 1;
        }
        int rc = cmd_predict(model_path, text, out, top_k, &model);
        ft_model_free(&model);
        return rc;
    } else if (strcmp(cmd, "batch") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        const char *model_path = argv[2];
        FtModel model;
        if (ft_model_load(&model, model_path) != 0) return 1;
        if (model.loss_type == 1 && ft_build_hs_tree(&model) != 0) {
            ft_model_free(&model); return 1;
        }
        int rc = cmd_batch(model_path, argv[3], (argc > 4) ? argv[4] : "output", &model);
        ft_model_free(&model);
        return rc;
    } else if (strcmp(cmd, "train") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_train(argv[2], argv[3]);
    } else if (strcmp(cmd, "detect-lang") == 0) {
        return cmd_detect_lang(argv[2], (argc > 3) ? argv[3] : "output");
    }

    print_usage();
    return 1;
}
