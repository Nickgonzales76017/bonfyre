/*
 * BonfyreTag — instant intent/topic tagging via fastText (pure C).
 *
 * Zero Python dependencies for inference. Loads .bin models natively.
 * 2ms, no GPU. Classify any text into topics, intents, or custom labels.
 * Train custom models on your own transcript corpus (still uses Python).
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
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <bonfyre.h>

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
    float   *output;      /* nlabels × dim */
    int64_t  input_rows;
    int64_t  output_rows;
    /* label index (into entries) */
    int32_t *label_ids;   /* nlabels indices into entries[] */
} FtModel;

static uint32_t ft_hash(const char *str, size_t len) {
    uint32_t h = FT_HASH_SEED;
    for (size_t i = 0; i < len; i++) {
        h ^= (uint32_t)(unsigned char)str[i];
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

    int8_t pruneidx_size = 0;
    if (version >= 12) { if (fread(&pruneidx_size, 1, 1, f) != 1) goto fail; }

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
        for (int8_t i = 0; i < pruneidx_size; i++) {
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
    fprintf(stderr, "[tag] Loaded model: %d words, %d labels, dim=%d, bucket=%d\n",
            m->nwords, m->nlabels, m->args.dim, m->args.bucket);
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
    memset(m, 0, sizeof(*m));
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

    /* Tokenize: split on whitespace, lowercase */
    size_t tlen = strlen(text);
    char *tmp = malloc(tlen + 1);
    if (!tmp) { free(hidden); return -1; }
    memcpy(tmp, text, tlen + 1);

    /* Lowercase */
    for (size_t i = 0; i < tlen; i++)
        tmp[i] = (char)tolower((unsigned char)tmp[i]);

    int32_t *ids = malloc(sizeof(int32_t) * (tlen + 1) * 20);  /* generous cap */
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
            /* word found: add word vector index */
            if (nids < id_cap) ids[nids++] = wid;
        }
        /* Add subword ngrams regardless */
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

    /* Multiply by output matrix: score[l] = output[l] · hidden */
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

    /* Softmax */
    float maxs = preds[0].score;
    for (int l = 1; l < nl; l++) if (preds[l].score > maxs) maxs = preds[l].score;
    float sum = 0;
    for (int l = 0; l < nl; l++) {
        preds[l].score = expf(preds[l].score - maxs);
        sum += preds[l].score;
    }
    if (sum > 0) for (int l = 0; l < nl; l++) preds[l].score /= sum;

    /* Top-k */
    qsort(preds, (size_t)nl, sizeof(FtPred), ft_pred_cmp);
    *nout = (top_k < nl) ? top_k : nl;
    memcpy(out, preds, sizeof(FtPred) * (size_t)*nout);
    free(preds);
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

/* ── Python wrapper for fastText training ─────────────────────── */

static int run_cmd(const char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return -1; }
    if (pid == 0) { execvp(argv[0], (char *const *)argv); _exit(127); }
    int st; waitpid(pid, &st, 0);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static int run_python_script(const char *script) {
    char tmp_path[PATH_MAX];
    snprintf(tmp_path, sizeof(tmp_path), "/tmp/bonfyre_tag_%d.py", getpid());
    FILE *f = fopen(tmp_path, "w");
    if (!f) { perror("fopen"); return -1; }
    fputs(script, f);
    fclose(f);

    const char *python = getenv("BONFYRE_PYTHON3");
    if (!python) python = "python3";
    const char *argv[] = { python, tmp_path, NULL };
    int rc = run_cmd(argv);
    unlink(tmp_path);
    return rc;
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

static int cmd_train(const char *training_data, const char *model_out) {
    fprintf(stderr, "[tag] Training model from %s -> %s\n", training_data, model_out);

    char script[2048];
    snprintf(script, sizeof(script),
        "import fasttext, warnings\n"
        "warnings.filterwarnings('ignore')\n"
        "model = fasttext.train_supervised(\n"
        "    input='%s',\n"
        "    epoch=25,\n"
        "    lr=1.0,\n"
        "    wordNgrams=2,\n"
        "    dim=100,\n"
        "    loss='softmax'\n"
        ")\n"
        "model.save_model('%s')\n"
        "result = model.test('%s')\n"
        "print(f'Trained: {result[0]} samples, precision={result[1]:.4f}, recall={result[2]:.4f}')\n",
        training_data, model_out, training_data);

    return run_python_script(script);
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

    ensure_dir(out_dir);
    char json_out[PATH_MAX];
    snprintf(json_out, sizeof(json_out), "%s/lang.json", out_dir);

    size_t tlen = 0;
    char *text = read_file_contents(text_file, &tlen);
    if (!text) { ft_model_free(&lang_model); return 1; }

    /* Collapse newlines, truncate to 5000 chars */
    for (size_t i = 0; i < tlen; i++) if (text[i] == '\n') text[i] = ' ';
    if (tlen > 5000) { text[5000] = '\0'; tlen = 5000; }

    FtPred results[5];
    int nresults = 0;
    ft_predict(&lang_model, text, 5, results, &nresults);

    FILE *out = fopen(json_out, "w");
    if (out) {
        fprintf(out, "{\"type\": \"language-detection\", \"source\": \"%s\", \"languages\": [", text_file);
        for (int i = 0; i < nresults; i++) {
            fprintf(out, "%s{\"language\": \"%s\", \"confidence\": %.4f}",
                    i ? ", " : "",
                    ft_label_name(&lang_model, results[i].label_idx),
                    results[i].score);
        }
        fprintf(out, "]}\n");
        fclose(out);
    }

    /* Also print to stdout */
    printf("{\"type\": \"language-detection\", \"source\": \"%s\", \"languages\": [", text_file);
    for (int i = 0; i < nresults; i++) {
        printf("%s{\"language\": \"%s\", \"confidence\": %.4f}",
               i ? ", " : "",
               ft_label_name(&lang_model, results[i].label_idx),
               results[i].score);
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
    if (argc >= 2 && strcmp(argv[1], "status") == 0) {
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
        int rc = cmd_predict(model_path, text, out, top_k, &model);
        ft_model_free(&model);
        return rc;
    } else if (strcmp(cmd, "batch") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        const char *model_path = argv[2];
        FtModel model;
        if (ft_model_load(&model, model_path) != 0) return 1;
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
