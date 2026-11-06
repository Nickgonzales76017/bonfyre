/*
 * static_embed.h — static embeddings distilled from the ONNX teacher.
 *
 * A sentence model is run once per vocabulary token ([CLS] token [SEP], the
 * teacher's own pooling), the token vectors are reduced by PCA, weighted by
 * vocabulary rank (Zipf) and stored as int8 rows with one scale each. A text
 * is then embedded without the model: tokenize, average the rows of its
 * tokens, normalise. This is the Model2Vec recipe, done with the native
 * tokenizer and runtime so nothing is downloaded.
 *
 * The .bfse file carries everything a reader needs to reproduce a vector
 * exactly, including the tokenizer's vocabulary and Unicode fold table, so a
 * browser reader (components/BonfyreServiceSite/static-embed.mjs) gives the
 * same numbers as `bonfyre-embed static` without knowing the model.
 *
 * Layout, little-endian:
 *   "BFSE" u32 version=1 u32 vocab u32 dims u32 cls u32 sep u32 unk
 *   u32 vocab_bytes u32 fold_count u32 fold_bytes u32 reserved
 *   vocab: tokens joined by '\n'
 *   fold:  fold_count x { u32 cp, u8 len, len bytes }
 *   pad to 4; float32 scale[vocab]; int8 row[vocab][dims]
 * Included by main.c after the tokenizer and ONNX helpers it reuses.
 */

#define BFSE_VERSION 1u
#define ORT_CHECK(expr) do { \
    OrtStatus *_s = (expr); \
    if (_s) { fprintf(stderr, "[embed] ORT: %s\n", api->GetErrorMessage(_s)); api->ReleaseStatus(_s); goto ort_fail; } \
} while(0)
#define DISTILL_SEQ 3

static int bfse_special(const char *token) {
    size_t n = strlen(token);
    return n >= 2 && token[0] == '[' && token[n - 1] == ']';
}

/* One forward pass over n tokens shaped [n, 3]; out receives n x hidden. */
static int ort_embed_token_batch(OrtContext *ctx, const int *ids, int n, int cls, int sep, float *out, int *hidden_out) {
    const OrtApi *api = ctx->api;
    size_t cells = (size_t)n * DISTILL_SEQ;
    int64_t *tok = malloc(cells * sizeof(int64_t)), *mask = malloc(cells * sizeof(int64_t)), *types = calloc(cells, sizeof(int64_t));
    OrtValue *t_ids = NULL, *t_mask = NULL, *t_types = NULL, *output = NULL;
    int rc = -1;
    if (!tok || !mask || !types) goto done;
    for (int i = 0; i < n; i++) {
        tok[i * 3] = cls; tok[i * 3 + 1] = ids[i]; tok[i * 3 + 2] = sep;
        mask[i * 3] = mask[i * 3 + 1] = mask[i * 3 + 2] = 1;
    }
    int64_t shape[2] = {n, DISTILL_SEQ};
    ORT_CHECK(api->CreateTensorWithDataAsOrtValue(ctx->mem, tok, cells * sizeof(int64_t), shape, 2, ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64, &t_ids));
    ORT_CHECK(api->CreateTensorWithDataAsOrtValue(ctx->mem, mask, cells * sizeof(int64_t), shape, 2, ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64, &t_mask));
    ORT_CHECK(api->CreateTensorWithDataAsOrtValue(ctx->mem, types, cells * sizeof(int64_t), shape, 2, ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64, &t_types));
    {
        const char *in_names[] = {"input_ids", "attention_mask", "token_type_ids"};
        const char *out_names[] = {"last_hidden_state"};
        const OrtValue *inputs[] = {t_ids, t_mask, t_types};
        ORT_CHECK(api->Run(ctx->session, NULL, in_names, inputs, 3, out_names, 1, &output));
    }
    {
        OrtTensorTypeAndShapeInfo *info = NULL;
        size_t ndims; int64_t oshape[4];
        api->GetTensorTypeAndShape(output, &info);
        api->GetDimensionsCount(info, &ndims); api->GetDimensions(info, oshape, ndims);
        api->ReleaseTensorTypeAndShapeInfo(info);
        int hidden = (int)oshape[ndims - 1];
        float *raw = NULL;
        api->GetTensorMutableData(output, (void **)&raw);
        for (int i = 0; i < n; i++)
            for (int d = 0; d < hidden; d++) {
                float sum = 0;
                for (int t = 0; t < DISTILL_SEQ; t++) sum += raw[((size_t)i * DISTILL_SEQ + t) * hidden + d];
                out[(size_t)i * hidden + d] = sum / DISTILL_SEQ;
            }
        *hidden_out = hidden;
    }
    rc = 0;
    goto done;
ort_fail:
done:
    if (output) api->ReleaseValue(output);
    if (t_ids) api->ReleaseValue(t_ids);
    if (t_mask) api->ReleaseValue(t_mask);
    if (t_types) api->ReleaseValue(t_types);
    free(tok); free(mask); free(types);
    return rc;
}

/* Symmetric eigen-decomposition by cyclic Jacobi rotations. a (n x n) is
 * destroyed; values[n] and vectors[n x n] (columns) are filled, sorted by
 * value descending. */
static void jacobi_eigen(double *a, int n, double *values, double *vectors) {
    for (int i = 0; i < n; i++) for (int j = 0; j < n; j++) vectors[(size_t)i * n + j] = i == j;
    for (int sweep = 0; sweep < 30; sweep++) {
        double off = 0;
        for (int p = 0; p < n; p++) for (int q = p + 1; q < n; q++) off += a[(size_t)p * n + q] * a[(size_t)p * n + q];
        if (off < 1e-22) break;
        for (int p = 0; p < n; p++)
            for (int q = p + 1; q < n; q++) {
                double apq = a[(size_t)p * n + q];
                if (fabs(apq) < 1e-30) continue;
                double app = a[(size_t)p * n + p], aqq = a[(size_t)q * n + q];
                double theta = (aqq - app) / (2 * apq);
                double t = (theta >= 0 ? 1 : -1) / (fabs(theta) + sqrt(theta * theta + 1));
                double c = 1 / sqrt(t * t + 1), s = t * c;
                for (int k = 0; k < n; k++) {
                    double akp = a[(size_t)k * n + p], akq = a[(size_t)k * n + q];
                    a[(size_t)k * n + p] = c * akp - s * akq; a[(size_t)k * n + q] = s * akp + c * akq;
                }
                for (int k = 0; k < n; k++) {
                    double apk = a[(size_t)p * n + k], aqk = a[(size_t)q * n + k];
                    a[(size_t)p * n + k] = c * apk - s * aqk; a[(size_t)q * n + k] = s * apk + c * aqk;
                }
                for (int k = 0; k < n; k++) {
                    double vkp = vectors[(size_t)k * n + p], vkq = vectors[(size_t)k * n + q];
                    vectors[(size_t)k * n + p] = c * vkp - s * vkq; vectors[(size_t)k * n + q] = s * vkp + c * vkq;
                }
            }
    }
    for (int i = 0; i < n; i++) values[i] = a[(size_t)i * n + i];
    /* selection sort of columns by value, descending */
    for (int i = 0; i < n; i++) {
        int best = i;
        for (int j = i + 1; j < n; j++) if (values[j] > values[best]) best = j;
        if (best == i) continue;
        double tv = values[i]; values[i] = values[best]; values[best] = tv;
        for (int k = 0; k < n; k++) { double t = vectors[(size_t)k * n + i]; vectors[(size_t)k * n + i] = vectors[(size_t)k * n + best]; vectors[(size_t)k * n + best] = t; }
    }
}

static int write_all(FILE *f, const void *p, size_t n) { return fwrite(p, 1, n, f) == n ? 0 : -1; }
static int write_u32(FILE *f, uint32_t v) { unsigned char b[4] = {v, v >> 8, v >> 16, v >> 24}; return write_all(f, b, 4); }

static int cmd_embed_distill_(int argc, char **argv) {
    const char *model = "nomic-embed-text-onnx", *out_path = NULL, *manifest = NULL;
    int dims = 128, batch = 256, limit = 0, zipf = 1;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--model") && i + 1 < argc) model = argv[++i];
        else if (!strcmp(argv[i], "--out") && i + 1 < argc) out_path = argv[++i];
        else if (!strcmp(argv[i], "--manifest") && i + 1 < argc) manifest = argv[++i];
        else if (!strcmp(argv[i], "--dims") && i + 1 < argc) dims = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--batch") && i + 1 < argc) batch = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--limit") && i + 1 < argc) limit = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--no-zipf")) zipf = 0;
        else { fprintf(stderr, "distill: unknown argument %s\n", argv[i]); return 2; }
    }
    if (!out_path || dims < 8 || dims > 1024 || batch < 1 || batch > 4096) {
        fprintf(stderr, "usage: bonfyre-embed distill --out model.bfse [--model nomic-embed-text-onnx] [--dims 128] [--batch 256] [--manifest m.json] [--no-zipf]\n");
        return 2;
    }
    const char *model_dir = resolve_model_dir(model);
    char vocab_path[PATH_MAX];
    snprintf(vocab_path, sizeof vocab_path, "%s/vocab.txt", model_dir);
    Vocab vocab = {0};
    if (vocab_load(&vocab, vocab_path) != 0) { fprintf(stderr, "distill: no vocab.txt in %s\n", model_dir); return 1; }
    int cls = vocab_lookup(&vocab, "[CLS]"), sep = vocab_lookup(&vocab, "[SEP]"), unk = vocab_lookup(&vocab, "[UNK]");
    if (cls < 0 || sep < 0 || unk < 0) { fprintf(stderr, "distill: vocabulary lacks [CLS], [SEP] or [UNK]\n"); vocab_free(&vocab); return 1; }
    OrtContext ort;
    if (ort_session_open(&ort, model_dir) != 0) { vocab_free(&vocab); return 1; }

    int count = vocab.count, use = limit > 0 && limit < count ? limit : count, hidden = 0;
    int *ids = malloc(sizeof(int) * (size_t)use);
    int real = 0;
    for (int i = 0; i < use; i++) if (!bfse_special(vocab.tokens[i])) ids[real++] = i;
    float *teacher = NULL, *buf = NULL;
    for (int at = 0; at < real; at += batch) {
        int n = real - at < batch ? real - at : batch;
        if (!buf) {
            float probe[4096];
            if (ort_embed_token_batch(&ort, ids, 1, cls, sep, probe, &hidden) != 0 || hidden < dims) { fprintf(stderr, "distill: teacher run failed or hidden < dims\n"); return 1; }
            teacher = malloc(sizeof(float) * (size_t)real * hidden);
            buf = malloc(sizeof(float) * (size_t)batch * hidden);
        }
        if (ort_embed_token_batch(&ort, ids + at, n, cls, sep, buf, &hidden) != 0) { fprintf(stderr, "distill: teacher run failed at token %d\n", ids[at]); return 1; }
        memcpy(teacher + (size_t)at * hidden, buf, sizeof(float) * (size_t)n * hidden);
        if ((at / batch) % 20 == 0) fprintf(stderr, "[distill] %d / %d tokens\n", at + n, real);
    }
    ort_session_close(&ort);
    free(buf);

    /* PCA over the real tokens. */
    double *mean = calloc((size_t)hidden, sizeof(double)), *cov = calloc((size_t)hidden * hidden, sizeof(double));
    for (int r = 0; r < real; r++) for (int d = 0; d < hidden; d++) mean[d] += teacher[(size_t)r * hidden + d];
    for (int d = 0; d < hidden; d++) mean[d] /= real;
    double *row = malloc(sizeof(double) * (size_t)hidden);
    for (int r = 0; r < real; r++) {
        for (int d = 0; d < hidden; d++) row[d] = teacher[(size_t)r * hidden + d] - mean[d];
        for (int i = 0; i < hidden; i++) { double ri = row[i]; if (ri == 0) continue; double *c = cov + (size_t)i * hidden; for (int j = i; j < hidden; j++) c[j] += ri * row[j]; }
    }
    for (int i = 0; i < hidden; i++) for (int j = i; j < hidden; j++) { cov[(size_t)i * hidden + j] /= (real - 1); cov[(size_t)j * hidden + i] = cov[(size_t)i * hidden + j]; }
    double *values = malloc(sizeof(double) * (size_t)hidden), *vectors = malloc(sizeof(double) * (size_t)hidden * hidden);
    fprintf(stderr, "[distill] PCA %d -> %d over %d tokens\n", hidden, dims, real);
    jacobi_eigen(cov, hidden, values, vectors);
    double total = 0, kept = 0;
    for (int i = 0; i < hidden; i++) { total += values[i] > 0 ? values[i] : 0; if (i < dims) kept += values[i] > 0 ? values[i] : 0; }

    /* Project, weight, quantize. Special tokens keep a zero row. */
    float *scales = calloc((size_t)count, sizeof(float));
    int8_t *rows = calloc((size_t)count * dims, 1);
    float *projected = malloc(sizeof(float) * (size_t)dims);
    for (int r = 0; r < real; r++) {
        int id = ids[r];
        double weight = zipf ? log((double)id + 2.0) : 1.0, top = 0;
        for (int d = 0; d < hidden; d++) row[d] = teacher[(size_t)r * hidden + d] - mean[d];
        for (int k = 0; k < dims; k++) {
            double s = 0;
            for (int d = 0; d < hidden; d++) s += row[d] * vectors[(size_t)d * hidden + k];
            projected[k] = (float)(s * weight);
            if (fabs(projected[k]) > top) top = fabs(projected[k]);
        }
        float scale = top > 0 ? (float)(top / 127.0) : 1.0f;
        scales[id] = scale;
        for (int k = 0; k < dims; k++) { long q = lround(projected[k] / scale); rows[(size_t)id * dims + k] = (int8_t)(q > 127 ? 127 : q < -127 ? -127 : q); }
    }

    FILE *f = fopen(out_path, "wb");
    if (!f) { fprintf(stderr, "distill: cannot write %s\n", out_path); return 1; }
    size_t vocab_bytes = 0;
    for (int i = 0; i < count; i++) vocab_bytes += strlen(vocab.tokens[i]) + (i + 1 < count);
    size_t fold_bytes = 0;
    for (size_t i = 0; i < BERT_FOLD_COUNT; i++) fold_bytes += 5 + strlen(BERT_FOLD[i].out);
    int ok = write_all(f, "BFSE", 4) | write_u32(f, BFSE_VERSION) | write_u32(f, (uint32_t)count) | write_u32(f, (uint32_t)dims) |
             write_u32(f, (uint32_t)cls) | write_u32(f, (uint32_t)sep) | write_u32(f, (uint32_t)unk) |
             write_u32(f, (uint32_t)vocab_bytes) | write_u32(f, (uint32_t)BERT_FOLD_COUNT) | write_u32(f, (uint32_t)fold_bytes) | write_u32(f, 0);
    for (int i = 0; i < count; i++) { ok |= write_all(f, vocab.tokens[i], strlen(vocab.tokens[i])); if (i + 1 < count) ok |= write_all(f, "\n", 1); }
    for (size_t i = 0; i < BERT_FOLD_COUNT; i++) {
        unsigned char len = (unsigned char)strlen(BERT_FOLD[i].out);
        ok |= write_u32(f, BERT_FOLD[i].cp) | write_all(f, &len, 1) | (len ? write_all(f, BERT_FOLD[i].out, len) : 0);
    }
    long pos = ftell(f);
    while (pos % 4) { ok |= write_all(f, "\0", 1); pos++; }
    for (int i = 0; i < count; i++) { uint32_t bits; memcpy(&bits, &scales[i], 4); ok |= write_u32(f, bits); }
    ok |= write_all(f, rows, (size_t)count * dims);
    if (fclose(f) != 0 || ok) { fprintf(stderr, "distill: write failed\n"); return 1; }

    char onnx_path[PATH_MAX];
    resolve_onnx_path(model_dir, onnx_path, sizeof onnx_path);
    if (manifest) {
        FILE *m = fopen(manifest, "w");
        if (m) {
            fprintf(m, "{\"schema\":\"bonfyre.static-embedding.v1\",\"teacher_model\":\"%s\",\"teacher_onnx\":\"%s\",\"vocab\":%d,\"embedded_tokens\":%d,"
                       "\"teacher_dims\":%d,\"dims\":%d,\"explained_variance\":%.6f,\"weighting\":\"%s\",\"quantization\":\"int8 per-row scale\","
                       "\"tokenizer\":\"BERT uncased WordPiece, ASCII trie, Unicode fold table carried in the file\","
                       "\"cannot_infer\":[\"a static vector -> the teacher's sentence vector; word order and context are gone\","
                       "\"similar scores -> the same meaning; scores compare only within one .bfse file\"]}\n",
                    model, onnx_path, count, real, hidden, dims, total > 0 ? kept / total : 0, zipf ? "zipf by vocabulary rank, log(id + 2)" : "none");
            fclose(m);
        }
    }
    printf("{\"out\":\"%s\",\"vocab\":%d,\"embedded_tokens\":%d,\"teacher_dims\":%d,\"dims\":%d,\"explained_variance\":%.4f}\n",
           out_path, count, real, hidden, dims, total > 0 ? kept / total : 0);
    free(teacher); free(mean); free(cov); free(row); free(values); free(vectors); free(scales); free(rows); free(projected); free(ids);
    vocab_free(&vocab);
    return 0;
}

#undef ORT_CHECK

/* bonfyre-embed static --model m.bfse (--text file | --input-jsonl f) --out f [--index-out f.bfsi]
 * One vector as JSON, or a JSONL line per input with id, tokens and vector.
 * With --index-out the inputs also become a .bfsi search index (rows in input
 * order); an input the model cannot see is refused rather than indexed, since
 * a zero vector would match nothing and say nothing. The reader is the shared
 * libbonfyre bf_static_embed, the same one the CMS and web address use. */
static int cmd_embed_static_(int argc, char **argv) {
    const char *model = NULL, *text_path = NULL, *jsonl = NULL, *out_path = NULL, *index_out = NULL, *text_field = "text", *id_field = "id";
    int store = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--model") && i + 1 < argc) model = argv[++i];
        else if (!strcmp(argv[i], "--text") && i + 1 < argc) text_path = argv[++i];
        else if (!strcmp(argv[i], "--input-jsonl") && i + 1 < argc) jsonl = argv[++i];
        else if (!strcmp(argv[i], "--out") && i + 1 < argc) out_path = argv[++i];
        else if (!strcmp(argv[i], "--index-out") && i + 1 < argc) index_out = argv[++i];
        else if (!strcmp(argv[i], "--text-field") && i + 1 < argc) text_field = argv[++i];
        else if (!strcmp(argv[i], "--id-field") && i + 1 < argc) id_field = argv[++i];
        else if (!strcmp(argv[i], "--store")) store = 1;
        else { fprintf(stderr, "static: unknown argument %s\n", argv[i]); return 2; }
    }
    if (!model || !out_path || (!text_path == !jsonl) || ((index_out || store) && !jsonl)) {
        fprintf(stderr, "usage: bonfyre-embed static --model m.bfse (--text file | --input-jsonl file [--index-out index.bfsi] [--store]) --out file\n");
        return 2;
    }
    char err[256];
    BfStaticModel *sm = bf_static_load(model, err, sizeof err);
    if (!sm) { fprintf(stderr, "static: %s: %s\n", model, err); return 1; }
    int dims = bf_static_dims(sm), rc = 0;
    float *vec = malloc(sizeof(float) * (size_t)dims);
    float *kept = NULL; size_t kept_n = 0, kept_cap = 0;
    FILE *out = fopen(out_path, "w");
    if (!out) { fprintf(stderr, "static: cannot write %s\n", out_path); bf_static_free(sm); free(vec); return 1; }
    if (text_path) {
        size_t len = 0;
        char *text = read_file_contents(text_path, &len);
        if (!text) { fprintf(stderr, "static: cannot read %s\n", text_path); rc = 1; goto end; }
        int used = bf_static_embed(sm, text, vec);
        fprintf(out, "{\"dims\":%d,\"tokens\":%d,\"vector\":[", dims, used);
        for (int d = 0; d < dims; d++) fprintf(out, "%s%.8g", d ? "," : "", vec[d]);
        fprintf(out, "]}\n");
        free(text);
    } else {
        JsonlReader reader;
        if (jsonl_reader_open(&reader, text_field, id_field) != 0) { rc = 1; goto end; }
        FILE *in = fopen(jsonl, "r");
        if (!in) { fprintf(stderr, "static: cannot read %s\n", jsonl); jsonl_reader_close(&reader); rc = 1; goto end; }
        char *line = NULL; size_t cap = 0; ssize_t got; long refused = 0;
        while ((got = getline(&line, &cap, in)) > 0) {
            char *id = NULL, *text = NULL;
            if (jsonl_reader_extract(&reader, line, &id, &text) != 1) continue;
            int used = bf_static_embed(sm, text, vec);
            /* --store files each visible vector in the shared object store under
             * sha256(schema, model digest, text), so `embed pack` can pack them
             * without colliding with ONNX cache entries for the same text. */
            if (store && used) {
                static const char schema[] = "bonfyre.embed.static-store.v1";
                uint8_t key[32]; BfSha256 kc;
                bf_sha256_init(&kc);
                bf_sha256_update(&kc, (const uint8_t *)schema, sizeof schema);
                bf_sha256_update(&kc, bf_static_model_sha256(sm), 32);
                bf_sha256_update(&kc, (const uint8_t *)text, strlen(text));
                bf_sha256_final(&kc, key);
                bf_embed_store(key, vec, (uint32_t)dims);
            }
            fputs("{\"id\":\"", out);
            for (const char *p = id; *p; p++) { if (*p == '"' || *p == '\\') fputc('\\', out); fputc(*p, out); }
            fprintf(out, "\",\"tokens\":%d,\"indexed\":%s,\"vector\":[", used, index_out && used ? "true" : "false");
            for (int d = 0; d < dims; d++) fprintf(out, "%s%.8g", d ? "," : "", vec[d]);
            fprintf(out, "]}\n");
            if (index_out) {
                if (!used) refused++;
                else {
                    if (kept_n == kept_cap) { kept_cap = kept_cap ? kept_cap * 2 : 256; kept = realloc(kept, sizeof(float) * kept_cap * (size_t)dims); }
                    memcpy(kept + kept_n * (size_t)dims, vec, sizeof(float) * (size_t)dims); kept_n++;
                }
            }
            free(id); free(text);
        }
        free(line); fclose(in); jsonl_reader_close(&reader);
        if (index_out) {
            if (bf_static_index_write(index_out, sm, (int)kept_n, kept) != 0) { fprintf(stderr, "static: cannot write %s\n", index_out); rc = 1; }
            else fprintf(stderr, "{\"index\":\"%s\",\"rows\":%zu,\"refused_invisible\":%ld}\n", index_out, kept_n, refused);
        }
    }
end:
    fclose(out); free(vec); free(kept); bf_static_free(sm);
    return rc;
}

/*
 * agree — how far a distilled .bfse ranks a corpus the way its teacher does.
 *
 * Model Commons could name a static embedding and its teacher but could not
 * say how close the two were on anything this estate searches: the distill
 * receipt records explained variance of token rows, which is not a ranking.
 * This embeds one bounded corpus with the ONNX teacher and with the .bfse,
 * ranks every text's neighbours under each, and reports the overlap of the
 * two top-k lists. The teacher load asks the memory ledger first and the
 * grant is released when the session closes.
 *
 * Only texts both representations can see are compared; the rest are counted.
 * The receipt names the exact bytes of graph, vocabulary, .bfse and corpus so
 * a reader can tell which pair it describes, and nothing else.
 */
#include <time.h>
#ifdef __APPLE__
#include <libproc.h>
#endif
#include <bf_admission.h>
#include <bf_jsonw.h>

static double agree_now_ms(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec * 1000.0 + (double)t.tv_nsec / 1e6;
}

static void agree_topk(const float *v, int n, int dims, int q, int k, int *out) {
    /* insertion into a k-long list, highest cosine first; ties keep lower index */
    float best[64]; int m = 0;
    const float *a = v + (size_t)q * dims;
    for (int j = 0; j < n; j++) {
        if (j == q) continue;
        const float *b = v + (size_t)j * dims;
        float s = 0;
        for (int d = 0; d < dims; d++) s += a[d] * b[d];
        if (m == k && s <= best[k - 1]) continue;
        int at = m < k ? m++ : k - 1;
        while (at > 0 && best[at - 1] < s) { best[at] = best[at - 1]; out[at] = out[at - 1]; at--; }
        best[at] = s; out[at] = j;
    }
}

static int cmd_embed_agree_(int argc, char **argv) {
    const char *model = "nomic-embed-text-onnx", *static_path = NULL, *jsonl = NULL, *out_path = NULL;
    const char *text_field = "text", *id_field = "id", *ledger_env = getenv("BONFYRE_ADMISSION_DB");
    int k = 10, limit = 2000;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--model") && i + 1 < argc) model = argv[++i];
        else if (!strcmp(argv[i], "--static") && i + 1 < argc) static_path = argv[++i];
        else if (!strcmp(argv[i], "--input-jsonl") && i + 1 < argc) jsonl = argv[++i];
        else if (!strcmp(argv[i], "--out") && i + 1 < argc) out_path = argv[++i];
        else if (!strcmp(argv[i], "--k") && i + 1 < argc) k = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--limit") && i + 1 < argc) limit = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--text-field") && i + 1 < argc) text_field = argv[++i];
        else if (!strcmp(argv[i], "--id-field") && i + 1 < argc) id_field = argv[++i];
        else if (!strcmp(argv[i], "--admission-db") && i + 1 < argc) ledger_env = argv[++i];
        else { fprintf(stderr, "agree: unknown argument %s\n", argv[i]); return 2; }
    }
    if (!static_path || !jsonl || !out_path || k < 1 || k > 64 || limit < 2 || limit > 20000) {
        fprintf(stderr, "usage: bonfyre-embed agree --static m.bfse --input-jsonl corpus.jsonl --out receipt.json "
                        "[--model nomic-embed-text-onnx] [--k 10] [--limit 2000] [--admission-db FILE]\n");
        return 2;
    }
    char err[256], ledger[PATH_MAX], onnx_path[PATH_MAX], vocab_path[PATH_MAX];
    char onnx_hex[65] = "", vocab_hex[65] = "", static_hex[65] = "", corpus_hex[65] = "";
    const char *home = getenv("HOME") ? getenv("HOME") : ".";
    if (ledger_env && *ledger_env) snprintf(ledger, sizeof ledger, "%s", ledger_env);
    else snprintf(ledger, sizeof ledger, "%s/.bonfyre/admission.db", home);
    const char *model_dir = resolve_model_dir(model);
    snprintf(vocab_path, sizeof vocab_path, "%s/vocab.txt", model_dir);
    struct stat onnx_st;
    if (resolve_onnx_path(model_dir, onnx_path, sizeof onnx_path) != 0 || stat(onnx_path, &onnx_st) != 0) {
        fprintf(stderr, "agree: no ONNX graph for %s in %s\n", model, model_dir); return 1;
    }
    if (bf_sha256_file(onnx_path, onnx_hex) != 0 || bf_sha256_file(vocab_path, vocab_hex) != 0 ||
        bf_sha256_file(static_path, static_hex) != 0 || bf_sha256_file(jsonl, corpus_hex) != 0) {
        fprintf(stderr, "agree: cannot hash graph, vocabulary, .bfse or corpus\n"); return 1;
    }
    BfStaticModel *sm = bf_static_load(static_path, err, sizeof err);
    if (!sm) { fprintf(stderr, "agree: %s: %s\n", static_path, err); return 1; }
    Vocab vocab = {0};
    if (vocab_load(&vocab, vocab_path) != 0) { fprintf(stderr, "agree: no vocab.txt in %s\n", model_dir); bf_static_free(sm); return 1; }

    /* Read the corpus first: nothing loads if the input is unusable. */
    JsonlReader reader;
    if (jsonl_reader_open(&reader, text_field, id_field) != 0) { vocab_free(&vocab); bf_static_free(sm); return 1; }
    FILE *in = fopen(jsonl, "r");
    if (!in) { fprintf(stderr, "agree: cannot read %s\n", jsonl); jsonl_reader_close(&reader); vocab_free(&vocab); bf_static_free(sm); return 1; }
    char **texts = calloc((size_t)limit, sizeof(char *));
    long read_rows = 0, invisible = 0, static_empty = 0;
    int n = 0, sdims = bf_static_dims(sm);
    float *svec = malloc(sizeof(float) * (size_t)limit * sdims);
    double static_ms = 0;
    char *line = NULL; size_t cap = 0;
    while (n < limit && getline(&line, &cap, in) > 0) {
        char *id = NULL, *text = NULL;
        if (jsonl_reader_extract(&reader, line, &id, &text) != 1) continue;
        read_rows++;
        int words = 0, unknown = 0, meaningful = 0;
        wordpiece_visibility(&vocab, text, &words, &unknown, &meaningful);
        if (visibility_refuses(words, unknown, meaningful)) { invisible++; free(id); free(text); continue; }
        double t0 = agree_now_ms();
        int used = bf_static_embed(sm, text, svec + (size_t)n * sdims);
        static_ms += agree_now_ms() - t0;
        if (!used) { static_empty++; free(id); free(text); continue; }
        texts[n++] = text; free(id);
    }
    free(line); fclose(in); jsonl_reader_close(&reader);
    if (n < k + 1) { fprintf(stderr, "agree: only %d texts both representations can see; need more than k=%d\n", n, k); return 1; }

    /* The teacher is a model load: ask the ledger. The estimate is twice the
     * graph's bytes (weights plus ORT's arena); the measured peak is reported
     * beside it so the rule can be checked rather than trusted. */
    BfAdmissionGrants *grants = NULL;
    BfAdmissionPolicy policy; BfAdmissionDecision decision; BfResourceRequest request = {0};
    int64_t grant_id = 0, estimate = (int64_t)onnx_st.st_size * 2;
    bf_admission_policy_memory(&policy);
    if (bf_admission_open(ledger, &grants, stderr) != 0) { fprintf(stderr, "agree: cannot open admission ledger %s\n", ledger); return 1; }
    request.plane = "embed"; request.kind = "onnx_embedding_teacher"; request.estimated_bytes = estimate;
    request.volume = BF_ADMISSION_MEMORY_VOLUME; request.detail = model;
    BfAdmissionVerdict verdict = bf_admission_request_grant(grants, &request, &policy,
        bf_admission_memory_available(), NULL, &decision, &grant_id);
    if (verdict != BF_ADMISSION_ADMIT) {
        fprintf(stderr, "agree: ResourceAdmission %s: %s\n", bf_admission_verdict_name(verdict), decision.reason);
        bf_admission_close(grants); return 3;
    }
    OrtContext ort;
    int tdims = 0, rc = 1;
    float *tvec = NULL;
    double teacher_ms = 0;
    if (ort_session_open(&ort, model_dir) != 0) goto release;
    for (int i = 0; i < n; i++) {
        float *v = NULL; int d = 0;
        double t0 = agree_now_ms();
        if (ort_embed_text(&ort, &vocab, texts[i], &v, &d) != 0) { fprintf(stderr, "agree: teacher failed on text %d\n", i); ort_session_close(&ort); goto release; }
        teacher_ms += agree_now_ms() - t0;
        if (!tvec) { tdims = d; tvec = malloc(sizeof(float) * (size_t)n * tdims); }
        memcpy(tvec + (size_t)i * tdims, v, sizeof(float) * (size_t)tdims); free(v);
    }
    ort_session_close(&ort);
    rc = 0;
release:
    bf_admission_release_grant(grants, grant_id, NULL);
    bf_admission_close(grants);
    if (rc) return 1;

    /* Lifetime peak physical footprint, the figure Activity Monitor calls
     * memory; -1 where the platform does not report one. */
    int64_t peak = -1;
#ifdef __APPLE__
    {
        struct rusage_info_v4 ri;
        if (proc_pid_rusage(getpid(), RUSAGE_INFO_V4, (rusage_info_t *)&ri) == 0)
            peak = (int64_t)ri.ri_lifetime_max_phys_footprint;
    }
#endif
    int *tk = malloc(sizeof(int) * k), *sk = malloc(sizeof(int) * k);
    long overlap = 0, top1 = 0; double worst = 1.0;
    for (int q = 0; q < n; q++) {
        agree_topk(tvec, n, tdims, q, k, tk);
        agree_topk(svec, n, sdims, q, k, sk);
        int hit = 0;
        for (int a = 0; a < k; a++) for (int b = 0; b < k; b++) if (tk[a] == sk[b]) { hit++; break; }
        overlap += hit; if (tk[0] == sk[0]) top1++;
        if ((double)hit / k < worst) worst = (double)hit / k;
    }
    bf_jsonw w; bf_jsonw_init(&w, 2);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "schema"); bf_jsonw_str(&w, "bonfyre.embed.agreement.v1");
    bf_jsonw_key(&w, "teacher"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "model"); bf_jsonw_str(&w, model);
    bf_jsonw_key(&w, "onnx_path"); bf_jsonw_str(&w, onnx_path);
    bf_jsonw_key(&w, "onnx_sha256"); bf_jsonw_str(&w, onnx_hex);
    bf_jsonw_key(&w, "onnx_bytes"); bf_jsonw_int(&w, (int64_t)onnx_st.st_size);
    bf_jsonw_key(&w, "vocab_sha256"); bf_jsonw_str(&w, vocab_hex);
    bf_jsonw_key(&w, "dims"); bf_jsonw_int(&w, tdims);
    bf_jsonw_key(&w, "max_tokens"); bf_jsonw_int(&w, MAX_SEQ_LEN);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "static"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "path"); bf_jsonw_str(&w, static_path);
    bf_jsonw_key(&w, "sha256"); bf_jsonw_str(&w, static_hex);
    bf_jsonw_key(&w, "dims"); bf_jsonw_int(&w, sdims);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "corpus"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "path"); bf_jsonw_str(&w, jsonl);
    bf_jsonw_key(&w, "sha256"); bf_jsonw_str(&w, corpus_hex);
    bf_jsonw_key(&w, "rows_read"); bf_jsonw_int(&w, read_rows);
    bf_jsonw_key(&w, "compared"); bf_jsonw_int(&w, n);
    bf_jsonw_key(&w, "refused_invisible_to_teacher"); bf_jsonw_int(&w, invisible);
    bf_jsonw_key(&w, "refused_empty_static"); bf_jsonw_int(&w, static_empty);
    bf_jsonw_key(&w, "limit"); bf_jsonw_int(&w, limit);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "k"); bf_jsonw_int(&w, k);
    bf_jsonw_key(&w, "neighbour_overlap_at_k"); bf_jsonw_num(&w, (double)overlap / ((double)n * k));
    bf_jsonw_key(&w, "worst_query_overlap_at_k"); bf_jsonw_num(&w, worst);
    bf_jsonw_key(&w, "top1_agreement"); bf_jsonw_num(&w, (double)top1 / n);
    bf_jsonw_key(&w, "teacher_ms_per_text"); bf_jsonw_num(&w, teacher_ms / n);
    bf_jsonw_key(&w, "static_ms_per_text"); bf_jsonw_num(&w, static_ms / n);
    bf_jsonw_key(&w, "peak_footprint_bytes"); if (peak >= 0) bf_jsonw_int(&w, peak); else bf_jsonw_null(&w);
    bf_jsonw_key(&w, "admission"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "volume"); bf_jsonw_str(&w, BF_ADMISSION_MEMORY_VOLUME);
    bf_jsonw_key(&w, "estimate_bytes"); bf_jsonw_int(&w, estimate);
    bf_jsonw_key(&w, "estimate_rule"); bf_jsonw_str(&w, "2 x ONNX graph bytes");
    bf_jsonw_key(&w, "verdict"); bf_jsonw_str(&w, bf_admission_verdict_name(verdict));
    bf_jsonw_key(&w, "released"); bf_jsonw_bool(&w, 1);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "cannot_infer"); bf_jsonw_arr(&w);
    bf_jsonw_str(&w, "neighbour overlap with the teacher -> retrieval quality; the teacher itself is unjudged here");
    bf_jsonw_str(&w, "agreement on this corpus -> agreement on another corpus or language");
    bf_jsonw_str(&w, "the .bfse was distilled from this teacher; only the bytes compared are named");
    bf_jsonw_arr_end(&w);
    bf_jsonw_obj_end(&w);
    FILE *out = fopen(out_path, "w");
    int wrote = out && fwrite(w.buf, 1, w.len, out) == w.len && fputc('\n', out) != EOF;
    if (out && fclose(out) != 0) wrote = 0;
    if (wrote) fwrite(w.buf, 1, w.len, stdout), fputc('\n', stdout);
    else fprintf(stderr, "agree: cannot write %s\n", out_path);
    bf_jsonw_free(&w);
    for (int i = 0; i < n; i++) free(texts[i]);
    free(texts); free(svec); free(tvec); free(tk); free(sk); vocab_free(&vocab); bf_static_free(sm);
    return wrote ? 0 : 1;
}
