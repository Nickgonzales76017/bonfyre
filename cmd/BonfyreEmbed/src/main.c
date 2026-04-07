/*
 * BonfyreEmbed — text embeddings via ONNX Runtime C API.
 *
 * Pure C. No Python. Real neural inference.
 * BERT WordPiece tokenizer + ONNX Runtime session + mean pooling.
 *
 * Backends:
 *   onnx  — real sentence embeddings (all-MiniLM-L6-v2, 384-dim)
 *   hash  — deterministic FNV1a fallback (no model required)
 */
#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <onnxruntime_c_api.h>

#define MAX_SEQ_LEN  128
#define VOCAB_CAP    32000
#define MAX_WORD_LEN 200

/* ── utilities ──────────────────────────────────────────────── */

static int ensure_dir(const char *path) {
    char tmp[PATH_MAX];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    strcpy(tmp, path);
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] == '/') {
            tmp[i] = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 1;
            tmp[i] = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 1;
    return 0;
}

static char *read_file_contents(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz < 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t rd = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[rd] = '\0';
    if (out_len) *out_len = rd;
    return buf;
}

/* ── BERT WordPiece tokenizer ───────────────────────────────── */

typedef struct {
    char **tokens;
    int count;
    int *ht_ids;
    char **ht_keys;
    int ht_cap;
} Vocab;

static unsigned int vocab_hash(const char *s) {
    unsigned int h = 5381;
    while (*s) { h = h * 33 + (unsigned char)*s; s++; }
    return h;
}

static int vocab_lookup(const Vocab *v, const char *token) {
    unsigned int h = vocab_hash(token) % (unsigned int)v->ht_cap;
    for (int i = 0; i < v->ht_cap; i++) {
        int idx = (int)((h + (unsigned int)i) % (unsigned int)v->ht_cap);
        if (v->ht_ids[idx] < 0) return -1;
        if (strcmp(v->ht_keys[idx], token) == 0) return v->ht_ids[idx];
    }
    return -1;
}

static void vocab_insert(Vocab *v, const char *key, int id) {
    unsigned int h = vocab_hash(key) % (unsigned int)v->ht_cap;
    for (int i = 0; i < v->ht_cap; i++) {
        int idx = (int)((h + (unsigned int)i) % (unsigned int)v->ht_cap);
        if (v->ht_ids[idx] < 0) {
            v->ht_keys[idx] = v->tokens[id];
            v->ht_ids[idx] = id;
            return;
        }
    }
}

static int vocab_load(Vocab *v, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    v->ht_cap = VOCAB_CAP * 3;
    v->ht_ids = malloc(sizeof(int) * (size_t)v->ht_cap);
    v->ht_keys = calloc((size_t)v->ht_cap, sizeof(char *));
    v->tokens = malloc(sizeof(char *) * VOCAB_CAP);
    v->count = 0;
    for (int i = 0; i < v->ht_cap; i++) v->ht_ids[i] = -1;
    char line[512];
    while (fgets(line, sizeof(line), f) && v->count < VOCAB_CAP) {
        size_t len = strlen(line);
        while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r')) len--;
        line[len] = '\0';
        v->tokens[v->count] = strdup(line);
        vocab_insert(v, line, v->count);
        v->count++;
    }
    fclose(f);
    return 0;
}

static void vocab_free(Vocab *v) {
    for (int i = 0; i < v->count; i++) free(v->tokens[i]);
    free(v->tokens);
    free(v->ht_ids);
    free(v->ht_keys);
}

/* BERT pre-tokenizer: split on whitespace + punctuation, lowercase */
typedef struct { char **words; int count; int cap; } WordList;

static void wordlist_push(WordList *wl, const char *w, size_t len) {
    if (wl->count == wl->cap) {
        wl->cap = wl->cap == 0 ? 64 : wl->cap * 2;
        wl->words = realloc(wl->words, sizeof(char *) * (size_t)wl->cap);
    }
    char *s = malloc(len + 1);
    for (size_t i = 0; i < len; i++) s[i] = (char)tolower((unsigned char)w[i]);
    s[len] = '\0';
    wl->words[wl->count++] = s;
}

static void wordlist_free(WordList *wl) {
    for (int i = 0; i < wl->count; i++) free(wl->words[i]);
    free(wl->words);
}

static WordList bert_pre_tokenize(const char *text) {
    WordList wl = {0};
    size_t i = 0, len = strlen(text);
    while (i < len) {
        while (i < len && isspace((unsigned char)text[i])) i++;
        if (i >= len) break;
        if (ispunct((unsigned char)text[i])) {
            wordlist_push(&wl, text + i, 1);
            i++;
            continue;
        }
        size_t start = i;
        while (i < len && !isspace((unsigned char)text[i]) && !ispunct((unsigned char)text[i]))
            i++;
        if (i > start) wordlist_push(&wl, text + start, i - start);
    }
    return wl;
}

/* WordPiece: greedy longest-match */
static int wordpiece_encode(const Vocab *vocab, const WordList *words,
                            int *ids, int *attn, int max_len,
                            int cls_id, int sep_id, int unk_id) {
    int pos = 0;
    ids[pos] = cls_id; attn[pos] = 1; pos++;

    for (int w = 0; w < words->count && pos < max_len - 1; w++) {
        const char *word = words->words[w];
        size_t wlen = strlen(word);
        size_t start = 0;
        int is_bad = 0;

        while (start < wlen && pos < max_len - 1) {
            size_t end = wlen;
            int found = -1;
            while (end > start) {
                char sub[MAX_WORD_LEN + 4];
                size_t slen = end - start;
                if (slen > MAX_WORD_LEN) slen = MAX_WORD_LEN;
                if (start > 0) {
                    sub[0] = '#'; sub[1] = '#';
                    memcpy(sub + 2, word + start, slen);
                    sub[2 + slen] = '\0';
                } else {
                    memcpy(sub, word + start, slen);
                    sub[slen] = '\0';
                }
                found = vocab_lookup(vocab, sub);
                if (found >= 0) break;
                end--;
            }
            if (found < 0) { is_bad = 1; break; }
            ids[pos] = found; attn[pos] = 1; pos++;
            start = end;
        }
        if (is_bad && pos < max_len - 1) {
            ids[pos] = unk_id; attn[pos] = 1; pos++;
        }
    }

    ids[pos] = sep_id; attn[pos] = 1; pos++;
    while (pos < max_len) { ids[pos] = 0; attn[pos] = 0; pos++; }
    return max_len;
}

/* ── ONNX Runtime inference ─────────────────────────────────── */

static const char *resolve_model_dir(const char *model) {
    if (model[0] == '/' || model[0] == '.') return model;
    static char resolved[PATH_MAX];
    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    snprintf(resolved, sizeof(resolved), "%s/.cache/bonfyre/models/%s", home, model);
    return resolved;
}

static int resolve_onnx_path(const char *model_dir, char *out, size_t out_sz) {
    const char *names[] = {
        "onnx/model_qint8_arm64.onnx", "onnx/model_O4.onnx", "onnx/model.onnx", NULL
    };
    struct stat st;
    for (int i = 0; names[i]; i++) {
        snprintf(out, out_sz, "%s/%s", model_dir, names[i]);
        if (stat(out, &st) == 0) return 0;
    }
    return -1;
}

#define ORT_CHECK(expr) do { \
    OrtStatus *_s = (expr); \
    if (_s) { fprintf(stderr, "[embed] ORT: %s\n", api->GetErrorMessage(_s)); api->ReleaseStatus(_s); goto ort_fail; } \
} while(0)

static int run_onnx_embed(const Vocab *vocab, const char *text,
                           const char *model_dir, float **out_vec, int *out_dims) {
    const OrtApi *api = OrtGetApiBase()->GetApi(ORT_API_VERSION);
    if (!api) return -1;

    char model_path[PATH_MAX];
    if (resolve_onnx_path(model_dir, model_path, sizeof(model_path)) != 0) {
        fprintf(stderr, "[embed] No ONNX model in %s/onnx/\n", model_dir);
        return -1;
    }

    /* Tokenize */
    WordList words = bert_pre_tokenize(text);
    int input_ids[MAX_SEQ_LEN] = {0};
    int attn_mask[MAX_SEQ_LEN] = {0};
    int cls = vocab_lookup(vocab, "[CLS]"); if (cls < 0) cls = 101;
    int sep = vocab_lookup(vocab, "[SEP]"); if (sep < 0) sep = 102;
    int unk = vocab_lookup(vocab, "[UNK]"); if (unk < 0) unk = 100;
    wordpiece_encode(vocab, &words, input_ids, attn_mask, MAX_SEQ_LEN, cls, sep, unk);
    wordlist_free(&words);

    int64_t ids64[MAX_SEQ_LEN], mask64[MAX_SEQ_LEN], types64[MAX_SEQ_LEN];
    for (int i = 0; i < MAX_SEQ_LEN; i++) {
        ids64[i] = input_ids[i]; mask64[i] = attn_mask[i]; types64[i] = 0;
    }

    OrtEnv *env = NULL;
    OrtSession *session = NULL;
    OrtSessionOptions *opts = NULL;
    OrtMemoryInfo *mem = NULL;
    OrtValue *t_ids = NULL, *t_mask = NULL, *t_types = NULL, *output = NULL;

    ORT_CHECK(api->CreateEnv(ORT_LOGGING_LEVEL_WARNING, "bonfyre-embed", &env));
    ORT_CHECK(api->CreateSessionOptions(&opts));
    api->SetIntraOpNumThreads(opts, 1);
    api->SetSessionGraphOptimizationLevel(opts, ORT_ENABLE_BASIC);
    ORT_CHECK(api->CreateSession(env, model_path, opts, &session));
    ORT_CHECK(api->CreateCpuMemoryInfo(OrtArenaAllocator, OrtMemTypeDefault, &mem));

    int64_t shape[2] = {1, MAX_SEQ_LEN};
    ORT_CHECK(api->CreateTensorWithDataAsOrtValue(mem, ids64, sizeof(ids64), shape, 2, ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64, &t_ids));
    ORT_CHECK(api->CreateTensorWithDataAsOrtValue(mem, mask64, sizeof(mask64), shape, 2, ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64, &t_mask));
    ORT_CHECK(api->CreateTensorWithDataAsOrtValue(mem, types64, sizeof(types64), shape, 2, ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64, &t_types));

    const char *in_names[] = {"input_ids", "attention_mask", "token_type_ids"};
    const char *out_names[] = {"last_hidden_state"};
    const OrtValue *inputs[] = {t_ids, t_mask, t_types};
    ORT_CHECK(api->Run(session, NULL, in_names, inputs, 3, out_names, 1, &output));

    /* Extract shape */
    OrtTensorTypeAndShapeInfo *info = NULL;
    api->GetTensorTypeAndShape(output, &info);
    size_t ndims; api->GetDimensionsCount(info, &ndims);
    int64_t oshape[4]; api->GetDimensions(info, oshape, ndims);
    api->ReleaseTensorTypeAndShapeInfo(info);
    int hidden = (int)oshape[ndims - 1];
    *out_dims = hidden;

    float *raw = NULL;
    api->GetTensorMutableData(output, (void **)&raw);

    /* Mean pooling */
    float *embedding = calloc((size_t)hidden, sizeof(float));
    float mask_sum = 0;
    for (int t = 0; t < MAX_SEQ_LEN; t++) {
        if (!attn_mask[t]) continue;
        mask_sum += 1.0f;
        for (int d = 0; d < hidden; d++)
            embedding[d] += raw[t * hidden + d];
    }
    if (mask_sum > 0)
        for (int d = 0; d < hidden; d++) embedding[d] /= mask_sum;

    /* L2 normalize */
    double norm = 0.0;
    for (int d = 0; d < hidden; d++) norm += (double)embedding[d] * embedding[d];
    norm = sqrt(norm);
    if (norm > 0)
        for (int d = 0; d < hidden; d++) embedding[d] = (float)(embedding[d] / norm);

    *out_vec = embedding;

    api->ReleaseValue(output);
    api->ReleaseValue(t_ids); api->ReleaseValue(t_mask); api->ReleaseValue(t_types);
    api->ReleaseMemoryInfo(mem);
    api->ReleaseSession(session); api->ReleaseSessionOptions(opts); api->ReleaseEnv(env);
    return 0;

ort_fail:
    if (t_ids) api->ReleaseValue(t_ids);
    if (t_mask) api->ReleaseValue(t_mask);
    if (t_types) api->ReleaseValue(t_types);
    if (mem) api->ReleaseMemoryInfo(mem);
    if (session) api->ReleaseSession(session);
    if (opts) api->ReleaseSessionOptions(opts);
    if (env) api->ReleaseEnv(env);
    return -1;
}

#undef ORT_CHECK

/* ── hash fallback ──────────────────────────────────────────── */

typedef struct { char **items; size_t count; size_t capacity; } TokenList;

static int token_list_push(TokenList *list, const char *value) {
    if (list->count == list->capacity) {
        size_t nc = list->capacity == 0 ? 64 : list->capacity * 2;
        char **ni = realloc(list->items, sizeof(char *) * nc);
        if (!ni) return 1;
        list->items = ni; list->capacity = nc;
    }
    list->items[list->count] = strdup(value);
    if (!list->items[list->count]) return 1;
    list->count++;
    return 0;
}

static void token_list_free(TokenList *list) {
    for (size_t i = 0; i < list->count; i++) free(list->items[i]);
    free(list->items);
}

static TokenList normalize_tokens(const char *text) {
    TokenList tokens = {0};
    char current[256]; size_t len = 0;
    for (size_t i = 0; ; i++) {
        unsigned char c = (unsigned char)text[i];
        if (isalnum(c) || c == '\'') {
            if (len + 1 < sizeof(current)) current[len++] = (char)tolower(c);
        } else {
            if (len > 0) { current[len] = '\0'; token_list_push(&tokens, current); len = 0; }
            if (c == '\0') break;
        }
    }
    return tokens;
}

static float *build_hash_embedding(const TokenList *tokens, int dims) {
    float *vector = calloc((size_t)dims, sizeof(float));
    if (!vector) return NULL;
    for (size_t i = 0; i < tokens->count; i++) {
        uint64_t hash = 1469598103934665603ULL;
        const char *s = tokens->items[i];
        while (*s) { hash ^= (unsigned char)*s; hash *= 1099511628211ULL; s++; }
        vector[(int)(hash % (uint64_t)dims)] += ((hash >> 8) & 1) ? -1.0f : 1.0f;
    }
    double norm = 0.0;
    for (int i = 0; i < dims; i++) norm += (double)vector[i] * (double)vector[i];
    norm = sqrt(norm);
    if (norm > 0) for (int i = 0; i < dims; i++) vector[i] = (float)(vector[i] / norm);
    return vector;
}

/* ── output writers ─────────────────────────────────────────── */

static int write_vector_json(const char *path, const float *vec, int dims,
                              const char *backend) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out, "{\n  \"vector\": [\n");
    for (int i = 0; i < dims; i++)
        fprintf(out, "    %.8f%s\n", vec[i], (i + 1 < dims) ? "," : "");
    fprintf(out, "  ],\n  \"dims\": %d,\n  \"backend\": \"%s\"\n}\n", dims, backend);
    fclose(out);
    return 0;
}

static int write_meta_json(const char *path, const char *text_path,
                           const char *vector_path, int dims, const char *model,
                           size_t token_count, const char *backend) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out,
        "{\n"
        "  \"sourceSystem\": \"BonfyreEmbed\",\n"
        "  \"textPath\": \"%s\",\n"
        "  \"vectorPath\": \"%s\",\n"
        "  \"vectorFormat\": \"json\",\n"
        "  \"dims\": %d,\n"
        "  \"model\": \"%s\",\n"
        "  \"tokens\": %zu,\n"
        "  \"deterministic\": true,\n"
        "  \"backend\": \"%s\"\n"
        "}\n",
        text_path, vector_path, dims, model, token_count, backend);
    fclose(out);
    return 0;
}

static int write_status_json(const char *path, const char *vector_path,
                              const char *meta_path, const char *backend) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out,
        "{\n"
        "  \"sourceSystem\": \"BonfyreEmbed\",\n"
        "  \"status\": \"completed\",\n"
        "  \"vectorPath\": \"%s\",\n"
        "  \"metaPath\": \"%s\",\n"
        "  \"deterministic\": true,\n"
        "  \"backend\": \"%s\"\n"
        "}\n",
        vector_path, meta_path, backend);
    fclose(out);
    return 0;
}

/* ── main ───────────────────────────────────────────────────── */

static void usage(void) {
    fprintf(stderr,
        "Usage: bonfyre-embed --text <path> --out <path> "
        "[--backend onnx|hash] [--model <name>] [--dims <n>] "
        "[--meta-out <path>] [--dry-run]\n");
}

int main(int argc, char **argv) {
    const char *text_path = NULL, *out_path = NULL, *meta_out = NULL;
    const char *model = "all-MiniLM-L6-v2", *backend = "onnx";
    int dims = 384, dry_run = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--text") == 0 && i+1 < argc) text_path = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i+1 < argc) out_path = argv[++i];
        else if (strcmp(argv[i], "--meta-out") == 0 && i+1 < argc) meta_out = argv[++i];
        else if (strcmp(argv[i], "--model") == 0 && i+1 < argc) model = argv[++i];
        else if (strcmp(argv[i], "--dims") == 0 && i+1 < argc) dims = atoi(argv[++i]);
        else if (strcmp(argv[i], "--backend") == 0 && i+1 < argc) backend = argv[++i];
        else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
        else { usage(); return 1; }
    }
    if (!text_path || !out_path || dims <= 0) { usage(); return 1; }

    if (dry_run) {
        printf("Would embed: %s -> %s  [%s, %s, %d dims]\n",
               text_path, out_path, backend, model, dims);
        return 0;
    }

    size_t text_len = 0;
    char *text = read_file_contents(text_path, &text_len);
    if (!text) { fprintf(stderr, "Missing text file: %s\n", text_path); return 2; }

    /* Output directory */
    char out_dir[PATH_MAX];
    strncpy(out_dir, out_path, sizeof(out_dir) - 1);
    out_dir[sizeof(out_dir) - 1] = '\0';
    char *sl = strrchr(out_dir, '/');
    if (sl) { *sl = '\0'; if (out_dir[0]) ensure_dir(out_dir); }

    char default_meta[PATH_MAX];
    if (!meta_out) { snprintf(default_meta, sizeof(default_meta), "%s.json", out_path); meta_out = default_meta; }
    char status_path[PATH_MAX];
    snprintf(status_path, sizeof(status_path), "%s/status.json",
             (sl && out_dir[0]) ? out_dir : ".");

    float *embedding = NULL;
    const char *backend_label = NULL;

    /* ONNX backend */
    if (strcmp(backend, "onnx") == 0) {
        const char *mdir = resolve_model_dir(model);
        fprintf(stderr, "[embed] backend=onnx-native  model=%s\n", mdir);
        char vpath[PATH_MAX];
        snprintf(vpath, sizeof(vpath), "%s/vocab.txt", mdir);
        Vocab vocab = {0};
        if (vocab_load(&vocab, vpath) != 0) {
            fprintf(stderr, "[embed] Cannot load vocab: %s — falling back to hash\n", vpath);
            backend = "hash";
        } else {
            int odims = 0;
            if (run_onnx_embed(&vocab, text, mdir, &embedding, &odims) == 0 && embedding) {
                dims = odims;
                backend_label = "onnx-runtime-native";
            } else {
                fprintf(stderr, "[embed] ONNX failed, falling back to hash\n");
                backend = "hash";
            }
            vocab_free(&vocab);
        }
    }

    /* Hash fallback */
    if (strcmp(backend, "hash") == 0) {
        if (dims <= 0) dims = 768;
        TokenList toks = normalize_tokens(text);
        embedding = build_hash_embedding(&toks, dims);
        token_list_free(&toks);
        backend_label = "hashed-token-native";
    }

    free(text);
    if (!embedding) { fprintf(stderr, "Embedding failed\n"); return 1; }

    /* Token count for metadata */
    char *t2 = read_file_contents(text_path, NULL);
    TokenList mt = {0};
    if (t2) { mt = normalize_tokens(t2); free(t2); }

    if (write_vector_json(out_path, embedding, dims, backend_label) != 0 ||
        write_meta_json(meta_out, text_path, out_path, dims, model, mt.count, backend_label) != 0 ||
        write_status_json(status_path, out_path, meta_out, backend_label) != 0) {
        free(embedding); token_list_free(&mt);
        return 1;
    }

    printf("Wrote embedding to %s  [%d dims, %s]\n", out_path, dims, backend_label);
    printf("Wrote metadata to %s\n", meta_out);
    free(embedding); token_list_free(&mt);
    return 0;
}
