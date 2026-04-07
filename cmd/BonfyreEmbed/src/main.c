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
#include <dirent.h>
#include <sys/stat.h>
#include <onnxruntime_c_api.h>
#include <sqlite3.h>
#include <bonfyre.h>

/* portable CPU count (macOS + Linux) */
#ifdef __APPLE__
extern int sysctlbyname(const char *, void *, size_t *, void *, size_t);
#endif
static int get_cpu_count(void) {
#ifdef __APPLE__
    int count = 0;
    size_t len = sizeof(count);
    if (sysctlbyname("hw.ncpu", &count, &len, NULL, 0) == 0 && count > 0)
        return count;
#elif defined(_SC_NPROCESSORS_ONLN)
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n > 0) return (int)n;
#endif
    return 4;
}

#define MAX_SEQ_LEN  128
#define VOCAB_CAP    32000
#define MAX_WORD_LEN 200

/* ── utilities ──────────────────────────────────────────────── */

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
static char *read_file_contents(const char *path, size_t *out_len) { return bf_read_file(path, out_len); }

/* ── BERT WordPiece tokenizer (trie-based) ──────────────────── */

/*
 * Trie with 128-wide children (ASCII).  Each node stores a vocab ID
 * if the path from root to that node is a complete token (-1 otherwise).
 * Gives O(token_length) lookup — single traversal vs hash-table's
 * shrinking-substring probes.
 */
#define TRIE_ALPHA 128    /* ASCII range */

typedef struct TrieNode {
    int id;               /* vocab token ID, or -1 */
    int children[TRIE_ALPHA];  /* index into pool, 0 = no child */
} TrieNode;

typedef struct {
    TrieNode *pool;
    int count;
    int cap;
} Trie;

static void trie_init(Trie *t) {
    t->cap = 256 * 1024;
    t->pool = calloc((size_t)t->cap, sizeof(TrieNode));
    t->count = 1;  /* node 0 is root */
    t->pool[0].id = -1;
    memset(t->pool[0].children, 0, sizeof(t->pool[0].children));
}

static void trie_insert(Trie *t, const char *key, int id) {
    int node = 0;
    for (const unsigned char *p = (const unsigned char *)key; *p; p++) {
        int c = (*p < TRIE_ALPHA) ? *p : 0;
        if (t->pool[node].children[c] == 0) {
            if (t->count >= t->cap) {
                t->cap *= 2;
                t->pool = realloc(t->pool, (size_t)t->cap * sizeof(TrieNode));
            }
            int nn = t->count++;
            t->pool[nn].id = -1;
            memset(t->pool[nn].children, 0, sizeof(t->pool[nn].children));
            t->pool[node].children[c] = nn;
        }
        node = t->pool[node].children[c];
    }
    t->pool[node].id = id;
}

/* Walk trie, return longest-match vocab ID.  Sets *match_len. */
static int trie_longest_match(const Trie *t, const char *s, size_t len,
                               size_t *match_len) {
    int node = 0;
    int best_id = -1;
    size_t best_len = 0;
    for (size_t i = 0; i < len; i++) {
        int c = (unsigned char)s[i];
        if (c >= TRIE_ALPHA || t->pool[node].children[c] == 0) break;
        node = t->pool[node].children[c];
        if (t->pool[node].id >= 0) {
            best_id = t->pool[node].id;
            best_len = i + 1;
        }
    }
    *match_len = best_len;
    return best_id;
}

static void trie_free(Trie *t) { free(t->pool); }

typedef struct {
    char **tokens;
    int count;
    Trie trie;       /* main vocab trie */
    Trie sub_trie;   /* ## subword trie — keys WITHOUT the ## prefix */
} Vocab;

static int vocab_load(Vocab *v, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    v->tokens = malloc(sizeof(char *) * VOCAB_CAP);
    v->count = 0;
    trie_init(&v->trie);
    trie_init(&v->sub_trie);
    char line[512];
    while (fgets(line, sizeof(line), f) && v->count < VOCAB_CAP) {
        size_t len = strlen(line);
        while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r')) len--;
        line[len] = '\0';
        v->tokens[v->count] = strdup(line);
        trie_insert(&v->trie, line, v->count);
        /* For ## subwords, also insert without the prefix for faster lookup */
        if (len > 2 && line[0] == '#' && line[1] == '#')
            trie_insert(&v->sub_trie, line + 2, v->count);
        v->count++;
    }
    fclose(f);
    return 0;
}

static int vocab_lookup(const Vocab *v, const char *token) {
    size_t mlen;
    size_t tlen = strlen(token);
    int id = trie_longest_match(&v->trie, token, tlen, &mlen);
    return (mlen == tlen) ? id : -1;
}

static void vocab_free(Vocab *v) {
    for (int i = 0; i < v->count; i++) free(v->tokens[i]);
    free(v->tokens);
    trie_free(&v->trie);
    trie_free(&v->sub_trie);
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

/* WordPiece: greedy longest-match via trie (O(word_len) per word) */
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
            size_t mlen = 0;
            int found;
            if (start == 0) {
                /* First piece: look up in main trie */
                found = trie_longest_match(&vocab->trie, word, wlen, &mlen);
            } else {
                /* Continuation: look up in sub_trie (keys without ##) */
                found = trie_longest_match(&vocab->sub_trie,
                                            word + start, wlen - start, &mlen);
            }
            if (found < 0 || mlen == 0) { is_bad = 1; break; }
            ids[pos] = found; attn[pos] = 1; pos++;
            start += mlen;
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

/* ── ONNX Runtime session (reusable for batch) ──────────────── */

typedef struct {
    const OrtApi   *api;
    OrtEnv         *env;
    OrtSession     *session;
    OrtSessionOptions *opts;
    OrtMemoryInfo  *mem;
    int             hidden_dims;  /* filled after first inference */
} OrtContext;

static int ort_session_open(OrtContext *ctx, const char *model_dir) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->api = OrtGetApiBase()->GetApi(ORT_API_VERSION);
    if (!ctx->api) return -1;
    const OrtApi *api = ctx->api;

    char model_path[PATH_MAX];
    if (resolve_onnx_path(model_dir, model_path, sizeof(model_path)) != 0) {
        fprintf(stderr, "[embed] No ONNX model in %s/onnx/\n", model_dir);
        return -1;
    }

    ORT_CHECK(api->CreateEnv(ORT_LOGGING_LEVEL_WARNING, "bonfyre-embed", &ctx->env));
    ORT_CHECK(api->CreateSessionOptions(&ctx->opts));
    api->SetIntraOpNumThreads(ctx->opts, get_cpu_count());
    api->SetSessionGraphOptimizationLevel(ctx->opts, ORT_ENABLE_ALL);
    ORT_CHECK(api->CreateSession(ctx->env, model_path, ctx->opts, &ctx->session));
    ORT_CHECK(api->CreateCpuMemoryInfo(OrtArenaAllocator, OrtMemTypeDefault, &ctx->mem));
    return 0;

ort_fail:
    if (ctx->mem) api->ReleaseMemoryInfo(ctx->mem);
    if (ctx->session) api->ReleaseSession(ctx->session);
    if (ctx->opts) api->ReleaseSessionOptions(ctx->opts);
    if (ctx->env) api->ReleaseEnv(ctx->env);
    memset(ctx, 0, sizeof(*ctx));
    return -1;
}

static void ort_session_close(OrtContext *ctx) {
    if (!ctx->api) return;
    const OrtApi *api = ctx->api;
    if (ctx->mem) api->ReleaseMemoryInfo(ctx->mem);
    if (ctx->session) api->ReleaseSession(ctx->session);
    if (ctx->opts) api->ReleaseSessionOptions(ctx->opts);
    if (ctx->env) api->ReleaseEnv(ctx->env);
    memset(ctx, 0, sizeof(*ctx));
}

/* Embed a single text using an open session.  Caller frees *out_vec. */
static int ort_embed_text(OrtContext *ctx, const Vocab *vocab,
                           const char *text, float **out_vec, int *out_dims) {
    const OrtApi *api = ctx->api;

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

    OrtValue *t_ids = NULL, *t_mask = NULL, *t_types = NULL, *output = NULL;
    int64_t shape[2] = {1, MAX_SEQ_LEN};
    ORT_CHECK(api->CreateTensorWithDataAsOrtValue(ctx->mem, ids64, sizeof(ids64), shape, 2, ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64, &t_ids));
    ORT_CHECK(api->CreateTensorWithDataAsOrtValue(ctx->mem, mask64, sizeof(mask64), shape, 2, ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64, &t_mask));
    ORT_CHECK(api->CreateTensorWithDataAsOrtValue(ctx->mem, types64, sizeof(types64), shape, 2, ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64, &t_types));

    const char *in_names[] = {"input_ids", "attention_mask", "token_type_ids"};
    const char *out_names[] = {"last_hidden_state"};
    const OrtValue *inputs[] = {t_ids, t_mask, t_types};
    ORT_CHECK(api->Run(ctx->session, NULL, in_names, inputs, 3, out_names, 1, &output));

    OrtTensorTypeAndShapeInfo *info = NULL;
    api->GetTensorTypeAndShape(output, &info);
    size_t ndims; api->GetDimensionsCount(info, &ndims);
    int64_t oshape[4]; api->GetDimensions(info, oshape, ndims);
    api->ReleaseTensorTypeAndShapeInfo(info);
    int hidden = (int)oshape[ndims - 1];
    *out_dims = hidden;
    ctx->hidden_dims = hidden;

    float *raw = NULL;
    api->GetTensorMutableData(output, (void **)&raw);

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

    double norm = 0.0;
    for (int d = 0; d < hidden; d++) norm += (double)embedding[d] * embedding[d];
    norm = sqrt(norm);
    if (norm > 0)
        for (int d = 0; d < hidden; d++) embedding[d] = (float)(embedding[d] / norm);

    *out_vec = embedding;

    api->ReleaseValue(output);
    api->ReleaseValue(t_ids); api->ReleaseValue(t_mask); api->ReleaseValue(t_types);
    return 0;

ort_fail:
    if (t_ids) api->ReleaseValue(t_ids);
    if (t_mask) api->ReleaseValue(t_mask);
    if (t_types) api->ReleaseValue(t_types);
    return -1;
}

/* Backward-compat wrapper: open session, embed, close */
static int run_onnx_embed(const Vocab *vocab, const char *text,
                           const char *model_dir, float **out_vec, int *out_dims) {
    OrtContext ctx;
    if (ort_session_open(&ctx, model_dir) != 0) return -1;
    int rc = ort_embed_text(&ctx, vocab, text, out_vec, out_dims);
    ort_session_close(&ctx);
    return rc;
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

/* ── inline sqlite-vec insertion (--insert-db) ──────────────── */

static const char *resolve_vec_ext(void) {
    const char *env = getenv("BONFYRE_VEC_EXT");
    if (env && env[0]) return env;
    static const char *paths[] = {
        "/Users/nickgonzales/Library/Python/3.9/lib/python/site-packages/sqlite_vec/vec0",
        "/opt/homebrew/lib/sqlite_vec/vec0",
        "/usr/local/lib/sqlite_vec/vec0",
        NULL
    };
    struct stat st2;
    for (int i = 0; paths[i]; i++) {
        char buf[PATH_MAX];
        snprintf(buf, sizeof(buf), "%s.dylib", paths[i]);
        if (stat(buf, &st2) == 0) return paths[i];
        snprintf(buf, sizeof(buf), "%s.so", paths[i]);
        if (stat(buf, &st2) == 0) return paths[i];
    }
    return "vec0";
}

typedef struct {
    sqlite3 *db;
    sqlite3_stmt *meta_stmt;
    sqlite3_stmt *vec_stmt;
} VecDB;

static int vec_db_open(VecDB *vdb, const char *db_path, int dims) {
    memset(vdb, 0, sizeof(*vdb));
    if (sqlite3_open(db_path, &vdb->db) != SQLITE_OK) {
        fprintf(stderr, "[embed] Cannot open DB: %s\n", sqlite3_errmsg(vdb->db));
        return -1;
    }
    sqlite3_enable_load_extension(vdb->db, 1);
    const char *ext = resolve_vec_ext();
    char *err = NULL;
    if (sqlite3_load_extension(vdb->db, ext, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "[embed] Failed to load sqlite-vec: %s\n", err ? err : "unknown");
        sqlite3_free(err);
        sqlite3_close(vdb->db);
        vdb->db = NULL;
        return -1;
    }

    char create_vec[256];
    snprintf(create_vec, sizeof(create_vec),
        "CREATE VIRTUAL TABLE IF NOT EXISTS vec_artifacts USING vec0("
        "  id TEXT PRIMARY KEY, embedding float[%d])", dims);
    sqlite3_exec(vdb->db,
        "CREATE TABLE IF NOT EXISTS artifacts("
        "  id TEXT PRIMARY KEY, source TEXT, type TEXT, text TEXT,"
        "  metadata TEXT, created_at TEXT)", NULL, NULL, NULL);
    sqlite3_exec(vdb->db, create_vec, NULL, NULL, NULL);

    sqlite3_prepare_v2(vdb->db,
        "INSERT OR REPLACE INTO artifacts(id, source, type, text, metadata, created_at) "
        "VALUES (?, 'BonfyreEmbed', 'embedding', ?, '{}', datetime('now'))",
        -1, &vdb->meta_stmt, NULL);
    sqlite3_prepare_v2(vdb->db,
        "INSERT OR REPLACE INTO vec_artifacts(id, embedding) VALUES (?, ?)",
        -1, &vdb->vec_stmt, NULL);
    return 0;
}

static int vec_db_insert(VecDB *vdb, const char *doc_id, const char *text,
                          const float *embedding, int dims) {
    sqlite3_exec(vdb->db, "BEGIN", NULL, NULL, NULL);
    if (vdb->meta_stmt) {
        sqlite3_reset(vdb->meta_stmt);
        sqlite3_bind_text(vdb->meta_stmt, 1, doc_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(vdb->meta_stmt, 2, text, -1, SQLITE_TRANSIENT);
        sqlite3_step(vdb->meta_stmt);
    }
    if (vdb->vec_stmt) {
        sqlite3_reset(vdb->vec_stmt);
        sqlite3_bind_text(vdb->vec_stmt, 1, doc_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_blob(vdb->vec_stmt, 2, embedding, dims * (int)sizeof(float), SQLITE_TRANSIENT);
        sqlite3_step(vdb->vec_stmt);
    }
    sqlite3_exec(vdb->db, "COMMIT", NULL, NULL, NULL);
    return 0;
}

static void vec_db_close(VecDB *vdb) {
    if (vdb->meta_stmt) sqlite3_finalize(vdb->meta_stmt);
    if (vdb->vec_stmt) sqlite3_finalize(vdb->vec_stmt);
    if (vdb->db) sqlite3_close(vdb->db);
    memset(vdb, 0, sizeof(*vdb));
}

/* ── output writers ─────────────────────────────────────────── */

#define VECF_MAGIC 0x46434556u  /* "VECF" little-endian */

static int write_vector_binary(const char *path, const float *vec, int dims) {
    FILE *out = fopen(path, "wb");
    if (!out) return 1;
    uint32_t magic = VECF_MAGIC;
    uint32_t d = (uint32_t)dims;
    fwrite(&magic, 4, 1, out);
    fwrite(&d, 4, 1, out);
    fwrite(vec, sizeof(float), (size_t)dims, out);
    fclose(out);
    return 0;
}

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
        "[--output-format json|binary] [--insert-db <db>] [--doc-id <id>] "
        "[--input-dir <dir>] [--meta-out <path>] [--dry-run]\n"
        "\n"
        "  --input-dir <dir>  Batch mode: embed all .txt files in <dir>\n"
        "                     (loads model once, amortizes startup)\n");
}

int main(int argc, char **argv) {
    const char *text_path = NULL, *out_path = NULL, *meta_out = NULL;
    const char *model = "all-MiniLM-L6-v2", *backend = "onnx";
    const char *output_fmt = "json";
    const char *insert_db = NULL, *doc_id = NULL, *input_dir = NULL;
    int dims = 384, dry_run = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--text") == 0 && i+1 < argc) text_path = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i+1 < argc) out_path = argv[++i];
        else if (strcmp(argv[i], "--meta-out") == 0 && i+1 < argc) meta_out = argv[++i];
        else if (strcmp(argv[i], "--model") == 0 && i+1 < argc) model = argv[++i];
        else if (strcmp(argv[i], "--dims") == 0 && i+1 < argc) dims = atoi(argv[++i]);
        else if (strcmp(argv[i], "--backend") == 0 && i+1 < argc) backend = argv[++i];
        else if (strcmp(argv[i], "--output-format") == 0 && i+1 < argc) output_fmt = argv[++i];
        else if (strcmp(argv[i], "--insert-db") == 0 && i+1 < argc) insert_db = argv[++i];
        else if (strcmp(argv[i], "--doc-id") == 0 && i+1 < argc) doc_id = argv[++i];
        else if (strcmp(argv[i], "--input-dir") == 0 && i+1 < argc) input_dir = argv[++i];
        else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
        else { usage(); return 1; }
    }

    /* ── Batch mode: --input-dir ──────────────────────────── */
    if (input_dir) {
        if (!out_path && !insert_db) { usage(); return 1; }
        DIR *d = opendir(input_dir);
        if (!d) { fprintf(stderr, "Cannot open directory: %s\n", input_dir); return 2; }

        /* Collect .txt files */
        struct dirent *ent;
        char **files = NULL;
        int nfiles = 0, fcap = 0;
        while ((ent = readdir(d)) != NULL) {
            size_t nlen = strlen(ent->d_name);
            if (nlen < 5 || strcmp(ent->d_name + nlen - 4, ".txt") != 0) continue;
            if (nfiles == fcap) { fcap = fcap ? fcap * 2 : 64; files = realloc(files, sizeof(char *) * (size_t)fcap); }
            char fp[PATH_MAX];
            snprintf(fp, sizeof(fp), "%s/%s", input_dir, ent->d_name);
            files[nfiles++] = strdup(fp);
        }
        closedir(d);

        if (nfiles == 0) { fprintf(stderr, "[embed] No .txt files in %s\n", input_dir); free(files); return 0; }
        fprintf(stderr, "[embed] Batch mode: %d files in %s\n", nfiles, input_dir);

        if (dry_run) {
            for (int i = 0; i < nfiles; i++) { printf("Would embed: %s\n", files[i]); free(files[i]); }
            free(files); return 0;
        }

        /* Load model ONCE */
        const char *mdir = resolve_model_dir(model);
        fprintf(stderr, "[embed] backend=onnx-native  model=%s\n", mdir);
        char vpath[PATH_MAX];
        snprintf(vpath, sizeof(vpath), "%s/vocab.txt", mdir);
        Vocab vocab = {0};
        int use_onnx = (strcmp(backend, "onnx") == 0 && vocab_load(&vocab, vpath) == 0);
        OrtContext ort_ctx = {0};
        if (use_onnx && ort_session_open(&ort_ctx, mdir) != 0) use_onnx = 0;

        if (out_path) ensure_dir(out_path);

        VecDB vdb = {0};
        if (insert_db && vec_db_open(&vdb, insert_db, dims) != 0) {
            fprintf(stderr, "[embed] Cannot open vec DB for batch\n");
            if (use_onnx) { ort_session_close(&ort_ctx); vocab_free(&vocab); }
            for (int i = 0; i < nfiles; i++) free(files[i]);
            free(files); return 1;
        }

        int ok = 0, fail = 0;
        for (int fi = 0; fi < nfiles; fi++) {
            size_t tlen = 0;
            char *text = read_file_contents(files[fi], &tlen);
            if (!text) { fprintf(stderr, "[embed] Skip (unreadable): %s\n", files[fi]); fail++; continue; }

            float *embedding = NULL;
            int edims = dims;
            const char *bl = NULL;

            if (use_onnx) {
                if (ort_embed_text(&ort_ctx, &vocab, text, &embedding, &edims) == 0)
                    bl = "onnx-runtime-native";
            }
            if (!embedding) {
                TokenList toks = normalize_tokens(text);
                embedding = build_hash_embedding(&toks, edims);
                token_list_free(&toks);
                bl = "hashed-token-native";
            }
            free(text);
            if (!embedding) { fail++; free(files[fi]); continue; }

            /* Derive doc-id from filename */
            const char *base = strrchr(files[fi], '/');
            base = base ? base + 1 : files[fi];
            char fid[PATH_MAX];
            snprintf(fid, sizeof(fid), "%s", base);
            char *dot = strrchr(fid, '.'); if (dot) *dot = '\0';

            if (insert_db) {
                char *ft = read_file_contents(files[fi], NULL);
                vec_db_insert(&vdb, fid, ft ? ft : "", embedding, edims);
                free(ft);
            }
            if (out_path) {
                char opath[PATH_MAX];
                if (strcmp(output_fmt, "binary") == 0)
                    snprintf(opath, sizeof(opath), "%s/%s.vecf", out_path, fid);
                else
                    snprintf(opath, sizeof(opath), "%s/%s.json", out_path, fid);
                if (strcmp(output_fmt, "binary") == 0)
                    write_vector_binary(opath, embedding, edims);
                else
                    write_vector_json(opath, embedding, edims, bl);
            }

            free(embedding);
            free(files[fi]);
            ok++;
            fprintf(stderr, "  [%d/%d] %s  (%d dims, %s)\n", ok + fail, nfiles, fid, edims, bl);
        }

        if (use_onnx) { ort_session_close(&ort_ctx); vocab_free(&vocab); }
        if (insert_db) vec_db_close(&vdb);
        free(files);
        printf("Batch complete: %d/%d succeeded\n", ok, ok + fail);
        return fail > 0 ? 1 : 0;
    }

    /* ── Single-file mode ─────────────────────────────────── */
    if (!text_path || (!out_path && !insert_db) || dims <= 0) { usage(); return 1; }

    if (dry_run) {
        printf("Would embed: %s -> %s  [%s, %s, %d dims]\n",
               text_path, out_path ? out_path : insert_db, backend, model, dims);
        return 0;
    }

    size_t text_len = 0;
    char *text = read_file_contents(text_path, &text_len);
    if (!text) { fprintf(stderr, "Missing text file: %s\n", text_path); return 2; }

    /* Output directory (only if writing files) */
    char out_dir[PATH_MAX] = "";
    if (out_path) {
        strncpy(out_dir, out_path, sizeof(out_dir) - 1);
        out_dir[sizeof(out_dir) - 1] = '\0';
        char *sl = strrchr(out_dir, '/');
        if (sl) { *sl = '\0'; if (out_dir[0]) ensure_dir(out_dir); }
    }

    char default_meta[PATH_MAX] = "";
    char status_path[PATH_MAX] = "";
    if (out_path) {
        if (!meta_out) { snprintf(default_meta, sizeof(default_meta), "%s.json", out_path); meta_out = default_meta; }
        snprintf(status_path, sizeof(status_path), "%s/status.json",
                 (out_dir[0]) ? out_dir : ".");
    }

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

    /* Inline insert to sqlite-vec DB (skip file I/O entirely) */
    if (insert_db) {
        /* Auto-generate doc-id from text filename if not provided */
        char auto_id[PATH_MAX];
        if (!doc_id) {
            const char *base = strrchr(text_path, '/');
            base = base ? base + 1 : text_path;
            snprintf(auto_id, sizeof(auto_id), "%s", base);
            char *dot = strrchr(auto_id, '.');
            if (dot) *dot = '\0';
            doc_id = auto_id;
        }
        VecDB vdb = {0};
        if (vec_db_open(&vdb, insert_db, dims) != 0) {
            free(embedding); return 1;
        }
        char *t_text = read_file_contents(text_path, NULL);
        vec_db_insert(&vdb, doc_id, t_text ? t_text : "", embedding, dims);
        vec_db_close(&vdb);
        printf("Inserted %s into %s  [%d dims, %s]\n", doc_id, insert_db, dims, backend_label);
        free(t_text);
    }

    /* Write files (unless insert-db-only mode) */
    if (out_path) {
        /* Token count for metadata */
        char *t2 = read_file_contents(text_path, NULL);
        TokenList mt = {0};
        if (t2) { mt = normalize_tokens(t2); free(t2); }

        /* Write vector (binary or JSON) */
        int write_ok;
        const char *fmt_label;
        if (strcmp(output_fmt, "binary") == 0) {
            write_ok = write_vector_binary(out_path, embedding, dims);
            fmt_label = "binary";
        } else {
            write_ok = write_vector_json(out_path, embedding, dims, backend_label);
            fmt_label = "json";
        }
        if (write_ok != 0 ||
            write_meta_json(meta_out, text_path, out_path, dims, model, mt.count, backend_label) != 0 ||
            write_status_json(status_path, out_path, meta_out, backend_label) != 0) {
            free(embedding); token_list_free(&mt);
            return 1;
        }

        printf("Wrote embedding to %s  [%d dims, %s, %s]\n", out_path, dims, backend_label, fmt_label);
        printf("Wrote metadata to %s\n", meta_out);
        token_list_free(&mt);
    }

    free(embedding);
    return 0;
}
