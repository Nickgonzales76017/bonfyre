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
#ifdef __APPLE__
#define _DARWIN_C_SOURCE /* libproc for the measured peak footprint in agree */
#endif

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <onnxruntime_c_api.h>

/* One JSON field reader for the estate, rather than a tenth one written here. */
#include "bf_lex.h"
#include "bf_bert_fold.h"
#include "bf_static_embed.h"
#include <sqlite3.h>
#include <bonfyre.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

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

static void wordlist_free(WordList *wl) {
    for (int i = 0; i < wl->count; i++) free(wl->words[i]);
    free(wl->words);
}

/* Unicode-correct BERT pre-tokenization.
 *
 * The version this replaces walked bytes with ASCII predicates. isspace and
 * ispunct see nothing above 0x7F, and tolower changes nothing there, so three
 * things uncased BERT does never happened:
 *
 *   folding       "São" stayed "são" with its combining tilde intact and
 *                 "Уроженка" kept its capital, while the vocabulary holds
 *                 only folded forms -- the word missed and became [UNK];
 *   punctuation   "women’s" was one word, because U+2019 is three bytes none
 *                 of which is ASCII punctuation, so "women" matched and "’s"
 *                 did not and the whole word became [UNK];
 *   CJK           a run of ideographs was one word the vocabulary could not
 *                 contain, where BERT splits every ideograph into its own.
 *
 * At the far end of that is a text none of whose words survive, which reached
 * the model as [CLS] [UNK]xN [SEP] and embedded as its word count: unrelated
 * headlines of equal length at cosine 1.000.
 *
 * The fold table is generated from unicodedata rather than written here, so
 * what counts as a mark or an uppercase letter is Unicode's answer and not a
 * hand-maintained list of the accents someone thought of. */
static int utf8_decode_cp(const unsigned char *s, size_t len, uint32_t *cp)
{
    if (s[0] < 0x80) { *cp = s[0]; return 1; }
    if ((s[0] & 0xE0) == 0xC0 && len >= 2 && (s[1] & 0xC0) == 0x80) {
        *cp = ((uint32_t)(s[0] & 0x1F) << 6) | (s[1] & 0x3F); return 2;
    }
    if ((s[0] & 0xF0) == 0xE0 && len >= 3 && (s[1] & 0xC0) == 0x80 && (s[2] & 0xC0) == 0x80) {
        *cp = ((uint32_t)(s[0] & 0x0F) << 12) | ((uint32_t)(s[1] & 0x3F) << 6) | (s[2] & 0x3F);
        return 3;
    }
    if ((s[0] & 0xF8) == 0xF0 && len >= 4 && (s[1] & 0xC0) == 0x80 &&
        (s[2] & 0xC0) == 0x80 && (s[3] & 0xC0) == 0x80) {
        *cp = ((uint32_t)(s[0] & 0x07) << 18) | ((uint32_t)(s[1] & 0x3F) << 12) |
              ((uint32_t)(s[2] & 0x3F) << 6) | (s[3] & 0x3F);
        return 4;
    }
    *cp = 0xFFFD;   /* an invalid byte is one replacement, never a wrong length */
    return 1;
}

static int utf8_encode_cp(uint32_t cp, char *out)
{
    if (cp < 0x80) { out[0] = (char)cp; return 1; }
    if (cp < 0x800) { out[0] = (char)(0xC0 | (cp >> 6)); out[1] = (char)(0x80 | (cp & 0x3F)); return 2; }
    if (cp < 0x10000) {
        out[0] = (char)(0xE0 | (cp >> 12)); out[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        out[2] = (char)(0x80 | (cp & 0x3F)); return 3;
    }
    out[0] = (char)(0xF0 | (cp >> 18)); out[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
    out[2] = (char)(0x80 | ((cp >> 6) & 0x3F)); out[3] = (char)(0x80 | (cp & 0x3F));
    return 4;
}

static int bert_is_space(uint32_t cp)
{
    return cp == ' ' || cp == '\t' || cp == '\n' || cp == '\r' || cp == 0x0B || cp == 0x0C ||
           cp == 0xA0 || (cp >= 0x2000 && cp <= 0x200B) || cp == 0x202F || cp == 0x205F ||
           cp == 0x3000 || cp == 0xFEFF;
}

static int bert_is_control(uint32_t cp)
{
    return cp < 0x20 || (cp >= 0x7F && cp <= 0x9F) || cp == 0xFFFD;
}

/* BERT treats all ASCII non-alphanumerics and every Unicode P* character as
 * punctuation. These are the P* ranges that occur in headline text. */
static int bert_is_punct(uint32_t cp)
{
    if (cp < 0x80) return ispunct((int)cp) != 0;
    if (cp >= 0xA1 && cp <= 0xBF && cp != 0xAA && cp != 0xB5 && cp != 0xBA) return 1;
    if (cp == 0xD7 || cp == 0xF7) return 1;
    if (cp >= 0x2010 && cp <= 0x2027) return 1;
    if (cp >= 0x2030 && cp <= 0x205E) return 1;
    if (cp >= 0x3001 && cp <= 0x303F) return 1;
    if (cp >= 0xFF01 && cp <= 0xFF0F) return 1;
    if (cp >= 0xFF1A && cp <= 0xFF20) return 1;
    if (cp == 0x060C || cp == 0x061B || cp == 0x061F || cp == 0x06D4) return 1;
    if (cp == 0x0964 || cp == 0x0965 || cp == 0x0589 || cp == 0x05BE) return 1;
    return 0;
}

static int bert_is_cjk(uint32_t cp)
{
    return (cp >= 0x4E00 && cp <= 0x9FFF) || (cp >= 0x3400 && cp <= 0x4DBF) ||
           (cp >= 0xF900 && cp <= 0xFAFF) || (cp >= 0x20000 && cp <= 0x2A6DF) ||
           (cp >= 0x2F800 && cp <= 0x2FA1F);
}

/* NULL when the code point folds to itself; otherwise its folded UTF-8, which
 * is the empty string for a nonspacing mark. */
static const char *bert_fold_cp(uint32_t cp)
{
    size_t lo = 0, hi = BERT_FOLD_COUNT;
    if (cp < 0x80) return NULL;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (BERT_FOLD[mid].cp == cp) return BERT_FOLD[mid].out;
        if (BERT_FOLD[mid].cp < cp) lo = mid + 1; else hi = mid;
    }
    return NULL;
}

static void wordlist_push_raw(WordList *wl, const char *w, size_t len)
{
    if (wl->count == wl->cap) {
        wl->cap = wl->cap == 0 ? 64 : wl->cap * 2;
        wl->words = realloc(wl->words, sizeof(char *) * (size_t)wl->cap);
    }
    char *s = malloc(len + 1);
    memcpy(s, w, len);
    s[len] = '\0';
    wl->words[wl->count++] = s;
}

static WordList bert_pre_tokenize(const char *text) {
    WordList wl = {0};
    const unsigned char *s = (const unsigned char *)text;
    size_t len = strlen(text), i = 0, wn = 0;
    /* A fold never lengthens a code point past eight bytes, and every code
     * point is at least one input byte. */
    char *word = malloc(len * 8 + 16);
    if (!word) return wl;
    while (i < len) {
        uint32_t cp;
        char enc[4];
        int n = utf8_decode_cp(s + i, len - i, &cp);
        i += (size_t)n;
        if (bert_is_space(cp) || bert_is_control(cp)) {
            if (wn) { wordlist_push_raw(&wl, word, wn); wn = 0; }
            continue;
        }
        if (bert_is_punct(cp) || bert_is_cjk(cp)) {
            if (wn) { wordlist_push_raw(&wl, word, wn); wn = 0; }
            wordlist_push_raw(&wl, enc, (size_t)utf8_encode_cp(cp, enc));
            continue;
        }
        if (cp < 0x80) { word[wn++] = (char)tolower((int)cp); continue; }
        {
            const char *folded = bert_fold_cp(cp);
            if (folded) {
                size_t fl = strlen(folded);
                memcpy(word + wn, folded, fl);
                wn += fl;
            } else {
                wn += (size_t)utf8_encode_cp(cp, word + wn);
            }
        }
    }
    if (wn) wordlist_push_raw(&wl, word, wn);
    free(word);
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
/* How much of a text the vocabulary can see.
 *
 * wordpiece_encode maps any word it cannot build from vocabulary pieces to a
 * single [UNK]. For a script the vocabulary does not carry that is every word,
 * so a Bengali headline of N words reaches the model as [CLS] [UNK]x N [SEP]
 * and its vector depends on nothing but N. Two unrelated headlines of the same
 * length then embed identically -- measured on this estate's own corpus as
 * cosine 1.000 between a dengue campaign and a BRICS communique.
 *
 * Counted by running the same pre-tokenizer and matcher the encoder uses, so
 * the count cannot drift from what the model is actually given. */
static void wordpiece_visibility(const Vocab *vocab, const char *text,
                                 int *out_words, int *out_unknown, int *out_meaningful)
{
    WordList words = bert_pre_tokenize(text);
    int unknown = 0, content = 0, meaningful = 0;
    for (int w = 0; w < words.count; w++) {
        const char *word = words.words[w];
        size_t wlen = strlen(word), start = 0;
        /* Punctuation does not count toward what the model can see. A
         * headline whose words are all unknown and whose comma is known
         * still embeds as its length -- the first version of this rule
         * counted the comma and let fifteen such titles through, each
         * still sitting at cosine 1.000 with an unrelated neighbour. */
        {
            uint32_t cp;
            int n = utf8_decode_cp((const unsigned char *)word, wlen, &cp);
            if ((size_t)n == wlen && bert_is_punct(cp)) continue;
        }
        content++;
        {
            int missed = 0;
            while (start < wlen) {
                size_t mlen = 0;
                int found = start == 0
                    ? trie_longest_match(&vocab->trie, word, wlen, &mlen)
                    : trie_longest_match(&vocab->sub_trie, word + start, wlen - start, &mlen);
                if (found < 0 || mlen == 0) { unknown++; missed = 1; break; }
                start += mlen;
            }
            /* A visible word made only of digits does not identify a text.
             * A Japanese truck crash and an Urdu shooting that each kept a
             * "4" and lost every other word embedded at cosine 0.980. */
            if (!missed) {
                size_t k;
                for (k = 0; k < wlen; k++)
                    if (word[k] < '0' || word[k] > '9') { meaningful++; break; }
            }
        }
    }
    *out_words = content;
    *out_unknown = unknown;
    *out_meaningful = meaningful;
    wordlist_free(&words);
}

/* Whether a text is too little seen to embed.
 *
 * Refused when no visible word carries meaning, or when unknown words
 * outnumber the words that do. The second clause is a consequence of mean
 * pooling rather than a tuned threshold: every unknown word contributes the
 * same [UNK] token, so once they are the majority the shared [UNK] mass
 * outweighs whatever the few visible words say, and texts collapse onto
 * whichever incidental token survived. On the corpus that was "g20" in two
 * different Russian headlines at cosine 1.000 (seven of eight words unknown),
 * "sargan" at 1.000 (ten of eleven), and an escaped "quot" joining unrelated
 * Ukrainian titles at 0.976. Those looked like corroboration because the
 * surviving token was the topic, which is what made them dangerous. */
static int visibility_refuses(int content_words, int unknown_words, int meaningful_words)
{
    if (content_words <= 0) return 0;
    return meaningful_words == 0 || unknown_words > meaningful_words;
}

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
        /* macOS Homebrew */
        "/opt/homebrew/lib/sqlite_vec/vec0",
        /* Linux system-wide */
        "/usr/lib/x86_64-linux-gnu/sqlite_vec/vec0",
        "/usr/lib/aarch64-linux-gnu/sqlite_vec/vec0",
        "/usr/lib/sqlite_vec/vec0",
        /* universal fallback */
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
    sqlite3_stmt *vec_del_stmt;
} VecDB;

static void vec_db_close(VecDB *vdb) {
    if (vdb->meta_stmt) sqlite3_finalize(vdb->meta_stmt);
    if (vdb->vec_stmt) sqlite3_finalize(vdb->vec_stmt);
    if (vdb->vec_del_stmt) sqlite3_finalize(vdb->vec_del_stmt);
    if (vdb->db) sqlite3_close(vdb->db);
    memset(vdb, 0, sizeof(*vdb));
}

static int vec_db_open(VecDB *vdb, const char *db_path, int dims) {
    memset(vdb, 0, sizeof(*vdb));
    if (sqlite3_open(db_path, &vdb->db) != SQLITE_OK) {
        fprintf(stderr, "[embed] Cannot open DB: %s\n", sqlite3_errmsg(vdb->db));
        return -1;
    }
    /* A reader holding the store makes COMMIT wait for its lock. Without a
     * timeout the wait is zero and the commit fails at once. */
    sqlite3_busy_timeout(vdb->db, 5000);
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

    if (sqlite3_prepare_v2(vdb->db,
            "INSERT OR REPLACE INTO artifacts(id, source, type, text, metadata, created_at) "
            "VALUES (?, 'BonfyreEmbed', 'embedding', ?, '{}', datetime('now'))",
            -1, &vdb->meta_stmt, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(vdb->db,
            "INSERT INTO vec_artifacts(id, embedding) VALUES (?, ?)",
            -1, &vdb->vec_stmt, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(vdb->db,
            "DELETE FROM vec_artifacts WHERE id = ?",
            -1, &vdb->vec_del_stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[embed] vector store is not writable: %s\n", sqlite3_errmsg(vdb->db));
        vec_db_close(vdb);
        return -1;
    }
    return 0;
}

static int vec_db_insert(VecDB *vdb, const char *doc_id, const char *text,
                          const float *embedding, int dims) {
    /* Every step and the commit are checked, and the writer only ever writes
     * inside a transaction it opened.
     *
     * The version this replaces checked nothing and returned 0 unconditionally,
     * and it opened a transaction only when sqlite3_get_autocommit said none
     * was active. One COMMIT failing with SQLITE_BUSY -- a concurrent reader
     * was enough -- left its transaction open; every later insert then saw
     * autocommit off, skipped BEGIN and COMMIT, and rode the orphaned
     * transaction, and sqlite3_close rolled all of it back. On this estate's
     * corpus the batch reported 6,963 rows stored and 1,216 were. */
    int ok;
    if (!vdb->db || !vdb->meta_stmt || !vdb->vec_stmt || !vdb->vec_del_stmt) return -1;
    if (!sqlite3_get_autocommit(vdb->db)) {
        fprintf(stderr, "[embed] store has a transaction this writer did not open; "
                        "refusing to write %s into it\n", doc_id);
        return -1;
    }
    if (sqlite3_exec(vdb->db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "[embed] cannot begin write for %s: %s\n", doc_id, sqlite3_errmsg(vdb->db));
        return -1;
    }
    sqlite3_reset(vdb->meta_stmt);
    sqlite3_bind_text(vdb->meta_stmt, 1, doc_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(vdb->meta_stmt, 2, text, -1, SQLITE_TRANSIENT);
    ok = sqlite3_step(vdb->meta_stmt) == SQLITE_DONE;
    sqlite3_reset(vdb->meta_stmt);
    /* sqlite-vec's vec0 does not honour OR REPLACE on its primary key: a
     * second insert for an id fails with a UNIQUE constraint and the old
     * vector stays. The unchecked writer therefore replaced a re-embedded
     * row's text and silently kept its stale vector, so the store described
     * one headline and returned another's neighbours. Replacement is done
     * explicitly, inside the same transaction as the insert. */
    if (ok) {
        sqlite3_reset(vdb->vec_del_stmt);
        sqlite3_bind_text(vdb->vec_del_stmt, 1, doc_id, -1, SQLITE_TRANSIENT);
        ok = sqlite3_step(vdb->vec_del_stmt) == SQLITE_DONE;
        sqlite3_reset(vdb->vec_del_stmt);
    }
    if (ok) {
        sqlite3_reset(vdb->vec_stmt);
        sqlite3_bind_text(vdb->vec_stmt, 1, doc_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_blob(vdb->vec_stmt, 2, embedding, dims * (int)sizeof(float), SQLITE_TRANSIENT);
        ok = sqlite3_step(vdb->vec_stmt) == SQLITE_DONE;
        sqlite3_reset(vdb->vec_stmt);
    }
    if (!ok) {
        fprintf(stderr, "[embed] insert of %s failed: %s\n", doc_id, sqlite3_errmsg(vdb->db));
        sqlite3_exec(vdb->db, "ROLLBACK", NULL, NULL, NULL);
        return -1;
    }
    if (sqlite3_exec(vdb->db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "[embed] commit of %s failed: %s\n", doc_id, sqlite3_errmsg(vdb->db));
        sqlite3_exec(vdb->db, "ROLLBACK", NULL, NULL, NULL);
        return -1;
    }
    return 0;
}


typedef struct {
    sqlite3 *db;
    sqlite3_stmt *stmt;
    char text_path[192];
    char id_path[192];
} JsonlReader;

static int jsonl_field_path(const char *field, char *out, size_t out_size) {
    if (!field || !*field || !out || out_size < 4) return -1;
    for (const unsigned char *p = (const unsigned char *)field; *p; ++p)
        if (!(isalnum(*p) || *p == '_' || *p == '-')) return -1;
    return snprintf(out, out_size, "$.%s", field) < (int)out_size ? 0 : -1;
}

static int jsonl_reader_open(JsonlReader *reader, const char *text_field, const char *id_field) {
    memset(reader, 0, sizeof(*reader));
    if (jsonl_field_path(text_field, reader->text_path, sizeof(reader->text_path)) != 0 ||
        jsonl_field_path(id_field, reader->id_path, sizeof(reader->id_path)) != 0)
        return -1;
    if (sqlite3_open(":memory:", &reader->db) != SQLITE_OK) return -1;
    if (sqlite3_prepare_v2(reader->db,
            "SELECT json_valid(?1), json_extract(?1, ?2), json_extract(?1, ?3)",
            -1, &reader->stmt, NULL) != SQLITE_OK) {
        sqlite3_close(reader->db); reader->db = NULL; return -1;
    }
    return 0;
}

static int jsonl_reader_extract(JsonlReader *reader, const char *line, char **id_out, char **text_out) {
    const unsigned char *id, *text;
    *id_out = NULL; *text_out = NULL;
    sqlite3_reset(reader->stmt); sqlite3_clear_bindings(reader->stmt);
    sqlite3_bind_text(reader->stmt, 1, line, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(reader->stmt, 2, reader->text_path, -1, SQLITE_STATIC);
    sqlite3_bind_text(reader->stmt, 3, reader->id_path, -1, SQLITE_STATIC);
    if (sqlite3_step(reader->stmt) != SQLITE_ROW || sqlite3_column_int(reader->stmt, 0) != 1)
        return 0;
    text = sqlite3_column_text(reader->stmt, 1);
    id = sqlite3_column_text(reader->stmt, 2);
    if (!text || !id || !*text || !*id) return 0;
    *text_out = strdup((const char *)text);
    *id_out = strdup((const char *)id);
    if (!*text_out || !*id_out) { free(*text_out); free(*id_out); *text_out = NULL; *id_out = NULL; return -1; }
    return 1;
}

static void jsonl_reader_close(JsonlReader *reader) {
    if (reader->stmt) sqlite3_finalize(reader->stmt);
    if (reader->db) sqlite3_close(reader->db);
    memset(reader, 0, sizeof(*reader));
}

static float *embed_text_native(OrtContext *ort_ctx, Vocab *vocab, int use_onnx,
                                const char *text, int dims, int *out_dims,
                                const char **backend_label) {
    float *embedding = NULL;
    int edims = dims;
    if (use_onnx && ort_embed_text(ort_ctx, vocab, text, &embedding, &edims) == 0)
        *backend_label = "onnx-runtime-native";
    if (!embedding) {
        TokenList toks = normalize_tokens(text);
        embedding = build_hash_embedding(&toks, edims);
        token_list_free(&toks);
        *backend_label = "hashed-token-native";
    }
    *out_dims = edims;
    return embedding;
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

/* ─── VECF helpers for subcommands ──────────────────────────── */

static float *load_vecf_sub_(const char *path, uint32_t *out_dim) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    uint32_t magic, dim;
    if (fread(&magic, 4, 1, f) != 1 || magic != 0x46434556u /* VECF */ ||
        fread(&dim,   4, 1, f) != 1 || dim == 0 || dim > 65536) {
        fclose(f); return NULL;
    }
    float *v = malloc(dim * sizeof(float));
    if (!v) { fclose(f); return NULL; }
    if (fread(v, sizeof(float), dim, f) != (size_t)dim) { free(v); fclose(f); return NULL; }
    fclose(f);
    *out_dim = dim;
    return v;
}

static int save_vecf_sub_(const char *path, const float *v, uint32_t dim) {
    FILE *f = fopen(path, "wb");
    if (!f) return -1;
    uint32_t magic = 0x46434556u;
    int ok = (fwrite(&magic, 4, 1, f) == 1 &&
              fwrite(&dim,   4, 1, f) == 1 &&
              fwrite(v, sizeof(float), dim, f) == dim);
    fclose(f);
    return ok ? 0 : -1;
}

/* ─── subcommand: pack ───────────────────────────────────────── */

static int cmd_embed_pack_(int argc, char **argv) {
    (void)argc; (void)argv;
    const char *home = getenv("HOME");
    char pack_path[PATH_MAX];
    snprintf(pack_path, sizeof(pack_path),
             "%s/.local/share/bonfyre/embeds/pack.bfpack",
             home ? home : "/tmp");
    uint32_t n = 0;
    if (bf_embed_pack_build(pack_path, &n) != 0) {
        fprintf(stderr, "embed pack: build failed\n"); return 1;
    }
    if (n == 0) { printf("embed pack: no loose objects to pack\n"); return 0; }
    printf("embed pack: packed %u vectors → %s\n", n, pack_path);
    printf("  run 'bonfyre embed gc' to remove loose files\n");
    return 0;
}

/* ─── subcommand: stat ───────────────────────────────────────── */

static int cmd_embed_stat_(int argc, char **argv) {
    (void)argc; (void)argv;
    const char *home = getenv("HOME");
    char dir[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s/.local/share/bonfyre/embeds",
             home ? home : "/tmp");

    int loose = 0; off_t loose_bytes = 0;
    DIR *dd = opendir(dir);
    if (dd) {
        struct dirent *de;
        while ((de = readdir(dd))) {
            const char *nm = de->d_name;
            size_t l = strlen(nm);
            if (l == 72 && strcmp(nm + 64, ".bfembed") == 0) {
                char fp[PATH_MAX];
                snprintf(fp, sizeof(fp), "%s/%s", dir, nm);
                struct stat stt;
                if (stat(fp, &stt) == 0) { loose++; loose_bytes += stt.st_size; }
            }
        }
        closedir(dd);
    }

    char pack_path[PATH_MAX];
    snprintf(pack_path, sizeof(pack_path), "%s/pack.bfpack", dir);
    struct stat pst;
    printf("embed store: %s\n", dir);
    printf("  loose objects: %d  (%.1f KB)\n", loose, loose_bytes / 1024.0);
    if (stat(pack_path, &pst) == 0) {
        BfEmbedPack pk = {0};
        if (bf_embed_pack_open(&pk, pack_path) == 0) {
            printf("  pack:  %u vectors, dim=%u  (%.1f KB)\n",
                   pk.n, pk.dim, pst.st_size / 1024.0);
            bf_embed_pack_close(&pk);
        } else {
            printf("  pack:  %s  (%.1f KB, bad header)\n",
                   pack_path, pst.st_size / 1024.0);
        }
    } else {
        printf("  pack:  none  (run: bonfyre embed pack)\n");
    }

    /* KV chain stats */
    char kv_root[PATH_MAX];
    snprintf(kv_root, sizeof(kv_root),
             "%s/.local/share/bonfyre/kvcache", home ? home : "/tmp");
    char kv2_root[PATH_MAX];
    snprintf(kv2_root, sizeof(kv2_root),
             "%s/.local/share/bonfyre/kvcache-chain", home ? home : "/tmp");
    struct stat kvst, kv2st;
    if (stat(kv_root, &kvst) == 0)
        printf("  kvcache store:  %s\n", kv_root);
    if (stat(kv2_root, &kv2st) == 0)
        printf("  kvcache chain:  %s\n", kv2_root);

    /* Steering vectors */
    char **snames = NULL; int scount = 0;
    bf_embed_steer_list(&snames, &scount);
    if (scount > 0) {
        printf("  branches (%d):", scount);
        for (int i = 0; i < scount; i++) { printf(" %s", snames[i]); free(snames[i]); }
        printf("\n");
    } else {
        printf("  branches: none\n");
    }
    free(snames);
    return 0;
}

/* ─── subcommand: gc ─────────────────────────────────────────── */

static int cmd_embed_gc_(int argc, char **argv) {
    (void)argc; (void)argv;
    const char *home = getenv("HOME");
    char dir[PATH_MAX], pack_path[PATH_MAX];
    snprintf(dir,       sizeof(dir),       "%s/.local/share/bonfyre/embeds", home ? home : "/tmp");
    snprintf(pack_path, sizeof(pack_path), "%s/pack.bfpack", dir);

    BfEmbedPack pack = {0};
    if (bf_embed_pack_open(&pack, pack_path) != 0) {
        printf("embed gc: no pack found — run 'bonfyre embed pack' first\n");
        return 0;
    }

    DIR *dd = opendir(dir);
    if (!dd) { bf_embed_pack_close(&pack); return 0; }

    int removed = 0;
    struct dirent *de;
    while ((de = readdir(dd))) {
        const char *nm = de->d_name;
        size_t l = strlen(nm);
        if (l != 72 || strcmp(nm + 64, ".bfembed") != 0) continue;
        uint8_t hash[32]; int ok = 1;
        for (int i = 0; i < 32; i++) {
            unsigned v;
            if (sscanf(nm + i * 2, "%02x", &v) != 1) { ok = 0; break; }
            hash[i] = (uint8_t)v;
        }
        if (!ok) continue;
        if (bf_embed_pack_lookup(&pack, hash)) {
            char fp[PATH_MAX];
            snprintf(fp, sizeof(fp), "%s/%s", dir, nm);
            unlink(fp);
            removed++;
        }
    }
    closedir(dd);
    bf_embed_pack_close(&pack);
    printf("embed gc: removed %d loose objects (now only in pack)\n", removed);
    return 0;
}

/* ─── subcommand: branch ─────────────────────────────────────── */

static int cmd_embed_branch_(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr,
            "Usage:\n"
            "  bonfyre embed branch list\n"
            "  bonfyre embed branch add   <name> <delta.vecf>\n"
            "  bonfyre embed branch delta <base.vecf> <target.vecf> <name>\n"
            "  bonfyre embed branch apply <input.vecf> --branch <name> [--alpha 0.5]"
                                        " [--branch <name2> ...] --out <output.vecf>\n"
        );
        return 1;
    }

    const char *action = argv[1];

    /* ── list ── */
    if (strcmp(action, "list") == 0) {
        char **names = NULL; int count = 0;
        bf_embed_steer_list(&names, &count);
        if (count == 0) printf("no branches stored\n");
        else for (int i = 0; i < count; i++) { printf("  %s\n", names[i]); free(names[i]); }
        free(names);
        return 0;
    }

    /* ── add: store a pre-computed delta vector as a branch ── */
    if (strcmp(action, "add") == 0) {
        if (argc < 4) { fprintf(stderr, "branch add: need <name> <delta.vecf>\n"); return 1; }
        const char *name = argv[2], *path = argv[3];
        uint32_t dim = 0;
        float *vec = load_vecf_sub_(path, &dim);
        if (!vec) { fprintf(stderr, "branch add: cannot read %s\n", path); return 1; }
        if (bf_embed_steer_add(name, vec, dim) != 0) {
            free(vec); fprintf(stderr, "branch add: store failed\n"); return 1;
        }
        free(vec);
        printf("branch '%s' stored  [%u dims]\n", name, dim);
        return 0;
    }

    /* ── delta: create branch from difference of two embeddings ── *
     * This is how you build concept branches:
     *   "apple (tech context)" - "apple (general)" = tech steering vector
     */
    if (strcmp(action, "delta") == 0) {
        if (argc < 5) {
            fprintf(stderr, "branch delta: need <base.vecf> <target.vecf> <name>\n");
            return 1;
        }
        const char *base_path   = argv[2];
        const char *target_path = argv[3];
        const char *name        = argv[4];
        uint32_t bdim = 0, tdim = 0;
        float *base_v   = load_vecf_sub_(base_path,   &bdim);
        float *target_v = load_vecf_sub_(target_path, &tdim);
        if (!base_v || !target_v) {
            free(base_v); free(target_v);
            fprintf(stderr, "branch delta: cannot read input files\n"); return 1;
        }
        if (bdim != tdim) {
            free(base_v); free(target_v);
            fprintf(stderr, "branch delta: dimension mismatch (%u vs %u)\n", bdim, tdim);
            return 1;
        }
        /* delta = target - base */
        for (uint32_t i = 0; i < bdim; i++) base_v[i] = target_v[i] - base_v[i];
        free(target_v);
        if (bf_embed_steer_add(name, base_v, bdim) != 0) {
            free(base_v); fprintf(stderr, "branch delta: store failed\n"); return 1;
        }
        free(base_v);
        printf("branch '%s' = (target - base)  [%u dims]\n", name, bdim);
        return 0;
    }

    /* ── apply: steer an embedding along one or more branches ── */
    if (strcmp(action, "apply") == 0) {
        if (argc < 3) { fprintf(stderr, "branch apply: need <input.vecf>\n"); return 1; }
        const char *inpath = argv[2], *outpath = NULL;
        const char *bnames[32]; float balphas[32]; int nb = 0;

        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--branch") == 0 && i + 1 < argc) {
                bnames[nb]  = argv[++i];
                balphas[nb] = 1.0f;
                nb++;
            } else if (strcmp(argv[i], "--alpha") == 0 && i + 1 < argc && nb > 0) {
                balphas[nb - 1] = (float)atof(argv[++i]);
            } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
                outpath = argv[++i];
            }
        }
        if (nb == 0 || !outpath) {
            fprintf(stderr, "branch apply: need --branch <name> --out <file>\n");
            return 1;
        }

        uint32_t dim = 0;
        float *vec = load_vecf_sub_(inpath, &dim);
        if (!vec) { fprintf(stderr, "branch apply: cannot read %s\n", inpath); return 1; }

        if (bf_embed_steer_apply(vec, dim, bnames, balphas, nb) != 0) {
            free(vec);
            fprintf(stderr, "branch apply: steer not found or dim mismatch\n");
            return 1;
        }
        if (save_vecf_sub_(outpath, vec, dim) != 0) {
            free(vec); fprintf(stderr, "branch apply: write failed\n"); return 1;
        }
        free(vec);
        printf("applied %d branch(es): %s → %s  [%u dims]\n", nb, inpath, outpath, dim);
        return 0;
    }

    fprintf(stderr, "branch: unknown action '%s'\n", action);
    return 1;
}

/* ─── usage + main ───────────────────────────────────────────── */

#include "static_embed.h"

static void usage(void) {
    fprintf(stderr,
        "Usage: bonfyre-embed --text <path> --out <path> "
        "[--backend onnx|hash] [--allow-degraded] [--model <name>] [--dims <n>] "
        "[--output-format json|binary] [--insert-db <db>] [--doc-id <id>] "
        "[--input-dir <dir> | --input-jsonl <file>] [--text-field <field>] "
        "[--id-field <field>] [--meta-out <path>] [--dry-run]\n"
        "\n"
        "  --input-dir <dir>  Batch mode: embed all .txt files in <dir>\n"
        "                     (loads model once, amortizes startup)\n"
        "  --input-jsonl <f>  Batch JSONL mode; defaults: --text-field text --id-field id\n"
        "  --prune-invisible  with --input-jsonl and --insert-db: delete stored rows the\n"
        "                     vocabulary cannot see, without embedding anything\n"
        "                     JSON strings are decoded by SQLite JSON1; model loads once\n"
        "\n"
        "Store management subcommands:\n"
        "  bonfyre embed pack           — consolidate loose objects into pack\n"
        "  bonfyre embed stat           — show store stats (loose/pack/branches)\n"
        "  bonfyre embed gc             — remove loose objects already in pack\n"
        "  bonfyre embed branch list    — list stored steering vectors\n"
        "  bonfyre embed branch add  <name> <delta.vecf>\n"
        "  bonfyre embed branch delta <base.vecf> <target.vecf> <name>\n"
        "  bonfyre embed branch apply <input.vecf> --branch <name> [--alpha 0.5] --out <out.vecf>\n"
        "\n"
        "Semantic search subcommands:\n"
        "  bonfyre embed index [--k N]           — build IVF ANN index from pack\n"
        "  bonfyre embed index-q8                — INT8-quantize pack then index\n"
        "  bonfyre embed nearest <input.vecf> [--k 10] [--n-probe 4] [--brute]\n"
        "  bonfyre embed merge <a.vecf> <b.vecf> [--alpha 0.5] --out <out.vecf>\n"
        "  bonfyre embed agree --static m.bfse --input-jsonl F --out receipt.json [--k 10] [--limit 2000]\n"
        "                     — top-k neighbour overlap of a .bfse with its ONNX teacher (memory grant first)\n"
        "\n"
        "Ref / history subcommands:\n"
        "  bonfyre embed tag <hash-hex> <name>   — store named ref\n"
        "  bonfyre embed tag list                — list all named refs\n"
        "  bonfyre embed tag delete <name>       — remove named ref\n"
        "  bonfyre embed refs                    — alias for tag list\n"
        "  bonfyre embed log [--n 20]            — reflog (newest first)\n"
    );
}

/* ── slerp helper ────────────────────────────────────────────── */
static void slerp_e_(const float *a, const float *b, float t,
                     float *out, uint32_t dim) {
    float dot = 0.0f;
    for (uint32_t i = 0; i < dim; i++) dot += a[i] * b[i];
    if (dot >  1.0f) dot =  1.0f;
    if (dot < -1.0f) dot = -1.0f;
    float theta = acosf(dot);
    if (theta < 1e-6f) {
        for (uint32_t i = 0; i < dim; i++) out[i] = a[i]*(1.0f-t) + b[i]*t;
        return;
    }
    float s = sinf(theta);
    float wa = sinf((1.0f - t) * theta) / s;
    float wb = sinf(t           * theta) / s;
    for (uint32_t i = 0; i < dim; i++) out[i] = wa * a[i] + wb * b[i];
}

static void norm_e_(float *v, uint32_t dim) {
    float n2 = 0.0f;
    for (uint32_t i = 0; i < dim; i++) n2 += v[i]*v[i];
    float n = sqrtf(n2);
    if (n > 1e-9f) for (uint32_t i = 0; i < dim; i++) v[i] /= n;
}

static void hex_out_e_(const uint8_t h[32], char out[65]) {
    static const char hc[] = "0123456789abcdef";
    for (int i = 0; i < 32; i++) { out[i*2]=hc[h[i]>>4]; out[i*2+1]=hc[h[i]&0xf]; }
    out[64] = '\0';
}

/* ── subcommand: index ───────────────────────────────────────── */
static int cmd_embed_index_(int argc, char **argv) {
    const char *home = getenv("HOME");
    char pack_path[PATH_MAX], idx_path[PATH_MAX];
    snprintf(pack_path, sizeof(pack_path),
             "%s/.local/share/bonfyre/embeds/pack.bfpack", home ? home : "/tmp");
    snprintf(idx_path,  sizeof(idx_path),
             "%s/.local/share/bonfyre/embeds/pack.bfidx",  home ? home : "/tmp");

    enum { EI_Q8, EI_Q8_LONG };
    static const BfCommandEntry embed_index_commands[] = {
        {"--q8", EI_Q8_LONG, 0, NULL},
        {"q8", EI_Q8, 0, NULL},
    };
    const BfCommandEntry *index_command = argc < 2 ? NULL : bf_command_lookup(embed_index_commands,
        sizeof(embed_index_commands) / sizeof(embed_index_commands[0]), argv[1]);
    int do_q8 = (index_command != NULL);
    if (do_q8) {
        uint32_t nq = 0;
        if (bf_embed_pack_build_q8(pack_path, &nq) != 0) {
            fprintf(stderr, "embed index: q8 pack build failed\n"); return 1;
        }
        printf("embed index: packed %u vectors (INT8) → %s\n", nq, pack_path);
    }

    uint32_t k = 0;
    for (int i = 1; i < argc; i++)
        if (strcmp(argv[i], "--k") == 0 && i+1 < argc) k = (uint32_t)atoi(argv[++i]);

    struct stat pst;
    if (stat(pack_path, &pst) != 0) {
        fprintf(stderr, "embed index: no pack — run 'bonfyre embed pack' first\n");
        return 1;
    }

    printf("embed index: clustering (k=%s)...\n", k ? "auto" : "auto");
    if (bf_embed_index_build(pack_path, k, idx_path) != 0) {
        fprintf(stderr, "embed index: build failed\n"); return 1;
    }
    BfEmbedIndex idx = {0};
    if (bf_embed_index_open(&idx, idx_path) == 0) {
        printf("embed index: %llu vectors, %u centroids → %s\n",
               (unsigned long long)idx.n_vectors, idx.n_centroids, idx_path);
        bf_embed_index_close(&idx);
    }
    return 0;
}

/* ── subcommand: nearest ─────────────────────────────────────── */
static int cmd_embed_nearest_(int argc, char **argv) {
    if (argc < 2) { fprintf(stderr, "nearest: need <input.vecf>\n"); return 1; }
    const char *inpath = argv[1];
    int top_k = 10, n_probe = 4, brute = 0;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i],"--k")==0      && i+1<argc) top_k  = atoi(argv[++i]);
        if (strcmp(argv[i],"--n-probe")==0 && i+1<argc) n_probe= atoi(argv[++i]);
        if (strcmp(argv[i],"--brute")==0) brute = 1;
    }
    uint32_t dim = 0;
    float *query = load_vecf_sub_(inpath, &dim);
    if (!query) { fprintf(stderr, "nearest: cannot read %s\n", inpath); return 1; }

    const char *home = getenv("HOME");
    char pack_path[PATH_MAX], idx_path[PATH_MAX];
    snprintf(pack_path, sizeof(pack_path),
             "%s/.local/share/bonfyre/embeds/pack.bfpack", home ? home : "/tmp");
    snprintf(idx_path,  sizeof(idx_path),
             "%s/.local/share/bonfyre/embeds/pack.bfidx",  home ? home : "/tmp");

    BfEmbedPack pack = {0};
    if (bf_embed_pack_open(&pack, pack_path) != 0) {
        fprintf(stderr, "nearest: no pack\n"); free(query); return 1;
    }
    if (dim != pack.dim) {
        fprintf(stderr, "nearest: dim mismatch (%u vs %u)\n", dim, pack.dim);
        bf_embed_pack_close(&pack); free(query); return 1;
    }

    BfEmbedSearchResult *res = malloc((size_t)top_k * sizeof(BfEmbedSearchResult));
    if (!res) { bf_embed_pack_close(&pack); free(query); return 1; }
    int found = 0;

    if (!brute) {
        BfEmbedIndex idx = {0};
        if (bf_embed_index_open(&idx, idx_path) == 0) {
            bf_embed_index_search(&idx, &pack, query, dim, top_k, n_probe, res, &found);
            bf_embed_index_close(&idx);
        } else {
            fprintf(stderr, "nearest: no index, using brute search\n");
            brute = 1;
        }
    }
    if (brute) bf_embed_brute_search(&pack, query, dim, top_k, res, &found);

    printf("Top-%d for %s:\n", found, inpath);
    for (int i = 0; i < found; i++) {
        char hex[65]; hex_out_e_(res[i].hash, hex);
        printf("  [%2d] %.4f  %s\n", i+1, res[i].score, hex);
    }
    free(res); bf_embed_pack_close(&pack); free(query);
    return 0;
}

/* ── subcommand: merge ───────────────────────────────────────── */
static int cmd_embed_merge_(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "merge: need <a.vecf> <b.vecf> --out <out.vecf>\n"); return 1;
    }
    float alpha = 0.5f; const char *outpath = NULL;
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i],"--alpha")==0 && i+1<argc) alpha = (float)atof(argv[++i]);
        if (strcmp(argv[i],"--out"  )==0 && i+1<argc) outpath = argv[++i];
    }
    if (!outpath) { fprintf(stderr, "merge: need --out\n"); return 1; }
    uint32_t da=0, db=0;
    float *va = load_vecf_sub_(argv[1], &da);
    float *vb = load_vecf_sub_(argv[2], &db);
    if (!va||!vb||da!=db) {
        free(va); free(vb);
        fprintf(stderr, "merge: load failed or dim mismatch\n"); return 1;
    }
    norm_e_(va, da); norm_e_(vb, db);
    float *out = malloc(da * sizeof(float));
    if (!out) { free(va); free(vb); return 1; }
    slerp_e_(va, vb, alpha, out, da);
    if (save_vecf_sub_(outpath, out, da) != 0) {
        fprintf(stderr, "merge: write failed\n");
        free(va); free(vb); free(out); return 1;
    }
    printf("merge: slerp(alpha=%.3f)  %s × %s → %s  [%u dims]\n",
           alpha, argv[1], argv[2], outpath, da);
    free(va); free(vb); free(out);
    return 0;
}

/* ── subcommand: tag / refs ──────────────────────────────────── */
static int cmd_embed_tag_(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "tag: need <hash-hex> <name> | list | delete <name>\n"); return 1;
    }
    enum { ET_DELETE, ET_LIST };
    static const BfCommandEntry embed_tag_commands[] = {
        {"delete", ET_DELETE, 0, NULL},
        {"list", ET_LIST, 0, NULL},
    };
    const BfCommandEntry *tag_command = bf_command_lookup(embed_tag_commands,
        sizeof(embed_tag_commands) / sizeof(embed_tag_commands[0]), argv[1]);
    if (tag_command && tag_command->id == ET_LIST) {
        char **names = NULL; int count = 0;
        bf_embed_ref_list(&names, &count);
        if (count == 0) { printf("no named refs\n"); free(names); return 0; }
        for (int i = 0; i < count; i++) {
            uint8_t h[32];
            if (bf_embed_ref_read(names[i], h) == 0) {
                char hex[65]; hex_out_e_(h, hex);
                printf("  %-30s  %s\n", names[i], hex);
            }
            free(names[i]);
        }
        free(names); return 0;
    }
    if (tag_command && tag_command->id == ET_DELETE) {
        if (argc < 3) { fprintf(stderr, "tag delete: need <name>\n"); return 1; }
        return bf_embed_ref_delete(argv[2]) == 0 ? 0 : 1;
    }
    if (argc < 3) { fprintf(stderr, "tag: need <hash-hex> <name>\n"); return 1; }
    const char *hexstr = argv[1];
    if (strlen(hexstr) != 64) { fprintf(stderr, "tag: bad hash\n"); return 1; }
    uint8_t hash[32];
    for (int i = 0; i < 32; i++) {
        unsigned v;
        if (sscanf(hexstr+i*2, "%02x", &v) != 1) { fprintf(stderr, "tag: bad hex\n"); return 1; }
        hash[i] = (uint8_t)v;
    }
    if (bf_embed_ref_write(argv[2], hash) != 0) {
        fprintf(stderr, "tag: write failed\n"); return 1;
    }
    printf("ref '%s' → %s\n", argv[2], hexstr);
    return 0;
}

/* ── subcommand: log ─────────────────────────────────────────── */
static int cmd_embed_log_(int argc, char **argv) {
    int max_n = 20;
    for (int i = 1; i < argc; i++)
        if (strcmp(argv[i], "--n") == 0 && i+1 < argc) max_n = atoi(argv[++i]);
    BfEmbedReflogEntry *ents = NULL; int count = 0;
    bf_embed_reflog_read(&ents, &count);
    if (count == 0) { printf("reflog empty\n"); return 0; }
    int start = count > max_n ? count - max_n : 0;
    for (int i = count-1; i >= start; i--) {
        char hex[65]; hex_out_e_(ents[i].hash, hex);
        printf("%s  %.8s  %s\n", ents[i].timestamp, hex, ents[i].message);
    }
    free(ents);
    return 0;
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    /* Subcommand routing */
    if (argc >= 2 && argv[1][0] != '-') {
        const char *sub = argv[1];
        if (strcmp(sub, "pack")    == 0) return cmd_embed_pack_   (argc-1, argv+1);
        if (strcmp(sub, "stat")    == 0) return cmd_embed_stat_   (argc-1, argv+1);
        if (strcmp(sub, "gc")      == 0) return cmd_embed_gc_     (argc-1, argv+1);
        if (strcmp(sub, "branch")  == 0) return cmd_embed_branch_ (argc-1, argv+1);
        if (strcmp(sub, "index")   == 0) return cmd_embed_index_  (argc-1, argv+1);
        if (strcmp(sub, "index-q8")== 0) {
            char *av2[] = {(char*)"index",(char*)"q8",NULL};
            return cmd_embed_index_(2, av2);
        }
        if (strcmp(sub, "nearest") == 0) return cmd_embed_nearest_(argc-1, argv+1);
        if (strcmp(sub, "merge")   == 0) return cmd_embed_merge_  (argc-1, argv+1);
        if (strcmp(sub, "tag")     == 0) return cmd_embed_tag_    (argc-1, argv+1);
        if (strcmp(sub, "refs")    == 0) {
            char *av2[] = {(char*)"tag",(char*)"list",NULL};
            return cmd_embed_tag_(2, av2);
        }
        if (strcmp(sub, "log")     == 0) return cmd_embed_log_    (argc-1, argv+1);
        if (strcmp(sub, "distill") == 0) return cmd_embed_distill_(argc-1, argv+1);
        if (strcmp(sub, "static")  == 0) return cmd_embed_static_ (argc-1, argv+1);
        if (strcmp(sub, "agree")   == 0) return cmd_embed_agree_  (argc-1, argv+1);
    }

    const char *text_path = NULL, *out_path = NULL, *meta_out = NULL;
    const char *model = "all-MiniLM-L6-v2", *backend = "onnx";
    int allow_degraded = 0;
    const char *output_fmt = "json";
    const char *insert_db = NULL, *doc_id = NULL, *input_dir = NULL;
    const char *input_jsonl = NULL, *text_field = "text", *id_field = "id";
    int prune_invisible = 0;
    int dims = 384, dry_run = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--text") == 0 && i+1 < argc) text_path = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i+1 < argc) out_path = argv[++i];
        else if (strcmp(argv[i], "--meta-out") == 0 && i+1 < argc) meta_out = argv[++i];
        else if (strcmp(argv[i], "--model") == 0 && i+1 < argc) model = argv[++i];
        else if (strcmp(argv[i], "--dims") == 0 && i+1 < argc) dims = atoi(argv[++i]);
        else if (strcmp(argv[i], "--backend") == 0 && i+1 < argc) backend = argv[++i];
        else if (strcmp(argv[i], "--allow-degraded") == 0) allow_degraded = 1;
        else if (strcmp(argv[i], "--output-format") == 0 && i+1 < argc) output_fmt = argv[++i];
        else if (strcmp(argv[i], "--insert-db") == 0 && i+1 < argc) insert_db = argv[++i];
        else if (strcmp(argv[i], "--doc-id") == 0 && i+1 < argc) doc_id = argv[++i];
        else if (strcmp(argv[i], "--input-dir") == 0 && i+1 < argc) input_dir = argv[++i];
        else if (strcmp(argv[i], "--input-jsonl") == 0 && i+1 < argc) input_jsonl = argv[++i];
        else if (strcmp(argv[i], "--text-field") == 0 && i+1 < argc) text_field = argv[++i];
        else if (strcmp(argv[i], "--id-field") == 0 && i+1 < argc) id_field = argv[++i];
        else if (strcmp(argv[i], "--prune-invisible") == 0) prune_invisible = 1;
        else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
        else { usage(); return 1; }
    }

    /* ── Batch mode: --input-dir ──────────────────────────── */
    /* Batch JSONL: one record per line, an id and a text.
     *
     * This flag has been in the usage text since the batch path was written
     * and was never parsed, so `--input-jsonl` fell through to the usage
     * screen -- advertised, accepted by the shell, and doing nothing. It is
     * the shape a corpus actually arrives in: 22,962 publication titles come
     * out of a database as rows, and writing each one to its own .txt file to
     * satisfy --input-dir is a detour through the filesystem for no reason.
     *
     * The model loads once here, which is the entire point of a batch mode:
     * per-item invocation spends most of its time opening an ONNX session. */
    if (input_jsonl) {
        FILE *jf = fopen(input_jsonl, "r");
        char *line = NULL;
        size_t cap = 0;
        ssize_t got;
        int ok = 0, fail = 0, degraded = 0, invisible = 0, partially_visible = 0;
        long words_total = 0, unknown_total = 0;
        if (!jf) { fprintf(stderr, "Cannot open %s\n", input_jsonl); return 2; }
        if (!insert_db && !out_path) { usage(); fclose(jf); return 1; }

        /* Remove rows a store should not hold under the current visibility
         * rule, without re-embedding anything. Tokenizing is microseconds and
         * the model is minutes, so tightening the rule after a corpus is
         * stored should not cost a full rebuild. */
        if (prune_invisible) {
            const char *pdir = resolve_model_dir(model);
            char ppath[PATH_MAX];
            Vocab pvocab = {0};
            VecDB pdb = {0};
            sqlite3_stmt *del_meta = NULL, *del_vec = NULL;
            long examined = 0, invisible_n = 0, pruned = 0, prune_failed = 0;
            snprintf(ppath, sizeof(ppath), "%s/vocab.txt", pdir);
            if (!insert_db || vocab_load(&pvocab, ppath) != 0) {
                fprintf(stderr, "[embed] --prune-invisible needs --insert-db and a readable %s\n", ppath);
                fclose(jf);
                return 1;
            }
            if (vec_db_open(&pdb, insert_db, dims) != 0 ||
                sqlite3_prepare_v2(pdb.db, "DELETE FROM artifacts WHERE id=?", -1, &del_meta, NULL) != SQLITE_OK ||
                sqlite3_prepare_v2(pdb.db, "DELETE FROM vec_artifacts WHERE id=?", -1, &del_vec, NULL) != SQLITE_OK) {
                fprintf(stderr, "[embed] cannot open %s for pruning\n", insert_db);
                vocab_free(&pvocab);
                fclose(jf);
                return 1;
            }
            while ((got = getline(&line, &cap, jf)) > 0) {
                char pid[512] = {0};
                char *ptext;
                int w = 0, u = 0, m = 0, ok = 1;
                if (got < 2) continue;
                if (!bf_lex_json_string(line, id_field, pid, sizeof(pid)) || !pid[0]) continue;
                ptext = malloc((size_t)got + 1);
                if (!ptext) continue;
                if (!bf_lex_json_string(line, text_field, ptext, (size_t)got + 1)) { free(ptext); continue; }
                examined++;
                wordpiece_visibility(&pvocab, ptext, &w, &u, &m);
                free(ptext);
                if (!visibility_refuses(w, u, m)) continue;
                invisible_n++;
                if (sqlite3_exec(pdb.db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) { prune_failed++; continue; }
                sqlite3_reset(del_meta); sqlite3_bind_text(del_meta, 1, pid, -1, SQLITE_TRANSIENT);
                ok = sqlite3_step(del_meta) == SQLITE_DONE;
                {
                    int removed = sqlite3_changes(pdb.db);
                    sqlite3_reset(del_meta);
                    if (ok) {
                        sqlite3_reset(del_vec); sqlite3_bind_text(del_vec, 1, pid, -1, SQLITE_TRANSIENT);
                        ok = sqlite3_step(del_vec) == SQLITE_DONE;
                        sqlite3_reset(del_vec);
                    }
                    if (!ok || sqlite3_exec(pdb.db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) {
                        sqlite3_exec(pdb.db, "ROLLBACK", NULL, NULL, NULL);
                        prune_failed++;
                        continue;
                    }
                    if (removed > 0) pruned++;
                }
            }
            free(line);
            fclose(jf);
            sqlite3_finalize(del_meta);
            sqlite3_finalize(del_vec);
            vec_db_close(&pdb);
            vocab_free(&pvocab);
            printf("{\"schema\":\"bonfyre-embed-prune.v1\",\"examined\":%ld,"
                   "\"invisible\":%ld,\"pruned\":%ld,\"prune_failed\":%ld,"
                   "\"cannot_infer\":[\"a kept row -> a well-represented text; this removes only "
                   "texts whose unknown words outnumber their meaningful visible words\"]}\n",
                   examined, invisible_n, pruned, prune_failed);
            return prune_failed ? 1 : 0;
        }

        const char *mdir = resolve_model_dir(model);
        fprintf(stderr, "[embed] backend=onnx-native  model=%s\n", mdir);
        char vpath[PATH_MAX];
        snprintf(vpath, sizeof(vpath), "%s/vocab.txt", mdir);
        Vocab vocab = {0};
        int use_onnx = (strcmp(backend, "onnx") == 0 && vocab_load(&vocab, vpath) == 0);
        OrtContext ort_ctx = {0};
        if (use_onnx && ort_session_open(&ort_ctx, mdir) != 0) use_onnx = 0;
        if (!use_onnx && !allow_degraded && strcmp(backend, "onnx") == 0) {
            /* The same refusal the single path makes, and for the same reason:
             * hashed-token geometry is not a learned metric, and a corpus
             * indexed in it answers queries that look like retrieval. */
            fprintf(stderr, "[embed] refusing to embed a corpus degraded: onnx was "
                            "requested and %s has no readable vocab.txt\n", mdir);
            fclose(jf);
            return 1;
        }

        /* The vector table is opened at the width the model actually produces,
         * not at the flag's default.
         *
         * --dims defaults to 384 and this model emits 768, so opening the
         * table before the first embedding creates a float[384] column and
         * every insert afterwards is a dimension mismatch. The first version
         * of this did exactly that, counted 200 rows embedded, and never
         * looked at what vec_db_insert returned -- a corpus that reports
         * success and stores nothing usable is worse than one that fails. */
        VecDB vdb = {0};
        int vdb_open = 0, vec_dims = 0, stored = 0, insert_failed = 0;

        while ((got = getline(&line, &cap, jf)) > 0) {
            char id[512] = {0}, *text = NULL;
            float *embedding = NULL;
            int edims = dims;
            size_t tlen = 0;
            if (got < 2) continue;
            if (!bf_lex_json_string(line, id_field, id, sizeof(id)) || !id[0]) { fail++; continue; }
            text = malloc((size_t)got + 1);
            if (!text) { fail++; continue; }
            if (!bf_lex_json_string(line, text_field, text, (size_t)got + 1) || !text[0]) {
                free(text); text = NULL; fail++; continue;
            }
            tlen = strlen(text);
            if (use_onnx) {
                int words = 0, unknown = 0, meaningful = 0;
                wordpiece_visibility(&vocab, text, &words, &unknown, &meaningful);
                words_total += words;
                unknown_total += unknown;
                if (visibility_refuses(words, unknown, meaningful)) {
                    /* Nothing of this text is visible to the model, so the
                     * vector would encode its word count and nothing else. It
                     * is not stored: a row that is near every other row of the
                     * same length is worse than a missing row, because it
                     * answers queries. */
                    invisible++;
                    free(text);
                    continue;
                }
                if (unknown > 0) partially_visible++;
                if (ort_embed_text(&ort_ctx, &vocab, text, &embedding, &edims) != 0)
                    embedding = NULL;
            }
            if (!embedding) {
                TokenList toks = normalize_tokens(text);
                embedding = build_hash_embedding(&toks, edims);
                token_list_free(&toks);
                degraded++;
            }
            if (!embedding) { free(text); fail++; continue; }
            if (insert_db) {
                if (!vdb_open) {
                    vec_dims = edims;
                    if (vec_db_open(&vdb, insert_db, vec_dims) != 0) {
                        fprintf(stderr, "[embed] Cannot open vec DB for batch\n");
                        free(embedding); free(text); free(line); fclose(jf);
                        if (use_onnx) { ort_session_close(&ort_ctx); vocab_free(&vocab); }
                        return 1;
                    }
                    vdb_open = 1;
                    fprintf(stderr, "[embed] vector store opened at %d dims\n", vec_dims);
                }
                if (edims != vec_dims) {
                    /* One corpus, one geometry. A row of a different width is
                     * not a row that compares badly, it is one that cannot be
                     * compared at all. */
                    fprintf(stderr, "[embed] %s embedded at %d dims into a %d-dim store; "
                                    "refused\n", id, edims, vec_dims);
                    insert_failed++;
                } else if (vec_db_insert(&vdb, id, text, embedding, edims) != 0) {
                    insert_failed++;
                } else {
                    stored++;
                }
            }
            free(embedding);
            free(text);
            ok++;
        }
        free(line);
        fclose(jf);
        if (vdb_open) vec_db_close(&vdb);
        if (use_onnx) { ort_session_close(&ort_ctx); vocab_free(&vocab); }
        fprintf(stderr, "[embed] jsonl batch: %d embedded, %d stored, %d failed, "
                        "%d not stored, %d degraded, %d invisible to the vocabulary, "
                        "%d partially visible\n",
                ok, stored, fail, insert_failed, degraded, invisible, partially_visible);
        printf("{\"schema\":\"bonfyre-embed-batch.v1\",\"embedded\":%d,\"stored\":%d,"
               "\"failed\":%d,\"not_stored\":%d,\"degraded\":%d,"
               "\"invisible_to_vocabulary\":%d,\"partially_visible\":%d,"
               "\"content_words\":%ld,\"unknown_words\":%ld,\"dims\":%d,"
               "\"backend\":\"%s\","
               "\"cannot_infer\":[\"an embedding -> a meaning; distances are comparable "
               "only within one model and one geometry\","
               "\"embedded -> stored; a vector that did not reach the store answers nothing\","
               "\"partially visible -> faithfully represented; unknown words are dropped "
               "to a single token each\"]}\n",
               ok, stored, fail, insert_failed, degraded, invisible, partially_visible,
               words_total, unknown_total,
               vec_dims ? vec_dims : dims,
               use_onnx ? "onnx-runtime-native" : "hashed-token-native");
        return (fail && !ok) || insert_failed ? 1 : 0;
    }

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
                if (vec_db_insert(&vdb, fid, ft ? ft : "", embedding, edims) != 0) fail++;
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

    /* ── Embed cache: hash the complete embedding contract ─── *
     * Text alone is not sufficient: a 256-d hash fallback cannot
     * satisfy a 384-d vector-store request for the same document. */
    uint8_t embed_hash[32];
    int cache_eligible = strcmp(backend, "onnx") == 0;
    {
        BfSha256 _sha;
        static const char cache_schema[] = "bonfyre-embed-cache-v3";
        char requested_dims[32];
        snprintf(requested_dims, sizeof(requested_dims), "%d", dims);
        bf_sha256_init(&_sha);
        bf_sha256_update(&_sha, (const uint8_t *)cache_schema, sizeof(cache_schema));
        bf_sha256_update(&_sha, (const uint8_t *)text, text_len);
        bf_sha256_update(&_sha, (const uint8_t *)"\\0", 1);
        bf_sha256_update(&_sha, (const uint8_t *)backend, strlen(backend));
        bf_sha256_update(&_sha, (const uint8_t *)"\\0", 1);
        bf_sha256_update(&_sha, (const uint8_t *)model, strlen(model));
        bf_sha256_update(&_sha, (const uint8_t *)"\\0", 1);
        bf_sha256_update(&_sha, (const uint8_t *)requested_dims, strlen(requested_dims));
        /* Runtime upgrades can change the vector produced by the same graph.
         * Model names also survive replaced weights and vocabularies. Hash
         * the actual inputs before admitting a cached ONNX representation. */
        if (cache_eligible) {
            const char *mdir = resolve_model_dir(model);
            const char *runtime = OrtGetApiBase()->GetVersionString();
            char onnx_path[PATH_MAX], vocab_path[PATH_MAX];
            char model_digest[65], vocab_digest[65];
            snprintf(vocab_path, sizeof(vocab_path), "%s/vocab.txt", mdir);
            if (resolve_onnx_path(mdir, onnx_path, sizeof(onnx_path)) != 0 ||
                bf_sha256_file(onnx_path, model_digest) != 0 ||
                bf_sha256_file(vocab_path, vocab_digest) != 0) {
                cache_eligible = 0;
            } else {
                bf_sha256_update(&_sha, (const uint8_t *)runtime, strlen(runtime) + 1);
                bf_sha256_update(&_sha, (const uint8_t *)model_digest, 64);
                bf_sha256_update(&_sha, (const uint8_t *)vocab_digest, 64);
            }
        }
        bf_sha256_final(&_sha, embed_hash);
    }

    float *embedding = NULL;
    const char *backend_label = NULL;
    uint32_t cached_dim = 0;

    if (cache_eligible && bf_embed_lookup(embed_hash, &embedding, &cached_dim) == 0) {
        dims = (int)cached_dim;
        backend_label = "embed-cache";
        fprintf(stderr, "[embed] cache hit  dims=%d\n", dims);
    }

    /* What was asked for, kept separately from what was produced.
     *
     * A requested ONNX/Nomic embedding whose vocab is missing degraded to a
     * hashed-token vector, warned on stderr, and exited 0. An orchestrator sees
     * the exit status and concludes the model ran. It did not, and the two are
     * not interchangeable: hashed-token geometry is not a learned metric, so
     * every downstream distance threshold calibrated on one is meaningless on
     * the other. Silence is the defect -- the fallback itself is a reasonable
     * thing to want, once it is something the caller chose. */
    const char *backend_requested = backend;
    const char *degraded_because = NULL;

    /* ONNX backend (only on cache miss) */
    if (!embedding && strcmp(backend, "onnx") == 0) {
        const char *mdir = resolve_model_dir(model);
        fprintf(stderr, "[embed] backend=onnx-native  model=%s\n", mdir);
        char vpath[PATH_MAX];
        snprintf(vpath, sizeof(vpath), "%s/vocab.txt", mdir);
        Vocab vocab = {0};
        if (vocab_load(&vocab, vpath) != 0) {
            degraded_because = "the model directory has no readable vocab.txt";
            fprintf(stderr, "[embed] Cannot load vocab: %s — falling back to hash\n", vpath);
            backend = "hash";
        } else {
            int odims = 0;
            if (run_onnx_embed(&vocab, text, mdir, &embedding, &odims) == 0 && embedding) {
                dims = odims;
                backend_label = "onnx-runtime-native";
                /* Persist for all future callers — atomic, idempotent */
                if (cache_eligible) bf_embed_store(embed_hash, embedding, (uint32_t)dims);
            } else {
                degraded_because = "the ONNX runtime did not produce a vector";
                fprintf(stderr, "[embed] ONNX failed, falling back to hash\n");
                backend = "hash";
            }
            vocab_free(&vocab);
        }
    }

    /* Hash fallback (cache miss + no ONNX — not stored; FNV != semantic) */
    if (!embedding && strcmp(backend, "hash") == 0) {
        if (dims <= 0) dims = 768;
        TokenList toks = normalize_tokens(text);
        embedding = build_hash_embedding(&toks, dims);
        token_list_free(&toks);
        backend_label = "hashed-token-native";
    }

    free(text);
    if (!embedding) { fprintf(stderr, "Embedding failed\n"); return 1; }

    /* Fail closed on an unrequested degradation. The caller who wants the
     * fallback says so and gets it labelled; the caller who does not is told
     * rather than handed a differently-shaped space with a zero exit. */
    if (degraded_because && !allow_degraded) {
        fprintf(stderr,
                "[embed] refusing to return a degraded representation: %s was requested, "
                "%s was produced, because %s.\n"
                "        Hashed-token geometry is not a learned metric and distances are "
                "not comparable across the two.\n"
                "        Install the model's vocab.txt, or pass --allow-degraded to accept "
                "the fallback with the receipt labelled.\n",
                backend_requested, backend_label, degraded_because);
        free(embedding);
        return 3;
    }
    if (degraded_because) {
        fprintf(stderr, "[embed] degraded: requested=%s obtained=%s because %s\n",
                backend_requested, backend_label, degraded_because);
    }

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
    int insert_failed_single = 0;
        VecDB vdb = {0};
        if (vec_db_open(&vdb, insert_db, dims) != 0) {
            free(embedding); return 1;
        }
        char *t_text = read_file_contents(text_path, NULL);
        if (vec_db_insert(&vdb, doc_id, t_text ? t_text : "", embedding, dims) != 0) {
            /* A row that did not commit is not in the store; saying otherwise is
             * the silent loss this writer used to commit. */
            fprintf(stderr, "[embed] %s was embedded and not stored\n", doc_id);
            insert_failed_single = 1;
        }
        vec_db_close(&vdb);
        if (insert_failed_single) {
            free(t_text);
            free(embedding);
            return 1;
        }
        printf("Inserted %s into %s  [%d dims, %s]\n", doc_id, insert_db, dims, backend_label);
        if (degraded_because) {
            printf("  requested %s, obtained %s (%s)\n",
                   backend_requested, backend_label, degraded_because);
        }
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
        if (degraded_because) {
            printf("  requested %s, obtained %s (%s)\n",
                   backend_requested, backend_label, degraded_because);
        }
        printf("Wrote metadata to %s\n", meta_out);
        token_list_free(&mt);
    }

    free(embedding);
    return 0;
}
