/* bf_static_embed.c — see include/bf_static_embed.h.
 *
 * The tokenizer here is BonfyreEmbed's uncased BERT WordPiece, kept to the
 * same behaviour byte for byte: Unicode-aware pre-tokenization with the fold
 * table the .bfse carries, greedy longest match over an ASCII-only trie, and
 * a word that cannot be finished keeping its matched pieces and adding
 * [UNK]. The browser reader repeats these rules, and the parity test holds
 * all three to the same vectors. */
#include "bf_static_embed.h"
#include "bonfyre.h"

#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ST_ALPHA 128
#define ST_CAP 8192

typedef struct { int id; int child[ST_ALPHA]; } StNode;
typedef struct { StNode *pool; int count, cap; } StTrie;
typedef struct { uint32_t cp; char out[12]; } StFold;

struct BfStaticModel {
    int count, dims, cls, sep, unk;
    StTrie main, sub;
    StFold *fold; int fold_count;
    float *scales; int8_t *rows;
    uint8_t sha256[32];
};

static int trie_init(StTrie *t) {
    t->cap = 64 * 1024; t->count = 1;
    t->pool = calloc((size_t)t->cap, sizeof(StNode));
    if (!t->pool) return -1;
    t->pool[0].id = -1;
    return 0;
}
static int trie_insert(StTrie *t, const char *key, int id) {
    int node = 0;
    for (const unsigned char *p = (const unsigned char *)key; *p; p++) {
        int c = *p < ST_ALPHA ? *p : 0;
        if (!t->pool[node].child[c]) {
            if (t->count >= t->cap) {
                StNode *grown = realloc(t->pool, (size_t)t->cap * 2 * sizeof(StNode));
                if (!grown) return -1;
                t->pool = grown; t->cap *= 2;
            }
            int n = t->count++;
            memset(&t->pool[n], 0, sizeof(StNode)); t->pool[n].id = -1;
            t->pool[node].child[c] = n;
        }
        node = t->pool[node].child[c];
    }
    t->pool[node].id = id;
    return 0;
}
static int trie_longest(const StTrie *t, const char *s, size_t len, size_t *match) {
    int node = 0, best = -1; size_t best_len = 0;
    for (size_t i = 0; i < len; i++) {
        int c = (unsigned char)s[i];
        if (c >= ST_ALPHA || !t->pool[node].child[c]) break;
        node = t->pool[node].child[c];
        if (t->pool[node].id >= 0) { best = t->pool[node].id; best_len = i + 1; }
    }
    *match = best_len;
    return best;
}

static unsigned char *read_whole(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return NULL; }
    long size = ftell(f);
    if (size < 0 || fseek(f, 0, SEEK_SET) != 0) { fclose(f); return NULL; }
    unsigned char *buf = malloc((size_t)size + 1);
    if (!buf || fread(buf, 1, (size_t)size, f) != (size_t)size) { free(buf); fclose(f); return NULL; }
    fclose(f); *len = (size_t)size;
    return buf;
}

static uint32_t rd32(const unsigned char *p) { return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24); }

void bf_static_free(BfStaticModel *m) {
    if (!m) return;
    free(m->main.pool); free(m->sub.pool); free(m->fold); free(m->scales); free(m->rows); free(m);
}

BfStaticModel *bf_static_load(const char *path, char *err, size_t err_size) {
#define FAIL(msg) do { if (err) snprintf(err, err_size, "%s", msg); free(b); bf_static_free(m); return NULL; } while (0)
    size_t len = 0;
    unsigned char *b = read_whole(path, &len);
    BfStaticModel *m = calloc(1, sizeof(*m));
    if (!m) FAIL("out of memory");
    if (!b) FAIL("cannot read the model file");
    if (len < 44 || memcmp(b, "BFSE", 4) || rd32(b + 4) != 1) FAIL("not a version 1 .bfse file");
    m->count = (int)rd32(b + 8); m->dims = (int)rd32(b + 12);
    m->cls = (int)rd32(b + 16); m->sep = (int)rd32(b + 20); m->unk = (int)rd32(b + 24);
    uint32_t vocab_bytes = rd32(b + 28), fold_count = rd32(b + 32), fold_bytes = rd32(b + 36);
    size_t tables = (44 + (size_t)vocab_bytes + fold_bytes + 3) & ~(size_t)3;
    if (m->count <= 0 || m->dims <= 0 || m->dims > 4096 || tables + (size_t)m->count * (4 + (size_t)m->dims) > len)
        FAIL("truncated .bfse file");
    if (trie_init(&m->main) || trie_init(&m->sub)) FAIL("out of memory");
    size_t start = 44, end = 44 + vocab_bytes;
    int id = 0;
    char token[512];
    for (size_t i = start; i <= end; i++) {
        if (i < end && b[i] != '\n') continue;
        size_t n = i - start;
        if (n >= sizeof token) n = sizeof token - 1;
        memcpy(token, b + start, n); token[n] = 0;
        if (trie_insert(&m->main, token, id)) FAIL("out of memory");
        if (n > 2 && token[0] == '#' && token[1] == '#' && trie_insert(&m->sub, token + 2, id)) FAIL("out of memory");
        id++; start = i + 1;
    }
    if (id != m->count) FAIL("vocabulary size does not match the header");
    m->fold = calloc(fold_count ? fold_count : 1, sizeof(StFold));
    if (!m->fold) FAIL("out of memory");
    for (size_t at = end, k = 0; k < fold_count; k++) {
        unsigned char n = b[at + 4];
        if (n >= sizeof m->fold[k].out || at + 5 + n > end + fold_bytes) FAIL("bad fold table");
        m->fold[k].cp = rd32(b + at); memcpy(m->fold[k].out, b + at + 5, n); m->fold[k].out[n] = 0;
        at += 5 + n;
    }
    m->fold_count = (int)fold_count;
    m->scales = malloc(sizeof(float) * (size_t)m->count);
    m->rows = malloc((size_t)m->count * m->dims);
    if (!m->scales || !m->rows) FAIL("out of memory");
    for (int i = 0; i < m->count; i++) { uint32_t bits = rd32(b + tables + 4 * (size_t)i); memcpy(&m->scales[i], &bits, 4); }
    memcpy(m->rows, b + tables + 4 * (size_t)m->count, (size_t)m->count * m->dims);
    BfSha256 h; bf_sha256_init(&h); bf_sha256_update(&h, b, len); bf_sha256_final(&h, m->sha256);
    free(b);
    return m;
#undef FAIL
}

int bf_static_dims(const BfStaticModel *m) { return m ? m->dims : 0; }
const uint8_t *bf_static_model_sha256(const BfStaticModel *m) { return m ? m->sha256 : NULL; }

static int decode_cp(const unsigned char *s, size_t len, uint32_t *cp) {
    if (s[0] < 0x80) { *cp = s[0]; return 1; }
    if ((s[0] & 0xE0) == 0xC0 && len >= 2 && (s[1] & 0xC0) == 0x80) { *cp = ((uint32_t)(s[0] & 0x1F) << 6) | (s[1] & 0x3F); return 2; }
    if ((s[0] & 0xF0) == 0xE0 && len >= 3 && (s[1] & 0xC0) == 0x80 && (s[2] & 0xC0) == 0x80) {
        *cp = ((uint32_t)(s[0] & 0x0F) << 12) | ((uint32_t)(s[1] & 0x3F) << 6) | (s[2] & 0x3F); return 3;
    }
    if ((s[0] & 0xF8) == 0xF0 && len >= 4 && (s[1] & 0xC0) == 0x80 && (s[2] & 0xC0) == 0x80 && (s[3] & 0xC0) == 0x80) {
        *cp = ((uint32_t)(s[0] & 0x07) << 18) | ((uint32_t)(s[1] & 0x3F) << 12) | ((uint32_t)(s[2] & 0x3F) << 6) | (s[3] & 0x3F); return 4;
    }
    *cp = 0xFFFD; return 1;
}
static int encode_cp(uint32_t cp, char *out) {
    if (cp < 0x80) { out[0] = (char)cp; return 1; }
    if (cp < 0x800) { out[0] = (char)(0xC0 | (cp >> 6)); out[1] = (char)(0x80 | (cp & 0x3F)); return 2; }
    if (cp < 0x10000) { out[0] = (char)(0xE0 | (cp >> 12)); out[1] = (char)(0x80 | ((cp >> 6) & 0x3F)); out[2] = (char)(0x80 | (cp & 0x3F)); return 3; }
    out[0] = (char)(0xF0 | (cp >> 18)); out[1] = (char)(0x80 | ((cp >> 12) & 0x3F)); out[2] = (char)(0x80 | ((cp >> 6) & 0x3F)); out[3] = (char)(0x80 | (cp & 0x3F)); return 4;
}
static int is_space(uint32_t cp) {
    return cp == ' ' || cp == '\t' || cp == '\n' || cp == '\r' || cp == 0x0B || cp == 0x0C || cp == 0xA0 ||
           (cp >= 0x2000 && cp <= 0x200B) || cp == 0x202F || cp == 0x205F || cp == 0x3000 || cp == 0xFEFF;
}
static int is_control(uint32_t cp) { return cp < 0x20 || (cp >= 0x7F && cp <= 0x9F) || cp == 0xFFFD; }
static int is_punct(uint32_t cp) {
    if (cp < 0x80) return ispunct((int)cp) != 0;
    if (cp >= 0xA1 && cp <= 0xBF && cp != 0xAA && cp != 0xB5 && cp != 0xBA) return 1;
    if (cp == 0xD7 || cp == 0xF7) return 1;
    if ((cp >= 0x2010 && cp <= 0x2027) || (cp >= 0x2030 && cp <= 0x205E) || (cp >= 0x3001 && cp <= 0x303F)) return 1;
    if ((cp >= 0xFF01 && cp <= 0xFF0F) || (cp >= 0xFF1A && cp <= 0xFF20)) return 1;
    return cp == 0x060C || cp == 0x061B || cp == 0x061F || cp == 0x06D4 || cp == 0x0964 || cp == 0x0965 || cp == 0x0589 || cp == 0x05BE;
}
static int is_cjk(uint32_t cp) {
    return (cp >= 0x4E00 && cp <= 0x9FFF) || (cp >= 0x3400 && cp <= 0x4DBF) || (cp >= 0xF900 && cp <= 0xFAFF) ||
           (cp >= 0x20000 && cp <= 0x2A6DF) || (cp >= 0x2F800 && cp <= 0x2FA1F);
}
static const char *fold_of(const BfStaticModel *m, uint32_t cp) {
    int lo = 0, hi = m->fold_count;
    if (cp < 0x80) return NULL;
    while (lo < hi) { int mid = (lo + hi) / 2; if (m->fold[mid].cp == cp) return m->fold[mid].out; if (m->fold[mid].cp < cp) lo = mid + 1; else hi = mid; }
    return NULL;
}

/* Emits token ids for one finished word, as wordpiece_encode does. */
static int add_word(const BfStaticModel *m, const char *w, size_t wlen, int *ids, int n) {
    size_t start = 0; int bad = 0;
    while (start < wlen && n < ST_CAP - 1) {
        size_t match = 0;
        int id = start == 0 ? trie_longest(&m->main, w, wlen, &match) : trie_longest(&m->sub, w + start, wlen - start, &match);
        if (id < 0 || match == 0) { bad = 1; break; }
        ids[n++] = id; start += match;
    }
    if (bad && n < ST_CAP - 1) ids[n++] = m->unk;
    return n;
}

int bf_static_embed(const BfStaticModel *m, const char *text, float *out) {
    static int ids[ST_CAP];
    const unsigned char *s = (const unsigned char *)text;
    size_t len = strlen(text), i = 0, wn = 0;
    char *word = malloc(len * 8 + 16);
    int n = 0;
    for (int d = 0; d < m->dims; d++) out[d] = 0;
    if (!word) return 0;
    ids[n++] = m->cls;
    while (i < len && n < ST_CAP - 1) {
        uint32_t cp; char enc[4];
        i += (size_t)decode_cp(s + i, len - i, &cp);
        if (is_space(cp) || is_control(cp)) { if (wn) { n = add_word(m, word, wn, ids, n); wn = 0; } continue; }
        if (is_punct(cp) || is_cjk(cp)) {
            if (wn) { n = add_word(m, word, wn, ids, n); wn = 0; }
            if (n < ST_CAP - 1) n = add_word(m, enc, (size_t)encode_cp(cp, enc), ids, n);
            continue;
        }
        if (cp < 0x80) { word[wn++] = (char)tolower((int)cp); continue; }
        const char *folded = fold_of(m, cp);
        if (folded) { size_t fl = strlen(folded); memcpy(word + wn, folded, fl); wn += fl; }
        else wn += (size_t)encode_cp(cp, word + wn);
    }
    if (wn && n < ST_CAP - 1) n = add_word(m, word, wn, ids, n);
    free(word);
    int used = 0;
    for (int t = 0; t < n; t++) {
        int id = ids[t];
        if (id == m->cls || id == m->sep || id == m->unk || id < 0 || id >= m->count || m->scales[id] == 0) continue;
        const int8_t *r = m->rows + (size_t)id * m->dims;
        for (int d = 0; d < m->dims; d++) out[d] += r[d] * m->scales[id];
        used++;
    }
    double norm = 0;
    for (int d = 0; d < m->dims; d++) norm += (double)out[d] * out[d];
    norm = sqrt(norm);
    if (norm > 0) for (int d = 0; d < m->dims; d++) out[d] = (float)(out[d] / norm);
    return used;
}

static int put32(FILE *f, uint32_t v) { unsigned char b[4] = {(unsigned char)v, (unsigned char)(v >> 8), (unsigned char)(v >> 16), (unsigned char)(v >> 24)}; return fwrite(b, 1, 4, f) == 4 ? 0 : -1; }

int bf_static_index_write(const char *path, const BfStaticModel *m, int count, const float *vectors) {
    char tmp[4096];
    if (snprintf(tmp, sizeof tmp, "%s.tmp", path) >= (int)sizeof tmp) return -1;
    FILE *f = fopen(tmp, "wb");
    if (!f) return -1;
    int bad = fwrite("BFSI", 1, 4, f) != 4 || put32(f, 1) || put32(f, (uint32_t)m->dims) || put32(f, (uint32_t)count) ||
              fwrite(m->sha256, 1, 32, f) != 32;
    int8_t *row = malloc((size_t)m->dims);
    float *scales = malloc(sizeof(float) * (size_t)(count ? count : 1));
    if (!row || !scales) bad = 1;
    for (int i = 0; !bad && i < count; i++) {
        float top = 0;
        for (int d = 0; d < m->dims; d++) { float a = fabsf(vectors[(size_t)i * m->dims + d]); if (a > top) top = a; }
        scales[i] = top > 0 ? top / 127.0f : 1.0f;
        uint32_t bits; memcpy(&bits, &scales[i], 4); bad |= put32(f, bits);
    }
    for (int i = 0; !bad && i < count; i++) {
        for (int d = 0; d < m->dims; d++) { long q = lroundf(vectors[(size_t)i * m->dims + d] / scales[i]); row[d] = (int8_t)(q > 127 ? 127 : q < -127 ? -127 : q); }
        bad |= fwrite(row, 1, (size_t)m->dims, f) != (size_t)m->dims;
    }
    free(row); free(scales);
    if (fclose(f) != 0) bad = 1;
    if (bad || rename(tmp, path) != 0) { remove(tmp); return -1; }
    return 0;
}
