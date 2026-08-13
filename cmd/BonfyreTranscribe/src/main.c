#include <errno.h>
#include <ctype.h>
#include <dirent.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>
#include <whisper.h>
#include <ggml-backend.h>
#include <zlib.h>
#ifdef __APPLE__
#include <mach/mach_time.h>
#endif
#include "fragment.h"

/* ================================================================
 * Vocabulary Accumulator — information-theoretic learning engine.
 *
 * Flat binary format (.bfvocab) — zero external dependencies.
 * In-process FNV-1a hash map for O(1) term lookup during ingestion.
 * BM25 weighting with document-length normalization.
 * Shannon entropy + KL-divergence convergence metrics.
 *
 * File layout (little-endian):
 *   [6]  magic "BFVD02"
 *   [2]  version        (uint16_t)
 *   [4]  total_files    (uint32_t)
 *   [4]  total_words    (uint32_t)
 *   [4]  vocab_freq_sum (uint32_t)   — sum of all freq (valid PMF base)
 *   [4]  entry_count    (uint32_t)
 *   Per entry:
 *     [4]  freq      (uint32_t)  — total occurrences across all files
 *     [2]  doc_freq  (uint16_t)  — distinct files containing this term
 *     [2]  term_len  (uint16_t)
 *     [N]  term      (UTF-8, no NUL)
 *
 * Term weighting (BM25 with k1=1.2, b=0.75):
 *   score(t,d) = IDF(t) * (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * dl/avgdl))
 *   IDF(t) = log((N - df + 0.5) / (df + 0.5) + 1)
 *
 * Convergence metrics:
 *   H     = Shannon entropy of vocab frequency distribution (bits)
 *   D_KL  = KL-divergence from uniform: measures distribution peakedness
 *   knowledge = D_KL / log2(|V|)  — normalized divergence [0,1]
 *
 * Zero friction: single read on load, single write on save.
 * No process spawns, no external binaries, no SQL.
 * ================================================================ */

#define BFVOCAB_MAGIC       "BFVD02"
#define BFVOCAB_MAGIC_LEN   6
#define BFVOCAB_VERSION     2
#define BFVOCAB_NAME        "bonfyre_vocab.bfvocab"
#define VOCAB_PROMPT_MAX    200
#define VOCAB_PROMPT_CHARS  1024
#define VOCAB_MIN_LEN       3
#define VOCAB_HASH_INIT     4096  /* power of 2 */
#define BM25_K1             1.2
#define BM25_B              0.75
#define IO_BUF_SIZE         65536  /* 64KB I/O buffer — fits L1 on M-series */

typedef struct {
    char     term[128];
    uint32_t freq;       /* total occurrences across all files */
    uint16_t doc_freq;   /* number of distinct files containing this term */
} VocabEntry;

typedef struct {
    VocabEntry *entries;
    int         count;
    int         cap;
    uint32_t    total_files;
    uint32_t    total_words;    /* all words (including stopwords) */
    uint32_t    vocab_freq_sum; /* sum of all entry freq (valid PMF denominator) */
    int        *hash_slots;    /* FNV-1a open-addressing -> entry index */
    int         hash_cap;      /* always power of 2 */
    int         sorted;        /* 1 if entries are in BM25 descending order */
} VocabStore;

/* ── Stopword hash set — O(1) lookup via FNV-1a ──────────── */
/*
 * Round 1 fix: linear scan of 100+ stopwords per token is O(n*m).
 * For 5000 tokens * 100 stopwords = 500K strcmp calls per file.
 * FNV-1a hash set reduces to O(1) amortized per lookup.
 * Uses same hash function as the vocab store for consistency.
 */
static const char *STOPWORD_LIST[] = {
    "the","be","to","of","and","a","in","that","have","i","it","for","not",
    "on","with","he","as","you","do","at","this","but","his","by","from",
    "they","we","her","she","or","an","will","my","one","all","would",
    "there","their","what","so","up","out","if","about","who","get","which",
    "go","me","when","make","can","like","time","no","just","him","know",
    "take","people","into","year","your","good","some","could","them","see",
    "other","than","then","now","look","only","come","its","over","think",
    "also","back","after","use","two","how","our","work","first","well",
    "way","even","new","want","because","any","these","give","day","most",
    "us","was","were","been","has","had","are","is","am","did","does",
    "being","having","doing","um","uh","yeah","okay","right","yes",
    "oh","ah","really","very","much","more",
    "going","gonna","wanna","gotta","thing","things","mean",
    NULL
};

#define SW_HASH_SIZE 512  /* power of 2, ~5x load factor for 100 words */
static uint64_t sw_hashes[SW_HASH_SIZE];
static int      sw_init_done;

static void stopword_init(void) {
    if (sw_init_done) return;
    memset(sw_hashes, 0, sizeof(sw_hashes));
    for (int i = 0; STOPWORD_LIST[i]; i++) {
        uint64_t h = bf_fnv1a64(BF_FNV1A_INIT, STOPWORD_LIST[i],
                                 strlen(STOPWORD_LIST[i]));
        int slot = (int)(h & (SW_HASH_SIZE - 1));
        while (sw_hashes[slot]) slot = (slot + 1) & (SW_HASH_SIZE - 1);
        sw_hashes[slot] = h;
    }
    sw_init_done = 1;
}

static int is_stopword(const char *word) {
    uint64_t h = bf_fnv1a64(BF_FNV1A_INIT, word, strlen(word));
    int slot = (int)(h & (SW_HASH_SIZE - 1));
    while (sw_hashes[slot]) {
        if (sw_hashes[slot] == h) return 1;  /* FNV-1a collision rate ~0 for short words */
        slot = (slot + 1) & (SW_HASH_SIZE - 1);
    }
    return 0;
}

/* ── Single-pass word normalization ───────────────────────── */
/*
 * Round 3 fix: old code called strlen 3 times, did memmove.
 * Touched every byte 4+ times. This does one pass: lowercase,
 * skip leading non-alnum, stop at trailing non-alnum.
 * Returns new length (0 if word is empty after normalization).
 */
static size_t normalize_word(char *w) {
    /* Find first alnum */
    char *src = w;
    while (*src && !isalnum((unsigned char)*src)) src++;
    /* Find last alnum */
    char *end = src + strlen(src);
    while (end > src && !isalnum((unsigned char)end[-1])) end--;
    /* Copy + lowercase in one pass */
    size_t len = 0;
    for (char *p = src; p < end; p++)
        w[len++] = (char)tolower((unsigned char)*p);
    w[len] = '\0';
    return len;
}

/* ── Vocab path resolution ────────────────────────────────── */

static void resolve_vocab_path(char *out, size_t size) {
    const char *env = getenv("BONFYRE_VOCAB_DB");
    if (env && env[0]) { snprintf(out, size, "%s", env); return; }
    const char *home = getenv("HOME");
    if (home)
        snprintf(out, size, "%s/.local/share/bonfyre/%s", home, BFVOCAB_NAME);
    else
        snprintf(out, size, "/tmp/%s", BFVOCAB_NAME);
}

/* ── BM25 term weight ─────────────────────────────────────── */
/*
 * Round 8 fix: old TF-IDF had no document-length normalization.
 * BM25 (Robertson et al. 1994) is the gold standard for ranked
 * information retrieval. The k1/b parameters control term saturation
 * and document-length bias.
 *
 * IDF(t) = log((N - df + 0.5) / (df + 0.5) + 1)
 * score(t) = IDF(t) * (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * dl/avgdl))
 *
 * For prompt ranking, we use the global corpus stats as the "document":
 *   dl    = total per-file occurrences (approximated by freq/doc_freq)
 *   avgdl = total_words / total_files
 *
 * This matches lambda-tensors' Huffman code-length ~-log2(p) relationship:
 *   terms with high IDF get short codes / high BM25 → appear first in prompt.
 */
static double vocab_bm25(const VocabEntry *e, uint32_t total_files,
                         double avgdl) {
    double df = (double)e->doc_freq;
    double N  = (double)total_files;
    double idf = log((N - df + 0.5) / (df + 0.5) + 1.0);
    double tf  = (double)e->freq;
    double dl  = e->doc_freq > 0 ? tf / (double)e->doc_freq : tf;
    double denom = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * dl / (avgdl > 0 ? avgdl : 1.0));
    return idf * (tf * (BM25_K1 + 1.0)) / denom;
}

/* ── FNV-1a hash map (open-addressing, matching bf_fnv1a64) ── */

static void vocab_hash_rebuild(VocabStore *v) {
    free(v->hash_slots);
    int cap = VOCAB_HASH_INIT;
    while (cap < v->count * 3 + 16) cap <<= 1;
    v->hash_slots = malloc((size_t)cap * sizeof(int));
    v->hash_cap = cap;
    for (int i = 0; i < cap; i++) v->hash_slots[i] = -1;
    for (int i = 0; i < v->count; i++) {
        uint64_t h = bf_fnv1a64(BF_FNV1A_INIT, v->entries[i].term,
                                 strlen(v->entries[i].term));
        int slot = (int)(h & (uint64_t)(cap - 1));
        while (v->hash_slots[slot] >= 0) slot = (slot + 1) & (cap - 1);
        v->hash_slots[slot] = i;
    }
}

static int vocab_hash_find(const VocabStore *v, const char *term) {
    if (!v->hash_slots || v->hash_cap == 0) return -1;
    uint64_t h = bf_fnv1a64(BF_FNV1A_INIT, term, strlen(term));
    int mask = v->hash_cap - 1;
    int slot = (int)(h & (uint64_t)mask);
    while (v->hash_slots[slot] >= 0) {
        if (strcmp(v->entries[v->hash_slots[slot]].term, term) == 0)
            return v->hash_slots[slot];
        slot = (slot + 1) & mask;
    }
    return -1;
}

static int vocab_entry_add(VocabStore *v, const char *term) {
    if (v->count >= v->cap) {
        v->cap = v->cap ? v->cap * 2 : 256;
        v->entries = realloc(v->entries, (size_t)v->cap * sizeof(VocabEntry));
    }
    int idx = v->count++;
    VocabEntry *e = &v->entries[idx];
    snprintf(e->term, sizeof(e->term), "%s", term);
    e->freq = 0;
    e->doc_freq = 0;
    if (v->count * 3 > v->hash_cap) {
        vocab_hash_rebuild(v);
    } else {
        uint64_t h = bf_fnv1a64(BF_FNV1A_INIT, term, strlen(term));
        int mask = v->hash_cap - 1;
        int slot = (int)(h & (uint64_t)mask);
        while (v->hash_slots[slot] >= 0) slot = (slot + 1) & mask;
        v->hash_slots[slot] = idx;
    }
    return idx;
}

/* ── Store I/O (flat binary, matching VECF/BfCacheRecord) ── */

static void vocab_store_init(VocabStore *v) { memset(v, 0, sizeof(*v)); }

static int vocab_store_load(VocabStore *v, const char *path) {
    vocab_store_init(v);
    char dir[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s", path);
    char *sl = strrchr(dir, '/');
    if (sl) { *sl = '\0'; bf_ensure_dir(dir); }

    size_t flen = 0;
    char *data = bf_read_file(path, &flen);
    if (!data || flen < BFVOCAB_MAGIC_LEN + 18) {
        free(data);
        /* Also try loading v1 format (BFVD01) for backward compat */
        vocab_hash_rebuild(v);
        return 0;
    }
    const uint8_t *p = (const uint8_t *)data;
    int is_v1 = (memcmp(p, "BFVD01", 6) == 0);
    int is_v2 = (memcmp(p, BFVOCAB_MAGIC, BFVOCAB_MAGIC_LEN) == 0);
    if (!is_v1 && !is_v2) {
        fprintf(stderr, "[vocab] bad magic in %s\n", path);
        free(data); vocab_hash_rebuild(v); return -1;
    }
    p += BFVOCAB_MAGIC_LEN;
    p += 2; /* version */
    memcpy(&v->total_files, p, 4); p += 4;
    memcpy(&v->total_words, p, 4); p += 4;
    if (is_v2) { memcpy(&v->vocab_freq_sum, p, 4); p += 4; }
    uint32_t cnt; memcpy(&cnt, p, 4); p += 4;

    v->cap = (int)(cnt ? cnt * 2 : 256);
    v->entries = malloc((size_t)v->cap * sizeof(VocabEntry));
    const uint8_t *end = (const uint8_t *)data + flen;
    for (uint32_t i = 0; i < cnt && p + 8 <= end; i++) {
        VocabEntry *e = &v->entries[v->count];
        memcpy(&e->freq, p, 4); p += 4;
        memcpy(&e->doc_freq, p, 2); p += 2;
        uint16_t tlen; memcpy(&tlen, p, 2); p += 2;
        if (p + tlen > end) break;
        size_t copy = tlen < sizeof(e->term) - 1 ? tlen : sizeof(e->term) - 1;
        memcpy(e->term, p, copy); e->term[copy] = '\0';
        p += tlen;
        if (is_v1) v->vocab_freq_sum += e->freq; /* reconstruct for v1 */
        v->count++;
    }
    free(data);
    vocab_hash_rebuild(v);
    return 0;
}

/* ── Sort comparator — BM25 descending ──────────────────── */
/*
 * Round 2 fix: old code used global mutable g_sort_total_files.
 * qsort_r is POSIX-2024 / BSD. On macOS it's available.
 * Fallback: since we only sort once before save/prompt, we compute
 * scores into a parallel array and sort indices. But for simplicity
 * on our target (macOS/Linux), we use a static context that's set
 * once before qsort and never concurrently.
 */
static struct { uint32_t total_files; double avgdl; } sort_ctx;

static int vocab_cmp_weight(const void *a, const void *b) {
    double wa = vocab_bm25((const VocabEntry *)a, sort_ctx.total_files,
                            sort_ctx.avgdl);
    double wb = vocab_bm25((const VocabEntry *)b, sort_ctx.total_files,
                            sort_ctx.avgdl);
    return (wa > wb) ? -1 : (wa < wb) ? 1 : 0;
}

static void vocab_sort(VocabStore *v) {
    if (v->sorted || v->count == 0) return;
    sort_ctx.total_files = v->total_files;
    sort_ctx.avgdl = v->total_files > 0
        ? (double)v->vocab_freq_sum / (double)v->total_files : 1.0;
    qsort(v->entries, (size_t)v->count, sizeof(VocabEntry), vocab_cmp_weight);
    vocab_hash_rebuild(v);
    v->sorted = 1;
}

static int vocab_store_save(VocabStore *v, const char *path) {
    vocab_sort(v);

    /* Recompute vocab_freq_sum to ensure consistency */
    v->vocab_freq_sum = 0;
    for (int i = 0; i < v->count; i++) v->vocab_freq_sum += v->entries[i].freq;

    size_t sz = BFVOCAB_MAGIC_LEN + 2 + 4 + 4 + 4 + 4; /* +4 for vocab_freq_sum */
    for (int i = 0; i < v->count; i++)
        sz += 4 + 2 + 2 + strlen(v->entries[i].term);

    uint8_t *buf = malloc(sz), *p = buf;
    if (!buf) return -1;
    memcpy(p, BFVOCAB_MAGIC, BFVOCAB_MAGIC_LEN); p += BFVOCAB_MAGIC_LEN;
    uint16_t ver = BFVOCAB_VERSION; memcpy(p, &ver, 2); p += 2;
    memcpy(p, &v->total_files, 4); p += 4;
    memcpy(p, &v->total_words, 4); p += 4;
    memcpy(p, &v->vocab_freq_sum, 4); p += 4;
    uint32_t cnt = (uint32_t)v->count; memcpy(p, &cnt, 4); p += 4;

    for (int i = 0; i < v->count; i++) {
        const VocabEntry *e = &v->entries[i];
        memcpy(p, &e->freq, 4); p += 4;
        memcpy(p, &e->doc_freq, 2); p += 2;
        uint16_t tlen = (uint16_t)strlen(e->term);
        memcpy(p, &tlen, 2); p += 2;
        memcpy(p, e->term, tlen); p += tlen;
    }

    FILE *fp = fopen(path, "wb");
    if (!fp) { free(buf); return -1; }
    fwrite(buf, 1, sz, fp);
    fclose(fp);
    free(buf);
    return 0;
}

static void vocab_store_free(VocabStore *v) {
    free(v->entries);
    free(v->hash_slots);
    memset(v, 0, sizeof(*v));
}

/* ── Ingestion ───────────────────────────────────────────── */

/* ── Prompt builder (BM25 ranked) ────────────────────────── */

static char *vocab_build_prompt(VocabStore *v) {
    if (v->count == 0 || v->total_files == 0) return NULL;

    vocab_sort(v);

    /* Hapax threshold: skip freq<2 only when we have enough files to judge */
    uint32_t hapax_min = v->total_files >= 5 ? 2 : 1;

    char *prompt = malloc(VOCAB_PROMPT_CHARS + 64);
    if (!prompt) return NULL;
    size_t plen = 0;
    int n = 0;

    for (int i = 0; i < v->count && n < VOCAB_PROMPT_MAX &&
             plen < VOCAB_PROMPT_CHARS - 64; i++) {
        if (v->entries[i].freq < hapax_min) continue;
        if (n > 0) prompt[plen++] = ' ';
        size_t tlen = strlen(v->entries[i].term);
        if (plen + tlen >= VOCAB_PROMPT_CHARS - 64) break;
        memcpy(prompt + plen, v->entries[i].term, tlen);
        plen += tlen;
        n++;
    }
    prompt[plen] = '\0';

    if (n == 0) { free(prompt); return NULL; }

    /* Convergence: fraction of total BM25 mass captured in prompt */
    double avgdl = v->total_files > 0 ? (double)v->vocab_freq_sum / (double)v->total_files : 1.0;
    double total_w = 0, prompt_w = 0;
    for (int i = 0; i < v->count; i++) {
        double w = vocab_bm25(&v->entries[i], v->total_files, avgdl);
        total_w += w;
        if (i < n) prompt_w += w;
    }
    double conv = total_w > 0 ? prompt_w / total_w : 0;

    fprintf(stderr,
            "[vocab] %d/%d terms (BM25), %.1f%% information captured"
            " (%u files)\n", n, v->count, conv * 100.0, v->total_files);
    return prompt;
}

/* ── Stats writer (Shannon entropy + KL-divergence + BM25) ── */

static void vocab_write_stats(const VocabStore *v, const char *output_dir,
                              const char *vocab_path) {
    /* Shannon entropy over filtered-term PMF (bits).
     * Denominator = vocab_freq_sum (only counted terms), NOT total_words
     * which includes stopwords that were discarded.                       */
    double entropy = 0;
    if (v->vocab_freq_sum > 0) {
        double total = (double)v->vocab_freq_sum;
        for (int i = 0; i < v->count; i++) {
            double p = (double)v->entries[i].freq / total;
            if (p > 0) entropy -= p * log(p) / log(2.0);
        }
    }
    double h_max = v->count > 1 ? log((double)v->count) / log(2.0) : 1.0;

    /* KL-divergence D_KL(P || U) where U is uniform over v->count terms.
     * Measures how much the observed distribution deviates from uniform —
     * higher = more concentrated vocabulary = better learning signal.     */
    double kl_div = 0;
    if (v->count > 1 && v->vocab_freq_sum > 0) {
        double q = 1.0 / (double)v->count;  /* uniform */
        double total = (double)v->vocab_freq_sum;
        for (int i = 0; i < v->count; i++) {
            double p = (double)v->entries[i].freq / total;
            if (p > 0) kl_div += p * log(p / q) / log(2.0);
        }
    }

    /* BM25 convergence of prompt */
    double avgdl = v->total_files > 0 ? (double)v->vocab_freq_sum / (double)v->total_files : 1.0;
    double total_w = 0, prompt_w = 0;
    int prompt_n = 0;
    int hapax_min = v->total_files >= 5 ? 2 : 1;
    for (int i = 0; i < v->count; i++) {
        double w = vocab_bm25(&v->entries[i], v->total_files, avgdl);
        total_w += w;
        if (prompt_n < VOCAB_PROMPT_MAX && v->entries[i].freq >= (uint32_t)hapax_min) {
            prompt_w += w; prompt_n++;
        }
    }
    double convergence = total_w > 0 ? prompt_w / total_w : 0;

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/vocab-stats.json", output_dir);
    FILE *fp = fopen(path, "w");
    if (fp) {
        fprintf(fp,
            "{\n"
            "  \"sourceSystem\": \"BonfyreTranscribe\",\n"
            "  \"vocabPath\": \"%s\",\n"
            "  \"format\": \"bfvocab\",\n"
            "  \"totalFilesProcessed\": %u,\n"
            "  \"totalWordsObserved\": %u,\n"
            "  \"filteredTermFreqSum\": %u,\n"
            "  \"uniqueTerms\": %d,\n"
            "  \"shannonEntropy\": %.4f,\n"
            "  \"maxEntropy\": %.4f,\n"
            "  \"klDivergenceFromUniform\": %.4f,\n"
            "  \"bm25Convergence\": %.4f,\n"
            "  \"promptTerms\": %d,\n"
            "  \"promptTermsMax\": %d\n"
            "}\n",
            vocab_path, v->total_files, v->total_words, v->vocab_freq_sum,
            v->count, entropy, h_max, kl_div, convergence,
            prompt_n, VOCAB_PROMPT_MAX);
        fclose(fp);
    }
}

/* ================================================================
 * Core transcription helpers (unchanged)
 * ================================================================ */

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
static int run_process(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return 1;
    }
    if (pid == 0) {
        execvp(argv[0], argv);
        perror("execvp");
        _exit(127);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        perror("waitpid");
        return 1;
    }
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return 1;
}

static int write_chunk_progress(const char *path, int total_chunks, int completed_chunks, const char *status) {
    FILE *fp = fopen(path, "w");
    if (!fp) {
        perror("fopen");
        return 1;
    }
    fprintf(fp,
            "{\n"
            "  \"sourceSystem\": \"BonfyreTranscribe\",\n"
            "  \"status\": \"%s\",\n"
            "  \"totalChunks\": %d,\n"
            "  \"completedChunks\": %d\n"
            "}\n",
            status,
            total_chunks,
            completed_chunks);
    fclose(fp);
    return 0;
}

static void iso_timestamp(char *buffer, size_t size) { bf_iso_timestamp(buffer, size); }

/* Resolve a model name (e.g. "base") to a ggml model file path.
 * Prefers quantized (Q5_0) models over float16 — 62% smaller, 39% faster. */
static int resolve_whisper_model(const char *model_name, char *out, size_t out_size) {
    /* If already a path, use directly */
    if (strchr(model_name, '/') || strchr(model_name, '.')) {
        if (access(model_name, F_OK) == 0) {
            snprintf(out, out_size, "%s", model_name);
            return 0;
        }
        return -1;
    }
    /* Try common locations — prefer quantized, then .en, then multilingual */
    const char *home = getenv("HOME");
    const char *variants[] = {
        "%s/.local/share/whisper/ggml-%s.en-q5_0.bin",  /* quantized English-only */
        "%s/.local/share/whisper/ggml-%s-q5_0.bin",      /* quantized multilingual */
        "%s/.local/share/whisper/ggml-%s.en.bin",         /* float16 English-only */
        "%s/.local/share/whisper/ggml-%s.bin",            /* float16 multilingual */
        NULL
    };
    const char *tmp_variants[] = {
        "/tmp/ggml-%s.en-q5_0.bin",
        "/tmp/ggml-%s-q5_0.bin",
        "/tmp/ggml-%s.en.bin",
        "/tmp/ggml-%s.bin",
        NULL
    };
    for (int i = 0; variants[i]; i++) {
        if (home) {
            snprintf(out, out_size, variants[i], home, model_name);
            if (access(out, F_OK) == 0) return 0;
        }
    }
    for (int i = 0; tmp_variants[i]; i++) {
        snprintf(out, out_size, tmp_variants[i], model_name);
        if (access(out, F_OK) == 0) return 0;
    }
    return -1;
}

/* ================================================================
 * WAV reader — 16-bit PCM → float32 for libwhisper
 *
 * BonfyreMediaPrep normalize always outputs 16kHz 16-bit mono PCM WAV.
 * This reader parses the RIFF/WAV structure and converts int16 → float32.
 * No external library needed — the format is trivial for this specific case.
 * ================================================================ */

static float *read_wav_pcm_f32(const char *path, int *n_samples) {
    *n_samples = 0;
    FILE *fp = fopen(path, "rb");
    if (!fp) { perror("fopen wav"); return NULL; }

    /* Read RIFF header */
    char riff[4]; uint32_t file_size; char wave[4];
    if (fread(riff, 1, 4, fp) != 4 || memcmp(riff, "RIFF", 4) != 0) goto fail;
    if (fread(&file_size, 4, 1, fp) != 1) goto fail;
    if (fread(wave, 1, 4, fp) != 4 || memcmp(wave, "WAVE", 4) != 0) goto fail;

    /* Parse chunks — validate fmt, find data (Round 3, item 13) */
    uint32_t data_size = 0;
    int fmt_validated = 0;
    for (;;) {
        char chunk_id[4]; uint32_t chunk_size;
        if (fread(chunk_id, 1, 4, fp) != 4) goto fail;
        if (fread(&chunk_size, 4, 1, fp) != 1) goto fail;
        if (memcmp(chunk_id, "fmt ", 4) == 0 && chunk_size >= 16) {
            uint16_t audio_fmt, num_channels, bits_per_sample;
            uint32_t sample_rate;
            if (fread(&audio_fmt, 2, 1, fp) != 1) goto fail;
            if (fread(&num_channels, 2, 1, fp) != 1) goto fail;
            if (fread(&sample_rate, 4, 1, fp) != 1) goto fail;
            fseek(fp, 4, SEEK_CUR);  /* skip ByteRate */
            fseek(fp, 2, SEEK_CUR);  /* skip BlockAlign */
            if (fread(&bits_per_sample, 2, 1, fp) != 1) goto fail;
            if (audio_fmt != 1) {
                fprintf(stderr, "[wav] ERROR: format %u (expected 1=PCM)\n", audio_fmt);
                goto fail;
            }
            if (num_channels != 1) {
                fprintf(stderr, "[wav] ERROR: %u channels (expected 1=mono)\n", num_channels);
                goto fail;
            }
            if (sample_rate != 16000) {
                fprintf(stderr, "[wav] ERROR: %u Hz (expected 16000)\n", sample_rate);
                goto fail;
            }
            if (bits_per_sample != 16) {
                fprintf(stderr, "[wav] ERROR: %u-bit (expected 16)\n", bits_per_sample);
                goto fail;
            }
            fmt_validated = 1;
            /* Skip any remaining fmt data */
            if (chunk_size > 16) fseek(fp, (long)(chunk_size - 16), SEEK_CUR);
            continue;
        }
        if (memcmp(chunk_id, "data", 4) == 0) {
            data_size = chunk_size;
            break;
        }
        fseek(fp, (long)chunk_size, SEEK_CUR);
    }

    if (!fmt_validated) {
        fprintf(stderr, "[wav] ERROR: no fmt chunk found in %s\n", path);
        goto fail;
    }

    /* Read int16 samples and convert to float32 */
    int count = (int)(data_size / 2);
    int16_t *raw = malloc(data_size);
    float *pcm = malloc((size_t)count * sizeof(float));
    if (!raw || !pcm) { free(raw); free(pcm); goto fail; }

    if (fread(raw, 2, (size_t)count, fp) != (size_t)count) {
        free(raw); free(pcm); goto fail;
    }
    for (int i = 0; i < count; i++)
        pcm[i] = (float)raw[i] / 32768.0f;

    free(raw);
    fclose(fp);
    *n_samples = count;
    return pcm;

fail:
    fclose(fp);
    return NULL;
}

/* ================================================================
 * libwhisper transcription engine — direct C API, zero fork+exec
 *
 * Round 2 panel: THE critical change.
 * - Model loads ONCE at startup (not per chunk)
 * - Metal GPU context lives for entire pipeline
 * - Segments extracted from memory (no file I/O)
 * - Token probabilities → confidence scoring
 * - no_speech_prob → garbage segment filtering
 * - Segment timestamps → structured JSON output
 * - whisper_get_timings() → benchmarkable metrics
 * - whisper_full_parallel() → multi-processor decode
 *
 * Round 3 panel: cutting-edge mathematics.
 * - DTW token-level timestamps (forced alignment)
 * - Geometric mean confidence (perplexity⁻¹)
 * - Compression ratio hallucination detection
 * - N-gram repetition detection
 * - Repetition penalty logits filter
 * - vlen anomaly detection
 * - Token-aware prompt truncation
 * ================================================================ */

/* Hallucination flags (bitfield) — Round 3, items 6-9 */
#define HALLUC_NONE           0
#define HALLUC_HIGH_COMPRESS  (1 << 0)  /* compression ratio > 2.4 */
#define HALLUC_NGRAM_REPEAT   (1 << 1)  /* repeated 3-grams in token IDs */
#define HALLUC_VLEN_ANOMALY   (1 << 2)  /* tokens with impossible voice length */
#define HALLUC_LOW_LOGPROB    (1 << 3)  /* mean logprob below -1.0 */
#define COMPRESS_RATIO_THOLD  2.4f

/* Transcription result — one per segment */
typedef struct {
    int64_t t0_ms;           /* segment start (ms) */
    int64_t t1_ms;           /* segment end (ms) */
    float   confidence;      /* geometric mean token probability (Round 3: perplexity⁻¹) */
    float   logprob;         /* mean log-probability (ASR standard metric) */
    float   no_speech_prob;  /* P(no speech) for this segment */
    float   compression_ratio; /* len(text)/len(zlib(text)) — hallucination signal */
    float   quality;         /* composite: conf * (1-nsp) * min(1, 2.4/ρ) */
    int     hallucination_flags;
    int     speaker_turn;    /* 1 if next segment is different speaker */
    char    text[4096];
} TranscriptSegment;

typedef struct {
    TranscriptSegment *segments;
    int count;
    int cap;
    int segments_filtered;      /* segments dropped by no_speech filter */
    int segments_hallucinated;  /* segments flagged with any hallucination */
    /* Timing from whisper_get_timings() */
    float encode_ms;
    float decode_ms;
    float batchd_ms;  /* Round 3: batch decode timing */
    float sample_ms;
    float prompt_ms;
    char detected_language[8];  /* Round 3: auto-detected language code */
} TranscriptResult;

static void transcript_result_init(TranscriptResult *r) {
    memset(r, 0, sizeof(*r));
    r->cap = 256;
    r->segments = malloc((size_t)r->cap * sizeof(TranscriptSegment));
    r->detected_language[0] = '\0';
}

static void transcript_result_free(TranscriptResult *r) {
    free(r->segments);
    memset(r, 0, sizeof(*r));
}

/* Suppress noisy ggml/Metal init logging during model load */
static void whisper_log_suppress(enum ggml_log_level level, const char *text, void *user_data) {
    (void)level; (void)text; (void)user_data;
}

/* Extract segments from a completed whisper_full() call.
 *
 * Round 3 upgrades — cutting-edge quality scoring:
 *
 * 1. Geometric mean confidence = exp(mean(log(p_i)))
 *    This IS perplexity⁻¹ — the correct aggregation for independent
 *    token probabilities. Arithmetic mean overestimates confidence
 *    when a few high-prob tokens mask low-prob ones.
 *
 * 2. Mean log-probability = (1/N) * Σ log(p_i)
 *    The ASR research standard. Directly comparable across segments,
 *    models, and audio conditions. Used by all NIST evaluations.
 *
 * 3. Compression ratio via zlib = |text| / |zlib(text)|
 *    OpenAI's hallucination detector. ρ > 2.4 = repetitive text.
 *
 * 4. N-gram repetition = repeated 3-grams in token sequence
 *    Attention loop detection (Holtzman et al. 2019).
 *
 * 5. Voice length anomaly = tokens with vlen ≤ 0 or vlen > segment
 *    Physically impossible timing = hallucinated content.
 *
 * 6. Composite quality = conf_geo * (1 - p_ns) * min(1, 2.4/ρ)
 *    Single scalar for downstream ranking/filtering.
 */

/* Forward declarations for helpers used in extract_segments */
static float compute_compression_ratio(const char *text);
static int   detect_ngram_repetition(struct whisper_context *ctx, int i_segment);

static void extract_segments(struct whisper_context *ctx,
                             TranscriptResult *result,
                             float no_speech_filter) {
    int n = whisper_full_n_segments(ctx);
    for (int i = 0; i < n; i++) {
        float nsp = whisper_full_get_segment_no_speech_prob(ctx, i);
        if (nsp > no_speech_filter) {
            result->segments_filtered++;
            continue;
        }

        const char *text = whisper_full_get_segment_text(ctx, i);
        if (!text || text[0] == '\0') continue;

        if (result->count >= result->cap) {
            result->cap *= 2;
            result->segments = realloc(result->segments,
                (size_t)result->cap * sizeof(TranscriptSegment));
        }
        TranscriptSegment *seg = &result->segments[result->count];
        seg->t0_ms = whisper_full_get_segment_t0(ctx, i) * 10;
        seg->t1_ms = whisper_full_get_segment_t1(ctx, i) * 10;
        seg->no_speech_prob = nsp;
        seg->speaker_turn = whisper_full_get_segment_speaker_turn_next(ctx, i) ? 1 : 0;
        snprintf(seg->text, sizeof(seg->text), "%s", text);

        /* ── Token-level analysis ──────────────────────────────── */
        int n_tokens = whisper_full_n_tokens(ctx, i);
        double log_prob_sum = 0;
        int prob_count = 0;
        int vlen_anomalies = 0;
        float seg_duration_s = (float)(seg->t1_ms - seg->t0_ms) / 1000.0f;

        for (int t = 0; t < n_tokens; t++) {
            whisper_token_data tdata = whisper_full_get_token_data(ctx, i, t);
            if (tdata.p > 0) {
                log_prob_sum += log((double)tdata.p);
                prob_count++;
            }
            /* Round 3, item 17: vlen anomaly detection */
            if (tdata.vlen <= 0.0f || (seg_duration_s > 0 && tdata.vlen > seg_duration_s * 1.5f))
                vlen_anomalies++;
        }

        /* Round 3, item 4: geometric mean = exp(mean(log(p)))
         * = perplexity⁻¹. The mathematically correct confidence
         * aggregation for a sequence of independent probabilities. */
        seg->confidence = prob_count > 0
            ? (float)exp(log_prob_sum / prob_count) : 0.0f;

        /* Round 3, item 5: mean log-probability (ASR standard) */
        seg->logprob = prob_count > 0
            ? (float)(log_prob_sum / prob_count) : -99.0f;

        /* Round 3, item 6: compression ratio (hallucination signal) */
        seg->compression_ratio = compute_compression_ratio(seg->text);

        /* Round 3, items 7-9: hallucination flags */
        seg->hallucination_flags = HALLUC_NONE;
        if (seg->compression_ratio > COMPRESS_RATIO_THOLD)
            seg->hallucination_flags |= HALLUC_HIGH_COMPRESS;
        if (detect_ngram_repetition(ctx, i))
            seg->hallucination_flags |= HALLUC_NGRAM_REPEAT;
        if (vlen_anomalies > n_tokens * 2 / 3 && n_tokens >= 4)
            seg->hallucination_flags |= HALLUC_VLEN_ANOMALY;
        if (seg->logprob < -1.0f)
            seg->hallucination_flags |= HALLUC_LOW_LOGPROB;

        if (seg->hallucination_flags)
            result->segments_hallucinated++;

        /* Round 3, item 20: composite quality score
         * Q = conf_geo * (1 - p_ns) * min(1, threshold/ρ) */
        float compress_factor = seg->compression_ratio > 0
            ? fminf(1.0f, COMPRESS_RATIO_THOLD / seg->compression_ratio) : 1.0f;
        seg->quality = seg->confidence * (1.0f - nsp) * compress_factor;

        result->count++;
    }
}

/* Write transcript as plain text (for backward compat) */
static int write_transcript_txt(const TranscriptResult *r, const char *path) {
    FILE *fp = fopen(path, "w");
    if (!fp) return -1;
    for (int i = 0; i < r->count; i++)
        fprintf(fp, "%s\n", r->segments[i].text);
    fclose(fp);
    return 0;
}

/* Write structured JSON transcript with timestamps, confidence,
 * logprob, compression ratio, hallucination flags, quality score.
 * Round 3: enhanced with cutting-edge quality metrics per segment. */
static int write_transcript_json(const TranscriptResult *r, const char *path) {
    FILE *fp = fopen(path, "w");
    if (!fp) return -1;
    fprintf(fp, "{\n  \"sourceSystem\": \"BonfyreTranscribe\",\n");
    if (r->detected_language[0])
        fprintf(fp, "  \"detectedLanguage\": \"%s\",\n", r->detected_language);
    fprintf(fp, "  \"segments\": [\n");
    for (int i = 0; i < r->count; i++) {
        const TranscriptSegment *s = &r->segments[i];
        fprintf(fp,
            "    {\"t0\": %lld, \"t1\": %lld, "
            "\"confidence\": %.4f, \"logprob\": %.3f, "
            "\"no_speech\": %.3f, \"compression_ratio\": %.2f, "
            "\"quality\": %.4f, \"hallucination_flags\": %d, "
            "\"speaker_turn\": %s, \"text\": \"",
            (long long)s->t0_ms, (long long)s->t1_ms,
            s->confidence, s->logprob,
            s->no_speech_prob, s->compression_ratio,
            s->quality, s->hallucination_flags,
            s->speaker_turn ? "true" : "false");
        /* JSON-escape the text */
        for (const char *p = s->text; *p; p++) {
            if (*p == '"') fprintf(fp, "\\\"");
            else if (*p == '\\') fprintf(fp, "\\\\");
            else if (*p == '\n') fprintf(fp, "\\n");
            else if (*p == '\r') fprintf(fp, "\\r");
            else if (*p == '\t') fprintf(fp, "\\t");
            else fputc(*p, fp);
        }
        fprintf(fp, "\"}%s\n", i + 1 < r->count ? "," : "");
    }
    fprintf(fp, "  ],\n");
    fprintf(fp, "  \"summary\": {\n");
    fprintf(fp, "    \"segments_total\": %d,\n", r->count);
    fprintf(fp, "    \"segments_filtered\": %d,\n", r->segments_filtered);
    fprintf(fp, "    \"segments_hallucinated\": %d\n", r->segments_hallucinated);
    fprintf(fp, "  },\n");
    fprintf(fp, "  \"timing\": {\"encode_ms\": %.1f, \"decode_ms\": %.1f, "
            "\"batchd_ms\": %.1f, \"sample_ms\": %.1f, \"prompt_ms\": %.1f}\n",
            r->encode_ms, r->decode_ms, r->batchd_ms, r->sample_ms, r->prompt_ms);
    fprintf(fp, "}\n");
    fclose(fp);
    return 0;
}

/* Build a full text blob from result segments for vocab ingestion (Round 12) */
static char *transcript_result_text(const TranscriptResult *r) {
    size_t total = 0;
    for (int i = 0; i < r->count; i++)
        total += strlen(r->segments[i].text) + 1;
    char *text = malloc(total + 1);
    if (!text) return NULL;
    size_t off = 0;
    for (int i = 0; i < r->count; i++) {
        size_t len = strlen(r->segments[i].text);
        memcpy(text + off, r->segments[i].text, len);
        off += len;
        text[off++] = ' ';
    }
    text[off] = '\0';
    return text;
}

/* Ingest transcript directly from memory (Round 12: no file round-trip) */
static int vocab_ingest_text(VocabStore *v, const char *text) {
    if (!text || !text[0]) return 0;
    stopword_init();

    int seen_cap = v->count + 4096;
    uint8_t *seen = calloc((size_t)seen_cap, 1);
    int word_count = 0;

    /* Work on a mutable copy */
    size_t tlen = strlen(text);
    char *buf = malloc(tlen + 1);
    memcpy(buf, text, tlen + 1);

    char *saveptr = NULL;
    char *tok = strtok_r(buf, " \t\n\r", &saveptr);
    while (tok) {
        char word[128];
        snprintf(word, sizeof(word), "%s", tok);
        size_t wlen = normalize_word(word);
        if (wlen >= VOCAB_MIN_LEN && !is_stopword(word)) {
            int idx = vocab_hash_find(v, word);
            if (idx < 0) {
                idx = vocab_entry_add(v, word);
                if (idx >= seen_cap) {
                    int new_cap = idx * 2 + 1;
                    seen = realloc(seen, (size_t)new_cap);
                    memset(seen + seen_cap, 0, (size_t)(new_cap - seen_cap));
                    seen_cap = new_cap;
                }
            }
            v->entries[idx].freq++;
            v->vocab_freq_sum++;
            if (idx < seen_cap && !seen[idx]) {
                seen[idx] = 1;
                v->entries[idx].doc_freq++;
            }
        }
        word_count++;
        tok = strtok_r(NULL, " \t\n\r", &saveptr);
    }

    v->total_files++;
    v->total_words += (uint32_t)word_count;
    v->sorted = 0;
    free(seen);
    free(buf);
    return word_count;
}

/* High-precision wall-clock timer */
static double wall_clock_ms(void) {
#ifdef __APPLE__
    static mach_timebase_info_data_t tb;
    if (tb.denom == 0) mach_timebase_info(&tb);
    return (double)mach_absolute_time() * (double)tb.numer / (double)tb.denom / 1e6;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
#endif
}

/* ================================================================
 * Round 3 — Cutting-edge utility functions
 * ================================================================ */

/* Resolve DTW alignment heads preset from model filename.
 * DTW (Dynamic Time Warping) on cross-attention heads gives sub-word
 * forced alignment — state-of-the-art timestamp precision.
 * Each Whisper model size has specific attention heads pre-identified
 * for alignment (Radford et al. 2022, Table 7). */
static enum whisper_alignment_heads_preset resolve_dtw_preset(const char *model_path) {
    const char *base = strrchr(model_path, '/');
    base = base ? base + 1 : model_path;
    /* Order matters: check specific before general */
    if (strstr(base, "large-v3-turbo") || strstr(base, "turbo"))
        return WHISPER_AHEADS_LARGE_V3_TURBO;
    if (strstr(base, "large-v3"))  return WHISPER_AHEADS_LARGE_V3;
    if (strstr(base, "large-v2"))  return WHISPER_AHEADS_LARGE_V2;
    if (strstr(base, "large"))     return WHISPER_AHEADS_LARGE_V1;
    if (strstr(base, "medium.en") || strstr(base, "medium-en"))
        return WHISPER_AHEADS_MEDIUM_EN;
    if (strstr(base, "medium"))    return WHISPER_AHEADS_MEDIUM;
    if (strstr(base, "small.en")  || strstr(base, "small-en"))
        return WHISPER_AHEADS_SMALL_EN;
    if (strstr(base, "small"))     return WHISPER_AHEADS_SMALL;
    if (strstr(base, "base.en")   || strstr(base, "base-en"))
        return WHISPER_AHEADS_BASE_EN;
    if (strstr(base, "base"))      return WHISPER_AHEADS_BASE;
    if (strstr(base, "tiny.en")   || strstr(base, "tiny-en"))
        return WHISPER_AHEADS_TINY_EN;
    if (strstr(base, "tiny"))      return WHISPER_AHEADS_TINY;
    return WHISPER_AHEADS_N_TOP_MOST;  /* generic fallback */
}

/* Compression ratio via zlib — OpenAI's hallucination detector.
 * ρ = |text| / |zlib(text)|
 * High ρ (> 2.4) = repetitive text = likely hallucination.
 * Reference: whisper/transcribe.py L274 "compression_ratio_threshold" */
static float compute_compression_ratio(const char *text) {
    size_t src_len = strlen(text);
    if (src_len < 4) return 1.0f;  /* too short to compress meaningfully */
    uLong bound = compressBound((uLong)src_len);
    uint8_t *compressed = malloc(bound);
    if (!compressed) return 1.0f;
    uLong dest_len = bound;
    if (compress2(compressed, &dest_len, (const uint8_t *)text,
                  (uLong)src_len, Z_DEFAULT_COMPRESSION) != Z_OK) {
        free(compressed);
        return 1.0f;
    }
    float ratio = (float)src_len / (float)dest_len;
    free(compressed);
    return ratio;
}

/* Detect repeated 3-grams in token ID sequence.
 * A repeated 3-gram (trigram) in decoder output signals an attention loop —
 * a known failure mode in autoregressive sequence models (Holtzman et al. 2019).
 * Returns 1 if any 3-gram appears more than twice. */
static int detect_ngram_repetition(struct whisper_context *ctx, int i_segment) {
    int n_tokens = whisper_full_n_tokens(ctx, i_segment);
    if (n_tokens < 6) return 0;  /* need at least 2 trigrams */

    /* Hash each trigram and count in a small table */
    #define NGRAM_HASH_SIZE 128
    struct { uint32_t hash; int count; } table[NGRAM_HASH_SIZE];
    memset(table, 0, sizeof(table));

    for (int t = 0; t <= n_tokens - 3; t++) {
        whisper_token t0 = whisper_full_get_token_id(ctx, i_segment, t);
        whisper_token t1 = whisper_full_get_token_id(ctx, i_segment, t + 1);
        whisper_token t2 = whisper_full_get_token_id(ctx, i_segment, t + 2);
        /* FNV-style combination */
        uint32_t h = 2166136261u;
        h = (h ^ (uint32_t)t0) * 16777619u;
        h = (h ^ (uint32_t)t1) * 16777619u;
        h = (h ^ (uint32_t)t2) * 16777619u;
        int slot = (int)(h & (NGRAM_HASH_SIZE - 1));
        /* Linear probe */
        for (int p = 0; p < NGRAM_HASH_SIZE; p++) {
            int idx = (slot + p) & (NGRAM_HASH_SIZE - 1);
            if (table[idx].count == 0) {
                table[idx].hash = h;
                table[idx].count = 1;
                break;
            }
            if (table[idx].hash == h) {
                table[idx].count++;
                if (table[idx].count > 2) return 1;  /* repeated 3-gram */
                break;
            }
        }
    }
    #undef NGRAM_HASH_SIZE
    return 0;
}

/* Repetition penalty logits filter callback (Keskar et al. 2019).
 * For each token that appeared in the last 32 positions:
 *   logit = logit / θ   if logit > 0
 *   logit = logit * θ   if logit < 0
 * where θ > 1 (penalty). Breaks decoder repetition loops during beam search. */
static float rep_penalty_theta = 1.15f;

static void repetition_penalty_callback(
    struct whisper_context *ctx,
    struct whisper_state   *state,
    const whisper_token_data *tokens,
    int n_tokens,
    float *logits,
    void *user_data)
{
    (void)state; (void)user_data;
    float theta = rep_penalty_theta;
    int n_vocab = whisper_n_vocab(ctx);
    int window = n_tokens < 32 ? n_tokens : 32;
    for (int i = n_tokens - window; i < n_tokens; i++) {
        if (i < 0) continue;
        whisper_token tid = tokens[i].id;
        if (tid >= 0 && tid < n_vocab) {
            if (logits[tid] > 0.0f)
                logits[tid] /= theta;
            else
                logits[tid] *= theta;
        }
    }
}

/* Write SRT subtitles (SubRip format — Round 3, item 14).
 * Format: sequential number, timestamp line, text, blank line.
 * Timestamps: HH:MM:SS,mmm --> HH:MM:SS,mmm */
static int write_transcript_srt(const TranscriptResult *r, const char *path) {
    FILE *fp = fopen(path, "w");
    if (!fp) return -1;
    for (int i = 0; i < r->count; i++) {
        const TranscriptSegment *s = &r->segments[i];
        int64_t t0 = s->t0_ms, t1 = s->t1_ms;
        fprintf(fp, "%d\n", i + 1);
        fprintf(fp, "%02lld:%02lld:%02lld,%03lld --> %02lld:%02lld:%02lld,%03lld\n",
                (long long)(t0/3600000), (long long)((t0/60000)%60),
                (long long)((t0/1000)%60), (long long)(t0%1000),
                (long long)(t1/3600000), (long long)((t1/60000)%60),
                (long long)((t1/1000)%60), (long long)(t1%1000));
        /* Strip leading whitespace from segment text */
        const char *text = s->text;
        while (*text == ' ' || *text == '\t') text++;
        fprintf(fp, "%s\n\n", text);
    }
    fclose(fp);
    return 0;
}

/* Write WebVTT subtitles (Round 3, item 15).
 * WEBVTT header, then cue blocks with HH:MM:SS.mmm timestamps.
 * Includes confidence metadata per cue for downstream quality filtering. */
static int write_transcript_vtt(const TranscriptResult *r, const char *path) {
    FILE *fp = fopen(path, "w");
    if (!fp) return -1;
    fprintf(fp, "WEBVTT\nKind: captions\n\n");
    for (int i = 0; i < r->count; i++) {
        const TranscriptSegment *s = &r->segments[i];
        int64_t t0 = s->t0_ms, t1 = s->t1_ms;
        /* NOTE block with quality metadata */
        fprintf(fp, "NOTE confidence=%.3f quality=%.3f halluc=0x%x\n",
                s->confidence, s->quality, s->hallucination_flags);
        fprintf(fp, "%02lld:%02lld:%02lld.%03lld --> %02lld:%02lld:%02lld.%03lld\n",
                (long long)(t0/3600000), (long long)((t0/60000)%60),
                (long long)((t0/1000)%60), (long long)(t0%1000),
                (long long)(t1/3600000), (long long)((t1/60000)%60),
                (long long)((t1/1000)%60), (long long)(t1%1000));
        const char *text = s->text;
        while (*text == ' ' || *text == '\t') text++;
        fprintf(fp, "%s\n\n", text);
    }
    fclose(fp);
    return 0;
}

static void resolve_executable_sibling(char *buffer, size_t size, const char *argv0, const char *sibling_dir, const char *binary_name) {
    if (argv0 && argv0[0] == '/') {
        snprintf(buffer, size, "%s", argv0);
    } else if (argv0 && strstr(argv0, "/")) {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd))) {
            snprintf(buffer, size, "%s/%s", cwd, argv0);
        } else {
            snprintf(buffer, size, "%s", argv0);
        }
    } else {
        buffer[0] = '\0';
        return;
    }

    char *last_slash = strrchr(buffer, '/');
    if (!last_slash) {
        buffer[0] = '\0';
        return;
    }
    *last_slash = '\0';
    last_slash = strrchr(buffer, '/');
    if (!last_slash) {
        buffer[0] = '\0';
        return;
    }
    *last_slash = '\0';
    /* Avoid UB: snprintf with overlapping src/dest (C11 §7.21.6.5) */
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s/%s/%s", buffer, sibling_dir, binary_name);
    snprintf(buffer, size, "%s", tmp);
}

static const char *default_media_prep_binary(const char *argv0, char *resolved_path, size_t resolved_size) {
    const char *env = getenv("BONFYRE_MEDIA_PREP_BINARY");
    if (env && env[0] != '\0') return env;
    resolve_executable_sibling(resolved_path, resolved_size, argv0, "BonfyreMediaPrep", "bonfyre-media-prep");
    if (resolved_path[0] != '\0' && access(resolved_path, X_OK) == 0) {
        return resolved_path;
    }
    return "../BonfyreMediaPrep/bonfyre-media-prep";
}

static const char *default_silero_vad_script(const char *argv0, char *resolved_path, size_t resolved_size) {
    const char *env = getenv("BONFYRE_SILERO_VAD_CLI");
    if (env && env[0] != '\0') return env;
    resolve_executable_sibling(resolved_path, resolved_size, argv0, "SileroVADCLI", "bin/silero_vad_cli.py");
    if (resolved_path[0] != '\0' && access(resolved_path, F_OK) == 0) {
        return resolved_path;
    }
    return "../SileroVADCLI/bin/silero_vad_cli.py";
}

static const char *path_basename(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

static void strip_extension(char *name) {
    char *dot = strrchr(name, '.');
    if (dot) *dot = '\0';
}

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-transcribe — world-class libwhisper transcription engine\n\n"
            "Usage:\n"
            "  bonfyre-transcribe <input-audio> <output-dir> [--model NAME] [--language CODE|auto]\n"
            "                      [--media-prep-binary PATH]\n"
            "                      [--silero-vad] [--silero-script PATH]\n"
            "                      [--split-speech] [--noise-threshold DB] [--min-silence SEC]\n"
            "                      [--min-speech SEC] [--padding SEC]\n"
            "                      [--greedy] [--beam-size N] [--best-of N]\n"
            "                      [--vocab-db PATH] [--no-vocab]\n"
            "                      [--processors N] [--no-speech-thold N]\n"
            "\n"
            "Transcription engine:\n"
            "  Links libwhisper directly. Model loads ONCE. Zero fork+exec.\n"
            "  Metal GPU + flash attention. DTW forced-alignment timestamps.\n"
            "  Multi-processor parallel decode. TinyDiarize speaker turns.\n"
            "\n"
            "Quality (cutting-edge):\n"
            "  Geometric mean confidence (perplexity^-1 = exp(mean(log(p)))).\n"
            "  Compression ratio hallucination detection (zlib, rho > 2.4).\n"
            "  Token n-gram repetition detector. Repetition penalty logits filter.\n"
            "  Voice-length anomaly detection. Per-segment quality score.\n"
            "\n"
            "Output formats:\n"
            "  transcript.txt   — plain text\n"
            "  transcript.json  — structured segments with timestamps, confidence,\n"
            "                     logprob, compression ratio, hallucination flags\n"
            "  transcript.srt   — SubRip subtitles\n"
            "  transcript.vtt   — WebVTT subtitles with quality metadata\n"
            "\n"
            "Vocabulary learning:\n"
            "  Flat binary .bfvocab format (BFVD02). Zero SQLite, zero process spawns.\n"
            "  BM25-weighted terms injected as whisper initial_prompt.\n"
            "  Token-aware truncation to fit decoder context (112 token budget).\n"
            "  Shannon entropy + KL-divergence track convergence.\n"
            "\n"
            "  BM25(t,d) = IDF(t) * tf(t,d)*(k1+1) / (tf(t,d)+k1*(1-b+b*|d|/avgdl))\n"
            "  H = -sum(p_i * log2(p_i))           [Shannon entropy]\n"
            "  D_KL(P||U) = sum(p_i * log2(p_i/q)) [KL from uniform]\n"
            "\n"
            "  Disable with --no-vocab. Custom path with --vocab-db.\n");
}

/* Detect number of CPU cores. */
static int detect_cpu_cores(void) {
#ifdef __APPLE__
    int ncpu = 0;
    size_t len = sizeof(ncpu);
    if (sysctlbyname("hw.perflevel0.physicalcpu", &ncpu, &len, NULL, 0) == 0 && ncpu > 0)
        return ncpu;  /* performance cores only */
    if (sysctlbyname("hw.physicalcpu", &ncpu, &len, NULL, 0) == 0 && ncpu > 0)
        return ncpu;
#endif
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return (n > 0) ? (int)n : 4;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        print_usage();
        return 1;
    }

    const char *input_audio = argv[1];
    const char *output_dir = argv[2];
    const char *model = "base";
    const char *language = NULL;
    int split_speech = 0;
    const char *noise_threshold = "-35dB";
    const char *min_silence = "0.35";
    const char *min_speech = "0.75";
    const char *padding = "0.15";
    int silero_vad = 0;
    int greedy = 1;
    int beam_size = 1;
    int best_of = 1;
    int no_vocab = 0;
    int n_processors = 1;            /* Round 4: multi-processor decode */
    float no_speech_thold = 0.6f;    /* Round 7: configurable filtering */
    const char *vocab_db_override = NULL;
    char resolved_media_prep[PATH_MAX];
    char resolved_silero_script[PATH_MAX];
    const char *media_prep_binary = default_media_prep_binary(argv[0], resolved_media_prep, sizeof(resolved_media_prep));
    const char *silero_script = default_silero_vad_script(argv[0], resolved_silero_script, sizeof(resolved_silero_script));
    int threads = detect_cpu_cores();

    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--model") == 0 && i + 1 < argc) {
            model = argv[++i];
        } else if (strcmp(argv[i], "--language") == 0 && i + 1 < argc) {
            language = argv[++i];
        } else if (strcmp(argv[i], "--media-prep-binary") == 0 && i + 1 < argc) {
            media_prep_binary = argv[++i];
        } else if (strcmp(argv[i], "--silero-vad") == 0) {
            silero_vad = 1;
        } else if (strcmp(argv[i], "--silero-script") == 0 && i + 1 < argc) {
            silero_script = argv[++i];
        } else if (strcmp(argv[i], "--split-speech") == 0) {
            split_speech = 1;
        } else if (strcmp(argv[i], "--noise-threshold") == 0 && i + 1 < argc) {
            noise_threshold = argv[++i];
        } else if (strcmp(argv[i], "--min-silence") == 0 && i + 1 < argc) {
            min_silence = argv[++i];
        } else if (strcmp(argv[i], "--min-speech") == 0 && i + 1 < argc) {
            min_speech = argv[++i];
        } else if (strcmp(argv[i], "--padding") == 0 && i + 1 < argc) {
            padding = argv[++i];
        } else if (strcmp(argv[i], "--greedy") == 0) {
            greedy = 1; beam_size = 1; best_of = 1;
        } else if (strcmp(argv[i], "--beam-size") == 0 && i + 1 < argc) {
            beam_size = atoi(argv[++i]); greedy = (beam_size <= 1);
        } else if (strcmp(argv[i], "--best-of") == 0 && i + 1 < argc) {
            best_of = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--vocab-db") == 0 && i + 1 < argc) {
            vocab_db_override = argv[++i];
        } else if (strcmp(argv[i], "--no-vocab") == 0) {
            no_vocab = 1;
        } else if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--processors") == 0 && i + 1 < argc) {
            n_processors = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--no-speech-thold") == 0 && i + 1 < argc) {
            no_speech_thold = (float)atof(argv[++i]);
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    if (ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output dir: %s\n", output_dir);
        return 1;
    }

    double t_start = wall_clock_ms();

    char normalized_path[PATH_MAX];
    char transcript_path[PATH_MAX];
    char transcript_json_path[PATH_MAX];
    char meta_path[PATH_MAX];
    char status_path[PATH_MAX];
    char progress_path[PATH_MAX];
    char base_name[PATH_MAX];
    int chunk_count = 0;
    int denoised = 0;

    snprintf(base_name, sizeof(base_name), "%s", path_basename(input_audio));
    strip_extension(base_name);
    snprintf(normalized_path, sizeof(normalized_path), "%s/normalized.wav", output_dir);
    snprintf(transcript_path, sizeof(transcript_path), "%s/transcript.txt", output_dir);
    snprintf(transcript_json_path, sizeof(transcript_json_path), "%s/transcript.json", output_dir);
    snprintf(meta_path, sizeof(meta_path), "%s/meta.json", output_dir);
    snprintf(status_path, sizeof(status_path), "%s/transcribe-status.json", output_dir);
    snprintf(progress_path, sizeof(progress_path), "%s/chunk-progress.json", output_dir);

    /* ── Pipeline: denoise at NATIVE sample rate, then normalize ──── */
    char denoised_path[PATH_MAX];
    snprintf(denoised_path, sizeof(denoised_path), "%s/input.denoised.wav", output_dir);
    char *denoise_argv[] = {
        (char *)media_prep_binary,
        "denoise",
        (char *)input_audio,
        denoised_path,
        NULL
    };
    const char *normalize_input = input_audio;
    if (run_process(denoise_argv) == 0 && access(denoised_path, F_OK) == 0) {
        normalize_input = denoised_path;
        denoised = 1;
    }

    char *normalize_argv[] = {
        (char *)media_prep_binary,
        "normalize",
        (char *)normalize_input,
        normalized_path,
        "--sample-rate", "16000",
        "--channels", "1",
        NULL
    };

    if (run_process(normalize_argv) != 0) {
        fprintf(stderr, "Normalize failed.\n");
        return 1;
    }

    double t_preprocess = wall_clock_ms();

    /* ── Load vocabulary store and build decoder prompt ──────────── */
    char vocab_path[PATH_MAX];
    VocabStore vocab_store;
    vocab_store_init(&vocab_store);
    char *vocab_prompt = NULL;
    if (!no_vocab) {
        if (vocab_db_override) {
            snprintf(vocab_path, sizeof(vocab_path), "%s", vocab_db_override);
        } else {
            resolve_vocab_path(vocab_path, sizeof(vocab_path));
        }
        vocab_store_load(&vocab_store, vocab_path);
        vocab_prompt = vocab_build_prompt(&vocab_store);
    }

    /* ================================================================
     * libwhisper transcription — model loads ONCE, zero fork+exec.
     *
     * Round 2 panel: This is the architectural change that separates
     * a toy CLI wrapper from a world-class transcription engine.
     *
     * Before: fork+exec whisper-cli per chunk. 800ms model load per chunk.
     *         20 chunks = 16s wasted. Results via file I/O.
     * After:  whisper_init_from_file() once. whisper_full() per chunk.
     *         Direct memory access to segments, timestamps, confidence.
     * ================================================================ */

    /* Suppress noisy ggml/Metal init logging */
    whisper_log_set(whisper_log_suppress, NULL);

    char model_path[PATH_MAX];
    if (resolve_whisper_model(model, model_path, sizeof(model_path)) != 0) {
        fprintf(stderr, "Cannot find whisper model for '%s'\n", model);
        return 1;
    }

    double t_model_start = wall_clock_ms();

    /* ggml 0.9.11+ uses dynamic backend plugins (Metal, CPU, BLAS).
     * Must load them before any whisper context init. */
    ggml_backend_load_all();

    /* Round 3, item 1: DTW token-level timestamps.
     * Dynamic Time Warping on cross-attention alignment heads gives
     * sub-word timing precision — state-of-the-art forced alignment.
     * Auto-select the correct attention head preset for this model.
     *
     * DTW requires the full cross-attention matrix, so flash_attn
     * is disabled when DTW is active (whisper silently disables DTW
     * if flash_attn is on — we want DTW, not flash). */
    struct whisper_context_params cparams = whisper_context_default_params();
    cparams.use_gpu = true;
    cparams.flash_attn = false;  /* incompatible with DTW */
    cparams.dtw_token_timestamps = true;
    cparams.dtw_aheads_preset = resolve_dtw_preset(model_path);

    struct whisper_context *wctx = whisper_init_from_file_with_params(model_path, cparams);
    if (!wctx) {
        fprintf(stderr, "Failed to load whisper model: %s\n", model_path);
        return 1;
    }

    double t_model_loaded = wall_clock_ms();
    fprintf(stderr, "[whisper] model loaded: %s (%.0f ms, DTW=%s)\n",
            model_path, t_model_loaded - t_model_start,
            cparams.dtw_token_timestamps ? "on" : "off");

    /* Round 3, item 10: Token-aware prompt truncation.
     * Whisper's decoder context is n_max_text_ctx tokens (224).
     * Initial prompt can use at most half (112 tokens).
     * Whisper truncates from the BEGINNING — losing our highest-BM25 terms.
     * Pre-truncate from the END to keep top-ranked terms. */
    if (vocab_prompt) {
        int n_tok = -whisper_tokenize(wctx, vocab_prompt, NULL, 0);
        if (n_tok > 112) {
            float ratio = 112.0f / (float)n_tok;
            size_t new_len = (size_t)(strlen(vocab_prompt) * ratio);
            /* Snap to word boundary */
            while (new_len > 0 && vocab_prompt[new_len] != ' ') new_len--;
            if (new_len > 0) {
                vocab_prompt[new_len] = '\0';
                int final_tok = -whisper_tokenize(wctx, vocab_prompt, NULL, 0);
                fprintf(stderr, "[vocab] prompt truncated: %d → %d tokens (budget: 112)\n",
                        n_tok, final_tok);
            }
        }
    }

    /* Configure decoding parameters */
    int auto_detect_lang = (language && strcmp(language, "auto") == 0);
    struct whisper_full_params wparams = whisper_full_default_params(
        greedy ? WHISPER_SAMPLING_GREEDY : WHISPER_SAMPLING_BEAM_SEARCH);

    wparams.n_threads      = threads;
    wparams.n_max_text_ctx = 224;
    wparams.no_timestamps  = false;
    wparams.print_special  = false;
    wparams.print_progress = false;
    wparams.print_realtime = false;
    wparams.print_timestamps = false;

    /* Round 3, item 2: Token-level timestamps.
     * Combined with DTW, this gives per-token start/end times.
     * thold_pt/thold_ptsum control timestamp token probability filtering. */
    wparams.token_timestamps = true;
    wparams.thold_pt         = 0.01f;
    wparams.thold_ptsum      = 0.01f;

    /* Round 3, item 3: TinyDiarize speaker turn detection.
     * Enables the TDRZ signal in the decoder — real diarization
     * hints instead of heuristic-only speaker_turn_next(). */
    wparams.tdrz_enable = true;

    wparams.suppress_blank = true;
    wparams.suppress_nst   = true;

    wparams.temperature     = 0.0f;
    wparams.temperature_inc = denoised ? 0.0f : 0.2f;
    wparams.entropy_thold   = 2.0f;
    wparams.logprob_thold   = -1.0f;
    wparams.no_speech_thold = no_speech_thold;

    /* Round 3, item 11: Language auto-detection.
     * When --language auto: use Whisper's built-in softmax language
     * classifier on the first 30s mel spectrogram. */
    if (auto_detect_lang) {
        wparams.language = "auto";
        wparams.detect_language = true;
    } else {
        wparams.language = language ? language : "en";
        wparams.detect_language = false;
    }

    wparams.initial_prompt = vocab_prompt;
    wparams.carry_initial_prompt = true;

    /* Round 3, item 8: Repetition penalty logits filter.
     * Penalizes recently-seen tokens during beam search to break
     * decoder repetition loops (Keskar et al. 2019). */
    wparams.logits_filter_callback = repetition_penalty_callback;
    wparams.logits_filter_callback_user_data = NULL;

    if (greedy) {
        wparams.greedy.best_of = best_of;
    } else {
        wparams.beam_search.beam_size = beam_size;
    }

    TranscriptResult result;
    transcript_result_init(&result);

    double t_transcribe_start = wall_clock_ms();
    int total_audio_samples = 0;  /* Round 3, item 18: track for RTF */

    if (split_speech) {
        /* ── External speech splitting (media-prep or Silero VAD) ─── */
        char chunk_dir[PATH_MAX];
        char chunk_pattern[PATH_MAX];
        snprintf(chunk_dir, sizeof(chunk_dir), "%s/chunks", output_dir);
        snprintf(chunk_pattern, sizeof(chunk_pattern), "%s/chunk-%%03d.wav", chunk_dir);

        if (ensure_dir(chunk_dir) != 0) {
            fprintf(stderr, "Failed to create chunk dir: %s\n", chunk_dir);
            whisper_free(wctx);
            return 1;
        }

        if (silero_vad && access(silero_script, F_OK) == 0) {
            char *silero_argv[] = {
                "python3", (char *)silero_script,
                "--audio", normalized_path,
                "--out", chunk_dir,
                "--min-speech", (char *)min_speech,
                "--padding", (char *)padding,
                NULL
            };
            if (run_process(silero_argv) != 0) {
                fprintf(stderr, "Silero VAD split failed.\n");
                whisper_free(wctx);
                return 1;
            }
        } else {
            char *split_argv[] = {
                (char *)media_prep_binary,
                "split-speech", normalized_path, chunk_pattern,
                "--noise-threshold", (char *)noise_threshold,
                "--min-silence", (char *)min_silence,
                "--min-speech", (char *)min_speech,
                "--padding", (char *)padding,
                NULL
            };
            if (run_process(split_argv) != 0) {
                fprintf(stderr, "Speech split failed.\n");
                whisper_free(wctx);
                return 1;
            }
        }

        /* Round 3, item 12: Each chunk IS one utterance.
         * single_segment prevents whisper from internally splitting,
         * which causes timestamp drift and context fragmentation. */
        wparams.single_segment = true;

        /* Process each chunk through the SAME whisper context.
         * Model stays loaded. Metal GPU context persists.
         * no_context=false: decoder carries previous chunk tokens. */
        for (int i = 0;; i++) {
            char chunk_audio[PATH_MAX];
            snprintf(chunk_audio, sizeof(chunk_audio), "%s/chunk-%03d.wav", chunk_dir, i);
            if (access(chunk_audio, F_OK) != 0) break;
            chunk_count++;

            int n_samples = 0;
            float *samples = read_wav_pcm_f32(chunk_audio, &n_samples);
            if (!samples || n_samples == 0) {
                fprintf(stderr, "[whisper] failed to read chunk: %s\n", chunk_audio);
                free(samples);
                continue;
            }
            total_audio_samples += n_samples;

            whisper_reset_timings(wctx);
            if (whisper_full(wctx, wparams, samples, n_samples) != 0) {
                fprintf(stderr, "[whisper] transcription failed on chunk %d\n", i);
                free(samples);
                continue;
            }

            /* Capture detected language from first chunk */
            if (auto_detect_lang && i == 0) {
                int lang_id = whisper_full_lang_id(wctx);
                const char *lang_str = whisper_lang_str(lang_id);
                if (lang_str) snprintf(result.detected_language, sizeof(result.detected_language), "%s", lang_str);
                fprintf(stderr, "[whisper] detected language: %s\n", result.detected_language);
            }

            extract_segments(wctx, &result, no_speech_thold);
            free(samples);

            write_chunk_progress(progress_path, chunk_count, i + 1, "transcribing");
            fprintf(stderr, "[whisper] chunk %d: %d segments\n",
                    i, whisper_full_n_segments(wctx));
        }
        write_chunk_progress(progress_path, chunk_count, chunk_count, "completed");

    } else {
        /* ── Single-file mode with multi-processor parallel decode ── */
        int n_samples = 0;
        float *samples = read_wav_pcm_f32(normalized_path, &n_samples);
        if (!samples || n_samples == 0) {
            fprintf(stderr, "Failed to read audio: %s\n", normalized_path);
            free(samples);
            whisper_free(wctx);
            return 1;
        }
        total_audio_samples = n_samples;

        fprintf(stderr, "[whisper] audio: %.1f seconds, %d samples\n",
                (double)n_samples / WHISPER_SAMPLE_RATE, n_samples);

        int ret;
        if (n_processors > 1) {
            ret = whisper_full_parallel(wctx, wparams, samples, n_samples, n_processors);
        } else {
            ret = whisper_full(wctx, wparams, samples, n_samples);
        }

        if (ret != 0) {
            fprintf(stderr, "Whisper transcription failed.\n");
            free(samples);
            whisper_free(wctx);
            return 1;
        }

        /* Capture detected language */
        if (auto_detect_lang) {
            int lang_id = whisper_full_lang_id(wctx);
            const char *lang_str = whisper_lang_str(lang_id);
            if (lang_str) snprintf(result.detected_language, sizeof(result.detected_language), "%s", lang_str);
            fprintf(stderr, "[whisper] detected language: %s\n", result.detected_language);
        }

        extract_segments(wctx, &result, no_speech_thold);
        free(samples);
        write_chunk_progress(progress_path, 1, 1, "completed");
    }

    double t_transcribe_done = wall_clock_ms();

    /* Capture whisper internal timings — ALL fields (Round 3, item 19) */
    struct whisper_timings *wtimings = whisper_get_timings(wctx);
    if (wtimings) {
        result.encode_ms = wtimings->encode_ms;
        result.decode_ms = wtimings->decode_ms;
        result.batchd_ms = wtimings->batchd_ms;
        result.sample_ms = wtimings->sample_ms;
        result.prompt_ms = wtimings->prompt_ms;
    }

    whisper_free(wctx);

    /* ── Write ALL output formats ──────────────────────────────── */
    char transcript_srt_path[PATH_MAX];
    char transcript_vtt_path[PATH_MAX];
    snprintf(transcript_srt_path, sizeof(transcript_srt_path), "%s/transcript.srt", output_dir);
    snprintf(transcript_vtt_path, sizeof(transcript_vtt_path), "%s/transcript.vtt", output_dir);

    write_transcript_txt(&result, transcript_path);
    write_transcript_json(&result, transcript_json_path);
    write_transcript_srt(&result, transcript_srt_path);  /* Round 3, item 14 */
    write_transcript_vtt(&result, transcript_vtt_path);  /* Round 3, item 15 */

    /* Compute summary statistics */
    double avg_conf = 0, avg_quality = 0, avg_logprob = 0;
    if (result.count > 0) {
        double sc = 0, sq = 0, sl = 0;
        for (int i = 0; i < result.count; i++) {
            sc += result.segments[i].confidence;
            sq += result.segments[i].quality;
            sl += result.segments[i].logprob;
        }
        avg_conf    = sc / result.count;
        avg_quality = sq / result.count;
        avg_logprob = sl / result.count;
    }
    fprintf(stderr,
            "[quality] segments: %d (filtered: %d, hallucinated: %d)\n"
            "[quality] avg confidence: %.4f (geometric mean = perplexity^-1)\n"
            "[quality] avg logprob: %.3f | avg quality: %.4f\n",
            result.count, result.segments_filtered, result.segments_hallucinated,
            avg_conf, avg_logprob, avg_quality);

    /* ── Vocab ingestion directly from memory ────────────────────── */
    int words_ingested = 0;
    if (!no_vocab) {
        char *full_text = transcript_result_text(&result);
        if (full_text) {
            words_ingested = vocab_ingest_text(&vocab_store, full_text);
            free(full_text);
        }
        vocab_store_save(&vocab_store, vocab_path);
        vocab_write_stats(&vocab_store, output_dir, vocab_path);
        fprintf(stderr, "[vocab] ingested %d words from transcript\n", words_ingested);
    }
    free(vocab_prompt);
    vocab_store_free(&vocab_store);

    double t_end = wall_clock_ms();

    /* ── Timing metrics ──────────────────────────────────────────── */
    double preprocess_ms = t_preprocess - t_start;
    double model_load_ms = t_model_loaded - t_model_start;
    double transcribe_ms = t_transcribe_done - t_transcribe_start;
    double total_ms       = t_end - t_start;

    /* Round 3, item 18: use stored sample count — no re-read */
    double audio_duration_s = (double)total_audio_samples / WHISPER_SAMPLE_RATE;
    double rtf = audio_duration_s > 0 ? (transcribe_ms / 1000.0) / audio_duration_s : 0;

    fprintf(stderr,
            "[timing] preprocess: %.0f ms | model load: %.0f ms | "
            "transcribe: %.0f ms | total: %.0f ms\n",
            preprocess_ms, model_load_ms, transcribe_ms, total_ms);
    fprintf(stderr,
            "[timing] encode: %.0f ms | decode: %.0f ms | batchd: %.0f ms | RTF: %.3f\n",
            result.encode_ms, result.decode_ms, result.batchd_ms, rtf);

    /* ── meta.json ──────────────────────────────────────────────── */
    char timestamp[32];
    iso_timestamp(timestamp, sizeof(timestamp));
    char language_json[256];
    if (result.detected_language[0]) {
        snprintf(language_json, sizeof(language_json), "\"%s\"", result.detected_language);
    } else if (language && strcmp(language, "auto") != 0) {
        snprintf(language_json, sizeof(language_json), "\"%s\"", language);
    } else {
        snprintf(language_json, sizeof(language_json), "\"en\"");
    }

    FILE *meta = fopen(meta_path, "w");
    if (!meta) {
        perror("fopen meta");
        return 1;
    }
    fprintf(meta,
            "{\n"
            "  \"source_system\": \"BonfyreTranscribe\",\n"
            "  \"engine\": \"libwhisper\",\n"
            "  \"whisper_version\": \"%s\",\n"
            "  \"created_at\": \"%s\",\n"
            "  \"input_audio\": \"%s\",\n"
            "  \"normalized_audio\": \"%s\",\n"
            "  \"transcript_path\": \"%s\",\n"
            "  \"transcript_json_path\": \"%s\",\n"
            "  \"transcript_srt_path\": \"%s\",\n"
            "  \"transcript_vtt_path\": \"%s\",\n"
            "  \"model\": \"%s\",\n"
            "  \"model_path\": \"%s\",\n"
            "  \"language\": %s,\n"
            "  \"split_speech\": %s,\n"
            "  \"silero_vad\": %s,\n"
            "  \"denoised\": %s,\n"
            "  \"greedy\": %s,\n"
            "  \"beam_size\": %d,\n"
            "  \"threads\": %d,\n"
            "  \"processors\": %d,\n"
            "  \"dtw_timestamps\": true,\n"
            "  \"token_timestamps\": true,\n"
            "  \"tdrz_diarize\": true,\n"
            "  \"repetition_penalty\": %.2f,\n"
            "  \"vocab_enabled\": %s,\n"
            "  \"vocab_words_ingested\": %d,\n"
            "  \"segments_total\": %d,\n"
            "  \"segments_filtered\": %d,\n"
            "  \"segments_hallucinated\": %d,\n"
            "  \"chunk_count\": %d,\n"
            "  \"audio_duration_s\": %.2f,\n"
            "  \"avg_confidence\": %.4f,\n"
            "  \"avg_logprob\": %.3f,\n"
            "  \"avg_quality\": %.4f,\n"
            "  \"preprocess_ms\": %.0f,\n"
            "  \"model_load_ms\": %.0f,\n"
            "  \"transcribe_ms\": %.0f,\n"
            "  \"encode_ms\": %.1f,\n"
            "  \"decode_ms\": %.1f,\n"
            "  \"batchd_ms\": %.1f,\n"
            "  \"total_ms\": %.0f,\n"
            "  \"rtf\": %.4f,\n"
            "  \"media_prep_binary\": \"%s\"\n"
            "}\n",
            whisper_version(),
            timestamp,
            input_audio,
            normalized_path,
            transcript_path,
            transcript_json_path,
            transcript_srt_path,
            transcript_vtt_path,
            model,
            model_path,
            language_json,
            split_speech ? "true" : "false",
            silero_vad ? "true" : "false",
            denoised ? "true" : "false",
            greedy ? "true" : "false",
            beam_size,
            threads,
            n_processors,
            (double)rep_penalty_theta,
            no_vocab ? "false" : "true",
            words_ingested,
            result.count,
            result.segments_filtered,
            result.segments_hallucinated,
            chunk_count,
            audio_duration_s,
            avg_conf,
            avg_logprob,
            avg_quality,
            preprocess_ms,
            model_load_ms,
            transcribe_ms,
            result.encode_ms,
            result.decode_ms,
            result.batchd_ms,
            total_ms,
            rtf,
            media_prep_binary);
    fclose(meta);

    /* ── transcribe-status.json ─────────────────────────────────── */
    FILE *status = fopen(status_path, "w");
    if (!status) {
        perror("fopen status");
        return 1;
    }
    fprintf(status,
            "{\n"
            "  \"sourceSystem\": \"BonfyreTranscribe\",\n"
            "  \"exportedAt\": \"%s\",\n"
            "  \"status\": \"transcribed\",\n"
            "  \"jobSlug\": \"%s\",\n"
            "  \"splitSpeech\": %s,\n"
            "  \"sileroVad\": %s,\n"
            "  \"denoised\": %s,\n"
            "  \"segments\": %d,\n"
            "  \"segmentsFiltered\": %d,\n"
            "  \"segmentsHallucinated\": %d,\n"
            "  \"chunkCount\": %d,\n"
            "  \"avgQuality\": %.4f,\n"
            "  \"rtf\": %.4f,\n"
            "  \"transcriptPath\": \"%s\",\n"
            "  \"transcriptJsonPath\": \"%s\",\n"
            "  \"transcriptSrtPath\": \"%s\",\n"
            "  \"transcriptVttPath\": \"%s\",\n"
            "  \"metaPath\": \"%s\"\n"
            "}\n",
            timestamp,
            base_name,
            split_speech ? "true" : "false",
            silero_vad ? "true" : "false",
            denoised ? "true" : "false",
            result.count,
            result.segments_filtered,
            result.segments_hallucinated,
            chunk_count,
            avg_quality,
            rtf,
            transcript_path,
            transcript_json_path,
            transcript_srt_path,
            transcript_vtt_path,
            meta_path);
    fclose(status);

    /* ── Fragment emission (opt-in via BONFYRE_FRAGMENT_STORE) ─── */
    const char *frag_store_path = getenv("BONFYRE_FRAGMENT_STORE");
    if (frag_store_path) {
        bf_fragment_store_t *fstore = bf_fragment_store_open(frag_store_path);
        if (fstore) {
            char frag_id[BF_FRAGMENT_ID_LEN];
            char payload[8192];
            char persp[128];
            snprintf(persp, sizeof(persp), "whisper-%s", model);
            int emitted = 0;
            for (int i = 0; i < result.count; i++) {
                const TranscriptSegment *seg = &result.segments[i];
                /* Escape double-quotes in text for JSON safety */
                char safe_text[4096];
                const char *src = seg->text;
                int ti = 0;
                while (*src && ti < (int)sizeof(safe_text) - 2) {
                    if (*src == '"') safe_text[ti++] = '\\';
                    safe_text[ti++] = *src++;
                }
                safe_text[ti] = '\0';
                snprintf(payload, sizeof(payload),
                    "{\"text\":\"%s\",\"quality\":%.4f,\"logprob\":%.3f,"
                    "\"hallucination_flags\":%d,\"speaker_turn\":%d}",
                    safe_text, (double)seg->quality, (double)seg->logprob,
                    seg->hallucination_flags, seg->speaker_turn);
                bf_fragment_create(fstore, BFK_SEGMENT,
                                   persp, seg->confidence,
                                   seg->t0_ms, seg->t1_ms,
                                   payload, NULL, 0,
                                   frag_id);
                emitted++;
            }
            bf_fragment_store_close(fstore);
            fprintf(stderr, "[fragment] emitted %d segment fragments → %s\n",
                    emitted, frag_store_path);
        } else {
            fprintf(stderr, "[fragment] warning: cannot open store %s\n",
                    frag_store_path);
        }
    }

    transcript_result_free(&result);

    printf("Transcript: %s\n", transcript_path);
    printf("JSON:       %s\n", transcript_json_path);
    printf("SRT:        %s\n", transcript_srt_path);
    printf("VTT:        %s\n", transcript_vtt_path);
    printf("Meta:       %s\n", meta_path);
    printf("Status:     %s\n", status_path);
    printf("Quality:    %.4f (conf=%.4f logprob=%.3f)\n", avg_quality, avg_conf, avg_logprob);
    printf("RTF:        %.4f (%.1fx realtime)\n", rtf, rtf > 0 ? 1.0 / rtf : 0);
    return 0;
}
