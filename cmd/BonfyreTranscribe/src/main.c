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
#ifdef __APPLE__
#include <mach/mach_time.h>
#endif

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

static void iso_timestamp(char *buffer, size_t size) {
    time_t now = time(NULL);
    struct tm tm_utc;
    gmtime_r(&now, &tm_utc);
    strftime(buffer, size, "%Y-%m-%dT%H:%M:%SZ", &tm_utc);
}

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

    /* Find "data" chunk (skip fmt and other chunks) */
    uint32_t data_size = 0;
    for (;;) {
        char chunk_id[4]; uint32_t chunk_size;
        if (fread(chunk_id, 1, 4, fp) != 4) goto fail;
        if (fread(&chunk_size, 4, 1, fp) != 1) goto fail;
        if (memcmp(chunk_id, "data", 4) == 0) {
            data_size = chunk_size;
            break;
        }
        fseek(fp, (long)chunk_size, SEEK_CUR);
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
 * ================================================================ */

/* Transcription result — one per segment */
typedef struct {
    int64_t t0_ms;           /* segment start (ms) */
    int64_t t1_ms;           /* segment end (ms) */
    float   confidence;      /* average token probability */
    float   no_speech_prob;  /* P(no speech) for this segment */
    int     speaker_turn;    /* 1 if next segment is different speaker */
    char    text[4096];
} TranscriptSegment;

typedef struct {
    TranscriptSegment *segments;
    int count;
    int cap;
    /* Timing from whisper_get_timings() */
    float encode_ms;
    float decode_ms;
    float sample_ms;
    float prompt_ms;
} TranscriptResult;

static void transcript_result_init(TranscriptResult *r) {
    memset(r, 0, sizeof(*r));
    r->cap = 256;
    r->segments = malloc((size_t)r->cap * sizeof(TranscriptSegment));
}

static void transcript_result_free(TranscriptResult *r) {
    free(r->segments);
    memset(r, 0, sizeof(*r));
}

/* Suppress noisy ggml/Metal init logging during model load */
static void whisper_log_suppress(enum ggml_log_level level, const char *text, void *user_data) {
    (void)level; (void)text; (void)user_data;
}

/* Extract segments from a completed whisper_full() call */
static void extract_segments(struct whisper_context *ctx,
                             TranscriptResult *result,
                             float no_speech_filter) {
    int n = whisper_full_n_segments(ctx);
    for (int i = 0; i < n; i++) {
        float nsp = whisper_full_get_segment_no_speech_prob(ctx, i);
        if (nsp > no_speech_filter) continue;  /* Round 7: filter silence garbage */

        const char *text = whisper_full_get_segment_text(ctx, i);
        if (!text || text[0] == '\0') continue;  /* Round 9: suppress_blank */

        if (result->count >= result->cap) {
            result->cap *= 2;
            result->segments = realloc(result->segments,
                (size_t)result->cap * sizeof(TranscriptSegment));
        }
        TranscriptSegment *seg = &result->segments[result->count];
        seg->t0_ms = whisper_full_get_segment_t0(ctx, i) * 10;  /* centiseconds → ms */
        seg->t1_ms = whisper_full_get_segment_t1(ctx, i) * 10;
        seg->no_speech_prob = nsp;
        seg->speaker_turn = whisper_full_get_segment_speaker_turn_next(ctx, i) ? 1 : 0;
        snprintf(seg->text, sizeof(seg->text), "%s", text);

        /* Compute average token probability for confidence scoring (Round 6) */
        int n_tokens = whisper_full_n_tokens(ctx, i);
        double prob_sum = 0;
        int prob_count = 0;
        for (int t = 0; t < n_tokens; t++) {
            float p = whisper_full_get_token_p(ctx, i, t);
            if (p > 0) { prob_sum += p; prob_count++; }
        }
        seg->confidence = prob_count > 0 ? (float)(prob_sum / prob_count) : 0;

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

/* Write structured JSON transcript with timestamps + confidence (Round 8, 18) */
static int write_transcript_json(const TranscriptResult *r, const char *path) {
    FILE *fp = fopen(path, "w");
    if (!fp) return -1;
    fprintf(fp, "{\n  \"sourceSystem\": \"BonfyreTranscribe\",\n  \"segments\": [\n");
    for (int i = 0; i < r->count; i++) {
        const TranscriptSegment *s = &r->segments[i];
        fprintf(fp,
            "    {\"t0\": %lld, \"t1\": %lld, \"confidence\": %.3f, "
            "\"no_speech\": %.3f, \"speaker_turn\": %s, \"text\": \"",
            (long long)s->t0_ms, (long long)s->t1_ms, s->confidence,
            s->no_speech_prob, s->speaker_turn ? "true" : "false");
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
    fprintf(fp, "  \"timing\": {\"encode_ms\": %.1f, \"decode_ms\": %.1f, "
            "\"sample_ms\": %.1f, \"prompt_ms\": %.1f}\n",
            r->encode_ms, r->decode_ms, r->sample_ms, r->prompt_ms);
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
            "bonfyre-transcribe — native libwhisper transcription engine\n\n"
            "Usage:\n"
            "  bonfyre-transcribe <input-audio> <output-dir> [--model NAME] [--language CODE]\n"
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
            "  Metal GPU + flash attention. Multi-processor parallel decode.\n"
            "  Segment timestamps, token-level confidence, no-speech filtering.\n"
            "  Structured JSON transcript output with timing metrics.\n"
            "\n"
            "Vocabulary learning:\n"
            "  Flat binary .bfvocab format (BFVD02). Zero SQLite, zero process spawns.\n"
            "  BM25-weighted terms injected as whisper initial_prompt.\n"
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

    struct whisper_context_params cparams = whisper_context_default_params();
    cparams.use_gpu = true;
    cparams.flash_attn = true;

    struct whisper_context *wctx = whisper_init_from_file_with_params(model_path, cparams);
    if (!wctx) {
        fprintf(stderr, "Failed to load whisper model: %s\n", model_path);
        return 1;
    }

    double t_model_loaded = wall_clock_ms();
    fprintf(stderr, "[whisper] model loaded: %s (%.0f ms)\n",
            model_path, t_model_loaded - t_model_start);

    /* Configure decoding parameters */
    struct whisper_full_params wparams = whisper_full_default_params(
        greedy ? WHISPER_SAMPLING_GREEDY : WHISPER_SAMPLING_BEAM_SEARCH);

    wparams.n_threads     = threads;
    wparams.n_max_text_ctx = 224;    /* Round 5: bound context window */
    wparams.no_timestamps = false;   /* Round 8: we want segment timestamps */
    wparams.print_special = false;
    wparams.print_progress = false;
    wparams.print_realtime = false;
    wparams.print_timestamps = false;

    wparams.suppress_blank = true;   /* Round 9: no empty segments */
    wparams.suppress_nst   = true;   /* Suppress non-speech tokens */

    wparams.temperature     = 0.0f;
    wparams.temperature_inc = denoised ? 0.0f : 0.2f;  /* Round 11: skip fallback on clean audio */
    wparams.entropy_thold   = 2.0f;
    wparams.logprob_thold   = -1.0f; /* Round 10: filter low-confidence */
    wparams.no_speech_thold = no_speech_thold;

    wparams.language = language ? language : "en";
    wparams.initial_prompt = vocab_prompt;
    wparams.carry_initial_prompt = true;  /* Always prepend domain terms */

    if (greedy) {
        wparams.greedy.best_of = best_of;
    } else {
        wparams.beam_search.beam_size = beam_size;
    }

    TranscriptResult result;
    transcript_result_init(&result);

    double t_transcribe_start = wall_clock_ms();

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

        /* Process each chunk through the SAME whisper context.
         * Model stays loaded. Metal GPU context persists.
         * No fork, no exec, no file I/O for results. */
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

            whisper_reset_timings(wctx);
            if (whisper_full(wctx, wparams, samples, n_samples) != 0) {
                fprintf(stderr, "[whisper] transcription failed on chunk %d\n", i);
                free(samples);
                continue;
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

        fprintf(stderr, "[whisper] audio: %.1f seconds, %d samples\n",
                (double)n_samples / WHISPER_SAMPLE_RATE, n_samples);

        /* Round 4: whisper_full_parallel() uses multiple processors for decode.
         * Each processor handles a separate audio segment independently —
         * true parallelism on multi-core machines. */
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

        extract_segments(wctx, &result, no_speech_thold);
        free(samples);
        write_chunk_progress(progress_path, 1, 1, "completed");
    }

    double t_transcribe_done = wall_clock_ms();

    /* Capture whisper internal timings (Round 20) */
    struct whisper_timings *wtimings = whisper_get_timings(wctx);
    if (wtimings) {
        result.encode_ms = wtimings->encode_ms;
        result.decode_ms = wtimings->decode_ms;
        result.sample_ms = wtimings->sample_ms;
        result.prompt_ms = wtimings->prompt_ms;
    }

    whisper_free(wctx);

    /* ── Write outputs ─────────────────────────────────────────── */
    write_transcript_txt(&result, transcript_path);
    write_transcript_json(&result, transcript_json_path);

    double avg_conf = 0;
    if (result.count > 0) {
        double sum = 0;
        for (int i = 0; i < result.count; i++) sum += result.segments[i].confidence;
        avg_conf = sum / result.count;
    }
    fprintf(stderr, "[whisper] %d segments, avg confidence: %.2f\n",
            result.count, avg_conf);

    /* ── Vocab ingestion directly from memory (Round 12) ────────── */
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

    /* ── Timing metrics (Round 13) ──────────────────────────────── */
    double preprocess_ms = t_preprocess - t_start;
    double model_load_ms = t_model_loaded - t_model_start;
    double transcribe_ms = t_transcribe_done - t_transcribe_start;
    double total_ms       = t_end - t_start;

    /* Compute audio duration from sample count for RTF */
    int n_audio_check = 0;
    float *audio_check = read_wav_pcm_f32(normalized_path, &n_audio_check);
    double audio_duration_s = (double)n_audio_check / WHISPER_SAMPLE_RATE;
    free(audio_check);
    double rtf = audio_duration_s > 0 ? (transcribe_ms / 1000.0) / audio_duration_s : 0;

    fprintf(stderr,
            "[timing] preprocess: %.0f ms | model load: %.0f ms | "
            "transcribe: %.0f ms | total: %.0f ms\n",
            preprocess_ms, model_load_ms, transcribe_ms, total_ms);
    fprintf(stderr,
            "[timing] encode: %.0f ms | decode: %.0f ms | RTF: %.3f\n",
            result.encode_ms, result.decode_ms, rtf);

    /* ── meta.json ──────────────────────────────────────────────── */
    char timestamp[32];
    iso_timestamp(timestamp, sizeof(timestamp));
    char language_json[256];
    if (language) {
        snprintf(language_json, sizeof(language_json), "\"%s\"", language);
    } else {
        snprintf(language_json, sizeof(language_json), "null");
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
            "  \"vocab_enabled\": %s,\n"
            "  \"vocab_words_ingested\": %d,\n"
            "  \"segments\": %d,\n"
            "  \"chunk_count\": %d,\n"
            "  \"audio_duration_s\": %.2f,\n"
            "  \"preprocess_ms\": %.0f,\n"
            "  \"model_load_ms\": %.0f,\n"
            "  \"transcribe_ms\": %.0f,\n"
            "  \"encode_ms\": %.1f,\n"
            "  \"decode_ms\": %.1f,\n"
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
            no_vocab ? "false" : "true",
            words_ingested,
            result.count,
            chunk_count,
            audio_duration_s,
            preprocess_ms,
            model_load_ms,
            transcribe_ms,
            result.encode_ms,
            result.decode_ms,
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
            "  \"chunkCount\": %d,\n"
            "  \"rtf\": %.4f,\n"
            "  \"transcriptPath\": \"%s\",\n"
            "  \"transcriptJsonPath\": \"%s\",\n"
            "  \"metaPath\": \"%s\"\n"
            "}\n",
            timestamp,
            base_name,
            split_speech ? "true" : "false",
            silero_vad ? "true" : "false",
            denoised ? "true" : "false",
            result.count,
            chunk_count,
            rtf,
            transcript_path,
            transcript_json_path,
            meta_path);
    fclose(status);

    transcript_result_free(&result);

    printf("Transcript: %s\n", transcript_path);
    printf("JSON:       %s\n", transcript_json_path);
    printf("Meta:       %s\n", meta_path);
    printf("Status:     %s\n", status_path);
    printf("RTF:        %.4f (%.1fx realtime)\n", rtf, rtf > 0 ? 1.0 / rtf : 0);
    return 0;
}
