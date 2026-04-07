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

static int vocab_ingest(VocabStore *v, const char *transcript_path) {
    size_t txt_len = 0;
    char *text = bf_read_file(transcript_path, &txt_len);
    if (!text || txt_len == 0) { free(text); return 0; }

    stopword_init();

    int seen_cap = v->count + 4096;
    uint8_t *seen = calloc((size_t)seen_cap, 1);
    int word_count = 0;

    char *saveptr = NULL;
    char *tok = strtok_r(text, " \t\n\r", &saveptr);
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
    v->sorted = 0;  /* invalidate sort order */
    free(seen);
    free(text);
    return word_count;
}

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

static int copy_file_to_stream(const char *path, FILE *out) {
    FILE *in = fopen(path, "rb");
    if (!in) {
        perror("fopen");
        return 1;
    }
    char buffer[IO_BUF_SIZE];
    size_t bytes = 0;
    while ((bytes = fread(buffer, 1, sizeof(buffer), in)) > 0) {
        if (fwrite(buffer, 1, bytes, out) != bytes) {
            fclose(in);
            return 1;
        }
    }
    fclose(in);
    return 0;
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

/* Return 1 if the whisper binary is whisper.cpp (whisper-cli), 0 for Python whisper. */
static int is_whisper_cpp(const char *binary_path) {
    const char *base = strrchr(binary_path, '/');
    base = base ? base + 1 : binary_path;
    return (strstr(base, "whisper-cli") != NULL ||
            strstr(base, "whisper-cpp") != NULL);
}

/* Resolve a model name (e.g. "base") to a ggml model file path for whisper.cpp.
 * Prefers quantized (Q5_0) models over float16 — 62% smaller, 39% faster. */
static int resolve_whisper_cpp_model(const char *model_name, char *out, size_t out_size) {
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

static const char *default_whisper_binary(void) {
    const char *env = getenv("BONFYRE_WHISPER_BINARY");
    if (env && env[0] != '\0') return env;
    /* Prefer whisper-cli (whisper.cpp) if available */
    if (access("/opt/homebrew/bin/whisper-cli", X_OK) == 0)
        return "/opt/homebrew/bin/whisper-cli";
    if (access("/usr/local/bin/whisper-cli", X_OK) == 0)
        return "/usr/local/bin/whisper-cli";
    /* Fall back to Python whisper on PATH */
    return "whisper";
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
            "bonfyre-transcribe\n\n"
            "Usage:\n"
            "  bonfyre-transcribe <input-audio> <output-dir> [--model NAME] [--language CODE]\n"
            "                      [--whisper-binary PATH] [--media-prep-binary PATH]\n"
            "                      [--silero-vad] [--silero-script PATH]\n"
            "                      [--split-speech] [--noise-threshold DB] [--min-silence SEC]\n"
            "                      [--min-speech SEC] [--padding SEC]\n"
            "                      [--greedy] [--beam-size N] [--best-of N]\n"
            "                      [--vocab-db PATH] [--no-vocab]\n"
            "\n"
            "Vocabulary learning:\n"
            "  Flat binary .bfvocab format (BFVD02). Zero SQLite, zero process spawns.\n"
            "  BM25-weighted terms injected as whisper --prompt.\n"
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
    const char *whisper_binary = default_whisper_binary();
    int split_speech = 0;
    const char *noise_threshold = "-35dB";
    const char *min_silence = "0.35";
    const char *min_speech = "0.75";
    const char *padding = "0.15";
    int silero_vad = 0;
    int greedy = 1;               /* P0: default to greedy (was beam=5) */
    int beam_size = 1;
    int best_of = 1;
    int no_vocab = 0;
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
        } else if (strcmp(argv[i], "--whisper-binary") == 0 && i + 1 < argc) {
            whisper_binary = argv[++i];
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
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    if (ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output dir: %s\n", output_dir);
        return 1;
    }

    char normalized_path[PATH_MAX];
    char transcript_path[PATH_MAX];
    char meta_path[PATH_MAX];
    char status_path[PATH_MAX];
    char progress_path[PATH_MAX];
    char base_name[PATH_MAX];
    int chunk_count = 0;
    int completed_chunks = 0;
    int denoised = 0;

    snprintf(base_name, sizeof(base_name), "%s", path_basename(input_audio));
    strip_extension(base_name);
    snprintf(normalized_path, sizeof(normalized_path), "%s/normalized.wav", output_dir);
    snprintf(transcript_path, sizeof(transcript_path), "%s/normalized.txt", output_dir);
    snprintf(meta_path, sizeof(meta_path), "%s/meta.json", output_dir);
    snprintf(status_path, sizeof(status_path), "%s/transcribe-status.json", output_dir);
    snprintf(progress_path, sizeof(progress_path), "%s/chunk-progress.json", output_dir);

    /* ── Pipeline order: denoise at NATIVE sample rate first ────────
     * Denoising spectral subtraction works best on the original
     * sample rate before downsampling destroys high-freq detail.
     * Then normalize to 16kHz mono for whisper.                        */
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

    /* ----------------------------------------------------------------
     * Load vocabulary store and build decoder prompt.
     * Flat binary format — single read, zero process spawns.
     * BM25-weighted prompt biases decoder toward domain terms.
     * ---------------------------------------------------------------- */
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

    /* Thread count and beam-size as strings for argv */
    char threads_str[16];
    char beam_str[16];
    char bestof_str[16];
    snprintf(threads_str, sizeof(threads_str), "%d", threads);
    snprintf(beam_str, sizeof(beam_str), "%d", beam_size);
    snprintf(bestof_str, sizeof(bestof_str), "%d", best_of);

    if (split_speech) {
        char chunk_dir[PATH_MAX];
        char chunk_pattern[PATH_MAX];
        snprintf(chunk_dir, sizeof(chunk_dir), "%s/chunks", output_dir);
        snprintf(chunk_pattern, sizeof(chunk_pattern), "%s/chunk-%%03d.wav", chunk_dir);

        if (ensure_dir(chunk_dir) != 0) {
            fprintf(stderr, "Failed to create chunk dir: %s\n", chunk_dir);
            return 1;
        }

        if (silero_vad && access(silero_script, F_OK) == 0) {
            char *silero_argv[] = {
                "python3",
                (char *)silero_script,
                "--audio",
                normalized_path,
                "--out",
                chunk_dir,
                "--min-speech",
                (char *)min_speech,
                "--padding",
                (char *)padding,
                NULL
            };
            if (run_process(silero_argv) != 0) {
                fprintf(stderr, "Silero VAD split failed.\n");
                return 1;
            }
        } else {
            char *split_argv[] = {
                (char *)media_prep_binary,
                "split-speech",
                normalized_path,
                chunk_pattern,
                "--noise-threshold", (char *)noise_threshold,
                "--min-silence", (char *)min_silence,
                "--min-speech", (char *)min_speech,
                "--padding", (char *)padding,
                NULL
            };

            if (run_process(split_argv) != 0) {
                fprintf(stderr, "Speech split failed.\n");
                return 1;
            }
        }

        /* Single-pass: discover chunks + transcribe in one loop.
         * Avoids double stat() — count comes from the loop index. */
        FILE *combined = fopen(transcript_path, "w");
        if (!combined) {
            perror("fopen transcript");
            return 1;
        }

        for (int i = 0;; i++) {
            char chunk_audio[PATH_MAX];
            char chunk_txt[PATH_MAX];
            snprintf(chunk_audio, sizeof(chunk_audio), "%s/chunk-%03d.wav", chunk_dir, i);
            if (access(chunk_audio, F_OK) != 0) break;  /* no more chunks */
            chunk_count++;
            snprintf(chunk_txt, sizeof(chunk_txt), "%s/chunk-%03d.txt", chunk_dir, i);

            char *whisper_argv[48];
            int idx = 0;
            char model_path[PATH_MAX];
            char of_prefix[PATH_MAX];
            whisper_argv[idx++] = (char *)whisper_binary;

            if (is_whisper_cpp(whisper_binary)) {
                if (resolve_whisper_cpp_model(model, model_path, sizeof(model_path)) != 0) {
                    fprintf(stderr, "Cannot find whisper.cpp model for '%s'\n", model);
                    fclose(combined);
                    return 1;
                }
                snprintf(of_prefix, sizeof(of_prefix), "%s/chunk-%03d", chunk_dir, i);
                whisper_argv[idx++] = "-f";
                whisper_argv[idx++] = chunk_audio;
                whisper_argv[idx++] = "-m";
                whisper_argv[idx++] = model_path;
                whisper_argv[idx++] = "--output-txt";
                whisper_argv[idx++] = "-of";
                whisper_argv[idx++] = of_prefix;
                whisper_argv[idx++] = "--no-prints";
                whisper_argv[idx++] = "--flash-attn";
                whisper_argv[idx++] = "-t";
                whisper_argv[idx++] = threads_str;
                whisper_argv[idx++] = "-bs";
                whisper_argv[idx++] = beam_str;
                whisper_argv[idx++] = "-bo";
                whisper_argv[idx++] = bestof_str;
                /* Denoised audio is clean — disable temperature fallback */
                if (denoised) whisper_argv[idx++] = "--no-fallback";
                /* Suppress non-speech tokens (filler, music, etc.) */
                whisper_argv[idx++] = "--suppress-nst";
                /* Tighter entropy threshold for better rejection */
                whisper_argv[idx++] = "--entropy-thold";
                whisper_argv[idx++] = "2.0";
                /* Carry prompt context across chunks for coherence */
                if (i > 0) whisper_argv[idx++] = "--carry-initial-prompt";
                if (language) {
                    whisper_argv[idx++] = "-l";
                    whisper_argv[idx++] = (char *)language;
                }
                if (vocab_prompt) {
                    whisper_argv[idx++] = "--prompt";
                    whisper_argv[idx++] = vocab_prompt;
                }
            } else {
                /* Python whisper: positional audio, --task, --model name, --output_format, --output_dir */
                whisper_argv[idx++] = chunk_audio;
                whisper_argv[idx++] = "--task";
                whisper_argv[idx++] = "transcribe";
                whisper_argv[idx++] = "--model";
                whisper_argv[idx++] = (char *)model;
                whisper_argv[idx++] = "--output_format";
                whisper_argv[idx++] = "txt";
                whisper_argv[idx++] = "--output_dir";
                whisper_argv[idx++] = chunk_dir;
                if (language) {
                    whisper_argv[idx++] = "--language";
                    whisper_argv[idx++] = (char *)language;
                }
            }
            whisper_argv[idx] = NULL;

            if (run_process(whisper_argv) != 0) {
                fclose(combined);
                fprintf(stderr, "Whisper failed on chunk %d.\n", i);
                return 1;
            }
            if (access(chunk_txt, F_OK) != 0) {
                fclose(combined);
                fprintf(stderr, "Expected chunk transcript not found: %s\n", chunk_txt);
                return 1;
            }

            if (copy_file_to_stream(chunk_txt, combined) != 0) {
                fclose(combined);
                fprintf(stderr, "Failed to append chunk transcript.\n");
                return 1;
            }
            fprintf(combined, "\n");
            completed_chunks++;
            write_chunk_progress(progress_path, chunk_count, completed_chunks, "transcribing");
        }
        fclose(combined);
        write_chunk_progress(progress_path, chunk_count, completed_chunks, "completed");
    } else {
        char *whisper_argv[48];
        int idx = 0;
        char model_path[PATH_MAX];
        char of_prefix[PATH_MAX];
        whisper_argv[idx++] = (char *)whisper_binary;

        if (is_whisper_cpp(whisper_binary)) {
            if (resolve_whisper_cpp_model(model, model_path, sizeof(model_path)) != 0) {
                fprintf(stderr, "Cannot find whisper.cpp model for '%s'\n", model);
                return 1;
            }
            snprintf(of_prefix, sizeof(of_prefix), "%s/normalized", output_dir);
            whisper_argv[idx++] = "-f";
            whisper_argv[idx++] = normalized_path;
            whisper_argv[idx++] = "-m";
            whisper_argv[idx++] = model_path;
            whisper_argv[idx++] = "--output-txt";
            whisper_argv[idx++] = "-of";
            whisper_argv[idx++] = of_prefix;
            whisper_argv[idx++] = "--no-prints";
            whisper_argv[idx++] = "--flash-attn";
            whisper_argv[idx++] = "-t";
            whisper_argv[idx++] = threads_str;
            whisper_argv[idx++] = "-bs";
            whisper_argv[idx++] = beam_str;
            whisper_argv[idx++] = "-bo";
            whisper_argv[idx++] = bestof_str;
            if (denoised) whisper_argv[idx++] = "--no-fallback";
            whisper_argv[idx++] = "--suppress-nst";
            whisper_argv[idx++] = "--entropy-thold";
            whisper_argv[idx++] = "2.0";
            if (language) {
                whisper_argv[idx++] = "-l";
                whisper_argv[idx++] = (char *)language;
            }
            if (vocab_prompt) {
                whisper_argv[idx++] = "--prompt";
                whisper_argv[idx++] = vocab_prompt;
            }
        } else {
            /* Python whisper: positional audio, --task, --model name, --output_format, --output_dir */
            whisper_argv[idx++] = normalized_path;
            whisper_argv[idx++] = "--task";
            whisper_argv[idx++] = "transcribe";
            whisper_argv[idx++] = "--model";
            whisper_argv[idx++] = (char *)model;
            whisper_argv[idx++] = "--output_format";
            whisper_argv[idx++] = "txt";
            whisper_argv[idx++] = "--output_dir";
            whisper_argv[idx++] = (char *)output_dir;
            if (language) {
                whisper_argv[idx++] = "--language";
                whisper_argv[idx++] = (char *)language;
            }
        }
        whisper_argv[idx] = NULL;

        if (run_process(whisper_argv) != 0) {
            fprintf(stderr, "Whisper failed.\n");
            return 1;
        }

        if (access(transcript_path, F_OK) != 0) {
            fprintf(stderr, "Expected transcript not found: %s\n", transcript_path);
            return 1;
        }
        write_chunk_progress(progress_path, 1, 1, "completed");
    }

    /* ----------------------------------------------------------------
     * Post-transcription: ingest transcript into vocabulary store.
     * BM25 weights update, distribution sharpens, entropy drops.
     * ---------------------------------------------------------------- */
    int words_ingested = 0;
    if (!no_vocab && access(transcript_path, F_OK) == 0) {
        words_ingested = vocab_ingest(&vocab_store, transcript_path);
        vocab_store_save(&vocab_store, vocab_path);
        vocab_write_stats(&vocab_store, output_dir, vocab_path);
        fprintf(stderr, "[vocab] ingested %d words from transcript\n", words_ingested);
    }
    free(vocab_prompt);
    vocab_store_free(&vocab_store);

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
            "  \"created_at\": \"%s\",\n"
            "  \"input_audio\": \"%s\",\n"
            "  \"normalized_audio\": \"%s\",\n"
            "  \"transcript_path\": \"%s\",\n"
            "  \"model\": \"%s\",\n"
            "  \"language\": %s,\n"
            "  \"split_speech\": %s,\n"
            "  \"silero_vad\": %s,\n"
            "  \"denoised\": %s,\n"
            "  \"greedy\": %s,\n"
            "  \"beam_size\": %d,\n"
            "  \"threads\": %d,\n"
            "  \"vocab_enabled\": %s,\n"
            "  \"vocab_words_ingested\": %d,\n"
            "  \"chunk_count\": %d,\n"
            "  \"chunk_progress_path\": \"%s\",\n"
            "  \"whisper_binary\": \"%s\",\n"
            "  \"media_prep_binary\": \"%s\"\n"
            "}\n",
            timestamp,
            input_audio,
            normalized_path,
            transcript_path,
            model,
            language_json,
            split_speech ? "true" : "false",
            silero_vad ? "true" : "false",
            denoised ? "true" : "false",
            greedy ? "true" : "false",
            beam_size,
            threads,
            no_vocab ? "false" : "true",
            words_ingested,
            chunk_count,
            progress_path,
            whisper_binary,
            media_prep_binary);
    fclose(meta);

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
            "  \"chunkCount\": %d,\n"
            "  \"chunkProgressPath\": \"%s\",\n"
            "  \"transcriptPath\": \"%s\",\n"
            "  \"metaPath\": \"%s\"\n"
            "}\n",
            timestamp,
            base_name,
            split_speech ? "true" : "false",
            silero_vad ? "true" : "false",
            denoised ? "true" : "false",
            chunk_count,
            progress_path,
            transcript_path,
            meta_path);
    fclose(status);

    printf("Normalized: %s\n", normalized_path);
    printf("Transcript: %s\n", transcript_path);
    printf("Meta: %s\n", meta_path);
    printf("Status: %s\n", status_path);
    printf("Progress: %s\n", progress_path);
    return 0;
}
