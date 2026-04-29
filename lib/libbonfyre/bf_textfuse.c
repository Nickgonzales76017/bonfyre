/*
 * bf_textfuse.c — Single-pass multi-pattern text scanner
 *
 * Backend selection:
 *   1. Hyperscan (if <hs/hs.h> available) — SIMD DFA, >1 GB/s
 *   2. Aho-Corasick fallback (pure C)    — ~200 MB/s
 *
 * Built-in patterns cover BonfyreClean, BonfyreTag, BonfyreBrief,
 * and BonfyreProof pattern sets.
 */

#include "bf_textfuse.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ── Try Hyperscan ───────────────────────────────────────────── */

#ifdef __has_include
#if __has_include(<hs/hs.h>)
#include <hs/hs.h>
#define FUSE_HAS_HYPERSCAN 1
#endif
#endif

/* ── Aho-Corasick fallback structures ────────────────────────── */

#define AC_ALPHABET     256
#define AC_MAX_STATES   16384
#define AC_MAX_PATTERNS 2048

typedef struct {
    int     go[AC_ALPHABET];     /* goto function  */
    int     fail;                /* failure link   */
    int     output;              /* pattern index (-1 = none) */
    int     depth;               /* depth in trie  */
} ac_state_t;

typedef struct {
    char       *text;
    size_t      len;
    uint16_t    pattern_class;
    int         case_insensitive;
} fuse_pattern_t;

struct bf_textfuse {
    /* Pattern database */
    fuse_pattern_t  patterns[AC_MAX_PATTERNS];
    int             n_patterns;
    int             compiled;

#ifdef FUSE_HAS_HYPERSCAN
    hs_database_t  *hs_db;
    hs_scratch_t   *hs_scratch;
#endif

    /* Aho-Corasick fallback */
    ac_state_t     *ac_states;
    int             ac_n_states;
};

/* ── Built-in patterns ───────────────────────────────────────── */

typedef struct {
    const char    *pattern;
    uint16_t       pattern_class;
    int            is_regex;
} builtin_pattern_t;

static const builtin_pattern_t BUILTINS[] = {
    /* Filler phrases (BonfyreClean multiword_fillers) */
    { "you know what I mean",           FUSE_CLASS_FILLER, 0 },
    { "or something like that",         FUSE_CLASS_FILLER, 0 },
    { "and stuff like that",            FUSE_CLASS_FILLER, 0 },
    { "you know what I'm saying",       FUSE_CLASS_FILLER, 0 },
    { "at the end of the day",          FUSE_CLASS_FILLER, 0 },
    { "to be honest with you",          FUSE_CLASS_FILLER, 0 },
    { "as a matter of fact",            FUSE_CLASS_FILLER, 0 },
    { "for what it's worth",            FUSE_CLASS_FILLER, 0 },
    { "kind of sort of",                FUSE_CLASS_FILLER, 0 },
    { "I mean like",                    FUSE_CLASS_FILLER, 0 },
    { "you know",                       FUSE_CLASS_FILLER, 0 },
    { "I mean",                         FUSE_CLASS_FILLER, 0 },
    { "sort of",                        FUSE_CLASS_FILLER, 0 },
    { "kind of",                        FUSE_CLASS_FILLER, 0 },
    { "basically",                      FUSE_CLASS_FILLER, 0 },
    { "literally",                      FUSE_CLASS_FILLER, 0 },
    { "essentially",                    FUSE_CLASS_FILLER, 0 },
    { "actually",                       FUSE_CLASS_FILLER, 0 },

    /* Hallucination patterns (BonfyreClean) */
    { "thank you thank you thank you",  FUSE_CLASS_HALLUC, 0 },
    { "thanks for watching",            FUSE_CLASS_HALLUC, 0 },
    { "please subscribe",               FUSE_CLASS_HALLUC, 0 },
    { "like and subscribe",             FUSE_CLASS_HALLUC, 0 },
    { "hit the bell icon",              FUSE_CLASS_HALLUC, 0 },
    { "don't forget to subscribe",      FUSE_CLASS_HALLUC, 0 },
    { "leave a comment below",          FUSE_CLASS_HALLUC, 0 },

    /* Chunk/section headers (BonfyreClean) */
    { "chapter ",                        FUSE_CLASS_CHUNK_HDR, 0 },
    { "section ",                        FUSE_CLASS_CHUNK_HDR, 0 },
    { "part ",                           FUSE_CLASS_CHUNK_HDR, 0 },

    /* Quality signals (BonfyreProof positive indicators) */
    { "in other words",                 FUSE_CLASS_QUALITY, 0 },
    { "for example",                    FUSE_CLASS_QUALITY, 0 },
    { "let me explain",                 FUSE_CLASS_QUALITY, 0 },
    { "the key point is",              FUSE_CLASS_QUALITY, 0 },
    { "specifically",                   FUSE_CLASS_QUALITY, 0 },
    { "in particular",                  FUSE_CLASS_QUALITY, 0 },
    { "importantly",                    FUSE_CLASS_QUALITY, 0 },
    { "therefore",                      FUSE_CLASS_QUALITY, 0 },
    { "consequently",                   FUSE_CLASS_QUALITY, 0 },
    { "furthermore",                    FUSE_CLASS_QUALITY, 0 },
    { "however",                        FUSE_CLASS_QUALITY, 0 },
    { "nevertheless",                   FUSE_CLASS_QUALITY, 0 },

    { NULL, 0, 0 }
};

/* ── Aho-Corasick builder ────────────────────────────────────── */

static int ac_add_pattern(bf_textfuse_t *fuse, const char *pat, size_t len,
                           int pat_idx, int case_insensitive) {
    if (!fuse->ac_states) {
        fuse->ac_states = calloc(AC_MAX_STATES, sizeof(ac_state_t));
        if (!fuse->ac_states) return -1;
        /* Initialize root state */
        memset(fuse->ac_states[0].go, -1, sizeof(fuse->ac_states[0].go));
        fuse->ac_states[0].fail = 0;
        fuse->ac_states[0].output = -1;
        fuse->ac_n_states = 1;
    }

    int cur = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char ch = (unsigned char)pat[i];
        if (case_insensitive) ch = (unsigned char)tolower(ch);

        if (fuse->ac_states[cur].go[ch] == -1) {
            if (fuse->ac_n_states >= AC_MAX_STATES) return -1;
            int ns = fuse->ac_n_states++;
            memset(fuse->ac_states[ns].go, -1, sizeof(fuse->ac_states[ns].go));
            fuse->ac_states[ns].fail = 0;
            fuse->ac_states[ns].output = -1;
            fuse->ac_states[ns].depth = fuse->ac_states[cur].depth + 1;
            fuse->ac_states[cur].go[ch] = ns;
        }
        cur = fuse->ac_states[cur].go[ch];
    }

    fuse->ac_states[cur].output = pat_idx;
    return 0;
}

static void ac_build_failure(bf_textfuse_t *fuse) {
    /* BFS to build failure links */
    int *queue = malloc(fuse->ac_n_states * sizeof(int));
    if (!queue) return;
    int head = 0, tail = 0;

    /* Initialize: depth-1 states fail to root */
    for (int c = 0; c < AC_ALPHABET; c++) {
        int s = fuse->ac_states[0].go[c];
        if (s > 0) {
            fuse->ac_states[s].fail = 0;
            queue[tail++] = s;
        } else {
            fuse->ac_states[0].go[c] = 0;  /* Missing → root */
        }
    }

    while (head < tail) {
        int u = queue[head++];
        for (int c = 0; c < AC_ALPHABET; c++) {
            int v = fuse->ac_states[u].go[c];
            if (v > 0) {
                fuse->ac_states[v].fail =
                    fuse->ac_states[fuse->ac_states[u].fail].go[c];
                if (fuse->ac_states[v].output == -1)
                    fuse->ac_states[v].output =
                        fuse->ac_states[fuse->ac_states[v].fail].output;
                queue[tail++] = v;
            } else {
                fuse->ac_states[u].go[c] =
                    fuse->ac_states[fuse->ac_states[u].fail].go[c];
            }
        }
    }

    free(queue);
}

/* ── Constructor ─────────────────────────────────────────────── */

bf_textfuse_t *bf_textfuse_new(void) {
    bf_textfuse_t *fuse = calloc(1, sizeof(*fuse));
    if (!fuse) return NULL;

    /* Add built-in patterns */
    for (int i = 0; BUILTINS[i].pattern; i++) {
        bf_textfuse_add(fuse, BUILTINS[i].pattern,
                         BUILTINS[i].pattern_class,
                         BUILTINS[i].is_regex, 1);
    }

    /* Auto-compile built-ins */
    bf_textfuse_compile(fuse);

    return fuse;
}

int bf_textfuse_add(bf_textfuse_t *fuse, const char *pattern,
                     uint16_t pattern_class, int is_regex,
                     int case_insensitive) {
    if (!fuse || !pattern || fuse->n_patterns >= AC_MAX_PATTERNS) return -1;

    fuse_pattern_t *p = &fuse->patterns[fuse->n_patterns];
    p->len = strlen(pattern);
    p->text = malloc(p->len + 1);
    if (!p->text) return -1;
    memcpy(p->text, pattern, p->len + 1);
    p->pattern_class = pattern_class;
    p->case_insensitive = case_insensitive;

    (void)is_regex;  /* Used only for Hyperscan path */
    fuse->n_patterns++;
    fuse->compiled = 0;

    return 0;
}

int bf_textfuse_compile(bf_textfuse_t *fuse) {
    if (!fuse || fuse->n_patterns == 0) return -1;

#ifdef FUSE_HAS_HYPERSCAN
    /* Build Hyperscan database */
    const char **expressions = malloc(fuse->n_patterns * sizeof(char *));
    unsigned *flags = malloc(fuse->n_patterns * sizeof(unsigned));
    unsigned *ids = malloc(fuse->n_patterns * sizeof(unsigned));
    if (!expressions || !flags || !ids) {
        free(expressions); free(flags); free(ids);
        goto fallback;
    }

    for (int i = 0; i < fuse->n_patterns; i++) {
        expressions[i] = fuse->patterns[i].text;
        flags[i] = HS_FLAG_SOM_LEFTMOST;
        if (fuse->patterns[i].case_insensitive)
            flags[i] |= HS_FLAG_CASELESS;
        ids[i] = (unsigned)i;
    }

    hs_compile_error_t *compile_err;
    hs_error_t err = hs_compile_multi(expressions, flags, ids,
                                       (unsigned)fuse->n_patterns,
                                       HS_MODE_BLOCK, NULL,
                                       &fuse->hs_db, &compile_err);
    free(expressions);
    free(flags);
    free(ids);

    if (err != HS_SUCCESS) {
        hs_free_compile_error(compile_err);
        goto fallback;
    }

    err = hs_alloc_scratch(fuse->hs_db, &fuse->hs_scratch);
    if (err != HS_SUCCESS) {
        hs_free_database(fuse->hs_db);
        fuse->hs_db = NULL;
        goto fallback;
    }

    fuse->compiled = 1;
    return 0;

fallback:
#endif

    /* Aho-Corasick fallback */
    for (int i = 0; i < fuse->n_patterns; i++) {
        ac_add_pattern(fuse, fuse->patterns[i].text, fuse->patterns[i].len,
                        i, fuse->patterns[i].case_insensitive);
    }
    ac_build_failure(fuse);
    fuse->compiled = 1;
    return 0;
}

/* ── Scan callbacks ──────────────────────────────────────────── */

static void add_match(bf_fuse_result_t *result, uint32_t pat_id,
                       uint16_t pat_class, uint64_t from, uint64_t to) {
    if (result->match_count >= result->match_cap) {
        int new_cap = result->match_cap ? result->match_cap * 2 : 256;
        bf_fuse_match_t *new_m = realloc(result->matches,
                                          new_cap * sizeof(bf_fuse_match_t));
        if (!new_m) return;
        result->matches = new_m;
        result->match_cap = new_cap;
    }

    bf_fuse_match_t *m = &result->matches[result->match_count++];
    m->pattern_id = pat_id;
    m->pattern_class = pat_class;
    m->from = from;
    m->to = to;

    /* Update counters */
    if (pat_class & FUSE_CLASS_FILLER)    result->filler_count++;
    if (pat_class & FUSE_CLASS_HALLUC)    result->halluc_count++;
    if (pat_class & FUSE_CLASS_KEYWORD)   result->keyword_count++;
    if (pat_class & FUSE_CLASS_QUALITY)   result->quality_signals++;
    if (pat_class & FUSE_CLASS_TAG)       result->tag_count++;
    if (pat_class & FUSE_CLASS_CHUNK_HDR) result->chunk_hdr_count++;
}

#ifdef FUSE_HAS_HYPERSCAN
typedef struct {
    bf_textfuse_t     *fuse;
    bf_fuse_result_t  *result;
    uint16_t           classes;
} hs_scan_ctx_t;

static int hs_match_handler(unsigned int id, unsigned long long from,
                              unsigned long long to,
                              unsigned int flags, void *ctx) {
    (void)flags;
    hs_scan_ctx_t *sc = (hs_scan_ctx_t *)ctx;
    if (id >= (unsigned)sc->fuse->n_patterns) return 0;

    uint16_t pat_class = sc->fuse->patterns[id].pattern_class;
    if (!(pat_class & sc->classes)) return 0;

    add_match(sc->result, id, pat_class, from, to);
    return 0;
}
#endif

/* ── Main scan ───────────────────────────────────────────────── */

int bf_textfuse_scan(bf_textfuse_t *fuse, const char *text, size_t len,
                      uint16_t classes, bf_fuse_result_t *result) {
    if (!fuse || !text || !result || !fuse->compiled) return -1;

    memset(result, 0, sizeof(*result));

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

#ifdef FUSE_HAS_HYPERSCAN
    if (fuse->hs_db && fuse->hs_scratch) {
        hs_scan_ctx_t ctx = { .fuse = fuse, .result = result, .classes = classes };
        hs_scan(fuse->hs_db, text, (unsigned)len, 0,
                fuse->hs_scratch, hs_match_handler, &ctx);
        goto done;
    }
#endif

    /* Aho-Corasick scan */
    if (fuse->ac_states) {
        int state = 0;
        for (size_t i = 0; i < len; i++) {
            unsigned char ch = (unsigned char)text[i];
            ch = (unsigned char)tolower(ch);  /* AC built case-insensitive */

            state = fuse->ac_states[state].go[ch];

            /* Check for matches via output chain */
            int out = fuse->ac_states[state].output;
            if (out >= 0 && out < fuse->n_patterns) {
                uint16_t pat_class = fuse->patterns[out].pattern_class;
                if (pat_class & classes) {
                    uint64_t match_end = i + 1;
                    uint64_t match_start = match_end - fuse->patterns[out].len;
                    add_match(result, (uint32_t)out, pat_class,
                              match_start, match_end);
                }
            }
        }
    }

#ifdef FUSE_HAS_HYPERSCAN
done:
#endif

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed_us = (double)(t1.tv_sec - t0.tv_sec) * 1e6 +
                         (double)(t1.tv_nsec - t0.tv_nsec) / 1e3;
    result->scan_time_us = elapsed_us;
    result->throughput_mbps = elapsed_us > 0
        ? (double)len / elapsed_us  /* bytes / microsecond = MB/s */
        : 0;

    /* Compute quality score */
    int total_words = 0;
    for (size_t i = 0; i < len; i++) {
        if (text[i] == ' ' && (i == 0 || text[i-1] != ' ')) total_words++;
    }
    if (total_words == 0) total_words = 1;

    double filler_ratio = (double)result->filler_count / (double)total_words;
    double quality_boost = (double)result->quality_signals * 0.02;
    result->quality_score = 1.0 - filler_ratio * 5.0 - (double)result->halluc_count * 0.1
                             + quality_boost;
    if (result->quality_score < 0) result->quality_score = 0;
    if (result->quality_score > 1) result->quality_score = 1;

    return 0;
}

/* ── Clean operation ─────────────────────────────────────────── */

char *bf_textfuse_clean(bf_textfuse_t *fuse, const char *text, size_t len,
                         size_t *out_len) {
    bf_fuse_result_t result = {0};
    int rc = bf_textfuse_scan(fuse, text, len,
                               FUSE_CLASS_FILLER | FUSE_CLASS_HALLUC, &result);
    if (rc != 0 || result.match_count == 0) {
        bf_fuse_result_free(&result);
        char *copy = malloc(len + 1);
        if (copy) { memcpy(copy, text, len); copy[len] = '\0'; }
        if (out_len) *out_len = len;
        return copy;
    }

    /* Sort matches by start offset (insertion sort — small N) */
    for (int i = 1; i < result.match_count; i++) {
        bf_fuse_match_t tmp = result.matches[i];
        int j = i - 1;
        while (j >= 0 && result.matches[j].from > tmp.from) {
            result.matches[j + 1] = result.matches[j];
            j--;
        }
        result.matches[j + 1] = tmp;
    }

    /* Build output skipping matched regions */
    char *out = malloc(len + 1);
    if (!out) { bf_fuse_result_free(&result); return NULL; }

    size_t olen = 0;
    size_t pos = 0;
    for (int i = 0; i < result.match_count; i++) {
        uint64_t from = result.matches[i].from;
        uint64_t to = result.matches[i].to;
        if (from > pos) {
            memcpy(out + olen, text + pos, (size_t)(from - pos));
            olen += (size_t)(from - pos);
        }
        if (to > pos) pos = (size_t)to;
    }
    if (pos < len) {
        memcpy(out + olen, text + pos, len - pos);
        olen += len - pos;
    }
    out[olen] = '\0';

    if (out_len) *out_len = olen;
    bf_fuse_result_free(&result);
    return out;
}

/* ── Cleanup ─────────────────────────────────────────────────── */

void bf_fuse_result_free(bf_fuse_result_t *result) {
    if (!result) return;
    free(result->matches);
    result->matches = NULL;
    result->match_count = 0;
    result->match_cap = 0;
}

void bf_textfuse_free(bf_textfuse_t *fuse) {
    if (!fuse) return;

#ifdef FUSE_HAS_HYPERSCAN
    if (fuse->hs_scratch) hs_free_scratch(fuse->hs_scratch);
    if (fuse->hs_db) hs_free_database(fuse->hs_db);
#endif

    free(fuse->ac_states);

    for (int i = 0; i < fuse->n_patterns; i++)
        free(fuse->patterns[i].text);

    free(fuse);
}
