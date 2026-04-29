/*
 * bf_textfuse.h — Hyperscan/Vectorscan single-pass text fusion
 *
 * Compiles ALL text processing patterns from BonfyreClean, BonfyreTag,
 * BonfyreBrief (keyword extraction), and BonfyreProof (quality scoring)
 * into a single Hyperscan DFA.  One scan fires all matches simultaneously
 * at >1 GB/s, replacing 4 sequential pipeline stages.
 *
 * Pattern classes:
 *   FUSE_CLASS_FILLER      — filler phrases to strip ("you know", "um")
 *   FUSE_CLASS_HALLUC      — hallucination patterns ("thank you" repeated)
 *   FUSE_CLASS_KEYWORD     — keyword extraction terms (TF-IDF candidates)
 *   FUSE_CLASS_QUALITY     — quality signals (sentence structure, fluency)
 *   FUSE_CLASS_TAG         — topic/entity tags (technical terms)
 *   FUSE_CLASS_CHUNK_HDR   — chunk/section headers
 *
 * When Hyperscan is not available, falls back to Aho-Corasick automaton
 * (pure C, ~100KB compiled state, ~200 MB/s throughput).
 */

#ifndef BF_TEXTFUSE_H
#define BF_TEXTFUSE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Pattern classes ─────────────────────────────────────────── */

#define FUSE_CLASS_FILLER     0x0001
#define FUSE_CLASS_HALLUC     0x0002
#define FUSE_CLASS_KEYWORD    0x0004
#define FUSE_CLASS_QUALITY    0x0008
#define FUSE_CLASS_TAG        0x0010
#define FUSE_CLASS_CHUNK_HDR  0x0020
#define FUSE_CLASS_ALL        0xFFFF

/* ── Match record ────────────────────────────────────────────── */

typedef struct {
    uint32_t    pattern_id;     /* Pattern index in the database     */
    uint16_t    pattern_class;  /* FUSE_CLASS_*                      */
    uint64_t    from;           /* Start offset in text              */
    uint64_t    to;             /* End offset in text (exclusive)    */
} bf_fuse_match_t;

/* ── Fused scan results ──────────────────────────────────────── */

typedef struct {
    /* Counts per class */
    int         filler_count;
    int         halluc_count;
    int         keyword_count;
    int         quality_signals;
    int         tag_count;
    int         chunk_hdr_count;

    /* Match array */
    bf_fuse_match_t *matches;
    int              match_count;
    int              match_cap;

    /* Quality score (0.0 – 1.0) */
    double      quality_score;

    /* Metrics */
    double      scan_time_us;
    double      throughput_mbps;
} bf_fuse_result_t;

/* ── Database ────────────────────────────────────────────────── */

typedef struct bf_textfuse bf_textfuse_t;

/*
 * Create a text fusion engine.  Compiles all built-in patterns.
 * If Hyperscan is available, uses vectorized DFA.
 * Otherwise falls back to Aho-Corasick.
 * Returns NULL on failure.
 */
bf_textfuse_t *bf_textfuse_new(void);

/*
 * Add a custom pattern (before first scan).
 *   pattern:      literal string or regex (if is_regex)
 *   pattern_class: FUSE_CLASS_*
 *   is_regex:     0 = literal, 1 = Hyperscan-compatible regex
 *   case_insensitive: 1 = case-insensitive matching
 */
int bf_textfuse_add(bf_textfuse_t *fuse, const char *pattern,
                     uint16_t pattern_class, int is_regex,
                     int case_insensitive);

/*
 * Compile all patterns.  Must be called after adding custom patterns
 * and before scanning.  The built-in patterns are compiled in _new().
 */
int bf_textfuse_compile(bf_textfuse_t *fuse);

/*
 * Scan text in a single pass.  Fires all pattern classes simultaneously.
 *   text:    input text (UTF-8, not necessarily null-terminated)
 *   len:     text length
 *   classes: bitmask of classes to match (FUSE_CLASS_ALL for everything)
 *   result:  output (caller must call bf_fuse_result_free when done)
 */
int bf_textfuse_scan(bf_textfuse_t *fuse, const char *text, size_t len,
                      uint16_t classes, bf_fuse_result_t *result);

/*
 * Apply Clean operations: strip all FILLER + HALLUC matches from text.
 * Returns new text (caller frees).  out_len receives cleaned length.
 */
char *bf_textfuse_clean(bf_textfuse_t *fuse, const char *text, size_t len,
                         size_t *out_len);

void bf_fuse_result_free(bf_fuse_result_t *result);
void bf_textfuse_free(bf_textfuse_t *fuse);

#ifdef __cplusplus
}
#endif

#endif /* BF_TEXTFUSE_H */
