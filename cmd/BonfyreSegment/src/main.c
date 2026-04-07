/*
 * BonfyreSegment — Idea Boundary Detection + Segment Graph
 *
 * Layer: Transform (pure — same input, same output)
 *
 * Takes Whisper JSON output (with word-level timestamps) and produces
 * a segment graph: nodes (ideas) with typed boundaries and edges (flow).
 *
 * Usage:
 *   bonfyre-segment graph    <whisper-json> <out-dir>  [--silence-gap 2.0]
 *   bonfyre-segment boundaries <whisper-json> <out-dir> [--silence-gap 2.0]
 *   bonfyre-segment rhythm   <whisper-json> <out-dir>
 *
 * Input: Whisper JSON (--output_format json) with segments[].start/end/text
 * Output: segment-graph.json, boundaries.json, rhythm.json
 */

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

/* ── Utilities ────────────────────────────────────────────────────── */

static int ensure_dir(const char *path) {
    char tmp[PATH_MAX];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    snprintf(tmp, sizeof(tmp), "%s", path);
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

static void iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL);
    struct tm t;
    gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
}

static char *read_file_contents(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;
    fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    if (sz < 0) { fclose(fp); return NULL; }
    rewind(fp);
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(fp); return NULL; }
    if (sz > 0 && fread(buf, 1, (size_t)sz, fp) != (size_t)sz) {
        free(buf); fclose(fp); return NULL;
    }
    buf[sz] = '\0';
    fclose(fp);
    return buf;
}

/* ── Whisper JSON segment parser ──────────────────────────────────── */

#define MAX_SEGMENTS 4096
#define MAX_TEXT     2048

typedef struct {
    double start;
    double end;
    char   text[MAX_TEXT];
} WhisperSeg;

typedef struct {
    WhisperSeg segs[MAX_SEGMENTS];
    int count;
} WhisperDoc;

/* Minimal JSON parser for Whisper output format:
 * { "text": "...", "segments": [ {"start": N, "end": N, "text": "..."}, ... ] }
 */
static double parse_number(const char **p) {
    while (**p == ' ' || **p == '\t' || **p == '\n' || **p == '\r' || **p == ':') (*p)++;
    char *end = NULL;
    double v = strtod(*p, &end);
    if (end) *p = end;
    return v;
}

static void parse_string(const char **p, char *out, size_t out_sz) {
    while (**p && **p != '"') (*p)++;
    if (**p == '"') (*p)++;
    size_t i = 0;
    while (**p && **p != '"' && i < out_sz - 1) {
        if (**p == '\\' && *(*p + 1)) {
            (*p)++;
            switch (**p) {
                case 'n':  out[i++] = '\n'; break;
                case 't':  out[i++] = '\t'; break;
                case '"':  out[i++] = '"'; break;
                case '\\': out[i++] = '\\'; break;
                default:   out[i++] = **p; break;
            }
        } else {
            out[i++] = **p;
        }
        (*p)++;
    }
    out[i] = '\0';
    if (**p == '"') (*p)++;
}

static int parse_whisper_json(const char *json, WhisperDoc *doc) {
    doc->count = 0;
    const char *seg_arr = strstr(json, "\"segments\"");
    if (!seg_arr) {
        fprintf(stderr, "error: no \"segments\" array in whisper JSON\n");
        return 1;
    }
    const char *p = seg_arr;
    /* Find opening bracket */
    while (*p && *p != '[') p++;
    if (!*p) return 1;
    p++;

    while (*p && doc->count < MAX_SEGMENTS) {
        /* Find next object */
        while (*p && *p != '{' && *p != ']') p++;
        if (!*p || *p == ']') break;
        p++; /* skip { */

        WhisperSeg *s = &doc->segs[doc->count];
        s->start = 0; s->end = 0; s->text[0] = '\0';

        /* Parse fields until closing } */
        int depth = 1;
        while (*p && depth > 0) {
            if (*p == '{') { depth++; p++; continue; }
            if (*p == '}') { depth--; p++; continue; }
            if (*p == '"') {
                char key[64] = {0};
                p++;
                int ki = 0;
                while (*p && *p != '"' && ki < 63) key[ki++] = *p++;
                key[ki] = '\0';
                if (*p == '"') p++;

                if (strcmp(key, "start") == 0) {
                    s->start = parse_number(&p);
                } else if (strcmp(key, "end") == 0) {
                    s->end = parse_number(&p);
                } else if (strcmp(key, "text") == 0) {
                    parse_string(&p, s->text, sizeof(s->text));
                } else {
                    /* Skip value - could be string, number, array, object */
                    while (*p == ' ' || *p == ':') p++;
                    if (*p == '"') {
                        p++;
                        while (*p && *p != '"') { if (*p == '\\') p++; p++; }
                        if (*p == '"') p++;
                    } else if (*p == '[') {
                        int d = 1; p++;
                        while (*p && d > 0) {
                            if (*p == '[') d++;
                            else if (*p == ']') d--;
                            p++;
                        }
                    }
                }
            } else {
                p++;
            }
        }
        if (s->text[0] != '\0') doc->count++;
    }
    return 0;
}

/* ── Segment type classification ──────────────────────────────────── */

typedef enum {
    SEG_INTRO,
    SEG_IDEA,
    SEG_EXAMPLE,
    SEG_STORY,
    SEG_CTA,
    SEG_TRANSITION,
    SEG_EXPLANATION,
    SEG_CLOSING
} SegType;

static const char *seg_type_str(SegType t) {
    switch (t) {
        case SEG_INTRO:       return "intro";
        case SEG_IDEA:        return "idea";
        case SEG_EXAMPLE:     return "example";
        case SEG_STORY:       return "story";
        case SEG_CTA:         return "call-to-action";
        case SEG_TRANSITION:  return "transition";
        case SEG_EXPLANATION: return "explanation";
        case SEG_CLOSING:     return "closing";
    }
    return "unknown";
}

static int str_contains_ci(const char *haystack, const char *needle) {
    size_t hlen = strlen(haystack), nlen = strlen(needle);
    if (nlen > hlen) return 0;
    for (size_t i = 0; i <= hlen - nlen; i++) {
        int match = 1;
        for (size_t j = 0; j < nlen; j++) {
            if (tolower((unsigned char)haystack[i+j]) != tolower((unsigned char)needle[j])) {
                match = 0; break;
            }
        }
        if (match) return 1;
    }
    return 0;
}

static SegType classify_segment(const char *text, int index, int total) {
    /* Position-based heuristics */
    if (index == 0) return SEG_INTRO;
    if (index == total - 1) return SEG_CLOSING;

    /* Keyword-based classification */
    static const char *cta_words[] = {
        "subscribe", "follow", "check out", "link", "click", "sign up",
        "try", "download", "share", "let me know", "comment", "challenge",
        "call to action", "go to", "head over", NULL
    };
    static const char *example_words[] = {
        "for example", "for instance", "like when", "imagine",
        "let's say", "picture this", "here's an example", "such as",
        "case study", "scenario", NULL
    };
    static const char *story_words[] = {
        "story", "happened", "remember when", "back when", "one time",
        "last year", "years ago", "told me", "experience", NULL
    };
    static const char *transition_words[] = {
        "now let's", "moving on", "next", "another thing", "also",
        "on top of that", "speaking of", "that brings us", "but here's", NULL
    };

    for (int i = 0; cta_words[i]; i++)
        if (str_contains_ci(text, cta_words[i])) return SEG_CTA;
    for (int i = 0; example_words[i]; i++)
        if (str_contains_ci(text, example_words[i])) return SEG_EXAMPLE;
    for (int i = 0; story_words[i]; i++)
        if (str_contains_ci(text, story_words[i])) return SEG_STORY;
    for (int i = 0; transition_words[i]; i++)
        if (str_contains_ci(text, transition_words[i])) return SEG_TRANSITION;

    return SEG_IDEA; /* Default: treat as idea/explanation */
}

/* ── Idea boundary detection ──────────────────────────────────────── */

typedef struct {
    int    seg_start;   /* First segment index in this boundary group */
    int    seg_end;     /* Last segment index (inclusive) */
    double time_start;
    double time_end;
    SegType type;
    double silence_before; /* Gap from previous group */
    double word_density;   /* words per second */
    int    word_count;
} IdeaBoundary;

#define MAX_BOUNDARIES 512

static int count_words(const char *text) {
    int count = 0, in_word = 0;
    for (const char *p = text; *p; p++) {
        if (isspace((unsigned char)*p)) { in_word = 0; }
        else if (!in_word) { in_word = 1; count++; }
    }
    return count;
}

static int detect_boundaries(const WhisperDoc *doc, double silence_gap,
                             IdeaBoundary *bounds) {
    if (doc->count == 0) return 0;
    int nb = 0;

    bounds[0].seg_start = 0;
    bounds[0].time_start = doc->segs[0].start;
    bounds[0].silence_before = 0;

    for (int i = 1; i < doc->count && nb < MAX_BOUNDARIES - 1; i++) {
        double gap = doc->segs[i].start - doc->segs[i-1].end;
        if (gap >= silence_gap) {
            /* Close current boundary */
            bounds[nb].seg_end = i - 1;
            bounds[nb].time_end = doc->segs[i-1].end;

            /* Compute word density and type */
            int wc = 0;
            char combined[MAX_TEXT * 10] = {0};
            for (int j = bounds[nb].seg_start; j <= bounds[nb].seg_end; j++) {
                wc += count_words(doc->segs[j].text);
                if (strlen(combined) + strlen(doc->segs[j].text) < sizeof(combined) - 2)
                    strcat(combined, doc->segs[j].text);
            }
            bounds[nb].word_count = wc;
            double dur = bounds[nb].time_end - bounds[nb].time_start;
            bounds[nb].word_density = dur > 0 ? wc / dur : 0;
            bounds[nb].type = classify_segment(combined, nb, -1); /* total unknown yet */
            nb++;

            /* Start new boundary */
            bounds[nb].seg_start = i;
            bounds[nb].time_start = doc->segs[i].start;
            bounds[nb].silence_before = gap;
        }
    }

    /* Close final boundary */
    bounds[nb].seg_end = doc->count - 1;
    bounds[nb].time_end = doc->segs[doc->count - 1].end;
    int wc = 0;
    char combined[MAX_TEXT * 10] = {0};
    for (int j = bounds[nb].seg_start; j <= bounds[nb].seg_end; j++) {
        wc += count_words(doc->segs[j].text);
        if (strlen(combined) + strlen(doc->segs[j].text) < sizeof(combined) - 2)
            strcat(combined, doc->segs[j].text);
    }
    bounds[nb].word_count = wc;
    double dur = bounds[nb].time_end - bounds[nb].time_start;
    bounds[nb].word_density = dur > 0 ? wc / dur : 0;
    nb++;

    /* Re-classify with known total */
    for (int i = 0; i < nb; i++) {
        char combined2[MAX_TEXT * 10] = {0};
        for (int j = bounds[i].seg_start; j <= bounds[i].seg_end; j++) {
            if (strlen(combined2) + strlen(doc->segs[j].text) < sizeof(combined2) - 2)
                strcat(combined2, doc->segs[j].text);
        }
        bounds[i].type = classify_segment(combined2, i, nb);
    }

    return nb;
}

/* ── JSON escape helper ───────────────────────────────────────────── */

static void fprint_json_str(FILE *fp, const char *s) {
    fputc('"', fp);
    for (; *s; s++) {
        switch (*s) {
            case '"':  fputs("\\\"", fp); break;
            case '\\': fputs("\\\\", fp); break;
            case '\n': fputs("\\n", fp); break;
            case '\r': fputs("\\r", fp); break;
            case '\t': fputs("\\t", fp); break;
            default:
                if ((unsigned char)*s < 0x20)
                    fprintf(fp, "\\u%04x", (unsigned char)*s);
                else
                    fputc(*s, fp);
        }
    }
    fputc('"', fp);
}

/* ── Output: Segment Graph ────────────────────────────────────────── */

static int emit_graph(const WhisperDoc *doc, const IdeaBoundary *bounds,
                      int nb, const char *out_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/segment-graph.json", out_dir);
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    fprintf(fp, "{\n");
    fprintf(fp, "  \"source_system\": \"BonfyreSegment\",\n");
    fprintf(fp, "  \"created_at\": \"%s\",\n", ts);
    fprintf(fp, "  \"total_segments\": %d,\n", doc->count);
    fprintf(fp, "  \"total_boundaries\": %d,\n", nb);
    fprintf(fp, "  \"nodes\": [\n");

    for (int i = 0; i < nb; i++) {
        /* Build combined text for this node */
        char text[MAX_TEXT * 4] = {0};
        for (int j = bounds[i].seg_start; j <= bounds[i].seg_end; j++) {
            const char *t = doc->segs[j].text;
            while (*t == ' ') t++;
            if (strlen(text) + strlen(t) < sizeof(text) - 2) {
                if (text[0]) strcat(text, " ");
                strcat(text, t);
            }
        }

        fprintf(fp, "    {\n");
        fprintf(fp, "      \"id\": %d,\n", i);
        fprintf(fp, "      \"type\": \"%s\",\n", seg_type_str(bounds[i].type));
        fprintf(fp, "      \"start\": %.2f,\n", bounds[i].time_start);
        fprintf(fp, "      \"end\": %.2f,\n", bounds[i].time_end);
        fprintf(fp, "      \"duration\": %.2f,\n", bounds[i].time_end - bounds[i].time_start);
        fprintf(fp, "      \"word_count\": %d,\n", bounds[i].word_count);
        fprintf(fp, "      \"word_density\": %.1f,\n", bounds[i].word_density);
        fprintf(fp, "      \"silence_before\": %.2f,\n", bounds[i].silence_before);
        fprintf(fp, "      \"text\": ");
        fprint_json_str(fp, text);
        fprintf(fp, "\n");
        fprintf(fp, "    }%s\n", i < nb - 1 ? "," : "");
    }

    fprintf(fp, "  ],\n");
    fprintf(fp, "  \"edges\": [\n");

    for (int i = 0; i < nb - 1; i++) {
        const char *rel = "follows";
        if (bounds[i+1].type == SEG_EXAMPLE && bounds[i].type == SEG_IDEA)
            rel = "illustrates";
        else if (bounds[i+1].type == SEG_CTA)
            rel = "concludes";
        else if (bounds[i+1].type == SEG_TRANSITION)
            rel = "transitions";
        else if (bounds[i+1].silence_before > 3.0)
            rel = "topic-shift";

        fprintf(fp, "    {\"from\": %d, \"to\": %d, \"relation\": \"%s\"}%s\n",
                i, i + 1, rel, i < nb - 2 ? "," : "");
    }

    fprintf(fp, "  ]\n");
    fprintf(fp, "}\n");

    fclose(fp);
    return 0;
}

/* ── Output: Boundaries ───────────────────────────────────────────── */

static int emit_boundaries(const WhisperDoc *doc, const IdeaBoundary *bounds,
                           int nb, const char *out_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/boundaries.json", out_dir);
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    fprintf(fp, "{\n");
    fprintf(fp, "  \"source_system\": \"BonfyreSegment\",\n");
    fprintf(fp, "  \"created_at\": \"%s\",\n", ts);
    fprintf(fp, "  \"boundaries\": [\n");

    for (int i = 0; i < nb; i++) {
        char label[128];
        snprintf(label, sizeof(label), "Segment %d: %s", i + 1, seg_type_str(bounds[i].type));

        fprintf(fp, "    {\n");
        fprintf(fp, "      \"index\": %d,\n", i + 1);
        fprintf(fp, "      \"label\": \"%s\",\n", label);
        fprintf(fp, "      \"type\": \"%s\",\n", seg_type_str(bounds[i].type));
        fprintf(fp, "      \"start\": %.2f,\n", bounds[i].time_start);
        fprintf(fp, "      \"end\": %.2f,\n", bounds[i].time_end);
        fprintf(fp, "      \"duration\": %.2f,\n", bounds[i].time_end - bounds[i].time_start);
        fprintf(fp, "      \"segments\": [%d, %d]\n", bounds[i].seg_start, bounds[i].seg_end);
        fprintf(fp, "    }%s\n", i < nb - 1 ? "," : "");
    }

    fprintf(fp, "  ]\n");
    fprintf(fp, "}\n");

    fclose(fp);

    (void)doc;
    return 0;
}

/* ── Output: Speech Rhythm ────────────────────────────────────────── */

static int emit_rhythm(const WhisperDoc *doc, const char *out_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/rhythm.json", out_dir);
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    /* Compute per-segment rhythm stats */
    double total_speech = 0, total_silence = 0;
    int total_words = 0;

    fprintf(fp, "{\n");
    fprintf(fp, "  \"source_system\": \"BonfyreSegment\",\n");
    fprintf(fp, "  \"created_at\": \"%s\",\n", ts);
    fprintf(fp, "  \"segments\": [\n");

    for (int i = 0; i < doc->count; i++) {
        double dur = doc->segs[i].end - doc->segs[i].start;
        int wc = count_words(doc->segs[i].text);
        double density = dur > 0 ? wc / dur : 0;
        double gap = (i > 0) ? doc->segs[i].start - doc->segs[i-1].end : 0;
        if (gap < 0) gap = 0;

        total_speech += dur;
        total_silence += gap;
        total_words += wc;

        fprintf(fp, "    {\n");
        fprintf(fp, "      \"index\": %d,\n", i);
        fprintf(fp, "      \"start\": %.2f,\n", doc->segs[i].start);
        fprintf(fp, "      \"end\": %.2f,\n", doc->segs[i].end);
        fprintf(fp, "      \"duration\": %.2f,\n", dur);
        fprintf(fp, "      \"silence_before\": %.2f,\n", gap);
        fprintf(fp, "      \"word_count\": %d,\n", wc);
        fprintf(fp, "      \"words_per_sec\": %.1f\n", density);
        fprintf(fp, "    }%s\n", i < doc->count - 1 ? "," : "");
    }

    double total_dur = doc->count > 0 ?
        doc->segs[doc->count - 1].end - doc->segs[0].start : 0;

    fprintf(fp, "  ],\n");
    fprintf(fp, "  \"summary\": {\n");
    fprintf(fp, "    \"total_duration\": %.2f,\n", total_dur);
    fprintf(fp, "    \"total_speech\": %.2f,\n", total_speech);
    fprintf(fp, "    \"total_silence\": %.2f,\n", total_silence);
    fprintf(fp, "    \"speech_ratio\": %.3f,\n", total_dur > 0 ? total_speech / total_dur : 0);
    fprintf(fp, "    \"total_words\": %d,\n", total_words);
    fprintf(fp, "    \"avg_words_per_sec\": %.1f,\n", total_speech > 0 ? total_words / total_speech : 0);
    fprintf(fp, "    \"avg_segment_duration\": %.2f\n", doc->count > 0 ? total_speech / doc->count : 0);
    fprintf(fp, "  }\n");
    fprintf(fp, "}\n");

    fclose(fp);
    return 0;
}

/* ── Main ─────────────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-segment — idea boundary detection + segment graph\n\n"
        "Usage:\n"
        "  bonfyre-segment graph      <whisper-json> <out-dir> [--silence-gap SEC]\n"
        "  bonfyre-segment boundaries <whisper-json> <out-dir> [--silence-gap SEC]\n"
        "  bonfyre-segment rhythm     <whisper-json> <out-dir>\n"
        "  bonfyre-segment all        <whisper-json> <out-dir> [--silence-gap SEC]\n\n"
        "Input: Whisper JSON with segments[].start/end/text\n"
        "       (whisper audio.wav --output_format json)\n");
}

int main(int argc, char **argv) {
    if (argc < 4) { print_usage(); return 1; }

    const char *mode = argv[1];
    const char *json_path = argv[2];
    const char *out_dir = argv[3];
    double silence_gap = 2.0;

    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "--silence-gap") == 0 && i + 1 < argc) {
            silence_gap = strtod(argv[++i], NULL);
        }
    }

    char *json = read_file_contents(json_path);
    if (!json) { fprintf(stderr, "error: cannot read %s\n", json_path); return 1; }

    WhisperDoc doc;
    if (parse_whisper_json(json, &doc) != 0) { free(json); return 1; }
    free(json);

    if (doc.count == 0) {
        fprintf(stderr, "error: no segments found in whisper JSON\n");
        return 1;
    }

    if (ensure_dir(out_dir) != 0) {
        fprintf(stderr, "error: cannot create %s\n", out_dir);
        return 1;
    }

    IdeaBoundary bounds[MAX_BOUNDARIES];
    int nb = detect_boundaries(&doc, silence_gap, bounds);

    int rc = 0;

    if (strcmp(mode, "graph") == 0 || strcmp(mode, "all") == 0) {
        rc |= emit_graph(&doc, bounds, nb, out_dir);
    }
    if (strcmp(mode, "boundaries") == 0 || strcmp(mode, "all") == 0) {
        rc |= emit_boundaries(&doc, bounds, nb, out_dir);
    }
    if (strcmp(mode, "rhythm") == 0 || strcmp(mode, "all") == 0) {
        rc |= emit_rhythm(&doc, out_dir);
    }

    if (strcmp(mode, "graph") != 0 && strcmp(mode, "boundaries") != 0 &&
        strcmp(mode, "rhythm") != 0 && strcmp(mode, "all") != 0) {
        fprintf(stderr, "error: unknown mode '%s'\n", mode);
        print_usage();
        return 1;
    }

    if (rc == 0) {
        printf("Segmented: %d whisper segments → %d idea boundaries (%s)\n",
               doc.count, nb, mode);
    }
    return rc;
}
