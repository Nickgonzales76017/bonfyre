/*
 * BonfyreClips — Clip Discovery Engine
 *
 * Layer: Transform (pure — same input, same output)
 *
 * Takes Whisper JSON output and auto-detects the best clip candidates
 * by scoring segments on: speech density, statement strength, emphasis
 * (pauses before/after), and content quality keywords.
 *
 * Usage:
 *   bonfyre-clips discover  <whisper-json> <out-dir> [--top N] [--min-dur SEC] [--max-dur SEC]
 *   bonfyre-clips timestamps <whisper-json> <out-dir> [--top N]
 *
 * Input: Whisper JSON (--output_format json) with segments[].start/end/text
 * Output: clips.json (ranked clip candidates with timestamps + scores)
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

/* ── Whisper JSON parser (same as BonfyreSegment) ─────────────────── */

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
    if (!seg_arr) { fprintf(stderr, "error: no segments array\n"); return 1; }
    const char *p = seg_arr;
    while (*p && *p != '[') p++;
    if (!*p) return 1;
    p++;

    while (*p && doc->count < MAX_SEGMENTS) {
        while (*p && *p != '{' && *p != ']') p++;
        if (!*p || *p == ']') break;
        p++;

        WhisperSeg *s = &doc->segs[doc->count];
        s->start = 0; s->end = 0; s->text[0] = '\0';

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

                if (strcmp(key, "start") == 0) s->start = parse_number(&p);
                else if (strcmp(key, "end") == 0) s->end = parse_number(&p);
                else if (strcmp(key, "text") == 0) parse_string(&p, s->text, sizeof(s->text));
                else {
                    while (*p == ' ' || *p == ':') p++;
                    if (*p == '"') { p++; while (*p && *p != '"') { if (*p == '\\') p++; p++; } if (*p == '"') p++; }
                    else if (*p == '[') { int d = 1; p++; while (*p && d > 0) { if (*p == '[') d++; else if (*p == ']') d--; p++; } }
                }
            } else p++;
        }
        if (s->text[0] != '\0') doc->count++;
    }
    return 0;
}

/* ── Clip candidate scoring ───────────────────────────────────────── */

typedef struct {
    int    seg_start;
    int    seg_end;
    double time_start;
    double time_end;
    double duration;
    double score;
    char   text[MAX_TEXT * 4];
    char   reason[256];
} ClipCandidate;

#define MAX_CLIPS 256

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

static int count_words(const char *text) {
    int count = 0, in_word = 0;
    for (const char *p = text; *p; p++) {
        if (isspace((unsigned char)*p)) in_word = 0;
        else if (!in_word) { in_word = 1; count++; }
    }
    return count;
}

static double score_clip(const WhisperDoc *doc, int start, int end,
                         char *reason, size_t rsz) {
    double score = 0;
    reason[0] = '\0';

    /* Build combined text */
    char text[MAX_TEXT * 4] = {0};
    for (int i = start; i <= end; i++) {
        const char *t = doc->segs[i].text;
        while (*t == ' ') t++;
        if (strlen(text) + strlen(t) < sizeof(text) - 2) {
            if (text[0]) strcat(text, " ");
            strcat(text, t);
        }
    }

    double duration = doc->segs[end].end - doc->segs[start].start;
    int wc = count_words(text);
    double density = duration > 0 ? wc / duration : 0;

    /* Speech density bonus (2-4 words/sec is good pacing) */
    if (density >= 2.0 && density <= 4.0) {
        score += 20;
        strncat(reason, "good-pacing ", rsz - strlen(reason) - 1);
    } else if (density > 4.0) {
        score += 10; /* Fast but engaging */
    }

    /* Strong statement keywords */
    static const char *strong[] = {
        "the key", "the problem", "the truth", "the biggest",
        "here's what", "here's the thing", "the real", "the secret",
        "most people", "nobody talks about", "what I learned",
        "the mistake", "the difference", "that's why", "think about",
        NULL
    };
    for (int i = 0; strong[i]; i++) {
        if (str_contains_ci(text, strong[i])) {
            score += 15;
            strncat(reason, "strong-statement ", rsz - strlen(reason) - 1);
            break;
        }
    }

    /* Actionable content */
    static const char *action[] = {
        "you should", "you need to", "stop", "start", "never", "always",
        "the best way", "the worst thing", "my advice", "do this",
        "don't", "focus on", "instead of", NULL
    };
    for (int i = 0; action[i]; i++) {
        if (str_contains_ci(text, action[i])) {
            score += 12;
            strncat(reason, "actionable ", rsz - strlen(reason) - 1);
            break;
        }
    }

    /* Numbers / specifics (concrete claims are clippable) */
    for (const char *c = text; *c; c++) {
        if (*c == '$' || (*c >= '0' && *c <= '9' && (c == text || !isalpha((unsigned char)*(c-1))))) {
            score += 8;
            strncat(reason, "specific ", rsz - strlen(reason) - 1);
            break;
        }
    }

    /* Emphasis bonus: pause before this segment */
    if (start > 0) {
        double gap = doc->segs[start].start - doc->segs[start - 1].end;
        if (gap >= 1.5) {
            score += 10;
            strncat(reason, "emphasis-pause ", rsz - strlen(reason) - 1);
        }
    }

    /* Length bonus: 15–60s is ideal clip length */
    if (duration >= 15 && duration <= 60) score += 10;
    else if (duration >= 8 && duration <= 90) score += 5;

    /* Word count — enough substance */
    if (wc >= 30 && wc <= 200) score += 5;

    return score;
}

static int clip_cmp(const void *a, const void *b) {
    double sa = ((const ClipCandidate *)a)->score;
    double sb = ((const ClipCandidate *)b)->score;
    if (sb > sa) return 1;
    if (sb < sa) return -1;
    return 0;
}

static void format_time(double secs, char *buf, size_t sz) {
    int m = (int)(secs / 60);
    double s = secs - m * 60;
    snprintf(buf, sz, "%d:%05.2f", m, s);
}

/* ── JSON escape ──────────────────────────────────────────────────── */

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
                else fputc(*s, fp);
        }
    }
    fputc('"', fp);
}

/* ── Main output ──────────────────────────────────────────────────── */

static int emit_clips(const WhisperDoc *doc, const char *out_dir,
                      int top_n, double min_dur, double max_dur) {
    /* Generate candidate clips using sliding windows of 1-5 segments */
    ClipCandidate clips[MAX_CLIPS];
    int nc = 0;

    for (int window = 1; window <= 5 && window <= doc->count; window++) {
        for (int i = 0; i <= doc->count - window && nc < MAX_CLIPS; i++) {
            int end = i + window - 1;
            double dur = doc->segs[end].end - doc->segs[i].start;
            if (dur < min_dur || dur > max_dur) continue;

            ClipCandidate *c = &clips[nc];
            c->seg_start = i;
            c->seg_end = end;
            c->time_start = doc->segs[i].start;
            c->time_end = doc->segs[end].end;
            c->duration = dur;
            c->score = score_clip(doc, i, end, c->reason, sizeof(c->reason));

            /* Build text */
            c->text[0] = '\0';
            for (int j = i; j <= end; j++) {
                const char *t = doc->segs[j].text;
                while (*t == ' ') t++;
                if (strlen(c->text) + strlen(t) < sizeof(c->text) - 2) {
                    if (c->text[0]) strcat(c->text, " ");
                    strcat(c->text, t);
                }
            }
            nc++;
        }
    }

    qsort(clips, (size_t)nc, sizeof(ClipCandidate), clip_cmp);

    /* Deduplicate overlapping clips — keep highest scored */
    int keep[MAX_CLIPS];
    memset(keep, 0, sizeof(keep));
    int kept = 0;
    for (int i = 0; i < nc && kept < top_n; i++) {
        int overlaps = 0;
        for (int j = 0; j < i; j++) {
            if (!keep[j]) continue;
            /* Check if segments overlap */
            if (clips[i].seg_start <= clips[j].seg_end &&
                clips[i].seg_end >= clips[j].seg_start) {
                overlaps = 1;
                break;
            }
        }
        if (!overlaps) {
            keep[i] = 1;
            kept++;
        }
    }

    /* Write output */
    char path[PATH_MAX];
    char ts[64];
    snprintf(path, sizeof(path), "%s/clips.json", out_dir);
    iso_timestamp(ts, sizeof(ts));

    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    fprintf(fp, "{\n");
    fprintf(fp, "  \"source_system\": \"BonfyreClips\",\n");
    fprintf(fp, "  \"created_at\": \"%s\",\n", ts);
    fprintf(fp, "  \"total_candidates\": %d,\n", nc);
    fprintf(fp, "  \"top_clips\": %d,\n", kept);
    fprintf(fp, "  \"clips\": [\n");

    int printed = 0;
    for (int i = 0; i < nc; i++) {
        if (!keep[i]) continue;
        ClipCandidate *c = &clips[i];
        char start_fmt[32], end_fmt[32];
        format_time(c->time_start, start_fmt, sizeof(start_fmt));
        format_time(c->time_end, end_fmt, sizeof(end_fmt));

        if (printed > 0) fprintf(fp, ",\n");
        fprintf(fp, "    {\n");
        fprintf(fp, "      \"rank\": %d,\n", printed + 1);
        fprintf(fp, "      \"score\": %.1f,\n", c->score);
        fprintf(fp, "      \"start\": %.2f,\n", c->time_start);
        fprintf(fp, "      \"end\": %.2f,\n", c->time_end);
        fprintf(fp, "      \"start_fmt\": \"%s\",\n", start_fmt);
        fprintf(fp, "      \"end_fmt\": \"%s\",\n", end_fmt);
        fprintf(fp, "      \"duration\": %.2f,\n", c->duration);
        fprintf(fp, "      \"reasons\": \"%s\",\n", c->reason);
        fprintf(fp, "      \"text\": ");
        fprint_json_str(fp, c->text);
        fprintf(fp, "\n");
        fprintf(fp, "    }");
        printed++;
    }

    fprintf(fp, "\n  ]\n");
    fprintf(fp, "}\n");

    fclose(fp);
    return 0;
}

/* ── Timestamp-only output (for ffmpeg clip extraction) ───────────── */

static int emit_timestamps(const WhisperDoc *doc, const char *out_dir,
                           int top_n, double min_dur, double max_dur) {
    /* Reuse clip discovery, then emit ffmpeg-ready timestamps */
    ClipCandidate clips[MAX_CLIPS];
    int nc = 0;

    for (int window = 1; window <= 5 && window <= doc->count; window++) {
        for (int i = 0; i <= doc->count - window && nc < MAX_CLIPS; i++) {
            int end = i + window - 1;
            double dur = doc->segs[end].end - doc->segs[i].start;
            if (dur < min_dur || dur > max_dur) continue;

            ClipCandidate *c = &clips[nc];
            c->seg_start = i;
            c->seg_end = end;
            c->time_start = doc->segs[i].start;
            c->time_end = doc->segs[end].end;
            c->duration = dur;
            c->score = score_clip(doc, i, end, c->reason, sizeof(c->reason));
            nc++;
        }
    }

    qsort(clips, (size_t)nc, sizeof(ClipCandidate), clip_cmp);

    int keep[MAX_CLIPS];
    memset(keep, 0, sizeof(keep));
    int kept = 0;
    for (int i = 0; i < nc && kept < top_n; i++) {
        int overlaps = 0;
        for (int j = 0; j < i; j++) {
            if (!keep[j]) continue;
            if (clips[i].seg_start <= clips[j].seg_end &&
                clips[i].seg_end >= clips[j].seg_start) { overlaps = 1; break; }
        }
        if (!overlaps) { keep[i] = 1; kept++; }
    }

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/clip-timestamps.txt", out_dir);
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    fprintf(fp, "# BonfyreClips — ffmpeg-ready timestamps\n");
    fprintf(fp, "# Usage: ffmpeg -i audio.wav -ss START -to END -c copy clip_N.wav\n\n");

    int n = 0;
    for (int i = 0; i < nc; i++) {
        if (!keep[i]) continue;
        n++;
        fprintf(fp, "# Clip %d (score: %.1f)\n", n, clips[i].score);
        fprintf(fp, "ffmpeg -i audio.wav -ss %.2f -to %.2f -c copy clip_%d.wav\n\n",
                clips[i].time_start, clips[i].time_end, n);
    }

    fclose(fp);
    return 0;
}

/* ── Main ─────────────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-clips — clip discovery engine\n\n"
        "Usage:\n"
        "  bonfyre-clips discover   <whisper-json> <out-dir> [--top N] [--min-dur S] [--max-dur S]\n"
        "  bonfyre-clips timestamps <whisper-json> <out-dir> [--top N] [--min-dur S] [--max-dur S]\n"
        "  bonfyre-clips all        <whisper-json> <out-dir> [--top N] [--min-dur S] [--max-dur S]\n\n"
        "Defaults: --top 5 --min-dur 8 --max-dur 90\n");
}

int main(int argc, char **argv) {
    if (argc < 4) { print_usage(); return 1; }

    const char *mode = argv[1];
    const char *json_path = argv[2];
    const char *out_dir = argv[3];
    int top_n = 5;
    double min_dur = 8.0;
    double max_dur = 90.0;

    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "--top") == 0 && i + 1 < argc) top_n = atoi(argv[++i]);
        else if (strcmp(argv[i], "--min-dur") == 0 && i + 1 < argc) min_dur = strtod(argv[++i], NULL);
        else if (strcmp(argv[i], "--max-dur") == 0 && i + 1 < argc) max_dur = strtod(argv[++i], NULL);
    }

    char *json = read_file_contents(json_path);
    if (!json) { fprintf(stderr, "error: cannot read %s\n", json_path); return 1; }

    WhisperDoc doc;
    if (parse_whisper_json(json, &doc) != 0) { free(json); return 1; }
    free(json);

    if (doc.count == 0) {
        fprintf(stderr, "error: no segments in whisper JSON\n");
        return 1;
    }

    if (ensure_dir(out_dir) != 0) return 1;

    int rc = 0;
    if (strcmp(mode, "discover") == 0 || strcmp(mode, "all") == 0)
        rc |= emit_clips(&doc, out_dir, top_n, min_dur, max_dur);
    if (strcmp(mode, "timestamps") == 0 || strcmp(mode, "all") == 0)
        rc |= emit_timestamps(&doc, out_dir, top_n, min_dur, max_dur);

    if (strcmp(mode, "discover") != 0 && strcmp(mode, "timestamps") != 0 &&
        strcmp(mode, "all") != 0) {
        fprintf(stderr, "error: unknown mode '%s'\n", mode);
        print_usage();
        return 1;
    }

    if (rc == 0) printf("Clips: %d segments analyzed → top %d candidates (%s)\n",
                        doc.count, top_n, mode);
    return rc;
}
