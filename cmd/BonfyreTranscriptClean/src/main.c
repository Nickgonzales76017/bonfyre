#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <bonfyre.h>

typedef struct {
    int filler_tokens_removed;
    int hallucinations_removed;
    int repeated_words_removed;
    int chunk_headers_removed;
} CleanStats;

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} TextBuffer;

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
static char *read_file(const char *path, size_t *size_out) {
    return bf_read_file(path, size_out);
}

static int write_text_file(const char *path, const char *text) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    if (fputs(text, out) == EOF) {
        fclose(out);
        return 1;
    }
    if (fputc('\n', out) == EOF) {
        fclose(out);
        return 1;
    }
    fclose(out);
    return 0;
}

static int textbuf_reserve(TextBuffer *buf, size_t extra) {
    size_t need = buf->len + extra + 1;
    if (need <= buf->cap) return 0;
    size_t next = buf->cap ? buf->cap * 2 : 1024;
    while (next < need) next *= 2;
    char *grown = realloc(buf->data, next);
    if (!grown) return 1;
    buf->data = grown;
    buf->cap = next;
    return 0;
}

static int textbuf_append_char(TextBuffer *buf, char ch) {
    if (textbuf_reserve(buf, 1) != 0) return 1;
    buf->data[buf->len++] = ch;
    buf->data[buf->len] = '\0';
    return 0;
}

static int textbuf_append_json_string(TextBuffer *buf, const char **cursor) {
    const char *p = *cursor;
    if (*p != '"') return 1;
    p++;
    while (*p && *p != '"') {
        if (*p == '\\') {
            p++;
            if (!*p) break;
            switch (*p) {
                case 'n': if (textbuf_append_char(buf, ' ') != 0) return 1; break;
                case 'r': break;
                case 't': if (textbuf_append_char(buf, ' ') != 0) return 1; break;
                case '"': if (textbuf_append_char(buf, '"') != 0) return 1; break;
                case '\\': if (textbuf_append_char(buf, '\\') != 0) return 1; break;
                case '/': if (textbuf_append_char(buf, '/') != 0) return 1; break;
                case 'u':
                    for (int i = 0; i < 4 && p[1]; i++) p++;
                    break;
                default:
                    if (textbuf_append_char(buf, *p) != 0) return 1;
                    break;
            }
            p++;
            continue;
        }
        if (textbuf_append_char(buf, *p) != 0) return 1;
        p++;
    }
    if (*p == '"') p++;
    *cursor = p;
    return 0;
}

/* Parse a numeric value after a JSON key within a bounded segment region */
static double json_num(const char *start, const char *end, const char *key) {
    size_t klen = strlen(key);
    for (const char *p = start; p + klen < end; p++) {
        if (strncmp(p, key, klen) == 0) {
            const char *v = p + klen;
            while (v < end && (*v == ' ' || *v == ':' || *v == '\t')) v++;
            if (v < end) return strtod(v, NULL);
        }
    }
    return -999.0;
}

static char *extract_segment_text_json(const char *json) {
    if (!json) return NULL;
    const char *segments = strstr(json, "\"segments\"");
    if (!segments) return NULL;
    const char *arr = strchr(segments, '[');
    if (!arr) return NULL;

    TextBuffer buf = {0};
    int found = 0;
    int skipped = 0;
    const char *p = arr + 1;

    while (*p) {
        while (*p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r' || *p == ','))
            p++;
        if (*p == ']') break;
        if (*p != '{') { p++; continue; }

        /* Find matching closing brace for this segment object */
        const char *seg_start = p;
        int depth = 1;
        p++;
        while (*p && depth > 0) {
            if (*p == '{') depth++;
            else if (*p == '}') depth--;
            p++;
        }
        const char *seg_end = p;

        /* Check quality fields */
        double confidence   = json_num(seg_start, seg_end, "\"confidence\"");
        double no_speech    = json_num(seg_start, seg_end, "\"no_speech\"");
        double hall_flags   = json_num(seg_start, seg_end, "\"hallucination_flags\"");
        double hcp_quality  = json_num(seg_start, seg_end, "\"hcp_quality\"");

        /* Filter low-quality / hallucinated segments.
         * If hcp_quality is available (from hcp-whisper), use it as primary signal.
         * Otherwise fall back to raw confidence + hallucination_flags. */
        if (no_speech > 0.5)                            { skipped++; continue; }
        if (hall_flags > 0)                             { skipped++; continue; }
        if (hcp_quality > -900 && hcp_quality < 0.3)    { skipped++; continue; }
        if (confidence > -900 && confidence < 0.3)      { skipped++; continue; }

        /* Find "text" field within this segment */
        const char *text_key = NULL;
        for (const char *s = seg_start; s + 6 < seg_end; s++) {
            if (strncmp(s, "\"text\"", 6) == 0) { text_key = s; break; }
        }
        if (!text_key) continue;

        const char *cursor = text_key + 6;
        while (cursor < seg_end && isspace((unsigned char)*cursor)) cursor++;
        if (cursor >= seg_end || *cursor != ':') continue;
        cursor++;
        while (cursor < seg_end && isspace((unsigned char)*cursor)) cursor++;
        if (cursor >= seg_end || *cursor != '"') continue;

        /* Add sentence boundary between segments when prior text lacks one */
        if (found && buf.len > 0) {
            char last = buf.data[buf.len - 1];
            if (last != '.' && last != '!' && last != '?') {
                textbuf_append_char(&buf, '.');
            }
            textbuf_append_char(&buf, ' ');
        }

        if (textbuf_append_json_string(&buf, &cursor) != 0) {
            free(buf.data);
            return NULL;
        }
        found = 1;
    }

    if (skipped > 0)
        fprintf(stderr, "[clean] Filtered %d low-quality segments\n", skipped);

    if (!found) {
        free(buf.data);
        return NULL;
    }
    return buf.data;
}

static int write_status_file(const char *path,
                             const char *transcript_path,
                             const char *cleaned_path,
                             int changed,
                             const CleanStats *stats) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out,
            "{\n"
            "  \"sourceSystem\": \"BonfyreTranscriptClean\",\n"
            "  \"status\": \"completed\",\n"
            "  \"transcriptPath\": \"%s\",\n"
            "  \"cleanedPath\": \"%s\",\n"
            "  \"changed\": %s,\n"
            "  \"fillerTokensRemoved\": %d,\n"
            "  \"hallucinationsRemoved\": %d,\n"
            "  \"repeatedWordsRemoved\": %d,\n"
            "  \"chunkHeadersRemoved\": %d\n"
            "}\n",
            transcript_path,
            cleaned_path,
            changed ? "true" : "false",
            stats->filler_tokens_removed,
            stats->hallucinations_removed,
            stats->repeated_words_removed,
            stats->chunk_headers_removed);
    fclose(out);
    return 0;
}

static int starts_with_chunk_header(const char *line) {
    const char *p = line;
    while (*p && isspace((unsigned char)*p)) p++;
    if (strncmp(p, "##", 2) != 0) return 0;
    p += 2;
    while (*p && isspace((unsigned char)*p)) p++;
    if (strncasecmp(p, "chunk", 5) != 0) return 0;
    p += 5;
    while (*p && isspace((unsigned char)*p)) p++;
    return isdigit((unsigned char)*p);
}

static int is_filler_token(const char *token) {
    static const char *fillers[] = {
        "um", "uh", "erm", "ah", "hmm", "hm", "mm", "mhm", "uh-huh",
        "uhh", "umm", "uhm", "ugh", "huh", "eh", "er",
        "mmm", "mmhmm", "uhhh", "ummm", "hmph", "heh",
        NULL
    };
    for (int i = 0; fillers[i]; i++) {
        if (strcasecmp(token, fillers[i]) == 0) return 1;
    }
    return 0;
}

static char *find_case_insensitive(char *haystack, const char *needle) {
    size_t needle_len = strlen(needle);
    if (needle_len == 0) return haystack;
    for (char *p = haystack; *p; p++) {
        if (strncasecmp(p, needle, needle_len) == 0) return p;
    }
    return NULL;
}

static void append_char(char **buffer, size_t *len, size_t *cap, char c) {
    if (*len + 2 >= *cap) {
        size_t new_cap = (*cap == 0) ? 1024 : (*cap * 2);
        char *next = realloc(*buffer, new_cap);
        if (!next) return;
        *buffer = next;
        *cap = new_cap;
    }
    (*buffer)[(*len)++] = c;
    (*buffer)[*len] = '\0';
}

static int is_token_char(unsigned char c) {
    return isalnum(c) || c == '\'' || c == '-';
}

static char *remove_chunk_headers(const char *text, CleanStats *stats) {
    char *copy = strdup(text);
    char *out = NULL;
    char *line;
    char *saveptr = NULL;
    size_t len = 0;
    size_t cap = 0;

    if (!copy) return NULL;
    out = calloc(1, 1);
    if (!out) {
        free(copy);
        return NULL;
    }

    line = strtok_r(copy, "\n", &saveptr);
    while (line) {
        if (!starts_with_chunk_header(line)) {
            for (size_t i = 0; line[i]; i++) append_char(&out, &len, &cap, line[i]);
            append_char(&out, &len, &cap, '\n');
        } else {
            stats->chunk_headers_removed++;
        }
        line = strtok_r(NULL, "\n", &saveptr);
    }

    free(copy);
    return out;
}

static char *remove_fillers_and_repeat_words(const char *text, CleanStats *stats) {
    char *out = calloc(1, strlen(text) + 2);
    char last_token[256] = {0};
    int last_count = 0;
    size_t out_len = 0;

    if (!out) return NULL;

    for (size_t i = 0; text[i]; ) {
        if (is_token_char((unsigned char)text[i])) {
            char token[256];
            size_t tlen = 0;
            size_t start = i;
            while (text[i] && is_token_char((unsigned char)text[i])) {
                if (tlen + 1 < sizeof(token)) token[tlen++] = text[i];
                i++;
            }
            token[tlen] = '\0';

            if (is_filler_token(token)) {
                stats->filler_tokens_removed++;
                continue;
            }

            if (last_token[0] != '\0' && strcasecmp(token, last_token) == 0) {
                last_count++;
                if (last_count >= 2) {
                    stats->repeated_words_removed++;
                    continue;
                }
            } else {
                strncpy(last_token, token, sizeof(last_token) - 1);
                last_token[sizeof(last_token) - 1] = '\0';
                last_count = 1;
            }

            if (out_len > 0 && !isspace((unsigned char)out[out_len - 1]) &&
                out[out_len - 1] != '\n' && out[out_len - 1] != '(' &&
                out[out_len - 1] != '/' && start > 0) {
                out[out_len++] = ' ';
            }
            memcpy(out + out_len, token, tlen);
            out_len += tlen;
            out[out_len] = '\0';
        } else {
            if (ispunct((unsigned char)text[i]) && !isspace((unsigned char)text[i])) {
                if (out_len > 0 && out[out_len - 1] == ' ') out_len--;
                out[out_len++] = text[i++];
                out[out_len] = '\0';
                while (text[i] == out[out_len - 1] &&
                       strchr(".,!?", out[out_len - 1]) != NULL) {
                    stats->hallucinations_removed++;
                    i++;
                }
            } else {
                out[out_len++] = text[i++];
                out[out_len] = '\0';
            }
            if (i > 0 && ispunct((unsigned char)text[i - 1]) &&
                !is_token_char((unsigned char)text[i - 1])) {
                last_token[0] = '\0';
                last_count = 0;
            }
        }
    }

    return out;
}

static char *remove_multiword_fillers(const char *text, CleanStats *stats) {
    static const char *fillers[] = {
        "you know what I mean", "you know what",
        "or something like that", "or whatever",
        "and stuff like that", "and stuff",
        "and things like that", "and everything",
        "if that makes sense", "does that make sense",
        "at the end of the day", "if you will",
        NULL
    };
    char *out = strdup(text);
    if (!out) return NULL;
    for (int i = 0; fillers[i]; i++) {
        size_t plen = strlen(fillers[i]);
        char *hit;
        while ((hit = find_case_insensitive(out, fillers[i])) != NULL) {
            memmove(hit, hit + plen, strlen(hit + plen) + 1);
            stats->filler_tokens_removed++;
        }
    }
    return out;
}

static char *remove_fixed_hallucinations(const char *text, CleanStats *stats) {
    const char *patterns[] = {
        "thank you thank you thank you",
        "thanks for watching thanks for watching",
        "please subscribe please subscribe",
        "thanks for watching. thanks for watching.",
        "thank you. thank you. thank you.",
        NULL
    };
    char *out = strdup(text);
    if (!out) return NULL;

    for (int i = 0; patterns[i]; i++) {
        size_t plen = strlen(patterns[i]);
        char *hit = NULL;
        while ((hit = find_case_insensitive(out, patterns[i])) != NULL) {
            memmove(hit, hit + plen, strlen(hit + plen) + 1);
            stats->hallucinations_removed++;
        }
    }

    return out;
}

static char *normalize_spacing(const char *text) {
    char *out = calloc(1, strlen(text) + 3);
    int pending_space = 0;
    size_t len = 0;
    if (!out) return NULL;

    for (size_t i = 0; text[i]; i++) {
        unsigned char c = (unsigned char)text[i];
        if (isspace(c)) {
            pending_space = 1;
            continue;
        }
        if (strchr(".,!?", c)) {
            if (len == 0) {
                pending_space = 0;
                continue;
            }
            if (len > 0 && out[len - 1] == ' ') len--;
            if (len > 0 && out[len - 1] == (char)c) continue;
            out[len++] = (char)c;
            pending_space = 1;
            continue;
        }
        if (pending_space && len > 0) out[len++] = ' ';
        out[len++] = (char)c;
        pending_space = 0;
    }

    while (len > 0 && isspace((unsigned char)out[len - 1])) len--;
    if (len > 0 && !strchr(".!?", out[len - 1])) out[len++] = '.';
    out[len] = '\0';
    return out;
}

static char *final_cleanup_pass(const char *text, CleanStats *stats) {
    char *copy = strdup(text);
    char *out = calloc(1, strlen(text) + 3);
    char *token;
    char *saveptr = NULL;
    char last[256] = {0};
    int last_count = 0;
    size_t len = 0;

    if (!copy || !out) {
        free(copy);
        free(out);
        return NULL;
    }

    token = strtok_r(copy, " ", &saveptr);
    while (token) {
        char cleaned[256];
        size_t start = 0;
        size_t tlen;

        while (token[start] && strchr(".,!?", token[start]) != NULL) start++;
        strncpy(cleaned, token + start, sizeof(cleaned) - 1);
        cleaned[sizeof(cleaned) - 1] = '\0';
        tlen = strlen(cleaned);
        while (tlen > 0 && strchr(".,!?", cleaned[tlen - 1]) != NULL &&
               tlen > 1 && isalpha((unsigned char)cleaned[tlen - 2])) {
            break;
        }

        if (cleaned[0] == '\0') {
            token = strtok_r(NULL, " ", &saveptr);
            continue;
        }

        if (last[0] != '\0' && strcasecmp(cleaned, last) == 0) {
            last_count++;
            if (last_count > 2) {
                stats->repeated_words_removed++;
                token = strtok_r(NULL, " ", &saveptr);
                continue;
            }
        } else {
            strncpy(last, cleaned, sizeof(last) - 1);
            last[sizeof(last) - 1] = '\0';
            last_count = 1;
        }

        if (len > 0) out[len++] = ' ';
        memcpy(out + len, cleaned, strlen(cleaned));
        len += strlen(cleaned);
        out[len] = '\0';
        token = strtok_r(NULL, " ", &saveptr);
    }

    while (len > 0 && strchr(".,!?", out[0]) != NULL) {
        memmove(out, out + 1, len);
        len--;
    }
    while (len > 0 && isspace((unsigned char)out[0])) {
        memmove(out, out + 1, len);
        len--;
    }
    if (len > 0 && !strchr(".!?", out[len - 1])) out[len++] = '.';
    out[len] = '\0';

    free(copy);
    return out;
}

static char *capitalize_sentences(const char *text) {
    char *out = strdup(text);
    if (!out) return NULL;
    int cap_next = 1;
    for (size_t i = 0; out[i]; i++) {
        if (cap_next && isalpha((unsigned char)out[i])) {
            out[i] = (char)toupper((unsigned char)out[i]);
            cap_next = 0;
        }
        if (out[i] == '.' || out[i] == '!' || out[i] == '?')
            cap_next = 1;
    }
    return out;
}

static void usage(void) {
    fprintf(stderr,
            "Usage: bonfyre-transcript-clean --transcript <path> --out <path> "
            "[--status-out <path>] [--dry-run]\n");
}

int main(int argc, char **argv) {
    const char *transcript_path = NULL;
    const char *out_path = NULL;
    const char *status_path = NULL;
    int dry_run = 0;
    size_t raw_size = 0;
    char *raw_text = NULL;
    char *pass1 = NULL;
    char *pass2 = NULL;
    char *pass3 = NULL;
    char *cleaned = NULL;
    CleanStats stats = {0};
    int changed;
    char status_default[PATH_MAX];
    char out_dir[PATH_MAX];

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--transcript") == 0 && i + 1 < argc) {
            transcript_path = argv[++i];
        } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
            out_path = argv[++i];
        } else if (strcmp(argv[i], "--status-out") == 0 && i + 1 < argc) {
            status_path = argv[++i];
        } else if (strcmp(argv[i], "--dry-run") == 0) {
            dry_run = 1;
        } else {
            usage();
            return 1;
        }
    }

    if (!transcript_path || !out_path) {
        usage();
        return 1;
    }

    if (dry_run) {
        printf("Would clean transcript: %s -> %s\n", transcript_path, out_path);
        return 0;
    }

    raw_text = read_file(transcript_path, &raw_size);
    if (!raw_text) {
        fprintf(stderr, "Missing transcript file: %s\n", transcript_path);
        return 2;
    }

    char *working_text = raw_text;
    char *json_text = NULL;
    if (raw_text[0] == '{' || raw_text[0] == '[') {
        json_text = extract_segment_text_json(raw_text);
        if (json_text) working_text = json_text;
    }

    pass1 = remove_chunk_headers(working_text, &stats);
    pass2 = pass1 ? remove_fillers_and_repeat_words(pass1, &stats) : NULL;
    char *pass2b = pass2 ? remove_multiword_fillers(pass2, &stats) : NULL;
    pass3 = pass2b ? remove_fixed_hallucinations(pass2b, &stats) : NULL;
    cleaned = pass3 ? normalize_spacing(pass3) : NULL;
    char *final = cleaned ? final_cleanup_pass(cleaned, &stats) : NULL;
    char *capitalized = final ? capitalize_sentences(final) : NULL;
    if (!capitalized) {
        fprintf(stderr, "Failed to clean transcript\n");
        free(raw_text);
        free(pass1);
        free(pass2);
        free(pass2b);
        free(pass3);
        free(cleaned);
        free(final);
        return 1;
    }

    strncpy(out_dir, out_path, sizeof(out_dir) - 1);
    out_dir[sizeof(out_dir) - 1] = '\0';
    char *slash = strrchr(out_dir, '/');
    if (slash) {
        *slash = '\0';
        if (out_dir[0] != '\0' && ensure_dir(out_dir) != 0) {
            fprintf(stderr, "Failed to create output dir: %s\n", out_dir);
            free(raw_text); free(pass1); free(pass2); free(pass3); free(cleaned);
            return 1;
        }
    }

    if (!status_path) {
        strncpy(status_default, out_path, sizeof(status_default) - 1);
        status_default[sizeof(status_default) - 1] = '\0';
        slash = strrchr(status_default, '/');
        if (slash) {
            strcpy(slash + 1, "status.json");
        } else {
            strcpy(status_default, "status.json");
        }
        status_path = status_default;
    }

    changed = strcmp(working_text, capitalized) != 0;
    if (write_text_file(out_path, capitalized) != 0 ||
        write_status_file(status_path, transcript_path, out_path, changed, &stats) != 0) {
        fprintf(stderr, "Failed to write output artifacts\n");
        free(raw_text); free(json_text); free(pass1); free(pass2); free(pass2b); free(pass3); free(cleaned); free(final); free(capitalized);
        return 1;
    }

    printf("Wrote cleaned transcript to %s\n", out_path);

    free(raw_text);
    free(json_text);
    free(pass1);
    free(pass2);
    free(pass2b);
    free(pass3);
    free(cleaned);
    free(final);
    free(capitalized);
    return 0;
}
