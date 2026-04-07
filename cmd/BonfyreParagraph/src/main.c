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

#define TARGET_SENTENCES 4
#define MAX_SENTENCES 7
#define MAX_SENTENCE_LEN 8192
#define MAX_SENTENCES_TOTAL 2048

typedef struct {
    char **items;
    size_t count;
    size_t capacity;
} StringList;

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
static char *read_file(const char *path) {
    FILE *in = fopen(path, "rb");
    char *buffer;
    long size;
    if (!in) return NULL;
    if (fseek(in, 0, SEEK_END) != 0) {
        fclose(in);
        return NULL;
    }
    size = ftell(in);
    if (size < 0) {
        fclose(in);
        return NULL;
    }
    rewind(in);
    buffer = malloc((size_t)size + 1);
    if (!buffer) {
        fclose(in);
        return NULL;
    }
    if (size > 0 && fread(buffer, 1, (size_t)size, in) != (size_t)size) {
        free(buffer);
        fclose(in);
        return NULL;
    }
    buffer[size] = '\0';
    fclose(in);
    return buffer;
}

static char *trim_copy(const char *src) {
    while (*src && isspace((unsigned char)*src)) src++;
    size_t len = strlen(src);
    while (len > 0 && isspace((unsigned char)src[len - 1])) len--;
    char *out = malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, src, len);
    out[len] = '\0';
    return out;
}

static int push_string(StringList *list, const char *value) {
    if (list->count == list->capacity) {
        size_t next_capacity = list->capacity == 0 ? 32 : list->capacity * 2;
        char **next_items = realloc(list->items, sizeof(char *) * next_capacity);
        if (!next_items) return 1;
        list->items = next_items;
        list->capacity = next_capacity;
    }
    list->items[list->count] = strdup(value);
    if (!list->items[list->count]) return 1;
    list->count++;
    return 0;
}

static void free_strings(StringList *list) {
    for (size_t i = 0; i < list->count; i++) free(list->items[i]);
    free(list->items);
}

static int split_sentences(const char *text, StringList *sentences) {
    const char *start = text;
    for (const char *p = text; ; p++) {
        if (*p == '.' || *p == '!' || *p == '?' || *p == '\0') {
            size_t len = (size_t)(p - start + (*p ? 1 : 0));
            if (len > 1 && len < MAX_SENTENCE_LEN) {
                char buffer[MAX_SENTENCE_LEN];
                memcpy(buffer, start, len);
                buffer[len] = '\0';
                char *trimmed = trim_copy(buffer);
                if (trimmed && trimmed[0]) {
                    if (push_string(sentences, trimmed) != 0) {
                        free(trimmed);
                        return 1;
                    }
                }
                free(trimmed);
            }
            if (*p == '\0') break;
            start = p + 1;
        }
    }
    return 0;
}

static int starts_with_transition(const char *sentence) {
    static const char *patterns[] = {
        "so ", "now ", "anyway", "moving on", "next", "also ", "another",
        "meanwhile", "however", "but ", "on the other hand",
        "let me", "let's", "i want to", "we should", "the next",
        "going back", "okay", "alright", "right", NULL
    };
    char lower[MAX_SENTENCE_LEN];
    size_t i = 0;
    while (sentence[i] && i + 1 < sizeof(lower)) {
        lower[i] = (char)tolower((unsigned char)sentence[i]);
        i++;
    }
    lower[i] = '\0';
    for (int j = 0; patterns[j]; j++) {
        if (strncmp(lower, patterns[j], strlen(patterns[j])) == 0) return 1;
    }
    return 0;
}

static char *join_range(StringList *sentences, size_t start, size_t end) {
    size_t total = 1;
    for (size_t i = start; i < end; i++) total += strlen(sentences->items[i]) + 1;
    char *out = calloc(total, 1);
    if (!out) return NULL;
    for (size_t i = start; i < end; i++) {
        strcat(out, sentences->items[i]);
        if (i + 1 < end) strcat(out, " ");
    }
    return out;
}

static char *header_from_paragraph(const char *paragraph, size_t index) {
    if (index == 0) return strdup("## Opening");
    char copy[MAX_SENTENCE_LEN];
    size_t i = 0;
    while (paragraph[i] && i + 1 < sizeof(copy)) {
        copy[i] = paragraph[i];
        i++;
    }
    copy[i] = '\0';

    char *token = strtok(copy, " ");
    char header[256] = {0};
    int words = 0;
    while (token && words < 5) {
        if (words > 0) strcat(header, " ");
        strcat(header, token);
        words++;
        token = strtok(NULL, " ");
    }
    size_t len = strlen(header);
    while (len > 0 && strchr(".,;:", header[len - 1])) {
        header[--len] = '\0';
    }
    char *out = malloc(len + 8);
    if (!out) return NULL;
    snprintf(out, len + 8, "## %s...", header);
    return out;
}

static int write_output(const char *path, StringList *paragraphs, int with_headers) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    for (size_t i = 0; i < paragraphs->count; i++) {
        if (with_headers) {
            char *header = header_from_paragraph(paragraphs->items[i], i);
            if (header) {
                fprintf(out, "%s\n\n", header);
                free(header);
            }
        }
        fprintf(out, "%s", paragraphs->items[i]);
        if (i + 1 < paragraphs->count) fprintf(out, "\n\n");
    }
    fprintf(out, "\n");
    fclose(out);
    return 0;
}

static int write_status(const char *path, const char *input, const char *output, size_t count, int with_headers) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out,
            "{\n"
            "  \"sourceSystem\": \"BonfyreParagraph\",\n"
            "  \"status\": \"completed\",\n"
            "  \"inputPath\": \"%s\",\n"
            "  \"outputPath\": \"%s\",\n"
            "  \"paragraphCount\": %zu,\n"
            "  \"withHeaders\": %s,\n"
            "  \"deterministic\": true\n"
            "}\n",
            input,
            output,
            count,
            with_headers ? "true" : "false");
    fclose(out);
    return 0;
}

static void usage(void) {
    fprintf(stderr, "Usage: bonfyre-paragraph --input <file> --out <file> [--with-headers] [--dry-run]\n");
}

int main(int argc, char **argv) {
    const char *input = NULL;
    const char *out_path = NULL;
    int with_headers = 0;
    int dry_run = 0;
    char *text;
    StringList sentences = {0};
    StringList paragraphs = {0};
    char out_dir[PATH_MAX];
    char status_path[PATH_MAX];
    size_t start = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) input = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out_path = argv[++i];
        else if (strcmp(argv[i], "--with-headers") == 0) with_headers = 1;
        else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
        else {
            usage();
            return 1;
        }
    }

    if (!input || !out_path) {
        usage();
        return 1;
    }
    if (dry_run) {
        printf("Would paragraphize %s -> %s\n", input, out_path);
        return 0;
    }

    text = read_file(input);
    if (!text) {
        fprintf(stderr, "Missing input file: %s\n", input);
        return 2;
    }

    if (split_sentences(text, &sentences) != 0) {
        free(text);
        free_strings(&sentences);
        return 1;
    }

    while (start < sentences.count) {
        size_t end = start;
        while (end < sentences.count) {
            size_t current_len = end - start;
            int is_shift = current_len >= TARGET_SENTENCES && starts_with_transition(sentences.items[end]);
            int is_max = current_len >= MAX_SENTENCES;
            if (current_len > 0 && (is_shift || is_max)) break;
            end++;
        }
        char *paragraph = join_range(&sentences, start, end > start ? end : start + 1);
        if (!paragraph || push_string(&paragraphs, paragraph) != 0) {
            free(paragraph);
            free(text);
            free_strings(&sentences);
            free_strings(&paragraphs);
            return 1;
        }
        free(paragraph);
        start = end > start ? end : start + 1;
    }

    strncpy(out_dir, out_path, sizeof(out_dir) - 1);
    out_dir[sizeof(out_dir) - 1] = '\0';
    char *slash = strrchr(out_dir, '/');
    if (slash) {
        *slash = '\0';
        if (out_dir[0] && ensure_dir(out_dir) != 0) {
            free(text); free_strings(&sentences); free_strings(&paragraphs);
            return 1;
        }
        snprintf(status_path, sizeof(status_path), "%s/status.json", out_dir);
    } else {
        strcpy(status_path, "status.json");
    }

    if (write_output(out_path, &paragraphs, with_headers) != 0 ||
        write_status(status_path, input, out_path, paragraphs.count, with_headers) != 0) {
        free(text); free_strings(&sentences); free_strings(&paragraphs);
        return 1;
    }

    printf("Paragraphed: %zu paragraphs\n", paragraphs.count);
    printf("Written: %s\n", out_path);

    free(text);
    free_strings(&sentences);
    free_strings(&paragraphs);
    return 0;
}
