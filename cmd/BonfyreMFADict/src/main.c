#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <bonfyre.h>

typedef struct {
    char **items;
    size_t count;
    size_t capacity;
} WordList;

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

static int push_unique_word(WordList *list, const char *word) {
    for (size_t i = 0; i < list->count; i++) {
        if (strcmp(list->items[i], word) == 0) return 0;
    }
    if (list->count == list->capacity) {
        size_t next_capacity = list->capacity == 0 ? 64 : list->capacity * 2;
        char **next_items = realloc(list->items, sizeof(char *) * next_capacity);
        if (!next_items) return 1;
        list->items = next_items;
        list->capacity = next_capacity;
    }
    list->items[list->count] = strdup(word);
    if (!list->items[list->count]) return 1;
    list->count++;
    return 0;
}

static void free_words(WordList *list) {
    for (size_t i = 0; i < list->count; i++) free(list->items[i]);
    free(list->items);
}

static int cmp_words(const void *a, const void *b) {
    const char *left = *(const char * const *)a;
    const char *right = *(const char * const *)b;
    return strcmp(left, right);
}

static WordList tokenize_words(const char *text) {
    WordList words = {0};
    char token[256];
    size_t len = 0;
    for (size_t i = 0; ; i++) {
        unsigned char c = (unsigned char)text[i];
        if (isalpha(c) || c == '\'') {
            if (len + 1 < sizeof(token)) token[len++] = (char)toupper(c);
        } else {
            if (len > 0) {
                token[len] = '\0';
                push_unique_word(&words, token);
                len = 0;
            }
            if (c == '\0') break;
        }
    }
    if (words.count > 1) qsort(words.items, words.count, sizeof(char *), cmp_words);
    return words;
}

static int write_lexicon(const char *path, const WordList *words) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    for (size_t i = 0; i < words->count; i++) {
        fprintf(out, "%s", words->items[i]);
        for (size_t j = 0; words->items[i][j]; j++) {
            fprintf(out, " %c", words->items[i][j]);
        }
        fputc('\n', out);
    }
    fclose(out);
    return 0;
}

static int write_status(const char *path, const char *transcript_path, const char *dict_path, size_t entries) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out,
            "{\n"
            "  \"sourceSystem\": \"BonfyreMFADict\",\n"
            "  \"status\": \"completed\",\n"
            "  \"transcriptPath\": \"%s\",\n"
            "  \"dictionaryPath\": \"%s\",\n"
            "  \"entryCount\": %zu,\n"
            "  \"deterministic\": true,\n"
            "  \"backend\": \"naive-grapheme-native\"\n"
            "}\n",
            transcript_path,
            dict_path,
            entries);
    fclose(out);
    return 0;
}

static void usage(void) {
    fprintf(stderr, "Usage: bonfyre-mfa-dict --transcript <path> --out <path> [--dry-run]\n");
}

int main(int argc, char **argv) {
    const char *transcript_path = NULL;
    const char *out_path = NULL;
    int dry_run = 0;
    char *text;
    WordList words;
    char out_dir[PATH_MAX];
    char status_path[PATH_MAX];

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--transcript") == 0 && i + 1 < argc) transcript_path = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out_path = argv[++i];
        else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
        else {
            usage();
            return 1;
        }
    }
    if (!transcript_path || !out_path) {
        usage();
        return 1;
    }

    text = read_file(transcript_path);
    if (!text) {
        fprintf(stderr, "Missing transcript file: %s\n", transcript_path);
        return 2;
    }
    words = tokenize_words(text);
    if (dry_run) {
        printf("Would generate %zu lexicon entries to %s\n", words.count, out_path);
        free(text);
        free_words(&words);
        return 0;
    }

    strncpy(out_dir, out_path, sizeof(out_dir) - 1);
    out_dir[sizeof(out_dir) - 1] = '\0';
    char *slash = strrchr(out_dir, '/');
    if (slash) {
        *slash = '\0';
        if (out_dir[0] && ensure_dir(out_dir) != 0) {
            free(text); free_words(&words);
            return 1;
        }
        snprintf(status_path, sizeof(status_path), "%s/status.json", out_dir);
    } else {
        strcpy(status_path, "status.json");
    }

    if (write_lexicon(out_path, &words) != 0 ||
        write_status(status_path, transcript_path, out_path, words.count) != 0) {
        free(text); free_words(&words);
        return 1;
    }

    printf("Wrote %zu entries to %s\n", words.count, out_path);
    free(text);
    free_words(&words);
    return 0;
}
