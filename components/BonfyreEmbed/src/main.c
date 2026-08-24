#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

typedef struct {
    char **items;
    size_t count;
    size_t capacity;
} TokenList;

static int ensure_dir(const char *path) {
    char tmp[PATH_MAX];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    strcpy(tmp, path);
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

static int token_list_push(TokenList *list, const char *value) {
    char *copy;
    if (list->count == list->capacity) {
        size_t next_capacity = list->capacity == 0 ? 64 : list->capacity * 2;
        char **next_items = realloc(list->items, sizeof(char *) * next_capacity);
        if (!next_items) return 1;
        list->items = next_items;
        list->capacity = next_capacity;
    }
    copy = strdup(value);
    if (!copy) return 1;
    list->items[list->count++] = copy;
    return 0;
}

static void token_list_free(TokenList *list) {
    for (size_t i = 0; i < list->count; i++) free(list->items[i]);
    free(list->items);
}

static TokenList normalize_tokens(const char *text) {
    TokenList tokens = {0};
    char current[256];
    size_t len = 0;

    for (size_t i = 0; ; i++) {
        unsigned char c = (unsigned char)text[i];
        if (isalnum(c) || c == '\'') {
            if (len + 1 < sizeof(current)) current[len++] = (char)tolower(c);
        } else {
            if (len > 0) {
                current[len] = '\0';
                token_list_push(&tokens, current);
                len = 0;
            }
            if (c == '\0') break;
        }
    }
    return tokens;
}

static uint64_t fnv1a64(const char *text) {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; text[i]; i++) {
        hash ^= (unsigned char)text[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

static float *build_embedding(const TokenList *tokens, int dims) {
    float *vector = calloc((size_t)dims, sizeof(float));
    if (!vector) return NULL;
    if (tokens->count == 0) return vector;

    for (size_t i = 0; i < tokens->count; i++) {
        uint64_t hash = fnv1a64(tokens->items[i]);
        int index = (int)(hash % (uint64_t)dims);
        float sign = ((hash >> 8) & 1ULL) == 0ULL ? 1.0f : -1.0f;
        float weight = 1.0f + (float)((hash >> 16) & 0xFFULL) / 255.0f;
        vector[index] += sign * weight;
    }

    double norm = 0.0;
    for (int i = 0; i < dims; i++) norm += (double)vector[i] * (double)vector[i];
    norm = sqrt(norm);
    if (norm > 0.0) {
        for (int i = 0; i < dims; i++) vector[i] = (float)(vector[i] / norm);
    }
    return vector;
}

static int write_vector_json(const char *path, const float *vector, int dims) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out, "{\n  \"vector\": [\n");
    for (int i = 0; i < dims; i++) {
        fprintf(out, "    %.8f%s\n", vector[i], (i + 1 < dims) ? "," : "");
    }
    fprintf(out, "  ]\n}\n");
    fclose(out);
    return 0;
}

static int write_meta_json(const char *path,
                           const char *text_path,
                           const char *vector_path,
                           int dims,
                           const char *model,
                           size_t token_count) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out,
            "{\n"
            "  \"sourceSystem\": \"BonfyreEmbed\",\n"
            "  \"textPath\": \"%s\",\n"
            "  \"vectorPath\": \"%s\",\n"
            "  \"vectorFormat\": \"json\",\n"
            "  \"dims\": %d,\n"
            "  \"model\": \"%s\",\n"
            "  \"tokens\": %zu,\n"
            "  \"deterministic\": true,\n"
            "  \"backend\": \"hashed-token-native\"\n"
            "}\n",
            text_path,
            vector_path,
            dims,
            model,
            token_count);
    fclose(out);
    return 0;
}

static int write_status_json(const char *path, const char *vector_path, const char *meta_path) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out,
            "{\n"
            "  \"sourceSystem\": \"BonfyreEmbed\",\n"
            "  \"status\": \"completed\",\n"
            "  \"vectorPath\": \"%s\",\n"
            "  \"metaPath\": \"%s\",\n"
            "  \"deterministic\": true,\n"
            "  \"backend\": \"hashed-token-native\"\n"
            "}\n",
            vector_path,
            meta_path);
    fclose(out);
    return 0;
}

static void usage(void) {
    fprintf(stderr,
            "Usage: bonfyre-embed --text <path> --out <path> "
            "[--meta-out <path>] [--model <name>] [--dims <n>] [--dry-run]\n");
}

int main(int argc, char **argv) {
    const char *text_path = NULL;
    const char *out_path = NULL;
    const char *meta_out = NULL;
    const char *model = "sentence_transformer.onnx";
    int dims = 768;
    int dry_run = 0;
    char *text;
    TokenList tokens;
    float *vector;
    char default_meta[PATH_MAX];
    char status_path[PATH_MAX];
    char out_dir[PATH_MAX];

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--text") == 0 && i + 1 < argc) {
            text_path = argv[++i];
        } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
            out_path = argv[++i];
        } else if (strcmp(argv[i], "--meta-out") == 0 && i + 1 < argc) {
            meta_out = argv[++i];
        } else if (strcmp(argv[i], "--model") == 0 && i + 1 < argc) {
            model = argv[++i];
        } else if (strcmp(argv[i], "--dims") == 0 && i + 1 < argc) {
            dims = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--dry-run") == 0) {
            dry_run = 1;
        } else {
            usage();
            return 1;
        }
    }

    if (!text_path || !out_path || dims <= 0) {
        usage();
        return 1;
    }

    if (dry_run) {
        printf("Would load model seam: %s\n", model);
        printf("Would embed transcript: %s\n", text_path);
        printf("Would write vector artifact: %s\n", out_path);
        if (meta_out) printf("Would write metadata artifact: %s\n", meta_out);
        return 0;
    }

    text = read_file(text_path);
    if (!text) {
        fprintf(stderr, "Missing text file: %s\n", text_path);
        return 2;
    }

    strncpy(out_dir, out_path, sizeof(out_dir) - 1);
    out_dir[sizeof(out_dir) - 1] = '\0';
    char *slash = strrchr(out_dir, '/');
    if (slash) {
        *slash = '\0';
        if (out_dir[0] != '\0' && ensure_dir(out_dir) != 0) {
            free(text);
            return 1;
        }
    }

    if (!meta_out) {
        snprintf(default_meta, sizeof(default_meta), "%s.json", out_path);
        meta_out = default_meta;
    }
    snprintf(status_path, sizeof(status_path), "%s/status.json", out_dir[0] ? out_dir : ".");

    tokens = normalize_tokens(text);
    vector = build_embedding(&tokens, dims);
    if (!vector) {
        free(text);
        token_list_free(&tokens);
        return 1;
    }

    if (write_vector_json(out_path, vector, dims) != 0 ||
        write_meta_json(meta_out, text_path, out_path, dims, model, tokens.count) != 0 ||
        write_status_json(status_path, out_path, meta_out) != 0) {
        free(text);
        free(vector);
        token_list_free(&tokens);
        return 1;
    }

    printf("Wrote embedding to %s\n", out_path);
    printf("Wrote metadata to %s\n", meta_out);

    free(text);
    free(vector);
    token_list_free(&tokens);
    return 0;
}
