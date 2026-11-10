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
#include <sys/wait.h>
#include <unistd.h>

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

/* ---------- fork/exec Python for ONNX inference ---------- */

static int run_cmd(const char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        execvp(argv[0], (char *const *)argv);
        _exit(127);
    }
    int st;
    waitpid(pid, &st, 0);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static const char *resolve_model_dir(const char *model) {
    /* If model looks like a path, use it directly */
    if (model[0] == '/' || model[0] == '.') return model;
    /* Otherwise resolve under ~/.cache/bonfyre/models/ */
    static char resolved[PATH_MAX];
    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    snprintf(resolved, sizeof(resolved),
             "%s/.cache/bonfyre/models/%s", home, model);
    return resolved;
}

static int run_onnx_embed(const char *text_path, const char *vector_out,
                           const char *model_dir, int *out_dims) {
    char script[4096];
    snprintf(script, sizeof(script),
        "import onnxruntime as ort, json, sys, os\n"
        "from tokenizers import Tokenizer\n"
        "import numpy as np\n"
        "\n"
        "model_dir = '%s'\n"
        "text_path = '%s'\n"
        "out_path  = '%s'\n"
        "\n"
        "tok_path = os.path.join(model_dir, 'tokenizer.json')\n"
        "if not os.path.exists(tok_path):\n"
        "    print('ERROR: tokenizer not found: ' + tok_path, file=sys.stderr)\n"
        "    sys.exit(1)\n"
        "\n"
        "# prefer quantized ARM64 model on Apple Silicon, fallback to full model\n"
        "onnx_dir = os.path.join(model_dir, 'onnx')\n"
        "for name in ['model_qint8_arm64.onnx', 'model_O4.onnx', 'model.onnx']:\n"
        "    mp = os.path.join(onnx_dir, name)\n"
        "    if os.path.exists(mp):\n"
        "        model_path = mp\n"
        "        break\n"
        "else:\n"
        "    print('ERROR: no ONNX model found in ' + onnx_dir, file=sys.stderr)\n"
        "    sys.exit(1)\n"
        "\n"
        "tokenizer = Tokenizer.from_file(tok_path)\n"
        "tokenizer.enable_padding(pad_id=0, pad_token='[PAD]', length=128)\n"
        "tokenizer.enable_truncation(max_length=128)\n"
        "\n"
        "sess = ort.InferenceSession(model_path, providers=['CPUExecutionProvider'])\n"
        "\n"
        "with open(text_path, 'r') as f:\n"
        "    text = f.read().strip()\n"
        "if not text:\n"
        "    print('ERROR: empty input text', file=sys.stderr)\n"
        "    sys.exit(1)\n"
        "\n"
        "enc = tokenizer.encode(text)\n"
        "input_ids = np.array([enc.ids], dtype=np.int64)\n"
        "attn_mask = np.array([enc.attention_mask], dtype=np.int64)\n"
        "token_type_ids = np.zeros_like(input_ids)\n"
        "\n"
        "outputs = sess.run(None, {\n"
        "    'input_ids': input_ids,\n"
        "    'attention_mask': attn_mask,\n"
        "    'token_type_ids': token_type_ids\n"
        "})\n"
        "\n"
        "tok_emb = outputs[0]\n"
        "mask_exp = attn_mask[:, :, np.newaxis].astype(np.float32)\n"
        "summed = np.sum(tok_emb * mask_exp, axis=1)\n"
        "counted = np.clip(mask_exp.sum(axis=1), a_min=1e-9, a_max=None)\n"
        "embedding = (summed / counted)[0]\n"
        "norm = np.linalg.norm(embedding)\n"
        "if norm > 0:\n"
        "    embedding = embedding / norm\n"
        "\n"
        "result = {\n"
        "    'vector': embedding.tolist(),\n"
        "    'dims': len(embedding),\n"
        "    'model_path': model_path,\n"
        "    'backend': 'onnx-runtime'\n"
        "}\n"
        "with open(out_path, 'w') as f:\n"
        "    json.dump(result, f)\n"
        "print(len(embedding))\n",
        model_dir, text_path, vector_out);

    /* Write temp script */
    char tmp_py[PATH_MAX];
    snprintf(tmp_py, sizeof(tmp_py), "%s.onnx_embed.py", vector_out);
    FILE *f = fopen(tmp_py, "w");
    if (!f) return -1;
    fputs(script, f);
    fclose(f);

    const char *python = getenv("BONFYRE_PYTHON3");
    if (!python) python = "python3";
    const char *argv[] = { python, tmp_py, NULL };
    int rc = run_cmd(argv);
    unlink(tmp_py);

    if (rc == 0 && out_dims) {
        /* Read dims from the JSON output */
        FILE *jf = fopen(vector_out, "r");
        if (jf) {
            char line[256];
            while (fgets(line, sizeof(line), jf)) {
                char *d = strstr(line, "\"dims\":");
                if (d) { *out_dims = atoi(d + 7); break; }
            }
            fclose(jf);
        }
    }
    return rc;
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
                           size_t token_count,
                           const char *backend) {
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
            "  \"backend\": \"%s\"\n"
            "}\n",
            text_path,
            vector_path,
            dims,
            model,
            token_count,
            backend);
    fclose(out);
    return 0;
}

static int write_status_json(const char *path, const char *vector_path,
                              const char *meta_path, const char *backend) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out,
            "{\n"
            "  \"sourceSystem\": \"BonfyreEmbed\",\n"
            "  \"status\": \"completed\",\n"
            "  \"vectorPath\": \"%s\",\n"
            "  \"metaPath\": \"%s\",\n"
            "  \"deterministic\": true,\n"
            "  \"backend\": \"%s\"\n"
            "}\n",
            vector_path,
            meta_path,
            backend);
    fclose(out);
    return 0;
}

static void usage(void) {
    fprintf(stderr,
            "Usage: bonfyre-embed --text <path> --out <path> "
            "[--meta-out <path>] [--model <name>] [--dims <n>] "
            "[--backend onnx|hash] [--dry-run]\n");
}

int main(int argc, char **argv) {
    const char *text_path = NULL;
    const char *out_path = NULL;
    const char *meta_out = NULL;
    const char *model = "all-MiniLM-L6-v2";
    const char *backend = "onnx";
    int dims = 384;
    int dry_run = 0;
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
        } else if (strcmp(argv[i], "--backend") == 0 && i + 1 < argc) {
            backend = argv[++i];
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
        printf("Would embed transcript: %s\n", text_path);
        printf("Would write vector artifact: %s\n", out_path);
        printf("Backend: %s  Model: %s  Dims: %d\n", backend, model, dims);
        if (meta_out) printf("Would write metadata artifact: %s\n", meta_out);
        return 0;
    }

    /* Verify input exists */
    if (access(text_path, R_OK) != 0) {
        fprintf(stderr, "Missing text file: %s\n", text_path);
        return 2;
    }

    /* Prepare output directory */
    strncpy(out_dir, out_path, sizeof(out_dir) - 1);
    out_dir[sizeof(out_dir) - 1] = '\0';
    char *slash = strrchr(out_dir, '/');
    if (slash) {
        *slash = '\0';
        if (out_dir[0] != '\0' && ensure_dir(out_dir) != 0) {
            return 1;
        }
    }

    if (!meta_out) {
        snprintf(default_meta, sizeof(default_meta), "%s.json", out_path);
        meta_out = default_meta;
    }
    snprintf(status_path, sizeof(status_path), "%s/status.json", out_dir[0] ? out_dir : ".");

    /* ---- ONNX backend: real neural embeddings ---- */
    if (strcmp(backend, "onnx") == 0) {
        const char *model_dir = resolve_model_dir(model);
        int actual_dims = dims;

        fprintf(stderr, "[embed] backend=onnx  model=%s\n", model_dir);
        int rc = run_onnx_embed(text_path, out_path, model_dir, &actual_dims);
        if (rc != 0) {
            fprintf(stderr, "[embed] ONNX inference failed (rc=%d), "
                    "falling back to hash backend\n", rc);
            backend = "hash";
            /* fall through to hash backend */
        } else {
            dims = actual_dims;
            /* Read token count from the text for metadata */
            char *text = read_file(text_path);
            TokenList tokens = {0};
            if (text) { tokens = normalize_tokens(text); free(text); }

            if (write_meta_json(meta_out, text_path, out_path, dims,
                                model, tokens.count, "onnx-runtime") != 0 ||
                write_status_json(status_path, out_path, meta_out,
                                   "onnx-runtime") != 0) {
                token_list_free(&tokens);
                return 1;
            }

            printf("Wrote embedding to %s  [%d dims, onnx-runtime]\n", out_path, dims);
            printf("Wrote metadata to %s\n", meta_out);
            token_list_free(&tokens);
            return 0;
        }
    }

    /* ---- Hash backend: deterministic fallback ---- */
    char *text = read_file(text_path);
    if (!text) {
        fprintf(stderr, "Cannot read text file: %s\n", text_path);
        return 2;
    }

    if (dims <= 0) dims = 768;  /* hash backend default */

    TokenList tokens = normalize_tokens(text);
    float *vector = build_embedding(&tokens, dims);
    if (!vector) {
        free(text);
        token_list_free(&tokens);
        return 1;
    }

    if (write_vector_json(out_path, vector, dims) != 0 ||
        write_meta_json(meta_out, text_path, out_path, dims, model,
                        tokens.count, "hashed-token-native") != 0 ||
        write_status_json(status_path, out_path, meta_out,
                           "hashed-token-native") != 0) {
        free(text);
        free(vector);
        token_list_free(&tokens);
        return 1;
    }

    printf("Wrote embedding to %s  [%d dims, hash]\n", out_path, dims);
    printf("Wrote metadata to %s\n", meta_out);

    free(text);
    free(vector);
    token_list_free(&tokens);
    return 0;
}
