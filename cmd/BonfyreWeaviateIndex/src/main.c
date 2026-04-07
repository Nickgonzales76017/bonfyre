#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <bonfyre.h>

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

static char *json_string_value(const char *json, const char *key) {
    char pattern[256];
    char *hit;
    char *start;
    char *end;
    snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    hit = strstr(json, pattern);
    if (!hit) return NULL;
    start = strchr(hit + strlen(pattern), ':');
    if (!start) return NULL;
    start++;
    while (*start && isspace((unsigned char)*start)) start++;
    if (*start != '"') return NULL;
    start++;
    end = start;
    while (*end && *end != '"') end++;
    if (*end != '"') return NULL;
    size_t len = (size_t)(end - start);
    char *value = malloc(len + 1);
    if (!value) return NULL;
    memcpy(value, start, len);
    value[len] = '\0';
    return value;
}

static int count_vector_length(const char *json) {
    char *vector = strstr(json, "\"vector\"");
    char *start;
    int count = 0;
    if (!vector) return 0;
    start = strchr(vector, '[');
    if (!start) return 0;
    start++;
    while (*start && *start != ']') {
        while (*start && (isspace((unsigned char)*start) || *start == ',')) start++;
        if (!*start || *start == ']') break;
        strtod(start, &start);
        count++;
    }
    return count;
}

static char *build_document_id(const char *meta_json) {
    const char *keys[] = {"job_slug", "jobSlug", "proofLabel", "id", "job_name", "jobName", NULL};
    for (int i = 0; keys[i]; i++) {
        char *value = json_string_value(meta_json, keys[i]);
        if (value && value[0]) {
            for (size_t j = 0; value[j]; j++) {
                if (isspace((unsigned char)value[j])) value[j] = '-';
                else value[j] = (char)tolower((unsigned char)value[j]);
            }
            return value;
        }
        free(value);
    }
    return strdup("bonfyre-document");
}

static int write_payload(const char *path,
                         const char *class_name,
                         const char *document_id,
                         const char *emb_path,
                         const char *meta_path,
                         int vector_length,
                         const char *job_slug,
                         const char *job_name,
                         const char *source_kind) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out,
            "{\n"
            "  \"sourceSystem\": \"BonfyreWeaviateIndex\",\n"
            "  \"className\": \"%s\",\n"
            "  \"documentId\": \"%s\",\n"
            "  \"embeddingPath\": \"%s\",\n"
            "  \"metadataPath\": \"%s\",\n"
            "  \"vectorLength\": %d,\n"
            "  \"properties\": {\n"
            "    \"jobSlug\": ",
            class_name,
            document_id,
            emb_path,
            meta_path,
            vector_length);
    if (job_slug) fprintf(out, "\"%s\"", job_slug); else fprintf(out, "null");
    fprintf(out, ",\n    \"jobName\": ");
    if (job_name) fprintf(out, "\"%s\"", job_name); else fprintf(out, "null");
    fprintf(out, ",\n    \"sourceKind\": ");
    if (source_kind) fprintf(out, "\"%s\"", source_kind); else fprintf(out, "null");
    fprintf(out,
            ",\n"
            "    \"metaPath\": \"%s\"\n"
            "  },\n"
            "  \"weaviateUpserted\": false\n"
            "}\n",
            meta_path);
    fclose(out);
    return 0;
}

static int write_status(const char *path, const char *payload_path, const char *document_id) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fprintf(out,
            "{\n"
            "  \"sourceSystem\": \"BonfyreWeaviateIndex\",\n"
            "  \"status\": \"completed\",\n"
            "  \"ingestPayloadPath\": \"%s\",\n"
            "  \"documentId\": \"%s\",\n"
            "  \"weaviateUpserted\": false\n"
            "}\n",
            payload_path,
            document_id);
    fclose(out);
    return 0;
}

static void usage(void) {
    fprintf(stderr, "Usage: bonfyre-weaviate-index --emb <path> --meta <path> [--out <path>] [--class-name <name>] [--dry-run]\n");
}

int main(int argc, char **argv) {
    const char *emb_path = NULL;
    const char *meta_path = NULL;
    const char *out_path = NULL;
    const char *class_name = "BonfyreTranscript";
    int dry_run = 0;
    char default_out[PATH_MAX];
    char status_path[PATH_MAX];
    char out_dir[PATH_MAX];
    char *emb_json;
    char *meta_json;
    char *document_id;
    char *job_slug;
    char *job_name;
    char *source_kind;
    int vector_length;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--emb") == 0 && i + 1 < argc) emb_path = argv[++i];
        else if (strcmp(argv[i], "--meta") == 0 && i + 1 < argc) meta_path = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out_path = argv[++i];
        else if (strcmp(argv[i], "--class-name") == 0 && i + 1 < argc) class_name = argv[++i];
        else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
        else {
            usage();
            return 1;
        }
    }

    if (!emb_path || !meta_path) {
        usage();
        return 1;
    }
    if (dry_run) {
        printf("Would ingest embedding %s with metadata %s into %s\n", emb_path, meta_path, class_name);
        return 0;
    }
    if (!out_path) {
        snprintf(default_out, sizeof(default_out), "%s.weaviate-batch.json", meta_path);
        out_path = default_out;
    }
    strncpy(out_dir, out_path, sizeof(out_dir) - 1);
    out_dir[sizeof(out_dir) - 1] = '\0';
    char *slash = strrchr(out_dir, '/');
    if (slash) {
        *slash = '\0';
        if (out_dir[0] && ensure_dir(out_dir) != 0) return 1;
        snprintf(status_path, sizeof(status_path), "%s/status.json", out_dir);
    } else {
        strcpy(status_path, "status.json");
    }

    emb_json = read_file(emb_path);
    meta_json = read_file(meta_path);
    if (!emb_json || !meta_json) {
        fprintf(stderr, "Missing emb or meta files\n");
        free(emb_json);
        free(meta_json);
        return 2;
    }

    vector_length = count_vector_length(emb_json);
    document_id = build_document_id(meta_json);
    job_slug = json_string_value(meta_json, "job_slug");
    if (!job_slug) job_slug = json_string_value(meta_json, "jobSlug");
    job_name = json_string_value(meta_json, "job_name");
    if (!job_name) job_name = json_string_value(meta_json, "jobName");
    source_kind = json_string_value(meta_json, "source_kind");
    if (!source_kind) source_kind = json_string_value(meta_json, "sourceKind");

    if (write_payload(out_path, class_name, document_id, emb_path, meta_path, vector_length, job_slug, job_name, source_kind) != 0 ||
        write_status(status_path, out_path, document_id) != 0) {
        free(emb_json); free(meta_json); free(document_id); free(job_slug); free(job_name); free(source_kind);
        return 1;
    }

    printf("Wrote ingest payload to %s\n", out_path);
    printf("Weaviate client/url unavailable; wrote local ingest payload only.\n");

    free(emb_json); free(meta_json); free(document_id); free(job_slug); free(job_name); free(source_kind);
    return 0;
}
