#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MAX_TEXT 16384
#define MAX_PATH 2048

static int ensure_dir(const char *path) {
    char tmp[MAX_PATH];
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

static void iso_timestamp(char *buffer, size_t size) {
    time_t now = time(NULL);
    struct tm tm_utc;
    gmtime_r(&now, &tm_utc);
    strftime(buffer, size, "%Y-%m-%dT%H:%M:%SZ", &tm_utc);
}

static char *read_file(const char *path, long *size_out) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (size < 0) {
        fclose(fp);
        return NULL;
    }
    char *buffer = malloc((size_t)size + 1);
    if (!buffer) {
        fclose(fp);
        return NULL;
    }
    fread(buffer, 1, (size_t)size, fp);
    fclose(fp);
    buffer[size] = '\0';
    if (size_out) *size_out = size;
    return buffer;
}

static int extract_string_value(const char *json, const char *key, char *buffer, size_t size) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *pos = strstr(json, needle);
    if (!pos) return 0;
    pos = strchr(pos + strlen(needle), ':');
    if (!pos) return 0;
    pos++;
    while (*pos && isspace((unsigned char)*pos)) pos++;
    if (*pos != '"') return 0;
    pos++;
    const char *end = strchr(pos, '"');
    if (!end) return 0;
    size_t len = (size_t)(end - pos);
    if (len >= size) len = size - 1;
    memcpy(buffer, pos, len);
    buffer[len] = '\0';
    return 1;
}

static int extract_int_value(const char *json, const char *key, int *value) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *pos = strstr(json, needle);
    if (!pos) return 0;
    pos = strchr(pos + strlen(needle), ':');
    if (!pos) return 0;
    pos++;
    while (*pos && isspace((unsigned char)*pos)) pos++;
    *value = atoi(pos);
    return 1;
}

static void path_join(char *buffer, size_t size, const char *left, const char *right) {
    snprintf(buffer, size, "%s/%s", left, right);
}

static int copy_file(const char *src, const char *dst) {
    long size = 0;
    char *content = read_file(src, &size);
    if (!content) return 1;
    FILE *fp = fopen(dst, "wb");
    if (!fp) {
        free(content);
        return 1;
    }
    fwrite(content, 1, (size_t)size, fp);
    fclose(fp);
    free(content);
    return 0;
}

static int command_inspect(const char *proof_dir) {
    char summary_path[MAX_PATH];
    char review_path[MAX_PATH];
    path_join(summary_path, sizeof(summary_path), proof_dir, "proof-summary.json");
    path_join(review_path, sizeof(review_path), proof_dir, "proof-review.json");

    long summary_size = 0;
    long review_size = 0;
    char *summary = read_file(summary_path, &summary_size);
    char *review = read_file(review_path, &review_size);
    if (!summary || !review) {
        fprintf(stderr, "Missing proof artifact.\n");
        free(summary);
        free(review);
        return 1;
    }

    char proof_slug[256] = "";
    char proof_label[256] = "";
    char recommendation[128] = "";
    int quality_score = 0;
    int review_score = 0;
    extract_string_value(summary, "proof_slug", proof_slug, sizeof(proof_slug));
    extract_string_value(summary, "proof_label", proof_label, sizeof(proof_label));
    extract_int_value(summary, "score", &quality_score);
    extract_string_value(review, "recommendation", recommendation, sizeof(recommendation));
    extract_int_value(review, "review_score", &review_score);

    printf("{\n");
    printf("  \"kind\": \"proof\",\n");
    printf("  \"proofDir\": \"%s\",\n", proof_dir);
    printf("  \"proofSlug\": \"%s\",\n", proof_slug);
    printf("  \"proofLabel\": \"%s\",\n", proof_label);
    printf("  \"qualityScore\": %d,\n", quality_score);
    printf("  \"reviewScore\": %d,\n", review_score);
    printf("  \"recommendation\": \"%s\"\n", recommendation);
    printf("}\n");

    free(summary);
    free(review);
    return 0;
}

static int command_bundle(const char *proof_dir, const char *output_dir) {
    char summary_path[MAX_PATH];
    char review_path[MAX_PATH];
    char deliverable_path[MAX_PATH];
    char transcript_path[MAX_PATH];
    char bundle_json_path[MAX_PATH];
    char bundle_md_path[MAX_PATH];
    path_join(summary_path, sizeof(summary_path), proof_dir, "proof-summary.json");
    path_join(review_path, sizeof(review_path), proof_dir, "proof-review.json");
    path_join(deliverable_path, sizeof(deliverable_path), proof_dir, "deliverable.md");
    path_join(transcript_path, sizeof(transcript_path), proof_dir, "transcript.txt");

    long summary_size = 0;
    long review_size = 0;
    char *summary = read_file(summary_path, &summary_size);
    char *review = read_file(review_path, &review_size);
    if (!summary || !review) {
        fprintf(stderr, "Missing proof artifact.\n");
        free(summary);
        free(review);
        return 1;
    }

    char proof_slug[256] = "";
    char proof_label[256] = "";
    char recommendation[128] = "";
    char quality_status[128] = "";
    int quality_score = 0;
    int review_score = 0;
    extract_string_value(summary, "proof_slug", proof_slug, sizeof(proof_slug));
    extract_string_value(summary, "proof_label", proof_label, sizeof(proof_label));
    extract_int_value(summary, "score", &quality_score);
    extract_string_value(summary, "status", quality_status, sizeof(quality_status));
    extract_string_value(review, "recommendation", recommendation, sizeof(recommendation));
    extract_int_value(review, "review_score", &review_score);

    if (ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output dir.\n");
        free(summary);
        free(review);
        return 1;
    }

    char copied_deliverable[MAX_PATH];
    char copied_transcript[MAX_PATH];
    path_join(copied_deliverable, sizeof(copied_deliverable), output_dir, "deliverable.md");
    path_join(copied_transcript, sizeof(copied_transcript), output_dir, "transcript.txt");
    path_join(bundle_json_path, sizeof(bundle_json_path), output_dir, "proof-bundle.json");
    path_join(bundle_md_path, sizeof(bundle_md_path), output_dir, "proof-bundle.md");

    if (copy_file(deliverable_path, copied_deliverable) != 0 || copy_file(transcript_path, copied_transcript) != 0) {
        fprintf(stderr, "Failed to copy proof files.\n");
        free(summary);
        free(review);
        return 1;
    }

    char timestamp[32];
    iso_timestamp(timestamp, sizeof(timestamp));

    FILE *json_fp = fopen(bundle_json_path, "w");
    if (!json_fp) {
        free(summary);
        free(review);
        return 1;
    }
    fprintf(json_fp,
            "{\n"
            "  \"sourceSystem\": \"BonfyreProof\",\n"
            "  \"bundledAt\": \"%s\",\n"
            "  \"proofSlug\": \"%s\",\n"
            "  \"proofLabel\": \"%s\",\n"
            "  \"proofDir\": \"%s\",\n"
            "  \"qualityScore\": %d,\n"
            "  \"qualityStatus\": \"%s\",\n"
            "  \"reviewScore\": %d,\n"
            "  \"recommendation\": \"%s\",\n"
            "  \"deliverablePath\": \"%s\",\n"
            "  \"transcriptPath\": \"%s\"\n"
            "}\n",
            timestamp, proof_slug, proof_label, proof_dir, quality_score, quality_status,
            review_score, recommendation, copied_deliverable, copied_transcript);
    fclose(json_fp);

    FILE *md_fp = fopen(bundle_md_path, "w");
    if (!md_fp) {
        free(summary);
        free(review);
        return 1;
    }
    fprintf(md_fp,
            "# %s\n\n"
            "- proof slug: `%s`\n"
            "- quality: `%s (%d)`\n"
            "- review: `%s (%d)`\n"
            "- deliverable: `%s`\n"
            "- transcript: `%s`\n",
            proof_label, proof_slug, quality_status, quality_score, recommendation, review_score,
            copied_deliverable, copied_transcript);
    fclose(md_fp);

    printf("Bundle JSON: %s\n", bundle_json_path);
    printf("Bundle MD: %s\n", bundle_md_path);

    free(summary);
    free(review);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr,
                "Usage:\n"
                "  bonfyre-proof inspect <proof-dir>\n"
                "  bonfyre-proof bundle <proof-dir> <output-dir>\n");
        return 1;
    }
    if (strcmp(argv[1], "inspect") == 0 && argc == 3) {
        return command_inspect(argv[2]);
    }
    if (strcmp(argv[1], "bundle") == 0 && argc == 4) {
        return command_bundle(argv[2], argv[3]);
    }
    fprintf(stderr, "Invalid command.\n");
    return 1;
}
