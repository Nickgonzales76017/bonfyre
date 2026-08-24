#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

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

static int command_generate(const char *bundle_json_path, const char *output_dir) {
    long size = 0;
    char *bundle = read_file(bundle_json_path, &size);
    if (!bundle) {
        fprintf(stderr, "Failed to read proof bundle.\n");
        return 1;
    }

    char proof_slug[256] = "";
    char proof_label[256] = "";
    char recommendation[128] = "";
    char quality_status[128] = "";
    int quality_score = 0;
    int review_score = 0;
    extract_string_value(bundle, "proofSlug", proof_slug, sizeof(proof_slug));
    extract_string_value(bundle, "proofLabel", proof_label, sizeof(proof_label));
    extract_string_value(bundle, "recommendation", recommendation, sizeof(recommendation));
    extract_string_value(bundle, "qualityStatus", quality_status, sizeof(quality_status));
    extract_int_value(bundle, "qualityScore", &quality_score);
    extract_int_value(bundle, "reviewScore", &review_score);

    if (ensure_dir(output_dir) != 0) {
        free(bundle);
        return 1;
    }

    char timestamp[32];
    char offer_name[512];
    char offer_json_path[MAX_PATH];
    char offer_md_path[MAX_PATH];
    char outreach_md_path[MAX_PATH];
    iso_timestamp(timestamp, sizeof(timestamp));
    snprintf(offer_name, sizeof(offer_name), "%s Native Offer", proof_label);
    path_join(offer_json_path, sizeof(offer_json_path), output_dir, "offer.json");
    path_join(offer_md_path, sizeof(offer_md_path), output_dir, "offer.md");
    path_join(outreach_md_path, sizeof(outreach_md_path), output_dir, "outreach.md");

    FILE *json_fp = fopen(offer_json_path, "w");
    if (!json_fp) {
        free(bundle);
        return 1;
    }
    fprintf(json_fp,
            "{\n"
            "  \"sourceSystem\": \"BonfyreOffer\",\n"
            "  \"createdAt\": \"%s\",\n"
            "  \"offerName\": \"%s\",\n"
            "  \"proofSlug\": \"%s\",\n"
            "  \"proofLabel\": \"%s\",\n"
            "  \"qualityScore\": %d,\n"
            "  \"qualityStatus\": \"%s\",\n"
            "  \"reviewScore\": %d,\n"
            "  \"recommendation\": \"%s\",\n"
            "  \"headline\": \"Local-first proof-backed deliverables\",\n"
            "  \"promise\": \"Send one messy recording and get back a clean transcript, structured summary, and next steps.\",\n"
            "  \"price\": \"$18\",\n"
            "  \"turnaround\": \"same day for short files\"\n"
            "}\n",
            timestamp, offer_name, proof_slug, proof_label, quality_score, quality_status, review_score, recommendation);
    fclose(json_fp);

    FILE *md_fp = fopen(offer_md_path, "w");
    if (!md_fp) {
        free(bundle);
        return 1;
    }
    fprintf(md_fp,
            "# %s\n\n"
            "## Headline\n"
            "Local-first proof-backed deliverables\n\n"
            "## Promise\n"
            "Send one messy recording and get back a clean transcript, structured summary, and next steps.\n\n"
            "## Proof\n"
            "- asset: `%s`\n"
            "- quality: `%s (%d)`\n"
            "- review: `%s (%d)`\n\n"
            "## Commercial Shape\n"
            "- price: `$18`\n"
            "- turnaround: `same day for short files`\n",
            offer_name, proof_label, quality_status, quality_score, recommendation, review_score);
    fclose(md_fp);

    FILE *outreach_fp = fopen(outreach_md_path, "w");
    if (!outreach_fp) {
        free(bundle);
        return 1;
    }
    fprintf(outreach_fp,
            "I packaged `%s` into a local-first deliverable flow that returns a transcript, summary, and next steps without relying on a SaaS stack. If you have one messy recording, I can turn it into a decision-ready asset quickly.\n",
            proof_label);
    fclose(outreach_fp);

    printf("Offer JSON: %s\n", offer_json_path);
    printf("Offer MD: %s\n", offer_md_path);
    printf("Outreach MD: %s\n", outreach_md_path);

    free(bundle);
    return 0;
}

int main(int argc, char **argv) {
    if (argc == 4 && strcmp(argv[1], "generate") == 0) {
        return command_generate(argv[2], argv[3]);
    }
    fprintf(stderr, "Usage:\n  bonfyre-offer generate <proof-bundle.json> <output-dir>\n");
    return 1;
}
