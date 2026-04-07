#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>

#define MAX_TEXT 16384
#define MAX_PATH 2048

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
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

/* ── score command: compute quality metrics from a brief + transcript ── */
static int command_score(const char *brief_dir, const char *output_dir) {
    char brief_path[MAX_PATH];
    char transcript_path[MAX_PATH];
    char summary_path[MAX_PATH];
    char review_path[MAX_PATH];
    char deliverable_path[MAX_PATH];
    char transcript_out[MAX_PATH];

    /* Find brief.md and the transcript it references */
    path_join(brief_path, sizeof(brief_path), brief_dir, "brief.md");
    path_join(deliverable_path, sizeof(deliverable_path), brief_dir, "brief.md");

    /* Look for transcript in parent dir or sibling */
    snprintf(transcript_path, sizeof(transcript_path), "%s/../normalized.txt", brief_dir);
    if (access(transcript_path, F_OK) != 0) {
        snprintf(transcript_path, sizeof(transcript_path), "%s/transcript.txt", brief_dir);
    }

    long brief_size = 0;
    char *brief = read_file(brief_path, &brief_size);
    if (!brief) {
        fprintf(stderr, "Missing brief.md in %s\n", brief_dir);
        return 1;
    }

    long trans_size = 0;
    char *transcript = read_file(transcript_path, &trans_size);
    /* Transcript is optional for scoring */

    if (ensure_dir(output_dir) != 0) {
        free(brief); free(transcript);
        return 1;
    }

    /* Compute quality metrics from the brief */
    int word_count = 0;
    int sentence_count = 0;
    int filler_count = 0;
    int has_summary = (strstr(brief, "## Summary") != NULL) ? 1 : 0;
    int has_action = (strstr(brief, "## Action Items") != NULL) ? 1 : 0;
    int has_transcript = (strstr(brief, "## Transcript") != NULL) ? 1 : 0;

    for (const char *p = brief; *p; p++) {
        if (*p == ' ' || *p == '\n') word_count++;
        if (*p == '.' || *p == '!' || *p == '?') sentence_count++;
    }

    /* Count filler patterns */
    const char *fillers[] = {"like,", "you know", "kind of", "sort of", "I mean", "basically", NULL};
    for (int i = 0; fillers[i]; i++) {
        const char *p = brief;
        while ((p = strstr(p, fillers[i])) != NULL) {
            filler_count++;
            p += strlen(fillers[i]);
        }
    }

    /* ── Multi-dimensional quality rubric ──
     *
     * Dimension 1 — Structure (0-25):  section presence + balance
     * Dimension 2 — Density (0-25):    information density (unique/total words)
     * Dimension 3 — Clarity (0-25):    filler ratio, sentence length variance
     * Dimension 4 — Coverage (0-25):   word count relative to expected
     *
     * Total: 0-100
     */

    /* Dimension 1: Structure */
    int structure_score = 0;
    if (has_summary) structure_score += 8;
    if (has_action) structure_score += 8;
    if (has_transcript) structure_score += 4;
    /* Bonus for balanced sections (not just headers with empty content) */
    if (has_summary && word_count > 100) structure_score += 3;
    if (has_action && sentence_count > 3) structure_score += 2;
    if (structure_score > 25) structure_score = 25;

    /* Dimension 2: Density — unique words / total words */
    int unique_words = 0;
    {
        /* Simple unique word count via hash set */
        #define HASH_BUCKETS 2048
        unsigned int seen[HASH_BUCKETS];
        memset(seen, 0, sizeof(seen));
        const char *wp = brief;
        while (*wp) {
            while (*wp && !isalpha((unsigned char)*wp)) wp++;
            if (!*wp) break;
            const char *wstart = wp;
            while (*wp && isalpha((unsigned char)*wp)) wp++;
            size_t wlen = (size_t)(wp - wstart);
            if (wlen < 2) continue;
            /* FNV-1a hash */
            unsigned int h = 2166136261u;
            for (size_t i = 0; i < wlen; i++) {
                h ^= (unsigned int)(unsigned char)tolower((unsigned char)wstart[i]);
                h *= 16777619u;
            }
            unsigned int bucket = h % HASH_BUCKETS;
            if (seen[bucket] != h) { seen[bucket] = h; unique_words++; }
        }
    }
    double density = word_count > 0 ? (double)unique_words / (double)word_count : 0;
    int density_score = (int)(density * 50.0); /* 0.5 ratio → 25 */
    if (density_score > 25) density_score = 25;

    /* Dimension 3: Clarity — penalize fillers, reward clean prose */
    int clarity_score = 20; /* start optimistic */
    double filler_ratio = word_count > 0 ? (double)filler_count / (double)word_count : 0;
    if (filler_ratio > 0.05) clarity_score -= 10;
    else if (filler_ratio > 0.02) clarity_score -= 5;
    else if (filler_ratio < 0.005) clarity_score += 5; /* very clean */
    /* Penalize very short sentences (likely fragments) */
    double avg_sent_len = sentence_count > 0 ? (double)word_count / (double)sentence_count : 0;
    if (avg_sent_len < 5) clarity_score -= 5;
    if (avg_sent_len > 8 && avg_sent_len < 30) clarity_score += 3; /* good range */
    if (clarity_score < 0) clarity_score = 0;
    if (clarity_score > 25) clarity_score = 25;

    /* Dimension 4: Coverage — enough content to be useful */
    int coverage_score = 0;
    if (word_count > 50) coverage_score += 5;
    if (word_count > 200) coverage_score += 5;
    if (word_count > 500) coverage_score += 5;
    if (word_count > 1000) coverage_score += 5;
    if (sentence_count > 5) coverage_score += 3;
    if (sentence_count > 20) coverage_score += 2;
    if (coverage_score > 25) coverage_score = 25;

    int score = structure_score + density_score + clarity_score + coverage_score;
    if (score > 100) score = 100;
    if (score < 0) score = 0;

    int review_score = score >= 80 ? 90 : score >= 50 ? 70 : 40;
    const char *status = score >= 80 ? "pass" : score >= 50 ? "marginal" : "fail";
    const char *recommendation = score >= 80 ? "approve" : score >= 50 ? "review" : "reject";

    /* Generate slug from brief dir name */
    const char *slug = strrchr(brief_dir, '/');
    slug = slug ? slug + 1 : brief_dir;

    char timestamp[32];
    iso_timestamp(timestamp, sizeof(timestamp));

    /* Write proof-summary.json */
    path_join(summary_path, sizeof(summary_path), output_dir, "proof-summary.json");
    FILE *sfp = fopen(summary_path, "w");
    if (!sfp) { free(brief); free(transcript); return 1; }
    fprintf(sfp,
            "{\n"
            "  \"proof_slug\": \"%s\",\n"
            "  \"proof_label\": \"Quality proof for %s\",\n"
            "  \"score\": %d,\n"
            "  \"status\": \"%s\",\n"
            "  \"dimensions\": {\"structure\": %d, \"density\": %d, \"clarity\": %d, \"coverage\": %d},\n"
            "  \"word_count\": %d,\n"
            "  \"unique_words\": %d,\n"
            "  \"sentence_count\": %d,\n"
            "  \"filler_count\": %d,\n"
            "  \"scored_at\": \"%s\"\n"
            "}\n",
            slug, slug, score, status,
            structure_score, density_score, clarity_score, coverage_score,
            word_count, unique_words, sentence_count, filler_count, timestamp);
    fclose(sfp);

    /* Write proof-review.json */
    path_join(review_path, sizeof(review_path), output_dir, "proof-review.json");
    FILE *rfp = fopen(review_path, "w");
    if (!rfp) { free(brief); free(transcript); return 1; }
    fprintf(rfp,
            "{\n"
            "  \"review_score\": %d,\n"
            "  \"recommendation\": \"%s\",\n"
            "  \"reviewed_at\": \"%s\"\n"
            "}\n",
            review_score, recommendation, timestamp);
    fclose(rfp);

    /* Copy deliverable.md and transcript.txt into output */
    path_join(deliverable_path, sizeof(deliverable_path), output_dir, "deliverable.md");
    copy_file(brief_path, deliverable_path);

    path_join(transcript_out, sizeof(transcript_out), output_dir, "transcript.txt");
    if (transcript) copy_file(transcript_path, transcript_out);

    printf("Score: %d/100 (%s)\n", score, status);
    printf("Recommendation: %s\n", recommendation);
    printf("Summary: %s\n", summary_path);
    printf("Review: %s\n", review_path);

    free(brief);
    free(transcript);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr,
                "Usage:\n"
                "  bonfyre-proof score <brief-dir> <output-dir>\n"
                "  bonfyre-proof inspect <proof-dir>\n"
                "  bonfyre-proof bundle <proof-dir> <output-dir>\n");
        return 1;
    }
    if (strcmp(argv[1], "score") == 0 && argc == 4) {
        return command_score(argv[2], argv[3]);
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
