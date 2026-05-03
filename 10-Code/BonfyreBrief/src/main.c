#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define MAX_SENTENCES 1024
#define MAX_LINE 8192
#define MAX_BULLETS 6

typedef struct {
    char *text;
    int score;
    int is_action;
} Sentence;

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

static void iso_timestamp(char *buffer, size_t size) {
    time_t now = time(NULL);
    struct tm tm_utc;
    gmtime_r(&now, &tm_utc);
    strftime(buffer, size, "%Y-%m-%dT%H:%M:%SZ", &tm_utc);
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

static int contains_any(const char *text, const char *words[]) {
    for (int i = 0; words[i]; i++) {
        if (strstr(text, words[i])) return 1;
    }
    return 0;
}

static int sentence_score(const char *text) {
    static const char *summary_words[] = {
        "problem", "customer", "market", "revenue", "pricing", "workflow",
        "decision", "learned", "traction", "focus", "strategy", "validation",
        "founder", "operator", "pain", "channel", "segment", NULL
    };
    static const char *action_words[] = {
        "should", "need to", "must", "next", "plan", "test", "focus", "send",
        "build", "validate", "launch", "review", "write", "ship", NULL
    };
    int score = 0;
    size_t len = strlen(text);
    if (len > 30) score += 1;
    if (len > 80) score += 1;
    if (contains_any(text, summary_words)) score += 3;
    if (contains_any(text, action_words)) score += 2;
    if (strchr(text, '$') || strstr(text, "percent")) score += 2;
    if (strstr(text, "I think") || strstr(text, "yeah") || strstr(text, "like")) score -= 2;
    return score;
}

static int is_action_sentence(const char *text) {
    static const char *action_words[] = {
        "should", "need to", "must", "next", "plan", "test", "focus", "send",
        "build", "validate", "launch", "review", "write", "ship", NULL
    };
    return contains_any(text, action_words);
}

static int cmp_sentence_desc(const void *a, const void *b) {
    const Sentence *left = (const Sentence *)a;
    const Sentence *right = (const Sentence *)b;
    return right->score - left->score;
}

static int split_sentences(const char *text, Sentence *sentences, int max_sentences) {
    int count = 0;
    const char *start = text;
    for (const char *p = text; ; p++) {
        if (*p == '.' || *p == '!' || *p == '?' || *p == '\0') {
            size_t len = (size_t)(p - start + (*p ? 1 : 0));
            if (len > 1) {
                char buffer[MAX_LINE];
                if (len >= sizeof(buffer)) len = sizeof(buffer) - 1;
                memcpy(buffer, start, len);
                buffer[len] = '\0';
                char *trimmed = trim_copy(buffer);
                if (trimmed && trimmed[0] != '\0' && count < max_sentences) {
                    sentences[count].text = trimmed;
                    sentences[count].score = sentence_score(trimmed);
                    sentences[count].is_action = is_action_sentence(trimmed);
                    count++;
                } else if (trimmed) {
                    free(trimmed);
                }
            }
            if (*p == '\0') break;
            start = p + 1;
        }
    }
    return count;
}

static void write_brief(const char *path, const char *title, Sentence *sentences, int count) {
    FILE *out = fopen(path, "w");
    if (!out) return;

    Sentence ranked[MAX_SENTENCES];
    memcpy(ranked, sentences, sizeof(Sentence) * count);
    qsort(ranked, count, sizeof(Sentence), cmp_sentence_desc);

    fprintf(out, "# %s\n\n", title);
    fprintf(out, "## Summary\n");
    int summary_written = 0;
    for (int i = 0; i < count && summary_written < MAX_BULLETS; i++) {
        if (ranked[i].score < 2 || ranked[i].is_action) continue;
        fprintf(out, "- %s\n", ranked[i].text);
        summary_written++;
    }
    if (!summary_written) fprintf(out, "- No summary generated\n");

    fprintf(out, "\n## Action Items\n");
    int action_written = 0;
    for (int i = 0; i < count && action_written < MAX_BULLETS; i++) {
        if (!ranked[i].is_action) continue;
        fprintf(out, "- %s\n", ranked[i].text);
        action_written++;
    }
    if (!action_written) fprintf(out, "- No action items detected\n");

    fprintf(out, "\n## Deep Summary\n");
    fprintf(out, "- Key Details\n");
    for (int i = 0; i < count && i < 4; i++) {
        if (ranked[i].score < 1) continue;
        fprintf(out, "  - %s\n", ranked[i].text);
    }

    fprintf(out, "\n## Transcript\n");
    for (int i = 0; i < count; i++) {
        fprintf(out, "%s%s", sentences[i].text, (i + 1 < count) ? " " : "");
    }
    fprintf(out, "\n");
    fclose(out);
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: bonfyre-brief <transcript-file> <output-dir> [--title TITLE]\n");
        return 1;
    }

    const char *transcript_path = argv[1];
    const char *output_dir = argv[2];
    const char *title = "Bonfyre Brief";

    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--title") == 0 && i + 1 < argc) {
            title = argv[++i];
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    if (ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output dir: %s\n", output_dir);
        return 1;
    }

    FILE *in = fopen(transcript_path, "r");
    if (!in) {
        perror("fopen");
        return 1;
    }

    fseek(in, 0, SEEK_END);
    long size = ftell(in);
    fseek(in, 0, SEEK_SET);
    if (size <= 0) {
        fclose(in);
        return 1;
    }

    char *buffer = malloc((size_t)size + 1);
    if (!buffer) {
        fclose(in);
        return 1;
    }
    fread(buffer, 1, (size_t)size, in);
    buffer[size] = '\0';
    fclose(in);

    Sentence sentences[MAX_SENTENCES] = {0};
    int count = split_sentences(buffer, sentences, MAX_SENTENCES);

    char brief_path[PATH_MAX];
    char meta_path[PATH_MAX];
    snprintf(brief_path, sizeof(brief_path), "%s/brief.md", output_dir);
    snprintf(meta_path, sizeof(meta_path), "%s/brief-meta.json", output_dir);

    write_brief(brief_path, title, sentences, count);

    char timestamp[32];
    iso_timestamp(timestamp, sizeof(timestamp));
    FILE *meta = fopen(meta_path, "w");
    if (meta) {
        fprintf(meta,
                "{\n"
                "  \"source_system\": \"BonfyreBrief\",\n"
                "  \"created_at\": \"%s\",\n"
                "  \"transcript_path\": \"%s\",\n"
                "  \"brief_path\": \"%s\",\n"
                "  \"sentence_count\": %d\n"
                "}\n",
                timestamp,
                transcript_path,
                brief_path,
                count);
        fclose(meta);
    }

    /* --- Emit artifact.json (Bonfyre universal manifest) --- */
    char artifact_path[PATH_MAX];
    snprintf(artifact_path, sizeof(artifact_path), "%s/artifact.json", output_dir);
    FILE *af = fopen(artifact_path, "w");
    if (af) {
        fprintf(af,
                "{\n"
                "  \"schema_version\": \"1.0.0\",\n"
                "  \"artifact_id\": \"brief-%s\",\n"
                "  \"artifact_type\": \"brief\",\n"
                "  \"created_at\": \"%s\",\n"
                "  \"source_system\": \"BonfyreBrief\",\n"
                "  \"tags\": [\"brief\"],\n"
                "  \"root_hash\": \"\",\n"
                "  \"atoms\": [\n"
                "    {\n"
                "      \"atom_id\": \"source-transcript\",\n"
                "      \"content_hash\": \"\",\n"
                "      \"media_type\": \"text/plain\",\n"
                "      \"path\": \"%s\",\n"
                "      \"label\": \"Source transcript\"\n"
                "    }\n"
                "  ],\n"
                "  \"operators\": [\n"
                "    {\n"
                "      \"operator_id\": \"op-brief-extract\",\n"
                "      \"op\": \"BriefExtract\",\n"
                "      \"inputs\": [\"source-transcript\"],\n"
                "      \"output\": \"brief-md\",\n"
                "      \"params\": {\"top_sentences\": 6, \"top_actions\": 6},\n"
                "      \"version\": \"1.0.0\",\n"
                "      \"deterministic\": true\n"
                "    },\n"
                "    {\n"
                "      \"operator_id\": \"op-brief-meta\",\n"
                "      \"op\": \"MetadataEmit\",\n"
                "      \"inputs\": [\"brief-md\"],\n"
                "      \"output\": \"brief-meta\",\n"
                "      \"params\": {},\n"
                "      \"version\": \"1.0.0\",\n"
                "      \"deterministic\": true\n"
                "    }\n"
                "  ],\n"
                "  \"realizations\": [\n"
                "    {\n"
                "      \"realization_id\": \"brief-md\",\n"
                "      \"media_type\": \"text/markdown\",\n"
                "      \"path\": \"%s\",\n"
                "      \"pinned\": true,\n"
                "      \"produced_by\": \"op-brief-extract\",\n"
                "      \"label\": \"Brief markdown\"\n"
                "    },\n"
                "    {\n"
                "      \"realization_id\": \"brief-meta\",\n"
                "      \"media_type\": \"application/json\",\n"
                "      \"path\": \"%s\",\n"
                "      \"pinned\": true,\n"
                "      \"produced_by\": \"op-brief-meta\",\n"
                "      \"label\": \"Brief metadata\"\n"
                "    }\n"
                "  ],\n"
                "  \"realization_targets\": [\n"
                "    {\n"
                "      \"target_id\": \"narrated-brief\",\n"
                "      \"media_type\": \"audio/wav\",\n"
                "      \"op\": \"Narrate\",\n"
                "      \"params\": {\"profile\": \"operator_status\"},\n"
                "      \"description\": \"Spoken brief via BonfyreNarrate\"\n"
                "    }\n"
                "  ],\n"
                "  \"metadata\": {\n"
                "    \"title\": \"%s\",\n"
                "    \"sentence_count\": %d\n"
                "  }\n"
                "}\n",
                timestamp,     /* artifact_id suffix */
                timestamp,     /* created_at */
                transcript_path,
                brief_path,
                meta_path,
                title,
                count);
        fclose(af);
    }

    printf("Brief: %s\n", brief_path);
    printf("Meta: %s\n", meta_path);
    if (af) printf("Artifact: %s\n", artifact_path);

    for (int i = 0; i < count; i++) free(sentences[i].text);
    free(buffer);
    return 0;
}
