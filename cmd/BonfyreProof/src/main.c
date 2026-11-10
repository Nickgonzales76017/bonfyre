#include <ctype.h>
#include "bf_lex.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#define MAX_TEXT 16384
#define MAX_PATH 2048

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
static void iso_timestamp(char *buffer, size_t size) { bf_iso_timestamp(buffer, size); }

static char *read_file(const char *path, long *size_out) {
    size_t sz = 0;
    char *buf = bf_read_file(path, &sz);
    if (size_out) *size_out = (long)sz;
    return buf;
}

static int extract_string_value(const char *json, const char *key, char *buffer, size_t size) {
    /* Delegates to the one implementation. This copy stopped at the first
     * quote, and truncated silently on overflow rather than reporting that the
     * value did not fit -- a prefix returned as if it were whole. */
    return bf_lex_json_string(json, key, buffer, size);
}

static int extract_int_value(const char *json, const char *key, int *value) {
    /* Delegates to the one implementation, which refuses a string where a
     * number was asked for instead of running atoi over a quote. */
    char scalar[64];
    if (!bf_lex_json_scalar(json, key, scalar, sizeof(scalar))) return 0;
    *value = atoi(scalar);
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
    path_join(summary_path, sizeof(summary_path), proof_dir, "proof-summary.json");

    long summary_size = 0;
    char *summary = read_file(summary_path, &summary_size);
    if (!summary) {
        fprintf(stderr, "Missing proof artifact.\n");
        free(summary);
        return 1;
    }

    char proof_slug[256] = "";
    char proof_label[256] = "";
    char recommendation[128] = "";
    int quality_score = 0;
    extract_string_value(summary, "proof_slug", proof_slug, sizeof(proof_slug));
    extract_string_value(summary, "proof_label", proof_label, sizeof(proof_label));
    extract_int_value(summary, "score", &quality_score);
    extract_string_value(summary, "recommendation", recommendation, sizeof(recommendation));

    printf("{\n");
    printf("  \"kind\": \"proof\",\n");
    printf("  \"proofDir\": \"%s\",\n", proof_dir);
    printf("  \"proofSlug\": \"%s\",\n", proof_slug);
    printf("  \"proofLabel\": \"%s\",\n", proof_label);
    printf("  \"qualityScore\": %d,\n", quality_score);
    printf("  \"recommendation\": \"%s\"\n", recommendation);
    printf("}\n");

    free(summary);
    return 0;
}

static int command_bundle(const char *proof_dir, const char *output_dir) {
    char summary_path[MAX_PATH];
    char deliverable_path[MAX_PATH];
    char transcript_path[MAX_PATH];
    char bundle_json_path[MAX_PATH];
    char bundle_md_path[MAX_PATH];
    path_join(summary_path, sizeof(summary_path), proof_dir, "proof-summary.json");
    path_join(deliverable_path, sizeof(deliverable_path), proof_dir, "deliverable.md");
    path_join(transcript_path, sizeof(transcript_path), proof_dir, "transcript.txt");

    long summary_size = 0;
    char *summary = read_file(summary_path, &summary_size);
    if (!summary) {
        fprintf(stderr, "Missing proof artifact.\n");
        free(summary);
        return 1;
    }

    char proof_slug[256] = "";
    char proof_label[256] = "";
    char recommendation[128] = "";
    char quality_status[128] = "";
    int quality_score = 0;
    extract_string_value(summary, "proof_slug", proof_slug, sizeof(proof_slug));
    extract_string_value(summary, "proof_label", proof_label, sizeof(proof_label));
    extract_int_value(summary, "score", &quality_score);
    extract_string_value(summary, "status", quality_status, sizeof(quality_status));
    extract_string_value(summary, "recommendation", recommendation, sizeof(recommendation));

    if (ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output dir.\n");
        free(summary);
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
        return 1;
    }

    char timestamp[32];
    iso_timestamp(timestamp, sizeof(timestamp));

    FILE *json_fp = fopen(bundle_json_path, "w");
    if (!json_fp) {
        free(summary);
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
            "  \"recommendation\": \"%s\",\n"
            "  \"deliverablePath\": \"%s\",\n"
            "  \"transcriptPath\": \"%s\"\n"
            "}\n",
            timestamp, proof_slug, proof_label, proof_dir, quality_score, quality_status,
            recommendation, copied_deliverable, copied_transcript);
    fclose(json_fp);

    FILE *md_fp = fopen(bundle_md_path, "w");
    if (!md_fp) {
        free(summary);
        return 1;
    }
    fprintf(md_fp,
            "# %s\n\n"
            "- proof slug: `%s`\n"
            "- quality: `%s (%d)`\n"
            "- recommendation: `%s` (computed; nobody reviewed this)\n"
            "- deliverable: `%s`\n"
            "- transcript: `%s`\n",
            proof_label, proof_slug, quality_status, quality_score, recommendation,
            copied_deliverable, copied_transcript);
    fclose(md_fp);

    printf("Bundle JSON: %s\n", bundle_json_path);
    printf("Bundle MD: %s\n", bundle_md_path);

    free(summary);
    return 0;
}

/* ── score command: check a brief against the source it was made from ──
 *
 * A proof of a brief is a comparison with its source. Two things are measured:
 * how many of the brief's statements are made of words the source contains,
 * and how many are whole statements rather than pieces of one. Nothing is
 * scored without the source, and nobody is said to have reviewed anything.
 */
static void lower_copy(char *dst, const char *src, size_t n) {
    size_t i = 0;
    for (; src[i] && i + 1 < n; i++) dst[i] = (char)tolower((unsigned char)src[i]);
    dst[i] = '\0';
}

/* Share of a statement's content words (four letters or more) found in the
 * lowercased source. Returns -1 when the statement has fewer than three. */
static double statement_grounding(const char *statement, const char *source_lower) {
    int words = 0, found = 0;
    const char *p = statement;
    while (*p) {
        while (*p && !isalnum((unsigned char)*p)) p++;
        char word[64];
        size_t n = 0;
        while (*p && (isalnum((unsigned char)*p) || *p == '\'' || *p == '-')) {
            if (n + 1 < sizeof(word)) word[n++] = (char)tolower((unsigned char)*p);
            p++;
        }
        word[n] = '\0';
        if (n < 4) continue;
        words++;
        if (strstr(source_lower, word)) found++;
    }
    return words < 3 ? -1.0 : (double)found / (double)words;
}

static int command_score(const char *brief_dir, const char *output_dir) {
    char brief_path[MAX_PATH];
    char meta_path[MAX_PATH];
    char transcript_path[MAX_PATH] = "";
    char summary_path[MAX_PATH];
    char deliverable_path[MAX_PATH];
    char transcript_out[MAX_PATH];

    path_join(brief_path, sizeof(brief_path), brief_dir, "brief.md");
    long brief_size = 0;
    char *brief = read_file(brief_path, &brief_size);
    if (!brief) {
        fprintf(stderr, "Missing brief.md in %s\n", brief_dir);
        return 1;
    }

    /* The source is the one the brief says it was made from, else the
     * pipeline's normalized text, else a transcript beside the brief. */
    path_join(meta_path, sizeof(meta_path), brief_dir, "brief-meta.json");
    long meta_size = 0;
    char *meta = read_file(meta_path, &meta_size);
    if (meta) {
        extract_string_value(meta, "transcript_path", transcript_path, sizeof(transcript_path));
        free(meta);
    }
    if (!transcript_path[0] || access(transcript_path, R_OK) != 0)
        snprintf(transcript_path, sizeof(transcript_path), "%s/../normalized.txt", brief_dir);
    if (access(transcript_path, R_OK) != 0)
        snprintf(transcript_path, sizeof(transcript_path), "%s/transcript.txt", brief_dir);

    long trans_size = 0;
    char *transcript = read_file(transcript_path, &trans_size);
    if (!transcript || trans_size == 0) {
        fprintf(stderr, "No source for %s: a brief cannot be proved against nothing. "
                        "Nothing was scored.\n", brief_path);
        free(brief); free(transcript);
        return 1;
    }
    char *source_lower = malloc((size_t)trans_size + 1);
    if (!source_lower || ensure_dir(output_dir) != 0) {
        free(brief); free(transcript); free(source_lower);
        return 1;
    }
    lower_copy(source_lower, transcript, (size_t)trans_size + 1);

    /* A statement is a bullet, or a line of prose, outside the copied
     * transcript section. Headings are not statements. */
    int statements = 0, grounded = 0, fragments = 0, word_count = 0;
    double grounding_sum = 0.0;
    int in_transcript_section = 0;
    char *copy = strdup(brief);
    if (!copy) { free(brief); free(transcript); free(source_lower); return 1; }
    for (char *line = copy, *next = NULL; line; line = next) {
        next = strchr(line, '\n');
        if (next) *next++ = '\0';
        char *t = line;
        while (*t == ' ' || *t == '\t') t++;
        if (*t == '#') { in_transcript_section = (strstr(t, "Transcript") != NULL); continue; }
        if (in_transcript_section) continue;
        if (t[0] == '-' || t[0] == '*') { t++; while (*t == ' ') t++; }
        if (strncmp(t, "[ ]", 3) == 0 || strncmp(t, "[x]", 3) == 0) { t += 3; while (*t == ' ') t++; }
        size_t len = strlen(t);
        while (len > 0 && (t[len - 1] == ' ' || t[len - 1] == '\r')) t[--len] = '\0';
        if (len == 0) continue;
        for (const char *w = t; *w; w++) if (*w == ' ') word_count++;
        word_count++;
        double g = statement_grounding(t, source_lower);
        if (g < 0.0) continue;
        statements++;
        grounding_sum += g;
        if (g >= 0.8) grounded++;
        char last = t[len - 1];
        int whole = (last == '.' || last == '!' || last == '?' || last == ':' || last == ')' || last == '"' || last == '`')
                    && !islower((unsigned char)t[0]);
        if (!whole) fragments++;
    }
    free(copy);
    free(source_lower);
    if (statements == 0) {
        fprintf(stderr, "%s makes no statement long enough to check. Nothing was scored.\n", brief_path);
        free(brief); free(transcript);
        return 1;
    }

    double grounded_fraction = (double)grounded / (double)statements;
    double fragment_fraction = (double)fragments / (double)statements;
    int score = (int)(100.0 * grounded_fraction * (1.0 - fragment_fraction) + 0.5);
    const char *status = (grounded_fraction >= 0.9 && fragment_fraction <= 0.2) ? "pass"
                       : (grounded_fraction >= 0.7 && fragment_fraction <= 0.5) ? "marginal" : "fail";
    const char *recommendation = !strcmp(status, "pass") ? "approve" : !strcmp(status, "marginal") ? "review" : "reject";

    const char *slug = strrchr(brief_dir, '/');
    slug = slug ? slug + 1 : brief_dir;
    char timestamp[32];
    iso_timestamp(timestamp, sizeof(timestamp));

    path_join(summary_path, sizeof(summary_path), output_dir, "proof-summary.json");
    FILE *sfp = fopen(summary_path, "w");
    if (!sfp) { free(brief); free(transcript); return 1; }
    fprintf(sfp,
            "{\n"
            "  \"proof_slug\": \"%s\",\n"
            "  \"proof_label\": \"Brief checked against its source: %s\",\n"
            "  \"score\": %d,\n"
            "  \"status\": \"%s\",\n"
            "  \"recommendation\": \"%s\",\n"
            "  \"basis\": \"share of statements whose words the source contains, times share that are whole statements; no person or model reviewed this\",\n"
            "  \"source_path\": \"%s\",\n"
            "  \"statements\": %d,\n"
            "  \"grounded_statements\": %d,\n"
            "  \"fragment_statements\": %d,\n"
            "  \"mean_grounding\": %.3f,\n"
            "  \"word_count\": %d,\n"
            "  \"scored_at\": \"%s\"\n"
            "}\n",
            slug, slug, score, status, recommendation, transcript_path,
            statements, grounded, fragments, grounding_sum / (double)statements,
            word_count, timestamp);
    fclose(sfp);

    path_join(deliverable_path, sizeof(deliverable_path), output_dir, "deliverable.md");
    path_join(transcript_out, sizeof(transcript_out), output_dir, "transcript.txt");
    if (copy_file(brief_path, deliverable_path) != 0 || copy_file(transcript_path, transcript_out) != 0) {
        free(brief); free(transcript);
        return 1;
    }

    printf("Score: %d/100 (%s)\n", score, status);
    printf("Statements: %d, grounded in source: %d, fragments: %d\n", statements, grounded, fragments);
    printf("Recommendation: %s\n", recommendation);
    printf("Summary: %s\n", summary_path);

    free(brief);
    free(transcript);
    return 0;
}

static int command_full(const char *brief_dir, const char *output_dir) {
    char scored_dir[MAX_PATH];

    if (ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output dir.\n");
        return 1;
    }
    path_join(scored_dir, sizeof(scored_dir), output_dir, "scored");
    if (command_score(brief_dir, scored_dir) != 0) {
        return 1;
    }
    return command_bundle(scored_dir, output_dir);
}

static int command_audit_features(const char *proof_dir, const char *features_json, const char *out_path_opt) {
    long feat_size = 0;
    char *features = read_file(features_json, &feat_size);
    if (!features) {
        fprintf(stderr, "Missing feature manifest: %s\n", features_json);
        return 1;
    }

    char model_family[128] = "unknown";
    int layer = 0;
    extract_string_value(features, "model_family", model_family, sizeof(model_family));
    extract_int_value(features, "layer", &layer);

    char out_path[MAX_PATH];
    if (out_path_opt && out_path_opt[0]) {
        snprintf(out_path, sizeof(out_path), "%s", out_path_opt);
    } else {
        path_join(out_path, sizeof(out_path), proof_dir, "proof-feature-audit.json");
    }

    FILE *fp = fopen(out_path, "w");
    if (!fp) {
        free(features);
        fprintf(stderr, "Failed to open output: %s\n", out_path);
        return 1;
    }

    char timestamp[32];
    iso_timestamp(timestamp, sizeof(timestamp));

    fprintf(fp,
            "{\n"
            "  \"kind\": \"proof-feature-audit\",\n"
            "  \"proof_dir\": \"%s\",\n"
            "  \"source_manifest\": \"%s\",\n"
            "  \"model_family\": \"%s\",\n"
            "  \"layer\": %d,\n"
            "  \"audited_at\": \"%s\",\n"
            "  \"dominant_features\": [\n",
            proof_dir, features_json, model_family, layer, timestamp);

    const char *needle = "\"feature_id\"";
    const char *p = features;
    int emitted = 0;
    while ((p = strstr(p, needle)) && emitted < 16) {
        p += strlen(needle);
        const char *colon = strchr(p, ':');
        if (!colon) break;
        int fid = atoi(colon + 1);

        const char *obj_end = strchr(colon, '}');
        if (!obj_end) break;

        float activation = 0.0f;
        int tags = 0;
        char label[256] = "";

        const char *a = strstr(colon, "\"activation\"");
        if (a && a < obj_end) {
            const char *ac = strchr(a, ':');
            if (ac) activation = strtof(ac + 1, NULL);
        }

        const char *t = strstr(colon, "\"tags\"");
        if (t && t < obj_end) {
            const char *tc = strchr(t, ':');
            if (tc) tags = atoi(tc + 1);
        }

        const char *l = strstr(colon, "\"label\"");
        if (l && l < obj_end) {
            const char *lc = strchr(l, ':');
            if (lc) {
                lc++;
                while (*lc && isspace((unsigned char)*lc)) lc++;
                if (*lc == '"') {
                    lc++;
                    size_t i = 0;
                    while (*lc && *lc != '"' && i + 1 < sizeof(label)) label[i++] = *lc++;
                    label[i] = '\0';
                }
            }
        }

        if (emitted > 0) fprintf(fp, ",\n");
        fprintf(fp,
                "    {\"rank\": %d, \"feature_id\": %d, \"activation\": %.6f, \"tags\": %d, \"label\": \"%s\"}",
                emitted + 1, fid, activation, tags, label);
        emitted++;
        p = obj_end + 1;
    }

    fprintf(fp, "\n  ]\n}\n");
    fclose(fp);
    free(features);

    printf("Feature audit: %s\n", out_path);
    return 0;
}

static void usage(void) {
    fprintf(stderr,
                "Usage:\n"
                "  bonfyre-proof full <brief-dir> <output-dir>\n"
                "  bonfyre-proof score <brief-dir> <output-dir>\n"
                "  bonfyre-proof bundle <proof-dir> <output-dir>\n"
                "  bonfyre-proof audit-features <proof-dir> <features.json> [out.json]\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    if (argc < 3) {
        usage();
        return 1;
    }
    enum {
        PF_AUDIT_FEATURES, PF_BUNDLE, PF_FULL, PF_INSPECT, PF_SCORE,
    };
    static const BfCommandEntry proof_commands[] = {
        {"audit-features", PF_AUDIT_FEATURES, 0, NULL},
        {"bundle", PF_BUNDLE, 0, NULL},
        {"full", PF_FULL, 0, NULL},
        {"inspect", PF_INSPECT, 0, NULL},
        {"score", PF_SCORE, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(proof_commands,
        sizeof(proof_commands) / sizeof(proof_commands[0]), argv[1]);
    if (command && command->id == PF_FULL && argc == 4) {
        return command_full(argv[2], argv[3]);
    }
    if (command && command->id == PF_SCORE && argc == 4) {
        return command_score(argv[2], argv[3]);
    }
    if (command && command->id == PF_INSPECT && argc == 3) {
        return command_inspect(argv[2]);
    }
    if (command && command->id == PF_BUNDLE && argc == 4) {
        return command_bundle(argv[2], argv[3]);
    }
    if (command && command->id == PF_AUDIT_FEATURES && (argc == 4 || argc == 5)) {
        return command_audit_features(argv[2], argv[3], argc == 5 ? argv[4] : NULL);
    }
    fprintf(stderr, "Invalid command.\n");
    return 1;
}
