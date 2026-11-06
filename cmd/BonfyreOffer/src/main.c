#include <ctype.h>
#include "bf_lex.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <bonfyre.h>
#include <bf_commerce_stage.h>

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
    int word_count = 0, sentence_count = 0;
    int word_known = 0, sentence_known = 0;
    extract_string_value(bundle, "proofSlug", proof_slug, sizeof(proof_slug));
    extract_string_value(bundle, "proofLabel", proof_label, sizeof(proof_label));
    extract_string_value(bundle, "recommendation", recommendation, sizeof(recommendation));
    extract_string_value(bundle, "qualityStatus", quality_status, sizeof(quality_status));
    int quality_known = extract_int_value(bundle, "qualityScore", &quality_score);
    int review_known = extract_int_value(bundle, "reviewScore", &review_score);
    if (!proof_slug[0] || !proof_label[0]) { fprintf(stderr, "Proof identity is required.\n"); free(bundle); return 2; }

    /* Also try to read proof-summary.json from same dir for word_count */
    {
        char summary_path[MAX_PATH];
        const char *slash = strrchr(bundle_json_path, '/');
        if (slash) {
            size_t dir_len = (size_t)(slash - bundle_json_path);
            snprintf(summary_path, sizeof(summary_path), "%.*s/proof-summary.json", (int)dir_len, bundle_json_path);
        } else {
            snprintf(summary_path, sizeof(summary_path), "proof-summary.json");
        }
        long ss = 0;
        char *summary_json = read_file(summary_path, &ss);
        if (summary_json) {
            word_known = extract_int_value(summary_json, "word_count", &word_count);
            sentence_known = extract_int_value(summary_json, "sentence_count", &sentence_count);
            free(summary_json);
        }
    }

    if (ensure_dir(output_dir) != 0) {
        free(bundle);
        return 1;
    }

    /* Proof quality is not a price, accepted order, or delivery promise. */

    char timestamp[32];
    char offer_name[512];
    char offer_json_path[MAX_PATH];
    char offer_md_path[MAX_PATH];
    char outreach_md_path[MAX_PATH];
    iso_timestamp(timestamp, sizeof(timestamp));
    snprintf(offer_name, sizeof(offer_name), "Evidence review: %s", proof_label);
    path_join(offer_json_path, sizeof(offer_json_path), output_dir, "offer.json");
    path_join(offer_md_path, sizeof(offer_md_path), output_dir, "offer.md");
    path_join(outreach_md_path, sizeof(outreach_md_path), output_dir, "outreach.md");

    FILE *json_fp = fopen(offer_json_path, "w");
    if (!json_fp) { free(bundle); return 1; }
    fputs("{\n  \"sourceSystem\":\"BonfyreOffer\",\n  \"createdAt\":", json_fp);
    bf_lex_json_emit(json_fp, timestamp);
    fputs(",\n  \"offerName\":", json_fp);
    bf_lex_json_emit(json_fp, offer_name);
    fputs(",\n  \"proofSlug\":", json_fp);
    bf_lex_json_emit(json_fp, proof_slug);
    fputs(",\n  \"proofLabel\":", json_fp);
    bf_lex_json_emit(json_fp, proof_label);
    fputs(",\n  \"proofRecommendation\":", json_fp);
    bf_lex_json_emit(json_fp, recommendation);
    fputs(",\n  \"qualityStatus\":", json_fp);
    bf_lex_json_emit(json_fp, quality_status);
    fputs(",\n  \"qualityScore\":", json_fp);
    if (quality_known) fprintf(json_fp, "%d", quality_score); else fputs("null", json_fp);
    fputs(",\n  \"reviewScore\":", json_fp);
    if (review_known) fprintf(json_fp, "%d", review_score); else fputs("null", json_fp);
    fputs(",\n  \"wordCount\":", json_fp);
    if (word_known) fprintf(json_fp, "%d", word_count); else fputs("null", json_fp);
    fputs(",\n  \"sentenceCount\":", json_fp);
    if (sentence_known) fprintf(json_fp, "%d", sentence_count); else fputs("null", json_fp);
    fputs(",\n  \"commercialState\":\"not_quoted\",\n  \"sendable\":false,"
          "\n  \"price\":null,\n  \"priceCents\":null,\n  \"pricingFactors\":null,"
          "\n  \"turnaround\":null,\n  \"promise\":null,"
          "\n  \"requirements\":[\"recipient_need\",\"deliverable_scope\",\"delivery_capacity\",\"source_permissions\",\"real_quote\",\"human_approval\"]\n}\n", json_fp);
    if (ferror(json_fp) || fclose(json_fp) != 0) { free(bundle); return 1; }

    FILE *md_fp = fopen(offer_md_path, "w");
    if (!md_fp) { free(bundle); return 1; }
    fprintf(md_fp, "# Evidence review: %s\n\n", proof_label);
    fputs("This proof packet is not a priced offer or a delivery commitment.\n\n", md_fp);
    fprintf(md_fp, "Proof identity: %s\nProof quality: %s\nProof recommendation: %s\n\n",
            proof_slug, quality_status, recommendation);
    fputs("Commercial state: not quoted.\n\n"
          "Before outreach: confirm the recipient's actual need, work scope, source permissions, "
          "delivery resources, source-backed cost and price, and approval to send.\n"
          "Unknown price and delivery dates remain unknown.\n", md_fp);
    if (ferror(md_fp) || fclose(md_fp) != 0) { free(bundle); return 1; }

    FILE *outreach_fp = fopen(outreach_md_path, "w");
    if (!outreach_fp) { free(bundle); return 1; }
    fputs("INTERNAL PREPARATION ONLY. Do not send this file as an offer.\n"
          "The proof bundle does not establish a buyer, approved scope, quote, "
          "delivery deadline, purchase order or authority to make a commitment.\n",
          outreach_fp);
    if (ferror(outreach_fp) || fclose(outreach_fp) != 0) { free(bundle); return 1; }

    printf("Offer JSON: %s\n", offer_json_path);
    printf("Offer MD: %s\n", offer_md_path);
    printf("Outreach MD: %s\n", outreach_md_path);

    free(bundle);
    return 0;
}

#include "quote.inc"
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static void usage(void) {
    fprintf(stderr, "Usage:\n  bonfyre-offer generate <proof-bundle.json> <output-dir>\n  bonfyre-offer quote <offer.json> --out FILE ...   a person states every term; see bonfyre-offer quote\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    enum { OF_GENERATE, OF_QUOTE };
    static const BfCommandEntry offer_commands[] = {
        {"generate", OF_GENERATE, 0, NULL},
        {"quote", OF_QUOTE, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(offer_commands,
        sizeof(offer_commands) / sizeof(offer_commands[0]), argv[1]);
    if (argc >= 2 && command && command->id == OF_QUOTE) {
        return command_quote(argc, argv);
    }
    if (argc == 4 && command && command->id == OF_GENERATE) {
        return command_generate(argv[2], argv[3]);
    }
    usage();
    return 1;
}
