/* ── quote ────────────────────────────────────────────────────────────────── */
/* Included by main.c.
 *
 * generate writes an evidence review that is not an offer: no price, no
 * promise, not sendable, and a list of six things that have to be true first.
 * quote is where a named person supplies those six things. Nothing here
 * works any of them out. Each one is a statement the person makes and is
 * stored as theirs, beside the digest of the review it was made about, and a
 * quote missing any of them is refused.
 *
 *   recipient need      who asked, and for what
 *   deliverable scope   what will be handed over
 *   delivery capacity   who or what will do the work, and that it is free to
 *   source permissions  the right to use the material involved
 *   real quote          a price in cents and a currency, and a turnaround
 *   human approval      the person who approves sending it
 *
 * The result is marked quoted and sendable. Sendable is permission for an
 * organ to compose a message from it; nothing in this file sends anything.
 */

static const char *quote_arg(int argc, char **argv, const char *name) {
    for (int i = 0; i + 1 < argc; i++) if (strcmp(argv[i], name) == 0) return argv[i + 1];
    return NULL;
}

static int command_quote(int argc, char **argv) {
    static const struct { const char *flag; const char *requirement; } STATEMENTS[] = {
        { "--recipient-need", "recipient_need" },
        { "--scope", "deliverable_scope" },
        { "--capacity", "delivery_capacity" },
        { "--permissions", "source_permissions" },
    };
    const char *offer_path = argc >= 3 ? argv[2] : NULL;
    const char *out_path = quote_arg(argc, argv, "--out");
    const char *price = quote_arg(argc, argv, "--price-cents");
    const char *currency = quote_arg(argc, argv, "--currency");
    const char *turnaround = quote_arg(argc, argv, "--turnaround");
    const char *promise = quote_arg(argc, argv, "--promise");
    const char *approved_by = quote_arg(argc, argv, "--approved-by");
    const char *missing = NULL;

    if (!offer_path || offer_path[0] == '-' || !out_path) {
        fprintf(stderr,
            "Usage:\n  bonfyre-offer quote <offer.json> --out FILE\n"
            "      --recipient-need TEXT --scope TEXT --capacity TEXT --permissions TEXT\n"
            "      --price-cents N --currency CODE --turnaround TEXT --promise TEXT --approved-by NAME\n"
            "  Every one is required. They are statements a person makes; none is worked out here.\n");
        return 1;
    }
    for (size_t i = 0; i < sizeof(STATEMENTS) / sizeof(STATEMENTS[0]); i++) {
        const char *value = quote_arg(argc, argv, STATEMENTS[i].flag);
        if (!value || !*value) { missing = STATEMENTS[i].flag; break; }
    }
    if (!missing && (!price || !*price)) missing = "--price-cents";
    if (!missing && (!currency || !*currency)) missing = "--currency";
    if (!missing && (!turnaround || !*turnaround)) missing = "--turnaround";
    if (!missing && (!promise || !*promise)) missing = "--promise";
    if (!missing && (!approved_by || !*approved_by)) missing = "--approved-by";
    if (missing) {
        fprintf(stderr, "Not quoted: %s was not given. A quote states all of its terms or it is not one.\n", missing);
        return 2;
    }
    char *end = NULL;
    long cents = strtol(price, &end, 10);
    if (!end || *end || cents <= 0) {
        fprintf(stderr, "Not quoted: --price-cents is a whole number of cents above zero; got %s.\n", price);
        return 2;
    }
    if (strlen(currency) != 3 || !isupper((unsigned char)currency[0]) ||
        !isupper((unsigned char)currency[1]) || !isupper((unsigned char)currency[2])) {
        fprintf(stderr, "Not quoted: --currency is a three-letter code such as USD; got %s.\n", currency);
        return 2;
    }

    long size = 0;
    char *source = read_file(offer_path, &size);
    if (!source) { fprintf(stderr, "Cannot read %s\n", offer_path); return 1; }
    char system[64] = "", offer_name[512] = "", proof_slug[256] = "", proof_label[512] = "", state[64] = "";
    extract_string_value(source, "sourceSystem", system, sizeof(system));
    extract_string_value(source, "offerName", offer_name, sizeof(offer_name));
    extract_string_value(source, "proofSlug", proof_slug, sizeof(proof_slug));
    extract_string_value(source, "proofLabel", proof_label, sizeof(proof_label));
    extract_string_value(source, "commercialState", state, sizeof(state));
    if (strcmp(system, "BonfyreOffer") != 0 || !offer_name[0] || !proof_slug[0]) {
        fprintf(stderr, "Not quoted: %s is not an evidence review this organ wrote.\n", offer_path);
        free(source); return 2;
    }
    if (strcmp(state, "not_quoted") != 0) {
        fprintf(stderr, "Not quoted: %s is already %s. A quote is made once, about a review.\n",
                offer_path, state[0] ? state : "in an unknown state");
        free(source); return 2;
    }
    char digest[65] = "";
    bf_sha256_hex((const uint8_t *)source, (size_t)size, digest);
    free(source);

    char timestamp[32];
    iso_timestamp(timestamp, sizeof(timestamp));
    FILE *out = fopen(out_path, "w");
    if (!out) { fprintf(stderr, "Cannot write %s\n", out_path); return 1; }
    fputs("{\n  \"sourceSystem\":\"BonfyreOffer\",\n  \"createdAt\":", out);
    bf_lex_json_emit(out, timestamp);
    fputs(",\n  \"offerName\":", out);
    bf_lex_json_emit(out, offer_name);
    fputs(",\n  \"proofSlug\":", out);
    bf_lex_json_emit(out, proof_slug);
    fputs(",\n  \"proofLabel\":", out);
    bf_lex_json_emit(out, proof_label);
    fputs(",\n  \"reviewSha256\":", out);
    bf_lex_json_emit(out, digest);
    fputs(",\n  \"commercialState\":\"quoted\",\n  \"sendable\":true,\n  \"price\":", out);
    char shown[64];
    snprintf(shown, sizeof(shown), "%ld.%02ld %s", cents / 100, cents % 100, currency);
    bf_lex_json_emit(out, shown);
    fprintf(out, ",\n  \"priceCents\":%ld,\n  \"currency\":", cents);
    bf_lex_json_emit(out, currency);
    fputs(",\n  \"turnaround\":", out);
    bf_lex_json_emit(out, turnaround);
    fputs(",\n  \"promise\":", out);
    bf_lex_json_emit(out, promise);
    fputs(",\n  \"approvedBy\":", out);
    bf_lex_json_emit(out, approved_by);
    fputs(",\n  \"quotedAt\":", out);
    bf_lex_json_emit(out, timestamp);
    fputs(",\n  \"statedBy\":", out);
    bf_lex_json_emit(out, approved_by);
    fputs(",\n  \"requirementsStated\":{", out);
    for (size_t i = 0; i < sizeof(STATEMENTS) / sizeof(STATEMENTS[0]); i++) {
        fprintf(out, "%s\n    \"%s\":", i ? "," : "", STATEMENTS[i].requirement);
        bf_lex_json_emit(out, quote_arg(argc, argv, STATEMENTS[i].flag));
    }
    fputs(",\n    \"real_quote\":", out);
    bf_lex_json_emit(out, shown);
    fputs(",\n    \"human_approval\":", out);
    bf_lex_json_emit(out, approved_by);
    fputs("\n  },\n  \"cannotInfer\":\"a stated requirement -> a checked one; these are the approver's statements, "
          "and a quoted offer is not an accepted one, a sent one or a paid one\"\n}\n", out);
    if (ferror(out) || fclose(out) != 0) { remove(out_path); fprintf(stderr, "Cannot write %s\n", out_path); return 1; }

    /* A quote made for an exchange is that exchange's quote stage, and the
     * chain decides whether it can be: it requires an offer to be established
     * first. When it refuses, there is no quote, so the file is taken back. */
    const char *exchange = quote_arg(argc, argv, "--exchange");
    const char *commerce_db = quote_arg(argc, argv, "--commerce-db");
    if ((exchange && !commerce_db) || (commerce_db && !exchange)) {
        remove(out_path);
        fprintf(stderr, "Not quoted: --exchange and --commerce-db go together.\n");
        return 2;
    }
    if (exchange) {
        char detail[256];
        snprintf(detail, sizeof(detail), "%s, %s, approved by %s; review %.16s", shown, turnaround, approved_by, digest);
        int stage = bf_commerce_stage_record(commerce_db, exchange, "quote", "ProtocolContract quote-accept", detail);
        if (stage != BF_COMMERCE_STAGE_RECORDED) {
            remove(out_path);
            fprintf(stderr, "Not quoted: the commerce chain did not record a quote for %s%s\n", exchange,
                    stage == BF_COMMERCE_STAGE_REFUSED ? "; an offer has to be established for it first." : ".");
            return 3;
        }
    }
    printf("Quoted: %s\n  %s, %s, approved by %s\n  %s\n", offer_name, shown, turnaround, approved_by, out_path);
    if (exchange) printf("  recorded as the quote stage of %s\n", exchange);
    return 0;
}
