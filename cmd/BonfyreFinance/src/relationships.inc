/* ── relationships ────────────────────────────────────────────────────────── */
/* Included by main.c.
 *
 * One line per commercial relationship, read across the stores that each own
 * a part of it and joined on the identity they already share: the finance
 * opportunity id, which the outreach entry carries as its offer name, the CRM
 * deal carries as its source, and the commerce chain carries as
 * "opportunity:<id>".
 *
 * Nothing is written and no store is given a new job. Finance still owns the
 * opportunity and its requirements, Outreach what was sent and what came
 * back, the content store the deal, Commerce the stage of a sale. This only
 * puts them side by side and says where they disagree or where one of them
 * has nothing.
 *
 * "furthest" is the last step some store holds evidence for, in this order:
 *   specified -> sent -> replied -> scope_accepted -> delivered -> paid
 * A price that was offered is not an accepted scope, and none of these is
 * inferred from the one before it.
 */

typedef struct {
    sqlite3 *finance, *outreach, *cms, *commerce;
} RelationshipStores;

static sqlite3 *relationships_open(const char *path) {
    sqlite3 *store = NULL;
    if (!path) return NULL;
    if (sqlite3_open_v2(path, &store, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
        sqlite3_exec(store, "SELECT COUNT(*) FROM sqlite_master", NULL, NULL, NULL) == SQLITE_OK)
        return store;
    if (store) sqlite3_close(store);
    store = NULL;
    /* A store kept in write-ahead mode opens read-only and then cannot be
     * read when its shared-memory file is not beside it, as with a copy.
     * Such a file is not changing, so it is read as immutable. A store that
     * still cannot be read is an error, never an empty answer. */
    {
        char uri[4200];
        snprintf(uri, sizeof(uri), "file:%s?immutable=1", path);
        if (sqlite3_open_v2(uri, &store, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK &&
            sqlite3_exec(store, "SELECT COUNT(*) FROM sqlite_master", NULL, NULL, NULL) == SQLITE_OK)
            return store;
    }
    fprintf(stderr, "relationships: cannot read %s\n", path);
    if (store) sqlite3_close(store);
    return NULL;
}

/* First column of the first row as text into out; returns 1 when a row came
 * back. A store that is absent, or lacks the table, answers 0. */
static int relationships_text(sqlite3 *store, const char *sql, const char *key, char *out, size_t size) {
    sqlite3_stmt *q = NULL;
    int found = 0;
    out[0] = '\0';
    if (!store || sqlite3_prepare_v2(store, sql, -1, &q, NULL) != SQLITE_OK) return 0;
    if (key) sqlite3_bind_text(q, 1, key, -1, SQLITE_STATIC);
    if (sqlite3_step(q) == SQLITE_ROW) {
        const char *value = (const char *)sqlite3_column_text(q, 0);
        snprintf(out, size, "%s", value ? value : "");
        found = 1;
    }
    sqlite3_finalize(q);
    return found;
}

static long relationships_count(sqlite3 *store, const char *sql, const char *key) {
    char text[32];
    return relationships_text(store, sql, key, text, sizeof(text)) ? atol(text) : -1;
}

static void relationships_json_text(const char *value) {
    putchar('"');
    for (const unsigned char *c = (const unsigned char *)value; *c; c++) {
        if (*c == '"' || *c == '\\') { putchar('\\'); putchar(*c); }
        else if (*c < 0x20) putchar(' ');
        else putchar(*c);
    }
    putchar('"');
}

static void relationships_number_or_null(const char *name, long value) {
    if (value < 0) printf(",\"%s\":null", name); else printf(",\"%s\":%ld", name, value);
}

static int cmd_relationships(sqlite3 *finance, int argc, char **argv) {
    RelationshipStores s;
    sqlite3_stmt *rows = NULL;
    long total = 0, sent = 0, replied = 0, priced = 0, costed = 0, with_deal = 0, in_chain = 0, paid = 0;
    long offered_cents = 0;
    int first = 1;
    s.finance = finance;
    s.outreach = relationships_open(arg_get(argc, argv, "--outreach-db"));
    s.cms = relationships_open(arg_get(argc, argv, "--cms-db"));
    s.commerce = relationships_open(arg_get(argc, argv, "--commerce-db"));
    /* A store that was named and could not be read stops the report. Leaving
     * it out would print "nothing sent" for every relationship. */
    if ((arg_get(argc, argv, "--outreach-db") && !s.outreach) || (arg_get(argc, argv, "--cms-db") && !s.cms) ||
        (arg_get(argc, argv, "--commerce-db") && !s.commerce)) {
        if (s.outreach) sqlite3_close(s.outreach);
        if (s.cms) sqlite3_close(s.cms);
        if (s.commerce) sqlite3_close(s.commerce);
        return 1;
    }

    if (sqlite3_prepare_v2(finance,
            "SELECT id, title, source_kind, stage, compensation_cents, currency, "
            "       expected_compute_cents + expected_external_cents, "
            "       COALESCE(authority_ref,''), COALESCE(delivery_ref,''), COALESCE(payment_ref,''), actual_revenue_cents, "
            "       (SELECT COUNT(*) FROM opportunity_requirements r WHERE r.opportunity_id = o.id), "
            "       (SELECT COUNT(*) FROM opportunity_requirements r WHERE r.opportunity_id = o.id AND r.state = 'satisfied'), "
            "       (SELECT COUNT(*) FROM opportunity_requirements r WHERE r.opportunity_id = o.id AND r.kind = 'material' "
            "                       AND r.requirement_key = 'cost_basis_confirmed' AND r.state = 'satisfied' "
            "                       AND r.evidence_ref NOT IN ('','-')) "
            "FROM opportunities o ORDER BY observed_at, id", -1, &rows, NULL) != SQLITE_OK) {
        fprintf(stderr, "relationships: this finance store has no readable opportunities: %s\n", sqlite3_errmsg(finance));
        return 1;
    }
    printf("{\"kind\":\"relationships\",\"joined_on\":\"finance opportunity id\",\"stores\":{\"outreach\":%s,\"cms\":%s,\"commerce\":%s},"
           "\"relationships\":[",
           s.outreach ? "true" : "false", s.cms ? "true" : "false", s.commerce ? "true" : "false");
    while (sqlite3_step(rows) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(rows, 0);
        const char *title = (const char *)sqlite3_column_text(rows, 1);
        const char *source_kind = (const char *)sqlite3_column_text(rows, 2);
        const char *stage = (const char *)sqlite3_column_text(rows, 3);
        int has_price = sqlite3_column_type(rows, 4) != SQLITE_NULL;
        long price = has_price ? (long)sqlite3_column_int64(rows, 4) : -1;
        const char *currency = (const char *)sqlite3_column_text(rows, 5);
        const char *authority = (const char *)sqlite3_column_text(rows, 7);
        const char *delivery = (const char *)sqlite3_column_text(rows, 8);
        const char *payment = (const char *)sqlite3_column_text(rows, 9);
        int has_revenue = sqlite3_column_type(rows, 10) != SQLITE_NULL && sqlite3_column_int64(rows, 10) > 0;
        long requirements = (long)sqlite3_column_int64(rows, 11), closed = (long)sqlite3_column_int64(rows, 12);
        int cost_evidenced = sqlite3_column_int64(rows, 13) > 0;
        char first_sent[40], reply[64], deal[32], deal_state[96], exchange[320], chain_stage[48];
        long entries, replies, deals;

        entries = relationships_count(s.outreach, "SELECT COUNT(*) FROM sends WHERE (offer_name = ?1 OR offer_name LIKE ?1 || '|%')", id);
        replies = relationships_count(s.outreach,
            "SELECT COUNT(*) FROM sends WHERE (offer_name = ?1 OR offer_name LIKE ?1 || '|%') AND reply_type IN ('positive','negative','neutral')", id);
        relationships_text(s.outreach, "SELECT MIN(sent_at) FROM sends WHERE (offer_name = ?1 OR offer_name LIKE ?1 || '|%')", id, first_sent, sizeof(first_sent));
        relationships_text(s.outreach,
            "SELECT reply_type FROM sends WHERE (offer_name = ?1 OR offer_name LIKE ?1 || '|%') AND reply_type IN ('positive','negative','neutral') "
            "ORDER BY replied_at DESC LIMIT 1", id, reply, sizeof(reply));
        deals = relationships_count(s.cms, "SELECT COUNT(*) FROM frappe_crm_crm_deal WHERE source = ?", id);
        relationships_text(s.cms, "SELECT id FROM frappe_crm_crm_deal WHERE source = ? ORDER BY id LIMIT 1", id, deal, sizeof(deal));
        relationships_text(s.cms, "SELECT COALESCE(communication_status,'') FROM frappe_crm_crm_deal WHERE source = ? ORDER BY id LIMIT 1",
                           id, deal_state, sizeof(deal_state));
        snprintf(exchange, sizeof(exchange), "opportunity:%s", id);
        relationships_text(s.commerce, "SELECT stage FROM commerce_progress WHERE exchange = ? ORDER BY rowid DESC LIMIT 1",
                           exchange, chain_stage, sizeof(chain_stage));

        const char *furthest = "specified";
        if (entries > 0) furthest = "sent";
        if (replies > 0) furthest = "replied";
        if (authority[0]) furthest = "scope_accepted";
        if (delivery[0]) furthest = "delivered";
        if (payment[0] && has_revenue) furthest = "paid";

        total++;
        if (entries > 0) sent++;
        if (replies > 0) replied++;
        if (has_price) { priced++; offered_cents += price; }
        if (cost_evidenced) costed++;
        if (deals > 0) with_deal++;
        if (chain_stage[0]) in_chain++;
        if (payment[0] && has_revenue) paid++;

        printf("%s\n{\"id\":", first ? "" : ",");
        first = 0;
        relationships_json_text(id);
        printf(",\"title\":"); relationships_json_text(title ? title : "");
        printf(",\"source\":"); relationships_json_text(source_kind ? source_kind : "");
        printf(",\"finance_stage\":"); relationships_json_text(stage ? stage : "");
        printf(",\"furthest\":\"%s\"", furthest);
        relationships_number_or_null("price_offered_cents", price);
        printf(",\"currency\":"); relationships_json_text(currency ? currency : "");
        printf(",\"cost_basis_evidenced\":%s", cost_evidenced ? "true" : "false");
        printf(",\"requirements\":{\"open\":%ld,\"closed\":%ld}", requirements - closed, closed);
        relationships_number_or_null("outreach_entries", entries);
        printf(",\"first_sent\":"); if (first_sent[0]) relationships_json_text(first_sent); else printf("null");
        relationships_number_or_null("replies_classified", replies);
        printf(",\"last_reply\":"); if (reply[0]) relationships_json_text(reply); else printf("null");
        printf(",\"crm_deal\":"); if (deal[0]) printf("%s", deal); else printf("null");
        printf(",\"crm_status\":"); if (deal_state[0]) relationships_json_text(deal_state); else printf("null");
        printf(",\"commerce_stage\":"); if (chain_stage[0]) relationships_json_text(chain_stage); else printf("null");
        printf(",\"gaps\":[");
        {
            int gap = 0;
            #define RELATIONSHIP_GAP(condition, text) if (condition) { printf("%s\"%s\"", gap++ ? "," : "", text); }
            RELATIONSHIP_GAP(s.outreach && entries == 0, "nothing recorded as sent")
            RELATIONSHIP_GAP(s.outreach && entries > 0 && replies == 0, "no reply classified")
            RELATIONSHIP_GAP(s.cms && deals == 0, "no CRM deal")
            RELATIONSHIP_GAP(s.cms && deals > 1, "more than one CRM deal")
            RELATIONSHIP_GAP(has_price && !cost_evidenced, "price offered with no evidenced cost, so no margin is known")
            RELATIONSHIP_GAP(!has_price, "no price stated")
            RELATIONSHIP_GAP(requirements == 0, "no requirements tracked")
            #undef RELATIONSHIP_GAP
        }
        printf("]}");
    }
    sqlite3_finalize(rows);

    /* What the other stores hold that finance does not know about. */
    long orphan_sends = relationships_count(s.outreach,
        "SELECT COUNT(*) FROM sends WHERE offer_name IS NULL OR offer_name = '' OR offer_name LIKE 'gmail:%'", NULL);
    long all_sends = relationships_count(s.outreach, "SELECT COUNT(*) FROM sends", NULL);
    long all_replies = relationships_count(s.outreach,
        "SELECT COUNT(*) FROM sends WHERE reply_type IS NOT NULL AND reply_type <> ''", NULL);
    long sourceless_deals = relationships_count(s.cms,
        "SELECT COUNT(*) FROM frappe_crm_crm_deal WHERE source IS NULL OR source = ''", NULL);

    printf("\n],\"totals\":{\"relationships\":%ld,\"sent\":%ld,\"replied\":%ld,\"priced\":%ld,\"price_offered_cents\":%ld,"
           "\"cost_basis_evidenced\":%ld,\"with_crm_deal\":%ld,\"in_commerce_chain\":%ld,\"paid\":%ld}",
           total, sent, replied, priced, offered_cents, costed, with_deal, in_chain, paid);
    printf(",\"outside_finance\":{");
    printf("\"outreach_entries\":"); if (all_sends < 0) printf("null"); else printf("%ld", all_sends);
    relationships_number_or_null("outreach_entries_with_no_opportunity", orphan_sends);
    relationships_number_or_null("outreach_replies_classified", all_replies);
    relationships_number_or_null("crm_deals_with_no_opportunity", sourceless_deals);
    printf("},\"reading\":\"a price offered is not an accepted scope, a sent message is not a reply, and an opportunity "
           "with no evidenced cost has no known margin\"}\n");

    if (s.outreach) sqlite3_close(s.outreach);
    if (s.cms) sqlite3_close(s.cms);
    if (s.commerce) sqlite3_close(s.commerce);
    return total > 0 ? 0 : 2;
}
