/* ── books ────────────────────────────────────────────────────────────────── */
/* Included by main.c.
 *
 * Derives a double-entry journal and a trial balance from what the estate has
 * already recorded: invoices, payments and credits in a pay store, and model
 * costs in an economy store. Nothing is posted by hand and nothing is written:
 * every line is one source row, read-only, and names that row.
 *
 *   invoice   debit receivable            credit revenue_invoiced
 *   payment   debit cash_evidenced        credit receivable
 *   credit    debit revenue_adjustment    credit receivable
 *   cost      debit model_cost            credit cost_owed_or_paid_unknown
 *
 * A payment row carries the reference that shows the money arrived, which the
 * pay organ requires. A cost row records what a model call cost; whether that
 * was ever billed or paid is not recorded anywhere, so its credit side says
 * so instead of naming cash or a supplier. Amounts are millionths of a dollar
 * so that cents and fractional model costs add exactly.
 *
 * What this is not: there is no tax, no currency other than the one the
 * sources imply, no period close and no chart of accounts beyond these. */

#define BOOKS_ACCOUNTS 7
static const char *const BOOKS_ACCOUNT[BOOKS_ACCOUNTS] = {
    "receivable", "revenue_invoiced", "cash_evidenced", "revenue_adjustment",
    "model_cost", "cost_owed_or_paid_unknown", NULL
};
enum { BK_RECEIVABLE, BK_REVENUE, BK_CASH, BK_ADJUSTMENT, BK_COST, BK_COST_UNKNOWN };

typedef struct {
    long long debit[BOOKS_ACCOUNTS];
    long long credit[BOOKS_ACCOUNTS];
    long entries;
    int journal;
} Books;

static void books_post(Books *b, int debit_account, int credit_account, long long micros,
                       const char *source, long long id, const char *detail) {
    b->debit[debit_account] += micros;
    b->credit[credit_account] += micros;
    b->entries++;
    if (b->journal)
        printf("%s:%lld\t%s\t%s\t%lld\t%s\n", source, id, BOOKS_ACCOUNT[debit_account],
               BOOKS_ACCOUNT[credit_account], micros, detail ? detail : "");
}

/* Posts every row a query returns: column 0 an id, column 1 an amount, column
 * 2 an optional detail. scale turns the amount into millionths of a dollar. */
static int books_read(Books *b, sqlite3 *store, const char *sql, double scale, int debit_account,
                      int credit_account, const char *source, long *count) {
    sqlite3_stmt *rows = NULL;
    *count = 0;
    if (sqlite3_prepare_v2(store, sql, -1, &rows, NULL) != SQLITE_OK) return -1;
    while (sqlite3_step(rows) == SQLITE_ROW) {
        double amount = sqlite3_column_double(rows, 1) * scale;
        long long micros = (long long)(amount < 0 ? amount - 0.5 : amount + 0.5);
        books_post(b, debit_account, credit_account, micros, source, sqlite3_column_int64(rows, 0),
                   (const char *)sqlite3_column_text(rows, 2));
        (*count)++;
    }
    sqlite3_finalize(rows);
    return 0;
}

static int cmd_books(int argc, char **argv) {
    const char *pay_path = arg_get(argc, argv, "--pay-db");
    const char *economy_path = arg_get(argc, argv, "--economy-db");
    Books b;
    long invoices = 0, payments = 0, credits = 0, costs = 0, payments_without_reference = 0;
    memset(&b, 0, sizeof(b));
    for (int i = 0; i < argc; i++) if (!strcmp(argv[i], "--journal")) b.journal = 1;
    if (!pay_path && !economy_path) {
        fprintf(stderr, "Usage: bonfyre-finance books [--pay-db FILE] [--economy-db FILE] [--journal]\n"
                        "  at least one store is needed; there are no books without records\n");
        return 1;
    }
    if (b.journal) printf("source\tdebit\tcredit\tmicro_usd\tdetail\n");

    if (pay_path) {
        sqlite3 *pay = NULL;
        sqlite3_stmt *q = NULL;
        if (sqlite3_open_v2(pay_path, &pay, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
            books_read(&b, pay, "SELECT id, amount_cents, api_key || ' ' || period_start FROM invoices ORDER BY id",
                       10000.0, BK_RECEIVABLE, BK_REVENUE, "invoice", &invoices) != 0 ||
            books_read(&b, pay, "SELECT id, amount_cents, 'invoice ' || invoice_id || ' ref ' || COALESCE(reference,'') "
                                "FROM payments ORDER BY id",
                       10000.0, BK_CASH, BK_RECEIVABLE, "payment", &payments) != 0 ||
            books_read(&b, pay, "SELECT id, amount_cents, reason FROM credits ORDER BY id",
                       10000.0, BK_ADJUSTMENT, BK_RECEIVABLE, "credit", &credits) != 0) {
            fprintf(stderr, "books: %s is not a readable pay store\n", pay_path);
            if (pay) sqlite3_close(pay);
            return 1;
        }
        if (sqlite3_prepare_v2(pay, "SELECT COUNT(*) FROM payments WHERE reference IS NULL OR reference=''",
                               -1, &q, NULL) == SQLITE_OK && sqlite3_step(q) == SQLITE_ROW)
            payments_without_reference = (long)sqlite3_column_int64(q, 0);
        if (q) sqlite3_finalize(q);
        sqlite3_close(pay);
    }
    if (economy_path) {
        sqlite3 *economy = NULL;
        if (sqlite3_open_v2(economy_path, &economy, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
            books_read(&b, economy, "SELECT id, usd, recipe || '/' || stage || '/' || model FROM costs ORDER BY id",
                       1000000.0, BK_COST, BK_COST_UNKNOWN, "cost", &costs) != 0) {
            fprintf(stderr, "books: %s is not a readable economy store\n", economy_path);
            if (economy) sqlite3_close(economy);
            return 1;
        }
        sqlite3_close(economy);
    }
    if (b.journal) return b.entries > 0 ? 0 : 2;

    long long total_debit = 0, total_credit = 0;
    printf("{\"kind\":\"books\",\"unit\":\"micro_usd\",\"sources\":{\"invoices\":%ld,\"payments\":%ld,"
           "\"credits\":%ld,\"costs\":%ld},\"entries\":%ld,\"accounts\":{",
           invoices, payments, credits, costs, b.entries);
    for (int a = 0; BOOKS_ACCOUNT[a]; a++) {
        printf("%s\"%s\":{\"debit\":%lld,\"credit\":%lld}", a ? "," : "", BOOKS_ACCOUNT[a], b.debit[a], b.credit[a]);
        total_debit += b.debit[a];
        total_credit += b.credit[a];
    }
    printf("},\"total_debit\":%lld,\"total_credit\":%lld,\"balanced\":%s,"
           "\"receivable_outstanding\":%lld,\"cash_evidenced\":%lld,\"model_cost_recorded\":%lld,"
           "\"payments_without_a_reference\":%ld,"
           "\"not_known_from_these_records\":[\"whether any recorded model cost was billed or paid\","
           "\"tax\",\"anything not in the stores named\"]",
           total_debit, total_credit, total_debit == total_credit ? "true" : "false",
           b.debit[BK_RECEIVABLE] - b.credit[BK_RECEIVABLE], b.debit[BK_CASH] - b.credit[BK_CASH],
           b.debit[BK_COST] - b.credit[BK_COST], payments_without_reference);

    /* What the estate has recorded about money that is not on the books: a
     * quote received is not a bill, a stated pledge is not a receipt, an
     * expected cost is not a cost. These are counted from the content store
     * and the finance store so they are seen beside the books, and none of
     * them is posted. A store that does not hold a kind of record says so
     * with null. */
    const char *cms_path = arg_get(argc, argv, "--cms-db");
    const char *finance_path = arg_get(argc, argv, "--finance-db");
    if (cms_path || finance_path) {
        static const struct { const char *name; int in_cms; const char *sql; } MEMO[] = {
            { "requests_for_quotation_drafted", 1, "SELECT COUNT(*) FROM frappe_erpnext_request_for_quotation" },
            { "supplier_quotations_received", 1, "SELECT COUNT(*) FROM frappe_erpnext_supplier_quotation" },
            { "supplier_quotations_total_usd_cents", 1,
              "SELECT CAST(ROUND(COALESCE(SUM(total),0)*100) AS INTEGER) FROM frappe_erpnext_supplier_quotation WHERE currency='USD'" },
            /* Review unexplained differences on each draft quotation. The
             * total/grand difference is legitimate when tax or adjustments
             * are declared; do not infer a booked purchase from either. */
            { "supplier_quotations_draft", 1, "SELECT COUNT(*) FROM frappe_erpnext_supplier_quotation WHERE status='draft'" },
            { "supplier_quotations_unexplained_amount_difference", 1,
              "SELECT COUNT(*) FROM frappe_erpnext_supplier_quotation WHERE total IS NOT NULL AND grand_total IS NOT NULL "
              "AND ABS(CAST(total AS REAL)-CAST(grand_total AS REAL))>0.01 "
              "AND COALESCE(total_taxes_and_charges,0)=0 AND COALESCE(taxes_and_charges_added,0)=0 "
              "AND COALESCE(taxes_and_charges_deducted,0)=0 AND COALESCE(discount_amount,0)=0 "
              "AND COALESCE(additional_discount_percentage,0)=0 AND COALESCE(rounding_adjustment,0)=0" },
            { "deals_open", 1, "SELECT COUNT(*) FROM frappe_crm_crm_deal" },
            { "support_stated_usd_cents", 1,
              "SELECT CAST(ROUND(COALESCE(SUM(amount_stated),0)*100) AS INTEGER) FROM project_support WHERE currency='USD' AND mode='stated'" },
            { "support_received_with_a_receipt", 1,
              "SELECT COUNT(*) FROM project_support WHERE receipt_ref IS NOT NULL AND receipt_ref<>''" },
            { "bounties", 1, "SELECT COUNT(*) FROM bounty" },
            { "bounties_with_a_stated_amount", 1, "SELECT COUNT(*) FROM bounty WHERE amount_stated IS NOT NULL AND amount_stated>0" },
            { "funding_work_packages", 1, "SELECT COUNT(*) FROM funding_work_package" },
            { "opportunities", 0, "SELECT COUNT(*) FROM opportunities" },
            { "opportunities_with_stated_compensation", 0, "SELECT COUNT(*) FROM opportunities WHERE compensation_cents IS NOT NULL" },
            { "opportunities_expected_cost_cents", 0, "SELECT COALESCE(SUM(expected_compute_cents + expected_external_cents),0) FROM opportunities" },
        };
        sqlite3 *cms = NULL, *finance = NULL;
        if (cms_path && sqlite3_open_v2(cms_path, &cms, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
            if (cms) sqlite3_close(cms);
            cms = NULL;
        }
        if (finance_path && sqlite3_open_v2(finance_path, &finance, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
            if (finance) sqlite3_close(finance);
            finance = NULL;
        }
        printf(",\"recorded_and_not_on_the_books\":{");
        for (size_t i = 0; i < sizeof(MEMO) / sizeof(MEMO[0]); i++) {
            sqlite3 *store = MEMO[i].in_cms ? cms : finance;
            sqlite3_stmt *q = NULL;
            printf("%s\"%s\":", i ? "," : "", MEMO[i].name);
            if (store && sqlite3_prepare_v2(store, MEMO[i].sql, -1, &q, NULL) == SQLITE_OK && sqlite3_step(q) == SQLITE_ROW)
                printf("%lld", (long long)sqlite3_column_int64(q, 0));
            else
                printf("null");
            if (q) sqlite3_finalize(q);
        }
        printf(",\"reading\":\"a quotation received is not a bill, a stated pledge is not a receipt, an expected cost is not a cost\"}");
        if (cms) sqlite3_close(cms);
        if (finance) sqlite3_close(finance);
    }
    printf("}\n");
    return b.entries > 0 ? 0 : 2;
}
