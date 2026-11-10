/* Read-only cross-owner commercial projection. Finance owns the opportunity,
 * CMS owns its Frappe CRM projection, Outreach owns its local send log.
 * No stage, payment, quote, delivery or contact is invented here.
 * Caller MUST supply all state coordinates; never create shadow stores. */
static void zc_inspect_json_string(const unsigned char *text) {
    const unsigned char *p = text ? text : (const unsigned char *)"";
    putchar('"');
    for (; *p; ++p) {
        if (*p == '"' || *p == '\\') { putchar('\\'); putchar(*p); }
        else if (*p == '\n') fputs("\\n", stdout);
        else if (*p == '\r') fputs("\\r", stdout);
        else if (*p == '\t') fputs("\\t", stdout);
        else if (*p < 0x20) printf("\\u%04x", *p);
        else putchar(*p);
    }
    putchar('"');
}

static void zc_inspect_number(sqlite3_stmt *st, int col) {
    if (sqlite3_column_type(st, col) == SQLITE_NULL) fputs("null", stdout);
    else printf("%lld", (long long)sqlite3_column_int64(st, col));
}

static int zc_inspect_opportunity(sqlite3 *finance, const char *id,
                                  const char *cms_path, const char *outreach_path) {
    sqlite3 *cms = NULL, *outreach = NULL;
    sqlite3_stmt *op = NULL, *req = NULL, *crm = NULL, *send = NULL;
    int status = 1, total = 0, closed = 0, cost_basis = 0, first;
    if (!id || !*id || !cms_path || !*cms_path || !outreach_path || !*outreach_path) {
        fputs("inspect requires --id, --cms-db and --outreach-db\n", stderr);
        return 2;
    }
    if (sqlite3_open_v2(cms_path, &cms, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        sqlite3_open_v2(outreach_path, &outreach, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fputs("inspect: at least one named read-only owner store is unavailable\n", stderr);
        goto done;
    }
    if (sqlite3_prepare_v2(finance,
            "SELECT title,stage,compensation_cents,source_ref,actual_revenue_cents,"
            "expected_compute_cents+expected_external_cents FROM opportunities WHERE id=?",
            -1, &op, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(finance,
            "SELECT count(*),coalesce(sum(state='satisfied'),0),"
            "coalesce(max(kind='material' AND requirement_key='cost_basis_confirmed' "
            "AND state='satisfied' AND evidence_ref NOT IN ('','-')),0) "
            "FROM opportunity_requirements WHERE opportunity_id=?",
            -1, &req, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(cms,
            "SELECT id,organization_name,email,communication_status,next_step "
            "FROM frappe_crm_crm_deal WHERE source=? AND namespace='aurekai-commercial' ORDER BY id",
            -1, &crm, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(outreach,
            "SELECT id,target,offer_name,reply_type FROM sends "
            "WHERE offer_name=?1 OR "
            "substr(coalesce(offer_name,''),1,length(?1)+1)=?1||'|' ORDER BY id",
            -1, &send, NULL) != SQLITE_OK) {
        fputs("inspect: an owner store does not expose the expected contract\n", stderr);
        goto done;
    }
    sqlite3_bind_text(op, 1, id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(req, 1, id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(crm, 1, id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(send, 1, id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(op) != SQLITE_ROW) {
        fprintf(stderr, "inspect: finance opportunity not found: %s\n", id);
        goto done;
    }
    if (sqlite3_step(req) != SQLITE_ROW) {
        fputs("inspect: requirements cannot be read\n", stderr);
        goto done;
    }
    total = sqlite3_column_int(req, 0);
    closed = sqlite3_column_int(req, 1);
    cost_basis = sqlite3_column_int(req, 2);
    fputs("{\"kind\":\"bonfyre.finance.opportunity_projection.v1\",\"id\":", stdout);
    zc_inspect_json_string((const unsigned char *)id);
    fputs(",\"title\":", stdout); zc_inspect_json_string(sqlite3_column_text(op, 0));
    fputs(",\"finance_stage\":", stdout); zc_inspect_json_string(sqlite3_column_text(op, 1));
    fputs(",\"listed_compensation_cents\":", stdout); zc_inspect_number(op, 2);
    fputs(",\"source_ref\":", stdout); zc_inspect_json_string(sqlite3_column_text(op, 3));
    fputs(",\"actual_revenue_cents\":", stdout); zc_inspect_number(op, 4);
    printf(",\"cost_basis_confirmed\":%s,\"estimated_margin_cents\":",
           cost_basis ? "true" : "false");
    if (cost_basis && sqlite3_column_type(op, 2) != SQLITE_NULL)
        printf("%lld", (long long)(sqlite3_column_int64(op, 2) - sqlite3_column_int64(op, 5)));
    else fputs("null", stdout);
    printf(",\"requirements\":{\"total\":%d,\"satisfied\":%d},\"crm\":[", total, closed);
    first = 1;
    while (sqlite3_step(crm) == SQLITE_ROW) {
        if (!first) putchar(','); first = 0;
        fputs("{\"native_id\":", stdout); zc_inspect_number(crm, 0);
        fputs(",\"organization\":", stdout); zc_inspect_json_string(sqlite3_column_text(crm, 1));
        fputs(",\"email\":", stdout); zc_inspect_json_string(sqlite3_column_text(crm, 2));
        fputs(",\"communication_status\":", stdout); zc_inspect_json_string(sqlite3_column_text(crm, 3));
        fputs(",\"next_step\":", stdout); zc_inspect_json_string(sqlite3_column_text(crm, 4));
        putchar('}');
    }
    fputs("],\"outreach\":[", stdout);
    first = 1;
    while (sqlite3_step(send) == SQLITE_ROW) {
        if (!first) putchar(','); first = 0;
        fputs("{\"local_entry_id\":", stdout); zc_inspect_number(send, 0);
        fputs(",\"target\":", stdout); zc_inspect_json_string(sqlite3_column_text(send, 1));
        fputs(",\"offer_ref\":", stdout); zc_inspect_json_string(sqlite3_column_text(send, 2));
        fputs(",\"reply_type\":", stdout);
        if (sqlite3_column_type(send, 3) == SQLITE_NULL) fputs("null", stdout);
        else zc_inspect_json_string(sqlite3_column_text(send, 3));
        putchar('}');
    }
    fputs("],\"limitations\":[\"native Outreach stores local log entries, not transport proof\","
          "\"CRM record is a local projection, not external ERPNext execution\","
          "\"a stated price is not an accepted order, incurred cost, invoice or settlement\"]}\n",
          stdout);
    status = 0;
done:
    sqlite3_finalize(op); sqlite3_finalize(req); sqlite3_finalize(crm); sqlite3_finalize(send);
    if (cms) sqlite3_close(cms);
    if (outreach) sqlite3_close(outreach);
    return status;
}
