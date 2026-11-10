/* Model-token evidence in the existing Meter event store.
 *
 * A reconciled agent-usage export (Claude Code message usage, Codex token
 * counter deltas) becomes one physical-attempt event per observed row, in the
 * same events table and under the same replay law as every other Meter
 * receipt: an identical row replays, a changed payload under the same
 * attempt identity is refused. No new ledger, no rate, no price.
 *
 * What enters: provider, session, execution/run, request, message, event and
 * turn coordinates, model, effort, the counters the source reported, the
 * counter basis, a per-row digest and the export's file digest. What never
 * enters: the transcript path, any transcript text, any credential.
 *
 * Valuation is a separate read. It applies only per-token rates an operator
 * admitted with evidence (provider, op "<model>:<dimension>", unit token or
 * megatoken, effective_at) at the time each attempt happened, is labelled
 * provisional, and never becomes a charge, an invoice or a payment. An
 * attempt with any reported dimension lacking an admitted rate stays
 * unvalued; unknown is never zero.
 *
 * Attribution to account Work is exact or absent: an event counts toward a
 * task only when the owner-derived Work index names its provider session on a
 * run of that task, in the same account, for the same provider, and on no
 * other task. Nothing is joined by time, title or proximity. */
#ifndef BONFYRE_METER_TOKENS_H
#define BONFYRE_METER_TOKENS_H

#define MT_OP "model.tokens"
#define MT_SNAPSHOT "inherited_cumulative_snapshot"
#define MT_COLS 22

static const char *const mt_header[MT_COLS] = {
    "provider","session_id","execution_id","run_id","message_id","request_id","event_uuid","parent_uuid",
    "agent_id","timestamp","model","effort","speed","uncached_input_tokens","cached_input_tokens",
    "cache_write_input_tokens","input_tokens_including_cache","output_tokens","reasoning_output_tokens",
    "total_tokens","source_file","accounting_note"};

static void mt_schema(sqlite3 *db) {
    static const char *const cols[] = {
        "ALTER TABLE events ADD COLUMN model TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE events ADD COLUMN effort TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE events ADD COLUMN session_ref TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE events ADD COLUMN execution_ref TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE events ADD COLUMN request_ref TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE events ADD COLUMN message_ref TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE events ADD COLUMN turn_ref TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE events ADD COLUMN uncached_input_tokens INTEGER",
        "ALTER TABLE events ADD COLUMN cached_input_tokens INTEGER",
        "ALTER TABLE events ADD COLUMN cache_write_tokens INTEGER",
        "ALTER TABLE events ADD COLUMN output_tokens INTEGER",
        "ALTER TABLE events ADD COLUMN reasoning_tokens INTEGER",
        "ALTER TABLE events ADD COLUMN counter_basis TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE events ADD COLUMN source_digest TEXT NOT NULL DEFAULT ''"};
    for (size_t i = 0; i < sizeof(cols) / sizeof(cols[0]); i++) sqlite3_exec(db, cols[i], NULL, NULL, NULL);
    sqlite3_exec(db, "CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_ref) WHERE session_ref<>''", NULL, NULL, NULL);
}

/* ---------- import ---------- */

static int mt_field(const bf_csv_row_t *row, size_t i, char *out, size_t cap) {
    const bf_csv_field_t *f = bf_csv_field(row, i);
    if (!f || f->len >= cap) return 0;
    for (size_t k = 0; k < f->len; k++) if ((unsigned char)f->data[k] < 0x20 || f->data[k] == 0x7f) return 0;
    memcpy(out, f->data, f->len); out[f->len] = 0;
    return 1;
}

/* Non-negative decimal integer; empty is allowed only where the source may
 * legitimately not report the counter, and then means unknown. */
static int mt_count(const char *s, int optional, long long *out, int *present) {
    *present = 0; *out = 0;
    if (!*s) return optional;
    long long n = 0;
    for (const char *p = s; *p; p++) {
        if (*p < '0' || *p > '9' || n > (LLONG_MAX - 9) / 10) return 0;
        n = n * 10 + (*p - '0');
    }
    *out = n; *present = 1;
    return 1;
}

static int mt_ident(const char *s, int required) {
    if (!*s) return !required;
    for (; *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (!(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') &&
            c != '-' && c != '_' && c != '.' && c != ':' && c != '<' && c != '>') return 0;
    }
    return 1;
}

/* RFC3339 UTC with optional fraction: YYYY-MM-DDTHH:MM:SS[.fff]Z */
static int mt_time(const char *s) {
    size_t n = strlen(s);
    if (n < 20 || n > 32 || s[n - 1] != 'Z') return 0;
    char head[21]; memcpy(head, s, 19); head[19] = 'Z'; head[20] = 0;
    if (!meter_packet_at(head)) return 0;
    if (n == 20) return 1;
    if (s[19] != '.') return 0;
    for (size_t i = 20; i < n - 1; i++) if (s[i] < '0' || s[i] > '9') return 0;
    return n > 21;
}

typedef struct { long long rows, imported, replayed, conflicts, malformed, nonadditive; } MtReport;

static int cmd_token_import(const char *db_path, const char *source, const char *subject) {
    MtReport rep = {0};
    char file_digest[65];
    FILE *fp = NULL; bf_csv_reader_t *rd = NULL; sqlite3 *db = NULL;
    sqlite3_stmt *find = NULL, *ins = NULL, *seen = NULL;
    int rc = 1, began = 0;
    if (!subject || !*subject || strlen(subject) > 128 || !mt_ident(subject, 1)) {
        fprintf(stderr, "[meter] token-import requires --subject naming the account that owns this usage\n"); return 2;
    }
    if (!source || bf_sha256_file(source, file_digest) != 0 || !(fp = fopen(source, "rb"))) {
        fprintf(stderr, "[meter] token-import cannot read the usage export\n"); return 2;
    }
    bf_csv_opts_t opts = {',', 0};
    if (!(rd = bf_csv_reader_open(fp, &opts)) || !(db = db_open(db_path))) goto done;
    ensure_schema(db); mt_schema(db);
    if (sqlite3_exec(db, "CREATE TEMP TABLE IF NOT EXISTS mt_seen(digest TEXT PRIMARY KEY,n INTEGER NOT NULL)", NULL, NULL, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT receipt_ref,key_id,op,timestamp,provider,realization_ref FROM events WHERE physical_attempt_ref=?1 AND physical_attempt_ref<>''", -1, &find, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "INSERT INTO temp.mt_seen VALUES(?1,0) ON CONFLICT(digest) DO UPDATE SET n=n+1 RETURNING n", -1, &seen, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db,
            "INSERT INTO events(key_id,op,bytes,duration_ms,timestamp,cost,provider,receipt_ref,physical_attempt_ref,realization_ref,"
            "model,effort,session_ref,execution_ref,request_ref,message_ref,turn_ref,uncached_input_tokens,cached_input_tokens,"
            "cache_write_tokens,output_tokens,reasoning_tokens,counter_basis,source_digest) "
            "VALUES(?1,'" MT_OP "',0,0,?2,NULL,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)", -1, &ins, NULL) != SQLITE_OK ||
        sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) goto done;
    began = 1;
    const bf_csv_row_t *row;
    int header_checked = 0;
    while ((row = bf_csv_next_row(rd))) {
        /* The first row is the header, read as a row: the reader's retained
         * header fields point into a buffer the next row overwrites. */
        if (!header_checked) {
            if (row->count != MT_COLS) goto bad_header;
            for (size_t i = 0; i < MT_COLS; i++)
                if (row->fields[i].len != strlen(mt_header[i]) || memcmp(row->fields[i].data, mt_header[i], row->fields[i].len)) goto bad_header;
            header_checked = 1;
            continue;
        }
        rep.rows++;
        char f[MT_COLS][160];
        long long v[7]; int has[7];
        int ok = row->count == MT_COLS;
        for (size_t i = 0; ok && i < MT_COLS; i++) ok = (i == 20) ? 1 : mt_field(row, i, f[i], sizeof(f[i]));
        /* 13..19: uncached, cached, cache-write, including-cache, output, reasoning (optional), total */
        for (int i = 0; ok && i < 7; i++) ok = mt_count(f[13 + i], i == 5, &v[i], &has[i]);
        const char *provider = ok ? f[0] : "";
        int claude = ok && !strcmp(provider, "claude"), codex = ok && !strcmp(provider, "codex");
        ok = ok && (claude || codex) && mt_time(f[9]) && *f[1] && *f[10];
        for (int i = 1; ok && i <= 12; i++) ok = (i == 9) || mt_ident(f[i], 0);
        /* Internal consistency of what the source reported, not a guess at it. */
        ok = ok && v[0] <= LLONG_MAX / 4 && v[1] <= LLONG_MAX / 4 && v[2] <= LLONG_MAX / 4 && v[4] <= LLONG_MAX / 4 &&
             v[3] == v[0] + v[1] + v[2] && v[6] == v[3] + v[4];
        const char *basis = NULL;
        if (ok && claude && !strncmp(f[21], "deduplicated_message_id_and_request_id", 38)) basis = "message_usage";
        else if (ok && codex && !strcmp(f[21], "cumulative_counter_delta")) basis = "counter_delta";
        else if (ok && codex && !strncmp(f[21], "first_cumulative_snapshot", 25)) basis = MT_SNAPSHOT;
        if (!basis) { rep.malformed++; continue; }

        char canon[2048], row_digest[65], attempt[512], realization[256], receipt[96];
        int cn = snprintf(canon, sizeof(canon), "%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%s"
                          "\x1f%lld\x1f%lld\x1f%lld\x1f%lld\x1f%s\x1f%s",
                          f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8], f[9], f[10], f[11], f[12],
                          v[0], v[1], v[2], v[4], f[18], basis);
        if (cn < 0 || (size_t)cn >= sizeof(canon)) { rep.malformed++; continue; }
        bf_sha256_hex((const uint8_t *)canon, (size_t)cn, row_digest);
        if (claude) {
            if (*f[5] && *f[4]) snprintf(attempt, sizeof(attempt), "claude:request:%s:message:%s", f[5], f[4]);
            else if (*f[6]) snprintf(attempt, sizeof(attempt), "claude:event:%s", f[6]);
            else { rep.malformed++; continue; }
        } else {
            /* Codex token_count events carry no provider event id. The identity
             * is the observation itself: its coordinates, its digest and its
             * ordinal among byte-identical observations in this export. */
            long long n = 0;
            sqlite3_reset(seen); sqlite3_bind_text(seen, 1, row_digest, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(seen) != SQLITE_ROW) goto done;
            n = sqlite3_column_int64(seen, 0);
            while (sqlite3_step(seen) == SQLITE_ROW) {}
            snprintf(attempt, sizeof(attempt), "codex:session:%s:execution:%s:at:%s:obs:%.16s:%lld", f[1], f[2], f[9], row_digest, n);
        }
        snprintf(realization, sizeof(realization), "session:%s:%s", provider, f[1]);
        snprintf(receipt, sizeof(receipt), "usage-row:sha256:%s", row_digest);
        if (!strcmp(basis, MT_SNAPSHOT)) rep.nonadditive++;

        sqlite3_reset(find); sqlite3_bind_text(find, 1, attempt, -1, SQLITE_TRANSIENT);
        int step = sqlite3_step(find);
        if (step == SQLITE_ROW) {
            int exact = same_text(sqlite3_column_text(find, 0), receipt) && same_text(sqlite3_column_text(find, 1), subject) &&
                        same_text(sqlite3_column_text(find, 2), MT_OP) && same_text(sqlite3_column_text(find, 3), f[9]) &&
                        same_text(sqlite3_column_text(find, 4), provider) && same_text(sqlite3_column_text(find, 5), realization);
            if (exact) rep.replayed++; else rep.conflicts++;
            continue;
        }
        if (step != SQLITE_DONE) goto done;
        const char *turn = claude ? f[6] : f[3];
        const char *texts[] = {subject, f[9], provider, receipt, attempt, realization, f[10], f[11], f[1], f[2], f[5], f[4], turn};
        sqlite3_reset(ins); sqlite3_clear_bindings(ins);
        for (int i = 0; i < 13; i++) sqlite3_bind_text(ins, i + 1, texts[i], -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(ins, 14, v[0]); sqlite3_bind_int64(ins, 15, v[1]);
        sqlite3_bind_int64(ins, 16, v[2]); sqlite3_bind_int64(ins, 17, v[4]);
        if (has[5]) sqlite3_bind_int64(ins, 18, v[5]); else sqlite3_bind_null(ins, 18);
        sqlite3_bind_text(ins, 19, basis, -1, SQLITE_STATIC);
        sqlite3_bind_text(ins, 20, file_digest, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(ins) != SQLITE_DONE) goto done;
        rep.imported++;
    }
    if (!header_checked) goto bad_header;
    printf("source_digest\tsha256:%s\nrows\t%lld\nimported\t%lld\nreplayed\t%lld\nconflicts\t%lld\nmalformed\t%lld\n"
           "nonadditive_snapshots\t%lld\nraw_source\tnot_retained\n",
           file_digest, rep.rows, rep.imported, rep.replayed, rep.conflicts, rep.malformed, rep.nonadditive);
    if (rep.conflicts || rep.malformed) {
        /* All or nothing: a partial import would make a later replay look
         * like new usage and hide which rows disagreed. */
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); began = 0;
        fprintf(stderr, "[meter] refused: %lld conflicting and %lld malformed rows; nothing was recorded\n", rep.conflicts, rep.malformed);
        rc = 2; goto done;
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto done;
    began = 0; rc = 0;
    goto done;
bad_header:
    fprintf(stderr, "[meter] refused: not a reconciled agent-usage export (header mismatch)\n");
    rc = 2;
done:
    if (rc == 1) fprintf(stderr, "[meter] token-import failed: %s\n", db ? sqlite3_errmsg(db) : "store unavailable");
    if (began) sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    sqlite3_finalize(find); sqlite3_finalize(ins); sqlite3_finalize(seen);
    if (db) sqlite3_close(db);
    if (rd) bf_csv_reader_free(rd);
    if (fp) fclose(fp);
    return rc;
}

/* ---------- readback ---------- */

/* Per-token USD for one dimension of one event, from an admitted rate in
 * effect when the attempt happened; NULL when none was admitted. */
#define MT_RATE(dim) \
    "(SELECT r.amount/CASE r.unit WHEN 'token' THEN 1.0 ELSE 1000000.0 END FROM rate r WHERE r.provider=e.provider " \
    "AND r.op=e.model||':" dim "' AND r.unit IN('token','megatoken') AND r.currency='USD' AND length(r.evidence_ref)>0 " \
    "AND r.amount>=0 AND julianday(r.effective_at)<=julianday(e.ts) ORDER BY julianday(r.effective_at) DESC,r.id DESC LIMIT 1)"

/* Prints per-model counters and the provisional valuation for the events the
 * filter admits. The filter is static SQL over `events` with numbered
 * parameters ?1.. bound by the caller-supplied binder. */
static int mt_report(sqlite3 *db, const char *filter, const char *const *params, int nparams) {
    char *sql = NULL; sqlite3_stmt *q = NULL; int ok = 0;
    long long tot[8] = {0};
    sql = sqlite3_mprintf(
        "SELECT provider,model,count(*),sum(uncached_input_tokens),sum(cached_input_tokens),sum(cache_write_tokens),"
        "sum(output_tokens),sum(reasoning_tokens),count(reasoning_tokens),"
        "sum(CASE WHEN counter_basis<>'" MT_SNAPSHOT "' THEN uncached_input_tokens+cached_input_tokens+cache_write_tokens+output_tokens ELSE 0 END),"
        "sum(CASE WHEN counter_basis='" MT_SNAPSHOT "' THEN uncached_input_tokens+cached_input_tokens+cache_write_tokens+output_tokens ELSE 0 END),"
        "count(DISTINCT session_ref) FROM events WHERE op='" MT_OP "' AND (%s) GROUP BY provider,model ORDER BY provider,model", filter);
    if (!sql || sqlite3_prepare_v2(db, sql, -1, &q, NULL) != SQLITE_OK) goto done;
    for (int i = 0; i < nparams; i++) sqlite3_bind_text(q, i + 1, params[i], -1, SQLITE_TRANSIENT);
    fputs("#model\tprovider\tmodel\trecords\tsessions\tuncached_input\tcached_input\tcache_write\toutput\treasoning\treasoning_unreported_records\tadditive_tokens\tnonadditive_snapshot_tokens\n", stdout);
    int s;
    while ((s = sqlite3_step(q)) == SQLITE_ROW) {
        long long rec = sqlite3_column_int64(q, 2), rsn = sqlite3_column_int64(q, 8);
        printf("model\t%s\t%s\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t", sqlite3_column_text(q, 0), sqlite3_column_text(q, 1), rec,
               sqlite3_column_int64(q, 11), sqlite3_column_int64(q, 3), sqlite3_column_int64(q, 4),
               sqlite3_column_int64(q, 5), sqlite3_column_int64(q, 6));
        if (rsn) printf("%lld", sqlite3_column_int64(q, 7)); else fputs("unknown", stdout);
        printf("\t%lld\t%lld\t%lld\n", rec - rsn, sqlite3_column_int64(q, 9), sqlite3_column_int64(q, 10));
        tot[0] += rec; tot[1] += sqlite3_column_int64(q, 9); tot[2] += sqlite3_column_int64(q, 10);
    }
    if (s != SQLITE_DONE) goto done;
    sqlite3_finalize(q); q = NULL; sqlite3_free(sql);
    printf("records\t%lld\nadditive_tokens\t%lld\nnonadditive_snapshot_tokens\t%lld\nsource_reported_tokens\t%lld\n",
           tot[0], tot[1], tot[2], tot[1] + tot[2]);
    sql = sqlite3_mprintf(
        "WITH e AS (SELECT provider,model,timestamp ts,uncached_input_tokens u,cached_input_tokens c,cache_write_tokens w,output_tokens o "
        "FROM events WHERE op='" MT_OP "' AND counter_basis<>'" MT_SNAPSHOT "' AND (%s)),"
        "p AS (SELECT u,c,w,o," MT_RATE("uncached_input") " ru," MT_RATE("cached_input") " rc," MT_RATE("cache_write") " rw," MT_RATE("output") " ro FROM e),"
        "v AS (SELECT *,(ru IS NOT NULL OR rc IS NOT NULL OR rw IS NOT NULL OR ro IS NOT NULL) AND (u=0 OR ru IS NOT NULL) AND (c=0 OR rc IS NOT NULL) "
        "AND (w=0 OR rw IS NOT NULL) AND (o=0 OR ro IS NOT NULL) AS ok FROM p) "
        "SELECT count(*),COALESCE(sum(ok),0),sum(CASE WHEN ok THEN u*COALESCE(ru,0)+c*COALESCE(rc,0)+w*COALESCE(rw,0)+o*COALESCE(ro,0) END) FROM v", filter);
    if (!sql || sqlite3_prepare_v2(db, sql, -1, &q, NULL) != SQLITE_OK) goto done;
    for (int i = 0; i < nparams; i++) sqlite3_bind_text(q, i + 1, params[i], -1, SQLITE_TRANSIENT);
    if (sqlite3_step(q) != SQLITE_ROW) goto done;
    long long additive = sqlite3_column_int64(q, 0), valued = sqlite3_column_int64(q, 1);
    printf("valuation_basis\tprovisional_admitted_rate_not_charged\nvalued_records\t%lld\nunvalued_records\t%lld\n"
           "nonadditive_records_not_valued\t%lld\nvaluation_coverage\t%s\n",
           valued, additive - valued, tot[0] - additive,
           !additive ? "no-additive-records" : !valued ? "none" : valued == additive ? "complete" : "partial");
    if (valued) printf("provisional_valuation_usd\t%.6f\n", sqlite3_column_double(q, 2));
    else fputs("provisional_valuation_usd\tunknown\n", stdout);
    fputs("charged_usd\tunknown_no_authoritative_ledger\nmoney_status\tunknown_not_inferred\n", stdout);
    ok = 1;
done:
    if (!ok) fprintf(stderr, "[meter] token readback failed: %s\n", sqlite3_errmsg(db));
    sqlite3_finalize(q); sqlite3_free(sql);
    return ok;
}

static sqlite3 *mt_open_ro(const char *meter_path) {
    sqlite3 *db = NULL;
    if (!meter_path || sqlite3_open_v2(meter_path, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return NULL;
    }
    sqlite3_busy_timeout(db, 1000);
    sqlite3_stmt *q = NULL; int has = 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM pragma_table_info('events') WHERE name='counter_basis'", -1, &q, NULL) == SQLITE_OK)
        has = sqlite3_step(q) == SQLITE_ROW;
    sqlite3_finalize(q);
    if (!has) { sqlite3_close(db); return NULL; }
    return db;
}

/* The Work index's session keys, read once: which task names each provider
 * session, and whether more than one task or provider claims it. A session
 * belongs to a task only when exactly one task and one provider name it. */
static int mt_attach_work(sqlite3 *db, const char *work_path) {
    sqlite3_stmt *s = NULL;
    char *uri = sqlite3_mprintf("file:%s?mode=ro", work_path);
    int ok = uri && sqlite3_prepare_v2(db, "ATTACH DATABASE ? AS work", -1, &s, NULL) == SQLITE_OK &&
             sqlite3_bind_text(s, 1, uri, -1, SQLITE_TRANSIENT) == SQLITE_OK && sqlite3_step(s) == SQLITE_DONE;
    sqlite3_finalize(s); sqlite3_free(uri);
    if (!ok) return 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM work.pragma_table_info('account_work_run') WHERE name='provider_session_ref'", -1, &s, NULL) != SQLITE_OK) return 0;
    ok = sqlite3_step(s) == SQLITE_ROW;
    sqlite3_finalize(s);
    return ok && sqlite3_exec(db,
        "CREATE TEMP TABLE mt_sess(ref TEXT PRIMARY KEY,work_id TEXT NOT NULL,n INTEGER NOT NULL,agent TEXT NOT NULL,na INTEGER NOT NULL);"
        "INSERT INTO temp.mt_sess SELECT provider_session_ref,min(work_id),count(DISTINCT work_id),min(agent),count(DISTINCT agent) "
        "FROM work.account_work_run WHERE provider_session_ref<>'' GROUP BY provider_session_ref;"
        "CREATE INDEX temp.mt_sess_work ON mt_sess(work_id);", NULL, NULL, NULL) == SQLITE_OK;
}

#define MT_EXACT "s.n=1 AND s.na=1"

static long long mt_scalar(sqlite3 *db, const char *sql, const char *const *params, int n) {
    sqlite3_stmt *q = NULL; long long v = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &q, NULL) == SQLITE_OK) {
        for (int i = 0; i < n; i++) sqlite3_bind_text(q, i + 1, params[i], -1, SQLITE_TRANSIENT);
        if (sqlite3_step(q) == SQLITE_ROW) v = sqlite3_column_int64(q, 0);
    }
    sqlite3_finalize(q);
    return v;
}

/* Account-wide token evidence, optionally with how much of it the account's
 * Work index can name exactly. Unmapped sessions stay unmapped. */
static int cmd_token_usage(const char *meter_path, const char *subject, const char *work_path, const char *scope) {
    sqlite3 *db = NULL; int rc = 1;
    if (!subject || !*subject) { fprintf(stderr, "[meter] token-usage requires --subject\n"); return 2; }
    if (!(db = mt_open_ro(meter_path))) { fprintf(stderr, "[meter] no token evidence in this store\n"); return 1; }
    const char *p[2] = {subject, scope ? scope : ""};
    printf("subject\t%s\n", subject);
    if (!mt_report(db, "key_id=?1", p, 1)) goto done;
    if (work_path) {
        if (!scope || !*scope || *work_path != '/' || !mt_attach_work(db, work_path)) {
            fprintf(stderr, "[meter] the account Work index is unavailable or carries no provider session key\n"); rc = 2; goto done;
        }
        long long all = mt_scalar(db, "SELECT count(DISTINCT provider||':'||session_ref) FROM events WHERE op='" MT_OP "' AND key_id=?1", p, 1);
        long long exact = mt_scalar(db,
            "SELECT count(DISTINCT e.provider||':'||e.session_ref) FROM events e JOIN temp.mt_sess s ON s.ref=e.session_ref "
            "JOIN work.account_work a ON a.id=s.work_id WHERE e.op='" MT_OP "' AND e.key_id=?1 AND a.subject=?1 AND a.scope=?2 "
            "AND " MT_EXACT " AND s.agent=e.provider", p, 2);
        long long named = mt_scalar(db,
            "SELECT count(DISTINCT e.provider||':'||e.session_ref) FROM events e JOIN temp.mt_sess s ON s.ref=e.session_ref "
            "WHERE e.op='" MT_OP "' AND e.key_id=?1", p, 1);
        long long tasks = mt_scalar(db,
            "SELECT count(DISTINCT s.work_id) FROM temp.mt_sess s JOIN work.account_work a ON a.id=s.work_id WHERE a.subject=?1 AND a.scope=?2 "
            "AND " MT_EXACT " AND EXISTS(SELECT 1 FROM events e WHERE e.session_ref=s.ref AND e.op='" MT_OP "' AND e.key_id=?1 AND e.provider=s.agent)", p, 2);
        if (all < 0 || exact < 0 || named < 0 || tasks < 0) goto done;
        printf("sessions\t%lld\nsessions_exact_task\t%lld\nsessions_named_not_exact_refused\t%lld\nsessions_unmapped\t%lld\ntasks_with_token_evidence\t%lld\n",
               all, exact, named - exact, all - named, tasks);
    }
    rc = 0;
done:
    sqlite3_close(db);
    return rc;
}

/* Token evidence for one task of one account, through exact session keys. */
static int cmd_work_tokens(const char *meter_path, const char *work_path, const char *subject, const char *scope, const char *work_id) {
    sqlite3 *db = NULL; int rc = 1;
    if (!work_path || *work_path != '/' || !subject || !*subject || !scope || !*scope || !work_id || !*work_id) return 2;
    if (!(db = mt_open_ro(meter_path))) { fprintf(stderr, "[meter] no token evidence in this store\n"); return 1; }
    if (!mt_attach_work(db, work_path)) {
        fprintf(stderr, "[meter] the account Work index is unavailable or carries no provider session key\n"); rc = 2; goto done;
    }
    const char *p[3] = {work_id, subject, scope};
    if (mt_scalar(db, "SELECT count(*) FROM work.account_work WHERE id=?1 AND subject=?2 AND scope=?3", p, 3) != 1) {
        fprintf(stderr, "[meter] refused: task is not evidenced in the requested account scope\n"); rc = 2; goto done;
    }
#define MT_RUNS "FROM work.account_work_run r JOIN temp.mt_sess s ON s.ref=r.provider_session_ref WHERE r.work_id=?1 "
#define MT_HAS_EVIDENCE "EXISTS(SELECT 1 FROM events e WHERE e.session_ref<>'' AND e.session_ref=s.ref AND e.op='" MT_OP "' AND e.key_id=?2 AND e.provider=s.agent)"
    long long runs = mt_scalar(db, "SELECT count(*) FROM work.account_work_run WHERE work_id=?1", p, 1);
    long long keyed = mt_scalar(db, "SELECT count(*) FROM work.account_work_run WHERE work_id=?1 AND provider_session_ref<>''", p, 1);
    long long exclusive = mt_scalar(db, "SELECT count(DISTINCT s.ref) " MT_RUNS "AND " MT_EXACT, p, 1);
    long long shared = mt_scalar(db, "SELECT count(DISTINCT s.ref) " MT_RUNS "AND NOT (" MT_EXACT ")", p, 1);
    long long evidenced = mt_scalar(db, "SELECT count(DISTINCT s.ref) " MT_RUNS "AND " MT_EXACT " AND " MT_HAS_EVIDENCE, p, 2);
    long long recovered = mt_scalar(db, "SELECT count(*) " MT_RUNS "AND r.state='failed' AND " MT_EXACT " AND " MT_HAS_EVIDENCE, p, 2);
    if (runs < 0 || keyed < 0 || exclusive < 0 || shared < 0 || evidenced < 0 || recovered < 0) goto done;
    printf("task\t%s\nruns\t%lld\nruns_without_session_key\t%lld\nsessions_exact\t%lld\nsessions_shared_refused\t%lld\n"
           "sessions_with_token_evidence\t%lld\nsessions_without_token_evidence\t%lld\nfailed_runs_with_token_evidence\t%lld\n",
           work_id, runs, runs - keyed, exclusive, shared, evidenced, exclusive - evidenced, recovered);
    /* Keep event ids in rowid order, as in the account-key scan, so the
     * report and floating-point valuation retain their prior input order. */
    sqlite3_stmt *select_events = NULL;
    const char *select_sql =
        "INSERT INTO temp.mt_work_event_ids SELECT e.id FROM temp.mt_sess s CROSS JOIN events e "
        "WHERE s.work_id=?1 AND " MT_EXACT " AND e.session_ref<>'' AND e.session_ref=s.ref "
        "AND e.provider=s.agent AND e.key_id=?2 AND e.op='" MT_OP "'";
    if (sqlite3_exec(db,"CREATE TEMP TABLE mt_work_event_ids(id INTEGER PRIMARY KEY)",NULL,NULL,NULL)!=SQLITE_OK ||
        sqlite3_prepare_v2(db,select_sql,-1,&select_events,NULL)!=SQLITE_OK) {
        sqlite3_finalize(select_events); goto done;
    }
    sqlite3_bind_text(select_events,1,work_id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(select_events,2,subject,-1,SQLITE_TRANSIENT);
    int selected = sqlite3_step(select_events)==SQLITE_DONE;
    sqlite3_finalize(select_events);
    if (!selected || !mt_report(db,"key_id=?1 AND id IN(SELECT id FROM temp.mt_work_event_ids)", p+1, 1)) goto done;
    fputs("allocation_status\tnot_evaluated\n", stdout);
    rc = 0;
done:
    sqlite3_close(db);
    return rc;
}

#endif
