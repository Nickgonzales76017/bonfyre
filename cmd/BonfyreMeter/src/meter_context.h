/* Context handoff comparison over Meter's existing token evidence.
 *
 * A developer task is run twice by the account's agents: once on its ordinary
 * context (baseline) and once on a compact handoff that BonfyreKVCache compiled
 * and `context-verify` admitted (compact). This read puts the two physical runs
 * side by side in the provider's own counter classes, so a reduction is seen
 * where it costs money -- new uncached input and cache writes -- and is not
 * claimed from a drop in cheap cache reads or from a tokenizer count.
 *
 * Nothing is recorded here and nothing is estimated. Each side is one indexed
 * run of the same Work item in the same account, whose provider session maps
 * to that task alone; its counters are the imported provider usage rows for
 * that session. A verified handoff receipt is required and must name the task;
 * which run consumed it is the caller's declaration, said so in the output.
 * Side counters are additive rows only; a Codex first cumulative snapshot is
 * shown apart and makes the pair incomparable, because the first request is
 * exactly where a compact handoff would act. Valuation uses only admitted per-token rates in effect at each attempt and
 * is shown for a side only when every record of it is valued. Quality is not
 * measured here: run outcome is reported, acceptance stays unmeasured. */
#ifndef BONFYRE_METER_CONTEXT_H
#define BONFYRE_METER_CONTEXT_H

#include "bf_json.h"

typedef struct {
    char state[32], agent[32], session[80], handoff[80];
    long long records, models, nonadditive, valued, reasoning_n;
    long long u, c, w, o, r, snapshot_tokens;
    double usd;
    char model[96];
} McSide;

static int mc_str(const bf_json_doc_t *d, const bf_json_node_t *o, const char *k, char *out, size_t cap) {
    const bf_json_node_t *n = bf_json_obj_get(d, o, k);
    if (!n || n->type != BF_JSON_STRING || n->val.str.len <= 0 || (size_t)n->val.str.len >= cap) return 0;
    memcpy(out, n->val.str.ptr, (size_t)n->val.str.len); out[n->val.str.len] = 0;
    for (char *p = out; *p; p++) if (*p == '\\' || (unsigned char)*p < 0x20) return 0;
    return 1;
}

static long long mc_int(const bf_json_doc_t *d, const bf_json_node_t *o, const char *k) {
    const bf_json_node_t *n = bf_json_obj_get(d, o, k);
    return n && n->type == BF_JSON_INT ? (long long)bf_json_get_int(n) : -1;
}

/* One run of the task: its outcome and its exactly-owned provider session. */
static const char *mc_run(sqlite3 *db, const char *work_id, const char *run_id, McSide *s) {
    sqlite3_stmt *q = NULL; const char *why = NULL;
    /* An index built from attested WorkGraph attempts names the handoff each run declared. */
    int recorded = mt_scalar(db, "SELECT count(*) FROM work.pragma_table_info('account_work_run') WHERE name='handoff_ref'", NULL, 0) == 1;
    if (sqlite3_prepare_v2(db, recorded
            ? "SELECT r.state,r.agent,r.provider_session_ref,COALESCE(s.n,0),COALESCE(s.na,0),COALESCE(s.agent,''),"
              "(SELECT count(*) FROM work.account_work_run x WHERE x.work_id=r.work_id AND x.run_id=r.run_id),r.handoff_ref "
              "FROM work.account_work_run r LEFT JOIN temp.mt_sess s ON s.ref=r.provider_session_ref "
              "WHERE r.work_id=?1 AND r.run_id=?2 LIMIT 1"
            : "SELECT r.state,r.agent,r.provider_session_ref,COALESCE(s.n,0),COALESCE(s.na,0),COALESCE(s.agent,''),"
              "(SELECT count(*) FROM work.account_work_run x WHERE x.work_id=r.work_id AND x.run_id=r.run_id),'' "
              "FROM work.account_work_run r LEFT JOIN temp.mt_sess s ON s.ref=r.provider_session_ref "
              "WHERE r.work_id=?1 AND r.run_id=?2 LIMIT 1", -1, &q, NULL) != SQLITE_OK) return "the Work index cannot be read";
    sqlite3_bind_text(q, 1, work_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(q, 2, run_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(q) != SQLITE_ROW) why = "the run is not indexed on this task";
    else {
        snprintf(s->state, sizeof(s->state), "%s", (const char *)sqlite3_column_text(q, 0));
        snprintf(s->agent, sizeof(s->agent), "%s", (const char *)sqlite3_column_text(q, 1));
        snprintf(s->session, sizeof(s->session), "%s", (const char *)sqlite3_column_text(q, 2));
        snprintf(s->handoff, sizeof(s->handoff), "%s", sqlite3_column_text(q, 7) ? (const char *)sqlite3_column_text(q, 7) : "");
        if (sqlite3_column_int64(q, 6) != 1) why = "the run id is indexed more than once";
        else if (!*s->session) why = "the run carries no provider session key";
        else if (sqlite3_column_int64(q, 3) != 1 || sqlite3_column_int64(q, 4) != 1 ||
                 strcmp((const char *)sqlite3_column_text(q, 5), s->agent)) why = "the run's provider session is shared with other work";
    }
    sqlite3_finalize(q);
    return why;
}

#define MC_EVENTS "FROM events e WHERE e.op='" MT_OP "' AND e.key_id=?1 AND e.session_ref=?2 AND e.provider=?3"

static int mc_counters(sqlite3 *db, const char *subject, McSide *s) {
    sqlite3_stmt *q = NULL; int ok = 0;
    const char *sql =
        "WITH e AS (SELECT e.provider,e.model,e.timestamp ts,e.counter_basis b,e.uncached_input_tokens u,e.cached_input_tokens c,"
        "e.cache_write_tokens w,e.output_tokens o,e.reasoning_tokens r " MC_EVENTS "),"
        "p AS (SELECT *," MT_RATE("uncached_input") " ru," MT_RATE("cached_input") " rc," MT_RATE("cache_write") " rw," MT_RATE("output") " ro FROM e),"
        "v AS (SELECT *,(ru IS NOT NULL OR rc IS NOT NULL OR rw IS NOT NULL OR ro IS NOT NULL) AND (u=0 OR ru IS NOT NULL) AND (c=0 OR rc IS NOT NULL) "
        "AND (w=0 OR rw IS NOT NULL) AND (o=0 OR ro IS NOT NULL) AND b<>'" MT_SNAPSHOT "' AS ok FROM p) "
        "SELECT count(*),count(DISTINCT model),COALESCE(min(model),''),COALESCE(sum(b='" MT_SNAPSHOT "'),0),COALESCE(sum(ok),0),"
        "COALESCE(sum(CASE WHEN a THEN u END),0),COALESCE(sum(CASE WHEN a THEN c END),0),COALESCE(sum(CASE WHEN a THEN w END),0),"
        "COALESCE(sum(CASE WHEN a THEN o END),0),COALESCE(sum(CASE WHEN a THEN r END),0),count(CASE WHEN a THEN r END),"
        "COALESCE(sum(CASE WHEN ok THEN u*COALESCE(ru,0)+c*COALESCE(rc,0)+w*COALESCE(rw,0)+o*COALESCE(ro,0) END),0),"
        "COALESCE(sum(CASE WHEN NOT a THEN u+c+w+o END),0) FROM (SELECT *,b<>'" MT_SNAPSHOT "' AS a FROM v)";
    if (sqlite3_prepare_v2(db, sql, -1, &q, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(q, 2, s->session, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(q, 3, s->agent, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(q) != SQLITE_ROW) goto done;
    s->records = sqlite3_column_int64(q, 0); s->models = sqlite3_column_int64(q, 1);
    snprintf(s->model, sizeof(s->model), "%s", (const char *)sqlite3_column_text(q, 2));
    s->nonadditive = sqlite3_column_int64(q, 3); s->valued = sqlite3_column_int64(q, 4);
    s->u = sqlite3_column_int64(q, 5); s->c = sqlite3_column_int64(q, 6); s->w = sqlite3_column_int64(q, 7);
    s->o = sqlite3_column_int64(q, 8); s->r = sqlite3_column_int64(q, 9); s->reasoning_n = sqlite3_column_int64(q, 10);
    s->usd = sqlite3_column_double(q, 11); s->snapshot_tokens = sqlite3_column_int64(q, 12);
    ok = 1;
done:
    sqlite3_finalize(q);
    return ok;
}

static void mc_print_side(const char *name, const char *run, const McSide *s) {
    printf("side\t%s\t%s\t%s\t%s\t%s\t%lld\t%lld\t%lld\t%lld\t%lld\t", name, run, s->state, s->agent,
           s->models == 1 ? s->model : "mixed", s->records, s->u, s->c, s->w, s->o);
    if (s->reasoning_n == s->records - s->nonadditive) printf("%lld", s->r); else fputs("unknown", stdout);
    printf("\t%lld\t%lld\t%lld\t%lld\t%lld\t", s->nonadditive, s->snapshot_tokens, s->u + s->w, s->valued, s->records - s->valued);
    if (s->records && s->valued == s->records) printf("%.6f\n", s->usd); else fputs("unknown\n", stdout);
}

static int cmd_context_compare(const char *meter_path, const char *work_path, const char *subject, const char *scope,
                               const char *work_id, const char *baseline_run, const char *compact_run, const char *handoff_path) {
    sqlite3 *db = NULL; int rc = 2;
    char *buf = NULL; bf_json_doc_t *doc = NULL;
    const char *why = NULL;
    if (!work_path || *work_path != '/' || !subject || !*subject || !scope || !*scope || !work_id || !*work_id ||
        !baseline_run || !*baseline_run || !compact_run || !*compact_run || !handoff_path) {
        fprintf(stderr, "[meter] context-compare requires --work-db ABS --subject --scope --mission --baseline-run --compact-run --handoff\n");
        return 2;
    }
    if (!strcmp(baseline_run, compact_run)) { fprintf(stderr, "[meter] refused: baseline and compact are the same run\n"); return 2; }

    /* The handoff receipt from `bonfyre-kvcache context-verify`. */
    FILE *fp = fopen(handoff_path, "rb");
    long len = -1;
    if (fp && fseek(fp, 0, SEEK_END) == 0) len = ftell(fp);
    if (fp && len > 0 && len < (1L << 20) && fseek(fp, 0, SEEK_SET) == 0 && (buf = malloc((size_t)len + 1)) &&
        fread(buf, 1, (size_t)len, fp) == (size_t)len) { buf[len] = 0; doc = bf_json_parse(buf, (size_t)len, NULL, 0); }
    if (fp) fclose(fp);
    const bf_json_node_t *h = doc ? bf_json_root(doc) : NULL;
    char schema[64], verdict[16], task[160], receipt[80], context[80], revision[160] = "";
    long long ctx_tokens = -1, src_tokens = -1, budget = -1;
    if (!h || h->type != BF_JSON_OBJECT || !mc_str(doc, h, "schema_version", schema, sizeof(schema)) ||
        strcmp(schema, "bonfyre.context.handoff.v1")) why = "not a context-verify handoff receipt";
    else if (!mc_str(doc, h, "verdict", verdict, sizeof(verdict)) || strcmp(verdict, "PASS")) why = "the handoff was not verified";
    else if (!mc_str(doc, h, "task", task, sizeof(task))) why = "the handoff was verified without a task";
    else if (strcmp(task, work_id)) why = "the handoff was verified for another task";
    else if (!mc_str(doc, h, "receipt_sha256", receipt, sizeof(receipt)) || !mc_str(doc, h, "context_sha256", context, sizeof(context)))
        why = "the handoff names no digests";
    else {
        mc_str(doc, h, "revision", revision, sizeof(revision));
        ctx_tokens = mc_int(doc, h, "context_tokens"); src_tokens = mc_int(doc, h, "source_tokens"); budget = mc_int(doc, h, "token_budget");
        if (ctx_tokens <= 0 || src_tokens < ctx_tokens || budget < ctx_tokens) why = "the handoff's token counts are inconsistent";
    }
    if (why) { fprintf(stderr, "[meter] refused: %s\n", why); goto done; }

    if (!(db = mt_open_ro(meter_path))) { fprintf(stderr, "[meter] no token evidence in this store\n"); rc = 1; goto done; }
    if (!mt_attach_work(db, work_path)) { fprintf(stderr, "[meter] the account Work index is unavailable or carries no provider session key\n"); goto done; }
    const char *p[3] = {work_id, subject, scope};
    if (mt_scalar(db, "SELECT count(*) FROM work.account_work WHERE id=?1 AND subject=?2 AND scope=?3", p, 3) != 1) {
        fprintf(stderr, "[meter] refused: task is not evidenced in the requested account scope\n"); goto done;
    }
    McSide b = {0}, c = {0};
    if ((why = mc_run(db, work_id, baseline_run, &b)) || (why = mc_run(db, work_id, compact_run, &c))) {
        fprintf(stderr, "[meter] refused: %s\n", why); goto done;
    }
    if (!strcmp(b.session, c.session)) { fprintf(stderr, "[meter] refused: both runs name one provider session\n"); goto done; }
    /* Where the governed WorkGraph attempt recorded its handoff, the record decides:
     * the compact run must name this receipt and the baseline run must not. */
    char named[80];
    snprintf(named, sizeof(named), "sha256:%s", receipt);
    if (!strcmp(b.handoff, named)) { fprintf(stderr, "[meter] refused: the baseline run recorded this handoff\n"); goto done; }
    if (*c.handoff && strcmp(c.handoff, named)) { fprintf(stderr, "[meter] refused: the compact run recorded another handoff\n"); goto done; }
    if (!mc_counters(db, subject, &b) || !mc_counters(db, subject, &c)) {
        fprintf(stderr, "[meter] context-compare failed: %s\n", sqlite3_errmsg(db)); rc = 1; goto done;
    }
    if (!b.records || !c.records) { fprintf(stderr, "[meter] refused: a run has no imported provider usage for this account\n"); goto done; }

    printf("task\t%s\ntask_equivalence\tsame_work_item\n", work_id);
    printf("handoff_receipt\tsha256:%s\nhandoff_context\tsha256:%s\nhandoff_revision\t%s\n", receipt, context, *revision ? revision : "unbound");
    printf("handoff_binding\t%s\n", *c.handoff ? "recorded_by_claimed_workgraph_attempt_not_consumption_observed"
                                                 : "declared_by_caller_not_launcher_observed");
    printf("handoff_context_tokens\t%lld\nhandoff_source_tokens\t%lld\nhandoff_token_budget\t%lld\n"
           "handoff_token_basis\ttokenizer_count_not_provider_usage\n", ctx_tokens, src_tokens, budget);
    fputs("#side\tside\trun\tstate\tprovider\tmodel\trecords\tuncached_input\tcached_input\tcache_write\toutput\treasoning"
          "\tnonadditive_records\tnonadditive_tokens\texpensive_new_input\tvalued_records\tunvalued_records\tprovisional_valuation_usd\n", stdout);
    mc_print_side("baseline", baseline_run, &b);
    mc_print_side("compact", compact_run, &c);

    const char *cmp = NULL;
    if (strcmp(b.agent, c.agent)) cmp = "no_provider_differs";
    else if (b.models != 1 || c.models != 1) cmp = "no_mixed_models";
    else if (strcmp(b.model, c.model)) cmp = "no_model_differs";
    else if (b.nonadditive || c.nonadditive) cmp = "no_inherited_snapshot_present";
    printf("comparable\t%s\n", cmp ? cmp : "yes");
    if (!cmp) {
        printf("delta_uncached_input\t%lld\ndelta_cache_write\t%lld\ndelta_expensive_new_input\t%lld\n"
               "delta_cached_input\t%lld\ndelta_output\t%lld\n",
               c.u - b.u, c.w - b.w, (c.u + c.w) - (b.u + b.w), c.c - b.c, c.o - b.o);
        if (b.reasoning_n == b.records - b.nonadditive && c.reasoning_n == c.records - c.nonadditive) printf("delta_reasoning\t%lld\n", c.r - b.r);
        else fputs("delta_reasoning\tunknown_not_reported\n", stdout);
        if (b.valued == b.records && c.valued == c.records) printf("delta_provisional_valuation_usd\t%.6f\n", c.usd - b.usd);
        else fputs("delta_provisional_valuation_usd\tunknown_no_admitted_rate_for_every_record\n", stdout);
    }
    printf("outcome\t%s\n", !strcmp(b.state, c.state) ? "same_run_state" : "run_state_differs");
    fputs("quality_acceptance\tnot_measured\n", stdout);
    fputs("charged_usd\tunknown_no_authoritative_ledger\nmoney_status\tunknown_not_inferred\nallocation_status\tnot_evaluated\n", stdout);
    rc = 0;
done:
    if (db) sqlite3_close(db);
    if (doc) bf_json_free(doc);
    free(buf);
    return rc;
}

#endif
