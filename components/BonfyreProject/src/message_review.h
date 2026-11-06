/* Read-only projection of the existing Project and HeldMessage owners. */
static void review_html_text(FILE *out, const char *text) {
    for (const unsigned char *p = (const unsigned char *)(text ? text : ""); *p; p++) {
        switch (*p) {
            case '&': fputs("&amp;", out); break;
            case '<': fputs("&lt;", out); break;
            case '>': fputs("&gt;", out); break;
            case '"': fputs("&quot;", out); break;
            case '\'': fputs("&#39;", out); break;
            default: fputc(*p, out);
        }
    }
}

static const char *review_column(sqlite3_stmt *st, int col) {
    const char *value = (const char *)sqlite3_column_text(st, col);
    return value ? value : "";
}

/* These are conservative address shapes, never deliverability or approval. */
enum { REVIEW_EMAIL, REVIEW_CRAIGSLIST, REVIEW_WEB, REVIEW_UNRESOLVED };
static const char *review_route_keys[] = {"email_address", "craigslist_post", "web_route", "unresolved"};
static const char *review_route_notes[] = {
    "Email address recorded; mailbox and contact role still need review.",
    "Craigslist route; reply details still need to be resolved.",
    "Web route recorded; application fields and current acceptance still need review.",
    "Contact route unresolved; this value is not a usable email address or web route."
};
static int review_contact_route(const char *value) {
    if (!value || !*value) return REVIEW_UNRESOLVED;
    for (const unsigned char *p = (const unsigned char *)value; *p; p++)
        if (*p <= 0x20 || *p >= 0x7f || strchr("<>\"", *p)) return REVIEW_UNRESOLVED;
    const char *host = NULL;
    if (!strncasecmp(value, "https://", 8)) host = value + 8;
    else if (!strncasecmp(value, "http://", 7)) host = value + 7;
    if (host) {
        size_t length = strcspn(host, "/?#");
        if (!length || memchr(host, '@', length) || memchr(host, '\\', length)) return REVIEW_UNRESOLVED;
        const char *domain = "craigslist.org";
        size_t n = strlen(domain);
        if ((length == n && !strncasecmp(host, domain, n)) ||
            (length > n && host[length-n-1] == '.' && !strncasecmp(host+length-n, domain, n))) return REVIEW_CRAIGSLIST;
        return REVIEW_WEB;
    }
    const char *at = strchr(value, '@');
    if (!at || at == value || !at[1] || strchr(at+1, '@') || strpbrk(value, ",;:/\\()[]")) return REVIEW_UNRESOLVED;
    const char *dot = strchr(at+1, '.');
    if (!dot || dot == at+1 || !dot[1] || value[strlen(value)-1] == '.' || strstr(at+1, "..")) return REVIEW_UNRESOLVED;
    return REVIEW_EMAIL;
}

static int message_review(int argc, char **argv) {
    const char *path = getenv("BONFYRE_CMS_DB"), *ns = NULL, *format = "json", *output = NULL;
    for (int i = 3; i < argc; i++) {
        if (!strcmp(argv[i], "--db") && i + 1 < argc) path = argv[++i];
        else if (!strcmp(argv[i], "--ns") && i + 1 < argc) ns = argv[++i];
        else if (!strcmp(argv[i], "--format") && i + 1 < argc) format = argv[++i];
        else if (!strcmp(argv[i], "--out") && i + 1 < argc) output = argv[++i];
        else { fprintf(stderr, "message review: unknown or incomplete option\n"); return 2; }
    }
    if (!path || !*path || !ns || !*ns || (strcmp(format, "json") && strcmp(format, "html"))) {
        fprintf(stderr, "message review: --db FILE and --ns NAME required; --format json|html\n"); return 2;
    }
    if (output) {
        struct stat db_stat, out_stat;
        if (!stat(path, &db_stat) && !stat(output, &out_stat) &&
            db_stat.st_dev == out_stat.st_dev && db_stat.st_ino == out_stat.st_ino) {
            fprintf(stderr, "message review: output cannot replace its source database\n"); return 2;
        }
    }
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL;
    char *snapshot = NULL, *cards = NULL, *index = NULL;
    size_t snapshot_size = 0, cards_size = 0, index_size = 0;
    FILE *json = NULL, *html = NULL, *nav = NULL, *out = NULL;
    char temporary[PATH_MAX] = "";
    int result = 1, count = 0, active = 0, projects = 0, step;
    int active_routes[4] = {0};
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) goto failed;
    sqlite3_busy_timeout(db, 5000);
    /* Both queries use one SQLite read snapshot. Namespace
       equality is part of the join; equal ids elsewhere cannot supply a project. */
    if (sqlite3_exec(db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) goto failed;
    const char *sql =
      "SELECT m.id,m.message_id,m.about,m.recipient,m.subject_line,m.body,m.state,m.status,"
      "m.state_reason,m.cannot_infer,m.composed_at,m.would_be_sent_by,"
      "p.title,p.stage,p.pitch,p.operating_stated,p.currency,p.cannot_infer,p.status,"
      "m.updated_at,p.updated_at "
      "FROM outbound_message m LEFT JOIN project p "
      "ON p.namespace=m.namespace AND p.project_id=m.about "
      "WHERE m.namespace=?1 "
      "ORDER BY CASE WHEN p.stage='proposal_review' AND p.status='draft' "
      "AND m.status='draft' AND m.state IN ('withheld','composed') THEN 0 ELSE 1 END,"
      "p.title COLLATE NOCASE,m.message_id LIMIT 1001";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) goto failed;
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC);
    json = open_memstream(&snapshot, &snapshot_size);
    html = open_memstream(&cards, &cards_size);
    nav = open_memstream(&index, &index_size);
    if (!json || !html || !nav) goto failed;
    fputs("{\"namespace\":", json); json_string(json, ns); fputs(",\"messages\":[", json);
    const char *keys[] = {"id","message_id","about","recipient","subject_line","body","state","status",
      "state_reason","cannot_infer","composed_at","would_be_sent_by","project_title","project_stage",
      "project_pitch","price_options","currency","project_cannot_infer","project_status",
      "message_updated_at","project_updated_at"};
    while ((step = sqlite3_step(st)) == SQLITE_ROW) {
        if (++count > 1000) { fprintf(stderr, "message review: more than 1000 rows; report refused, narrow the namespace\n"); goto failed; }
        const char *state = review_column(st, 6), *stage = review_column(st, 13);
        int included = !strcmp(stage, "proposal_review") && !strcmp(review_column(st, 18), "draft") &&
            !strcmp(review_column(st, 7), "draft") && (!strcmp(state, "withheld") || !strcmp(state, "composed"));
        active += included;
        int route = review_contact_route(review_column(st, 3));
        if (included) active_routes[route]++;
        if (included) {
            fprintf(nav, "<li><a href=\"#message-%lld\">", (long long)sqlite3_column_int64(st, 0));
            review_html_text(nav, *review_column(st, 12) ? review_column(st, 12) : review_column(st, 2));
            fputs("</a></li>", nav);
        } else if (count == active + 1) {
            fputs("<details class=\"background\" id=\"outside-batch\"><summary>Messages outside the active batch</summary>", html);
        }
        if (count > 1) fputc(',', json);
        fputc('{', json);
        for (int col = 0; col < 21; col++) {
            if (col) fputc(',', json);
            json_string(json, keys[col]); fputc(':', json);
            json_string(json, review_column(st, col));
        }
        fputs(",\"contact_route\":", json); json_string(json, review_route_keys[route]);
        fputs(",\"contact_route_note\":", json); json_string(json, review_route_notes[route]);
        fprintf(json, ",\"in_active_batch\":%s}", included ? "true" : "false");
        fprintf(html, "<article id=\"message-%lld\"><p class=\"state\">%s · ", (long long)sqlite3_column_int64(st, 0),
          included ? "Awaiting review" : "Outside active batch");
        review_html_text(html, state);
        if (*stage) { fputs(" · ", html); review_html_text(html, stage); }
        fputs("</p><h2>", html);
        review_html_text(html, *review_column(st, 12) ? review_column(st, 12) : review_column(st, 2));
        fputs("</h2><p>To: ", html); review_html_text(html, review_column(st, 3));
        fputs("</p><p class=\"state\">", html); review_html_text(html, review_route_notes[route]);
        fputs("</p><h3>", html); review_html_text(html, review_column(st, 4));
        fputs("</h3><pre class=\"message\">", html); review_html_text(html, review_column(st, 5)); fputs("</pre>", html);
        const int cols[] = {15,14,8,9,17};
        const char *labels[] = {"Scope and commercial notes","Need, evidence and delivery approach",
          "Review notes","Message limits","Project limits"};
        for (int n = 0; n < 5; n++) if (*review_column(st, cols[n])) {
            fprintf(html, "<details><summary>%s</summary><pre>", labels[n]);
            review_html_text(html, review_column(st, cols[n])); fputs("</pre></details>", html);
        }
        fputs("<p class=\"reference\">Message ", html); review_html_text(html, review_column(st, 0));
        fputs(" · ", html); review_html_text(html, review_column(st, 1));
        fputs(" · Intended sender: ", html); review_html_text(html, review_column(st, 11));
        fputs("</p>", html);
        if (included) fputs("<a class=\"back\" href=\"#draft-index\">Back to draft list</a>", html);
        fputs("</article>\n", html);
        if (fflush(json) || fflush(html) || fflush(nav) || snapshot_size > 8 * 1024 * 1024 || cards_size + index_size > 8 * 1024 * 1024) {
            fprintf(stderr, "message review: report exceeds its 8 MiB limit\n"); goto failed;
        }
    }
    if (step != SQLITE_DONE) goto failed;
    if (count > active) fputs("</details>", html);
    sqlite3_finalize(st); st = NULL;
    fputs("],\"projects_without_messages\":[", json);
    const char *project_sql =
      "SELECT p.id,p.project_id,p.subject_ref,p.title,p.stage,p.pitch,p.operating_stated,"
      "p.currency,p.cannot_infer,p.status,p.updated_at FROM project p "
      "WHERE p.namespace=?1 AND p.status='draft' AND NOT EXISTS "
      "(SELECT 1 FROM outbound_message m WHERE m.namespace=p.namespace AND m.about=p.project_id) "
      "ORDER BY p.title COLLATE NOCASE,p.project_id LIMIT 1001";
    if (sqlite3_prepare_v2(db, project_sql, -1, &st, NULL) != SQLITE_OK) goto failed;
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC);
    const char *project_keys[] = {"id","project_id","subject_ref","title","stage","pitch",
      "operating_stated","currency","cannot_infer","status","updated_at"};
    while ((step = sqlite3_step(st)) == SQLITE_ROW) {
        if (++projects > 1000) { fprintf(stderr, "message review: more than 1000 projects without messages; report refused\n"); goto failed; }
        if (projects == 1) fputs("<details class=\"background\" id=\"research\"><summary>Projects without a message record</summary><p>These projects have no linked message in this namespace. Their saved stages and notes may record prior outreach; check that history before preparing a message.</p>", html);
        if (projects > 1) fputc(',', json);
        fputc('{', json);
        for (int col = 0; col < 11; col++) {
            if (col) fputc(',', json);
            json_string(json, project_keys[col]); fputc(':', json);
            json_string(json, review_column(st, col));
        }
        fputc('}', json);
        fprintf(html, "<article id=\"project-%lld\"><p class=\"state\">No linked message record · ", (long long)sqlite3_column_int64(st, 0));
        review_html_text(html, review_column(st, 4));
        fputs("</p><h2>", html); review_html_text(html, review_column(st, 3)); fputs("</h2>", html);
        const int cols[] = {6,5,8};
        const char *labels[] = {"Scope and commercial notes","Need, evidence and delivery approach","Project limits"};
        for (int n = 0; n < 3; n++) if (*review_column(st, cols[n])) {
            fprintf(html, "<details><summary>%s</summary><pre>", labels[n]);
            review_html_text(html, review_column(st, cols[n])); fputs("</pre></details>", html);
        }
        fputs("<p class=\"reference\">Project ", html); review_html_text(html, review_column(st, 0));
        fputs(" · ", html); review_html_text(html, review_column(st, 1));
        fputs("<br>Source reference: ", html); review_html_text(html, review_column(st, 2));
        fputs("</p></article>\n", html);
        if (fflush(json) || fflush(html) || snapshot_size > 8 * 1024 * 1024 || cards_size + index_size > 8 * 1024 * 1024) {
            fprintf(stderr, "message review: report exceeds its 8 MiB limit\n"); goto failed;
        }
    }
    if (step != SQLITE_DONE) goto failed;
    if (projects) fputs("</details>", html);
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto failed;
    fputs("]}", json);
    int json_close = fclose(json), html_close = fclose(html), nav_close = fclose(nav); json = NULL; html = NULL; nav = NULL;
    if (json_close || html_close || nav_close) goto failed;
    char digest[65]; bf_sha256_hex((const uint8_t *)snapshot, snapshot_size, digest);
    if (output) {
        if (snprintf(temporary, sizeof(temporary), "%s.XXXXXX", output) >= (int)sizeof(temporary)) goto failed;
        int fd = mkstemp(temporary);
        if (fd < 0) goto failed;
        out = fdopen(fd, "w");
        if (!out) { close(fd); goto failed; }
    } else out = stdout;
    if (!strcmp(format, "json")) {
        fprintf(out, "{\"schema\":\"bonfyre.message.review.v1\",\"snapshot_sha256\":\"%s\","
          "\"total\":%d,\"active\":%d,\"outside_active_batch\":%d,\"projects_without_messages\":%d,"
          "\"active_contact_routes\":{\"email_address\":%d,\"craigslist_post\":%d,\"web_route\":%d,\"unresolved\":%d},\"source\":%s,"
          "\"cannot_infer\":\"Review membership is not approval, source freshness, delivery or revenue. This command sends nothing and changes no CMS record.\"}\n",
          digest, count, active, count - active, projects, active_routes[0], active_routes[1], active_routes[2], active_routes[3], snapshot);
    } else {
        fputs("<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
          "<meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'\">"
          "<title>Client proposals for review</title><style>"
          "body{font:17px/1.6 system-ui,sans-serif;color:#202822;background:#f7f6f2;margin:0}"
          "main{max-width:900px;margin:auto;padding:32px 20px}article{background:white;border:1px solid #d9ddd6;border-radius:8px;padding:26px;margin:24px 0}"
          "h1,h2,h3{line-height:1.25}h2{font-size:26px}.state{font-size:14px;color:#44564a}"
          "pre{white-space:pre-wrap;overflow-wrap:anywhere;font:inherit}.message{border-left:3px solid #335544;padding-left:18px}"
          "summary{cursor:pointer;padding:12px 0;font-weight:600}.reference{font-size:13px;overflow-wrap:anywhere}"
          "a{color:#285541;text-underline-offset:3px}a:focus-visible,summary:focus-visible{outline:3px solid #8a602b;outline-offset:4px}"
          "nav{border-block:1px solid #d9ddd6;padding:12px 0}nav ul{padding-left:22px;columns:2;column-gap:32px}nav li{break-inside:avoid;margin:0 0 10px}"
          ".back{font-size:14px}.background{margin:24px 0;border-top:1px solid #d9ddd6}article,nav{scroll-margin-top:20px}"
          "@media(max-width:600px){nav ul{columns:1}}"
          "@media(max-width:600px){main{padding:20px 12px}article{padding:18px}h2{font-size:22px}}"
          "@media print{body{background:white}article{break-inside:avoid}}"
          "</style><main><h1>Client proposals for review</h1><p>", out);
        fprintf(out, "%d active draft%s. %d message%s outside the active batch. %d project%s without a message.</p>", active, active == 1 ? "" : "s", count - active, count - active == 1 ? "" : "s", projects, projects == 1 ? "" : "s");
        fprintf(out, "<p>Contact routes in the active batch: %d recorded email addresses; %d Craigslist routes; %d other web routes; %d unresolved values. These counts describe the saved addresses, not verified delivery or completed applications.</p>", active_routes[0], active_routes[1], active_routes[2], active_routes[3]);
        fputs("<p>Reading this report does not approve or send a message. The active batch follows the saved project stage and draft state; it does not establish that a posting is still open.</p><p class=\"reference\">Namespace: ", out);
        review_html_text(out, ns);
        fprintf(out, "<br>Snapshot: %s</p>", digest);
        if (active) fprintf(out, "<nav id=\"draft-index\" aria-label=\"Active drafts\"><h2>Choose a draft</h2><ul>%s</ul></nav>", index);
        fprintf(out, "%s</main></html>\n", cards);
    }
    if (ferror(out)) goto failed;
    if (output) {
        int closed = fclose(out); out = NULL;
        if (closed || rename(temporary, output)) goto failed;
        temporary[0] = '\0';
        fputs("{\"schema\":\"bonfyre.message.review.export.v1\",\"path\":", stdout);
        json_string(stdout, output);
        fprintf(stdout, ",\"snapshot_sha256\":\"%s\",\"total\":%d,\"active\":%d,\"outside_active_batch\":%d,\"projects_without_messages\":%d,\"cms_changed\":false,\"transmitted\":false}\n", digest, count, active, count - active, projects);
    }
    result = 0;
failed:
    if (result) fprintf(stderr, "message review: no complete report produced (%s)\n",
      db ? sqlite3_errmsg(db) : "could not open records");
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    if (json) fclose(json);
    if (html) fclose(html);
    if (nav) fclose(nav);
    if (out && out != stdout) fclose(out);
    if (*temporary) unlink(temporary);
    free(snapshot); free(cards); free(index);
    return result;
}
