#ifndef BONFYRE_DESK_AND_INTERNET_OBSERVATORY_H
#define BONFYRE_DESK_AND_INTERNET_OBSERVATORY_H
/* Extracted verbatim from main.c lines 29042-31735 (mechanical split, no logic changes). */
static int desk_unpriced_caps(DeskRow *out, int cap, int *listed) {
    int n = 0;
    *listed = X402_CATALOG_N;
    for (int i = 0; i < X402_CATALOG_N && n < cap; i++) {
        double usd = 0.0;
        if (x402_price_usd(X402_CATALOG[i].name, &usd) && usd > 0.0) continue;
        snprintf(out[n].label, sizeof(out[n].label), "%s", X402_CATALOG[i].name);
        snprintf(out[n].sub, sizeof(out[n].sub), "%s", X402_CATALOG[i].summary);
        snprintf(out[n].href, sizeof(out[n].href), "/room/capability/%s", X402_CATALOG[i].name);
        n++;
    }
    return n;
}

/* Notes that exist but were never published. Writing and publishing are
 * separate acts here, so a draft is a decision someone stopped short of. */
static int desk_drafts(DeskRow *out, int cap, int *total, const char *bearer, const char *auth_db) {
    sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL; int n = 0;
    *total = 0;
    char namespace_name[192] = {0};
    if (!db) return 0;
    if (!cms_namespace_for(bearer, auth_db, namespace_name, sizeof(namespace_name))) { sqlite3_close(db); return 0; }
    /* Counted separately from the page. A count that stops at the page size is
     * the page size, and reporting it as the count understates what is open. */
    if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM wiki_page "
                               "WHERE (namespace=?1 OR namespace='root') AND (status IS NULL OR status <> 'published')",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, namespace_name, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) *total = sqlite3_column_int(st, 0);
    }
    if (st) { sqlite3_finalize(st); st = NULL; }
    if (sqlite3_prepare_v2(db, "SELECT title, route FROM wiki_page "
                               "WHERE (namespace=?1 OR namespace='root') AND (status IS NULL OR status <> 'published') "
                               "ORDER BY id DESC LIMIT ?2", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, namespace_name, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, cap);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *title = (const char*)sqlite3_column_text(st, 0);
            const char *route = (const char*)sqlite3_column_text(st, 1);
            if (n >= cap || !route) continue;
            snprintf(out[n].label, sizeof(out[n].label), "%s", title ? title : route);
            snprintf(out[n].sub, sizeof(out[n].sub), "%s", route);
            if (!strncmp(route, "notes/", 6))
                snprintf(out[n].href, sizeof(out[n].href), "/room/note/%s", route + 6);
            else
                snprintf(out[n].href, sizeof(out[n].href), "/cms/");
            n++;
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    return n;
}

/* Offers sitting in the book at zero. A composed offer is created unpriced on
 * purpose: the sentence proposed an exchange, it did not name a number. */
static int desk_unpriced_offers(DeskRow *out, int cap, int *total) {
    const char *fin = getenv("BONFYRE_FINANCE_DB");
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int n = 0;
    *total = 0;
    if (!fin || !*fin) fin = "/var/lib/bonfyre/state/finance.db";
    if (sqlite3_open_v2(fin, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 0;
    }
    /* One offer entered twenty-four times is one pricing decision, not
     * twenty-four. A loop that appends a timestamp to a name was filling this
     * queue with copies of itself, and a queue that counts rows instead of
     * decisions asks the operator to make the same choice over and over.
     * Trailing digits and their separator are the timestamp; what remains is
     * the offer. */
    if (sqlite3_prepare_v2(db, "SELECT COUNT(DISTINCT CASE WHEN length(name) - length(rtrim(name,'0123456789')) >= 9 THEN rtrim(rtrim(name,'0123456789'),'-') ELSE name END) "
                               "FROM services WHERE sell_price <= 0",
                           -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW)
        *total = sqlite3_column_int(st, 0);
    if (st) { sqlite3_finalize(st); st = NULL; }
    if (sqlite3_prepare_v2(db, "SELECT CASE WHEN length(name) - length(rtrim(name,'0123456789')) >= 9 THEN rtrim(rtrim(name,'0123456789'),'-') ELSE name END AS family, "
                               "       COUNT(*) AS copies, MAX(source) AS source "
                               "FROM services WHERE sell_price <= 0 "
                               "GROUP BY family ORDER BY copies DESC, family LIMIT ?",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, cap);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *name = (const char*)sqlite3_column_text(st, 0);
            int copies       = sqlite3_column_int(st, 1);
            const char *src  = (const char*)sqlite3_column_text(st, 2);
            if (n >= cap || !name) continue;
            snprintf(out[n].label, sizeof(out[n].label), "%s", name);
            /* Say how many copies are behind one row, so the operator can see
             * that pricing it once settles all of them. */
            if (copies > 1)
                snprintf(out[n].sub, sizeof(out[n].sub),
                         "entered as %s, %d times, no asking price",
                         src && *src ? src : "unattributed", copies);
            else
                snprintf(out[n].sub, sizeof(out[n].sub), "entered as %s, no asking price",
                         src && *src ? src : "unattributed");
            snprintf(out[n].href, sizeof(out[n].href), "/market");
            n++;
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    return n;
}

/* Missions that were started and have not finished. */
static int desk_open_work(DeskRow *out, int cap, int *total) {
    char workdb[PATH_MAX];
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int n = 0;
    *total = 0;
    if (snprintf(workdb, sizeof(workdb), "%s/fleming-work.db", fleming_root()) >= (int)sizeof(workdb))
        return 0;
    if (sqlite3_open_v2(workdb, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 0;
    }
    /* The terminal state this store writes is 'complete'. This filter excluded
     * 'completed', which never matches, so all 129 finished missions counted as
     * unfinished and the desk reported 217 open when 88 were. Both spellings are
     * listed now so the queue cannot be inflated again by a vocabulary drift on
     * one side of a string comparison. */
    if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM missions "
                               "WHERE id NOT LIKE 'account-%' AND status NOT IN ('complete','completed','failed','cancelled')",
                           -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW)
        *total = sqlite3_column_int(st, 0);
    if (st) { sqlite3_finalize(st); st = NULL; }
    if (sqlite3_prepare_v2(db, "SELECT id, status, created_at FROM missions "
                               "WHERE id NOT LIKE 'account-%' AND status NOT IN ('complete','completed','failed','cancelled') "
                               "ORDER BY created_at DESC LIMIT ?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, cap);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *id = (const char*)sqlite3_column_text(st, 0);
            const char *stt = (const char*)sqlite3_column_text(st, 1);
            const char *at = (const char*)sqlite3_column_text(st, 2);
            if (n >= cap || !id) continue;
            snprintf(out[n].label, sizeof(out[n].label), "%s", id);
            snprintf(out[n].sub, sizeof(out[n].sub), "%s since %s",
                     stt ? stt : "open", at ? at : "an unrecorded time");
            snprintf(out[n].href, sizeof(out[n].href), "/agents");
            n++;
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    return n;
}

/* Messages that were written and are being held. The queue files them under
 * one family and nothing claims it, because nothing here sends: each waits for
 * a person to hand it over themselves or drop it. The store is named by
 * BONFYRE_HELD_MESSAGE_DB; with none named there is nothing to count. */
static int desk_held_messages(DeskRow *out, int cap, int *total) {
    const char *path = getenv("BONFYRE_HELD_MESSAGE_DB");
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int n = 0;
    *total = 0;
    if (!path || !path[0]) return 0;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 0;
    }
    if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM workgraph_nodes WHERE family='held-message' AND mission_id NOT LIKE 'account-%' "
                               "AND status NOT IN ('complete','completed','failed','cancelled','dead_letter')",
                           -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW)
        *total = sqlite3_column_int(st, 0);
    if (st) { sqlite3_finalize(st); st = NULL; }
    if (sqlite3_prepare_v2(db,
          "SELECT COALESCE(json_extract(input_uri,'$.subject'), json_extract(input_uri,'$.notification'), mission_id),"
          " json_extract(input_uri,'$.document'), COALESCE(json_extract(input_uri,'$.addressed'), 0),"
          " COALESCE(json_array_length(input_uri,'$.recipients'), 0)"
          " FROM workgraph_nodes WHERE family='held-message' AND mission_id NOT LIKE 'account-%' AND json_valid(input_uri)"
          " AND status NOT IN ('complete','completed','failed','cancelled','dead_letter')"
          " ORDER BY created_at_ms DESC LIMIT ?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, cap);
        while (sqlite3_step(st) == SQLITE_ROW && n < cap) {
            const char *subject = (const char *)sqlite3_column_text(st, 0);
            const char *about = (const char *)sqlite3_column_text(st, 1);
            int addressed = sqlite3_column_int(st, 2), named = sqlite3_column_int(st, 3);
            snprintf(out[n].label, sizeof(out[n].label), "%s", subject ? subject : "A message with no subject");
            if (addressed)
                snprintf(out[n].sub, sizeof(out[n].sub), "about %s, for %d %s with an address%s",
                         about && about[0] ? about : "a record", addressed, addressed == 1 ? "person" : "people",
                         named > addressed ? " and others nobody here can reach" : "");
            else
                snprintf(out[n].sub, sizeof(out[n].sub), "about %s, for people nobody here can reach",
                         about && about[0] ? about : "a record");
            snprintf(out[n].href, sizeof(out[n].href), "/studio/controls");
            n++;
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    return n;
}

/* The browser queue is not a separate application. Review happens on Desk,
 * and the selected material enters the existing native creation grammar. */
static int desk_held_pages(DeskRow *out, int cap, int *total) {
    const char *path = getenv("BONFYRE_BROWSER_QUEUE_DB");
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int n = 0;
    *total = 0;
    if (!path || !*path) return 0;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 0;
    }
    const char *where = " FROM workgraph_nodes WHERE family='held-selection' AND operator_id='queue.page.selection' AND mission_id NOT LIKE 'account-%' "
        "AND status NOT IN ('complete','completed','failed','cancelled','dead_letter')";
    char sql[1024];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*)%s", where);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW)
        *total = sqlite3_column_int(st, 0);
    if (st) { sqlite3_finalize(st); st = NULL; }
    snprintf(sql, sizeof(sql), "SELECT mission_id,CASE WHEN json_valid(input_uri) THEN json_extract(input_uri,'$.note') END,CASE WHEN json_valid(input_uri) THEN json_extract(input_uri,'$.url') END%s ORDER BY created_at_ms DESC LIMIT ?", where);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, cap);
        while (sqlite3_step(st) == SQLITE_ROW && n < cap) {
            const char *id = (const char *)sqlite3_column_text(st, 0);
            if (!held_page_id(id)) continue;
            const char *purpose = (const char *)sqlite3_column_text(st, 1);
            const char *url = (const char *)sqlite3_column_text(st, 2);
            snprintf(out[n].label, sizeof(out[n].label), "%s", purpose && *purpose ? purpose : "A selected page");
            snprintf(out[n].sub, sizeof(out[n].sub), "%s", url && *url ? url : "Review the retained source");
            snprintf(out[n].href, sizeof(out[n].href), "/make?held_selection=%s", id);
            n++;
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db); return n;
}

static size_t desk_queue(char *body, size_t cap, size_t w,
                         const char *heading, const char *why,
                         DeskRow *rows, int n, int total,
                         const char *act, const char *empty_means,
                         int may_name)
{
    char el[256], es[256];
    /* A queue of things nobody has published yet is a queue of things nobody
     * has decided to disclose. The count is a fact about the institution and
     * costs nothing to state; the titles and routes are the unpublished content
     * itself. A viewer who cannot act on the row has no reason to be handed its
     * identity, and a crawler that followed this page would otherwise harvest
     * every draft title and every route on the site. */
    if (!may_name && n) {
        return (size_t)snprintf(body + w, cap - w,
          "<section class=\"desk-q\"><div class=\"desk-qh\"><h3>%s</h3>"
          "<span class=\"desk-n\">%d</span></div><p class=\"desk-why\">%s</p>"
          "<p class=\"desk-empty\">Signed-in accounts with write scope see which "
          "ones. The count is a fact about the institution; the rows are content "
          "nobody has published yet.</p></section>", heading, total, why) + w;
    }
    w += (size_t)snprintf(body + w, cap - w,
      "<section class=\"desk-q%s\"><div class=\"desk-qh\"><h3>%s</h3>"
      "<span class=\"desk-n\">%d</span></div><p class=\"desk-why\">%s</p>",
      n ? "" : " is-clear", heading, total, why);
    if (!n) {
        w += (size_t)snprintf(body + w, cap - w,
          "<p class=\"desk-empty\">Nothing open. %s</p></section>", empty_means);
        return w;
    }
    for (int i = 0; i < n && w < cap - 1024; i++) {
        html_escape(rows[i].label, el, sizeof(el));
        html_escape(rows[i].sub, es, sizeof(es));
        w += (size_t)snprintf(body + w, cap - w,
          "<a class=\"desk-row\" href=\"%s\"><span class=\"desk-l\">%s</span>"
          "<span class=\"desk-s\">%s</span><span class=\"desk-a\">%s &rarr;</span></a>",
          rows[i].href, el, es, act);
    }
    if (total > n)
        w += (size_t)snprintf(body + w, cap - w,
          "<p class=\"desk-more\">%d more not shown.</p>", total - n);
    return (size_t)snprintf(body + w, cap - w, "</section>") + w;
}

/* ── Aurekai Internet Observatory ────────────────────────────────────────
 *
 * This is a Matrix institution, not a crawler dashboard.  A source/index hit
 * is retained only as CandidateAddress state with a source/query receipt.
 * The ForeignTwin owner is the separate, explicit hydration boundary.  The
 * public aperture exposes maintained-view counts; the authenticated Operator
 * Lab can inspect and admit candidates without making their existence a claim
 * about a live origin, an agreement, or a callable capability.
 */
static int internet_host(const char *host) {
    char normalized[256];
    return lower_host(host, normalized) && !strcmp(normalized, "internet.aurekai.work");
}

static const char *internet_db_path(void) {
    const char *configured = getenv("BONFYRE_INTERNET_DB");
    return configured && *configured ? configured : "/var/lib/bonfyre/state/internet-observatory.db";
}

static sqlite3 *internet_open(int writable) {
    sqlite3 *db = NULL;
    int flags = writable ? SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE : SQLITE_OPEN_READONLY;
    if (sqlite3_open_v2(internet_db_path(), &db, flags, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return NULL;
    }
    if (writable) {
        sqlite3_busy_timeout(db, 2000);
        if (sqlite3_exec(db,
            "PRAGMA journal_mode=WAL;"
            "CREATE TABLE IF NOT EXISTS candidate_address("
            " id INTEGER PRIMARY KEY, address TEXT NOT NULL UNIQUE, candidate_kind TEXT NOT NULL,"
            " source_index TEXT NOT NULL, source_query_digest TEXT NOT NULL,"
            " evidence_level TEXT NOT NULL CHECK(evidence_level='discovery_only'),"
            " discovered_at TEXT NOT NULL, admitted_by TEXT NOT NULL);"
            "CREATE INDEX IF NOT EXISTS candidate_address_kind ON candidate_address(candidate_kind);"
            "CREATE TABLE IF NOT EXISTS discovery_receipt("
            " candidate_id INTEGER NOT NULL REFERENCES candidate_address(id), receipt_digest TEXT NOT NULL,"
            " provider TEXT NOT NULL, query_digest TEXT NOT NULL, executed_at TEXT NOT NULL,"
            " result_count INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(candidate_id,receipt_digest));"
            /* A passport is a read-selection receipt, not a retained foreign
             * body.  It records how a selected candidate asks machines to
             * read it; ForeignTwin remains the only body-admission owner. */
            "CREATE TABLE IF NOT EXISTS source_passport("
            " candidate_id INTEGER PRIMARY KEY REFERENCES candidate_address(id), root TEXT NOT NULL,"
            " best_carrier TEXT NOT NULL, robots_allows INTEGER NOT NULL, observed_at TEXT NOT NULL,"
            " passport_digest TEXT NOT NULL, passport_json TEXT NOT NULL, probed_by TEXT NOT NULL);",
            NULL, NULL, NULL) != SQLITE_OK) {
            sqlite3_close(db); return NULL;
        }
    }
    return db;
}

static int internet_token(const char *value, char *out, size_t out_size) {
    size_t n = 0;
    if (!value || !*value || out_size < 2) return 0;
    for (; *value && n + 1 < out_size; value++) {
        unsigned char c = (unsigned char)*value;
        if (!(isalnum(c) || c == ':' || c == '-' || c == '_' || c == '.')) return 0;
        out[n++] = (char)tolower(c);
    }
    out[n] = '\0';
    return n > 0 && !*value;
}

static const char *internet_discover_binary(void) {
    const char *configured = getenv("BONFYRE_FOREIGN_DISCOVER_BIN");
    return configured && *configured ? configured : "bonfyre-foreign-discover";
}

/* Candidate addresses often name a protocol descriptor below a host.  The
 * membrane passport is intentionally compiled for the publisher root: robots,
 * feeds, sitemaps and conditional headers live at that boundary, not at a
 * guessed descriptor path. */
static int internet_origin(const char *address, char *out, size_t out_size) {
    const char *scheme, *host_end;
    size_t n;
    if (!address || !(scheme = strstr(address, "://"))) return 0;
    host_end = strchr(scheme + 3, '/');
    if (!host_end) host_end = address + strlen(address);
    n = (size_t)(host_end - address);
    if (n < 9 || n + 2 > out_size) return 0;
    memcpy(out, address, n); out[n] = '/'; out[n + 1] = '\0';
    return 1;
}

/* The membrane script emits a compact JSON passport.  The server deliberately
 * extracts only carrier-selection metadata; it retains the complete JSON by
 * digest so an HTML parser never turns the source body into local truth. */
static int internet_json_string(const char *doc, const char *field, char *out, size_t out_size) {
    char needle[96]; const char *p; size_t n = 0;
    if (!doc || !field || snprintf(needle, sizeof(needle), "\"%s\"", field) >= (int)sizeof(needle)) return 0;
    if (!(p = strstr(doc, needle)) || !(p = strchr(p + strlen(needle), ':'))) return 0;
    while (*++p && isspace((unsigned char)*p)) {}
    if (*p != '\"') return 0;
    for (p++; *p && *p != '\"' && n + 1 < out_size; p++) {
        if (*p == '\\') return 0; /* metadata tokens we read are plain ASCII */
        out[n++] = *p;
    }
    out[n] = '\0';
    return *p == '\"' && n > 0;
}

static int internet_json_bool(const char *doc, const char *field, int *out) {
    char needle[96]; const char *p;
    if (!doc || !field || !out || snprintf(needle, sizeof(needle), "\"%s\"", field) >= (int)sizeof(needle)) return 0;
    if (!(p = strstr(doc, needle)) || !(p = strchr(p + strlen(needle), ':'))) return 0;
    while (*++p && isspace((unsigned char)*p)) {}
    if (!strncmp(p, "true", 4)) { *out = 1; return 1; }
    if (!strncmp(p, "false", 5)) { *out = 0; return 1; }
    return 0;
}

static const char *internet_view_kind(const char *target) {
    if (!strcmp(target, "/agents") || !strcmp(target, "/agents/")) return "agent";
    if (!strcmp(target, "/services") || !strcmp(target, "/services/")) return "service";
    if (!strcmp(target, "/protocols") || !strcmp(target, "/protocols/")) return "protocol";
    if (!strcmp(target, "/gaps") || !strcmp(target, "/gaps/")) return "gap";
    if (!strcmp(target, "/changes") || !strcmp(target, "/changes/")) return "change";
    if (!strcmp(target, "/sources") || !strcmp(target, "/sources/")) return "source";
    if (!strncmp(target, "/agents/", 8)) return "agent";
    if (!strncmp(target, "/services/", 10)) return "service";
    if (!strncmp(target, "/protocols/", 11)) return "protocol";
    if (!strncmp(target, "/gaps/", 6)) return "gap";
    if (!strncmp(target, "/changes/", 9)) return "change";
    if (!strncmp(target, "/sources/", 9)) return "source";
    return NULL;
}

static int internet_count(sqlite3 *db, const char *kind_prefix) {
    sqlite3_stmt *st = NULL; int count = 0;
    if (!db) return 0;
    if (!kind_prefix) {
        if (sqlite3_prepare_v2(db, "SELECT count(*) FROM candidate_address", -1, &st, NULL) == SQLITE_OK &&
            sqlite3_step(st) == SQLITE_ROW) count = sqlite3_column_int(st, 0);
    } else {
        char pattern[96];
        snprintf(pattern, sizeof(pattern), "%s:%%", kind_prefix);
        if (sqlite3_prepare_v2(db, "SELECT count(*) FROM candidate_address WHERE candidate_kind LIKE ?", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, pattern, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) count = sqlite3_column_int(st, 0);
        }
    }
    if (st) sqlite3_finalize(st);
    return count;
}

static void internet_context(const BfMatrixAddressResolution *resolved) {
    fputs("<script>window.BonfyreMatrixContext={\"subject\":", stdout); json(resolved->subject_id);
    fputs(",\"presence\":", stdout); json(resolved->presence_id);
    fputs(",\"territory\":", stdout); json(resolved->territory);
    fputs(",\"world\":", stdout); json(bf_matrix_world_name(resolved->world));
    fputs(",\"audience\":", stdout); json(bf_matrix_scope_name(resolved->scope));
    fputs(",\"canonicalAddress\":", stdout); json(resolved->canonical_address);
    fputs("};</script>", stdout);
}

/* SurfaceIR's shared document grammar owns the document.  Inject the exact
 * Matrix cut into its head rather than prepending a script before <!doctype>,
 * which would make the closure correct but the HTML malformed. */
static void internet_html_reply(const char *page, const BfMatrixAddressResolution *resolved,
                                const char *cache_control, int private_operator) {
    const char *head = page ? strstr(page, "</head>") : NULL;
    const char *tail, *nav, *nav_end;
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: %s\r\nVary: Host\r\n%sConnection: close\r\n\r\n",
           cache_control, private_operator ? "X-Robots-Tag: noindex, nofollow\r\n" : "");
    if (!head) { fputs(page ? page : "", stdout); return; }
    fwrite(page, 1, (size_t)(head - page), stdout);
    internet_context(resolved);
    fputs("</head>", stdout);
    tail = head + strlen("</head>");
    /* SurfaceIR's document chrome is intentionally shared, but its generic
     * product links are not a lawful navigation contract for this institution.
     * Replace only that grammar here, keeping the human surface native HTML
     * while ensuring every visible link stays inside Observatory coordinates. */
    nav = strstr(tail, "<nav><a href=\"/property\">");
    nav_end = nav ? strstr(nav, "</nav>") : NULL;
    if (!nav || !nav_end) { fputs(tail, stdout); return; }
    fwrite(tail, 1, (size_t)(nav - tail), stdout);
    fputs("<nav><a href=\"/agents\">Agents</a><a href=\"/services\">Services</a>"
          "<a href=\"/protocols\">Protocols</a><a href=\"/gaps\">Gaps</a>"
          "<a href=\"/operators/\">Operator Lab</a></nav>", stdout);
    fputs(nav_end + strlen("</nav>"), stdout);
}

static int serve_internet_observatory(BfMatrix *matrix, const char *method, const char *target,
                                      const char *body, const char *bearer, const char *auth_db,
                                      const char *host) {
    static char page[131072];
    BfMatrixAddressResolution resolved;
    const char *view_kind;
    const char *matrix_target;
    sqlite3 *db;
    size_t w;
    if (!internet_host(host)) return 0;
    if (strcmp(method, "GET") && strcmp(method, "POST")) return 0;
    /* A controlled host may never fall through to a generic estate just
     * because an address was not compiled for this institution.  In
     * particular, the shared SurfaceIR header must not turn /property or
     * /market into an accidental view of another institution's state.  The
     * carrier endpoints above this dispatcher remain separately host-bound. */
    if (!strcmp(target, "/property") || !strcmp(target, "/property/") ||
        !strcmp(target, "/market") || !strcmp(target, "/market/") ||
        !strcmp(target, "/notes") || !strcmp(target, "/notes/") ||
        !strcmp(target, "/flow") || !strcmp(target, "/flow/") ||
        !strcmp(target, "/room") || !strcmp(target, "/room/") ||
        !strcmp(target, "/make") || !strcmp(target, "/make/") ||
        !strcmp(target, "/surfaces") || !strcmp(target, "/surfaces/")) {
        printf("HTTP/1.1 404 Not Found\r\nContent-Type: text/html; charset=utf-8\r\n"
               "Cache-Control: no-store\r\nVary: Host\r\nConnection: close\r\n\r\n"
               "<section class=\"flow-empty\"><p>This address is not compiled for the Internet Observatory.</p><a class=\"button primary\" href=\"/\">Return to Observatory <span>&rarr;</span></a></section>");
        return 1;
    }
    /* Candidate submission is an action beneath the already-addressed
     * Operator Lab; it does not manufacture a second semantic URL subject. */
    matrix_target = (!strcmp(target, "/operators/candidates") || !strcmp(target, "/operators/probe") ||
                     !strcmp(target, "/operators/")) ? "/operators" : target;
    if (!matrix_path_resolution(matrix, host, matrix_target, &resolved)) return 0;

    if (!strcmp(target, "/operators") || !strcmp(target, "/operators/") ||
        !strcmp(target, "/operators/candidates") || !strcmp(target, "/operators/probe")) {
        char actor[32];
        if (!session_valid(bearer, auth_db)) {
            printf("HTTP/1.1 401 Unauthorized\r\nContent-Type: text/html; charset=utf-8\r\n"
                   "Cache-Control: no-store\r\nX-Robots-Tag: noindex, nofollow\r\nConnection: close\r\n\r\n"
                   "<section class=\"flow-empty\"><p>The Internet Operator Lab is an authenticated work surface.</p><a class=\"button primary\" href=\"/login\">Sign in <span>&rarr;</span></a></section>");
            return 1;
        }
        if (!studio_auth_user(bearer, auth_db, actor, sizeof(actor))) {
            printf("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n<p class=\"flow-empty\">Identity closure is unavailable. No discovery state was read.</p>");
            return 1;
        }
        if (!strcmp(target, "/operators/candidates")) {
            char address[1024], kind[96], source[96], query[512], material[2048], digest[65], stamp[32];
            sqlite3_stmt *insert = NULL, *receipt = NULL;
            int candidate_id = 0;
            if (strcmp(method, "POST") || !session_authorized(bearer, auth_db, "write")) {
                flow_reply("<p class=\"flow-empty\">Adding a discovery candidate needs a write-scoped account.</p>"); return 1;
            }
            if (!query_value(body, "address", address, sizeof(address)) || !public_http_url(address) ||
                !query_value(body, "kind", kind, sizeof(kind)) || !internet_token(kind, kind, sizeof(kind)) ||
                !query_value(body, "source", source, sizeof(source)) || !internet_token(source, source, sizeof(source)) ||
                !query_value(body, "query", query, sizeof(query))) {
                flow_reply("<p class=\"flow-empty\">Provide a public address, typed candidate kind, source index, and query receipt input. Nothing was admitted.</p>"); return 1;
            }
            iso_utc_now(stamp, sizeof(stamp));
            if (snprintf(material, sizeof(material), "candidate-address-v1|%s|%s|%s|%s", address, kind, source, query) >= (int)sizeof(material)) {
                flow_reply("<p class=\"flow-empty\">Candidate receipt material exceeded the safe boundary. Nothing was admitted.</p>"); return 1;
            }
            bf_sha256_hex((const uint8_t *)material, strlen(material), digest);
            db = internet_open(1);
            if (!db || sqlite3_prepare_v2(db,
                "INSERT INTO candidate_address(address,candidate_kind,source_index,source_query_digest,evidence_level,discovered_at,admitted_by) VALUES(?,?,?,?, 'discovery_only',?,?) "
                "ON CONFLICT(address) DO NOTHING", -1, &insert, NULL) != SQLITE_OK) {
                if (db) sqlite3_close(db);
                flow_reply("<p class=\"flow-empty\">The discovery owner is unavailable. Nothing was admitted.</p>"); return 1;
            }
            sqlite3_bind_text(insert, 1, address, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(insert, 2, kind, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(insert, 3, source, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(insert, 4, digest, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(insert, 5, stamp, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(insert, 6, actor, -1, SQLITE_TRANSIENT);
            sqlite3_step(insert); sqlite3_finalize(insert); insert = NULL;
            if (sqlite3_prepare_v2(db, "SELECT id FROM candidate_address WHERE address=?", -1, &insert, NULL) == SQLITE_OK) {
                sqlite3_bind_text(insert, 1, address, -1, SQLITE_TRANSIENT);
                if (sqlite3_step(insert) == SQLITE_ROW) candidate_id = sqlite3_column_int(insert, 0);
            }
            if (insert) sqlite3_finalize(insert);
            if (candidate_id && sqlite3_prepare_v2(db,
                "INSERT OR IGNORE INTO discovery_receipt(candidate_id,receipt_digest,provider,query_digest,executed_at) VALUES(?,?,?,?,?)", -1, &receipt, NULL) == SQLITE_OK) {
                sqlite3_bind_int(receipt, 1, candidate_id); sqlite3_bind_text(receipt, 2, digest, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(receipt, 3, source, -1, SQLITE_TRANSIENT); sqlite3_bind_text(receipt, 4, digest, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(receipt, 5, stamp, -1, SQLITE_TRANSIENT); sqlite3_step(receipt);
            }
            if (receipt) sqlite3_finalize(receipt);
            sqlite3_close(db);
            {
                char eaddr[2048]; html_escape(address, eaddr, sizeof(eaddr));
                snprintf(page, sizeof(page), "<section class=\"mk-made\"><p class=\"ap-label\">CandidateAddress retained</p><article class=\"ap-act is-strongest\"><span class=\"ap-do\">%s</span><span class=\"ap-why\">%s via %s &middot; discovery-only</span></article><p class=\"subj-note\">The source/index result is receipted as a candidate. It has not been fetched, promoted to a ForeignTwin, validated as live, or authorized for use.</p></section>", eaddr, kind, source);
                flow_reply(page);
            }
            return 1;
        }
        if (!strcmp(target, "/operators/probe")) {
            char raw_id[32], origin[1024], address[1024], kind[96], passport[65536], errbuf[4096];
            char carrier[64], observed_at[64], digest[65], stamp[32], *argv[3];
            sqlite3_stmt *select = NULL, *store = NULL, *receipt = NULL;
            long candidate_id; char *end = NULL; int robots_allows = 0, exit_code;
            if (strcmp(method, "POST") || !session_authorized(bearer, auth_db, "write") ||
                !query_value(body, "candidate_id", raw_id, sizeof(raw_id))) {
                flow_reply("<p class=\"flow-empty\">A write-scoped account and one retained CandidateAddress are required. No source was probed.</p>"); return 1;
            }
            errno = 0; candidate_id = strtol(raw_id, &end, 10);
            if (errno || !raw_id[0] || !end || *end || candidate_id < 1) {
                flow_reply("<p class=\"flow-empty\">Choose a valid retained CandidateAddress. No source was probed.</p>"); return 1;
            }
            db = internet_open(1);
            if (!db || sqlite3_prepare_v2(db, "SELECT address,candidate_kind FROM candidate_address WHERE id=?", -1, &select, NULL) != SQLITE_OK) {
                if (db) sqlite3_close(db);
                flow_reply("<p class=\"flow-empty\">The discovery owner is unavailable. No source was probed.</p>"); return 1;
            }
            sqlite3_bind_int64(select, 1, candidate_id);
            if (sqlite3_step(select) != SQLITE_ROW) {
                sqlite3_finalize(select); sqlite3_close(db);
                flow_reply("<p class=\"flow-empty\">That CandidateAddress is not retained in this closure. No source was probed.</p>"); return 1;
            }
            snprintf(address, sizeof(address), "%s", (const char *)sqlite3_column_text(select, 0));
            snprintf(kind, sizeof(kind), "%s", (const char *)sqlite3_column_text(select, 1));
            sqlite3_finalize(select); select = NULL;
            if (!internet_origin(address, origin, sizeof(origin))) {
                sqlite3_close(db);
                flow_reply("<p class=\"flow-empty\">The retained address has no safe public origin. No source was probed.</p>"); return 1;
            }
            /* argv is never passed through a shell.  The only network action is
             * the already-owned MachineMembrane passport ladder, bounded to one
             * previously retained public origin. */
            argv[0] = (char *)internet_discover_binary(); argv[1] = origin; argv[2] = NULL;
            exit_code = auth_call_split(argv, NULL, passport, sizeof(passport), errbuf, sizeof(errbuf));
            if (exit_code != 0 || !internet_json_string(passport, "best_carrier", carrier, sizeof(carrier)) ||
                !internet_json_string(passport, "observed_at", observed_at, sizeof(observed_at)) ||
                !internet_json_bool(passport, "robots_allows_us", &robots_allows)) {
                sqlite3_close(db);
                flow_reply("<p class=\"flow-empty\">The MachineMembrane passport could not be compiled. No ForeignTwin was created and no source body was admitted.</p>"); return 1;
            }
            bf_sha256_hex((const uint8_t *)passport, strlen(passport), digest);
            iso_utc_now(stamp, sizeof(stamp));
            if (sqlite3_prepare_v2(db,
                "INSERT INTO source_passport(candidate_id,root,best_carrier,robots_allows,observed_at,passport_digest,passport_json,probed_by) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(candidate_id) DO UPDATE SET root=excluded.root,best_carrier=excluded.best_carrier,robots_allows=excluded.robots_allows,observed_at=excluded.observed_at,passport_digest=excluded.passport_digest,passport_json=excluded.passport_json,probed_by=excluded.probed_by",
                -1, &store, NULL) == SQLITE_OK) {
                sqlite3_bind_int64(store, 1, candidate_id); sqlite3_bind_text(store, 2, origin, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(store, 3, carrier, -1, SQLITE_TRANSIENT); sqlite3_bind_int(store, 4, robots_allows);
                sqlite3_bind_text(store, 5, observed_at, -1, SQLITE_TRANSIENT); sqlite3_bind_text(store, 6, digest, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(store, 7, passport, -1, SQLITE_TRANSIENT); sqlite3_bind_text(store, 8, actor, -1, SQLITE_TRANSIENT);
                sqlite3_step(store);
            }
            if (store) sqlite3_finalize(store);
            if (sqlite3_prepare_v2(db,
                "INSERT OR IGNORE INTO discovery_receipt(candidate_id,receipt_digest,provider,query_digest,executed_at) VALUES(?,?,?,?,?)", -1, &receipt, NULL) == SQLITE_OK) {
                sqlite3_bind_int64(receipt, 1, candidate_id); sqlite3_bind_text(receipt, 2, digest, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(receipt, 3, "machine-membrane", -1, SQLITE_TRANSIENT); sqlite3_bind_text(receipt, 4, digest, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(receipt, 5, stamp, -1, SQLITE_TRANSIENT); sqlite3_step(receipt);
            }
            if (receipt) sqlite3_finalize(receipt);
            sqlite3_close(db);
            {
                char eorigin[2048]; html_escape(origin, eorigin, sizeof(eorigin));
                snprintf(page, sizeof(page), "<section class=\"mk-made\"><p class=\"ap-label\">MachineMembrane passport retained</p><article class=\"ap-act is-strongest\"><span class=\"ap-do\">%s</span><span class=\"ap-why\">%s &middot; %s carrier &middot; robots %s</span></article><p class=\"subj-note\">The passport tells the Observatory which public carrier is cheapest to read. It is not a ForeignTwin, retained source body, live-service assertion, or authority grant. Hydration remains a separate explicit owner action.</p></section>", eorigin, kind, carrier, robots_allows ? "permits this agent" : "declines this agent");
                flow_reply(page);
            }
            return 1;
        }
        db = internet_open(0);
        w = (size_t)snprintf(page, sizeof(page), SUBJ_HEAD, "Internet Operator Lab");
        w += (size_t)snprintf(page + w, sizeof(page) - w,
            "<article class=\"subj\"><p class=\"eyebrow\"><i></i>Internet Observatory &middot; operator closure</p>"
            "<h2 class=\"subj-name\">Discover the address first. <em>Observe only after admission.</em></h2>"
            "<p class=\"subj-desc\">This is the candidate frontier for the Aurekai Internet Observatory. A registry row, crawl capture, code hit, or search result is retained with its query receipt but remains discovery-only until the ForeignTwin owner performs an admitted observation.</p>"
            "<dl class=\"flow-facts\"><div><dt>Candidate addresses</dt><dd>%d</dd></div><div><dt>Hydrated origins</dt><dd>0 by this carrier</dd></div><div><dt>Authority</dt><dd>separate</dd></div></dl>"
            "<form class=\"mk-commit\" hx-post=\"/operators/candidates\" hx-target=\"#candidate-result\" hx-swap=\"innerHTML\">"
            "<label>Public candidate address<input name=\"address\" type=\"url\" required placeholder=\"https://example.org/.well-known/agent-card.json\"></label>"
            "<div class=\"mk-money\"><label>Typed kind<input name=\"kind\" required pattern=\"[A-Za-z0-9:._-]+\" placeholder=\"agent:a2a\"></label>"
            "<label>Source index<input name=\"source\" required pattern=\"[A-Za-z0-9:._-]+\" placeholder=\"official-registry\"></label></div>"
            "<label>Query / discovery receipt input<textarea name=\"query\" required placeholder=\"protocol=A2A; capability=hotel-booking; registry page 2\"></textarea></label>"
            "<p class=\"subj-note\">Adding this creates CandidateAddress + QueryReceipt lineage only. It never fetches the target or grants a relationship, tool right, payment, or effect authority.</p>"
            "<button class=\"button primary\" type=\"submit\">Retain candidate <span>&rarr;</span></button></form><div id=\"candidate-result\"></div>",
            internet_count(db, NULL));
        w += (size_t)snprintf(page + w, sizeof(page) - w,
            "<section class=\"desk-q\"><div class=\"desk-qh\"><h3>Compile a source passport</h3><span class=\"desk-n\">one selected origin</span></div>"
            "<p class=\"desk-why\">This runs the existing MachineMembrane ladder against a retained public candidate's publisher root: robots first, then the cheapest declared carrier. It stores a passport only; it does not hydrate a ForeignTwin.</p>"
            "<form class=\"mk-commit\" hx-post=\"/operators/probe\" hx-target=\"#passport-result\" hx-swap=\"innerHTML\">"
            "<label>Candidate ID<input name=\"candidate_id\" type=\"number\" min=\"1\" required placeholder=\"1\"></label>"
            "<button class=\"button primary\" type=\"submit\">Compile passport <span>&rarr;</span></button></form><div id=\"passport-result\"></div></section>");
        if (db) {
            sqlite3_stmt *listed = NULL;
            w += (size_t)snprintf(page + w, sizeof(page) - w,
                "<section class=\"desk-q\"><div class=\"desk-qh\"><h3>Retained candidate frontier</h3><span class=\"desk-n\">private operator state</span></div>");
            if (sqlite3_prepare_v2(db,
                "SELECT c.id,c.address,c.candidate_kind,c.source_index,p.best_carrier,p.robots_allows FROM candidate_address c LEFT JOIN source_passport p ON p.candidate_id=c.id ORDER BY c.id DESC LIMIT 16",
                -1, &listed, NULL) == SQLITE_OK) {
                int any = 0;
                while (sqlite3_step(listed) == SQLITE_ROW && w < sizeof(page) - 4096) {
                    char eaddr[2048], ekind[256], esource[256]; const char *carrier;
                    html_escape((const char *)sqlite3_column_text(listed, 1), eaddr, sizeof(eaddr));
                    html_escape((const char *)sqlite3_column_text(listed, 2), ekind, sizeof(ekind));
                    html_escape((const char *)sqlite3_column_text(listed, 3), esource, sizeof(esource));
                    carrier = (const char *)sqlite3_column_text(listed, 4); any = 1;
                    w += (size_t)snprintf(page + w, sizeof(page) - w,
                        "<div class=\"desk-row\"><span class=\"desk-l\">#%d &middot; %s</span><span class=\"desk-s\">%s &middot; %s</span><span class=\"desk-a\">%s</span></div>",
                        sqlite3_column_int(listed, 0), ekind, eaddr, esource,
                        carrier ? carrier : "passport pending");
                }
                if (!any) w += (size_t)snprintf(page + w, sizeof(page) - w,
                    "<p class=\"desk-empty\">No candidates are retained yet. Start with one source/index result above.</p>");
            }
            if (listed) sqlite3_finalize(listed);
            w += (size_t)snprintf(page + w, sizeof(page) - w, "</section>");
        }
        if (db) sqlite3_close(db);
        w += (size_t)snprintf(page + w, sizeof(page) - w,
            "<div class=\"aperture\"><p class=\"ap-label\">Use the same closure through</p>"
            "<a class=\"ap-act\" href=\"/agents/a2a\"><span class=\"ap-do\">A2A maintained view</span><span class=\"ap-why\">candidate count, never an authorization list</span></a>"
            "<a class=\"ap-act\" href=\"/services/agent-registerable\"><span class=\"ap-do\">Service gaps</span><span class=\"ap-why\">only after exact protocol evidence exists</span></a></div>"
            "</article><script defer src=\"/assets/matrix-htmx.js\"></script>%s", SUBJ_FOOT);
        internet_html_reply(page, &resolved, "no-store", 1); return 1;
    }

    if (!strcmp(target, "/")) {
        db = internet_open(0);
        w = (size_t)snprintf(page, sizeof(page), SUBJ_HEAD, "Aurekai Internet Observatory");
        w += (size_t)snprintf(page + w, sizeof(page) - w,
            "<article class=\"subj\"><p class=\"eyebrow\"><i></i>Aurekai Internet Observatory &middot; Work</p>"
            "<h2 class=\"subj-name\">Find what the world already <em>publishes.</em></h2>"
            "<p class=\"subj-desc\">The Observatory compiles a real missing capability into registry, index, protocol and public-data discovery paths. Results begin as CandidateAddresses with receipts. They are not ForeignTwins, facts, agreements, or authorized tools.</p>"
            "<dl class=\"flow-facts\"><div><dt>Candidate frontier</dt><dd>%d</dd></div><div><dt>Public views</dt><dd>maintained</dd></div><div><dt>Hydration</dt><dd>separate owner</dd></div></dl>"
            "<div class=\"aperture\"><p class=\"ap-label\">Read a live maintained cut</p>"
            "<a class=\"ap-act is-strongest\" href=\"/agents/a2a\"><span class=\"ap-do\">Agents advertising A2A</span><span class=\"ap-why\">a card advertises an interface, not an agreement</span></a>"
            "<a class=\"ap-act\" href=\"/agents/mcp\"><span class=\"ap-do\">MCP candidates</span><span class=\"ap-why\">a descriptor is not a callable right</span></a>"
            "<a class=\"ap-act\" href=\"/gaps/discovery\"><span class=\"ap-do\">Discovery gaps</span><span class=\"ap-why\">what still needs an admitted source</span></a></div>"
            "<p class=\"subj-note\">The public aperture reports only maintained state. The operator frontier, raw query inputs, and all candidate addresses require an authenticated operator closure.</p></article>%s", internet_count(db, NULL), SUBJ_FOOT);
        if (db) sqlite3_close(db);
        internet_html_reply(page, &resolved, "no-cache", 0); return 1;
    }

    view_kind = internet_view_kind(target);
    if (view_kind && !strcmp(method, "GET")) {
        const char *label = target + 1;
        db = internet_open(0);
        w = (size_t)snprintf(page, sizeof(page), SUBJ_HEAD, "Internet maintained view");
        w += (size_t)snprintf(page + w, sizeof(page) - w,
            "<article class=\"subj\"><p class=\"eyebrow\"><i></i>StandingAddress &middot; %s</p>"
            "<h2 class=\"subj-name\">A maintained %s cut.</h2>"
            "<p class=\"subj-desc\">This view is derived from CandidateAddress state owned by the Observatory. It does not publish the candidate list or assert an origin is live; use an authenticated Operator Lab closure to review exact receipt lineage and request a separate ForeignTwin observation.</p>"
            "<dl class=\"flow-facts\"><div><dt>Matching candidates</dt><dd>%d</dd></div><div><dt>Evidence level</dt><dd>discovery-only</dd></div><div><dt>Effect authority</dt><dd>none</dd></div></dl>"
            "<p class=\"subj-note\">An empty view means no retained CandidateAddress currently matches this typed cut. It does not mean the Internet has no such service.</p></article>%s",
            label, view_kind, internet_count(db, view_kind), SUBJ_FOOT);
        if (db) sqlite3_close(db);
        internet_html_reply(page, &resolved, "no-cache", 0); return 1;
    }
    return 0;
}

static int desk_external_candidate_route(const char *occurrence, const char *url,
                                         char *out, size_t out_sz)
{
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    const char *dbpath = getenv("BONFYRE_EXTERNAL_ESTATE_DB");
    char bin[PATH_MAX];
    char *argv[9];
    if (!occurrence || !studio_safe_twin(occurrence) || !public_http_url(url) || !out || !out_sz)
        return -1;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    if (!dbpath || !*dbpath) dbpath = "/var/lib/bonfyre/external-estate.db";
    if (snprintf(bin, sizeof(bin), "%s/bonfyre-external-estate", bindir) >= (int)sizeof(bin) ||
        access(bin, X_OK) != 0) return 0;
    argv[0] = bin; argv[1] = (char *)"route";
    argv[2] = (char *)"--db"; argv[3] = (char *)dbpath;
    argv[4] = (char *)"--occurrence"; argv[5] = (char *)occurrence;
    argv[6] = (char *)"--url"; argv[7] = (char *)url; argv[8] = NULL;
    out[0] = '\0';
    return auth_call(argv, NULL, out, out_sz) == 0 && out[0] ? 1 : 0;
}

/* A person can review an address without claiming it came from discovery.
 * Only a subsequent authorized POST asks the existing ForeignTwin owner to
 * observe it. Candidate IDs continue through their separate exact-pair check. */
static int serve_desk_url_review(const char *url, const char *bearer, const char *auth_db) {
    char *body = NULL, *next = NULL, digest[65];
    size_t body_size = 0, next_size = 0;
    FILE *page = open_memstream(&body, &body_size), *continuation = NULL;
    if (!page) { html_error(503, "Review unavailable", "Try opening this address again."); return 1; }
    fprintf(page, SUBJ_HEAD, "Review a public address");
    fputs("<article class=\"subj desk\"><p class=\"eyebrow\">Studio</p><h2 class=\"subj-name\">Review a public address</h2>", page);
    if (!public_http_url(url)) {
        fputs("<p>Enter a complete http:// or https:// address. Nothing has been fetched.</p>", page);
    } else {
        char escaped[8192];
        html_escape(url, escaped, sizeof(escaped));
        fprintf(page, "<p class=\"subj-desc\">You supplied this address. Its contents have not been fetched.</p>"
                      "<p class=\"review-address\" style=\"overflow-wrap:anywhere\">%s</p>", escaped);
        if (session_valid(bearer, auth_db) && session_authorized(bearer, auth_db, "write")) {
            bf_sha256_hex((const uint8_t *)url, strlen(url), digest);
            fprintf(page, "<form class=\"actions\" action=\"/studio/v1/foreign/observe\" method=\"post\" "
                          "hx-post=\"/studio/v1/foreign/observe\" hx-target=\"#selected-foreign-result\" hx-swap=\"innerHTML\">"
                          "<input type=\"hidden\" name=\"twin_id\" value=\"url:%s\">"
                          "<input type=\"hidden\" name=\"url\" value=\"%s\">"
                          "<button class=\"button\" type=\"submit\">Read this address</button></form>"
                          "<p>This asks the source for a copy and keeps a receipt. It does not publish the source or verify its claims.</p>"
                          "<div id=\"selected-foreign-result\" aria-live=\"polite\"></div>", digest, escaped);
        } else if (session_valid(bearer, auth_db)) {
            fputs("<p>Your account can review this address. Reading and retaining its contents requires write access.</p>", page);
        } else {
            continuation = open_memstream(&next, &next_size);
            if (continuation) {
                fputs("/studio/?foreign_url=", continuation); url_query_value_to(continuation, url);
                if (!fclose(continuation)) {
                    char e_next[20000]; html_escape(next, e_next, sizeof(e_next));
                    fprintf(page, "<form action=\"/login\" method=\"get\"><input type=\"hidden\" name=\"next\" value=\"%s\">"
                                  "<button class=\"button\" type=\"submit\">Sign in to continue</button></form>", e_next);
                }
            }
        }
    }
    fputs("<p><a href=\"/\">Back to the home page</a></p></article>", page);
    fputs(SUBJ_FOOT, page);
    if (fclose(page)) html_error(503, "Review unavailable", "Try opening this address again.");
    else flow_reply(body);
    free(next); free(body); return 1;
}

static int serve_desk_external_candidate(const char *method, const char *target,
                                         const char *query, const char *bearer,
                                         const char *auth_db, const char *accept)
{
    static char body[131072], route_html[32768];
    char occurrence[97] = {0}, foreign_url[1400] = {0};
    char e_occurrence[256], e_url[3000];
    int signed_in, may_write, routed;
    size_t w;

    if (strcmp(target, "/studio/") && strcmp(target, "/studio")) return 0;
    if (strcmp(method, "GET") || !wants_html(accept) || !query) return 0;
    if (!query_value(query, "foreign_occurrence", occurrence, sizeof(occurrence)) &&
        !query_value(query, "foreign_url", foreign_url, sizeof(foreign_url))) return 0;
    if (!occurrence[0]) (void)query_value(query, "foreign_occurrence", occurrence, sizeof(occurrence));
    if (!foreign_url[0]) (void)query_value(query, "foreign_url", foreign_url, sizeof(foreign_url));

    if (!page_query_has_key(query, "foreign_occurrence"))
        return serve_desk_url_review(foreign_url, bearer, auth_db);

    signed_in = session_valid(bearer, auth_db);
    may_write = signed_in && session_authorized(bearer, auth_db, "write");
    routed = occurrence[0] && foreign_url[0]
        ? desk_external_candidate_route(occurrence, foreign_url, route_html, sizeof(route_html)) : -1;

    w = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, "External candidate · The desk");
    w += (size_t)snprintf(body + w, sizeof(body) - w,
      "<article class=\"subj desk\"><p class=\"eyebrow\"><i></i>Desk &middot; selected external candidate</p>"
      "<h2 class=\"subj-name\">Decide whether this candidate deserves an observation</h2>"
      "<p class=\"subj-desc\">The route is re-resolved by ExternalEstate before any observation control appears. "
      "A URL and occurrence carried by the browser are not trusted as a pair.</p>");

    if (routed == 1) {
        html_escape(occurrence, e_occurrence, sizeof(e_occurrence));
        html_escape(foreign_url, e_url, sizeof(e_url));
        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<section class=\"desk-q\"><div class=\"desk-qh\"><h3>Owner-checked candidate</h3>"
          "<span class=\"desk-n\">candidate only</span></div>%s"
          "<p class=\"desk-why\">No source admission, evidence promotion, provider admission, authority, "
          "payment, settlement or effect follows from this route.</p>", route_html);
        if (may_write) {
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<form class=\"actions\" hx-post=\"/studio/v1/foreign/observe\" "
              "hx-target=\"#selected-foreign-result\" hx-swap=\"innerHTML\">"
              "<input type=\"hidden\" name=\"twin_id\" value=\"%s\">"
              "<input type=\"hidden\" name=\"url\" value=\"%s\">"
              "<button class=\"button\" type=\"submit\">Observe this exact candidate</button></form>"
              "<div id=\"selected-foreign-result\"></div>", e_occurrence, e_url);
        } else {
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<p class=\"desk-empty\">Write authority is required to ask ForeignTwin to observe this source.</p>");
        }
        w += (size_t)snprintf(body + w, sizeof(body) - w, "</section>");
    } else {
        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<section class=\"desk-q\"><h3>Candidate route refused</h3>"
          "<p class=\"desk-why\">The occurrence and public URL did not resolve as one ExternalEstate candidate. "
          "Nothing was observed.</p></section>");
    }

    snprintf(body + w, sizeof(body) - w,
      "<div class=\"aperture\"><a class=\"ap-act\" href=\"/studio/\">"
      "<span class=\"ap-do\">Return to the full desk</span>"
      "<span class=\"ap-why\">open decisions remain owned by their native stores</span></a></div></article>%s",
      SUBJ_FOOT);
    flow_reply(body);
    return 1;
}

static int serve_desk(const char *method, const char *target,
                      const char *bearer, const char *auth_db, const char *accept)
{
    static char body[262144];
    DeskRow rows[8];
    int n, total, listed, signed_in, may_write;
    size_t w;

    if (strcmp(target, "/studio/") && strcmp(target, "/studio")) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!wants_html(accept)) return 0;   /* a machine still gets the descriptor */

    signed_in = session_valid(bearer, auth_db);
    may_write = signed_in && session_authorized(bearer, auth_db, "write");

    w = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, "The desk");
    w += (size_t)snprintf(body + w, sizeof(body) - w,
      "<article class=\"subj desk\">"
      "<p class=\"eyebrow\"><i></i>Desk &middot; what is open right now</p>"
      "<h2 class=\"subj-name\">What is waiting on a decision</h2>"
      "<p class=\"subj-desc\">Every line below is a real thing in a real store, held open because "
      "someone has to choose. This is not a menu of features: an empty queue means the institution "
      "has nothing of that kind open, which is a fact about the institution and not a gap in the "
      "page.%s</p>",
      may_write ? ""
        : signed_in ? " You are signed in without write scope, so you can see what is open and not close it."
                    : " You are not signed in, so you are seeing what is open publicly and cannot close any of it.");

    n = desk_unpriced_caps(rows, 8, &listed);
    w = desk_queue(body, sizeof(body), w, "Capabilities with no asking price",
      "Advertised and unpriced is not free &mdash; it answers not_for_sale to every grammar at once.",
      rows, n, n, may_write ? "Price it" : "See it",
      "Every advertised capability carries a price the service book agrees on.",
      /* An advertised capability is already public in the x402 catalogue, so
       * naming it here discloses nothing the catalogue does not. */
      1);

    n = desk_drafts(rows, 8, &total, bearer, auth_db);
    w = desk_queue(body, sizeof(body), w, "Written but not published",
      "Writing and publishing are separate acts, so a draft is a decision someone stopped short of.",
      rows, n, total, may_write ? "Open it" : "See it",
      "Nothing is written and waiting.", may_write);

    n = desk_unpriced_offers(rows, 8, &total);
    w = desk_queue(body, sizeof(body), w, "Offers in the book at zero",
      "A composed offer is created unpriced on purpose: the sentence proposed an exchange, it did not name a number.",
      rows, n, total, may_write ? "Price it" : "See the book",
      "Every offer in the book carries a number.", may_write);

    n = desk_open_work(rows, 8, &total);
    w = desk_queue(body, sizeof(body), w, "Work started and not finished",
      "A mission holds until it completes or fails; neither happens because a page was refreshed.",
      rows, n, total, "Follow it",
      "No mission is outstanding.", may_write);

    if (held_page_allowed(bearer, auth_db)) {
        n = desk_held_pages(rows, 8, &total);
        w = desk_queue(body, sizeof(body), w, "Selections brought from the browser",
            "Continue in the native workspace using the exact material held by Queue.",
            rows, n, total, "Review", "No browser selections await this institution.", 1);
    }

    n = desk_held_messages(rows, 8, &total);
    w = desk_queue(body, sizeof(body), w, "Messages written and held",
      "A message is written and kept. Nothing here sends one, so each waits for a person to hand it over themselves or drop it.",
      rows, n, total, "Held",
      "No message is written and waiting.",
      /* What a message says and whom it names is not a stranger's to read. */
      may_write);

    w += (size_t)snprintf(body + w, sizeof(body) - w,
      "<div class=\"aperture\"><p class=\"ap-label\">From here</p>"
      "<a class=\"ap-act is-strongest\" href=\"/make\"><span class=\"ap-do\">Make something</span>"
      "<span class=\"ap-why\">say it plainly and it is read for what it proposes to bring into being</span></a>"
      "<a class=\"ap-act is-strong\" href=\"/flow\"><span class=\"ap-do\">Move through what exists</span>"
      "<span class=\"ap-why\">one gesture across places, capabilities, notes and observed sources</span></a>"
      "<a class=\"ap-act\" href=\"/market\"><span class=\"ap-do\">Open the service book</span>"
      "<span class=\"ap-why\">the one book every commerce grammar quotes from</span></a>"
      "<a class=\"ap-act\" href=\"/surfaces\"><span class=\"ap-do\">See which shapes are earning a page</span>"
      "<span class=\"ap-why\">traces become candidates only by repeating across distinct viewers</span></a>"
      "<a class=\"ap-act\" href=\"/studio/controls\"><span class=\"ap-do\">Every control, unsorted</span>"
      "<span class=\"ap-why\">the full operator panel: observing, publishing, compiling, capping, pricing, "
      "the ledger and the estate. Nothing here is chosen for you, which is why it is not the front door</span></a></div>");

    snprintf(body + w, sizeof(body) - w, "</article>%s", SUBJ_FOOT);
    flow_reply(body);
    return 1;
}

/* The membrane, as a surface.
 *
 * Both halves of the machine world were only ever visible as a script's stdout:
 * which foreign sources we can read and at what cost, and which machine readers
 * arrive and what we answered them. Neither is a developer detail. The inbound
 * half is the institution's sensor coverage, and the outbound half is the
 * plainest measure of whether anything we publish is reaching anyone.
 *
 * Rendered from the membrane store, so it states what is recorded rather than
 * probing on request: a page that reaches out to five external sites when a
 * person opens it is a page that fails when one of them is slow.
 */
static int serve_membrane(const char *method, const char *target,
                          const char *bearer, const char *auth_db,
                          const char *accept)
{
    static char body[262144];
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL;
    size_t w = 0;
    const char *dbp = getenv("BONFYRE_MEMBRANE_DB");
    int signed_in, superuser;

    if (strcmp(target, "/membrane") && strcmp(target, "/membrane/")) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!wants_html(accept)) return 0;
    if (!dbp || !*dbp) dbp = "/var/lib/bonfyre/state/membrane.db";
    signed_in = session_valid(bearer, auth_db);
    superuser = signed_in && session_institution_authorized(bearer, auth_db, "bonfyre", "admin");

    w = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, "The machine membrane");
    w = machine_head(body, sizeof(body), w, "/membrane", "The machine membrane",
                     "Which machines read Aurekai, and which foreign sources "
                     "Aurekai can read.", NULL, NULL);
    w += (size_t)snprintf(body + w, sizeof(body) - w,
      "<article class=\"subj\">"
      "<p class=\"eyebrow\"><i></i>Membrane &middot; the machine world, both directions</p>"
      "<h2 class=\"subj-name\">What machines read here, and what we can read</h2>"
      "<p class=\"subj-desc\">The website is one projection for people. The same state "
      "is consumed by crawlers, preview bots, retrieval systems and agents, and the "
      "same institution reads the machine-readable internet back. This is both halves, "
      "measured rather than assumed.</p>");

    if (sqlite3_open_v2(dbp, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<p class=\"desk-empty\">The membrane store has not been written yet. "
          "It is filled by probing sources and folding the access log; until then "
          "this page states nothing rather than guessing.</p>");
        if (db) sqlite3_close(db);
        snprintf(body + w, sizeof(body) - w, "</article>%s", SUBJ_FOOT);
        flow_reply(body);
        return 1;
    }

    /* Outbound: who arrives, and what they were answered. */
    w += (size_t)snprintf(body + w, sizeof(body) - w,
      "<section class=\"desk-q\"><div class=\"desk-qh\"><h3>Machines that read us</h3>"
      "<span class=\"desk-n\">%s</span></div>"
      "<p class=\"desk-why\">Declared identity only. A user agent is a claim, not a "
      "credential, and nothing here is authorised on the strength of one.</p>",
      "by purpose");
    if (superuser && sqlite3_prepare_v2(db,
          "SELECT family, sum(requests), sum(answered_200), sum(answered_404) "
          "FROM machine_reader GROUP BY family ORDER BY 2 DESC", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW && w < sizeof(body) - 2048) {
            const char *fam = (const char *)sqlite3_column_text(st, 0);
            long long n = sqlite3_column_int64(st, 1);
            long long ok = sqlite3_column_int64(st, 2);
            long long miss = sqlite3_column_int64(st, 3);
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
              "<span class=\"desk-s\">%lld requests &middot; %lld answered &middot; "
              "%lld found nothing</span>"
              "<span class=\"desk-a\">%lld%%</span></div>",
              fam ? fam : "unclassified", n, ok, miss,
              n ? (100 * ok / n) : 0);
        }
    }
    if (st) { sqlite3_finalize(st); st = NULL; }
    if (!superuser) {
        long long readers = 0;
        if (sqlite3_prepare_v2(db, "SELECT count(*) FROM machine_reader", -1, &st, NULL) == SQLITE_OK &&
            sqlite3_step(st) == SQLITE_ROW) readers = sqlite3_column_int64(st, 0);
        if (st) { sqlite3_finalize(st); st = NULL; }
        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<p class=\"desk-empty\">%lld declared machine reader%s are recorded. "
          "Reader identity and path detail stay inside the Bonfÿre control closure.</p>",
          readers, readers == 1 ? "" : "s");
    }
    w += (size_t)snprintf(body + w, sizeof(body) - w, "</section>");

    /* Inbound: what we can read, ordered by what it costs. */
    w += (size_t)snprintf(body + w, sizeof(body) - w,
      "<section class=\"desk-q\"><div class=\"desk-qh\"><h3>Sources we read</h3>"
      "<span class=\"desk-n\">by cost</span></div>"
      "<p class=\"desk-why\">Each source is read through the cheapest representation it "
      "already publishes. A low rung is push or a feed; a high one means nothing "
      "machine-legible is offered and reading it costs a full fetch and an "
      "interpretation.</p>");
    if (superuser && sqlite3_prepare_v2(db,
          "SELECT root, best_carrier, rung, declined_reason FROM source_passport "
          "ORDER BY rung ASC, root", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW && w < sizeof(body) - 2048) {
            const char *root = (const char *)sqlite3_column_text(st, 0);
            const char *carrier = (const char *)sqlite3_column_text(st, 1);
            int rung = sqlite3_column_int(st, 2);
            const char *declined = (const char *)sqlite3_column_text(st, 3);
            char e_root[512], e_why[512];
            html_escape(root ? root : "", e_root, sizeof(e_root));
            html_escape(declined ? declined : "", e_why, sizeof(e_why));
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
              "<span class=\"desk-s\">rung %d &middot; %s%s%s</span>"
              "<span class=\"desk-a\">%s</span></div>",
              e_root, rung, carrier ? carrier : "?",
              declined ? " &middot; " : "", declined ? e_why : "",
              rung <= 4 ? "cheap" : "expensive");
        }
    }
    if (st) sqlite3_finalize(st);
    if (!superuser) {
        long long sources = 0, cheap = 0;
        st = NULL;
        if (sqlite3_prepare_v2(db,
              "SELECT count(*), coalesce(sum(CASE WHEN rung <= 4 THEN 1 ELSE 0 END),0) FROM source_passport",
              -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) {
            sources = sqlite3_column_int64(st, 0); cheap = sqlite3_column_int64(st, 1);
        }
        if (st) sqlite3_finalize(st);
        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<p class=\"desk-empty\">%lld source passport%s recorded; %lld currently have a "
          "lower-cost published representation. Source identities and declared protocol candidates "
          "stay inside the Bonfÿre control closure.</p>",
          sources, sources == 1 ? "" : "s", cheap);
    }
    if (superuser) {
        st = NULL;
        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<section class=\"desk-q\"><div class=\"desk-qh\"><h3>Declared external carriers</h3>"
          "<span class=\"desk-n\">candidate only</span></div>"
          "<p class=\"desk-why\">These are public descriptors retained by the membrane. "
          "They are not live endpoints, admitted providers, authorized tools, payment routes, "
          "identity assertions, or effect rights.</p>");
        if (sqlite3_prepare_v2(db,
              "SELECT root,family,descriptor,active FROM source_protocol_descriptor "
              "ORDER BY active DESC,family,root LIMIT 200", -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW && w < sizeof(body) - 2048) {
                const char *root = (const char *)sqlite3_column_text(st, 0);
                const char *family = (const char *)sqlite3_column_text(st, 1);
                const char *descriptor = (const char *)sqlite3_column_text(st, 2);
                int active = sqlite3_column_int(st, 3);
                char e_root[512], e_family[256], e_descriptor[768];
                html_escape(root ? root : "", e_root, sizeof(e_root));
                html_escape(family ? family : "", e_family, sizeof(e_family));
                html_escape(descriptor ? descriptor : "", e_descriptor, sizeof(e_descriptor));
                w += (size_t)snprintf(body + w, sizeof(body) - w,
                  "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                  "<span class=\"desk-s\">%s &middot; %s</span>"
                  "<span class=\"desk-a\">%s</span></div>",
                  e_family, e_root, e_descriptor, active ? "active" : "cooled");
            }
        } else {
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<p class=\"desk-empty\">No descriptor candidate relation is retained yet.</p>");
        }
        if (st) { sqlite3_finalize(st); st = NULL; }
        w += (size_t)snprintf(body + w, sizeof(body) - w, "</section>");
    }
    sqlite3_close(db);
    w += (size_t)snprintf(body + w, sizeof(body) - w, "</section>");

    w += (size_t)snprintf(body + w, sizeof(body) - w,
      "<p class=\"subj-note\">A passport records how a source publishes and what our "
      "probes observed &mdash; never what its documents assert. A source that answers a "
      "challenge page, or whose robots cannot be reached, is recorded as declining; "
      "that refusal is a fact about the source and is respected rather than routed "
      "around.%s</p>",
      superuser ? "" : " The Bonfÿre control closure may inspect source and reader detail.");
    w += (size_t)snprintf(body + w, sizeof(body) - w,
      "<div class=\"aperture\"><p class=\"ap-label\">From here</p>"
      "<a class=\"ap-act is-strongest\" href=\"/protocols\"><span class=\"ap-do\">Every carrier we speak</span>"
      "<span class=\"ap-why\">eighteen grammars over one state</span></a>"
      "<a class=\"ap-act\" href=\"/sitemap.xml\"><span class=\"ap-do\">What we offer crawlers</span>"
      "<span class=\"ap-why\">the sitemap family, and the feed beside it</span></a>"
      "<a class=\"ap-act\" href=\"/surfaces\"><span class=\"ap-do\">Which shapes earn a page</span>"
      "<span class=\"ap-why\">traces become candidates only by repeating</span></a></div>");
    snprintf(body + w, sizeof(body) - w, "</article>%s", SUBJ_FOOT);
    flow_reply(body);
    return 1;
}

static int serve_flow(const char *method, const char *target, const char *query,
                      const char *bearer, const char *auth_db)
{
    static char body[262144], card[65536];
    const char *bindir = bindir_or_default();
    char raw[32];
    int i = 0;
    (void)bearer; (void)auth_db;

    /* One native material/quantity implementation is used by the Recipe CLI,
     * the mixed feed and this existing subject-page grammar. No client planner. */
    if (!strcmp(target, "/recipes/plan") || !strcmp(target, "/recipes/plan/")) {
        char spec[BF_RECIPE_WEEK_SPEC]={0}, add_key[256]={0}, edit_key[256]={0};
        char copies_text[32]={0}, search[512]={0}, offset_text[32]={0}, format[16]={0};
        static char content[524288],page[600000];
        const char *plan_query=query?query:"";
        BfRecipeWeek week; long offset=0,copies=1; char *end;
        if (strcmp(method,"GET")) { protocol_reply(405,"text/plain","GET required\n");return 1; }
        if (bf_lex_query_value(plan_query,"items",spec,sizeof(spec),1)<0 ||
            bf_lex_query_value(plan_query,"add",add_key,sizeof(add_key),1)<0 ||
            bf_lex_query_value(plan_query,"edit",edit_key,sizeof(edit_key),1)<0 ||
            bf_lex_query_value(plan_query,"copies",copies_text,sizeof(copies_text),1)<0 ||
            bf_lex_query_value(plan_query,"q",search,sizeof(search),1)<0 ||
            bf_lex_query_value(plan_query,"offset",offset_text,sizeof(offset_text),1)<0 ||
            bf_lex_query_value(plan_query,"format",format,sizeof(format),1)<0 ||
            bf_recipe_week_parse(spec,&week)) {
            protocol_reply(400,"text/plain","Invalid or oversized preparation selection\n");return 1;
        }
        if (*offset_text) { errno=0;offset=strtol(offset_text,&end,10);if(errno||*end||offset<0||offset>1000000){protocol_reply(400,"text/plain","Invalid offset\n");return 1;} }
        if (*copies_text) { errno=0;copies=strtol(copies_text,&end,10);if(errno||*end||copies<0||copies>100){protocol_reply(400,"text/plain","Invalid copies\n");return 1;} }
        int already_selected=0;
        for(int k=0;k<week.count;k++)if(!strcmp(week.items[k].key,add_key))already_selected=1;
        if ((*add_key && *edit_key) || (*add_key && !already_selected && bf_recipe_week_set(&week,add_key,1)) ||
            (*edit_key && bf_recipe_week_set(&week,edit_key,(int)copies))) {
            protocol_reply(400,"text/plain","Invalid selection edit or too many selections\n");return 1;
        }
        if (*format) {
            if (strcmp(format,"list") && strcmp(format,"brief")){protocol_reply(400,"text/plain","Unknown plan format\n");return 1;}
            char *text=NULL;size_t size=0;FILE *stream=open_memstream(&text,&size);int failed=!stream;
            if(stream){failed=bf_recipe_week_export(bf_recipe_material_path(),&week,!strcmp(format,"brief"),stream);if(fclose(stream))failed=1;}
            protocol_reply(failed?422:200,!strcmp(format,"brief")?"text/plain; charset=utf-8":"text/tab-separated-values; charset=utf-8",failed?"No complete verified plan is available\n":text);
            free(text);return 1;
        }
        int can_save=session_authorized(bearer,auth_db,"write");
        if(bf_recipe_week_view(bf_recipe_material_path(),&week,search,(int)offset,can_save,content,sizeof(content))){protocol_reply(422,"text/plain","The complete plan could not be verified\n");return 1;}
        int n=snprintf(page,sizeof(page),SUBJ_HEAD,"Household preparation plan");
        if(n<0||(size_t)n>=sizeof(page)||strlen(content)+strlen(SUBJ_FOOT)+1>sizeof(page)-(size_t)n){protocol_reply(500,"text/plain","Plan exceeds surface capacity\n");return 1;}
        snprintf(page+(size_t)n,sizeof(page)-(size_t)n,"%s%s",content,SUBJ_FOOT);flow_reply(page);return 1;
    }
    if (!strcmp(target, "/recipes") || !strcmp(target, "/recipes/")) {
        char key[256] = {0}, search[512] = {0}, sets[32] = {0}, format[32] = {0}, offset_text[32] = {0};
        static char material[131072];
        long copies = 1, offset = 0;
        if (strcmp(method, "GET")) { protocol_reply(405, "text/plain", "GET required\n"); return 1; }
        query_value(query, "id", key, sizeof(key));
        query_value(query, "q", search, sizeof(search));
        query_value(query, "format", format, sizeof(format));
        if (query_value(query, "offset", offset_text, sizeof(offset_text))) {
            char *end; errno = 0; offset = strtol(offset_text, &end, 10);
            if (errno || end == offset_text || *end || offset < 0 || offset > 1000000) {
                protocol_reply(400, "text/plain", "Invalid recipe offset\n"); return 1;
            }
        }
        if (query_value(query, "copies", sets, sizeof(sets))) {
            char *end; errno = 0; copies = strtol(sets, &end, 10);
            if (errno || end == sets || *end || copies < 1 || copies > 100) {
                protocol_reply(400, "text/plain", "Sets must be an integer from 1 to 100\n"); return 1;
            }
        }
        if (!strcmp(format, "list") || !strcmp(format, "brief")) {
            char *list = NULL; size_t length = 0; FILE *stream = open_memstream(&list, &length);
            int failed = !stream;
            if (stream) { failed = !strcmp(format, "brief") ? bf_recipe_material_brief(bf_recipe_material_path(), key, (int)copies, stream) : bf_recipe_material_list(bf_recipe_material_path(), key, (int)copies, stream); if (fclose(stream)) failed = 1; }
            protocol_reply(failed ? 422 : 200, !strcmp(format, "brief") ? "text/plain; charset=utf-8" : "text/tab-separated-values; charset=utf-8", failed ? "No complete ingredient plan is available\n" : list);
            free(list); return 1;
        }
        if (format[0] || bf_recipe_material_view_page(bf_recipe_material_path(), key, search, (int)copies, (int)offset, material, sizeof(material))) {
            protocol_reply(422, "text/plain", "Recipe material or query could not be verified\n"); return 1;
        }
        size_t written = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, "Recipes");
        if (written >= sizeof(body) || strlen(material) + strlen(SUBJ_FOOT) + 1 > sizeof(body) - written) { protocol_reply(500, "text/plain", "Recipe view exceeds the surface bound\n"); return 1; }
        snprintf(body + written, sizeof(body) - written, "%s%s", material, SUBJ_FOOT);
        flow_reply(body); return 1;
    }
    if (strncmp(target, "/flow", 5)) return 0;
    if (strcmp(method, "GET")) return 0;

    /* One position in the flow. The gesture that gets here never changes. */
    if (!strncmp(target, "/flow/at", 8)) {
        int kind_i, nth, made = 0, tries = 0;
        if (query_value(query, "i", raw, sizeof(raw))) i = atoi(raw);
        if (i < 0) i = 0;
        if (i > 100000) i = 100000;
        /* Interleave kinds so the object grammar changes on every move, and
         * skip a kind that has nothing at this depth rather than showing a gap. */
        for (tries = 0; tries < FLOW_KIND_N && !made; tries++) {
            kind_i = (i + tries) % FLOW_KIND_N;
            nth = (i + tries) / FLOW_KIND_N;
            made = FLOW_CARD_RENDER[kind_i](bindir, nth, card, sizeof(card));
        }
        if (!made) {
            snprintf(body, sizeof(body),
              "<div class=\"fl-end\"><p>That is everything this world holds so far.</p>"
              "<a class=\"button quiet\" href=\"/flow/at?i=0\" hx-get=\"/flow/at?i=0\" "
              "hx-target=\"#flow-card\" hx-swap=\"innerHTML\">Start again <span>&rarr;</span></a></div>");
            flow_reply(body);
            return 1;
        }
        snprintf(body, sizeof(body),
          "%s"
          "<div class=\"fl-move\">"
          "<button class=\"fl-next\" hx-get=\"/flow/at?i=%d\" hx-target=\"#flow-card\" "
          "hx-swap=\"innerHTML\" data-flow-next=\"%d\">Next <span>&darr;</span></button>"
          "<span class=\"fl-pos\">%d</span>"
          "</div>", card, i + 1, i + 1, i + 1);
        flow_reply(body);
        return 1;
    }

    if (!strcmp(target, "/flow") || !strcmp(target, "/flow/")) {
        size_t w = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, "Flow");
        snprintf(body + w, sizeof(body) - w,
          "<article class=\"subj flow-shell\">"
          "<p class=\"eyebrow\"><i></i>Flow &middot; one gesture, many kinds of thing</p>"
          "<h2 class=\"subj-name\">Keep moving.</h2>"
          "<p class=\"subj-desc\">The way you move stays the same. What arrives does not: a place, then "
          "something you can buy, then something written down, then something admitted from outside. "
          "Each is drawn in the grammar of its own kind, and each is a real object you can open.</p>"
          "<div id=\"flow-card\" hx-get=\"/flow/at?i=0\" hx-trigger=\"load\" hx-swap=\"innerHTML\">"
          "<p class=\"flow-empty\">Loading&hellip;</p></div>"
          "<p class=\"subj-note\">Position is in the address, so where you are is shareable and the sequence "
          "is the same for everyone. Nothing here is ranked for you.</p>"
          "</article>"
          "<script defer src=\"/assets/matrix-htmx.js\"></script>"
          "<script>"
          "/* Same-origin HTMX requests carry the HttpOnly session cookie. */"
          "/* One gesture: the same key and the same control, whatever arrives. */"
          "document.addEventListener('keydown',e=>{"
          "if(e.key==='ArrowDown'||e.key===' '||e.key==='j'){"
          "const b=document.querySelector('[data-flow-next]');"
          "if(b){e.preventDefault();b.click();}}});"
          "</script>%s", SUBJ_FOOT);
        flow_reply(body);
        return 1;
    }
    return 0;
}

static int serve_studio(BfMatrix *matrix, const char *method, const char *target, const char *body, const char *bearer, const char *auth_db, const char *host, const char *origin_method, const char *institution, const char *accept) {
    int private_foreign_observe = private_foreign_root[0] && !strcmp(target, "/foreign/observe");
    int self_publish = !strcmp(target, "/studio/v1/matrix/self-publish");
    int self_foreign_observe = !strcmp(target, "/studio/v1/self/foreign/observe");
    int self_foreign_workspace = !strcmp(target, "/studio/v1/self/foreign") ||
                                 !strcmp(target, "/studio/v1/self/foreign/inspect");
    studio_html_mode = accept && strstr(accept, "text/html") != NULL;
    if (strcmp(target, "/_bonfyre_session") && strcmp(target, "/_bonfyre_write") &&
        strcmp(target, "/_bonfyre_institution") && strncmp(target, "/studio/v1/", 11) && !private_foreign_observe) return 0;
    /* Institution-scoped reads, gated before any route handler runs.
     *
     * This check previously sat a thousand lines further down, after the
     * handlers it was meant to guard. /studio/v1/economy/budget answered its
     * own parameter-validation error to a read-only session and never reached
     * the gate at all -- no data escaped, because that route returns data only
     * to a POST that nginx refuses, but a read-only account learned the
     * endpoint exists and what it takes. A gate that a route can be dispatched
     * ahead of is not a gate; it is a gate for the routes that happen to be
     * declared after it.
     *
     * Cannot infer: write scope -> authority over what is read here. This
     * decides who may look, and the organs behind it still own their effects. */
    if (!strncmp(target, "/studio/v1/economy/", 19) ||
        !strncmp(target, "/studio/v1/models/", 18) ||
        !strncmp(target, "/studio/v1/ledger/", 18) ||
        !strcmp(target, "/studio/v1/frappe-lineages")) {
        /* Authentication precedes authorization.  Asking a browser with no
         * session to sign in must be a 401; returning a scope refusal first
         * makes the login boundary look broken and confuses an anonymous
         * request with an authenticated self-scoped account. */
        if (!session_valid(bearer, auth_db)) {
            studio_reply(401, "application/json", "{\"error\":\"session_required\"}\n");
            return 1;
        }
        if (!session_authorized(bearer, auth_db, "write")) {
            studio_reply(403, "application/json",
              "{\"error\":\"institution_scope_required\","
              "\"note\":\"this projection reads the institution's economics, model estate or "
              "derivation ledger; a self-scoped session reads its own closure at "
              "/studio/v1/profile and /studio/v1/closure\"}\n");
            return 1;
        }
    }
    if (strcmp(method, "GET") && !self_foreign_workspace && !( !strcmp(method, "POST") && (private_foreign_observe || self_publish || self_foreign_observe || !strcmp(target, "/studio/v1/foreign/observe") || !strcmp(target, "/studio/v1/matrix/publish") || !strcmp(target, "/studio/v1/economy/budget") || !strcmp(target, "/studio/v1/fleming/watch") || !strcmp(target, "/studio/v1/clients") || !strcmp(target, "/studio/v1/work/claim") || !strcmp(target, "/studio/v1/work/complete") || !strcmp(target, "/studio/v1/economy/cost") || !strcmp(target, "/studio/v1/fleming/opportunity") || !strcmp(target, "/studio/v1/matrix/standing") || !strcmp(target, "/studio/v1/matrix/presence") || !strcmp(target, "/studio/v1/matrix/alias") || !strcmp(target, "/studio/v1/identity/group") || !strcmp(target, "/studio/v1/work/escalate") || !strcmp(target, "/studio/v1/work/supersede") || !strcmp(target, "/studio/v1/economy/offer") || !strcmp(target, "/studio/v1/model/prepare") || !strcmp(target, "/studio/v1/frappe/compile") || !strcmp(target, "/studio/v1/assets/observe") || !strcmp(target, "/studio/v1/theme")) )) { studio_reply(503, "application/json", "{\"error\":\"read_only_surface\"}\n"); return 1; }
    if (!session_valid(bearer, auth_db)) { studio_reply(401, "application/json", "{\"error\":\"session_required\"}\n"); return 1; }
    /* A mutation needs write scope. Being signed in is not authority. */
    if (strcmp(method, "GET") && !session_authorized(bearer, auth_db, "write") &&
        !((self_publish || self_foreign_observe || self_foreign_workspace) && session_authorized(bearer, auth_db, "self"))) {
        studio_reply(403, "application/json",
            "{\"error\":\"write_scope_required\",\"reason\":\"this account is authenticated but not authorized to change anything here\"}\n");
        return 1;
    }
    if (!strcmp(target, "/_bonfyre_session")) {
        /* nginx forwards the original verb here because an auth subrequest is
         * always a GET. A read only needs a session; a mutation needs write
         * scope, which is what stops a newly created account from editing
         * another person's published page. */
        if (origin_method && *origin_method && strcmp(origin_method, "GET") && strcmp(origin_method, "HEAD") &&
            !session_authorized(bearer, auth_db, "write")) {
            studio_reply(403, "application/json",
                "{\"error\":\"write_scope_required\",\"reason\":\"this account is authenticated but not authorized to change content\"}\n");
            return 1;
        }
        studio_reply(200, "application/json", "{\"valid\":true}\n"); return 1;
    }
    /* Nginx invokes this only from exact protected institutional hosts and
     * supplies the static host→institution binding.  Read/write is selected
     * from the original request verb because auth_request itself is always a
     * GET.  The native owner evaluates the grant at request time, so revoke
     * stops an otherwise valid bearer immediately. */
    if (!strcmp(target, "/_bonfyre_institution")) {
        const char *required_role = "read";
        if (!institution || !*institution) {
            studio_reply(403, "application/json", "{\"error\":\"institution_required\"}\n"); return 1;
        }
        if (origin_method && *origin_method && strcmp(origin_method, "GET") && strcmp(origin_method, "HEAD"))
            required_role = "write";
        if (session_institution_authorized(bearer, auth_db, institution, required_role)) {
            studio_reply(200, "application/json", "{\"institution_authorized\":true}\n");
        } else {
            studio_reply(403, "application/json",
                "{\"error\":\"institution_authority_required\",\"reason\":\"a valid Bonfyre session does not grant client-estate access\"}\n");
        }
        return 1;
    }
    /* Subrequest target for nginx: succeeds only with write scope. */
    if (!strcmp(target, "/_bonfyre_write")) {
        if (session_authorized(bearer, auth_db, "write")) { studio_reply(200, "application/json", "{\"write\":true}\n"); }
        else { studio_reply(403, "application/json", "{\"error\":\"write_scope_required\"}\n"); }
        return 1;
    }
    const char *bindir = getenv("BONFYRE_BIN_DIR"); if (!bindir || !*bindir) bindir = "/usr/local/bin";
    char bin[PATH_MAX], output[262144]; char *argv[6] = { bin, NULL, NULL, NULL, NULL, NULL };
    const char *ctype = "application/json; charset=utf-8";
    /* One human-readable entry into the same recursive closure used by web,
     * RO-Crate, Zarr and MCP. This is deliberately a status of owner planes,
     * not a JSON inspector and not a fake aggregation of their state. */
    if (!strcmp(target, "/studio/v1/closure")) {
        BfMatrixAddressResolution resolution; char crate[768], zarr[768];
        if (!matrix_root_resolution(matrix, host, &resolution) || !closure_urls(host, crate, zarr)) {
            studio_reply(503, "application/json", "{\"error\":\"matrix_closure_unavailable\"}\n"); return 1;
        }
        if (studio_html_mode) {
            char fragment[8192], esubject[768], epresence[768], eterritory[128], escope[128], ecrate[1600], ezarr[1600];
            html_escape(resolution.subject_id, esubject, sizeof(esubject));
            html_escape(resolution.presence_id, epresence, sizeof(epresence));
            html_escape(resolution.territory, eterritory, sizeof(eterritory));
            html_escape(bf_matrix_scope_name(resolution.scope), escope, sizeof(escope));
            html_escape(crate, ecrate, sizeof(ecrate)); html_escape(zarr, ezarr, sizeof(ezarr));
            int n = snprintf(fragment, sizeof(fragment),
                "<section class=\"studio-result is-ok\" aria-live=\"polite\"><span class=\"result-kicker\">Recursive Matrix closure</span>"
                "<h3>%s is one shared closure</h3><p>This room, Content Studio, Frappe grammar, agent carriers, and the public web begin from the same resolved subject. Only a named owner may add its plane.</p>"
                "<dl class=\"result-facts\"><div><dt>Subject</dt><dd>%s</dd></div><div><dt>Presence</dt><dd>%s</dd></div><div><dt>Territory</dt><dd>%s · %s</dd></div>"
                "<div><dt>Semantic plane</dt><dd>Resolved by Matrix</dd></div><div><dt>Material / latent</dt><dd>Await their body and passport owners</dd></div>"
                "<div><dt>Work / economy</dt><dd>Await WorkGraph and financial owners</dd></div></dl>"
                "<p class=\"hint\">Machine forms are linked, not shown as a dashboard: <a href=\"%s\">RO-Crate</a> · <a href=\"%s\">Zarr group</a>. Neither link grants authority.</p></section>",
                esubject, esubject, epresence, eterritory, escope, ecrate, ezarr);
            if (n < 0 || n >= (int)sizeof(fragment)) { studio_reply(503, "application/json", "{\"error\":\"matrix_closure_overflow\"}\n"); return 1; }
            printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s", fragment);
            return 1;
        }
        printf("HTTP/1.1 200 OK\r\nContent-Type: application/ld+json; charset=utf-8\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n");
        return bf_matrix_closure_emit_ro_crate(stdout, matrix, &resolution, crate, zarr) == 0;
    }
    /* Human x402 surface: the protocol endpoint remains a machine JSON
     * carrier, while Studio reads the same capability/service book as an HTMX
     * fragment. A capability with no sell price is deliberately not shown as
     * free or purchasable. */
    if (!strcmp(target, "/studio/v1/x402/catalog")) {
        static char xbody[8192];
        int w = snprintf(xbody, sizeof(xbody),
            "{\"network\":\"%s\",\"facilitator_configured\":%s,\"capabilities\":[",
            x402_network(), x402_facilitator()[0] ? "true" : "false");
        for (int i = 0; i < X402_CATALOG_N && w > 0 && (size_t)w < sizeof(xbody) - 512; i++) {
            double usd = 0.0; int priced = x402_price_usd(X402_CATALOG[i].name, &usd);
            w += snprintf(xbody + w, sizeof(xbody) - (size_t)w,
                "%s{\"capability\":\"%s\",\"description\":\"%s\",\"for_sale\":%s,\"price_usd\":%.2f}",
                i ? "," : "", X402_CATALOG[i].name, X402_CATALOG[i].summary,
                priced && usd > 0.0 ? "true" : "false", priced ? usd : 0.0);
        }
        if (w < 0 || (size_t)w >= sizeof(xbody) - 128) {
            studio_reply(503, "application/json", "{\"error\":\"x402_catalog_overflow\"}\n"); return 1;
        }
        snprintf(xbody + w, sizeof(xbody) - (size_t)w,
            "],\"limit\":\"A listed price is an agent quote only. Settlement still requires a configured facilitator and verified X-PAYMENT.\"}\n");
        studio_reply(200, ctype, xbody); return 1;
    }
    if (self_foreign_workspace) return serve_self_foreign_workspace(method, target, body, bearer, auth_db, host);
    if (self_publish) return studio_self_publish(matrix, body, bearer, auth_db, host);
    if (!strcmp(target, "/studio/v1/matrix/publish")) return studio_publish(matrix, body, bearer, auth_db, host);
    if (!strcmp(target, "/studio/v1/economy/budget")) {
        char recipe[121], cap[32];
        if (!studio_json_string(body, "recipe", recipe, sizeof(recipe)) || !studio_safe_recipe(recipe) ||
            !studio_json_string(body, "usd_cap", cap, sizeof(cap)) || !studio_safe_usd(cap, cap)) {
            studio_reply(503, "application/json", "{\"error\":\"recipe and non-negative usd_cap required\"}\n"); return 1;
        }
        snprintf(bin, sizeof(bin), "%s/bonfyre-economy", bindir);
        char *budget_argv[] = { bin, "budget", "set", recipe, cap, NULL };
        if (access(bin, X_OK) != 0 || auth_call(budget_argv, NULL, output, sizeof(output)) != 0) { studio_reply(503, "application/json", "{\"error\":\"native_economy_unavailable\"}\n"); return 1; }
        studio_reply(200, "text/plain; charset=utf-8", output); return 1;
    }
    if (!strcmp(target, "/studio/v1/foreign/observe") || private_foreign_observe || self_foreign_observe) {
        char twin[97], url[1400], receipt[PATH_MAX], retained[PATH_MAX], stamp[32], self_root[PATH_MAX];
        char host_norm[256];
        const char *foreign_root = private_foreign_root[0] ? private_foreign_root : getenv("BONFYRE_FOREIGN_ROOT");
        if (self_foreign_observe) {
            if (!session_self_authoring(bearer, auth_db) || !lower_host(host, host_norm) || strcmp(host_norm, "aurekai.life") ||
                !self_foreign_root_for(bearer, auth_db, self_root, sizeof(self_root))) {
                studio_reply(403, "application/json", "{\"error\":\"self_private_foreign_requires_life_self_scope\"}\n"); return 1;
            }
            foreign_root = self_root;
        }
        if (!studio_json_string(body, "twin_id", twin, sizeof(twin)) || !studio_safe_twin(twin) ||
            !studio_json_string(body, "url", url, sizeof(url)) ||
            (strncmp(url, "https://", 8) && strncmp(url, "http://", 7))) {
            studio_reply(503, "application/json", "{\"error\":\"valid twin_id and public http(s) url required\"}\n"); return 1;
        }
        iso_utc_now(stamp, sizeof(stamp));
        for (char *p = stamp; *p; p++) if (!isdigit((unsigned char)*p)) *p = '-';
        if (!foreign_root || !*foreign_root) foreign_root = "/var/lib/bonfyre/foreign";
        if (private_foreign_root[0] || self_foreign_observe) {
            const char *base = getenv("BONFYRE_FOREIGN_ROOT");
            char private_base[PATH_MAX], scope_root[PATH_MAX], estate_root[PATH_MAX];
            if (!base || !*base) base = "/var/lib/bonfyre/foreign";
            if (snprintf(private_base, sizeof(private_base), "%s/private", base) >= (int)sizeof(private_base) ||
                snprintf(scope_root, sizeof(scope_root), "%s/%s", private_base,
                         self_foreign_observe ? "self" : private_estate_for_host(host)) >= (int)sizeof(scope_root) ||
                snprintf(estate_root, sizeof(estate_root), "%s/%s", scope_root,
                         self_foreign_observe ? strrchr(self_root, '/') + 1 : private_cms_namespace + strlen("private:") + strlen(private_estate_for_host(host)) + 1) >= (int)sizeof(estate_root)) {
                studio_reply(503, "application/json", "{\"error\":\"private_foreign_path_overflow\"}\n"); return 1;
            }
            (void)mkdir(base, 0750); (void)mkdir(private_base, 0750); (void)mkdir(scope_root, 0750); (void)mkdir(estate_root, 0750);
        } else {
            (void)mkdir(foreign_root, 0750);
        }
        (void)mkdir(foreign_root, 0750);
        if (snprintf(receipt, sizeof(receipt), "%s/receipts", foreign_root) >= (int)sizeof(receipt) ||
            (mkdir(receipt, 0750) != 0 && errno != EEXIST) ||
            snprintf(retained, sizeof(retained), "%s/bodies", foreign_root) >= (int)sizeof(retained) ||
            (mkdir(retained, 0750) != 0 && errno != EEXIST) ||
            snprintf(receipt, sizeof(receipt), "%s/receipts/%s-%s.json", foreign_root, twin, stamp) >= (int)sizeof(receipt) ||
            snprintf(retained, sizeof(retained), "%s/bodies/%s-%s.body", foreign_root, twin, stamp) >= (int)sizeof(retained)) { studio_reply(503, "application/json", "{\"error\":\"receipt_path_overflow\"}\n"); return 1; }
        snprintf(bin, sizeof(bin), "%s/bonfyre-project", bindir);
        char *foreign_argv[] = { bin, "foreign", "observe", twin, url, "--receipt", receipt, "--body", retained, "--observe-right", "public", NULL };
        int rc = access(bin, X_OK) == 0 ? auth_call(foreign_argv, NULL, output, sizeof(output)) : 127;
        FILE *f = fopen(receipt, "rb"); size_t got = 0;
        if (f) { got = fread(output, 1, sizeof(output) - 1, f); output[got] = '\0'; fclose(f); }
        if (rc != 0 && !got) { studio_reply(503, "application/json", "{\"error\":\"foreign_observer_unavailable\"}\n"); return 1; }
        if (self_foreign_observe && studio_html_mode) {
            char e_twin[256], fragment[2048]; int n;
            html_escape(twin, e_twin, sizeof(e_twin));
            n = snprintf(fragment, sizeof(fragment),
                "<section class=\"studio-result is-ok\" aria-live=\"polite\"><span class=\"result-kicker\">Private ForeignTwin retained</span>"
                "<h3>%s</h3><p>The observed body and receipt are retained only below your actor-derived private root. "
                "This did not publish the source, promote it to truth, grant a relationship, or create authority to act.</p></section>", e_twin);
            if (n < 0 || n >= (int)sizeof(fragment)) { studio_reply(503, "application/json", "{\"error\":\"self_foreign_result_overflow\"}\n"); return 1; }
            printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s", fragment);
            return 1;
        }
        studio_reply(200, "application/json; charset=utf-8", output); return 1;
    }
    /* Fleming: the harvested DFW estate, readable and addable by a signed-in
     * person. Every number below is computed from receipted observations at
     * request time -- none of it is a stored summary. */
    if (!strcmp(target, "/studio/v1/fleming/watch")) {
        char account[33], context[96], actor[160], subject[160], continuation[128], title[161];
        const char *scope = NULL;
        /* A write verb reached by GET should say so, rather than complain about
         * a body the caller was never asked to send. */
        if (strcmp(method, "POST")) {
            studio_reply(405, "application/json",
                "{\"error\":\"POST required\",\"read_path\":\"/studio/v1/fleming/watchlist\"}\n");
            return 1;
        }
        char qbin[PATH_MAX], workdb[PATH_MAX];
        if (!aaw_fleming_input(body, account, context)) {
            studio_reply(503, "application/json",
                "{\"error\":\"a tax account number (alphanumeric, max 32) is required\"}\n");
            return 1;
        }
        if (psetup_context(bearer, auth_db, context, 2, actor, sizeof(actor), subject, sizeof(subject), &scope) != 1) {
            studio_reply(403, "application/json", "{\"error\":\"account_work_access_required\"}\n"); return 1;
        }
        snprintf(continuation, sizeof(continuation), "/apps?context=%s", context);
        snprintf(title, sizeof(title), "Watch parcel %s", account);
        if (snprintf(workdb, sizeof(workdb), "%s/fleming-work.db", fleming_root()) >= (int)sizeof(workdb)) {
            studio_reply(503, "application/json", "{\"error\":\"work_path_overflow\"}\n"); return 1;
        }
        snprintf(qbin, sizeof(qbin), "%s/bonfyre-queue", bindir);
        if (access(qbin, X_OK) != 0 || access(workdb, R_OK) != 0) {
            studio_reply(503, "application/json", "{\"error\":\"native_workgraph_unavailable\"}\n"); return 1;
        }
        {
            /* A real durable mission in the WorkGraph, not a note in a side
             * table: it is claimable, leasable, and receipted like any other. */
            char *wargv[] = { qbin, "enqueue", "fleming-watch", account, "--db", workdb,
                "--subject", subject, "--scope", (char *)scope, "--title", title, "--continuation", continuation, NULL };
            if (auth_call(wargv, NULL, output, sizeof(output)) != 0 || !output[0]) {
                studio_reply(503, "application/json", "{\"error\":\"enqueue_failed\"}\n"); return 1;
            }
        }
        studio_reply(200, "application/json; charset=utf-8", output);
        return 1;
    }
    if (!strcmp(target, "/studio/v1/fleming/estate")) {
        if (fleming_query(bindir,
                "SELECT (SELECT count(*) FROM observation) AS observations, "
                "(SELECT count(DISTINCT src_id) FROM observation) AS sources, "
                "(SELECT count(*) FROM page) AS pages, "
                "(SELECT count(*) FROM parcel_flat) AS parcels_projected",
                output, sizeof(output)) != 0 || !output[0]) {
            studio_reply(503, "application/json", "{\"error\":\"fleming_estate_unavailable\"}\n"); return 1;
        }
        studio_reply(200, ctype, output); return 1;
    }
    if (!strcmp(target, "/studio/v1/fleming/coverage")) {
        if (fleming_query(bindir,
                "SELECT src_id, count(*) AS rows, count(DISTINCT page_sha256) AS pages "
                "FROM observation GROUP BY src_id ORDER BY rows DESC LIMIT 40",
                output, sizeof(output)) != 0 || !output[0]) {
            studio_reply(503, "application/json", "{\"error\":\"fleming_estate_unavailable\"}\n"); return 1;
        }
        studio_reply(200, ctype, output); return 1;
    }
    if (!strcmp(target, "/studio/v1/fleming/markets")) {
        /* Per-city land and exit anchors from the whole Tarrant roll. A median
         * is a market anchor, never a comp or a price for any single parcel --
         * the projection carries that limit with it. */
        if (fleming_query(bindir,
                "WITH p AS (SELECT trim(json_extract_string(observed,'$.City')) AS city, "
                " TRY_CAST(trim(json_extract_string(observed,'$.Land_Value')) AS DOUBLE) AS lv, "
                " TRY_CAST(trim(json_extract_string(observed,'$.Improvemen')) AS DOUBLE) AS iv, "
                " TRY_CAST(trim(json_extract_string(observed,'$.Total_Valu')) AS DOUBLE) AS tv, "
                " TRY_CAST(trim(json_extract_string(observed,'$.Land_SqFt')) AS DOUBLE) AS sqft "
                " FROM observation WHERE src_id='src:tad-parcel-view') "
                "SELECT city, "
                " count(*) FILTER (WHERE iv=0 AND lv>0 AND sqft BETWEEN 3000 AND 20000) AS vacant_n, "
                " median(lv) FILTER (WHERE iv=0 AND lv>0 AND sqft BETWEEN 3000 AND 20000) AS land_median, "
                " count(*) FILTER (WHERE tv>0) AS improved_n, "
                " median(tv) FILTER (WHERE tv>0) AS exit_median "
                "FROM p GROUP BY city HAVING vacant_n >= 40 AND improved_n >= 500 "
                "ORDER BY improved_n DESC LIMIT 20",
                output, sizeof(output)) != 0 || !output[0]) {
            studio_reply(503, "application/json", "{\"error\":\"fleming_estate_unavailable\"}\n"); return 1;
        }
        studio_reply(200, ctype, output); return 1;
    }
    if (!strcmp(target, "/studio/v1/fleming/owners")) {
        /* A taxpayer name is not the legal owner, a sole owner, or capacity to
         * convey. This counts parcels per name on the roll and nothing more. */
        if (fleming_query(bindir,
                "SELECT trim(json_extract_string(observed,'$.Owner_Name')) AS owner, "
                "count(*) AS parcels FROM observation WHERE src_id='src:tad-parcel-view' "
                "AND trim(json_extract_string(observed,'$.Owner_Name')) <> '' "
                "GROUP BY 1 HAVING parcels >= 60 ORDER BY parcels DESC LIMIT 25",
                output, sizeof(output)) != 0 || !output[0]) {
            studio_reply(503, "application/json", "{\"error\":\"fleming_estate_unavailable\"}\n"); return 1;
        }
        studio_reply(200, ctype, output); return 1;
    }
    if (!strcmp(target, "/studio/v1/fleming/watchlist")) {
        char qbin[PATH_MAX], workdb[PATH_MAX];
        static char wsql[4096];
        snprintf(qbin, sizeof(qbin), "%s/bonfyre-query", bindir);
        if (snprintf(workdb, sizeof(workdb), "%s/fleming-work.db", fleming_root()) >= (int)sizeof(workdb) ||
            access(qbin, X_OK) != 0 || access(workdb, R_OK) != 0) {
            studio_reply(503, "application/json", "{\"error\":\"native_workgraph_unavailable\"}\n"); return 1;
        }
        /* A watch is a WorkGraph node, not a row in a bespoke table: the queue
         * operator carries the kind and the node's input_uri carries the tax
         * account. Reading it back through the graph is what keeps the watch a
         * claimable unit of work rather than a decorative record. */
        if (snprintf(wsql, sizeof(wsql),
                "SET home_directory='%s'; INSTALL sqlite; LOAD sqlite; "
                "ATTACH '%s' AS w (TYPE sqlite, READ_ONLY); "
                "SELECT n.mission_id, n.input_uri AS account, n.status, m.created_at "
                "FROM w.workgraph_nodes n JOIN w.missions m ON m.id = n.mission_id "
                "WHERE n.operator_id = 'queue.fleming-watch' AND n.mission_id NOT LIKE 'account-%%' "
                "ORDER BY m.created_at DESC LIMIT 50",
                duckdb_home(), workdb) >= (int)sizeof(wsql)) {
            studio_reply(503, "application/json", "{\"error\":\"watchlist_query_overflow\"}\n"); return 1;
        }
        {
            char *largv[] = { qbin, "sql", ":memory:", wsql, NULL };
            char diag[2048];
            if (auth_call_split(largv, NULL, output, sizeof(output), diag, sizeof(diag)) != 0 || !output[0]) {
                fprintf(stderr, "fleming watchlist: %s\n", diag);
                studio_reply(503, "application/json", "{\"error\":\"watchlist_unavailable\"}\n"); return 1;
            }
        }
        studio_reply(200, ctype, output); return 1;
    }
    /* ── people ──────────────────────────────────────────────────────────
     * A profile has two renderings. Which one you get is decided by the
     * verified session, never by anything in the request: viewing yourself
     * returns the self view, viewing anyone else returns the public view. */
    if (!strcmp(target, "/studio/v1/profile") || !strncmp(target, "/studio/v1/profile/", 19)) {
        StudioViewer who;
        char subject[192] = {0};   /* must hold a full StudioViewer.email */
        int self_view;
        if (!session_identity(bearer, auth_db, &who)) {
            studio_reply(401, "application/json", "{\"error\":\"session_required\"}\n"); return 1;
        }
        if (!strcmp(target, "/studio/v1/profile")) {
            self_view = 1;
            snprintf(subject, sizeof(subject), "%s", who.email);
        } else {
            if (!studio_path_tail(target, "/studio/v1/profile/", subject, sizeof(subject)) ||
                !studio_safe_slug_arg(subject)) {
                studio_reply(503, "application/json", "{\"error\":\"invalid_subject\"}\n"); return 1;
            }
            /* Case-insensitive on the address, because an email is not
             * case-sensitive in its domain and people type it either way. */
            self_view = (strcasecmp(subject, who.email) == 0) || (strcasecmp(subject, who.name) == 0);
        }
        if (self_view) {
            /* The self view carries the account facts the person owns. It never
             * includes a session token or gate key: possessing a profile page
             * must not be a way to obtain a credential. */
            int may_write = session_authorized(bearer, auth_db, "write");
            int may_self_author = session_self_authoring(bearer, auth_db);
            if (studio_html_mode) {
                char e_name[384], e_email[576], fragment[8192];
                int n;
                html_escape(who.name, e_name, sizeof(e_name));
                html_escape(who.email, e_email, sizeof(e_email));
                n = snprintf(fragment, sizeof(fragment),
                    "<section class=\"studio-result is-ok\" aria-live=\"polite\">"
                    "<span class=\"result-kicker\">Your closure</span>"
                    "<h3>%s</h3><p>Your account is a private starting point, not a public profile or an institution. "
                    "Its own Life presence and carrier recipes are separate, explicit actions.</p>"
                    "<dl class=\"result-facts\"><div><dt>Account</dt><dd>%s</dd></div>"
                    "<div><dt>Self presence</dt><dd>%s</dd></div><div><dt>Generic write</dt><dd>%s</dd></div></dl>"
                    "%s"
                    "<p class=\"hint\">A carrier recipe is a governed handoff plan only. It does not make a QR code, authenticate someone, publish private material, or trigger a payment/device effect.</p>"
                    "</section>",
                    e_name, e_email, may_self_author ? "available" : "unavailable",
                    may_write ? "available" : "not granted",
                    may_self_author ?
                    "<form class=\"actions\" hx-post=\"/studio/v1/matrix/self-publish\" hx-target=\"#self-presence-result\" hx-swap=\"innerHTML\">"
                    "<label>Life presence <input name=\"title\" placeholder=\"How you appear\" required></label>"
                    "<label>Address <input name=\"slug\" pattern=\"[a-z0-9-]+\" placeholder=\"my-friday\" required></label>"
                    "<button class=\"button primary\" type=\"submit\">Create my Life presence <span>&rarr;</span></button></form>"
                    "<div id=\"self-presence-result\"></div>"
                    "<form class=\"actions\" hx-post=\"/studio/v1/self/foreign/observe\" hx-target=\"#self-foreign-result\" hx-swap=\"innerHTML\">"
                    "<label>Private source label <input name=\"twin_id\" pattern=\"[A-Za-z0-9:_-]+\" placeholder=\"my-source\" required></label>"
                    "<label>Public source URL <input name=\"url\" type=\"url\" placeholder=\"https://example.org/source\" required></label>"
                    "<button class=\"button quiet\" type=\"submit\">Observe privately <span>&rarr;</span></button></form>"
                    "<div id=\"self-foreign-result\"></div>"
                    "<section id=\"self-foreign-workspace\" hx-get=\"/studio/v1/self/foreign\" hx-trigger=\"load\" hx-swap=\"innerHTML\"><p class=\"hint\">Loading your private source desk…</p></section>"
                    "<p><a href=\"/cms/\">Open my carrier recipes</a></p>" :
                    "<p class=\"hint\">This session has no self-authoring scope. Sign in again or contact the institution that issued it.</p>");
                if (n < 0 || n >= (int)sizeof(fragment)) {
                    studio_reply(503, "application/json", "{\"error\":\"profile_overflow\"}\n"); return 1;
                }
                printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s", fragment);
                return 1;
            }
            int n = snprintf(output, sizeof(output),
                "{\"view\":\"self\",\"subject\":\"%s\",\"id\":%ld,\"email\":\"%s\","
                "\"name\":\"%s\",\"tier\":\"%s\","
                "\"can_write\":%s,\"can_self_author\":%s,"
                "\"surfaces\":[\"/studio/v1/fleming/watch\",\"/studio/v1/clients\",\"/studio/v1/membrane\"],"
                "\"self_foreign_twin\":\"/studio/v1/self/foreign/observe\","
                "\"self_foreign_workspace\":\"/studio/v1/self/foreign\"}\n",
                subject, who.id, who.email, who.name, who.tier,
                may_write ? "true" : "false", may_self_author ? "true" : "false");
            if (n < 0 || (size_t)n >= sizeof(output)) { studio_reply(503, "application/json", "{\"error\":\"profile_overflow\"}\n"); return 1; }
            studio_reply(200, ctype, output); return 1;
        }
        {
            /* The public view answers only what the graph already publishes
             * about a subject. No email, no tier, no account state -- being
             * signed in lets you look someone up, not read their account. */
            char sql[1024];
            snprintf(sql, sizeof(sql),
                "SELECT display, kind, obs_count, first_seen, last_seen FROM e.entities "
                "WHERE lower(display) = lower('%s') LIMIT 1", subject);
            if (entity_query(bindir, sql, output, sizeof(output)) != 0 || !output[0]) {
                studio_reply(503, "application/json", "{\"error\":\"entity_graph_unavailable\"}\n"); return 1;
            }
            {
                char wrapped[8192];
                int n = snprintf(wrapped, sizeof(wrapped),
                    "{\"view\":\"public\",\"subject\":\"%s\",\"can_write\":false,\"record\":%s}\n",
                    subject, output[0] == '[' ? output : "[]");
                if (n < 0 || (size_t)n >= (int)sizeof(wrapped)) { studio_reply(503, "application/json", "{\"error\":\"profile_overflow\"}\n"); return 1; }
                studio_reply(200, ctype, wrapped); return 1;
            }
        }
    }

    /* ── clients ─────────────────────────────────────────────────────────
     * One registry for every engagement, old and new. A client is an entity in
     * the identity graph, so registering one is a real resolve rather than a
     * row in a list that only this surface understands. */
    if (!strcmp(target, "/studio/v1/clients")) {
        if (!strcmp(method, "POST")) {
            char slug[97], ebin[PATH_MAX], value[160];
            if (!studio_json_string(body, "slug", slug, sizeof(slug)) || !studio_safe_slug_arg(slug)) {
                studio_reply(503, "application/json",
                    "{\"error\":\"a client slug is required (alphanumeric, dash, dot, max 96)\"}\n");
                return 1;
            }
            snprintf(ebin, sizeof(ebin), "%s/bonfyre-entity", bindir);
            snprintf(value, sizeof(value), "client:%s", slug);
            if (access(ebin, X_OK) != 0) { studio_reply(503, "application/json", "{\"error\":\"native_entity_unavailable\"}\n"); return 1; }
            {
                /* A client engagement is an organisation, not a person. */
                char *eargv[] = { ebin, "resolve", "text", value, "--kind", "org", NULL };
                char diag[1024];
                if (auth_call_split(eargv, NULL, output, sizeof(output), diag, sizeof(diag)) != 0) {
                    fprintf(stderr, "client register: %s\n", diag);
                    studio_reply(503, "application/json", "{\"error\":\"register_failed\"}\n"); return 1;
                }
            }
            studio_reply(200, "text/plain; charset=utf-8", output); return 1;
        }
        if (entity_query(bindir,
                "SELECT replace(display,'client:','') AS slug, display, obs_count, first_seen "
                "FROM e.entities WHERE display LIKE 'client:%' ORDER BY display",
                output, sizeof(output)) != 0 || !output[0]) {
            studio_reply(503, "application/json", "{\"error\":\"entity_graph_unavailable\"}\n"); return 1;
        }
        studio_reply(200, ctype, output); return 1;
    }
    if (!strncmp(target, "/studio/v1/client/", 18)) {
        char slug[128], sql[1024];
        if (!studio_path_tail(target, "/studio/v1/client/", slug, sizeof(slug)) ||
            !studio_safe_slug_arg(slug)) {
            studio_reply(503, "application/json", "{\"error\":\"invalid_client\"}\n"); return 1;
        }
        snprintf(sql, sizeof(sql),
            "SELECT display, kind, obs_count, first_seen, last_seen FROM e.entities "
            "WHERE display = 'client:%s' LIMIT 1", slug);
        if (entity_query(bindir, sql, output, sizeof(output)) != 0 || !output[0]) {
            studio_reply(503, "application/json", "{\"error\":\"entity_graph_unavailable\"}\n"); return 1;
        }
        studio_reply(200, ctype, output); return 1;
    }

    /* ── financial membrane ──────────────────────────────────────────────
     * The membrane is a composition, not one organ: BonfyreEconomy holds spend
     * against caps, BonfyreFinance holds the service and job book, BonfyreLedger
     * holds what was actually written down. Reporting them together is the only
     * way the number means anything; reporting one alone reads as the whole. */
    if (!strcmp(target, "/studio/v1/membrane")) {
        /* Read the three books directly rather than scraping the organs' human
         * tables: a rendered table is for a person, and parsing one back into
         * numbers is how a report starts quietly disagreeing with its source. */
        const char *econ_db = getenv("BONFYRE_ECONOMY_DB");
        const char *fin_db  = getenv("BONFYRE_FINANCE_DB");
        char qbin[PATH_MAX];
        static char sql[4096];
        char diag[1024];
        if (!econ_db || !*econ_db) econ_db = "/var/lib/bonfyre/state/economy.db";
        if (!fin_db  || !*fin_db)  fin_db  = "/var/lib/bonfyre/state/finance.db";
        snprintf(qbin, sizeof(qbin), "%s/bonfyre-query", bindir);
        if (access(qbin, X_OK) != 0 || access(econ_db, R_OK) != 0 || access(fin_db, R_OK) != 0) {
            studio_reply(503, "application/json", "{\"error\":\"membrane_organs_unavailable\"}\n"); return 1;
        }
        if (snprintf(sql, sizeof(sql),
                "SET home_directory='%s'; INSTALL sqlite; LOAD sqlite; "
                "ATTACH '%s' AS ec (TYPE sqlite, READ_ONLY); "
                "ATTACH '%s' AS fi (TYPE sqlite, READ_ONLY); "
                "SELECT 'bonfyre-financial-membrane.v1' AS membrane, "
                " (SELECT count(*) FROM ec.budgets) AS budgets, "
                " (SELECT coalesce(sum(max_usd),0) FROM ec.budgets) AS usd_capped, "
                " (SELECT coalesce(sum(spent),0) FROM ec.budgets) AS usd_spent_against_caps, "
                " (SELECT count(*) FROM ec.costs) AS cost_records, "
                " (SELECT coalesce(sum(usd),0) FROM ec.costs) AS usd_recorded, "
                " (SELECT count(*) FROM fi.services) AS services, "
                " (SELECT coalesce(sum(sell_price-buy_price),0) FROM fi.services) AS book_margin_usd, "
                " (SELECT count(*) FROM fi.service_jobs) AS service_jobs, "
                " (SELECT count(*) FROM fi.bundles) AS bundles, "
                " 'a budget cap is not committed capital; a recorded cost is not an invoice' "
                "   AS forbidden_inference",
                duckdb_home(), econ_db, fin_db) >= (int)sizeof(sql)) {
            studio_reply(503, "application/json", "{\"error\":\"membrane_query_overflow\"}\n"); return 1;
        }
        {
            char *a[] = { qbin, "sql", ":memory:", sql, NULL };
            if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0 || !output[0]) {
                fprintf(stderr, "membrane: %s\n", diag);
                studio_reply(503, "application/json", "{\"error\":\"membrane_unavailable\"}\n"); return 1;
            }
        }
        studio_reply(200, ctype, output); return 1;
    }

    /* ── WORK: the mission lifecycle, not just enqueue ───────────────────
     * A queue you can only add to is a list. Claim, complete and fail are what
     * make it a work graph, so they are typed verbs here rather than shell. */
    if (!strcmp(target, "/studio/v1/work/claim")) {
        char worker[97], workdb[PATH_MAX], qbin[PATH_MAX], diag[1024];
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        if (!studio_json_string(body, "worker", worker, sizeof(worker)) || !studio_safe_ident(worker)) {
            studio_reply(503, "application/json", "{\"error\":\"a worker id is required\"}\n"); return 1;
        }
        snprintf(qbin, sizeof(qbin), "%s/bonfyre-queue", bindir);
        if (snprintf(workdb, sizeof(workdb), "%s/fleming-work.db", fleming_root()) >= (int)sizeof(workdb) ||
            access(qbin, X_OK) != 0 || access(workdb, R_OK) != 0) {
            studio_reply(503, "application/json", "{\"error\":\"native_workgraph_unavailable\"}\n"); return 1;
        }
        {
            char *a[] = { qbin, "claim-next", worker, "--db", workdb, NULL };
            if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0 || !output[0]) {
                fprintf(stderr, "work claim: %s\n", diag);
                studio_reply(503, "application/json", "{\"error\":\"claim_failed\"}\n"); return 1;
            }
        }
        studio_reply(200, ctype, output); return 1;
    }
    if (!strcmp(target, "/studio/v1/work/complete")) {
        char mission[97], node[97], worker[97], token[97];
        char workdb[PATH_MAX], qbin[PATH_MAX], diag[1024];
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        if (!studio_json_string(body, "mission", mission, sizeof(mission)) || !studio_safe_ident(mission) ||
            !studio_json_string(body, "node", node, sizeof(node)) || !studio_safe_ident(node) ||
            !studio_json_string(body, "worker", worker, sizeof(worker)) || !studio_safe_ident(worker) ||
            !studio_json_string(body, "token", token, sizeof(token)) || !studio_safe_hex(token, 64)) {
            studio_reply(503, "application/json",
                "{\"error\":\"mission, node, worker and the 64-hex claim token are required\"}\n"); return 1;
        }
        snprintf(qbin, sizeof(qbin), "%s/bonfyre-queue", bindir);
        if (snprintf(workdb, sizeof(workdb), "%s/fleming-work.db", fleming_root()) >= (int)sizeof(workdb) ||
            access(qbin, X_OK) != 0 || access(workdb, R_OK) != 0) {
            studio_reply(503, "application/json", "{\"error\":\"native_workgraph_unavailable\"}\n"); return 1;
        }
        {
            /* The claim token is the authority to complete this node: the queue
             * rejects an ack from anyone who does not hold the current lease. */
            char *a[] = { qbin, "ack", mission, node, worker, token, "--db", workdb, NULL };
            if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0 || !output[0]) {
                fprintf(stderr, "work complete: %s\n", diag);
                studio_reply(503, "application/json", "{\"error\":\"complete_failed\"}\n"); return 1;
            }
        }
        studio_reply(200, ctype, output); return 1;
    }

    /* ── ECONOMY: record what a run actually cost ────────────────────────── */
    if (!strcmp(target, "/studio/v1/economy/cost")) {
        char recipe[121], stage[97], model[97], usd_raw[32], usd[32];
        char ebin[PATH_MAX], diag[1024];
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        if (!studio_json_string(body, "recipe", recipe, sizeof(recipe)) || !studio_safe_recipe(recipe) ||
            !studio_json_string(body, "stage", stage, sizeof(stage)) || !studio_safe_ident(stage) ||
            !studio_json_string(body, "model", model, sizeof(model)) || !studio_safe_ident(model) ||
            !studio_json_string(body, "usd", usd_raw, sizeof(usd_raw)) || !studio_safe_usd_amount(usd_raw, usd)) {
            studio_reply(503, "application/json",
                "{\"error\":\"recipe, stage, model and a non-negative usd amount are required\"}\n"); return 1;
        }
        snprintf(ebin, sizeof(ebin), "%s/bonfyre-economy", bindir);
        if (access(ebin, X_OK) != 0) { studio_reply(503, "application/json", "{\"error\":\"native_economy_unavailable\"}\n"); return 1; }
        {
            char *a[] = { ebin, "cost", "record", recipe, stage, model, usd, NULL };
            if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0 || !output[0]) {
                fprintf(stderr, "cost record: %s\n", diag);
                studio_reply(503, "application/json", "{\"error\":\"cost_record_failed\"}\n"); return 1;
            }
        }
        studio_reply(200, "text/plain; charset=utf-8", output); return 1;
    }

    /* ── FLEMING: an opportunity is a mission, not a note ─────────────────── */
    if (!strcmp(target, "/studio/v1/fleming/opportunity")) {
        char account[33], workdb[PATH_MAX], qbin[PATH_MAX], diag[1024];
        char context[96], actor[160], subject[160], continuation[128], title[161];
        const char *scope = NULL;
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\",\"read_path\":\"/studio/v1/fleming/watchlist\"}\n"); return 1; }
        if (!aaw_fleming_input(body, account, context)) {
            studio_reply(503, "application/json", "{\"error\":\"a tax account number is required\"}\n"); return 1;
        }
        if (psetup_context(bearer, auth_db, context, 2, actor, sizeof(actor), subject, sizeof(subject), &scope) != 1) {
            studio_reply(403, "application/json", "{\"error\":\"account_work_access_required\"}\n"); return 1;
        }
        snprintf(continuation, sizeof(continuation), "/apps?context=%s", context);
        snprintf(title, sizeof(title), "Parcel opportunity %s", account);
        snprintf(qbin, sizeof(qbin), "%s/bonfyre-queue", bindir);
        if (snprintf(workdb, sizeof(workdb), "%s/fleming-work.db", fleming_root()) >= (int)sizeof(workdb) ||
            access(qbin, X_OK) != 0 || access(workdb, R_OK) != 0) {
            studio_reply(503, "application/json", "{\"error\":\"native_workgraph_unavailable\"}\n"); return 1;
        }
        {
            char *a[] = { qbin, "enqueue", "fleming-opportunity", account, "--db", workdb,
                "--subject", subject, "--scope", (char *)scope, "--title", title, "--continuation", continuation, NULL };
            if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0 || !output[0]) {
                fprintf(stderr, "opportunity: %s\n", diag);
                studio_reply(503, "application/json", "{\"error\":\"enqueue_failed\"}\n"); return 1;
            }
        }
        studio_reply(200, ctype, output); return 1;
    }
    /* ── MATRIX: typed binding mutations ─────────────────────────────────
     * Each one compiles through the organ's own Matrix verbs, so the binding
     * rules live in one place and a surface cannot invent a looser one. */
    if (!strcmp(target, "/studio/v1/matrix/standing")) {
        char address[160], kind[64], territory[64], scope[32], world[32], evidence[192], subject[256];
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        if (!studio_json_string(body, "address", address, sizeof(address)) || !studio_safe_label(address, 159) ||
            !studio_json_string(body, "subject_kind", kind, sizeof(kind)) || !studio_safe_ident(kind)) {
            studio_reply(503, "application/json", "{\"error\":\"address and subject_kind are required\"}\n"); return 1;
        }
        if (bf_matrix_address_subject(matrix, address, subject, sizeof(subject)) != 1 ||
            !studio_owns_subject(subject, bearer, auth_db)) {
            studio_reply(403, "application/json", "{\"error\":\"local_subject_owner_required\"}\n"); return 1;
        }
        /* Territory is a closed set in the Matrix core (life|live|shop|work|
         * independent), not a free label -- "global" or a metro name is
         * rejected there, so defaulting to one would only move the failure. */
        if (!studio_json_string(body, "territory", territory, sizeof(territory)) || !studio_safe_ident(territory))
            snprintf(territory, sizeof(territory), "%s", "life");
        if (!studio_json_string(body, "scope", scope, sizeof(scope)) || !studio_safe_ident(scope))
            snprintf(scope, sizeof(scope), "%s", "public");
        if (!studio_json_string(body, "world", world, sizeof(world)) || !studio_safe_ident(world))
            snprintf(world, sizeof(world), "%s", "live");
        if (!studio_json_string(body, "evidence", evidence, sizeof(evidence)) || !studio_safe_label(evidence, 191))
            snprintf(evidence, sizeof(evidence), "%s", "studio-write-path");
        {
            char *a[] = { (char*)"standing", (char*)"--address", address, (char*)"--subject-kind", kind,
                          (char*)"--territory", territory, (char*)"--scope", scope,
                          (char*)"--world", world, (char*)"--repr", (char*)"HTML",
                          (char*)"--evidence", evidence };
            if (matrix_cli(bindir, a, 15, output, sizeof(output)) != 0) {
                /* The core refuses a standing address that does not stand on an
                 * already-bound address, and rejects any territory outside its
                 * closed set. Name both, rather than return a bare failure the
                 * caller cannot act on. */
                studio_reply(503, "application/json",
                    "{\"error\":\"standing_bind_refused\",\"preconditions\":["
                    "\"address must already be bound in the Matrix (bind it before standing on it)\","
                    "\"territory must be one of life|live|shop|work|independent\","
                    "\"scope must be one of self|household|group|community|public|market|institution\","
                    "\"world must be one of live|shadow|replay|candidate\"]}\n");
                return 1;
            }
        }
        {
            char ok[1024];
            snprintf(ok, sizeof(ok),
                "{\"bound\":\"standing_address\",\"address\":\"%s\",\"subject_kind\":\"%s\","
                "\"territory\":\"%s\",\"scope\":\"%s\",\"world\":\"%s\"}\n",
                address, kind, territory, scope, world);
            studio_reply(200, ctype, ok); return 1;
        }
    }
    if (!strcmp(target, "/studio/v1/matrix/presence")) {
        /* Bind a new presence for an existing subject. */
        char id[128], subject[160], territory[64], scope[32], world[32], repr[32], evidence[192];
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        /* Presence ids and subject ids are colon-delimited in the Matrix
         * (presence:institution:aurekai:life:root), which studio_safe_ident
         * rejects; the label class is the one that admits them. */
        if (!studio_json_string(body, "id", id, sizeof(id)) || !studio_safe_label(id, 127) ||
            !studio_json_string(body, "subject", subject, sizeof(subject)) || !studio_safe_label(subject, 159)) {
            studio_reply(503, "application/json", "{\"error\":\"presence id and subject are required\"}\n"); return 1;
        }
        if (!studio_owns_subject(subject, bearer, auth_db)) {
            studio_reply(403, "application/json", "{\"error\":\"local_subject_owner_required\"}\n"); return 1;
        }
        if (!studio_json_string(body, "territory", territory, sizeof(territory)) || !studio_safe_ident(territory))
            snprintf(territory, sizeof(territory), "%s", "life");
        if (!studio_json_string(body, "scope", scope, sizeof(scope)) || !studio_safe_ident(scope))
            snprintf(scope, sizeof(scope), "%s", "public");
        if (!studio_json_string(body, "world", world, sizeof(world)) || !studio_safe_ident(world))
            snprintf(world, sizeof(world), "%s", "live");
        if (!studio_json_string(body, "repr", repr, sizeof(repr)) || !studio_safe_ident(repr))
            snprintf(repr, sizeof(repr), "%s", "HTML");
        if (!studio_json_string(body, "evidence", evidence, sizeof(evidence)) || !studio_safe_label(evidence, 191))
            snprintf(evidence, sizeof(evidence), "%s", "studio-write-path");
        {
            char *a[] = { (char*)"presence", (char*)"--id", id, (char*)"--subject", subject,
                          (char*)"--territory", territory, (char*)"--scope", scope,
                          (char*)"--world", world, (char*)"--repr", repr,
                          (char*)"--evidence", evidence };
            if (matrix_cli(bindir, a, 15, output, sizeof(output)) != 0) {
                /* This binds a NEW presence. insert_presence() refuses an id it
                  * already holds with DUPLICATE_PRESENCE -- the core has no
                  * re-bind path, so "move" and "change audience" are not this
                  * verb and are not silently emulated by deleting and
                  * reinserting, which would discard the original evidence. */
                studio_reply(503, "application/json",
                    "{\"error\":\"presence_bind_refused\",\"preconditions\":["
                    "\"id must not already be bound (this verb creates a presence; the core has no re-bind)\","
                    "\"subject must already exist in the Matrix subjects table\","
                    "\"territory must be one of life|live|shop|work|independent\","
                    "\"scope must be one of self|household|group|community|public|market|institution\"]}\n");
                return 1;
            }
        }
        {
            char ok[1024];
            snprintf(ok, sizeof(ok),
                "{\"bound\":\"presence\",\"created\":true,\"id\":\"%s\",\"subject\":\"%s\",\"territory\":\"%s\","
                "\"scope\":\"%s\",\"world\":\"%s\",\"repr\":\"%s\"}\n",
                id, subject, territory, scope, world, repr);
            studio_reply(200, ctype, ok); return 1;
        }
    }
    if (!strcmp(target, "/studio/v1/matrix/alias")) {
        char alias[160], canonical[160], evidence[192], subject[256];
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        if (!studio_json_string(body, "alias", alias, sizeof(alias)) || !studio_safe_label(alias, 159) ||
            !studio_json_string(body, "canonical", canonical, sizeof(canonical)) || !studio_safe_label(canonical, 159)) {
            studio_reply(503, "application/json", "{\"error\":\"alias and canonical are required\"}\n"); return 1;
        }
        if (bf_matrix_address_subject(matrix, canonical, subject, sizeof(subject)) != 1 ||
            !studio_owns_subject(subject, bearer, auth_db)) {
            studio_reply(403, "application/json", "{\"error\":\"local_subject_owner_required\"}\n"); return 1;
        }
        if (!studio_json_string(body, "evidence", evidence, sizeof(evidence)) || !studio_safe_label(evidence, 191))
            snprintf(evidence, sizeof(evidence), "%s", "studio-write-path");
        {
            /* The organ enforces that an alias may redirect only to an address
             * of the same semantic subject; this surface does not relax it. */
            char *a[] = { (char*)"alias", (char*)"--alias", alias,
                          (char*)"--canonical", canonical, (char*)"--evidence", evidence };
            if (matrix_cli(bindir, a, 7, output, sizeof(output)) != 0) {
                /* The core refuses an alias whose two addresses do not resolve
                  * to the same semantic subject. That is the invariant that
                  * stops an alias from quietly retargeting one subject at
                  * another, so the surface reports it rather than softening it. */
                studio_reply(503, "application/json",
                    "{\"error\":\"alias_bind_refused\",\"preconditions\":["
                    "\"both alias and canonical must already be bound addresses\","
                    "\"both must resolve to the same semantic subject\","
                    "\"alias and canonical must differ\"]}\n");
                return 1;
            }
        }
        {
            char ok[1024];
            snprintf(ok, sizeof(ok), "{\"bound\":\"alias\",\"alias\":\"%s\",\"canonical\":\"%s\"}\n", alias, canonical);
            studio_reply(200, ctype, ok); return 1;
        }
    }

    /* ── IDENTITY: groups and relationships ──────────────────────────────── */
    if (!strcmp(target, "/studio/v1/identity/group")) {
        char slug[97], ebin[PATH_MAX], value[160], diag[1024];
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        if (!studio_json_string(body, "slug", slug, sizeof(slug)) || !studio_safe_slug_arg(slug)) {
            studio_reply(503, "application/json", "{\"error\":\"a group slug is required\"}\n"); return 1;
        }
        snprintf(ebin, sizeof(ebin), "%s/bonfyre-entity", bindir);
        snprintf(value, sizeof(value), "group:%s", slug);
        if (access(ebin, X_OK) != 0) { studio_reply(503, "application/json", "{\"error\":\"native_entity_unavailable\"}\n"); return 1; }
        {
            char *a[] = { ebin, (char*)"resolve", (char*)"text", value, (char*)"--kind", (char*)"org", NULL };
            if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0) {
                fprintf(stderr, "group create: %s\n", diag);
                studio_reply(503, "application/json", "{\"error\":\"group_create_failed\"}\n"); return 1;
            }
        }
        studio_reply(200, "text/plain; charset=utf-8", output); return 1;
    }
    if (!strcmp(target, "/studio/v1/identity/relationship")) {
        /* Bind two entities. A link is a stated relationship with a confidence,
         * never an assertion that either party agreed to it. */
        char from[128], to[128], conf_raw[32], conf[32], ebin[PATH_MAX], diag[1024];
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        if (!studio_json_string(body, "from", from, sizeof(from)) || !studio_safe_ident(from) ||
            !studio_json_string(body, "to", to, sizeof(to)) || !studio_safe_ident(to)) {
            studio_reply(503, "application/json", "{\"error\":\"from and to entity ids are required\"}\n"); return 1;
        }
        if (!studio_json_string(body, "confidence", conf_raw, sizeof(conf_raw))) snprintf(conf_raw, sizeof(conf_raw), "%s", "1.0");
        {
            char *end = NULL; double v;
            errno = 0; v = strtod(conf_raw, &end);
            if (errno || !end || *end || v < 0.0 || v > 1.0) {
                studio_reply(503, "application/json", "{\"error\":\"confidence must be between 0 and 1\"}\n"); return 1;
            }
            snprintf(conf, sizeof(conf), "%.4f", v);
        }
        snprintf(ebin, sizeof(ebin), "%s/bonfyre-entity", bindir);
        if (access(ebin, X_OK) != 0) { studio_reply(503, "application/json", "{\"error\":\"native_entity_unavailable\"}\n"); return 1; }
        {
            char *a[] = { ebin, (char*)"link", from, to, conf, NULL };
            if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0) {
                fprintf(stderr, "relationship: %s\n", diag);
                studio_reply(503, "application/json", "{\"error\":\"link_failed\"}\n"); return 1;
            }
        }
        studio_reply(200, "text/plain; charset=utf-8", output); return 1;
    }

    /* ── WORK: escalate and supersede ────────────────────────────────────── */
    if (!strcmp(target, "/studio/v1/work/escalate") || !strcmp(target, "/studio/v1/work/supersede")) {
        int supersede = !strcmp(target, "/studio/v1/work/supersede");
        char mission[97], node[97], worker[97], token[97];
        char workdb[PATH_MAX], qbin[PATH_MAX], diag[1024];
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        if (!studio_json_string(body, "mission", mission, sizeof(mission)) || !studio_safe_ident(mission) ||
            !studio_json_string(body, "node", node, sizeof(node)) || !studio_safe_ident(node)) {
            studio_reply(503, "application/json", "{\"error\":\"mission and node are required\"}\n"); return 1;
        }
        snprintf(qbin, sizeof(qbin), "%s/bonfyre-queue", bindir);
        if (snprintf(workdb, sizeof(workdb), "%s/fleming-work.db", fleming_root()) >= (int)sizeof(workdb) ||
            access(qbin, X_OK) != 0 || access(workdb, R_OK) != 0) {
            studio_reply(503, "application/json", "{\"error\":\"native_workgraph_unavailable\"}\n"); return 1;
        }
        if (supersede) {
            char *a[] = { qbin, (char*)"cancel", mission, node, (char*)"--db", workdb, NULL };
            if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0 || !output[0]) {
                fprintf(stderr, "supersede: %s\n", diag);
                studio_reply(503, "application/json", "{\"error\":\"supersede_failed\"}\n"); return 1;
            }
        } else {
            /* Escalation is a failed attempt with its reason recorded, so the
             * retry policy and the receipt trail both stay intact. */
            if (!studio_json_string(body, "worker", worker, sizeof(worker)) || !studio_safe_ident(worker) ||
                !studio_json_string(body, "token", token, sizeof(token)) || !studio_safe_hex(token, 64)) {
                studio_reply(503, "application/json",
                    "{\"error\":\"escalation requires the worker and its 64-hex claim token\"}\n"); return 1;
            }
            {
                /* A failure carries its class and message: the queue records
                 * both on the attempt, so an escalation is a reason in the
                 * receipt trail rather than an unexplained retry. */
                char reason[241], cls[65];
                if (!studio_json_string(body, "reason", reason, sizeof(reason)) || !studio_safe_label(reason, 240))
                    snprintf(reason, sizeof(reason), "%s", "escalated from the studio surface");
                /* The scheduler accepts a closed set of failure classes and
                 * decides retryability from it. An escalation is a dependency
                 * on something outside the graph (a human, an approval), so
                 * that is the default; anything outside the set is refused
                 * here rather than rejected later as invalid_failure_class. */
                if (!studio_json_string(body, "class", cls, sizeof(cls)) || !studio_safe_ident(cls))
                    snprintf(cls, sizeof(cls), "%s", "dependency");
                if (strcmp(cls, "transient") && strcmp(cls, "timeout") && strcmp(cls, "resource") &&
                    strcmp(cls, "permanent") && strcmp(cls, "validation") && strcmp(cls, "dependency")) {
                    studio_reply(503, "application/json",
                        "{\"error\":\"invalid_failure_class\",\"supported\":"
                        "{\"retryable\":[\"transient\",\"timeout\",\"resource\"],"
                        "\"terminal\":[\"permanent\",\"validation\",\"dependency\"]}}\n");
                    return 1;
                }
                char *a[] = { qbin, (char*)"fail", mission, node, worker, token,
                              (char*)"--class", cls, (char*)"--message", reason,
                              (char*)"--db", workdb, NULL };
                if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0 || !output[0]) {
                    fprintf(stderr, "escalate: %s\n", diag);
                    studio_reply(503, "application/json", "{\"error\":\"escalate_failed\"}\n"); return 1;
                }
            }
        }
        studio_reply(200, ctype, output); return 1;
    }

    /* ── ECONOMY: offers and agreements ──────────────────────────────────── */
    if (!strcmp(target, "/studio/v1/economy/offer")) {
        char name[97], buy_raw[32], sell_raw[32], buy[32], sell[32], source[97];
        char fbin[PATH_MAX], diag[1024];
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        if (!studio_json_string(body, "name", name, sizeof(name)) || !studio_safe_ident(name) ||
            !studio_json_string(body, "buy", buy_raw, sizeof(buy_raw)) || !studio_safe_usd_amount(buy_raw, buy) ||
            !studio_json_string(body, "sell", sell_raw, sizeof(sell_raw)) || !studio_safe_usd_amount(sell_raw, sell)) {
            studio_reply(503, "application/json",
                "{\"error\":\"name, buy and sell are required; buy and sell must be non-negative\"}\n"); return 1;
        }
        if (!studio_json_string(body, "source", source, sizeof(source)) || !studio_safe_ident(source))
            snprintf(source, sizeof(source), "%s", "studio");
        snprintf(fbin, sizeof(fbin), "%s/bonfyre-finance", bindir);
        if (access(fbin, X_OK) != 0) { studio_reply(503, "application/json", "{\"error\":\"native_finance_unavailable\"}\n"); return 1; }
        {
            char *a[] = { fbin, (char*)"service", (char*)"add", (char*)"--name", name,
                          (char*)"--buy", buy, (char*)"--sell", sell, (char*)"--source", source, NULL };
            if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0 || !output[0]) {
                fprintf(stderr, "offer: %s\n", diag);
                studio_reply(503, "application/json", "{\"error\":\"offer_create_failed\"}\n"); return 1;
            }
        }
        studio_reply(200, "text/plain; charset=utf-8", output); return 1;
    }

    /* ── MODEL: register a body into the commons ─────────────────────────── */
    if (!strcmp(target, "/studio/v1/model/prepare")) {
        char id[97], mbin[PATH_MAX], diag[1024] = {0};
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\",\"read_path\":\"/studio/v1/models/list\"}\n"); return 1; }
        if (!studio_json_string(body, "model", id, sizeof(id)) || !studio_safe_ident(id)) {
            studio_reply(503, "application/json", "{\"error\":\"a registered model id is required\"}\n"); return 1;
        }
        snprintf(mbin, sizeof(mbin), "%s/bonfyre-model", bindir);
        if (access(mbin, X_OK) != 0) { studio_reply(503, "application/json", "{\"error\":\"native_model_unavailable\"}\n"); return 1; }
        {
            /* pull is the prepare step: it makes the body resident, and reports
             * honestly when the source is unreachable rather than faking it. */
            char *a[] = { mbin, (char*)"pull", id, NULL };
            int rc = auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag));
            if (rc != 0) {
                char err[2048];
                snprintf(err, sizeof(err), "{\"error\":\"prepare_failed\",\"model\":\"%s\",\"detail\":\"%.900s\"}\n",
                         id, diag[0] ? diag : output);
                studio_reply(503, "application/json", err); return 1;
            }
        }
        studio_reply(200, "text/plain; charset=utf-8", output); return 1;
    }
    if (!strcmp(target, "/studio/v1/frappe/compile")) {
        char lineage[32], stamp[32], app_path[PATH_MAX], pack_path[PATH_MAX], fbin[PATH_MAX], response[8192];
        const char *app_dir, *apps_root, *source_root;
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        if (!studio_json_string(body, "lineage", lineage, sizeof(lineage)) || !(app_dir = studio_frappe_app_dir(lineage))) {
            studio_reply(503, "application/json", "{\"error\":\"known Frappe lineage required\"}\n"); return 1;
        }
        apps_root = getenv("BONFYRE_FRAPPE_APPS_ROOT");
        if (!apps_root || !*apps_root) apps_root = "/opt/bonfyre/native-src/integrations/frappe-bench/apps";
        source_root = getenv("BONFYRE_NATIVE_SOURCE_ROOT");
        if (!source_root || !*source_root) source_root = "/opt/bonfyre/native-src";
        iso_utc_now(stamp, sizeof(stamp));
        for (char *p = stamp; *p; p++) if (!isalnum((unsigned char)*p)) *p = '-';
        if (snprintf(app_path, sizeof(app_path), "%s/%s", apps_root, app_dir) >= (int)sizeof(app_path) ||
            snprintf(pack_path, sizeof(pack_path), "/var/lib/bonfyre/frappe/%s-%s.pack.json", lineage, stamp) >= (int)sizeof(pack_path) ||
            snprintf(fbin, sizeof(fbin), "%s/bonfyre-frappe-compiler", bindir) >= (int)sizeof(fbin) ||
            access(app_path, R_OK) != 0 || access(fbin, X_OK) != 0) {
            studio_reply(503, "application/json", "{\"error\":\"native_frappe_source_unavailable\"}\n"); return 1;
        }
        if (mkdir("/var/lib/bonfyre/frappe", 0750) != 0 && errno != EEXIST) {
            studio_reply(503, "application/json", "{\"error\":\"frappe_pack_store_unavailable\"}\n"); return 1;
        }
        if (access(pack_path, F_OK) == 0) {
            studio_reply(503, "application/json", "{\"error\":\"frappe_compile_collision_retry\"}\n"); return 1;
        }
        {
            char *a[] = { fbin, "--root", (char *)source_root, "--phase", "1", "--emit-pack", pack_path, app_path, NULL };
            char diag[2048];
            if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0 || access(pack_path, R_OK) != 0) {
                fprintf(stderr, "frappe compile: %s\n", diag);
                studio_reply(503, "application/json", "{\"error\":\"native_frappe_compile_failed\"}\n"); return 1;
            }
        }
        snprintf(response, sizeof(response),
                 "{\"compiled\":true,\"lineage\":\"%s\",\"app_dir\":\"%s\",\"pack_ref\":\"file:%s\",\"phase\":1,\"forbidden_inference\":\"an AppPack is metadata grammar, not execution authority or a running Frappe site\"}\n",
                 lineage, app_dir, pack_path);
        studio_reply(200, ctype, response); return 1;
    }
    /* Per-lineage Frappe schema projection.
     *
     * studio_frappe_app_dir() is the allowlist that makes this safe: the caller
     * names a lineage, never a path, so no request can point the compiler at an
     * arbitrary directory. It had no caller and the build refused it as dead
     * code; this is the surface it was written for. */
    if (!strncmp(target, "/studio/v1/frappe/", 18)) {
        char lineage[64], apppath[PATH_MAX], fbin[PATH_MAX], diag[2048] = {0};
        const char *root = getenv("BONFYRE_FRAPPE_APPS");
        const char *dir;
        if (!studio_path_tail(target, "/studio/v1/frappe/", lineage, sizeof(lineage)) ||
            !studio_safe_ident(lineage)) {
            studio_reply(503, "application/json", "{\"error\":\"invalid_lineage\"}\n"); return 1;
        }
        dir = studio_frappe_app_dir(lineage);
        if (!dir) {
            studio_reply(503, "application/json",
                "{\"error\":\"unknown_lineage\",\"known\":[\"frappe-core\",\"crm\",\"helpdesk\","
                "\"erpnext\",\"hrms\",\"lms\",\"wiki\",\"insights\",\"drive\"]}\n");
            return 1;
        }
        if (!root || !*root) root = "/opt/bonfyre/native-src/integrations/frappe-bench/apps";
        snprintf(fbin, sizeof(fbin), "%s/bonfyre-frappe-compiler", bindir);
        if (snprintf(apppath, sizeof(apppath), "%s/%s", root, dir) >= (int)sizeof(apppath) ||
            access(fbin, X_OK) != 0 || access(apppath, R_OK) != 0) {
            studio_reply(503, "application/json", "{\"error\":\"frappe_app_unavailable\"}\n"); return 1;
        }
        {
            /* The compiler writes the IR to a file: only --emit-lineage-catalog
             * accepts "-", and its stdout is a human banner. It also runs a
             * phased pipeline that needs a root and a writable output dir, so
             * both are supplied inside the unit's own ReadWritePaths. Phase 1
             * stops after schema extraction: no controller, hook or migration
             * is executed, and nothing external is touched. */
            const char *outroot = getenv("BONFYRE_FRAPPE_OUT");
            char irpath[PATH_MAX], workdir[PATH_MAX];
            FILE *f;
            size_t got = 0;
            if (!outroot || !*outroot) outroot = "/var/lib/bonfyre/state/frappe-out";
            (void)mkdir(outroot, 0750);
            if (snprintf(irpath, sizeof(irpath), "%s/%s-schema-ir.json", outroot, lineage) >= (int)sizeof(irpath) ||
                snprintf(workdir, sizeof(workdir), "%s", outroot) >= (int)sizeof(workdir)) {
                studio_reply(503, "application/json", "{\"error\":\"frappe_path_overflow\"}\n"); return 1;
            }
            {
                char *a[] = { fbin, (char*)"-r", (char*)"/opt/bonfyre/native-src",
                              (char*)"-o", workdir, (char*)"--emit-schema-ir", irpath,
                              (char*)"--phase", (char*)"1", apppath, NULL };
                if (auth_call_split(a, NULL, output, sizeof(output), diag, sizeof(diag)) != 0) {
                    fprintf(stderr, "frappe %s: %s\n", lineage, diag[0] ? diag : output);
                    studio_reply(503, "application/json", "{\"error\":\"schema_extract_failed\"}\n"); return 1;
                }
            }
            (void)f; (void)got;
            /* Stream it: the ERPNext IR alone is ~960 KB. */
            if (!studio_reply_file(ctype, irpath)) {
                studio_reply(503, "application/json", "{\"error\":\"schema_ir_unreadable\"}\n");
            }
            return 1;
        }
    }
    /* ── assets and themes are ForeignTwins bound to people ───────────────
     * Observing admits the bytes under the foreign law: a declared observe
     * right, a robots preflight, a receipted body and a content digest. Binding
     * records an edge in the identity graph between the account and the asset
     * twin, which is what turns a shared cache of external things into one
     * account's expression. Binding states that this account uses this twin --
     * never that it holds a right in the underlying work. */
    if (!strcmp(target, "/studio/v1/assets/observe")) {
        char url[1400], twin[97], receipt[PATH_MAX], retained[PATH_MAX], stamp[32], digest[80];
        StudioViewer who;
        if (strcmp(method, "POST")) { studio_reply(405, "application/json", "{\"error\":\"POST required\"}\n"); return 1; }
        if (!session_identity(bearer, auth_db, &who)) {
            studio_reply(401, "application/json", "{\"error\":\"session_required\"}\n"); return 1;
        }
        if (!studio_json_string(body, "url", url, sizeof(url)) ||
            (strncmp(url, "https://", 8) && strncmp(url, "http://", 7))) {
            studio_reply(503, "application/json", "{\"error\":\"a public http(s) url is required\"}\n"); return 1;
        }
        iso_utc_now(stamp, sizeof(stamp));
        for (char *pz = stamp; *pz; pz++) if (!isdigit((unsigned char)*pz)) *pz = '-';
        snprintf(twin, sizeof(twin), "asset-%ld-%s", who.id, stamp);
        (void)mkdir("/var/lib/bonfyre/foreign", 0750);
        (void)mkdir("/var/lib/bonfyre/foreign/receipts", 0750);
        (void)mkdir("/var/lib/bonfyre/foreign/bodies", 0750);
        if (snprintf(receipt, sizeof(receipt), "/var/lib/bonfyre/foreign/receipts/%s.json", twin) >= (int)sizeof(receipt) ||
            snprintf(retained, sizeof(retained), "/var/lib/bonfyre/foreign/bodies/%s.body", twin) >= (int)sizeof(retained)) {
            studio_reply(503, "application/json", "{\"error\":\"receipt_path_overflow\"}\n"); return 1;
        }
        {
            char pbin[PATH_MAX];
            FILE *rf;
            size_t got = 0;
            int rc;
            snprintf(pbin, sizeof(pbin), "%s/bonfyre-project", bindir);
            {
                char *a[] = { pbin, (char*)"foreign", (char*)"observe", twin, url,
                              (char*)"--receipt", receipt, (char*)"--body", retained,
                              (char*)"--observe-right", (char*)"public", NULL };
                rc = access(pbin, X_OK) == 0 ? auth_call(a, NULL, output, sizeof(output)) : 127;
            }
            rf = fopen(receipt, "rb");
            if (rf) { got = fread(output, 1, sizeof(output) - 1, rf); output[got] = '\0'; fclose(rf); }
            if (rc != 0 && !got) {
                studio_reply(503, "application/json", "{\"error\":\"foreign_observer_unavailable\"}\n"); return 1;
            }
        }
        if (!studio_json_field(output, "content_sha256", digest, sizeof(digest)) || !studio_safe_hex(digest, 64)) {
            /* Nothing was admitted. Return the receipt so the caller sees which
             * gate refused, rather than a bare failure. */
            studio_reply(200, "application/json; charset=utf-8", output); return 1;
        }
        {
            char ebin[PATH_MAX], value[160], eout[4096], diag[1024];
            snprintf(ebin, sizeof(ebin), "%s/bonfyre-entity", bindir);
            snprintf(value, sizeof(value), "asset:%s", digest);
            if (access(ebin, X_OK) == 0) {
                char *a[] = { ebin, (char*)"resolve", (char*)"text", value, (char*)"--kind", (char*)"object", NULL };
                (void)auth_call_split(a, NULL, eout, sizeof(eout), diag, sizeof(diag));
            }
        }
        {
            const char *fmt = "{\"admitted\":true,\"twin_id\":\"%s\",\"digest\":\"%s\",\"href\":\"/assets/twin/%s\","
              "\"entity\":\"asset:%s\",\"receipt\":%s,"
              "\"forbidden_inference\":\"admitting an asset is not a licence; a cached projection carries no right in the underlying work\"}\n";
            size_t need = strlen(fmt) + strlen(twin) + 3 * strlen(digest) + strlen(output) + 1;
            char *reply = malloc(need);
            if (!reply) { studio_reply(503, "application/json", "{\"error\":\"asset_receipt_reply_allocation_failed\"}\n"); return 1; }
            snprintf(reply, need, fmt, twin, digest, digest, digest, output);
            studio_reply(200, ctype, reply);
            free(reply);
        }
        return 1;
    }
    if (!strcmp(target, "/studio/v1/theme")) {
        StudioViewer who;
        char ebin[PATH_MAX], diag[1024];
        if (!session_identity(bearer, auth_db, &who)) {
            studio_reply(401, "application/json", "{\"error\":\"session_required\"}\n"); return 1;
        }
        snprintf(ebin, sizeof(ebin), "%s/bonfyre-entity", bindir);
        if (!strcmp(method, "POST")) {
            char digest[80], acct[224], asset[224], apath[PATH_MAX];
            if (!studio_json_string(body, "digest", digest, sizeof(digest)) || !studio_safe_hex(digest, 64)) {
                studio_reply(503, "application/json",
                    "{\"error\":\"a 64-hex asset twin digest is required\","
                    "\"hint\":\"POST /studio/v1/assets/observe first\"}\n"); return 1;
            }
            if (!asset_body_for_digest(digest, apath, sizeof(apath))) {
                studio_reply(503, "application/json",
                    "{\"error\":\"unknown_asset_twin\",\"reason\":\"no receipted observation carries this digest\"}\n");
                return 1;
            }
            snprintf(acct, sizeof(acct), "account:%s", who.email);
            snprintf(asset, sizeof(asset), "asset:%s", digest);
            if (access(ebin, X_OK) != 0) {
                studio_reply(503, "application/json", "{\"error\":\"native_entity_unavailable\"}\n"); return 1;
            }
            {
                char e1[4096], e2[4096], id1[128] = {0}, id2[128] = {0};
                char *r1[] = { ebin, (char*)"resolve", (char*)"text", acct, (char*)"--kind", (char*)"person", NULL };
                char *r2[] = { ebin, (char*)"resolve", (char*)"text", asset, (char*)"--kind", (char*)"object", NULL };
                const char *pp;
                auth_call_split(r1, NULL, e1, sizeof(e1), diag, sizeof(diag));
                auth_call_split(r2, NULL, e2, sizeof(e2), diag, sizeof(diag));
                /* resolve prints "id=<entity>" when it creates one and
                 * "entity=<entity>" when it matched an existing one. Read the
                 * identifier itself rather than either surrounding phrase. */
                pp = strstr(e1, "entity-"); if (pp) sscanf(pp, "%127[a-zA-Z0-9-]", id1);
                pp = strstr(e2, "entity-"); if (pp) sscanf(pp, "%127[a-zA-Z0-9-]", id2);
                if (!id1[0] || !id2[0]) {
                    studio_reply(503, "application/json", "{\"error\":\"entity_resolve_failed\"}\n"); return 1;
                }
                {
                    char *lk[] = { ebin, (char*)"link", id1, id2, (char*)"1.0", NULL };
                    if (auth_call_split(lk, NULL, output, sizeof(output), diag, sizeof(diag)) != 0) {
                        fprintf(stderr, "theme bind: %s\n", diag);
                        studio_reply(503, "application/json", "{\"error\":\"bind_failed\"}\n"); return 1;
                    }
                }
                {
                    static char reply[2048];
                    snprintf(reply, sizeof(reply),
                      "{\"bound\":true,\"account\":\"%s\",\"asset\":\"%s\",\"href\":\"/assets/twin/%s\","
                      "\"forbidden_inference\":\"a bound theme is a stated preference of this account, not a right in the work it points at\"}\n",
                      acct, asset, digest);
                    studio_reply(200, ctype, reply);
                }
            }
            return 1;
        }
        {
            char sql[1024];
            snprintf(sql, sizeof(sql),
              "SELECT e2.display AS asset, l.confidence FROM e.links l "
              "JOIN e.entities e1 ON e1.id = l.a JOIN e.entities e2 ON e2.id = l.b "
              "WHERE lower(e1.display) = lower('account:%s') AND e2.display LIKE 'asset:%%'",
              who.email);
            if (entity_query(bindir, sql, output, sizeof(output)) != 0 || !output[0]) {
                studio_reply(200, ctype, "{\"bound\":[],\"note\":\"nothing bound to this account yet\"}\n");
                return 1;
            }
        }
        studio_reply(200, ctype, output);
        return 1;
    }
    /* Institution-scoped reads.
     *
     * profile, closure, membrane and theme describe the caller's own closure and
     * are correctly readable by any session. These four describe the
     * institution's interior: budget consumption per recipe, the model estate
     * with sizes, the compiled Frappe lineage catalogue, and a 241KB derivation
     * ledger. The write gate above covers mutations only, so every one of them
     * was readable by any authenticated session -- an account created sixty
     * seconds earlier with a throwaway address read `fleming-harvest 63.6% of
     * budget spent`. Anonymous callers were already refused; the boundary was
     * drawn at "has a session" rather than "was admitted".
     *
     * Cannot infer: write scope -> authority over what is read here. This gate
     * decides who may look, and the organs behind it still own their effects. */
    if (!strcmp(target, "/studio/v1/frappe-lineages")) { snprintf(bin, sizeof(bin), "%s/bonfyre-frappe-compiler", bindir); argv[1] = "--emit-lineage-catalog"; argv[2] = "-"; }
    else if (!strcmp(target, "/studio/v1/models/status")) { snprintf(bin, sizeof(bin), "%s/bonfyre-model", bindir); argv[1] = "status"; ctype = "text/plain; charset=utf-8"; }
    else if (!strcmp(target, "/studio/v1/models/list")) { snprintf(bin, sizeof(bin), "%s/bonfyre-model", bindir); argv[1] = "list"; ctype = "text/plain; charset=utf-8"; }
    else if (!strcmp(target, "/studio/v1/economy/status")) { snprintf(bin, sizeof(bin), "%s/bonfyre-economy", bindir); argv[1] = "status"; ctype = "text/plain; charset=utf-8"; }
    else if (!strcmp(target, "/studio/v1/ledger/export")) { const char *root = getenv("BONFYRE_LEDGER_ROOT"); if (!root || !*root) root = "/opt/bonfyre/output/recursive-institutional-growth"; snprintf(bin, sizeof(bin), "%s/bonfyre-ledger", bindir); argv[1] = "export"; argv[2] = (char*)root; argv[3] = "--format"; argv[4] = "json"; }
    /* An unrecognised path is 404, not 503. This answered "service unavailable"
     * -- the server is broken, try later -- to a caller whose only mistake was
     * a path that does not exist, and the projections here take sub-paths
     * (models/status, economy/status) so guessing the bare name is the normal
     * first attempt. Naming the routes turns three failed probes into one. */
    else {
        studio_reply(404, "application/json",
          "{\"error\":\"unknown_studio_projection\","
          "\"known\":[\"/studio/v1/profile\",\"/studio/v1/closure\",\"/studio/v1/membrane\","
          "\"/studio/v1/theme\",\"/studio/v1/frappe-lineages\",\"/studio/v1/models/status\","
          "\"/studio/v1/models/list\",\"/studio/v1/economy/status\",\"/studio/v1/ledger/export\"],"
          "\"note\":\"a listed projection still answers only within the caller's own closure\"}\n");
        return 1;
    }
    if (access(bin, X_OK) != 0 || auth_call(argv, NULL, output, sizeof(output)) != 0) { studio_reply(503, "application/json", "{\"error\":\"native_organ_unavailable\"}\n"); return 1; }
    studio_reply(200, ctype, output); return 1;
}

static int serve_surface(BfMatrix *m, const char *host_in, const char *target,
                         const char *root, const char *accept, const char *query) {
    static const struct { const char *prefix; const char *name; } routes[] = {
        {"/api/",           "api"},
        {"/api",            "api"},
        {"/cms/",           "cms"},
        {"/cms",            "cms"},
        {"/studio/",        "studio"},
        {"/studio",         "studio"},
        {"/studio/controls","studio"},
        /* /habitat, /property, /market, /sources and /agents are authored
         * surfaces, and serve_authored_page answers before this table is
         * consulted -- so in normal operation these rows serve nobody.
         *
         * They were deleted on exactly that reasoning, and that was wrong.
         * serve_authored_page declines when the CMS is unreachable, and these
         * rows are what answers then: with them gone, a CMS outage stopped
         * /agents and /market being HTML at all and turned a carrier surface
         * into a 404. The a2a suite runs the organ with no CMS at all, which
         * is why it caught this and a live probe never would.
         *
         * So they stay, as the compiled floor under the authored surface. The
         * authored page is what a reader gets; this is what they get when the
         * store that holds it cannot be read.
         *
         * /cms and /studio are here for a different reason: they are
         * applications, not pages. Authoring them would file an application
         * under a name meant for copy with a heading. */
        /* One page, four territories, six modes. It reads the live /_surface/
         * pattern catalogue rather than owning records, so it is a page with a
         * heading and belongs here beside the other carrier surfaces rather
         * than under /cms or /studio. */
        {"/territory/",     "territory"},
        {"/territory",      "territory"},
        /* The clone surfaces. Each borrows one market interface's behaviour --
         * its layout, density and gesture -- and reads this estate's own live
         * endpoints. They are pages with headings, so they belong here beside
         * /market and /habitat rather than under /cms or /studio. */
        {"/discover/",      "discover"},
        {"/discover",       "discover"},
        {"/board/",         "board"},
        {"/board",          "board"},
        {"/community/",     "community"},
        {"/community",      "community"},
        {"/listings/",      "listings"},
        {"/listings",       "listings"},
        {"/parcels/",       "parcels"},
        {"/parcels",        "parcels"},
        {"/inbox/",         "inbox"},
        {"/inbox",          "inbox"},
        {"/channels/",      "channels"},
        {"/channels",       "channels"},
        {"/stage/",         "stage"},
        {"/stage",          "stage"},
        /* The township is the habitat projection itself -- a walkable town
         * compiled from real fabric/CMS/capital rows. It gets its own route
         * because /habitat is an authored CMS page, and serve_authored_page
         * answers before this table is consulted; a surface named "habitat"
         * would be shadowed by the authored one and never reached. */
        {"/township/",      "township"},
        {"/township",       "township"},
        /* The console is the surface a host's own person operates. It is on
         * every host on purpose: an institution is a host (S12), so the place
         * where its owner decides what is shown and at what density has to
         * answer at that host's own address rather than at a central one. */
        {"/console/",       "console"},
        {"/console",        "console"},
        /* Linking is the other half of federation. The estate observes the
         * outside world already; /link is where a person points it at one more
         * source and connects an account, using the authorization-server
         * metadata the estate has itself observed rather than a provider list
         * somebody typed in. */
        {"/link/",          "link"},
        {"/link",           "link"},
        /* The rich existing authored workspace is separately addressable.
         * Neither alias reclassifies a generic work item as an observed source. */
        {"/foreign/workbench/",     "link"},
        {"/foreign/workbench",      "link"},
        {"/s/work/foreign-twins/",  "link"},
        {"/s/work/foreign-twins",   "link"},
        /* The pack graph and a closure resolved in front of you. It is a page
         * with a heading, so it belongs here rather than under /studio: what a
         * community can see about the behaviour it runs is not an operator
         * tool. */
        {"/packs/",         "packs"},
        {"/packs",          "packs"},
        /* A world is one page that renders whichever world the address names.
         * Fifteen worlds are not fifteen templates; they are one composition
         * over the same grammars, wearing the visual character the subject
         * actually calls for. */
        {"/w/",             "world"},
        {"/w",              "world"},
        /* The territory root. serve_authored_page declines the human
         * projection of "/" when this surface is deployed, and keeps answering
         * machines with the discovery descriptor they already had. */
        {"/",               "home"},
        {"/habitat/",       "habitat"},
        {"/habitat",        "habitat"},
        {"/property/",      "property"},
        {"/property",       "property"},
        {"/market/",        "market"},
        {"/market",         "market"},
        {"/sources/",       "sources"},
        {"/sources",        "sources"},
        {"/agents/",        "agents"},
        {"/agents",         "agents"},
        {"/mcp/resources/", "mcp"},
        {"/mcp/resources",  "mcp"},
        {"/a2a/resources/", "a2a"},
        {"/a2a/resources",  "a2a"},
    };
    const char *name = NULL;
    for (size_t i = 0; i < sizeof(routes)/sizeof(routes[0]); i++) {
        if (!strcmp(target, routes[i].prefix)) { name = routes[i].name; break; }
    }
    /* A world is addressed by name: /w/<world>. The table matches exact paths,
     * which is right for a surface and wrong for a surface that takes a
     * subject in the path, so this one prefix is resolved here rather than
     * enumerating every world as a route. */
    if (!name && !strncmp(target, "/w/", 3) && target[3]) name = "world";
    if (!name) return 0;

    char host[256]; BfMatrixDomainResolution domain;
    if (!lower_host(host_in, host) || bf_matrix_domain_resolve(m, host, &domain) != 1) {
        printf("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n"
               "Connection: close\r\n\r\n{\"verdict\":\"unknown_host\"}\n");
        return 1;
    }
    if (!surface_live(name, root)) {
        printf("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n"
               "Connection: close\r\n\r\n{\"verdict\":\"surface_not_served\",\"surface\":");
        json(name); printf("}\n");
        return 1;
    }
    if (is_generated(name)) return emit_generated_surface(name);

    char path[PATH_MAX];
    if (!surface_file_pref(path, sizeof(path), root, name, wants_json(accept))) {
        printf("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n"
               "Connection: close\r\n\r\n{\"verdict\":\"surface_not_served\",\"surface\":");
        json(name);
        printf("}\n");
        return 1;
    }
    if (strstr(path, ".html")) {
        if (emit_closure_linked_html(m, path, host, target, query)) return 1;
        printf("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
        return 1;
    }
    FILE *f = fopen(path, "rb");
    if (!f) {
        printf("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
        return 1;
    }
    const char *ctype = "application/json; charset=utf-8";
    long len = 0;
    if (!fseek(f, 0, SEEK_END)) { len = ftell(f); rewind(f); }
    printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\nContent-Length: %ld\r\n"
           "Cache-Control: no-cache\r\nConnection: close\r\n\r\n", ctype, len);
    char buf[8192]; size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) fwrite(buf, 1, n, stdout);
    fclose(f);
    return 1;
}

/* The address plane, listed. Every host the Matrix carries, with the
 * institution that holds it and the territory it participates in. Public,
 * because a directory of public addresses is not a disclosure -- each of these
 * hosts already answers /.well-known/bonfyre to anybody. It answers any origin
 * for the same reason that document does: a page on one territory has to be
 * able to show the estate's real shape without pretending to know it. */
#endif /* BONFYRE_DESK_AND_INTERNET_OBSERVATORY_H */
