#ifndef BONFYRE_X402_CAPABILITY_AND_APERTURE_FORMS_H
#define BONFYRE_X402_CAPABILITY_AND_APERTURE_FORMS_H
/* Extracted verbatim from main.c lines 14174-15948 (mechanical split, no logic changes). */
static int x402_price_usd(const char *name, double *out) {
    const char *fin = getenv("BONFYRE_FINANCE_DB");
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int found = 0;
    if (!fin || !*fin) fin = "/var/lib/bonfyre/state/finance.db";
    if (sqlite3_open_v2(fin, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { if (db) sqlite3_close(db); return 0; }
    if (sqlite3_prepare_v2(db, "SELECT sell_price FROM services WHERE name = ? LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, name, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) { *out = sqlite3_column_double(st, 0); found = 1; }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    return found;
}

static const char *x402_network(void) {
    const char *n = getenv("BONFYRE_X402_NETWORK");
    return n && *n ? n : "base-sepolia";
}
static const char *x402_pay_to(void) {
    const char *a = getenv("BONFYRE_X402_PAY_TO");
    return a && *a ? a : "";
}
static const char *x402_asset(void) {
    const char *a = getenv("BONFYRE_X402_ASSET");
    return a && *a ? a : "";
}
/* Which world does this institution execute in?
 *
 * Declared in architecture/institutional/namespaces.yaff and generated into
 * BF_INSTITUTION_WORLDS. An institution that declares no world is LIVE: an
 * estate that forgets to say must be treated as real, never silently
 * sandboxed, because the failure directions are not symmetric. Treating a live
 * institution as demo would quietly refuse real settlement; treating a demo
 * institution as live would let a synthetic estate raise a real external
 * effect. Only one of those is recoverable.
 *
 * Cannot infer: an institution's world -> its authority. A LIVE world does not
 * grant anything; it only stops withholding what DEMO withholds. */
static const char *institution_world(const char *institution) {
    static const char *worlds[][2] = BF_INSTITUTION_WORLDS;
    size_t i;
    if (!institution || !*institution) return "LIVE";
    for (i = 0; i < BF_INSTITUTION_WORLD_N; i++)
        if (!strcmp(institution, worlds[i][0])) return worlds[i][1];
    return "LIVE";
}

static int institution_is_demo(const char *institution) {
    return !strcmp(institution_world(institution), "DEMO");
}

/* The facilitator, as seen by a given institution.
 *
 * The worlds contract says a DEMO world admits real public observation and
 * never a live external effect against a third party, and that financial
 * projection in DEMO is proposal economics rather than revenue. Settlement is
 * the external effect that matters most here, so a demo institution has no
 * facilitator no matter what the environment configures.
 *
 * This is the whole reason the world is generated rather than written in a
 * comment: without a lookup the declaration gates nothing, and "demo" is a
 * label on a page instead of a property of execution. */
static const char *x402_facilitator_for(const char *institution) {
    const char *f;
    if (institution_is_demo(institution)) return "";
    f = getenv("BONFYRE_X402_FACILITATOR");
    return f && *f ? f : "";
}

static const char *x402_facilitator(void) {
    return x402_facilitator_for(NULL);
}

/* What could settle here, and what could not.
 *
 * Governance and the chain are the same question asked twice. An institution
 * declares the world it executes in; that declaration -- generated, not
 * written in a comment -- is what decides whether a payment presentation can
 * become a live external effect or is proposal economics. A DEMO institution
 * has no facilitator however the environment is configured, and an institution
 * that declares no world is LIVE, because an estate that forgets to say must be
 * treated as real.
 *
 * What is shown is the standing, never a settlement. A receipt reference is a
 * SHA-256 of something presented; it is not proof that value moved, and this
 * estate holds no authority to move any.
 */
static void room_settlement(char *out, size_t cap, const char *institution_id) {
    sqlite3 *mx = NULL; sqlite3_stmt *st = NULL;
    char world[32] = {0}, ikind[64] = {0}, ev[512] = {0};
    char e_inst[256], e_ev[1024];
    const char *path = getenv("BONFYRE_MATRIX_DB");
    char uri[PATH_MAX];
    const char *net, *asset, *fac;
    long receipts = 0, realized = 0;

    out[0] = '\0';
    if (!institution_id || !*institution_id) return;

    if (path && *path && bf_sqlite_readonly_uri(uri, sizeof(uri), path) < (int)sizeof(uri) &&
        sqlite3_open_v2(uri, &mx, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK &&
        sqlite3_prepare_v2(mx, "SELECT COALESCE(institution_kind,''), COALESCE(world,''),"
                               " COALESCE(evidence_ref,'') FROM matrix_institutions"
                               " WHERE institution_id = ?1 LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, institution_id, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            snprintf(ikind, sizeof(ikind), "%s", (const char *)sqlite3_column_text(st, 0));
            snprintf(world, sizeof(world), "%s", (const char *)sqlite3_column_text(st, 1));
            snprintf(ev, sizeof(ev), "%s", (const char *)sqlite3_column_text(st, 2));
        }
        sqlite3_finalize(st); st = NULL;
    }
    if (mx) sqlite3_close(mx);
    if (!world[0]) return;   /* no declaration here: say nothing rather than guess */

    /* What the estate has actually receipted, so the standing is shown beside
     * the evidence rather than on its own. */
    {
        const char *mpath = getenv("BONFYRE_METER_DB");
        sqlite3 *me = NULL;
        if (mpath && *mpath && bf_sqlite_readonly_uri(uri, sizeof(uri), mpath) < (int)sizeof(uri) &&
            sqlite3_open_v2(uri, &me, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK &&
            sqlite3_prepare_v2(me, "SELECT COUNT(DISTINCT receipt_ref),"
                                   " SUM(trim(realization_ref) <> '') FROM events", -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) {
                receipts = (long)sqlite3_column_int64(st, 0);
                realized = (long)sqlite3_column_int64(st, 1);
            }
            sqlite3_finalize(st); st = NULL;
        }
        if (me) sqlite3_close(me);
    }

    net = x402_network();
    asset = x402_asset();
    fac = x402_facilitator_for(institution_id);

    html_escape(institution_id, e_inst, sizeof(e_inst));
    html_escape(ev, e_ev, sizeof(e_ev));
    snprintf(out, cap,
      "<h3 class=\"room-h\">What could settle here</h3>"
      "<dl class=\"flow-facts\">"
      "<div><dt>Institution</dt><dd class=\"mono\">%s</dd></div>"
      "<div><dt>Kind</dt><dd>%s</dd></div>"
      "<div><dt>World</dt><dd>%s</dd></div>"
      "<div><dt>Settlement path</dt><dd>%s</dd></div>"
      "<div><dt>Chain</dt><dd class=\"mono\">%s</dd></div>"
      "<div><dt>Receipted</dt><dd>%ld</dd></div>"
      "<div><dt>Realized</dt><dd>%ld</dd></div>"
      "</dl>"
      "%s%s%s"
      "<p class=\"subj-note\">The world an institution declares is what decides whether a payment "
      "presentation can become a live external effect. A DEMO institution has no facilitator "
      "however the environment is configured, and an institution that declares no world is treated "
      "as LIVE, because an estate that forgets to say must be treated as real. %s "
      "A receipt reference is a SHA-256 of something presented: it is not proof that value moved, "
      "and nothing on this page moves any.</p>",
      e_inst, ikind[0] ? ikind : "not stated", world,
      fac && *fac ? "a facilitator stands behind it" : "none: nothing here can settle",
      (asset && *asset) ? asset : net,
      receipts, realized,
      e_ev[0] ? "<p class=\"subj-note\">Admitted on the evidence: " : "", e_ev, e_ev[0] ? "</p>" : "",
      strcmp(world, "DEMO") ? "" :
        "In DEMO the estate admits real public observation and never a live external effect against "
        "a third party, and financial projection is proposal economics rather than revenue.");
}


/* Which institution is this host serving?
 *
 * Resolved through the Matrix, which already binds every host to an
 * institution_id, rather than by reading the hostname. Hostname convention is
 * how "upper90.aurekai.work belongs to upper90" becomes true by spelling
 * instead of by declaration, and this estate has spent real effort removing
 * exactly that.
 *
 * institution_id is a colon path -- institution:aurekai:life,
 * institution:nightgrid -- and the world table is keyed by the root name, so
 * the segment after the prefix is what the world is looked up under.
 *
 * Returns an empty string when the host resolves to nothing, which
 * institution_world reads as LIVE. An unresolvable host must not become a
 * sandboxed one. */
static const char *serving_institution(const char *host_in) {
    static char root[128];
    char host[256];
    BfMatrixDomainResolution domain;
    BfMatrix *m = NULL;
    const char *db = getenv("BONFYRE_MATRIX_DB");
    const char *id, *seg;
    size_t n;
    int resolved;

    root[0] = '\0';
    if (!host_in || !db || !*db || !lower_host(host_in, host)) return root;
    /* The capability dispatcher holds no Matrix handle, so this opens its own
     * read path rather than threading one through every protocol surface. The
     * lookup happens once per quote, which is not a hot path. */
    if (bf_matrix_open(db, &m, NULL) || !m) return root;
    resolved = bf_matrix_domain_resolve(m, host, &domain);
    if (resolved != 1) { bf_matrix_close(m); return root; }
    id = domain.institution_id;
    if (!id || !*id) { bf_matrix_close(m); return root; }
    seg = strchr(id, ':');
    seg = seg ? seg + 1 : id;
    for (n = 0; seg[n] && seg[n] != ':' && n + 1 < sizeof(root); n++) root[n] = seg[n];
    root[n] = '\0';
    bf_matrix_close(m);
    return root;
}

/* One settlement declaration, rendered into each commerce grammar.
 *
 * "x402" was written separately into the UCP checkout block, the ACP settlement
 * block and the MPP quote, while Beckn quoted an amount and declared nothing at
 * all -- so an agent arriving over Beckn received a price with no way to pay it,
 * and the other three could drift apart one edit at a time. The rail is one
 * fact about this institution; a carrier is a rendering of that fact, not a
 * second copy of it. Changing where settlement happens is now one edit.
 *
 * Cannot infer: a declared rail -> a configured one. These name where payment
 * would settle; whether it can settle is the x402 requirements block's answer,
 * and today that answer is no. */
#define BF_SETTLEMENT_RAIL      "x402"
#define BF_SETTLEMENT_QUOTE     "/mpp/quote"
#define BF_SETTLEMENT_DISCOVERY "/capability"

/* Format the x402 requirements for one capability into `out`.
 *
 * Both the HTTP 402 on /capability and the JSON-RPC payment error on /a2a are
 * built from this, so the two carriers cannot describe the same purchase with
 * different numbers. Returns 0 on overflow. */
static int x402_accepts_json(char *out, size_t out_sz, const char *host,
                             const X402Capability *cap, double usd) {
    char resource[512];
    /* x402 quotes an integer amount in the asset's base units. USDC is 6dp. */
    long long base_units = (long long)(usd * 1000000.0 + 0.5);
    const char *payto = x402_pay_to();
    const char *asset = x402_asset();
    int n;
    snprintf(resource, sizeof(resource), "https://%s/capability/%s",
             host ? host : "aurekai.life", cap->name);
    n = snprintf(out, out_sz,
        "[{\"scheme\":\"exact\",\"network\":\"%s\",\"maxAmountRequired\":\"%lld\","
        "\"resource\":\"%s\",\"description\":\"%s\",\"mimeType\":\"application/json\","
        "\"payTo\":\"%s\",\"maxTimeoutSeconds\":60,\"asset\":\"%s\","
        "\"extra\":{\"priceUsd\":\"%.6f\",\"decimals\":6}}]",
        x402_network(), base_units, resource, cap->summary,
        payto[0] ? payto : "unset", asset[0] ? asset : "unset", usd);
    return (n > 0 && (size_t)n < out_sz);
}

/* Emit the x402 payment-required response for one capability. */
static void x402_require_payment(const char *host, const X402Capability *cap, double usd) {
    const char *serving = serving_institution(host);
    static char body[8192];
    char resource[512];
    /* x402 quotes an integer amount in the asset's base units. USDC is 6dp. */
    long long base_units = (long long)(usd * 1000000.0 + 0.5);
    const char *payto = x402_pay_to();
    const char *asset = x402_asset();
    snprintf(resource, sizeof(resource), "https://%s/capability/%s", host ? host : "aurekai.life", cap->name);
    int n = snprintf(body, sizeof(body),
        "{\"x402Version\":1,"
        "\"error\":\"payment required\","
        "\"accepts\":[{"
          "\"scheme\":\"exact\","
          "\"network\":\"%s\","
          "\"maxAmountRequired\":\"%lld\","
          "\"resource\":\"%s\","
          "\"description\":\"%s\","
          "\"mimeType\":\"application/json\","
          "\"payTo\":\"%s\","
          "\"maxTimeoutSeconds\":60,"
          "\"asset\":\"%s\","
          "\"extra\":{\"priceUsd\":\"%.6f\",\"decimals\":6}"
        "}],"
        "\"settlement\":{\"facilitator_configured\":%s,"
          "\"world\":\"%s\","
          "\"note\":\"an X-PAYMENT header is verified through the configured facilitator; "
                     "without one this endpoint refuses rather than treating a payment as settled. "
                     "A DEMO institution has no facilitator by contract: its economics are "
                     "proposal economics, never revenue.\"}}\n",
        x402_network(), base_units, resource, cap->summary,
        payto[0] ? payto : "unset", asset[0] ? asset : "unset", usd,
        x402_facilitator_for(serving)[0] ? "true" : "false",
        institution_world(serving));
    if (n < 0 || (size_t)n >= sizeof(body)) { studio_reply(503, "application/json", "{\"error\":\"quote_overflow\"}\n"); return; }
    printf("HTTP/1.1 402 Payment Required\r\nContent-Type: application/json\r\n"
           "Content-Length: %zu\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
           strlen(body), body);
    fflush(stdout);
}

/* The capability surface: discovery is free, execution is paid.
 *
 * GET /capability            catalogue with live prices from the service book
 * GET /capability/<name>     402 with x402-v1 requirements, or the result once
 *                            a presented payment has actually been verified
 *
 * The same resources are readable by a signed-in person through /studio/v1/;
 * this is the machine-facing edge of the same capability, not a second one. */
/* Machine-facing reply: never rewritten into HTML, whatever a previous
 * request on this connection asked for. */
static void protocol_reply(int code, const char *type, const char *body);

/* One page grammar, held in one place.
 *
 * /protocols was the only projection that answered a browser with a page. The
 * capability catalogue and the A2A carrier handed a person exactly the JSON
 * they hand a crawler, which is how a priced catalogue ends up unreadable by
 * the person deciding whether to buy. The rule was already written down here
 * -- a browser gets a page organised by intent, a machine gets the same facts
 * as JSON -- and applied to one surface out of nine.
 *
 * Keeping the chrome in a macro rather than copying it is what stops the second
 * page from becoming a second design. Every page below is the same document:
 * same stylesheet, same topbar, same doors.
 *
 * Cannot infer: a page that renders a price -> an offer to the reader looking
 * at it. The price is what the service book says today, not an agreement. */
/* The doors grammar these pages share lives in matrix-aperture.pcss under
 * body.ak-page. The page opens the one shell: body.ak-app and the estate bar;
 * protocol_reply closes it with the one footer and player. */
#define BF_PAGE_CSS \
"</head><body class=\"ak-app ak-page\" data-ak-page>"

/* The navigation is authored, not compiled.
 *
 * These five links were a macro baked into every page, so renaming one or
 * adding a sixth meant editing C, rebuilding an organ and deploying it. The
 * nav is content; it belongs where content lives. site_nav rows in the CMS fill
 * the %s below, and the compiled list stays as the fallback so an unreachable
 * store degrades to the previous navigation rather than to a page with no way
 * out of it. */
#define BF_NAV_FALLBACK \
"<a href=\"/habitat/\">Habitat</a><a href=\"/cms/\">Content</a><a href=\"/notes/\">Notes</a>" \
"<a href=\"/protocols\">Build</a><a href=\"/studio/\">Studio</a>"

#define BF_PAGE_CHROME \
BF_ESTATE_BAR \
"<nav class=\"ak-local\" hidden>%s</nav>"

static const char *site_nav_html(void);
static const char *site_nav_links(void);
static const char *site_speculation_html(void);

#define BF_PAGE_OPEN \
"<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">" \
"<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">" \
"<title>"

/* Every page is also a machine discovery root.
 *
 * The institution expects foreign systems to publish machine representations it
 * can find, and published its own only on the routes that happened to emit them
 * -- so a reader who arrived at a page had no way to reach the same subject's
 * agent card, MCP descriptor, feed or federated identity. A page that is
 * discoverable only by a person is half a surface.
 *
 * These are alternate representations of this institution, not of the
 * particular page; a page-specific closure is linked separately where one
 * exists. Cannot infer: a linked representation is not an authorized one. */
#define BF_PAGE_MACHINE_MANIFEST \
"<link rel=\"alternate\" type=\"application/activity+json\" href=\"/ap/actor\">" \
"<link rel=\"alternate\" type=\"application/json\" title=\"Agent card\" href=\"/.well-known/agent.json\">" \
"<link rel=\"alternate\" type=\"application/json\" title=\"MCP resources\" href=\"/mcp/resources/\">" \
"<link rel=\"alternate\" type=\"application/json\" title=\"A2A resources\" href=\"/a2a/resources/\">" \
"<link rel=\"alternate\" type=\"application/json\" title=\"Machine discovery\" href=\"/.well-known/bonfyre\">" \
"<link rel=\"alternate\" type=\"application/ld+json\" title=\"RO-Crate\" href=\"/ro-crate/ro-crate-metadata.json\">" \
"<link rel=\"alternate\" type=\"application/vnd.zarr+json\" title=\"Zarr\" href=\"/zarr/zarr.json\">" \
"<link rel=\"alternate\" type=\"application/json\" title=\"Surface catalogue\" href=\"/_surface/\">" \
"<link rel=\"alternate\" type=\"application/json\" title=\"Capabilities\" href=\"/capability\">" \
"<link rel=\"lrdd\" template=\"/.well-known/webfinger?resource={uri}\">"

#define BF_PAGE_AFTER_TITLE \
"</title>" \
/* The runtime every fragment on the page waits on. Discovered here rather than
 * at the end of body, so it is fetched beside the stylesheets instead of after
 * the parser reaches the bottom of the document. */ \
"<link rel=\"preload\" as=\"script\" fetchpriority=\"high\" href=\"/assets/matrix-htmx.js\">" \
"<link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=Literata:opsz,wght@7..72,400;7..72,600&amp;display=swap\">" \
"<link rel=\"stylesheet\" href=\"/assets/matrix.css\">" \
"<link rel=\"stylesheet\" href=\"/assets/matrix-aperture.css\">" \
BF_PAGE_MACHINE_MANIFEST \
BF_PAGE_CSS BF_PAGE_CHROME

/* The operator may publish an existing inquiry form beside discovery. It is
 * a human-reviewed request route, never a price or a payment alternative. */
static const char *capability_inquiry_url(void) {
    const char *url = getenv("BONFYRE_CAPABILITY_INQUIRY_URL");
    if (!url || strncmp(url, "https://", 8) || !public_http_url(url) ||
        strpbrk(url, " @\\\"<>\\\\")) return "";
    return url;
}

/* The catalogue a person reads: the Stitch tool-definition studio layout on
 * the one shell. Left, each capability as its definition with the live price;
 * right, the same catalogue exactly as a machine is handed it. Both read
 * x402_price_usd, so the two can never disagree about a number. */
static void capability_catalogue_page(const char *inquiry) {
    int priced_n = 0;
    for (int i = 0; i < X402_CATALOG_N; i++) { double usd = 0.0; if (x402_price_usd(X402_CATALOG[i].name, &usd) && usd > 0.0) priced_n++; }
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\n"
           "X-Content-Type-Options: nosniff\r\nReferrer-Policy: strict-origin-when-cross-origin\r\nConnection: close\r\n\r\n");
    ak_app_open("Capabilities", "workbench", "route-capability");
    fputs("<header class=\"ak-wb-hero\"><div><p class=\"ak-kicker\">Aurekai <span>/</span> Capabilities <span>/</span> x402 resources</p>"
          "<h1>Capabilities you can call</h1><p class=\"ak-lede\">Each price is read live from the finance service book, and the "
          "agent-to-agent carrier quotes the same number. Looking is free: nobody can decide to buy what they cannot see first.</p></div>"
          "<div class=\"ak-actions\">", stdout);
    if (inquiry && *inquiry) {
        fputs("<a class=\"ak-btn is-primary\" href=\"", stdout); html(inquiry);
        fputs("\">Discuss your workflow</a><p class=\"cap-note\">Describe the result you need. Scope and pricing are agreed after review.</p>", stdout);
    }
    fputs("<a class=\"ak-btn\" href=\"/a2a\">Agent-to-agent carrier</a></div></header>", stdout);
    printf("<dl class=\"ak-stats is-row\"><div><dt>Capabilities</dt><dd>%d</dd></div><div><dt>Priced to call</dt><dd class=\"is-on\">%d</dd>"
           "<small>%s</small></div>"
           "<div><dt>Payment carrier</dt><dd>x402 v1</dd></div><div><dt>Settlement</dt><dd%s>%s</dd><small>%s</small></div></dl>",
           (int)X402_CATALOG_N, priced_n, priced_n < X402_CATALOG_N ? "The rest are not for sale, which is not the same as free" : "Every capability here has a price",
           x402_facilitator()[0] ? " class=\"is-on\"" : "",
           x402_facilitator()[0] ? "Facilitator set" : "Refused",
           x402_facilitator()[0] ? "Payments are verified before a result is returned" : "No facilitator is configured, so an unverified payment is never treated as settled");
    fputs("<div class=\"ak-grid is-2 cap-studio\"><section class=\"ak-panel\" aria-labelledby=\"cap-defs-h\"><header><h2 id=\"cap-defs-h\">Capability definitions</h2>"
          "<span class=\"ak-tag\">01 / catalogue</span></header><ol class=\"cap-defs\">", stdout);
    for (int i = 0; i < X402_CATALOG_N; i++) {
        double usd = 0.0; int priced = x402_price_usd(X402_CATALOG[i].name, &usd) && usd > 0.0;
        fputs("<li data-capability=\"", stdout); html(X402_CATALOG[i].name); fputs("\"><div class=\"cap-id\"><h3><code>", stdout); html(X402_CATALOG[i].name);
        fputs("</code></h3><span class=\"ak-tag\">", stdout); html(X402_CATALOG[i].binary); fputs("</span></div><p>", stdout); html(X402_CATALOG[i].summary);
        if (priced) printf("</p><p class=\"cap-price\">$%.*f <small>USD / call</small></p>", usd < 0.01 ? 4 : 2, usd);
        else fputs("</p><p class=\"cap-price is-unpriced\">Unpriced <small>not for sale</small></p>", stdout);
        fputs("<nav class=\"cap-ways\"><a class=\"ak-chip\" href=\"/capability/", stdout); html(X402_CATALOG[i].name);
        fputs("\">x402 resource</a><a class=\"ak-chip\" href=\"/a2a\">Over A2A</a></nav></li>", stdout);
    }
    fputs("</ol></section><div class=\"cap-side\"><section class=\"ak-panel\" aria-labelledby=\"cap-json-h\"><header><h2 id=\"cap-json-h\">As a machine reads it</h2>"
          "<span class=\"ak-tag is-on\">x402Version 1</span></header><pre class=\"ak-console cap-json\">{\n  \"x402Version\": 1,\n  \"capabilities\": [", stdout);
    for (int i = 0; i < X402_CATALOG_N; i++) {
        double usd = 0.0; int priced = x402_price_usd(X402_CATALOG[i].name, &usd);
        fputs(i ? ",\n    { \"name\": \"" : "\n    { \"name\": \"", stdout); html(X402_CATALOG[i].name);
        fputs("\", \"resource\": \"/capability/", stdout); html(X402_CATALOG[i].name);
        printf("\", \"priced\": %s, \"priceUsd\": %.2f }", priced ? "true" : "false", usd);
    }
    fputs("\n  ]\n}</pre><p class=\"cap-note\">Ask <code>/capability</code> for <code>application/json</code> to receive this catalogue with each description.</p></section>"
          "<section class=\"ak-panel\" aria-labelledby=\"cap-carriers-h\"><header><h2 id=\"cap-carriers-h\">Where it is offered</h2><span class=\"ak-tag\">02 / carriers</span></header>"
          "<ul class=\"ak-rows\"><li><strong>x402 resources</strong><small>/capability/&lt;name&gt; answers 402 with its requirements</small><a class=\"ak-go\" href=\"/capability/", stdout); html(X402_CATALOG[0].name);
    fputs("\">Open one</a></li>"
          "<li><strong>Agent-to-agent</strong><small>The same skills and the same prices over A2A</small><a class=\"ak-go\" href=\"/a2a\">Open</a></li>"
          "<li><strong>Agent card</strong><small>/.well-known/agent.json</small><a class=\"ak-go\" href=\"/.well-known/agent.json\">Read</a></li></ul></section></div></div>", stdout);
    ak_app_close();
    fflush(stdout);
}

static int serve_capability(const char *method, const char *target,
                            const char *xpayment, const char *host,
                            const char *accept) {
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    static char out[262144];
    char name[128];

    if (strncmp(target, "/capability", 11)) return 0;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    if (strcmp(method, "GET")) {
        protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n");
        return 1;
    }

    /* Catalogue. Free on purpose: an agent cannot decide to buy what it cannot
     * first see, and the price is part of the description. */
    if (!strcmp(target, "/capability") || !strcmp(target, "/capability/")) {
        static char body[49152];
        const char *inquiry = capability_inquiry_url();
        /* A catalogue a person cannot read is not a catalogue. The price is the
         * same number in both carriers because both call x402_price_usd. */
        if (wants_html(accept)) { capability_catalogue_page(inquiry); return 1; }
        int w = snprintf(body, sizeof(body),
            "{\"x402Version\":1,\"seller\":\"%s\",\"capabilities\":[",
            host ? host : "aurekai.life");
        for (int i = 0; i < X402_CATALOG_N; i++) {
            double usd = 0.0;
            int priced = x402_price_usd(X402_CATALOG[i].name, &usd);
            w += snprintf(body + w, sizeof(body) - (size_t)w,
                "%s{\"name\":\"%s\",\"resource\":\"/capability/%s\","
                "\"description\":\"%s\",\"priced\":%s,\"priceUsd\":%.6f}",
                i ? "," : "", X402_CATALOG[i].name, X402_CATALOG[i].name,
                X402_CATALOG[i].summary, priced ? "true" : "false", usd);
            if ((size_t)w >= sizeof(body) - 256) break;
        }
        if (*inquiry) {
            char escaped[6144]; json_escape_into(inquiry, escaped, sizeof(escaped));
            w += snprintf(body + w, sizeof(body) - (size_t)w,
                "],\"inquiry\":{\"url\":\"%s\",\"method\":\"human_reviewed_web_form\","
                "\"purpose\":\"Discuss scope and pricing\",\"grants_execution\":false},\"note\":"
                "\"An inquiry is not a quote, order or payment. Unpriced capabilities remain unavailable for purchase.\"}\n", escaped);
            if (w < 0 || (size_t)w >= sizeof(body)) { protocol_reply(503, "application/json", "{\"error\":\"catalogue_overflow\"}\n"); return 1; }
        } else snprintf(body + w, sizeof(body) - (size_t)w,
            "],\"note\":\"an unpriced capability is not for sale, which is not the same as free\"}\n");
        protocol_reply(200, "application/json; charset=utf-8", body);
        return 1;
    }

    if (strncmp(target, "/capability/", 12)) return 0;
    if (!studio_path_tail(target, "/capability/", name, sizeof(name)) || !studio_safe_ident(name)) {
        protocol_reply(503, "application/json", "{\"error\":\"invalid_capability\"}\n");
        return 1;
    }
    {
        const X402Capability *cap = x402_find(name);
        double usd = 0.0;
        if (!cap) {
            protocol_reply(404, "application/json",
                "{\"error\":\"unknown_capability\",\"catalogue\":\"/capability\"}\n");
            return 1;
        }
        if (!x402_price_usd(cap->name, &usd) || usd <= 0.0) {
            const char *inquiry = capability_inquiry_url();
            if (*inquiry) {
                char escaped[6144], refusal[8192];
                json_escape_into(inquiry, escaped, sizeof(escaped));
                snprintf(refusal, sizeof(refusal),
                    "{\"error\":\"not_for_sale\",\"reason\":\"this capability has no sell price in the service book\","
                    "\"inquiry\":{\"url\":\"%s\",\"method\":\"human_reviewed_web_form\",\"grants_execution\":false}}\n", escaped);
                protocol_reply(503, "application/json", refusal);
                return 1;
            }
            protocol_reply(503, "application/json",
                "{\"error\":\"not_for_sale\",\"reason\":\"this capability has no sell price in the service book\","
                "\"fix\":\"POST /studio/v1/economy/offer to price it\"}\n");
            return 1;
        }
        if (!xpayment || !*xpayment) { x402_require_payment(host, cap, usd); return 1; }
        if (!x402_facilitator()[0]) {
            /* A payment was presented and cannot be verified. Refusing is the
             * only honest answer: serving here would mean treating an
             * unverified claim of payment as settled revenue. */
            static char body[1024];
            snprintf(body, sizeof(body),
                "{\"x402Version\":1,\"error\":\"payment_unverifiable\","
                "\"reason\":\"an X-PAYMENT header was presented but no facilitator is configured, "
                "so this payment cannot be verified and will not be treated as settled\","
                "\"configure\":\"BONFYRE_X402_FACILITATOR\"}\n");
            printf("HTTP/1.1 402 Payment Required\r\nContent-Type: application/json\r\n"
                   "Content-Length: %zu\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
                   strlen(body), body);
            fflush(stdout);
            return 1;
        }
        /* Facilitator configured: verification would run here before execution.
         * Until that path is exercised against a real facilitator it is not
         * claimed to work, so the surface still refuses rather than guess. */
        {
            static char body[512];
            snprintf(body, sizeof(body),
                "{\"x402Version\":1,\"error\":\"verification_not_exercised\","
                "\"reason\":\"a facilitator is configured but settlement verification has not been "
                "proven against it; this endpoint will not serve paid output on an unproven path\"}\n");
            printf("HTTP/1.1 402 Payment Required\r\nContent-Type: application/json\r\n"
                   "Content-Length: %zu\r\nConnection: close\r\n\r\n%s", strlen(body), body);
            fflush(stdout);
            (void)out;
            return 1;
        }
    }
}

/* ── protocol projections ────────────────────────────────────────────────
 *
 * One semantic source, many carrier grammars. Each projection below is a
 * different protocol's view of state Bonfyre already holds -- the receipted
 * parcel estate, the WorkGraph, the service book -- not a separate copy with
 * its own truth. A protocol is a way of speaking about the Matrix; it never
 * becomes the owner of what it describes.
 *
 * Where the underlying data does not carry a field the protocol expects, the
 * field is emitted null with a stated reason. The TAD parcel roll carries
 * Shape.STArea() and Shape.STLength() but no coordinates, so OGC features have
 * null geometry: a fabricated point would be worse than an honest absence. */

static void protocol_reply(int code, const char *type, const char *body) {
    /* A page opened with BF_PAGE_OPEN closes on the one footer and player. */
    const char *close = NULL;
    if (!strncmp(type, "text/html", 9) && strstr(body, " data-ak-page>") && !strstr(body, "class=\"ak-foot\"")) {
        const char *at = strstr(body, "</body>");
        for (const char *n = at; n; n = strstr(n + 1, "</body>")) at = n;
        close = at;
    }
    size_t foot = close ? strlen(BF_ESTATE_FOOT) : 0;
    printf("HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
           code, code == 200 ? "OK" : "Service Unavailable", type, strlen(body) + foot);
    if (close) { fwrite(body, 1, (size_t)(close - body), stdout); fputs(BF_ESTATE_FOOT, stdout); fputs(close, stdout); }
    else fputs(body, stdout);
    fflush(stdout);
}

/* Run a Fleming projection and hand back raw JSON rows. */
static int protocol_rows(const char *bindir, const char *sql, char *out, size_t out_sz) {
    int rc = fleming_query(bindir, sql, out, out_sz);
    if (rc != 0) return rc;
    /* auth_call_split fills to capacity and NUL-terminates, so a result that
     * exactly reaches the cap was almost certainly cut mid-value. Serving that
     * under application/json hands the caller invalid JSON and calls it 200. */
    if (strlen(out) >= out_sz - 2) return -2;
    return 0;
}

static void protocol_page_too_large(int limit) {
    char body[512];
    snprintf(body, sizeof(body),
      "{\"error\":\"page_too_large\",\"limit\":%d,"
      "\"reason\":\"the response would exceed what this projection can return in one page, "
      "and a truncated body would be invalid JSON\","
      "\"fix\":\"request a smaller limit\"}\n", limit);
    printf("HTTP/1.1 413 Payload Too Large\r\nContent-Type: application/json\r\n"
           "Content-Length: %zu\r\nConnection: close\r\n\r\n%s", strlen(body), body);
    fflush(stdout);
}

static const char *PARCEL_COLS =
    "json_extract_string(observed,'$.Account_Nu') AS account, "
    "json_extract_string(observed,'$.Situs_Addr') AS address, "
    "json_extract_string(observed,'$.City') AS city, "
    "json_extract_string(observed,'$.ZipCode') AS postal, "
    "json_extract_string(observed,'$.Owner_Name') AS owner, "
    "TRY_CAST(json_extract_string(observed,'$.Year_Built') AS BIGINT) AS year_built, "
    "TRY_CAST(json_extract_string(observed,'$.Num_Bedroo') AS BIGINT) AS beds, "
    "TRY_CAST(json_extract_string(observed,'$.Num_Bathro') AS DOUBLE) AS baths, "
    "TRY_CAST(json_extract_string(observed,'$.Living_Are') AS DOUBLE) AS living_area, "
    "TRY_CAST(json_extract_string(observed,'$.Land_SqFt') AS DOUBLE) AS lot_sqft, "
    "TRY_CAST(json_extract_string(observed,'$.Land_Value') AS DOUBLE) AS land_value, "
    "TRY_CAST(json_extract_string(observed,'$.Total_Valu') AS DOUBLE) AS total_value ";

/* The columns a parcel row is, by name. The union orders on observation_id and
 * must not serve it: an internal key projected into a carrier payload is an
 * undeclared field, and a deployment growing one of those is how one meaning
 * becomes three grammars again. */
static const char *PARCEL_OUT =
    "account, address, city, postal, owner, year_built, beds, baths, "
    "living_area, lot_sqft, land_value, total_value, territory, source_id";

static const char *DALLAS_PARCEL_COLS =
    "json_extract_string(observed,'$.GIS_ACCT') AS account, "
    "trim(coalesce(json_extract_string(observed,'$.ST_NUM'),'') || ' ' || "
         "coalesce(json_extract_string(observed,'$.ST_NAME'),'')) AS address, "
    "json_extract_string(observed,'$.CITY') AS city, "
    "NULL::VARCHAR AS postal, "
    "json_extract_string(observed,'$.TAXPANAME1') AS owner, "
    "NULL::BIGINT AS year_built, "
    "NULL::BIGINT AS beds, "
    "NULL::DOUBLE AS baths, "
    "NULL::DOUBLE AS living_area, "
    "TRY_CAST(json_extract_string(observed,'$.AREA_FEET') AS DOUBLE) AS lot_sqft, "
    "NULL::DOUBLE AS land_value, "
    "NULL::DOUBLE AS total_value ";

static const char *protocol_territory(const char *query, char *out, size_t out_sz) {
    if (!query_value(query, "territory", out, out_sz) || !out[0]) snprintf(out, out_sz, "all");
    if (!strcmp(out, "tarrant") || !strcmp(out, "dallas") || !strcmp(out, "all")) return out;
    snprintf(out, out_sz, "all");
    return out;
}

/* One projection, three dialects, and they see the same rows.
 *
 * LIMIT without ORDER BY returns whichever rows the engine reaches first, and
 * over a UNION ALL of two sources read from columnar files that is not the
 * same set twice. Observed live: RESO returned account 02145189 where OGC and
 * MISMO returned 00000051, from this projection, seconds apart. A client
 * paging any carrier could see a row twice or never, and three dialects that
 * cannot agree on the order of the same rows are not projections of one thing.
 *
 * The obvious repair is the expensive one. account is
 * json_extract_string(observed,'$.Account_Nu') -- a computed field with no
 * index -- so ordering on it parses twelve JSON fields across six and a half
 * million observations before it can pick twenty-five. Measured: twenty
 * seconds a request, and enough concurrent memory to take the host off the
 * network.
 *
 * So order on what the store already has. observation_id is the primary key of
 * the hot table and a real column of the cold Parquet, unique and present in
 * both halves of the composed view. Sorting on it is a scan of one column that
 * an engine answers with a bounded heap; the JSON is then extracted for the
 * surviving page rather than for the whole estate. Deterministic, and the
 * extraction cost falls from every row to the page.
 */
static int parcel_projection_sql(char *sql, size_t sql_sz, const char *territory, int limit) {
    int n;
    if (!strcmp(territory, "tarrant")) {
        n = snprintf(sql, sql_sz,
          "SELECT %s, 'tarrant' AS territory, 'src:tad-parcel-view' AS source_id FROM ("
            "SELECT observed FROM observation WHERE src_id='src:tad-parcel-view' "
            "ORDER BY observation_id LIMIT %d)",
          PARCEL_COLS, limit);
    } else if (!strcmp(territory, "dallas")) {
        n = snprintf(sql, sql_sz,
          "SELECT %s, 'dallas' AS territory, 'src:dallas-tax-parcels' AS source_id FROM ("
            "SELECT observed FROM observation WHERE src_id='src:dallas-tax-parcels' "
            "AND json_extract_string(observed,'$.COUNTY')='DALLAS COUNTY' "
            "ORDER BY observation_id LIMIT %d)",
          DALLAS_PARCEL_COLS, limit);
    } else {
        /* Each source contributes its own ordered page, and the union of two
         * pages is small enough to order exactly. */
        n = snprintf(sql, sql_sz,
          "SELECT %s FROM ("
            "(SELECT %s, 'tarrant' AS territory, 'src:tad-parcel-view' AS source_id, "
             "observation_id FROM ("
               "SELECT observed, observation_id FROM observation "
               "WHERE src_id='src:tad-parcel-view' ORDER BY observation_id LIMIT %d)) "
            "UNION ALL "
            "(SELECT %s, 'dallas' AS territory, 'src:dallas-tax-parcels' AS source_id, "
             "observation_id FROM ("
               "SELECT observed, observation_id FROM observation "
               "WHERE src_id='src:dallas-tax-parcels' "
               "AND json_extract_string(observed,'$.COUNTY')='DALLAS COUNTY' "
               "ORDER BY observation_id LIMIT %d))"
          ") ORDER BY territory, observation_id LIMIT %d",
          PARCEL_OUT, PARCEL_COLS, limit, DALLAS_PARCEL_COLS, limit, limit);
    }
    return n < 0 || (size_t)n >= sql_sz ? -1 : 0;
}



static int protocol_limit(const char *query) {
    char raw[32];
    int n = 10;
    /* `target` never carries the query string -- the request loop separates it
     * -- so reading it there silently pinned every projection to the default
     * and no caller could page a 6.5M-row estate. */
    if (query_value(query, "limit", raw, sizeof(raw))) n = atoi(raw);
    if (n < 1) n = 1;
    if (n > 1000) n = 1000;  /* a projection is a page, not a bulk export */
    return n;
}

/* Render the protocol surface for whoever is asking.
 *
 * These endpoints were JSON to everyone, which made them useless to the person
 * who arrives wanting to bring in a source, sell a capability, publish, or wire
 * an agent -- they could not even find the door. A browser gets a page
 * organised by intent; a machine gets the same facts as JSON. */
/* /tonight -- one demo world, rendered by whichever territory asked.
 *
 * The same path on four hosts. aurekai.life answers with what a person saved,
 * aurekai.live with what is happening and where the lit exits are, aurekai.shop
 * with what it costs, aurekai.work with how it is operated and only to an
 * admitted session. The rows behind all four are the same five CMS subjects.
 *
 * This is the demonstration the surface patterns exist for: a reader can open
 * two territories side by side and see one institution described two lawful
 * ways, rather than two sites that happen to share a name. Serving it from four
 * hosts on one path is deliberate -- a separate nightgrid.aurekai.live would
 * have needed its own certificate and would have proved the opposite thing,
 * that the demo is a site rather than a projection.
 *
 * Cannot infer: a territory rendering these subjects -> that territory owning
 * them. The subjects belong to a DemoInstitution in world DEMO; each territory
 * is a view of it, and none is the estate. */
typedef struct {
    const char *territory;   /* as territory_of() reports it */
    const char *pattern;     /* the surface pattern this territory projects */
    const char *eyebrow;
    const char *heading;
    const char *lede;
    const char *fallback;    /* rendered when the store is unreachable */
} NightProjection;

static const NightProjection NIGHT_PROJECTIONS[] = {
    {"life", "night-life", "Deep Ellum Night Grid / yours",
     "Your night, <em>kept.</em>",
     "What you saved, who else is going, and what you have seen before. This is the "
     "personal projection: the same subjects the public page shows, filtered to what "
     "is yours to remember.",
     "<dl class=\"surface-record\"><dt>name</dt><dd>The Foundry Room</dd>"
     "<dt>personal_note</dt><dd>You saved this room after the March showcase.</dd></dl>"},
    {"live", "night-live", "Deep Ellum Night Grid / tonight",
     "Tonight, <em>as it stands.</em>",
     "Set times, how full each room is, and where the lit exits are. Public, live, and "
     "the same subjects the personal and market pages read -- described for someone "
     "deciding where to go in the next hour.",
     "<dl class=\"surface-record\"><dt>name</dt><dd>The Foundry Room</dd>"
     "<dt>tonight_status</dt><dd>Doors 8pm, second set 11pm</dd></dl>"},
    {"shop", "night-shop", "Deep Ellum Night Grid / market",
     "What tonight <em>costs.</em>",
     "Offers, covers and wristbands. The market projection of the same rooms and acts "
     "the live page is describing right now.",
     "<dl class=\"surface-record\"><dt>name</dt><dd>The Foundry Room</dd>"
     "<dt>offer_note</dt><dd>Two-for-one before 9pm</dd></dl>"},
    {"work", "night-work", "Deep Ellum Night Grid / operations",
     "How the night is <em>run.</em>",
     "Curfews, load-in, marshals and reconciliation. This projection requires an "
     "admitted institutional role: a valid session is not an entitlement, and a "
     "read-role member of the institution is refused it.",
     "<p class=\"surface-denied\">This projection needs an admitted role in the "
     "institution. Sign in with a granted account.</p>"}
};

/* Action apertures -- the bridge between what may exist and what is addressed.
 *
 * Three different questions get confused constantly:
 *
 *   DEFINE    what kinds of thing may exist here?      ObjectContract
 *   DECLARE   how does one enter the world?            an action aperture
 *   ADDRESS   once it exists, how is it operated on?   AddressPlane
 *
 * The middle one has no home unless apertures are computed. Friday's event is
 * imaginary, so nothing can be addressed at it; what genuinely exists is the
 * institution, its events lens, and the Event grammar admitted for it. The
 * operation exists before the instance does, which is why this resolves from a
 * parent closure rather than from the subject someone wants to create.
 *
 * An aperture is offered when three things agree: the contract applies to this
 * kind of subject, the subject's capability admits it, and the actor's
 * authority satisfies the contract's requirement. A contract the actor cannot
 * act on is omitted rather than shown disabled -- an aperture list is a
 * statement about what this actor may do, not a menu with locked entries.
 *
 * Cannot infer: an offered aperture -> a completed effect. The aperture says
 * the grammar applies and the actor is admitted; the native owner still owns
 * what happens, and a DEMO institution still cannot reach an external rail. */
/* AtomicForm -- a partial evaluator for a semantic operation.
 *
 * A schema-to-form generator asks the person for every field the object has.
 * That is why enterprise forms are long: they re-ask what the system already
 * knows. An AtomicForm starts from the operation and the closure it is being
 * invoked in, resolves everything the institution can lawfully close, and
 * renders only what is genuinely an unresolved human choice.
 *
 * Declaring an Event inside institution:nightgrid, the closure already answers
 * institution, territory, timezone, organizer and initial state. What is left
 * is title, start, end, venue, description. Five fields instead of ten, and the
 * five that remain are the ones a person actually has an opinion about.
 *
 * The closed fields are still shown, as resolved context rather than inputs.
 * Hiding them entirely would make the form feel magical and unauditable; the
 * person should be able to see that the system decided the organizer is them.
 *
 * Cannot infer: a rendered form -> an admitted submission. This renders what
 * would be asked. The operation is still gated at submit by Authority, by the
 * contract's birth policy, and by the native owner of the effect. */
static void form_field_list(const char *csv, int as_input) {
    const char *p = csv;
    while (p && *p) {
        char field[64];
        size_t n = 0;
        while (*p == ' ' || *p == ',') p++;
        while (*p && *p != ',' && n + 1 < sizeof(field)) field[n++] = *p++;
        while (n && field[n-1] == ' ') n--;
        field[n] = '\0';
        if (!n) continue;
        if (as_input) {
            /* A field named password renders as one. Rendering it as text
             * would put a credential on screen and into autofill history for
             * the sake of a generic loop. */
            {
                int secret = !strcmp(field, "password");
                printf("<label class=\"atomic-field\"><span>");
                html(field);
                printf("</span><input name=\"");
                html(field);
                printf("\" type=\"%s\" autocomplete=\"%s\"></label>",
                       secret ? "password" : "text",
                       secret ? "current-password" : "off");
            }
        } else {
            printf("<li><code>");
            html(field);
            printf("</code></li>");
        }
    }
}

/* The birth transaction -- where a semantic operation becomes institutional state.
 *
 * Order matters and is not stylistic:
 *
 *   authority        the actor must be admitted for this contract, at submit
 *                    and not merely when the form was rendered
 *   birth policy     the contract's declared mode decides what may happen at
 *                    all, before any identity or row is considered
 *   identity         the Matrix allocates the subject, so the subject exists
 *                    before anything claims to describe it
 *   authored body    CMS writes what the person actually wrote, bound to that
 *                    subject id
 *   receipt          what happened, recorded as its own fact
 *
 * Writing the body first and allocating identity second would leave an
 * orphaned row describing nothing whenever allocation failed.
 *
 * Birth mode is enforced here rather than described. A DERIVE operation
 * submitted to this endpoint creates nothing and says so: its result is
 * computed when addressed, and storing it would turn a derivation into a stale
 * fact that outlives its inputs. An INVOKE creates no CMS row either -- Finance
 * and the Ledger own settlement state, and a settlement row written here would
 * be a second copy of a truth this organ does not own.
 *
 * Cannot infer: a completed birth -> a published, occurred, or authorized-
 * onward thing. The Event exists in its declared initial state and nothing
 * more. */
static int serve_aperture_submit(const char *method, const char *target,
                                 const char *body_in, const char *accept,
                                 const char *bearer, const char *auth_db) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char subject[256], operation[128], kind[64];
    char mode[24], req[64], state[32], body_type[64], alloc[32];
    char human[512], object_name[64], lens_path[64], slug_field[64], applies_kind[64];
    char title[512];
    /* start/end/venue/description are no longer named here: the contract says
     * which fields a person supplies, and the body is assembled from that. */
    char sid[160], digest[65], stamp[32], receipt[128];

    char slug[160], address[256];
    int presence_bound = 0, receipt_written = 0, relation_bound = 0, delta_written = 0;
    const char *colon;
    int found = 0;

    if (strcmp(target, "/_aperture/submit")) return 0;
    if (strcmp(method, "POST")) return 0;
    (void)accept;

    if (!query_value(body_in, "subject", subject, sizeof(subject)) || !*subject ||
        !query_value(body_in, "operation", operation, sizeof(operation)) || !*operation) {
        protocol_reply(400, "application/json",
            "{\"error\":\"subject_and_operation_required\"}\n");
        return 1;
    }
    colon = strchr(subject, ':');
    {
        size_t n = colon ? (size_t)(colon - subject) : strlen(subject);
        if (n == 0 || n >= sizeof(kind)) {
            protocol_reply(400, "application/json", "{\"error\":\"unusable_subject_kind\"}\n");
            return 1;
        }
        memcpy(kind, subject, n); kind[n] = '\0';
    }
    if (!studio_safe_ident(kind)) {
        protocol_reply(400, "application/json", "{\"error\":\"unsafe_subject_kind\"}\n");
        return 1;
    }
    /* A foreign twin resolves against what it was classified as, so a property
     * source and a specification are offered different things. */
    if (!strcmp(kind, "foreign_twin")) {
        char classified[64];
        if (twin_classified_kind(subject, classified, sizeof(classified)))
            snprintf(kind, sizeof(kind), "%s", classified);
    }

    db = cms_open_ro();
    if (!db || !cms_type_registered(db, "object_contract")) {
        if (db) sqlite3_close(db);
        protocol_reply(503, "application/json", "{\"error\":\"no_object_grammar\"}\n");
        return 1;
    }
    if (sqlite3_prepare_v2(db,
            "SELECT birth_mode, COALESCE(requires_authority,''), COALESCE(initial_state,''), "
            "       COALESCE(identity_allocation,''), COALESCE(body_content_type,''), "
            "       COALESCE(human_fields,''), object_name, collection_lens, "
            "       COALESCE(slug_field,''), applies_to_kind "
            "FROM object_contract WHERE applies_to_kind = ? AND birth_operation = ? "
            "  AND status = 'published' LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, kind, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, operation, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            found = 1;
            snprintf(mode,  sizeof(mode),  "%s", (const char *)sqlite3_column_text(st, 0));
            snprintf(req,   sizeof(req),   "%s", (const char *)sqlite3_column_text(st, 1));
            snprintf(state, sizeof(state), "%s", (const char *)sqlite3_column_text(st, 2));
            snprintf(alloc, sizeof(alloc), "%s", (const char *)sqlite3_column_text(st, 3));
            snprintf(body_type, sizeof(body_type), "%s", (const char *)sqlite3_column_text(st, 4));
            snprintf(human, sizeof(human), "%s", (const char *)sqlite3_column_text(st, 5));
            snprintf(object_name, sizeof(object_name), "%s", (const char *)sqlite3_column_text(st, 6));
            snprintf(lens_path, sizeof(lens_path), "%s", (const char *)sqlite3_column_text(st, 7));
            snprintf(slug_field, sizeof(slug_field), "%s", (const char *)sqlite3_column_text(st, 8));
            snprintf(applies_kind, sizeof(applies_kind), "%s", (const char *)sqlite3_column_text(st, 9));
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    if (!found) {
        protocol_reply(404, "application/json",
            "{\"error\":\"no_such_aperture\"}\n");
        return 1;
    }

    /* Authority at submit, not at render. A form rendered an hour ago is not a
     * grant, and a revocation between the two must be honoured. */
    if (*req && !surface_authority_satisfied(req, bearer, auth_db)) {
        protocol_reply(403, "application/json",
            "{\"error\":\"aperture_authority_required\"}\n");
        return 1;
    }

    /* Birth policy, enforced before anything is created. */
    if (!strcmp(mode, "DERIVE")) {
        /* A derivation computes and returns. It does not store.
         *
         * Refusing to store was only half of it: an operation that declines to
         * write and also never computes is a refusal wearing an operation's
         * name, and the aperture that offers it is advertising something the
         * estate cannot do. The result is produced here, from the owner of the
         * observations, every time it is addressed -- which is exactly why it
         * is not written down. A stored derivation outlives its inputs; a
         * computed one cannot.
         *
         * Cannot infer: a derived result -> a fact about the world. It
         * summarises what this institution observed, and inherits every limit
         * of those observations. */
        const char *bindir = getenv("BONFYRE_BIN_DIR");
        char qbin[PATH_MAX], sql[1024], out[65536], src[256];
        const char *parquet = getenv("BONFYRE_OBSERVATION_PARQUET");
        if (!bindir || !*bindir) bindir = "/usr/local/bin";
        if (!parquet || !*parquet) parquet = "/var/lib/bonfyre/state/fleming/estate.parquet";
        snprintf(qbin, sizeof(qbin), "%s/bonfyre-query", bindir);

        /* The addressed subject names which source to summarise. A twin
         * registered as foreign_twin:src-dallas-tax-parcels was observed as
         * src:dallas-tax-parcels; the id is the slug, so the source is
         * recovered from it rather than stored twice. */
        if (!strncmp(subject, "foreign_twin:src-", 17))
            snprintf(src, sizeof(src), "src:%.200s", subject + 17);
        else
            snprintf(src, sizeof(src), "%s", "");

        if (!*src || !studio_safe_ident(src + 4)) {
            protocol_reply(422, "application/json",
                "{\"error\":\"nothing_to_derive_from\",\"birth_mode\":\"DERIVE\","
                "\"note\":\"this operation summarises an observation corpus and the "
                "addressed subject names none\"}\n");
            return 1;
        }
        snprintf(sql, sizeof(sql),
            "SELECT COUNT(*) AS observations, COUNT(DISTINCT page_sha256) AS pages, "
            "COUNT(DISTINCT candidate_id) AS candidates, MIN(observed_at) AS first_seen, "
            "MAX(observed_at) AS last_seen FROM read_parquet('%s') WHERE src_id='%s'",
            parquet, src);
        out[0] = '\0';
        {
            char *argv[] = { qbin, (char*)"sql", (char*)"/tmp/derive.duckdb", sql, NULL };
            if (auth_call(argv, NULL, out, sizeof(out)) != 0 || !out[0]) {
                protocol_reply(503, "application/json",
                    "{\"error\":\"derivation_owner_unavailable\"}\n");
                return 1;
            }
        }
        {
            /* The owner prefixes progress lines, and one of them opens with a
             * bracket -- "[query] Running SQL" -- so taking the first '[' in
             * the stream captured the log instead of the answer. The result is
             * the last line that begins a JSON array. */
            const char *body = NULL, *scan = out;
            while (scan && *scan) {
                if (*scan == '[' && (scan == out || scan[-1] == '\n') &&
                    (scan[1] == '{' || scan[1] == ']'))
                    body = scan;
                scan = strchr(scan, '\n');
                if (scan) scan++;
            }
            printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                   "Cache-Control: no-store\r\nConnection: close\r\n\r\n"
                   "{\"derived\":true,\"birth_mode\":\"DERIVE\",\"operation\":\"%s\","
                   "\"subject\":\"%s\",\"source\":\"%s\",\"stored\":false,"
                   "\"result\":%s,"
                   "\"forbidden_inference\":\"a derived result is not a fact about the "
                   "world; it summarises what this institution observed and inherits every "
                   "limit of those observations. It is not stored, because a stored "
                   "derivation outlives its inputs\"}\n",
                   operation, subject, src, body && *body ? body : "null");
            fflush(stdout);
        }
        return 1;
    }
    if (!strcmp(mode, "INVOKE")) {
        protocol_reply(409, "application/json",
            "{\"error\":\"invoke_is_owned_elsewhere\",\"birth_mode\":\"INVOKE\","
            "\"note\":\"this operation performs an effect owned by another organ; no record "
            "is created here, and a row written here would be a second copy of a truth this "
            "organ does not own\"}\n");
        return 1;
    }
    if (strcmp(mode, "DECLARE") && strcmp(mode, "PROPOSE") && strcmp(mode, "ATTACH")) {
        protocol_reply(501, "application/json",
            "{\"error\":\"birth_mode_not_wired\"}\n");
        return 1;
    }

    /* ATTACH binds authority to something that already exists.
     *
     * The whole point is that no second object is created: claiming a merchant
     * that a ForeignTwin already represents must not produce a rival merchant
     * record, or the institution ends up holding two of one thing and has to
     * decide later which is real.
     *
     * So the target subject is required and must already be a subject. Creating
     * it here would be a DECLARE wearing an ATTACH's name, and the caller would
     * have no way to tell the difference. */
    if (!strcmp(mode, "ATTACH")) {
        char target[256];
        const char *mdb = getenv("BONFYRE_MATRIX_DB");
        BfMatrix *m = NULL;
        int present_now = 0;
        if (!query_value(body_in, "subject_ref", target, sizeof(target)) || !*target) {
            /* When the contract applies to the same kind as the subject being
             * addressed, the thing being attached to is the thing you are
             * looking at. Claiming a discovered business from that business's
             * own surface should not require naming it again -- the aperture
             * was resolved on it. */
            if (!strcmp(kind, applies_kind))
                snprintf(target, sizeof(target), "%s", subject);
            else {
                protocol_reply(400, "application/json",
                    "{\"error\":\"subject_ref_required\",\"birth_mode\":\"ATTACH\","
                    "\"note\":\"an attachment binds an existing subject; name the one being "
                    "claimed, or invoke this from that subject's own context\"}\n");
                return 1;
            }
        }
        if (!mdb || !*mdb || bf_matrix_open(mdb, &m, NULL) || !m) {
            protocol_reply(503, "application/json", "{\"error\":\"identity_unavailable\"}\n");
            return 1;
        }
        present_now = bf_matrix_subject_exists(m, target) == 1;
        if (!present_now) {
            bf_matrix_close(m);
            protocol_reply(409, "application/json",
                "{\"error\":\"subject_does_not_exist\",\"birth_mode\":\"ATTACH\","
                "\"note\":\"an attachment does not create the thing it attaches to; observe "
                "or declare the subject first, then claim it\"}\n");
            return 1;
        }
        iso_utc_now(stamp, sizeof(stamp));
        {
            char seed[1024];
            snprintf(seed, sizeof(seed), "%s|%s|%s|%s", subject, operation, target, stamp);
            bf_sha256_hex((const uint8_t *)seed, strlen(seed), digest);
        }
        snprintf(receipt, sizeof(receipt), "aperture:%.80s:%.16s", operation, digest);
        /* The claim is an edge, not an object: claimant --claims--> subject. */
        {
            /* claimed_by points at whoever is claiming. When the aperture was
             * resolved on the twin itself, the claimant must be named or the
             * edge would say the thing claims itself. */
            char claimant[256];
            if (!query_value(body_in, "claimant", claimant, sizeof(claimant)) || !*claimant)
                snprintf(claimant, sizeof(claimant), "%s",
                         strcmp(kind, applies_kind) ? subject : "institution:unnamed");
            if (strcmp(claimant, target) &&
                bf_matrix_subject_relate(m, target, "claimed_by", claimant, receipt) == BF_MATRIX_OK)
                relation_bound = 1;
            snprintf(subject, sizeof(subject), "%s", claimant);
        }
        bf_matrix_close(m);

        if (*body_type && studio_safe_ident(body_type)) {
            const char *bindir = getenv("BONFYRE_BIN_DIR");
            char cms[PATH_MAX], json[8192], out[8192], e_t[512];
            size_t n;
            if (!bindir || !*bindir) bindir = "/usr/local/bin";
            snprintf(cms, sizeof(cms), "%s/bonfyre-cms", bindir);
            json_escape_into(target, e_t, sizeof(e_t));
            n = (size_t)snprintf(json, sizeof(json),
                "{\"subject_id\":\"%s\",\"institution\":\"%s\",\"declared_by\":\"aperture\","
                "\"lifecycle_state\":\"%s\",\"receipt\":\"%s\"",
                e_t, subject, state[0] ? state : "asserted", receipt);
            {
                const char *f = human;
                while (*f && n < sizeof(json) - 512) {
                    char field[64], raw[2048], esc[2048];
                    size_t k = 0;
                    while (*f == ' ' || *f == ',') f++;
                    while (*f && *f != ',' && k + 1 < sizeof(field)) field[k++] = *f++;
                    while (k && field[k-1] == ' ') k--;
                    field[k] = '\0';
                    if (!k || !studio_safe_ident(field)) continue;
                    if (!query_value(body_in, field, raw, sizeof(raw))) continue;
                    json_escape_into(raw, esc, sizeof(esc));
                    n += (size_t)snprintf(json + n, sizeof(json) - n, ",\"%s\":\"%.1500s\"", field, esc);
                }
            }
            snprintf(json + n, sizeof(json) - n, "}");
            {
                char *argv[] = { cms, (char*)"entry", (char*)"create", body_type, json, NULL };
                (void)auth_call(argv, NULL, out, sizeof(out));
            }
        }

        printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
               "Cache-Control: no-store\r\nConnection: close\r\n\r\n"
               "{\"attached\":true,\"birth_mode\":\"ATTACH\",\"operation\":\"%s\","
               "\"subject_id\":\"%s\",\"claimed_by\":\"%s\",\"relation_bound\":%s,"
               "\"identity_allocated\":false,\"receipt\":\"%s\","
               "\"forbidden_inference\":\"an attachment binds authority to a subject that "
               "already existed; no second object was created, and the claim is an assertion "
               "rather than a verified fact about the world\"}\n",
               operation, target, subject, relation_bound ? "true" : "false", receipt);
        fflush(stdout);
        return 1;
    }

    /* The body type comes from the contract, not from a comparison against an
     * operation name compiled in here. Wiring a new object is then an authoring
     * act: declare the contract, name its body type, and births work without
     * touching this organ. A contract with no body type is refused rather than
     * allocating an identity that nothing describes. */
    if (!*body_type || !studio_safe_ident(body_type)) {
        protocol_reply(501, "application/json",
            "{\"error\":\"no_body_binding\",\"note\":\"this contract has no authored body "
            "type wired yet; allocating a subject nothing describes would be worse than "
            "refusing\"}\n");
        return 1;
    }

    /* query_value decodes a form-urlencoded body directly -- same shape as a
     * query string. It refuses control characters, so a multi-line description
     * is rejected rather than silently mangled; that is the right direction for
     * now and is a real limit on this field. */
    /* A title is required whenever the contract asks for one; a grammar that
     * does not ask for a title is not forced to have one. */
    if (strstr(human, "title") && (!query_value(body_in, "title", title, sizeof(title)) || !*title)) {
        protocol_reply(400, "application/json",
            "{\"error\":\"title_required\",\"note\":\"the AtomicForm lists what a person must "
            "supply; title is one of them\"}\n");
        return 1;
    }

    iso_utc_now(stamp, sizeof(stamp));
    {
        char seed[3072];
        snprintf(seed, sizeof(seed), "%s|%s|%s|%s", subject, operation, title, stamp);
        bf_sha256_hex((const uint8_t *)seed, strlen(seed), digest);
    }
    {
        /* The identity prefix follows the object the contract declares, so a
         * BudgetRequest is not born as an event: */
        char lower[64];
        size_t li = 0;
        for (; object_name[li] && li + 1 < sizeof(lower); li++)
            lower[li] = (char)tolower((unsigned char)object_name[li]);
        lower[li] = '\0';
        snprintf(sid, sizeof(sid), "%s:%.32s", lower[0] ? lower : "object", digest);
    }
    /* The operation is bounded here: a receipt that silently lost its digest
     * tail would still look like a receipt. */
    snprintf(receipt, sizeof(receipt), "aperture:%.80s:%.16s", operation, digest);

    /* Identity first: the Matrix allocates the subject, so nothing describes a
     * subject that does not exist.
     *
     * A readable address is bound in the same step, and it is deliberately not
     * the identity. The slug is derived from a title a person can change; the
     * subject id is a digest that never does. Binding the address here means a
     * born thing is reachable immediately, and re-titling it later can move the
     * route without disturbing anything that referenced the subject. */
    {
        const char *mdb = getenv("BONFYRE_MATRIX_DB");
        BfMatrix *m = NULL;
        BfMatrixSubjectSpec spec = { sid, object_name, digest, receipt };
        BfMatrixStatus mst;
        if (!mdb || !*mdb || bf_matrix_open(mdb, &m, NULL) || !m) {
            protocol_reply(503, "application/json",
                "{\"error\":\"identity_unavailable\",\"note\":\"the Matrix could not be opened "
                "to allocate a subject; nothing was written\"}\n");
            return 1;
        }
        mst = bf_matrix_subject_bind(m, &spec);
        if (mst != BF_MATRIX_OK) {
            bf_matrix_close(m);
            protocol_reply(503, "application/json",
                "{\"error\":\"identity_refused\",\"note\":\"the Matrix refused the subject; "
                "no authored body was written\"}\n");
            return 1;
        }
        if (!studio_safe_ident(lens_path)) snprintf(lens_path, sizeof(lens_path), "%s", "objects");
        /* Which field names the thing is the contract's answer. A BudgetRequest
         * has no title, and defaulting every object to one produced a route of
         * /finance/budgetrequest that every budget request would share. */
        {
            char named[512];
            named[0] = '\0';
            if (*slug_field && studio_safe_ident(slug_field))
                query_value(body_in, slug_field, named, sizeof(named));
            if (!named[0] && *title) snprintf(named, sizeof(named), "%s", title);
            compose_slug(named[0] ? named : object_name, slug, sizeof(slug), "");
        }
        snprintf(address, sizeof(address), "https://nightgrid.aurekai.live/%.24s/%.96s", lens_path, slug);
        /* A route that collides with an existing one is disambiguated by the
         * identity that cannot collide, rather than by refusing the birth: two
         * events may honestly share a title. */
        {
            char taken[192];
            if (bf_matrix_address_subject(m, address, taken, sizeof(taken)) == 1 &&
                strcmp(taken, sid) != 0)
                snprintf(address, sizeof(address),
                         "https://nightgrid.aurekai.live/%.24s/%.80s-%.8s", lens_path, slug, digest);
        }
        if (bf_matrix_address_bind(m, sid, "web_url", address, receipt) != BF_MATRIX_OK)
            address[0] = '\0';   /* reported as unbound rather than claimed */

        /* A presence, without which the address resolves to nothing.
         *
         * Resolution joins address to presence: an address with no presence is
         * a row in a table nobody reads. A presence is a projection of the
         * subject into one territory, world, scope and representation -- never
         * a second copy of the subject -- so the event is born already visible
         * in the world its institution executes in, and nowhere else.
         *
         * The world comes from the institution rather than a default. A demo
         * institution's event must be a DEMO presence, or the estate would
         * project synthetic state into the live world. */
        {
            char presence_id[192];
            const char *inst = subject + (colon ? (size_t)(colon - subject) + 1 : 0);
            BfMatrixPresenceSpec pres;
            snprintf(presence_id, sizeof(presence_id), "presence:%.40s:live", sid);
            pres.presence_id = presence_id;
            pres.subject_id = sid;
            /* Only an institution can host a presence. A birth declared from a
             * foreign twin's own surface is addressed at the twin, and naming
             * that as the host asked the Matrix to accept a subject where an
             * institution belongs -- correctly refused, leaving the object born
             * and unpresent. No host is the honest answer: the address it was
             * declared from does not imply one. */
            pres.institution_id = strncmp(subject, "institution:", 12) ? NULL : subject;
            pres.territory = "live";
            pres.scope = BF_MATRIX_PUBLIC;
            pres.world = institution_is_demo(inst) ? BF_MATRIX_DEMO : BF_MATRIX_LIVE;
            pres.representation = "HTML";
            pres.evidence_ref = receipt;
            if (bf_matrix_presence_bind(m, &pres) != BF_MATRIX_OK) presence_id[0] = '\0';
            presence_bound = presence_id[0] ? 1 : 0;
        }

        /* The edge to the closure it was declared in.
         *
         * Without this the institution was known only as a string inside an
         * authored body: readable, but not traversable, and invisible to
         * anything asking what an institution contains. The relation is
         * evidence-bound to the same receipt as the subject itself. */
        if (bf_matrix_subject_relate(m, sid, "declared_in", subject, receipt) == BF_MATRIX_OK)
            relation_bound = 1;
        bf_matrix_close(m);
    }

    /* Authored body second, bound to the subject that now exists. */
    {
        const char *bindir = getenv("BONFYRE_BIN_DIR");
        char cms[PATH_MAX], json[12288], out[8192];
        size_t n;
        if (!bindir || !*bindir) bindir = "/usr/local/bin";
        snprintf(cms, sizeof(cms), "%s/bonfyre-cms", bindir);
        /* The authored body is assembled from what the contract says a person
         * supplies, plus the context every born thing carries. Naming Event's
         * fields here would have made this handler the second place that knows
         * what an Event is. */
        n = (size_t)snprintf(json, sizeof(json),
            "{\"subject_id\":\"%s\",\"institution\":\"%s\",\"declared_by\":\"aperture\","
            "\"lifecycle_state\":\"%s\",\"receipt\":\"%s\"",
            sid, subject, state[0] ? state : "draft", receipt);
        {
            const char *f = human;
            while (*f && n < sizeof(json) - 512) {
                char field[64], raw[4096], esc[4096];
                size_t k = 0;
                while (*f == ' ' || *f == ',') f++;
                while (*f && *f != ',' && k + 1 < sizeof(field)) field[k++] = *f++;
                while (k && field[k-1] == ' ') k--;
                field[k] = '\0';
                if (!k || !studio_safe_ident(field)) continue;
                if (!query_value(body_in, field, raw, sizeof(raw))) continue;
                json_escape_into(raw, esc, sizeof(esc));
                n += (size_t)snprintf(json + n, sizeof(json) - n,
                                      ",\"%s\":\"%.3500s\"", field, esc);
            }
        }
        snprintf(json + n, sizeof(json) - n, "}");
        {
            char *argv[] = { cms, (char*)"entry", (char*)"create", body_type, json, NULL };
            if (auth_call(argv, NULL, out, sizeof(out)) != 0 || !strstr(out, "\"id\"")) {
                /* The subject exists and its body does not. Say so precisely
                 * rather than reporting success: a caller that retries must
                 * know identity was already allocated. */
                printf("HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n"
                       "Cache-Control: no-store\r\nConnection: close\r\n\r\n"
                       "{\"error\":\"body_write_failed\",\"subject_id\":\"%s\","
                       "\"note\":\"the subject was allocated and its authored body was not "
                       "written; the subject exists and describes nothing\"}\n", sid);
                fflush(stdout);
                return 1;
            }
        }
    }

    /* The receipt as its own record, in the shape foreign observations already
     * use: a schema'd JSON file per event, never overwritten. The receipt id was
     * already bound as the evidence_ref of the subject, the address and the
     * presence, so this is what those three references point at rather than a
     * new claim. A birth whose receipt cannot be written still succeeded -- the
     * institutional state exists -- so this reports the failure without
     * pretending the birth did not happen. */
    {
        /* The directory is bounded separately from the filename so the compiler
         * can see the join fits; PATH_MAX for both leaves it unable to prove it. */
        char dir[512], path[PATH_MAX];
        FILE *rf;
        const char *root = getenv("BONFYRE_BIRTH_RECEIPTS");
        if (!root || !*root) root = "/var/lib/bonfyre/births/receipts";
        snprintf(dir, sizeof(dir), "%s", root);
        (void)mkdir("/var/lib/bonfyre/births", 0750);
        (void)mkdir(dir, 0750);
        snprintf(path, sizeof(path), "%.480s/%.40s-%.16s.json", dir, sid, stamp);
        for (char *z = path + strlen(dir) + 1; *z; z++)
            if (*z == ':' || *z == '/') *z = '-';
        rf = fopen(path, "wb");
        if (rf) {
            fprintf(rf,
                "{\"schema\":\"bonfyre-birth-receipt.v1\",\"receipt\":\"%s\","
                "\"born_at\":\"%s\",\"operation\":\"%s\",\"birth_mode\":\"%s\","
                "\"object\":\"%s\",\"subject_id\":\"%s\",\"declared_in\":\"%s\","
                "\"lifecycle_state\":\"%s\",\"identity_allocation\":\"%s\","
                "\"address\":\"%s\",\"address_bound\":%s,\"presence_bound\":%s,"
                "\"body_content_type\":\"%s\","
                "\"forbidden_inference\":\"a birth receipt records that institutional state "
                "was created; it is not a claim that the thing is published, occurred, or "
                "authorized onward\"}\n",
                receipt, stamp, operation, mode, object_name, sid, subject,
                state[0] ? state : "draft", alloc, address,
                address[0] ? "true" : "false", presence_bound ? "true" : "false", body_type);
            fclose(rf);
            receipt_written = 1;
        }
    }

    /* The write set, emitted as a FactDelta.
     *
     * A delta names what a transaction wrote and where it sits causally, so a
     * later derivation can recompute only what those writes touched instead of
     * everything. bf_semantic_derivation_slice consumes exactly this shape
     * against a graph of programs; no such graph is populated in this estate
     * yet, which makes this the producer half of a loop whose consumer is still
     * owed -- the same order the foreign observation receipts were built in,
     * where the records existed before anything read them.
     *
     * Recording it now is not speculative: the write set is only knowable here,
     * at the moment of the transaction, and reconstructing it afterwards from
     * three separate stores is exactly the archaeology a delta exists to
     * prevent. */
    {
        char dir[512], path[PATH_MAX];
        FILE *df;
        const char *root = getenv("BONFYRE_FACT_DELTAS");
        if (!root || !*root) root = "/var/lib/bonfyre/births/deltas";
        snprintf(dir, sizeof(dir), "%s", root);
        (void)mkdir("/var/lib/bonfyre/births", 0750);
        (void)mkdir(dir, 0750);
        snprintf(path, sizeof(path), "%.480s/%.40s-%.16s.json", dir, sid, stamp);
        for (char *z = path + strlen(dir) + 1; *z; z++)
            if (*z == ':' || *z == '/') *z = '-';
        df = fopen(path, "wb");
        if (df) {
            /* writes[] names every store this transaction touched, in the order
             * it touched them. The causal frontier is the receipt: everything
             * here happened after it and because of it. */
            fprintf(df,
                "{\"schema\":\"bonfyre-fact-delta.v1\",\"delta_id\":\"%s\","
                "\"causal_frontier\":\"%s\",\"hlc\":{\"physical\":\"%s\",\"logical\":0,"
                "\"node\":\"bonfyre-web-address\"},\"writes\":["
                "\"matrix_subjects:%s\","
                "\"matrix_addresses:%s\","
                "\"matrix_presences:presence:%s:live\","
                "\"matrix_subject_relations:%s declared_in %s\","
                "\"cms:%s\"],\"write_count\":5,"
                "\"forbidden_inference\":\"a delta names what was written and when; it is not "
                "a claim that anything has recomputed against it\"}\n",
                receipt, receipt, stamp, sid, address[0] ? address : "(unbound)",
                sid, sid, subject, body_type);
            fclose(df);
            delta_written = 1;
        }
    }

    printf("HTTP/1.1 201 Created\r\nContent-Type: application/json\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n"
           "{\"born\":true,\"birth_mode\":\"%s\",\"operation\":\"%s\",\"subject_id\":\"%s\","
           "\"lifecycle_state\":\"%s\",\"identity_allocation\":\"%s\",\"receipt\":\"%s\","
           "\"declared_in\":\"%s\",\"address\":\"%s\",\"address_bound\":%s,"
           "\"presence_bound\":%s,\"receipt_written\":%s,"
           "\"relation_bound\":%s,\"fact_delta_written\":%s,"
           "\"forbidden_inference\":\"a completed birth is not a published, occurred, or "
           "onward-authorized thing; it exists in its declared initial state and nothing more. "
           "The address is a resolution route into the subject and is not its identity: the slug "
           "follows a title a person may change, the subject id never changes\"}\n",
           mode, operation, sid, state[0] ? state : "draft", alloc, receipt, subject,
           address, address[0] ? "true" : "false",
           presence_bound ? "true" : "false",
           receipt_written ? "true" : "false",
           relation_bound ? "true" : "false",
           delta_written ? "true" : "false");
    fflush(stdout);
    return 1;
}

/* The verb of an operation, for a button a person reads. "Session.open" is a
 * contract identifier; "open" is what the button should say. */
static const char *object_name_of(const char *operation) {
    const char *dot = operation ? strrchr(operation, '.') : NULL;
    return dot && dot[1] ? dot + 1 : (operation ? operation : "submit");
}

/* An outside owner can identify the addressed subject by its local identifier
 * rather than its full Matrix address. This is authored contract metadata,
 * not a second subject lookup: project:neighborhood-garden lowers to the
 * id=neighborhood-garden the existing governance owner already owns. */
static int contract_owner_subject_value(const char *subject, const char *mode,
                                        char *out, size_t cap) {
    const char *value;
    if (!subject || !mode || !out || !cap) return 0;
    if (!strcmp(mode, "subject")) value = subject;
    else if (!strcmp(mode, "subject_identifier")) {
        value = strchr(subject, ':');
        value = value ? value + 1 : NULL;
    } else return 0;
    if (!value || !*value ||
        strspn(value, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:") != strlen(value) ||
        snprintf(out, cap, "%s", value) >= (int)cap) return 0;
    return 1;
}

/* Return routes are authored local templates. A contract cannot turn an
 * AtomicForm into an open redirect by supplying a full URL or query value. */
static int contract_owner_return_value(const char *template, const char *identifier,
                                       char *out, size_t cap) {
    static const char marker[] = "{subject_identifier}";
    const char *at;
    size_t prefix, suffix;
    if (!template || !identifier || !out || !cap || template[0] != '/' ||
        template[1] == '/' || strpbrk(template, "\r\n\\")) return 0;
    at = strstr(template, marker);
    if (!at || strstr(at + sizeof(marker) - 1, marker)) return 0;
    prefix = (size_t)(at - template);
    suffix = strlen(at + sizeof(marker) - 1);
    if (prefix + strlen(identifier) + suffix >= cap) return 0;
    memcpy(out, template, prefix);
    snprintf(out + prefix, cap - prefix, "%s%s", identifier, at + sizeof(marker) - 1);
    return 1;
}

static int serve_aperture_form(const char *method, const char *target, const char *query,
                               const char *accept, const char *bearer, const char *auth_db,
                               const char *host) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char subject[256], operation[128], kind[64];
    const char *colon;
    int as_json, found = 0;

    if (strcmp(target, "/_aperture/form")) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!query_value(query, "subject", subject, sizeof(subject)) || !*subject ||
        !query_value(query, "operation", operation, sizeof(operation)) || !*operation) {
        protocol_reply(400, "application/json",
            "{\"error\":\"subject_and_operation_required\"}\n");
        return 1;
    }
    colon = strchr(subject, ':');
    {
        size_t n = colon ? (size_t)(colon - subject) : strlen(subject);
        if (n == 0 || n >= sizeof(kind)) {
            protocol_reply(400, "application/json", "{\"error\":\"unusable_subject_kind\"}\n");
            return 1;
        }
        memcpy(kind, subject, n); kind[n] = '\0';
    }
    if (!studio_safe_ident(kind)) {
        protocol_reply(400, "application/json", "{\"error\":\"unsafe_subject_kind\"}\n");
        return 1;
    }
    /* A foreign twin resolves against what it was classified as, so a property
     * source and a specification are offered different things. */
    if (!strcmp(kind, "foreign_twin")) {
        char classified[64];
        if (twin_classified_kind(subject, classified, sizeof(classified)))
            snprintf(kind, sizeof(kind), "%s", classified);
    }

    as_json = wants_json(accept);
    db = cms_open_ro();
    if (!db || !cms_type_registered(db, "object_contract")) {
        if (db) sqlite3_close(db);
        protocol_reply(503, "application/json", "{\"error\":\"no_object_grammar\"}\n");
        return 1;
    }

    if (sqlite3_prepare_v2(db,
            "SELECT object_name, birth_mode, COALESCE(requires_authority,''), "
            "       COALESCE(initial_state,''), COALESCE(identity_allocation,''), "
            "       COALESCE(closed_fields,''), COALESCE(human_fields,''), "
            "       COALESCE(owner_note,''), COALESCE(submit_to,''), "
            "       COALESCE(submit_method,'POST'), COALESCE(owner_subject_param,''), "
            "       COALESCE(owner_subject_value,''), COALESCE(return_template,''), "
            "       COALESCE(action_label,'') "
            "FROM object_contract WHERE applies_to_kind = ? AND birth_operation = ? "
            "  AND status = 'published' LIMIT 1", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        protocol_reply(503, "application/json", "{\"error\":\"grammar_unreadable\"}\n");
        return 1;
    }
    sqlite3_bind_text(st, 1, kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, operation, -1, SQLITE_TRANSIENT);

    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *name  = (const char *)sqlite3_column_text(st, 0);
        const char *mode  = (const char *)sqlite3_column_text(st, 1);
        const char *req   = (const char *)sqlite3_column_text(st, 2);
        const char *state = (const char *)sqlite3_column_text(st, 3);
        const char *alloc = (const char *)sqlite3_column_text(st, 4);
        const char *closed= (const char *)sqlite3_column_text(st, 5);
        const char *human = (const char *)sqlite3_column_text(st, 6);
        const char *note  = (const char *)sqlite3_column_text(st, 7);
        const char *submit = (const char *)sqlite3_column_text(st, 8);
        const char *method = (const char *)sqlite3_column_text(st, 9);
        const char *owner_param = (const char *)sqlite3_column_text(st, 10);
        const char *owner_value = (const char *)sqlite3_column_text(st, 11);
        const char *return_template = (const char *)sqlite3_column_text(st, 12);
        const char *label  = (const char *)sqlite3_column_text(st, 13);
        found = 1;

        if (req && *req && !surface_authority_satisfied(req, bearer, auth_db)) {
            sqlite3_finalize(st);
            sqlite3_close(db);
            printf("HTTP/1.1 403 Forbidden\r\nContent-Type: %s\r\nCache-Control: no-store\r\n"
                   "Connection: close\r\n\r\n", as_json ? "application/json" : "text/html; charset=utf-8");
            if (as_json)
                printf("{\"error\":\"aperture_authority_required\",\"operation\":\"%s\","
                       "\"requires\":\"%s\"}\n", operation, req);
            else
                printf("<p class=\"surface-denied\">This operation is not offered to this session.</p>");
            return 1;
        }

        /* Headers written here for both carriers: protocol_reply sends a
         * Content-Length for the body it is given, and this body is streamed
         * after the call, so using it for JSON declared a length of zero and
         * the client read an empty document. */
        printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\n"
               "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
               as_json ? "application/json" : "text/html; charset=utf-8");
        if (as_json) {
            printf("{\"object\":\"%s\",\"operation\":\"%s\",\"birth_mode\":\"%s\","
                   "\"subject\":\"%s\",\"initial_state\":\"%s\",\"identity_allocation\":\"%s\","
                   "\"closed_by_context\":\"%s\",\"human_must_supply\":\"%s\","
                   "\"owner_note\":\"%s\",\"submit_to\":\"%s\",\"submit_method\":\"%s\","
                   "\"forbidden_inference\":\"a rendered form is not an admitted submission; "
                   "the operation is gated at submit by Authority, by the contract's birth "
                   "policy, and by the owner of the effect\"}\n",
                   name, operation, mode, subject, state, alloc, closed, human, note,
                   submit, method);
        } else {
            /* The form posts to whoever owns the operation.
             *
             * Most births are performed by the aperture itself. An operation
             * owned elsewhere -- opening a session, moving money -- posts to
             * its owner, and routing it through here would make this organ a
             * second implementation of something it does not own. For a
             * session that also means no credential passes through the
             * aperture at any point. */
            char auth_next[BF_AUTH_NEXT_MAX] = {0};
            int auth_form = !strcmp(submit, "/login") || !strcmp(submit, "/signup");
            if (auth_form) auth_form_next("", query, host, auth_next, sizeof(auth_next));
            printf("<form class=\"atomic-form\" method=\"%s\" action=\"%s\" data-operation=\"",
                   method[0] ? method : "POST",
                   submit[0] ? submit : "/_aperture/submit");
            html(operation);
            printf("\"><input type=\"hidden\" name=\"subject\" value=\"");
            html(subject);
            printf("\"><input type=\"hidden\" name=\"operation\" value=\"");
            html(operation);
            printf("\">");
            if (auth_form) {
                fputs("<input type=\"hidden\" name=\"next\" value=\"", stdout);
                bf_auth_html(stdout, auth_next); fputs("\">", stdout);
            }
            if (owner_param && *owner_param && owner_value && *owner_value &&
                studio_safe_ident(owner_param)) {
                char owner_id[256], return_to[512];
                if (contract_owner_subject_value(subject, owner_value, owner_id, sizeof(owner_id))) {
                    printf("<input type=\"hidden\" name=\""); html(owner_param);
                    printf("\" value=\""); html(owner_id); printf("\">");
                    if (return_template && *return_template &&
                        contract_owner_return_value(return_template, owner_id, return_to, sizeof(return_to))) {
                        printf("<input type=\"hidden\" name=\"return\" value=\"");
                        html(return_to); printf("\">");
                    }
                }
            }
            printf("<h3>");
            html(name);
            printf("</h3><p class=\"mode\">");
            html(mode);
            printf(" &middot; identity ");
            html(alloc[0] ? alloc : "none");
            printf("</p>");
            if (*human) {
                printf("<div class=\"atomic-inputs\">");
                form_field_list(human, 1);
                printf("</div>");
            } else {
                printf("<p>Nothing is asked of a person: this operation is fully closed "
                       "by its context.</p>");
            }
            if (*closed) {
                printf("<details class=\"atomic-closed\"><summary>Already resolved by this "
                       "context</summary><ul>");
                form_field_list(closed, 0);
                printf("</ul></details>");
            }
            /* The contract names the words on its button. Deriving them from
             * the operation put "open" on a sign-in form: correct as an
             * identifier and wrong as a thing a person reads. */
            printf("<button class=\"button primary\" type=\"submit\">");
            page_glyph(page_operation_glyph(operation));
            html(label[0] ? label : object_name_of(operation));
            printf("</button>");
            if (*note) { printf("<p class=\"owner-note\">"); html(note); printf("</p>"); }
            printf("</form>");
            if (auth_form) {
                fputs(!strcmp(submit, "/login") ? "<p><a href=\"/signup?next=" : "<p><a href=\"/login?next=", stdout);
                bf_auth_query(stdout, auth_next);
                fputs(!strcmp(submit, "/login") ? "\">Create an account</a></p>" : "\">Sign in</a></p>", stdout);
            }
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    if (!found)
        protocol_reply(404, "application/json",
            "{\"error\":\"no_such_aperture\",\"note\":\"no published ObjectContract declares "
            "this operation for this subject kind\"}\n");
    fflush(stdout);
    return 1;
}

/* The classified kind of a foreign twin.
 *
 * Every twin has subject kind ForeignTwin, which says only that its authority
 * is outside this institution. It does not say what the thing is, and a parcel
 * endpoint and a specification do not afford the same actions: investigating
 * ownership is meaningless for a spec, and mapping a vocabulary is meaningless
 * for a tax roll. Applicable apertures derive from what a thing is.
 *
 * The classification is authored rather than inferred from the URL, because
 * guessing a kind from a hostname is how a merchant becomes a dataset. An
 * unclassified twin keeps the generic kind and is offered only what is true of
 * anything foreign.
 *
 * Cannot infer: a classified kind -> a verified one. Someone said this is a
 * property source; that is an assertion like any other. */
#endif /* BONFYRE_X402_CAPABILITY_AND_APERTURE_FORMS_H */
