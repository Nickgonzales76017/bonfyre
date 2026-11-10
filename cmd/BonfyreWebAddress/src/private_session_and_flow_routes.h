#ifndef BONFYRE_PRIVATE_SESSION_AND_FLOW_ROUTES_H
#define BONFYRE_PRIVATE_SESSION_AND_FLOW_ROUTES_H
/* Extracted verbatim from main.c lines 21184-22434 (mechanical split, no logic changes). */
static int asset_twin_digest_target(const char *target, char digest[128]) {
    const char *tail;
    size_t n;
    if (strncmp(target, "/assets/twin/", 13)) return 0;
    tail = target + 13;
    n = strlen(tail);
    if (n < 64 || n > 80) return 0;
    memcpy(digest, tail, 64);
    digest[64] = '\0';
    if (!studio_safe_hex(digest, 64)) return 0;
    if (tail[64] == '\0') return 1;
    if (tail[64] != '.' || !tail[65]) return 0;
    for (size_t i = 65; tail[i]; i++) {
        if (!isalnum((unsigned char)tail[i]) || i > 79) return 0;
    }
    return 1;
}

/* Locate a retained body by its content digest, checking every receipt. */
static int asset_body_for_digest(const char *digest, char *out_path, size_t sz) {
    const char *dir = getenv("BONFYRE_FOREIGN_RECEIPTS");
    BfReceiptDir *d;
    struct dirent *e;
    int found = 0;
    if (!dir || !*dir) dir = "/var/lib/bonfyre/foreign/receipts";
    d = bf_receipt_opendir(dir);
    if (!d) return 0;
    while (!found && (e = bf_receipt_readdir(d))) {
        char path[PATH_MAX], buf[8192];
        FILE *f;
        size_t n;
        if (e->d_name[0] == '.') continue;
        if (snprintf(path, sizeof(path), "%s/%s", dir, e->d_name) >= (int)sizeof(path)) continue;
        f = bf_receipt_fopen(path, "rb");
        if (!f) continue;
        n = fread(buf, 1, sizeof(buf) - 1, f);
        buf[n] = '\0';
        fclose(f);
        if (strstr(buf, digest)) {
            char bp[PATH_MAX];
            if (studio_json_field(buf, "body_path", bp, sizeof(bp)) && bp[0]) {
                snprintf(out_path, sz, "%s", bp);
                found = 1;
            }
        }
    }
    bf_receipt_closedir(d);
    return found;
}

static int serve_asset_twin(const char *method, const char *target) {
    char digest[128], body_path[PATH_MAX];
    unsigned char *buf = NULL;
    long len = 0;
    FILE *f;
    char hex[65];

    if (strncmp(target, "/assets/twin/", 13)) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!asset_twin_digest_target(target, digest)) {
        protocol_reply(404, "application/json", "{\"error\":\"asset_twin_not_found\"}\n");
        return 1;
    }
    if (!asset_body_for_digest(digest, body_path, sizeof(body_path))) {
        protocol_reply(404, "application/json",
            "{\"error\":\"asset_twin_not_found\",\"note\":\"no receipted observation carries this digest\"}\n");
        return 1;
    }
    f = fopen(body_path, "rb");
    if (!f) { protocol_reply(503, "application/json", "{\"error\":\"retained_body_unreadable\"}\n"); return 1; }
    fseek(f, 0, SEEK_END); len = ftell(f); fseek(f, 0, SEEK_SET);
    /* The old 16 MiB image ceiling silently made receipted audio incapable of
     * becoming a program.  Keep an explicit bounded native read, but make it
     * large enough for ordinary public recordings.  Static publication carries
     * the same verified body to an object store for the large-carrier path. */
    if (len <= 0 || len > 128 * 1024 * 1024) { fclose(f);
        protocol_reply(503, "application/json", "{\"error\":\"retained_body_out_of_range\"}\n"); return 1; }
    buf = (unsigned char *)malloc((size_t)len);
    if (!buf) { fclose(f); protocol_reply(503, "application/json", "{\"error\":\"oom\"}\n"); return 1; }
    if (fread(buf, 1, (size_t)len, f) != (size_t)len) { free(buf); fclose(f);
        protocol_reply(503, "application/json", "{\"error\":\"retained_body_short_read\"}\n"); return 1; }
    fclose(f);

    bf_sha256_hex(buf, (size_t)len, hex);
    if (strcmp(hex, digest) != 0) {
        /* The bytes on disk are no longer the bytes that were receipted. */
        free(buf);
        protocol_reply(409, "application/json",
            "{\"error\":\"digest_mismatch\",\"reason\":\"the retained body no longer hashes to its receipt, "
            "so it is not served\"}\n");
        return 1;
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\nContent-Length: %ld\r\n"
           "Cache-Control: public, max-age=31536000, immutable\r\n"
           "X-Bonfyre-Twin-Digest: %s\r\nConnection: close\r\n\r\n",
           asset_media_type(buf, (size_t)len), len, digest);
    fwrite(buf, 1, (size_t)len, stdout);
    fflush(stdout);
    free(buf);
    return 1;
}

/* Authorization, not just authentication.
 *
 * session_valid() answers "is someone signed in", which is the wrong question
 * for a mutation: every write verb on this surface was reachable by any account
 * that had just signed up, including setting a budget cap and editing another
 * person's published page. BonfyreAuth already decides scope from the account's
 * tier and denies "write" to a free account; the surface simply never asked. */
static int session_authorized(const char *bearer, const char *db, const char *scope) {
    char out[2048];
    const char *bin = auth_binary();
    if (!valid_token(bearer)) return 0;
    {
        char *const argv[] = { (char*)bin, "--db", (char*)db, "authorize",
                               "--token", (char*)bearer, "--scope", (char*)scope, NULL };
        if (auth_call(argv, NULL, out, sizeof(out)) != 0) return 0;
    }
    return strstr(out, "\"authorized\":true") != NULL;
}

/* Self-authoring is intentionally a strict subset of ordinary write.  It is
 * not a wildcard scope: each caller must still name and enforce its narrow
 * self-owned mutation grammar. */
static int session_self_authoring(const char *bearer, const char *db) {
    return session_authorized(bearer, db, "write") || session_authorized(bearer, db, "self");
}

/* A valid Matrix session is not a client-estate session.  The native auth
 * owner keeps revocable institution grants separately from account tier, so a
 * Pro account cannot use a guessed JRSS, AI Loss, Upper90, or Leads hostname
 * as an authority upgrade. */
static int session_institution_authorized(const char *bearer, const char *db,
                                          const char *institution,
                                          const char *role) {
    char out[2048];
    const char *bin = auth_binary();
    if (!valid_token(bearer) || !institution || !*institution || !role || !*role) return 0;
    {
        char *const argv[] = { (char*)bin, "--db", (char*)db, "institution-authorize",
                               "--token", (char*)bearer, "--institution", (char*)institution,
                               "--role", (char*)role, NULL };
        if (auth_call(argv, NULL, out, sizeof(out)) != 0) return 0;
    }
    return strstr(out, "\"authorized\":true") != NULL;
}

/* Delegated institutions are not public Matrix territories.  This small
 * dispatcher is deliberately a carrier over the native Upper90 product owner:
 * it resolves the browser session to an actor, verifies a host-specific
 * institutional grant, and renders the role/scope closure as HTML.  It never
 * opens a JRSS or AI Loss record, interprets an opaque reference, or turns a
 * request into a legacy effect.
 *
 * There are two separate gates on purpose:
 *   1. BonfyreAuth grants entry to the exact client estate at this host.
 *   2. BonfyreUpper90 grants the named role and scope inside that estate.
 * A paid account, a JRSS grant, or an Upper90 role never implies either of the
 * others.  Keeping the second gate native also lets a revoked role disappear
 * from a still-valid browser session immediately. */
static const char *private_estate_for_host(const char *host) {
    char normalized[256];
    if (!lower_host(host, normalized)) return NULL;
    if (!strcmp(normalized, "upper90.aurekai.work")) return "upper90";
    if (!strcmp(normalized, "jrss.aurekai.work")) return "jrss";
    if (!strcmp(normalized, "ai-loss.aurekai.work")) return "ai-loss";
    if (!strcmp(normalized, "leads.aurekai.work")) return "leads";
    return NULL;
}

static int private_machine_path(const char *target) {
    static const char *const prefixes[] = {
        "/.well-known", "/robots.txt", "/sitemap", "/feed", "/ro-crate",
        "/zarr", "/mcp", "/a2a", "/ap", "/events", "/ogc", "/reso",
        "/mismo", "/beckn", "/ucp", "/acp", "/mpp", "/cap", "/epcis",
        "/wot", "/openrtb", NULL
    };
    for (size_t i = 0; prefixes[i]; i++) {
        size_t n = strlen(prefixes[i]);
        if (!strncmp(target, prefixes[i], n) &&
            (target[n] == '\0' || target[n] == '/' || target[n] == '.')) return 1;
    }
    return 0;
}

/* The organ serving a delegated vertical, and the store it reads.
 *
 * These were named for the one vertical that exists. A second delegated
 * business would have needed its own env var, its own accessor and its own
 * route, so the tenant was baked into the shape of the code rather than being
 * one row of the namespace contract. The general name is read first and the
 * tenant-specific one still answers, so a deployment that has not been updated
 * keeps working rather than losing its estate at restart.
 *
 * Cannot infer: a resolved organ -> authority over the estate it serves. This
 * finds the binary; admission is still the grant's answer. */
static const char *vertical_binary(void) {
    const char *configured = getenv("BONFYRE_VERTICAL_BINARY");
    if (!configured || !*configured) configured = getenv("BONFYRE_UPPER90_BINARY");
    static char path[PATH_MAX];
    const char *bindir;
    if (configured && *configured) return configured;
    bindir = getenv("BONFYRE_BIN_DIR");
    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    if (snprintf(path, sizeof(path), "%s/bonfyre-upper90", bindir) >= (int)sizeof(path)) return "";
    return path;
}

/* Upper90 scopes are deliberately opaque to the browser, but the carrier has
 * to turn one already-admitted scope into the CMS owner's namespace.  Keep the
 * accepted alphabet identical to BonfyreUpper90's safe_atom contract.  This
 * makes the command answer a narrow closure coordinate, not a JSON API whose
 * fields can be reinterpreted by the edge. */
static int private_scope_safe(const char *scope) {
    if (!scope || !*scope || strlen(scope) > 128) return 0;
    for (; *scope; scope++)
        if (!(isalnum((unsigned char)*scope) || *scope == ':' || *scope == '-' ||
              *scope == '_' || *scope == '.')) return 0;
    return 1;
}

/* The CMS is a normal native owner, but its namespace must be compiled from
 * the same client-role closure that admitted this request.  Do not accept a
 * scope query parameter, a hidden field, or a role name here: a caller can
 * select neither a client estate nor another role's records by changing a URL.
 *
 * This phase intentionally uses the first stable scope returned by the native
 * owner.  Multi-scope work remains separate rather than being silently unioned
 * into one writable CMS bucket; a later Room/StandingAddress can make such a
 * selection explicit without broadening the closure. */
static int private_cms_namespace_for(const char *binary, const char *db_path,
                                     const char *actor, const char *estate,
                                     char *out, size_t out_sz) {
    char output[4096] = {0}, diagnostic[1024] = {0}, scope[129] = {0};
    const char *p, *q;
    char *const argv[] = { (char *)binary, "--db", (char *)db_path,
                           "closure", "--actor", (char *)actor,
                           "--estate", (char *)estate, NULL };
    if (!binary || !*binary || !db_path || !*db_path || !actor || !estate ||
        !out || !out_sz || auth_call_split(argv, NULL, output, sizeof(output),
                                            diagnostic, sizeof(diagnostic)) != 0)
        return 0;
    p = strstr(output, "\"scopes\":[\"");
    if (!p) return 0;
    p += strlen("\"scopes\":[\"");
    q = strchr(p, '\"');
    if (!q || q == p || (size_t)(q - p) >= sizeof(scope)) return 0;
    memcpy(scope, p, (size_t)(q - p));
    scope[q - p] = '\0';
    if (!private_scope_safe(scope)) return 0;
    return snprintf(out, out_sz, "private:%s:%s", estate, scope) > 0 &&
           strlen(out) < out_sz;
}

static int private_cms_target(const char *target) {
    if (!strcmp(target, "/cms") || !strcmp(target, "/cms/")) return 1;
    if (!strncmp(target, "/cms/parts/", 11)) return 1;
    if (!strncmp(target, "/cms/v1/api/", 12)) return 1;
    return 0;
}

static int private_foreign_target(const char *target) {
    return !strcmp(target, "/foreign") || !strcmp(target, "/foreign/") ||
           !strcmp(target, "/foreign/observe");
}

/* `/studio` is a client workroom, not an alias for the global operational
 * console.  It is intentionally small because every included plane has to be
 * supplied by a real owner under this same closure: Upper90 supplies durable
 * review work, BonfyreCMS supplies declared content grammar, and ForeignTwin
 * supplies receipted private observation. */
static int private_studio_target(const char *target) {
    return !strcmp(target, "/studio") || !strcmp(target, "/studio/");
}

static int private_foreign_root_for(const char *estate, const char *namespace_name,
                                    char *out, size_t out_sz) {
    const char *base = getenv("BONFYRE_FOREIGN_ROOT");
    const char *scope;
    size_t prefix;
    if (!base || !*base) base = "/var/lib/bonfyre/foreign";
    if (!estate || !*estate || !namespace_name || !out || !out_sz) return 0;
    prefix = strlen("private:") + strlen(estate) + 1;
    if (strncmp(namespace_name, "private:", strlen("private:")) ||
        strncmp(namespace_name + strlen("private:"), estate, strlen(estate)) ||
        namespace_name[strlen("private:") + strlen(estate)] != ':') return 0;
    scope = namespace_name + prefix;
    if (!private_scope_safe(scope)) return 0;
    return snprintf(out, out_sz, "%s/private/%s/%s", base, estate, scope) > 0 &&
           strlen(out) < out_sz;
}

static void private_html_reply(int code, const char *fragment, int document) {
    const char *body = fragment && *fragment ? fragment :
        "<section class=\"u90-workspace\"><h2>Private estate unavailable</h2><p>No client data was requested.</p></section>";
    const char *reason = code == 200 ? "OK" : code == 401 ? "Unauthorized" :
                         code == 403 ? "Forbidden" : code == 404 ? "Not Found" :
                         code == 503 ? "Service Unavailable" : "Error";
    if (document) {
        static char page[131072];
        int written = snprintf(page, sizeof(page),
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
            "<meta name=\"robots\" content=\"noindex,nofollow,noarchive,nosnippet\"><title>Upper90 private estate</title>"
            "<link rel=\"stylesheet\" href=\"/assets/matrix.css\"><script src=\"/assets/matrix-htmx.js\" defer></script>"
            "</head><body class=\"bonfyre-private-estate\"><main id=\"u90-content\">%s</main></body></html>", body);
        if (written > 0 && written < (int)sizeof(page)) body = page;
    }
    printf("HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: %zu\r\n"
           "Cache-Control: private, no-store\r\nX-Robots-Tag: noindex, nofollow, noarchive, nosnippet\r\n"
           "Referrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nConnection: close\r\n\r\n%s",
           code, reason,
           strlen(body), body);
}

/* This is a deliberately small human doorway over the native ForeignTwin
 * observer.  The action is useful: it creates a provenance-bound observation
 * after robots preflight and then returns its owner receipt as an HTMX
 * fragment.  It is not a second ForeignTwin database or a fake URL catalogue. */
static int serve_private_foreign_surface(const char *method, const char *target,
                                         const char *estate, const char *namespace_name) {
    static char fragment[8192];
    char e_estate[128], e_namespace[384];
    if (!strcmp(target, "/foreign/observe")) return 0;
    if (strcmp(target, "/foreign") && strcmp(target, "/foreign/")) return 0;
    if (strcmp(method, "GET")) {
        private_html_reply(404, "<section class=\"u90-workspace\"><h2>Not found</h2></section>", 0);
        return 1;
    }
    html_escape(estate, e_estate, sizeof(e_estate));
    html_escape(namespace_name, e_namespace, sizeof(e_namespace));
    snprintf(fragment, sizeof(fragment),
        "<section class=\"u90-workspace\"><p class=\"eyebrow\"><i></i>Private evidence intake</p>"
        "<h2>Observe a source for %s.</h2><p>Bonf\xC3\xBFre retains the exact observed body and receipt inside this admitted institutional closure. "
        "A source remains foreign evidence: observing it does not make it canonical truth, grant action authority, or publish it.</p>"
        "<form class=\"u90-request\" hx-post=\"/foreign/observe\" hx-target=\"#foreign-result\" hx-swap=\"innerHTML\">"
        "<label><span>Twin label</span><input name=\"twin_id\" maxlength=\"96\" pattern=\"[A-Za-z0-9:_-]+\" placeholder=\"source-brief\" required></label>"
        "<label><span>Public URL</span><input type=\"url\" name=\"url\" maxlength=\"1400\" placeholder=\"https://example.org/source\" required></label>"
        "<button type=\"submit\">Observe source</button></form><div id=\"foreign-result\" aria-live=\"polite\"></div>"
        "<p class=\"u90-boundary\">Closure: %s. Public and private ForeignTwin paths are separate stores; a public carrier cannot enumerate this intake.</p></section>",
        e_estate, e_namespace);
    private_html_reply(200, fragment, 1);
    return 1;
}

/* Compose the already-native Upper90 workroom with the two other closure
 * carriers.  This is deliberately not a panel of JSON status calls: the work
 * section is the Upper90 owner's real HTML surface and the two routes below
 * lead to native CMS/ForeignTwin actions that independently re-evaluate this
 * request's closure before use. */
static int serve_private_studio_surface(const char *method, const char *target,
                                        const char *estate, const char *namespace_name,
                                        const char *binary, const char *db_path,
                                        const char *actor) {
    static char owner_html[65536], fragment[131072];
    char diagnostic[4096], e_estate[128], e_namespace[384];
    char *const argv[] = { (char *)binary, "--db", (char *)db_path,
                           "render", "--actor", (char *)actor,
                           "--estate", (char *)estate, NULL };
    if (strcmp(target, "/studio") && strcmp(target, "/studio/")) return 0;
    if (strcmp(method, "GET")) {
        private_html_reply(404, "<section class=\"u90-workspace\"><h2>Not found</h2></section>", 0);
        return 1;
    }
    if (auth_call_split(argv, NULL, owner_html, sizeof(owner_html),
                        diagnostic, sizeof(diagnostic)) != 0 || !owner_html[0]) {
        private_html_reply(503,
            "<section class=\"u90-workspace\"><h2>Private Studio is unavailable</h2><p>No client work was requested.</p></section>",
            1);
        return 1;
    }
    html_escape(estate, e_estate, sizeof(e_estate));
    html_escape(namespace_name, e_namespace, sizeof(e_namespace));
    if (snprintf(fragment, sizeof(fragment),
        "<section class=\"u90-workspace\"><header><p class=\"u90-eyebrow\">%s / private Studio</p>"
        "<h2>Work from one admitted closure.</h2><p>This room compiles the same current role and scope into durable review work, private content, and private evidence. It does not expose the global Aurekai operator estate.</p></header>"
        "<div class=\"u90-card\"><div class=\"u90-card-head\"><div><p class=\"u90-kicker\">Content</p><h3>Write and organize this estate</h3>"
        "<p>Declared CMS grammar is opened only inside the current private namespace.</p></div>"
        "<a class=\"button primary\" href=\"/cms/\">Open Content Studio <span>&rarr;</span></a></div></div>"
        "<div class=\"u90-card\"><div class=\"u90-card-head\"><div><p class=\"u90-kicker\">Physical / mobile</p><h3>Prepare a lawful handoff</h3>"
        "<p>Physical Carrier recipes—QR, NFC, Wallet, device, or signed handoff—remain inside this same closure. A scan can resolve an address or begin a bounded ceremony; it cannot publish client work, authenticate a person, or authorize an effect by itself.</p></div>"
        "<a class=\"button quiet\" href=\"/cms/\">Open carrier recipes <span>&rarr;</span></a></div></div>"
        "<div class=\"u90-card\"><div class=\"u90-card-head\"><div><p class=\"u90-kicker\">Evidence</p><h3>Observe a public source privately</h3>"
        "<p>ForeignTwin retains an exact body and receipt below this closure; observation never publishes it or promotes a claim.</p></div>"
        "<a class=\"button quiet\" href=\"/foreign/\">Open evidence intake <span>&rarr;</span></a></div></div>"
        "<p class=\"u90-boundary\">Current closure: %s. Changing a URL, hidden field, or browser state cannot choose another estate, role, scope, namespace, or private body store.</p>"
        "</section><section class=\"u90-studio-work\"><p class=\"eyebrow\"><i></i>Native operating work</p>%s</section>",
        e_estate, e_namespace, owner_html) >= (int)sizeof(fragment)) {
        private_html_reply(503,
            "<section class=\"u90-workspace\"><h2>Private Studio is too large to render safely</h2><p>No client data was sent.</p></section>",
            1);
        return 1;
    }
    private_html_reply(200, fragment, 1);
    return 1;
}

static int serve_vertical_private(const char *method, const char *target, const char *body,
                                 const char *bearer, const char *auth_db, const char *host) {
    const char *estate = private_estate_for_host(host);
    char user[32], actor[64], output[65536], diagnostic[4096], db_path[PATH_MAX], submitted_estate[32];
    const char *binary;
    int write = strcmp(method, "GET") && strcmp(method, "HEAD");
    int cms_target, foreign_target, studio_target;
    if (!estate) return 0;

    /* The tiny shared HTMX/CSS carrier has no subject, client, or session
     * content.  Let the ordinary immutable asset handler serve it; every
     * semantic/private route remains below the gates in this function. */
    if (!strncmp(target, "/assets/", 8)) return 0;

    /* Private hostnames must not accidentally participate in the public
     * MachineMembrane—even when this binary is reached directly in a local
     * test rather than through its nginx refusal block. */
    if (private_machine_path(target)) {
        private_html_reply(404, "<section class=\"u90-workspace\"><h2>Not found</h2></section>", 0);
        return 1;
    }
    if (!studio_auth_user(bearer, auth_db, user, sizeof(user))) {
        private_html_reply(401,
            "<section class=\"u90-workspace\"><h2>Sign in to enter this private estate.</h2><p>A public Aurekai account does not disclose client work.</p></section>",
            !strcmp(method, "GET"));
        return 1;
    }
    if (!session_institution_authorized(bearer, auth_db, estate, write ? "write" : "read")) {
        private_html_reply(403,
            "<section class=\"u90-workspace\"><h2>This client estate is not assigned to this session.</h2><p>Client access is revocable and host-specific; an Aurekai account or another client grant is not sufficient.</p></section>",
            !strcmp(method, "GET"));
        return 1;
    }
    cms_target = private_cms_target(target);
    foreign_target = private_foreign_target(target);
    studio_target = private_studio_target(target);
    if (strcmp(target, "/") && strcmp(target, "/upper90") && strcmp(target, "/upper90/") &&
        strcmp(target, "/upper90/requests") && !cms_target && !foreign_target && !studio_target) {
        /* Nginx may choose its protected legacy fallback for existing client
         * software.  This native owner returns an honest miss, never a Matrix
         * route that could expose a new private surface by accident. */
        private_html_reply(404, "<section class=\"u90-workspace\"><h2>Not found</h2></section>", 0);
        return 1;
    }
    if (!strcmp(target, "/upper90/requests")) {
        if (strcmp(method, "POST")) {
            private_html_reply(404, "<section class=\"u90-workspace\"><h2>Not found</h2></section>", 0);
            return 1;
        }
        if (!query_value(body, "estate", submitted_estate, sizeof(submitted_estate)) || strcmp(submitted_estate, estate)) {
            private_html_reply(403,
                "<section class=\"u90-workspace\"><h2>The request does not belong to this client estate.</h2><p>No work was created.</p></section>", 0);
            return 1;
        }
    } else if (!cms_target && !foreign_target && !studio_target && strcmp(method, "GET") && strcmp(method, "HEAD")) {
        private_html_reply(404, "<section class=\"u90-workspace\"><h2>Not found</h2></section>", 0);
        return 1;
    }
    snprintf(actor, sizeof(actor), "auth-user:%s", user);
    binary = vertical_binary();
    {
        const char *configured_db = getenv("BONFYRE_VERTICAL_DB");
        if (!configured_db || !*configured_db) configured_db = getenv("BONFYRE_UPPER90_DB");
        if (configured_db && *configured_db) snprintf(db_path, sizeof(db_path), "%s", configured_db);
        else snprintf(db_path, sizeof(db_path), "/var/lib/bonfyre/upper90.db");
    }
    if (!binary[0] || access(binary, X_OK) != 0) {
        private_html_reply(404, "<section class=\"u90-workspace\"><h2>Private estate unavailable</h2><p>No client system was contacted.</p></section>", !strcmp(method, "GET"));
        return 1;
    }
    if (cms_target || foreign_target || studio_target) {
        /* The CMS receives no client id, role id, or scope from the browser.
         * Its only namespace comes from Upper90's freshly evaluated closure.
         * Returning zero lets the normal HTMX/CMS owner serve the request;
         * `private_cms_namespace` is reset before every request dispatch. */
        if (!private_cms_namespace_for(binary, db_path, actor, estate,
                                       private_cms_namespace,
                                       sizeof(private_cms_namespace))) {
            private_html_reply(403,
                "<section class=\"u90-workspace\"><h2>Content access is not assigned.</h2><p>A client admission needs a current native role and scope before the CMS can open.</p></section>",
                !strcmp(method, "GET"));
            return 1;
        }
        if (foreign_target) {
            if (!private_foreign_root_for(estate, private_cms_namespace,
                                          private_foreign_root,
                                          sizeof(private_foreign_root))) {
                private_html_reply(403,
                    "<section class=\"u90-workspace\"><h2>Foreign evidence access is unavailable.</h2><p>No source was observed.</p></section>",
                    !strcmp(method, "GET"));
                return 1;
            }
            return serve_private_foreign_surface(method, target, estate,
                                                 private_cms_namespace);
        }
        if (studio_target) {
            return serve_private_studio_surface(method, target, estate,
                                                private_cms_namespace, binary,
                                                db_path, actor);
        }
        return 0;
    }
    if (!strcmp(target, "/upper90/requests")) {
        char *const argv[] = { (char*)binary, "--db", db_path, "submit", "--actor", actor, "--estate", (char*)estate, NULL };
        if (auth_call_split(argv, body, output, sizeof(output), diagnostic, sizeof(diagnostic)) != 0 || !output[0]) {
            private_html_reply(404, "<section class=\"u90-workspace\"><h2>Protected work is unavailable</h2><p>No client action was attempted.</p></section>", 0);
            return 1;
        }
        private_html_reply(200, output, 0); return 1;
    }
    {
        char *const argv[] = { (char*)binary, "--db", db_path, "render", "--actor", actor, "--estate", (char*)estate, NULL };
        if (auth_call_split(argv, NULL, output, sizeof(output), diagnostic, sizeof(diagnostic)) != 0 || !output[0]) {
            private_html_reply(404, "<section class=\"u90-workspace\"><h2>Private estate unavailable</h2><p>No client data was requested.</p></section>", 1);
            return 1;
        }
    }
    private_html_reply(200, output, 1);
    return 1;
}

/* ── Bonfyre superuser repair room ────────────────────────────────────────
 *
 * The full Bonfyre admin is not a slightly wider client closure.  It is the
 * one explicit operator closure that can repair canonical account admission
 * and copy grammar itself.  It still calls the existing native owners: Auth
 * owns users/tier/grants and CMS owns copy/schema.  The browser owns neither.
 *
 * Keeping this surface on the controlled Work root makes it a private human
 * carrier, not a public account directory or a new admin database. */
static int superuser_host(const char *host) {
    char normalized[256];
    return lower_host(host, normalized) && !strcmp(normalized, "aurekai.work");
}

static int superuser_positive_id(const char *text, int *out) {
    char *end = NULL; long value;
    if (!text || !*text || strlen(text) > 10 || !out) return 0;
    errno = 0; value = strtol(text, &end, 10);
    if (errno || !end || *end || value <= 0 || value > 2147483647L) return 0;
    *out = (int)value; return 1;
}

static int superuser_institution_name(const char *text) {
    if (!text || !*text || strlen(text) > 96) return 0;
    for (; *text; text++)
        if (!(isalnum((unsigned char)*text) || *text == ':' || *text == '_' ||
              *text == '-' || *text == '.')) return 0;
    return 1;
}

static void superuser_html_reply(int code, const char *fragment) {
    static char page[131072];
    const char *body = fragment && *fragment ? fragment :
        "<section class=\"studio-result\"><h2>Control room unavailable</h2><p>No native owner was changed.</p></section>";
    int n = snprintf(page, sizeof(page),
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        "<meta name=\"robots\" content=\"noindex,nofollow,noarchive,nosnippet\"><title>Bonfÿre control room</title>"
        "<link rel=\"stylesheet\" href=\"/assets/matrix.css\"><script src=\"/assets/matrix-htmx.js\" defer></script>"
        "</head><body class=\"bonfyre-private-estate\"><main id=\"u90-content\">%s</main></body></html>", body);
    if (n <= 0 || n >= (int)sizeof(page)) body = "<p>Control room render refused.</p>";
    else body = page;
    printf("HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: %zu\r\n"
           "Cache-Control: private, no-store\r\nX-Robots-Tag: noindex, nofollow, noarchive, nosnippet\r\n"
           "Referrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nConnection: close\r\n\r\n%s",
           code, code == 200 ? "OK" : code == 401 ? "Unauthorized" : code == 403 ? "Forbidden" : "Bad Request", strlen(body), body);
}

static int serve_superuser_control(const char *method, const char *target, const char *body,
                                   const char *bearer, const char *auth_db, const char *host) {
    char out[32768] = {0}, err[4096] = {0}, escaped[65536], fragment[98304];
    const char *bin = auth_binary();
    int is_action = !strcmp(target, "/studio/admin/user") || !strcmp(target, "/studio/admin/grant") ||
                    !strcmp(target, "/studio/admin/revoke") || !strcmp(target, "/studio/admin/sessions-revoke");
    if (strcmp(target, "/studio/admin") && strcmp(target, "/studio/admin/") && !is_action) return 0;
    if (!superuser_host(host)) { superuser_html_reply(404, "<section class=\"studio-result\"><h2>Not found</h2></section>"); return 1; }
    if (!session_institution_authorized(bearer, auth_db, "bonfyre", "admin")) {
        superuser_html_reply(valid_token(bearer) ? 403 : 401,
            "<section class=\"studio-result\"><h2>Bonfÿre admin closure required.</h2><p>This room is separately checked on every request. A paid account, an Upper90 role, or another institution grant is not enough.</p></section>");
        return 1;
    }
    if (is_action && strcmp(method, "POST")) { superuser_html_reply(405, "<section class=\"studio-result\"><h2>POST required</h2></section>"); return 1; }
    if (is_action) {
        char id_text[32] = {0}, institution[128] = {0}, tier[32] = {0}, active[16] = {0}, role[16] = {0};
        int id = 0, rc = -1;
        (void)query_value(body, "user_id", id_text, sizeof(id_text));
        if (!superuser_positive_id(id_text, &id)) { superuser_html_reply(400, "<section class=\"studio-result\"><h2>Valid user id required.</h2><p>No native owner was changed.</p></section>"); return 1; }
        if (!strcmp(target, "/studio/admin/user")) {
            char *argv[] = { (char *)bin, "--db", (char *)auth_db, "update", "--id", id_text, NULL, NULL, NULL, NULL, NULL };
            (void)query_value(body, "tier", tier, sizeof(tier)); (void)query_value(body, "active", active, sizeof(active));
            if (tier[0] && strcmp(tier, "free") && strcmp(tier, "pro") && strcmp(tier, "enterprise")) { superuser_html_reply(400, "<section class=\"studio-result\"><h2>Unknown tier.</h2></section>"); return 1; }
            if (active[0] && strcmp(active, "0") && strcmp(active, "1")) { superuser_html_reply(400, "<section class=\"studio-result\"><h2>Active must be 0 or 1.</h2></section>"); return 1; }
            if (!tier[0] && !active[0]) { superuser_html_reply(400, "<section class=\"studio-result\"><h2>Choose a tier or active state.</h2></section>"); return 1; }
            int n = 6; if (tier[0]) { argv[n++] = "--tier"; argv[n++] = tier; } if (active[0]) { argv[n++] = "--active"; argv[n++] = active; } argv[n] = NULL;
            rc = auth_call_split(argv, NULL, out, sizeof(out), err, sizeof(err));
        } else if (!strcmp(target, "/studio/admin/grant")) {
            (void)query_value(body, "institution", institution, sizeof(institution)); (void)query_value(body, "role", role, sizeof(role));
            if (!superuser_institution_name(institution) || (strcmp(role, "read") && strcmp(role, "write") && strcmp(role, "admin"))) { superuser_html_reply(400, "<section class=\"studio-result\"><h2>Valid institution and read/write/admin role required.</h2></section>"); return 1; }
            char *argv[] = { (char *)bin, "--db", (char *)auth_db, "institution-grant", "--user-id", id_text, "--institution", institution, "--role", role, NULL };
            rc = auth_call_split(argv, NULL, out, sizeof(out), err, sizeof(err));
        } else if (!strcmp(target, "/studio/admin/revoke")) {
            (void)query_value(body, "institution", institution, sizeof(institution));
            if (!superuser_institution_name(institution)) { superuser_html_reply(400, "<section class=\"studio-result\"><h2>Valid institution required.</h2></section>"); return 1; }
            char *argv[] = { (char *)bin, "--db", (char *)auth_db, "institution-revoke", "--user-id", id_text, "--institution", institution, NULL };
            rc = auth_call_split(argv, NULL, out, sizeof(out), err, sizeof(err));
        } else {
            char *argv[] = { (char *)bin, "--db", (char *)auth_db, "sessions-revoke", "--user-id", id_text, NULL };
            rc = auth_call_split(argv, NULL, out, sizeof(out), err, sizeof(err));
        }
        studio_html_append(escaped, sizeof(escaped), &(size_t){0}, rc == 0 ? out : err);
        snprintf(fragment, sizeof(fragment),
            "<section class=\"studio-result %s\" aria-live=\"polite\"><span class=\"result-kicker\">Canonical owner receipt</span><h2>%s</h2><pre>%s</pre><p><a href=\"/studio/admin\">Return to the control room</a></p></section>",
            rc == 0 ? "is-ok" : "is-error", rc == 0 ? "Change applied" : "Change refused", escaped);
        superuser_html_reply(rc == 0 ? 200 : 400, fragment); return 1;
    }
    if (strcmp(method, "GET")) { superuser_html_reply(405, "<section class=\"studio-result\"><h2>GET required</h2></section>"); return 1; }
    {
        char *argv[] = { (char *)bin, "--db", (char *)auth_db, "users", NULL };
        int rc = auth_call_split(argv, NULL, out, sizeof(out), err, sizeof(err));
        size_t used = 0; studio_html_append(escaped, sizeof(escaped), &used, rc == 0 ? out : err);
        snprintf(fragment, sizeof(fragment),
            "<section class=\"u90-workspace\"><header><p class=\"u90-eyebrow\">Bonfÿre / operator closure</p><h2>Repair the institution without copying its owners.</h2>"
            "<p>Users, levels, and institution admissions are changed only through BonfyreAuth. Copy remains schema-bound in BonfyreCMS. This room is private, rechecked on every request, and never makes an account tier into subject, provider, payment, publication, or effect authority.</p></header>"
            "<div class=\"u90-card\"><div class=\"u90-card-head\"><div><p class=\"u90-kicker\">Copy</p><h3>Content Studio</h3><p>Edit declared copy with the native schema owner. Publication and public readability stay separate.</p></div><a class=\"button primary\" href=\"/cms/\">Open Content Studio <span>&rarr;</span></a></div></div>"
            "<div class=\"u90-card\"><div class=\"u90-card-head\"><div><p class=\"u90-kicker\">External membrane</p><h3>Candidate carriers</h3><p>Inspect retained public source passports and declared protocol candidates. They remain candidates: observation, admission, authority, and effect each have their own owner.</p></div><a class=\"button\" href=\"/membrane\">Open membrane <span>&rarr;</span></a></div></div>"
            "<div class=\"u90-card\"><p class=\"u90-kicker\">Users and levels</p><form class=\"u90-request\" hx-post=\"/studio/admin/user\" hx-target=\"#admin-result\" hx-swap=\"innerHTML\"><label><span>User id</span><input name=\"user_id\" inputmode=\"numeric\" required></label><label><span>Tier</span><select name=\"tier\"><option value=\"\">No change</option><option>free</option><option>pro</option><option>enterprise</option></select></label><label><span>Active</span><select name=\"active\"><option value=\"\">No change</option><option value=\"1\">active</option><option value=\"0\">disabled</option></select></label><button type=\"submit\">Apply user change</button></form></div>"
            "<div class=\"u90-card\"><p class=\"u90-kicker\">Institution closure</p><form class=\"u90-request\" hx-post=\"/studio/admin/grant\" hx-target=\"#admin-result\" hx-swap=\"innerHTML\"><label><span>User id</span><input name=\"user_id\" inputmode=\"numeric\" required></label><label><span>Institution</span><input name=\"institution\" placeholder=\"jrss\" required></label><label><span>Role</span><select name=\"role\"><option>read</option><option>write</option><option>admin</option></select></label><button type=\"submit\">Grant admission</button></form><form class=\"u90-request\" hx-post=\"/studio/admin/revoke\" hx-target=\"#admin-result\" hx-swap=\"innerHTML\"><label><span>User id</span><input name=\"user_id\" inputmode=\"numeric\" required></label><label><span>Institution</span><input name=\"institution\" placeholder=\"jrss\" required></label><button type=\"submit\">Revoke admission</button></form></div>"
            "<div class=\"u90-card\"><p class=\"u90-kicker\">Session containment</p><form class=\"u90-request\" hx-post=\"/studio/admin/sessions-revoke\" hx-target=\"#admin-result\" hx-swap=\"innerHTML\"><label><span>User id</span><input name=\"user_id\" inputmode=\"numeric\" required></label><button type=\"submit\">Revoke all active sessions</button></form><p>Changing a tier or active state also revokes sessions automatically, so the next login compiles a fresh scope.</p></div>"
            "<div id=\"admin-result\" aria-live=\"polite\"></div><section class=\"u90-studio-work\"><p class=\"u90-kicker\">Canonical users</p><pre>%s</pre></section>"
            "<p class=\"u90-boundary\">The MCP closure exposes the same owner controls for agents. A self or delegated institution slice never sees this room or those tools.</p></section>", escaped);
    }
    superuser_html_reply(200, fragment); return 1;
}

/* ── the property flow ───────────────────────────────────────────────────
 *
 * One intent carried through, rather than a panel of controls that each fire a
 * call and print a receipt. A person arrives with an address or an account
 * number; they see that parcel; they see what the surrounding market looks like
 * from the same receipted estate; and they can act on it. Each step renders the
 * next, so the page always knows where the person is.
 *
 * Every number below comes from observations admitted under the foreign law.
 * None of it is an appraisal, an offer, or advice.
 */
static void flow_reply_status(int code, const char *reason, const char *html_body) {
    printf("HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\n"
           "Content-Length: %zu\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
           code, reason, strlen(html_body), html_body);
    fflush(stdout);
}

static void flow_reply(const char *html_body) {
    flow_reply_status(200, "OK", html_body);
}

/* A search term reaches a SQL literal, so it is restricted to the characters a
 * street address or account number actually contains, and quoted-doubled. */
/* A subject identifier, decoded and checked as an identifier.
 *
 * The route ran the id through flow_safe_term, which is a SQL-literal
 * sanitiser for property search: it silently drops every character a street
 * address does not contain -- the colon in "institution:aurekai:internet"
 * included -- and doubles quotes. So the canonical address received an id
 * that matched nothing, and nothing said so. A bound parameter does not want
 * a sanitiser; it wants a validator, and one that refuses rather than
 * quietly rewrites. Nothing here reaches SQL as text.
 */
static int subject_ident(const char *in, char *out, size_t cap) {
    size_t w = 0;
    if (!in || !*in) return 0;
    while (*in && w + 1 < cap) {
        unsigned char c = (unsigned char)*in;
        if (c == '%' && isxdigit((unsigned char)in[1]) && isxdigit((unsigned char)in[2])) {
            char hex[3];
            hex[0] = in[1]; hex[1] = in[2]; hex[2] = 0;
            c = (unsigned char)strtol(hex, NULL, 16);
            in += 3;
        } else {
            in++;
        }
        if (!(isalnum(c) || c == ':' || c == '-' || c == '_' || c == '.' || c == '@'))
            return 0;
        out[w++] = (char)c;
    }
    if (*in) return 0;          /* longer than we hold: refuse, never truncate */
    out[w] = '\0';
    return w > 0;
}

static int flow_safe_term(const char *in, char *out, size_t cap) {
    size_t w = 0;
    if (!in || !*in) return 0;
    for (; *in && w + 2 < cap; in++) {
        unsigned char c = (unsigned char)*in;
        if (!(isalnum(c) || c == ' ' || c == '-' || c == '.' || c == '#' || c == '\'')) continue;
        if (c == '\'') { out[w++] = '\''; }          /* double it for SQL */
        out[w++] = (char)c;
    }
    out[w] = '\0';
    return w > 0;
}

static int serve_property_flow(const char *method, const char *target, const char *query,
                               const char *bearer, const char *auth_db) {
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    static char rows[262144], body[524288];
    char sql[4096], term[128], raw[128];
    int w;

    if (strncmp(target, "/property", 9)) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";

    /* Step 2+: a specific parcel, then its market, then what can be done. */
    if (!strncmp(target, "/property/parcel", 16)) {
        char acct[64];
        if (!query_value(query, "account", raw, sizeof(raw)) || !flow_safe_term(raw, acct, sizeof(acct))) {
            flow_reply("<p class=\"flow-empty\">No parcel selected.</p>"); return 1;
        }
        snprintf(sql, sizeof(sql),
          "SELECT json_extract_string(observed,'$.Account_Nu') AS account, "
          "json_extract_string(observed,'$.Situs_Addr') AS address, "
          "json_extract_string(observed,'$.Owner_Name') AS owner, "
          "json_extract_string(observed,'$.City') AS city, "
          "TRY_CAST(json_extract_string(observed,'$.Year_Built') AS BIGINT) AS year_built, "
          "TRY_CAST(json_extract_string(observed,'$.Living_Are') AS DOUBLE) AS living_area, "
          "TRY_CAST(json_extract_string(observed,'$.Land_SqFt') AS DOUBLE) AS lot_sqft, "
          "TRY_CAST(json_extract_string(observed,'$.Land_Value') AS DOUBLE) AS land_value, "
          "TRY_CAST(json_extract_string(observed,'$.Total_Valu') AS DOUBLE) AS total_value "
          "FROM observation WHERE src_id='src:tad-parcel-view' "
          "AND trim(json_extract_string(observed,'$.Account_Nu'))='%s' LIMIT 1", acct);
        if (fleming_query(bindir, sql, rows, sizeof(rows)) != 0 || rows[0] != '[') {
            flow_reply("<p class=\"flow-empty\">That parcel is not in the observed roll.</p>"); return 1;
        }
        {
            char addr[256]={0}, owner[256]={0}, city[64]={0};
            char lv[64]={0}, tv[64]={0}, yb[32]={0}, la[64]={0}, ls[64]={0};
            studio_json_field(rows, "address", addr, sizeof(addr));
            studio_json_field(rows, "owner", owner, sizeof(owner));
            studio_json_field(rows, "city", city, sizeof(city));
            studio_json_field(rows, "land_value", lv, sizeof(lv)); num_fix(lv, sizeof(lv));
            studio_json_field(rows, "total_value", tv, sizeof(tv)); num_fix(tv, sizeof(tv));
            studio_json_field(rows, "year_built", yb, sizeof(yb));
            studio_json_field(rows, "living_area", la, sizeof(la)); num_fix(la, sizeof(la));
            studio_json_field(rows, "lot_sqft", ls, sizeof(ls)); num_fix(ls, sizeof(ls));
            {
                char e_addr[512], e_owner[512];
                html_escape(addr, e_addr, sizeof(e_addr));
                html_escape(owner, e_owner, sizeof(e_owner));
                w = snprintf(body, sizeof(body),
                  "<article class=\"flow-parcel\">"
                  "<p class=\"eyebrow\"><i></i>Step 2 &middot; this parcel</p>"
                  "<h3>%s</h3>"
                  "<dl class=\"flow-facts\">"
                  "<div><dt>Account</dt><dd class=\"mono\">%s</dd></div>"
                  "<div><dt>Taxpayer of record</dt><dd>%s</dd></div>"
                  "<div><dt>Year built</dt><dd>%s</dd></div>"
                  "<div><dt>Living area</dt><dd>%s sq ft</dd></div>"
                  "<div><dt>Lot</dt><dd>%s sq ft</dd></div>"
                  "<div><dt>Land value</dt><dd>$%s</dd></div>"
                  "<div><dt>Total assessed</dt><dd>$%s</dd></div>"
                  "</dl>"
                  "<p class=\"flow-note\">This is the public assessment roll as observed, with the page digest it "
                  "came from. A taxpayer name is not the legal owner, and an assessed value is not an appraisal, "
                  "a listing price, or an offer.</p>"
                  "<div class=\"flow-next\">"
                  "<button class=\"button primary\" hx-get=\"/property/market?city=%s&amp;account=%s\" "
                  "hx-target=\"#flow-step3\" hx-swap=\"innerHTML\">"
                  "See what this market looks like <span>&rarr;</span></button>"
                  "</div></article><div id=\"flow-step3\"></div>",
                  e_addr[0] ? e_addr : "(no situs address on the roll)",
                  acct, e_owner[0] ? e_owner : "&mdash;",
                  yb[0]?yb:"&mdash;", la[0]?la:"&mdash;", ls[0]?ls:"&mdash;",
                  lv[0]?lv:"&mdash;", tv[0]?tv:"&mdash;", city, acct);
                (void)w;
                flow_reply(body);
            }
        }
        return 1;
    }

    /* Step 3: the market this parcel sits in, from the same estate. */
    if (!strncmp(target, "/property/market", 16)) {
        char city[64], acct[64];
        if (!query_value(query, "city", raw, sizeof(raw)) || !flow_safe_term(raw, city, sizeof(city))) {
            flow_reply("<p class=\"flow-empty\">No market selected.</p>"); return 1;
        }
        if (!query_value(query, "account", raw, sizeof(raw)) || !flow_safe_term(raw, acct, sizeof(acct)))
            acct[0] = '\0';
        snprintf(sql, sizeof(sql),
          "WITH p AS (SELECT "
          " TRY_CAST(json_extract_string(observed,'$.Land_Value') AS DOUBLE) AS lv, "
          " TRY_CAST(json_extract_string(observed,'$.Improvemen') AS DOUBLE) AS iv, "
          " TRY_CAST(json_extract_string(observed,'$.Total_Valu') AS DOUBLE) AS tv, "
          " TRY_CAST(json_extract_string(observed,'$.Land_SqFt') AS DOUBLE) AS sqft "
          " FROM observation WHERE src_id='src:tad-parcel-view' "
          " AND trim(json_extract_string(observed,'$.City'))='%s') "
          "SELECT count(*) AS parcels, "
          " count(*) FILTER (WHERE iv=0 AND lv>0) AS vacant, "
          " median(lv) FILTER (WHERE iv=0 AND lv>0 AND sqft BETWEEN 3000 AND 20000) AS land_median, "
          " median(tv) FILTER (WHERE tv>0) AS exit_median FROM p", city);
        if (fleming_query(bindir, sql, rows, sizeof(rows)) != 0 || rows[0] != '[') {
            flow_reply("<p class=\"flow-empty\">That market has no observed comparables.</p>"); return 1;
        }
        {
            char parcels[32]={0}, vacant[32]={0}, lm[64]={0}, em[64]={0};
            studio_json_field(rows, "parcels", parcels, sizeof(parcels));
            studio_json_field(rows, "vacant", vacant, sizeof(vacant));
            studio_json_field(rows, "land_median", lm, sizeof(lm));
            studio_json_field(rows, "exit_median", em, sizeof(em));
            snprintf(body, sizeof(body),
              "<article class=\"flow-market\">"
              "<p class=\"eyebrow\"><i></i>Step 3 &middot; the market around it</p>"
              "<h3>City code %s</h3>"
              "<dl class=\"flow-facts\">"
              "<div><dt>Parcels observed here</dt><dd>%s</dd></div>"
              "<div><dt>Vacant with land value</dt><dd>%s</dd></div>"
              "<div><dt>Median land value</dt><dd>$%s</dd></div>"
              "<div><dt>Median assessed total</dt><dd>$%s</dd></div>"
              "</dl>"
              "<p class=\"flow-note\">A median across a city is a market anchor. It is not a comparable sale, a "
              "valuation of the parcel above, or a price anyone has offered.</p>"
              "<div class=\"flow-next\">"
              "<button class=\"button primary\" hx-get=\"/property/act?account=%s\" "
              "hx-target=\"#flow-step4\" hx-swap=\"innerHTML\">What can I do with this? <span>&rarr;</span></button>"
              "</div></article><div id=\"flow-step4\"></div>",
              city, parcels[0]?parcels:"0", vacant[0]?vacant:"0",
              lm[0]?lm:"&mdash;", em[0]?em:"&mdash;", acct);
            flow_reply(body);
        }
        return 1;
    }

    /* Step 4: what this person can actually do next, given who they are. */
    if (!strncmp(target, "/property/act", 13)) {
        char acct[64];
        int signed_in = session_valid(bearer, auth_db);
        int may_write = signed_in && session_authorized(bearer, auth_db, "write");
        if (!query_value(query, "account", raw, sizeof(raw)) || !flow_safe_term(raw, acct, sizeof(acct)))
            acct[0] = '\0';
        snprintf(body, sizeof(body),
          "<article class=\"flow-act\">"
          "<p class=\"eyebrow\"><i></i>Step 4 &middot; what happens next</p>"
          "<h3>Act on %s</h3>"
          "%s"
          "<p class=\"flow-note\">Watching a parcel opens a durable mission in the work graph. It is not an offer, "
          "an option, a lien position, or any claim on the property.</p>"
          "</article>",
          acct[0]?acct:"this parcel",
          may_write
            ? "<p>You can open work on this parcel. It becomes a mission someone can claim and complete.</p>"
              "<div class=\"flow-next\"><button class=\"button primary\" data-watch=\"1\">"
              "Watch this parcel <span>&rarr;</span></button></div>"
            : (signed_in
               ? "<p>Your account can read this estate but is not authorized to open work. "
                 "A write-scoped account can watch parcels and open opportunities.</p>"
               : "<p>Reading is open to everyone. To open work on a parcel, "
                 "<a class=\"text-link\" href=\"/login\">sign in</a> with a write-scoped account.</p>"));
        flow_reply(body);
        return 1;
    }

    /* Step 1: search. */
    if (!strcmp(target, "/property") || !strcmp(target, "/property/") ||
        !strncmp(target, "/property/search", 16)) {
        int searching = !strncmp(target, "/property/search", 16);
        if (searching) {
            if (!query_value(query, "q", raw, sizeof(raw)) || !flow_safe_term(raw, term, sizeof(term))) {
                flow_reply("<p class=\"flow-empty\">Type an address or an account number.</p>"); return 1;
            }
            snprintf(sql, sizeof(sql),
              "SELECT json_extract_string(observed,'$.Account_Nu') AS account, "
              "json_extract_string(observed,'$.Situs_Addr') AS address, "
              "json_extract_string(observed,'$.City') AS city, "
              "TRY_CAST(json_extract_string(observed,'$.Total_Valu') AS DOUBLE) AS total_value "
              "FROM observation WHERE src_id='src:tad-parcel-view' AND ("
              " upper(json_extract_string(observed,'$.Situs_Addr')) LIKE upper('%%%s%%') "
              " OR trim(json_extract_string(observed,'$.Account_Nu'))='%s') LIMIT 25", term, term);
            if (fleming_query(bindir, sql, rows, sizeof(rows)) != 0 || rows[0] != '[') {
                flow_reply("<p class=\"flow-empty\">Nothing in the observed roll matches that.</p>"); return 1;
            }
            if (!strcmp(rows, "[]") || strlen(rows) < 4) {
                flow_reply("<p class=\"flow-empty\">No parcel matches that address or account. "
                           "The roll covers Tarrant County as observed.</p>");
                return 1;
            }
            /* Render each match as a row that leads to step 2. */
            w = snprintf(body, sizeof(body), "<ul class=\"flow-results\">");
            {
                const char *p = rows;
                while ((p = strstr(p, "{\"account\"")) && (size_t)w < sizeof(body) - 2048) {
                    char acc[64]={0}, addr[256]={0}, city[64]={0}, tv[64]={0};
                    char e_addr[512];
                    const char *rec_end = strchr(p, '}');
                    char rec[2048];
                    size_t n = rec_end ? (size_t)(rec_end - p + 1) : 0;
                    if (!n || n >= sizeof(rec)) break;
                    memcpy(rec, p, n); rec[n] = '\0';
                    studio_json_field(rec, "account", acc, sizeof(acc));
                    studio_json_field(rec, "address", addr, sizeof(addr));
                    studio_json_field(rec, "city", city, sizeof(city));
                    studio_json_field(rec, "total_value", tv, sizeof(tv));
                    html_escape(addr, e_addr, sizeof(e_addr));
                    w += snprintf(body + w, sizeof(body) - (size_t)w,
                      "<li><button class=\"flow-result\" hx-get=\"/property/parcel?account=%s\" "
                      "hx-target=\"#flow-step2\" hx-swap=\"innerHTML\">"
                      "<span class=\"fr-addr\">%s</span>"
                      "<span class=\"fr-meta mono\">acct %s &middot; city %s &middot; assessed $%s</span>"
                      "</button></li>", acc, e_addr[0]?e_addr:"(no situs address)", acc,
                      city[0]?city:"&mdash;", tv[0]?tv:"&mdash;");
                    p = rec_end + 1;
                }
            }
            snprintf(body + w, sizeof(body) - (size_t)w, "</ul><div id=\"flow-step2\"></div>");
            flow_reply(body);
            return 1;
        }
    }
    return 0;
}

/* ── the market flow ─────────────────────────────────────────────────────
 *
 * x402 and MPP compete here rather than being listed side by side: the same
 * capability is quoted through every commerce grammar at once, and the point of
 * the page is that the numbers have to agree. A buyer sees what an agent would
 * be charged; a seller sees what their listing actually exposes.
 *
 * Rendered as HTML fragments for htmx. The machine views remain at /capability,
 * /mpp/quote, /ucp/catalog, /acp/products and /beckn/search.
 */
static int market_capability_row(const char *name, const char *summary,
                                 char *out, size_t cap, size_t used)
{
    double usd = 0.0;
    int priced = x402_price_usd(name, &usd);
    return (int)used + snprintf(out + used, cap - used,
      "<article class=\"mk-cap%s\">"
      "<div class=\"mk-head\"><h3>%s</h3>%s</div>"
      "<p class=\"mk-desc\">%s</p>"
      "<dl class=\"mk-grammars\">"
        "<div><dt>x402</dt><dd>%s</dd></div>"
        "<div><dt>MPP</dt><dd>%s</dd></div>"
        "<div><dt>UCP</dt><dd>%s</dd></div>"
        "<div><dt>ACP</dt><dd>%s</dd></div>"
        "<div><dt>Beckn</dt><dd>%s</dd></div>"
      "</dl>"
      "<p class=\"mk-note\">%s</p>"
      "<div class=\"flow-next\">"
        "<button class=\"button quiet\" hx-get=\"/market/quote?capability=%s\" "
        "hx-target=\"#mk-detail\" hx-swap=\"innerHTML\">What an agent is charged <span>&rarr;</span></button>"
      "</div></article>",
      priced ? "" : " is-unlisted",
      name,
      priced ? "<span class=\"pill published\">listed</span>"
             : "<span class=\"pill draft\">not for sale</span>",
      summary,
      priced ? "402 + requirements" : "not_for_sale",
      priced ? "quotes" : "quotes 0.00",
      priced ? "in catalog" : "listed, unpriced",
      priced ? "in products" : "listed, unpriced",
      priced ? "in on_search" : "listed, unpriced",
      priced
        ? "Every grammar above quotes the same number, because they read one service book. "
          "A quote is not a reservation, and the resource answers 402 until a payment verifies."
        : "This capability is discoverable but has no sell price, so it is not for sale. "
          "That is different from being free.",
      name);
}

static int serve_market_flow(const char *method, const char *target, const char *query,
                             const char *bearer, const char *auth_db)
{
    static char body[262144];
    size_t w = 0;

    if (strncmp(target, "/market", 7)) return 0;
    if (strcmp(method, "GET")) return 0;

    /* Step 2: one capability, priced through every rail at once. */
    if (!strncmp(target, "/market/quote", 13)) {
        char name[128];
        const X402Capability *cap;
        double usd = 0.0;
        int priced;
        if (!query_value(query, "capability", name, sizeof(name)) || !studio_safe_ident(name) ||
            !(cap = x402_find(name))) {
            flow_reply("<p class=\"flow-empty\">Unknown capability.</p>"); return 1;
        }
        priced = x402_price_usd(cap->name, &usd);
        {
            long long base = (long long)(usd * 1000000.0 + 0.5);
            int signed_in = session_valid(bearer, auth_db);
            int may_write = signed_in && session_authorized(bearer, auth_db, "write");
            snprintf(body, sizeof(body),
              "<article class=\"mk-detail\">"
              "<p class=\"eyebrow\"><i></i>Step 2 &middot; what an agent is charged</p>"
              "<h3>%s</h3>"
              "%s"
              "<dl class=\"flow-facts\">"
                "<div><dt>Price</dt><dd>%s</dd></div>"
                "<div><dt>Base units</dt><dd class=\"mono\">%lld</dd></div>"
                "<div><dt>Scheme</dt><dd class=\"mono\">exact</dd></div>"
                "<div><dt>Network</dt><dd class=\"mono\">%s</dd></div>"
                "<div><dt>Settlement</dt><dd>%s</dd></div>"
              "</dl>"
              "<p class=\"mk-note\">%s</p>"
              "%s"
              "</article>",
              cap->name,
              priced ? "" : "<p class=\"mk-note\">No sell price is set, so this capability answers "
                            "<code>not_for_sale</code> rather than a payment demand.</p>",
              priced ? "listed" : "&mdash;", base, x402_network(),
              x402_facilitator()[0] ? "a facilitator is configured"
                                    : "no facilitator configured, so nothing can settle",
              "An unverified X-PAYMENT never delivers the goods. Without a configured facilitator this "
              "resource answers 402 payment_unverifiable rather than treating a claim of payment as revenue.",
              may_write
                ? "<div class=\"flow-next\"><button class=\"button primary\" data-price=\"1\">"
                  "Change what this costs <span>&rarr;</span></button></div>"
                : (signed_in
                   ? "<p class=\"mk-note\">Your account can read this market but is not authorized to price it.</p>"
                   : "<p class=\"mk-note\">Pricing a capability needs a write-scoped account. "
                     "<a class=\"text-link\" href=\"/login\">Sign in <span>&rarr;</span></a></p>"));
            flow_reply(body);
        }
        return 1;
    }

    /* Step 1: the whole market, every capability through every grammar. */
    if (!strncmp(target, "/market/list", 12)) {
        w = (size_t)snprintf(body, sizeof(body), "<div class=\"mk-grid\">");
        for (int i = 0; i < X402_CATALOG_N && w < sizeof(body) - 4096; i++)
            w = (size_t)market_capability_row(X402_CATALOG[i].name, X402_CATALOG[i].summary,
                                              body, sizeof(body), w);
        snprintf(body + w, sizeof(body) - w,
          "</div><div id=\"mk-detail\"></div>");
        flow_reply(body);
        return 1;
    }
    return 0;
}

/* ── the sources flow ────────────────────────────────────────────────────
 *
 * CAP, EPCIS, WoT, OpenRTB and the social plane are empty for one reason:
 * nothing has been admitted to observe. This flow is the thing that changes
 * that, so it shows each protocol's world, what it is waiting for, and the one
 * act that fills it -- admitting a source under the foreign law.
 */
static int sources_admitted(const char *prefix) {
    const char *dir = getenv("BONFYRE_FOREIGN_RECEIPTS");
    BfReceiptDir *d; struct dirent *e; int n = 0;
    if (!dir || !*dir) dir = "/var/lib/bonfyre/foreign/receipts";
    d = bf_receipt_opendir(dir);
    if (!d) return 0;
    while ((e = bf_receipt_readdir(d))) {
        if (e->d_name[0] == '.') continue;
        if (!prefix || !*prefix || !strncmp(e->d_name, prefix, strlen(prefix))) n++;
    }
    bf_receipt_closedir(d);
    return n;
}

static int serve_sources_flow(const char *method, const char *target, const char *query,
                              const char *bearer, const char *auth_db)
{
    (void)query;   /* these steps take no parameters yet */
    static char body[262144];
    size_t w = 0;

    if (strncmp(target, "/sources", 8)) return 0;
    if (strcmp(method, "GET")) return 0;

    if (!strncmp(target, "/sources/list", 13)) {
        struct { const char *proto, *world, *waiting, *at; } worlds[] = {
          {"CAP 1.2","Hazards and public alerts",
           "a public alert feed with a declared observe right","/cap/alerts"},
          {"GS1 EPCIS 2.0","Product identity and provenance",
           "a GS1-identified catalogue or an EPCIS capture endpoint","/epcis/events"},
          {"W3C WoT","Physical things and their state",
           "a Thing Description or a device that publishes one","/wot/thing"},
          {"OpenRTB 2.6","Attention and demand",
           "registered inventory and a recorded consent basis","/openrtb/bidrequest"},
          {"ActivityPub","The social plane",
           "an actor to follow, or followers of this one","/ap/actor"},
          {"OGC API Features","Places and geometry",
           "a source that carries coordinates; the parcel roll carries area only","/ogc"},
        };
        int admitted = sources_admitted(NULL);
        int signed_in = session_valid(bearer, auth_db);
        int may_write = signed_in && session_authorized(bearer, auth_db, "write");

        w = (size_t)snprintf(body, sizeof(body),
          "<p class=\"flow-hint\">%d source%s admitted so far. Each world below is empty until one is.</p>"
          "<div class=\"mk-grid\">", admitted, admitted == 1 ? "" : "s");
        for (size_t i = 0; i < sizeof(worlds)/sizeof(worlds[0]) && w < sizeof(body) - 4096; i++) {
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<article class=\"mk-cap is-unlisted\">"
              "<div class=\"mk-head\"><h3>%s</h3><span class=\"pill draft\">waiting</span></div>"
              "<p class=\"mk-desc\">%s</p>"
              "<p class=\"mk-note\">Waiting on %s.</p>"
              "<div class=\"flow-next\"><a class=\"button quiet\" href=\"%s\">See it as data <span>&rarr;</span></a></div>"
              "</article>",
              worlds[i].proto, worlds[i].world, worlds[i].waiting, worlds[i].at);
        }
        snprintf(body + w, sizeof(body) - w,
          "</div>"
          "<article class=\"mk-detail\">"
          "<p class=\"eyebrow\"><i></i>The one act that fills them</p>"
          "<h3>Admit a source</h3>"
          "<p class=\"mk-desc\">Point at a public URL. robots.txt is checked before anything is fetched, the "
          "exact bytes are retained with a receipt and a content digest, and the result never becomes canonical "
          "truth. Images, fonts and themes come in the same way.</p>"
          "%s"
          "<p class=\"mk-note\">A cached projection is not a licence, and an observation is not a verified fact "
          "about the world. A source excluded by robots is receipted without its URL ever being fetched.</p>"
          "</article>",
          may_write
            ? "<form class=\"flow-search\" id=\"admit\">"
              "<input type=\"text\" name=\"url\" placeholder=\"https://example.org/feed\" required>"
              "<button class=\"button primary\" type=\"button\" data-admit=\"1\">Observe it <span>&rarr;</span></button>"
              "</form><p class=\"mk-note\" data-admit-status></p>"
            : (signed_in
               ? "<p class=\"mk-note\">Your account can read what has been admitted but is not authorized to admit a source.</p>"
               : "<p class=\"mk-note\">Admitting a source needs a write-scoped account. "
                 "<a class=\"text-link\" href=\"/login\">Sign in <span>&rarr;</span></a></p>"));
        flow_reply(body);
        return 1;
    }
    return 0;
}

/* ── the agents flow ─────────────────────────────────────────────────────
 *
 * MCP past tools/list, A2A as a bridge into the work graph, CloudEvents as the
 * envelope that carries what happened back out. Shown as one loop, because that
 * is what it is: an agent discovers a skill, opens work, and the work becomes
 * an event anyone can subscribe to.
 */
static int serve_agents_flow(const char *method, const char *target, const char *query,
                             const char *bearer, const char *auth_db)
{
    (void)query;   /* these steps take no parameters yet */
    static char body[262144], rows[131072];
    /* sql holds two PATH_MAX paths plus the statement, so it is sized for them
     * rather than for the statement alone. */
    char workdb[PATH_MAX], qbin[PATH_MAX], sql[PATH_MAX * 3], diag[1024];
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    int ready = 0, running = 0, complete = 0;

    if (strncmp(target, "/agents", 7)) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";

    if (!strncmp(target, "/agents/list", 12)) {
        int signed_in = session_valid(bearer, auth_db);
        int may_write = signed_in && session_authorized(bearer, auth_db, "write");
        snprintf(qbin, sizeof(qbin), "%s/bonfyre-query", bindir);
        snprintf(workdb, sizeof(workdb), "%s/fleming-work.db", fleming_root());
        if (access(qbin, X_OK) == 0 && access(workdb, R_OK) == 0) {
            snprintf(sql, sizeof(sql),
              "SET home_directory='%s'; INSTALL sqlite; LOAD sqlite; "
              "ATTACH '%s' AS w (TYPE sqlite, READ_ONLY); "
              "SELECT status, count(*) AS n FROM w.workgraph_nodes WHERE mission_id NOT LIKE 'account-%%' GROUP BY status",
              duckdb_home(), workdb);
            {
                char *a[] = { qbin, (char*)"sql", (char*)":memory:", sql, NULL };
                if (auth_call_split(a, NULL, rows, sizeof(rows), diag, sizeof(diag)) == 0) {
                    const char *p = rows;
                    while ((p = strstr(p, "\"status\":\""))) {
                        char st[32] = {0}, cnt[16] = {0};
                        const char *rec_end = strchr(p, '}');
                        char rec[256];
                        size_t n = rec_end ? (size_t)(rec_end - p + 1) : 0;
                        if (!n || n >= sizeof(rec)) break;
                        memcpy(rec, p, n); rec[n] = '\0';
                        studio_json_field(rec, "status", st, sizeof(st));
                        studio_json_field(rec, "n", cnt, sizeof(cnt));
                        if (!strcmp(st, "ready")) ready = atoi(cnt);
                        else if (!strcmp(st, "running")) running = atoi(cnt);
                        else if (!strcmp(st, "complete")) complete = atoi(cnt);
                        p = rec_end + 1;
                    }
                }
            }
        }
        snprintf(body, sizeof(body),
          "<div class=\"mk-grid\">"
          "<article class=\"mk-cap\"><div class=\"mk-head\"><h3>1 &middot; Discover</h3>"
          "<span class=\"pill published\">live</span></div>"
          "<p class=\"mk-desc\">An agent card advertises what this world can be asked to do. MCP goes past "
          "listing tools: seven of them write.</p>"
          "<dl class=\"mk-grammars\"><div><dt>A2A</dt><dd>agent card</dd></div>"
          "<div><dt>MCP</dt><dd>10 studio tools, 7 write</dd></div></dl>"
          "<div class=\"flow-next\"><a class=\"button quiet\" href=\"/.well-known/agent.json\">"
          "See the card <span>&rarr;</span></a></div></article>"

          "<article class=\"mk-cap\"><div class=\"mk-head\"><h3>2 &middot; Open work</h3>"
          "<span class=\"pill %s\">%s</span></div>"
          "<p class=\"mk-desc\">A skill call becomes a durable mission. It can be claimed under lease, "
          "completed only by the lease holder, escalated, or superseded.</p>"
          "<dl class=\"flow-facts\">"
          "<div><dt>Ready</dt><dd>%d</dd></div>"
          "<div><dt>Running</dt><dd>%d</dd></div>"
          "<div><dt>Complete</dt><dd>%d</dd></div></dl>"
          "%s</article>"

          "<article class=\"mk-cap\"><div class=\"mk-head\"><h3>3 &middot; It becomes an event</h3>"
          "<span class=\"pill published\">live</span></div>"
          "<p class=\"mk-desc\">Everything the work graph does leaves a CloudEvents record, so a system that "
          "never spoke to this one can still follow what happened.</p>"
          "<dl class=\"mk-grammars\"><div><dt>CloudEvents</dt><dd>1.0 batch</dd></div></dl>"
          "<div class=\"flow-next\"><a class=\"button quiet\" href=\"/events?limit=25\">"
          "Read the stream <span>&rarr;</span></a></div></article>"
          "</div>"
          "<p class=\"mk-note\">An advertised skill is not an agreement to perform it, and an agent holding a "
          "write verb still never crosses a human submit line.</p>",
          may_write ? "published" : "draft",
          may_write ? "you can open work" : "read only",
          ready, running, complete,
          may_write
            ? "<div class=\"flow-next\"><a class=\"button quiet\" href=\"/property\">"
              "Open work from a parcel <span>&rarr;</span></a></div>"
            : (signed_in
               ? "<p class=\"mk-note\">Your account reads this graph but cannot open work.</p>"
               : "<p class=\"mk-note\"><a class=\"text-link\" href=\"/login\">Sign in <span>&rarr;</span></a> "
                 "with a write-scoped account to open work.</p>"));
        flow_reply(body);
        return 1;
    }
    return 0;
}

/* ── SUBJECT and APERTURE ────────────────────────────────────────────────
 *
 * Source E's law: touch the object, understand the object, then offer the
 * operations that pertain to it. Not navigation to a feature to a form.
 *
 * The aperture is computed, never a static menu. For one subject it asks, in
 * order: what is this, what could exist for it, what is reachable right now,
 * what can THIS viewer do, what are they authorised to do, and what does it
 * cost. Two to five actions come out, each with a strength that drives its
 * visual weight -- so the hierarchy on screen is a projection of closure, and
 * two people looking at the same subject correctly see different apertures.
 */
typedef struct {
    const char *label;
    const char *hint;
    char href[256];
    int strength;        /* 3 strongest, 2 strong, 1 ordinary */
    int is_write;
} Action;

#endif /* BONFYRE_PRIVATE_SESSION_AND_FLOW_ROUTES_H */
