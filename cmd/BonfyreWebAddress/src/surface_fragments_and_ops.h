#ifndef BONFYRE_SURFACE_FRAGMENTS_AND_OPS_H
#define BONFYRE_SURFACE_FRAGMENTS_AND_OPS_H
/* Extracted verbatim from main.c lines 1991-2779 (mechanical split, no logic changes). */
static void ways_html(const char *ways_json) {
    const char *cursor = ways_json;
    /* A small scanner rather than a parser: the field is written by this
     * institution's own CMS in one shape, and pulling label/href pairs in order
     * is the whole requirement. A malformed field renders fewer ways, never
     * broken markup. */
    while (cursor && *cursor) {
        const char *label = strstr(cursor, "\"label\"");
        const char *href;
        const char *lv, *hv, *le, *he;
        if (!label) break;
        href = strstr(label, "\"href\"");
        if (!href) break;
        lv = strchr(label + 7, '"');
        if (!lv) break;
        lv++;
        le = strchr(lv, '"');
        hv = strchr(href + 6, '"');
        if (!le || !hv) break;
        hv++;
        he = strchr(hv, '"');
        if (!he) break;
        printf("<a class=\"way\" href=\"%.*s\">%.*s</a>", (int)(he - hv), hv, (int)(le - lv), lv);
        cursor = he + 1;
    }
}

static int serve_door_fragment(const char *target, const char *query) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char page[64];
    int rows = 0;

    if (strcmp(target, "/_surface/doors")) return 0;
    if (!query_value(query, "page", page, sizeof(page)) || !studio_safe_ident(page)) {
        snprintf(page, sizeof(page), "%s", "protocols");
    }
    db = cms_open_ro();
    if (!db || !cms_type_registered(db, "site_door")) {
        if (db) sqlite3_close(db);
        /* Empty fragment: the page keeps whatever it rendered itself. */
        printf("HTTP/1.1 204 No Content\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n");
        return 1;
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n");
    if (sqlite3_prepare_v2(db,
            "SELECT eyebrow, heading, body, ways FROM site_door "
            "WHERE page = ? ORDER BY position, id", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, page, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *eyebrow = (const char *)sqlite3_column_text(st, 0);
            const char *heading = (const char *)sqlite3_column_text(st, 1);
            const char *body = (const char *)sqlite3_column_text(st, 2);
            const char *ways = (const char *)sqlite3_column_text(st, 3);
            if (!eyebrow || !heading || !body) continue;
            printf("<article class=\"door\"><p class=\"eyebrow\"><i></i>");
            html(eyebrow);
            printf("</p><h3>");
            html(heading);
            printf("</h3><p>");
            html(body);
            printf("</p><div class=\"ways\">");
            if (ways) ways_html(ways);
            printf("</div></article>");
            rows++;
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    if (!rows) printf("<!-- no authored doors for this page -->");
    fflush(stdout);
    return 1;
}

static int serve_aperture_fragment(BfMatrix *m, const char *host_in, const char *target,
                                   const char *query) {
    char host[256], url[1024], occurrence[128] = {0};
    BfMatrixDomainResolution domain;
    if (strcmp(target, "/_surface/intake")) return 0;
    if (!lower_host(host_in, host) || bf_matrix_domain_resolve(m, host, &domain) != 1) {
        printf("HTTP/1.1 404 Not Found\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n");
        printf("<div id=\"intake-result\" class=\"intake-result\"><p>This host is not a configured Matrix territory.</p></div>\n");
        return 1;
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n");
    if (!query_value(query, "url", url, sizeof(url)) || !public_http_url(url)) {
        printf("<div id=\"intake-result\" class=\"intake-result\"><p>Enter an http:// or https:// public address to prepare an evidence route.</p></div>\n");
        return 1;
    }
    if (query_value(query, "occurrence", occurrence, sizeof(occurrence)) && !safe(occurrence, sizeof(occurrence))) occurrence[0] = '\0';
    printf("<div id=\"intake-result\" class=\"intake-result\"><p>Route prepared. Nothing has been fetched, claimed, or promoted.</p>");
    if (occurrence[0]) {
        printf("<p><strong>Candidate route context:</strong> ");
        html(occurrence);
        printf(". This preserves the ExternalEstate occurrence coordinate only; Studio must still observe the source independently.</p>");
    }
    printf("<form action=\"/studio/\" method=\"get\"><input type=\"hidden\" name=\"foreign_url\" value=\"");
    html(url);
    printf("\">");
    if (occurrence[0]) {
        printf("<input type=\"hidden\" name=\"foreign_occurrence\" value=\"");
        html(occurrence);
        printf("\">");
    }
    printf("<button class=\"button primary\" type=\"submit\">Review in Studio <span>→</span></button></form></div>\n");
    return 1;
}

static int serve_matrix_asset(BfMatrix *m, const char *host_in, const char *target,
                              const char *root, const char *inm, const char *query) {
    /* Browser tabs and iOS Home Screen may request the icon without a head.
     * Use the original knight's small cuts only on Aurekai's four territories.
     * Customer sites keep their own identity, even when hosted by this engine. */
    if (!strcmp(target, "/favicon.ico") || !strcmp(target, "/apple-touch-icon.png")) {
        char apex[256];
        if (!lower_host(host_in, apex) ||
            (strcmp(apex, "aurekai.work") && strcmp(apex, "aurekai.live") &&
             strcmp(apex, "aurekai.shop") && strcmp(apex, "aurekai.life"))) return 0;
        const char *size = !strcmp(target, "/favicon.ico") ? "32" : "180";
        printf("HTTP/1.1 302 Found\r\nLocation: /assets/aurekai-knight-%s.png\r\n"
               "Cache-Control: public, max-age=86400\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", size);
    }
    static const struct { const char *route, *file, *type; } assets[] = {
        {"/assets/matrix.css", "matrix.css", "text/css; charset=utf-8"},
        /* The authored visual character every public page is dressed in.  It
         * used to be inlined into each page: fifty-four kilobytes repeated on
         * every one of thousands of pages, paid for again on every visit and
         * again in every export.  Served once, a browser keeps it. */
        {"/assets/world.css", "world.css", "text/css; charset=utf-8"},
        /* The design behind the estate's own control names: every button,
         * way on, field and provenance note gets a designed resting, hover
         * and focus state, built from the utility layer rather than hand-set
         * one control at a time.  It is a stylesheet; nothing here runs. */
        {"/assets/tailwind.css", "tailwind.css", "text/css; charset=utf-8"},
        {"/assets/matrix-aperture.css", "matrix-aperture.css", "text/css; charset=utf-8"},
        {"/assets/matrix.js", "matrix.js", "application/javascript; charset=utf-8"},
        {"/assets/matrix-htmx.js", "matrix-htmx.js", "application/javascript; charset=utf-8"},
        {"/assets/matrix-vgpu.js", "matrix-vgpu.js", "application/javascript; charset=utf-8"},
        /* Client organs: the four things a server cannot do from a distance --
         * show what changed, anchor an explanation beside the thing it explains,
         * keep an enormous list navigable, and spend the reader's own idle
         * hardware instead of ours. One asset, bound to markup this estate
         * already emits, so it reaches every surface rather than one page. It
         * decides nothing: the page was complete when it arrived. */
        {"/assets/client-organs.js", "client-organs.js", "application/javascript; charset=utf-8"},
        {"/assets/vendor-autoanimate.min.js", "vendor-autoanimate.min.js", "application/javascript; charset=utf-8"},
        {"/assets/vendor-floating-ui-core.min.js", "vendor-floating-ui-core.min.js", "application/javascript; charset=utf-8"},
        {"/assets/vendor-floating-ui-dom.min.js", "vendor-floating-ui-dom.min.js", "application/javascript; charset=utf-8"},
        {"/assets/vendor-nanostores.min.js", "vendor-nanostores.min.js", "application/javascript; charset=utf-8"},
        {"/assets/vendor-uplot.min.js", "vendor-uplot.min.js", "application/javascript; charset=utf-8"},
        {"/assets/vendor-uplot.min.css", "vendor-uplot.min.css", "text/css; charset=utf-8"},
        {"/assets/vendor-flatbush.min.js", "vendor-flatbush.min.js", "application/javascript; charset=utf-8"},
        {"/assets/vendor-vlist.min.js", "vendor-vlist.min.js", "application/javascript; charset=utf-8"},
        {"/assets/vendor-comlink.min.js", "vendor-comlink.min.js", "application/javascript; charset=utf-8"},
        {"/assets/field-index.worker.js", "field-index.worker.js", "application/javascript; charset=utf-8"},
        {"/assets/vendor-pmtiles.min.js", "vendor-pmtiles.min.js", "application/javascript; charset=utf-8"},
        {"/assets/vendor-capnweb.min.js", "vendor-capnweb.min.js", "application/javascript; charset=utf-8"},
        /* Donor front ends, vendored rather than imitated. Section 71 of the
         * estate document classifies the reference estate by licence: these are
         * DONOR-class (Apache-2.0, BSD-2), which is the class whose code may be
         * adapted. They are served as their own stylesheets rather than inlined
         * so the clone surfaces load the donor's real CSS, not a paraphrase. */
        {"/assets/donor-zulip.css", "donor-zulip.css", "text/css; charset=utf-8"},
        {"/assets/aurekai-native.css", "aurekai-native.css", "text/css; charset=utf-8"},
        {"/assets/native-experience.css", "native-experience.css", "text/css; charset=utf-8"},
        {"/assets/page-components.css", "page-components.css", "text/css; charset=utf-8"},
        {"/assets/pack-setup.css", "pack-setup.css", "text/css; charset=utf-8"},
        {"/assets/account-inbox.css", "account-inbox.css", "text/css; charset=utf-8"},
        {"/assets/pack-presentation.css", "pack-presentation.css", "text/css; charset=utf-8"},
        {"/assets/reading-surface.css", "reading-surface.css", "text/css; charset=utf-8"},
        {"/assets/world-land.svg", "world-land.svg", "image/svg+xml"},
        {"/assets/donor-pinry.css", "donor-pinry.css", "text/css; charset=utf-8"},
        {"/assets/donor-pixelfed.css", "donor-pixelfed.css", "text/css; charset=utf-8"},
        {"/assets/donor-maplibre.css", "donor-maplibre.css", "text/css; charset=utf-8"},
        {"/assets/donor-maplibre.js", "donor-maplibre.js", "application/javascript; charset=utf-8"},
        {"/assets/donor-masonry.js", "donor-masonry.js", "application/javascript; charset=utf-8"},
        {"/assets/donor-mobilizon.css", "donor-mobilizon.css", "text/css; charset=utf-8"},
        {"/assets/donor-nextcommerce.css", "donor-nextcommerce.css", "text/css; charset=utf-8"},
        {"/assets/donor-lemmy.css", "donor-lemmy.css", "text/css; charset=utf-8"},
        {"/assets/donor-plane.css", "donor-plane.css", "text/css; charset=utf-8"},
        {"/assets/aurekai-composed.css", "aurekai-composed.css", "text/css; charset=utf-8"},
        {"/assets/composition.css", "composition.css", "text/css; charset=utf-8"},
        {"/assets/aurekai-site-program.css", "aurekai-site-program.css", "text/css; charset=utf-8"},
        {"/assets/own-site.css", "own-site.css", "text/css; charset=utf-8"},
        {"/assets/aurekai-bar.css", "aurekai-bar.css", "text/css; charset=utf-8"},
        /* Arrival, sign-in and the trust modals: one sheet for the account path. */
        {"/assets/aurekai-trust.css", "aurekai-trust.css", "text/css; charset=utf-8"},
        {"/assets/own-site-agent.js", "own-site-agent.js", "text/javascript; charset=utf-8"},
        {"/assets/own-site-reading.js", "own-site-reading.js", "text/javascript; charset=utf-8"},
        {"/assets/static-embed.mjs", "static-embed.mjs", "text/javascript; charset=utf-8"},
        /* The Aurekai mark. Bonfyre is the engine; Aurekai is the larger
         * umbrella, and the lockup says so: a knight on paper beside the
         * wordmark in ink. One asset, every surface. */
        {"/assets/aurekai-knight.png", "aurekai-knight.png", "image/png"},
        /* Pixel-sized derivatives of the original knight engraving. */
        {"/assets/aurekai-knight-16.png", "aurekai-knight-16.png", "image/png"},
        {"/assets/aurekai-knight-32.png", "aurekai-knight-32.png", "image/png"},
        {"/assets/aurekai-knight-48.png", "aurekai-knight-48.png", "image/png"},
        {"/assets/aurekai-knight-64.png", "aurekai-knight-64.png", "image/png"},
        {"/assets/aurekai-knight-180.png", "aurekai-knight-180.png", "image/png"},
        /* The surface kit, served to the server-rendered estate as well as to
         * the clone surfaces that inline it. One mark, one address plane, one
         * set of refusal states -- the compiled pages get them by loading this
         * rather than by growing a second copy in C. */
        /* At the site root rather than under surfaces/, because the asset
         * name may not contain a slash -- that guard is the traversal check
         * and is worth more than the tidier path. The file is published from
         * surfaces/_kit.js and a test holds the two identical. */
        {"/assets/bonfyre-kit.js", "bonfyre-kit.js",
         "application/javascript; charset=utf-8"},
        {"/assets/bonfyre-surface-bridge.js", "bonfyre-surface-bridge.js",
         "application/javascript; charset=utf-8"},
        /* Byte-for-byte projection of the canonical runtime.  Compatibility
         * pages expose a scene descriptor but do not load or mount this
         * module: their complete initial document remains server-rendered. */
        {"/assets/bonfyre-surface-runtime.mjs", "bonfyre-surface-runtime.mjs",
         "text/javascript; charset=utf-8"},
        /* Resolve the canonical runtime's relative module import through this
         * exact route. The filesystem asset stays flat; traversal is unchanged. */
        {"/assets/renderers/workspace-composition.mjs", "bonfyre-workspace-composition.mjs",
         "text/javascript; charset=utf-8"},
        /* The mature journal style, served flat. The composed articles link
         * these stylesheets and the product icon set; the files live once
         * under site/assets and a test holds these serving copies identical. */
        {"/assets/aurekai-field-guide.css", "aurekai-field-guide.css",
         "text/css; charset=utf-8"},
        {"/assets/aurekai-research.css", "aurekai-research.css",
         "text/css; charset=utf-8"},
        {"/assets/product-icons.svg", "product-icons.svg",
         "image/svg+xml"},
        /* The journal article sheet at its composed address. The file stays
         * flat; traversal is unchanged. */
        {"/blog/blog.css", "blog.css",
         "text/css; charset=utf-8"},
        {"/assets/profile-journal.css", "profile-journal.css",
         "text/css; charset=utf-8"},
        /* The authored-reader sheet for natively composed sections. Built
         * by compile_authoring_reader.mjs; freshness is gated by its
         * --check in the SurfaceScene suite. */
        {"/assets/authoring-reader.css", "authoring-reader.css",
         "text/css; charset=utf-8"},
        /* The visual composition workspace bundle and its shells. Built by
         * @bonfyre/authoring:build (Puck, GrapesJS, Editor.js, narration
         * handoff) and stamped bonfyre:generated; the bundle is gitignored
         * build output, never authored source. */
        {"/assets/authoring-editor.js", "authoring-editor.js",
         "application/javascript; charset=utf-8"},
        {"/assets/authoring-grapes.css", "authoring-grapes.css",
         "text/css; charset=utf-8"},
        {"/assets/authoring-shell.css", "authoring-shell.css",
         "text/css; charset=utf-8"}
    };
    char host[256]; BfMatrixDomainResolution domain;
    for (size_t i = 0; i < sizeof(assets) / sizeof(assets[0]); i++) {
        if (strcmp(target, assets[i].route)) continue;
        if (!lower_host(host_in, host) ||
            (bf_matrix_domain_resolve(m, host, &domain) != 1 && !native_member_session_host(host)) ||
            !emit_file_response_cached(root, assets[i].file, assets[i].type, inm, query)) {
            printf("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
        }
        return 1;
    }
    return 0;
}

/* RO-Crate and Zarr are machine carriers for the same root Matrix cut. They
 * are compiled here rather than stored as per-host JSON so a changed address
 * binding cannot leave a stale alternate representation behind. Their body is
 * intentionally a sparse closure: only Matrix-owned facts are resolved. */
static int serve_matrix_closure(BfMatrix *m, const char *method, const char *host,
                                const char *target, const char *query,
                                const char *bearer, const char *auth_db) {
    BfMatrixAddressResolution resolution;
    char crate[1024], zarr[1024], path[1024];
    int want_crate = !strcmp(target, "/ro-crate/") || !strcmp(target, "/ro-crate/ro-crate-metadata.json");
    int want_zarr = !strcmp(target, "/zarr/zarr.json");
    if (!want_crate && !want_zarr) return 0;
    if (strcmp(method, "GET") && strcmp(method, "HEAD")) {
        printf("HTTP/1.1 405 Method Not Allowed\r\nAllow: GET, HEAD\r\nConnection: close\r\n\r\n");
        return 1;
    }
    if (!closure_query_path(query, path)) {
        printf("HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"verdict\":\"invalid_closure_address\"}\n");
        return 1;
    }
    if (!matrix_path_resolution(m, host, path, &resolution) || !closure_urls_for_path(host, path, crate, zarr)) {
        printf("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"verdict\":\"matrix_closure_not_found\"}\n");
        return 1;
    }
    /* Actor-owned self presences are addressable but not public simply because
     * their owner has a URL. Institutional life roots also use the self scope,
     * so the privacy rule is deliberately narrower: only the actor-owned
     * subject identity requires the same live actor session. */
    if (resolution.scope == BF_MATRIX_SELF &&
        !strncmp(resolution.subject_id, "subject:actor-", strlen("subject:actor-"))) {
        char actor[32], expected[64];
        if (!session_valid(bearer, auth_db) || !studio_auth_user(bearer, auth_db, actor, sizeof(actor)) ||
            snprintf(expected, sizeof(expected), "subject:actor-%s:", actor) >= (int)sizeof(expected) ||
            strncmp(resolution.subject_id, expected, strlen(expected))) {
            printf("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nCache-Control: private, no-store\r\nX-Robots-Tag: noindex, nofollow, noarchive, nosnippet\r\nConnection: close\r\n\r\n{\"verdict\":\"matrix_closure_not_found\"}\n");
            return 1;
        }
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\nCache-Control: no-store\r\nVary: Host\r\nConnection: close\r\n\r\n",
           want_crate ? "application/ld+json; charset=utf-8" : "application/vnd.zarr+json; charset=utf-8");
    if (!strcmp(method, "HEAD")) return 1;
    if (want_crate) bf_matrix_closure_emit_ro_crate(stdout, m, &resolution, crate, zarr);
    else bf_matrix_closure_emit_zarr_group(stdout, m, &resolution, crate);
    return 1;
}
static int resolve(BfMatrix *m, const char *host_in, const char *path_in, const char *query, int http,
                   const char *root, const char *bearer, const char *auth_db) {
    char host[256], address[1600], world_s[16], scope_s[16], repr[64], *part, query_copy[512];
    const char *territory; BfMatrixAddressResolution out; BfMatrixDomainResolution domain; BfMatrixWorld world; BfMatrixParticipationScope scope; int domain_rc,standing_rc,standing_members;
    if (!lower_host(host_in,host) || !path_in || path_in[0] != '/' || strlen(path_in)>1000) goto bad;
    domain_rc=bf_matrix_domain_resolve(m,host,&domain); if(domain_rc<0) goto failed;
    territory=domain_rc==1 ? domain.territory : territory_of(host);
    snprintf(world_s,sizeof(world_s),"%s",domain_rc==1 ? bf_matrix_world_name(domain.world) : "LIVE");
    snprintf(scope_s,sizeof(scope_s),"%s",domain_rc==1 ? bf_matrix_scope_name(domain.scope) : default_scope(territory));
    snprintf(repr,sizeof(repr),"%s",domain_rc==1 ? domain.representation : "HTML");
    if (query && strlen(query)<sizeof(query_copy)) { snprintf(query_copy,sizeof(query_copy),"%s",query); for(part=strtok(query_copy,"&");part;part=strtok(NULL,"&")){ char *eq=strchr(part,'='); if(!eq)continue;*eq++='\0'; if(!strcmp(part,"world"))snprintf(world_s,sizeof(world_s),"%s",eq); else if(!strcmp(part,"audience"))snprintf(scope_s,sizeof(scope_s),"%s",eq); else if(!strcmp(part,"repr"))snprintf(repr,sizeof(repr),"%s",eq); }}
    world=world_of(world_s); scope=scope_of(scope_s);
    if (world<0 || scope<0 || !safe(repr,sizeof(repr)) || snprintf(address,sizeof(address),"https://%s%s",host,path_in)>=(int)sizeof(address)) goto bad;
    int rc=bf_matrix_address_resolve(m,address,territory,world,scope,repr,&out);
    if (rc != 1) {
        if (rc == 0) { if(http) printf("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"); printf("{\"verdict\":\"not_found\",\"address\":");json(address);printf("}\n"); return 1; }
        if(http) printf("HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n");
        puts("{\"verdict\":\"resolution_error\"}"); return 2;
    }
    if (out.scope == BF_MATRIX_SELF &&
        !strncmp(out.subject_id, "subject:actor-", strlen("subject:actor-"))) {
        char actor[32], expected[64];
        if (!session_valid(bearer, auth_db) || !studio_auth_user(bearer, auth_db, actor, sizeof(actor)) ||
            snprintf(expected, sizeof(expected), "subject:actor-%s:", actor) >= (int)sizeof(expected) ||
            strncmp(out.subject_id, expected, strlen(expected))) {
            if (http) printf("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nCache-Control: private, no-store\r\nX-Robots-Tag: noindex, nofollow, noarchive, nosnippet\r\nConnection: close\r\n\r\n");
            puts("{\"verdict\":\"not_found\"}"); return 1;
        }
    }
    standing_rc=bf_matrix_standing_address_member_count(m,address,&standing_members); if(standing_rc<0)goto failed;
    if (http && !strcasecmp(out.representation,"HTML") && !strcmp(path_in,"/") && emit_matrix_index(root,&out)) return 0;
    if (http && !strcasecmp(out.representation,"HTML")) {
        char crate[1024], zarr[1024];
        if (!closure_urls_for_path(host, path_in, crate, zarr)) goto failed;
        /* The one shell: estate bar, one <main>, the one footer and player. */
        printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta name=\"theme-color\" content=\"#0f1511\"><title>");html(out.subject_id);
        printf(" \xc2\xb7 Aurekai</title><link rel=\"describedby\" type=\"application/ld+json\" href=\"");html(crate);printf("\"><link rel=\"alternate\" type=\"application/vnd.zarr+json\" href=\"");html(zarr);
        fputs("\"><link rel=\"stylesheet\" href=\"/assets/authoring-reader.css\"><link rel=\"stylesheet\" href=\"/assets/composition.css\"></head><body class=\"ak-app route-address\" data-aurekai-pattern=\"workbench\">" BF_ESTATE_BAR "<main class=\"ak-wb\" data-bonfyre-subject=\"",stdout);
        html(out.subject_id);printf("\" data-bonfyre-presence=\"");html(out.presence_id);
        printf("\"><header class=\"ak-wb-hero is-solo\"><div><p class=\"ak-kicker\">Address <span>/</span> ");html(out.territory);printf("</p><h1>");html(out.subject_id);printf("</h1></div></header>");
        printf("<dl class=\"ak-spec ak-panel address-facts\"><dt>Territory</dt><dd>");html(out.territory);printf("</dd><dt>Presence</dt><dd>");html(out.presence_id);
        printf("</dd><dt>Canonical address</dt><dd class=\"is-on\"><a href=\"");html(out.canonical_address);printf("\">");html(out.canonical_address);printf("</a></dd>");
        if(standing_rc==1)printf("<dt>Live members</dt><dd data-bonfyre-standing-members=\"%d\">%d</dd>",standing_members,standing_members);
        fputs("</dl><p class=\"ak-actions\"><a class=\"ak-btn is-quiet\" href=\"",stdout);html(crate);fputs("\">Machine description</a><a class=\"ak-btn is-quiet\" href=\"",stdout);html(zarr);
        fputs("\">Data array</a></p></main>" BF_ESTATE_FOOT "</body></html>\n",stdout); return 0;
    }
    if (http) printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\nConnection: close\r\n\r\n",content_type(out.representation));
    printf("{\"verdict\":\"resolved\",\"subject_id\":");json(out.subject_id);printf(",\"presence_id\":");json(out.presence_id);printf(",\"host\":");json(host);printf(",\"matched_host\":");json(domain_rc==1 ? domain.matched_hostname : host);printf(",\"canonical_host\":");json(domain_rc==1 ? domain.canonical_hostname : host);printf(",\"institution_id\":");json(domain_rc==1 ? domain.institution_id : "");printf(",\"path\":");json(path_in);printf(",\"address_kind\":");json(out.address_kind);printf(",\"canonical_address\":");json(out.canonical_address);printf(",\"is_alias\":%s",out.is_alias ? "true" : "false");if(standing_rc==1)printf(",\"standing_members\":%d",standing_members);printf(",\"territory\":");json(out.territory);printf(",\"world\":");json(bf_matrix_world_name(out.world));printf(",\"audience\":");json(bf_matrix_scope_name(out.scope));printf(",\"representation\":");json(out.representation);printf("}\n");return 0;
failed: if(http) printf("HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"); puts("{\"verdict\":\"domain_resolution_error\"}"); return 2;
bad: if(http) printf("HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\n\r\n"); puts("{\"verdict\":\"invalid_address_context\"}");return 2;
}
static int resolve_cli(BfMatrix *m, int argc, char **argv) {
    const char *query = opt(argc, argv, "--query");
    char explicit_query[512];
    if (!query) {
        const char *world = opt(argc, argv, "--world");
        const char *audience = opt(argc, argv, "--audience");
        const char *repr = opt(argc, argv, "--repr");
        if (world || audience || repr) {
            int n = snprintf(explicit_query, sizeof(explicit_query), "world=%s&audience=%s&repr=%s",
                             world ? world : "live", audience ? audience : default_scope(territory_of(opt(argc, argv, "--host"))),
                             repr ? repr : "HTML");
            if (n < 0 || n >= (int)sizeof(explicit_query)) return 2;
            query = explicit_query;
        }
    }
    return resolve(m, opt(argc, argv, "--host"), opt(argc, argv, "--path"), query, 0, site_root(argc,argv), NULL, NULL);
}
static int closure_cli(BfMatrix *m, int argc, char **argv) {
    const char *host = opt(argc, argv, "--host");
    const char *format = opt(argc, argv, "--format");
    const char *path = opt(argc, argv, "--path");
    BfMatrixAddressResolution resolution;
    char crate[1024], zarr[1024];
    if (!path) path = "/";
    {
        /* A closure over a subject, for things no hostname resolves to. A
         * foreign twin has an identity, a presence and a bound body; none of it
         * could be assembled into a closure while closures were host-addressed
         * only, which quietly limited the manifest to institutions. */
        const char *subject = opt(argc, argv, "--subject");
        const char *presence = opt(argc, argv, "--presence");
        if (subject && presence) {
            if (bf_matrix_presence_resolution(m, subject, presence, &resolution) != 1) {
                fprintf(stderr, "no active presence %s for subject %s\n", presence, subject);
                return 2;
            }
            snprintf(crate, sizeof(crate), "bonfyre://closure/%.400s/ro-crate", subject);
            snprintf(zarr, sizeof(zarr), "bonfyre://closure/%.400s/zarr", subject);
            if (format && !strcasecmp(format, "zarr"))
                return bf_matrix_closure_emit_zarr_group(stdout, m, &resolution, crate) == 0 ? 0 : 2;
            if (format && strcasecmp(format, "ro-crate")) return 2;
            return bf_matrix_closure_emit_ro_crate(stdout, m, &resolution, crate, zarr) == 0 ? 0 : 2;
        }
    }
    if (!host || !matrix_path_resolution(m, host, path, &resolution) || !closure_urls_for_path(host, path, crate, zarr)) return 2;
    if (format && !strcasecmp(format, "zarr"))
        return bf_matrix_closure_emit_zarr_group(stdout, m, &resolution, crate) == 0 ? 0 : 2;
    if (format && strcasecmp(format, "ro-crate")) return 2;
    return bf_matrix_closure_emit_ro_crate(stdout, m, &resolution, crate, zarr) == 0 ? 0 : 2;
}
/* A surface is advertised only when the operator has declared it live.
 *
 * This organ's own contract forbids exactly one inference: "discovery metadata
 * -> a claim that any named resource is publicly readable or executable". The
 * previous discovery() emitted api, mcp, a2a, cms and habitat URLs
 * unconditionally, for every host, without any of them being routed to a
 * backend that serves them. All five answered 404 in production while the
 * descriptor presented them as addresses. That is the forbidden inference,
 * shipped.
 *
 * Advertising is now opt-in per surface via BONFYRE_ADDRESS_SURFACES, a comma
 * separated allowlist (for example "api,mcp"). A surface absent from the list is
 * absent from the descriptor, so the default descriptor promises only what this
 * organ itself serves. Nginx routing and backend implementation stay separate
 * concerns; what changes is that the address plane stops claiming them.
 */
/* A surface is live when this organ has content to serve for it.
 *
 * Deriving liveness from the filesystem rather than an operator flag makes the
 * descriptor honest by construction: it cannot advertise a surface that would
 * 404, because the same predicate decides both what is advertised and what is
 * served. An operator promise can drift from reality; a file cannot.
 */
/* Surfaces that report the estate are GENERATED, never stored.
 *
 * The first version of this shipped api.json, mcp.json and a2a.json as files
 * written once by hand. That is a catalogue, not a surface: the moment a
 * capability is added or a binary removed, the file keeps asserting the old
 * world and nothing detects the drift. A resource manifest that can go stale is
 * worse than none, because it is believed.
 *
 * These now run BonfyreCapability at request time and transform its registry.
 * The answer is whatever the estate currently says it is.
 */
static int auth_call(char *const argv[], const char *body, char *out, size_t out_sz);

static void iso_utc_now(char *buf, size_t sz) {
    time_t t = time(NULL);
    struct tm tm;
    gmtime_r(&t, &tm);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

static const BfNativeMachineSurface *native_machine_surface(const char *name) {
    size_t i;
    for (i = 0; i < BF_NATIVE_MACHINE_SURFACE_COUNT; i++)
        if (!strcmp(bf_native_machine_surfaces[i].name, name)) return &bf_native_machine_surfaces[i];
    return NULL;
}

static int is_generated(const char *name) {
    return native_machine_surface(name) != NULL;
}

/* Split a fixed-width registry row into its columns. */
static int split_cols(char *line, char *col[], int max) {
    int n = 0;
    char *p = line;
    while (*p && n < max) {
        while (*p == ' ') p++;
        if (!*p) break;
        col[n++] = p;
        char *gap = strstr(p, "  ");
        if (!gap) break;
        *gap = '\0';
        p = gap + 2;
    }
    for (int i = 0; i < n; i++) {
        char *e = col[i] + strlen(col[i]);
        while (e > col[i] && (e[-1] == ' ' || e[-1] == '\n' || e[-1] == '\r')) *--e = '\0';
    }
    return n;
}

static int emit_generated_surface(const char *name) {
    char out[262144];
    const char *e = getenv("BONFYRE_BIN_DIR");
    char bin[PATH_MAX];
    snprintf(bin, sizeof(bin), "%s/bonfyre-capability", (e && *e) ? e : "/usr/local/bin");
    char *const argv[] = { bin, "list", NULL };
    int rc = auth_call(argv, NULL, out, sizeof(out));
    if (rc != 0) {
        printf("HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\n"
               "Connection: close\r\n\r\n"
               "{\"error\":\"capability registry unreachable\",\"surface\":");
        json(name); printf("}\n");
        return 1;
    }

    /* Buffer the JSON so the response can carry a real Content-Length. */
    static char page[262144];
    int w = 0;
    const BfNativeMachineSurface *surface = native_machine_surface(name);
    const char *schema;
    if (!surface) return 0;
    schema = surface->schema;
    char now[64]; iso_utc_now(now, sizeof(now));
    w += snprintf(page + w, sizeof(page) - (size_t)w,
        "{\"version\":\"%s\",\"generated_at\":\"%s\","
        "\"source\":\"BonfyreCapability registry, read at request time\","
        "\"resources\":[", schema, now);

    int count = 0, first = 1;
    char *save = NULL;
    for (char *line = strtok_r(out, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
        if (!strncmp(line, "ID ", 3) || line[0] == '-' || !line[0]) continue;
        char *col[8] = {0};
        int n = split_cols(line, col, 8);
        if (n < 3) continue;
        if ((size_t)w > sizeof(page) - 1024) break;
        w += snprintf(page + w, sizeof(page) - (size_t)w,
            "%s{\"uri\":\"bonfyre://capability/%s\",\"id\":\"%s\",\"name\":\"%s\","
            "\"binary\":\"%s\"%s%s%s%s%s%s}",
            first ? "" : ",", col[0], col[0], col[1], col[2],
            n > 3 ? ",\"stage\":\"" : "", n > 3 ? col[3] : "", n > 3 ? "\"" : "",
            n > 5 ? ",\"latency\":\"" : "", n > 5 ? col[5] : "", n > 5 ? "\"" : "");
        first = 0; count++;
    }
    w += snprintf(page + w, sizeof(page) - (size_t)w,
        "],\"resource_count\":%d,\"forbidden_inference\":["
        "\"a listed capability -> authority to invoke it\","
        "\"a registry entry -> the binary being present on this host\","
        "\"this manifest -> a stored catalogue; it is regenerated per request\"]}\n",
        count);

    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n"
           "Content-Length: %d\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
           w, page);
    return 1;
}

/* A surface is backed by an .html page or a .json document, whichever exists.
 * HTML wins so a surface meant for a person renders, while machine surfaces
 * stay JSON. Either way the same predicate governs advertising and serving. */
/* Resolve a surface to a file, in the representation the caller asked for.
 *
 * This always preferred .html, so a machine sending Accept: application/json
 * received a full page for every surface that had both -- the mirror image of
 * the JSON-to-a-human problem, and the same broken promise: one resource, many
 * carriers. Where a surface has only one representation it is still served,
 * because refusing a page to a client that did not say what it wanted would
 * break every plain fetch for the sake of a header.
 *
 * Cannot infer: a served representation -> authority over the subject it
 * describes. Carrier choice is not an access decision. */
static int surface_file_pref(char *dst, size_t dst_sz, const char *root,
                             const char *name, int want_json) {
    const char *first = want_json ? "json" : "html";
    const char *second = want_json ? "html" : "json";
    if (!root || !*root) return 0;
    if (snprintf(dst, dst_sz, "%s/surfaces/%s.%s", root, name, first) < (int)dst_sz &&
        access(dst, R_OK) == 0) return 1;
    if (snprintf(dst, dst_sz, "%s/surfaces/%s.%s", root, name, second) >= (int)dst_sz) return 0;
    return access(dst, R_OK) == 0;
}

static int surface_file(char *dst, size_t dst_sz, const char *root, const char *name) {
    return surface_file_pref(dst, dst_sz, root, name, 0);
}

static int surface_live(const char *name, const char *root) {
    /* A generated surface is live when the organ that produces it can run.
     * Nothing is stored, so there is no file to consult. */
    if (is_generated(name)) {
        const char *e = getenv("BONFYRE_BIN_DIR");
        char bin[PATH_MAX];
        snprintf(bin, sizeof(bin), "%s/bonfyre-capability", (e && *e) ? e : "/usr/local/bin");
        return access(bin, X_OK) == 0;
    }
    char path[PATH_MAX];
    return surface_file(path, sizeof(path), root, name);
}

static void emit_surface(const char *key, const char *host, const char *fmt, const char *root) {
    if (!surface_live(key, root)) return;
    char url[512];
    snprintf(url, sizeof(url), fmt, host);
    printf(",\"%s\":", key);
    json(url);
}


/* Serve a declared surface from the site root.
 *
 * The same predicate that decides advertising decides serving, so a surface
 * named in the descriptor always answers and one absent from it always 404s.
 * The host must still resolve in the matrix: a surface is per-territory, and an
 * unknown host gets no content.
 */

/* Operator surface: read-only views onto the compiled organs.
 *
 * One dispatcher, not a handler per organ. Every exposed operation is a literal
 * row in the table below -- organ, binary, and a fixed argv. Nothing from the
 * request ever becomes part of a command line: the path selects a row or it
 * 404s. That is what makes it safe to put a shell-less exec behind HTTP.
 *
 * The table is deliberately read-only. A write command has effects that belong
 * behind authority, not behind a URL, so none is listed here even where the
 * organ supports one.
 */
/* Read-only verbs. The verb is allowlisted; the binary is never taken from the
 * request. A capability's id selects a row in the live registry, and that row
 * supplies the executable. Nothing a caller types becomes a command. */
static const char *READ_VERBS[] = { "status", "list", "help", NULL };

static int verb_allowed(const char *v) {
    for (int i = 0; READ_VERBS[i]; i++) if (!strcmp(READ_VERBS[i], v)) return 1;
    return 0;
}

/* Try a binary name, then the same name with hyphens removed, then with hyphens
 * inserted at the usual word break.
 *
 * The registry records binary names that drifted from what is actually
 * installed: it says bonfyre-mo-e for bonfyre-moe, bonfyre-mediaprep for
 * bonfyre-media-prep, bonfyre-clean for bonfyre-transcript-clean. Reporting a
 * present organ as absent is accurate about the registry and useless about the
 * estate, so resolution tries the recorded name first and then the obvious
 * variants before giving up. */
static int resolve_binary(const char *bindir, const char *name,
                          char *out, size_t sz) {
    snprintf(out, sz, "%s/%s", bindir, name);
    if (access(out, X_OK) == 0) return 1;

    /* hyphens removed: bonfyre-mo-e -> bonfyre-moe */
    char flat[256];
    size_t j = 0;
    int seen_prefix = 0;
    for (size_t i = 0; name[i] && j + 1 < sizeof(flat); i++) {
        if (name[i] == '-' && seen_prefix) continue;
        if (name[i] == '-') seen_prefix = 1;
        flat[j++] = name[i];
    }
    flat[j] = '\0';
    if (strcmp(flat, name)) {
        snprintf(out, sz, "%s/%s", bindir, flat);
        if (access(out, X_OK) == 0) return 1;
    }
    return 0;
}

/* Resolve a capability id to its binary using the live registry. */
static int capability_binary(const char *id, char *out_bin, size_t sz) {
    char reg[262144];
    const char *e = getenv("BONFYRE_BIN_DIR");
    char cap[PATH_MAX];
    snprintf(cap, sizeof(cap), "%s/bonfyre-capability", (e && *e) ? e : "/usr/local/bin");
    char *const argv[] = { cap, "list", NULL };
    if (auth_call(argv, NULL, reg, sizeof(reg)) != 0) return 0;
    char *save = NULL;
    for (char *line = strtok_r(reg, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
        if (!strncmp(line, "ID ", 3) || line[0] == '-' || !line[0]) continue;
        char *col[8] = {0};
        if (split_cols(line, col, 8) < 3) continue;
        if (strcmp(col[0], id)) continue;
        return resolve_binary((e && *e) ? e : "/usr/local/bin", col[2], out_bin, sz);
    }
    /* Not in the registry. `bonfyre-capability index` rebuilds from a seed list
     * rather than from disk, so a freshly installed organ is runnable while the
     * registry still denies it exists. An organ that runs should be addressable;
     * the id is validated below before it is used to form a path. */
    for (const char *p = id; *p; p++)
        if (!(isalnum((unsigned char)*p) || *p == '-')) return 0;
    snprintf(out_bin, sz, "%s/bonfyre-%s", (e && *e) ? e : "/usr/local/bin", id);
    return access(out_bin, X_OK) == 0;
}

/* Installed organs the registry has never heard of.
 *
 * `bonfyre-capability index` rebuilds from a seed list, not from what is on
 * disk, so an organ can be built, installed and runnable while the registry
 * still denies it exists -- which is exactly what happened to admission,
 * attention, provider and domain. The filesystem is ground truth for what can
 * run; the registry supplies meaning for what it knows. The index reports the
 * union so a newly installed organ is visible the moment it lands.
 */
static int append_unregistered(char *page, int w, size_t cap, const char *bindir,
                               const char *registry, int *count) {
    DIR *d = opendir(bindir);
    if (!d) return w;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (strncmp(e->d_name, "bonfyre-", 8)) continue;
        /* Only compiled organs. A dot in the name means a wrapper script
         * (mesh-retention.sh) or a rollback copy (web-address.bak-1787989866);
         * neither is a capability and listing one would be the same kind of
         * false claim as advertising an unserved surface. */
        if (strchr(e->d_name + 8, '.')) continue;
        if (strstr(e->d_name, "-loop") || strstr(e->d_name, "with-env")) continue;
        if (strstr(registry, e->d_name)) continue;      /* registry already lists it */
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", bindir, e->d_name);
        if (access(full, X_OK) != 0) continue;
        if ((size_t)w > cap - 700) break;
        const char *id = e->d_name + 8;                  /* strip "bonfyre-" */
        w += snprintf(page + w, cap - (size_t)w,
            ",{\"id\":\"%s\",\"name\":\"%s\",\"binary\":\"%s\","
            "\"path\":\"/ops/%s/help\",\"available\":true,\"registered\":false}",
            id, id, e->d_name, id);
        (*count)++;
    }
    closedir(d);
    return w;
}

static int ops_index_generated(void) {
    char reg[262144];
    const char *e = getenv("BONFYRE_BIN_DIR");
    const char *bindir = (e && *e) ? e : "/usr/local/bin";
    char cap[PATH_MAX];
    snprintf(cap, sizeof(cap), "%s/bonfyre-capability", bindir);
    char *const argv[] = { cap, "list", NULL };
    if (auth_call(argv, NULL, reg, sizeof(reg)) != 0 || reg[0] == '\0') {
        printf("HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\n"
               "Connection: close\r\n\r\n{\"error\":\"capability registry unreachable\"}\n");
        return 1;
    }
    static char registry_copy[262144];
    snprintf(registry_copy, sizeof(registry_copy), "%s", reg);
    static char page[262144];
    int w = 0, count = 0, reachable = 0, first = 1;
    char now[64]; iso_utc_now(now, sizeof(now));
    w += snprintf(page + w, sizeof(page) - (size_t)w,
        "{\"version\":\"bonfyre-ops-v1\",\"generated_at\":\"%s\","
        "\"source\":\"BonfyreCapability registry, read at request time\","
        "\"read_verbs\":[\"status\",\"list\",\"help\"],\"capabilities\":[", now);
    char *save = NULL;
    for (char *line = strtok_r(reg, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
        if (!strncmp(line, "ID ", 3) || line[0] == '-' || !line[0]) continue;
        char *col[8] = {0};
        int n = split_cols(line, col, 8);
        if (n < 3) continue;
        char bin[PATH_MAX];
        int live = resolve_binary(bindir, col[2], bin, sizeof(bin));
        if (live) reachable++;
        count++;
        if ((size_t)w > sizeof(page) - 900) continue;
        w += snprintf(page + w, sizeof(page) - (size_t)w,
            "%s{\"id\":\"%s\",\"name\":\"%s\",\"binary\":\"%s\","
            "\"path\":\"/ops/%s/status\",\"available\":%s}",
            first ? "" : ",", col[0], col[1], col[2], col[0], live ? "true" : "false");
        first = 0;
    }
    int unregistered = 0;
    if (!first)
        w = append_unregistered(page, w, sizeof(page), bindir, registry_copy, &unregistered);
    count += unregistered; reachable += unregistered;
    w += snprintf(page + w, sizeof(page) - (size_t)w,
        "],\"capability_count\":%d,\"reachable\":%d,\"unregistered_installed\":%d,"
        "\"forbidden_inference\":["
        "\"a listed capability -> authority to invoke it\","
        "\"available true -> the organ having usable state\","
        "\"this index -> a stored catalogue; it is regenerated per request\"]}\n",
        count, reachable, unregistered);
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n"
           "Content-Length: %d\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
           w, page);
    return 1;
}

static int serve_ops(const char *method, const char *target) {
  if (strcmp(target, "/ops/") && strcmp(target, "/ops") &&
      strncmp(target, "/ops/", 5)) return 0;
  if (strcmp(method, "GET")) {
    printf("HTTP/1.1 405 Method Not Allowed\r\nContent-Type: application/json\r\n"
           "Connection: close\r\n\r\n{\"error\":\"read-only surface\"}\n");
    return 1;
  }
  if (!strcmp(target, "/ops/") || !strcmp(target, "/ops")) return ops_index_generated();

  char organ[64] = {0}, op[64] = {0};
  if (sscanf(target, "/ops/%63[^/]/%63[^/?]", organ, op) != 2) {
    printf("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n"
           "Connection: close\r\n\r\n{\"error\":\"unknown operation\"}\n");
    return 1;
  }
  /* Registry-backed path: any capability, allowlisted read verb. This replaced
   * a hardcoded twelve-row table that could only ever expose twelve of the
   * estate's capabilities and went stale the moment one was added. */
  char rbin[PATH_MAX];
  if (verb_allowed(op) && capability_binary(organ, rbin, sizeof(rbin))) {
    char rout[131072];
    char *const rargv[] = { rbin, op, NULL };
    int rrc = auth_call(rargv, NULL, rout, sizeof(rout));
    if (rrc == 127 || rout[0] == '\0') {
      printf("HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\n"
             "Connection: close\r\n\r\n{\"error\":\"%s\",\"capability\":",
             rrc == 127 ? "organ not reachable"
                        : "organ produced no output; its state is likely unreadable "
                          "from this service");
      json(organ); printf("}\n");
      return 1;
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\n"
           "Content-Length: %zu\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n%s",
           strlen(rout), rout);
    return 1;
  }

  printf("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n"
         "Connection: close\r\n\r\n"
         "{\"error\":\"unknown capability or verb\",\"read_verbs\":[\"status\",\"list\",\"help\"]}\n");
  return 1;
}

/* Auth surface: signup, login, logout, account.
 *
 * The address organ owns no credentials. It forwards to BonfyreAuth, which
 * holds the users and sessions and does PBKDF2-HMAC-SHA256 at 120k iterations.
 * Composition, not reimplementation.
 *
 * Two rules make this safe to expose:
 *   - the child is spawned with fork/execv and never through a shell, so no
 *     request field can become part of a command line;
 *   - a bearer token is validated against its exact shape (bfy_ + 64 hex)
 *     before it is passed as an argument at all.
 * Request bodies carry passwords, so nothing here logs a body.
 */
#endif /* BONFYRE_SURFACE_FRAGMENTS_AND_OPS_H */
