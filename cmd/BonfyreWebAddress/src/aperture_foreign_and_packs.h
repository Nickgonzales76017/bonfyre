#ifndef BONFYRE_APERTURE_FOREIGN_AND_PACKS_H
#define BONFYRE_APERTURE_FOREIGN_AND_PACKS_H
/* Extracted verbatim from main.c lines 15949-18032 (mechanical split, no logic changes). */
static int twin_classified_kind(const char *subject, char *out, size_t cap) {
    sqlite3 *db = cms_open_ro();
    sqlite3_stmt *st = NULL;
    int found = 0;
    out[0] = '\0';
    if (!db) return 0;
    if (!cms_type_registered(db, "twin_policy")) { sqlite3_close(db); return 0; }
    if (sqlite3_prepare_v2(db,
            "SELECT twin_kind FROM twin_policy WHERE twin_subject = ? "
            "  AND status = 'published' AND twin_kind IS NOT NULL AND twin_kind != '' LIMIT 1",
            -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, subject, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0));
            found = studio_safe_ident(out);
            if (!found) out[0] = '\0';
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    return found;
}

static int serve_aperture(const char *method, const char *target, const char *query,
                          const char *accept, const char *bearer, const char *auth_db) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char subject[256], kind[64];
    const char *dot;
    int as_json, offered = 0, withheld = 0;

    if (strcmp(target, "/_aperture") && strcmp(target, "/_aperture/")) return 0;
    if (strcmp(method, "GET")) return 0;

    if (!query_value(query, "subject", subject, sizeof(subject)) || !*subject) {
        protocol_reply(400, "application/json",
            "{\"error\":\"subject_required\",\"note\":\"apertures are computed from an "
            "existing addressed context, for example subject=institution:nightgrid\"}\n");
        return 1;
    }
    /* institution:nightgrid -> kind "institution". The kind is what a contract
     * declares applicability against; the rest identifies which one. */
    dot = strchr(subject, ':');
    {
        /* A kind that does not fit is refused rather than truncated: a silently
         * shortened kind would match a different contract's applicability. */
        size_t n = dot ? (size_t)(dot - subject) : strlen(subject);
        if (n == 0 || n >= sizeof(kind)) {
            protocol_reply(400, "application/json",
                "{\"error\":\"unusable_subject_kind\",\"note\":\"the kind before the first "
                "colon is empty or longer than this grammar admits\"}\n");
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
        protocol_reply(503, "application/json",
            "{\"error\":\"no_object_grammar\",\"note\":\"no ObjectContract grammar is registered\"}\n");
        return 1;
    }

    printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
           as_json ? "application/json" : "text/html; charset=utf-8");
    if (as_json)
        printf("{\"subject\":\"%s\",\"subject_kind\":\"%s\",\"apertures\":[", subject, kind);
    else
        printf("<ul class=\"apertures\">");

    if (sqlite3_prepare_v2(db,
            "SELECT object_name, birth_mode, birth_operation, collection_lens, "
            "       COALESCE(requires_authority,''), COALESCE(initial_state,''), "
            "       COALESCE(identity_allocation,''), COALESCE(closed_fields,''), "
            "       COALESCE(human_fields,''), COALESCE(applies_to_capability,'') "
            "FROM object_contract WHERE applies_to_kind = ? AND status = 'published' "
            "ORDER BY object_name", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, kind, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *name  = (const char *)sqlite3_column_text(st, 0);
            const char *mode  = (const char *)sqlite3_column_text(st, 1);
            const char *op    = (const char *)sqlite3_column_text(st, 2);
            const char *lens  = (const char *)sqlite3_column_text(st, 3);
            const char *req   = (const char *)sqlite3_column_text(st, 4);
            const char *state = (const char *)sqlite3_column_text(st, 5);
            const char *alloc = (const char *)sqlite3_column_text(st, 6);
            const char *closed= (const char *)sqlite3_column_text(st, 7);
            const char *human = (const char *)sqlite3_column_text(st, 8);
            const char *cap   = (const char *)sqlite3_column_text(st, 9);
            if (!name || !op) continue;
            if (req && *req && !surface_authority_satisfied(req, bearer, auth_db)) {
                withheld++;
                continue;
            }
            if (as_json) {
                printf("%s{\"object\":\"%s\",\"operation\":\"%s\",\"birth_mode\":\"%s\","
                       "\"lens\":\"%s\",\"capability\":\"%s\",\"initial_state\":\"%s\","
                       "\"identity_allocation\":\"%s\",\"closed_by_context\":\"%s\","
                       "\"human_must_supply\":\"%s\",\"form\":\"/_aperture/form?subject=%s&operation=%s\"}",
                       offered ? "," : "", name, op, mode ? mode : "", lens ? lens : "",
                       cap ? cap : "", state, alloc, closed, human, subject, op);
            } else {
                printf("<li><strong>");
                html(name);
                printf("</strong> <span class=\"mode\">");
                html(mode ? mode : "");
                printf("</span> <a href=\"/_aperture/form?subject=");
                html(subject);
                printf("&amp;operation=");
                html(op);
                printf("\">");
                html(op);
                printf("</a></li>");
            }
            offered++;
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);

    if (as_json)
        printf("],\"offered\":%d,\"withheld_for_authority\":%d,"
               "\"forbidden_inference\":\"an offered aperture is not a completed effect: "
               "the grammar applies and the actor is admitted, and the native owner still "
               "owns what happens\"}\n", offered, withheld);
    else {
        printf("</ul>");
        if (withheld) printf("<p class=\"surface-denied\">%d further operation(s) exist here "
                             "and are not offered to this session.</p>", withheld);
    }
    fflush(stdout);
    return 1;
}

/* Three small readers over one program's own JSON output. Not a parser: the
 * shape is fixed by the producer, and a malformed field yields an empty string
 * rather than a wrong row. */
static void surface_scan_string(const char *json, const char *key, char *out, size_t cap) {
    const char *p = strstr(json, key), *q;
    size_t n;
    out[0] = '\0';
    if (!p) return;
    p += strlen(key);
    q = strchr(p, '"');
    if (!q) return;
    n = (size_t)(q - p);
    if (n >= cap) n = cap - 1;
    memcpy(out, p, n); out[n] = '\0';
}

static void surface_scan_number(const char *json, const char *key, char *out, size_t cap) {
    const char *p = strstr(json, key);
    size_t n = 0;
    out[0] = '\0';
    if (!p) return;
    p += strlen(key);
    while (*p == ' ') p++;
    while ((isdigit((unsigned char)*p) || *p == '-') && n + 1 < cap) out[n++] = *p++;
    out[n] = '\0';
}

static void surface_scan_token(const char *json, const char *key, char *out, size_t cap) {
    const char *p = strstr(json, key);
    size_t n = 0;
    out[0] = '\0';
    if (!p) return;
    p += strlen(key);
    while (*p == ' ') p++;
    while (isalpha((unsigned char)*p) && n + 1 < cap) out[n++] = *p++;
    out[n] = '\0';
}

/* /observations -- what this institution believes, and what it no longer does.
 *
 * Every external observation is kept as its own receipt and nothing overwrites
 * anything, which is the honest storage shape and also an unreadable one: 35
 * receipts on disk answered no question a person would ask. This renders the
 * conflict set -- resources whose observations disagree, because their content
 * changed or because bytes are held that the source no longer serves.
 *
 * Rendered from BonfyreProject rather than by reading the receipt directory
 * here. The relation between two observations is that organ's answer to give;
 * a second reader would be a second supersession model free to disagree.
 *
 * Cannot infer: a listed conflict -> a source that lied. Content changing is
 * ordinary; the institution simply has to know its earlier belief is no longer
 * current. */
/* /twins -- what this institution has observed, and what became of it.
 *
 * Observation, promotion and claim are three different standings, and they were
 * legible only by reading three stores. A twin here is something we fetched; a
 * promoted one is something we chose to be able to name; a claimed one is
 * something an institution has asserted a relationship to. None of the three is
 * ownership.
 *
 * Rendered from BonfyreProject, which owns the receipts and now joins them to
 * the Matrix. A second reader here would be a second answer to the same
 * question. */
/* Landing surfaces served from authored state.
 *
 * These routes were static HTML files under the site root. A file is a
 * perfectly good way to serve a page and a poor way to own one: changing a
 * sentence meant editing something the institution's own machinery could not
 * reach, the routes were invisible to the CMS that is supposed to hold
 * authored state, and the nav had to be patched into thirteen files by hand
 * because each carried its own copy.
 *
 * The organ keeps what an organ should keep -- chrome, render primitives,
 * routing, the door fragment -- and the page's words come from a row. Adding a
 * surface is now an authoring act.
 *
 * This is checked before the static file is served, so an authored page wins
 * and an unauthored route still finds its file. That ordering is deliberate:
 * migration proceeds page by page without a flag day, and a route nobody has
 * authored yet keeps working.
 *
 * Cannot infer: an authored page -> a working capability behind its doors. The
 * page describes ways in; whether each answers is that carrier's own business. */
/* The closure layer of a surface: live addressed state, rendered where the page
 * says it belongs.
 *
 * A lens is the name of a surface pattern, so the page does not describe the
 * data -- it says which already-authored projection to render here. The pattern
 * still enforces its own authority, which is why a page can name a lens the
 * reader will not be shown: the refusal belongs to the pattern, not to the
 * page that mentions it. */
static const char *lens_html(const char *csv) {
    static char out[4096];
    size_t n = 0;
    const char *p = csv;
    out[0] = '\0';
    if (!csv || !*csv) return out;
    n = (size_t)snprintf(out, sizeof(out), "%s",
        "<section class=\"wrap\" style=\"padding:8px 0 24px\">");
    while (*p && n < sizeof(out) - 320) {
        char lens[64];
        size_t k = 0;
        while (*p == ' ' || *p == ',') p++;
        while (*p && *p != ',' && k + 1 < sizeof(lens)) lens[k++] = *p++;
        while (k && lens[k-1] == ' ') k--;
        lens[k] = '\0';
        if (!k || !studio_safe_ident(lens)) continue;
        n += (size_t)snprintf(out + n, sizeof(out) - n,
            "<div class=\"doors lens-panel\" hx-get=\"/_surface/%s\" hx-trigger=\"load\" "
            "hx-swap=\"innerHTML\"></div>", lens);
    }
    snprintf(out + n, sizeof(out) - n, "%s", "</section>");
    return out;
}

/* The action layer: what may be done here, by whoever is reading.
 *
 * The aperture list is fetched rather than rendered, because what a person may
 * do depends on who they are and a page cached for everyone cannot say. An
 * anonymous reader is offered nothing and told so; the same markup serves an
 * admitted operator the operations they hold. */
static const char *aperture_html(const char *subject, const char *operation,
                                  const char *next, const char *notice) {
    static char out[20000];
    char *markup = NULL; size_t length = 0; FILE *view;
    out[0] = '\0';
    if (!subject || !*subject) return out;
    view = open_memstream(&markup, &length);
    if (!view) return out;
    fputs("<section class=\"wrap\" style=\"padding:8px 0 48px\">", view);
    if (notice && *notice) {
        fputs("<p class=\"surface-status\" role=\"status\">", view);
        bf_auth_html(view, notice); fputs("</p>", view);
    }
    if (operation && *operation) {
        fputs("<div class=\"apertures\" hx-get=\"/_aperture/form?subject=", view);
        bf_auth_query(view, subject); fputs("&amp;operation=", view); bf_auth_query(view, operation);
        if (next && *next) { fputs("&amp;next=", view); bf_auth_query(view, next); }
    } else {
        fputs("<p class=\"eyebrow\"><i></i><span>What you may do here</span></p>"
              "<div class=\"apertures\" hx-get=\"/_aperture?subject=", view);
        bf_auth_query(view, subject);
    }
    fputs("\" hx-trigger=\"load\" hx-swap=\"innerHTML\"></div></section>", view);
    if (fclose(view) == 0 && markup && length < sizeof(out)) memcpy(out, markup, length + 1);
    free(markup); return out;
}

/* How much of a surface to render.
 *
 * A surface is one addressed closure, and the extent is how much of it the
 * caller wants -- not a different page, a different amount of the same page.
 * That is what makes "an application appears as a card somewhere else" an
 * ordinary projection instead of a second implementation: a workspace and a
 * chip differ in extent and in nothing else.
 *
 * chip       the name and where it goes
 * card       enough to decide whether to open it
 * section    the authored body, embeddable in another page
 * page       a full document: authored state and its live lenses
 * workspace  a full document that also offers what may be done here
 *
 * chip, card and section are fragments with no document chrome, because a
 * surface embedded in another surface must not bring its own <html>. */
typedef enum {
    EXTENT_CHIP = 0, EXTENT_CARD, EXTENT_SECTION, EXTENT_PAGE, EXTENT_WORKSPACE
} SurfaceExtent;

static SurfaceExtent extent_of(const char *query) {
    char want[32];
    if (!query || !query_value(query, "extent", want, sizeof(want))) return EXTENT_PAGE;
    if (!strcmp(want, "chip")) return EXTENT_CHIP;
    if (!strcmp(want, "card")) return EXTENT_CARD;
    if (!strcmp(want, "section")) return EXTENT_SECTION;
    if (!strcmp(want, "workspace")) return EXTENT_WORKSPACE;
    return EXTENT_PAGE;
}

static const char *extent_name(SurfaceExtent e) {
    switch (e) {
        case EXTENT_CHIP: return "chip";
        case EXTENT_CARD: return "card";
        case EXTENT_SECTION: return "section";
        case EXTENT_WORKSPACE: return "workspace";
        default: return "page";
    }
}

/* A surface that carries an authenticated pane must know, before it renders,
 * whether the caller holds a session. htmx does not swap on a 4xx -- its
 * responseHandling declares `[45].. swap:false` -- so a pane whose fragment
 * answers 401 leaves its placeholder on screen permanently. /account did
 * exactly that: an anonymous visitor read "Reading your authorized context..."
 * for as long as they cared to wait, and nothing on the page ever said why.
 *
 * The fix is not a better placeholder. A surface shows its pane to a caller who
 * can fill it and its declared operation to one who cannot, and the organ knows
 * which before a byte is written. */
static int serve_authored_page(const char *method, const char *target, const char *query,
                               const char *accept, const char *bearer, const char *auth_db,
                               const char *root, const char *host) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    static char body[65536];
    char route[256];
    int found = 0;

    if (strcmp(method, "GET")) return 0;
    if (!target || target[0] != '/' || strchr(target + 1, '/')) return 0;
    snprintf(route, sizeof(route), "%s", target);

    /* The territory root is a landing page, not an authored document.
     *
     * It was hero + two grids of authored cards, identical on all four hosts,
     * and a stranger arriving at .live and .shop saw the same sentence. A
     * landing page's job is to show what is actually here and to make the
     * gated half legible as a door rather than a wall, which is a projection
     * over live patterns rather than prose somebody wrote once.
     *
     * Machines keep the authored contract. This declines only the human
     * projection of "/", and only when the surface is actually deployed, so
     * removing the file restores the authored page rather than breaking the
     * root. */
    if (!strcmp(route, "/") && !wants_json(accept)) {
        char probe[PATH_MAX];
        if (root && *root && surface_file(probe, sizeof(probe), root, "home"))
            return 0;
    }

    db = cms_open_ro();
    if (!db) return 0;
    if (!cms_type_registered(db, "site_page")) { sqlite3_close(db); return 0; }
    if (sqlite3_prepare_v2(db,
            "SELECT title, COALESCE(eyebrow,''), heading, COALESCE(lede,''), "
            "       COALESCE(door_page,''), COALESCE(note,''), "
            "       COALESCE(lenses,''), COALESCE(aperture_subject,''), "
            "       COALESCE(aperture_operation,''), COALESCE(authenticated_pane,''), "
            "       COALESCE(machine_view,'authored') "
            /* The estate's own pages only: a site standing at its own address keeps
             * its pages in its own namespace, and they answer there and nowhere else. */
            "FROM site_page WHERE route = ? AND status = 'published' AND namespace = 'root' LIMIT 1",
            -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    sqlite3_bind_text(st, 1, route, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *title   = (const char *)sqlite3_column_text(st, 0);
        const char *eyebrow = (const char *)sqlite3_column_text(st, 1);
        const char *heading = (const char *)sqlite3_column_text(st, 2);
        const char *lede    = (const char *)sqlite3_column_text(st, 3);
        const char *doors   = (const char *)sqlite3_column_text(st, 4);
        const char *note    = (const char *)sqlite3_column_text(st, 5);
        const char *lenses  = (const char *)sqlite3_column_text(st, 6);
        const char *apsubj  = (const char *)sqlite3_column_text(st, 7);
        const char *apop    = (const char *)sqlite3_column_text(st, 8);
        const char *pane    = (const char *)sqlite3_column_text(st, 9);
        const char *machine = (const char *)sqlite3_column_text(st, 10);
        static char pane_html[1024];
        (void)apsubj;
        found = 1;
        pane_html[0] = '\0';

        /* A machine is a client that ASKED for JSON, not one that failed to ask
         * for HTML. This read `!wants_html(accept)`, so curl's default
         * wildcard Accept -- which asks for anything -- was served the
         * authored descriptor,
         * silently changing what every plain shell fetch of /agents and /market
         * had been reading. The estate already distinguishes the two: that is
         * why wants_json exists beside wants_html rather than being its
         * negation. */
        if (wants_json(accept)) {
            /* Authoring a route must never take away a machine contract the
             * route already had. The territory root answers machines with its
             * discovery descriptor; /account answers them by verifying the
             * session on the request. Authoring either one silently replaced a
             * contract other systems depend on, and a human visitor saw nothing
             * wrong.
             *
             * Which routes those are is not the organ's to know. It was a
             * literal `strcmp(route, "/")` here, which was correct for exactly
             * one route and became wrong the moment a second authored surface
             * sat on an existing machine answer. The page declares it:
             * machine_view `deferred` means a native owner answers machines
             * here and the authored layer is human-only.
             *
             * Declining is not the same as never emitting JSON: a machine
             * asking about /property should still learn the page is authored
             * and where its doors are. */
            if (!strcmp(machine, "deferred")) {
                sqlite3_finalize(st);
                sqlite3_close(db);
                return 0;
            }
            snprintf(body, sizeof(body),
                "{\"route\":\"%s\",\"title\":\"%s\",\"heading\":\"%s\",\"authored\":true,"
                "\"extent\":\"%s\",\"extents\":[\"chip\",\"card\",\"section\",\"page\",\"workspace\"],"
                "\"doors\":\"/_surface/doors?page=%s\","
                "\"note\":\"this surface is authored in the CMS; the organ holds only the "
                "chrome and the render primitives\"}\n",
                route, title, heading, extent_name(extent_of(query)),
                doors[0] ? doors : "protocols");
            sqlite3_finalize(st);
            sqlite3_close(db);
            protocol_reply(200, "application/json", body);
            return 1;
        }

        {
            SurfaceExtent extent = extent_of(query);
            if (extent < EXTENT_PAGE) {
                /* Copy before finalizing. sqlite3_finalize invalidates every
                 * text pointer the step handed out, and rendering from them
                 * afterwards printed raw stack bytes into the page -- which
                 * looked like an encoding fault rather than a use-after-free. */
                char c_eyebrow[256], c_heading[512], c_lede[2048], c_doors[64];
                snprintf(c_eyebrow, sizeof(c_eyebrow), "%s", eyebrow);
                snprintf(c_heading, sizeof(c_heading), "%s", heading);
                snprintf(c_lede, sizeof(c_lede), "%s", lede);
                snprintf(c_doors, sizeof(c_doors), "%s", doors[0] ? doors : "protocols");
                eyebrow = c_eyebrow; heading = c_heading; lede = c_lede; doors = c_doors;
                sqlite3_finalize(st);
                sqlite3_close(db);
                printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
                       "Cache-Control: no-store\r\nConnection: close\r\n\r\n");
                if (extent == EXTENT_CHIP) {
                    printf("<a class=\"chip\" href=\"%s\">", route);
                    html(heading);
                    printf("</a>");
                } else if (extent == EXTENT_CARD) {
                    printf("<article class=\"door\"><p class=\"eyebrow\"><i></i>%s</p><h3>", eyebrow);
                    html(heading);
                    printf("</h3><p>");
                    html(lede);
                    printf("</p><div class=\"ways\"><a class=\"way\" href=\"%s\">Open</a></div></article>",
                           route);
                } else {
                    printf("<section class=\"hero wrap\"><div class=\"eyebrow\"><i></i><span>%s</span></div>"
                           "<h1>", eyebrow);
                    html(heading);
                    printf("</h1><p class=\"lede\">");
                    html(lede);
                    printf("</p></section><div class=\"wrap\"><div class=\"doors\" "
                           "hx-get=\"/_surface/doors?page=%s\" hx-trigger=\"load\" "
                           "hx-swap=\"innerHTML\"></div></div>",
                           doors);
                }
                fflush(stdout);
                return 1;
            }
            /* The compact extents omit the action layer: a chip, a card and a
             * section are references to a surface, and there is no room to act
             * inside a reference. A page and a workspace both carry it.
             *
             * This was briefly written as `extent == EXTENT_PAGE`, which read
             * as a sensible restraint and made bare /login render no sign-in
             * form -- the operation appeared only at ?extent=workspace, which
             * no person types. The surface tested green because the test asked
             * for the extent that worked. */
            if (extent < EXTENT_PAGE) apsubj = "";
        }
        /* A declared pane is offered only to a caller who can fill it. Holding
         * a session is the whole test: what the pane is then allowed to show is
         * the pane owner's question, answered where authority is answered. */
        if (pane[0] && session_valid(bearer, auth_db)) {
            snprintf(pane_html, sizeof(pane_html),
                     "<div class=\"wrap\"><div hx-get=\"%s\" hx-trigger=\"load\" "
                     "hx-swap=\"innerHTML\"><p class=\"lede\">Reading your authorized "
                     "context\xe2\x80\xa6</p></div></div>", pane);
            /* The pane is the page for a signed-in caller; offering the sign-in
             * operation underneath it would invite them to do what they have
             * already done. */
            apsubj = ""; apop = "";
        }
        char auth_next[BF_AUTH_NEXT_MAX] = {0}, auth_flag[32];
        const char *auth_notice = "";
        if (!strcmp(route, "/login") || !strcmp(route, "/signup")) {
            auth_form_next("", query, host, auth_next, sizeof(auth_next));
            if (query_value(query, "failed", auth_flag, sizeof(auth_flag)) && !strcmp(auth_flag, "1"))
                auth_notice = !strcmp(route, "/signup") ? "Account creation failed. Check your details or sign in." : "Sign-in failed. Check your email and password.";
            else if (!strcmp(route, "/login") && query_value(query, "created", auth_flag, sizeof(auth_flag)) && !strcmp(auth_flag, "1"))
                auth_notice = "Account created. Sign in to continue.";
        }
        snprintf(body, sizeof(body),
            /* The institution's name is already in most titles a person would
             * author; appending it unconditionally produced "Bonfyre - Bonfyre"
             * on the front page. */
            BF_PAGE_OPEN "%s" BF_PAGE_AFTER_TITLE
            "<main><section class=\"hero wrap\" style=\"padding:56px 0 10px\">"
            "<div class=\"eyebrow\"><i></i><span>%s</span></div>"
            "<h1>%s</h1><p class=\"lede\">%s</p>%s</section>"
            "<div class=\"wrap\"><div class=\"doors\" hx-get=\"/_surface/doors?page=%s\" "
            "hx-trigger=\"load\" hx-swap=\"innerHTML\">"
            "<!-- authored doors load here; an unreachable store leaves this empty -->"
            "</div></div>%s%s%s</main>"
            "%s<script defer src=\"/assets/matrix-htmx.js\"></script></body></html>\n",
            title, site_nav_html(), eyebrow, heading, lede,
            note[0] ? "<p class=\"lede\" style=\"opacity:.72;font-size:.94em\">" : "",
            doors[0] ? doors : "protocols",
            lens_html(lenses), pane_html, aperture_html(apsubj, apop, auth_next, auth_notice),
            site_speculation_html());
        sqlite3_finalize(st);
        sqlite3_close(db);
        if (!bf_route_presentation_emit_authored(host, route, query, body))
            protocol_reply(200, "text/html; charset=utf-8", body);
        return 1;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    (void)found;
    return 0;
}

/* The observed external graph, at the scope the caller is actually admitted to.
 *
 * The scope is derived from the session and never read from the query. That is
 * the whole security property: a scope a caller can ask for is a scope every
 * caller has. `?scope=restricted` must be exactly as powerful as sending
 * nothing, and it is, because this function does not look at it.
 *
 * The filter is also applied inside the owner's query rather than here. Loading
 * every edge and dropping the ones this caller may not see would mean the rows
 * had already left the closure -- and every reader after that point (a cache, a
 * log, a template, a JSON body) is another place the disclosure can escape.
 *
 * Cannot infer: an observed edge is not an institutional relation. `follows` is
 * not friendship and a federation participant is not an admitted provider; that
 * crossing is a separate, deliberate act. */
/* A filename taken from a request, held to a stricter rule than an identifier.
 *
 * studio_safe_ident admits '.', which is right for a pattern name and wrong for
 * anything that becomes a path: "..". This is the check for the second case,
 * and it exists separately rather than as a tightening of the first because the
 * two questions are different and one of them is a traversal. */
static int foreign_safe_file(const char *text) {
    size_t i, n;
    if (!text || !*text) return 0;
    n = strlen(text);
    if (n > 128 || text[0] == '.' || text[0] == '-') return 0;
    for (i = 0; i < n; i++) {
        char c = text[i];
        if (!(isalnum((unsigned char)c) || c == '-' || c == '_' || c == '.')) return 0;
        if (c == '.' && i + 1 < n && text[i + 1] == '.') return 0;
    }
    return 1;
}

/* Retained foreign bytes, served back with the receipt that admitted them.
 *
 * Section 60: external platforms are human distribution edges. The estate
 * already observes them -- robots preflight, exact bytes, a receipt -- and then
 * had nowhere to put what it retained: a feed observed and stored is a feed
 * nobody can read. The bodies are RSS, Atom, sitemaps, nodeinfo documents,
 * peer lists, OAuth authorization-server metadata and auth.md; the whole of
 * what the outside world says about itself, sitting on disk.
 *
 * Two rules make serving them safe rather than reckless.
 *
 * The first is that these bytes never become a document on this origin. They
 * go out as text/plain with nosniff and a sandbox policy, so a retained page
 * with a script in it is text, not code, no matter what its own headers said
 * when it was fetched. Parsing it for display happens in the reader, inertly.
 *
 * The second is that the request never becomes a path. The caller names a twin;
 * the twin names a receipt; the receipt names the body -- and the body is
 * refused unless it resolves inside the bodies directory, because a receipt is
 * a file this process wrote but not one it should trust blindly.
 *
 * Cannot infer: retained bytes are what a source served at a moment, with a
 * digest saying so. They are not true, not current, and not this estate's
 * claim about the source.
 */
static int serve_foreign_body(const char *method, const char *target,
                              const char *query) {
    static char receipt[65536];
    static char body[1048576];
    const char *root = getenv("BONFYRE_FOREIGN_ROOT");
    char twin[160], path[PATH_MAX], bodies[PATH_MAX], bpath[PATH_MAX] = {0};
    char sha[80] = {0}, at[64] = {0}, src[1024] = {0}, decision[32] = {0};
    FILE *f = NULL;
    size_t n = 0;

    if (strcmp(target, "/foreign/body")) return 0;
    if (strcmp(method, "GET")) {
        protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n");
        return 1;
    }
    if (!root || !*root) root = "/var/lib/bonfyre/foreign";
    if (!query_value(query, "twin", twin, sizeof(twin)) || !foreign_safe_file(twin)) {
        protocol_reply(400, "application/json",
                       "{\"error\":\"twin_required\",\"note\":\"?twin=<id from /foreign/twins>\"}\n");
        return 1;
    }
    if (snprintf(path, sizeof(path), "%s/receipts/%s.json", root, twin) >= (int)sizeof(path) ||
        snprintf(bodies, sizeof(bodies), "%s/bodies/", root) >= (int)sizeof(bodies)) {
        protocol_reply(400, "application/json", "{\"error\":\"twin_too_long\"}\n");
        return 1;
    }
    if (!(f = bf_receipt_fopen(path, "rb"))) {
        protocol_reply(404, "application/json",
                       "{\"error\":\"no_receipt\",\"note\":\"nothing was observed under that twin\"}\n");
        return 1;
    }
    /* Read the tail, not the head.
     *
     * A receipt is the twin's whole history, one observation per line,
     * appended forever. Reading the first 64KB and taking the last line inside
     * that prefix works only while the history fits: past that the "last line"
     * is a line from the middle of the past, and the final one is cut
     * mid-JSON, so body_path never parses and the twin answers 404. The
     * failure arrives by growth, so the sources observed most often break
     * first -- hn-front-page and github-public-events had already crossed it,
     * and their bodies were on disk and unservable.
     *
     * Seeking to the end costs the same for a receipt of any size, which is
     * the property this needs: the estate keeps every observation, so the
     * history only grows. */
    {
        long size = 0;
        if (fseek(f, 0, SEEK_END) == 0 && (size = ftell(f)) >= 0) {
            long want = (long)sizeof(receipt) - 1;
            long from = size > want ? size - want : 0;
            if (fseek(f, from, SEEK_SET) != 0) from = 0;
        }
        n = fread(receipt, 1, sizeof(receipt) - 1, f);
    }
    fclose(f);
    receipt[n] = '\0';

    /* The tail may open mid-line; that partial first line is history, and the
     * last complete line is the newest observation. */
    {
        char *last = receipt, *p = receipt;
        for (;;) {
            char *nl = strchr(p, '\n');
            if (!nl) break;
            *nl = '\0';
            if (*p) last = p;
            p = nl + 1;
        }
        if (*p) last = p;
        if (last != receipt) memmove(receipt, last, strlen(last) + 1);
    }

    if (!studio_json_field(receipt, "body_path", bpath, sizeof(bpath))) bpath[0] = '\0';
    studio_json_field(receipt, "content_sha256", sha, sizeof(sha));
    studio_json_field(receipt, "observed_at", at, sizeof(at));
    studio_json_field(receipt, "source_url", src, sizeof(src));
    studio_json_field(receipt, "policy_decision", decision, sizeof(decision));

    /* The receipt names the body. It does not get to name any other file. */
    if (!bpath[0] || strncmp(bpath, bodies, strlen(bodies)) || strstr(bpath, "..")) {
        protocol_reply(404, "application/json",
                       "{\"error\":\"no_retained_body\",\"note\":\"the observation was receipted "
                       "without retaining bytes, or its body has been cooled off disk\"}\n");
        return 1;
    }
    if (!(f = fopen(bpath, "rb"))) {
        protocol_reply(404, "application/json",
                       "{\"error\":\"body_not_on_disk\",\"note\":\"receipted and since cooled; the "
                       "receipt remains and the bytes do not\"}\n");
        return 1;
    }
    n = fread(body, 1, sizeof(body) - 1, f);
    fclose(f);
    body[n] = '\0';

    printf("HTTP/1.1 200 OK\r\n"
           /* Never a document on this origin. */
           "Content-Type: text/plain; charset=utf-8\r\n"
           "X-Content-Type-Options: nosniff\r\n"
           "Content-Security-Policy: sandbox; default-src 'none'\r\n"
           "Content-Disposition: inline; filename=\"%s.txt\"\r\n"
           "Access-Control-Allow-Origin: *\r\n"
           "X-Bonfyre-Observed-At: %s\r\n"
           "X-Bonfyre-Content-Sha256: %s\r\n"
           "X-Bonfyre-Policy-Decision: %s\r\n"
           "X-Bonfyre-Cannot-Infer: retained bytes are what a source served at a moment, "
           "not a claim that they are true or current\r\n"
           "Content-Length: %zu\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
           twin, at, sha, decision[0] ? decision : "unknown", n);
    (void)src;
    fwrite(body, 1, n, stdout);
    fflush(stdout);
    return 1;
}

/* Every receipt, so a reader can find the bytes.
 *
 * /foreign/twins says what was observed and when. It does not say which
 * observation retained a body, and the body is addressed by the receipt's own
 * id -- so without this listing the retained bytes were reachable only by
 * somebody who already knew the filename. An estate that observes the outside
 * world and cannot enumerate what it kept is keeping it for nobody.
 *
 * Cannot infer: a receipt is a record of one fetch. That two receipts exist for
 * a URL says it was read twice, not that anything about it changed.
 */
static int serve_foreign_receipts(const char *method, const char *target,
                                  const char *query) {
    static char buf[65536];
    const char *root = getenv("BONFYRE_FOREIGN_ROOT");
    char dirpath[PATH_MAX], want_kind[64] = {0};
    BfReceiptDir *d = NULL;
    struct dirent *e;
    int rows = 0, retained = 0;

    if (strcmp(target, "/foreign/receipts") && strcmp(target, "/foreign/receipts/")) return 0;
    if (strcmp(method, "GET")) {
        protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n");
        return 1;
    }
    if (!root || !*root) root = "/var/lib/bonfyre/foreign";
    (void)query_value(query, "kind", want_kind, sizeof(want_kind));
    if (snprintf(dirpath, sizeof(dirpath), "%s/receipts", root) >= (int)sizeof(dirpath) ||
        !(d = bf_receipt_opendir(dirpath))) {
        protocol_reply(503, "application/json", "{\"error\":\"no_receipt_store\"}\n");
        return 1;
    }

    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
           "Access-Control-Allow-Origin: *\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n");
    printf("{\"schema\":\"bonfyre-foreign-receipts.v1\",\"receipts\":[");

    while ((e = bf_receipt_readdir(d))) {
        char path[PATH_MAX], twin[192];
        char url[1024] = {0}, at[64] = {0}, status[16] = {0}, sha[80] = {0};
        char bpath[PATH_MAX] = {0}, bytes[24] = {0}, kind[64] = {0}, decision[32] = {0};
        char e_twin[512], e_url[2048], e_kind[192];
        size_t n, len;
        FILE *f;
        len = strlen(e->d_name);
        if (len < 6 || strcmp(e->d_name + len - 5, ".json")) continue;
        if (len - 5 >= sizeof(twin)) continue;
        memcpy(twin, e->d_name, len - 5);
        twin[len - 5] = '\0';
        if (!foreign_safe_file(twin)) continue;
        if (snprintf(path, sizeof(path), "%s/%s", dirpath, e->d_name) >= (int)sizeof(path)) continue;
        if (!(f = bf_receipt_fopen(path, "rb"))) continue;
        n = fread(buf, 1, sizeof(buf) - 1, f);
        fclose(f);
        buf[n] = '\0';

        studio_json_field(buf, "source_url", url, sizeof(url));
        studio_json_field(buf, "observed_at", at, sizeof(at));
        studio_json_field(buf, "http_status", status, sizeof(status));
        studio_json_field(buf, "content_sha256", sha, sizeof(sha));
        studio_json_field(buf, "body_path", bpath, sizeof(bpath));
        studio_json_field(buf, "body_bytes", bytes, sizeof(bytes));
        studio_json_field(buf, "observation_kind", kind, sizeof(kind));
        studio_json_field(buf, "policy_decision", decision, sizeof(decision));
        if (*want_kind && strcmp(kind, want_kind)) continue;

        /* Retained means the bytes are still there, not merely that a path was
         * written down: bodies are cooled off disk and the receipt outlives
         * them. Saying "retained" for a body that has been archived would send
         * every reader to a 404. */
        {
            int have = 0;
            if (bpath[0] && !strstr(bpath, "..")) {
                FILE *b = fopen(bpath, "rb");
                if (b) { have = 1; fclose(b); }
            }
            json_escape_into(twin, e_twin, sizeof(e_twin));
            json_escape_into(url, e_url, sizeof(e_url));
            json_escape_into(kind, e_kind, sizeof(e_kind));
            printf("%s{\"twin\":\"%s\",\"url\":\"%s\",\"observed_at\":\"%s\","
                   "\"http_status\":%d,\"kind\":\"%s\",\"policy_decision\":\"%s\","
                   "\"sha256\":\"%s\",\"bytes\":%ld,\"body\":%s",
                   rows ? "," : "", e_twin, e_url, at, atoi(status), e_kind,
                   decision[0] ? decision : "unknown", sha, atol(bytes),
                   have ? "true" : "false");
            if (have) {
                printf(",\"body_href\":\"/foreign/body?twin=%s\"", e_twin);
                retained++;
            }
            printf("}");
        }
        rows++;
    }
    bf_receipt_closedir(d);
    printf("],\"count\":%d,\"retained\":%d,"
           "\"cannot_infer\":\"a receipt records one fetch; two receipts for a URL say it was "
           "read twice, never that anything about it changed\"}\n", rows, retained);
    fflush(stdout);
    return 1;
}

/* What the reheat decided, last time it ran.
 *
 * ResourceWeather is the difference between an estate that holds almost
 * nothing and one that mirrors the internet slowly: a source is asked when it
 * is worth asking, demoted when nobody's closure needs it, and frozen when it
 * says not to ask. This is that decision, published, so a surface can show the
 * machine breathing rather than describing it.
 *
 * Cannot infer: a reheated source is a fresh observation and never a fresh
 * fact. The receipt says when it was read, not that what it says is true.
 */
static int serve_foreign_weather(const char *method, const char *target) {
    static char buf[262144];
    const char *path = getenv("BONFYRE_WEATHER");
    FILE *f;
    size_t n;

    if (strcmp(target, "/foreign/weather") && strcmp(target, "/foreign/weather/")) return 0;
    if (strcmp(method, "GET")) {
        protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n");
        return 1;
    }
    if (!path || !*path) path = "/var/lib/bonfyre/foreign/weather.json";
    if (!(f = fopen(path, "rb"))) {
        /* No weather is not an empty forecast. It means the reheat has not run
         * on this machine, which is a different thing from nothing being due. */
        protocol_reply(503, "application/json",
                       "{\"error\":\"no_weather\",\"note\":\"the reheat has not run here; "
                       "systemctl status bonfyre-reheat.timer\"}\n");
        return 1;
    }
    n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
           "Access-Control-Allow-Origin: *\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n%s", buf);
    fflush(stdout);
    return 1;
}

/* Home asks for a tiny, server-owned ResourceWeather projection.  The browser
 * receives an HTMX fragment, never a JSON descriptor that client JavaScript
 * must interpret into a product claim. */
static int serve_home_weather_fragment(const char *method, const char *target) {
    const char *path = getenv("BONFYRE_WEATHER");
    FILE *file;
    char *body, *cursor;
    long size;
    int active = 0;

    if (strcmp(target, "/_surface/home-weather")) return 0;
    if (strcmp(method, "GET")) {
        protocol_reply(405, "text/html; charset=utf-8", "<span>GET required.</span>");
        return 1;
    }
    if (!path || !*path) path = "/var/lib/bonfyre/foreign/weather.json";
    file = fopen(path, "rb");
    if (!file || fseek(file, 0, SEEK_END) != 0 || (size = ftell(file)) < 0 ||
        size > 1024 * 1024 || fseek(file, 0, SEEK_SET) != 0 ||
        !(body = malloc((size_t)size + 1))) {
        if (file) fclose(file);
        protocol_reply(503, "text/html; charset=utf-8",
                       "<span>No public carrier is available from this address right now.</span>");
        return 1;
    }
    if (fread(body, 1, (size_t)size, file) != (size_t)size) {
        fclose(file); free(body);
        protocol_reply(503, "text/html; charset=utf-8",
                       "<span>The public carrier could not be read right now.</span>");
        return 1;
    }
    fclose(file); body[size] = '\0';
    cursor = body;
    while ((cursor = strstr(cursor, "\"tier\"")) != NULL) {
        char tier[16] = {0};
        char *colon = strchr(cursor + 6, ':');
        char *quote = colon ? strchr(colon, '"') : NULL;
        if (quote && sscanf(quote + 1, "%15[^\"]", tier) == 1 &&
            (!strcmp(tier, "HOT") || !strcmp(tier, "WARM"))) active++;
        cursor += 6;
    }
    free(body);
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n"
           "<strong>%d</strong> available carrier%s · public observation only",
           active, active == 1 ? "" : "s");
    fflush(stdout);
    return 1;
}

/* The observed twins, over HTTP. The organ has answered this since foreign
 * observation existed; nothing served it, so every surface that wanted to know
 * what the estate had observed had to be told rather than shown. */
#include "foreign_catalogue_query.h"

static int serve_foreign_twins(const char *method, const char *target, const char *query) {
    /* The compact source-release catalogue is intentionally bounded by source
     * contracts, not the 135k-row receipt ledger, but a full estate still has
     * ~2.5k rich rows.  The old 256 KiB pipe silently made the real catalogue
     * unavailable before the surface could paginate it. */
    static char out[4 * 1024 * 1024];
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    char bin[PATH_MAX];
    char page[32] = "0";
    char search[241] = {0};
    char snapshot[32] = {0};
    const char *suffix = "/foreign/twins/page/";

    if (strcmp(target, "/foreign/twins") && strcmp(target, "/foreign/twins/")) {
        size_t i, n;
        if (strncmp(target, suffix, strlen(suffix))) return 0;
        n = strlen(target + strlen(suffix));
        if (!n || n >= sizeof(page)) return 0;
        for (i = 0; i < n; i++) if (!isdigit((unsigned char)target[strlen(suffix) + i])) return 0;
        memcpy(page, target + strlen(suffix), n);
        page[n] = '\0';
    }
    if (strcmp(method, "GET")) {
        protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n");
        return 1;
    }
    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    if (snprintf(bin, sizeof(bin), "%s/bonfyre-project", bindir) >= (int)sizeof(bin)) return 0;
    /* Query remains an intentionally literal operator filter.  It is passed as
     * one argv element (never through a shell) and the project owner returns
     * both this result-set total and the untouched whole-estate summary. */
    if (!foreign_catalogue_query(query, search, snapshot)) {
        protocol_reply(400, "application/json", "{\"error\":\"invalid_foreign_twin_query\"}\n");
        return 1;
    }

    out[0] = '\0';
    {
        char *argv[] = { bin, (char*)"foreign", (char*)"twins", (char*)"--page",
                         page, (char*)"--limit", (char*)"200", (char*)"--query", search,
                         (char*)"--expect-generation", snapshot, NULL };
        if (auth_call(argv, NULL, out, sizeof(out)) != 0 || !out[0]) {
            protocol_reply(503, "application/json", "{\"error\":\"foreign_graph_unavailable\"}\n");
            return 1;
        }
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
           "Access-Control-Allow-Origin: *\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n%s", out);
    fflush(stdout);
    return 1;
}


/* ------------------------------------------------------------- PackGraph
 *
 * The estate stopped needing a branch per product.
 *
 * A pack is an addressed behaviour artifact, and it is deliberately not any of
 * the four things it keeps being confused with. It is not a subject: DFW Gold
 * Wing Riders is a community, and the Gold Wing Community Pack is a thing that
 * community runs. It is not an institution: a pack holds no authority and no
 * state. It is not an internet cut: the pack says what external reality it
 * needs, and the cut is where that reality lives. It is not a surface: the
 * pack says which grammars its closure benefits from and implements none of
 * them.
 *
 * Keeping those five apart is what lets a community fork behaviour without
 * forking Bonfyre, and what stops behaviour from becoming a second identity
 * that could grant itself authority.
 *
 * What this organ does is resolve a closure -- the smallest sufficient set of
 * packs for one actor in one scope -- and it enforces the one rule that keeps
 * that from becoming plugin chaos: an overlay may only hide, add, specialize,
 * present, tighten or default. There is no verb for granting authority,
 * widening disclosure, rewriting identity, dropping evidence, or promoting an
 * observation to a fact. An overlay attempting one is reported as refused
 * rather than quietly ignored, because a silently dropped override is a
 * community believing it configured something it did not.
 *
 * Cannot infer: an activated pack -> an authorized effect. A closure decides
 * what is composed and offers verbs; Authority still decides every effect at
 * effect time, and it is not this organ.
 */

/* Composition order. Lower binds first and cannot be widened by anything
 * later: official, then the institution publisher, then the community, then
 * the household or person, then a temporary scenario. */
static int pack_layer_rank(const char *layer) {
    if (!layer) return 9;
    if (!strcmp(layer, "official"))    return 0;
    if (!strcmp(layer, "institution")) return 1;
    if (!strcmp(layer, "community"))   return 2;
    if (!strcmp(layer, "household"))   return 3;
    if (!strcmp(layer, "scenario"))    return 4;
    return 9;
}

/* The closed list. Everything an overlay is allowed to be. */
static int pack_verb_allowed(const char *v) {
    static const char *ok[] = { "hide", "add", "specialize", "present",
                                "tighten", "default", NULL };
    int i;
    if (!v) return 0;
    for (i = 0; ok[i]; i++) if (!strcmp(v, ok[i])) return 1;
    return 0;
}

/* Audience, narrowest first. A tighten may move down this list and never up,
 * whichever layer asks. */
static int pack_audience_rank(const char *a) {
    static const char *order[] = { "self", "household", "group", "community",
                                   "public", "market", "institution", NULL };
    int i;
    if (!a || !*a) return -1;
    for (i = 0; order[i]; i++) if (!strcmp(a, order[i])) return i;
    return -1;
}

#define PACK_MAX 64

typedef struct {
    char pack[96];
    char layer[24];
    char kind[24];
    char root[128];
    char version[64];
    int  rank;
    int  active;      /* activated for this actor, rather than only reachable */
    int  hidden;
    char hidden_by[96];
} PackNode;
#include "pack_setup.h"
#include "application_workspace.h"
static void cjw_profile(sqlite3 *db,long uid,const char *q,const char *cursor);
static void cjw_signin(const char *path);
#include "participant_state.h"
#include "participant_journey.h"
#include "profile_home.h"
#include "account_world_web.h"
#include "community_journey.h"
#include "organization_workspace_web.h"
#include "account_choices.h"
#include "household_members_web.h"
#include "guild_records_web.h"
#include "household_circles_web.h"
#include "charters_web.h"

static int pack_find(PackNode *n, int count, const char *pack) {
    int i;
    if (!pack || !*pack) return -1;
    for (i = 0; i < count; i++) if (!strcmp(n[i].pack, pack)) return i;
    return -1;
}

static void pack_refusal_append(char *out, size_t cap, size_t *used,
                                const char *overlay, const char *verb, const char *reason) {
    char eo[256], ev[128], er[768], entry[1400];
    int n;
    json_escape_into(overlay ? overlay : "", eo, sizeof(eo));
    json_escape_into(verb ? verb : "", ev, sizeof(ev));
    json_escape_into(reason, er, sizeof(er));
    n = snprintf(entry, sizeof(entry), "%s{\"overlay\":\"%s\",\"verb\":\"%s\",\"reason\":\"%s\"}",
                 *used ? "," : "", eo, ev, er);
    if (n < 0 || (size_t)n >= sizeof(entry) || *used >= cap || (size_t)n >= cap - *used) return;
    memcpy(out + *used, entry, (size_t)n + 1);
    *used += (size_t)n;
}

/* Add a pack and everything it depends on. Depth is bounded because a
 * dependency cycle is an authoring error and not a reason to hang. */
static int pack_close(sqlite3 *db, PackNode *n, int count, const char *pack,
                      int active, int depth) {
    sqlite3_stmt *st = NULL;
    if (depth > 8 || count >= PACK_MAX) return count;
    if (pack_find(n, count, pack) >= 0) {
        int i = pack_find(n, count, pack);
        if (active) n[i].active = 1;
        return count;
    }
    if (sqlite3_prepare_v2(db,
            "SELECT a.layer, a.kind, COALESCE(v.root,''), COALESCE(v.version,'') "
            "FROM pack_artifact a "
            "LEFT JOIN pack_head h ON h.pack = a.pack AND h.channel = 'stable' "
            "     AND h.status = 'published' AND h.namespace='root' "
            "LEFT JOIN pack_version v ON v.pack = a.pack AND v.version = h.version "
            "     AND v.status = 'published' AND v.namespace='root' "
            "WHERE a.pack = ? AND a.status = 'published' AND a.namespace='root' LIMIT 1",
            -1, &st, NULL) != SQLITE_OK) return count;
    sqlite3_bind_text(st, 1, pack, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        PackNode *p = &n[count];
        memset(p, 0, sizeof(*p));
        snprintf(p->pack, sizeof(p->pack), "%s", pack);
        snprintf(p->layer, sizeof(p->layer), "%s", (const char *)sqlite3_column_text(st, 0));
        snprintf(p->kind, sizeof(p->kind), "%s", (const char *)sqlite3_column_text(st, 1));
        snprintf(p->root, sizeof(p->root), "%s", (const char *)sqlite3_column_text(st, 2));
        snprintf(p->version, sizeof(p->version), "%s", (const char *)sqlite3_column_text(st, 3));
        p->rank = pack_layer_rank(p->layer);
        p->active = active;
        count++;
    }
    sqlite3_finalize(st);
    st = NULL;

    if (sqlite3_prepare_v2(db,
            "SELECT depends_on FROM pack_dependency "
            "WHERE pack = ? AND status = 'published' AND namespace='root'", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, pack, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *dep = (const char *)sqlite3_column_text(st, 0);
            if (dep) count = pack_close(db, n, count, dep, 0, depth + 1);
        }
        sqlite3_finalize(st);
    }
    return count;
}


static int serve_packs(const char *method, const char *target, const char *query,
                       const char *host_in, const char *accept,
                       const char *bearer, const char *auth_db) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    PackNode nodes[PACK_MAX];
    int count = 0, i, rows = 0;
    int is_closure;
    char actor[160] = {0}, scenario[96] = {0};
    char audience[32] = {0}, keep[32] = {0};
    int audience_rank = -1;
    /* Views the closure asks for, hides, and orders. A pack says which
     * grammars its closure benefits from; it implements none of them, and the
     * renderer registry is the only place they exist. */
    char view_hidden[16][32] = {{0}}; int view_hidden_n = 0;
    char view_order[16][32];  int view_rank[16]; int view_order_n = 0;

    if (!strcmp(target, "/packs") || !strcmp(target, "/packs/")) {
        /* The registry has two projections and only one of them lives here.
         * A browser asking for /packs wants the page; the page then asks this
         * organ for the same bytes a machine would get. */
        if (!wants_json(accept)) return 0;
        is_closure = 0;
    }
    else if (!strcmp(target, "/packs/closure")) is_closure = 1;
    else return 0;
    if (strcmp(method, "GET")) {
        protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n");
        return 1;
    }
    if (!(db = cms_open_ro())) {
        protocol_reply(503, "application/json", "{\"error\":\"pack_graph_unavailable\"}\n");
        return 1;
    }
    if (!cms_type_registered(db, "pack_artifact")) {
        sqlite3_close(db);
        protocol_reply(503, "application/json",
                       "{\"error\":\"pack_graph_unauthored\"}\n");
        return 1;
    }

    if (sqlite3_exec(db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(db); protocol_reply(503, "text/plain", "The configuration store is unavailable.\n"); return 1;
    }

    /* ------------------------------------------------------ the registry */
    if (!is_closure) {
        printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
               "Access-Control-Allow-Origin: *\r\n"
               "Cache-Control: no-store\r\nConnection: close\r\n\r\n");
        printf("{\"schema\":\"bonfyre-pack-graph.v1\",\"packs\":[");
        if (sqlite3_prepare_v2(db,
                "SELECT a.pack, a.canonical_name, a.publisher, a.layer, a.kind, "
                "  a.purpose, COALESCE(a.forks_from,''), a.governed_by, a.cannot_infer, "
                "  COALESCE(h.version,''), COALESCE(v.root,''), "
                "  (SELECT count(*) FROM pack_dependency d WHERE d.pack = a.pack AND d.namespace='root'), "
                "  (SELECT count(*) FROM pack_overlay o WHERE o.overlay = a.pack AND o.namespace='root'), "
                "  (SELECT count(*) FROM pack_demand m WHERE m.pack = a.pack AND m.namespace='root'), "
                "  (SELECT count(*) FROM pack_installation s WHERE s.pack = a.pack AND s.namespace='root'), "
                "  (SELECT count(*) FROM pack_activation c WHERE c.pack = a.pack AND c.namespace='root') "
                "FROM pack_artifact a "
                "LEFT JOIN pack_head h ON h.pack = a.pack AND h.channel='stable' AND h.namespace='root' AND h.status='published' "
                "LEFT JOIN pack_version v ON v.pack=a.pack AND v.version=h.version AND v.namespace='root' AND v.status='published' "
                "WHERE a.status = 'published' AND a.namespace='root' "
                "ORDER BY CASE a.layer WHEN 'official' THEN 0 WHEN 'institution' THEN 1 "
                "  WHEN 'community' THEN 2 WHEN 'household' THEN 3 ELSE 4 END, a.pack",
                -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                char e[6][2048];
                int c;
                for (c = 0; c < 6; c++)
                    json_escape_into((const char *)sqlite3_column_text(st, c == 0 ? 1 :
                                     c == 1 ? 2 : c == 2 ? 5 : c == 3 ? 6 :
                                     c == 4 ? 7 : 8), e[c], sizeof(e[c]));
                printf("%s{\"pack\":\"%s\",\"canonical_name\":\"%s\",\"publisher\":\"%s\","
                       "\"layer\":\"%s\",\"kind\":\"%s\",\"purpose\":\"%s\","
                       "\"forks_from\":\"%s\",\"governed_by\":\"%s\","
                       "\"cannot_infer\":\"%s\",\"head\":\"%s\",\"root\":\"%s\","
                       "\"dependencies\":%d,\"overlays\":%d,\"demands\":%d,"
                       "\"installations\":%d,\"activations\":%d}",
                       rows ? "," : "",
                       (const char *)sqlite3_column_text(st, 0), e[0], e[1],
                       (const char *)sqlite3_column_text(st, 3),
                       (const char *)sqlite3_column_text(st, 4),
                       e[2], e[3], e[4], e[5],
                       (const char *)sqlite3_column_text(st, 9),
                       (const char *)sqlite3_column_text(st, 10),
                       sqlite3_column_int(st, 11), sqlite3_column_int(st, 12),
                       sqlite3_column_int(st, 13), sqlite3_column_int(st, 14),
                       sqlite3_column_int(st, 15));
                rows++;
            }
            sqlite3_finalize(st);
        }
        printf("],\"count\":%d,"
               "\"layers\":[\"official\",\"institution\",\"community\",\"household\","
               "\"scenario\"],"
               "\"overlay_verbs\":[\"hide\",\"add\",\"specialize\",\"present\","
               "\"tighten\",\"default\"],"
               "\"cannot_infer\":\"a pack is behaviour and never an identity; forking is "
               "not installing, installing is not activating, and activating is not "
               "authorizing\"}\n", rows);
        sqlite3_close(db);
        fflush(stdout);
        return 1;
    }

    /* ------------------------------------------------------- the closure */
    int scoped = psetup_scoped_closure(db, query, bearer, auth_db, nodes, &count, actor, sizeof(actor));
    if (scoped < 0) { sqlite3_close(db); return 1; }
    if (!scoped && (!query_value(query, "actor", actor, sizeof(actor)) || !*actor)) {
        /* No actor named: the host's own institution is the actor, which is
         * what a person opening their own host means. The lookup is a read of
         * matrix_domains rather than a resolve, because this organ composes
         * behaviour and has no business holding a Matrix handle. */
        char host[256];
        const char *mp = getenv("BONFYRE_MATRIX_DB");
        sqlite3 *mdb = NULL;
        if (!mp || !*mp) mp = "/var/lib/bonfyre/matrix.db";
        if (lower_host(host_in, host) &&
            sqlite3_open_v2(mp, &mdb, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
            sqlite3_stmt *ms = NULL;
            if (sqlite3_prepare_v2(mdb,
                    "SELECT institution_id FROM matrix_domains WHERE hostname = ? LIMIT 1",
                    -1, &ms, NULL) == SQLITE_OK) {
                sqlite3_bind_text(ms, 1, host, -1, SQLITE_TRANSIENT);
                if (sqlite3_step(ms) == SQLITE_ROW)
                    snprintf(actor, sizeof(actor), "%s",
                             (const char *)sqlite3_column_text(ms, 0));
                sqlite3_finalize(ms);
            }
            sqlite3_close(mdb);
        }
    }
    if (!scoped) (void)query_value(query, "scenario", scenario, sizeof(scenario));

    if (!*actor) {
        sqlite3_close(db);
        protocol_reply(400, "application/json",
                       "{\"error\":\"actor_required\",\"note\":\"?actor=<institution or "
                       "person id>, or open this from a host that resolves to one\"}\n");
        return 1;
    }

    /* Activated, not merely installed. Holding a pack is not running it. */
    if (!scoped && sqlite3_prepare_v2(db,
            "SELECT pack FROM pack_activation WHERE activated_for = ? "
            "AND status = 'published' AND namespace='root'", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, actor, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *p = (const char *)sqlite3_column_text(st, 0);
            if (p) count = pack_close(db, nodes, count, p, 1, 0);
        }
        sqlite3_finalize(st);
        st = NULL;
    }
    /* A scenario is temporary configuration, asked for rather than installed. */
    if (*scenario) count = pack_close(db, nodes, count, scenario, 1, 0);

    if (scoped && !wants_json(accept)) {
        PackSetupInput selected; (void)psetup_input(query, &selected);
        psetup_start(200, "Active configuration");
        fputs("<h1>Active configuration</h1><p>These installed versions are current for the selected workspace. Each product still checks its own data access and execution permissions.</p><table><thead><tr><th>Behavior</th><th>Version</th><th>Role in this configuration</th></tr></thead><tbody>", stdout);
        for (i = 0; i < count; i++) {
            fputs("<tr><td>", stdout); html(nodes[i].pack); fputs("</td><td>", stdout); html(nodes[i].version);
            fputs("</td><td>", stdout); fputs(nodes[i].active ? "Selected configuration" : "Required behavior", stdout); fputs("</td></tr>", stdout);
        }
        fputs("</tbody></table><p><a href=\"", stdout); psetup_link(&selected, 0, NULL);
        fputs("\">Manage this installation</a></p>", stdout); psetup_end(); sqlite3_close(db); return 1;
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n");
    if (!scoped) fputs("Access-Control-Allow-Origin: *\r\n", stdout);
    printf("Cache-Control: %sno-store\r\nConnection: close\r\n\r\n", scoped ? "private, " : "");
    /* Private installations are resolved only through session-derived context.
     * The existing actor query remains a projection of authored public rows. */
    if (!scoped && (!bfpg_token(actor, sizeof(actor)) || (*scenario && !bfpg_token(scenario, sizeof(scenario))))) {
        fputs("{\"error\":\"invalid_public_coordinate\"}\n", stdout); sqlite3_close(db); return 1;
    }
    printf("{\"schema\":\"bonfyre-pack-closure.v1\",\"actor\":\"%s\"", actor);
    if (*scenario) printf(",\"scenario\":\"%s\"", scenario);

    /* Overlays, in composition order. Anything outside the closed verb list is
     * refused out loud. */
    printf(",\"applied\":[");
    rows = 0;
    {
        int refused = 0;
        char refusals[8192];
        size_t rn = 0;
        refusals[0] = '\0';
        if (sqlite3_prepare_v2(db,
                "SELECT o.overlay, o.overrides, o.layer, o.verb, o.target, "
                "  COALESCE(o.value,''), o.why FROM pack_overlay o "
                "WHERE o.status = 'published' AND o.namespace='root' "
                "ORDER BY CASE o.layer WHEN 'official' THEN 0 WHEN 'institution' THEN 1 "
                "  WHEN 'community' THEN 2 WHEN 'household' THEN 3 ELSE 4 END, o.id",
                -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *ov   = (const char *)sqlite3_column_text(st, 0);
                const char *over = (const char *)sqlite3_column_text(st, 1);
                const char *lay  = (const char *)sqlite3_column_text(st, 2);
                const char *verb = (const char *)sqlite3_column_text(st, 3);
                const char *targ = (const char *)sqlite3_column_text(st, 4);
                const char *val  = (const char *)sqlite3_column_text(st, 5);
                char ew[1024], et[512];
                int oi = pack_find(nodes, count, ov);
                if (oi < 0 || !nodes[oi].active) continue;      /* not in this closure */
                if (pack_find(nodes, count, over) < 0) continue; /* overrides nothing here */

                if (!pack_verb_allowed(verb)) {
                    /* Silently dropping this is how a community ends up believing
                     * it configured something it did not. */
                    pack_refusal_append(refusals, sizeof(refusals), &rn, ov, verb,
                        "not one of hide, add, specialize, present, tighten or default; an overlay "
                        "has no verb for granting authority, widening disclosure, rewriting identity, "
                        "dropping evidence or promoting an observation to a fact");
                    refused++;
                    continue;
                }
                if (!targ || !*targ) {
                    pack_refusal_append(refusals, sizeof(refusals), &rn, ov, verb, "missing target");
                    refused++;
                    continue;
                }
                json_escape_into(targ ? targ : "", et, sizeof(et));
                json_escape_into((const char *)sqlite3_column_text(st, 6), ew, sizeof(ew));

                if (!strcmp(verb, "hide") && !strncmp(targ, "view:", 5)) {
                    /* Hiding a grammar is presentation, not policy: the pattern
                     * behind it still answers, and a machine asking for it gets
                     * the same bytes. This says only that this person does not
                     * want to be handed that view. */
                    if (view_hidden_n < 16)
                        snprintf(view_hidden[view_hidden_n++], 32, "%s", targ + 5);
                } else if (!strcmp(verb, "present") && !strncmp(targ, "view:", 5)) {
                    if (view_order_n < 16) {
                        snprintf(view_order[view_order_n], 32, "%s", targ + 5);
                        view_rank[view_order_n] = val && *val ? atoi(val) : 99;
                        view_order_n++;
                    }
                } else if (!strcmp(verb, "hide")) {
                    int hi = pack_find(nodes, count, targ);
                    if (hi >= 0) {
                        nodes[hi].hidden = 1;
                        snprintf(nodes[hi].hidden_by, sizeof(nodes[hi].hidden_by), "%s", ov);
                    }
                } else if (!strcmp(verb, "add")) {
                    count = pack_close(db, nodes, count, targ, 1, 0);
                } else if (!strcmp(verb, "tighten") && !strcmp(targ, "audience")) {
                    int r = pack_audience_rank(val);
                    /* Narrower only. A later layer may tighten further and can
                     * never widen what an earlier one narrowed. */
                    if (r >= 0 && (audience_rank < 0 || r < audience_rank)) {
                        audience_rank = r;
                        snprintf(audience, sizeof(audience), "%s", val);
                    }
                } else if (!strcmp(verb, "default") && !strcmp(targ, "keep")) {
                    snprintf(keep, sizeof(keep), "%s", val);
                }
                printf("%s{\"overlay\":\"%s\",\"overrides\":\"%s\",\"layer\":\"%s\","
                       "\"verb\":\"%s\",\"target\":\"%s\",\"value\":\"%s\",\"why\":\"%s\"}",
                       rows ? "," : "", ov, over, lay, verb, et, val ? val : "", ew);
                rows++;
            }
            sqlite3_finalize(st);
            st = NULL;
        }
        printf("],\"applied_count\":%d,\"refused\":[%s],\"refused_count\":%d",
               rows, refusals, refused);
    }

    /* The closure itself. */
    printf(",\"packs\":[");
    rows = 0;
    for (i = 0; i < count; i++) {
        if (nodes[i].hidden) continue;
        printf("%s{\"pack\":\"%s\",\"layer\":\"%s\",\"kind\":\"%s\",\"version\":\"%s\","
               "\"root\":\"%s\",\"reached\":\"%s\"}",
               rows ? "," : "", nodes[i].pack, nodes[i].layer, nodes[i].kind,
               nodes[i].version, nodes[i].root,
               nodes[i].active ? "activated" : "dependency");
        rows++;
    }
    printf("],\"pack_count\":%d,\"hidden\":[", rows);
    {
        int h = 0;
        for (i = 0; i < count; i++) {
            if (!nodes[i].hidden) continue;
            printf("%s{\"pack\":\"%s\",\"hidden_by\":\"%s\"}",
                   h ? "," : "", nodes[i].pack, nodes[i].hidden_by);
            h++;
        }
        printf("],\"hidden_count\":%d", h);
    }

    /* What this closure needs of the world, of the machine and of the surface.
     * Declared by the packs that are actually in it -- a demand from a pack
     * somebody hid is not a demand. */
    {
        static const char *kinds[] = { "internet", "external", "capability", "renderer", "surface", NULL };
        int k;
        for (k = 0; kinds[k]; k++) {
            int d = 0;
            printf(",\"%s_demand\":[", kinds[k]);
            if (sqlite3_prepare_v2(db,
                    "SELECT pack, needs, COALESCE(preferred_sources,''), "
                    "  COALESCE(fields,''), COALESCE(body_regime,''), "
                    "  COALESCE(reheat_when,''), COALESCE(optional,'') "
                    "FROM pack_demand WHERE demand_kind = ? AND status='published' AND namespace='root'",
                    -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, kinds[k], -1, SQLITE_TRANSIENT);
                while (sqlite3_step(st) == SQLITE_ROW) {
                    const char *pk = (const char *)sqlite3_column_text(st, 0);
                    int pi = pack_find(nodes, count, pk);
                    char e[5][2048];
                    int c;
                    if (pi < 0 || nodes[pi].hidden) continue;
                    for (c = 0; c < 5; c++)
                        json_escape_into((const char *)sqlite3_column_text(st, c + 1),
                                         e[c], sizeof(e[c]));
                    printf("%s{\"pack\":\"%s\",\"needs\":\"%s\",\"preferred_sources\":\"%s\","
                           "\"fields\":\"%s\",\"body_regime\":\"%s\",\"reheat_when\":\"%s\"",
                           d ? "," : "", pk, e[0], e[1], e[2], e[3], e[4]);
                    {
                        char eo[2048];
                        json_escape_into((const char *)sqlite3_column_text(st, 6),
                                         eo, sizeof(eo));
                        if (eo[0]) printf(",\"optional\":\"%s\"", eo);
                    }
                    printf("}");
                    d++;
                }
                sqlite3_finalize(st);
                st = NULL;
            }
            printf("]");
        }
    }

    /* The views this closure resolves to: what its packs asked for, minus what
     * an overlay hid, ordered by what an overlay preferred. This is the answer
     * the view control wants, and it is a closure result rather than a menu of
     * everything that exists. */
    {
        char offered[24][32];
        int on = 0, k, j;
        printf(",\"views\":[");
        if (sqlite3_prepare_v2(db,
                "SELECT pack, needs FROM pack_demand "
                "WHERE demand_kind = 'renderer' AND status = 'published' AND namespace='root'",
                -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *pk = (const char *)sqlite3_column_text(st, 0);
                const char *needs = (const char *)sqlite3_column_text(st, 1);
                int pi = pack_find(nodes, count, pk);
                char buf[512], *tok, *save = NULL;
                if (pi < 0 || nodes[pi].hidden || !needs) continue;
                snprintf(buf, sizeof(buf), "%s", needs);
                for (tok = strtok_r(buf, ", ", &save); tok; tok = strtok_r(NULL, ", ", &save)) {
                    int dup = 0, hid = 0;
                    for (k = 0; k < on; k++) if (!strcmp(offered[k], tok)) dup = 1;
                    for (k = 0; k < view_hidden_n; k++)
                        if (!strcmp(view_hidden[k], tok)) hid = 1;
                    if (dup || hid || on >= 24) continue;
                    snprintf(offered[on++], 32, "%s", tok);
                }
            }
            sqlite3_finalize(st);
            st = NULL;
        }
        /* Preferred first, in the order the overlay asked for; then the rest. */
        for (j = 0, k = 0; k < view_order_n; k++) {
            int best = -1, bi = -1, q;
            for (q = 0; q < view_order_n; q++) {
                if (view_rank[q] < 0) continue;
                if (best < 0 || view_rank[q] < best) { best = view_rank[q]; bi = q; }
            }
            if (bi < 0) break;
            view_rank[bi] = -1;
            printf("%s{\"view\":\"%s\",\"why\":\"preferred by an overlay\"}",
                   j ? "," : "", view_order[bi]);
            j++;
        }
        for (k = 0; k < on; k++) {
            int already = 0, q;
            for (q = 0; q < view_order_n; q++)
                if (!strcmp(view_order[q], offered[k])) already = 1;
            if (already) continue;
            printf("%s{\"view\":\"%s\",\"why\":\"a pack in this closure asks for it\"}",
                   j ? "," : "", offered[k]);
            j++;
        }
        printf("],\"views_hidden\":[");
        for (k = 0; k < view_hidden_n; k++)
            printf("%s\"%s\"", k ? "," : "", view_hidden[k]);
        printf("]");
    }

    printf(",\"policy\":{\"audience_ceiling\":\"%s\",\"keep_floor\":\"%s\"}",
           *audience ? audience : "", *keep ? keep : "");
    printf(",\"cannot_infer\":\"a closure decides what is composed and offers verbs; "
           "Authority decides every effect at effect time and is not this organ. An "
           "activated pack grants nothing.\"}\n");

    sqlite3_close(db);
    fflush(stdout);
    return 1;
}

/* The subject index: one addressed subject, every projection of it.
 *
 * Section 43, the central compression law:
 *
 *     ONE SEMANTIC DEFINITION -> many projections, carriers, actors, worlds,
 *     scales
 *
 *     Therefore avoid: separate social object, separate ERP object, separate
 *     API object, separate agent object, separate finance object, separate
 *     mobile object, separate embed object, separate analytics object -- when
 *     one addressed subject can lawfully project all of them.
 *
 * The estate holds 326 subjects, 274 addresses and 319 presences and had no way
 * to enumerate any of them. Every surface therefore started from a pattern --
 * a list of records of one type -- which is the shape the compression law says
 * not to build: the social list, the market list and the work list, side by
 * side, with nothing saying they are the same subjects seen differently.
 *
 * This answers the other question. Not "what records does this type hold" but
 * "what is this subject, everywhere at once": its addresses, and its presences,
 * which are the territory / scope / world / representation it is projected
 * into. A subject in four territories is one row here, not four.
 *
 * Cannot infer: a presence is a projection, not a permission. That a subject is
 * present in `market` scope says the estate projects it there; whether this
 * caller may see or act on it is decided at read and at effect time by owners
 * that are not this index.
 */
static int serve_subject_index(const char *method, const char *target,
                               const char *query, const char *accept) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char want_id[192] = {0}, want_kind[64] = {0};
    int rows = 0, limit = 400;
    const char *dbpath = getenv("BONFYRE_MATRIX_DB");

    if (strcmp(target, "/subjects") && strcmp(target, "/subjects/")) return 0;
    if (strcmp(method, "GET")) {
        protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n");
        return 1;
    }
    (void)accept;   /* one projection: this is the machine index the pages read */
    if (!dbpath || !*dbpath) dbpath = "/var/lib/bonfyre/matrix.db";
    if (sqlite3_open_v2(dbpath, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        protocol_reply(503, "application/json", "{\"error\":\"matrix_unavailable\"}\n");
        return 1;
    }

    (void)query_value(query, "id", want_id, sizeof(want_id));
    (void)query_value(query, "kind", want_kind, sizeof(want_kind));
    {
        char lim[16];
        if (query_value(query, "limit", lim, sizeof(lim))) {
            int v = atoi(lim);
            if (v > 0 && v <= 1000) limit = v;
        }
    }

    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
           "Access-Control-Allow-Origin: *\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n");
    printf("{\"schema\":\"bonfyre-subject-index.v1\",\"subjects\":[");

    if (sqlite3_prepare_v2(db,
            "SELECT s.subject_id, s.subject_kind, "
            "  (SELECT count(*) FROM matrix_addresses a WHERE a.subject_id = s.subject_id), "
            "  (SELECT count(*) FROM matrix_presences p WHERE p.subject_id = s.subject_id "
            "     AND p.active != 0), "
            "  (SELECT count(*) FROM matrix_subject_relations r "
            "     WHERE r.from_subject = s.subject_id OR r.to_subject = s.subject_id), "
            /* Something a person can read. A subject id is a digest for most
             * kinds, and eighty-four rows of budgetrequest:01135de0 is a list
             * nobody can use -- the estate knows an address for most of them
             * and was not saying so. */
            "  (SELECT a.address FROM matrix_addresses a WHERE a.subject_id = s.subject_id "
            "     ORDER BY length(a.address), a.address LIMIT 1) "
            "FROM matrix_subjects s "
            "WHERE (?1 = '' OR s.subject_id = ?1) AND (?2 = '' OR s.subject_kind = ?2) "
            "ORDER BY 3 DESC, 4 DESC, s.subject_kind, s.subject_id LIMIT ?3",
            -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        printf("],\"count\":0,\"error\":\"index_query_failed\"}\n");
        fflush(stdout);
        return 1;
    }
    sqlite3_bind_text(st, 1, want_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, want_kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 3, limit);

    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *sid = (const char *)sqlite3_column_text(st, 0);
        const char *kind = (const char *)sqlite3_column_text(st, 1);
        char esid[512], ekind[192];
        sqlite3_stmt *q = NULL;
        int inner = 0;
        if (!sid || !kind) continue;
        json_escape_into(sid, esid, sizeof(esid));
        json_escape_into(kind, ekind, sizeof(ekind));
        printf("%s{\"id\":\"%s\",\"kind\":\"%s\",\"addresses\":%d,\"presences\":%d,"
               "\"relations\":%d",
               rows ? "," : "", esid, ekind,
               sqlite3_column_int(st, 2), sqlite3_column_int(st, 3),
               sqlite3_column_int(st, 4));
        {
            const char *addr = (const char *)sqlite3_column_text(st, 5);
            if (addr && *addr) {
                char ea[1024];
                json_escape_into(addr, ea, sizeof(ea));
                printf(",\"label\":\"%s\"", ea);
            }
        }

        /* The projections themselves, only when one subject was asked for. The
         * index stays a list; a constellation is what you get when you name
         * the subject. */
        if (*want_id) {
            printf(",\"address\":[");
            if (sqlite3_prepare_v2(db,
                    "SELECT address, address_kind FROM matrix_addresses "
                    "WHERE subject_id = ? ORDER BY address", -1, &q, NULL) == SQLITE_OK) {
                sqlite3_bind_text(q, 1, sid, -1, SQLITE_TRANSIENT);
                while (sqlite3_step(q) == SQLITE_ROW) {
                    char ea[1024], ek[128];
                    json_escape_into((const char *)sqlite3_column_text(q, 0), ea, sizeof(ea));
                    json_escape_into((const char *)sqlite3_column_text(q, 1), ek, sizeof(ek));
                    printf("%s{\"address\":\"%s\",\"kind\":\"%s\"}", inner++ ? "," : "", ea, ek);
                }
                sqlite3_finalize(q);
            }
            printf("],\"presence\":[");
            inner = 0;
            if (sqlite3_prepare_v2(db,
                    "SELECT territory, scope, world, representation FROM matrix_presences "
                    "WHERE subject_id = ? AND active != 0 "
                    "ORDER BY territory, scope", -1, &q, NULL) == SQLITE_OK) {
                sqlite3_bind_text(q, 1, sid, -1, SQLITE_TRANSIENT);
                while (sqlite3_step(q) == SQLITE_ROW) {
                    char a[64], b[64], c[64], d[64];
                    json_escape_into((const char *)sqlite3_column_text(q, 0), a, sizeof(a));
                    json_escape_into((const char *)sqlite3_column_text(q, 1), b, sizeof(b));
                    json_escape_into((const char *)sqlite3_column_text(q, 2), c, sizeof(c));
                    json_escape_into((const char *)sqlite3_column_text(q, 3), d, sizeof(d));
                    printf("%s{\"territory\":\"%s\",\"scope\":\"%s\",\"world\":\"%s\","
                           "\"representation\":\"%s\"}", inner++ ? "," : "", a, b, c, d);
                }
                sqlite3_finalize(q);
            }
            printf("],\"relation\":[");
            inner = 0;
            if (sqlite3_prepare_v2(db,
                    "SELECT relation, to_subject, 'out' FROM matrix_subject_relations "
                    "  WHERE from_subject = ? "
                    "UNION ALL "
                    "SELECT relation, from_subject, 'in' FROM matrix_subject_relations "
                    "  WHERE to_subject = ? LIMIT 60", -1, &q, NULL) == SQLITE_OK) {
                sqlite3_bind_text(q, 1, sid, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(q, 2, sid, -1, SQLITE_TRANSIENT);
                while (sqlite3_step(q) == SQLITE_ROW) {
                    char r[128], o[512];
                    json_escape_into((const char *)sqlite3_column_text(q, 0), r, sizeof(r));
                    json_escape_into((const char *)sqlite3_column_text(q, 1), o, sizeof(o));
                    printf("%s{\"relation\":\"%s\",\"other\":\"%s\",\"direction\":\"%s\"}",
                           inner++ ? "," : "", r, o,
                           (const char *)sqlite3_column_text(q, 2));
                }
                sqlite3_finalize(q);
            }
            printf("]");
        }
        printf("}");
        rows++;
    }
    sqlite3_finalize(st);

    printf("],\"count\":%d,\"kinds\":{", rows);
    if (sqlite3_prepare_v2(db,
            "SELECT subject_kind, count(*) FROM matrix_subjects "
            "GROUP BY 1 ORDER BY 2 DESC", -1, &st, NULL) == SQLITE_OK) {
        int k = 0;
        while (sqlite3_step(st) == SQLITE_ROW) {
            char ek[192];
            json_escape_into((const char *)sqlite3_column_text(st, 0), ek, sizeof(ek));
            printf("%s\"%s\":%d", k++ ? "," : "", ek, sqlite3_column_int(st, 1));
        }
        sqlite3_finalize(st);
    }
    printf("},\"cannot_infer\":\"a presence is a projection and never a permission; "
           "whether this caller may read or act on a subject is decided by owners that "
           "are not this index\"}\n");
    sqlite3_close(db);
    fflush(stdout);
    return 1;
}

static int serve_foreign_edges(const char *method, const char *target, const char *accept,
                               const char *bearer, const char *auth_db) {
    static char out[262144];
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    char bin[PATH_MAX];
    const char *scope = "public";

    if (strcmp(target, "/foreign/edges") && strcmp(target, "/foreign/edges/")) return 0;
    if (strcmp(method, "GET")) {
        protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n");
        return 1;
    }
    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    if (snprintf(bin, sizeof(bin), "%s/bonfyre-project", bindir) >= (int)sizeof(bin)) return 0;

    /* Widening by what is actually held, in one direction only. A caller with no
     * session is the public, and the public sees the public scope.
     *
     * The grants in this estate are institutional -- `nightgrid:admin`,
     * `bonfyre:admin` -- so the test has to be institutional too. An earlier
     * draft asked for a bare "admin", which an admin of Night Grid does not
     * hold and should not: running one estate is not authority over this
     * institution's observations of the outside world.
     *
     * self-private and partner-shared are deliberately not derived from a role.
     * They are per-actor and per-arrangement, and inferring them from a rank
     * would hand one person's saved candidate to everyone of equal standing. */
    if (session_valid(bearer, auth_db)) {
        if (surface_authority_satisfied("bonfyre:admin", bearer, auth_db))
            scope = "restricted";
        else if (surface_authority_satisfied("nightgrid:admin", bearer, auth_db) ||
                 surface_authority_satisfied("upper90:admin", bearer, auth_db))
            scope = "institution-private";
    }

    out[0] = '\0';
    {
        char *argv[] = { bin, (char*)"foreign", (char*)"edges",
                         (char*)"--scope", (char*)scope, NULL };
        if (auth_call(argv, NULL, out, sizeof(out)) != 0 || !out[0]) {
            protocol_reply(503, "application/json", "{\"error\":\"foreign_graph_unavailable\"}\n");
            return 1;
        }
    }
    if (!wants_html(accept)) { protocol_reply(200, "application/json", out); return 1; }

    {
        static char body[262144];
        size_t n = (size_t)snprintf(body, sizeof(body),
            BF_PAGE_OPEN "Observed edges \xc2\xb7 Aurekai" BF_PAGE_AFTER_TITLE
            "<main><section class=\"hero wrap\" style=\"padding:56px 0 10px\">"
            "<div class=\"eyebrow\"><i></i><span>Aurekai / foreign graph</span></div>"
            "<h1>What the networks <em>say.</em></h1>"
            "<p class=\"lede\">Edges observed in external federation state, each carrying which "
            "federation said it, over which carrier, when, and against what evidence. None of "
            "them is an institutional relation. A follow is not a friendship, a listing is not "
            "employment, and a network participant is not an admitted provider &mdash; crossing "
            "that line is a separate act, and nothing on this page has crossed it.</p>"
            "</section><div class=\"wrap\"><div class=\"doors\">", site_nav_html());
        {
            const char *cursor = out;
            while ((cursor = strstr(cursor, "\"from\":\"")) && n < sizeof(body) - 2048) {
                char f[256], pr[128], to[256], fed[128], car[128], at[64], sc[64];
                const char *end;
                size_t len;
                #define TAKE(key, dst) do {                                        \
                    const char *k = strstr(cursor, "\"" key "\":\"");               \
                    dst[0] = '\0';                                                  \
                    if (k) { k += strlen(key) + 4; end = strchr(k, '"');             \
                        if (end) { len = (size_t)(end - k);                          \
                            if (len >= sizeof(dst)) len = sizeof(dst) - 1;           \
                            memcpy(dst, k, len); dst[len] = '\0'; } }                \
                } while (0)
                TAKE("from", f); TAKE("predicate", pr); TAKE("to", to);
                TAKE("federation", fed); TAKE("carrier", car);
                TAKE("observed_at", at); TAKE("scope", sc);
                #undef TAKE
                n += (size_t)snprintf(body + n, sizeof(body) - n,
                    "<article class=\"door\"><p class=\"eyebrow\"><i></i><span>%s</span></p>"
                    "<h3>%s</h3><p><strong>%s</strong> %s <strong>%s</strong></p>"
                    "<p style=\"font-size:13px;opacity:.72\">said by %s over %s, observed %s</p>"
                    "</article>", sc, pr, f, pr, to, fed, car, at);
                cursor += 8;
            }
        }
        {
            const char *w = strstr(out, "\"withheld_for_scope\":");
            int withheld = w ? atoi(w + 21) : 0;
            n += (size_t)snprintf(body + n, sizeof(body) - n,
                "</div></div><div class=\"wrap\"><p class=\"lede\" style=\"font-size:14px;opacity:.75\">"
                "Held at scope <code>%s</code>. %d edge%s withheld because this session is not "
                "admitted to see them &mdash; the count is not their content, and the filter ran "
                "in the query rather than on the way out.</p></div></main>"
                "<script defer src=\"/assets/matrix-htmx.js\"></script></body></html>\n",
                scope, withheld, withheld == 1 ? " is" : "s are");
        }
        protocol_reply(200, "text/html; charset=utf-8", body);
    }
    return 1;
}

static int serve_twins_page(const char *method, const char *target, const char *accept) {
    static char out[262144];
    static char body[131072];
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    char bin[PATH_MAX];
    size_t n = 0;

    if (strcmp(target, "/twins") && strcmp(target, "/twins/")) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    if (snprintf(bin, sizeof(bin), "%s/bonfyre-project", bindir) >= (int)sizeof(bin)) return 0;

    out[0] = '\0';
    {
        char *argv[] = { bin, (char*)"foreign", (char*)"twins", NULL };
        if (auth_call(argv, NULL, out, sizeof(out)) != 0 || !out[0]) {
            protocol_reply(503, "application/json", "{\"error\":\"twin_owner_unavailable\"}\n");
            return 1;
        }
    }
    if (!wants_html(accept)) { protocol_reply(200, "application/json", out); return 1; }

    n = (size_t)snprintf(body, sizeof(body),
        BF_PAGE_OPEN "Observed \xc2\xb7 Aurekai" BF_PAGE_AFTER_TITLE
        "<main><section class=\"hero wrap\" style=\"padding:56px 0 10px\">"
        "<div class=\"eyebrow\"><i></i><span>Aurekai / foreign twins</span></div>"
        "<h1>What we have <em>observed.</em></h1>"
        "<p class=\"lede\">Every external resource this institution has fetched, with what "
        "became of it. Observing something records that it answered. Promoting it means we "
        "chose to be able to name it, identified by the bytes we actually retained. Claiming "
        "it is an assertion that some institution holds a relationship to it. None of the "
        "three is ownership, and each is a separate act.</p>"
        "</section><div class=\"wrap\"><div class=\"doors\">", site_nav_html());

    {
        const char *cursor = out;
        while ((cursor = strstr(cursor, "\"url\":\"")) && n < sizeof(body) - 2048) {
            char url[600], obs[16], chg[16], prom[16], claims[16], status[16];
            const char *end;
            size_t len;
            cursor += 7;
            end = strchr(cursor, '"');
            if (!end) break;
            len = (size_t)(end - cursor);
            if (len >= sizeof(url)) len = sizeof(url) - 1;
            memcpy(url, cursor, len); url[len] = '\0';
            cursor = end;
            surface_scan_number(cursor, "\"observations\":", obs, sizeof(obs));
            surface_scan_number(cursor, "\"content_changes\":", chg, sizeof(chg));
            surface_scan_token(cursor, "\"promoted\":", prom, sizeof(prom));
            surface_scan_number(cursor, "\"claims\":", claims, sizeof(claims));
            surface_scan_number(cursor, "\"latest_status\":", status, sizeof(status));

            n += (size_t)snprintf(body + n, sizeof(body) - n,
                "<article class=\"door\"><p class=\"eyebrow\"><i></i>%s</p>"
                "<h3 style=\"word-break:break-all;font-size:1.0em\">%s</h3>"
                "<p>%s observation%s, %s content change%s. Last answered %s.</p>"
                "<div class=\"ways\"><span class=\"way\">%s</span></div></article>",
                (claims[0] && strcmp(claims, "0")) ? "claimed"
                    : (!strcmp(prom, "true") ? "promoted, unclaimed" : "observed only"),
                url, obs[0] ? obs : "0", (obs[0] == '1' && !obs[1]) ? "" : "s",
                chg[0] ? chg : "0", (chg[0] == '1' && !chg[1]) ? "" : "s",
                status[0] ? status : "?",
                !strcmp(prom, "true") ? "addressable subject" : "not promoted");
        }
    }
    n += (size_t)snprintf(body + n, sizeof(body) - n,
        "</div></div>"
        "<section class=\"wrap\" style=\"padding:24px 0 56px;opacity:.8\">"
        "<p class=\"lede\" style=\"font-size:.94em\">A resource that has never yielded bytes "
        "cannot be promoted: there would be nothing to identify it by, and an identity whose "
        "digest we invented would be a claim wearing the shape of an observation. "
        "See also <a href=\"/observations\">what changed</a>.</p></section>"
        "</main></body></html>\n");
    protocol_reply(200, "text/html; charset=utf-8", body);
    return 1;
}

static int serve_observations_page(const char *method, const char *target,
                                   const char *accept) {
    static char out[262144];
    static char body[65536];
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    char bin[PATH_MAX];
    size_t n = 0;

    if (strcmp(target, "/observations") && strcmp(target, "/observations/")) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    if (snprintf(bin, sizeof(bin), "%s/bonfyre-project", bindir) >= (int)sizeof(bin)) return 0;

    out[0] = '\0';
    {
        char *argv[] = { bin, (char*)"foreign", (char*)"conflicts", NULL };
        if (auth_call(argv, NULL, out, sizeof(out)) != 0 || !out[0]) {
            protocol_reply(503, "application/json",
                "{\"error\":\"observation_owner_unavailable\"}\n");
            return 1;
        }
    }
    if (!wants_html(accept)) { protocol_reply(200, "application/json", out); return 1; }

    /* Render the owner's own JSON into rows. A scanner, not a parser: the
     * fields are written by one program in one shape. */
    {
        const char *cursor = out;
        n = (size_t)snprintf(body, sizeof(body),
            BF_PAGE_OPEN "Observations \xc2\xb7 Aurekai" BF_PAGE_AFTER_TITLE
            "<main><section class=\"hero wrap\" style=\"padding:56px 0 10px\">"
            "<div class=\"eyebrow\"><i></i><span>Aurekai / evidence</span></div>"
            "<h1>What we saw, and <em>when it changed.</em></h1>"
            "<p class=\"lede\">Every observation of an external resource is kept as its own "
            "receipt. A 404 today does not erase bytes fetched last week &mdash; it records "
            "that the resource stopped answering. Those are two different questions, so this "
            "page reports both: what content we currently hold, and whether the source still "
            "serves it.</p>"
            "<p class=\"lede\" style=\"opacity:.72;font-size:.94em\">Below are resources whose "
            "observations disagree with each other. A changed source is ordinary; the "
            "institution only has to know its earlier belief is no longer current.</p>"
            "</section><div class=\"wrap\"><div class=\"doors\">", site_nav_html());

        while ((cursor = strstr(cursor, "\"source_url\":\"")) && n < sizeof(body) - 2048) {
            char url[512], changes[16], reach[16], held[16], kind[48], status[16];
            const char *end;
            size_t len;
            cursor += 14;
            end = strchr(cursor, '"');
            if (!end) break;
            len = (size_t)(end - cursor);
            if (len >= sizeof(url)) len = sizeof(url) - 1;
            memcpy(url, cursor, len); url[len] = '\0';
            cursor = end;
            surface_scan_number(cursor, "\"content_changes\":", changes, sizeof(changes));
            surface_scan_token(cursor, "\"content_still_reachable\":", reach, sizeof(reach));
            surface_scan_token(cursor, "\"ever_retained_content\":", held, sizeof(held));
            surface_scan_string(cursor, "\"latest_kind\":\"", kind, sizeof(kind));
            surface_scan_number(cursor, "\"latest_status\":", status, sizeof(status));

            n += (size_t)snprintf(body + n, sizeof(body) - n,
                "<article class=\"door\"><p class=\"eyebrow\"><i></i>%s</p>"
                "<h3 style=\"word-break:break-all;font-size:1.02em\">%s</h3>"
                "<p>%s content change%s recorded. Latest observation answered %s.</p>"
                "<div class=\"ways\"><span class=\"way\">%s</span></div></article>",
                !strcmp(held, "false") ? "never retained content"
                    : (!strcmp(reach, "true") ? "changed, still reachable"
                                              : "held bytes, no longer served"),
                url, changes[0] ? changes : "0",
                (changes[0] == '1' && !changes[1]) ? "" : "s",
                status[0] ? status : "?",
                kind[0] ? kind : "unknown");
        }
        n += (size_t)snprintf(body + n, sizeof(body) - n,
            "</div></div>"
            "<section class=\"wrap\" style=\"padding:24px 0 56px;opacity:.8\">"
            "<p class=\"lede\" style=\"font-size:.94em\">Ask for one resource's full history "
            "with <code>bonfyre-project foreign history &lt;url&gt;</code>, or over MCP with "
            "<code>foreign_resolution</code>. Nothing here is deleted when it is superseded."
            "</p></section></main></body></html>\n");
        protocol_reply(200, "text/html; charset=utf-8", body);
    }
    return 1;
}

static int serve_night_page(const char *method, const char *target, const char *host,
                            const char *accept) {
    static char body[16384];
    const char *territory;
    const NightProjection *proj = NULL;
    size_t i;

    if (strcmp(target, "/tonight") && strcmp(target, "/tonight/")) return 0;
    if (strcmp(method, "GET")) return 0;

    territory = territory_of(host);
    for (i = 0; i < sizeof(NIGHT_PROJECTIONS)/sizeof(NIGHT_PROJECTIONS[0]); i++)
        if (territory && !strcmp(territory, NIGHT_PROJECTIONS[i].territory))
            proj = &NIGHT_PROJECTIONS[i];
    /* A territory with no declared projection gets the public one rather than a
     * 404: the demo is public, and an unmapped host is a gap in this table, not
     * a reason to deny a reader. */
    if (!proj) proj = &NIGHT_PROJECTIONS[1];

    if (!wants_html(accept)) {
        /* A machine asking for /tonight is asking which projection this host
         * serves, and where to read it. */
        snprintf(body, sizeof(body),
            "{\"institution\":\"nightgrid\",\"world\":\"DEMO\",\"territory\":\"%s\","
            "\"pattern\":\"%s\",\"surface\":\"/_surface/%s\","
            "\"note\":\"one demo institution projected per territory; the records are "
            "the same subjects in every projection\"}\n",
            proj->territory, proj->pattern, proj->pattern);
        protocol_reply(200, "application/json", body);
        return 1;
    }

    snprintf(body, sizeof(body),
        BF_PAGE_OPEN "Tonight \xc2\xb7 Deep Ellum Night Grid" BF_PAGE_AFTER_TITLE
        "<main><section class=\"hero wrap\" style=\"padding:56px 0 10px\">"
        "<div class=\"eyebrow\"><i></i><span>%s</span></div>"
        "<h1>%s</h1>"
        "<p class=\"lede\">%s</p>"
        "<p class=\"lede\" style=\"opacity:.72;font-size:.94em\">Every venue, act and "
        "event here is synthetic. This is a demo institution executing in world DEMO: "
        "it observes nothing real, settles nothing, and its economics are proposals "
        "rather than revenue.</p>"
        "</section>"
        "<div class=\"wrap\"><div class=\"doors\" hx-get=\"/_surface/%s\" "
        "hx-trigger=\"load\" hx-swap=\"innerHTML\">%s</div></div>"
        "<section class=\"wrap\" style=\"padding:24px 0 56px;opacity:.8\">"
        "<p class=\"lede\" style=\"font-size:.94em\">The same five subjects are served to "
        "<a href=\"/_surface/night-life\">life</a>, "
        "<a href=\"/_surface/night-live\">live</a>, "
        "<a href=\"/_surface/night-shop\">shop</a> and "
        "<a href=\"/_surface/night-work\">work</a> with different fields. "
        "Open <a href=\"/_surface/\">the surface catalogue</a> to see what each "
        "projection reads and what it requires.</p></section>"
        "</main><script defer src=\"/assets/matrix-htmx.js\"></script></body></html>\n",
        site_nav_html(), proj->eyebrow, proj->heading, proj->lede,
        proj->pattern, proj->fallback);
    protocol_reply(200, "text/html; charset=utf-8", body);
    return 1;
}

typedef struct {
    char world[128], title[768], character[96], lede[2048], subject[512];
    char sources[4096], grammars[1024], cannot_infer[2048];
    char site_id[128], program_id[128];
    char source_collections[4096], capability_programs[4096];
} BfWorldFederation;

#endif /* BONFYRE_APERTURE_FOREIGN_AND_PACKS_H */
