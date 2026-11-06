#ifndef BONFYRE_SURFACE_CATALOGUE_ROUTES_H
#define BONFYRE_SURFACE_CATALOGUE_ROUTES_H
/* Extracted verbatim from main.c lines 1017-1990 (mechanical split, no logic changes). */
static int surface_safe_order(const char *text) {
    int have_ident = 0;
    if (!text || !*text || strlen(text) > 96) return 0;
    for (; *text; text++) {
        if (isalnum((unsigned char)*text) || *text == '_' || *text == '.') { have_ident = 1; continue; }
        if (*text == ',' || *text == ' ') continue;
        return 0;
    }
    return have_ident;
}

#define BF_SURFACE_MAX_FIELDS 32

/* Split "a,b,c" into validated column names. Returns the count, or 0 if any
 * name is not a plain identifier.
 *
 * `max` is the caller's array bound, not a preference. The first version took
 * a fixed char[8][64] and callers passed smaller arrays, capping the result
 * afterwards -- which is one write too late. GCC caught it; clang did not. */
static int surface_fields(const char *list, char (*out)[64], int max) {
    int n = 0;
    const char *p = list;
    if (!list || !*list || max <= 0) return 0;
    while (*p && n < max) {
        const char *start = p;
        size_t len;
        while (*p && *p != ',') p++;
        len = (size_t)(p - start);
        while (len && start[len - 1] == ' ') len--;
        while (len && *start == ' ') { start++; len--; }
        if (!len || len >= 64) return 0;
        memcpy(out[n], start, len);
        out[n][len] = '\0';
        if (!studio_safe_ident(out[n])) return 0;
        n++;
        if (*p == ',') p++;
    }
    /* More fields than the projection can carry is a broken descriptor, not a
     * reason to render the first few. It silently dropped the ninth column of
     * a nine-column pattern -- the field was authored, stored and served by
     * nothing, and the surface looked complete. */
    if (*p) return 0;
    return n;
}

/* The pattern catalogue: this institution's read surfaces, self-described.
 *
 * Cannot infer: a listed pattern -> a pattern this caller may read. The
 * catalogue reports the requirement; the pattern still enforces it. */
static int serve_surface_catalogue(const char *accept) {
    sqlite3 *db = cms_open_ro();
    sqlite3_stmt *st = NULL;
    int as_json, rows = 0;

    if (!db) return 0;
    if (!cms_type_registered(db, "surface_pattern")) { sqlite3_close(db); return 0; }
    as_json = wants_json(accept);

    printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
           as_json ? "application/json" : "text/html; charset=utf-8");
    if (as_json) printf("{\"surfaces\":[");
    else printf("<ul class=\"surface-catalogue\">");

    /* Section 68: a pattern declares the owner it reads, the extent it renders
     * at, the action layer it carries and the authority that layer demands.
     * extent and lineage are newer columns, so the catalogue asks for them and
     * falls back when the deployment has not been migrated -- an organ that
     * refused to list anything until the CMS caught up would take the whole
     * catalogue down for a column. */
    int have_extent = 0;
    if (sqlite3_prepare_v2(db,
            "SELECT pattern, reads_type, effect_class, COALESCE(requires_authority,''), "
            "       COALESCE(status_filter,'published'), COALESCE(visible_when,''), "
            "       COALESCE(reads_organ,''), COALESCE(extent,''), COALESCE(lineage,'') "
            "FROM surface_pattern WHERE status = 'published' ORDER BY pattern",
            -1, &st, NULL) == SQLITE_OK) {
        have_extent = 1;
    }
    if (!have_extent && sqlite3_prepare_v2(db,
            "SELECT pattern, reads_type, effect_class, COALESCE(requires_authority,''), "
            "       COALESCE(status_filter,'published'), COALESCE(visible_when,''), "
            "       COALESCE(reads_organ,'') "
            "FROM surface_pattern WHERE status = 'published' ORDER BY pattern",
            -1, &st, NULL) == SQLITE_OK) {
        have_extent = 0;
    }
    if (st) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *pat = (const char *)sqlite3_column_text(st, 0);
            const char *rd  = (const char *)sqlite3_column_text(st, 1);
            const char *ef  = (const char *)sqlite3_column_text(st, 2);
            const char *rq  = (const char *)sqlite3_column_text(st, 3);
            const char *sf  = (const char *)sqlite3_column_text(st, 4);
            const char *vw  = (const char *)sqlite3_column_text(st, 5);
            const char *org = (const char *)sqlite3_column_text(st, 6);
            /* What a pattern reads is a CMS type or a native organ. The
             * catalogue reported reads_type unconditionally, so an
             * organ-backed pattern advertised itself as reading nothing --
             * a machine could see it existed and not what it was for.
             * `source` says which kind of thing the name refers to. */
            const char *src = (org && *org) ? "organ" : "type";
            const char *ext = have_extent ? (const char *)sqlite3_column_text(st, 7) : "";
            const char *lin = have_extent ? (const char *)sqlite3_column_text(st, 8) : "";
            if (org && *org) rd = org;
            if (!pat) continue;
            if (!ext) ext = "";
            if (!lin) lin = "";
            /* Extent decides the action layer. Only a page or a workspace
             * carries one; a chip, card or section is a reference to a
             * surface, and there is no room to act inside a reference. A card
             * that advertised BOOK would be asking for a decision from a
             * thumbnail, so the catalogue reports no effect class for one
             * rather than trusting the row. */
            int carries_actions = !*ext
                                || !strcmp(ext, "page") || !strcmp(ext, "workspace");
            const char *eff = carries_actions ? (ef ? ef : "") : "";
            const char *req = carries_actions ? (rq ? rq : "") : "";
            if (as_json) {
                printf("%s{\"pattern\":\"%s\",\"href\":\"/_surface/%s\",\"reads\":\"%s\","
                       "\"source\":\"%s\",\"extent\":\"%s\","
                       "\"carries_action_layer\":%s,"
                       "\"effect_class\":\"%s\",\"requires_authority\":\"%s\","
                       "\"lineage\":\"%s\","
                       "\"status_filter\":\"%s\",\"visible_when\":\"%s\"}",
                       rows ? "," : "", pat, pat, rd ? rd : "", src, ext,
                       carries_actions ? "true" : "false", eff, req, lin,
                       sf ? sf : "", vw ? vw : "");
            } else {
                printf("<li><a href=\"/_surface/%s\">", pat);
                html(pat);
                printf("</a> <span class=\"reads\">reads %s ", src);
                html(rd ? rd : "");
                printf("</span> <span class=\"effect\">");
                html(ef ? ef : "");
                printf("</span>");
                if (rq && *rq) { printf(" <span class=\"requires\">requires "); html(rq); printf("</span>"); }
                printf("</li>");
            }
            rows++;
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    /* The extent each pattern is authored at is above. That it can be asked
     * for at a lower one is a property of the address, and a machine that is
     * not told so has to guess that ?extent exists. */
    if (as_json)
        printf("],\"count\":%d,\"extents\":[\"chip\",\"card\",\"section\",\"page\",\"workspace\"],"
               "\"extent_parameter\":\"?extent=<extent>, narrowing only\","
               "\"extent_note\":\"extent decides the action layer: chip, card and section are "
               "references to a surface and carry none\"}\n", rows);
    else printf("</ul>");
    fflush(stdout);
    return 1;
}

/* Does this session satisfy a pattern's declared authority?
 *
 * Two different questions wear the same word. A session scope ("write") is what
 * the account may do at all; an institutional grant ("nightgrid:write") is what
 * it may do inside one estate. The pattern compiler originally asked only the
 * first, so a persona holding institutional write on nightgrid was refused its
 * own institution's projection -- the grant was real, the check was asking
 * about something else.
 *
 * A requirement containing a colon is institutional: <institution>:<role>. A
 * bare word remains a session scope, so every pattern authored before this
 * keeps its meaning.
 *
 * Cannot infer: satisfying a pattern's authority -> authority over what the
 * records describe. This decides who may read the projection. */
static int surface_authority_satisfied(const char *requirement, const char *bearer,
                                       const char *auth_db) {
    char institution[64], role[32];
    const char *colon;
    size_t n;

    if (!requirement || !*requirement) return 1;
    colon = strchr(requirement, ':');
    if (!colon) return session_authorized(bearer, auth_db, requirement);

    n = (size_t)(colon - requirement);
    if (!n || n >= sizeof(institution)) return 0;
    memcpy(institution, requirement, n);
    institution[n] = '\0';
    snprintf(role, sizeof(role), "%s", colon + 1);
    if (!studio_safe_ident(institution) || !studio_safe_ident(role)) return 0;
    if (!valid_token(bearer)) return 0;
    {
        char out[2048];
        const char *bin = auth_binary();
        char *const argv[] = { (char*)bin, "--db", (char*)auth_db, "institution-authorize",
                               "--token", (char*)bearer, "--institution", institution,
                               "--role", role, NULL };
        if (auth_call(argv, NULL, out, sizeof(out)) != 0) return 0;
        return strstr(out, "\"authorized\":true") != NULL;
    }
}

static int split_cols(char *line, char *col[], int max);

/* The heading a labelled panel renders above its rows. Silent when the
 * descriptor named nothing, so an unlabelled pattern is unchanged. */
static void surface_heading(const char *title, const char *caption) {
    if (!title || !*title) return;
    printf("<h2 class=\"surface-title\">");
    html(title);
    printf("</h2>");
    if (caption && *caption) {
        printf("<p class=\"surface-caption\">");
        html(caption);
        printf("</p>");
    }
}

/* An organ name a descriptor may ask for: a bare `bonfyre-` identifier, never a
 * path. The CMS is content, and content must not be able to choose what this
 * organ executes -- only which of the estate's own organs is asked. */
static int surface_safe_binary(const char *s) {
    size_t i;
    if (!s || strncmp(s, "bonfyre-", 8)) return 0;
    for (i = 0; s[i]; i++) {
        if (!((s[i] >= 'a' && s[i] <= 'z') || (s[i] >= '0' && s[i] <= '9') || s[i] == '-'))
            return 0;
    }
    return i > 8 && i < 64;
}

/* Extent is the density dial, and it belongs to the address.
 *
 * Section 68: "Extent decides the action layer. Only a page or a workspace
 * carries one; a chip, card or section is a reference to a surface, and there
 * is no room to act inside a reference. A card offering BOOK asks for a
 * decision from a thumbnail."
 *
 * Until this existed the extent lived only in the descriptor, so a person
 * operating their own host could not shape the product they were served -- the
 * institution author chose the density once, for everyone. Reading the extent
 * from the query makes density something an end user sets and can hand to
 * someone else, because it is then part of the URL rather than a preference
 * hidden in a browser.
 *
 * It narrows only. A pattern authored at card extent asked for a workspace
 * would be granted an action layer nobody authored it to carry, so that is
 * refused rather than clamped: silently serving less than was asked for is how
 * a surface ends up lying about what it is. */
static int surface_extent_rank(const char *e) {
    if (!e || !*e) return -1;
    if (!strcmp(e, "chip"))      return 0;
    if (!strcmp(e, "card"))      return 1;
    if (!strcmp(e, "section"))   return 2;
    if (!strcmp(e, "page"))      return 3;
    if (!strcmp(e, "workspace")) return 4;
    return -1;
}

/* How many records the extent has room for. A chip is one line; a card is a
 * tile; a section is a block you skim. Page and workspace are the whole set,
 * which is what 0 means here. The budget is not a page size -- the count of
 * what was withheld is reported, so a reference never reads as the whole. */
static int surface_extent_rows(int rank) {
    switch (rank) {
    case 0:  return 1;
    case 1:  return 3;
    case 2:  return 8;
    default: return 0;
    }
}

/* One JSON head for both projections. The organ-backed branch and the
 * CMS-backed branch each wrote their own, which is how effect_class came to be
 * reported at every extent in one of them and not the other. */
static void surface_json_head(const char *pattern, const char *title, const char *caption,
                              const char *effect, const char *reads,
                              const char *authored_extent, const char *served_extent, int acts) {
    char et[320], ec[1024];
    json_escape_into(title, et, sizeof(et));
    json_escape_into(caption, ec, sizeof(ec));
    printf("{\"pattern\":\"%s\",\"title\":\"%s\",\"caption\":\"%s\","
           "\"effect_class\":\"%s\",\"reads\":\"%s\","
           "\"extent\":\"%s\",\"extent_authored\":\"%s\",\"carries_action_layer\":%s,",
           pattern, et, ec, acts ? effect : "", reads,
           served_extent, authored_extent, acts ? "true" : "false");
    if (!acts)
        printf("\"action_layer_withheld_because\":\"%s extent is a reference to a surface, "
               "not room to act inside one\",", served_extent);
    printf("\"records\":[");
}

/* What a narrowed surface withheld, said in the answer rather than left to be
 * inferred from a short list. */
static void surface_json_tail(int rows, int withheld) {
    printf("],\"count\":%d", rows);
    if (withheld)
        printf(",\"withheld_by_extent\":%d,\"whole_at\":\"the same address without ?extent\"",
               withheld);
    printf("}\n");
}

static void surface_withheld_html(int withheld, const char *authored) {
    printf("<p class=\"surface-withheld\">%d more at %s extent.</p>", withheld, authored);
}

static int serve_surface_pattern(const char *target, const char *query, const char *accept,
                                 const char *bearer, const char *auth_db) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char pattern[64], reads[64], record_ns[64], selector[64], order[96], fields_raw[1024];
    char shape[32], effect[16], requires[64], visible[128], statusf[16];
    char time_field[64], time_mode[16];
    char names[BF_SURFACE_MAX_FIELDS][64], vis[4][64];
    /* The slice: which served columns this request narrowed on, the value
     * bound for each, and whether it was asked for as a substring. */
    char slice_field[8][64], slice_bound[8][160];
    int  slice_like[8];
    int  nslice = 0;
    char sel_value[128] = {0};
    int  sel_given = 0;
    /* One word, asked of every text column this pattern serves. */
    char q_bound[192] = {0};
    int  q_given = 0;
    int  asked_limit = 0;
    int nvis = 0, i;
    char sql[4096];
    const char *name;
    char organ[64] = {0}, organ_cmd[64] = {0}, ptitle[160] = {0}, caption[512] = {0};
    char authored_extent[16] = {0}, asked_extent[16] = {0};
    /* A per-record affordance the pattern authored, so a reader can act on a
     * row without the page growing a renderer to build the link. */
    char act_href[512] = {0}, act_label[96] = {0};
    int authored_rank = 3, served_rank = 3, row_budget = 0, withheld = 0, acts = 1;
    int nfields = 0, rows = 0, as_json;

    if (strncmp(target, "/_surface/", 10)) return 0;
    name = target + 10;
    /* The catalogue. A machine that must guess pattern names has not been told
     * what this institution serves, so /_surface/ lists every published
     * descriptor with its contract: what it reads, its effect class, the
     * authority it requires and whether it filters on publication. Listing
     * that a pattern exists and requires admission is a capability
     * declaration, not a disclosure of what it would return. */
    if (!*name) return serve_surface_catalogue(accept);
    if (!studio_safe_ident(name)) return 0;
    /* The intake fragment is an effectful form, not a read pattern; it keeps
     * its own handler. */
    if (!strcmp(name, "intake")) return 0;

    as_json = wants_json(accept);
    db = cms_open_ro();
    if (!db) return 0;
    if (!cms_type_registered(db, "surface_pattern")) { sqlite3_close(db); return 0; }

    if (sqlite3_prepare_v2(db,
            "SELECT reads_type, COALESCE(selector_field,''), COALESCE(order_by,'id'), "
            "       item_fields, shape, effect_class, COALESCE(requires_authority,''), "
            "       COALESCE(visible_when,''), COALESCE(status_filter,'published'), "
            "       COALESCE(time_field,''), COALESCE(time_mode,'any'), "
            "       COALESCE(reads_organ,''), COALESCE(organ_command,''), "
            "       COALESCE(title,''), COALESCE(caption,''), COALESCE(extent,'page'), "
            "       COALESCE(record_action_href,''), COALESCE(record_action_label,''), "
            "       COALESCE(record_namespace,'') "
            "FROM surface_pattern WHERE pattern = ? AND status = 'published' LIMIT 2", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    sqlite3_bind_text(st, 1, name, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) {
        /* No descriptor: not this handler's route. A bespoke handler may still
         * claim it, which is how the door fragment kept working while the
         * descriptor was being authored. */
        sqlite3_finalize(st);
        sqlite3_close(db);
        return 0;
    }
    snprintf(pattern,    sizeof(pattern),    "%s", name);
    snprintf(reads,      sizeof(reads),      "%s", (const char *)sqlite3_column_text(st, 0));
    snprintf(selector,   sizeof(selector),   "%s", (const char *)sqlite3_column_text(st, 1));
    snprintf(order,      sizeof(order),      "%s", (const char *)sqlite3_column_text(st, 2));
    snprintf(fields_raw, sizeof(fields_raw), "%s", (const char *)sqlite3_column_text(st, 3));
    snprintf(shape,      sizeof(shape),      "%s", (const char *)sqlite3_column_text(st, 4));
    snprintf(effect,     sizeof(effect),     "%s", (const char *)sqlite3_column_text(st, 5));
    snprintf(requires,   sizeof(requires),   "%s", (const char *)sqlite3_column_text(st, 6));
    snprintf(visible,    sizeof(visible),    "%s", (const char *)sqlite3_column_text(st, 7));
    snprintf(statusf,    sizeof(statusf),    "%s", (const char *)sqlite3_column_text(st, 8));
    snprintf(time_field, sizeof(time_field), "%s", (const char *)sqlite3_column_text(st, 9));
    snprintf(time_mode,  sizeof(time_mode),  "%s", (const char *)sqlite3_column_text(st, 10));
    snprintf(organ,      sizeof(organ),      "%s", (const char *)sqlite3_column_text(st, 11));
    snprintf(organ_cmd,  sizeof(organ_cmd),  "%s", (const char *)sqlite3_column_text(st, 12));
    snprintf(ptitle,     sizeof(ptitle),     "%s", (const char *)sqlite3_column_text(st, 13));
    snprintf(caption,    sizeof(caption),    "%s", (const char *)sqlite3_column_text(st, 14));
    snprintf(authored_extent, sizeof(authored_extent), "%s", (const char *)sqlite3_column_text(st, 15));
    snprintf(act_href,  sizeof(act_href),  "%s", (const char *)sqlite3_column_text(st, 16));
    snprintf(act_label, sizeof(act_label), "%s", (const char *)sqlite3_column_text(st, 17));
    snprintf(record_ns, sizeof(record_ns), "%s", (const char *)sqlite3_column_text(st, 18));
    if (sqlite3_step(st) == SQLITE_ROW) {
        sqlite3_finalize(st);
        sqlite3_close(db);
        printf("HTTP/1.1 500 Internal Server Error\r\nContent-Type: %s\r\n"
               "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
               as_json ? "application/json" : "text/html; charset=utf-8");
        if (as_json)
            printf("{\"error\":\"ambiguous_pattern_descriptor\",\"pattern\":\"%s\"}\n", pattern);
        else
            printf("<p class=\"surface-denied\">This surface has more than one published descriptor and is refused until its ownership is reconciled.</p>");
        return 1;
    }
    sqlite3_finalize(st);
    st = NULL;

    /* The density the caller asked for, held against the density the pattern
     * was authored at. */
    authored_rank = surface_extent_rank(authored_extent);
    if (authored_rank < 0) { snprintf(authored_extent, sizeof(authored_extent), "page"); authored_rank = 3; }
    served_rank = authored_rank;
    if (query_value(query, "extent", asked_extent, sizeof(asked_extent)) && *asked_extent) {
        int r = surface_extent_rank(asked_extent);
        if (r < 0) {
            sqlite3_close(db);
            printf("HTTP/1.1 400 Bad Request\r\nContent-Type: %s\r\n"
                   "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
                   as_json ? "application/json" : "text/html; charset=utf-8");
            if (as_json)
                printf("{\"error\":\"unknown_extent\",\"asked\":\"%s\","
                       "\"extents\":[\"chip\",\"card\",\"section\",\"page\",\"workspace\"]}\n",
                       studio_safe_ident(asked_extent) ? asked_extent : "");
            else
                printf("<p class=\"surface-denied\">Extent must be one of chip, card, section, "
                       "page or workspace.</p>");
            return 1;
        }
        if (r > authored_rank) {
            /* Asking for more than was authored is not a density preference,
             * it is a request for an action layer this pattern was never
             * given. Refuse, and say what it was authored at. */
            sqlite3_close(db);
            printf("HTTP/1.1 409 Conflict\r\nContent-Type: %s\r\n"
                   "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
                   as_json ? "application/json" : "text/html; charset=utf-8");
            if (as_json)
                printf("{\"error\":\"extent_exceeds_authored\",\"pattern\":\"%s\","
                       "\"authored\":\"%s\",\"asked\":\"%s\","
                       "\"reason\":\"extent narrows a surface; it cannot grant an action layer "
                       "the pattern was not authored to carry\"}\n",
                       pattern, authored_extent, asked_extent);
            else
                printf("<p class=\"surface-denied\">This pattern is authored at %s extent. "
                       "Extent narrows a surface; it cannot grant it an action layer.</p>",
                       authored_extent);
            return 1;
        }
        served_rank = r;
    }
    row_budget = surface_extent_rows(served_rank);
    acts = served_rank >= 3;

    /* A descriptor that fails validation is a broken descriptor, not a reason
     * to render something approximate. Say so and render nothing. */
    nfields = surface_fields(fields_raw, names, BF_SURFACE_MAX_FIELDS);

    /* Read the slice before anything is built.
     *
     * A parameter that names a column this pattern serves narrows it. One that
     * does not is refused with the list that would have worked: a filter which
     * silently matches everything is how a caller comes to believe a narrowing
     * happened when it did not. The reserved names are the ones the surface
     * itself already means something by. */
    if (query && *query) {
        /* Three names are controls of the surface compiler itself. The other
         * address coordinates -- world, audience, repr and template -- are
         * context, not controls: a pattern is allowed to carry or select on
         * them. The previous list reserved all seven before the authored
         * selector was consulted, so a descriptor with selector_field=template
         * silently ignored ?template=ghost-wire and returned the whole set.
         * That is especially dangerous because the response still looks like a
         * successful narrowing. */
        /* `q` is a control like extent and limit: it is the surface's own
         * word rather than a column, and a pattern that happens to serve a
         * column called q would still be searched rather than sliced on it.
         *
         * A slice narrows by a field the caller already knows the name of. A
         * search is for a caller who does not, and without one every surface
         * that wanted to be searchable had to grow an endpoint beside itself. */
        static const char *controls[] = {"extent", "facets", "limit", "q", NULL};
        static const char *context[] = {"world", "audience", "repr", "template", NULL};
        char qcopy[1024], *part;
        snprintf(qcopy, sizeof(qcopy), "%s", query);
        for (part = strtok(qcopy, "&"); part; part = strtok(NULL, "&")) {
            char *eq = strchr(part, '=');
            const char *val;
            int i, known = 0, control = 0, contextual = 0;
            if (!eq) continue;
            *eq++ = '\0';
            val = eq;
            for (i = 0; controls[i]; i++) if (!strcmp(part, controls[i])) control = 1;
            if (control) {
                if (!strcmp(part, "q") && *val) {
                    snprintf(q_bound, sizeof(q_bound), "%%%s%%", val);
                    q_given = 1;
                }
                /* limit was reserved and then ignored, so a caller asking for
                 * three rows received sixty-five and nothing said so -- the
                 * same silent non-narrowing the unknown-slice refusal exists
                 * to prevent, sitting inside the list of names that skip it. */
                else if (!strcmp(part, "limit") && *val) {
                    int n = atoi(val);
                    if (n > 0) asked_limit = n;
                }
                continue;
            }
            for (i = 0; context[i]; i++) if (!strcmp(part, context[i])) contextual = 1;
            for (i = 0; i < nfields; i++) if (!strcmp(part, names[i])) known = 1;
            /* A selector may deliberately be a hidden key which is not part of
             * the projected item_fields. It still narrows the read; it simply
             * is not rendered back to the caller. */
            if (*selector && !strcmp(part, selector)) known = 1;
            if (!known) {
                /* Address context irrelevant to this pattern passes through
                 * without becoming an accidental filter. If the pattern does
                 * carry the coordinate, `known` above makes it a real slice. */
                if (contextual) continue;
                sqlite3_close(db);
                if (as_json) {
                    printf("HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\n"
                           "Cache-Control: no-store\r\nConnection: close\r\n\r\n"
                           "{\"error\":\"unknown_slice\",\"pattern\":\"%s\",\"asked\":\"%s\","
                           "\"sliceable\":[", pattern, studio_safe_ident(part) ? part : "");
                    for (i = 0; i < nfields; i++)
                        printf("%s\"%s\"", i ? "," : "", names[i]);
                    printf("],\"cannot_infer\":\"a sliceable column -> a column worth "
                           "slicing on; this says what this pattern serves, never that "
                           "any value of it exists\"}\n");
                } else {
                    printf("HTTP/1.1 400 Bad Request\r\nContent-Type: text/html; charset=utf-8\r\n"
                           "Cache-Control: no-store\r\nConnection: close\r\n\r\n"
                           "<p class=\"surface-denied\">This surface does not carry that "
                           "field, so it cannot narrow on it.</p>\n");
                }
                return 1;
            }
            if (nslice >= 8) continue;
            /* A leading ~ asks for a substring. Columns that hold a list in one
             * cell -- grammars, sources, label -- can only be narrowed that way,
             * and exact match on them is whole-list equality. */
            if (*val == '~') {
                slice_like[nslice] = 1;
                snprintf(slice_bound[nslice], sizeof(slice_bound[nslice]), "%%%s%%", val + 1);
            } else {
                slice_like[nslice] = 0;
                snprintf(slice_bound[nslice], sizeof(slice_bound[nslice]), "%s", val);
            }
            snprintf(slice_field[nslice], sizeof(slice_field[nslice]), "%s", part);
            if (*selector && !strcmp(part, selector)) {
                /* The authored selector is one of these; keep it where it was. */
                sel_given = 1;
                snprintf(sel_value, sizeof(sel_value), "%s", slice_bound[nslice]);
                if (slice_like[nslice]) sel_given = 0;   /* a substring is a slice, not the selector */
                else continue;
            }
            nslice++;
        }
    }
    if (*visible) nvis = surface_fields(visible, vis, 4);
    if ((organ[0] ? (!surface_safe_binary(organ) || !studio_safe_ident(organ_cmd))
                  : !studio_safe_ident(reads)) ||
        (!organ[0] && !surface_safe_order(order)) || nfields <= 0 ||
        (*visible && nvis <= 0) ||
        (*record_ns && !studio_safe_ident(record_ns)) ||
        (strcmp(statusf, "published") && strcmp(statusf, "any")) ||
        (*time_field && !studio_safe_ident(time_field)) ||
        (strcmp(time_mode, "any") && strcmp(time_mode, "upcoming") && strcmp(time_mode, "past")) ||
        (*selector && (!studio_safe_ident(selector)
                    || !strcmp(selector, "extent")
                    || !strcmp(selector, "facets")
                    || !strcmp(selector, "limit")))) {
        sqlite3_close(db);
        fprintf(stderr, "[surface] pattern '%s' has an unsafe descriptor; refusing to render\n", pattern);
        printf("HTTP/1.1 500 Internal Server Error\r\nContent-Type: %s\r\n"
               "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
               as_json ? "application/json" : "text/html; charset=utf-8");
        if (as_json) printf("{\"error\":\"unsafe_pattern_descriptor\",\"pattern\":\"%s\"}\n", pattern);
        else printf("<!-- pattern descriptor refused -->");
        return 1;
    }

    /* Authority is the descriptor's to declare and the session's to satisfy.
     * A pattern with no requirement is public; one that declares a scope is
     * refused to anyone who has not been admitted, in both projections. */
    if (*requires && !surface_authority_satisfied(requires, bearer, auth_db)) {
        sqlite3_close(db);
        printf("HTTP/1.1 403 Forbidden\r\nContent-Type: %s\r\n"
               "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
               as_json ? "application/json" : "text/html; charset=utf-8");
        if (as_json)
            printf("{\"error\":\"surface_authority_required\",\"pattern\":\"%s\",\"requires\":\"%s\"}\n",
                   pattern, requires);
        else
            printf("<p class=\"surface-denied\">This section needs an admitted session.</p>");
        return 1;
    }

    /* A descriptor may read a native organ instead of a CMS type.
     *
     * Without this the compiler could project only what the CMS stored, so
     * every organ-backed panel had to be hand-written: /habitat carried 2.7KB
     * of bespoke fetch-and-render JavaScript doing by hand what this function
     * does from a descriptor -- and doing it only for browsers, so the same
     * panels had no machine projection at all.
     *
     * The organ is named, never a path: surface_safe_binary admits a bare
     * `bonfyre-*` identifier and the binary is resolved inside the estate's own
     * bin directory. A CMS write therefore cannot choose what executes, only
     * which of this estate's organs is asked.
     *
     * Columns are positional. The organ's registry rows are fixed-width text,
     * and item_fields names the columns in order -- the descriptor says what
     * the columns mean, the organ says what they are. */
    if (organ[0]) {
        static char out[262144];
        const char *bindir = getenv("BONFYRE_BIN_DIR");
        char bin[PATH_MAX];
        char *line, *save = NULL;
        sqlite3_close(db);
        db = NULL;
        snprintf(bin, sizeof(bin), "%s/%s", (bindir && *bindir) ? bindir : "/usr/local/bin", organ);
        {
            char *argv[] = { bin, organ_cmd, NULL };
            if (auth_call(argv, NULL, out, sizeof(out)) != 0) {
                /* An organ that will not answer is a real gap. Saying so beats
                 * rendering an empty panel that reads as "nothing here". */
                printf("HTTP/1.1 503 Service Unavailable\r\nContent-Type: %s\r\n"
                       "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
                       as_json ? "application/json" : "text/html; charset=utf-8");
                if (as_json)
                    printf("{\"error\":\"organ_unavailable\",\"pattern\":\"%s\",\"organ\":\"%s\"}\n",
                           pattern, organ);
                else
                    printf("<p class=\"surface-denied\">%s is not answering; this panel is a real "
                           "gap, not an empty list.</p>", organ);
                return 1;
            }
        }
        printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\n"
               "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
               as_json ? "application/json" : "text/html; charset=utf-8");
    /* A panel labels itself. A page that composes three lenses would otherwise
     * have to carry their headings, which puts the name of a thing somewhere
     * other than the thing -- and every page including it repeats the label,
     * free to disagree. The descriptor says what the panel is called; each
     * projection renders that in its own grammar. */
        if (as_json)
            surface_json_head(pattern, ptitle, caption, effect, organ,
                              authored_extent, *asked_extent ? asked_extent : authored_extent, acts);
        else surface_heading(ptitle, caption);
        for (line = strtok_r(out, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
            char *col[BF_SURFACE_MAX_FIELDS]; int ncol, i, header;
            if (!*line || line[0] == '-') continue;
            ncol = split_cols(line, col, nfields);
            if (ncol < 1) continue;
            /* A registry table labels its columns. Which row is the header is
             * not the organ's convention to guess at -- the descriptor already
             * named these columns, so a row whose fields ARE those names is the
             * header. The older handler tested for the literal "ID ", which was
             * true of one organ's output and of nothing else. */
            header = 1;
            for (i = 0; i < ncol && header; i++)
                if (strcasecmp(col[i], names[i])) header = 0;
            if (header && ncol > 1) continue;
            /* Past the extent's budget the rows are counted, not rendered. A
             * reference that quietly stopped at three would read as a
             * three-record set. */
            if (row_budget && rows >= row_budget) { withheld++; continue; }
            if (as_json) {
                printf("%s{", rows ? "," : "");
                for (i = 0; i < nfields; i++) {
                    char esc[4096];
                    json_escape_into(i < ncol ? col[i] : "", esc, sizeof(esc));
                    printf("%s\"%s\":\"%s\"", i ? "," : "", names[i], esc);
                }
                printf("}");
            } else if (!strcmp(shape, "link") && ncol > 1) {
                printf("<a class=\"way\" href=\"");
                html(col[1]); printf("\">"); html(col[0]); printf("</a>");
            } else {
                printf("<dl class=\"surface-record\">");
                for (i = 0; i < nfields; i++) {
                    printf("<dt>"); html(names[i]);
                    printf("</dt><dd>"); html(i < ncol ? col[i] : ""); printf("</dd>");
                }
                printf("</dl>");
            }
            rows++;
        }
        if (as_json) surface_json_tail(rows, withheld);
        else if (!rows) printf("<!-- the organ answered with no rows -->");
        else if (withheld) surface_withheld_html(withheld, authored_extent);
        fflush(stdout);
        return 1;
    }

    /* Extent decides the ceiling; a limit may only come in under it, and it is
     * applied here because the query has to be read before its value exists.
     * Clamping where the extent was resolved meant asked_limit was always zero
     * and every limit was silently ignored -- the same non-narrowing the
     * unknown-slice refusal exists to prevent. */
    if (asked_limit > 0 && (!row_budget || asked_limit < row_budget))
        row_budget = asked_limit;

    if (!cms_type_registered(db, reads)) {
        sqlite3_close(db);
        printf("HTTP/1.1 204 No Content\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n");
        return 1;
    }

    {
        int i;
        BfBoundedOutput sql_out;
        bf_output_init(&sql_out, sql, sizeof(sql));
        bf_output_text(&sql_out, "SELECT ");
        for (i = 0; i < nfields; i++) {
            if (i) bf_output_text(&sql_out, ", ");
            bf_output_text(&sql_out, names[i]);
        }
        int where = 0;
        bf_output_text(&sql_out, " FROM ");
        bf_output_text(&sql_out, reads);
        /* Publication is the default, not an option a descriptor opts into.
         * The doors served publicly while every row was status='draft',
         * because the compiler had no notion of publication at all. A pattern
         * may declare status_filter='any' for a type where status is not
         * meaningful; it must declare it, and it is visible in the catalogue
         * when it does. */
        if (!strcmp(statusf, "published")) {
            bf_output_text(&sql_out, where++ ? " AND" : " WHERE");
            bf_output_text(&sql_out, " status = 'published'");
        }
        /* A SurfacePattern may project one named CMS namespace.  That is
         * ownership of the read set, not an audience selector: it prevents a
         * reference census and an earlier seed set from silently becoming one
         * duplicate truth set merely because they share a template key. */
        if (*record_ns) {
            bf_output_text(&sql_out, where++ ? " AND" : " WHERE");
            bf_output_text(&sql_out, " namespace = ?");
        }
        /* Row-level visibility flags the type itself carries -- wiki_page's
         * published and allow_guest. A surface that ignored these would have
         * served thirty-seven pages of which one was publishable. */
        for (i = 0; i < nvis; i++) {
            bf_output_text(&sql_out, where++ ? " AND" : " WHERE");
            bf_output_text(&sql_out, " COALESCE(");
            bf_output_text(&sql_out, vis[i]);
            bf_output_text(&sql_out, ",0) != 0");
        }
        /* Time, when a pattern says its content has any.
         *
         * An events surface that lists last month's sets alongside tonight's is
         * not a schedule, it is a table. The comparison is lexical on ISO-8601,
         * which sorts correctly by construction, so no date parsing is needed
         * and a malformed value simply fails to match rather than being
         * silently treated as now. */
        if (*time_field && strcmp(time_mode, "any")) {
            bf_output_text(&sql_out, where++ ? " AND" : " WHERE");
            bf_output_text(&sql_out, " ");
            bf_output_text(&sql_out, time_field);
            bf_output_text(&sql_out, " != '' AND ");
            bf_output_text(&sql_out, time_field);
            bf_output_text(&sql_out, !strcmp(time_mode, "upcoming") ? " >= ?" : " < ?");
        }
        if (*selector && sel_given) {
            bf_output_text(&sql_out, where++ ? " AND" : " WHERE");
            bf_output_text(&sql_out, " ");
            bf_output_text(&sql_out, selector);
            bf_output_text(&sql_out, " = ?");
        }
        /* Compounding slice: every served column is filterable by its own
         * name, and the terms AND together. A term whose value begins with ~
         * matches a substring, which is what a multi-valued column like
         * grammars or label needs -- those hold a list in one cell, and exact
         * match on such a column can only ever return whole-list equality. */
        for (i = 0; i < nslice; i++) {
            bf_output_text(&sql_out, where++ ? " AND" : " WHERE");
            bf_output_text(&sql_out, " ");
            bf_output_text(&sql_out, slice_field[i]);
            bf_output_text(&sql_out, slice_like[i] ? " LIKE ?" : " = ?");
        }
        if (q_given && nfields) {
            /* Grouped, so a search ANDs with the slices rather than widening
             * past them. Narrowing to one estate and then searching must not
             * hand back the whole table. */
            bf_output_text(&sql_out, where++ ? " AND" : " WHERE");
            bf_output_text(&sql_out, " (");
            for (i = 0; i < nfields; i++) {
                if (i) bf_output_text(&sql_out, " OR ");
                bf_output_text(&sql_out, names[i]);
                bf_output_text(&sql_out, " LIKE ?");
            }
            bf_output_text(&sql_out, ")");
        }
        bf_output_text(&sql_out, " ORDER BY ");
        bf_output_text(&sql_out, order);
        if (sql_out.failed) {
            sqlite3_close(db);
            printf("HTTP/1.1 503 Service Unavailable\r\nContent-Type: %s\r\n"
                   "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
                   as_json ? "application/json" : "text/html; charset=utf-8");
            puts(as_json ? "{\"error\":\"surface_query_too_large\"}"
                         : "<p class=\"surface-denied\">The surface descriptor exceeds the query budget.</p>");
            return 1;
        }
    }

    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "[surface] pattern '%s': %s\n", pattern, sqlite3_errmsg(db));
        sqlite3_close(db);
        printf("HTTP/1.1 204 No Content\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n");
        return 1;
    }
    {
        /* Bind order follows clause order. Namespace is a descriptor-owned
         * read boundary, never a caller-controlled address coordinate. */
        int bindn = 1;
        if (*record_ns)
            sqlite3_bind_text(st, bindn++, record_ns, -1, SQLITE_TRANSIENT);
        if (*time_field && strcmp(time_mode, "any")) {
            char nowiso[32];
            iso_utc_now(nowiso, sizeof(nowiso));
            sqlite3_bind_text(st, bindn++, nowiso, -1, SQLITE_TRANSIENT);
        }
        if (*selector && sel_given)
            sqlite3_bind_text(st, bindn++, sel_value, -1, SQLITE_TRANSIENT);
        for (i = 0; i < nslice; i++)
            sqlite3_bind_text(st, bindn++, slice_bound[i], -1, SQLITE_TRANSIENT);
        if (q_given)
            for (i = 0; i < nfields; i++)
                sqlite3_bind_text(st, bindn++, q_bound, -1, SQLITE_TRANSIENT);
    }

    printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
           as_json ? "application/json" : "text/html; charset=utf-8");

    if (as_json)
        surface_json_head(pattern, ptitle, caption, effect, reads,
                          authored_extent, *asked_extent ? asked_extent : authored_extent, acts);
    else surface_heading(ptitle, caption);

    while (sqlite3_step(st) == SQLITE_ROW) {
        int i;
        if (row_budget && rows >= row_budget) { withheld++; continue; }
        if (as_json) {
            printf("%s{", rows ? "," : "");
            for (i = 0; i < nfields; i++) {
                const char *v = (const char *)sqlite3_column_text(st, i);
                char esc[4096];
                json_escape_into(v ? v : "", esc, sizeof(esc));
                printf("%s\"%s\":\"%s\"", i ? "," : "", names[i], esc);
            }
            printf("}");
        } else if (!strcmp(shape, "door")) {
            /* The door shape: eyebrow, heading, body, ways. Render primitives
             * live here rather than in the descriptor, so a CMS write can
             * never introduce markup. */
            const char *eyebrow = (const char *)sqlite3_column_text(st, 0);
            const char *heading = nfields > 1 ? (const char *)sqlite3_column_text(st, 1) : NULL;
            const char *body    = nfields > 2 ? (const char *)sqlite3_column_text(st, 2) : NULL;
            const char *ways    = nfields > 3 ? (const char *)sqlite3_column_text(st, 3) : NULL;
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
        } else if (!strcmp(shape, "link")) {
            const char *label = (const char *)sqlite3_column_text(st, 0);
            const char *href  = nfields > 1 ? (const char *)sqlite3_column_text(st, 1) : NULL;
            if (!label || !href) continue;
            printf("<a class=\"way\" href=\"");
            html(href);
            printf("\">");
            html(label);
            printf("</a>");
        } else {
            /* An unknown shape renders as a definition list rather than
             * nothing: the records are real and the reader should see them
             * even when the presentation has not been given a name yet. */
            printf("<dl class=\"surface-record\">");
            for (i = 0; i < nfields; i++) {
                const char *v = (const char *)sqlite3_column_text(st, i);
                printf("<dt>");
                html(names[i]);
                printf("</dt><dd>");
                html(v ? v : "");
                printf("</dd>");
            }
            /* The authored affordance, rendered server-side.
             *
             * A row a reader can see and cannot act on sends the page back to
             * building its own buttons in script, which is where a second
             * renderer -- and a second idea of what a record is -- comes from.
             *
             * Only fields this pattern serves may appear in the template. A
             * placeholder naming anything else drops the whole affordance
             * rather than emitting a link with a hole in it, checked before
             * anything is written so a bad template cannot half-print. */
            if (act_href[0] && act_label[0]) {
                const char *p;
                int ok = 1;
                for (p = act_href; *p && ok; p++) {
                    const char *end;
                    char field[64];
                    size_t flen;
                    if (*p != '{') continue;
                    end = strchr(p, '}');
                    flen = end ? (size_t)(end - p - 1) : 0;
                    if (!end || !flen || flen >= sizeof(field)) { ok = 0; break; }
                    memcpy(field, p + 1, flen); field[flen] = '\0';
                    ok = 0;
                    for (i = 0; i < nfields; i++)
                        if (!strcmp(field, names[i])) { ok = 1; break; }
                    p = end;
                }
                if (ok) {
                    printf("<a class=\"surface-act\" href=\"");
                    for (p = act_href; *p; p++) {
                        if (*p == '{') {
                            const char *end = strchr(p, '}');
                            char field[64];
                            size_t flen = (size_t)(end - p - 1);
                            memcpy(field, p + 1, flen); field[flen] = '\0';
                            for (i = 0; i < nfields; i++)
                                if (!strcmp(field, names[i])) {
                                    const char *v = (const char *)sqlite3_column_text(st, i);
                                    url_query_value(v ? v : "");
                                    break;
                                }
                            p = end;
                        } else if (*p == '&') fputs("&amp;", stdout);
                        else putchar(*p);
                    }
                    printf("\">");
                    html(act_label);
                    printf("</a>");
                }
            }
            printf("</dl>");
        }
        rows++;
    }

    if (as_json) surface_json_tail(rows, withheld);
    else if (!rows) printf("<!-- no authored records for this pattern -->");
    else if (withheld) surface_withheld_html(withheld, authored_extent);

    sqlite3_finalize(st);
    sqlite3_close(db);
    fflush(stdout);
    return 1;
}

/* Landing-page doors, authored rather than compiled.
 *
 * The six doors on /protocols were 2KB of C string literals, so changing one
 * sentence meant editing an organ, rebuilding it and deploying it. They are
 * content. This serves them as an HTMX fragment over the same /_surface/
 * mechanism the intake form already uses, and the page keeps the compiled doors
 * inside the swap target so a reader with no JavaScript, or an unreachable
 * store, still sees a complete page rather than an empty one.
 *
 * Cannot infer: an authored door -> a served capability. A door describes a way
 * in; whether the thing behind it answers is the carrier's own business. */
#endif /* BONFYRE_SURFACE_CATALOGUE_ROUTES_H */
