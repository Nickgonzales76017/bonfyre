#ifndef BONFYRE_SURFACE_WORLD_AND_COMMUNITY_H
#define BONFYRE_SURFACE_WORLD_AND_COMMUNITY_H
/* Extracted verbatim from main.c lines 4028-5110 (mechanical split, no logic changes). */
static int serve_presentation_class(const char *method, const char *target, const char *query,
                                    const char *bearer, const char *auth_db) {
    char grammar[32] = "discover";
    const char *klass;
    if (strcmp(target, "/_surface/presentation-class")) return 0;
    if (strcmp(method, "GET")) { protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n"); return 1; }
    if (presentation_has_forbidden_coordinate(query)) {
        protocol_reply(400, "application/json", "{\"error\":\"forbidden_presentation_coordinate\",\"limit\":\"presentation class is resolved from native closure; actor, audience, institution, role and authority are not URL coordinates\"}\n");
        return 1;
    }
    (void)query_value(query, "grammar", grammar, sizeof(grammar));
    if (!presentation_grammar_allowed(grammar)) {
        protocol_reply(400, "application/json", "{\"error\":\"unknown_presentation_grammar\"}\n"); return 1;
    }
    klass = native_presentation_class(bearer, auth_db);
    {
        char body[1024];
        snprintf(body, sizeof(body),
            "{\"grammar\":\"%s\",\"presentation_class\":\"%s\","
            "\"closure_source\":\"native_session_and_current_grant_closure\","
            "\"forbidden_inference\":\"presentation class is not actor identity, audience entitlement, provider admission, payment authority, spend authority or effect authority\"}\n",
            grammar, klass);
        protocol_reply(200, "application/json", body);
    }
    return 1;
}

/* The declared candidate lowerer is shared by full human compositions and
 * fragment refreshes. It takes only shape coordinates; identity, audience and
 * authority stay outside this argument list and are resolved by native
 * closure. Return values distinguish malformed shape from an unavailable
 * realization, so neither caller has to invent a second interpretation. */
static int lower_external_candidate_fragment(const char *query, const char *bearer,
                                             const char *auth_db, char *out, size_t out_sz,
                                             int as_json) {
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    const char *dbpath = getenv("BONFYRE_EXTERNAL_ESTATE_DB");
    char bin[PATH_MAX], grammar[32] = "discover", q[256] = {0}, domain[96] = {0};
    char world[128] = {0}, affordance[64] = {0}, chain[96] = {0}, limit[16] = "8";
    const char *klass;
    char *argv[20];
    int ai = 0;
    if (presentation_has_forbidden_coordinate(query)) return -1;
    (void)query_value(query, "grammar", grammar, sizeof(grammar));
    if (!presentation_grammar_allowed(grammar)) return -1;
    (void)query_value(query, "q", q, sizeof(q));
    (void)query_value(query, "domain", domain, sizeof(domain));
    (void)query_value(query, "world", world, sizeof(world));
    (void)query_value(query, "affordance", affordance, sizeof(affordance));
    (void)query_value(query, "chain", chain, sizeof(chain));
    (void)query_value(query, "limit", limit, sizeof(limit));
    if (!safe(q, sizeof(q)) && q[0]) return -1;
    if ((!safe(domain,sizeof(domain))&&domain[0]) || (!safe(world,sizeof(world))&&world[0]) || (!safe(affordance,sizeof(affordance))&&affordance[0]) || (!safe(chain,sizeof(chain))&&chain[0])) return -1;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    if (!dbpath || !*dbpath) dbpath = "/var/lib/bonfyre/external-estate.db";
    if (snprintf(bin, sizeof(bin), "%s/bonfyre-external-estate", bindir) >= (int)sizeof(bin) || access(bin, X_OK) != 0) return -2;
    klass = native_presentation_class(bearer, auth_db);
    argv[ai++]=bin; argv[ai++]=(char*)"fragment"; argv[ai++]=(char*)"--db"; argv[ai++]=(char*)dbpath;
    argv[ai++]=(char*)"--grammar"; argv[ai++]=grammar; argv[ai++]=(char*)"--class"; argv[ai++]=(char*)klass;
    if (q[0]) { argv[ai++]=(char*)"--q"; argv[ai++]=q; }
    if (domain[0]) { argv[ai++]=(char*)"--domain"; argv[ai++]=domain; }
    if (world[0]) { argv[ai++]=(char*)"--world"; argv[ai++]=world; }
    if (affordance[0]) { argv[ai++]=(char*)"--affordance"; argv[ai++]=affordance; }
    if (chain[0]) { argv[ai++]=(char*)"--chain"; argv[ai++]=chain; }
    argv[ai++]=(char*)"--limit"; argv[ai++]=limit;
    /* Same lowerer, same coordinates; only the projection differs. */
    if (as_json) { argv[ai++]=(char*)"--as"; argv[ai++]=(char*)"json"; }
    argv[ai]=NULL;
    if (auth_call(argv, NULL, out, out_sz) != 0 || !out[0]) return -2;
    return 1;
}

/* Native external-candidate fragment lowering. The browser asks only for an
 * existing SurfaceIR fragment; C resolves presentation class from the session,
 * invokes the read-only C/SQLite reservoir, and returns its HTML. No page
 * builds a card, fetches JSON, or supplies a class/authority coordinate. */
static int serve_external_candidate_fragment(const char *method, const char *target,
                                             const char *query, const char *accept,
                                             const char *bearer, const char *auth_db) {
    static char out[131072];
    char shaped[2048], requested_grammar[32] = {0};
    int public_observation = !strcmp(target, "/_surface/external-estate");
    /* The public aperture answered 200 text/html to `Accept: application/json`
     * -- served, and serving the machine the human's page. One address, one
     * corpus, two projections; the reader says which one it can read. */
    int machine = public_observation && !wants_html(accept);
    int lowered;
    if (!public_observation && strcmp(target, "/_surface/external-candidates")) return 0;
    if (strcmp(method, "GET")) { protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n"); return 1; }
    if (!public_observation && !wants_html(accept)) { protocol_reply(406, "application/json", "{\"error\":\"human_fragment_requires_html\",\"limit\":\"This is a native HTML fragment; machine clients use the owner contract.\"}\n"); return 1; }
    /* The external estate is reagent material.  It is inspectable in the
     * operator closure, but an anonymous/personal/member page must not turn
     * source descriptors into a faux feed.  A SitePresentation lowerer first
     * promotes lawful material into a typed application (PublicationFeed,
     * Watch, Weather, Marketplace, &c.) and that application owns the human
     * fragment. */
    if (!public_observation && strcmp(native_presentation_class(bearer, auth_db), "operator")) {
        protocol_reply(409, "text/html; charset=utf-8", "<p class=\"bf-refuse\">External source material is not a public application. It must be compiled into a typed, lawful surface before it can be shown here.</p>");
        return 1;
    }
    if (public_observation) {
        if (query_value(query, "grammar", requested_grammar, sizeof(requested_grammar)) &&
            strcmp(requested_grammar, "observations")) {
            protocol_reply(400, machine ? "application/json" : "text/html; charset=utf-8", machine ? "{\"error\":\"grammar_not_served_here\",\"limit\":\"This public aperture is an observations surface.\"}\n" : "<p class=\"bf-refuse\">This public aperture is an observations surface. Other grammars are compiled by their own applications.</p>");
            return 1;
        }
        if (snprintf(shaped, sizeof(shaped), "grammar=observations%s%s",
                     query && *query ? "&" : "", query && *query ? query : "") >= (int)sizeof(shaped)) {
            protocol_reply(400, machine ? "application/json" : "text/html; charset=utf-8", machine ? "{\"error\":\"shape_too_large\"}\n" : "<p class=\"bf-refuse\">The requested public search shape is too large.</p>");
            return 1;
        }
        lowered = lower_external_candidate_fragment(shaped, bearer, auth_db, out, sizeof(out), machine);
    } else {
        lowered = lower_external_candidate_fragment(query, bearer, auth_db, out, sizeof(out), 0);
    }
    if (lowered == -1) { protocol_reply(400, machine ? "application/json" : "text/html; charset=utf-8", machine ? "{\"error\":\"forbidden_coordinate\",\"limit\":\"Presentation class, authority and invalid shape coordinates are not accepted here.\"}\n" : "<p class=\"bf-refuse\">Presentation class, authority, and invalid shape coordinates are not accepted here.</p>"); return 1; }
    if (lowered < 1) { protocol_reply(503, machine ? "application/json" : "text/html; charset=utf-8", machine ? "{\"error\":\"reservoir_unavailable\"}\n" : "<p class=\"bf-refuse\">The native candidate reservoir could not lower this declared fragment.</p>"); return 1; }
    protocol_reply(200, machine ? "application/json" : "text/html; charset=utf-8", out);
    return 1;
}

/* These subjects are not a second content store: they are the published
 * public condition of the existing Night Grid World.  The two donor surfaces
 * below are server projections over that owned state.  In particular, an
 * ExperienceMap chain, provider descriptor, protocol or `source family' is
 * never admitted into this structure as a post or an occurrence. */
#define NATIVE_NIGHT_MAX 32
typedef struct {
    char name[256], kind[64], summary[1536], tonight[512], capacity[512];
    char offer[512], price[64], operations[512], safety[512];
} NativeNightSubject;

#define NATIVE_PUBLICATION_MAX 48
typedef struct {
    char title[1536], href[2048], when[256], attribution[512];
} NativePublication;

static int native_night_subjects(sqlite3 *db, NativeNightSubject *out, size_t cap) {
    sqlite3_stmt *st = NULL; int count = 0;
    if (!db || !out || !cap || !cms_type_registered(db, "night_subject")) return 0;
    if (sqlite3_prepare_v2(db,
        "SELECT name,kind,COALESCE(summary,''),COALESCE(tonight_status,''),"
        "COALESCE(capacity_note,''),COALESCE(offer_note,''),COALESCE(offer_usd,''),"
        "COALESCE(operations_note,''),COALESCE(safety_note,'') "
        "FROM night_subject WHERE status='published' ORDER BY position,id LIMIT ?1",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_int(st, 1, (int)cap);
    while (sqlite3_step(st) == SQLITE_ROW && (size_t)count < cap) {
        NativeNightSubject *s = &out[count++];
#define NIGHT_COPY(field,col) snprintf(s->field, sizeof(s->field), "%s", sqlite3_column_text(st,col) ? (const char *)sqlite3_column_text(st,col) : "")
        NIGHT_COPY(name,0); NIGHT_COPY(kind,1); NIGHT_COPY(summary,2); NIGHT_COPY(tonight,3);
        NIGHT_COPY(capacity,4); NIGHT_COPY(offer,5); NIGHT_COPY(price,6); NIGHT_COPY(operations,7); NIGHT_COPY(safety,8);
#undef NIGHT_COPY
    }
    sqlite3_finalize(st); return count;
}

/* Discover is a Pixelfed-style public stream over admitted publication
 * occurrences.  It is intentionally a material projection, not a search over
 * ExternalEstate descriptors or an execution-chain catalogue. */
/* A discover domain is a presentation cut, never a keyword search across the
 * ExternalEstate. It may release an already-admitted PublicationOccurrence
 * only when CMS has explicitly placed its source reference in a published
 * SourceCollection for that same domain. `weather' must not become whichever
 * unrelated publication happened to be newest. */
#include "native_publications_query.h"
static int native_publications(sqlite3 *db, NativePublication *out, size_t cap,
                               const char *domain) {
    sqlite3_stmt *st = NULL;
    int count = 0;
    if (!db || !out || !cap || !cms_type_registered(db, "publication_occurrence")) return 0;
    if (domain && *domain && !cms_type_registered(db, "source_collection")) return 0;
    if (sqlite3_prepare_v2(db, bf_native_publications_query,
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_int(st, 1, (int)cap);
    sqlite3_bind_text(st, 2, domain ? domain : "", -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW && (size_t)count < cap) {
        NativePublication *p = &out[count++];
#define PUB_COPY(field,col) snprintf(p->field, sizeof(p->field), "%s", sqlite3_column_text(st,col) ? (const char *)sqlite3_column_text(st,col) : "")
        PUB_COPY(title, 0); PUB_COPY(href, 1); PUB_COPY(when, 2); PUB_COPY(attribution, 3);
#undef PUB_COPY
    }
    sqlite3_finalize(st);
    return count;
}

static const char *native_night_primary(const NativeNightSubject *s, const char *territory) {
    size_t i;
    BfSurfacePrimaryField field = BF_SURFACE_PRIMARY_SUMMARY;
    for (i = 0; i < BF_SURFACE_TERRITORY_PRIMARY_COUNT; i++)
        if (!strcmp(bf_surface_territory_primary[i].territory, territory)) {
            field = bf_surface_territory_primary[i].field;
            break;
        }
    if (field == BF_SURFACE_PRIMARY_OFFER && s->offer[0]) return s->offer;
    if (field == BF_SURFACE_PRIMARY_OPERATIONS && s->operations[0]) return s->operations;
    if (field == BF_SURFACE_PRIMARY_TONIGHT && s->tonight[0]) return s->tonight;
    return s->summary;
}

static void native_night_date(const char *part) {
    time_t now = time(NULL); struct tm local;
    char buf[48] = {0};
    if (!localtime_r(&now, &local)) return;
    if (!strcmp(part,"weekday")) strftime(buf,sizeof(buf),"%a",&local);
    else if (!strcmp(part,"day")) strftime(buf,sizeof(buf),"%d",&local);
    else if (!strcmp(part,"month")) strftime(buf,sizeof(buf),"%b",&local);
    else strftime(buf,sizeof(buf),"%Y",&local);
    html(buf);
}


typedef struct {
    const char *program;
    const BfSurfaceRendererPresentation *presentation;
    const char *territory;
    const char *domain;
    const char *world;
    const char *bearer;
    const char *auth_db;
    /* The community this request named, when it named one. A page that
     * silently renders a different community than the address asked for is
     * agreeing with its record by coincidence. */
    const char *community;
    const NativeNightSubject *subjects;
    int subject_count;
    const NativePublication *publications;
    int publication_count;
    const char *target, *query;
} NativeSurfaceContext;

static int page_contains_ci(const char *haystack, const char *needle);
static void page_emit_reading_query(const char *query, const char *replace, const char *value);

static int surface_reading_matches(const NativeSurfaceContext *ctx, const char *by,
                                   const char *title, const char *body) {
    char q[256] = {0}, from[256] = {0};
    query_value(ctx->query, "q", q, sizeof(q));
    query_value(ctx->query, "from", from, sizeof(from));
    return (!*from || (by && !strcasecmp(by, from))) &&
           (!*q || page_contains_ci(title, q) || page_contains_ci(body, q) || page_contains_ci(by, q));
}

static int surface_reading_narrowed(const NativeSurfaceContext *ctx) {
    char q[256] = {0}, from[256] = {0};
    query_value(ctx->query, "q", q, sizeof(q));
    query_value(ctx->query, "from", from, sizeof(from));
    return *q || *from;
}

static void surface_emit_reading_controls(const NativeSurfaceContext *ctx) {
    static const char *keys[] = {"from", "s", "domain", "community"};
    char q[256] = {0}; size_t i;
    if (strcmp(ctx->program, "community") && strcmp(ctx->program, "discover") && strcmp(ctx->program, "stage")) return;
    if (!ctx->target || (strncmp(ctx->target, "/w/", 3) && !surface_reading_narrowed(ctx))) return;
    query_value(ctx->query, "q", q, sizeof(q));
    fputs("<form class=\"bf-reading-controls\" method=\"get\" role=\"search\" action=\"", stdout);
    html(ctx->target); fputs("\"><label>Search this view <input type=\"search\" name=\"q\" value=\"", stdout);
    html(q); fputs("\"></label>", stdout);
    for (i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) {
        char value[256] = {0};
        if (query_value(ctx->query, keys[i], value, sizeof(value)) && *value) {
            fputs("<input type=\"hidden\" name=\"", stdout); html(keys[i]);
            fputs("\" value=\"", stdout); html(value); fputs("\">", stdout);
        }
    }
    fputs("<button type=\"submit\">Search</button></form>", stdout);
}

static const char *next_native_marker(const char *from, const char *kind) {
    (void)kind;
    return strstr(from, "<!--BF_SLOT:");
}

static int native_surface_slot_number(const char *marker) {
    const char *p; int n = 0;
    if (!marker || strncmp(marker, "<!--BF_SLOT:", 12)) return -1;
    p = marker + 12;
    while (*p && isdigit((unsigned char)*p)) { n = n * 10 + (*p++ - '0'); if (n > 255) return -1; }
    return !strncmp(p, "-->", 3) ? n : -1;
}

static const BfNativeSurfaceProgram *native_surface_program(const char *name) {
    size_t i;
    for (i = 0; i < BF_NATIVE_SURFACE_PROGRAM_COUNT; i++)
        if (!strcmp(bf_native_surface_programs[i].program, name)) return &bf_native_surface_programs[i];
    return NULL;
}

static const BfNativeSurfaceProgram *native_surface_program_route(const char *route) {
    size_t i;
    for (i = 0; i < BF_NATIVE_SURFACE_PROGRAM_COUNT; i++)
        if (!strcmp(bf_native_surface_programs[i].route, route)) return &bf_native_surface_programs[i];
    return NULL;
}

static const BfSurfaceProgramSlot *native_surface_slot(const char *program, int slot) {
    const BfNativeSurfaceProgram *p = native_surface_program(program);
    if (!p || slot < 0 || (unsigned int)slot >= p->slot_count) return NULL;
    return &p->slots[slot];
}

static void surface_world_href(const char *world, const char *cut) {
    fputs("/w/", stdout); html(world);
    if (cut && *cut) { fputc('/', stdout); html(cut); }
}

/* Presentation branches here are renderer variants, not semantic decisions:
 * every subject, post, fragment and offer remains queried from its canonical
 * owner.  The authored SurfaceProgram selects the opcode; this generic
 * executor only lowers its declared interaction grammar into donor structure. */
static void surface_emit_territory_nav(const NativeSurfaceContext *ctx) {
    static const char *const territories[] = {"live", "life", "shop", "work"};
    int i;
    for (i = 0; i < 4; i++) {
        if (ctx->presentation->territory_nav == BF_SURFACE_NAV_STREAM) {
            fputs("<li class=\"nav-item\"><a class=\"nav-link", stdout);
            if (!strcmp(ctx->territory, territories[i])) fputs(" font-weight-bold text-white", stdout);
            fputs("\" href=\"https://aurekai.", stdout); html(territories[i]);
            fputs("/discover\">.", stdout); html(territories[i]); fputs("</a></li>", stdout);
        } else {
            fputs("<a href=\"https://aurekai.", stdout); html(territories[i]);
            fputs("/stage\">.", stdout); html(territories[i]); fputs("</a>", stdout);
        }
    }
}

static void surface_emit_world_link(const NativeSurfaceContext *ctx, const BfSurfaceProgramOp *op) {
    const char *label = op->text;
    switch (ctx->presentation->world_link) {
    case BF_SURFACE_LINK_BOARD: fputs("<a class=\"brand\" href=\"", stdout); break;
    case BF_SURFACE_LINK_COMMUNITY: fputs("<a class=\"navbar-brand\" href=\"", stdout); break;
    case BF_SURFACE_LINK_MARKET: fputs("<a href=\"", stdout); break;
    case BF_SURFACE_LINK_STAGE: fputs("<a class=\"bg-primary text-white rounded px-4 py-2 font-semibold text-center\" href=\"", stdout); break;
    default: fputs("<a class=\"btn btn-light btn-sm rounded-pill font-weight-bold\" href=\"", stdout); break;
    }
    surface_world_href(ctx->world, op->cut); fputs("\">", stdout); html(label ? label : "World"); fputs("</a>", stdout);
}

static void surface_emit_world_nav(const NativeSurfaceContext *ctx) {
    size_t i;
    for (i = 0; i < BF_NATIVE_SURFACE_PROGRAM_COUNT; i++) {
        const BfNativeSurfaceProgram *candidate = &bf_native_surface_programs[i];
        const char *cut = candidate->program;
        if (!candidate->world_navigation) continue;
        if (ctx->presentation->world_nav == BF_SURFACE_NAV_STREAM && !strcmp(cut, "discover")) continue;
        /* A named interaction grammar is not a published World cut. The
         * compiled nav follows the World owner's actual fragment closure, so
         * a card cannot dead-link into an uncomposed application. */
        if (candidate->world_route && !world_fragment_route_published(ctx->world, cut)) continue;
        if (ctx->presentation->world_nav == BF_SURFACE_NAV_STREAM) {
            fputs("<li class=\"nav-item\"><a class=\"nav-link px-2 font-weight-bold\" href=\"", stdout);
        } else if (ctx->presentation->world_nav == BF_SURFACE_NAV_COMMUNITY) {
            fputs("<li class=\"nav-item\"><a class=\"nav-link", stdout);
            if (!strcmp(cut, "community")) fputs(" active", stdout);
            fputs("\" href=\"", stdout);
        } else if (ctx->presentation->world_nav == BF_SURFACE_NAV_MARKET) {
            fputs("<li><a href=\"", stdout);
        } else if (ctx->presentation->world_nav == BF_SURFACE_NAV_BOARD) {
            fputs("<a class=\"t\"", stdout);
            if (!strcmp(cut, "board")) fputs(" aria-current=\"page\"", stdout);
            fputs(" href=\"", stdout);
        } else {
            fputs("<a href=\"", stdout);
        }
        if (candidate->world_route) surface_world_href(ctx->world, cut);
        else fputs(candidate->route, stdout);
        fputs("\">", stdout); html(page_grammar_label(cut) ? page_grammar_label(cut) : cut); fputs("</a>", stdout);
        if (ctx->presentation->world_nav == BF_SURFACE_NAV_STREAM || ctx->presentation->world_nav == BF_SURFACE_NAV_COMMUNITY || ctx->presentation->world_nav == BF_SURFACE_NAV_MARKET) fputs("</li>", stdout);
    }
}

/* The next three data operations intentionally have no surface-name branches.
 * YAFF/CUE chooses an owner-backed operation; this executor projects its rows
 * in the current renderer grammar.  A Pack, capability program or World is
 * never manufactured merely because an operator chose an attractive shell. */
static void surface_emit_pack_demands(const NativeSurfaceContext *ctx) {
    sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL; int n = 0;
    (void)ctx;
    if (db && cms_type_registered(db, "pack_demand") && sqlite3_prepare_v2(db,
        "SELECT pack,demand_kind,needs,COALESCE(preferred_sources,''),COALESCE(body_regime,'') "
        "FROM pack_demand WHERE status='published' ORDER BY pack,demand_kind,id LIMIT 48", -1, &st, NULL) == SQLITE_OK) {
        fputs("<div class=\"bf-program-list bf-pack-demands\">", stdout);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *pack=(const char *)sqlite3_column_text(st,0), *kind=(const char *)sqlite3_column_text(st,1),
                *needs=(const char *)sqlite3_column_text(st,2), *preferred=(const char *)sqlite3_column_text(st,3),
                *regime=(const char *)sqlite3_column_text(st,4);
            fputs("<article class=\"bf-program-row\"><header><strong>",stdout); html(pack); fputs("</strong><span>",stdout); html(kind); fputs("</span></header><p>",stdout); html(needs); fputs("</p>",stdout);
            if (preferred && *preferred) { fputs("<small>preferred source families · ",stdout); html(preferred); fputs("</small>",stdout); }
            if (regime && *regime) { fputs("<small>body regime · ",stdout); html(regime); fputs("</small>",stdout); }
            fputs("</article>",stdout); n++;
        }
        fputs("</div>",stdout);
    }
    if (st) { sqlite3_finalize(st); } if (db) sqlite3_close(db);
    if (!n) fputs("<p class=\"bf-native-empty\">Nothing has been asked for here yet.</p>",stdout);
}

static void surface_emit_capability_programs(const NativeSurfaceContext *ctx) {
    sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL; int n = 0;
    (void)ctx;
    if (db && cms_type_registered(db, "capability_program") && sqlite3_prepare_v2(db,
        "SELECT application_kind,action,authority_requirement,failure_projection "
        "FROM capability_program WHERE status='published' ORDER BY application_kind,action,id LIMIT 48", -1, &st, NULL) == SQLITE_OK) {
        fputs("<div class=\"bf-program-list bf-capability-programs\">",stdout);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *app=(const char *)sqlite3_column_text(st,0), *action=(const char *)sqlite3_column_text(st,1),
                *authority=(const char *)sqlite3_column_text(st,2), *failure=(const char *)sqlite3_column_text(st,3);
            fputs("<article class=\"bf-program-row\"><header><strong>",stdout); html(action); fputs("</strong><span>",stdout); html(app); fputs("</span></header><p>Requires current authority · ",stdout); html(authority); fputs("</p><small>if unavailable · ",stdout); html(failure); fputs("</small></article>",stdout); n++;
        }
        fputs("</div>",stdout);
    }
    if (st) { sqlite3_finalize(st); } if (db) sqlite3_close(db);
    if (!n) fputs("<p class=\"bf-native-empty\">Nothing is on offer here yet.</p>",stdout);
}

static void surface_emit_world_catalogue(const NativeSurfaceContext *ctx) {
    sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL; int n = 0;
    (void)ctx;
    if (db && cms_type_registered(db, "world") && sqlite3_prepare_v2(db,
        "SELECT world,title,character,COALESCE(lede,'') FROM world WHERE status='published' ORDER BY id DESC LIMIT 48", -1, &st, NULL) == SQLITE_OK) {
        fputs("<div class=\"bf-program-list bf-world-catalogue\">",stdout);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *world=(const char *)sqlite3_column_text(st,0), *title=(const char *)sqlite3_column_text(st,1),
                *character=(const char *)sqlite3_column_text(st,2), *lede=(const char *)sqlite3_column_text(st,3);
            if (!world || !studio_safe_slug_arg(world)) continue;
            /* The character dresses the page; it is not something to read. */
            fputs("<article class=\"bf-program-row\" data-character=\"",stdout); html(character ? character : ""); fputs("\"><header><a href=\"/w/",stdout); html(world); fputs("\"><strong>",stdout); html(title); fputs("</strong></a></header>",stdout);
            if (lede && *lede) { fputs("<p>",stdout); html(lede); fputs("</p>",stdout); }
            fputs("</article>",stdout); n++;
        }
        fputs("</div>",stdout);
    }
    if (st) { sqlite3_finalize(st); } if (db) sqlite3_close(db);
    if (!n) fputs("<p class=\"bf-native-empty\">Nothing here yet.</p>",stdout);
}

static void surface_emit_subjects(const NativeSurfaceContext *ctx) {
    int i;
    for (i = 0; i < ctx->subject_count; i++) {
        const NativeNightSubject *s = &ctx->subjects[i];
        if (!surface_reading_matches(ctx, s->name, s->summary, native_night_primary(s, ctx->territory))) continue;
        if (ctx->presentation->subjects == BF_SURFACE_SUBJECT_STAGE) {
            fputs("<a class=\"mbz-card snap-center sm:max-w-xs w-[18rem] shrink-0 flex flex-col text-left p-3 rounded-lg\" href=\"", stdout);
            surface_world_href(ctx->world, NULL); fputs("\"><span class=\"text-xs uppercase tracking-widest opacity-70\">", stdout); html(s->kind);
            fputs("</span><span class=\"font-semibold mt-1 truncate\">", stdout); html(s->name);
            fputs("</span><span class=\"text-sm opacity-90 mt-1\">", stdout); html(s->tonight[0] ? s->tonight : s->summary); fputs("</span></a>", stdout);
        } else {
            const char *primary = native_night_primary(s, ctx->territory);
            fputs("<article class=\"timeline-status-component mb-4\"><div class=\"card shadow-sm\" style=\"border-radius:15px\"><div class=\"card-header border-0\"><div class=\"media align-items-center\"><span class=\"bf-av mr-2\" aria-hidden=\"true\">", stdout); html(s->kind);
            fputs("</span><div class=\"media-body\"><p class=\"font-weight-bold username mb-0\">", stdout); html(s->name);
            fputs("</p><p class=\"text-lighter mb-0\" style=\"font-size:13px\">published ", stdout); html(s->kind);
            fputs("</p></div></div></div><div class=\"px-3 py-3\"><p class=\"mb-2\">", stdout); html(primary); fputs("</p>", stdout);
            if (s->capacity[0]) { fputs("<span class=\"badge badge-light mr-1\">", stdout); html(s->capacity); fputs("</span>", stdout); }
            if (s->price[0]) { fputs("<span class=\"badge badge-light mr-1\">$", stdout); html(s->price); fputs("</span>", stdout); }
            if (s->safety[0]) { fputs("<p class=\"bf-cant\">", stdout); html(s->safety); fputs("</p>", stdout); }
            fputs("<p class=\"mb-0 mt-3\"><a class=\"btn btn-light font-weight-bold rounded-pill\" href=\"", stdout); surface_world_href(ctx->world, NULL); fputs("\">Open this World</a></p></div></div></article>", stdout);
        }
    }
}

static void surface_emit_publications(const NativeSurfaceContext *ctx) {
    int i, shown = 0;
    for (i = 0; i < ctx->publication_count; i++) {
        const NativePublication *p = &ctx->publications[i];
        if (!surface_reading_matches(ctx, p->attribution, p->title, "")) continue;
        shown++;
        fputs("<article class=\"timeline-status-component mb-4\"><div class=\"card shadow-sm\" style=\"border-radius:15px\"><div class=\"card-header border-0\"><p class=\"font-weight-bold username mb-0\">", stdout); html(p->attribution);
        fputs("</p><p class=\"text-lighter mb-0\" style=\"font-size:13px\">observed publication", stdout); if (p->when[0]) { fputs(" · ", stdout); html(p->when); }
        fputs("</p></div><div class=\"px-3 py-3\"><p class=\"mb-0 font-weight-bold\">", stdout);
        if (p->href[0]) { fputs("<a href=\"", stdout); html(p->href); fputs("\" rel=\"noopener\" target=\"_blank\">", stdout); }
        html(p->title); if (p->href[0]) fputs("</a>", stdout); fputs("</p></div></div></article>", stdout);
    }
    if (!shown && surface_reading_narrowed(ctx))
        fputs("<p class=\"note\">Nothing here matches this search.</p>", stdout);
}

static void surface_emit_world_fragments(const NativeSurfaceContext *ctx) {
    sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL; int n = 0;
    if (db && cms_type_registered(db, "world_fragment") && sqlite3_prepare_v2(db,
        "SELECT fragment_id,surface_grammar,renderer_family,COALESCE(visible_sections,''),COALESCE(source_projection,'') FROM world_fragment WHERE world=?1 AND status='published' ORDER BY ordinal,id", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, ctx->world, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) { const char *id=(const char *)sqlite3_column_text(st,0),*grammar=(const char *)sqlite3_column_text(st,1),*renderer=(const char *)sqlite3_column_text(st,2),*sections=(const char *)sqlite3_column_text(st,3),*projection=(const char *)sqlite3_column_text(st,4); if(!id||!grammar||!renderer)continue;
            fputs("<article class=\"pin-card grid-item\"><a class=\"pin-preview-image\" href=\"",stdout);surface_world_href(ctx->world,grammar);page_emit_reading_query(ctx->query,NULL,NULL);fputs("\"><span class=\"pin-shade\"><b>",stdout);html(grammar);fputs("</b></span><span class=\"pin-terr\">",stdout);html(renderer);fputs("</span><span class=\"pin-gen\">world view</span></a><div class=\"pin-footer\"><p class=\"description\">",stdout);html(sections&&*sections?sections:"Published World fragment");fputs("</p><p class=\"details\"><a href=\"",stdout);surface_world_href(ctx->world,grammar);page_emit_reading_query(ctx->query,NULL,NULL);fputs("\">Open ",stdout);html(grammar);fputs("</a>",stdout);if(projection&&*projection)fputs(" · native projection",stdout);fputs("</p></div></article>",stdout);n++; }
    }
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    if (!n) fputs("<section class=\"pn-refuse\"><strong>No published arrangement exists for this World.</strong></section>", stdout);
}

static int community_active_member_count(sqlite3 *db, const char *community);
static int community_public_presence_count(sqlite3 *db, const char *community);
static int community_post_reaction_count(sqlite3 *db, const char *post);
static void surface_emit_participation_nav(void){
    fputs("<nav aria-label=\"Community discovery\"><a href=\"/communities\">Find communities</a> · <a href=\"/community/new\">Start a community</a> · <a href=\"/profile?tab=communities\">My communities</a></nav>",stdout);
}

static int world_community_id(sqlite3 *db, const char *world, char *out, size_t out_sz) {
    sqlite3_stmt *st = NULL; const char *subject; int found = 0;
    if (!db || !world || !*world || !out || out_sz < 2 || !cms_type_registered(db,"world_fragment") ||
        sqlite3_prepare_v2(db,"SELECT subject_address FROM world_fragment WHERE world=?1 AND surface_grammar='community' AND status='published' ORDER BY ordinal,id LIMIT 1",-1,&st,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_text(st,1,world,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st)==SQLITE_ROW && (subject=(const char *)sqlite3_column_text(st,0)) && !strncmp(subject,"community:",10) && studio_safe_slug_arg(subject+10) && snprintf(out,out_sz,"%s",subject+10)<(int)out_sz) found=1;
    sqlite3_finalize(st); return found;
}

/* Which community this page is about.
 *
 * The address decides, then the World it is addressed under. Before this the
 * only answer was the World's, so /community?community=<name> rendered the
 * World's community while displaying the asked-for name nowhere -- the page
 * and the record could disagree without either being wrong on its own terms. */
static int surface_community_id(sqlite3 *db, const NativeSurfaceContext *ctx,
                                char *out, size_t cap) {
    if (ctx->community && *ctx->community) {
        snprintf(out, cap, "%s", ctx->community);
        return 1;
    }
    return world_community_id(db, ctx->world, out, cap);
}

/* One published community_body, or nothing. There is no default row and no
 * invented one: a community nobody published is not rendered as an empty one. */
static int surface_community_body(sqlite3 *db, const char *community, int column,
                                 char *out, size_t cap) {
    sqlite3_stmt *st = NULL; int got = 0;
    out[0] = 0;
    if (!db || !community || !*community || !cms_type_registered(db, "community_body")) return 0;
    if (sqlite3_prepare_v2(db,
          "SELECT title,COALESCE(description,''),COALESCE(governance_scope,''),"
          " COALESCE(moderation_policy,''),cannot_infer FROM community_body"
          " WHERE community=?1 AND status='published' AND visibility='public'"
          " ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, community, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *v = (const char *)sqlite3_column_text(st, column);
        if (v && *v) { snprintf(out, cap, "%s", v); got = 1; }
    }
    sqlite3_finalize(st);
    return got;
}

/* A community's own words about itself, with the authored wording as the
 * fallback for the one case where no record exists to speak for it. */
static void surface_emit_community_field(const NativeSurfaceContext *ctx,
                                         const BfSurfaceProgramOp *op, int column) {
    sqlite3 *db = cms_open_ro(); char community[128] = {0}, value[2048] = {0};
    int got = 0;
    if (db && surface_community_id(db, ctx, community, sizeof(community)))
        got = surface_community_body(db, community, column, value, sizeof(value));
    if (db) sqlite3_close(db);
    if (got) html(value);
    else if (op->text) html(op->text);
}

/* Who is here, counted rather than described. Silent where no community body
 * was published, because a count of nobody is not the same claim as no claim. */
static void surface_emit_community_standing(const NativeSurfaceContext *ctx) {
    sqlite3 *db = cms_open_ro(); char community[128] = {0}, title[256] = {0};
    if (db && surface_community_id(db, ctx, community, sizeof(community)) &&
        surface_community_body(db, community, 0, title, sizeof(title))) {
        int members = community_active_member_count(db, community);
        int present = community_public_presence_count(db, community);
        printf("%d active members \xc2\xb7 %d publicly here now", members, present);
    }
    if (db) sqlite3_close(db);
}

/* What the page is not. The community states its own limit; the authored text
 * follows it, because where federation is projected is this estate's fact and
 * not the community's. */
static void surface_emit_community_limit(const NativeSurfaceContext *ctx,
                                        const BfSurfaceProgramOp *op) {
    sqlite3 *db = cms_open_ro(); char community[128] = {0}, value[2048] = {0};
    int got = 0;
    if (db && surface_community_id(db, ctx, community, sizeof(community)))
        got = surface_community_body(db, community, 4, value, sizeof(value));
    if (db) sqlite3_close(db);
    if (got) { fputs("<strong>What this is not.</strong> ", stdout); html(value); fputs(" ", stdout); }
    if (op->text) html(op->text);
}

static void surface_emit_community_posts(const NativeSurfaceContext *ctx) {
    surface_emit_participation_nav();
    sqlite3 *db=cms_open_ro(); sqlite3_stmt *st=NULL; char community[128]={0}; int n=0;
    if(db && surface_community_id(db,ctx,community,sizeof(community)) && cms_type_registered(db,"community_post")&&sqlite3_prepare_v2(db,"SELECT author_subject,COALESCE(title,''),body,published_at,COALESCE(reply_to,''),post FROM community_post WHERE community=?1 AND status='published' AND visibility='public' AND moderation_state IN ('accepted','visible','published') ORDER BY published_at DESC,id DESC LIMIT 100",-1,&st,NULL)==SQLITE_OK) { sqlite3_bind_text(st,1,community,-1,SQLITE_TRANSIENT); while(sqlite3_step(st)==SQLITE_ROW){const char *author=(const char*)sqlite3_column_text(st,0),*title=(const char*)sqlite3_column_text(st,1),*body=(const char*)sqlite3_column_text(st,2),*at=(const char*)sqlite3_column_text(st,3),*reply=(const char*)sqlite3_column_text(st,4),*post=(const char*)sqlite3_column_text(st,5);int reactions;if(!surface_reading_matches(ctx,author,title,body))continue;reactions=community_post_reaction_count(db,post);fputs("<article class=\"post-listing my-2\"><small class=\"text-muted\">",stdout);html(author);fputs(" · ",stdout);html(at);if(reply&&*reply)fputs(" · reply",stdout);fputs("</small>",stdout);if(title&&*title){fputs("<h2 class=\"h6 mb-1\">",stdout);html(title);fputs("</h2>",stdout);}fputs("<div class=\"md-div\">",stdout);html(body);fputs("</div>",stdout);if(reactions)printf("<small class=\"text-muted\">%d accepted reactions</small>",reactions);fputs("</article>",stdout);n++;} }
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    if (!n) fputs(surface_reading_narrowed(ctx) ?
        "<section class=\"post-listing my-2\"><strong>Nothing here matches this search.</strong></section>" :
        "<section class=\"post-listing my-2\"><strong>Nothing has been posted here yet.</strong></section>",stdout);
}

static int surface_offer_count(const char *world) {
    sqlite3 *db=cms_open_ro(); sqlite3_stmt *st=NULL; int n=0;
    if(db&&cms_type_registered(db,"service_offer")&&sqlite3_prepare_v2(db,"SELECT count(*) FROM service_offer WHERE status='published' AND visibility='public' AND (world=?1 OR subject=?1)",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,world,-1,SQLITE_TRANSIENT);if(sqlite3_step(st)==SQLITE_ROW)n=sqlite3_column_int(st,0);}if(st)sqlite3_finalize(st);if(db)sqlite3_close(db);return n;
}

static void surface_emit_offer_status(const NativeSurfaceContext *ctx) { printf("<li>Published: %d</li><li>Drafts: not shown</li>", surface_offer_count(ctx->world)); }
static void surface_emit_offer_empty(const NativeSurfaceContext *ctx) { if(!surface_offer_count(ctx->world)) { fputs("<section class=\"rounded-lg border border-neutral-200 p-6 dark:border-neutral-700\"><h2 class=\"text-lg font-semibold\">Nothing is listed here yet.</h2><p class=\"mt-4\"><a class=\"underline\" href=\"",stdout);surface_world_href(ctx->world,"stage");fputs("\">See what is on</a></p></section>",stdout); } }

/* SurfaceProgram may ask for the lawful actions of the World it resolved, but
 * it cannot name an action, its authority, or an endpoint in its presentation
 * declaration.  ObjectContract owns that vocabulary; this generic bridge
 * merely lowers the current caller's already-authorized aperture into links to
 * AtomicForms.  The form and submit paths re-check authority themselves. */
static void surface_query_value(const char *value) {
    const unsigned char *p = (const unsigned char *)(value ? value : "");
    for (; *p; p++) {
        if (isalnum(*p) || strchr("-_.~:", *p)) putchar(*p);
        else printf("%%%02X", (unsigned)*p);
    }
}

static int surface_world_subject(sqlite3 *db, const char *world,
                                 char *out, size_t out_sz) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    out[0] = '\0';
    if (!db || !world || !*world || !cms_type_registered(db, "world") ||
        sqlite3_prepare_v2(db,
          "SELECT COALESCE(subject,'') FROM world WHERE world=?1 AND status='published' "
          "ORDER BY id DESC LIMIT 2", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, world, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *subject = (const char *)sqlite3_column_text(st, 0);
        if (subject && *subject && snprintf(out, out_sz, "%s", subject) < (int)out_sz)
            found = 1;
    }
    /* A World slug cannot silently select one of two published semantic
     * subjects. Ambiguity is not an action context. */
    if (sqlite3_step(st) == SQLITE_ROW) found = 0;
    sqlite3_finalize(st);
    return found;
}

static void surface_emit_subject_apertures(const NativeSurfaceContext *ctx) {
    sqlite3 *db = cms_open_ro();
    sqlite3_stmt *st = NULL;
    char subject[256], kind[64];
    const char *colon;
    int offered = 0;
    if (!db || !surface_world_subject(db, ctx->world, subject, sizeof(subject)) ||
        !cms_type_registered(db, "object_contract")) goto out;
    colon = strchr(subject, ':');
    {
        size_t n = colon ? (size_t)(colon - subject) : strlen(subject);
        if (!n || n >= sizeof(kind)) goto out;
        memcpy(kind, subject, n); kind[n] = '\0';
    }
    if (!studio_safe_ident(kind) ||
        sqlite3_prepare_v2(db,
          "SELECT birth_operation,COALESCE(action_label,''),COALESCE(requires_authority,'') "
          "FROM object_contract WHERE applies_to_kind=?1 AND status='published' "
          "ORDER BY object_name,birth_operation,id", -1, &st, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_text(st, 1, kind, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *operation = (const char *)sqlite3_column_text(st, 0);
        const char *label = (const char *)sqlite3_column_text(st, 1);
        const char *requires = (const char *)sqlite3_column_text(st, 2);
        if (!operation || !*operation ||
            (requires && *requires && !surface_authority_satisfied(requires, ctx->bearer, ctx->auth_db)))
            continue;
        if (!offered) fputs("<nav class=\"bf-surface-apertures\" aria-label=\"Available actions\">", stdout);
        fputs("<a class=\"bf-surface-aperture\" hx-get=\"/_aperture/form?subject=", stdout);
        surface_query_value(subject);
        fputs("&amp;operation=", stdout);
        surface_query_value(operation);
        fputs("\" hx-target=\"#bf-aperture-result\" hx-swap=\"innerHTML\" href=\"/_aperture/form?subject=", stdout);
        surface_query_value(subject);
        fputs("&amp;operation=", stdout);
        surface_query_value(operation);
        fputs("\">", stdout);
        page_glyph(page_operation_glyph(operation));
        html(label && *label ? label : operation);
        fputs("</a>", stdout);
        offered++;
    }
    if (offered) fputs("</nav><section id=\"bf-aperture-result\" aria-live=\"polite\"></section>", stdout);
out:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
}

static void surface_emit_text(const BfSurfaceProgramOp *op) {
    if (!op->text) return;
    html(op->text);
}

static void bf_surface_render_program_slot(const BfSurfaceProgramSlot *slot, const BfNativeSurfaceProgram *program, const NativeSurfaceContext *ctx) {
    unsigned short i;
    for (i = 0; i < slot->count; i++) { const BfSurfaceProgramOp *op = &program->ops[slot->first + i];
        switch (op->opcode) {
        case BF_SURFACE_TEXT: surface_emit_text(op); break;
        case BF_SURFACE_DATE: native_night_date(op->text); break;
        case BF_SURFACE_TERRITORY_NAV: surface_emit_territory_nav(ctx); break;
        case BF_SURFACE_WORLD_NAV: surface_emit_world_nav(ctx); break;
        case BF_SURFACE_WORLD_LINK: surface_emit_world_link(ctx, op); break;
        case BF_SURFACE_NIGHT_SUBJECTS: surface_emit_subjects(ctx); break;
        case BF_SURFACE_PUBLICATIONS: surface_emit_publications(ctx); break;
        case BF_SURFACE_PACK_DEMANDS: surface_emit_pack_demands(ctx); break;
        case BF_SURFACE_CAPABILITY_PROGRAMS: surface_emit_capability_programs(ctx); break;
        case BF_SURFACE_WORLD_CATALOGUE: surface_emit_world_catalogue(ctx); break;
        case BF_SURFACE_SUBJECT_APERTURES: surface_emit_subject_apertures(ctx); break;
        case BF_SURFACE_WORLD_FRAGMENTS: surface_emit_world_fragments(ctx); break;
        case BF_SURFACE_COMMUNITY_POSTS: surface_emit_community_posts(ctx); break;
        case BF_SURFACE_COMMUNITY_TITLE: surface_emit_community_field(ctx, op, 0); break;
        case BF_SURFACE_COMMUNITY_LEDE: surface_emit_community_field(ctx, op, 1); break;
        case BF_SURFACE_COMMUNITY_STANDING: surface_emit_community_standing(ctx); break;
        case BF_SURFACE_COMMUNITY_LIMIT: surface_emit_community_limit(ctx, op); break;
        case BF_SURFACE_OFFER_STATUS: surface_emit_offer_status(ctx); break;
        case BF_SURFACE_OFFER_EMPTY: surface_emit_offer_empty(ctx); break;
        }
    }
}

#include "source_desk.h"

static void bf_surface_render_native_document(const BfNativeSurfaceProgram *program,
                                              const NativeSurfaceContext *ctx) {
    unsigned short slot;
    if(!strcmp(program->program,"link")){
        fputs("<body", stdout);
        bf_stitch_work_emit_body_attribute(stdout, bf_stitch_work_native_pattern(ctx->territory, program->route));
        fputs(">" BF_ESTATE_BAR,stdout);source_desk_content();fputs("</body></html>",stdout);return;
    }
    fputs("<body class=\"bf-native-surface renderer-", stdout); html(program->presentation->renderer);
    fputs(" territory-", stdout); html(ctx->territory);
    fputs("\"", stdout);
    bf_stitch_work_emit_body_attribute(stdout, bf_stitch_work_native_pattern(ctx->territory, program->route));
    fputs(">" BF_ESTATE_BAR "<main class=\"bf-native-layout\"><header class=\"bf-native-masthead\"><p class=\"bf-native-kicker\">AUREKAI · ", stdout);
    html(ctx->territory); fputs("</p><h1>", stdout);
    html(page_grammar_label(program->program) ? page_grammar_label(program->program) : program->program);
    fputs("</h1></header>", stdout);
    surface_emit_reading_controls(ctx);
    for (slot = 0; slot < program->slot_count; slot++) {
        const BfSurfaceProgramSlot *compiled_slot = &program->slots[slot];
        if (!compiled_slot->count) continue;
        printf("<section class=\"bf-native-slot bf-native-slot-%u\">", slot);
        bf_surface_render_program_slot(compiled_slot, program, ctx);
        fputs("</section>", stdout);
    }
    fputs("</main></body></html>", stdout);
}

static int emit_native_donor_surface(const char *path, const char *kind, const char *territory,
                                     const char *domain, const char *world,
                                     const char *community, const char *bearer,
                                     const char *auth_db, const char *target, const char *query) {
    FILE *f; long size; char *body,*limit,*cursor; sqlite3 *db; NativeNightSubject subjects[NATIVE_NIGHT_MAX]; NativePublication publications[NATIVE_PUBLICATION_MAX]; int count, publication_count;
    const char *publication_scope = (domain && *domain) ? domain : "discover";
    if (!(f=fopen(path,"rb"))) return 0;
    if (fseek(f,0,SEEK_END)|| (size=ftell(f))<1 || size>2*1024*1024 || fseek(f,0,SEEK_SET)) {fclose(f);return 0;}
    if (!(body=malloc((size_t)size+1))) {fclose(f);return 0;} if (fread(body,1,(size_t)size,f)!=(size_t)size){free(body);fclose(f);return 0;} fclose(f); body[size]='\0';
    if (!(limit=strstr(body,"<script"))) {free(body);return 0;}
    count=publication_count=0;
    if(strcmp(kind,"link")){db=cms_open_ro();count=db?native_night_subjects(db,subjects,NATIVE_NIGHT_MAX):0;publication_count=db?native_publications(db,publications,NATIVE_PUBLICATION_MAX,publication_scope):0;if(db)sqlite3_close(db);}
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nVary: Host\r\nX-Bonfyre-Surface-Program: CUE-native\r\nConnection: close\r\n\r\n");
    cursor=body;
    { const BfNativeSurfaceProgram *program = native_surface_program(kind);
      NativeSurfaceContext ctx;
      if (!program) { free(body); return 0; }
      ctx = (NativeSurfaceContext){kind, program->presentation, territory, domain, world,
                                   bearer, auth_db, community, subjects, count, publications, publication_count, target, query};
      if (program->document_mode == BF_SURFACE_DOCUMENT_NATIVE) {
          char *head_end = strstr(body, "</head>");
          if (!head_end) { free(body); return 0; }
          /* A native document states its structure; this one stylesheet is
           * what makes that structure readable in every territory. */
          if(!strcmp(kind,"link"))source_desk_head();
          else {
              fwrite(body,1,(size_t)(head_end-body),stdout);
              fputs("<link rel=\"stylesheet\" href=\"/assets/aurekai-native.css?v=2\">",stdout);
              bf_stitch_work_emit_head(stdout, bf_stitch_work_native_pattern(territory, program->route));
              fputs("</head>",stdout);
          }
          bf_surface_render_native_document(program, &ctx);
      } else {
          /* Every donor surface wears the estate's bar before its own body. */
          char *head_end = strstr(body, "</head>");
          /* Locate the actual body after the head: CSS and documentation
           * comments may mention a literal <body> before the real tag. */
          char *open = head_end ? strstr(head_end + 7, "<body") : NULL;
          char *close = open ? strchr(open, '>') : NULL;
          const char *first = next_native_marker(body, kind);
          if (close && close < limit && (!first || first > close)) {
              const char *pattern = bf_stitch_work_native_pattern(territory, program->route);
              if (pattern && head_end && head_end < open) {
                  fwrite(body, 1, (size_t)(head_end - body), stdout);
                  bf_stitch_work_emit_head(stdout, pattern);
                  fwrite(head_end, 1, (size_t)(close - head_end), stdout);
                  bf_stitch_work_emit_body_attribute(stdout, pattern);
                  fputc('>', stdout);
              } else fwrite(body, 1, (size_t)(close + 1 - body), stdout);
              fputs(BF_ESTATE_BAR, stdout); surface_emit_reading_controls(&ctx); cursor = close + 1;
          }
      }
      if (program->document_mode != BF_SURFACE_DOCUMENT_NATIVE) while(cursor<limit){const char *mark=next_native_marker(cursor,kind), *mark_end; const BfSurfaceProgramSlot *compiled_slot; int slot=-1; if(!mark||mark>=limit){fwrite(cursor,1,(size_t)(limit-cursor),stdout);break;} mark_end=strstr(mark,"-->"); if(!mark_end||mark_end>=limit||(slot=native_surface_slot_number(mark))<0||(compiled_slot=native_surface_slot(kind,slot))==NULL||!compiled_slot->count){free(body);return 0;} fwrite(cursor,1,(size_t)(mark-cursor),stdout); bf_surface_render_program_slot(compiled_slot, program, &ctx); cursor=(char *)mark_end+3;}
    }
    if (native_surface_program(kind)->document_mode != BF_SURFACE_DOCUMENT_NATIVE) { fputs("</body></html>",stdout); }free(body);return 1;
}

/* Discover and Stage keep their Pixelfed/Mobilizon documents, but the browser
 * no longer owns a second renderer for their addressed World material.  The
 * authored donor shell is read once; native lowering replaces its explicit
 * markers before the response is sent. */
static int studio_safe_slug_arg(const char *text);

/* A nested World grammar is an addressed cut, not a query parameter.  It is
 * accepted only when the World owner published that exact fragment. */
static int world_fragment_route_published(const char *world, const char *grammar) {
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int found = 0;
    if (!studio_safe_slug_arg(world) || !presentation_grammar_allowed(grammar)) return 0;
    if (!(db = cms_open_ro()) || !cms_type_registered(db, "world_fragment") ||
        sqlite3_prepare_v2(db, "SELECT 1 FROM world_fragment WHERE world=?1 AND surface_grammar=?2 AND status='published' LIMIT 1", -1, &st, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_text(st, 1, world, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, grammar, -1, SQLITE_TRANSIENT);
    found = sqlite3_step(st) == SQLITE_ROW;
out:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    return found;
}

static int serve_native_donor_route(const char *method, const char *target,
                                    const char *accept, const char *root,
                                    const char *host, const char *query,
                                    const char *bearer, const char *auth_db) {
    const BfNativeSurfaceProgram *program = NULL;
    const char *kind = NULL;
    char path[PATH_MAX], domain[96] = {0}, world[128] = "tonight-in-this-city";
    char community[128] = {0};
    /* Discover and Stage are public documents.  An explicit machine ask may
     * take the owner contract, but a wildcard client must not fall through to
     * the authored donor file with its legacy browser renderer.  That would
     * make crawlers and plain HTTP clients observe a second, script-owned
     * version of the same route. */
    if (!root || !*root || strcmp(method, "GET") ||
        (wants_json(accept) && !wants_html(accept))) return 0;
    /* A main public route composed from real material outranks the donor
     * program that would otherwise lower owners with nothing published. */
    if (emit_item_calendar(target, host)) return 1;
    if (emit_agenda_calendar(target, host)) return 1;
    if (emit_page_program_route(root, target, host, query)) return 1;
    if ((program = native_surface_program_route(target)) != NULL) kind = program->program;
    else if (!strncmp(target, "/w/", 3)) {
        const char *tail = target + 3, *slash = strrchr(tail, '/'); size_t n;
        if (!slash || !slash[1]) return 0;
        n = (size_t)(slash - tail);
        if (!n || n >= sizeof(world)) return 0;
        memcpy(world, tail, n); world[n] = '\0'; kind = slash + 1;
        program = native_surface_program(kind);
        if (!program || !program->world_route) return 0;
        if (!world_fragment_route_published(world, kind)) {
            protocol_reply(404, "text/html; charset=utf-8", "<p class=\"bf-refuse\">This World has not published that interaction grammar.</p>");
            return 1;
        }
    }
    else return 0;
    if (query_value(query, "domain", domain, sizeof(domain)) &&
        (!safe(domain, sizeof(domain)) || !studio_safe_ident(domain))) {
        protocol_reply(400, "text/html; charset=utf-8", "<p class=\"bf-refuse\">The requested domain shape is invalid.</p>");
        return 1;
    }
    if (snprintf(path, sizeof(path), "%s/surfaces/%s.html", root, program->template_name) >= (int)sizeof(path)) return 0;
    if (query_value(query, "community", community, sizeof(community)) &&
        !studio_safe_slug_arg(community)) {
        protocol_reply(400, "text/html; charset=utf-8", "<p class=\"bf-refuse\">That is not a community identifier.</p>");
        return 1;
    }
    return emit_native_donor_surface(path, kind, territory_of(host), domain, world,
                                     community[0] ? community : NULL, bearer, auth_db, target, query);
}

/* A URL path segment used as a lookup key. Kept to the same character class as
 * the other validators because it reaches a SQL literal and an argv slot. */
static int studio_safe_slug_arg(const char *text) {
    if (!text || !*text || strlen(text) > 96) return 0;
    for (; *text; text++)
        if (!(isalnum((unsigned char)*text) || *text == '-' || *text == '_' || *text == '.' || *text == '@')) return 0;
    return 1;
}

/* A renderer family is the portable interaction physics; a variant is the
 * presentation strategy inside that family. */
/* The complete 15 × 4 presentation algebra is CUE-authored and Zig-compiled.
 * These are generic descriptor lookups, not a second grammar/role policy in
 * C: the resolved class chooses presentation only and never grants closure. */
static const BfNativePresentationCell *presentation_cell(const char *grammar, const char *klass) {
    size_t i;
    for (i = 0; i < BF_NATIVE_PRESENTATION_CELL_COUNT; i++)
        if (!strcmp(bf_native_presentation_cells[i].grammar, grammar) &&
            !strcmp(bf_native_presentation_cells[i].presentation_class, klass))
            return &bf_native_presentation_cells[i];
    return NULL;
}

static const char *presentation_density(const char *klass) {
    size_t i;
    for (i = 0; i < BF_NATIVE_PRESENTATION_CELL_COUNT; i++)
        if (!strcmp(bf_native_presentation_cells[i].presentation_class, klass))
            return bf_native_presentation_cells[i].density;
    return "low";
}

#define WORLD_FRAGMENT_PLAN_MAX 64
typedef struct {
    char fragment_id[192];
    char subject_address[512];
    char surface_grammar[64];
    char renderer_family[64];
    char renderer_variant[96];
    char extent[32];
    char template_name[256];
    char source_projection[512];
    char media_posture[64];
    char temporal_posture[64];
    char continuity_group[128];
    char visible_sections[512];
    char action_aperture[512];
    char provenance[768];
    char cannot_infer[768];
} WorldFragmentPlan;

static int composition_renderer_family_allowed(const char *family) {
    size_t i;
    if (!family || !*family) return 0;
    for (i = 0; i < BF_SURFACE_RENDERER_FAMILY_COUNT; i++)
        if (!strcmp(family, bf_surface_renderer_families[i])) return 1;
    return 0;
}

static int composition_extent_allowed(const char *extent) {
    size_t i;
    if (!extent || !*extent) return 0;
    for (i = 0; i < BF_SURFACE_EXTENT_COUNT; i++)
        if (!strcmp(extent, bf_surface_extents[i])) return 1;
    return 0;
}

static int load_world_fragment_plans(sqlite3 *db, const char *world,
                                     WorldFragmentPlan *out, size_t cap) {
    sqlite3_stmt *st = NULL;
    int count = 0;
    const char *sql =
        "SELECT fragment_id,subject_address,surface_grammar,renderer_family,"
        "COALESCE(renderer_variant,''),extent,COALESCE(template,''),COALESCE(source_projection,''),"
        "COALESCE(media_posture,''),COALESCE(temporal_posture,''),COALESCE(continuity_group,''),"
        "COALESCE(visible_sections,''),COALESCE(action_aperture,''),COALESCE(provenance,''),cannot_infer "
        "FROM world_fragment WHERE world=?1 AND status='published' ORDER BY ordinal,id";
    if (!db || !cms_type_registered(db, "world_fragment")) return 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, world, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        WorldFragmentPlan *f;
        const char *grammar = (const char *)sqlite3_column_text(st, 2);
        const char *family = (const char *)sqlite3_column_text(st, 3);
        const char *extent = (const char *)sqlite3_column_text(st, 5);
        if ((size_t)count >= cap || !presentation_grammar_allowed(grammar) ||
            !composition_renderer_family_allowed(family) || !composition_extent_allowed(extent)) {
            sqlite3_finalize(st);
            return -1;
        }
        f = &out[count++];
#define COPY_COL(field, col) snprintf(f->field, sizeof(f->field), "%s", sqlite3_column_text(st, col) ? (const char *)sqlite3_column_text(st, col) : "")
        COPY_COL(fragment_id, 0); COPY_COL(subject_address, 1); COPY_COL(surface_grammar, 2);
        COPY_COL(renderer_family, 3); COPY_COL(renderer_variant, 4); COPY_COL(extent, 5);
        COPY_COL(template_name, 6); COPY_COL(source_projection, 7); COPY_COL(media_posture, 8);
        COPY_COL(temporal_posture, 9); COPY_COL(continuity_group, 10); COPY_COL(visible_sections, 11);
        COPY_COL(action_aperture, 12); COPY_COL(provenance, 13); COPY_COL(cannot_infer, 14);
#undef COPY_COL
        if (!f->fragment_id[0] || !f->subject_address[0] || !f->cannot_infer[0]) {
            sqlite3_finalize(st);
            return -1;
        }
    }
    sqlite3_finalize(st);
    return count;
}

/* Native heterogeneous World composition. This lowerer decides presentation
 * structure only. Subject truth remains in the World/source owners, actor
 * class comes from native closure, and every effect still crosses its owner. */
static int serve_world_composition(const char *method, const char *target, const char *query,
                                   const char *bearer, const char *auth_db) {
    char world[128], title[512], character[128], template_name[256], subject[512];
    char grammars[1024], cannot_infer[1024];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    const char *klass;
    const BfNativePresentationCell *world_cell;
    int rc, fragment_count = 0;

    if (strcmp(target, "/_surface/world-composition")) return 0;
    if (strcmp(method, "GET")) {
        protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n");
        return 1;
    }
    if (presentation_has_forbidden_coordinate(query)) {
        protocol_reply(400, "application/json", "{\"error\":\"forbidden_presentation_coordinate\",\"limit\":\"World composition resolves presentation class from native closure; actor, audience, institution, role and authority are not URL coordinates\"}\n");
        return 1;
    }
    if (!query_value(query, "world", world, sizeof(world)) || !studio_safe_slug_arg(world)) {
        protocol_reply(400, "application/json", "{\"error\":\"world_required\"}\n");
        return 1;
    }
    db = cms_open_ro();
    if (!db || !cms_type_registered(db, "world")) {
        if (db) sqlite3_close(db);
        protocol_reply(503, "application/json", "{\"error\":\"world_store_unavailable\"}\n");
        return 1;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT title,character,COALESCE(template,''),COALESCE(subject,''),COALESCE(grammars,''),cannot_infer "
        "FROM world WHERE world=?1 AND status='published' ORDER BY id DESC LIMIT 2",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        protocol_reply(500, "application/json", "{\"error\":\"world_query_failed\"}\n");
        return 1;
    }
    sqlite3_bind_text(st, 1, world, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(st);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(st); sqlite3_close(db);
        protocol_reply(404, "application/json", "{\"error\":\"world_not_found\"}\n");
        return 1;
    }
    snprintf(title, sizeof(title), "%s", sqlite3_column_text(st, 0) ? (const char *)sqlite3_column_text(st, 0) : "");
    snprintf(character, sizeof(character), "%s", sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : "");
    snprintf(template_name, sizeof(template_name), "%s", sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "");
    snprintf(subject, sizeof(subject), "%s", sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "");
    snprintf(grammars, sizeof(grammars), "%s", sqlite3_column_text(st, 4) ? (const char *)sqlite3_column_text(st, 4) : "");
    snprintf(cannot_infer, sizeof(cannot_infer), "%s", sqlite3_column_text(st, 5) ? (const char *)sqlite3_column_text(st, 5) : "");
    if (sqlite3_step(st) == SQLITE_ROW) {
        sqlite3_finalize(st); sqlite3_close(db);
        protocol_reply(409, "application/json", "{\"error\":\"ambiguous_world\",\"limit\":\"a World slug must identify one published composition before it can be lowered\"}\n");
        return 1;
    }
    sqlite3_finalize(st); sqlite3_close(db);
    klass = native_presentation_class(bearer, auth_db);
    world_cell = presentation_cell("worlds", klass);
    if (!world_cell) {
        protocol_reply(500, "application/json", "{\"error\":\"presentation_cell_missing\"}\n");
        return 1;
    }

    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n");
    fputs("{\"schema_version\":\"bonfyre.surface_composition.v1\",\"world\":", stdout); json(world);
    fputs(",\"world_address\":", stdout); { char address[192]; snprintf(address, sizeof(address), "/w/%s", world); json(address); }
    fputs(",\"title\":", stdout); json(title);
    fputs(",\"subject_address\":", stdout);
    if (*subject) json(subject); else { char fallback[192]; snprintf(fallback, sizeof(fallback), "world:%s", world); json(fallback); }
    fputs(",\"character\":", stdout); json(character);
    fputs(",\"template_baseline\":", stdout); json(template_name);
    fputs(",\"presentation_class\":", stdout); json(klass);
    fputs(",\"interaction_density\":", stdout); json(world_cell->density);
    fputs(",\"ordered_fragments\":[", stdout);
    {
        char work[sizeof(grammars)];
        char *p, *end;
        int first = 1;
        snprintf(work, sizeof(work), "%s", grammars);
        p = work;
        while (*p) {
            char *start = p;
            while (*p && *p != ',') p++;
            end = p;
            if (*p == ',') *p++ = '\0';
            while (*start && isspace((unsigned char)*start)) start++;
            while (end > start && isspace((unsigned char)end[-1])) *--end = '\0';
            if (!*start || !presentation_grammar_allowed(start)) continue;
            if (!first) putchar(',');
            first = 0;
            {
                char fragment_id[320];
                const BfNativePresentationCell *cell = presentation_cell(start, klass);
                if (!cell) continue;
                /* `start` comes from an authored list but remains physically
                 * bounded here. The grammar validator admits short named
                 * grammars today; the precision keeps a malformed future CMS
                 * value from turning a presentation identifier into a buffer
                 * claim. */
                snprintf(fragment_id, sizeof(fragment_id), "%s:%02d:%.96s", world, fragment_count, start);
                fputs("{\"fragment_id\":", stdout); json(fragment_id);
                fputs(",\"surface_grammar\":", stdout); json(start);
                fputs(",\"renderer_family\":", stdout); json(cell->renderer_family);
                fputs(",\"renderer_variant\":", stdout); json(cell->renderer_variant);
                fputs(",\"extent\":", stdout); json(cell->extent);
                fputs(",\"interaction_density\":", stdout); json(cell->density);
                fputs(",\"template\":", stdout); json(template_name);
                fputs(",\"truth_level\":\"presentation_plan\"}", stdout);
                fragment_count++;
            }
        }
    }
    fputs("],\"fragment_count\":", stdout); printf("%d", fragment_count);
    fputs(",\"cannot_infer\":[", stdout); json(cannot_infer);
    fputs(",\"presentation plan -> subject truth\",\"presentation class -> actor identity or authority\",\"renderer or affordance -> authorized effect\",\"template reference -> copied third-party theme code\"],", stdout);
    fputs("\"required_followups\":[\"InformationPolicy before disclosure\",\"CapabilityClosure before executable action\",\"native effect owner before mutation or spend\"]}\n", stdout);
    return 1;
}

/* SiteProgram material is selected through the binding's authored collection.
 * There is deliberately no global-publication fallback: a published SiteProgram
 * with no material remains a truthful, typed empty surface. */
#endif /* BONFYRE_SURFACE_WORLD_AND_COMMUNITY_H */
