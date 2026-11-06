#ifndef BONFYRE_PAGE_ROUTES_AND_EMITTERS_H
#define BONFYRE_PAGE_ROUTES_AND_EMITTERS_H
/* Extracted verbatim from main.c lines 10682-13518 (mechanical split, no logic changes). */
static int emit_native_world_page(const char *path, const char *target, const char *query) {
    char world[128], title[768] = {0}, family[64] = {0}, lede[2048] = {0};
    char sources[4096] = {0}, resolved_sources[4096] = {0}, grammars[1024] = {0}, cannot[2048] = {0}, template_name[128] = {0};
    char subject[512] = {0};
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int ok = 0;
    const char *slug = target && !strncmp(target, "/w/", 3) ? target + 3 : NULL;
    char requested_grammar[64] = {0}, from[256] = {0}, stream[256] = {0}, q[256] = {0};
    if (!slug || !path) return 0;
    query_value(query, "from", from, sizeof(from));
    query_value(query, "s", stream, sizeof(stream));
    query_value(query, "q", q, sizeof(q));
    {
        const char *slash = strchr(slug, '/');
        if (slash) {
            size_t n = (size_t)(slash - slug);
            if (!n || n >= sizeof(world) || !studio_safe_slug_arg(slash + 1)) return 0;
            memcpy(world, slug, n); world[n] = '\0';
            snprintf(requested_grammar, sizeof(requested_grammar), "%s", slash + 1);
            if (!world_fragment_route_published(world, requested_grammar)) return 0;
        } else {
            if (!studio_safe_slug_arg(slug)) return 0;
            snprintf(world, sizeof(world), "%s", slug);
        }
    }
    if (!(db = cms_open_ro()) ||
        sqlite3_prepare_v2(db,
          "SELECT title,character,lede,sources,COALESCE(grammars,''),cannot_infer,COALESCE(template,''),COALESCE(subject,'') "
          "FROM world WHERE world=?1 AND status='published' ORDER BY id DESC LIMIT 1",
          -1, &st, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_text(st, 1, world, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) goto out;
#define WWCOPY(dst,col) snprintf(dst, sizeof(dst), "%s", sqlite3_column_text(st,col) ? (const char *)sqlite3_column_text(st,col) : "")
    WWCOPY(title, 0); WWCOPY(family, 1); WWCOPY(lede, 2); WWCOPY(sources, 3); WWCOPY(grammars, 4); WWCOPY(cannot, 5); WWCOPY(template_name, 6);
    WWCOPY(subject, 7);
#undef WWCOPY
    sqlite3_finalize(st); st = NULL;
    if (!*title || !studio_safe_slug_arg(family)) goto out;
    /* A published World may truthfully have no admitted sources yet. That is
     * an empty native page, not permission to fall through to the retired
     * browser renderer. Nonempty declarations still require exact resolution. */
    if (*sources && !world_resolve_sources(db, sources, resolved_sources, sizeof(resolved_sources))) goto out;
    snprintf(sources, sizeof(sources), "%s", resolved_sources);
    {
        PageDocument doc = {title, lede, family, template_name, cannot, sources, world, requested_grammar, target, NULL, grammars,
                            bf_page_world_regions, BF_PAGE_WORLD_REGION_COUNT, from, stream, q, NULL, NULL, target, NULL, NULL, 0, query, subject};
        ok = page_emit_document(db, path, &doc, NULL);
    }
out:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    return ok;
}


static int page_csv_has(const char *csv, const char *item) {
    size_t len = item ? strlen(item) : 0;
    const char *p = csv;
    while (p && *p && len) {
        const char *comma = strchr(p, ',');
        size_t n = comma ? (size_t)(comma - p) : strlen(p);
        if (n == len && !strncmp(p, item, len)) return 1;
        p = comma ? comma + 1 : NULL;
    }
    return 0;
}


static int page_territory_apex(const char *host, const char *territory) {
    char apex[64], www[64]; size_t n;
    if (!host || !territory) return 0;
    n = strcspn(host, ":");   /* a Host header may carry a port */
    snprintf(apex, sizeof(apex), "aurekai.%s", territory);
    snprintf(www, sizeof(www), "www.aurekai.%s", territory);
    return (n == strlen(apex) && !strncasecmp(host, apex, n)) || (n == strlen(www) && !strncasecmp(host, www, n));
}


/* Whether a host is a territory's own apex rather than a subject standing in
 * that territory.  The territory home is composed from everything the estate
 * reads and must keep its apex ahead of any SitePresentation naming the same
 * name; a subject host -- an institution, a member -- is the opposite case,
 * where the published program for that subject is the whole point of the
 * address.  Only the shape is decided here: aurekai.<territory> and its www
 * alias are apexes, anything standing in front of them is a subject. */
static int host_is_territory_apex(const char *host) {
    const char *dot; size_t n;
    if (!host || !*host) return 0;
    n = strcspn(host, ":");
    if (n > 8 && !strncasecmp(host, "aurekai.", 8)) return 1;
    if (n > 12 && !strncasecmp(host, "www.aurekai.", 12)) return 1;
    dot = memchr(host, '.', n);
    return dot == NULL;   /* a bare name is no subject's address either */
}

static int page_session_actor(const char *bearer, const char *auth_db, char *actor, size_t cap);
static const char *page_request_bearer, *page_request_auth_db;

static int emit_page_program_route_uncached(const char *root, const char *target, const char *host, const char *query);

/* The anonymous page cache must see the exact SubjectProjection generation it
 * consumes. Global archive mtimes do not record a route replacement, and a
 * changed route may otherwise render from yesterday's public cache for a full
 * TTL. This is a read-only derived key, never a replacement for CMS authority
 * or a FactDelta. Absent/legacy projections retain the existing TTL fallback. */
static int page_program_compiled_revision(const char *host, const char *target, char revision[65]) {
    sqlite3 *cms = NULL, *fabric = NULL;
    sqlite3_stmt *st = NULL;
    char subject[384], route[256];
    const char *digest = NULL, *compiler = NULL, *tail;
    BfSha256 sha;
    unsigned char bytes[32];
    size_t n;
    int found = 0;
    if (!host || !*host || !target || !*target ||
        (n = strlen(target)) >= sizeof(route)) return 0;
    memcpy(route, target, n + 1);
    /* /discover/2 is the same source closure as /discover?page=2. */
    tail = strrchr(route, '/');
    if (tail && tail != route && tail[1] >= '1' && tail[1] <= '9' &&
        strspn(tail + 1, "0123456789") == strlen(tail + 1))
        route[tail - route] = '\0';
    if (!(cms = cms_open_ro()) ||
        !page_subject_for_host(cms, host, subject, sizeof(subject)) ||
        !(fabric = page_subject_fabric_open()) ||
        sqlite3_prepare_v2(fabric,
          "SELECT r.route_digest,b.compiler_sha256 FROM subject_route r "
          "JOIN subject_projection_build b ON b.build_id=1 "
          "WHERE r.subject=?1 AND r.scope='public' AND r.route=?2 LIMIT 1",
          -1, &st, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_text(st, 1, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, route, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        digest = (const char *)sqlite3_column_text(st, 0);
        compiler = (const char *)sqlite3_column_text(st, 1);
        if (digest && compiler && strlen(digest) == 64 && strlen(compiler) == 64) {
            bf_sha256_init(&sha);
            bf_sha256_update(&sha, (const uint8_t *)subject, strlen(subject));
            bf_sha256_update(&sha, (const uint8_t *)"\0", 1);
            bf_sha256_update(&sha, (const uint8_t *)route, strlen(route));
            bf_sha256_update(&sha, (const uint8_t *)"\0", 1);
            bf_sha256_update(&sha, (const uint8_t *)digest, 64);
            bf_sha256_update(&sha, (const uint8_t *)compiler, 64);
            bf_sha256_final(&sha, bytes);
            bf_sha256_digest_hex(bytes, revision);
            found = 1;
        }
    }
out:
    if (st) sqlite3_finalize(st);
    if (fabric) sqlite3_close(fabric);
    if (cms) sqlite3_close(cms);
    return found;
}
typedef struct { const char *root, *target, *host, *query; } BfPageRouteEmit;
static int emit_page_route_body(void *opaque) {
    const BfPageRouteEmit *r = opaque;
    return emit_page_program_route_uncached(r->root, r->target, r->host, r->query);
}

/* A composed page costs what its sources cost: every admitted channel's
 * retained body is read and parsed to find the few items the page shows, and
 * an estate that reads hundreds of channels pays that on every request.  The
 * composition is the same for every anonymous visitor within the same few
 * minutes, so it is composed once and kept.
 *
 * Kept only where it is safe to: no query (a search, a page two or a filter is
 * that person's), no signed-in reader (a page carries what they saved), and
 * keyed by the material's own state, so a page changes when its material
 * does. The clock is part of the key at ten-minute granularity, which is finer
 * than the coarsest thing a page says about time. */
static int emit_page_program_route(const char *root, const char *target, const char *host, const char *query) {
    BfPageRouteEmit emit = {root, target, host, query};
    char key[512], fingerprint[65], actor[64] = {0};
    static const char *const dependencies[] = {
        "cms:internet_source", "foreign:retained-bodies", "frontier:channel", "surface:page-composition",
        "cms:community_body", "cms:community_post"
    };
    const char *cache = public_projection_cache_db();
    {
        /* A page number is not a person: ?page=3 of a route is the same for
         * everyone and is kept like the route's own front page.  Anything else
         * in the query is that person's and is composed for them alone. */
        char only_page[16] = "";
        int query_is_just_a_page = 0;
        if (query && *query) {
            query_value(query, "page", only_page, sizeof(only_page));
            query_is_just_a_page = only_page[0] && !strchr(query, '&') && !strncmp(query, "page=", 5);
        }
        if (!cache || (query && *query && !query_is_just_a_page) ||
            page_session_actor(page_request_bearer, page_request_auth_db, actor, sizeof(actor)))
            return emit_page_route_body(&emit);
    }
    {
        /* The material's own state, the clock to ten minutes, and the host the
         * page is answering as. */
        char clock[32]; struct tm tm; time_t now = time(NULL);
        page_local_time(now, &tm);
        snprintf(clock, sizeof(clock), "%04d%02d%02d-%02d%02d", tm.tm_year + 1900, tm.tm_mon + 1,
                 tm.tm_mday, tm.tm_hour, tm.tm_min / 10);
        /* The build is part of the page: a page composed by an older binary
         * is not this binary's page, however recent its clock. */
        if (snprintf(key, sizeof(key), "public-page|%s|%s|%s|%s|%s %s", host ? host : "", target ? target : "",
                     query && *query ? query : "", clock, __DATE__, __TIME__) >= (int)sizeof(key))
            return emit_page_route_body(&emit);
        public_projection_fingerprint(fingerprint, getenv("BONFYRE_SOURCE_FRONTIER_DB"),
                                      getenv("BONFYRE_ITEM_ARCHIVE_DB"), clock);
        /* Read current owner standing even while SQLite keeps changes in its
         * WAL. The large editorial body stays reusable until this exact
         * public selection or its visible words change. */
        if(front_community_route(host,target)) {
            char revision[65];front_community_revision(revision);
            if(!*revision)return emit_page_route_body(&emit);
            BfSha256 sha;unsigned char digest[32];bf_sha256_init(&sha);
            bf_sha256_update(&sha,(const uint8_t *)fingerprint,64);
            bf_sha256_update(&sha,(const uint8_t *)revision,64);
            bf_sha256_final(&sha,digest);bf_sha256_digest_hex(digest,fingerprint);
        }
        /* Exact route material can change while neither CMS nor the archive
         * mtime moves. Carry the committed, per-subject route revision in the
         * public key, without caching a private/actor-specific projection. */
        {
            char revision[65];
            if (page_program_compiled_revision(host, target, revision)) {
                BfSha256 sha;
                unsigned char bytes[32];
                bf_sha256_init(&sha);
                bf_sha256_update(&sha, (const uint8_t *)fingerprint, 64);
                bf_sha256_update(&sha, (const uint8_t *)revision, 64);
                bf_sha256_final(&sha, bytes);
                bf_sha256_digest_hex(bytes, fingerprint);
            }
        }
    }
    return public_projection_emit(emit_page_route_body, &emit, key, fingerprint,
                                  dependencies, sizeof(dependencies) / sizeof(dependencies[0]));
}

static int emit_page_program_route_uncached(const char *root, const char *target, const char *host, const char *query) {
    const BfPageProgram *program = NULL; size_t i;
    BfPageProgram focused_program;
    int territory_apex_page = 1;   /* until a subject host says otherwise */
    char path[PATH_MAX], sources[4096] = {0}, from[256] = {0}, stream[256] = {0}, q[256] = {0};
    char walked_route[64] = {0}; long walked_page = 0;
    const char *territory = NULL;
    PageMaterial *prefilled = NULL;
    PageProjectionRead projection = {0};
    unsigned char compiled_regions[32];
    int projected = -1, compiled_region_count = -1, stage_prefilled = 0;
    sqlite3 *db = NULL; int ok = 0;
    /* A further length of a route is a path, not a question: /discover/2 is
     * the same page reading publishers it has not shown yet.  A path can be
     * linked, bookmarked, crawled and served by anything that serves files,
     * which a query string cannot. */
    {
        const char *tail = target ? strrchr(target, '/') : NULL;
        if (tail && tail[1] >= '1' && tail[1] <= '9') {
            const char *digit = tail + 1;
            while (*digit >= '0' && *digit <= '9') digit++;
            if (!*digit && (size_t)(tail - target) < sizeof(walked_route)) {
                if (tail == target) snprintf(walked_route, sizeof(walked_route), "/");
                else {
                    memcpy(walked_route, target, (size_t)(tail - target));
                    walked_route[tail - target] = '\0';
                }
                walked_page = atol(tail + 1);
                target = walked_route;
            }
        }
    }
    for (i = 0; target && !program && i < BF_PAGE_PROGRAM_COUNT; i++) {
        const BfPageProgram *candidate = &bf_page_programs[i];
        if (strcmp(target, candidate->route)) continue;
        if (!strcmp(candidate->route, "/")) {
            /* The front page of a territory, and also the front page of an
             * institution standing in it: a host that resolves to a subject
             * with a public bundle is that subject's own address, and it has
             * to carry that subject's material rather than the territory's. */
            territory_apex_page = page_territory_apex(host, candidate->territory);
            if (!territory_apex_page) {
                int is_a_subject = 0;
                if (!strcmp(candidate->territory, territory_of(host)) && (db || (db = cms_open_ro())))
                    is_a_subject = page_subject_for_host(db, host, projection.subject, sizeof(projection.subject));
                if (!is_a_subject) continue;
            }
            territory = candidate->territory;
        } else if (strcmp(candidate->territory, "any") && strcmp(candidate->territory, territory_of(host))) continue;
        program = candidate;
    }
    {
        /* How far down this route's publishers this page starts.  Always set,
         * never inherited: it is a global the composer reads, and a request
         * that left it alone would be composed with the last request's depth. */
        char page[16] = "";
        long asked = walked_page ? walked_page
                   : ((query && *query && query_value(query, "page", page, sizeof(page))) ? atol(page) : 0);
        if (asked < 1) asked = 1;
        if (asked > 400) asked = 400;        /* a walk, not a way to ask for the whole estate at once */
        page_source_offset = program ? (unsigned)((asked - 1) * (long)program->source_limit) : 0;
    }
    if (!program || !root || snprintf(path, sizeof(path), "%s/surfaces/world.html", root) >= (int)sizeof(path)) {
        if (db) sqlite3_close(db);
        return 0;
    }
    if (query && *query) {
        query_value(query, "from", from, sizeof(from));
        query_value(query, "s", stream, sizeof(stream));
        query_value(query, "q", q, sizeof(q));

    }
    /* Directories own a maintained relational cut below.  They share the
     * document shell, but they neither select a program's source reservoir
     * nor resolve a subject fabric. Doing either first makes one source-bound
     * credit page parse the whole estate only to discard it. */
    page_render_host = host;
    if (!strcmp(program->route, "/people") || !strcmp(program->route, "/organisations")) {
        PageDocument doc;
        if (!db && !(db = cms_open_ro())) return 0;
        doc = (PageDocument){program->title, program->lede, program->character, "",
                             program->cannot_infer, "", NULL, "", program->route,
                             NULL, NULL, program->regions, program->region_count,
                             from, stream, q, program, NULL, program->route,
                             NULL, NULL, 0, query, NULL};
        ok = page_emit_document(db, path, &doc, NULL);
        sqlite3_close(db);
        return ok;
    }
    if (!db && !(db = cms_open_ro())) return 0;
    page_render_host = host;
    prefilled = calloc(1, sizeof(*prefilled));
    if (prefilled) {
        page_projection_read_open(&projection, db, host);
        projected = page_subject_projection_material(&projection, program->route, prefilled);
    }
    if (projected >= 0) {
        sources[0] = '\0';
        compiled_region_count = page_subject_projection_regions(
            &projection, program->route, compiled_regions, sizeof(compiled_regions));
    } else {
        free(prefilled);
        prefilled = NULL;
    }
    /* A subject's address carries that subject's material or it carries
     * nothing.  The territory's own reading is the right fallback for the
     * territory's own front page; standing it under a publisher's or a
     * member's name would make the address say this estate has read them when
     * it has not.  Fifteen addressed publishers had no bundle at all and were
     * showing the territory's night page under their own names. */
    if (projected < 0 && page_subject_fabric_sources(projection.db, projection.subject, program, program->source_limit, sources, sizeof(sources)) < 0
        && territory_apex_page)
        page_program_sources(db, program, program->source_limit, sources, sizeof(sources));
    if (!strcmp(program->route, "/stage") && !strncmp(q, "item:", 5)) {
        if (!prefilled) {
            prefilled = calloc(1, sizeof(*prefilled));
            if (prefilled) page_collect_stories(db, sources, prefilled);
        }
        if (prefilled) {
            if (!page_stage_focus(prefilled, q + 5)) {
                int s;
                for (s = 0; s < BF_PAGE_MATERIAL_COUNT; s++)
                    prefilled->stream[s].count = prefilled->stream[s].cursor = 0;
            }
            stage_prefilled = 1;
            q[0] = '\0';
            projected = 0;
            /* The agenda, by name: a region's position in the declared table
             * moves whenever a rhythm is added, and a number pinned here
             * silently pointed at whatever had taken its place. */
            compiled_region_count = 0;
            {
                size_t rr;
                for (rr = 0; rr < BF_PAGE_REGION_COUNT; rr++)
                    if (!strcmp(bf_page_regions[rr].role, "happening")) {
                        compiled_regions[compiled_region_count++] = (unsigned char)rr;
                        break;
                    }
                for (rr = 0; rr < BF_PAGE_REGION_COUNT && compiled_region_count < 2; rr++)
                    if (!strcmp(bf_page_regions[rr].role, "stage")) {
                        compiled_regions[compiled_region_count++] = (unsigned char)rr;
                        break;
                    }
            }
        }
    }
    if (stage_prefilled) {
        sources[0] = '\0';
        focused_program = *program;
        focused_program.sections = NULL;
        focused_program.section_count = 0;
        program = &focused_program;
    }
    /* An institution's own address is not the territory's front page.
     *
     * A territory front page is deliberately one source and one lead: it gets
     * its depth from the sections beneath it, and those compose the whole
     * territory. Standing on a publisher's own host that is exactly wrong --
     * the sections show everyone else's material and the page itself shows one
     * item. So the program is refocused on the subject: every rhythm it can
     * fill from that publisher's own channels, and no sections carrying the
     * territory in over the top of it. */
    if (!territory_apex_page && program && !strcmp(program->route, "/")) {
        size_t r;
        focused_program = *program;
        focused_program.sections = NULL;
        focused_program.section_count = 0;
        if (focused_program.source_limit < 48) focused_program.source_limit = 48;
        /* Every rhythm the estate has, in its declared order: what the
         * publisher actually carries decides which of them appear, because a
         * region with nothing to show is skipped. */
        static unsigned char every_rhythm[BF_PAGE_REGION_COUNT];
        static size_t every_rhythm_count;
        if (!every_rhythm_count)
            for (r = 0; r < BF_PAGE_REGION_COUNT; r++)
                every_rhythm[every_rhythm_count++] = (unsigned char)r;
        focused_program.regions = every_rhythm;
        focused_program.region_count = every_rhythm_count;
        program = &focused_program;
    }
    {
        char actor[64] = {0}, subject_masthead[160] = {0};
        PageProjectionContract projection_contract;
        int have_projection_contract = projected >= 0 &&
            page_subject_projection_contract(&projection, program->route, &projection_contract);
        const char *title = have_projection_contract && projection_contract.title[0]
                          ? projection_contract.title : program->title;
        const char *lede = have_projection_contract && projection_contract.lede[0]
                         ? projection_contract.lede : program->lede;
        const char *character = have_projection_contract && projection_contract.character[0]
                              ? projection_contract.character : program->character;
        const char *cannot = have_projection_contract && projection_contract.cannot[0]
                           ? projection_contract.cannot : program->cannot_infer;
        const BfPageHomePhase *phase = NULL;
        PageDocument doc;
        if (territory) {
            /* A front page is a different page at eight in the morning and at
             * midnight, because the day is.  The phase is read from this
             * estate's own clock; it renames and reorders what the territory
             * already publishes and can admit nothing of its own. */
            struct tm tm; size_t p;
            page_local_time(time(NULL), &tm);
            for (p = 0; p < BF_PAGE_HOME_PHASE_COUNT; p++) {
                const BfPageHomePhase *candidate = &bf_page_home_phases[p];
                if (strcmp(candidate->territory, territory)) continue;
                if ((unsigned)tm.tm_hour >= candidate->from_hour && (!phase || candidate->from_hour >= phase->from_hour)) phase = candidate;
            }
            /* Before the first phase of the day, the last phase of the night still holds. */
            if (!phase) for (p = 0; p < BF_PAGE_HOME_PHASE_COUNT; p++) {
                const BfPageHomePhase *candidate = &bf_page_home_phases[p];
                if (!strcmp(candidate->territory, territory) && (!phase || candidate->from_hour > phase->from_hour)) phase = candidate;
            }
            if (phase) { title = phase->title; lede = phase->lede; }
            /* The hour's rhythm belongs to the territory; the name does not.
             * A publisher's address headed "Still open" tells a reader, a tab
             * and a crawler that they are looking at the territory's night
             * page, when they are looking at what this estate read from that
             * publisher.  The phase still orders the page; the subject names
             * it. */
            if (!territory_apex_page && host && *host) {
                snprintf(subject_masthead, sizeof(subject_masthead), "%.*s",
                         (int)strcspn(host, ":"), host);
                title = subject_masthead;
                lede = "";
            }
        }
        doc = (PageDocument){title, lede, character, "", cannot, sources,
                            NULL, "", program->route, territory, NULL, program->regions, program->region_count,
                            from, stream, q, program, NULL, program->route, NULL, NULL, 0, query, NULL};
        doc.subject_address=projection.subject[0]?projection.subject:NULL;
        if (projected >= 0) {
            doc.prefilled = prefilled;
            doc.sequence = compiled_regions;
            doc.sequence_count = compiled_region_count > 0 ? (size_t)compiled_region_count : 0;
        }
        /* A publisher's own address is everything this estate keeps from it:
         * the current slot the projection holds, every earlier piece in the
         * archive, the organizations its pieces name and every way it
         * publishes.  The scene compiler orders all of it, and the page is
         * headed with the publisher's name, not this estate's host for it. */
        /* The archive is read per reader below, so a publisher whose compiled
         * route holds no items still has its own material: since the 19
         * October recompile every publisher's "/" cut was empty, and all of
         * them published as a bare heading with their host for a name. */
        if (!territory_apex_page && !prefilled && projection.db && !strncmp(projection.subject, "institution:publisher:", 22)
            && (prefilled = calloc(1, sizeof(*prefilled))) != NULL) {
            doc.prefilled = prefilled;
            doc.sequence = NULL;
            doc.sequence_count = 0;
        }
        if (!territory_apex_page && prefilled && projection.db && !strncmp(projection.subject, "institution:publisher:", 22)) {
            static unsigned char publisher_scene[BF_PAGE_SCENE_GRAMMAR_COUNT];
            static char publisher_title[256];
            char readers[2048] = "", why[256] = "";
            sqlite3_stmt *rs = NULL;
            PageMaterial *kept = calloc(1, sizeof(*kept));
            EstateReferenceStores stores = {0}; EstateReference self;
            size_t count;
            if (sqlite3_prepare_v2(projection.db, "SELECT DISTINCT census_source FROM subject_reader WHERE subject=?1 AND scope='public' LIMIT 12", -1, &rs, NULL) == SQLITE_OK) {
                sqlite3_bind_text(rs, 1, projection.subject, -1, SQLITE_STATIC);
                while (sqlite3_step(rs) == SQLITE_ROW) {
                    size_t used = strlen(readers);
                    const char *one = (const char *)sqlite3_column_text(rs, 0);
                    if (one && used + strlen(one) + 2 < sizeof(readers)) {
                        snprintf(readers + used, sizeof(readers) - used, "%s%s", used ? "," : "", one);
                        if (kept) page_collect_archive_source(one, kept);
                    }
                }
            }
            sqlite3_finalize(rs);
            /* The archive's copy of a story the projection already holds is
             * folded into it, never shown twice. */
            if (kept) { page_merge_material(prefilled, kept); free(kept); }
            if (readers[0]) {
                page_finalize_material(prefilled);
                estate_reference_open(&stores);
                if (estate_reference_resolve(&stores, readers, &self) || self.name[0]) {
                    char first[96]; size_t n = strcspn(readers, ",");
                    snprintf(first, sizeof(first), "%.*s", (int)n, readers);
                    estate_reference_resolve(&stores, first, &self);
                }
                if (self.name[0]) { snprintf(publisher_title, sizeof(publisher_title), "%s", self.name); doc.title = publisher_title; }
                else {
                    /* No reference names the publisher: the source record's
                     * own label does, as the reading's MCP card already says. */
                    sqlite3_stmt *ls = NULL; char first[160]; size_t n = strcspn(readers, ",");
                    snprintf(first, sizeof(first), "%.*s", (int)n, readers);
                    if (sqlite3_prepare_v2(db, "SELECT label FROM internet_source WHERE source=?1 AND COALESCE(label,'')<>'' LIMIT 1", -1, &ls, NULL) == SQLITE_OK) {
                        sqlite3_bind_text(ls, 1, first, -1, SQLITE_TRANSIENT);
                        if (sqlite3_step(ls) == SQLITE_ROW) {
                            snprintf(publisher_title, sizeof(publisher_title), "%s", (const char *)sqlite3_column_text(ls, 0));
                            doc.title = publisher_title;
                        }
                    }
                    sqlite3_finalize(ls);
                }
                depth_publisher_relations(readers, self.name[0] ? self.name : doc.title, &stores, prefilled);
                estate_reference_close(&stores);
                page_scene_in_depth = 1;
                count = page_compile_scene(prefilled, publisher_scene, sizeof(publisher_scene), why, sizeof(why), time(NULL));
                page_scene_in_depth = 0;
                if (count) { doc.sequence = publisher_scene; doc.sequence_count = count; }
            }
        }
        if (phase) { doc.phase_sections = phase->sections; doc.phase_section_count = phase->section_count; }
        if (page_session_actor(page_request_bearer, page_request_auth_db, actor, sizeof(actor))) doc.actor = actor;
        ok = page_emit_document(db, path, &doc, &projection);
        free(prefilled);
        page_render_host = NULL;
    }
    page_projection_read_close(&projection);
    sqlite3_close(db);
    return ok;
}

/* ---------------------------------------------------------------- item page
 *
 * /o/<source>/<key> is one thing, opened.  Its verbs are the ones people
 * already know for that kind of thing -- play, view, read, see on a map, share,
 * save, more from whoever made it -- each a link or a form that works without
 * a script.  The item is found again in its source's retained material by the
 * same key the page that linked to it computed. */
static int page_peertube_embed(const char *href, char *out, size_t cap) {
    const char *host, *slash, *marker; size_t host_len;
    out[0] = '\0';
    if (!public_http_url(href) || strncmp(href, "https://", 8)) return 0;
    host = href + 8; slash = strchr(host, '/'); if (!slash) return 0;
    host_len = (size_t)(slash - host);
    if ((marker = strstr(slash, "/videos/watch/")) != NULL) marker += strlen("/videos/watch/");
    else if (!strncmp(slash, "/w/", 3)) marker = slash + 3;
    else return 0;
    if (!*marker || strspn(marker, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-") != strlen(marker)) return 0;
    return snprintf(out, cap, "https://%.*s/videos/embed/%s", (int)host_len, host, marker) < (int)cap;
}

static int page_words_shared(const char *a, const char *b) {
    char words[24][32]; int count = 0, shared = 0, i; const char *p = a;
    while (p && *p && count < 24) {
        size_t n;
        while (*p && !isalnum((unsigned char)*p)) p++;
        n = strspn(p, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        if (n >= 5 && n < sizeof(words[0])) { memcpy(words[count], p, n); words[count][n] = '\0'; count++; }
        p += n ? n : (*p ? 1 : 0);
    }
    for (i = 0; i < count; i++) if (page_contains_ci(b, words[i])) shared++;
    return shared;
}

/* The estate's own clock.  A time is shown in the zone the place it is about
 * keeps, not in UTC: an event at noon in Fort Worth must not read as five in
 * the evening.  The zone is declared in the environment so a different estate
 * reads its own hours. */
static struct tm *page_local_time(time_t at, struct tm *tm) {
    /* The estate's zone is set once, not around every time a page writes. */
    static char current[64] = "";
    const char *zone = getenv("BONFYRE_LOCAL_ZONE");
    if (!zone || !*zone) zone = "America/Chicago";
    if (strcmp(current, zone)) {
        snprintf(current, sizeof(current), "%s", zone);
        setenv("TZ", zone, 1);
        tzset();
    }
    localtime_r(&at, tm);
    return tm;
}

/* ---- what phase an event is in --------------------------------------------
 *
 * The same address is a different thing at different hours: something to plan
 * for, something happening now, something that has just finished.  The phase
 * is read from the organizer's own start and end, never from a guess, and an
 * event with no end is given the rest of its day rather than an invented
 * length. */
typedef enum { PAGE_EVENT_ANNOUNCED, PAGE_EVENT_THIS_WEEK, PAGE_EVENT_TODAY,
               PAGE_EVENT_SOON, PAGE_EVENT_ON_NOW, PAGE_EVENT_JUST_ENDED, PAGE_EVENT_PAST } PageEventPhase;

static PageEventPhase page_event_phase(const PageItem *item, time_t now, time_t *ends_out) {
    time_t ends;
    if (!item || item->at <= 0) return PAGE_EVENT_PAST;
    ends = item->duration_ms > 0 ? item->at + item->duration_ms / 1000 : item->at + 86400;
    if (ends_out) *ends_out = ends;
    if (now >= ends) return now - ends < 6 * 3600 ? PAGE_EVENT_JUST_ENDED : PAGE_EVENT_PAST;
    if (now >= item->at) return PAGE_EVENT_ON_NOW;
    if (item->at - now <= 2 * 3600) return PAGE_EVENT_SOON;
    if (item->at - now <= 20 * 3600) return PAGE_EVENT_TODAY;
    if (item->at - now <= 7 * 86400) return PAGE_EVENT_THIS_WEEK;
    return PAGE_EVENT_ANNOUNCED;
}

static const char *page_event_phase_words(PageEventPhase phase) {
    switch (phase) {
    case PAGE_EVENT_ON_NOW: return "Happening now";
    case PAGE_EVENT_SOON: return "Starting soon";
    case PAGE_EVENT_TODAY: return "Later today";
    case PAGE_EVENT_THIS_WEEK: return "This week";
    case PAGE_EVENT_JUST_ENDED: return "Just finished";
    case PAGE_EVENT_PAST: return "This already happened";
    default: return "Coming up";
    }
}

static void page_item_action(const char *href, const char *glyph, const char *label, int external, int primary) {
    fputs(primary ? "<a class=\"verb primary\" href=\"" : "<a class=\"verb\" href=\"", stdout); html(href); fputs("\"", stdout);
    if (external) fputs(" rel=\"noopener\" target=\"_blank\"", stdout);
    fputs(">", stdout); page_glyph(glyph); fputs("<span>", stdout); html(label); fputs("</span></a>", stdout);
}

/* ------------------------------------------------------------- what is around
 *
 * Several of this estate's sources say where a thing is: an organizer's
 * calendar carries GEO, a Commons photograph carries the point it was taken
 * from, a place carries its own coordinates, a wildlife record carries where
 * it was seen.  Until now each of those reached a different region of a
 * different page and nothing met anything else.
 *
 * A placed thing can therefore carry its own surroundings: what else this
 * estate holds within a short walk, in whatever form it takes, nearest first,
 * with the distance said in the words a person would use.  Distance is
 * measured, not implied: it is the great-circle distance between two stated
 * points, and it says nothing about whether the two things are related.
 */
static double page_km_between(double lat_a, double lon_a, double lat_b, double lon_b) {
    const double radius = 6371.0088, rad = 3.14159265358979323846 / 180.0;
    double dlat = (lat_b - lat_a) * rad, dlon = (lon_b - lon_a) * rad;
    double a = sin(dlat / 2) * sin(dlat / 2) +
               cos(lat_a * rad) * cos(lat_b * rad) * sin(dlon / 2) * sin(dlon / 2);
    return 2 * radius * atan2(sqrt(a), sqrt(1 - a));
}

static void page_distance_words(double km, char *out, size_t cap) {
    int minutes = (int)(km / 0.08 + 0.5);
    if (km < 0.06) snprintf(out, cap, "right here");
    else if (km < 1.4) snprintf(out, cap, minutes <= 1 ? "a minute's walk" : "%d minutes' walk", minutes);
    else if (km < 10) snprintf(out, cap, "%.1f km away", km);
    else snprintf(out, cap, "%d km away", (int)(km + 0.5));
}

/* The program whose families are the placed ones, so "around here" draws on
 * every kind of placed material this estate admits rather than one source. */
static const BfPageProgram *page_placed_program(void) {
    size_t i;
    for (i = 0; i < BF_PAGE_PROGRAM_COUNT; i++)
        if (!strcmp(bf_page_programs[i].route, "/nearby")) return &bf_page_programs[i];
    return NULL;
}

static sqlite3 *page_archive_open(int writable);

/* The neighbourhood this estate already computed, on the vGPU, for every
 * placed thing it holds -- not just the material one route happens to name.
 * Where it exists a page reads it; where it does not, the page still measures
 * what it can reach. */
static int page_emit_around_precomputed(const char *source, const char *key, const char *not_ident, time_t now) {
    sqlite3 *archive = page_archive_open(0); sqlite3_stmt *st = NULL; char address[224]; int shown = 0;
    if (!archive || !source || !key) return 0;
    snprintf(address, sizeof(address), "o/%s/%s", source, key);
    if (sqlite3_prepare_v2(archive,
            "SELECT n.near_address, n.metres, i.title, i.kind, i.href, i.image, i.by, i.publisher, i.at, i.ident"
            " FROM neighbour n JOIN item i ON ('o/' || i.source || '/' || i.key) = n.near_address"
            " WHERE n.address=?1 ORDER BY n.rank LIMIT 8", -1, &st, NULL) != SQLITE_OK) { sqlite3_close(archive); return 0; }
    sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *near = (const char *)sqlite3_column_text(st, 0);
        double metres = sqlite3_column_double(st, 1);
        const char *title = (const char *)sqlite3_column_text(st, 2);
        const char *kind = (const char *)sqlite3_column_text(st, 3);
        const char *href = (const char *)sqlite3_column_text(st, 4);
        const char *image = (const char *)sqlite3_column_text(st, 5);
        const char *by = (const char *)sqlite3_column_text(st, 6);
        time_t at = (time_t)sqlite3_column_int64(st, 8);
        const char *ident = (const char *)sqlite3_column_text(st, 9);
        char words[48]; const char *slash;
        if (!title || !*title) continue;
        if (not_ident && *not_ident && ident && !strcmp(ident, not_ident)) continue;
        if (!shown++) fputs("<section class=\"region region-around\"><h2 class=\"region-label\">Around here</h2><ol class=\"more around\">", stdout);
        page_distance_words(metres / 1000.0, words, sizeof(words));
        fputs(image && page_image_url_safe(image) ? "<li class=\"with-picture\">" : "<li>", stdout);
        slash = near && !strncmp(near, "o/", 2) ? strchr(near + 2, '/') : NULL;
        if (slash) { fputs("<a href=\"/", stdout); html(near); fputs("\">", stdout); }
        else if (href && public_http_url(href)) { fputs("<a href=\"", stdout); html(href); fputs("\" rel=\"noopener\">", stdout); }
        html(title);
        if (slash || (href && public_http_url(href))) fputs("</a>", stdout);
        if (image && page_image_url_safe(image)) { char sized[1024]; page_picture_at(image, 240, sized, sizeof(sized)); page_emit_picture(sized, "thumb"); }
        fputs("<span class=\"by\"><span class=\"how-far\">", stdout); html(words); fputs("</span>", stdout);
        if (kind && !strcmp(kind, "event") && at > 0) {
            char when[48];
            page_when_ahead(at, at + 86400, now, when, sizeof(when));
            fputs("<span>", stdout); html(when); fputs("</span>", stdout);
        } else if (by && *by) { fputs("<span>", stdout); html(by); fputs("</span>", stdout); }
        fputs("</span></li>", stdout);
    }
    if (shown) fputs("</ol><p class=\"src\">Distance is between the two points these sources state; being near something is not being about it.</p></section>", stdout);
    sqlite3_finalize(st); sqlite3_close(archive);
    return shown;
}

/* The same story, told by someone else.
 *
 * Every census item has a coordinate in this estate's own chart of the words
 * it uses, and the kin kernel compares all of them on the vGPU, once, away
 * from any request.  Well above the noise -- and only across two different
 * publishers -- what that finds is one story two publishers both carried.
 *
 * The page says exactly that and no more: two texts closely resemble each
 * other. It is not a shared source, a syndication agreement anyone has
 * confirmed, corroboration of anything either states, or a claim that either
 * publisher was first.  Where the estate has computed no chart, the page
 * simply does not say it. */
static int page_emit_kin(const char *source, const char *key, const char *not_ident) {
    sqlite3 *archive = page_archive_open(0); sqlite3_stmt *st = NULL; char address[224]; int shown = 0;
    if (!archive || !source || !key) return 0;
    snprintf(address, sizeof(address), "o/%s/%s", source, key);
    if (sqlite3_prepare_v2(archive,
            "SELECT k.kin_address, i.title, i.kind, i.image, i.by, i.publisher, i.ident, i.source"
            " FROM kin k JOIN item i ON ('o/' || i.source || '/' || i.key) = k.kin_address"
            " WHERE k.address=?1 AND i.source<>?2 ORDER BY k.rank LIMIT 6", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(archive); return 0;
    }
    sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, source, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *kin = (const char *)sqlite3_column_text(st, 0);
        const char *title = (const char *)sqlite3_column_text(st, 1);
        const char *image = (const char *)sqlite3_column_text(st, 3);
        const char *by = (const char *)sqlite3_column_text(st, 4);
        const char *publisher = (const char *)sqlite3_column_text(st, 5);
        const char *ident = (const char *)sqlite3_column_text(st, 6);
        if (!title || !*title || !kin) continue;
        if (not_ident && *not_ident && ident && !strcmp(ident, not_ident)) continue;
        if (!shown++) fputs("<section class=\"region region-kin\"><h2 class=\"region-label\">Also reported by</h2><ol class=\"more kin\">", stdout);
        fputs(image && page_image_url_safe(image) ? "<li class=\"with-picture\">" : "<li>", stdout);
        fputs("<a href=\"/", stdout); html(kin); fputs("\">", stdout); html(title); fputs("</a>", stdout);
        if (image && page_image_url_safe(image)) { char sized[1024]; page_picture_at(image, 240, sized, sizeof(sized)); page_emit_picture(sized, "thumb"); }
        fputs("<span class=\"by\">", stdout);
        if (publisher && *publisher) { fputs("<span>", stdout); html(publisher); fputs("</span>", stdout); }
        else if (by && *by) { fputs("<span>", stdout); html(by); fputs("</span>", stdout); }
        fputs("</span></li>", stdout);
    }
    if (shown)
        fputs("</ol><p class=\"src\">These texts closely resemble this one in this estate's own chart of the words "
              "they use. That is a measurement of two texts, not a shared source, not confirmation of what either "
              "says, and no claim about who published first.</p></section>", stdout);
    sqlite3_finalize(st); sqlite3_close(archive);
    return shown;
}

static int page_emit_around(sqlite3 *db, double lat, double lon, const char *not_source, const char *not_key,
                            const char *not_ident, double radius_km, time_t now) {
    const BfPageProgram *program = page_placed_program();
    PageMaterial *material; char sources[4096];
    struct { const PageItem *item; double km; } near[24];
    int found = 0, k, m, shown = 0;
    if (!program || lat == 0.0 || lon == 0.0 || !(material = calloc(1, sizeof(*material)))) return 0;
    page_program_sources(db, program, program->source_limit, sources, sizeof(sources));
    page_collect_stories(db, sources, material);
    for (m = 0; m < BF_PAGE_MATERIAL_COUNT; m++) {
        if (m == BF_PAGE_MATERIAL_MIX) continue;
        for (k = 0; k < material->stream[m].count; k++) {
            const PageItem *item = &material->stream[m].item[k];
            double km;
            int at;
            if (!item->placed || !item->title[0]) continue;
            if (not_source && !strcmp(item->source, not_source) && not_key && !strcmp(item->key, not_key)) continue;
            if (not_ident && *not_ident && (!strcmp(item->ident, not_ident) || !strcmp(item->about, not_ident))) continue;
            km = page_km_between(lat, lon, item->lat, item->lon);
            if (km > radius_km) continue;
            for (at = 0; at < found; at++)
                if (near[at].item && !strcmp(near[at].item->title, item->title)) break;
            if (at < found) continue;               /* one thing once, however often it repeats */
            /* kept in order of nearness, the furthest falling off the end */
            for (at = 0; at < found; at++) if (near[at].km > km) break;
            if (found < (int)(sizeof(near) / sizeof(near[0]))) found++;
            else if (at >= found) continue;
            {
                int move;
                for (move = found - 1; move > at; move--) near[move] = near[move - 1];
                near[at].item = item; near[at].km = km;
            }
        }
    }
    for (k = 0; k < found && shown < 8; k++) {
        const PageItem *item = near[k].item; char words[48]; const char *close;
        if (!item) continue;
        if (!shown++) fputs("<section class=\"region region-around\"><h2 class=\"region-label\">Around here</h2><ol class=\"more around\">", stdout);
        page_distance_words(near[k].km, words, sizeof(words));
        fputs(item->image[0] ? "<li class=\"with-picture\">" : "<li>", stdout);
        close = page_emit_item_open(item, NULL);
        html(item->title);
        fputs(close, stdout);
        if (item->image[0]) { char sized[1024]; page_picture_at(item->image, 240, sized, sizeof(sized)); page_emit_picture(sized, "thumb"); }
        fputs("<span class=\"by\"><span class=\"how-far\">", stdout); html(words); fputs("</span>", stdout);
        if (item->kind == BF_PAGE_MATERIAL_EVENT && item->at > 0) {
            char when[48];
            page_when_ahead(item->at, item->duration_ms > 0 ? item->at + item->duration_ms / 1000 : item->at + 86400, now, when, sizeof(when));
            fputs("<span>", stdout); html(when); fputs("</span>", stdout);
        } else page_emit_by(item, now);
        fputs("</span></li>", stdout);
    }
    if (shown) fputs("</ol><p class=\"src\">Distance is between the two points these sources state; being near something is not being about it.</p></section>", stdout);
    free(material);
    return shown;
}

/* ---------------------------------------------------------- the item archive
 *
 * A feed holds a page of recent items; a calendar holds a year.  What either
 * one dropped last week is not thereby untrue, and a person who was sent its
 * address should not meet an empty page.  So every item the estate reads is
 * kept here once, with when it was first and last seen.
 *
 * An archived item is what this estate read at a time, not a claim that its
 * source still publishes it: the page says when it was read, keeps the
 * original link, and never presents it as current.
 */
#include "item_credit_indexes.h"
static sqlite3 *page_archive_open(int writable) {
    const char *path = getenv("BONFYRE_ITEM_ARCHIVE_DB");
    sqlite3 *db = NULL;
    if (!path || !*path) return NULL;
    if (writable) {
        if (sqlite3_open(path, &db) != SQLITE_OK) { sqlite3_close(db); return NULL; }
        sqlite3_busy_timeout(db, 5000);
        if (sqlite3_exec(db, "PRAGMA journal_mode=DELETE;"
            "CREATE TABLE IF NOT EXISTS item(source TEXT NOT NULL, key TEXT NOT NULL, kind TEXT NOT NULL, family TEXT DEFAULT '',"
            " title TEXT NOT NULL, body TEXT DEFAULT '', href TEXT DEFAULT '', image TEXT DEFAULT '', by TEXT DEFAULT '',"
            " publisher TEXT DEFAULT '', ident TEXT DEFAULT '', at INTEGER DEFAULT 0, first_read TEXT NOT NULL, last_read TEXT NOT NULL,"
            " PRIMARY KEY(source, key));"
            "CREATE INDEX IF NOT EXISTS item_by_source ON item(source, at DESC);"
            "CREATE INDEX IF NOT EXISTS idx_item_credit_by_source_at ON item(by, source, at DESC);"
            BF_ITEM_CREDIT_INDEXES
            "CREATE INDEX IF NOT EXISTS item_by_ident ON item(ident);", NULL, NULL, NULL) != SQLITE_OK) { sqlite3_close(db); return NULL; }
        return db;
    }
    return bf_sqlite_open_readonly_path(path, 1500);
}

static int page_archive_item(sqlite3 *archive, const char *source, const char *key, PageItem *item, char *family, size_t family_cap) {
    sqlite3_stmt *st = NULL; int found = 0;
    if (!archive) return 0;
    if (sqlite3_prepare_v2(archive, "SELECT kind,family,title,body,href,image,by,publisher,ident,at,first_read FROM item"
                                    " WHERE source=?1 AND key=?2", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, key, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *kind = (const char *)sqlite3_column_text(st, 0);
        memset(item, 0, sizeof(*item));
        snprintf(item->source, sizeof(item->source), "%.*s", (int)sizeof(item->source) - 1, source);
        snprintf(item->key, sizeof(item->key), "%.*s", (int)sizeof(item->key) - 1, key);
        if (family && family_cap) snprintf(family, family_cap, "%s", (const char *)sqlite3_column_text(st, 1));
        snprintf(item->title, sizeof(item->title), "%s", sqlite3_column_text(st, 2));
        snprintf(item->body, sizeof(item->body), "%s", sqlite3_column_text(st, 3));
        snprintf(item->href, sizeof(item->href), "%s", sqlite3_column_text(st, 4));
        snprintf(item->image, sizeof(item->image), "%s", sqlite3_column_text(st, 5));
        snprintf(item->by, sizeof(item->by), "%s", sqlite3_column_text(st, 6));
        snprintf(item->note, sizeof(item->note), "%s", sqlite3_column_text(st, 7));
        snprintf(item->ident, sizeof(item->ident), "%s", sqlite3_column_text(st, 8));
        item->at = (time_t)sqlite3_column_int64(st, 9);
        item->kind = kind && !strcmp(kind, "event") ? BF_PAGE_MATERIAL_EVENT
                   : kind && !strcmp(kind, "photo") ? BF_PAGE_MATERIAL_PHOTO
                   : kind && !strcmp(kind, "video") ? BF_PAGE_MATERIAL_VIDEO
                   : kind && !strcmp(kind, "audio") ? BF_PAGE_MATERIAL_AUDIO
                   : BF_PAGE_MATERIAL_PUBLICATION;
        found = 1;
    }
    sqlite3_finalize(st);
    return found;
}

/* A source body is a present-tense convenience; the archive is the record of
 * what the estate already read.  This fallback is deliberately narrow: it
 * runs only when the live retained-body projector produced no material for
 * that source, and it carries only fields the archive itself retained. */
static int page_collect_archive_source(const char *source, PageMaterial *material) {
    sqlite3 *archive = page_archive_open(0);
    sqlite3_stmt *st = NULL;
    int loaded = 0;
    if (!archive || !source || !*source || !material) { if (archive) sqlite3_close(archive); return 0; }
    if (sqlite3_prepare_v2(archive,
          "SELECT key,kind,title,body,href,image,by,publisher,ident,at FROM item "
          "WHERE source=?1 ORDER BY at DESC,first_read DESC LIMIT ?2",
          -1, &st, NULL) != SQLITE_OK) { sqlite3_close(archive); return 0; }
    sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, page_per_source > 0 ? page_per_source : 1);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *kind_name = (const char *)sqlite3_column_text(st, 1);
        const char *title = (const char *)sqlite3_column_text(st, 2);
        int kind = kind_name && !strcmp(kind_name, "event") ? BF_PAGE_MATERIAL_EVENT
                 : kind_name && !strcmp(kind_name, "photo") ? BF_PAGE_MATERIAL_PHOTO
                 : kind_name && !strcmp(kind_name, "video") ? BF_PAGE_MATERIAL_VIDEO
                 : kind_name && !strcmp(kind_name, "audio") ? BF_PAGE_MATERIAL_AUDIO
                 : BF_PAGE_MATERIAL_PUBLICATION;
        PageItem *item = page_push(&material->stream[kind], title);
        if (!item) continue;
        snprintf(item->source, sizeof(item->source), "%s", source);
        snprintf(item->key, sizeof(item->key), "%s", (const char *)sqlite3_column_text(st, 0));
        snprintf(item->body, sizeof(item->body), "%s", (const char *)sqlite3_column_text(st, 3));
        snprintf(item->href, sizeof(item->href), "%s", (const char *)sqlite3_column_text(st, 4));
        snprintf(item->image, sizeof(item->image), "%s", (const char *)sqlite3_column_text(st, 5));
        snprintf(item->by, sizeof(item->by), "%s", (const char *)sqlite3_column_text(st, 6));
        snprintf(item->note, sizeof(item->note), "%s", (const char *)sqlite3_column_text(st, 7));
        snprintf(item->ident, sizeof(item->ident), "%s", (const char *)sqlite3_column_text(st, 8));
        item->at = (time_t)sqlite3_column_int64(st, 9);
        item->kind = kind;
        /* This row is the durable record of something read earlier. It fills a
         * page a thin source would leave empty, but it is not evidence that the
         * source still publishes it, and nothing downstream may read it as
         * current without saying so. */
        item->archived = 1;
        loaded++;
    }
    sqlite3_finalize(st);
    sqlite3_close(archive);
    return loaded;
}

/* The identity graph, read where a page is composed.
 *
 * bonfyre-entity holds what this estate has resolved: a name it kept seeing,
 * the kind of thing it turned out to be, the register's identifier for it, and
 * where on earth it is when it is anywhere. That graph existed and no composed
 * page had ever read it, so a reader met a headline naming Fort Worth and the
 * estate said nothing about Fort Worth.
 *
 * What is shown is what the graph observed, with the source of each
 * observation. A title naming an entity is a mention: it is not a claim that
 * the piece is about that entity, and nothing here resolves that difference.
 */
static sqlite3 *page_entity_open(void) {
    const char *path = getenv("BONFYRE_ENTITY_DB");
    return bf_sqlite_open_readonly_path(path, 1000);
}

/* One observation of a given modal, most confident first. */
static int page_entity_observation(sqlite3 *db, const char *eid, const char *modal,
                                   char *out, size_t cap, char *from, size_t from_cap) {
    sqlite3_stmt *st = NULL; int got = 0;
    if (sqlite3_prepare_v2(db,
            "SELECT value, COALESCE(source,'') FROM observations"
            " WHERE entity=?1 AND modal=?2 ORDER BY confidence DESC, ts DESC LIMIT 1",
            -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, eid, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, modal, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0));
        if (from) snprintf(from, from_cap, "%s", (const char *)sqlite3_column_text(st, 1));
        got = 1;
    }
    sqlite3_finalize(st);
    return got;
}

static int page_emit_entities(const char *title) {
    sqlite3 *db; sqlite3_stmt *st = NULL; int shown = 0;
    char flat[1024]; size_t fi = 0;
    if (!title || !*title) return 0;
    /* A name is matched between word boundaries, and a comma is a word
     * boundary. Matching on spaces alone missed "Fort Worth, Texas" entirely
     * and reported the "Fort" inside it instead, which is an object in a
     * register somewhere and not what the headline said. Punctuation becomes
     * space so the boundary is the boundary. */
    for (const char *p = title; *p && fi + 1 < sizeof(flat); p++)
        flat[fi++] = (isalnum((unsigned char)*p) || (unsigned char)*p >= 0x80) ? *p : ' ';
    flat[fi] = '\0';
    if (!(db = page_entity_open())) return 0;
    /* The longest name that fits wins, so "Fort Worth" is not also reported as
     * "Worth", and a name is matched whole rather than inside a longer word. */
    if (sqlite3_prepare_v2(db,
            "SELECT id, display, kind, obs_count FROM entities"
            " WHERE instr(lower(' ' || ?1 || ' '), lower(' ' || display || ' ')) > 0"
            " ORDER BY length(display) DESC LIMIT 6", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db); return 0;
    }
    sqlite3_bind_text(st, 1, flat, -1, SQLITE_STATIC);
    /* Longest wins, and it has to be enforced rather than only ordered for.
     * "Fort Worth" contains "Worth", which is also a real place with its own
     * Q-number in Germany; ordering by length put the right answer first and
     * then printed the wrong two underneath it, so a Texas headline claimed to
     * name a Rhineland village. A candidate already covered by an accepted one
     * is the same words read again, not a second thing. */
    char taken[6][128]; int ntaken = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *eid = (const char *)sqlite3_column_text(st, 0);
        const char *display = (const char *)sqlite3_column_text(st, 1);
        const char *kind = (const char *)sqlite3_column_text(st, 2);
        int covered = 0, t;
        for (t = 0; t < ntaken && !covered; t++)
            if (strstr(taken[t], display)) covered = 1;
        if (covered) continue;
        if (ntaken < (int)(sizeof(taken) / sizeof(taken[0])))
            snprintf(taken[ntaken++], sizeof(taken[0]), "%s", display);
        char tag[96] = "", tag_from[128] = "", where[96] = "", where_from[128] = "";
        int has_tag = page_entity_observation(db, eid, "tag", tag, sizeof(tag),
                                              tag_from, sizeof(tag_from));
        int has_where = page_entity_observation(db, eid, "location", where, sizeof(where),
                                                where_from, sizeof(where_from));
        if (!shown++)
            fputs("<section class=\"region names\"><h2 class=\"region-label\">What this names</h2>"
                  "<ol class=\"more\">", stdout);
        fputs("<li><strong>", stdout); html(display); fputs("</strong> — ", stdout);
        html(kind);
        fputs("<span class=\"by\">", stdout);
        if (has_tag) {
            fputs("<span><a href=\"https://www.wikidata.org/wiki/", stdout); html(tag);
            fputs("\" rel=\"noopener noreferrer\" target=\"_blank\">", stdout); html(tag);
            fputs("</a></span>", stdout);
        }
        if (has_where) { fputs("<span>", stdout); html(where); fputs("</span>", stdout); }
        if (has_tag && tag_from[0]) { fputs("<span>", stdout); html(tag_from); fputs("</span>", stdout); }
        else if (has_where && where_from[0]) { fputs("<span>", stdout); html(where_from); fputs("</span>", stdout); }
        fputs("</span></li>", stdout);
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    if (shown)
        fputs("</ol></section>", stdout);
    return shown;
}

/* What this source published before whatever it is publishing now. */
static int page_emit_archive_earlier(const char *source, const char *except_key, const char *heading, int limit, time_t now) {
    sqlite3 *archive = page_archive_open(0); sqlite3_stmt *st = NULL; int shown = 0;
    if (!archive) return 0;
    if (sqlite3_prepare_v2(archive, "SELECT key,title,at,first_read FROM item WHERE source=?1 AND key<>?2"
                                    " ORDER BY at DESC, first_read DESC LIMIT ?3", -1, &st, NULL) != SQLITE_OK) { sqlite3_close(archive); return 0; }
    sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, except_key ? except_key : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 3, limit);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *key = (const char *)sqlite3_column_text(st, 0);
        const char *title = (const char *)sqlite3_column_text(st, 1);
        time_t at = (time_t)sqlite3_column_int64(st, 2);
        char when[48] = "";
        if (!title || !*title) continue;
        if (!shown++) {
            fputs("<section class=\"region region-earlier\"><h2 class=\"region-label\">", stdout); html(heading);
            fputs("</h2><ol class=\"more\">", stdout);
        }
        fputs("<li><a href=\"/o/", stdout); page_url_escape(source); fputs("/", stdout); html(key); fputs("\">", stdout);
        html(title); fputs("</a>", stdout);
        if (at > 0) page_when(at, now, when, sizeof(when));
        if (when[0]) { fputs("<span class=\"by\"><span>", stdout); html(when); fputs("</span></span>", stdout); }
        fputs("</li>", stdout);
    }
    if (shown) fputs("</ol></section>", stdout);
    sqlite3_finalize(st); sqlite3_close(archive);
    return shown;
}

/* ------------------------------------------------------------- saved items
 *
 * Save is a person's private, reversible reference to something they chose to
 * keep, owned by the saved_item content type and written only through the CMS
 * binary.  It needs a signed-in person, it is refused from another site, and
 * it keeps the public description the source gave so a kept thing outlives
 * its source's latest feed slot without becoming a copy or a claim. */
typedef struct {
    long id;
    char item_ref[160], title[768], href[1024], image[1024], publisher[256], maker[256], summary[1024], saved_at[64];
} PageSaved;

static int page_session_actor(const char *bearer, const char *auth_db, char *actor, size_t cap) {
    char out[2048]; const char *id; long uid;
    actor[0] = '\0';
    if (!bearer || !*bearer || !auth_db || !*auth_db || !valid_token(bearer)) return 0;
    {
        char *const argv[] = {(char *)auth_binary(), "--db", (char *)auth_db, "verify", "--token", (char *)bearer, NULL};
        if (auth_call(argv, NULL, out, sizeof(out)) != 0 || !strstr(out, "\"valid\":true") || !(id = strstr(out, "\"id\":"))) return 0;
    }
    uid = strtol(id + 5, NULL, 10);
    if (uid <= 0) return 0;
    snprintf(actor, cap, "user:%ld", uid);
    return 1;
}

static int page_saved_get(sqlite3 *db, const char *actor, const char *item_ref, PageSaved *out) {
    sqlite3_stmt *st = NULL; int found = 0;
    memset(out, 0, sizeof(*out));
    if (!db || !actor || !*actor || !cms_type_registered(db, "saved_item") ||
        sqlite3_prepare_v2(db, "SELECT id,item_ref,title,COALESCE(href,''),COALESCE(image,''),COALESCE(publisher,''),COALESCE(maker,''),"
          "COALESCE(summary,''),saved_at FROM saved_item WHERE actor=?1 AND item_ref=?2 AND status='published' ORDER BY id DESC LIMIT 1",
          -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, actor, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, item_ref, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        found = 1;
        out->id = (long)sqlite3_column_int64(st, 0);
#define SCOPY(f,c) snprintf(out->f, sizeof(out->f), "%s", sqlite3_column_text(st,c) ? (const char *)sqlite3_column_text(st,c) : "")
        SCOPY(item_ref,1); SCOPY(title,2); SCOPY(href,3); SCOPY(image,4); SCOPY(publisher,5); SCOPY(maker,6); SCOPY(summary,7); SCOPY(saved_at,8);
#undef SCOPY
    }
    sqlite3_finalize(st);
    return found;
}

static void page_item_from_saved(const PageSaved *saved, PageItem *item) {
    const char *slash = strchr(saved->item_ref, '/');
    memset(item, 0, sizeof(*item));
    snprintf(item->title, sizeof(item->title), "%s", saved->title);
    snprintf(item->by, sizeof(item->by), "%s", saved->maker[0] ? saved->maker : saved->publisher);
    snprintf(item->note, sizeof(item->note), "%.*s", (int)sizeof(item->note) - 1, saved->publisher);
    snprintf(item->body, sizeof(item->body), "%s", saved->summary);
    if (public_http_url(saved->href) || !strncmp(saved->href, "/s/", 3)) snprintf(item->href, sizeof(item->href), "%s", saved->href);
    if (page_image_url_safe(saved->image)) snprintf(item->image, sizeof(item->image), "%s", saved->image);
    if (slash && strncmp(saved->item_ref, "parcel/", 7) && (size_t)(slash - saved->item_ref) < sizeof(item->source) && strlen(slash + 1) < sizeof(item->key)) {
        snprintf(item->source, sizeof(item->source), "%.*s", (int)(slash - saved->item_ref), saved->item_ref);
        snprintf(item->key, sizeof(item->key), "%s", slash + 1);
    }
    item->kind = page_image_url_safe(item->image) ? BF_PAGE_MATERIAL_PHOTO : BF_PAGE_MATERIAL_PUBLICATION;
    item->at = page_time(saved->saved_at);
}

static const char *page_cms_env(const char *name, const char *fallback) {
    const char *v = getenv(name);
    return v && *v ? v : fallback;
}

static void page_json_field(char *out, size_t cap, size_t *used, const char *key, const char *value, int last) {
    /* A profile introduction or post can contain 6,000 UTF-8 bytes.
     * JSON escaping may expand each byte; never truncate its saved text. */
    char escaped[40000];
    json_escape_into(value ? value : "", escaped, sizeof(escaped));
    if (*used < cap) *used += (size_t)snprintf(out + *used, cap - *used, "\"%s\":\"%s\"%s", key, escaped, last ? "" : ",");
    if (*used >= cap) *used = cap - 1;
}

static void page_redirect(const char *location) {
    printf("HTTP/1.1 303 See Other\r\nLocation: %s\r\nCache-Control: no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", location);
}

static int serve_page_save(const char *method, const char *target, const char *body, const char *raw,
                           const char *host, const char *bearer, const char *auth_db) {
    char item[200] = {0}, back[400] = {0}, actor[64], ref_source[128], ref_key[32], next[900];
    const char *slash; int unsave; sqlite3 *db = NULL; PageSaved saved;
    if (!target || (strcmp(target, "/o/save") && strcmp(target, "/o/unsave"))) return 0;
    unsave = !strcmp(target, "/o/unsave");
    if (strcmp(method, "POST")) { protocol_reply(405, "text/plain; charset=utf-8", "Save is a form submission.\n"); return 1; }
    /* Saving is only ever asked for from this site. */
    {
        const char *origin = raw ? strstr(raw, "\nOrigin:") : NULL;
        if (!origin && raw) origin = strstr(raw, "\norigin:");
        if (origin) {
            char value[300] = {0}, expect_https[300], expect_http[300];
            sscanf(origin + 8, " %299[^\r\n]", value);
            snprintf(expect_https, sizeof(expect_https), "https://%s", host ? host : "");
            snprintf(expect_http, sizeof(expect_http), "http://%s", host ? host : "");
            if (strcmp(value, expect_https) && strcmp(value, expect_http) && strcmp(value, "null")) {
                protocol_reply(403, "text/plain; charset=utf-8", "Saving is only accepted from this site.\n"); return 1;
            }
        }
    }
    int parcel_ref = body && query_value(body, "item", item, sizeof(item)) && !strncmp(item, "parcel/", 7) &&
                     strlen(item + 7) >= 1 && strlen(item + 7) <= 16 && strspn(item + 7, "0123456789") == strlen(item + 7);
    if (!parcel_ref && (!body || !query_value(body, "item", item, sizeof(item)) || !(slash = strchr(item, '/')) ||
        (size_t)(slash - item) >= sizeof(ref_source) || strlen(slash + 1) != 24 || strspn(slash + 1, "0123456789abcdef") != 24 ||
        strspn(item, "abcdefghijklmnopqrstuvwxyz0123456789-_./") != strlen(item))) {
        protocol_reply(400, "text/plain; charset=utf-8", "That is not something that can be saved.\n"); return 1;
    }
    slash = strchr(item, '/');
    snprintf(ref_source, sizeof(ref_source), "%.*s", (int)(slash - item), item);
    snprintf(ref_key, sizeof(ref_key), "%.*s", (int)sizeof(ref_key) - 1, slash + 1);
    if (!query_value(body, "return", back, sizeof(back)) || back[0] != '/' || back[1] == '/' || strpbrk(back, "\r\n\\"))
        snprintf(back, sizeof(back), parcel_ref ? "/s/%s" : "/o/%s", item);
    if (!page_session_actor(bearer, auth_db, actor, sizeof(actor))) {
        snprintf(next, sizeof(next), "/login?next=%s", back);
        page_redirect(next); return 1;
    }
    {
        const char *cms_bin = page_cms_env("BONFYRE_CMS_BINARY", "/usr/local/bin/bonfyre-cms");
        const char *cms_db = page_cms_env("BONFYRE_CMS_DB", "/var/lib/bonfyre/cms/bonfyre_cms.db");
        const char *schemas = page_cms_env("BONFYRE_SAVED_ITEM_SCHEMAS", "/var/lib/bonfyre/cms/saved-item-type");
        char out[4096];
        PageItem projected;
        int projection_state = 0;
        db = cms_open_ro();
        if (!unsave && !parcel_ref && db) {
            projection_state = page_subject_projection_item(db, host, ref_source, ref_key,
                                                           &projected, NULL, 0);
            if (projection_state < 0) {
                sqlite3_close(db);
                protocol_reply(404, "text/plain; charset=utf-8", "Nothing here names that item.\n");
                return 1;
            }
        }
        if (unsave) {
            if (db && page_saved_get(db, actor, item, &saved)) {
                char id_text[32];
                snprintf(id_text, sizeof(id_text), "%ld", saved.id);
                {
                    char *const argv[] = {(char *)cms_bin, "entry", "delete", "saved_item", id_text, "--db", (char *)cms_db, "--schemas", (char *)schemas, NULL};
                    auth_call(argv, NULL, out, sizeof(out));
                }
            }
        } else if (!db || !page_saved_get(db, actor, item, &saved)) {
            PageMaterial *material = calloc(1, sizeof(*material));
            const PageItem *found = NULL; int k;
            if (material && parcel_ref) {
                /* A property is kept by what the observed roll says about it now. */
                char sql[1200], rows[8192], address[256] = {0}, owner[256] = {0}, total[64] = {0};
                const char *bindir = page_cms_env("BONFYRE_BIN_DIR", "/usr/local/bin");
                snprintf(sql, sizeof(sql),
                  "SELECT json_extract_string(observed,'$.Situs_Addr') AS address, json_extract_string(observed,'$.Owner_Name') AS owner, "
                  "trim(json_extract_string(observed,'$.Total_Valu')) AS total FROM observation WHERE src_id='src:tad-parcel-view' "
                  "AND trim(json_extract_string(observed,'$.Account_Nu'))='%s' LIMIT 1", ref_key);
                if (fleming_query(bindir, sql, rows, sizeof(rows)) == 0 && rows[0] == '[' && strlen(rows) > 4) {
                    PageItem *p = &material->stream[BF_PAGE_MATERIAL_MIX].item[0];
                    studio_json_field(rows, "address", address, sizeof(address));
                    studio_json_field(rows, "owner", owner, sizeof(owner));
                    studio_json_field(rows, "total", total, sizeof(total));
                    memset(p, 0, sizeof(*p));
                    page_title_case(address[0] ? address : "A Tarrant County property", p->title, sizeof(p->title));
                    snprintf(p->note, sizeof(p->note), "Tarrant Appraisal District");
                    page_title_case(owner, p->by, sizeof(p->by));
                    snprintf(p->href, sizeof(p->href), "/s/parcel/%s", ref_key);
                    if (total[0]) snprintf(p->body, sizeof(p->body), "Assessed at $%s on the observed roll.", total);
                    material->stream[BF_PAGE_MATERIAL_MIX].count = 1;
                    found = p;
                }
            } else if (material && db) {
                if (projection_state > 0) found = &projected;
                else {
                    page_collect_stories(db, ref_source, material);
                    for (k = 0; !found && k < material->stream[BF_PAGE_MATERIAL_MIX].count; k++)
                        if (!strcmp(material->stream[BF_PAGE_MATERIAL_MIX].item[k].key, ref_key)) found = &material->stream[BF_PAGE_MATERIAL_MIX].item[k];
                }
            }
            if (found) {
                char json[16384], seed[300], digest[65], observed[40] = {0}, now_text[40] = {0};
                size_t used = 0; time_t now = time(NULL); struct tm tm;
                snprintf(seed, sizeof(seed), "%s\n%s", actor, item);
                bf_sha256_hex((const uint8_t *)seed, strlen(seed), digest); digest[32] = '\0';
                if (found->at > 0 && gmtime_r(&found->at, &tm)) strftime(observed, sizeof(observed), "%Y-%m-%dT%H:%M:%SZ", &tm);
                if (now == (time_t)-1 || !gmtime_r(&now, &tm) ||
                    !strftime(now_text, sizeof(now_text), "%Y-%m-%dT%H:%M:%SZ", &tm)) {
                    free(material);
                    if (db) sqlite3_close(db);
                    studio_reply(503, "application/json", "{\"error\":\"clock_unavailable\"}\n");
                    return 1;
                }
                used += (size_t)snprintf(json, sizeof(json), "{");
                page_json_field(json, sizeof(json), &used, "saved", digest, 0);
                page_json_field(json, sizeof(json), &used, "actor", actor, 0);
                page_json_field(json, sizeof(json), &used, "item_ref", item, 0);
                page_json_field(json, sizeof(json), &used, "title", found->title, 0);
                page_json_field(json, sizeof(json), &used, "href", found->href, 0);
                page_json_field(json, sizeof(json), &used, "image", found->image, 0);
                page_json_field(json, sizeof(json), &used, "publisher", found->note, 0);
                page_json_field(json, sizeof(json), &used, "maker", found->by, 0);
                page_json_field(json, sizeof(json), &used, "summary", found->body, 0);
                page_json_field(json, sizeof(json), &used, "observed_at", observed, 0);
                page_json_field(json, sizeof(json), &used, "saved_at", now_text, 0);
                page_json_field(json, sizeof(json), &used, "collection", "Saved", 0);
                page_json_field(json, sizeof(json), &used, "cannot_infer", "a saved item is a private reference to what a source published; it is not a copy, an endorsement or a claim", 0);
                page_json_field(json, sizeof(json), &used, "status", "published", 1);
                if (used + 2 < sizeof(json)) { json[used++] = '}'; json[used] = '\0'; }
                {
                    char *const argv[] = {(char *)cms_bin, "entry", "create", "saved_item", json, "--ns", "root", "--db", (char *)cms_db, "--schemas", (char *)schemas, NULL};
                    auth_call(argv, NULL, out, sizeof(out));
                }
            }
            free(material);
        }
        if (db) sqlite3_close(db);
    }
    page_redirect(back);
    return 1;
}

static int emit_saved_page(const char *root, const char *host, const char *bearer, const char *auth_db) {
    char actor[64], path[PATH_MAX]; sqlite3 *db = NULL; sqlite3_stmt *st = NULL; PageMaterial *material; size_t r;
    unsigned char sequence[1] = {0}; int ok = 0;
    (void)host;
    if (!page_session_actor(bearer, auth_db, actor, sizeof(actor))) { page_redirect("/login?next=/saved"); return 1; }
    if (!root || snprintf(path, sizeof(path), "%s/surfaces/world.html", root) >= (int)sizeof(path)) return 0;
    for (r = 0; r < BF_PAGE_REGION_COUNT; r++) if (!strcmp(bf_page_regions[r].role, "feed")) sequence[0] = (unsigned char)r;
    if (!(material = calloc(1, sizeof(*material)))) return 0;
    db = cms_open_ro();
    if (db && cms_type_registered(db, "saved_item") &&
        sqlite3_prepare_v2(db, "SELECT id,item_ref,title,COALESCE(href,''),COALESCE(image,''),COALESCE(publisher,''),COALESCE(maker,''),"
          "COALESCE(summary,''),saved_at FROM saved_item WHERE actor=?1 AND status='published' ORDER BY saved_at DESC,id DESC LIMIT ?2",
          -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, actor, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, PAGE_STREAM_MAX);
        while (sqlite3_step(st) == SQLITE_ROW && material->stream[BF_PAGE_MATERIAL_MIX].count < PAGE_STREAM_MAX) {
            PageSaved saved; memset(&saved, 0, sizeof(saved));
#define SCOPY(f,c) snprintf(saved.f, sizeof(saved.f), "%s", sqlite3_column_text(st,c) ? (const char *)sqlite3_column_text(st,c) : "")
            SCOPY(item_ref,1); SCOPY(title,2); SCOPY(href,3); SCOPY(image,4); SCOPY(publisher,5); SCOPY(maker,6); SCOPY(summary,7); SCOPY(saved_at,8);
#undef SCOPY
            page_item_from_saved(&saved, &material->stream[BF_PAGE_MATERIAL_MIX].item[material->stream[BF_PAGE_MATERIAL_MIX].count++]);
        }
        sqlite3_finalize(st);
    }
    {
        PageDocument doc = {"Saved", "What you kept. It stays here after its source moves on, and only you can see it.", "scrapbook", "",
                            "a saved item is a private reference to what a source published; it is not a copy, an endorsement or a claim",
                            "", NULL, "", "/saved", NULL, NULL, sequence, 1, NULL, NULL, NULL, NULL, actor, "/saved", material, NULL, 0, NULL, NULL};
        ok = db ? page_emit_document(db, path, &doc, NULL) : 0;
    }
    if (db) sqlite3_close(db);
    free(material);
    return ok;
}

/* --------------------------------------------------------------- properties
 *
 * A property is a real, continuous subject: its facts come from the observed
 * Tarrant appraisal roll every time it is opened, its address never changes
 * with a feed, and the things a person can do with it are the ones the parcel
 * aperture already offers.  Nothing here is stored except what a person saves. */
static void page_title_case(const char *in, char *out, size_t cap) {
    size_t n = 0; int start = 1, after_comma = 0;
    for (; in && *in && n + 1 < cap; in++) {
        unsigned char c = (unsigned char)*in;
        if (c == ' ' && n && out[n - 1] == ' ') continue;
        /* A two-letter word after a comma is a state ("Fort Worth, TX"). */
        if (after_comma && start && isalpha(c) && isalpha((unsigned char)in[1]) && (!in[2] || in[2] == ',')) {
            out[n++] = (char)toupper(c);
            if (n + 1 < cap) out[n++] = (char)toupper((unsigned char)in[1]);
            in++; start = 0; after_comma = 0; continue;
        }
        if (c == ',') after_comma = 1;
        else if (isalnum(c)) after_comma = 0;
        out[n++] = start ? (char)toupper(c) : (char)tolower(c);
        start = !isalnum(c);
    }
    while (n && out[n - 1] == ' ') n--;
    out[n] = '\0';
}

static int page_money(const char *raw, char *out, size_t cap) {
    char digits[64]; size_t n = 0; const char *p;
    for (p = raw ? raw : ""; *p && n + 1 < sizeof(digits); p++) if (isdigit((unsigned char)*p) || *p == '.') digits[n++] = *p;
    digits[n] = '\0';
    if (!n || atof(digits) <= 0) { out[0] = '\0'; return 0; }
    snprintf(digits, sizeof(digits), "%.0f", atof(digits));
    num_fix(digits, sizeof(digits));
    if (snprintf(out, cap, "$%s", digits) >= (int)cap) { out[0] = '\0'; return 0; }
    return 1;
}

static void page_fact(const char *label, const char *value) {
    if (!value || !*value) return;
    fputs("<div><dt>", stdout); html(label); fputs("</dt><dd>", stdout); html(value); fputs("</dd></div>", stdout);
}

/* Property matches for a search that reads like an address or an account. */
static void page_emit_property_results(const char *q) {
    char term[200], sql[1600], rows[65536]; const char *bindir = page_cms_env("BONFYRE_BIN_DIR", "/usr/local/bin");
    const char *p; int shown = 0;
    if (!q || !strpbrk(q, "0123456789") || !flow_safe_term(q, term, sizeof(term))) return;
    snprintf(sql, sizeof(sql),
      "SELECT trim(json_extract_string(observed,'$.Account_Nu')) AS account, json_extract_string(observed,'$.Situs_Addr') AS address, "
      "json_extract_string(observed,'$.Owner_Name') AS owner, trim(json_extract_string(observed,'$.Total_Valu')) AS total "
      "FROM observation WHERE src_id='src:tad-parcel-view' AND (upper(json_extract_string(observed,'$.Situs_Addr')) LIKE upper('%%%s%%') "
      "OR trim(json_extract_string(observed,'$.Account_Nu'))='%s') LIMIT 12", term, term);
    if (fleming_query(bindir, sql, rows, sizeof(rows)) != 0 || rows[0] != '[') return;
    for (p = rows; (p = strstr(p, "{\"account\"")) != NULL; ) {
        const char *end = strchr(p, '}'); char rec[2048], account[32] = {0}, address[256] = {0}, owner[256] = {0}, total[64] = {0}, pretty[256], money[48];
        size_t n = end ? (size_t)(end - p + 1) : 0;
        if (!n || n >= sizeof(rec)) break;
        memcpy(rec, p, n); rec[n] = '\0'; p = end;
        studio_json_field(rec, "account", account, sizeof(account));
        studio_json_field(rec, "address", address, sizeof(address));
        studio_json_field(rec, "owner", owner, sizeof(owner));
        studio_json_field(rec, "total", total, sizeof(total));
        if (!account[0] || strspn(account, "0123456789") != strlen(account)) continue;
        if (!shown++) fputs("<section class=\"region region-properties\" aria-label=\"Properties\"><h2 class=\"region-label\">Properties</h2><ol class=\"results\">", stdout);
        page_title_case(address[0] ? address : "Address not recorded", pretty, sizeof(pretty));
        fputs("<li><a class=\"result\" href=\"/s/parcel/", stdout); html(account); fputs("\"><span class=\"kind\">", stdout); page_glyph("pin");
        fputs("</span><span class=\"what\"><b>", stdout); html(pretty); fputs("</b><span class=\"dek\">", stdout);
        { char owner_pretty[256]; page_title_case(owner, owner_pretty, sizeof(owner_pretty)); html(owner_pretty); }
        if (page_money(total, money, sizeof(money))) { fputs(" &middot; assessed ", stdout); html(money); }
        fputs("</span><span class=\"by\"><span>Tarrant County property roll</span></span></span></a></li>", stdout);
    }
    if (shown) fputs("</ol></section>", stdout);
}

static int emit_parcel_page(const char *root, const char *target, const char *host, const char *bearer, const char *auth_db) {
    char account[32], path[PATH_MAX], sql[2400], rows[16384], *template = NULL, *head_end, actor[64] = {0};
    const char *bindir = page_cms_env("BONFYRE_BIN_DIR", "/usr/local/bin");
    char address[256] = {0}, owner[256] = {0}, mail[256] = {0}, mail_city[128] = {0}, legal[512] = {0}, land_value[64] = {0}, improvement[64] = {0};
    char total[64] = {0}, year_built[16] = {0}, living[32] = {0}, beds[16] = {0}, baths[16] = {0}, acres[32] = {0}, deed[32] = {0}, appraisal[16] = {0}, use[16] = {0};
    char pretty[256], owner_pretty[256], money[48], self[600], ref[64];
    sqlite3 *db = NULL; PageSaved saved; int is_saved = 0, may_write = 0;
    (void)host;
    if (!root || strncmp(target, "/s/parcel/", 10)) return 0;
    snprintf(account, sizeof(account), "%.*s", (int)sizeof(account) - 1, target + 10);
    if (!account[0] || strlen(account) > 16 || strspn(account, "0123456789") != strlen(account)) return 0;
    if (snprintf(path, sizeof(path), "%s/surfaces/world.html", root) >= (int)sizeof(path) || !page_open_template(path, &template, &head_end)) return 0;
    snprintf(sql, sizeof(sql),
      "SELECT json_extract_string(observed,'$.Situs_Addr') AS address, json_extract_string(observed,'$.Owner_Name') AS owner, "
      "json_extract_string(observed,'$.Owner_Addr') AS mail, json_extract_string(observed,'$.Owner_City') AS mail_city, "
      "json_extract_string(observed,'$.LegalDescr') AS legal, trim(json_extract_string(observed,'$.Land_Value')) AS land_value, "
      "trim(json_extract_string(observed,'$.Improvemen')) AS improvement, trim(json_extract_string(observed,'$.Total_Valu')) AS total, "
      "trim(json_extract_string(observed,'$.Year_Built')) AS year_built, trim(json_extract_string(observed,'$.Living_Are')) AS living, "
      "trim(json_extract_string(observed,'$.Num_Bedroo')) AS beds, trim(json_extract_string(observed,'$.Num_Bathro')) AS baths, "
      "trim(json_extract_string(observed,'$.Land_Acres')) AS acres, trim(json_extract_string(observed,'$.Deed_Date')) AS deed, "
      "trim(json_extract_string(observed,'$.Appraisal_')) AS appraisal, trim(json_extract_string(observed,'$.State_Use_')) AS use "
      "FROM observation WHERE src_id='src:tad-parcel-view' AND trim(json_extract_string(observed,'$.Account_Nu'))='%s' LIMIT 1", account);
    if (fleming_query(bindir, sql, rows, sizeof(rows)) != 0 || rows[0] != '[' || strlen(rows) < 4) {
        free(template);
        return 0;   /* the legacy subject door explains a missing parcel */
    }
#define PF(name, buf) studio_json_field(rows, name, buf, sizeof(buf))
    PF("address", address); PF("owner", owner); PF("mail", mail); PF("mail_city", mail_city); PF("legal", legal);
    PF("land_value", land_value); PF("improvement", improvement); PF("total", total); PF("year_built", year_built);
    PF("living", living); PF("beds", beds); PF("baths", baths); PF("acres", acres); PF("deed", deed); PF("appraisal", appraisal); PF("use", use);
#undef PF
    page_title_case(address[0] ? address : "Address not recorded", pretty, sizeof(pretty));
    page_title_case(owner, owner_pretty, sizeof(owner_pretty));
    snprintf(ref, sizeof(ref), "parcel/%s", account);
    if (page_session_actor(bearer, auth_db, actor, sizeof(actor))) {
        db = cms_open_ro();
        is_saved = db && page_saved_get(db, actor, ref, &saved);
        may_write = session_authorized(bearer, auth_db, "write");
    }
    snprintf(self, sizeof(self), "https://%s%s", host && *host ? host : "aurekai.work", target);
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: %s\r\nVary: Host, Accept, Cookie\r\nConnection: close\r\n\r\n",
           actor[0] ? "private, no-store" : "public, max-age=300");
    {
        char description[400];
        snprintf(description, sizeof(description), "%s in Tarrant County, as the appraisal roll records it.", pretty);
        page_emit_head(template, head_end, pretty, description);
    }
    fputs("</head><body class=\"atlas item-page route-property\" data-character=\"atlas\"><main class=\"wrap\">", stdout);
    {
        PageDocument nav = {0};
        nav.route = "/parcels"; nav.actor = actor[0] ? actor : NULL; nav.self_path = target;
        page_emit_nav(&nav);
    }
    fputs("<p class=\"crumbs\"><a href=\"/search\">Properties</a> &middot; Tarrant County</p><article class=\"thing property\"><div class=\"thing-text\"><h1>", stdout);
    html(pretty); fputs("</h1><p class=\"by\"><span>Tarrant Appraisal District roll", stdout);
    if (appraisal[0]) { fputs(", ", stdout); html(appraisal); }
    fputs("</span><span class=\"via\">account ", stdout); html(account); fputs("</span></p>", stdout);
    fputs("<nav class=\"verbs\" aria-label=\"Actions\">", stdout);
    {
        char market[200];
        snprintf(market, sizeof(market), "/property/market?account=%s", account);
        page_item_action(market, "list", "See this market", 0, 1);
    }
    if (actor[0]) {
        fputs(is_saved ? "<form class=\"verb-form\" method=\"post\" action=\"/o/unsave\">" : "<form class=\"verb-form\" method=\"post\" action=\"/o/save\">", stdout);
        fputs("<input type=\"hidden\" name=\"item\" value=\"", stdout); html(ref);
        fputs("\"><input type=\"hidden\" name=\"return\" value=\"", stdout); html(target);
        fputs(is_saved ? "\"><button class=\"verb is-on\" type=\"submit\" aria-pressed=\"true\">" : "\"><button class=\"verb\" type=\"submit\" aria-pressed=\"false\">", stdout);
        page_glyph("save"); fputs(is_saved ? "<span>Saved</span>" : "<span>Save</span>", stdout); fputs("</button></form>", stdout);
        if (may_write) {
            /* The parcel aperture's own work verbs, offered only with write scope. */
            char watch[200];
            snprintf(watch, sizeof(watch), "/property?watch=%s", account);
            page_item_action(watch, "pin", "Watch this property", 0, 0);
            page_item_action(watch, "out", "Open an opportunity", 0, 0);
        }
    } else {
        char login[200];
        snprintf(login, sizeof(login), "/login?next=%s", target);
        page_item_action(login, "save", "Sign in to save", 0, 0);
    }
    {
        char share[1400]; int w = snprintf(share, sizeof(share), "mailto:?subject=");
        const unsigned char *p;
        for (p = (const unsigned char *)pretty; *p && w < (int)sizeof(share) - 8; p++) w += isalnum(*p) ? snprintf(share + w, sizeof(share) - (size_t)w, "%c", *p) : snprintf(share + w, sizeof(share) - (size_t)w, "%%%02X", *p);
        w += snprintf(share + w, sizeof(share) - (size_t)w, "&body=");
        for (p = (const unsigned char *)self; *p && w < (int)sizeof(share) - 8; p++) w += isalnum(*p) || strchr("-._~", *p) ? snprintf(share + w, sizeof(share) - (size_t)w, "%c", *p) : snprintf(share + w, sizeof(share) - (size_t)w, "%%%02X", *p);
        page_item_action(share, "share", "Share", 0, 0);
    }
    fputs("</nav><p class=\"link-line\"><label>Link <input readonly value=\"", stdout); html(self); fputs("\"></label></p></div>", stdout);
    fputs("<dl class=\"facts\">", stdout);
    page_fact("Owner of record", owner_pretty);
    if (mail[0]) { char m[400], mp[400]; snprintf(m, sizeof(m), "%s, %s", mail, mail_city); page_title_case(m, mp, sizeof(mp)); page_fact("Mailing address", mp); }
    page_fact("Assessed total", page_money(total, money, sizeof(money)) ? money : "");
    page_fact("Land", page_money(land_value, money, sizeof(money)) ? money : "");
    page_fact("Buildings", page_money(improvement, money, sizeof(money)) ? money : "");
    if (atoi(year_built) > 1800) page_fact("Built", year_built);
    if (atoi(living) > 0) { char v[64]; snprintf(v, sizeof(v), "%d sq ft", atoi(living)); num_fix(v, sizeof(v)); page_fact("Living area", v); }
    if (atoi(beds) > 0 || atoi(baths) > 0) { char v[64]; snprintf(v, sizeof(v), "%d bed, %d bath", atoi(beds), atoi(baths)); page_fact("Rooms", v); }
    if (atof(acres) > 0) { char v[64]; snprintf(v, sizeof(v), "%.2f acres", atof(acres)); page_fact("Lot", v); }
    if (deed[0] && strcmp(deed, "12-31-1900")) page_fact("Deed recorded", deed);
    { char lp[512]; page_title_case(legal, lp, sizeof(lp)); page_fact("Legal description", lp); }
    fputs("</dl></article>", stdout);
    /* Neighbours on the same street, from the same roll. */
    {
        const char *street = strchr(address, ' ');
        char term[200], neighbours[32768];
        if (street && flow_safe_term(street + 1, term, sizeof(term))) {
            snprintf(sql, sizeof(sql),
              "SELECT trim(json_extract_string(observed,'$.Account_Nu')) AS account, json_extract_string(observed,'$.Situs_Addr') AS address, "
              "trim(json_extract_string(observed,'$.Total_Valu')) AS total, trim(json_extract_string(observed,'$.Year_Built')) AS year_built "
              "FROM observation WHERE src_id='src:tad-parcel-view' AND upper(json_extract_string(observed,'$.Situs_Addr')) LIKE upper('%% %s') "
              "AND trim(json_extract_string(observed,'$.Account_Nu'))<>'%s' LIMIT 10", term, account);
            if (fleming_query(bindir, sql, neighbours, sizeof(neighbours)) == 0 && neighbours[0] == '[' && strlen(neighbours) > 4) {
                const char *p = neighbours; int shown = 0;
                while ((p = strstr(p, "{\"account\"")) != NULL) {
                    const char *end = strchr(p, '}'); char rec[1024], acc[32] = {0}, addr[256] = {0}, tv[64] = {0}, yb[16] = {0}, ap[256];
                    size_t n = end ? (size_t)(end - p + 1) : 0;
                    if (!n || n >= sizeof(rec)) break;
                    memcpy(rec, p, n); rec[n] = '\0'; p = end;
                    studio_json_field(rec, "account", acc, sizeof(acc)); studio_json_field(rec, "address", addr, sizeof(addr));
                    studio_json_field(rec, "total", tv, sizeof(tv)); studio_json_field(rec, "year_built", yb, sizeof(yb));
                    if (!acc[0] || strspn(acc, "0123456789") != strlen(acc)) continue;
                    if (!shown++) {
                        char st[200]; page_title_case(street + 1, st, sizeof(st));
                        fputs("<section class=\"region\"><h2 class=\"region-label\">Also on ", stdout); html(st);
                        fputs("</h2><div class=\"ledger-wrap\"><table class=\"ledger\"><thead><tr><th>Address</th><th>Assessed</th><th>Built</th></tr></thead><tbody>", stdout);
                    }
                    page_title_case(addr, ap, sizeof(ap));
                    fputs("<tr><td class=\"name\"><a href=\"/s/parcel/", stdout); html(acc); fputs("\">", stdout); html(ap); fputs("</a></td><td>", stdout);
                    html(page_money(tv, money, sizeof(money)) ? money : ""); fputs("</td><td class=\"when\">", stdout);
                    if (atoi(yb) > 1800) html(yb);
                    fputs("</td></tr>", stdout);
                }
                if (shown) fputs("</tbody></table></div></section>", stdout);
            }
        }
    }
    fputs("<footer class=\"src\">Facts here are what the Tarrant Appraisal District roll recorded; they are not a survey, a title search or an appraisal.</footer></main></body></html>", stdout);
    if (db) sqlite3_close(db);
    free(template);
    return 1;
}

/* Where a picture or a file is kept, and the terms its source states, as a
 * line a person can follow: "Picture from Wikimedia Commons · CC BY-SA 4.0". */
static void page_emit_credit(const PageItem *item, const char *element) {
    if (!item->origin[0] && !item->rights[0]) return;
    printf("<%s class=\"credit\">", element);
    if (item->origin[0]) {
        fputs(item->asset[0] && item->kind == BF_PAGE_MATERIAL_AUDIO ? "Recording from " : item->asset[0] ? "Video from " : "Picture from ", stdout);
        fputs("<a rel=\"noopener\" target=\"_blank\" href=\"", stdout); html(item->origin_href); fputs("\">", stdout); html(item->origin); fputs("</a>", stdout);
    }
    if (item->rights[0]) {
        if (item->origin[0]) fputs(" &middot; ", stdout);
        if (item->rights_href[0]) { fputs("<a rel=\"license noopener\" target=\"_blank\" href=\"", stdout); html(item->rights_href); fputs("\">", stdout); html(item->rights); fputs("</a>", stdout); }
        else html(item->rights);
    }
    printf("</%s>", element);
}

/* Using a picture someone shared is a thing a person can do, not a question
 * they have to research: the credit line its terms ask for, ready to copy,
 * and the page where the full file and its terms are. */
static void page_emit_reuse(const PageItem *item) {
    char embed[1100], where_name[128] = "", where_href[1024] = "";
    int plays = page_embed_for(item->href, embed, sizeof(embed));
    const char *noun = (item->asset[0] || plays) && item->kind == BF_PAGE_MATERIAL_AUDIO ? "recording" : (item->asset[0] || plays) ? "film" : "picture";
    /* Terms are kept by a commons for its files, or stated by the source for
     * its item; either way the page that states them is linked. */
    if (item->origin[0]) { snprintf(where_name, sizeof(where_name), "%s", item->origin); snprintf(where_href, sizeof(where_href), "%s", item->origin_href); }
    else if (item->rights[0] && public_http_url(item->href)) { page_publisher(item->href, where_name, sizeof(where_name)); snprintf(where_href, sizeof(where_href), "%s", item->href); }
    if (!where_name[0]) return;
    printf("<details class=\"reuse\"><summary>Use this %s</summary>", noun);
    if (item->rights[0]) {
        const BfPageLicence *licence = page_licence(item->rights, item->rights_href);
        if (licence) {
            /* What its terms let a person do, in plain words. */
            fputs("<ul class=\"terms-list\">", stdout);
            fputs(licence->commercial ? "<li>You may use it in something you sell.</li>" : "<li>Not for commercial use.</li>", stdout);
            fputs(licence->adapt ? "<li>You may crop, edit or remix it.</li>" : "<li>Use it as it is, without changes.</li>", stdout);
            if (licence->credit) fputs("<li>Credit who made it.</li>", stdout);
            if (licence->share_alike) fputs("<li>Share what you make from it under the same terms.</li>", stdout);
            fputs("</ul>", stdout);
        }
        fputs("<p>Credit it like this wherever you use it:</p><input readonly value=\"", stdout);
        fputs("&ldquo;", stdout); html(item->title); fputs("&rdquo;", stdout);
        if (item->by[0] && strcmp(item->by, item->note)) { fputs(" by ", stdout); html(item->by); }
        fputs(", ", stdout); html(item->rights); fputs(", via ", stdout); html(where_name); fputs("\">", stdout);
        fputs("<p>", stdout);
        if (item->rights_href[0]) { fputs("<a rel=\"license noopener\" target=\"_blank\" href=\"", stdout); html(item->rights_href); fputs("\">What ", stdout); html(item->rights); fputs(" lets you do</a> &middot; ", stdout); }
    } else {
        fputs("<p>Its page says who made it and how it may be used.</p><p>", stdout);
    }
    fputs("<a rel=\"noopener\" target=\"_blank\" href=\"", stdout); html(where_href); fputs(item->origin[0] ? "\">Full size and its terms at " : "\">Its terms at ", stdout); html(where_name); fputs("</a></p></details>", stdout);
}

/* The same thing as other sources report it: today's featured article may
 * also be one of the pages people are reading and something that happened on
 * this day.  One identity, several reports; each keeps its own source. */
static void page_emit_same_subject(sqlite3 *db, const PageItem *item, time_t now) {
    sqlite3_stmt *st = NULL; PageMaterial *other; int shown = 0, saved = page_per_source, k, i;
    if (!item->ident[0] || !(other = malloc(sizeof(*other)))) return;
    if (sqlite3_prepare_v2(db, "SELECT source FROM internet_source WHERE status='published' AND COALESCE(ident_field,'')<>'' "
          "AND source<>?1 ORDER BY id LIMIT 16", -1, &st, NULL) != SQLITE_OK) { free(other); return; }
    sqlite3_bind_text(st, 1, item->source, -1, SQLITE_TRANSIENT);
    page_per_source = 60;
    while (sqlite3_step(st) == SQLITE_ROW && shown < 8) {
        for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) other->stream[k].count = other->stream[k].cursor = 0;
        page_collect_retained(db, (const char *)sqlite3_column_text(st, 0), other);
        for (k = 0; k < BF_PAGE_MATERIAL_COUNT && shown < 8; k++) {
            if (k == BF_PAGE_MATERIAL_MIX || k == BF_PAGE_MATERIAL_PLACE) continue;
            for (i = 0; i < other->stream[k].count && shown < 8; i++) {
                const PageItem *same = &other->stream[k].item[i]; const char *close;
                if (strcmp(same->ident, item->ident)) continue;
                if (!shown++) fputs("<section class=\"region same-subject\"><h2 class=\"region-label\">Also about this</h2><ol class=\"more\">", stdout);
                fputs("<li>", stdout); close = page_emit_item_open(same, NULL); html(same->title); fputs(close, stdout);
                fputs("<span class=\"by\">", stdout); page_emit_by(same, now); fputs("</span></li>", stdout);
            }
        }
    }
    if (shown) fputs("</ol></section>", stdout);
    page_per_source = saved;
    sqlite3_finalize(st); free(other);
}

static int page_item_by_ref(sqlite3 *db, const char *source, const char *key, PageMaterial *material, const PageItem **found);

/* Every retained report whose identity (or, with depicts, whose depicted
 * identity) is this one, gathered into one material so any rhythm can show it.
 * Reports keep their own sources; nothing here merges them. */
static int page_collect_identity(sqlite3 *db, const char *ident, int depicts, PageMaterial *into, PageMaterial *scratch) {
    sqlite3_stmt *st = NULL; int found = 0, k, i, saved = page_per_source;
    if (!ident || !*ident) return 0;
    if (sqlite3_prepare_v2(db, depicts
          ? "SELECT source FROM internet_source WHERE status='published' AND COALESCE(about_field,'')<>'' ORDER BY id LIMIT 16"
          : "SELECT source FROM internet_source WHERE status='published' AND COALESCE(ident_field,'')<>'' ORDER BY id LIMIT 16", -1, &st, NULL) != SQLITE_OK) return 0;
    page_per_source = 60;
    while (sqlite3_step(st) == SQLITE_ROW) {
        for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) scratch->stream[k].count = scratch->stream[k].cursor = 0;
        page_collect_retained(db, (const char *)sqlite3_column_text(st, 0), scratch);
        for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) {
            if (k == BF_PAGE_MATERIAL_MIX) continue;
            for (i = 0; i < scratch->stream[k].count; i++) {
                const PageItem *it = &scratch->stream[k].item[i];
                PageStream *to = &into->stream[k];
                if (strcmp(depicts ? it->about : it->ident, ident) || to->count >= PAGE_STREAM_MAX) continue;
                to->item[to->count++] = *it;
                if (k != BF_PAGE_MATERIAL_PLACE && into->stream[BF_PAGE_MATERIAL_MIX].count < PAGE_STREAM_MAX)
                    into->stream[BF_PAGE_MATERIAL_MIX].item[into->stream[BF_PAGE_MATERIAL_MIX].count++] = *it;
                found++;
            }
        }
    }
    page_per_source = saved;
    sqlite3_finalize(st);
    return found;
}

/* The name a retained report gives an identity, for a link that says what it
 * leads to. */
static int page_identity_name(sqlite3 *db, const char *ident, char *name, size_t cap) {
    PageMaterial *into = calloc(1, sizeof(*into)), *scratch = malloc(sizeof(*scratch)); int ok = 0;
    if (into && scratch && page_collect_identity(db, ident, 0, into, scratch) && into->stream[BF_PAGE_MATERIAL_MIX].count) {
        snprintf(name, cap, "%s", into->stream[BF_PAGE_MATERIAL_MIX].item[0].title);
        ok = 1;
    } else {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db, "SELECT publisher_name FROM internet_source WHERE status='published' AND publisher_ident=?1 AND COALESCE(publisher_name,'')<>'' LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) { snprintf(name, cap, "%s", sqlite3_column_text(st, 0)); ok = 1; }
        }
        sqlite3_finalize(st);
    }
    free(into); free(scratch);
    return ok;
}

/* Pictures of this thing that other sources keep, and a way to see
 * everything the estate holds about it. */
static void page_emit_pictures_of(sqlite3 *db, const PageItem *item, time_t now) {
    PageMaterial *into = calloc(1, sizeof(*into)), *scratch = malloc(sizeof(*scratch)); int k, shown = 0;
    if (!into || !scratch || !item->ident[0]) { free(into); free(scratch); return; }
    if (page_collect_identity(db, item->ident, 1, into, scratch)) {
        PageStream *photos = &into->stream[BF_PAGE_MATERIAL_PHOTO];
        fputs("<section class=\"region region-photos\" aria-label=\"Pictures of it\"><h2 class=\"region-label\">Pictures of it</h2><div class=\"field\">", stdout);
        for (k = 0; k < photos->count && shown < 12; k++, shown++) {
            const PageItem *it = &photos->item[k]; const char *close;
            fputs("<figure class=\"photo\">", stdout); close = page_emit_item_open(it, NULL);
            page_emit_picture(it->image, "picture");
            fputs("<figcaption>", stdout); html(it->title); fputs("<span class=\"by\">", stdout); page_emit_by(it, now);
            if (it->rights[0]) { fputs("<span class=\"terms\">", stdout); html(it->rights); fputs("</span>", stdout); }
            fputs("</span></figcaption>", stdout); fputs(close, stdout); fputs("</figure>", stdout);
        }
        fputs("</div></section>", stdout);
    }
    free(into); free(scratch);
}

/* Recordings that sound alike, from the addressed atlas: neighbours in the
 * spectrum chart across every recording the atlas has grouped, each reopened
 * against current retained material before it is linked (one gone from its
 * source is simply not shown).  Nearness is not sameness, and the section says
 * what it measured. */
static void page_emit_sounds_similar(sqlite3 *db, const PageItem *item, time_t now) {
    const char *path = getenv("BONFYRE_LATENT_ATLAS_DB");
    sqlite3 *atlas = NULL; sqlite3_stmt *st = NULL; char address[200], group[300] = "";
    float mine[24]; int have = 0, count = 0, i, j, shown = 0;
    typedef struct { char address[200]; double d; } Near;
    Near near[64] = {0};
    if (!path || !*path || item->kind != BF_PAGE_MATERIAL_AUDIO) return;
    snprintf(address, sizeof(address), "o/%s/%s", item->source, item->key);
    if (sqlite3_open_v2(path, &atlas, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { sqlite3_close(atlas); return; }
    if (sqlite3_prepare_v2(atlas, "SELECT coordinate FROM projection WHERE address=? AND chart_id='spectrum-24'", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_bytes(st, 0) == 24 * 4) { memcpy(mine, sqlite3_column_blob(st, 0), sizeof(mine)); have = 1; }
    }
    sqlite3_finalize(st); st = NULL;
    if (have && sqlite3_prepare_v2(atlas, "SELECT dst FROM relation WHERE src=? AND relation='member_of' AND dst LIKE '%@spectrum-24' LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) snprintf(group, sizeof(group), "%s", sqlite3_column_text(st, 0));
    }
    sqlite3_finalize(st); st = NULL;
    if (have && sqlite3_prepare_v2(atlas, "SELECT p.address,p.coordinate FROM projection p WHERE p.chart_id='spectrum-24' AND p.address<>?1 "
          "AND (?2='' OR EXISTS(SELECT 1 FROM relation r WHERE r.src=p.address AND r.relation='member_of' AND r.dst LIKE '%@spectrum-24'))", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, group, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW && count < 64) {
            float other[24]; double sa = 0, sb = 0, bc = 0;
            if (sqlite3_column_bytes(st, 1) != 24 * 4) continue;
            memcpy(other, sqlite3_column_blob(st, 1), sizeof(other));
            for (i = 0; i < 24; i++) { sa += mine[i] > 0 ? mine[i] : 0; sb += other[i] > 0 ? other[i] : 0; }
            for (i = 0; sa > 0 && sb > 0 && i < 24; i++) bc += sqrt((mine[i] > 0 ? mine[i] / sa : 0) * (other[i] > 0 ? other[i] / sb : 0));
            snprintf(near[count].address, sizeof(near[count].address), "%s", sqlite3_column_text(st, 0));
            near[count].d = sqrt(fabs(1.0 - bc));
            count++;
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(atlas);
    for (i = 1; i < count; i++) for (j = i; j > 0 && near[j].d < near[j - 1].d; j--) { Near t = near[j]; near[j] = near[j - 1]; near[j - 1] = t; }
    {
        PageMaterial *material = malloc(sizeof(*material));
        for (i = 0; material && i < count && shown < 4; i++) {
            char source[128], key[32]; const char *slash = strchr(near[i].address + 2, '/'); const PageItem *found = NULL; const char *close;
            if (strncmp(near[i].address, "o/", 2) || !slash || (size_t)(slash - near[i].address - 2) >= sizeof(source)) continue;
            snprintf(source, sizeof(source), "%.*s", (int)(slash - near[i].address - 2), near[i].address + 2);
            snprintf(key, sizeof(key), "%.24s", slash + 1);
            for (j = 0; j < BF_PAGE_MATERIAL_COUNT; j++) material->stream[j].count = material->stream[j].cursor = 0;
            if (!page_item_by_ref(db, source, key, material, &found)) continue;   /* gone from its source: not linked */
            if (!shown++) fputs("<section class=\"region sounds-similar\"><h2 class=\"region-label\">Sounds similar</h2><p class=\"narrowed\">By how its sound is spread across pitches in its first half-minute, not by who made it.</p><ol class=\"more\">", stdout);
            fputs("<li>", stdout); close = page_emit_item_open(found, NULL); html(found->title); fputs(close, stdout);
            fputs("<span class=\"by\">", stdout); page_emit_by(found, now); fputs("</span></li>", stdout);
        }
        if (shown) fputs("</ol></section>", stdout);
        free(material);
    }
}

/* The atlas, when this estate keeps one: read-only, and optional. */
static sqlite3 *page_atlas_open(void) {
    const char *path = getenv("BONFYRE_LATENT_ATLAS_DB"); sqlite3 *atlas = NULL;
    if (!path || !*path || sqlite3_open_v2(path, &atlas, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { sqlite3_close(atlas); return NULL; }
    return atlas;
}

/* Items the atlas relates to a target by one typed relation, reopened
 * against current retained material; an item gone from its source is not
 * listed.  Returns how many were listed. */
static int page_emit_related_items(sqlite3 *db, const char *relation, const char *target, int by_source, const char *heading, int limit, time_t now) {
    sqlite3 *atlas = page_atlas_open(); sqlite3_stmt *st = NULL; PageMaterial *material = NULL; int shown = 0;
    char last_source[128] = "";
    if (!atlas) return 0;
    if (sqlite3_prepare_v2(atlas, by_source ? "SELECT dst FROM relation WHERE relation=?1 AND src=?2 ORDER BY dst LIMIT 64"
                                            : "SELECT src FROM relation WHERE relation=?1 AND dst=?2 ORDER BY src LIMIT 256", -1, &st, NULL) != SQLITE_OK ||
        !(material = malloc(sizeof(*material)))) { sqlite3_finalize(st); sqlite3_close(atlas); free(material); return 0; }
    sqlite3_bind_text(st, 1, relation, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, target, -1, SQLITE_TRANSIENT);
    for (int k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) material->stream[k].count = material->stream[k].cursor = 0;
    while (sqlite3_step(st) == SQLITE_ROW && shown < limit) {
        const char *address = (const char *)sqlite3_column_text(st, 0), *slash; char source[128], key[32]; const PageItem *found = NULL; const char *close;
        int k;
        if (!address || strncmp(address, "o/", 2) || !(slash = strchr(address + 2, '/')) || (size_t)(slash - address - 2) >= sizeof(source)) continue;
        snprintf(source, sizeof(source), "%.*s", (int)(slash - address - 2), address + 2);
        snprintf(key, sizeof(key), "%.24s", slash + 1);
        if (strcmp(source, last_source)) {
            for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) material->stream[k].count = material->stream[k].cursor = 0;
            page_collect_stories(db, source, material);
            snprintf(last_source, sizeof(last_source), "%s", source);
        }
        for (k = 0; k < material->stream[BF_PAGE_MATERIAL_MIX].count; k++)
            if (!strcmp(material->stream[BF_PAGE_MATERIAL_MIX].item[k].key, key)) { found = &material->stream[BF_PAGE_MATERIAL_MIX].item[k]; break; }
        for (k = 0; !found && k < material->stream[BF_PAGE_MATERIAL_EVENT].count; k++)
            if (!strcmp(material->stream[BF_PAGE_MATERIAL_EVENT].item[k].key, key)) { found = &material->stream[BF_PAGE_MATERIAL_EVENT].item[k]; break; }
        if (!found) continue;
        if (!shown++) { fputs("<section class=\"region\"><h2 class=\"region-label\">", stdout); html(heading); fputs("</h2><ol class=\"more\">", stdout); }
        fputs("<li>", stdout); close = page_emit_item_open(found, NULL); html(found->title); fputs(close, stdout);
        fputs("<span class=\"by\">", stdout);
        if (found->kind == BF_PAGE_MATERIAL_EVENT && found->at > 0) {
            /* An event is spoken of forwards even when it is reached sideways. */
            char when[48];
            page_when_ahead(found->at, found->duration_ms > 0 ? found->at + found->duration_ms / 1000 : found->at + 86400, now, when, sizeof(when));
            html(when); fputs(" &middot; ", stdout);
        }
        page_emit_by(found, now); fputs("</span></li>", stdout);
    }
    if (shown) fputs("</ol></section>", stdout);
    sqlite3_finalize(st); sqlite3_close(atlas); free(material);
    return shown;
}

/* The identities an item is related to by one typed relation, as links to
 * their pages, named by the retained report that names each. */
static void page_emit_identity_links(sqlite3 *db, const PageItem *item, const char *relation, const char *verb, const char *glyph, int limit) {
    sqlite3 *atlas = page_atlas_open(); sqlite3_stmt *st = NULL; char address[200]; int shown = 0;
    if (!atlas) return;
    snprintf(address, sizeof(address), "o/%s/%s", item->source, item->key);
    if (sqlite3_prepare_v2(atlas, "SELECT dst FROM relation WHERE relation=?1 AND src=?2 AND dst LIKE 'qid:%' ORDER BY dst LIMIT 16", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, relation, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, address, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW && shown < limit) {
            const char *qid = (const char *)sqlite3_column_text(st, 0) + 4; char name[768], href[80], label[900];
            if (!strcmp(qid, item->ident) || !strcmp(qid, item->about) || !page_identity_name(db, qid, name, sizeof(name))) continue;
            snprintf(href, sizeof(href), "/about/%s", qid);
            snprintf(label, sizeof(label), "%s %s", verb, name);
            page_item_action(href, glyph, label, 0, 0);
            shown++;
        }
    }
    sqlite3_finalize(st); sqlite3_close(atlas);
}

/* Items near this one in the atlas's words chart, within its family,
 * reopened before they are linked. */
static int page_emit_atlas_related(sqlite3 *db, const PageItem *item, const char *family, time_t now) {
    sqlite3 *atlas = page_atlas_open(); sqlite3_stmt *st = NULL; char address[200], group[200]; float mine[256]; int have = 0, count = 0, i, j, shown = 0;
    typedef struct { char address[200]; double d; } Near;
    Near *near = NULL;
    if (!atlas) return 0;
    snprintf(address, sizeof(address), "o/%s/%s", item->source, item->key);
    snprintf(group, sizeof(group), "family:%s@words-256", family);
    if (sqlite3_prepare_v2(atlas, "SELECT coordinate FROM projection WHERE address=? AND chart_id='words-256'", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_bytes(st, 0) == 256 * 4) { memcpy(mine, sqlite3_column_blob(st, 0), sizeof(mine)); have = 1; }
    }
    sqlite3_finalize(st); st = NULL;
    if (have && (near = malloc(sizeof(Near) * 1024)) &&
        sqlite3_prepare_v2(atlas, "SELECT p.address,p.coordinate FROM projection p JOIN relation r ON r.src=p.address AND r.relation='member_of' AND r.dst=?2 "
            "WHERE p.chart_id='words-256' AND p.address<>?1 LIMIT 1024", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, group, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW && count < 1024) {
            float other[256]; double dot = 0, na = 0, nb = 0;
            if (sqlite3_column_bytes(st, 1) != 256 * 4) continue;
            memcpy(other, sqlite3_column_blob(st, 1), sizeof(other));
            for (i = 0; i < 256; i++) { dot += mine[i] * other[i]; na += mine[i] * mine[i]; nb += other[i] * other[i]; }
            snprintf(near[count].address, sizeof(near[count].address), "%s", sqlite3_column_text(st, 0));
            near[count].d = na > 0 && nb > 0 ? 1.0 - dot / (sqrt(na) * sqrt(nb)) : 1.0;
            count++;
        }
    }
    sqlite3_finalize(st); sqlite3_close(atlas);
    if (!count) { free(near); return 0; }
    for (i = 1; i < count; i++) for (j = i; j > 0 && near[j].d < near[j - 1].d; j--) { Near t = near[j]; near[j] = near[j - 1]; near[j - 1] = t; }
    {
        PageMaterial *material = malloc(sizeof(*material)); char last_source[128] = "";
        for (i = 0; material && i < count && i < 40 && shown < 6; i++) {
            char source[128], key[32]; const char *slash = strchr(near[i].address + 2, '/'); const PageItem *found = NULL; const char *close; int k;
            if (near[i].d > 0.85 || strncmp(near[i].address, "o/", 2) || !slash || (size_t)(slash - near[i].address - 2) >= sizeof(source)) continue;
            snprintf(source, sizeof(source), "%.*s", (int)(slash - near[i].address - 2), near[i].address + 2);
            snprintf(key, sizeof(key), "%.24s", slash + 1);
            if (strcmp(source, last_source)) {
                for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) material->stream[k].count = material->stream[k].cursor = 0;
                page_collect_stories(db, source, material);
                snprintf(last_source, sizeof(last_source), "%s", source);
            }
            for (k = 0; k < material->stream[BF_PAGE_MATERIAL_MIX].count; k++)
                if (!strcmp(material->stream[BF_PAGE_MATERIAL_MIX].item[k].key, key)) { found = &material->stream[BF_PAGE_MATERIAL_MIX].item[k]; break; }
            if (!found) continue;
            if (!shown++) fputs("<section class=\"region\"><h2 class=\"region-label\">Related</h2><ol class=\"more\">", stdout);
            fputs("<li>", stdout); close = page_emit_item_open(found, NULL); html(found->title); fputs(close, stdout);
            fputs("<span class=\"by\">", stdout); page_emit_by(found, now); fputs("</span></li>", stdout);
        }
        if (shown) fputs("</ol></section>", stdout);
        free(material);
    }
    free(near);
    return shown;
}

/* ---- the calendar this estate writes --------------------------------------
 *
 * An event page can hand a person the event as a file their own calendar
 * understands.  It is written here, from what the organizer published, and it
 * says so: the description carries the source address, and the product id
 * names this estate as the compiler rather than the organizer.  It is never a
 * copy of the organizer's calendar and never claims their authority.
 */
static void page_ics_escape(const char *s) {
    for (; s && *s; s++) {
        if (*s == '\\' || *s == ';' || *s == ',') { putchar('\\'); putchar(*s); }
        else if (*s == '\n' || *s == '\r') fputs("\\n", stdout);
        else putchar(*s);
    }
}

static void page_ics_stamp(time_t at, char out[20]) {
    struct tm tm;
    gmtime_r(&at, &tm);
    strftime(out, 20, "%Y%m%dT%H%M%SZ", &tm);
}

static void page_emit_ics_event(const PageItem *item, const char *host, const char *source, const char *key) {
    char starts[20], ends[20], stamp[20];
    time_t until = item->duration_ms > 0 ? item->at + item->duration_ms / 1000 : item->at + 3600;
    page_ics_stamp(item->at, starts);
    page_ics_stamp(until, ends);
    page_ics_stamp(time(NULL), stamp);
    fputs("BEGIN:VEVENT\r\nUID:", stdout); page_ics_escape(source); putchar('-'); page_ics_escape(key);
    printf("@%s\r\nDTSTAMP:%s\r\nDTSTART:%s\r\nDTEND:%s\r\nSUMMARY:", host && *host ? host : "aurekai.life", stamp, starts, ends);
    page_ics_escape(item->title);
    if (item->where[0]) { fputs("\r\nLOCATION:", stdout); page_ics_escape(item->where); }
    if (item->placed) printf("\r\nGEO:%.5f;%.5f", item->lat, item->lon);
    if (public_http_url(item->href)) { fputs("\r\nURL:", stdout); page_ics_escape(item->href); }
    fputs("\r\nDESCRIPTION:", stdout);
    if (item->body[0]) { page_ics_escape(item->body); fputs("\\n\\n", stdout); }
    fputs("Compiled by Aurekai from what ", stdout); page_ics_escape(item->by);
    fputs(" published. Times and details are theirs; check the source before you rely on them.", stdout);
    if (public_http_url(item->href)) { fputs("\\n", stdout); page_ics_escape(item->href); }
    fputs("\r\nEND:VEVENT\r\n", stdout);
}

/* /o/<source>/<key>.ics -- one event as a file; /whats-on.ics -- everything
 * admitted that has not happened yet, in one file. */
static int emit_item_calendar(const char *target, const char *host) {
    char source[128], key[32]; const char *slash, *dot;
    sqlite3 *db = NULL; PageMaterial *material = NULL; const PageItem *item = NULL;
    size_t n; int k, ok = 0;
    if (!target || strncmp(target, "/o/", 3)) return 0;
    dot = strstr(target, ".ics");
    if (!dot || dot[4]) return 0;
    slash = strchr(target + 3, '/');
    if (!slash || (n = (size_t)(slash - (target + 3))) == 0 || n >= sizeof(source)) return 0;
    if ((size_t)(dot - (slash + 1)) != 24 || strspn(slash + 1, "0123456789abcdef") != 24) return 0;
    memcpy(source, target + 3, n); source[n] = '\0';
    if (strspn(source, "abcdefghijklmnopqrstuvwxyz0123456789-_.") != n) return 0;
    memcpy(key, slash + 1, 24); key[24] = '\0';
    if (!(db = cms_open_ro()) || !(material = calloc(1, sizeof(*material)))) goto out;
    page_collect_stories(db, source, material);
    for (k = 0; !item && k < material->stream[BF_PAGE_MATERIAL_EVENT].count; k++)
        if (!strcmp(material->stream[BF_PAGE_MATERIAL_EVENT].item[k].key, key)) item = &material->stream[BF_PAGE_MATERIAL_EVENT].item[k];
    if (!item || item->at <= 0) goto out;
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/calendar; charset=utf-8\r\n"
           "Content-Disposition: attachment; filename=\"event.ics\"\r\nCache-Control: public, max-age=300\r\nConnection: close\r\n\r\n");
    fputs("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Aurekai//compiled from published calendars//EN\r\nCALSCALE:GREGORIAN\r\n", stdout);
    page_emit_ics_event(item, host, source, key);
    fputs("END:VCALENDAR\r\n", stdout);
    ok = 1;
out:
    free(material);
    if (db) sqlite3_close(db);
    return ok;
}

static int emit_agenda_calendar(const char *target, const char *host) {
    const BfPageProgram *program = NULL; sqlite3 *db = NULL; PageMaterial *material = NULL;
    char sources[4096] = {0}; size_t i; int k, written = 0;
    if (!target || strcmp(target, "/whats-on.ics")) return 0;
    for (i = 0; !program && i < BF_PAGE_PROGRAM_COUNT; i++)
        if (!strcmp(bf_page_programs[i].route, "/whats-on")) program = &bf_page_programs[i];
    if (!program || !(db = cms_open_ro()) || !(material = calloc(1, sizeof(*material)))) goto out;
    page_program_sources(db, program, program->source_limit, sources, sizeof(sources));
    page_collect_stories(db, sources, material);
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/calendar; charset=utf-8\r\n"
           "Content-Disposition: attachment; filename=\"whats-on.ics\"\r\nCache-Control: public, max-age=600\r\nConnection: close\r\n\r\n");
    fputs("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Aurekai//compiled from published calendars//EN\r\n"
          "CALSCALE:GREGORIAN\r\nX-WR-CALNAME:What's on\r\n", stdout);
    for (k = 0; k < material->stream[BF_PAGE_MATERIAL_EVENT].count && written < 200; k++) {
        const PageItem *item = &material->stream[BF_PAGE_MATERIAL_EVENT].item[k];
        if (item->at <= 0) continue;
        page_emit_ics_event(item, host, item->source, item->key);
        written++;
    }
    fputs("END:VCALENDAR\r\n", stdout);
out:
    free(material);
    if (db) sqlite3_close(db);
    return written > 0;
}

static int pr_source(sqlite3 *db,const char *source,char *twin,size_t cap);
static int pr_item_fingerprint(const PageItem *item,const char *source,const char *twin,char digest[65]);
static int emit_item_page(const char *root, const char *target, const char *host, const char *bearer, const char *auth_db) {
    ItemDepth *depth = NULL; PageMaterial *relations = NULL;
    EstateReferenceStores stores = {0}; EstateReference publisher = {0};
    char source[128], key[32], path[PATH_MAX], family[64] = {0}, url[1024] = {0}, self[1024], publisher_ident[48] = {0}, publisher_name[256] = {0};
    char actor[64] = {0}; PageSaved saved; int is_saved = 0, from_archive = 0, from_projection = 0; PageItem *snapshot = NULL; PageItem archived;
    const char *slash; size_t n, i; sqlite3 *db = NULL; sqlite3_stmt *st = NULL;
    PageMaterial *material = NULL, *family_material = NULL; char *template = NULL, *head_end;
    const PageItem *item = NULL; const BfPageProgram *program = NULL;
    time_t now = time(NULL); int s, k, projected_item;
    if (!root || !target || strncmp(target, "/o/", 3)) return 0;
    char code_target[BF_PUBLIC_CODE_URI_MAX+1];
    int code_only=bf_public_code_item_path(target,code_target,sizeof(code_target));
    if(code_only)target=code_target;
    slash = strchr(target + 3, '/');
    if (!slash || (n = (size_t)(slash - (target + 3))) == 0 || n >= sizeof(source) ||
        strlen(slash + 1) != 24 || strspn(slash + 1, "0123456789abcdef") != 24) return 0;
    memcpy(source, target + 3, n); source[n] = '\0';
    if (strspn(source, "abcdefghijklmnopqrstuvwxyz0123456789-_.") != n) return 0;
    snprintf(key, sizeof(key), "%s", slash + 1);
    if (snprintf(path, sizeof(path), "%s/surfaces/world.html", root) >= (int)sizeof(path)) return 0;
    if (!(db = cms_open_ro())) goto out;
    projected_item = page_subject_projection_item(db, host, source, key, &archived, family, sizeof(family));
    if (projected_item > 0) {
        item = &archived;
        from_projection = 1;
    } else if (projected_item < 0) {
        char ref[200];
        snprintf(ref, sizeof(ref), "%s/%s", source, key);
        if (page_session_actor(bearer, auth_db, actor, sizeof(actor)) &&
            page_saved_get(db, actor, ref, &saved) &&
            (snapshot = calloc(1, sizeof(*snapshot)))) {
            page_item_from_saved(&saved, snapshot);
            item = snapshot;
            is_saved = 1;
        } else {
            sqlite3_close(db);
            protocol_reply(404, "text/plain; charset=utf-8", "Nothing here names that item.\n");
            return 1;
        }
    } else {
        if (!cms_type_registered(db, "internet_source") ||
            sqlite3_prepare_v2(db, "SELECT COALESCE(family,''),COALESCE(url,'') FROM internet_source WHERE source=?1 AND status='published' ORDER BY id DESC LIMIT 1",
                               -1, &st, NULL) != SQLITE_OK) {
            sqlite3_close(db);
            protocol_reply(404, "text/plain; charset=utf-8", "Nothing here names that item.\n");
            return 1;
        }
        sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            snprintf(family, sizeof(family), "%s", sqlite3_column_text(st, 0) ? (const char *)sqlite3_column_text(st, 0) : "");
            snprintf(url, sizeof(url), "%s", sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : "");
        } else {
            sqlite3_finalize(st);
            sqlite3_close(db);
            protocol_reply(404, "text/plain; charset=utf-8", "Nothing here names that item.\n");
            return 1;
        }
        sqlite3_finalize(st); st = NULL;
        /* A source that is one organization's own voice names it. */
        if (sqlite3_prepare_v2(db, "SELECT COALESCE(publisher_ident,''),COALESCE(publisher_name,'') FROM internet_source WHERE source=?1 AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) {
                page_ident((const char *)sqlite3_column_text(st, 0), publisher_ident, sizeof(publisher_ident));
                snprintf(publisher_name, sizeof(publisher_name), "%s", sqlite3_column_text(st, 1));
            }
        }
        sqlite3_finalize(st); st = NULL;
    }
    for (i = 0; !program && i < BF_PAGE_PROGRAM_COUNT; i++) {
        size_t f;
        if (!strcmp(bf_page_programs[i].route, "/") || !strcmp(bf_page_programs[i].route, "/search")) continue;
        for (f = 0; f < bf_page_programs[i].family_count; f++) if (!strcmp(bf_page_programs[i].families[f], family)) program = &bf_page_programs[i];
    }
    if (!(material = calloc(1, sizeof(*material))) || !page_open_template(path, &template, &head_end)) goto out;
    if (!item) page_collect_stories(db, source, material);
    for (k = 0; !item && k < material->stream[BF_PAGE_MATERIAL_MIX].count; k++)
        if (!strcmp(material->stream[BF_PAGE_MATERIAL_MIX].item[k].key, key)) item = &material->stream[BF_PAGE_MATERIAL_MIX].item[k];
    /* An event is not in the mix -- the mix is what was published, in time
     * order, and an event is something that will happen -- so its own stream
     * is searched for it. */
    for (k = 0; !item && k < material->stream[BF_PAGE_MATERIAL_EVENT].count; k++)
        if (!strcmp(material->stream[BF_PAGE_MATERIAL_EVENT].item[k].key, key)) item = &material->stream[BF_PAGE_MATERIAL_EVENT].item[k];
    /* Found, but found in the durable record rather than in the source's own
     * feed: the page says so rather than presenting it as current. */
    if (item && item->archived) from_archive = 1;
    if (!item) {
        /* Its source has moved on, and the address a person was given has not.
         * What this estate read is shown, dated, and says it is no longer in
         * the source's own feed. */
        sqlite3 *archive = page_archive_open(0);
        if (archive) {
            if (page_archive_item(archive, source, key, &archived, family, sizeof(family))) { item = &archived; from_archive = 1; }
            sqlite3_close(archive);
        }
    }
    if (page_session_actor(bearer, auth_db, actor, sizeof(actor))) {
        char ref[200];
        snprintf(ref, sizeof(ref), "%s/%s", source, key);
        is_saved = page_saved_get(db, actor, ref, &saved);
        /* A kept thing outlives its source's feed slot, for the person who kept it. */
        if (!item && is_saved && (snapshot = calloc(1, sizeof(*snapshot)))) { page_item_from_saved(&saved, snapshot); item = snapshot; }
    }
    snprintf(self, sizeof(self), "https://%s%s", host && *host ? host : "aurekai.live", target);
    if(code_only) {
        /* Resolve exactly the same public item before deriving a carrier.
         * Saved-only snapshots and account state never enter its payload. */
        if(!item||snapshot||!bf_public_code_url(host,target,self,sizeof(self))) {
            protocol_reply(404,"text/plain; charset=utf-8","No public page is available for this code.\n");
            goto done;
        }
        char *svg=NULL;size_t size=0;FILE *buffer=open_memstream(&svg,&size);
        int encoded=buffer?bf_public_code_emit_svg(buffer,self):0;
        if(buffer&&fclose(buffer)!=0)encoded=0;
        if(!encoded||size>128*1024)protocol_reply(500,"text/plain; charset=utf-8","The public code could not be rendered.\n");
        else {
            printf("HTTP/1.1 200 OK\r\nContent-Type: image/svg+xml; charset=utf-8\r\nContent-Length: %zu\r\nCache-Control: public, max-age=60\r\nVary: Host, Accept, Cookie\r\nX-Content-Type-Options: nosniff\r\nContent-Security-Policy: default-src 'none'; sandbox\r\nConnection: close\r\n\r\n",size);
            fwrite(svg,1,size,stdout);
        }
        free(svg);
        goto done;
    }
    printf("HTTP/1.1 %s\r\nContent-Type: text/html; charset=utf-8\r\n"
           "Cache-Control: %s\r\nVary: Host, Accept, Cookie\r\nConnection: close\r\n\r\n",
           item ? "200 OK" : "404 Not Found", actor[0] ? "private, no-store" : "public, max-age=60, stale-while-revalidate=300");
    page_emit_head(template, head_end, item ? item->title : "Not here any more", item && item->body[0] ? item->body : (item ? item->by : ""));
    if (item && page_image_url_safe(item->image)) { fputs("<meta property=\"og:image\" content=\"", stdout); html(item->image); fputs("\">", stdout); }
    fputs("</head><body class=\"", stdout); html(program ? program->character : "journal"); fputs(" item-page\" data-character=\"", stdout);
    html(program ? program->character : "journal"); fputs("\"><main class=\"wrap\">", stdout);
    {
        PageDocument nav = {0};
        nav.route = program ? program->route : NULL;
        nav.actor = actor[0] ? actor : NULL;
        nav.self_path = target;
        page_emit_nav(&nav);
    }
    if (!item) {
        fputs("<header class=\"hero\"><h1>Not here any more</h1><p class=\"lede\">This came from a source whose latest material no longer includes it.</p>", stdout);
        if (program) { fputs("<p class=\"jump\"><a href=\"", stdout); html(program->route); fputs("\">Back to ", stdout); html(program->title); fputs("</a></p>", stdout); }
        fputs("</header></main></body></html>", stdout);
        goto done;
    }
    fputs("<p class=\"crumbs\">", stdout);
    if (program) { fputs("<a href=\"", stdout); html(program->route); fputs("\">", stdout); html(program->title); fputs("</a>", stdout); }
    if (item->note[0]) { fputs(" &middot; ", stdout); page_emit_filter_link(&(PageEmitContext){.route = program ? program->route : "/search", .now = now, .db = NULL, .surface_scene = NULL}, "from", item->note); html(item->note); fputs("</a>", stdout); }
    fputs("</p>", stdout);
    if (snapshot) fputs("<p class=\"narrowed\">You saved this. Its source has moved on, so this is what it published when you kept it.</p>", stdout);
    fputs("<article class=\"thing\">", stdout);
    {
        char embed[1100];
        char sized[1024];
        page_picture_at(item->image, 1280, sized, sizeof(sized));
        if ((page_is_video(item) && page_peertube_embed(item->href, embed, sizeof(embed))) ||
            (!item->asset[0] && (page_is_video(item) || item->kind == BF_PAGE_MATERIAL_AUDIO) && page_embed_for(item->href, embed, sizeof(embed)))) {
            fputs("<div class=\"player\"><iframe src=\"", stdout); html(embed);
            fputs("\" title=\"", stdout); html(item->title);
            fputs("\" allow=\"fullscreen; picture-in-picture\" allowfullscreen loading=\"lazy\" referrerpolicy=\"no-referrer\" sandbox=\"allow-same-origin allow-scripts allow-popups\"></iframe></div>", stdout);
        } else if (item->asset[0] && item->kind == BF_PAGE_MATERIAL_VIDEO) {
            /* A file that plays is played here, from where it is kept. */
            fputs("<figure class=\"playing\"><div class=\"player\"><video controls playsinline preload=\"metadata\" src=\"", stdout); html(item->asset); fputs("\"", stdout);
            if (page_image_url_safe(sized)) { fputs(" poster=\"", stdout); html(sized); fputs("\"", stdout); }
            fputs("></video></div>", stdout); page_emit_credit(item, "figcaption"); fputs("</figure>", stdout);
        } else if (item->asset[0] && item->kind == BF_PAGE_MATERIAL_AUDIO) {
            fputs("<figure class=\"hero-picture listening\">", stdout);
            if (page_image_url_safe(sized)) page_emit_picture(sized, "picture");
            fputs("<audio controls preload=\"metadata\" src=\"", stdout); html(item->asset); fputs("\"></audio>", stdout);
            page_emit_credit(item, "figcaption"); fputs("</figure>", stdout);
        } else if (page_image_url_safe(item->image)) {
            fputs("<figure class=\"hero-picture\">", stdout); page_emit_picture(page_image_url_safe(sized) ? sized : item->image, "picture");
            page_emit_credit(item, "figcaption"); fputs("</figure>", stdout);
        } else if (item->placed) {
            PageStream *one = calloc(1, sizeof(*one));
            if (one) {
                one->count = 1; one->item[0] = *item;
                fputs("<figure class=\"map\">", stdout); page_emit_map_plate(one, 1, 0); fputs("</figure>", stdout);
                free(one);
            }
        }
    }
    if (item->kind == BF_PAGE_MATERIAL_EVENT && item->at > 0) {
        /* The same thing, said once more for machines: exactly what this page
         * shows, with the organizer's own address as what it is based on, so a
         * reader that is not a person still learns where this came from. */
        struct tm tm; char iso[32], ends_iso[32] = "";
        time_t ends = item->duration_ms > 0 ? item->at + item->duration_ms / 1000 : 0;
        gmtime_r(&item->at, &tm); strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ", &tm);
        if (ends > 0 && gmtime_r(&ends, &tm)) strftime(ends_iso, sizeof(ends_iso), "%Y-%m-%dT%H:%M:%SZ", &tm);
        fputs("<script type=\"application/ld+json\">{\"@context\":\"https://schema.org\",\"@type\":\"Event\",\"name\":", stdout);
        json(item->title);
        printf(",\"startDate\":\"%s\"", iso);
        if (ends_iso[0]) printf(",\"endDate\":\"%s\"", ends_iso);
        if (item->where[0]) { fputs(",\"location\":{\"@type\":\"Place\",\"name\":", stdout); json(item->where);
            if (item->placed) printf(",\"geo\":{\"@type\":\"GeoCoordinates\",\"latitude\":%.5f,\"longitude\":%.5f}", item->lat, item->lon);
            fputs("}", stdout); }
        if (publisher_name[0]) { fputs(",\"organizer\":{\"@type\":\"Organization\",\"name\":", stdout); json(publisher_name); fputs("}", stdout); }
        if (public_http_url(item->href)) { fputs(",\"url\":", stdout); json(item->href); fputs(",\"isBasedOn\":", stdout); json(item->href); }
        fputs(",\"description\":\"Compiled by Aurekai from what the organizer published; it is not an invitation, a reservation or a promise that it will happen.\"}</script>", stdout);
    }
    fputs("<div class=\"thing-text\">", stdout);
    if (item->kind == BF_PAGE_MATERIAL_EVENT) {
        /* An event says first what it is right now. */
        time_t ends = 0; PageEventPhase phase = page_event_phase(item, now, &ends);
        fputs("<p class=\"phase phase-", stdout);
        fputs(phase == PAGE_EVENT_ON_NOW ? "now" : phase == PAGE_EVENT_SOON ? "soon" : phase == PAGE_EVENT_PAST || phase == PAGE_EVENT_JUST_ENDED ? "past" : "ahead", stdout);
        fputs("\">", stdout); html(page_event_phase_words(phase)); fputs("</p>", stdout);
    }
    /* Who published it opens the publisher's own address here. */
    estate_reference_open(&stores);
    estate_reference_resolve(&stores, source, &publisher);
    char shown_publisher[256] = "";
    page_short_publisher(publisher.estate && publisher.name[0] ? publisher.name : publisher_name[0] ? publisher_name : item->by,
                         item->href, shown_publisher, sizeof(shown_publisher));
    fputs("<h1>", stdout); html(item->title); fputs("</h1><p class=\"by\">", stdout);
    if (publisher.estate && publisher.name[0]) {
        PageItem shown = *item;
        snprintf(shown.by, sizeof(shown.by), "%s", "");
        fputs("<span><a href=\"", stdout); html(publisher.url); fputs("\">", stdout); html(shown_publisher[0] ? shown_publisher : publisher.name); fputs("</a></span>", stdout);
        page_emit_by(&shown, now);
    } else {
        /* No estate profile yet: keep the source byline without its marketing tagline. */
        PageItem shown = *item;
        if (shown_publisher[0]) snprintf(shown.by, sizeof(shown.by), "%s", shown_publisher);
        page_emit_by(&shown, now);
    }
    fputs("</p>", stdout);
    if (from_projection) fputs("<p class=\"narrowed\">Read from a public source earlier. Its current version may differ.</p>", stdout);
    if (from_archive) {
        /* Kept, not current: this estate read it, and its source has since
         * moved on.  The link still goes to the source, which may have moved
         * on too. */
        fputs("<p class=\"kept\">This is what was read here when it was published. ", stdout);
        html(item->by[0] ? item->by : "The source");
        fputs(" no longer carries it in what it publishes now.</p>", stdout);
    }
    /* What the piece says, read from the source's own retained entry; the
     * short body the census kept is the fallback. */
    if ((depth = calloc(1, sizeof(*depth))) && depth_read(source, item, depth)) depth_resolve_names(depth, &stores);
    if (depth && depth->paras) depth_emit_said(depth);
    else if (item->body[0]) { fputs("<p class=\"thing-body\">", stdout); html(item->body); fputs("</p>", stdout); }
    if (depth) depth_emit_facts(depth);
    if (item->kind == BF_PAGE_MATERIAL_EVENT && item->at > 0) {
        /* What the organizer stated, as a person reads it: when it starts, how
         * long it runs, where it is.  Nothing here is inferred. */
        time_t ends = 0; struct tm tm;
        char starts_words[64] = "", ends_words[64] = "", iso[32] = "";
        page_event_phase(item, now, &ends);
        page_local_time(item->at, &tm);
        strftime(starts_words, sizeof(starts_words), "%A %e %B, %l:%M%p", &tm);
        { struct tm utc; gmtime_r(&item->at, &utc); strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ", &utc); }
        if (item->duration_ms > 0 && page_local_time(ends, &tm)) strftime(ends_words, sizeof(ends_words), "%l:%M%p", &tm);
        fputs("<dl class=\"facts event\"><div><dt>Starts</dt><dd><time datetime=\"", stdout); html(iso); fputs("\">", stdout);
        html(starts_words); fputs("</time></dd></div>", stdout);
        if (ends_words[0]) { fputs("<div><dt>Until</dt><dd>", stdout); html(ends_words); fputs("</dd></div>", stdout); }
        if (item->where[0]) { fputs("<div><dt>Where</dt><dd>", stdout); html(item->where); fputs("</dd></div>", stdout); }
        if (publisher_name[0]) { fputs("<div><dt>Published by</dt><dd>", stdout); html(publisher_name); fputs("</dd></div>", stdout); }
        fputs("</dl>", stdout);
    } else if (item->people[0] || item->where[0] || item->duration_ms > 0) {
        /* What the show says about its episode, as facts a person scans. */
        fputs("<dl class=\"facts episode\">", stdout);
        if (item->duration_ms > 0) { fputs("<div><dt>Length</dt><dd>", stdout); page_emit_duration(item->duration_ms); fputs("</dd></div>", stdout); }
        if (item->people[0]) { fputs("<div><dt>With</dt><dd>", stdout); html(item->people); fputs("</dd></div>", stdout); }
        if (item->where[0]) { fputs("<div><dt>Where</dt><dd>", stdout); html(item->where); fputs("</dd></div>", stdout); }
        fputs("</dl>", stdout);
    }
    /* The few things a person came to do sit together: open it where it lives,
     * keep it, pass it on.  Everything else waits behind one quiet disclosure,
     * and nothing that is a section of its own is printed inside this row. */
    fputs("<nav id=\"item-keep\" class=\"verbs\" aria-label=\"Actions\">", stdout);
    if (public_http_url(item->href)) {
        /* The link out is named by where it goes, not by who carried it. */
        char label[320], destination[128];
        page_publisher(item->href, destination, sizeof(destination));
        if (!destination[0]) snprintf(destination, sizeof(destination), "%s", item->note[0] ? item->note : "the source");
        /* A picture is seen where it is kept; a link anywhere else is read. */
        char playing[1100];
        snprintf(label, sizeof(label), "%s %s", item->asset[0] ? "Read about it at" : page_embed_for(item->href, playing, sizeof(playing)) ? "See it at" : page_is_video(item) ? "Watch on" :
                 page_is_photo(item) && (!item->origin[0] || !strcmp(item->origin, destination)) ? "See at" : "Open on", destination);
        page_item_action(item->href, page_is_video(item) && !item->asset[0] ? "play" : "out", label, 1, 1);
    }
    if (item->kind == BF_PAGE_MATERIAL_EVENT) {
        /* Two verbs a listing cannot give: the event as a file a calendar
         * understands, written here from what the organizer published, and the
         * way there when the organizer said where it is. */
        char calendar[260], directions[200];
        snprintf(calendar, sizeof(calendar), "%s.ics", target);
        page_item_action(calendar, "save", "Add to your calendar", 0, 1);
        if (item->placed) {
            snprintf(directions, sizeof(directions), "https://www.openstreetmap.org/directions?to=%.5f%%2C%.5f", item->lat, item->lon);
            page_item_action(directions, "pin", "Get directions", 1, 0);
        }
    }
    if (item->transcript_href[0]) page_item_action(item->transcript_href, "text", "Read the transcript", 1, 0);
    if (item->chapters_href[0]) page_item_action(item->chapters_href, "list", "Chapters", 1, 0);
    if (item->talk_href[0]) page_item_action(item->talk_href, "share", "Join the conversation", 1, 0);
    if (page_is_photo(item) && page_image_url_safe(item->image) && !item->origin[0]) page_item_action(item->image, "picture", "View full size", 1, 0);
    if (actor[0]) {
        /* A form, so saving works before and without any script, and comes
         * back to this page. */
        fputs(is_saved ? "<form class=\"verb-form\" method=\"post\" action=\"/o/unsave\">" : "<form class=\"verb-form\" method=\"post\" action=\"/o/save\">", stdout);
        fputs("<input type=\"hidden\" name=\"item\" value=\"", stdout); html(source); fputs("/", stdout); html(key);
        fputs("\"><input type=\"hidden\" name=\"return\" value=\"", stdout); html(target);
        fputs(is_saved ? "\"><button class=\"verb is-on\" type=\"submit\" aria-pressed=\"true\">" : "\"><button class=\"verb\" type=\"submit\" aria-pressed=\"false\">", stdout);
        page_glyph("save"); fputs(is_saved ? "<span>Saved</span>" : "<span>Save</span>", stdout); fputs("</button></form>", stdout);
    } else {
        char login[1200];
        snprintf(login, sizeof(login), "/login?next=%s", target);
        page_item_action(login, "save", "Save", 0, 0);
    }
    {
        char share[2400]; int w = snprintf(share, sizeof(share), "mailto:?subject=");
        const unsigned char *p;
        for (p = (const unsigned char *)item->title; *p && w < (int)sizeof(share) - 8; p++) w += isalnum(*p) ? snprintf(share + w, sizeof(share) - (size_t)w, "%c", *p) : snprintf(share + w, sizeof(share) - (size_t)w, "%%%02X", *p);
        w += snprintf(share + w, sizeof(share) - (size_t)w, "&body=");
        for (p = (const unsigned char *)self; *p && w < (int)sizeof(share) - 8; p++) w += isalnum(*p) || strchr("-._~", *p) ? snprintf(share + w, sizeof(share) - (size_t)w, "%c", *p) : snprintf(share + w, sizeof(share) - (size_t)w, "%%%02X", *p);
        page_item_action(share, "share", "Share", 0, 0);
    }
    /* A saved source may be placed in an account-owned World on an explicitly
     * chosen profile site; never infer a site or auto-publish the item. */
    if(actor[0]&&is_saved){page_item_action("/profile?tab=sites","world","Add to a World",0,0);}
    /* Installed applications are the signed-in reader's next step, not plumbing. */
    if (actor[0] && !snapshot && strlen(source) < 96) {
        char use[512];
        snprintf(use, sizeof(use), "/feed-use?context=self&item=%s%%2F%s", source, key);
        page_item_action(use, "apps", "Use in your work", 0, 0);
    }
    fputs("</nav>", stdout);
    /* Where else this leads, as places rather than buttons. */
    fputs("<nav class=\"verbs leads\" aria-label=\"Related places\">", stdout);
    if (publisher.estate && publisher.name[0]) {
        char label[320];
        snprintf(label, sizeof(label), "More from %s", shown_publisher[0] ? shown_publisher : publisher.name);
        page_item_action(publisher.url, "list", label, 0, 0);
    } else if (publisher_ident[0] && publisher_name[0]) {
        char about[80], label[320];
        snprintf(about, sizeof(about), "/about/%s", publisher_ident);
        snprintf(label, sizeof(label), "More from %s", shown_publisher[0] ? shown_publisher : publisher_name);
        page_item_action(about, "list", label, 0, 0);
    }
    if (program && item->by[0] && strcmp(item->by, item->note)) {
        char more[800]; int w = snprintf(more, sizeof(more), "%s?from=", program->route);
        const unsigned char *p;
        for (p = (const unsigned char *)item->by; *p && w < (int)sizeof(more) - 4; p++)
            w += isalnum(*p) ? snprintf(more + w, sizeof(more) - (size_t)w, "%c", *p) : snprintf(more + w, sizeof(more) - (size_t)w, "%%%02X", *p);
        { char label[320]; snprintf(label, sizeof(label), "More from %s", item->by); page_item_action(more, "list", label, 0, 0); }
    }
    if (item->ident[0]) {
        char about[80];
        snprintf(about, sizeof(about), "/about/%s", item->ident);
        page_item_action(about, "list", "Everything about this", 0, 0);
    }
    page_emit_identity_links(db, item, "happens_at", "Everything at", "pin", 2);
    page_emit_identity_links(db, item, "mentions", "Mentions", "pin", 3);
    page_emit_identity_links(db, item, "same_subject_as", "Also listed as", "list", 1);
    if (item->about[0]) {
        char name[768], about[80], label[800];
        if (page_identity_name(db, item->about, name, sizeof(name))) {
            snprintf(about, sizeof(about), "/about/%s", item->about);
            snprintf(label, sizeof(label), "Shows %s", name);
            page_item_action(about, "picture", label, 0, 0);
        }
    }
    if (item->kind == BF_PAGE_MATERIAL_EVENT) {
        char stage[1024];
        if (snprintf(stage, sizeof(stage), "/stage?q=item:%s/%s", item->source, item->key) < (int)sizeof(stage))
            page_item_action(stage, "play", "Open the live stage", 0, 0);
        if (public_http_url(url)) page_item_action(url, "list", "The organizer's own calendar", 1, 0);
    }
    if (item->placed) {
        char map[256];
        snprintf(map, sizeof(map), "https://www.openstreetmap.org/?mlat=%.4f&mlon=%.4f#map=7/%.4f/%.4f", item->lat, item->lon, item->lat, item->lon);
        page_item_action(map, "pin", "Open in a map", 1, 0);
    }
    fputs("</nav>", stdout);
    /* Tools for people who build with what they read stay one tap away. */
    fputs("<details class=\"more-verbs\"><summary>More ways to use this</summary><nav class=\"verbs quiet\" aria-label=\"More ways to use this\">", stdout);
    if (!snapshot && strlen(source)<96) {
        char twin[256],digest[65],continuation[768];
        if(pr_source(db,source,twin,sizeof(twin))&&pr_item_fingerprint(item,source,twin,digest)){
            snprintf(continuation,sizeof(continuation),"/sources?source=%s&item=%s&snapshot=%s&return=%%2Fo%%2F%s%%2F%s",source,key,digest,source,key);
            page_item_action(continuation,"list","Work with this source",0,0);
        }
    if (!actor[0]) { char use[512];snprintf(use,sizeof(use),"/feed-use?context=self&item=%s%%2F%s",source,key);page_item_action(use,"apps","Use in your work",0,0); }
    { char compose[512];snprintf(compose,sizeof(compose),"/profile?tab=compose&ref=%s%%2F%s",source,key);page_item_action(compose,"text","Write a post about it",0,0); }
    }
    if (actor[0] && !snapshot && page_is_photo(item) && page_image_url_safe(item->image) && !strcmp(item->rights, "CC0")) {
        /* The owner reviews the header or gallery choice on their own profile. */
        char use[320]; snprintf(use, sizeof(use), "/profile?tab=artwork&art=%s%%2F%s", source, key);
        page_item_action(use, "picture", "Use on my profile", 0, 0);
    }
    if (public_http_url(item->href) && !snapshot) {
        char changes[200];
        snprintf(changes, sizeof(changes), "%s/changes", target);
        page_item_action(changes, "list", "How this page changed", 0, 0);
    }
    fputs("</nav>", stdout);
    page_emit_reuse(item);
    if(!snapshot)bf_public_code_emit_details(stdout,host,target);
    fputs("<p class=\"link-line\"><label>Link <input readonly value=\"", stdout); html(self); fputs("\"></label></p></details>", stdout);
    if (item->placed && (page_image_url_safe(item->image) || item->asset[0])) {
        /* A place with a picture also shows where it is. */
        PageStream *one = calloc(1, sizeof(*one));
        if (one) {
            one->count = 1; one->item[0] = *item;
            fputs("<figure class=\"map where\">", stdout); page_emit_map_plate(one, 1, 0); fputs("</figure>", stdout);
            free(one);
        }
    }
    fputs("</div></article>", stdout);
    /* Sections of their own, after the piece rather than inside its actions. */
    if (item->placed && !page_emit_around_precomputed(source, key, item->ident, now))
        page_emit_around(db, item->lat, item->lon, source, key, item->ident, 2.5, now);
    page_emit_kin(source, key, item->ident);
    /* What the title names, as the identity graph has it. */
    page_emit_entities(item->title);
    if (!snapshot) {
        char heading[320];
        if (shown_publisher[0]) snprintf(heading, sizeof(heading), "Also from %s", shown_publisher);
        else snprintf(heading, sizeof(heading), "%s", "Also from here");
        page_emit_archive_earlier(source, key, heading, 8, now);
    }
    /* What the piece relates to -- its parts, the organizations it names, the
     * ways its publisher puts things out -- is material like any other: the
     * scene compiler decides which rhythms it argues for, and each region is
     * dressed by a published presentation of its own. */
    if ((relations = calloc(1, sizeof(*relations)))) {
        unsigned char sequence[BF_PAGE_SCENE_GRAMMAR_COUNT]; char why[256] = "";
        const char *by = publisher.name[0] ? publisher.name : item->by;
        size_t count;
        if (depth) depth_fill_material(depth, item, by, relations);
        /* How the publisher puts things out is plumbing, not something a reader
         * came for; the item page no longer lists its feeds. */
        page_scene_in_depth = 1;
        count = page_compile_scene(relations, sequence, sizeof(sequence), why, sizeof(why), now);
        page_scene_in_depth = 0;
        if (count) {
            PageEmitContext rctx = {.route = program ? program->route : "/search", .now = now, .db = db, .surface_scene = NULL};
            if (why[0]) { fputs("<meta name=\"bonfyre:relations\" content=\"", stdout); html(why); fputs("\">", stdout); }
            page_emit_regions(relations, sequence, count, &rctx);
        }
    }
    /* Where a visitor goes next: the feed, publisher, makers, reader,
     * application shapes, topics and printed names this piece reaches. */
    if (!snapshot) { BfDetailFocus focus = {BF_DETAIL_ITEM, source, key, 0}; bf_detail_continue(stdout, &focus); }
    page_emit_pictures_of(db, item, now);
    page_emit_sounds_similar(db, item, now);
    page_emit_same_subject(db, item, now);
    if (material->stream[BF_PAGE_MATERIAL_MIX].count > 1) {   /* More from the same maker. */
        int shown = 0;
        fputs("<section class=\"region\"><h2 class=\"region-label\">", stdout);
        html(item->note[0] ? "More from " : "More"); html(item->note); fputs("</h2><div class=\"index\">", stdout);
        for (k = 0; k < material->stream[BF_PAGE_MATERIAL_MIX].count && shown < 6; k++) {
            const PageItem *other = &material->stream[BF_PAGE_MATERIAL_MIX].item[k];
            const char *close;
            if (!strcmp(other->key, item->key)) continue;
            shown++;
            close = page_emit_item_open(other, NULL);
            fputs("<b>", stdout); html(other->title); fputs("</b><span class=\"by\">", stdout); page_emit_by(other, now); fputs("</span>", stdout);
            page_emit_picture(other->image, "thumb"); fputs(close, stdout);
        }
        fputs("</div></section>", stdout);
    }
    if (!page_emit_atlas_related(db, item, family, now) && !from_projection &&
        program && (family_material = calloc(1, sizeof(*family_material)))) {
        char sources[4096]; int shown = 0;
        page_program_sources(db, program, 8, sources, sizeof(sources));
        page_collect_stories(db, sources, family_material);
        /* Only what shares words with this piece; filling the space with
         * whatever else the route carries said nothing about this one. */
        for (s = 0; s < 1 && shown < 6; s++)
            for (k = 0; k < family_material->stream[BF_PAGE_MATERIAL_MIX].count && shown < 6; k++) {
                const PageItem *other = &family_material->stream[BF_PAGE_MATERIAL_MIX].item[k];
                const char *close;
                if (!strcmp(other->source, item->source)) continue;
                if (s == 0 && page_words_shared(item->title, other->title) < 1) continue;
                if (s == 1 && page_words_shared(item->title, other->title) >= 1) continue;
                if (!shown) { fputs("<section class=\"region\"><h2 class=\"region-label\">", stdout); fputs(s == 0 ? "Related" : "Also in ", stdout); if (s == 1) html(program->title); fputs("</h2><ol class=\"more\">", stdout); }
                shown++;
                fputs("<li>", stdout); close = page_emit_item_open(other, NULL); html(other->title); fputs(close, stdout);
                fputs("<span class=\"by\">", stdout); page_emit_by(other, now); fputs("</span></li>", stdout);
            }
        if (shown) fputs("</ol></section>", stdout);
    }
    fputs("</main></body></html>", stdout);
done:
    free(template); free(material); free(family_material); free(snapshot); free(depth); free(relations);
    estate_reference_close(&stores);
    sqlite3_close(db);
    return 1;
out:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    free(template); free(material); free(snapshot); free(depth); free(relations);
    estate_reference_close(&stores);
    return 0;
}

/* ------------------------------------------------------------ what changed
 *
 * A thing that links out also has a history there: the Wayback Machine's
 * capture index says when the page at that address answered with different
 * content.  The index is read only when a person asks, at most once a day per
 * address, through the ForeignTwin observer (robots preflight, receipt,
 * retained body); the page shows the retained answer and links each change to
 * the Archive's own copy.  A capture is what the Archive's crawler saw, not
 * what every visitor saw or what is true. */
static void page_history_twin(const char *href, char *twin, size_t cap) {
    char hex[65];
    bf_sha256_hex((const uint8_t *)href, strlen(href), hex);
    snprintf(twin, cap, "history-%.24s", hex);
}

static int page_item_by_ref(sqlite3 *db, const char *source, const char *key, PageMaterial *material, const PageItem **found) {
    int k;
    *found = NULL;
    page_collect_stories(db, source, material);
    for (k = 0; k < material->stream[BF_PAGE_MATERIAL_MIX].count; k++)
        if (!strcmp(material->stream[BF_PAGE_MATERIAL_MIX].item[k].key, key)) { *found = &material->stream[BF_PAGE_MATERIAL_MIX].item[k]; return 1; }
    return 0;
}

static int serve_page_changes(const char *root, const char *method, const char *target, const char *body, const char *raw,
                              const char *host, const char *bearer, const char *auth_db) {
    char source[128], key[32], path[PATH_MAX], twin[64], receipt_path[PATH_MAX], body_path[PATH_MAX], actor[64] = {0};
    const char *slash, *foreign = getenv("BONFYRE_FOREIGN_ROOT");
    const PageItem *item = NULL; PageMaterial *material = NULL; sqlite3 *db = NULL; char *template = NULL, *head_end;
    struct stat seen; int posted, have = 0, fresh = 0; time_t now = time(NULL);
    size_t n;
    if (!foreign || !*foreign) foreign = "/var/lib/bonfyre/foreign";
    posted = !strcmp(target, "/o/changes");
    if (posted) {
        char ref[200] = {0};
        if (strcmp(method, "POST")) { protocol_reply(405, "text/plain; charset=utf-8", "Looking up a page's history is a form submission.\n"); return 1; }
        {
            const char *origin = raw ? strstr(raw, "\nOrigin:") : NULL;
            if (!origin && raw) origin = strstr(raw, "\norigin:");
            if (origin) {
                char value[300] = {0}, a[300], b[300];
                sscanf(origin + 8, " %299[^\r\n]", value);
                snprintf(a, sizeof(a), "https://%s", host ? host : ""); snprintf(b, sizeof(b), "http://%s", host ? host : "");
                if (strcmp(value, a) && strcmp(value, b) && strcmp(value, "null")) { protocol_reply(403, "text/plain; charset=utf-8", "Only accepted from this site.\n"); return 1; }
            }
        }
        if (!body || !query_value(body, "item", ref, sizeof(ref)) || !(slash = strchr(ref, '/')) || (size_t)(slash - ref) >= sizeof(source) ||
            strlen(slash + 1) != 24 || strspn(slash + 1, "0123456789abcdef") != 24 || strspn(ref, "abcdefghijklmnopqrstuvwxyz0123456789-_./") != strlen(ref)) {
            protocol_reply(400, "text/plain; charset=utf-8", "That has no page history to look up.\n"); return 1;
        }
        snprintf(source, sizeof(source), "%.*s", (int)(slash - ref), ref);
        snprintf(key, sizeof(key), "%s", slash + 1);
    } else {
        size_t len = strlen(target);
        if (len < 3 + 1 + 24 + 8 || strcmp(target + len - 8, "/changes") || strcmp(method, "GET")) return 0;
        slash = strchr(target + 3, '/');
        if (!slash || (n = (size_t)(slash - (target + 3))) == 0 || n >= sizeof(source) || (size_t)(target + len - 8 - (slash + 1)) != 24) return 0;
        memcpy(source, target + 3, n); source[n] = '\0';
        snprintf(key, sizeof(key), "%.24s", slash + 1);
        if (strspn(key, "0123456789abcdef") != 24 || strspn(source, "abcdefghijklmnopqrstuvwxyz0123456789-_.") != strlen(source)) return 0;
    }
    if (!(db = cms_open_ro()) || !(material = calloc(1, sizeof(*material))) || !page_item_by_ref(db, source, key, material, &item) || !public_http_url(item->href)) {
        if (db) sqlite3_close(db);
        free(material);
        protocol_reply(404, "text/plain; charset=utf-8", "Not here any more.\n");
        return 1;
    }
    page_history_twin(item->href, twin, sizeof(twin));
    snprintf(receipt_path, sizeof(receipt_path), "%s/receipts/%s.json", foreign, twin);
    snprintf(body_path, sizeof(body_path), "%s/bodies/%s.body", foreign, twin);
    have = stat(body_path, &seen) == 0 && seen.st_size > 0;
    fresh = have && now - seen.st_mtime < 86400;
    {
        /* A lookup the Archive did not answer is not repeated for an hour. */
        struct stat asked;
        if (!fresh && stat(receipt_path, &asked) == 0 && now - asked.st_mtime < 3600) fresh = -1;
    }
    if (posted) {
        if (fresh == 0) {
            char url[2400], out[4096]; int w = snprintf(url, sizeof(url), "https://web.archive.org/cdx/search/cdx?output=json&fl=timestamp,statuscode,digest,length&collapse=digest&limit=-80&url=");
            const unsigned char *p;
            for (p = (const unsigned char *)item->href; *p && w < (int)sizeof(url) - 4; p++)
                w += isalnum(*p) || strchr("-._~", *p) ? snprintf(url + w, sizeof(url) - (size_t)w, "%c", *p) : snprintf(url + w, sizeof(url) - (size_t)w, "%%%02X", *p);
            {
                const char *project = page_cms_env("BONFYRE_PROJECT_BINARY", "/usr/local/bin/bonfyre-project");
                char *const argv[] = {(char *)project, "foreign", "observe", twin, url, "--receipt", receipt_path, "--body", body_path, NULL};
                auth_call(argv, NULL, out, sizeof(out));
            }
        }
        {
            char back[200];
            snprintf(back, sizeof(back), "/o/%s/%s/changes", source, key);
            page_redirect(back);
        }
        sqlite3_close(db); free(material);
        return 1;
    }
    page_session_actor(bearer, auth_db, actor, sizeof(actor));
    if (snprintf(path, sizeof(path), "%s/surfaces/world.html", root) >= (int)sizeof(path) || !page_open_template(path, &template, &head_end)) {
        sqlite3_close(db); free(material); return 0;
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: %s\r\nVary: Host, Cookie\r\nConnection: close\r\n\r\n",
           actor[0] ? "private, no-store" : "public, max-age=60");
    {
        char title[900];
        snprintf(title, sizeof(title), "How %s changed", item->title);
        page_emit_head(template, head_end, title, "When the page this links to answered with different content, as the Internet Archive recorded it.");
    }
    fputs("</head><body class=\"journal item-page\" data-character=\"journal\"><main class=\"wrap\">", stdout);
    { PageDocument nav = {0}; nav.actor = actor[0] ? actor : NULL; nav.self_path = target; page_emit_nav(&nav); }
    fputs("<p class=\"crumbs\"><a href=\"/o/", stdout); page_url_escape(source); fputs("/", stdout); html(key); fputs("\">", stdout); html(item->title); fputs("</a></p>", stdout);
    fputs("<header class=\"hero\"><h1>How this page changed</h1><p class=\"lede\">", stdout); html(item->href); fputs("</p></header>", stdout);
    if (have) {
        char *text = malloc(262144), err[160];
        bf_json_doc_t *doc = text && world_body_for_twin(twin, text, 262144) ? bf_json_parse_str(text, err, sizeof(err)) : NULL;
        const bf_json_node_t *rows = doc ? bf_json_root(doc) : NULL, *row;
        int count = 0, shown = 0, i;
        const bf_json_node_t *list[128];
        if (rows && rows->type == BF_JSON_ARRAY)
            for (row = bf_json_child_first(doc, rows); row && count < 128; row = bf_json_child_next(doc, row))
                if (row->type == BF_JSON_ARRAY) list[count++] = row;
        if (count > 1) {
            char first[16] = "", last[16] = "", when[32];
            page_json_text(bf_json_arr_get(doc, list[1], 0), 1, first, sizeof(first));
            page_json_text(bf_json_arr_get(doc, list[count - 1], 0), 1, last, sizeof(last));
            printf("<p class=\"narrowed\">The Internet Archive saw this page's content change %d time%s between %.4s and %.4s.</p>", count - 1, count == 2 ? "" : "s", first, last);
            fputs("<ol class=\"results changes\">", stdout);
            for (i = count - 1; i >= 1 && shown < 60; i--, shown++) {
                char stamp[16] = "", status[8] = "";
                page_json_text(bf_json_arr_get(doc, list[i], 0), 1, stamp, sizeof(stamp));
                page_json_text(bf_json_arr_get(doc, list[i], 1), 1, status, sizeof(status));
                if (strlen(stamp) != 14 || strspn(stamp, "0123456789") != 14) continue;
                snprintf(when, sizeof(when), "%.4s-%.2s-%.2s %.2s:%.2s", stamp, stamp + 4, stamp + 6, stamp + 8, stamp + 10);
                fputs("<li><a class=\"result\" rel=\"noopener\" target=\"_blank\" href=\"https://web.archive.org/web/", stdout); html(stamp); fputs("/", stdout); html(item->href);
                fputs("\"><span class=\"kind\">", stdout); page_glyph("text"); fputs("</span><span class=\"what\"><b>", stdout);
                fputs(i == 1 ? "First seen" : !strcmp(status, "200") || !strcmp(status, "-") ? "Content changed" : "Answered differently", stdout);
                fputs("</b><span class=\"by\"><span>", stdout); html(when); fputs(" UTC</span>", stdout);
                if (status[0] && strcmp(status, "200") && strcmp(status, "-")) { fputs("<span class=\"via\">answered ", stdout); html(status); fputs("</span>", stdout); }
                fputs("</span></span></a></li>", stdout);
            }
            fputs("</ol>", stdout);
        } else {
            fputs("<p class=\"narrowed\">The Internet Archive has no earlier copies of this page.</p>", stdout);
        }
        if (doc) bf_json_free(doc);
        free(text);
    } else if (fresh == -1) {
        fputs("<p class=\"narrowed\">The Internet Archive did not answer when this was looked up in the last hour. Try again later.</p>", stdout);
    } else {
        fputs("<p class=\"narrowed\">Its history has not been looked up yet.</p>", stdout);
    }
    if (!fresh) {
        fputs("<form class=\"verb-form\" method=\"post\" action=\"/o/changes\"><input type=\"hidden\" name=\"item\" value=\"", stdout);
        html(source); fputs("/", stdout); html(key);
        fputs("\"><button class=\"verb primary\" type=\"submit\">", stdout); page_glyph("search"); fputs(have ? "<span>Look again</span>" : "<span>Look up its history</span>", stdout);
        fputs("</button></form>", stdout);
    }
    fputs("<footer class=\"src\">Captures are what the Internet Archive's crawler saw at those moments, not what every visitor saw or what is true.</footer></main></body></html>", stdout);
    free(template); free(material); sqlite3_close(db);
    return 1;
}

/* ------------------------------------------------------------ about a thing
 *
 * One identity, every report the estate keeps about it: the thing itself as
 * its sources describe it, pictures that depict it, where it is, and the other
 * reports -- each still carrying its own source.  The page is organized around
 * the thing, not around who published it.  An identity no retained report
 * names has no page. */
static int emit_about_page(const char *root, const char *target, const char *host, const char *bearer, const char *auth_db) {
    const char *ident = target + 7; char path[PATH_MAX], actor[64] = "", *template = NULL, *head_end; size_t n = strlen(ident);
    char publisher_sources[2048], publisher_name[256], atlas_target[64];
    PageMaterial *material = NULL, *scratch = NULL, *pictures = NULL; sqlite3 *db = NULL; int named, k;
    unsigned char sequence[BF_PAGE_SCENE_GRAMMAR_COUNT]; size_t sequence_count = 0, r;
    char scene_why[256] = "";
    static const char *const roles[] = {"lead", "wall", "places", "latest", "more"};
    (void)host;
    if (strncmp(target, "/about/", 7) || !n || n > 40 || strspn(ident, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-") != n) return 0;
    if (snprintf(path, sizeof(path), "%s/surfaces/world.html", root) >= (int)sizeof(path)) return 0;
    page_offered_a_start = 0;
    if (!(db = cms_open_ro()) || !(material = calloc(1, sizeof(*material))) || !(scratch = malloc(sizeof(*scratch))) || !(pictures = calloc(1, sizeof(*pictures)))) goto out;
    named = page_collect_identity(db, ident, 0, material, scratch);
    {
        /* An organization with its own voice is named by the row that reads it. */
        sqlite3_stmt *pub = NULL;
        publisher_sources[0] = '\0'; publisher_name[0] = '\0';
        if (sqlite3_prepare_v2(db, "SELECT source,COALESCE(publisher_name,'') FROM internet_source WHERE status='published' AND publisher_ident=?1 ORDER BY id LIMIT 8", -1, &pub, NULL) == SQLITE_OK) {
            sqlite3_bind_text(pub, 1, ident, -1, SQLITE_TRANSIENT);
            while (sqlite3_step(pub) == SQLITE_ROW) {
                size_t used = strlen(publisher_sources);
                snprintf(publisher_sources + used, sizeof(publisher_sources) - used, "%s%s", used ? "," : "", sqlite3_column_text(pub, 0));
                if (!publisher_name[0]) snprintf(publisher_name, sizeof(publisher_name), "%s", sqlite3_column_text(pub, 1));
            }
        }
        sqlite3_finalize(pub);
    }
    if (!named && !publisher_sources[0]) {
        printf("HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\nNothing here names that.\n");
        goto done;
    }
    if (page_collect_identity(db, ident, 1, pictures, scratch)) {
        PageStream *from = &pictures->stream[BF_PAGE_MATERIAL_PHOTO], *to = &material->stream[BF_PAGE_MATERIAL_PHOTO];
        for (k = 0; k < from->count && to->count < PAGE_STREAM_MAX; k++) to->item[to->count++] = from->item[k];
        from = &pictures->stream[BF_PAGE_MATERIAL_PLACE]; to = &material->stream[BF_PAGE_MATERIAL_PLACE];
        for (k = 0; k < from->count && to->count < PAGE_STREAM_MAX; k++) to->item[to->count++] = from->item[k];
    }
    /* The thing itself leads: a named report with a picture is also its lead story. */
    {
        PageStream *pub = &material->stream[BF_PAGE_MATERIAL_PUBLICATION], *photo = &material->stream[BF_PAGE_MATERIAL_PHOTO];
        int kept = 0;
        for (k = 0; k < photo->count; k++) {
            if (!strcmp(photo->item[k].ident, ident)) {
                if (pub->count < PAGE_STREAM_MAX) { pub->item[pub->count] = photo->item[k]; pub->item[pub->count].kind = BF_PAGE_MATERIAL_PUBLICATION; pub->count++; }
            } else photo->item[kept++] = photo->item[k];   /* pictures of it, not the thing's own picture again */
        }
        photo->count = kept;
    }
    /* What this subject is decides how it reads: the compiler asks the material
     * itself which rhythms it argues for.  Where a subject has too little for
     * any of them, the authored order still carries what there is. */
    page_scene_in_depth = 1;
    sequence_count = page_compile_scene(material, sequence, sizeof(sequence), scene_why, sizeof(scene_why), time(NULL));
    page_scene_in_depth = 0;
    if (!sequence_count)
        for (r = 0; r < sizeof(roles) / sizeof(roles[0]); r++)
            for (k = 0; k < (int)BF_PAGE_REGION_COUNT; k++)
                if (!strcmp(bf_page_regions[k].role, roles[r])) { sequence[sequence_count++] = (unsigned char)k; break; }
    page_session_actor(bearer, auth_db, actor, sizeof(actor));
    if (!page_open_template(path, &template, &head_end)) goto out;
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: %s\r\nVary: Host, Cookie\r\nConnection: close\r\n\r\n",
           actor[0] ? "private, no-store" : "public, max-age=60, stale-while-revalidate=300");
    {
        const PageItem *first = &material->stream[BF_PAGE_MATERIAL_MIX].item[0];
        const char *title = named ? first->title : publisher_name;
        page_emit_head(template, head_end, title, named && first->body[0] ? first->body : "Everything kept about it, from each of its sources.");
        fputs("</head><body class=\"atlas item-page\" data-character=\"atlas\"><main class=\"wrap\">", stdout);
        { PageDocument nav = {0}; nav.actor = actor[0] ? actor : NULL; nav.self_path = target; page_emit_nav(&nav); }
        fputs("<header class=\"hero\"><h1>", stdout); html(title); fputs("</h1><p class=\"lede\">", stdout);
        if (named) printf("Everything the estate keeps about it: %d report%s from its sources", named, named == 1 ? "" : "s");
        else fputs("Everything the estate keeps about it, including what it publishes itself", stdout);
        if (pictures->stream[BF_PAGE_MATERIAL_PHOTO].count) printf(" and %d picture%s of it", pictures->stream[BF_PAGE_MATERIAL_PHOTO].count, pictures->stream[BF_PAGE_MATERIAL_PHOTO].count == 1 ? "" : "s");
        fputs(".</p>", stdout);
        {
            /* A publisher this estate addresses has its own page here. */
            EstateReferenceStores stores = {0}; EstateReference self;
            estate_reference_open(&stores);
            if (estate_reference_resolve(&stores, ident, &self) && strncmp(self.url, "/about/", 7)) {
                fputs("<p class=\"jump\"><a href=\"", stdout); html(self.url); fputs("\">", stdout);
                html(self.name[0] ? self.name : title); fputs(" at its own address</a></p>", stdout);
            }
            estate_reference_close(&stores);
        }
        fputs("</header>", stdout);
        if (scene_why[0]) { fputs("<meta name=\"bonfyre:scene\" content=\"", stdout); html(scene_why); fputs("\">", stdout); }
        if (sequence_count) {
            char look[64] = "";
            page_emit_profile(db, ident, bf_page_regions[sequence[0]].material, look, sizeof(look));
            if (look[0]) { fputs("<meta name=\"bonfyre:look\" content=\"", stdout); html(look); fputs("\">", stdout); }
        }
    }
    {
        PageEmitContext ctx = {.route = "/search", .now = time(NULL), .db = db, .surface_scene = NULL};
        const PageItem *lead = &material->stream[BF_PAGE_MATERIAL_MIX].item[0];
        if (named) page_emit_regions(material, sequence, sequence_count, &ctx);
        if (publisher_sources[0]) {
            /* What it says itself, newest first. */
            PageMaterial *own = calloc(1, sizeof(*own));
            if (own) {
                page_collect_stories(db, publisher_sources, own);
                if (own->stream[BF_PAGE_MATERIAL_MIX].count) {
                    int latest = -1, take;
                    for (k = 0; k < (int)BF_PAGE_REGION_COUNT; k++) if (!strcmp(bf_page_regions[k].role, "latest")) latest = k;
                    take = own->stream[BF_PAGE_MATERIAL_MIX].count < 12 ? own->stream[BF_PAGE_MATERIAL_MIX].count : 12;
                    if (latest >= 0) {
                        fputs("<section class=\"region region-latest\" aria-label=\"In its own words\"><h2 class=\"region-label\">In its own words</h2>", stdout);
                        page_emit_region(&bf_page_regions[latest], &own->stream[BF_PAGE_MATERIAL_MIX], take, &ctx);
                        fputs("</section>", stdout);
                    }
                }
                free(own);
            }
        }
        if (publisher_sources[0]) {
            /* Who its pieces name, how often, and every way it publishes, as
             * material the scene compiler composes like any other. */
            PageMaterial *relations = calloc(1, sizeof(*relations));
            if (relations) {
                EstateReferenceStores stores = {0};
                unsigned char rseq[BF_PAGE_SCENE_GRAMMAR_COUNT]; char rwhy[256] = ""; size_t rcount;
                estate_reference_open(&stores);
                depth_publisher_relations(publisher_sources, publisher_name[0] ? publisher_name : ident, &stores, relations);
                estate_reference_close(&stores);
                page_scene_in_depth = 1;
                rcount = page_compile_scene(relations, rseq, sizeof(rseq), rwhy, sizeof(rwhy), ctx.now);
                page_scene_in_depth = 0;
                if (rcount) page_emit_regions(relations, rseq, rcount, &ctx);
                free(relations);
            }
        }
        snprintf(atlas_target, sizeof(atlas_target), "qid:%s", ident);
        page_emit_related_items(db, "happens_at", atlas_target, 0, "Happening here", 12, ctx.now);
        if (named && lead->placed) page_emit_around(db, lead->lat, lead->lon, NULL, NULL, ident, 3.0, ctx.now);
        page_emit_related_items(db, "mentions", atlas_target, 0, "Mentioned in", 12, ctx.now);
        page_emit_related_items(db, "same_subject_as", atlas_target, 0, "Also listed as", 6, ctx.now);
        if (named && lead->placed && material->stream[BF_PAGE_MATERIAL_PLACE].count < 3) {
            /* One place is still somewhere: its own street map. */
            PageStream *one = calloc(1, sizeof(*one));
            if (one) {
                one->count = 1; one->item[0] = *lead;
                fputs("<section class=\"region region-places\" aria-label=\"Where\"><h2 class=\"region-label\">Where</h2><figure class=\"map where\">", stdout);
                page_emit_map_plate(one, 1, 0);
                fputs("</figure></section>", stdout);
                free(one);
            }
        }
    }
    fputs("</main></body></html>", stdout);
done:
    free(template); free(material); free(scratch); free(pictures);
    sqlite3_close(db);
    return 1;
out:
    free(template); free(material); free(scratch); free(pictures);
    if (db) sqlite3_close(db);
    return 0;
}

/* The shelf is a publication rack, not a generic index.  It shares the same
 * native boundary as an addressed World so it cannot fall back to the retired
 * browser reader merely because no single World was selected. */
static int emit_native_world_shelf(const char *path) {
    FILE *f = NULL; long size; char *template = NULL, *head_end;
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int count = 0;
    if (!path || !(db = cms_open_ro()) ||
        sqlite3_prepare_v2(db,
          "SELECT world,title,character,COALESCE(grammars,'') FROM world "
          "WHERE status='published' ORDER BY id DESC", -1, &st, NULL) != SQLITE_OK) goto out;
    if (!(f = fopen(path, "rb")) || fseek(f, 0, SEEK_END) || (size = ftell(f)) < 1 ||
        size > 2 * 1024 * 1024 || fseek(f, 0, SEEK_SET) ||
        !(template = malloc((size_t)size + 1)) ||
        fread(template, 1, (size_t)size, f) != (size_t)size) goto out;
    fclose(f); f = NULL; template[size] = '\0';
    if (!(head_end = strstr(template, "</head>"))) goto out;
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
           "Cache-Control: public, max-age=60, stale-while-revalidate=300\r\n"
           "Vary: Host, Accept\r\nX-Bonfyre-World-Projection: native\r\nConnection: close\r\n\r\n");
    fwrite(template, 1, (size_t)(head_end - template + strlen("</head>")), stdout);
    fputs("<body class=\"shelf\"><main class=\"wrap\"><nav class=\"bar\" aria-label=\"World navigation\"><a class=\"home\" href=\"/\">&larr; Aurekai</a><span class=\"ch\">worlds</span></nav><header class=\"hero\"><h1>Things that became worlds.</h1><p class=\"lede\">Each is a temporary publication over an addressed subject, made from observed material and existing interaction grammars.</p></header><section class=\"rack\">", stdout);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *world = (const char *)sqlite3_column_text(st, 0);
        const char *title = (const char *)sqlite3_column_text(st, 1);
        const char *family = (const char *)sqlite3_column_text(st, 2);
        const char *grammars = (const char *)sqlite3_column_text(st, 3);
        char grammar_copy[1024], *part;
        if (!world || !title || !family || !studio_safe_slug_arg(world) || !studio_safe_slug_arg(family)) continue;
        fputs("<a class=\"cover c-", stdout); html(family); fputs("\" href=\"/w/", stdout); html(world); fputs("\"><div class=\"t\">", stdout); html(title); fputs("</div><div><div class=\"m\">", stdout); html(family); fputs(" publication</div><div class=\"g\">", stdout);
        snprintf(grammar_copy, sizeof(grammar_copy), "%s", grammars ? grammars : "");
        part = grammar_copy;
        while (part && *part) {
            char *comma = strchr(part, ','); if (comma) *comma = '\0';
            while (*part && isspace((unsigned char)*part)) part++;
            if (*part && world_grammar_route(part)) { fputs("<i>", stdout); html(part); fputs("</i>", stdout); }
            part = comma ? comma + 1 : NULL;
        }
        fputs("</div></div></a>", stdout); count++;
    }
    if (!count) fputs("<p class=\"note\">No World is published yet.</p>", stdout);
    fputs("</section></main></body></html>", stdout);
    sqlite3_finalize(st); sqlite3_close(db); free(template); return 1;
out:
    if (f) fclose(f);
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    free(template);
    return 0;
}

typedef struct { const char *path, *target, *query; } BfWorldProjectionEmit;
static int emit_cached_world_body(void *opaque) {
    BfWorldProjectionEmit *world = opaque;
    return emit_native_world_page(world->path, world->target, world->query);
}
#endif /* BONFYRE_PAGE_ROUTES_AND_EMITTERS_H */
