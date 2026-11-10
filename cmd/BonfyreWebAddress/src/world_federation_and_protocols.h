#ifndef BONFYRE_WORLD_FEDERATION_AND_PROTOCOLS_H
#define BONFYRE_WORLD_FEDERATION_AND_PROTOCOLS_H
/* Extracted verbatim from main.c lines 18033-19093 (mechanical split, no logic changes). */
static int world_federation_path(const char *target, char *world, size_t world_sz,
                                 const char **leaf) {
    const char *p, *slash;
    size_t n;
    if (!target || strncmp(target, "/w/", 3) || !(p = target + 3) ||
        !(slash = strchr(p, '/'))) return 0;
    n = (size_t)(slash - p);
    if (!n || n >= world_sz) return 0;
    memcpy(world, p, n); world[n] = '\0';
    if (!studio_safe_slug_arg(world)) return 0;
    *leaf = slash + 1;
    return !strcmp(*leaf, "manifest.json") || !strcmp(*leaf, "feed.atom") ||
           !strcmp(*leaf, "ap/outbox") || !strcmp(*leaf, "events");
}

static int world_federation_load(sqlite3 *db, const char *world, BfWorldFederation *out) {
    sqlite3_stmt *st = NULL;
    if (!db || !world || !out || !cms_type_registered(db, "world") ||
        sqlite3_prepare_v2(db,
          "SELECT title,character,lede,COALESCE(subject,''),sources,COALESCE(grammars,''),cannot_infer "
          "FROM world WHERE world=?1 AND status='published' ORDER BY id DESC LIMIT 1",
          -1, &st, NULL) != SQLITE_OK) return 0;
    memset(out, 0, sizeof(*out));
    sqlite3_bind_text(st, 1, world, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); return 0; }
    snprintf(out->world, sizeof(out->world), "%s", world);
#define WFCOPY(dst,col) snprintf((dst), sizeof(dst), "%s", sqlite3_column_text(st,col) ? (const char *)sqlite3_column_text(st,col) : "")
    WFCOPY(out->title,0); WFCOPY(out->character,1); WFCOPY(out->lede,2); WFCOPY(out->subject,3);
    WFCOPY(out->sources,4); WFCOPY(out->grammars,5); WFCOPY(out->cannot_infer,6);
#undef WFCOPY
    sqlite3_finalize(st); st = NULL;
    snprintf(out->source_collections, sizeof(out->source_collections), "[]");
    snprintf(out->capability_programs, sizeof(out->capability_programs), "[]");
    if (!out->subject[0] || !cms_type_registered(db, "site_presentation") ||
        !cms_type_registered(db, "site_program")) return 1;
    if (sqlite3_prepare_v2(db,
          "SELECT p.site_id,p.program_id,p.source_collections,p.capability_programs "
          "FROM site_program p JOIN site_presentation s ON s.site_id=p.site_id "
          "WHERE s.subject_address=?1 AND s.status='published' AND p.status='published' "
          "ORDER BY p.id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, out->subject, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        snprintf(out->site_id, sizeof(out->site_id), "%s", sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "");
        snprintf(out->program_id, sizeof(out->program_id), "%s", sqlite3_column_text(st,1) ? (const char *)sqlite3_column_text(st,1) : "");
        snprintf(out->source_collections, sizeof(out->source_collections), "%s", sqlite3_column_text(st,2) ? (const char *)sqlite3_column_text(st,2) : "[]");
        snprintf(out->capability_programs, sizeof(out->capability_programs), "%s", sqlite3_column_text(st,3) ? (const char *)sqlite3_column_text(st,3) : "[]");
    }
    sqlite3_finalize(st);
    return 1;
}

static void emit_csv_json_array(const char *raw) {
    char copy[4096], *part, *save = NULL; int first = 1;
    snprintf(copy, sizeof(copy), "%s", raw ? raw : "");
    putchar('[');
    for (part = strtok_r(copy, ",", &save); part; part = strtok_r(NULL, ",", &save)) {
        char *end;
        while (*part && isspace((unsigned char)*part)) part++;
        end = part + strlen(part); while (end > part && isspace((unsigned char)end[-1])) *--end = '\0';
        if (!*part) continue;
        if (!first) { putchar(','); } first = 0; json(part);
    }
    putchar(']');
}

static int world_publications_prepare(sqlite3 *db, const char *sources, sqlite3_stmt **out) {
    const char *sql =
      "WITH RECURSIVE src(rest,item) AS ( SELECT trim(?1),'' UNION ALL "
      "SELECT CASE WHEN instr(rest,',')>0 THEN ltrim(substr(rest,instr(rest,',')+1)) ELSE '' END, "
      "trim(CASE WHEN instr(rest,',')>0 THEN substr(rest,1,instr(rest,',')-1) ELSE rest END) FROM src WHERE rest<>'' ) "
      "SELECT occurrence_id,source_ref,COALESCE(canonical_href,''),title,COALESCE(summary,''),COALESCE(observed_at,''),COALESCE(published_at,''),cannot_infer "
      "FROM publication_occurrence WHERE source_ref IN (SELECT item FROM src WHERE item<>'') AND status='published' AND visibility='public' AND assertion_state='OBSERVED' "
      "ORDER BY observed_at DESC,id DESC LIMIT 50";
    if (!db || !out || !cms_type_registered(db, "publication_occurrence") || sqlite3_prepare_v2(db, sql, -1, out, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(*out, 1, sources ? sources : "", -1, SQLITE_TRANSIENT);
    return 1;
}

static int cms_json_array_valid(sqlite3 *db, const char *raw) {
    sqlite3_stmt *st = NULL; int ok = 0;
    if (!db || !raw || sqlite3_prepare_v2(db,
        "SELECT json_valid(?1) AND json_type(?1)='array'", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, raw, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) ok = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return ok;
}

static void emit_cms_json_array(sqlite3 *db, const char *raw) {
    if (cms_json_array_valid(db, raw)) fputs(raw, stdout); else fputs("[]", stdout);
}

static void emit_world_source_collection_descriptors(sqlite3 *db, const char *ids) {
    sqlite3_stmt *st = NULL; int first = 1;
    if (!db || !cms_type_registered(db, "source_collection") || !cms_json_array_valid(db, ids) ||
        sqlite3_prepare_v2(db,
          "SELECT collection_id,subject_address,source_selector,material_kind,refresh_policy "
          "FROM source_collection WHERE status='published' "
          "AND collection_id IN (SELECT value FROM json_each(?1)) ORDER BY collection_id",
          -1, &st, NULL) != SQLITE_OK) { fputs("[]", stdout); return; }
    sqlite3_bind_text(st, 1, ids, -1, SQLITE_TRANSIENT);
    putchar('[');
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (!first) { putchar(','); } first = 0;
        fputs("{\"collection_id\":",stdout); json((const char*)sqlite3_column_text(st,0));
        fputs(",\"subject_address\":",stdout); json((const char*)sqlite3_column_text(st,1));
        fputs(",\"source_selector\":",stdout); fputs((const char*)sqlite3_column_text(st,2),stdout);
        fputs(",\"material_kind\":",stdout); fputs((const char*)sqlite3_column_text(st,3),stdout);
        fputs(",\"refresh_policy\":",stdout); json((const char*)sqlite3_column_text(st,4));
        putchar('}');
    }
    putchar(']'); sqlite3_finalize(st);
}

static void emit_world_capability_descriptors(sqlite3 *db, const char *ids) {
    sqlite3_stmt *st = NULL; int first = 1;
    if (!db || !cms_type_registered(db, "capability_program") || !cms_json_array_valid(db, ids) ||
        sqlite3_prepare_v2(db,
          "SELECT program_id,application_kind,action,consumes,produces,carrier_candidates,"
          "capability_requirements,authority_requirement,transition_conditions,failure_projection "
          "FROM capability_program WHERE status='published' "
          "AND program_id IN (SELECT value FROM json_each(?1)) ORDER BY program_id",
          -1, &st, NULL) != SQLITE_OK) { fputs("[]", stdout); return; }
    sqlite3_bind_text(st, 1, ids, -1, SQLITE_TRANSIENT);
    putchar('[');
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (!first) { putchar(','); } first = 0;
        fputs("{\"program_id\":",stdout); json((const char*)sqlite3_column_text(st,0));
        fputs(",\"application_kind\":",stdout); json((const char*)sqlite3_column_text(st,1));
        fputs(",\"action\":",stdout); json((const char*)sqlite3_column_text(st,2));
        fputs(",\"consumes\":",stdout); fputs((const char*)sqlite3_column_text(st,3),stdout);
        fputs(",\"produces\":",stdout); fputs((const char*)sqlite3_column_text(st,4),stdout);
        fputs(",\"carrier_candidates\":",stdout); fputs((const char*)sqlite3_column_text(st,5),stdout);
        fputs(",\"capability_requirements\":",stdout); fputs((const char*)sqlite3_column_text(st,6),stdout);
        fputs(",\"authority_requirement\":",stdout); json((const char*)sqlite3_column_text(st,7));
        fputs(",\"transition_conditions\":",stdout); fputs((const char*)sqlite3_column_text(st,8),stdout);
        fputs(",\"failure_projection\":",stdout); json((const char*)sqlite3_column_text(st,9));
        putchar('}');
    }
    putchar(']'); sqlite3_finalize(st);
}

static size_t world_federation_dependencies(sqlite3 *db, const BfWorldFederation *world,
                                            char storage[][640], const char **out,
                                            size_t cap);

static int emit_world_federation_manifest(sqlite3 *db, const BfWorldFederation *world,
                                           const char *host) {
    char dep_storage[64][640], origin[512];
    const char *deps[64];
    size_t dep_count = world_federation_dependencies(db, world, dep_storage, deps, 64);
    snprintf(origin, sizeof(origin), "https://%s", host && *host ? host : "aurekai.life");
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n"
           "Cache-Control: public, max-age=30\r\nVary: Host, Accept\r\nConnection: close\r\n\r\n");
    fputs("{\"schema\":\"bonfyre.world-federation.v1\",\"world\":",stdout); json(world->world);
    fputs(",\"subject_address\":",stdout); json(world->subject);
    fputs(",\"title\":",stdout); json(world->title);
    fputs(",\"character\":",stdout); json(world->character);
    fputs(",\"human\":{\"html\":",stdout);
    { char href[768]; snprintf(href,sizeof(href),"%s/w/%s",origin,world->world); json(href); }
    fputs(",\"grammars\":",stdout); emit_csv_json_array(world->grammars); putchar('}');
    fputs(",\"publication\":{\"source_refs\":",stdout); emit_csv_json_array(world->sources);
    fputs(",\"atom\":",stdout);
    { char href[768]; snprintf(href,sizeof(href),"%s/w/%s/feed.atom",origin,world->world); json(href); }
    fputs(",\"activitypub\":{\"actor\":",stdout);
    { char href[768]; snprintf(href,sizeof(href),"%s/ap/actor",origin); json(href); }
    fputs(",\"outbox\":",stdout);
    { char href[768]; snprintf(href,sizeof(href),"%s/w/%s/ap/outbox",origin,world->world); json(href); }
    fputs("},\"cloudevents\":",stdout);
    { char href[768]; snprintf(href,sizeof(href),"%s/w/%s/events",origin,world->world); json(href); }
    putchar('}');
    fputs(",\"site_program\":{\"site_id\":",stdout); json(world->site_id);
    fputs(",\"program_id\":",stdout); json(world->program_id);
    fputs(",\"source_collections\":",stdout); emit_cms_json_array(db, world->source_collections);
    fputs(",\"capability_programs\":",stdout); emit_cms_json_array(db, world->capability_programs); putchar('}');
    fputs(",\"source_collection_descriptors\":",stdout); emit_world_source_collection_descriptors(db, world->source_collections);
    fputs(",\"capability_program_descriptors\":",stdout); emit_world_capability_descriptors(db, world->capability_programs);
    fputs(",\"dependency_frontier\":[",stdout);
    for (size_t i=0;i<dep_count;i++) { if(i)putchar(','); json(deps[i]); }
    fputs("],\"truth\":\"derived_projection_over_published_owner_state\",\"cannot_infer\":[",stdout);
    json(world->cannot_infer);
    fputs(",\"feed publication -> authorship, endorsement or evidence\",\"carrier candidate -> provider admission, authority, payment or effect\"]}\n",stdout);
    return 1;
}

static int emit_world_atom(sqlite3 *db, const BfWorldFederation *world, const char *host) {
    sqlite3_stmt *st = NULL;
    char origin[512], stamp[64], et[1536], es[4096], eh[2048], eid[1536], esource[512];
    const char *hub = getenv("BONFYRE_WEBSUB_HUB");
    int rc;
    snprintf(origin, sizeof(origin), "https://%s", host && *host ? host : "aurekai.life");
    if (!world_publications_prepare(db, world->sources, &st)) return 0;
    rc = sqlite3_step(st);
    if (rc == SQLITE_ROW && sqlite3_column_text(st,5))
        snprintf(stamp,sizeof(stamp),"%s",(const char*)sqlite3_column_text(st,5));
    else iso_utc_now(stamp,sizeof(stamp));
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/atom+xml; charset=utf-8\r\n"
           "Cache-Control: public, max-age=30\r\nVary: Host, Accept\r\n");
    printf("Link: <%s/w/%s/feed.atom>; rel=\"self\", <%s/w/%s/manifest.json>; rel=\"describedby\"",
           origin,world->world,origin,world->world);
    if (hub && *hub) printf(", <%s>; rel=\"hub\"",hub);
    printf("\r\nConnection: close\r\n\r\n<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
           "<feed xmlns=\"http://www.w3.org/2005/Atom\">\n");
    html_escape(world->title,et,sizeof(et));
    html_escape(world->lede,es,sizeof(es));
    printf("<title>%s</title><id>%s/w/%s</id><link rel=\"self\" href=\"%s/w/%s/feed.atom\"/>"
           "<link href=\"%s/w/%s\"/><updated>%s</updated><subtitle>%s</subtitle>\n",
           et,origin,world->world,origin,world->world,origin,world->world,stamp,es);
    while (rc == SQLITE_ROW) {
        const char *id=(const char*)sqlite3_column_text(st,0), *source=(const char*)sqlite3_column_text(st,1);
        const char *href=(const char*)sqlite3_column_text(st,2), *title=(const char*)sqlite3_column_text(st,3);
        const char *summary=(const char*)sqlite3_column_text(st,4), *observed=(const char*)sqlite3_column_text(st,5);
        html_escape(title?title:"",et,sizeof(et)); html_escape(summary?summary:"",es,sizeof(es));
        html_escape(href?href:"",eh,sizeof(eh)); html_escape(id?id:"",eid,sizeof(eid)); html_escape(source?source:"",esource,sizeof(esource));
        printf("<entry><title>%s</title><id>urn:bonfyre:publication:%s</id>",et,eid);
        if (eh[0]) printf("<link href=\"%s\"/>",eh);
        printf("<updated>%s</updated><category term=\"%s\"/><summary>%s</summary></entry>\n",
               observed&&*observed?observed:stamp,esource,es);
        rc = sqlite3_step(st);
    }
    fputs("</feed>\n",stdout); sqlite3_finalize(st); return 1;
}

static int emit_world_activitypub(sqlite3 *db, const BfWorldFederation *world, const char *host) {
    sqlite3_stmt *st = NULL; int first = 1;
    char origin[512];
    snprintf(origin, sizeof(origin), "https://%s", host && *host ? host : "aurekai.life");
    if (!world_publications_prepare(db, world->sources, &st)) return 0;
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/activity+json\r\n"
           "Cache-Control: public, max-age=30\r\nVary: Host, Accept\r\nConnection: close\r\n\r\n");
    fputs("{\"@context\":\"https://www.w3.org/ns/activitystreams\",\"type\":\"OrderedCollection\",\"id\":",stdout);
    { char href[768]; snprintf(href,sizeof(href),"%s/w/%s/ap/outbox",origin,world->world); json(href); }
    fputs(",\"attributedTo\":",stdout);
    { char actor[768]; snprintf(actor,sizeof(actor),"%s/ap/actor",origin); json(actor); }
    fputs(",\"summary\":",stdout); json(world->lede);
    fputs(",\"orderedItems\":[",stdout);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id=(const char*)sqlite3_column_text(st,0), *href=(const char*)sqlite3_column_text(st,2);
        const char *title=(const char*)sqlite3_column_text(st,3), *observed=(const char*)sqlite3_column_text(st,5);
        char activity[1024], fallback[1024];
        if (!first) { putchar(','); } first = 0;
        snprintf(activity,sizeof(activity),"%s/w/%s/ap/outbox#%s",origin,world->world,id?id:"occurrence");
        snprintf(fallback,sizeof(fallback),"%s/w/%s#%s",origin,world->world,id?id:"occurrence");
        fputs("{\"type\":\"Announce\",\"id\":",stdout); json(activity);
        fputs(",\"actor\":",stdout);
        { char actor[768]; snprintf(actor,sizeof(actor),"%s/ap/actor",origin); json(actor); }
        fputs(",\"object\":",stdout); json(href&&*href?href:fallback);
        fputs(",\"published\":",stdout); json(observed?observed:"");
        fputs(",\"summary\":",stdout); json(title?title:"");
        putchar('}');
    }
    fputs("],\"forbidden_inference\":\"Announce projects an observed foreign publication; it does not make Aurekai its author, endorser, evidence owner or rights holder\"}\n",stdout);
    sqlite3_finalize(st); return 1;
}

static int emit_world_cloudevents(sqlite3 *db, const BfWorldFederation *world, const char *host) {
    sqlite3_stmt *st = NULL; int first = 1;
    char origin[512], source[768];
    snprintf(origin, sizeof(origin), "https://%s", host && *host ? host : "aurekai.life");
    snprintf(source, sizeof(source), "%s/w/%s", origin, world->world);
    if (!world_publications_prepare(db, world->sources, &st)) return 0;
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/cloudevents-batch+json\r\n"
           "Cache-Control: public, max-age=30\r\nVary: Host, Accept\r\nConnection: close\r\n\r\n[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id=(const char*)sqlite3_column_text(st,0), *source_ref=(const char*)sqlite3_column_text(st,1);
        const char *href=(const char*)sqlite3_column_text(st,2), *title=(const char*)sqlite3_column_text(st,3);
        const char *observed=(const char*)sqlite3_column_text(st,5);
        if (!first) { putchar(','); } first = 0;
        fputs("{\"specversion\":\"1.0\",\"id\":",stdout); json(id?id:"");
        fputs(",\"source\":",stdout); json(source);
        fputs(",\"type\":\"org.aurekai.world.publication.observed\",\"time\":",stdout); json(observed?observed:"");
        fputs(",\"datacontenttype\":\"application/json\",\"subject\":",stdout); json(world->subject);
        fputs(",\"data\":{\"source_ref\":",stdout); json(source_ref?source_ref:"");
        fputs(",\"title\":",stdout); json(title?title:"");
        fputs(",\"canonical_href\":",stdout); json(href?href:"");
        fputs(",\"truth\":\"observed_foreign_publication_not_aurekai_claim\"}}",stdout);
    }
    fputs("]\n",stdout); sqlite3_finalize(st); return 1;
}

static int serve_world_federation(const char *method, const char *target, const char *host) {
    char world_id[128]; const char *leaf = NULL; sqlite3 *db = NULL; BfWorldFederation world; int served = 0;
    if (strcmp(method, "GET") || !world_federation_path(target, world_id, sizeof(world_id), &leaf)) return 0;
    if (!(db = cms_open_ro())) { protocol_reply(503,"application/json","{\"error\":\"world_store_unavailable\"}\n"); return 1; }
    if (!world_federation_load(db, world_id, &world)) {
        sqlite3_close(db); protocol_reply(404,"application/json","{\"error\":\"world_not_found\"}\n"); return 1;
    }
    if (!strcmp(leaf,"manifest.json")) served = emit_world_federation_manifest(db,&world,host);
    else if (!strcmp(leaf,"feed.atom")) served = emit_world_atom(db,&world,host);
    else if (!strcmp(leaf,"ap/outbox")) served = emit_world_activitypub(db,&world,host);
    else if (!strcmp(leaf,"events")) served = emit_world_cloudevents(db,&world,host);
    sqlite3_close(db);
    if (!served) protocol_reply(503,"application/json","{\"error\":\"world_projection_unavailable\"}\n");
    return 1;
}

static int serve_protocols(const char *method, const char *target, const char *host,
                           const char *accept_hdr, const char *query) {
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    static char rows[262144];
    static char body[524288];
    char sql[8192];
    char territory[32];
    const char *h = host && *host ? host : "aurekai.life";
    int limit;

    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    if (strcmp(method, "GET")) return 0;
    if (serve_world_federation(method,target,h)) return 1;
    limit = protocol_limit(query);
    protocol_territory(query, territory, sizeof(territory));

    /* ---- index of every projection ------------------------------------- */
    if (!strcmp(target, "/protocols") || !strcmp(target, "/protocols/")) {
        if (wants_html(accept_hdr)) {
            snprintf(body, sizeof(body),
BF_PAGE_OPEN "Build on Bonf\xc3\xbfre" BF_PAGE_AFTER_TITLE
"<main><section class=\"hero wrap\" style=\"padding:56px 0 10px\">"
"<div class=\"eyebrow\"><i></i><span>Aurekai / build on this</span></div>"
"<h1>Bring your own <em>protocol.</em></h1>"
"<p class=\"lede\">The same subjects, priced capabilities and receipted observations, spoken in whichever "
"grammar you already use. Pick the door you came for.</p></section>"
/* The doors below are the fallback, not the content.
 *
 * HTMX replaces them on load with whatever is authored in the CMS for this
 * page. A reader with no JavaScript, or an unreachable store, still gets a
 * complete page rather than an empty frame -- which is why the compiled set
 * stays inside the swap target instead of being deleted once the fragment
 * existed. Editing a door is a CMS write; the compiled copy is what the site
 * degrades to, not what it serves. */
"<div class=\"wrap\"><div class=\"doors\" hx-get=\"/_surface/doors?page=protocols\" "
"hx-trigger=\"load\" hx-swap=\"innerHTML\">"

"<article class=\"door\"><p class=\"eyebrow\"><i></i>Bring in a source</p>"
"<h3>Observe the outside world</h3>"
"<p>Point us at a public URL. We check robots.txt before fetching, keep the exact bytes and a receipt, and "
"never promote the result to canonical truth. Images, fonts and themes come in the same way.</p>"
"<div class=\"ways\"><a class=\"way\" href=\"/studio/#foreign\">ForeignTwin observe</a>"
"<a class=\"way\" href=\"/studio/\">Assets &amp; themes</a></div></article>"

"<article class=\"door\"><p class=\"eyebrow\"><i></i>Buy or sell capability</p>"
"<h3>Machine payment</h3>"
"<p>Every capability answers HTTP 402 with x402 payment requirements until a payment verifies. Prices come "
"from the same service book the financial membrane reports, so a quote and a ledger never disagree.</p>"
"<div class=\"ways\"><a class=\"way\" href=\"/capability\">x402 catalogue</a>"
"<a class=\"way\" href=\"/mpp/quote\">MPP quote</a>"
"<a class=\"way\" href=\"/ucp/catalog\">UCP</a>"
"<a class=\"way\" href=\"/acp/products\">ACP</a>"
"<a class=\"way\" href=\"/beckn/search\">Beckn</a></div></article>"

"<article class=\"door\"><p class=\"eyebrow\"><i></i>Publish something</p>"
"<h3>Write once, reach every surface</h3>"
"<p>Content types describe their own fields. Publishing and readability are separate decisions, so a page can "
"be published and still answer 401 to a stranger.</p>"
"<div class=\"ways\"><a class=\"way\" href=\"/cms/\">Content Studio</a>"
"<a class=\"way\" href=\"/notes/\">Public notes</a></div></article>"

"<article class=\"door\"><p class=\"eyebrow\"><i></i>Wire an agent</p>"
"<h3>Agents act, not just read</h3>"
"<p>Ten studio tools over MCP, seven of them writes: register a client, open work, claim and complete a leased "
"mission, set a budget cap. An A2A agent card advertises the same skills.</p>"
"<div class=\"ways\"><a class=\"way\" href=\"/.well-known/agent.json\">A2A card</a>"
"<a class=\"way\" href=\"/mcp/resources/\">MCP</a>"
"<a class=\"way\" href=\"/events\">CloudEvents</a></div></article>"

"<article class=\"door\"><p class=\"eyebrow\"><i></i>Read it in your standard</p>"
"<h3>Property, place and provenance</h3>"
"<p>6.5 million receipted parcel observations, projected into whichever vocabulary your system already speaks. "
"A projection is not a listing, an appraisal, or a loan file.</p>"
"<div class=\"ways\"><a class=\"way\" href=\"/reso/Property\">RESO</a>"
"<a class=\"way\" href=\"/ogc\">OGC Features</a>"
"<a class=\"way\" href=\"/mismo/subject-property\">MISMO</a>"
"<a class=\"way\" href=\"/wot/thing\">W3C WoT</a>"
"<a class=\"way\" href=\"/epcis/events\">GS1 EPCIS</a></div></article>"

"<article class=\"door\"><p class=\"eyebrow\"><i></i>Somewhere social</p>"
"<h3>Follow and federate</h3>"
"<p>An ActivityPub actor and WebFinger record, so this world can be followed from software that already knows "
"how to follow things.</p>"
"<div class=\"ways\"><a class=\"way\" href=\"/ap/actor\">ActivityPub</a>"
"<a class=\"way\" href=\"/.well-known/webfinger?resource=acct:bonfyre@%s\">WebFinger</a>"
"<a class=\"way\" href=\"/cap/alerts\">CAP alerts</a></div></article>"

"</div>"
"<p class=\"lede\" style=\"margin-bottom:60px\">Every link here also answers JSON when a machine asks for it. "
"Where a collection is empty it says why, because nothing has been admitted to observe yet &mdash; an invented "
"alert or product movement would be worse than an honest empty list.</p></div></main>"
"<footer class=\"footer wrap\"><a class=\"wordmark\" href=\"/\">BONF<span>\xc5\xb8</span>RE</a>"
"<p>One semantic Matrix. Many carrier grammars.</p><a href=\"/protocols\">Machine view &#8599;</a></footer>"
"<script defer src=\"/assets/matrix-htmx.js\"></script>"
"<script src=\"/assets/matrix.js\"></script></body></html>", site_nav_html(), h);
            protocol_reply(200, "text/html; charset=utf-8", body);
            return 1;
        }
        snprintf(body, sizeof(body),
          "{\"seller\":\"%s\","
          "\"principle\":\"a protocol is a carrier grammar over one semantic Matrix, never a second owner of the data\","
          "\"projections\":["
            "{\"protocol\":\"x402\",\"role\":\"payment\",\"at\":\"/capability\"},"
            "{\"protocol\":\"MPP\",\"role\":\"machine payment quote\",\"at\":\"/mpp/quote\"},"
            "{\"protocol\":\"MCP\",\"role\":\"agent tools\",\"at\":\"/mcp/\"},"
            "{\"protocol\":\"A2A\",\"role\":\"agent-to-agent tasks\",\"at\":\"/.well-known/agent.json\"},"
            "{\"protocol\":\"CloudEvents 1.0\",\"role\":\"event envelope\",\"at\":\"/events\"},"
            "{\"protocol\":\"OGC API Features\",\"role\":\"geospatial\",\"at\":\"/ogc\"},"
            "{\"protocol\":\"RESO Web API\",\"role\":\"real estate\",\"at\":\"/reso/Property\"},"
            "{\"protocol\":\"MISMO\",\"role\":\"mortgage subject property\",\"at\":\"/mismo/subject-property\"},"
            "{\"protocol\":\"ActivityPub\",\"role\":\"social\",\"at\":\"/ap/actor\"},"
            "{\"protocol\":\"WebFinger\",\"role\":\"social discovery\",\"at\":\"/.well-known/webfinger\"},"
            "{\"protocol\":\"Beckn\",\"role\":\"local commerce\",\"at\":\"/beckn/search\"},"
            "{\"protocol\":\"UCP\",\"role\":\"shopping catalog\",\"at\":\"/ucp/catalog\"},"
            "{\"protocol\":\"ACP\",\"role\":\"agent commerce\",\"at\":\"/acp/products\"},"
            "{\"protocol\":\"CAP 1.2\",\"role\":\"alerting\",\"at\":\"/cap/alerts\"},"
            "{\"protocol\":\"GS1 EPCIS 2.0\",\"role\":\"product provenance\",\"at\":\"/epcis/events\"},"
            "{\"protocol\":\"W3C WoT\",\"role\":\"thing description\",\"at\":\"/wot/thing\"},"
            "{\"protocol\":\"OpenRTB 2.6\",\"role\":\"ad demand\",\"at\":\"/openrtb/bidrequest\"}"
          "],"
          "\"forbidden_inference\":\"a protocol projection is not a licence, a listing agreement, an appraisal, or an offer\"}\n", h);
        protocol_reply(200, "application/json; charset=utf-8", body);
        return 1;
    }

    /* ---- A2A agent card ------------------------------------------------ */
    if (!strcmp(target, "/.well-known/agent.json") || !strcmp(target, "/a2a/agent.json")) {
        static char skills[8192]; size_t sn = 0; int trunc = 0;
        for (int i = 0; i < A2A_SKILLS_N && !trunc; i++) {
            int w = snprintf(skills + sn, sizeof(skills) - sn,
                "%s{\"id\":\"%s\",\"name\":\"%s\",\"description\":\"%s\",\"tags\":%s}",
                i ? "," : "", A2A_SKILLS[i].id, A2A_SKILLS[i].name,
                A2A_SKILLS[i].description, A2A_SKILLS[i].tags);
            if (w < 0 || (size_t)w >= sizeof(skills) - sn) trunc = 1; else sn += (size_t)w;
        }
        if (trunc) {
            protocol_reply(503, "application/json",
                "{\"error\":\"card_overflow\",\"note\":\"a truncated agent card would advertise "
                "a skill list that is not the one this service holds\"}\n");
            return 1;
        }
        snprintf(body, sizeof(body),
          "{\"name\":\"Bonfyre\",\"description\":\"Receipted public-record estate, work graph and priced capabilities\","
          "\"url\":\"https://%s/a2a\",\"version\":\"1.0\","
          "\"capabilities\":{\"streaming\":false,\"pushNotifications\":false,\"stateTransitionHistory\":true},"
          "\"defaultInputModes\":[\"application/json\"],\"defaultOutputModes\":[\"application/json\"],"
          "\"skills\":[%s],"
          "\"forbidden_inference\":\"an advertised skill is not an agreement to perform it for any particular requester\"}\n",
          h, skills);
        protocol_reply(200, "application/json; charset=utf-8", body);
        return 1;
    }

    /* ---- WebFinger + ActivityPub actor --------------------------------- */
    if (!strncmp(target, "/.well-known/webfinger", 22)) {
        snprintf(body, sizeof(body),
          "{\"subject\":\"acct:bonfyre@%s\",\"aliases\":[\"https://%s/ap/actor\"],"
          "\"links\":[{\"rel\":\"self\",\"type\":\"application/activity+json\",\"href\":\"https://%s/ap/actor\"}]}\n",
          h, h, h);
        protocol_reply(200, "application/jrd+json", body);
        return 1;
    }
    if (!strcmp(target, "/ap/actor")) {
        snprintf(body, sizeof(body),
          "{\"@context\":[\"https://www.w3.org/ns/activitystreams\"],"
          "\"type\":\"Service\",\"id\":\"https://%s/ap/actor\",\"preferredUsername\":\"bonfyre\","
          "\"name\":\"Bonfyre\",\"summary\":\"Receipted public-record observation and priced capability\","
          "\"inbox\":\"https://%s/ap/inbox\",\"outbox\":\"https://%s/ap/outbox\","
          "\"url\":\"https://%s/\"}\n", h, h, h, h);
        protocol_reply(200, "application/activity+json", body);
        return 1;
    }
    if (!strcmp(target, "/ap/outbox") || !strncmp(target, "/ap/outbox?", 11)) {
        snprintf(sql, sizeof(sql),
          "SELECT src_id, count(*) AS n FROM observation GROUP BY src_id ORDER BY n DESC LIMIT %d", limit);
        {
            int prc = protocol_rows(bindir, sql, rows, sizeof(rows));
            if (prc == -2) { protocol_page_too_large(limit); return 1; }
            if (prc != 0 || !rows[0]) {
                protocol_reply(503, "application/json", "{\"error\":\"estate_unavailable\"}\n"); return 1;
            }
        }
        snprintf(body, sizeof(body),
          "{\"@context\":\"https://www.w3.org/ns/activitystreams\",\"type\":\"OrderedCollection\","
          "\"id\":\"https://%s/ap/outbox\",\"summary\":\"observation coverage by source\","
          "\"orderedItems\":%s}\n", h, rows);
        protocol_reply(200, "application/activity+json", body);
        return 1;
    }

    /* ---- CloudEvents 1.0 over the WorkGraph ---------------------------- */
    if (!strncmp(target, "/events", 7)) {
        char workdb[PATH_MAX], qbin[PATH_MAX], esql[8192], diag[1024];
        snprintf(qbin, sizeof(qbin), "%s/bonfyre-query", bindir);
        if (snprintf(workdb, sizeof(workdb), "%s/fleming-work.db", fleming_root()) >= (int)sizeof(workdb) ||
            access(qbin, X_OK) != 0 || access(workdb, R_OK) != 0) {
            protocol_reply(503, "application/json", "{\"error\":\"native_workgraph_unavailable\"}\n"); return 1;
        }
        snprintf(esql, sizeof(esql),
          "SET home_directory='%s'; INSTALL sqlite; LOAD sqlite; "
          "ATTACH '%s' AS w (TYPE sqlite, READ_ONLY); "
          "SELECT '1.0' AS specversion, "
          "'org.bonfyre.workgraph.' || n.status AS type, "
          "'https://%s/a2a' AS source, n.mission_id AS id, m.created_at AS time, "
          "'application/json' AS datacontenttype, "
          "n.operator_id AS subject, n.input_uri AS data "
          "FROM w.workgraph_nodes n JOIN w.missions m ON m.id = n.mission_id "
          "WHERE n.mission_id NOT LIKE 'account-%%' "
          "ORDER BY m.created_at DESC LIMIT %d", duckdb_home(), workdb, h, limit);
        {
            char *a[] = { qbin, (char*)"sql", (char*)":memory:", esql, NULL };
            if (auth_call_split(a, NULL, rows, sizeof(rows), diag, sizeof(diag)) != 0 || !rows[0]) {
                fprintf(stderr, "cloudevents: %s\n", diag);
                protocol_reply(503, "application/json", "{\"error\":\"events_unavailable\"}\n"); return 1;
            }
        }
        /* CloudEvents batch content mode: a JSON array of event objects. */
        snprintf(body, sizeof(body), "%s", rows);
        protocol_reply(200, "application/cloudevents-batch+json", body);
        return 1;
    }

    /* ---- OGC API - Features -------------------------------------------- */
    if (!strcmp(target, "/ogc") || !strcmp(target, "/ogc/")) {
        snprintf(body, sizeof(body),
          "{\"title\":\"Bonfyre OGC API - Features\","
          "\"description\":\"Receipted multi-territory external-estate observations\","
          "\"links\":["
            "{\"rel\":\"self\",\"type\":\"application/json\",\"href\":\"https://%s/ogc\"},"
            "{\"rel\":\"conformance\",\"type\":\"application/json\",\"href\":\"https://%s/ogc/conformance\"},"
            "{\"rel\":\"data\",\"type\":\"application/json\",\"href\":\"https://%s/ogc/collections\"}]}\n", h, h, h);
        protocol_reply(200, "application/json", body);
        return 1;
    }
    if (!strcmp(target, "/ogc/conformance")) {
        protocol_reply(200, "application/json",
          "{\"conformsTo\":["
            "\"http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core\","
            "\"http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson\"]}\n");
        return 1;
    }
    if (!strcmp(target, "/ogc/collections")) {
        snprintf(body, sizeof(body),
          "{\"collections\":[{\"id\":\"parcels\",\"title\":\"Receipted appraisal parcels\","
          "\"description\":\"Normalized projection over distinct Tarrant and Dallas public-roll observations; use territory=tarrant|dallas|all\","
          "\"itemType\":\"feature\",\"crs\":[\"http://www.opengis.net/def/crs/OGC/1.3/CRS84\"],"
          "\"links\":[{\"rel\":\"items\",\"type\":\"application/geo+json\","
          "\"href\":\"https://%s/ogc/collections/parcels/items?territory=all\"}]}]}\n", h);
        protocol_reply(200, "application/json", body);
        return 1;
    }
    if (!strncmp(target, "/ogc/collections/parcels/items", 30)) {
        char base[8192], profile_fields[4096];
        if (parcel_projection_sql(base, sizeof(base), territory, limit) != 0 ||
            !bf_domain_profile_select_list(profile_fields, sizeof(profile_fields), "parcel", "ogc", BF_AUDIENCE_PUBLIC) ||
            snprintf(sql, sizeof(sql), "SELECT %s FROM (%s)", profile_fields, base) >= (int)sizeof(sql)) {
            protocol_reply(503, "application/json", "{\"error\":\"projection_unavailable\"}\n"); return 1;
        }
        {
            int prc = protocol_rows(bindir, sql, rows, sizeof(rows));
            if (prc == -2) { protocol_page_too_large(limit); return 1; }
            if (prc != 0 || !rows[0]) {
                protocol_reply(503, "application/json", "{\"error\":\"estate_unavailable\"}\n"); return 1;
            }
        }
        /* geometry is null: the roll carries area and length, never coordinates.
         * GeoJSON permits a null geometry; inventing one would not be a parcel. */
        snprintf(body, sizeof(body),
          "{\"type\":\"FeatureCollection\",\"numberReturned\":%d,"
          "\"geometry_note\":\"null geometry: the source roll carries Shape.STArea() and Shape.STLength() but no coordinates\","
          "\"features_source\":\"src:tad-parcel-view\",\"properties_rows\":%s,"
          "\"links\":[{\"rel\":\"self\",\"type\":\"application/geo+json\",\"href\":\"https://%s/ogc/collections/parcels/items\"}]}\n",
          limit, rows, h);
        protocol_reply(200, "application/geo+json", body);
        return 1;
    }

    if (!strncmp(target, "/ogc/collections/recorded-rights/items", 38)) {
        snprintf(sql, sizeof(sql),
          "SELECT json_extract_string(observed,'$.AccountID') AS account_id, "
          "json_extract_string(observed,'$.ParcelID') AS parcel_id, "
          "trim(coalesce(json_extract_string(observed,'$.SiteAddrNum'),'') || ' ' || coalesce(json_extract_string(observed,'$.SiteStreetname'),'')) AS address, "
          "json_extract_string(observed,'$.Legal1') AS legal1, "
          "json_extract_string(observed,'$.Legal2') AS legal2, "
          "json_extract_string(observed,'$.Legal3') AS legal3, "
          "json_extract_string(observed,'$.Legal4') AS legal4, "
          "json_extract_string(observed,'$.Owner1') AS owner_of_record, "
          "'src:dallas-tax-account-points' AS source_id "
          "FROM observation WHERE src_id='src:dallas-tax-account-points' "
          "AND json_extract_string(observed,'$.CityJuris')='DALLAS' AND ("
          "upper(json_extract_string(observed,'$.Legal1')) LIKE '%%SUBSURFACE%%' OR "
          "upper(json_extract_string(observed,'$.Legal1')) LIKE '%%AIR RIGHTS%%' OR "
          "upper(json_extract_string(observed,'$.Legal2')) LIKE '%%SUBSURFACE%%' OR "
          "upper(json_extract_string(observed,'$.Legal2')) LIKE '%%AIR RIGHTS%%') LIMIT %d", limit);
        {
            int prc = protocol_rows(bindir, sql, rows, sizeof(rows));
            if (prc == -2) { protocol_page_too_large(limit); return 1; }
            if (prc != 0 || !rows[0]) {
                protocol_reply(503, "application/json", "{\"error\":\"estate_unavailable\"}\n"); return 1;
            }
        }
        snprintf(body, sizeof(body),
          "{\"type\":\"FeatureCollection\",\"numberReturned\":%d,\"geometry_note\":\"null geometry: legal-description observations are not surveyed passage geometries\","
          "\"properties_rows\":%s,\"forbidden_inference\":\"a recorded air/subsurface-right observation or shared block description does not prove a physical passage, current access, operating control, ownership, or authority to enter or use it\"}\n",
          limit, rows);
        protocol_reply(200, "application/geo+json", body);
        return 1;
    }

    /* ---- RESO Web API (OData shape) ------------------------------------ */
    if (!strncmp(target, "/reso/Property", 14)) {
        char base[8192], profile_fields[4096];
        if (parcel_projection_sql(base, sizeof(base), territory, limit) != 0 ||
            !bf_domain_profile_select_list(profile_fields, sizeof(profile_fields), "parcel", "reso", BF_AUDIENCE_PUBLIC) ||
            snprintf(sql, sizeof(sql), "SELECT %s FROM (%s)", profile_fields, base) >= (int)sizeof(sql)) {
            protocol_reply(503, "application/json", "{\"error\":\"projection_unavailable\"}\n"); return 1;
        }
        {
            int prc = protocol_rows(bindir, sql, rows, sizeof(rows));
            if (prc == -2) { protocol_page_too_large(limit); return 1; }
            if (prc != 0 || !rows[0]) {
                protocol_reply(503, "application/json", "{\"error\":\"estate_unavailable\"}\n"); return 1;
            }
        }
        snprintf(body, sizeof(body),
          "{\"@odata.context\":\"https://%s/reso/$metadata#Property\",\"value\":%s,"
          "\"forbidden_inference\":\"assessment roll data is not an MLS listing, a listing agreement, or an appraisal\"}\n",
          h, rows);
        protocol_reply(200, "application/json", body);
        return 1;
    }

    /* ---- MISMO subject property ---------------------------------------- */
    if (!strncmp(target, "/mismo/subject-property", 23)) {
        char base[8192], profile_fields[4096];
        if (parcel_projection_sql(base, sizeof(base), territory, limit) != 0 ||
            !bf_domain_profile_select_list(profile_fields, sizeof(profile_fields), "parcel", "mismo", BF_AUDIENCE_PUBLIC) ||
            snprintf(sql, sizeof(sql), "SELECT %s FROM (%s)", profile_fields, base) >= (int)sizeof(sql)) {
            protocol_reply(503, "application/json", "{\"error\":\"projection_unavailable\"}\n"); return 1;
        }
        {
            int prc = protocol_rows(bindir, sql, rows, sizeof(rows));
            if (prc == -2) { protocol_page_too_large(limit); return 1; }
            if (prc != 0 || !rows[0]) {
                protocol_reply(503, "application/json", "{\"error\":\"estate_unavailable\"}\n"); return 1;
            }
        }
        snprintf(body, sizeof(body),
          "{\"MESSAGE\":{\"ABOUT_VERSIONS\":{\"MISMOReferenceModelIdentifier\":\"3.4\"},"
          "\"DEAL_SETS\":{\"SUBJECT_PROPERTIES\":%s}},"
          "\"forbidden_inference\":\"a subject property projection is not an appraisal, a valuation opinion, or a loan file\"}\n",
          rows);
        protocol_reply(200, "application/json", body);
        return 1;
    }

    /* ---- Beckn / UCP / ACP: three commerce grammars, one service book --- */
    if (!strncmp(target, "/beckn/search", 13) || !strncmp(target, "/ucp/catalog", 12) ||
        !strncmp(target, "/acp/products", 13)) {
        static char items[16384];
        int w = 0;
        items[0] = '\0';
        for (int i = 0; i < X402_CATALOG_N; i++) {
            double usd = 0.0;
            int priced = x402_price_usd(X402_CATALOG[i].name, &usd);
            w += snprintf(items + w, sizeof(items) - (size_t)w,
              "%s{\"id\":\"%s\",\"descriptor\":{\"name\":\"%s\",\"long_desc\":\"%s\"},"
              "\"price\":{\"currency\":\"USD\",\"value\":\"%.6f\",\"listed\":%s},"
              "\"fulfillment\":{\"type\":\"x402\",\"resource\":\"/capability/%s\"}}",
              i ? "," : "", X402_CATALOG[i].name, X402_CATALOG[i].name,
              X402_CATALOG[i].summary, usd, priced ? "true" : "false", X402_CATALOG[i].name);
            if ((size_t)w >= sizeof(items) - 512) break;
        }
        if (!strncmp(target, "/beckn/search", 13)) {
            snprintf(body, sizeof(body),
              "{\"context\":{\"domain\":\"capability\",\"action\":\"on_search\",\"version\":\"1.1.0\","
              "\"bpp_id\":\"%s\",\"bpp_uri\":\"https://%s\",\"country\":\"USA\",\"city\":\"std:817\"},"
              "\"message\":{\"catalog\":{\"bpp/descriptor\":{\"name\":\"Bonfyre\"},"
              "\"bpp/payments\":[{\"id\":\"" BF_SETTLEMENT_RAIL "\",\"type\":\"PRE-FULFILLMENT\","
              "\"collected_by\":\"BPP\",\"status\":\"NOT-PAID\","
              "\"uri\":\"https://%s" BF_SETTLEMENT_DISCOVERY "\"}],"
              "\"bpp/providers\":[{\"id\":\"bonfyre\",\"items\":[%s]}]}}}\n", h, h, h, items);
            protocol_reply(200, "application/json", body);
        } else if (!strncmp(target, "/ucp/catalog", 12)) {
            snprintf(body, sizeof(body),
              "{\"protocol\":\"UCP\",\"version\":\"0.1\",\"merchant\":\"%s\",\"products\":[%s],"
              "\"checkout\":{\"rail\":\"" BF_SETTLEMENT_RAIL "\",\"discovery\":\"" BF_SETTLEMENT_DISCOVERY "\"}}\n", h, items);
            protocol_reply(200, "application/json", body);
        } else {
            snprintf(body, sizeof(body),
              "{\"protocol\":\"ACP\",\"version\":\"0.1\",\"seller\":\"%s\",\"products\":[%s],"
              "\"settlement\":{\"rail\":\"" BF_SETTLEMENT_RAIL "\",\"quote\":\"" BF_SETTLEMENT_QUOTE "\"}}\n", h, items);
            protocol_reply(200, "application/json", body);
        }
        return 1;
    }

    /* ---- MPP quote ------------------------------------------------------ */
    if (!strncmp(target, "/mpp/quote", 10)) {
        double usd = 0.0;
        char name[128] = "parcel-underwrite";
        char requested[128];
        /* An absent capability asks for the catalogue's default quote.  The
         * request dispatcher appends representation negotiation to `query`,
         * so that string can be nonempty even when it has no capability key.
         * Read into a separate buffer and replace the default only when that
         * key is actually present. */
        if (query_value(query, "capability", requested, sizeof(requested)))
            snprintf(name, sizeof(name), "%s", requested);
        if (!studio_safe_ident(name) || !x402_find(name)) {
            protocol_reply(404, "application/json",
              "{\"error\":\"unknown_capability\",\"catalogue\":\"/capability\"}\n"); return 1;
        }
        {
            int priced = x402_price_usd(name, &usd);
            snprintf(body, sizeof(body),
              "{\"protocol\":\"MPP\",\"capability\":\"%s\",\"priced\":%s,\"currency\":\"USD\","
              "\"amount\":\"%.6f\",\"settlement_rails\":[\"" BF_SETTLEMENT_RAIL "\"],"
              "\"resource\":\"https://%s/capability/%s\","
              "\"note\":\"a quote is not a reservation; the resource still answers 402 until payment verifies\"}\n",
              name, priced ? "true" : "false", usd, h, name);
            protocol_reply(200, "application/json", body);
        }
        return 1;
    }

    /* ---- CAP 1.2 alerts ------------------------------------------------- */
    if (!strncmp(target, "/cap/alerts", 11)) {
        char stamp[32];
        iso_utc_now(stamp, sizeof(stamp));
        /* No hazard feed is admitted yet, so the collection is honestly empty
         * rather than populated with a sample alert that nothing observed. */
        snprintf(body, sizeof(body),
          "{\"protocol\":\"CAP\",\"version\":\"1.2\",\"sender\":\"bonfyre@%s\",\"sent\":\"%s\","
          "\"status\":\"Actual\",\"msgType\":\"Alert\",\"scope\":\"Public\",\"alerts\":[],"
          "\"empty_reason\":\"no hazard source has been admitted through the foreign-observation law yet; "
          "an empty alert list is reported rather than a sample alert nothing observed\"}\n", h, stamp);
        protocol_reply(200, "application/json", body);
        return 1;
    }

    /* ---- GS1 EPCIS 2.0 --------------------------------------------------- */
    if (!strncmp(target, "/epcis/events", 13)) {
        char stamp[32];
        iso_utc_now(stamp, sizeof(stamp));
        snprintf(body, sizeof(body),
          "{\"@context\":[\"https://ref.gs1.org/standards/epcis/2.0.0/epcis-context.jsonld\"],"
          "\"type\":\"EPCISDocument\",\"schemaVersion\":\"2.0\",\"creationDate\":\"%s\","
          "\"epcisBody\":{\"eventList\":[]},"
          "\"empty_reason\":\"no GS1-identified product or EPCIS capture source is admitted yet; "
          "the event list is empty rather than seeded with invented product movements\"}\n", stamp);
        protocol_reply(200, "application/json", body);
        return 1;
    }

    /* ---- W3C WoT Thing Description --------------------------------------- */
    if (!strncmp(target, "/wot/thing", 10)) {
        snprintf(body, sizeof(body),
          "{\"@context\":\"https://www.w3.org/2022/wot/td/v1.1\","
          "\"title\":\"Bonfyre Estate\",\"id\":\"urn:bonfyre:estate\","
          "\"securityDefinitions\":{\"bearer_sc\":{\"scheme\":\"bearer\"}},\"security\":[\"bearer_sc\"],"
          "\"properties\":{"
            "\"observations\":{\"type\":\"integer\",\"readOnly\":true,"
             "\"forms\":[{\"href\":\"https://%s/studio/v1/fleming/estate\",\"contentType\":\"application/json\"}]},"
            "\"membrane\":{\"type\":\"object\",\"readOnly\":true,"
             "\"forms\":[{\"href\":\"https://%s/studio/v1/membrane\",\"contentType\":\"application/json\"}]}},"
          "\"actions\":{"
            "\"watchParcel\":{\"forms\":[{\"href\":\"https://%s/studio/v1/fleming/watch\",\"op\":\"invokeaction\"}]},"
            "\"claimWork\":{\"forms\":[{\"href\":\"https://%s/studio/v1/work/claim\",\"op\":\"invokeaction\"}]}}}\n",
          h, h, h, h);
        protocol_reply(200, "application/td+json", body);
        return 1;
    }

    /* ---- OpenRTB 2.6 ------------------------------------------------------ */
    if (!strncmp(target, "/openrtb/bidrequest", 19)) {
        char stamp[32];
        iso_utc_now(stamp, sizeof(stamp));
        snprintf(body, sizeof(body),
          "{\"id\":\"bonfyre-%s\",\"at\":2,\"tmax\":120,"
          "\"site\":{\"id\":\"%s\",\"domain\":\"%s\",\"page\":\"https://%s/\"},"
          "\"imp\":[],"
          "\"empty_reason\":\"no inventory is registered and no consent basis is recorded, so no impression object is offered; "
          "an invented impression would be an offer to sell attention that does not exist\"}\n",
          stamp, h, h, h);
        protocol_reply(200, "application/json", body);
        return 1;
    }
    return 0;
}

/* ── A2A: the agent-to-agent carrier of the capability catalogue ──────────
 *
 * The agent card has always advertised "url":"https://<host>/a2a" while nothing
 * was routed there, so every agent that discovered the card and followed it
 * arrived at a 404. The card was a promise with no door behind it, and the
 * membrane counted the knocks.
 *
 * This is the door, and it is deliberately the same capability catalogue the
 * x402 surface sells -- same executor, same price read from the same finance
 * service book through x402_accepts_json(). A2A and x402 are two carriers of
 * one capability; if they could quote different numbers there would be two
 * price lists, which is the drift the carrier law exists to forbid.
 *
 *   GET  /a2a   service descriptor: skills, prices, how to invoke. Free,
 *               because an agent cannot decide to buy what it cannot first see.
 *   POST /a2a   A2A JSON-RPC 2.0. `message/send` names a skill by id.
 *
 * Execution stays paid and unsimulated. With no facilitator configured a priced
 * skill answers with its requirements rather than treating an unverified
 * X-PAYMENT as settled -- the same refusal /capability makes, for the same
 * reason: a payment rail that pretends is worse than one that refuses.
 *
 * Cannot infer: an advertised skill is not an agreement to perform it for any
 * particular requester, and a quote carried in a JSON-RPC error is not an
 * invoice. */
static void a2a_reply(int code, const char *body) {
    printf("HTTP/1.1 %d %s\r\nContent-Type: application/a2a+json\r\n"
           "Content-Length: %zu\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
           code, code == 200 ? "OK" : code == 402 ? "Payment Required"
                 : code == 405 ? "Method Not Allowed" : "Bad Request",
           strlen(body), body);
    fflush(stdout);
}

static int serve_a2a(const char *method, const char *target, const char *body_in,
                     const char *host, const char *xpayment, const char *accept) {
    static char body[49152];
    char accepts[4096], rpc_id[128], rpc_method[64], skill_id[128];
    const char *h = host && *host ? host : "aurekai.life";
    const A2ASkill *sk;
    const X402Capability *cap;
    double usd = 0.0;

    if (strcmp(target, "/a2a") && strcmp(target, "/a2a/")) return 0;

    /* ---- GET from a browser: the same facts, as a page ------------------
     * A person who follows "Sign in to price it" or arrives from the card
     * should not be handed a JSON-RPC envelope to read. Same skills, same
     * prices, same book -- rendered. */
    if (!strcmp(method, "GET") && wants_html(accept)) {
        char doors[24576]; size_t dn = 0; int trunc = 0;
        for (int i = 0; i < A2A_SKILLS_N && !trunc; i++) {
            const A2ASkill *s = &A2A_SKILLS[i];
            char price[512];
            if (s->capability && x402_price_usd(s->capability, &usd) && usd > 0.0) {
                snprintf(price, sizeof(price),
                    "<p class=\"price\">$%.2f <small>USD / call &middot; x402</small></p>", usd);
            } else if (s->capability) {
                snprintf(price, sizeof(price),
                    "<p class=\"price unpriced\">Unpriced <small>not for sale yet &mdash; "
                    "unpriced is not free</small></p>");
            } else if (s->execute) {
                snprintf(price, sizeof(price),
                    "<p class=\"price\">Free <small>executes now &middot; no payment, no "
                    "facilitator, no quote</small></p>");
            } else {
                snprintf(price, sizeof(price),
                    "<p class=\"price unpriced\">Not sold here <small>admitted through the "
                    "work surface</small></p>");
            }
            int w = snprintf(doors + dn, sizeof(doors) - dn,
                "<article class=\"door\"><p class=\"eyebrow\"><i></i>%s</p><h3>%s</h3>%s"
                "<p>%s</p><div class=\"ways\">"
                "<a class=\"way\" href=\"/.well-known/agent.json\">agent card</a>"
                "<a class=\"way\" href=\"/capability/%s\">x402 resource</a>"
                "<a class=\"way\" href=\"/protocols\">other carriers</a></div></article>",
                s->id, s->name, price, s->description,
                s->capability ? s->capability : "");
            if (w < 0 || (size_t)w >= sizeof(doors) - dn) trunc = 1; else dn += (size_t)w;
        }
        if (trunc) { protocol_reply(503, "text/html", "<p>page overflow</p>"); return 1; }
        snprintf(body, sizeof(body),
            BF_PAGE_OPEN "Agent-to-agent \xc2\xb7 Aurekai" BF_PAGE_AFTER_TITLE
            "<main><section class=\"hero wrap\" style=\"padding:56px 0 10px\">"
            "<div class=\"eyebrow\"><i></i><span>Aurekai / agent-to-agent</span></div>"
            "<h1>Talk to this institution <em>as an agent.</em></h1>"
            "<p class=\"lede\">The card at <a href=\"/.well-known/agent.json\">"
            "/.well-known/agent.json</a> advertises these skills. This is the endpoint it "
            "names. Discovery is free; execution is priced from the same service book the "
            "x402 catalogue quotes, so the two carriers cannot disagree about a number.</p>"
            "</section><div class=\"wrap\"><div class=\"doors\">%s</div>"
            "<section style=\"margin:0 0 70px\"><h3 style=\"font-weight:500;letter-spacing:-.04em\">"
            "Calling it</h3><p style=\"color:#526159;font-size:14px;max-width:60ch\">"
            "JSON-RPC 2.0 over POST to <code>https://%s/a2a</code>, method "
            "<code>message/send</code>, naming a skill:</p>"
            "<pre style=\"background:#eef3e2;border:1px solid var(--line);padding:14px 16px;"
            "overflow-x:auto;font:500 12px 'DM Mono',monospace\">"
            "curl -X POST https://%s/a2a \\\n"
            "  -d '{\"jsonrpc\":\"2.0\",\"id\":\"1\",\"method\":\"message/send\","
            "\"params\":{\"skill\":\"parcel-underwrite\"}}'</pre>"
            "<p style=\"color:#526159;font-size:13px;max-width:60ch\">A priced skill answers "
            "402 with its x402 requirements rather than a result. Settlement is never "
            "simulated: with no facilitator configured this endpoint refuses instead of "
            "treating an unverified payment as settled. An advertised skill is not an "
            "agreement to perform it, and a quote is not an invoice.</p></section>"
            "</div></main></body></html>\n",
            site_nav_html(), doors, h, h);
        protocol_reply(200, "text/html; charset=utf-8", body);
        return 1;
    }

    /* ---- GET: the descriptor. Discovery is free. ---------------------- */
    if (!strcmp(method, "GET")) {
        char skills[16384]; size_t sn = 0; int trunc = 0;
        for (int i = 0; i < A2A_SKILLS_N && !trunc; i++) {
            const A2ASkill *s = &A2A_SKILLS[i];
            char priced[512];
            if (s->capability && x402_price_usd(s->capability, &usd)) {
                snprintf(priced, sizeof(priced),
                    "\"execution\":\"paid\",\"priceUsd\":\"%.6f\","
                    "\"payment\":\"x402\",\"resource\":\"https://%s/capability/%s\"",
                    usd, h, s->capability);
            } else if (s->capability) {
                snprintf(priced, sizeof(priced),
                    "\"execution\":\"not_for_sale\",\"reason\":\"this capability carries no "
                    "asking price in the service book, which is different from being free\"");
            } else if (s->execute) {
                snprintf(priced, sizeof(priced),
                    "\"execution\":\"free\",\"reason\":\"computed in process against a real "
                    "corpus this request; no payment, quote or facilitator applies\"");
            } else {
                snprintf(priced, sizeof(priced),
                    "\"execution\":\"elsewhere\",\"reason\":\"%s\"", s->note);
            }
            int w = snprintf(skills + sn, sizeof(skills) - sn,
                "%s{\"id\":\"%s\",\"name\":\"%s\",\"description\":\"%s\",\"tags\":%s,%s}",
                i ? "," : "", s->id, s->name, s->description, s->tags, priced);
            if (w < 0 || (size_t)w >= sizeof(skills) - sn) trunc = 1; else sn += (size_t)w;
        }
        if (trunc) {
            a2a_reply(503, "{\"error\":\"descriptor_overflow\"}\n");
            return 1;
        }
        snprintf(body, sizeof(body),
          "{\"protocol\":\"A2A\",\"name\":\"Bonfyre\","
          "\"card\":\"https://%s/.well-known/agent.json\","
          "\"endpoint\":\"https://%s/a2a\","
          "\"transport\":\"JSON-RPC 2.0 over HTTP POST\","
          "\"methods\":[\"message/send\"],"
          "\"params\":{\"skill\":\"one of the advertised skill ids\"},"
          "\"discovery\":\"free\",\"execution\":\"paid per skill, quoted from the finance service book\","
          "\"facilitator_configured\":%s,"
          "\"skills\":[%s],"
          "\"forbidden_inference\":\"an advertised skill is not an agreement to perform it for "
          "any particular requester, and a quote is not an invoice\"}\n",
          h, h, x402_facilitator()[0] ? "true" : "false", skills);
        a2a_reply(200, body);
        return 1;
    }

    if (strcmp(method, "POST")) {
        a2a_reply(405, "{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32600,"
                       "\"message\":\"GET for the descriptor, POST for JSON-RPC\"}}\n");
        return 1;
    }

    /* ---- POST: JSON-RPC 2.0 ------------------------------------------- */
    /* A numeric id does not survive string extraction, so it is echoed as null
     * rather than guessed at -- a wrong echo is worse than an absent one. */
    if (!studio_json_string(body_in, "id", rpc_id, sizeof(rpc_id))) rpc_id[0] = '\0';

    if (!studio_json_string(body_in, "method", rpc_method, sizeof(rpc_method))) {
        snprintf(body, sizeof(body),
          "{\"jsonrpc\":\"2.0\",\"id\":%s%s%s,\"error\":{\"code\":-32600,"
          "\"message\":\"invalid request: no method\"}}\n",
          rpc_id[0] ? "\"" : "", rpc_id[0] ? rpc_id : "null", rpc_id[0] ? "\"" : "");
        a2a_reply(400, body);
        return 1;
    }
    if (strcmp(rpc_method, "message/send")) {
        snprintf(body, sizeof(body),
          "{\"jsonrpc\":\"2.0\",\"id\":%s%s%s,\"error\":{\"code\":-32601,"
          "\"message\":\"method not found\",\"data\":{\"supported\":[\"message/send\"]}}}\n",
          rpc_id[0] ? "\"" : "", rpc_id[0] ? rpc_id : "null", rpc_id[0] ? "\"" : "");
        a2a_reply(400, body);
        return 1;
    }
    if (!studio_json_string(body_in, "skill", skill_id, sizeof(skill_id)) ||
        !(sk = a2a_find(skill_id))) {
        snprintf(body, sizeof(body),
          "{\"jsonrpc\":\"2.0\",\"id\":%s%s%s,\"error\":{\"code\":-32602,"
          "\"message\":\"invalid params: name an advertised skill\","
          "\"data\":{\"card\":\"https://%s/.well-known/agent.json\"}}}\n",
          rpc_id[0] ? "\"" : "", rpc_id[0] ? rpc_id : "null", rpc_id[0] ? "\"" : "", h);
        a2a_reply(400, body);
        return 1;
    }

    if (!sk->capability) {
        if (sk->execute) {
            /* Real, free, in-process. No quote, no facilitator, no settlement
             * question -- the same reason /recipes answers a browser with no
             * payment gate at all. */
            static char result[32768];
            int ok = sk->execute(body_in, result, sizeof(result));
            snprintf(body, sizeof(body),
              "{\"jsonrpc\":\"2.0\",\"id\":%s%s%s,\"result\":{\"skill\":\"%s\","
              "\"execution\":\"free\",\"data\":%s}}\n",
              rpc_id[0] ? "\"" : "", rpc_id[0] ? rpc_id : "null", rpc_id[0] ? "\"" : "",
              sk->id, result);
            a2a_reply(ok ? 200 : 422, body);
            return 1;
        }
        snprintf(body, sizeof(body),
          "{\"jsonrpc\":\"2.0\",\"id\":%s%s%s,\"error\":{\"code\":-32004,"
          "\"message\":\"skill is not executed over this carrier\",\"data\":{\"reason\":\"%s\"}}}\n",
          rpc_id[0] ? "\"" : "", rpc_id[0] ? rpc_id : "null", rpc_id[0] ? "\"" : "", sk->note);
        a2a_reply(400, body);
        return 1;
    }
    if (!(cap = x402_find(sk->capability)) || !x402_price_usd(sk->capability, &usd) || usd <= 0.0) {
        snprintf(body, sizeof(body),
          "{\"jsonrpc\":\"2.0\",\"id\":%s%s%s,\"error\":{\"code\":-32004,"
          "\"message\":\"capability carries no asking price, so it is not for sale here\","
          "\"data\":{\"note\":\"unpriced is not free\"}}}\n",
          rpc_id[0] ? "\"" : "", rpc_id[0] ? rpc_id : "null", rpc_id[0] ? "\"" : "");
        a2a_reply(400, body);
        return 1;
    }
    if (!x402_accepts_json(accepts, sizeof(accepts), h, cap, usd)) {
        a2a_reply(503, "{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32603,"
                       "\"message\":\"quote overflow\"}}\n");
        return 1;
    }
    /* Settlement is never simulated. An X-PAYMENT presented with no configured
     * facilitator is unverifiable, and an unverifiable payment is not a paid
     * one, so the answer is the quote again rather than a result. */
    snprintf(body, sizeof(body),
      "{\"jsonrpc\":\"2.0\",\"id\":%s%s%s,\"error\":{\"code\":-32003,"
      "\"message\":\"payment required\",\"data\":{\"x402Version\":1,\"accepts\":%s,"
      "\"payment_presented\":%s,\"facilitator_configured\":%s,"
      "\"note\":\"present X-PAYMENT to https://%s/capability/%s; this endpoint refuses "
      "rather than treating an unverified payment as settled\"}}}\n",
      rpc_id[0] ? "\"" : "", rpc_id[0] ? rpc_id : "null", rpc_id[0] ? "\"" : "",
      accepts, (xpayment && *xpayment) ? "true" : "false",
      x402_facilitator()[0] ? "true" : "false", h, cap->name);
    a2a_reply(402, body);
    return 1;
}

/* ── CMS studio partials ─────────────────────────────────────────────────
 *
 * htmx swaps HTML, not JSON. Rendering these fragments on the server keeps the
 * page usable without client-side assembly and means a reader never meets a raw
 * JSON body where a human surface was expected. Reads come straight from the
 * CMS store read-only; writes still go through the CMS service, which owns
 * validation and the published_at stamp. */
static int session_authorized(const char *bearer, const char *db, const char *scope);

static sqlite3 *cms_open_ro(void) {
    const char *db = getenv("BONFYRE_CMS_DB");
    sqlite3 *h = NULL;
    if (!db || !*db) db = "/var/lib/bonfyre/cms/bonfyre_cms.db";
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { if (h) sqlite3_close(h); return NULL; }
    /* A store held for a moment by a writer is not an empty store. Without a
     * wait, a busy read came back as "no rows" and a site's own address was
     * answered with somebody else's kind of page. */
    sqlite3_busy_timeout(h, 5000);
    return h;
}

/* Governance that does something.
 *
 * A grammar nobody can act through is a vocabulary. These are the three acts a
 * member can perform against a project -- say where they stand on a proposal,
 * put their name to a role it needs, and state that they will support it --
 * and each is written down as what it is and nothing more.
 *
 * None of them moves money. Support is an intention and a relationship,
 * recorded before any payment and independent of whether one ever happens;
 * money movement belongs to a payment rail and returns a receipt reference,
 * which is a digest of something presented rather than proof of settlement.
 */
static sqlite3 *cms_open_rw(void) {
    const char *db = getenv("BONFYRE_CMS_DB");
    sqlite3 *h = NULL;
    if (!db || !*db) return NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK) {
        if (h) sqlite3_close(h); return NULL;
    }
    sqlite3_busy_timeout(h, 3000);
    return h;
}

/* A digest over what was actually written, so a row can be pointed at later
 * without the estate claiming the act settled anything. */
#endif /* BONFYRE_WORLD_FEDERATION_AND_PROTOCOLS_H */
