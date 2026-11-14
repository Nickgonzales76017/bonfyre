#ifndef BONFYRE_MACHINE_WEB_AND_WEBSUB_H
#define BONFYRE_MACHINE_WEB_AND_WEBSUB_H
/* Extracted verbatim from main.c lines 22435-23599 (mechanical split, no logic changes). */
static int aperture_push(Action *acts, int n, int cap, const char *label,
                         const char *hint, int strength, int is_write, const char *href) {
    if (n >= cap) return n;
    acts[n].label = label; acts[n].hint = hint;
    acts[n].strength = strength; acts[n].is_write = is_write;
    snprintf(acts[n].href, sizeof(acts[n].href), "%s", href ? href : "");
    return n + 1;
}

static int aperture_cmp(const void *a, const void *b) {
    const Action *x = (const Action *)a, *y = (const Action *)b;
    return y->strength - x->strength;
}

/* ---------------------------------------------------------------------------
 * The machine web.
 *
 * This organ speaks x402, MPP, OGC, RESO, MISMO, Beckn, UCP, ACP, WoT, EPCIS,
 * CAP, OpenRTB, ActivityPub, WebFinger, CloudEvents, A2A and MCP -- and until
 * now emitted no sitemap, no JSON-LD, no Open Graph, no feed, no ETag and no
 * canonical link. The 900-level course was built before the 100-level plumbing
 * that far larger machine ecosystems already consume.
 *
 * None of this introduces a second owner of facts. A carrier is a projection of
 * canonical state, and so is a preview card: Slack range-fetches the head of a
 * document looking for Open Graph, Pinterest wants schema.org, a search crawler
 * wants a sitemap with an honest lastmod. Four consumers, four representations,
 * one state. The routing decides which projection to serve; it never decides
 * what is true.
 *
 * Cost is the point. These are materialized from state that already exists, so
 * ten thousand machine reads cost approximately what one costs.
 * ------------------------------------------------------------------------- */

static const char *site_origin_for_host(const char *explicit_host) {
    const char *host = explicit_host ? explicit_host : (psetup_presentation_host ? psetup_presentation_host : page_render_host);
    static char public_origin[280];
    char normalized[256], subject[384];
    sqlite3 *cms = NULL;
    int published = 0;
    /* Only released public subjects may produce host-specific machine URLs.
     * Arbitrary Host headers must not poison canonicals or sitemap entries. */
    if (host && lower_host(host, normalized)) {
        cms = cms_open_ro();
        published = page_subject_for_host(cms, normalized, subject, sizeof(subject));
        if (cms) sqlite3_close(cms);
        if (published && snprintf(public_origin, sizeof(public_origin),
                                  "https://%s", normalized) < (int)sizeof(public_origin))
            return public_origin;
    }
    const char *o = getenv("BONFYRE_PUBLIC_ORIGIN");
    return (o && *o) ? o : "https://aurekai.life";
}
static const char *site_origin(void) { return site_origin_for_host(NULL); }

/* Metadata for one subject, emitted into the document head.
 *
 * `alternates` routes a machine onward: a crawler that wants JSON should not
 * have to guess the address, and stating it here is what makes content
 * negotiation discoverable rather than folklore. Every URL is absolute because
 * a preview bot resolves these out of context.
 *
 * Open Graph is placed first and kept small on purpose: Slack's crawler
 * deliberately fetches as little as it can, sometimes by HTTP Range, so
 * metadata that arrives late may never be read at all. */
static size_t machine_head(char *out, size_t cap, size_t w,
                           const char *path, const char *title,
                           const char *description, const char *kind,
                           const char *subject_id)
{
    char et[512], ed[1024];
    if (!out || !cap) return 0;
    if (w >= cap) { out[cap - 1] = '\0'; return cap - 1; }
    html_escape(title, et, sizeof(et));
    html_escape(description ? description : "", ed, sizeof(ed));
    if (!bf_bounded_appendf(out, cap, &w, 0,
      "<link rel=\"canonical\" href=\"%s%s\">"
      "<meta property=\"og:type\" content=\"website\">"
      "<meta property=\"og:site_name\" content=\"Aurekai\">"
      "<meta property=\"og:title\" content=\"%s\">"
      "<meta property=\"og:description\" content=\"%s\">"
      "<meta property=\"og:url\" content=\"%s%s\">"
      "<meta name=\"twitter:card\" content=\"summary\">"
      "<meta name=\"twitter:title\" content=\"%s\">"
      "<meta name=\"twitter:description\" content=\"%s\">"
      "<meta name=\"description\" content=\"%s\">",
      site_origin(), path, et, ed, site_origin(), path, et, ed, ed)) return w;
    /* Two ways to the same representations, because machines differ: content
     * negotiation on this address for clients that send Accept, and explicit
     * addresses for the many crawlers that only follow links. A manifest names
     * them all, so one fetch discovers the family. */
    if (kind && *kind && subject_id && *subject_id) {
        const char *o = site_origin();
        (void)bf_bounded_appendf(out, cap, &w, 0,
          "<link rel=\"alternate\" type=\"application/json\" href=\"%s/subjects/%s/%s/index.json\">"
          "<link rel=\"alternate\" type=\"application/ld+json\" href=\"%s/subjects/%s/%s/schema.jsonld\">"
          "<link rel=\"alternate\" type=\"application/geo+json\" href=\"%s/subjects/%s/%s/map.geojson\">"
          "<link rel=\"alternate\" type=\"application/x-ndjson\" href=\"%s/subjects/%s/%s/evidence.ndjson\">"
          "<link rel=\"describedby\" type=\"application/json\" href=\"%s/subjects/%s/%s/manifest.json\">",
          o, kind, subject_id, o, kind, subject_id, o, kind, subject_id,
          o, kind, subject_id, o, kind, subject_id);
    }
    return w;
}

/* A weak validator over the bytes we are about to send.
 *
 * Weak rather than strong because the page is assembled per viewer -- an
 * aperture differs by closure -- so two responses may be semantically the same
 * document without being byte-identical. A conditional request that matches
 * gets 304 and the body is never written, which is most of what a well-behaved
 * crawler costs us. */
static void etag_of(const char *body, char *out, size_t cap) {
    char digest[65] = {0};
    bf_sha256_hex((const uint8_t *)body, strlen(body), digest);
    snprintf(out, cap, "W/\"%.16s\"", digest);
}

/* Reply with a validator, and answer 304 when the client already has it. */
static int cached_reply_m(const char *body, const char *ctype,
                          const char *if_none_match, int head_only)
{
    char tag[64];
    etag_of(body, tag, sizeof(tag));
    if (if_none_match && *if_none_match && strstr(if_none_match, tag + 2)) {
        printf("HTTP/1.1 304 Not Modified\r\nETag: %s\r\n"
               "Cache-Control: public, max-age=60\r\nConnection: close\r\n\r\n", tag);
        fflush(stdout);
        return 1;
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\n"
           "Content-Length: %zu\r\nETag: %s\r\n"
           "Cache-Control: public, max-age=60\r\n"
           "Vary: Accept, Accept-Encoding\r\nConnection: close\r\n\r\n",
           ctype, strlen(body), tag);
    if (!head_only) fputs(body, stdout);
    fflush(stdout);
    return 1;
}

static int cached_reply(const char *body, const char *ctype,
                        const char *if_none_match) {
    return cached_reply_m(body, ctype, if_none_match, 0);
}

/* Which representation did the caller actually ask for?
 *
 * The alternate links in every subject head promise JSON and linked data at the
 * same address. A promise a surface does not keep is worse than one it never
 * made, so the negotiation those links advertise has to exist. */
static int prefers(const char *accept, const char *type) {
    return accept && *accept && strstr(accept, type) != NULL;
}


/* The discovery tier: the cheapest, widest-reach machine surface there is.
 *
 * A sitemap is how a crawler learns what exists without walking every link, and
 * lastmod is how it learns what changed without refetching everything. Emitting
 * it costs one query; not emitting it costs every crawler a full recrawl, or a
 * miss. This organ spoke twelve vertical protocols before it spoke this one.
 *
 * The sitemap lists only what is genuinely public. A draft note is absent,
 * because a sitemap is a publication claim and listing an unpublished thing
 * would be one more way to leak it.
 */
static int serve_machine_web(const char *method, const char *target,
                             const char *host, const char *inm)
{
    static char body[262144];
    size_t w = 0;
    int head_only = !strcmp(method, "HEAD");
    if (strcmp(method, "GET") && !head_only) return 0;

    if (!strcmp(target, "/robots.txt")) {
        char subject[384], normalized[256];
        sqlite3 *cms;
        int published;
        if (!lower_host(host, normalized)) return 0;
        cms = cms_open_ro();
        published = page_subject_for_host(cms, normalized, subject, sizeof(subject));
        if (cms) sqlite3_close(cms);
        if (!published) return 0;
        snprintf(body, sizeof(body),
            "User-agent: *\nAllow: /\nSitemap: https://%s/sitemap.xml\n", normalized);
        return cached_reply_m(body, "text/plain; charset=utf-8", inm, head_only);
    }

    if (!strcmp(target, "/sitemap.xml")) {
        char subject[384], normalized[256];
        sqlite3 *cms = NULL;
        int published = 0;
        if (!lower_host(host, normalized)) return 0;
        cms = cms_open_ro();
        published = page_subject_for_host(cms, normalized, subject, sizeof(subject));
        if (cms) sqlite3_close(cms);
        if (!published) return 0;
        const char *o = site_origin_for_host(host);
        const int apex = host_is_territory_apex(host);
        const char *territory = territory_of(host);
        w = (size_t)snprintf(body, sizeof(body),
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          "<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n"
          "<sitemap><loc>%s/sitemap-surfaces.xml</loc></sitemap>\n", o);
        if (apex && !strcmp(territory, "life"))
            w += (size_t)snprintf(body + w, sizeof(body) - w,
                "<sitemap><loc>%s/sitemap-notes.xml</loc></sitemap>\n"
                "<sitemap><loc>%s/sitemap-subjects.xml</loc></sitemap>\n"
                "<sitemap><loc>%s/sitemap-community.xml</loc></sitemap>\n", o, o, o);
        if (apex && !strcmp(territory, "work"))
            w += (size_t)snprintf(body + w, sizeof(body) - w,
                "<sitemap><loc>%s/sitemap-capabilities.xml</loc></sitemap>\n", o);
        snprintf(body + w, sizeof(body) - w, "</sitemapindex>\n");
        return cached_reply_m(body, "application/xml; charset=utf-8", inm, head_only);
    }

    if (!strcmp(target, "/sitemap-surfaces.xml")) {
        sqlite3 *cms = cms_open_ro(), *fabric = NULL;
        sqlite3_stmt *st = NULL;
        char subject[384] = {0};
        const char *o = site_origin_for_host(host);
        int published = page_subject_for_host(cms, host, subject, sizeof(subject));
        if (cms) sqlite3_close(cms);
        if (!published) return 0;
        w = (size_t)snprintf(body, sizeof(body),
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
        if (host_is_territory_apex(host)) {
            const char *territory = territory_of(host);
            for (size_t i = 0; i < BF_PAGE_PROGRAM_COUNT && w < sizeof(body) - 1024; i++) {
                const BfPageProgram *program = &bf_page_programs[i];
                if (strcmp(program->territory, "any") && strcmp(program->territory, territory)) continue;
                w += (size_t)snprintf(body + w, sizeof(body) - w,
                    "<url><loc>%s%s</loc></url>\n", o, program->route);
            }
        } else {
            w += (size_t)snprintf(body + w, sizeof(body) - w,
                "<url><loc>%s/</loc></url>\n", o);
            fabric = page_subject_fabric_open();
            if (fabric && sqlite3_prepare_v2(fabric,
                "SELECT DISTINCT route FROM subject_route "
                "WHERE subject=?1 AND scope='public' ORDER BY route LIMIT 1500",
                -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, subject, -1, SQLITE_TRANSIENT);
                while (w < sizeof(body) - 1024 && sqlite3_step(st) == SQLITE_ROW) {
                    const char *route = (const char *)sqlite3_column_text(st, 0);
                    if (!route || route[0] != '/' || !strcmp(route, "/") || strlen(route) > 600 ||
                        strspn(route, "/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~%") != strlen(route)) continue;
                    w += (size_t)snprintf(body + w, sizeof(body) - w,
                        "<url><loc>%s%s</loc></url>\n", o, route);
                }
            }
            if (st) sqlite3_finalize(st);
            if (fabric) sqlite3_close(fabric);
            /* A member's released editorial is not necessarily a SubjectFabric
             * route yet. Carry the exact same public-fragment acceptance used
             * by the journal reader; held drafts never reach discovery. */
            {
                sqlite3 *journal = cms_open_ro();
                sqlite3_stmt *site = NULL, *pages = NULL;
                char site_id[128] = {0}, normalized[256];
                int first_article = 1;
                if (journal && lower_host(host, normalized) && sqlite3_prepare_v2(journal,
                    "SELECT site_id FROM site_presentation WHERE namespace='root' "
                    "AND status='published' AND profile='member-profile-v1' AND host=?1 "
                    "ORDER BY id DESC LIMIT 1", -1, &site, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(site, 1, normalized, -1, SQLITE_TRANSIENT);
                    if (sqlite3_step(site) == SQLITE_ROW && sqlite3_column_text(site, 0))
                        snprintf(site_id, sizeof(site_id), "%s",
                            (const char *)sqlite3_column_text(site, 0));
                }
                if (site) sqlite3_finalize(site);
                if (site_id[0] && sqlite3_prepare_v2(journal,
                    "SELECT p.route FROM site_page p WHERE " PH_JOURNAL_RELEASED
                    " ORDER BY p.route LIMIT 5000", -1, &pages, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(pages, 1, site_id, -1, SQLITE_TRANSIENT);
                    while (w < sizeof(body) - 1024 && sqlite3_step(pages) == SQLITE_ROW) {
                        const char *route = (const char *)sqlite3_column_text(pages, 0);
                        size_t len = 0;
                        if (!ph_blog_slug_safe(route, &len)) continue;
                        if (first_article) {
                            w += (size_t)snprintf(body + w, sizeof(body) - w,
                                "<url><loc>%s/blog/</loc></url>\n", o);
                            first_article = 0;
                        }
                        w += (size_t)snprintf(body + w, sizeof(body) - w,
                            "<url><loc>%s%.*s/</loc></url>\n", o, (int)(6 + len), route);
                    }
                }
                if (pages) sqlite3_finalize(pages);
                if (journal) sqlite3_close(journal);
            }
        }
        snprintf(body + w, sizeof(body) - w, "</urlset>\n");
        return cached_reply_m(body, "application/xml; charset=utf-8", inm, head_only);
    }

    if (!strcmp(target, "/sitemap-community.xml")) {
        if (!host || strcmp(host,"aurekai.life")) return 0;
        sqlite3 *db=cms_open_ro();
        int pages=cjw_search_sitemap_pages(db);
        if(db)sqlite3_close(db);
        if(pages<0)return 0;
        if(pages==0){
            snprintf(body,sizeof(body),
              "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
              "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n"
              "<url><loc>https://aurekai.life/communities</loc></url>\n"
              "<url><loc>https://aurekai.life/community</loc></url>\n"
              "</urlset>\n");
            return cached_reply_m(body,"application/xml; charset=utf-8",inm,head_only);
        }
        w=(size_t)snprintf(body,sizeof(body),
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          "<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
        for(int page=0;page<pages;page++) {
            int n=snprintf(body+w,sizeof(body)-w,
                "<sitemap><loc>https://aurekai.life/sitemap-community-%d.xml</loc></sitemap>\n",page);
            if(n<0||(size_t)n>=sizeof(body)-w)return 0;
            w+=(size_t)n;
        }
        snprintf(body+w,sizeof(body)-w,"</sitemapindex>\n");
        return cached_reply_m(body,"application/xml; charset=utf-8",inm,head_only);
    }

    if (!strncmp(target,"/sitemap-community-",19)) {
        unsigned page=0;
        char expected[64];
        if(!host||strcmp(host,"aurekai.life")||
           sscanf(target,"/sitemap-community-%u.xml",&page)!=1||
           snprintf(expected,sizeof(expected),"/sitemap-community-%u.xml",page)<0||
           strcmp(expected,target)) return 0;
        sqlite3 *db=cms_open_ro(), *verify=db;
        sqlite3_stmt *st=NULL;
        int pages=cjw_search_sitemap_pages(db);
        if(pages<0||page>=(unsigned)pages){if(db)sqlite3_close(db);return 0;}
        if(sqlite3_prepare_v2(db,
          "SELECT p.community,p.post,p.author_subject,strftime('%Y-%m-%dT%H:%M:%SZ',p.published_at) " CJW_SEARCH_PUBLIC_FROM
          " ORDER BY p.id ASC LIMIT ?1 OFFSET ?2",-1,&st,NULL)!=SQLITE_OK) {
            if(db)sqlite3_close(db);
            return 0;
        }
        sqlite3_bind_int(st,1,CJW_SEARCH_PER_SITEMAP);
        sqlite3_bind_int64(st,2,(sqlite3_int64)page*CJW_SEARCH_PER_SITEMAP);
        w=(size_t)snprintf(body,sizeof(body),
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
        while(w<sizeof(body)-512&&sqlite3_step(st)==SQLITE_ROW) {
            const char *community=(const char*)sqlite3_column_text(st,0);
            const char *post=(const char*)sqlite3_column_text(st,1);
            const char *author=(const char*)sqlite3_column_text(st,2);
            const char *lastmod=(const char*)sqlite3_column_text(st,3);
            CjrCommunity c;
            if(!cjr_key(community)||!cjr_key(post)||!cjr_actor(author)||!lastmod||!*lastmod||
               cjr_community(verify,community,&c)!=1||!cjr_public(&c))continue;
            int n=snprintf(body+w,sizeof(body)-w,
                "<url><loc>https://aurekai.life/community/%s/thread/%s/</loc><lastmod>%s</lastmod></url>\n",
                community,post,lastmod);
            if(n<0||(size_t)n>=sizeof(body)-w)break;
            w+=(size_t)n;
        }
        sqlite3_finalize(st);
        sqlite3_close(db);
        snprintf(body+w,sizeof(body)-w,"</urlset>\n");
        return cached_reply_m(body,"application/xml; charset=utf-8",inm,head_only);
    }


    if (!strcmp(target, "/sitemap-notes.xml")) {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
        const char *o = site_origin_for_host(host);
        w = (size_t)snprintf(body, sizeof(body),
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
        /* published AND guest-readable: the same rule the reader path enforces,
         * so the sitemap can never advertise something the site would refuse. */
        if (db && sqlite3_prepare_v2(db,
              "SELECT route, published_at FROM wiki_page "
              "WHERE status='published' AND allow_guest=1 AND route LIKE 'notes/%' "
              "ORDER BY id DESC LIMIT 5000", -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW && w < sizeof(body) - 512) {
                const char *route = (const char *)sqlite3_column_text(st, 0);
                const char *at = (const char *)sqlite3_column_text(st, 1);
                if (!route) continue;
                w += (size_t)snprintf(body + w, sizeof(body) - w,
                  "<url><loc>%s/%s</loc>%s%s%s</url>\n", o, route,
                  at ? "<lastmod>" : "", at ? at : "", at ? "</lastmod>" : "");
            }
        }
        if (st) sqlite3_finalize(st);
        if (db) sqlite3_close(db);
        snprintf(body + w, sizeof(body) - w, "</urlset>\n");
        return cached_reply_m(body, "application/xml; charset=utf-8", inm, head_only);
    }

    if (!strcmp(target, "/sitemap-subjects.xml")) {
        /* A bounded sample rather than six million URLs. A sitemap that cannot
         * be fetched is worse than a smaller one that can, and the estate is
         * better reached through its bulk representations than one URL per row. */
        const char *bindir = getenv("BONFYRE_BIN_DIR");
        static char rows[131072];
        const char *o = site_origin_for_host(host);
        if (!bindir || !*bindir) bindir = "/usr/local/bin";
        w = (size_t)snprintf(body, sizeof(body),
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
        if (fleming_query(bindir,
              "SELECT DISTINCT trim(json_extract_string(observed,'$.Account_Nu')) AS acct "
              "FROM observation WHERE src_id='src:tad-parcel-view' "
              "AND trim(json_extract_string(observed,'$.Account_Nu')) <> '' LIMIT 1000",
              rows, sizeof(rows)) == 0 && rows[0] == '[') {
            const char *p3 = rows;
            while ((p3 = strstr(p3, "\"acct\":\"")) && w < sizeof(body) - 640) {
                char acct[64] = {0};
                p3 += 8;
                for (size_t k = 0; k + 1 < sizeof(acct) && p3[k] && p3[k] != '"'; k++)
                    acct[k] = p3[k];
                if (acct[0])
                    w += (size_t)snprintf(body + w, sizeof(body) - w,
                      "<url><loc>%s/room/parcel/%s</loc></url>\n", o, acct);
            }
        }
        snprintf(body + w, sizeof(body) - w, "</urlset>\n");
        return cached_reply_m(body, "application/xml; charset=utf-8", inm, head_only);
    }

    if (!strcmp(target, "/sitemap-capabilities.xml")) {
        const char *o = site_origin_for_host(host);
        w = (size_t)snprintf(body, sizeof(body),
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
        for (int i = 0; i < X402_CATALOG_N && w < sizeof(body) - 512; i++)
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<url><loc>%s/capability/%s</loc></url>\n", o, X402_CATALOG[i].name);
        snprintf(body + w, sizeof(body) - w, "</urlset>\n");
        return cached_reply_m(body, "application/xml; charset=utf-8", inm, head_only);
    }

    if (!strcmp(target, "/feed.xml") || !strcmp(target, "/feed.atom")) {
        sqlite3 *cms = cms_open_ro();
        sqlite3 *fabric = NULL;
        char subject[384] = {0}, normalized_host[256] = {0};
        int served;
        if (page_subject_for_host(cms, host, subject, sizeof(subject)) &&
            lower_host(host, normalized_host))
            fabric = page_subject_fabric_open();
        served = bf_atom_feed_serve(stdout, cms, fabric, subject, normalized_host,
                                    site_origin_for_host(host), inm, head_only,
                                    getenv("BONFYRE_WEBSUB_HUB"));
        if (fabric) sqlite3_close(fabric);
        if (cms) sqlite3_close(cms);
        return served;
    }

    return 0;
}

static void json_escape_into(const char *in, char *out, size_t cap);

/* The push tier.
 *
 * Everything above is pull: a crawler decides when to look. IndexNow inverts
 * that -- the site tells participating engines that a URL changed, and they
 * redistribute the notification among themselves. A change that would have
 * waited days for a recrawl propagates in minutes, and a withdrawal propagates
 * just as fast, which matters more here than the addition does: when a source
 * is withdrawn and a claim loses its support, the stale copy in someone else's
 * index is the thing that keeps asserting it.
 *
 * The key is served at its own address because that is how the protocol proves
 * the notifier controls the host. It is a published verification token, not a
 * credential: it authorises nothing except "this host asked for a recrawl".
 */
static const char *indexnow_key(void) {
    const char *k = getenv("BONFYRE_INDEXNOW_KEY");
    return (k && *k) ? k : "";
}

static int serve_indexnow_key(const char *method, const char *target,
                              const char *inm)
{
    static char body[128];
    const char *key = indexnow_key();
    char want[160];
    if (strcmp(method, "GET") && strcmp(method, "HEAD")) return 0;
    if (!key[0]) return 0;
    snprintf(want, sizeof(want), "/%s.txt", key);
    if (strcmp(target, want)) return 0;
    snprintf(body, sizeof(body), "%s\n", key);
    return cached_reply_m(body, "text/plain; charset=utf-8", inm,
                          !strcmp(method, "HEAD"));
}

/* The artifact family for one subject.
 *
 * Content negotiation is correct and many machines still will not do it: a
 * crawler that found a link wants an address it can fetch, cache and cite.
 * So every subject also stands at explicit addresses, one per representation,
 * and a manifest that names them all with a digest each.
 *
 *   /subjects/parcel/<id>/manifest.json   what exists, and the digest of each
 *   /subjects/parcel/<id>/index.json      the subject as plain JSON
 *   /subjects/parcel/<id>/schema.jsonld   the subject as schema.org
 *   /subjects/parcel/<id>/map.geojson     the subject as a GeoJSON Feature
 *   /subjects/parcel/<id>/evidence.ndjson one observation per line
 *
 * These are projections, not copies: each is derived from the same canonical
 * row at request time and carries the same forbidden inference. Nothing here
 * becomes a second owner of the fact, which is why the manifest digests the
 * representation rather than asserting a version of the subject.
 */
static int subject_artifact(const char *method, const char *target,
                            const char *inm)
{
    static char body[131072], rows[65536];
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    char ident[64], leaf[64], sql[2048];
    char addr[256]={0}, owner[256]={0}, city[64]={0}, tv[64]={0}, lv[64]={0};
    char e_addr[512], e_owner[512];
    int head_only = !strcmp(method, "HEAD");
    const char *o = site_origin();

    if (strncmp(target, "/subjects/parcel/", 17)) return 0;
    if (strcmp(method, "GET") && !head_only) return 0;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";

    {
        const char *p2 = target + 17, *slash = strchr(p2, '/');
        if (!slash || (size_t)(slash - p2) >= sizeof(ident)) return 0;
        memcpy(ident, p2, (size_t)(slash - p2)); ident[slash - p2] = '\0';
        snprintf(leaf, sizeof(leaf), "%s", slash + 1);
        for (const char *z = ident; *z; z++)
            if (!isalnum((unsigned char)*z) && *z != '-') return 0;
    }

    snprintf(sql, sizeof(sql),
      "SELECT json_extract_string(observed,'$.Situs_Addr') AS address, "
      "json_extract_string(observed,'$.Owner_Name') AS owner, "
      "trim(json_extract_string(observed,'$.City')) AS city, "
      "TRY_CAST(json_extract_string(observed,'$.Land_Value') AS DOUBLE) AS land_value, "
      "TRY_CAST(json_extract_string(observed,'$.Total_Valu') AS DOUBLE) AS total_value "
      "FROM observation WHERE src_id='src:tad-parcel-view' "
      "AND trim(json_extract_string(observed,'$.Account_Nu'))='%s' LIMIT 1", ident);
    if (fleming_query(bindir, sql, rows, sizeof(rows)) != 0 || rows[0] != '[' || strlen(rows) < 4)
        return 0;
    studio_json_field(rows, "address", addr, sizeof(addr));
    studio_json_field(rows, "owner", owner, sizeof(owner));
    studio_json_field(rows, "city", city, sizeof(city));
    studio_json_field(rows, "land_value", lv, sizeof(lv));
    studio_json_field(rows, "total_value", tv, sizeof(tv));
    json_escape_into(addr, e_addr, sizeof(e_addr));
    json_escape_into(owner, e_owner, sizeof(e_owner));

    if (!strcmp(leaf, "index.json")) {
        snprintf(body, sizeof(body),
          "{\"account\":\"%s\",\"address\":\"%s\",\"owner_of_record\":\"%s\","
          "\"city\":\"%s\",\"land_value_usd\":%s,\"total_assessed_usd\":%s,"
          "\"source\":\"src:tad-parcel-view\",\"html\":\"%s/room/parcel/%s\","
          "\"observation_of\":\"a published public record\","
          "\"not\":[\"title\",\"appraisal\",\"permission to contact an owner\"]}\n",
          ident, e_addr, e_owner, city[0] ? city : "",
          lv[0] ? lv : "null", tv[0] ? tv : "null", o, ident);
        return cached_reply_m(body, "application/json; charset=utf-8", inm, head_only);
    }

    if (!strcmp(leaf, "schema.jsonld")) {
        snprintf(body, sizeof(body),
          "{\"@context\":\"https://schema.org\",\"@type\":\"Place\","
          "\"identifier\":\"%s\",\"name\":\"%s\","
          "\"address\":{\"@type\":\"PostalAddress\",\"streetAddress\":\"%s\","
          "\"addressLocality\":\"%s\",\"addressRegion\":\"TX\"},"
          "\"url\":\"%s/room/parcel/%s\","
          "\"isBasedOn\":\"observation of a published public record; "
          "not title and not an appraisal\"}\n",
          ident, e_addr[0] ? e_addr : ident, e_addr, city[0] ? city : "", o, ident);
        return cached_reply_m(body, "application/ld+json", inm, head_only);
    }

    if (!strcmp(leaf, "map.geojson")) {
        /* Geometry is null and says so. The roll this is observed from
         * publishes an address, not a coordinate, and inventing a centroid
         * would turn an absent fact into a plausible one. A map client can
         * still geocode the address itself and know that it did. */
        snprintf(body, sizeof(body),
          "{\"type\":\"Feature\",\"id\":\"%s\",\"geometry\":null,"
          "\"geometry_absent_because\":\"the observed roll publishes an address, "
          "not a coordinate; no centroid is synthesised here\","
          "\"properties\":{\"account\":\"%s\",\"address\":\"%s\","
          "\"owner_of_record\":\"%s\",\"total_assessed_usd\":%s}}\n",
          ident, ident, e_addr, e_owner, tv[0] ? tv : "null");
        return cached_reply_m(body, "application/geo+json", inm, head_only);
    }

    if (!strcmp(leaf, "evidence.ndjson")) {
        char sql2[2048];
        snprintf(sql2, sizeof(sql2),
          "SELECT src_id, observed FROM observation "
          "WHERE trim(json_extract_string(observed,'$.Account_Nu'))='%s' LIMIT 200", ident);
        if (fleming_query(bindir, sql2, rows, sizeof(rows)) != 0) return 0;
        /* One observation per line: the shape a bulk reader wants, and the one
         * that streams without holding the whole set. */
        {
            size_t w = 0; const char *p3 = rows;
            while ((p3 = strstr(p3, "{\"src_id\"")) && w < sizeof(body) - 2048) {
                const char *end = strstr(p3, "}\n");
                if (!end) end = p3 + strlen(p3);
                w += (size_t)snprintf(body + w, sizeof(body) - w, "%.*s\n",
                                      (int)(end - p3), p3);
                p3 = end;
            }
            if (!w) w = (size_t)snprintf(body, sizeof(body),
              "{\"note\":\"no observation rows carry this account\"}\n");
            body[w] = '\0';
        }
        return cached_reply_m(body, "application/x-ndjson", inm, head_only);
    }

    if (!strcmp(leaf, "manifest.json")) {
        /* Names every representation and digests each one, so a machine can
         * tell what changed without refetching all of them. */
        static const char *names[] = {"index.json", "schema.jsonld",
                                      "map.geojson", "evidence.ndjson"};
        static const char *types[] = {"application/json", "application/ld+json",
                                      "application/geo+json", "application/x-ndjson"};
        size_t w = (size_t)snprintf(body, sizeof(body),
          "{\"subject\":\"parcel/%s\",\"html\":\"%s/room/parcel/%s\","
          "\"representations\":[", ident, o, ident);
        for (size_t i = 0; i < sizeof(names)/sizeof(names[0]); i++)
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "%s{\"name\":\"%s\",\"type\":\"%s\",\"url\":\"%s/subjects/parcel/%s/%s\"}",
              i ? "," : "", names[i], types[i], o, ident, names[i]);
        snprintf(body + w, sizeof(body) - w,
          "],\"canonical_state\":\"one subject; each representation is a projection "
          "of it and none is a second owner of the fact\"}\n");
        return cached_reply_m(body, "application/json; charset=utf-8", inm, head_only);
    }

    return 0;
}

/* ---------------------------------------------------------------------------
 * WebSub: rung two of the carrier ladder.
 *
 * Every rung below this one polls. A watcher fetches, hashes, finds nothing,
 * and does it again -- twenty thousand times an hour across a source
 * constellation, almost always to learn that nothing happened. WebSub inverts
 * it: the publisher's hub calls us when the topic actually changes, and the
 * quiet sources cost nothing at all.
 *
 * Three decisions here are security decisions, not conveniences.
 *
 * **A delivery is a notification, never content.** The hub POSTs the changed
 * document; we do not admit it. We verify the signature, then re-read the topic
 * through `bonfyre-project foreign observe`, which does the robots preflight and
 * writes a receipt. So a forged or truncated POST body cannot become foreign
 * state, and the bytes we keep are always the ones the origin served us. This
 * also sidesteps the request buffer entirely: the payload's size stops
 * mattering when the payload is not the thing being kept.
 *
 * **The topic must be one we asked for.** A hub's verification GET names a
 * topic; if it is not the exact topic this callback subscribed to, the
 * subscription is refused. Without that check a hub -- or anyone who guessed a
 * callback -- could enrol us into a topic we never chose.
 *
 * **The callback is unguessable and per-subscription.** It carries its own
 * secret, so a signature proves the delivery came from the hub we handed that
 * secret to, and compares in constant time.
 * ------------------------------------------------------------------------- */

/* Header names are case-insensitive and strcasestr is a GNU extension the
 * strict C11 build here does not admit, so find one portably. */
static const char *header_find(const char *headers, const char *name) {
    size_t n = strlen(name);
    if (!headers) return NULL;
    for (const char *p = headers; (p = strchr(p, '\n')) != NULL; p++) {
        const char *line = p + 1;
        size_t i = 0;
        while (i < n && line[i] &&
               tolower((unsigned char)line[i]) == tolower((unsigned char)name[i])) i++;
        if (i == n) return line;
    }
    return NULL;
}

/* HMAC-SHA256. The library ships HMAC-BLAKE2b, which is the better primitive
 * and the wrong one here: the hub picks the algorithm and WebSub hubs sign with
 * SHA-family digests. Verification has to speak what the sender speaks. */
static void hmac_sha256(const uint8_t *key, size_t key_len,
                        const uint8_t *msg, size_t msg_len,
                        uint8_t out[32])
{
    uint8_t k[64] = {0}, ipad[64], opad[64], inner[32];
    BfSha256 ctx;
    if (key_len > 64) {
        bf_sha256_init(&ctx);
        bf_sha256_update(&ctx, key, key_len);
        bf_sha256_final(&ctx, k);
    } else {
        memcpy(k, key, key_len);
    }
    for (int i = 0; i < 64; i++) { ipad[i] = k[i] ^ 0x36; opad[i] = k[i] ^ 0x5c; }
    bf_sha256_init(&ctx);
    bf_sha256_update(&ctx, ipad, 64);
    bf_sha256_update(&ctx, msg, msg_len);
    bf_sha256_final(&ctx, inner);
    bf_sha256_init(&ctx);
    bf_sha256_update(&ctx, opad, 64);
    bf_sha256_update(&ctx, inner, 32);
    bf_sha256_final(&ctx, out);
}

/* Constant time: a signature check that returns early leaks how much of the
 * digest was right, one byte at a time. */
static int const_time_eq(const char *a, const char *b, size_t n) {
    unsigned char diff = 0;
    for (size_t i = 0; i < n; i++) diff |= (unsigned char)(a[i] ^ b[i]);
    return diff == 0;
}

static const char *websub_db_path(void) {
    const char *p = getenv("BONFYRE_WEBSUB_DB");
    return (p && *p) ? p : "/var/lib/bonfyre/state/websub.db";
}

static sqlite3 *websub_open(int writable) {
    sqlite3 *db = NULL;
    int flags = writable ? (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)
                         : SQLITE_OPEN_READONLY;
    if (sqlite3_open_v2(websub_db_path(), &db, flags, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return NULL;
    }
    if (writable) {
        sqlite3_exec(db,
          "CREATE TABLE IF NOT EXISTS subscriptions("
          "token TEXT PRIMARY KEY, topic TEXT NOT NULL, hub TEXT NOT NULL,"
          "secret TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',"
          "lease_expires INTEGER DEFAULT 0, created_at TEXT, verified_at TEXT,"
          "last_delivery_at TEXT, last_fetch_at INTEGER DEFAULT 0,"
          "deliveries INTEGER DEFAULT 0, rejected INTEGER DEFAULT 0,"
          "authority_receipt TEXT NOT NULL DEFAULT '',"
          "renewal_requested_at TEXT)",
          NULL, NULL, NULL);
        /* Old native and Python subscriptions predate the authority receipt.
         * Migrate their shape, but deliberately leave them non-executable:
         * an empty receipt cannot enqueue an external observation. */
        sqlite3_exec(db,
          "ALTER TABLE subscriptions ADD COLUMN authority_receipt TEXT NOT NULL DEFAULT ''",
          NULL, NULL, NULL);
        sqlite3_exec(db,
          "ALTER TABLE subscriptions ADD COLUMN renewal_requested_at TEXT",
          NULL, NULL, NULL);
        sqlite3_exec(db,
          "CREATE TABLE IF NOT EXISTS source_dirty("
          "token TEXT PRIMARY KEY, topic TEXT NOT NULL, frontier TEXT NOT NULL,"
          "state TEXT NOT NULL DEFAULT 'pending', dirty_at INTEGER NOT NULL,"
          "claimed_at INTEGER DEFAULT 0, attempts INTEGER DEFAULT 0, last_error TEXT DEFAULT '')",
          NULL, NULL, NULL);
    }
    return db;
}

static int serve_websub(const char *method, const char *target, const char *query,
                        const char *raw_headers, const char *body_in)
{
    char token[80] = {0}, mode[32] = {0}, topic[1024] = {0}, challenge[256] = {0};
    sqlite3 *db; sqlite3_stmt *st = NULL;
    char want_topic[1024] = {0}, secret[128] = {0}, state[32] = {0}, authority_receipt[1024] = {0};

    if (strncmp(target, "/websub/", 8)) return 0;
    snprintf(token, sizeof(token), "%s", target + 8);
    for (const char *z = token; *z; z++)
        if (!isalnum((unsigned char)*z) && *z != '-') return 0;
    if (!token[0]) return 0;

    db = websub_open(0);
    if (!db) { protocol_reply(503, "text/plain", "no subscription store\n"); return 1; }
    if (sqlite3_prepare_v2(db, "SELECT topic, secret, state, authority_receipt FROM subscriptions "
                               "WHERE token=? LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, token, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            snprintf(want_topic, sizeof(want_topic), "%s", (const char *)sqlite3_column_text(st, 0));
            snprintf(secret, sizeof(secret), "%s", (const char *)sqlite3_column_text(st, 1));
            snprintf(state, sizeof(state), "%s", (const char *)sqlite3_column_text(st, 2));
            if (sqlite3_column_text(st, 3))
                snprintf(authority_receipt, sizeof(authority_receipt), "%s", (const char *)sqlite3_column_text(st, 3));
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    if (!want_topic[0]) {
        /* Unknown callbacks are indistinguishable from wrong ones on purpose. */
        protocol_reply(404, "text/plain", "no such subscription\n");
        return 1;
    }

    if (!strcmp(method, "GET")) {
        query_value(query, "hub.mode", mode, sizeof(mode));
        query_value(query, "hub.topic", topic, sizeof(topic));
        query_value(query, "hub.challenge", challenge, sizeof(challenge));
        if (strcmp(topic, want_topic)) {
            /* The check that keeps a hub from enrolling us into a topic we
             * never chose. Refusing is the whole mechanism. */
            protocol_reply(404, "text/plain",
              "that is not the topic this callback subscribed to\n");
            return 1;
        }
        if (!strcmp(mode, "subscribe") || !strcmp(mode, "unsubscribe")) {
            sqlite3 *w = websub_open(1);
            if (w) {
                char lease[32] = {0};
                query_value(query, "hub.lease_seconds", lease, sizeof(lease));
                sqlite3_stmt *u = NULL;
                const char *sql = !strcmp(mode, "subscribe")
                  ? "UPDATE subscriptions SET state='active', verified_at=datetime('now'),"
                    " lease_expires=strftime('%s','now')+? WHERE token=?"
                  : "UPDATE subscriptions SET state='unsubscribed',"
                    " verified_at=datetime('now'), lease_expires=? WHERE token=?";
                if (sqlite3_prepare_v2(w, sql, -1, &u, NULL) == SQLITE_OK) {
                    sqlite3_bind_int(u, 1, lease[0] ? atoi(lease) : 86400);
                    sqlite3_bind_text(u, 2, token, -1, SQLITE_TRANSIENT);
                    sqlite3_step(u);
                }
                if (u) sqlite3_finalize(u);
                sqlite3_close(w);
            }
            protocol_reply(200, "text/plain", challenge);
            return 1;
        }
        protocol_reply(400, "text/plain", "unrecognised hub.mode\n");
        return 1;
    }

    if (!strcmp(method, "POST")) {
        char sig[160] = {0}, expect[80];
        uint8_t mac[32];
        const char *h = header_find(raw_headers, "x-hub-signature");
        if (strcmp(state, "active")) {
            protocol_reply(409, "text/plain", "subscription is not active\n");
            return 1;
        }
        if (h) {
            const char *eq = strchr(h, '=');
            if (eq) sscanf(eq + 1, "%159[^\r\n]", sig);
        }
        if (secret[0] && !sig[0]) {
            /* This subscription was created with a secret, so every genuine
             * delivery carries a signature. One that does not is forged, or a
             * hub that quietly stopped signing; either way it is not admitted. */
            protocol_reply(403, "text/plain",
              "this subscription is signed; an unsigned delivery is not accepted\n");
            return 1;
        }
        if (secret[0]) {
            hmac_sha256((const uint8_t *)secret, strlen(secret),
                        (const uint8_t *)(body_in ? body_in : ""),
                        body_in ? strlen(body_in) : 0, mac);
            for (int i = 0; i < 32; i++) snprintf(expect + i * 2, 3, "%02x", mac[i]);
            if (strlen(sig) < 64 || !const_time_eq(sig, expect, 64)) {
                sqlite3 *w = websub_open(1);
                if (w) {
                    sqlite3_exec(w, "PRAGMA busy_timeout=2000", NULL, NULL, NULL);
                    char q[256];
                    snprintf(q, sizeof(q),
                      "UPDATE subscriptions SET rejected=rejected+1 WHERE token='%.60s'", token);
                    sqlite3_exec(w, q, NULL, NULL, NULL);
                    sqlite3_close(w);
                }
                protocol_reply(403, "text/plain", "signature does not verify\n");
                return 1;
            }
        }
        /* Verified. Record a coalesced dirty source; WebSub is notification,
         * not a body. A bounded native worker later calls ForeignTwin once.
         * A subscription without a stored authority receipt is deliberately not
         * allowed to induce an external read merely because a hub can reach us. */
        {
            sqlite3 *w = websub_open(1);
            if (w) {
                sqlite3_stmt *u = NULL;
                sqlite3_exec(w, "PRAGMA busy_timeout=2000", NULL, NULL, NULL);
                if (sqlite3_prepare_v2(w,
                      "UPDATE subscriptions SET deliveries=deliveries+1,"
                      " last_delivery_at=datetime('now') WHERE token=?",
                      -1, &u, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(u, 1, token, -1, SQLITE_TRANSIENT);
                    sqlite3_step(u);
                }
                if (u) { sqlite3_finalize(u); u = NULL; }
                if (authority_receipt[0] && sqlite3_prepare_v2(w,
                      "INSERT INTO source_dirty(token,topic,frontier,state,dirty_at) "
                      "VALUES(?,?,?,'pending',strftime('%s','now')) "
                      "ON CONFLICT(token) DO UPDATE SET topic=excluded.topic,frontier=excluded.frontier,"
                      "dirty_at=excluded.dirty_at,state=CASE WHEN source_dirty.state='running' THEN 'running' ELSE 'pending' END",
                      -1, &u, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(u, 1, token, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(u, 2, want_topic, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(u, 3, "websub-delivery", -1, SQLITE_STATIC);
                    sqlite3_step(u);
                }
                if (u) sqlite3_finalize(u);
                sqlite3_close(w);
            }
            protocol_reply(204, "text/plain", "");
        }
        return 1;
    }

    protocol_reply(405, "text/plain", "GET verifies, POST delivers\n");
    return 1;
}

static const char *SUBJ_HEAD =
  "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
  "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
  "<meta name=\"theme-color\" content=\"#101916\"><title>%s &middot; Aurekai</title>"
  "<link rel=\"stylesheet\" href=\"/assets/matrix.css\">"
  "<link rel=\"stylesheet\" href=\"/assets/matrix-aperture.css\">"
  "<link rel=\"stylesheet\" href=\"/assets/vendor-uplot.min.css\">"
  "<style>"
  ".subj{max-width:760px;margin:34px auto 90px}"
  ".subj-name{margin:6px 0 18px;font-size:clamp(34px,5vw,58px);font-weight:500;letter-spacing:-.06em;line-height:.98}"
  ".subj-desc{margin:10px 0 0;color:#526159;font-size:15px;line-height:1.55}"
  ".subj-note{margin:22px 0 0;color:#657068;font-size:12px;line-height:1.6;max-width:62ch}"
  ".flow-facts{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:0;margin:0 0 8px;border-top:1px solid var(--line)}"
  ".flow-facts div{padding:10px 0;border-bottom:1px solid var(--line)}"
  ".flow-facts dt{font:500 10px 'DM Mono',monospace;letter-spacing:.06em;text-transform:uppercase;color:#657068}"
  ".flow-facts dd{margin:3px 0 0;font-size:18px;letter-spacing:-.03em;font-variant-numeric:tabular-nums}"
  ".aperture{margin:30px 0 0}"
  ".ap-label{margin:0 0 12px;font:500 10px 'DM Mono',monospace;letter-spacing:.1em;text-transform:uppercase;color:#657068}"
  ".ap-act{display:flex;flex-direction:column;gap:3px;padding:14px 16px;margin-bottom:8px;"
  "border:1px solid var(--line);background:var(--ak-surface-low,#171d19);text-decoration:none;color:inherit;transition:.16s ease}"
  ".ap-act:hover{border-color:var(--ink);transform:translateY(-2px);box-shadow:4px 4px 0 var(--ak-outline-variant,#444935)}"
  ".ap-do{font-size:17px;letter-spacing:-.03em}"
  ".ap-why{font:500 11px 'DM Mono',monospace;color:#657068}"
  ".ap-act.is-strong{border-color:var(--ink)}.ap-act.is-strong .ap-do{font-size:21px}"
  ".ap-act.is-strongest{border-color:var(--ink);background:var(--night);color:#eaf0e5;padding:20px 18px}"
  ".ap-act.is-strongest .ap-do{font-size:26px;letter-spacing:-.05em}"
  ".ap-act.is-strongest .ap-why{color:#a3b3a8}.ap-act.is-strongest:hover{background:#1b2f26}"
  ".ap-none{color:#657068;font:500 13px 'DM Mono',monospace}"
  ".trail{margin:38px 0 0;padding-top:24px;border-top:1px solid var(--line)}"
  ".tr-dir{display:flex;justify-content:space-between;align-items:baseline;gap:16px;padding:12px 0;"
  "border-bottom:1px solid var(--line);text-decoration:none;color:inherit}"
  "a.tr-dir:hover .tr-label{color:#527d28}"
  ".tr-label{font-size:16px;letter-spacing:-.03em}"
  ".tr-why{font:500 11px 'DM Mono',monospace;color:#657068;text-align:right}"
  ".tr-dir.is-dark{opacity:.5}.tr-dir.is-dark .tr-label{text-decoration:line-through}"
  ".lens-field{font-size:15px;letter-spacing:-.02em;line-height:1.4}"
  ".lens-stream{margin:26px 0 0;border-top:1px solid var(--line)}"
  ".lens-item{display:grid;grid-template-columns:130px 1fr auto;gap:16px;align-items:baseline;"
  "padding:13px 0;border-bottom:1px solid var(--line);text-decoration:none;color:inherit}"
  "a.lens-item:hover .li-name{color:#527d28}"
  ".li-weight{font:500 19px 'DM Mono',monospace;letter-spacing:-.04em;font-variant-numeric:tabular-nums}"
  ".li-name{font-size:16px;letter-spacing:-.03em}"
  ".li-meta{font:500 10px 'DM Mono',monospace;color:#657068;text-transform:uppercase;letter-spacing:.06em}"
  ".room-band{padding:22px 0;border-top:1px solid var(--line)}"
  ".room-band.is-absent{opacity:.62}"
  ".room-h{margin:0 0 12px;font:500 10px 'DM Mono',monospace;letter-spacing:.1em;text-transform:uppercase;color:#657068}"
  ".room-owner{margin:0;font-size:26px;letter-spacing:-.04em}"
  ".room-read{margin:0;font:400 18px/1.62 Georgia,'Iowan Old Style',serif;max-width:62ch}"
  ".room-note{white-space:pre-wrap;overflow-wrap:anywhere;tab-size:4}"
  ".ambient .tr-label{font-family:'DM Mono',monospace;font-size:13px}"
  ".ap-act.is-dark{opacity:.45}.ap-act.is-dark .ap-do{text-decoration:line-through}"
  ".sir-acts{font:500 11px 'DM Mono',monospace;color:#657068}"
  ".flow-shell{max-width:640px}"
  ".fl-card{border:1px solid var(--line);border-radius:18px;background:var(--ak-surface-low,#171d19);padding:30px 26px;margin:24px 0 0;min-height:340px;"
  "display:flex;flex-direction:column;gap:6px}"
  ".fl-kind{margin:0;font:500 10px 'DM Mono',monospace;letter-spacing:.12em;text-transform:uppercase;color:#657068}"
  ".fl-title{margin:6px 0 2px;font-size:34px;font-weight:500;letter-spacing:-.055em;line-height:1.02}"
  ".fl-title.fl-serif{font-family:'Instrument Serif',Georgia,serif;font-weight:400;letter-spacing:-.03em}"
  ".fl-title.fl-mono{font:500 17px/1.35 'DM Mono',monospace;letter-spacing:-.02em;word-break:break-all}"
  ".fl-sub{margin:0;color:#526159;font-size:14px}"
  ".fl-big{margin:16px 0 0;font-size:46px;font-weight:500;letter-spacing:-.06em;font-variant-numeric:tabular-nums}"
  ".fl-cap{margin:0 0 14px;font:500 10px 'DM Mono',monospace;letter-spacing:.08em;text-transform:uppercase;color:#657068}"
  ".fl-card .button{align-self:flex-start;margin-top:6px}"
  ".fl-card.is-parcel,.fl-card.is-cap,.fl-card.is-note,.fl-card.is-source{background:var(--ak-surface-mid,#1b211d)}"
  ".fl-move{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-top:14px}"
  ".fl-next{all:unset;cursor:pointer;flex:1;text-align:center;padding:16px;border:1px solid var(--ink);"
  "background:var(--night);color:#eaf0e5;font:500 13px 'DM Mono',monospace;letter-spacing:.06em}"
  ".fl-next:hover{background:#1b2f26}"
  ".fl-pos{font:500 12px 'DM Mono',monospace;color:#657068;min-width:3ch;text-align:right}"
  ".fl-end{padding:40px 0;text-align:center;color:#657068;font:500 13px 'DM Mono',monospace}"
  ".mk-read{margin:26px 0 0}"
  ".desk-q{margin:34px 0 0;border-top:1px solid rgba(16,32,26,.16);padding-top:18px}"
  ".desk-qh{display:flex;align-items:baseline;justify-content:space-between;gap:12px}"
  ".desk-qh h3{margin:0;font-size:19px;letter-spacing:-.03em;font-weight:600}"
  ".desk-n{font:500 13px var(--mono);color:#657068;font-variant-numeric:tabular-nums}"
  ".desk-why{margin:6px 0 14px;color:#657068;font-size:14px;line-height:1.55;max-width:70ch}"
  ".desk-q.is-clear .desk-n{color:#93a08f}"
  ".desk-row{display:grid;grid-template-columns:1fr auto;gap:2px 16px;padding:13px 0;"
    "border-top:1px solid rgba(16,32,26,.08);text-decoration:none;color:inherit}"
  ".desk-row:first-of-type{border-top:0}"
  ".desk-row:hover{background:rgba(184,229,53,.16)}"
  ".desk-l{font-weight:600;letter-spacing:-.02em;overflow-wrap:anywhere}"
  ".desk-s{grid-column:1;color:#657068;font-size:13px;line-height:1.5;overflow-wrap:anywhere}"
  ".desk-a{grid-row:1/3;align-self:center;font:500 12px var(--mono);letter-spacing:.04em;white-space:nowrap}"
  ".desk-empty{margin:0;color:#657068;font:500 13px var(--mono);line-height:1.6}"
  ".desk-more{margin:12px 0 0;color:#657068;font:500 12px var(--mono)}"
  ".mk-parts{margin:0 0 14px;padding:0 0 0 20px;font:600 14px/1.7 var(--mono);letter-spacing:.02em;color:var(--ink)}"
  "button.ap-act{width:100%%;text-align:left;cursor:pointer;font:inherit}"
  ".flow-search{display:flex;gap:10px;margin:24px 0 6px;flex-wrap:wrap}"
  ".flow-search input{flex:1 1 340px;padding:14px 16px;border:1px solid var(--ink);background:var(--ak-surface-lowest,#0a100c);"
  "color:var(--ink);font:500 17px 'DM Sans',Arial,sans-serif;letter-spacing:-.02em}"
  ".flow-hint{margin:0;color:#657068;font:500 11px 'DM Mono',monospace}"
  ".flow-empty{padding:26px 0;color:#657068;font:500 13px 'DM Mono',monospace}"
  ".mono{font-family:'DM Mono',monospace}"
  "</style><link rel=\"stylesheet\" href=\"/assets/composition.css\"></head><body class=\"ak-app subj-page\" data-aurekai-pattern=\"workbench\">"
  BF_ESTATE_BAR
  /* The nav above carries hx-get, which is inert without the runtime. A page
   * that declares a swap and never loads htmx renders its fallback forever and
   * looks correct in the source. */
  "<script defer src=\"/assets/matrix-htmx.js\"></script>"
  "<main class=\"wrap\">";

static const char *SUBJ_FOOT =
  "</main>" BF_ESTATE_FOOT
  /* The organ layer. A composed room was server-rendered prose with no way to
   * animate what changed, anchor a boundary note, draw a distribution or
   * virtualise a list of thousands -- the libraries existed and nothing on
   * these pages could reach them. Loading it here gives every subject room the
   * same organs the estate's other surfaces have, and the room still reads
   * correctly if none of them arrive. */
  "<script src=\"/assets/matrix.js\"></script>"
  "<script src=\"/assets/client-organs.js\" defer></script></body></html>";

/* ── TRAIL: relations become directions ──────────────────────────────────
 *
 * Source E names the property directions directly: near, owned by, changed
 * since, similar parcel, same builder, same phenotype, same district, near
 * opportunity. Each one below is a real query over the receipted roll, with a
 * live count, so a direction is only offered when there is somewhere to go.
 *
 * Two of E's directions are not computable from this source and are shown as
 * unavailable with the reason. The roll carries area and length but no
 * coordinates, so "near" has no geometry to work from; and it is a single
 * observation per parcel, so "changed since" has no prior state to diff. An
 * invented neighbour would be worse than a named gap.
 */
typedef struct { char label[48]; char why[96]; char href[256]; long long n; int live; } Trail;

static long long trail_count(const char *bindir, const char *sql) {
    static char rows[8192];
    char n[32] = {0};
    if (fleming_query(bindir, sql, rows, sizeof(rows)) != 0 || rows[0] != '[') return -1;
    if (!studio_json_field(rows, "n", n, sizeof(n))) return -1;
    return atoll(n);
}

static int parcel_trail(const char *bindir, const char *acct, const char *city,
                        const char *owner, double total_value, Trail *t, int cap)
{
    char sql[3072]; int n = 0;
    /* owned by -- the same taxpayer name of record elsewhere on the roll */
    if (owner && *owner && n < cap) {
        char esc[512]; size_t w = 0;
        for (const char *p = owner; *p && w + 2 < sizeof(esc); p++) {
            if (*p == '\'') esc[w++] = '\'';
            esc[w++] = *p;
        }
        esc[w] = '\0';
        snprintf(sql, sizeof(sql),
          "SELECT count(*) AS n FROM observation WHERE src_id='src:tad-parcel-view' "
          "AND trim(json_extract_string(observed,'$.Owner_Name'))='%s' "
          "AND trim(json_extract_string(observed,'$.Account_Nu'))<>'%s'", esc, acct);
        {
            long long c = trail_count(bindir, sql);
            if (c > 0) {
                snprintf(t[n].label, sizeof(t[n].label), "Owned by the same name");
                snprintf(t[n].why, sizeof(t[n].why), "%lld other parcels list this taxpayer of record", c);
                snprintf(t[n].href, sizeof(t[n].href), "/studio/v1/fleming/owners");
                t[n].n = c; t[n].live = 1; n++;
            }
        }
    }
    /* same district -- the city code this parcel sits in */
    if (city && *city && n < cap) {
        snprintf(sql, sizeof(sql),
          "SELECT count(*) AS n FROM observation WHERE src_id='src:tad-parcel-view' "
          "AND trim(json_extract_string(observed,'$.City'))='%s'", city);
        {
            long long c = trail_count(bindir, sql);
            if (c > 0) {
                snprintf(t[n].label, sizeof(t[n].label), "Same district");
                snprintf(t[n].why, sizeof(t[n].why), "%lld parcels observed in city %s", c, city);
                snprintf(t[n].href, sizeof(t[n].href), "/property/market?city=%s&amp;account=%s", city, acct);
                t[n].n = c; t[n].live = 1; n++;
            }
        }
    }
    /* similar parcel -- assessed within a fifth of this one, same district */
    if (city && *city && total_value > 0 && n < cap) {
        snprintf(sql, sizeof(sql),
          "SELECT count(*) AS n FROM observation WHERE src_id='src:tad-parcel-view' "
          "AND trim(json_extract_string(observed,'$.City'))='%s' "
          "AND TRY_CAST(json_extract_string(observed,'$.Total_Valu') AS DOUBLE) "
          "BETWEEN %.0f AND %.0f "
          "AND trim(json_extract_string(observed,'$.Account_Nu'))<>'%s'",
          city, total_value * 0.8, total_value * 1.2, acct);
        {
            long long c = trail_count(bindir, sql);
            if (c > 0) {
                snprintf(t[n].label, sizeof(t[n].label), "Similar parcels");
                snprintf(t[n].why, sizeof(t[n].why), "%lld assessed within a fifth of this one", c);
                snprintf(t[n].href, sizeof(t[n].href), "/reso/Property?limit=50");
                t[n].n = c; t[n].live = 1; n++;
            }
        }
    }
    /* near opportunity -- vacant land carrying value in the same district */
    if (city && *city && n < cap) {
        snprintf(sql, sizeof(sql),
          "SELECT count(*) AS n FROM observation WHERE src_id='src:tad-parcel-view' "
          "AND trim(json_extract_string(observed,'$.City'))='%s' "
          "AND TRY_CAST(json_extract_string(observed,'$.Improvemen') AS DOUBLE)=0 "
          "AND TRY_CAST(json_extract_string(observed,'$.Land_Value') AS DOUBLE)>0", city);
        {
            long long c = trail_count(bindir, sql);
            if (c > 0) {
                snprintf(t[n].label, sizeof(t[n].label), "Vacant nearby");
                snprintf(t[n].why, sizeof(t[n].why), "%lld parcels here carry land value and no improvement", c);
                snprintf(t[n].href, sizeof(t[n].href), "/property/market?city=%s&amp;account=%s", city, acct);
                t[n].n = c; t[n].live = 1; n++;
            }
        }
    }
    /* Directions this source cannot support, named rather than omitted. */
    if (n < cap) {
        snprintf(t[n].label, sizeof(t[n].label), "Near");
        snprintf(t[n].why, sizeof(t[n].why), "the roll carries area and length, never coordinates");
        t[n].href[0] = '\0'; t[n].n = 0; t[n].live = 0; n++;
    }
    if (n < cap) {
        snprintf(t[n].label, sizeof(t[n].label), "Changed since");
        snprintf(t[n].why, sizeof(t[n].why), "one observation per parcel, so there is no prior state to diff");
        t[n].href[0] = '\0'; t[n].n = 0; t[n].live = 0; n++;
    }
    return n;
}

static size_t trail_render(char *out, size_t cap, size_t w, Trail *t, int n) {
    static const char foot[] =
      "<p class=\"subj-note\">Relations are directions. A direction is offered only when the graph has "
      "somewhere to go, and a direction this source cannot support is named rather than hidden.</p></div>";
    if (!n) return w;
    if (!out || !cap) return 0;
    if (w >= cap) { out[cap - 1] = '\0'; return cap - 1; }
    if (!bf_bounded_appendf(out, cap, &w, sizeof(foot) - 1,
          "<div class=\"trail\"><p class=\"ap-label\">Where this leads</p>")) return w;
    for (int i = 0; i < n; i++) {
        int appended = t[i].live
            ? bf_bounded_appendf(out, cap, &w, sizeof(foot) - 1,
                "<a class=\"tr-dir\" href=\"%s\"><span class=\"tr-label\">%s</span>"
                "<span class=\"tr-why\">%s</span></a>",
                t[i].href, t[i].label, t[i].why)
            : bf_bounded_appendf(out, cap, &w, sizeof(foot) - 1,
                "<span class=\"tr-dir is-dark\"><span class=\"tr-label\">%s</span>"
                "<span class=\"tr-why\">not from this source &mdash; %s</span></span>",
                t[i].label, t[i].why);
        if (!appended) break;
    }
    (void)bf_bounded_appendf(out, cap, &w, 0, "%s", foot);
    return w;
}

/* Defined with SurfaceIR, below: records the shape of one interaction. */
static void surfaceir_trace(const char *kind, const char *closure,
                            Action *acts, int n, const char *viewer_seed);

/* Defined with the room, below: lists every carrier one subject appears through. */
static size_t ambient_render(char *out, size_t cap, size_t w,
                             const char *kind, const char *ident);

/* A subject that cannot be found is still a page, not a bare fragment. */

static void subject_miss(const char *why) {
    static char page[16384];
    size_t w = 0;
    page[0] = '\0';
    if (!bf_bounded_appendf(page, sizeof(page), &w, strlen(SUBJ_FOOT),
            SUBJ_HEAD, "Not found") ||
        !bf_bounded_appendf(page, sizeof(page), &w, strlen(SUBJ_FOOT),
      "<article class=\"subj\"><p class=\"flow-empty\">%s</p>"
      "<p class=\"subj-note\">Subjects are addressed as <span class=\"mono\">/s/&lt;kind&gt;/&lt;id&gt;</span>. "
      "The index at <span class=\"mono\">/subjects</span> lists every one this estate "
      "holds.</p></article>", why) ||
        !bf_bounded_appendf(page, sizeof(page), &w, 0, "%s", SUBJ_FOOT))
        {
            char escaped_why[4096];html_escape(why?why:"Subject not found.",escaped_why,sizeof(escaped_why));
            snprintf(page,sizeof(page),"<!doctype html><title>Not found</title><p>%s</p>",escaped_why);
        }
    /* A miss is a miss. This went out as 200 with a page explaining itself,
     * which told every crawler and every link checker that the subject
     * resolved -- and told this estate's own tests the same thing. */
    flow_reply_status(404, "Not Found", page);
}

static void aperture_render(char *out, size_t cap, const char *kind, const char *ident,
                            const char *headline, const char *facts,
                            Action *acts, int n, int signed_in, int may_write,
                            const char *viewer_seed)
{
    size_t w = 0;
    (void)ident;   /* callers put the identifier in facts where it belongs */
    qsort(acts, (size_t)n, sizeof(Action), aperture_cmp);
    /* The shape of this interaction, recorded before it is drawn. */
    surfaceir_trace(kind, may_write ? "write" : (signed_in ? "read" : "anonymous"),
                    acts, n, viewer_seed);
    w += (size_t)snprintf(out + w, cap - w, SUBJ_HEAD, headline);
    w += (size_t)snprintf(out + w, cap - w,
      "<article class=\"subj\">"
      "<p class=\"eyebrow\"><i></i>%s</p>"
      "<h2 class=\"subj-name\">%s</h2>"
      "%s"
      "<div class=\"aperture\">"
      "<p class=\"ap-label\">What you can do with this</p>",
      kind, headline, facts ? facts : "");
    for (int i = 0; i < n && w < cap - 1024; i++) {
        const char *cls = acts[i].strength >= 3 ? "ap-act is-strongest"
                        : acts[i].strength == 2 ? "ap-act is-strong" : "ap-act";
        w += (size_t)snprintf(out + w, cap - w,
          "<a class=\"%s\" href=\"%s\"><span class=\"ap-do\">%s</span>"
          "<span class=\"ap-why\">%s</span></a>", cls, acts[i].href, acts[i].label, acts[i].hint);
    }
    if (!n)
        w += (size_t)snprintf(out + w, cap - w,
          "<p class=\"ap-none\">Nothing is offered here for you. That is a closure, not an error.</p>");
    snprintf(out + w, cap - w,
      "</div>"
      "%.0s"
      "</article>",
      may_write ? "You hold write scope, so acts that change things are offered."
      : (signed_in ? "Your account reads but does not write, so only reads are offered."
                   : "You are not signed in, so only what is open to everyone is offered."));
    /* The trail is appended by the caller before the document closes. */
}

static void aperture_close(char *out, size_t cap) {
    size_t w = strlen(out);
    snprintf(out + w, cap - w, "%s", SUBJ_FOOT);
}


/* Which authored operations apply to this subject, for this session.
 *
 * The same resolution /_aperture performs, pushed into an Action list so the
 * canonical subject address can offer them. Kept in one place because two
 * readings of one contract table is how the page and the endpoint come to
 * disagree about what a subject affords.
 *
 * Cannot infer: an offered aperture -> a completed effect. The grammar applies
 * and the actor is admitted; the native owner still owns what happens, and
 * re-checks at submit.
 */
static int aperture_offer_authored(const char *ident, Action *acts, int n, int cap,
                                   const char *bearer, const char *auth_db,
                                   int *withheld_out) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    /* The labels and hrefs outlive this call: Action holds pointers for the
     * label and hint, and the renderer runs after the statement is finalised. */
    static char labels[6][96], hints[6][192], hrefs[6][256];
    char kind[64];
    const char *colon;
    int slot = 0;

    if (withheld_out) *withheld_out = 0;
    if (!ident || !*ident) return n;

    /* institution:aurekai:internet -> "institution". The kind is what a
     * contract declares applicability against; the rest identifies which one. */
    colon = strchr(ident, ':');
    {
        size_t k = colon ? (size_t)(colon - ident) : strlen(ident);
        if (k == 0 || k >= sizeof(kind)) return n;
        memcpy(kind, ident, k); kind[k] = '\0';
    }
    if (!studio_safe_ident(kind)) return n;
    /* A foreign twin is offered what it was classified as, so a property
     * source and a specification afford different things. */
    if (!strcmp(kind, "foreign_twin")) {
        char classified[64];
        if (twin_classified_kind(ident, classified, sizeof(classified)))
            snprintf(kind, sizeof(kind), "%s", classified);
    }

    if (!(db = cms_open_ro())) return n;
    if (!cms_type_registered(db, "object_contract")) { sqlite3_close(db); return n; }
    if (sqlite3_prepare_v2(db,
            "SELECT object_name, birth_mode, birth_operation, "
            "       COALESCE(requires_authority,''), COALESCE(action_label,''), "
            "       COALESCE(human_fields,'') "
            "FROM object_contract WHERE applies_to_kind = ? AND status = 'published' "
            "ORDER BY object_name", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return n;
    }
    sqlite3_bind_text(st, 1, kind, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW && slot < 6 && n < cap) {
        const char *name  = (const char *)sqlite3_column_text(st, 0);
        const char *mode  = (const char *)sqlite3_column_text(st, 1);
        const char *op    = (const char *)sqlite3_column_text(st, 2);
        const char *req   = (const char *)sqlite3_column_text(st, 3);
        const char *label = (const char *)sqlite3_column_text(st, 4);
        const char *human = (const char *)sqlite3_column_text(st, 5);
        int writes;
        if (!name || !op) continue;
        if (req && *req && !surface_authority_satisfied(req, bearer, auth_db)) {
            if (withheld_out) (*withheld_out)++;
            continue;
        }
        /* What the mode actually does, said in the hint rather than implied by
         * the verb: DERIVE computes and stores nothing, PROPOSE creates demand
         * without effect, INVOKE performs something owned elsewhere. */
        writes = mode && (!strcmp(mode, "DECLARE") || !strcmp(mode, "ATTACH"));
        snprintf(labels[slot], sizeof(labels[slot]), "%s",
                 (label && *label) ? label : name);
        snprintf(hints[slot], sizeof(hints[slot]),
                 "%s%s%s%s",
                 mode ? mode : "",
                 mode ? " · " : "",
                 op,
                 (human && *human) ? " · asks you for what it cannot resolve" : "");
        snprintf(hrefs[slot], sizeof(hrefs[slot]),
                 "/_aperture/form?subject=%.100s&operation=%.60s", ident, op);
        n = aperture_push(acts, n, cap, labels[slot], hints[slot], 2, writes, hrefs[slot]);
        slot++;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return n;
}


/* What this subject is waiting on, per declared closure domain.
 *
 * closure_domain says which subject kind it covers, which table holds the
 * conditions, which column scopes them and which state means satisfied. Every
 * one of those is authored, so a new domain is a row rather than a code
 * change -- and this stays a count rather than a closure, because the door
 * must not cost more than the room.
 */
#endif /* BONFYRE_MACHINE_WEB_AND_WEBSUB_H */
