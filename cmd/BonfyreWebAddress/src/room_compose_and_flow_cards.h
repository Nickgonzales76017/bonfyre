#ifndef BONFYRE_ROOM_COMPOSE_AND_FLOW_CARDS_H
#define BONFYRE_ROOM_COMPOSE_AND_FLOW_CARDS_H
/* Extracted verbatim from main.c lines 27583-29041 (mechanical split, no logic changes). */
static size_t ambient_render(char *out, size_t cap, size_t w,
                             const char *kind, const char *ident)
{
    struct { const char *carrier, *what, *href; } c[8];
    int n = 0;
    char h1[256], h2[256], h3[256], h4[256];

    if (!strcmp(kind, "parcel")) {
        snprintf(h1, sizeof(h1), "/reso/Property?limit=25");
        snprintf(h2, sizeof(h2), "/ogc/collections/parcels/items?limit=25");
        snprintf(h3, sizeof(h3), "/mismo/subject-property?limit=25");
        snprintf(h4, sizeof(h4), "/property/parcel?account=%s", ident);
        c[n].carrier="RESO Web API";      c[n].what="a Property record";            c[n].href=h1; n++;
        c[n].carrier="OGC API Features";  c[n].what="a Feature, geometry null";     c[n].href=h2; n++;
        c[n].carrier="MISMO 3.4";         c[n].what="a subject property";           c[n].href=h3; n++;
        c[n].carrier="Bonfyre estate";    c[n].what="receipted observations";       c[n].href=h4; n++;
        c[n].carrier="CloudEvents";       c[n].what="once work is opened on it";    c[n].href="/events?limit=25"; n++;
    } else if (!strcmp(kind, "capability")) {
        snprintf(h1, sizeof(h1), "/capability/%s", ident);
        snprintf(h2, sizeof(h2), "/mpp/quote?capability=%s", ident);
        c[n].carrier="x402";   c[n].what="a payment demand";        c[n].href=h1; n++;
        c[n].carrier="MPP";    c[n].what="a quote";                 c[n].href=h2; n++;
        c[n].carrier="UCP";    c[n].what="a merchant product";      c[n].href="/ucp/catalog"; n++;
        c[n].carrier="ACP";    c[n].what="an agent-commerce item";  c[n].href="/acp/products"; n++;
        c[n].carrier="Beckn";  c[n].what="an on_search item";       c[n].href="/beckn/search"; n++;
        c[n].carrier="A2A";    c[n].what="an advertised skill";     c[n].href="/.well-known/agent.json"; n++;
    } else if (!strcmp(kind, "note")) {
        snprintf(h1, sizeof(h1), "/notes/%s", ident);
        c[n].carrier="Web";          c[n].what="a page at its own route";  c[n].href=h1; n++;
        c[n].carrier="CMS";          c[n].what="an entry you can edit";    c[n].href="/cms/"; n++;
        c[n].carrier="ActivityPub";  c[n].what="an object to follow";      c[n].href="/ap/outbox"; n++;
    } else return w;

    w += (size_t)snprintf(out + w, cap - w,
      "<div class=\"trail ambient\"><p class=\"ap-label\">Where else this same thing appears</p>");
    for (int i = 0; i < n && w < cap - 768; i++)
        w += (size_t)snprintf(out + w, cap - w,
          "<a class=\"tr-dir\" href=\"%s\"><span class=\"tr-label\">%s</span>"
          "<span class=\"tr-why\">%s</span></a>", c[i].href, c[i].carrier, c[i].what);
    w += (size_t)snprintf(out + w, cap - w,
      "<p class=\"subj-note\">These are coordinates of one subject, not copies of it. A carrier is a way of "
      "speaking about the thing; none of them becomes a second owner of it.</p></div>");
    return w;
}

/* ── ROOM: a contextual operating environment for one subject ────────────
 *
 * Navigation continuity, representational discontinuity. You arrive the same
 * way for every subject, and what you find is shaped by what the subject is: a
 * property room is not a project room wearing different words. Source E's
 * property room asks for the place, the parcel, its history, state changes,
 * nearby occurrences and deal structure -- so the room is honest about which of
 * those this estate can actually supply.
 */
static int serve_room(const char *method, const char *target, const char *query,
                      const char *bearer, const char *auth_db, const char *accept)
{
    static char body[262144], rows[131072];
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    char kind[32], ident[160], raw[192];
    int signed_in, may_write;
    size_t w = 0;
    (void)query;

    if (strncmp(target, "/room/", 6)) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    signed_in = session_valid(bearer, auth_db);
    may_write = signed_in && session_authorized(bearer, auth_db, "write");
    {
        const char *p = target + 6, *slash = strchr(p, '/');
        if (!slash || (size_t)(slash - p) >= sizeof(kind)) { subject_miss("No room named."); return 1; }
        memcpy(kind, p, (size_t)(slash - p)); kind[slash - p] = '\0';
        snprintf(raw, sizeof(raw), "%s", slash + 1);
        if (!subject_ident(raw, ident, sizeof(ident))) { subject_miss("No room named."); return 1; }
    }

    if (!strcmp(kind, "parcel")) {
        char sql[2048], addr[256]={0}, owner[256]={0}, city[64]={0};
        char lv[64]={0}, tv[64]={0}, yb[32]={0}, la[64]={0}, ls[64]={0}, pc[64]={0};
        char e_addr[512], e_owner[512];
        snprintf(sql, sizeof(sql),
          "SELECT json_extract_string(observed,'$.Situs_Addr') AS address, "
          "json_extract_string(observed,'$.Owner_Name') AS owner, "
          "json_extract_string(observed,'$.City') AS city, "
          "json_extract_string(observed,'$.Property_C') AS prop_class, "
          "TRY_CAST(json_extract_string(observed,'$.Year_Built') AS BIGINT) AS year_built, "
          "TRY_CAST(json_extract_string(observed,'$.Living_Are') AS DOUBLE) AS living_area, "
          "TRY_CAST(json_extract_string(observed,'$.Land_SqFt') AS DOUBLE) AS lot_sqft, "
          "TRY_CAST(json_extract_string(observed,'$.Land_Value') AS DOUBLE) AS land_value, "
          "TRY_CAST(json_extract_string(observed,'$.Total_Valu') AS DOUBLE) AS total_value "
          "FROM observation WHERE src_id='src:tad-parcel-view' "
          "AND trim(json_extract_string(observed,'$.Account_Nu'))='%s' LIMIT 1", ident);
        if (fleming_query(bindir, sql, rows, sizeof(rows)) != 0 || rows[0] != '[' || strlen(rows) < 4) {
            subject_miss("No parcel in the observed roll carries that account."); return 1;
        }
        studio_json_field(rows,"address",addr,sizeof(addr));
        studio_json_field(rows,"owner",owner,sizeof(owner));
        studio_json_field(rows,"city",city,sizeof(city));
        studio_json_field(rows,"prop_class",pc,sizeof(pc));
        studio_json_field(rows,"year_built",yb,sizeof(yb));
        studio_json_field(rows,"living_area",la,sizeof(la)); num_fix(la, sizeof(la));
        studio_json_field(rows,"lot_sqft",ls,sizeof(ls)); num_fix(ls, sizeof(ls));
        studio_json_field(rows,"land_value",lv,sizeof(lv)); num_fix(lv, sizeof(lv));
        studio_json_field(rows,"total_value",tv,sizeof(tv)); num_fix(tv, sizeof(tv));
        html_escape(addr,e_addr,sizeof(e_addr));
        html_escape(owner,e_owner,sizeof(e_owner));

        w = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, e_addr[0]?e_addr:ident);
        {
            /* A parcel is a Place with a postal address and an assessed value:
             * exactly what a search, map or preview crawler is shaped to read.
             * schema.org here is the same state the HTML shows, in the grammar
             * those machines already parse -- not a second claim about it. */
            char path[256], jl[2048], e_city[256];
            html_escape(city, e_city, sizeof(e_city));
            snprintf(path, sizeof(path), "/room/parcel/%s", ident);
            if (prefers(accept, "application/json") ||
                prefers(accept, "application/ld+json")) {
                /* The same subject, in the grammar the caller asked for. Not a
                 * summary of the page and not a different set of facts: the
                 * page is one projection of this state and so is this. */
                static char doc[8192];
                int ld = prefers(accept, "application/ld+json");
                snprintf(doc, sizeof(doc), ld
                  ? "{\"@context\":\"https://schema.org\",\"@type\":\"Place\","
                    "\"identifier\":\"%s\",\"name\":\"%s\","
                    "\"address\":{\"@type\":\"PostalAddress\",\"streetAddress\":\"%s\","
                    "\"addressLocality\":\"%s\",\"addressRegion\":\"TX\"},"
                    "\"url\":\"%s%s\",\"isBasedOn\":\"observation of a published "
                    "public record; not title and not an appraisal\"}\n"
                  : "{\"account\":\"%s\",\"address\":\"%s\",\"situs\":\"%s\","
                    "\"city\":\"%s\",\"url\":\"%s%s\","
                    "\"observation_of\":\"a published public record\","
                    "\"not\":[\"title\",\"appraisal\",\"permission to contact an owner\"]}\n",
                  ident, e_addr[0] ? e_addr : ident, e_addr[0] ? e_addr : "",
                  city[0] ? city : "", site_origin(), path);
                return cached_reply(doc, ld ? "application/ld+json"
                                            : "application/json; charset=utf-8", NULL);
            }
            w = machine_head(body, sizeof(body), w, path,
                             e_addr[0] ? e_addr : ident,
                             "A parcel in the receipted DFW public record, with its "
                             "assessed value and the work and market around it.", "parcel", ident);
            snprintf(jl, sizeof(jl),
              "<script type=\"application/ld+json\">"
              "{\"@context\":\"https://schema.org\",\"@type\":\"Place\","
              "\"name\":\"%s\",\"identifier\":\"%s\","
              "\"address\":{\"@type\":\"PostalAddress\","
              "\"streetAddress\":\"%s\",\"addressRegion\":\"TX\","
              "\"addressLocality\":\"%s\"},"
              "\"url\":\"%s%s\","
              "\"isBasedOn\":\"Observation of a published public record; not title, "
              "not an appraisal.\"}</script>",
              e_addr[0] ? e_addr : ident, ident, e_addr[0] ? e_addr : "",
              e_city[0] ? e_city : "", site_origin(), path);
            w += (size_t)snprintf(body + w, sizeof(body) - w, "%s", jl);
        }
        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<article class=\"subj room\">"
          "<p class=\"eyebrow\"><i></i>Property room &middot; a place, not a record</p>"
          "<h2 class=\"subj-name\">%s</h2>"

          "<section class=\"room-band\"><h3 class=\"room-h\">The place</h3>"
          "<dl class=\"flow-facts\">"
          "<div><dt>Account</dt><dd class=\"mono\">%s</dd></div>"
          "<div><dt>District</dt><dd>city %s</dd></div>"
          "<div><dt>Class</dt><dd>%s</dd></div>"
          "<div><dt>Built</dt><dd>%s</dd></div>"
          "<div><dt>Living area</dt><dd>%s sq ft</dd></div>"
          "<div><dt>Lot</dt><dd>%s sq ft</dd></div></dl></section>"

          "<section class=\"room-band\"><h3 class=\"room-h\">Who it is carried under</h3>"
          "<p class=\"room-owner\">%s</p>"
          "<p class=\"subj-note\">A taxpayer name of record is not the legal owner, a sole owner, or capacity "
          "to convey.</p></section>"

          "<section class=\"room-band\"><h3 class=\"room-h\">What it is assessed at</h3>"
          "<dl class=\"flow-facts\">"
          "<div><dt>Land</dt><dd>$%s</dd></div>"
          "<div><dt>Total</dt><dd>$%s</dd></div></dl>"
          "<p class=\"subj-note\">An assessment is not an appraisal, a listing price, or an offer.</p></section>"

          "<section class=\"room-band is-absent\"><h3 class=\"room-h\">History and state changes</h3>"
          "<p class=\"subj-note\">This room has none. The estate holds one observation per parcel, so there is "
          "no prior state to show a change against. Nearby occurrences and images are absent for the same "
          "reason: nothing has been admitted that carries them.</p></section>",
          e_addr[0]?e_addr:"(no situs address)", ident, city,
          pc[0]?pc:"&mdash;", yb[0]?yb:"&mdash;", la[0]?la:"&mdash;", ls[0]?ls:"&mdash;",
          e_owner[0]?e_owner:"&mdash;", lv[0]?lv:"&mdash;", tv[0]?tv:"&mdash;");

        /* The room's own actions, still a closure. */
        {
            Action acts[8]; int n = 0; char href[256];
            snprintf(href, sizeof(href), "/s/parcel/%s", ident);
            n = aperture_push(acts, n, 8, "Open the subject", "the parcel with its computed actions", 2, 0, href);
            snprintf(href, sizeof(href), "/property/market?city=%s&amp;account=%s", city, ident);
            n = aperture_push(acts, n, 8, "Compare", "the market this sits in", 2, 0, href);
            if (may_write) {
                snprintf(href, sizeof(href), "/property?watch=%s", ident);
                n = aperture_push(acts, n, 8, "Watch", "opens a claimable mission", 3, 1, href);
            } else if (!signed_in)
                n = aperture_push(acts, n, 8, "Sign in to act", "watching needs write scope", 2, 0, "/login");
            n = aperture_push(acts, n, 8, "Build a package",
                              "not built: a deal package needs terms this estate does not hold", 1, 0, "");
            qsort(acts, (size_t)n, sizeof(Action), aperture_cmp);
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<div class=\"aperture\"><p class=\"ap-label\">In this room you can</p>");
            for (int i = 0; i < n && w < sizeof(body) - 1024; i++) {
                const char *cls = acts[i].strength >= 3 ? "ap-act is-strongest"
                                : acts[i].strength == 2 ? "ap-act is-strong" : "ap-act";
                if (acts[i].href[0])
                    w += (size_t)snprintf(body + w, sizeof(body) - w,
                      "<a class=\"%s\" href=\"%s\"><span class=\"ap-do\">%s</span>"
                      "<span class=\"ap-why\">%s</span></a>", cls, acts[i].href, acts[i].label, acts[i].hint);
                else
                    w += (size_t)snprintf(body + w, sizeof(body) - w,
                      "<span class=\"%s is-dark\"><span class=\"ap-do\">%s</span>"
                      "<span class=\"ap-why\">%s</span></span>", cls, acts[i].label, acts[i].hint);
            }
            w += (size_t)snprintf(body + w, sizeof(body) - w, "</div>");
        }
        w = ambient_render(body, sizeof(body), w, "parcel", ident);
        snprintf(body + w, sizeof(body) - w, "</article>%s", SUBJ_FOOT);
        flow_reply(body);
        return 1;
    }

    if (!strcmp(kind, "capability")) {
        const X402Capability *c = x402_find(ident);
        double usd = 0.0; int priced;
        if (!c) { subject_miss("No capability by that name."); return 1; }
        priced = x402_price_usd(c->name, &usd);
        w = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, c->name);
        {
            /* A priced capability is a schema.org Offer. Shopping and agent
             * crawlers read this; x402 and MPP read the same number from the
             * same book, so the three cannot disagree. */
            char path[256];
            double usd_ld = 0.0;
            int priced_ld = x402_price_usd(c->name, &usd_ld);
            snprintf(path, sizeof(path), "/room/capability/%s", ident);
            w = machine_head(body, sizeof(body), w, path, c->name, c->summary, NULL, NULL);
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<script type=\"application/ld+json\">"
              "{\"@context\":\"https://schema.org\",\"@type\":\"Service\","
              "\"name\":\"%s\",\"description\":\"%s\",\"url\":\"%s%s\","
              "\"provider\":{\"@type\":\"Organization\",\"name\":\"Aurekai\"},"
              "\"offers\":{\"@type\":\"Offer\",\"availability\":\"%s\"%s%.2f%s}}"
              "</script>",
              c->name, c->summary, site_origin(), path,
              priced_ld && usd_ld > 0 ? "https://schema.org/InStock"
                                      : "https://schema.org/OutOfStock",
              priced_ld && usd_ld > 0 ? ",\"priceCurrency\":\"USD\",\"price\":" : "",
              priced_ld && usd_ld > 0 ? usd_ld : 0.0,
              priced_ld && usd_ld > 0 ? "" : "");
        }
        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<article class=\"subj room\">"
          "<p class=\"eyebrow\"><i></i>Capability room &middot; a thing that can be sold</p>"
          "<h2 class=\"subj-name\">%s</h2>"

          "<section class=\"room-band\"><h3 class=\"room-h\">What it does</h3>"
          "<p class=\"room-owner\" style=\"font-size:19px;letter-spacing:-.02em\">%s</p></section>"

          "<section class=\"room-band\"><h3 class=\"room-h\">What it costs, in every grammar</h3>"
          "<dl class=\"flow-facts\">"
          "<div><dt>x402</dt><dd>%s</dd></div>"
          "<div><dt>MPP</dt><dd>%s</dd></div>"
          "<div><dt>UCP / ACP / Beckn</dt><dd>%s</dd></div></dl>"
          "<p class=\"subj-note\">One service book, so these cannot disagree. A quote is not a "
          "reservation.</p></section>"

          "<section class=\"room-band%s\"><h3 class=\"room-h\">Whether it can settle</h3>"
          "<p class=\"subj-note\">%s</p></section>",
          c->name, c->summary,
          priced ? "402 with requirements" : "not_for_sale",
          priced ? "quotes the same number" : "quotes nothing",
          priced ? "listed" : "listed, unpriced",
          x402_facilitator()[0] ? "" : " is-absent",
          x402_facilitator()[0]
            ? "A facilitator is configured, so a presented payment can be verified."
            : "No facilitator is configured. A presented payment cannot be verified, so this resource "
              "answers 402 payment_unverifiable rather than treating a claim of payment as revenue.");
        {
            Action acts[6]; int an = 0; char href[256];
            snprintf(href, sizeof(href), "/s/capability/%s", ident);
            an = aperture_push(acts, an, 6, "Open the subject", "with its computed actions", 2, 0, href);
            snprintf(href, sizeof(href), "/capability/%s", ident);
            an = aperture_push(acts, an, 6, priced ? "Try to buy it" : "See it refuse",
                               priced ? "answers 402 until payment verifies" : "not_for_sale is not free",
                               priced ? 3 : 2, 0, href);
            if (may_write) an = aperture_push(acts, an, 6, "Price it", "writes the book every grammar reads",
                                              3, 1, "/market");
            qsort(acts, (size_t)an, sizeof(Action), aperture_cmp);
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<div class=\"aperture\"><p class=\"ap-label\">In this room you can</p>");
            for (int k = 0; k < an && w < sizeof(body) - 1024; k++)
                w += (size_t)snprintf(body + w, sizeof(body) - w,
                  "<a class=\"%s\" href=\"%s\"><span class=\"ap-do\">%s</span>"
                  "<span class=\"ap-why\">%s</span></a>",
                  acts[k].strength >= 3 ? "ap-act is-strongest" : acts[k].strength == 2 ? "ap-act is-strong" : "ap-act",
                  acts[k].href, acts[k].label, acts[k].hint);
            w += (size_t)snprintf(body + w, sizeof(body) - w, "</div>");
        }
        w = ambient_render(body, sizeof(body), w, "capability", ident);
        snprintf(body + w, sizeof(body) - w, "</article>%s", SUBJ_FOOT);
        flow_reply(body);
        return 1;
    }

    if (!strcmp(kind, "note")) {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
        char title[512]={0}, status[64]={0}, stamp[64]={0};
        char *content = NULL, *ec = NULL, *note_body = NULL;
        size_t escaped_capacity, note_capacity;
        int guest = 0, id = 0;
        char et[1024];
        char note_namespace[192] = {0};
        (void)cms_namespace_for(bearer, auth_db, note_namespace, sizeof(note_namespace));
        if (db && sqlite3_prepare_v2(db,
              "SELECT id,title,status,allow_guest,content,published_at FROM wiki_page WHERE route=?1 AND (namespace=?2 OR (namespace='root' AND ?3) OR (status='published' AND allow_guest=1)) ORDER BY (namespace=?2) DESC,id DESC LIMIT 1",
              -1, &st, NULL) == SQLITE_OK) {
            char route[256]; snprintf(route, sizeof(route), "notes/%s", ident);
            sqlite3_bind_text(st, 1, route, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 2, note_namespace, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 3, signed_in);
            if (sqlite3_step(st) == SQLITE_ROW) {
                id = sqlite3_column_int(st,0);
                snprintf(title,sizeof(title),"%s",(const char*)sqlite3_column_text(st,1));
                snprintf(status,sizeof(status),"%s",sqlite3_column_text(st,2)?(const char*)sqlite3_column_text(st,2):"draft");
                guest = sqlite3_column_int(st,3);
                content = strdup(sqlite3_column_text(st,4) ? (const char*)sqlite3_column_text(st,4) : "");
                if (sqlite3_column_text(st,5)) snprintf(stamp,sizeof(stamp),"%s",(const char*)sqlite3_column_text(st,5));
            }
        }
        if (st) sqlite3_finalize(st);
        if (db) sqlite3_close(db);
        if (!id) { free(content); subject_miss("No note at that route."); return 1; }
        /* A room describes its subject; it is not a second way in.
         *
         * /notes/<route> already refuses a draft, and refuses a published note
         * that is not guest-readable. This room served the same body to anyone
         * who asked -- while printing "readable by: signed-in accounts only" on
         * the same page. Readability is decided once, here, by the same rule the
         * reader path uses, and a viewer who may not read the body is told the
         * note exists and nothing more. Existence is already disclosed by the
         * address they used to arrive. */
        {
            int public_readable = (!strcmp(status, "published") && guest);
            if (!public_readable && !signed_in) {
                free(content);
                subject_miss("That note is not published for public reading. "
                             "Sign in to open it.");
                return 1;
            }
        }
        if (!content || strlen(content) > (SIZE_MAX - 65544) / 6) {
            free(content);
            flow_reply_status(503, "Service Unavailable", "The note could not be loaded in full. Please try again.");
            return 1;
        }
        escaped_capacity = strlen(content) * 6 + 8;
        note_capacity = escaped_capacity + 65536;
        ec = malloc(escaped_capacity);
        note_body = malloc(note_capacity);
        if (!ec || !note_body) {
            free(content); free(ec); free(note_body);
            flow_reply_status(503, "Service Unavailable", "The note could not be loaded in full. Please try again.");
            return 1;
        }
        html_escape(title,et,sizeof(et));
        html_escape(content,ec,escaped_capacity);
        free(content);
        w = (size_t)snprintf(note_body, note_capacity, SUBJ_HEAD, et);
        w += (size_t)snprintf(note_body + w, note_capacity - w,
          "<article class=\"subj room\">"
          "<p class=\"eyebrow\"><i></i>Note room &middot; something written down</p>"
          "<h2 class=\"subj-name fl-serif\">%s</h2>"
          "<section class=\"room-band\"><h3 class=\"room-h\">What it says</h3>"
          "<p class=\"room-read room-note\">%s</p></section>"
          "<section class=\"room-band\"><h3 class=\"room-h\">Who can read it</h3>"
          "<dl class=\"flow-facts\">"
          "<div><dt>Status</dt><dd>%s</dd></div>"
          "<div><dt>Readable by</dt><dd>%s</dd></div>"
          "<div><dt>Published</dt><dd>%s</dd></div></dl>"
          "<p class=\"subj-note\">Publishing and readability are separate. A published note that is not "
          "guest-readable answers 401 to a stranger and never appears in the index.</p></section>",
          et, ec[0]?ec:"&mdash;", status,
          guest ? "anyone" : "signed-in accounts only",
          stamp[0]?stamp:"not yet");
        {
            Action acts[6]; int an = 0; char href[256];
            if (!strcmp(status,"published") && guest) {
                snprintf(href, sizeof(href), "/notes/%s", ident);
                an = aperture_push(acts, an, 6, "Read it as a reader sees it", "published and open", 3, 0, href);
            }
            snprintf(href, sizeof(href), "/s/note/%s", ident);
            an = aperture_push(acts, an, 6, "Open the subject", "with its computed actions", 2, 0, href);
            if (may_write) an = aperture_push(acts, an, 6, "Edit it", "in the content studio", 2, 1, "/cms/");
            qsort(acts, (size_t)an, sizeof(Action), aperture_cmp);
            w += (size_t)snprintf(note_body + w, note_capacity - w,
              "<div class=\"aperture\"><p class=\"ap-label\">In this room you can</p>");
            for (int k = 0; k < an && w < note_capacity - 1024; k++)
                w += (size_t)snprintf(note_body + w, note_capacity - w,
                  "<a class=\"%s\" href=\"%s\"><span class=\"ap-do\">%s</span>"
                  "<span class=\"ap-why\">%s</span></a>",
                  acts[k].strength >= 3 ? "ap-act is-strongest" : acts[k].strength == 2 ? "ap-act is-strong" : "ap-act",
                  acts[k].href, acts[k].label, acts[k].hint);
            w += (size_t)snprintf(note_body + w, note_capacity - w, "</div>");
        }
        w = ambient_render(note_body, note_capacity, w, "note", ident);
        snprintf(note_body + w, note_capacity - w, "</article>%s", SUBJ_FOOT);
        flow_reply(note_body);
        free(ec); free(note_body);
        return 1;
    }

    subject_miss("Rooms exist for parcels, capabilities and notes. A room is shaped by what its subject "
                 "is, so each kind is built rather than templated.");
    return 1;
}

/* Percent-encode a value for a query string. */
static void url_escape_into(const char *in, char *out, size_t cap) {
    static const char *hex = "0123456789ABCDEF";
    size_t w = 0;
    for (const char *p = in; *p && w + 4 < cap; p++) {
        unsigned char c = (unsigned char)*p;
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') out[w++] = (char)c;
        else { out[w++] = '%'; out[w++] = hex[c >> 4]; out[w++] = hex[c & 15]; }
    }
    out[w] = '\0';
}

static const char *bindir_or_default(void) {
    const char *e = getenv("BONFYRE_BIN_DIR");
    return (e && *e) ? e : "/usr/local/bin";
}

/* Escape a value for embedding in a JSON string. */
/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 */
static void json_escape_into(const char *in, char *out, size_t cap) {
    bf_lex_json_escape(in, out, cap);
}

/* ── COMPOSE: creation makes reality, not posts ──────────────────────────
 *
 * Source E's rule: the prompt is not "what would you like to post" but "what
 * are you making, doing, starting". So the sentence is read for what it is
 * proposing to bring into being, and the reading is shown before anything is
 * created -- the person confirms a typed object, not a guess applied silently.
 *
 * Publishing is one projection of creation, not the point of it. Each type
 * below lands in a real owner: work in the WorkGraph, a source under the
 * foreign law, an offer in the service book, a note in the CMS.
 */
typedef struct {
    const char *type;      /* what it would become */
    const char *owner;     /* which organ would hold it */
    const char *why;       /* the reading, in words */
    const char *verb;      /* the surface that creates it */
    int confidence;        /* 3 clear, 2 likely, 1 fallback */
} Reading;

static int compose_has(const char *hay, const char *needle) {
    size_t n = strlen(needle);
    for (const char *p = hay; *p; p++) {
        size_t i = 0;
        while (i < n && p[i] && tolower((unsigned char)p[i]) == tolower((unsigned char)needle[i])) i++;
        if (i == n) return 1;
    }
    return 0;
}

/* Read a sentence for what it proposes to create. Deliberately legible rules
 * rather than a model: the person is shown the reading and can pick another,
 * so a wrong guess costs a click, not a wrong object. */
static int compose_read(const char *text, Reading *out, int cap) {
    int n = 0;
    int is_url = (!strncmp(text, "http://", 7) || !strncmp(text, "https://", 8)) ||
                 compose_has(text, "http://") || compose_has(text, "https://");
    int wants_help = compose_has(text,"need ") || compose_has(text,"help") ||
                     compose_has(text,"fix ")  || compose_has(text,"translate") ||
                     compose_has(text,"review") || compose_has(text,"looking for");
    int is_offer   = compose_has(text,"selling") || compose_has(text,"for sale") ||
                     compose_has(text,"offer")   || compose_has(text,"$") ||
                     compose_has(text,"price");
    int is_group   = compose_has(text,"starting a") || compose_has(text,"we're starting") ||
                     compose_has(text,"neighborhood") || compose_has(text,"community") ||
                     compose_has(text,"crew");

    /* Some sentences propose more than one thing at once. E's example is a
     * neighbourhood clean-up: it gathers people AND asks for work to be done,
     * and reading it as only one of those loses half of what was said. The
     * combination is offered first and creates its parts together; the single
     * readings stay available underneath, weakened rather than removed, because
     * a wrong combination should cost a click and not a lost option. */
    int combo_gw = is_group && wants_help;
    int combo_fo = is_offer && is_url;

    if (combo_gw && n < cap) {
        out[n].type="Group+Work"; out[n].owner="the identity graph and the WorkGraph";
        out[n].why="it gathers people and asks for something to be done, so it becomes both at once";
        out[n].verb="/make/create"; out[n].confidence=3; n++;
    }
    if (combo_fo && n < cap) {
        out[n].type="ForeignTwin+Offer"; out[n].owner="the foreign law and the service book";
        out[n].why="it points outside and proposes an exchange, so the source is observed and the offer listed";
        out[n].verb="/make/create"; out[n].confidence=3; n++;
    }
    if (is_url && n < cap) {
        out[n].type="ForeignTwin"; out[n].owner="the foreign observation law";
        out[n].why="it names something outside this world, so it is observed rather than asserted";
        out[n].verb="/studio/v1/assets/observe"; out[n].confidence=combo_fo?1:3; n++;
    }
    if (wants_help && n < cap) {
        out[n].type="Work"; out[n].owner="the WorkGraph";
        out[n].why="it asks for something to be done, so it becomes a claimable mission";
        out[n].verb="/studio/v1/fleming/watch"; out[n].confidence=combo_gw?1:3; n++;
    }
    if (is_offer && n < cap) {
        out[n].type="Offer"; out[n].owner="the service book";
        out[n].why="it proposes an exchange, so it becomes something the market can quote";
        out[n].verb="/studio/v1/economy/offer"; out[n].confidence=combo_fo?1:3; n++;
    }
    if (is_group && n < cap) {
        out[n].type="Group"; out[n].owner="the identity graph";
        out[n].why="it gathers people, so it becomes an entity others can be related to";
        out[n].verb="/studio/v1/identity/group"; out[n].confidence=combo_gw?1:2; n++;
    }
    if (n < cap) {
        out[n].type="Note"; out[n].owner="the CMS";
        out[n].why="anything can be written down and addressed; publishing is a separate decision";
        out[n].verb="/cms/v1/api/wiki_page"; out[n].confidence=1; n++;
    }
    return n;
}

/* Derive a slug from a sentence: lowercase, words joined by dashes, bounded.
 * Used for routes and identifiers, so it is restricted to what those admit. */
static void compose_slug(const char *text, char *out, size_t cap, const char *prefix) {
    size_t w = 0;
    int last_dash = 1;
    if (prefix && *prefix) { w = (size_t)snprintf(out, cap, "%s", prefix); last_dash = 0; }
    for (const char *p = text; *p && w + 2 < cap; p++) {
        unsigned char c = (unsigned char)*p;
        if (isalnum(c)) { out[w++] = (char)tolower(c); last_dash = 0; }
        else if (!last_dash) { out[w++] = '-'; last_dash = 1; }
    }
    while (w && out[w-1] == '-') w--;
    out[w] = '\0';
    if (!out[0]) snprintf(out, cap, "%suntitled", prefix ? prefix : "");
}

/* Pull the first plausible http(s) URL out of a sentence. */
static int compose_url(const char *text, char *out, size_t cap) {
    const char *p = strstr(text, "http");
    if (!p) return 0;
    {
        size_t w = 0;
        while (*p && !isspace((unsigned char)*p) && w + 1 < cap) out[w++] = *p++;
        out[w] = '\0';
    }
    return (!strncmp(out, "http://", 7) || !strncmp(out, "https://", 8));
}

/* Create the chosen type for real, through the organ that owns it.
 * Returns 1 when something was created and fills `where` with its subject. */
static int compose_create(const char *bindir, const char *type, const char *text,
                          const char *bearer, const char *auth_db, char *out, size_t out_sz,
                          char *where, size_t where_sz)
{
    char diag[2048];
    where[0] = '\0';

    if (!strcmp(type, "Work")) {
        char slug[64], workdb[PATH_MAX], qbin[PATH_MAX], actor[160], subject[160], title[161];
        const char *scope = NULL;
        if (psetup_context(bearer, auth_db, "self", 2, actor, sizeof(actor), subject, sizeof(subject), &scope) != 1) return 0;
        compose_slug(text, slug, sizeof(slug), NULL);
        slug[31] = '\0';                      /* the queue's input field is bounded */
        snprintf(title, sizeof(title), "Work: %s", slug);
        snprintf(qbin, sizeof(qbin), "%s/bonfyre-queue", bindir);
        snprintf(workdb, sizeof(workdb), "%s/fleming-work.db", fleming_root());
        if (access(qbin, X_OK) != 0 || access(workdb, R_OK) != 0) return 0;
        {
            char *a[] = { qbin, (char*)"enqueue", (char*)"composed-work", slug,
                          (char*)"--db", workdb, "--subject", subject, "--scope", (char *)scope,
                          "--title", title, "--continuation", "/apps?context=self", NULL };
            if (auth_call_split(a, NULL, out, out_sz, diag, sizeof(diag)) != 0 || !out[0]) return 0;
        }
        {
            char mid[128] = {0};
            studio_json_field(out, "mission_id", mid, sizeof(mid));
            snprintf(where, where_sz, "/apps?context=self");
            (void)mid;
        }
        return 1;
    }

    if (!strcmp(type, "Group")) {
        char slug[97], ebin[PATH_MAX], value[160];
        compose_slug(text, slug, sizeof(slug), NULL);
        slug[64] = '\0';
        snprintf(ebin, sizeof(ebin), "%s/bonfyre-entity", bindir);
        snprintf(value, sizeof(value), "group:%s", slug);
        if (access(ebin, X_OK) != 0) return 0;
        {
            char *a[] = { ebin, (char*)"resolve", (char*)"text", value,
                          (char*)"--kind", (char*)"org", NULL };
            if (auth_call_split(a, NULL, out, out_sz, diag, sizeof(diag)) != 0) return 0;
        }
        snprintf(where, where_sz, "/studio/v1/clients");
        return 1;
    }

    if (!strcmp(type, "ForeignTwin")) {
        char url[1400], twin[97], receipt[PATH_MAX], retained[PATH_MAX], stamp[32], pbin[PATH_MAX];
        FILE *rf; size_t got = 0;
        if (!compose_url(text, url, sizeof(url))) return 0;
        iso_utc_now(stamp, sizeof(stamp));
        for (char *z = stamp; *z; z++) if (!isdigit((unsigned char)*z)) *z = '-';
        snprintf(twin, sizeof(twin), "composed-%s", stamp);
        (void)mkdir("/var/lib/bonfyre/foreign", 0750);
        (void)mkdir("/var/lib/bonfyre/foreign/receipts", 0750);
        (void)mkdir("/var/lib/bonfyre/foreign/bodies", 0750);
        snprintf(receipt, sizeof(receipt), "/var/lib/bonfyre/foreign/receipts/%s.json", twin);
        snprintf(retained, sizeof(retained), "/var/lib/bonfyre/foreign/bodies/%s.body", twin);
        snprintf(pbin, sizeof(pbin), "%s/bonfyre-project", bindir);
        if (access(pbin, X_OK) != 0) return 0;
        {
            char *a[] = { pbin, (char*)"foreign", (char*)"observe", twin, url,
                          (char*)"--receipt", receipt, (char*)"--body", retained,
                          (char*)"--observe-right", (char*)"public", NULL };
            (void)auth_call(a, NULL, out, out_sz);
        }
        rf = fopen(receipt, "rb");
        if (rf) { got = fread(out, 1, out_sz - 1, rf); out[got] = '\0'; fclose(rf); }
        if (!got) return 0;
        snprintf(where, where_sz, "/sources");
        return 1;
    }

    if (!strcmp(type, "Offer")) {
        char name[97], fbin[PATH_MAX];
        compose_slug(text, name, sizeof(name), NULL);
        name[48] = '\0';
        snprintf(fbin, sizeof(fbin), "%s/bonfyre-finance", bindir);
        if (access(fbin, X_OK) != 0) return 0;
        {
            /* An offer composed from a sentence carries no price yet: the
             * sentence proposes an exchange, it does not set a number. It is
             * created unpriced, which reads as not_for_sale until someone
             * prices it deliberately. */
            char *a[] = { fbin, (char*)"service", (char*)"add", (char*)"--name", name,
                          (char*)"--buy", (char*)"0", (char*)"--sell", (char*)"0",
                          (char*)"--source", (char*)"composed", NULL };
            if (auth_call_split(a, NULL, out, out_sz, diag, sizeof(diag)) != 0 || !out[0]) return 0;
        }
        snprintf(where, where_sz, "/market");
        return 1;
    }

    /* A combination is created part by part. If a later part fails the earlier
     * ones still exist, so the reply names exactly what was made rather than
     * implying the whole reading succeeded. */
    if (strchr(type, '+')) {
        char parts[4][32]; int np = 0, madeN = 0;
        char one[16384], where_one[256], names[256] = {0};
        const char *p2 = type;
        while (*p2 && np < 4) {
            size_t k = 0;
            while (*p2 && *p2 != '+' && k + 1 < sizeof(parts[0])) parts[np][k++] = *p2++;
            parts[np][k] = '\0';
            if (*p2 == '+') p2++;
            if (parts[np][0]) np++;
        }
        out[0] = '\0';
        for (int k = 0; k < np; k++) {
            int ok = compose_create(bindir, parts[k], text, bearer, auth_db, one, sizeof(one),
                                    where_one, sizeof(where_one));
            /* One line per part, made or not, so the confirmation can say what
             * actually landed instead of implying the whole reading succeeded. */
            snprintf(out + strlen(out), out_sz - strlen(out), "%s\t%s\t%s\n",
                     parts[k], ok ? "made" : "failed", ok && where_one[0] ? where_one : "-");
            if (ok) {
                madeN++;
                snprintf(names + strlen(names), sizeof(names) - strlen(names),
                         "%s%s", names[0] ? " and " : "", parts[k]);
                if (!where[0]) snprintf(where, where_sz, "%s", where_one);
            }
        }
        return madeN > 0;
    }

    if (!strcmp(type, "Note")) {
        /* The CMS owns validation and the published_at stamp, so this goes
         * through its HTTP surface rather than its store. */
        /* payload holds three escaped fields; escaping can double a value's
         * length, so it is sized for that rather than for the raw text. */
        char route[192], title[256], payload[8192], cmd_url[256];
        compose_slug(text, route, sizeof(route), "notes/");
        route[96] = '\0';
        snprintf(title, sizeof(title), "%.120s", text);
        {
            char et[768], er[512], ec[3072];
            json_escape_into(title, et, sizeof(et));
            json_escape_into(route, er, sizeof(er));
            json_escape_into(text,  ec, sizeof(ec));
            snprintf(payload, sizeof(payload),
              "{\"title\":\"%s\",\"route\":\"%s\",\"content\":\"%s\"}", et, er, ec);
        }
        snprintf(cmd_url, sizeof(cmd_url), "http://127.0.0.1:8790/v1/api/wiki_page");
        {
            char auth[192];
            snprintf(auth, sizeof(auth), "Authorization: Bearer %s", bearer ? bearer : "");
            char *a[] = { (char*)"curl", (char*)"-sS", (char*)"-m", (char*)"30",
                          (char*)"-X", (char*)"POST", cmd_url,
                          (char*)"-H", (char*)"Content-Type: application/json",
                          (char*)"-H", auth, (char*)"-d", payload, NULL };
            if (auth_call_split(a, NULL, out, out_sz, diag, sizeof(diag)) != 0 || !out[0]) return 0;
        }
        if (!strstr(out, "\"id\"")) return 0;
        snprintf(where, where_sz, "/s/note/%s", route + 6);
        return 1;
    }
    return 0;
}

/* Browser captures join the existing Desk and Make flow. The queue remains
 * their owner. Legacy captures have no authenticated person binding, so only
 * an administrator of the configured institution may disclose them. */
typedef struct {
    char id[64], url[2049], selection[8193], note[1601];
} HeldPageSelection;

static int held_page_allowed(const char *bearer, const char *auth_db) {
    const char *institution = getenv("BONFYRE_BROWSER_QUEUE_INSTITUTION");
    return session_institution_authorized(bearer, auth_db,
        institution && *institution ? institution : "bonfyre", "admin");
}

static int held_page_id(const char *id) {
    if (!id || strlen(id) != 39 || strncmp(id, "page-selection-", 15)) return 0;
    for (const char *p = id + 15; *p; p++)
        if (!((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f'))) return 0;
    return 1;
}

static int held_page_text(sqlite3_stmt *st, int column, char *out, size_t cap) {
    const unsigned char *text = sqlite3_column_text(st, column);
    int bytes = sqlite3_column_bytes(st, column);
    out[0] = '\0';
    if (!text) return 1;
    if (bytes < 0 || (size_t)bytes >= cap || memchr(text, 0, (size_t)bytes)) return 0;
    memcpy(out, text, (size_t)bytes); out[bytes] = '\0'; return 1;
}

static int held_page_read(const char *id, HeldPageSelection *held) {
    const char *path = getenv("BONFYRE_BROWSER_QUEUE_DB");
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int found = 0;
    memset(held, 0, sizeof(*held));
    if (!path || !*path || !held_page_id(id)) return 0;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 0;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT json_extract(input_uri,'$.url'),json_extract(input_uri,'$.selection'),"
        "COALESCE(json_extract(input_uri,'$.note'),'') FROM workgraph_nodes "
        "WHERE mission_id=? AND family='held-selection' AND operator_id='queue.page.selection' "
        "AND json_valid(input_uri) AND json_type(input_uri,'$.url')='text' "
        "AND json_type(input_uri,'$.selection')='text' "
        "AND status NOT IN ('complete','completed','failed','cancelled','dead_letter') LIMIT 2",
        -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW &&
            held_page_text(st, 0, held->url, sizeof(held->url)) &&
            held_page_text(st, 1, held->selection, sizeof(held->selection)) &&
            held_page_text(st, 2, held->note, sizeof(held->note)) &&
            held->selection[0] && !strncmp(held->url, "https://", 8) &&
            sqlite3_step(st) == SQLITE_DONE) {
            snprintf(held->id, sizeof(held->id), "%s", id); found = 1;
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db); return found;
}

static int serve_held_page_make(const char *target, const char *query,
                               const char *bearer, const char *auth_db) {
    char id[128], type[32]; HeldPageSelection held;
    static char body[131072], quoted[84000], note[12000], url[13000], content[14000];
    size_t w; int form = !strcmp(target, "/make/form");
    if (strcmp(target, "/make") && strcmp(target, "/make/") && !form) return 0;
    if (!query_value(query, "held_selection", id, sizeof(id))) return 0;
    if (!held_page_allowed(bearer, auth_db)) {
        html_error(403, "Institution access required", "This held selection has no personal ownership binding. Only the configured institution administrator may review it.");
        return 1;
    }
    if (!held_page_read(id, &held)) {
        html_error(404, "Selection unavailable", "The exact held selection is unavailable, resolved, or cannot be read completely. Nothing was created.");
        return 1;
    }
    html_escape(held.note, note, sizeof(note));
    html_escape(held.url, url, sizeof(url));
    if (form) {
        if (!query_value(query, "type", type, sizeof(type)) || strcmp(type, "Note")) {
            html_error(400, "Choose a creation", "A retained selection can be carried into a working note. Other actions use the ordinary creation flow."); return 1;
        }
        if (!session_authorized(bearer, auth_db, "write")) {
            html_error(403, "Write access required", "Reviewing this selection does not grant permission to create a CMS record."); return 1;
        }
        int n = snprintf(content, sizeof(content), "Purpose: %s\n\nObserved page: %s\nHeld selection: %s\n\nSelected material (unverified):\n%s",
                         held.note, held.url, held.id, held.selection);
        if (n < 0 || (size_t)n >= sizeof(content)) {
            html_error(413, "Selection too large", "The complete selection cannot fit this draft. It remains in its owner without truncation."); return 1;
        }
        html_escape(content, quoted, sizeof(quoted));
        snprintf(body, sizeof(body),
            "<div id=\"cms-pane\"><form class=\"mk-commit\" method=\"post\" action=\"/cms/v1/api/wiki_page\" "
            "hx-post=\"/cms/v1/api/wiki_page\" hx-target=\"#make-commit\" hx-swap=\"innerHTML\">"
            "<p class=\"ap-label\">Continue in the native workspace</p>"
            "<label>Title<input name=\"title\" required maxlength=\"240\" value=\"Working note from a selected page\"></label>"
            "<label>Private route<input name=\"route\" required value=\"notes/%s\"></label>"
            "<label>Working material<textarea name=\"content\" required rows=\"14\">%s</textarea></label>"
            "<input type=\"hidden\" name=\"status\" value=\"draft\">"
            "<p class=\"subj-note\">The native CMS validates and keeps this draft in your account namespace. "
            "The original selection stays in Queue; this is not publication or a supplier action.</p>"
            "<button class=\"button primary\" type=\"submit\">Create working draft <span>&rarr;</span></button>"
            "</form><div id=\"make-commit\"></div></div>", held.id, quoted);
        flow_reply(body); return 1;
    }
    html_escape(held.selection, quoted, sizeof(quoted));
    w = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, "Continue from a selected page");
    snprintf(body + w, sizeof(body) - w,
        "<article class=\"subj\"><p class=\"eyebrow\"><i></i>Compose &middot; retained browser material</p>"
        "<h2 class=\"subj-name\">Continue where you left off</h2>"
        "<p class=\"subj-desc\">%s</p><p><a href=\"%s\" rel=\"noreferrer\">Read the original page</a></p>"
        "<section class=\"desk-q\"><h3>What was selected</h3><pre>%s</pre>"
        "<p class=\"subj-note\">Quoted source material, not verified terms or instructions to execute. Reference: %s.</p></section>"
        "<a class=\"button primary\" href=\"/make/form?type=Note&amp;held_selection=%s\" "
        "hx-get=\"/make/form?type=Note&amp;held_selection=%s\" hx-target=\"#make-read\" hx-swap=\"innerHTML\">"
        "Make a working note <span>&rarr;</span></a><div id=\"make-read\"></div>"
        "<form class=\"flow-search\" action=\"/make/read\" method=\"get\" hx-get=\"/make/read\" hx-target=\"#make-read\" hx-swap=\"innerHTML\">"
        "<label>Or describe what you are making<input name=\"text\" required maxlength=\"900\" value=\"%s\"></label>"
        "<button class=\"button\" type=\"submit\">Read it <span>&rarr;</span></button></form>"
        "<p><a href=\"/studio/\">Back to the Desk</a></p></article>"
        "<script defer src=\"/assets/matrix-htmx.js\"></script>%s",
        held.note[0] ? note : "A page selection is waiting for your next step.", url, quoted, held.id,
        held.id, held.id, note, SUBJ_FOOT);
    flow_reply(body); return 1;
}

static int serve_compose(const char *method, const char *target, const char *query,
                         const char *req_body, const char *bearer, const char *auth_db)
{
    static char body[131072];
    char raw[1024], text[1024], esc[2048];
    int signed_in, may_write;
    size_t w = 0;

    if (strncmp(target, "/make", 5)) return 0;
    if (!strcmp(method, "GET") && serve_held_page_make(target, query, bearer, auth_db)) return 1;
    /* Creating is the one write here, so it is the one POST. Reading a sentence
     * and drawing a form change nothing and stay GETs. */
    {
        int is_create = !strncmp(target, "/make/create", 12);
        if (strcmp(method, "GET") && !(is_create && !strcmp(method, "POST"))) return 0;
        /* A posted form arrives in the body in exactly the shape a query string
         * has, so the same reader serves both. */
        if (is_create && !strcmp(method, "POST")) query = req_body ? req_body : "";
    }
    signed_in = session_valid(bearer, auth_db);
    may_write = signed_in && session_authorized(bearer, auth_db, "write");

    if (!strcmp(target, "/make/form")) {
        char type[64];
        if (!may_write) { flow_reply("<p class=\"flow-empty\">Creating needs a write-scoped account.</p>"); return 1; }
        if (!query_value(query, "type", type, sizeof(type))) { flow_reply("<p class=\"flow-empty\">Choose what to make.</p>"); return 1; }
        query_value(query, "text", raw, sizeof(raw)); html_escape(raw, esc, sizeof(esc));
        /* A combination arrives as "Group Work" once the + is url-decoded. */
        for (char *z = type; *z; z++) if (*z == ' ') *z = '+';
        if (strchr(type, '+')) {
            /* A combination commits through /make/create, which walks the parts
             * in order. The form names every part before anything is written, so
             * the person is agreeing to all of them and not just the first. */
            char parts[256] = {0}; char et[64];
            html_escape(type, et, sizeof(et));
            for (const char *z = type; *z; ) {
                const char *e2 = strchr(z, '+'); size_t len = e2 ? (size_t)(e2 - z) : strlen(z);
                snprintf(parts + strlen(parts), sizeof(parts) - strlen(parts),
                         "<li>%.*s</li>", (int)len, z);
                z = e2 ? e2 + 1 : z + len;
            }
            snprintf(body, sizeof(body),
              "<form class=\"mk-commit\" hx-post=\"/make/create\" hx-target=\"#make-commit\" hx-swap=\"innerHTML\">"
              "<p class=\"ap-label\">Make both at once</p>"
              "<input type=\"hidden\" name=\"type\" value=\"%s\">"
              "<input type=\"hidden\" name=\"text\" value=\"%s\">"
              "<ul class=\"mk-parts\">%s</ul>"
              "<p class=\"subj-note\">Each part is created by the organ that owns it, in order. "
              "If a later part fails the earlier ones still exist, and the reply says exactly "
              "what was made.</p>"
              "<button class=\"button primary\">Make them <span>&rarr;</span></button></form>"
              "<div id=\"make-commit\"></div>", et, esc, parts);
            flow_reply(body); return 1;
        }
        if (!strcmp(type, "ForeignTwin")) {
            snprintf(body, sizeof(body), "<form class=\"mk-commit\" hx-post=\"/studio/v1/assets/observe\" hx-target=\"#make-commit\" hx-swap=\"innerHTML\"><p class=\"ap-label\">Observe a foreign source</p><label>Public address<input name=\"url\" type=\"url\" required value=\"%s\"></label><p class=\"subj-note\">This retains a receipted public observation. It does not claim the source, an account, or a right to act there.</p><button class=\"button primary\">Observe it <span>&rarr;</span></button></form><div id=\"make-commit\"></div>", esc);
        } else if (!strcmp(type, "Group")) {
            snprintf(body, sizeof(body), "<form class=\"mk-commit\" hx-post=\"/studio/v1/identity/group\" hx-target=\"#make-commit\" hx-swap=\"innerHTML\"><p class=\"ap-label\">Start a group</p><label>Short address<input name=\"slug\" pattern=\"[a-z0-9][a-z0-9-]{0,95}\" required placeholder=\"neighborhood-cleanup\"></label><p class=\"subj-note\">This resolves an organization in the identity graph. Membership, governance, money, and publication remain separate subsequent acts.</p><button class=\"button primary\">Create group <span>&rarr;</span></button></form><div id=\"make-commit\"></div>");
        } else if (!strcmp(type, "Offer")) {
            snprintf(body, sizeof(body), "<form class=\"mk-commit\" hx-post=\"/studio/v1/economy/offer\" hx-target=\"#make-commit\" hx-swap=\"innerHTML\"><p class=\"ap-label\">Open an offer</p><label>Offer key<input name=\"name\" pattern=\"[A-Za-z][A-Za-z0-9_-]{0,95}\" required placeholder=\"monstera-cutting\"></label><div class=\"mk-money\"><label>Carrying cost (USD)<input name=\"buy\" type=\"number\" min=\"0\" step=\"0.01\" required></label><label>Asking price (USD)<input name=\"sell\" type=\"number\" min=\"0\" step=\"0.01\" required></label></div><input type=\"hidden\" name=\"source\" value=\"compose\"><p class=\"subj-note\">This records a service offer in the operating book. It is not an invoice, payment, or settlement.</p><button class=\"button primary\">Open offer <span>&rarr;</span></button></form><div id=\"make-commit\"></div>");
        } else if (!strcmp(type, "Note")) {
            snprintf(body, sizeof(body), "<form class=\"mk-commit\" hx-post=\"/cms/v1/api/wiki_page\" hx-target=\"#make-commit\" hx-swap=\"innerHTML\"><p class=\"ap-label\">Write it down</p><label>Title<input name=\"title\" required placeholder=\"A clear title\"></label><label>Private route<input name=\"route\" required placeholder=\"notes/next-step\"></label><label>Note<textarea name=\"content\" required>%s</textarea></label><input type=\"hidden\" name=\"status\" value=\"draft\"><p class=\"subj-note\">This begins as a draft. Publication and guest readability are separate lifecycle decisions.</p><button class=\"button primary\">Create draft <span>&rarr;</span></button></form><div id=\"make-commit\"></div>", esc);
        } else { flow_reply("<p class=\"flow-empty\">This needs a named program before it can become work. A sentence alone must not invent a mission carrier.</p>"); return 1; }
        flow_reply(body); return 1;
    }

    /* The reading, before anything exists. */
    if (!strncmp(target, "/make/read", 10)) {
        Reading r[6];
        int n;
        if (!query_value(query, "text", raw, sizeof(raw)) || !raw[0]) {
            flow_reply("<p class=\"flow-empty\">Say what you are making.</p>"); return 1;
        }
        snprintf(text, sizeof(text), "%s", raw);
        /* The text rides in the link so choosing a type needs no stored state. */
        url_escape_into(text, esc, sizeof(esc));
        n = compose_read(text, r, 6);
        w = (size_t)snprintf(body, sizeof(body),
          "<div class=\"mk-read\"><p class=\"ap-label\">What this would become</p>");
        for (int i = 0; i < n && w < sizeof(body) - 1536; i++) {
            const char *cls = r[i].confidence >= 3 ? "ap-act is-strongest"
                            : r[i].confidence == 2 ? "ap-act is-strong" : "ap-act";
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<%s class=\"%s\"%s hx-get=\"/make/form?type=%s\" hx-include=\"#make-source\" hx-target=\"#make-read\" hx-swap=\"innerHTML\">"
              "<span class=\"ap-do\">%s</span>"
              "<span class=\"ap-why\">%s &mdash; held by %s</span></%s>",
              may_write ? "button" : "span", cls, may_write ? " type=\"button\"" : "",
              r[i].type, r[i].type, r[i].why, r[i].owner,
              may_write ? "button" : "span");
        }
        snprintf(body + w, sizeof(body) - w,
          "<p class=\"subj-note\">%s A reading is a proposal: nothing exists until you choose one, and "
          "choosing a different one is a click, not a correction.</p></div>",
          may_write ? "Pick what it should become."
                    : (signed_in ? "Your account reads but cannot create, so these are shown and not offered."
                                 : "Creating needs a write-scoped account."));
        flow_reply(body);
        return 1;
    }

    /* Making it exist. This is the only place in the flow that writes. */
    if (!strncmp(target, "/make/create", 12)) {
        static char created[65536];
        char type[64], where[256];
        if (!may_write) {
            flow_reply(signed_in
              ? "<p class=\"flow-empty\">This account is signed in but not authorized to create.</p>"
              : "<p class=\"flow-empty\">Creating needs a write-scoped account. "
                "<a class=\"text-link\" href=\"/login\">Sign in <span>&rarr;</span></a></p>");
            return 1;
        }
        if (!query_value(query, "type", type, sizeof(type)) ||
            !query_value(query, "text", raw, sizeof(raw)) || !raw[0]) {
            flow_reply("<p class=\"flow-empty\">Nothing to make.</p>"); return 1;
        }
        /* A combination arrives as "Group Work" once the + is decoded; restore
         * it, then hold the whole thing to the same character class. */
        for (char *z = type; *z; z++) if (*z == ' ') *z = '+';
        {
            int ok = type[0] != '\0';
            for (const char *z = type; *z && ok; z++)
                if (!(isalnum((unsigned char)*z) || *z == '+')) ok = 0;
            if (!ok) {
                flow_reply("<p class=\"flow-empty\">Nothing to make.</p>");
                return 1;
            }
        }
        snprintf(text, sizeof(text), "%s", raw);
        if (!compose_create(bindir_or_default(), type, text, bearer, auth_db,
                            created, sizeof(created), where, sizeof(where))) {
            char msg[512];
            snprintf(msg, sizeof(msg),
              "<p class=\"flow-empty\">That could not be made as a %s. "
              "Nothing was created, and the other readings are still open.</p>", type);
            flow_reply(msg);
            return 1;
        }
        html_escape(text, esc, sizeof(esc));
        if (strchr(type, '+')) {
            /* A combination reports part by part. A part that failed is named as
             * failed rather than omitted, because the person asked for both and
             * is owed the difference between what they asked for and what is
             * now real. */
            char line[256]; const char *p2 = created; int failed = 0;
            w = (size_t)snprintf(body, sizeof(body),
                  "<div class=\"mk-made\"><p class=\"ap-label\">What is real now</p>");
            while (*p2 && w < sizeof(body) - 1024) {
                const char *nl = strchr(p2, '\n');
                size_t len = nl ? (size_t)(nl - p2) : strlen(p2);
                char pt[64] = {0}, st[16] = {0}, wh[160] = {0};
                if (len >= sizeof(line)) len = sizeof(line) - 1;
                memcpy(line, p2, len); line[len] = '\0';
                if (sscanf(line, "%63[^\t]\t%15[^\t]\t%159[^\n]", pt, st, wh) == 3) {
                    int made = !strcmp(st, "made");
                    if (!made) failed = 1;
                    w += (size_t)snprintf(body + w, sizeof(body) - w,
                      "<a class=\"ap-act%s\" href=\"%s\"><span class=\"ap-do\">%s %s</span>"
                      "<span class=\"ap-why\">%s</span></a>",
                      made ? " is-strongest" : "", made && wh[0] != '-' ? wh : "/make",
                      pt, made ? "created" : "not created",
                      made ? "open it where its organ keeps it"
                           : "the organ that owns this kind refused it, so nothing of it exists");
                }
                if (!nl) break;
                p2 = nl + 1;
            }
            snprintf(body + w, sizeof(body) - w,
              "<p class=\"subj-note\">From &ldquo;%s&rdquo;. %sEach part was made by the organ that owns "
              "it, so each is the same object every other surface sees.</p></div>",
              esc,
              failed ? "The parts that failed left nothing behind, and can be made on their own. " : "");
            flow_reply(body);
            return 1;
        }
        snprintf(body, sizeof(body),
          "<div class=\"mk-made\">"
          "<p class=\"ap-label\">It exists now</p>"
          "<article class=\"ap-act is-strongest\">"
          "<span class=\"ap-do\">%s created</span>"
          "<span class=\"ap-why\">from &ldquo;%s&rdquo;</span></article>"
          "<a class=\"button primary\" href=\"%s\">Go to it <span>&rarr;</span></a>"
          "<p class=\"subj-note\">Made through the organ that owns this kind, so it is the same object every "
          "other surface sees. Publishing, pricing and sharing are separate decisions from making.</p></div>",
          type, esc, where[0] ? where : "/");
        flow_reply(body);
        return 1;
    }

    /* The prompt itself. */
    if (!strcmp(target, "/make") || !strcmp(target, "/make/")) {
        w = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, "Make something");
        snprintf(body + w, sizeof(body) - w,
          "<article class=\"subj\">"
          "<p class=\"eyebrow\"><i></i>Compose &middot; creation, not posting</p>"
          "<h2 class=\"subj-name\">What are you making, doing, or starting?</h2>"
          "<p class=\"subj-desc\">Not &ldquo;what would you like to post&rdquo;. Say it plainly and it is read "
          "for what it proposes to bring into being &mdash; work someone can claim, a source observed under the "
          "foreign law, an offer the market can quote, a group others can join, or something written down.</p>"
          "<form id=\"make-source\" class=\"flow-search\" hx-get=\"/make/read\" hx-target=\"#make-read\" hx-swap=\"innerHTML\" "
          "hx-trigger=\"submit\">"
          "<input type=\"text\" name=\"text\" required autocomplete=\"off\" "
          "placeholder=\"I need somebody to help translate this README\">"
          "<button class=\"button primary\" type=\"submit\">Read it <span>&rarr;</span></button></form>"
          "<p class=\"flow-hint\">Try: a URL &middot; &ldquo;selling this cutting, $20&rdquo; &middot; "
          "&ldquo;we're starting a neighborhood clean-up&rdquo;</p>"
          "<div id=\"make-read\"></div>"
          "<p class=\"subj-note\">Publishing is one projection of creation, not the point of it. "
          "Nothing here crosses a human submit line on your behalf.</p>"
          "</article>"
          "<script defer src=\"/assets/matrix-htmx.js\"></script>"
          "%s", SUBJ_FOOT);
        flow_reply(body);
        return 1;
    }
    return 0;
}

/* ── SurfaceIR: interaction trace to cooled page ─────────────────────────
 *
 * Source E's mechanism, in order: an interaction trace becomes a repeated
 * closure, a repeated closure becomes a SurfaceCandidate, a candidate needs
 * proof, and only then does it cool into a stable deterministic page.
 *
 * What is recorded is the *shape* of an interaction -- which kind of subject,
 * under which closure, offering which actions -- never which subject a person
 * looked at. A viewer is counted through a salted digest so repetition can be
 * distinguished from one person reloading, without keeping who they were.
 *
 * A candidate is never described as proven by this code. It states its count
 * against the threshold and lets that be read.
 */
#define SURFACEIR_CANDIDATE_AT 5    /* repeats before a shape is a candidate */
#define SURFACEIR_PROOF_VIEWERS 3   /* distinct viewers before proof is claimed */

static const char *surfaceir_db(void) {
    const char *e = getenv("BONFYRE_SURFACEIR_DB");
    return (e && *e) ? e : "/var/lib/bonfyre/state/surfaceir.db";
}

static sqlite3 *surfaceir_open(int writable) {
    sqlite3 *db = NULL;
    int flags = writable ? (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE) : SQLITE_OPEN_READONLY;
    if (sqlite3_open_v2(surfaceir_db(), &db, flags, NULL) != SQLITE_OK) { if (db) sqlite3_close(db); return NULL; }
    if (writable) {
        sqlite3_busy_timeout(db, 2000);
        sqlite3_exec(db,
          "CREATE TABLE IF NOT EXISTS trace("
          " shape TEXT NOT NULL,"      /* subject kind + closure + offered actions */
          " kind TEXT NOT NULL,"
          " closure TEXT NOT NULL,"
          " actions TEXT NOT NULL,"
          " viewer TEXT NOT NULL,"     /* salted digest, never an identity */
          " ts INTEGER NOT NULL);"
          "CREATE INDEX IF NOT EXISTS trace_shape ON trace(shape);", NULL, NULL, NULL);
    }
    return db;
}

/* Record one interaction shape. Best effort: a surface must never fail because
 * its own telemetry could not be written. */
static void surfaceir_trace(const char *kind, const char *closure,
                            Action *acts, int n, const char *viewer_seed)
{
    sqlite3 *db; sqlite3_stmt *st = NULL;
    char actions[512] = {0}, shape[768], vhex[65], bare[64];
    size_t w = 0;
    /* The shape is keyed on the subject kind, not its subtitle, so "Parcel" and
     * "Parcel . observed public record" are the same shape. The subtitle also
     * carries markup entities, which would be escaped twice on the way out. */
    {
        size_t k = 0;
        for (const char *p2 = kind; *p2 && k + 1 < sizeof(bare); p2++) {
            if (*p2 == '&' || (*p2 == ' ' && k && bare[k-1] == ' ')) break;
            bare[k++] = *p2;
        }
        while (k && (bare[k-1] == ' ')) k--;
        bare[k] = '\0';
        if (!bare[0]) snprintf(bare, sizeof(bare), "%s", "subject");
        kind = bare;
    }
    for (int i = 0; i < n && w < sizeof(actions) - 40; i++)
        w += (size_t)snprintf(actions + w, sizeof(actions) - w, "%s%s", w ? "," : "", acts[i].label);
    snprintf(shape, sizeof(shape), "%s|%s|%s", kind, closure, actions);
    {
        /* Salted so the stored value cannot be walked back to a session token. */
        char salted[512];
        snprintf(salted, sizeof(salted), "surfaceir|%s", viewer_seed ? viewer_seed : "anonymous");
        bf_sha256_hex((const uint8_t *)salted, strlen(salted), vhex);
        vhex[16] = '\0';
    }
    db = surfaceir_open(1);
    if (!db) return;
    if (sqlite3_prepare_v2(db,
          "INSERT INTO trace(shape,kind,closure,actions,viewer,ts) VALUES(?,?,?,?,?,?)",
          -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, shape, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, kind, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, closure, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 4, actions, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 5, vhex, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 6, (sqlite3_int64)time(NULL));
        sqlite3_step(st);
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
}

static int serve_surfaceir(const char *method, const char *target, const char *query,
                           const char *bearer, const char *auth_db)
{
    static char body[262144];
    sqlite3 *db; sqlite3_stmt *st = NULL;
    size_t w = 0;
    long long traces = 0, shapes = 0, candidates = 0, proven = 0;
    (void)query; (void)bearer; (void)auth_db;

    if (strcmp(target, "/surfaces") && strcmp(target, "/surfaces/")) return 0;
    if (strcmp(method, "GET")) return 0;

    w = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, "Surface frontier");
    w += (size_t)snprintf(body + w, sizeof(body) - w,
      "<article class=\"subj\">"
      "<p class=\"eyebrow\"><i></i>SurfaceIR &middot; discovery, then cooling</p>"
      "<h2 class=\"subj-name\">Interfaces earn their way <em>into being fixed.</em></h2>"
      "<p class=\"subj-desc\">Nothing here is designed up front and nothing is invented per request. "
      "An interaction leaves a trace; a shape that repeats becomes a candidate; a candidate that survives "
      "distinct viewers can be cooled into a stable page. What is recorded is the shape of an interaction "
      "&mdash; which kind of subject, under which closure, offering which actions &mdash; never which "
      "subject anyone looked at.</p>");

    db = surfaceir_open(0);
    if (!db) {
        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<p class=\"flow-empty\">No traces recorded yet. The frontier is empty until surfaces are used.</p>");
    } else {
        sqlite3_stmt *c = NULL;
        if (sqlite3_prepare_v2(db, "SELECT count(*), count(DISTINCT shape) FROM trace", -1, &c, NULL) == SQLITE_OK
            && sqlite3_step(c) == SQLITE_ROW) {
            traces = sqlite3_column_int64(c, 0);
            shapes = sqlite3_column_int64(c, 1);
        }
        if (c) sqlite3_finalize(c);

        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<dl class=\"flow-facts\">"
          "<div><dt>Traces</dt><dd>%lld</dd></div>"
          "<div><dt>Distinct shapes</dt><dd>%lld</dd></div>"
          "<div><dt>Candidate at</dt><dd>%d repeats</dd></div>"
          "<div><dt>Proof needs</dt><dd>%d viewers</dd></div></dl>",
          traces, shapes, SURFACEIR_CANDIDATE_AT, SURFACEIR_PROOF_VIEWERS);

        w += (size_t)snprintf(body + w, sizeof(body) - w, "<div class=\"lens-stream\">");
        if (sqlite3_prepare_v2(db,
              "SELECT kind, closure, actions, count(*) AS n, count(DISTINCT viewer) AS v "
              "FROM trace GROUP BY shape ORDER BY n DESC LIMIT 25", -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW && w < sizeof(body) - 2048) {
                const char *kind = (const char *)sqlite3_column_text(st, 0);
                const char *closure = (const char *)sqlite3_column_text(st, 1);
                const char *actions = (const char *)sqlite3_column_text(st, 2);
                long long n = sqlite3_column_int64(st, 3), v = sqlite3_column_int64(st, 4);
                const char *stage, *why;
                char ek[128], ec[128], ea[1024];
                if (n >= SURFACEIR_CANDIDATE_AT && v >= SURFACEIR_PROOF_VIEWERS) {
                    stage = "proven"; why = "repeated across distinct viewers; it can be cooled";
                    proven++; candidates++;
                } else if (n >= SURFACEIR_CANDIDATE_AT) {
                    stage = "candidate"; why = "repeats, but not yet across enough viewers";
                    candidates++;
                } else {
                    stage = "trace"; why = "seen, not yet repeated enough to be a candidate";
                }
                html_escape(kind ? kind : "", ek, sizeof(ek));
                html_escape(closure ? closure : "", ec, sizeof(ec));
                html_escape(actions ? actions : "", ea, sizeof(ea));
                w += (size_t)snprintf(body + w, sizeof(body) - w,
                  "<span class=\"lens-item\">"
                  "<span class=\"li-weight\">%lld&times;</span>"
                  "<span class=\"li-name\">%s &middot; %s<br><span class=\"sir-acts\">%s</span></span>"
                  "<span class=\"li-meta\"><span class=\"pill %s\">%s</span><br>%lld viewer%s</span></span>",
                  n, ek, ec, ea,
                  !strcmp(stage,"proven") ? "published" : "draft", stage,
                  v, v == 1 ? "" : "s");
                (void)why;
            }
        }
        if (st) sqlite3_finalize(st);
        sqlite3_close(db);
        if (!traces)
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<p class=\"flow-empty\">No traces yet.</p>");
        w += (size_t)snprintf(body + w, sizeof(body) - w, "</div>");
    }

    snprintf(body + w, sizeof(body) - w,
      "<p class=\"subj-note\">A shape reaching the threshold is a candidate, not a decision: cooling one into a "
      "fixed page is a deliberate act, and nothing here performs it. %lld of %lld shapes have repeated enough "
      "to be candidates, and %lld of those across enough distinct viewers to be worth cooling.</p>"
      "</article>%s", candidates, shapes, proven, SUBJ_FOOT);
    flow_reply(body);
    return 1;
}

/* ── FLOW: stable movement, changing object grammar ──────────────────────
 *
 * TikTok's contribution kept, its ontology discarded: movement costs one
 * gesture and never changes, while what arrives is a parcel, then a capability,
 * then something published, then a source someone admitted. Each card is drawn
 * in the grammar of its own kind -- a parcel is not a note wearing different
 * words -- and every card is a real object you can open.
 *
 * The position is in the address, so a place in the flow is shareable and the
 * sequence is reproducible rather than a private shuffle.
 */
static int flow_card_parcel(const char *bindir, int nth, char *out, size_t cap) {
    static char rows[65536];
    char sql[1024], addr[256]={0}, acct[64]={0}, owner[256]={0}, tv[64]={0}, city[64]={0};
    char e_addr[512], e_owner[512];
    /* Deterministic pick: ordering by a hash of the account gives a stable,
     * shareable sequence without storing one. */
    snprintf(sql, sizeof(sql),
      "SELECT json_extract_string(observed,'$.Situs_Addr') AS address, "
      "trim(json_extract_string(observed,'$.Account_Nu')) AS account, "
      "json_extract_string(observed,'$.Owner_Name') AS owner, "
      "trim(json_extract_string(observed,'$.City')) AS city, "
      "TRY_CAST(json_extract_string(observed,'$.Total_Valu') AS DOUBLE) AS total_value "
      "FROM observation WHERE src_id='src:tad-parcel-view' "
      "AND TRY_CAST(json_extract_string(observed,'$.Total_Valu') AS DOUBLE) > 50000 "
      "ORDER BY hash(json_extract_string(observed,'$.Account_Nu')) LIMIT 1 OFFSET %d", nth);
    if (fleming_query(bindir, sql, rows, sizeof(rows)) != 0 || rows[0] != '[' || strlen(rows) < 4) return 0;
    studio_json_field(rows,"address",addr,sizeof(addr));
    studio_json_field(rows,"account",acct,sizeof(acct));
    studio_json_field(rows,"owner",owner,sizeof(owner));
    studio_json_field(rows,"city",city,sizeof(city));
    studio_json_field(rows,"total_value",tv,sizeof(tv)); num_fix(tv, sizeof(tv));
    if (!acct[0]) return 0;
    html_escape(addr,e_addr,sizeof(e_addr));
    html_escape(owner,e_owner,sizeof(e_owner));
    snprintf(out, cap,
      "<article class=\"fl-card is-parcel\">"
      "<p class=\"fl-kind\">A place</p>"
      "<h2 class=\"fl-title\">%s</h2>"
      "<p class=\"fl-sub\">carried under %s &middot; district %s</p>"
      "<p class=\"fl-big\">$%s</p><p class=\"fl-cap\">total assessed</p>"
      "<a class=\"button primary\" href=\"/s/parcel/%s\">Open it <span>&rarr;</span></a>"
      "<a class=\"button quiet\" href=\"/room/parcel/%s\">Its room <span>&rarr;</span></a>"
      "</article>",
      e_addr[0]?e_addr:"(no situs address)", e_owner[0]?e_owner:"an unnamed taxpayer",
      city[0]?city:"&mdash;", tv[0]?tv:"&mdash;", acct, acct);
    return 1;
}

static int flow_card_capability(int nth, char *out, size_t cap) {
    const X402Capability *c = &X402_CATALOG[nth % X402_CATALOG_N];
    double usd = 0.0;
    int priced = x402_price_usd(c->name, &usd);
    snprintf(out, cap,
      "<article class=\"fl-card is-cap\">"
      "<p class=\"fl-kind\">Something you can buy</p>"
      "<h2 class=\"fl-title\">%s</h2>"
      "<p class=\"fl-sub\">%s</p>"
      "<p class=\"fl-big\">%s</p><p class=\"fl-cap\">%s</p>"
      "<a class=\"button primary\" href=\"/s/capability/%s\">Open it <span>&rarr;</span></a>"
      "<a class=\"button quiet\" href=\"/market\">The market <span>&rarr;</span></a>"
      "</article>",
      c->name, c->summary,
      priced ? "listed" : "not for sale",
      priced ? "quoted the same in every grammar" : "discoverable, but carries no price",
      c->name);
    return 1;
}

static int flow_card_note(int nth, char *out, size_t cap) {
    sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
    char title[512]={0}, route[256]={0}, et[1024], er[512];
    int found = 0;
    if (!db) return 0;
    if (sqlite3_prepare_v2(db,
          "SELECT title, route FROM wiki_page WHERE status='published' AND allow_guest=1 "
          "ORDER BY id LIMIT 1 OFFSET ?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, nth);
        if (sqlite3_step(st) == SQLITE_ROW) {
            snprintf(title,sizeof(title),"%s",(const char*)sqlite3_column_text(st,0));
            snprintf(route,sizeof(route),"%s",(const char*)sqlite3_column_text(st,1));
            found = 1;
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    if (!found) return 0;
    html_escape(title,et,sizeof(et));
    html_escape(route+ (strncmp(route,"notes/",6)?0:6), er, sizeof(er));
    snprintf(out, cap,
      "<article class=\"fl-card is-note\">"
      "<p class=\"fl-kind\">Something written down</p>"
      "<h2 class=\"fl-title fl-serif\">%s</h2>"
      "<p class=\"fl-sub\">published, and open to anyone</p>"
      "<a class=\"button primary\" href=\"/%s\">Read it <span>&rarr;</span></a>"
      "<a class=\"button quiet\" href=\"/s/note/%s\">Open the subject <span>&rarr;</span></a>"
      "</article>", et, route, er);
    return 1;
}

static int flow_card_source(int nth, char *out, size_t cap) {
    const char *dir = getenv("BONFYRE_FOREIGN_RECEIPTS");
    BfReceiptDir *d; struct dirent *e;
    char names[64][256];
    int n = 0;
    if (!dir || !*dir) dir = "/var/lib/bonfyre/foreign/receipts";
    d = bf_receipt_opendir(dir);
    if (!d) return 0;
    while ((e = bf_receipt_readdir(d)) && n < 64) {
        if (e->d_name[0] == '.') continue;
        snprintf(names[n++], sizeof(names[0]), "%s", e->d_name);
    }
    bf_receipt_closedir(d);
    if (!n) return 0;
    {
        char path[PATH_MAX], buf[8192], url[1024]={0}, dec[64]={0}, bytes[32]={0}, dg[80]={0};
        FILE *f; size_t got;
        char eu[2048];
        snprintf(path, sizeof(path), "%s/%s", dir, names[nth % n]);
        f = bf_receipt_fopen(path, "rb");
        if (!f) return 0;
        got = fread(buf, 1, sizeof(buf)-1, f); buf[got] = '\0'; fclose(f);
        studio_json_field(buf,"source_url",url,sizeof(url));
        studio_json_field(buf,"policy_decision",dec,sizeof(dec));
        studio_json_field(buf,"body_bytes",bytes,sizeof(bytes));
        studio_json_field(buf,"content_sha256",dg,sizeof(dg));
        if (!url[0]) return 0;
        html_escape(url,eu,sizeof(eu));
        /* A twin with a digest can offer its exact retained bytes; one refused
         * by robots has none, and says so instead. */
        if (dg[0])
            snprintf(out, cap,
              "<article class=\"fl-card is-source\">"
              "<p class=\"fl-kind\">Something from outside</p>"
              "<h2 class=\"fl-title fl-mono\">%.90s</h2>"
              "<p class=\"fl-sub\">robots said <strong>%s</strong> &middot; %s bytes retained</p>"
              "<p class=\"fl-cap\">a cached projection, receipted and digested; not a licence</p>"
              "<a class=\"button primary\" href=\"/assets/twin/%s\">The exact bytes <span>&rarr;</span></a>"
              "<a class=\"button quiet\" href=\"/sources\">Where these come from <span>&rarr;</span></a>"
              "</article>", eu, dec[0]?dec:"nothing recorded", bytes[0]?bytes:"0", dg);
        else
            snprintf(out, cap,
              "<article class=\"fl-card is-source\">"
              "<p class=\"fl-kind\">Something from outside</p>"
              "<h2 class=\"fl-title fl-mono\">%.90s</h2>"
              "<p class=\"fl-sub\">robots said <strong>%s</strong></p>"
              "<p class=\"fl-cap\">receipted without the source ever being fetched</p>"
              "<a class=\"button quiet\" href=\"/sources\">Where these come from <span>&rarr;</span></a>"
              "</article>", eu, dec[0]?dec:"nothing recorded");
    }
    return 1;
}

typedef int (*FlowCardRender)(const char *bindir, int nth, char *out, size_t cap);

static int flow_card_capability_bound(const char *bindir, int nth, char *out, size_t cap) {
    (void)bindir;
    return flow_card_capability(nth, out, cap);
}

static int flow_card_note_bound(const char *bindir, int nth, char *out, size_t cap) {
    (void)bindir;
    return flow_card_note(nth, out, cap);
}

static int flow_card_source_bound(const char *bindir, int nth, char *out, size_t cap) {
    (void)bindir;
    return flow_card_source(nth, out, cap);
}

static int flow_card_recipe_bound(const char *bindir, int nth, char *out, size_t cap) {
    (void)bindir;
    int result = bf_recipe_material_card(bf_recipe_material_path(), nth, out, cap);
    if (result < 0 && cap) out[0] = '\0';
    return result > 0;
}

/* Position selects the grammar directly; no request-time string dispatch. */
static const FlowCardRender FLOW_CARD_RENDER[] = {
    flow_card_parcel, flow_card_capability_bound, flow_card_note_bound, flow_card_source_bound, flow_card_recipe_bound
};
#define FLOW_KIND_N ((int)(sizeof(FLOW_CARD_RENDER) / sizeof(FLOW_CARD_RENDER[0])))

/* ---------------------------------------------------------------------------
 * The desk.
 *
 * /studio/ was a wall of cards: ten panels, each firing one call, none leading
 * anywhere. A panel of controls is not a surface a person operates -- it asks
 * them to already know which button matters today.
 *
 * A desk states what the institution is holding open right now, computed from
 * real state, and gives each open thing the one act that closes it. Nothing is
 * demonstrated here. If a queue is empty it says what would fill it, because an
 * empty queue is a fact about the institution and not a gap in the page.
 * ------------------------------------------------------------------------- */

typedef struct { char label[192]; char sub[192]; char href[192]; } DeskRow;

/* Capabilities that are advertised but carry no price. Advertised-and-unpriced
 * is not free: it answers not_for_sale. Someone has to decide. */
#endif /* BONFYRE_ROOM_COMPOSE_AND_FLOW_CARDS_H */
