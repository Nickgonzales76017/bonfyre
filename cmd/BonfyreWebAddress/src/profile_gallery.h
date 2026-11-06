#ifndef BONFYRE_PROFILE_GALLERY_H
#define BONFYRE_PROFILE_GALLERY_H
/* Account-chosen artwork on one member profile site. The source owner keeps
 * the artwork; a site_fragment only names its exact ref, the reviewed
 * fingerprint and its publication state. A header (kind profile-cover) and a
 * gallery entry (kind profile-gallery) re-resolve the source and its rights on
 * every read, so a changed or withdrawn source drops out of the public page.
 * Adding to a gallery saves a private draft; only an explicit owner review
 * makes an entry public. Nothing here copies the image or its record. */
#include "bf_jsonw.h"

#define PH_COVER_LABEL "bonfyre.profile-cover-review.v1"
#define PH_GALLERY_LABEL "bonfyre.profile-gallery-review.v1"
#define PH_GALLERY_MAX 48

typedef struct { char source[96], key[25], ref[128]; } PhArtRef;
typedef struct { char title[768], by[256], href[1024], image[1024], digest[65]; } PhArt;
typedef struct { int id, position; char ref[128], status[16], audience[16], evidence[65], snapshot[65]; } PhArtRow;

static int ph_art_ref(const char *text, PhArtRef *out) {
    const char *slash = text ? strchr(text, '/') : NULL;
    memset(out, 0, sizeof(*out));
    if (!slash || slash == text || (size_t)(slash - text) >= sizeof(out->source) ||
        strlen(slash + 1) != 24 || strspn(slash + 1, "0123456789abcdef") != 24) return 0;
    memcpy(out->source, text, (size_t)(slash - text));
    if (strspn(out->source, "abcdefghijklmnopqrstuvwxyz0123456789-_.") != strlen(out->source)) return 0;
    snprintf(out->key, sizeof(out->key), "%s", slash + 1);
    snprintf(out->ref, sizeof(out->ref), "%s/%s", out->source, out->key);
    return 1;
}

/* Only a photographed item whose own source states CC0 qualifies; the
 * fingerprint fails for anything else, so callers cannot widen it. */
static int ph_art_resolve(sqlite3 *db, const PhArtRef *ref, const char *label, int fx, int fy, PhArt *art) {
    PageMaterial *material = calloc(1, sizeof(*material));
    const PageItem *item = NULL; int ok = 0;
    memset(art, 0, sizeof(*art));
    if (material && db && page_item_by_ref(db, ref->source, ref->key, material, &item) && item &&
        page_is_photo(item) && page_image_url_safe(item->image)) {
        BfProfileCoverSource s = {ref->source, ref->key, item->title, item->by, item->href, item->image, item->rights, fx, fy};
        if (bf_profile_art_review_digest(label, &s, art->digest)) {
            snprintf(art->title, sizeof(art->title), "%s", item->title);
            snprintf(art->by, sizeof(art->by), "%s", item->by);
            snprintf(art->href, sizeof(art->href), "%s", item->href);
            snprintf(art->image, sizeof(art->image), "%s", item->image);
            ok = 1;
        }
    }
    free(material);
    return ok;
}

static int ph_art_text(sqlite3_stmt *st, int col, char *out, size_t cap) {
    const char *s = (const char *)sqlite3_column_text(st, col);
    if (!s || strlen(s) >= cap) return 0;
    snprintf(out, cap, "%s", s); return 1;
}

/* One exact row in this site's namespace, by coordinate or by id.
 * Returns 1 for exactly one row, 0 for none, -1 when it cannot be trusted. */
static int ph_art_row(sqlite3 *db, const char *ns, const char *kind, const char *coordinate, int id, PhArtRow *row) {
    sqlite3_stmt *st = NULL; int found = 0;
    memset(row, 0, sizeof(*row));
    if (!db || sqlite3_prepare_v2(db,
        "SELECT id,COALESCE(source_ref,''),status,audience,position,COALESCE(evidence_ref,'') FROM site_fragment "
        "WHERE namespace=?1 AND kind=?2 AND ((?3<>'' AND coordinate=?3) OR (?4>0 AND id=?4)) LIMIT 2", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, kind, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, coordinate ? coordinate : "", -1, SQLITE_STATIC); sqlite3_bind_int(st, 4, id);
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (found++) { found = -1; break; }
        sqlite3_int64 rowid = sqlite3_column_int64(st, 0);
        if (rowid < 1 || rowid > INT_MAX || sqlite3_column_type(st, 4) != SQLITE_INTEGER ||
            !ph_art_text(st, 1, row->ref, sizeof(row->ref)) || !ph_art_text(st, 2, row->status, sizeof(row->status)) ||
            !ph_art_text(st, 3, row->audience, sizeof(row->audience)) || !ph_art_text(st, 5, row->evidence, sizeof(row->evidence))) { found = -1; break; }
        sqlite3_int64 p = sqlite3_column_int64(st, 4);
        if (p < 0 || p > 1000000) { found = -1; break; }
        row->id = (int)rowid; row->position = (int)p;
    }
    sqlite3_finalize(st);
    if (found == 1 && !cms_snapshot_row(db, "site_fragment", row->id, ns, row->snapshot)) found = -1;
    if (found != 1) { int keep = found; memset(row, 0, sizeof(*row)); return keep; }
    return 1;
}

static void ph_cover_coordinate(const PhSite *site, char out[160]) { snprintf(out, 160, "profile-cover-%s", site->id); }
static void ph_gallery_coordinate(const PhArtRef *ref, char out[160]) { snprintf(out, 160, "profile-gallery-%s-%s", ref->source, ref->key); }

static char *ph_art_payload(const char *coordinate, const char *slot, const char *kind, const char *audience,
                            long long position, const char *ref, const char *evidence, const char *status) {
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    if (coordinate) { bf_jsonw_key(&w, "coordinate"); bf_jsonw_str(&w, coordinate); }
    if (slot) { bf_jsonw_key(&w, "page"); bf_jsonw_str(&w, "/"); bf_jsonw_key(&w, "slot"); bf_jsonw_str(&w, slot); }
    if (kind) { bf_jsonw_key(&w, "kind"); bf_jsonw_str(&w, kind); }
    bf_jsonw_key(&w, "audience"); bf_jsonw_str(&w, audience);
    if (position >= 0) { bf_jsonw_key(&w, "position"); bf_jsonw_int(&w, position); }
    if (ref) { bf_jsonw_key(&w, "source_ref"); bf_jsonw_str(&w, ref); }
    if (evidence) { bf_jsonw_key(&w, "evidence_ref"); bf_jsonw_str(&w, evidence); }
    bf_jsonw_key(&w, "status"); bf_jsonw_str(&w, status);
    bf_jsonw_obj_end(&w);
    char *out = NULL;
    if (!w.oom && w.buf && (out = malloc(w.len + 1))) { memcpy(out, w.buf, w.len); out[w.len] = 0; }
    bf_jsonw_free(&w);
    return out;
}

/* Writes go through the CMS owner. An update carries the snapshot the owner
 * reviewed; the CMS refuses it if anything changed the row since. */
static int ph_art_cms(const char *action, const char *ns, int id, const char *payload, const char *digest) {
    const char *bin = page_cms_env("BONFYRE_CMS_BINARY", "/usr/local/bin/bonfyre-cms"), *db = getenv("BONFYRE_CMS_DB"), *schemas = getenv("BONFYRE_CMS_SCHEMAS");
    char record[24], output[8192]; char *args[20]; int n = 0;
    if (!db || !schemas || !payload) return 0;
    args[n++] = (char *)bin; args[n++] = "entry"; args[n++] = (char *)action; args[n++] = "site_fragment";
    if (id > 0) { snprintf(record, sizeof(record), "%d", id); args[n++] = record; }
    args[n++] = (char *)payload; args[n++] = "--ns"; args[n++] = (char *)ns;
    args[n++] = "--db"; args[n++] = (char *)db; args[n++] = "--schemas"; args[n++] = (char *)schemas;
    if (digest) { args[n++] = "--if-digest"; args[n++] = (char *)digest; }
    args[n] = NULL;
    return auth_call(args, NULL, output, sizeof(output)) == 0;
}

static void ph_art_link(const PhArtRef *ref) { fputs("/o/", stdout); page_url_escape(ref->source); fputc('/', stdout); html(ref->key); }
static void ph_art_tab_url(const PhSite *site, const char *art) { ph_url(site, "artwork", 1); fputs("&amp;art=", stdout); url_query_value(art); }

static void ph_gallery_style(void) {
    fputs("<style>"
      ".ph-gallery{margin:6px 0 40px}.ph-gallery-head{display:flex;justify-content:space-between;align-items:baseline;gap:16px;flex-wrap:wrap}"
      ".ph-gallery-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:30px 22px;align-items:start;grid-auto-flow:dense;margin-top:14px}"
      ".ph-art{margin:0;grid-column:span 2;min-width:0}.ph-art[data-span=wide]{grid-column:span 4}"
      ".ph-art-frame{display:block;overflow:hidden;background:#193d30;height:clamp(180px,17vw,270px)}"
      ".ph-art[data-span=wide] .ph-art-frame{height:clamp(260px,31vw,480px)}.ph-art[data-span=tall] .ph-art-frame{height:clamp(300px,34vw,540px)}"
      ".ph-art-frame img{width:100%;height:100%;object-fit:cover;display:block}"
      ".ph-art figcaption{padding-top:10px;display:grid;gap:2px}.ph-art figcaption>a{font:400 19px/1.25 Georgia,serif;color:var(--ph-ink);overflow-wrap:anywhere}"
      ".ph-art[data-span=wide] figcaption>a{font-size:25px}.ph-art figcaption span,.ph-art figcaption small{font-size:12px;color:var(--ph-muted);overflow-wrap:anywhere}"
      ".ph-art-state{font:600 11px ui-monospace,monospace;letter-spacing:.06em;text-transform:uppercase;color:#8a5a12}"
      ".ph-art-owner{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}.ph-art-owner form{margin:0}.ph-art-owner button{min-height:40px;padding:8px 12px;font-size:12px}"
      ".ph-art-owner button.ph-quiet,.ph-shell .ph-art-owner button.ph-quiet{background:transparent;color:#2b4c36;border-color:#b5c8a1}"
      ".ph-art[data-gallery-stale] .ph-art-frame{opacity:.45}"
      "@media(max-width:700px){.ph-gallery-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:22px 12px}.ph-art{grid-column:span 1}.ph-art[data-span=wide]{grid-column:span 2}"
      ".ph-art-frame{height:170px}.ph-art[data-span=wide] .ph-art-frame{height:230px}.ph-art[data-span=tall] .ph-art-frame{height:260px}.ph-art figcaption>a{font-size:16px}.ph-art[data-span=wide] figcaption>a{font-size:20px}}"
      "</style>", stdout);
}

static void ph_gallery_form(const PhSite *site, const char *action, const PhArtRow *row, const char *label, int quiet) {
    char id[24]; snprintf(id, sizeof(id), "%d", row->id);
    fputs("<form method=\"post\" action=\"", stdout); fputs(action, stdout); fputs("\">", stdout);
    ph_hidden("site", site->id); ph_hidden("id", id); ph_hidden("expected", row->snapshot);
    fputs(quiet ? "<button class=\"ph-quiet\" type=\"submit\">" : "<button type=\"submit\">", stdout); html(label); fputs("</button></form>", stdout);
}

/* The gallery as an editorial mosaic: a wide piece, then smaller and taller
 * ones, in the owner's order. Public readers get published entries whose
 * source still matches its review; the owner also sees private drafts. */
static void ph_gallery_emit(sqlite3 *db, const PhSite *site, int owner, int limit, int full) {
    sqlite3_stmt *st = NULL; int shown = 0, more = 0, seen = 0;
    if (!db || !site) return;
    if (sqlite3_prepare_v2(db, owner
        ? "SELECT id FROM site_fragment WHERE namespace=?1 AND kind='profile-gallery' AND slot='gallery' AND page='/' AND status IN ('draft','published') ORDER BY position,id LIMIT ?2"
        : "SELECT id FROM site_fragment WHERE namespace=?1 AND kind='profile-gallery' AND slot='gallery' AND page='/' AND status='published' AND audience='public' ORDER BY position,id LIMIT ?2",
        -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC); sqlite3_bind_int(st, 2, PH_GALLERY_MAX);
    int ids[PH_GALLERY_MAX], n = 0;
    while (n < PH_GALLERY_MAX && sqlite3_step(st) == SQLITE_ROW) { sqlite3_int64 v = sqlite3_column_int64(st, 0); if (v > 0 && v <= INT_MAX) ids[n++] = (int)v; }
    sqlite3_finalize(st);
    for (int i = 0; i < n; i++) {
        PhArtRow row; PhArtRef ref; PhArt art; int valid, published;
        if (ph_art_row(db, site->id, "profile-gallery", NULL, ids[i], &row) != 1 || !ph_art_ref(row.ref, &ref)) continue;
        published = !strcmp(row.status, "published") && !strcmp(row.audience, "public");
        valid = ph_art_resolve(db, &ref, PH_GALLERY_LABEL, 50, 50, &art) && (!published || !strcmp(art.digest, row.evidence));
        if (!owner && (!valid || !published)) continue;
        if (shown == limit) { more = 1; break; }
        if (!shown) {
            ph_gallery_style();
            fputs("<section class=\"ph-gallery\" data-profile-gallery><div class=\"ph-gallery-head\"><h2>Gallery</h2>", stdout);
            if (!full) { fputs("<a href=\"", stdout); ph_url(site, "gallery", owner); fputs("\">See the whole gallery</a>", stdout); }
            fputs("</div>", stdout);
            if (owner) fputs("<p class=\"ph-privacy\">Pieces marked private are visible only to you. Visitors see a piece only after you choose to show it.</p>", stdout);
            fputs("<div class=\"ph-gallery-grid\">", stdout);
        }
        const char *span = seen % 7 == 0 ? "wide" : (seen % 7 == 1 || seen % 7 == 4) ? "tall" : "normal";
        seen++; shown++;
        fputs("<figure class=\"ph-art\" data-gallery-item data-span=\"", stdout); fputs(span, stdout);
        fputs("\" data-gallery-state=\"", stdout); fputs(published ? "public" : "private", stdout); fputs("\"", stdout);
        if (!valid) fputs(" data-gallery-stale", stdout);
        fputs(">", stdout);
        if (valid) {
            fputs("<a class=\"ph-art-frame\" href=\"", stdout); ph_art_link(&ref); fputs("\"><img loading=\"lazy\" decoding=\"async\" referrerpolicy=\"no-referrer\" src=\"", stdout);
            html(art.image); fputs("\" alt=\"", stdout); html(art.title); fputs("\"></a><figcaption><a href=\"", stdout); ph_art_link(&ref); fputs("\">", stdout);
            html(art.title); fputs("</a>", stdout);
            if (art.by[0]) { fputs("<span>", stdout); html(art.by); fputs("</span>", stdout); }
            fputs("<small>Public domain (CC0)</small>", stdout);
        } else {
            fputs("<div class=\"ph-art-frame\" aria-hidden=\"true\"></div><figcaption><span class=\"ph-art-state\">Hidden from visitors</span><span>The source for this piece changed or is no longer available, so it is not shown publicly. Remove it, or add it again from its page.</span>", stdout);
        }
        if (owner) {
            if (valid) { fputs("<span class=\"ph-art-state\">", stdout); fputs(published ? "In your public gallery" : "Private, only you can see this", stdout); fputs("</span>", stdout); }
            fputs("<div class=\"ph-art-owner\">", stdout);
            if (valid && !published) ph_gallery_form(site, "/profile/gallery/publish", &row, "Show in public gallery", 0);
            if (published) ph_gallery_form(site, "/profile/gallery/hide", &row, "Make private", 1);
            ph_gallery_form(site, "/profile/gallery/remove", &row, "Remove", 1);
            if (valid) { fputs("<a class=\"ph-button\" href=\"", stdout); ph_art_tab_url(site, row.ref); fputs("\">Use as header</a>", stdout); }
            fputs("</div>", stdout);
        }
        fputs("</figcaption></figure>", stdout);
    }
    if (shown) {
        fputs("</div>", stdout);
        if (more && full) fputs("<p>Only the first 48 pieces are shown.</p>", stdout);
        fputs("</section>", stdout);
    } else if (full) {
        fputs("<div class=\"ph-empty\" data-profile-gallery-empty><h2>", stdout);
        fputs(owner ? "Your gallery is empty" : "Nothing in this gallery yet", stdout);
        fputs("</h2><p>", stdout);
        fputs(owner ? "Open a public-domain artwork anywhere on Aurekai and choose Use on my profile. It stays private here until you show it." : "This gallery will show the artwork its owner chooses to share.", stdout);
        fputs("</p></div>", stdout);
    }
}

static int ph_focus_value(const char *s, int fallback) {
    if (!s || !*s) return fallback;
    if (strlen(s) > 3 || strspn(s, "0123456789") != strlen(s)) return -1;
    int v = atoi(s); return v <= 100 ? v : -1;
}

/* The owner's review of one artwork for one of their own sites: header with
 * a focus point, and gallery as a private draft. Nothing is written by
 * opening this page. */
static void ph_artwork_view(sqlite3 *db, const PhSite *sites, int count, const PhSite *site, const char *art_text, const char *fxs, const char *fys) {
    PhArtRef ref; PhArt art; PhArtRow cover, entry; char coordinate[160];
    int fx = ph_focus_value(fxs, 50), fy = ph_focus_value(fys, 50);
    int have_cover = -1;
    fputs("<h2>Use this artwork on your profile</h2>", stdout);
    if (site && ph_art_ref(art_text, &ref)) {
        /* Reopening the current header starts from its saved focus point. */
        ph_cover_coordinate(site, coordinate);
        have_cover = ph_art_row(db, site->id, "profile-cover", coordinate, 0, &cover);
        if (have_cover == 1 && !*fxs && !*fys && !strcmp(cover.ref, ref.ref) && cover.position % 101 <= 100) { fx = cover.position / 101; fy = cover.position % 101; }
    }
    if (!ph_art_ref(art_text, &ref) || fx < 0 || fy < 0 || !ph_art_resolve(db, &ref, PH_COVER_LABEL, fx, fy, &art)) {
        fputs("<div class=\"ph-empty\" data-artwork-refused><p>This artwork can't be used on a profile. Only pictures whose source marks them public domain (CC0) can be, and only while that source still lists them.</p></div>", stdout);
        return;
    }
    if (!site) {
        fputs("<div class=\"ph-empty\"><p>You need a published profile site first. <a href=\"/account#presence\">Set up a site</a>.</p></div>", stdout);
        return;
    }
    fputs("<style>.ph-art-sites{display:flex;flex-wrap:wrap;gap:8px;margin:6px 0 22px}.ph-art-sites a{padding:9px 15px;border-radius:999px;border:1px solid var(--ph-line);font-size:13px}.ph-art-sites a[aria-current=page]{background:#2b5038;color:#fffef7;border-color:#2b5038}"
          ".ph-art-preview{margin:0 0 26px}.ph-art-crop{height:clamp(180px,25vw,310px);overflow:hidden;background:#193d30}.ph-art-crop img{width:100%;height:100%;object-fit:cover;display:block}"
          ".ph-art-preview figcaption{display:flex;flex-wrap:wrap;gap:6px 16px;padding-top:10px;font-size:13px;color:var(--ph-muted)}.ph-art-preview figcaption strong{font:400 21px/1.2 Georgia,serif;color:var(--ph-ink);flex-basis:100%}"
          ".ph-art-choices{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(0,1fr);gap:22px}.ph-art-choices .ph-form{max-width:none}.ph-art-choices h3{font:400 23px/1.2 Georgia,serif;margin:0 0 8px}.ph-art-choices p{font-size:13px;color:var(--ph-muted)}"
          ".ph-shell .ph-art-choices input[type=range]{min-height:44px;padding:0;border:0;background:transparent;accent-color:#2b5038}"
          "@media(max-width:800px){.ph-art-choices{grid-template-columns:minmax(0,1fr)}}</style>", stdout);
    fputs("<nav class=\"ph-art-sites\" aria-label=\"Choose which site\">", stdout);
    for (int i = 0; i < count; i++) {
        fputs("<a href=\"", stdout); ph_art_tab_url(&sites[i], ref.ref); fputs("\"", stdout);
        if (!strcmp(sites[i].id, site->id)) fputs(" aria-current=\"page\"", stdout);
        fputs(">", stdout); html(ph_label(sites[i].territory)); fputs(" · ", stdout); html(sites[i].host); fputs("</a>", stdout);
    }
    fputs("</nav><figure class=\"ph-art-preview\" data-artwork-review><div class=\"ph-art-crop\"><img data-art-preview src=\"", stdout);
    html(art.image); fputs("\" alt=\"", stdout); html(art.title);
    printf("\" style=\"object-position:%d%% %d%%\" referrerpolicy=\"no-referrer\"></div><figcaption><strong>", fx, fy);
    html(art.title); fputs("</strong>", stdout);
    if (art.by[0]) { fputs("<span>", stdout); html(art.by); fputs("</span>", stdout); }
    fputs("<span>Public domain (CC0)</span><a href=\"", stdout); ph_art_link(&ref); fputs("\">Artwork record</a>", stdout);
    if (public_http_url(art.href)) { fputs("<a rel=\"noopener noreferrer\" href=\"", stdout); html(art.href); fputs("\">Where it comes from ↗</a>", stdout); }
    fputs("</figcaption></figure><div class=\"ph-art-choices\">", stdout);

    int current = have_cover == 1 && !strcmp(cover.status, "published") && !strcmp(cover.ref, ref.ref);
    fputs("<form class=\"ph-form\" method=\"post\" action=\"/profile/cover/set\" data-cover-form><h3>Profile header</h3><p>", stdout);
    if (current) { fputs("This is your header on ", stdout); html(site->host); fputs(". Move the focus point and save it.", stdout); }
    else if (have_cover == 1 && !strcmp(cover.status, "published")) { fputs("This replaces the header you have now on ", stdout); html(site->host); fputs(".", stdout); }
    else { fputs("It goes at the top of ", stdout); html(site->host); fputs(". Your profile picture stays the same.", stdout); }
    fputs("</p>", stdout);
    ph_hidden("site", site->id); ph_hidden("art", ref.ref); ph_hidden("expected", have_cover == 1 ? cover.snapshot : "none");
    printf("<label>Focus left to right<input type=\"range\" name=\"fx\" min=\"0\" max=\"100\" step=\"1\" value=\"%d\" data-art-focus=\"x\"></label>"
           "<label>Focus top to bottom<input type=\"range\" name=\"fy\" min=\"0\" max=\"100\" step=\"1\" value=\"%d\" data-art-focus=\"y\"></label>", fx, fy);
    fputs("<button type=\"submit\">", stdout); fputs(current ? "Save the focus point" : "Use as my header", stdout); fputs("</button></form>", stdout);

    ph_gallery_coordinate(&ref, coordinate);
    int have_entry = ph_art_row(db, site->id, "profile-gallery", coordinate, 0, &entry);
    fputs("<div class=\"ph-form\" data-gallery-choice><h3>Gallery</h3>", stdout);
    if (have_entry == 1 && !strcmp(entry.status, "published")) {
        fputs("<p>This is in your public gallery on ", stdout); html(site->host); fputs(".</p>", stdout);
    } else if (have_entry == 1 && !strcmp(entry.status, "draft")) {
        fputs("<p>This is in your gallery on ", stdout); html(site->host); fputs(", private until you show it.</p>", stdout);
    } else {
        fputs("<p>Add it to your gallery on ", stdout); html(site->host); fputs(". It stays private until you choose to show it.</p><form method=\"post\" action=\"/profile/gallery/add\">", stdout);
        ph_hidden("site", site->id); ph_hidden("art", ref.ref);
        fputs("<button type=\"submit\">Add to my gallery</button></form>", stdout);
    }
    fputs("<p><a href=\"", stdout); ph_url(site, "gallery", 1); fputs("\">Review your gallery</a></p></div></div>", stdout);
    fputs("<script>(function(){var i=document.querySelector('[data-art-preview]'),x=document.querySelector('[data-art-focus=x]'),y=document.querySelector('[data-art-focus=y]');"
          "if(!i||!x||!y)return;function u(){i.style.objectPosition=x.value+'% '+y.value+'%';}x.addEventListener('input',u);y.addEventListener('input',u);})();</script>", stdout);
}

static int ph_art_target(const char *t) {
    return !strcmp(t, "/profile/cover/set") || !strcmp(t, "/profile/gallery/add") || !strcmp(t, "/profile/gallery/publish") ||
           !strcmp(t, "/profile/gallery/hide") || !strcmp(t, "/profile/gallery/remove");
}

static int ph_art_mutate(const char *target, const char *body, const char *raw, const char *host, const char *actor, sqlite3 *db) {
    char siteid[128] = "", art_text[128] = "", fxs[8] = "", fys[8] = "", id[24] = "", expected[72] = "";
    PhField fields[] = {{"site", siteid, sizeof(siteid), 0}, {"art", art_text, sizeof(art_text), 0}, {"fx", fxs, sizeof(fxs), 0},
                        {"fy", fys, sizeof(fys), 0}, {"id", id, sizeof(id), 0}, {"expected", expected, sizeof(expected), 0}};
    if (!ph_origin(raw, host) || !ph_fields(body, fields, 6, 4096)) { ph_error(403, "Review and submit this form from its own profile page.\n"); return 1; }
    PhSite site;
    if (!ph_site(db, NULL, siteid, &site) || strcmp(site.actor, actor)) { ph_error(404, "This profile site is not available to this account.\n"); return 1; }
    PhArtRef ref; PhArt art; PhArtRow row; char coordinate[160];
    if (!strcmp(target, "/profile/cover/set")) {
        int fx = ph_focus_value(fxs, -1), fy = ph_focus_value(fys, -1);
        if (fx < 0 || fy < 0 || !ph_art_ref(art_text, &ref) || (strcmp(expected, "none") && (strlen(expected) != 64 || strspn(expected, "0123456789abcdef") != 64))) {
            ph_error(400, "Choose an artwork and a focus point from the review page.\n"); return 1;
        }
        if (!ph_art_resolve(db, &ref, PH_COVER_LABEL, fx, fy, &art)) { ph_error(409, "This artwork is no longer available for a profile header. Reload its page.\n"); return 1; }
        ph_cover_coordinate(&site, coordinate);
        int have = ph_art_row(db, site.id, "profile-cover", coordinate, 0, &row);
        if (have < 0 || (have == 1 ? strcmp(row.snapshot, expected) : strcmp(expected, "none"))) { ph_error(409, "Your header changed since you opened this page. Reload it before saving.\n"); return 1; }
        long long position = (long long)fx * 101 + fy;
        char *payload = ph_art_payload(have ? NULL : coordinate, "cover", "profile-cover", "public", position, ref.ref, art.digest, "published");
        int ok = ph_art_cms(have ? "update" : "create", site.id, have ? row.id : 0, payload, have ? row.snapshot : NULL);
        free(payload);
        PhArtRow after;
        if (ph_art_row(db, site.id, "profile-cover", coordinate, 0, &after) != 1 || strcmp(after.status, "published") || strcmp(after.audience, "public") ||
            strcmp(after.ref, ref.ref) || after.position != position || strcmp(after.evidence, art.digest)) {
            ph_error(ok ? 503 : 409, "The header change was not confirmed. Reload your profile before trying again.\n"); return 1;
        }
        ph_redirect(&site, "overview"); return 1;
    }
    if (!strcmp(target, "/profile/gallery/add")) {
        if (!ph_art_ref(art_text, &ref)) { ph_error(400, "Choose an artwork from its page.\n"); return 1; }
        if (!ph_art_resolve(db, &ref, PH_GALLERY_LABEL, 50, 50, &art)) { ph_error(409, "This artwork is no longer available for a gallery. Reload its page.\n"); return 1; }
        ph_gallery_coordinate(&ref, coordinate);
        int have = ph_art_row(db, site.id, "profile-gallery", coordinate, 0, &row), ok = 1;
        if (have < 0) { ph_error(409, "This gallery entry could not be read. Reload your gallery.\n"); return 1; }
        if (!have || (strcmp(row.status, "draft") && strcmp(row.status, "published"))) {
            sqlite3_stmt *st = NULL; long long next = 1, live = 0;
            if (sqlite3_prepare_v2(db, "SELECT COALESCE(MAX(position),0)+1,SUM(CASE WHEN status IN ('draft','published') THEN 1 ELSE 0 END) FROM site_fragment WHERE namespace=?1 AND kind='profile-gallery'", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, site.id, -1, SQLITE_STATIC);
                if (sqlite3_step(st) == SQLITE_ROW) { next = sqlite3_column_int64(st, 0); live = sqlite3_column_int64(st, 1); }
            }
            sqlite3_finalize(st);
            if (live >= PH_GALLERY_MAX) { ph_error(409, "This gallery already holds 48 pieces. Remove one before adding another.\n"); return 1; }
            char *payload = ph_art_payload(have ? NULL : coordinate, "gallery", "profile-gallery", "self", next, ref.ref, art.digest, "draft");
            ok = ph_art_cms(have ? "update" : "create", site.id, have ? row.id : 0, payload, have ? row.snapshot : NULL);
            free(payload);
        }
        PhArtRow after;
        if (ph_art_row(db, site.id, "profile-gallery", coordinate, 0, &after) != 1 || (strcmp(after.status, "draft") && strcmp(after.status, "published")) || strcmp(after.ref, ref.ref)) {
            ph_error(ok ? 503 : 409, "Adding to your gallery was not confirmed. Reload your gallery before trying again.\n"); return 1;
        }
        ph_redirect(&site, "gallery"); return 1;
    }
    /* publish, hide and remove act on one exact reviewed row. */
    if (!ph_digits(id) || strlen(expected) != 64 || strspn(expected, "0123456789abcdef") != 64) { ph_error(400, "Reload your gallery and choose the piece again.\n"); return 1; }
    long rowid = strtol(id, NULL, 10);
    if (rowid < 1 || rowid > INT_MAX || ph_art_row(db, site.id, "profile-gallery", NULL, (int)rowid, &row) != 1 ||
        (strcmp(row.status, "draft") && strcmp(row.status, "published"))) { ph_error(404, "That piece is not in this gallery.\n"); return 1; }
    if (strcmp(row.snapshot, expected)) { ph_error(409, "This piece changed since you opened your gallery. Reload it first.\n"); return 1; }
    const char *status = "draft", *audience = "self", *evidence = NULL;
    if (!strcmp(target, "/profile/gallery/publish")) {
        if (!ph_art_ref(row.ref, &ref) || !ph_art_resolve(db, &ref, PH_GALLERY_LABEL, 50, 50, &art)) { ph_error(409, "This artwork's source changed or is gone, so it can't be shown. Remove it from your gallery.\n"); return 1; }
        status = "published"; audience = "public"; evidence = art.digest;
    } else if (!strcmp(target, "/profile/gallery/remove")) status = "archived";
    char *payload = ph_art_payload(NULL, NULL, NULL, audience, -1, NULL, evidence, status);
    int ok = ph_art_cms("update", site.id, row.id, payload, row.snapshot);
    free(payload);
    PhArtRow after;
    if (ph_art_row(db, site.id, "profile-gallery", NULL, row.id, &after) != 1 || strcmp(after.status, status) || strcmp(after.audience, audience) ||
        (evidence && strcmp(after.evidence, evidence))) {
        ph_error(ok ? 503 : 409, "The gallery change was not confirmed. Reload your gallery before trying again.\n"); return 1;
    }
    ph_redirect(&site, "gallery"); return 1;
}
#endif
