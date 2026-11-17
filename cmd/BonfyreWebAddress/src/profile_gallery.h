#ifndef BONFYRE_PROFILE_GALLERY_H
#define BONFYRE_PROFILE_GALLERY_H
#include "ui_tree.h"
#include "form_tree.h"
/* Account-chosen artwork on one member profile site. The source owner keeps
 * the artwork; a site_fragment only names its exact ref, the reviewed
 * fingerprint and its publication state. A header (kind profile-cover) and a
 * gallery entry (kind profile-gallery) re-resolve the source and its rights on
 * every read, so a changed or withdrawn source drops out of the public page.
 * Adding to a gallery saves a private draft; only an explicit owner review
 * makes an entry public. Nothing here copies the image or its record. */
#include "bf_jsonw.h"
#include "profile_authoring_overview.h"

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
}

static void ph_gallery_form(const PhSite *site, const char *action, const PhArtRow *row, const char *label, int quiet) {
    char id[24]; snprintf(id, sizeof(id), "%d", row->id);
    const FtField hidden[] = {{"site", site->id}, {"id", id}, {"expected", row->snapshot}};
    const FtSubmit go[] = {{.label = label, .tone = quiet ? "quiet" : NULL}};
    const FtForm f = {.id = "ph-gallery", .action = action, .hidden = hidden, .nhidden = 3, .submits = go, .nsubmits = 1};
    ft_form(stdout, &f);
}

/* The gallery as an editorial mosaic: a wide piece, then smaller and taller
 * ones, in the owner's order. Public readers get published entries whose
 * source still matches its review; the owner also sees private drafts. */
static void ph_gallery_emit(sqlite3 *db, const PhSite *site, int owner, int limit, int full) {
    sqlite3_stmt *st = NULL; int shown = 0, more = 0, seen = 0;
    if (!db || !site) return;
    /* Profile composition belongs to this site, not to the artwork gallery.
     * Keep the gallery tab separate from the public editorial overview. */
    if (!full) (void)bf_profile_authoring_overview_emit(stdout, db, site->id);
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
            ui_start(stdout,"section");ui_attr(stdout,"class","ph-gallery");ui_flag(stdout,"data-profile-gallery");ui_gt(stdout);ui_open(stdout,"div","ph-gallery-head");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Gallery");ui_close(stdout,"h2");
            if (!full) { ui_start(stdout,"a");ui_attr_open(stdout,"href"); ph_url(site, "gallery", owner); ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"See the whole gallery");ui_close(stdout,"a"); }
            ui_close(stdout,"div");
            if (owner) ui_text(stdout, "p", "ph-privacy", "Pieces marked private are visible only to you. Visitors see a piece only after you choose to show it.");
            ui_open(stdout,"div","ph-gallery-grid");
        }
        const char *span = seen % 7 == 0 ? "wide" : (seen % 7 == 1 || seen % 7 == 4) ? "tall" : "normal";
        seen++; shown++;
        ui_start(stdout,"figure");ui_attr(stdout,"class","ph-art");ui_flag(stdout,"data-gallery-item");ui_attr_open(stdout,"data-span"); ui_copy(stdout,span);
        ui_attr_close(stdout);ui_attr_open(stdout,"data-gallery-state"); ui_copy(stdout,published ? "public" : "private"); ui_attr_close(stdout);
        if (!valid) ui_flag(stdout,"data-gallery-stale");
        ui_gt(stdout);
        if (valid) {
            ui_start(stdout,"a");ui_attr(stdout,"class","ph-art-frame");ui_attr_open(stdout,"href"); ph_art_link(&ref); ui_attr_close(stdout);ui_gt(stdout);ui_start(stdout,"img");ui_attr(stdout,"loading","lazy");ui_attr(stdout,"decoding","async");ui_attr(stdout,"referrerpolicy","no-referrer");ui_attr_open(stdout,"src");
            html(art.image); ui_attr_close(stdout);ui_attr_open(stdout,"alt"); html(art.title); ui_attr_close(stdout);ui_gt(stdout);ui_close(stdout,"a");ui_open(stdout,"figcaption",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href"); ph_art_link(&ref); ui_attr_close(stdout);ui_gt(stdout);
            html(art.title); ui_close(stdout,"a");
            if (art.by[0]) { ui_text(stdout, "span", NULL, art.by); }
            ui_text(stdout, "small", NULL, "Public domain (CC0)");
        } else {
            ui_start(stdout,"div");ui_attr(stdout,"class","ph-art-frame");ui_attr(stdout,"aria-hidden","true");ui_gt(stdout);ui_close(stdout,"div");ui_open(stdout,"figcaption",NULL);ui_open(stdout,"span","ph-art-state");ui_copy(stdout,"Hidden from visitors");ui_close(stdout,"span");ui_open(stdout,"span",NULL);ui_copy(stdout,"The source for this piece changed or is no longer available, so it is not shown publicly. Remove it, or add it again from its page.");ui_close(stdout,"span");
        }
        if (owner) {
            if (valid) { ui_open(stdout,"span","ph-art-state"); ui_copy(stdout,published ? "In your public gallery" : "Private, only you can see this"); ui_close(stdout,"span"); }
            ui_open(stdout,"div","ph-art-owner");
            if (valid && !published) ph_gallery_form(site, "/profile/gallery/publish", &row, "Show in public gallery", 0);
            if (published) ph_gallery_form(site, "/profile/gallery/hide", &row, "Make private", 1);
            ph_gallery_form(site, "/profile/gallery/remove", &row, "Remove", 1);
            if (valid) { ui_start(stdout,"a");ui_attr(stdout,"class","ph-button");ui_attr_open(stdout,"href"); ph_art_tab_url(site, row.ref); ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Use as header");ui_close(stdout,"a"); }
            ui_close(stdout,"div");
        }
        ui_close(stdout,"figcaption");ui_close(stdout,"figure");
    }
    if (shown) {
        ui_close(stdout,"div");
        if (more && full) ui_text(stdout, "p", NULL, "Only the first 48 pieces are shown.");
        ui_close(stdout,"section");
    } else if (full) {
        ui_start(stdout,"div");ui_attr(stdout,"class","ph-empty");ui_flag(stdout,"data-profile-gallery-empty");ui_gt(stdout);ui_open(stdout,"h2",NULL);
        ui_copy(stdout,owner ? "Your gallery is empty" : "Nothing in this gallery yet");
        ui_close(stdout,"h2");ui_open(stdout,"p",NULL);
        ui_copy(stdout,owner ? "Open a public-domain artwork anywhere on Aurekai and choose Use on my profile. It stays private here until you show it." : "This gallery will show the artwork its owner chooses to share.");
        ui_close(stdout,"p");ui_close(stdout,"div");
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
    ui_text(stdout, "h2", NULL, "Use this artwork on your profile");
    if (site && ph_art_ref(art_text, &ref)) {
        /* Reopening the current header starts from its saved focus point. */
        ph_cover_coordinate(site, coordinate);
        have_cover = ph_art_row(db, site->id, "profile-cover", coordinate, 0, &cover);
        if (have_cover == 1 && !*fxs && !*fys && !strcmp(cover.ref, ref.ref) && cover.position % 101 <= 100) { fx = cover.position / 101; fy = cover.position % 101; }
    }
    if (!ph_art_ref(art_text, &ref) || fx < 0 || fy < 0 || !ph_art_resolve(db, &ref, PH_COVER_LABEL, fx, fy, &art)) {
        ui_start(stdout,"div");ui_attr(stdout,"class","ph-empty");ui_flag(stdout,"data-artwork-refused");ui_gt(stdout);ui_open(stdout,"p",NULL);ui_copy(stdout,"This artwork can't be used on a profile. Only pictures whose source marks them public domain (CC0) can be, and only while that source still lists them.");ui_close(stdout,"p");ui_close(stdout,"div");
        return;
    }
    if (!site) {
        ui_open(stdout,"div","ph-empty");ui_open(stdout,"p",NULL);ui_copy(stdout,"You need a published profile site first. ");ui_start(stdout,"a");ui_attr(stdout,"href","/account#presence");ui_gt(stdout);ui_copy(stdout,"Set up a site");ui_close(stdout,"a");ui_copy(stdout,".");ui_close(stdout,"p");ui_close(stdout,"div");
        return;
    }
    ui_start(stdout,"nav");ui_attr(stdout,"class","ph-art-sites");ui_attr(stdout,"aria-label","Choose which site");ui_gt(stdout);
    for (int i = 0; i < count; i++) {
        ui_start(stdout,"a");ui_attr_open(stdout,"href"); ph_art_tab_url(&sites[i], ref.ref); ui_attr_close(stdout);
        if (!strcmp(sites[i].id, site->id)) ui_attr(stdout,"aria-current","page");
        ui_gt(stdout); html(ph_label(sites[i].territory)); ui_copy(stdout," · "); html(sites[i].host); ui_close(stdout,"a");
    }
    ui_close(stdout,"nav");ui_start(stdout,"figure");ui_attr(stdout,"class","ph-art-preview");ui_flag(stdout,"data-artwork-review");ui_gt(stdout);ui_open(stdout,"div","ph-art-crop");ui_start(stdout,"img");ui_flag(stdout,"data-art-preview");ui_attr_open(stdout,"src");
    html(art.image); ui_attr_close(stdout);ui_attr_open(stdout,"alt"); html(art.title);
    ui_attr_close(stdout);ui_attr_open(stdout,"style");printf("object-position:%d%% %d%%",fx,fy);ui_attr_close(stdout);ui_attr(stdout,"referrerpolicy","no-referrer");ui_gt(stdout);ui_close(stdout,"div");ui_open(stdout,"figcaption",NULL);ui_open(stdout,"strong",NULL);
    html(art.title); ui_close(stdout,"strong");
    if (art.by[0]) { ui_text(stdout, "span", NULL, art.by); }
    ui_open(stdout,"span",NULL);ui_copy(stdout,"Public domain (CC0)");ui_close(stdout,"span");ui_start(stdout,"a");ui_attr_open(stdout,"href"); ph_art_link(&ref); ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Artwork record");ui_close(stdout,"a");
    if (public_http_url(art.href)) { ui_start(stdout,"a");ui_attr(stdout,"rel","noopener noreferrer");ui_attr_open(stdout,"href"); html(art.href); ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Where it comes from ↗");ui_close(stdout,"a"); }
    ui_close(stdout,"figcaption");ui_close(stdout,"figure");ui_open(stdout,"div","ph-art-choices");

    int current = have_cover == 1 && !strcmp(cover.status, "published") && !strcmp(cover.ref, ref.ref);
    ui_start(stdout,"form");ui_attr(stdout,"class","ph-form");ui_attr(stdout,"method","post");ui_attr(stdout,"action","/profile/cover/set");ui_flag(stdout,"data-cover-form");ui_gt(stdout);ui_open(stdout,"h3",NULL);ui_copy(stdout,"Profile header");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);
    if (current) { ui_copy(stdout,"This is your header on "); html(site->host); ui_copy(stdout,". Move the focus point and save it."); }
    else if (have_cover == 1 && !strcmp(cover.status, "published")) { ui_copy(stdout,"This replaces the header you have now on "); html(site->host); ui_copy(stdout,"."); }
    else { ui_copy(stdout,"It goes at the top of "); html(site->host); ui_copy(stdout,". Your profile picture stays the same."); }
    ui_close(stdout,"p");
    ph_hidden("site", site->id); ph_hidden("art", ref.ref); ph_hidden("expected", have_cover == 1 ? cover.snapshot : "none");
    ui_open(stdout,"label",NULL);ui_copy(stdout,"Focus left to right");ui_start(stdout,"input");ui_attr(stdout,"type","range");ui_attr(stdout,"name","fx");ui_attr(stdout,"min","0");ui_attr(stdout,"max","100");ui_attr(stdout,"step","1");ui_attr_open(stdout,"value");printf("%d",fx);ui_attr_close(stdout);ui_attr(stdout,"data-art-focus","x");ui_gt(stdout);ui_close(stdout,"label");
    ui_open(stdout,"label",NULL);ui_copy(stdout,"Focus top to bottom");ui_start(stdout,"input");ui_attr(stdout,"type","range");ui_attr(stdout,"name","fy");ui_attr(stdout,"min","0");ui_attr(stdout,"max","100");ui_attr(stdout,"step","1");ui_attr_open(stdout,"value");printf("%d",fy);ui_attr_close(stdout);ui_attr(stdout,"data-art-focus","y");ui_gt(stdout);ui_close(stdout,"label");
    ui_start(stdout,"button");ui_attr(stdout,"type","submit");ui_gt(stdout); ui_copy(stdout,current ? "Save the focus point" : "Use as my header"); ui_close(stdout,"button");ui_close(stdout,"form");

    ph_gallery_coordinate(&ref, coordinate);
    int have_entry = ph_art_row(db, site->id, "profile-gallery", coordinate, 0, &entry);
    ui_start(stdout,"div");ui_attr(stdout,"class","ph-form");ui_flag(stdout,"data-gallery-choice");ui_gt(stdout);ui_open(stdout,"h3",NULL);ui_copy(stdout,"Gallery");ui_close(stdout,"h3");
    if (have_entry == 1 && !strcmp(entry.status, "published")) {
        ui_open(stdout,"p",NULL);ui_copy(stdout,"This is in your public gallery on "); html(site->host); ui_copy(stdout,".");ui_close(stdout,"p");
    } else if (have_entry == 1 && !strcmp(entry.status, "draft")) {
        ui_open(stdout,"p",NULL);ui_copy(stdout,"This is in your gallery on "); html(site->host); ui_copy(stdout,", private until you show it.");ui_close(stdout,"p");
    } else {
        ui_open(stdout,"p",NULL);ui_copy(stdout,"Add it to your gallery on "); html(site->host); ui_copy(stdout,". It stays private until you choose to show it.");ui_close(stdout,"p");
        {const FtField hidden[]={{"site",site->id},{"art",ref.ref}};
         const FtSubmit go[]={{.label="Add to my gallery"}};
         const FtForm f={.id="ph-gallery",.action="/profile/gallery/add",.hidden=hidden,.nhidden=2,.submits=go,.nsubmits=1};
         ft_form(stdout,&f);}
    }
    ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href"); ph_url(site, "gallery", 1); ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Review your gallery");ui_close(stdout,"a");ui_close(stdout,"p");ui_close(stdout,"div");ui_close(stdout,"div");
    ui_script(stdout,"(function(){var i=document.querySelector('[data-art-preview]'),x=document.querySelector('[data-art-focus=x]'),y=document.querySelector('[data-art-focus=y]');"
          "if(!i||!x||!y)return;function u(){i.style.objectPosition=x.value+'% '+y.value+'%';}x.addEventListener('input',u);y.addEventListener('input',u);})();");
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
    if (!ph_owned_site(db, siteid, &site) || strcmp(site.actor, actor)) { ph_error(404, "This profile site is not available to this account.\n"); return 1; }
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
