#ifndef BONFYRE_PROFILE_COVER_PROJECTION_H
#define BONFYRE_PROFILE_COVER_PROJECTION_H
#include "profile_cover_source.h"
#include "../../BonfyreCMS/src/entry_snapshot.h"

/* A published fragment selects one source-owned image for this exact site.
 * Re-resolve the source and its rights on every read; a withdrawn or changed
 * source cannot leave an old image published by this projection. */
static int ph_cover_emit(const PhSite *site) {
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL;
    PageMaterial *material = NULL; const PageItem *item = NULL;
    char ref[128] = "", evidence[65] = "", source[96] = "", key[25] = "", digest[65], coordinate[160];
    int position = -1, result = 0;
    if (!site || snprintf(coordinate, sizeof(coordinate), "profile-cover-%s", site->id) >= (int)sizeof(coordinate) || !(db = cms_open_ro())) goto done;
    if (sqlite3_prepare_v2(db,
        "SELECT source_ref,position,evidence_ref FROM site_fragment "
        "WHERE namespace=?1 AND coordinate=?2 AND page='/' "
        "AND slot='cover' AND audience='public' AND kind='profile-cover' AND status='published' LIMIT 2",
        -1, &st, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, coordinate, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) goto done;
    const char *r = (const char *)sqlite3_column_text(st, 0);
    const char *e = (const char *)sqlite3_column_text(st, 2);
    if (!r || !e || strlen(r) >= sizeof(ref) || strlen(e) != 64) goto done;
    snprintf(ref, sizeof(ref), "%s", r); snprintf(evidence, sizeof(evidence), "%s", e);
    if (sqlite3_column_type(st, 1) != SQLITE_INTEGER) goto done;
    sqlite3_int64 stored_position = sqlite3_column_int64(st, 1);
    if (stored_position < 0 || stored_position > 10200) goto done;
    position = (int)stored_position;
    if (sqlite3_step(st) != SQLITE_DONE || position < 0 || position > 10200) goto done;
    sqlite3_finalize(st); st = NULL;
    const char *slash = strchr(ref, '/');
    if (!slash || slash == ref || (size_t)(slash-ref) >= sizeof(source) ||
        strlen(slash+1) != 24 || strchr(slash+1, '/')) goto done;
    memcpy(source, ref, (size_t)(slash-ref)); source[slash-ref] = 0;
    snprintf(key, sizeof(key), "%s", slash+1);
    material = calloc(1, sizeof(*material));
    if (!material || !page_item_by_ref(db, source, key, material, &item) || !item ||
        !page_is_photo(item) || !page_image_url_safe(item->image)) goto done;
    BfProfileCoverSource review = {source,key,item->title,item->by,item->href,item->image,item->rights,
                                   position/101,position%101};
    if (!bf_profile_cover_source_digest(&review, digest) || strcmp(digest, evidence)) goto done;
    fputs("<style>.ph-cover-art{margin:0;height:clamp(180px,25vw,310px);position:relative;overflow:hidden;border-radius:24px 24px 0 0;background:#193d30}.ph-cover-art img{width:100%;height:100%;object-fit:cover;display:block}.ph-cover-credit{position:absolute;right:12px;bottom:12px;max-width:75%;padding:6px 10px;border-radius:8px;background:#122c22e8;color:#fff;font:12px/1.4 system-ui}.ph-cover-credit a{color:inherit;text-decoration:underline}.ph-cover-credit a:focus-visible{outline:3px solid #f5ce67;outline-offset:3px}</style><figure class=\"ph-cover-art\" data-profile-cover><img src=\"",stdout);
    html(item->image); fputs("\" alt=\"",stdout); html(item->title);
    printf("\" style=\"object-position:%d%% %d%%\" referrerpolicy=\"no-referrer\">",review.focal_x,review.focal_y);
    fputs("<figcaption class=\"ph-cover-credit\"><a href=\"/o/",stdout); html(source); fputc('/',stdout); html(key);
    fputs("\">",stdout); html(item->title); if (item->by[0]) { fputs(" · ",stdout); html(item->by); }
    fputs(" · CC0</a></figcaption></figure>",stdout);
    result = 1;
done:
    sqlite3_finalize(st); if (db) sqlite3_close(db); free(material); return result;
}
/* The control snapshots the complete CMS row. The owner rechecks the same
 * digest atomically when removing it, including changes made by other tools. */
static void ph_cover_remove_form(const PhSite *site) {
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int id = 0, position = -1; char digest[65],coordinate[160],text[24],ref[128] = "";
    if (!site || snprintf(coordinate,sizeof(coordinate),"profile-cover-%s",site->id) >= (int)sizeof(coordinate) ||
        !(db=cms_open_ro())) goto done;
    if (sqlite3_prepare_v2(db,"SELECT id,COALESCE(source_ref,''),position FROM site_fragment WHERE namespace=?1 AND coordinate=?2 AND kind='profile-cover' AND status='published' LIMIT 2",-1,&st,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_text(st,1,site->id,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,coordinate,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st)!=SQLITE_ROW) goto done;
    sqlite3_int64 rowid=sqlite3_column_int64(st,0);
    { const char *r=(const char*)sqlite3_column_text(st,1); if(r&&strlen(r)<sizeof(ref))snprintf(ref,sizeof(ref),"%s",r);
      if(sqlite3_column_type(st,2)==SQLITE_INTEGER){sqlite3_int64 p=sqlite3_column_int64(st,2);if(p>=0&&p<=10200&&p%101<=100)position=(int)p;} }
    if (rowid<1 || rowid>INT_MAX || sqlite3_step(st)!=SQLITE_DONE) goto done;
    id=(int)rowid;sqlite3_finalize(st);st=NULL;
    if (!cms_snapshot_row(db,"site_fragment",id,site->id,digest)) goto done;
    snprintf(text,sizeof(text),"%d",id);
    fputs("<details class=\"ph-cover-options\"><summary>Cover options</summary>",stdout);
    if(ref[0]&&position>=0){fputs("<a data-cover-adjust href=\"/profile?site=",stdout);url_query_value(site->id);fputs("&amp;tab=artwork&amp;art=",stdout);url_query_value(ref);
        printf("&amp;fx=%d&amp;fy=%d\">Move the focus point</a>",position/101,position%101);}
    fputs("<form action=\"/profile/cover/remove\" method=\"post\">",stdout);
    ph_hidden("site",site->id);ph_hidden("id",text);ph_hidden("expected",digest);
    fputs("<button type=\"submit\">Remove cover</button></form></details>",stdout);
done:
    sqlite3_finalize(st);if(db)sqlite3_close(db);
}
#endif
