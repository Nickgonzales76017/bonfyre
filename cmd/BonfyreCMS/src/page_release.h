#ifndef BONFYRE_PAGE_RELEASE_H
#define BONFYRE_PAGE_RELEASE_H
/* A reviewed site's article release is one retained CMS transition, not
 * a sequence of independent page/fragment updates. The browser owner checks
 * the current actor, site, origin and acknowledgement before invoking this. */
static int page_release_route(const char *route) {
    if (!route || strncmp(route, "/blog/", 6)) return 0;
    size_t n = strlen(route);
    if (n < 9 || n > 127 || route[n-1] != '/') return 0;
    for (size_t i = 6; i < n-1; i++)
        if (!(route[i]>='a'&&route[i]<='z') &&
            !(route[i]>='0'&&route[i]<='9') && route[i]!='-') return 0;
    return 1;
}
static int page_release_digest(const char *s) {
    if (!s || strlen(s)!=64) return 0;
    for (int i=0;i<64;i++) if (!((s[i]>='0'&&s[i]<='9')||(s[i]>='a'&&s[i]<='f'))) return 0;
    return 1;
}
static int entry_page_release(sqlite3 *db,const char *ns,const char *route,
                              long long expected_id,const char *expected_digest,
                              const char *author) {
    sqlite3_stmt *st=NULL;
    int ids[512],parents[512],count=0,story=0,step,started=0,committed=0,rc=1;
    char coordinates[512][129],parent_names[512][129],before[65]={0},after[65]={0};
    char *snapshot=NULL;long long id=0,after_id=0;
    if (!db || !ns || !*ns || strlen(ns)>127 || !page_release_route(route) ||
        expected_id<1 || expected_id>INT_MAX || !page_release_digest(expected_digest) ||
        !author || strncmp(author,"person:",7) || strlen(author)>159) goto done;
    if (sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK) goto done;
    started=1;defer_hooks=1;deferred_write_error=0;
    snapshot=page_snapshot(db,ns,route,before,&id);
    if (!snapshot || id!=expected_id || strcmp(before,expected_digest)) goto done;
    free(snapshot);snapshot=NULL;
    if (sqlite3_prepare_v2(db,"SELECT status,COALESCE(editorial_kind,''),COALESCE(author_credit,''),COALESCE(author_subject_ref,''),COALESCE(machine_view,'') FROM site_page WHERE id=?1 AND namespace=?2 AND route=?3",-1,&st,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_int64(st,1,id);sqlite3_bind_text(st,2,ns,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,route,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW) goto done;
    const char *status=(const char*)sqlite3_column_text(st,0),*kind=(const char*)sqlite3_column_text(st,1),
               *credit=(const char*)sqlite3_column_text(st,2),*identity=(const char*)sqlite3_column_text(st,3),
               *view=(const char*)sqlite3_column_text(st,4);
    if (!status||strcmp(status,"draft") || !kind||strcmp(kind,"article") ||
        !credit||!*credit || !identity||strcmp(identity,author) ||
        !view||strcmp(view,"authored")) goto done;
    sqlite3_finalize(st);st=NULL;
    const char *sql=
      "SELECT id,coordinate,kind,status,COALESCE(source_ref,''),COALESCE(source_digest,''),"
      "CASE WHEN json_valid(semantic_tags) THEN json_extract(semantic_tags,'$.authoring_v1.parent') END "
      "FROM site_fragment WHERE namespace=?1 AND page=?2 AND slot='showcase' AND audience='public' "
      "AND json_valid(semantic_tags) AND json_extract(semantic_tags,'$.authoring_v1.version')=1 "
      "AND json_valid(projection_contract) AND json_extract(projection_contract,'$.html')=1 "
      "ORDER BY position,id LIMIT 513";
    if (sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_text(st,1,ns,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,route,-1,SQLITE_STATIC);
    while ((step=sqlite3_step(st))==SQLITE_ROW) {
        if (count==512) goto done;
        const char *coordinate=(const char*)sqlite3_column_text(st,1),
                   *fragment_kind=(const char*)sqlite3_column_text(st,2),
                   *fragment_status=(const char*)sqlite3_column_text(st,3),
                   *source=(const char*)sqlite3_column_text(st,4),
                   *digest=(const char*)sqlite3_column_text(st,5),
                   *parent=(const char*)sqlite3_column_text(st,6);
        if (!coordinate||!*coordinate||strlen(coordinate)>128||
            !fragment_kind||(!strstr(fragment_kind,"profile-") &&
            strncmp(fragment_kind,"ui:",3)&&strncmp(fragment_kind,"service:",8)) ||
            !fragment_status||strcmp(fragment_status,"draft") ||
            !source||!digest || (source[0] && !page_release_digest(digest)) ||
            (!source[0] && digest[0]) || (parent && strlen(parent)>128)) goto done;
        if (source[0] && strncmp(source,"editorial/posts/",16) &&
            strncmp(source,"local-markdown:",15)) goto done;
        if (strstr(source,"..") || strchr(source,'\\')) goto done;
        if (!strcmp(fragment_kind,"profile-story") ||
            !strcmp(fragment_kind,"service:prose") ||
            !strcmp(fragment_kind,"ui:editorial-story")) story++;
        ids[count]=sqlite3_column_int(st,0);
        snprintf(coordinates[count],sizeof(coordinates[count]),"%s",coordinate);
        snprintf(parent_names[count],sizeof(parent_names[count]),"%s",parent?parent:"");
        parents[count]=-1;count++;
    }
    if (step!=SQLITE_DONE || !count || !story) goto done;
    sqlite3_finalize(st);st=NULL;
    /* Do not release an inconsistent or cyclic authoring structure. */
    for (int i=0;i<count;i++) {
        if (!parent_names[i][0]) continue;
        for (int j=0;j<count;j++) if (!strcmp(parent_names[i],coordinates[j])) {
            if (parents[i]>=0) goto done;
            parents[i]=j;
        }
        if (parents[i]<0) goto done;
        int cursor=i;
        for (int depth=0;depth<=count;depth++) {
            cursor=parents[cursor];
            if (cursor<0) break;
            if (cursor==i||depth==count) goto done;
        }
    }
    /* A page cannot hide nonpublic or uncompiled fragments in its aggregate. */
    if (sqlite3_prepare_v2(db,"SELECT count(*) FROM site_fragment WHERE namespace=?1 AND page=?2",-1,&st,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_text(st,1,ns,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,route,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW || sqlite3_column_int(st,0)!=count) goto done;
    sqlite3_finalize(st);st=NULL;
    for (int i=0;i<count;i++) if (entry_update(db,"site_fragment",ids[i],ns,"{\"status\":\"published\"}")) goto done;
    if (entry_update(db,"site_page",(int)id,ns,"{\"status\":\"published\"}") || deferred_write_error) goto done;
    snapshot=page_snapshot(db,ns,route,after,&after_id);
    if (!snapshot || after_id!=id || !strcmp(after,before)) goto done;
    free(snapshot);snapshot=NULL;
    if (sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK) goto done;
    started=0;committed=1;finish_deferred_hooks(db,1);
    int delta_ok=cms_emit_fact_delta("site_page",route,NULL,id);
    for (int i=0;i<count;i++) if (!cms_emit_fact_delta("site_fragment",coordinates[i],NULL,ids[i])) delta_ok=0;
    printf("{\"committed\":true,\"id\":%lld,\"released\":%d,\"digest\":\"%s\",\"fact_delta\":\"%s\"}\n",
        id,count,after,!cms_fact_deltas_enabled()?"DISABLED":delta_ok?"DELIVERED":"DELIVERY_FAILED");
    rc=delta_ok?0:2;
done:
    sqlite3_finalize(st);free(snapshot);
    if (started) sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
    if (!committed) { finish_deferred_hooks(db,0); fputs("{\"committed\":false,\"error\":\"article release refused or changed\"}\n",stdout); }
    return rc;
}
#endif
