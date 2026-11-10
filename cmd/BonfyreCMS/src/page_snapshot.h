#ifndef BONFYRE_PAGE_SNAPSHOT_H
#define BONFYRE_PAGE_SNAPSHOT_H
/* Reads the existing page aggregate. The caller holds a SQLite transaction. */
static int page_snapshot_append(char **body, size_t *used, const char *text) {
    size_t n = strlen(text);
    if (n > MAX_BODY || *used > MAX_BODY - n) return 0;
    char *next = realloc(*body, *used + n + 1);
    if (!next) return 0;
    memcpy(next + *used, text, n + 1); *used += n; *body = next;
    return 1;
}
static char *page_snapshot(sqlite3 *db, const char *ns, const char *route, char digest[65], long long *id) {
    sqlite3_stmt *st = NULL;
    char *body = NULL, *row = NULL;
    size_t used = 0; int step, count = 0, ok = 0;
    const ContentType *page_type = find_type("site_page"), *fragment_type = find_type("site_fragment");
    *id = 0; digest[0] = 0;
    if (!ns || !*ns || strlen(ns) > 127 || !route || !*route || strlen(route) > 127 || !page_type || !fragment_type) goto done;
    if (sqlite3_prepare_v2(db,"SELECT json_object('namespace',?1,'route',?2)",-1,&st,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_text(st,1,ns,-1,SQLITE_STATIC); sqlite3_bind_text(st,2,route,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW || !page_snapshot_append(&body,&used,(const char*)sqlite3_column_text(st,0))) goto done;
    body[--used]=0; sqlite3_finalize(st); st=NULL;
    if (!page_snapshot_append(&body,&used,",\"page\":")) goto done;
    if (sqlite3_prepare_v2(db,"SELECT id FROM site_page WHERE namespace=?1 AND route=?2",-1,&st,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_text(st,1,ns,-1,SQLITE_STATIC); sqlite3_bind_text(st,2,route,-1,SQLITE_STATIC);
    step=sqlite3_step(st);
    if (step==SQLITE_ROW) { *id=sqlite3_column_int64(st,0); step=sqlite3_step(st); }
    if (step!=SQLITE_DONE) goto done;
    sqlite3_finalize(st); st=NULL;
    row=*id?entry_row_json(db,page_type,*id):strdup("null");
    if (!row || !page_snapshot_append(&body,&used,row) || !page_snapshot_append(&body,&used,",\"fragments\":[")) goto done;
    free(row); row=NULL;
    if (sqlite3_prepare_v2(db,"SELECT id FROM site_fragment WHERE namespace=?1 AND page=?2 ORDER BY position,id",-1,&st,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_text(st,1,ns,-1,SQLITE_STATIC); sqlite3_bind_text(st,2,route,-1,SQLITE_STATIC);
    while ((step=sqlite3_step(st))==SQLITE_ROW) {
        if (++count>512 || (count>1 && !page_snapshot_append(&body,&used,","))) goto done;
        row=entry_row_json(db,fragment_type,sqlite3_column_int64(st,0));
        if (!row || !page_snapshot_append(&body,&used,row)) goto done;
        free(row); row=NULL;
    }
    if (step!=SQLITE_DONE || !page_snapshot_append(&body,&used,"]}")) goto done;
    bf_sha256_hex((const uint8_t*)body,used,digest); ok=1;
 done:
    sqlite3_finalize(st); free(row);
    if (!ok) { free(body); body=NULL; digest[0]=0; }
    return body;
}
static int entry_page_read(sqlite3 *db,const char *ns,const char *route) {
    char digest[65],*body=NULL; long long id=0;
    if (sqlite3_exec(db,"BEGIN",NULL,NULL,NULL)!=SQLITE_OK) goto refused;
    body=page_snapshot(db,ns,route,digest,&id);
    if (!body || sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK) {
        sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL); free(body); goto refused;
    }
    printf("{\"schema\":\"bonfyre.page-snapshot.v1\",\"id\":%lld,\"digest\":\"%s\",\"snapshot\":%s}\n",id,digest,body);
    free(body); return 0;
 refused:
    fputs("{\"error\":\"page snapshot unavailable\"}\n",stdout); return 1;
}
#endif
