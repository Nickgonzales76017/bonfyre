#ifndef BONFYRE_PUBLICATION_WORKSPACE_H
#define BONFYRE_PUBLICATION_WORKSPACE_H
/* Public retained excerpts are inputs. Selection grants no publication rights. */
static int pw_inputs(const char *method,const char *query) {
    if(strcmp(method,"GET")){aw_error(405,"Use GET to choose published material.\n");return 1;}
    char search[257]="",cursor[24]="0",details[2]="0";unsigned seen=0;
    if(query&&strlen(query)>2048){aw_error(400,"Shorten the publication search.\n");return 1;}
    for(const char *p=query?query:"";*p;){
        const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char name[16];
        if(!eq||!psetup_decode(p,(size_t)(eq-p),name,sizeof(name)))goto bad_query;
        unsigned bit;char *out;size_t cap;
        if(!strcmp(name,"q")){bit=1;out=search;cap=sizeof(search);}else if(!strcmp(name,"cursor")){bit=2;out=cursor;cap=sizeof(cursor);}else if(!strcmp(name,"details")){bit=4;out=details;cap=sizeof(details);}else goto bad_query;
        if(seen&bit||!psetup_decode(eq+1,(size_t)(end-eq-1),out,cap))goto bad_query;
        seen|=bit;p=*end?end+1:end;
    }
    if(!*cursor||strlen(cursor)>18||strspn(cursor,"0123456789")!=strlen(cursor)||(strcmp(details,"0")&&strcmp(details,"1")))goto bad_query;
    int with_subject=!strcmp(details,"1");
    long long after=strtoll(cursor,NULL,10);sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;int ok=0,step=SQLITE_DONE,count=0;long long last=0;
    bf_mp_writer_t rows,out;bf_mp_writer_init(&rows,4096);bf_mp_writer_init(&out,AW_PACKET+64);
    const char *sql="SELECT p.occurrence_id,p.title,COALESCE(p.summary,''),p.canonical_href,p.source_ref,p.receipt_ref,p.rights_basis,p.retention,COALESCE(p.published_at,p.observed_at,''),p.id,COALESCE(p.source_twin,'') "
        "FROM publication_occurrence p WHERE p.namespace='root' AND p.status='published' AND p.visibility='public' AND p.assertion_state='OBSERVED' "
        "AND (?2=0 OR p.id<?2) AND (instr(lower(p.title),lower(?1))>0 OR instr(lower(COALESCE(p.summary,'')),lower(?1))>0) "
        "AND length(p.occurrence_id) BETWEEN 1 AND 256 AND length(p.title) BETWEEN 1 AND 300 AND length(COALESCE(p.summary,''))<=12000 "
        "AND length(p.canonical_href) BETWEEN 1 AND 2048 AND (p.canonical_href LIKE 'https://%' OR p.canonical_href LIKE 'http://%') "
        "AND length(p.source_ref) BETWEEN 1 AND 256 AND length(p.receipt_ref) BETWEEN 1 AND 512 AND length(p.rights_basis)<=1024 AND length(p.retention)<=256 "
        "AND length(COALESCE(p.published_at,p.observed_at,''))<=128 AND EXISTS (SELECT 1 FROM internet_source i WHERE i.namespace=p.namespace AND i.source=p.source_ref AND i.status='published') "
        "ORDER BY p.id DESC LIMIT 25";
    if(!db||sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,search,-1,SQLITE_STATIC);sqlite3_bind_int64(s,2,after);ok=1;
    while((step=sqlite3_step(s))==SQLITE_ROW){
        if(count==24)break;
        if(bf_mp_pack_array(&rows,with_subject?10:9)){ok=0;break;}
        for(int col=0;col<9;col++){
            const char *value=(const char *)sqlite3_column_text(s,col);int n=sqlite3_column_bytes(s,col);
            if(!value||n<0||memchr(value,0,(size_t)n)||bf_mp_pack_str_cstr(&rows,value)){ok=0;break;}
        }
        if(ok&&with_subject){
            const char *subject=(const char*)sqlite3_column_text(s,10);int n=sqlite3_column_bytes(s,10);
            if(!subject||n<0||n>256||memchr(subject,0,(size_t)n)||bf_mp_pack_str_cstr(&rows,subject))ok=0;
        }
        if(!ok)break;last=sqlite3_column_int64(s,9);count++;
    }
    if(step!=SQLITE_ROW&&step!=SQLITE_DONE)ok=0;
    if(ok){
        ok=!bf_mp_pack_array(&out,3)&&!bf_mp_pack_uint(&out,with_subject?2:1)&&!bf_mp_pack_array(&out,(uint32_t)count);
        if(ok&&rows.len<=AW_PACKET-64&&out.buf&&out.cap>=out.len+rows.len){memcpy(out.buf+out.len,rows.buf,rows.len);out.len+=rows.len;}else ok=0;
        if(ok)ok=!bf_mp_pack_uint(&out,step==SQLITE_ROW?(uint64_t)last:0);
        if(ok&&out.len<=AW_PACKET)aw_response(200,"application/msgpack",out.buf,out.len,0);else ok=0;
    }
 done:sqlite3_finalize(s);if(db)sqlite3_close(db);bf_mp_writer_free(&rows);bf_mp_writer_free(&out);
    if(!ok)aw_error(503,"The published source catalogue could not be read.\n");return 1;
 bad_query:aw_error(400,"Use one search phrase and a valid publication cursor.\n");return 1;
}
#endif
