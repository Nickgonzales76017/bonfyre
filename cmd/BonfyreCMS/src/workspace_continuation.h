#ifndef BONFYRE_CMS_WORKSPACE_CONTINUATION_H
#define BONFYRE_CMS_WORKSPACE_CONTINUATION_H
/* Called inside cw_dispatch's existing read transaction. Resolve one subject,
 * one record and all compatible installed bindings at the same database cut. */
static int cw_context_pack(bf_mp_writer_t *out,const CwBinding *b) {
    const char *fields[]={b->id,b->subject,b->scope,b->pack,b->release,b->entry,b->family,b->record_ns};
    if(bf_mp_pack_array(out,9) || bf_mp_pack_uint(out,1))return 0;
    for(size_t n=0;n<8;n++)if(bf_mp_pack_str_cstr(out,fields[n]))return 0;return 1;
}
static int cw_continuation(sqlite3 *db,const CwBinding *source,const char *actor,const char *key) {
    sqlite3_stmt *record=NULL,*query=NULL;bf_mp_writer_t candidates,out;bf_mp_writer_init(&candidates,2048);bf_mp_writer_init(&out,2048);
    int rc=6,step=SQLITE_DONE;uint32_t count=0;size_t visited=0;
    if(!cw_key(source,key)){rc=4;goto done;}
    if(sqlite3_prepare_v2(db,"SELECT r.record_key,r.revision,b.body,r.deleted,r.body_digest FROM _workspace_record r JOIN _workspace_body b ON b.scope_key=r.scope_key AND b.digest=r.body_digest WHERE r.scope_key=?1 AND r.record_key=?2 AND r.deleted=0",-1,&record,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(record,1,source->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(record,2,key,-1,SQLITE_STATIC);step=sqlite3_step(record);
    if(step==SQLITE_DONE){rc=4;goto done;}if(step!=SQLITE_ROW)goto done;
    if(bf_mp_pack_array(&out,4) || bf_mp_pack_uint(&out,1) || !cw_context_pack(&out,source) || !cw_row(&out,record))goto done;
    if(sqlite3_prepare_v2(db,"SELECT w.binding_id,COALESCE(p.canonical_name,i.pack) FROM _application_workspace w JOIN application_binding a ON a.id=w.cms_id AND a.binding_id=w.binding_id JOIN pack_installation i ON i.id=w.installation_id AND i.namespace=a.namespace LEFT JOIN pack_artifact p ON p.namespace='root' AND p.pack=i.pack WHERE i.installed_for=?1 AND a.continuity_key=?2 AND i.status='published' AND a.status='active' ORDER BY w.binding_id LIMIT 65",-1,&query,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(query,1,source->subject,-1,SQLITE_STATIC);sqlite3_bind_text(query,2,source->record_ns,-1,SQLITE_STATIC);
    char previous[64]="";
    while((step=sqlite3_step(query))==SQLITE_ROW){
        char id[64],label[256];if(++visited>64){rc=5;goto done;}
        if(!bfpg_text(query,0,id,sizeof(id)) || !bfpg_text(query,1,label,sizeof(label)) || !strcmp(previous,id))goto done;snprintf(previous,sizeof(previous),"%s",id);
        CwBinding target;if(!cw_lookup(db,id,source->subject,source->scope,&target) || strcmp(target.record_ns,source->record_ns) || strcmp(target.family,source->family) || !cw_key(&target,key))continue;
        if(bf_mp_pack_array(&candidates,2) || !cw_context_pack(&candidates,&target) || bf_mp_pack_str_cstr(&candidates,label))goto done;count++;
    }
    if(step!=SQLITE_DONE || bf_mp_pack_array(&out,count) || out.len+candidates.len>CW_MAX_PACKET)goto done;
    rc=fwrite(out.buf,1,out.len,stdout)==out.len && fwrite(candidates.buf,1,candidates.len,stdout)==candidates.len?0:6;
 done:sqlite3_finalize(record);sqlite3_finalize(query);bf_mp_writer_free(&out);bf_mp_writer_free(&candidates);(void)actor;return rc;
}
#endif
