#ifndef BONFYRE_PUBLICATION_RETAINED_H
#define BONFYRE_PUBLICATION_RETAINED_H
/* A bounded projection through the existing public source readers. No remote
 * fetch, source admission, publication write or duplicate text store occurs. */
static int pr_source(sqlite3 *db,const char *source,char *twin,size_t cap) {
    sqlite3_stmt *s=NULL;int ok=0;
    const char *sql="SELECT twin FROM internet_source WHERE source=?1 AND namespace='root' AND status='published' AND front_door='yes' AND state='observed' AND extract LIKE 'rss:%' AND (SELECT count(*) FROM internet_source x WHERE x.source=?1 AND x.status='published')=1";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(s,1,source,-1,SQLITE_STATIC);
    if(sqlite3_step(s)==SQLITE_ROW){const char *value=(const char*)sqlite3_column_text(s,0);int bytes=sqlite3_column_bytes(s,0);if(value&&bytes>0&&(size_t)bytes<cap&&!memchr(value,0,(size_t)bytes)&&foreign_safe_file(value)){memcpy(twin,value,(size_t)bytes+1);ok=1;}}
    sqlite3_finalize(s);return ok&&!page_twin_withheld(db,twin);
}
/* The item link and retained reader fingerprint the same complete input. */
static int pr_pack_item(bf_mp_writer_t *out,const PageItem *item,const char *source,const char *twin) {
    if(!item->body[0]||!item->title[0]||strlen(item->title)>300||!public_http_url(item->href))return 0;
    char fingerprint[65],receipt[480],rights[768],dated[64]="";bf_sha256_hex((const uint8_t*)item->body,strlen(item->body),fingerprint);
    snprintf(receipt,sizeof(receipt),"public-material:%s/%s;body-sha256:%s",source,item->key,fingerprint);
    if(item->rights[0]||item->rights_href[0])snprintf(rights,sizeof(rights),"%s%s%s",item->rights,item->rights[0]&&item->rights_href[0]?" · ":"",item->rights_href);else snprintf(rights,sizeof(rights),"Source terms not supplied. Retained public excerpt; no reuse licence inferred.");
    if(item->at>0){struct tm utc;if(gmtime_r(&item->at,&utc))strftime(dated,sizeof(dated),"%Y-%m-%dT%H:%M:%SZ",&utc);}
    int ok=!bf_mp_pack_array(out,10);const char *values[]={item->key,item->title,item->body,item->href,source,receipt,rights,"native normalized public excerpt",dated,twin};
    for(int k=0;k<10&&ok;k++)ok=!bf_mp_pack_str_cstr(out,values[k]);return ok;
}
static int pr_item_fingerprint(const PageItem *item,const char *source,const char *twin,char digest[65]) {
    bf_mp_writer_t out;bf_mp_writer_init(&out,4096);int ok=pr_pack_item(&out,item,source,twin);if(ok)bf_sha256_hex(out.buf,out.len,digest);bf_mp_writer_free(&out);return ok;
}
static int pr_query(const char *query,char *source,char *search,char *snapshot,char *selected,long long *cursor,int *readers) {
    unsigned seen=0;char next[24]="0",mode[2]="0";source[0]=search[0]=snapshot[0]=selected[0]=0;
    if(query&&strlen(query)>2048)return 0;
    for(const char *p=query?query:"";*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char name[20];char *out=NULL;size_t cap=0;unsigned bit=0;
      if(!eq||!psetup_decode(p,(size_t)(eq-p),name,sizeof(name)))return 0;
      if(!strcmp(name,"source")){out=source;cap=96;bit=1;}else if(!strcmp(name,"q")){out=search;cap=257;bit=2;}else if(!strcmp(name,"snapshot")){out=snapshot;cap=65;bit=4;}else if(!strcmp(name,"cursor")){out=next;cap=24;bit=8;}else if(!strcmp(name,"item")){out=selected;cap=25;bit=32;}else if(!strcmp(name,"readers")){out=mode;cap=2;bit=16;}else return 0;
      if(seen&bit||!psetup_decode(eq+1,(size_t)(end-eq-1),out,cap))return 0;seen|=bit;p=*end?end+1:end;
    }
    if(!next[0]||strlen(next)>15||strspn(next,"0123456789")!=strlen(next)||(!strcmp(mode,"0")&&!source[0])||(strcmp(mode,"0")&&strcmp(mode,"1")))return 0;
    *cursor=strtoll(next,NULL,10);*readers=!strcmp(mode,"1");
    if(*readers)return !source[0]&&!snapshot[0]&&!selected[0];
    if((seen&32)&&(strlen(selected)!=24||strspn(selected,"0123456789abcdef")!=24||*cursor||!snapshot[0]))return 0;
    return strspn(source,"abcdefghijklmnopqrstuvwxyz0123456789-_.")==strlen(source)&&!search[0]&&*cursor<=PAGE_STREAM_MAX&&(!snapshot[0]||(strlen(snapshot)==64&&strspn(snapshot,"0123456789abcdef")==64))&&(!*cursor||snapshot[0]);
}
static int pr_inputs(const char *method,const char *query,const char *host) {
    if(strcmp(method,"GET")){aw_error(405,"Use GET for retained source material.\n");return 1;}
    char source[96],search[257],snapshot[65],selected[25],twin[256];long long cursor=0;int readers=0;
    if(!pr_query(query,source,search,snapshot,selected,&cursor,&readers)){aw_error(400,"Choose one public feed or a bounded source search.\n");return 1;}
    sqlite3 *db=cms_open_ro();if(!db){aw_error(503,"The source owner is unavailable.\n");return 1;}
    bf_mp_writer_t out;bf_mp_writer_init(&out,4096);int ok=1;
    if(readers){
      sqlite3_stmt *s=NULL;struct{char source[96],label[301];long long id;} rows[24];int count=0,scanned=0,step=SQLITE_DONE;long long next=0;
      const char *sql="SELECT id,source,label FROM internet_source WHERE namespace='root' AND status='published' AND state='observed' AND front_door='yes' AND extract LIKE 'rss:%' AND id>?1 AND (instr(lower(source),lower(?2))>0 OR instr(lower(label),lower(?2))>0) AND length(source) BETWEEN 1 AND 95 AND length(CAST(label AS BLOB)) BETWEEN 1 AND 300 ORDER BY id LIMIT 128";
      if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)ok=0;
      if(ok){sqlite3_bind_int64(s,1,cursor);sqlite3_bind_text(s,2,search,-1,SQLITE_STATIC);while((step=sqlite3_step(s))==SQLITE_ROW){scanned++;const char *name=(const char*)sqlite3_column_text(s,1),*label=(const char*)sqlite3_column_text(s,2);next=sqlite3_column_int64(s,0);if(!name||!label||strspn(name,"abcdefghijklmnopqrstuvwxyz0123456789-_.")!=strlen(name)||!pr_source(db,name,twin,sizeof(twin)))continue;snprintf(rows[count].source,sizeof(rows[count].source),"%s",name);snprintf(rows[count].label,sizeof(rows[count].label),"%s",label);rows[count++].id=next;if(count==24)break;}if(step!=SQLITE_ROW&&step!=SQLITE_DONE)ok=0;}
      sqlite3_finalize(s);if(step==SQLITE_DONE&&scanned<128)next=0;
      if(ok)ok=!bf_mp_pack_array(&out,3)&&!bf_mp_pack_uint(&out,1)&&!bf_mp_pack_array(&out,(uint32_t)count);
      for(int i=0;i<count&&ok;i++)ok=!bf_mp_pack_array(&out,2)&&!bf_mp_pack_str_cstr(&out,rows[i].source)&&!bf_mp_pack_str_cstr(&out,rows[i].label);
      if(ok)ok=!bf_mp_pack_uint(&out,(uint64_t)next);
    }else{
      if(!pr_source(db,source,twin,sizeof(twin))){sqlite3_close(db);bf_mp_writer_free(&out);aw_error(404,"This source is not currently released for public reading.\n");return 1;}
      PageMaterial *material=calloc(1,sizeof(*material));if(!material)ok=0;
      if(ok){int prior=page_per_source,all_prior=page_read_everything;page_read_everything=selected[0]?1:all_prior;page_collect_stories(db,source,material);page_per_source=prior;page_read_everything=all_prior;
        if(selected[0]){PageItem projected;char family[64];int admission=page_subject_projection_item(db,host,source,selected,&projected,family,sizeof(family));if(admission<0)material->stream[BF_PAGE_MATERIAL_MIX].count=0;else if(admission>0){material->stream[BF_PAGE_MATERIAL_MIX].count=1;material->stream[BF_PAGE_MATERIAL_MIX].item[0]=projected;}}
      }
      bf_mp_writer_t all;bf_mp_writer_init(&all,4096);size_t starts[PAGE_STREAM_MAX+1]={0};int count=0;
      if(ok){PageStream *stream=&material->stream[BF_PAGE_MATERIAL_MIX];for(int i=0;i<stream->count&&count<PAGE_STREAM_MAX;i++){
        PageItem *item=&stream->item[i],projected;char family[64];
        if((selected[0]&&strcmp(item->key,selected))||strcmp(item->source,source)||strlen(item->key)!=24||strspn(item->key,"0123456789abcdef")!=24)continue;
        int admission=page_subject_projection_item(db,host,source,item->key,&projected,family,sizeof(family));if(admission<0)continue;if(admission>0)item=&projected;
        if(!item->body[0]||!item->title[0]||strlen(item->title)>300||!public_http_url(item->href))continue;
        starts[count]=all.len;ok=pr_pack_item(&all,item,source,twin);if(!ok)break;count++;
      }}
      starts[count]=all.len;char generation[65];bf_sha256_hex(all.buf,all.len,generation);
      if(ok&&selected[0]&&count!=1){free(material);bf_mp_writer_free(&all);bf_mp_writer_free(&out);sqlite3_close(db);aw_error(404,"The selected public excerpt is no longer available.\n");return 1;}
      if(ok&&snapshot[0]&&strcmp(snapshot,generation)){free(material);bf_mp_writer_free(&all);bf_mp_writer_free(&out);sqlite3_close(db);aw_error(409,"This source changed between pages. Reload the feed before selecting more material.\n");return 1;}
      if(ok&&cursor>count)ok=0;int take=ok?count-(int)cursor:0;if(take>24)take=24;
      if(ok)ok=!bf_mp_pack_array(&out,4)&&!bf_mp_pack_uint(&out,3)&&!bf_mp_pack_array(&out,(uint32_t)take);
      for(int i=0;i<take&&ok;i++){bf_mp_reader_t r;bf_mp_reader_init(&r,all.buf+starts[cursor+i],starts[cursor+i+1]-starts[cursor+i]);bf_mp_value_t v;if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=10){ok=0;break;}ok=!bf_mp_pack_array(&out,10);for(int j=0;j<10&&ok;j++){if(bf_mp_read_next(&r,&v)!=BF_MP_STR){ok=0;break;}ok=!bf_mp_pack_str(&out,(const char*)v.v.raw.ptr,v.v.raw.len);}}
      if(ok)ok=!bf_mp_pack_uint(&out,cursor+take<count?(uint64_t)(cursor+take):0)&&!bf_mp_pack_str_cstr(&out,generation);
      free(material);bf_mp_writer_free(&all);
    }
    sqlite3_close(db);if(ok&&out.len<=AW_PACKET)aw_response(200,"application/msgpack",out.buf,out.len,0);else aw_error(503,"The retained public source cut could not be read completely.\n");bf_mp_writer_free(&out);return 1;
}
#endif
