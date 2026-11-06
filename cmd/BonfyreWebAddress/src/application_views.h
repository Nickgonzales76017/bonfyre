#ifndef BONFYRE_APPLICATION_VIEWS_H
#define BONFYRE_APPLICATION_VIEWS_H
/* One saved record, viewed through current installations sharing its exact
 * owner coordinate. A matching key or similar title cannot join destinations. */
typedef struct {char id[64],pack[96],label[256],view[64],release[65],url[AC_URL];} AcAlternative;
static int ac_view_query(const char *query,char *key,char *revision,char *release,int *machine) {
    unsigned seen=0;char repr[24]="";key[0]=revision[0]=release[0]=0;*machine=0;
    if(!query || strlen(query)>4096)return 0;
    for(const char *p=query;*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char name[24],*out=NULL;size_t cap=0;unsigned bit=0;
        if(!eq || !ac_decode(p,(size_t)(eq-p),name,sizeof(name)))return 0;
        if(!strcmp(name,"key")){out=key;cap=512;bit=1;}else if(!strcmp(name,"revision")){out=revision;cap=24;bit=2;}else if(!strcmp(name,"release")){out=release;cap=65;bit=4;}else if(!strcmp(name,"repr")){out=repr;cap=24;bit=8;}else return 0;
        if(seen&bit || !ac_decode(eq+1,(size_t)(end-eq-1),out,cap))return 0;seen|=bit;p=*end?end+1:end;
    }
    if((seen!=7 && seen!=15) || !*key || !aw_hex(release,64) || !*revision || strlen(revision)>10 || strspn(revision,"0123456789")!=strlen(revision) || strtoll(revision,NULL,10)<1 || strtoll(revision,NULL,10)>=INT_MAX)return 0;
    if(*repr && strcmp(repr,"html") && strcmp(repr,"msgpack"))return 0;*machine=!strcmp(repr,"msgpack");return 1;
}
static int ac_alternative_url(AcAlternative *out,const AcContext *destination,const AcView *view,const AcRow *row,const char *digest) {
    char key[1536],vid[192];if(!ac_encoded(row->key,key,sizeof(key)) || !ac_encoded(view->id,vid,sizeof(vid)))return 0;
    int size=snprintf(out->url,sizeof(out->url),"/apps/%s/_open?view=%s&key=%s&revision=%lld&release=%s&digest=%s",destination->id,vid,key,row->revision,destination->release,digest);
    if(size<0 || size>=(int)sizeof(out->url))return 0;
    snprintf(out->id,sizeof(out->id),"%s",destination->id);snprintf(out->pack,sizeof(out->pack),"%s",destination->pack);snprintf(out->release,sizeof(out->release),"%s",destination->release);snprintf(out->view,sizeof(out->view),"%s",view->id);return 1;
}
static int ac_views(const char *relative,const char *query,const char *id,const char *subject,const char *scope,const char *pack,const char *release,const char *actor,const char *context) {
    if(strcmp(relative,"_views"))return 0;
    char key[512],revision[24],selected_release[65];int machine=0;
    if(!ac_view_query(query,key,revision,selected_release,&machine)){aw_error(400,"Choose one saved record and its reviewed version.\n");return 1;}
    if(strcmp(release,selected_release)){aw_error(409,"This editor installation changed. Reload its saved work before continuing.\n");return 1;}
    const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*dbpath=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
    char *const args[]={(char *)bin,"pack-workspace","--operation","continuation","--binding",(char *)id,"--subject",(char *)subject,"--scope",(char *)scope,"--actor",(char *)actor,"--prefix",key,"--db",(char *)dbpath,NULL};
    unsigned char *body=NULL;size_t bytes=0;AcContext source;AcRow selected={0};const AcRow *row=&selected;AcAlternative *alternatives=NULL;
    int read_rc=aw_native(args,NULL,0,&body,&bytes),ok=0;long long version;bf_mp_reader_t reader;bf_mp_value_t item;
    if(read_rc){free(body);aw_error(read_rc==4?409:503,"The current saved record could not be confirmed.\n");return 1;}
    bf_mp_reader_init(&reader,body,bytes);
    if(!ac_array(&reader,4) || !ac_uint(&reader,&version) || version!=1 || !ac_context_read(&reader,&source) ||
        strcmp(source.id,id) || strcmp(source.subject,subject) || strcmp(source.scope,scope) || strcmp(source.release,release) || strcmp(source.pack,pack) ||
        !ac_array(&reader,3) || !ac_text(&reader,selected.key,sizeof(selected.key)) || strcmp(selected.key,key) || !ac_uint(&reader,&selected.revision) ||
        bf_mp_read_next(&reader,&item)!=BF_MP_BIN || item.v.raw.len>256u*1024u)goto unavailable;
    selected.body=item.v.raw.ptr;selected.bytes=item.v.raw.len;
    if(row->revision!=strtoll(revision,NULL,10)){free(body);aw_error(409,"Another saved version is now current. Refresh before choosing a view.\n");return 1;}
    if(bf_mp_read_next(&reader,&item)!=BF_MP_ARRAY || item.v.count>64)goto unavailable;
    uint32_t candidates=item.v.count;size_t count=0;char digest[65],title[1024]="",previous[64]="";
    bf_sha256_hex(row->body,row->bytes,digest);alternatives=calloc(64,sizeof(*alternatives));if(!alternatives)goto unavailable;
    for(uint32_t n=0;n<candidates;n++){
        AcContext destination;char label[256];
        if(!ac_array(&reader,2) || !ac_context_read(&reader,&destination) || !ac_text(&reader,label,sizeof(label)) ||
            strcmp(destination.subject,subject) || strcmp(destination.scope,scope) || strcmp(destination.records,source.records) || strcmp(destination.family,source.family) ||
            (n && strcmp(previous,destination.id)>=0))goto unavailable;
        snprintf(previous,sizeof(previous),"%s",destination.id);
        AcView views[AC_VIEWS];size_t vcount=0;if(!ac_profiles(destination.release,views,&vcount))continue;
        for(size_t v=0;v<vcount;v++){
            char url[AC_URL];if(!ac_destination(&views[v],row,destination.id,url,sizeof(url)))continue;
            if(count==64 || !ac_alternative_url(&alternatives[count],&destination,&views[v],row,digest))goto unavailable;
            if(!*title)ac_value(row,views[v].title,title,sizeof(title));
            snprintf(alternatives[count].label,sizeof(alternatives[count].label),"%s",label);count++;
        }
    }
    if(reader.pos!=reader.len)goto unavailable;
    if(machine){
        bf_mp_writer_t out;bf_mp_writer_init(&out,2048);
        ok=!bf_mp_pack_array(&out,9) && !bf_mp_pack_uint(&out,1) && !bf_mp_pack_str_cstr(&out,source.id) && !bf_mp_pack_str_cstr(&out,subject) && !bf_mp_pack_str_cstr(&out,scope) && !bf_mp_pack_str_cstr(&out,source.records) && !bf_mp_pack_str_cstr(&out,key) && !bf_mp_pack_uint(&out,(uint64_t)row->revision) && !bf_mp_pack_str_cstr(&out,digest) && !bf_mp_pack_array(&out,(uint32_t)count);
        for(size_t n=0;n<count && ok;n++){
            AcAlternative *a=&alternatives[n];const char *fields[]={a->id,a->pack,a->label,a->view,a->release,a->url};ok=!bf_mp_pack_array(&out,6);
            for(size_t k=0;k<6 && ok;k++)ok=!bf_mp_pack_str_cstr(&out,fields[k]);
        }
        if(ok && out.len<=AW_PACKET)aw_response(200,"application/msgpack",out.buf,out.len,0);else aw_error(503,"The continuation projection exceeded its bound.\n");bf_mp_writer_free(&out);
    }else{
        psetup_start(200,"Continue this work");size_t nav_size=0;unsigned char *nav=ac_navigation(id,context,pack,&nav_size);if(nav){fwrite(nav,1,nav_size,stdout);free(nav);}
        fputs("<h1>",stdout);html(*title?title:"Continue this work");printf("</h1><p>Saved version %lld. These views open the same record in this destination. Unsaved edits stay in your original tab.</p><div class=\"pack-cards\">",row->revision);
        for(size_t n=0;n<count;n++){AcAlternative *a=&alternatives[n];fputs("<article class=\"pack-panel\"><h2>",stdout);html(a->label);fputs("</h2><p>",stdout);html(a->view);fputs("</p><a data-compatible-view href=\"",stdout);html(a->url);fputs("\">Open this view</a></article>",stdout);}
        fputs("</div>",stdout);if(!count)fputs("<p>No active editor currently declares a compatible view. The saved record has not changed.</p>",stdout);psetup_end();
    }
    free(alternatives);free(body);return 1;
 unavailable:free(alternatives);free(body);aw_error(503,"The compatible views could not be resolved completely. Nothing was changed.\n");return 1;
}
#endif
