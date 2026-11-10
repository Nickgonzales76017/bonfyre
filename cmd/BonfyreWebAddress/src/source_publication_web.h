#ifndef BONFYRE_SOURCE_PUBLICATION_WEB_H
#define BONFYRE_SOURCE_PUBLICATION_WEB_H
/* Existing CMS authority and native binary transport; no browser-owned writes. */
static int serve_source_publication(const char *method,const char *target,const char *query,const char *body,const char *raw,const char *bearer,const char *auth_db,const char *host) {
    if(strcmp(target,"/cms/source-publication"))return 0;
    int writing=!strcmp(method,"POST");char ns[192]={0},request[65]={0},origin[300]={0},content[80]={0},length[32]={0};
    if((!writing&&strcmp(method,"GET"))||(writing&&query&&*query)){aw_error(400,"Use the exact source-publication address.\n");return 1;}
    if(!session_valid(bearer,auth_db)||!session_authorized(bearer,auth_db,"write")||!cms_namespace_for(bearer,auth_db,ns,sizeof(ns))||strlen(ns)>127){aw_error(403,"A current CMS write grant is required for this publication.\n");return 1;}
    const char *bin=getenv("BONFYRE_CMS_BINARY"),*db=getenv("BONFYRE_CMS_DB"),*schemas=getenv("BONFYRE_CMS_SCHEMAS");
    if(!bin||!*bin||!db||!*db||!schemas||!*schemas){aw_error(503,"The native source-publication owner is unavailable.\n");return 1;}
    size_t bytes=0;bf_mp_writer_t w;bf_mp_writer_init(&w,96);int valid=1;
    if(writing){
        char http[300],https[300];snprintf(http,sizeof(http),"http://%s",host?host:"");snprintf(https,sizeof(https),"https://%s",host?host:"");
        if(!aw_header(raw,"Origin",origin,sizeof(origin))||(strcmp(origin,http)&&strcmp(origin,https))){bf_mp_writer_free(&w);aw_error(403,"Publish from the current workspace origin.\n");return 1;}
        if(!aw_header(raw,"Content-Type",content,sizeof(content))||strcmp(content,"application/msgpack")||!aw_header(raw,"Content-Length",length,sizeof(length))||!*length||strlen(length)>5||strspn(length,"0123456789")!=strlen(length)||!body){valid=0;}
        else{bytes=(size_t)strtoul(length,NULL,10);valid=bytes>0&&bytes<=16384;}
    }else{
        if(!query||strlen(query)!=72||strncmp(query,"request=",8)||strlen(query+8)!=64||strspn(query+8,"0123456789abcdef")!=64)valid=0;
        else{memcpy(request,query+8,65);valid=!bf_mp_pack_array(&w,2)&&!bf_mp_pack_uint(&w,1)&&!bf_mp_pack_str_cstr(&w,request);body=(const char*)w.buf;bytes=w.len;}
    }
    if(!valid){bf_mp_writer_free(&w);aw_error(400,"Provide one bounded native publication request.\n");return 1;}
    char *argv[]={(char*)bin,"entry","source-publication","--ns",ns,"--operation",writing?"publish":"read","--db",(char*)db,"--schemas",(char*)schemas,NULL};
    unsigned char *output=NULL;size_t size=0;int rc=aw_native_result(argv,body,bytes,&output,&size,1);bf_mp_writer_free(&w);
    bf_mp_reader_t r;bf_mp_value_t v;bf_mp_reader_init(&r,output,size);char state[32]={0};
    valid=output&&size&&size<=16384&&bf_mp_read_next(&r,&v)==BF_MP_ARRAY&&v.v.count==9;
    if(valid){bf_mp_type_t t=bf_mp_read_next(&r,&v);valid=(t==BF_MP_UINT&&v.v.u==1)||(t==BF_MP_INT&&v.v.i==1);}
    if(valid){valid=bf_mp_read_next(&r,&v)==BF_MP_STR&&v.v.raw.len<sizeof(state)&&!memchr(v.v.raw.ptr,0,v.v.raw.len);if(valid){memcpy(state,v.v.raw.ptr,v.v.raw.len);state[v.v.raw.len]=0;}}
    for(int i=0;i<7&&valid;i++)valid=bf_mp_skip(&r)==0;
    valid=valid&&r.pos==r.len;
    if(valid&&(rc==0||rc==2)&&strcmp(state,"committed")&&strcmp(state,"absent"))valid=0;
    if(valid&&rc==2&&strcmp(state,"committed"))valid=0;
    if(valid){int status=(rc==0||rc==2)?200:rc==3?400:rc==4?409:503;aw_response(status,"application/msgpack",output,size,0);}
    else aw_error(503,"The native publication outcome is uncertain. Read the same request before retrying.\n");
    free(output);return 1;
}
#endif
