#ifndef BONFYRE_APPLICATION_CONTEXT_H
#define BONFYRE_APPLICATION_CONTEXT_H
/* Ephemeral projection of the existing CMS binding. No new subject or store. */
typedef struct {
    char id[64],subject[160],scope[24],pack[96],release[65],entry[128],family[64],records[80];
} AcContext;
static int ac_context_read(bf_mp_reader_t *reader,AcContext *out) {
    long long version;memset(out,0,sizeof(*out));
    if(!ac_array(reader,9) || !ac_uint(reader,&version) || version!=1)return 0;
#define AC_CONTEXT_FIELD(field) if(!ac_text(reader,out->field,sizeof(out->field)))return 0
    AC_CONTEXT_FIELD(id);AC_CONTEXT_FIELD(subject);AC_CONTEXT_FIELD(scope);AC_CONTEXT_FIELD(pack);
    AC_CONTEXT_FIELD(release);AC_CONTEXT_FIELD(entry);AC_CONTEXT_FIELD(family);AC_CONTEXT_FIELD(records);
#undef AC_CONTEXT_FIELD
    return strlen(out->id)==43 && !strncmp(out->id,"ws-",3) && aw_hex(out->id+3,40) && aw_hex(out->release,64) &&
        bfpg_token(out->subject,sizeof(out->subject)) && bfpg_token(out->pack,sizeof(out->pack)) &&
        (!strcmp(out->scope,"self") || !strcmp(out->scope,"institution") || !strcmp(out->scope,"household")) &&
        aw_path(out->entry) && ac_word(out->family) && ac_word(out->records);
}
static int ac_context(const char *id,const char *subject,const char *scope,const char *actor,AcContext *out) {
    const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*db=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
    char *const args[]={(char *)bin,"pack-workspace","--operation","context","--binding",(char *)id,"--subject",(char *)subject,"--scope",(char *)scope,"--actor",(char *)actor,"--db",(char *)db,NULL};
    unsigned char *body=NULL;size_t bytes=0;int rc=aw_native(args,NULL,0,&body,&bytes),ok=0;
    if(!rc && bytes<=2048){bf_mp_reader_t reader;bf_mp_reader_init(&reader,body,bytes);
        ok=ac_context_read(&reader,out) && reader.pos==reader.len && !strcmp(out->id,id) && !strcmp(out->subject,subject) && !strcmp(out->scope,scope);}
    free(body);if(!ok)memset(out,0,sizeof(*out));return ok;
}
#endif
