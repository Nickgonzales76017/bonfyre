#ifndef BONFYRE_A2A_RETAINED_MATERIAL_H
#define BONFYRE_A2A_RETAINED_MATERIAL_H
#include <stdarg.h>
/* Public, read-only, typed projection of the SAME retained material used by
 * /o/<source>/<24-hex-key>. Not a feed fetch, foreign-twin admission or write.
 * The CMS publication gate and withheld-twin check are the existing ones
 * (pr_source). Private and unreleased sources cannot cross this aperture. */
#define A2A_PUBLIC_PAGE 10
#define A2A_PUBLIC_SCAN 64
typedef struct {
    char *out;
    size_t cap, used;
    int count, overflow;
} A2APublicResults;
static int a2a_public_append(A2APublicResults *p,const char *fmt,...){
    if(p->overflow||p->used>=p->cap){p->overflow=1;return 0;}
    va_list ap;va_start(ap,fmt);
    int wrote=vsnprintf(p->out+p->used,p->cap-p->used,fmt,ap);
    va_end(ap);
    if(wrote<0||(size_t)wrote>=p->cap-p->used){p->overflow=1;return 0;}
    p->used+=(size_t)wrote;return 1;
}
static int a2a_public_error(char *out,size_t outsz,const char *reason){
    snprintf(out,outsz,"{\"error\":\"%s\"}",reason);return 0;
}
static int a2a_public_source(const char *body,char src[97]){
    if(!studio_json_string(body,"source",src,97))return 0;
    size_t n=strlen(src);
    return n>0&&n<=96&&strspn(src,"abcdefghijklmnopqrstuvwxyz0123456789-_.")==n;
}
static int a2a_public_cursor(const char *body,int *cursor){
    char number[16]={0};*cursor=0;
    if(!studio_json_string(body,"cursor",number,sizeof(number)))return 1;
    if(!number[0]||strlen(number)>6||strspn(number,"0123456789")!=strlen(number))return 0;
    long n=strtol(number,NULL,10);
    if(n<0||n>100000)return 0;
    *cursor=(int)n;return 1;
}
static int a2a_public_released(sqlite3 *cms,const char *src){
    char twin[256];return cms&&pr_source(cms,src,twin,sizeof(twin));
}
/* List only published, front-door, observed RSS sources whose twins are not
 * withheld, and return the scanned-candidate offset as the next cursor. */
static int a2a_exec_retained_sources(const char *body,char *out,size_t outsz){
    sqlite3 *cms=cms_open_ro();sqlite3_stmt *q=NULL;
    if(!cms)return a2a_public_error(out,outsz,"cms_unavailable");
    int cursor=0;
    if(!a2a_public_cursor(body,&cursor)){
        sqlite3_close(cms);return a2a_public_error(out,outsz,"invalid_cursor");
    }
    const char *sql=
        "SELECT DISTINCT source,family,kind FROM internet_source "
        "WHERE namespace='root' AND status='published' AND front_door='yes' "
        "AND state='observed' AND extract LIKE 'rss:%' "
        "ORDER BY source LIMIT ?1 OFFSET ?2";
    if(sqlite3_prepare_v2(cms,sql,-1,&q,NULL)!=SQLITE_OK){
        sqlite3_close(cms);return a2a_public_error(out,outsz,"source_query_failed");
    }
    sqlite3_bind_int(q,1,A2A_PUBLIC_SCAN);
    sqlite3_bind_int(q,2,cursor);
    A2APublicResults p={out,outsz,0,0,0};
    int scanned=0,step=SQLITE_DONE;
    a2a_public_append(&p,"{\"sources\":[");
    while((step=sqlite3_step(q))==SQLITE_ROW){
        scanned++;
        const char *src=(const char*)sqlite3_column_text(q,0);
        if(!src||!a2a_public_released(cms,src))continue;
        char es[512],ef[512],ek[512];
        json_escape_into(src,es,sizeof(es));
        json_escape_into((const char*)sqlite3_column_text(q,1),ef,sizeof(ef));
        json_escape_into((const char*)sqlite3_column_text(q,2),ek,sizeof(ek));
        if(!a2a_public_append(&p,"%s{\"source\":\"%s\",\"family\":\"%s\",\"kind\":\"%s\"}",
                 p.count?",":"",es,ef,ek))break;
        p.count++;
        if(p.count==A2A_PUBLIC_PAGE)break;
    }
    sqlite3_finalize(q);sqlite3_close(cms);
    if(step!=SQLITE_ROW&&step!=SQLITE_DONE)return a2a_public_error(out,outsz,"source_query_failed");
    if(p.overflow)return a2a_public_error(out,outsz,"result_overflow");
    if(!a2a_public_append(&p,"],\"count\":%d,\"next_cursor\":\"%d\",\"mode\":\"released-only\"}",
                          p.count,cursor+scanned))return a2a_public_error(out,outsz,"result_overflow");
    return 1;
}
/* One published source, up to ten recent retained links. No expensive archive
 * sweep, no remote request and no unbounded text search: this uses item_by_source. */
static int a2a_exec_retained_latest(const char *body,char *out,size_t outsz){
    char source[97];
    if(!a2a_public_source(body,source))return a2a_public_error(out,outsz,"invalid_source");
    int cursor=0;
    if(!a2a_public_cursor(body,&cursor))return a2a_public_error(out,outsz,"invalid_cursor");
    sqlite3 *cms=cms_open_ro();
    if(!a2a_public_released(cms,source)){
        if(cms)sqlite3_close(cms);return a2a_public_error(out,outsz,"source_not_public");
    }
    sqlite3_close(cms);
    sqlite3 *archive=page_archive_open(0);sqlite3_stmt *q=NULL;
    if(!archive)return a2a_public_error(out,outsz,"archive_unavailable");
    const char *sql=
        "SELECT key,title,href,at FROM item WHERE source=?1 "
        "ORDER BY at DESC,key LIMIT ?2 OFFSET ?3";
    if(sqlite3_prepare_v2(archive,sql,-1,&q,NULL)!=SQLITE_OK){
        sqlite3_close(archive);return a2a_public_error(out,outsz,"archive_query_failed");
    }
    sqlite3_bind_text(q,1,source,-1,SQLITE_STATIC);
    sqlite3_bind_int(q,2,A2A_PUBLIC_PAGE);
    sqlite3_bind_int(q,3,cursor);
    A2APublicResults p={out,outsz,0,0,0};
    char es[512];json_escape_into(source,es,sizeof(es));
    a2a_public_append(&p,"{\"source\":\"%s\",\"items\":[",es);
    int step=SQLITE_DONE,scanned=0;
    while((step=sqlite3_step(q))==SQLITE_ROW){
        scanned++; /* Cursor consumes source rows, including unusable legacy keys. */
        const char *key=(const char*)sqlite3_column_text(q,0);
        if(!key||strlen(key)!=24||strspn(key,"0123456789abcdef")!=24)continue;
        char ek[128],et[3072],eh[4096];
        json_escape_into(key,ek,sizeof(ek));
        json_escape_into((const char*)sqlite3_column_text(q,1),et,sizeof(et));
        json_escape_into((const char*)sqlite3_column_text(q,2),eh,sizeof(eh));
        if(!a2a_public_append(&p,"%s{\"key\":\"%s\",\"title\":\"%s\",\"href\":\"%s\",\"at\":%lld,\"publicPath\":\"/o/%s/%s\",\"continuePath\":\"/feed-use?context=self&item=%s%%2F%s\"}",
              p.count?",":"",ek,et,eh,(long long)sqlite3_column_int64(q,3),es,ek,es,ek))break;
        p.count++;
    }
    sqlite3_finalize(q);sqlite3_close(archive);
    if(step!=SQLITE_ROW&&step!=SQLITE_DONE)return a2a_public_error(out,outsz,"archive_query_failed");
    if(p.overflow)return a2a_public_error(out,outsz,"result_overflow");
    if(!a2a_public_append(&p,"],\"count\":%d,\"next_cursor\":\"%d\",\"mode\":\"retained-public\"}",p.count,cursor+scanned))
        return a2a_public_error(out,outsz,"result_overflow");
    return 1;
}
/* Exact archived item, including a possibly empty original excerpt. Never
 * infer missing text, reuse rights, identity matches or live freshness. */
static int a2a_exec_retained_item(const char *body,char *out,size_t outsz){
    char source[97],key[25]={0};
    if(!a2a_public_source(body,source)||!studio_json_string(body,"key",key,sizeof(key))||
       strlen(key)!=24||strspn(key,"0123456789abcdef")!=24)
        return a2a_public_error(out,outsz,"invalid_item_ref");
    sqlite3 *cms=cms_open_ro();
    if(!a2a_public_released(cms,source)){
        if(cms)sqlite3_close(cms);return a2a_public_error(out,outsz,"source_not_public");
    }
    sqlite3_close(cms);
    sqlite3 *archive=page_archive_open(0);
    if(!archive)return a2a_public_error(out,outsz,"archive_unavailable");
    PageItem *item=calloc(1,sizeof(*item));
    if(!item){sqlite3_close(archive);return a2a_public_error(out,outsz,"allocation_failed");}
    char family[64]={0};
    int found=page_archive_item(archive,source,key,item,family,sizeof(family));
    sqlite3_close(archive);
    if(!found){free(item);return a2a_public_error(out,outsz,"item_not_found");}
    char es[512],ek[128],et[3072],eh[4096],eb[12288],ef[512],eauthor[1024];
    json_escape_into(source,es,sizeof(es));json_escape_into(key,ek,sizeof(ek));
    json_escape_into(item->title,et,sizeof(et));
    json_escape_into(item->href,eh,sizeof(eh));
    json_escape_into(item->body,eb,sizeof(eb));
    json_escape_into(family,ef,sizeof(ef));json_escape_into(item->by,eauthor,sizeof(eauthor));
    int written=snprintf(out,outsz,"{\"source\":\"%s\",\"key\":\"%s\",\"family\":\"%s\","
        "\"title\":\"%s\",\"href\":\"%s\",\"excerpt\":\"%s\",\"author\":\"%s\","
        "\"published_at\":%lld,\"publicPath\":\"/o/%s/%s\",\"continuePath\":\"/feed-use?context=self&item=%s%%2F%s\","
        "\"basis\":\"retained public excerpt; no inferred reuse rights or live freshness\"}",
        es,ek,ef,et,eh,eb,eauthor,(long long)item->at,es,ek,es,ek);
    free(item);
    if(written<0||(size_t)written>=outsz)return a2a_public_error(out,outsz,"result_overflow");
    return 1;
}
#endif
