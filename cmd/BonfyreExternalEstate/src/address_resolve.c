#define _POSIX_C_SOURCE 200809L
#include "address_resolve.h"
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
typedef struct { char axis[48], term[160]; } Term;
typedef struct { int word; sqlite3_int64 mask; } Word;
typedef struct { char *v[7]; char **dependencies; size_t count; } OwnedCandidate;
static int token(const char *p) {
    if(!p||!*p)return 0;
    for(;*p;p++)if(!((*p>='a'&&*p<='z')||(*p>='0'&&*p<='9')||*p=='_'||*p=='-'))return 0;
    return 1;
}
static int term(const char *raw,Term *out) {
    const char *colon=raw?strchr(raw,':'):NULL;
    if(!colon)return 1;
    size_t a=(size_t)(colon-raw),n=strlen(colon+1);
    if(!a||a>=sizeof(out->axis)||!n||n>=sizeof(out->term))return 1;
    memcpy(out->axis,raw,a);out->axis[a]=0;memcpy(out->term,colon+1,n+1);
    for(size_t i=0;i<a;i++)out->axis[i]=(char)tolower((unsigned char)out->axis[i]);
    for(size_t i=0;i<n;i++)out->term[i]=(char)tolower((unsigned char)out->term[i]);
    return !token(out->axis)||!token(out->term);
}
int bf_address_open_index(sqlite3 **db,FILE *err) {
    char path[4096];const char *p=getenv("BONFYRE_ADDRESS_INDEX");
    if(!p||!*p)p=getenv("BONFYRE_EXTERNAL_ESTATE_DB");
    if(!p||!*p){const char *home=getenv("HOME");
        if(!home||snprintf(path,sizeof(path),"%s/.bonfyre/estate/estate.db",home)>=(int)sizeof(path))return 1;
        p=path;
    }
    *db=NULL;
    if(sqlite3_open_v2(p,db,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK)return 0;
    if(err)fprintf(err,"AddressPlane index unavailable: %s\n",*db?sqlite3_errmsg(*db):"open failed");
    if(*db)sqlite3_close(*db);
    *db=NULL;return 1;
}
int bf_address_resolve(sqlite3 *db,const BfAddressQuery *q,BfAddressVisitor visit,
                       void *context,BfAddressResolution *result,FILE *err) {
    Word words[BF_ADDRESS_TERMS];Term forbidden[BF_ADDRESS_TERMS];
    sqlite3_stmt *lookup=NULL,*rows=NULL,*dependencies=NULL;sqlite3_str *builder=NULL;char *query=NULL;
    OwnedCandidate *values=NULL;int wc=0,n=0,rc=1,step,started=0;
    if(result)memset(result,0,sizeof(*result));
    if(!db||!q||!result||!visit||!token(q->kind)||q->limit<1||q->limit>256||
       q->need_count<0||q->need_count>BF_ADDRESS_TERMS||q->forbid_count<0||q->forbid_count>BF_ADDRESS_TERMS)return 1;
    for(int i=0;i<q->forbid_count;i++)if(term(q->forbid[i],&forbidden[i]))return 1;
    if(sqlite3_get_autocommit(db)){if(sqlite3_exec(db,"BEGIN",NULL,NULL,NULL)!=SQLITE_OK)goto done;started=1;}
    if(sqlite3_prepare_v2(db,"SELECT count(*) FROM candidate_advertisement WHERE candidate_kind=?",-1,&lookup,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(lookup,1,q->kind,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(lookup)!=SQLITE_ROW)goto done;
    result->considered=sqlite3_column_int64(lookup,0);sqlite3_finalize(lookup);lookup=NULL;
    for(int i=0;i<q->need_count;i++){
        Term t;if(term(q->need[i],&t))goto done;
        if(sqlite3_prepare_v2(db,"SELECT word,bit FROM address_bit WHERE axis=? AND term=?",-1,&lookup,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(lookup,1,t.axis,-1,SQLITE_TRANSIENT);sqlite3_bind_text(lookup,2,t.term,-1,SQLITE_TRANSIENT);
        step=sqlite3_step(lookup);
        if(step==SQLITE_DONE){result->unknown_coordinate=1;sqlite3_finalize(lookup);lookup=NULL;rc=0;goto done;}
        if(step!=SQLITE_ROW)goto done;
        int word=sqlite3_column_int(lookup,0),bit=sqlite3_column_int(lookup,1),at=-1;
        if(word<0||bit<0||bit>=62)goto done;
        sqlite3_finalize(lookup);lookup=NULL;
        for(int j=0;j<wc;j++)if(words[j].word==word)at=j;
        if(at<0){at=wc++;words[at]=(Word){word,0};}
        words[at].mask|=(sqlite3_int64)1<<bit;
    }
    result->mask_words=wc;
    builder=sqlite3_str_new(db);if(!builder)goto done;
    sqlite3_str_appendall(builder,"SELECT a.identity,a.owner_ref,a.exact_verifier,a.lowerer_ref,a.validity_frontier,coalesce(o.observed_name,a.identity),coalesce(o.primary_url,''),count(*) OVER () FROM candidate_advertisement a LEFT JOIN occurrence o ON o.id=a.identity WHERE a.candidate_kind=?");
    for(int i=0;i<wc;i++)sqlite3_str_appendall(builder," AND EXISTS(SELECT 1 FROM candidate_advertisement_mask m WHERE m.identity=a.identity AND m.word=? AND (m.provides & ?)=?)");
    for(int i=0;i<q->forbid_count;i++)sqlite3_str_appendall(builder," AND NOT EXISTS(SELECT 1 FROM candidate_advertisement_term t WHERE t.identity=a.identity AND t.axis=? AND t.term=?)");
    sqlite3_str_appendall(builder," ORDER BY a.identity LIMIT ?");query=sqlite3_str_finish(builder);builder=NULL;
    if(!query||sqlite3_prepare_v2(db,query,-1,&rows,NULL)!=SQLITE_OK)goto done;
    int bind=1;sqlite3_bind_text(rows,bind++,q->kind,-1,SQLITE_TRANSIENT);
    for(int i=0;i<wc;i++){sqlite3_bind_int(rows,bind++,words[i].word);sqlite3_bind_int64(rows,bind++,words[i].mask);sqlite3_bind_int64(rows,bind++,words[i].mask);}
    for(int i=0;i<q->forbid_count;i++){sqlite3_bind_text(rows,bind++,forbidden[i].axis,-1,SQLITE_TRANSIENT);sqlite3_bind_text(rows,bind++,forbidden[i].term,-1,SQLITE_TRANSIENT);}
    sqlite3_bind_int(rows,bind,q->limit);values=calloc((size_t)q->limit,sizeof(*values));if(!values)goto done;
    while((step=sqlite3_step(rows))==SQLITE_ROW){
        if(n>=q->limit)goto done;
        OwnedCandidate *v=&values[n++];
        for(int j=0;j<7;j++){
            const char *s=(const char*)sqlite3_column_text(rows,j);int len=sqlite3_column_bytes(rows,j);
            if(!s||len<0||len>8192||(size_t)len!=strlen(s)||(j<4&&!len))goto done;
            v->v[j]=strdup(s);if(!v->v[j])goto done;
        }
        result->eligible=sqlite3_column_int64(rows,7);
    }
    if(step!=SQLITE_DONE)goto done;
    sqlite3_finalize(rows);rows=NULL;
    if(sqlite3_prepare_v2(db,"SELECT dependency_ref FROM candidate_advertisement_dependency WHERE identity=? ORDER BY dependency_ref",-1,&dependencies,NULL)!=SQLITE_OK)goto done;
    for(int i=0;i<n;i++){
        OwnedCandidate *v=&values[i];sqlite3_reset(dependencies);sqlite3_clear_bindings(dependencies);
        sqlite3_bind_text(dependencies,1,v->v[0],-1,SQLITE_STATIC);
        while((step=sqlite3_step(dependencies))==SQLITE_ROW){
            const char *ref=(const char*)sqlite3_column_text(dependencies,0);int len=sqlite3_column_bytes(dependencies,0);
            if(!ref||!len||len>8192||strlen(ref)!=(size_t)len||v->count>=16384)goto done;
            char **grown=realloc(v->dependencies,(v->count+1)*sizeof(*grown));if(!grown)goto done;
            v->dependencies=grown;v->dependencies[v->count]=strdup(ref);if(!v->dependencies[v->count])goto done;v->count++;
        }
        if(step!=SQLITE_DONE)goto done;
    }
    sqlite3_finalize(dependencies);dependencies=NULL;
    if(started){if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK)goto done;started=0;}
    for(int i=0;i<n;i++){
        OwnedCandidate *v=&values[i];BfAddressCandidate c={v->v[0],v->v[1],v->v[2],v->v[3],v->v[4],v->v[5],v->v[6],(const char *const *)v->dependencies,v->count};
        if(visit(context,&c))goto done;
        result->returned++;
    }
    result->has_more=result->eligible>result->returned;rc=0;
done:
    if(rc&&err)fprintf(err,"AddressPlane resolution failed: %s\n",sqlite3_errmsg(db));
    sqlite3_finalize(lookup);sqlite3_finalize(rows);sqlite3_finalize(dependencies);sqlite3_free(query);
    if(builder)sqlite3_free(sqlite3_str_finish(builder));
    if(started)sqlite3_exec(db,rc?"ROLLBACK":"COMMIT",NULL,NULL,NULL);
    if(values){for(int i=0;i<n;i++){for(int j=0;j<7;j++)free(values[i].v[j]);for(size_t j=0;j<values[i].count;j++)free(values[i].dependencies[j]);free(values[i].dependencies);}free(values);}
    return rc;
}
int bf_address_resolve_path(const char *path,const BfAddressQuery *q,BfAddressVisitor visit,
                            void *context,BfAddressResolution *result,FILE *err) {
    sqlite3 *db=NULL;int rc;
    if(!path){if(bf_address_open_index(&db,err))return 1;}
    else if(sqlite3_open_v2(path,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){if(db)sqlite3_close(db);return 1;}
    rc=bf_address_resolve(db,q,visit,context,result,err);sqlite3_close(db);return rc;
}
