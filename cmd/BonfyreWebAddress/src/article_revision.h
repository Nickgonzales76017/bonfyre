/* Source revision identity shared by article narration and CMS. Identity is
 * not permission to publish or evidence that a recording speaks the source. */
#ifndef BONFYRE_ARTICLE_REVISION_H
#define BONFYRE_ARTICLE_REVISION_H
#include "bonfyre.h"
#include <sqlite3.h>
#include <stdint.h>
#include <ctype.h>
#include <string.h>
#include <stdio.h>
static inline int bf_narration_ref(const char *s){
    if(!s||strlen(s)<6||strlen(s)>190)return 0;
    for(const unsigned char *p=(const unsigned char*)s;*p;p++)
        if(!isalnum(*p)&&*p!=':'&&*p!='-'&&*p!='_'&&*p!='.')return 0;
    return 1;
}
static inline int bf_narration_article_revision(sqlite3 *db,const char *ns,
        const char *subject,const char *version,char out[65]){
    sqlite3_stmt *st=NULL;BfSha256 h;uint8_t digest[32];char route[256];
    int ok=0,step,count=0;size_t total=0;
    if(out)out[0]=0;
    if(!db||!out||!ns||!*ns||strlen(ns)>96||!bf_narration_ref(subject)||
       strncmp(subject,"article:",8)||!version||
       (strcmp(version,"draft")&&strcmp(version,"published")))return 0;
    if(sqlite3_exec(db,"SAVEPOINT bf_article_revision",NULL,NULL,NULL)!=SQLITE_OK)return 0;
    const char *page="SELECT route,json_array(title,heading,lede,editorial_kind) FROM site_page "
        "WHERE namespace=?1 AND media_subject_ref=?2 AND status=?3 AND "
        "editorial_kind IN ('article','essay','report','feature','longform') LIMIT 2";
    if(sqlite3_prepare_v2(db,page,-1,&st,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(st,1,ns,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,subject,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,version,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)!=SQLITE_ROW)goto done;
    const char *r=(const char*)sqlite3_column_text(st,0),*fields=(const char*)sqlite3_column_text(st,1);
    int bytes=sqlite3_column_bytes(st,1);
    if(!r||r[0]!='/'||strlen(r)>=sizeof(route)||!fields||bytes<0||bytes>1048576)goto done;
    snprintf(route,sizeof(route),"%s",r);bf_sha256_init(&h);
    const char *domain="bonfyre.article-revision.v2";
    bf_sha256_update(&h,(const uint8_t*)domain,strlen(domain)+1);
    bf_sha256_update(&h,(const uint8_t*)ns,strlen(ns)+1);
    bf_sha256_update(&h,(const uint8_t*)subject,strlen(subject)+1);
    bf_sha256_update(&h,(const uint8_t*)route,strlen(route)+1);
    bf_sha256_update(&h,(const uint8_t*)fields,(size_t)bytes+1);total=(size_t)bytes;
    if(sqlite3_step(st)!=SQLITE_DONE)goto done;
    sqlite3_finalize(st);st=NULL;
    const char *fragments="SELECT json_array(coordinate,slot,kind,position,heading,body,semantic_tags) "
        "FROM site_fragment WHERE namespace=?1 AND page=?2 AND status=?3 AND audience='public' "
        "ORDER BY position,coordinate,id LIMIT 513";
    if(sqlite3_prepare_v2(db,fragments,-1,&st,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(st,1,ns,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,route,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,version,-1,SQLITE_TRANSIENT);
    while((step=sqlite3_step(st))==SQLITE_ROW){
        const char *row=(const char*)sqlite3_column_text(st,0);bytes=sqlite3_column_bytes(st,0);
        if(++count>512||!row||bytes<0||(size_t)bytes>1048576-total)goto done;
        total+=(size_t)bytes;bf_sha256_update(&h,(const uint8_t*)row,(size_t)bytes+1);
    }
    if(step!=SQLITE_DONE||!count)goto done;
    bf_sha256_final(&h,digest);bf_sha256_digest_hex(digest,out);ok=1;
 done:
    sqlite3_finalize(st);
    if(sqlite3_exec(db,"RELEASE bf_article_revision",NULL,NULL,NULL)!=SQLITE_OK)ok=0;
    if(!ok)out[0]=0;return ok;
}
#endif
