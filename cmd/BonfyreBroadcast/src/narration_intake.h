/* Canonical native narration intake for locally authored content.
 * Broadcast owns script identity; WebAddress rechecks the same source digest.
 * Neither the source registry nor an audio take grants publication or rights.
 */
#ifndef BF_BROADCAST_NARRATION_INTAKE_H
#define BF_BROADCAST_NARRATION_INTAKE_H
#include <bonfyre.h>
#include <sqlite3.h>
#include <ctype.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#define BF_NARRATION_TEXT_MAX 65536
typedef struct {
    char title[256],route[256],kind[20],sha[65];
    char text[BF_NARRATION_TEXT_MAX];
    size_t length;
} BfNarrationSource;
static inline int bf_narration_ref(const char *s){
    if(!s||strlen(s)<6||strlen(s)>190)return 0;
    for(const unsigned char *p=(const unsigned char*)s;*p;p++)
        if(!isalnum(*p)&&*p!=':'&&*p!='-'&&*p!='_'&&*p!='.')return 0;
    return 1;
}
static inline int bf_narration_add(BfNarrationSource *s,const char *text){
    if(!text||!*text)return 1;
    size_t n=strlen(text);
    if(n>sizeof(s->text)-s->length-3)return 0;
    if(s->length){memcpy(s->text+s->length,"\n\n",2);s->length+=2;}
    memcpy(s->text+s->length,text,n);s->length+=n;s->text[s->length]=0;
    return 1;
}
/* Deterministic script and fingerprint of the original publication revision.
 * A title-only placeholder, duplicated subject or non-public item refuses. */
static inline int bf_narration_source(sqlite3 *db,const char *ns,
                                      const char *subject,BfNarrationSource *out){
    sqlite3_stmt *st=NULL;int found=0,ok=0;size_t authored_body=0;
    if(!db||!ns||!*ns||strlen(ns)>96||!bf_narration_ref(subject)||!out)return 0;
    memset(out,0,sizeof(*out));
    if(!strncmp(subject,"article:",8)){
        const char *query="SELECT COALESCE(heading,title),COALESCE(lede,''),route "
          "FROM site_page WHERE namespace=?1 AND media_subject_ref=?2 "
          "AND status='published' AND editorial_kind IN "
          "('article','essay','report','feature','longform') ORDER BY id DESC LIMIT 2";
        if(sqlite3_prepare_v2(db,query,-1,&st,NULL)!=SQLITE_OK)return 0;
        sqlite3_bind_text(st,1,ns,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,subject,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(st)==SQLITE_ROW){
            const char *title=(const char*)sqlite3_column_text(st,0),
                       *lede=(const char*)sqlite3_column_text(st,1),
                       *route=(const char*)sqlite3_column_text(st,2);
            if(!title||!route||strlen(title)>=sizeof(out->title)||
               strlen(route)>=sizeof(out->route))goto done;
            snprintf(out->title,sizeof(out->title),"%s",title);
            snprintf(out->route,sizeof(out->route),"%s",route);
            snprintf(out->kind,sizeof(out->kind),"article");
            if(!bf_narration_add(out,title)||!bf_narration_add(out,lede))goto done;
            found=1;
        }
        if(!found||sqlite3_step(st)==SQLITE_ROW)goto done;
        sqlite3_finalize(st);st=NULL;
        const char *fragments=
          "SELECT COALESCE(heading,''),COALESCE(body,'') FROM site_fragment "
          "WHERE namespace=?1 AND page=?2 AND status='published' AND "
           "audience='public' AND kind IN "
           "('prose','body','paragraph','text','rich-text','story','quote',"
           "'article','essay','report','feature','section','longform','profile-story') "
           ""
          "ORDER BY position,id LIMIT 256";
        if(sqlite3_prepare_v2(db,fragments,-1,&st,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(st,1,ns,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,out->route,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(st)==SQLITE_ROW){
            const char *body=(const char*)sqlite3_column_text(st,1);authored_body+=body?strlen(body):0;
            if(!bf_narration_add(out,(const char*)sqlite3_column_text(st,0))||!bf_narration_add(out,body))goto done;
        }
    }else if(!strncmp(subject,"post:",5)){
        const char *query=
          "SELECT COALESCE(title,''),body FROM community_post WHERE namespace=?1 "
          "AND post=?2 AND status='published' AND visibility='public' AND "
          "moderation_state IN ('accepted','visible','published') "
          "ORDER BY id DESC LIMIT 2";
        if(sqlite3_prepare_v2(db,query,-1,&st,NULL)!=SQLITE_OK)return 0;
        sqlite3_bind_text(st,1,ns,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,subject+5,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(st)==SQLITE_ROW){
            const char *title=(const char*)sqlite3_column_text(st,0),
                       *body=(const char*)sqlite3_column_text(st,1);
            if(!title||strlen(title)>=sizeof(out->title))goto done;
            snprintf(out->title,sizeof(out->title),"%s",*title?title:"Community post");
            snprintf(out->kind,sizeof(out->kind),"post");
            authored_body=body?strlen(body):0;if(!bf_narration_add(out,title)||!bf_narration_add(out,body))goto done;
            found=1;
        }
        if(!found||sqlite3_step(st)==SQLITE_ROW)goto done;
    }else return 0;
    if(out->length<32||authored_body<32)goto done;
    BfSha256 h;uint8_t digest[32];
    bf_sha256_init(&h);
    bf_sha256_update(&h,(const uint8_t*)ns,strlen(ns)+1);
    bf_sha256_update(&h,(const uint8_t*)subject,strlen(subject)+1);
    bf_sha256_update(&h,(const uint8_t*)out->route,strlen(out->route)+1);
    bf_sha256_update(&h,(const uint8_t*)out->text,out->length);
    bf_sha256_final(&h,digest);bf_sha256_digest_hex(digest,out->sha);
    ok=1;
done:
    sqlite3_finalize(st);
    if(!ok)memset(out,0,sizeof(*out));
    return ok;
}
/* A separate v2 article revision covers public editor data, without changing
 * the legacy script digest used by existing published MediaPrograms. It is an
 * identity only: it does not establish that a take speaks the source. */
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
static inline int bf_narration_digest_column(sqlite3 *db){
    sqlite3_stmt *st=NULL;int has=0;
    if(sqlite3_prepare_v2(db,"PRAGMA table_info(media_program)",-1,&st,NULL)!=SQLITE_OK)return 0;
    while(sqlite3_step(st)==SQLITE_ROW){
        const char *col=(const char*)sqlite3_column_text(st,1);
        if(col&&!strcmp(col,"narration_source_sha256")){has=1;break;}
    }
    sqlite3_finalize(st);return has;
}
static inline int bf_narration_article_revision_column(sqlite3 *db){
    sqlite3_stmt *st=NULL;int has=0;
    if(sqlite3_prepare_v2(db,"PRAGMA table_info(media_program)",-1,&st,NULL)!=SQLITE_OK)return 0;
    while(sqlite3_step(st)==SQLITE_ROW){const char *col=(const char*)sqlite3_column_text(st,1);
        if(col&&!strcmp(col,"narration_article_revision_sha256")){has=1;break;}}
    sqlite3_finalize(st);return has;
}
static inline const char *bf_narration_state(sqlite3 *db,const char *ns,
          const char *subject,const char *current,int with_sha){
    sqlite3_stmt *st=NULL;const char *state="needs-narration";
    int with_v2=with_sha&&bf_narration_article_revision_column(db);
    const char *sql=with_v2?
      "SELECT status,publication_state,COALESCE(narration_source_sha256,''),COALESCE(narration_article_revision_sha256,'') "
      "FROM media_program WHERE subject_ref=?1 AND media_kind='audio' AND "
      "namespace IN ('root',?2) AND status IN ('draft','published') "
      "ORDER BY CASE WHEN status='published' AND publication_state='published' THEN 0 ELSE 1 END,id DESC LIMIT 1":with_sha?
      "SELECT status,publication_state,COALESCE(narration_source_sha256,'') "
      "FROM media_program WHERE subject_ref=?1 AND media_kind='audio' AND "
      "namespace IN ('root',?2) AND status IN ('draft','published') "
      "ORDER BY CASE WHEN status='published' AND publication_state='published' THEN 0 ELSE 1 END,id DESC LIMIT 1":
      "SELECT status,publication_state,'' FROM media_program WHERE subject_ref=?1 "
      "AND media_kind='audio' AND namespace IN ('root',?2) "
      "AND status IN ('draft','published') "
      "ORDER BY CASE WHEN status='published' AND publication_state='published' THEN 0 ELSE 1 END,id DESC LIMIT 1";
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return "unavailable";
    sqlite3_bind_text(st,1,subject,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,ns,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW){
        const char *status=(const char*)sqlite3_column_text(st,0),
                   *publication=(const char*)sqlite3_column_text(st,1),
                   *revision=(const char*)sqlite3_column_text(st,2);
        int published=status&&publication&&!strcmp(status,"published")&&!strcmp(publication,"published");
        const char *v2=with_v2?(const char*)sqlite3_column_text(st,3):NULL;
        if(v2&&*v2){
            char exact[65]={0};
            if(!bf_narration_article_revision(db,ns,subject,"published",exact)||strcmp(v2,exact))
                state=published?"stale-published":"stale-draft";
            else state=published?"published-current":"awaiting-review";
        }else if(revision&&*revision&&strcmp(revision,current))state=published?"stale-published":"stale-draft";
        else if(revision&&*revision)state=published?"published-current":"awaiting-review";
        else state=published?"published-unverified":"awaiting-review-unverified";
    }
    sqlite3_finalize(st);return state;
}
#endif
