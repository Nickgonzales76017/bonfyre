#define _GNU_SOURCE
#include "site_fragment_projection.h"
#include "../generated/native_authoring_contract.h"
#include "../generated/native_site_fragment_template.h"
#include "../../BonfyreSurfaceScene/src/authoring_template.h"
#include "bf_jsonw.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Native row selection remains the authority: template rendering is pure. */
typedef struct {char *buf;size_t cap,used;int failed;} Writer;
static void raw(Writer *w,const char *s) {
 if(!w||w->failed||!s)return;
 size_t n=strlen(s);
 if(w->used>=w->cap||n>=w->cap-w->used){w->failed=1;return;}
 memcpy(w->buf+w->used,s,n);w->used+=n;w->buf[w->used]='\0';
}
static int ident(const char *s){
 size_t n;
 if(!s||!(n=strlen(s))||n>128)return 0;
 for(size_t i=0;i<n;i++){
  unsigned char c=(unsigned char)s[i];
  if(!(isalnum(c)||c=='-'||c=='_'||c=='.'||c==':'))return 0;
 }
 return 1;
}
static int href_ok(const char *s){
 if(!s||!*s||strlen(s)>2000||(s[0]=='/'&&s[1]=='/')||strstr(s,".."))return 0;
 for(const unsigned char *p=(const unsigned char *)s;*p;p++)
  if(*p<33||*p==127||*p=='<'||*p=='>'||*p=='"'||*p==39||*p=='\\')return 0;
 return s[0]=='/'||s[0]=='#'||!strncmp(s,"https://",8)||!strncmp(s,"http://",7);
}
static int page_ok(const char *path){
 if(ident(path))return 1; /* retained semantic page aliases */
 if(!path||*path!='/'||strlen(path)>180||strstr(path,"..")||strstr(path,"//"))return 0;
 for(const unsigned char *p=(const unsigned char *)(path+1);*p;p++)
  if(!(isalnum(*p)||*p=='-'||*p=='_'||*p=='/'))return 0;
 return 1;
}
static const char *repr_key(BfSiteFragmentRepresentation r){
 return r==BF_SITE_FRAGMENT_HTML?"html":r==BF_SITE_FRAGMENT_HTMX?"htmx":NULL;
}
static int count_rows(sqlite3 *db,const char *sql,const BfSiteFragmentProjectionRequest *r){
 sqlite3_stmt *st=NULL;int n=-1;
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(st,1,r->namespace_name,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(st,2,r->page,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(st,3,r->slot,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(st,4,r->audience,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(st)==SQLITE_ROW)n=sqlite3_column_int(st,0);
 sqlite3_finalize(st);return n;
}
static int render_fragment(Writer *w,sqlite3_stmt *row){
 const char *coordinate=(const char *)sqlite3_column_text(row,0);
 const char *kind=(const char *)sqlite3_column_text(row,1);
 const char *eyebrow=(const char *)sqlite3_column_text(row,2);
 const char *heading=(const char *)sqlite3_column_text(row,3);
 const char *body=(const char *)sqlite3_column_text(row,4);
 const char *label=(const char *)sqlite3_column_text(row,5);
 const char *href=(const char *)sqlite3_column_text(row,6);
 if(!coordinate||!kind||!ident(coordinate)||!ident(kind))return 0;
 /* Native grammar limits for authored blocks, all other kinds retain their owner. */
 if(bf_authoring_block(kind)&&
    !bf_authoring_block_accepts(kind,heading?strlen(heading):0,body?strlen(body):0))return 0;
 bf_jsonw json;bf_jsonw_init(&json,0);bf_jsonw_obj(&json);
 bf_jsonw_key(&json,"kind");bf_jsonw_str(&json,kind);
 bf_jsonw_key(&json,"coordinate");bf_jsonw_str(&json,coordinate);
 bf_jsonw_key(&json,"eyebrow");bf_jsonw_str(&json,eyebrow?eyebrow:"");
 bf_jsonw_key(&json,"heading");bf_jsonw_str(&json,heading?heading:"");
 bf_jsonw_key(&json,"body");bf_jsonw_str(&json,body?body:"");
 int cta=label&&*label&&href_ok(href);
 bf_jsonw_key(&json,"cta");bf_jsonw_bool(&json,cta);
 bf_jsonw_key(&json,"label");bf_jsonw_str(&json,cta?label:"");
 bf_jsonw_key(&json,"href");bf_jsonw_str(&json,cta?href:"");
 bf_jsonw_obj_end(&json);
 if(json.oom||!json.buf){bf_jsonw_free(&json);return -1;}
 char *rendered=NULL;size_t bytes=0;
 FILE *dest=open_memstream(&rendered,&bytes);
 if(!dest){bf_jsonw_free(&json);return -1;}
 const size_t allowed=w->cap>w->used?w->cap-w->used-1:0;
 int done=allowed&&bf_authoring_template_render(dest,bf_native_site_fragment_template,json.buf,allowed);
 if(fclose(dest))done=0;
 bf_jsonw_free(&json);
 if(!done){free(rendered);return -1;}
 raw(w,rendered);
 free(rendered);
 return w->failed?-1:1;
}
int bf_site_fragment_project(sqlite3 *db,const BfSiteFragmentProjectionRequest *r,
                             char *out,size_t cap,BfSiteFragmentProjectionStats *stats){
 sqlite3_stmt *st=NULL;Writer w={out,cap,0,0};char sql[1024];
 const char *key;int rows=0;
 if(stats)memset(stats,0,sizeof(*stats));
 if(!db||!r||!out||cap<2||!ident(r->namespace_name)||!page_ok(r->page)||
    !ident(r->slot)||!ident(r->audience)||!(key=repr_key(r->representation)))return -1;
 out[0]='\0';
 if(stats){
  stats->exact_coordinate_rows=count_rows(db,
   "SELECT count(*) FROM site_fragment WHERE namespace=?1 AND page=?2 AND slot=?3 AND audience=?4",r);
  stats->published_rows=count_rows(db,
   "SELECT count(*) FROM site_fragment WHERE namespace=?1 AND page=?2 AND slot=?3 AND audience=?4 AND status='published'",r);
  if(stats->exact_coordinate_rows<0||stats->published_rows<0)return -1;
 }
 /* The template never determines eligibility; SQL enforces exact scope and
  * status, plus explicit representation admission in projection_contract. */
 if(snprintf(sql,sizeof(sql),
  "SELECT coordinate,kind,eyebrow,heading,body,label,href FROM site_fragment "
  "WHERE namespace=?1 AND page=?2 AND slot=?3 AND audience=?4 "
  "AND status='published' AND COALESCE(json_extract(projection_contract,'$.%s'),0)=1 "
  "ORDER BY position,id",key)>=(int)sizeof(sql))return -1;
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(st,1,r->namespace_name,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(st,2,r->page,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(st,3,r->slot,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(st,4,r->audience,-1,SQLITE_TRANSIENT);
 int step=SQLITE_DONE;
 while((step=sqlite3_step(st))==SQLITE_ROW){
  int result=render_fragment(&w,st);
  if(result<0){sqlite3_finalize(st);out[0]='\0';return -1;}
  rows+=result;
 }
 sqlite3_finalize(st);
 if(step!=SQLITE_DONE||w.failed){out[0]='\0';return -1;}
 if(stats){stats->rows=rows;stats->contract_rows=rows;}
 return rows?1:0;
}
