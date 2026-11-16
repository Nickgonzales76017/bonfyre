#ifndef BONFYRE_PROFILE_JOURNAL_INDEX_H
#define BONFYRE_PROFILE_JOURNAL_INDEX_H
#include "ui_tree.h"
/* A member site's journal, inside the one estate shell (BF_ESTATE_BAR,
 * BF_ESTATE_FOOT, body.ak-app).  The index reads as a plain reading list
 * (edition strip, headline with subjects, the newest article, then every
 * other article ruled one under another); the article reads as a quiet
 * single column.  Both read released site_page rows; the article body is the
 * shared native projector's output.  Anything to hear goes to the shared
 * player through data-ak-track. */

static void ph_shell_open(const char *title,const char *description,const char *canonical,const char *body_class){
 fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nVary: Host\r\nReferrer-Policy: same-origin\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n"
       "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta name=\"theme-color\" content=\"#0f1511\"><title>",stdout);
 html(title);ui_close(stdout,"title");
 if(description&&*description){ui_start(stdout,"meta");ui_attr(stdout,"name","description");ui_attr_open(stdout,"content");html(description);fputs("\">",stdout);}
 if(canonical&&*canonical){ui_start(stdout,"link");ui_attr(stdout,"rel","canonical");ui_attr_open(stdout,"href");html(canonical);fputs("\">",stdout);}
 ui_start(stdout,"meta");ui_attr(stdout,"property","og:site_name");ui_attr(stdout,"content","Aurekai");ui_gt(stdout);ui_start(stdout,"meta");ui_attr(stdout,"property","og:type");ui_attr_open(stdout,"content");
 fputs(body_class&&!strcmp(body_class,"ak-journal-article")?"article":"website",stdout);
 fputs("\"><meta property=\"og:title\" content=\"",stdout);html(title);fputs("\">",stdout);
 if(description&&*description){ui_start(stdout,"meta");ui_attr(stdout,"property","og:description");ui_attr_open(stdout,"content");html(description);fputs("\">",stdout);}
 if(canonical&&*canonical){ui_start(stdout,"meta");ui_attr(stdout,"property","og:url");ui_attr_open(stdout,"content");html(canonical);fputs("\">",stdout);}
 ui_start(stdout,"meta");ui_attr(stdout,"name","twitter:card");ui_attr(stdout,"content","summary");ui_gt(stdout);
 ui_start(stdout,"link");ui_attr(stdout,"rel","stylesheet");ui_attr(stdout,"href","https://fonts.googleapis.com/css2?family=Literata:ital,opsz,wght@0,7..72,400;0,7..72,600;1,7..72,400&amp;display=swap");ui_gt(stdout);ui_start(stdout,"link");ui_attr(stdout,"rel","stylesheet");ui_attr(stdout,"href","/assets/authoring-reader.css");ui_gt(stdout);ui_start(stdout,"link");ui_attr(stdout,"rel","stylesheet");ui_attr(stdout,"href","/assets/composition.css");ui_gt(stdout);ui_close(stdout,"head");ui_start(stdout,"body");ui_attr_open(stdout,"class");ui_copy(stdout,"ak-app ak-journal ");
 html(body_class);fputs("\">" BF_ESTATE_BAR "<main id=\"main\" class=\"ak-journal-main\">",stdout);
}
static void ph_shell_close(void){fputs("</main>" BF_ESTATE_FOOT "</body></html>",stdout);}

static int ph_blog_slug_safe(const char *route,size_t *len_out){
 if(!route||strncmp(route,"/blog/",6))return 0;
 const char *slug=route+6;size_t len=strlen(slug);if(len&&slug[len-1]=='/')len--;
 if(!len||len>96||!((slug[0]>='a'&&slug[0]<='z')||(slug[0]>='0'&&slug[0]<='9')))return 0;
 for(size_t i=1;i<len;i++)if(!((slug[i]>='a'&&slug[i]<='z')||(slug[i]>='0'&&slug[i]<='9')||slug[i]=='-'))return 0;
 if(slug[len]&&slug[len]!='/')return 0;
 *len_out=len;return 1;
}

/* Released journal pages that have composed public sections. */
#define PH_JOURNAL_RELEASED "p.namespace=?1 AND p.status='published' AND p.route LIKE '/blog/%' AND EXISTS(SELECT 1 FROM site_fragment f WHERE f.namespace=p.namespace AND f.page=p.route AND f.slot='showcase' AND f.audience='public' AND f.status='published' AND (f.kind LIKE 'profile-%' OR f.kind LIKE 'ui:%' OR f.kind LIKE 'service:%') AND CASE WHEN json_valid(f.projection_contract) THEN COALESCE(json_extract(f.projection_contract,'$.html'),0)=1 ELSE 0 END AND CASE WHEN json_valid(f.semantic_tags) THEN COALESCE(json_extract(f.semantic_tags,'$.authoring_v1.version'),0)=1 ELSE 0 END)"

static int ph_blog_index(sqlite3 *db,const PhSite *site,const char *host,const char *query){
 if(!db||!site||!host){ph_error(503,"The journal store is unavailable.\n");return 1;}
 char subject[121]="",find[121]="",canonical[300];int dated=ph_page_has_date(db);
 if(query&&*query){query_value(query,"subject",subject,sizeof(subject));query_value(query,"q",find,sizeof(find));}
 snprintf(canonical,sizeof(canonical),"https://%s/blog/",host);
 char title[220];snprintf(title,sizeof(title),"Journal · %s",site->name);
 ph_shell_open(title,"Articles published on this site.",canonical,"ak-journal-index");
 /* The strip: how many articles, the newest date, who writes them. */
 int total=0;char newest[11]="",author[160]="";
 {sqlite3_stmt *st=NULL;
  if(sqlite3_prepare_v2(db,dated?"SELECT count(*),COALESCE(max(editorial_published_on),''),COALESCE(max(author_credit),'') FROM site_page p WHERE " PH_JOURNAL_RELEASED:
                                 "SELECT count(*),'',COALESCE(max(author_credit),'') FROM site_page p WHERE " PH_JOURNAL_RELEASED,-1,&st,NULL)==SQLITE_OK){
   sqlite3_bind_text(st,1,site->id,-1,SQLITE_STATIC);
   if(sqlite3_step(st)==SQLITE_ROW){total=sqlite3_column_int(st,0);
    if(sqlite3_column_text(st,1))snprintf(newest,sizeof(newest),"%s",(const char*)sqlite3_column_text(st,1));
    if(sqlite3_column_text(st,2))snprintf(author,sizeof(author),"%s",(const char*)sqlite3_column_text(st,2));}}
  sqlite3_finalize(st);}
 char newest_long[32]="";if(newest[0])ph_date_long(newest,newest_long,sizeof(newest_long));
 ui_open(stdout,"div","fd ak-journal-list");ui_start(stdout,"div");ui_attr(stdout,"class","fd-strip");ui_attr(stdout,"role","note");ui_gt(stdout);ui_open(stdout,"p",NULL);ui_start(stdout,"span");ui_attr(stdout,"class","fd-dot");ui_attr(stdout,"aria-hidden","true");ui_gt(stdout);ui_close(stdout,"span");ui_copy(stdout,"Journal");
 if(author[0]||site->name[0]){fputs(" &middot; ",stdout);html(author[0]?author:site->name);}
 ui_close(stdout,"p");ui_open(stdout,"p","fd-strip-facts");
 if(newest_long[0]){ui_open(stdout,"span",NULL);ui_copy(stdout,"Latest ");html(newest_long);ui_close(stdout,"span");}
 ui_open(stdout,"span","fd-strip-count");fprintf(stdout,"%d",total);ui_copy(stdout," articles");ui_close(stdout,"span");ui_close(stdout,"p");ui_close(stdout,"div");
 ui_open(stdout,"header","fd-head");ui_open(stdout,"div","fd-head-words");ui_open(stdout,"h1",NULL);ui_copy(stdout,"Journal");ui_close(stdout,"h1");ui_open(stdout,"p","fd-lede");ui_copy(stdout,"Writing by ");html(author[0]?author:site->name);
 fputs(", newest first.</p></div><form class=\"fd-find\" action=\"/blog/\" method=\"get\" role=\"search\"><label>",stdout);page_glyph("search");
 fputs("<span class=\"fd-sr\">Search the journal</span><input type=\"search\" name=\"q\" maxlength=\"120\" placeholder=\"Search articles\" value=\"",stdout);html(find);
 fputs("\"></label></form><nav class=\"fd-chips\" aria-label=\"Subjects\"><a href=\"/blog/\"",stdout);if(!subject[0])fputs(" aria-current=\"page\"",stdout);fputs(">All</a>",stdout);
 {sqlite3_stmt *cats=NULL;
  if(sqlite3_prepare_v2(db,"SELECT COALESCE(p.eyebrow,''),count(*) FROM site_page p WHERE " PH_JOURNAL_RELEASED " AND COALESCE(p.eyebrow,'')<>'' GROUP BY 1 ORDER BY 2 DESC,1",-1,&cats,NULL)==SQLITE_OK){
   sqlite3_bind_text(cats,1,site->id,-1,SQLITE_STATIC);
   while(sqlite3_step(cats)==SQLITE_ROW){const char *c=(const char*)sqlite3_column_text(cats,0);if(!c||strlen(c)>120)continue;
    ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/blog/?subject=");url_query_value(c);fputs("\"",stdout);if(!strcmp(c,subject))fputs(" aria-current=\"page\"",stdout);fputs(">",stdout);html(c);ui_close(stdout,"a");}}
  sqlite3_finalize(cats);}
 ui_close(stdout,"nav");ui_close(stdout,"header");
 sqlite3_stmt *st=NULL;int count=0;char pattern[130]="";
 if(find[0])snprintf(pattern,sizeof(pattern),"%%%s%%",find);
 const char *sql=dated?
  "SELECT p.route,p.title,COALESCE(p.lede,''),COALESCE(p.eyebrow,''),COALESCE(p.editorial_published_on,''),COALESCE(p.author_credit,'') FROM site_page p WHERE " PH_JOURNAL_RELEASED
  " AND (?2='' OR p.eyebrow=?2) AND (?3='' OR p.title LIKE ?3 OR COALESCE(p.lede,'') LIKE ?3) ORDER BY p.editorial_published_on IS NULL,p.editorial_published_on DESC,p.route LIMIT 200":
  "SELECT p.route,p.title,COALESCE(p.lede,''),COALESCE(p.eyebrow,''),'',COALESCE(p.author_credit,'') FROM site_page p WHERE " PH_JOURNAL_RELEASED
  " AND (?2='' OR p.eyebrow=?2) AND (?3='' OR p.title LIKE ?3 OR COALESCE(p.lede,'') LIKE ?3) ORDER BY p.route LIMIT 200";
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)==SQLITE_OK){
  sqlite3_bind_text(st,1,site->id,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,subject,-1,SQLITE_STATIC);sqlite3_bind_text(st,3,pattern,-1,SQLITE_STATIC);
  while(sqlite3_step(st)==SQLITE_ROW){
   const char *route=(const char*)sqlite3_column_text(st,0),*t=(const char*)sqlite3_column_text(st,1),*lede=(const char*)sqlite3_column_text(st,2);
   const char *cat=(const char*)sqlite3_column_text(st,3),*on=(const char*)sqlite3_column_text(st,4),*by=(const char*)sqlite3_column_text(st,5);
   size_t len=0;if(!t||!*t||!ph_blog_slug_safe(route,&len))continue;
   char when[32]="";if(on&&*on)ph_date_long(on,when,sizeof(when));
   long mins=ph_read_mins(db,site->id,route);
   if(!count){
    /* The newest article leads, the way the hub opens on one story. */
    fputs("<section class=\"fd-lead fd-lead-plain ak-journal-lead\" aria-label=\"Newest article\"><div class=\"fd-lead-words\"><p class=\"fd-kicker\"><span>",stdout);
    html(cat&&*cat?cat:"Journal");ui_close(stdout,"span");ui_open(stdout,"span",NULL);html(when);ui_close(stdout,"span");ui_close(stdout,"p");ui_start(stdout,"div");ui_attr(stdout,"id","jl-lead");ui_gt(stdout);ui_open(stdout,"h2",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");fwrite(route,1,6+len,stdout);fputs("/\">",stdout);html(t);ui_close(stdout,"a");ui_close(stdout,"h2");
    if(lede&&*lede){ui_text(stdout, "p", "fd-dek", lede);}
    ui_close(stdout,"div");ui_open(stdout,"p","fd-by");html(by&&*by?by:site->name);printf(" &middot; %ld min read</p><div class=\"fd-lead-actions\"><a class=\"fd-primary\" href=\"",mins);
    fwrite(route,1,6+len,stdout);fputs("/\">Read the article</a><button type=\"button\" class=\"fd-ghost\" data-ak-track data-ak-default data-title=\"",stdout);html(t);
    fputs("\" data-by=\"",stdout);html(by&&*by?by:site->name);fputs("\" data-narrate=\"#jl-lead\">",stdout);page_glyph("narration");fputs("<span>Read aloud</span></button></div></div></section>"
          "<section class=\"ak-journal-rows\" aria-label=\"All articles\"><ol>",stdout);
   } else {
    ui_open(stdout,"li",NULL);ui_open(stdout,"p","fd-card-top");ui_open(stdout,"span","fd-beat");html(cat&&*cat?cat:"Journal");ui_close(stdout,"span");ui_open(stdout,"span",NULL);html(when);
    printf("%s%ld min read</span></p><h3><a href=\"",when[0]?" &middot; ":"",mins);fwrite(route,1,6+len,stdout);fputs("/\">",stdout);html(t);ui_close(stdout,"a");ui_close(stdout,"h3");
    if(lede&&*lede){ui_text(stdout, "p", "fd-dek", lede);}
    ui_close(stdout,"li");
   }
   count++;
  }
 }
 sqlite3_finalize(st);
 if(count){ui_close(stdout,"ol");ui_close(stdout,"section");}
 else if(subject[0]||find[0]){ui_open(stdout,"p","fd-empty");ui_copy(stdout,"No articles match. ");ui_start(stdout,"a");ui_attr(stdout,"href","/blog/");ui_gt(stdout);ui_copy(stdout,"Show every article");ui_close(stdout,"a");ui_copy(stdout,".");ui_close(stdout,"p");}
 else ui_text(stdout, "p", "fd-empty", "Nothing has been published here yet.");
 ui_close(stdout,"div");
 ph_shell_close();
 return 1;
}
#endif
