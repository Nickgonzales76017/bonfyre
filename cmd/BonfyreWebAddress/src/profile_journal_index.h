#ifndef BONFYRE_PROFILE_JOURNAL_INDEX_H
#define BONFYRE_PROFILE_JOURNAL_INDEX_H
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
 html(title);fputs("</title>",stdout);
 if(description&&*description){fputs("<meta name=\"description\" content=\"",stdout);html(description);fputs("\">",stdout);}
 if(canonical&&*canonical){fputs("<link rel=\"canonical\" href=\"",stdout);html(canonical);fputs("\">",stdout);}
 fputs("<meta property=\"og:site_name\" content=\"Aurekai\"><meta property=\"og:type\" content=\"",stdout);
 fputs(body_class&&!strcmp(body_class,"ak-journal-article")?"article":"website",stdout);
 fputs("\"><meta property=\"og:title\" content=\"",stdout);html(title);fputs("\">",stdout);
 if(description&&*description){fputs("<meta property=\"og:description\" content=\"",stdout);html(description);fputs("\">",stdout);}
 if(canonical&&*canonical){fputs("<meta property=\"og:url\" content=\"",stdout);html(canonical);fputs("\">",stdout);}
 fputs("<meta name=\"twitter:card\" content=\"summary\">",stdout);
 fputs("<link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=Literata:ital,opsz,wght@0,7..72,400;0,7..72,600;1,7..72,400&amp;display=swap\">"
       "<link rel=\"stylesheet\" href=\"/assets/authoring-reader.css\"><link rel=\"stylesheet\" href=\"/assets/composition.css\"></head><body class=\"ak-app ak-journal ",stdout);
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
 fputs("<div class=\"fd ak-journal-list\"><div class=\"fd-strip\" role=\"note\"><p><span class=\"fd-dot\" aria-hidden=\"true\"></span>Journal",stdout);
 if(author[0]||site->name[0]){fputs(" &middot; ",stdout);html(author[0]?author:site->name);}
 fputs("</p><p class=\"fd-strip-facts\">",stdout);
 if(newest_long[0]){fputs("<span>Latest ",stdout);html(newest_long);fputs("</span>",stdout);}
 printf("<span class=\"fd-strip-count\">%d articles</span></p></div>",total);
 fputs("<header class=\"fd-head\"><div class=\"fd-head-words\"><h1>Journal</h1><p class=\"fd-lede\">Writing by ",stdout);html(author[0]?author:site->name);
 fputs(", newest first.</p></div><form class=\"fd-find\" action=\"/blog/\" method=\"get\" role=\"search\"><label>",stdout);page_glyph("search");
 fputs("<span class=\"fd-sr\">Search the journal</span><input type=\"search\" name=\"q\" maxlength=\"120\" placeholder=\"Search articles\" value=\"",stdout);html(find);
 fputs("\"></label></form><nav class=\"fd-chips\" aria-label=\"Subjects\"><a href=\"/blog/\"",stdout);if(!subject[0])fputs(" aria-current=\"page\"",stdout);fputs(">All</a>",stdout);
 {sqlite3_stmt *cats=NULL;
  if(sqlite3_prepare_v2(db,"SELECT COALESCE(p.eyebrow,''),count(*) FROM site_page p WHERE " PH_JOURNAL_RELEASED " AND COALESCE(p.eyebrow,'')<>'' GROUP BY 1 ORDER BY 2 DESC,1",-1,&cats,NULL)==SQLITE_OK){
   sqlite3_bind_text(cats,1,site->id,-1,SQLITE_STATIC);
   while(sqlite3_step(cats)==SQLITE_ROW){const char *c=(const char*)sqlite3_column_text(cats,0);if(!c||strlen(c)>120)continue;
    fputs("<a href=\"/blog/?subject=",stdout);url_query_value(c);fputs("\"",stdout);if(!strcmp(c,subject))fputs(" aria-current=\"page\"",stdout);fputs(">",stdout);html(c);fputs("</a>",stdout);}}
  sqlite3_finalize(cats);}
 fputs("</nav></header>",stdout);
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
    html(cat&&*cat?cat:"Journal");fputs("</span><span>",stdout);html(when);fputs("</span></p><div id=\"jl-lead\"><h2><a href=\"",stdout);fwrite(route,1,6+len,stdout);fputs("/\">",stdout);html(t);fputs("</a></h2>",stdout);
    if(lede&&*lede){fputs("<p class=\"fd-dek\">",stdout);html(lede);fputs("</p>",stdout);}
    fputs("</div><p class=\"fd-by\">",stdout);html(by&&*by?by:site->name);printf(" &middot; %ld min read</p><div class=\"fd-lead-actions\"><a class=\"fd-primary\" href=\"",mins);
    fwrite(route,1,6+len,stdout);fputs("/\">Read the article</a><button type=\"button\" class=\"fd-ghost\" data-ak-track data-ak-default data-title=\"",stdout);html(t);
    fputs("\" data-by=\"",stdout);html(by&&*by?by:site->name);fputs("\" data-narrate=\"#jl-lead\">",stdout);page_glyph("narration");fputs("<span>Read aloud</span></button></div></div></section>"
          "<section class=\"ak-journal-rows\" aria-label=\"All articles\"><ol>",stdout);
   } else {
    fputs("<li><p class=\"fd-card-top\"><span class=\"fd-beat\">",stdout);html(cat&&*cat?cat:"Journal");fputs("</span><span>",stdout);html(when);
    printf("%s%ld min read</span></p><h3><a href=\"",when[0]?" &middot; ":"",mins);fwrite(route,1,6+len,stdout);fputs("/\">",stdout);html(t);fputs("</a></h3>",stdout);
    if(lede&&*lede){fputs("<p class=\"fd-dek\">",stdout);html(lede);fputs("</p>",stdout);}
    fputs("</li>",stdout);
   }
   count++;
  }
 }
 sqlite3_finalize(st);
 if(count)fputs("</ol></section>",stdout);
 else if(subject[0]||find[0])fputs("<p class=\"fd-empty\">No articles match. <a href=\"/blog/\">Show every article</a>.</p>",stdout);
 else fputs("<p class=\"fd-empty\">Nothing has been published here yet.</p>",stdout);
 fputs("</div>",stdout);
 ph_shell_close();
 return 1;
}
#endif
