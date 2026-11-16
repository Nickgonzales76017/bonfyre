#ifndef BONFYRE_SOURCE_DESK_H
#define BONFYRE_SOURCE_DESK_H
#include "form_tree.h"
#include "web_store.h"
#include "../generated/native_estate_hosts.h"
static int session_institution_authorized(const char *,const char *,const char *,const char *);
static int page_query_has_key(const char *,const char *);
/* Sources, feeds and connections, one native owner of their pages.
 *
 * Every page here is read from records that already exist: the item archive
 * (what the estate kept from each source), the source frontier (what it has
 * listed, admitted and refused) and the auth store (identities a person has
 * declared).  It writes nothing.  Forms post to the owners that already do the
 * work: /studio/ reviews an address and offers observation, /account/connections
 * declares and revokes an identity link, /feed-use hands a kept item to a
 * workspace, /apps/connections opens the mail connection studio.  The pages
 * wear the one shell: BF_ESTATE_BAR, <main>, BF_ESTATE_FOOT. */

#define SD_LIST 24

static sqlite3 *sd_open_env(const char *var,const BfStoreDurability *policy){
 const char *p=getenv(var);sqlite3 *db=NULL;
 if(!p||!*p||bf_store_open(p, &db, SQLITE_OPEN_READONLY, policy)!=SQLITE_OK){if(db)sqlite3_close(db);return NULL;}
 sqlite3_busy_timeout(db,1500);return db;
}
static const char *sd_col(sqlite3_stmt *s,int i){const char *t=(const char*)sqlite3_column_text(s,i);return t?t:"";}
static long long sd_one(sqlite3 *db,const char *sql,const char *bind){
 sqlite3_stmt *s=NULL;long long n=0;if(!db||sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return 0;
 if(bind)sqlite3_bind_text(s,1,bind,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW)n=sqlite3_column_int64(s,0);sqlite3_finalize(s);return n;
}
static void sd_num(long long n){
 if(n>=1000000)printf("%lld,%03lld,%03lld",n/1000000,(n/1000)%1000,n%1000);
 else if(n>=1000)printf("%lld,%03lld",n/1000,n%1000);else printf("%lld",n);
}
static void sd_ago(long long at,const char *read){
 time_t now=time(NULL);
 if(at<=0||at>(long long)now+86400){if(read&&strlen(read)>=10)fwrite(read,1,10,stdout);return;}
 long long d=(long long)now-at;if(d<0)d=0;
 if(d<3600)printf("%lldm ago",d/60+1);else if(d<86400)printf("%lldh ago",d/3600);
 else if(d<86400LL*21)printf("%lldd ago",d/86400);
 else{char b[24];time_t t=(time_t)at;struct tm tm;gmtime_r(&t,&tm);strftime(b,sizeof b,"%d %b %Y",&tm);fputs(b,stdout);}
}
static void sd_label(const char *family){
 char b[80];size_t i=0;if(!family||!*family){fputs("Unfiled",stdout);return;}
 for(;family[i]&&i+1<sizeof b;i++)b[i]=family[i]=='-'||family[i]=='_'?' ':family[i];b[i]=0;
 if(b[0]>='a'&&b[0]<='z')b[0]=(char)(b[0]-32);html(b);
}
static int sd_safe_word(const char *s,size_t cap){
 size_t n=s?strlen(s):0;if(!n||n>=cap)return 0;
 for(;*s;s++)if(!(isalnum((unsigned char)*s)||strchr("-_.@",*s)))return 0;return 1;
}
/* The publisher's printed name when the frontier holds one, else its host. */
static void sd_name(sqlite3 *frontier,const char *host){
 sqlite3_stmt *s=NULL;int done=0;
 if(frontier&&host&&*host&&sqlite3_prepare_v2(frontier,"SELECT name FROM root WHERE host=?1 AND name<>'' LIMIT 1",-1,&s,NULL)==SQLITE_OK){
  sqlite3_bind_text(s,1,host,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW&&*sd_col(s,0)){html(sd_col(s,0));done=1;}
 }
 if(s)sqlite3_finalize(s);if(!done)html(host&&*host?host:"Unnamed source");
}

/* ---------------------------------------------------------------- shell */
static void sd_page_open(const char *title,const char *host,const char *route,const char *query,const char *variant){
 const char *territory=bf_route_presentation_territory(host);
 const char *pattern=territory?bf_route_presentation_native_pattern(territory,route):NULL;
 fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\n"
       "Vary: Host, Accept, Cookie, Authorization\r\nX-Bonfyre-Surface-Program: native-source-workspace\r\nConnection: close\r\n\r\n"
       "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>",stdout);
 html(title);fputs(" · Aurekai</title>"
       "<link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=Literata:ital,opsz,wght@0,7..72,400;0,7..72,500;1,7..72,400&amp;display=swap\">"
       "<link rel=\"stylesheet\" href=\"/assets/composition.css\">",stdout);
 if(!strcmp(variant,"research"))ui_open_a(stdout,"link","rel","stylesheet","href","/apps/source-desk.css");
 if(pattern)bf_route_presentation_emit_head_for(stdout,territory,route,query,pattern);
 fputs("</head><body class=\"ak-app sx-page sx-",stdout);html(variant);fputs("\"",stdout);
 if(pattern)bf_route_presentation_emit_body_attribute_for(stdout,territory,route,query,pattern);
 fputs(">" BF_ESTATE_BAR,stdout);
}
static void sd_page_close(void){fputs(BF_ESTATE_FOOT "<script>document.addEventListener('click',function(e){var o=e.target.closest&&e.target.closest('[data-sx-open]');if(o){var d=document.getElementById(o.dataset.sxOpen);if(d&&d.showModal){e.preventDefault();d.showModal();var f=d.querySelector('input:not([type=hidden])');if(f)f.focus()}}var c=e.target.closest&&e.target.closest('[data-sx-close]');if(c){var p=c.closest('dialog');if(p)p.close()}});</script></body></html>",stdout);}

static void sd_icon(const char *name){page_glyph(name);}

/* The one dialog that brings a new address.  It posts nowhere new: /studio/
 * shows the address without fetching it and, for a writer, offers the
 * ForeignTwin observation that reads it and keeps a receipt. */
static void sd_add_dialog(void){
 ui_open_a(stdout,"dialog","class","sx-dialog","id","sx-add","aria-labelledby","sx-add-title"); ui_open_a(stdout,"form","method","get","action","/studio/"); ui_open(stdout,"header",NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"p","sx-kicker"); fputs("New source",stdout); ui_close(stdout,"p"); ui_open_a(stdout,"h2","id","sx-add-title"); fputs("Add a feed or page",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p",NULL); fputs("Paste an RSS, Atom or JSON Feed address, or any public page. You see it before anything is read.",stdout); ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open_a(stdout,"button","type","button","class","sx-x","data-sx-close",(const char *)NULL,"aria-label","Close"); fputs("&times;",stdout); ui_close(stdout,"button"); ui_close(stdout,"header"); ui_open_a(stdout,"label","class","sx-field","for","sx-add-url"); ui_open(stdout,"span",NULL); fputs("Feed or page address",stdout); ui_close(stdout,"span"); ui_open_a(stdout,"input","id","sx-add-url","name","foreign_url","type","url","required",(const char *)NULL,"autocomplete","url","placeholder","https://publisher.example/feed"); ui_close(stdout,"label"); ui_open(stdout,"ol","sx-steps"); ui_open(stdout,"li",NULL); ui_open(stdout,"b",NULL); fputs("Review.",stdout); ui_close(stdout,"b"); fputs(" The address is shown back to you. Nothing is fetched.",stdout); ui_close(stdout,"li"); ui_open(stdout,"li",NULL); ui_open(stdout,"b",NULL); fputs("Read.",stdout); ui_close(stdout,"b"); fputs(" With write access you ask the source for one copy, and a receipt is kept with it.",stdout); ui_close(stdout,"li"); ui_open(stdout,"li",NULL); ui_open(stdout,"b",NULL); fputs("Keep it coming.",stdout); ui_close(stdout,"b"); fputs(" When the publisher names a WebSub hub, new posts can be pushed instead of polled.",stdout); ui_close(stdout,"li"); ui_close(stdout,"ol"); ui_open(stdout,"footer",NULL); ui_open_a(stdout,"button","type","button","class","sx-btn","data-sx-close",(const char *)NULL); fputs("Cancel",stdout); ui_close(stdout,"button"); ui_open_a(stdout,"button","type","submit","class","sx-btn sx-primary"); fputs("Review this address",stdout); ui_close(stdout,"button"); ui_close(stdout,"footer"); ui_close(stdout,"form"); ui_close(stdout,"dialog");
}

/* ---------------------------------------------------------- source hub */
static void sd_chip(const char *route,const char *key,const char *value,const char *current,const char *label_family,long long n,int all){
 int on=all?!*current:!strcmp(current,value);
 fputs("<a class=\"sx-chip\" href=\"",stdout);html(route);if(!all){fputs("?",stdout);fputs(key,stdout);fputs("=",stdout);url_query_value(value);}
 fputs("\"",stdout);if(on)fputs(" aria-current=\"true\"",stdout);fputs(">",stdout);
 if(all)fputs("All sources",stdout);else sd_label(label_family);fputs(" <span>",stdout);sd_num(n);ui_close(stdout,"span"); ui_close(stdout,"a");
}
static void sd_card(sqlite3 *items,sqlite3 *frontier,const char *route,const char *source,const char *host,const char *family,long long n,const char *kind){
 sqlite3_stmt *s=NULL;
 fputs("<article class=\"sx-card\"><header><h3><a href=\"",stdout);html(route);fputs("?source=",stdout);url_query_value(source);fputs("\">",stdout);
 sd_name(frontier,host);ui_close(stdout,"a"); ui_close(stdout,"h3");
 if(*host){ui_text(stdout, "span", "sx-host", host);}
 ui_open(stdout,"span","sx-badge");sd_label(family);ui_close(stdout,"span"); ui_close(stdout,"header"); ui_open(stdout,"p","sx-meta"); ui_open(stdout,"b",NULL); fputs("Kept",stdout); ui_close(stdout,"b"); fputs(" · ",stdout);
 html(kind&&*kind?kind:"item");fputs(" · ",stdout);sd_num(n);fputs(n==1?" piece":" pieces",stdout);ui_close(stdout,"p");
 if(sqlite3_prepare_v2(items,"SELECT by FROM item WHERE source=?1 AND by<>'' AND by<>publisher GROUP BY by ORDER BY COUNT(*) DESC LIMIT 2",-1,&s,NULL)==SQLITE_OK){
  int k=0;sqlite3_bind_text(s,1,source,-1,SQLITE_TRANSIENT);
  while(sqlite3_step(s)==SQLITE_ROW){fputs(k++?", ":"<p class=\"sx-makers\">Credited: ",stdout);html(sd_col(s,0));}
  if(k)ui_close(stdout,"p");sqlite3_finalize(s);s=NULL;
 }
 if(sqlite3_prepare_v2(items,"SELECT key,title,at,first_read FROM item WHERE source=?1 ORDER BY at DESC LIMIT 1",-1,&s,NULL)==SQLITE_OK){
  sqlite3_bind_text(s,1,source,-1,SQLITE_TRANSIENT);
  if(sqlite3_step(s)==SQLITE_ROW){fputs("<a class=\"sx-latest\" href=\"",stdout);html(route);fputs("?source=",stdout);url_query_value(source);fputs("&amp;item=",stdout);url_query_value(sd_col(s,0));
   fputs("\">",stdout); ui_open(stdout,"small",NULL); fputs("Latest kept piece",stdout); ui_close(stdout,"small"); ui_open(stdout,"time",NULL);sd_ago(sqlite3_column_int64(s,2),sd_col(s,3));ui_close(stdout,"time"); ui_open(stdout,"span",NULL);html(sd_col(s,1));ui_close(stdout,"span"); ui_close(stdout,"a");}
  sqlite3_finalize(s);
 }
 fputs("<footer><a href=\"",stdout);html(route);fputs("?source=",stdout);url_query_value(source);fputs("\">",stdout);sd_icon("book");fputs("Read what was kept (",stdout);sd_num(n);fputs(")</a>",stdout);
 if(*host){fputs("<a href=\"/channels?root=",stdout);url_query_value(host);fputs("\">",stdout);sd_icon("rss");fputs("Channels</a>",stdout);}
 ui_close(stdout,"footer"); ui_close(stdout,"article");
}
static void sd_hub(const char *route,const char *host,const char *query){
 char q[121]="",fam[65]="",sort[12]="",off[12]="";unsigned offset=0;
 if(query){query_value(query,"q",q,sizeof q);query_value(query,"family",fam,sizeof fam);query_value(query,"sort",sort,sizeof sort);query_value(query,"offset",off,sizeof off);}
 if(*fam&&!sd_safe_word(fam,sizeof fam))fam[0]=0;
 if(*off&&strspn(off,"0123456789")==strlen(off))offset=(unsigned)strtoul(off,NULL,10);if(offset>100000)offset=0;
 int most=!strcmp(sort,"most");
 sqlite3 *items=sd_open_env("BONFYRE_ITEM_ARCHIVE_DB",&bf_store_policy_item_archive),*frontier=sd_open_env("BONFYRE_SOURCE_FRONTIER_DB",&bf_store_policy_source_frontier);
 long long sources=sd_one(items,"SELECT COUNT(DISTINCT source) FROM item",NULL),pieces=sd_one(items,"SELECT COUNT(*) FROM item",NULL);
 long long families=sd_one(items,"SELECT COUNT(DISTINCT family) FROM item WHERE family<>''",NULL);
 sd_page_open("Links and sources",host,route,query,"hub");
 ui_open_a(stdout,"main","class","sx","data-source-desk",(const char *)NULL); ui_open_a(stdout,"nav","class","sx-crumbs","aria-label","Breadcrumb"); ui_open_a(stdout,"a","href","/"); fputs("Aurekai",stdout); ui_close(stdout,"a"); ui_open(stdout,"span",NULL); fputs("/",stdout); ui_close(stdout,"span"); ui_open(stdout,"span",NULL); fputs("Links and sources",stdout); ui_close(stdout,"span"); ui_open(stdout,"em",NULL); fputs("from the internet to your work",stdout); ui_close(stdout,"em"); ui_close(stdout,"nav"); ui_open(stdout,"header","sx-hero"); ui_open(stdout,"div",NULL); ui_open(stdout,"h1",NULL); fputs("Find a source. ",stdout); ui_open(stdout,"em",NULL); fputs("Work with its contents.",stdout); ui_close(stdout,"em"); ui_close(stdout,"h1"); ui_open(stdout,"p",NULL); fputs("Browse the sources Aurekai already keeps, read what was retained from each, and carry a piece into your own workspace.",stdout); ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open_a(stdout,"nav","class","sx-actions","aria-label","Continue"); ui_open_a(stdout,"a","class","sx-btn","href","/apps?context=self");sd_icon("briefcase");
 fputs("Your work</a><a class=\"sx-btn\" href=\"/s/sets/all\">",stdout);sd_icon("board");fputs("Source collections</a>"
       "<a class=\"sx-btn sx-primary\" href=\"/studio/\" data-sx-open=\"sx-add\">",stdout);sd_icon("plus");fputs("Add a feed</a></nav></header>",stdout);
 /* Search and the facets a person narrows by. */
 ui_open_a(stdout,"section","class","sx-panel sx-finder","aria-label","Find a source"); ui_open(stdout,"div","sx-modes"); ui_open_a(stdout,"span","aria-current","true");sd_icon("search");
 fputs("Browse known sources</span><button type=\"button\" data-sx-open=\"sx-add\">",stdout);sd_icon("link");fputs("Bring a web address</button>"
       "<p class=\"sx-live\">",stdout);sd_num(sources);fputs(" sources kept · ",stdout);sd_num(pieces);fputs(" pieces</p></div>",stdout);
 fputs("<form class=\"sx-search\" method=\"get\" action=\"",stdout);html(route);fputs("\" role=\"search\"><label class=\"sx-visually-hidden\" for=\"sx-q\">Search sources</label>"
       "<input id=\"sx-q\" name=\"q\" type=\"search\" maxlength=\"120\" placeholder=\"Search by publisher, host, credited name or title\" value=\"",stdout);html(q);fputs("\">",stdout);
 if(*fam){ui_open_a(stdout,"input","type","hidden","name","family",UI_DYN,"value","",(fam));}
 ui_open_a(stdout,"button","class","sx-btn","type","submit"); fputs("Find",stdout); ui_close(stdout,"button"); ui_close(stdout,"form"); ui_open_a(stdout,"nav","class","sx-chips","aria-label","Kinds of source");
 sd_chip(route,"family","",fam,"",sources,1);
 {sqlite3_stmt *s=NULL;if(items&&sqlite3_prepare_v2(items,"SELECT family,COUNT(DISTINCT source) FROM item WHERE family<>'' GROUP BY family ORDER BY 2 DESC LIMIT 8",-1,&s,NULL)==SQLITE_OK){
  while(sqlite3_step(s)==SQLITE_ROW)sd_chip(route,"family",sd_col(s,0),fam,sd_col(s,0),sqlite3_column_int64(s,1),0);sqlite3_finalize(s);}}
 ui_close(stdout,"nav"); ui_close(stdout,"section");
 /* How a source comes to be here: the frontier's own standing counts. */
 {long long admitted=sd_one(frontier,"SELECT COUNT(*) FROM channel WHERE state='admitted'",NULL),candidate=sd_one(frontier,"SELECT COUNT(*) FROM channel WHERE state='candidate'",NULL);
  long long refused=sd_one(frontier,"SELECT COUNT(*) FROM channel WHERE refusal<>''",NULL),roots=sd_one(frontier,"SELECT COUNT(*) FROM root",NULL);
  ui_open_a(stdout,"section","class","sx-panel sx-policy","aria-labelledby","sx-policy-title"); ui_open(stdout,"div",NULL); ui_open(stdout,"p","sx-kicker"); fputs("How a source gets here",stdout); ui_close(stdout,"p"); ui_open_a(stdout,"h2","id","sx-policy-title"); fputs("Listed, admitted, then read",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p",NULL); fputs("Publishers are found from public listings and feeds. A channel is read only once it is admitted; everything refused keeps its reason.",stdout); ui_close(stdout,"p"); ui_open(stdout,"dl","sx-tiles"); ui_open(stdout,"div",NULL); ui_open(stdout,"dt",NULL); fputs("Kept sources",stdout); ui_close(stdout,"dt"); ui_open(stdout,"dd",NULL);sd_num(sources);ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Kinds"); ui_open(stdout,"dd",NULL);sd_num(families);
  ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Publishers listed"); ui_open(stdout,"dd",NULL);sd_num(roots);ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_close(stdout,"dl"); ui_close(stdout,"div"); ui_open(stdout,"ul","sx-rules"); ui_open(stdout,"li",NULL); ui_open_a(stdout,"a","href","/channels?standing=admitted"); ui_open(stdout,"code",NULL); fputs("admitted",stdout); ui_close(stdout,"code"); ui_open(stdout,"span",NULL); fputs("Channels read here on their own cadence",stdout); ui_close(stdout,"span"); ui_open(stdout,"b",NULL);sd_num(admitted);
  ui_close(stdout,"b"); ui_close(stdout,"a"); ui_close(stdout,"li"); ui_open(stdout,"li",NULL); ui_open_a(stdout,"a","href","/channels?standing=candidate"); ui_open(stdout,"code",NULL); fputs("candidate",stdout); ui_close(stdout,"code"); ui_open(stdout,"span",NULL); fputs("Listed and waiting to be looked at",stdout); ui_close(stdout,"span"); ui_open(stdout,"b",NULL);sd_num(candidate);
  ui_close(stdout,"b"); ui_close(stdout,"a"); ui_close(stdout,"li"); ui_open(stdout,"li",NULL); ui_open_a(stdout,"a","href","/channels"); ui_open(stdout,"code",NULL); fputs("refused",stdout); ui_close(stdout,"code"); ui_open(stdout,"span",NULL); fputs("Turned away, each with the reason kept",stdout); ui_close(stdout,"span"); ui_open(stdout,"b",NULL);sd_num(refused);ui_close(stdout,"b"); ui_close(stdout,"a"); ui_close(stdout,"li"); ui_close(stdout,"ul"); ui_close(stdout,"section");}
 /* The catalogue itself. */
 {sqlite3_stmt *s=NULL;char like[130];int shown=0;long long total;snprintf(like,sizeof like,"%%%s%%",q);
  const char *filter="(?1='' OR family=?1) AND (?2='' OR publisher LIKE ?3 OR source LIKE ?3 OR title LIKE ?3 OR by LIKE ?3)";
  char sql[640];snprintf(sql,sizeof sql,"SELECT COUNT(DISTINCT source) FROM item WHERE %s",filter);
  total=0;if(items&&sqlite3_prepare_v2(items,sql,-1,&s,NULL)==SQLITE_OK){sqlite3_bind_text(s,1,fam,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,q,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,like,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW)total=sqlite3_column_int64(s,0);sqlite3_finalize(s);s=NULL;}
  ui_open_a(stdout,"section","class","sx-catalogue","aria-labelledby","sx-cat-title"); ui_open(stdout,"header","sx-section-head"); ui_open(stdout,"div",NULL); ui_open_a(stdout,"h2","id","sx-cat-title");
  if(*fam){sd_label(fam);fputs(" sources",stdout);}else fputs("Kept sources",stdout);
  ui_close(stdout,"h2"); ui_open(stdout,"p",NULL);sd_num(total);fputs(total==1?" source":" sources",stdout);if(*q){fputs(" matching “",stdout);html(q);fputs("”",stdout);}
  fputs(". Every piece links back to the publisher that printed it.</p></div><nav class=\"sx-sort\" aria-label=\"Sort sources\"><span>Sort</span>",stdout);
  for(int by_count=0;by_count<2;by_count++){
   fputs("<a class=\"sx-chip\" href=\"",stdout);html(route);fputs(by_count?"?sort=most":"?sort=recent",stdout);
   if(*q){fputs("&amp;q=",stdout);url_query_value(q);}if(*fam){fputs("&amp;family=",stdout);url_query_value(fam);}
   fputs("\"",stdout);if(most==by_count)fputs(" aria-current=\"true\"",stdout);
   fputs(by_count?">Most kept</a>":">Most recent</a>",stdout);
  }
  ui_close(stdout,"nav"); ui_close(stdout,"header"); ui_open(stdout,"div","sx-grid");
  snprintf(sql,sizeof sql,"SELECT source,MAX(publisher),MAX(family),COUNT(*) n,MAX(at) last,MAX(kind) FROM item WHERE %s GROUP BY source ORDER BY %s LIMIT %d OFFSET ?4",filter,most?"n DESC,last DESC":"MAX(first_read) DESC,n DESC",SD_LIST);
  if(items&&sqlite3_prepare_v2(items,sql,-1,&s,NULL)==SQLITE_OK){
   sqlite3_bind_text(s,1,fam,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,q,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,like,-1,SQLITE_TRANSIENT);sqlite3_bind_int(s,4,(int)offset);
   while(sqlite3_step(s)==SQLITE_ROW){char src[160],pub[160],family[80],kind[40];
    snprintf(src,sizeof src,"%s",sd_col(s,0));snprintf(pub,sizeof pub,"%s",sd_col(s,1));snprintf(family,sizeof family,"%s",sd_col(s,2));snprintf(kind,sizeof kind,"%s",sd_col(s,5));
    sd_card(items,frontier,route,src,pub,family,sqlite3_column_int64(s,3),kind);shown++;}
   sqlite3_finalize(s);
  }
  if(!shown){ui_open(stdout,"p","sx-empty");fputs(items?"No kept source matches that. Try a host name, or bring the address yourself.":"The item archive is not connected here, so no kept source can be listed.",stdout);ui_close(stdout,"p");}
  ui_close(stdout,"div");
  if(offset||offset+SD_LIST<(unsigned long long)total){ui_open_a(stdout,"nav","class","sx-pager","aria-label","More sources");
   for(int dir=-1;dir<=1;dir+=2){long long to=(long long)offset+dir*SD_LIST;if(to<0||to>=total)continue;
    fputs("<a class=\"sx-btn\" href=\"",stdout);html(route);printf("?offset=%lld",to);if(*q){fputs("&amp;q=",stdout);url_query_value(q);}if(*fam){fputs("&amp;family=",stdout);url_query_value(fam);}if(most)fputs("&amp;sort=most",stdout);
    fputs(dir<0?"\">Earlier sources</a>":"\">More sources</a>",stdout);}
   ui_close(stdout,"nav");}
  ui_close(stdout,"section");}
 ui_open(stdout,"aside","sx-panel sx-notice"); ui_open(stdout,"p","sx-kicker"); fputs("About these names",stdout); ui_close(stdout,"p"); ui_open(stdout,"blockquote",NULL); fputs("A name here is what a publisher printed. It is not an account, a verified identity or consent, and the same name at two sources is not taken to be one person.",stdout); ui_close(stdout,"blockquote"); ui_open(stdout,"nav",NULL); ui_open_a(stdout,"a","class","sx-btn","href","/s/sets/all"); fputs("Explore working sets",stdout); ui_close(stdout,"a"); ui_open_a(stdout,"a","class","sx-btn","href","/sources/web"); fputs("Web research",stdout); ui_close(stdout,"a"); ui_open_a(stdout,"a","class","sx-btn sx-primary","href","/connections"); fputs("Your connections",stdout); ui_close(stdout,"a"); ui_close(stdout,"nav"); ui_close(stdout,"aside");
 if(session_institution_authorized(page_request_bearer,page_request_auth_db,"bonfyre","admin")){
  ui_open(stdout,"p","sx-operator"); ui_open_a(stdout,"a","href","/sources/push"); fputs("WebSub push delivery",stdout); ui_close(stdout,"a"); fputs(" · leases, challenges and deliveries for the sources pushed to this estate",stdout); ui_close(stdout,"p");}
 ui_close(stdout,"main");sd_add_dialog();
 if(items)sqlite3_close(items);if(frontier)sqlite3_close(frontier);sd_page_close();
}

/* ----------------------------------------------- reading room and reader */
/* One kept piece, read in place.  The archive keeps a title and the summary
 * the publisher sent, never the whole page, so the piece always ends at the
 * publisher's own address. */
static void sd_reading(sqlite3 *items,sqlite3 *frontier,const char *source,const char *key,const char *back){
 sqlite3_stmt *s=NULL;
 if(!items||sqlite3_prepare_v2(items,"SELECT title,body,href,image,by,publisher,family,kind,at,first_read FROM item WHERE source=?1 AND key=?2",-1,&s,NULL)!=SQLITE_OK)return;
 sqlite3_bind_text(s,1,source,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,key,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(s)!=SQLITE_ROW){sqlite3_finalize(s);ui_open(stdout,"article","sx-reading"); ui_text(stdout,"p","sx-empty","That piece is no longer kept here."); ui_close(stdout,"article");return;}
 const char *title=sd_col(s,0),*body=sd_col(s,1),*href=sd_col(s,2),*image=sd_col(s,3),*by=sd_col(s,4),*pub=sd_col(s,5);
 fputs("<article class=\"sx-reading\" id=\"sx-reading\"><dl class=\"sx-provenance\"><div><dt>Kept from</dt><dd><a href=\"",stdout);html(back);fputs("?source=",stdout);url_query_value(source);fputs("\">",stdout);sd_name(frontier,pub);
 ui_close(stdout,"a"); ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"First read"); ui_open(stdout,"dd",NULL);html(sd_col(s,9));ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Kind"); ui_open(stdout,"dd",NULL);sd_label(sd_col(s,6));fputs(" · ",stdout);html(sd_col(s,7));ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_close(stdout,"dl");
 ui_open(stdout,"h2",NULL);html(title);ui_close(stdout,"h2"); ui_open(stdout,"p","sx-byline");if(*by){fputs("By ",stdout);html(by);fputs(" · ",stdout);}sd_ago(sqlite3_column_int64(s,8),sd_col(s,9));ui_close(stdout,"p");
 if(public_http_url(image)){ui_open(stdout,"figure",NULL); ui_open_a(stdout,"img","loading","lazy","alt","",UI_DYN,"src","",(image)); ui_close(stdout,"figure");}
 if(*body){ui_open(stdout,"div","sx-body"); ui_open(stdout,"p",NULL);html(body);ui_close(stdout,"p"); ui_close(stdout,"div");}
 else ui_text(stdout, "p", "sx-quiet", "The publisher sent a title only. The piece itself is at their address.");
 ui_open(stdout,"footer","sx-reading-actions");
 ui_open_a(stdout,"button","type","button","class","sx-btn","data-ak-track",(const char *)NULL,"data-narrate","#sx-reading",UI_DYN,"data-title","",(title),UI_DYN,"data-by","",(*by?by:pub));sd_icon("listen");fputs("Listen</button>",stdout);
 if(public_http_url(href)){ui_open_a(stdout,"a","class","sx-btn sx-primary","rel","nofollow noopener",UI_DYN,"href","",(href));sd_icon("link");fputs("Read at the publisher</a>",stdout);}
 fputs("<a class=\"sx-btn\" href=\"/o/",stdout);url_query_value(source);fputs("/",stdout);url_query_value(key);fputs("\">Item page</a>",stdout);
 fputs("<a class=\"sx-btn\" href=\"/feed-use?context=self&amp;item=",stdout);url_query_value(source);fputs("%2F",stdout);url_query_value(key);fputs("\">",stdout);sd_icon("briefcase");fputs("Use in your work</a></footer></article>",stdout);
 sqlite3_finalize(s);
}
static void sd_register_item(sqlite3_stmt *s,const char *route,const char *param_source,const char *selected_source,const char *selected_key,int show_source,sqlite3 *frontier){
 const char *src=sd_col(s,0),*key=sd_col(s,1);int on=!strcmp(src,selected_source)&&!strcmp(key,selected_key);
 fputs("<li><a class=\"sx-dispatch\" href=\"",stdout);html(route);fputs("?",stdout);
 if(param_source&&*param_source){fputs("source=",stdout);url_query_value(param_source);fputs("&amp;item=",stdout);url_query_value(key);}
 else{fputs("read=",stdout);url_query_value(src);fputs("%2F",stdout);url_query_value(key);}
 fputs("\"",stdout);if(on)fputs(" aria-current=\"true\"",stdout);fputs("><small>",stdout);
 if(show_source){sd_name(frontier,sd_col(s,5));if(*sd_col(s,4)){fputs(" · ",stdout);html(sd_col(s,4));}}else html(*sd_col(s,4)?sd_col(s,4):sd_col(s,5));
 ui_open(stdout,"time",NULL);sd_ago(sqlite3_column_int64(s,6),sd_col(s,7));ui_close(stdout,"time"); ui_close(stdout,"small"); ui_open(stdout,"b",NULL);html(sd_col(s,2));ui_close(stdout,"b");
 if(*sd_col(s,3)){ui_text(stdout, "span", NULL, sd_col(s,3));}
 ui_open(stdout,"em",NULL);sd_label(sd_col(s,8));ui_close(stdout,"em"); ui_close(stdout,"a"); ui_close(stdout,"li");
}
static void sd_room(const char *route,const char *host,const char *query,const char *source){
 char key[64]="",q[121]="",like[130];sqlite3_stmt *s=NULL;
 if(query){query_value(query,"item",key,sizeof key);query_value(query,"q",q,sizeof q);}
 snprintf(like,sizeof like,"%%%s%%",q);
 sqlite3 *items=sd_open_env("BONFYRE_ITEM_ARCHIVE_DB",&bf_store_policy_item_archive),*frontier=sd_open_env("BONFYRE_SOURCE_FRONTIER_DB",&bf_store_policy_source_frontier);
 char pub[160]="",fam[80]="",first[32]="";long long n=0;
 if(items&&sqlite3_prepare_v2(items,"SELECT MAX(publisher),MAX(family),COUNT(*),MIN(first_read) FROM item WHERE source=?1",-1,&s,NULL)==SQLITE_OK){
  sqlite3_bind_text(s,1,source,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW){snprintf(pub,sizeof pub,"%s",sd_col(s,0));snprintf(fam,sizeof fam,"%s",sd_col(s,1));n=sqlite3_column_int64(s,2);snprintf(first,sizeof first,"%s",sd_col(s,3));}
  sqlite3_finalize(s);s=NULL;}
 if(!n){if(items)sqlite3_close(items);if(frontier)sqlite3_close(frontier);
  fputs("HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\nNothing is kept from that source.\n",stdout);return;}
 if(!*key&&sqlite3_prepare_v2(items,"SELECT key FROM item WHERE source=?1 ORDER BY at DESC LIMIT 1",-1,&s,NULL)==SQLITE_OK){
  sqlite3_bind_text(s,1,source,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW)snprintf(key,sizeof key,"%s",sd_col(s,0));sqlite3_finalize(s);s=NULL;}
 sd_page_open("Reading room",host,route,query,"room");
 ui_open(stdout,"main","sx"); ui_open_a(stdout,"nav","class","sx-crumbs","aria-label","Breadcrumb"); ui_open_a(stdout,"a","href","/"); fputs("Aurekai",stdout); ui_close(stdout,"a"); ui_open(stdout,"span",NULL); fputs("/",stdout); ui_close(stdout,"span"); ui_open_a(stdout,"a",UI_DYN,"href","",(route)); fputs("Links and sources",stdout); ui_close(stdout,"a"); ui_open(stdout,"span",NULL); fputs("/",stdout); ui_close(stdout,"span"); ui_open(stdout,"span",NULL);sd_name(frontier,pub);ui_close(stdout,"span"); ui_close(stdout,"nav");
 ui_open(stdout,"header","sx-hero sx-hero-room"); ui_open(stdout,"div",NULL); ui_open(stdout,"h1",NULL);sd_name(frontier,pub);fputs(" <em>· Reading room</em></h1><p>",stdout);sd_num(n);fputs(n==1?" piece":" pieces",stdout);
 fputs(" kept from ",stdout);html(*pub?pub:source);if(*first){fputs(" since ",stdout);fwrite(first,1,strlen(first)>10?10:strlen(first),stdout);}fputs(", in plain type. Each one ends at the publisher's own address.</p></div><nav class=\"sx-actions\">",stdout);
 if(*pub){fputs("<a class=\"sx-btn\" href=\"/channels?root=",stdout);url_query_value(pub);fputs("\">",stdout);sd_icon("rss");fputs("Every channel from ",stdout);html(pub);ui_close(stdout,"a");}
 fputs("<a class=\"sx-btn sx-primary\" href=\"",stdout);html(route);fputs("?family=",stdout);url_query_value(fam);fputs("\">More ",stdout);sd_label(fam);fputs(" sources</a></nav></header>",stdout);
 fputs("<form class=\"sx-search sx-room-filter\" method=\"get\" action=\"",stdout);html(route);fputs("\" role=\"search\"><input type=\"hidden\" name=\"source\" value=\"",stdout);html(source);
 fputs("\"><label class=\"sx-visually-hidden\" for=\"sx-rq\">Filter kept pieces</label><input id=\"sx-rq\" name=\"q\" type=\"search\" maxlength=\"120\" placeholder=\"Filter kept pieces by title or name\" value=\"",stdout);html(q);fputs("\"><button class=\"sx-btn\" type=\"submit\">Filter</button></form>",stdout);
 ui_open(stdout,"div","sx-split"); ui_open_a(stdout,"section","class","sx-register","aria-labelledby","sx-reg"); ui_open_a(stdout,"h2","id","sx-reg"); fputs("Kept pieces",stdout); ui_close(stdout,"h2"); ui_open(stdout,"ol",NULL);
 if(sqlite3_prepare_v2(items,"SELECT source,key,title,substr(body,1,180),by,publisher,at,first_read,family FROM item WHERE source=?1 AND (?2='' OR title LIKE ?3 OR by LIKE ?3) ORDER BY at DESC LIMIT 40",-1,&s,NULL)==SQLITE_OK){
  int k=0;sqlite3_bind_text(s,1,source,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,q,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,like,-1,SQLITE_TRANSIENT);
  while(sqlite3_step(s)==SQLITE_ROW){sd_register_item(s,route,source,source,key,0,frontier);k++;}
  if(!k)ui_text(stdout, "li", "sx-empty", "No kept piece matches that.");sqlite3_finalize(s);s=NULL;}
 ui_close(stdout,"ol"); ui_close(stdout,"section");
 if(*key)sd_reading(items,frontier,source,key,route);
 ui_close(stdout,"div"); ui_close(stdout,"main");
 if(items)sqlite3_close(items);if(frontier)sqlite3_close(frontier);sd_page_close();
}

/* /channels with nothing chosen: the living reader across every kept source.
 * The publisher ledger cuts (?root= ?standing= ...) stay with SourceDepth. */
static void sd_reader(const char *route,const char *host,const char *query){
 char read[200]="",feed[65]="",src[160]="",key[64]="";sqlite3_stmt *s=NULL;
 if(query){query_value(query,"read",read,sizeof read);query_value(query,"feed",feed,sizeof feed);}
 if(*feed&&!sd_safe_word(feed,sizeof feed))feed[0]=0;
 {char *slash=strrchr(read,'/');if(slash&&slash!=read&&slash[1]&&(size_t)(slash-read)<sizeof src){memcpy(src,read,(size_t)(slash-read));src[slash-read]=0;snprintf(key,sizeof key,"%s",slash+1);}}
 sqlite3 *items=sd_open_env("BONFYRE_ITEM_ARCHIVE_DB",&bf_store_policy_item_archive),*frontier=sd_open_env("BONFYRE_SOURCE_FRONTIER_DB",&bf_store_policy_source_frontier);
 long long day=sd_one(items,"SELECT COUNT(*) FROM item WHERE first_read>=strftime('%Y-%m-%dT%H:%M:%SZ','now','-1 day')",NULL);
 long long all=sd_one(items,"SELECT COUNT(*) FROM item WHERE at>0",NULL);
 if(!*src&&items&&sqlite3_prepare_v2(items,"SELECT source,key FROM item WHERE at>0 AND at<=CAST(strftime('%s','now') AS INTEGER) AND (?1='' OR family=?1) ORDER BY at DESC LIMIT 1",-1,&s,NULL)==SQLITE_OK){
  sqlite3_bind_text(s,1,feed,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW){snprintf(src,sizeof src,"%s",sd_col(s,0));snprintf(key,sizeof key,"%s",sd_col(s,1));}sqlite3_finalize(s);s=NULL;}
 sd_page_open("Living feeds",host,route,query,"reader");
 ui_open(stdout,"main","sx"); ui_open_a(stdout,"nav","class","sx-crumbs","aria-label","Breadcrumb"); ui_open_a(stdout,"a","href","/"); fputs("Aurekai",stdout); ui_close(stdout,"a"); ui_open(stdout,"span",NULL); fputs("/",stdout); ui_close(stdout,"span"); ui_open(stdout,"span",NULL); fputs("Feeds",stdout); ui_close(stdout,"span"); ui_open(stdout,"em",NULL); fputs("reading stream",stdout); ui_close(stdout,"em"); ui_close(stdout,"nav"); ui_open(stdout,"header","sx-hero sx-hero-reader"); ui_open(stdout,"div",NULL); ui_open(stdout,"h1",NULL); fputs("Living feeds",stdout); ui_close(stdout,"h1"); ui_open(stdout,"p",NULL); fputs("The newest pieces from every source Aurekai reads, in plain type. Nothing here tracks you, and every piece ends at its publisher.",stdout); ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open(stdout,"nav","sx-actions"); ui_open_a(stdout,"a","class","sx-btn","href","/sources");sd_icon("search");fputs("Find a source</a><a class=\"sx-btn\" href=\"/channels?roots=1\">",stdout);sd_icon("rss");
 fputs("Every publisher</a><a class=\"sx-btn sx-primary\" href=\"/studio/\" data-sx-open=\"sx-add\">",stdout);sd_icon("plus");fputs("Add a feed</a></nav></header><nav class=\"sx-chips\" aria-label=\"Feeds\">",stdout);
 fputs("<a class=\"sx-chip\" href=\"",stdout);html(route);fputs("\"",stdout);if(!*feed)fputs(" aria-current=\"true\"",stdout);fputs(">All feeds <span>",stdout);sd_num(all);ui_close(stdout,"span"); ui_close(stdout,"a");
 if(items&&sqlite3_prepare_v2(items,"SELECT family,COUNT(*) FROM item WHERE family<>'' AND at>0 GROUP BY family ORDER BY 2 DESC LIMIT 6",-1,&s,NULL)==SQLITE_OK){
  while(sqlite3_step(s)==SQLITE_ROW){fputs("<a class=\"sx-chip\" href=\"",stdout);html(route);fputs("?feed=",stdout);url_query_value(sd_col(s,0));fputs("\"",stdout);if(!strcmp(feed,sd_col(s,0)))fputs(" aria-current=\"true\"",stdout);
   fputs(">",stdout);sd_label(sd_col(s,0));fputs(" <span>",stdout);sd_num(sqlite3_column_int64(s,1));ui_close(stdout,"span"); ui_close(stdout,"a");}
  sqlite3_finalize(s);s=NULL;}
 ui_close(stdout,"nav"); ui_open(stdout,"div","sx-split sx-three"); ui_open_a(stdout,"section","class","sx-register","aria-labelledby","sx-reg"); ui_open_a(stdout,"h2","id","sx-reg"); fputs("Dispatches ",stdout); ui_open(stdout,"span",NULL);if(day>0){sd_num(day);fputs(" kept in the last day",stdout);}else fputs("newest first",stdout);ui_close(stdout,"span"); ui_close(stdout,"h2"); ui_open(stdout,"ol",NULL);
 if(items&&sqlite3_prepare_v2(items,"SELECT source,key,title,substr(body,1,140),by,publisher,at,first_read,family FROM item WHERE at>0 AND at<=CAST(strftime('%s','now') AS INTEGER) AND (?1='' OR family=?1) ORDER BY at DESC LIMIT 30",-1,&s,NULL)==SQLITE_OK){
  int k=0;sqlite3_bind_text(s,1,feed,-1,SQLITE_TRANSIENT);while(sqlite3_step(s)==SQLITE_ROW){sd_register_item(s,route,NULL,src,key,1,frontier);k++;}
  if(!k)ui_text(stdout, "li", "sx-empty", "Nothing has been kept for this feed yet.");sqlite3_finalize(s);s=NULL;}
 ui_close(stdout,"ol"); ui_close(stdout,"section");
 if(*src)sd_reading(items,frontier,src,key,"/sources");else do{ ui_open(stdout,"article","sx-reading"); ui_text(stdout,"p","sx-empty","No piece is kept yet. Add a feed to start the stream."); ui_close(stdout,"article"); }while(0);
 /* The rail: what else this source holds and where the piece can go. */
 ui_open_a(stdout,"aside","class","sx-rail","aria-label","About this source");
 if(*src&&items&&sqlite3_prepare_v2(items,"SELECT MAX(publisher),COUNT(*),MAX(family) FROM item WHERE source=?1",-1,&s,NULL)==SQLITE_OK){
  sqlite3_bind_text(s,1,src,-1,SQLITE_TRANSIENT);
  if(sqlite3_step(s)==SQLITE_ROW){char pub[160];snprintf(pub,sizeof pub,"%s",sd_col(s,0));long long n=sqlite3_column_int64(s,1);
   ui_open(stdout,"section","sx-panel"); ui_text(stdout,"p","sx-kicker","This source"); ui_open(stdout,"h2",NULL);sd_name(frontier,pub);ui_close(stdout,"h2"); ui_open(stdout,"p",NULL);sd_num(n);fputs(n==1?" piece kept":" pieces kept",stdout);fputs(" · ",stdout);sd_label(sd_col(s,2));
   fputs("</p><a class=\"sx-btn\" href=\"/sources?source=",stdout);url_query_value(src);fputs("\">Open its reading room</a></section>",stdout);
   sqlite3_finalize(s);s=NULL;
   if(sqlite3_prepare_v2(items,"SELECT key,title,at,first_read FROM item WHERE source=?1 AND key<>?2 ORDER BY at DESC LIMIT 4",-1,&s,NULL)==SQLITE_OK){int k=0;
    sqlite3_bind_text(s,1,src,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,key,-1,SQLITE_TRANSIENT);
    while(sqlite3_step(s)==SQLITE_ROW){if(!k++)do{ ui_open(stdout,"section","sx-panel"); ui_text(stdout,"p","sx-kicker","Also from here"); ui_open(stdout,"ul","sx-also"); }while(0);
     fputs("<li><a href=\"",stdout);html(route);fputs("?read=",stdout);url_query_value(src);fputs("%2F",stdout);url_query_value(sd_col(s,0));fputs("\">",stdout);html(sd_col(s,1));ui_close(stdout,"a"); ui_open(stdout,"time",NULL);sd_ago(sqlite3_column_int64(s,2),sd_col(s,3));ui_close(stdout,"time"); ui_close(stdout,"li");}
    if(k)do{ ui_close(stdout,"ul"); ui_close(stdout,"section"); }while(0);}
  }
  if(s)sqlite3_finalize(s);s=NULL;}
 ui_open(stdout,"section","sx-panel"); ui_open(stdout,"p","sx-kicker"); fputs("Agents and tools",stdout); ui_close(stdout,"p"); ui_open(stdout,"p",NULL); fputs("Your own tools can read kept sources through the Aurekai MCP and A2A carriers, with the same limits this page has.",stdout); ui_close(stdout,"p"); ui_open_a(stdout,"a","class","sx-btn","href","/connect"); fputs("Connect a tool",stdout); ui_close(stdout,"a"); ui_close(stdout,"section"); ui_close(stdout,"aside"); ui_close(stdout,"div"); ui_close(stdout,"main");
 sd_add_dialog();
 if(items)sqlite3_close(items);if(frontier)sqlite3_close(frontier);sd_page_close();
}

/* ---------------------------------------------------------- connections */
typedef struct{char id[16],provider[64],account[256],url[512],label[256],state[32],updated[40];}SdLink;
static int sd_links(const char *bearer,const char *auth_db,SdLink *out,int cap){
 static char buf[16384];int n=0;
 if(!bearer||!*bearer||!auth_db||!session_valid(bearer,auth_db))return -1;
 const char *bin=auth_binary();if(!bin)return 0;
 char *const argv[]={(char*)bin,"--db",(char*)auth_db,"external-link-list","--token",(char*)bearer,NULL};
 if(auth_call(argv,NULL,buf,sizeof buf)!=0)return 0;
 bf_json_doc_t *d=bf_json_parse(buf,strlen(buf),NULL,0);if(!d)return 0;
 const bf_json_node_t *r=bf_json_root(d),*rows=r?bf_json_obj_get(d,r,"links"):NULL;
 for(int i=0;rows&&rows->type==BF_JSON_ARRAY&&i<rows->child_count&&n<cap;i++){
  const bf_json_node_t *row=bf_json_arr_get(d,rows,i),*f;SdLink *l=&out[n];memset(l,0,sizeof *l);
  if(!row||row->type!=BF_JSON_OBJECT)continue;
  if((f=bf_json_obj_get(d,row,"id"))&&f->type==BF_JSON_INT)snprintf(l->id,sizeof l->id,"%lld",(long long)f->val.i);
  if((f=bf_json_obj_get(d,row,"provider")))bf_json_get_str_copy(f,l->provider,sizeof l->provider);
  if((f=bf_json_obj_get(d,row,"remote_account")))bf_json_get_str_copy(f,l->account,sizeof l->account);
  if((f=bf_json_obj_get(d,row,"profile_url")))bf_json_get_str_copy(f,l->url,sizeof l->url);
  if((f=bf_json_obj_get(d,row,"display_name")))bf_json_get_str_copy(f,l->label,sizeof l->label);
  if((f=bf_json_obj_get(d,row,"proof_state")))bf_json_get_str_copy(f,l->state,sizeof l->state);
  if((f=bf_json_obj_get(d,row,"updated_at")))bf_json_get_str_copy(f,l->updated,sizeof l->updated);
  if(l->id[0]&&strcmp(l->state,"revoked"))n++;
 }
 bf_json_free(d);return n;
}
typedef struct{const char *key,*name,*host,*glyph,*plane,*what,*href,*action;int declared;}SdProvider;
static const SdProvider sd_providers[]={
 {"gmail","Gmail","gmail.com","text","Mail · read-only","Newsletters and subscriptions clipped into your reading room. Private threads stay out.","/connections?provider=gmail","Sync rules",0},
 {"proton","Proton Mail","proton.me","text","Mail · bridge","Archival newsletters and dispatch lists from a Proton mailbox, through the same studio.","/apps/connections?context=self&identity=proton","Open studio",0},
 {"github","GitHub","github.com","tool","Code · app install","Release notes, commits and discussions from repositories you install the Aurekai app on.","/connect/github-app","Install app",0},
 {"bluesky","Bluesky","bsky.app","public","AT Protocol · declared","Your Bluesky handle, declared on your account so your pages and posts can point to it.","/connections?provider=bluesky","Link handle",1},
 {"mastodon","Mastodon / ActivityPub","fediverse","world","ActivityPub · declared","A fediverse actor you name as yours. Aurekai answers WebFinger and ActivityPub for your own address.","/connections?provider=mastodon","Link actor",1},
};
static void sd_link_form(const char *provider,const char *placeholder,const char *profile){
 fputs("<form class=\"sx-form\" method=\"post\" action=\"/account/connections\"><input type=\"hidden\" name=\"provider\" value=\"",stdout);html(provider);
 fputs("\"><label class=\"sx-field\"><span>Handle or account</span><input name=\"remote_account\" required maxlength=\"240\" placeholder=\"",stdout);html(placeholder);
 fputs("\"></label><label class=\"sx-field\"><span>Public profile address</span><input name=\"profile_url\" type=\"url\" required pattern=\"https://.*\" placeholder=\"",stdout);html(profile);
 fputs("\"></label><label class=\"sx-field\"><span>Name to show (optional)</span><input name=\"display_name\" maxlength=\"240\"></label><button class=\"sx-btn sx-primary\" type=\"submit\">Declare this link</button>"
       "<p class=\"sx-quiet\">A declared link is a public pointer you state. It does not prove you control the account or import anything from it.</p></form>",stdout);
}
static void sd_revoke(const SdLink *l){
 const FtField hidden[]={{"link_id",l->id}};
 const FtSubmit go[]={{.label="Remove",.tone="quiet"}};
 const FtForm f={.id="sd-revoke",.action="/account/connections/revoke",.hook="connection-revoke",.hidden=hidden,.nhidden=1,.submits=go,.nsubmits=1};
 ft_form(stdout,&f);
}
static void sd_connections(const char *route,const char *host,const char *query,const char *bearer,const char *auth_db){
 char provider[24]="";if(query)query_value(query,"provider",provider,sizeof provider);
 SdLink links[24];int nl=sd_links(bearer,auth_db,links,24),signed_in=nl>=0;if(nl<0)nl=0;
 const SdProvider *chosen=NULL;for(size_t i=0;i<sizeof sd_providers/sizeof sd_providers[0];i++)if(!strcmp(provider,sd_providers[i].key))chosen=&sd_providers[i];
 sqlite3 *items=sd_open_env("BONFYRE_ITEM_ARCHIVE_DB",&bf_store_policy_item_archive),*frontier=sd_open_env("BONFYRE_SOURCE_FRONTIER_DB",&bf_store_policy_source_frontier);
 sd_page_open(chosen?chosen->name:"Connections",host,route,query,"connections");
 ui_open(stdout,"main","sx"); ui_open_a(stdout,"nav","class","sx-crumbs","aria-label","Breadcrumb"); ui_open_a(stdout,"a","href","/"); fputs("Aurekai",stdout); ui_close(stdout,"a"); ui_open(stdout,"span",NULL); fputs("/",stdout); ui_close(stdout,"span");
 if(chosen){ui_open_a(stdout,"a","href","/connections"); fputs("Connections",stdout); ui_close(stdout,"a"); ui_open(stdout,"span",NULL); fputs("/",stdout); ui_close(stdout,"span"); ui_open(stdout,"span",NULL);html(chosen->name);ui_close(stdout,"span");}else ui_text(stdout, "span", NULL, "Connections");
 ui_open(stdout,"em",NULL);fputs(signed_in?"signed in":"signed out",stdout);ui_close(stdout,"em"); ui_close(stdout,"nav");
 if(!chosen){
  long long day=sd_one(items,"SELECT COUNT(*) FROM item WHERE first_read>=strftime('%Y-%m-%dT%H:%M:%SZ','now','-1 day')",NULL),sources=sd_one(items,"SELECT COUNT(DISTINCT source) FROM item",NULL);
  long long admitted=sd_one(frontier,"SELECT COUNT(*) FROM channel WHERE state='admitted'",NULL);
  ui_open(stdout,"header","sx-hero"); ui_open(stdout,"div",NULL); ui_open(stdout,"p","sx-kicker"); fputs("Your identities and what they bring in",stdout); ui_close(stdout,"p"); ui_open(stdout,"h1",NULL); fputs("Connections",stdout); ui_close(stdout,"h1"); ui_open(stdout,"p",NULL); fputs("The accounts you link, the mail you let in and the feeds you follow, kept in your own workspace. Aurekai reads only what you point it at.",stdout); ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open(stdout,"nav","sx-actions"); ui_open_a(stdout,"a","class","sx-btn sx-primary","href","#sx-link","data-sx-open","sx-link");sd_icon("plus");
  fputs("Link an identity</a><a class=\"sx-btn\" href=\"/sources\" >",stdout);sd_icon("rss");fputs("Sources</a><a class=\"sx-btn\" href=\"/connect\">",stdout);sd_icon("robot");fputs("Tools and agents</a></nav></header>",stdout);
  ui_open(stdout,"dl","sx-stats"); ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Kept in the last day"); ui_open(stdout,"dd",NULL);sd_num(day);ui_text(stdout,"small",NULL,"pieces"); ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Sources kept"); ui_open(stdout,"dd",NULL);sd_num(sources);
  ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Channels admitted"); ui_open(stdout,"dd",NULL);sd_num(admitted);ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Your linked identities"); ui_open(stdout,"dd",NULL);
  if(signed_in)printf("%d",nl);else fputs("<a href=\"/login?next=%2Fconnections\">Sign in</a>",stdout);ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_close(stdout,"dl");
  ui_open_a(stdout,"section","aria-labelledby","sx-relays"); ui_open(stdout,"header","sx-section-head"); ui_open_a(stdout,"h2","id","sx-relays"); fputs("Identity relays",stdout); ui_close(stdout,"h2"); ui_close(stdout,"header"); ui_open(stdout,"div","sx-grid");
  for(size_t i=0;i<sizeof sd_providers/sizeof sd_providers[0];i++){const SdProvider *p=&sd_providers[i];int have=0;
   for(int k=0;k<nl;k++)if(!strcmp(links[k].provider,p->key))have++;
   ui_open(stdout,"article","sx-card sx-relay"); ui_open(stdout,"header",NULL); ui_open(stdout,"span","sx-mark");sd_icon(p->glyph);ui_close(stdout,"span"); ui_open(stdout,"h3",NULL); ui_open_a(stdout,"a",UI_DYN,"href","",(p->href));html(p->name);
   ui_close(stdout,"a"); ui_close(stdout,"h3"); ui_open(stdout,"span","sx-host");html(p->host);fputs(" · ",stdout);html(p->plane);fputs("</span><span class=\"sx-badge",stdout);fputs(have?" is-on\">Declared":"\">Not linked",stdout);
   ui_close(stdout,"span"); ui_close(stdout,"header"); ui_open(stdout,"p",NULL);html(p->what);ui_close(stdout,"p");
   for(int k=0;k<nl;k++)if(!strcmp(links[k].provider,p->key)){ui_open(stdout,"p","sx-meta"); ui_open(stdout,"b",NULL);html(links[k].account);ui_close(stdout,"b"); fputs(" · ",stdout);html(links[k].state);ui_close(stdout,"p");}
   ui_open(stdout,"footer",NULL); ui_open(stdout,"span",NULL);html(p->declared?"Declared on your account":"Connected through its own owner");ui_close(stdout,"span"); ui_open_a(stdout,"a","class","sx-btn",UI_DYN,"href","",(p->href));html(p->action);ui_close(stdout,"a"); ui_close(stdout,"footer"); ui_close(stdout,"article");}
  for(int k=0;k<nl;k++){int known=0;for(size_t i=0;i<sizeof sd_providers/sizeof sd_providers[0];i++)if(!strcmp(links[k].provider,sd_providers[i].key))known=1;if(known)continue;
   ui_open(stdout,"article","sx-card sx-relay"); ui_open(stdout,"header",NULL); ui_open(stdout,"span","sx-mark");sd_icon("link");ui_close(stdout,"span"); ui_open(stdout,"h3",NULL);html(*links[k].label?links[k].label:links[k].provider);ui_close(stdout,"h3"); ui_open(stdout,"span","sx-host");html(links[k].provider);
   ui_close(stdout,"span"); ui_open(stdout,"span","sx-badge is-on");html(links[k].state);ui_close(stdout,"span"); ui_close(stdout,"header"); ui_open(stdout,"p","sx-meta"); ui_open(stdout,"b",NULL);html(links[k].account);fputs("</b></p><footer><a href=\"",stdout);if(public_http_url(links[k].url))html(links[k].url);fputs("\" rel=\"nofollow noopener\">Profile</a>",stdout);sd_revoke(&links[k]);ui_close(stdout,"footer"); ui_close(stdout,"article");}
  ui_close(stdout,"div"); ui_close(stdout,"section");
  /* What actually came in most recently, across every kept source. */
  ui_open_a(stdout,"section","class","sx-log","aria-labelledby","sx-log-title"); ui_open(stdout,"header","sx-section-head"); ui_open(stdout,"div",NULL); ui_open_a(stdout,"h2","id","sx-log-title"); fputs("Recently kept",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p",NULL); fputs("The last pieces the estate read, with the source each came from.",stdout); ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open_a(stdout,"a","class","sx-btn","href","/channels"); fputs("Open the reader",stdout); ui_close(stdout,"a"); ui_close(stdout,"header"); ui_open(stdout,"div","sx-table-wrap"); ui_open(stdout,"table","sx-table"); ui_open(stdout,"thead",NULL); ui_open(stdout,"tr",NULL); ui_open(stdout,"th",NULL); fputs("Piece",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("Source",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("Kind",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("Kept",stdout); ui_close(stdout,"th"); ui_close(stdout,"tr"); ui_close(stdout,"thead"); ui_open(stdout,"tbody",NULL);
  {sqlite3_stmt *s=NULL;int k=0;if(items&&sqlite3_prepare_v2(items,"SELECT source,key,title,by,publisher,family,first_read FROM item ORDER BY first_read DESC LIMIT 8",-1,&s,NULL)==SQLITE_OK){
   while(sqlite3_step(s)==SQLITE_ROW){k++;fputs("<tr><td><a href=\"/sources?source=",stdout);url_query_value(sd_col(s,0));fputs("&amp;item=",stdout);url_query_value(sd_col(s,1));fputs("\">",stdout);html(sd_col(s,2));ui_close(stdout,"a");
    if(*sd_col(s,3)){ui_text(stdout, "small", NULL, sd_col(s,3));}ui_close(stdout,"td"); ui_open(stdout,"td",NULL);sd_name(frontier,sd_col(s,4));ui_close(stdout,"td"); ui_open(stdout,"td",NULL); ui_open(stdout,"span","sx-badge");sd_label(sd_col(s,5));ui_close(stdout,"span"); ui_close(stdout,"td"); ui_open(stdout,"td",NULL);html(sd_col(s,6));ui_close(stdout,"td"); ui_close(stdout,"tr");}
   sqlite3_finalize(s);}
   if(!k)do{ ui_open(stdout,"tr",NULL); ui_open_a(stdout,"td","colspan","4","class","sx-empty"); fputs("Nothing has been kept yet.",stdout); ui_close(stdout,"td"); ui_close(stdout,"tr"); }while(0);}
  ui_close(stdout,"tbody"); ui_close(stdout,"table"); ui_close(stdout,"div"); ui_close(stdout,"section");
  ui_open(stdout,"aside","sx-panel sx-notice"); ui_open(stdout,"p","sx-kicker"); fputs("What a connection is",stdout); ui_close(stdout,"p"); ui_open(stdout,"blockquote",NULL); fputs("Mail connections run through their own studio and only read what its rules allow. Declared links are public pointers you state; removing one takes effect at once.",stdout); ui_close(stdout,"blockquote"); ui_open(stdout,"nav",NULL); ui_open_a(stdout,"a","class","sx-btn","href","/account"); fputs("Account settings",stdout); ui_close(stdout,"a"); ui_close(stdout,"nav"); ui_close(stdout,"aside");
  ui_close(stdout,"main"); ui_open_a(stdout,"dialog","class","sx-dialog","id","sx-link","aria-labelledby","sx-link-title"); ui_open(stdout,"header",NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"p","sx-kicker"); fputs("New identity",stdout); ui_close(stdout,"p"); ui_open_a(stdout,"h2","id","sx-link-title"); fputs("Link an identity",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p",NULL); fputs("Name an account elsewhere as yours. Mail and GitHub have their own connect steps above.",stdout); ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open_a(stdout,"button","type","button","class","sx-x","data-sx-close",(const char *)NULL,"aria-label","Close"); fputs("&times;",stdout); ui_close(stdout,"button"); ui_close(stdout,"header");
  if(signed_in){ui_open_a(stdout,"form","class","sx-form","method","post","action","/account/connections"); ui_open(stdout,"label","sx-field"); ui_open(stdout,"span",NULL); fputs("Service",stdout); ui_close(stdout,"span"); ui_open_a(stdout,"select","name","provider"); ui_open_a(stdout,"option","value","bluesky"); fputs("Bluesky",stdout); ui_close(stdout,"option"); ui_open_a(stdout,"option","value","mastodon"); fputs("Mastodon / ActivityPub",stdout); ui_close(stdout,"option"); ui_open_a(stdout,"option","value","github"); fputs("GitHub profile",stdout); ui_close(stdout,"option"); ui_open_a(stdout,"option","value","website"); fputs("Website",stdout); ui_close(stdout,"option"); ui_close(stdout,"select"); ui_close(stdout,"label"); ui_open(stdout,"label","sx-field"); ui_open(stdout,"span",NULL); fputs("Handle or account",stdout); ui_close(stdout,"span"); ui_open_a(stdout,"input","name","remote_account","required",(const char *)NULL,"maxlength","240"); ui_close(stdout,"label"); ui_open(stdout,"label","sx-field"); ui_open(stdout,"span",NULL); fputs("Public profile address",stdout); ui_close(stdout,"span"); ui_open_a(stdout,"input","name","profile_url","type","url","required",(const char *)NULL,"pattern","https://.*","placeholder","https://"); ui_close(stdout,"label"); ui_open(stdout,"label","sx-field"); ui_open(stdout,"span",NULL); fputs("Name to show (optional)",stdout); ui_close(stdout,"span"); ui_open_a(stdout,"input","name","display_name","maxlength","240"); ui_close(stdout,"label"); ui_open(stdout,"footer",NULL); ui_open_a(stdout,"button","type","button","class","sx-btn","data-sx-close",(const char *)NULL); fputs("Cancel",stdout); ui_close(stdout,"button"); ui_open_a(stdout,"button","class","sx-btn sx-primary","type","submit"); fputs("Declare this link",stdout); ui_close(stdout,"button"); ui_close(stdout,"footer"); ui_close(stdout,"form");}
  else fputs("<p>Linking an identity belongs to a signed-in account.</p><footer><button type=\"button\" class=\"sx-btn\" data-sx-close>Cancel</button><a class=\"sx-btn sx-primary\" href=\"/login?next=%2Fconnections\">Sign in</a></footer>",stdout);
  ui_close(stdout,"dialog");
 } else {
  int mail=!strcmp(chosen->key,"gmail");
  ui_open(stdout,"header","sx-hero"); ui_open(stdout,"div",NULL); ui_open(stdout,"p","sx-kicker");html(chosen->plane);ui_close(stdout,"p"); ui_open(stdout,"h1",NULL);html(chosen->name);fputs(mail?" sync settings":" link",stdout);ui_close(stdout,"h1"); ui_open(stdout,"p",NULL);html(chosen->what);ui_close(stdout,"p"); ui_close(stdout,"div");
  ui_open(stdout,"aside","sx-panel sx-invariant"); ui_open(stdout,"p","sx-kicker");fputs(mail?"Your password never comes here":"A pointer, not a login",stdout);ui_close(stdout,"p"); ui_open(stdout,"p",NULL);
  fputs(mail?"Mail is read through a read-only grant held by the Connection Studio. Aurekai never sees or stores your Google password.":"Aurekai does not sign in to this service for you. You state the account, and your own pages can point to it.",stdout);ui_close(stdout,"p"); ui_close(stdout,"aside"); ui_close(stdout,"header");
  ui_open_a(stdout,"section","class","sx-panel sx-session","aria-labelledby","sx-sess"); ui_open(stdout,"header","sx-section-head"); ui_open_a(stdout,"h2","id","sx-sess");fputs(mail?"Session and scope":"Your declared accounts",stdout);ui_close(stdout,"h2");
  if(mail){fputs("<a class=\"sx-btn sx-primary\" href=\"/apps/connections?context=self&amp;identity=gmail\">",stdout);sd_icon("activity");fputs("Open the Connection Studio</a>",stdout);}
  ui_close(stdout,"header"); ui_open(stdout,"dl","sx-stats");
  int have=0;for(int k=0;k<nl;k++)if(!strcmp(links[k].provider,chosen->key)){have++;ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Account"); ui_open(stdout,"dd",NULL);html(links[k].account);ui_open(stdout,"small",NULL);html(links[k].state);fputs(" · ",stdout);html(links[k].updated);ui_close(stdout,"small"); ui_close(stdout,"dd");sd_revoke(&links[k]);ui_close(stdout,"div");}
  if(mail)do{ ui_open(stdout,"div",NULL); ui_open(stdout,"dt",NULL); fputs("Scope",stdout); ui_close(stdout,"dt"); ui_open(stdout,"dd",NULL); ui_open(stdout,"code",NULL); fputs("gmail.readonly",stdout); ui_close(stdout,"code"); ui_open(stdout,"small",NULL); fputs("Read only. Nothing is sent or deleted.",stdout); ui_close(stdout,"small"); ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_open(stdout,"dt",NULL); fputs("Where it lands",stdout); ui_close(stdout,"dt"); ui_open(stdout,"dd",NULL); fputs("Reading room",stdout); ui_open(stdout,"small",NULL); fputs("Clipped newsletters join your source library.",stdout); ui_close(stdout,"small"); ui_close(stdout,"dd"); ui_close(stdout,"div"); }while(0);
  if(!have&&!mail){ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Account"); ui_open(stdout,"dd",NULL);fputs(signed_in?"None declared yet":"<a href=\"/login?next=%2Fconnections\">Sign in</a> to declare one",stdout);ui_close(stdout,"dd"); ui_close(stdout,"div");}
  if(mail&&!signed_in)fputs("<div><dt>Account</dt><dd><a href=\"/login?next=%2Fconnections%3Fprovider%3Dgmail\">Sign in</a><small>Mail connections belong to a signed-in account.</small></dd></div>",stdout);
  ui_close(stdout,"dl"); ui_close(stdout,"section");
  if(mail){
   fputs("<div class=\"sx-split sx-settings\"><section class=\"sx-panel\" aria-labelledby=\"sx-strat\"><p class=\"sx-kicker\">What comes in</p><h2 id=\"sx-strat\">Choose what to clip</h2>"
         "<ul class=\"sx-options\"><li><b>Newsletters and subscriptions</b><span>Messages that carry List-Unsubscribe or bulk headers. Personal threads are left alone.</span></li>"
         "<li><b>A search you write</b><span>Your own Gmail search string, such as <code>label:Newsletters OR from:*.substack.com</code>.</span></li>"
         "<li><b>Everything, filtered</b><span>Every inbound message, checked against the exclusions below before anything is kept.</span></li></ul>"
         "<p>These rules are set and saved in the Connection Studio, which owns the grant.</p><a class=\"sx-btn\" href=\"/apps/connections?context=self&amp;identity=gmail\">Set rules in the studio</a></section>"
         "<section class=\"sx-panel\" aria-labelledby=\"sx-excl\"><p class=\"sx-kicker\">Never kept</p><h2 id=\"sx-excl\">Exclusions</h2><ul class=\"sx-checks\"><li>Direct personal mail</li><li>Banking and statements</li><li>Receipts and invoices</li><li>Security codes and sign-in alerts</li></ul>"
         "<p class=\"sx-kicker\">Where it goes</p><p>Clipped issues are kept as reading-room pieces with their sender as the source.</p><a class=\"sx-btn\" href=\"/sources\">Open sources</a></section></div>",stdout);
  } else {
   ui_open_a(stdout,"section","class","sx-panel","aria-labelledby","sx-decl"); ui_open(stdout,"p","sx-kicker"); fputs("Declare",stdout); ui_close(stdout,"p"); ui_open_a(stdout,"h2","id","sx-decl"); fputs("Name your ",stdout);html(chosen->name);fputs(" account</h2>",stdout);
   if(signed_in)sd_link_form(chosen->key,!strcmp(chosen->key,"bluesky")?"you.bsky.social":"@you@instance.social",!strcmp(chosen->key,"bluesky")?"https://bsky.app/profile/you.bsky.social":"https://instance.social/@you");
   if(!strcmp(chosen->key,"bluesky"))do{ ui_open(stdout,"p","sx-note"); ui_open_a(stdout,"a","href","/connections/bluesky/sync"); fputs("Ask for a sync of your Bluesky repository",stdout); ui_close(stdout,"a"); ui_close(stdout,"p"); }while(0);
   else fputs("<p><a class=\"sx-btn sx-primary\" href=\"/login?next=%2Fconnections\">Sign in to declare an account</a></p>",stdout);
   ui_close(stdout,"section");
  }
  ui_open(stdout,"p","sx-back"); ui_open_a(stdout,"a","href","/connections"); fputs("All connections",stdout); ui_close(stdout,"a"); ui_close(stdout,"p"); ui_close(stdout,"main");
 }
 if(items)sqlite3_close(items);if(frontier)sqlite3_close(frontier);sd_page_close();
}

/* /connect: the tools and agents side — what an outside program can use. */
static void sd_connect(const char *route,const char *host,const char *query,const char *bearer,const char *auth_db){
 int signed_in=bearer&&*bearer&&session_valid(bearer,auth_db);
 sqlite3 *items=sd_open_env("BONFYRE_ITEM_ARCHIVE_DB",&bf_store_policy_item_archive);long long sources=sd_one(items,"SELECT COUNT(DISTINCT source) FROM item",NULL);if(items)sqlite3_close(items);
 sd_page_open("Tools and agents",host,route,query,"connect");
 ui_open(stdout,"main","sx"); ui_open_a(stdout,"nav","class","sx-crumbs","aria-label","Breadcrumb"); ui_open_a(stdout,"a","href","/"); fputs("Aurekai",stdout); ui_close(stdout,"a"); ui_open(stdout,"span",NULL); fputs("/",stdout); ui_close(stdout,"span"); ui_open_a(stdout,"a","href","/connections"); fputs("Connections",stdout); ui_close(stdout,"a"); ui_open(stdout,"span",NULL); fputs("/",stdout); ui_close(stdout,"span"); ui_open(stdout,"span",NULL); fputs("Tools and agents",stdout); ui_close(stdout,"span"); ui_close(stdout,"nav"); ui_open(stdout,"header","sx-hero"); ui_open(stdout,"div",NULL); ui_open(stdout,"p","sx-kicker"); fputs("Carriers",stdout); ui_close(stdout,"p"); ui_open(stdout,"h1",NULL); fputs("Tools, agents and push feeds",stdout); ui_close(stdout,"h1"); ui_open(stdout,"p",NULL); fputs("How outside programs read what Aurekai keeps: an MCP server for your desktop tools, an A2A card for other agents, and WebSub so publishers can push new posts instead of being polled.",stdout); ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open(stdout,"nav","sx-actions"); ui_open_a(stdout,"a","class","sx-btn","href","/protocols");sd_icon("link");fputs("Every carrier</a><a class=\"sx-btn sx-primary\" href=\"/sources\" data-sx-open=\"sx-add\">",stdout);sd_icon("plus");fputs("Add a feed</a></nav></header>",stdout);
 ui_open(stdout,"dl","sx-stats"); ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Sources readable"); ui_open(stdout,"dd",NULL);sd_num(sources);ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_open(stdout,"dt",NULL); fputs("MCP",stdout); ui_close(stdout,"dt"); ui_open(stdout,"dd",NULL); fputs("/mcp",stdout); ui_open(stdout,"small",NULL); fputs("Model Context Protocol",stdout); ui_close(stdout,"small"); ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_open(stdout,"dt",NULL); fputs("A2A",stdout); ui_close(stdout,"dt"); ui_open(stdout,"dd",NULL); fputs("Agent card",stdout); ui_open(stdout,"small",NULL); fputs("/.well-known/agent-card.json",stdout); ui_close(stdout,"small"); ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_open(stdout,"dt",NULL); fputs("WebSub",stdout); ui_close(stdout,"dt"); ui_open(stdout,"dd",NULL); fputs("Hub",stdout); ui_open(stdout,"small",NULL); fputs("push in, push out",stdout); ui_close(stdout,"small"); ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_close(stdout,"dl");
 fputs("<div class=\"sx-split sx-settings\"><section class=\"sx-panel\" aria-labelledby=\"sx-mcp\"><p class=\"sx-kicker\">Your desktop tools</p><h2 id=\"sx-mcp\">MCP server</h2>"
       "<p>Point Claude, Cursor or any MCP client at Aurekai. The tools read sources and kept passages with your account's own limits; nothing is granted that your account does not already have.</p>"
       "<pre class=\"sx-code\">{\"mcpServers\":{\"aurekai\":{\"url\":\"" BF_ORIGIN_WORK "/mcp\"}}}</pre>",stdout);
 fputs(signed_in?"<a class=\"sx-btn\" href=\"/account\">Review your sessions</a>":"<a class=\"sx-btn sx-primary\" href=\"/login?next=%2Fconnect\">Sign in to authorize a tool</a>",stdout);
 ui_close(stdout,"section"); ui_open_a(stdout,"section","class","sx-panel","aria-labelledby","sx-a2a"); ui_open(stdout,"p","sx-kicker"); fputs("Other agents",stdout); ui_close(stdout,"p"); ui_open_a(stdout,"h2","id","sx-a2a"); fputs("Agent-to-agent",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p",NULL); fputs("Other agents discover what Aurekai offers from its agent card and call it over A2A. A call is an ask; nothing past a person's own submit is done for them.",stdout); ui_close(stdout,"p"); ui_open_a(stdout,"a","class","sx-btn","href","/.well-known/agent-card.json"); fputs("Read the agent card",stdout); ui_close(stdout,"a"); ui_close(stdout,"section"); ui_close(stdout,"div");
 ui_open_a(stdout,"section","class","sx-panel","aria-labelledby","sx-push"); ui_open(stdout,"p","sx-kicker"); fputs("WebSub",stdout); ui_close(stdout,"p"); ui_open_a(stdout,"h2","id","sx-push"); fputs("Push delivery",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p",NULL); fputs("When a publisher declares a hub, Aurekai can lease a subscription and receive each new post as it is published, checked with an HMAC signature before it is accepted. A delivery asks for a fresh observation; it does not publish anything by itself.",stdout); ui_close(stdout,"p");
 if(session_institution_authorized(bearer,auth_db,"bonfyre","admin"))do{ ui_open_a(stdout,"a","class","sx-btn sx-primary","href","/sources/push"); fputs("Open push delivery operations",stdout); ui_close(stdout,"a"); }while(0);
 else do{ ui_open_a(stdout,"a","class","sx-btn","href","/protocols"); fputs("How the hub works",stdout); ui_close(stdout,"a"); }while(0);
 ui_close(stdout,"section"); ui_close(stdout,"main");sd_add_dialog();sd_page_close();
}

/* The research handoff (?source=&item=&snapshot=) is carried by the reviewed
 * browser module; it keeps its exact coordinate contract. */
static void source_desk_research(const char *host,const char *route,const char *query){
 sd_page_open("Prepare research",host,route,query,"research");
 fputs("<main class=\"sx source-desk\" data-source-desk><nav class=\"sx-crumbs\" aria-label=\"Breadcrumb\"><a href=\"/\">Aurekai</a><span>/</span><a href=\"/sources\">Links and sources</a><span>/</span><span>Prepare research</span></nav>"
       "<header class=\"sx-hero\"><div><h1>Prepare a research review</h1><p>Choose the workspace that will receive this exact kept passage. Nothing is saved until you confirm it there.</p></div><nav aria-label=\"Continue your work\"><a class=\"sx-btn\" href=\"/apps?context=self\">Your work</a></nav></header>"
       /* source-desk.mjs mounts its tabs, address review, search and command
        * palette on exactly these controls; without any one of them the module
        * refuses to mount and the exact-coordinate handoff never opens. */
       "<div class=\"sd-mode-tabs\" aria-label=\"How to start\"><button type=\"button\" data-mode=\"browse\">Browse known sources</button><button type=\"button\" data-mode=\"address\">Bring a web address</button></div>"
       "<section data-mode-panel=\"address\"><form class=\"sd-address-form\" action=\"/studio/\" method=\"get\"><label for=\"sd-url\">Page or feed address</label><p id=\"sd-url-help\">Paste a complete public address. Reviewing it here does not fetch, connect, or publish anything.</p><div class=\"sd-input-action\"><input id=\"sd-url\" name=\"foreign_url\" type=\"url\" required autocomplete=\"url\" placeholder=\"https://publisher.example/article\" aria-describedby=\"sd-url-help\"><button type=\"submit\">Review this address</button></div><div data-address-preview aria-live=\"polite\"></div></form></section>"
       "<section data-mode-panel=\"browse\"><form class=\"sd-search\" action=\"/sources\" method=\"get\"><label for=\"sd-query\">Search sources already known to Aurekai</label><div class=\"sd-input-action\"><input id=\"sd-query\" name=\"q\" type=\"search\" maxlength=\"240\" placeholder=\"Publisher, topic, source name, or hostname\"><button type=\"submit\">Find sources</button></div></form>"
       "<div class=\"sd-catalogue-bar\"><p data-catalogue-status role=\"status\" aria-live=\"polite\">Opening the kept passage…</p><button type=\"button\" data-catalogue-retry hidden>Retry</button><button type=\"button\" data-source-commands>Commands</button></div><div data-source-collection></div><button type=\"button\" data-more-sources hidden>Load more</button></section>"
       "<noscript><p>Research review needs JavaScript. <a href=\"/apps?context=self\">Your installed work</a> remains available.</p></noscript></main>"
       "<script type=\"module\" src=\"/apps/source-desk.mjs\"></script>",stdout);
 sd_page_close();
}

/* A route the Subject Fabric compiled for this host's subject carries its own
 * contract: title, lede, regions and the exact material cut. When that
 * program exists the page program owns the route (it renders inside the same
 * shell); the reader below is the route only where nothing was compiled. */
static int sd_route_compiled(const char *host,const char *route){
 char subject[384];sqlite3 *cms=NULL,*fabric=NULL;sqlite3_stmt *st=NULL;int found=0;
 const char *path=getenv("BONFYRE_SUBJECT_FABRIC_DB");
 if(!path||!*path||!host||!*host)return 0;
 if(!(cms=cms_open_ro()))return 0;
 if(page_subject_for_host(cms,host,subject,sizeof subject)&&(fabric=bf_store_open_ro(path,1000,&bf_store_policy_subject_fabric))&&
    sqlite3_prepare_v2(fabric,"SELECT 1 FROM subject_route WHERE subject=?1 AND scope='public' AND route=?2 LIMIT 1",-1,&st,NULL)==SQLITE_OK){
  sqlite3_bind_text(st,1,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,route,-1,SQLITE_TRANSIENT);
  found=sqlite3_step(st)==SQLITE_ROW;}
 sqlite3_finalize(st);if(fabric)sqlite3_close(fabric);sqlite3_close(cms);return found;
}

static int serve_source_desk(const BfRequest *r) {
    const char *method=r->method; const char *target=r->target; const char *query=r->query; const char *host=r->host; const char *accept=r->accept; const char *bearer=r->bearer; const char *auth_db=r->auth_db;
    (void)method;(void)target;(void)query;(void)host;(void)accept;(void)bearer;(void)auth_db;
 int link=bf_request_is(r,"link"),sources=link||bf_request_is(r,"sources"),legacy=bf_request_is(r,"legacy");
 int connections=bf_request_is(r,"connections"),connect=bf_request_is(r,"connect"),channels=bf_request_is(r,"channels");
 char source[160]="";
 if(channels){
  /* Only the reader's own coordinates; SourceDepth keeps every ledger cut. */
  const char *p=query?query:"";
  while(*p){if(strncmp(p,"read=",5)&&strncmp(p,"feed=",5))return 0;p=strchr(p,'&');if(!p)break;p++;}
  if(sd_route_compiled(host,"/channels"))return 0;
  sd_reader("/channels",host,query);return 1;
 }
 if(connections){sd_connections("/connections",host,query,bearer,auth_db);return 1;}
 if(connect){sd_connect("/connect",host,query,bearer,auth_db);return 1;}
 if(!sources&&!legacy)return 0;
 if(query&&page_query_has_key(query,"snapshot")){source_desk_research(host,"/sources",query);return 1;}
 if(query&&query_value(query,"source",source,sizeof source)&&*source){sd_room(link?"/link":"/sources",host,query,source);return 1;}
 sd_hub(link?"/link":"/sources",host,query);return 1;
}
#endif
