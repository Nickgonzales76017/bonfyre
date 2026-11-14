#ifndef BONFYRE_ACCOUNT_WORLD_CINEMA_WEB_H
#define BONFYRE_ACCOUNT_WORLD_CINEMA_WEB_H
/* Screenings, film club and watch diary on an account space.
 *
 * /workspace/world/film   private API (JSON, or a 303 back to the studio or club)
 * /workspace/worlds/club  the club room for any signed-in account
 * /worlds/<space>         gains a programme, public diary notes and a club door
 *
 * Included by account_world_web.h; the CMS owner (account_world.c) holds every
 * row. A film is a PageComposition reference re-read on each request. A
 * screening plays here only when its own source states public domain or CC0
 * and gives an https video file; anything else is linked to where it is
 * published. Nothing here states availability, rights or audience numbers. */
static int world_web_https(const char *u);
static int world_web_resolve(sqlite3 *db,const char *ref,PageMaterial *material,char loaded[96],PageItem *out);
static void world_web_studio_location(const char *site,sqlite3_int64 world,const char *space);
/* account_world_literature_web.h: a space that holds writing calls its club a reading circle */
static int wl_space_reads(sqlite3 *db,const char *ns,sqlite3_int64 world,const char *space);
static void wl_club_options(sqlite3 *db,const char *ns,sqlite3_int64 world,const char *space);
static int wl_club_title(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *ref,int link);

typedef struct{PageMaterial *m;PageItem *it;char loaded[96];} CineFilms;
static void cine_films_open(CineFilms *f){f->m=calloc(1,sizeof(*f->m));f->it=calloc(1,sizeof(*f->it));f->loaded[0]=0;}
static void cine_films_close(CineFilms *f){free(f->m);free(f->it);f->m=NULL;f->it=NULL;}
static const PageItem *cine_film(sqlite3 *db,CineFilms *f,const char *ref){
    if(!f->m||!f->it||!ref||!*ref||!strncmp(ref,"space:",6))return NULL;
    return world_web_resolve(db,ref,f->m,f->loaded,f->it)?f->it:NULL;
}
/* Public domain or CC0 as the source states it; nothing narrower plays. */
static int cine_public_domain(const char *r){
    char s[128];size_t n=0;if(!r)return 0;while(*r==' ')r++;
    for(;*r&&n+1<sizeof(s);r++)s[n++]=(char)tolower((unsigned char)*r);s[n]=0;
    return !strncmp(s,"public domain",13)||!strncmp(s,"cc0",3)||strstr(s,"creativecommons.org/publicdomain/")!=NULL;
}
static int cine_playable(const PageItem *it){
    return it&&it->kind==BF_PAGE_MATERIAL_VIDEO&&cine_public_domain(it->rights)&&world_web_https(it->asset);
}
/* "2026-11-07T14:00" -> "Sat 7 Nov 2026, 14:00"; a day alone without the time. */
static void cine_when(const char *s){
    static const char *wd[]={"Sun","Mon","Tue","Wed","Thu","Fri","Sat"},*mo[]={"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
    static const int t[]={0,3,2,5,0,3,5,1,4,6,2,4};
    if(!s||strlen(s)<10){html(s?s:"");return;}
    int y=atoi(s),m=atoi(s+5),d=atoi(s+8);if(m<1||m>12){html(s);return;}
    int yy=m<3?y-1:y,w=(yy+yy/4-yy/100+yy/400+t[m-1]+d)%7;
    printf("%s %d %s %d",wd[w],d,mo[m-1],y);if(strlen(s)>=16)printf(", %.5s",s+11);
}
static void cine_month(const char *s){
    static const char *mo[]={"January","February","March","April","May","June","July","August","September","October","November","December"};
    int m=s&&strlen(s)>=7?atoi(s+5):0;if(m<1||m>12)return;printf("%s %.4s",mo[m-1],s);
}
static void cine_stars(int rating){
    if(rating<1||rating>10)return;
    printf("<span class=\"aw-stars\" role=\"img\" aria-label=\"Rated %d%s out of 5\">",rating/2,rating%2?" and a half":"");
    for(int i=0;i<rating/2;i++)fputs("\xE2\x98\x85",stdout);if(rating%2)fputs("\xC2\xBD",stdout);fputs("</span>",stdout);
}
/* A member's name as their account states it; never the account number. */
static void cine_name(const char *auth_db,const char *subject,char out[129]){
    sqlite3 *db=NULL;sqlite3_stmt *st=NULL;snprintf(out,129,"A member");
    if(!subject||strncmp(subject,"user:",5)||!auth_db||sqlite3_open_v2(auth_db,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){if(db)sqlite3_close(db);return;}
    if(sqlite3_prepare_v2(db,"SELECT name FROM users WHERE id=?1",-1,&st,NULL)==SQLITE_OK){
        sqlite3_bind_int64(st,1,strtoll(subject+5,NULL,10));
        if(sqlite3_step(st)==SQLITE_ROW&&sqlite3_column_text(st,0)&&*sqlite3_column_text(st,0))snprintf(out,129,"%s",(const char*)sqlite3_column_text(st,0));
    }
    sqlite3_finalize(st);sqlite3_close(db);
}
static void cine_film_title(const PageItem *it,const char *ref){
    if(it)html(it->title);else{fputs("A film no longer at its source (",stdout);html(ref);fputs(")",stdout);}
}

/* ------------------------------------------------------------------- API */
enum{CF_SITE,CF_WORLD,CF_OP,CF_SPACE,CF_SCREENING,CF_STARTS,CF_VENUE,CF_REF,CF_NOTE,CF_BODY,CF_ITEM,CF_KIND,
     CF_MEMBER,CF_STATE,CF_RATING,CF_WATCHED,CF_REWATCH,CF_VISIBILITY,CF_BEFORE,CF_BEFORE_DAY,CF_RETURN,CF_COUNT};
#define CB(x) (1u<<(x))
typedef struct{const char *op;int post,owner;unsigned required,optional;} CineOp;
static const CineOp cine_ops[]={
    {"screening",1,1,CB(CF_WORLD)|CB(CF_SPACE)|CB(CF_SCREENING)|CB(CF_STARTS)|CB(CF_VENUE)|CB(CF_REF),CB(CF_NOTE)},
    {"announce",1,1,CB(CF_WORLD)|CB(CF_SPACE)|CB(CF_SCREENING),0},
    {"cancel",1,1,CB(CF_WORLD)|CB(CF_SPACE)|CB(CF_SCREENING),0},
    {"club",1,1,CB(CF_WORLD)|CB(CF_SPACE)|CB(CF_STATE),0},
    {"hide",1,1,CB(CF_WORLD)|CB(CF_SPACE)|CB(CF_KIND)|CB(CF_ITEM),0},
    {"remove",1,1,CB(CF_WORLD)|CB(CF_SPACE)|CB(CF_MEMBER),0},
    {"note",1,1,CB(CF_WORLD)|CB(CF_REF)|CB(CF_WATCHED)|CB(CF_VISIBILITY),CB(CF_ITEM)|CB(CF_RATING)|CB(CF_REWATCH)|CB(CF_BODY)|CB(CF_SPACE)},
    {"unnote",1,1,CB(CF_WORLD)|CB(CF_ITEM),CB(CF_SPACE)},
    {"programme",0,1,CB(CF_WORLD)|CB(CF_SPACE),0},
    {"notes",0,1,CB(CF_WORLD),CB(CF_BEFORE)|CB(CF_BEFORE_DAY)},
    {"join",1,0,CB(CF_SPACE),0},
    {"leave",1,0,CB(CF_SPACE),0},
    {"pick",1,0,CB(CF_SPACE)|CB(CF_REF),CB(CF_NOTE)},
    {"vote",1,0,CB(CF_SPACE)|CB(CF_ITEM),0},
    {"say",1,0,CB(CF_SPACE)|CB(CF_BODY),CB(CF_REF)},
    {"retract",1,0,CB(CF_SPACE)|CB(CF_ITEM),0},
    {"picks",0,0,CB(CF_SPACE),0},
    {"posts",0,0,CB(CF_SPACE),CB(CF_BEFORE)},
};
/* Any signed-in account on a published member site; the owner checks the club. */
static int cine_member_admit(const char *bearer,const char *auth_db,sqlite3 *db,const char *site_id,int write,char subject[160]){
    char actor[160];const char *scope=NULL;PhSite site;
    return psetup_context(bearer,auth_db,"self",write?2:0,actor,sizeof(actor),subject,160,&scope)==1&&ph_site(db,NULL,site_id,&site);
}
static void cine_club_location(const char *site,const char *space){
    char location[300];snprintf(location,sizeof(location),"/workspace/worlds/club?site=%s&space=%s",site,space);page_redirect(location);
}
static void cine_json_texts(bf_jsonw *w,sqlite3_stmt *row,const char **keys,int n,unsigned ints){
    for(int i=0;i<n;i++){bf_jsonw_key(w,keys[i]);if(ints&(1u<<i))bf_jsonw_int(w,sqlite3_column_int64(row,i));else bf_jsonw_str(w,(const char*)sqlite3_column_text(row,i));}
}
static int world_cine_api(const char *method,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db){
    int post=!strcmp(method,"POST"),get=!strcmp(method,"GET");
    if(!post&&!get){aw_error(405,"Use GET or POST.\n");return 1;}
    if(post&&(!ph_origin(raw,host)||(query&&*query))){aw_error(403,"Submit from this workspace.\n");return 1;}
    char v[CF_COUNT][2008];memset(v,0,sizeof(v));
    static const char *names[CF_COUNT]={"site","world","operation","space","screening","starts","venue","ref","note","body","item","kind",
                                         "member","state","rating","watched","rewatch","visibility","before","before_day","return"};
    static const size_t caps[CF_COUNT]={128,24,24,65,65,17,121,161,281,2001,24,5,160,7,3,11,2,8,24,11,8};
    PhField fields[CF_COUNT];for(int i=0;i<CF_COUNT;i++){fields[i].key=names[i];fields[i].value=v[i];fields[i].cap=caps[i];fields[i].multiline=i==CF_NOTE||i==CF_BODY;}
    const CineOp *op=NULL;
    if(!ph_fields(post?body:query,fields,CF_COUNT,8192)||!*v[CF_SITE]){aw_error(400,"Invalid workspace request.\n");return 1;}
    for(size_t i=0;i<sizeof(cine_ops)/sizeof(cine_ops[0]);i++)if(!strcmp(v[CF_OP],cine_ops[i].op)&&cine_ops[i].post==post)op=&cine_ops[i];
    if(!op){aw_error(400,"Invalid workspace request.\n");return 1;}
    unsigned allowed=op->required|op->optional|CB(CF_SITE)|CB(CF_OP)|(post?CB(CF_RETURN):0);
    for(int i=0;i<CF_COUNT;i++)if((*v[i]&&!(allowed&CB(i)))||(!*v[i]&&(op->required&CB(i)))){aw_error(400,"Invalid workspace coordinates.\n");return 1;}
    const char *ret=v[CF_RETURN];
    if(*ret&&strcmp(ret,"studio")&&strcmp(ret,"club")){aw_error(400,"Invalid workspace coordinates.\n");return 1;}
    sqlite3_int64 world_id=0,item=0,rating=0,before=0;
    if((*v[CF_WORLD]&&!world_web_number(v[CF_WORLD],&world_id,0))||(*v[CF_ITEM]&&!world_web_number(v[CF_ITEM],&item,0))||
       (*v[CF_RATING]&&(!world_web_number(v[CF_RATING],&rating,1)||rating>10))||(*v[CF_BEFORE]&&!world_web_number(v[CF_BEFORE],&before,0))||
       (*v[CF_REWATCH]&&strcmp(v[CF_REWATCH],"1"))||(*v[CF_STATE]&&strcmp(v[CF_STATE],"open")&&strcmp(v[CF_STATE],"closed"))||
       (!strcmp(ret,"studio")&&!op->owner)){aw_error(400,"Invalid workspace coordinates.\n");return 1;}
    const char *path=getenv("BONFYRE_CMS_DB");sqlite3 *db=NULL;char subject[160],again[160];const char *site=v[CF_SITE],*space=v[CF_SPACE],*o=op->op;
    if(!path||!*path||sqlite3_open_v2(path,&db,post?SQLITE_OPEN_READWRITE:SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){if(db)sqlite3_close(db);aw_error(503,"Workspace unavailable.\n");return 1;}
    sqlite3_busy_timeout(db,2000);
    int admitted=op->owner?world_web_admit(bearer,auth_db,db,site,post,subject):cine_member_admit(bearer,auth_db,db,site,post,subject);
    if(!admitted||(post&&aw_initialize(db)!=1)){sqlite3_close(db);aw_error(admitted?503:404,"Workspace unavailable.\n");return 1;}
    if(post&&(!(op->owner?world_web_admit(bearer,auth_db,db,site,post,again):cine_member_admit(bearer,auth_db,db,site,post,again))||strcmp(subject,again))){sqlite3_close(db);aw_error(404,"Workspace unavailable.\n");return 1;}
    sqlite3_stmt *rows=NULL;int result;
    if(!strcmp(o,"screening"))result=aw_screening_put(db,subject,site,world_id,space,v[CF_SCREENING],v[CF_STARTS],v[CF_VENUE],v[CF_REF],v[CF_NOTE]);
    else if(!strcmp(o,"announce"))result=aw_screening_announce(db,subject,site,world_id,space,v[CF_SCREENING]);
    else if(!strcmp(o,"cancel"))result=aw_screening_cancel(db,subject,site,world_id,space,v[CF_SCREENING]);
    else if(!strcmp(o,"club"))result=aw_club_set(db,subject,site,world_id,space,!strcmp(v[CF_STATE],"open"));
    else if(!strcmp(o,"hide"))result=aw_club_hide(db,subject,site,world_id,space,v[CF_KIND],item);
    else if(!strcmp(o,"remove"))result=aw_club_remove(db,subject,site,world_id,space,v[CF_MEMBER]);
    else if(!strcmp(o,"note"))result=aw_note_put(db,subject,site,world_id,item,v[CF_REF],v[CF_WATCHED],(int)rating,*v[CF_REWATCH]!=0,v[CF_BODY],v[CF_VISIBILITY]);
    else if(!strcmp(o,"unnote"))result=aw_note_remove(db,subject,site,world_id,item);
    else if(!strcmp(o,"programme"))result=aw_screenings(db,subject,site,world_id,space,&rows);
    else if(!strcmp(o,"notes"))result=aw_notes(db,subject,site,world_id,v[CF_BEFORE_DAY],before,&rows);
    else if(!strcmp(o,"join"))result=aw_club_join(db,subject,site,space);
    else if(!strcmp(o,"leave"))result=aw_club_leave(db,subject,site,space);
    else if(!strcmp(o,"pick"))result=aw_club_pick(db,subject,site,space,v[CF_REF],v[CF_NOTE]);
    else if(!strcmp(o,"vote"))result=aw_club_vote(db,subject,site,space,item);
    else if(!strcmp(o,"say"))result=aw_club_say(db,subject,site,space,v[CF_REF],v[CF_BODY]);
    else if(!strcmp(o,"retract"))result=aw_club_retract(db,subject,site,space,item);
    else if(!strcmp(o,"picks"))result=aw_club_picks(db,subject,site,space,&rows);
    else result=aw_club_posts(db,subject,site,space,before,&rows);
    if(result!=1){sqlite3_finalize(rows);sqlite3_close(db);aw_error(result<0?503:post?409:404,"Workspace request could not be completed.\n");return 1;}
    if(post&&*ret){sqlite3_close(db);if(!strcmp(ret,"club"))cine_club_location(site,space);else world_web_studio_location(site,world_id,space);return 1;}
    bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"state");bf_jsonw_str(&w,post?"saved":"draft");
    int step=SQLITE_DONE,count=0;
    if(rows){
        static const char *sk[]={"screening","starts","venue","ref","note","state"},*nk[]={"note","ref","watched","rating","rewatch","body","visibility"},
                          *pk[]={"pick","ref","note","proposed_by","votes","state","voted"},*ck[]={"post","author","ref","body","state"};
        const char *list=!strcmp(o,"programme")?"screenings":!strcmp(o,"notes")?"notes":!strcmp(o,"picks")?"picks":"posts";
        int paged=!strcmp(o,"notes")||!strcmp(o,"posts");char next_day[11]="";sqlite3_int64 next=0;
        bf_jsonw_key(&w,list);bf_jsonw_arr(&w);
        while((step=sqlite3_step(rows))==SQLITE_ROW){
            if(paged&&++count>AW_CLUB_PAGE){step=SQLITE_DONE;break;}
            bf_jsonw_obj(&w);
            if(!strcmp(o,"programme"))cine_json_texts(&w,rows,sk,6,0);
            else if(!strcmp(o,"notes")){cine_json_texts(&w,rows,nk,7,1u|8u|16u);next=sqlite3_column_int64(rows,0);snprintf(next_day,sizeof(next_day),"%s",(const char*)sqlite3_column_text(rows,2));}
            else if(!strcmp(o,"picks"))cine_json_texts(&w,rows,pk,7,1u|16u|64u);
            else{cine_json_texts(&w,rows,ck,5,1u);next=sqlite3_column_int64(rows,0);}
            bf_jsonw_obj_end(&w);
        }
        bf_jsonw_arr_end(&w);
        if(paged&&count>AW_CLUB_PAGE){bf_jsonw_key(&w,"next");bf_jsonw_obj(&w);bf_jsonw_key(&w,"before");bf_jsonw_int(&w,next);
            if(*next_day){bf_jsonw_key(&w,"before_day");bf_jsonw_str(&w,next_day);}bf_jsonw_obj_end(&w);}
    }
    bf_jsonw_obj_end(&w);sqlite3_finalize(rows);sqlite3_close(db);
    if(step!=SQLITE_DONE||w.oom)aw_error(503,"Workspace unavailable.\n");else aw_response(200,"application/json",w.buf,w.len,0);bf_jsonw_free(&w);return 1;
}

/* ---------------------------------------------------------------- studio */
static void cine_hidden(const char *site,sqlite3_int64 world,const char *space,const char *op){
    char n[24];ph_hidden("site",site);snprintf(n,sizeof(n),"%lld",(long long)world);ph_hidden("world",n);ph_hidden("operation",op);
    if(space&&*space)ph_hidden("space",space);ph_hidden("return","studio");
}
static void cine_film_options(sqlite3 *db,CineFilms *films,sqlite3_stmt *items,const char *id){
    fputs("<datalist id=\"",stdout);html(id);fputs("\">",stdout);
    while(items&&sqlite3_step(items)==SQLITE_ROW){const char *ref=(const char*)sqlite3_column_text(items,1);const PageItem *it=cine_film(db,films,ref);
        if(!it||it->kind!=BF_PAGE_MATERIAL_VIDEO)continue;fputs("<option value=\"",stdout);html(ref);fputs("\">",stdout);html(it->title);fputs("</option>",stdout);}
    fputs("</datalist>",stdout);
}
/* Owner tools for one space: programme, club and diary. Shown for every
 * space; the medium word never switches them on or off. */
static void world_cine_studio(sqlite3 *db,const char *subject,const char *site,sqlite3_int64 world,const char *space){
    sqlite3_stmt *rows=NULL;CineFilms films;cine_films_open(&films);char state[8]="";
    fputs("<details class=\"aw-cine\" data-world-cine open><summary><h3>Screenings, film club and diary</h3></summary>",stdout);
    /* programme */
    fputs("<h3>Programme</h3><p>Screenings show on the public page once you announce them and the space is public. A film plays there only when its own source says it is in the public domain or CC0. Anything else gets a link to where it is published.</p><ol class=\"aw-items\" data-cine-programme>",stdout);
    int have=0;
    if(aw_screenings(db,subject,site,world,space,&rows)==1)while(sqlite3_step(rows)==SQLITE_ROW){
        const char *sid=(const char*)sqlite3_column_text(rows,0),*starts=(const char*)sqlite3_column_text(rows,1),*venue=(const char*)sqlite3_column_text(rows,2),
                   *ref=(const char*)sqlite3_column_text(rows,3),*note=(const char*)sqlite3_column_text(rows,4),*st=(const char*)sqlite3_column_text(rows,5);
        const PageItem *it=cine_film(db,&films,ref);have++;
        fputs("<li data-cine-screening=\"",stdout);html(sid);fputs("\" data-state=\"",stdout);html(st);fputs("\"><div><strong>",stdout);cine_when(starts);fputs(" &middot; ",stdout);html(venue);
        fputs("</strong><br>",stdout);cine_film_title(it,ref);if(note&&*note){fputs("<br>",stdout);html(note);}
        fputs("<br><span class=\"aw-state\">",stdout);fputs(!strcmp(st,"draft")?"Not announced":!strcmp(st,"announced")?"Announced":"Cancelled",stdout);
        if(it&&strcmp(st,"cancelled"))fputs(cine_playable(it)?" &middot; plays here":" &middot; linked, not played",stdout);
        fputs("</span></div><div class=\"aw-actions\">",stdout);
        if(!strcmp(st,"draft")){fputs("<form method=\"post\" action=\"/workspace/world/film\">",stdout);cine_hidden(site,world,space,"announce");ph_hidden("screening",sid);fputs("<button type=\"submit\">Announce</button></form>",stdout);}
        if(strcmp(st,"cancelled")){fputs("<form method=\"post\" action=\"/workspace/world/film\">",stdout);cine_hidden(site,world,space,"cancel");ph_hidden("screening",sid);
            fputs("<button type=\"submit\" class=\"aw-quiet\">",stdout);fputs(!strcmp(st,"draft")?"Delete":"Cancel screening",stdout);fputs("</button></form>",stdout);}
        fputs("</div></li>",stdout);
    }
    sqlite3_finalize(rows);rows=NULL;if(!have)fputs("<li>No screenings yet.</li>",stdout);fputs("</ol>",stdout);
    fputs("<form method=\"post\" action=\"/workspace/world/film\" data-cine-form=\"screening\">",stdout);cine_hidden(site,world,space,"screening");
    fputs("<div class=\"aw-row\"><label>Film<small>One of the films in this space, or a piece address</small><input name=\"ref\" required maxlength=\"160\" list=\"cine-films\">",stdout);
    if(aw_items(db,subject,site,world,space,&rows)==1)cine_film_options(db,&films,rows,"cine-films");sqlite3_finalize(rows);rows=NULL;
    fputs("</label><label>When<input type=\"datetime-local\" name=\"starts\" required></label></div>"
          "<div class=\"aw-row\"><label>Where<small>A room, a hall, a rooftop, or online</small><input name=\"venue\" required maxlength=\"120\"></label>"
          "<label>Short name<small>Lowercase, for your own list</small><input name=\"screening\" required maxlength=\"64\" pattern=\"[a-z0-9][a-z0-9-]*\"></label></div>"
          "<label>Note<small>Optional. Shown with the screening</small><textarea name=\"note\" rows=\"2\" maxlength=\"280\"></textarea></label>"
          "<div class=\"aw-actions\"><button type=\"submit\">Add screening</button></div></form>",stdout);
    /* club */
    aw_club_state(db,subject,site,world,space,state);
    fputs("<h3>Film club</h3><p>",stdout);
    fputs(!strcmp(state,"open")?"The club is open. Signed-in people can join from the public page, pick films together and talk about them. Only members can read what is said.":
          "Open a club and people can join from the public page once the space is public. Members pick films and talk about them; you can hide posts and remove members.",stdout);
    fputs("</p><div class=\"aw-actions\"><form method=\"post\" action=\"/workspace/world/film\" data-cine-form=\"club\">",stdout);cine_hidden(site,world,space,"club");
    ph_hidden("state",!strcmp(state,"open")?"closed":"open");fputs("<button type=\"submit\"",stdout);if(!strcmp(state,"open"))fputs(" class=\"aw-quiet\"",stdout);
    fputs(">",stdout);fputs(!strcmp(state,"open")?"Close the club":"Open a club",stdout);fputs("</button></form>",stdout);
    if(!strcmp(state,"open")){fputs("<a class=\"ph-button\" href=\"/workspace/worlds/club?site=",stdout);url_query_value(site);fputs("&amp;space=",stdout);url_query_value(space);fputs("\">Go to the club</a>",stdout);}
    fputs("</div>",stdout);
    /* diary */
    fputs("<h3>Watch diary</h3><p>Your own log. A note marked for everyone shows on the public page beside that film while the film is in a public space.</p><ol class=\"aw-items\" data-cine-diary>",stdout);
    have=0;int count=0;
    if(aw_notes(db,subject,site,world,"",0,&rows)==1)while(sqlite3_step(rows)==SQLITE_ROW&&++count<=AW_CLUB_PAGE){
        sqlite3_int64 id=sqlite3_column_int64(rows,0);const char *ref=(const char*)sqlite3_column_text(rows,1),*watched=(const char*)sqlite3_column_text(rows,2),
        *words=(const char*)sqlite3_column_text(rows,5),*vis=(const char*)sqlite3_column_text(rows,6);int rating=sqlite3_column_int(rows,3),rewatch=sqlite3_column_int(rows,4);
        const PageItem *it=cine_film(db,&films,ref);have++;
        printf("<li data-cine-note=\"%lld\" data-visibility=\"",(long long)id);html(vis);fputs("\"><div><strong>",stdout);cine_film_title(it,ref);fputs("</strong><br>",stdout);
        cine_when(watched);if(rewatch)fputs(" &middot; watched again",stdout);if(rating){fputs(" &middot; ",stdout);cine_stars(rating);}
        if(words&&*words){fputs("<br>",stdout);html(words);}fputs("<br><span class=\"aw-state\">",stdout);fputs(!strcmp(vis,"public")?"Everyone":"Only you",stdout);
        fputs("</span></div><form method=\"post\" action=\"/workspace/world/film\">",stdout);cine_hidden(site,world,space,"unnote");char n[24];snprintf(n,sizeof(n),"%lld",(long long)id);ph_hidden("item",n);
        fputs("<button type=\"submit\" class=\"aw-quiet\">Remove</button></form></li>",stdout);
    }
    sqlite3_finalize(rows);rows=NULL;if(!have)fputs("<li>Nothing logged yet.</li>",stdout);fputs("</ol>",stdout);
    if(count>AW_CLUB_PAGE)fputs("<p>Showing your newest 20 entries.</p>",stdout);
    fputs("<form method=\"post\" action=\"/workspace/world/film\" data-cine-form=\"note\">",stdout);cine_hidden(site,world,space,"note");
    fputs("<div class=\"aw-row\"><label>Film<input name=\"ref\" required maxlength=\"160\" list=\"cine-films\"></label><label>Watched on<input type=\"date\" name=\"watched\" required></label>"
          "<label>Rating<select name=\"rating\"><option value=\"0\">No rating</option>",stdout);
    for(int r=1;r<=10;r++){printf("<option value=\"%d\">%d%s star%s</option>",r,r/2,r%2?"\xC2\xBD":"",r==2?"":"s");}
    fputs("</select></label></div><label>Your words<small>Optional</small><textarea name=\"body\" rows=\"3\" maxlength=\"2000\"></textarea></label>"
          "<div class=\"aw-row\"><label>Who can read it<select name=\"visibility\"><option value=\"private\">Only you</option><option value=\"public\">Everyone, on the public page</option></select></label>"
          "<label class=\"aw-check\"><input type=\"checkbox\" name=\"rewatch\" value=\"1\"> Watched it before</label></div>"
          "<div class=\"aw-actions\"><button type=\"submit\">Log it</button></div></form></details>",stdout);
    cine_films_close(&films);
}

/* The one shell for a space's rooms (club, reading, reader): world.css keeps
 * the room grammar, composition.css the app ground; body.ak-app carries both. */
static void world_room_open(const char *title,const char *word,const char *character,const char *room,int private_room){
    fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
          "<meta name=\"theme-color\" content=\"#0f1511\">",stdout);if(private_room)fputs("<meta name=\"robots\" content=\"noindex,nofollow\">",stdout);fputs("<title>",stdout);html(title);
    if(word&&*word){fputs(" \xc2\xb7 ",stdout);html(word);}
    fputs("</title><link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=Literata:ital,opsz,wght@0,7..72,400;0,7..72,600;1,7..72,400&amp;display=swap\">"
          "<link rel=\"stylesheet\" href=\"/assets/world.css\"></head><body class=\"ak-app ",stdout);
    html(character);fputs(" aw-space ",stdout);html(room);fputs("\" data-world-look=\"",stdout);html(character);fputs("\">" BF_ESTATE_BAR,stdout);
}
static void world_room_close(void){fputs("</main>" BF_ESTATE_FOOT "</body></html>",stdout);}

/* ---------------------------------------------------------------- public */
/* Programme, public diary notes and a club door under a public space. */
static void world_cine_public(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *space){
    sqlite3_stmt *rows=NULL;CineFilms films;cine_films_open(&films);
    if(aw_public_screenings(db,site->id,world,space,&rows)==1){
        int shown=0;char month[8]="";
        while(sqlite3_step(rows)==SQLITE_ROW){
            const char *sid=(const char*)sqlite3_column_text(rows,0),*starts=(const char*)sqlite3_column_text(rows,1),*venue=(const char*)sqlite3_column_text(rows,2),
                       *ref=(const char*)sqlite3_column_text(rows,3),*note=(const char*)sqlite3_column_text(rows,4),*st=(const char*)sqlite3_column_text(rows,5);
            const PageItem *it=cine_film(db,&films,ref);int cancelled=!strcmp(st,"cancelled"),play=!cancelled&&cine_playable(it);
            if(!shown++){fputs("<section class=\"aw-programme\" aria-labelledby=\"aw-programme-h\" data-world-programme><h2 id=\"aw-programme-h\" class=\"region-label\">Programme</h2>",stdout);fputs("<div class=\"aw-cinema-tools\" data-cinema-tools><label>Find a film or venue<input type=\"search\" data-cinema-search maxlength=\"120\" placeholder=\"Search this programme\"></label><div class=\"aw-cinema-filters\" role=\"group\" aria-label=\"Screening type\"><button type=\"button\" data-cinema-filter=\"all\" aria-pressed=\"true\">All</button><button type=\"button\" data-cinema-filter=\"media\" aria-pressed=\"false\">Watch here</button><button type=\"button\" data-cinema-filter=\"link\" aria-pressed=\"false\">At source</button><button type=\"button\" data-cinema-filter=\"cancelled\" aria-pressed=\"false\">Cancelled</button></div><output role=\"status\" aria-live=\"polite\" data-cinema-count></output></div>",stdout);}
            if(strncmp(month,starts,7)){snprintf(month,sizeof(month),"%.7s",starts);fputs("<h3 class=\"aw-month\">",stdout);cine_month(starts);fputs("</h3>",stdout);}
            fputs("<article class=\"aw-show\" id=\"screening-",stdout);html(sid);fputs("\" data-screening=\"",stdout);html(sid);fputs("\" data-state=\"",stdout);html(st);
            fputs("\" data-shown=\"",stdout);fputs(play?"media":"link",stdout);fputs("\"><p class=\"aw-when\"><time datetime=\"",stdout);html(starts);fputs("\">",stdout);cine_when(starts);
            fputs("</time> &middot; ",stdout);html(venue);if(cancelled)fputs(" &middot; <strong>Cancelled</strong>",stdout);fputs("</p><h3>",stdout);
            if(it){fputs("<a href=\"/o/",stdout);html(ref);fputs("\">",stdout);html(it->title);fputs("</a>",stdout);}else fputs("A film no longer available from its source",stdout);
            fputs("</h3>",stdout);if(it&&it->by[0]){fputs("<p class=\"by\">",stdout);html(it->by);fputs("</p>",stdout);}
            if(note&&*note){fputs("<p class=\"aw-note\">",stdout);html(note);fputs("</p>",stdout);}
            if(play){fputs("<details class=\"aw-watch\"><summary>Watch it here</summary><video controls preload=\"none\" playsinline",stdout);
                if(world_web_https(it->image)&&page_image_url_safe(it->image)){fputs(" poster=\"",stdout);html(it->image);fputs("\"",stdout);}
                fputs(" src=\"",stdout);html(it->asset);fputs("\"><a href=\"",stdout);html(it->asset);fputs("\">Open the film file</a></video></details>",stdout);}
            if(it){fputs("<p class=\"aw-terms\">",stdout);
                if(it->rights[0]){if(world_web_https(it->rights_href)){fputs("<a rel=\"license noopener\" href=\"",stdout);html(it->rights_href);fputs("\">",stdout);html(it->rights);fputs("</a>",stdout);}else html(it->rights);}
                else fputs("Its source states no terms",stdout);
                if(!play&&!cancelled)fputs(" &middot; not played here",stdout);
                if(public_http_url(it->href)){fputs(" &middot; <a rel=\"noopener\" href=\"",stdout);html(it->href);fputs("\">Where it is published</a>",stdout);}else{fputs(" &middot; from ",stdout);html(it->source);}
                fputs("</p>",stdout);}
            fputs("</article>",stdout);
        }
        if(shown){fputs("</section>",stdout);fputs("<script>(()=>{\n const section=document.currentScript?.previousElementSibling;\n if(!section?.matches('[data-world-programme]'))return;\n const tools=section.querySelector('[data-cinema-tools]');if(!tools)return;\n const shows=[...section.querySelectorAll(':scope > article[data-screening]')],months=[...section.querySelectorAll(':scope > .aw-month')];\n const search=tools.querySelector('[data-cinema-search]'),buttons=[...tools.querySelectorAll('[data-cinema-filter]')],count=tools.querySelector('[data-cinema-count]');let filter='all';\n function update(){const term=search.value.trim().toLocaleLowerCase();let visible=0;\n  for(const show of shows){const category=show.dataset.state==='cancelled'?'cancelled':show.dataset.shown;\n   const matches=(filter==='all'||category===filter)&&(!term||show.textContent.toLocaleLowerCase().includes(term));\n   show.hidden=!matches;if(matches)visible++;}\n  for(const heading of months){let next=heading.nextElementSibling,found=false;\n   while(next&&!next.matches('.aw-month')){if(next.matches('article[data-screening]')&&!next.hidden)found=true;next=next.nextElementSibling;}heading.hidden=!found;}\n  for(const b of buttons)b.setAttribute('aria-pressed',String(b.dataset.cinemaFilter===filter));\n  count.textContent='Showing '+visible+' of '+shows.length+' announced screenings in this programme. Rights and availability are source-stated.';\n }\n search.addEventListener('input',update);for(const b of buttons)b.addEventListener('click',()=>{filter=b.dataset.cinemaFilter;update();});update();\n})();</script>",stdout);}
    }
    sqlite3_finalize(rows);rows=NULL;
    if(aw_public_notes(db,site->id,world,space,&rows)==1){
        int shown=0;
        while(sqlite3_step(rows)==SQLITE_ROW){
            const char *ref=(const char*)sqlite3_column_text(rows,0),*watched=(const char*)sqlite3_column_text(rows,1),*words=(const char*)sqlite3_column_text(rows,4);
            int rating=sqlite3_column_int(rows,2),rewatch=sqlite3_column_int(rows,3);const PageItem *it=cine_film(db,&films,ref);if(!it)continue;
            if(!shown++)fputs("<section class=\"aw-diary\" aria-labelledby=\"aw-diary-h\" data-world-diary><h2 id=\"aw-diary-h\" class=\"region-label\">From the diary</h2>",stdout);
            fputs("<article class=\"aw-entry\"><p class=\"aw-when\">",stdout);cine_when(watched);if(rewatch)fputs(" &middot; watched again",stdout);
            if(rating){fputs(" &middot; ",stdout);cine_stars(rating);}fputs("</p><h3><a href=\"/o/",stdout);html(ref);fputs("\">",stdout);html(it->title);fputs("</a></h3>",stdout);
            if(words&&*words){fputs("<p class=\"aw-note\">",stdout);html(words);fputs("</p>",stdout);}fputs("</article>",stdout);
        }
        if(shown)fputs("</section>",stdout);
    }
    sqlite3_finalize(rows);rows=NULL;cine_films_close(&films);
    /* the club door: shown only while the club is open; the room itself checks membership */
    if(aw_public_club(db,site->id,space)==1){
        int reads=wl_space_reads(db,site->id,world,space);
        fputs(reads?"<section class=\"aw-club\" data-world-club data-club-word=\"reading circle\"><h2 class=\"region-label\">Reading circle</h2><p class=\"aw-note\">Members choose what to read together and talk about it. Only members can read the circle.</p>":
              "<section class=\"aw-club\" data-world-club><h2 class=\"region-label\">Film club</h2><p class=\"aw-note\">Members choose films together and talk about them. Only members can read the club.</p>",stdout);
        fputs("<p><a class=\"aw-door\" href=\"https://aurekai.",stdout);html(site->territory);fputs("/workspace/worlds/club?site=",stdout);url_query_value(site->id);fputs("&amp;space=",stdout);url_query_value(space);
        fputs(reads?"\">Join or open the circle</a></p></section>":"\">Join or open the club</a></p></section>",stdout);
    }
}

/* ------------------------------------------------------------- club room */
/* Members' rooms open on the one shell: the estate bar, one <main>, the one
 * footer and player. The room keeps world.css for its grammar; the Stitch
 * tokens replace the space's character inside the app (world.pcss). */
static void cine_room_head(const char *title,const char *character,const char *space,const char *word){
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Cookie, Authorization\r\n"
          "X-Content-Type-Options: nosniff\r\nReferrer-Policy: same-origin\r\nConnection: close\r\n\r\n",stdout);
    world_room_open(title,word,character,"aw-clubroom",1);
    fputs("<main class=\"wrap aw-room\" data-world-club=\"",stdout);html(space);fputs("\">",stdout);
}
static void cine_room_form(const char *site,const char *space,const char *op){
    fputs("<form method=\"post\" action=\"/workspace/world/film\">",stdout);ph_hidden("site",site);ph_hidden("operation",op);ph_hidden("space",space);ph_hidden("return","club");
}
static int serve_world_club(const char *method,const char *query,const char *bearer,const char *auth_db){
    if(strcmp(method,"GET")){aw_error(405,"Open the club with GET.\n");return 1;}
    char site_id[128]="",space[65]="",before_s[24]="";sqlite3_int64 before=0;
    PhField params[]={{"site",site_id,sizeof(site_id),0},{"space",space,sizeof(space),0},{"before",before_s,sizeof(before_s),0}};
    if(!ph_fields(query,params,3,1024)||!*site_id||!*space||(*before_s&&!world_web_number(before_s,&before,0))){aw_error(400,"Choose a club.\n");return 1;}
    sqlite3 *db=cms_open_ro();char viewer[160];PhSite site;sqlite3_stmt *row=NULL;
    if(!db||!cine_member_admit(bearer,auth_db,db,site_id,0,viewer)||!ph_site(db,NULL,site_id,&site)||aw_club_view(db,viewer,site_id,space,&row)!=1){
        sqlite3_finalize(row);if(db)sqlite3_close(db);aw_error(404,"This club is not open.\n");return 1;}
    char title[257],character[33],lede[601],role[16];sqlite3_int64 world=sqlite3_column_int64(row,0);
    snprintf(title,sizeof(title),"%s",(const char*)sqlite3_column_text(row,1));snprintf(character,sizeof(character),"%s",(const char*)sqlite3_column_text(row,2));
    snprintf(lede,sizeof(lede),"%s",(const char*)sqlite3_column_text(row,3));snprintf(role,sizeof(role),"%s",(const char*)sqlite3_column_text(row,4));
    sqlite3_finalize(row);row=NULL;if(!world_web_look(character))snprintf(character,sizeof(character),"journal");
    int host=!strcmp(role,"host"),member=host||!strcmp(role,"member");
    int reads=wl_space_reads(db,site_id,world,space);const char *word=reads?"reading circle":"film club";
    cine_room_head(title,character,space,word);
    fputs("<nav class=\"aw-crumb\" aria-label=\"Where you are\"><a class=\"home\" href=\"https://",stdout);html(site.host);fputs("/worlds/",stdout);html(space);fputs("\">",stdout);html(title);
    fputs("</a><span class=\"ch\">",stdout);html(word);fputs("</span></nav><header class=\"hero\"><h1>",stdout);html(title);fputs(reads?": the circle</h1><p class=\"lede\">":": the club</h1><p class=\"lede\">",stdout);
    if(reads)fputs(host?"You host this circle. Members propose what to read and talk here; you can hide a post or a proposal and take someone out of the circle.":
          member?"Propose what to read next with the other members and talk about it. Only members can read this page.":
          "Members choose what to read together and talk about it here. Only members can read what is said.",stdout);
    else fputs(host?"You host this club. Members pick films and talk here; you can hide a post or a pick and take someone out of the club.":
          member?"Pick films with the other members and talk about them. Only members can read this page.":
          "Members pick films together and talk about them here. Only members can read what is said.",stdout);fputs("</p></header>",stdout);
    if(!strcmp(role,"removed")){fputs("<p class=\"aw-note\" data-club-role=\"removed\">The host has taken you out of this club.</p>",stdout);world_room_close();sqlite3_close(db);return 1;}
    if(!member){
        fputs("<section class=\"aw-join\" data-club-role=\"",stdout);html(*role?role:"guest");fputs("\"><p class=\"aw-note\">When you join, the host and the other members see your account name here.</p>",stdout);
        cine_room_form(site_id,space,"join");fputs("<button type=\"submit\">Join the club</button></form></section>",stdout);world_room_close();sqlite3_close(db);return 1;
    }
    CineFilms films;cine_films_open(&films);char name[129];
    /* picks */
    fputs("<section class=\"aw-picks\" aria-labelledby=\"aw-picks-h\" data-club-role=\"",stdout);html(role);fputs("\"><h2 id=\"aw-picks-h\" class=\"region-label\">Picks</h2><ol>",stdout);
    int have=0;
    if(aw_club_picks(db,viewer,site_id,space,&row)==1)while(sqlite3_step(row)==SQLITE_ROW){
        sqlite3_int64 id=sqlite3_column_int64(row,0);const char *ref=(const char*)sqlite3_column_text(row,1),*note=(const char*)sqlite3_column_text(row,2),
        *by=(const char*)sqlite3_column_text(row,3),*st=(const char*)sqlite3_column_text(row,5);long long votes=sqlite3_column_int64(row,4);int voted=sqlite3_column_int(row,6);
        const PageItem *it=cine_film(db,&films,ref);cine_name(auth_db,by,name);have++;char n[24];snprintf(n,sizeof(n),"%lld",(long long)id);
        printf("<li data-club-pick=\"%lld\" data-state=\"",(long long)id);html(st);fputs("\"><div class=\"aw-words\"><h3>",stdout);
        if(!it&&wl_club_title(db,&site,world,ref,1)){}
        else{if(it){fputs("<a href=\"https://",stdout);html(site.host);fputs("/o/",stdout);html(ref);fputs("\">",stdout);}cine_film_title(it,ref);if(it)fputs("</a>",stdout);}
        fputs("</h3><p class=\"by\">Proposed by ",stdout);html(!strcmp(by,viewer)?"you":name);printf(" &middot; %lld vote%s",votes,votes==1?"":"s");
        if(!strcmp(st,"hidden"))fputs(" &middot; hidden from members",stdout);fputs("</p>",stdout);
        if(note&&*note){fputs("<p class=\"aw-note\">",stdout);html(note);fputs("</p>",stdout);}
        fputs("</div><div class=\"aw-acts\">",stdout);
        if(voted)fputs("<span class=\"aw-mark\">You voted</span>",stdout);
        else if(!strcmp(st,"open")){cine_room_form(site_id,space,"vote");ph_hidden("item",n);fputs("<button type=\"submit\">Vote for it</button></form>",stdout);}
        if(host&&!strcmp(st,"open")){fputs("<form method=\"post\" action=\"/workspace/world/film\">",stdout);ph_hidden("site",site_id);char w[24];snprintf(w,sizeof(w),"%lld",(long long)world);
            ph_hidden("world",w);ph_hidden("operation","hide");ph_hidden("space",space);ph_hidden("kind","pick");ph_hidden("item",n);ph_hidden("return","club");fputs("<button type=\"submit\" class=\"aw-quiet\">Hide</button></form>",stdout);}
        fputs("</div></li>",stdout);
    }
    sqlite3_finalize(row);row=NULL;if(!have)fputs("<li><p class=\"aw-body\">No picks yet. Propose the first one.</p></li>",stdout);fputs("</ol>",stdout);
    cine_room_form(site_id,space,"pick");
    fputs(reads?"<div class=\"aw-fields\"><label>What to read<select name=\"ref\" required>":"<div class=\"aw-fields\"><label>Film<select name=\"ref\" required>",stdout);
    if(aw_public_items(db,site_id,world,space,&row)==1)while(sqlite3_step(row)==SQLITE_ROW){const char *ref=(const char*)sqlite3_column_text(row,1);const PageItem *it=cine_film(db,&films,ref);
        if(!it||it->kind!=BF_PAGE_MATERIAL_VIDEO)continue;fputs("<option value=\"",stdout);html(ref);fputs("\">",stdout);html(it->title);fputs("</option>",stdout);}
    sqlite3_finalize(row);row=NULL;
    if(reads)wl_club_options(db,site_id,world,space);
    fputs("</select></label><label>Why this one<input name=\"note\" maxlength=\"280\"></label></div><button type=\"submit\">Propose</button></form></section>",stdout);
    /* talk */
    fputs("<section class=\"aw-talk\" aria-labelledby=\"aw-talk-h\"><h2 id=\"aw-talk-h\" class=\"region-label\">Talk</h2>",stdout);
    cine_room_form(site_id,space,"say");fputs("<label class=\"aw-say\">Say something to the club<textarea name=\"body\" rows=\"3\" maxlength=\"1000\" required></textarea></label><button type=\"submit\">Post</button></form><ol>",stdout);
    int count=0;sqlite3_int64 last=0;
    if(aw_club_posts(db,viewer,site_id,space,before,&row)==1)while(sqlite3_step(row)==SQLITE_ROW){
        if(++count>AW_CLUB_PAGE)break;
        sqlite3_int64 id=sqlite3_column_int64(row,0);const char *author=(const char*)sqlite3_column_text(row,1),*ref=(const char*)sqlite3_column_text(row,2),
        *words=(const char*)sqlite3_column_text(row,3),*st=(const char*)sqlite3_column_text(row,4);last=id;char n[24];snprintf(n,sizeof(n),"%lld",(long long)id);
        int mine=!strcmp(author,viewer);cine_name(auth_db,author,name);
        printf("<li data-club-post=\"%lld\" data-state=\"",(long long)id);html(st);fputs("\"><p class=\"by\">",stdout);html(mine?"You":name);
        if(*ref){const PageItem *it=cine_film(db,&films,ref);if(it){fputs(" on ",stdout);html(it->title);}else if(!strncmp(ref,"works/",6)){fputs(" on ",stdout);wl_club_title(db,&site,world,ref,0);}}
        if(!strcmp(st,"hidden"))fputs(" &middot; hidden from members",stdout);fputs("</p>",stdout);
        if(!strcmp(st,"retracted"))fputs("<p class=\"aw-body\">Withdrawn by its author.</p>",stdout);else{fputs("<p class=\"aw-note\">",stdout);html(words);fputs("</p>",stdout);}
        fputs("<div class=\"aw-acts\">",stdout);
        if(mine&&strcmp(st,"retracted")){cine_room_form(site_id,space,"retract");ph_hidden("item",n);fputs("<button type=\"submit\" class=\"aw-quiet\">Withdraw</button></form>",stdout);}
        if(host&&!mine){char w[24];snprintf(w,sizeof(w),"%lld",(long long)world);
            if(!strcmp(st,"shown")){fputs("<form method=\"post\" action=\"/workspace/world/film\">",stdout);ph_hidden("site",site_id);ph_hidden("world",w);ph_hidden("operation","hide");ph_hidden("space",space);
                ph_hidden("kind","post");ph_hidden("item",n);ph_hidden("return","club");fputs("<button type=\"submit\" class=\"aw-quiet\">Hide</button></form>",stdout);}
            fputs("<form method=\"post\" action=\"/workspace/world/film\">",stdout);ph_hidden("site",site_id);ph_hidden("world",w);ph_hidden("operation","remove");ph_hidden("space",space);
            ph_hidden("member",author);ph_hidden("return","club");fputs("<button type=\"submit\" class=\"aw-quiet\">Take out of the club</button></form>",stdout);}
        fputs("</div></li>",stdout);
    }
    sqlite3_finalize(row);row=NULL;if(!count)fputs("<li><p class=\"aw-body\">Nobody has said anything yet.</p></li>",stdout);fputs("</ol>",stdout);
    if(count>AW_CLUB_PAGE){fputs("<p><a class=\"aw-door\" rel=\"next\" href=\"/workspace/worlds/club?site=",stdout);url_query_value(site_id);fputs("&amp;space=",stdout);url_query_value(space);printf("&amp;before=%lld\">Older posts</a></p>",(long long)last);}
    else if(before){fputs("<p><a class=\"aw-door\" href=\"/workspace/worlds/club?site=",stdout);url_query_value(site_id);fputs("&amp;space=",stdout);url_query_value(space);fputs("\">Back to the newest</a></p>",stdout);}
    fputs("</section>",stdout);
    if(!host){fputs("<section class=\"aw-leave\">",stdout);cine_room_form(site_id,space,"leave");fputs("<button type=\"submit\" class=\"aw-quiet\">Leave the club</button></form></section>",stdout);}
    world_room_close();cine_films_close(&films);sqlite3_close(db);return 1;
}
#endif
