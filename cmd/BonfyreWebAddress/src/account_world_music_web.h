#ifndef BONFYRE_ACCOUNT_WORLD_MUSIC_WEB_H
#define BONFYRE_ACCOUNT_WORLD_MUSIC_WEB_H
#include "../../BonfyreCMS/src/account_world_music.h"

/* Artists, labels, releases and listening rooms in account spaces
 * (account-world-music-1).
 *
 * /workspace/world-music          private API: identity, works, recordings, releases,
 *                                 tracks, credits, history, rooms; fans collect
 * /workspace/worlds/collection    a signed-in listener's own collection
 * studio section on /workspace/worlds
 * /worlds/X                       discography, label roster, history, rooms, search
 * /worlds/X/release/ID            tracklist, credits, liner notes, editions, cover
 * /worlds/X/recording/ID          performers, the work, time marks, where it was heard
 * /worlds/X/work/ID               writers and every recording of it
 * /worlds/X/listen/ID             a listening room at an announced time
 * /worlds/X/releases.xml          RSS 2.0 of the space's releases
 *
 * Playback reuses the sound additions: a recording plays only through
 * wa_open_audio (its source, re-read on this request, states public domain,
 * CC0, CC BY or CC BY-SA and gives an https file), and its time marks are the
 * account_world_mark rows of the space that holds its piece. Anything else is
 * refused in words and linked to where it is published. Nothing here copies
 * media, states rights or counts listeners in public. Included from
 * account_world_web.h after the sound additions. */

/* -------------------------------------------------------------- helpers */
static const char *wm_col(sqlite3_stmt *s,int i){const char *v=(const char*)sqlite3_column_text(s,i);return v?v:"";}
typedef struct{PageMaterial *m;PageItem *it;char loaded[96];} WmPieces;
static void wm_open(WmPieces *p){p->m=calloc(1,sizeof(*p->m));p->it=calloc(1,sizeof(*p->it));p->loaded[0]=0;}
static void wm_close(WmPieces *p){free(p->m);free(p->it);p->m=NULL;p->it=NULL;}
static const PageItem *wm_piece(sqlite3 *db,WmPieces *p,const char *ref){
    if(!p->m||!p->it||!ref||!*ref||!strchr(ref,'/'))return NULL;
    return world_web_resolve(db,ref,p->m,p->loaded,p->it)?p->it:NULL;
}
static int wm_open_photo(const PageItem *it){
    return it&&it->kind==BF_PAGE_MATERIAL_PHOTO&&world_web_open_terms(it->rights)&&world_web_https(it->image)&&page_image_url_safe(it->image);
}
static void wm_words(const char *s){ /* "liner-notes" -> "Liner notes" */
    char out[48];size_t n=0;for(;s&&*s&&n+1<sizeof(out);s++)out[n++]=*s=='-'?' ':*s;out[n]=0;if(n)out[0]=(char)toupper((unsigned char)out[0]);html(out);
}
static const char *wm_type(const char *t){
    return !strcmp(t,"album")?"Album":!strcmp(t,"ep")?"EP":!strcmp(t,"single")?"Single":!strcmp(t,"compilation")?"Compilation":
           !strcmp(t,"live")?"Live album":!strcmp(t,"soundtrack")?"Soundtrack":"Release";
}
static const char *wm_relation(const char *k){
    return !strcmp(k,"member-of")?"Member of":!strcmp(k,"founded")?"Founded by":!strcmp(k,"signed-to")?"Signed to":
           !strcmp(k,"collaborated-with")?"Worked with":!strcmp(k,"split-from")?"Split from":!strcmp(k,"also-known-as")?"Also known as":
           !strcmp(k,"influenced-by")?"Influenced by":k;
}
static void wm_date(const char *d){ /* partial dates as people say them */
    static const char *mo[]={"January","February","March","April","May","June","July","August","September","October","November","December"};
    size_t n=d?strlen(d):0;if(!n)return;
    fputs("<time datetime=\"",stdout);html(d);fputs("\">",stdout);
    if(n==4)html(d);else{int m=atoi(d+5);if(m<1||m>12){html(d);}else if(n==7)printf("%s %.4s",mo[m-1],d);else printf("%d %s %.4s",atoi(d+8),mo[m-1],d);}
    fputs("</time>",stdout);
}
static void wm_href_open(const char *href){ /* "", https, or space:<id> */
    if(!strncmp(href,"https://",8)){fputs("<a rel=\"noopener\" href=\"",stdout);html(href);fputs("\">",stdout);}
    else if(!strncmp(href,"space:",6)){fputs("<a href=\"/worlds/",stdout);html(href+6);fputs("\">",stdout);}
}
static void wm_ms(long ms){if(ms>0)printf("%ld:%02ld",ms/60000,(ms/1000)%60);}
/* Why a recording is not played, in words, and where to hear it instead. */
static void wm_refusal(const PageItem *it){
    fputs("<p class=\"aw-terms\" data-music-refused>",stdout);
    if(!it){fputs("This recording is no longer available from where it was published.</p>",stdout);return;}
    if(it->kind!=BF_PAGE_MATERIAL_AUDIO)fputs("Its source does not publish it as a sound recording, so it is not played here.",stdout);
    else if(!world_web_open_terms(it->rights)){fputs("Not played here: its source states ",stdout);
        if(it->rights[0]){fputs("&ldquo;",stdout);html(it->rights);fputs("&rdquo;",stdout);}else fputs("no terms",stdout);
        fputs(", and only openly licensed or public-domain recordings play on this site.",stdout);}
    else fputs("Its source states open terms but gives no file to play.",stdout);
    if(public_http_url(it->href)){fputs(" <a rel=\"noopener\" href=\"",stdout);html(it->href);fputs("\">Listen where it is published</a>",stdout);}
    fputs("</p>",stdout);
}
static void wm_player(const PageItem *it){
    fputs("<audio controls preload=\"none\" src=\"",stdout);html(it->asset);fputs("\"></audio><p class=\"aw-terms\" data-music-terms>",stdout);
    if(world_web_https(it->rights_href)){fputs("<a rel=\"license noopener\" href=\"",stdout);html(it->rights_href);fputs("\">",stdout);html(it->rights);fputs("</a>",stdout);}else html(it->rights);
    fputs(" &middot; ",stdout);if(public_http_url(it->href)){fputs("<a rel=\"noopener\" href=\"",stdout);html(it->href);fputs("\">From ",stdout);html(it->source);fputs("</a>",stdout);}else{fputs("From ",stdout);html(it->source);}
    fputs("</p>",stdout);
}
/* Credits on one target, "Vocals Jo Reyes · Piano Avery Lane". */
static int wm_credits(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 world,const char *space,const char *target,const char *id,const char *attr){
    char key[80];snprintf(key,sizeof(key),"%s:%s",target,id);sqlite3_stmt *s=NULL;int n=0;
    if(awm_rows(db,subject,ns,world,space,AWM_CREDITS,key,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!n++){fputs("<dl class=\"aw-credits\" ",stdout);fputs(attr,stdout);fputs(">",stdout);}
        const char *role=wm_col(s,3),*detail=wm_col(s,5),*href=wm_col(s,6);
        fputs("<div><dt>",stdout);if(!strcmp(role,"instrument")&&*detail)wm_words(detail);else{wm_words(role);if(*detail){fputs(" (",stdout);html(detail);fputs(")",stdout);}}
        fputs("</dt><dd>",stdout);wm_href_open(href);html(wm_col(s,4));if(*href)fputs("</a>",stdout);fputs("</dd></div>",stdout);
    }
    sqlite3_finalize(s);if(n)fputs("</dl>",stdout);return n;
}
typedef struct{char kind[8],name[164],mbid[40],area[124],begun[8];} WmParty;
static int wm_party(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 world,const char *space,WmParty *p){
    sqlite3_stmt *s=NULL;memset(p,0,sizeof(*p));
    if(awm_rows(db,subject,ns,world,space,AWM_PARTY,NULL,&s)!=1||sqlite3_step(s)!=SQLITE_ROW){sqlite3_finalize(s);return 0;}
    snprintf(p->kind,sizeof(p->kind),"%s",wm_col(s,0));snprintf(p->name,sizeof(p->name),"%s",wm_col(s,1));snprintf(p->mbid,sizeof(p->mbid),"%s",wm_col(s,2));
    snprintf(p->area,sizeof(p->area),"%s",wm_col(s,3));snprintf(p->begun,sizeof(p->begun),"%s",wm_col(s,4));sqlite3_finalize(s);return 1;
}
typedef struct{char id[65],title[260],type[16],on[12],label[164],catalog[68],cover[100],notes[AWM_NOTES_LIMIT+1],edition[65],mbid[40];int ready;} WmRelease;
static int wm_release(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 world,const char *space,const char *id,WmRelease *r){
    sqlite3_stmt *s=NULL;memset(r,0,sizeof(*r));
    if(awm_rows(db,subject,ns,world,space,AWM_RELEASES,id,&s)!=1||sqlite3_step(s)!=SQLITE_ROW){sqlite3_finalize(s);return 0;}
    char *dst[]={r->id,r->title,r->type,r->on,r->label,r->catalog,r->cover,r->notes,r->edition,r->mbid};
    size_t cap[]={sizeof(r->id),sizeof(r->title),sizeof(r->type),sizeof(r->on),sizeof(r->label),sizeof(r->catalog),sizeof(r->cover),sizeof(r->notes),sizeof(r->edition),sizeof(r->mbid)};
    for(int i=0;i<10;i++)snprintf(dst[i],cap[i],"%s",wm_col(s,i));r->ready=sqlite3_column_int(s,10);sqlite3_finalize(s);return 1;
}
typedef struct{char id[65],work[65],title[260],ref[100],isrc[16],mbid[40];} WmRecording;
static int wm_recording(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 world,const char *space,const char *id,WmRecording *r){
    sqlite3_stmt *s=NULL;memset(r,0,sizeof(*r));
    if(awm_rows(db,subject,ns,world,space,AWM_RECORDINGS,id,&s)!=1||sqlite3_step(s)!=SQLITE_ROW){sqlite3_finalize(s);return 0;}
    char *dst[]={r->id,r->work,r->title,r->ref,r->isrc,r->mbid};size_t cap[]={sizeof(r->id),sizeof(r->work),sizeof(r->title),sizeof(r->ref),sizeof(r->isrc),sizeof(r->mbid)};
    for(int i=0;i<6;i++)snprintf(dst[i],cap[i],"%s",wm_col(s,i));sqlite3_finalize(s);return 1;
}
static void wm_label(const char *label){
    if(!strncmp(label,"space:",6)){fputs("<a href=\"/worlds/",stdout);html(label+6);fputs("\">",stdout);
        /* the label's own name when its space is public; the address otherwise */
        html(label+6);fputs("</a>",stdout);}else html(label);
}
static void wm_mb(const char *kind,const char *mbid){
    if(!*mbid)return;fputs(" <a class=\"aw-mb\" rel=\"noopener\" href=\"https://musicbrainz.org/",stdout);fputs(kind,stdout);fputs("/",stdout);html(mbid);fputs("\">MusicBrainz</a>",stdout);
}

/* -------------------------------------------------------------- private API */
enum{WM_SITE,WM_WORLD,WM_SPACE,WM_REVISION,WM_OP,WM_RETURN,WM_KIND,WM_NAME,WM_MBID,WM_AREA,WM_BEGUN,WM_ENDED,WM_ID,WM_WORK,WM_TITLE,WM_REF,
     WM_CODE,WM_DATE,WM_LABEL,WM_CATALOG,WM_COVER,WM_NOTES,WM_EDITION,WM_RECORDING,WM_DISC,WM_POSITION,WM_OF,WM_ROLE,WM_DETAIL,WM_HREF,WM_NUMBER,WM_READY,WM_COUNT};
static void wm_json_table(bf_jsonw *w,sqlite3 *db,const char *subject,const char *site,sqlite3_int64 world,const char *space,int table,const char *name){
    sqlite3_stmt *s=NULL;bf_jsonw_key(w,name);bf_jsonw_arr(w);
    if(awm_rows(db,subject,site,world,space,table,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){bf_jsonw_obj(w);
        for(int i=0;i<sqlite3_column_count(s);i++){bf_jsonw_key(w,sqlite3_column_name(s,i));if(sqlite3_column_type(s,i)==SQLITE_INTEGER)bf_jsonw_int(w,sqlite3_column_int64(s,i));else bf_jsonw_str(w,wm_col(s,i));}
        bf_jsonw_obj_end(w);}
    sqlite3_finalize(s);bf_jsonw_arr_end(w);
}
static void wm_collection_location(const char *site,const char *space,const char *release){
    char location[400];snprintf(location,sizeof(location),"/workspace/worlds/collection?site=%s&space=%s&release=%s",site,space,release);page_redirect(location);
}
static int world_music_api(const char *method,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db){
    int post=!strcmp(method,"POST"),get=!strcmp(method,"GET");
    if(!post&&!get){aw_error(405,"Use GET or POST.\n");return 1;}
    if(post&&(!ph_origin(raw,host)||(query&&*query))){aw_error(403,"Submit from this workspace.\n");return 1;}
    static const char *names[WM_COUNT]={"site","world","space","revision","operation","return","kind","name","mbid","area","begun","ended","id","work","title","ref",
        "code","date","label","catalog","cover","notes","edition","recording","disc","position","of","role","detail","href","number","ready"};
    static const size_t caps[WM_COUNT]={128,24,65,24,24,12,24,161,37,121,5,5,65,65,257,161,16,17,161,65,161,AWM_NOTES_LIMIT+1,65,65,4,4,80,24,65,513,24,4};
    char *buf=calloc(WM_COUNT,AWM_NOTES_LIMIT+2);if(!buf){aw_error(503,"Workspace unavailable.\n");return 1;}
    char *v[WM_COUNT];PhField fields[WM_COUNT];
    for(int i=0;i<WM_COUNT;i++){v[i]=buf+(size_t)i*(AWM_NOTES_LIMIT+2);fields[i].key=names[i];fields[i].value=v[i];fields[i].cap=caps[i];fields[i].multiline=i==WM_NOTES;}
    #define WM_FAIL(code,text) do{free(buf);aw_error(code,text);return 1;}while(0)
    if(!ph_fields(post?body:query,fields,WM_COUNT,AWM_NOTES_LIMIT*3+8192)||!*v[WM_SITE]||!*v[WM_SPACE])WM_FAIL(400,"Invalid workspace request.\n");
    const char *op=v[WM_OP],*ret=v[WM_RETURN];
    int member=!strcmp(op,"collect")||!strcmp(op,"uncollect");
    if(get&&strcmp(op,"draft"))WM_FAIL(400,"Invalid workspace request.\n");
    if(*ret&&strcmp(ret,member?"collection":"studio"))WM_FAIL(400,"Invalid workspace coordinates.\n");
    sqlite3_int64 world=0,rev=0;
    if(!member&&(!world_web_number(v[WM_WORLD],&world,0)||(post&&!world_web_number(v[WM_REVISION],&rev,0))))WM_FAIL(400,"Invalid workspace request.\n");
    if(member&&(*v[WM_WORLD]||*v[WM_REVISION]||!*v[WM_ID]))WM_FAIL(400,"Invalid workspace request.\n");
    const char *path=getenv("BONFYRE_CMS_DB");sqlite3 *db=NULL;char subject[160],again[160],actor[160];const char *scope=NULL;const char *site=v[WM_SITE],*space=v[WM_SPACE];
    if(!path||!*path||sqlite3_open_v2(path,&db,post?SQLITE_OPEN_READWRITE:SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){if(db)sqlite3_close(db);WM_FAIL(503,"Workspace unavailable.\n");}
    sqlite3_busy_timeout(db,2000);PhSite where;
    #define WM_ADMIT(out) (member?(psetup_context(bearer,auth_db,"self",post?2:0,actor,sizeof(actor),out,160,&scope)==1&&ph_site(db,NULL,site,&where)):world_web_admit(bearer,auth_db,db,site,post,out))
    if(!WM_ADMIT(subject)){sqlite3_close(db);WM_FAIL(404,"Workspace unavailable.\n");}
    if(post&&awm_initialize(db)!=1){sqlite3_close(db);WM_FAIL(503,"Workspace unavailable.\n");}
    if(!WM_ADMIT(again)||strcmp(subject,again)){sqlite3_close(db);WM_FAIL(404,"Workspace unavailable.\n");}
    #undef WM_ADMIT
    int r=0,R=(int)rev;sqlite3_int64 a=0,b=0;
    if(get){
        sqlite3_stmt *probe=NULL;r=aw_space_get(db,subject,site,world,space,&probe);sqlite3_finalize(probe);
        if(r!=1){sqlite3_close(db);WM_FAIL(r<0?503:404,"Workspace request could not be completed.\n");}
        bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"state");bf_jsonw_str(&w,"draft");
        static const char *tn[]={"party","works","recordings","releases","tracks","credits","relations","rooms"};
        for(int t=AWM_PARTY;t<=AWM_ROOMS;t++)wm_json_table(&w,db,subject,site,world,space,t,tn[t]);
        bf_jsonw_key(&w,"collectors");bf_jsonw_arr(&w);sqlite3_stmt *c=NULL;
        if(awm_collectors(db,subject,site,world,space,&c)==1)while(sqlite3_step(c)==SQLITE_ROW){bf_jsonw_obj(&w);bf_jsonw_key(&w,"release_id");bf_jsonw_str(&w,wm_col(c,0));bf_jsonw_key(&w,"collectors");bf_jsonw_int(&w,sqlite3_column_int64(c,1));bf_jsonw_obj_end(&w);}
        sqlite3_finalize(c);bf_jsonw_arr_end(&w);bf_jsonw_obj_end(&w);sqlite3_close(db);free(buf);
        if(w.oom)aw_error(503,"Workspace unavailable.\n");else aw_response(200,"application/json",w.buf,w.len,0);bf_jsonw_free(&w);return 1;
    }
    char target[16]="",target_id[65]="";
    if(*v[WM_OF]){const char *colon=strchr(v[WM_OF],':');if(colon&&(size_t)(colon-v[WM_OF])<sizeof(target)&&strlen(colon+1)<sizeof(target_id)){memcpy(target,v[WM_OF],(size_t)(colon-v[WM_OF]));target[colon-v[WM_OF]]=0;snprintf(target_id,sizeof(target_id),"%s",colon+1);}}
    if(!strcmp(op,"party"))r=awm_party_put(db,subject,site,world,space,v[WM_KIND],v[WM_NAME],v[WM_MBID],v[WM_AREA],v[WM_BEGUN],R);
    else if(!strcmp(op,"work"))r=awm_work_put(db,subject,site,world,space,v[WM_ID],v[WM_TITLE],v[WM_CODE],v[WM_MBID],R);
    else if(!strcmp(op,"recording"))r=awm_recording_put(db,subject,site,world,space,v[WM_ID],v[WM_WORK],v[WM_TITLE],v[WM_REF],v[WM_CODE],v[WM_MBID],R);
    else if(!strcmp(op,"release"))r=awm_release_put(db,subject,site,world,space,v[WM_ID],v[WM_TITLE],v[WM_KIND],v[WM_DATE],v[WM_LABEL],v[WM_CATALOG],v[WM_COVER],v[WM_NOTES],v[WM_EDITION],v[WM_MBID],R);
    else if(!strcmp(op,"ready")){if(!strcmp(v[WM_READY],"1")||!strcmp(v[WM_READY],"0"))r=awm_release_ready(db,subject,site,world,space,v[WM_ID],v[WM_READY][0]=='1',R);}
    else if(!strcmp(op,"track")||!strcmp(op,"untrack")){
        if(world_web_number(v[WM_DISC],&a,0)&&a<=9&&world_web_number(v[WM_POSITION],&b,0)&&b<=99)
            r=op[0]=='t'?awm_track_put(db,subject,site,world,space,v[WM_ID],(int)a,(int)b,v[WM_RECORDING],R):awm_track_remove(db,subject,site,world,space,v[WM_ID],(int)a,(int)b,R);
    }
    else if(!strcmp(op,"credit"))r=awm_credit_put(db,subject,site,world,space,target,target_id,v[WM_ROLE],v[WM_NAME],v[WM_DETAIL],v[WM_HREF],R);
    else if(!strcmp(op,"uncredit")){if(world_web_number(v[WM_NUMBER],&a,0))r=awm_credit_remove(db,subject,site,world,space,a,R);}
    else if(!strcmp(op,"relation"))r=awm_relation_put(db,subject,site,world,space,v[WM_KIND],v[WM_NAME],v[WM_HREF],v[WM_BEGUN],v[WM_ENDED],R);
    else if(!strcmp(op,"unrelation")){if(world_web_number(v[WM_NUMBER],&a,0))r=awm_relation_remove(db,subject,site,world,space,a,R);}
    else if(!strcmp(op,"room"))r=awm_room_put(db,subject,site,world,space,v[WM_ID],v[WM_DATE],target,target_id,v[WM_NOTES],v[WM_HREF],R);
    else if(!strcmp(op,"room-state"))r=awm_room_state(db,subject,site,world,space,v[WM_ID],v[WM_KIND],R);
    else if(!strcmp(op,"remove"))r=awm_remove(db,subject,site,world,space,v[WM_KIND],v[WM_ID],R);
    else if(!strcmp(op,"collect"))r=awm_collect(db,subject,site,space,v[WM_ID]);
    else if(!strcmp(op,"uncollect"))r=awm_uncollect(db,subject,site,space,v[WM_ID]);
    else{sqlite3_close(db);WM_FAIL(400,"Invalid workspace request.\n");}
    sqlite3_close(db);
    if(r!=1)WM_FAIL(r<0?503:409,"Workspace request could not be completed.\n");
    if(*ret){if(member)wm_collection_location(site,space,v[WM_ID]);else{char where_to[80];snprintf(where_to,sizeof(where_to),"%s#music",space);world_web_studio_location(site,world,where_to);}free(buf);return 1;}
    free(buf);aw_response(200,"application/json","{\"state\":\"saved\"}",17,0);return 1;
    #undef WM_FAIL
}

/* -------------------------------------------------------------- studio */
static void wm_form(const char *site,sqlite3_int64 world,const char *space,const char *op,long long revision,const char *name){
    fputs("<form method=\"post\" action=\"/workspace/world-music\"",stdout);if(name){fputs(" data-music-form=\"",stdout);html(name);fputs("\"",stdout);}fputs(">",stdout);
    char n[24];ph_hidden("site",site);snprintf(n,sizeof(n),"%lld",(long long)world);ph_hidden("world",n);ph_hidden("space",space);
    ph_hidden("operation",op);snprintf(n,sizeof(n),"%lld",revision);ph_hidden("revision",n);ph_hidden("return","studio");
}
static void wm_input(const char *label,const char *hint,const char *name,const char *value,int max,int required,const char *pattern){
    fputs("<label>",stdout);fputs(label,stdout);if(hint){fputs("<small>",stdout);fputs(hint,stdout);fputs("</small>",stdout);}
    printf("<input name=\"%s\" maxlength=\"%d\"",name,max);if(required)fputs(" required",stdout);if(pattern){fputs(" pattern=\"",stdout);fputs(pattern,stdout);fputs("\"",stdout);}
    fputs(" value=\"",stdout);html(value?value:"");fputs("\"></label>",stdout);
}
static void wm_select(const char *label,const char *name,const char *const *values,const char *const *labels,const char *chosen){
    fputs("<label>",stdout);fputs(label,stdout);printf("<select name=\"%s\">",name);
    for(int i=0;values[i];i++){fputs("<option value=\"",stdout);html(values[i]);fputs("\"",stdout);if(chosen&&!strcmp(chosen,values[i]))fputs(" selected",stdout);fputs(">",stdout);fputs(labels[i],stdout);fputs("</option>",stdout);}
    fputs("</select></label>",stdout);
}
static void wm_remove_button(const char *site,sqlite3_int64 world,const char *space,long long revision,const char *op,const char *field,const char *value,const char *kind){
    wm_form(site,world,space,op,revision,NULL);ph_hidden(field,value);if(kind)ph_hidden("kind",kind);fputs("<button type=\"submit\" class=\"aw-quiet\">Remove</button></form>",stdout);
}
/* What a listener would get from this piece today, checked against its source. */
static void wm_media_check(const PageItem *it,int cover){
    fputs("<span class=\"aw-flag\"",stdout);
    if(cover){int ok=wm_open_photo(it);if(ok)fputs(" data-ok",stdout);fputs(" data-media-check=\"",stdout);fputs(ok?"shown":it?"linked":"missing",stdout);fputs("\">",stdout);
        fputs(ok?"Cover shown":!it?"Not found at its source":it->kind!=BF_PAGE_MATERIAL_PHOTO?"Not a picture":"Cover linked, terms not open",stdout);}
    else{int ok=wa_open_audio(it);if(ok)fputs(" data-ok",stdout);fputs(" data-media-check=\"",stdout);fputs(ok?"plays":it?"linked":"missing",stdout);fputs("\">",stdout);
        fputs(ok?"Plays here":!it?"Not found at its source":it->kind!=BF_PAGE_MATERIAL_AUDIO?"Not a sound recording":!world_web_open_terms(it->rights)?"Linked, terms not open":"No file to play",stdout);}
    fputs("</span>",stdout);
    if(it&&it->rights[0]){fputs(" <small>",stdout);html(it->rights);fputs("</small>",stdout);}
}
static void wm_studio_style(void){
    fputs("<style>.aw-music-studio details{border-top:1px solid var(--ph-line);padding:12px 0}.aw-music-studio summary{cursor:pointer;font:600 14px system-ui;min-height:32px}"
          ".aw-music-studio details[open] summary{margin-bottom:12px}.aw-music-studio table{border-collapse:collapse;width:100%;font-size:14px}"
          ".aw-music-studio td,.aw-music-studio th{text-align:left;padding:8px 6px;border-bottom:1px solid var(--ph-line);vertical-align:top}.aw-music-studio td form{display:inline-flex;margin:0 4px 4px 0}"
          ".aw-music-studio .aw-flag{font:600 11px ui-monospace,monospace;letter-spacing:.06em;text-transform:uppercase;color:#8a4b12}.aw-music-studio .aw-flag[data-ok]{color:#2b4c36}"
          ".aw-music-studio ul.aw-checks{margin:0;padding-left:18px;font-size:14px}.aw-music-studio ul.aw-checks li[data-ok]{color:#2b4c36}.aw-music-studio ul.aw-checks li:not([data-ok]){color:#8a4b12}"
          "@media(max-width:640px){.aw-music-studio table,.aw-music-studio tbody,.aw-music-studio tr,.aw-music-studio td{display:block}.aw-music-studio th{display:none}.aw-music-studio tr{border-bottom:1px solid var(--ph-line);padding:8px 0}.aw-music-studio td{border:0;padding:3px 0}}</style>",stdout);
}
static void world_music_studio(sqlite3 *db,const char *subject,const char *site_id,sqlite3_int64 world,const char *space,long long revision){
    WmParty party;int have=wm_party(db,subject,site_id,world,space,&party);WmPieces P;wm_open(&P);
    wm_studio_style();
    fputs("<section id=\"music\" class=\"aw-music-studio\" data-world-music-studio><h3>Artist or label</h3>"
          "<p>Keep a discography here: songs as works, the recordings of them, and the releases they came out on. A recording plays for listeners only when its own source states open terms, such as CC BY or public domain; anything else is linked to where it is published, with the reason given.</p>",stdout);
    wm_form(site_id,world,space,"party",revision,"party");
    static const char *const kinds[]={"artist","label",NULL},*const kind_words[]={"An artist or band","A record label",NULL};
    fputs("<div class=\"aw-row\">",stdout);wm_select("This space is","kind",kinds,kind_words,have?party.kind:"artist");
    wm_input("Name",NULL,"name",have?party.name:"",160,1,NULL);wm_input("From","A city or region","area",have?party.area:"",120,0,NULL);
    wm_input("Since","A year","begun",have?party.begun:"",4,0,"[0-9]{4}");fputs("</div>",stdout);
    wm_input("MusicBrainz identifier","Optional. Links listeners to the shared record of who this is","mbid",have?party.mbid:"",36,0,"[0-9a-f-]{36}");
    fputs("<div class=\"aw-actions\"><button type=\"submit\">Save</button></div></form>",stdout);
    if(!have){fputs("</section>",stdout);wm_close(&P);return;}
    /* works */
    sqlite3_stmt *s=NULL;int n=0;char works[64][65];int nworks=0;
    fputs("<h3>Songs and compositions</h3><p>A work is the song itself, whoever records it. Writers are credited here.</p>",stdout);
    if(awm_rows(db,subject,site_id,world,space,AWM_WORKS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!n++)fputs("<table data-music-works><thead><tr><th>Work</th><th>Writers</th><th></th></tr></thead><tbody>",stdout);
        const char *id=wm_col(s,0);if(nworks<64)snprintf(works[nworks++],65,"%s",id);
        fputs("<tr data-work=\"",stdout);html(id);fputs("\"><td><strong>",stdout);html(wm_col(s,1));fputs("</strong>",stdout);if(*wm_col(s,2)){fputs(" <code>",stdout);html(wm_col(s,2));fputs("</code>",stdout);}
        fputs("</td><td>",stdout);if(!wm_credits(db,subject,site_id,world,space,"work",id,"data-credits"))fputs("No writers credited yet",stdout);
        fputs("</td><td>",stdout);wm_remove_button(site_id,world,space,revision,"remove","id",id,"work");fputs("</td></tr>",stdout);
    }
    sqlite3_finalize(s);if(n)fputs("</tbody></table>",stdout);
    wm_form(site_id,world,space,"work",revision,"work");fputs("<div class=\"aw-row\">",stdout);
    wm_input("Title",NULL,"title","",256,1,NULL);wm_input("Address","Lowercase, used in the link","id","",64,1,"[a-z0-9][a-z0-9-]*");
    wm_input("ISWC","Optional, such as T-345.246.800-1","code","",15,0,NULL);fputs("</div><div class=\"aw-actions\"><button type=\"submit\">Add the work</button></div></form>",stdout);
    /* recordings */
    char recs[128][65];int nrecs=0;n=0;
    fputs("<h3>Recordings</h3><p>A recording is one performance of a work. Performers are credited here, and the check shows what a listener gets from its source today.</p>",stdout);
    if(awm_rows(db,subject,site_id,world,space,AWM_RECORDINGS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!n++)fputs("<table data-music-recordings><thead><tr><th>Recording</th><th>Listening</th><th>Performers</th><th></th></tr></thead><tbody>",stdout);
        const char *id=wm_col(s,0),*ref=wm_col(s,3);if(nrecs<128)snprintf(recs[nrecs++],65,"%s",id);
        fputs("<tr data-recording=\"",stdout);html(id);fputs("\"><td><strong>",stdout);html(wm_col(s,2));fputs("</strong><br><code>",stdout);html(ref);fputs("</code>",stdout);
        if(*wm_col(s,1)){fputs("<br>Records ",stdout);html(wm_col(s,1));}if(*wm_col(s,4)){fputs("<br>ISRC ",stdout);html(wm_col(s,4));}
        fputs("</td><td>",stdout);wm_media_check(wm_piece(db,&P,ref),0);fputs("</td><td>",stdout);
        if(!wm_credits(db,subject,site_id,world,space,"recording",id,"data-credits"))fputs("No performers credited yet",stdout);
        fputs("</td><td>",stdout);wm_remove_button(site_id,world,space,revision,"remove","id",id,"recording");fputs("</td></tr>",stdout);
    }
    sqlite3_finalize(s);if(n)fputs("</tbody></table>",stdout);
    wm_form(site_id,world,space,"recording",revision,"recording");fputs("<div class=\"aw-row\">",stdout);
    wm_input("Title",NULL,"title","",256,1,NULL);wm_input("Address","Lowercase, used in the link","id","",64,1,"[a-z0-9][a-z0-9-]*");
    wm_input("Audio piece","The address after /o/ on the recording&rsquo;s page","ref","",120,1,NULL);fputs("</div><div class=\"aw-row\">",stdout);
    fputs("<label>Records the work<select name=\"work\"><option value=\"\">No work named</option>",stdout);
    for(int i=0;i<nworks;i++){fputs("<option value=\"",stdout);html(works[i]);fputs("\">",stdout);html(works[i]);fputs("</option>",stdout);}
    fputs("</select></label>",stdout);wm_input("ISRC","Optional, 12 characters","code","",12,0,"[A-Z]{2}[A-Z0-9]{3}[0-9]{7}");
    fputs("</div><div class=\"aw-actions\"><button type=\"submit\">Add the recording</button></div></form>",stdout);
    /* releases */
    fputs("<h3>Releases</h3><p>Albums, EPs and singles, in track order. A release reaches listeners once you mark it ready and publish the space; any later change returns it to a draft for another look.</p>",stdout);
    sqlite3_stmt *col=NULL;char counted[AWM_RELEASE_LIMIT][65];long long counts[AWM_RELEASE_LIMIT];int ncounted=0;
    if(awm_collectors(db,subject,site_id,world,space,&col)==1)while(ncounted<AWM_RELEASE_LIMIT&&sqlite3_step(col)==SQLITE_ROW){snprintf(counted[ncounted],65,"%s",wm_col(col,0));counts[ncounted++]=sqlite3_column_int64(col,1);}
    sqlite3_finalize(col);
    char rels[AWM_RELEASE_LIMIT][65];int nrels=0;
    if(awm_rows(db,subject,site_id,world,space,AWM_RELEASES,NULL,&s)==1)while(nrels<AWM_RELEASE_LIMIT&&sqlite3_step(s)==SQLITE_ROW)snprintf(rels[nrels++],65,"%s",wm_col(s,0));
    sqlite3_finalize(s);
    WmRelease *rel=calloc(1,sizeof(*rel));
    for(int i=0;rel&&i<nrels;i++){
        if(!wm_release(db,subject,site_id,world,space,rels[i],rel))continue;
        fputs("<details data-music-release=\"",stdout);html(rel->id);fputs("\"><summary>",stdout);html(rel->title);fputs(" &middot; ",stdout);fputs(wm_type(rel->type),stdout);
        if(rel->on[0]){fputs(" &middot; ",stdout);html(rel->on);}fputs(" <span class=\"aw-flag\"",stdout);if(rel->ready)fputs(" data-ok",stdout);fputs(" data-release-state>",stdout);fputs(rel->ready?"Ready":"Draft",stdout);fputs("</span>",stdout);
        for(int c=0;c<ncounted;c++)if(!strcmp(counted[c],rel->id))printf(" <small data-collectors=\"%lld\">in %lld listener collection%s</small>",counts[c],counts[c],counts[c]==1?"":"s");
        fputs("</summary>",stdout);
        /* tracklist with media checks */
        sqlite3_stmt *t=NULL;int tracks=0,playable=0,missing=0,uncredited=0,maxpos=0;
        if(awm_rows(db,subject,site_id,world,space,AWM_TRACKS,rel->id,&t)==1)while(sqlite3_step(t)==SQLITE_ROW){
            if(!tracks++)fputs("<table data-music-tracks><thead><tr><th>#</th><th>Recording</th><th>Listening</th><th></th></tr></thead><tbody>",stdout);
            int disc=sqlite3_column_int(t,1),pos=sqlite3_column_int(t,2);if(disc==1&&pos>maxpos)maxpos=pos;WmRecording rec;
            printf("<tr><td>%d.%d</td><td>",disc,pos);int found=wm_recording(db,subject,site_id,world,space,wm_col(t,3),&rec);html(found?rec.title:wm_col(t,3));
            fputs("</td><td>",stdout);const PageItem *it=found?wm_piece(db,&P,rec.ref):NULL;wm_media_check(it,0);if(wa_open_audio(it))playable++;if(!it)missing++;
            char key[80];snprintf(key,sizeof(key),"recording:%s",found?rec.id:"");sqlite3_stmt *cr=NULL;int hc=0;
            if(found&&awm_rows(db,subject,site_id,world,space,AWM_CREDITS,key,&cr)==1&&sqlite3_step(cr)==SQLITE_ROW)hc=1;sqlite3_finalize(cr);if(!hc)uncredited++;
            fputs("</td><td>",stdout);wm_form(site_id,world,space,"untrack",revision,NULL);ph_hidden("id",rel->id);char nb[8];snprintf(nb,sizeof(nb),"%d",disc);ph_hidden("disc",nb);snprintf(nb,sizeof(nb),"%d",pos);ph_hidden("position",nb);
            fputs("<button type=\"submit\" class=\"aw-quiet\">Remove</button></form></td></tr>",stdout);
        }
        sqlite3_finalize(t);if(tracks)fputs("</tbody></table>",stdout);else fputs("<p>No tracks yet.</p>",stdout);
        if(nrecs){wm_form(site_id,world,space,"track",revision,"track");ph_hidden("id",rel->id);
            fputs("<div class=\"aw-row\"><label>Recording<select name=\"recording\">",stdout);
            for(int k=0;k<nrecs;k++){fputs("<option value=\"",stdout);html(recs[k]);fputs("\">",stdout);html(recs[k]);fputs("</option>",stdout);}
            printf("</select></label><label>Disc<input type=\"number\" name=\"disc\" min=\"1\" max=\"9\" value=\"1\"></label><label>Track<input type=\"number\" name=\"position\" min=\"1\" max=\"99\" value=\"%d\"></label></div>",maxpos+1);
            fputs("<div class=\"aw-actions\"><button type=\"submit\">Add the track</button></div></form>",stdout);}
        /* release credits */
        fputs("<p><strong>Release credits</strong></p>",stdout);if(!wm_credits(db,subject,site_id,world,space,"release",rel->id,"data-credits"))fputs("<p>None yet.</p>",stdout);
        /* preparation checklist */
        int cover_ok=1;if(rel->cover[0]){const PageItem *ci=wm_piece(db,&P,rel->cover);cover_ok=wm_open_photo(ci);fputs("<p>Cover ",stdout);wm_media_check(ci,1);fputs("</p>",stdout);}
        fputs("<ul class=\"aw-checks\" data-release-checks>",stdout);
        printf("<li%s>%d track%s</li>",tracks?" data-ok":"",tracks,tracks==1?"":"s");
        printf("<li%s>%d of %d play here; the rest are linked with the reason</li>",playable==tracks&&tracks?" data-ok":"",playable,tracks);
        if(missing)printf("<li>%d no longer found at their source</li>",missing);
        printf("<li%s>%s</li>",uncredited?"":" data-ok",uncredited?"Some recordings have no performer credited":"Every recording has a performer credited");
        printf("<li%s>%s</li>",rel->cover[0]&&cover_ok?" data-ok":"",!rel->cover[0]?"No cover chosen":cover_ok?"Cover can be shown":"Cover will be linked, not shown");
        fputs("</ul><div class=\"aw-actions\">",stdout);
        wm_form(site_id,world,space,"ready",revision,"ready");ph_hidden("id",rel->id);ph_hidden("ready",rel->ready?"0":"1");
        fputs(rel->ready?"<button type=\"submit\" class=\"aw-quiet\">Back to draft</button></form>":"<button type=\"submit\">Mark ready for listeners</button></form>",stdout);
        wm_remove_button(site_id,world,space,revision,"remove","id",rel->id,"release");fputs("</div>",stdout);
        /* edit */
        wm_form(site_id,world,space,"release",revision,"release-edit");ph_hidden("id",rel->id);fputs("<div class=\"aw-row\">",stdout);
        static const char *const types[]={"album","ep","single","compilation","live","soundtrack","other",NULL},*const type_words[]={"Album","EP","Single","Compilation","Live album","Soundtrack","Other",NULL};
        wm_input("Title",NULL,"title",rel->title,256,1,NULL);wm_select("Kind","kind",types,type_words,rel->type);
        wm_input("Released","YYYY, YYYY-MM or YYYY-MM-DD","date",rel->on,10,0,"[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?");fputs("</div><div class=\"aw-row\">",stdout);
        wm_input("Label","A name, or space:address for a label space here","label",rel->label,160,0,NULL);wm_input("Catalogue number",NULL,"catalog",rel->catalog,64,0,NULL);
        wm_input("Cover piece","Optional. A picture&rsquo;s address after /o/","cover",rel->cover,120,0,NULL);fputs("</div><div class=\"aw-row\">",stdout);
        fputs("<label>Edition of<select name=\"edition\"><option value=\"\">The original release</option>",stdout);
        for(int k=0;k<nrels;k++)if(strcmp(rels[k],rel->id)){fputs("<option value=\"",stdout);html(rels[k]);fputs("\"",stdout);if(!strcmp(rels[k],rel->edition))fputs(" selected",stdout);fputs(">",stdout);html(rels[k]);fputs("</option>",stdout);}
        fputs("</select></label>",stdout);wm_input("MusicBrainz release","Optional","mbid",rel->mbid,36,0,"[0-9a-f-]{36}");
        fputs("</div><label>Liner notes<textarea name=\"notes\" rows=\"5\" maxlength=\"4000\">",stdout);html(rel->notes);fputs("</textarea></label><div class=\"aw-actions\"><button type=\"submit\">Save the release</button></div></form></details>",stdout);
    }
    free(rel);
    if(nrels<AWM_RELEASE_LIMIT){
        wm_form(site_id,world,space,"release",revision,"release");fputs("<div class=\"aw-row\">",stdout);
        static const char *const types[]={"album","ep","single","compilation","live","soundtrack","other",NULL},*const type_words[]={"Album","EP","Single","Compilation","Live album","Soundtrack","Other",NULL};
        wm_input("New release",NULL,"title","",256,1,NULL);wm_input("Address","Lowercase, used in the link","id","",64,1,"[a-z0-9][a-z0-9-]*");
        wm_select("Kind","kind",types,type_words,"album");wm_input("Released","YYYY, YYYY-MM or YYYY-MM-DD","date","",10,0,"[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?");
        fputs("</div><div class=\"aw-actions\"><button type=\"submit\">Start the release</button></div></form>",stdout);
    }
    /* credits */
    fputs("<h3>Credits</h3><p>Writers belong to works, performers to recordings, and artwork, notes and mastering to releases; the form only offers the roles that fit.</p>",stdout);
    wm_form(site_id,world,space,"credit",revision,"credit");fputs("<div class=\"aw-row\"><label>On<select name=\"of\">",stdout);
    for(int i=0;i<nworks;i++){fputs("<option value=\"work:",stdout);html(works[i]);fputs("\">Work: ",stdout);html(works[i]);fputs("</option>",stdout);}
    for(int i=0;i<nrecs;i++){fputs("<option value=\"recording:",stdout);html(recs[i]);fputs("\">Recording: ",stdout);html(recs[i]);fputs("</option>",stdout);}
    for(int i=0;i<nrels;i++){fputs("<option value=\"release:",stdout);html(rels[i]);fputs("\">Release: ",stdout);html(rels[i]);fputs("</option>",stdout);}
    fputs("</select></label><label>Role<select name=\"role\"><optgroup label=\"Works\"><option>composer</option><option>lyricist</option><option>songwriter</option><option>arranger</option><option>translator</option><option>publisher</option></optgroup>"
          "<optgroup label=\"Recordings\"><option>performer</option><option>vocals</option><option>instrument</option><option>producer</option><option>engineer</option><option>mixing</option><option>remixer</option><option>conductor</option><option>featured</option></optgroup>"
          "<optgroup label=\"Releases\"><option>artwork</option><option>design</option><option>photography</option><option>liner-notes</option><option>mastering</option><option>executive-producer</option></optgroup></select></label></div><div class=\"aw-row\">",stdout);
    wm_input("Name",NULL,"name","",160,1,NULL);wm_input("Detail","Such as piano or backing vocals","detail","",64,0,NULL);wm_input("Link","Optional https address or space:address","href","",512,0,NULL);
    fputs("</div><div class=\"aw-actions\"><button type=\"submit\">Add the credit</button></div></form>",stdout);
    n=0;if(awm_rows(db,subject,site_id,world,space,AWM_CREDITS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!n++)fputs("<details><summary>Every credit</summary><table data-music-credits><tbody>",stdout);
        fputs("<tr><td>",stdout);html(wm_col(s,1));fputs(" ",stdout);html(wm_col(s,2));fputs("</td><td>",stdout);wm_words(wm_col(s,3));fputs(": ",stdout);html(wm_col(s,4));
        if(*wm_col(s,5)){fputs(" (",stdout);html(wm_col(s,5));fputs(")",stdout);}fputs("</td><td>",stdout);
        char idn[24];snprintf(idn,sizeof(idn),"%lld",sqlite3_column_int64(s,0));wm_remove_button(site_id,world,space,revision,"uncredit","number",idn,NULL);fputs("</td></tr>",stdout);}
    sqlite3_finalize(s);if(n)fputs("</tbody></table></details>",stdout);
    /* history */
    fputs("<h3>History</h3><p>Who this is connected to and when: members, labels, collaborators. Listeners can search it.</p>",stdout);
    n=0;if(awm_rows(db,subject,site_id,world,space,AWM_RELATIONS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!n++)fputs("<table data-music-relations><tbody>",stdout);
        fputs("<tr><td>",stdout);fputs(wm_relation(wm_col(s,1)),stdout);fputs(" ",stdout);html(wm_col(s,2));fputs("</td><td>",stdout);html(wm_col(s,4));if(*wm_col(s,5)){fputs("&ndash;",stdout);html(wm_col(s,5));}
        fputs("</td><td>",stdout);char idn[24];snprintf(idn,sizeof(idn),"%lld",sqlite3_column_int64(s,0));wm_remove_button(site_id,world,space,revision,"unrelation","number",idn,NULL);fputs("</td></tr>",stdout);}
    sqlite3_finalize(s);if(n)fputs("</tbody></table>",stdout);
    wm_form(site_id,world,space,"relation",revision,"relation");fputs("<div class=\"aw-row\">",stdout);
    static const char *const rk[]={"member-of","founded","signed-to","collaborated-with","split-from","also-known-as","influenced-by",NULL},*const rw[]={"Member of","Founded by","Signed to","Worked with","Split from","Also known as","Influenced by",NULL};
    wm_select("Connection","kind",rk,rw,"collaborated-with");wm_input("Name",NULL,"name","",160,1,NULL);wm_input("Link","Optional https address or space:address","href","",512,0,NULL);
    wm_input("From","Year","begun","",4,0,"[0-9]{4}");wm_input("Until","Year","ended","",4,0,"[0-9]{4}");fputs("</div><div class=\"aw-actions\"><button type=\"submit\">Add to the history</button></div></form>",stdout);
    /* rooms */
    fputs("<h3>Listening rooms</h3><p>Pick a time and a release or recording; everyone who opens the room hears it from the same point. Only recordings that play here are played; the rest are named and skipped.</p>",stdout);
    n=0;if(awm_rows(db,subject,site_id,world,space,AWM_ROOMS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!n++)fputs("<table data-music-rooms><tbody>",stdout);const char *id=wm_col(s,0),*state=wm_col(s,6);
        fputs("<tr data-room=\"",stdout);html(id);fputs("\" data-state=\"",stdout);html(state);fputs("\"><td>",stdout);cine_when(wm_col(s,1));fputs(" UTC</td><td>",stdout);html(wm_col(s,2));fputs(" ",stdout);html(wm_col(s,3));
        fputs(" <span class=\"aw-flag\">",stdout);html(state);fputs("</span></td><td>",stdout);
        if(!strcmp(state,"draft")||!strcmp(state,"announced")){wm_form(site_id,world,space,"room-state",revision,NULL);ph_hidden("id",id);ph_hidden("kind",!strcmp(state,"draft")?"announced":"cancelled");
            fputs(!strcmp(state,"draft")?"<button type=\"submit\">Announce</button></form>":"<button type=\"submit\" class=\"aw-quiet\">Cancel</button></form>",stdout);}
        wm_remove_button(site_id,world,space,revision,"remove","id",id,"room");fputs("</td></tr>",stdout);}
    sqlite3_finalize(s);if(n)fputs("</tbody></table>",stdout);
    if(nrels||nrecs){wm_form(site_id,world,space,"room",revision,"room");fputs("<div class=\"aw-row\">",stdout);
        wm_input("Room address","Lowercase","id","",64,1,"[a-z0-9][a-z0-9-]*");
        fputs("<label>Starts (UTC)<input type=\"datetime-local\" name=\"date\" required></label><label>Plays<select name=\"of\">",stdout);
        for(int i=0;i<nrels;i++){fputs("<option value=\"release:",stdout);html(rels[i]);fputs("\">Release: ",stdout);html(rels[i]);fputs("</option>",stdout);}
        for(int i=0;i<nrecs;i++){fputs("<option value=\"recording:",stdout);html(recs[i]);fputs("\">Recording: ",stdout);html(recs[i]);fputs("</option>",stdout);}
        fputs("</select></label></div><div class=\"aw-row\">",stdout);wm_input("Note","Optional","notes","",280,0,NULL);wm_input("Tickets","Optional https address","href","",512,0,NULL);
        fputs("</div><div class=\"aw-actions\"><button type=\"submit\">Save the room</button></div></form>",stdout);}
    fputs("</section>",stdout);wm_close(&P);
}

/* -------------------------------------------------------------- public */
static int wm_page_scripted;
static void wm_bar(const PhSite *site,const char *space,const char *space_title){
    fputs("<nav class=\"bar\" aria-label=\"Where you are\"><a class=\"home\" href=\"/\">",stdout);html(site->name[0]?site->name:site->host);
    fputs("</a><span class=\"ch\"><a href=\"/worlds/",stdout);html(space);fputs("\">",stdout);html(space_title);fputs("</a></span><span class=\"gr\"><a href=\"/worlds\">All spaces</a></span></nav>",stdout);
}
static void wm_collect_door(const PhSite *site,const char *space,const char *release){
    fputs("<p class=\"aw-listen\"><a class=\"aw-door\" data-music-collect href=\"https://aurekai.",stdout);html(site->territory);fputs("/workspace/worlds/collection?site=",stdout);url_query_value(site->id);
    fputs("&amp;space=",stdout);url_query_value(space);fputs("&amp;release=",stdout);url_query_value(release);fputs("\">Add to your collection</a></p>",stdout);
}
/* On the space page: identity, discography, label roster, history, rooms. */
static void world_music_public(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *space){
    WmParty party;if(!wm_party(db,NULL,site->id,world,space,&party))return;
    WmPieces P;wm_open(&P);sqlite3_stmt *s=NULL;
    fputs("<section class=\"aw-music\" id=\"discography\" data-world-music=\"",stdout);html(party.kind);fputs("\" aria-label=\"Discography\">",stdout);
    fputs("<p class=\"aw-credit\">",stdout);fputs(!strcmp(party.kind,"label")?"A record label":"An artist",stdout);
    if(party.area[0]){fputs(" from ",stdout);html(party.area);}if(party.begun[0]){fputs(", since ",stdout);html(party.begun);}fputs(".",stdout);wm_mb(!strcmp(party.kind,"label")?"label":"artist",party.mbid);fputs("</p>",stdout);
    fputs("<label class=\"aw-search\">Search releases, songs and history <input type=\"search\" data-music-search autocomplete=\"off\"></label>",stdout);
    int n=0;
    if(awm_rows(db,NULL,site->id,world,space,AWM_RELEASES,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!n++)fputs("<h2 class=\"region-label\">Releases</h2><ol class=\"aw-discography\" data-music-releases>",stdout);
        const char *id=wm_col(s,0),*cover=wm_col(s,6);
        fputs("<li data-music-row><a href=\"/worlds/",stdout);html(space);fputs("/release/",stdout);html(id);fputs("\">",stdout);
        const PageItem *ci=*cover?wm_piece(db,&P,cover):NULL;
        if(wm_open_photo(ci)){fputs("<img loading=\"lazy\" decoding=\"async\" alt=\"\" src=\"",stdout);html(ci->image);fputs("\">",stdout);}else fputs("<span class=\"aw-sleeve\" aria-hidden=\"true\"></span>",stdout);
        fputs("<strong>",stdout);html(wm_col(s,1));fputs("</strong></a><span class=\"aw-epmeta\">",stdout);fputs(wm_type(wm_col(s,2)),stdout);
        if(*wm_col(s,3)){fputs(" &middot; ",stdout);html(wm_col(s,3));}if(*wm_col(s,8))fputs(" &middot; edition",stdout);fputs("</span></li>",stdout);
    }
    sqlite3_finalize(s);if(n)fputs("</ol>",stdout);
    if(!strcmp(party.kind,"label")){int m=0;
        if(awm_label_releases(db,site->id,world,space,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
            if(!m++)fputs("<h2 class=\"region-label\">On the label</h2><ol class=\"aw-discography\" data-music-roster>",stdout);
            fputs("<li data-music-row><a href=\"/worlds/",stdout);html(wm_col(s,0));fputs("/release/",stdout);html(wm_col(s,1));fputs("\"><span class=\"aw-sleeve\" aria-hidden=\"true\"></span><strong>",stdout);
            html(wm_col(s,2));fputs("</strong></a><span class=\"aw-epmeta\">",stdout);fputs(wm_type(wm_col(s,3)),stdout);if(*wm_col(s,4)){fputs(" &middot; ",stdout);html(wm_col(s,4));}fputs("</span></li>",stdout);}
        sqlite3_finalize(s);if(m)fputs("</ol>",stdout);}
    n=0;if(awm_rows(db,NULL,site->id,world,space,AWM_WORKS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!n++)fputs("<h2 class=\"region-label\">Songs</h2><ul class=\"aw-marks\" data-music-works>",stdout);
        fputs("<li data-music-row><a href=\"/worlds/",stdout);html(space);fputs("/work/",stdout);html(wm_col(s,0));fputs("\">",stdout);html(wm_col(s,1));fputs("</a></li>",stdout);}
    sqlite3_finalize(s);if(n)fputs("</ul>",stdout);
    n=0;if(awm_rows(db,NULL,site->id,world,space,AWM_RELATIONS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!n++)fputs("<h2 class=\"region-label\">History</h2><ul class=\"aw-marks\" data-music-history>",stdout);
        fputs("<li data-music-row><span class=\"aw-at\">",stdout);html(wm_col(s,4));if(*wm_col(s,5)){fputs("&ndash;",stdout);html(wm_col(s,5));}else if(*wm_col(s,4))fputs("&ndash;",stdout);
        fputs("</span> ",stdout);fputs(wm_relation(wm_col(s,1)),stdout);fputs(" ",stdout);wm_href_open(wm_col(s,3));html(wm_col(s,2));if(*wm_col(s,3))fputs("</a>",stdout);fputs("</li>",stdout);}
    sqlite3_finalize(s);if(n)fputs("</ul>",stdout);
    n=0;if(awm_rows(db,NULL,site->id,world,space,AWM_ROOMS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!n++)fputs("<h2 class=\"region-label\">Listening rooms</h2><ul class=\"aw-marks\" data-music-rooms>",stdout);
        fputs("<li data-state=\"",stdout);html(wm_col(s,6));fputs("\"><span class=\"aw-at\">",stdout);cine_when(wm_col(s,1));fputs(" UTC</span> <a href=\"/worlds/",stdout);html(space);fputs("/listen/",stdout);html(wm_col(s,0));fputs("\">",stdout);
        fputs(!strcmp(wm_col(s,6),"cancelled")?"Cancelled":"Open the room",stdout);fputs("</a>",stdout);if(*wm_col(s,4)){fputs(" <span class=\"aw-credit\">",stdout);html(wm_col(s,4));fputs("</span>",stdout);}fputs("</li>",stdout);}
    sqlite3_finalize(s);if(n)fputs("</ul>",stdout);
    fputs("<p class=\"aw-listen\"><a href=\"/worlds/",stdout);html(space);fputs("/releases.xml\" data-music-feed>Follow new releases</a></p></section>",stdout);
    fputs("<script>(()=>{const i=document.querySelector('[data-music-search]');if(!i)return;const rows=[...document.querySelectorAll('[data-music-row]')];"
          "i.addEventListener('input',()=>{const q=i.value.trim().toLowerCase();rows.forEach(r=>{r.hidden=!!q&&!r.textContent.toLowerCase().includes(q)})})})()</script>",stdout);
    wm_close(&P);
}
static void wm_track_row(sqlite3 *db,WmPieces *P,const PhSite *site,sqlite3_int64 world,const char *space,const char *recording_id,int disc,int pos,int show_disc){
    WmRecording rec;int found=wm_recording(db,NULL,site->id,world,space,recording_id,&rec);
    PageItem copy;const PageItem *it=found?wm_piece(db,P,rec.ref):NULL;if(it){copy=*it;it=&copy;}
    printf("<li class=\"aw-track\" data-track=\"%d.%d\" itemprop=\"track\" itemscope itemtype=\"https://schema.org/MusicRecording\"><span class=\"aw-at\">",disc,pos);
    if(show_disc)printf("%d.",disc);printf("%d</span><div class=\"aw-words\"><h3 itemprop=\"name\"><a href=\"/worlds/",pos);html(space);fputs("/recording/",stdout);html(recording_id);fputs("\">",stdout);html(found?rec.title:recording_id);fputs("</a>",stdout);
    if(it&&it->duration_ms>0){fputs(" <span class=\"aw-at\">",stdout);wm_ms(it->duration_ms);fputs("</span>",stdout);}fputs("</h3>",stdout);
    if(found)wm_credits(db,NULL,site->id,world,space,"recording",rec.id,"data-track-credits");
    if(wa_open_audio(it))wm_player(it);else wm_refusal(found?it:NULL);
    fputs("</div></li>",stdout);
}
static void wm_release_page(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *space,const char *space_title,const char *character,const char *id){
    WmRelease *rel=calloc(1,sizeof(*rel));WmParty party;wm_party(db,NULL,site->id,world,space,&party);WmPieces P;wm_open(&P);
    if(!rel||!wm_release(db,NULL,site->id,world,space,id,rel)){free(rel);wm_close(&P);protocol_reply(404,"text/plain; charset=utf-8","There is no release at this address.\n");return;}
    char title[400];snprintf(title,sizeof(title),"%s%s%s",rel->title,party.name[0]?" by ":"",party.name);
    world_web_public_head(200,title,site->name,rel->notes);
    fputs("<body class=\"",stdout);html(character);fputs(" aw-space aw-release\" data-world-look=\"",stdout);html(character);fputs("\"><main class=\"wrap\" data-music-release=\"",stdout);html(rel->id);
    fputs("\" itemscope itemtype=\"https://schema.org/MusicAlbum\">",stdout);wm_bar(site,space,space_title);
    fputs("<header class=\"hero aw-release-head\">",stdout);
    const PageItem *ci=rel->cover[0]?wm_piece(db,&P,rel->cover):NULL;
    if(wm_open_photo(ci)){fputs("<figure class=\"aw-cover\"><img itemprop=\"image\" src=\"",stdout);html(ci->image);fputs("\" alt=\"Cover of ",stdout);html(rel->title);fputs("\"><figcaption class=\"aw-terms\">",stdout);html(ci->rights);fputs("</figcaption></figure>",stdout);}
    else if(ci){fputs("<p class=\"aw-terms\" data-cover-linked>The cover is not shown because its source does not state open terms. ",stdout);if(public_http_url(ci->href)){fputs("<a rel=\"noopener\" href=\"",stdout);html(ci->href);fputs("\">See it where it is published</a>",stdout);}fputs("</p>",stdout);}
    fputs("<div><p class=\"aw-role\">",stdout);fputs(wm_type(rel->type),stdout);fputs("</p><h1 itemprop=\"name\">",stdout);html(rel->title);fputs("</h1>",stdout);
    if(party.name[0]){fputs("<p class=\"lede\" itemprop=\"byArtist\">",stdout);html(party.name);fputs("</p>",stdout);}
    fputs("<p class=\"aw-epmeta\">",stdout);int dot=0;if(rel->on[0]){wm_date(rel->on);dot=1;}
    if(rel->label[0]){if(dot)fputs(" &middot; ",stdout);wm_label(rel->label);dot=1;}if(rel->catalog[0]){if(dot)fputs(" &middot; ",stdout);html(rel->catalog);dot=1;}
    wm_mb("release",rel->mbid);fputs("</p></div></header>",stdout);
    /* editions */
    if(rel->edition[0]){WmRelease *o=calloc(1,sizeof(*o));if(o&&wm_release(db,NULL,site->id,world,space,rel->edition,o)){fputs("<p class=\"aw-note\" data-music-edition>Another edition of <a href=\"/worlds/",stdout);html(space);fputs("/release/",stdout);html(o->id);fputs("\">",stdout);html(o->title);fputs("</a>.</p>",stdout);}free(o);}
    sqlite3_stmt *s=NULL;int ed=0;
    if(awm_rows(db,NULL,site->id,world,space,AWM_RELEASES,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW)if(!strcmp(wm_col(s,8),rel->id)){
        if(!ed++)fputs("<p class=\"aw-note\" data-music-editions>Also issued as ",stdout);else fputs(", ",stdout);
        fputs("<a href=\"/worlds/",stdout);html(space);fputs("/release/",stdout);html(wm_col(s,0));fputs("\">",stdout);html(wm_col(s,1));fputs("</a>",stdout);if(*wm_col(s,3)){fputs(" (",stdout);html(wm_col(s,3));fputs(")",stdout);}}
    sqlite3_finalize(s);if(ed)fputs(".</p>",stdout);
    /* tracklist */
    int discs=1,n=0;
    if(awm_rows(db,NULL,site->id,world,space,AWM_TRACKS,rel->id,&s)==1)while(sqlite3_step(s)==SQLITE_ROW)if(sqlite3_column_int(s,1)>discs)discs=sqlite3_column_int(s,1);
    sqlite3_finalize(s);s=NULL;
    fputs("<section class=\"aw-tracklist\" aria-label=\"Tracklist\"><h2 class=\"region-label\">Tracklist</h2><ol class=\"aw-tracks\" data-music-tracklist>",stdout);
    if(awm_rows(db,NULL,site->id,world,space,AWM_TRACKS,rel->id,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){n++;
        char recid[65];snprintf(recid,sizeof(recid),"%s",wm_col(s,3));wm_track_row(db,&P,site,world,space,recid,sqlite3_column_int(s,1),sqlite3_column_int(s,2),discs>1);}
    sqlite3_finalize(s);printf("</ol><meta itemprop=\"numTracks\" content=\"%d\"></section>",n);
    if(rel->notes[0]){fputs("<section class=\"aw-liner\" aria-label=\"Liner notes\"><h2 class=\"region-label\">Liner notes</h2>",stdout);
        char *copy=strdup(rel->notes);for(char *line=copy?strtok(copy,"\n"):NULL;line;line=strtok(NULL,"\n")){size_t l=strlen(line);if(l&&line[l-1]=='\r')line[l-1]=0;if(*line){fputs("<p>",stdout);html(line);fputs("</p>",stdout);}}free(copy);fputs("</section>",stdout);}
    fputs("<section class=\"aw-liner\" aria-label=\"Credits\">",stdout);
    if(awm_rows(db,NULL,site->id,world,space,AWM_CREDITS,NULL,&s)==1){int any=0;while(sqlite3_step(s)==SQLITE_ROW)if(!strcmp(wm_col(s,1),"release")&&!strcmp(wm_col(s,2),rel->id))any=1;
        sqlite3_finalize(s);s=NULL;if(any){fputs("<h2 class=\"region-label\">Credits</h2>",stdout);wm_credits(db,NULL,site->id,world,space,"release",rel->id,"data-release-credits");}}
    fputs("</section>",stdout);
    n=0;if(awm_rows(db,NULL,site->id,world,space,AWM_ROOMS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW)if(!strcmp(wm_col(s,2),"release")&&!strcmp(wm_col(s,3),rel->id)){
        if(!n++)fputs("<section aria-label=\"Listening rooms\"><h2 class=\"region-label\">Listen together</h2><ul class=\"aw-marks\">",stdout);
        fputs("<li><span class=\"aw-at\">",stdout);cine_when(wm_col(s,1));fputs(" UTC</span> <a href=\"/worlds/",stdout);html(space);fputs("/listen/",stdout);html(wm_col(s,0));fputs("\">",stdout);
        fputs(!strcmp(wm_col(s,6),"cancelled")?"Cancelled":"Open the room",stdout);fputs("</a></li>",stdout);}
    sqlite3_finalize(s);if(n)fputs("</ul></section>",stdout);
    wm_collect_door(site,space,rel->id);
    fputs("<footer class=\"src\">Recordings play here only when their own source states open terms; the others are linked with the reason.</footer></main></body></html>",stdout);
    free(rel);wm_close(&P);
}
static void wm_recording_page(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *space,const char *space_title,const char *character,const char *id){
    WmRecording rec;WmParty party;wm_party(db,NULL,site->id,world,space,&party);
    if(!wm_recording(db,NULL,site->id,world,space,id,&rec)){protocol_reply(404,"text/plain; charset=utf-8","There is no recording at this address.\n");return;}
    WmPieces P;wm_open(&P);const PageItem *found=wm_piece(db,&P,rec.ref);PageItem it;if(found)it=*found;
    world_web_public_head(200,rec.title,site->name,"");
    fputs("<body class=\"",stdout);html(character);fputs(" aw-space aw-release\" data-world-look=\"",stdout);html(character);fputs("\"><main class=\"wrap\" data-music-recording=\"",stdout);html(rec.id);fputs("\">",stdout);
    wm_bar(site,space,space_title);
    fputs("<header class=\"hero\"><p class=\"aw-role\">Recording</p><h1>",stdout);html(rec.title);fputs("</h1>",stdout);if(party.name[0]){fputs("<p class=\"lede\">",stdout);html(party.name);fputs("</p>",stdout);}
    fputs("<p class=\"aw-epmeta\">",stdout);if(found&&it.duration_ms>0){wm_ms(it.duration_ms);}if(rec.isrc[0]){fputs(found&&it.duration_ms>0?" &middot; ISRC ":"ISRC ",stdout);html(rec.isrc);}wm_mb("recording",rec.mbid);fputs("</p></header>",stdout);
    /* the same article shape as a space piece, so the sound additions' marks and seek script find it */
    fputs("<article class=\"aw-piece\" id=\"piece-1\" data-kind=\"audio\"><div class=\"aw-words\">",stdout);
    if(found&&wa_open_audio(&it))wm_player(&it);else wm_refusal(found?&it:NULL);
    wm_credits(db,NULL,site->id,world,space,"recording",rec.id,"data-recording-credits");fputs("</div></article>",stdout);
    if(found)world_audio_episode(db,site,world,space,1,rec.ref,&it);
    if(rec.work[0]){sqlite3_stmt *w=NULL;
        if(awm_rows(db,NULL,site->id,world,space,AWM_WORKS,rec.work,&w)==1&&sqlite3_step(w)==SQLITE_ROW){
            fputs("<section class=\"aw-liner\" data-music-work><h2 class=\"region-label\">The song</h2><p><a href=\"/worlds/",stdout);html(space);fputs("/work/",stdout);html(rec.work);fputs("\">",stdout);html(wm_col(w,1));fputs("</a>",stdout);
            if(*wm_col(w,2)){fputs(" <span class=\"aw-at\">ISWC ",stdout);html(wm_col(w,2));fputs("</span>",stdout);}fputs("</p>",stdout);wm_credits(db,NULL,site->id,world,space,"work",rec.work,"data-work-credits");fputs("</section>",stdout);}
        sqlite3_finalize(w);}
    sqlite3_stmt *s=NULL;int n=0;
    if(awm_rows(db,NULL,site->id,world,space,AWM_TRACKS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW)if(!strcmp(wm_col(s,3),rec.id)){
        if(!n++)fputs("<section class=\"aw-liner\"><h2 class=\"region-label\">Appears on</h2><ul class=\"aw-marks\" data-music-appears>",stdout);
        WmRelease *r=calloc(1,sizeof(*r));if(r&&wm_release(db,NULL,site->id,world,space,wm_col(s,0),r)){fputs("<li><a href=\"/worlds/",stdout);html(space);fputs("/release/",stdout);html(r->id);fputs("\">",stdout);html(r->title);
            printf("</a> <span class=\"aw-at\">track %d</span></li>",sqlite3_column_int(s,2));}free(r);}
    sqlite3_finalize(s);if(n)fputs("</ul></section>",stdout);
    /* where this exact piece is heard: a show's tracklist or another space */
    n=0;if(awm_heard_on(db,site->id,world,rec.ref,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!strcmp(wm_col(s,0),space)&&!strcmp(wm_col(s,3),"holds"))continue;
        if(!n++)fputs("<section class=\"aw-liner\"><h2 class=\"region-label\">Heard on</h2><ul class=\"aw-marks\" data-music-heard>",stdout);
        fputs("<li><a href=\"/worlds/",stdout);html(wm_col(s,0));fputs("\">",stdout);html(wm_col(s,1));fputs("</a>",stdout);
        if(!strcmp(wm_col(s,3),"played")){char t[24];wa_clock(sqlite3_column_int64(s,4),t);fputs(" <span class=\"aw-at\">played at ",stdout);html(t);fputs("</span>",stdout);}
        else{fputs(" <span class=\"aw-at\">",stdout);world_web_words(wm_col(s,2));fputs("</span>",stdout);}fputs("</li>",stdout);}
    sqlite3_finalize(s);if(n)fputs("</ul></section>",stdout);
    n=0;if(awm_rows(db,NULL,site->id,world,space,AWM_ROOMS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW)if(!strcmp(wm_col(s,2),"recording")&&!strcmp(wm_col(s,3),rec.id)){
        if(!n++)fputs("<section class=\"aw-liner\"><h2 class=\"region-label\">Listen together</h2><ul class=\"aw-marks\" data-music-recording-rooms>",stdout);
        fputs("<li><span class=\"aw-at\">",stdout);cine_when(wm_col(s,1));fputs(" UTC</span> <a href=\"/worlds/",stdout);html(space);fputs("/listen/",stdout);html(wm_col(s,0));fputs("\">",stdout);
        fputs(!strcmp(wm_col(s,6),"cancelled")?"Cancelled":"Open the room",stdout);fputs("</a></li>",stdout);}
    sqlite3_finalize(s);if(n)fputs("</ul></section>",stdout);
    world_audio_script();
    fputs("<footer class=\"src\">Played only under the terms its source states. A tracklist or schedule is not a licence.</footer></main></body></html>",stdout);
    wm_close(&P);
}
static void wm_work_page(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *space,const char *space_title,const char *character,const char *id){
    sqlite3_stmt *w=NULL;
    if(awm_rows(db,NULL,site->id,world,space,AWM_WORKS,id,&w)!=1||sqlite3_step(w)!=SQLITE_ROW){sqlite3_finalize(w);protocol_reply(404,"text/plain; charset=utf-8","There is no song at this address.\n");return;}
    char title[260],iswc[20],mbid[40];snprintf(title,sizeof(title),"%s",wm_col(w,1));snprintf(iswc,sizeof(iswc),"%s",wm_col(w,2));snprintf(mbid,sizeof(mbid),"%s",wm_col(w,3));sqlite3_finalize(w);
    world_web_public_head(200,title,site->name,"");
    fputs("<body class=\"",stdout);html(character);fputs(" aw-space aw-release\" data-world-look=\"",stdout);html(character);fputs("\"><main class=\"wrap\" data-music-work=\"",stdout);html(id);fputs("\">",stdout);
    wm_bar(site,space,space_title);fputs("<header class=\"hero\"><p class=\"aw-role\">Song</p><h1>",stdout);html(title);fputs("</h1><p class=\"aw-epmeta\">",stdout);
    if(*iswc){fputs("ISWC ",stdout);html(iswc);}wm_mb("work",mbid);fputs("</p></header>",stdout);
    fputs("<section class=\"aw-liner\"><h2 class=\"region-label\">Written by</h2>",stdout);if(!wm_credits(db,NULL,site->id,world,space,"work",id,"data-work-credits"))fputs("<p class=\"aw-body\">No writers are credited.</p>",stdout);fputs("</section>",stdout);
    sqlite3_stmt *s=NULL;int n=0;
    if(awm_rows(db,NULL,site->id,world,space,AWM_RECORDINGS,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW)if(!strcmp(wm_col(s,1),id)){
        if(!n++)fputs("<section class=\"aw-liner\"><h2 class=\"region-label\">Recordings</h2><ul class=\"aw-marks\" data-music-work-recordings>",stdout);
        fputs("<li><a href=\"/worlds/",stdout);html(space);fputs("/recording/",stdout);html(wm_col(s,0));fputs("\">",stdout);html(wm_col(s,2));fputs("</a></li>",stdout);}
    sqlite3_finalize(s);if(n)fputs("</ul></section>",stdout);
    fputs("</main></body></html>",stdout);
}
static void wm_room_page(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *space,const char *space_title,const char *character,const char *id){
    sqlite3_stmt *r=NULL;
    if(awm_rows(db,NULL,site->id,world,space,AWM_ROOMS,id,&r)!=1||sqlite3_step(r)!=SQLITE_ROW){sqlite3_finalize(r);protocol_reply(404,"text/plain; charset=utf-8","There is no listening room at this address.\n");return;}
    char starts[20],target[12],target_id[65],note[284],ticket[516],state[12];
    snprintf(starts,sizeof(starts),"%s",wm_col(r,1));snprintf(target,sizeof(target),"%s",wm_col(r,2));snprintf(target_id,sizeof(target_id),"%s",wm_col(r,3));
    snprintf(note,sizeof(note),"%s",wm_col(r,4));snprintf(ticket,sizeof(ticket),"%s",wm_col(r,5));snprintf(state,sizeof(state),"%s",wm_col(r,6));sqlite3_finalize(r);
    char what[260]="";char recs[AWM_TRACK_LIMIT][65];int nr=0;
    if(!strcmp(target,"release")){WmRelease *rel=calloc(1,sizeof(*rel));if(rel&&wm_release(db,NULL,site->id,world,space,target_id,rel))snprintf(what,sizeof(what),"%s",rel->title);free(rel);
        sqlite3_stmt *t=NULL;if(awm_rows(db,NULL,site->id,world,space,AWM_TRACKS,target_id,&t)==1)while(nr<AWM_TRACK_LIMIT&&sqlite3_step(t)==SQLITE_ROW)snprintf(recs[nr++],65,"%s",wm_col(t,3));sqlite3_finalize(t);}
    else{WmRecording rec;if(wm_recording(db,NULL,site->id,world,space,target_id,&rec)){snprintf(what,sizeof(what),"%s",rec.title);snprintf(recs[nr++],65,"%s",rec.id);}}
    char title[300];snprintf(title,sizeof(title),"Listening room: %s",what);
    world_web_public_head(200,title,site->name,note);
    fputs("<body class=\"",stdout);html(character);fputs(" aw-space aw-room\" data-world-look=\"",stdout);html(character);fputs("\"><main class=\"wrap\" data-music-room=\"",stdout);html(id);
    fputs("\" data-state=\"",stdout);html(state);fputs("\" data-starts=\"",stdout);html(starts);fputs("\">",stdout);wm_bar(site,space,space_title);
    fputs("<header class=\"hero\"><p class=\"aw-role\">Listening room</p><h1><a href=\"/worlds/",stdout);html(space);fputs(!strcmp(target,"release")?"/release/":"/recording/",stdout);html(target_id);fputs("\">",stdout);html(what);
    fputs("</a></h1><p class=\"lede\"><time datetime=\"",stdout);html(starts);fputs("Z\">",stdout);cine_when(starts);fputs(" UTC</time></p>",stdout);if(note[0]){fputs("<p class=\"aw-note\">",stdout);html(note);fputs("</p>",stdout);}
    if(ticket[0]&&strcmp(state,"cancelled")){fputs("<p class=\"aw-listen\"><a rel=\"noopener\" href=\"",stdout);html(ticket);fputs("\">Tickets</a></p>",stdout);}fputs("</header>",stdout);
    if(!strcmp(state,"cancelled")){fputs("<p class=\"aw-note\" data-room-cancelled>This listening room has been cancelled.</p></main></body></html>",stdout);return;}
    WmPieces P;wm_open(&P);int playable=0;
    fputs("<section class=\"aw-roomplayer\" aria-label=\"The room\"><p class=\"aw-note\" data-room-status aria-live=\"polite\">Everyone who joins hears the same moment. Nothing plays until you join.</p>"
          "<p><button type=\"button\" class=\"aw-join-room\" data-room-join>Join the room</button></p><audio controls preload=\"none\" data-room-player></audio></section>",stdout);
    fputs("<section class=\"aw-tracklist\"><h2 class=\"region-label\">In the room</h2><ol class=\"aw-tracks\" data-room-tracks>",stdout);
    for(int i=0;i<nr;i++){WmRecording rec;if(!wm_recording(db,NULL,site->id,world,space,recs[i],&rec))continue;const PageItem *it=wm_piece(db,&P,rec.ref);
        int plays=wa_open_audio(it);if(plays)playable++;
        printf("<li class=\"aw-track\"%s",plays?" data-room-plays":"");if(plays){fputs(" data-src=\"",stdout);html(it->asset);printf("\" data-ms=\"%ld\"",it->duration_ms>0?it->duration_ms:0L);}
        fputs("><div class=\"aw-words\"><h3><a href=\"/worlds/",stdout);html(space);fputs("/recording/",stdout);html(rec.id);fputs("\">",stdout);html(rec.title);fputs("</a></h3>",stdout);
        if(plays){fputs("<p class=\"aw-terms\">",stdout);html(it->rights);fputs("</p>",stdout);}else wm_refusal(it);fputs("</div></li>",stdout);}
    fputs("</ol></section>",stdout);
    if(!playable)fputs("<p class=\"aw-note\" data-room-silent>None of these recordings can be played here, so the room is a list to follow where each is published.</p>",stdout);
    fputs("<script>(()=>{const main=document.querySelector('[data-music-room]'),btn=document.querySelector('[data-room-join]'),player=document.querySelector('[data-room-player]'),"
          "status=document.querySelector('[data-room-status]'),list=[...document.querySelectorAll('[data-room-plays]')];if(!list.length){btn.hidden=true;player.hidden=true;return}"
          "const start=Date.parse(main.dataset.starts+':00Z');let at=0;const mark=i=>list.forEach((l,k)=>l.toggleAttribute('aria-current',k===i));"
          "const play=(i,offset)=>{at=i;mark(i);player.src=list[i].dataset.src;player.currentTime=0;player.addEventListener('loadedmetadata',()=>{player.currentTime=offset},{once:true});player.play()};"
          "player.addEventListener('ended',()=>{if(at+1<list.length)play(at+1,0);else status.textContent='The room has finished.'});"
          "btn.addEventListener('click',()=>{const gone=(Date.now()-start)/1000;if(gone<0){status.textContent='The room opens at '+new Date(start).toLocaleString()+'. Stay here and it will start for you.';"
          "setTimeout(()=>play(0,0),-gone*1000);return}let left=gone;for(let i=0;i<list.length;i++){const d=(+list[i].dataset.ms||0)/1000;if(!d||left<d){status.textContent='You joined the room.';play(i,d?left:0);return}left-=d}"
          "status.textContent='The room has finished. Playing from the start.';play(0,0)})})()</script>",stdout);
    fputs("</main></body></html>",stdout);wm_close(&P);
}
static void wm_rss_date(const char *on,char out[40]){
    char d[11]="";size_t n=strlen(on);if(n==4)snprintf(d,sizeof(d),"%s-01-01",on);else if(n==7)snprintf(d,sizeof(d),"%s-01",on);else snprintf(d,sizeof(d),"%s",on);
    wa_rfc822(d,out);
}
static void wm_feed(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *space,const char *space_title,const char *lede){
    WmParty party;wm_party(db,NULL,site->id,world,space,&party);char base[400];snprintf(base,sizeof(base),"https://%s/worlds/%s",site->host,space);
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/rss+xml; charset=utf-8\r\nCache-Control: public, no-cache\r\nVary: Host\r\nX-Content-Type-Options: nosniff\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n");
    fputs("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rss version=\"2.0\" xmlns:atom=\"http://www.w3.org/2005/Atom\">\n<channel>\n<title>",stdout);wa_xml(space_title);fputs(": releases</title>\n<link>",stdout);wa_xml(base);
    fputs("</link>\n<atom:link rel=\"self\" type=\"application/rss+xml\" href=\"",stdout);wa_xml(base);fputs("/releases.xml\"/>\n<description>",stdout);wa_xml(*lede?lede:space_title);fputs("</description>\n<generator>Aurekai Worlds</generator>\n",stdout);
    sqlite3_stmt *s=NULL;
    if(awm_rows(db,NULL,site->id,world,space,AWM_RELEASES,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        const char *id=wm_col(s,0),*on=wm_col(s,3);
        fputs("<item>\n<title>",stdout);wa_xml(wm_col(s,1));fputs("</title>\n<link>",stdout);wa_xml(base);fputs("/release/",stdout);wa_xml(id);fputs("</link>\n<guid isPermaLink=\"true\">",stdout);wa_xml(base);fputs("/release/",stdout);wa_xml(id);fputs("</guid>\n",stdout);
        if(*on){char date[40];wm_rss_date(on,date);printf("<pubDate>%s</pubDate>\n",date);}
        char desc[600];snprintf(desc,sizeof(desc),"%s%s%s%s%s",wm_type(wm_col(s,2)),party.name[0]?" by ":"",party.name,*wm_col(s,4)&&strncmp(wm_col(s,4),"space:",6)?", ":"",*wm_col(s,4)&&strncmp(wm_col(s,4),"space:",6)?wm_col(s,4):"");
        fputs("<description>",stdout);wa_xml(desc);fputs("</description>\n<category>",stdout);wa_xml(wm_type(wm_col(s,2)));fputs("</category>\n</item>\n",stdout);
    }
    sqlite3_finalize(s);fputs("</channel>\n</rss>\n",stdout);
}
static int serve_world_music_public(const char *method,const char *target,const char *host){
    const char *rest=target+8,*slash=strchr(rest,'/');char space[65],kind[12]="",id[65]="";
    if(!slash||slash==rest||(size_t)(slash-rest)>64)return 0;memcpy(space,rest,(size_t)(slash-rest));space[slash-rest]=0;
    if(strspn(space,"abcdefghijklmnopqrstuvwxyz0123456789-")!=strlen(space))return 0;
    const char *what=slash+1;
    if(!strcmp(what,"releases.xml"))snprintf(kind,sizeof(kind),"feed");
    else{const char *s2=strchr(what,'/');if(!s2||(size_t)(s2-what)>=sizeof(kind)||!s2[1]||strlen(s2+1)>64)return 0;memcpy(kind,what,(size_t)(s2-what));kind[s2-what]=0;snprintf(id,sizeof(id),"%s",s2+1);
        if(strcmp(kind,"release")&&strcmp(kind,"recording")&&strcmp(kind,"work")&&strcmp(kind,"listen"))return 0;
        if(strspn(id,"abcdefghijklmnopqrstuvwxyz0123456789-")!=strlen(id))return 0;}
    char normalized[256];if(!lower_host(host,normalized)||host_is_territory_apex(host))return 0;
    sqlite3 *db=cms_open_ro();PhSite site;
    if(!db||!ph_site(db,normalized,NULL,&site)){if(db)sqlite3_close(db);return 0;}
    if(strcmp(method,"GET")&&strcmp(method,"HEAD")){sqlite3_close(db);aw_error(405,"Use GET.\n");return 1;}
    sqlite3_stmt *row=NULL;
    if(aw_public_space(db,site.id,space,&row)!=1){sqlite3_close(db);protocol_reply(404,"text/plain; charset=utf-8","There is no space at this address.\n");return 1;}
    char title[257],character[33],lede[601],state[16];sqlite3_int64 world=sqlite3_column_int64(row,6);
    snprintf(title,sizeof(title),"%s",wm_col(row,0));snprintf(character,sizeof(character),"%s",wm_col(row,3));snprintf(lede,sizeof(lede),"%s",wm_col(row,4));snprintf(state,sizeof(state),"%s",wm_col(row,5));
    sqlite3_finalize(row);
    if(strcmp(state,"public")){sqlite3_close(db);protocol_reply(410,"text/plain; charset=utf-8","Its owner has taken this space down.\n");return 1;}
    if(!world_web_look(character))snprintf(character,sizeof(character),"journal");
    WmParty party;
    if(!wm_party(db,NULL,site.id,world,space,&party)){sqlite3_close(db);protocol_reply(404,"text/plain; charset=utf-8","This space keeps no discography.\n");return 1;}
    if(!strcmp(kind,"feed"))wm_feed(db,&site,world,space,title,lede);
    else if(!strcmp(kind,"release"))wm_release_page(db,&site,world,space,title,character,id);
    else if(!strcmp(kind,"recording"))wm_recording_page(db,&site,world,space,title,character,id);
    else if(!strcmp(kind,"work"))wm_work_page(db,&site,world,space,title,character,id);
    else wm_room_page(db,&site,world,space,title,character,id);
    sqlite3_close(db);(void)wm_page_scripted;return 1;
}

/* -------------------------------------------------------------- collection */
static int serve_world_collection(const char *method,const char *query,const char *bearer,const char *auth_db){
    if(strcmp(method,"GET")){aw_error(405,"Open your collection with GET.\n");return 1;}
    char site_id[128]="",space[65]="",release[65]="";PhField params[]={{"site",site_id,sizeof(site_id),0},{"space",space,sizeof(space),0},{"release",release,sizeof(release),0}};
    if(!ph_fields(query,params,3,1024)||((*site_id||*space||*release)&&(!*site_id||!*space||!*release))){aw_error(400,"Choose a release.\n");return 1;}
    char actor[160],member[160];const char *scope=NULL;sqlite3 *db=cms_open_ro();
    if(!db||psetup_context(bearer,auth_db,"self",0,actor,sizeof(actor),member,sizeof(member),&scope)!=1){if(db)sqlite3_close(db);aw_error(404,"Sign in to keep a collection.\n");return 1;}
    cine_room_head("Your collection","journal","collection");
    fputs("<nav class=\"bar\" aria-label=\"Where you are\"><span class=\"home\">Your collection</span></nav><header class=\"hero\"><h1>Your collection</h1>"
          "<p class=\"lede\">Releases you keep from artists and labels on Aurekai. Only you see this list.</p></header>",stdout);
    if(*site_id){
        PhSite site;sqlite3_stmt *row=NULL;WmRelease *rel=calloc(1,sizeof(*rel));int shown=0;
        if(rel&&ph_site(db,NULL,site_id,&site)&&aw_public_space(db,site_id,space,&row)==1&&!strcmp(wm_col(row,5),"public")){
            sqlite3_int64 world=sqlite3_column_int64(row,6);
            if(wm_release(db,NULL,site_id,world,space,release,rel)){shown=1;int have=awm_collected(db,member,site_id,space,release)==1;
                fputs("<section class=\"aw-join\" data-collect-release=\"",stdout);html(release);fputs("\"><h2 class=\"region-label\">",stdout);fputs(wm_type(rel->type),stdout);fputs("</h2><h3><a href=\"https://",stdout);
                html(site.host);fputs("/worlds/",stdout);html(space);fputs("/release/",stdout);html(release);fputs("\">",stdout);html(rel->title);fputs("</a></h3>",stdout);
                fputs("<form method=\"post\" action=\"/workspace/world-music\">",stdout);ph_hidden("site",site_id);ph_hidden("space",space);ph_hidden("id",release);ph_hidden("operation",have?"uncollect":"collect");ph_hidden("return","collection");
                fputs(have?"<p class=\"aw-note\" data-collected>It is in your collection.</p><button type=\"submit\" class=\"aw-quiet\">Remove from your collection</button></form>":
                           "<button type=\"submit\">Add to your collection</button></form>",stdout);fputs("</section>",stdout);}
        }
        sqlite3_finalize(row);free(rel);if(!shown)fputs("<p class=\"aw-note\">That release is not published right now.</p>",stdout);
    }
    sqlite3_stmt *s=NULL;int n=0;
    if(awm_collection(db,member,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        if(!n++)fputs("<section class=\"aw-talk\"><h2 class=\"region-label\">Kept</h2><ol data-collection>",stdout);
        PhSite site;int live=sqlite3_column_int(s,7)&&ph_site(db,NULL,wm_col(s,0),&site);
        fputs("<li",stdout);if(!live)fputs(" data-state=\"gone\"",stdout);fputs("><h3>",stdout);
        if(live){fputs("<a href=\"https://",stdout);html(site.host);fputs("/worlds/",stdout);html(wm_col(s,1));fputs("/release/",stdout);html(wm_col(s,2));fputs("\">",stdout);}
        html(*wm_col(s,3)?wm_col(s,3):wm_col(s,2));if(live)fputs("</a>",stdout);fputs("</h3><p class=\"by\">",stdout);
        if(live){fputs(wm_type(wm_col(s,4)),stdout);if(*wm_col(s,5)){fputs(" &middot; ",stdout);html(wm_col(s,5));}}else fputs("No longer published by its owner",stdout);
        fputs(" &middot; kept ",stdout);html(wm_col(s,6));fputs("</p></li>",stdout);}
    sqlite3_finalize(s);if(n)fputs("</ol></section>",stdout);else fputs("<p class=\"aw-note\" data-collection-empty>Nothing kept yet. Open a release on an artist&rsquo;s page and add it here.</p>",stdout);
    fputs("</main></body></html>",stdout);sqlite3_close(db);return 1;
}
#endif
