#ifndef BONFYRE_ACCOUNT_WORLD_AUDIO_WEB_H
#define BONFYRE_ACCOUNT_WORLD_AUDIO_WEB_H
#include "../../BonfyreCMS/src/account_world_audio.h"

/* Stations and shows inside account spaces (account-world-audio-1).
 *
 * /workspace/world-audio            private API: programme, schedule, episodes, marks, transcript
 * studio section on /workspace/worlds  the owner's forms for the same
 * /worlds/X                          on air now and next, live stream, schedule, episode details
 * /worlds/X/feed.xml                 RSS 2.0 with iTunes and Podcasting 2.0 tags
 * /worlds/X/schedule.ics             the weekly schedule for a calendar
 * /worlds/X/chapters/N.json          Podcasting 2.0 chapters of item N
 * /worlds/X/transcript/N.vtt         reviewed cues of item N
 *
 * Audio plays, and enters the feed as an enclosure, only when the piece's own
 * source states open terms (world_web_open_terms) at the time of the request.
 * Anything else is linked to where it is published. Nothing here copies media
 * or states rights. Included from account_world_web.h after the reader. */

/* defined further down in account_world_web.h */
static int world_web_open_terms(const char *r);
static int world_web_https(const char *u);
static int world_web_resolve(sqlite3 *db,const char *ref,PageMaterial *material,char loaded[96],PageItem *out);

/* ------------------------------------------------------------- helpers */
static void wa_clock(long long ms,char out[24]){
    if(ms<0)ms=0;long long s=ms/1000,h=s/3600,m=(s/60)%60,sec=s%60;
    if(h)snprintf(out,24,"%lld:%02lld:%02lld",h,m,sec);else snprintf(out,24,"%lld:%02lld",m,sec);
}
/* "h:mm:ss", "m:ss" or seconds, each with an optional ".mmm"; -1 if malformed. */
static long long wa_parse_clock(const char *s){
    if(!s||!*s||strlen(s)>16)return -1;long long part[3]={0},ms=0;int n=0;const char *p=s;
    for(;;){if(!isdigit((unsigned char)*p))return -1;long long v=0;int d=0;while(isdigit((unsigned char)*p)&&d<6){v=v*10+(*p++-'0');d++;}
        if(n>=3)return -1;part[n++]=v;if(*p==':'){p++;continue;}break;}
    if(*p=='.'){p++;int d=0;while(isdigit((unsigned char)*p)&&d<3){ms=ms*10+(*p++-'0');d++;}if(!d)return -1;while(d++<3)ms*=10;}
    if(*p)return -1;
    long long h=n==3?part[0]:0,m=n==3?part[1]:n==2?part[0]:0,sec=part[n-1];
    if(n>1&&sec>59)return -1;if(n==3&&m>59)return -1;
    return ((h*60+m)*60+sec)*1000+ms;
}
static void wa_xml(const char *s){ /* XML text and attribute escape */
    for(const unsigned char *p=(const unsigned char*)(s?s:"");*p;p++){
        if(*p=='&')fputs("&amp;",stdout);else if(*p=='<')fputs("&lt;",stdout);else if(*p=='>')fputs("&gt;",stdout);
        else if(*p=='"')fputs("&quot;",stdout);else if(*p=='\'')fputs("&apos;",stdout);
        else if(*p<32&&*p!='\n'&&*p!='\t')fputc(' ',stdout);else fputc(*p,stdout);
    }
}
static const char *wa_day(int iso){static const char *d[]={"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"};return iso>=1&&iso<=7?d[iso-1]:"";}
static int wa_zone_known(const char *zone){
    if(!zone||!*zone)return 0;if(!strcmp(zone,"UTC"))return 1;
    if(strstr(zone,"..")||*zone=='/'||strlen(zone)>64)return 0;
    char path[160];snprintf(path,sizeof(path),"/usr/share/zoneinfo/%s",zone);struct stat st;return stat(path,&st)==0&&S_ISREG(st.st_mode);
}
/* Station-local wall clock for an instant; the host's zone database decides. */
static int wa_local(const char *zone,time_t now,struct tm *out){
    if(!wa_zone_known(zone))return 0;
    char saved[96]="";const char *old=getenv("TZ");int had=old!=NULL;if(had)snprintf(saved,sizeof(saved),"%s",old);
    setenv("TZ",zone,1);tzset();int ok=localtime_r(&now,out)!=NULL;
    if(had)setenv("TZ",saved,1);else unsetenv("TZ");tzset();return ok;
}
static int wa_open_audio(const PageItem *it){
    return it&&it->kind==BF_PAGE_MATERIAL_AUDIO&&world_web_open_terms(it->rights)&&world_web_https(it->asset);
}
static const char *wa_mime(const char *u){
    const char *q=strchr(u,'?');size_t n=q?(size_t)(q-u):strlen(u);
    struct{const char *ext,*type;}m[]={{".mp3","audio/mpeg"},{".m4a","audio/mp4"},{".aac","audio/aac"},{".ogg","audio/ogg"},{".oga","audio/ogg"},
                                        {".opus","audio/ogg"},{".wav","audio/wav"},{".flac","audio/flac"}};
    for(size_t i=0;i<sizeof(m)/sizeof(m[0]);i++){size_t e=strlen(m[i].ext);if(n>e&&!strncasecmp(u+n-e,m[i].ext,e))return m[i].type;}
    return "audio/mpeg";
}
static const char *wa_col(sqlite3_stmt *s,int i){const char *v=(const char*)sqlite3_column_text(s,i);return v?v:"";}

typedef struct{char kind[12],timezone[72],language[20],author[164],category[68],stream[100];int explicit_content;} WaSound;
/* subject NULL = released public version */
static int wa_sound(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 world,const char *space,WaSound *out){
    sqlite3_stmt *s=NULL;memset(out,0,sizeof(*out));
    if(awa_rows(db,subject,ns,world,space,AWA_SOUND,NULL,&s)!=1||sqlite3_step(s)!=SQLITE_ROW){sqlite3_finalize(s);return 0;}
    snprintf(out->kind,sizeof(out->kind),"%s",wa_col(s,0));snprintf(out->timezone,sizeof(out->timezone),"%s",wa_col(s,1));
    snprintf(out->language,sizeof(out->language),"%s",wa_col(s,2));snprintf(out->author,sizeof(out->author),"%s",wa_col(s,3));
    snprintf(out->category,sizeof(out->category),"%s",wa_col(s,4));out->explicit_content=sqlite3_column_int(s,5);
    snprintf(out->stream,sizeof(out->stream),"%s",wa_col(s,6));sqlite3_finalize(s);return 1;
}
typedef struct{int slot,weekday,start,minutes;char programme[164],host[164],ref[100];} WaSlot;
static int wa_slots(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 world,const char *space,WaSlot out[AWA_SLOT_LIMIT]){
    sqlite3_stmt *s=NULL;int n=0;if(awa_rows(db,subject,ns,world,space,AWA_SLOTS,NULL,&s)!=1)return 0;
    while(n<AWA_SLOT_LIMIT&&sqlite3_step(s)==SQLITE_ROW){WaSlot *o=&out[n++];o->slot=sqlite3_column_int(s,0);o->weekday=sqlite3_column_int(s,1);o->start=sqlite3_column_int(s,2);o->minutes=sqlite3_column_int(s,3);
        snprintf(o->programme,sizeof(o->programme),"%s",wa_col(s,4));snprintf(o->host,sizeof(o->host),"%s",wa_col(s,5));snprintf(o->ref,sizeof(o->ref),"%s",wa_col(s,6));}
    sqlite3_finalize(s);return n;
}
typedef struct{int found,season,number;char published[12];} WaEpisode;
static WaEpisode wa_episode(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 world,const char *space,const char *ref){
    WaEpisode e={0};sqlite3_stmt *s=NULL;
    if(awa_rows(db,subject,ns,world,space,AWA_EPISODES,ref,&s)==1&&sqlite3_step(s)==SQLITE_ROW){e.found=1;e.season=sqlite3_column_int(s,1);e.number=sqlite3_column_int(s,2);snprintf(e.published,sizeof(e.published),"%s",wa_col(s,3));}
    sqlite3_finalize(s);return e;
}
/* minute of the week, Monday 00:00 = 0 */
static int wa_week_minute(const struct tm *t){int iso=t->tm_wday?t->tm_wday:7;return (iso-1)*1440+t->tm_hour*60+t->tm_min;}

/* ------------------------------------------------------------- studio */
static void wa_hidden(const char *site,sqlite3_int64 world,const char *space,const char *op,long long revision){
    char n[24];ph_hidden("site",site);snprintf(n,sizeof(n),"%lld",(long long)world);ph_hidden("world",n);ph_hidden("space",space);
    ph_hidden("operation",op);snprintf(n,sizeof(n),"%lld",revision);ph_hidden("revision",n);ph_hidden("return","studio");
}
static void wa_form(const char *site,sqlite3_int64 world,const char *space,const char *op,long long revision,const char *name){
    fputs("<form method=\"post\" action=\"/workspace/world-audio\"",stdout);if(name){fputs(" data-sound-form=\"",stdout);html(name);fputs("\"",stdout);}fputs(">",stdout);
    wa_hidden(site,world,space,op,revision);
}
static const char *wa_mark_label(const char *kind){
    return !strcmp(kind,"track")?"Track":!strcmp(kind,"chapter")?"Chapter":!strcmp(kind,"cue")?"Transcript line":!strcmp(kind,"clip")?"Clip":
           !strcmp(kind,"person")?"Person":!strcmp(kind,"source")?"Source":kind;
}

static void world_audio_studio(sqlite3 *db,const char *subject,const char *site_id,sqlite3_int64 world,const char *space,long long revision,const char *state){
    WaSound snd;int have=wa_sound(db,subject,site_id,world,space,&snd);PhSite site;ph_site(db,NULL,site_id,&site);
    fputs("<section id=\"sound\" class=\"aw-sound-studio\" data-world-sound-studio><h3>Station or show</h3>"
          "<p>Set this up if the space is a radio station or a podcast. Listeners only hear a recording when its own source states open terms, such as CC BY or public domain. Anything else is linked to where it is published.</p>",stdout);
    wa_form(site_id,world,space,"sound",revision,"sound");
    fputs("<div class=\"aw-row\"><label>This space is<select name=\"kind\"><option value=\"station\"",stdout);if(have&&!strcmp(snd.kind,"station"))fputs(" selected",stdout);
    fputs(">A station, with a weekly schedule</option><option value=\"show\"",stdout);if(have&&!strcmp(snd.kind,"show"))fputs(" selected",stdout);
    fputs(">A show, with episodes and a feed</option></select></label><label>Time zone<small>The station&rsquo;s own clock, such as America/Chicago</small><input name=\"timezone\" required maxlength=\"64\" list=\"aw-zones\" value=\"",stdout);
    html(have?snd.timezone:"UTC");fputs("\"><datalist id=\"aw-zones\">",stdout);
    const char *zones[]={"UTC","America/Chicago","America/New_York","America/Los_Angeles","America/Denver","Europe/London","Europe/Berlin","Europe/Paris","Africa/Lagos","Asia/Tokyo","Asia/Kolkata","Australia/Sydney"};
    for(size_t i=0;i<sizeof(zones)/sizeof(zones[0]);i++){fputs("<option value=\"",stdout);html(zones[i]);fputs("\">",stdout);}
    fputs("</datalist></label></div><div class=\"aw-row\"><label>Made by<small>The name listeners see</small><input name=\"author\" required maxlength=\"160\" value=\"",stdout);html(have?snd.author:site.name[0]?site.name:"");
    fputs("\"></label><label>Language<small>A code such as en or es-MX</small><input name=\"language\" required maxlength=\"16\" pattern=\"[A-Za-z-]{2,16}\" value=\"",stdout);html(have?snd.language:"en");
    fputs("\"></label><label>Category<small>Used by podcast apps</small><input name=\"category\" required maxlength=\"64\" list=\"aw-categories\" value=\"",stdout);html(have?snd.category:"Music");
    fputs("\"><datalist id=\"aw-categories\"><option value=\"Music\"><option value=\"Arts\"><option value=\"History\"><option value=\"Society &amp; Culture\"><option value=\"News\"><option value=\"Science\"><option value=\"Education\"><option value=\"Comedy\"><option value=\"Fiction\"></datalist></label></div>",stdout);
    fputs("<label>Live stream<small>Optional. The address after /o/ of the piece that is the stream. It plays only if its source states open terms</small><input name=\"stream\" maxlength=\"120\" value=\"",stdout);html(have?snd.stream:"");
    fputs("\"></label><label class=\"aw-check\"><input type=\"checkbox\" name=\"explicit\" value=\"1\"",stdout);if(have&&snd.explicit_content)fputs(" checked",stdout);
    fputs("> Contains explicit language</label><div class=\"aw-actions\"><button type=\"submit\">Save station or show</button></div></form>",stdout);
    if(!have){fputs("</section>",stdout);return;}
    /* schedule: stations only */
    WaSlot slots[AWA_SLOT_LIMIT];int ns=wa_slots(db,subject,site_id,world,space,slots),used[AWA_SLOT_LIMIT+1]={0};
    if(!strcmp(snd.kind,"station")){
    fputs("<h3>Weekly schedule</h3>",stdout);
    if(!ns)fputs("<p>No programmes scheduled yet.</p>",stdout);
    else{fputs("<table data-sound-schedule><thead><tr><th>When</th><th>Programme</th><th></th></tr></thead><tbody>",stdout);
        for(int i=0;i<ns;i++){used[slots[i].slot]=1;int end=slots[i].start+slots[i].minutes;
            printf("<tr data-slot=\"%d\"><td>%s %02d:%02d&ndash;%02d:%02d</td><td>",slots[i].slot,wa_day(slots[i].weekday),slots[i].start/60,slots[i].start%60,(end/60)%24,end%60);
            html(slots[i].programme);if(slots[i].host[0]){fputs(" with ",stdout);html(slots[i].host);}fputs("</td><td>",stdout);
            wa_form(site_id,world,space,"unslot",revision,NULL);char n[8];snprintf(n,sizeof(n),"%d",slots[i].slot);ph_hidden("slot",n);fputs("<button type=\"submit\" class=\"aw-quiet\">Remove</button></form></td></tr>",stdout);}
        fputs("</tbody></table>",stdout);}
    int free_slot=0;for(int i=1;i<=AWA_SLOT_LIMIT&&!free_slot;i++)if(!used[i])free_slot=i;
    if(free_slot){wa_form(site_id,world,space,"slot",revision,"slot");char n[8];snprintf(n,sizeof(n),"%d",free_slot);ph_hidden("slot",n);
        fputs("<div class=\"aw-row\"><label>Day<select name=\"weekday\">",stdout);for(int d=1;d<=7;d++)printf("<option value=\"%d\">%s</option>",d,wa_day(d));
        fputs("</select></label><label>Starts<input type=\"time\" name=\"start\" required value=\"21:00\"></label><label>Minutes<input type=\"number\" name=\"minutes\" required min=\"1\" max=\"1440\" value=\"60\"></label></div>"
              "<div class=\"aw-row\"><label>Programme<input name=\"title\" required maxlength=\"160\"></label><label>Host<small>Optional</small><input name=\"host\" maxlength=\"160\"></label></div>"
              "<div class=\"aw-actions\"><button type=\"submit\">Add to the schedule</button></div></form>",stdout);}
    }
    /* episodes */
    fputs("<h3>Episodes</h3><p>Each recording in this space can carry its date, chapters, a tracklist, people, cited sources, clips and a transcript. Transcript lines and clips stay hidden from listeners until you mark them reviewed. Tracks you have not confirmed are shown as unconfirmed.</p>",stdout);
    sqlite3_stmt *items=NULL;int shown=0;
    if(aw_items(db,subject,site_id,world,space,&items)==1)while(sqlite3_step(items)==SQLITE_ROW){
        int pos=sqlite3_column_int(items,0);const char *ref=wa_col(items,1);if(!strncmp(ref,"space:",6))continue;shown++;
        WaEpisode ep=wa_episode(db,subject,site_id,world,space,ref);
        printf("<details data-sound-episode=\"%d\"><summary>%d. ",pos,pos);world_web_words(wa_col(items,2));fputs(" <code>",stdout);html(ref);fputs("</code>",stdout);
        if(ep.found)printf(" &middot; season %d, episode %d, %s",ep.season,ep.number,ep.published);fputs("</summary>",stdout);
        wa_form(site_id,world,space,"episode",revision,"episode");ph_hidden("ref",ref);
        char n[16];fputs("<div class=\"aw-row\"><label>Season<input type=\"number\" name=\"season\" min=\"0\" max=\"999\" value=\"",stdout);snprintf(n,sizeof(n),"%d",ep.found?ep.season:1);html(n);
        fputs("\"></label><label>Episode<input type=\"number\" name=\"number\" min=\"0\" max=\"9999\" value=\"",stdout);snprintf(n,sizeof(n),"%d",ep.found?ep.number:pos);html(n);
        fputs("\"></label><label>Date<input type=\"date\" name=\"published\" required value=\"",stdout);html(ep.found?ep.published:"");fputs("\"></label></div><div class=\"aw-actions\"><button type=\"submit\">Save episode details</button></div></form>",stdout);
        sqlite3_stmt *marks=NULL;int nm=0;
        if(awa_rows(db,subject,site_id,world,space,AWA_MARKS,ref,&marks)==1)while(sqlite3_step(marks)==SQLITE_ROW){
            if(!nm++)fputs("<table data-sound-marks><thead><tr><th>At</th><th>What</th><th>State</th><th></th></tr></thead><tbody>",stdout);
            long long id=sqlite3_column_int64(marks,0),start=sqlite3_column_int64(marks,3),end=sqlite3_column_int64(marks,4);int checked=sqlite3_column_int(marks,8);
            const char *kind=wa_col(marks,2);char a[24],b[24];wa_clock(start,a);wa_clock(end,b);
            printf("<tr data-mark=\"%lld\" data-mark-kind=\"%s\"><td>",id,kind);
            if(strcmp(kind,"person")){html(a);if(end){fputs("&ndash;",stdout);html(b);}}fputs("</td><td><strong>",stdout);html(wa_mark_label(kind));fputs("</strong> ",stdout);
            const char *credit=wa_col(marks,6);if(*credit){html(credit);fputs(!strcmp(kind,"person")?" &middot; ":": ",stdout);}html(wa_col(marks,5));
            const char *href=wa_col(marks,7);if(*href){fputs(" <code>",stdout);html(href);fputs("</code>",stdout);}fputs("</td><td>",stdout);
            int reviewable=!strcmp(kind,"track")||!strcmp(kind,"cue")||!strcmp(kind,"clip");
            if(reviewable){fputs("<span class=\"aw-flag\"",stdout);if(checked)fputs(" data-ok",stdout);fputs(">",stdout);
                fputs(!strcmp(kind,"track")?(checked?"Confirmed":"Unconfirmed"):(checked?"Reviewed":"Needs review"),stdout);fputs("</span>",stdout);}
            fputs("</td><td>",stdout);char idn[24];snprintf(idn,sizeof(idn),"%lld",id);
            if(reviewable){wa_form(site_id,world,space,"check",revision,NULL);ph_hidden("id",idn);if(!checked)ph_hidden("checked","1");
                fputs("<button type=\"submit\" class=\"aw-quiet\">",stdout);fputs(checked?"Mark unreviewed":!strcmp(kind,"track")?"Confirm":"Mark reviewed",stdout);fputs("</button></form>",stdout);}
            wa_form(site_id,world,space,"unmark",revision,NULL);ph_hidden("id",idn);fputs("<button type=\"submit\" class=\"aw-quiet\">Remove</button></form></td></tr>",stdout);
        }
        sqlite3_finalize(marks);if(nm)fputs("</tbody></table>",stdout);
        wa_form(site_id,world,space,"mark",revision,"mark");ph_hidden("ref",ref);
        fputs("<div class=\"aw-row\"><label>Add<select name=\"type\"><option value=\"chapter\">Chapter</option><option value=\"track\">Track</option><option value=\"clip\">Clip</option>"
              "<option value=\"person\">Person</option><option value=\"source\">Cited source</option><option value=\"cue\">Transcript line</option></select></label>"
              "<label>At<small>h:mm:ss or m:ss</small><input name=\"at\" required maxlength=\"16\" value=\"0:00\"></label><label>Until<small>Clips and transcript lines</small><input name=\"until\" maxlength=\"16\"></label></div>"
              "<div class=\"aw-row\"><label>Title or words<input name=\"title\" required maxlength=\"600\"></label><label>Artist, speaker or role<small>For people: host, guest, producer</small><input name=\"credit\" maxlength=\"160\"></label>"
              "<label>Link<small>Optional https address or piece address</small><input name=\"href\" maxlength=\"512\"></label></div>"
              "<label class=\"aw-check\"><input type=\"checkbox\" name=\"checked\" value=\"1\"> Confirmed or reviewed</label><div class=\"aw-actions\"><button type=\"submit\">Add</button></div></form>",stdout);
        wa_form(site_id,world,space,"transcript",revision,"transcript");ph_hidden("ref",ref);
        fputs("<label>Transcript<small>Paste WebVTT from your transcription. Lines arrive unreviewed; reviewed lines are kept when you paste again</small><textarea name=\"vtt\" rows=\"4\" required placeholder=\"WEBVTT\"></textarea></label>"
              "<div class=\"aw-actions\"><button type=\"submit\" class=\"aw-quiet\">Bring in transcript</button></div></form></details>",stdout);
    }
    sqlite3_finalize(items);if(!shown)fputs("<p>Add a recording to this space first.</p>",stdout);
    if(state&&!strcmp(state,"public")&&site.host[0]){fputs("<p>Podcast apps can follow <a href=\"https://",stdout);html(site.host);fputs("/worlds/",stdout);html(space);fputs("/feed.xml\">this feed</a> once an episode has a date and a playable recording.</p>",stdout);}
    fputs("</section>",stdout);
}

/* ------------------------------------------------------------- private API */
enum{WA_SITE,WA_WORLD,WA_SPACE,WA_REVISION,WA_OP,WA_RETURN,WA_KIND,WA_ZONE,WA_LANGUAGE,WA_AUTHOR,WA_CATEGORY,WA_EXPLICIT,WA_STREAM,
     WA_SLOT,WA_WEEKDAY,WA_START,WA_MINUTES,WA_TITLE,WA_HOST,WA_REF,WA_SEASON,WA_NUMBER,WA_PUBLISHED,WA_TYPE,WA_AT,WA_UNTIL,
     WA_CREDIT,WA_HREF,WA_CHECKED,WA_ID,WA_VTT,WA_COUNT};
static int wa_int(const char *s,long long lo,long long hi,long long *out){sqlite3_int64 v=0;if(!world_web_number(s,&v,1)||v<lo||v>hi)return 0;*out=v;return 1;}
static void wa_json_rows(bf_jsonw *w,sqlite3 *db,const char *subject,const char *site,sqlite3_int64 world,const char *space,int table,const char *name){
    static const char *names[4][9]={{"kind","timezone","language","author","category","explicit","stream",0},{"slot","weekday","start_minute","minutes","programme","host","ref",0},
                                     {"ref","season","number","published",0},{"id","ref","kind","start_ms","end_ms","title","credit","href","checked"}};
    sqlite3_stmt *s=NULL;bf_jsonw_key(w,name);bf_jsonw_arr(w);
    if(awa_rows(db,subject,site,world,space,table,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){bf_jsonw_obj(w);
        for(int i=0;i<9&&names[table][i];i++){bf_jsonw_key(w,names[table][i]);if(sqlite3_column_type(s,i)==SQLITE_INTEGER)bf_jsonw_int(w,sqlite3_column_int64(s,i));else bf_jsonw_str(w,wa_col(s,i));}
        bf_jsonw_obj_end(w);}
    sqlite3_finalize(s);bf_jsonw_arr_end(w);
}
static int world_audio_api(const char *method,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db){
    int post=!strcmp(method,"POST"),get=!strcmp(method,"GET");
    if(!post&&!get){aw_error(405,"Use GET or POST.\n");return 1;}
    if(post&&(!ph_origin(raw,host)||(query&&*query))){aw_error(403,"Submit from this workspace.\n");return 1;}
    static const char *names[WA_COUNT]={"site","world","space","revision","operation","return","kind","timezone","language","author","category","explicit","stream",
        "slot","weekday","start","minutes","title","host","ref","season","number","published","type","at","until","credit","href","checked","id","vtt"};
    static const size_t caps[WA_COUNT]={128,24,65,24,24,8,12,72,20,161,65,4,121,8,4,8,8,601,161,161,8,8,12,12,20,20,161,513,4,24,AWA_VTT_LIMIT+1};
    char *buf=calloc(WA_COUNT,AWA_VTT_LIMIT+2);
    if(!buf){aw_error(503,"Workspace unavailable.\n");return 1;}
    char *val[WA_COUNT];PhField fields[WA_COUNT];
    for(int i=0;i<WA_COUNT;i++){val[i]=buf+(size_t)i*(AWA_VTT_LIMIT+2);fields[i].key=names[i];fields[i].value=val[i];fields[i].cap=caps[i];fields[i].multiline=i==WA_VTT;}
    sqlite3_int64 world=0,rev=0;
    if(!ph_fields(post?body:query,fields,WA_COUNT,AWA_VTT_LIMIT*3+8192)||!*val[WA_SITE]||!*val[WA_SPACE]||!world_web_number(val[WA_WORLD],&world,0)||
       (post&&!world_web_number(val[WA_REVISION],&rev,0))||(*val[WA_RETURN]&&strcmp(val[WA_RETURN],"studio"))){free(buf);aw_error(400,"Invalid workspace request.\n");return 1;}
    const char *op=val[WA_OP];
    if(get&&strcmp(op,"draft")){free(buf);aw_error(400,"Invalid workspace request.\n");return 1;}
    const char *path=getenv("BONFYRE_CMS_DB");sqlite3 *db=NULL;char subject[160],again[160];const char *site=val[WA_SITE],*space=val[WA_SPACE];
    if(!path||!*path||sqlite3_open_v2(path,&db,post?SQLITE_OPEN_READWRITE:SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){if(db)sqlite3_close(db);free(buf);aw_error(503,"Workspace unavailable.\n");return 1;}
    sqlite3_busy_timeout(db,2000);
    if(!world_web_admit(bearer,auth_db,db,site,post,subject)){sqlite3_close(db);free(buf);aw_error(404,"Workspace unavailable.\n");return 1;}
    if(post&&awa_initialize(db)!=1){sqlite3_close(db);free(buf);aw_error(503,"Workspace unavailable.\n");return 1;}
    if(!world_web_admit(bearer,auth_db,db,site,post,again)||strcmp(subject,again)){sqlite3_close(db);free(buf);aw_error(404,"Workspace unavailable.\n");return 1;}
    int r=0,R=(int)rev,imported=0;long long a=0,b=0,c=0,d=0;
    if(get){
        sqlite3_stmt *probe=NULL;r=aw_space_get(db,subject,site,world,space,&probe);sqlite3_finalize(probe);
        if(r!=1){sqlite3_close(db);free(buf);aw_error(r<0?503:404,"Workspace request could not be completed.\n");return 1;}
        bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"state");bf_jsonw_str(&w,"draft");
        wa_json_rows(&w,db,subject,site,world,space,AWA_SOUND,"sound");wa_json_rows(&w,db,subject,site,world,space,AWA_SLOTS,"schedule");
        wa_json_rows(&w,db,subject,site,world,space,AWA_EPISODES,"episodes");wa_json_rows(&w,db,subject,site,world,space,AWA_MARKS,"marks");
        bf_jsonw_obj_end(&w);sqlite3_close(db);free(buf);
        if(w.oom)aw_error(503,"Workspace unavailable.\n");else aw_response(200,"application/json",w.buf,w.len,0);bf_jsonw_free(&w);return 1;
    }
    if(!strcmp(op,"sound")){if(wa_zone_known(val[WA_ZONE]))r=awa_sound_put(db,subject,site,world,space,val[WA_KIND],val[WA_ZONE],val[WA_LANGUAGE],val[WA_AUTHOR],
                                            val[WA_CATEGORY],!strcmp(val[WA_EXPLICIT],"1"),val[WA_STREAM],R);}
    else if(!strcmp(op,"slot")){
        const char *t=val[WA_START];int hh=-1,mm=-1;
        if(strlen(t)==5&&t[2]==':'&&isdigit((unsigned char)t[0])&&isdigit((unsigned char)t[1])&&isdigit((unsigned char)t[3])&&isdigit((unsigned char)t[4])){hh=atoi(t);mm=atoi(t+3);}
        if(hh>=0&&hh<24&&mm>=0&&mm<60&&wa_int(val[WA_SLOT],1,AWA_SLOT_LIMIT,&a)&&wa_int(val[WA_WEEKDAY],1,7,&b)&&wa_int(val[WA_MINUTES],1,1440,&c))
            r=awa_slot_put(db,subject,site,world,space,(int)a,(int)b,hh*60+mm,(int)c,val[WA_TITLE],val[WA_HOST],val[WA_REF],R);
    }
    else if(!strcmp(op,"unslot")){if(wa_int(val[WA_SLOT],1,AWA_SLOT_LIMIT,&a))r=awa_slot_remove(db,subject,site,world,space,(int)a,R);}
    else if(!strcmp(op,"episode")){if(wa_int(val[WA_SEASON],0,999,&a)&&wa_int(val[WA_NUMBER],0,9999,&b))r=awa_episode_put(db,subject,site,world,space,val[WA_REF],(int)a,(int)b,val[WA_PUBLISHED],R);}
    else if(!strcmp(op,"mark")){
        a=wa_parse_clock(val[WA_AT]);b=*val[WA_UNTIL]?wa_parse_clock(val[WA_UNTIL]):0;
        if(a>=0&&b>=0)r=awa_mark_put(db,subject,site,world,space,val[WA_REF],val[WA_TYPE],a,b,val[WA_TITLE],val[WA_CREDIT],val[WA_HREF],!strcmp(val[WA_CHECKED],"1"),R);
    }
    else if(!strcmp(op,"check")){if(wa_int(val[WA_ID],1,INT64_MAX/2,&d))r=awa_mark_check(db,subject,site,world,space,d,!strcmp(val[WA_CHECKED],"1"),R);}
    else if(!strcmp(op,"unmark")){if(wa_int(val[WA_ID],1,INT64_MAX/2,&d))r=awa_mark_remove(db,subject,site,world,space,d,R);}
    else if(!strcmp(op,"transcript"))r=awa_cues_import(db,subject,site,world,space,val[WA_REF],val[WA_VTT],R,&imported);
    else{sqlite3_close(db);free(buf);aw_error(400,"Invalid workspace request.\n");return 1;}
    sqlite3_close(db);
    if(r!=1){free(buf);aw_error(r<0?503:409,"Workspace request could not be completed.\n");return 1;}
    if(*val[WA_RETURN]){world_web_studio_location(site,world,space);free(buf);return 1;}
    bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"state");bf_jsonw_str(&w,"saved");
    if(!strcmp(op,"transcript")){bf_jsonw_key(&w,"imported");bf_jsonw_int(&w,imported);}
    bf_jsonw_obj_end(&w);free(buf);
    if(w.oom)aw_error(503,"Workspace unavailable.\n");else aw_response(200,"application/json",w.buf,w.len,0);bf_jsonw_free(&w);return 1;
}

/* ------------------------------------------------------------- public page */
static int wa_page_scripted;
/* Station header: live stream, on air now and next, the week. */
static void world_audio_station(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *space){
    WaSound snd;if(!wa_sound(db,NULL,site->id,world,space,&snd))return;
    WaSlot slots[AWA_SLOT_LIMIT];int n=wa_slots(db,NULL,site->id,world,space,slots);
    fputs("<section class=\"aw-sound\" data-world-sound=\"",stdout);html(snd.kind);fputs("\" aria-label=\"",stdout);fputs(!strcmp(snd.kind,"station")?"On air":"About the show",stdout);fputs("\">",stdout);
    fputs("<p class=\"aw-credit\">",stdout);fputs(!strcmp(snd.kind,"station")?"A station by ":"A show by ",stdout);html(snd.author);fputs("</p>",stdout);
    if(snd.stream[0]){
        PageMaterial *m=calloc(1,sizeof(*m));PageItem *it=calloc(1,sizeof(*it));char loaded[96]="";
        int found=m&&it&&world_web_resolve(db,snd.stream,m,loaded,it);
        if(found&&wa_open_audio(it)){fputs("<div class=\"aw-live\" id=\"live\" data-live=\"playing\"><p class=\"aw-role\">Live</p><audio controls preload=\"none\" src=\"",stdout);html(it->asset);
            fputs("\"></audio><p class=\"aw-terms\">",stdout);html(it->title);fputs(" &middot; ",stdout);html(it->rights);fputs("</p></div>",stdout);}
        else if(found){fputs("<div class=\"aw-live\" data-live=\"linked\"><p class=\"aw-body\">The live stream&rsquo;s source does not state open terms, so it is not played here. ",stdout);
            if(public_http_url(it->href)){fputs("<a rel=\"noopener\" href=\"",stdout);html(it->href);fputs("\">Listen where it is published</a>",stdout);}fputs("</p></div>",stdout);}
        else fputs("<div class=\"aw-live\" data-live=\"gone\"><p class=\"aw-body\">The live stream is not available from its source right now.</p></div>",stdout);
        free(m);free(it);
    }else if(!strcmp(snd.kind,"station"))fputs("<div class=\"aw-live\" data-live=\"none\"><p class=\"aw-body\">There is no live stream. Recordings below play on demand.</p></div>",stdout);
    struct tm local;time_t now=time(NULL);
    if(n&&wa_local(snd.timezone,now,&local)){
        int wk=wa_week_minute(&local),on=-1,next=-1,best=10081;
        for(int i=0;i<n;i++){int s=(slots[i].weekday-1)*1440+slots[i].start,since=((wk-s)%10080+10080)%10080,until=((s-wk)%10080+10080)%10080;
            if(since<slots[i].minutes&&on<0)on=i;else if(until>0&&until<best){best=until;next=i;}}
        if(next<0&&on>=0){for(int i=0;i<n;i++)if(i!=on){int s=(slots[i].weekday-1)*1440+slots[i].start,u=((s-wk)%10080+10080)%10080;if(!u)u=10080;if(u<best){best=u;next=i;}}}
        char stamp[8];strftime(stamp,sizeof(stamp),"%H:%M",&local);
        fputs("<div class=\"aw-onair\" data-onair>",stdout);
        if(on>=0){int end=slots[on].start+slots[on].minutes;printf("<p data-now><span class=\"aw-role\">On air now</span> <strong>");html(slots[on].programme);fputs("</strong>",stdout);
            if(slots[on].host[0]){fputs(" with ",stdout);html(slots[on].host);}printf(", until %02d:%02d</p>",(end/60)%24,end%60);}
        else fputs("<p data-now><span class=\"aw-role\">On air now</span> Nothing is scheduled right now.</p>",stdout);
        if(next>=0){time_t at=now-local.tm_sec+(time_t)best*60;struct tm u;gmtime_r(&at,&u);char iso[32];strftime(iso,sizeof(iso),"%Y-%m-%dT%H:%MZ",&u);
            printf("<p data-next><span class=\"aw-role\">Next</span> <time datetime=\"%s\">%s %02d:%02d</time> &middot; ",iso,best>=1440||slots[next].weekday!=(local.tm_wday?local.tm_wday:7)?wa_day(slots[next].weekday):"Today",slots[next].start/60,slots[next].start%60);
            html(slots[next].programme);if(slots[next].host[0]){fputs(" with ",stdout);html(slots[next].host);}fputs("</p>",stdout);}
        fputs("<p class=\"aw-terms\">Times are the station&rsquo;s clock, ",stdout);html(snd.timezone);printf(". It is %s there now.</p></div>",stamp);
    }else if(n){fputs("<p class=\"aw-terms\">The station&rsquo;s time zone is not known here, so what is on now cannot be shown.</p>",stdout);}
    if(n){fputs("<table class=\"aw-schedule\" data-world-schedule><caption>Every week</caption><tbody>",stdout);
        for(int i=0;i<n;i++){int end=slots[i].start+slots[i].minutes;printf("<tr><th scope=\"row\">%s</th><td>%02d:%02d&ndash;%02d:%02d</td><td>",wa_day(slots[i].weekday),slots[i].start/60,slots[i].start%60,(end/60)%24,end%60);
            if(slots[i].ref[0]&&!strncmp(slots[i].ref,"space:",6)){fputs("<a href=\"/worlds/",stdout);html(slots[i].ref+6);fputs("\">",stdout);html(slots[i].programme);fputs("</a>",stdout);}
            else if(slots[i].ref[0]){fputs("<a href=\"/o/",stdout);html(slots[i].ref);fputs("\">",stdout);html(slots[i].programme);fputs("</a>",stdout);}
            else html(slots[i].programme);
            if(slots[i].host[0]){fputs(" <span>with ",stdout);html(slots[i].host);fputs("</span>",stdout);}fputs("</td></tr>",stdout);}
        fputs("</tbody></table>",stdout);}
    fputs("<p class=\"aw-listen\">",stdout);
    if(n){fputs("<a href=\"/worlds/",stdout);html(space);fputs("/schedule.ics\" data-world-calendar>Add the schedule to your calendar</a>",stdout);}
    fputs("<a href=\"/worlds/",stdout);html(space);fputs("/feed.xml\" data-world-feed>Follow in a podcast app</a></p></section>",stdout);
}
/* Episode details under item N: people, chapters, tracklist, clips, sources, transcript. */
static void world_audio_episode(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *space,int position,const char *ref,const PageItem *item){
    if(!item||!strchr(ref,'/')||!strncmp(ref,"space:",6))return;
    WaEpisode ep=wa_episode(db,NULL,site->id,world,space,ref);sqlite3_stmt *s=NULL;
    if(awa_rows(db,NULL,site->id,world,space,AWA_MARKS,ref,&s)!=1){sqlite3_finalize(s);s=NULL;}
    typedef struct{long long start,end;char kind[8],title[601],credit[161],href[513];int checked;} Mark;
    Mark *m=calloc(AWA_MARK_LIMIT,sizeof(Mark));int n=0;
    while(s&&m&&n<AWA_MARK_LIMIT&&sqlite3_step(s)==SQLITE_ROW){Mark *k=&m[n++];k->start=sqlite3_column_int64(s,3);k->end=sqlite3_column_int64(s,4);
        snprintf(k->kind,sizeof(k->kind),"%s",wa_col(s,2));snprintf(k->title,sizeof(k->title),"%s",wa_col(s,5));snprintf(k->credit,sizeof(k->credit),"%s",wa_col(s,6));
        snprintf(k->href,sizeof(k->href),"%s",wa_col(s,7));k->checked=sqlite3_column_int(s,8);}
    sqlite3_finalize(s);
    if(!ep.found&&!n){free(m);return;}
    int play=wa_open_audio(item);char t[24],u[24];
    printf("<section class=\"aw-episode\" data-episode-for=\"piece-%d\" aria-label=\"Episode details\">",position);
    if(ep.found||item->duration_ms>0){fputs("<p class=\"aw-epmeta\">",stdout);
        if(ep.found){if(ep.season)printf("Season %d &middot; ",ep.season);if(ep.number)printf("Episode %d &middot; ",ep.number);
            struct tm d={0};d.tm_year=atoi(ep.published)-1900;d.tm_mon=atoi(ep.published+5)-1;d.tm_mday=atoi(ep.published+8);char nice[40];strftime(nice,sizeof(nice),"%e %B %Y",&d);
            fputs("<time datetime=\"",stdout);html(ep.published);fputs("\">",stdout);html(nice[0]==' '?nice+1:nice);fputs("</time>",stdout);}
        if(item->duration_ms>0)printf("%s%ld min",ep.found?" &middot; ":"",(item->duration_ms+59999)/60000);fputs("</p>",stdout);}
    #define WA_AT(k) do{wa_clock((k)->start,t);if(play)printf("<button type=\"button\" class=\"aw-at\" data-seek=\"%lld\" aria-label=\"Play from %s\">%s</button> ",(k)->start,t,t);else printf("<span class=\"aw-at\">%s</span> ",t);}while(0)
    #define WA_LINK_OPEN(k) do{if((k)->href[0]){fputs("<a ",stdout);if(!strncmp((k)->href,"https://",8)){fputs("rel=\"noopener\" href=\"",stdout);html((k)->href);}else{fputs("href=\"/o/",stdout);html((k)->href);}fputs("\">",stdout);}}while(0)
    /* people */
    int first=1;for(int i=0;i<n;i++)if(!strcmp(m[i].kind,"person")){
        if(first){fputs("<p class=\"aw-people\" data-episode-people>With ",stdout);first=0;}else fputs(", ",stdout);
        WA_LINK_OPEN(&m[i]);html(m[i].title);if(m[i].href[0])fputs("</a>",stdout);if(m[i].credit[0]){fputs(" <span>(",stdout);html(m[i].credit);fputs(")</span>",stdout);}}
    if(!first)fputs("</p>",stdout);
    struct{const char *kind,*heading,*attr;}lists[]={{"chapter","Chapters","data-episode-chapters"},{"track","Tracklist","data-episode-tracks"},{"clip","Clips","data-episode-clips"},{"source","Sources cited","data-episode-sources"}};
    for(size_t L=0;L<4;L++){int open=0;
        for(int i=0;i<n;i++){Mark *k=&m[i];if(strcmp(k->kind,lists[L].kind))continue;if(!strcmp(k->kind,"clip")&&!k->checked)continue;
            if(!open++){fputs("<div class=\"aw-marklist\"><h3>",stdout);fputs(lists[L].heading,stdout);printf("</h3><ol class=\"aw-marks\" %s>",lists[L].attr);}
            fputs("<li>",stdout);
            if(!strcmp(k->kind,"clip")){wa_clock(k->start,t);wa_clock(k->end,u);
                if(play)printf("<button type=\"button\" class=\"aw-at\" data-seek=\"%lld\" data-until=\"%lld\" aria-label=\"Play the clip from %s to %s\">%s&ndash;%s</button> ",k->start,k->end,t,u,t,u);
                else printf("<span class=\"aw-at\">%s&ndash;%s</span> ",t,u);html(k->title);}
            else{WA_AT(k);
                if(!strcmp(k->kind,"track")){if(k->credit[0]){html(k->credit);fputs(" &mdash; ",stdout);}WA_LINK_OPEN(k);html(k->title);if(k->href[0])fputs("</a>",stdout);
                    if(!k->checked)fputs(" <span class=\"aw-flag\" data-unconfirmed>not yet confirmed</span>",stdout);}
                else{WA_LINK_OPEN(k);html(k->title);if(k->href[0])fputs("</a>",stdout);if(k->credit[0]){fputs(" <span class=\"aw-credit\">",stdout);html(k->credit);fputs("</span>",stdout);}}}
            fputs("</li>",stdout);}
        if(open)fputs("</ol></div>",stdout);}
    int reviewed=0,pending=0;for(int i=0;i<n;i++)if(!strcmp(m[i].kind,"cue")){if(m[i].checked)reviewed++;else pending++;}
    if(reviewed){
        printf("<details class=\"aw-transcript\" data-episode-transcript><summary>Transcript</summary><label class=\"aw-search\">Search the transcript <input type=\"search\" data-cue-search autocomplete=\"off\"></label><ol class=\"aw-marks\">");
        for(int i=0;i<n;i++){Mark *k=&m[i];if(strcmp(k->kind,"cue")||!k->checked)continue;printf("<li data-cue data-start=\"%lld\" data-end=\"%lld\">",k->start,k->end);WA_AT(k);
            if(k->credit[0]){fputs("<strong>",stdout);html(k->credit);fputs(":</strong> ",stdout);}html(k->title);fputs("</li>",stdout);}
        fputs("</ol>",stdout);if(pending)printf("<p class=\"aw-terms\">%d more line%s still being checked.</p>",pending,pending==1?" is":"s are");
        printf("<p class=\"aw-terms\"><a href=\"/worlds/");html(space);printf("/transcript/%d.vtt\">Transcript file</a></p></details>",position);
    }
    printf("</section>");free(m);wa_page_scripted|=1;
    #undef WA_AT
    #undef WA_LINK_OPEN
}
static void world_audio_script(void){
    if(!wa_page_scripted)return;wa_page_scripted=0;
    fputs("<script>(()=>{const all=(s,r=document)=>[...r.querySelectorAll(s)];"
          "const audioFor=el=>{const box=el.closest('[data-episode-for]');return box&&document.querySelector('#'+box.dataset.episodeFor+' audio')};"
          "all('[data-seek]').forEach(b=>b.addEventListener('click',()=>{const a=audioFor(b);if(!a)return;a.currentTime=b.dataset.seek/1000;const until=+b.dataset.until||0;a.play();"
          "if(until){const stop=()=>{if(a.currentTime*1000>=until){a.pause();a.removeEventListener('timeupdate',stop)}};a.addEventListener('timeupdate',stop)}}));"
          "all('[data-cue-search]').forEach(i=>i.addEventListener('input',()=>{const q=i.value.trim().toLowerCase();all('[data-cue]',i.closest('details')).forEach(li=>{li.hidden=!!q&&!li.textContent.toLowerCase().includes(q)})}));"
          "all('[data-episode-for]').forEach(box=>{const a=document.querySelector('#'+box.dataset.episodeFor+' audio');if(!a)return;const cues=all('[data-cue]',box);"
          "a.addEventListener('timeupdate',()=>{const ms=a.currentTime*1000;cues.forEach(c=>{const on=ms>=+c.dataset.start&&ms<+c.dataset.end;c.toggleAttribute('aria-current',on)})})});"
          "if('mediaSession' in navigator&&window.MediaMetadata)all('audio').forEach(a=>a.addEventListener('play',()=>{const art=a.closest('article,.aw-live');const h=art&&art.querySelector('h2,.aw-terms');"
          "const by=art&&art.querySelector('.by');navigator.mediaSession.metadata=new MediaMetadata({title:h?h.textContent:document.title,artist:by?by.textContent:'',album:document.title})}));"
          "})()</script>",stdout);
}

/* ------------------------------------------------------------- feeds */
static void wa_rfc822(const char *date,char out[40]){
    struct tm d={0};d.tm_year=atoi(date)-1900;d.tm_mon=atoi(date+5)-1;d.tm_mday=atoi(date+8);d.tm_hour=12;time_t t=timegm(&d);struct tm g;gmtime_r(&t,&g);
    static const char *wd[]={"Sun","Mon","Tue","Wed","Thu","Fri","Sat"},*mo[]={"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
    snprintf(out,40,"%s, %02d %s %04d 12:00:00 GMT",wd[g.tm_wday],g.tm_mday,mo[g.tm_mon],g.tm_year+1900);
}
static void wa_ics_text(const char *prefix,const char *s){ /* one folded, escaped content line */
    char line[2048];size_t n=0;
    for(const char *p=prefix;*p&&n+1<sizeof(line);p++)line[n++]=*p;
    for(const char *p=s?s:"";*p&&n+3<sizeof(line);p++){if(*p=='\\'||*p==';'||*p==','){line[n++]='\\';line[n++]=*p;}else if(*p=='\n'){line[n++]='\\';line[n++]='n';}else if((unsigned char)*p>=32)line[n++]=*p;}
    line[n]=0;size_t col=0;
    for(size_t i=0;i<n;i++){if(col>=74&&((unsigned char)line[i]&0xC0)!=0x80){fputs("\r\n ",stdout);col=1;}fputc(line[i],stdout);col++;}
    fputs("\r\n",stdout);
}
static int serve_world_audio_public(const char *method,const char *target,const char *host){
    const char *rest=target+8,*slash=strchr(rest,'/');char space[65];
    if(!slash||slash==rest||(size_t)(slash-rest)>64)return 0;memcpy(space,rest,(size_t)(slash-rest));space[slash-rest]=0;
    if(strspn(space,"abcdefghijklmnopqrstuvwxyz0123456789-")!=strlen(space))return 0;
    const char *what=slash+1;int pos=0,kind=0;char tail[16];
    if(!strcmp(what,"feed.xml"))kind=1;else if(!strcmp(what,"schedule.ics"))kind=2;
    else if(sscanf(what,"chapters/%d%15s",&pos,tail)==2&&!strcmp(tail,".json"))kind=3;
    else if(sscanf(what,"transcript/%d%15s",&pos,tail)==2&&!strcmp(tail,".vtt"))kind=4;
    else return 0;
    if(kind>=3&&(pos<1||pos>AW_ITEM_LIMIT))return 0;
    char normalized[256];if(!lower_host(host,normalized)||host_is_territory_apex(host))return 0;
    sqlite3 *db=cms_open_ro();PhSite site;
    if(!db||!ph_site(db,normalized,NULL,&site)){if(db)sqlite3_close(db);return 0;}
    if(strcmp(method,"GET")&&strcmp(method,"HEAD")){sqlite3_close(db);aw_error(405,"Use GET.\n");return 1;}
    sqlite3_stmt *row=NULL;
    if(aw_public_space(db,site.id,space,&row)!=1){sqlite3_close(db);protocol_reply(404,"text/plain; charset=utf-8","There is no space at this address.\n");return 1;}
    char title[257],lede[601],state[16];sqlite3_int64 world=sqlite3_column_int64(row,6);
    snprintf(title,sizeof(title),"%s",wa_col(row,0));snprintf(lede,sizeof(lede),"%s",wa_col(row,4));snprintf(state,sizeof(state),"%s",wa_col(row,5));sqlite3_finalize(row);row=NULL;
    if(strcmp(state,"public")){sqlite3_close(db);protocol_reply(410,"text/plain; charset=utf-8","Its owner has taken this space down.\n");return 1;}
    WaSound snd;if(!wa_sound(db,NULL,site.id,world,space,&snd)){sqlite3_close(db);protocol_reply(404,"text/plain; charset=utf-8","This space is not a station or a show.\n");return 1;}
    typedef struct{int position;char ref[161],note[281];} Row;Row rows[AW_ITEM_LIMIT];int nr=0;
    if(aw_public_items(db,site.id,world,space,&row)==1)while(nr<AW_ITEM_LIMIT&&sqlite3_step(row)==SQLITE_ROW){rows[nr].position=sqlite3_column_int(row,0);
        snprintf(rows[nr].ref,sizeof(rows[nr].ref),"%s",wa_col(row,1));snprintf(rows[nr].note,sizeof(rows[nr].note),"%s",wa_col(row,3));nr++;}
    sqlite3_finalize(row);row=NULL;
    const char *hdr="HTTP/1.1 200 OK\r\nContent-Type: %s\r\nCache-Control: public, no-cache\r\nVary: Host\r\nX-Content-Type-Options: nosniff\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n";
    if(kind==2){
        WaSlot slots[AWA_SLOT_LIMIT];int n=wa_slots(db,NULL,site.id,world,space,slots);struct tm local;time_t now=time(NULL);
        if(!n||!wa_local(snd.timezone,now,&local)){sqlite3_close(db);protocol_reply(404,"text/plain; charset=utf-8","This space has no schedule.\n");return 1;}
        printf(hdr,"text/calendar; charset=utf-8");
        fputs("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Aurekai//Worlds schedule//EN\r\nCALSCALE:GREGORIAN\r\nMETHOD:PUBLISH\r\n",stdout);wa_ics_text("X-WR-CALNAME:",title);
        if(strcmp(snd.timezone,"UTC")){fputs("X-WR-TIMEZONE:",stdout);fputs(snd.timezone,stdout);fputs("\r\n",stdout);}
        struct tm g;gmtime_r(&now,&g);char stamp[24];strftime(stamp,sizeof(stamp),"%Y%m%dT%H%M%SZ",&g);
        int iso=local.tm_wday?local.tm_wday:7;
        for(int i=0;i<n;i++){
            struct tm d={0};d.tm_year=local.tm_year;d.tm_mon=local.tm_mon;d.tm_mday=local.tm_mday+(slots[i].weekday-iso);d.tm_hour=12;time_t t=timegm(&d);gmtime_r(&t,&d);
            char start[24];snprintf(start,sizeof(start),"%04d%02d%02dT%02d%02d00",d.tm_year+1900,d.tm_mon+1,d.tm_mday,slots[i].start/60,slots[i].start%60);
            printf("BEGIN:VEVENT\r\nUID:slot-%d-",slots[i].slot);fputs(space,stdout);fputs("@",stdout);fputs(site.host,stdout);printf("\r\nDTSTAMP:%s\r\n",stamp);
            if(!strcmp(snd.timezone,"UTC"))printf("DTSTART:%sZ\r\n",start);else{fputs("DTSTART;TZID=",stdout);fputs(snd.timezone,stdout);printf(":%s\r\n",start);}
            printf("DURATION:PT%dM\r\nRRULE:FREQ=WEEKLY\r\n",slots[i].minutes);wa_ics_text("SUMMARY:",slots[i].programme);
            if(slots[i].host[0]){char with[200];snprintf(with,sizeof(with),"With %s",slots[i].host);wa_ics_text("DESCRIPTION:",with);}
            char url[400];snprintf(url,sizeof(url),"https://%s/worlds/%s",site.host,space);wa_ics_text("URL:",url);fputs("END:VEVENT\r\n",stdout);
        }
        fputs("END:VCALENDAR\r\n",stdout);sqlite3_close(db);return 1;
    }
    PageMaterial *m=calloc(1,sizeof(*m));PageItem *it=calloc(1,sizeof(*it));char loaded[96]="";
    if(!m||!it){free(m);free(it);sqlite3_close(db);aw_error(503,"Unavailable.\n");return 1;}
    if(kind==3||kind==4){
        const char *ref=NULL;for(int i=0;i<nr;i++)if(rows[i].position==pos&&strncmp(rows[i].ref,"space:",6))ref=rows[i].ref;
        sqlite3_stmt *s=NULL;int any=0;
        if(!ref||awa_rows(db,NULL,site.id,world,space,AWA_MARKS,ref,&s)!=1){sqlite3_finalize(s);free(m);free(it);sqlite3_close(db);protocol_reply(404,"text/plain; charset=utf-8","Nothing here.\n");return 1;}
        bf_jsonw w;bf_jsonw_init(&w,0);
        if(kind==3){bf_jsonw_obj(&w);bf_jsonw_key(&w,"version");bf_jsonw_str(&w,"1.2.0");bf_jsonw_key(&w,"chapters");bf_jsonw_arr(&w);}
        else{printf(hdr,"text/vtt; charset=utf-8");fputs("WEBVTT\n",stdout);}
        while(sqlite3_step(s)==SQLITE_ROW){const char *k=wa_col(s,2);
            if(kind==3&&!strcmp(k,"chapter")){any=1;bf_jsonw_obj(&w);bf_jsonw_key(&w,"startTime");bf_jsonw_num(&w,sqlite3_column_int64(s,3)/1000.0);bf_jsonw_key(&w,"title");bf_jsonw_str(&w,wa_col(s,5));
                const char *h=wa_col(s,7);if(!strncmp(h,"https://",8)){bf_jsonw_key(&w,"url");bf_jsonw_str(&w,h);}bf_jsonw_obj_end(&w);}
            if(kind==4&&!strcmp(k,"cue")&&sqlite3_column_int(s,8)){any=1;long long a=sqlite3_column_int64(s,3),b=sqlite3_column_int64(s,4);
                printf("\n%02lld:%02lld:%02lld.%03lld --> %02lld:%02lld:%02lld.%03lld\n",a/3600000,a/60000%60,a/1000%60,a%1000,b/3600000,b/60000%60,b/1000%60,b%1000);
                const char *sp=wa_col(s,6);if(*sp){fputs("<v ",stdout);wa_xml(sp);fputs(">",stdout);}
                for(const char *p=wa_col(s,5);*p;p++){if(*p=='&')fputs("&amp;",stdout);else if(*p=='<')fputs("&lt;",stdout);else if(*p=='>')fputs("&gt;",stdout);else fputc(*p,stdout);}fputs("\n",stdout);}
        }
        sqlite3_finalize(s);
        if(kind==3){bf_jsonw_arr_end(&w);bf_jsonw_obj_end(&w);if(!any||w.oom)protocol_reply(404,"text/plain; charset=utf-8","No chapters.\n");
            else{printf(hdr,"application/json+chapters");fwrite(w.buf,1,w.len,stdout);}}
        bf_jsonw_free(&w);free(m);free(it);sqlite3_close(db);(void)any;return 1;
    }
    /* RSS: one item per dated episode whose recording its source states open terms for */
    printf(hdr,"application/rss+xml; charset=utf-8");
    char base[400];snprintf(base,sizeof(base),"https://%s/worlds/%s",site.host,space);
    fputs("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rss version=\"2.0\" xmlns:itunes=\"http://www.itunes.com/dtds/podcast-1.0.dtd\" xmlns:podcast=\"https://podcastindex.org/namespace/1.0\" xmlns:atom=\"http://www.w3.org/2005/Atom\">\n<channel>\n",stdout);
    fputs("<title>",stdout);wa_xml(title);fputs("</title>\n<link>",stdout);wa_xml(base);fputs("</link>\n<atom:link rel=\"self\" type=\"application/rss+xml\" href=\"",stdout);wa_xml(base);fputs("/feed.xml\"/>\n<description>",stdout);
    wa_xml(*lede?lede:title);fputs("</description>\n<language>",stdout);wa_xml(snd.language);fputs("</language>\n<generator>Aurekai Worlds</generator>\n<itunes:author>",stdout);wa_xml(snd.author);
    fputs("</itunes:author>\n<itunes:explicit>",stdout);fputs(snd.explicit_content?"true":"false",stdout);fputs("</itunes:explicit>\n<itunes:type>episodic</itunes:type>\n<itunes:category text=\"",stdout);wa_xml(snd.category);fputs("\"/>\n<podcast:medium>",stdout);
    fputs("podcast</podcast:medium>\n",stdout);
    typedef struct{int row;WaEpisode ep;} Pick;Pick picks[AW_ITEM_LIMIT];int np=0;
    for(int i=0;i<nr;i++){if(!strncmp(rows[i].ref,"space:",6))continue;WaEpisode ep=wa_episode(db,NULL,site.id,world,space,rows[i].ref);if(ep.found){picks[np].row=i;picks[np].ep=ep;np++;}}
    for(int i=1;i<np;i++)for(int j=i;j>0&&strcmp(picks[j].ep.published,picks[j-1].ep.published)>0;j--){Pick t=picks[j];picks[j]=picks[j-1];picks[j-1]=t;}
    for(int p=0;p<np;p++){
        Row *r=&rows[picks[p].row];WaEpisode *ep=&picks[p].ep;
        if(!world_web_resolve(db,r->ref,m,loaded,it)||!wa_open_audio(it))continue;
        char date[40];wa_rfc822(ep->published,date);
        fputs("<item>\n<title>",stdout);wa_xml(it->title);printf("</title>\n<link>%s#piece-%d</link>\n<guid isPermaLink=\"false\">",base,r->position);wa_xml(site.host);fputs("/worlds/",stdout);wa_xml(space);fputs("/",stdout);wa_xml(r->ref);
        printf("</guid>\n<pubDate>%s</pubDate>\n<description>",date);
        char desc[2400];snprintf(desc,sizeof(desc),"%s%s%s%s%s",r->note,*r->note&&it->body[0]?" ":"",it->body,it->rights[0]?" Terms: ":"",it->rights);wa_xml(desc);
        fputs("</description>\n<enclosure url=\"",stdout);wa_xml(it->asset);fputs("\" length=\"0\" type=\"",stdout);fputs(wa_mime(it->asset),stdout);fputs("\"/>\n",stdout);
        if(ep->season)printf("<itunes:season>%d</itunes:season>\n",ep->season);if(ep->number)printf("<itunes:episode>%d</itunes:episode>\n",ep->number);
        if(it->duration_ms>0)printf("<itunes:duration>%ld</itunes:duration>\n",it->duration_ms/1000);
        sqlite3_stmt *s=NULL;int chapters=0,cues=0;
        if(awa_rows(db,NULL,site.id,world,space,AWA_MARKS,r->ref,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){const char *k=wa_col(s,2);
            if(!strcmp(k,"chapter"))chapters++;
            else if(!strcmp(k,"cue")&&sqlite3_column_int(s,8))cues++;
            else if(!strcmp(k,"person")){const char *role=wa_col(s,6),*h=wa_col(s,7);fputs("<podcast:person",stdout);
                if(!strcasecmp(role,"guest")||!strcasecmp(role,"host")||!strcasecmp(role,"producer")||!strcasecmp(role,"editor")||!strcasecmp(role,"composer")||!strcasecmp(role,"narrator")){fputs(" role=\"",stdout);for(const char *q=role;*q;q++)fputc(tolower((unsigned char)*q),stdout);fputs("\"",stdout);}
                if(!strncmp(h,"https://",8)){fputs(" href=\"",stdout);wa_xml(h);fputs("\"",stdout);}fputs(">",stdout);wa_xml(wa_col(s,5));fputs("</podcast:person>\n",stdout);}
            else if(!strcmp(k,"clip")&&sqlite3_column_int(s,8)){long long a=sqlite3_column_int64(s,3),b=sqlite3_column_int64(s,4);
                printf("<podcast:soundbite startTime=\"%lld.%03lld\" duration=\"%lld.%03lld\">",a/1000,a%1000,(b-a)/1000,(b-a)%1000);wa_xml(wa_col(s,5));fputs("</podcast:soundbite>\n",stdout);}
        }
        sqlite3_finalize(s);
        if(chapters)printf("<podcast:chapters url=\"%s/chapters/%d.json\" type=\"application/json+chapters\"/>\n",base,r->position);
        if(cues)printf("<podcast:transcript url=\"%s/transcript/%d.vtt\" type=\"text/vtt\"/>\n",base,r->position);
        fputs("</item>\n",stdout);
    }
    fputs("</channel>\n</rss>\n",stdout);
    free(m);free(it);sqlite3_close(db);return 1;
}
#endif
