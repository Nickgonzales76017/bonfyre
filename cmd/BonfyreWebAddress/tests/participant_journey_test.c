/* Local rendering/HTTP-adapter harness. Auth identity and estate chrome are
 * explicit fixture adapters; full native Auth and the four public HTTPS sites
 * must still be checked by participant_journey_browser.test.mjs on the Mac. */
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <openssl/sha.h>
static void bf_sha256_hex(const unsigned char *p,size_t n,char out[65]){
    unsigned char d[32];SHA256(p,n,d);for(size_t i=0;i<32;i++)snprintf(out+2*i,3,"%02x",d[i]);
}
static void html(const char *s){for(const unsigned char *p=(const unsigned char*)(s?s:"");*p;p++){
    if(*p=='&')fputs("&amp;",stdout);else if(*p=='<')fputs("&lt;",stdout);else if(*p=='>')fputs("&gt;",stdout);
    else if(*p=='\"')fputs("&quot;",stdout);else if(*p=='\'')fputs("&#39;",stdout);else putchar(*p);
}}
/* The one shell (app_shell.h), as a fixture adapter: a page writes only its <main>. */
static void ak_app_open(const char *title,const char *pattern,const char *route_class){
    fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>",stdout);html(title);
    fputs(" \xc2\xb7 Aurekai</title></head><body class=\"ak-app ",stdout);html(route_class);fputs("\" data-aurekai-pattern=\"",stdout);html(pattern);
    fputs("\"><nav aria-label=\"Fixture navigation\"><a href=\"/profile\">Profile</a></nav><main class=\"ak-wb\">",stdout);
}
static void ak_app_close(void){fputs("</main></body></html>",stdout);}
static void url_query_value(const char *s){for(const unsigned char *p=(const unsigned char*)(s?s:"");*p;p++){
    if((*p>='a'&&*p<='z')||(*p>='A'&&*p<='Z')||(*p>='0'&&*p<='9')||strchr("-._~",*p))putchar(*p);else printf("%%%02X",*p);
}}
static int public_http_url(const char *s){return s&&(!strncmp(s,"http://",7)||!strncmp(s,"https://",8));}
static void protocol_reply(int code,const char *type,const char *body){printf("HTTP/1.1 %d Fixture\r\nContent-Type: %s\r\nCache-Control: no-store\r\n\r\n%s",code,type,body);}
static void page_redirect(const char *path){printf("HTTP/1.1 303 See Other\r\nLocation: %s\r\nContent-Length: 0\r\nCache-Control: no-store\r\n\r\n",path);}
static int wants_html(const char *a){return a&&strstr(a,"text/html");}
static int page_session_actor(const char *token,const char *unused,char *actor,size_t cap){
    (void)unused;actor[0]=0;
    if(token&&!strcmp(token,"fixture-user-1"))snprintf(actor,cap,"user:1");
    if(token&&!strcmp(token,"fixture-user-2"))snprintf(actor,cap,"user:2");
    return !!actor[0];
}
static sqlite3 *open_store(int mode){sqlite3 *db=NULL;const char *p=getenv("PJ_FIXTURE_DB");if(!p||sqlite3_open_v2(p,&db,mode,NULL)!=SQLITE_OK){sqlite3_close(db);return NULL;}sqlite3_busy_timeout(db,3000);return db;}
static sqlite3 *cms_open_rw(void){return open_store(SQLITE_OPEN_READWRITE);}
static sqlite3 *cms_open_ro(void){return open_store(SQLITE_OPEN_READONLY);}
#include "../src/participant_state.h"
#include "../src/participant_journey.h"
int main(int argc,char **argv){
    if(argc==2&&!strcmp(argv[1],"--headers")){
        const char *valid="POST /participation/change HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/x-www-form-urlencoded";
        assert(pj_same_origin(valid,"aurekai.work"));
        assert(pj_same_origin("POST / HTTP/1.1\ncontent-type: application/x-www-form-urlencoded\norigin: https://aurekai.work","aurekai.work"));
        assert(!pj_same_origin("POST / HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/x-www-form-urlencoded\r\nOrigin: https://aurekai.work","aurekai.work"));
        assert(!pj_same_origin("POST / HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Type: application/x-www-form-urlencoded","aurekai.work"));
        assert(!pj_same_origin("POST / HTTP/1.1\r\nOrigin: https://attacker.test\r\nContent-Type: application/x-www-form-urlencoded","aurekai.work"));
        puts("5 native-framed header checks passed");return 0;
    }

    if(argc<8){fprintf(stderr,"method target query body host fixture-token origin\n");return 2;}
    char raw[2048];const char *type=getenv("PJ_CONTENT_TYPE");if(!type)type="application/x-www-form-urlencoded";
    snprintf(raw,sizeof(raw),"%s %s HTTP/1.1\r\nHost: %s\r\n%s%s%sContent-Type: %s\r\n%s\r\n",argv[1],argv[2],argv[5],argv[7][0]?"Origin: ":"",argv[7],argv[7][0]?"\r\n":"",type,getenv("PJ_EXTRA_HEADERS")?getenv("PJ_EXTRA_HEADERS"):"");
    /* Match native readiness: the header/body separator is NUL-terminated. */
    char *framed=strstr(raw,"\r\n\r\n");if(framed)*framed=0;
    if(!strcmp(argv[2],"/profile")){
        char actor[64],kind[16]="occurrence",q[160]="",cursor[24]="",site[128]="",tab[24]="activity";
        PjField fields[]={{"kind",kind,sizeof(kind)},{"q",q,sizeof(q)},{"cursor",cursor,sizeof(cursor)},{"site",site,sizeof(site)},{"tab",tab,sizeof(tab)}};
        if(!page_session_actor(argv[6],NULL,actor,sizeof(actor))){if(!pj_profile_login_redirect(argv[3]))protocol_reply(400,"text/plain","Invalid profile continuation");return 0;}
        if(!pj_fields(argv[3],fields,5,2048)||!pj_kind(kind)){protocol_reply(400,"text/plain","Bad list query");return 0;}
        sqlite3 *db=cms_open_ro();fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\n\r\n<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>My activity · Local fixture</title></head><body><main class=\"pj-page\">",stdout);
        pj_profile_list(db,!strcmp(actor,"user:1")?1:2,site,pj_kind(kind),q,*cursor?strtoll(cursor,NULL,10):0);
        fputs("</main></body></html>",stdout);sqlite3_close(db);return 0;
    }
    if(pj_legacy(argv[1],argv[2],argv[4],raw,argv[5],argv[6],"fixture-auth"))return 0;
    const char *accept=getenv("PJ_ACCEPT");if(!accept)accept="text/html";
    if(serve_participant_journey(argv[1],argv[2],argv[3],argv[4],raw,argv[5],argv[6],"fixture-auth",accept))return 0;
    protocol_reply(404,"text/plain","Fixture route not found");return 0;
}
