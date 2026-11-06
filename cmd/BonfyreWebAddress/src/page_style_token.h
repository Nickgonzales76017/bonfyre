#ifndef BONFYRE_PAGE_STYLE_TOKEN_H
#define BONFYRE_PAGE_STYLE_TOKEN_H
#include <sqlite3.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
/* Exact top-level native field, never substring selection or truncated CSS. */
static int page_style_value(sqlite3 *db,const char *tokens,const char *key,char *out,size_t cap) {
    sqlite3_stmt *st=NULL;
    char path[80];int ok=0;
    if(!out||!cap)return 0;
    out[0]=0;
    if(!db||!tokens||!key||!key[0])return 0;
    /* Some deployed SQLite builds truncate decoded NUL during extraction.
     * Reject that escape before extraction, respecting escaped backslashes. */
    for(const char *p=tokens;*p;p++) {
        if(*p!='\\')continue;
        if(!strncmp(p+1,"u0000",5))return 0;
        if(p[1])p++;
    }
    for(const char *p=key;*p;p++)if(!((*p>='a'&&*p<='z')||(*p>='0'&&*p<='9')||*p=='_'))return 0;
    if(snprintf(path,sizeof(path),"$.%s",key)>=(int)sizeof(path))return 0;
    if(sqlite3_prepare_v2(db,"SELECT CASE WHEN json_valid(?1) THEN (SELECT CASE WHEN count(*)=1 AND json_type(?1,?2) IN ('text','integer','real') THEN json_extract(?1,?2) END FROM json_each(?1) WHERE key=?3) END",-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,tokens,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,path,-1,SQLITE_STATIC);sqlite3_bind_text(st,3,key,-1,SQLITE_STATIC);
    if(sqlite3_step(st)==SQLITE_ROW) {
        int type=sqlite3_column_type(st,0);
        const unsigned char *value=sqlite3_column_text(st,0);
        int bytes=sqlite3_column_bytes(st,0);
        if((type!=SQLITE_TEXT&&type!=SQLITE_INTEGER&&type!=SQLITE_FLOAT)||!value||bytes<=0||(size_t)bytes>=cap)goto done;
        memcpy(out,value,(size_t)bytes);out[bytes]=0;
        if(strlen(out)!=(size_t)bytes)goto done;
        for(const unsigned char *p=(const unsigned char *)out;*p;p++)if(*p<32||*p==127)goto done;
        if(strpbrk(out,"<>\\{}"))goto done;
        if(!strcmp(key,"font_href")) {
            if(strncmp(out,"https://fonts.googleapis.com/css2?",34)||strpbrk(out,"\"' "))goto done;
        } else {
            if(strchr(out,';'))goto done;
            for(const char *p=out;*p;p++)if(!strncasecmp(p,"url(",4)||!strncasecmp(p,"expression(",11)||!strncmp(p,"/*",2))goto done;
        }
        ok=1;
    }
done:
    sqlite3_finalize(st);
    if(!ok)out[0]=0;
    return ok;
}
#endif
