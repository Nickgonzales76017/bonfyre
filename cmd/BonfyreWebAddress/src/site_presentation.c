#include "site_presentation.h"
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

static const struct { const char *key, *css; } roles[] = {
    {"ground","--bf-ground"},{"ink","--bf-ink"},{"display","--bf-display-ink"},
    {"accent","--bf-accent"},{"muted","--bf-muted"},{"rule","--bf-rule"},{"panel","--bf-panel"},
    {"measure","--bf-measure"},{"gap","--bf-gap"},{"radius","--bf-radius"},
    {"section_space","--bf-section-space"},{"face_display","--bf-face-display"},
    {"face_body","--bf-face-body"},{"face_mono","--bf-face-mono"},
    {"display_weight","--bf-display-weight"},{"display_transform","--bf-display-transform"},
    {"display_tracking","--bf-display-tracking"},{"display_leading","--bf-display-leading"},
    {"display_size","--bf-display-size"},{"card_border","--bf-card-border"},{"card_shadow","--bf-card-shadow"}
};
static const struct {const char *field, *values;} modes[] = {
    {"hero_mode","|lead|headline-stack|oversized-title|quiet-title|masthead|sectioned|none|"},
    {"navigation_mode","|header|rail|inline|minimal|sidebar|"},
    {"density","|standard|tight|loud|airy|regular|"},
    {"card_mode","|borderless|ruled-row|soft-panel|column-rule|soft-card|list-row|"},
    {"typography_mode","|mixed|condensed-sans|display-grotesque|optical-serif|high-contrast-serif|modern-sans-serif-pair|uniform-sans|"}
};
static int role_index(const char *key) {
    for(size_t i=0;i<sizeof(roles)/sizeof(roles[0]);i++) if(!strcmp(key,roles[i].key)) return (int)i;
    return -1;
}
static int mode_index(const char *key) {
    for(size_t i=0;i<sizeof(modes)/sizeof(modes[0]);i++) if(!strcmp(key,modes[i].field)) return (int)i;
    return -1;
}
static int mode_safe(int i, const char *value) {
    char needle[96];
    if(!value || !*value || strlen(value)>80 || strchr(value,'|')) return 0;
    snprintf(needle,sizeof(needle),"|%s|",value);
    return strstr(modes[i].values,needle)!=NULL;
}
static int css_safe(const char *value) {
    if(!value || !*value || strlen(value)>255) return 0;
    for(const unsigned char *p=(const unsigned char *)value;*p;p++)
        if(!isalnum(*p) && !strchr("#%(),./ +-\"'",*p)) return 0;
    return 1;
}
static int font_safe(const char *value) {
    const char *prefix="https://fonts.googleapis.com/css2?";
    if(!value || strncmp(value,prefix,strlen(prefix)) || strlen(value)>1024) return 0;
    for(const unsigned char *p=(const unsigned char *)value+strlen(prefix);*p;p++)
        if(!isalnum(*p) && !strchr("=:+,;@.&%_-",*p)) return 0;
    return 1;
}
static void escaped(FILE *out,const char *s) {
    for(;s && *s;s++) {
        if(*s=='&') fputs("&amp;",out); else if(*s=='\"') fputs("&quot;",out);
        else if(*s=='<') fputs("&lt;",out); else if(*s=='>') fputs("&gt;",out); else fputc(*s,out);
    }
}
static int tokens_valid(sqlite3 *db,const char *json,int strict) {
    sqlite3_stmt *st=NULL;int valid=1,rc;
    if(sqlite3_prepare_v2(db,"SELECT key,value,type FROM json_each(?1)",-1,&st,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_text(st,1,json,-1,SQLITE_STATIC);
    while((rc=sqlite3_step(st))==SQLITE_ROW) {
        const char *key=(const char *)sqlite3_column_text(st,0), *v=(const char *)sqlite3_column_text(st,1), *type=(const char *)sqlite3_column_text(st,2);
        int index=role_index(key),font=!strcmp(key,"font_href");
        if(index<0 && !font) { if(strict) valid=0; continue; }
        if(strcmp(type,"text") || !v || strlen(v)!=(size_t)sqlite3_column_bytes(st,1) || !(font?font_safe(v):css_safe(v))) valid=0;
    }
    sqlite3_finalize(st);return valid && rc==SQLITE_DONE;
}
int bf_site_presentation_validate(sqlite3 *db,const char *json,const char **why) {
    sqlite3_stmt *st=NULL;int valid=0,count=0,rc;
    *why="presentation must be a JSON object";
    if(!json || strstr(json,"\\u0000")) return 0;
    sqlite3_prepare_v2(db,"SELECT CASE WHEN json_valid(?1) THEN json_type(?1)='object' ELSE 0 END",-1,&st,NULL);
    sqlite3_bind_text(st,1,json,-1,SQLITE_STATIC);
    valid=sqlite3_step(st)==SQLITE_ROW && sqlite3_column_int(st,0);sqlite3_finalize(st);
    if(!valid) return 0;
    sqlite3_prepare_v2(db,"SELECT key,value,type FROM json_each(?1)",-1,&st,NULL);
    sqlite3_bind_text(st,1,json,-1,SQLITE_STATIC);
    while((rc=sqlite3_step(st))==SQLITE_ROW) {
        const char *key=(const char *)sqlite3_column_text(st,0), *v=(const char *)sqlite3_column_text(st,1), *type=(const char *)sqlite3_column_text(st,2);
        count++;
        if(!strcmp(key,"layout_tokens")) {
            if(strcmp(type,"object") || !tokens_valid(db,v,1)) {valid=0;*why="unsupported or unsafe layout token";break;}
        } else {
            int i=mode_index(key);
            if(i<0 || strcmp(type,"text") || !mode_safe(i,v)) {valid=0;*why="unsupported presentation field or mode";break;}
        }
    }
    sqlite3_finalize(st);return valid && count && rc==SQLITE_DONE;
}
static sqlite3_stmt *selected(sqlite3 *db,const char *profile,const char *cms_namespace) {
    sqlite3_stmt *st=NULL;char alternate[256]="";const char *bare=profile;
    const char *scoped =
      "SELECT layout_tokens,hero_mode,navigation_mode,density,card_mode,typography_mode FROM presentation_template "
      "WHERE namespace=?4 AND status='published' AND (template=?1 OR template=?2 OR template=?3) "
      "AND json_valid(layout_tokens) AND json_type(layout_tokens)='object' "
      "ORDER BY CASE template WHEN ?1 THEN 0 WHEN ?2 THEN 1 ELSE 2 END,id DESC LIMIT 1";
    const char *historical =
      "SELECT layout_tokens,hero_mode,navigation_mode,density,card_mode,typography_mode FROM presentation_template "
      "WHERE status='published' AND (template=?1 OR template=?2 OR template=?3) "
      "AND json_valid(layout_tokens) AND json_type(layout_tokens)='object' "
      "ORDER BY CASE template WHEN ?1 THEN 0 WHEN ?2 THEN 1 ELSE 2 END,id DESC LIMIT 1";
    if(!profile || !*profile) return NULL;
    if(!strncmp(profile,"ghost:",6)) {bare=profile+6;snprintf(alternate,sizeof(alternate),"ghost-%s",bare);}
    if(sqlite3_prepare_v2(db,cms_namespace&&*cms_namespace?scoped:historical,-1,&st,NULL)!=SQLITE_OK) return NULL;
    sqlite3_bind_text(st,1,profile,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,alternate,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,bare,-1,SQLITE_STATIC);
    if(cms_namespace&&*cms_namespace) sqlite3_bind_text(st,4,cms_namespace,-1,SQLITE_STATIC);
    if(sqlite3_step(st)!=SQLITE_ROW || !tokens_valid(db,(const char *)sqlite3_column_text(st,0),0)) {
        sqlite3_finalize(st);return NULL;
    }
    return st;
}
/* Tokens are scoped by the caller: member sites use .site-composition, territory
 * pages body[data-bf-template]. One emitter, one record shape, no per-surface copy. */
static int bf_site_presentation_emit_with_namespace(FILE *out,sqlite3 *db,const char *profile,
                                                    const char *selector,const char *cms_namespace) {
    sqlite3_stmt *st=selected(db,profile,cms_namespace),*token=NULL;
    if(!st || !selector || !*selector || strpbrk(selector,"{}<>;")) {if(st) sqlite3_finalize(st); return 0;}
    sqlite3_prepare_v2(db,"SELECT key,value FROM json_each(?1)",-1,&token,NULL);
    sqlite3_bind_text(token,1,(const char *)sqlite3_column_text(st,0),-1,SQLITE_STATIC);
    fprintf(out,"<style data-bf-source=\"presentation_template\">%s{",selector);
    while(sqlite3_step(token)==SQLITE_ROW) {
        const char *key=(const char *)sqlite3_column_text(token,0),*v=(const char *)sqlite3_column_text(token,1);
        int i=role_index(key);if(i>=0) fprintf(out,"%s:%s;",roles[i].css,v);
    }
    fputs("}</style>",out);sqlite3_reset(token);
    while(sqlite3_step(token)==SQLITE_ROW) if(!strcmp((const char *)sqlite3_column_text(token,0),"font_href")) {
        fputs("<link rel=\"stylesheet\" data-bf-fonts href=\"",out);escaped(out,(const char *)sqlite3_column_text(token,1));fputs("\">",out);
    }
    sqlite3_finalize(token);sqlite3_finalize(st);return 1;
}
int bf_site_presentation_emit_scoped(FILE *out,sqlite3 *db,const char *profile,const char *selector) {
    return bf_site_presentation_emit_with_namespace(out,db,profile,selector,NULL);
}
int bf_site_presentation_emit_scoped_owned(FILE *out,sqlite3 *db,const char *profile,
                                           const char *selector,const char *cms_namespace) {
    if (!cms_namespace || !*cms_namespace) return 0; /* explicit owner required */
    return bf_site_presentation_emit_with_namespace(out,db,profile,selector,cms_namespace);
}
void bf_site_presentation_emit(FILE *out,sqlite3 *db,const char *profile) {
    bf_site_presentation_emit_scoped(out,db,profile,".site-composition");
}
void bf_site_presentation_attributes(FILE *out,sqlite3 *db,const char *profile) {
    sqlite3_stmt *st=selected(db,profile,NULL);if(!st) return;
    fputs(" data-template=\"",out);escaped(out,profile);fputs("\"",out);
    for(size_t i=0;i<sizeof(modes)/sizeof(modes[0]);i++) {
        const char *v=(const char *)sqlite3_column_text(st,(int)i+1);
        if(mode_safe((int)i,v)) fprintf(out," data-%s=\"%s\"",modes[i].field,v);
    }
    sqlite3_finalize(st);
}
