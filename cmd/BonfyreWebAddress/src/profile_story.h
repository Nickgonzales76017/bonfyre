#ifndef BONFYRE_PROFILE_STORY_H
#define BONFYRE_PROFILE_STORY_H
/* Public profile story composition: CMS supplies every section, title, link
 * and variant. No saved items, private drafts, accounts or source bodies are
 * automatically published. The site_presentation.site_id is the only scope. */
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void bf_ps_escape(FILE *f,const char *s) {
    for(const unsigned char *p=(const unsigned char *)(s?s:"");*p;p++)
        switch(*p) {
            case '&':fputs("&amp;",f);break; case '<':fputs("&lt;",f);break;
            case '>':fputs("&gt;",f);break; case '"':fputs("&quot;",f);break;
            case 39:fputs("&#39;",f);break; default:fputc(*p,f);
        }
}
static int bf_ps_url(const char *s) {
    if(!s||strncmp(s,"https://",8)||!s[8]||strlen(s)>1800)return 0;
    const char *h=s+8,*end=strpbrk(h,"/?#");if(!end)end=h+strlen(h);
    if(end==h||!memchr(h,'.',(size_t)(end-h))||memchr(h,'@',(size_t)(end-h)))return 0;
    for(const unsigned char *p=(const unsigned char *)s;*p;p++)
        if(*p<=32||*p==127||*p=='<'||*p=='>'||*p=='"'||*p==39||*p=='\\')return 0;
    return 1;
}
static const char *bf_ps_theme(const char *s) {
    static const char *const styles[]={"forest","paper","night","rust","wide","slate","editorial"};
    if(s)for(size_t i=0;i<sizeof(styles)/sizeof(*styles);i++)
        if(!strcmp(s,styles[i]))return styles[i];
    return "paper";
}

/* Returns published block count; -1 on a failed read. No partial HTML escapes
 * a failed SQLite read. Max 80 blocks and 5 KB body per block. */
static int bf_profile_story_emit(FILE *output,sqlite3 *db,const char *site_id) {
    if(!output||!db||!site_id||!*site_id||strlen(site_id)>127)return -1;
    const char *sql=
        "SELECT kind,COALESCE(eyebrow,''),COALESCE(heading,''),COALESCE(body,''),"
        "COALESCE(label,''),COALESCE(href,''),COALESCE(variant_key,'')"
        " FROM site_fragment WHERE namespace=?1 AND page='/' AND slot='showcase'"
        " AND audience='public' AND status='published' AND position BETWEEN 0 AND 999999"
        " AND COALESCE(json_extract(projection_contract,'$.html'),0) != 1"
        " AND kind IN ('profile-hero','profile-section','profile-card','profile-story',"
        "'profile-quote','profile-metric','profile-action')"
        " AND length(COALESCE(heading,''))<=220 AND length(COALESCE(body,''))<=5000"
        " ORDER BY position,id LIMIT 81";
    sqlite3_stmt *st=NULL;
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(st,1,site_id,-1,SQLITE_TRANSIENT);
    char *body=NULL;size_t bytes=0;FILE *f=open_memstream(&body,&bytes);
    if(!f){sqlite3_finalize(st);return -1;}
    fputs("<div class=\"ph-storyboard\" data-profile-storyboard aria-label=\"Published projects and stories\">",f);
    int count=0,opened=0,step=SQLITE_DONE;
    while((step=sqlite3_step(st))==SQLITE_ROW) {
        if(count==80){step=SQLITE_DONE;break;}
        const char *kind=(const char *)sqlite3_column_text(st,0);
        const char *eyebrow=(const char *)sqlite3_column_text(st,1);
        const char *heading=(const char *)sqlite3_column_text(st,2);
        const char *copy=(const char *)sqlite3_column_text(st,3);
        const char *label=(const char *)sqlite3_column_text(st,4);
        const char *href=(const char *)sqlite3_column_text(st,5);
        const char *theme=bf_ps_theme((const char *)sqlite3_column_text(st,6));
        int hero=!strcmp(kind,"profile-hero"),section=!strcmp(kind,"profile-section");
        int quote=!strcmp(kind,"profile-quote")||!strcmp(kind,"profile-action");
        if(hero||section||quote){if(opened){fputs("</div></section>",f);opened=0;}}
        if(section){
            fputs("<section class=\"ps-group\" data-theme=\"",f);bf_ps_escape(f,theme);
            fputs("\"><header>",f);
            if(*eyebrow){fputs("<span class=\"ps-eyebrow\">",f);bf_ps_escape(f,eyebrow);fputs("</span>",f);}
            fputs("<h2>",f);bf_ps_escape(f,heading);fputs("</h2><p>",f);
            bf_ps_escape(f,copy);fputs("</p></header><div class=\"ps-grid\">",f);opened=1;
        }else{
            if(!hero&&!quote&&!opened){fputs("<section class=\"ps-group\"><header><h2>Selected work</h2></header><div class=\"ps-grid\">",f);opened=1;}
            fputs(hero?"<section class=\"ps-hero\"":quote?"<aside class=\"ps-quote\"":"<article class=\"ps-card\"",f);
            fputs(" data-theme=\"",f);bf_ps_escape(f,theme);fputs("\">",f);
            if(*eyebrow){fputs("<span class=\"ps-eyebrow\">",f);bf_ps_escape(f,eyebrow);fputs("</span>",f);}
            fputs(hero||quote?"<h2>":"<h3>",f);bf_ps_escape(f,heading);fputs(hero||quote?"</h2>":"</h3>",f);
            if(*copy){fputs("<p>",f);bf_ps_escape(f,copy);fputs("</p>",f);}
            if(*label&&bf_ps_url(href)){fputs("<a rel=\"noopener noreferrer\" href=\"",f);bf_ps_escape(f,href);fputs("\">",f);bf_ps_escape(f,label);fputs(" ↗</a>",f);}
            fputs(hero?"</section>":quote?"</aside>":"</article>",f);
        }
        count++;
    }
    if(opened)fputs("</div></section>",f);
    fputs("</div>",f);
    int closed=fclose(f);sqlite3_finalize(st);
    if(closed||step!=SQLITE_DONE||!count){free(body);return closed||step!=SQLITE_DONE?-1:0;}
    fwrite(body,1,bytes,output);free(body);
    return count;
}
#endif
