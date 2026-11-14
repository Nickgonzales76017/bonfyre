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
static const char bf_ps_css[]="<style>"
".ph-storyboard{display:grid;gap:24px;margin:18px 0 42px;color:#24392f}"
".ph-storyboard *{box-sizing:border-box}"
".ph-storyboard :is(h2,h3){font-family:Georgia,serif;letter-spacing:-.035em;line-height:1.13;margin:6px 0 14px}"
".ph-storyboard .ps-eyebrow{display:block;font:750 11px/1.3 system-ui;letter-spacing:.15em;text-transform:uppercase;opacity:.75}"
".ph-storyboard .ps-hero{display:flex;flex-direction:column;justify-content:center;gap:12px;padding:clamp(28px,5vw,56px);min-height:280px;border-radius:20px;background:#1f4337;color:#f2f0e6}"
".ph-storyboard .ps-hero h2{font-size:clamp(2.5rem,5.5vw,5rem);max-width:20ch}.ph-storyboard .ps-hero p{font-size:clamp(17px,1.7vw,22px);max-width:64ch}"
".ph-storyboard .ps-group{border:1px solid #9ba99e;border-radius:20px;padding:clamp(20px,3vw,34px);background:#f1eee6}"
".ph-storyboard .ps-group[data-theme=night],.ph-storyboard .ps-group[data-theme=forest]{background:#203b33;color:#f5f0e6}"
".ph-storyboard .ps-group>header{border-bottom:1px solid currentColor;padding-bottom:18px;margin-bottom:18px}"
".ph-storyboard .ps-group h2{font-size:clamp(27px,3.6vw,48px)}.ph-storyboard .ps-group>header p{max-width:75ch;opacity:.8}"
".ph-storyboard .ps-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:15px}"
".ph-storyboard .ps-card{min-width:0;display:flex;flex-direction:column;gap:8px;background:#fffdf7;color:#24392f;border:1px solid #b7c6b7;border-radius:13px;padding:24px}"
".ph-storyboard .ps-card[data-theme=forest],.ph-storyboard .ps-card[data-theme=night]{background:#244a3d;color:#f9f3e8;border-color:#486758}"
".ph-storyboard .ps-card[data-theme=rust]{background:#f2dfd2;color:#553226;border-color:#d8b8a7}"
".ph-storyboard .ps-card[data-theme=wide],.ph-storyboard .ps-card[data-theme=editorial]{grid-column:1/-1}"
".ph-storyboard .ps-card h3{font-size:clamp(22px,2.4vw,33px)}.ph-storyboard p{margin:0;line-height:1.7;white-space:pre-line}"
".ph-storyboard .ps-quote{background:#eae7db;padding:28px;border-left:5px solid #aa5c3f;border-radius:0 16px 16px 0}"
".ph-storyboard .ps-quote h2{font-size:clamp(22px,3vw,35px)}"
".ph-storyboard a{display:inline-flex;align-self:start;margin-top:auto;min-height:44px;align-items:center;color:inherit;font:750 13px system-ui;text-underline-offset:5px}"
".ph-storyboard a:focus-visible{outline:3px solid #c3653e;outline-offset:3px}"
"@media(max-width:760px){.ph-storyboard .ps-grid{grid-template-columns:1fr}.ph-storyboard .ps-card[data-theme=wide]{grid-column:auto}}"
".ph-storyboard{gap:0;max-width:1280px;margin:0 auto 50px;background:transparent}"
".ph-storyboard .ps-group{border:0;border-bottom:1px solid #d5ded6;border-radius:0;padding:clamp(28px,5vw,74px) clamp(16px,4vw,60px);background:transparent}"
".ph-storyboard .ps-card{border:0;border-top:1px solid #cbd5cd;border-radius:0;background:transparent;padding:clamp(17px,2vw,30px) 0}"
".ph-storyboard .ps-hero{min-height:clamp(340px,46vw,620px);border-radius:0;padding:clamp(42px,7vw,100px) clamp(20px,5vw,85px)}"
".ph-storyboard .ps-card h3{font-weight:500;letter-spacing:-.04em}.ph-storyboard .ps-card p{max-width:65ch}"
".ph-storyboard .ps-quote{border-radius:0;background:#f3eee6;padding:clamp(25px,4vw,45px)}"
"@media(max-width:760px){.ph-storyboard .ps-group{padding:40px 16px}.ph-storyboard .ps-grid{gap:0}.ph-storyboard .ps-card{padding:20px 0}}"
"</style>";

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
    fputs(bf_ps_css,output);fwrite(body,1,bytes,output);free(body);
    return count;
}
#endif
