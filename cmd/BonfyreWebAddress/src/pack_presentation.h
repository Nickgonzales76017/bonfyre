#ifndef BONFYRE_PACK_PRESENTATION_H
#define BONFYRE_PACK_PRESENTATION_H
#include "pack_icon_map.h"
/* Presentation is read from the existing published PackGraph demands. It owns
 * no application state, installation, permission, source truth or preview runtime. */
static int pack_presentation_asset(const char *url) {
    const char *prefix="/apps/material/";size_t n=strlen(prefix);
    if(!url||strncmp(url,prefix,n)||strlen(url)>510||strlen(url)<n+73)return 0;
    if(strspn(url+n,"abcdef0123456789")!=64||strncmp(url+n+64,"/assets/",8))return 0;
    const char *p=url+n+72;if(!*p||strspn(p,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._/")!=strlen(p)||strstr(p,"..")||strstr(p,"//"))return 0;
    const char *ext=strrchr(p,'.');return ext&&(!strcmp(ext,".png")||!strcmp(ext,".jpg")||!strcmp(ext,".webp"));
}
static void pack_presentation_open(sqlite3 *db,const char *pack,const char *label,const char *purpose,const char *kind,int installed) {
    const char *glyph=bf_pack_icon(pack,kind);
    fputs("<article class=\"pack-panel pack-tool\"",stdout);
    if(installed)fputs(" data-installed-workspace",stdout);
    fputs(" data-pack=\"",stdout);html(pack);fputs("\" data-pack-icon=\"",stdout);html(glyph);fputs("\"><div class=\"pack-art\">",stdout);
    sqlite3_stmt *st=NULL;int count=0;
    const char *sql="SELECT d.needs,COALESCE(d.preferred_sources,'Product preview') FROM pack_demand d WHERE d.namespace='root' AND d.status='published' AND d.demand_kind='preview-media' AND d.fields='image' AND (d.pack=?1 OR d.pack IN (SELECT depends_on FROM pack_dependency WHERE pack=?1 AND namespace='root' AND status='published')) ORDER BY d.pack<>?1,d.id LIMIT 4";
    if(db&&sqlite3_prepare_v2(db,sql,-1,&st,NULL)==SQLITE_OK){
        sqlite3_bind_text(st,1,pack,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(st)==SQLITE_ROW){const char *url=(const char*)sqlite3_column_text(st,0);if(!pack_presentation_asset(url))continue;
            fputs("<img class=\"pack-preview",stdout);if(count)fputs(" pack-preview-alt",stdout);fputs("\" src=\"",stdout);html(url);
            fputs("\" alt=\"",stdout);html((const char*)sqlite3_column_text(st,1));
            fputs("\" loading=\"lazy\" decoding=\"async\" referrerpolicy=\"no-referrer\" width=\"1440\" height=\"950\">",stdout);if(++count==2)break;
        }
    }
    sqlite3_finalize(st);
    if(!count){fputs("<div class=\"pack-art-type\" aria-hidden=\"true\"><span>AUREKAI</span><strong>",stdout);html(label);fputs("</strong></div>",stdout);}
    fputs("<span class=\"pack-kind\">",stdout);html(kind);fputs("</span></div><div class=\"pack-copy\"><span class=\"pack-glyph\" aria-hidden=\"true\">",stdout);page_glyph(glyph);fputs("</span><h3>",stdout);html(label);fputs("</h3>",stdout);
    if(purpose&&*purpose){fputs("<p class=\"pack-purpose\">",stdout);html(purpose);fputs("</p>",stdout);}
    fputs("<div class=\"pack-card-actions\">",stdout);
}
static void pack_presentation_close(void){fputs("</div></div></article>",stdout);}
static void pack_presentation_style(void){
    /* A CMS-authored Stitch pattern must reach the real account UI, not only
     * leave an inert data attribute on the body. Owner components stay intact. */
    fputs("<link rel=\"stylesheet\" href=\"/assets/pack-presentation.css\">",stdout);
}
#endif
