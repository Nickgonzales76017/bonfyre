#ifndef BONFYRE_PAGE_MEDIA_SPACES_H
#define BONFYRE_PAGE_MEDIA_SPACES_H
/* A public World release is the one account-owned source of truth for its
 * cinema, audio station and announced screenings. These cards are a read-only
 * discovery projection, not imported films, new memberships or media rights. */
static int page_media_space_host(const char *host) {
    if (!host) return 0;
    size_t n=strlen(host);
    if(n<15||n>250||strspn(host,"abcdefghijklmnopqrstuvwxyz0123456789-.")!=n||
       !strstr(host,".aurekai."))return 0;
    static const char *suffix[]={".aurekai.work",".aurekai.life",".aurekai.live",".aurekai.shop"};
    for(size_t i=0;i<sizeof(suffix)/sizeof(suffix[0]);i++){
        size_t size=strlen(suffix[i]);
        if(n>size&&!strcmp(host+n-size,suffix[i]))return 1;
    }
    return 0;
}
static int page_media_space_slug(const char *value,size_t cap){
    if(!value)return 0;
    size_t n=strlen(value);
    return n>0&&n<cap&&strspn(value,"abcdefghijklmnopqrstuvwxyz0123456789-")==n;
}
/* Defined by the existing page archive owner after core renderers. */
static int page_archive_item(sqlite3 *,const char *,const char *,PageItem *,char *,size_t);
/* Source coordinates are retained item identities, not foreign media URLs. */
static int page_media_item_ref(const char *ref,char source[128],char key[25]){
    const char *slash=ref?strchr(ref,'/'):NULL;
    if(!slash||strchr(slash+1,'/')||slash==ref||(size_t)(slash-ref)>=128||strlen(slash+1)!=24)return 0;
    memcpy(source,ref,(size_t)(slash-ref));source[slash-ref]=0;snprintf(key,25,"%s",slash+1);
    return strspn(source,"abcdefghijklmnopqrstuvwxyz0123456789-_.")==strlen(source)&&
           strspn(key,"0123456789abcdef")==24;
}
static void page_media_screening_record(sqlite3 *archive,const char *ref){
    char source[128],key[25],family[64]="";
    if(!page_media_item_ref(ref,source,key))return;
    PageItem item={0};
    int have=archive&&page_archive_item(archive,source,key,&item,family,sizeof(family))&&item.title[0];
    fputs("<p class=\"media-space-film\"><a data-media-source href=\"/o/",stdout);
    page_url_escape(source);ui_lit(stdout, "/");page_url_escape(key);ui_lit(stdout, "\">");
    if(have)html(item.title);else ui_lit(stdout, "Open retained film item");
    ui_close_n(stdout, "a", "p", NULL);
}
static void page_media_space_link(const char *host,const char *space,const char *screening){
    ui_lit(stdout, "https://");html(host);ui_lit(stdout, "/worlds/");html(space);
    if(screening&&*screening){ui_lit(stdout, "#screening-");html(screening);}
}
static void page_media_space_controls(int events){
    (ui_open(stdout, "div", "media-space-controls"), ui_open(stdout, "label", NULL), ui_lit(stdout, "Find "));
    fputs(events?"a screening or venue":"a published space",stdout);
    (ui_elem(stdout, "input", (const UiAttr[]){{"type", "search"}, {"maxlength", "120"}, {"data-media-space-search", NULL}, {"placeholder", "Search this page"}}, 4, 0), ui_close(stdout, "label"));
    if(events)(ui_elem(stdout, "div", (const UiAttr[]){{"class", "media-space-modes"}, {"role", "group"}, {"aria-label", "Screening dates"}}, 3, 0), ui_elem(stdout, "button", (const UiAttr[]){{"type", "button"}, {"data-media-space-filter", "all"}, {"aria-pressed", "true"}}, 3, 0), ui_lit(stdout, "All dates"), ui_close(stdout, "button"), ui_elem(stdout, "button", (const UiAttr[]){{"type", "button"}, {"data-media-space-filter", "weekend"}, {"aria-pressed", "false"}}, 3, 0), ui_lit(stdout, "Weekend"), ui_close(stdout, "button"), ui_close(stdout, "div"));
    (ui_elem(stdout, "output", (const UiAttr[]){{"role", "status"}, {"aria-live", "polite"}, {"data-media-space-count", NULL}}, 3, 0), ui_close(stdout, "output"), ui_close(stdout, "div"));
}
/* The control operates only on already rendered public cards; no network,
 * account selection, installation or CMS mutation follows from filtering. */
static void page_media_space_script(void){
    fputs("<script>(()=>{const section=document.currentScript?.previousElementSibling;"
          "if(!section?.matches('[data-media-space-catalogue]'))return;"
          "const input=section.querySelector('[data-media-space-search]'),"
          "count=section.querySelector('[data-media-space-count]'),"
          "cards=[...section.querySelectorAll('[data-media-space-card]')],"
          "buttons=[...section.querySelectorAll('[data-media-space-filter]')];"
          "let mode='all';function draw(){let shown=0;const term=input.value.trim().toLocaleLowerCase();"
          "for(const card of cards){const ok=(mode==='all'||card.dataset.weekend==='1')"
          "&&(!term||card.textContent.toLocaleLowerCase().includes(term));"
          "card.hidden=!ok;if(ok)shown++;}"
          "for(const button of buttons)button.setAttribute('aria-pressed',String(button.dataset.mediaSpaceFilter===mode));"
          "count.textContent='Showing '+shown+' of '+cards.length+' published records on this page.';}"
          "input.addEventListener('input',draw);"
          "for(const b of buttons)b.addEventListener('click',()=>{mode=b.dataset.mediaSpaceFilter;draw()});"
          "draw()})();</script>",stdout);
}
static int page_emit_media_spaces(sqlite3 *db,const char *route,const char *query,const char *host){
    if(!db||!route||!host||strcmp(host,"aurekai.live"))return 0;
    int events=!strcmp(route,"/whats-on");
    int films=!strcmp(route,"/watch")||!strcmp(route,"/film");
    int sounds=!strcmp(route,"/listen")||!strcmp(route,"/music");
    if(!events&&!films&&!sounds)return 0;
    char from[12]="";unsigned page=0;
    if(query&&*query)query_value(query,"spaces-page",from,sizeof(from));
    if(*from&&strlen(from)<=4&&strspn(from,"0123456789")==strlen(from)){
        page=(unsigned)atoi(from);if(page>1000)page=0;
    }
    /* All joins pin the same published account site, World and release;
     * a draft, withdrawn World, unrelated account or unsupported host cannot
     * turn up in the public cross-creator catalogue. */
    const char *joins=
        " FROM account_world_release r "
        "JOIN account_world_binding b ON b.world_id=r.world_id AND b.namespace=r.namespace "
        "JOIN world w ON w.id=b.world_id AND w.namespace=b.namespace AND w.subject=b.subject "
        "AND w.status IN('draft','published') "
        "JOIN site_presentation p ON p.site_id=r.namespace AND p.namespace='root' "
        "AND p.status='published' AND p.profile='member-profile-v1' ";
    char sql[3300];sqlite3_stmt *rows=NULL;int count=0;
    const char *where=events?
        "JOIN account_world_screening s ON s.world_id=r.world_id AND s.space_id=r.space_id "
        "AND s.state='announced' AND datetime(s.starts)>=datetime('now') "
        "WHERE r.state='public' AND r.medium IN('film','cinema','movie') "
        "ORDER BY s.starts,p.host,r.space_id,s.screening_id LIMIT 25 OFFSET ?1":
        films?"WHERE r.state='public' AND r.medium IN('film','cinema','movie') "
        "ORDER BY p.host,r.space_id LIMIT 25 OFFSET ?1":
        "WHERE r.state='public' AND r.medium IN('radio','music','podcast','audio') "
        "ORDER BY p.host,r.space_id LIMIT 25 OFFSET ?1";
    int length=snprintf(sql,sizeof(sql),"%s%s%s",
        events?"SELECT r.space_id,r.title,r.lede,p.host,p.site_id,r.medium,s.screening_id,s.starts,s.venue,s.ref ":
               "SELECT r.space_id,r.title,r.lede,p.host,p.site_id,r.medium,'','','','' ",
        joins,where);
    if(length<=0||(size_t)length>=sizeof(sql)||
       sqlite3_prepare_v2(db,sql,-1,&rows,NULL)!=SQLITE_OK){sqlite3_finalize(rows);return 0;}
    sqlite3_bind_int64(rows,1,(sqlite3_int64)page*25);
    sqlite3 *archive=events?page_archive_open(0):NULL;
    while(sqlite3_step(rows)==SQLITE_ROW){
        const char *space=(const char*)sqlite3_column_text(rows,0),
                   *title=(const char*)sqlite3_column_text(rows,1),
                   *lede=(const char*)sqlite3_column_text(rows,2),
                   *publisher=(const char*)sqlite3_column_text(rows,3),
                   *site=(const char*)sqlite3_column_text(rows,4),
                   *medium=(const char*)sqlite3_column_text(rows,5),
                   *screening=(const char*)sqlite3_column_text(rows,6),
                   *starts=(const char*)sqlite3_column_text(rows,7),
                   *venue=(const char*)sqlite3_column_text(rows,8),
                   *film_ref=(const char*)sqlite3_column_text(rows,9);
        if(!page_media_space_host(publisher)||!page_media_space_slug(space,65)||
           !site||!*site||!title||!*title||(events&&(!page_media_space_slug(screening,65)||!starts||!*starts)))continue;
        if(!count++){
            (ui_elem(stdout, "section", (const UiAttr[]){{"class", "media-space-catalogue"}, {"data-media-space-catalogue", NULL}, {"aria-label", "Published account media spaces"}}, 3, 0), ui_open(stdout, "h2", "media-space-heading"));
            page_glyph(events?"calendar":films?"play":"listen");
            fputs(events?"Screenings from the commons":films?"Cinema from the commons":"Listen in the commons",stdout);
            (ui_close(stdout, "h2"), ui_open(stdout, "p", NULL), ui_lit(stdout, "Published spaces kept by their own accounts. Each space opens at its owner's address; playback, licences and club access are checked there."), ui_close(stdout, "p"));
            page_media_space_controls(events);
            ui_open(stdout, "ol", "media-space-cards");
        }
        int weekend=0;
        if(events&&starts&&strlen(starts)>=10){
            int y=0,m=0,d=0;
            if(sscanf(starts,"%4d-%2d-%2d",&y,&m,&d)==3&&y>=1970&&y<=2200&&m>=1&&m<=12&&d>=1&&d<=31){
                struct tm stamp={0};stamp.tm_year=y-1900;stamp.tm_mon=m-1;stamp.tm_mday=d;stamp.tm_isdst=-1;
                time_t when=mktime(&stamp);if(when!=(time_t)-1){struct tm check={0};if(localtime_r(&when,&check)&&check.tm_year==y-1900&&check.tm_mon==m-1&&check.tm_mday==d)weekend=check.tm_wday==0||check.tm_wday==6;}
            }
        }
        fputs("<li data-media-space-card data-weekend=\"",stdout);fputs(weekend?"1":"0",stdout);
        (ui_lit(stdout, "\">"), ui_open(stdout, "small", NULL));html(events?"Announced screening":medium);
        ui_lit(stdout, " &middot; ");html(publisher);fputs("</small><h3><a href=\"",stdout);
        page_media_space_link(publisher,space,events?screening:NULL);
        ui_lit(stdout, "\">");html(title);ui_close_n(stdout, "a", "h3", NULL);
        if(events){
            fputs("<p><time datetime=\"",stdout);html(starts);ui_lit(stdout, "\">");html(starts);ui_close(stdout, "time");
            if(venue&&*venue){ui_lit(stdout, " &middot; ");html(venue);}ui_close(stdout, "p");
        }else if(lede&&*lede){ui_text(stdout, "p", NULL, lede);}
        if(events)page_media_screening_record(archive,film_ref);
        fputs("<p><a href=\"",stdout);page_media_space_link(publisher,space,NULL);
        fputs("\">Open published space</a> &middot; <a href=\"https://",stdout);html(publisher);
        (ui_lit(stdout, "/\">Owner's public profile"), ui_close(stdout, "a"), ui_close(stdout, "p"), ui_close(stdout, "li"));
    }
    sqlite3_finalize(rows);
    if(archive)sqlite3_close(archive);
    if(count){
        ui_close(stdout, "ol");
        if(count==25&&page<1000)printf("<p class=\"media-space-next\"><a href=\"%s?spaces-page=%u\" rel=\"next\">More published spaces</a></p>",route,page+1);
        if(page)printf("<p><a href=\"%s?spaces-page=%u\" rel=\"prev\">Previous spaces</a></p>",route,page-1);
        ui_close(stdout, "section");
        page_media_space_script();
    }
    return count;
}
/* ------------------------------------------------------------ media screens
 *
 * Listen, Watch, Film, Music, Stage, Sports and Art each read as one screen of
 * the shared shell (BF_ESTATE_BAR above, BF_ESTATE_FOOT and its one player
 * below), composed from the material the route program already collected.
 * Every recording, episode, track and article read aloud is a data-ak-track
 * element; the shared player is the only player. Layout lives in
 * site/matrix/composition.css under .ms. Nothing here invents a count, a
 * price, a right or an audience: the numbers shown are counts of the items on
 * this page, and an empty route says so in the screen's own layout. */
enum { MS_NONE, MS_LISTEN, MS_MUSIC, MS_CINEMA, MS_ART };
#define MS_TAKE 96
typedef struct { const PageItem *item[MS_TAKE]; int count; } MsList;

static int ms_mode(const char *host,const char *route){
    if(!host||!route)return MS_NONE;
    int live=!strcmp(host,"aurekai.live"),work=!strcmp(host,"aurekai.work");
    if(!live&&!work)return MS_NONE;
    if(!strcmp(route,"/listen"))return MS_LISTEN;
    if(!strcmp(route,"/watch")||!strcmp(route,"/stage"))return MS_CINEMA;
    if(!live)return MS_NONE;
    if(!strcmp(route,"/music"))return MS_MUSIC;
    if(!strcmp(route,"/film")||!strcmp(route,"/sports"))return MS_CINEMA;
    if(!strcmp(route,"/art"))return MS_ART;
    return MS_NONE;
}
static int ms_has(const MsList *l,const PageItem *it){
    for(int i=0;i<l->count;i++){
        if(l->item[i]==it)return 1;
        if(it->key[0]&&!strcmp(l->item[i]->key,it->key))return 1;
        if(!strcasecmp(l->item[i]->title,it->title))return 1;
    }
    return 0;
}
static void ms_take(MsList *l,const PageMaterial *m,int kind,int want_image){
    const PageStream *s=&m->stream[kind];
    for(int k=s->cursor;k<s->count&&l->count<MS_TAKE;k++){
        const PageItem *it=&s->item[k];
        if(!it->title[0]||ms_has(l,it))continue;
        if(want_image&&!it->image[0]&&!it->poster[0])continue;
        l->item[l->count++]=it;
    }
}
static int ms_playable(const PageItem *it){
    return it&&it->asset[0]&&public_http_url(it->asset)&&!strpbrk(it->asset," \"'<>\\");
}
static const char *ms_picture(const PageItem *it){ return it->image[0]?it->image:it->poster; }
static void ms_img(const PageItem *it,int width,const char *cls){
    char sized[1024];
    page_picture_at(ms_picture(it),width,sized,sizeof(sized));
    if(page_image_url_safe(sized))page_emit_picture(sized,cls);
    else{fputs("<span class=\"",stdout);html(cls);ui_lit(stdout, " ms-noart\" aria-hidden=\"true\">");page_glyph(it->kind==BF_PAGE_MATERIAL_VIDEO?"play":it->kind==BF_PAGE_MATERIAL_AUDIO?"listen":"discover");ui_close(stdout, "span");}
}
static const char *ms_from(const PageItem *it){ return it->note[0]?it->note:it->group[0]?it->group:it->by; }
/* The track attributes the shared player reads. A recording plays its file;
 * an article is read aloud from its own text block on this page. */
static void ms_track(const PageItem *it,int dflt){
    if(ms_playable(it)){
        ui_lit(stdout, " data-ak-track data-src=\"");html(it->asset);
    }else if(it->kind==BF_PAGE_MATERIAL_PUBLICATION&&it->key[0]&&it->body[0]){
        ui_lit(stdout, " data-ak-track data-narrate=\"#ms-n-");html(it->key);
    }else return;
    ui_lit(stdout, "\" data-title=\"");html(it->title);
    ui_lit(stdout, "\" data-by=\"");html(it->by[0]?it->by:ms_from(it));ui_lit(stdout, "\"");
    if(dflt)ui_lit(stdout, " data-ak-default");
}
static int ms_hearable(const PageItem *it){
    return ms_playable(it)||(it->kind==BF_PAGE_MATERIAL_PUBLICATION&&it->key[0]&&it->body[0]);
}
static void ms_play_button(const PageItem *it,const char *label,int dflt){
    if(!ms_hearable(it))return;
    fputs("<button type=\"button\" class=\"ms-play\" data-ak-queue=\"listen\"",stdout);ms_track(it,dflt);
    ui_lit(stdout, " aria-label=\"");html(label);ui_lit(stdout, ": ");html(it->title);ui_lit(stdout, "\">");
    page_glyph("play");ui_close(stdout, "button");
}
/* Saving is one tap where the thing is heard: the form posts to the one
 * saved-item owner (/o/save, or /o/unsave when it is already in the library)
 * and comes back here. Signed out, that owner sends the person to sign in
 * and back. Nothing is published by saving. */
static int page_session_actor(const char *bearer,const char *auth_db,char *actor,size_t cap);
static sqlite3 *ms_saved_db;static char ms_actor[64];
static void ms_saved_open(void){
    ms_actor[0]=0;ms_saved_db=NULL;
    if(page_session_actor(page_request_bearer,page_request_auth_db,ms_actor,sizeof(ms_actor)))ms_saved_db=cms_open_ro();
}
static void ms_saved_close(void){if(ms_saved_db)sqlite3_close(ms_saved_db);ms_saved_db=NULL;ms_actor[0]=0;}
static int ms_is_saved(const char *ref){
    sqlite3_stmt *s=NULL;int found=0;
    if(!ms_saved_db||!ms_actor[0]||sqlite3_prepare_v2(ms_saved_db,"SELECT 1 FROM saved_item WHERE actor=?1 AND item_ref=?2 AND status='published' LIMIT 1",-1,&s,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(s,1,ms_actor,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,ref,-1,SQLITE_STATIC);
    found=sqlite3_step(s)==SQLITE_ROW;sqlite3_finalize(s);return found;
}
static void ms_keep(const PageItem *it,const char *back,int compact){
    char ref[200];
    if(!page_item_has_page(it)||snprintf(ref,sizeof(ref),"%s/%s",it->source,it->key)>=(int)sizeof(ref))return;
    int saved=ms_is_saved(ref);
    printf("<form class=\"ms-keep\" method=\"post\" action=\"/o/%s\"><input type=\"hidden\" name=\"item\" value=\"",saved?"unsave":"save");
    html(ref);fputs("\"><input type=\"hidden\" name=\"return\" value=\"",stdout);html(back&&*back=='/'?back:"/");
    printf("\"><button type=\"submit\" class=\"%s\" aria-pressed=\"%s\"",compact?"ms-icon":"ms-ghost",saved?"true":"false");
    if(compact){ui_lit(stdout, " aria-label=\"");fputs(saved?"In your library: ":"Save to library: ",stdout);html(it->title);ui_lit(stdout, "\">");page_glyph(saved?"check":"save");}
    else{ui_lit(stdout, ">");page_glyph(saved?"check":"save");fputs(saved?"In your library":"Save to library",stdout);}
    ui_close_n(stdout, "button", "form", NULL);
}
/* Making starts from what was heard: the person's own Audio Studio. */
static void ms_make(void){ui_elem(stdout, "a", (const UiAttr[]){{"class", "ms-ghost"}, {"href", "/apps/audio"}}, 2, 0);page_glyph("edit");(ui_lit(stdout, "Make with it"), ui_close(stdout, "a"));}
static void ms_narration_text(const PageItem *it){
    if(it->kind!=BF_PAGE_MATERIAL_PUBLICATION||!it->key[0]||!it->body[0]||ms_playable(it))return;
    (ui_open_a(stdout, "div", "class", "ms-narration", "hidden", NULL, UI_DYN, "id", "ms-n-", it->key), ui_open(stdout, "h3", NULL));
    html(it->title);(ui_close(stdout, "h3"), ui_open(stdout, "p", NULL));html(it->body);ui_close_n(stdout, "p", "div", NULL);
}
static void ms_excerpt(const PageItem *it,const char *cls){
    if(!it->body[0])return;
    ui_open_a(stdout, "p", "class", cls);html(it->body);ui_close(stdout, "p");
}
static void ms_when(const PageItem *it,time_t now){
    char when[48];page_when(it->at,now,when,sizeof(when));
    if(when[0]){ui_text(stdout, "time", NULL, when);}
}
static void ms_dur(const PageItem *it){
    if(it->duration_ms<=0)return;
    ui_open(stdout, "span", "ms-dur");page_emit_duration(it->duration_ms);ui_close(stdout, "span");
}
static void ms_title_link(const PageItem *it){
    const char *close=page_emit_item_open(it,NULL);html(it->title);fputs(close,stdout);
}
/* A waveform drawn from the item's own key: decoration, never a measurement. */
static void ms_wave(const PageItem *it,int bars){
    unsigned h=2166136261u;const char *k=it->key[0]?it->key:it->title;
    ui_elem(stdout, "span", (const UiAttr[]){{"class", "ms-wave"}, {"aria-hidden", "true"}}, 2, 0);
    for(int i=0;i<bars;i++){
        h^=(unsigned char)k[i%(strlen(k)?strlen(k):1)];h*=16777619u;
        printf("<i style=\"--h:%u%%\"></i>",18+(h>>8)%80);
    }
    ui_close(stdout, "span");
}
static void ms_query_link(const char *route,const char *key,const char *value){
    fputs(route,stdout);ui_lit(stdout, "?");fputs(key,stdout);ui_lit(stdout, "=");page_url_escape(value);
}
/* Distinct publishers on this page, as links that narrow the route. */
static void ms_chips(const MsList *l,const char *route,const char *from,const char *all_label,int limit){
    const char *seen[16];int n=0;
    fputs("<nav class=\"ms-chips\" aria-label=\"Narrow by publisher\"><a href=\"",stdout);html(route);ui_lit(stdout, "\"");
    if(!from||!*from)ui_lit(stdout, " aria-current=\"page\"");
    ui_lit(stdout, ">");html(all_label);ui_close(stdout, "a");
    for(int i=0;i<l->count&&n<limit&&n<16;i++){
        const char *name=ms_from(l->item[i]);int dup=0;
        if(!name[0])continue;
        for(int j=0;j<n;j++)if(!strcasecmp(seen[j],name))dup=1;
        if(dup)continue;seen[n++]=name;
        fputs("<a href=\"",stdout);ms_query_link(route,"from",name);ui_lit(stdout, "\"");
        if(from&&*from&&!strcasecmp(from,name))ui_lit(stdout, " aria-current=\"page\"");
        ui_lit(stdout, ">");html(name);ui_close(stdout, "a");
    }
    ui_close(stdout, "nav");
}
static int ms_publishers(const MsList *l){
    int n=0;
    for(int i=0;i<l->count;i++){
        int dup=0;const char *a=ms_from(l->item[i]);
        for(int j=0;j<i&&!dup;j++)if(!strcasecmp(a,ms_from(l->item[j])))dup=1;
        if(!dup&&a[0])n++;
    }
    return n;
}
static void ms_narrowed(const char *route,const char *from,const char *q){
    if((!from||!*from)&&(!q||!*q))return;
    (ui_open(stdout, "p", "ms-narrowed"), ui_lit(stdout, "Showing "));
    if(q&&*q){(ui_lit(stdout, "matches for "), ui_open(stdout, "b", NULL));html(q);(ui_close(stdout, "b"), ui_lit(stdout, " "));}
    if(from&&*from){(ui_lit(stdout, "from "), ui_open(stdout, "b", NULL));html(from);(ui_close(stdout, "b"), ui_lit(stdout, " "));}
    fputs("&middot; <a href=\"",stdout);html(route);(ui_lit(stdout, "\">Show all"), ui_close(stdout, "a"), ui_close(stdout, "p"));
}
static void ms_empty(const char *what){
    ui_text(stdout, "p", "ms-empty", what);
}
static void ms_section_head(const char *kicker,const char *title,const char *aside){
    (ui_open(stdout, "header", "ms-sh"), ui_open(stdout, "div", NULL), ui_open(stdout, "p", "ms-kicker"));html(kicker);
    (ui_close(stdout, "p"), ui_open(stdout, "h2", NULL));html(title);ui_close_n(stdout, "h2", "div", NULL);
    if(aside&&*aside){ui_text(stdout, "p", "ms-aside", aside);}
    ui_close(stdout, "header");
}

/* Listen: the narration and episode queue. */
static void ms_listen(const MsList *l,const char *route,const char *title,const char *lede,
                      const char *from,const char *q,const char *host,time_t now){
    long total_ms=0;int narrated=0,recordings=0;
    for(int i=0;i<l->count;i++){
        if(l->item[i]->duration_ms>0)total_ms+=l->item[i]->duration_ms;
        if(ms_playable(l->item[i]))recordings++;else if(ms_hearable(l->item[i]))narrated++;
    }
    const PageItem *lead=NULL;
    for(int i=0;i<l->count&&i<12&&!lead;i++)if(ms_playable(l->item[i])&&ms_picture(l->item[i])[0])lead=l->item[i];
    for(int i=0;i<l->count&&!lead;i++)if(ms_playable(l->item[i]))lead=l->item[i];
    if(!lead&&l->count)lead=l->item[0];
    (ui_open(stdout, "header", "ms-head"), ui_open(stdout, "p", "ms-kicker"), ui_open(stdout, "i", "ms-dot"), ui_close(stdout, "i"));
    fputs(strcmp(host,"aurekai.work")?"Live &middot; listening queue":"Work &middot; listening queue",stdout);
    (ui_close(stdout, "p"), ui_open(stdout, "h1", NULL));html(title);ui_close(stdout, "h1");
    if(lede&&*lede){ui_text(stdout, "p", "ms-lede", lede);}
    printf("<ul class=\"ms-stats\"><li><b>%d</b> publishers</li><li><b>%d</b> recordings</li>",ms_publishers(l),recordings);
    if(narrated)printf("<li><b>%d</b> read aloud</li>",narrated);
    if(total_ms>=60000)printf("<li><b>%ld</b> hours to hear</li>",total_ms/3600000+(total_ms%3600000>=1800000));
    ui_close(stdout, "ul");
    ms_narrowed(route,from,q);
    ms_chips(l,route,from,"All audio",6);
    ui_close(stdout, "header");
    if(!lead){ms_empty("Nothing to hear here yet. Recordings and articles appear as their publishers release them.");return;}
    /* Now playing: what the shared player holds on arrival. */
    (ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-now"}, {"aria-label", "Now playing"}}, 2, 0), ui_open(stdout, "div", "ms-now-art"));
    ms_img(lead,640,"ms-cover");
    (ui_open(stdout, "p", "ms-badge"), ui_open(stdout, "i", "ms-dot"), ui_close(stdout, "i"), ui_lit(stdout, "Up first"), ui_close(stdout, "p"), ui_open(stdout, "p", "ms-now-src"), ui_open(stdout, "small", NULL), ui_lit(stdout, "From"), ui_close(stdout, "small"), ui_open(stdout, "b", NULL));
    html(ms_from(lead));(ui_close(stdout, "b"), ui_close(stdout, "p"), ui_close(stdout, "div"), ui_open(stdout, "div", "ms-now-body"), ui_open(stdout, "p", "ms-meta"));
    if(lead->group[0]){ui_text(stdout, "span", "ms-tag", lead->group);}
    ms_when(lead,now);ms_dur(lead);(ui_close(stdout, "p"), ui_open(stdout, "h2", NULL));ms_title_link(lead);ui_close(stdout, "h2");
    if(lead->by[0]){ui_text(stdout, "p", "ms-by", lead->by);}
    ms_excerpt(lead,"ms-excerpt");
    ms_wave(lead,64);
    ui_open(stdout, "div", "ms-controls");
    ms_play_button(lead,"Play",1);
    if(lead->transcript_href[0]&&public_http_url(lead->transcript_href)){
        ui_open_a(stdout, "a", "class", "ms-ghost", "rel", "noopener", "href", lead->transcript_href);page_glyph("text");(ui_lit(stdout, "Transcript"), ui_close(stdout, "a"));
    }
    ms_keep(lead,route,0);
    if(lead->by[0]&&lead->source[0]){
        fputs("<a class=\"ms-ghost\" href=\"/people?who=",stdout);page_url_escape(lead->by);ui_lit(stdout, "&amp;source=");page_url_escape(lead->source);
        ui_lit(stdout, "\">");page_glyph("group");ui_lit(stdout, "More from ");html(lead->by);ui_close(stdout, "a");
    }
    ui_close_n(stdout, "div", "div", "section", NULL);
    ms_narration_text(lead);
    /* The queue: every row plays through the one player. */
    int rows=0;
    for(int i=0;i<l->count;i++)if(l->item[i]!=lead&&ms_hearable(l->item[i]))rows++;
    if(rows){
        char aside[64];snprintf(aside,sizeof(aside),"%d in the queue",rows);
        ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-queue"}, {"aria-label", "Queue"}}, 2, 0);
        ms_section_head("Up next","Episodes, recordings and articles read aloud",aside);
        ui_open(stdout, "ol", "ms-rows");
        int shown=0;
        for(int i=0;i<l->count&&shown<40;i++){
            const PageItem *it=l->item[i];
            if(it==lead||!ms_hearable(it))continue;shown++;
            ui_open(stdout, "li", NULL);ms_play_button(it,ms_playable(it)?"Play":"Read aloud",0);
            (ui_open(stdout, "div", "ms-row-main"), ui_open(stdout, "p", "ms-row-src"), ui_open(stdout, "b", NULL));html(ms_from(it));ui_close(stdout, "b");
            if(it->by[0]&&strcmp(it->by,ms_from(it))){ui_lit(stdout, " &middot; ");html(it->by);}
            if(!ms_playable(it))(ui_lit(stdout, " &middot; "), ui_open(stdout, "em", NULL), ui_lit(stdout, "read aloud"), ui_close(stdout, "em"));
            (ui_close(stdout, "p"), ui_open(stdout, "h3", NULL));ms_title_link(it);ui_close(stdout, "h3");ms_excerpt(it,"ms-row-text");
            (ui_close(stdout, "div"), ui_open(stdout, "p", "ms-row-meta"));ms_dur(it);ms_when(it,now);ui_close(stdout, "p");
            ms_keep(it,route,1);
            if(public_http_url(it->href)){
                fputs("<a class=\"ms-icon\" rel=\"noopener\" target=\"_blank\" href=\"",stdout);html(it->href);
                ui_lit(stdout, "\" aria-label=\"Open at the publisher\">");page_glyph("out");ui_close(stdout, "a");
            }
            ms_narration_text(it);
            ui_close(stdout, "li");
        }
        ui_close_n(stdout, "ol", "section", NULL);
    }
    /* Longest listens: two wide cards for the recordings that run longest. */
    const PageItem *longest[2]={NULL,NULL};
    for(int i=0;i<l->count;i++){
        const PageItem *it=l->item[i];
        if(it==lead||!ms_playable(it)||it->duration_ms<20*60000)continue;
        if(!longest[0]||it->duration_ms>longest[0]->duration_ms){longest[1]=longest[0];longest[0]=it;}
        else if(!longest[1]||it->duration_ms>longest[1]->duration_ms)longest[1]=it;
    }
    if(longest[0]){
        ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-long"}, {"aria-label", "Long listens"}}, 2, 0);
        ms_section_head("For a long sit","The longest recordings here",NULL);
        ui_open(stdout, "div", "ms-long-grid");
        for(int i=0;i<2&&longest[i];i++){
            const PageItem *it=longest[i];
            (ui_open(stdout, "article", NULL), ui_open(stdout, "p", "ms-meta"), ui_open(stdout, "span", "ms-tag"));html(ms_from(it));ui_close(stdout, "span");
            ms_when(it,now);(ui_close(stdout, "p"), ui_open(stdout, "h3", NULL));ms_title_link(it);ui_close(stdout, "h3");ms_excerpt(it,"ms-long-text");
            ui_open(stdout, "footer", NULL);ms_wave(it,24);ms_dur(it);
            fputs("<button type=\"button\" class=\"ms-pill\"",stdout);ms_track(it,0);ui_lit(stdout, ">");page_glyph("play");(ui_lit(stdout, "Play"), ui_close(stdout, "button"), ui_close(stdout, "footer"), ui_close(stdout, "article"));
        }
        ui_close_n(stdout, "div", "section", NULL);
    }
}

/* Music: library, the record on the turntable, its tracks, and stations. */
static void ms_music(const MsList *l,const char *route,const char *title,const char *lede,
                     const char *from,const char *q,time_t now){
    const PageItem *lead=NULL;
    for(int i=0;i<l->count&&!lead;i++)if(ms_playable(l->item[i])&&ms_picture(l->item[i])[0])lead=l->item[i];
    for(int i=0;i<l->count&&!lead;i++)if(ms_playable(l->item[i]))lead=l->item[i];
    if(!lead&&l->count)lead=l->item[0];
    (ui_open(stdout, "header", "ms-head ms-head-slim"), ui_open(stdout, "p", "ms-kicker"), ui_open(stdout, "i", "ms-dot"), ui_close(stdout, "i"), ui_lit(stdout, "Live &middot; music"), ui_close(stdout, "p"), ui_open(stdout, "h1", NULL));
    html(title);ui_close(stdout, "h1");
    if(lede&&*lede){ui_text(stdout, "p", "ms-lede", lede);}
    ms_narrowed(route,from,q);ui_close(stdout, "header");
    if(!lead){ms_empty("No music has been released here yet.");return;}
    (ui_open(stdout, "div", "ms-music-grid"), ui_elem(stdout, "aside", (const UiAttr[]){{"class", "ms-library"}, {"aria-label", "Library"}}, 2, 0), ui_open(stdout, "section", NULL), ui_open(stdout, "p", "ms-kicker"), ui_lit(stdout, "Library"), ui_close(stdout, "p"), ui_open(stdout, "ul", NULL));
    {
        const char *seen[12];int counts[12],n=0;
        for(int i=0;i<l->count;i++){
            const char *name=ms_from(l->item[i]);int at=-1;if(!name[0])continue;
            for(int j=0;j<n;j++)if(!strcasecmp(seen[j],name))at=j;
            if(at>=0)counts[at]++;else if(n<12){seen[n]=name;counts[n++]=1;}
        }
        fputs("<li><a href=\"",stdout);html(route);ui_lit(stdout, "\"");if(!from||!*from)ui_lit(stdout, " aria-current=\"page\"");
        printf(">All music<span>%d</span></a></li>",l->count);
        for(int j=0;j<n;j++){
            fputs("<li><a href=\"",stdout);ms_query_link(route,"from",seen[j]);ui_lit(stdout, "\"");
            if(from&&*from&&!strcasecmp(from,seen[j]))ui_lit(stdout, " aria-current=\"page\"");
            ui_lit(stdout, ">");html(seen[j]);printf("<span>%d</span></a></li>",counts[j]);
        }
    }
    (ui_close(stdout, "ul"), ui_close(stdout, "section"), ui_open(stdout, "section", NULL), ui_open(stdout, "p", "ms-kicker"), ui_lit(stdout, "Yours"), ui_close(stdout, "p"), ui_open(stdout, "ul", NULL), ui_open(stdout, "li", NULL), ui_elem(stdout, "a", (const UiAttr[]){{"href", "/profile"}}, 1, 0), ui_lit(stdout, "Your page"), ui_close(stdout, "a"), ui_close(stdout, "li"), ui_open(stdout, "li", NULL), ui_elem(stdout, "a", (const UiAttr[]){{"href", "/apps"}}, 1, 0), ui_lit(stdout, "Your tools"), ui_close(stdout, "a"), ui_close(stdout, "li"), ui_open(stdout, "li", NULL), ui_elem(stdout, "a", (const UiAttr[]){{"href", "/listen"}}, 1, 0), ui_lit(stdout, "Listening queue"), ui_close(stdout, "a"), ui_close(stdout, "li"), ui_close(stdout, "ul"), ui_close(stdout, "section"), ui_close(stdout, "aside"));
    (ui_open(stdout, "div", "ms-music-main"), ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-record"}, {"aria-label", "On the turntable"}}, 2, 0), ui_open(stdout, "div", "ms-record-art"));
    ms_img(lead,560,"ms-cover");(ui_close(stdout, "div"), ui_open(stdout, "div", "ms-record-body"), ui_open(stdout, "p", "ms-meta"));
    if(lead->group[0]){ui_text(stdout, "span", "ms-tag", lead->group);}
    ms_when(lead,now);(ui_close(stdout, "p"), ui_open(stdout, "h2", NULL));ms_title_link(lead);ui_close(stdout, "h2");
    if(lead->by[0]){ui_text(stdout, "p", "ms-by", lead->by);}
    ms_excerpt(lead,"ms-excerpt");
    if(lead->rights[0]){ui_text(stdout, "p", "ms-rights", lead->rights);}
    ui_open(stdout, "div", "ms-controls");
    if(ms_playable(lead)){fputs("<button type=\"button\" class=\"ms-primary\"",stdout);ms_track(lead,1);ui_lit(stdout, ">");page_glyph("play");(ui_lit(stdout, "Play"), ui_close(stdout, "button"));}
    ms_keep(lead,route,0);ms_make();
    if(page_item_has_page(lead)){
        fputs("<a class=\"ms-ghost\" href=\"/o/",stdout);page_url_escape(lead->source);ui_lit(stdout, "/");page_url_escape(lead->key);
        ui_lit(stdout, "\">");page_glyph("text");(ui_lit(stdout, "Notes"), ui_close(stdout, "a"));
    }
    ui_close_n(stdout, "div", "div", "section", NULL);
    ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-tracks"}, {"aria-label", "Tracks"}}, 2, 0);
    ms_section_head("Tracks","Recordings",NULL);
    (ui_open(stdout, "table", NULL), ui_open(stdout, "thead", NULL), ui_open(stdout, "tr", NULL), ui_elem(stdout, "th", (const UiAttr[]){{"scope", "col"}}, 1, 0), ui_lit(stdout, "#"), ui_close(stdout, "th"), ui_elem(stdout, "th", (const UiAttr[]){{"scope", "col"}}, 1, 0), ui_lit(stdout, "Title"), ui_close(stdout, "th"), ui_elem(stdout, "th", (const UiAttr[]){{"scope", "col"}, {"class", "ms-col-src"}}, 2, 0), ui_lit(stdout, "From"), ui_close(stdout, "th"), ui_elem(stdout, "th", (const UiAttr[]){{"scope", "col"}, {"class", "ms-col-when"}}, 2, 0), ui_lit(stdout, "Released"), ui_close(stdout, "th"), ui_elem(stdout, "th", (const UiAttr[]){{"scope", "col"}}, 1, 0), ui_lit(stdout, "Time"), ui_close(stdout, "th"), ui_elem(stdout, "th", (const UiAttr[]){{"scope", "col"}, {"class", "ms-col-keep"}, {"aria-label", "Your library"}}, 3, 0), ui_close(stdout, "th"), ui_close(stdout, "tr"), ui_close(stdout, "thead"), ui_open(stdout, "tbody", NULL));
    int no=0;
    for(int i=0;i<l->count&&no<30;i++){
        const PageItem *it=l->item[i];if(!ms_playable(it))continue;no++;
        printf("<tr%s><td>",it==lead?" class=\"is-on\"":"");
        fputs("<button type=\"button\" class=\"ms-no\"",stdout);ms_track(it,0);ui_lit(stdout, " aria-label=\"Play ");html(it->title);
        printf("\"><span>%02d</span>",no);page_glyph("play");(ui_close(stdout, "button"), ui_close(stdout, "td"), ui_open(stdout, "td", NULL), ui_open(stdout, "b", NULL));ms_title_link(it);
        (ui_close(stdout, "b"), ui_open(stdout, "small", NULL));html(it->by);(ui_close(stdout, "small"), ui_close(stdout, "td"), ui_open(stdout, "td", "ms-col-src"));html(ms_from(it));
        (ui_close(stdout, "td"), ui_open(stdout, "td", "ms-col-when"));ms_when(it,now);(ui_close(stdout, "td"), ui_open(stdout, "td", NULL));ms_dur(it);(ui_close(stdout, "td"), ui_open(stdout, "td", "ms-col-keep"));ms_keep(it,route,1);ui_close_n(stdout, "td", "tr", NULL);
    }
    ui_close_n(stdout, "tbody", "table", NULL);
    if(!no)ms_empty("None of these releases carry a file this page can play; each opens at its publisher.");
    ui_close(stdout, "section");
    /* Stations: each publisher's newest release, played from here. */
    {
        const PageItem *st[8];int n=0;
        for(int i=0;i<l->count&&n<8;i++){
            int dup=0;for(int j=0;j<n;j++)if(!strcasecmp(ms_from(st[j]),ms_from(l->item[i])))dup=1;
            if(!dup&&ms_from(l->item[i])[0])st[n++]=l->item[i];
        }
        if(n>1){
            ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-stations"}, {"aria-label", "Stations"}}, 2, 0);
            ms_section_head("Stations","Publishers on this page","Each opens on its newest release");
            ui_open(stdout, "ul", "ms-cards ms-cards-3");
            for(int j=0;j<n;j++){
                (ui_open(stdout, "li", NULL), ui_open(stdout, "div", "ms-card-art"));ms_img(st[j],420,"ms-cover");
                if(ms_hearable(st[j])){fputs("<button type=\"button\" class=\"ms-play ms-on-art\"",stdout);ms_track(st[j],0);ui_lit(stdout, " aria-label=\"Play\">");page_glyph("play");ui_close(stdout, "button");}
                fputs("</div><p class=\"ms-kicker\"><a href=\"",stdout);ms_query_link(route,"from",ms_from(st[j]));ui_lit(stdout, "\">");html(ms_from(st[j]));
                (ui_close(stdout, "a"), ui_close(stdout, "p"), ui_open(stdout, "h3", NULL));ms_title_link(st[j]);ui_close(stdout, "h3");ms_excerpt(st[j],"ms-card-text");ui_close(stdout, "li");
            }
            ui_close_n(stdout, "ul", "section", NULL);
        }
    }
    {
        int n=0;
        for(int i=0;i<l->count;i++)if(l->item[i]!=lead&&ms_picture(l->item[i])[0])n++;
        if(n>=4){
            ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-recent"}, {"aria-label", "Recently released"}}, 2, 0);
            ms_section_head("Recently released","New on this page",NULL);
            ui_open(stdout, "ul", "ms-cards ms-cards-4");
            int shown=0;
            for(int i=0;i<l->count&&shown<8;i++){
                const PageItem *it=l->item[i];if(it==lead||!ms_picture(it)[0])continue;shown++;
                (ui_open(stdout, "li", NULL), ui_open(stdout, "div", "ms-card-art"));ms_img(it,360,"ms-cover");(ui_close(stdout, "div"), ui_open(stdout, "h3", NULL));ms_title_link(it);
                (ui_close(stdout, "h3"), ui_open(stdout, "p", "ms-card-by"));html(it->by[0]?it->by:ms_from(it));ui_close_n(stdout, "p", "li", NULL);
            }
            ui_close_n(stdout, "ul", "section", NULL);
        }
    }
    ui_close_n(stdout, "div", "div", NULL);
}

static void ms_cinema_card(const PageItem *it,time_t now,int width){
    (ui_open(stdout, "li", NULL), ui_open(stdout, "div", "ms-card-art"));
    const char *close=page_emit_item_open(it,"ms-art-link");ms_img(it,width,"ms-cover");fputs(close,stdout);
    if(it->duration_ms>0){ui_open(stdout, "span", "ms-over");page_emit_duration(it->duration_ms);ui_close(stdout, "span");}
    if(it->rights[0]){ui_text(stdout, "span", "ms-over ms-over-l", it->rights);}
    (ui_close(stdout, "div"), ui_open(stdout, "p", "ms-kicker"));html(ms_from(it));(ui_close(stdout, "p"), ui_open(stdout, "h3", NULL));ms_title_link(it);ui_close(stdout, "h3");
    ms_excerpt(it,"ms-card-text");
    (ui_open(stdout, "footer", NULL), ui_open(stdout, "span", NULL));if(it->by[0]&&strcmp(it->by,ms_from(it)))html(it->by);else ms_when(it,now);ui_close(stdout, "span");
    if(ms_playable(it)){fputs("<button type=\"button\" class=\"ms-link\"",stdout);ms_track(it,0);(ui_lit(stdout, ">Listen"), ui_close(stdout, "button"));}
    if(page_item_has_page(it)){fputs("<a class=\"ms-link\" href=\"/o/",stdout);page_url_escape(it->source);ui_lit(stdout, "/");page_url_escape(it->key);(ui_lit(stdout, "\">Watch"), ui_close(stdout, "a"));}
    ui_close_n(stdout, "footer", "li", NULL);
}
/* Watch, Film, Stage, Sports: the cinema commons catalogue. */
static void ms_cinema(const MsList *l,const MsList *sound,const char *route,const char *title,const char *lede,
                      const char *from,const char *q,const char *host,time_t now){
    const PageItem *lead=NULL;
    for(int i=0;i<l->count&&!lead;i++)if(ms_picture(l->item[i])[0])lead=l->item[i];
    if(!lead&&l->count)lead=l->item[0];
    const char *territory=strcmp(host,"aurekai.work")?"Live":"Work";
    if(!lead){
        (ui_open(stdout, "header", "ms-head"), ui_open(stdout, "p", "ms-kicker"), ui_open(stdout, "i", "ms-dot"), ui_close(stdout, "i"));html(territory);(ui_close(stdout, "p"), ui_open(stdout, "h1", NULL));html(title);ui_close(stdout, "h1");
        if(lede&&*lede){ui_text(stdout, "p", "ms-lede", lede);}
        ms_narrowed(route,from,q);ui_close(stdout, "header");
        ms_empty("Nothing has been released here yet.");return;
    }
    char sized[1024];page_picture_at(ms_picture(lead),1600,sized,sizeof(sized));
    ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-marquee"}, {"aria-label", "Featured"}}, 2, 0);
    if(page_image_url_safe(sized))page_emit_picture(sized,"ms-marquee-bg");
    (ui_open(stdout, "div", "ms-marquee-body"), ui_open(stdout, "p", "ms-meta"), ui_open(stdout, "span", "ms-tag"), ui_open(stdout, "i", "ms-dot"), ui_close(stdout, "i"));html(territory);ui_lit(stdout, " &middot; ");html(title);ui_close(stdout, "span");
    if(lead->group[0]){ui_text(stdout, "span", "ms-tag ms-tag-quiet", lead->group);}
    (ui_close(stdout, "p"), ui_open(stdout, "h1", NULL));ms_title_link(lead);(ui_close(stdout, "h1"), ui_open(stdout, "p", "ms-facts"));
    ui_text(stdout, "span", NULL, ms_from(lead));ms_when(lead,now);ms_dur(lead);
    if(lead->rights[0]){ui_text(stdout, "span", "ms-chip", lead->rights);}
    ui_close(stdout, "p");
    if(lead->body[0])ms_excerpt(lead,"ms-lede");else if(lede&&*lede){ui_text(stdout, "p", "ms-lede", lede);}
    ui_open(stdout, "div", "ms-controls");
    if(page_item_has_page(lead)){fputs("<a class=\"ms-primary\" href=\"/o/",stdout);page_url_escape(lead->source);ui_lit(stdout, "/");page_url_escape(lead->key);ui_lit(stdout, "\">");page_glyph("play");(ui_lit(stdout, "Watch"), ui_close(stdout, "a"));}
    else if(public_http_url(lead->href)){ui_open_a(stdout, "a", "class", "ms-primary", "rel", "noopener", "href", lead->href);page_glyph("play");(ui_lit(stdout, "Watch at the publisher"), ui_close(stdout, "a"));}
    if(ms_playable(lead)){fputs("<button type=\"button\" class=\"ms-ghost\"",stdout);ms_track(lead,1);ui_lit(stdout, ">");page_glyph("listen");(ui_lit(stdout, "Listen"), ui_close(stdout, "button"));}
    if(page_item_has_page(lead)){fputs("<a class=\"ms-ghost\" href=\"/o/",stdout);page_url_escape(lead->source);ui_lit(stdout, "/");page_url_escape(lead->key);ui_lit(stdout, "#item-keep\">");page_glyph("save");(ui_lit(stdout, "Keep for later"), ui_close(stdout, "a"));}
    if(lead->rights_href[0]&&public_http_url(lead->rights_href)){ui_open_a(stdout, "a", "class", "ms-ghost", "rel", "noopener", "href", lead->rights_href);page_glyph("text");(ui_lit(stdout, "Terms"), ui_close(stdout, "a"));}
    ui_close_n(stdout, "div", "div", "section", NULL);
    ui_open(stdout, "div", "ms-filter");ms_chips(l,route,from,"Everything",7);ms_narrowed(route,from,q);
    printf("<p class=\"ms-aside\"><i class=\"ms-dot\"></i>%d releases from %d publishers</p></div>",l->count,ms_publishers(l));
    int at=0;const PageItem *rest[MS_TAKE];int n=0;
    for(int i=0;i<l->count;i++)if(l->item[i]!=lead)rest[n++]=l->item[i];
    if(n){
        ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-sec"}, {"aria-label", "Latest releases"}}, 2, 0);
        ms_section_head("Latest","New releases",NULL);
        ui_open(stdout, "ul", "ms-cards ms-cards-4");
        for(;at<n&&at<4;at++)ms_cinema_card(rest[at],now,480);
        ui_close_n(stdout, "ul", "section", NULL);
    }
    if(at<n){
        /* Longer watches: what remains, longest first. */
        const PageItem *longer[3]={0};
        for(int i=at;i<n;i++){
            const PageItem *it=rest[i];
            for(int s=0;s<3;s++){
                if(!longer[s]||it->duration_ms>longer[s]->duration_ms){for(int t=2;t>s;t--)longer[t]=longer[t-1];longer[s]=it;break;}
            }
        }
        ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-sec"}, {"aria-label", "Longer watches"}}, 2, 0);
        ms_section_head("Settle in","Longer watches",NULL);
        ui_open(stdout, "ul", "ms-cards ms-cards-3");
        for(int s=0;s<3&&longer[s];s++)ms_cinema_card(longer[s],now,640);
        ui_close_n(stdout, "ul", "section", NULL);
        int more=0;
        for(int i=at;i<n;i++){int used=0;for(int s=0;s<3;s++)if(longer[s]==rest[i])used=1;if(!used)more++;}
        if(more){
            ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-sec"}, {"aria-label", "More"}}, 2, 0);
            ms_section_head("More","Everything else here",NULL);
            ui_open(stdout, "ul", "ms-cards ms-cards-3 ms-cards-compact");
            int shown=0;
            for(int i=at;i<n&&shown<24;i++){int used=0;for(int s=0;s<3;s++)if(longer[s]==rest[i])used=1;if(used)continue;shown++;ms_cinema_card(rest[i],now,400);}
            ui_close_n(stdout, "ul", "section", NULL);
        }
    }
    /* Folded from the 10-foot screen: something to put on without looking. */
    if(sound&&sound->count){
        int shown=0;
        for(int i=0;i<sound->count;i++)if(ms_hearable(sound->item[i]))shown++;
        if(shown){
            ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-sec"}, {"aria-label", "Put this on"}}, 2, 0);
            ms_section_head("Audio","Put this on","Plays in the player below");
            ui_open(stdout, "ul", "ms-cards ms-cards-3 ms-audio-cards");
            shown=0;
            for(int i=0;i<sound->count&&shown<3;i++){
                const PageItem *it=sound->item[i];if(!ms_hearable(it))continue;shown++;
                (ui_open(stdout, "li", NULL), ui_open(stdout, "p", "ms-meta"), ui_open(stdout, "span", NULL));html(ms_from(it));ui_close(stdout, "span");ms_dur(it);(ui_close(stdout, "p"), ui_open(stdout, "h3", NULL));ms_title_link(it);
                ui_close(stdout, "h3");ms_excerpt(it,"ms-card-text");(ui_open(stdout, "footer", NULL), ui_open(stdout, "span", NULL));html(it->by);fputs("</span><button type=\"button\" class=\"ms-link\"",stdout);
                ms_track(it,0);ui_lit(stdout, ">");fputs(ms_playable(it)?"Listen":"Read aloud",stdout);ui_close_n(stdout, "button", "footer", NULL);ms_narration_text(it);ui_close(stdout, "li");
            }
            (ui_close(stdout, "ul"), ui_open(stdout, "p", "ms-more"), ui_elem(stdout, "a", (const UiAttr[]){{"href", "/listen"}}, 1, 0), ui_lit(stdout, "The whole listening queue"), ui_close(stdout, "a"), ui_close(stdout, "p"), ui_close(stdout, "section"));
        }
    }
}

/* Art: a monograph of plates. */
static void ms_art(const MsList *l,const char *route,const char *title,const char *lede,
                   const char *from,const char *q,time_t now){
    const PageItem *lead=NULL;
    for(int i=0;i<l->count&&!lead;i++)if(ms_picture(l->item[i])[0])lead=l->item[i];
    if(!lead&&l->count)lead=l->item[0];
    (ui_open(stdout, "header", "ms-head ms-mono-head"), ui_open(stdout, "div", NULL), ui_open(stdout, "p", "ms-kicker"), ui_open(stdout, "i", "ms-dot"), ui_close(stdout, "i"), ui_lit(stdout, "Live &middot; monograph of the visual commons"), ui_close(stdout, "p"), ui_open(stdout, "h1", NULL));
    html(title);ui_close(stdout, "h1");
    if(lede&&*lede){ui_text(stdout, "p", "ms-lede", lede);}
    ms_narrowed(route,from,q);
    fputs("</div><div class=\"ms-controls ms-controls-col\"><button type=\"button\" class=\"ms-primary\" data-ak-track data-narrate=\".ms-art-text\" data-title=\"",stdout);
    html(title);ui_lit(stdout, "\" data-by=\"Aurekai\">");page_glyph("listen");(ui_lit(stdout, "Hear this page read"), ui_close(stdout, "button"));
    if(lead&&page_item_has_page(lead)){fputs("<a class=\"ms-ghost\" href=\"/o/",stdout);page_url_escape(lead->source);ui_lit(stdout, "/");page_url_escape(lead->key);ui_lit(stdout, "\">");page_glyph("eye");(ui_lit(stdout, "Open the first plate"), ui_close(stdout, "a"));}
    ui_close(stdout, "div");
    {
        time_t newest=0;for(int i=0;i<l->count;i++)if(l->item[i]->at>newest)newest=l->item[i]->at;
        char when[48]="";page_when(newest,now,when,sizeof(when));
        printf("<dl class=\"ms-facts-grid\"><div><dt>Plates</dt><dd>%d</dd></div><div><dt>Publishers</dt><dd>%d</dd></div><div><dt>Newest</dt><dd>",l->count,ms_publishers(l));
        html(when[0]?when:"none yet");(ui_close(stdout, "dd"), ui_close(stdout, "div"), ui_open(stdout, "div", NULL), ui_open(stdout, "dt", NULL), ui_lit(stdout, "Narrow"), ui_close(stdout, "dt"), ui_open(stdout, "dd", NULL));
        ms_chips(l,route,from,"All",3);ui_close_n(stdout, "dd", "div", "dl", NULL);
    }
    ui_close(stdout, "header");
    if(!lead){ms_empty("No plates have been published here yet.");return;}
    ui_open(stdout, "div", "ms-art-text");
    (ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-plate"}, {"aria-label", "Plate I"}}, 2, 0), ui_open(stdout, "header", "ms-sh"), ui_open(stdout, "div", NULL), ui_open(stdout, "p", "ms-kicker"), ui_lit(stdout, "Plate I &middot; "));html(ms_from(lead));
    (ui_close(stdout, "p"), ui_open(stdout, "h2", NULL));ms_title_link(lead);(ui_close(stdout, "h2"), ui_close(stdout, "div"), ui_open(stdout, "p", "ms-aside"));html(lead->by);(ui_close(stdout, "p"), ui_close(stdout, "header"), ui_open(stdout, "figure", NULL));
    {char sized[1024];page_picture_at(ms_picture(lead),1400,sized,sizeof(sized));if(page_image_url_safe(sized))page_emit_picture(sized,"ms-plate-img");}
    ui_open(stdout, "figcaption", NULL);ms_when(lead,now);if(lead->rights[0]){ui_lit(stdout, " &middot; ");html(lead->rights);}
    if(lead->origin[0]){ui_lit(stdout, " &middot; kept by ");html(lead->origin);}ui_close_n(stdout, "figcaption", "figure", NULL);
    if(lead->body[0]){(ui_open(stdout, "div", "ms-columns"), ui_open(stdout, "p", NULL));html(lead->body);ui_close_n(stdout, "p", "div", NULL);}
    ui_close(stdout, "section");
    int plates=0;
    for(int i=0;i<l->count;i++)if(l->item[i]!=lead&&ms_picture(l->item[i])[0])plates++;
    if(plates){
        ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-sec"}, {"aria-label", "Plates"}}, 2, 0);
        ms_section_head("Plates II onward","The collection",NULL);
        ui_open(stdout, "ul", "ms-plates");int no=1,shown=0;
        for(int i=0;i<l->count&&shown<12;i++){
            const PageItem *it=l->item[i];if(it==lead||!ms_picture(it)[0])continue;shown++;no++;
            (ui_open(stdout, "li", NULL), ui_open(stdout, "figure", NULL));const char *close=page_emit_item_open(it,"ms-art-link");ms_img(it,560,"ms-cover");fputs(close,stdout);
            printf("<figcaption><span class=\"ms-kicker\">Plate %d &middot; ",no);html(ms_from(it));(ui_close(stdout, "span"), ui_open(stdout, "b", NULL));html(it->title);
            ui_close_n(stdout, "b", "figcaption", "figure", "li", NULL);
        }
        ui_close_n(stdout, "ul", "section", NULL);
    }
    int words=0;
    for(int i=0;i<l->count;i++)if(l->item[i]!=lead&&!ms_picture(l->item[i])[0])words++;
    if(words||l->count>13){
        ui_elem(stdout, "section", (const UiAttr[]){{"class", "ms-sec"}, {"aria-label", "Index"}}, 2, 0);
        ms_section_head("Index","Everything in this monograph",NULL);
        ui_open(stdout, "ol", "ms-lexicon");int no=0;
        for(int i=0;i<l->count&&no<32;i++){
            const PageItem *it=l->item[i];no++;
            printf("<li><p class=\"ms-kicker\"><span>%02d</span>",no);html(ms_from(it));(ui_close(stdout, "p"), ui_open(stdout, "h3", NULL));ms_title_link(it);ui_close(stdout, "h3");
            ms_excerpt(it,"ms-card-text");ui_close(stdout, "li");
        }
        ui_close_n(stdout, "ol", "section", NULL);
    }
    ui_close(stdout, "div");
}

/* Returns 1 when the route is one of the media screens and has been written. */
static int page_media_screen(sqlite3 *db,const char *host,const char *route,const char *query,
                             const char *title,const char *lede,const char *from,const char *q,
                             const PageMaterial *m,time_t now){
    int mode=ms_mode(host,route);
    if(mode==MS_NONE||!m)return 0;
    MsList *l=calloc(1,sizeof(*l)),*sound=calloc(1,sizeof(*sound));
    if(!l||!sound){free(l);free(sound);return 0;}
    switch(mode){
    case MS_LISTEN:
        ms_take(l,m,BF_PAGE_MATERIAL_AUDIO,0);ms_take(l,m,BF_PAGE_MATERIAL_VIDEO,0);
        ms_take(l,m,BF_PAGE_MATERIAL_PUBLICATION,0);ms_take(l,m,BF_PAGE_MATERIAL_POST,0);break;
    case MS_MUSIC:
        ms_take(l,m,BF_PAGE_MATERIAL_AUDIO,0);ms_take(l,m,BF_PAGE_MATERIAL_VIDEO,0);ms_take(l,m,BF_PAGE_MATERIAL_PUBLICATION,0);break;
    case MS_CINEMA:
        ms_take(l,m,BF_PAGE_MATERIAL_VIDEO,0);ms_take(l,m,BF_PAGE_MATERIAL_PHOTO,1);ms_take(l,m,BF_PAGE_MATERIAL_PUBLICATION,1);
        ms_take(l,m,BF_PAGE_MATERIAL_EVENT,1);
        ms_take(sound,m,BF_PAGE_MATERIAL_AUDIO,0);ms_take(sound,m,BF_PAGE_MATERIAL_PUBLICATION,0);break;
    case MS_ART:
        ms_take(l,m,BF_PAGE_MATERIAL_PHOTO,1);ms_take(l,m,BF_PAGE_MATERIAL_VIDEO,1);ms_take(l,m,BF_PAGE_MATERIAL_PUBLICATION,1);
        ms_take(l,m,BF_PAGE_MATERIAL_PUBLICATION,0);ms_take(l,m,BF_PAGE_MATERIAL_POST,0);break;
    }
    static const char *names[]={"","listen","music","cinema","art"};
    ms_saved_open();
    fputs("<div class=\"ms ms-",stdout);fputs(names[mode],stdout);ui_lit(stdout, "\" data-media-screen=\"");fputs(names[mode],stdout);ui_lit(stdout, "\">");
    if(mode==MS_LISTEN)ms_listen(l,route,title,lede,from,q,host,now);
    else if(mode==MS_MUSIC)ms_music(l,route,title,lede,from,q,now);
    else if(mode==MS_CINEMA)ms_cinema(l,sound,route,title,lede,from,q,host,now);
    else ms_art(l,route,title,lede,from,q,now);
    if((!from||!*from)&&(!q||!*q))page_emit_media_spaces(db,route,query,host);
    ui_close(stdout, "div");
    ms_saved_close();free(l);free(sound);
    return 1;
}

#endif
