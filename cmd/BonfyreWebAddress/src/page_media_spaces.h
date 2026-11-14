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
    page_url_escape(source);fputs("/",stdout);page_url_escape(key);fputs("\">",stdout);
    if(have)html(item.title);else fputs("Open retained film item",stdout);
    fputs("</a></p>",stdout);
}
static void page_media_space_link(const char *host,const char *space,const char *screening){
    fputs("https://",stdout);html(host);fputs("/worlds/",stdout);html(space);
    if(screening&&*screening){fputs("#screening-",stdout);html(screening);}
}
static void page_media_space_controls(int events){
    fputs("<div class=\"media-space-controls\"><label>Find ",stdout);
    fputs(events?"a screening or venue":"a published space",stdout);
    fputs("<input type=\"search\" maxlength=\"120\" data-media-space-search placeholder=\"Search this page\"></label>",stdout);
    if(events)fputs("<div class=\"media-space-modes\" role=\"group\" aria-label=\"Screening dates\"><button type=\"button\" data-media-space-filter=\"all\" aria-pressed=\"true\">All dates</button><button type=\"button\" data-media-space-filter=\"weekend\" aria-pressed=\"false\">Weekend</button></div>",stdout);
    fputs("<output role=\"status\" aria-live=\"polite\" data-media-space-count></output></div>",stdout);
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
            fputs("<section class=\"media-space-catalogue\" data-media-space-catalogue aria-label=\"Published account media spaces\"><h2 class=\"media-space-heading\">",stdout);
            page_glyph(events?"calendar":films?"play":"listen");
            fputs(events?"Screenings from the commons":films?"Cinema from the commons":"Listen in the commons",stdout);
            fputs("</h2><p>Published spaces kept by their own accounts. Each space opens at its owner's address; playback, licences and club access are checked there.</p>",stdout);
            page_media_space_controls(events);
            fputs("<ol class=\"media-space-cards\">",stdout);
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
        fputs("\"><small>",stdout);html(events?"Announced screening":medium);
        fputs(" &middot; ",stdout);html(publisher);fputs("</small><h3><a href=\"",stdout);
        page_media_space_link(publisher,space,events?screening:NULL);
        fputs("\">",stdout);html(title);fputs("</a></h3>",stdout);
        if(events){
            fputs("<p><time datetime=\"",stdout);html(starts);fputs("\">",stdout);html(starts);fputs("</time>",stdout);
            if(venue&&*venue){fputs(" &middot; ",stdout);html(venue);}fputs("</p>",stdout);
        }else if(lede&&*lede){fputs("<p>",stdout);html(lede);fputs("</p>",stdout);}
        if(events)page_media_screening_record(archive,film_ref);
        fputs("<p><a href=\"",stdout);page_media_space_link(publisher,space,NULL);
        fputs("\">Open published space</a> &middot; <a href=\"https://",stdout);html(publisher);
        fputs("/\">Owner's public profile</a></p></li>",stdout);
    }
    sqlite3_finalize(rows);
    if(archive)sqlite3_close(archive);
    if(count){
        fputs("</ol>",stdout);
        if(count==25&&page<1000)printf("<p class=\"media-space-next\"><a href=\"%s?spaces-page=%u\" rel=\"next\">More published spaces</a></p>",route,page+1);
        if(page)printf("<p><a href=\"%s?spaces-page=%u\" rel=\"prev\">Previous spaces</a></p>",route,page-1);
        fputs("</section>",stdout);
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
    else{fputs("<span class=\"",stdout);html(cls);fputs(" ms-noart\" aria-hidden=\"true\">",stdout);page_glyph(it->kind==BF_PAGE_MATERIAL_VIDEO?"play":it->kind==BF_PAGE_MATERIAL_AUDIO?"listen":"discover");fputs("</span>",stdout);}
}
static const char *ms_from(const PageItem *it){ return it->note[0]?it->note:it->group[0]?it->group:it->by; }
/* The track attributes the shared player reads. A recording plays its file;
 * an article is read aloud from its own text block on this page. */
static void ms_track(const PageItem *it,int dflt){
    if(ms_playable(it)){
        fputs(" data-ak-track data-src=\"",stdout);html(it->asset);
    }else if(it->kind==BF_PAGE_MATERIAL_PUBLICATION&&it->key[0]&&it->body[0]){
        fputs(" data-ak-track data-narrate=\"#ms-n-",stdout);html(it->key);
    }else return;
    fputs("\" data-title=\"",stdout);html(it->title);
    fputs("\" data-by=\"",stdout);html(it->by[0]?it->by:ms_from(it));fputs("\"",stdout);
    if(dflt)fputs(" data-ak-default",stdout);
}
static int ms_hearable(const PageItem *it){
    return ms_playable(it)||(it->kind==BF_PAGE_MATERIAL_PUBLICATION&&it->key[0]&&it->body[0]);
}
static void ms_play_button(const PageItem *it,const char *label,int dflt){
    if(!ms_hearable(it))return;
    fputs("<button type=\"button\" class=\"ms-play\" data-ak-queue=\"listen\"",stdout);ms_track(it,dflt);
    fputs(" aria-label=\"",stdout);html(label);fputs(": ",stdout);html(it->title);fputs("\">",stdout);
    page_glyph("play");fputs("</button>",stdout);
}
static void ms_narration_text(const PageItem *it){
    if(it->kind!=BF_PAGE_MATERIAL_PUBLICATION||!it->key[0]||!it->body[0]||ms_playable(it))return;
    fputs("<div class=\"ms-narration\" hidden id=\"ms-n-",stdout);html(it->key);fputs("\"><h3>",stdout);
    html(it->title);fputs("</h3><p>",stdout);html(it->body);fputs("</p></div>",stdout);
}
static void ms_excerpt(const PageItem *it,const char *cls){
    if(!it->body[0])return;
    fputs("<p class=\"",stdout);html(cls);fputs("\">",stdout);html(it->body);fputs("</p>",stdout);
}
static void ms_when(const PageItem *it,time_t now){
    char when[48];page_when(it->at,now,when,sizeof(when));
    if(when[0]){fputs("<time>",stdout);html(when);fputs("</time>",stdout);}
}
static void ms_dur(const PageItem *it){
    if(it->duration_ms<=0)return;
    fputs("<span class=\"ms-dur\">",stdout);page_emit_duration(it->duration_ms);fputs("</span>",stdout);
}
static void ms_title_link(const PageItem *it){
    const char *close=page_emit_item_open(it,NULL);html(it->title);fputs(close,stdout);
}
/* A waveform drawn from the item's own key: decoration, never a measurement. */
static void ms_wave(const PageItem *it,int bars){
    unsigned h=2166136261u;const char *k=it->key[0]?it->key:it->title;
    fputs("<span class=\"ms-wave\" aria-hidden=\"true\">",stdout);
    for(int i=0;i<bars;i++){
        h^=(unsigned char)k[i%(strlen(k)?strlen(k):1)];h*=16777619u;
        printf("<i style=\"--h:%u%%\"></i>",18+(h>>8)%80);
    }
    fputs("</span>",stdout);
}
static void ms_query_link(const char *route,const char *key,const char *value){
    fputs(route,stdout);fputs("?",stdout);fputs(key,stdout);fputs("=",stdout);page_url_escape(value);
}
/* Distinct publishers on this page, as links that narrow the route. */
static void ms_chips(const MsList *l,const char *route,const char *from,const char *all_label,int limit){
    const char *seen[16];int n=0;
    fputs("<nav class=\"ms-chips\" aria-label=\"Narrow by publisher\"><a href=\"",stdout);html(route);fputs("\"",stdout);
    if(!from||!*from)fputs(" aria-current=\"page\"",stdout);
    fputs(">",stdout);html(all_label);fputs("</a>",stdout);
    for(int i=0;i<l->count&&n<limit&&n<16;i++){
        const char *name=ms_from(l->item[i]);int dup=0;
        if(!name[0])continue;
        for(int j=0;j<n;j++)if(!strcasecmp(seen[j],name))dup=1;
        if(dup)continue;seen[n++]=name;
        fputs("<a href=\"",stdout);ms_query_link(route,"from",name);fputs("\"",stdout);
        if(from&&*from&&!strcasecmp(from,name))fputs(" aria-current=\"page\"",stdout);
        fputs(">",stdout);html(name);fputs("</a>",stdout);
    }
    fputs("</nav>",stdout);
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
    fputs("<p class=\"ms-narrowed\">Showing ",stdout);
    if(q&&*q){fputs("matches for <b>",stdout);html(q);fputs("</b> ",stdout);}
    if(from&&*from){fputs("from <b>",stdout);html(from);fputs("</b> ",stdout);}
    fputs("&middot; <a href=\"",stdout);html(route);fputs("\">Show all</a></p>",stdout);
}
static void ms_empty(const char *what){
    fputs("<p class=\"ms-empty\">",stdout);html(what);fputs("</p>",stdout);
}
static void ms_section_head(const char *kicker,const char *title,const char *aside){
    fputs("<header class=\"ms-sh\"><div><p class=\"ms-kicker\">",stdout);html(kicker);
    fputs("</p><h2>",stdout);html(title);fputs("</h2></div>",stdout);
    if(aside&&*aside){fputs("<p class=\"ms-aside\">",stdout);html(aside);fputs("</p>",stdout);}
    fputs("</header>",stdout);
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
    fputs("<header class=\"ms-head\"><p class=\"ms-kicker\"><i class=\"ms-dot\"></i>",stdout);
    fputs(strcmp(host,"aurekai.work")?"Live &middot; listening queue":"Work &middot; listening queue",stdout);
    fputs("</p><h1>",stdout);html(title);fputs("</h1>",stdout);
    if(lede&&*lede){fputs("<p class=\"ms-lede\">",stdout);html(lede);fputs("</p>",stdout);}
    printf("<ul class=\"ms-stats\"><li><b>%d</b> publishers</li><li><b>%d</b> recordings</li>",ms_publishers(l),recordings);
    if(narrated)printf("<li><b>%d</b> read aloud</li>",narrated);
    if(total_ms>=60000)printf("<li><b>%ld</b> hours to hear</li>",total_ms/3600000+(total_ms%3600000>=1800000));
    fputs("</ul>",stdout);
    ms_narrowed(route,from,q);
    ms_chips(l,route,from,"All audio",6);
    fputs("</header>",stdout);
    if(!lead){ms_empty("Nothing to hear here yet. Recordings and articles appear as their publishers release them.");return;}
    /* Now playing: what the shared player holds on arrival. */
    fputs("<section class=\"ms-now\" aria-label=\"Now playing\"><div class=\"ms-now-art\">",stdout);
    ms_img(lead,640,"ms-cover");
    fputs("<p class=\"ms-badge\"><i class=\"ms-dot\"></i>Up first</p><p class=\"ms-now-src\"><small>From</small><b>",stdout);
    html(ms_from(lead));fputs("</b></p></div><div class=\"ms-now-body\"><p class=\"ms-meta\">",stdout);
    if(lead->group[0]){fputs("<span class=\"ms-tag\">",stdout);html(lead->group);fputs("</span>",stdout);}
    ms_when(lead,now);ms_dur(lead);fputs("</p><h2>",stdout);ms_title_link(lead);fputs("</h2>",stdout);
    if(lead->by[0]){fputs("<p class=\"ms-by\">",stdout);html(lead->by);fputs("</p>",stdout);}
    ms_excerpt(lead,"ms-excerpt");
    ms_wave(lead,64);
    fputs("<div class=\"ms-controls\">",stdout);
    ms_play_button(lead,"Play",1);
    if(lead->transcript_href[0]&&public_http_url(lead->transcript_href)){
        fputs("<a class=\"ms-ghost\" rel=\"noopener\" href=\"",stdout);html(lead->transcript_href);fputs("\">",stdout);page_glyph("text");fputs("Transcript</a>",stdout);
    }
    if(page_item_has_page(lead)){
        fputs("<a class=\"ms-ghost\" href=\"/o/",stdout);page_url_escape(lead->source);fputs("/",stdout);page_url_escape(lead->key);
        fputs("#item-keep\">",stdout);page_glyph("save");fputs("Keep for later</a>",stdout);
    }
    if(lead->by[0]&&lead->source[0]){
        fputs("<a class=\"ms-ghost\" href=\"/people?who=",stdout);page_url_escape(lead->by);fputs("&amp;source=",stdout);page_url_escape(lead->source);
        fputs("\">",stdout);page_glyph("group");fputs("More from ",stdout);html(lead->by);fputs("</a>",stdout);
    }
    fputs("</div></div></section>",stdout);
    ms_narration_text(lead);
    /* The queue: every row plays through the one player. */
    int rows=0;
    for(int i=0;i<l->count;i++)if(l->item[i]!=lead&&ms_hearable(l->item[i]))rows++;
    if(rows){
        char aside[64];snprintf(aside,sizeof(aside),"%d in the queue",rows);
        fputs("<section class=\"ms-queue\" aria-label=\"Queue\">",stdout);
        ms_section_head("Up next","Episodes, recordings and articles read aloud",aside);
        fputs("<ol class=\"ms-rows\">",stdout);
        int shown=0;
        for(int i=0;i<l->count&&shown<40;i++){
            const PageItem *it=l->item[i];
            if(it==lead||!ms_hearable(it))continue;shown++;
            fputs("<li>",stdout);ms_play_button(it,ms_playable(it)?"Play":"Read aloud",0);
            fputs("<div class=\"ms-row-main\"><p class=\"ms-row-src\"><b>",stdout);html(ms_from(it));fputs("</b>",stdout);
            if(it->by[0]&&strcmp(it->by,ms_from(it))){fputs(" &middot; ",stdout);html(it->by);}
            if(!ms_playable(it))fputs(" &middot; <em>read aloud</em>",stdout);
            fputs("</p><h3>",stdout);ms_title_link(it);fputs("</h3>",stdout);ms_excerpt(it,"ms-row-text");
            fputs("</div><p class=\"ms-row-meta\">",stdout);ms_dur(it);ms_when(it,now);fputs("</p>",stdout);
            if(page_item_has_page(it)){
                fputs("<a class=\"ms-icon\" href=\"/o/",stdout);page_url_escape(it->source);fputs("/",stdout);page_url_escape(it->key);
                fputs("#item-keep\" aria-label=\"Keep for later\">",stdout);page_glyph("save");fputs("</a>",stdout);
            }
            if(public_http_url(it->href)){
                fputs("<a class=\"ms-icon\" rel=\"noopener\" target=\"_blank\" href=\"",stdout);html(it->href);
                fputs("\" aria-label=\"Open at the publisher\">",stdout);page_glyph("out");fputs("</a>",stdout);
            }
            ms_narration_text(it);
            fputs("</li>",stdout);
        }
        fputs("</ol></section>",stdout);
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
        fputs("<section class=\"ms-long\" aria-label=\"Long listens\">",stdout);
        ms_section_head("For a long sit","The longest recordings here",NULL);
        fputs("<div class=\"ms-long-grid\">",stdout);
        for(int i=0;i<2&&longest[i];i++){
            const PageItem *it=longest[i];
            fputs("<article><p class=\"ms-meta\"><span class=\"ms-tag\">",stdout);html(ms_from(it));fputs("</span>",stdout);
            ms_when(it,now);fputs("</p><h3>",stdout);ms_title_link(it);fputs("</h3>",stdout);ms_excerpt(it,"ms-long-text");
            fputs("<footer>",stdout);ms_wave(it,24);ms_dur(it);
            fputs("<button type=\"button\" class=\"ms-pill\"",stdout);ms_track(it,0);fputs(">",stdout);page_glyph("play");fputs("Play</button></footer></article>",stdout);
        }
        fputs("</div></section>",stdout);
    }
}

/* Music: library, the record on the turntable, its tracks, and stations. */
static void ms_music(const MsList *l,const char *route,const char *title,const char *lede,
                     const char *from,const char *q,time_t now){
    const PageItem *lead=NULL;
    for(int i=0;i<l->count&&!lead;i++)if(ms_playable(l->item[i])&&ms_picture(l->item[i])[0])lead=l->item[i];
    for(int i=0;i<l->count&&!lead;i++)if(ms_playable(l->item[i]))lead=l->item[i];
    if(!lead&&l->count)lead=l->item[0];
    fputs("<header class=\"ms-head ms-head-slim\"><p class=\"ms-kicker\"><i class=\"ms-dot\"></i>Live &middot; music</p><h1>",stdout);
    html(title);fputs("</h1>",stdout);
    if(lede&&*lede){fputs("<p class=\"ms-lede\">",stdout);html(lede);fputs("</p>",stdout);}
    ms_narrowed(route,from,q);fputs("</header>",stdout);
    if(!lead){ms_empty("No music has been released here yet.");return;}
    fputs("<div class=\"ms-music-grid\"><aside class=\"ms-library\" aria-label=\"Library\"><section><p class=\"ms-kicker\">Library</p><ul>",stdout);
    {
        const char *seen[12];int counts[12],n=0;
        for(int i=0;i<l->count;i++){
            const char *name=ms_from(l->item[i]);int at=-1;if(!name[0])continue;
            for(int j=0;j<n;j++)if(!strcasecmp(seen[j],name))at=j;
            if(at>=0)counts[at]++;else if(n<12){seen[n]=name;counts[n++]=1;}
        }
        fputs("<li><a href=\"",stdout);html(route);fputs("\"",stdout);if(!from||!*from)fputs(" aria-current=\"page\"",stdout);
        printf(">All music<span>%d</span></a></li>",l->count);
        for(int j=0;j<n;j++){
            fputs("<li><a href=\"",stdout);ms_query_link(route,"from",seen[j]);fputs("\"",stdout);
            if(from&&*from&&!strcasecmp(from,seen[j]))fputs(" aria-current=\"page\"",stdout);
            fputs(">",stdout);html(seen[j]);printf("<span>%d</span></a></li>",counts[j]);
        }
    }
    fputs("</ul></section><section><p class=\"ms-kicker\">Yours</p><ul><li><a href=\"/profile\">Your page</a></li><li><a href=\"/apps\">Your tools</a></li><li><a href=\"/listen\">Listening queue</a></li></ul></section></aside>",stdout);
    fputs("<div class=\"ms-music-main\"><section class=\"ms-record\" aria-label=\"On the turntable\"><div class=\"ms-record-art\">",stdout);
    ms_img(lead,560,"ms-cover");fputs("</div><div class=\"ms-record-body\"><p class=\"ms-meta\">",stdout);
    if(lead->group[0]){fputs("<span class=\"ms-tag\">",stdout);html(lead->group);fputs("</span>",stdout);}
    ms_when(lead,now);fputs("</p><h2>",stdout);ms_title_link(lead);fputs("</h2>",stdout);
    if(lead->by[0]){fputs("<p class=\"ms-by\">",stdout);html(lead->by);fputs("</p>",stdout);}
    ms_excerpt(lead,"ms-excerpt");
    if(lead->rights[0]){fputs("<p class=\"ms-rights\">",stdout);html(lead->rights);fputs("</p>",stdout);}
    fputs("<div class=\"ms-controls\">",stdout);
    if(ms_playable(lead)){fputs("<button type=\"button\" class=\"ms-primary\"",stdout);ms_track(lead,1);fputs(">",stdout);page_glyph("play");fputs("Play</button>",stdout);}
    if(page_item_has_page(lead)){
        fputs("<a class=\"ms-ghost\" href=\"/o/",stdout);page_url_escape(lead->source);fputs("/",stdout);page_url_escape(lead->key);
        fputs("#item-keep\">",stdout);page_glyph("save");fputs("Keep</a>",stdout);
        fputs("<a class=\"ms-ghost\" href=\"/o/",stdout);page_url_escape(lead->source);fputs("/",stdout);page_url_escape(lead->key);
        fputs("\">",stdout);page_glyph("text");fputs("Notes</a>",stdout);
    }
    fputs("</div></div></section>",stdout);
    fputs("<section class=\"ms-tracks\" aria-label=\"Tracks\">",stdout);
    ms_section_head("Tracks","Recordings",NULL);
    fputs("<table><thead><tr><th scope=\"col\">#</th><th scope=\"col\">Title</th><th scope=\"col\" class=\"ms-col-src\">From</th><th scope=\"col\" class=\"ms-col-when\">Released</th><th scope=\"col\">Time</th></tr></thead><tbody>",stdout);
    int no=0;
    for(int i=0;i<l->count&&no<30;i++){
        const PageItem *it=l->item[i];if(!ms_playable(it))continue;no++;
        printf("<tr%s><td>",it==lead?" class=\"is-on\"":"");
        fputs("<button type=\"button\" class=\"ms-no\"",stdout);ms_track(it,0);fputs(" aria-label=\"Play ",stdout);html(it->title);
        printf("\"><span>%02d</span>",no);page_glyph("play");fputs("</button></td><td><b>",stdout);ms_title_link(it);
        fputs("</b><small>",stdout);html(it->by);fputs("</small></td><td class=\"ms-col-src\">",stdout);html(ms_from(it));
        fputs("</td><td class=\"ms-col-when\">",stdout);ms_when(it,now);fputs("</td><td>",stdout);ms_dur(it);fputs("</td></tr>",stdout);
    }
    fputs("</tbody></table>",stdout);
    if(!no)ms_empty("None of these releases carry a file this page can play; each opens at its publisher.");
    fputs("</section>",stdout);
    /* Stations: each publisher's newest release, played from here. */
    {
        const PageItem *st[8];int n=0;
        for(int i=0;i<l->count&&n<8;i++){
            int dup=0;for(int j=0;j<n;j++)if(!strcasecmp(ms_from(st[j]),ms_from(l->item[i])))dup=1;
            if(!dup&&ms_from(l->item[i])[0])st[n++]=l->item[i];
        }
        if(n>1){
            fputs("<section class=\"ms-stations\" aria-label=\"Stations\">",stdout);
            ms_section_head("Stations","Publishers on this page","Each opens on its newest release");
            fputs("<ul class=\"ms-cards ms-cards-3\">",stdout);
            for(int j=0;j<n;j++){
                fputs("<li><div class=\"ms-card-art\">",stdout);ms_img(st[j],420,"ms-cover");
                if(ms_hearable(st[j])){fputs("<button type=\"button\" class=\"ms-play ms-on-art\"",stdout);ms_track(st[j],0);fputs(" aria-label=\"Play\">",stdout);page_glyph("play");fputs("</button>",stdout);}
                fputs("</div><p class=\"ms-kicker\"><a href=\"",stdout);ms_query_link(route,"from",ms_from(st[j]));fputs("\">",stdout);html(ms_from(st[j]));
                fputs("</a></p><h3>",stdout);ms_title_link(st[j]);fputs("</h3>",stdout);ms_excerpt(st[j],"ms-card-text");fputs("</li>",stdout);
            }
            fputs("</ul></section>",stdout);
        }
    }
    {
        int n=0;
        for(int i=0;i<l->count;i++)if(l->item[i]!=lead&&ms_picture(l->item[i])[0])n++;
        if(n>=4){
            fputs("<section class=\"ms-recent\" aria-label=\"Recently released\">",stdout);
            ms_section_head("Recently released","New on this page",NULL);
            fputs("<ul class=\"ms-cards ms-cards-4\">",stdout);
            int shown=0;
            for(int i=0;i<l->count&&shown<8;i++){
                const PageItem *it=l->item[i];if(it==lead||!ms_picture(it)[0])continue;shown++;
                fputs("<li><div class=\"ms-card-art\">",stdout);ms_img(it,360,"ms-cover");fputs("</div><h3>",stdout);ms_title_link(it);
                fputs("</h3><p class=\"ms-card-by\">",stdout);html(it->by[0]?it->by:ms_from(it));fputs("</p></li>",stdout);
            }
            fputs("</ul></section>",stdout);
        }
    }
    fputs("</div></div>",stdout);
}

static void ms_cinema_card(const PageItem *it,time_t now,int width){
    fputs("<li><div class=\"ms-card-art\">",stdout);
    const char *close=page_emit_item_open(it,"ms-art-link");ms_img(it,width,"ms-cover");fputs(close,stdout);
    if(it->duration_ms>0){fputs("<span class=\"ms-over\">",stdout);page_emit_duration(it->duration_ms);fputs("</span>",stdout);}
    if(it->rights[0]){fputs("<span class=\"ms-over ms-over-l\">",stdout);html(it->rights);fputs("</span>",stdout);}
    fputs("</div><p class=\"ms-kicker\">",stdout);html(ms_from(it));fputs("</p><h3>",stdout);ms_title_link(it);fputs("</h3>",stdout);
    ms_excerpt(it,"ms-card-text");
    fputs("<footer><span>",stdout);if(it->by[0]&&strcmp(it->by,ms_from(it)))html(it->by);else ms_when(it,now);fputs("</span>",stdout);
    if(ms_playable(it)){fputs("<button type=\"button\" class=\"ms-link\"",stdout);ms_track(it,0);fputs(">Listen</button>",stdout);}
    if(page_item_has_page(it)){fputs("<a class=\"ms-link\" href=\"/o/",stdout);page_url_escape(it->source);fputs("/",stdout);page_url_escape(it->key);fputs("\">Watch</a>",stdout);}
    fputs("</footer></li>",stdout);
}
/* Watch, Film, Stage, Sports: the cinema commons catalogue. */
static void ms_cinema(const MsList *l,const MsList *sound,const char *route,const char *title,const char *lede,
                      const char *from,const char *q,const char *host,time_t now){
    const PageItem *lead=NULL;
    for(int i=0;i<l->count&&!lead;i++)if(ms_picture(l->item[i])[0])lead=l->item[i];
    if(!lead&&l->count)lead=l->item[0];
    const char *territory=strcmp(host,"aurekai.work")?"Live":"Work";
    if(!lead){
        fputs("<header class=\"ms-head\"><p class=\"ms-kicker\"><i class=\"ms-dot\"></i>",stdout);html(territory);fputs("</p><h1>",stdout);html(title);fputs("</h1>",stdout);
        if(lede&&*lede){fputs("<p class=\"ms-lede\">",stdout);html(lede);fputs("</p>",stdout);}
        ms_narrowed(route,from,q);fputs("</header>",stdout);
        ms_empty("Nothing has been released here yet.");return;
    }
    char sized[1024];page_picture_at(ms_picture(lead),1600,sized,sizeof(sized));
    fputs("<section class=\"ms-marquee\" aria-label=\"Featured\">",stdout);
    if(page_image_url_safe(sized))page_emit_picture(sized,"ms-marquee-bg");
    fputs("<div class=\"ms-marquee-body\"><p class=\"ms-meta\"><span class=\"ms-tag\"><i class=\"ms-dot\"></i>",stdout);html(territory);fputs(" &middot; ",stdout);html(title);fputs("</span>",stdout);
    if(lead->group[0]){fputs("<span class=\"ms-tag ms-tag-quiet\">",stdout);html(lead->group);fputs("</span>",stdout);}
    fputs("</p><h1>",stdout);ms_title_link(lead);fputs("</h1><p class=\"ms-facts\">",stdout);
    fputs("<span>",stdout);html(ms_from(lead));fputs("</span>",stdout);ms_when(lead,now);ms_dur(lead);
    if(lead->rights[0]){fputs("<span class=\"ms-chip\">",stdout);html(lead->rights);fputs("</span>",stdout);}
    fputs("</p>",stdout);
    if(lead->body[0])ms_excerpt(lead,"ms-lede");else if(lede&&*lede){fputs("<p class=\"ms-lede\">",stdout);html(lede);fputs("</p>",stdout);}
    fputs("<div class=\"ms-controls\">",stdout);
    if(page_item_has_page(lead)){fputs("<a class=\"ms-primary\" href=\"/o/",stdout);page_url_escape(lead->source);fputs("/",stdout);page_url_escape(lead->key);fputs("\">",stdout);page_glyph("play");fputs("Watch</a>",stdout);}
    else if(public_http_url(lead->href)){fputs("<a class=\"ms-primary\" rel=\"noopener\" href=\"",stdout);html(lead->href);fputs("\">",stdout);page_glyph("play");fputs("Watch at the publisher</a>",stdout);}
    if(ms_playable(lead)){fputs("<button type=\"button\" class=\"ms-ghost\"",stdout);ms_track(lead,1);fputs(">",stdout);page_glyph("listen");fputs("Listen</button>",stdout);}
    if(page_item_has_page(lead)){fputs("<a class=\"ms-ghost\" href=\"/o/",stdout);page_url_escape(lead->source);fputs("/",stdout);page_url_escape(lead->key);fputs("#item-keep\">",stdout);page_glyph("save");fputs("Keep for later</a>",stdout);}
    if(lead->rights_href[0]&&public_http_url(lead->rights_href)){fputs("<a class=\"ms-ghost\" rel=\"noopener\" href=\"",stdout);html(lead->rights_href);fputs("\">",stdout);page_glyph("text");fputs("Terms</a>",stdout);}
    fputs("</div></div></section>",stdout);
    fputs("<div class=\"ms-filter\">",stdout);ms_chips(l,route,from,"Everything",7);ms_narrowed(route,from,q);
    printf("<p class=\"ms-aside\"><i class=\"ms-dot\"></i>%d releases from %d publishers</p></div>",l->count,ms_publishers(l));
    int at=0;const PageItem *rest[MS_TAKE];int n=0;
    for(int i=0;i<l->count;i++)if(l->item[i]!=lead)rest[n++]=l->item[i];
    if(n){
        fputs("<section class=\"ms-sec\" aria-label=\"Latest releases\">",stdout);
        ms_section_head("Latest","New releases",NULL);
        fputs("<ul class=\"ms-cards ms-cards-4\">",stdout);
        for(;at<n&&at<4;at++)ms_cinema_card(rest[at],now,480);
        fputs("</ul></section>",stdout);
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
        fputs("<section class=\"ms-sec\" aria-label=\"Longer watches\">",stdout);
        ms_section_head("Settle in","Longer watches",NULL);
        fputs("<ul class=\"ms-cards ms-cards-3\">",stdout);
        for(int s=0;s<3&&longer[s];s++)ms_cinema_card(longer[s],now,640);
        fputs("</ul></section>",stdout);
        int more=0;
        for(int i=at;i<n;i++){int used=0;for(int s=0;s<3;s++)if(longer[s]==rest[i])used=1;if(!used)more++;}
        if(more){
            fputs("<section class=\"ms-sec\" aria-label=\"More\">",stdout);
            ms_section_head("More","Everything else here",NULL);
            fputs("<ul class=\"ms-cards ms-cards-3 ms-cards-compact\">",stdout);
            int shown=0;
            for(int i=at;i<n&&shown<24;i++){int used=0;for(int s=0;s<3;s++)if(longer[s]==rest[i])used=1;if(used)continue;shown++;ms_cinema_card(rest[i],now,400);}
            fputs("</ul></section>",stdout);
        }
    }
    /* Folded from the 10-foot screen: something to put on without looking. */
    if(sound&&sound->count){
        int shown=0;
        for(int i=0;i<sound->count;i++)if(ms_hearable(sound->item[i]))shown++;
        if(shown){
            fputs("<section class=\"ms-sec\" aria-label=\"Put this on\">",stdout);
            ms_section_head("Audio","Put this on","Plays in the player below");
            fputs("<ul class=\"ms-cards ms-cards-3 ms-audio-cards\">",stdout);
            shown=0;
            for(int i=0;i<sound->count&&shown<3;i++){
                const PageItem *it=sound->item[i];if(!ms_hearable(it))continue;shown++;
                fputs("<li><p class=\"ms-meta\"><span>",stdout);html(ms_from(it));fputs("</span>",stdout);ms_dur(it);fputs("</p><h3>",stdout);ms_title_link(it);
                fputs("</h3>",stdout);ms_excerpt(it,"ms-card-text");fputs("<footer><span>",stdout);html(it->by);fputs("</span><button type=\"button\" class=\"ms-link\"",stdout);
                ms_track(it,0);fputs(">",stdout);fputs(ms_playable(it)?"Listen":"Read aloud",stdout);fputs("</button></footer>",stdout);ms_narration_text(it);fputs("</li>",stdout);
            }
            fputs("</ul><p class=\"ms-more\"><a href=\"/listen\">The whole listening queue</a></p></section>",stdout);
        }
    }
}

/* Art: a monograph of plates. */
static void ms_art(const MsList *l,const char *route,const char *title,const char *lede,
                   const char *from,const char *q,time_t now){
    const PageItem *lead=NULL;
    for(int i=0;i<l->count&&!lead;i++)if(ms_picture(l->item[i])[0])lead=l->item[i];
    if(!lead&&l->count)lead=l->item[0];
    fputs("<header class=\"ms-head ms-mono-head\"><div><p class=\"ms-kicker\"><i class=\"ms-dot\"></i>Live &middot; monograph of the visual commons</p><h1>",stdout);
    html(title);fputs("</h1>",stdout);
    if(lede&&*lede){fputs("<p class=\"ms-lede\">",stdout);html(lede);fputs("</p>",stdout);}
    ms_narrowed(route,from,q);
    fputs("</div><div class=\"ms-controls ms-controls-col\"><button type=\"button\" class=\"ms-primary\" data-ak-track data-narrate=\".ms-art-text\" data-title=\"",stdout);
    html(title);fputs("\" data-by=\"Aurekai\">",stdout);page_glyph("listen");fputs("Hear this page read</button>",stdout);
    if(lead&&page_item_has_page(lead)){fputs("<a class=\"ms-ghost\" href=\"/o/",stdout);page_url_escape(lead->source);fputs("/",stdout);page_url_escape(lead->key);fputs("\">",stdout);page_glyph("eye");fputs("Open the first plate</a>",stdout);}
    fputs("</div>",stdout);
    {
        time_t newest=0;for(int i=0;i<l->count;i++)if(l->item[i]->at>newest)newest=l->item[i]->at;
        char when[48]="";page_when(newest,now,when,sizeof(when));
        printf("<dl class=\"ms-facts-grid\"><div><dt>Plates</dt><dd>%d</dd></div><div><dt>Publishers</dt><dd>%d</dd></div><div><dt>Newest</dt><dd>",l->count,ms_publishers(l));
        html(when[0]?when:"none yet");fputs("</dd></div><div><dt>Narrow</dt><dd>",stdout);
        ms_chips(l,route,from,"All",3);fputs("</dd></div></dl>",stdout);
    }
    fputs("</header>",stdout);
    if(!lead){ms_empty("No plates have been published here yet.");return;}
    fputs("<div class=\"ms-art-text\">",stdout);
    fputs("<section class=\"ms-plate\" aria-label=\"Plate I\"><header class=\"ms-sh\"><div><p class=\"ms-kicker\">Plate I &middot; ",stdout);html(ms_from(lead));
    fputs("</p><h2>",stdout);ms_title_link(lead);fputs("</h2></div><p class=\"ms-aside\">",stdout);html(lead->by);fputs("</p></header><figure>",stdout);
    {char sized[1024];page_picture_at(ms_picture(lead),1400,sized,sizeof(sized));if(page_image_url_safe(sized))page_emit_picture(sized,"ms-plate-img");}
    fputs("<figcaption>",stdout);ms_when(lead,now);if(lead->rights[0]){fputs(" &middot; ",stdout);html(lead->rights);}
    if(lead->origin[0]){fputs(" &middot; kept by ",stdout);html(lead->origin);}fputs("</figcaption></figure>",stdout);
    if(lead->body[0]){fputs("<div class=\"ms-columns\"><p>",stdout);html(lead->body);fputs("</p></div>",stdout);}
    fputs("</section>",stdout);
    int plates=0;
    for(int i=0;i<l->count;i++)if(l->item[i]!=lead&&ms_picture(l->item[i])[0])plates++;
    if(plates){
        fputs("<section class=\"ms-sec\" aria-label=\"Plates\">",stdout);
        ms_section_head("Plates II onward","The collection",NULL);
        fputs("<ul class=\"ms-plates\">",stdout);int no=1,shown=0;
        for(int i=0;i<l->count&&shown<12;i++){
            const PageItem *it=l->item[i];if(it==lead||!ms_picture(it)[0])continue;shown++;no++;
            fputs("<li><figure>",stdout);const char *close=page_emit_item_open(it,"ms-art-link");ms_img(it,560,"ms-cover");fputs(close,stdout);
            printf("<figcaption><span class=\"ms-kicker\">Plate %d &middot; ",no);html(ms_from(it));fputs("</span><b>",stdout);html(it->title);
            fputs("</b></figcaption></figure></li>",stdout);
        }
        fputs("</ul></section>",stdout);
    }
    int words=0;
    for(int i=0;i<l->count;i++)if(l->item[i]!=lead&&!ms_picture(l->item[i])[0])words++;
    if(words||l->count>13){
        fputs("<section class=\"ms-sec\" aria-label=\"Index\">",stdout);
        ms_section_head("Index","Everything in this monograph",NULL);
        fputs("<ol class=\"ms-lexicon\">",stdout);int no=0;
        for(int i=0;i<l->count&&no<32;i++){
            const PageItem *it=l->item[i];no++;
            printf("<li><p class=\"ms-kicker\"><span>%02d</span>",no);html(ms_from(it));fputs("</p><h3>",stdout);ms_title_link(it);fputs("</h3>",stdout);
            ms_excerpt(it,"ms-card-text");fputs("</li>",stdout);
        }
        fputs("</ol></section>",stdout);
    }
    fputs("</div>",stdout);
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
    fputs("<div class=\"ms ms-",stdout);fputs(names[mode],stdout);fputs("\" data-media-screen=\"",stdout);fputs(names[mode],stdout);fputs("\">",stdout);
    if(mode==MS_LISTEN)ms_listen(l,route,title,lede,from,q,host,now);
    else if(mode==MS_MUSIC)ms_music(l,route,title,lede,from,q,now);
    else if(mode==MS_CINEMA)ms_cinema(l,sound,route,title,lede,from,q,host,now);
    else ms_art(l,route,title,lede,from,q,now);
    if((!from||!*from)&&(!q||!*q))page_emit_media_spaces(db,route,query,host);
    fputs("</div>",stdout);
    free(l);free(sound);
    return 1;
}

#endif
