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
            fputs("<style>.media-space-catalogue{margin:24px 0 42px;padding:24px;border:1px solid var(--w-rule,#444935);border-radius:18px;background:var(--w-panel,#171d19)}"
                  ".media-space-heading{display:flex;gap:12px;align-items:center;margin:0 0 12px}.media-space-heading .ak-icon{width:26px;height:26px}"
                  ".media-space-controls{display:grid;grid-template-columns:minmax(200px,1fr) auto;gap:12px 18px;align-items:end;margin:16px 0}"
                  ".media-space-controls label{display:grid;gap:7px;font:600 12px/1.4 system-ui;min-width:0}.media-space-controls input{min-height:44px;width:100%;min-width:0;padding:10px;border-radius:9px;background:var(--w-ground,#0f1511);color:inherit;border:1px solid var(--w-rule,#444935)}"
                  ".media-space-controls output{grid-column:1/-1;font:12px/1.4 system-ui}.media-space-modes{display:flex;gap:7px;flex-wrap:wrap}.media-space-modes button{min-height:44px;border:1px solid var(--w-rule,#444935);border-radius:99px;background:transparent;color:inherit;padding:8px 14px;cursor:pointer}.media-space-modes [aria-pressed=true]{background:var(--w-accent,#d0fc4f);color:var(--w-ground,#0f1511)}"
                  ".media-space-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,265px),1fr));gap:12px;list-style:none;margin:0;padding:0}.media-space-cards li{padding:18px;border:1px solid var(--w-rule,#444935);border-radius:13px;min-width:0}"
                  ".media-space-cards li[hidden]{display:none!important}.media-space-cards h3{font:600 clamp(18px,2vw,23px)/1.2 system-ui;margin:9px 0}.media-space-cards p{line-height:1.5;margin:8px 0}.media-space-cards small{color:var(--w-muted,#c5c9af)}"
                  ".media-space-catalogue a{color:inherit;text-underline-offset:3px}.media-space-catalogue a:focus-visible,.media-space-catalogue :is(input,button):focus-visible{outline:2px solid var(--w-accent,#d0fc4f);outline-offset:3px}"
                  "@media(max-width:650px){.media-space-catalogue{padding:14px}.media-space-controls{grid-template-columns:1fr}.media-space-cards{grid-template-columns:1fr}}</style>",stdout);
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
#endif
