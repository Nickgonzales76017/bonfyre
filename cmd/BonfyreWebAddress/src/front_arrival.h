#ifndef BONFYRE_FRONT_ARRIVAL_H
#define BONFYRE_FRONT_ARRIVAL_H

typedef struct {
    const char *type;
    const char *href;
    const char *glyph;
    const char *kicker;
    const char *title;
    const char *copy;
    const char *count_sql;
    const char *sample_sql;
} FrontArrivalDoor;

static int front_arrival_count(sqlite3 *db,const FrontArrivalDoor *door) {
    sqlite3_stmt *st=NULL;int value=-1;
    if(!db||!door||!door->type||!door->count_sql||!cms_type_registered(db,door->type))return -1;
    if(sqlite3_prepare_v2(db,door->count_sql,-1,&st,NULL)==SQLITE_OK&&sqlite3_step(st)==SQLITE_ROW)
        value=sqlite3_column_int(st,0);
    sqlite3_finalize(st);return value;
}

static int front_arrival_samples(sqlite3 *db,const FrontArrivalDoor *door,char out[3][161]) {
    sqlite3_stmt *st=NULL;int n=0;
    if(!db||!door||!door->type||!door->sample_sql||!cms_type_registered(db,door->type))return 0;
    if(sqlite3_prepare_v2(db,door->sample_sql,-1,&st,NULL)!=SQLITE_OK)return 0;
    while(n<3&&sqlite3_step(st)==SQLITE_ROW){
        const char *value=(const char *)sqlite3_column_text(st,0);size_t len=value?strlen(value):0;
        if(!len||len>160)continue;
        snprintf(out[n],161,"%s",value);n++;
    }
    sqlite3_finalize(st);return n;
}

static void front_arrival_door(sqlite3 *db,const FrontArrivalDoor *door) {
    char samples[3][161]={{0}};int n=front_arrival_samples(db,door,samples),count=front_arrival_count(db,door);
    fputs("<article class=\"front-arrival-door\" data-arrival-door><a class=\"front-arrival-main\" href=\"",stdout);
    html(door->href);fputs("\"><span class=\"front-arrival-icon\" data-arrival-icon>",stdout);page_glyph(door->glyph);
    fputs("</span><span class=\"front-arrival-copy\"><span class=\"front-arrival-kicker\">",stdout);html(door->kicker);
    fputs("</span><strong>",stdout);html(door->title);fputs("</strong><span>",stdout);html(door->copy);fputs("</span></span>",stdout);
    if(count>=0){fputs("<span class=\"front-arrival-count\">",stdout);printf("%d",count);fputs(" published</span>",stdout);}
    fputs("</a>",stdout);
    if(n){fputs("<ul class=\"front-arrival-samples\" aria-label=\"Published examples\">",stdout);for(int i=0;i<n;i++){fputs("<li>",stdout);html(samples[i]);fputs("</li>",stdout);}fputs("</ul>",stdout);}
    fputs("</article>",stdout);
}

static const char *front_arrival_lede(const char *territory) {
    if(!strcmp(territory,"live"))return "Follow the people, sources and communities behind what is happening.";
    if(!strcmp(territory,"life"))return "Move from what is around you into people, communities, plans, tools and repeatable programs.";
    if(!strcmp(territory,"shop"))return "Trace what is worth buying into makers, sources and reusable workflows.";
    return "Move from reading into reusable applications, programs, sources and people.";
}

static int front_arrival(sqlite3 *db,const char *territory) {
    static const FrontArrivalDoor doors[]={
        {"site_presentation","/people","user","People","People & profiles","Find the people and published profile surfaces already connected to the estate.",
         "SELECT COUNT(*) FROM site_presentation WHERE namespace='root' AND status='published'",
         "SELECT host FROM site_presentation WHERE namespace='root' AND status='published' ORDER BY id DESC LIMIT 3"},
        {"community_body","/communities","group","Community","Communities","Read public conversations, find a community, or start one around a shared subject.",
         "SELECT COUNT(*) FROM community_body WHERE namespace='root' AND status='published' AND visibility='public'",
         "SELECT title FROM community_body WHERE namespace='root' AND status='published' AND visibility='public' ORDER BY id DESC LIMIT 3"},
        {"pack_artifact","/apps?context=self","apps","Tools","Applications & workflows","Open your applications and continue your work.",
         "SELECT COUNT(*) FROM pack_artifact WHERE namespace='root' AND status='published'",
         NULL},
        {"program_template","/programs?context=self","play","Programs","Programs","Continue account-scoped plans and repeatable programs over the capabilities already installed.",
         "SELECT COUNT(*) FROM program_template WHERE namespace='root' AND status='published'",
         "SELECT name FROM program_template WHERE namespace='root' AND status='published' ORDER BY COALESCE(ordinal,2147483647),id LIMIT 3"},
        {"internet_source","/sources","link","Source estate","Sources & feeds","Inspect where public material comes from and move from reading into source-backed work.",
         "SELECT COUNT(*) FROM internet_source WHERE namespace='root' AND status='published'",
         "SELECT label FROM internet_source WHERE namespace='root' AND status='published' ORDER BY id DESC LIMIT 3"},
        {"world","/w","world","Worlds","Worlds","Open temporary publications that assemble a subject, its sources and the interaction grammars around it.",
         "SELECT COUNT(*) FROM world WHERE namespace='root' AND status='published'",
         "SELECT DISTINCT title FROM world WHERE namespace='root' AND status='published' ORDER BY title LIMIT 3"},
        {NULL,"/organisations","briefcase","Institutions","Organisations","Browse public organisations and the people, work and services addressed around them.",NULL,NULL}
    };
    if(!territory||!*territory)return 0;
    /* Territory-first choices; every remaining published owner stays reachable. */
    static const unsigned char work_first[4]={2,3,4,6};
    static const unsigned char life_first[4]={0,1,3,5};
    static const unsigned char live_first[4]={0,1,5,4};
    static const unsigned char shop_first[4]={4,2,6,3};
    const unsigned char *first=!strcmp(territory,"work")?work_first:
                              !strcmp(territory,"live")?live_first:
                              !strcmp(territory,"shop")?shop_first:life_first;
    fputs("<section class=\"front-arrival\" data-front-arrival data-territory=\"",stdout);html(territory);
    fputs("\" aria-labelledby=\"front-arrival-title\"><header><div><p class=\"front-arrival-kicker\">Beyond this front</p><h2 id=\"front-arrival-title\">Explore the rest of Aurekai</h2><p data-arrival-lede>",stdout);
    html(front_arrival_lede(territory));
    fputs("</p></div><nav aria-label=\"Account continuations\"><a href=\"/profile\">Your profile</a><a href=\"/apps?context=self\">Your work</a><a href=\"/programs?context=self\">Your programs</a></nav></header><div class=\"front-arrival-grid\">",stdout);
    for(size_t i=0;i<4;i++)front_arrival_door(db,&doors[first[i]]);
    fputs("</div><details class=\"front-arrival-more\" data-arrival-more><summary>Explore more of Aurekai</summary><div class=\"front-arrival-grid\">",stdout);
    for(size_t i=0;i<sizeof(doors)/sizeof(doors[0]);i++){
        int selected=0;for(size_t j=0;j<4;j++)if(i==first[j])selected=1;
        if(!selected)front_arrival_door(db,&doors[i]);
    }
    fputs("</div>",stdout);
    fputs("</details>",stdout);
    fputs("</section><style>"
        ".front-arrival{margin:clamp(36px,7vw,104px) 0;padding:clamp(24px,4vw,56px) 0;border-block:1px solid color-mix(in srgb,currentColor 22%,transparent);color:inherit;box-sizing:border-box}"
        ".front-arrival *{box-sizing:border-box}.front-arrival>header{display:grid;grid-template-columns:minmax(0,1.25fr) minmax(260px,.75fr);gap:clamp(24px,6vw,84px);align-items:end;margin-bottom:clamp(30px,5vw,62px)}"
        ".front-arrival>header>div{max-width:70ch}.front-arrival-kicker{font:600 11px/1.5 'DM Mono',ui-monospace,monospace;letter-spacing:.11em;text-transform:uppercase;margin:0 0 8px}"
        ".front-arrival h2{font:400 clamp(38px,5.5vw,76px)/.98 'Instrument Serif',Georgia,serif;letter-spacing:-.045em;margin:0 0 18px;max-width:16ch}.front-arrival>header p{font:400 clamp(16px,1.7vw,20px)/1.55 'DM Sans',system-ui;margin:0;max-width:62ch}"
        ".front-arrival>header nav{display:flex;flex-wrap:wrap;gap:9px;justify-content:flex-end}.front-arrival>header nav a{display:inline-flex;align-items:center;min-height:44px;padding:9px 15px;border:1px solid currentColor;border-radius:999px;color:inherit;text-decoration:none;font:600 13px/1.3 'DM Sans',system-ui}"
        ".front-arrival-grid{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:0 clamp(18px,3vw,42px)}.front-arrival-door{grid-column:span 4;min-width:0;border-top:2px solid currentColor;padding:20px 0 28px}.front-arrival-door:nth-child(1),.front-arrival-door:nth-child(2){grid-column:span 6}"
        ".front-arrival-more{margin-top:12px;border-top:1px solid color-mix(in srgb,currentColor 24%,transparent)}.front-arrival-more>summary{cursor:pointer;padding:16px 4px;min-height:52px;font:600 14px/1.5 'DM Sans',system-ui;list-style-position:inside}.front-arrival-more[open]>summary{margin-bottom:8px}.front-arrival-more>.front-arrival-grid{padding-top:12px}"
        ".front-arrival-main{display:grid;grid-template-columns:auto minmax(0,1fr);grid-template-areas:'icon copy' 'count copy';align-items:start;column-gap:14px;color:inherit;text-decoration:none}.front-arrival-icon{grid-area:icon;display:grid;place-items:center;width:44px;height:44px;border:1px solid currentColor;border-radius:999px}.front-arrival-icon .ak-icon{width:22px;height:22px}"
        ".front-arrival-copy{grid-area:copy;display:grid;gap:7px;min-width:0}.front-arrival-copy strong{font:600 clamp(20px,2vw,28px)/1.1 'DM Sans',system-ui;letter-spacing:-.025em}.front-arrival-copy>span:last-child{font:400 14px/1.55 'DM Sans',system-ui;opacity:.78}"
        ".front-arrival-count{grid-area:count;font:500 10px/1.3 'DM Mono',ui-monospace,monospace;margin-top:7px;white-space:nowrap;opacity:.7}.front-arrival-main:hover strong{text-decoration:underline;text-underline-offset:4px}.front-arrival-samples{list-style:none;margin:16px 0 0 58px;padding:0;display:flex;flex-wrap:wrap;gap:6px 12px}.front-arrival-samples li{font:500 11px/1.4 'DM Sans',system-ui;opacity:.72;overflow-wrap:anywhere}"
        ".front-arrival-templates{margin-top:22px;padding-top:24px;border-top:1px solid color-mix(in srgb,currentColor 18%,transparent);display:grid;grid-template-columns:minmax(220px,.7fr) minmax(0,1.3fr);gap:clamp(24px,4vw,56px);align-items:start}.front-arrival-templates h3{font:500 clamp(24px,3vw,36px)/1.08 'DM Sans',system-ui;letter-spacing:-.03em;margin:5px 0 10px}.front-arrival-templates p{font:14px/1.55 'DM Sans',system-ui;opacity:.76;margin:0}.front-arrival-template-list{display:flex;flex-wrap:wrap;gap:8px}.front-arrival-template{display:grid;gap:2px;padding:9px 12px;border:1px solid color-mix(in srgb,currentColor 28%,transparent);border-radius:12px}.front-arrival-template strong{font:600 12px/1.25 'DM Sans',system-ui}.front-arrival-template small{font:500 9px/1.3 'DM Mono',ui-monospace,monospace;opacity:.65}"
        ".front-arrival :focus-visible{outline:3px solid currentColor;outline-offset:4px}@media(max-width:900px){.front-arrival>header{grid-template-columns:1fr}.front-arrival>header nav{justify-content:flex-start}.front-arrival-door,.front-arrival-door:nth-child(1),.front-arrival-door:nth-child(2){grid-column:span 6}.front-arrival-templates{grid-template-columns:1fr}}"
        "@media(max-width:620px){.front-arrival-door,.front-arrival-door:nth-child(1),.front-arrival-door:nth-child(2){grid-column:1/-1}.front-arrival-samples{margin-left:0}.front-arrival h2{max-width:none}}"
        "</style>",stdout);
    return 1;
}
#endif
