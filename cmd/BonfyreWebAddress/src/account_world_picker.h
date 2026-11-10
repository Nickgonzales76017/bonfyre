#ifndef BONFYRE_ACCOUNT_WORLD_PICKER_H
#define BONFYRE_ACCOUNT_WORLD_PICKER_H
/* The picker never grants a licence or imports material. It selects exact
 * retained references for the existing revisioned, account-scoped World API.
 * Saved references are private to the authenticated self actor; source rows
 * require an observed published source. */
static int aw_picker_source(const char *source){
    if(!source)return 0;
    size_t n=strlen(source);
    return n>0&&n<=94&&strspn(source,"abcdefghijklmnopqrstuvwxyz0123456789_.:-")==n;
}
static int aw_picker_ref(const char *ref){
    if(!ref)return 0;
    const char *slash=strchr(ref,'/');
    if(!slash||slash==ref||strchr(slash+1,'/')||strlen(slash+1)!=24)return 0;
    char source[96];
    size_t n=(size_t)(slash-ref);if(n>=sizeof(source))return 0;
    memcpy(source,ref,n);source[n]=0;
    return aw_picker_source(source)&&strspn(slash+1,"0123456789abcdef")==24;
}
static void aw_picker_card(const char *site,sqlite3_int64 world,const char *space,long long revision,
                           int position,const char *ref,const char *title,const char *by,
                           const char *kind,const char *context){
    if(!aw_picker_ref(ref)||!title||!*title)return;
    fputs("<li class=\"aw-picker-card\" data-world-picker-card><div class=\"aw-picker-copy\"><strong>",stdout);
    html(title);fputs("</strong>",stdout);
    if(by&&*by){fputs("<small>",stdout);html(by);fputs("</small>",stdout);}
    if(context&&*context){fputs("<small>",stdout);html(context);fputs("</small>",stdout);}
    fputs("<a href=\"/o/",stdout);
    const char *slash=strchr(ref,'/');
    char source[96];size_t n=(size_t)(slash-ref);memcpy(source,ref,n);source[n]=0;
    page_url_escape(source);fputs("/",stdout);html(slash+1);
    fputs("\">Open retained item</a></div>",stdout);
    fputs("<form action=\"/workspace/world\" method=\"post\" data-world-picker-add>",stdout);
    world_web_hidden_base(site,world,space,"item",revision);
    char p[12];snprintf(p,sizeof(p),"%d",position);ph_hidden("position",p);
    ph_hidden("ref",ref);ph_hidden("role",kind&&*kind?kind:"feature");
    fputs("<button type=\"submit\">Add to draft</button></form></li>",stdout);
}
static void aw_picker_emit(sqlite3 *db,const char *site,sqlite3_int64 world,const char *space,
                           long long revision,int position,long uid,const char *selected,const char *query){
    if(!db||!site||!space||uid<1||position<1||position>AW_ITEM_LIMIT)return;
    fputs("<style>"
          ".aw-picker{display:grid;gap:14px;padding:20px 0}.aw-picker h3{font:400 24px/1.2 Georgia,serif;margin:0}"
          ".aw-picker p{max-width:75ch;font:14px/1.55 system-ui}"
          ".aw-picker ul{list-style:none;margin:0;padding:0;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,260px),1fr));gap:12px}"
          ".aw-picker-card{padding:17px;border:1px solid var(--ph-line);border-radius:13px;background:#fffef8;display:flex;flex-direction:column;gap:12px;justify-content:space-between}"
          ".aw-picker-copy{display:grid;gap:7px;min-width:0}.aw-picker-copy strong{font:400 20px/1.3 Georgia,serif}"
          ".aw-picker-copy small{font:12px/1.5 system-ui;color:var(--ph-muted)}"
          ".aw-picker-copy a{font:600 12px system-ui;text-decoration:underline;text-underline-offset:3px}"
          ".aw-picker-card form{margin:0}.aw-picker-card button{width:100%}"
          ".aw-picker-source{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,205px),1fr));gap:9px}"
          ".aw-picker-source a{display:grid;gap:6px;padding:14px;background:#fffef8;border:1px solid var(--ph-line);border-radius:11px;text-decoration:none}"
          ".aw-picker-source small{color:var(--ph-muted);font-size:12px}"
          ".aw-picker-search{display:flex!important;flex-wrap:wrap;gap:9px!important;align-items:end}"
          ".aw-picker-search label{flex:1;min-width:180px}"
          ".aw-picker a:focus-visible,.aw-picker button:focus-visible,.aw-picker input:focus-visible{outline:2px solid #49774a;outline-offset:2px}"
          "</style>",stdout);
    fputs("<section class=\"aw-picker\" data-world-source-picker><h3>Choose from your library</h3>"
          "<p>Use something you saved, or browse a retained source. The selection stays in this draft until you explicitly publish the room. Its original rights, authorship and availability remain with the source.</p>",stdout);
    char actor[56];snprintf(actor,sizeof(actor),"user:%ld",uid);
    sqlite3_stmt *st=NULL;int found=0;
    if(sqlite3_prepare_v2(db,
        "SELECT item_ref,title,COALESCE(publisher,''),COALESCE(collection,'') "
        "FROM saved_item WHERE actor=?1 AND status='published' "
        "ORDER BY id DESC LIMIT 30",-1,&st,NULL)==SQLITE_OK){
        sqlite3_bind_text(st,1,actor,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(st)==SQLITE_ROW&&found<12){
            const char *ref=(const char*)sqlite3_column_text(st,0);
            if(!aw_picker_ref(ref))continue;
            if(!found++)fputs("<h4>Your private saved references</h4><ul data-world-saved-picks>",stdout);
            aw_picker_card(site,world,space,revision,position,ref,
                (const char*)sqlite3_column_text(st,1),(const char*)sqlite3_column_text(st,2),
                "feature",(const char*)sqlite3_column_text(st,3));
        }
    }
    sqlite3_finalize(st);st=NULL;if(found)fputs("</ul>",stdout);
    else fputs("<p>No retained references are saved in this account yet. You can choose items from a published source below.</p>",stdout);
    fputs("<h4>Browse observed sources</h4><form method=\"get\" action=\"/workspace/worlds\" class=\"aw-picker-search\">",stdout);
    ph_hidden("site",site);char number[24];snprintf(number,sizeof(number),"%lld",(long long)world);ph_hidden("world",number);ph_hidden("space",space);
    fputs("<label>Search publisher, topic, or source<input name=\"q\" type=\"search\" maxlength=\"95\" value=\"",stdout);
    html(query?query:"");fputs("\" placeholder=\"Find retained sources\"></label><button type=\"submit\">Search</button></form>",stdout);
    found=0;
    if(sqlite3_prepare_v2(db,
        "SELECT source,label,family FROM internet_source WHERE namespace='root' AND status='published' "
        "AND state='observed' AND COALESCE(twin,'')!='' AND "
        "(?1='' OR instr(lower(COALESCE(source,'')||' '||COALESCE(label,'')||' '||COALESCE(family,'')),lower(?1))>0) "
        "ORDER BY id DESC LIMIT 24",-1,&st,NULL)==SQLITE_OK){
        sqlite3_bind_text(st,1,query?query:"",-1,SQLITE_TRANSIENT);
        while(sqlite3_step(st)==SQLITE_ROW){
            const char *source=(const char*)sqlite3_column_text(st,0);
            if(!aw_picker_source(source))continue;
            if(!found++)fputs("<ul class=\"aw-picker-source\" data-world-observed-sources>",stdout);
            fputs("<li><a href=\"/workspace/worlds?site=",stdout);url_query_value(site);
            printf("&amp;world=%lld&amp;space=",(long long)world);url_query_value(space);
            fputs("&amp;source=",stdout);url_query_value(source);fputs("\"><strong>",stdout);
            const char *label=(const char*)sqlite3_column_text(st,1),*family=(const char*)sqlite3_column_text(st,2);
            html(label&&*label?label:source);fputs("</strong><small>",stdout);html(family?family:"Source");fputs("</small></a></li>",stdout);
        }
    }
    sqlite3_finalize(st);st=NULL;
    if(found)fputs("</ul>",stdout);else fputs("<p>No currently observed sources match this search.</p>",stdout);
    if(selected&&*selected){
        int admitted=0;
        if(sqlite3_prepare_v2(db,
            "SELECT 1 FROM internet_source WHERE namespace='root' AND status='published' "
            "AND state='observed' AND COALESCE(twin,'')!='' AND source=?1 LIMIT 1",-1,&st,NULL)==SQLITE_OK){
            sqlite3_bind_text(st,1,selected,-1,SQLITE_TRANSIENT);admitted=sqlite3_step(st)==SQLITE_ROW;
        }
        sqlite3_finalize(st);
        if(!admitted)fputs("<p>This source is not currently available for selection.</p>",stdout);
        else{
            PageMaterial *material=calloc(1,sizeof(*material));
            if(material){
                page_collect_stories(db,selected,material);
                const PageStream *mix=&material->stream[BF_PAGE_MATERIAL_MIX];int shown=0;
                for(int i=0;i<mix->count&&shown<18;i++){
                    const PageItem *item=&mix->item[i];
                    if(strcmp(item->source,selected)||strlen(item->key)!=24||
                       strspn(item->key,"0123456789abcdef")!=24||!item->title[0])continue;
                    char ref[160];snprintf(ref,sizeof(ref),"%s/%s",item->source,item->key);
                    if(!shown++)fputs("<h4>Current material from this source</h4><ul data-world-source-picks>",stdout);
                    const char *kind=item->kind==BF_PAGE_MATERIAL_AUDIO?"track":
                                     item->kind==BF_PAGE_MATERIAL_VIDEO?"screening":
                                     item->kind==BF_PAGE_MATERIAL_PHOTO?"artwork":"feature";
                    aw_picker_card(site,world,space,revision,position,ref,item->title,item->by,
                                   kind,item->rights);
                }
                if(shown)fputs("</ul>",stdout);
                else fputs("<p>No currently retained pieces are available from this source.</p>",stdout);
                free(material);
            }else fputs("<p>The source reader is unavailable. Your draft has not changed.</p>",stdout);
        }
    }
    fputs("</section>",stdout);
}
#endif
