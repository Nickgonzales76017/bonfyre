#ifndef BONFYRE_ACCOUNT_WORLD_PICKER_H
#define BONFYRE_ACCOUNT_WORLD_PICKER_H
#include "form_tree.h"
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
    ui_open_a(stdout,"li","class","aw-picker-card","data-world-picker-card",(const char *)NULL); ui_open(stdout,"div","aw-picker-copy"); ui_open(stdout,"strong",NULL);
    html(title);ui_close(stdout,"strong");
    if(by&&*by){ui_text(stdout, "small", NULL, by);}
    if(context&&*context){ui_text(stdout, "small", NULL, context);}
    ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/o/");
    const char *slash=strchr(ref,'/');
    char source[96];size_t n=(size_t)(slash-ref);memcpy(source,ref,n);source[n]=0;
    page_url_escape(source);ui_copy(stdout,"/");html(slash+1);
    ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Open retained item"); ui_close(stdout,"a"); ui_close(stdout,"div");
    ui_open_a(stdout,"form","action","/workspace/world","method","post","data-world-picker-add",(const char *)NULL);
    world_web_hidden_base(site,world,space,"item",revision);
    char p[12];snprintf(p,sizeof(p),"%d",position);ph_hidden("position",p);
    ph_hidden("ref",ref);ph_hidden("role",kind&&*kind?kind:"feature");
    ui_open_a(stdout,"button","type","submit"); fputs("Add to draft",stdout); ui_close(stdout,"button"); ui_close(stdout,"form"); ui_close(stdout,"li");
}
static void aw_picker_emit(sqlite3 *db,const char *site,sqlite3_int64 world,const char *space,
                           long long revision,int position,long uid,const char *selected,const char *query){
    if(!db||!site||!space||uid<1||position<1||position>AW_ITEM_LIMIT)return;

    ui_open_a(stdout,"section","class","aw-picker","data-world-source-picker",(const char *)NULL); ui_open(stdout,"h3",NULL); fputs("Choose from your library",stdout); ui_close(stdout,"h3"); ui_open(stdout,"p",NULL); fputs("Use something you saved, or browse a retained source. The selection stays in this draft until you explicitly publish the room. Its original rights, authorship and availability remain with the source.",stdout); ui_close(stdout,"p");
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
            if(!found++)do{ ui_open(stdout,"h4",NULL); fputs("Your private saved references",stdout); ui_close(stdout,"h4"); ui_open_a(stdout,"ul","data-world-saved-picks",(const char *)NULL); }while(0);
            aw_picker_card(site,world,space,revision,position,ref,
                (const char*)sqlite3_column_text(st,1),(const char*)sqlite3_column_text(st,2),
                "feature",(const char*)sqlite3_column_text(st,3));
        }
    }
    sqlite3_finalize(st);st=NULL;if(found)ui_close(stdout,"ul");
    else ui_text(stdout, "p", NULL, "No retained references are saved in this account yet. You can choose items from a published source below.");
    ui_text(stdout, "h4", NULL, "Browse observed sources");
    {char number[24];snprintf(number,sizeof(number),"%lld",(long long)world);
     const FtField hidden[]={{"site",site},{"world",number},{"space",space}};
     const FtInput find[]={{.label="Search publisher, topic, or source",.type="search",.name="q",.maxlength="95",.value=query?query:"",.placeholder="Find retained sources"}};
     const FtForm f={.id="aw-picker-search",.method="get",.action="/workspace/worlds",.label="Search",.hook="aw-picker-search",.hidden=hidden,.nhidden=3,.inputs=find,.ninputs=1};
     ft_form(stdout,&f);}
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
            if(!found++)ui_open_a(stdout,"ul","class","aw-picker-source","data-world-observed-sources",(const char *)NULL);
            ui_open(stdout,"li",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");
            ui_copy(stdout,"/workspace/worlds?site=");url_query_value(site);
            printf("&amp;world=%lld&amp;space=",(long long)world);url_query_value(space);
            ui_copy(stdout,"&amp;source=");url_query_value(source);ui_attr_close(stdout);ui_gt(stdout);
            ui_open(stdout,"strong",NULL);
            const char *label=(const char*)sqlite3_column_text(st,1),*family=(const char*)sqlite3_column_text(st,2);
            html(label&&*label?label:source);ui_close(stdout,"strong"); ui_open(stdout,"small",NULL);html(family?family:"Source");ui_close(stdout,"small"); ui_close(stdout,"a"); ui_close(stdout,"li");
        }
    }
    sqlite3_finalize(st);st=NULL;
    if(found)ui_close(stdout,"ul");else ui_text(stdout, "p", NULL, "No currently observed sources match this search.");
    if(selected&&*selected){
        int admitted=0;
        if(sqlite3_prepare_v2(db,
            "SELECT 1 FROM internet_source WHERE namespace='root' AND status='published' "
            "AND state='observed' AND COALESCE(twin,'')!='' AND source=?1 LIMIT 1",-1,&st,NULL)==SQLITE_OK){
            sqlite3_bind_text(st,1,selected,-1,SQLITE_TRANSIENT);admitted=sqlite3_step(st)==SQLITE_ROW;
        }
        sqlite3_finalize(st);
        if(!admitted)ui_text(stdout, "p", NULL, "This source is not currently available for selection.");
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
                    if(!shown++)do{ ui_open(stdout,"h4",NULL); fputs("Current material from this source",stdout); ui_close(stdout,"h4"); ui_open_a(stdout,"ul","data-world-source-picks",(const char *)NULL); }while(0);
                    const char *kind=item->kind==BF_PAGE_MATERIAL_AUDIO?"track":
                                     item->kind==BF_PAGE_MATERIAL_VIDEO?"screening":
                                     item->kind==BF_PAGE_MATERIAL_PHOTO?"artwork":"feature";
                    aw_picker_card(site,world,space,revision,position,ref,item->title,item->by,
                                   kind,item->rights);
                }
                if(shown)ui_close(stdout,"ul");
                else ui_text(stdout, "p", NULL, "No currently retained pieces are available from this source.");
                free(material);
            }else ui_text(stdout, "p", NULL, "The source reader is unavailable. Your draft has not changed.");
        }
    }
    ui_close(stdout,"section");
}
#endif
