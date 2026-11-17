#ifndef BONFYRE_PRODUCTION_PUBLICATION_H
#define BONFYRE_PRODUCTION_PUBLICATION_H
#include "ui_tree.h"
/* Read-only projection of explicitly published CMS page aggregates. Private
 * workspace records, source bodies, account links and grants are never read. */
typedef struct {char kind[16],label[641],role[16];} BfProductionPanel;
typedef struct {int from,to;} BfProductionEdge;
typedef struct {char ns[128],slug[72],title[801],description[16001],layout[16],appearance[16];BfProductionPanel panels[4];BfProductionEdge edges[4];int count,edge_count;} BfPublicProduction;
static int production_reference(const char *ref,char *ns,char *slug){
    const char *slash=ref?strchr(ref,'/'):NULL;size_t n=slash?(size_t)(slash-ref):0;
    if(!n||n>127||strspn(ref,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")!=n||strlen(slash+1)!=71||strncmp(slash+1,"design-",7)||strspn(slash+8,"abcdef0123456789")!=64)return 0;
    memcpy(ns,ref,n);ns[n]=0;memcpy(slug,slash+1,72);return 1;
}
static int production_text(sqlite3_stmt *s,int col,char *out,size_t cap){
    const char *v=(const char*)sqlite3_column_text(s,col);int n=sqlite3_column_bytes(s,col);
    if(sqlite3_column_type(s,col)!=SQLITE_TEXT||!v||n<0||(size_t)n>=cap||strlen(v)!=(size_t)n)return 0;
    for(int i=0;i<n;i++)if((unsigned char)v[i]<32&&v[i]!='\n'&&v[i]!='\t'&&v[i]!='\r')return 0;
    memcpy(out,v,(size_t)n+1);return 1;
}
static int production_read(sqlite3 *db,const char *ns,const char *slug,BfPublicProduction *out){
    sqlite3_stmt *s=NULL;char route[80],*design=NULL;int ok=0,step;
    memset(out,0,sizeof(*out));snprintf(route,sizeof(route),"/%s",slug);
    const char *sql="SELECT f.projection_contract FROM site_page p JOIN site_fragment f ON f.namespace=p.namespace AND f.page=p.route WHERE p.namespace=?1 AND p.route=?2 AND p.status='published' AND p.machine_view='production-design-v1' AND f.status='published' AND f.audience='anonymous' AND f.kind='production-design' AND f.slot='body' LIMIT 2";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,route,-1,SQLITE_STATIC);
    if(sqlite3_step(s)!=SQLITE_ROW||sqlite3_column_bytes(s,0)>30000)goto done;
    const char *contract=(const char*)sqlite3_column_text(s,0);if(!contract)goto done;design=strdup(contract);if(!design||sqlite3_step(s)!=SQLITE_DONE)goto done;
    sqlite3_finalize(s);s=NULL;
    if(sqlite3_prepare_v2(db,"SELECT json_valid(?1)",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,design,-1,SQLITE_STATIC);if(sqlite3_step(s)!=SQLITE_ROW||!sqlite3_column_int(s,0))goto done;sqlite3_finalize(s);s=NULL;
    sql="SELECT json_extract(?1,'$.design.title'),json_extract(?1,'$.design.description'),json_extract(?1,'$.design.layout'),json_extract(?1,'$.design.appearance'),json_array_length(?1,'$.design.panels'),json_array_length(?1,'$.design.connections') FROM (SELECT 1) WHERE json_extract(?1,'$.html')=1 AND json_type(?1,'$.design')='object' AND json_extract(?1,'$.design.schema')='bonfyre.public-production-design.v1' AND (SELECT count(*) FROM json_each(?1,'$.design'))=7 AND json_type(?1,'$.design.panels')='array' AND json_type(?1,'$.design.connections')='array' AND NOT EXISTS(SELECT 1 FROM json_tree(?1) WHERE parent IS NOT NULL GROUP BY parent,key HAVING count(*)>1)";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(s,1,design,-1,SQLITE_STATIC);
    if(sqlite3_step(s)!=SQLITE_ROW||!production_text(s,0,out->title,sizeof(out->title))||!out->title[0]||!production_text(s,1,out->description,sizeof(out->description))||!production_text(s,2,out->layout,sizeof(out->layout))||!production_text(s,3,out->appearance,sizeof(out->appearance)))goto done;
    out->count=sqlite3_column_int(s,4);out->edge_count=sqlite3_column_int(s,5);
    if(out->count<1||out->count>4||out->edge_count<0||out->edge_count>4||(strcmp(out->layout,"columns")&&strcmp(out->layout,"stack")&&strcmp(out->layout,"focus"))||(strcmp(out->appearance,"paper")&&strcmp(out->appearance,"night")))goto done;
    sqlite3_finalize(s);s=NULL;
    sql="SELECT json_extract(value,'$.kind'),json_extract(value,'$.label'),json_extract(value,'$.role'),(SELECT count(*) FROM json_each(e.value)) FROM json_each(?1,'$.design.panels') e ORDER BY CAST(key AS INTEGER)";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(s,1,design,-1,SQLITE_STATIC);int n=0;
    while((step=sqlite3_step(s))==SQLITE_ROW){if(n>=out->count)goto done;BfProductionPanel *p=&out->panels[n++];if(sqlite3_column_int(s,3)!=3||!production_text(s,0,p->kind,sizeof(p->kind))||!production_text(s,1,p->label,sizeof(p->label))||!p->label[0]||!production_text(s,2,p->role,sizeof(p->role)))goto done;if(strcmp(p->kind,"research")&&strcmp(p->kind,"product")&&strcmp(p->kind,"traces")&&strcmp(p->kind,"schema"))goto done;if(strcmp(p->role,"source")&&strcmp(p->role,"working")&&strcmp(p->role,"output"))goto done;}
    if(step!=SQLITE_DONE||n!=out->count)goto done;sqlite3_finalize(s);s=NULL;
    sql="SELECT json_extract(value,'$.from'),json_extract(value,'$.to'),json_extract(value,'$.operation'),(SELECT count(*) FROM json_each(e.value)) FROM json_each(?1,'$.design.connections') e ORDER BY CAST(key AS INTEGER)";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(s,1,design,-1,SQLITE_STATIC);n=0;unsigned used=0;
    while((step=sqlite3_step(s))==SQLITE_ROW){char operation[64];if(n>=out->edge_count||sqlite3_column_type(s,0)!=SQLITE_INTEGER||sqlite3_column_type(s,1)!=SQLITE_INTEGER||sqlite3_column_int(s,3)!=3||!production_text(s,2,operation,sizeof(operation))||strcmp(operation,"research-opportunity"))goto done;sqlite3_int64 av=sqlite3_column_int64(s,0),bv=sqlite3_column_int64(s,1);if(av<0||bv<0||av>=out->count||bv>=out->count)goto done;int a=(int)av,b=(int)bv;if((used&(1u<<a))||strcmp(out->panels[a].kind,"research")||strcmp(out->panels[b].kind,"product"))goto done;used|=1u<<a;out->edges[n++]=(BfProductionEdge){a,b};}
    if(step!=SQLITE_DONE||n!=out->edge_count)goto done;snprintf(out->ns,sizeof(out->ns),"%s",ns);snprintf(out->slug,sizeof(out->slug),"%s",slug);ok=1;
 done:sqlite3_finalize(s);free(design);return ok;
}
static const char *production_label(const char *kind){return !strcmp(kind,"research")?"Source reading":!strcmp(kind,"product")?"Product decisions":!strcmp(kind,"schema")?"Database rehearsal":"Observed process";}
static void production_response(int status,const char *type){printf("HTTP/1.1 %d %s\r\nContent-Type: %s\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: same-origin\r\nConnection: close\r\n\r\n",status,status==200?"OK":status==404?"Not Found":"Bad Request",type);}
static void production_card(const BfPublicProduction *p,int detail){
    ui_start(stdout,"article");ui_attr(stdout,"class","production-edition");ui_flag(stdout,"data-production-design");ui_gt(stdout);ui_open(stdout,"header",NULL);ui_open(stdout,"span","production-kicker");ui_copy(stdout,"Authored / reusable production");ui_close(stdout,"span");ui_open(stdout,"h3",NULL);html(p->title);ui_close(stdout,"h3");ui_open(stdout,"span","production-kicker");html(p->layout);fputs(" · ",stdout);html(p->appearance);ui_close(stdout,"span");ui_close(stdout,"header");ui_open(stdout,"div","production-copy");ui_open(stdout,"p",NULL);html(p->description);ui_close(stdout,"p");ui_open(stdout,"ol","production-outline-preview");
    for(int i=0;i<p->count;i++){ui_open(stdout,"li",NULL);ui_open(stdout,"b",NULL);fprintf(stdout,"%02d",i+1);ui_close(stdout,"b");ui_open(stdout,"span",NULL);html(p->panels[i].label);ui_open(stdout,"small",NULL);html(production_label(p->panels[i].kind));fputs(" · ",stdout);html(p->panels[i].role);ui_close(stdout,"small");ui_close(stdout,"span");ui_close(stdout,"li");}ui_close(stdout,"ol");
    for(int i=0;i<p->edge_count;i++){ui_open(stdout,"p","production-handoff");html(p->panels[p->edges[i].from].label);fputs(" → ",stdout);html(p->panels[p->edges[i].to].label);ui_open(stdout,"br",NULL);ui_copy(stdout,"Reviewed finding → editable opportunity");ui_close(stdout,"p");}
    ui_text(stdout, "p", "production-scope", "Reuse the arrangement and supported handoffs. Bring your own sources; private material, membership and credentials are not included.");
    if(!detail){ui_start(stdout,"a");ui_flag(stdout,"data-production-inspect");ui_attr_open(stdout,"href");ui_copy(stdout,"/productions/");html(p->ns);fputs("/",stdout);html(p->slug);fputs("\">Inspect this production ↗</a>",stdout);}
    ui_start(stdout,"a");ui_flag(stdout,"data-production-reuse");ui_attr_open(stdout,"href");ui_copy(stdout,"/cms/productions?context=self&amp;design=");html(p->ns);fputs("/",stdout);html(p->slug);fputs("\">Use this design →</a></div></article>",stdout);
}
static int serve_production_publication(const BfRequest *r) {
    const char *method=r->method; const char *target=r->target; const char *query=r->query;
    (void)method;(void)target;(void)query;
    if(bf_request_is(r,"script")){
        if(strcmp(method,"GET")||(query&&*query)){production_response(400,"text/plain; charset=utf-8");fputs("Use the public script address without parameters.",stdout);return 1;}
        production_response(200,"application/javascript; charset=utf-8");
        fputs(BF_PUBLIC_PRODUCTION_HOME_SCRIPT,stdout);return 1;
    }
    int fragment=bf_request_is(r,"fragment"),detail=bf_request_is(r,"detail");
if(strcmp(method,"GET")||(query&&*query)){production_response(400,"text/plain; charset=utf-8");fputs("Use the published production address without parameters.",stdout);return 1;}
    sqlite3 *db=cms_open_ro();if(!db||sqlite3_exec(db,"BEGIN",NULL,NULL,NULL)!=SQLITE_OK){if(db)sqlite3_close(db);production_response(503,"text/plain; charset=utf-8");fputs("Published productions are unavailable.",stdout);return 1;}
    if(detail){char ref[208],ns[128],slug[72];size_t n=strlen(target+13);int packet=n>7&&!strcmp(target+strlen(target)-7,"/design");if(packet)n-=7;BfPublicProduction p;int ok=n<sizeof(ref);if(ok){memcpy(ref,target+13,n);ref[n]=0;ok=production_reference(ref,ns,slug)&&production_read(db,ns,slug,&p);}sqlite3_exec(db,"COMMIT",NULL,NULL,NULL);sqlite3_close(db);
        if(!ok){production_response(404,"text/plain; charset=utf-8");fputs("This production is not currently published or its design is unsupported.",stdout);return 1;}
        if(packet){bf_mp_writer_t w;bf_mp_writer_init(&w,2048);ok=!bf_mp_pack_array(&w,7)&&!bf_mp_pack_uint(&w,1)&&!bf_mp_pack_str_cstr(&w,p.title)&&!bf_mp_pack_str_cstr(&w,p.description)&&!bf_mp_pack_str_cstr(&w,p.layout)&&!bf_mp_pack_str_cstr(&w,p.appearance)&&!bf_mp_pack_array(&w,(uint32_t)p.count);for(int i=0;i<p.count&&ok;i++)ok=!bf_mp_pack_array(&w,3)&&!bf_mp_pack_str_cstr(&w,p.panels[i].kind)&&!bf_mp_pack_str_cstr(&w,p.panels[i].label)&&!bf_mp_pack_str_cstr(&w,p.panels[i].role);if(ok)ok=!bf_mp_pack_array(&w,(uint32_t)p.edge_count);for(int i=0;i<p.edge_count&&ok;i++)ok=!bf_mp_pack_array(&w,3)&&!bf_mp_pack_uint(&w,(uint64_t)p.edges[i].from)&&!bf_mp_pack_uint(&w,(uint64_t)p.edges[i].to)&&!bf_mp_pack_str_cstr(&w,"research-opportunity");if(ok){production_response(200,"application/msgpack");fwrite(w.buf,1,w.len,stdout);}else production_response(400,"text/plain");bf_mp_writer_free(&w);return 1;}
        production_response(200,"text/html; charset=utf-8");ak_app_open(p.title,"workbench","route-productions");ui_open(stdout,"div","production-gallery");ui_open(stdout,"p","ak-kicker");ui_open_a(stdout,"a","href","/productions");ui_copy(stdout,"\xe2\x86\x90 All productions");ui_close(stdout,"a");ui_close(stdout,"p");ui_open(stdout,"h1",NULL);ui_copy(stdout,"A working arrangement, ready for your material.");ui_close(stdout,"h1");production_card(&p,1);ui_close(stdout,"div");ak_app_close();return 1;
    }
    BfPublicProduction *items=calloc(24,sizeof(*items));sqlite3_stmt *s=NULL;int count=0,step=SQLITE_DONE;
    if(!items||sqlite3_prepare_v2(db,"SELECT namespace,substr(route,2) FROM site_page WHERE status='published' AND machine_view='production-design-v1' ORDER BY updated_at DESC,id DESC LIMIT 128",-1,&s,NULL)!=SQLITE_OK){
        sqlite3_finalize(s);sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);sqlite3_close(db);free(items);production_response(503,"text/plain; charset=utf-8");fputs("The published design catalogue could not be read completely.",stdout);return 1;}
    while(count<(fragment?6:24)&&(step=sqlite3_step(s))==SQLITE_ROW){const char *ns=(const char*)sqlite3_column_text(s,0),*slug=(const char*)sqlite3_column_text(s,1);char ref[208],a[128],b[72];if(ns&&slug&&snprintf(ref,sizeof(ref),"%s/%s",ns,slug)<(int)sizeof(ref)&&production_reference(ref,a,b)&&production_read(db,a,b,&items[count]))count++;}
    if(step!=SQLITE_DONE&&step!=SQLITE_ROW){sqlite3_finalize(s);sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);sqlite3_close(db);free(items);production_response(503,"text/plain; charset=utf-8");fputs("The published design catalogue read was interrupted.",stdout);return 1;}
    sqlite3_finalize(s);sqlite3_exec(db,"COMMIT",NULL,NULL,NULL);sqlite3_close(db);production_response(200,"text/html; charset=utf-8");
    if(!fragment)ak_app_open("Productions","workbench","route-productions");
    ui_start(stdout,"section");ui_attr(stdout,"class","production-gallery");ui_flag(stdout,"data-public-productions");ui_gt(stdout);ui_open(stdout,"p","production-kicker");ui_copy(stdout,"Made with Aurekai / reusable work");ui_close(stdout,"p");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Start with a production.");ui_open(stdout,"br",NULL);ui_copy(stdout,"Make it your own.");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Inspect an authored arrangement and its supported handoffs. Then bring your own material into the working editors. Showing the latest published designs, up to 24 per catalogue view.");ui_close(stdout,"p");ui_open(stdout,"div","production-editions");
    for(int i=0;i<count;i++)production_card(&items[i],0);if(!count)ui_text(stdout, "p", NULL, "No reusable productions have been published here yet. Your private working compositions are not listed.");free(items);ui_close(stdout,"div");ui_start(stdout,"a");ui_attr(stdout,"href","/cms/productions?context=self");ui_gt(stdout);ui_copy(stdout,"Author a production →");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"href","/productions");ui_gt(stdout);ui_copy(stdout,"Explore published designs ↗");ui_close(stdout,"a");ui_close(stdout,"section");if(!fragment)ak_app_close();return 1;
}
#endif
