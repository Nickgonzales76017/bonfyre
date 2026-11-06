#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include <sqlite3.h>
#include "bf_msgpack.h"
#include "bonfyre.h"
/* Explicit direct command: immutable SourceFrontier item and joined native
 * Finance/CRM data enter existing user:19 workspaces only through the CMS
 * pack-workspace owner. It checks installed release, actor, scope, CAS and
 * declared key family. No operator admission, invoice or external effect. */
#define USER "user:19"
#define SRC_BIND "ws-263b80ea03eb96c910a5422a190b34bc63e09ed2"
#define FIN_BIND "ws-7420da15754d3b1a70b6a07213ea37254bb17393"
#define CRM_BIND "ws-e67ee1d400acf6ecdcf7ee3d385b45521159ed10"
#define SRC "feed-weworkremotely"
#define FEED_KEY "3242ae9381d6436b35ee5b7f"
#define FIN_KEY "finance-workbook:commercial-opportunity-index"
#define MAX_ROWS 400
static int k(bf_mp_writer_t *w,const char *v){return !bf_mp_pack_str_cstr(w,v);}
static int kv(bf_mp_writer_t *w,const char *name,const char *v){return !bf_mp_pack_kv_str(w,name,v?v:"");}
static int kn(bf_mp_writer_t *w,const char *name,long long v){return !bf_mp_pack_kv_int(w,name,v);}
static int empty(bf_mp_writer_t *w,const char *name,int map){return k(w,name)&&!(map?bf_mp_pack_map(w,0):bf_mp_pack_array(w,0));}
static int nil(bf_mp_writer_t *w,const char *name){return k(w,name)&&!bf_mp_pack_nil(w);}
static int cell(bf_mp_writer_t *w,int col,const char *v){
    char n[12];snprintf(n,sizeof(n),"%d",col);
    return k(w,n)&&!bf_mp_pack_map(w,2)&&kn(w,"t",1)&&kv(w,"v",v);
}
static const char *txt(sqlite3_stmt *s,int col){
    const unsigned char *p=sqlite3_column_text(s,col);return p?(const char*)p:"";
}
static int dbopen(const char *path,sqlite3 **db){
    *db=NULL;
    if(sqlite3_open_v2(path,db,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK)return 1;
    if(*db)sqlite3_close(*db);
    *db=NULL;return 0;
}
/* 0 absent, 1 existing different, 2 byte-identical; negative unreadable. */
static int readback(sqlite3 *db,const char *binding,const char *key,const char *digest,int *revision){
    sqlite3_stmt *q=NULL;int state=-1;
    const char *sql="SELECT r.revision,r.body_digest,r.deleted FROM _workspace_record r JOIN application_binding a ON a.continuity_key=r.scope_key WHERE a.binding_id=?1 AND a.status='active' AND r.record_key=?2";
    if(sqlite3_prepare_v2(db,sql,-1,&q,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(q,1,binding,-1,SQLITE_STATIC);
    sqlite3_bind_text(q,2,key,-1,SQLITE_STATIC);
    int step=sqlite3_step(q);
    if(step==SQLITE_DONE)state=0;
    else if(step==SQLITE_ROW){
        *revision=sqlite3_column_int(q,0);
        const char *kept=txt(q,1);
        state=sqlite3_column_int(q,2)?-1:(digest&&strcmp(kept,digest)==0?2:1);
        if(sqlite3_step(q)!=SQLITE_DONE)state=-1;
    }
    sqlite3_finalize(q);return state;
}
static int save(const char *cmsbin,const char *cmsdb,sqlite3 *db,const char *binding,const char *key,const bf_mp_writer_t *value,int *revision){
    char digest[65],req[65],seed[900];bf_sha256_hex(value->buf,value->len,digest);
    int seen=readback(db,binding,key,digest,revision);
    if(seen==2){printf("{\"result\":\"already_saved\",\"record\":\"%s\",\"revision\":%d}\n",key,*revision);return 1;}
    if(seen!=0){fprintf(stderr,"Existing or unreadable native record refused: %s (%d)\n",key,seen);return 0;}
    snprintf(seed,sizeof(seed),"%s\n%s\n%s",binding,key,digest);
    bf_sha256_hex((const uint8_t*)seed,strlen(seed),req);
    bf_mp_writer_t packet;bf_mp_writer_init(&packet,value->len+128);
    int valid=!bf_mp_pack_array(&packet,3)&&!bf_mp_pack_uint(&packet,1)&&!bf_mp_pack_str_cstr(&packet,req)
      &&!bf_mp_pack_array(&packet,1)&&!bf_mp_pack_array(&packet,4)&&!bf_mp_pack_str_cstr(&packet,key)
      &&!bf_mp_pack_uint(&packet,0)&&!bf_mp_pack_uint(&packet,0)
      &&!bf_mp_pack_bin(&packet,value->buf,(uint32_t)value->len);
    if(!valid||packet.len>300000){bf_mp_writer_free(&packet);return 0;}
    int pipefd[2];if(pipe(pipefd)!=0){bf_mp_writer_free(&packet);return 0;}
    pid_t child=fork();
    if(child==0){
        dup2(pipefd[0],STDIN_FILENO);close(pipefd[0]);close(pipefd[1]);
        int devnull=open("/dev/null",O_WRONLY);
        if(devnull>=0){dup2(devnull,STDOUT_FILENO);close(devnull);}
        char *const args[]={(char*)cmsbin,"pack-workspace","--operation","write",
          "--binding",(char*)binding,"--subject",USER,"--scope","self",
          "--actor",USER,"--db",(char*)cmsdb,NULL};
        execv(cmsbin,args);_exit(127);
    }
    close(pipefd[0]);if(child<0){close(pipefd[1]);bf_mp_writer_free(&packet);return 0;}
    signal(SIGPIPE,SIG_IGN);
    size_t total=packet.len,done=0;
    while(done<total){ssize_t n=write(pipefd[1],packet.buf+done,total-done);
        if(n>0)done+=(size_t)n;else if(n<0&&errno==EINTR)continue;else break;}
    close(pipefd[1]);int status=0;pid_t waited=waitpid(child,&status,0);
    bf_mp_writer_free(&packet);
    if(done!=total||waited!=child||!WIFEXITED(status)||WEXITSTATUS(status)!=0){
        fprintf(stderr,"Native CMS owner refused %s\n",key);return 0;
    }
    int persisted=readback(db,binding,key,digest,revision);
    if(persisted!=2||*revision!=1){fprintf(stderr,"Failed persisted digest readback for %s\n",key);return 0;}
    printf("{\"result\":\"saved_and_reopened\",\"binding\":\"%s\",\"record\":\"%s\",\"revision\":%d,\"sha256\":\"%s\"}\n",binding,key,*revision,digest);
    return 1;
}
static int source_record(sqlite3 *items,sqlite3 *cms,bf_mp_writer_t *w,char record_key[96]){
    sqlite3_stmt *q=NULL,*decl=NULL;
    if(sqlite3_prepare_v2(items,"SELECT title,href,at FROM item WHERE source=?1 AND key=?2 LIMIT 2",-1,&q,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(q,1,SRC,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,FEED_KEY,-1,SQLITE_STATIC);
    if(sqlite3_step(q)!=SQLITE_ROW){sqlite3_finalize(q);fputs("Retained Lithic source item absent\n",stderr);return 0;}
    char title[501],href[2049];snprintf(title,sizeof(title),"%s",txt(q,0));snprintf(href,sizeof(href),"%s",txt(q,1));
    time_t published=(time_t)sqlite3_column_int64(q,2);
    int unique=sqlite3_step(q)==SQLITE_DONE;sqlite3_finalize(q);
    if(!unique||published<=0||!title[0]||strlen(href)<12||
       (strncmp(href,"https://",8)&&strncmp(href,"http://",7))||strchr(href,'@'))return 0;
    if(sqlite3_prepare_v2(cms,"SELECT family,kind FROM internet_source WHERE source=?1 AND namespace='root' AND status='published' LIMIT 2",-1,&decl,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(decl,1,SRC,-1,SQLITE_STATIC);
    if(sqlite3_step(decl)!=SQLITE_ROW){sqlite3_finalize(decl);return 0;}
    char family[65],kind[65];snprintf(family,sizeof(family),"%s",txt(decl,0));snprintf(kind,sizeof(kind),"%s",txt(decl,1));
    unique=sqlite3_step(decl)==SQLITE_DONE;sqlite3_finalize(decl);if(!unique||!family[0]||!kind[0])return 0;
    char hash[65],date[24];bf_sha256_hex((const uint8_t*)href,strlen(href),hash);
    struct tm at;if(!gmtime_r(&published,&at)||!strftime(date,sizeof(date),"%Y-%m-%dT%H:%M:%SZ",&at))return 0;
    snprintf(record_key,96,"source-material:%s",hash);
    int valid=!bf_mp_pack_array(w,3)&&!bf_mp_pack_uint(w,2)&&!bf_mp_pack_map(w,13)
      &&kv(w,"schema","bonfyre.source-material.v1")&&kv(w,"id",hash)
      &&kv(w,"kind","link")&&kv(w,"state","active")&&kv(w,"title",title)
      &&kv(w,"collection","Feed handoffs")
      &&k(w,"tags")&&!bf_mp_pack_array(w,2)&&!bf_mp_pack_str_cstr(w,family)&&!bf_mp_pack_str_cstr(w,SRC)
      &&kv(w,"notes","Saved from a retained Aurekai feed item.")
      &&kv(w,"url",href)&&kv(w,"contentSHA256",hash)&&kv(w,"createdAt",date)
      &&empty(w,"history",0)&&k(w,"provenance")&&!bf_mp_pack_map(w,6)
      &&kv(w,"schema","bonfyre.feed-handoff.v1")
      &&kv(w,"itemRef","feed-weworkremotely/3242ae9381d6436b35ee5b7f")
      &&kv(w,"publicPath","/o/feed-weworkremotely/3242ae9381d6436b35ee5b7f")
      &&kv(w,"source",SRC)&&kv(w,"family",family)&&kv(w,"kind",kind)
      &&!bf_mp_pack_array(w,0);
    if(valid)fprintf(stderr,"source verified: %s (%s)\n",title,href);
    return valid&&w->len<=256*1024;
}
static int finance_record(sqlite3 *fin,sqlite3 *cms,bf_mp_writer_t *w,int *rows,int *deals){
    sqlite3_stmt *q=NULL,*crm=NULL;
    const char *sql="SELECT o.id,o.title,o.source_kind,o.stage,o.compensation_cents,o.currency,o.source_ref,o.source_digest,"
      "(SELECT COUNT(*) FROM opportunity_requirements r WHERE r.opportunity_id=o.id AND r.state<>'satisfied'),"
      "(SELECT COUNT(*) FROM opportunity_requirements r WHERE r.opportunity_id=o.id AND r.kind='material' AND r.requirement_key='cost_basis_confirmed' AND r.state='satisfied' AND r.evidence_ref NOT IN ('','-'))"
      " FROM opportunities o ORDER BY o.observed_at,o.id";
    if(sqlite3_prepare_v2(fin,sql,-1,&q,NULL)!=SQLITE_OK)return 0;
    if(sqlite3_prepare_v2(cms,"SELECT id,COALESCE(communication_status,'') FROM frappe_crm_crm_deal WHERE source=?1 ORDER BY id LIMIT 2",-1,&crm,NULL)!=SQLITE_OK){sqlite3_finalize(q);return 0;}
    int step,count=0,ok=0;
    while((step=sqlite3_step(q))==SQLITE_ROW){if(++count>MAX_ROWS)goto done;}
    if(step!=SQLITE_DONE||count<1)goto done;
    sqlite3_reset(q);*rows=count;*deals=0;
    static const char *heads[]={"Opportunity ID","Title","Source kind","Finance stage","Offer cents, not revenue","Currency","Cost basis","Open requirements","CRM deal ID","CRM communication","Source ref","Source digest"};
    ok=!bf_mp_pack_array(w,3)&&!bf_mp_pack_uint(w,2)&&!bf_mp_pack_map(w,8)
      &&kv(w,"schema","bonfyre.finance-workbook.v1")
      &&kv(w,"title","CRM and Finance opportunities — review only")
      &&k(w,"snapshot")&&!bf_mp_pack_map(w,7)
      &&kv(w,"id","aurekai-commercial-owners")
      &&kv(w,"name","Commercial opportunity review")&&kv(w,"appVersion","1.1.0")
      &&kv(w,"locale","enUS")
      &&k(w,"sheetOrder")&&!bf_mp_pack_array(w,1)&&!bf_mp_pack_str_cstr(w,"sheet-0")
      &&k(w,"sheets")&&!bf_mp_pack_map(w,1)&&k(w,"sheet-0")
      &&!bf_mp_pack_map(w,8)&&kv(w,"id","sheet-0")&&kv(w,"name","Opportunities")
      &&kn(w,"rowCount",count+20)&&kn(w,"columnCount",16)
      &&k(w,"cellData")&&!bf_mp_pack_map(w,(uint32_t)(count+1));
    if(!ok)goto done;
    for(int row=0;row<=count;row++){
        char row_key[16];snprintf(row_key,sizeof(row_key),"%d",row);
        if(!k(w,row_key)||bf_mp_pack_map(w,12)){ok=0;goto done;}
        if(row==0){for(int c=0;c<12;c++)if(!cell(w,c,heads[c])){ok=0;goto done;}continue;}
        if(sqlite3_step(q)!=SQLITE_ROW){ok=0;goto done;}
        char ident[256],price[64],deal[64]="",crm_status[500]="",needs[32];
        snprintf(ident,sizeof(ident),"%s",txt(q,0));
        if(sqlite3_bind_text(crm,1,ident,-1,SQLITE_TRANSIENT)!=SQLITE_OK){ok=0;goto done;}
        if(sqlite3_step(crm)==SQLITE_ROW){
            snprintf(deal,sizeof(deal),"%s",txt(crm,0));snprintf(crm_status,sizeof(crm_status),"%s",txt(crm,1));
            (*deals)++;if(sqlite3_step(crm)!=SQLITE_DONE){ok=0;goto done;}
        }
        sqlite3_reset(crm);sqlite3_clear_bindings(crm);
        if(sqlite3_column_type(q,4)==SQLITE_NULL)snprintf(price,sizeof(price),"Not stated");
        else snprintf(price,sizeof(price),"%lld",(long long)sqlite3_column_int64(q,4));
        snprintf(needs,sizeof(needs),"%d",sqlite3_column_int(q,8));
        const char *values[]={ident,txt(q,1),txt(q,2),txt(q,3),price,txt(q,5),
          sqlite3_column_int(q,9)?"Evidenced":"Not evidenced",needs,
          deal[0]?deal:"No matching deal",crm_status,txt(q,6),txt(q,7)};
        for(int c=0;c<12;c++)if(!cell(w,c,values[c])){ok=0;goto done;}
    }
    ok=empty(w,"rowData",1)&&empty(w,"columnData",1)&&kn(w,"hidden",0)
      &&empty(w,"styles",1)&&k(w,"source")&&!bf_mp_pack_map(w,4)
      &&kv(w,"schema","bonfyre.finance-source-review.v1")
      &&kv(w,"financeOwner","BonfyreFinance")
      &&kv(w,"crmOwner","BonfyreCMS/frappe_crm_crm_deal")
      &&kv(w,"limitations","Read-only evidence review; unpriced offers are unknown. No invoice, accepted scope, ledger posting, cash or payment.")
      &&nil(w,"review")&&nil(w,"scenarios")
      &&k(w,"calculation")&&!bf_mp_pack_map(w,2)
      &&kv(w,"engine","Univer 1.1.0")&&kn(w,"errors",0)
      &&kv(w,"status","review-draft")&&!bf_mp_pack_array(w,0);
done:
    sqlite3_finalize(q);sqlite3_finalize(crm);
    return ok&&w->len<=256*1024;
}
/* Exact publication_retained.h and relationship_observation.h evidence carrier. */
static int crm_record(sqlite3 *items,sqlite3 *cms,bf_mp_writer_t *w,char record_key[96]){
    sqlite3_stmt *q=NULL,*s=NULL;int valid=0;
    if(sqlite3_prepare_v2(items,"SELECT title,body,href,COALESCE(by,''),at FROM item WHERE source=?1 AND key=?2 LIMIT 2",-1,&q,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(q,1,SRC,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,FEED_KEY,-1,SQLITE_STATIC);
    if(sqlite3_step(q)!=SQLITE_ROW){fputs("CRM: exact retained item missing\n",stderr);goto done;}
    char title[768],body[2048],href[1024],by[256];
    snprintf(title,sizeof(title),"%s",txt(q,0));snprintf(body,sizeof(body),"%s",txt(q,1));
    snprintf(href,sizeof(href),"%s",txt(q,2));snprintf(by,sizeof(by),"%s",txt(q,3));
    time_t at=(time_t)sqlite3_column_int64(q,4);
    if(sqlite3_step(q)!=SQLITE_DONE||!title[0]||strlen(title)>300||at<=0||
      (strncmp(href,"https://",8)&&strncmp(href,"http://",7))){fprintf(stderr,"CRM: item validation title=%zu body=%zu at=%lld url=%s\n",strlen(title),strlen(body),(long long)at,href);goto done;}
    if(sqlite3_prepare_v2(cms,"SELECT family,kind,twin FROM internet_source WHERE source=?1 AND namespace='root' AND status='published' AND front_door='yes' AND state='observed' AND extract LIKE 'rss:%' LIMIT 2",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,SRC,-1,SQLITE_STATIC);
    if(sqlite3_step(s)!=SQLITE_ROW){fputs("CRM: published front-door feed contract absent\n",stderr);goto done;}
    char family[64],kind[64],twin[256];
    snprintf(family,sizeof(family),"%s",txt(s,0));snprintf(kind,sizeof(kind),"%s",txt(s,1));snprintf(twin,sizeof(twin),"%s",txt(s,2));
    if(sqlite3_step(s)!=SQLITE_DONE||!family[0]||!kind[0]||!twin[0]){fprintf(stderr,"CRM: ambiguous feed contract family=%s kind=%s twin=%s\n",family,kind,twin);goto done;}
    char date[24],bodyhash[65],material[65],snapshot[65],identity[65],receipt[480];
    struct tm utc;if(!gmtime_r(&at,&utc)||!strftime(date,sizeof(date),"%Y-%m-%dT%H:%M:%SZ",&utc))goto done;
    bf_sha256_hex((const uint8_t*)body,strlen(body),bodyhash);
    snprintf(receipt,sizeof(receipt),"public-material:%s/%s;body-sha256:%s",SRC,FEED_KEY,bodyhash);
    const char *ref="feed-weworkremotely/3242ae9381d6436b35ee5b7f";
    bf_mp_writer_t item,frame;bf_mp_writer_init(&item,4096);bf_mp_writer_init(&frame,1024);
    const char *pieces[]={FEED_KEY,title,body,href,SRC,receipt,
      "Source terms not supplied. Retained public excerpt; no reuse licence inferred.",
      "native normalized public excerpt",date,twin};
    int ok=!bf_mp_pack_array(&item,10);
    for(int i=0;i<10&&ok;i++)ok=!bf_mp_pack_str_cstr(&item,pieces[i]);
    if(ok)bf_sha256_hex(item.buf,item.len,material);
    if(ok)ok=!bf_mp_pack_array(&frame,4)&&!bf_mp_pack_str_cstr(&frame,material)
      &&!bf_mp_pack_str_cstr(&frame,by)&&!bf_mp_pack_str_cstr(&frame,family)&&!bf_mp_pack_str_cstr(&frame,kind);
    if(ok)bf_sha256_hex(frame.buf,frame.len,snapshot);
    bf_mp_writer_reset(&frame);
    if(ok){if(by[0]){ok=!bf_mp_pack_array(&frame,2)&&!bf_mp_pack_str_cstr(&frame,ref)&&!bf_mp_pack_str_cstr(&frame,by);
      if(ok)bf_sha256_hex(frame.buf,frame.len,identity);}
      else bf_sha256_hex((const uint8_t*)ref,strlen(ref),identity);}
    bf_mp_writer_free(&item);bf_mp_writer_free(&frame);
    if(!ok)goto done;
    snprintf(record_key,96,"opportunity:%s:%s",by[0]?"actor":"feed",identity);
    valid=!bf_mp_pack_array(w,3)&&!bf_mp_pack_uint(w,2)&&!bf_mp_pack_map(w,4)
      &&kv(w,"name",by[0]?by:title)&&kv(w,"saved_at",date)&&kv(w,"save_id",identity)
      &&k(w,"state")&&!bf_mp_pack_map(w,7)
      &&empty(w,"reqs",0)&&empty(w,"options",0)&&empty(w,"weightOverrides",1)
      &&empty(w,"poc",0)&&empty(w,"ops",1)&&empty(w,"own",1)
      &&k(w,"relationship")&&!bf_mp_pack_map(w,8)
      &&kv(w,"schema","bonfyre.relationship.v1")&&kv(w,"id",identity)
      &&kv(w,"state","observed")&&kv(w,"label",by[0]?by:title)
      &&kv(w,"createdAt",date)
      &&k(w,"actor")&&!bf_mp_pack_map(w,2)&&kv(w,"state","unresolved")&&nil(w,"subject")
      &&k(w,"source")&&!bf_mp_pack_map(w,11)
      &&kv(w,"itemRef",ref)&&kv(w,"publicPath","/o/feed-weworkremotely/3242ae9381d6436b35ee5b7f")
      &&kv(w,"source",SRC)&&kv(w,"family",family)&&kv(w,"kind",kind)
      &&kv(w,"title",title)&&kv(w,"summary",body)&&kv(w,"canonicalHref",href)
      &&kv(w,"authorName",by)&&kv(w,"snapshot",snapshot)
      &&kv(w,"evidenceKind",by[0]?"retained-source-credit":"retained-source-item")
      &&k(w,"opportunity")&&!bf_mp_pack_map(w,2)
      &&kv(w,"kind",!strcmp(family,"jobs")?"job-opening":"observed-mention")
      &&kv(w,"stage","observed")&&!bf_mp_pack_array(w,0);
    if(valid)fprintf(stderr,"CRM retained fingerprint %s\n",snapshot);
done:
    sqlite3_finalize(s);sqlite3_finalize(q);return valid&&w->len<=256*1024;
}
int main(int argc,char **argv){
    if(argc!=2||(strcmp(argv[1],"inspect")&&strcmp(argv[1],"apply"))){
        fputs("Usage: bonfyre-account-continuity inspect|apply\n",stderr);return 2;
    }
    const char *home=getenv("HOME");if(!home||strlen(home)>2048)return 2;
    char cmsdb[4096],financedb[4096],itemsdb[4096],cmsbin[4096];
    snprintf(cmsdb,sizeof(cmsdb),"%s/.bonfyre/estate-local/cms.db",home);
    snprintf(financedb,sizeof(financedb),"%s/.bonfyre/estate-local/finance.db",home);
    snprintf(itemsdb,sizeof(itemsdb),"%s/.bonfyre/estate-local/items.db",home);
    snprintf(cmsbin,sizeof(cmsbin),"%s/.bonfyre/bin/bonfyre-cms",home);
    sqlite3 *cms=NULL,*fin=NULL,*items=NULL;
    if(!dbopen(cmsdb,&cms)||!dbopen(financedb,&fin)||!dbopen(itemsdb,&items)){
        fputs("Canonical CMS, Finance or item archive unavailable\n",stderr);
        if(cms)sqlite3_close(cms);if(fin)sqlite3_close(fin);if(items)sqlite3_close(items);return 3;
    }
    bf_mp_writer_t source,finance,crm;bf_mp_writer_init(&source,4096);bf_mp_writer_init(&finance,32768);bf_mp_writer_init(&crm,4096);
    char sourcekey[96]="",crmkey[96]="";int rows=0,deals=0,ok=0,source_rev=0,finance_rev=0,crm_rev=0;
    if(!source_record(items,cms,&source,sourcekey)||!finance_record(fin,cms,&finance,&rows,&deals)||!crm_record(items,cms,&crm,crmkey)){
        fputs("Retained source or finance projection refused validation\n",stderr);goto out;
    }
    char sdig[65],fdig[65],cdig[65];bf_sha256_hex(source.buf,source.len,sdig);bf_sha256_hex(finance.buf,finance.len,fdig);bf_sha256_hex(crm.buf,crm.len,cdig);
    int srcstate=readback(cms,SRC_BIND,sourcekey,sdig,&source_rev);
    int finstate=readback(cms,FIN_BIND,FIN_KEY,fdig,&finance_rev);
    int crmstate=readback(cms,CRM_BIND,crmkey,cdig,&crm_rev);
    printf("{\"stage\":\"preflight\",\"source_key\":\"%s\",\"source_state\":%d,\"finance_key\":\"%s\",\"finance_state\":%d,\"crm_key\":\"%s\",\"crm_state\":%d,\"opportunities\":%d,\"crm_deals\":%d}\n",sourcekey,srcstate,FIN_KEY,finstate,crmkey,crmstate,rows,deals);
    if(!strcmp(argv[1],"inspect")){ok=1;goto out;}
    if(srcstate<0||finstate<0||crmstate<0||srcstate==1||finstate==1||crmstate==1){
        fputs("Changed or removed existing record; refusing overwrite\n",stderr);goto out;
    }
    if(!save(cmsbin,cmsdb,cms,SRC_BIND,sourcekey,&source,&source_rev)||
       !save(cmsbin,cmsdb,cms,CRM_BIND,crmkey,&crm,&crm_rev)||
       !save(cmsbin,cmsdb,cms,FIN_BIND,FIN_KEY,&finance,&finance_rev))goto out;
    printf("{\"stage\":\"accepted\",\"source_revision\":%d,\"finance_revision\":%d,\"crm_revision\":%d,\"opportunities\":%d,\"crm_deals\":%d}\n",source_rev,finance_rev,crm_rev,rows,deals);
    ok=1;
out:
    bf_mp_writer_free(&source);bf_mp_writer_free(&finance);bf_mp_writer_free(&crm);
    sqlite3_close(items);sqlite3_close(fin);sqlite3_close(cms);return ok?0:1;
}
