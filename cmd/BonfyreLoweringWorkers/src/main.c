/* Typed, non-owning lowerer consumers.  They recheck the named declaration,
 * publish derived artifacts/demands atomically, and never enact a protocol or
 * observe an external URL themselves. */
#define _POSIX_C_SOURCE 200809L
#include <sqlite3.h>
#include <dirent.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>
#include "bf_foreign_twin.h"

typedef struct { const char *db,*inbox,*closure,*protocols,*cms,*media,*channels,*channel_bin,*grammar,*external,*provider,*demands,*foreign_receipts,*owner_image,*program_resolver,*program_runtime; int limit; } Opt;
typedef struct { char identity[160],body[PATH_MAX],content[65],receipt[72],frontier[64]; int owner_verified; } ContextMaterial;
static int sql(sqlite3*d,const char*s){return sqlite3_exec(d,s,0,0,0)==SQLITE_OK;}
static int table_has_column(sqlite3*d,const char*table,const char*column){sqlite3_stmt*s=0;char q[128];int ok=0;snprintf(q,sizeof(q),"PRAGMA table_info(%s)",table);if(sqlite3_prepare_v2(d,q,-1,&s,0)!=SQLITE_OK)return 0;while(sqlite3_step(s)==SQLITE_ROW){const char*n=(const char*)sqlite3_column_text(s,1);if(n&&!strcmp(n,column)){ok=1;break;}}sqlite3_finalize(s);return ok;}
static char* slurp(const char*p){FILE*f;long n;char*b;if(!(f=fopen(p,"rb")))return 0;if(fseek(f,0,2)||(n=ftell(f))<0||fseek(f,0,0)){fclose(f);return 0;}b=malloc((size_t)n+1);if(!b||fread(b,1,(size_t)n,f)!=(size_t)n){free(b);fclose(f);return 0;}b[n]=0;fclose(f);return b;}
/* OwnerContractImage is a stale-input guard for derived lowering. It names
 * exact gates but cannot stand in for them, so every consumer still reopens
 * its owner after this check. */
static int owner_image_valid(const char *path){char *body=slurp(path);int ok=body&&strstr(body,"\"schema\":\"bonfyre-owner-contract-image.v1\"")&&strstr(body,"\"unresolved\":0")&&strstr(body,"\"realized_unmapped\":0");free(body);return ok;}
static int pref(const char*s,const char*p){return s&&p&&!strncmp(s,p,strlen(p));}
static int fields(const char*b,const char*k,const char*l,char**id,char**owner){sqlite3*d=0;sqlite3_stmt*s=0;int ok=0;*id=*owner=0;if(sqlite3_open(":memory:",&d)!=SQLITE_OK)return 0;if(sqlite3_prepare_v2(d,"SELECT json_extract(?1,'$.identity'),json_extract(?1,'$.owner_ref') WHERE json_valid(?1) AND json_extract(?1,'$.schema')='bonfyre-lowering-handoff.v1' AND json_extract(?1,'$.candidate_kind')=?2 AND json_extract(?1,'$.lowerer_ref')=?3 AND json_extract(?1,'$.truth')='dependency_cut_only'",-1,&s,0)==SQLITE_OK){sqlite3_bind_text(s,1,b,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,k,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,l,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_text(s,0)&&sqlite3_column_text(s,1)){*id=strdup((const char*)sqlite3_column_text(s,0));*owner=strdup((const char*)sqlite3_column_text(s,1));ok=*id&&*owner;}}if(s)sqlite3_finalize(s);sqlite3_close(d);if(!ok){free(*id);free(*owner);*id=*owner=0;}return ok;}
/* A projection artifact is derived and disposable, but its cache key must be
 * content-addressed so identical authenticated closure snapshots can reuse it
 * safely across worker restarts.  This is not a semantic identity or proof. */
static int digest(const char*b,char out[72]){char hex[65];if(!b||!out)return 0;bf_sha256_hex((const uint8_t*)b,strlen(b),hex);snprintf(out,72,"sha256:%s",hex);return 1;}
static sqlite3* store(const char*p){sqlite3*d=0;if(sqlite3_open_v2(p,&d,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE,0)!=SQLITE_OK){if(d)sqlite3_close(d);return 0;}sqlite3_busy_timeout(d,5000);if(!sql(d,"PRAGMA journal_mode=WAL;CREATE TABLE IF NOT EXISTS mcp_projection_artifact(identity TEXT PRIMARY KEY,closure_digest TEXT NOT NULL,projection_json TEXT NOT NULL,compiled_at INTEGER NOT NULL);CREATE TABLE IF NOT EXISTS protocol_compilation_artifact(identity TEXT PRIMARY KEY,program_digest TEXT NOT NULL,program_json TEXT NOT NULL,compiled_at INTEGER NOT NULL);CREATE TABLE IF NOT EXISTS provider_embedding_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,provider TEXT NOT NULL,model TEXT NOT NULL,representation_id TEXT NOT NULL,dimensions INTEGER NOT NULL,admission_state TEXT NOT NULL CHECK(admission_state='unrequested'),compiled_at INTEGER NOT NULL);CREATE TABLE IF NOT EXISTS material_observation_demand(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,topic TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'queued',requested_at INTEGER NOT NULL,attempts INTEGER NOT NULL DEFAULT 0);CREATE TABLE IF NOT EXISTS material_foreign_receipt_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,topic TEXT NOT NULL,receipt_path TEXT NOT NULL,receipt_digest TEXT NOT NULL,observation_kind TEXT NOT NULL,content_sha256 TEXT,body_path TEXT,compiled_at INTEGER NOT NULL);CREATE TABLE IF NOT EXISTS material_collection_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,subject_address TEXT NOT NULL,compiled_at INTEGER NOT NULL);CREATE TABLE IF NOT EXISTS material_pack_demand_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,compiled_at INTEGER NOT NULL);CREATE TABLE IF NOT EXISTS media_library_surface_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,library_id TEXT NOT NULL,subject_ref TEXT NOT NULL,media_program_ref TEXT NOT NULL,grammar TEXT NOT NULL,compiled_at INTEGER NOT NULL);CREATE TABLE IF NOT EXISTS channel_surface_projection(identity TEXT PRIMARY KEY,projection_digest TEXT NOT NULL,projection_json TEXT NOT NULL,compiled_at INTEGER NOT NULL);CREATE TABLE IF NOT EXISTS interaction_surface_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,grammar_id TEXT NOT NULL,subject_ref TEXT NOT NULL,institution_ref TEXT NOT NULL,visibility TEXT NOT NULL,exact_verifier TEXT NOT NULL,target_lowerer_ref TEXT NOT NULL,state TEXT NOT NULL CHECK(state='pending_exact_owner'),compiled_at INTEGER NOT NULL);CREATE TABLE IF NOT EXISTS interaction_surface_composition(identity TEXT PRIMARY KEY,composition_digest TEXT NOT NULL,composition_json TEXT NOT NULL,exact_verifier_state TEXT NOT NULL CHECK(exact_verifier_state='unqueried'),compiled_at INTEGER NOT NULL);")){sqlite3_close(d);return 0;}return d;}

/* Exact closure recheck: an MCP advertisement absent from the authenticated
 * snapshot is refused.  The stored projection is only its typed descriptor. */
static int mcp(sqlite3*d,const Opt*o,const char*b,const char*l){char *id=0,*own=0,*cl=0,*v=0,h[72]={0};sqlite3*x=0;sqlite3_stmt*s=0;int ok=0;if(!o->closure||!pref(l,"MCP:")||!fields(b,"mcp_projection",l,&id,&own)||!pref(id,"mcp:")||!pref(own,"MCPClosure:"))goto out;cl=slurp(o->closure);if(!cl||sqlite3_open(":memory:",&x)!=SQLITE_OK)goto out;if(!strcmp(l,"MCP:tool")){if(sqlite3_prepare_v2(x,"SELECT value FROM json_each(?1,'$.tools') WHERE json_extract(value,'$.name')=substr(?2,10)",-1,&s,0)!=SQLITE_OK)goto out;}else if(!strcmp(l,"MCP:resource")){if(sqlite3_prepare_v2(x,"SELECT value FROM json_each(?1,'$.resources') WHERE json_extract(value,'$.uri')=substr(?2,14)",-1,&s,0)!=SQLITE_OK)goto out;}else if(!strcmp(l,"MCP:resource_template")){if(sqlite3_prepare_v2(x,"SELECT value FROM json_each(?1,'$.resourceTemplates') WHERE json_extract(value,'$.uriTemplate')=substr(?2,23)",-1,&s,0)!=SQLITE_OK)goto out;}else if(!strcmp(l,"MCP:prompt")){if(sqlite3_prepare_v2(x,"SELECT value FROM json_each(?1,'$.prompts') WHERE json_extract(value,'$.name')=substr(?2,12)",-1,&s,0)!=SQLITE_OK)goto out;}else goto out;sqlite3_bind_text(s,1,cl,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,id,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)!=SQLITE_ROW||!sqlite3_column_text(s,0))goto out;v=strdup((const char*)sqlite3_column_text(s,0));if(!v||!digest(cl,h))goto out;sqlite3_finalize(s);s=0;if(sqlite3_prepare_v2(d,"INSERT INTO mcp_projection_artifact VALUES(?1,?2,?3,?4) ON CONFLICT(identity) DO UPDATE SET closure_digest=excluded.closure_digest,projection_json=excluded.projection_json,compiled_at=excluded.compiled_at",-1,&s,0)!=SQLITE_OK)goto out;sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,h,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,v,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,4,(sqlite3_int64)time(0));ok=sqlite3_step(s)==SQLITE_DONE;out:if(s)sqlite3_finalize(s);if(x)sqlite3_close(x);free(id);free(own);free(cl);free(v);return ok;}

/* A list refresh can withdraw a previously advertised MCP descriptor.  Its
 * derived artifact must disappear atomically with the current authenticated
 * closure rather than remaining a stale cache entry until an unrelated delta
 * happens to touch it.  This is cache retirement only; it neither revokes a
 * token nor changes any canonical tool/resource owner. */
static int mcp_retire_absent(sqlite3 *d,const Opt *o,int *retired){
    char *closure=0;sqlite3_stmt *valid=0,*del=0;int ok=0;
    if(retired)*retired=0;if(!o->closure)return 1;
    if(!(closure=slurp(o->closure)))goto out;
    if(sqlite3_prepare_v2(d,"SELECT json_valid(?1) AND json_type(?1,'$.tools')='array' AND json_type(?1,'$.resources')='array' AND json_type(?1,'$.resourceTemplates')='array' AND json_type(?1,'$.prompts')='array'",-1,&valid,0)!=SQLITE_OK)goto out;
    sqlite3_bind_text(valid,1,closure,-1,SQLITE_TRANSIENT);if(sqlite3_step(valid)!=SQLITE_ROW||!sqlite3_column_int(valid,0))goto out;sqlite3_finalize(valid);valid=0;
    if(!sql(d,"BEGIN IMMEDIATE;"))goto out;
    if(sqlite3_prepare_v2(d,
       "DELETE FROM mcp_projection_artifact WHERE "
       "(identity LIKE 'mcp:tool:%' AND NOT EXISTS(SELECT 1 FROM json_each(?1,'$.tools') WHERE json_extract(value,'$.name')=substr(mcp_projection_artifact.identity,10))) OR "
       "(identity LIKE 'mcp:resource:%' AND NOT EXISTS(SELECT 1 FROM json_each(?1,'$.resources') WHERE json_extract(value,'$.uri')=substr(mcp_projection_artifact.identity,14))) OR "
       "(identity LIKE 'mcp:resource_template:%' AND NOT EXISTS(SELECT 1 FROM json_each(?1,'$.resourceTemplates') WHERE json_extract(value,'$.uriTemplate')=substr(mcp_projection_artifact.identity,23))) OR "
       "(identity LIKE 'mcp:prompt:%' AND NOT EXISTS(SELECT 1 FROM json_each(?1,'$.prompts') WHERE json_extract(value,'$.name')=substr(mcp_projection_artifact.identity,12)))",-1,&del,0)!=SQLITE_OK){sql(d,"ROLLBACK;");goto out;}
    sqlite3_bind_text(del,1,closure,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(del)!=SQLITE_DONE){sql(d,"ROLLBACK;");goto out;}
    if(retired)*retired=sqlite3_changes(d);sqlite3_finalize(del);del=0;
    ok=sql(d,"COMMIT;");
out:if(valid)sqlite3_finalize(valid);if(del)sqlite3_finalize(del);free(closure);return ok;
}

/* Recheck an authored chain declaration, then retain only a compiled program.
 * No carrier, authority check, commitment, or external effect occurs here. */
static int protocol(sqlite3*d,const Opt*o,const char*b,const char*l){char *id=0,*own=0,*ps=0,*v=0,h[72]={0};sqlite3*x=0;sqlite3_stmt*s=0,*i=0;int ok=0;if(!o->protocols||strcmp(l,"ProtocolCompiler:late_carrier")||!fields(b,"protocol_chain",l,&id,&own)||!pref(id,"protocol:")||!pref(own,"ProtocolCommons:"))goto out;ps=slurp(o->protocols);if(!ps||sqlite3_open(":memory:",&x)!=SQLITE_OK||!sql(x,"CREATE TABLE p(j TEXT NOT NULL);"))goto out;for(char*p=ps,*n;p&&*p;p=n){n=strchr(p,'\n');if(n)*n++=0;if(*p){if(sqlite3_prepare_v2(x,"INSERT INTO p VALUES(?1)",-1,&i,0)!=SQLITE_OK)goto out;sqlite3_bind_text(i,1,p,-1,SQLITE_TRANSIENT);if(sqlite3_step(i)!=SQLITE_DONE)goto out;sqlite3_finalize(i);i=0;}}if(sqlite3_prepare_v2(x,"SELECT j FROM p WHERE json_valid(j) AND json_extract(j,'$.id')=substr(?1,10)",-1,&s,0)!=SQLITE_OK)goto out;sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)!=SQLITE_ROW||!sqlite3_column_text(s,0))goto out;v=strdup((const char*)sqlite3_column_text(s,0));if(!v||!digest(v,h))goto out;sqlite3_finalize(s);s=0;if(sqlite3_prepare_v2(d,"INSERT INTO protocol_compilation_artifact VALUES(?1,?2,?3,?4) ON CONFLICT(identity) DO UPDATE SET program_digest=excluded.program_digest,program_json=excluded.program_json,compiled_at=excluded.compiled_at",-1,&s,0)!=SQLITE_OK)goto out;sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,h,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,v,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,4,(sqlite3_int64)time(0));ok=sqlite3_step(s)==SQLITE_DONE;out:if(i)sqlite3_finalize(i);if(s)sqlite3_finalize(s);if(x)sqlite3_close(x);free(id);free(own);free(ps);free(v);return ok;}

/* ProviderCompiler rechecks a finished same-width projection run, then keeps
 * only a disposable capability artifact. It never requests a provider call:
 * ResourceAdmission remains at the subsequent purpose-bound rank/embed step. */
static int provider_embedding(sqlite3*d,const Opt*o,const char*b){
    const char *provider_db=o->provider?o->provider:getenv("BONFYRE_PROVIDER_DB");
    char *id=0,*own=0,*provider=0,*model=0,*representation=0;
    sqlite3*p=0;sqlite3_stmt*s=0;int dimensions=0,ok=0;
    if(!provider_db||!fields(b,"provider","ProviderCompiler:embedding",&id,&own)||!pref(id,"provider-embedding:")||!pref(own,"ProviderEmbeddingRun:")||strcmp(id+19,own+21))goto out;
    if(sqlite3_open_v2(provider_db,&p,SQLITE_OPEN_READONLY,0)!=SQLITE_OK)goto out;
    if(sqlite3_prepare_v2(p,"SELECT provider,model,representation_id,dimensions FROM provider_embedding_run WHERE run_id=?1 AND status='complete' AND requested_count>0 AND stored_count=requested_count AND dimensions>0",-1,&s,0)!=SQLITE_OK)goto out;
    sqlite3_bind_text(s,1,id+19,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(s)!=SQLITE_ROW||!sqlite3_column_text(s,0)||!sqlite3_column_text(s,1)||!sqlite3_column_text(s,2))goto out;
    provider=strdup((const char*)sqlite3_column_text(s,0));
    model=strdup((const char*)sqlite3_column_text(s,1));
    representation=strdup((const char*)sqlite3_column_text(s,2));
    dimensions=sqlite3_column_int(s,3);
    if(!provider||!model||!representation)goto out;
    sqlite3_finalize(s);s=0;
    if(sqlite3_prepare_v2(d,"INSERT INTO provider_embedding_artifact(identity,owner_ref,provider,model,representation_id,dimensions,admission_state,compiled_at) VALUES(?1,?2,?3,?4,?5,?6,'unrequested',?7) ON CONFLICT(identity) DO UPDATE SET owner_ref=excluded.owner_ref,provider=excluded.provider,model=excluded.model,representation_id=excluded.representation_id,dimensions=excluded.dimensions,admission_state='unrequested',compiled_at=excluded.compiled_at",-1,&s,0)!=SQLITE_OK)goto out;
    sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,own,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,provider,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,model,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,5,representation,-1,SQLITE_TRANSIENT);sqlite3_bind_int(s,6,dimensions);sqlite3_bind_int64(s,7,(sqlite3_int64)time(0));ok=sqlite3_step(s)==SQLITE_DONE;
out:if(s)sqlite3_finalize(s);if(p)sqlite3_close(p);free(id);free(own);free(provider);free(model);free(representation);return ok;
}

static int topic(const char*cms,const char*name,char*out,size_t cap){sqlite3*d=0;sqlite3_stmt*s=0;int ok=0;if(!cms||sqlite3_open_v2(cms,&d,SQLITE_OPEN_READONLY,0)!=SQLITE_OK)goto out;if(sqlite3_prepare_v2(d,"SELECT url FROM internet_source WHERE source=?1 AND status='published' ORDER BY id DESC LIMIT 1",-1,&s,0)!=SQLITE_OK)goto out;sqlite3_bind_text(s,1,name,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_text(s,0)){snprintf(out,cap,"%s",sqlite3_column_text(s,0));ok=*out!=0;}out:if(s)sqlite3_finalize(s);if(d)sqlite3_close(d);return ok;}
static int demand(const char*dir,const char*id,const char*t){char key[512],p[PATH_MAX],tmp[PATH_MAX];int fd,dd;FILE*f;size_t j=0;if(!dir || (mkdir(dir,0750)&&errno!=EEXIST))return 0;for(size_t i=0;id[i]&&j+1<sizeof(key);i++){unsigned char c=(unsigned char)id[i];key[j++]=(c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='-'||c=='_'?(char)c:'-';}key[j]=0;if(snprintf(p,sizeof(p),"%s/%s.json",dir,key)>=(int)sizeof(p)||snprintf(tmp,sizeof(tmp),"%s/.demand-XXXXXX",dir)>=(int)sizeof(tmp))return 0;if(access(p,F_OK)==0)return 1;fd=mkstemp(tmp);if(fd<0)return 0;f=fdopen(fd,"w");if(!f){close(fd);unlink(tmp);return 0;}fprintf(f,"{\"schema\":\"bonfyre-foreign-observation-demand.v1\",\"identity\":\"%s\",\"topic\":\"%s\",\"truth\":\"observation_requested_not_performed\"}\n",id,t);if(fflush(f)||fsync(fd)||fclose(f)){unlink(tmp);return 0;}if(rename(tmp,p)){unlink(tmp);return 0;}dd=open(dir,O_RDONLY);if(dd>=0){int ok=fsync(dd)==0;close(dd);return ok;}return 1;}

/* Context material is a strict, disposable intersection: BitNetRankRun names
 * the bounded AddressPlane frontier; ForeignTwin receipts name retained bytes.
 * This compiler neither observes a URL nor promotes a body into an owner fact. */
static int json_file_string(FILE*out,const char*s){const unsigned char*p=(const unsigned char*)(s?s:"");if(fputc('"',out)==EOF)return 0;for(;*p;p++){if(*p=='"'||*p=='\\'){if(fputc('\\',out)==EOF||fputc(*p,out)==EOF)return 0;}else if(*p=='\n'){if(fputs("\\n",out)==EOF)return 0;}else if(*p=='\r'){if(fputs("\\r",out)==EOF)return 0;}else if(*p=='\t'){if(fputs("\\t",out)==EOF)return 0;}else if(*p<0x20){if(fprintf(out,"\\u%04x",*p)<0)return 0;}else if(fputc(*p,out)==EOF)return 0;}return fputc('"',out)!=EOF;}
static int material_slot(ContextMaterial*m,int n,const char*id){for(int i=0;i<n;i++)if(!strcmp(m[i].identity,id))return i;return -1;}
static int context_manifest(const char*rank_path,const char*run,const char*evidence_path,const char*query_path,const char*policy,const char*out_path,int shadow){sqlite3*rank=0,*mem=0;sqlite3_stmt*rows=0,*parse=0;FILE*evidence=0,*queries=0,*out=0;char*line=0,tmp[PATH_MAX]={0};size_t cap=0;ssize_t len;ContextMaterial*m=0;int n=0,used=0,fd=-1,ok=0;char gate[16]={0};
 if(!rank_path||!run||!evidence_path||!policy||!out_path)return 0;if(sqlite3_open_v2(rank_path,&rank,SQLITE_OPEN_READONLY,0)!=SQLITE_OK)goto done;if(sqlite3_prepare_v2(rank,"SELECT gate_state FROM bitnet_rank_run WHERE run_id=?1",-1,&rows,0)!=SQLITE_OK)goto done;sqlite3_bind_text(rows,1,run,-1,SQLITE_TRANSIENT);if(sqlite3_step(rows)!=SQLITE_ROW||!sqlite3_column_text(rows,0))goto done;snprintf(gate,sizeof(gate),"%s",sqlite3_column_text(rows,0));sqlite3_finalize(rows);rows=0;if(strcmp(gate,"ADMIT")&&(!shadow||strcmp(gate,"SHADOW")))goto done;
 if(sqlite3_prepare_v2(rank,"SELECT candidate_identity FROM embedding_projection WHERE run_id=?1 ORDER BY candidate_identity",-1,&rows,0)!=SQLITE_OK)goto done;sqlite3_bind_text(rows,1,run,-1,SQLITE_TRANSIENT);while(sqlite3_step(rows)==SQLITE_ROW){const char*id=(const char*)sqlite3_column_text(rows,0);ContextMaterial*next;if(!id||!*id)goto done;next=realloc(m,(size_t)(n+1)*sizeof(*m));if(!next)goto done;m=next;memset(&m[n],0,sizeof(*m));snprintf(m[n++].identity,sizeof(m[n-1].identity),"%s",id);}sqlite3_finalize(rows);rows=0;if(!n)goto done;
 if(sqlite3_open(":memory:",&mem)!=SQLITE_OK)goto done;
 /* A caller may require the native exact-owner feedback ledger.  It is an
  * additional gate on retained material, not a replacement for digesting the
  * body and receipt below. */
 if(query_path){if(!(queries=fopen(query_path,"r"))||sqlite3_prepare_v2(mem,"SELECT json_extract(?1,'$.run_id'),json_extract(?1,'$.candidate_identity') WHERE json_valid(?1) AND json_extract(?1,'$.schema')='bonfyre-exact-owner-query-evidence.v1' AND json_extract(?1,'$.query_state')='queried' AND json_extract(?1,'$.outcome')='observed' AND json_extract(?1,'$.exact_verifier_ref')='ForeignTwin:admission'",-1,&parse,0)!=SQLITE_OK)goto done;while((len=getline(&line,&cap,queries))>0){const char*qrun,*id;int slot;if(len&&line[len-1]=='\n')line[--len]=0;sqlite3_bind_text(parse,1,line,(int)len,SQLITE_TRANSIENT);if(sqlite3_step(parse)==SQLITE_ROW){qrun=(const char*)sqlite3_column_text(parse,0);id=(const char*)sqlite3_column_text(parse,1);slot=id?material_slot(m,n,id):-1;if(qrun&&!strcmp(qrun,run)&&slot>=0)m[slot].owner_verified=1;}sqlite3_reset(parse);sqlite3_clear_bindings(parse);}sqlite3_finalize(parse);parse=0;fclose(queries);queries=0;free(line);line=0;cap=0;}
 if(!(evidence=fopen(evidence_path,"r"))||sqlite3_prepare_v2(mem,"SELECT json_extract(?1,'$.twin_id'),json_extract(?1,'$.content_sha256'),json_extract(?1,'$.body_path'),json_extract(?1,'$.observed_at') WHERE json_valid(?1) AND json_extract(?1,'$.schema')='bonfyre-foreign-observation.v1' AND json_extract(?1,'$.observation_kind') IN ('source_observed','boundary_response')",-1,&parse,0)!=SQLITE_OK)goto done;
 while((len=getline(&line,&cap,evidence))>0){const char*id,*hash,*body,*at;int slot;char actual[65],receipt[65];if(len&&line[len-1]=='\n')line[--len]=0;sqlite3_bind_text(parse,1,line,(int)len,SQLITE_TRANSIENT);if(sqlite3_step(parse)!=SQLITE_ROW){sqlite3_reset(parse);sqlite3_clear_bindings(parse);continue;}id=(const char*)sqlite3_column_text(parse,0);hash=(const char*)sqlite3_column_text(parse,1);body=(const char*)sqlite3_column_text(parse,2);at=(const char*)sqlite3_column_text(parse,3);slot=id?material_slot(m,n,id):-1;if(slot>=0&&(!query_path||m[slot].owner_verified)&&hash&&body&&at&&*hash&&*body&&*at&&bf_sha256_file(body,actual)==0&&!strcmp(hash,actual)){bf_sha256_hex((const uint8_t*)line,(size_t)len,receipt);snprintf(m[slot].body,sizeof(m[slot].body),"%s",body);snprintf(m[slot].content,sizeof(m[slot].content),"%s",actual);snprintf(m[slot].receipt,sizeof(m[slot].receipt),"sha256:%s",receipt);snprintf(m[slot].frontier,sizeof(m[slot].frontier),"%s",at);}sqlite3_reset(parse);sqlite3_clear_bindings(parse);}
 if(snprintf(tmp,sizeof(tmp),"%s.tmp.XXXXXX",out_path)>=(int)sizeof(tmp)||(fd=mkstemp(tmp))<0||(out=fdopen(fd,"w"))==0)goto done;fd=-1;for(int i=0;i<n;i++){if(!m[i].body[0])continue;if(fputs("{\"candidate_identity\":",out)<0||!json_file_string(out,m[i].identity)||fputs(",\"body_path\":",out)<0||!json_file_string(out,m[i].body)||fputs(",\"content_sha256\":\"sha256:",out)<0||fputs(m[i].content,out)<0||fputs("\",\"receipt_digest\":",out)<0||!json_file_string(out,m[i].receipt)||fputs(",\"evidence_ref\":",out)<0||!json_file_string(out,evidence_path)||fputs(",\"information_policy_class\":",out)<0||!json_file_string(out,policy)||fputs(",\"temporal_frontier\":",out)<0||!json_file_string(out,m[i].frontier)||fputs("}\n",out)<0)goto done;used++;}if(!used||fflush(out)||fsync(fileno(out))||fclose(out)||rename(tmp,out_path)){out=0;goto done;}out=0;printf("{\"schema\":\"bonfyre-context-material-manifest.v1\",\"run_id\":");json_file_string(stdout,run);printf(",\"rank_gate_state\":");json_file_string(stdout,gate);printf(",\"retained_materials\":%d,\"frontier_candidates\":%d,\"output\":",used,n);json_file_string(stdout,out_path);puts(",\"truth\":\"receipted_foreign_bodies_intersected_with_persisted_frontier_only\"}");ok=1;
done:if(rows)sqlite3_finalize(rows);if(parse)sqlite3_finalize(parse);if(rank)sqlite3_close(rank);if(mem)sqlite3_close(mem);if(evidence)fclose(evidence);if(queries)fclose(queries);free(line);free(m);if(out)fclose(out);if(fd>=0)close(fd);if(!ok&&tmp[0])unlink(tmp);return ok;}

/* An exact verifier is an owner query, not a score interpretation.  This
 * consumes independently retained ForeignTwin receipts, records those
 * receipts idempotently through the native owner ABI, then asks that owner
 * about every member of a persisted BitNet frontier.  The resulting envelope
 * is deliberately useful to a lowerer (it distinguishes retained material
 * from absent material) without treating a retained byte as relevance,
 * authority, evidence admission, or selection. */
typedef struct { int rank; int observed; } QueryRank;
static int json_nullable_text(FILE*out,const char*s){return s&&*s?json_file_string(out,s):fputs("null",out)>=0;}
static int foreign_verify(const char *rank_path,const char *run,const char *evidence_path,
                          const char *foreign_db,const char *out_path,int shadow){
 sqlite3 *rank=0,*parse=0;sqlite3_stmt *rows=0,*receipt=0;BfForeignTwin*twin=0;FILE*in=0,*out=0;char*line=0,tmp[PATH_MAX]={0},gate[16]={0},representation[256]={0};size_t cap=0;ssize_t len;int fd=-1,accepted=0,queried=0,observed=0,ok=0;QueryRank *curve=0;int curve_n=0;
 if(!rank_path||!run||!evidence_path||!foreign_db||!out_path)return 0;
 if(sqlite3_open_v2(rank_path,&rank,SQLITE_OPEN_READONLY,0)!=SQLITE_OK)goto done;
 if(sqlite3_prepare_v2(rank,"SELECT representation_id,gate_state FROM bitnet_rank_run WHERE run_id=?1",-1,&rows,0)!=SQLITE_OK)goto done;
 sqlite3_bind_text(rows,1,run,-1,SQLITE_TRANSIENT);if(sqlite3_step(rows)!=SQLITE_ROW||!sqlite3_column_text(rows,0)||!sqlite3_column_text(rows,1))goto done;
 snprintf(representation,sizeof(representation),"%s",sqlite3_column_text(rows,0));snprintf(gate,sizeof(gate),"%s",sqlite3_column_text(rows,1));sqlite3_finalize(rows);rows=0;
 if(strcmp(gate,"ADMIT")&&(!shadow||strcmp(gate,"SHADOW")))goto done;
 if(!(in=fopen(evidence_path,"r"))||bf_foreign_twin_open(foreign_db,&twin,stderr)!=0)goto done;
 if(sqlite3_open(":memory:",&parse)!=SQLITE_OK)goto done;
 if(sqlite3_prepare_v2(parse,"SELECT json_extract(?1,'$.twin_id'),json_extract(?1,'$.source_url'),json_extract(?1,'$.observed_at'),json_extract(?1,'$.content_sha256') WHERE json_valid(?1) AND json_extract(?1,'$.schema')='bonfyre-foreign-observation.v1' AND json_extract(?1,'$.observation_kind') IN ('source_observed','boundary_response') AND json_extract(?1,'$.policy_decision')='allow' AND length(json_extract(?1,'$.content_sha256'))=64",-1,&receipt,0)!=SQLITE_OK)goto done;
 while((len=getline(&line,&cap,in))>0){const char *id,*url,*at,*sha;char hex[65],rid[80];BfForeignTwinObservation o={0};if(len&&line[len-1]=='\n')line[--len]=0;sqlite3_bind_text(receipt,1,line,(int)len,SQLITE_TRANSIENT);if(sqlite3_step(receipt)!=SQLITE_ROW){sqlite3_reset(receipt);sqlite3_clear_bindings(receipt);continue;}id=(const char*)sqlite3_column_text(receipt,0);url=(const char*)sqlite3_column_text(receipt,1);at=(const char*)sqlite3_column_text(receipt,2);sha=(const char*)sqlite3_column_text(receipt,3);if(id&&url&&at&&sha){bf_sha256_hex((const uint8_t*)line,(size_t)len,hex);snprintf(rid,sizeof(rid),"foreign:%s",hex);o.receipt_id=rid;o.subject=id;o.predicate="retained_body";o.source_url=url;o.body_sha256=sha;o.observed_at=at;o.observer="ForeignTwin:participation_evidence";if(bf_foreign_twin_record(twin,&o)==0)accepted++;}sqlite3_reset(receipt);sqlite3_clear_bindings(receipt);}
 sqlite3_finalize(receipt);receipt=0;fclose(in);in=0;free(line);line=0;cap=0;
 if(sqlite3_prepare_v2(rank,"SELECT c.rank,c.candidate_identity,c.exact_verifier,e.source_digest,e.vector_digest FROM ranked_candidate_envelope c JOIN embedding_projection e ON e.run_id=c.run_id AND e.candidate_identity=c.candidate_identity WHERE c.run_id=?1 ORDER BY c.rank",-1,&rows,0)!=SQLITE_OK)goto done;
 sqlite3_bind_text(rows,1,run,-1,SQLITE_TRANSIENT);
 if(snprintf(tmp,sizeof(tmp),"%s.tmp.XXXXXX",out_path)>=(int)sizeof(tmp)||(fd=mkstemp(tmp))<0||(out=fdopen(fd,"w"))==0)goto done;fd=-1;
 while(sqlite3_step(rows)==SQLITE_ROW){int pos=sqlite3_column_int(rows,0),found;const char*id=(const char*)sqlite3_column_text(rows,1),*verifier=(const char*)sqlite3_column_text(rows,2),*source=(const char*)sqlite3_column_text(rows,3),*vector=(const char*)sqlite3_column_text(rows,4);char latest[128]={0};QueryRank*next;if(!id||!verifier||strcmp(verifier,"ForeignTwin:admission"))goto done;found=bf_foreign_twin_observed(twin,id,"retained_body");if(found<0)goto done;if(found&&bf_foreign_twin_latest_receipt(twin,id,"retained_body",latest,sizeof(latest))<0)goto done;next=realloc(curve,(size_t)(curve_n+1)*sizeof(*curve));if(!next)goto done;curve=next;curve[curve_n++]=(QueryRank){pos,found==1};if(found==1)observed++;queried++;
  if(fputs("{\"schema\":\"bonfyre-exact-owner-query-evidence.v1\",\"run_id\":",out)<0||!json_file_string(out,run)||fputs(",\"representation_id\":",out)<0||!json_file_string(out,representation)||fputs(",\"gate_state\":",out)<0||!json_file_string(out,gate)||fprintf(out,",\"rank\":%d,\"occurrence_id\":",pos)<0||!json_file_string(out,id)||fputs(",\"candidate_identity\":",out)<0||!json_file_string(out,id)||fputs(",\"exact_verifier_ref\":\"ForeignTwin:admission\",\"exact_owner\":\"ForeignTwin\",\"predicate\":\"retained_body\",\"state\":\"queried\",\"query_state\":\"queried\",\"outcome\":",out)<0||!json_file_string(out,found==1?"observed":"unobserved")||fputs(",\"receipt_id\":",out)<0||!json_nullable_text(out,found?latest:0)||fputs(",\"source_digest\":",out)<0||!json_file_string(out,source?source:"")||fputs(",\"vector_digest\":",out)<0||!json_file_string(out,vector?vector:"")||fputs(",\"causal_frontier\":\"AddressPlane:forward-result\",\"truth\":\"exact_retained_observation_query_only_not_relevance_authority_or_selection\"}\n",out)<0)goto done;
 }
 if(!queried||fflush(out)||fsync(fileno(out))||fclose(out)||rename(tmp,out_path)) {out=0;goto done;}out=0;
 {const int cuts[]={1,5,10,20,50,92};printf("{\"schema\":\"bonfyre-bitnet-foreign-verification-run.v1\",\"run_id\":");json_file_string(stdout,run);printf(",\"gate_state\":");json_file_string(stdout,gate);printf(",\"execution_mode\":\"");fputs(!strcmp(gate,"ADMIT")?"admitted_owner_verification":"shadow_owner_verification",stdout);printf("\",\"receipts_recorded_idempotently\":%d,\"exact_owner_queries\":%d,\"retained_observations\":%d,\"retained_observation_curve\":{",accepted,queried,observed);for(size_t i=0;i<sizeof(cuts)/sizeof(*cuts);i++){int n=cuts[i]<queried?cuts[i]:queried,hits=0;if(i)putchar(',');for(int j=0;j<curve_n&&curve[j].rank<n;j++)hits+=curve[j].observed;printf("\"top_%d\":{\"queried\":%d,\"observed\":%d}",cuts[i],n,hits);}printf("},\"output\":");json_file_string(stdout,out_path);puts(",\"truth\":\"native_ForeignTwin_queries_measure_retained_observation_coverage_only\"}");}
 ok=1;
done:if(rows)sqlite3_finalize(rows);if(receipt)sqlite3_finalize(receipt);if(rank)sqlite3_close(rank);if(parse)sqlite3_close(parse);if(twin)bf_foreign_twin_close(twin);if(in)fclose(in);free(line);free(curve);if(out)fclose(out);if(fd>=0)close(fd);if(!ok&&tmp[0])unlink(tmp);return ok;
}
/* MaterialCompiler requests a ForeignTwin observation only after the current
 * CMS row says source + URL are published. The carrier owns the eventual read. */
static int material(sqlite3*d,const Opt*o,const char*b,const char*l){char *id=0,*own=0,t[2048]={0};sqlite3_stmt*s=0;int ok=0;if(!o->cms||!o->demands||strcmp(l,"MaterialCompiler:source")||!fields(b,"external_source",l,&id,&own)||!pref(id,"cms:internet_source:")||!pref(own,"CMS:internet_source:")||!topic(o->cms,id+20,t,sizeof(t)))goto out;if(sqlite3_prepare_v2(d,"INSERT INTO material_observation_demand(identity,owner_ref,topic,state,requested_at) VALUES(?1,?2,?3,'queued',?4) ON CONFLICT(identity) DO UPDATE SET owner_ref=excluded.owner_ref,topic=excluded.topic,state='queued',requested_at=excluded.requested_at",-1,&s,0)!=SQLITE_OK)goto out;sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,own,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,t,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,4,(sqlite3_int64)time(0));if(sqlite3_step(s)!=SQLITE_DONE)goto out;sqlite3_finalize(s);s=0;ok=demand(o->demands,id,t);out:if(s)sqlite3_finalize(s);free(id);free(own);return ok;}
/* ExternalEstate has already supplied a compact, addressable candidate
 * population.  Its MaterialCompiler handoff may only recover the current
 * source topic from that owner and request one durable ForeignTwin read.  It
 * must not turn the candidate census into an observation, evidence, provider,
 * or external effect. */
static int external_topic(const char *external,const char *id,char*out,size_t cap){sqlite3*d=0;sqlite3_stmt*s=0;int ok=0;if(!external||sqlite3_open_v2(external,&d,SQLITE_OPEN_READONLY,0)!=SQLITE_OK)goto out;if(sqlite3_prepare_v2(d,"SELECT primary_url FROM occurrence WHERE id=?1",-1,&s,0)!=SQLITE_OK)goto out;sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_text(s,0)){snprintf(out,cap,"%s",sqlite3_column_text(s,0));ok=*out!=0;}out:if(s)sqlite3_finalize(s);if(d)sqlite3_close(d);return ok;}
/* A ForeignTwin FactDelta must materialize the receipt it names, rather than
 * generating another request for the same source.  The full write set stays
 * on the handoff envelope specifically so this loop cannot reobserve forever. */
static int handoff_has_foreign_topic(const char *handoff,const char *topic){sqlite3*x=0;sqlite3_stmt*s=0;int ok=0;if(!handoff||!topic||sqlite3_open(":memory:",&x)!=SQLITE_OK)return 0;if(sqlite3_prepare_v2(x,"SELECT EXISTS(SELECT 1 FROM json_each(?1,'$.causal_writes') WHERE value='ForeignTwin:'||?2)",-1,&s,0)==SQLITE_OK){sqlite3_bind_text(s,1,handoff,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,topic,-1,SQLITE_TRANSIENT);ok=sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_int(s,0)==1;}if(s)sqlite3_finalize(s);sqlite3_close(x);return ok;}
static int latest_foreign_receipt(const char *dir,const char *topic,char *path,size_t path_cap,char *kind,size_t kind_cap,char *content,size_t content_cap,char *body,size_t body_cap,char digest_out[72]){
    DIR *d=0;struct dirent*e;sqlite3*x=0;sqlite3_stmt*s=0;char best_at[96]={0};int ok=0;if(!dir||!topic||!(d=opendir(dir))||sqlite3_open(":memory:",&x)!=SQLITE_OK)goto out;
    if(sqlite3_prepare_v2(x,"SELECT json_extract(?1,'$.observed_at'),json_extract(?1,'$.observation_kind'),json_extract(?1,'$.content_sha256'),json_extract(?1,'$.body_path') WHERE json_valid(?1) AND json_extract(?1,'$.schema')='bonfyre-foreign-observation.v1' AND json_extract(?1,'$.source_url')=?2 AND json_extract(?1,'$.observation_kind') IN ('source_observed','repeat_observed','boundary_response','policy_excluded','policy_indeterminate')",-1,&s,0)!=SQLITE_OK)goto out;
    while((e=readdir(d))){char p[PATH_MAX],*raw=0,*save=0,*line;if(e->d_name[0]=='.'||!strstr(e->d_name,".json")||snprintf(p,sizeof(p),"%s/%s",dir,e->d_name)>=(int)sizeof(p)||(raw=slurp(p))==0)continue;for(line=strtok_r(raw,"\n",&save);line;line=strtok_r(0,"\n",&save)){const char *at,*k,*c,*b;if(line[0]!='{')continue;sqlite3_bind_text(s,1,line,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,topic,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW&&(at=(const char*)sqlite3_column_text(s,0))&&*at&&strcmp(at,best_at)>=0){k=(const char*)sqlite3_column_text(s,1);c=(const char*)sqlite3_column_text(s,2);b=(const char*)sqlite3_column_text(s,3);snprintf(best_at,sizeof(best_at),"%s",at);snprintf(path,path_cap,"%s",p);snprintf(kind,kind_cap,"%s",k?k:"");snprintf(content,content_cap,"%s",c?c:"");snprintf(body,body_cap,"%s",b?b:"");digest(line,digest_out);ok=1;}sqlite3_reset(s);sqlite3_clear_bindings(s);}free(raw);}
out:if(s)sqlite3_finalize(s);if(x)sqlite3_close(x);if(d)closedir(d);return ok;
}
static int material_external(sqlite3*d,const Opt*o,const char*b){char *id=0,*own=0,t[2048]={0};sqlite3_stmt*s=0;int ok=0;if(!o->external||!fields(b,"external_occurrence","MaterialCompiler:external",&id,&own)||!pref(id,"external:")||!pref(own,"ExternalEstate:")||strcmp(own+15,id)||!external_topic(o->external,id,t,sizeof(t)))goto out;
 if(handoff_has_foreign_topic(b,t)){char receipt[PATH_MAX]={0},kind[64]={0},content[72]={0},body[PATH_MAX]={0},receipt_digest[72]={0};if(!o->foreign_receipts||!latest_foreign_receipt(o->foreign_receipts,t,receipt,sizeof(receipt),kind,sizeof(kind),content,sizeof(content),body,sizeof(body),receipt_digest))goto out;if(sqlite3_prepare_v2(d,"INSERT INTO material_foreign_receipt_artifact(identity,owner_ref,topic,receipt_path,receipt_digest,observation_kind,content_sha256,body_path,compiled_at) VALUES(?1,?2,?3,?4,?5,?6,NULLIF(?7,''),NULLIF(?8,''),?9) ON CONFLICT(identity) DO UPDATE SET owner_ref=excluded.owner_ref,topic=excluded.topic,receipt_path=excluded.receipt_path,receipt_digest=excluded.receipt_digest,observation_kind=excluded.observation_kind,content_sha256=excluded.content_sha256,body_path=excluded.body_path,compiled_at=excluded.compiled_at",-1,&s,0)!=SQLITE_OK)goto out;sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,own,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,t,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,receipt,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,5,receipt_digest,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,6,kind,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,7,content,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,8,body,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,9,(sqlite3_int64)time(0));ok=sqlite3_step(s)==SQLITE_DONE;goto out;}
 if(!o->demands||sqlite3_prepare_v2(d,"INSERT INTO material_observation_demand(identity,owner_ref,topic,state,requested_at) VALUES(?1,?2,?3,'queued',?4) ON CONFLICT(identity) DO UPDATE SET owner_ref=excluded.owner_ref,topic=excluded.topic,state='queued',requested_at=excluded.requested_at",-1,&s,0)!=SQLITE_OK)goto out;sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,own,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,t,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,4,(sqlite3_int64)time(0));if(sqlite3_step(s)!=SQLITE_DONE)goto out;sqlite3_finalize(s);s=0;ok=demand(o->demands,id,t);
out:if(s)sqlite3_finalize(s);free(id);free(own);return ok;}
/* Collection refresh is delegated to the existing programme owner. The worker
 * supplies only checked identities and configured executables, never code or
 * paths from a handoff. A failed or unavailable resolver leaves the handoff. */
static int refresh_program_collection(const Opt *o,const char *instance,const char *ns){
    pid_t child;int status=0;struct timespec begin,now,pause={0,20000000};
    if(!o->program_resolver||!o->program_runtime||!o->external)return 0;
    if(clock_gettime(CLOCK_MONOTONIC,&begin))return 0;
    child=fork();if(child<0)return 0;
    if(!child){
        int null=open("/dev/null",O_RDWR);setpgid(0,0);
        if(null<0||dup2(null,STDIN_FILENO)<0||dup2(null,STDOUT_FILENO)<0)_exit(127);
        if(null>STDERR_FILENO)close(null);
        char *const args[]={(char*)o->program_runtime,(char*)o->program_resolver,
            "--cms-db",(char*)o->cms,"--instance",(char*)instance,"--namespace",(char*)ns,
            "--address-index",(char*)o->external,"--apply",NULL};
        execv(o->program_runtime,args);_exit(127);
    }
    setpgid(child,child);
    for(;;){pid_t result=waitpid(child,&status,WNOHANG);
        if(result==child)return WIFEXITED(status)&&WEXITSTATUS(status)==0;
        if(result<0&&errno!=EINTR)return 0;
        if(clock_gettime(CLOCK_MONOTONIC,&now)||now.tv_sec-begin.tv_sec>=60)break;
        nanosleep(&pause,NULL);
    }
    kill(-child,SIGKILL);kill(child,SIGKILL);while(waitpid(child,&status,0)<0&&errno==EINTR){}
    return 0;
}
static int material_collection(sqlite3 *d,const Opt *o,const char *body){
    char *id=NULL,*owner=NULL,*subject=NULL,*ns=NULL,*template=NULL,*instance=NULL;
    sqlite3 *cms=NULL;sqlite3_stmt *s=NULL;int ok=0;char query[1200];
    if(!o->cms||!fields(body,"source_collection","MaterialCompiler:source_collection",&id,&owner)||
       !pref(id,"cms:source_collection:")||!pref(owner,"CMS:source_collection:")||strcmp(id+22,owner+22))goto done;
    if(sqlite3_open_v2(o->cms,&cms,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
    snprintf(query,sizeof(query),"SELECT subject_address,%s,%s FROM source_collection WHERE collection_id=? AND status='published' LIMIT 2",
       table_has_column(cms,"source_collection","namespace")?"namespace":"'root'",
       table_has_column(cms,"source_collection","source_selector")?"json_extract(CASE WHEN source_selector IS NULL OR source_selector='' THEN '{}' ELSE source_selector END,'$.template_ref')":"NULL");
    if(sqlite3_prepare_v2(cms,query,-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,id+22,-1,SQLITE_STATIC);if(sqlite3_step(s)!=SQLITE_ROW)goto done;
    const char *a=(const char*)sqlite3_column_text(s,0),*b=(const char*)sqlite3_column_text(s,1),*t=(const char*)sqlite3_column_text(s,2);
    if(!a||!b||!*b||strlen(a)>1024||strlen(b)>240||(t&&strlen(t)>512))goto done;
    subject=strdup(a);ns=strdup(b);template=t?strdup(t):NULL;
    if(!subject||!ns||(t&&!template)||sqlite3_step(s)!=SQLITE_DONE)goto done;
    sqlite3_finalize(s);s=NULL;
    if(template&&*template){
        if(sqlite3_prepare_v2(cms,"SELECT instance_id FROM program_instance WHERE namespace=?1 AND template_id=?2 AND owner_subject=?3 AND instance_id||'-sources'=?4 AND status='published' LIMIT 2",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(s,1,ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,template,-1,SQLITE_STATIC);
        sqlite3_bind_text(s,3,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,4,id+22,-1,SQLITE_STATIC);
        if(sqlite3_step(s)!=SQLITE_ROW||sqlite3_column_type(s,0)!=SQLITE_TEXT)goto done;
        const char *value=(const char*)sqlite3_column_text(s,0);if(!value||!*value||strlen(value)>512)goto done;
        instance=strdup(value);if(!instance||sqlite3_step(s)!=SQLITE_DONE)goto done;
        sqlite3_finalize(s);s=NULL;sqlite3_close(cms);cms=NULL;
        if(!refresh_program_collection(o,instance,ns))goto done;
    }
    if(sqlite3_prepare_v2(d,"INSERT INTO material_collection_artifact(identity,owner_ref,subject_address,compiled_at) VALUES(?1,?2,?3,?4) ON CONFLICT(identity) DO UPDATE SET owner_ref=excluded.owner_ref,subject_address=excluded.subject_address,compiled_at=excluded.compiled_at",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,id,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,owner,-1,SQLITE_STATIC);
    sqlite3_bind_text(s,3,subject,-1,SQLITE_STATIC);sqlite3_bind_int64(s,4,(sqlite3_int64)time(NULL));ok=sqlite3_step(s)==SQLITE_DONE;
 done:sqlite3_finalize(s);if(cms)sqlite3_close(cms);free(id);free(owner);free(subject);free(ns);free(template);free(instance);return ok;
}

/* Pack demand is a declared compiler input, not a schedule.  Retain a compact
 * rechecked reference only; the later material planner must still read the
 * current owner declaration before choosing sources, bytes, or an admission. */
static int material_pack_demand(sqlite3*d,const Opt*o,const char*b){char *id=0,*own=0;sqlite3*c=0;sqlite3_stmt*s=0;int ok=0;if(!o->cms||!fields(b,"pack_demand","MaterialCompiler:pack_demand",&id,&own)||!pref(id,"cms:pack_demand:")||!pref(own,"CMS:pack_demand:"))goto out;if(sqlite3_open_v2(o->cms,&c,SQLITE_OPEN_READONLY,0)!=SQLITE_OK)goto out;if(sqlite3_prepare_v2(c,"SELECT 1 FROM pack_demand WHERE status='published' AND 'cms:pack_demand:'||pack||':'||demand_kind||':'||needs=?1 LIMIT 1",-1,&s,0)!=SQLITE_OK)goto out;sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)!=SQLITE_ROW)goto out;sqlite3_finalize(s);s=0;if(sqlite3_prepare_v2(d,"INSERT INTO material_pack_demand_artifact(identity,owner_ref,compiled_at) VALUES(?1,?2,?3) ON CONFLICT(identity) DO UPDATE SET owner_ref=excluded.owner_ref,compiled_at=excluded.compiled_at",-1,&s,0)!=SQLITE_OK)goto out;sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,own,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,3,(sqlite3_int64)time(0));ok=sqlite3_step(s)==SQLITE_DONE;out:if(s)sqlite3_finalize(s);if(c)sqlite3_close(c);free(id);free(own);return ok;}
/* A MediaLibrary handoff rechecks the current binding before retaining a
 * disposable surface artifact.  It deliberately has no actor argument: the
 * actor-specific access closure is evaluated again when a catalog or playback
 * plan is requested. */
static int media_library_surface(sqlite3*d,const Opt*o,const char*b){
    char *id=0,*own=0,*lib=0;sqlite3*c=0;sqlite3_stmt*s=0;int ok=0;
    char expected[640],grammar[64]={0},subject[256]={0},program[256]={0};const char *mode;
    if(!o->media||!fields(b,"media_library","MediaLibraryCompiler:surface",&id,&own)||!pref(id,"media:library:")||!pref(own,"MediaLibrary:"))goto out;
    lib=own+13;if(!*lib||sqlite3_open_v2(o->media,&c,SQLITE_OPEN_READONLY,0)!=SQLITE_OK)goto out;
    if(sqlite3_prepare_v2(c,"SELECT l.mode,m.subject_ref,m.media_program_ref FROM media_library l JOIN media_library_membership m ON m.library_id=l.library_id WHERE l.library_id=?1 AND l.state='active' AND m.state='active'",-1,&s,0)!=SQLITE_OK)goto out;
    sqlite3_bind_text(s,1,lib,-1,SQLITE_TRANSIENT);
    while(sqlite3_step(s)==SQLITE_ROW){mode=(const char*)sqlite3_column_text(s,0);const char *row_subject=(const char*)sqlite3_column_text(s,1),*row_program=(const char*)sqlite3_column_text(s,2);if(!mode||!row_subject||!row_program||snprintf(expected,sizeof(expected),"media:library:%s:%s",lib,row_program)>=(int)sizeof(expected)||strcmp(id,expected))continue;snprintf(subject,sizeof(subject),"%s",row_subject);snprintf(program,sizeof(program),"%s",row_program);snprintf(grammar,sizeof(grammar),"%s",!strcmp(mode,"personal")?"personal_media_habitat":"community_media_channel");break;}
    sqlite3_finalize(s);s=0;if(!grammar[0])goto out;
    if(sqlite3_prepare_v2(d,"INSERT INTO media_library_surface_artifact(identity,owner_ref,library_id,subject_ref,media_program_ref,grammar,compiled_at) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(identity) DO UPDATE SET owner_ref=excluded.owner_ref,library_id=excluded.library_id,subject_ref=excluded.subject_ref,media_program_ref=excluded.media_program_ref,grammar=excluded.grammar,compiled_at=excluded.compiled_at",-1,&s,0)!=SQLITE_OK)goto out;
    sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,own,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,lib,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,5,program,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,6,grammar,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,7,(sqlite3_int64)time(0));ok=sqlite3_step(s)==SQLITE_DONE;
out:if(s)sqlite3_finalize(s);if(c)sqlite3_close(c);free(id);free(own);return ok;
}
/* Reopen an actual owner source when the binding was derived from one.  A
 * creator grammar additionally requires the matching published program kind,
 * so an old channel snapshot cannot continue to surface after its last video,
 * short, or live occurrence was withdrawn. */
static int interaction_source_current(const Opt*o,const char*kind,const char*source,const char*owner,const char*grammar){
    sqlite3*c=0;sqlite3_stmt*s=0;int ok=0;const char*id;
    if(!strcmp(kind,"manual"))return 1;
    if(!strcmp(kind,"creator_channel")){
        if(!o->channels||!pref(owner,"CreatorChannel:")||strcmp(source,owner+15))return 0;
        id=owner+15;if(sqlite3_open_v2(o->channels,&c,SQLITE_OPEN_READONLY,0)!=SQLITE_OK)goto out;
        if(sqlite3_prepare_v2(c,"SELECT 1 FROM creator_channel c WHERE c.channel_id=?1 AND c.state='active' AND EXISTS(SELECT 1 FROM channel_publication p WHERE p.channel_id=c.channel_id AND p.state='published' AND ((?2='youtube-creator-channel' AND p.program_kind IN ('video','live')) OR (?2='tiktok-short-stream' AND p.program_kind='short'))) LIMIT 1",-1,&s,0)!=SQLITE_OK)goto out;
        sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,grammar,-1,SQLITE_TRANSIENT);ok=sqlite3_step(s)==SQLITE_ROW;
    }else if(!strcmp(kind,"media_library")){
        if(!o->media||!pref(owner,"MediaLibrary:")||strcmp(source,owner+13)||strcmp(grammar,"spotify-audio-habitat"))return 0;
        id=owner+13;if(sqlite3_open_v2(o->media,&c,SQLITE_OPEN_READONLY,0)!=SQLITE_OK)goto out;
        if(sqlite3_prepare_v2(c,"SELECT 1 FROM media_library l WHERE l.library_id=?1 AND l.state='active' AND EXISTS(SELECT 1 FROM media_library_membership m WHERE m.library_id=l.library_id AND m.state='active') LIMIT 1",-1,&s,0)!=SQLITE_OK)goto out;
        sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);ok=sqlite3_step(s)==SQLITE_ROW;
    }
out:if(s)sqlite3_finalize(s);if(c)sqlite3_close(c);return ok;
}
/* Channel owns the public creator projection.  The generic worker never
 * reproduces it: when given the native lowerer and CMS owner it executes that
 * bounded, actor-free public projection and retains the returned derived
 * body content-addressed.  Private/actor-specific channel surfaces remain on
 * Channel's authenticated request path. */
static char* channel_public_surface(const Opt*o,const char*channel){
    int p[2],status=0;pid_t child;char *out=0,*grown;size_t used=0,cap=4096;ssize_t n;
    if(!o->channel_bin||!o->channels||!o->cms||!channel||pipe(p))return 0;
    if((child=fork())<0){close(p[0]);close(p[1]);return 0;}
    if(!child){char*const av[]={(char*)o->channel_bin,"surface","--db",(char*)o->channels,"--cms-db",(char*)o->cms,"--channel",(char*)channel,"--actor","anonymous",0};close(p[0]);if(dup2(p[1],STDOUT_FILENO)<0)_exit(127);close(p[1]);execv(o->channel_bin,av);_exit(127);}
    close(p[1]);out=malloc(cap);if(!out){close(p[0]);waitpid(child,&status,0);return 0;}
    while((n=read(p[0],out+used,cap-used-1))>0){used+=(size_t)n;if(used+1==cap){if(cap>1024*1024){free(out);close(p[0]);waitpid(child,&status,0);return 0;}grown=realloc(out,cap*2);if(!grown){free(out);close(p[0]);waitpid(child,&status,0);return 0;}out=grown;cap*=2;}}
    close(p[0]);out[used]=0;if(n<0||waitpid(child,&status,0)<0||!WIFEXITED(status)||WEXITSTATUS(status)||!used){free(out);return 0;}return out;
}
/* The donor grammar consumer is intentionally stricter than a catalogue
 * lookup.  It reopens the canonical binding and basis, requires the queued
 * owner reference to match exactly, and records only a pending exact-owner
 * composition.  It cannot manufacture a Community, Workspace, relationship,
 * disclosure, renderer result, or effect from a donor-shaped handoff. */
static int interaction_surface(sqlite3*d,const Opt*o,const char*b){
    char *id=0,*own=0,*composition=0,*channel_projection=0;sqlite3*c=0;sqlite3_stmt*s=0;int ok=0,in_tx=0;char composition_digest[72]={0},channel_digest[72]={0};
    const char *binding,*source_state,*target_state="not_executed",*binding_q;char gid[256]={0},subject[512]={0},institution[512]={0},owner[512]={0},visibility[32]={0},verifier[256]={0},target[256]={0},source_kind[64]={0},source_ref[512]={0};
    if(!o->grammar||!fields(b,"interaction_grammar","InteractionGrammarCompiler:surface",&id,&own)||!pref(id,"interaction-grammar:"))goto out;
    binding=id+20;if(!*binding||sqlite3_open_v2(o->grammar,&c,SQLITE_OPEN_READONLY,0)!=SQLITE_OK)goto out;
    binding_q=table_has_column(c,"interaction_grammar_binding","source_kind")?"SELECT b.grammar_id,b.subject_ref,b.institution_ref,b.owner_ref,b.visibility,c.exact_verifier,c.lowerer_ref,COALESCE(b.source_kind,'manual'),COALESCE(b.source_ref,'') FROM interaction_grammar_binding b JOIN interaction_grammar_catalog c ON c.grammar_id=b.grammar_id WHERE b.binding_id=?1 AND b.state='active'":"SELECT b.grammar_id,b.subject_ref,b.institution_ref,b.owner_ref,b.visibility,c.exact_verifier,c.lowerer_ref,'manual','' FROM interaction_grammar_binding b JOIN interaction_grammar_catalog c ON c.grammar_id=b.grammar_id WHERE b.binding_id=?1 AND b.state='active'";
    if(sqlite3_prepare_v2(c,binding_q,-1,&s,0)!=SQLITE_OK)goto out;
    sqlite3_bind_text(s,1,binding,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(s)!=SQLITE_ROW)goto out;
    if(!sqlite3_column_text(s,0)||!sqlite3_column_text(s,1)||!sqlite3_column_text(s,3)||!sqlite3_column_text(s,4)||!sqlite3_column_text(s,5)||!sqlite3_column_text(s,6)||!sqlite3_column_text(s,7))goto out;
    snprintf(gid,sizeof(gid),"%s",sqlite3_column_text(s,0));snprintf(subject,sizeof(subject),"%s",sqlite3_column_text(s,1));snprintf(institution,sizeof(institution),"%s",sqlite3_column_text(s,2)?(const char*)sqlite3_column_text(s,2):"");snprintf(owner,sizeof(owner),"%s",sqlite3_column_text(s,3));snprintf(visibility,sizeof(visibility),"%s",sqlite3_column_text(s,4));snprintf(verifier,sizeof(verifier),"%s",sqlite3_column_text(s,5));snprintf(target,sizeof(target),"%s",sqlite3_column_text(s,6));snprintf(source_kind,sizeof(source_kind),"%s",sqlite3_column_text(s,7));snprintf(source_ref,sizeof(source_ref),"%s",sqlite3_column_text(s,8)?(const char*)sqlite3_column_text(s,8):"");
    sqlite3_finalize(s);s=0;
    if(strcmp(owner,own)||!interaction_source_current(o,source_kind,source_ref,owner,gid))goto out;
    source_state=!strcmp(source_kind,"manual")?"binding_row_current":"current_native_owner_row";
    if(!strcmp(source_kind,"creator_channel")&&o->channel_bin&&o->cms){channel_projection=channel_public_surface(o,source_ref);if(!channel_projection||!digest(channel_projection,channel_digest))goto out;target_state="executed_public_channel_surface";}
    if(!sql(d,"BEGIN IMMEDIATE;"))goto out;in_tx=1;
    if(channel_projection){
        if(sqlite3_prepare_v2(d,"INSERT INTO channel_surface_projection(identity,projection_digest,projection_json,compiled_at) VALUES(?1,?2,?3,?4) ON CONFLICT(identity) DO UPDATE SET projection_digest=excluded.projection_digest,projection_json=excluded.projection_json,compiled_at=excluded.compiled_at",-1,&s,0)!=SQLITE_OK)goto out;
        sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,channel_digest,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,channel_projection,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,4,(sqlite3_int64)time(0));ok=sqlite3_step(s)==SQLITE_DONE;sqlite3_finalize(s);s=0;if(!ok)goto out;
    }
    if(sqlite3_prepare_v2(d,"INSERT INTO interaction_surface_artifact(identity,owner_ref,grammar_id,subject_ref,institution_ref,visibility,exact_verifier,target_lowerer_ref,state,compiled_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'pending_exact_owner',?9) ON CONFLICT(identity) DO UPDATE SET owner_ref=excluded.owner_ref,grammar_id=excluded.grammar_id,subject_ref=excluded.subject_ref,institution_ref=excluded.institution_ref,visibility=excluded.visibility,exact_verifier=excluded.exact_verifier,target_lowerer_ref=excluded.target_lowerer_ref,state='pending_exact_owner',compiled_at=excluded.compiled_at",-1,&s,0)!=SQLITE_OK)goto out;
    sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,own,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,gid,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,5,institution,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,6,visibility,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,7,verifier,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,8,target,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,9,(sqlite3_int64)time(0));ok=sqlite3_step(s)==SQLITE_DONE;
    sqlite3_finalize(s);s=0;if(!ok)goto out;
    /* Let SQLite serialize the object so every string is escaped by the same
     * native JSON engine that verifies the handoff.  The digest identifies a
     * disposable derived composition, never its subject or its owner truth. */
    if(sqlite3_prepare_v2(d,"SELECT json_object('schema','bonfyre-interaction-surface-composition.v1','identity',?1,'owner_ref',?2,'grammar_id',?3,'subject_ref',?4,'institution_ref',?5,'visibility',?6,'exact_verifier',?7,'exact_verifier_state','unqueried','binding_source_kind',?8,'binding_source_ref',?9,'owner_binding_state',?10,'target_lowerer_ref',?11,'target_lowerer_state',?12,'state','pending_exact_owner','truth','composition_not_render_or_authority')",-1,&s,0)!=SQLITE_OK)goto out;
    sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,own,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,gid,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,5,institution,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,6,visibility,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,7,verifier,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,8,source_kind,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,9,source_ref,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,10,source_state,-1,SQLITE_STATIC);sqlite3_bind_text(s,11,target,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,12,target_state,-1,SQLITE_STATIC);
    if(sqlite3_step(s)!=SQLITE_ROW||!sqlite3_column_text(s,0))goto out;composition=strdup((const char*)sqlite3_column_text(s,0));sqlite3_finalize(s);s=0;if(!composition||!digest(composition,composition_digest))goto out;
    if(sqlite3_prepare_v2(d,"INSERT INTO interaction_surface_composition(identity,composition_digest,composition_json,exact_verifier_state,compiled_at) VALUES(?1,?2,?3,'unqueried',?4) ON CONFLICT(identity) DO UPDATE SET composition_digest=excluded.composition_digest,composition_json=excluded.composition_json,exact_verifier_state='unqueried',compiled_at=excluded.compiled_at",-1,&s,0)!=SQLITE_OK)goto out;
    sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,composition_digest,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,composition,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,4,(sqlite3_int64)time(0));ok=sqlite3_step(s)==SQLITE_DONE;sqlite3_finalize(s);s=0;if(!ok)goto out;
    ok=sql(d,"COMMIT;");in_tx=0;
out:if(s)sqlite3_finalize(s);if(in_tx)sql(d,"ROLLBACK;");if(c)sqlite3_close(c);free(id);free(own);free(composition);free(channel_projection);return ok;
}
static int consume(sqlite3*d,const Opt*o,const char*b,const char*part){if(pref(part,"MCP-")){char l[64];snprintf(l,sizeof(l),"MCP:%s",part+4);return mcp(d,o,b,l);}if(!strcmp(part,"ProtocolCompiler-late_carrier"))return protocol(d,o,b,"ProtocolCompiler:late_carrier");if(!strcmp(part,"ProviderCompiler-embedding"))return provider_embedding(d,o,b);if(!strcmp(part,"MediaLibraryCompiler-surface"))return media_library_surface(d,o,b);if(!strcmp(part,"InteractionGrammarCompiler-surface"))return interaction_surface(d,o,b);if(!strcmp(part,"MaterialCompiler-external"))return material_external(d,o,b);if(!strcmp(part,"MaterialCompiler-source"))return material(d,o,b,"MaterialCompiler:source");if(!strcmp(part,"MaterialCompiler-source_collection"))return material_collection(d,o,b);if(!strcmp(part,"MaterialCompiler-pack_demand"))return material_pack_demand(d,o,b);return 0;}
static int json_handoff_name(const char *name){size_t n=name?strlen(name):0;return n>=6&&!strcmp(name+n-5,".json");}
static int drain(sqlite3*d,const Opt*o){DIR*r,*q;struct dirent*e,*f;int seen=0,done=0,bad=0,retired=0;if(!mcp_retire_absent(d,o,&retired))return 0;if(!(r=opendir(o->inbox)))return 0;while((e=readdir(r))&&seen<o->limit){char folder[PATH_MAX];if(e->d_name[0]=='.'||snprintf(folder,sizeof(folder),"%s/%s",o->inbox,e->d_name)>=(int)sizeof(folder)||!(q=opendir(folder)))continue;while((f=readdir(q))&&seen<o->limit){char p[PATH_MAX],h[PATH_MAX],*b;if(f->d_name[0]=='.'||!json_handoff_name(f->d_name)||snprintf(p,sizeof(p),"%s/%s",folder,f->d_name)>=(int)sizeof(p))continue;seen++;b=slurp(p);if(b&&consume(d,o,b,e->d_name)&&snprintf(h,sizeof(h),"%s.handled",p)<(int)sizeof(h)&&rename(p,h)==0)done++;else bad++;free(b);}closedir(q);}closedir(r);printf("{\"worker\":\"lowering-consumers\",\"seen\":%d,\"handled\":%d,\"mcp_artifacts_retired\":%d,\"failed_or_deferred\":%d,\"truth\":\"derived_artifacts_and_observation_demands_only\"}\n",seen,done,retired,bad);return !bad;}
static void use(void){fputs("bonfyre-lowering-workers drain --db STORE --lowerer-dir DIR [--owner-contract-image IMAGE] [--mcp-closure JSON] [--protocols JSONL] [--cms DB] [--program-resolver SCRIPT --program-runtime BINARY] [--provider-db DB] [--media-library DB] [--creator-channel-db DB --creator-channel-bin PATH] [--interaction-grammar-db DB] [--external-estate DB --foreign-receipts DIR] [--demand-dir DIR] [--limit N]\n       bonfyre-lowering-workers context-manifest --rank-db DB --run-id ID --foreign-evidence JSONL [--foreign-query-evidence JSONL] --policy CLASS --out JSONL [--shadow-diagnostic]\n       bonfyre-lowering-workers foreign-verify --rank-db DB --run-id ID --foreign-evidence JSONL --foreign-db FILE --out JSONL [--shadow-evaluation]\n",stderr);}
int main(int ac,char**av){Opt o={0};sqlite3*d;if(ac<2){use();return 2;}if(!strcmp(av[1],"context-manifest")){const char*rank=0,*run=0,*evidence=0,*queries=0,*policy=0,*out=0;int shadow=0;for(int i=2;i<ac;i++){const char**p=0;if(!strcmp(av[i],"--rank-db"))p=&rank;else if(!strcmp(av[i],"--run-id"))p=&run;else if(!strcmp(av[i],"--foreign-evidence"))p=&evidence;else if(!strcmp(av[i],"--foreign-query-evidence"))p=&queries;else if(!strcmp(av[i],"--policy"))p=&policy;else if(!strcmp(av[i],"--out"))p=&out;else if(!strcmp(av[i],"--shadow-diagnostic")){shadow=1;continue;}else{use();return 2;}if(++i>=ac){use();return 2;}*p=av[i];}return context_manifest(rank,run,evidence,queries,policy,out,shadow)?0:1;}if(!strcmp(av[1],"foreign-verify")){const char*rank=0,*run=0,*evidence=0,*foreign=0,*out=0;int shadow=0;for(int i=2;i<ac;i++){const char**p=0;if(!strcmp(av[i],"--rank-db"))p=&rank;else if(!strcmp(av[i],"--run-id"))p=&run;else if(!strcmp(av[i],"--foreign-evidence"))p=&evidence;else if(!strcmp(av[i],"--foreign-db"))p=&foreign;else if(!strcmp(av[i],"--out"))p=&out;else if(!strcmp(av[i],"--shadow-evaluation")){shadow=1;continue;}else{use();return 2;}if(++i>=ac){use();return 2;}*p=av[i];}return foreign_verify(rank,run,evidence,foreign,out,shadow)?0:1;}if(strcmp(av[1],"drain")){use();return 2;}o.limit=64;o.program_resolver=getenv("BONFYRE_PROGRAM_RESOLVER");o.program_runtime=getenv("BONFYRE_BUN_BINARY");for(int i=2;i<ac;i++){const char**p=0;if(!strcmp(av[i],"--db"))p=&o.db;else if(!strcmp(av[i],"--lowerer-dir"))p=&o.inbox;else if(!strcmp(av[i],"--owner-contract-image"))p=&o.owner_image;else if(!strcmp(av[i],"--mcp-closure"))p=&o.closure;else if(!strcmp(av[i],"--protocols"))p=&o.protocols;else if(!strcmp(av[i],"--cms"))p=&o.cms;else if(!strcmp(av[i],"--program-resolver"))p=&o.program_resolver;else if(!strcmp(av[i],"--program-runtime"))p=&o.program_runtime;else if(!strcmp(av[i],"--provider-db"))p=&o.provider;else if(!strcmp(av[i],"--media-library"))p=&o.media;else if(!strcmp(av[i],"--creator-channel-db"))p=&o.channels;else if(!strcmp(av[i],"--creator-channel-bin"))p=&o.channel_bin;else if(!strcmp(av[i],"--interaction-grammar-db"))p=&o.grammar;else if(!strcmp(av[i],"--external-estate"))p=&o.external;else if(!strcmp(av[i],"--foreign-receipts"))p=&o.foreign_receipts;else if(!strcmp(av[i],"--demand-dir"))p=&o.demands;else if(!strcmp(av[i],"--limit")&&i+1<ac){o.limit=atoi(av[++i]);continue;}else{use();return 2;}if(++i>=ac){use();return 2;}*p=av[i];}if(!o.db||!o.inbox||o.limit<1||o.limit>4096||(o.channel_bin&&!o.channels)||(o.owner_image&&!owner_image_valid(o.owner_image))){use();return 2;}if(!(d=store(o.db)))return 1;int ok=drain(d,&o);sqlite3_close(d);return ok?0:1;}
