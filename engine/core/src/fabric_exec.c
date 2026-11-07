#define _POSIX_C_SOURCE 200809L
#include "bonfyre_fabric.h"
#include "bonfyre_fabric_internal.h"
#include "bf_filesystem.h"
#include "bf_authority.h"
#include "bf_operator.h"
#include "bf_operator_contract.h"
#include "bf_workgraph.h"
#include "bf_lifecycle.h"
#include "bf_json.h"
#include "bf_information_policy.h"
#include "bf_utf8.h"
#include "bonfyre.h"

#include <errno.h>
#include <stdbool.h>
#include <ctype.h>
#include <glob.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>



#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

static void stamp(char out[32]) { time_t t=time(NULL); struct tm *u=gmtime(&t); if(!u||!strftime(out,32,"%Y-%m-%dT%H:%M:%SZ",u)) snprintf(out,32,"1970-01-01T00:00:00Z"); }
static void hex_digest(const uint8_t digest[32], char out[65]) { static const char h[]="0123456789abcdef"; for(int i=0;i<32;i++){out[i*2]=h[digest[i]>>4];out[i*2+1]=h[digest[i]&15];}out[64]=0; }
static int mkdirs(const char *path) { char p[PATH_MAX]; size_t n=strlen(path); if(!n||n>=sizeof(p))return -1; memcpy(p,path,n+1); for(char *q=p+1;*q;q++)if(*q=='/'){*q=0;if(mkdir(p,0700)&&errno!=EEXIST)return -1;*q='/';}return mkdir(p,0700)&&errno!=EEXIST?-1:0; }
static void id(char *out,size_t n,const char *prefix,const char *seed){char raw[1024],h[65],ts[32];stamp(ts);snprintf(raw,sizeof(raw),"%s|%s|%s|%ld",prefix,seed?seed:"",ts,(long)getpid());bf_sha256_hex((const uint8_t*)raw,strlen(raw),h);snprintf(out,n,"%s-%.*s",prefix,20,h);}
static int step(sqlite3 *db,sqlite3_stmt *s,FILE *err){int rc=sqlite3_step(s);if(rc!=SQLITE_DONE)fprintf(err,"fabric: database write failed: %s\n",sqlite3_errmsg(db));sqlite3_finalize(s);return rc==SQLITE_DONE?0:-1;}
static int scalar(sqlite3 *db,const char *sql,const char *a,char *out,size_t n){sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,sql,-1,&s,NULL);sqlite3_bind_text(s,1,a,-1,SQLITE_TRANSIENT);int ok=sqlite3_step(s)==SQLITE_ROW;if(ok)snprintf(out,n,"%s",sqlite3_column_text(s,0)?(const char*)sqlite3_column_text(s,0):"");sqlite3_finalize(s);return ok;}
static int state_dir(char out[PATH_MAX],FILE *err){char db[PATH_MAX];return bf_fabric_bootstrap(out,PATH_MAX,db,sizeof(db),err);}
static char *read_text(const char *path,size_t *len){FILE*f=fopen(path,"rb");if(!f)return NULL;if(fseek(f,0,SEEK_END)){fclose(f);return NULL;}long z=ftell(f);if(z<0||fseek(f,0,SEEK_SET)){fclose(f);return NULL;}char*p=malloc((size_t)z+1);if(!p){fclose(f);return NULL;}if(fread(p,1,(size_t)z,f)!=(size_t)z){free(p);fclose(f);return NULL;}p[z]=0;fclose(f);if(len)*len=(size_t)z;return p;}
static int absolute_path(const char *path, char output[PATH_MAX]) {
    char cwd[PATH_MAX];
    if (!path || !path[0]) return -1;
    if (path[0] == '/') {
        if (strlen(path) >= PATH_MAX) return -1;
        snprintf(output, PATH_MAX, "%s", path);
        return 0;
    }
    if (!getcwd(cwd, sizeof(cwd))) return -1;
    if (snprintf(output, PATH_MAX, "%s/%s", cwd, path) >= PATH_MAX) return -1;
    return 0;
}

static int parent_directory(const char *path, char output[PATH_MAX]) {
    const char *slash = strrchr(path, '/');
    size_t length;
    if (!slash) return -1;
    length = (size_t)(slash - path);
    if (length == 0) length = 1;
    if (length >= PATH_MAX) return -1;
    memcpy(output, path, length);
    output[length] = '\0';
    return 0;
}

static int resolve_from(const char *base, const char *path, char output[PATH_MAX]) {
    if (!path || !path[0]) return -1;
    if (path[0] == '/') {
        if (strlen(path) >= PATH_MAX) return -1;
        snprintf(output, PATH_MAX, "%s", path);
        return 0;
    }
    return snprintf(output, PATH_MAX, "%s/%s", base, path) >= PATH_MAX ? -1 : 0;
}
static int record_decl(sqlite3 *db,const char *kind,const char *path,const char *generation,FILE *err){char h[65],ts[32];if(bf_sha256_file(path,h)){fprintf(err,"fabric: declaration missing: %s\n",path);return -1;}stamp(ts);sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"INSERT INTO declarations(path,kind,content_hash,generation,compiled_at) VALUES(?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET kind=excluded.kind,content_hash=excluded.content_hash,generation=excluded.generation,compiled_at=excluded.compiled_at",-1,&s,NULL);sqlite3_bind_text(s,1,path,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,kind,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,h,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,generation,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,5,ts,-1,SQLITE_TRANSIENT);return step(db,s,err);}
static int meta(sqlite3 *db,const char *key,const char *value,FILE *err){sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"INSERT INTO fabric_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",-1,&s,NULL);sqlite3_bind_text(s,1,key,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,value,-1,SQLITE_TRANSIENT);return step(db,s,err);}
static int namespace_row(sqlite3 *db,const char *uri,const char *kind,const char *native,const char *loc,const char *evidence,FILE *err){char ts[32];stamp(ts);sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO namespace_objects(uri,kind,owner,source_authority,native_id,version,locator,policy,sensitivity,freshness,evidence_state,operations,content_contract,query_contract,effect_contract,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",-1,&s,NULL);sqlite3_bind_text(s,1,uri,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,kind,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,"local-user",-1,SQLITE_STATIC);sqlite3_bind_text(s,4,"fabric-core",-1,SQLITE_STATIC);sqlite3_bind_text(s,5,native,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,6,"1",-1,SQLITE_STATIC);sqlite3_bind_text(s,7,loc,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,8,"default",-1,SQLITE_STATIC);sqlite3_bind_text(s,9,"standard",-1,SQLITE_STATIC);sqlite3_bind_text(s,10,"current",-1,SQLITE_STATIC);sqlite3_bind_text(s,11,evidence,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,12,"read,verify",-1,SQLITE_STATIC);sqlite3_bind_text(s,13,"fabric.v1",-1,SQLITE_STATIC);sqlite3_bind_text(s,14,"typed-lookup.v1",-1,SQLITE_STATIC);sqlite3_bind_text(s,15,"governed",-1,SQLITE_STATIC);sqlite3_bind_text(s,16,ts,-1,SQLITE_TRANSIENT);return step(db,s,err);}
static int ingest_file(sqlite3 *db,const char *path,const char *type,char uri[256],FILE *err){struct stat st;char h[65],ts[32];if(stat(path,&st)||!S_ISREG(st.st_mode)||bf_sha256_file(path,h)){fprintf(err,"fabric: cannot ingest produced file %s\n",path);return -1;}snprintf(uri,256,"bonfyre://artifact/%s",h);stamp(ts);if(namespace_row(db,uri,"artifact",h,path,"hashed",err))return -1;sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO artifacts(digest,uri,media_type,source_uri,locator,bytes,representation,created_at) VALUES(?,?,?,?,?,?,?,?)",-1,&s,NULL);sqlite3_bind_text(s,1,h,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,uri,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,type,-1,SQLITE_TRANSIENT);sqlite3_bind_null(s,4);sqlite3_bind_text(s,5,path,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,6,(sqlite3_int64)st.st_size);sqlite3_bind_text(s,7,"fabric-owned",-1,SQLITE_STATIC);sqlite3_bind_text(s,8,ts,-1,SQLITE_TRANSIENT);return step(db,s,err);}
static int receipt(sqlite3 *db,const char *kind,const char *subject,const char *payload,char out[64],FILE *err){char h[65],ts[32],uri[256];id(out,64,"rcpt",subject);bf_sha256_hex((const uint8_t*)payload,strlen(payload),h);stamp(ts);sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"INSERT INTO receipts(id,subject_kind,subject_id,content_hash,payload,created_at) VALUES(?,?,?,?,?,?)",-1,&s,NULL);sqlite3_bind_text(s,1,out,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,kind,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,h,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,5,payload,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,6,ts,-1,SQLITE_TRANSIENT);if(step(db,s,err))return -1;snprintf(uri,sizeof(uri),"bonfyre://receipt/%s",out);return namespace_row(db,uri,"receipt",out,"fabric:receipt","hashed",err);}

static int upsert_root(sqlite3 *db,const char *rid,const char *kind,const char *locator,const char *authority,const char *mode,FILE *err){char ts[32];stamp(ts);sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"INSERT INTO roots(id,kind,locator,owner,authority_class,durability,trust_level,sensitivity,access_mode,retention,backup_policy,hash_strategy,watch_strategy,materialization_policy,runtime_visibility,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,locator=excluded.locator,authority_class=excluded.authority_class,access_mode=excluded.access_mode,created_at=excluded.created_at",-1,&s,NULL);sqlite3_bind_text(s,1,rid,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,kind,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,locator,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,"local-user",-1,SQLITE_STATIC);sqlite3_bind_text(s,5,authority,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,6,"durable",-1,SQLITE_STATIC);sqlite3_bind_text(s,7,"local",-1,SQLITE_STATIC);sqlite3_bind_text(s,8,"standard",-1,SQLITE_STATIC);sqlite3_bind_text(s,9,mode,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,10,"policy-default",-1,SQLITE_STATIC);sqlite3_bind_text(s,11,"declared",-1,SQLITE_STATIC);sqlite3_bind_text(s,12,"sha256",-1,SQLITE_STATIC);sqlite3_bind_text(s,13,"bounded",-1,SQLITE_STATIC);sqlite3_bind_text(s,14,"reference",-1,SQLITE_STATIC);sqlite3_bind_text(s,15,"scoped",-1,SQLITE_STATIC);sqlite3_bind_text(s,16,ts,-1,SQLITE_TRANSIENT);return step(db,s,err);}
static int compile_workspace(sqlite3 *db,const char *path,const char *gen,FILE *err){size_t z;char *txt=read_text(path,&z);if(!txt){fprintf(err,"fabric: cannot read workspace %s\n",path);return -1;}char repo[PATH_MAX],state[PATH_MAX];if(parent_directory(path,repo)||state_dir(state,err)){free(txt);return -1;}/* These hold tokens parsed out of a workspace line, and a line can be up to
   1023 bytes. At 128/64/96 a long identifier was silently truncated into a
   different, valid-looking id -- snprintf is memory-safe but the resulting
   root would simply be the wrong one. Sized so no valid line can truncate. */
    char *save=NULL,*line=strtok_r(txt,"\n",&save),rid[1024]="",kind[1024]="",locator[PATH_MAX]="",authority[1024]="governed",mode[1024]="read-write";int count=0;while(line){char *p=line;while(*p==' '||*p=='\t')p++;if(!strncmp(p,"root ",5)){if(rid[0]){char resolved[PATH_MAX];if(!strcmp(locator,"platform-default")){if(!strcmp(kind,"state"))snprintf(resolved,sizeof(resolved),"%s",state);else snprintf(resolved,sizeof(resolved),"%s/%s",state,rid);}else if(resolve_from(repo,locator,resolved)){fprintf(err,"fabric: cannot resolve root '%s' locator '%s' relative to %s\n",rid,locator,repo);free(txt);return -1;}if(strcmp(kind,"source"))mkdirs(resolved);if(upsert_root(db,rid,kind,resolved,authority,mode,err)){free(txt);return -1;}count++;}if(sscanf(p,"root %127s",rid)!=1){free(txt);return -1;}kind[0]=locator[0]=0;snprintf(authority,sizeof(authority),"governed");snprintf(mode,sizeof(mode),"read-write");}else if(rid[0]){char k[64],v[1024];if(sscanf(p,"%63s %1023s",k,v)==2){if(!strcmp(k,"kind"))snprintf(kind,sizeof(kind),"%s",v);else if(!strcmp(k,"locator"))snprintf(locator,sizeof(locator),"%s",v);else if(!strcmp(k,"authority"))snprintf(authority,sizeof(authority),"%s",v);else if(!strcmp(k,"mode"))snprintf(mode,sizeof(mode),"%s",v);}}line=strtok_r(NULL,"\n",&save);}if(rid[0]){char resolved[PATH_MAX];if(!strcmp(locator,"platform-default")){if(!strcmp(kind,"state"))snprintf(resolved,sizeof(resolved),"%s",state);else snprintf(resolved,sizeof(resolved),"%s/%s",state,rid);}else if(resolve_from(repo,locator,resolved)){fprintf(err,"fabric: cannot resolve root '%s' locator '%s' relative to %s\n",rid,locator,repo);free(txt);return -1;}if(strcmp(kind,"source"))mkdirs(resolved);if(upsert_root(db,rid,kind,resolved,authority,mode,err)){free(txt);return -1;}count++;}free(txt);if(!count){fprintf(err,"fabric: workspace has no roots\n");return -1;}return record_decl(db,"workspace",path,gen,err);}

static int bind(sqlite3 *db,const char *op,const char *binary,const char *source,const char *gen,FILE *err){char h[65]="";if(source&&access(source,R_OK)==0)bf_sha256_file(source,h);sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"INSERT INTO catalog_bindings(operator_id,binary_name,source_path,installed_path,source_hash,input_contract,output_contract,file_inputs,streaming,cwd_contract,environment_contract,timeout_seconds,cancellation,retry_behavior,resource_requirements,health_probe,workload_probe,quality_probe,produced_families,catalog_generation,binding_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(operator_id) DO UPDATE SET binary_name=excluded.binary_name,source_path=excluded.source_path,source_hash=excluded.source_hash,catalog_generation=excluded.catalog_generation,binding_state=excluded.binding_state",-1,&s,NULL);sqlite3_bind_text(s,1,op,-1,SQLITE_TRANSIENT);if(binary)sqlite3_bind_text(s,2,binary,-1,SQLITE_TRANSIENT);else sqlite3_bind_null(s,2);if(source)sqlite3_bind_text(s,3,source,-1,SQLITE_TRANSIENT);else sqlite3_bind_null(s,3);sqlite3_bind_null(s,4);sqlite3_bind_text(s,5,h,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,6,"artifact.v1",-1,SQLITE_STATIC);sqlite3_bind_text(s,7,"artifact.v1",-1,SQLITE_STATIC);sqlite3_bind_text(s,8,"optional",-1,SQLITE_STATIC);sqlite3_bind_text(s,9,"false",-1,SQLITE_STATIC);sqlite3_bind_text(s,10,"mission-directory",-1,SQLITE_STATIC);sqlite3_bind_text(s,11,"scoped",-1,SQLITE_STATIC);sqlite3_bind_int(s,12,30);sqlite3_bind_text(s,13,"process-group",-1,SQLITE_STATIC);sqlite3_bind_text(s,14,"bounded:0",-1,SQLITE_STATIC);sqlite3_bind_text(s,15,"cpu:1",-1,SQLITE_STATIC);sqlite3_bind_text(s,16,"explicit",-1,SQLITE_STATIC);sqlite3_bind_text(s,17,"explicit",-1,SQLITE_STATIC);sqlite3_bind_text(s,18,"explicit",-1,SQLITE_STATIC);sqlite3_bind_text(s,19,"derived-artifact",-1,SQLITE_STATIC);sqlite3_bind_text(s,20,gen,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,21,binary&&source&&access(source,X_OK)==0?"bound":"declared-unbound",-1,SQLITE_STATIC);return step(db,s,err);}
static int catalog_row(sqlite3*db,const char*i,const char*n,const char*v,const char*in,const char*outp,const char*effect,const char*auth,const char*lane,const char*mat,const char*gen,FILE*err){char ts[32];stamp(ts);sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"INSERT INTO catalog(id,kind,name,version,input_schema,output_schema,effect_class,authorization_class,execution_lane,idempotency,retry_semantics,timeout_seconds,source_ref,maturity,health_state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,version=excluded.version,input_schema=excluded.input_schema,output_schema=excluded.output_schema,effect_class=excluded.effect_class,authorization_class=excluded.authorization_class,execution_lane=excluded.execution_lane,maturity=excluded.maturity,source_ref=excluded.source_ref,health_state='declared'",-1,&s,NULL);sqlite3_bind_text(s,1,i,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,"operator",-1,SQLITE_STATIC);sqlite3_bind_text(s,3,n,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,v,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,5,in,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,6,outp,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,7,effect,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,8,auth,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,9,lane,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,10,"idempotent",-1,SQLITE_STATIC);sqlite3_bind_text(s,11,"bounded:0",-1,SQLITE_STATIC);sqlite3_bind_int(s,12,30);sqlite3_bind_text(s,13,gen,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,14,mat,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,15,"declared",-1,SQLITE_STATIC);sqlite3_bind_text(s,16,ts,-1,SQLITE_TRANSIENT);return step(db,s,err);}
/*
 * Repository-root governance: a path physically beneath the Bonfyre tree
 * (a Frappe app, HVM4, ...) can belong to its own independent Git
 * repository -- physical containment is not Git ownership. This gives
 * each real sibling repository a stable root identity (roots.kind=
 * 'source'), a real Git identity (repository_roots: git dir, worktree,
 * HEAD OID, branch, dirty-state digest), and a namespace URI
 * (bonfyre://source/<slug>, plus an immutable revision-qualified form
 * bonfyre://source/<slug>@<oid>) -- all resolved by shelling out to the
 * real `git` binary against the real checkout, never inferred or faked.
 * Discovery is best-effort: if the known parent directories don't exist
 * on this checkout, zero repositories are found and compile proceeds
 * normally (these are environment-dependent integrations, unlike
 * estate/providers.yaff which is always committed).
 */
static int run_capture(char *const arguments[], char *output, size_t output_size) {
    int descriptors[2];
    pid_t child;
    size_t used = 0;
    int status = 0;

    if (pipe(descriptors) != 0) return -1;
    child = fork();
    if (child == 0) {
        close(descriptors[0]);
        dup2(descriptors[1], STDOUT_FILENO);
        close(STDERR_FILENO);
        close(descriptors[1]);
        execvp(arguments[0], arguments);
        _exit(127);
    }
    close(descriptors[1]);
    if (child < 0) { close(descriptors[0]); return -1; }
    for (;;) {
        char buffer[512];
        ssize_t bytes = read(descriptors[0], buffer, sizeof(buffer));
        if (bytes <= 0) break;
        if (output && output_size > 0 && used < output_size - 1) {
            size_t available = output_size - 1 - used;
            size_t copied = (size_t)bytes < available ? (size_t)bytes : available;
            memcpy(output + used, buffer, copied);
            used += copied;
        }
    }
    close(descriptors[0]);
    if (output && output_size > 0) output[used] = '\0';
    if (waitpid(child, &status, 0) != child) return -1;
    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : -1;
}

static void rtrim_newline(char *s) {
    size_t n = strlen(s);
    while (n > 0 && (s[n-1] == '\n' || s[n-1] == '\r')) s[--n] = '\0';
}

static int valid_slug(const char *value) {
    if (!value || !value[0] || strlen(value) > 120) return 0;
    for (const unsigned char *p = (const unsigned char *)value; *p; ++p)
        if (!(isalnum(*p) || *p == '-' || *p == '_' || *p == '.')) return 0;
    return 1;
}

static int register_repository_root(sqlite3 *db, const char *slug, const char *path, FILE *err) {
    char root_id[192], git_dir[PATH_MAX], head_oid[128] = "", branch[256] = "",
         porcelain[8192] = "", dirty_hex[65], source_material[512], source_generation[65],
         ts[32], namespace_uri[256], revision_uri[320];

    snprintf(root_id, sizeof(root_id), "source.%s", slug);

    {
        char *arguments[] = {"git", "-C", (char *)path, "rev-parse", "--absolute-git-dir", NULL};
        if (run_capture(arguments, git_dir, sizeof(git_dir)) != 0) {
            fprintf(err, "fabric: cannot resolve git dir for sibling repository %s\n", path);
            return -1;
        }
        rtrim_newline(git_dir);
    }
    {
        char *arguments[] = {"git", "-C", (char *)path, "rev-parse", "HEAD", NULL};
        if (run_capture(arguments, head_oid, sizeof(head_oid)) != 0) {
            fprintf(err, "fabric: cannot resolve HEAD for sibling repository %s\n", path);
            return -1;
        }
        rtrim_newline(head_oid);
    }
    {
        char *arguments[] = {"git", "-C", (char *)path, "rev-parse", "--abbrev-ref", "HEAD", NULL};
        if (run_capture(arguments, branch, sizeof(branch)) != 0) {
            fprintf(err, "fabric: cannot resolve branch for sibling repository %s\n", path);
            return -1;
        }
        rtrim_newline(branch);
    }
    {
        /* Dirty state is recorded, never mutated: `git status --porcelain`
         * only reads. A clean tree hashes the empty string deterministically. */
        char *arguments[] = {"git", "-C", (char *)path, "status", "--porcelain", NULL};
        if (run_capture(arguments, porcelain, sizeof(porcelain)) != 0) {
            fprintf(err, "fabric: cannot resolve dirty state for sibling repository %s\n", path);
            return -1;
        }
    }
    bf_sha256_hex((const uint8_t *)porcelain, strlen(porcelain), dirty_hex);
    snprintf(source_material, sizeof(source_material), "%s|%s", head_oid, dirty_hex);
    bf_sha256_hex((const uint8_t *)source_material, strlen(source_material), source_generation);
    stamp(ts);

    if (upsert_root(db, root_id, "source", path, "repository", "read-only", err)) return -1;

    {
        sqlite3_stmt *s = NULL;
        sqlite3_prepare_v2(db,
            "INSERT INTO repository_roots(root_id,git_dir,worktree_locator,head_oid,branch,"
            "dirty_state_digest,dirty,source_generation,parent_root_id,discovered_at) "
            "VALUES(?,?,?,?,?,?,?,?,NULL,?) ON CONFLICT(root_id) DO UPDATE SET "
            "git_dir=excluded.git_dir,worktree_locator=excluded.worktree_locator,"
            "head_oid=excluded.head_oid,branch=excluded.branch,"
            "dirty_state_digest=excluded.dirty_state_digest,dirty=excluded.dirty,"
            "source_generation=excluded.source_generation,discovered_at=excluded.discovered_at",
            -1, &s, NULL);
        sqlite3_bind_text(s, 1, root_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 2, git_dir, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 3, path, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 4, head_oid, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 5, branch, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 6, dirty_hex, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(s, 7, porcelain[0] != '\0');
        sqlite3_bind_text(s, 8, source_generation, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 9, ts, -1, SQLITE_TRANSIENT);
        if (step(db, s, err)) return -1;
    }

    snprintf(namespace_uri, sizeof(namespace_uri), "bonfyre://source/%s", slug);
    snprintf(revision_uri, sizeof(revision_uri), "bonfyre://source/%s@%s", slug, head_oid);
    if (namespace_row(db, namespace_uri, "source-repository", root_id, path, "discovered", err)) return -1;
    if (namespace_row(db, revision_uri, "source-repository-revision", root_id, path, source_generation, err)) return -1;
    return 0;
}

static int discover_repository_roots(sqlite3 *db, const char *workspace_repo_root, FILE *err) {
    static const char *parents[] = {"integrations/frappe-bench/apps", "vendor", NULL};
    for (int p = 0; parents[p]; p++) {
        char pattern[PATH_MAX];
        glob_t results;
        snprintf(pattern, sizeof(pattern), "%s/%s/*", workspace_repo_root, parents[p]);
        if (glob(pattern, 0, NULL, &results) != 0) continue;
        for (size_t i = 0; i < results.gl_pathc; i++) {
            const char *candidate = results.gl_pathv[i];
            char git_marker[PATH_MAX];
            struct stat metadata;
            snprintf(git_marker, sizeof(git_marker), "%s/.git", candidate);
            if (stat(git_marker, &metadata) != 0) continue;
            const char *slash = strrchr(candidate, '/');
            const char *slug = slash ? slash + 1 : candidate;
            if (!valid_slug(slug)) continue;
            if (register_repository_root(db, slug, candidate, err) != 0) {
                globfree(&results);
                return -1;
            }
        }
        globfree(&results);
    }
    return 0;
}

/* Catalog identity + RuntimeImage contract for external providers (Restate,
 * Feldera, ...): a real catalog row (kind='provider', source_ref=runtime
 * image reference) plus a real root registration under authority
 * runtime-image, declared in estate/providers.yaff and compiled every
 * `fabric compile` alongside operators. This is the identity surface a
 * provider occupies whether or not the provider process itself is running
 * locally right now. */
static int provider_row(sqlite3*db,const char*i,const char*n,const char*image,const char*in,const char*outp,const char*effect,const char*auth,const char*lane,const char*mat,const char*health,FILE*err){char ts[32];stamp(ts);sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"INSERT INTO catalog(id,kind,name,version,input_schema,output_schema,effect_class,authorization_class,execution_lane,idempotency,retry_semantics,timeout_seconds,source_ref,maturity,health_state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,input_schema=excluded.input_schema,output_schema=excluded.output_schema,effect_class=excluded.effect_class,authorization_class=excluded.authorization_class,execution_lane=excluded.execution_lane,maturity=excluded.maturity,source_ref=excluded.source_ref,health_state=excluded.health_state",-1,&s,NULL);sqlite3_bind_text(s,1,i,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,"provider",-1,SQLITE_STATIC);sqlite3_bind_text(s,3,n,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,"1.0.0",-1,SQLITE_STATIC);sqlite3_bind_text(s,5,in,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,6,outp,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,7,effect,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,8,auth,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,9,lane,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,10,"idempotent",-1,SQLITE_STATIC);sqlite3_bind_text(s,11,"bounded:0",-1,SQLITE_STATIC);sqlite3_bind_int(s,12,30);sqlite3_bind_text(s,13,image,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,14,mat,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,15,health,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,16,ts,-1,SQLITE_TRANSIENT);return step(db,s,err);}
static int compile_providers(sqlite3*db,const char*path,const char*gen,FILE*err){size_t z;char*t=read_text(path,&z);if(!t){fprintf(err,"fabric: cannot read providers %s\n",path);return -1;}char*save=NULL,*line=strtok_r(t,"\n",&save);char i[160]="",n[256]="",image[512]="",in[256]="",outp[256]="",effect[96]="",auth[96]="mission-scoped",lane[96]="external-process",mat[96]="declared",health[96]="declared";int count=0;
    while(line){char*p=line;while(*p==' '||*p=='\t')p++;
        if(!strncmp(p,"provider ",9)){
            if(i[0]){char puri[256];snprintf(puri,sizeof(puri),"bonfyre://provider/%s",i+9);if(!image[0]||provider_row(db,i,n,image,in,outp,effect,auth,lane,mat,health,err)||upsert_root(db,i,"provider",image,"runtime-image","read-only",err)||namespace_row(db,puri,"provider",i,image,mat,err)){free(t);return -1;}count++;}
            char rid[128];if(sscanf(p,"provider %127s",rid)!=1){free(t);return -1;}
            snprintf(i,sizeof(i),"provider.%s",rid);snprintf(n,sizeof(n),"%s",rid);image[0]=in[0]=outp[0]=effect[0]=0;
            snprintf(auth,sizeof(auth),"mission-scoped");snprintf(lane,sizeof(lane),"external-process");snprintf(mat,sizeof(mat),"declared");snprintf(health,sizeof(health),"declared");
            line=strtok_r(NULL,"\n",&save); continue;
        }
        if(!i[0]){line=strtok_r(NULL,"\n",&save);continue;}
        char k[64],v[512];
        if(sscanf(p," %63s %511[^\n]",k,v)==2){
            if(!strcmp(k,"name"))snprintf(n,sizeof(n),"%s",v);
            else if(!strcmp(k,"runtime_image"))snprintf(image,sizeof(image),"%s",v);
            else if(!strcmp(k,"input"))snprintf(in,sizeof(in),"%s",v);
            else if(!strcmp(k,"output"))snprintf(outp,sizeof(outp),"%s",v);
            else if(!strcmp(k,"effect"))snprintf(effect,sizeof(effect),"%s",v);
            else if(!strcmp(k,"authorization"))snprintf(auth,sizeof(auth),"%s",v);
            else if(!strcmp(k,"lane"))snprintf(lane,sizeof(lane),"%s",v);
            else if(!strcmp(k,"maturity"))snprintf(mat,sizeof(mat),"%s",v);
            else if(!strcmp(k,"health"))snprintf(health,sizeof(health),"%s",v);
        }
        line=strtok_r(NULL,"\n",&save);
    }
    if(i[0]){char puri[256];snprintf(puri,sizeof(puri),"bonfyre://provider/%s",i+9);if(!image[0]||provider_row(db,i,n,image,in,outp,effect,auth,lane,mat,health,err)||upsert_root(db,i,"provider",image,"runtime-image","read-only",err)||namespace_row(db,puri,"provider",i,image,mat,err)){free(t);return -1;}count++;}
    free(t);
    if(!count){fprintf(err,"fabric: providers declaration has no providers\n");return -1;}
    return record_decl(db,"providers",path,gen,err);
}
static int compile_catalog(sqlite3*db,const char*path,const char*gen,FILE*err){size_t z;char*t=read_text(path,&z);if(!t)return -1;char*save=NULL,*line=strtok_r(t,"\n",&save),i[160]="",v[64]="1.0.0",in[256]="artifact.v1",outp[256]="artifact.v1",effect[96]="pure-read",auth[96]="mission-scoped",lane[96]="",mat[96]="defined";int n=0;while(line){char*p=line;while(*p==' '||*p=='\t')p++;if(!strncmp(p,"operator ",9)){if(i[0]){if(!lane[0]||catalog_row(db,i,i,v,in,outp,effect,auth,lane,mat,gen,err)||bind(db,i,NULL,NULL,gen,err)){free(t);return -1;}n++;}if(sscanf(p,"operator %159s",i)!=1){free(t);return -1;}snprintf(v,sizeof(v),"1.0.0");snprintf(in,sizeof(in),"artifact.v1");snprintf(outp,sizeof(outp),"artifact.v1");snprintf(effect,sizeof(effect),"pure-read");snprintf(auth,sizeof(auth),"mission-scoped");lane[0]=0;snprintf(mat,sizeof(mat),"defined");}else if(i[0]){char k[64],x[256];if(sscanf(p,"%63s %255s",k,x)==2){if(!strcmp(k,"version"))snprintf(v,sizeof(v),"%s",x);else if(!strcmp(k,"input"))snprintf(in,sizeof(in),"%s",x);else if(!strcmp(k,"output"))snprintf(outp,sizeof(outp),"%s",x);else if(!strcmp(k,"effect"))snprintf(effect,sizeof(effect),"%s",x);else if(!strcmp(k,"authorization"))snprintf(auth,sizeof(auth),"%s",x);else if(!strcmp(k,"lane"))snprintf(lane,sizeof(lane),"%s",x);else if(!strcmp(k,"maturity"))snprintf(mat,sizeof(mat),"%s",x);}}line=strtok_r(NULL,"\n",&save);}if(i[0]){if(!lane[0]||catalog_row(db,i,i,v,in,outp,effect,auth,lane,mat,gen,err)||bind(db,i,NULL,NULL,gen,err)){free(t);return -1;}n++;}free(t);if(!n){fprintf(err,"fabric: catalog has no operators\n");return -1;}return record_decl(db,"catalog",path,gen,err);}
static int compile_legacy(sqlite3*db,const char*path,const char*gen,FILE*err){size_t z;char*t=read_text(path,&z);if(!t)return -1;char estate_directory[PATH_MAX],workspace_directory[PATH_MAX];if(parent_directory(path,estate_directory)||parent_directory(estate_directory,workspace_directory)){free(t);return -1;};char contracts_path[PATH_MAX],bindings_path[PATH_MAX];snprintf(contracts_path,sizeof(contracts_path),"%s/operator-contracts.yaff",estate_directory);snprintf(bindings_path,sizeof(bindings_path),"%s/operator-contract-bindings.tsv",estate_directory);if(bf_operator_contracts_compile(db,contracts_path,gen,err)){free(t);return -1;}char*save=NULL,*l=strtok_r(t,"\n",&save);int n=0;while(l){if(l[0]!='#'&&l[0]){char*c=strtok(l,"\t"),*b=strtok(NULL,"\t"),*m=strtok(NULL,"\r\n");if(!c||!b||!m){free(t);return -1;}char op[180],src[PATH_MAX],built[PATH_MAX];snprintf(op,sizeof(op),"command.%s",c);snprintf(built,sizeof(built),"%s/cmd/%s/build/%s",workspace_directory,m,b);snprintf(src,sizeof(src),"%s/cmd/%s/%s",workspace_directory,m,b);if(access(built,X_OK)==0)snprintf(src,sizeof(src),"%s",built);if(catalog_row(db,op,c,"1.0.0","artifact.v1","artifact.v1","pure-read","mission-scoped","process","defined",gen,err)||bind(db,op,b,src,gen,err)){free(t);return -1;}n++;}l=strtok_r(NULL,"\n",&save);}if(bf_operator_contract_bindings_compile(db,bindings_path,gen,err)||record_decl(db,"operator-contracts",contracts_path,gen,err)||record_decl(db,"operator-contract-bindings",bindings_path,gen,err)){free(t);return -1;}free(t);if(n!=93){fprintf(err,"fabric: expected 93 public identities, got %d\n",n);return -1;}return record_decl(db,"legacy-operators",path,gen,err);}
typedef struct BfParsedComposition {
    char id[160];
    char node[128][128];
    char operator_id[128][160];
    char dependencies[128][512];
    int count;
} BfParsedComposition;

static int parsed_node(const BfParsedComposition *composition, const char *node_id) {
    for (int index = 0; index < composition->count; ++index)
        if (!strcmp(composition->node[index], node_id)) return index;
    return -1;
}

static int dependency_cycle(const BfParsedComposition *composition, int node, int *marks) {
    char dependencies[512];
    char *save = NULL;
    char *dependency;
    if (marks[node] == 1) return 1;
    if (marks[node] == 2) return 0;
    marks[node] = 1;
    snprintf(dependencies, sizeof(dependencies), "%s", composition->dependencies[node]);
    for (dependency = strtok_r(dependencies, ",", &save); dependency;
         dependency = strtok_r(NULL, ",", &save)) {
        int parent = parsed_node(composition, dependency);
        if (parent < 0 || dependency_cycle(composition, parent, marks)) return 1;
    }
    marks[node] = 2;
    return 0;
}

static int flush_composition(sqlite3 *db, const BfParsedComposition *composition, FILE *err) {
    int marks[128] = {0};
    if (!composition->id[0]) return 0;
    if (composition->count == 0) { fprintf(err, "fabric: composition %s has no nodes\n", composition->id); return -1; }
    for (int index = 0; index < composition->count; ++index)
        if (dependency_cycle(composition, index, marks)) { fprintf(err, "fabric: composition %s has an invalid dependency cycle\n", composition->id); return -1; }
    for (int index = 0; index < composition->count; ++index) {
        sqlite3_stmt *statement = NULL;
        sqlite3_prepare_v2(db, "INSERT INTO composition_nodes(composition_id,node_id,operator_id,depends_on) VALUES(?,?,?,?)", -1, &statement, NULL);
        sqlite3_bind_text(statement, 1, composition->id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, composition->node[index], -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 3, composition->operator_id[index], -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 4, composition->dependencies[index], -1, SQLITE_TRANSIENT);
        if (step(db, statement, err)) return -1;
    }
    return 0;
}

static int compile_compositions(sqlite3 *db, const char *path, const char *generation, FILE *err) {
    size_t size;
    char *text = read_text(path, &size);
    char *save = NULL;
    char *line;
    BfParsedComposition composition = {0};
    int compiled = 0;
    if (!text) return -1;
    sqlite3_exec(db, "DELETE FROM composition_nodes", NULL, NULL, NULL);
    for (line = strtok_r(text, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
        char *trimmed = line;
        while (*trimmed == ' ' || *trimmed == '\t') ++trimmed;
        if (*trimmed == '\0' || *trimmed == '#') continue;
        if (!strncmp(trimmed, "composition ", 12)) {
            if (flush_composition(db, &composition, err)) { free(text); return -1; }
            if (composition.id[0]) ++compiled;
            memset(&composition, 0, sizeof(composition));
            if (sscanf(trimmed, "composition %159s", composition.id) != 1) { free(text); return -1; }
        } else if (!strncmp(trimmed, "node ", 5)) {
            char node_id[128], keyword[32], operator_id[160];
            if (!composition.id[0] || composition.count == 128 ||
                sscanf(trimmed, "node %127s %31s %159s", node_id, keyword, operator_id) != 3 ||
                strcmp(keyword, "operator") || parsed_node(&composition, node_id) >= 0) { free(text); return -1; }
            snprintf(composition.node[composition.count], sizeof(composition.node[0]), "%s", node_id);
            snprintf(composition.operator_id[composition.count], sizeof(composition.operator_id[0]), "%s", operator_id);
            ++composition.count;
        } else if (!strncmp(trimmed, "dependency ", 11)) {
            char node_id[128], parent_id[128];
            int node;
            if (sscanf(trimmed, "dependency %127s %127s", node_id, parent_id) != 2 ||
                (node = parsed_node(&composition, node_id)) < 0 || parsed_node(&composition, parent_id) < 0) { free(text); return -1; }
            if (composition.dependencies[node][0]) strncat(composition.dependencies[node], ",", sizeof(composition.dependencies[node]) - strlen(composition.dependencies[node]) - 1);
            strncat(composition.dependencies[node], parent_id, sizeof(composition.dependencies[node]) - strlen(composition.dependencies[node]) - 1);
        } else if (!strncmp(trimmed, "input ", 6) ||
                   !strncmp(trimmed, "output ", 7) ||
                   !strncmp(trimmed, "terminal ", 9) ||
                   !strncmp(trimmed, "retry ", 6) ||
                   !strncmp(trimmed, "timeout ", 8) ||
                   !strncmp(trimmed, "effect ", 7)) {
            /* Composition-level contracts are preserved by the declaration
             * hash until their typed columns are introduced in migration 2. */
            continue;
        } else { free(text); return -1; }
    }
    if (flush_composition(db, &composition, err)) { free(text); return -1; }
    if (composition.id[0]) ++compiled;
    free(text);
    if (!compiled) { fprintf(err, "fabric: no compositions declared\n"); return -1; }
    return record_decl(db, "compositions", path, generation, err);
}
static int validate_lock(const char *path, char source_hash[65], FILE *err) {
    size_t size;
    char *text = read_text(path, &size);
    char directory[PATH_MAX];
    char source_path[PATH_MAX];
    int seen_lock = 0, seen_schema = 0, seen_runtime = 0, seen_source = 0;
    char *save = NULL;
    char *line;
    if (!text || parent_directory(path, directory)) {
        fprintf(err, "fabric: cannot read lock %s\n", path);
        free(text);
        return -1;
    }
    for (line = strtok_r(text, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
        char key[64] = "", value[PATH_MAX] = "";
        if (line[0] == '\0' || line[0] == '#') continue;
        if (sscanf(line, "%63s %1023s", key, value) != 2) { free(text); return -1; }
        if (!strcmp(key, "lock")) seen_lock = !strcmp(value, "Bonfyre");
        else if (!strcmp(key, "schema")) seen_schema = !strcmp(value, "fabric-1");
        else if (!strcmp(key, "runtime")) seen_runtime = !strcmp(value, "bonfyre-fabric-0.1.0");
        else if (!strcmp(key, "source")) {
            if (seen_source) { fprintf(err, "fabric: lock declares more than one source\n"); free(text); return -1; }
            if (resolve_from(directory, value, source_path) || bf_sha256_file(source_path, source_hash)) {
                fprintf(err, "fabric: lock-pinned source is unreadable: %s\n", value);
                free(text); return -1;
            }
            seen_source = 1;
        } else if (strcmp(key, "generation") && strcmp(key, "hash") && strcmp(key, "sqlite")) {
            fprintf(err, "fabric: unrecognized lock key: %s\n", key);
            free(text); return -1;
        }
    }
    free(text);
    if (!seen_lock || !seen_schema || !seen_runtime || !seen_source) {
        fprintf(err, "fabric: lock is incompatible with this runtime or pinned source is missing\n");
        return -1;
    }
    return 0;
}

static int compile_all(sqlite3*db,int argc,char**argv,FILE*out,FILE*err){
    const char *workspace_arg=argc>2?argv[2]:"bonfyre.workspace.yaff";
    const char *lock_arg=argc>3?argv[3]:"bonfyre.lock.yaff";
    const char *catalog_arg=argc>4?argv[4]:"estate/catalog.yaff";
    const char *composition_arg=argc>5?argv[5]:"estate/compositions.yaff";
    const char *profile_arg=argc>6?argv[6]:"estate/profiles.yaff";
    const char *legacy_arg=argc>7?argv[7]:"estate/legacy-operators.tsv";
    const char *providers_arg=argc>8?argv[8]:"estate/providers.yaff";
    char workspace[PATH_MAX], directory[PATH_MAX], lock[PATH_MAX], catalog[PATH_MAX], composition[PATH_MAX], profile[PATH_MAX], legacy[PATH_MAX], contracts[PATH_MAX], contract_bindings[PATH_MAX], providers[PATH_MAX], source_hash[65];
    const char *paths[9]; BfSha256 ctx; uint8_t digest[32]; char generation[65];
    if (absolute_path(workspace_arg, workspace) || parent_directory(workspace, directory)) {
        fprintf(err, "fabric: cannot resolve workspace path: %s\n", workspace_arg); return 1;
    }
    {
        const char *args[8] = {lock_arg, catalog_arg, composition_arg, profile_arg, legacy_arg,
                               "estate/operator-contracts.yaff", "estate/operator-contract-bindings.tsv", providers_arg};
        char *outs[8] = {lock, catalog, composition, profile, legacy, contracts, contract_bindings, providers};
        for (int q = 0; q < 8; q++) {
            if (resolve_from(directory, args[q], outs[q])) {
                fprintf(err, "fabric: cannot resolve declaration '%s' relative to %s\n", args[q], directory);
                return 1;
            }
        }
    }
    if (validate_lock(lock, source_hash, err)) return 1;
    paths[0]=workspace; paths[1]=lock; paths[2]=catalog; paths[3]=composition; paths[4]=profile; paths[5]=legacy; paths[6]=contracts; paths[7]=contract_bindings; paths[8]=providers;
    bf_sha256_init(&ctx);
    for(int q=0;q<9;q++){char hash[65];if(bf_sha256_file(paths[q],hash)){fprintf(err,"fabric: required declaration missing: %s\n",paths[q]);return 1;}bf_sha256_update(&ctx,(const uint8_t*)hash,strlen(hash));}
    bf_sha256_update(&ctx,(const uint8_t*)source_hash,strlen(source_hash)); bf_sha256_final(&ctx,digest); hex_digest(digest,generation);
    sqlite3_exec(db,"BEGIN IMMEDIATE;DELETE FROM catalog_bindings;DELETE FROM catalog WHERE id LIKE 'core.%' OR id LIKE 'command.%' OR id LIKE 'provider.%';",NULL,NULL,NULL);
    if(compile_workspace(db,workspace,generation,err)||record_decl(db,"lock",lock,generation,err)||compile_catalog(db,catalog,generation,err)||compile_legacy(db,legacy,generation,err)||compile_compositions(db,composition,generation,err)||compile_providers(db,providers,generation,err)||discover_repository_roots(db,directory,err)||record_decl(db,"profiles",profile,generation,err)||meta(db,"catalog_generation",generation,err)||meta(db,"workspace_generation",generation,err)){sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);return 1;}
    sqlite3_exec(db,"COMMIT",NULL,NULL,NULL);
    {
        sqlite3_stmt *count_statement = NULL;
        int sibling_repos = 0;
        if (sqlite3_prepare_v2(db, "SELECT count(*) FROM repository_roots", -1, &count_statement, NULL) == SQLITE_OK
            && sqlite3_step(count_statement) == SQLITE_ROW) {
            sibling_repos = sqlite3_column_int(count_statement, 0);
        }
        sqlite3_finalize(count_statement);
        fprintf(out,"workspace_generation=%s\ncatalog_generation=%s\nroots_compiled=1\noperators_compiled=96\ncompositions_compiled=1\nsibling_repositories_discovered=%d\n",generation,generation,sibling_repos);
    }
    return 0;
}

static int node_status(sqlite3*db,const char*mission,const char*node,char*out,size_t n){sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"SELECT status FROM workgraph_nodes WHERE mission_id=? AND node_id=?",-1,&s,NULL);sqlite3_bind_text(s,1,mission,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,node,-1,SQLITE_TRANSIENT);int ok=sqlite3_step(s)==SQLITE_ROW;if(ok)snprintf(out,n,"%s",sqlite3_column_text(s,0));sqlite3_finalize(s);return ok;}
static int ready(sqlite3 *db, const char *mission, const char *deps, int *bad) {
    if (!deps || !deps[0] || !strcmp(deps, "[]")) return 1;
    char work[512];
    snprintf(work, sizeof(work), "%s", deps);
    char *cursor = work;
    if (*cursor == '[') ++cursor;
    char *save = NULL;
    for (char *dependency = strtok_r(cursor, ",", &save); dependency;
         dependency = strtok_r(NULL, ",", &save)) {
        while (*dependency == ' ' || *dependency == '\t' || *dependency == '"') ++dependency;
        size_t length = strlen(dependency);
        while (length && (dependency[length - 1] == ' ' || dependency[length - 1] == '\t' ||
                          dependency[length - 1] == ']' || dependency[length - 1] == '"'))
            dependency[--length] = '\0';
        if (!length) continue;
        char status[32];
        if (!node_status(db, mission, dependency, status, sizeof(status)) ||
            !strcmp(status, "failed") || !strcmp(status, "blocked")) {
            *bad = 1;
            return 0;
        }
        if (strcmp(status, "complete")) return 0;
    }
    return 1;
}
static int emit_exec(sqlite3 *db, const char *mission, const char *node,
                     const char *op, const char *input, const char *output,
                     const BfProcessResult *process, const char *receipt_id,
                     FILE *err) {
    char event_id[64], timestamp[32];
    sqlite3_stmt *statement = NULL;
    sqlite3_int64 input_bytes = 0;
    id(event_id, sizeof(event_id), "evt", node);
    stamp(timestamp);
    sqlite3_prepare_v2(db, "SELECT bytes FROM artifacts WHERE uri=?", -1, &statement, NULL);
    sqlite3_bind_text(statement, 1, input, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) input_bytes = sqlite3_column_int64(statement, 0);
    sqlite3_finalize(statement);
    sqlite3_prepare_v2(db, "INSERT INTO events(id,mission_id,task_id,attempt,actor,operator_id,provider_id,model_id,start_at,end_at,duration_ms,input_uri,output_uri,effect_class,status,error_code,receipt_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", -1, &statement, NULL);
    sqlite3_bind_text(statement,1,event_id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,2,mission,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(statement,3,node,-1,SQLITE_TRANSIENT); sqlite3_bind_int(statement,4,1);
    sqlite3_bind_text(statement,5,"bonfyred",-1,SQLITE_STATIC); sqlite3_bind_text(statement,6,op,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(statement,7,"native",-1,SQLITE_STATIC); sqlite3_bind_null(statement,8);
    sqlite3_bind_text(statement,9,timestamp,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,10,timestamp,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement,11,process->duration_ms); sqlite3_bind_text(statement,12,input,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(statement,13,output,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,14,"pure-read",-1,SQLITE_STATIC);
    sqlite3_bind_text(statement,15,"complete",-1,SQLITE_STATIC); sqlite3_bind_null(statement,16);
    sqlite3_bind_text(statement,17,receipt_id,-1,SQLITE_TRANSIENT);
    if (step(db, statement, err)) return -1;
    sqlite3_prepare_v2(db, "INSERT INTO execution_metrics(event_id,catalog_generation,runtime_generation,bytes_in,bytes_out,cpu_ms,quality_result) VALUES(?,(SELECT value FROM fabric_meta WHERE key='catalog_generation'),(SELECT value FROM fabric_meta WHERE key='runtime_generation'),?,?,?,?)", -1, &statement, NULL);
    sqlite3_bind_text(statement,1,event_id,-1,SQLITE_TRANSIENT); sqlite3_bind_int64(statement,2,input_bytes);
    sqlite3_bind_int64(statement,3,(sqlite3_int64)(process->stdout_bytes + process->stderr_bytes));
    if (process->cpu_user_ms >= 0 && process->cpu_system_ms >= 0) sqlite3_bind_int64(statement,4,process->cpu_user_ms + process->cpu_system_ms); else sqlite3_bind_null(statement,4);
    sqlite3_bind_text(statement,5,process->quality_result[0] ? process->quality_result : "failed",-1,SQLITE_TRANSIENT);
    if (step(db, statement, err)) return -1;
    sqlite3_prepare_v2(db, "INSERT INTO usage_ledger(event_id,bytes_in,bytes_out,duration_ms,created_at) VALUES(?,?,?,?,?)", -1, &statement, NULL);
    sqlite3_bind_text(statement,1,event_id,-1,SQLITE_TRANSIENT); sqlite3_bind_int64(statement,2,input_bytes);
    sqlite3_bind_int64(statement,3,(sqlite3_int64)(process->stdout_bytes + process->stderr_bytes));
    sqlite3_bind_int64(statement,4,process->duration_ms); sqlite3_bind_text(statement,5,timestamp,-1,SQLITE_TRANSIENT);
    if (step(db, statement, err)) return -1;
    /* Native operators have no externally billed provider charge.  Persist
     * the measured zero instead of NULL so downstream accounting can
     * distinguish a factual local execution from missing cost evidence. */
    sqlite3_prepare_v2(db,
        "INSERT INTO economic_ledger(event_id,projected_cost,realized_cost,created_at) "
        "VALUES(?,0.0,0.0,?)", -1, &statement, NULL);
    sqlite3_bind_text(statement, 1, event_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, timestamp, -1, SQLITE_TRANSIENT);
    if (step(db, statement, err)) return -1;
    sqlite3_prepare_v2(db, "INSERT INTO value_ledger(event_id,accepted,created_at) VALUES(?,NULL,?)", -1, &statement, NULL);
    sqlite3_bind_text(statement,1,event_id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,2,timestamp,-1,SQLITE_TRANSIENT);
    return step(db, statement, err);
}

static int regular_nonempty_file(const char *path) {
    struct stat info;
    return path && stat(path, &info) == 0 && S_ISREG(info.st_mode) && info.st_size > 0;
}

static int structured_file(const char *path) {
    FILE *file = fopen(path, "rb");
    int character = EOF;
    if (!file) return 0;
    do { character = fgetc(file); } while (character != EOF && isspace((unsigned char)character));
    fclose(file);
    return character == '{' || character == '[';
}

static int valid_json_document(const char *path) {
    FILE *file=fopen(path,"rb"); int character, depth=0, quoted=0, escaped=0, seen=0, opener=0;
    if(!file) return 0;
    while((character=fgetc(file))!=EOF) {
        if(!seen && isspace((unsigned char)character)) continue;
        if(!seen) { opener=character; if(opener!='{'&&opener!='['){fclose(file);return 0;} seen=1; }
        if(quoted) { if(escaped) escaped=0; else if(character=='\\') escaped=1; else if(character=='"') quoted=0; continue; }
        if(character=='"') { quoted=1; continue; }
        if(character=='{'||character=='[') ++depth;
        else if(character=='}'||character==']') { if(--depth<0){fclose(file);return 0;} }
    }
    fclose(file); return seen && !quoted && depth==0;
}

static int valid_utf8_file(const char *path) {
    FILE *file = fopen(path, "rb");
    int byte;
    if (!file) return 0;
    while ((byte = fgetc(file)) != EOF) {
        unsigned char first = (unsigned char)byte;
        int extra = first < 0x80 ? 0 :
                    (first >= 0xc2 && first <= 0xdf) ? 1 :
                    (first >= 0xe0 && first <= 0xef) ? 2 :
                    (first >= 0xf0 && first <= 0xf4) ? 3 : -1;
        if (extra < 0) { fclose(file); return 0; }
        for (int index = 0; index < extra; ++index) {
            int continuation = fgetc(file);
            if (continuation == EOF || ((unsigned char)continuation & 0xc0) != 0x80) {
                fclose(file); return 0;
            }
        }
    }
    fclose(file);
    return 1;
}

static int file_contains_text(const char *path, const char *required) {
    char *content = read_text(path, NULL);
    int found = content != NULL && strstr(content, required) != NULL;

    free(content);
    return found;
}

/* Reads the next word of a text as lowercase letters and digits; returns its
 * length, or 0 at the end. */
static size_t next_plain_word(const char **cursor, char *word, size_t size) {
    const unsigned char *at = (const unsigned char *)*cursor;
    size_t length = 0;

    while (*at && !isalnum(*at)) at++;
    while (*at && (isalnum(*at) || *at == '\'')) {
        if (*at != '\'' && length + 1 < size) word[length++] = (char)tolower(*at);
        at++;
    }
    word[length] = '\0';
    *cursor = (const char *)at;
    return length;
}

/* True when every word of the shorter text appears, in the same order, in the
 * longer one.  The counts let a caller bound how much was removed or added. */
static int words_kept_in_order(const char *shorter_path, const char *longer_path,
                               size_t *shorter_words, size_t *longer_words) {
    char *shorter = read_text(shorter_path, NULL), *longer = read_text(longer_path, NULL);
    const char *short_at = shorter, *long_at = longer;
    char want[96], have[96];
    int kept = shorter != NULL && longer != NULL;

    *shorter_words = 0; *longer_words = 0;
    while (kept && next_plain_word(&short_at, want, sizeof(want))) {
        int found = 0;
        (*shorter_words)++;
        while (next_plain_word(&long_at, have, sizeof(have))) {
            (*longer_words)++;
            if (!strcmp(want, have)) { found = 1; break; }
        }
        kept = found;
    }
    while (kept && next_plain_word(&long_at, have, sizeof(have))) (*longer_words)++;
    free(shorter); free(longer);
    return kept;
}

static int file_has_prefix(const char *path, const unsigned char *prefix, size_t prefix_size) {
    unsigned char buffer[16];
    FILE *file;
    int matches;

    if (prefix_size > sizeof(buffer)) {
        return 0;
    }
    file = fopen(path, "rb");
    if (file == NULL) {
        return 0;
    }
    matches = fread(buffer, 1, prefix_size, file) == prefix_size &&
        memcmp(buffer, prefix, prefix_size) == 0;
    fclose(file);
    return matches;
}

static int qwen_digest_field(const char *json, const char *field) {
    char prefix[96];
    const char *value;
    snprintf(prefix, sizeof(prefix), "\"%s\": \"", field);
    value = strstr(json, prefix);
    if (!value) return 0;
    value += strlen(prefix);
    for (int index = 0; index < 64; ++index) {
        if (!isxdigit((unsigned char)value[index])) return 0;
    }
    return value[64] == '\"';
}

static int qwen_string_field(const char *json, const char *field, char *out, size_t cap) {
    char prefix[96];
    const char *value, *end;
    if (!json || !field || !out || cap == 0) return 0;
    snprintf(prefix, sizeof(prefix), "\"%s\": \"", field);
    value = strstr(json, prefix);
    if (!value) return 0;
    value += strlen(prefix);
    end = strchr(value, '"');
    if (!end || (size_t)(end - value) >= cap) return 0;
    memcpy(out, value, (size_t)(end - value));
    out[end - value] = '\0';
    return 1;
}

static int qwen_integer_field(const char *json, const char *field, int *out) {
    char prefix[96];
    const char *value;
    char *end = NULL;
    long parsed;
    if (!json || !field || !out) return 0;
    snprintf(prefix, sizeof(prefix), "\"%s\":", field);
    value = strstr(json, prefix);
    if (!value) return 0;
    parsed = strtol(value + strlen(prefix), &end, 10);
    if (end == value + strlen(prefix) || parsed < INT_MIN || parsed > INT_MAX) return 0;
    *out = (int)parsed;
    return 1;
}

static int qwen_parse_single_blender_statement(const char *path) {
    char buffer[1024];
    FILE *file;
    size_t len;
    char *start;
    char *end;
    if (!path) return 0;
    file = fopen(path, "rb");
    if (!file) return 0;
    len = fread(buffer, 1, sizeof(buffer) - 1, file);
    if (ferror(file) || fgetc(file) != EOF) {
        fclose(file);
        return 0;
    }
    fclose(file);
    buffer[len] = '\0';
    start = buffer;
    while (*start && isspace((unsigned char)*start)) start++;
    end = start + strlen(start);
    while (end > start && isspace((unsigned char)end[-1])) *--end = '\0';
    return strncmp(start, "bpy.ops.mesh.primitive_cube_add(", 32) == 0 &&
           end > start + 32 && end[-1] == ')' &&
           !strchr(start, ';') && !strchr(start, '#');
}

static int qwen_blender_line_output(const char *path) {
    char directory[PATH_MAX], manifest_path[PATH_MAX], trace_path[PATH_MAX], raw_path[PATH_MAX];
    char raw_digest[65] = {0};
    char *output = NULL, *manifest = NULL, *trace = NULL, *prompt = NULL, *raw = NULL;
    size_t output_len = 0, prompt_len = 0;
    const char *slash, *tokens, *generated, *prompt_key, *prompt_value;
    int token_lines = 0, span_start = -1, span_end = -1, result = 0;

    if (!regular_nonempty_file(path) || !valid_utf8_file(path) ||
        !qwen_parse_single_blender_statement(path)) return 0;
    output = read_text(path, &output_len);
    if (!output || !strstr(output, "bpy.ops.mesh.primitive_cube_add")) goto done;
    /* A generated artifact must be one operation, not a prompt echo. */
    for (size_t index = 0; index < output_len; ++index) {
        if (output[index] == '\n' && index + 1 < output_len && output[index + 1] != '\0') goto done;
    }
    slash = strrchr(path, '/');
    if (!slash || (size_t)(slash - path) >= sizeof(directory)) goto done;
    memcpy(directory, path, (size_t)(slash - path));
    directory[slash - path] = '\0';
    if (snprintf(manifest_path, sizeof(manifest_path), "%s/model-inference.json", directory) >= (int)sizeof(manifest_path) ||
        snprintf(trace_path, sizeof(trace_path), "%s/model-token-trace.jsonl", directory) >= (int)sizeof(trace_path)) goto done;
    manifest = read_text(manifest_path, NULL);
    trace = read_text(trace_path, NULL);
    if (!manifest || !trace || !valid_json_document(manifest_path) ||
        !strstr(manifest, "\"status\": \"ok\"") ||
        !strstr(manifest, "\"finite_model_state\": true") ||
        !qwen_digest_field(manifest, "model_hash") || !qwen_digest_field(manifest, "tokenizer_hash") ||
        !qwen_digest_field(manifest, "raw_output_digest") ||
        !strstr(trace, "bpy.ops.mesh.primitive_cube_add")) goto done;
    if (!qwen_string_field(manifest, "raw_output_path", raw_path, sizeof(raw_path)) ||
        !regular_nonempty_file(raw_path) || !valid_utf8_file(raw_path) ||
        bf_sha256_file(raw_path, raw_digest) != 0 || !strstr(manifest, raw_digest)) goto done;
    raw = read_text(raw_path, NULL);
    if (!raw || !strstr(raw, output)) goto done;
    for (tokens = trace; (tokens = strstr(tokens, "\"type\":\"token\"")) != NULL; ++tokens) ++token_lines;
    generated = strstr(manifest, "\"generated_tokens\":");
    if (!generated || strtol(generated + strlen("\"generated_tokens\":"), NULL, 10) <= 1 || token_lines <= 1) goto done;
    if (!qwen_integer_field(manifest, "normalized_from_token_start", &span_start) ||
        !qwen_integer_field(manifest, "normalized_from_token_end", &span_end) ||
        span_start < 0 || span_end <= span_start || span_end > token_lines) goto done;
    prompt_key = strstr(manifest, "\"prompt_path\": \"");
    if (!prompt_key) goto done;
    prompt_value = prompt_key + strlen("\"prompt_path\": \"");
    {
        const char *end = strchr(prompt_value, '\"');
        char prompt_path[PATH_MAX];
        size_t length = end ? (size_t)(end - prompt_value) : 0;
        if (!length || length >= sizeof(prompt_path)) goto done;
        memcpy(prompt_path, prompt_value, length); prompt_path[length] = '\0';
        prompt = read_text(prompt_path, &prompt_len);
        if (!prompt || (prompt_len == output_len && !memcmp(prompt, output, output_len))) goto done;
    }
    result = 1;
done:
    free(output); free(manifest); free(trace); free(prompt); free(raw);
    return result;
}

static int model_embedding_output(const char *path) {
    char *json = read_text(path, NULL);
    char *cursor;
    int dimensions = 0, values = 0;
    double norm = 0.0;
    int result = 0;
    if (!json || !strstr(json, "\"backend\": \"native-moe-token-embedding\"") ||
        !strstr(json, "\"normalized\": true") ||
        sscanf(strstr(json, "\"dimensions\":") ? strstr(json, "\"dimensions\":") : "", "\"dimensions\": %d", &dimensions) != 1 ||
        dimensions != 2048 || !strstr(json, "\"token_count\":") ||
        !strstr(json, "\"vector\": [")) goto done;
    cursor = strstr(json, "\"vector\": [") + strlen("\"vector\": [");
    while (*cursor && *cursor != ']') {
        char *end = NULL;
        float value;
        while (isspace((unsigned char)*cursor) || *cursor == ',') ++cursor;
        value = strtof(cursor, &end);
        if (end == cursor || !isfinite(value)) goto done;
        norm += (double)value * value;
        ++values;
        cursor = end;
    }
    result = values == dimensions && fabs(sqrt(norm) - 1.0) < 1e-3;
done:
    free(json);
    return result;
}

static int vector_store_output(const char *path) {
    char *text = read_text(path, NULL);
    static const char *required[] = {
        "\"backend\":\"native-moe-token-embedding\"",
        "\"dimensions\":2048", "\"distance_metric\":\"cosine\"",
        "\"inserted\":true", "\"updated\":true",
        "\"nearest_neighbor\":\"native-reference\"", "\"metadata_filtered\":true",
        "\"deleted\":true", "\"restart_persisted\":true",
        "\"reference_neighbor\":true", NULL
    };
    int result = text != NULL;
    for (int index = 0; result && required[index]; ++index) result = strstr(text, required[index]) != NULL;
    free(text);
    return result;
}

static int queue_workflow_output(const char *path) {
    char *text = read_text(path, NULL);
    static const char *required[] = {
        "\"enqueue\":true", "\"claim\":true", "\"lease\":true",
        "\"lease_renewal\":true", "\"ack\":true", "\"lease_expiry\":true",
        "\"retry\":true", "\"backoff\":true", "\"dead_letter\":true",
        "\"cancellation\":true", "\"fan_out\":true", "\"fan_in\":true",
        "\"pipeline_continuation\":true", "\"restart_resume\":true", NULL
    };
    int result = text != NULL;
    for (int index = 0; result && required[index]; ++index) result = strstr(text, required[index]) != NULL;
    free(text);
    return result;
}

static int auth_authority_output(const char *path) {
    char *text = read_text(path, NULL);
    static const char *required[] = {
        "\"user_created\":true", "\"credential_verified\":true", "\"authorized_operation\":true",
        "\"wrong_credential_rejected\":true", "\"missing_authority_rejected\":true",
        "\"expired_authority_rejected\":true", "\"scope_violation_rejected\":true",
        "\"effect_gate_approved\":true", "\"effect_gate_denied\":true",
        "\"secret_redacted\":true", "\"restart_persisted\":true", NULL
    };
    int result = text != NULL;
    for (int index = 0; result && required[index]; ++index) result = strstr(text, required[index]) != NULL;
    free(text);
    return result;
}

static int graph_lineage_output(const char *path) {
    char *text = read_text(path, NULL);
    int result = text &&
        strstr(text, "\"atom_exists\":true") &&
        strstr(text, "\"operation_exists\":true") &&
        strstr(text, "\"lineage_correct\":true") &&
        strstr(text, "\"no_dangling_relation\":true") &&
        strstr(text, "\"query_identity\":true") &&
        strstr(text, "\"restart_persisted\":true");
    free(text);
    return result;
}

static int project_space_output(const char *path) {
    char *text = read_text(path, NULL);
    int result = text && strstr(text, "\"put\":true") && strstr(text, "\"get\":true") &&
        strstr(text, "\"isolation\":true") && strstr(text, "\"handoff\":true") &&
        strstr(text, "\"restart_persisted\":true");
    free(text);
    return result;
}

static int cms_crud_output(const char *path) {
    char *text = read_text(path, NULL);
    static const char *required[] = {
        "\"schema_created\":true", "\"record_created\":true", "\"read\":true",
        "\"updated\":true", "\"typed_relation\":true", "\"pagination\":true",
        "\"authorized\":true", "\"deleted\":true", NULL
    };
    int result = text != NULL;
    for (int index = 0; result && required[index]; ++index) result = strstr(text, required[index]) != NULL;
    free(text);
    return result;
}

static int parse_labeled_double(const char *text, const char *label, double *out) {
    const char *found = strstr(text, label);
    char *end = NULL;
    double value;

    if (!found) return 0;
    value = strtod(found + strlen(label), &end);
    if (end == found + strlen(label)) return 0;
    *out = value;
    return 1;
}

/* One number read from a store the node was handed, opened read-only. The
 * probes below use it to check an organ's report against the records the
 * report was made from. */
static int store_number(const char *path, const char *sql, double *value) {
    sqlite3 *store = NULL;
    sqlite3_stmt *statement = NULL;
    int ok = 0;
    if (sqlite3_open_v2(path, &store, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
        sqlite3_prepare_v2(store, sql, -1, &statement, NULL) == SQLITE_OK &&
        sqlite3_step(statement) == SQLITE_ROW) {
        *value = sqlite3_column_double(statement, 0);
        ok = 1;
    }
    if (statement) sqlite3_finalize(statement);
    if (store) sqlite3_close(store);
    return ok;
}

/* Lines of a report that contain needle, or every non-empty line when
 * needle is NULL. */
static long report_lines(const char *path, const char *needle) {
    char *text = read_text(path, NULL);
    long count = 0;
    if (!text) return -1;
    for (char *line = text; line && *line; ) {
        char *end = strchr(line, '\n');
        if (end) *end = '\0';
        if (*line && (!needle || strstr(line, needle))) count++;
        line = end ? end + 1 : NULL;
    }
    free(text);
    return count;
}

static unsigned int pcap_u32(const unsigned char *bytes, int swapped) {
    if (swapped)
        return ((unsigned int)bytes[0] << 24) | ((unsigned int)bytes[1] << 16) |
            ((unsigned int)bytes[2] << 8) | (unsigned int)bytes[3];
    return ((unsigned int)bytes[3] << 24) | ((unsigned int)bytes[2] << 16) |
        ((unsigned int)bytes[1] << 8) | (unsigned int)bytes[0];
}

/* Walks a classic pcap file and totals its records and captured bytes. */
static int pcap_record_totals(const char *path, unsigned long long *packets,
                              unsigned long long *bytes) {
    unsigned char header[24];
    unsigned char record[16];
    unsigned int magic;
    int swapped;
    FILE *file = fopen(path, "rb");

    if (!file) return -1;
    if (fread(header, 1, sizeof(header), file) != sizeof(header)) { fclose(file); return -1; }
    magic = pcap_u32(header, 0);
    if (magic == 0xa1b2c3d4u || magic == 0xa1b23c4du) swapped = 0;
    else if (magic == 0xd4c3b2a1u || magic == 0x4d3cb2a1u) swapped = 1;
    else { fclose(file); return -1; }
    *packets = 0;
    *bytes = 0;
    while (fread(record, 1, sizeof(record), file) == sizeof(record)) {
        unsigned int captured = pcap_u32(record + 8, swapped);
        if (fseek(file, (long)captured, SEEK_CUR) != 0) { fclose(file); return -1; }
        *packets += 1;
        *bytes += captured;
    }
    fclose(file);
    return 0;
}

static int probe_contract_output(const char *probe, const char *path, int semantic_stream,
                                 const char *input_path) {
    static const unsigned char riff_prefix[] = {'R', 'I', 'F', 'F'};
    static const unsigned char jpeg_prefix[] = {0xff, 0xd8, 0xff};

    if (!probe || !*probe) return 0;
    if (!strcmp(probe,"output-artifact") || !strcmp(probe,"output-exists") ||
        !strcmp(probe,"exit-plus-artifact") || !strcmp(probe,"manifest-complete") ||
        !strcmp(probe,"relation-created") || !strcmp(probe,"state-transition")) return regular_nonempty_file(path);
    if (!strcmp(probe,"structured-output")) return structured_file(path);
    if (!strcmp(probe,"quant-roundtrip")) {
        /* The report must name the model file this node was handed, carry a
         * per-tensor measurement, and its lowest cosine must be one a
         * roundtrip can be said to have survived. */
        char *text;
        char model_line[PATH_MAX + 16];
        double measured = 0.0, lowest = 0.0;
        int ok;

        if (!input_path || !input_path[0]) return 0;
        text = read_text(path, NULL);
        if (!text) return 0;
        snprintf(model_line, sizeof(model_line), "Model: %s\n", input_path);
        ok = strstr(text, model_line) != NULL &&
            strstr(text, "cos=") && strstr(text, "rmse=") && strstr(text, "bpw=") &&
            parse_labeled_double(text, "tensors measured:", &measured) && measured >= 1.0 &&
            parse_labeled_double(text, "lowest ", &lowest) && lowest >= 0.99 && lowest <= 1.0;
        free(text);
        return ok;
    }
    if (!strcmp(probe,"qwen-blender-line")) return qwen_blender_line_output(path);
    if (!strcmp(probe,"model-embedding")) return model_embedding_output(path);
    if (!strcmp(probe,"vector-store-state")) return vector_store_output(path);
    if (!strcmp(probe,"queue-workflow-state")) return queue_workflow_output(path);
    if (!strcmp(probe,"auth-authority-state")) return auth_authority_output(path);
    if (!strcmp(probe,"graph-lineage")) return graph_lineage_output(path);
    if (!strcmp(probe,"project-space-state")) return project_space_output(path);
    if (!strcmp(probe,"cms-crud-state")) return cms_crud_output(path);
    if (!strcmp(probe,"nonempty-stream")) return semantic_stream && regular_nonempty_file(path);
    if (!strcmp(probe,"clean-transcript")) {
        /* Cleaning only removes: what is left must be the transcript's own
         * words in their own order, and most of them. */
        size_t kept = 0, heard = 0;
        return input_path && input_path[0] && valid_utf8_file(path) &&
            words_kept_in_order(path, input_path, &kept, &heard) &&
            kept >= 1 && kept * 2 >= heard;
    }
    if (!strcmp(probe,"paragraph-structure")) {
        /* Paragraphing only rebreaks: every word of the source is still
         * there in order, and nothing was written in beyond markers. */
        size_t source = 0, written = 0;
        return input_path && input_path[0] && valid_utf8_file(path) &&
            words_kept_in_order(input_path, path, &source, &written) &&
            source >= 1 && written <= source * 2;
    }
    if (!strcmp(probe,"brief-semantics"))
        return file_contains_text(path, "## Summary") && file_contains_text(path, "## Action Items");
    if (!strcmp(probe,"proof-semantics"))
        return valid_json_document(path) && file_contains_text(path, "\"qualityScore\"");
    if (!strcmp(probe,"offer-semantics"))
        return valid_json_document(path) && file_contains_text(path, "\"offer");
    if (!strcmp(probe,"package-manifest"))
        return file_contains_text(path, "proof-bundle.json") && file_contains_text(path, "offer.json");
    if (!strcmp(probe,"repurpose-manifest"))
        return valid_json_document(path) && file_contains_text(path, "\"formats\"");
    if (!strcmp(probe,"scene-boundaries"))
        return valid_json_document(path) && file_contains_text(path, "\"boundaries\"");
    if (!strcmp(probe,"object-detections"))
        return valid_json_document(path) && file_contains_text(path, "\"objects\"");
    if (!strcmp(probe,"fragment-semantic"))
        return file_contains_text(path, "sha256:");
    if (!strcmp(probe,"workflow-semantic"))
        return file_contains_text(path, "A3") && file_contains_text(path, "steps");
    if (!strcmp(probe,"family-semantic"))
        return file_contains_text(path, "T_SHARED_QK") && file_contains_text(path, "category");
    if (!strcmp(probe,"segment-graph"))
        return valid_json_document(path) && file_contains_text(path, "\"nodes\"") &&
            file_contains_text(path, "\"edges\"");
    if (!strcmp(probe,"recipe-semantic"))
        return file_contains_text(path, "recipe:") && file_contains_text(path, "bonfyre-transcribe");
    if (!strcmp(probe,"fpq-semantic"))
        return file_contains_text(path, "Best cosine:") && file_contains_text(path, "HIGH FIDELITY");
    if (!strcmp(probe,"flashqla-semantic"))
        return file_has_prefix(path, (const unsigned char *)"NDGO", 4);
    if (!strcmp(probe,"kvcache-semantic")) {
        /* The report must cover every float in the cache it was handed and
         * the reconstruction must be one a model could still attend over. */
        char *text;
        struct stat input_info;
        double floats = 0.0, cosine = 0.0;
        int ok;
        if (!input_path || !input_path[0] || stat(input_path, &input_info) != 0 ||
            input_info.st_size < 4) return 0;
        text = read_text(path, NULL);
        if (!text) return 0;
        ok = strstr(text, "nan") == NULL &&
            parse_labeled_double(text, "KV roundtrip:", &floats) &&
            floats == (double)(input_info.st_size / 4) &&
            parse_labeled_double(text, "cos=", &cosine) && cosine >= 0.99 && cosine <= 1.0;
        free(text);
        return ok;
    }
    if (!strcmp(probe,"entity-semantic"))
        return file_contains_text(path, "alice@example.com") && file_contains_text(path, "entity");
    if (!strcmp(probe,"flow-semantic"))
        return file_contains_text(path, "completion-flow") && file_contains_text(path, "defined");
    if (!strcmp(probe,"gate-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"tier\": \"pro\"") &&
            file_contains_text(path, "\"status\": \"active\"");
    /* The next five probes are handed a snapshot of one of the estate's own
     * stores. Each counts the records in that snapshot itself and requires
     * the organ's report to account for exactly that many, so the check
     * follows whatever the estate holds on the day it is run. A store with
     * nothing in it certifies nothing. */
    if (!strcmp(probe,"economy-semantic")) {
        double costs = 0.0;
        if (!input_path || !store_number(input_path, "SELECT COUNT(*) FROM costs", &costs) || costs < 1.0) return 0;
        return report_lines(path, NULL) == (long)costs + 1;   /* one heading line */
    }
    if (!strcmp(probe,"finance-semantic")) {
        /* One ranked line per opportunity in the store, and an opportunity
         * whose compensation nobody has stated must show a dash, not a zero. */
        double opportunities = 0.0, unstated = 0.0;
        if (!input_path || !store_number(input_path, "SELECT COUNT(*) FROM opportunities", &opportunities) ||
            opportunities < 1.0 ||
            !store_number(input_path, "SELECT COUNT(*) FROM opportunities WHERE compensation_cents IS NULL", &unstated))
            return 0;
        return report_lines(path, NULL) == (long)opportunities + 1 &&     /* one heading line */
            report_lines(path, "\t-\t") >= (long)unstated;
    }
    if (!strcmp(probe,"control-semantic"))
        return file_contains_text(path, "entropy") && file_contains_text(path, "PASS");
    if (!strcmp(probe,"meter-record")) {
        /* The event must be for an operator execution and its byte count
         * must be the size of the artifact that execution produced, which
         * is the input this node was handed. */
        struct stat input_info;
        char marker[64];
        if (!input_path || !input_path[0] || stat(input_path, &input_info) != 0) return 0;
        snprintf(marker, sizeof(marker), "bytes=%lld ", (long long)input_info.st_size);
        return file_contains_text(path, "[meter] recorded key=") &&
            file_contains_text(path, "op=operator-execution") &&
            file_contains_text(path, marker);
    }
    if (!strcmp(probe,"tone-semantic")) {
        /* The report must name its engine and its feature set and agree
         * with itself: eGeMAPS only from OpenSMILE and then all 88 features,
         * the estate's own set only from the native engine. Either way the
         * recording is speech, so a pitch must have been found. */
        char *text;
        double count = 0.0, pitch = 0.0;
        int egemaps, ok;
        if (!valid_json_document(path)) return 0;
        text = read_text(path, NULL);
        if (!text) return 0;
        egemaps = strstr(text, "\"feature_set\": \"eGeMAPSv02\"") != NULL;
        ok = parse_labeled_double(text, "\"feature_count\":", &count) &&
            (egemaps ? (count == 88.0 && strstr(text, "\"engine\": \"opensmile\"") != NULL)
                     : (count >= 20.0 && strstr(text, "\"engine\": \"native\"") != NULL &&
                        strstr(text, "\"feature_set\": \"bonfyre-voice-fingerprint\"") != NULL)) &&
            parse_labeled_double(text, "\"F0semitoneFrom27.5Hz_sma3nz_amean\":", &pitch) && pitch > 0.0;
        free(text);
        return ok;
    }
    if (!strcmp(probe,"tag-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"language\": \"en\"") &&
            file_contains_text(path, "\"confidence\"");
    if (!strcmp(probe,"sync-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"valid\": true") &&
            file_contains_text(path, "\"format\": \"bonfyre-intake-v1\"");
    if (!strcmp(probe,"time-semantic"))
        return file_contains_text(path, "scheduled:") && file_contains_text(path, "completion-recipe");
    if (!strcmp(probe,"learn-semantic")) {
        /* history shows the thirty newest records. */
        double feedback = 0.0;
        if (!input_path || !store_number(input_path, "SELECT COUNT(*) FROM feedback", &feedback) || feedback < 1.0) return 0;
        return report_lines(path, NULL) == (long)(feedback < 30.0 ? feedback : 30.0) + 1;
    }
    if (!strcmp(probe,"compete-semantic"))
        return file_contains_text(path, "competition created: comp-completion-recipe-proof-");
    if (!strcmp(probe,"pay-semantic")) {
        /* The invoice must count every event in the meter store it was
         * handed and bill the sum of the costs the meter recorded for them. */
        double events = 0.0, cost = 0.0;
        char events_marker[64], cents_marker[64];
        if (!input_path || !store_number(input_path, "SELECT COUNT(*) FROM events", &events) || events < 1.0 ||
            !store_number(input_path, "SELECT COALESCE(SUM(cost),0.0) FROM events", &cost)) return 0;
        snprintf(events_marker, sizeof(events_marker), "Metered events: %ld ", (long)events);
        snprintf(cents_marker, sizeof(cents_marker), "(%ld cents)", (long)(cost * 100.0 + 0.5));
        return file_contains_text(path, events_marker) && file_contains_text(path, cents_marker);
    }
    if (!strcmp(probe,"tier-semantic"))
        return file_contains_text(path, "completion-recipe/proof") && file_contains_text(path, "tier=fast");
    if (!strcmp(probe,"outreach-semantic")) {
        /* report lists the ten newest outbound entries, and must say that
         * the store does not verify delivery. */
        double sends = 0.0;
        if (!input_path || !store_number(input_path, "SELECT COUNT(*) FROM sends", &sends) || sends < 1.0) return 0;
        return report_lines(path, " -> ") == (long)(sends < 10.0 ? sends : 10.0) &&
            file_contains_text(path, "does not verify external delivery");
    }
    if (!strcmp(probe,"surface-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"valid\":true");
    if (!strcmp(probe,"stitch-semantic"))
        return file_contains_text(path, "MATERIALIZATION PLAN for 'normalized'") &&
            file_contains_text(path, "TOTAL STEPS:");
    if (!strcmp(probe,"distribute-semantic")) {
        /* The count of composed messages must be the count in the store the
         * node was handed, and the organ must have dispatched none. */
        double messages = 0.0;
        char total_marker[48];
        if (!input_path || !store_number(input_path, "SELECT COUNT(*) FROM outbound_message", &messages) ||
            messages < 1.0) return 0;
        snprintf(total_marker, sizeof(total_marker), "\"total\":%ld,", (long)messages);
        return valid_json_document(path) && file_contains_text(path, "\"kind\":\"outbound-messages\"") &&
            file_contains_text(path, total_marker) &&
            file_contains_text(path, "\"dispatched_by_this_command\":0");
    }
    if (!strcmp(probe,"recipe-validation"))
        return file_contains_text(path, "Recipe is valid") && file_contains_text(path, "SHA-256:");
    if (!strcmp(probe,"wire-ingest-semantic")) {
        /* The counts are recomputed here from the capture the node was
         * handed, so the organ's report is checked against the packets and
         * not against a figure written into this file. */
        unsigned long long packets = 0, bytes = 0;
        char packet_marker[64];
        char byte_marker[64];

        if (!input_path || !input_path[0] || pcap_record_totals(input_path, &packets, &bytes) != 0 ||
            packets == 0) return 0;
        snprintf(packet_marker, sizeof(packet_marker), "\"packet_count\":%llu,", packets);
        snprintf(byte_marker, sizeof(byte_marker), "\"byte_count\":%llu,", bytes);
        return valid_json_document(path) && file_contains_text(path, packet_marker) &&
            file_contains_text(path, byte_marker) &&
            file_contains_text(path, "\"mode\":\"metadata-only\"");
    }
    if (!strcmp(probe,"whisper-semantic")) {
        /* The transcript is checked against its own run: the text file and
         * the segments say the same words, and no segment ends after the
         * audio that was heard does. */
        char directory[PATH_MAX], spoken[PATH_MAX], heard[PATH_MAX];
        struct stat audio;
        size_t said = 0, carried = 0;
        char *text;
        const char *at, *last = NULL;
        double end_ms = 0.0;

        if (!valid_json_document(path) || !file_contains_text(path, "\"sourceSystem\": \"BonfyreTranscribe\"") ||
            !file_contains_text(path, "\"confidence\":") || parent_directory(path, directory) != 0) return 0;
        snprintf(spoken, sizeof(spoken), "%s/transcript.txt", directory);
        snprintf(heard, sizeof(heard), "%s/normalized.wav", directory);
        if (!words_kept_in_order(spoken, path, &said, &carried) || said < 1) return 0;
        if (stat(heard, &audio) != 0 || audio.st_size <= 44) return 0;
        text = read_text(path, NULL);
        if (!text) return 0;
        for (at = text; (at = strstr(at, "\"t1\":")) != NULL; at += 5) last = at;
        if (last) end_ms = strtod(last + 5, NULL);
        free(text);
        /* normalized.wav is 16 kHz mono 16-bit: 32 bytes to the millisecond. */
        return end_ms > 0.0 && end_ms <= (double)(audio.st_size - 44) / 32.0 + 1000.0;
    }
    if (!strcmp(probe,"transcript-family-semantic")) {
        char directory[PATH_MAX];
        char cleaned[PATH_MAX];
        char paragraphed[PATH_MAX];
        char transcript[PATH_MAX];

        if (!valid_json_document(path) || !file_contains_text(path, "\"status\": \"ok\"") ||
            parent_directory(path, directory) != 0) {
            return 0;
        }
        snprintf(cleaned, sizeof(cleaned), "%s/cleaned.txt", directory);
        snprintf(paragraphed, sizeof(paragraphed), "%s/paragraphed.md", directory);
        snprintf(transcript, sizeof(transcript), "%s/transcribe/transcript.json", directory);
        return regular_nonempty_file(cleaned) && regular_nonempty_file(paragraphed) &&
            valid_json_document(transcript) &&
            words_kept_in_order(cleaned, transcript, &(size_t){0}, &(size_t){0});
    }
    if (!strcmp(probe,"narrate-semantic")) {
        char directory[PATH_MAX];
        char audio_path[PATH_MAX];

        struct stat audio_info;

        /* Whichever voice spoke, the manifest must say a render completed
         * and the audio beside it must be a WAV holding more than a header. */
        if (!valid_json_document(path) ||
            !file_contains_text(path, "\"renderStatus\": \"completed\"") ||
            !file_contains_text(path, "\"voiceModel\": \"") ||
            file_contains_text(path, "\"voiceModel\": \"\"") ||
            parent_directory(path, directory) != 0) {
            return 0;
        }
        snprintf(audio_path, sizeof(audio_path), "%s/artifact.wav", directory);
        return file_has_prefix(audio_path, riff_prefix, sizeof(riff_prefix)) &&
            stat(audio_path, &audio_info) == 0 && audio_info.st_size > 4096;
    }
    if (!strcmp(probe,"speechloop-semantic")) {
        char directory[PATH_MAX];
        char transcript[PATH_MAX];
        char narration_manifest[PATH_MAX];
        char narration_audio[PATH_MAX];

        if (!valid_json_document(path) || !file_contains_text(path, "\"status\": \"complete\"") ||
            !file_contains_text(path, "\"transcribe\"") ||
            !file_contains_text(path, "\"narrate\"") ||
            parent_directory(path, directory) != 0) {
            return 0;
        }
        snprintf(transcript, sizeof(transcript), "%s/transcribe/transcript.json", directory);
        snprintf(narration_manifest, sizeof(narration_manifest), "%s/narrate/artifact.manifest.json", directory);
        snprintf(narration_audio, sizeof(narration_audio), "%s/narrate/artifact.wav", directory);
        return valid_json_document(transcript) && valid_json_document(narration_manifest) &&
            file_contains_text(narration_manifest, "\"renderStatus\": \"completed\"") &&
            file_has_prefix(narration_audio, riff_prefix, sizeof(riff_prefix));
    }
    if (!strcmp(probe,"layer-extract-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"type\": \"layer_fragment\"") &&
            file_contains_text(path, "\"node_range\": [0, 10]") &&
            file_contains_text(path, "en_US-lessac-medium.onnx");
    if (!strcmp(probe,"layer-inspect-semantic"))
        return file_contains_text(path, "IR ver:   8") && file_contains_text(path, "Nodes:    2755") &&
            file_contains_text(path, "Params:   15650534");
    if (!strcmp(probe,"capability-semantic"))
        return file_contains_text(path, "transcribe") && file_contains_text(path, "bonfyre-transcribe") &&
            file_contains_text(path, "[metric: ") && !file_contains_text(path, "$0.");
    if (!strcmp(probe,"index-semantic"))
        return file_contains_text(path, "Indexed") && file_contains_text(path, "artifact families") &&
            file_contains_text(path, "index.sqlite");
    if (!strcmp(probe,"query-semantic"))
        return file_contains_text(path, "Found") && file_contains_text(path, "JSON files") &&
            file_contains_text(path, "artifacts.duckdb");
    if (!strcmp(probe,"emit-semantic"))
        return file_contains_text(path, "<!DOCTYPE html>") && file_contains_text(path, "proof") &&
            file_contains_text(path, "quality");
    if (!strcmp(probe,"ledger-semantic")) {
        /* "Raw bytes" is the total size of the artifact's containing
         * directory (bonfyre-ledger assess sums dir_size(dirname(input))),
         * so it must be at least the real input artifact's own size --
         * the directory necessarily contains that file. This rejects a
         * hardcoded or disconnected figure while tolerating the shared
         * artifact directory's other contents. */
        char *text;
        double reported_bytes;
        struct stat input_info;
        int ok;

        if (!file_contains_text(path, "Replacement cost:") || !file_contains_text(path, "Portfolio value:") ||
            !file_contains_text(path, "Raw bytes:")) return 0;
        if (!input_path || !input_path[0] || stat(input_path, &input_info) != 0) return 0;
        text = read_text(path, NULL);
        if (!text) return 0;
        ok = parse_labeled_double(text, "Raw bytes:", &reported_bytes) &&
            reported_bytes >= (double)input_info.st_size;
        free(text);
        return ok;
    }
    if (!strcmp(probe,"swarm-selftest-semantic"))
        /* Two independent real BitTorrent-wire-protocol workers each
         * received byte-identical piece data from the running coordinator,
         * and a separate `status` process invocation against the same
         * --index db durably shows both peers and the real bytes moved --
         * not just in-memory bookkeeping. */
        return file_contains_text(path, "\"worker1_piece_matched\":true") &&
            file_contains_text(path, "\"worker2_piece_matched\":true") &&
            file_contains_text(path, "\"kind\":\"swarm-status\"") &&
            file_contains_text(path, "\"peers\":2") &&
            !file_contains_text(path, "\"total_uploaded\":0");
    if (!strcmp(probe,"moq-selftest-semantic"))
        /* The native listener and the native client completed a QUIC
         * handshake and the paired SETUP exchange, a SUBSCRIBE was submitted,
         * and the server refused it for want of a transport lease. This is a
         * session and a refusal; it does not show media relayed. */
        return valid_json_document(path) && file_contains_text(path, "\"ready\":true") &&
            file_contains_text(path, "\"transport\":\"native_quic\"") &&
            file_contains_text(path, "\"control_stream\":\"paired_setup_completed\"") &&
            file_contains_text(path, "\"request_stream\":\"subscribe_submitted\"") &&
            file_contains_text(path, "\"unauthorized_subscribe_refused\":true");
    if (!strcmp(probe,"tel-selftest-semantic"))
        /* A call placed through the organ must exist in the switch when the
         * switch is asked, be recorded by the organ under the switch's own
         * identifier, be gone after the organ hangs it up, and the report
         * must say that none of this reached the telephone network. */
        return valid_json_document(path) && file_contains_text(path, "\"ready\":true") &&
            file_contains_text(path, "\"switch\":\"FreeSWITCH") &&
            file_contains_text(path, "\"switch_has_call_after_call\":true") &&
            file_contains_text(path, "\"call_recorded_by_organ\":1") &&
            file_contains_text(path, "\"switch_has_call_after_hangup\":false") &&
            file_contains_text(path, "\"hangup_of_unknown_call_refused\":true") &&
            file_contains_text(path, "\"reaches_telephone_network\":false");
    if (!strcmp(probe,"api-selftest-semantic"))
        /* Proves a real ephemeral HTTP listener: a request bearing a key
         * actually inserted into the server's own database is accepted
         * (200) and logged, while a nonexistent key is genuinely rejected
         * (401) by the same running server. */
        return valid_json_document(path) && file_contains_text(path, "\"ready\":true") &&
            file_contains_text(path, "\"authenticated_status\":200") &&
            file_contains_text(path, "\"rejected_status\":401") &&
            !file_contains_text(path, "\"authenticated_requests_logged\":0");
    if (!strcmp(probe,"proxy-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"status\":\"available\"") &&
            file_contains_text(path, "\"binary\":\"bonfyre-proxy\"");
    if (!strcmp(probe,"detect-objects-semantic"))
        /* The workload hands this node a photograph with a person in it, so
         * a detector that ran must have read the frame and boxed something.
         * An empty list here means no detection happened that can be told
         * apart from none being attempted. */
        return valid_json_document(path) && file_contains_text(path, "\"frame_count\": 1") &&
            file_contains_text(path, "\"frames_not_detected\": 0") &&
            file_contains_text(path, "\"model\": \"") &&
            !file_contains_text(path, "\"model\": \"\"") &&
            file_contains_text(path, "\"detections\": [{\"label\"");
    if (!strcmp(probe,"weaviate-semantic"))
        /* Real (non-dry-run) execution: documentId is genuinely parsed from
         * the real metadata JSON's "id" field, not a canned placeholder. */
        return valid_json_document(path) && file_contains_text(path, "\"status\": \"completed\"") &&
            file_contains_text(path, "\"documentId\": \"weaviate-selftest-doc\"");
    if (!strcmp(probe,"watch-semantic"))
        return file_contains_text(path, "\"event\":\"triggered\"") && file_contains_text(path, "\"exit_code\":0") &&
            !file_contains_text(path, "\"event\":\"failed\"");
    if (!strcmp(probe,"pipeline-run-semantic"))
        /* Every real stage in the single-process pipeline must actually
         * have run, in order, ending in a real completion marker. Compress
         * is best-effort (async zstd, absent when zstd isn't on the
         * governed PATH) so it is not required here. */
        return file_contains_text(path, "[pipeline:gate] PASS") &&
            file_contains_text(path, "[pipeline:ingest]") &&
            file_contains_text(path, "[pipeline:meter]") &&
            file_contains_text(path, "[pipeline:stitch] manifest written") &&
            file_contains_text(path, "[pipeline:ledger] recorded") &&
            file_contains_text(path, "[pipeline] COMPLETE ->");
    if (!strcmp(probe,"run-semantic"))
        /* Real (non-dry-run) recipe execution: the stage genuinely invoked
         * the real BonfyreHash sibling binary and it exited 0, recorded in
         * the durable run manifest -- not just a validated plan. */
        return valid_json_document(path) && file_contains_text(path, "\"status\": \"success\"") &&
            file_contains_text(path, "\"operator\": \"BonfyreHash\"") &&
            file_contains_text(path, "\"exit_code\": 0");
    if (!strcmp(probe,"self-ontology-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"surface\":\"self\"") &&
            file_contains_text(path, "\"subjects\":[");
    if (!strcmp(probe,"precision-ontology-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"surface\":\"precision\"") &&
            file_contains_text(path, "\"heuristics\":[");
    if (!strcmp(probe,"cli-status-semantic")) {
        /* "commands: N/M ready" -- require every configured command to
         * actually resolve on this machine (N == M), not merely that the
         * report was produced. */
        char *text = read_text(path, NULL);
        const char *marker;
        int ready = -1, total = -1;
        int ok = 0;
        if (!text) return 0;
        marker = strstr(text, "commands: ");
        if (marker) {
            char *end = NULL;
            ready = (int)strtol(marker + strlen("commands: "), &end, 10);
            if (end && *end == '/') total = (int)strtol(end + 1, NULL, 10);
        }
        ok = total >= 1 && ready == total;
        free(text);
        return ok;
    }
    if (!strcmp(probe,"sli-semantic")) {
        /* The report is checked against the header of the file the node was
         * handed, and that file must carry enough blocks for its weights: a
         * header and a codebook with no blocks is not an encoded tensor. */
        unsigned char head[16 + 256 + 24];
        unsigned long long elements = 0, blocks = 0;
        unsigned int tile_count;
        char elements_marker[64], tiles_marker[32];
        FILE *model;
        if (!input_path || !input_path[0]) return 0;
        model = fopen(input_path, "rb");
        if (!model) return 0;
        if (fread(head, 1, sizeof(head), model) != sizeof(head)) { fclose(model); return 0; }
        fclose(model);
        if (pcap_u32(head, 0) != 0x50464251u) return 0;
        for (int byte = 7; byte >= 0; byte--) {
            elements = (elements << 8) | head[272 + byte];
            blocks = (blocks << 8) | head[280 + byte];
        }
        tile_count = pcap_u32(head + 288, 0);
        if (elements == 0 || blocks == 0 || blocks * 256ull < elements) return 0;
        snprintf(elements_marker, sizeof(elements_marker), "%llu elem", elements);
        snprintf(tiles_marker, sizeof(tiles_marker), "eff_k=%u", tile_count);
        return file_contains_text(path, "tensors:  1") && file_contains_text(path, elements_marker) &&
            file_contains_text(path, tiles_marker);
    }
    if (!strcmp(probe,"sae-semantic")) {
        /* The dictionary must have been fitted to every vector in the
         * recording this node was handed, must reconstruct the vectors it
         * never saw better than their mean does, and must claim no labels. */
        char *text;
        struct stat input_info;
        double vectors = 0.0, dim = 0.0, explained = 0.0, held = 0.0;
        int ok;
        if (!input_path || !input_path[0] || stat(input_path, &input_info) != 0) return 0;
        text = read_text(path, NULL);
        if (!text) return 0;
        ok = strstr(text, "\"kind\":\"sae-train\"") != NULL &&
            parse_labeled_double(text, "\"hidden_dim\":", &dim) && dim >= 1.0 &&
            parse_labeled_double(text, "\"vectors\":", &vectors) &&
            vectors * dim * 4.0 == (double)input_info.st_size &&
            parse_labeled_double(text, "\"held_out\":", &held) && held >= 4.0 &&
            parse_labeled_double(text, "\"held_out_variance_explained\":", &explained) && explained >= 0.5 &&
            strstr(text, "\"labelled_features\":0") != NULL;
        free(text);
        return ok;
    }
    if (!strcmp(probe,"reason-semantic")) {
        /* The session must be keyed by the digest of the prompt this node
         * was handed, and a model must have answered it: the organ prints
         * the answer's size and the model's name only after a completion
         * came back. */
        char digest[65];
        char marker[96];
        if (!input_path || !input_path[0] || bf_sha256_file(input_path, digest) != 0) return 0;
        snprintf(marker, sizeof(marker), "prompt_sha256: %s", digest);
        return file_contains_text(path, marker) && file_contains_text(path, " bytes from ") &&
            !file_contains_text(path, ": 0 bytes from ");
    }
    if (!strcmp(probe,"physics-semantic"))
        /* The 3-4-5 fixture vector has a norm of exactly 5; verifying the
         * printed |q| against that ties output to the real input, not a
         * canned figure. Default zero-momentum init also fixes |p|=0. */
        return file_contains_text(path, "\xe2\x80\x96q\xe2\x80\x96   = 5.0000") &&
            file_contains_text(path, "\xe2\x80\x96p\xe2\x80\x96   = 0.0000");
    if (!strcmp(probe,"orchestrate-semantic")) {
        char *text; int operators = -1; int ok;
        if (!valid_json_document(path) || !file_contains_text(path, "\"status\":\"ok\"")) return 0;
        text = read_text(path, NULL);
        if (!text) return 0;
        ok = qwen_integer_field(text, "operators", &operators) && operators >= 1;
        free(text);
        return ok;
    }
    if (!strcmp(probe,"net-semantic")) {
        /* A netlist ran when every component it names was run as its organ:
         * the summary's ran count must equal its component count, and more
         * than one must have run or no net was exercised. */
        char *text = read_text(path, NULL);
        double components = 0.0, ran = 0.0;
        int ok;
        if (!text) return 0;
        ok = strstr(text, "status=OK") != NULL && strstr(text, "status=FAIL") == NULL &&
            parse_labeled_double(text, "components=", &components) &&
            parse_labeled_double(text, "  ran=", &ran) &&
            components >= 2.0 && ran == components;
        free(text);
        return ok;
    }
    if (!strcmp(probe,"fpqx-semantic")) {
        /* Two different tensors of one model are aligned. Their codebooks
         * must yield anchors, and a mean cosine that is a real figure: above
         * zero, and short of the 1.0000 a file aligned with itself gives,
         * which was all this probe used to accept. */
        char *text = read_text(path, NULL);
        double anchors = 0.0, cosine = 0.0;
        int ok;
        if (!text) return 0;
        ok = parse_labeled_double(text, "anchors:", &anchors) && anchors >= 1.0 &&
            parse_labeled_double(text, "cosine_mean:", &cosine) && cosine > 0.0 && cosine < 0.9999;
        free(text);
        return ok;
    }
    if (!strcmp(probe,"discipl-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"status\":\"ok\"") &&
            file_contains_text(path, "\"imported_contracts\":34");
    if (!strcmp(probe,"doctor-semantic")) {
        /* Every sibling binary bonfyre-runtime knows about must genuinely
         * resolve on this machine (summary.ready == summary.total); a
         * doctor report is not proof of a healthy install if it merely
         * exists. */
        char *text;
        char *summary;
        int ready = -1, total = -1;
        int ok = 0;

        if (!valid_json_document(path)) return 0;
        text = read_text(path, NULL);
        if (!text) return 0;
        /* "ready" also appears earlier as a per-registry boolean
         * (registries.*.ready:true); anchor to the summary object so that
         * doesn't get parsed as an integer field by mistake. */
        summary = strstr(text, "\"summary\":");
        if (summary) {
            ok = qwen_integer_field(summary, "ready", &ready) &&
                qwen_integer_field(summary, "total", &total) &&
                total >= 1 && ready == total;
        }
        free(text);
        return ok;
    }
    if (!strcmp(probe,"runtime-semantic")) {
        /* /bin/echo's stdout can only contain this exact string, including a
         * dynamically generated --out path, if the real child process was
         * actually invoked with that argv; verify the declared directory it
         * names was genuinely created rather than just pattern-matching text. */
        static const char marker[] = "runtime-executed --out ";
        char *text = read_text(path, NULL);
        char *found;
        int ok = 0;

        if (!text) return 0;
        found = strstr(text, marker);
        if (found) {
            const char *start = found + sizeof(marker) - 1;
            const char *end = start;
            char directory[PATH_MAX];
            struct stat info;

            while (*end && *end != '\n' && *end != ' ') ++end;
            if (end > start && (size_t)(end - start) < sizeof(directory)) {
                memcpy(directory, start, (size_t)(end - start));
                directory[end - start] = '\0';
                ok = stat(directory, &info) == 0 && S_ISDIR(info.st_mode);
            }
        }
        free(text);
        return ok;
    }
    if (!strcmp(probe,"leapfrog-semantic")) {
        /* Parse the real forward Hamiltonian drift and reversibility norms
         * and require them within a sane numeric tolerance, rather than only
         * checking that the report's section headers are present. */
        char *text;
        double max_dh, q_gap, p_gap;
        int ok;

        if (!file_contains_text(path, "Hamiltonian drift") ||
            !file_contains_text(path, "Reversibility") ||
            !file_contains_text(path, "should match") ||
            file_contains_text(path, "nan")) return 0;
        text = read_text(path, NULL);
        if (!text) return 0;
        ok = parse_labeled_double(text, "max |\xce\x94H|   :", &max_dh) && max_dh >= 0.0 && max_dh < 1.0 &&
            parse_labeled_double(text, "\xe2\x80\x96q_rev - q_0\xe2\x80\x96  :", &q_gap) && q_gap >= 0.0 && q_gap < 1e-1 &&
            parse_labeled_double(text, "\xe2\x80\x96p_rev - p_0\xe2\x80\x96  :", &p_gap) && p_gap >= 0.0 && p_gap < 1e-1;
        free(text);
        return ok;
    }
    if (!strcmp(probe,"violence-semantic"))
        return file_contains_text(path, "FINAL REPORT") &&
            file_contains_text(path, "physics.step  : 32") &&
            file_contains_text(path, "nearest score : 1.000000") &&
            !file_contains_text(path, "nan");
    if (!strcmp(probe,"whisper-transcript"))
        return valid_json_document(path) && file_contains_text(path, "\"segments\"");
    if (!strcmp(probe,"clip-candidates"))
        return valid_json_document(path) && file_contains_text(path, "\"clips\"");
    if (!strcmp(probe,"wave-audio"))
        return file_has_prefix(path, riff_prefix, sizeof(riff_prefix));
    if (!strcmp(probe,"jpeg-frame"))
        return file_has_prefix(path, jpeg_prefix, sizeof(jpeg_prefix));
    if (!strcmp(probe,"database-row") || !strcmp(probe,"namespace-object")) return 0;
    if (!strcmp(probe,"sha256-output") || !strcmp(probe,"artifact-integrity")) { char digest[65]; return regular_nonempty_file(path) && bf_sha256_file(path,digest)==0; }
    if (!strcmp(probe,"compress-roundtrip-semantic")) {
        static const unsigned char zstd_magic[] = {0x28, 0xb5, 0x2f, 0xfd};
        char decoded_path[PATH_MAX];
        char input_digest[65], decoded_digest[65];
        pid_t child;
        int status = 0;
        int ok = 0;

        if (!input_path || !input_path[0] || !file_has_prefix(path, zstd_magic, sizeof(zstd_magic))) return 0;
        if (snprintf(decoded_path, sizeof(decoded_path), "%s.roundtrip", path) >= (int)sizeof(decoded_path)) return 0;
        unlink(decoded_path);
        child = fork();
        if (child < 0) return 0;
        if (child == 0) {
            int null_fd = open("/dev/null", O_WRONLY);
            if (null_fd >= 0) { dup2(null_fd, STDOUT_FILENO); dup2(null_fd, STDERR_FILENO); }
            execlp("zstd", "zstd", "-d", "-f", path, "-o", decoded_path, (char *)NULL);
            _exit(127);
        }
        if (waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0 &&
            bf_sha256_file(input_path, input_digest) == 0 &&
            bf_sha256_file(decoded_path, decoded_digest) == 0) {
            ok = strcmp(input_digest, decoded_digest) == 0;
        }
        unlink(decoded_path);
        return ok;
    }
    if (!strcmp(probe,"json-schema")) return valid_json_document(path);
    if (!strcmp(probe,"database-invariant") || !strcmp(probe,"namespace-invariant") ||
        !strcmp(probe,"relation-integrity") || !strcmp(probe,"semantic-fixture") ||
        !strcmp(probe,"round-trip") || !strcmp(probe,"reference-comparison")) return 0;
    return 0;
}

static const char *skip_json_space(const char *cursor) {
    while (*cursor != '\0' && isspace((unsigned char)*cursor)) {
        ++cursor;
    }
    return cursor;
}

static int parse_json_string_value(const char **cursor, char *output, size_t output_size) {
    size_t used = 0;

    if (**cursor != '"' || output_size == 0) {
        return -1;
    }
    ++*cursor;
    while (**cursor != '\0' && **cursor != '"') {
        unsigned char value = (unsigned char)**cursor;

        if (value == '\\') {
            ++*cursor;
            value = (unsigned char)**cursor;
            if (value != '"' && value != '\\' && value != '/') {
                return -1;
            }
        }
        if (used + 1 >= output_size || value < 0x20) {
            return -1;
        }
        output[used++] = (char)value;
        ++*cursor;
    }
    if (**cursor != '"') {
        return -1;
    }
    ++*cursor;
    output[used] = '\0';
    return 0;
}

static int parse_contract_arguments(const char *json,
                                    BfContractArgument arguments[BF_CONTRACT_ARGUMENT_MAX],
                                    char names[BF_CONTRACT_ARGUMENT_MAX][128],
                                    char values[BF_CONTRACT_ARGUMENT_MAX][PATH_MAX],
                                    size_t *argument_count) {
    const char *cursor = skip_json_space(json);
    size_t count = 0;

    if (*cursor != '{') {
        return -1;
    }
    cursor = skip_json_space(cursor + 1);
    while (*cursor != '}') {
        const char *value_start;
        size_t value_length;

        if (count >= BF_CONTRACT_ARGUMENT_MAX ||
            parse_json_string_value(&cursor, names[count], sizeof(names[count])) != 0) {
            return -1;
        }
        cursor = skip_json_space(cursor);
        if (*cursor != ':') {
            return -1;
        }
        cursor = skip_json_space(cursor + 1);
        if (*cursor == '"') {
            if (parse_json_string_value(&cursor, values[count], sizeof(values[count])) != 0) {
                return -1;
            }
        } else {
            value_start = cursor;
            while (*cursor != '\0' && *cursor != ',' && *cursor != '}') {
                ++cursor;
            }
            value_length = (size_t)(cursor - value_start);
            while (value_length > 0 && isspace((unsigned char)value_start[value_length - 1])) {
                --value_length;
            }
            if (value_length == 0 || value_length >= sizeof(values[count])) {
                return -1;
            }
            memcpy(values[count], value_start, value_length);
            values[count][value_length] = '\0';
        }
        arguments[count].name = names[count];
        arguments[count].value = values[count];
        ++count;
        cursor = skip_json_space(cursor);
        if (*cursor == ',') {
            cursor = skip_json_space(cursor + 1);
        } else if (*cursor != '}') {
            return -1;
        }
    }
    cursor = skip_json_space(cursor + 1);
    if (*cursor != '\0') {
        return -1;
    }
    *argument_count = count;
    return 0;
}

static int path_inside_root(const char *root, const char *path) {
    char resolved_root[PATH_MAX], resolved_path[PATH_MAX];
    size_t root_length;
    if (!root || !path || !realpath(root,resolved_root) || !realpath(path,resolved_path)) return 0;
    root_length = strlen(resolved_root);
    return !strncmp(resolved_root,resolved_path,root_length) &&
           (resolved_path[root_length] == '/' || resolved_path[root_length] == '\0');
}

static int ingest_discovered_output(sqlite3 *db, const char *output_root, const char *path,
                                    const char *media_type, char output_uri[256], FILE *err) {
    if (!path_inside_root(output_root,path)) {
        fprintf(err,"fabric: discovered output escapes authorized root: %s\n",path); return -1;
    }
    return ingest_file(db,path,media_type,output_uri,err);
}

/* Output discovery operates only inside a governed operator directory.  Its
 * filenames are therefore part of the typed contract, unlike a foreign source
 * filename.  Retain the concrete realization MIME instead of flattening every
 * discovered body to application/octet-stream. */
static const char *discovered_media_type(const char *path) {
    const char *extension = strrchr(path, '.');
    if (!extension) return "application/octet-stream";
    ++extension;
    if (!strcasecmp(extension, "json")) return "application/json";
    if (!strcasecmp(extension, "txt") || !strcasecmp(extension, "md")) return "text/plain";
    if (!strcasecmp(extension, "csv")) return "text/csv";
    if (!strcasecmp(extension, "jpg") || !strcasecmp(extension, "jpeg")) return "image/jpeg";
    if (!strcasecmp(extension, "png")) return "image/png";
    if (!strcasecmp(extension, "wav")) return "audio/wav";
    if (!strcasecmp(extension, "pdf")) return "application/pdf";
    return "application/octet-stream";
}

static int ingest_glob_outputs(sqlite3 *db, const char *output_root, const char *pattern,
                               char output_uri[256], char semantic_path[PATH_MAX], FILE *err) {
    glob_t matches = {0}; char absolute[PATH_MAX]; int result = -1;
    if (!pattern || !*pattern || strstr(pattern,"..") || pattern[0] == '/') return -1;
    snprintf(absolute,sizeof(absolute),"%s/%s",output_root,pattern);
    if (glob(absolute,GLOB_NOSORT,NULL,&matches) != 0 || !matches.gl_pathc || matches.gl_pathc > 64) goto done;
    for (size_t index=0; index<matches.gl_pathc; ++index) {
        char uri[256]="";
        if (!regular_nonempty_file(matches.gl_pathv[index]) ||
            ingest_discovered_output(db,output_root,matches.gl_pathv[index],discovered_media_type(matches.gl_pathv[index]),uri,err)) goto done;
        if (!output_uri[0]) { snprintf(output_uri,256,"%s",uri); snprintf(semantic_path,PATH_MAX,"%s",matches.gl_pathv[index]); }
    }
    result = 0;
done:
    globfree(&matches); return result;
}

static int ingest_manifest_outputs(sqlite3 *db, const char *output_root, const char *manifest,
                                   char output_uri[256], char semantic_path[PATH_MAX], FILE *err) {
    FILE *file = fopen(manifest,"r"); char line[PATH_MAX]; int count=0;
    if (!file || !path_inside_root(output_root,manifest)) { if(file)fclose(file); return -1; }
    while (fgets(line,sizeof(line),file)) {
        char *relative=line; relative[strcspn(relative,"\r\n")]='\0';
        if (!*relative || relative[0]=='#') continue;
        if (relative[0]=='/' || strstr(relative,"..") || count >= 64) { fclose(file); return -1; }
        char target[PATH_MAX], uri[256]="";
        snprintf(target,sizeof(target),"%s/%s",output_root,relative);
        if (!regular_nonempty_file(target) || ingest_discovered_output(db,output_root,target,discovered_media_type(target),uri,err)) { fclose(file); return -1; }
        if (!output_uri[0]) { snprintf(output_uri,256,"%s",uri); snprintf(semantic_path,PATH_MAX,"%s",target); }
        ++count;
    }
    fclose(file); return count ? 0 : -1;
}
static int execute_operator(sqlite3 *db, const char *mission, const char *node,
                        const char *op, const char *input,
                        char output_uri[256], BfProcessResult *result,
                        FILE *err) {
    char generation[80] = "uncompiled";
    char binding_generation[80] = "";
    char binary[PATH_MAX] = "";
    char source[PATH_MAX] = "";
    char locator[PATH_MAX] = "";
    char input_directory[PATH_MAX] = "";
    char state[PATH_MAX];
    char directory[PATH_MAX];
    char output_directory[PATH_MAX];
    char stdout_path[PATH_MAX];
    char stderr_path[PATH_MAX];
    char process_error[256] = "";
    BfOperatorContract contract;
    BfOperatorContractBinding contract_binding;
    BfContractExpansion expansion;
    BfContractArgument arguments[BF_CONTRACT_ARGUMENT_MAX];
    char argument_names[BF_CONTRACT_ARGUMENT_MAX][128];
    char argument_values[BF_CONTRACT_ARGUMENT_MAX][PATH_MAX];
    size_t argument_count = 0;
    sqlite3_stmt *statement = NULL;

    scalar(db, "SELECT value FROM fabric_meta WHERE key='catalog_generation'",
           "catalog_generation", generation, sizeof(generation));
    sqlite3_prepare_v2(db,
        "SELECT binary_name,source_path,catalog_generation FROM catalog_bindings WHERE operator_id=?",
        -1, &statement, NULL);
    sqlite3_bind_text(statement, 1, op, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        fprintf(err, "fabric: operator is declared but unbound: %s\n", op);
        return -1;
    }
    snprintf(binary, sizeof(binary), "%s",
             sqlite3_column_text(statement, 0) ? (const char *)sqlite3_column_text(statement, 0) : "");
    snprintf(source, sizeof(source), "%s",
             sqlite3_column_text(statement, 1) ? (const char *)sqlite3_column_text(statement, 1) : "");
    snprintf(binding_generation, sizeof(binding_generation), "%s",
             sqlite3_column_text(statement, 2) ? (const char *)sqlite3_column_text(statement, 2) : "");
    sqlite3_finalize(statement);
    if (strcmp(generation, binding_generation) != 0) {
        fprintf(err, "fabric: stale catalog binding for %s\n", op);
        return -1;
    }
    if (bf_operator_contract_load(db, op, generation, &contract, &contract_binding, err) != 0) return -1;
    if (!scalar(db, "SELECT locator FROM artifacts WHERE uri=?", input, locator, sizeof(locator))) {
        fprintf(err, "fabric: input artifact unresolved: %s\n", input);
        return -1;
    }
    if (parent_directory(locator, input_directory) != 0) {
        fprintf(err, "fabric: input artifact has no containing directory: %s\n", locator);
        return -1;
    }
    if (state_dir(state, err) != 0) return -1;
    snprintf(directory, sizeof(directory), "%s/missions/%s/%s-1", state, mission, node);
    if (mkdirs(directory) != 0) {
        fprintf(err, "fabric: cannot create mission directory\n");
        return -1;
    }
    snprintf(output_directory,sizeof(output_directory),"%s/outputs",directory);
    if (mkdirs(output_directory) != 0) {
        fprintf(err,"fabric: cannot create operator output directory\n"); return -1;
    }
    snprintf(stdout_path, sizeof(stdout_path), "%s/stdout.txt", directory);
    snprintf(stderr_path, sizeof(stderr_path), "%s/stderr.txt", directory);
    BfContractContext context = {
        /* argv[0] is the catalog-resolved executable, not a display name.
         * Commands that locate sibling tools use argv[0] for that resolution. */
        .binary = source, .input_path = locator, .input_dir = input_directory, .input_uri = input,
        .mission_id = mission, .mission_dir = directory, .output_dir = output_directory,
        .state_dir = state,
    };
    if (parse_contract_arguments(contract_binding.argument_defaults, arguments,
                                 argument_names, argument_values, &argument_count) != 0) {
        fprintf(err, "fabric: invalid argument defaults for %s\n", op);
        return -1;
    }
    context.arguments = arguments;
    context.argument_count = argument_count;
    /* A contract may name the receipt of the execution that produced its
     * input, so an operator that records evidence about an attempt is handed
     * a real one. An ingested input has none, and a contract that asks for it
     * then fails to expand. */
    /* The fabric's own event for that execution holds its receipt, when it
     * started and how long it ran, so all three come from one row. */
    char input_receipt[96] = "", input_receipt_at[64] = "", input_receipt_duration[32] = "";
    if (input && input[0]) {
        sqlite3_stmt *receipt_statement = NULL;
        if (sqlite3_prepare_v2(db, "SELECT receipt_id,start_at,duration_ms FROM events "
                                   "WHERE output_uri=? AND status='complete' AND receipt_id IS NOT NULL "
                                   "ORDER BY rowid DESC LIMIT 1",
                               -1, &receipt_statement, NULL) == SQLITE_OK) {
            sqlite3_bind_text(receipt_statement, 1, input, -1, SQLITE_STATIC);
            if (sqlite3_step(receipt_statement) == SQLITE_ROW && sqlite3_column_text(receipt_statement, 0)) {
                snprintf(input_receipt, sizeof(input_receipt), "%s",
                         (const char *)sqlite3_column_text(receipt_statement, 0));
                if (sqlite3_column_text(receipt_statement, 1))
                    snprintf(input_receipt_at, sizeof(input_receipt_at), "%s",
                             (const char *)sqlite3_column_text(receipt_statement, 1));
                if (sqlite3_column_type(receipt_statement, 2) != SQLITE_NULL)
                    snprintf(input_receipt_duration, sizeof(input_receipt_duration), "%lld",
                             (long long)sqlite3_column_int64(receipt_statement, 2));
            }
        }
        sqlite3_finalize(receipt_statement);
    }
    context.input_receipt = input_receipt;
    context.input_receipt_at = input_receipt_at;
    context.input_receipt_duration_ms = input_receipt_duration;
    if (bf_operator_contract_expand(&contract, &context, &expansion, process_error, sizeof(process_error)) != 0) {
        fprintf(err, "fabric: contract expansion failed for %s: %s\n", op, process_error);
        return -1;
    }
    char environment_storage[512];
    const char *environment[16] = {0};
    size_t environment_count = 0;
    snprintf(environment_storage,sizeof(environment_storage),"%s",contract.environment_allowlist);
    char *environment_save = NULL;
    for (char *entry = strtok_r(environment_storage, ",", &environment_save); entry; entry = strtok_r(NULL, ",", &environment_save)) {
        if (!strchr(entry,'=') || environment_count + 1 >= sizeof(environment)/sizeof(environment[0])) {
            bf_operator_contract_free_expansion(&expansion);
            fprintf(err,"fabric: invalid bounded contract environment\n"); return -1;
        }
        environment[environment_count++] = entry;
    }
    const char *working_directory = directory;
    if (!strcmp(contract.working_directory_policy,"output.dir")) working_directory = output_directory;
    else if (!strcmp(contract.working_directory_policy,"state.dir") || !strcmp(contract.working_directory_policy,"runtime.dir")) working_directory = state;
    else if (strcmp(contract.working_directory_policy,"mission.dir")) {
        bf_operator_contract_free_expansion(&expansion);
        fprintf(err,"fabric: unsupported governed working directory policy %s\n",contract.working_directory_policy); return -1;
    }
    BfProcessRequest request = {
        .executable = source,
        .argv = (const char *const *)expansion.argv,
        .working_directory = working_directory,
        .stdout_path = stdout_path,
        .stderr_path = stderr_path,
        .environment = environment,
        .timeout_seconds = contract.timeout_seconds,
        .output_limit_bytes = contract.output_limit_bytes,
    };
    if (bf_process_operator_run(&request, result, process_error, sizeof(process_error)) != 0) {
        bf_operator_contract_free_expansion(&expansion);
        fprintf(err, "fabric: operator %s failed: %s; stderr=%s\n", op, process_error, stderr_path);
        return -1;
    }
    snprintf(result->contract_id,sizeof(result->contract_id),"%s",contract.id);
    snprintf(result->contract_generation,sizeof(result->contract_generation),"%s",contract.generation);
    snprintf(result->contract_family,sizeof(result->contract_family),"%s",contract_binding.family);
    const char *semantic_path = NULL;
    const char *semantic_type = "application/octet-stream";
    char discovered_path[PATH_MAX] = "";
    int semantic_stream = 0;
    int outputs_already_ingested = 0;
    if (!strcmp(contract.output_discovery, "stdout") || !strcmp(contract.output_discovery, "stdout-json")) {
        semantic_path = stdout_path; semantic_stream = 1;
        semantic_type = !strcmp(contract.output_discovery, "stdout-json") ? "application/json" : "text/plain";
    } else if (!strcmp(contract.output_discovery, "stderr")) {
        semantic_path = stderr_path;
        semantic_type = "text/plain";
    } else if (!strncmp(contract.output_discovery, "output.dir/", 11)) {
        const char *relative = contract.output_discovery + 11;
        if (!*relative || strstr(relative,"..") || relative[0] == '/') {
            bf_operator_contract_free_expansion(&expansion);
            fprintf(err,"fabric: output path escapes governed mission directory\n"); return -1;
        }
        snprintf(discovered_path, sizeof(discovered_path), "%s/%s", output_directory, relative);
        semantic_path = discovered_path;
    } else if (!strncmp(contract.output_discovery, "glob:", 5)) {
        const char *relative = contract.output_discovery + 5;
        if (!*relative || strchr(relative,'/') || strstr(relative,"..")) {
            bf_operator_contract_free_expansion(&expansion);
            fprintf(err,"fabric: unsafe output glob\n"); return -1;
        }
        if (ingest_glob_outputs(db,output_directory,relative,output_uri,discovered_path,err)) {
            bf_operator_contract_free_expansion(&expansion);
            fprintf(err,"fabric: no bounded glob outputs for %s\n",op); return -1;
        }
        semantic_path = discovered_path; outputs_already_ingested = 1;
    } else if (!strncmp(contract.output_discovery, "manifest:", 9)) {
        const char *relative = contract.output_discovery + 9;
        if (!*relative || relative[0]=='/' || strstr(relative,"..")) {
            bf_operator_contract_free_expansion(&expansion);
            fprintf(err,"fabric: unsafe output manifest path\n"); return -1;
        }
        char manifest_path[PATH_MAX];
        snprintf(manifest_path,sizeof(manifest_path),"%s/%s",output_directory,relative);
        if (ingest_manifest_outputs(db,output_directory,manifest_path,output_uri,discovered_path,err)) {
            bf_operator_contract_free_expansion(&expansion);
            fprintf(err,"fabric: manifest output discovery failed for %s\n",op); return -1;
        }
        semantic_path = discovered_path; outputs_already_ingested = 1;
    } else if (!strcmp(contract.output_discovery,"stream")) {
        semantic_path = stdout_path; semantic_stream = 1; semantic_type = "text/plain";
    } else if (!strcmp(contract.output_discovery,"database-state") || !strcmp(contract.output_discovery,"namespace-state")) {
        bf_operator_contract_free_expansion(&expansion);
        fprintf(err,"fabric: state output discovery requires a dedicated state adapter: %s\n",contract.output_discovery);
        return -1;
    } else {
        bf_operator_contract_free_expansion(&expansion);
        fprintf(err, "fabric: unsupported output discovery for %s: %s\n", op, contract.output_discovery);
        return -1;
    }
    if (!probe_contract_output(contract.workload_probe, semantic_path, semantic_stream, locator)) {
        bf_operator_contract_free_expansion(&expansion);
        snprintf(result->workload_result,sizeof(result->workload_result),"failed");
        unlink(stdout_path); unlink(stderr_path);
        if (semantic_path != stdout_path && semantic_path != stderr_path) unlink(semantic_path);
        fprintf(err,"fabric: workload probe %s failed for %s\n",contract.workload_probe,op); return -1;
    }
    snprintf(result->workload_result,sizeof(result->workload_result),"passed");
    const char *quality_probe = contract_binding.quality_probe_override[0] != '\0' ?
        contract_binding.quality_probe_override : contract.quality_probe;
    if (!probe_contract_output(quality_probe, semantic_path, semantic_stream, locator)) {
        bf_operator_contract_free_expansion(&expansion);
        snprintf(result->quality_result,sizeof(result->quality_result),"failed");
        unlink(stdout_path); unlink(stderr_path);
        if (semantic_path != stdout_path && semantic_path != stderr_path) unlink(semantic_path);
        fprintf(err,"fabric: quality probe %s failed for %s\n",quality_probe,op); return -1;
    }
    snprintf(result->quality_result,sizeof(result->quality_result),"passed");
    const char *authorized_root = (semantic_path == stdout_path || semantic_path == stderr_path) ? directory : output_directory;
    if (!outputs_already_ingested && ingest_discovered_output(db, authorized_root, semantic_path, semantic_type, output_uri, err) != 0) { bf_operator_contract_free_expansion(&expansion); return -1; }
    snprintf(result->normalized_output_uri, sizeof(result->normalized_output_uri), "%s", output_uri);
    if (!strcmp(contract.id, "qwen_native_inference_v1")) {
        char raw_path[PATH_MAX];
        snprintf(raw_path, sizeof(raw_path), "%s/raw_generation.txt", output_directory);
        if (!regular_nonempty_file(raw_path) ||
            ingest_discovered_output(db, output_directory, raw_path, "text/plain", result->raw_output_uri, err) != 0) {
            bf_operator_contract_free_expansion(&expansion);
            fprintf(err, "fabric: Qwen raw-generation lineage missing\n");
            return -1;
        }
    }
    bf_operator_contract_free_expansion(&expansion);
    return 0;
}
static int work_run(sqlite3*db,const char*mission,FILE*out,FILE*err){sqlite3_stmt*reset=NULL;sqlite3_prepare_v2(db,"UPDATE workgraph_nodes SET status='ready' WHERE mission_id=? AND status='running'",-1,&reset,NULL);sqlite3_bind_text(reset,1,mission,-1,SQLITE_TRANSIENT);if(step(db,reset,err))return 1;int progress=1,failed=0;while(progress){progress=0;sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"SELECT node_id,operator_id,input_uri,depends_on FROM workgraph_nodes WHERE mission_id=? AND status IN ('ready','defined') ORDER BY node_id",-1,&s,NULL);sqlite3_bind_text(s,1,mission,-1,SQLITE_TRANSIENT);while(sqlite3_step(s)==SQLITE_ROW){const char*n=(const char*)sqlite3_column_text(s,0),*op=(const char*)sqlite3_column_text(s,1),*input=(const char*)sqlite3_column_text(s,2),*deps=(const char*)sqlite3_column_text(s,3);int bad=0;if(!ready(db,mission,deps,&bad)){if(bad){sqlite3_stmt*u=NULL;sqlite3_prepare_v2(db,"UPDATE workgraph_nodes SET status='blocked' WHERE mission_id=? AND node_id=?",-1,&u,NULL);sqlite3_bind_text(u,1,mission,-1,SQLITE_TRANSIENT);sqlite3_bind_text(u,2,n,-1,SQLITE_TRANSIENT);step(db,u,err);failed=1;progress=1;}continue;}char output[256]="",rcpt[64]="",payload[1536];BfProcessResult process={0};if(execute_operator(db,mission,n,op,input,output,&process,err)<0){sqlite3_stmt*u=NULL;sqlite3_prepare_v2(db,"UPDATE workgraph_nodes SET status='blocked' WHERE mission_id=? AND node_id=?",-1,&u,NULL);sqlite3_bind_text(u,1,mission,-1,SQLITE_TRANSIENT);sqlite3_bind_text(u,2,n,-1,SQLITE_TRANSIENT);step(db,u,err);failed=1;progress=1;continue;}snprintf(payload,sizeof(payload),"{\"mission\":\"%s\",\"node\":\"%s\",\"operator\":\"%s\",\"contract\":\"%s\",\"contract_generation\":\"%s\",\"family\":\"%s\",\"output\":\"%s\",\"stdout_bytes\":%zu,\"stderr_bytes\":%zu,\"workload_result\":\"%s\",\"quality_result\":\"%s\"}",mission,n,op,process.contract_id,process.contract_generation,process.contract_family,output,process.stdout_bytes,process.stderr_bytes,process.workload_result,process.quality_result);if(receipt(db,"operator-execution",n,payload,rcpt,err)||emit_exec(db,mission,n,op,input,output,&process,rcpt,err)){sqlite3_finalize(s);return 1;}sqlite3_stmt*u=NULL;sqlite3_prepare_v2(db,"UPDATE workgraph_nodes SET status='complete',attempt=attempt+1,output_uri=?,receipt_id=? WHERE mission_id=? AND node_id=?",-1,&u,NULL);sqlite3_bind_text(u,1,output,-1,SQLITE_TRANSIENT);sqlite3_bind_text(u,2,rcpt,-1,SQLITE_TRANSIENT);sqlite3_bind_text(u,3,mission,-1,SQLITE_TRANSIENT);sqlite3_bind_text(u,4,n,-1,SQLITE_TRANSIENT);if(step(db,u,err)){sqlite3_finalize(s);return 1;}sqlite3_stmt*promote=NULL;sqlite3_prepare_v2(db,"UPDATE catalog SET maturity='quality_proven' WHERE id=? AND ?='passed' AND ?='passed'",-1,&promote,NULL);sqlite3_bind_text(promote,1,op,-1,SQLITE_TRANSIENT);sqlite3_bind_text(promote,2,process.workload_result,-1,SQLITE_TRANSIENT);sqlite3_bind_text(promote,3,process.quality_result,-1,SQLITE_TRANSIENT);if(step(db,promote,err)){sqlite3_finalize(s);return 1;}fprintf(out,"node=%s status=complete output=%s receipt=bonfyre://receipt/%s\n",n,output,rcpt);progress=1;}sqlite3_finalize(s);}sqlite3_stmt*c=NULL;sqlite3_prepare_v2(db,"SELECT count(*) FROM workgraph_nodes WHERE mission_id=? AND status!='complete'",-1,&c,NULL);sqlite3_bind_text(c,1,mission,-1,SQLITE_TRANSIENT);int remaining=sqlite3_step(c)==SQLITE_ROW?sqlite3_column_int(c,0):1;sqlite3_finalize(c);char ts[32];stamp(ts);sqlite3_stmt*u=NULL;sqlite3_prepare_v2(db,"UPDATE missions SET status=?,workgraph_cursor=?,updated_at=? WHERE id=?",-1,&u,NULL);sqlite3_bind_text(u,1,remaining?"partial":"complete",-1,SQLITE_STATIC);sqlite3_bind_text(u,2,remaining?"blocked":"terminal",-1,SQLITE_STATIC);sqlite3_bind_text(u,3,ts,-1,SQLITE_TRANSIENT);sqlite3_bind_text(u,4,mission,-1,SQLITE_TRANSIENT);step(db,u,err);return failed||remaining?1:0;}

/* Semantic surfaces invoke a native Power here, rather than growing their own
 * subprocess runner.  The request must name an already-ingested artifact and
 * a compiled command.* contract; this adapter only contributes the exact ACT
 * decision and a durable link to the normal Fabric execution receipt. */
static int safe_power_token(const char *value) {
    if (!value || !*value || strlen(value) > 128) return 0;
    for (; *value; ++value) {
        if (!(isalnum((unsigned char)*value) || *value == '.' || *value == '_' ||
              *value == '-' || *value == ':')) return 0;
    }
    return 1;
}

static int power_invoke(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    const char *operator_id;
    const char *actor;
    const char *purpose;
    const char *input;
    BfAuthority *authority = NULL;
    char mission[96], receipt_id[64] = "", output_uri[256] = "";
    char generation[65] = "uncompiled", payload[1024];
    sqlite3_stmt *statement = NULL;

    if (argc != 9 || strcmp(argv[1], "invoke") ||
        strcmp(argv[3], "--actor") || strcmp(argv[5], "--purpose") ||
        strcmp(argv[7], "--input")) {
        fprintf(err, "usage: operator invoke <command.id> --actor <actor> --purpose <purpose> --input <artifact-uri>\n");
        return 2;
    }
    operator_id = argv[2]; actor = argv[4]; purpose = argv[6]; input = argv[8];
    /* argc includes the terminating option value; keep this explicit so a
     * future option cannot accidentally become an authority-bearing argv. */
    if (!safe_power_token(operator_id) || strncmp(operator_id, "command.", 8) ||
        !safe_power_token(actor) || !safe_power_token(purpose) ||
        strncmp(input, "bonfyre://artifact/", 19)) {
        fprintf(err, "fabric: malformed governed native Power invocation\n");
        return 2;
    }
    if (bf_authority_open_database(db, &authority, err) != 0) return 1;
    int permitted = bf_authority_has(authority, actor, "act", operator_id, NULL, purpose);
    bf_authority_close(authority);
    if (permitted != 1) {
        fprintf(err, "fabric: ACT authority denied for %s on %s (purpose=%s)\n",
                actor, operator_id, purpose);
        return 1;
    }
    if (!scalar(db, "SELECT value FROM fabric_meta WHERE key=?", "catalog_generation",
                generation, sizeof(generation)) || !strcmp(generation, "uncompiled")) {
        fprintf(err, "fabric: native Power invocation requires a compiled catalog\n");
        return 1;
    }
    id(mission, sizeof(mission), "power", operator_id);
    sqlite3_prepare_v2(db,
        "INSERT INTO missions(id,status,context_generation,catalog_generation,provider_generation,input_snapshot,artifact_root,workgraph_cursor,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
        -1, &statement, NULL);
    if (!statement) return 1;
    char timestamp[32]; stamp(timestamp);
    sqlite3_bind_text(statement, 1, mission, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, "defined", -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 3, "semantic-shell", -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 4, generation, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, "native", -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 6, input, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 7, "", -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 8, "defined", -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 9, timestamp, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 10, timestamp, -1, SQLITE_TRANSIENT);
    if (step(db, statement, err)) return 1;
    sqlite3_prepare_v2(db,
        "INSERT INTO workgraph_nodes(mission_id,node_id,operator_id,input_uri,status,attempt,retry_limit,timeout_seconds,depends_on) VALUES(?,?,?,?,'defined',0,0,30,'[]')",
        -1, &statement, NULL);
    if (!statement) return 1;
    sqlite3_bind_text(statement, 1, mission, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, "invoke", -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 3, operator_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, input, -1, SQLITE_TRANSIENT);
    if (step(db, statement, err) || work_run(db, mission, out, err)) return 1;
    sqlite3_prepare_v2(db, "SELECT receipt_id,output_uri FROM workgraph_nodes WHERE mission_id=? AND node_id='invoke'",
                       -1, &statement, NULL);
    if (!statement) return 1;
    sqlite3_bind_text(statement, 1, mission, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) { sqlite3_finalize(statement); return 1; }
    snprintf(receipt_id, sizeof(receipt_id), "%s", sqlite3_column_text(statement, 0));
    snprintf(output_uri, sizeof(output_uri), "%s", sqlite3_column_text(statement, 1));
    sqlite3_finalize(statement);
    snprintf(payload, sizeof(payload),
             "{\"operator_id\":\"%s\",\"actor\":\"%s\",\"purpose\":\"%s\",\"authority\":\"admitted\",\"input\":\"%s\",\"process_receipt\":\"bonfyre://receipt/%s\"}",
             operator_id, actor, purpose, input, receipt_id);
    char authority_receipt[64] = "";
    if (receipt(db, "native-power-invocation", operator_id, payload, authority_receipt, err)) return 1;
    fprintf(out, "native_power_operator=%s\nauthority=admitted\nmission=bonfyre://mission/%s\noutput=%s\nprocess_receipt=bonfyre://receipt/%s\nauthority_receipt=bonfyre://receipt/%s\n",
            operator_id, mission, output_uri, receipt_id, authority_receipt);
    return 0;
}
static const char *work_option(int argc, char **argv, const char *name);
static long long work_integer_option(int argc, char **argv, const char *name,
                                     long long fallback);
/* Writes one argv template the way a person types it: the binary becomes the
 * front-door command, bound values become named places, and a path that
 * belongs to this machine is never carried into a reference page. */
static void reference_argv(FILE *out, const char *template_text, const char *name) {
    char copy[2048], *save = NULL, *token;

    snprintf(copy, sizeof(copy), "%s", template_text);
    for (token = strtok_r(copy, " ", &save); token; token = strtok_r(NULL, " ", &save)) {
        if (!strcmp(token, "${binary}")) { fprintf(out, "bonfyre %s", name); continue; }
        fputc(' ', out);
        if (token[0] == '/' && strncmp(token, "/usr/", 5) && strncmp(token, "/bin/", 5)) {
            fputs("<path>", out);
            continue;
        }
        for (const char *at = token; *at; ) {
            const char *close = at[0] == '$' && at[1] == '{' ? strchr(at, '}') : NULL;
            if (!close) { fputc(*at++, out); continue; }
            fputc('<', out);
            for (const char *inner = at + 2; inner < close; ++inner)
                fputc(*inner == '.' ? '-' : *inner, out);
            fputc('>', out);
            at = close + 1;
        }
    }
}

/* What each check establishes, in the words a reference page uses. */
static const char *probe_meaning(const char *probe) {
    static const struct { const char *probe, *meaning; } known[] = {
        {"output-artifact", "the command exited cleanly and left the output its contract names"},
        {"artifact-integrity", "the output is a regular, non-empty file whose digest was recorded"},
        {"sha256-output", "the printed digest is the SHA-256 of the input that was handed over"},
        {"structured-output", "the output parses as a structured document"},
        {"clean-transcript", "every word left is one of the transcript's own words, in the original order, and at least half of them remain"},
        {"paragraph-structure", "every word of the source is still present in order and nothing beyond structure was added"},
        {"whisper-semantic", "the text file and the timed segments carry the same words, and no segment ends after the audio does"},
        {"transcript-family-semantic", "the cleaned text, the paragraphed text and the raw transcript are all present and the cleaned words come from the transcript"},
        {"nonempty-stream", "the command produced output on its stream"},
        {"quant-roundtrip", "the report names the model it was given and its lowest per-tensor cosine is at least 0.99"},
    };
    for (size_t index = 0; index < sizeof(known) / sizeof(known[0]); ++index)
        if (!strcmp(probe, known[index].probe)) return known[index].meaning;
    return NULL;
}

/* Additional path redaction for usage text; this is not disclosure authority. */
static int reference_showable(const char *text) {
    return text && *text && !strstr(text, "/Users/") && !strstr(text, "/home/") &&
        !strstr(text, "/private/") && !strstr(text, "/var/folders/");
}

/* The first lines of an artifact, fenced, when it is plain text. */
static char *reference_read_bounded(const char *path, size_t *size) {
    int descriptor = open(path, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    FILE *file;
    struct stat info;
    char *text;
    if (descriptor < 0) return NULL;
    if (fstat(descriptor, &info) || !S_ISREG(info.st_mode) || info.st_size > 65536) { close(descriptor); return NULL; }
    file = fdopen(descriptor, "rb");
    if (!file) { close(descriptor); return NULL; }
    text = malloc(65538);
    if (!text) { fclose(file); return NULL; }
    *size = fread(text, 1, 65537, file);
    int bad = ferror(file) || *size > 65536 || memchr(text, 0, *size) != NULL;
    fclose(file);
    if (bad) { free(text); return NULL; }
    text[*size] = 0;
    return text;
}

static int reference_object_keys(const bf_json_doc_t *doc, const bf_json_node_t *object,
                                 const char *const *keys, int count) {
    unsigned int seen = 0;
    if (!object || object->type != BF_JSON_OBJECT) return 0;
    for (const bf_json_node_t *child = bf_json_child_first(doc, object); child; child = bf_json_child_next(doc, child)) {
        int index;
        for (index = 0; index < count; index++)
            if (child->key_len == (int)strlen(keys[index]) && !memcmp(child->key, keys[index], (size_t)child->key_len)) break;
        if (index == count || (seen & (1u << index))) return 0;
        seen |= 1u << index;
    }
    return 1;
}

static int reference_string(const bf_json_node_t *node, char *out, size_t capacity) {
    if (!node || node->type != BF_JSON_STRING || node->val.str.len <= 0 ||
        (size_t)node->val.str.len >= capacity || memchr(node->val.str.ptr, 0, (size_t)node->val.str.len)) return 0;
    bf_json_get_str_copy(node, out, capacity);
    return 1;
}

static int reference_disclosure(sqlite3 *db, const char *path, const char *operator_id, const char *generation,
                                BfInformationPolicy *policy, char digest[65], FILE *err) {
    static const char *const root_keys[] = {"schema", "operator", "contract_generation", "fields"};
    static const char *const field_keys[] = {"field_name", "audiences"};
    char value[256], field[256];
    size_t size = 0;
    char *text = reference_read_bounded(path, &size);
    sqlite3_stmt *validation = NULL;
    int complete_json = 0;
    if (text && sqlite3_prepare_v2(db, "SELECT json_valid(?)", -1, &validation, NULL) == SQLITE_OK) {
        sqlite3_bind_text(validation, 1, text, (int)size, SQLITE_TRANSIENT);
        complete_json = sqlite3_step(validation) == SQLITE_ROW && sqlite3_column_int(validation, 0);
    }
    if (validation) sqlite3_finalize(validation);
    bf_json_doc_t *doc = complete_json ? bf_json_parse(text, size, NULL, 0) : NULL;
    const bf_json_node_t *root = doc ? bf_json_root(doc) : NULL, *fields;
    int valid = 0;
    if (!reference_object_keys(doc, root, root_keys, 4) || root->child_count != 4) goto done;
    if (!reference_string(bf_json_obj_get(doc, root, "schema"), value, sizeof(value)) || strcmp(value, "bonfyre.reference-disclosure.v1")) goto done;
    if (!reference_string(bf_json_obj_get(doc, root, "operator"), value, sizeof(value)) || strcmp(value, operator_id)) goto done;
    if (!reference_string(bf_json_obj_get(doc, root, "contract_generation"), value, sizeof(value)) || strcmp(value, generation)) goto done;
    fields = bf_json_obj_get(doc, root, "fields");
    if (!fields || fields->type != BF_JSON_ARRAY || fields->child_count > BF_INFORMATION_POLICY_MAX_RULES) goto done;
    for (const bf_json_node_t *rule = bf_json_child_first(doc, fields); rule; rule = bf_json_child_next(doc, rule)) {
        unsigned int mask = 0;
        if (!reference_object_keys(doc, rule, field_keys, 2) || rule->child_count != 2 ||
            !reference_string(bf_json_obj_get(doc, rule, "field_name"), field, sizeof(field))) goto done;
        const char *prefix = "bonfyre://artifact/";
        size_t prefix_size = strlen(prefix);
        if (strlen(field) != prefix_size + 64 + strlen("#excerpt") || strncmp(field, prefix, prefix_size) || strcmp(field + prefix_size + 64, "#excerpt")) goto done;
        for (size_t i = prefix_size; i < prefix_size + 64; i++) if (!strchr("0123456789abcdef", field[i])) goto done;
        const bf_json_node_t *audiences = bf_json_obj_get(doc, rule, "audiences");
        if (!audiences || audiences->type != BF_JSON_ARRAY || audiences->child_count > BF_INFORMATION_POLICY_MAX_AUDIENCES) goto done;
        for (const bf_json_node_t *audience = bf_json_child_first(doc, audiences); audience; audience = bf_json_child_next(doc, audience)) {
            int index;
            if (!reference_string(audience, value, sizeof(value))) goto done;
            for (index = 0; index < BF_INFORMATION_POLICY_MAX_AUDIENCES; index++)
                if (!strcmp(value, bf_information_policy_audience_name((BfDisclosureAudience)index))) break;
            if (index == BF_INFORMATION_POLICY_MAX_AUDIENCES || (mask & (1u << index))) goto done;
            mask |= bf_information_policy_audience_mask((BfDisclosureAudience)index);
        }
        if (bf_information_policy_disclose(policy, field, mask, "excerpt not disclosed to the public audience") != BF_INFORMATION_POLICY_OK) goto done;
    }
    bf_sha256_hex((const uint8_t *)text, size, digest);
    valid = 1;
done:
    if (doc) bf_json_free(doc);
    free(text);
    if (!valid) fprintf(err, "fabric: invalid or mismatched reference disclosure declaration\n");
    return valid;
}

static void reference_excerpt(sqlite3 *db, FILE *out, const char *uri, const char *label,
                               const BfInformationPolicy *policy) {
    sqlite3_stmt *row = NULL;
    char *text = NULL;
    size_t size = 0;
    int lines = 0;
    char field[256], actual_digest[65], expected_uri[128];
    BfDisclosureDecision decision;
    if (!uri || snprintf(field, sizeof(field), "%s#excerpt", uri) >= (int)sizeof(field) ||
        bf_information_policy_field_decision(policy, field, BF_AUDIENCE_PUBLIC, &decision) != BF_INFORMATION_POLICY_OK || !decision.allowed) {
        fprintf(out, "%s excerpt withheld: no public disclosure rule.\n\n", label);
        return;
    }

    if (sqlite3_prepare_v2(db, "SELECT locator,media_type,digest FROM artifacts WHERE uri=?", -1, &row, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(row, 1, uri, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(row) == SQLITE_ROW) {
        const char *path = (const char *)sqlite3_column_text(row, 0);
        const char *digest = (const char *)sqlite3_column_text(row, 2);
        text = reference_read_bounded(path, &size);
        if (text) {
            bf_sha256_hex((const uint8_t *)text, size, actual_digest);
            snprintf(expected_uri, sizeof(expected_uri), "bonfyre://artifact/%s", actual_digest);
            if (!digest || strcmp(actual_digest, digest) || strcmp(expected_uri, uri) || !bf_utf8_valid(text, size, NULL)) { free(text); text = NULL; }
        }
    }
    sqlite3_finalize(row);
    if (!text || !reference_showable(text)) { free(text); fprintf(out, "%s excerpt unavailable: retained text did not pass verification.\n\n", label); return; }
    size_t fence = 3, run = 0;
    for (size_t i = 0; i < size; i++) { run = text[i] == '`' ? run + 1 : 0; if (run >= fence) fence = run + 1; }
    for (size_t i = 0; i < fence; i++) fputc('`', out);
    fprintf(out, "text %s\n", label);
    for (const char *at = text; *at && lines < 14; ++at) {
        fputc(*at, out);
        if (*at == '\n') lines++;
    }
    if (size && text[size - 1] != '\n') fputc('\n', out);
    for (size_t i = 0; i < fence; i++) fputc('`', out);
    fprintf(out, "\n\n");
    free(text);
}

/* The command's own usage text, asked of the binary the catalog binds. */
static void reference_usage(FILE *out, const char *binary) {
    int channel[2];
    pid_t child;
    char text[8192];
    size_t used = 0;
    ssize_t got;
    int status = 0;

    if (!binary || !binary[0] || strchr(binary, '/') || pipe(channel) != 0) return;
    child = fork();
    if (child < 0) { close(channel[0]); close(channel[1]); return; }
    if (child == 0) {
        dup2(channel[1], STDOUT_FILENO); dup2(channel[1], STDERR_FILENO);
        close(channel[0]); close(channel[1]);
        alarm(5);
        execlp(binary, binary, "--help", (char *)NULL);
        _exit(127);
    }
    close(channel[1]);
    while (used + 1 < sizeof(text) && (got = read(channel[0], text + used, sizeof(text) - 1 - used)) > 0) used += (size_t)got;
    close(channel[0]);
    waitpid(child, &status, 0);
    text[used] = '\0';
    if (used < 12 || !reference_showable(text) || !strstr(text, "sage")) return;
    fprintf(out, "## Options\n\nThe command's own usage text.\n\n```text\n%s%s```\n\n", text, text[used - 1] == '\n' ? "" : "\n");
}

/* `operator reference <command.id>`: the reference page for one command,
 * read from the compiled catalog, its bound contract and the latest verified
 * run from that generation. Historical events remain in their owner. */
static int operator_reference(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    sqlite3_stmt *row = NULL;
    const char *id, *meaning;
    char name[160], contract[160], family[160], binary[160], argv_template[2048], generation[80];
    char workload[96], quality[96], retries[64], gives[256], input_uri[256] = "", output_uri[256] = "", receipt[96] = "";
    int witnessed = 0, limit_seconds, related = 0;
    long long limit_bytes;
    BfInformationPolicy disclosure;
    char disclosure_digest[65] = "";
    bf_information_policy_init(&disclosure);

    if ((argc != 3 && (argc != 5 || strcmp(argv[3], "--disclosure"))) || strncmp(argv[2], "command.", 8)) {
        fprintf(err, "usage: operator reference <command.id> [--disclosure FILE]\n");
        return 2;
    }
    id = argv[2];
    if (sqlite3_prepare_v2(db,
            "SELECT c.name,c.effect_class,c.maturity,b.family,k.id,k.argv_template,k.input_binding,"
            "k.output_discovery,k.timeout_seconds,k.output_limit_bytes,k.retry_policy,k.workload_probe,"
            "COALESCE(NULLIF(b.quality_probe_override,''),k.quality_probe),k.generation,"
            "COALESCE((SELECT binary_name FROM catalog_bindings WHERE operator_id=c.id),'') "
            "FROM catalog c JOIN operator_contract_bindings b ON b.operator_id=c.id "
            "JOIN operator_contracts k ON k.id=b.contract_id "
            "JOIN catalog_bindings cb ON cb.operator_id=c.id "
            "JOIN fabric_meta m ON m.key='catalog_generation' "
            "WHERE c.id=? AND b.generation=k.generation AND cb.catalog_generation=k.generation "
            "AND m.value=k.generation", -1, &row, NULL) != SQLITE_OK) {
        fprintf(err, "fabric: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_bind_text(row, 1, id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(row) != SQLITE_ROW) {
        sqlite3_finalize(row);
        fprintf(err, "fabric: no current compiled contract binding for %s\n", id);
        return 1;
    }
    snprintf(name, sizeof(name), "%s", (const char *)sqlite3_column_text(row, 0));
    snprintf(family, sizeof(family), "%s", (const char *)sqlite3_column_text(row, 3));
    snprintf(contract, sizeof(contract), "%s", (const char *)sqlite3_column_text(row, 4));
    snprintf(argv_template, sizeof(argv_template), "%s", (const char *)sqlite3_column_text(row, 5));
    snprintf(gives, sizeof(gives), "%s", (const char *)sqlite3_column_text(row, 7));
    limit_seconds = sqlite3_column_int(row, 8);
    limit_bytes = (long long)sqlite3_column_int64(row, 9);
    snprintf(retries, sizeof(retries), "%s", (const char *)sqlite3_column_text(row, 10));
    snprintf(workload, sizeof(workload), "%s", (const char *)sqlite3_column_text(row, 11));
    snprintf(quality, sizeof(quality), "%s", (const char *)sqlite3_column_text(row, 12));
    snprintf(binary, sizeof(binary), "%s", (const char *)sqlite3_column_text(row, 14));
    snprintf(generation, sizeof(generation), "%s", (const char *)sqlite3_column_text(row, 13));
    if (argc == 5 && !reference_disclosure(db, argv[4], id, generation, &disclosure, disclosure_digest, err)) {
        sqlite3_finalize(row);
        return 1;
    }
    fprintf(out, "# bonfyre %s\n\n", name);
    fprintf(out, "Takes one %s and gives `%s`. Bound to contract `%s` in the `%s` family, "
                 "with effect `%s` and maturity `%s` at catalog generation `%s`.\n\n",
            sqlite3_column_text(row, 6), gives, contract, family,
            sqlite3_column_text(row, 1), sqlite3_column_text(row, 2), sqlite3_column_text(row, 13));
    sqlite3_finalize(row);

    if (disclosure_digest[0]) fprintf(out, "Excerpt disclosure declaration: `%s`.\n\n", disclosure_digest);

    fprintf(out, "## Run it\n\nFrom a terminal, or as one node of a mission that leaves a receipt.\n\n```sh Terminal\n");
    reference_argv(out, argv_template, name);
    fprintf(out, "\n```\n\n```sh Mission\n"
                 "bonfyre fabric artifact ingest <input-path>   # prints uri=bonfyre://artifact/...\n"
                 "bonfyre fabric mission create first\n"
                 "bonfyre fabric work add first step %s <uri>\n"
                 "bonfyre fabric work run first                 # prints the output and its receipt\n```\n\n", id);

    if (sqlite3_prepare_v2(db,
            "SELECT e.input_uri,e.output_uri,e.duration_ms,e.receipt_id,e.end_at,u.bytes_in,"
            "json_extract(r.payload,'$.workload_result'),json_extract(r.payload,'$.quality_result'),"
            "r.payload,r.content_hash "
            "FROM events e JOIN receipts r ON r.id=e.receipt_id LEFT JOIN usage_ledger u ON u.event_id=e.id "
            "JOIN execution_metrics m ON m.event_id=e.id AND m.catalog_generation=?3 "
            "WHERE e.operator_id=?1 AND e.status='complete' AND e.output_uri IS NOT NULL "
            "AND e.output_uri<>'' AND r.subject_kind='operator-execution' AND r.subject_id=e.task_id "
            "AND CASE WHEN json_valid(r.payload) THEN "
            "json_extract(r.payload,'$.operator')=e.operator_id "
            "AND json_extract(r.payload,'$.contract')=?2 "
            "AND json_extract(r.payload,'$.contract_generation')=?3 "
            "AND json_extract(r.payload,'$.mission')=e.mission_id "
            "AND json_extract(r.payload,'$.node')=e.task_id "
            "AND json_extract(r.payload,'$.output')=e.output_uri "
            "AND json_extract(r.payload,'$.workload_result')='passed' "
            "AND json_extract(r.payload,'$.quality_result')='passed' ELSE 0 END "
            "ORDER BY e.end_at DESC,e.id DESC", -1, &row, NULL) != SQLITE_OK) {
        fprintf(err, "fabric: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_bind_text(row, 1, id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(row, 2, contract, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(row, 3, generation, -1, SQLITE_TRANSIENT);
    fprintf(out, "## Last verified run\n\n");
    while (sqlite3_step(row) == SQLITE_ROW) {
        const char *payload = (const char *)sqlite3_column_text(row, 8);
        const char *retained_hash = (const char *)sqlite3_column_text(row, 9);
        char actual_hash[65];
        if (!payload || !retained_hash) continue;
        bf_sha256_hex((const uint8_t *)payload, (size_t)sqlite3_column_bytes(row, 8), actual_hash);
        if (strcmp(actual_hash, retained_hash)) continue;
        witnessed = 1;
        snprintf(input_uri, sizeof(input_uri), "%s", (const char *)sqlite3_column_text(row, 0));
        snprintf(output_uri, sizeof(output_uri), "%s", (const char *)sqlite3_column_text(row, 1));
        snprintf(receipt, sizeof(receipt), "%s", (const char *)sqlite3_column_text(row, 3));
        fprintf(out, "| | |\n|---|---|\n| When | %s |\n| In | `%s`, %lld bytes |\n| Out | `%s` |\n"
                     "| Took | %d ms |\n| Workload check | %s |\n| Quality check | %s |\n| Receipt | `%s` |\n\n",
                sqlite3_column_text(row, 4), input_uri, (long long)sqlite3_column_int64(row, 5), output_uri,
                sqlite3_column_int(row, 2), sqlite3_column_text(row, 6), sqlite3_column_text(row, 7), receipt);
        break;
    }
    sqlite3_finalize(row);
    if (witnessed) {
        reference_excerpt(db, out, input_uri, "In", &disclosure);
        reference_excerpt(db, out, output_uri, "Out", &disclosure);
        fprintf(out, "Read the receipt back:\n\n```sh\nbonfyre fabric receipt show %s\n```\n\n", receipt);
    } else {
        fprintf(out, "No verified run recorded for this contract generation in this state.\n\n");
    }

    fprintf(out, "## What is checked\n\nA run is complete only when both checks pass. The result of each is written into the receipt.\n\n");
    meaning = probe_meaning(workload);
    fprintf(out, "- `%s`%s%s\n", workload, meaning ? ": " : "", meaning ? meaning : "");
    meaning = probe_meaning(quality);
    fprintf(out, "- `%s`%s%s\n\n", quality, meaning ? ": " : "", meaning ? meaning : "");

    fprintf(out, "## Limits\n\n| Limit | Value |\n|---|---|\n| Time | %d seconds |\n| Output | %lld bytes |\n| Retries | `%s` |\n\n"
                 "> A run that passes a limit, exits non-zero or fails a check ends as a failed node. Its output is not admitted as an artifact and no later node receives it.\n\n",
            limit_seconds, limit_bytes, retries);

    reference_usage(out, binary);

    /* service_dependent records that a contract waits on an outside service;
     * it is a status, not a function, so it relates no commands to each other. */
    if (strcmp(family, "service_dependent") && sqlite3_prepare_v2(db,
            "SELECT c.name FROM operator_contract_bindings b JOIN catalog c ON c.id=b.operator_id "
            "WHERE b.family=? AND b.operator_id<>? ORDER BY c.name", -1, &row, NULL) == SQLITE_OK) {
        sqlite3_bind_text(row, 1, family, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(row, 2, id, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(row) == SQLITE_ROW) {
            if (!related++) fprintf(out, "## In the same family\n\n");
            fprintf(out, "- [bonfyre %s](/bonfyre-%s)\n", sqlite3_column_text(row, 0), sqlite3_column_text(row, 0));
        }
        sqlite3_finalize(row);
    }
    return 0;
}

static int workflow_start(sqlite3*db,int argc,char**argv,FILE*out,FILE*err){if(argc<5){fprintf(err,"usage: workflow start <mission> <composition> <input-uri>\n");return 2;}sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"SELECT node_id,operator_id,depends_on FROM composition_nodes WHERE composition_id=? ORDER BY node_id",-1,&s,NULL);sqlite3_bind_text(s,1,argv[3],-1,SQLITE_TRANSIENT);int n=0;while(sqlite3_step(s)==SQLITE_ROW){sqlite3_stmt*i=NULL;sqlite3_prepare_v2(db,"INSERT OR REPLACE INTO workgraph_nodes(mission_id,node_id,operator_id,input_uri,status,attempt,retry_limit,timeout_seconds,depends_on) VALUES(?,?,?,?, 'defined',0,0,30,?)",-1,&i,NULL);sqlite3_bind_text(i,1,argv[2],-1,SQLITE_TRANSIENT);sqlite3_bind_text(i,2,(const char*)sqlite3_column_text(s,0),-1,SQLITE_TRANSIENT);sqlite3_bind_text(i,3,(const char*)sqlite3_column_text(s,1),-1,SQLITE_TRANSIENT);sqlite3_bind_text(i,4,argv[4],-1,SQLITE_TRANSIENT);sqlite3_bind_text(i,5,(const char*)sqlite3_column_text(s,2),-1,SQLITE_TRANSIENT);if(step(db,i,err)){sqlite3_finalize(s);return 1;}n++;}sqlite3_finalize(s);if(!n){fprintf(err,"fabric: composition not found or empty\n");return 1;}sqlite3_stmt*g=NULL;sqlite3_prepare_v2(db,"UPDATE missions SET catalog_generation=(SELECT value FROM fabric_meta WHERE key='catalog_generation'),workgraph_cursor='defined' WHERE id=?",-1,&g,NULL);sqlite3_bind_text(g,1,argv[2],-1,SQLITE_TRANSIENT);if(step(db,g,err))return 1;fprintf(out,"mission=%s composition=%s nodes=%d state=defined\n",argv[2],argv[3],n);return 0;}
enum { BF_FRAPPE_MAX_LINEAGES = 64 };

typedef struct {
    char lineage[64];
    char app_dir[64];
    char record_type[160];
    char company_flow_after[64];
    int parent_index;
    int step_index;
} BfFrappeLineageNode;

static int frappe_catalog_string(const bf_json_doc_t *doc, const bf_json_node_t *object,
                                 const char *key, char *out, size_t out_size) {
    const bf_json_node_t *value = bf_json_obj_get(doc, object, key);
    return value && value->type == BF_JSON_STRING &&
           bf_json_get_str_copy(value, out, out_size) > 0;
}

static int load_frappe_lineage_catalog(const char *path, BfFrappeLineageNode nodes[],
                                       size_t *count_out, FILE *err) {
    size_t body_size = 0;
    char *body = read_text(path, &body_size);
    char parse_error[160] = "";
    bf_json_doc_t *document = body ? bf_json_parse(body, body_size, parse_error, sizeof(parse_error)) : NULL;
    const bf_json_node_t *root = document ? bf_json_root(document) : NULL;
    const bf_json_node_t *lineages = root ? bf_json_obj_get(document, root, "lineages") : NULL;
    size_t count = 0;

    if (!lineages || lineages->type != BF_JSON_ARRAY) {
        fprintf(err, "fabric: Frappe lineage catalog is invalid: %s\n",
                parse_error[0] ? parse_error : path);
        bf_json_free(document);
        free(body);
        return -1;
    }
    for (const bf_json_node_t *entry = bf_json_child_first(document, lineages); entry;
         entry = bf_json_child_next(document, entry)) {
        const bf_json_node_t *after;
        BfFrappeLineageNode *node;
        if (count >= BF_FRAPPE_MAX_LINEAGES || entry->type != BF_JSON_OBJECT) {
            fprintf(err, "fabric: Frappe lineage catalog exceeds native limits or contains a non-object\n");
            bf_json_free(document);
            free(body);
            return -1;
        }
        node = &nodes[count];
        memset(node, 0, sizeof(*node));
        node->parent_index = -1;
        if (!frappe_catalog_string(document, entry, "lineage", node->lineage, sizeof(node->lineage)) ||
            !frappe_catalog_string(document, entry, "app_dir", node->app_dir, sizeof(node->app_dir)) ||
            !frappe_catalog_string(document, entry, "representative_doctype",
                                   node->record_type, sizeof(node->record_type))) {
            fprintf(err, "fabric: Frappe lineage is missing lineage, app_dir, or representative_doctype\n");
            bf_json_free(document);
            free(body);
            return -1;
        }
        after = bf_json_obj_get(document, entry, "company_flow_after");
        if (after && after->type != BF_JSON_NULL &&
            !(after->type == BF_JSON_STRING &&
              bf_json_get_str_copy(after, node->company_flow_after,
                                   sizeof(node->company_flow_after)) > 0)) {
            fprintf(err, "fabric: Frappe lineage company_flow_after must be a string or null\n");
            bf_json_free(document);
            free(body);
            return -1;
        }
        for (size_t prior = 0; prior < count; ++prior) {
            if (!strcmp(nodes[prior].lineage, node->lineage) ||
                !strcmp(nodes[prior].app_dir, node->app_dir)) {
                fprintf(err, "fabric: Frappe lineage catalog has duplicate lineage or app_dir\n");
                bf_json_free(document);
                free(body);
                return -1;
            }
        }
        ++count;
    }
    if (!count) {
        fprintf(err, "fabric: Frappe lineage catalog is empty\n");
        bf_json_free(document);
        free(body);
        return -1;
    }
    for (size_t index = 0; index < count; ++index) {
        if (!nodes[index].company_flow_after[0]) continue;
        for (size_t candidate = 0; candidate < count; ++candidate) {
            if (!strcmp(nodes[index].company_flow_after, nodes[candidate].lineage)) {
                nodes[index].parent_index = (int)candidate;
                break;
            }
        }
        if (nodes[index].parent_index < 0) {
            fprintf(err, "fabric: Frappe lineage %s depends on missing predecessor %s\n",
                    nodes[index].lineage, nodes[index].company_flow_after);
            bf_json_free(document);
            free(body);
            return -1;
        }
    }

    size_t emitted = 0;
    bool selected[BF_FRAPPE_MAX_LINEAGES] = {false};
    while (emitted < count) {
        bool progressed = false;
        for (size_t index = 0; index < count; ++index) {
            if (selected[index] ||
                (nodes[index].parent_index >= 0 && !selected[nodes[index].parent_index])) continue;
            selected[index] = true;
            nodes[index].step_index = (int)++emitted;
            progressed = true;
        }
        if (!progressed) {
            fprintf(err, "fabric: Frappe lineage dependency cycle detected\n");
            bf_json_free(document);
            free(body);
            return -1;
        }
    }
    bf_json_free(document);
    free(body);
    *count_out = count;
    return 0;
}

static int app_cross_transition(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    const char *mission_option = work_option(argc, argv, "--mission");
    const char *lineage_catalog = work_option(argc, argv, "--lineage-catalog");
    const char *hold_option = work_option(argc, argv, "--hold-after");
    int hold_after = hold_option != NULL ? atoi(hold_option) : -1;
    int64_t lease_ms = work_integer_option(argc, argv, "--lease-ms", 30000);
    BfFrappeLineageNode lineages[BF_FRAPPE_MAX_LINEAGES];
    size_t lineage_count = 0;
    char mission[128];
    char generation[65] = "uncompiled";
    char last_receipt[64] = "";
    BfWorkgraph *graph = NULL;
    BfWorkgraphResult result;
    int completed = 0;

    if (argc < 3 || !lineage_catalog) {
        fprintf(err, "usage: app cross-transition <AppPack-directory> --lineage-catalog <catalog.json> [--mission ID] [--lease-ms N] [--hold-after N]\n");
        return 2;
    }
    if (load_frappe_lineage_catalog(lineage_catalog, lineages, &lineage_count, err) != 0)
        return 1;
    for (size_t index = 0; index < lineage_count; ++index) {
        char pack[PATH_MAX];
        snprintf(pack, sizeof(pack), "%s/%s.apppack.json", argv[2], lineages[index].app_dir);
        if (access(pack, R_OK) != 0) {
            fprintf(err, "fabric: required AppPack missing: %s\n", pack);
            return 1;
        }
    }
    if (mission_option != NULL) snprintf(mission, sizeof(mission), "%s", mission_option);
    else id(mission, sizeof(mission), "frappe-lineage", "transition");
    scalar(db, "SELECT value FROM fabric_meta WHERE key=?", "catalog_generation",
           generation, sizeof(generation));
    if (bf_workgraph_open_database(db, &graph, err) != 0) return 1;

    if (mission_option == NULL) {
        result = bf_workgraph_create_mission(graph, mission);
        if (result.status != BF_WORKGRAPH_OK) goto workgraph_failure;
        for (size_t step = 1; step <= lineage_count; ++step) {
            BfFrappeLineageNode *lineage = NULL;
            char node_id[32];
            char operator_id[256];
            BfWorkgraphNodeSpec spec;
            for (size_t index = 0; index < lineage_count; ++index) {
                if (lineages[index].step_index == (int)step) {
                    lineage = &lineages[index];
                    break;
                }
            }
            if (!lineage) {
                result.status = BF_WORKGRAPH_INVALID;
                snprintf(result.error_message, sizeof(result.error_message),
                         "Frappe lineage topological order is incomplete");
                goto workgraph_failure;
            }
            snprintf(node_id, sizeof(node_id), "step-%02zu", step);
            snprintf(operator_id, sizeof(operator_id), "application.%s.%s",
                     lineage->lineage, lineage->record_type);
            memset(&spec, 0, sizeof(spec));
            spec.mission_id = mission;
            spec.node_id = node_id;
            spec.operator_id = operator_id;
            spec.family = "frappe-lineage";
            spec.priority = 100;
            spec.retry_limit = 1;
            spec.timeout_seconds = 30;
            spec.backoff_base_ms = 20;
            spec.backoff_multiplier = 2.0;
            spec.backoff_max_ms = 20;
            result = bf_workgraph_add_node(graph, &spec);
            if (result.status != BF_WORKGRAPH_OK) goto workgraph_failure;
            if (lineage->parent_index >= 0) {
                char parent_node[32];
                snprintf(parent_node, sizeof(parent_node), "step-%02d",
                         lineages[lineage->parent_index].step_index);
                result = bf_workgraph_add_dependency(graph, mission, node_id,
                                                     parent_node, "require_success");
                if (result.status != BF_WORKGRAPH_OK) goto workgraph_failure;
            }
        }
        fprintf(out, "mission=bonfyre://mission/%s\n", mission);
        fflush(out);
    } else {
        result = bf_workgraph_reap_expired(graph, mission);
        if (result.status != BF_WORKGRAPH_OK && result.status != BF_WORKGRAPH_NOT_ELIGIBLE)
            goto workgraph_failure;
        result = bf_workgraph_resume(graph, mission);
        if (result.status != BF_WORKGRAPH_OK) goto workgraph_failure;
    }

    for (;;) {
        BfWorkgraphClaimSpec claim_spec = {"frappe-lineage-worker", "frappe-lineage", lease_ms};
        char token[65];
        char record_id[160];
        char uri[256];
        char parent_uri[256] = "";
        char pack[PATH_MAX];
        char revision[65];
        char timestamp[32];
        int step_index;
        BfFrappeLineageNode *lineage = NULL;
        sqlite3_stmt *statement = NULL;

        result = bf_workgraph_claim_next(graph, &claim_spec);
        if (result.status == BF_WORKGRAPH_NOT_ELIGIBLE) break;
        if (result.status != BF_WORKGRAPH_OK) goto workgraph_failure;
        snprintf(token, sizeof(token), "%s", result.claim_token);
        if (sscanf(result.node_id, "step-%d", &step_index) != 1 ||
            step_index < 1 || (size_t)step_index > lineage_count) {
            result.status = BF_WORKGRAPH_INVALID;
            snprintf(result.error_message, sizeof(result.error_message),
                     "invalid Frappe lineage node identifier");
            goto workgraph_failure;
        }
        for (size_t index = 0; index < lineage_count; ++index) {
            if (lineages[index].step_index == step_index) {
                lineage = &lineages[index];
                break;
            }
        }
        if (!lineage) {
            result.status = BF_WORKGRAPH_INVALID;
            snprintf(result.error_message, sizeof(result.error_message),
                     "Frappe lineage node has no catalog entry");
            goto workgraph_failure;
        }
        if (completed == hold_after) {
            fprintf(out, "node_claimed=%s\nlease_expires_at_ms=%lld\n",
                    result.node_id, (long long)result.lease_expires_at_ms);
            fflush(out);
            for (;;) pause();
        }
        snprintf(record_id, sizeof(record_id), "%s-%d", mission, step_index);
        snprintf(uri, sizeof(uri), "bonfyre://application/%s/%s",
                 lineage->lineage, record_id);
        snprintf(pack, sizeof(pack), "%s/%s.apppack.json", argv[2], lineage->app_dir);
        if (bf_sha256_file(pack, revision) != 0) goto storage_failure;
        if (lineage->parent_index >= 0) {
            BfFrappeLineageNode *parent = &lineages[lineage->parent_index];
            snprintf(parent_uri, sizeof(parent_uri), "bonfyre://application/%s/%s-%d",
                     parent->lineage, mission, parent->step_index);
        }
        if (namespace_row(db, uri, "application", record_id, pack, "app-pack", err) != 0)
            goto storage_failure;
        stamp(timestamp);
        if (sqlite3_prepare_v2(db,
                "INSERT INTO application_records(uri,family,record_type,record_id,parent_uri,"
                "workflow_state,permissions,app_pack_revision,catalog_generation,created_at) "
                "VALUES(?,?,?,?,?,'complete','native-lineage-transition',?,?,?) "
                "ON CONFLICT(family,record_type,record_id) DO NOTHING",
                -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
        sqlite3_bind_text(statement, 1, uri, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, lineage->lineage, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 3, lineage->record_type, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 4, record_id, -1, SQLITE_TRANSIENT);
        if (parent_uri[0]) sqlite3_bind_text(statement, 5, parent_uri, -1, SQLITE_TRANSIENT);
        else sqlite3_bind_null(statement, 5);
        sqlite3_bind_text(statement, 6, revision, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 7, generation, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 8, timestamp, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(statement) != SQLITE_DONE) {
            sqlite3_finalize(statement);
            goto storage_failure;
        }
        sqlite3_finalize(statement);
        result = bf_workgraph_complete(graph, mission, result.node_id,
                                       "frappe-lineage-worker", token, uri);
        if (result.status != BF_WORKGRAPH_OK) goto workgraph_failure;
        snprintf(last_receipt, sizeof(last_receipt), "%s", result.receipt_id);
        if (sqlite3_prepare_v2(db,
                "UPDATE application_records SET event_id=?,receipt_id=? WHERE uri=?",
                -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
        sqlite3_bind_text(statement, 1, result.event_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, result.receipt_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 3, uri, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(statement) != SQLITE_DONE) {
            sqlite3_finalize(statement);
            goto storage_failure;
        }
        sqlite3_finalize(statement);
        ++completed;
        fprintf(out, "node_complete=%s record=%s\n", result.node_id, uri);
        fflush(out);
    }
    bf_workgraph_close(graph);
    fprintf(out, "receipt=bonfyre://receipt/%s\nmission=bonfyre://mission/%s\n"
                 "workgraph=frappe-lineage-transition\nterminal_state=complete\nrecords_created=%zu\n",
            last_receipt, mission, lineage_count);
    return 0;

storage_failure:
    fprintf(err, "fabric: Frappe lineage storage failure: %s\n", sqlite3_errmsg(db));
    bf_workgraph_close(graph);
    return 1;
workgraph_failure:
    fprintf(err, "fabric: Frappe lineage WorkGraph failure: %s: %s\n",
            result.error_code, result.error_message);
    bf_workgraph_close(graph);
    return 1;
}
static int system_teardown(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    const char *runtime_root = NULL, *state_root = NULL;
    (void)db;
    if (argc != 7 || strcmp(argv[1],"teardown") || strcmp(argv[2],"--runtime-root") ||
        strcmp(argv[4],"--state-root") || strcmp(argv[6],"--preserve-durable")) {
        fprintf(err,"usage: system teardown --runtime-root <path> --state-root <path> --preserve-durable\n"); return 2;
    }
    runtime_root=argv[3]; state_root=argv[5];
    if (runtime_root[0] != '/' || state_root[0] != '/' || !strcmp(runtime_root,"/") || !strcmp(state_root,"/")) {
        fprintf(err,"fabric: teardown requires explicit non-root absolute paths\n"); return 1;
    }
    static const char *files[] = { "bonfyred.pid", "worker.pid", "runtime.lock", "worker.lock", "bonfyre.sock", "tunnel.sock", "ephemeral-credentials", NULL };
    static const char *directories[] = { "cache", "workers", "sockets", "projections", "plugins", "tmp", NULL };
    for (size_t index=0; files[index]; ++index) {
        char path[PATH_MAX]; snprintf(path,sizeof(path),"%s/%s",runtime_root,files[index]);
        if (strstr(files[index],".pid")) {
            FILE *file=fopen(path,"r"); long pid=0;
            if(file){ if(fscanf(file,"%ld",&pid)==1 && pid>1) kill((pid_t)pid,SIGTERM); fclose(file); }
        }
        if (unlink(path) != 0 && errno != ENOENT) { fprintf(err,"fabric: cannot remove managed runtime file %s\n",path); return 1; }
    }
    for (size_t index=0; directories[index]; ++index) {
        char path[PATH_MAX]; snprintf(path,sizeof(path),"%s/%s",runtime_root,directories[index]);
        if (rmdir(path) != 0 && errno != ENOENT && errno != ENOTEMPTY) { fprintf(err,"fabric: cannot remove managed runtime directory %s\n",path); return 1; }
    }
    char durable_db[PATH_MAX]; snprintf(durable_db,sizeof(durable_db),"%s/fabric.db",state_root);
    if (access(durable_db,F_OK) != 0) { fprintf(err,"fabric: durable state database was not preserved\n"); return 1; }
    fprintf(out,"runtime_root=%s\nstate_root=%s\nstate=teardown-complete\npreserve_durable=true\n",runtime_root,state_root);
    return 0;
}
static int copy_materialized_file(const char *source, const char *destination) {
    int in = open(source, O_RDONLY);
    int out = open(destination, O_CREAT | O_EXCL | O_WRONLY, 0600);
    char buffer[65536];
    ssize_t read_bytes;
    if (in < 0 || out < 0) { if (in >= 0) close(in); if (out >= 0) close(out); return -1; }
    while ((read_bytes = read(in, buffer, sizeof(buffer))) > 0) {
        ssize_t offset = 0;
        while (offset < read_bytes) { ssize_t written = write(out, buffer + offset, (size_t)(read_bytes - offset)); if (written <= 0) { close(in); close(out); unlink(destination); return -1; } offset += written; }
    }
    int result = read_bytes < 0 || fsync(out) != 0 ? -1 : 0;
    close(in); close(out);
    if (result != 0) unlink(destination);
    return result;
}

static int effect_commit(sqlite3 *db, const char *effect_id, int rollback, FILE *out, FILE *err) {
    char source[PATH_MAX] = "", digest[65] = "", kind[96] = "", state[32] = "", target[512] = "";
    sqlite3_stmt *query = NULL;
    sqlite3_prepare_v2(db, "SELECT effect_kind,state,target_uri FROM effects WHERE id=?", -1, &query, NULL);
    sqlite3_bind_text(query, 1, effect_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(query) != SQLITE_ROW) { sqlite3_finalize(query); return 1; }
    snprintf(kind, sizeof(kind), "%s", sqlite3_column_text(query, 0)); snprintf(state, sizeof(state), "%s", sqlite3_column_text(query, 1)); snprintf(target, sizeof(target), "%s", sqlite3_column_text(query, 2)); sqlite3_finalize(query);
    if (rollback) {
        sqlite3_prepare_v2(db, "SELECT created_path FROM effect_operations WHERE effect_id=?", -1, &query, NULL); sqlite3_bind_text(query, 1, effect_id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(query) != SQLITE_ROW) { sqlite3_finalize(query); return 1; }
        char path[PATH_MAX]; snprintf(path, sizeof(path), "%s", sqlite3_column_text(query, 0)); sqlite3_finalize(query);
        if (unlink(path) != 0 && errno != ENOENT) { fprintf(err, "fabric: rollback failed: %s\n", strerror(errno)); return 1; }
        sqlite3_stmt *update = NULL; sqlite3_prepare_v2(db, "UPDATE effects SET state='compensated' WHERE id=?", -1, &update, NULL); sqlite3_bind_text(update, 1, effect_id, -1, SQLITE_TRANSIENT);
        if (step(db, update, err)) return 1;
        fprintf(out, "effect=bonfyre://effect/%s state=compensated\n", effect_id);
        return 0;
    }
    if (strcmp(state, "approved") || strcmp(kind, "publish-local")) { fprintf(err, "fabric: effect is unbound or lacks reversible local authority\n"); return 1; }
    sqlite3_prepare_v2(db, "SELECT locator,digest FROM artifacts WHERE uri=?", -1, &query, NULL); sqlite3_bind_text(query, 1, target, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(query) != SQLITE_ROW) { sqlite3_finalize(query); fprintf(err, "fabric: local publication target is not an artifact\n"); return 1; }
    snprintf(source, sizeof(source), "%s", sqlite3_column_text(query, 0)); snprintf(digest, sizeof(digest), "%s", sqlite3_column_text(query, 1)); sqlite3_finalize(query);
    char state_root[PATH_MAX], directory[PATH_MAX], path[PATH_MAX], actual_digest[65], receipt_id[64], timestamp[32], uri[256], payload[1024];
    if (state_dir(state_root, err)) return 1;
    snprintf(directory, sizeof(directory), "%s/published", state_root);
    if (mkdirs(directory)) return 1;
    snprintf(path, sizeof(path), "%s/%s", directory, digest); if (copy_materialized_file(source, path)) { fprintf(err, "fabric: publication materialization failed\n"); return 1; }
    if (bf_sha256_file(path, actual_digest) || strcmp(actual_digest, digest)) { unlink(path); fprintf(err, "fabric: publication digest verification failed\n"); return 1; }
    snprintf(payload, sizeof(payload), "{\"effect\":\"%s\",\"artifact\":\"%s\",\"published_path\":\"%s\"}", effect_id, target, path);
    if (receipt(db, "effect", effect_id, payload, receipt_id, err)) { unlink(path); return 1; }
    stamp(timestamp); sqlite3_stmt *operation = NULL; sqlite3_prepare_v2(db, "INSERT INTO effect_operations(effect_id,adapter_id,created_path,rollback_path,status,updated_at) VALUES(?,?,?,?,?,?)", -1, &operation, NULL);
    sqlite3_bind_text(operation,1,effect_id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(operation,2,"publish-local-materialize.v1",-1,SQLITE_STATIC); sqlite3_bind_text(operation,3,path,-1,SQLITE_TRANSIENT); sqlite3_bind_text(operation,4,path,-1,SQLITE_TRANSIENT); sqlite3_bind_text(operation,5,"committed",-1,SQLITE_STATIC); sqlite3_bind_text(operation,6,timestamp,-1,SQLITE_TRANSIENT); if (step(db,operation,err)) { unlink(path); return 1; }
    sqlite3_stmt *update = NULL; sqlite3_prepare_v2(db, "UPDATE effects SET state='committed',committed_at=?,receipt_id=? WHERE id=?", -1, &update, NULL); sqlite3_bind_text(update,1,timestamp,-1,SQLITE_TRANSIENT); sqlite3_bind_text(update,2,receipt_id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(update,3,effect_id,-1,SQLITE_TRANSIENT); if (step(db,update,err)) { unlink(path); return 1; }
    snprintf(uri,sizeof(uri),"bonfyre://published/%s",effect_id); if (namespace_row(db,uri,"published",effect_id,path,"verified",err)) return 1;
    fprintf(out,"effect=bonfyre://effect/%s state=committed publication=%s receipt=bonfyre://receipt/%s\n",effect_id,uri,receipt_id); return 0;
}

static const char *work_option(int argc, char **argv, const char *name) {
    for (int index = 2; index + 1 < argc; ++index) {
        if (strcmp(argv[index], name) == 0) {
            return argv[index + 1];
        }
    }
    return NULL;
}

static long long work_integer_option(int argc, char **argv, const char *name,
                                     long long fallback) {
    const char *value = work_option(argc, argv, name);
    char *end = NULL;
    long long parsed;

    if (value == NULL) {
        return fallback;
    }
    parsed = strtoll(value, &end, 10);
    return end != NULL && *end == '\0' ? parsed : fallback;
}

static int print_work_result(const BfWorkgraphResult *result, FILE *out, FILE *err) {
    FILE *stream = result->status == BF_WORKGRAPH_OK ? out : err;

    fprintf(stream, "result=%s\n", bf_workgraph_status_name(result->status));
    fprintf(stream, "mission_id=%s\n", result->mission_id);
    fprintf(stream, "node_id=%s\n", result->node_id);
    fprintf(stream, "effect_id=%s\n", result->effect_id);
    fprintf(stream, "status=%s\n", result->node_status);
    fprintf(stream, "attempt=%d\n", result->attempt);
    fprintf(stream, "worker_id=%s\n", result->worker_id);
    if (result->status == BF_WORKGRAPH_OK && result->claim_token[0] != '\0') {
        fprintf(stream, "claim_token=%s\n", result->claim_token);
    }
    fprintf(stream, "lease_expires_at_ms=%lld\n", (long long)result->lease_expires_at_ms);
    fprintf(stream, "next_attempt_at_ms=%lld\n", (long long)result->next_attempt_at_ms);
    fprintf(stream, "event_id=%s\n", result->event_id);
    fprintf(stream, "receipt_id=%s\n", result->receipt_id);
    fprintf(stream, "error_code=%s\n", result->error_code);
    fprintf(stream, "error_message=%s\n", result->error_message);
    return result->status == BF_WORKGRAPH_OK ? 0 : 1;
}

static int work_dispatch(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    BfWorkgraph *graph = NULL;
    BfWorkgraphResult result;
    const char *verb;

    if (argc < 2) {
        fprintf(err, "usage: work add|dependency|claim-next|claim|renew|complete|fail|cancel-node|cancel-mission|reap-expired|fanout|fanin|effect-prepare|effect-commit|effect-status|compensation-claim|compensate|reconcile-effects|resume|status|history ...\n");
        return 2;
    }
    if (bf_workgraph_open_database(db, &graph, err) != 0) {
        return 1;
    }
    verb = argv[1];
    if (strcmp(verb, "history") == 0 && argc >= 4) {
        BfWorkgraphTransition *transitions = NULL;
        size_t transition_count = 0;

        result = bf_workgraph_list_transitions(graph, argv[2], argv[3],
                                               &transitions, &transition_count);
        if (result.status != BF_WORKGRAPH_OK) {
            bf_workgraph_close(graph);
            return print_work_result(&result, out, err);
        }
        for (size_t index = 0; index < transition_count; ++index) {
            fprintf(out,
                    "sequence=%lld attempt=%d from_status=%s to_status=%s actor=%s "
                    "event_id=%s receipt_id=%s created_at_ms=%lld\n",
                    (long long)transitions[index].sequence,
                    transitions[index].attempt,
                    transitions[index].from_status,
                    transitions[index].to_status,
                    transitions[index].actor,
                    transitions[index].event_id,
                    transitions[index].receipt_id,
                    (long long)transitions[index].created_at_ms);
        }
        bf_workgraph_free_transitions(transitions);
        bf_workgraph_close(graph);
        return 0;
    } else if (strcmp(verb, "add") == 0 && argc >= 5) {
        BfWorkgraphNodeSpec spec;
        memset(&spec, 0, sizeof(spec));
        spec.mission_id = argv[2];
        spec.node_id = argv[3];
        spec.operator_id = argv[4];
        spec.input_uri = argc > 5 && strncmp(argv[5], "--", 2) != 0 ? argv[5] : NULL;
        spec.family = work_option(argc, argv, "--family");
        spec.priority = (int)work_integer_option(argc, argv, "--priority", 100);
        spec.retry_limit = (int)work_integer_option(argc, argv, "--retries", 1);
        spec.timeout_seconds = (int)work_integer_option(argc, argv, "--timeout-seconds", 30);
        spec.backoff_base_ms = work_integer_option(argc, argv, "--backoff-base-ms", 1000);
        spec.backoff_multiplier = (double)work_integer_option(argc, argv, "--backoff-multiplier", 2);
        spec.backoff_max_ms = work_integer_option(argc, argv, "--backoff-max-ms", 60000);
        spec.jitter_percent = (int)work_integer_option(argc, argv, "--jitter-percent", 0);
        spec.fanout_group = work_option(argc, argv, "--fanout-group");
        spec.parent_node = work_option(argc, argv, "--parent-node");
        spec.fanin_required = (int)work_integer_option(argc, argv, "--fanin-required", 0);
        result = bf_workgraph_add_node(graph, &spec);
    } else if (strcmp(verb, "dependency") == 0 && argc >= 5) {
        result = bf_workgraph_add_dependency(graph, argv[2], argv[3], argv[4],
                                             work_option(argc, argv, "--policy"));
    } else if (strcmp(verb, "claim-next") == 0 && argc >= 3) {
        BfWorkgraphClaimSpec spec = {argv[2], work_option(argc, argv, "--family"),
                                     work_integer_option(argc, argv, "--lease-ms", 30000)};
        result = bf_workgraph_claim_next(graph, &spec);
    } else if (strcmp(verb, "claim") == 0 && argc >= 5) {
        BfWorkgraphClaimSpec spec = {argv[4], work_option(argc, argv, "--family"),
                                     work_integer_option(argc, argv, "--lease-ms", 30000)};
        result = bf_workgraph_claim_node(graph, argv[2], argv[3], &spec);
    } else if (strcmp(verb, "renew") == 0 && argc >= 6) {
        result = bf_workgraph_renew(graph, argv[2], argv[3], argv[4], argv[5],
                                    work_integer_option(argc, argv, "--lease-ms", 30000));
    } else if (strcmp(verb, "complete") == 0 && argc >= 6) {
        result = bf_workgraph_complete(graph, argv[2], argv[3], argv[4], argv[5],
                                       work_option(argc, argv, "--output-uri"));
    } else if (strcmp(verb, "fail") == 0 && argc >= 6) {
        BfWorkgraphFailure failure = {work_option(argc, argv, "--class"),
                                      work_option(argc, argv, "--message")};
        result = bf_workgraph_fail(graph, argv[2], argv[3], argv[4], argv[5], &failure);
    } else if ((strcmp(verb, "cancel-node") == 0 || strcmp(verb, "cancel") == 0) && argc >= 4) {
        result = bf_workgraph_cancel_node(graph, argv[2], argv[3],
                                          argc > 4 ? argv[4] : NULL,
                                          argc > 5 ? argv[5] : NULL);
    } else if (strcmp(verb, "cancel-mission") == 0 && argc >= 3) {
        result = bf_workgraph_cancel_mission(graph, argv[2]);
    } else if (strcmp(verb, "reap-expired") == 0) {
        result = bf_workgraph_reap_expired(graph, argc > 2 ? argv[2] : NULL);
    } else if (strcmp(verb, "fanout") == 0 && argc >= 8) {
        BfWorkgraphFanoutSpec spec = {argv[2], argv[3], argv[4], argv[5],
                                      work_option(argc, argv, "--family"), argv[6],
                                      atoi(argv[7]), work_option(argc, argv, "--failure-policy")};
        result = bf_workgraph_create_fanout(graph, &spec);
    } else if (strcmp(verb, "fanin") == 0 && argc >= 4) {
        result = bf_workgraph_evaluate_fanin(graph, argv[2], argv[3]);
    } else if (strcmp(verb, "effect-prepare") == 0 && argc >= 8) {
        BfWorkgraphEffectSpec spec = {
            argv[6],
            work_option(argc, argv, "--adapter"),
            argv[7],
            work_option(argc, argv, "--input-artifact-uri"),
            work_option(argc, argv, "--verification-policy"),
            work_option(argc, argv, "--rollback-contract"),
            work_option(argc, argv, "--authority")
        };

        if (spec.adapter_id == NULL) {
            spec.adapter_id = "derive-file";
        }
        if (spec.authority_identity == NULL) {
            spec.authority_identity = argv[4];
        }
        result = bf_workgraph_prepare_effect(graph, argv[2], argv[3], argv[4], argv[5], &spec);
    } else if (strcmp(verb, "effect-commit") == 0 && argc >= 7) {
        result = bf_workgraph_commit_effect(graph, argv[2], argv[3], argv[4], argv[5], argv[6]);
    } else if (strcmp(verb, "effect-status") == 0 && argc >= 5) {
        BfWorkgraphEffectRecord record;

        result = bf_workgraph_effect_status(graph, argv[2], argv[3], argv[4], &record);
        if (result.status != BF_WORKGRAPH_OK) {
            bf_workgraph_close(graph);
            return print_work_result(&result, out, err);
        }
        fprintf(out,
                "effect_id=%s\nadapter_id=%s\ntarget_uri=%s\ninput_artifact_uri=%s\n"
                "prepared_state=%s\nverification_policy=%s\nrollback_contract=%s\n"
                "authority_identity=%s\nsimulation=%s\neffect_state=%s\n"
                "recovery_action=%s\nattempt=%d\nreceipt_id=%s\n",
                record.effect_id, record.adapter_id, record.target_uri,
                record.input_artifact_uri, record.prepared_state,
                record.verification_policy, record.rollback_contract,
                record.authority_identity, record.simulation, record.state,
                record.recovery_action, record.attempt, record.receipt_id);
        bf_workgraph_close(graph);
        return 0;
    } else if (strcmp(verb, "compensation-claim") == 0 && argc >= 3) {
        BfWorkgraphClaimSpec spec = {
            argv[2],
            NULL,
            work_integer_option(argc, argv, "--lease-ms", 30000)
        };

        result = bf_workgraph_claim_compensation(graph,
                                                  work_option(argc, argv, "--mission"),
                                                  &spec);
    } else if (strcmp(verb, "compensate") == 0 && argc >= 7) {
        const char *compensation_result = work_option(argc, argv, "--result");
        result = bf_workgraph_compensate(graph, argv[2], argv[3], argv[4], argv[5], argv[6],
                                         compensation_result == NULL || strcmp(compensation_result, "failed") != 0);
    } else if (strcmp(verb, "resume") == 0 && argc >= 3) {
        if (bf_workgraph_reconcile_effects(graph, argv[2]) < 0) {
            bf_workgraph_close(graph);
            fprintf(err, "fabric: effect reconciliation failed before resume\n");
            return 1;
        }
        result = bf_workgraph_resume(graph, argv[2]);
    } else if (strcmp(verb, "reconcile-effects") == 0) {
        const char *mission_filter = argc >= 3 ? argv[2] : NULL;
        int inspected;

        if (mission_filter != NULL && !strcmp(mission_filter, "*")) {
            mission_filter = NULL;
        }
        inspected = bf_workgraph_reconcile_effects(graph, mission_filter);
        bf_workgraph_close(graph);
        if (inspected < 0) {
            fprintf(err, "fabric: effect reconciliation failed\n");
            return 1;
        }
        fprintf(out, "reconciled=%d\n", inspected);
        return 0;
    } else if (strcmp(verb, "status") == 0 && argc >= 4) {
        result = bf_workgraph_status(graph, argv[2], argv[3]);
    } else {
        bf_workgraph_close(graph);
        fprintf(err, "fabric: invalid work command\n");
        return 2;
    }
    bf_workgraph_close(graph);
    return print_work_result(&result, out, err);
}

static int print_lifecycle_result(const BfLifecycleResult *result, FILE *out,
                                  FILE *err) {
    FILE *stream = result->status == BF_LIFECYCLE_OK ? out : err;
    fprintf(stream, "result=%s\nidentity=%s\nstate=%s\nevent_id=%s\nreplayed=%d\nerror_message=%s\n",
            bf_lifecycle_status_name(result->status), result->identity,
            result->state, result->event_id, result->replayed,
            result->error_message);
    return result->status == BF_LIFECYCLE_OK ? 0 : 1;
}

static int lifecycle_dispatch(sqlite3 *db, int argc, char **argv, FILE *out,
                              FILE *err) {
    BfLifecycleResult result;
    if (!strcmp(argv[0], "lifecycle") && argc == 2 &&
        !strcmp(argv[1], "register-restate")) {
        result = bf_lifecycle_register_restate(db);
    } else if (!strcmp(argv[0], "lifecycle") && argc == 2 &&
               !strcmp(argv[1], "register-capability-fulfillment")) {
        result = bf_lifecycle_register_capability_fulfillment(db);
    } else if (!strcmp(argv[0], "lifecycle") && argc == 2 &&
               !strcmp(argv[1], "register-capability-fulfillment-rail")) {
        result = bf_lifecycle_register_capability_fulfillment_rail(db);
    } else if (!strcmp(argv[0], "worker-run") && argc >= 2 &&
               !strcmp(argv[1], "start") && argc == 6) {
        BfWorkerRunSpec spec = {argv[2], argv[3], argv[4], argv[5]};
        result = bf_worker_run_start(db, &spec);
    } else if (!strcmp(argv[0], "worker-run") && argc >= 2 &&
               !strcmp(argv[1], "step") && argc >= 5) {
        result = bf_worker_run_step(db, argv[2], argv[3], argv[4],
                                    work_option(argc, argv, "--receipt"));
    } else if (!strcmp(argv[0], "worker-run") && argc == 3 &&
               !strcmp(argv[1], "status")) {
        result = bf_worker_run_status(db, argv[2]);
    } else if (!strcmp(argv[0], "protocol-machine") && argc >= 5 &&
               !strcmp(argv[1], "start")) {
        BfProtocolInstanceSpec spec = {argv[2], argv[3], argv[4],
                                       work_option(argc, argv, "--receipt")};
        result = bf_protocol_instance_start(db, &spec);
    } else if (!strcmp(argv[0], "protocol-machine") && argc >= 5 &&
               !strcmp(argv[1], "step")) {
        result = bf_protocol_instance_step(db, argv[2], argv[3], argv[4],
                                           work_option(argc, argv, "--receipt"));
    } else if (!strcmp(argv[0], "protocol-machine") && argc == 3 &&
               !strcmp(argv[1], "status")) {
        result = bf_protocol_instance_status(db, argv[2]);
    } else {
        fprintf(err, "usage: lifecycle register-restate|register-capability-fulfillment | worker-run start|step|status ... | protocol-machine start|step|status ...\n");
        return 2;
    }
    return print_lifecycle_result(&result, out, err);
}

static int mcp_dispatch(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    sqlite3_stmt *statement = NULL;
    char generation[65] = "uncompiled";
    int tools = 0;
    int contracts = 0;

    if (argc < 2 || (strcmp(argv[1], "meta-abi") && strcmp(argv[1], "tools-list"))) {
        fprintf(err, "usage: mcp meta-abi|tools-list\n");
        return 2;
    }
    scalar(db, "SELECT value FROM fabric_meta WHERE key=?", "catalog_generation",
           generation, sizeof(generation));
    if (sqlite3_prepare_v2(db,
            "SELECT count(*),sum(CASE WHEN c.id IS NOT NULL THEN 1 ELSE 0 END) "
            "FROM catalog_bindings b LEFT JOIN operator_contracts c "
            "ON c.id=b.input_contract OR c.id=(SELECT contract_id FROM operator_contract_bindings "
            "WHERE operator_id=b.operator_id) "
            "WHERE b.operator_id LIKE 'command.%' AND b.binding_state='bound'",
            -1, &statement, NULL) != SQLITE_OK) {
        fprintf(err, "fabric: MCP metadata query failed\n");
        return 1;
    }
    if (sqlite3_step(statement) == SQLITE_ROW) {
        tools = sqlite3_column_int(statement, 0);
        contracts = sqlite3_column_int(statement, 1);
    }
    sqlite3_finalize(statement);
    if (!strcmp(argv[1], "meta-abi")) {
        fprintf(out,
                "{\"protocolVersion\":\"2025-06-18\",\"serverInfo\":{\"name\":\"bonfyre\","
                "\"version\":\"%s\"},\"capabilities\":{\"tools\":true},"
                "\"catalog_generation\":\"%s\",\"tools\":%d,\"typed_contracts\":%d}\n",
                BF_FABRIC_VERSION, generation, tools, contracts);
    } else {
        fprintf(out, "{\"tools\":[");
        if (sqlite3_prepare_v2(db,
                "SELECT b.operator_id,c.input_schema,c.output_schema FROM catalog_bindings b "
                "JOIN catalog c ON c.id=b.operator_id "
                "WHERE b.operator_id LIKE 'command.%' AND b.binding_state='bound' "
                "ORDER BY b.operator_id",
                -1, &statement, NULL) != SQLITE_OK) return 1;
        int first = 1;
        while (sqlite3_step(statement) == SQLITE_ROW) {
            fprintf(out, "%s{\"name\":\"%s\",\"inputSchema\":\"%s\",\"outputSchema\":\"%s\"}",
                    first ? "" : ",", sqlite3_column_text(statement, 0),
                    sqlite3_column_text(statement, 1), sqlite3_column_text(statement, 2));
            first = 0;
        }
        sqlite3_finalize(statement);
        fprintf(out, "],\"catalog_generation\":\"%s\"}\n", generation);
    }
    return tools == 93 ? 0 : 1;
}

/*
 * Deterministic Feldera adapter query surface: exposes the live readiness
 * views created by workgraph_schema.c's version-8 migration. Real Feldera,
 * once it has capacity to run, replaces the computation engine behind these
 * names (SQL pipelines submitted to a running Feldera instance instead of
 * SQLite views recomputed on every query) -- this verb's output contract
 * (view name -> row set) is what stays stable across that swap.
 */
static const char *readiness_view_for(const char *name) {
    if (!strcmp(name, "mission")) return "bf_readiness_mission";
    if (!strcmp(name, "effect-backlog")) return "bf_readiness_effect_backlog";
    if (!strcmp(name, "compensation-backlog")) return "bf_readiness_compensation_backlog";
    if (!strcmp(name, "lease-pressure")) return "bf_readiness_lease_pressure";
    if (!strcmp(name, "capability")) return "bf_readiness_capability";
    return NULL;
}

static int readiness_dispatch(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    if (argc < 2) {
        fprintf(err, "usage: readiness mission|effect-backlog|compensation-backlog|"
                      "lease-pressure|capability [mission-id]\n");
        return 2;
    }
    const char *view = readiness_view_for(argv[1]);
    if (!view) {
        fprintf(err, "fabric: unknown readiness view: %s\n", argv[1]);
        return 2;
    }

    sqlite3_stmt *check = NULL;
    int exists = 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_master WHERE type='view' AND name=?",
                            -1, &check, NULL) == SQLITE_OK) {
        sqlite3_bind_text(check, 1, view, -1, SQLITE_TRANSIENT);
        exists = sqlite3_step(check) == SQLITE_ROW;
    }
    sqlite3_finalize(check);
    if (!exists) {
        fprintf(err, "fabric: readiness view %s not present (run fabric compile first)\n", view);
        return 1;
    }

    int has_mission_id = strcmp(view, "bf_readiness_capability") != 0;
    char sql[256];
    if (argc > 2 && has_mission_id) {
        snprintf(sql, sizeof(sql), "SELECT * FROM %s WHERE mission_id=?", view);
    } else {
        snprintf(sql, sizeof(sql), "SELECT * FROM %s", view);
    }

    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) {
        fprintf(err, "fabric: readiness query failed: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    if (argc > 2 && has_mission_id) {
        sqlite3_bind_text(statement, 1, argv[2], -1, SQLITE_TRANSIENT);
    }

    fprintf(out, "{\"view\":\"%s\",\"rows\":[", view);
    int first_row = 1;
    int column_count = sqlite3_column_count(statement);
    while (sqlite3_step(statement) == SQLITE_ROW) {
        fprintf(out, "%s{", first_row ? "" : ",");
        for (int column = 0; column < column_count; ++column) {
            const char *name = sqlite3_column_name(statement, column);
            fprintf(out, "%s\"%s\":", column ? "," : "", name);
            switch (sqlite3_column_type(statement, column)) {
                case SQLITE_INTEGER:
                    fprintf(out, "%lld", (long long)sqlite3_column_int64(statement, column));
                    break;
                case SQLITE_NULL:
                    fprintf(out, "null");
                    break;
                default:
                    fprintf(out, "\"%s\"", sqlite3_column_text(statement, column));
                    break;
            }
        }
        fprintf(out, "}");
        first_row = 0;
    }
    sqlite3_finalize(statement);
    fprintf(out, "]}\n");
    return 0;
}

int bf_fabric_extended_dispatch(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    if (argc < 1) return BF_FABRIC_NOT_HANDLED;
    if (!strcmp(argv[0], "fabric") && argc > 1 && !strcmp(argv[1], "compile")) return compile_all(db, argc, argv, out, err);
    if (!strcmp(argv[0], "operator") && argc > 1 && !strcmp(argv[1], "reference")) return operator_reference(db, argc, argv, out, err);
    if (!strcmp(argv[0], "operator") && argc > 1 && !strcmp(argv[1], "invoke")) return power_invoke(db, argc, argv, out, err);
    if (!strcmp(argv[0], "workflow") && argc > 1 && !strcmp(argv[1], "start")) return workflow_start(db, argc, argv, out, err);
    if (!strcmp(argv[0], "work") && argc > 1 && strcmp(argv[1], "run")) return work_dispatch(db, argc, argv, out, err);
    if (!strcmp(argv[0], "work") && argc > 2 && !strcmp(argv[1], "run")) return work_run(db, argv[2], out, err);
    if (!strcmp(argv[0], "lifecycle") || !strcmp(argv[0], "worker-run") ||
        !strcmp(argv[0], "protocol-machine")) return lifecycle_dispatch(db, argc, argv, out, err);
    if (!strcmp(argv[0], "mcp")) return mcp_dispatch(db, argc, argv, out, err);
    if (!strcmp(argv[0], "filesystem") && argc == 3 && !strcmp(argv[1], "project")) {
        return bf_filesystem_project(db, argv[2], out, err);
    }
    if (!strcmp(argv[0], "app") && argc > 2 && !strcmp(argv[1], "cross-transition")) return app_cross_transition(db, argc, argv, out, err);
    if (!strcmp(argv[0], "system") && argc > 1 && !strcmp(argv[1], "teardown")) return system_teardown(db, argc, argv, out, err);
    if (!strcmp(argv[0], "effect") && argc > 2 && !strcmp(argv[1], "commit")) return effect_commit(db, argv[2], 0, out, err);
    if (!strcmp(argv[0], "effect") && argc > 2 && !strcmp(argv[1], "rollback")) return effect_commit(db, argv[2], 1, out, err);
    if (!strcmp(argv[0], "readiness")) return readiness_dispatch(db, argc, argv, out, err);
    return BF_FABRIC_NOT_HANDLED;
}
