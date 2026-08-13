#define _POSIX_C_SOURCE 200809L
#include "bonfyre_fabric.h"
#include "bonfyre_fabric_internal.h"
#include "bf_filesystem.h"
#include "bf_operator.h"
#include "bf_operator_contract.h"
#include "bf_workgraph.h"
#include "bonfyre.h"

#include <errno.h>
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
static int compile_workspace(sqlite3 *db,const char *path,const char *gen,FILE *err){size_t z;char *txt=read_text(path,&z);if(!txt){fprintf(err,"fabric: cannot read workspace %s\n",path);return -1;}char repo[PATH_MAX],state[PATH_MAX];if(parent_directory(path,repo)||state_dir(state,err)){free(txt);return -1;}char *save=NULL,*line=strtok_r(txt,"\n",&save),rid[128]="",kind[64]="",locator[PATH_MAX]="",authority[96]="governed",mode[64]="read-write";int count=0;while(line){char *p=line;while(*p==' '||*p=='\t')p++;if(!strncmp(p,"root ",5)){if(rid[0]){char resolved[PATH_MAX];if(!strcmp(locator,"platform-default")){if(!strcmp(kind,"state"))snprintf(resolved,sizeof(resolved),"%s",state);else snprintf(resolved,sizeof(resolved),"%s/%s",state,rid);}else if(resolve_from(repo,locator,resolved)){fprintf(err,"fabric: cannot resolve root '%s' locator '%s' relative to %s\n",rid,locator,repo);free(txt);return -1;}if(strcmp(kind,"source"))mkdirs(resolved);if(upsert_root(db,rid,kind,resolved,authority,mode,err)){free(txt);return -1;}count++;}if(sscanf(p,"root %127s",rid)!=1){free(txt);return -1;}kind[0]=locator[0]=0;snprintf(authority,sizeof(authority),"governed");snprintf(mode,sizeof(mode),"read-write");}else if(rid[0]){char k[64],v[1024];if(sscanf(p,"%63s %1023s",k,v)==2){if(!strcmp(k,"kind"))snprintf(kind,sizeof(kind),"%s",v);else if(!strcmp(k,"locator"))snprintf(locator,sizeof(locator),"%s",v);else if(!strcmp(k,"authority"))snprintf(authority,sizeof(authority),"%s",v);else if(!strcmp(k,"mode"))snprintf(mode,sizeof(mode),"%s",v);}}line=strtok_r(NULL,"\n",&save);}if(rid[0]){char resolved[PATH_MAX];if(!strcmp(locator,"platform-default")){if(!strcmp(kind,"state"))snprintf(resolved,sizeof(resolved),"%s",state);else snprintf(resolved,sizeof(resolved),"%s/%s",state,rid);}else if(resolve_from(repo,locator,resolved)){fprintf(err,"fabric: cannot resolve root '%s' locator '%s' relative to %s\n",rid,locator,repo);free(txt);return -1;}if(strcmp(kind,"source"))mkdirs(resolved);if(upsert_root(db,rid,kind,resolved,authority,mode,err)){free(txt);return -1;}count++;}free(txt);if(!count){fprintf(err,"fabric: workspace has no roots\n");return -1;}return record_decl(db,"workspace",path,gen,err);}

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
    static const char program[] =
        "import ast,sys; s=open(sys.argv[1],encoding='utf-8').read(); "
        "m=ast.parse(s); assert len(m.body)==1; n=m.body[0]; "
        "assert isinstance(n,ast.Expr) and isinstance(n.value,ast.Call); "
        "assert 'bpy.ops.mesh.primitive_cube_add' in s";
    pid_t child = fork();
    int status = 0;
    if (child < 0) return 0;
    if (child == 0) {
        execl("/usr/bin/python3", "python3", "-c", program, path, (char *)NULL);
        _exit(127);
    }
    return waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
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
        FILE *file = fopen(path, "r");
        char line[1024];
        int cosine_pass = 0, reported = 0;
        if (!file) return 0;
        while (fgets(line, sizeof(line), file)) {
            if (strstr(line, "cos=") && strstr(line, "rmse=") && strstr(line, "bpw=")) reported = 1;
            if (strstr(line, "PASS") && strstr(line, "quantization")) cosine_pass = 1;
        }
        fclose(file);
        return reported && cosine_pass;
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
    if (!strcmp(probe,"clean-transcript"))
        return file_contains_text(path, "Bonfyre completion validates durable evidence");
    if (!strcmp(probe,"paragraph-structure"))
        return file_contains_text(path, "Bonfyre completion validates durable evidence") &&
            valid_utf8_file(path);
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
    if (!strcmp(probe,"kvcache-semantic"))
        return file_contains_text(path, "KV roundtrip:") && file_contains_text(path, "cos=") &&
            !file_contains_text(path, "nan");
    if (!strcmp(probe,"entity-semantic"))
        return file_contains_text(path, "alice@example.com") && file_contains_text(path, "entity");
    if (!strcmp(probe,"flow-semantic"))
        return file_contains_text(path, "completion-flow") && file_contains_text(path, "defined");
    if (!strcmp(probe,"gate-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"tier\": \"pro\"") &&
            file_contains_text(path, "\"status\": \"active\"");
    if (!strcmp(probe,"economy-semantic"))
        return file_contains_text(path, "completion/execute/local") && file_contains_text(path, "$0.010000");
    if (!strcmp(probe,"finance-semantic"))
        return file_contains_text(path, "Added: completion-proof") && file_contains_text(path, "Margin:");
    if (!strcmp(probe,"control-semantic"))
        return file_contains_text(path, "entropy") && file_contains_text(path, "PASS");
    if (!strcmp(probe,"meter-record"))
        return file_contains_text(path, "key=fabric-fixture") &&
            file_contains_text(path, "op=model-inference") &&
            file_contains_text(path, "bytes=64");
    if (!strcmp(probe,"tone-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"feature_set\": \"eGeMAPSv02\"") &&
            file_contains_text(path, "\"feature_count\": 88");
    if (!strcmp(probe,"tag-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"language\": \"en\"") &&
            file_contains_text(path, "\"confidence\"");
    if (!strcmp(probe,"sync-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"valid\": true") &&
            file_contains_text(path, "\"format\": \"bonfyre-intake-v1\"");
    if (!strcmp(probe,"time-semantic"))
        return file_contains_text(path, "scheduled:") && file_contains_text(path, "completion-recipe");
    if (!strcmp(probe,"learn-semantic"))
        return file_contains_text(path, "run=completion-run") && file_contains_text(path, "score=0.920");
    if (!strcmp(probe,"compete-semantic"))
        return file_contains_text(path, "competition created: comp-completion-recipe-proof-");
    if (!strcmp(probe,"pay-semantic"))
        return file_contains_text(path, "Credit $5.00") && file_contains_text(path, "user #42");
    if (!strcmp(probe,"tier-semantic"))
        return file_contains_text(path, "completion-recipe/proof") && file_contains_text(path, "tier=fast");
    if (!strcmp(probe,"outreach-semantic"))
        return file_contains_text(path, "dm -> acceptance@example.com") &&
            file_contains_text(path, "Offer: completion-proof");
    if (!strcmp(probe,"surface-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"valid\":true");
    if (!strcmp(probe,"stitch-semantic"))
        return file_contains_text(path, "MATERIALIZATION PLAN for 'normalized'") &&
            file_contains_text(path, "TOTAL STEPS:");
    if (!strcmp(probe,"distribute-semantic"))
        return file_contains_text(path, "Channel: dm") && file_contains_text(path, "proof-backed local-first offer");
    if (!strcmp(probe,"recipe-validation"))
        return file_contains_text(path, "Recipe is valid") && file_contains_text(path, "SHA-256:");
    if (!strcmp(probe,"wire-ingest-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"packet_count\":2") &&
            file_contains_text(path, "\"byte_count\":1536") &&
            file_contains_text(path, "\"mode\":\"metadata-only\"");
    if (!strcmp(probe,"whisper-semantic"))
        return valid_json_document(path) && file_contains_text(path, "\"sourceSystem\": \"BonfyreTranscribe\"") &&
            file_contains_text(path, "completion validates durable evidence") &&
            file_contains_text(path, "\"confidence\":");
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
            file_contains_text(transcript, "completion validates durable evidence");
    }
    if (!strcmp(probe,"narrate-semantic")) {
        char directory[PATH_MAX];
        char audio_path[PATH_MAX];

        if (!valid_json_document(path) ||
            !file_contains_text(path, "\"renderStatus\": \"completed\"") ||
            !file_contains_text(path, "en_US-lessac-medium.onnx") ||
            parent_directory(path, directory) != 0) {
            return 0;
        }
        snprintf(audio_path, sizeof(audio_path), "%s/artifact.wav", directory);
        return file_has_prefix(audio_path, riff_prefix, sizeof(riff_prefix));
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
            file_contains_text(path, "transcript");
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
        /* A real publisher wrote a random payload to the real ingest
         * socket; a real subscriber, connecting only afterward, must have
         * read back byte-identical content replayed by the running relay. */
        return file_contains_text(path, "\"ready\":true") && file_contains_text(path, "\"payload_matched\":true");
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
        return valid_json_document(path) && file_contains_text(path, "\"model\": \"yolo-v8\"") &&
            file_contains_text(path, "\"frame_count\": 1");
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
    if (!strcmp(probe,"sli-semantic"))
        /* eff_k=4, bits: 8, and tensor count 1 are exactly the values
         * encoded in the real BQFP fixture being inspected. */
        return file_contains_text(path, "tensors:  1") && file_contains_text(path, "bits:     8") &&
            file_contains_text(path, "eff_k=4");
    if (!strcmp(probe,"sae-semantic"))
        /* The feature count and layer are exactly what the contract's own
         * fixed argv requested, so a real synth run is the only way this
         * text can appear. */
        return file_contains_text(path, "synthetic \xc3\x97 256 features @ layer 24");
    if (!strcmp(probe,"reason-semantic")) {
        /* prompt_hash must equal the real SHA-256 of the actual input
         * artifact, not a placeholder -- proves the session was genuinely
         * keyed from this input. */
        char digest[65];
        char marker[96];
        if (!input_path || !input_path[0] || bf_sha256_file(input_path, digest) != 0) return 0;
        snprintf(marker, sizeof(marker), "prompt_hash: %s", digest);
        return file_contains_text(path, marker);
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
    if (!strcmp(probe,"net-semantic"))
        return file_contains_text(path, "status=OK") && file_contains_text(path, "CONVERGED") &&
            !file_contains_text(path, "status=FAIL") && !file_contains_text(path, "status=GAP");
    if (!strcmp(probe,"fpqx-semantic"))
        /* Self-alignment of an identical codebook against itself must
         * mathematically yield perfect cosine preservation. */
        return file_contains_text(path, "cosine_mean: 1.0000") && file_contains_text(path, "anchors:");
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

static int ingest_glob_outputs(sqlite3 *db, const char *output_root, const char *pattern,
                               char output_uri[256], char semantic_path[PATH_MAX], FILE *err) {
    glob_t matches = {0}; char absolute[PATH_MAX]; int result = -1;
    if (!pattern || !*pattern || strstr(pattern,"..") || pattern[0] == '/') return -1;
    snprintf(absolute,sizeof(absolute),"%s/%s",output_root,pattern);
    if (glob(absolute,GLOB_NOSORT,NULL,&matches) != 0 || !matches.gl_pathc || matches.gl_pathc > 64) goto done;
    for (size_t index=0; index<matches.gl_pathc; ++index) {
        char uri[256]="";
        if (!regular_nonempty_file(matches.gl_pathv[index]) ||
            ingest_discovered_output(db,output_root,matches.gl_pathv[index],"application/octet-stream",uri,err)) goto done;
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
        if (!regular_nonempty_file(target) || ingest_discovered_output(db,output_root,target,"application/octet-stream",uri,err)) { fclose(file); return -1; }
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
static const char *work_option(int argc, char **argv, const char *name);
static long long work_integer_option(int argc, char **argv, const char *name,
                                     long long fallback);
static int workflow_start(sqlite3*db,int argc,char**argv,FILE*out,FILE*err){if(argc<5){fprintf(err,"usage: workflow start <mission> <composition> <input-uri>\n");return 2;}sqlite3_stmt*s=NULL;sqlite3_prepare_v2(db,"SELECT node_id,operator_id,depends_on FROM composition_nodes WHERE composition_id=? ORDER BY node_id",-1,&s,NULL);sqlite3_bind_text(s,1,argv[3],-1,SQLITE_TRANSIENT);int n=0;while(sqlite3_step(s)==SQLITE_ROW){sqlite3_stmt*i=NULL;sqlite3_prepare_v2(db,"INSERT OR REPLACE INTO workgraph_nodes(mission_id,node_id,operator_id,input_uri,status,attempt,retry_limit,timeout_seconds,depends_on) VALUES(?,?,?,?, 'defined',0,0,30,?)",-1,&i,NULL);sqlite3_bind_text(i,1,argv[2],-1,SQLITE_TRANSIENT);sqlite3_bind_text(i,2,(const char*)sqlite3_column_text(s,0),-1,SQLITE_TRANSIENT);sqlite3_bind_text(i,3,(const char*)sqlite3_column_text(s,1),-1,SQLITE_TRANSIENT);sqlite3_bind_text(i,4,argv[4],-1,SQLITE_TRANSIENT);sqlite3_bind_text(i,5,(const char*)sqlite3_column_text(s,2),-1,SQLITE_TRANSIENT);if(step(db,i,err)){sqlite3_finalize(s);return 1;}n++;}sqlite3_finalize(s);if(!n){fprintf(err,"fabric: composition not found or empty\n");return 1;}sqlite3_stmt*g=NULL;sqlite3_prepare_v2(db,"UPDATE missions SET catalog_generation=(SELECT value FROM fabric_meta WHERE key='catalog_generation'),workgraph_cursor='defined' WHERE id=?",-1,&g,NULL);sqlite3_bind_text(g,1,argv[2],-1,SQLITE_TRANSIENT);if(step(db,g,err))return 1;fprintf(out,"mission=%s composition=%s nodes=%d state=defined\n",argv[2],argv[3],n);return 0;}
static int app_cross_transition(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    static const char *families[] = {
        "frappe", "crm", "erpnext", "erpnext", "erpnext", "hrms",
        "drive", "wiki", "lms", "helpdesk", "insights"
    };
    static const char *pack_families[] = {
        "frappe", "erpnext", "crm", "hrms", "helpdesk", "lms", "wiki", "drive", "insights"
    };
    static const char *types[] = {
        "PlatformContext", "Deal", "Quotation", "SalesOrder", "Project",
        "StaffingAssignment", "ProjectFolder", "OperatingProcedure",
        "TrainingAssignment", "SupportConfiguration", "AnalyticalProjection"
    };
    const char *mission_option = work_option(argc, argv, "--mission");
    const char *hold_option = work_option(argc, argv, "--hold-after");
    int hold_after = hold_option != NULL ? atoi(hold_option) : -1;
    int64_t lease_ms = work_integer_option(argc, argv, "--lease-ms", 30000);
    char mission[128];
    char generation[65] = "uncompiled";
    char last_receipt[64] = "";
    BfWorkgraph *graph = NULL;
    BfWorkgraphResult result;
    int completed = 0;

    if (argc < 3) {
        fprintf(err, "usage: app cross-transition <AppPack-directory> [--mission ID] [--lease-ms N] [--hold-after N]\n");
        return 2;
    }
    for (size_t index = 0; index < 9; ++index) {
        char pack[PATH_MAX];
        snprintf(pack, sizeof(pack), "%s/%s.apppack.json", argv[2], pack_families[index]);
        if (access(pack, R_OK) != 0) {
            fprintf(err, "fabric: required AppPack missing: %s\n", pack);
            return 1;
        }
    }
    if (mission_option != NULL) snprintf(mission, sizeof(mission), "%s", mission_option);
    else id(mission, sizeof(mission), "cross-app", "transition");
    scalar(db, "SELECT value FROM fabric_meta WHERE key=?", "catalog_generation",
           generation, sizeof(generation));
    if (bf_workgraph_open_database(db, &graph, err) != 0) return 1;

    if (mission_option == NULL) {
        result = bf_workgraph_create_mission(graph, mission);
        if (result.status != BF_WORKGRAPH_OK) goto workgraph_failure;
        for (size_t index = 0; index < 11; ++index) {
            char node_id[32];
            char operator_id[160];
            BfWorkgraphNodeSpec spec;
            snprintf(node_id, sizeof(node_id), "step-%02zu", index + 1);
            snprintf(operator_id, sizeof(operator_id), "application.%s.%s", families[index], types[index]);
            memset(&spec, 0, sizeof(spec));
            spec.mission_id = mission;
            spec.node_id = node_id;
            spec.operator_id = operator_id;
            spec.family = "cross-app";
            spec.priority = 100;
            spec.retry_limit = 1;
            spec.timeout_seconds = 30;
            spec.backoff_base_ms = 20;
            spec.backoff_multiplier = 2.0;
            spec.backoff_max_ms = 20;
            result = bf_workgraph_add_node(graph, &spec);
            if (result.status != BF_WORKGRAPH_OK) goto workgraph_failure;
            if (index > 0) {
                char parent_node[32];
                snprintf(parent_node, sizeof(parent_node), "step-%02zu", index);
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
        BfWorkgraphClaimSpec claim_spec = {"cross-app-worker", "cross-app", lease_ms};
        char token[65];
        char record_id[160];
        char uri[256];
        char parent_uri[256] = "";
        char pack[PATH_MAX];
        char revision[65];
        char timestamp[32];
        int index;
        sqlite3_stmt *statement = NULL;

        result = bf_workgraph_claim_next(graph, &claim_spec);
        if (result.status == BF_WORKGRAPH_NOT_ELIGIBLE) break;
        if (result.status != BF_WORKGRAPH_OK) goto workgraph_failure;
        snprintf(token, sizeof(token), "%s", result.claim_token);
        if (sscanf(result.node_id, "step-%d", &index) != 1 || index < 1 || index > 11) {
            result.status = BF_WORKGRAPH_INVALID;
            snprintf(result.error_message, sizeof(result.error_message), "invalid cross-app node identifier");
            goto workgraph_failure;
        }
        if (completed == hold_after) {
            fprintf(out, "node_claimed=%s\nlease_expires_at_ms=%lld\n",
                    result.node_id, (long long)result.lease_expires_at_ms);
            fflush(out);
            for (;;) pause();
        }
        snprintf(record_id, sizeof(record_id), "%s-%d", mission, index);
        snprintf(uri, sizeof(uri), "bonfyre://application/%s/%s", families[index - 1], record_id);
        snprintf(pack, sizeof(pack), "%s/%s.apppack.json", argv[2], families[index - 1]);
        if (bf_sha256_file(pack, revision) != 0) goto storage_failure;
        if (index > 1) {
            snprintf(parent_uri, sizeof(parent_uri), "bonfyre://application/%s/%s-%d",
                     families[index - 2], mission, index - 1);
        }
        if (namespace_row(db, uri, "application", record_id, pack, "app-pack", err) != 0)
            goto storage_failure;
        stamp(timestamp);
        if (sqlite3_prepare_v2(db,
                "INSERT INTO application_records(uri,family,record_type,record_id,parent_uri,"
                "workflow_state,permissions,app_pack_revision,catalog_generation,created_at) "
                "VALUES(?,?,?,?,?,'complete','native-transition',?,?,?) "
                "ON CONFLICT(family,record_type,record_id) DO NOTHING",
                -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
        sqlite3_bind_text(statement, 1, uri, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, families[index - 1], -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 3, types[index - 1], -1, SQLITE_TRANSIENT);
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
        result = bf_workgraph_complete(graph, mission, result.node_id, "cross-app-worker", token, uri);
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
                 "workgraph=cross-app-transition\nterminal_state=complete\nrecords_created=11\n",
            last_receipt, mission);
    return 0;

storage_failure:
    fprintf(err, "fabric: cross-app storage failure: %s\n", sqlite3_errmsg(db));
    bf_workgraph_close(graph);
    return 1;
workgraph_failure:
    fprintf(err, "fabric: cross-app workgraph failure: %s: %s\n",
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
        if (step(db, update, err)) return 1; fprintf(out, "effect=bonfyre://effect/%s state=compensated\n", effect_id); return 0;
    }
    if (strcmp(state, "approved") || strcmp(kind, "publish-local")) { fprintf(err, "fabric: effect is unbound or lacks reversible local authority\n"); return 1; }
    sqlite3_prepare_v2(db, "SELECT locator,digest FROM artifacts WHERE uri=?", -1, &query, NULL); sqlite3_bind_text(query, 1, target, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(query) != SQLITE_ROW) { sqlite3_finalize(query); fprintf(err, "fabric: local publication target is not an artifact\n"); return 1; }
    snprintf(source, sizeof(source), "%s", sqlite3_column_text(query, 0)); snprintf(digest, sizeof(digest), "%s", sqlite3_column_text(query, 1)); sqlite3_finalize(query);
    char state_root[PATH_MAX], directory[PATH_MAX], path[PATH_MAX], actual_digest[65], receipt_id[64], timestamp[32], uri[256], payload[1024];
    if (state_dir(state_root, err)) return 1; snprintf(directory, sizeof(directory), "%s/published", state_root); if (mkdirs(directory)) return 1;
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
    if (!strcmp(argv[0], "workflow") && argc > 1 && !strcmp(argv[1], "start")) return workflow_start(db, argc, argv, out, err);
    if (!strcmp(argv[0], "work") && argc > 1 && strcmp(argv[1], "run")) return work_dispatch(db, argc, argv, out, err);
    if (!strcmp(argv[0], "work") && argc > 2 && !strcmp(argv[1], "run")) return work_run(db, argv[2], out, err);
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
