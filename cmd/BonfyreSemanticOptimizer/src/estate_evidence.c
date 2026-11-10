#define _POSIX_C_SOURCE 200809L
#include "estate_evidence.h"
#include "bf_evidence.h"
#include "bf_foreign_twin.h"
#include "bf_proof.h"
#include "bonfyre.h"
#include <ctype.h>
#include <limits.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

typedef struct { sqlite3 *db; BfEvidence *evidence; BfForeignTwin *foreign; BfProof *proof; } EvidenceOwners;
typedef struct {
    char twin[256],source_url[2048],kind[64],body_path[PATH_MAX],sha256[65],observed_at[64];
    sqlite3_int64 body_bytes; int http_status;
} NativeReceipt;

static void now_iso(char out[32]){time_t v=time(NULL);struct tm u;gmtime_r(&v,&u);if(!strftime(out,32,"%Y-%m-%dT%H:%M:%SZ",&u))snprintf(out,32,"1970-01-01T00:00:00Z");}
static int regular_file(const char *p){struct stat s;return p&&lstat(p,&s)==0&&S_ISREG(s.st_mode)&&!S_ISLNK(s.st_mode);}
static int directory(const char *p){struct stat s;return p&&lstat(p,&s)==0&&S_ISDIR(s.st_mode)&&!S_ISLNK(s.st_mode);}
static int safe_twin(const char *v){size_t n=v?strlen(v):0;if(!n||n>200)return 0;for(const unsigned char*p=(const unsigned char*)v;*p;p++)if(!(isalnum(*p)||*p=='-'||*p=='_'||*p=='.'))return 0;return strcmp(v,".")&&strcmp(v,"..");}
static int hex64(const char *v){if(!v||strlen(v)!=64)return 0;for(int i=0;i<64;i++)if(!((v[i]>='0'&&v[i]<='9')||(v[i]>='a'&&v[i]<='f')))return 0;return 1;}
static int traversal(const char *p){if(!p||!*p)return 1;if(!strcmp(p,"..")||!strncmp(p,"../",3)||strstr(p,"/../"))return 1;size_t n=strlen(p);return n>=3&&!strcmp(p+n-3,"/..");}

static int read_file(const char *path,size_t limit,unsigned char **out,size_t *size){
    struct stat st;if(!regular_file(path)||stat(path,&st)||st.st_size<0||(uint64_t)st.st_size>limit)return 0;
    unsigned char *data=malloc((size_t)st.st_size+1);if(!data)return 0;FILE*f=fopen(path,"rb");if(!f){free(data);return 0;}
    size_t got=fread(data,1,(size_t)st.st_size,f);int close_rc=fclose(f);
    if(got!=(size_t)st.st_size||close_rc){free(data);return 0;}data[got]=0;*out=data;*size=got;return 1;
}
static char *last_line(const unsigned char *data,size_t size){
    while(size&&isspace(data[size-1]))size--;if(!size)return NULL;size_t start=size;
    while(start&&data[start-1]!='\n'&&data[start-1]!='\r')start--;while(start<size&&isspace(data[start]))start++;
    if(start>=size)return NULL;size_t n=size-start;char *v=malloc(n+1);if(!v)return NULL;memcpy(v,data+start,n);v[n]=0;return v;
}
static int schema(sqlite3 *db){
    return sqlite3_exec(db,
      "CREATE TABLE IF NOT EXISTS estate_evidence_sync_receipts("
      "sync_id TEXT PRIMARY KEY,cms_path TEXT NOT NULL,publication_count INTEGER NOT NULL,"
      "publication_watermark TEXT NOT NULL,considered INTEGER NOT NULL,exact INTEGER NOT NULL,"
      "unresolved INTEGER NOT NULL,evidence_relations INTEGER NOT NULL,foreign_observations INTEGER NOT NULL,"
      "measured_invariants INTEGER NOT NULL,proven_invariants INTEGER NOT NULL,generation TEXT NOT NULL DEFAULT '',"
      "synced_at TEXT NOT NULL);"
      "CREATE INDEX IF NOT EXISTS estate_evidence_sync_latest ON estate_evidence_sync_receipts(synced_at DESC,sync_id DESC);",
      NULL,NULL,NULL)==SQLITE_OK;
}
static int open_owners(const char *path,EvidenceOwners *o,FILE *err){
    memset(o,0,sizeof(*o));if(!path||!*path||sqlite3_open_v2(path,&o->db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_FULLMUTEX,NULL)!=SQLITE_OK)return 0;
    sqlite3_busy_timeout(o->db,5000);
    return schema(o->db)&&bf_evidence_open_database(o->db,&o->evidence,err)==0&&bf_foreign_twin_open_database(o->db,&o->foreign,err)==0&&bf_proof_open_database(o->db,&o->proof,err)==0;
}
static void close_owners(EvidenceOwners *o){if(o->proof)bf_proof_close(o->proof);if(o->foreign)bf_foreign_twin_close(o->foreign);if(o->evidence)bf_evidence_close(o->evidence);if(o->db)sqlite3_close(o->db);memset(o,0,sizeof(*o));}
static int cms_state(sqlite3 *db,sqlite3_int64 *count,char watermark[64]){
    sqlite3_stmt*s=NULL;*count=0;watermark[0]=0;
    if(sqlite3_prepare_v2(db,"SELECT COUNT(*),COALESCE(MAX(updated_at),'') FROM publication_occurrence WHERE status='published' AND assertion_state='OBSERVED'",-1,&s,NULL)!=SQLITE_OK)return 0;
    if(sqlite3_step(s)!=SQLITE_ROW){sqlite3_finalize(s);return 0;}*count=sqlite3_column_int64(s,0);const char*v=(const char*)sqlite3_column_text(s,1);snprintf(watermark,64,"%s",v?v:"");sqlite3_finalize(s);return 1;
}
static int receipt_path(const char *root,const char *twin,const char *ref,char out[PATH_MAX]){
    if(!root||!directory(root)||!safe_twin(twin)||!ref||!*ref)return 0;char expected[320];if(snprintf(expected,sizeof expected,"%s.json",twin)>=(int)sizeof expected)return 0;
    static const char prefix[]="foreign/receipts/";const char*name=NULL;
    if(!strncmp(ref,prefix,sizeof(prefix)-1))name=ref+sizeof(prefix)-1;else if(!strcmp(ref,twin))name=expected;else return 0;
    if(!*name||strchr(name,'/')||strcmp(name,expected))return 0;return snprintf(out,PATH_MAX,"%s/receipts/%s",root,name)<PATH_MAX&&regular_file(out);
}
static int copy_text(sqlite3_stmt*s,int c,char*out,size_t n){const char*v=(const char*)sqlite3_column_text(s,c);if(!v||!*v||strlen(v)>=n)return 0;memcpy(out,v,strlen(v)+1);return 1;}
static int parse_receipt(sqlite3 *db,const char *json,const char *twin,NativeReceipt*r){
    sqlite3_stmt*s=NULL;memset(r,0,sizeof(*r));
    const char *sql="SELECT COALESCE(json_extract(?1,'$.schema'),''),COALESCE(json_extract(?1,'$.twin_id'),''),"
      "COALESCE(json_extract(?1,'$.source_url'),''),COALESCE(json_extract(?1,'$.observe_right'),''),"
      "COALESCE(json_extract(?1,'$.policy_decision'),''),COALESCE(json_extract(?1,'$.observation_kind'),''),"
      "COALESCE(json_extract(?1,'$.http_status'),0),COALESCE(json_extract(?1,'$.body_path'),''),"
      "COALESCE(json_extract(?1,'$.body_bytes'),-1),COALESCE(json_extract(?1,'$.content_sha256'),''),"
      "COALESCE(json_extract(?1,'$.observed_at'),'')";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return 0;sqlite3_bind_text(s,1,json,-1,SQLITE_TRANSIENT);int ok=sqlite3_step(s)==SQLITE_ROW;
    if(ok){const char*sc=(const char*)sqlite3_column_text(s,0),*right=(const char*)sqlite3_column_text(s,3),*policy=(const char*)sqlite3_column_text(s,4);
      ok=sc&&!strcmp(sc,"bonfyre-foreign-observation.v1")&&right&&!strcmp(right,"public")&&policy&&!strcmp(policy,"allow")&&
         copy_text(s,1,r->twin,sizeof r->twin)&&copy_text(s,2,r->source_url,sizeof r->source_url)&&copy_text(s,5,r->kind,sizeof r->kind)&&
         copy_text(s,7,r->body_path,sizeof r->body_path)&&copy_text(s,9,r->sha256,sizeof r->sha256)&&copy_text(s,10,r->observed_at,sizeof r->observed_at);
      r->http_status=sqlite3_column_int(s,6);r->body_bytes=sqlite3_column_int64(s,8);
      ok=ok&&!strcmp(r->twin,twin)&&!strcmp(r->kind,"source_observed")&&r->http_status>=200&&r->http_status<300&&r->body_bytes>=0&&hex64(r->sha256);}
    sqlite3_finalize(s);return ok;
}
static int valid_body(const char *root,const NativeReceipt*r){
    if(traversal(r->body_path))return 0;const char*base=strrchr(r->body_path,'/');base=base?base+1:r->body_path;if(!*base||!strcmp(base,".")||!strcmp(base,"..")||strchr(base,'\\'))return 0;
    char path[PATH_MAX];if(snprintf(path,sizeof path,"%s/bodies/%s",root,base)>=(int)sizeof path)return 0;unsigned char*b=NULL;size_t n=0;char digest[65];
    if(!read_file(path,128u*1024u*1024u,&b,&n))return 0;bf_sha256_hex(b,n,digest);free(b);return (sqlite3_int64)n==r->body_bytes&&!strcmp(digest,r->sha256);
}
static sqlite3_int64 scalar(sqlite3 *db,const char *sql){sqlite3_stmt*s=NULL;sqlite3_int64 v=-1;if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK&&sqlite3_step(s)==SQLITE_ROW)v=sqlite3_column_int64(s,0);sqlite3_finalize(s);return v;}
static int save_sync(sqlite3 *db,const char *cms,sqlite3_int64 count,const char *watermark,sqlite3_int64 considered,sqlite3_int64 exact,sqlite3_int64 unresolved,const char *generation,const char *at){
    sqlite3_int64 evidence=scalar(db,"SELECT COUNT(*) FROM evidence_relations WHERE claim LIKE 'publication-occurrence:%:observed'");
    sqlite3_int64 foreign=scalar(db,"SELECT COUNT(*) FROM foreign_twin_observations WHERE predicate='source-observed'");
    sqlite3_int64 measured=scalar(db,"SELECT COUNT(*) FROM solved_invariants WHERE layer='foreign-observation' AND truth_plane='measured'");
    sqlite3_int64 proven=scalar(db,"SELECT COUNT(*) FROM solved_invariants WHERE layer='foreign-observation' AND truth_plane='proven' AND status NOT IN ('challenged','invalidated')");
    if(evidence<0||foreign<0||measured<0||proven<0)return 0;char material[1024],id[65];int n=snprintf(material,sizeof material,"%s|%lld|%s|%s",cms,(long long)count,watermark,generation?generation:"");if(n<0||n>=(int)sizeof material)return 0;bf_sha256_hex((const uint8_t*)material,(size_t)n,id);
    sqlite3_stmt*s=NULL;const char*sql="INSERT INTO estate_evidence_sync_receipts(sync_id,cms_path,publication_count,publication_watermark,considered,exact,unresolved,evidence_relations,foreign_observations,measured_invariants,proven_invariants,generation,synced_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(sync_id) DO UPDATE SET considered=excluded.considered,exact=excluded.exact,unresolved=excluded.unresolved,evidence_relations=excluded.evidence_relations,foreign_observations=excluded.foreign_observations,measured_invariants=excluded.measured_invariants,proven_invariants=excluded.proven_invariants,synced_at=excluded.synced_at";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return 0;sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,cms,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,3,count);sqlite3_bind_text(s,4,watermark,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,5,considered);sqlite3_bind_int64(s,6,exact);sqlite3_bind_int64(s,7,unresolved);sqlite3_bind_int64(s,8,evidence);sqlite3_bind_int64(s,9,foreign);sqlite3_bind_int64(s,10,measured);sqlite3_bind_int64(s,11,proven);sqlite3_bind_text(s,12,generation?generation:"",-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,13,at,-1,SQLITE_TRANSIENT);int ok=sqlite3_step(s)==SQLITE_DONE;sqlite3_finalize(s);return ok;
}

int bf_estate_evidence_sync(const char *control,const char *cms_path,const char *root,const char *generation,FILE *err){
    EvidenceOwners o={0};sqlite3*cms=NULL;sqlite3_stmt*rows=NULL;sqlite3_int64 count=0,considered=0,exact=0,unresolved=0;char watermark[64],at[32];int rc=1,tx=0;now_iso(at);
    if(!control||!*control||!cms_path||!regular_file(cms_path)||!root||!directory(root))return 2;
    if(sqlite3_open_v2(cms_path,&cms,SQLITE_OPEN_READONLY|SQLITE_OPEN_FULLMUTEX,NULL)!=SQLITE_OK||!cms_state(cms,&count,watermark)||!open_owners(control,&o,err))goto done;
    if(sqlite3_exec(o.db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK)goto done;tx=1;
    if(sqlite3_prepare_v2(cms,"SELECT occurrence_id,source_twin,receipt_ref,observed_at FROM publication_occurrence WHERE status='published' AND assertion_state='OBSERVED' ORDER BY occurrence_id",-1,&rows,NULL)!=SQLITE_OK)goto rollback;
    while(sqlite3_step(rows)==SQLITE_ROW){
      const char*occ=(const char*)sqlite3_column_text(rows,0),*twin=(const char*)sqlite3_column_text(rows,1),*ref=(const char*)sqlite3_column_text(rows,2),*occ_at=(const char*)sqlite3_column_text(rows,3);considered++;
      char receipt_file[PATH_MAX];unsigned char*raw=NULL;size_t raw_n=0;char*json=NULL;NativeReceipt receipt;
      if(!occ||!*occ||!twin||!occ_at||!receipt_path(root,twin,ref,receipt_file)||!read_file(receipt_file,4u*1024u*1024u,&raw,&raw_n)||!(json=last_line(raw,raw_n))||!parse_receipt(o.db,json,twin,&receipt)||strcmp(receipt.observed_at,occ_at)||!valid_body(root,&receipt)){free(json);free(raw);unresolved++;continue;}
      char rid[768],subject[320],claim[640],invariant[640],resource[640],statement[1024];
      int good=snprintf(rid,sizeof rid,"foreign:%s:%s:%s",twin,receipt.sha256,receipt.observed_at)<(int)sizeof rid&&snprintf(subject,sizeof subject,"foreign-twin:%s",twin)<(int)sizeof subject&&snprintf(claim,sizeof claim,"publication-occurrence:%s:observed",occ)<(int)sizeof claim&&snprintf(invariant,sizeof invariant,"estate-observation:%s",occ)<(int)sizeof invariant&&snprintf(resource,sizeof resource,"publication-occurrence:%s",occ)<(int)sizeof resource&&snprintf(statement,sizeof statement,"estate retained exact receipt-backed bytes for observation %s; this measures retention/observation and does not prove remote truth",occ)<(int)sizeof statement;
      if(good){BfForeignTwinObservation observation={.receipt_id=rid,.subject=subject,.predicate="source-observed",.source_url=receipt.source_url,.body_sha256=receipt.sha256,.observed_at=receipt.observed_at,.observer="BonfyreProject.ForeignTwin"};
        good=bf_foreign_twin_record(o.foreign,&observation)==0&&bf_evidence_relate(o.evidence,rid,"supports",claim)==BF_EVIDENCE_OK&&bf_proof_record_invariant(o.proof,invariant,resource,"estate","foreign-observation",statement,"measured","cooled",at)==0&&bf_proof_invariant_is_proven(o.proof,invariant)==0;}
      free(json);free(raw);if(!good)goto rollback;exact++;
    }
    sqlite3_finalize(rows);rows=NULL;
    if(!save_sync(o.db,cms_path,count,watermark,considered,exact,unresolved,generation,at)||sqlite3_exec(o.db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK)goto rollback;tx=0;
    printf("{\"schema\":\"bonfyre.estate-evidence-sync.v1\",\"publication_count\":%lld,\"considered\":%lld,\"exact\":%lld,\"unresolved\":%lld,\"truth\":\"measured observations are not proven remote truth\"}\n",(long long)count,(long long)considered,(long long)exact,(long long)unresolved);rc=0;goto done;
rollback:
    if(tx)sqlite3_exec(o.db,"ROLLBACK",NULL,NULL,NULL);
done:
    sqlite3_finalize(rows);if(cms)sqlite3_close(cms);close_owners(&o);return rc;
}
int bf_estate_evidence_status(const char *control,const char *cms_path,FILE *err){
    EvidenceOwners o={0};sqlite3*cms=NULL;sqlite3_stmt*s=NULL;sqlite3_int64 current_count=-1,stored_count=-1;char current_watermark[64]="",stored_watermark[64]="";if(!control||!regular_file(control)||!open_owners(control,&o,err))return 2;
    if(cms_path&&*cms_path&&regular_file(cms_path)&&sqlite3_open_v2(cms_path,&cms,SQLITE_OPEN_READONLY|SQLITE_OPEN_FULLMUTEX,NULL)==SQLITE_OK)cms_state(cms,&current_count,current_watermark);
    if(sqlite3_prepare_v2(o.db,"SELECT publication_count,publication_watermark FROM estate_evidence_sync_receipts ORDER BY synced_at DESC,sync_id DESC LIMIT 1",-1,&s,NULL)==SQLITE_OK&&sqlite3_step(s)==SQLITE_ROW){stored_count=sqlite3_column_int64(s,0);const char*w=(const char*)sqlite3_column_text(s,1);snprintf(stored_watermark,sizeof stored_watermark,"%s",w?w:"");}
    sqlite3_finalize(s);int current=current_count>=0&&stored_count==current_count&&!strcmp(stored_watermark,current_watermark);
    sqlite3_int64 evidence=scalar(o.db,"SELECT COUNT(*) FROM evidence_relations"),foreign=scalar(o.db,"SELECT COUNT(*) FROM foreign_twin_observations"),measured=scalar(o.db,"SELECT COUNT(*) FROM solved_invariants WHERE truth_plane='measured'"),proven=scalar(o.db,"SELECT COUNT(*) FROM solved_invariants WHERE truth_plane='proven' AND status NOT IN ('challenged','invalidated')");
    printf("{\"schema\":\"bonfyre.estate-evidence-status.v1\",\"sync_current\":%s,\"evidence_relations\":%lld,\"foreign_observations\":%lld,\"measured_invariants\":%lld,\"proven_invariants\":%lld,\"truth\":\"measured observations remain distinct from proof\"}\n",current?"true":"false",(long long)evidence,(long long)foreign,(long long)measured,(long long)proven);
    if(cms)sqlite3_close(cms);close_owners(&o);return 0;
}
