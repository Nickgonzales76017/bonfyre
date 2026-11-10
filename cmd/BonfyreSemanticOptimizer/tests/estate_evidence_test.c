#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE 1
#endif
#define _POSIX_C_SOURCE 200809L
#include "bonfyre.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#define CHECK(x) do{if(!(x)){fprintf(stderr,"estate_evidence_test:%d: %s\n",__LINE__,#x);return 1;}}while(0)
static int sql(sqlite3*d,const char*q){return sqlite3_exec(d,q,NULL,NULL,NULL)==SQLITE_OK;}
static int scalar(sqlite3*d,const char*q){sqlite3_stmt*s=NULL;int v=-1;if(sqlite3_prepare_v2(d,q,-1,&s,NULL)==SQLITE_OK&&sqlite3_step(s)==SQLITE_ROW)v=sqlite3_column_int(s,0);sqlite3_finalize(s);return v;}
static int write_text(const char*p,const char*t){FILE*f=fopen(p,"wb");if(!f)return 0;size_t n=strlen(t),w=fwrite(t,1,n,f);int c=fclose(f);return w==n&&c==0;}
static int run_sync(const char*b,const char*control,const char*cms,const char*root){char c[4096];if(snprintf(c,sizeof c,"%s estate-evidence-sync --db %s --cms %s --foreign-root %s --generation fixture-v1",b,control,cms,root)>=(int)sizeof c)return 0;return system(c)==0;}
int main(int argc,char**argv){
 const char*b=argc>1?argv[1]:"./bonfyre-semantic-optimizer";char root[]="/tmp/bf-estate-evidence-XXXXXX",receipts[1024],bodies[1024],cms[1024],control[1024],body[1024],path[1024],json[4096];sqlite3*d=NULL;
 CHECK(mkdtemp(root)!=NULL);CHECK(snprintf(receipts,sizeof receipts,"%s/receipts",root)<(int)sizeof receipts);CHECK(snprintf(bodies,sizeof bodies,"%s/bodies",root)<(int)sizeof bodies);CHECK(mkdir(receipts,0700)==0&&mkdir(bodies,0700)==0);
 CHECK(snprintf(cms,sizeof cms,"%s/cms.db",root)<(int)sizeof cms);CHECK(snprintf(control,sizeof control,"%s/control.db",root)<(int)sizeof control);CHECK(snprintf(body,sizeof body,"%s/good.body",bodies)<(int)sizeof body);CHECK(write_text(body,"exact retained bytes\n"));
 char digest[65];bf_sha256_hex((const uint8_t*)"exact retained bytes\n",21,digest);
 CHECK(sqlite3_open(cms,&d)==SQLITE_OK);CHECK(sql(d,"CREATE TABLE publication_occurrence(id INTEGER PRIMARY KEY,namespace TEXT,updated_at TEXT,status TEXT,occurrence_id TEXT,source_twin TEXT,receipt_ref TEXT,observed_at TEXT,assertion_state TEXT);"
 "INSERT INTO publication_occurrence VALUES"
 "(1,'root','2026-10-26T22:00:00Z','published','good-1','good-twin','foreign/receipts/good-twin.json','2026-10-26T21:00:00Z','OBSERVED'),"
 "(2,'root','2026-10-26T22:00:00Z','published','missing-1','missing-twin','foreign/receipts/missing-twin.json','2026-10-26T21:00:00Z','OBSERVED'),"
 "(3,'root','2026-10-26T22:00:00Z','published','tampered-1','tampered-twin','foreign/receipts/tampered-twin.json','2026-10-26T21:00:00Z','OBSERVED'),"
 "(4,'root','2026-10-26T22:00:00Z','published','traversal-1','traversal-twin','foreign/receipts/traversal-twin.json','2026-10-26T21:00:00Z','OBSERVED'),"
 "(5,'root','2026-10-26T22:00:00Z','published','stale-1','stale-twin','foreign/receipts/stale-twin.json','2026-10-26T20:00:00Z','OBSERVED');"));sqlite3_close(d);d=NULL;
 CHECK(snprintf(path,sizeof path,"%s/good-twin.json",receipts)<(int)sizeof path);CHECK(snprintf(json,sizeof json,"{\"schema\":\"bonfyre-foreign-observation.v1\",\"twin_id\":\"good-twin\",\"source_url\":\"https://example.test/feed\",\"observe_right\":\"public\",\"policy_decision\":\"allow\",\"observation_kind\":\"source_observed\",\"http_status\":200,\"body_path\":\"/old/foreign/bodies/good.body\",\"body_bytes\":21,\"content_sha256\":\"%s\",\"observed_at\":\"2026-10-26T21:00:00Z\"}\n",digest)<(int)sizeof json);CHECK(write_text(path,json));
 CHECK(snprintf(path,sizeof path,"%s/tampered-twin.json",receipts)<(int)sizeof path);CHECK(snprintf(json,sizeof json,"{\"schema\":\"bonfyre-foreign-observation.v1\",\"twin_id\":\"tampered-twin\",\"source_url\":\"https://example.test/tampered\",\"observe_right\":\"public\",\"policy_decision\":\"allow\",\"observation_kind\":\"source_observed\",\"http_status\":200,\"body_path\":\"/old/foreign/bodies/good.body\",\"body_bytes\":21,\"content_sha256\":\"%064d\",\"observed_at\":\"2026-10-26T21:00:00Z\"}\n",0)<(int)sizeof json);CHECK(write_text(path,json));
 CHECK(snprintf(path,sizeof path,"%s/traversal-twin.json",receipts)<(int)sizeof path);CHECK(snprintf(json,sizeof json,"{\"schema\":\"bonfyre-foreign-observation.v1\",\"twin_id\":\"traversal-twin\",\"source_url\":\"https://example.test/traversal\",\"observe_right\":\"public\",\"policy_decision\":\"allow\",\"observation_kind\":\"source_observed\",\"http_status\":200,\"body_path\":\"../../good.body\",\"body_bytes\":21,\"content_sha256\":\"%s\",\"observed_at\":\"2026-10-26T21:00:00Z\"}\n",digest)<(int)sizeof json);CHECK(write_text(path,json));
 CHECK(snprintf(path,sizeof path,"%s/stale-twin.json",receipts)<(int)sizeof path);CHECK(snprintf(json,sizeof json,"{\"schema\":\"bonfyre-foreign-observation.v1\",\"twin_id\":\"stale-twin\",\"source_url\":\"https://example.test/stale\",\"observe_right\":\"public\",\"policy_decision\":\"allow\",\"observation_kind\":\"source_observed\",\"http_status\":200,\"body_path\":\"/old/foreign/bodies/good.body\",\"body_bytes\":21,\"content_sha256\":\"%s\",\"observed_at\":\"2026-10-26T21:00:00Z\"}\n",digest)<(int)sizeof json);CHECK(write_text(path,json));
 CHECK(run_sync(b,control,cms,root));CHECK(sqlite3_open(control,&d)==SQLITE_OK);CHECK(scalar(d,"SELECT COUNT(*) FROM foreign_twin_observations")==1);CHECK(scalar(d,"SELECT COUNT(*) FROM evidence_relations")==1);CHECK(scalar(d,"SELECT COUNT(*) FROM solved_invariants WHERE truth_plane='measured' AND status='cooled'")==1);CHECK(scalar(d,"SELECT COUNT(*) FROM solved_invariants WHERE truth_plane='proven'")==0);CHECK(scalar(d,"SELECT exact FROM estate_evidence_sync_receipts ORDER BY synced_at DESC LIMIT 1")==1);CHECK(scalar(d,"SELECT unresolved FROM estate_evidence_sync_receipts ORDER BY synced_at DESC LIMIT 1")==4);sqlite3_close(d);d=NULL;
 CHECK(run_sync(b,control,cms,root));CHECK(sqlite3_open(control,&d)==SQLITE_OK);CHECK(scalar(d,"SELECT COUNT(*) FROM foreign_twin_observations")==1);CHECK(scalar(d,"SELECT COUNT(*) FROM evidence_relations")==1);CHECK(scalar(d,"SELECT COUNT(*) FROM solved_invariants")==1);CHECK(scalar(d,"SELECT COUNT(*) FROM estate_evidence_sync_receipts")==1);sqlite3_close(d);
 unlink(control);unlink(cms);const char*names[]={"good-twin.json","tampered-twin.json","traversal-twin.json","stale-twin.json",NULL};for(int i=0;names[i];i++){snprintf(path,sizeof path,"%s/%s",receipts,names[i]);unlink(path);}unlink(body);rmdir(receipts);rmdir(bodies);rmdir(root);puts("estate_evidence_test: PASS");return 0;
}
