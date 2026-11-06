#define _POSIX_C_SOURCE 200809L
#include "model_catalogue_index.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int contains(const char *html,const char *needle) {
  if(strstr(html,needle))return 1;
  fprintf(stderr,"missing model index fragment: %s\n",needle);return 0;
}
int main(void){
  char path[]="/tmp/bonfyre-model-index-XXXXXX",html[16384],detail[262144],label[320];size_t used=42;
  sqlite3 *db=NULL;int ok=1,fd=mkstemp(path);
  if(fd<0)return 1;close(fd);
  if(sqlite3_open(path,&db)!=SQLITE_OK)return 1;
  if(sqlite3_exec(db,
    "CREATE TABLE catalog_nodes(node_id TEXT PRIMARY KEY,kind TEXT,external_id TEXT,name TEXT,json_data TEXT,summary TEXT DEFAULT '',category TEXT DEFAULT '',source_hash TEXT DEFAULT '');"
    "CREATE TABLE catalog_edges(src_node_id TEXT,dst_node_id TEXT,rel TEXT);"
    "INSERT INTO catalog_nodes(node_id,kind,external_id,name,json_data) VALUES('m1','model','test-model','Test & <Safe>','{\"format\":\"GGUF\",\"size_mb\":150}');"
    "INSERT INTO catalog_nodes(node_id,kind,external_id,name,json_data) VALUES('c1','capability','cap','Capability','{}');"
    "INSERT INTO catalog_edges VALUES('m1','c1','powers_capability');"
    "INSERT INTO catalog_edges VALUES('m1','missing','uses_model');",NULL,NULL,NULL)!=SQLITE_OK)return 1;
  sqlite3_close(db);db=NULL;
  ok&=(bf_model_catalogue_index(path,html,sizeof(html),&used)==1);
  ok&=used==strlen(html);
  ok&=contains(html,"Registered models");
  ok&=contains(html,"Test &amp; &lt;Safe&gt;");
  ok&=contains(html,"/s/model/test-model");
  ok&=contains(html,"<dt>Relations</dt><dd>1</dd>");
  ok&=contains(html,"<dt>Unresolved</dt><dd>1</dd>");
  ok&=contains(html,"powers 1 &middot; used by 0");
  ok&=contains(html,"data-model-coverage");
  ok&=contains(html,"class=\"desk-row model-card\"");
  ok&=contains(html,"class=\"model-mark\"><svg class=\"glyph ak-icon\"");
  ok&=contains(html,"aria-hidden=\"true\"");
  ok&=(bf_model_catalogue_detail(path,"test-model",label,sizeof(label),detail,sizeof(detail),&used)==1);
  ok&=contains(label,"Test & <Safe>");
  ok&=contains(detail,"<dt>Format</dt><dd>GGUF");
  ok&=contains(detail,"<dt>Size</dt><dd>150 MB");
  ok&=contains(detail,"References the catalogue cannot follow");
  ok&=contains(detail,"Measured quality");
  if(sqlite3_open(path,&db)!=SQLITE_OK)return 1;
  if(sqlite3_exec(db,
      "INSERT INTO catalog_nodes(node_id,kind,external_id,name,json_data) VALUES('s1','model_source','private','file:///Users/private/model.gguf','{}'),('s2','model_source','public','hf://example/model.gguf','{}'),('s3','model_source','mixed','FILE:///Users/private/upper.gguf','{}');"
      "INSERT INTO catalog_edges VALUES('m1','s1','has_source'),('m1','s2','has_source'),('m1','s3','has_source');",
      NULL,NULL,NULL)!=SQLITE_OK)return 1;
  sqlite3_close(db);db=NULL;
  ok&=(bf_model_catalogue_detail(path,"test-model",label,sizeof(label),detail,sizeof(detail),&used)==1);
  ok&=contains(detail,"Local file");
  ok&=contains(detail,"hf://example/model.gguf");
  if(strstr(detail,"/Users/private/") || strstr(detail,"file:") || strstr(detail,"FILE:")){
    fprintf(stderr,"local model path leaked into public page\n");ok=0;
  }
  // A registered format is source data, not trusted page markup.
  if(sqlite3_open(path,&db)!=SQLITE_OK)return 1;
  if(sqlite3_exec(db,
      "UPDATE catalog_nodes SET json_data='{\"format\":\"<script>alert(1)</script>\",\"size_mb\":150}' WHERE node_id='m1'",
      NULL,NULL,NULL)!=SQLITE_OK)return 1;
  sqlite3_close(db);db=NULL;
  ok&=(bf_model_catalogue_detail(path,"test-model",label,sizeof(label),detail,sizeof(detail),&used)==1);
  ok&=contains(detail,"&lt;script&gt;alert(1)&lt;/script&gt;");
  if(strstr(detail,"<script>alert(1)</script>")){fprintf(stderr,"raw format markup escaped incorrectly\n");ok=0;}
  ok&=(used==strlen(detail));
  ok&=(bf_model_catalogue_detail(path,"unlisted",label,sizeof(label),detail,sizeof(detail),&used)==0);
  ok&=(bf_model_catalogue_detail(path,"test-model",label,sizeof(label),detail,8,&used)==-1);
  ok&=(bf_model_catalogue_index(path,html,4,&used)==-1 && used==0);
  ok&=(bf_model_catalogue_index("/no/such/model-catalogue",html,sizeof(html),&used)==-1 && used==0);
  if(sqlite3_open(path,&db)!=SQLITE_OK)return 1;
  // A truncated catalogue must fail closed, never publish a partial list as complete.
  if(sqlite3_exec(db,
      "INSERT INTO catalog_nodes(node_id,kind,external_id,name,json_data) VALUES('m2','model','second-model','Another Model','{\"format\":\"GGUF\"}');",
      NULL,NULL,NULL)!=SQLITE_OK)return 1;
  ok&=(bf_model_catalogue_index(path,html,2100,&used)==-1 && used==0);
    if(sqlite3_exec(db,"DELETE FROM catalog_nodes WHERE kind='model'",NULL,NULL,NULL)!=SQLITE_OK)return 1;
  sqlite3_close(db);db=NULL;
  ok&=(bf_model_catalogue_index(path,html,sizeof(html),&used)==0);
  unlink(path);
  if(ok)puts("model catalogue index: PASS");
  return ok?0:1;
}
