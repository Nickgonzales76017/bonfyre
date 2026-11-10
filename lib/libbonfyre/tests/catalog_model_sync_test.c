#define _DARWIN_C_SOURCE 1
#define _POSIX_C_SOURCE 200809L
#include <bonfyre.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #x); exit(1); } } while (0)
static void sql(sqlite3 *db, const char *s) { char *e = NULL; if (sqlite3_exec(db,s,NULL,NULL,&e) != SQLITE_OK) { fprintf(stderr,"SQL: %s\n", e); sqlite3_free(e); exit(1); } }
static sqlite3 *open_db(const char *path) { sqlite3 *db = NULL; CHECK(sqlite3_open(path,&db)==SQLITE_OK); return db; }
static int count(sqlite3 *db, const char *s) { sqlite3_stmt *st=NULL; CHECK(sqlite3_prepare_v2(db,s,-1,&st,NULL)==SQLITE_OK); CHECK(sqlite3_step(st)==SQLITE_ROW); int n=sqlite3_column_int(st,0); sqlite3_finalize(st); return n; }
static void graph(sqlite3 *db) {
    CHECK(count(db,"SELECT count(*) FROM catalog_nodes WHERE node_id IN ('model:m','model-source:m:0001','representation:r')") == 3);
    CHECK(count(db,"SELECT count(*) FROM catalog_edges WHERE rel IN ('has_representation','represents_model','has_source','sources_model')") == 4);
    CHECK(count(db,"SELECT count(*) FROM catalog_edges WHERE (src_node_id='capability:cap' AND rel='uses_model' AND dst_node_id='model:m') OR (src_node_id='model:m' AND rel='powers_capability' AND dst_node_id='capability:cap')") == 2);
}
int main(void) {
    char tmp[]="/tmp/bf-catalog-test-XXXXXX", model[512], cat[512], bad[512], cap[512];
    CHECK(mkdtemp(tmp));
    snprintf(model,sizeof model,"%s/models.db",tmp); snprintf(cat,sizeof cat,"%s/catalog.db",tmp); snprintf(bad,sizeof bad,"%s/away.db",tmp);
    char dir[512];
    const char *dirs[]={"docs", "docs/recipes", "recipes", "recipes/google", "recipes/topology", "recipes/cross_fusion", "recipes/families", NULL};
    for (int i=0; dirs[i]; i++) { snprintf(dir,sizeof dir,"%s/%s",tmp,dirs[i]); CHECK(mkdir(dir,0700)==0); }
    snprintf(dir,sizeof dir,"%s/recipes/families/family_index.json",tmp);
    FILE *fixture=fopen(dir,"w"); CHECK(fixture); CHECK(fputs("{}",fixture)>=0); CHECK(fclose(fixture)==0);
    CHECK(setenv("HOME",tmp,1)==0); CHECK(setenv("BONFYRE_MODEL_DB",model,1)==0);
    CHECK(setenv("BONFYRE_CAPABILITY_DB", "/nonexistent/bf-capability",1)==0);
    CHECK(setenv("BONFYRE_LAYER_DB", "/nonexistent/bf-layer",1)==0);
    CHECK(bf_catalog_sync_repo(cat,tmp)==0); /* no registry bootstrap */
    sqlite3 *c=open_db(cat);
    char stale_yaml[512], current_yaml[512];
    snprintf(stale_yaml,sizeof stale_yaml,"%s/recipes/google/stale.yaml",tmp);
    snprintf(current_yaml,sizeof current_yaml,"%s/recipes/google/current.yaml",tmp);
    fixture=fopen(stale_yaml,"w"); CHECK(fixture); CHECK(fputs("recipe: stale\nsource_model: old\n",fixture)>=0); CHECK(fclose(fixture)==0);
    fixture=fopen(current_yaml,"w"); CHECK(fixture); CHECK(fputs("recipe: current\nsource_model: retained\n",fixture)>=0); CHECK(fclose(fixture)==0);
    CHECK(bf_catalog_sync_repo(cat,tmp)==0);
    CHECK(count(c,"SELECT count(*) FROM catalog_nodes WHERE node_id IN ('model:old','model:retained')")==2);
    CHECK(unlink(stale_yaml)==0);
    sql(c,"CREATE TRIGGER reject_bootstrap_delete BEFORE DELETE ON catalog_nodes WHEN OLD.node_id='model:old' BEGIN SELECT RAISE(ABORT,'refused'); END");
    CHECK(bf_catalog_sync_repo(cat,tmp)!=0);
    CHECK(count(c,"SELECT count(*) FROM catalog_nodes WHERE node_id IN ('model:old','model:retained')")==2);
    CHECK(count(c,"SELECT count(*) FROM catalog_edges WHERE src_node_id='recipe:stale' AND rel='uses_model' AND dst_node_id='model:old'")==1);
    sql(c,"DROP TRIGGER reject_bootstrap_delete");
    CHECK(bf_catalog_sync_repo(cat,tmp)==0);
    CHECK(count(c,"SELECT count(*) FROM catalog_nodes WHERE node_id='model:old'")==0);
    CHECK(count(c,"SELECT count(*) FROM catalog_nodes WHERE node_id='model:retained'")==1);
    CHECK(count(c,"SELECT count(*) FROM catalog_edges WHERE src_node_id='recipe:current' AND rel='uses_model' AND dst_node_id='model:retained'")==1);
    sql(c,"INSERT INTO catalog_nodes(node_id,kind,external_id,name,source_path,updated_at) VALUES('model:unknown','model','unknown','unknown',NULL,0)");
    CHECK(bf_catalog_sync_repo(cat,tmp)!=0);
    CHECK(count(c,"SELECT count(*) FROM catalog_nodes WHERE node_id IN ('model:unknown','model:retained')")==2);
    sql(c,"DELETE FROM catalog_nodes WHERE node_id='model:unknown'");
    CHECK(unlink(current_yaml)==0);
    CHECK(bf_catalog_sync_repo(cat,tmp)==0);
    CHECK(count(c,"SELECT count(*) FROM catalog_nodes WHERE node_id='model:retained'")==0);
    snprintf(cap,sizeof cap,"%s/capability.db",tmp); sqlite3 *capdb=open_db(cap);
    sql(capdb,"CREATE TABLE capabilities(id,name,description,binary,command,model_id,hardware_tier,cost_estimate,latency_tier,stage_class,artifact_out,keywords,source); INSERT INTO capabilities VALUES('cap','Capability','','','','m','',0,'','','','','');");
    sqlite3_close(capdb); CHECK(setenv("BONFYRE_CAPABILITY_DB",cap,1)==0);
    sqlite3 *r=open_db(model);
    sql(r,"CREATE TABLE models(id,name,description,format,sha256,size_mb,fpq_sha256,fpq_size_mb,transform_family,geometry,geometry_condition,layer_frag_spec,mean_f1);"
          "CREATE TABLE sources(model_id,url,priority); CREATE TABLE recipe_models(recipe_code,model_id,role);"
          "INSERT INTO models VALUES('m','Model','desc','bin','sha',1,'',0,'','','','',0);"
          "INSERT INTO sources VALUES('m','https://example.test/model',1);");
    CHECK(bf_catalog_sync_repo(cat,tmp)==0); /* old registry, no passports */
    sql(r,"CREATE TABLE model_representation_passport(representation_id,model_id,representation_family,representation_version,exactness_class,measurement_state,admission_state,admission_scope,quality_envelope,receipt_ref,receipt_digest,physical_body_ref,physical_digest,physical_bytes,supports_json,recorded_at);"
          "INSERT INTO model_representation_passport VALUES('r','m','native','1','exact','measured','DEFER','generation','q','receipt','digest','body','hash',7,'[]','today');");
    CHECK(bf_catalog_sync_repo(cat,tmp)==0);
    graph(c);
    sqlite3_close(r);
    CHECK(rename(model,bad)==0); CHECK(bf_catalog_sync_repo(cat,tmp)!=0); graph(c);
    FILE *f=fopen(model,"w"); CHECK(f); CHECK(fputs("not a sqlite database",f)>=0); CHECK(fclose(f)==0);
    CHECK(bf_catalog_sync_repo(cat,tmp)!=0); graph(c); CHECK(unlink(model)==0); CHECK(rename(bad,model)==0);
    r=open_db(model); sql(r,"ALTER TABLE model_representation_passport RENAME TO passport_backup");
    CHECK(bf_catalog_sync_repo(cat,tmp)!=0); graph(c);
    sql(r,"CREATE TABLE model_representation_passport(representation_id TEXT)");
    CHECK(bf_catalog_sync_repo(cat,tmp)!=0); graph(c);
    sql(r,"DROP TABLE model_representation_passport; ALTER TABLE passport_backup RENAME TO model_representation_passport;");
    sql(r,"ALTER TABLE sources RENAME TO source_backup; CREATE TABLE sources(model_id)");
    CHECK(bf_catalog_sync_repo(cat,tmp)!=0); graph(c);
    sql(r,"DROP TABLE sources; ALTER TABLE source_backup RENAME TO sources");
    sql(r,"UPDATE model_representation_passport SET supports_json='invalid'");
    CHECK(bf_catalog_sync_repo(cat,tmp)!=0); graph(c);
    sql(r,"UPDATE model_representation_passport SET supports_json='[]'");
    sql(r,"UPDATE models SET id=''");
    CHECK(bf_catalog_sync_repo(cat,tmp)!=0); graph(c);
    sql(r,"UPDATE models SET id='m'");
    sql(c,"CREATE TRIGGER reject_rep BEFORE INSERT ON catalog_edges WHEN NEW.rel='represents_model' BEGIN SELECT RAISE(ABORT,'refused'); END");
    CHECK(bf_catalog_sync_repo(cat,tmp)!=0); graph(c); sql(c,"DROP TRIGGER reject_rep");
    sql(c,"CREATE TRIGGER reject_delete BEFORE DELETE ON catalog_nodes WHEN OLD.node_id='model:m' BEGIN SELECT RAISE(ABORT,'refused'); END");
    CHECK(bf_catalog_sync_repo(cat,tmp)!=0); graph(c); sql(c,"DROP TRIGGER reject_delete");
    sql(r,"DELETE FROM model_representation_passport; DELETE FROM sources; DELETE FROM models;");
    CHECK(bf_catalog_sync_repo(cat,tmp)==0); CHECK(count(c,"SELECT count(*) FROM catalog_nodes WHERE node_id IN ('model:m','model-source:m:0001','representation:r')")==0);
    sqlite3_close(r); sqlite3_close(c);
    printf("catalog model sync: PASS (%s)\n",tmp); return 0;
}
