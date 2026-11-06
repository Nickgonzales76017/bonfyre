/* Native read-only query stage planning over pinned warehouse metadata.
 * Does not authorize a tenant, reserve resources, schedule workers, or execute SQL. */
#ifndef BONFYRE_WAREHOUSE_QUERY_PLAN_H
#define BONFYRE_WAREHOUSE_QUERY_PLAN_H
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>

typedef struct {
  const char *semantic;
  const char *kind;
  const char *generation;
  const char *bound;
} warehouse_query_input;
typedef struct {
  sqlite3_int64 generation;
  sqlite3_int64 max_bytes;
  char representation_id[241];
  char digest[65];
} warehouse_query_head;

static int warehouse_parse_positive(const char *s, uint64_t *value) {
  if (!s || !*s) return 0;
  for (const char *p=s; *p; p++)
    if (*p < '0' || *p > '9') return 0;
  errno=0;
  unsigned long long n=strtoull(s,NULL,10);
  if(errno || !n || n>INT64_MAX) return 0;
  *value=(uint64_t)n;
  return 1;
}
static int warehouse_pinned_input(sqlite3 *db, warehouse_query_input in,
                                  warehouse_query_head *out, uint64_t *bound) {
  uint64_t g=0,bytes=0;
  if(!key(in.semantic) || !in.kind || strcmp(in.kind,"parquet") ||
     !warehouse_parse_positive(in.generation,&g) ||
     !warehouse_parse_positive(in.bound,&bytes)) return 0;
  const char *sql =
    "SELECT h.generation,h.representation_id,h.digest,s.realization_count,s.max_realization_bytes "
    "FROM warehouse_snapshot_head h "
    "JOIN warehouse_snapshot_stats s ON s.semantic_kind=h.semantic_kind "
    "AND s.representation_kind=h.representation_kind AND s.generation=h.generation "
    "AND s.representation_id=h.representation_id AND s.digest=h.digest "
    "WHERE h.semantic_kind=? AND h.representation_kind='parquet' AND h.generation=? "
    "AND (SELECT COUNT(*) FROM source_binding b WHERE b.semantic_kind=h.semantic_kind)=1 "
    "AND EXISTS(SELECT 1 FROM representation_target rt "
    "WHERE rt.semantic_kind=h.semantic_kind AND rt.representation_kind='parquet' "
    "AND rt.status='physical') "
    "AND EXISTS(SELECT 1 FROM representation_instance ri "
    "WHERE ri.id=h.representation_id AND ri.schema_ref=h.semantic_kind AND ri.kind='parquet' "
    "AND ri.state='physical' AND ri.digest=h.digest AND ri.frontier=h.frontier "
    "AND ri.created_at=h.created_at) "
    "AND s.realization_count=1 "
    "AND (SELECT COUNT(*) FROM physical_realization p "
    "WHERE p.representation_id=h.representation_id AND p.available=1 AND p.digest=h.digest)=1 "
    "AND (SELECT MAX(p.bytes) FROM physical_realization p "
    "WHERE p.representation_id=h.representation_id AND p.available=1 AND p.digest=h.digest)"
    " IS s.max_realization_bytes";
  sqlite3_stmt *st=NULL;
  if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK) return 0;
  sqlite3_bind_text(st,1,in.semantic,-1,SQLITE_TRANSIENT);
  sqlite3_bind_int64(st,2,(sqlite3_int64)g);
  int rc=sqlite3_step(st),ok=0;
  if(rc==SQLITE_ROW && sqlite3_column_type(st,4)!=SQLITE_NULL) {
    const unsigned char *id=sqlite3_column_text(st,1);
    const unsigned char *digest=sqlite3_column_text(st,2);
    sqlite3_int64 observed=sqlite3_column_int64(st,4);
    if(id && digest && strlen((const char*)id)<sizeof(out->representation_id) &&
       strlen((const char*)digest)==64 && observed>0 && (uint64_t)observed<=bytes) {
      strcpy(out->representation_id,(const char*)id);
      strcpy(out->digest,(const char*)digest);
      out->generation=sqlite3_column_int64(st,0);
      out->max_bytes=observed;
      *bound=bytes;
      ok=1;
    }
  }
  sqlite3_finalize(st);
  return ok;
}
static int warehouse_query_plan(sqlite3 *db, const char *operation,
                                warehouse_query_input left,
                                warehouse_query_input right,
                                const char *mem_arg,const char *workers_arg) {
  uint64_t memory=0,workers=0,left_bytes=0,right_bytes=0;
  if(!operation ||
     (strcmp(operation,"scan") && strcmp(operation,"aggregate") && strcmp(operation,"join")) ||
     !warehouse_parse_positive(mem_arg,&memory) ||
     !warehouse_parse_positive(workers_arg,&workers) ||
     memory<1048576 || workers>64) return 0;
  const int join=!strcmp(operation,"join");
  if(join && (!right.semantic || !right.generation || !right.bound)) return 0;
  if(!join && (right.semantic || right.generation || right.bound)) return 0;
  warehouse_query_head lh={0},rh={0};
  if(table_exists(db,"warehouse_snapshot_head")!=1 ||
     table_exists(db,"warehouse_snapshot_stats")!=1 ||
     !warehouse_pinned_input(db,left,&lh,&left_bytes)) return 0;
  if(join && !warehouse_pinned_input(db,right,&rh,&right_bytes)) return 0;
  if(join && left_bytes>UINT64_MAX-right_bytes) return 0;
  uint64_t total=left_bytes+right_bytes;
  uint64_t budget=memory/4;
  if(budget==0 || workers>UINT64_MAX/budget) return 0;
  uint64_t aggregate=budget*workers;
  const char *decision=total<=budget?"LOCAL_CANDIDATE":
    (workers>1 && total<=aggregate)?"DISTRIBUTED_CANDIDATE":"RESOURCE_REFUSED";
  printf("{\"schema\":\"bonfyre.warehouse.query-plan.v1\","
    "\"decision\":");json_string(decision);
  printf(",\"operation\":");json_string(operation);
  printf(",\"input_bound_bytes\":%" PRIu64 ",\"per_worker_memory_bytes\":%" PRIu64
    ",\"worker_count\":%" PRIu64 ",\"stage_memory_cap_bytes\":%" PRIu64
    ",\"aggregate_stage_budget_bytes\":%" PRIu64,
    total,memory,workers,budget,aggregate);
  printf(",\"left\":{\"semantic_kind\":");json_string(left.semantic);
  printf(",\"generation\":%lld,\"representation_id\":",(long long)lh.generation);
  json_string(lh.representation_id);printf(",\"digest\":");json_string(lh.digest);
  printf(",\"max_observed_realization_bytes\":%lld}",(long long)lh.max_bytes);
  if(join){
    printf(",\"right\":{\"semantic_kind\":");json_string(right.semantic);
    printf(",\"generation\":%lld,\"representation_id\":",(long long)rh.generation);
    json_string(rh.representation_id);printf(",\"digest\":");json_string(rh.digest);
    printf(",\"max_observed_realization_bytes\":%lld}",(long long)rh.max_bytes);
  }
  printf(",\"proposed_route\":");
  json_string(total<=budget?"local-duckdb-candidate":(workers>1 && total<=aggregate)?
    "distributed-exchange-candidate":"none");
  printf(",\"proposed_stages\":[");
  if(total<=budget) printf("\"local-scan\",\"local-finalize\"");
  else if(workers>1 && total<=aggregate) {
    if(join) printf("\"bounded-scan\",\"partition-exchange\",\"join-stage\",\"result-merge\"");
    else if(!strcmp(operation,"aggregate"))
      printf("\"bounded-scan\",\"partial-aggregate\",\"partition-exchange\",\"final-aggregate\"");
    else printf("\"bounded-scan\",\"partition-exchange\",\"result-merge\"");
  }
  puts("],\"actor_closure_verified\":false,\"packgraph_verified\":false,"
       "\"scheduler_admitted\":false,\"exchange_executed\":false,"
       "\"worker_recovery_verified\":false,\"physical_bytes_verified\":false}");
  return 1;
}
#endif
