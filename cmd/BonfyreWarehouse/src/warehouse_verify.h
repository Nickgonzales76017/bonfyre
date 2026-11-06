/* Independent read-only acceptance of generated warehouse metadata.
 * Never proves physical bytes, distributed execution, or effect authority. */
#ifndef BONFYRE_WAREHOUSE_VERIFY_H
#define BONFYRE_WAREHOUSE_VERIFY_H
static int warehouse_verify(sqlite3 *db) {
  sqlite3_int64 sources=0,heads=0,stats=0,invalid=0,indexes=0;
  const char *required[]={"source_binding","representation_instance","physical_realization",
    "representation_target","warehouse_snapshot_head","warehouse_snapshot_stats"};
  for(size_t i=0;i<sizeof(required)/sizeof(required[0]);i++)
    if(table_exists(db,required[i])!=1) {
      puts("{\"state\":\"NOT_INSTALLED\",\"verified\":false,\"readonly\":true}");
      return 1;
    }
  const char *bad =
    "SELECT COUNT(*) FROM warehouse_snapshot_head h "
    "LEFT JOIN warehouse_snapshot_stats s ON s.semantic_kind=h.semantic_kind "
    "AND s.representation_kind=h.representation_kind "
    "WHERE h.representation_kind!='parquet' OR h.generation<1 "
    "OR (SELECT COUNT(*) FROM source_binding b WHERE b.semantic_kind=h.semantic_kind)!=1 "
    "OR NOT EXISTS(SELECT 1 FROM representation_target rt "
    "WHERE rt.semantic_kind=h.semantic_kind AND rt.representation_kind=h.representation_kind "
    "AND rt.status='physical') "
    "OR NOT EXISTS(SELECT 1 FROM representation_instance ri "
    "WHERE ri.id=h.representation_id AND ri.schema_ref=h.semantic_kind "
    "AND ri.kind=h.representation_kind AND ri.state='physical' "
    "AND ri.digest=h.digest AND ri.frontier=h.frontier AND ri.created_at=h.created_at) "
    "OR (SELECT COUNT(DISTINCT ri.id) FROM representation_instance ri "
    "JOIN physical_realization p ON p.representation_id=ri.id AND p.available=1 "
    "AND p.digest=ri.digest WHERE ri.schema_ref=h.semantic_kind "
    "AND ri.kind=h.representation_kind AND ri.state='physical')!=1 "
    "OR s.representation_id IS NULL OR s.generation!=h.generation "
    "OR s.representation_id!=h.representation_id OR s.digest!=h.digest "
    "OR (SELECT COUNT(*) FROM physical_realization p "
    "WHERE p.representation_id=h.representation_id AND p.available=1 AND p.digest=h.digest)"
    "!=s.realization_count "
    "OR (SELECT MAX(p.bytes) FROM physical_realization p "
    "WHERE p.representation_id=h.representation_id AND p.available=1 AND p.digest=h.digest)"
    " IS NOT s.max_realization_bytes";
  if(!count_rows(db,"SELECT COUNT(*) FROM source_binding",&sources) ||
     !count_rows(db,"SELECT COUNT(*) FROM warehouse_snapshot_head",&heads) ||
     !count_rows(db,"SELECT COUNT(*) FROM warehouse_snapshot_stats",&stats) ||
     !count_rows(db,bad,&invalid) ||
     !count_rows(db,"SELECT COUNT(*) FROM sqlite_master WHERE type='index' "
       "AND name IN ('idx_warehouse_ri_latest','idx_warehouse_pr_live','idx_warehouse_sb_kind')",&indexes))
    return 0;
  sqlite3_stmt *quick=NULL;
  if(sqlite3_prepare_v2(db,"PRAGMA quick_check(1)",-1,&quick,NULL)!=SQLITE_OK) return 0;
  const int rc=sqlite3_step(quick);
  const unsigned char *answer=rc==SQLITE_ROW?sqlite3_column_text(quick,0):NULL;
  const int sqlite_ok=answer && strcmp((const char*)answer,"ok")==0;
  sqlite3_finalize(quick);
  const int verified=sources>0 && sources==heads && heads==stats && invalid==0 &&
    indexes==3 && sqlite_ok;
  printf("{\"state\":\"%s\",\"verified\":%s,\"readonly\":true,"
    "\"source_keys\":%lld,\"heads\":%lld,\"statistics\":%lld,"
    "\"invalid_heads\":%lld,\"indexes_present\":%lld,\"sqlite_quick_check\":%s,"
    "\"physical_bytes_verified\":false,\"distributed_execution_verified\":false}\n",
    verified?"WAREHOUSE_METADATA_VERIFIED":"RECOVERY_REFUSED",
    verified?"true":"false",(long long)sources,(long long)heads,
    (long long)stats,(long long)invalid,(long long)indexes,sqlite_ok?"true":"false");
  return 1;
}
#endif
