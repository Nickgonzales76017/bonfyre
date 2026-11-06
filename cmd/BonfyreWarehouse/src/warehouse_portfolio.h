/* A bounded, read-only data-product listing for Aurekai.
 * Only generation-pinned metadata leaves the Representation Manifold.
 * Neither body paths nor tenant authority are exposed. */
#ifndef BONFYRE_WAREHOUSE_PORTFOLIO_H
#define BONFYRE_WAREHOUSE_PORTFOLIO_H
static int warehouse_portfolio(sqlite3 *db,const char *offset_arg,const char *limit_arg) {
  uint64_t offset=0,limit=0;
  if(!offset_arg||!limit_arg||!warehouse_parse_positive(limit_arg,&limit)||
     limit>100) return 0;
  if(strcmp(offset_arg,"0")&& !warehouse_parse_positive(offset_arg,&offset)) return 0;
  if(offset>100000000) return 0;
  const char *tables[]={"source_binding","warehouse_snapshot_head",
    "warehouse_snapshot_stats","representation_instance",
    "physical_realization","representation_target"};
  for(size_t i=0;i<sizeof(tables)/sizeof(tables[0]);i++)
    if(table_exists(db,tables[i])!=1) {
      puts("{\"schema\":\"bonfyre.warehouse.portfolio.v1\",\"state\":\"NOT_INSTALLED\","
        "\"rows\":[],\"next_offset\":null,\"read_only\":true,"
        "\"execution_authority\":false,\"physical_bytes_verified\":false,"
        "\"tenant_access_verified\":false}");
      return 1;
    }
  sqlite3_int64 total=0;
  if(!count_rows(db,"SELECT COUNT(*) FROM source_binding",&total))return 0;
  const char *sql=
    "SELECT sb.semantic_kind,h.generation,h.representation_id,h.digest,"
    "s.realization_count,s.max_realization_bytes,"
    "CASE WHEN h.representation_id IS NOT NULL "
    "AND (SELECT COUNT(*) FROM source_binding b WHERE b.semantic_kind=sb.semantic_kind)=1 "
    "AND EXISTS(SELECT 1 FROM representation_target rt WHERE "
    "rt.semantic_kind=sb.semantic_kind AND rt.representation_kind='parquet' AND rt.status='physical') "
    "AND EXISTS(SELECT 1 FROM representation_instance ri WHERE "
    "ri.id=h.representation_id AND ri.schema_ref=h.semantic_kind AND ri.kind='parquet' "
    "AND ri.state='physical' AND ri.digest=h.digest AND ri.frontier=h.frontier AND ri.created_at=h.created_at) "
    "AND s.generation=h.generation AND s.representation_id=h.representation_id "
    "AND s.digest=h.digest AND s.realization_count>0 "
    "AND (SELECT COUNT(*) FROM physical_realization pr WHERE "
    "pr.representation_id=h.representation_id AND pr.available=1 AND pr.digest=h.digest)"
    "=s.realization_count "
    "AND (SELECT MAX(pr.bytes) FROM physical_realization pr "
    "WHERE pr.representation_id=h.representation_id AND pr.available=1 AND pr.digest=h.digest)"
    " IS s.max_realization_bytes THEN 1 ELSE 0 END valid "
    "FROM source_binding sb "
    "LEFT JOIN warehouse_snapshot_head h ON h.semantic_kind=sb.semantic_kind AND h.representation_kind='parquet' "
    "LEFT JOIN warehouse_snapshot_stats s ON s.semantic_kind=h.semantic_kind AND s.representation_kind='parquet' "
    "ORDER BY sb.semantic_kind COLLATE BINARY LIMIT ? OFFSET ?";
  sqlite3_stmt *st=NULL;
  if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return 0;
  sqlite3_bind_int64(st,1,(sqlite3_int64)limit);
  sqlite3_bind_int64(st,2,(sqlite3_int64)offset);
  printf("{\"schema\":\"bonfyre.warehouse.portfolio.v1\","
    "\"state\":\"READ_ONLY\",\"total\":%lld,\"offset\":%" PRIu64
    ",\"limit\":%" PRIu64 ",\"rows\":[",(long long)total,offset,limit);
  int first=1,rc=SQLITE_DONE;
  while((rc=sqlite3_step(st))==SQLITE_ROW) {
    const char *semantic=(const char*)sqlite3_column_text(st,0);
    if(!semantic) {rc=SQLITE_ERROR;break;}
    if(!first)putchar(',');first=0;
    printf("{\"semantic_kind\":");json_string(semantic);
    const int valid=sqlite3_column_int(st,6)==1;
    printf(",\"state\":\"%s\"",valid?"PINNED_METADATA":"UNAVAILABLE");
    if(valid){
      const char *id=(const char*)sqlite3_column_text(st,2);
      const char *digest=(const char*)sqlite3_column_text(st,3);
      if(!id||!digest){rc=SQLITE_ERROR;break;}
      printf(",\"representation_kind\":\"parquet\",\"generation\":%lld,"
        "\"representation_id\":",(long long)sqlite3_column_int64(st,1));
      json_string(id);printf(",\"digest\":");json_string(digest);
      printf(",\"realization_count\":%lld,\"max_realization_bytes\":%lld",
        (long long)sqlite3_column_int64(st,4),(long long)sqlite3_column_int64(st,5));
    }
    putchar('}');
  }
  sqlite3_finalize(st);
  if(rc!=SQLITE_DONE)return 0;
  const uint64_t next=offset+(uint64_t)(total>0?total:0);
  (void)next;
  printf("],\"next_offset\":");
  if(offset+limit<(uint64_t)total)printf("%" PRIu64,offset+limit);
  else printf("null");
  puts(",\"read_only\":true,\"execution_authority\":false,"
    "\"physical_bytes_verified\":false,\"tenant_access_verified\":false}");
  return 1;
}
#endif
