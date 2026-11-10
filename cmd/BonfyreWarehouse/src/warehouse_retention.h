/* Maintenance-only metadata pressure census. This cannot authorize GC or delete.
 * Selection is conservative; every record remains semantically retained. */
#ifndef BONFYRE_WAREHOUSE_RETENTION_H
#define BONFYRE_WAREHOUSE_RETENTION_H
static int warehouse_retention_date(const char *s) {
  const char *shape="dddd-dd-ddTdd:dd:ddZ";
  if(!s || strlen(s)!=20) return 0;
  for(size_t i=0;i<20;i++) {
    if(shape[i]=='d') {
      if(s[i]<'0'||s[i]>'9') return 0;
    } else if(s[i]!=shape[i]) return 0;
  }
  const int year=(s[0]-'0')*1000+(s[1]-'0')*100+(s[2]-'0')*10+(s[3]-'0');
  const int month=(s[5]-'0')*10+(s[6]-'0');
  const int day=(s[8]-'0')*10+(s[9]-'0');
  const int hour=(s[11]-'0')*10+(s[12]-'0');
  const int minute=(s[14]-'0')*10+(s[15]-'0');
  const int second=(s[17]-'0')*10+(s[18]-'0');
  if(year<1 || month<1 || month>12 || hour>23 || minute>59 || second>59) return 0;
  const int mdays[]={0,31,28,31,30,31,30,31,31,30,31,30,31};
  const int leap=(year%4==0 && (year%100!=0 || year%400==0));
  const int maximum=mdays[month]+(month==2 && leap);
  return day>=1 && day<=maximum;
}
static int warehouse_retention_count(sqlite3 *db,const char *sql,const char *cutoff,
                                      sqlite3_int64 *value) {
  sqlite3_stmt *stmt=NULL;
  if(sqlite3_prepare_v2(db,sql,-1,&stmt,NULL)!=SQLITE_OK) return 0;
  sqlite3_bind_text(stmt,1,cutoff,-1,SQLITE_TRANSIENT);
  const int rc=sqlite3_step(stmt);
  if(rc==SQLITE_ROW) *value=sqlite3_column_int64(stmt,0);
  sqlite3_finalize(stmt);
  return rc==SQLITE_ROW;
}
static int warehouse_retention(sqlite3 *db,const char *cutoff) {
  if(!warehouse_retention_date(cutoff)) return 0;
  const char *tables[]={"source_binding","representation_instance","physical_realization",
    "representation_edge","eviction_receipt"};
  for(size_t i=0;i<sizeof(tables)/sizeof(tables[0]);i++)
    if(table_exists(db,tables[i])!=1) return 0;
  sqlite3_int64 total=0,pinned=0,unbound=0,old_empty=0,
    candidate=0,receipts=0,freelist=0,page_size=0;
  if(!count_rows(db,"SELECT COUNT(*) FROM representation_instance",&total) ||
     !count_rows(db,"SELECT COUNT(*) FROM eviction_receipt",&receipts) ||
     !count_rows(db,"PRAGMA freelist_count",&freelist) ||
     !count_rows(db,"PRAGMA page_size",&page_size) ||
     !count_rows(db,"SELECT COUNT(*) FROM representation_instance ri "
       "WHERE NOT EXISTS(SELECT 1 FROM source_binding sb WHERE sb.semantic_kind=ri.schema_ref)",
       &unbound)) return 0;
  if(table_exists(db,"warehouse_snapshot_head")==1 &&
     !count_rows(db,"SELECT COUNT(*) FROM warehouse_snapshot_head",&pinned)) return 0;
  const char *no_body =
    "SELECT COUNT(*) FROM representation_instance ri "
    "WHERE ri.created_at<? AND ri.reconstructable=1 AND ri.state='reconstructable' "
    "AND NOT EXISTS(SELECT 1 FROM physical_realization p "
    "WHERE p.representation_id=ri.id AND p.available=1)";
  const char *unreferenced =
    "SELECT COUNT(*) FROM representation_instance ri "
    "WHERE ri.created_at<? AND ri.authority='derived' "
    "AND ri.reconstructable=1 AND ri.state='reconstructable' "
    "AND NOT EXISTS(SELECT 1 FROM source_binding sb WHERE sb.semantic_kind=ri.schema_ref) "
    "AND NOT EXISTS(SELECT 1 FROM physical_realization p WHERE p.representation_id=ri.id) "
    "AND NOT EXISTS(SELECT 1 FROM representation_edge e "
    "WHERE e.parent_representation_id=ri.id OR e.child_representation_id=ri.id) "
    "AND NOT EXISTS(SELECT 1 FROM eviction_receipt er WHERE er.representation_id=ri.id) "
    "AND NOT EXISTS(SELECT 1 FROM warehouse_snapshot_head h WHERE h.representation_id=ri.id)";
  /* Old estates may not have warehouse_snapshot_head yet. Never query a
   * nonexistent table; this audit must work before the migration. */
  const char *unreferenced_before_install =
    "SELECT COUNT(*) FROM representation_instance ri "
    "WHERE ri.created_at<? AND ri.authority='derived' "
    "AND ri.reconstructable=1 AND ri.state='reconstructable' "
    "AND NOT EXISTS(SELECT 1 FROM source_binding sb WHERE sb.semantic_kind=ri.schema_ref) "
    "AND NOT EXISTS(SELECT 1 FROM physical_realization p WHERE p.representation_id=ri.id) "
    "AND NOT EXISTS(SELECT 1 FROM representation_edge e "
    "WHERE e.parent_representation_id=ri.id OR e.child_representation_id=ri.id) "
    "AND NOT EXISTS(SELECT 1 FROM eviction_receipt er WHERE er.representation_id=ri.id)";
  if(!warehouse_retention_count(db,no_body,cutoff,&old_empty) ||
     !warehouse_retention_count(db,table_exists(db,"warehouse_snapshot_head")==1?
       unreferenced:unreferenced_before_install,cutoff,&candidate)) return 0;
  if(freelist<0||page_size<=0||freelist>INT64_MAX/page_size) return 0;
  printf("{\"schema\":\"bonfyre.warehouse.metadata-pressure.v1\","
    "\"state\":\"RETAINED_METADATA_CENSUS\",\"cutoff\":");
  json_string(cutoff);
  printf(",\"representation_instances\":%lld,\"pinned_heads\":%lld,"
    "\"instances_without_current_source\":%lld,"
    "\"old_reconstructable_without_available_body\":%lld,"
    "\"unreferenced_history_candidates\":%lld,\"eviction_receipts\":%lld,"
    "\"sqlite_freelist_bytes\":%lld,\"deleted\":0,"
    "\"metadata_only\":true,\"gc_authority\":false,\"candidate_safe_to_delete\":false}\n",
    (long long)total,(long long)pinned,(long long)unbound,(long long)old_empty,
    (long long)candidate,(long long)receipts,(long long)(freelist*page_size));
  return 1;
}
#endif
