/* Exact source-key and live-Parquet cohort audit; read-only owner. */
#ifndef BONFYRE_WAREHOUSE_COHORT_AUDIT_H
#define BONFYRE_WAREHOUSE_COHORT_AUDIT_H
/* Read-only admission audit: never collapse two physical source tables into one
 * semantic/format head. Count live versions separately from source identities. */
static int warehouse_cohorts(sqlite3 *db) {
  sqlite3_int64 source_rows=0,keys=0,collisions=0,colliding_sources=0,live_keys=0,multi_live=0,singular=0;
  const char *live = " FROM source_binding sb "
    "JOIN representation_instance ri ON ri.schema_ref=sb.semantic_kind AND ri.kind='parquet' "
    "JOIN physical_realization pr ON pr.representation_id=ri.id AND pr.available=1 AND pr.digest=ri.digest "
    "JOIN representation_target rt ON rt.semantic_kind=sb.semantic_kind "
    "AND rt.representation_kind='parquet' AND rt.status='physical' "
    "WHERE ri.state='physical' AND ri.digest IS NOT NULL AND ri.digest<>'' AND ri.frontier IS NOT NULL ";
  if (!count_rows(db, "SELECT COUNT(*) FROM source_binding", &source_rows) ||
      !count_rows(db, "SELECT COUNT(*) FROM (SELECT 1 FROM source_binding "
        "GROUP BY semantic_kind)", &keys) ||
      !count_rows(db, "SELECT COUNT(*) FROM (SELECT 1 FROM source_binding "
        "GROUP BY semantic_kind HAVING COUNT(*)>1)", &collisions) ||
      !count_rows(db, "SELECT COALESCE(SUM(n),0) FROM (SELECT COUNT(*) n FROM source_binding "
        "GROUP BY semantic_kind HAVING n>1)", &colliding_sources))
    return 0;
  char sql[2048];
  if (snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM (SELECT 1 %s "
      "GROUP BY sb.semantic_kind)", live) >= (int)sizeof(sql) ||
      !count_rows(db, sql, &live_keys)) return 0;
  if (snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM (SELECT 1 %s "
      "GROUP BY sb.semantic_kind HAVING COUNT(DISTINCT ri.id)>1)", live)
      >= (int)sizeof(sql) || !count_rows(db, sql, &multi_live)) return 0;
  if (!count_rows(db, "SELECT COUNT(*) FROM (SELECT 1 FROM ("
      "SELECT sb.semantic_kind FROM source_binding sb "
      "GROUP BY sb.semantic_kind HAVING COUNT(*)=1) sole "
      "JOIN representation_instance ri "
      "ON ri.schema_ref=sole.semantic_kind AND ri.kind='parquet' "
      "JOIN physical_realization pr ON pr.representation_id=ri.id "
      "AND pr.available=1 AND pr.digest=ri.digest "
      "JOIN representation_target rt ON rt.semantic_kind=sole.semantic_kind "
      "AND rt.representation_kind='parquet' AND rt.status='physical' "
      "WHERE ri.state='physical' AND ri.digest IS NOT NULL AND ri.digest<>'' "
      "AND ri.frontier IS NOT NULL GROUP BY sole.semantic_kind "
      "HAVING COUNT(DISTINCT ri.id)=1)", &singular)) return 0;
  printf("{\"state\":\"SOURCE_KEY_COHORT_AUDIT\",\"source_rows\":%lld,"
    "\"semantic_format_keys\":%lld,\"collision_keys\":%lld,"
    "\"source_rows_in_collision_keys\":%lld,\"keys_with_live_physical\":%lld,"
    "\"keys_with_multiple_live_generations\":%lld,\"unambiguous_single_live_keys\":%lld,"
    "\"safe_to_seed_all\":%s,\"readonly\":true,\"heads_written\":0}\n",
    (long long)source_rows,(long long)keys,(long long)collisions,(long long)colliding_sources,
    (long long)live_keys,(long long)multi_live,(long long)singular,
    (collisions==0 && multi_live==0 && keys==singular) ? "true" : "false");
  return 1;
}
#endif
