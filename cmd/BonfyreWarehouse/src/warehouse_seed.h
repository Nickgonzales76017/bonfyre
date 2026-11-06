/* One-transaction derived metadata admission in the existing manifold database.
 * Only the local native warehouse command may call this explicit seed boundary.
 * No physical bytes are opened, rewritten, or collected. */
#ifndef BONFYRE_WAREHOUSE_SEED_H
#define BONFYRE_WAREHOUSE_SEED_H
#include <ctype.h>
#include <errno.h>
#include <time.h>
static int warehouse_rows_bound(sqlite3_stmt *s, int i, const char *frontier) {
  const char *p = frontier ? strstr(frontier, ";rows:") : NULL;
  if (!p) return sqlite3_bind_null(s, i) == SQLITE_OK;
  p += 6;
  const char *end = p;
  while (isdigit((unsigned char)*end)) end++;
  if (p == end || (*end && *end != ';') || (size_t)(end-p) >= 20)
    return sqlite3_bind_null(s, i) == SQLITE_OK;
  errno = 0;
  long long value = strtoll(p, NULL, 10);
  if (errno || value < 0) return sqlite3_bind_null(s, i) == SQLITE_OK;
  return sqlite3_bind_int64(s, i, (sqlite3_int64)value) == SQLITE_OK;
}
static int warehouse_seed(sqlite3 *db) {
  const char *ddl[] = {
    "CREATE TABLE IF NOT EXISTS warehouse_snapshot_head("
      "semantic_kind TEXT NOT NULL,representation_kind TEXT NOT NULL,"
      "generation INTEGER NOT NULL CHECK(generation>0),representation_id TEXT NOT NULL,"
      "digest TEXT NOT NULL,frontier TEXT NOT NULL,created_at TEXT NOT NULL,changed_at TEXT NOT NULL,"
      "PRIMARY KEY(semantic_kind,representation_kind))",
    "CREATE TABLE IF NOT EXISTS warehouse_snapshot_stats("
      "semantic_kind TEXT NOT NULL,representation_kind TEXT NOT NULL,"
      "generation INTEGER NOT NULL,representation_id TEXT NOT NULL,digest TEXT NOT NULL,"
      "realization_count INTEGER NOT NULL,max_realization_bytes INTEGER,frontier_rows INTEGER,"
      "updated_at TEXT NOT NULL,PRIMARY KEY(semantic_kind,representation_kind))",
    "CREATE INDEX IF NOT EXISTS idx_warehouse_ri_latest "
      "ON representation_instance(schema_ref,kind,created_at DESC,id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_warehouse_pr_live "
      "ON physical_realization(representation_id,available,digest)",
    "CREATE INDEX IF NOT EXISTS idx_warehouse_sb_kind ON source_binding(semantic_kind)"
  };
  const char *candidate_sql =
    "SELECT sb.semantic_kind,ri.id,ri.digest,ri.frontier,ri.created_at,"
    "COUNT(pr.id) realization_count,MAX(pr.bytes) max_bytes "
    "FROM source_binding sb "
    "JOIN representation_target rt ON rt.semantic_kind=sb.semantic_kind "
    "AND rt.representation_kind='parquet' AND rt.status='physical' "
    "JOIN representation_instance ri ON ri.schema_ref=sb.semantic_kind "
    "AND ri.kind='parquet' AND ri.state='physical' "
    "JOIN physical_realization pr ON pr.representation_id=ri.id "
    "AND pr.available=1 AND pr.digest=ri.digest "
    "WHERE ri.digest IS NOT NULL AND ri.digest<>'' AND ri.frontier IS NOT NULL "
    "GROUP BY sb.semantic_kind,ri.id ORDER BY sb.semantic_kind COLLATE BINARY,ri.id";
  const char *insert_sql =
    "INSERT OR IGNORE INTO warehouse_snapshot_head VALUES(?,?,?,?,?,?,?,?)";
  const char *check_sql =
    "SELECT generation,representation_id,digest,frontier,created_at "
    "FROM warehouse_snapshot_head WHERE semantic_kind=? AND representation_kind='parquet'";
  const char *stats_sql =
    "INSERT INTO warehouse_snapshot_stats VALUES(?,?,?,?,?,?,?,?,?) "
    "ON CONFLICT(semantic_kind,representation_kind) DO UPDATE SET "
    "generation=excluded.generation,representation_id=excluded.representation_id,"
    "digest=excluded.digest,realization_count=excluded.realization_count,"
    "max_realization_bytes=excluded.max_realization_bytes,frontier_rows=excluded.frontier_rows,"
    "updated_at=excluded.updated_at";
  sqlite3_stmt *candidates=NULL,*add=NULL,*check=NULL,*stats=NULL;
  sqlite3_int64 sources=0,collisions=0,eligible=0,seeded=0,reused=0;
  int active=0,ok=0;
  char stamp[40]={0};
  time_t current=time(NULL);
  struct tm utc;
  if (!gmtime_r(&current,&utc) || !strftime(stamp,sizeof(stamp),"%Y-%m-%dT%H:%M:%SZ",&utc))
    goto done;
  sqlite3_busy_timeout(db,30000);
  if (sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK) goto done;
  active=1;
  if (table_exists(db,"representation_instance")!=1 ||
      table_exists(db,"physical_realization")!=1 ||
      table_exists(db,"source_binding")!=1 ||
      table_exists(db,"representation_target")!=1) goto done;
  if (!count_rows(db,"SELECT COUNT(*) FROM source_binding",&sources) ||
      !count_rows(db,"SELECT COUNT(*) FROM (SELECT 1 FROM source_binding "
        "GROUP BY semantic_kind HAVING COUNT(*)>1)",&collisions) ||
      sources<=0 || collisions!=0) goto done;
  for (size_t i=0;i<sizeof(ddl)/sizeof(ddl[0]);i++)
    if(sqlite3_exec(db,ddl[i],NULL,NULL,NULL)!=SQLITE_OK) goto done;
  if (sqlite3_prepare_v2(db,candidate_sql,-1,&candidates,NULL)!=SQLITE_OK) goto done;
  const char *eligible_sql =
    "SELECT COUNT(*) FROM (SELECT sb.semantic_kind,ri.id "
    "FROM source_binding sb JOIN representation_target rt "
    "ON rt.semantic_kind=sb.semantic_kind AND rt.representation_kind='parquet' "
    "AND rt.status='physical' JOIN representation_instance ri "
    "ON ri.schema_ref=sb.semantic_kind AND ri.kind='parquet' AND ri.state='physical' "
    "JOIN physical_realization pr ON pr.representation_id=ri.id "
    "AND pr.available=1 AND pr.digest=ri.digest "
    "WHERE ri.digest IS NOT NULL AND ri.digest<>'' AND ri.frontier IS NOT NULL "
    "GROUP BY sb.semantic_kind,ri.id)";
  if (!count_rows(db,eligible_sql,&eligible) || eligible!=sources) goto done;
  if (sqlite3_prepare_v2(db,insert_sql,-1,&add,NULL)!=SQLITE_OK ||
      sqlite3_prepare_v2(db,check_sql,-1,&check,NULL)!=SQLITE_OK ||
      sqlite3_prepare_v2(db,stats_sql,-1,&stats,NULL)!=SQLITE_OK) goto done;
  int rc;
  while((rc=sqlite3_step(candidates))==SQLITE_ROW) {
    const char *semantic=(const char*)sqlite3_column_text(candidates,0);
    const char *id=(const char*)sqlite3_column_text(candidates,1);
    const char *digest=(const char*)sqlite3_column_text(candidates,2);
    const char *frontier=(const char*)sqlite3_column_text(candidates,3);
    const char *created=(const char*)sqlite3_column_text(candidates,4);
    if(!semantic||!id||!digest||!frontier||!created) goto done;
    sqlite3_reset(add);sqlite3_clear_bindings(add);
    sqlite3_bind_text(add,1,semantic,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(add,2,"parquet",-1,SQLITE_STATIC);
    sqlite3_bind_int64(add,3,1);
    sqlite3_bind_text(add,4,id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(add,5,digest,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(add,6,frontier,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(add,7,created,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(add,8,stamp,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(add)!=SQLITE_DONE) goto done;
    int fresh=sqlite3_changes(db);
    sqlite3_reset(check);sqlite3_clear_bindings(check);
    sqlite3_bind_text(check,1,semantic,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(check)!=SQLITE_ROW ||
        sqlite3_column_int64(check,0)<1 ||
        strcmp((const char*)sqlite3_column_text(check,1),id) ||
        strcmp((const char*)sqlite3_column_text(check,2),digest) ||
        strcmp((const char*)sqlite3_column_text(check,3),frontier) ||
        strcmp((const char*)sqlite3_column_text(check,4),created)) goto done;
    sqlite3_int64 generation=sqlite3_column_int64(check,0);
    sqlite3_reset(stats);sqlite3_clear_bindings(stats);
    sqlite3_bind_text(stats,1,semantic,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(stats,2,"parquet",-1,SQLITE_STATIC);
    sqlite3_bind_int64(stats,3,generation);
    sqlite3_bind_text(stats,4,id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(stats,5,digest,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(stats,6,sqlite3_column_int64(candidates,5));
    if(sqlite3_column_type(candidates,6)==SQLITE_NULL) sqlite3_bind_null(stats,7);
    else sqlite3_bind_int64(stats,7,sqlite3_column_int64(candidates,6));
    if(!warehouse_rows_bound(stats,8,frontier)) goto done;
    sqlite3_bind_text(stats,9,stamp,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(stats)!=SQLITE_DONE) goto done;
    if(fresh) seeded++; else reused++;
  }
  if(rc!=SQLITE_DONE || seeded+reused!=sources) goto done;
  sqlite3_finalize(candidates);candidates=NULL;
  sqlite3_finalize(add);add=NULL;
  sqlite3_finalize(check);check=NULL;
  sqlite3_finalize(stats);stats=NULL;
  if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK) goto done;
  active=0;ok=1;
done:
  sqlite3_finalize(candidates);
  sqlite3_finalize(add);
  sqlite3_finalize(check);
  sqlite3_finalize(stats);
  if(active) sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
  if(!ok) {
    fprintf(stderr,"warehouse seed refused; transaction rolled back: %s\n",sqlite3_errmsg(db));
    return 0;
  }
  printf("{\"state\":\"HEADS_SEEDED\",\"source_keys\":%lld,\"seeded\":%lld,"
    "\"reused\":%lld,\"statistics_updated\":%lld,\"metadata_only\":true,"
    "\"physical_bytes_verified\":false,\"physical_mutations\":0}\n",
    (long long)sources,(long long)seeded,(long long)reused,(long long)(seeded+reused));
  return 1;
}
#endif
